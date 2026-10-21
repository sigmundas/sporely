"""Pass 2 author-citation comparator (review-codex finding 4).

Parses an authorship into roles and compares author IDENTITIES, never just
surnames:

    (basionym part) combination part
    part   = main [: sanctioning]        ICN sanctioning citation
    main   = [pre-ex ex] validating      ICN 'ex' citation
    author = [initials] surname          'J.E. Lange', 'Th. Fr.'

Per person: 'same' needs the same folded surname AND the same initials (or
none on both sides). 'Fr.' and 'Th. Fr.' are different people, as are
'J.E. Lange' and 'M. Lange'.

Result levels, best to worst:
  identical            same people in every role (typography/diacritics only)
  compatible           a part omits a sanctioning citation that the other has,
                       with every other role identical (ICN Art. 7.7 / 15:
                       the sanctioning citation may be omitted)
  candidate:<why>      plausible but unverifiable without an author standard:
                       surname abbreviation prefix, initials on one side only,
                       'et al.' / partial list, 'A ex B' vs 'B', basionym part
                       on one side only
  role_mismatch        same names in different roles, e.g. 'A: B' vs 'A ex B'
                       (sanctioning versus validating author), 'A ex B' vs 'A'
  different            a person differs in some role
Nothing below 'compatible' is evidence.
"""
from __future__ import annotations

import re
import unicodedata

PARTICLES = {"de", "des", "du", "van", "von", "der", "la", "le", "d", "di", "da", "del", "den", "ter"}
RANK = {"identical": 0, "compatible": 1, "candidate": 2, "role_mismatch": 3, "different": 4}


def fold(s: str) -> str:
    s = (s or "").replace("ð", "d").replace("Ð", "D").replace("ø", "o").replace("Ø", "O") \
        .replace("æ", "ae").replace("Æ", "Ae").replace("ß", "ss").replace("þ", "th").replace("ł", "l")
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c)).lower()


def split_paren(a: str):
    m = re.match(r"^\s*\((.+?)\)\s*(.*)$", a or "")
    return (m.group(1).strip(), m.group(2).strip()) if m else (None, (a or "").strip())


def _person(raw: str):
    toks = [t for t in re.split(r"[\s]+", fold(raw).replace(".", ". ").strip()) if t]
    toks = [t for t in toks if t.rstrip(".") not in PARTICLES]
    if not toks:
        return None
    surname = re.sub(r"[^a-z\-]", "", toks[-1]).strip("-")
    abbreviated = toks[-1].endswith(".")
    initials = tuple(re.sub(r"[^a-z]", "", t) for t in toks[:-1] if re.sub(r"[^a-z]", "", t))
    return {"surname": surname, "initials": initials, "abbr": abbreviated}


def _people(s: str):
    s = (s or "").strip()
    etal = bool(re.search(r"\bet\s+al\.?", s, re.I))
    s = re.sub(r"\bet\s+al\.?", "", s, flags=re.I)
    parts = [p for p in re.split(r"\s*&\s*|\s*,\s*|\s+et\s+", s) if p.strip()]
    return [p for p in map(_person, parts) if p], etal


def parse_part(p: str | None):
    if p is None:
        return None
    main, sanct = (p.split(":", 1) + [None])[:2] if ":" in p else (p, None)
    pre, valid = (re.split(r"\s+ex\s+", main, maxsplit=1) + [None])[:2] if re.search(r"\s+ex\s+", main) else (None, main)
    if valid is None:
        valid, pre = pre, None
    return {"pre": _people(pre) if pre else None, "valid": _people(valid),
            "sanct": _people(sanct) if sanct and sanct.strip() else None}


def person_cmp(a, b) -> str:
    if a["surname"] == b["surname"]:
        if a["initials"] == b["initials"]:
            return "identical"
        x, y = a["initials"], b["initials"]
        if not x or not y or x[:len(y)] == y or y[:len(x)] == x:
            return "candidate"            # initials on one side only, or W. vs W.A.
        return "different"                # J.E. Lange vs M. Lange
    x, y = a["surname"], b["surname"]
    if min(len(x), len(y)) >= 3 and (x.startswith(y) or y.startswith(x)) and (a["abbr"] or b["abbr"]):
        if a["initials"] == b["initials"] or not a["initials"] or not b["initials"]:
            return "candidate"            # Schaer. / Schaerer, Heiðm. / Heidmarsson
    return "different"


def list_cmp(x, y) -> str:
    (px, ex), (py, ey) = x, y
    if not px or not py:
        return "different"
    if len(px) != len(py):
        if (ex or ey) and all(person_cmp(a, b) != "different" for a, b in zip(px, py)):
            return "candidate"            # et al.
        return "different"
    worst = max((person_cmp(a, b) for a, b in zip(px, py)), key=RANK.get)
    if worst == "identical" and (ex != ey):
        return "candidate"
    return worst


def part_cmp(a, b) -> str:
    if a is None and b is None:
        return "identical"
    if a is None or b is None:
        return "candidate"                # basionym part on one side only
    levels = [list_cmp(a["valid"], b["valid"])]
    if levels[0] == "different":
        # 'A ex B' vs 'A' (validating author dropped, pre-ex kept) is a role mismatch
        if a["pre"] and b["pre"] is None and list_cmp(a["pre"], b["valid"]) in ("identical", "compatible"):
            return "role_mismatch"
        if b["pre"] and a["pre"] is None and list_cmp(b["pre"], a["valid"]) in ("identical", "compatible"):
            return "role_mismatch"
        # 'A: B' vs 'A ex B' (sanctioning vs validating)
        if a["sanct"] and b["pre"] and list_cmp(a["valid"], b["pre"]) == "identical" \
                and list_cmp(a["sanct"], b["valid"]) == "identical":
            return "role_mismatch"
        if b["sanct"] and a["pre"] and list_cmp(b["valid"], a["pre"]) == "identical" \
                and list_cmp(b["sanct"], a["valid"]) == "identical":
            return "role_mismatch"
        # 'A: B' vs 'B' (the sanctioning author cited as if he were the author)
        if a["sanct"] and not b["sanct"] and not b["pre"] and list_cmp(a["sanct"], b["valid"]) == "identical":
            return "role_mismatch"
        if b["sanct"] and not a["sanct"] and not a["pre"] and list_cmp(b["sanct"], a["valid"]) == "identical":
            return "role_mismatch"
        return "different"
    if a["pre"] and b["pre"]:
        levels.append(list_cmp(a["pre"], b["pre"]))
    elif a["pre"] or b["pre"]:
        levels.append("candidate")        # 'A ex B' vs 'B'
    if a["sanct"] and b["sanct"]:
        levels.append(list_cmp(a["sanct"], b["sanct"]))
    elif a["sanct"] or b["sanct"]:
        levels.append("compatible")       # sanctioning citation omitted
    return max(levels, key=RANK.get)


def compare(a: str, b: str) -> str:
    """Overall level for two full authorship strings ('' -> 'candidate')."""
    a, b = (a or "").strip(), (b or "").strip()
    if not a or not b:
        return "candidate"
    (ba, ca), (bb, cb) = split_paren(a), split_paren(b)
    ca, cb = ca or None, cb or None
    return max(part_cmp(parse_part(ba), parse_part(bb)), part_cmp(parse_part(ca), parse_part(cb)), key=RANK.get)


def basionym_cmp(a: str, b: str) -> str | None:
    """Level for the basionym (parenthetical) parts only; None if either has none."""
    ba, bb = split_paren(a)[0], split_paren(b)[0]
    if ba is None or bb is None:
        return None
    return part_cmp(parse_part(ba), parse_part(bb))


def combination_cmp(a: str, b: str) -> str:
    return part_cmp(parse_part(split_paren(a)[1] or None), parse_part(split_paren(b)[1] or None))
