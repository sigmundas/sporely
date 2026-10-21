"""Pass 3 author-citation comparator (corrects pass 2 after v2/review-codex.md).

An authorship is parsed into roles and people:

    (basionym part) combination part
    part    = main [: sanctioning]           ICN sanctioning citation (Art. F.3)
    main    = [pre-ex ex] validating [in host]
    person  = [initials] [particles] surname [f.]

Changes against pass 2 (`../../v2/scripts/authors_v2.py`):
  * Initials are never particles. 'D. Hawksw.' keeps the initial D.; only an
    unabbreviated lower-case particle word ('de', 'van', 'des', ...) or an
    elided 'd'' / 'l'' is a particle.
  * Particles are kept, not deleted: 'de Vries' and 'van Vries' differ; a
    particle on one side only ('des Abb.' / 'Abbayes') is a candidate.
  * 'f.' / 'fil.' (filius) is a flag on the person, not a surname:
    'Hook. f.' and 'Hook.' are different people.
  * 'A in B': B is a bibliographic host (ICN Art. 46.2). 'A in B' vs 'A' is
    compatible (host omitted); 'A in B' vs 'B' is a role mismatch.
  * Role moves BETWEEN citation parts are detected, whatever the preliminary
    level (also when it is identical/compatible): a person who holds a
    sanctioning, pre-'ex' or 'in'-host role on one side and a different role
    on the other ('(Pers.: Fr.) Fr.' vs 'Pers. ex Fr.'; '(Bull.: Fr.) Lam.'
    vs 'Bull. ex Lam.'; 'Pike in Hale' vs 'Pike: Hale'; '(A in B) C' vs
    '(A: B) C') is a role_mismatch.
  * Letters: fold() lower-cases, maps ð→d, ø→o, æ→ae, ß→ss, þ→th, ł→l
    (capitals too) and strips combining marks. Every other letter is kept,
    so 'Łind.' ≠ 'Ind.', 'Þor' ≠ 'Or', 'Halıcı' ≠ 'Halici'.
  * A multiword surname token (a non-initial, non-particle word before the
    surname: 'Smith John', 'Willi Krieg.', 'San Martín', 'Müll. Arg.',
    'Parg.-Leduc') cannot be split reliably. Such a person is 'identical'
    only when its exact person key matches (folded tokens without spaces and
    full stops: 'Müll. Arg.' = 'Müll.Arg'); 'different' when no word of one
    person equals or abbreviates (is a prefix of) a word of the other
    ('Müll. Arg.' / 'Hale'); otherwise 'unparsed'. Never candidate/compatible.
  * 'et al.' is accepted only once and only at the end of an author list;
    anywhere else ('Smith et al. Jones') the authorship is 'unparsed', even
    when both strings are equal.
  * 'unparsed' is returned for these tested constructs: digits other than a
    strict trailing year; '?', '[', ']' and other non-letter symbols outside
    . , & : ( ) ' -; the qualifiers sensu, auct., ss., s. str., s. lat.,
    lato, stricto, non, nec, emend., pro parte, nom., comb., inval., illeg.,
    nud., ined., prov., sp., nov., stat., orth., var., cf., aff., fide;
    unbalanced, nested, repeated or non-leading parentheses; an empty person
    or list member; more than one ':' in a part, or repeated 'ex'/'in'; an
    initial after a surname word ('Vainio D. Galloway'); misplaced 'et al.'.
    'unparsed' and 'missing' (an empty authorship) are never evidence.
    NOT guaranteed: other malformed citations that happen to fit the grammar
    (e.g. a missing '&' between two unabbreviated surnames is read as one
    multiword surname, handled by the rule above), author identity beyond
    surname/initials/particles/filius (no author standard is used), and
    correctness of the source strings themselves.
  * Typography only is tidied before parsing (`tidy`): whitespace, doubled
    full stops, a trailing comma, a serial comma before '&' and a strict
    trailing publication year (', 1923'; pass 1 dropped it the same way).

Result levels (best to worst), returned as 'level' or 'level:reason[+reason]':
  identical       same people in every role (typography / diacritics only)
  compatible      ICN-permitted omission with every other role identical:
                  sanctioning_omitted, in_host_omitted
  candidate       plausible, unverifiable without an author standard:
                  surname_abbreviation, initials_one_side, initials_prefix,
                  particle_one_side, et_al, pre_ex_omitted, pre_ex_differs,
                  in_host_differs, basionym_part_one_side, combination_part_one_side
  missing         one or both authorships empty
  unparsed        one of the tested unparsable constructs above, or a multiword person
                  whose exact key differs
  role_mismatch   the same person in different roles
  different       a person differs in some role
Nothing below 'compatible' is evidence.
"""
from __future__ import annotations

import re
import unicodedata

PARTICLES = {"de", "des", "du", "van", "von", "der", "den", "la", "le", "di", "da", "del",
             "ter", "dos", "das", "af", "zu", "vom", "dei", "della", "degli"}
ELIDED = {"d", "l"}                        # d'Urv., l'Hér.
FILIUS = {"f", "fil", "filius"}
LEVELS = ("identical", "compatible", "candidate", "missing", "unparsed", "role_mismatch", "different")
RANK = {k: i for i, k in enumerate(LEVELS)}
OK = ("identical", "compatible")
COMPATIBLE_REASONS = {"sanctioning_omitted", "in_host_omitted"}
SPECIAL_ROLES = ("pre", "sanct", "in")
L = r"[^\W\d_]"                            # any Unicode letter
# qualifiers, nomenclatural notes and other non-authorship content: fail closed
_BAD_WORDS = re.compile(r"(?i)(?<![a-z])(sensu|auct|auctt|ss|non|nec|emend|pro\s+parte|s\.\s*str[a-z]*|s\.\s*lat[a-z]*|"
                        r"lato|stricto|nom|comb|inval|illeg|nud|ined|prov|sp|nov|stat|orth|var|cf|aff|fide)(?![a-z])\.?")


class Unparsed(ValueError):
    pass


def fold(s: str) -> str:
    """Lower-case first, then the documented letter map (ð→d, ø→o, æ→ae, ß→ss,
    þ→th, ł→l, also for capitals), then strip combining marks. Any other letter
    (e.g. dotless 'ı') is KEPT, never deleted."""
    s = (s or "").lower().replace("ð", "d").replace("ø", "o").replace("æ", "ae").replace("ß", "ss") \
        .replace("þ", "th").replace("ł", "l").replace("’", "'").replace("`", "'")
    s = unicodedata.normalize("NFKD", s)
    return unicodedata.normalize("NFC", "".join(c for c in s if not unicodedata.combining(c)))


def _chars_ok(s: str) -> bool:
    return all(ch.isalpha() or ch in " .,&:()'-" for ch in s)


def split_paren(a: str):
    """('basionym part' or None, 'combination part'). Raises Unparsed."""
    a = (a or "").strip()
    if a.count("(") != a.count(")") or a.count("(") > 1:
        raise Unparsed("parentheses")
    if "(" in a:
        m = re.match(r"^\((.*)\)\s*(.*)$", a)
        if not m or not m.group(1).strip():
            raise Unparsed("parenthetical not leading")
        return m.group(1).strip(), m.group(2).strip()
    return None, a


def _person(raw: str):
    t = fold(raw).replace(".", ". ")
    t = re.sub(r"\b([dl])'\s*", r"\1' ", t)
    toks = [x for x in t.split() if x]
    if not toks:
        raise Unparsed("empty person")
    filius = False
    if len(toks) >= 2 and toks[-1].rstrip(".") in FILIUS:
        filius = True
        toks = toks[:-1]
    last = toks[-1]
    surname = re.sub(r"[^\w\-]|[\d_]", "", last).strip("-")
    if not surname or (last in PARTICLES) or last.endswith("'"):      # 'Del.' is a surname, 'del' is not
        raise Unparsed("no surname")
    initials, particles, compound = [], [], []
    for x in toks[:-1]:
        core = x.rstrip(".")
        if x.endswith("'") and x[:-1] in ELIDED:
            particles.append(x[:-1])
        elif not x.endswith(".") and core in PARTICLES:
            particles.append(core)
        elif x.endswith(".") and re.fullmatch(rf"-?{L}{{1,3}}", core):
            if particles or compound:
                raise Unparsed("initial after a surname word")   # 'Vainio D. Galloway' (missing '&')
            initials.append(core.lstrip("-"))                    # 'A.-S. Karlsson'
        elif re.fullmatch(rf"(?:{L}|-)+\.?", x):
            compound.append(core)              # multiword surname, or a written-out given name: see compare()
        else:
            raise Unparsed("token " + x)
    if compound:
        surname = " ".join(compound + [surname])
    return {"surname": surname, "initials": tuple(initials), "particles": tuple(particles),
            "filius": filius, "abbr": last.endswith("."), "multiword": bool(compound),
            # exact person key: folded tokens without spaces and full stops ('Müll. Arg.' = 'Müll.Arg')
            "key": "".join(x.rstrip(".") for x in toks) + ("|f" if filius else ""),
            "tokens": tuple(t for t in (re.sub(r"[^\w]|[\d_]", "", x) for x in toks) if t)}


def _people(s: str):
    s = (s or "").strip()
    n_etal = len(re.findall(r"\bet\s+al\b", s, re.I))
    etal = bool(re.search(r"\s\bet\s+al\.?$", s, re.I))
    if n_etal > (1 if etal else 0):
        raise Unparsed("'et al.' not at the end of the author list")   # 'Smith et al. Jones'
    s = re.sub(r"\s*\bet\s+al\.?$", "", s, flags=re.I)
    parts = re.split(r"\s*&\s*|\s*,\s*|\s+et\s+", s)
    if any(not p.strip() for p in parts):
        raise Unparsed("empty list member")
    return [_person(p) for p in parts], etal


def parse_part(p: str | None):
    """Parse one part ('A ex B in C : D'). Returns None for None; raises Unparsed."""
    if p is None:
        return None
    if p.count(":") > 1:
        raise Unparsed("several sanctioning colons")
    main, sanct = (p.split(":", 1) if ":" in p else (p, None))
    if sanct is not None and not sanct.strip():
        raise Unparsed("empty sanctioning author")
    if len(re.findall(r"\s+in\s+", main)) > 1 or len(re.findall(r"\s+ex\.?\s+", main)) > 1:
        raise Unparsed("repeated in/ex")
    host = None
    if re.search(r"\s+in\s+", main):
        main, host = re.split(r"\s+in\s+", main, maxsplit=1)
    pre = None
    if re.search(r"\s+ex\.?\s+", main):
        pre, main = re.split(r"\s+ex\.?\s+", main, maxsplit=1)
    if not main.strip():
        raise Unparsed("empty main author")
    return {"pre": _people(pre) if pre else None, "valid": _people(main),
            "in": _people(host) if host else None, "sanct": _people(sanct) if sanct else None}


def tidy(a: str) -> str:
    """Typography only: whitespace, doubled full stops, a trailing comma, ', &'
    (serial comma) and a strict trailing publication year (', 1923'), as pass 1."""
    a = re.sub(r"\s+", " ", unicodedata.normalize("NFC", a or "")).strip()
    a = re.sub(r"(?<=[\w.)]),?\s+\(?[12]\d{3}\)?$", "", a)
    a = re.sub(r"\.{2,}", ".", a)
    a = re.sub(r",\s*&", " &", a)
    return a.rstrip(", ").strip()


def parse(a: str):
    a = tidy(a)
    if not _chars_ok(a) or _BAD_WORDS.search(fold(a)):
        raise Unparsed("characters or qualifier")
    b, c = split_paren(a)
    return parse_part(b), parse_part(c or None)


def _initials_cmp(x, y):
    if x == y:
        return "identical", set()
    if not x or not y:
        return "candidate", {"initials_one_side"}
    short, long_ = (x, y) if len(x) <= len(y) else (y, x)
    if all(long_[i].startswith(short[i]) or short[i].startswith(long_[i]) for i in range(len(short))):
        return "candidate", {"initials_prefix"}          # W. vs W.A., Th. vs T.
    return "different", set()


def person_cmp(a, b):
    """(level, reasons) for two people."""
    if a["multiword"] or b["multiword"]:
        # A multiword surname token ('Smith John', 'Willi Krieg.', 'San Martín', 'Müll. Arg.') cannot be
        # split into given name and surname reliably: identical only on the exact person key, else unparsed.
        if a["key"] == b["key"]:
            return "identical", set()
        ta, tb = a["tokens"], b["tokens"]
        if not any(x.startswith(y) or y.startswith(x) for x in ta for y in tb):
            return "different", set()                 # no shared or abbreviated word at all ('Müll. Arg.' / 'Hale')
        return "unparsed", set()
    if a["filius"] != b["filius"]:
        return "different", set()
    reasons = set()
    if a["particles"] != b["particles"]:
        if a["particles"] and b["particles"]:
            return "different", set()                       # de Vries vs van Vries
        reasons.add("particle_one_side")
    if a["surname"] != b["surname"]:
        x, y = a["surname"], b["surname"]
        if " " in x or " " in y or not (min(len(x), len(y)) >= 3 and (x.startswith(y) or y.startswith(x))
                and ((a["abbr"] and len(x) < len(y)) or (b["abbr"] and len(y) < len(x)))):
            return "different", set()
        reasons.add("surname_abbreviation")                  # Schaer. / Schaerer, Heiðm. / Heidmarsson
    lvl, r = _initials_cmp(a["initials"], b["initials"])
    if lvl == "different":
        return "different", set()                           # J.E. Lange vs M. Lange, D. vs J. Hawksw.
    reasons |= r
    return ("candidate", reasons) if reasons else ("identical", set())


def list_cmp(x, y):
    (px, ex), (py, ey) = x, y
    if len(px) != len(py):
        n = min(len(px), len(py))
        lv = [person_cmp(a, b)[0] for a, b in zip(px, py)]
        if (ex or ey) and n and all(x in ("identical", "candidate") for x in lv):
            return "candidate", {"et_al"}
        if (ex or ey) and n and "unparsed" in lv and "different" not in lv:
            return "unparsed", set()
        return "different", set()
    worst, reasons = "identical", set()
    for a, b in zip(px, py):
        lvl, r = person_cmp(a, b)
        if RANK[lvl] > RANK[worst]:
            worst = lvl
        reasons |= r
    if worst == "different":
        return "different", set()
    if ex != ey and worst in ("identical", "candidate"):
        reasons.add("et_al")
        worst = "candidate"
    return worst, reasons


def _combine(levels):
    lvl = max((x[0] for x in levels), key=RANK.get)
    reasons = set()
    for x in levels:
        if x[0] in ("compatible", "candidate"):
            reasons |= x[1]
    return lvl, reasons


def part_cmp(a, b, which: str = "basionym"):
    """(level, reasons) for two parsed parts (either may be None)."""
    if a is None and b is None:
        return "identical", set()
    if a is None or b is None:
        return "candidate", {f"{which}_part_one_side"}
    v = list_cmp(a["valid"], b["valid"])
    if v[0] == "different":
        return "different", set()
    levels = [v]
    if a["pre"] and b["pre"]:
        p = list_cmp(a["pre"], b["pre"])
        levels.append(p if p[0] != "different" else ("candidate", {"pre_ex_differs"}))
    elif a["pre"] or b["pre"]:
        levels.append(("candidate", {"pre_ex_omitted"}))   # 'Nägeli ex Hepp' vs 'Hepp'
    if a["in"] and b["in"]:
        h = list_cmp(a["in"], b["in"])
        levels.append(h if h[0] != "different" else ("candidate", {"in_host_differs"}))
    elif a["in"] or b["in"]:
        levels.append(("compatible", {"in_host_omitted"}))  # 'Pike in Hale' vs 'Pike'
    if a["sanct"] and b["sanct"]:
        s = list_cmp(a["sanct"], b["sanct"])
        if s[0] == "different":
            return "different", set()                      # 'Sowerby: Fr.' vs 'Vahl: Fr.'
        levels.append(s)
    elif a["sanct"] or b["sanct"]:
        levels.append(("compatible", {"sanctioning_omitted"}))
    return _combine(levels)


def _roles(parsed):
    out = []
    for pname, part in zip(("bas", "comb"), parsed):
        if part is None:
            continue
        for role in ("pre", "valid", "in", "sanct"):
            if part[role]:
                out += [(p, f"{pname}.{role}") for p in part[role][0]]
    return out


def _same_person(p, q):
    return person_cmp(p, q)[0] in ("identical", "candidate")


def role_moves(pa, pb) -> list[str]:
    """People who hold a special role (pre-ex, sanctioning, in-host) on one side
    and, on the other side, a role they do not hold on the first side."""
    ra, rb = _roles(pa), _roles(pb)
    moves = []
    for X, Y in ((ra, rb), (rb, ra)):
        for p, r in X:
            if r.split(".")[1] not in SPECIAL_ROLES:
                continue
            if any(_same_person(p, q) and rq == r for q, rq in Y):
                continue
            ys = {rq for q, rq in Y if _same_person(p, q)}
            xs = {rx for q, rx in X if _same_person(p, q)}
            if ys - xs:
                moves.append(f"{p['surname']}:{r}->{'/'.join(sorted(ys - xs))}")
    return sorted(set(moves))


def _fmt(lvl, reasons):
    return lvl + (":" + "+".join(sorted(reasons)) if reasons and lvl in ("compatible", "candidate") else "")


def compare(a: str, b: str) -> str:
    """Overall level for two full authorship strings."""
    a, b = (a or "").strip(), (b or "").strip()
    if not a or not b:
        return "missing"
    try:
        pa, pb = parse(a), parse(b)
    except Unparsed:
        return "unparsed"
    lvl, reasons = _combine([part_cmp(pa[0], pb[0], "basionym"), part_cmp(pa[1], pb[1], "combination")])
    if role_moves(pa, pb):                     # also when the preliminary result is OK ('Pike in Hale' / 'Pike: Hale')
        return "role_mismatch"
    return _fmt(lvl, reasons)


def basionym_cmp(a: str, b: str) -> str | None:
    """Level for the parenthetical parts only; None if either has none or is empty."""
    try:
        ba, bb = split_paren(tidy(a))[0], split_paren(tidy(b))[0]
        if ba is None or bb is None:
            return None
        pa, pb = parse_part(ba), parse_part(bb)
    except Unparsed:
        return "unparsed"
    lvl, reasons = part_cmp(pa, pb)
    if role_moves((pa, None), (pb, None)):
        return "role_mismatch"
    return _fmt(lvl, reasons)


def original_author(a: str) -> str | None:
    """The author of the name-bringing (original) combination: the parenthetical
    part if present, else the whole citation. None if empty; raises Unparsed."""
    a = (a or "").strip()
    if not a:
        return None
    b, c = split_paren(a)
    return b if b is not None else c


def original_cmp(a: str, b: str) -> str:
    """Compare the original-combination authors of two homotypic names."""
    try:
        oa, ob = original_author(tidy(a)), original_author(tidy(b))
    except Unparsed:
        return "unparsed"
    if oa is None or ob is None:
        return "missing"
    return compare(oa, ob)


def level(x: str | None) -> str | None:
    return None if x is None else x.split(":")[0]


def reasons(x: str | None) -> set[str]:
    return set(x.split(":", 1)[1].split("+")) if x and ":" in x else set()
