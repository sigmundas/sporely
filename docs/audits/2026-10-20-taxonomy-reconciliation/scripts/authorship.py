"""Authorship comparison for the 2026-10-20 reconciliation audit
(revision 3, after review-2: docs/audits/2026-10-20-taxonomy-reconciliation/review-2.md).

FAIL-CLOSED DESIGN. No recognised author-abbreviation reference (IPNI /
Index Fungorum standard forms) is available locally: the repository has none,
the NorTaxa archive has none, and the COL archive's
col:combinationAuthorshipID / basionymAuthorshipID columns are empty for all
620,974 Fungi rows (checked 2026-10-20). Therefore no abbreviation can be
mapped to a standard form, and abbreviation equivalence is NEVER a basis for
an executable batch (A0). The former KNOWN_DISTINCT block-list is removed.

compare(a, b) returns one of:

  A0-eligible (EXACT):
  identical            equal after whitespace collapse
  typography           equal after NFKD diacritic strip, casefold and removal
                       of whitespace, periods, commas, apostrophes and hyphens;
                       STRUCTURE IS KEPT: parentheses, ':' and '&' must match
                       ("Scop.: Fr." != "(Scop.) Fr.")
  citation_stripped    typography-equal after dropping ONLY a trailing
                       publication year (rev 4; no comma-tail/citation stripping)
  sanctioning          typography-equal after removing ": Fr." / ": Pers."
                       only (the sanctioning authors seen in the data; rev 4)
  never batchable (rev 4): malformed_authorship (unbalanced/nested/multiple
                       parentheses); identical strings carrying a qualifier
  A1 only (needs a standard-form list or expert validation):
  abbreviation         every author slot (basionym and combining, in order)
                       matches under the person rule below, with the
                       validating (post-"ex") author compared
  never batchable:
  qualified_usage      "ss.", "sensu", "non", "auct.", "p.p.", "pro parte",
                       "s.l.", "s. lat.", "s. str.", "emend.", "excl.", "incl.",
                       "fide", "nom. illeg./inval./nud./rej./superfl."
  unpublished_or_doubtful  "ined.", "nom. prov.", "prov.", "comb. ined.",
                       "ad int.", "?"
  missing_one / missing_both / ex_author_differs / basionym_omitted /
  partial_list / conflicting

Person rule (A1 only): surnames equal after folding; or the shorter surname is
written as an abbreviation (token ends at a period) and is a prefix of a
surname written in full. Two different full surnames (Lind / Lindau) or two
different abbreviations (Fr. / Fresen., Mich. / Michx.) never match. A prefix
match (Fr. / Fritsch, Fr. / Frank, Bres. / Bresinsky) is at most A1. Initials
cited on both sides must agree.
"""
from __future__ import annotations

import re
import unicodedata

EXACT = ("identical", "typography", "citation_stripped", "sanctioning")
COMPATIBLE = EXACT + ("abbreviation",)
NON_BATCHABLE_QUALIFIER = ("qualified_usage", "unpublished_or_doubtful", "malformed_authorship")
ORDER = COMPATIBLE + NON_BATCHABLE_QUALIFIER + (
    "missing_one", "missing_both", "ex_author_differs", "basionym_omitted",
    "partial_list", "conflicting")


_QUALIFIER = re.compile(
    r"(\bss\.|\bsensu\b|\bnon\b|\bauct\b\.?|\bauctt?\.|\bp\.\s*p\b\.?|\bpro\s+parte\b|"
    r"(?:^|(?<=[\s,(]))s\.\s*(?:l|lat|str)\.?(?=[\s,)]|$)|\bsensu\s+lato\b|\bemend\b\.?|"
    r"\bexcl\b\.?|\bincl\b\.?|\bfide\b|\bauctorum\b|\bnom\.?\s*(illeg|inval|nud|rej|superfl)\b|\bnom\.?\s*cons\.?\s*prop\b)",
    re.I)
_UNPUBLISHED = re.compile(
    r"(\bined\b\.?|\bnom\.\s*prov\b\.?|\bprov\.|\bcomb\.\s*ined\b|\bad\s+int\b\.?|\?)", re.I)


def _ws(s: str) -> str:
    return " ".join(unicodedata.normalize("NFC", str(s or "").strip()).split())


def _fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.replace("ø", "o").replace("Ø", "O").replace("æ", "ae").replace("Æ", "AE") \
        .replace("ß", "ss").replace("ł", "l").casefold()


def _squash(s: str) -> str:
    """Typography key that keeps structure: '(' ')' ':' '&' ',' survive
    (rev 4: list separators are significant, 'Smith, Brown' != 'Smith Brown')."""
    s = _fold(s)
    s = re.sub(r"\s*:\s*", ":", s)
    s = re.sub(r"\s*&\s*", "&", s)
    s = re.sub(r"\s*,\s*", ",", s)
    return re.sub(r"[\s\.;'’`\-]+", "", s)


#: Sanctioning authors actually seen after ':' in both archives (rev 4):
#: 'Fr.' (3,432 + case/period variants) and 'Pers.' (24). Nothing else is
#: stripped as a sanctioning citation ("Smith: Jones" is NOT sanctioning).
_SANCT = re.compile(r"\s*:\s*(?:fr|pers)\.?(?=\s*\)|\s*$)", re.I)
_YEAR = re.compile(r",?\s*\(?\b1[5-9]\d\d\b\)?|,?\s*\b20\d\d\b")
#: Strict: only a single trailing publication year, ", 1871" / " 1871" / " (1871)".
_TRAIL_YEAR = re.compile(r"(?:,\s*|\s+)\(?(1[5-9]\d\d|20\d\d)\)?\s*$")


def flags(s: str) -> set[str]:
    out = set()
    s = _ws(s)
    if _QUALIFIER.search(s):
        out.add("qualified_usage")
    if _UNPUBLISHED.search(s):
        out.add("unpublished_or_doubtful")
    return out


def _clean(s: str) -> str:
    """Revision 4: drop ONLY a trailing publication year (strict pattern).
    Bibliographic citations and comma tails are no longer stripped; a pair that
    differs by one is not citation_stripped (fail-closed)."""
    return _TRAIL_YEAR.sub("", s).strip()


def malformed(s: str) -> bool:
    """Unbalanced or nested parentheses, more than one parenthesised group,
    or a ')' before its '(' (e.g. '((Roberge ex Desm.) J. Schröt.')."""
    depth = groups = 0
    for ch in s:
        if ch == "(":
            depth += 1
            groups += 1
            if depth > 1:
                return True
        elif ch == ")":
            depth -= 1
            if depth < 0:
                return True
    return depth != 0 or groups > 1

def _strip_sanct(s: str) -> str:
    return _SANCT.sub("", s)


def _split_parts(s: str) -> tuple[str | None, str]:
    s = s.strip()
    m = re.match(r"^\((.*?)\)\s*(.*)$", s)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return None, s


def _authors(part: str) -> tuple[list[list[str]], bool]:
    """List of author groups; a group is [pre-ex..., validating]; et-al flag."""
    part = _YEAR.sub("", part).strip(" ,")
    etal = bool(re.search(r"\bet\s+al\b\.?", part, re.I))
    part = re.sub(r"\bet\s+al\b\.?", "", part, flags=re.I)
    out = []
    for raw in re.split(r"\s*(?:&|,|\bet\b|\band\b)\s*", part):
        raw = raw.strip()
        if not raw:
            continue
        alts = [x.strip() for x in re.split(r"\s+\bex\b\.?\s+", raw) if x.strip()]
        out.append(alts)
    return out, etal


def _person(a: str) -> tuple[list[str], str, bool]:
    """(initials, surname, surname_is_abbreviated)."""
    f = _fold(a).strip()
    abbreviated = f.endswith(".")
    toks = [t for t in re.split(r"[\.\s]+", f) if t]
    toks = [re.sub(r"[^a-z\-']", "", t) for t in toks]
    toks = [t for t in toks if t]
    if not toks:
        return [], "", False
    surname = toks[-1]
    initials = toks[:-1]
    while initials and initials[-1] in ("de", "van", "von", "der", "da", "du", "le", "la", "di"):
        surname = initials.pop() + surname
    return initials, surname, abbreviated


def _author_compatible(a: str, b: str) -> bool:
    ia, sa, aa = _person(a)
    ib, sb, ab = _person(b)
    if not sa or not sb:
        return False
    if sa != sb:
        short, long_ = (sa, sb) if len(sa) <= len(sb) else (sb, sa)
        short_abbr = aa if short == sa else ab
        long_abbr = ab if short == sa else aa
        # only "abbreviation of a full surname": Vain. / Vainio
        if not (long_.startswith(short) and short_abbr and not long_abbr):
            return False
    short_len = min(len(sa), len(sb))
    if ia and ib:
        return all(x[0] == y[0] for x, y in zip(ia, ib))
    if ia or ib:
        return short_len >= 4 or (sa == sb and short_len >= 3)
    return short_len >= 2


def _group_compare(x: list[str], y: list[str]) -> str:
    """'same', 'ex_differs' or 'conflict' for one author slot."""
    vx, vy = x[-1], y[-1]  # validating author
    if _author_compatible(vx, vy):
        if len(x) > 1 and len(y) > 1 and not _author_compatible(x[0], y[0]):
            return "ex_differs"
        return "same"
    # agreement only through a pre-ex author ("Cooke" vs "Cooke ex Stev.",
    # "Secr. ex Gillet" vs "Secr. ex J. Schröt.")
    if any(_author_compatible(p, q) for p in x for q in y):
        return "ex_differs"
    return "conflict"


def _list_compare(x, y) -> str:
    (ax, ex), (ay, ey) = x, y
    if not ax and not ay:
        return "same"
    n = min(len(ax), len(ay))
    worst = "same"
    for i in range(n):
        g = _group_compare(ax[i], ay[i])
        if g == "conflict":
            return "conflict"
        if g == "ex_differs":
            worst = "ex_differs"
    if worst != "same":
        return worst
    if len(ax) == len(ay):
        return "same"
    if (len(ax) < len(ay) and ex) or (len(ay) < len(ax) and ey):
        return "same"
    return "prefix"


def compare(a: str, b: str) -> str:
    a, b = _ws(a), _ws(b)
    if not a and not b:
        return "missing_both"
    fl = flags(a) | flags(b)
    if "qualified_usage" in fl:
        return "qualified_usage"
    if "unpublished_or_doubtful" in fl:
        return "unpublished_or_doubtful"
    if malformed(a) or malformed(b):
        return "malformed_authorship"
    if not a or not b:
        return "missing_one"
    if a == b:
        return "identical"
    if _squash(a) == _squash(b):
        return "typography"
    a, b = _clean(a), _clean(b)
    if _squash(a) == _squash(b):
        return "citation_stripped"
    sa, sb = _strip_sanct(a), _strip_sanct(b)
    if _squash(sa) == _squash(sb):
        return "sanctioning"
    pa, ca = _split_parts(sa)
    pb, cb = _split_parts(sb)
    comb = _list_compare(_authors(ca), _authors(cb))
    if comb == "conflict":
        return "conflicting"
    if (pa is None) != (pb is None):
        return "basionym_omitted" if comb == "same" else "conflicting"
    bas = "same" if pa is None else _list_compare(_authors(pa), _authors(pb))
    if bas == "conflict":
        return "conflicting"
    if "ex_differs" in (comb, bas):
        return "ex_author_differs"
    if comb == "same" and bas == "same":
        return "abbreviation"
    return "partial_list"


def compatible(a: str, b: str) -> bool:
    return compare(a, b) in COMPATIBLE


NOT_A0 = "!EXACT"  # expectation: any class outside EXACT (never an A0 basis)

CASES = [
    # --- A0-eligible
    ("(Schaeff. : Fr.) P. Kumm.", "(Schaeff.) P. Kumm.", "sanctioning"),
    ("(Tuck.)Th.Fr.", "(Tuck.) Th.Fr.", "typography"),
    ("Gjærum", "Gjaerum", "typography"),
    ("Bandini, B. Oertel & U. Eberh, 2022", "Bandini, B. Oertel & U. Eberh.", "citation_stripped"),
    ("(Schaeff.) P. Kumm.", "(Schaeff.) P.Kumm., 1871", "citation_stripped"),
    # --- A1 only: person-level equivalence without a standard-form reference
    ("Vainio", "Vain.", "abbreviation"),
    ("(Ach.) Schaerer", "(Ach.) Schaer.", "abbreviation"),
    ("(Vahl) S.F. Gray", "(Vahl) Gray", "abbreviation"),
    ("Elix & Jen Johnston", "Elix & J. Johnst.", "abbreviation"),
    ("S. Ito", "Ito, 1959", "abbreviation"),
    ("Knudsen & T. Borgen", "Bas ex Knudsen & T. Borgen", "abbreviation"),
    ("(H.Magn.) Arup et al.", "(H. Magn.) Arup, Søchting & Frödén", "abbreviation"),
    ("Spirin & Malysheva", "Lloyd ex Spirin & Malysheva, in Malysheva & Spirin, Fungal Biology 121 (8): 709, 2017", NOT_A0),
    ("(Ach.) S.L.F. Meyer", "(Ach.) S.L.F. Mey.", "abbreviation"),
    # prefix of a different person: never A0 (at most A1; expert check)
    ("Fr.", "Fritsch", NOT_A0),
    ("Fr.", "Frank", NOT_A0),
    ("Bres.", "Bresinsky", NOT_A0),
    ("Arn.", "Arnaud", NOT_A0),
    ("Henn.", "Hennebert", NOT_A0),
    # --- never batchable: different people / forms
    ("Fr.", "Th. Fr.", "conflicting"),
    ("Fr.", "Fresen.", "conflicting"),
    ("Bon", "Bonord.", "conflicting"),
    ("(P. Karst.) Lind", "(P. Karst.) Lindau", "conflicting"),
    ("Rick", "Ricken", "conflicting"),
    ("Lange", "Langer", "conflicting"),
    ("Kühner", "Kühn", "conflicting"),
    ("(Mich.) Vain.", "(Michx.) Vain.", "conflicting"),
    ("(Nyl.) Gyeln.", "(Nyl.) Gyel.", "conflicting"),
    ("Burt", "LLoyd", "conflicting"),
    ("(Mudd) Hepp", "(Leight.) Hepp", "conflicting"),
    ("Kashiw. & K.M. Moon", "Kashiw. & K.H. Moon", "conflicting"),
    ("(Pers.) Gray", "(Pers.) Fr., 1818", "conflicting"),
    # structure: sanctioning vs basionym parentheses
    ("Scop.: Fr.", "(Scop.) Fr.", NOT_A0),
    ("Scop. : Fr.", "(Scop.) Fr.", NOT_A0),
    ("Müll. Arg.", "(Tuck.) Müll. Arg.", "basionym_omitted"),
    ("P.M. Jørg.", "P.M. Jørg. & Kashiw.", "partial_list"),
    # validating author after ex
    ("Secr. ex Gillet", "Secr. ex J. Schröt.", "ex_author_differs"),
    ("(Nyl. ex Willey) R.C. Harris", "(Nyl. ex Fink) R.C. Harris", "ex_author_differs"),
    ("(Berk. & M.A. Curtis ex Cooke) Bondartseva & S. Herrera",
     "(Berk. & M.A. Curtis ex A.L. Sm.) Bondartseva & S. Herrera", "ex_author_differs"),
    ("(Cooke) Nannf.", "(Cooke ex Stev.) Nannf.", "ex_author_differs"),
    # misapplied / qualified (either side)
    ("Singer ss. Blum (non Singer)", "Singer", "qualified_usage"),
    ("sensu Lange", "(Fr.) P. Kumm.", "qualified_usage"),
    ("auct.", "Fr.", "qualified_usage"),
    ("Pers. p.p.", "Pers.", "qualified_usage"),
    ("Fr. nom. illeg.", "Fr.", "qualified_usage"),
    ("Fr. nom. inval.", "Fr. nom. inval.", "qualified_usage"),
    ("(Fr.) Fr. s.l.", "(Fr.) Fr.", "qualified_usage"),
    # provisional / unpublished / doubtful (either side, even when identical)
    ("Niskanen, Kytöv. & Liimat. (ined.)", "Niskanen, Kytöv. & Liimat.", "unpublished_or_doubtful"),
    ("Bjerke & Elvebakk, ined.", "Bjerke & Elvebakk", "unpublished_or_doubtful"),
    ("Øvst. ined", "Øvstedal", "unpublished_or_doubtful"),
    ("(Nyl.) Lettau (?)", "(Nyl.) Lettau", "unpublished_or_doubtful"),
    ("Marr & D.E. Stuntz nom. prov.", "Marr & D.E. Stuntz nom.prov.", "unpublished_or_doubtful"),
    ("Kotl. nom. prov.", "Kotl.", "unpublished_or_doubtful"),
    ("Bas ined.", "Bas ined.", "unpublished_or_doubtful"),
    ("nom. prov.", "nom.prov.", "unpublished_or_doubtful"),   # Ramaria ammophila 626337/603885
    # --- rev 4 (Codex review-3) fail-closed probes
    ("Smith: Jones", "Smith", NOT_A0),
    ("Smith, Jones: Brown", "Smith", NOT_A0),
    ("Smith, Brown", "Smith Brown", NOT_A0),
    ("Smith nom illeg", "Smith nom illeg", "qualified_usage"),
    ("Smith auctorum", "Smith auctorum", "qualified_usage"),
    ("((Roberge ex Desm.) J. Schröt.", "(Roberge ex Desm.) J. Schröt.", "malformed_authorship"),
    ("Vouaux) R. Sierra & Molinari", "(Vouaux) R. Sierra & Molinari", "malformed_authorship"),
    ("Kalchbr. & MacOwan", "Kalchbr. & MacOwan, Grevillea 10: 58, 1881", NOT_A0),
    ("(Bull. : Fr.) P. Karst.", "(Bull.) P. Karst.", "sanctioning"),
    ("(Pers. : Pers.) Boud.", "(Pers.) Boud.", "sanctioning"),
    ("nom. prov.", "nom.prov", "unpublished_or_doubtful"),    # Ascocoryne inflata 623714
]


def _ok(got: str, expected: str) -> bool:
    return got not in EXACT if expected == NOT_A0 else got == expected


if __name__ == "__main__":
    bad = [(a, b, e, compare(a, b)) for a, b, e in CASES if not _ok(compare(a, b), e)]
    bad += [(b, a, e, compare(b, a)) for a, b, e in CASES if not _ok(compare(b, a), e)]
    for row in bad:
        print("MISMATCH", row)
    print(f"ok ({len(CASES)} cases, both orders)" if not bad else f"{len(bad)} mismatches")
    raise SystemExit(1 if bad else 0)
