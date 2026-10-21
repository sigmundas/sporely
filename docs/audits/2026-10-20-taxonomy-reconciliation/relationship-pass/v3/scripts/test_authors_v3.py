"""Tests for the pass-3 comparator: pass-2 cases kept, plus the v2/review-codex.md
negative cases (findings 2-5).

  <venv>/bin/python -B -m pytest -q -p no:cacheprovider relationship-pass/v3/scripts/test_authors_v3.py
"""
import sys
from pathlib import Path

import pytest

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
import authors_v3 as av  # noqa: E402

CASES = [
    # --- pass-2 negative cases (never equal)
    ("J.E. Lange", "M. Lange", "different"),
    ("Sowerby: Fr.", "Vahl: Fr.", "different"),
    ("Vahl: Fr.", "Fr.", "role_mismatch"),
    ("(Sowerby: Fr.) W. Phillips", "(Fr.) W. Phillips", "role_mismatch"),
    ("(Nees: Fr.) Ces. & De Not.", "(Nees ex Fr.) Ces. & De Not.", "role_mismatch"),        # Cucurbitaria spartii
    ("(Alb. & Schwein.: Fr.) Nannf.", "(Alb. & Schwein. ex Fr.) Nannf.", "role_mismatch"),  # Pyrenopeziza petiolaris
    ("(Ach.) Grognot", "(Ach.) O.J. Rich.", "different"),
    ("Dicks.", "O.F. Müll.", "different"),
    ("Navaeu", "Naveau", "different"),
    ("Niessl ex Rehm", "Niessl", "role_mismatch"),
    ("J. Lange", "M. Lange", "different"),
    # --- review-codex finding 2: roles moving between citation parts
    ("(Pers.: Fr.) Fr.", "Pers. ex Fr.", "role_mismatch"),          # Gyromitra esculenta 79004/3HZQ4 pattern
    ("(Bull.: Fr.) Lam.", "Bull. ex Lam.", "role_mismatch"),         # Amanita verna 52179/5TYR8 pattern
    ("(Pers. : Fr.) Fr.", "Pers. ex Fr.", "role_mismatch"),
    ("(Bull.) Lam.", "Bull. ex Lam.", "role_mismatch"),              # basionym author vs pre-ex author
    ("Fr. : Fr.", "(Fr.) Fr.", "role_mismatch"),                     # Fr. as sanctioning author vs as basionym author
    # --- review-codex finding 3: initials are not particles; particles are kept
    ("D. Hawksw.", "J. Hawksw.", "different"),
    ("D. Hawksw.", "Hawksw.", "candidate"),                          # not identical any more
    ("de Vries", "van Vries", "different"),
    ("des Abb.", "Abbayes", "candidate"),
    ("de Vries", "Vries", "candidate"),
    ("de Vries", "de Vries", "identical"),
    ("D. de Vries", "J. de Vries", "different"),
    # --- review-codex finding 4: 'in' and 'f.'
    ("Pike in Hale", "Pike", "compatible"),                          # Hypogymnia rugosa 71787/PLQNZ
    ("Pike in Hale", "Hale", "role_mismatch"),
    ("Pike in Hale", "Pike in Hale", "identical"),
    ("Hook. f.", "Hook.", "different"),                              # filius is a different person
    ("Hook.f.", "Hook. f.", "identical"),
    ("(Fr.) Kickx f.", "(Fr.) Kickx", "different"),
    ("(Fr.) Kickx f.", "(Fr.) J. Kickx f.", "candidate"),
    # --- review-codex finding 5: verified's exception kinds are labelled
    ("Hepp", "Nägeli ex Hepp", "candidate"),                         # Caloplaca cerina witness: pre_ex_omitted
    ("Maasen ex Claussen", "Claussen", "candidate"),
    # --- fail closed: missing and unparsable authorship are never evidence
    ("", "Fr.", "missing"),
    ("", "", "missing"),
    ("(Fr. : Fr.) Fr. ss. M.M. Moser", "(Fr.) Fr.", "unparsed"),
    ("Fr. 18", "Fr.", "unparsed"),
    ("(Delise) Vainio D. Galloway", "(Delise) Vain.", "unparsed"),     # missing '&': fail closed
    ("(Bull. : Fr.) P. Kumm. S. lato", "(Bull.) P. Kumm.", "unparsed"),
    ("Vainio (fide Tucker)", "Vain.", "unparsed"),
    ("auct. non Fr.", "Fr.", "unparsed"),
    ("((Roberge ex Desm.) J. Schröt.", "(Roberge ex Desm.) J. Schröt.", "unparsed"),
    ("Fr.) Kumm.", "(Fr.) Kumm.", "unparsed"),
    ("Fr. & ", "Fr.", "unparsed"),
    ("Fr. ?", "Fr.", "unparsed"),
    ("A ex B ex C", "C", "unparsed"),
    # --- candidates (not evidence)
    ("Fr.", "Th. Fr.", "candidate"),
    ("Heidmarsson", "Heiðm.", "candidate"),
    ("(Ach.) Schaerer", "(Ach.) Schaer.", "candidate"),
    ("(Tuck.) W. Weber.", "(Tuck.) W.A. Weber", "candidate"),
    ("Weber", "W.A. Weber", "candidate"),
    ("(Lib.) Fryday et al.", "(Lib.) Fryday, Printzen & S. Ekman", "candidate"),
    ("Fr.", "(Fr.) Fr.", "candidate"),
    # --- compatible / identical
    ("(Pers. : Fr.) Fr.", "(Pers.) Fr.", "compatible"),
    ("(Th. Fr.) Zahlbr.", "(Th.Fr.) Zahlbr.", "identical"),
    ("Heiðm.", "Heidm.", "identical"),
    ("(Naveau) Kühner & Romagn.", "(Naveau) Kuhner & Romagn.", "identical"),
    ("Maaßen ex Claussen", "Maassen ex Claussen", "identical"),
    ("Ces. & De Not.", "Ces. & de Not.", "identical"),
    # typography only
    ("(Hue) Darb.", "(Hue) Darb., 1923", "identical"),
    ("Fr.", "Fr. 1821", "identical"),
    ("(Ach.) Del.", "(Ach.) Delise", "candidate"),
    ("E. Ludw. & Karlsson", "E. Ludw. & A.-S. Karlsson", "candidate"),
    ("V. Kučera, Tomšovský\u00a0& Lizoň", "V. Kučera, Tomšovský & Lizoň", "identical"),
    ("Coppins & Aptroot,", "Coppins & Aptroot", "identical"),
    ("Kaz. Tanaka, Raja, & Shearer", "Kaz. Tanaka, Raja & Shearer", "identical"),
    ("Fr. : Fr..", "Fr. : Fr.", "identical"),
    ("(Ach.) S.L.F. Meyer", "(Ach.) S.L.F. Mey.", "candidate"),
    ("(W. Gams) Crous]", "(W. Gams) Crous", "unparsed"),
]


@pytest.mark.parametrize("a,b,want", CASES)
def test_compare(a, b, want):
    got = av.compare(a, b)
    assert av.level(got) == want, (a, b, got)
    assert av.level(av.compare(b, a)) == want, (b, a)


@pytest.mark.parametrize("a,b,reason", [
    ("Hepp", "Nägeli ex Hepp", "pre_ex_omitted"),
    ("D. Hawksw.", "Hawksw.", "initials_one_side"),
    ("des Abb.", "Abbayes", "particle_one_side"),
    ("(Ach.) Schaerer", "(Ach.) Schaer.", "surname_abbreviation"),
    ("Pike in Hale", "Pike", "in_host_omitted"),
    ("(Pers. : Fr.) Fr.", "(Pers.) Fr.", "sanctioning_omitted"),
    ("Fr.", "(Fr.) Fr.", "basionym_part_one_side"),
    ("(Ahti)", "(Ahti) Ahti & DePriest", "combination_part_one_side"),
])
def test_reason_labels(a, b, reason):
    assert reason in av.reasons(av.compare(a, b)), av.compare(a, b)


def test_initial_D_is_not_a_particle():
    p = av.parse_part("D. Hawksw.")["valid"][0][0]
    assert p["initials"] == ("d",) and p["particles"] == () and p["surname"] == "hawksw"


def test_filius_is_a_flag():
    p = av.parse_part("Hook. f.")["valid"][0][0]
    assert p["surname"] == "hook" and p["filius"] is True


@pytest.mark.parametrize("a,b,want", [
    # review-codex finding 1: homotypic names must share the original author
    ("(Pers.) Bres.", "Bres.", "different"),                        # Sebacina grisea 57952 vs witness 62018 pattern
    ("(Sacc.) Wehm.", "Sacc. & Briard", "different"),               # Cryptodiaporthe populea basionym 37GG3 pattern
    ("(Fr.) P. Kumm.", "Fr. : Fr.", "compatible"),
    ("(Naveau) Kühner & Romagn.", "Naveau", "identical"),
    ("Fr.", "", "missing"),
])
def test_original_author(a, b, want):
    assert av.level(av.original_cmp(a, b)) == want


# --- pass-3 review (../review-codex.md) findings 2 and 3: focused negative tests
@pytest.mark.parametrize("a,b,want", [
    # role moves are detected even when the preliminary level is compatible
    ("Pike in Hale", "Pike: Hale", "role_mismatch"),
    ("(A in B) C", "(A: B) C", "role_mismatch"),
    ("(Pers. : Fr.) Fr.", "(Pers.) Fr.", "compatible"),          # still compatible: no role moves
    ("Pike in Hale", "Pike", "compatible"),
    # residual Unicode letters are kept, never deleted
    ("Łind.", "Ind.", "different"),
    ("Þor", "Or", "different"),
    ("Halıcı", "Halici", "different"),
    ("Łubek", "Lubek", "identical"),                             # documented ł→l map
    ("Arup et al.", "Arup, Halıcı & Vondrák", "candidate"),      # 173281/75SJH
    ("Guzow-Krzeminska et al.", "Guz.-Krzem., Łubek, Malíček & Kukwa", "different"),  # 172709/3ST32
    # malformed 'et al.': unparsed, even against itself
    ("Smith et al. Jones", "Smith et al. Jones", "unparsed"),
    ("Smith et al. Jones", "Smith & Jones", "unparsed"),
    ("Smith et al. et al.", "Smith et al.", "unparsed"),
    # multiword surname tokens: identical only on the exact person key, else unparsed
    ("Smith John", "Smith John", "identical"),
    ("Smith John", "John", "unparsed"),
    ("Smith John", "J. Smith", "unparsed"),
    ("Willi Krieg.", "Willi Krieg.", "identical"),
    ("Willi Krieg.", "Krieg.", "unparsed"),
    ("Willi Krieg.", "W. Krieg.", "unparsed"),
    ("(Rehm) San Martin & J.D. Rogers", "(Rehm) F. San Martín & J.D. Rogers", "unparsed"),
    ("Müll.Arg.", "Müll. Arg.", "identical"),
    ("(Müll.Arg) Vezda", "(Müll. Arg.) Vězda", "identical"),
    ("Müll. Arg.", "Arg.", "unparsed"),
    ("Müll.Arg.", "Hale", "different"),                          # no shared word: still different
    ("(Müll. Arg.) P.M. Jørg", "(Pers.) P.M. Jørg.", "different"),
    ("Smith John et al.", "Smith Jon, A. Berg & C. Dahl", "unparsed"),  # et al. never rescues an unparsed person
    ("Smith John et al.", "Smith John, A. Berg & C. Dahl", "candidate"),
])
def test_pass3_review_negatives(a, b, want):
    assert av.level(av.compare(a, b)) == want, (a, b, av.compare(a, b))
    assert av.level(av.compare(b, a)) == want, (b, a, av.compare(b, a))


def test_basionym_cmp_detects_role_move_when_compatible():
    assert av.basionym_cmp("(A in B) C", "(A: B) C") == "role_mismatch"
