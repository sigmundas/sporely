"""Tests for the pass-2 comparator, including review-codex negative cases.

  <venv>/bin/python -B -m pytest -q -p no:cacheprovider relationship-pass/v2/scripts/test_authors_v2.py
"""
import sys
from pathlib import Path

import pytest

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
import authors_v2 as av  # noqa: E402

CASES = [
    # --- negative: never equal (review-codex finding 4)
    ("J.E. Lange", "M. Lange", "different"),
    ("Sowerby: Fr.", "Vahl: Fr.", "different"),
    ("Vahl: Fr.", "Fr.", "role_mismatch"),        # never equal: sanctioning author cited as author
    ("(Sowerby: Fr.) W. Phillips", "(Fr.) W. Phillips", "role_mismatch"),
    ("(Nees: Fr.) Ces. & De Not.", "(Nees ex Fr.) Ces. & De Not.", "role_mismatch"),        # Cucurbitaria spartii
    ("(Alb. & Schwein.: Fr.) Nannf.", "(Alb. & Schwein. ex Fr.) Nannf.", "role_mismatch"),  # Pyrenopeziza petiolaris
    ("Fr.", "Th. Fr.", "candidate"),
    ("(Ach.) Grognot", "(Ach.) O.J. Rich.", "different"),
    ("Dicks.", "O.F. Müll.", "different"),
    ("Navaeu", "Naveau", "different"),
    ("Niessl ex Rehm", "Niessl", "role_mismatch"),
    ("(Fr. : Fr.) Fr. ss. M.M. Moser", "(Fr.) Fr.", "different"),
    # --- not evidence (candidate)
    ("Heidmarsson", "Heiðm.", "candidate"),
    ("des Abb.", "Abbayes", "candidate"),
    ("(Ach.) Schaerer", "(Ach.) Schaer.", "candidate"),
    ("(Tuck.) W. Weber.", "(Tuck.) W.A. Weber", "candidate"),
    ("J. Lange", "M. Lange", "different"),
    ("Weber", "W.A. Weber", "candidate"),
    ("(Lib.) Fryday et al.", "(Lib.) Fryday, Printzen & S. Ekman", "candidate"),
    ("Maasen ex Claussen", "Claussen", "candidate"),
    # --- compatible / identical
    ("(Pers. : Fr.) Fr.", "(Pers.) Fr.", "compatible"),
    ("(Th. Fr.) Zahlbr.", "(Th.Fr.) Zahlbr.", "identical"),
    ("Heiðm.", "Heidm.", "identical"),
    ("(Naveau) Kühner & Romagn.", "(Naveau) Kuhner & Romagn.", "identical"),
    ("Maaßen ex Claussen", "Maassen ex Claussen", "identical"),
]


@pytest.mark.parametrize("a,b,want", CASES)
def test_compare(a, b, want):
    got = av.compare(a, b)
    assert got.split(":")[0] == want, (a, b, got)
    assert av.compare(b, a).split(":")[0] == want, (b, a)
