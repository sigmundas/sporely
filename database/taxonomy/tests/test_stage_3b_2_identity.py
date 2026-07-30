"""Stage 3B.2 identity-independence tests.

Cover the DB-level contracts that Stage 3B.2 requires. UI wiring tests live
in ``tests/`` where Qt fixtures exist.

Contracts under test:

* ``list_vernacular_alternatives(sporely_id)`` returns strictly the rows for
  that Sporely id — never anything from a different id that happens to share
  a scientific name.
* ``taxon_id_from_scientific`` refuses to bind when multiple Sporely ids
  share the (genus, species). ``taxon_ids_from_scientific`` enumerates
  the ambiguous set so callers can present a chooser.
* Display capitalization is a pure read-side operation.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _seed(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE taxon_min (
            taxon_id INTEGER PRIMARY KEY,
            genus TEXT, specific_epithet TEXT, family TEXT,
            canonical_scientific_name TEXT
        );
        CREATE TABLE scientific_name_min (
            scientific_name_id INTEGER PRIMARY KEY AUTOINCREMENT,
            taxon_id INTEGER NOT NULL,
            language_code TEXT NOT NULL,
            scientific_name TEXT NOT NULL,
            is_preferred_name INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE vernacular_min (
            vernacular_id INTEGER PRIMARY KEY AUTOINCREMENT,
            taxon_id INTEGER NOT NULL,
            language_code TEXT NOT NULL,
            vernacular_name TEXT NOT NULL,
            is_preferred_name INTEGER NOT NULL DEFAULT 0,
            source TEXT
        );
        CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT);
        """
    )
    conn.executemany("INSERT INTO taxon_min VALUES (?, ?, ?, ?, ?)", [
        (133345, "Candolleomyces", "candolleanus", "Psathyrellaceae",
         "Candolleomyces candolleanus"),
        (103805, "Laccaria", "laccata", "Hydnangiaceae", "Laccaria laccata"),
        (625355, "Laccaria", "laccata", "Hydnangiaceae", "Laccaria laccata"),
    ])
    conn.executemany(
        "INSERT INTO scientific_name_min (taxon_id, language_code, "
        "scientific_name, is_preferred_name) VALUES (?, ?, ?, ?)",
        [
            (133345, "sci", "Candolleomyces candolleanus", 1),
            (103805, "sci", "Laccaria laccata", 1),
            (625355, "sci", "Laccaria laccata", 1),
        ],
    )
    conn.executemany(
        "INSERT INTO vernacular_min (taxon_id, language_code, "
        "vernacular_name, is_preferred_name, source) VALUES (?, ?, ?, ?, ?)",
        [
            (133345, "nb", "hvit sprøsopp", 1, "nortaxa"),
            (133345, "nn", "kvit sprøsopp", 1, "nortaxa"),
            (625355, "nb", "lakssopp", 1, "nortaxa"),
            (625355, "nn", "lakssopp", 1, "nortaxa"),
            (625355, "en", "deceiver", 1, "inat_csv"),
        ],
    )
    conn.executemany("INSERT INTO taxonomy_meta VALUES (?, ?)", [
        ("taxonomy_schema_version", "2"),
        ("state", "candidate"),
    ])
    conn.commit()
    conn.close()


def test_alternatives_query_strictly_by_sporely_id(tmp_path: Path) -> None:
    """The observation editor must load alternatives strictly by the
    already-known ``sporely_taxon_id`` — never by (genus, species)."""
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    db = VernacularDB(src)
    # Sporely 103805 has zero vernaculars — the alternatives query MUST
    # return an empty list even though a same-named taxon 625355 has 14.
    assert db.list_vernacular_alternatives(103805) == []
    # Sporely 625355 returns only its own rows, not 103805's absence.
    ids_625355 = {r["language_code"] for r in db.list_vernacular_alternatives(625355)}
    assert ids_625355 == {"nb", "nn", "en"}


def test_two_laccaria_ids_are_never_collapsed(tmp_path: Path) -> None:
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    db = VernacularDB(src)
    both = db.taxon_ids_from_scientific("Laccaria", "laccata")
    assert both == [103805, 625355]
    # `taxon_id_from_scientific` refuses to bind either one.
    assert db.taxon_id_from_scientific("Laccaria", "laccata") is None


def test_unique_scientific_name_still_resolves(tmp_path: Path) -> None:
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    db = VernacularDB(src)
    assert db.taxon_id_from_scientific("Candolleomyces", "candolleanus") == 133345


def test_alternatives_query_ignores_other_taxa_with_same_scientific_name(tmp_path: Path) -> None:
    """Given a known Sporely id, the alternatives list must be a strict
    partition — no vernacular row from another Sporely id leaks in even
    if the two share a canonical scientific name."""
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    db = VernacularDB(src)
    # 133345 (Candolleomyces) has 2 vernaculars; 625355 (Laccaria) has 3;
    # they never mix even though we ask for the same set of language codes.
    names_133345 = {r["vernacular_name"] for r in db.list_vernacular_alternatives(133345)}
    names_625355 = {r["vernacular_name"] for r in db.list_vernacular_alternatives(625355)}
    assert names_133345 == {"hvit sprøsopp", "kvit sprøsopp"}
    assert names_625355 == {"lakssopp", "deceiver"}
    assert not (names_133345 & names_625355)


def test_capitalization_is_pure_display(tmp_path: Path) -> None:
    from utils.vernacular_utils import display_vernacular_name
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    db = VernacularDB(src)
    # Capitalize a couple of names.
    assert display_vernacular_name("hvit sprøsopp") == "Hvit sprøsopp"
    assert display_vernacular_name("lakssopp") == "Lakssopp"
    # Underlying DB rows unchanged.
    with sqlite3.connect(str(src)) as conn:
        rows = {r[0] for r in conn.execute(
            "SELECT vernacular_name FROM vernacular_min "
            "WHERE taxon_id IN (133345, 625355)")}
    assert "hvit sprøsopp" in rows
    assert "lakssopp" in rows
    # Not capitalized in storage.
    assert "Hvit sprøsopp" not in rows
    assert "Lakssopp" not in rows
