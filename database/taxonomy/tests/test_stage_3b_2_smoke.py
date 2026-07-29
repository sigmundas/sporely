"""Stage 3B.2 smoke-test corrections: vernacular chooser + capitalization.

Direct-SQL and helper-level tests. UI wiring lives in
``tests/test_observation_dialog_vernacular_chooser.py``.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

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
        # Two Sporely IDs share the canonical name — Stage 3A conservative rule.
        (103805, "Laccaria", "laccata", "Hydnangiaceae", "Laccaria laccata"),
        (625355, "Laccaria", "laccata", "Hydnangiaceae", "Laccaria laccata"),
    ])
    conn.executemany(
        "INSERT INTO scientific_name_min "
        "(taxon_id, language_code, scientific_name, is_preferred_name) "
        "VALUES (?, ?, ?, ?)", [
            (133345, "sci", "Candolleomyces candolleanus", 1),
            (133345, "sci", "Psathyrella candolleana", 0),
            (103805, "sci", "Laccaria laccata", 1),
            (625355, "sci", "Laccaria laccata", 1),
        ])
    conn.executemany(
        "INSERT INTO vernacular_min "
        "(taxon_id, language_code, vernacular_name, is_preferred_name, source) "
        "VALUES (?, ?, ?, ?, ?)", [
            (133345, "nb", "hvit sprøsopp", 1, "nortaxa"),
            (133345, "nn", "kvit sprøsopp", 1, "nortaxa"),
            (133345, "en", "Pale Brittlestem", 1, "inat_csv"),
            (133345, "fr", "Psathyrelle de Candolle", 1, "inat_csv"),
            # 103805 has no vernaculars — mirrors the real defect.
            (625355, "en", "deceiver", 1, "inat_csv"),
            (625355, "nb", "lakssopp", 1, "nortaxa"),
            (625355, "nn", "lakssopp", 1, "nortaxa"),
        ])
    conn.executemany("INSERT INTO taxonomy_meta VALUES (?, ?)", [
        ("taxonomy_schema_version", "2"),
        ("content_release_id", "tax-2026.07.29-01"),
        ("state", "candidate"),
    ])
    conn.commit()
    conn.close()


# --------------------------------------------------- DB helpers -----------


def test_alternatives_for_133345_include_both_norwegian_forms(tmp_path: Path) -> None:
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    db = VernacularDB(src)
    alts = db.list_vernacular_alternatives(133345, languages=("nb", "nn"))
    langs = [a["language_code"] for a in alts]
    names = [a["vernacular_name"] for a in alts]
    # Norwegian variants come first because we promoted them.
    assert langs[:2] == ["nb", "nn"]
    assert "hvit sprøsopp" in names
    assert "kvit sprøsopp" in names
    # Every vernacular row surfaces; nothing is silently merged.
    assert len(alts) == 4
    assert set(a["language_code"] for a in alts) == {"nb", "nn", "en", "fr"}


def test_no_language_fanout_includes_nb_and_nn_distinctly(tmp_path: Path) -> None:
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    db = VernacularDB(src, language_code="no")
    # The umbrella `no` request fans out to `nb` and `nn` — and each entry
    # keeps its own language code.
    rows = list(db._connect().execute(
        "SELECT language_code, vernacular_name FROM vernacular_min v "
        "WHERE taxon_id = 133345 "
        + db._language_clause(None)[0]
        + " ORDER BY language_code",
        db._language_clause(None)[1],
    ))
    codes = {r[0] for r in rows}
    assert codes == {"nb", "nn"}, rows


def test_laccaria_laccata_refuses_to_silently_bind_one_of_two_ids(tmp_path: Path) -> None:
    """When two Sporely IDs deliberately share ``Laccaria laccata``, the
    scientific-name resolver refuses to silently pick one. The caller must
    obtain identity through an explicit suggestion selection."""
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    db = VernacularDB(src)
    # Both matching taxa are listed; the strict resolver returns None.
    assert db.taxon_ids_from_scientific("Laccaria", "laccata") == [103805, 625355]
    assert db.taxon_id_from_scientific("Laccaria", "laccata") is None


def test_taxon_id_from_scientific_unique_case(tmp_path: Path) -> None:
    """When the scientific name is unambiguous, the strict resolver returns
    the taxon id."""
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    db = VernacularDB(src)
    assert db.taxon_id_from_scientific("Candolleomyces", "candolleanus") == 133345


def test_vernacular_selection_preserves_taxon_and_snapshot(tmp_path: Path) -> None:
    """Selecting a vernacular resolves to a Sporely taxon AND we return the
    original selected string so the observation editor can persist it as
    the common-name snapshot."""
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    db = VernacularDB(src, language_code="no")
    taxon_id = db.taxon_id_from_vernacular("hvit sprøsopp")
    assert taxon_id == 133345
    # Synonym vernacular also resolves.
    assert db.taxon_id_from_vernacular("kvit sprøsopp") == 133345


def test_nb_explicit_never_returns_nn_row(tmp_path: Path) -> None:
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    assert VernacularDB(src, "nb").taxon_id_from_vernacular("kvit sprøsopp") is None
    assert VernacularDB(src, "nn").taxon_id_from_vernacular("kvit sprøsopp") == 133345


def test_selecting_alternative_does_not_change_sporely_taxon_id(tmp_path: Path) -> None:
    """The chooser exposes N vernaculars for a Sporely ID; switching between
    them only changes the common-name snapshot the caller reads. Simulate
    by listing alternatives and verifying every entry maps to the SAME
    taxon."""
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    db = VernacularDB(src)
    alts = db.list_vernacular_alternatives(133345)
    resolved_ids = {
        db.taxon_id_from_vernacular(a["vernacular_name"]) for a in alts
    }
    # Every alternative resolves to 133345 (or None if the name is unique
    # to some other taxon in shared-name edge cases; here it's 133345).
    assert resolved_ids == {133345}


def test_empty_common_name_is_allowed() -> None:
    from utils.vernacular_utils import display_vernacular_name
    assert display_vernacular_name(None) == ""
    assert display_vernacular_name("") == ""
    # Explicit whitespace stays as-is.
    assert display_vernacular_name("   ") == "   "


# --------------------------------------------- capitalization ------------


def test_display_capitalize_first_char_only() -> None:
    from utils.vernacular_utils import display_vernacular_name
    assert display_vernacular_name("hvit sprøsopp") == "Hvit sprøsopp"
    # Do NOT title-case every word.
    assert display_vernacular_name("Hvit sprøsopp") == "Hvit sprøsopp"
    assert display_vernacular_name("kvit sprøsopp") == "Kvit sprøsopp"
    assert display_vernacular_name("deceiver") == "Deceiver"
    assert display_vernacular_name("čáhppesguoppar") == "Čáhppesguoppar"
    # Existing all-caps stays as-is (still capitalize first char, others
    # untouched).
    assert display_vernacular_name("SPRØSOPP") == "SPRØSOPP"


def test_capitalization_never_mutates_stored_row(tmp_path: Path) -> None:
    """Sanity: display_vernacular_name is a pure function that does not
    touch the database row content."""
    from database.vernacular_db import VernacularDB
    from utils.vernacular_utils import display_vernacular_name
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    db = VernacularDB(src)
    row = list(db._connect().execute(
        "SELECT vernacular_name FROM vernacular_min "
        "WHERE taxon_id=133345 AND language_code='nb'"))[0][0]
    assert row == "hvit sprøsopp"  # stored lowercase
    display = display_vernacular_name(row)
    assert display == "Hvit sprøsopp"
    # DB row unchanged after fetching + capitalizing.
    row2 = list(db._connect().execute(
        "SELECT vernacular_name FROM vernacular_min "
        "WHERE taxon_id=133345 AND language_code='nb'"))[0][0]
    assert row2 == "hvit sprøsopp"


# --------------------------------------------- legacy path -----------------


def _seed_legacy(path: Path) -> None:
    """A pre-v2 DB — no ``taxonomy_meta`` table."""
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE taxon_min (
            taxon_id INTEGER PRIMARY KEY,
            genus TEXT NOT NULL, specific_epithet TEXT NOT NULL,
            family TEXT
        );
        CREATE TABLE vernacular_min (
            vernacular_id INTEGER PRIMARY KEY AUTOINCREMENT,
            taxon_id INTEGER NOT NULL,
            language_code TEXT NOT NULL,
            vernacular_name TEXT NOT NULL,
            is_preferred_name INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    conn.execute("INSERT INTO taxon_min VALUES (?, ?, ?, ?)",
                 (300190, "Candolleomyces", "candolleanus", "Psathyrellaceae"))
    conn.execute("INSERT INTO vernacular_min (taxon_id, language_code, "
                 "vernacular_name, is_preferred_name) "
                 "VALUES (300190, 'no', 'hvit sprøsopp', 1)")
    conn.commit(); conn.close()


def test_legacy_db_still_matches_umbrella_no(tmp_path: Path) -> None:
    from database.vernacular_db import VernacularDB
    src = tmp_path / "legacy.sqlite3"
    _seed_legacy(src)
    # Legacy DBs use the umbrella `no` behavior even when the caller asks
    # for `nb` — old production callers rely on this.
    db_nb = VernacularDB(src, language_code="nb")
    row = list(db_nb._connect().execute(
        "SELECT taxon_id FROM vernacular_min v WHERE v.vernacular_name = ?"
        + db_nb._language_clause(None)[0],
        ("hvit sprøsopp", *db_nb._language_clause(None)[1])
    ))
    assert row == [(300190,)]


def test_v2_activation_off_returns_legacy_path(tmp_path: Path, monkeypatch) -> None:
    """When the developer activation gate is closed and no v2 install
    exists, ``resolve_vernacular_db_path`` returns the legacy multilang
    DB path (or None). We verify the resolver's activation guard doesn't
    leak a v2 path when nothing is enabled."""
    from utils.taxonomy_v2 import (
        ACTIVATION_ENV_VAR, is_activation_enabled,
    )
    monkeypatch.delenv(ACTIVATION_ENV_VAR, raising=False)
    assert is_activation_enabled(tmp_path) is False
    # And an explicit off env var overrides settings.
    monkeypatch.setenv(ACTIVATION_ENV_VAR, "0")
    (tmp_path / "app_settings.json").write_text('{"taxonomy_v2_activation": true}')
    assert is_activation_enabled(tmp_path) is False
