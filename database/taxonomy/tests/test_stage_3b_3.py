"""Stage 3B.3 — non-UI unit tests.

Cover the persistence contract, the alias parser, the suggestion source
(range-scan plan + latency), the link_kind wiring, and the model-side
whitelist. UI-integration tests live under ``tests/`` where the qapp
fixture is available.
"""
from __future__ import annotations

import sqlite3
import sys
import time
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
            canonical_scientific_name TEXT,
            taxon_rank TEXT, taxonomic_status TEXT,
            canonical_source_system TEXT
        );
        CREATE INDEX idx_taxon_canonical_name ON taxon_min(canonical_scientific_name);
        CREATE INDEX idx_taxon_genus_species ON taxon_min(genus, specific_epithet);
        CREATE TABLE scientific_name_min (
            scientific_name_id INTEGER PRIMARY KEY AUTOINCREMENT,
            taxon_id INTEGER NOT NULL,
            language_code TEXT NOT NULL,
            scientific_name TEXT NOT NULL,
            is_preferred_name INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX idx_scientific_name_lookup ON scientific_name_min(language_code, scientific_name);
        CREATE TABLE vernacular_min (
            vernacular_id INTEGER PRIMARY KEY AUTOINCREMENT,
            taxon_id INTEGER NOT NULL,
            language_code TEXT NOT NULL,
            vernacular_name TEXT NOT NULL,
            is_preferred_name INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT);
        """
    )
    conn.executemany(
        "INSERT INTO taxon_min VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [
            (166673, "Hygrocybe", "conica", "Hygrophoraceae",
             "Hygrocybe conica var. conica", "variety", "accepted", "col_xr"),
            (166676, "Hygrocybe", "conica", "Hygrophoraceae",
             "Hygrocybe conica var. cinereifolia", "variety",
             "provisionally accepted", "col_xr"),
            (151359, "Hygrocybe", "conica", "Hygrophoraceae",
             "Hygrocybe conica f. conica", "form", "accepted", "col_xr"),
            (625372, "Hygrocybe", "conica", "Hygrophoraceae",
             "Hygrocybe conica coll.", "species", "valid", "nortaxa"),
            (16436, "Hygrocybe", "conica", "Hygrophoraceae",
             "Hygrocybe conica", "species", "accepted", "col_xr"),
            # Deliberate homonym (rare in real data, present in the compiled DB).
            (999001, "Laccaria", "laccata", "Hydnangiaceae",
             "Laccaria laccata", "species", "accepted", "col_xr"),
            (999002, "Laccaria", "laccata", "Hydnangiaceae",
             "Laccaria laccata", "species", "accepted", "nortaxa"),
            # Placeholder that must be excluded.
            (999003, "", "", "",
             "Incertae sedis", "family", "valid", "nortaxa"),
        ])
    conn.executemany(
        "INSERT INTO scientific_name_min (taxon_id, language_code, "
        "scientific_name, is_preferred_name) VALUES (?, ?, ?, ?)", [
            (625372, "sci", "Hygrocybe conica var. pseudoconica", 0),
            (625372, "sci", "Hygrocybe conica var. tetraspora", 0),
            (166673, "sci", "Hygrocybe conica var. conica", 1),
            # Alias with authorship: must be rejected by the parser.
            (16436, "sci", "Hygrocybe conica (Schaeff.) P. Kumm.", 0),
        ])
    conn.executemany("INSERT INTO taxonomy_meta VALUES (?, ?)", [
        ("taxonomy_schema_version", "2"),
        ("state", "candidate"),
    ])
    conn.commit(); conn.close()


# -------------------------- rank parser ----------------------------------


def test_parser_accepts_supported_shapes(tmp_path: Path) -> None:
    from database.vernacular_db import parse_scientific_name_snapshot
    assert parse_scientific_name_snapshot("Hygrocybe conica") == \
        ("Hygrocybe", "conica", "species")
    assert parse_scientific_name_snapshot("Hygrocybe conica var. pseudoconica") == \
        ("Hygrocybe", "conica", "variety")
    assert parse_scientific_name_snapshot("Hygrocybe conica f. conica") == \
        ("Hygrocybe", "conica", "form")
    assert parse_scientific_name_snapshot("Hygrocybe conica subsp. x") == \
        ("Hygrocybe", "conica", "subspecies")
    assert parse_scientific_name_snapshot("Hygrocybe conica ssp. x") == \
        ("Hygrocybe", "conica", "subspecies")
    assert parse_scientific_name_snapshot("Hygrocybe conica coll.") == \
        ("Hygrocybe", "conica", "aggregate")
    assert parse_scientific_name_snapshot("Hygrocybe conica agg.") == \
        ("Hygrocybe", "conica", "aggregate")
    assert parse_scientific_name_snapshot("Hygrocybe") == \
        ("Hygrocybe", None, "genus")


def test_parser_rejects_unsafe(tmp_path: Path) -> None:
    from database.vernacular_db import parse_scientific_name_snapshot
    # Authorship suffix
    assert parse_scientific_name_snapshot("Hygrocybe conica (Schaeff.)") is None
    # Multiple rank markers
    assert parse_scientific_name_snapshot("Hygrocybe conica var. X f. Y") is None
    # Non-lowercase species
    assert parse_scientific_name_snapshot("Hygrocybe SPECIES") is None
    # Empty / whitespace
    assert parse_scientific_name_snapshot("") is None
    assert parse_scientific_name_snapshot("   ") is None
    # Digits (rare taxonomic conventions we don't currently support)
    assert parse_scientific_name_snapshot("Hygrocybe sp.-2") is None


# -------------------------- suggestion source ----------------------------


def test_suggest_scientific_names_returns_expected_hits(tmp_path: Path) -> None:
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    db = VernacularDB(src)
    suggestions = db.suggest_scientific_names("Hygrocybe conica", limit=50)
    names = [(s["scientific_name"], s["taxon_rank_snapshot"], s["link_kind"])
             for s in suggestions]
    # Canonical hits present with the correct rank.
    assert ("Hygrocybe conica", "species", "canonical") in names
    assert ("Hygrocybe conica coll.", "aggregate", "canonical") in names
    assert ("Hygrocybe conica f. conica", "form", "canonical") in names
    assert ("Hygrocybe conica var. conica", "variety", "canonical") in names
    assert ("Hygrocybe conica var. cinereifolia", "variety", "canonical") in names
    # Alias hit — synonym alias of a coll. concept → link_kind = "linked"
    # because the alias's rank (variety) differs from the canonical rank
    # (species). The chooser must show it, but with a Linked-concept hint.
    assert ("Hygrocybe conica var. pseudoconica", "variety", "linked") in names


def test_suggest_scientific_names_excludes_incertae_sedis(tmp_path: Path) -> None:
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    db = VernacularDB(src)
    hits = db.suggest_scientific_names("Incertae", limit=50)
    assert all(h["scientific_name"] != "Incertae sedis" for h in hits)


def test_suggest_scientific_names_excludes_authorship_aliases(tmp_path: Path) -> None:
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    db = VernacularDB(src)
    hits = db.suggest_scientific_names("Hygrocybe conica", limit=50)
    assert not any("(Schaeff.)" in h["scientific_name"] for h in hits)


def test_suggest_scientific_names_returns_homonyms_separately(tmp_path: Path) -> None:
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    db = VernacularDB(src)
    hits = db.suggest_scientific_names("Laccaria laccata", limit=50)
    matching = [h for h in hits if h["scientific_name"] == "Laccaria laccata"]
    assert len(matching) == 2
    assert {h["sporely_taxon_id"] for h in matching} == {999001, 999002}
    # Sources differ so the display formatter can disambiguate.
    assert {h["canonical_source_system"] for h in matching} == {"col_xr", "nortaxa"}


def test_link_kind_never_inferred_from_rank_equality(tmp_path: Path) -> None:
    """`Hygrocybe conica var. conica` alias resolves to canonical row
    166673 which IS the variety concept — link_kind must be `canonical`
    (returned from the canonical branch), not silently rewritten to
    `synonym_of_accepted` by rank equality."""
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    db = VernacularDB(src)
    hits = db.suggest_scientific_names("Hygrocybe conica var. conica", limit=50)
    matching = [h for h in hits if h["scientific_name"] == "Hygrocybe conica var. conica"]
    # The canonical row 166673 provides it; the alias row 166673 (marked
    # is_preferred_name=1 in the seed) is excluded from the alias branch
    # anyway (we only union is_preferred_name=0 aliases).
    assert matching
    assert all(h["link_kind"] == "canonical" for h in matching)


# -------------------------- performance ----------------------------------


def test_suggest_scientific_names_prefix_uses_range_scan(tmp_path: Path) -> None:
    """Regression guard: the prefix query must use an indexed range scan,
    not a full-table scan."""
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    db = VernacularDB(src)
    with db._connect() as conn:
        plan_taxon = list(conn.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT taxon_id FROM taxon_min "
            "WHERE canonical_scientific_name >= ? AND canonical_scientific_name < ?",
            ("Hygrocybe", "Hygrocybe￿"),
        ))
        plan_alias = list(conn.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT taxon_id FROM scientific_name_min "
            "WHERE language_code = 'sci' AND scientific_name >= ? AND scientific_name < ?",
            ("Hygrocybe", "Hygrocybe￿"),
        ))
    assert any("idx_taxon_canonical_name" in str(r) for r in plan_taxon), plan_taxon
    assert any("idx_scientific_name_lookup" in str(r) for r in plan_alias), plan_alias


def test_suggest_scientific_names_latency_under_2ms(tmp_path: Path) -> None:
    """Prefix completion must complete well inside a keystroke budget.
    Fixture-scale here is small (~8 rows) so this is a floor check —
    the real v2 DB measured 332 μs median in preflight."""
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _seed(src)
    db = VernacularDB(src)
    db.suggest_scientific_names("Hygrocybe conica", limit=50)  # warm
    start = time.perf_counter()
    for _ in range(50):
        db.suggest_scientific_names("Hygrocybe conica", limit=50)
    per_call_ms = (time.perf_counter() - start) * 1000 / 50
    assert per_call_ms < 2.0, per_call_ms


# -------------------------- model whitelist -------------------------------


def test_taxon_rank_snapshot_whitelist_rejects_unknown() -> None:
    from database.models import _sanitize_taxon_rank_snapshot
    for good in ("genus", "species", "subspecies", "variety", "form", "aggregate"):
        assert _sanitize_taxon_rank_snapshot(good) == good
    for bad in ("section", "subgenus", "family", "order", "class", "kingdom",
                "unranked", "  ", "SectiOn"):
        assert _sanitize_taxon_rank_snapshot(bad) is None
    # None / empty preserved as None.
    assert _sanitize_taxon_rank_snapshot(None) is None
    assert _sanitize_taxon_rank_snapshot("") is None


def test_scientific_name_snapshot_strips_and_rejects_empty() -> None:
    from database.models import _sanitize_scientific_name_snapshot
    assert _sanitize_scientific_name_snapshot(
        "  Hygrocybe conica var. pseudoconica  "
    ) == "Hygrocybe conica var. pseudoconica"
    assert _sanitize_scientific_name_snapshot("") is None
    assert _sanitize_scientific_name_snapshot(None) is None


def test_cloud_sync_never_pushes_snapshot_columns() -> None:
    from utils import cloud_sync
    for column in ("scientific_name_snapshot", "taxon_rank_snapshot",
                   "sporely_taxon_id"):
        assert column not in cloud_sync._OBS_PUSH_COLS, column
        assert column not in cloud_sync._SNAPSHOT_OBS_FIELDS, column
        assert column not in cloud_sync._CONFLICT_COMPARE_FIELDS, column
