"""Stage 3B.3 — ObservationDB persistence round-trip for the two new
snapshot columns. Confirms:
- create_observation accepts and stores the pair;
- update_observation can null them out explicitly;
- unknown rank strings are rejected at the model layer (whitelist);
- unknown ranks appear as NULL, not as the invalid string.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _fresh_db(tmp_path, monkeypatch):
    from pathlib import Path as _P
    db_path = _P(tmp_path) / "obs.sqlite3"
    from database import schema as _schema
    monkeypatch.setattr(_schema, "get_database_path", lambda: db_path)
    _schema.init_database()
    from database.models import ObservationDB
    return ObservationDB, db_path


def _row(db_path: Path, obs_id: int) -> dict:
    conn = sqlite3.connect(str(db_path)); conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT scientific_name_snapshot, taxon_rank_snapshot, sporely_taxon_id, "
        "genus, species, common_name "
        "FROM observations WHERE id = ?", (obs_id,)).fetchone()
    conn.close()
    return dict(row)


def test_create_and_read_back_snapshot(tmp_path, monkeypatch):
    db, path = _fresh_db(tmp_path, monkeypatch)
    obs_id = db.create_observation(
        date="2026-07-01 12:00",
        genus="Hygrocybe", species="conica",
        common_name="Witch's hat",
        scientific_name_snapshot="Hygrocybe conica var. pseudoconica",
        taxon_rank_snapshot="variety",
    )
    row = _row(path, obs_id)
    assert row["scientific_name_snapshot"] == "Hygrocybe conica var. pseudoconica"
    assert row["taxon_rank_snapshot"] == "variety"


def test_update_can_null_out_snapshot(tmp_path, monkeypatch):
    db, path = _fresh_db(tmp_path, monkeypatch)
    obs_id = db.create_observation(
        date="2026-07-01 12:00",
        genus="Hygrocybe", species="conica",
        scientific_name_snapshot="Hygrocybe conica agg.",
        taxon_rank_snapshot="aggregate",
    )
    db.update_observation(
        obs_id,
        scientific_name_snapshot=None,
        taxon_rank_snapshot=None,
        allow_nulls=True,
    )
    row = _row(path, obs_id)
    assert row["scientific_name_snapshot"] is None
    assert row["taxon_rank_snapshot"] is None
    # Structured genus/species preserved.
    assert row["genus"] == "Hygrocybe"
    assert row["species"] == "conica"


def test_invalid_rank_stored_as_null(tmp_path, monkeypatch):
    db, path = _fresh_db(tmp_path, monkeypatch)
    obs_id = db.create_observation(
        date="2026-07-01 12:00",
        genus="Hygrocybe", species="conica",
        scientific_name_snapshot="Hygrocybe conica sect. Foo",
        taxon_rank_snapshot="section",  # not on the whitelist
    )
    row = _row(path, obs_id)
    # Rank was rejected; snapshot text is still stored (verbatim text is fine),
    # but the constrained column is nulled.
    assert row["taxon_rank_snapshot"] is None


def test_empty_scientific_name_stored_as_null(tmp_path, monkeypatch):
    db, path = _fresh_db(tmp_path, monkeypatch)
    obs_id = db.create_observation(
        date="2026-07-01 12:00",
        genus="Hygrocybe", species="conica",
        scientific_name_snapshot="   ",
        taxon_rank_snapshot=None,
    )
    row = _row(path, obs_id)
    assert row["scientific_name_snapshot"] is None
