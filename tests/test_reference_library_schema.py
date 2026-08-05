"""Schema/migration tests for the normalized reference library."""
from __future__ import annotations

import sqlite3

import pytest

from database import schema as _schema
from database import reference_library_schema as _lib_schema


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }


def _isolated_db_paths(tmp_path, monkeypatch):
    db_path = tmp_path / "mushrooms.db"
    ref_path = tmp_path / "reference_values.db"
    monkeypatch.setattr(_schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(_schema, "get_reference_database_path", lambda: ref_path)
    monkeypatch.setattr(_schema, "get_bundled_reference_database_path", lambda: tmp_path / "does_not_exist.db")
    return db_path, ref_path


def test_fresh_reference_db_contains_new_tables(tmp_path, monkeypatch):
    _, ref_path = _isolated_db_paths(tmp_path, monkeypatch)
    _schema.init_reference_database(ref_path, seed_from_bundle=False, migrate_legacy=False)
    conn = sqlite3.connect(ref_path)
    try:
        tables = _tables(conn)
        for expected in (
            "reference_values",
            "reference_works",
            "reference_taxon_treatments",
            "reference_measurement_sets",
        ):
            assert expected in tables, f"missing {expected}"
    finally:
        conn.close()


def test_fresh_main_db_contains_observation_reference_uses(tmp_path, monkeypatch):
    db_path, _ = _isolated_db_paths(tmp_path, monkeypatch)
    _schema.init_database()
    conn = sqlite3.connect(db_path)
    try:
        tables = _tables(conn)
        assert "observation_reference_uses" in tables
        cols = _table_columns(conn, "observation_reference_uses")
        for expected in (
            "id",
            "observation_id",
            "reference_measurement_set_id",
            "role",
            "note",
            "selected_at",
            "reference_revision",
            "snapshot_json",
            "created_at",
            "updated_at",
        ):
            assert expected in cols
    finally:
        conn.close()


def test_initialization_is_idempotent(tmp_path, monkeypatch):
    _, ref_path = _isolated_db_paths(tmp_path, monkeypatch)
    _schema.init_reference_database(ref_path, seed_from_bundle=False, migrate_legacy=False)
    _schema.init_reference_database(ref_path, seed_from_bundle=False, migrate_legacy=False)
    # Also idempotent inline.
    conn = sqlite3.connect(ref_path)
    try:
        _lib_schema.init_reference_library_schema(conn)
        _lib_schema.init_reference_library_schema(conn)
    finally:
        conn.close()


def test_legacy_reference_rows_are_preserved_on_upgrade(tmp_path, monkeypatch):
    _, ref_path = _isolated_db_paths(tmp_path, monkeypatch)
    # Simulate a pre-existing legacy database with a row present.
    _schema.init_reference_database(ref_path, seed_from_bundle=False, migrate_legacy=False)
    conn = sqlite3.connect(ref_path)
    try:
        conn.execute(
            """
            INSERT INTO reference_values (genus, species, source, length_min, length_max)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("Russula", "paludosa", "Petersen 1990", 7.5, 10.5),
        )
        conn.commit()
    finally:
        conn.close()

    # Upgrade: re-run init. New tables should exist and legacy row remain.
    _schema.init_reference_database(ref_path, seed_from_bundle=False, migrate_legacy=False)
    conn = sqlite3.connect(ref_path)
    try:
        row = conn.execute(
            "SELECT genus, species, source, length_min, length_max FROM reference_values"
        ).fetchone()
        assert row == ("Russula", "paludosa", "Petersen 1990", 7.5, 10.5)
        tables = _tables(conn)
        assert "reference_works" in tables
    finally:
        conn.close()


def test_representative_legacy_observation_db_upgrades_without_data_loss(
    tmp_path, monkeypatch
):
    db_path, _ = _isolated_db_paths(tmp_path, monkeypatch)

    # First initialization creates the observations table.
    _schema.init_database()
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (date, location) VALUES (?, ?)",
            ("2025-01-01", "Test location"),
        )
        conn.commit()
    finally:
        conn.close()

    # Simulate a re-run of init (upgrade); observation should remain.
    _schema.init_database()
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT date, location FROM observations").fetchall()
        assert rows == [("2025-01-01", "Test location")]
        assert "observation_reference_uses" in _tables(conn)
    finally:
        conn.close()


def test_observation_reference_uses_has_unique_pair_index(tmp_path, monkeypatch):
    db_path, _ = _isolated_db_paths(tmp_path, monkeypatch)
    _schema.init_database()
    conn = sqlite3.connect(db_path)
    try:
        indexes = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='observation_reference_uses'"
            ).fetchall()
        }
        assert "idx_observation_reference_uses_observation_set_unique" in indexes
    finally:
        conn.close()
