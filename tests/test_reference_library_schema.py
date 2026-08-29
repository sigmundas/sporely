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
            "reference_cloud_sync_state",
            "reference_cloud_tombstones",
            "reference_cloud_pull_cursors",
            "reference_cloud_remote_tombstone_markers",
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
        assert "observation_reference_use_cloud_sync_state" in tables
        assert "observation_reference_use_cloud_tombstones" in tables
        assert "observation_reference_use_cloud_pull_cursors" in tables
        assert "observation_reference_use_cloud_remote_tombstone_markers" in tables
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


def test_reference_sync_state_backfills_and_initialization_preserves_state(
    tmp_path, monkeypatch
):
    _, ref_path = _isolated_db_paths(tmp_path, monkeypatch)
    _schema.init_reference_database(ref_path, seed_from_bundle=False, migrate_legacy=False)
    with sqlite3.connect(ref_path) as conn:
        conn.execute(
            "INSERT INTO reference_works (id, type, title, short_label) "
            "VALUES ('work-1', 'book', 'Work', 'Work')"
        )
        conn.execute(
            "UPDATE reference_cloud_sync_state SET cloud_user_id='user-1', "
            "remote_identity_state='acknowledged', cloud_row_version=7, "
            "accepted_payload_json='{" + '"id":"work-1"' + "}', "
            "sync_status='clean' WHERE entity_type='work' AND entity_id='work-1'"
        )
        conn.commit()

    _schema.init_reference_database(ref_path, seed_from_bundle=False, migrate_legacy=False)
    _schema.init_reference_database(ref_path, seed_from_bundle=False, migrate_legacy=False)

    with sqlite3.connect(ref_path) as conn:
        row = conn.execute(
            "SELECT cloud_user_id, remote_identity_state, cloud_row_version, "
            "accepted_payload_json, sync_status FROM reference_cloud_sync_state "
            "WHERE entity_type='work' AND entity_id='work-1'"
        ).fetchone()
    assert row == ("user-1", "acknowledged", 7, '{"id":"work-1"}', "clean")


def test_observation_delete_captures_acknowledged_reference_use_before_cascade(
    tmp_path, monkeypatch
):
    db_path, _ = _isolated_db_paths(tmp_path, monkeypatch)
    _schema.init_database()
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(
            "INSERT INTO observations (id, date, cloud_id) "
            "VALUES (1, '2026-08-28', 'cloud-observation-1')"
        )
        conn.execute(
            "INSERT INTO observation_reference_uses "
            "(id, observation_id, reference_measurement_set_id, role, "
            "reference_revision, snapshot_json) "
            "VALUES ('use-1', 1, 'set-1', 'compared', 1, '{}')"
        )
        conn.execute(
            "UPDATE observation_reference_use_cloud_sync_state "
            "SET cloud_user_id='user-1', remote_identity_state='acknowledged', "
            "cloud_row_version=4, accepted_payload_json='{" + '"id":"use-1"' + "}', "
            "sync_status='clean' WHERE use_id='use-1'"
        )
        conn.execute("DELETE FROM observations WHERE id=1")
        conn.commit()

        assert conn.execute(
            "SELECT COUNT(*) FROM observation_reference_uses"
        ).fetchone()[0] == 0
        row = conn.execute(
            "SELECT use_id, reference_measurement_set_id, local_observation_id, "
            "observation_cloud_id, cloud_user_id, remote_identity_state, "
            "expected_row_version, accepted_payload_json "
            "FROM observation_reference_use_cloud_tombstones"
        ).fetchone()
    assert row == (
        "use-1",
        "set-1",
        1,
        "cloud-observation-1",
        "user-1",
        "acknowledged",
        4,
        '{"id":"use-1"}',
    )


@pytest.mark.parametrize("delete_parent", [False, True])
def test_remote_use_deletion_requires_verified_observation_cloud_id(
    tmp_path, monkeypatch, delete_parent
):
    db_path, _ = _isolated_db_paths(tmp_path, monkeypatch)
    _schema.init_database()
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(
            "INSERT INTO observations (id, date, cloud_id) "
            "VALUES (1, '2026-08-28', NULL)"
        )
        conn.execute(
            "INSERT INTO observation_reference_uses "
            "(id, observation_id, reference_measurement_set_id, role, "
            "reference_revision, snapshot_json) "
            "VALUES ('use-missing-parent', 1, 'set-1', 'compared', 1, '{}')"
        )
        conn.execute(
            "UPDATE observation_reference_use_cloud_sync_state "
            "SET cloud_user_id='user-1', remote_identity_state='acknowledged', "
            "cloud_row_version=4, accepted_payload_json='{}', sync_status='clean' "
            "WHERE use_id='use-missing-parent'"
        )
        conn.commit()

        table = "observations" if delete_parent else "observation_reference_uses"
        row_id = 1 if delete_parent else "use-missing-parent"
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(f'DELETE FROM "{table}" WHERE id=?', (row_id,))
        conn.rollback()

        assert conn.execute(
            "SELECT COUNT(*) FROM observations WHERE id=1"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM observation_reference_uses "
            "WHERE id='use-missing-parent'"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM observation_reference_use_cloud_tombstones"
        ).fetchone()[0] == 0


def test_never_attempted_reference_deletion_cancels_without_tombstone(
    tmp_path, monkeypatch
):
    _, ref_path = _isolated_db_paths(tmp_path, monkeypatch)
    _schema.init_reference_database(ref_path, seed_from_bundle=False, migrate_legacy=False)
    with sqlite3.connect(ref_path) as conn:
        conn.execute(
            "INSERT INTO reference_works (id, type, title, short_label) "
            "VALUES ('work-local', 'book', 'Local only', 'Local')"
        )
        conn.execute("DELETE FROM reference_works WHERE id='work-local'")
        conn.commit()
        assert conn.execute(
            "SELECT COUNT(*) FROM reference_cloud_sync_state "
            "WHERE entity_type='work' AND entity_id='work-local'"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM reference_cloud_tombstones "
            "WHERE entity_type='work' AND entity_id='work-local'"
        ).fetchone()[0] == 0


def test_existing_databases_backfill_reference_transport_state(
    tmp_path, monkeypatch
):
    db_path, ref_path = _isolated_db_paths(tmp_path, monkeypatch)
    _schema.init_database()
    with sqlite3.connect(ref_path) as conn:
        trigger_names = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' "
                "AND name LIKE 'reference_%_cloud_sync_%'"
            )
        ]
        for name in trigger_names:
            conn.execute(f'DROP TRIGGER "{name}"')
        conn.execute("DROP TABLE reference_cloud_tombstones")
        conn.execute("DROP TABLE reference_cloud_sync_state")
        conn.execute(
            "INSERT INTO reference_works (id, type, title, short_label) "
            "VALUES ('legacy-work', 'book', 'Legacy work', 'Legacy')"
        )
        conn.commit()
    with sqlite3.connect(db_path) as conn:
        trigger_names = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' "
                "AND name LIKE '%reference_use%cloud_sync%'"
            )
        ]
        for name in trigger_names:
            conn.execute(f'DROP TRIGGER "{name}"')
        conn.execute("DROP TABLE observation_reference_use_cloud_tombstones")
        conn.execute("DROP TABLE observation_reference_use_cloud_sync_state")
        conn.execute(
            "INSERT INTO observations (id, date) VALUES (1, '2026-08-28')"
        )
        conn.execute(
            "INSERT INTO observation_reference_uses "
            "(id, observation_id, reference_measurement_set_id, role, "
            "reference_revision, snapshot_json) "
            "VALUES ('legacy-use', 1, 'legacy-set', 'compared', 1, '{}')"
        )
        conn.commit()

    _schema.init_database()

    with sqlite3.connect(ref_path) as conn:
        assert conn.execute(
            "SELECT remote_identity_state FROM reference_cloud_sync_state "
            "WHERE entity_type='work' AND entity_id='legacy-work'"
        ).fetchone() == ("never_attempted",)
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT remote_identity_state "
            "FROM observation_reference_use_cloud_sync_state "
            "WHERE use_id='legacy-use'"
        ).fetchone() == ("never_attempted",)


def test_observation_delete_tombstone_rolls_back_with_parent_delete(
    tmp_path, monkeypatch
):
    db_path, _ = _isolated_db_paths(tmp_path, monkeypatch)
    _schema.init_database()
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(
            "INSERT INTO observations (id, date, cloud_id) "
            "VALUES (1, '2026-08-28', 'cloud-observation-1')"
        )
        conn.execute(
            "INSERT INTO observation_reference_uses "
            "(id, observation_id, reference_measurement_set_id, role, "
            "reference_revision, snapshot_json) "
            "VALUES ('use-rollback', 1, 'set-1', 'compared', 1, '{}')"
        )
        conn.execute(
            "UPDATE observation_reference_use_cloud_sync_state "
            "SET cloud_user_id='user-1', remote_identity_state='acknowledged', "
            "cloud_row_version=2, accepted_payload_json='{}', sync_status='clean' "
            "WHERE use_id='use-rollback'"
        )
        conn.commit()

        conn.execute("BEGIN")
        conn.execute("DELETE FROM observations WHERE id=1")
        assert conn.execute(
            "SELECT COUNT(*) FROM observation_reference_use_cloud_tombstones"
        ).fetchone()[0] == 1
        conn.rollback()

        assert conn.execute(
            "SELECT COUNT(*) FROM observations WHERE id=1"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM observation_reference_uses "
            "WHERE id='use-rollback'"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM observation_reference_use_cloud_tombstones"
        ).fetchone()[0] == 0


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
