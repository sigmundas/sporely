from __future__ import annotations

import sqlite3

import pytest

from database import schema


def test_explicit_main_database_requires_explicit_reference_database(tmp_path):
    staged_main = tmp_path / "staged" / "mushrooms.db"

    with pytest.raises(ValueError, match="reference_path"):
        schema.init_database(db_path=staged_main, run_model_backfills=False)

    assert not staged_main.exists()


def test_explicit_reference_database_requires_explicit_main_database(tmp_path):
    staged_reference = tmp_path / "staged" / "reference_values.db"

    with pytest.raises(ValueError, match="db_path"):
        schema.init_database(
            reference_path=staged_reference,
            run_model_backfills=False,
        )

    assert not staged_reference.exists()


def test_portable_schema_helper_owns_provenance_and_pending_identity(tmp_path):
    database = tmp_path / "destination.db"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE observations (id INTEGER PRIMARY KEY, date TEXT NOT NULL)"
        )

        schema.ensure_portable_import_schema(connection)

        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(observations)")
        }
        assert "portable_cloud_identity_pending" in columns
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='portable_import_provenance'"
        ).fetchone() == (1,)


def test_staged_init_runs_sql_migrations_without_model_backfills(
    tmp_path, monkeypatch
):
    staged_main = tmp_path / "staged" / "mushrooms.db"
    staged_reference = tmp_path / "staged" / "reference_values.db"

    from database.models import CalibrationAssetDB

    calls = 0

    def fail_if_called():
        nonlocal calls
        calls += 1

    monkeypatch.setattr(CalibrationAssetDB, "backfill_all", fail_if_called)

    schema.init_database(
        db_path=staged_main,
        reference_path=staged_reference,
        run_model_backfills=False,
    )

    assert calls == 0
    with sqlite3.connect(staged_main) as connection:
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(observations)")
        }
        assert "portable_cloud_identity_pending" in columns
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='portable_import_provenance'"
        ).fetchone() == (1,)
    with sqlite3.connect(staged_reference) as connection:
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='reference_values'"
        ).fetchone() == (1,)
