"""Script backups include changes that still sit in the WAL sidecar."""

import sqlite3

from database import migrate


def test_snapshot_includes_uncheckpointed_wal_changes(tmp_path):
    source = tmp_path / "mushrooms.db"
    backup = tmp_path / "mushrooms.db.backup"
    live = sqlite3.connect(source)
    try:
        live.execute("PRAGMA journal_mode=WAL")
        live.execute("PRAGMA wal_autocheckpoint=0")
        live.execute("CREATE TABLE t (v TEXT)")
        live.execute("INSERT INTO t VALUES ('committed in wal')")
        live.commit()
        assert (tmp_path / "mushrooms.db-wal").stat().st_size > 0

        migrate.snapshot_sqlite_database(source, backup)
    finally:
        live.close()

    copy = sqlite3.connect(backup)
    try:
        assert copy.execute("SELECT v FROM t").fetchall() == [("committed in wal",)]
        assert copy.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    finally:
        copy.close()
