"""F07 (2026-11-26 security audit): a full restore must not install triggers
or views that the app itself does not create."""

from __future__ import annotations

import gc
import hashlib
import json
import sqlite3
from pathlib import Path
from zipfile import ZipFile

import pytest

from database import schema
from utils.archive.full_backup import create_full_backup
from utils.archive.full_restore import FullRestoreError, restore_full_backup
from utils.archive.manifest import ArchiveManifest


@pytest.fixture
def installation(monkeypatch, tmp_path):
    root = tmp_path / "installation"
    root.mkdir()
    monkeypatch.setattr(schema, "_app_dir", root)
    monkeypatch.setattr(schema, "DATABASE_PATH", root / "mushrooms.db")
    monkeypatch.setattr(schema, "REFERENCE_DATABASE_PATH", root / "reference_values.db")
    monkeypatch.setattr(schema, "SETTINGS_PATH", root / "app_settings.json")
    schema.init_database()
    return root


def _restore(archive: Path, tmp_path: Path):
    return restore_full_backup(
        archive,
        app_version="test",
        safety_backup_path=tmp_path / "safety.sporely",
        close_live=lambda: None,
        reopen_live=lambda: None,
    )


def _make_backup(tmp_path: Path) -> Path:
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("INSERT INTO observations (date, genus) VALUES ('2026-08-27', 'Archived')")
    archive = tmp_path / "source.sporely"
    create_full_backup(archive, app_version="test", qsettings_values={})
    return archive


def _tampered(archive: Path, tmp_path: Path, member: str, sql: str) -> Path:
    with ZipFile(archive) as source:
        manifest = ArchiveManifest.from_json(source.read("manifest.json"))
        members = {
            info.filename: source.read(info.filename)
            for info in source.infolist()
            if info.filename != "manifest.json"
        }
    staged = tmp_path / "tamper.db"
    staged.unlink(missing_ok=True)
    staged.write_bytes(members[member])
    connection = sqlite3.connect(staged)
    try:
        connection.executescript(sql)
        connection.commit()
    finally:
        connection.close()
    members[member] = staged.read_bytes()
    value = manifest.to_dict()
    for entry in value["files"]:
        if entry["path"] == member:
            entry["size"] = len(members[member])
            entry["sha256"] = hashlib.sha256(members[member]).hexdigest()
    broken = tmp_path / "tampered.sporely"
    broken.unlink(missing_ok=True)
    with ZipFile(broken, "w") as target:
        target.writestr("manifest.json", json.dumps(value).encode("utf-8"))
        for name, payload in members.items():
            target.writestr(name, payload)
    return broken


def _live_objects(path: Path) -> set[tuple[str, str]]:
    gc.collect()
    connection = sqlite3.connect(path)
    try:
        return {
            (str(row[0]), str(row[1]))
            for row in connection.execute(
                "SELECT type, name FROM sqlite_master WHERE type IN ('trigger', 'view')"
            )
        }
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("member", "sql"),
    [
        (
            "databases/mushrooms.db",
            "CREATE TRIGGER audit_extra_trigger AFTER UPDATE ON observations "
            "BEGIN UPDATE observations SET notes = 'altered' WHERE id = NEW.id; END;",
        ),
        (
            "databases/mushrooms.db",
            "CREATE VIEW audit_extra_view AS SELECT id FROM observations;",
        ),
        (
            "databases/reference_values.db",
            "DROP TRIGGER reference_work_cloud_sync_insert; "
            "CREATE TRIGGER reference_work_cloud_sync_insert AFTER INSERT ON reference_works "
            "BEGIN SELECT 1; END;",
        ),
    ],
    ids=["extra-trigger", "extra-view", "changed-trigger"],
)
def test_restore_rejects_unknown_or_altered_schema_objects(installation, tmp_path, member, sql):
    archive = _make_backup(tmp_path)
    broken = _tampered(archive, tmp_path, member, sql)
    before = {
        "main": _live_objects(schema.get_database_path()),
        "reference": _live_objects(schema.get_reference_database_path()),
    }

    with pytest.raises(FullRestoreError, match="trigger|view"):
        _restore(broken, tmp_path)

    assert _live_objects(schema.get_database_path()) == before["main"]
    assert _live_objects(schema.get_reference_database_path()) == before["reference"]
    assert ("trigger", "audit_extra_trigger") not in before["main"]


def test_unmodified_backup_still_restores(installation, tmp_path):
    archive = _make_backup(tmp_path)
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("DELETE FROM observations")
    gc.collect()

    _restore(archive, tmp_path)

    with sqlite3.connect(schema.get_database_path()) as connection:
        genera = [row[0] for row in connection.execute("SELECT genus FROM observations")]
    assert genera == ["Archived"]


def test_foreign_trigger_is_rejected_before_the_writable_migration(
    installation, tmp_path, monkeypatch
):
    from utils.archive import full_restore

    archive = _make_backup(tmp_path)
    broken = _tampered(
        archive, tmp_path, "databases/mushrooms.db",
        "CREATE TRIGGER audit_extra_trigger AFTER UPDATE ON observations "
        "BEGIN SELECT 1; END;",
    )
    migrations: list[Path] = []
    original = full_restore._migrate_staged_databases
    monkeypatch.setattr(
        full_restore, "_migrate_staged_databases",
        lambda main, reference: (migrations.append(main), original(main, reference)),
    )

    with pytest.raises(FullRestoreError, match="trigger"):
        _restore(broken, tmp_path)
    assert migrations == []
