"""F06 (2026-11-26 security audit): a restored backup must not replay
pending cloud observation deletes, and a pending delete is retried only for
a named owner equal to the signed-in account."""

from __future__ import annotations

import gc
import sqlite3
from pathlib import Path
from zipfile import ZipFile

import pytest

from database import schema
from utils.archive.full_backup import create_full_backup
from utils.archive.full_restore import restore_full_backup
from utils.archive.inventory import SettingPolicy, database_setting_policy
from utils.cloud_sync_impl.observation_delete import (
    PENDING_OBSERVATION_DELETE_PREFIX,
    mark_observation_delete_pending,
    pending_observation_deletes,
    retry_pending_observation_deletes,
)


ACCOUNT = "account-test-1"


class RecordingClient:
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.deleted: list[str] = []

    def delete_cloud_observation(self, cloud_id):
        self.deleted.append(str(cloud_id))


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


def _restore(archive: Path, tmp_path: Path) -> None:
    restore_full_backup(
        archive,
        app_version="test",
        safety_backup_path=tmp_path / "safety.sporely",
        close_live=lambda: None,
        reopen_live=lambda: None,
    )
    gc.collect()


def test_pending_delete_intents_are_not_restorable_settings():
    key = f"{PENDING_OBSERVATION_DELETE_PREFIX}cloud-obs-1"
    assert database_setting_policy(key) is SettingPolicy.EXCLUDE


def test_restored_backup_sends_no_pending_delete(installation, tmp_path):
    mark_observation_delete_pending("cloud-same-owner", ACCOUNT)
    mark_observation_delete_pending("cloud-empty-owner", "")
    mark_observation_delete_pending("cloud-foreign-owner", "account-test-2")
    archive = tmp_path / "with-intents.sporely"
    create_full_backup(archive, app_version="test", qsettings_values={})
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute(
            "DELETE FROM settings WHERE key LIKE ?", (f"{PENDING_OBSERVATION_DELETE_PREFIX}%",)
        )
    gc.collect()

    _restore(archive, tmp_path)
    client = RecordingClient(ACCOUNT)
    summary = retry_pending_observation_deletes(client)

    assert client.deleted == []
    assert summary["completed"] == []
    assert pending_observation_deletes() == {}


def test_empty_owner_intent_is_never_sent(installation):
    mark_observation_delete_pending("cloud-empty-owner", "")
    client = RecordingClient(ACCOUNT)

    retry_pending_observation_deletes(client)

    assert client.deleted == []


def test_signed_out_client_sends_no_delete(installation):
    mark_observation_delete_pending("cloud-same-owner", ACCOUNT)
    client = RecordingClient("")

    retry_pending_observation_deletes(client)

    assert client.deleted == []
    assert pending_observation_deletes() == {"cloud-same-owner": ACCOUNT}


def test_interrupted_delete_of_this_installation_is_still_retried(installation):
    mark_observation_delete_pending("cloud-same-owner", ACCOUNT)
    client = RecordingClient(ACCOUNT)

    summary = retry_pending_observation_deletes(client)

    assert client.deleted == ["cloud-same-owner"]
    assert summary["completed"] == ["cloud-same-owner"]
    assert pending_observation_deletes() == {}


def test_backup_archive_carries_no_pending_delete(installation, tmp_path):
    mark_observation_delete_pending("cloud-same-owner", ACCOUNT)
    archive = tmp_path / "with-intent.sporely"
    create_full_backup(archive, app_version="test", qsettings_values={})

    staged = tmp_path / "archived.db"
    with ZipFile(archive) as source:
        staged.write_bytes(source.read("databases/mushrooms.db"))
    connection = sqlite3.connect(staged)
    try:
        keys = [row[0] for row in connection.execute("SELECT key FROM settings")]
    finally:
        connection.close()
    assert not [key for key in keys if key.startswith(PENDING_OBSERVATION_DELETE_PREFIX)]
    # The source installation keeps its own intent.
    assert pending_observation_deletes() == {"cloud-same-owner": ACCOUNT}


def test_lookalike_keys_retained_by_restore_are_not_delete_intents(installation):
    # ``_`` is a LIKE wildcard: an older desktop's reader would take this key
    # for an intent, so backups and restores drop it, and this reader ignores it.
    lookalike = "sporely_cloud_observationXdelete_pending_cloud-victim"
    assert database_setting_policy(lookalike) is SettingPolicy.EXCLUDE
    assert database_setting_policy("sporely_cloud_snapshot_obs_abc") is SettingPolicy.EXACT
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("INSERT INTO settings (key, value) VALUES (?, ?)", (lookalike, ACCOUNT))
    gc.collect()
    client = RecordingClient(ACCOUNT)

    retry_pending_observation_deletes(client)

    assert client.deleted == []
    assert pending_observation_deletes() == {}
