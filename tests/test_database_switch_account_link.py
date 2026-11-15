"""Switching the profile to another database must re-check who owns it.

The cloud account link lives in profile settings, not in the database. If it
survived a database-folder switch, a database holding another account's
observations would sync into the linked account without the first-link
ownership check.
"""

from __future__ import annotations

import os
import sqlite3

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

from database import schema
from ui.database_settings_dialog import DatabaseSettingsDialog
from utils import cloud_sync


class _Client:
    def __init__(self, user_id, owned=()):
        self.access_token = None
        self.user_id = user_id
        self._owned = {str(value) for value in owned}

    def list_owned_observation_ids(self, cloud_ids):
        return {str(value) for value in cloud_ids if str(value) in self._owned}


@pytest.fixture
def profile_settings():
    """Restore the (test-isolated) profile settings file afterwards."""
    path = schema.SETTINGS_PATH
    original = path.read_bytes() if path.exists() else None
    yield
    if original is None:
        path.unlink(missing_ok=True)
    else:
        path.write_bytes(original)


def _open_dialog(qtbot, folder) -> DatabaseSettingsDialog:
    dialog = DatabaseSettingsDialog()
    qtbot.addWidget(dialog)
    dialog._loading_settings = False
    dialog.db_path_input.setText(str(folder))
    return dialog


def _add_synced_observation(cloud_id: str) -> None:
    conn = schema.get_connection()
    try:
        conn.execute(
            "INSERT INTO observations (date, cloud_id, synced_at) VALUES (?, ?, ?)",
            ("2026-05-01", cloud_id, "2026-05-02T00:00:00Z"),
        )
        conn.commit()
    finally:
        conn.close()


def test_switching_to_another_database_drops_the_account_link(qtbot, tmp_path, profile_settings):
    folder_a = tmp_path / "account-a"
    folder_b = tmp_path / "account-b"
    schema.save_app_settings({"database_folder": str(folder_a), "linked_cloud_user_id": "acct-a"})
    schema.init_database()

    _open_dialog(qtbot, folder_b)._save_settings()

    settings = schema.get_app_settings()
    assert settings["database_folder"] == str(folder_b)
    assert "linked_cloud_user_id" not in settings

    # The database in folder B holds observations account A does not own:
    # the first-link check now refuses it instead of syncing it into A.
    _add_synced_observation("9001")
    with pytest.raises(cloud_sync.AccountMismatchError):
        cloud_sync.ensure_database_linked_to_cloud_user(_Client("acct-a", owned=()))
    assert "linked_cloud_user_id" not in schema.get_app_settings()


def test_switching_back_to_an_owned_database_links_again(qtbot, tmp_path, profile_settings):
    folder_a = tmp_path / "account-a"
    folder_b = tmp_path / "other"
    schema.save_app_settings({"database_folder": str(folder_a), "linked_cloud_user_id": "acct-a"})
    schema.init_database()
    _add_synced_observation("101")
    _open_dialog(qtbot, folder_b)._save_settings()

    _open_dialog(qtbot, folder_a)._save_settings()

    assert "linked_cloud_user_id" not in schema.get_app_settings()
    assert cloud_sync.ensure_database_linked_to_cloud_user(_Client("acct-a", owned={"101"})) == "acct-a"
    assert schema.get_app_settings()["linked_cloud_user_id"] == "acct-a"


def test_saving_the_same_database_folder_keeps_the_link(qtbot, tmp_path, profile_settings):
    folder_a = tmp_path / "account-a"
    schema.save_app_settings({"database_folder": str(folder_a), "linked_cloud_user_id": "acct-a"})
    schema.init_database()

    _open_dialog(qtbot, folder_a)._save_settings()

    assert schema.get_app_settings()["linked_cloud_user_id"] == "acct-a"


def test_moving_the_database_to_a_new_folder_keeps_the_link(qtbot, tmp_path, profile_settings):
    old_db = tmp_path / "legacy" / "mushrooms.db"
    old_db.parent.mkdir()
    schema.save_app_settings({"database_path": str(old_db), "linked_cloud_user_id": "acct-a"})
    schema.init_database()
    new_folder = tmp_path / "moved"

    _open_dialog(qtbot, new_folder)._save_settings()

    assert (new_folder / "mushrooms.db").exists()
    assert not old_db.exists()
    assert schema.get_app_settings()["linked_cloud_user_id"] == "acct-a"


def test_no_settings_write_links_the_new_database_to_the_old_account(
    qtbot, tmp_path, monkeypatch, profile_settings
):
    import ui.database_settings_dialog as dialog_module

    folder_a = tmp_path / "account-a"
    folder_b = tmp_path / "account-b"
    schema.save_app_settings({"database_folder": str(folder_a), "linked_cloud_user_id": "acct-a"})
    schema.init_database()
    writes: list[dict] = []
    real_save = dialog_module.save_app_settings

    def recording_save(settings):
        writes.append(dict(settings))
        real_save(settings)

    monkeypatch.setattr(dialog_module, "save_app_settings", recording_save)

    _open_dialog(qtbot, folder_b)._save_settings()

    assert writes, "the dialog saved the switch"
    for written in writes:
        if written.get("database_folder") == str(folder_b):
            assert "linked_cloud_user_id" not in written
