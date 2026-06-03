from __future__ import annotations

import json
import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QWidget, QCheckBox, QDialogButtonBox
from PySide6.QtWidgets import QPlainTextEdit
from PySide6.QtCore import QPoint

import ui.main_window as main_window
import utils.cloud_sync as cloud_sync


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class _StubDatabaseSettingsDialog(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)


class _FakeMainWindow(QWidget):
    def __init__(self, *, running: bool = False):
        super().__init__()
        self.observations_tab = SimpleNamespace(_cloud_sync_worker=None)
        self._running = bool(running)
        self.start_calls: list[dict] = []

    def start_cloud_sync(self, *, show_status: bool, run_refresh_flow: bool, materialize_remote_images: bool) -> bool:
        self.start_calls.append(
            {
                "show_status": bool(show_status),
                "run_refresh_flow": bool(run_refresh_flow),
                "materialize_remote_images": bool(materialize_remote_images),
            }
        )
        self._running = True
        return True

    def is_cloud_sync_running(self) -> bool:
        return self._running

    def _update_corner_ui(self) -> None:
        return None


def _build_settings_hub_dialog(
    monkeypatch,
    qapp,
    *,
    app_settings: dict | None = None,
    client=None,
    running: bool = False,
):
    default_client = {
        "user_id": "user-123",
        "fetch_current_user_info": lambda: {"email": "sigmund.as@gmail.com"},
        "fetch_profile": lambda: {
            "username": "sigmundas",
            "display_name": "Sigmundas",
            "bio": "",
            "avatar_url": "",
        },
        "fetch_cloud_plan_profile": lambda: {"cloud_plan": "free", "is_pro": False},
        "list_remote_observations": lambda: [
            {"visibility": "private", "location_precision": "exact"},
            {"visibility": "public", "location_precision": "exact"},
        ],
    }
    fake_client = SimpleNamespace(**default_client)
    if client is not None:
        fake_client.__dict__.update(getattr(client, "__dict__", {}))
    fake_parent = _FakeMainWindow(running=running)

    monkeypatch.setattr(main_window, "DatabaseSettingsDialog", _StubDatabaseSettingsDialog)
    monkeypatch.setattr(main_window.SettingsDB, "get_profile", lambda: {
        "username": "",
        "name": "",
        "email": "",
        "bio": "",
        "avatar_url": "",
    })
    monkeypatch.setattr(main_window.SettingsDB, "get_setting", lambda key, default=None: default)
    monkeypatch.setattr(main_window.SettingsDB, "set_setting", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_window.SettingsDB, "set_profile", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_window, "get_app_settings", lambda: dict(app_settings or {}))
    monkeypatch.setattr(main_window.ArtsobservasjonerSettingsDialog, "_update_status", lambda self: None)
    monkeypatch.setattr(main_window.ArtsobservasjonerSettingsDialog, "_update_controls", lambda self: None)
    monkeypatch.setattr("utils.artsobs_uploaders.list_uploaders", lambda: [])
    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "from_stored_credentials", lambda: fake_client)

    dialog = main_window.SettingsHubDialog(fake_parent)
    dialog.show()
    qapp.processEvents()
    dialog.refresh_cloud_sync_status()
    qapp.processEvents()
    return fake_parent, dialog


def test_profile_cloud_controls_expose_sync_actions(monkeypatch, qapp):
    parent, dialog = _build_settings_hub_dialog(monkeypatch, qapp)

    assert dialog.cloud_sync_now_button.text() == "Sync now"
    assert dialog.cloud_offline_media_button.text() == "Download missing cloud media for offline use"
    assert dialog._hub_cancel_button.text() == "Cancel"
    assert dialog._hub_save_button.text() == "Save"
    assert dialog.cloud_sync_now_button.isEnabled() is True
    assert dialog.cloud_offline_media_button.isEnabled() is True
    assert dialog._artsobs_dialog._cloud_section.objectName() == "sectionCard"
    assert dialog.cloud_sync_group.objectName() == "sectionCard"
    cloud_card = dialog._artsobs_dialog
    assert cloud_card.cloud_status_label.text() == "Signed in as: sigmund.as@gmail.com"
    assert cloud_card.cloud_plan_label.text() == "Plan: Free"
    assert cloud_card.cloud_privacy_slots_label.text() == "Private/fuzzed slots: 1 used of 20 (19 available)"
    assert cloud_card.cloud_upgrade_label.isVisible() is True
    assert "sporely.no" in cloud_card.cloud_upgrade_label.text()
    login_x = cloud_card.cloud_login_button.mapTo(cloud_card, QPoint(0, 0)).x()
    status_x = cloud_card.cloud_status_label.mapTo(cloud_card, QPoint(0, 0)).x()
    selector_x = cloud_card.cloud_sharing_selector.mapTo(cloud_card, QPoint(0, 0)).x()
    assert abs(login_x - selector_x) <= 1
    assert abs(status_x - selector_x) <= 1

    label_texts = {label.text() for label in dialog.findChildren(main_window.QLabel)}
    assert "Default sharing" in label_texts
    assert "Default sharing:" not in label_texts
    assert "Cloud sign-in" in label_texts

    checkbox_texts = {checkbox.text() for checkbox in dialog.findChildren(QCheckBox)}
    assert "Upload desktop images to cloud" not in checkbox_texts
    assert "Download cloud images to this device" not in checkbox_texts

    dialog.deleteLater()
    parent.deleteLater()


def test_profile_cloud_controls_hide_privacy_slot_count_for_pro(monkeypatch, qapp):
    pro_client = SimpleNamespace(
        user_id="user-123",
        fetch_current_user_info=lambda: {"email": "sigmund.as@gmail.com"},
        fetch_profile=lambda: {
            "username": "sigmundas",
            "display_name": "Sigmundas",
            "bio": "",
            "avatar_url": "",
        },
        fetch_cloud_plan_profile=lambda: {"cloud_plan": "pro", "is_pro": True},
        list_remote_observations=lambda: [
            {"visibility": "private", "location_precision": "fuzzed"},
            {"visibility": "friends", "location_precision": "exact"},
        ],
    )

    parent, dialog = _build_settings_hub_dialog(monkeypatch, qapp, client=pro_client)
    cloud_card = dialog._artsobs_dialog

    assert cloud_card.cloud_plan_label.text() == "Plan: Pro"
    assert cloud_card.cloud_privacy_slots_label.isVisible() is False
    assert cloud_card.cloud_privacy_slots_label.text() == ""
    assert cloud_card.cloud_upgrade_label.isVisible() is False

    dialog.deleteLater()
    parent.deleteLater()


def test_profile_cloud_avatar_url_triggers_avatar_fetch(monkeypatch, qapp):
    avatar_calls: list[str] = []

    fake_client = SimpleNamespace(
        user_id="user-123",
        fetch_current_user_info=lambda: {"email": "sigmund.as@gmail.com"},
        fetch_profile=lambda: {
            "username": "sigmundas",
            "display_name": "Sigmundas",
            "bio": "",
            "avatar_url": "https://example.com/avatar.jpg",
        },
    )

    monkeypatch.setattr(
        main_window.SettingsHubDialog,
        "_fetch_and_set_profile_avatar",
        lambda self, avatar_url: avatar_calls.append(str(avatar_url)),
    )

    parent, dialog = _build_settings_hub_dialog(monkeypatch, qapp, client=fake_client)

    assert avatar_calls == ["https://example.com/avatar.jpg"]

    dialog.deleteLater()
    parent.deleteLater()


def test_profile_cloud_controls_show_last_status_and_details(monkeypatch, qapp):
    app_settings = {
        "cloud_last_sync_at": "2026-06-03T08:30:00+00:00",
        "cloud_last_sync_summary": "1 observation pushed.\nOriginal uploads: 2 uploaded, 1 skipped, 1 failed.",
        "cloud_last_sync_error_count": 2,
        "cloud_last_sync_errors_json": json.dumps(["conflict A", "conflict B"]),
    }
    parent, dialog = _build_settings_hub_dialog(monkeypatch, qapp, app_settings=app_settings)

    assert dialog.cloud_sync_status_label.text() == "Last sync: 2026-06-03 08:30 UTC"
    assert "Original uploads: 2 uploaded, 1 skipped, 1 failed." in dialog.cloud_sync_summary_label.text()
    assert dialog.cloud_sync_error_label.text() == "Errors: 2"
    assert dialog.cloud_sync_details_button.isVisible() is True
    assert dialog.cloud_sync_details_button.isEnabled() is True

    dialog.deleteLater()
    parent.deleteLater()


def test_profile_cloud_sync_details_window_shows_raw_errors(monkeypatch, qapp):
    parent, dialog = _build_settings_hub_dialog(
        monkeypatch,
        qapp,
        app_settings={
            "cloud_last_sync_errors_json": json.dumps(["conflict A", "conflict B"]),
            "cloud_last_sync_error_count": 2,
        },
    )

    details_dialog = dialog._build_cloud_sync_details_dialog(["conflict A", "conflict B"])
    text_edit = details_dialog.findChild(QPlainTextEdit)

    assert text_edit is not None
    assert text_edit.toPlainText() == "conflict A\nconflict B"
    assert details_dialog.findChild(QDialogButtonBox) is None

    details_dialog.deleteLater()
    dialog.deleteLater()
    parent.deleteLater()


def test_profile_cloud_sync_now_starts_metadata_first_sync(monkeypatch, qapp):
    parent, dialog = _build_settings_hub_dialog(monkeypatch, qapp)

    dialog.cloud_sync_now_button.click()
    qapp.processEvents()

    assert parent.start_calls == [
        {
            "show_status": True,
            "run_refresh_flow": False,
            "materialize_remote_images": False,
        }
    ]

    dialog.deleteLater()
    parent.deleteLater()


def test_profile_cloud_offline_media_action_starts_materializing_sync(monkeypatch, qapp):
    parent, dialog = _build_settings_hub_dialog(monkeypatch, qapp)

    dialog.cloud_offline_media_button.click()
    qapp.processEvents()

    assert parent.start_calls == [
        {
            "show_status": True,
            "run_refresh_flow": False,
            "materialize_remote_images": True,
        }
    ]

    dialog.deleteLater()
    parent.deleteLater()
