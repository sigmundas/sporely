from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QCheckBox

import ui.cloud_sync_dialog as cloud_sync_dialog
import ui.main_window as main_window
from ui.cloud_sync_dialog import CloudSyncDialog
from ui.main_window import ArtsobservasjonerSettingsDialog
from utils import cloud_sync


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _build_cloud_dialog(monkeypatch) -> CloudSyncDialog:
    fake_client = SimpleNamespace(user_id="user-123")
    monkeypatch.setattr(cloud_sync_dialog.SporelyCloudClient, "from_stored_credentials", lambda: fake_client)
    monkeypatch.setattr(cloud_sync_dialog, "load_saved_cloud_password", lambda: ("", None, False))
    monkeypatch.setattr(cloud_sync_dialog, "get_app_settings", lambda: {})
    return CloudSyncDialog()


def _build_preferences_dialog(monkeypatch, tmp_path) -> ArtsobservasjonerSettingsDialog:
    monkeypatch.setattr(main_window, "app_data_dir", lambda: tmp_path)
    monkeypatch.setattr(main_window.SettingsDB, "get_setting", lambda key, default=None: default)
    monkeypatch.setattr(main_window.SettingsDB, "set_setting", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_window.ArtsobservasjonerSettingsDialog, "_update_status", lambda self: None)
    monkeypatch.setattr(main_window.ArtsobservasjonerSettingsDialog, "_update_controls", lambda self: None)
    monkeypatch.setattr("utils.artsobs_uploaders.list_uploaders", lambda: [])
    return ArtsobservasjonerSettingsDialog()


def test_original_upload_summary_is_quiet_when_disabled():
    assert (
        cloud_sync.format_original_upload_summary(
            {
                "enabled": False,
                "uploaded": 0,
                "skipped_disabled": 3,
                "skipped_ineligible": 0,
                "skipped_too_large": 0,
                "failed_uploads": 0,
            }
        )
        is None
    )


def test_original_upload_summary_reports_counts_when_enabled():
    summary = cloud_sync.format_original_upload_summary(
        {
            "enabled": True,
            "uploaded": 2,
            "skipped_disabled": 0,
            "skipped_ineligible": 1,
            "skipped_too_large": 1,
            "failed_uploads": 1,
        }
    )

    assert summary == "Original uploads: 2 uploaded, 2 skipped, 1 failed."


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("skipped_disabled", None),
        ("downloaded_to_cache", "Original recovery: 1 downloaded."),
        ("skipped_existing_cache", "Original recovery: 1 skipped."),
        ("download_failed", "Original recovery: 1 failed."),
    ],
)
def test_original_recovery_summary_formats_statuses(status, expected):
    assert cloud_sync.format_original_recovery_summary({"status": status}) == expected


def test_preferences_dialog_does_not_expose_original_sync_toggle(monkeypatch, qapp, tmp_path):
    dialog = _build_preferences_dialog(monkeypatch, tmp_path)

    assert hasattr(dialog, "cloud_originals_checkbox") is False
    assert hasattr(dialog, "cloud_originals_note") is False

    checkbox_texts = {checkbox.text() for checkbox in dialog.findChildren(QCheckBox)}
    assert "Sync full-resolution originals" not in checkbox_texts
    assert "Uploads eligible field and microscope originals for recovery and reproducibility." not in " ".join(
        checkbox_texts
    )


def test_cloud_sync_dialog_appends_original_upload_summary(monkeypatch, qapp):
    dialog = _build_cloud_dialog(monkeypatch)

    dialog._on_sync_done(
        {
            "pushed": 1,
            "pulled": 0,
            "calibrations_pushed": 0,
            "calibrations_pulled": 0,
            "errors": [],
            "deleted_remote": [],
            "original_sync": {
                "enabled": True,
                "uploaded": 2,
                "skipped_disabled": 0,
                "skipped_ineligible": 1,
                "skipped_too_large": 0,
                "failed_uploads": 1,
            },
        }
    )

    status_text = dialog._status_label.text()
    assert "1 observation pushed." in status_text
    assert "Original uploads: 2 uploaded, 1 skipped, 1 failed." in status_text


def test_cloud_sync_dialog_stays_quiet_when_original_sync_disabled(monkeypatch, qapp):
    dialog = _build_cloud_dialog(monkeypatch)

    dialog._on_sync_done(
        {
            "pushed": 1,
            "pulled": 0,
            "calibrations_pushed": 0,
            "calibrations_pulled": 0,
            "errors": [],
            "deleted_remote": [],
            "original_sync": {
                "enabled": False,
                "uploaded": 0,
                "skipped_disabled": 3,
                "skipped_ineligible": 0,
                "skipped_too_large": 0,
                "failed_uploads": 0,
            },
        }
    )

    assert "Original uploads:" not in dialog._status_label.text()
