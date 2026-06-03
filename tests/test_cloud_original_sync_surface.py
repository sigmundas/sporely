from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication

import ui.cloud_sync_dialog as cloud_sync_dialog
from ui.cloud_sync_dialog import CloudSyncDialog
from utils import cloud_sync


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _build_dialog(monkeypatch, *, original_enabled: bool = False) -> CloudSyncDialog:
    fake_client = SimpleNamespace(user_id="user-123")
    monkeypatch.setattr(cloud_sync_dialog.SporelyCloudClient, "from_stored_credentials", lambda: fake_client)
    monkeypatch.setattr(cloud_sync_dialog, "load_saved_cloud_password", lambda: ("", None, False))
    monkeypatch.setattr(cloud_sync_dialog, "get_app_settings", lambda: {})
    monkeypatch.setattr(cloud_sync_dialog, "is_full_resolution_original_sync_enabled", lambda: original_enabled)
    return CloudSyncDialog()


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


def test_cloud_sync_dialog_persists_original_sync_checkbox(monkeypatch, qapp):
    dialog = _build_dialog(monkeypatch, original_enabled=False)
    saved_values: list[bool] = []
    monkeypatch.setattr(
        cloud_sync_dialog,
        "set_full_resolution_original_sync_enabled",
        lambda enabled: saved_values.append(bool(enabled)),
    )

    assert dialog._original_sync_check.isChecked() is False
    assert "Uploads eligible field and microscope originals for recovery." in dialog._original_sync_note.text()

    dialog._original_sync_check.setChecked(True)

    assert saved_values == [True]


def test_cloud_sync_dialog_appends_original_upload_summary(monkeypatch, qapp):
    dialog = _build_dialog(monkeypatch, original_enabled=True)

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
    dialog = _build_dialog(monkeypatch, original_enabled=False)

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
