"""Stage 10d1: Preferanser's profile and cloud-sync messages as notices.

The Profile page shows the bottom bar of the Sporely Cloud card it holds;
the profile photo, the cloud-sync buttons and a profile that did not reach
Sporely Cloud say so there instead of in message boxes. One builder makes
the Sync log for the main window and Preferanser.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QDialogButtonBox, QMessageBox, QPlainTextEdit, QWidget

import ui.main_window as main_window
from tests.test_preferences_cloud_sync_controls import _build_settings_hub_dialog, qapp  # noqa: F401


@pytest.fixture(autouse=True)
def _no_information_or_warning_boxes(monkeypatch):
    def _fail(*args, **_kwargs):
        raise AssertionError(f"a message box was shown: {args[1:3]}")

    monkeypatch.setattr(QMessageBox, "information", _fail)
    monkeypatch.setattr(QMessageBox, "warning", _fail)


@pytest.fixture
def hub(monkeypatch, qapp):  # noqa: F811
    parent, dialog = _build_settings_hub_dialog(monkeypatch, qapp)
    yield dialog
    dialog.deleteLater()
    parent.deleteLater()


def _notice(hub):
    return hub._profile_hint_controller._notice


def test_the_profile_page_has_its_own_bottom_bar(hub, qapp):  # noqa: F811
    """Not the publishing dialog's: that bar shows the publishing services'
    sign-in state, which is not the Profile page's (render review)."""
    bar = hub._profile_hint_bar
    hub._nav.setCurrentRow(hub.PAGE_PROFILE)
    qapp.processEvents()
    assert bar.parent() is hub._hub_hint_holder and bar.isVisible()
    assert not hub._artsobs_dialog.hint_bar.isVisible()
    hub._nav.setCurrentRow(hub.PAGE_PUBLISHING)
    qapp.processEvents()
    assert not bar.isVisible() and hub._artsobs_dialog.hint_bar.isVisible()


def test_a_profile_photo_needs_sign_in_and_says_so_in_the_bar(hub, qapp):  # noqa: F811
    hub._cloud_client = None
    hub._get_cloud_client = lambda: None
    hub._nav.setCurrentRow(hub.PAGE_LANGUAGE)
    hub._choose_profile_avatar()
    qapp.processEvents()
    assert hub._nav.currentRow() == hub.PAGE_PROFILE  # shown where the photo is
    notice = _notice(hub)
    assert notice.tone == "warning" and notice.text.startswith("Sign in to Sporely Cloud")
    assert hub._profile_hint_bar.isVisible()


def test_a_profile_photo_upload_says_how_it_went(hub, monkeypatch, tmp_path):
    monkeypatch.setattr(main_window.QFileDialog, "getOpenFileName", lambda *_a, **_k: (str(tmp_path / "me.jpg"), ""))
    monkeypatch.setattr(hub, "_avatar_jpeg_bytes_from_path", lambda _path: b"jpeg")

    def fail(_bytes):
        raise RuntimeError("413 Payload Too Large")

    hub._cloud_client.upload_profile_avatar = fail
    hub._choose_profile_avatar()
    notice = _notice(hub)
    assert (notice.tone, notice.text, notice.details) == (
        "error", "Could not update profile photo.", "413 Payload Too Large",
    )

    hub._cloud_client.upload_profile_avatar = lambda _bytes: "https://example.test/avatar.jpg"
    hub._choose_profile_avatar()
    assert (_notice(hub).tone, _notice(hub).text) == ("success", "Profile photo updated.")


def test_cloud_sync_that_cannot_start_says_so_in_the_bar(hub, monkeypatch):
    monkeypatch.setattr(hub.parent(), "start_cloud_sync", lambda **_k: False)
    hub._request_settings_cloud_sync(sync_images=True, materialize_remote_images=False, full_pull=False)
    notice = _notice(hub)
    assert (notice.tone, notice.text) == ("info", "Cloud sync is already running or could not start.")

    monkeypatch.setattr(hub, "_settings_hub_main_window", lambda: None)
    hub._request_settings_cloud_sync(sync_images=True, materialize_remote_images=False, full_pull=False)
    assert (_notice(hub).tone, _notice(hub).text) == ("error", "Cloud sync is not available right now.")


def test_a_profile_that_did_not_reach_the_cloud_keeps_preferences_open_on_the_profile(hub, qapp):  # noqa: F811
    def fail(**_kwargs):
        raise RuntimeError("HTTP 500 from the profile endpoint")

    hub._cloud_client.update_profile = fail
    hub._nav.setCurrentRow(hub.PAGE_LANGUAGE)
    hub._hub_save_button.click()
    qapp.processEvents()
    assert hub.isVisible() and hub.result() == 0
    assert hub._nav.currentRow() == hub.PAGE_PROFILE
    notice = _notice(hub)
    assert notice.tone == "error"
    assert notice.text == "The local profile was saved, but Sporely Cloud was not updated."
    assert notice.details == "HTTP 500 from the profile endpoint"


def test_a_taken_username_needs_no_details(hub):
    text, details = hub._profile_sync_error_notice(RuntimeError('duplicate key "profiles_username_key"'))
    assert text == "That username is already taken. Please choose a different one." and details is None


def test_one_sync_log_builder_for_preferences_and_the_main_window(qtbot):
    dialog = main_window.build_sync_log_dialog(None, "Status: ok\nErrors: 2")
    qtbot.addWidget(dialog)
    assert dialog.windowTitle() == "Sync log"
    assert dialog.findChild(QPlainTextEdit).toPlainText() == "Status: ok\nErrors: 2"
    assert dialog.findChild(QDialogButtonBox).standardButtons() == QDialogButtonBox.Ok
    # The main window's own builder is the same one, with its text.
    window = QWidget()
    qtbot.addWidget(window)
    window._cloud_sync_details_text = lambda: "Blocked observations:\n- 4"
    built = main_window.MainWindow._build_cloud_sync_details_dialog(window)
    qtbot.addWidget(built)
    assert built.findChild(QPlainTextEdit).toPlainText() == "Blocked observations:\n- 4"
