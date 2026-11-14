"""Signing in or out in Preferanser updates Preferanser and the main window.

Preferanser holds the publishing dialog (its Sporely Cloud card) inside its
page widgets, so the dialog's ``parent()`` is a page, not Preferanser. Its
sign-in, sign-out and account-link reset hooks looked Preferanser up through
``parent()`` and never reached it: after signing out there, the main window
kept the old account's cached client and avatar. The dialog now finds
Preferanser among its ancestors.

The real Preferanser and publishing dialog; the main window is the shared
fake (a real widget with an Observations tab), so the main window's own
state is what the hooks are checked against.
"""
from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QMessageBox

import ui.main_window as main_window
from tests.test_preferences_cloud_sync_controls import _build_settings_hub_dialog, qapp  # noqa: F401
from utils import cloud_sync


@pytest.fixture
def signed_in(monkeypatch, qapp):  # noqa: F811
    parent, hub = _build_settings_hub_dialog(monkeypatch, qapp)
    corner = []
    parent._update_corner_ui = lambda: corner.append("corner")
    parent._avatar_pixmap_cached = object()  # the old account's avatar
    monkeypatch.setattr(main_window, "update_app_settings", lambda _payload: None)
    yield parent, hub, corner
    hub.deleteLater()
    parent.deleteLater()


def _signed_out_everywhere(monkeypatch):
    """Stored credentials are gone after a sign-out or reset."""
    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "from_stored_credentials", lambda: None)
    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "clear_credentials", staticmethod(lambda: None))


def test_the_publishing_dialog_finds_preferences_among_its_ancestors(signed_in):
    _parent, hub, _corner = signed_in
    publishing = hub._artsobs_dialog
    assert publishing.parent() is not hub  # a page widget
    assert publishing._settings_hub() is hub
    assert publishing._settings_host() is hub


def test_signing_out_in_preferences_forgets_the_account_in_the_main_window(signed_in, monkeypatch, qapp):  # noqa: F811
    parent, hub, corner = signed_in
    assert parent._cloud_client is not None and hub._cloud_client is not None
    _signed_out_everywhere(monkeypatch)
    publishing = hub._artsobs_dialog

    publishing._logout_cloud()
    qapp.processEvents()

    assert parent._cloud_client is None and hub._cloud_client is None
    assert not hasattr(parent, "_avatar_pixmap_cached")  # the corner shows no old avatar
    assert corner  # the main window's corner was redrawn
    assert publishing._cached_cloud_client() is None  # nothing brings the old client back
    assert publishing.cloud_status_label.text() == "Not logged in"
    assert publishing.cloud_login_button.isEnabled() and not publishing.cloud_logout_button.isEnabled()


def test_resetting_the_account_link_in_preferences_forgets_the_account(signed_in, monkeypatch):
    parent, hub, _corner = signed_in
    _signed_out_everywhere(monkeypatch)
    monkeypatch.setattr(QMessageBox, "question", lambda *_a, **_k: QMessageBox.Ok)
    monkeypatch.setattr("database.models.reset_cloud_sync_state", lambda: None)
    monkeypatch.setattr(cloud_sync, "clear_saved_cloud_password", lambda: [])

    hub._artsobs_dialog._on_reset_cloud_account()

    assert parent._cloud_client is None and hub._cloud_client is None
    assert not hasattr(parent, "_avatar_pixmap_cached")


def test_signing_in_in_preferences_reaches_the_profile_and_the_main_window(signed_in, monkeypatch):
    parent, hub, corner = signed_in
    monkeypatch.setattr(cloud_sync, "ensure_database_linked_to_cloud_user", lambda _client: None)
    new_client = SimpleNamespace(**dict(hub._cloud_client.__dict__))
    new_client.user_id = "user-456"
    new_client.save_credentials = lambda **_kwargs: None
    new_client.fetch_current_user_info = lambda: {"email": "new@example.com"}

    hub._artsobs_dialog._on_cloud_login_success(new_client, "new@example.com")

    assert parent._cloud_client is new_client and hub._cloud_client is new_client
    assert hub._profile_email.text() == "new@example.com"
    assert corner


def test_opened_on_its_own_the_dialog_has_no_preferences(qtbot, monkeypatch):
    """“Manage online publishing accounts”: the window that opened it is the host."""
    monkeypatch.setattr(main_window.ArtsobservasjonerSettingsDialog, "_update_status", lambda self: None)
    monkeypatch.setattr(main_window, "get_app_settings", lambda: {})
    monkeypatch.setattr(main_window.SettingsDB, "get_setting", lambda key, default=None: default)
    monkeypatch.setattr("utils.artsobs_uploaders.list_uploaders", lambda: [])
    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "from_stored_credentials", lambda: None)
    from PySide6.QtWidgets import QWidget

    host = QWidget()
    qtbot.addWidget(host)
    dialog = main_window.ArtsobservasjonerSettingsDialog(host)  # the host owns it
    assert dialog._settings_hub() is None
    assert dialog._settings_host() is host


def test_signing_out_clears_the_main_window_even_if_reading_a_setting_fails(signed_in, monkeypatch):
    """Codex: a locked database made the hook stop before the main window's client went."""
    parent, hub, _corner = signed_in
    _signed_out_everywhere(monkeypatch)

    def locked_for_the_avatar(key, default=None):
        if key == "profile_avatar_url":  # the setting the hook reads
            raise RuntimeError("database is locked")
        return default

    monkeypatch.setattr(main_window.SettingsDB, "get_setting", locked_for_the_avatar)
    hub._artsobs_dialog._logout_cloud()
    assert parent._cloud_client is None
    assert hub._artsobs_dialog.__dict__.get("_cloud_client") is None


def test_changing_account_from_the_standalone_dialog_redraws_the_corner(monkeypatch):
    """Opened from the main window (no Preferanser): the corner shows signed out."""
    from tests.test_preferences_cloud_sync_controls import _make_logout_dialog, _with_host_lookup  # noqa: F401

    corner = []
    window = SimpleNamespace(_cloud_client=object(), _update_corner_ui=lambda: corner.append("corner"))
    dialog, _events = _make_logout_dialog(monkeypatch, parent=window, cached_client=window._cloud_client)
    dialog._show_cloud_notice = lambda *_a, **_k: None
    monkeypatch.setattr(QMessageBox, "question", lambda *_a, **_k: QMessageBox.Ok)
    monkeypatch.setattr("database.models.reset_cloud_sync_state", lambda: None)
    monkeypatch.setattr(cloud_sync, "clear_saved_cloud_password", lambda: [])

    main_window.ArtsobservasjonerSettingsDialog._on_reset_cloud_account(dialog)

    assert window._cloud_client is None and corner == ["corner"]


def _another_account(hub, *, fetch_profile):
    client = SimpleNamespace(**dict(hub._cloud_client.__dict__))
    client.user_id = "user-456"
    client.save_credentials = lambda **_kwargs: None
    client.fetch_current_user_info = lambda: {"email": "new@example.com"}
    client.fetch_profile = fetch_profile
    return client


A_PHOTO = "https://example.supabase.co/storage/v1/object/public/avatars/user-123/avatar.jpg"


def test_signing_in_to_an_account_without_a_photo_drops_the_previous_photo(signed_in, monkeypatch):
    """Codex: the previous account's photo stayed, and was saved into the profile."""
    _parent, hub, _corner = signed_in
    monkeypatch.setattr(cloud_sync, "ensure_database_linked_to_cloud_user", lambda _client: None)
    saved = []
    monkeypatch.setattr(main_window.SettingsDB, "set_profile", lambda *args: saved.append(args))
    hub._profile_avatar_url = A_PHOTO
    no_photo = _another_account(hub, fetch_profile=lambda: {"id": "user-456", "username": "b", "avatar_url": None})

    hub._artsobs_dialog._on_cloud_login_success(no_photo, "new@example.com")

    assert hub._profile_avatar_url == ""
    assert saved and saved[-1][-1] == ""  # the profile keeps no photo URL


def test_a_failed_profile_fetch_keeps_the_photo(signed_in, monkeypatch):
    _parent, hub, _corner = signed_in
    monkeypatch.setattr(cloud_sync, "ensure_database_linked_to_cloud_user", lambda _client: None)
    hub._profile_avatar_url = A_PHOTO

    def unavailable():
        raise cloud_sync.CloudSyncError("GET profiles: 503")

    same_account = _another_account(hub, fetch_profile=unavailable)
    same_account.user_id = "user-123"

    hub._artsobs_dialog._on_cloud_login_success(same_account, "a@example.com")

    assert hub._profile_avatar_url == A_PHOTO  # not known to be gone


def test_a_failed_profile_fetch_for_another_account_drops_the_photo(signed_in, monkeypatch):
    """Codex: kept after a failed fetch, A's photo showed for B."""
    _parent, hub, _corner = signed_in
    monkeypatch.setattr(cloud_sync, "ensure_database_linked_to_cloud_user", lambda _client: None)
    hub._profile_avatar_url = A_PHOTO

    def unavailable():
        raise cloud_sync.CloudSyncError("GET profiles: 503")

    hub._artsobs_dialog._on_cloud_login_success(_another_account(hub, fetch_profile=unavailable), "new@example.com")

    assert hub._profile_avatar_url == ""
