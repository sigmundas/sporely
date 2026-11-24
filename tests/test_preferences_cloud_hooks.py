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


# --- Username, display name and bio follow the account (owner's choice) -------

OWNER = main_window.PROFILE_CLOUD_USER_ID_SETTING


@pytest.fixture
def profile_store(monkeypatch):
    """SettingsDB's settings as a dict; the profile fields are account A's."""
    store = {OWNER: "user-123"}
    monkeypatch.setattr(main_window.SettingsDB, "get_setting", lambda key, default=None: store.get(key, default))
    monkeypatch.setattr(main_window.SettingsDB, "set_setting", lambda key, value: store.__setitem__(key, value))
    saved = []
    monkeypatch.setattr(main_window.SettingsDB, "set_profile", lambda *args: saved.append(args))
    monkeypatch.setattr(cloud_sync, "ensure_database_linked_to_cloud_user", lambda _client: None)
    return store, saved


def _no_owner(hub, store):
    """Text typed before any sign-in: no account, stored or in Preferanser."""
    store.pop(OWNER, None)
    hub._profile_text_owner = ""


def _fill(hub, username="anna", name="Anna A", bio="Mycologist in Oslo"):
    hub._profile_username.setText(username)
    hub._profile_name.setText(name)
    hub._profile_bio.setPlainText(bio)


def _texts(hub):
    return hub._profile_username.text(), hub._profile_name.text(), hub._profile_bio.toPlainText()


def _unavailable():
    raise cloud_sync.CloudSyncError("GET profiles: 503")


def _bare_profile():
    return {"id": "user-456", "username": None, "display_name": "", "bio": None}


def test_switching_to_an_account_without_profile_text_clears_the_previous_accounts(signed_in, profile_store):
    _parent, hub, _corner = signed_in
    store, saved = profile_store
    _fill(hub)

    hub._artsobs_dialog._on_cloud_login_success(_another_account(hub, fetch_profile=_bare_profile), "new@example.com")

    assert _texts(hub) == ("", "", "")
    name, email, bio, username, _avatar = saved[-1]
    assert (name, bio, username) == ("", "", "") and email == "new@example.com"
    assert store[OWNER] == "user-456"


def test_switching_accounts_shows_the_new_accounts_own_text(signed_in, profile_store):
    _parent, hub, _corner = signed_in
    _fill(hub)
    other = _another_account(hub, fetch_profile=lambda: {"username": "bjorn", "display_name": "Bjørn B", "bio": ""})

    hub._artsobs_dialog._on_cloud_login_success(other, "new@example.com")

    assert _texts(hub) == ("bjorn", "Bjørn B", "")


def test_text_typed_before_any_sign_in_is_kept_for_the_first_account(signed_in, profile_store):
    _parent, hub, _corner = signed_in
    store, _saved = profile_store
    _no_owner(hub, store)  # never signed in
    _fill(hub)

    hub._artsobs_dialog._on_cloud_login_success(_another_account(hub, fetch_profile=_bare_profile), "new@example.com")

    assert _texts(hub) == ("anna", "Anna A", "Mycologist in Oslo")
    assert store[OWNER] == "user-456"


def test_the_same_accounts_text_is_kept_where_its_profile_has_none(signed_in, profile_store):
    _parent, hub, _corner = signed_in
    _fill(hub)
    same = _another_account(hub, fetch_profile=lambda: {"username": "anna", "display_name": "", "bio": ""})
    same.user_id = "user-123"

    hub._artsobs_dialog._on_cloud_login_success(same, "a@example.com")

    assert _texts(hub) == ("anna", "Anna A", "Mycologist in Oslo")


def test_kept_text_still_follows_the_account_on_the_next_switch(signed_in, profile_store):
    """Codex (on an earlier design): a same-account reload lost track of the text."""
    _parent, hub, _corner = signed_in
    _fill(hub)
    same = _another_account(hub, fetch_profile=lambda: {"username": "", "display_name": "", "bio": ""})
    same.user_id = "user-123"
    hub._artsobs_dialog._on_cloud_login_success(same, "a@example.com")
    assert _texts(hub) == ("anna", "Anna A", "Mycologist in Oslo")

    hub._artsobs_dialog._on_cloud_login_success(_another_account(hub, fetch_profile=_bare_profile), "new@example.com")

    assert _texts(hub) == ("", "", "")


def test_a_failed_profile_fetch_leaves_the_text_and_its_account(signed_in, profile_store):
    _parent, hub, _corner = signed_in
    store, _saved = profile_store
    _fill(hub, username="@anna")

    hub._artsobs_dialog._on_cloud_login_success(_another_account(hub, fetch_profile=_unavailable), "new@example.com")

    assert _texts(hub) == ("@anna", "Anna A", "Mycologist in Oslo")  # nothing is lost
    assert store[OWNER] == "user-123"  # still A's: Save will not send them to B


def _signed_in_as(hub, monkeypatch, user_id):
    sent = []
    client = SimpleNamespace(user_id=user_id, update_profile=lambda **fields: sent.append(fields))
    monkeypatch.setattr(hub, "_get_cloud_client", lambda: client)
    notices = []
    monkeypatch.setattr(hub, "_show_profile_notice", lambda text, tone, **_kwargs: notices.append((tone, text)))
    return client, sent, notices


def test_save_before_the_new_accounts_profile_loaded_sends_nothing(signed_in, profile_store, monkeypatch):
    """Codex: A's text was sent to B after B's profile failed to load."""
    _parent, hub, _corner = signed_in
    store, saved = profile_store
    _client, sent, notices = _signed_in_as(hub, monkeypatch, "user-456")
    _fill(hub, name="Bjørn B")  # a name typed for B; A's username and bio still there

    assert hub._save_profile() is False  # Preferanser stays open with the notice

    assert sent == []
    assert saved and saved[-1][0] == "Bjørn B"  # kept on this computer
    assert [tone for tone, _text in notices] == ["warning"]
    assert store[OWNER] == "user-123"


def test_save_sends_the_accounts_own_text(signed_in, profile_store, monkeypatch):
    _parent, hub, _corner = signed_in
    store, _saved = profile_store
    _client, sent, notices = _signed_in_as(hub, monkeypatch, "user-123")
    _fill(hub, bio="")  # cleared on purpose

    assert hub._save_profile() is True

    assert sent == [{"username": "anna", "display_name": "Anna A", "bio": ""}]
    assert notices == []


def test_saving_text_typed_before_any_sign_in_makes_it_the_accounts(signed_in, profile_store, monkeypatch):
    _parent, hub, _corner = signed_in
    store, _saved = profile_store
    _no_owner(hub, store)
    _client, sent, _notices = _signed_in_as(hub, monkeypatch, "user-789")
    _fill(hub)

    assert hub._save_profile() is True

    assert sent == [{"username": "anna", "display_name": "Anna A", "bio": "Mycologist in Oslo"}]
    assert store[OWNER] == "user-789"


def test_a_failed_save_does_not_change_whose_text_it_is(signed_in, profile_store, monkeypatch):
    _parent, hub, _corner = signed_in
    store, _saved = profile_store
    _no_owner(hub, store)
    client, _sent, _notices = _signed_in_as(hub, monkeypatch, "user-789")

    def unavailable(**_fields):
        raise cloud_sync.CloudSyncError("PATCH profiles: 503")

    client.update_profile = unavailable
    _fill(hub)

    assert hub._save_profile() is False
    assert store[OWNER] == "" and hub._profile_text_owner == ""  # still no account's



def test_switching_databases_does_not_change_whose_text_the_fields_hold(signed_in, profile_store, monkeypatch):
    """Codex: a new database has no owner; the fields still hold A's text."""
    _parent, hub, _corner = signed_in
    store, _saved = profile_store
    _fill(hub)
    store.clear()  # Preferanser › Database: another database, no profile owner
    _client, sent, _notices = _signed_in_as(hub, monkeypatch, "user-456")

    assert hub._save_profile() is False
    assert sent == []


def test_a_failed_load_is_tried_again(signed_in, profile_store):
    """Codex: a failed load counted as loaded, so it was never tried again."""
    _parent, hub, _corner = signed_in
    calls = []

    def fails_once():
        calls.append("fetch")
        if len(calls) == 1:
            _unavailable()
        return {"username": "bjorn", "display_name": "Bjørn B", "bio": ""}

    client = _another_account(hub, fetch_profile=fails_once)
    hub._artsobs_dialog._on_cloud_login_success(client, "new@example.com")
    hub._refresh_cloud_profile_fields()  # not forced: Preferanser asks again

    assert calls == ["fetch", "fetch"]
    assert _texts(hub)[:2] == ("bjorn", "Bjørn B")


def test_text_is_no_accounts_while_it_changes_hands(signed_in, profile_store, monkeypatch):
    """Codex: a failure between writing the text and its account misattributed it.

    The account is written "pending" first; if writing the new account fails,
    the stored text belongs to no account: replaced on the next load, never sent.
    """
    _parent, hub, _corner = signed_in
    store, _saved = profile_store
    _fill(hub)
    real_set = main_window.SettingsDB.set_setting

    def locked_for_the_new_owner(key, value):
        if key == OWNER and value == "user-456":
            raise RuntimeError("database is locked")
        real_set(key, value)

    monkeypatch.setattr(main_window.SettingsDB, "set_setting", locked_for_the_new_owner)
    other = _another_account(hub, fetch_profile=_bare_profile)
    monkeypatch.setattr(hub, "_cached_cloud_client", lambda: other)

    with pytest.raises(RuntimeError):
        hub._refresh_cloud_profile_fields(force=True)

    assert store[OWNER] == "pending"
    reopened = SimpleNamespace(_profile_text_owner=None)  # Preferanser opened again
    assert main_window._profile_text_belongs_to_another_account(reopened, "user-123")
    assert main_window._profile_text_belongs_to_another_account(reopened, "user-456")



def test_save_keeps_the_text_with_its_account_in_another_database(signed_in, profile_store, monkeypatch):
    """Codex: Save wrote A's text into a switched-to database without its account."""
    _parent, hub, _corner = signed_in
    store, saved = profile_store
    _fill(hub)
    store.clear()  # another database
    _signed_in_as(hub, monkeypatch, "user-456")

    hub._save_profile()

    assert saved and saved[-1][0] == "Anna A"
    assert store[OWNER] == "user-123"  # reopened there, it is still A's text


def test_an_interrupted_load_leaves_the_fields_no_accounts(signed_in, profile_store, monkeypatch):
    """Codex: the fields held B's text while Preferanser still recorded A."""
    _parent, hub, _corner = signed_in
    _fill(hub)

    def disk_full(*_args):
        raise OSError("disk full")

    monkeypatch.setattr(main_window.SettingsDB, "set_profile", disk_full)
    other = _another_account(hub, fetch_profile=lambda: {"username": "bjorn", "display_name": "Bjørn B", "bio": ""})
    monkeypatch.setattr(hub, "_cached_cloud_client", lambda: other)

    with pytest.raises(OSError):
        hub._refresh_cloud_profile_fields(force=True)

    assert _texts(hub)[:2] == ("bjorn", "Bjørn B")
    assert hub._profile_text_owner == "pending"
    assert main_window._profile_text_belongs_to_another_account(hub, "user-123")  # Save to A: blocked
