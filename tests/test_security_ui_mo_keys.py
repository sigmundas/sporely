"""F13: Mushroom Observer keys live in the OS keyring, not in the settings table."""
from __future__ import annotations

import pytest
from PySide6.QtWidgets import QDialog, QLineEdit, QWidget

from database.models import SettingsDB
from utils import mo_keys

APP = "mushroomobserver_app_api_key"
USER = "mushroomobserver_user_api_key"


class FakeKeyring:
    """In-memory stand-in; the real OS keychain is never touched."""

    def __init__(self):
        self.store = {}

    def get_password(self, service, account):
        return self.store.get((service, account))

    def set_password(self, service, account, value):
        self.store[(service, account)] = value

    def delete_password(self, service, account):
        self.store.pop((service, account), None)


@pytest.fixture(autouse=True)
def fake_keyring(monkeypatch):
    fake = FakeKeyring()
    monkeypatch.setattr(mo_keys, "_get_keyring_module", lambda: fake)
    for name in ("MO_APP_API_KEY", "MUSHROOMOBSERVER_APP_API_KEY", "MO_USER_API_KEY", "MUSHROOMOBSERVER_USER_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    SettingsDB.set_setting(APP, "")
    SettingsDB.set_setting(USER, "")
    return fake


def _settings_dialog_cls():
    from ui.main_window import ArtsobservasjonerSettingsDialog

    return ArtsobservasjonerSettingsDialog


def _make_dialog_stub(qtbot):
    cls = _settings_dialog_cls()

    class Stub(QWidget):
        SETTING_MO_APP_API_KEY = cls.SETTING_MO_APP_API_KEY
        SETTING_MO_USER_API_KEY = cls.SETTING_MO_USER_API_KEY
        _mushroomobserver_credentials = cls._mushroomobserver_credentials
        _prompt_mushroomobserver_login = cls._prompt_mushroomobserver_login

    widget = Stub()
    qtbot.addWidget(widget)
    return widget


def test_setting_names_unchanged():
    cls = _settings_dialog_cls()
    assert (cls.SETTING_MO_APP_API_KEY, cls.SETTING_MO_USER_API_KEY) == (APP, USER)


def test_saving_keys_leaves_no_value_in_settings_table(qtbot, monkeypatch, fake_keyring):
    widget = _make_dialog_stub(qtbot)

    def fake_exec(dialog):
        edits = dialog.findChildren(QLineEdit)
        edits[0].setText("app-secret-1")
        edits[1].setText("user-secret-1")
        return QDialog.Accepted

    monkeypatch.setattr(QDialog, "exec", fake_exec)
    assert widget._prompt_mushroomobserver_login(require_app_key=True) is True

    assert (SettingsDB.get_setting(APP, "") or "") == ""
    assert (SettingsDB.get_setting(USER, "") or "") == ""
    assert widget._mushroomobserver_credentials() == ("app-secret-1", "user-secret-1")
    assert set(fake_keyring.store.values()) == {"app-secret-1", "user-secret-1"}


def test_existing_database_value_is_migrated_and_blanked(qtbot, fake_keyring):
    SettingsDB.set_setting(APP, "legacy-app")
    SettingsDB.set_setting(USER, "legacy-user")
    widget = _make_dialog_stub(qtbot)

    assert widget._mushroomobserver_credentials() == ("legacy-app", "legacy-user")

    assert (SettingsDB.get_setting(APP, "") or "") == ""
    assert (SettingsDB.get_setting(USER, "") or "") == ""
    assert set(fake_keyring.store.values()) == {"legacy-app", "legacy-user"}
    # second read comes from the keyring alone
    assert widget._mushroomobserver_credentials() == ("legacy-app", "legacy-user")


def test_database_value_is_kept_when_keyring_unavailable(monkeypatch):
    monkeypatch.setattr(mo_keys, "_get_keyring_module", lambda: None)
    SettingsDB.set_setting(USER, "legacy-user")
    assert mo_keys.get_key(USER) == "legacy-user"
    assert SettingsDB.get_setting(USER, "") == "legacy-user"
    with pytest.raises(RuntimeError):
        mo_keys.set_key(USER, "new")


def test_clearing_a_key_removes_it(fake_keyring):
    mo_keys.set_key(USER, "u")
    mo_keys.set_key(USER, "")
    assert fake_keyring.store == {}
    assert mo_keys.get_key(USER) == ""


def test_observations_tab_reads_the_keyring(fake_keyring):
    from ui.observations_tab import ObservationsTab

    mo_keys.set_key(APP, "app-secret-2")
    mo_keys.set_key(USER, "user-secret-2")
    stub = type("Stub", (), {
        "SETTING_MO_APP_API_KEY": APP,
        "SETTING_MO_USER_API_KEY": USER,
    })()
    assert ObservationsTab._publish_target_logged_in(stub, "mo") is True
    assert ObservationsTab._publish_target_has_saved_login(stub, "mo") is True
    fake_keyring.store.clear()
    assert ObservationsTab._publish_target_logged_in(stub, "mo") is False


def test_keys_are_scoped_by_profile(monkeypatch, fake_keyring):
    import app_identity

    monkeypatch.setattr(app_identity, "runtime_profile_scope", lambda: "profile-a")
    mo_keys.set_key(USER, "key-a")
    monkeypatch.setattr(app_identity, "runtime_profile_scope", lambda: "profile-b")
    assert mo_keys.get_key(USER) == ""
    mo_keys.set_key(USER, "")  # sign-out in profile B must not touch profile A
    monkeypatch.setattr(app_identity, "runtime_profile_scope", lambda: "profile-a")
    assert mo_keys.get_key(USER) == "key-a"


def test_save_keeps_database_value_when_keyring_silently_drops_it(monkeypatch):
    class NullKeyring(FakeKeyring):
        def set_password(self, service, account, value):
            pass  # like keyring's null backend: accepts and forgets

    monkeypatch.setattr(mo_keys, "_get_keyring_module", lambda: NullKeyring())
    SettingsDB.set_setting(USER, "legacy-user")
    with pytest.raises(RuntimeError):
        mo_keys.set_key(USER, "new-user")
    assert SettingsDB.get_setting(USER, "") == "legacy-user"


def test_restored_legacy_value_is_blanked_when_keyring_has_the_key(fake_keyring):
    mo_keys.set_key(USER, "from-keyring")
    SettingsDB.set_setting(USER, "old-plaintext")
    assert mo_keys.get_key(USER) == "from-keyring"
    assert (SettingsDB.get_setting(USER, "") or "") == ""


def test_logout_blanks_database_even_without_keyring(monkeypatch):
    monkeypatch.setattr(mo_keys, "_get_keyring_module", lambda: None)
    SettingsDB.set_setting(USER, "legacy-user")
    mo_keys.set_key(USER, "")
    assert (SettingsDB.get_setting(USER, "") or "") == ""
