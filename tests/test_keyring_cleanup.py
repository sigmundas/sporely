from __future__ import annotations

from types import SimpleNamespace

from utils import artportalen_auth, artsobservasjoner_auto_login, cloud_sync, keyring_cleanup


class _PasswordDeleteError(Exception):
    pass


class _FakeKeyring:
    errors = SimpleNamespace(PasswordDeleteError=_PasswordDeleteError)

    def __init__(self, failures: dict[str, str] | None = None):
        self.failures = dict(failures or {})
        self.deleted: list[tuple[str, str]] = []
        self.reads: list[tuple[str, str]] = []

    def delete_password(self, service: str, account: str) -> None:
        self.deleted.append((service, account))
        message = self.failures.get(service)
        if message is not None:
            raise _PasswordDeleteError(message)

    def get_password(self, service: str, account: str):
        self.reads.append((service, account))
        return "secret"


def test_delete_password_entries_ignores_absence_but_reports_denial():
    keyring = _FakeKeyring(
        {
            "missing": "Item not found",
            "denied": "User interaction is not allowed",
        }
    )

    failures = keyring_cleanup.delete_password_entries(
        keyring,
        (("missing", "account"), ("denied", "account"), ("present", "account")),
    )

    assert keyring.deleted == [
        ("missing", "account"),
        ("denied", "account"),
        ("present", "account"),
    ]
    assert failures == ["denied: User interaction is not allowed"]


def test_artportalen_does_not_read_keyring_without_saved_username(monkeypatch):
    keyring = _FakeKeyring()
    monkeypatch.setattr(artportalen_auth, "_get_saved_username", lambda: "")
    monkeypatch.setattr(artportalen_auth, "_get_keyring_module", lambda: keyring)

    assert artportalen_auth._load_saved_credentials() == ("", None, True)
    assert keyring.reads == []


def test_artsobservasjoner_does_not_read_keyring_without_saved_username(monkeypatch):
    keyring = _FakeKeyring()
    monkeypatch.setattr(artsobservasjoner_auto_login, "_get_saved_web_username", lambda: "")
    monkeypatch.setattr(artsobservasjoner_auto_login, "_get_keyring_module", lambda: keyring)

    assert artsobservasjoner_auto_login._load_saved_web_credentials() == ("", None, True)
    assert keyring.reads == []


def test_clear_artportalen_credentials_targets_current_and_legacy_entries(monkeypatch):
    keyring = _FakeKeyring()
    saved_usernames: list[str] = []
    monkeypatch.setattr(artportalen_auth, "_get_keyring_module", lambda: keyring)
    monkeypatch.setattr(
        artportalen_auth,
        "_set_saved_username",
        lambda value: saved_usernames.append(value),
    )

    assert artportalen_auth.clear_saved_credentials() == []
    assert saved_usernames == [""]
    assert keyring.deleted == [
        ("Sporely.Artportalen", "password"),
        ("MycoLog.Artportalen", "password"),
    ]


def test_clear_artsobservasjoner_credentials_targets_current_and_legacy_entries(monkeypatch):
    keyring = _FakeKeyring()
    saved_usernames: list[str] = []
    monkeypatch.setattr(artsobservasjoner_auto_login, "_get_keyring_module", lambda: keyring)
    monkeypatch.setattr(
        artsobservasjoner_auto_login,
        "_set_saved_web_username",
        lambda value: saved_usernames.append(value),
    )

    assert artsobservasjoner_auto_login.clear_saved_web_credentials() == []
    assert saved_usernames == [""]
    assert keyring.deleted == [
        ("Sporely.Artsobservasjoner", "web_password"),
        ("MycoLog.Artsobservasjoner", "web_password"),
    ]


def test_clear_cloud_password_targets_current_and_legacy_entries(monkeypatch):
    keyring = _FakeKeyring()
    monkeypatch.setattr(cloud_sync, "_get_keyring_module", lambda: keyring)
    monkeypatch.setattr(cloud_sync, "_CLOUD_KEYRING_ACCOUNT", "password:test")

    assert cloud_sync.clear_saved_cloud_password() == []
    assert keyring.deleted == [
        ("Sporely.Cloud", "password:test"),
        ("MycoLog.Cloud", "password:test"),
    ]
