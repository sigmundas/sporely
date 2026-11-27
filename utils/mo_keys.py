"""Mushroom Observer API keys, kept in the OS keyring instead of the settings table.

The setting names are unchanged (``mushroomobserver_app_api_key`` and
``mushroomobserver_user_api_key``); they are the keyring account names, and the
database row is only read to migrate an older value once and is then blanked.
"""
from __future__ import annotations

import os

SETTING_MO_APP_API_KEY = "mushroomobserver_app_api_key"
SETTING_MO_USER_API_KEY = "mushroomobserver_user_api_key"
MO_KEYRING_SERVICE = "Sporely.MushroomObserver"


def _get_keyring_module():
    try:
        import keyring  # type: ignore

        return keyring
    except Exception:
        return None


def _db_get(name: str) -> str:
    from database.models import SettingsDB

    return (SettingsDB.get_setting(name, "") or "").strip()


def _db_set(name: str, value: str) -> None:
    from database.models import SettingsDB

    SettingsDB.set_setting(name, value)


def get_key(name: str) -> str:
    """Return the stored key; migrate a legacy database value into the keyring once."""
    keyring = _get_keyring_module()
    if keyring is not None:
        try:
            value = (keyring.get_password(MO_KEYRING_SERVICE, name) or "").strip()
        except Exception:
            value = ""
        if value:
            return value
    try:
        legacy = _db_get(name)
    except Exception:
        return ""
    if not legacy:
        return ""
    if keyring is not None:
        try:
            keyring.set_password(MO_KEYRING_SERVICE, name, legacy)
            if (keyring.get_password(MO_KEYRING_SERVICE, name) or "").strip() == legacy:
                _db_set(name, "")
        except Exception:
            pass  # keep the database value rather than lose the key
    return legacy


def set_key(name: str, value: str) -> None:
    """Save ``value`` in the keyring (empty deletes it) and blank any database row."""
    value = (value or "").strip()
    keyring = _get_keyring_module()
    if keyring is None:
        raise RuntimeError("Secure key storage is unavailable on this system.")
    try:
        if value:
            keyring.set_password(MO_KEYRING_SERVICE, name, value)
        else:
            try:
                keyring.delete_password(MO_KEYRING_SERVICE, name)
            except Exception as exc:
                if exc.__class__.__name__ != "PasswordDeleteError":
                    raise
    except Exception as exc:
        raise RuntimeError(f"Could not securely save the key: {exc}") from exc
    _db_set(name, "")


def credentials() -> tuple[str, str]:
    """Return (app_key, user_key): keyring first, then the environment."""
    app_key = (
        get_key(SETTING_MO_APP_API_KEY)
        or (os.getenv("MO_APP_API_KEY", "") or "").strip()
        or (os.getenv("MUSHROOMOBSERVER_APP_API_KEY", "") or "").strip()
    )
    user_key = (
        get_key(SETTING_MO_USER_API_KEY)
        or (os.getenv("MO_USER_API_KEY", "") or "").strip()
        or (os.getenv("MUSHROOMOBSERVER_USER_API_KEY", "") or "").strip()
    )
    return app_key, user_key
