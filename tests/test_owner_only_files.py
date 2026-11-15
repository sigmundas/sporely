"""Files holding tokens or session cookies are owner-only (0600)."""

from __future__ import annotations

import json
import os
import stat
import sys

import pytest

from database import schema
from utils.artportalen_auth import ArtportalenAuth
from utils.artsobservasjoner_auto_login import ArtsObservasjonerAuth
from utils.private_files import write_private_json

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX file modes")


def _mode(path) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


@pytest.fixture
def loose_umask():
    previous = os.umask(0o022)
    yield
    os.umask(previous)


def test_new_private_file_is_owner_only(tmp_path, loose_umask):
    path = tmp_path / "nested" / "secret.json"
    write_private_json(path, {"token": "t"})

    assert _mode(path) == 0o600
    assert json.loads(path.read_text(encoding="utf-8")) == {"token": "t"}


def test_existing_readable_file_is_tightened(tmp_path, loose_umask):
    path = tmp_path / "secret.json"
    path.write_text("{}", encoding="utf-8")
    os.chmod(path, 0o644)

    write_private_json(path, {"token": "t"})

    assert _mode(path) == 0o600


def test_app_settings_with_cloud_tokens_are_owner_only(loose_umask):
    path = schema.SETTINGS_PATH
    original = path.read_bytes() if path.exists() else None
    try:
        if path.exists():
            os.chmod(path, 0o644)
        schema.update_app_settings({"cloud_refresh_token": "refresh-secret"})

        assert _mode(path) == 0o600
        assert schema.get_app_settings()["cloud_refresh_token"] == "refresh-secret"
    finally:
        if original is None:
            path.unlink(missing_ok=True)
        else:
            path.write_bytes(original)


def test_artsobservasjoner_cookies_are_owner_only(tmp_path, monkeypatch, loose_umask):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    auth = ArtsObservasjonerAuth(cookies_file=tmp_path / "ao_cookies.json")

    auth.save_cookies({"__Host-bff": "session"}, target="mobile")
    auth.save_cookies({".ASPXAUTH": "session"}, target="web")

    for target in ("mobile", "web"):
        assert _mode(auth._cookies_path(target)) == 0o600
    assert auth.load_cookies("mobile") == {"__Host-bff": "session"}


def test_artportalen_cookies_are_owner_only(tmp_path, loose_umask):
    auth = ArtportalenAuth(cookies_file=tmp_path / "ap_cookies.json")

    auth.save_cookies({"session": "value"})

    assert _mode(auth.cookies_file) == 0o600
    assert auth.load_cookies() == {"session": "value"}


def test_settings_file_keeps_its_json_layout(tmp_path):
    path = tmp_path / "s.json"
    write_private_json(path, {"a": 1})
    assert path.read_text(encoding="utf-8") == '{\n  "a": 1\n}'
