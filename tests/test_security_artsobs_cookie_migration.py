"""F20: legacy Artsobservasjoner cookie files must not come back after logout."""
import json
from pathlib import Path

from utils import artsobservasjoner_auto_login as aso
from utils.artsobservasjoner_auto_login import ArtsObservasjonerAuth

WEB = {".ASPXAUTHNO": "dummy-web-cookie"}
MOBILE = {"__Host-bff": "dummy-mobile-cookie"}


def _setup(tmp_path, monkeypatch):
    home = tmp_path / "home"
    legacy = home / ".myco_log" / "artsobservasjoner_cookies.json"
    legacy.parent.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: home))
    data = tmp_path / "data"
    data.mkdir()
    monkeypatch.setattr(aso, "app_data_dir", lambda: data)
    return legacy, data / "artsobservasjoner_cookies.json"


def test_logout_then_new_instance_loads_no_legacy_cookies(tmp_path, monkeypatch):
    legacy, shared = _setup(tmp_path, monkeypatch)
    legacy.write_text(json.dumps(WEB))
    shared.write_text(json.dumps(MOBILE))

    auth = ArtsObservasjonerAuth()
    assert auth.load_cookies("web") == WEB
    assert auth.load_cookies("mobile") == MOBILE

    auth.clear_cookies()
    again = ArtsObservasjonerAuth()
    assert again.load_cookies("web") is None
    assert again.load_cookies("mobile") is None
    assert not legacy.exists()


def test_target_logout_removes_legacy_sources_too(tmp_path, monkeypatch):
    legacy, shared = _setup(tmp_path, monkeypatch)
    legacy.write_text(json.dumps(WEB))

    auth = ArtsObservasjonerAuth()
    auth.clear_cookies("web")
    again = ArtsObservasjonerAuth()
    assert again.load_cookies("web") is None


def test_legacy_files_recreated_after_migration_are_not_imported_again(tmp_path, monkeypatch):
    legacy, shared = _setup(tmp_path, monkeypatch)
    legacy.write_text(json.dumps(WEB))
    auth = ArtsObservasjonerAuth()
    auth.clear_cookies()

    legacy.write_text(json.dumps(WEB))
    again = ArtsObservasjonerAuth()
    assert again.load_cookies("web") is None


def test_custom_cookie_path_does_not_consume_the_default_profiles_legacy_file(tmp_path, monkeypatch):
    legacy, _shared = _setup(tmp_path, monkeypatch)
    legacy.write_text(json.dumps(WEB))
    custom = tmp_path / "other" / "artsobservasjoner_cookies.json"

    auth = ArtsObservasjonerAuth(cookies_file=custom)
    assert auth.load_cookies("web") is None
    assert legacy.exists()
    auth.clear_cookies()
    assert legacy.exists()
