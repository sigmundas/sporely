"""F20: legacy Artsobservasjoner cookie files must not come back after logout."""
import json
from pathlib import Path

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
    return legacy, data / "artsobservasjoner_cookies.json"


def test_logout_then_new_instance_loads_no_legacy_cookies(tmp_path, monkeypatch):
    legacy, shared = _setup(tmp_path, monkeypatch)
    legacy.write_text(json.dumps(WEB))
    shared.write_text(json.dumps(MOBILE))

    auth = ArtsObservasjonerAuth(cookies_file=shared)
    assert auth.load_cookies("web") == WEB
    assert auth.load_cookies("mobile") == MOBILE

    auth.clear_cookies()
    again = ArtsObservasjonerAuth(cookies_file=shared)
    assert again.load_cookies("web") is None
    assert again.load_cookies("mobile") is None


def test_target_logout_removes_legacy_sources_too(tmp_path, monkeypatch):
    legacy, shared = _setup(tmp_path, monkeypatch)
    legacy.write_text(json.dumps(WEB))

    auth = ArtsObservasjonerAuth(cookies_file=shared)
    auth.clear_cookies("web")
    again = ArtsObservasjonerAuth(cookies_file=shared)
    assert again.load_cookies("web") is None


def test_migration_runs_once_and_removes_the_sources(tmp_path, monkeypatch):
    legacy, shared = _setup(tmp_path, monkeypatch)
    legacy.write_text(json.dumps(WEB))
    ArtsObservasjonerAuth(cookies_file=shared)
    assert not legacy.exists()
    # A second construction keeps the migrated cookies.
    auth = ArtsObservasjonerAuth(cookies_file=shared)
    assert auth.load_cookies("web") == WEB
