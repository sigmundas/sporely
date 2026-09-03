"""Tests for Stage 5: OAuth session integration in OAuthSporelyCloudClient.

Covers construction, persistence, reload, refresh routing, token rotation,
error mapping, and legacy compatibility.
"""
from __future__ import annotations

import base64
import json
import time
from types import SimpleNamespace

import pytest

import utils.cloud_sync as cloud_sync
from utils.cloud_sync import (
    OAuthSporelyCloudClient,
    SporelyCloudClient,
    CloudReauthRequiredError,
    CloudTemporarilyUnavailableError,
    CloudSyncError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _b64url(payload: bytes) -> str:
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


def _make_jwt(*, sub: str = "user-abc", exp_offset: int = 3600) -> str:
    header = _b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = _b64url(json.dumps({"sub": sub, "exp": int(time.time()) + exp_offset}).encode())
    sig = _b64url(b"sig")
    return f"{header}.{payload}.{sig}"


def _make_oauth_result(
    *,
    access_token: str | None = None,
    refresh_token: str = "refresh-tok",
    user_id: str | None = None,
    user_email: str | None = "user@example.com",
) -> SimpleNamespace:
    """Minimal stand-in for OAuthTokenResult."""
    return SimpleNamespace(
        access_token=access_token or _make_jwt(),
        refresh_token=refresh_token,
        token_type="bearer",
        expires_at=int(time.time()) + 3600,
        user_id=user_id,
        user_email=user_email,
    )


def _settings_store() -> dict:
    return {}


def _patch_settings(monkeypatch, store: dict) -> dict:
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: store)
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda updates: store.update(updates))
    return store


# ---------------------------------------------------------------------------
# Test 1: construct from OAuthTokenResult
# ---------------------------------------------------------------------------

def test_from_oauth_session_constructs_client():
    result = _make_oauth_result()
    client = OAuthSporelyCloudClient.from_oauth_session(result)
    assert isinstance(client, OAuthSporelyCloudClient)
    assert client.access_token == result.access_token
    assert client.user_id  # resolved from JWT sub
    assert client.user_email == "user@example.com"


def test_from_oauth_session_uses_jwt_sub_as_authoritative_user_id():
    sub = "jwt-user-uuid"
    access_token = _make_jwt(sub=sub)
    result = _make_oauth_result(access_token=access_token, user_id="server-user-id")
    client = OAuthSporelyCloudClient.from_oauth_session(result)
    assert client.user_id == sub


def test_from_oauth_session_falls_back_to_server_user_id_when_jwt_has_no_sub():
    # Build a JWT with no sub claim
    header = _b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = _b64url(json.dumps({"exp": int(time.time()) + 3600}).encode())
    access_token = f"{header}.{payload}.{_b64url(b'sig')}"
    result = _make_oauth_result(access_token=access_token, user_id="fallback-user-id")
    client = OAuthSporelyCloudClient.from_oauth_session(result)
    assert client.user_id == "fallback-user-id"


def test_from_oauth_session_raises_on_empty_access_token():
    result = SimpleNamespace(
        access_token="",
        refresh_token="refresh-tok",
        token_type="bearer",
        expires_at=int(time.time()) + 3600,
        user_id="user-abc",
        user_email="user@example.com",
    )
    with pytest.raises(CloudSyncError, match="missing an access token"):
        OAuthSporelyCloudClient.from_oauth_session(result)


def test_from_oauth_session_raises_on_missing_user_identity():
    header = _b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = _b64url(json.dumps({"exp": int(time.time()) + 3600}).encode())
    access_token = f"{header}.{payload}.{_b64url(b'sig')}"
    result = _make_oauth_result(access_token=access_token, user_id=None)
    with pytest.raises(CloudSyncError, match="missing user identity"):
        OAuthSporelyCloudClient.from_oauth_session(result)


# ---------------------------------------------------------------------------
# Test 2: save_credentials persists cloud_auth_method=oauth
# ---------------------------------------------------------------------------

def test_save_credentials_persists_oauth_auth_method(monkeypatch):
    store = _patch_settings(monkeypatch, {})
    result = _make_oauth_result()
    client = OAuthSporelyCloudClient.from_oauth_session(result)
    client.save_credentials()
    assert store.get("cloud_auth_method") == "oauth"
    assert store.get("cloud_access_token") == client.access_token
    assert store.get("cloud_user_id") == client.user_id
    assert store.get("cloud_refresh_token") == client.refresh_token


def test_save_credentials_persists_email_when_provided(monkeypatch):
    store = _patch_settings(monkeypatch, {})
    result = _make_oauth_result()
    client = OAuthSporelyCloudClient.from_oauth_session(result)
    client.save_credentials(email="me@example.com")
    assert store.get("cloud_user_email") == "me@example.com"


def test_save_credentials_does_not_write_password(monkeypatch):
    save_calls: list = []
    store = _patch_settings(monkeypatch, {})
    monkeypatch.setattr(cloud_sync, "save_cloud_password", lambda *a: save_calls.append(a))
    result = _make_oauth_result()
    client = OAuthSporelyCloudClient.from_oauth_session(result)
    client.save_credentials(email="me@example.com", password="secret", remember_password=True)
    assert save_calls == [], "OAuth save_credentials must never write a password"


# ---------------------------------------------------------------------------
# Test 3: OAuth session survives reload via from_stored_credentials
# ---------------------------------------------------------------------------

def test_oauth_session_survives_reload(monkeypatch):
    jwt = _make_jwt(sub="user-abc", exp_offset=7200)
    store = {
        "cloud_auth_method": "oauth",
        "cloud_access_token": jwt,
        "cloud_user_id": "user-abc",
        "cloud_refresh_token": "refresh-stored",
    }
    _patch_settings(monkeypatch, store)
    client = OAuthSporelyCloudClient.from_stored_credentials()
    assert client is not None
    assert isinstance(client, OAuthSporelyCloudClient)
    assert client.user_id == "user-abc"


# ---------------------------------------------------------------------------
# Test 4 & 5: OAuth refresh uses SporelyDesktopOAuthClient, not legacy path
# ---------------------------------------------------------------------------

def test_refresh_login_calls_oauth_client_not_legacy(monkeypatch):
    refresh_calls: list[str] = []
    legacy_calls: list = []

    class FakeOAuthClient:
        def refresh(self, token: str):
            refresh_calls.append(token)
            new_jwt = _make_jwt(sub="user-abc")
            return SimpleNamespace(
                access_token=new_jwt,
                refresh_token="new-refresh",
                user_id="user-abc",
                user_email=None,
            )

    import utils.sporely_cloud_auth as sca
    monkeypatch.setattr(sca, "SporelyDesktopOAuthClient", FakeOAuthClient)

    # Patch the legacy password-grant path to detect if it's called
    import requests as _requests
    monkeypatch.setattr(_requests, "request", lambda *a, **kw: legacy_calls.append(a) or SimpleNamespace(ok=False, status_code=500, text=""))

    client = OAuthSporelyCloudClient.refresh_login("old-refresh-token")

    assert refresh_calls == ["old-refresh-token"]
    assert legacy_calls == [], "Must not call legacy password-grant path"
    assert isinstance(client, OAuthSporelyCloudClient)


def test_oauth_session_does_not_call_legacy_refresh_login(monkeypatch):
    """_refresh_session_if_possible on OAuthSporelyCloudClient uses OAuthSporelyCloudClient.refresh_login."""
    legacy_calls: list = []

    original_refresh = SporelyCloudClient.refresh_login.__func__

    def tracking_legacy(cls, token):
        legacy_calls.append(token)
        return original_refresh(cls, token)

    # Only patch the base class method, not the subclass
    monkeypatch.setattr(SporelyCloudClient, "refresh_login", classmethod(tracking_legacy))

    refresh_calls: list[str] = []

    class FakeOAuthClient:
        def refresh(self, token: str):
            refresh_calls.append(token)
            new_jwt = _make_jwt(sub="user-abc")
            return SimpleNamespace(
                access_token=new_jwt,
                refresh_token="rotated",
                user_id="user-abc",
                user_email=None,
            )

    import utils.sporely_cloud_auth as sca
    monkeypatch.setattr(sca, "SporelyDesktopOAuthClient", FakeOAuthClient)

    OAuthSporelyCloudClient.refresh_login("some-refresh")
    assert legacy_calls == [], "OAuthSporelyCloudClient.refresh_login must not call base class legacy path"
    assert refresh_calls == ["some-refresh"]


# ---------------------------------------------------------------------------
# Test 6 & 7: access-token and refresh-token rotation
# ---------------------------------------------------------------------------

def test_refresh_login_rotates_access_and_refresh_tokens(monkeypatch):
    new_jwt = _make_jwt(sub="user-abc")

    class FakeOAuthClient:
        def refresh(self, token: str):
            return SimpleNamespace(
                access_token=new_jwt,
                refresh_token="rotated-refresh",
                user_id="user-abc",
                user_email=None,
            )

    import utils.sporely_cloud_auth as sca
    monkeypatch.setattr(sca, "SporelyDesktopOAuthClient", FakeOAuthClient)

    client = OAuthSporelyCloudClient.refresh_login("old-refresh")
    assert client.access_token == new_jwt
    assert client.refresh_token == "rotated-refresh"


# ---------------------------------------------------------------------------
# Test 8: empty refresh token preserves previous stored token (critical)
# ---------------------------------------------------------------------------

def test_refresh_login_preserves_old_token_when_server_returns_empty(monkeypatch):
    """Server returning empty refresh_token must not replace the stored token with ''."""
    new_jwt = _make_jwt(sub="user-abc")
    old_refresh = "old-refresh-token"

    class FakeOAuthClient:
        def refresh(self, token: str):
            return SimpleNamespace(
                access_token=new_jwt,
                refresh_token="",   # server did not rotate
                user_id="user-abc",
                user_email=None,
            )

    import utils.sporely_cloud_auth as sca
    monkeypatch.setattr(sca, "SporelyDesktopOAuthClient", FakeOAuthClient)

    client = OAuthSporelyCloudClient.refresh_login(old_refresh)
    assert client.refresh_token == old_refresh, (
        "When server returns empty refresh_token, old token must be preserved"
    )
    assert client.access_token == new_jwt


# ---------------------------------------------------------------------------
# Test 9: OAuthError -> CloudReauthRequiredError
# ---------------------------------------------------------------------------

def test_refresh_login_maps_oauth_error_to_reauth_required(monkeypatch):
    import utils.sporely_cloud_auth as sca

    class FakeOAuthClient:
        def refresh(self, token: str):
            raise sca.OAuthError("Refresh token is invalid or has been revoked.")

    monkeypatch.setattr(sca, "SporelyDesktopOAuthClient", FakeOAuthClient)

    with pytest.raises(CloudReauthRequiredError, match="invalid_grant"):
        OAuthSporelyCloudClient.refresh_login("dead-refresh")


# ---------------------------------------------------------------------------
# Test 10: RuntimeError -> CloudTemporarilyUnavailableError
# ---------------------------------------------------------------------------

def test_refresh_login_maps_runtime_error_to_temporarily_unavailable(monkeypatch):
    import utils.sporely_cloud_auth as sca

    class FakeOAuthClient:
        def refresh(self, token: str):
            raise RuntimeError("Token refresh request failed (ConnectionError).")

    monkeypatch.setattr(sca, "SporelyDesktopOAuthClient", FakeOAuthClient)

    with pytest.raises(CloudTemporarilyUnavailableError):
        OAuthSporelyCloudClient.refresh_login("some-refresh")


# ---------------------------------------------------------------------------
# Test 11: legacy stored session still uses legacy refresh behavior
# ---------------------------------------------------------------------------

def test_legacy_session_not_routed_to_oauth_client(monkeypatch):
    jwt = _make_jwt(sub="user-legacy", exp_offset=7200)
    store = {
        # No cloud_auth_method key -> legacy behavior
        "cloud_access_token": jwt,
        "cloud_user_id": "user-legacy",
        "cloud_refresh_token": "legacy-refresh",
    }
    _patch_settings(monkeypatch, store)

    client = SporelyCloudClient.from_stored_credentials()
    assert client is not None
    # Must be the base class, not OAuthSporelyCloudClient
    assert type(client) is SporelyCloudClient


def test_legacy_session_with_password_auth_method_not_routed_to_oauth(monkeypatch):
    jwt = _make_jwt(sub="user-pw", exp_offset=7200)
    store = {
        "cloud_auth_method": "password",
        "cloud_access_token": jwt,
        "cloud_user_id": "user-pw",
        "cloud_refresh_token": "pw-refresh",
    }
    _patch_settings(monkeypatch, store)

    client = SporelyCloudClient.from_stored_credentials()
    assert client is not None
    assert type(client) is SporelyCloudClient


# ---------------------------------------------------------------------------
# Test 12: base class from_stored_credentials routes to OAuthSporelyCloudClient
# ---------------------------------------------------------------------------

def test_base_class_routes_oauth_stored_session_to_oauth_subclass(monkeypatch):
    jwt = _make_jwt(sub="user-oauth", exp_offset=7200)
    store = {
        "cloud_auth_method": "oauth",
        "cloud_access_token": jwt,
        "cloud_user_id": "user-oauth",
        "cloud_refresh_token": "oauth-refresh",
    }
    _patch_settings(monkeypatch, store)

    client = SporelyCloudClient.from_stored_credentials()
    assert client is not None
    assert isinstance(client, OAuthSporelyCloudClient)


# ---------------------------------------------------------------------------
# Test 13: no password is read or written
# ---------------------------------------------------------------------------

def test_oauth_from_stored_credentials_never_calls_load_saved_cloud_password(monkeypatch):
    load_calls: list = []
    jwt = _make_jwt(sub="user-oauth", exp_offset=7200)
    store = {
        "cloud_auth_method": "oauth",
        "cloud_access_token": jwt,
        "cloud_user_id": "user-oauth",
        "cloud_refresh_token": "oauth-refresh",
    }
    _patch_settings(monkeypatch, store)
    monkeypatch.setattr(cloud_sync, "load_saved_cloud_password", lambda: load_calls.append(1) or ("", "", False))

    OAuthSporelyCloudClient.from_stored_credentials()
    assert load_calls == [], "OAuth from_stored_credentials must not read saved passwords"


# ---------------------------------------------------------------------------
# Test 14: cloud operations work with OAuth-issued client
# ---------------------------------------------------------------------------

class _FakeResponse:
    def __init__(self, ok: bool, status_code: int, text: str, payload=None):
        self.ok = ok
        self.status_code = status_code
        self.text = text
        self._payload = payload
        self.content = text.encode("utf-8")
        self.headers = {}

    def json(self):
        if self._payload is None:
            raise ValueError("No JSON")
        return self._payload


def test_oauth_client_get_works_with_mocked_request(monkeypatch):
    jwt = _make_jwt(sub="user-abc")
    client = OAuthSporelyCloudClient(access_token=jwt, user_id="user-abc", refresh_token="ref")

    rows = [{"id": "obs-1"}]

    def fake_request_with_refresh(method, url, *, refresh_on_auth_error=True, **kwargs):
        return _FakeResponse(True, 200, json.dumps(rows), payload=rows)

    monkeypatch.setattr(client, "_request_with_refresh", fake_request_with_refresh)

    result = client._get("observations?limit=1&select=id")
    assert result == rows


def test_oauth_client_fetch_current_user_id(monkeypatch):
    jwt = _make_jwt(sub="user-abc")
    client = OAuthSporelyCloudClient(access_token=jwt, user_id="user-abc", refresh_token="ref")

    user_payload = {"id": "user-abc", "email": "user@example.com"}

    def fake_request_with_refresh(method, url, *, refresh_on_auth_error=True, **kwargs):
        return _FakeResponse(True, 200, json.dumps(user_payload), payload=user_payload)

    monkeypatch.setattr(client, "_request_with_refresh", fake_request_with_refresh)

    uid = client.fetch_current_user_id()
    assert uid == "user-abc"


# ---------------------------------------------------------------------------
# Test: reauth propagates from from_stored_credentials (no silent None)
# ---------------------------------------------------------------------------

def test_oauth_from_stored_credentials_propagates_reauth_required(monkeypatch):
    """When only a refresh_token is stored and it is dead, CloudReauthRequiredError must propagate."""
    store = {
        "cloud_auth_method": "oauth",
        "cloud_access_token": None,
        "cloud_user_id": None,
        "cloud_refresh_token": "dead-refresh",
    }
    _patch_settings(monkeypatch, store)

    import utils.sporely_cloud_auth as sca

    class FakeOAuthClient:
        def refresh(self, token: str):
            raise sca.OAuthError("revoked")

    monkeypatch.setattr(sca, "SporelyDesktopOAuthClient", FakeOAuthClient)

    with pytest.raises(CloudReauthRequiredError):
        OAuthSporelyCloudClient.from_stored_credentials()
