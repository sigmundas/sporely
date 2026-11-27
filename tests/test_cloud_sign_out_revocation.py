"""F09: explicit sign-out ends the session at Supabase, then clears local state.

``sign_out_cloud_session`` revokes the stored password session with
``POST /auth/v1/logout?scope=local`` and clears the local credentials whatever
the outcome; the result tells the caller whether the revocation landed. No
real network: ``requests.request`` is replaced by a recorder.
"""

import base64
import json
import time

import pytest
import requests

from utils import cloud_sync

USER_ID = "00000000-0000-4000-8000-0000000000a1"
LOGOUT_URL = f"{cloud_sync.SUPABASE_URL}/auth/v1/logout?scope=local"
REFRESH_URL = f"{cloud_sync.SUPABASE_URL}/auth/v1/token?grant_type=refresh_token"


def _jwt(*, sub=USER_ID, exp_offset=3600) -> str:
    def enc(obj):
        raw = json.dumps(obj, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    return f"{enc({'alg': 'HS256', 'typ': 'JWT'})}.{enc({'sub': sub, 'exp': int(time.time()) + exp_offset})}.sig"


class _Resp:
    def __init__(self, status_code=204, payload=None, text=""):
        self.status_code = status_code
        self.ok = 200 <= status_code < 300
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload


@pytest.fixture
def store(monkeypatch):
    data: dict = {}
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(data))

    def _update(updates):
        data.update(updates)
        return dict(data)

    monkeypatch.setattr(cloud_sync, "update_app_settings", _update)
    password_clears = []
    monkeypatch.setattr(cloud_sync, "clear_saved_cloud_password", lambda: password_clears.append(True) or [])
    monkeypatch.setattr(cloud_sync, "_sleep_supabase_backoff", lambda attempt: None)
    data["_password_clears"] = password_clears
    return data


def _seed(store, *, access, refresh="refresh-a", method=None):
    store.update(
        {
            "cloud_access_token": access,
            "cloud_user_id": USER_ID,
            "cloud_refresh_token": refresh,
            "cloud_user_email": "tester@example.invalid",
            "cloud_auth_method": method,
            "linked_cloud_user_id": USER_ID,
        }
    )


def _recorder(monkeypatch, responder):
    calls = []

    def fake_request(method, url, **kwargs):
        calls.append({"method": method, "url": url, **kwargs})
        return responder(method, url, kwargs)

    monkeypatch.setattr(requests, "request", fake_request)
    return calls


def _assert_cleared(store):
    for key in ("cloud_access_token", "cloud_user_id", "cloud_refresh_token", "cloud_auth_method", "cloud_user_email"):
        assert store[key] is None, key
    assert store["_password_clears"], "saved password must be cleared"
    assert store["linked_cloud_user_id"] == USER_ID, "sign-out must not unlink the database"


def test_sign_out_revokes_current_password_session_then_clears(monkeypatch, store):
    access = _jwt()
    _seed(store, access=access)
    calls = _recorder(monkeypatch, lambda m, u, k: _Resp(204))

    result = cloud_sync.sign_out_cloud_session()

    assert [(c["method"], c["url"]) for c in calls] == [("POST", LOGOUT_URL)]
    assert calls[0]["headers"]["Authorization"] == f"Bearer {access}"
    assert calls[0]["headers"]["apikey"] == cloud_sync.SUPABASE_KEY
    assert result.revoked is True
    assert result.status == "revoked"
    _assert_cleared(store)


def test_sign_out_clears_local_state_when_revocation_cannot_be_sent(monkeypatch, store):
    _seed(store, access=_jwt())

    def offline(method, url, kwargs):
        raise requests.ConnectionError("offline")

    calls = _recorder(monkeypatch, offline)

    result = cloud_sync.sign_out_cloud_session()

    assert calls and all(c["url"] == LOGOUT_URL for c in calls)
    assert result.revoked is False
    assert result.status == "failed"
    assert result.message
    _assert_cleared(store)


def test_sign_out_clears_local_state_when_revocation_is_rejected(monkeypatch, store):
    _seed(store, access=_jwt())
    _recorder(monkeypatch, lambda m, u, k: _Resp(403, text="forbidden"))

    result = cloud_sync.sign_out_cloud_session()

    assert result.revoked is False
    assert result.status == "failed"
    _assert_cleared(store)


def test_sign_out_treats_already_ended_session_as_revoked(monkeypatch, store):
    _seed(store, access=_jwt())
    _recorder(monkeypatch, lambda m, u, k: _Resp(404, text='{"code":"session_not_found"}'))

    result = cloud_sync.sign_out_cloud_session()

    assert result.revoked is True
    _assert_cleared(store)


def test_sign_out_refreshes_an_expired_access_token_before_revoking(monkeypatch, store):
    _seed(store, access=_jwt(exp_offset=-600), refresh="refresh-old")
    fresh = _jwt()

    def responder(method, url, kwargs):
        if url == REFRESH_URL:
            return _Resp(200, {"access_token": fresh, "refresh_token": "refresh-new", "user": {"id": USER_ID}})
        return _Resp(204)

    calls = _recorder(monkeypatch, responder)

    result = cloud_sync.sign_out_cloud_session()

    assert [c["url"] for c in calls] == [REFRESH_URL, LOGOUT_URL]
    assert calls[0]["json"] == {"refresh_token": "refresh-old"}
    assert calls[1]["headers"]["Authorization"] == f"Bearer {fresh}"
    assert result.revoked is True
    _assert_cleared(store)


def test_sign_out_of_oauth_session_clears_and_reports_revocation_unsupported(monkeypatch, store):
    _seed(store, access=_jwt(), method="oauth")
    calls = _recorder(monkeypatch, lambda m, u, k: _Resp(204))

    result = cloud_sync.sign_out_cloud_session()

    assert calls == []
    assert result.revoked is False
    assert result.status == "not_supported"
    _assert_cleared(store)


def test_sign_out_without_a_stored_session_sends_nothing(monkeypatch, store):
    store.update({"cloud_access_token": None, "cloud_refresh_token": None, "cloud_user_id": None})
    calls = _recorder(monkeypatch, lambda m, u, k: _Resp(204))

    result = cloud_sync.sign_out_cloud_session()

    assert calls == []
    assert result.status == "no_session"
    assert store["cloud_access_token"] is None


def test_clear_credentials_alone_still_sends_nothing(monkeypatch, store):
    """Account reset and forced re-login keep the local-only clear."""
    _seed(store, access=_jwt())
    calls = _recorder(monkeypatch, lambda m, u, k: _Resp(204))

    cloud_sync.SporelyCloudClient.clear_credentials()

    assert calls == []
    _assert_cleared(store)
