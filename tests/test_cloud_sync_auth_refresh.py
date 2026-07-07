from __future__ import annotations

import base64
import json
import time

import pytest
from types import SimpleNamespace

import utils.cloud_sync as cloud_sync


def _b64url(payload: bytes) -> str:
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


def _make_jwt(*, sub: str = "user-123", exp_offset_seconds: int = 3600, kid: str = "k1") -> str:
    """Build a fake JWT whose ``exp`` claim is *exp_offset_seconds* from now.

    Only the payload segment matters for our decoder — the header and
    signature are placeholders so tests do not depend on real crypto.
    """
    header = _b64url(json.dumps({"alg": "HS256", "typ": "JWT", "kid": kid}).encode("utf-8"))
    payload = _b64url(
        json.dumps({"sub": sub, "exp": int(time.time()) + int(exp_offset_seconds)}).encode("utf-8")
    )
    signature = _b64url(f"sig-{kid}".encode("utf-8"))
    return f"{header}.{payload}.{signature}"


class _FakeResponse:
    def __init__(self, ok: bool, status_code: int, text: str, payload=None):
        self.ok = ok
        self.status_code = status_code
        self.text = text
        self._payload = payload
        self.content = text.encode("utf-8")

    def json(self):
        if self._payload is None:
            raise ValueError("No JSON payload")
        return self._payload


def test_cloud_client_refreshes_expired_token_and_retries_get(monkeypatch):
    client = cloud_sync.SporelyCloudClient("expired-token", "user-123", "refresh-token")
    requests_seen: list[tuple[str, str, str | None]] = []
    responses = iter(
        [
            _FakeResponse(
                False,
                401,
                '{"code":"PGRST303","message":"JWT expired"}',
            ),
            _FakeResponse(True, 200, '[{"id":"obs-1"}]', payload=[{"id": "obs-1"}]),
        ]
    )

    def fake_request(method, url, **kwargs):
        requests_seen.append((method, url, client._s.headers.get("Authorization")))
        return next(responses)

    def fake_refresh() -> bool:
        client.access_token = "fresh-token"
        client.user_id = "user-123"
        client.refresh_token = "fresh-refresh"
        client._s.headers["Authorization"] = "Bearer fresh-token"
        return True

    monkeypatch.setattr(client._s, "request", fake_request)
    monkeypatch.setattr(client, "_refresh_session_if_possible", fake_refresh)

    rows = client._get("observations?limit=1&select=id")

    assert rows == [{"id": "obs-1"}]
    assert requests_seen == [
        ("GET", "https://zkpjklzfwzefhjluvhfw.supabase.co/rest/v1/observations?limit=1&select=id", "Bearer expired-token"),
        ("GET", "https://zkpjklzfwzefhjluvhfw.supabase.co/rest/v1/observations?limit=1&select=id", "Bearer fresh-token"),
    ]


def test_cloud_client_retries_transient_503_with_backoff(monkeypatch):
    client = cloud_sync.SporelyCloudClient("access-token", "user-123")
    responses = iter(
        [
            _FakeResponse(False, 503, '{"message":"Service Unavailable"}'),
            _FakeResponse(True, 200, '[{"id":"obs-1"}]', payload=[{"id": "obs-1"}]),
        ]
    )
    sleep_calls: list[float] = []

    def fake_request(method, url, **kwargs):
        return next(responses)

    monkeypatch.setattr(client._s, "request", fake_request)
    monkeypatch.setattr(cloud_sync.random, "uniform", lambda low, high: high)
    monkeypatch.setattr(cloud_sync.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    rows = client._get("observations?limit=1&select=id")

    assert rows == [{"id": "obs-1"}]
    assert sleep_calls == [0.5]


def test_cloud_request_auth_refresh_failure_is_temporarily_unavailable(monkeypatch):
    client = cloud_sync.SporelyCloudClient("expired-token", "user-123", "refresh-token")
    responses = iter(
        [
            _FakeResponse(
                False,
                401,
                '{"code":"PGRST303","message":"JWT expired"}',
            ),
        ]
    )

    def fake_request(method, url, **kwargs):
        return next(responses)

    monkeypatch.setattr(client._s, "request", fake_request)
    monkeypatch.setattr(client, "_refresh_session_if_possible", lambda: False)

    with pytest.raises(cloud_sync.CloudTemporarilyUnavailableError):
        client._get("observations?limit=1&select=id")


def test_temporary_wrapper_from_auth_refresh_failure_is_not_reauth_required():
    """A CloudTemporarilyUnavailableError chained from a generic
    ``auth refresh failed`` string must not by itself be treated as a
    terminal session state.  Otherwise a rotation race between two
    client instances would wipe still-valid tokens on next restart."""
    try:
        raise cloud_sync.CloudSyncError(
            "GET https://example.test/rest/v1/profiles status=401: auth refresh failed"
        )
    except cloud_sync.CloudSyncError as cause:
        error = cloud_sync.CloudTemporarilyUnavailableError(
            "Supabase/cloud sync is temporarily unavailable; local data was not overwritten."
        )
        error.__cause__ = cause

    assert cloud_sync.is_cloud_reauth_required_error(error) is False
    # The generic ``auth refresh failed`` hint no longer counts as an
    # auth error either — otherwise callers using is_cloud_auth_error()
    # would still route this into session-wipe paths.
    assert cloud_sync.is_cloud_auth_error(error) is False


def test_is_cloud_auth_error_matches_expired_jwt_body():
    """Expired-JWT responses from Supabase must still classify as an
    auth error so the request layer retries with a refresh."""
    exc = cloud_sync.CloudSyncError(
        '{"code":"PGRST303","message":"JWT expired"}'
    )
    assert cloud_sync.is_cloud_auth_error(exc) is True


def test_is_cloud_auth_error_ignores_bare_403_rls():
    """A PostgREST/Supabase 403 is an authorization (RLS) denial, not
    an authentication failure.  It must not trigger a token refresh or
    session wipe."""
    class _Response403:
        def __init__(self) -> None:
            self.status_code = 403
            self.text = '{"code":"42501","message":"new row violates row-level security policy"}'

    response = _Response403()

    assert cloud_sync._response_indicates_auth_error(response) is False
    assert cloud_sync.is_cloud_auth_error(cloud_sync.CloudSyncError(response.text)) is False


def test_is_cloud_reauth_required_error_matches_invalid_grant():
    """A refresh-endpoint invalid_grant proves the token is dead."""
    exc = cloud_sync.CloudReauthRequiredError(
        'Refresh failed (status=400): {"error":"invalid_grant"}'
    )
    assert cloud_sync.is_cloud_reauth_required_error(exc) is True
    # is_cloud_auth_error also matches (invalid_grant is a shared hint)
    # so existing sync-abort paths keep working.
    assert cloud_sync.is_cloud_auth_error(exc) is True


def test_refresh_login_invalid_grant_raises_reauth_required(monkeypatch):
    """Supabase returning 400 + invalid_grant on the refresh endpoint
    means the refresh token is truly dead.  refresh_login must raise
    the terminal subclass so callers can distinguish it from a rotation
    race."""
    invalid_grant_response = _FakeResponse(
        False,
        400,
        '{"error":"invalid_grant","error_description":"Refresh Token Not Found"}',
    )

    def fake_request(method, url, **kwargs):
        return invalid_grant_response

    monkeypatch.setattr(cloud_sync.requests, "request", fake_request)

    with pytest.raises(cloud_sync.CloudReauthRequiredError):
        cloud_sync.SporelyCloudClient.refresh_login("dead-refresh")


def test_refresh_login_generic_400_is_recoverable(monkeypatch):
    """A non-``invalid_grant`` 400 could be a transient parse/routing
    issue at Supabase and must not be treated as terminal."""
    generic_response = _FakeResponse(False, 400, '{"error":"bad_request"}')

    def fake_request(method, url, **kwargs):
        return generic_response

    monkeypatch.setattr(cloud_sync.requests, "request", fake_request)

    with pytest.raises(cloud_sync.CloudSyncError) as excinfo:
        cloud_sync.SporelyCloudClient.refresh_login("still-good")

    assert not isinstance(excinfo.value, cloud_sync.CloudReauthRequiredError)


def test_refresh_login_invalid_grant_does_not_clear_tokens(monkeypatch):
    """CloudReauthRequiredError signals re-auth is needed but the code
    path that raises it must not delete stored tokens.  Wiping is
    reserved for explicit sign-out."""
    settings = {
        "cloud_access_token": "kept",
        "cloud_user_id": "user-123",
        "cloud_refresh_token": "dead-refresh",
    }
    invalid_grant_response = _FakeResponse(
        False,
        400,
        '{"error":"invalid_grant","error_description":"Token has expired or is invalid"}',
    )

    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))
    monkeypatch.setattr(
        cloud_sync,
        "update_app_settings",
        lambda updates: pytest.fail(
            "refresh_login on invalid_grant must not touch stored tokens"
        ),
    )
    monkeypatch.setattr(cloud_sync.requests, "request", lambda method, url, **kw: invalid_grant_response)

    with pytest.raises(cloud_sync.CloudReauthRequiredError):
        cloud_sync.SporelyCloudClient.refresh_login("dead-refresh")

    # Tokens still present after the terminal-refresh signal.
    assert settings["cloud_refresh_token"] == "dead-refresh"
    assert settings["cloud_access_token"] == "kept"


def test_clear_session_preserves_saved_cloud_email(monkeypatch):
    settings = {
        "cloud_user_email": "keep@example.com",
        "cloud_access_token": "access-token",
        "cloud_user_id": "user-123",
        "cloud_refresh_token": "refresh-token",
    }

    def fake_update_app_settings(updates):
        settings.update(dict(updates))
        return dict(settings)

    monkeypatch.setattr(cloud_sync, "update_app_settings", fake_update_app_settings)

    cloud_sync.SporelyCloudClient.clear_session()

    assert settings["cloud_user_email"] == "keep@example.com"
    assert settings["cloud_access_token"] is None
    assert settings["cloud_user_id"] is None
    assert settings["cloud_refresh_token"] is None


def test_pull_observation_identifications_schema_cache_error_is_temporarily_unavailable(monkeypatch):
    client = cloud_sync.SporelyCloudClient("access-token", "user-123")

    def fake_get(path):
        raise cloud_sync.CloudSyncError(
            'GET observation_identifications?observation_id=eq.obs-1&select=*: '
            '{"code":"PGRST002","message":"schema cache is not loaded"}'
        )

    monkeypatch.setattr(client, "_get", fake_get)

    with pytest.raises(cloud_sync.CloudTemporarilyUnavailableError):
        client.pull_observation_identifications("obs-1")


def test_from_stored_credentials_returns_cached_client_without_probing(monkeypatch):
    settings = {
        "cloud_access_token": "cached-token",
        "cloud_user_id": "user-123",
        "cloud_refresh_token": "refresh-token",
    }

    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))
    monkeypatch.setattr(cloud_sync, "load_saved_cloud_password", lambda: ("", None, False))
    monkeypatch.setattr(
        cloud_sync.SporelyCloudClient,
        "_get",
        lambda self, path: pytest.fail("from_stored_credentials() should not probe the API"),
    )

    client = cloud_sync.SporelyCloudClient.from_stored_credentials()

    assert client is not None
    assert client.access_token == "cached-token"
    assert client.user_id == "user-123"
    assert client.refresh_token == "refresh-token"


def test_from_stored_credentials_refreshes_from_saved_refresh_token(monkeypatch):
    settings = {
        "cloud_refresh_token": "refresh-token",
    }
    save_calls: list[dict[str, object]] = []
    refreshed_client = SimpleNamespace(
        access_token="fresh-token",
        user_id="user-123",
        refresh_token="new-refresh-token",
        save_credentials=lambda **kwargs: save_calls.append(dict(kwargs)),
    )

    def fake_refresh_login(refresh_token: str):
        assert refresh_token == "refresh-token"
        return refreshed_client

    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))
    monkeypatch.setattr(cloud_sync, "load_saved_cloud_password", lambda: ("", None, False))
    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "refresh_login", fake_refresh_login)

    client = cloud_sync.SporelyCloudClient.from_stored_credentials()

    assert client is refreshed_client
    assert client.access_token == "fresh-token"
    assert client.user_id == "user-123"
    assert client.refresh_token == "new-refresh-token"
    assert save_calls == [{}]


def test_push_images_for_observation_surfaces_auth_errors(monkeypatch):
    class DummyClient:
        def pull_image_metadata(self, obs_cloud_id, include_deleted_for_sync=False):
            raise cloud_sync.CloudSyncError(
                'GET observation_images?observation_id=eq.26&select=*: {"code":"PGRST303","message":"JWT expired"}'
            )

    monkeypatch.setattr(cloud_sync, "_push_pending_image_tombstones", lambda client: [])

    with pytest.raises(cloud_sync.CloudSyncError, match="JWT expired"):
        cloud_sync._push_images_for_observation(
            DummyClient(),
            {"id": 1},
            "cloud-obs-1",
            prepare_images_cb=lambda obs, progress_cb: ([], None, []),
        )


# ---------------------------------------------------------------------
# Stage 3 — refresh-token rotation race protection
# ---------------------------------------------------------------------


def test_refresh_adopts_newer_access_token_from_settings_without_network(monkeypatch):
    """Two clients raced Supabase: client A already refreshed and wrote
    the rotated tokens to settings.  Client B, which is holding stale
    in-memory copies, must adopt the settings tokens without calling
    the refresh endpoint."""
    fresh_access = _make_jwt(sub="user-123", exp_offset_seconds=3600, kid="fresh")
    stale_access = _make_jwt(sub="user-123", exp_offset_seconds=-60, kid="stale")

    client_b = cloud_sync.SporelyCloudClient(stale_access, "user-123", "R1-old")

    settings = {
        "cloud_access_token": fresh_access,
        "cloud_user_id": "user-123",
        "cloud_refresh_token": "R2-rotated",
    }
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))

    def _fail_refresh(refresh_token):
        pytest.fail("refresh endpoint must not be called when settings already contain fresh tokens")

    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "refresh_login", _fail_refresh)

    assert client_b._refresh_session_if_possible() is True
    assert client_b.access_token == fresh_access
    assert client_b.refresh_token == "R2-rotated"
    assert client_b._s.headers["Authorization"] == f"Bearer {fresh_access}"


def test_refresh_prefers_newest_refresh_token_from_settings(monkeypatch):
    """When settings holds a newer refresh token than the client's
    in-memory copy, the refresh call must go out with the newer one."""
    stale_access = _make_jwt(exp_offset_seconds=-60, kid="stale")
    client_b = cloud_sync.SporelyCloudClient(stale_access, "user-123", "R1-old")

    settings = {
        "cloud_access_token": stale_access,
        "cloud_user_id": "user-123",
        "cloud_refresh_token": "R2-newer",
    }
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))

    refresh_calls: list[str] = []
    fresh_access = _make_jwt(exp_offset_seconds=3600, kid="fresh")

    def fake_refresh_login(refresh_token: str):
        refresh_calls.append(refresh_token)
        return cloud_sync.SporelyCloudClient(fresh_access, "user-123", "R3-latest")

    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "refresh_login", classmethod(lambda cls, tok: fake_refresh_login(tok)))
    monkeypatch.setattr(
        cloud_sync.SporelyCloudClient,
        "save_credentials",
        lambda self, *args, **kwargs: None,
    )

    assert client_b._refresh_session_if_possible() is True
    assert refresh_calls == ["R2-newer"]
    assert client_b.access_token == fresh_access
    assert client_b.refresh_token == "R3-latest"


def test_refresh_retries_with_settings_token_after_stale_invalid_grant(monkeypatch):
    """First refresh attempt uses a stale token and gets invalid_grant.
    Meanwhile another thread has already rotated the token on disk.
    A single retry with the newer refresh token must succeed and the
    session must not be marked as re-auth-required."""
    stale_access = _make_jwt(exp_offset_seconds=-60, kid="stale")
    client_b = cloud_sync.SporelyCloudClient(stale_access, "user-123", "R1-doomed")

    fresh_access = _make_jwt(exp_offset_seconds=3600, kid="fresh")

    settings_snapshots = iter(
        [
            {
                "cloud_access_token": stale_access,
                "cloud_user_id": "user-123",
                "cloud_refresh_token": "R1-doomed",
            },
            {
                "cloud_access_token": stale_access,
                "cloud_user_id": "user-123",
                "cloud_refresh_token": "R2-hot",
            },
        ]
    )
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: next(settings_snapshots))

    refresh_calls: list[str] = []

    def fake_refresh_login(refresh_token: str):
        refresh_calls.append(refresh_token)
        if refresh_token == "R1-doomed":
            raise cloud_sync.CloudReauthRequiredError(
                'Refresh failed (status=400): {"error":"invalid_grant"}'
            )
        return cloud_sync.SporelyCloudClient(fresh_access, "user-123", "R3-brand-new")

    monkeypatch.setattr(
        cloud_sync.SporelyCloudClient,
        "refresh_login",
        classmethod(lambda cls, tok: fake_refresh_login(tok)),
    )
    save_calls: list[tuple] = []
    monkeypatch.setattr(
        cloud_sync.SporelyCloudClient,
        "save_credentials",
        lambda self, *args, **kwargs: save_calls.append((self.access_token, self.refresh_token)),
    )

    assert client_b._refresh_session_if_possible() is True
    assert refresh_calls == ["R1-doomed", "R2-hot"]
    assert client_b.access_token == fresh_access
    assert client_b.refresh_token == "R3-brand-new"
    assert save_calls == [(fresh_access, "R3-brand-new")]


def test_refresh_propagates_reauth_required_when_no_newer_token(monkeypatch):
    """A genuine invalid_grant with no newer refresh token on disk must
    propagate CloudReauthRequiredError.  Stored tokens must not be
    cleared by the request layer — that decision belongs to the UI."""
    stale_access = _make_jwt(exp_offset_seconds=-60, kid="stale")
    client = cloud_sync.SporelyCloudClient(stale_access, "user-123", "R1-dead")

    settings = {
        "cloud_access_token": stale_access,
        "cloud_user_id": "user-123",
        "cloud_refresh_token": "R1-dead",
    }
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))
    monkeypatch.setattr(
        cloud_sync,
        "update_app_settings",
        lambda updates: pytest.fail("refresh path must not clear settings"),
    )

    refresh_calls: list[str] = []

    def fake_refresh_login(refresh_token: str):
        refresh_calls.append(refresh_token)
        raise cloud_sync.CloudReauthRequiredError(
            'Refresh failed (status=400): {"error":"invalid_grant"}'
        )

    monkeypatch.setattr(
        cloud_sync.SporelyCloudClient,
        "refresh_login",
        classmethod(lambda cls, tok: fake_refresh_login(tok)),
    )

    with pytest.raises(cloud_sync.CloudReauthRequiredError):
        client._refresh_session_if_possible()

    # Stored tokens preserved.
    assert settings["cloud_refresh_token"] == "R1-dead"
    # Only tried once — no newer token appeared, so no retry.
    assert refresh_calls == ["R1-dead"]


def test_refresh_temporary_error_does_not_clear_or_flag(monkeypatch):
    """A temporary refresh failure must propagate CloudTemporarilyUnavailableError
    and must not touch settings or emit re-auth flags from cloud_sync itself."""
    stale_access = _make_jwt(exp_offset_seconds=-60, kid="stale")
    client = cloud_sync.SporelyCloudClient(stale_access, "user-123", "R1")

    settings = {
        "cloud_access_token": stale_access,
        "cloud_user_id": "user-123",
        "cloud_refresh_token": "R1",
    }
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))
    monkeypatch.setattr(
        cloud_sync,
        "update_app_settings",
        lambda updates: pytest.fail("temporary refresh error must not write settings"),
    )

    def fake_refresh_login(refresh_token: str):
        raise cloud_sync.CloudTemporarilyUnavailableError(
            "Supabase/cloud sync is temporarily unavailable; local data was not overwritten."
        )

    monkeypatch.setattr(
        cloud_sync.SporelyCloudClient,
        "refresh_login",
        classmethod(lambda cls, tok: fake_refresh_login(tok)),
    )

    with pytest.raises(cloud_sync.CloudTemporarilyUnavailableError):
        client._refresh_session_if_possible()

    # Stored tokens preserved.
    assert settings["cloud_refresh_token"] == "R1"


def test_refresh_lock_serializes_concurrent_refresh_attempts(monkeypatch):
    """Two threads calling _refresh_session_if_possible concurrently must
    result in exactly one call to the refresh endpoint.  The second
    thread should adopt the tokens the first thread persisted."""
    import threading as _threading

    fresh_access = _make_jwt(exp_offset_seconds=3600, kid="fresh")
    stale_access = _make_jwt(exp_offset_seconds=-60, kid="stale")

    client_a = cloud_sync.SporelyCloudClient(stale_access, "user-123", "R1")
    client_b = cloud_sync.SporelyCloudClient(stale_access, "user-123", "R1")

    settings: dict[str, object] = {
        "cloud_access_token": stale_access,
        "cloud_user_id": "user-123",
        "cloud_refresh_token": "R1",
    }
    settings_lock = _threading.Lock()

    def fake_get_settings():
        with settings_lock:
            return dict(settings)

    def fake_update_settings(updates):
        with settings_lock:
            settings.update(updates)

    monkeypatch.setattr(cloud_sync, "get_app_settings", fake_get_settings)
    monkeypatch.setattr(cloud_sync, "update_app_settings", fake_update_settings)

    refresh_calls: list[str] = []
    start_gate = _threading.Event()

    def fake_refresh_login(refresh_token: str):
        # Give the second thread time to queue up on the refresh lock.
        start_gate.wait(timeout=1.0)
        refresh_calls.append(refresh_token)
        return cloud_sync.SporelyCloudClient(fresh_access, "user-123", "R2")

    monkeypatch.setattr(
        cloud_sync.SporelyCloudClient,
        "refresh_login",
        classmethod(lambda cls, tok: fake_refresh_login(tok)),
    )

    results: dict[str, bool] = {}

    def _run(name, client):
        results[name] = client._refresh_session_if_possible()

    t_a = _threading.Thread(target=_run, args=("a", client_a))
    t_b = _threading.Thread(target=_run, args=("b", client_b))
    t_a.start()
    # Give A a moment to acquire the lock before B contends for it.
    time.sleep(0.05)
    t_b.start()
    start_gate.set()
    t_a.join(timeout=2.0)
    t_b.join(timeout=2.0)

    assert results == {"a": True, "b": True}
    assert refresh_calls == ["R1"], "only one refresh endpoint call should be issued"
    assert client_a.access_token == fresh_access
    assert client_b.access_token == fresh_access
    assert client_a.refresh_token == "R2"
    assert client_b.refresh_token == "R2"


# ---------------------------------------------------------------------
# Stage 3 — from_stored_credentials expiry-aware behavior
# ---------------------------------------------------------------------


def test_from_stored_credentials_returns_client_when_token_is_valid(monkeypatch):
    """A valid, decodable JWT keeps the fast path — no refresh call."""
    fresh = _make_jwt(exp_offset_seconds=3600)
    settings = {
        "cloud_access_token": fresh,
        "cloud_user_id": "user-123",
        "cloud_refresh_token": "R1",
    }
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))
    monkeypatch.setattr(
        cloud_sync.SporelyCloudClient,
        "refresh_login",
        classmethod(lambda cls, tok: pytest.fail("valid token must not trigger refresh")),
    )
    monkeypatch.setattr(cloud_sync, "load_saved_cloud_password", lambda: ("", None, False))

    client = cloud_sync.SporelyCloudClient.from_stored_credentials()

    assert client is not None
    assert client.access_token == fresh
    assert client.refresh_token == "R1"


def test_from_stored_credentials_refreshes_when_stored_token_is_expired(monkeypatch):
    """A stored JWT within 5 minutes of expiry must trigger a locked
    refresh at construction time.  This turns the "first API call gets
    401" race into a synchronous refresh at startup."""
    stale = _make_jwt(exp_offset_seconds=-120)
    fresh = _make_jwt(exp_offset_seconds=3600, kid="new")
    settings = {
        "cloud_access_token": stale,
        "cloud_user_id": "user-123",
        "cloud_refresh_token": "R1",
    }
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))
    monkeypatch.setattr(cloud_sync, "load_saved_cloud_password", lambda: ("", None, False))

    refresh_calls: list[str] = []

    def fake_refresh_login(refresh_token: str):
        refresh_calls.append(refresh_token)
        return cloud_sync.SporelyCloudClient(fresh, "user-123", "R2")

    monkeypatch.setattr(
        cloud_sync.SporelyCloudClient,
        "refresh_login",
        classmethod(lambda cls, tok: fake_refresh_login(tok)),
    )
    monkeypatch.setattr(
        cloud_sync.SporelyCloudClient,
        "save_credentials",
        lambda self, *args, **kwargs: None,
    )

    client = cloud_sync.SporelyCloudClient.from_stored_credentials()

    assert client is not None
    assert refresh_calls == ["R1"]
    assert client.access_token == fresh
    assert client.refresh_token == "R2"


def test_from_stored_credentials_keeps_fast_path_for_undecodable_token(monkeypatch):
    """A token whose ``exp`` cannot be decoded (e.g. legacy/opaque) must
    NOT trigger a proactive refresh — Stage 3 only refreshes when we
    can prove near-expiry.  A first-request 401 will still cover the
    otherwise-expired case."""
    settings = {
        "cloud_access_token": "opaque-not-a-jwt",
        "cloud_user_id": "user-123",
        "cloud_refresh_token": "R1",
    }
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))
    monkeypatch.setattr(
        cloud_sync.SporelyCloudClient,
        "refresh_login",
        classmethod(lambda cls, tok: pytest.fail("undecodable token must not trigger refresh")),
    )
    monkeypatch.setattr(cloud_sync, "load_saved_cloud_password", lambda: ("", None, False))

    client = cloud_sync.SporelyCloudClient.from_stored_credentials()

    assert client is not None
    assert client.access_token == "opaque-not-a-jwt"
    assert client.refresh_token == "R1"


# ---------------------------------------------------------------------
# Stage 4 — account-safety guard on locked adoption
# ---------------------------------------------------------------------


def test_refresh_adopts_settings_tokens_when_user_matches(monkeypatch):
    """Baseline same-account rotation must still work under the guard."""
    stale_access = _make_jwt(sub="user-U1", exp_offset_seconds=-60, kid="stale")
    fresh_access = _make_jwt(sub="user-U1", exp_offset_seconds=3600, kid="fresh")
    client = cloud_sync.SporelyCloudClient(stale_access, "user-U1", "R1-old")

    settings = {
        "cloud_access_token": fresh_access,
        "cloud_user_id": "user-U1",
        "cloud_refresh_token": "R2-rotated",
    }
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))
    monkeypatch.setattr(
        cloud_sync.SporelyCloudClient,
        "refresh_login",
        classmethod(lambda cls, tok: pytest.fail("must not hit refresh endpoint")),
    )

    assert client._refresh_session_if_possible() is True
    assert client.user_id == "user-U1"
    assert client.access_token == fresh_access
    assert client.refresh_token == "R2-rotated"


def test_refresh_refuses_to_adopt_access_token_from_different_account(monkeypatch):
    """Client is U1, settings now hold U2's session.  The refresh path
    must NOT adopt the foreign access token and must NOT call the
    refresh endpoint with the foreign refresh token."""
    stale_u1_access = _make_jwt(sub="user-U1", exp_offset_seconds=-60, kid="u1")
    fresh_u2_access = _make_jwt(sub="user-U2", exp_offset_seconds=3600, kid="u2")
    client = cloud_sync.SporelyCloudClient(stale_u1_access, "user-U1", "R1")

    settings = {
        "cloud_access_token": fresh_u2_access,
        "cloud_user_id": "user-U2",
        "cloud_refresh_token": "R2-u2",
    }
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))
    monkeypatch.setattr(
        cloud_sync,
        "update_app_settings",
        lambda updates: pytest.fail("account guard must not touch settings"),
    )
    monkeypatch.setattr(
        cloud_sync.SporelyCloudClient,
        "refresh_login",
        classmethod(lambda cls, tok: pytest.fail("must not call refresh endpoint on account mismatch")),
    )

    with pytest.raises(cloud_sync.CloudSessionAccountMismatchError):
        client._refresh_session_if_possible()

    # Client's in-memory session must be untouched.
    assert client.user_id == "user-U1"
    assert client.access_token == stale_u1_access
    assert client.refresh_token == "R1"
    # Settings must be untouched.
    assert settings["cloud_refresh_token"] == "R2-u2"
    assert settings["cloud_access_token"] == fresh_u2_access


def test_refresh_refuses_to_use_foreign_refresh_token_when_only_refresh_rotated(monkeypatch):
    """Settings has U2/R2 but the same access token as the client.  Even
    though the fast-adopt branch would not fire, the guard must trip
    and prevent us from spending R2 (which belongs to another
    account) at the refresh endpoint."""
    stale_access = _make_jwt(sub="user-U1", exp_offset_seconds=-60, kid="stale")
    client = cloud_sync.SporelyCloudClient(stale_access, "user-U1", "R1")

    settings = {
        # Note: settings access token is empty here — the only foreign
        # thing on disk is the refresh token and user id.
        "cloud_access_token": "",
        "cloud_user_id": "user-U2",
        "cloud_refresh_token": "R2-u2",
    }
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))
    monkeypatch.setattr(
        cloud_sync.SporelyCloudClient,
        "refresh_login",
        classmethod(lambda cls, tok: pytest.fail("must not call refresh endpoint on account mismatch")),
    )

    with pytest.raises(cloud_sync.CloudSessionAccountMismatchError):
        client._refresh_session_if_possible()

    assert client.refresh_token == "R1"


def test_refresh_reauth_retry_does_not_cross_accounts(monkeypatch):
    """First refresh for U1 raises CloudReauthRequiredError.  While we
    were mid-refresh the settings rotated to U2/R2.  The retry branch
    must refuse to use R2 and must propagate a mismatch/reauth
    signal, not authenticate as the other account."""
    stale_u1_access = _make_jwt(sub="user-U1", exp_offset_seconds=-60, kid="u1")
    fresh_u2_access = _make_jwt(sub="user-U2", exp_offset_seconds=3600, kid="u2")
    client = cloud_sync.SporelyCloudClient(stale_u1_access, "user-U1", "R1-doomed")

    settings_snapshots = iter(
        [
            # Initial snapshot — same user, so the guard passes and we
            # attempt to refresh with R1-doomed.
            {
                "cloud_access_token": stale_u1_access,
                "cloud_user_id": "user-U1",
                "cloud_refresh_token": "R1-doomed",
            },
            # Post-reauth-required snapshot — settings has flipped to
            # a different account.  Guard must refuse.
            {
                "cloud_access_token": fresh_u2_access,
                "cloud_user_id": "user-U2",
                "cloud_refresh_token": "R2-u2",
            },
        ]
    )
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: next(settings_snapshots))
    monkeypatch.setattr(
        cloud_sync,
        "update_app_settings",
        lambda updates: pytest.fail("cross-account retry must not touch settings"),
    )

    refresh_calls: list[str] = []

    def fake_refresh_login(refresh_token: str):
        refresh_calls.append(refresh_token)
        raise cloud_sync.CloudReauthRequiredError(
            'Refresh failed (status=400): {"error":"invalid_grant"}'
        )

    monkeypatch.setattr(
        cloud_sync.SporelyCloudClient,
        "refresh_login",
        classmethod(lambda cls, tok: fake_refresh_login(tok)),
    )

    with pytest.raises(cloud_sync.CloudReauthRequiredError):
        client._refresh_session_if_possible()

    # Only R1-doomed was tried — R2-u2 (foreign account) was never
    # sent to Supabase.
    assert refresh_calls == ["R1-doomed"]
    # Client identity preserved.
    assert client.user_id == "user-U1"
    assert client.refresh_token == "R1-doomed"


def test_refresh_allows_adoption_when_settings_user_id_missing_but_jwt_subject_matches(monkeypatch):
    """Settings lost the ``cloud_user_id`` row (older format, partial
    migration, etc.) but the settings access token decodes to the
    client's own user.  Adoption must proceed."""
    stale_access = _make_jwt(sub="user-U1", exp_offset_seconds=-60, kid="stale")
    fresh_access = _make_jwt(sub="user-U1", exp_offset_seconds=3600, kid="fresh")
    client = cloud_sync.SporelyCloudClient(stale_access, "user-U1", "R1")

    settings = {
        "cloud_access_token": fresh_access,
        "cloud_user_id": "",  # missing
        "cloud_refresh_token": "R2",
    }
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))
    monkeypatch.setattr(
        cloud_sync.SporelyCloudClient,
        "refresh_login",
        classmethod(lambda cls, tok: pytest.fail("adoption must not hit the network")),
    )

    assert client._refresh_session_if_possible() is True
    assert client.access_token == fresh_access
    assert client.refresh_token == "R2"


def test_refresh_refuses_adoption_when_settings_user_missing_and_subject_undecodable(monkeypatch):
    """If settings user_id is missing AND the settings access token has
    no decodable subject, we cannot prove ownership.  Adoption must be
    refused for a client that already carries an identity."""
    stale_access = _make_jwt(sub="user-U1", exp_offset_seconds=-60, kid="stale")
    client = cloud_sync.SporelyCloudClient(stale_access, "user-U1", "R1")

    settings = {
        "cloud_access_token": "opaque-token-no-subject",
        "cloud_user_id": "",
        "cloud_refresh_token": "R-unknown",
    }
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))
    monkeypatch.setattr(
        cloud_sync.SporelyCloudClient,
        "refresh_login",
        classmethod(lambda cls, tok: pytest.fail("must not refresh with unverifiable settings tokens")),
    )

    with pytest.raises(cloud_sync.CloudSessionAccountMismatchError):
        client._refresh_session_if_possible()

    # Client untouched.
    assert client.access_token == stale_access
    assert client.refresh_token == "R1"


def test_settings_session_is_compatible_handles_edge_cases():
    """Direct unit coverage for the account-compat helper."""
    fresh_u1 = _make_jwt(sub="user-U1", exp_offset_seconds=3600, kid="u1")
    fresh_u2 = _make_jwt(sub="user-U2", exp_offset_seconds=3600, kid="u2")

    # Client with no identity accepts anything.
    assert cloud_sync._settings_session_is_compatible(None, "user-U2", fresh_u2) is True
    assert cloud_sync._settings_session_is_compatible("", "user-U2", fresh_u2) is True

    # Matching explicit user ids.
    assert cloud_sync._settings_session_is_compatible("user-U1", "user-U1", fresh_u1) is True

    # Mismatched explicit user ids.
    assert cloud_sync._settings_session_is_compatible("user-U1", "user-U2", fresh_u2) is False

    # Settings user id missing, JWT subject matches.
    assert cloud_sync._settings_session_is_compatible("user-U1", "", fresh_u1) is True

    # Settings user id missing, JWT subject mismatches.
    assert cloud_sync._settings_session_is_compatible("user-U1", "", fresh_u2) is False

    # Settings user id missing, no access token — nothing to conflict
    # with.  Caller may proceed with its own tokens.
    assert cloud_sync._settings_session_is_compatible("user-U1", "", "") is True
    assert cloud_sync._settings_session_is_compatible("user-U1", None, None) is True

    # Settings has an access token but its subject is undecodable and
    # user id is missing — refuse.
    assert cloud_sync._settings_session_is_compatible("user-U1", "", "not-a-jwt") is False
