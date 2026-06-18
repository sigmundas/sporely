from __future__ import annotations

import pytest

import utils.cloud_sync as cloud_sync


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
