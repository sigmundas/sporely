"""A token refresh never undoes a sign-out or a sign-in that happened while
its request was in flight.

The session write (sign-out, sign-in) happens inside the fake refresh request,
which is exactly the window between the refresh's settings snapshot and its
persistence that a UI-thread sign-out can hit while a worker refreshes.
"""

from __future__ import annotations

import base64
import json
import time

import pytest

import utils.cloud_sync as cloud_sync


def _jwt(sub: str, exp_offset_seconds: int = 3600) -> str:
    def b64(data: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(data).encode()).rstrip(b"=").decode()

    return f"{b64({'alg': 'HS256'})}.{b64({'sub': sub, 'exp': int(time.time()) + exp_offset_seconds})}.sig"


@pytest.fixture
def stored(monkeypatch):
    settings = {
        "cloud_access_token": _jwt("user-a", exp_offset_seconds=-60),
        "cloud_user_id": "user-a",
        "cloud_refresh_token": "refresh-a",
    }
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))

    def update(updates):
        settings.update(dict(updates))
        return dict(settings)

    monkeypatch.setattr(cloud_sync, "update_app_settings", update)
    return settings


def _client_a(stored) -> cloud_sync.SporelyCloudClient:
    return cloud_sync.SporelyCloudClient(
        stored["cloud_access_token"], "user-a", refresh_token="refresh-a"
    )


def _refresh_that(monkeypatch, during_request):
    refreshed_token = _jwt("user-a")

    def refresh_login(cls, token):
        during_request()
        return cloud_sync.SporelyCloudClient(refreshed_token, "user-a", refresh_token="refresh-a2")

    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "refresh_login", classmethod(refresh_login))
    return refreshed_token


def test_refresh_saves_the_rotated_session_when_nothing_else_wrote(stored, monkeypatch):
    client = _client_a(stored)
    refreshed = _refresh_that(monkeypatch, lambda: None)

    assert client._refresh_session_if_possible() is True
    assert stored["cloud_access_token"] == refreshed
    assert stored["cloud_refresh_token"] == "refresh-a2"
    assert client.access_token == refreshed


def test_sign_out_during_refresh_is_not_undone(stored, monkeypatch):
    client = _client_a(stored)
    stale_access = client.access_token
    _refresh_that(monkeypatch, cloud_sync.SporelyCloudClient.clear_session)

    assert client._refresh_session_if_possible() is False
    assert stored["cloud_access_token"] is None
    assert stored["cloud_refresh_token"] is None
    assert stored["cloud_user_id"] is None
    assert client.access_token == stale_access, "the refreshed tokens are not adopted either"


def test_sign_in_to_another_account_during_refresh_is_kept(stored, monkeypatch):
    client = _client_a(stored)
    token_b = _jwt("user-b")
    _refresh_that(
        monkeypatch,
        lambda: cloud_sync.SporelyCloudClient(token_b, "user-b", refresh_token="refresh-b").save_credentials(),
    )

    assert client._refresh_session_if_possible() is False
    assert stored["cloud_access_token"] == token_b
    assert stored["cloud_user_id"] == "user-b"
    assert stored["cloud_refresh_token"] == "refresh-b"
    assert client.user_id == "user-a"


def test_a_newer_session_of_the_same_account_is_adopted_not_overwritten(stored, monkeypatch):
    client = _client_a(stored)
    newer = _jwt("user-a", exp_offset_seconds=7200)
    _refresh_that(
        monkeypatch,
        lambda: cloud_sync.SporelyCloudClient(newer, "user-a", refresh_token="refresh-a3").save_credentials(),
    )

    assert client._refresh_session_if_possible() is True
    assert stored["cloud_access_token"] == newer
    assert stored["cloud_refresh_token"] == "refresh-a3"
    assert client.access_token == newer


def test_a_client_created_before_sign_out_does_not_refresh_after_it(stored, monkeypatch):
    stored["cloud_access_token"] = _jwt("user-a")
    client = cloud_sync.SporelyCloudClient.from_stored_credentials()
    assert client is not None
    cloud_sync.SporelyCloudClient.clear_session()
    monkeypatch.setattr(
        cloud_sync.SporelyCloudClient,
        "refresh_login",
        classmethod(lambda cls, token: pytest.fail("a signed-out profile must not refresh")),
    )

    assert client._refresh_session_if_possible() is False
    assert stored["cloud_access_token"] is None
    assert stored["cloud_refresh_token"] is None


@pytest.mark.parametrize("client_class", ["password", "oauth"])
def test_loading_from_a_refresh_token_does_not_undo_a_sign_out(stored, monkeypatch, client_class):
    stored.update({"cloud_access_token": None, "cloud_user_id": None})
    cls = cloud_sync.SporelyCloudClient
    if client_class == "oauth":
        stored["cloud_auth_method"] = "oauth"
        cls = cloud_sync.OAuthSporelyCloudClient

    def refresh_login(klass, token):
        cloud_sync.SporelyCloudClient.clear_session()  # sign-out during the request
        return klass(_jwt("user-a"), "user-a", refresh_token="refresh-a2")

    monkeypatch.setattr(cls, "refresh_login", classmethod(refresh_login))

    assert cloud_sync.SporelyCloudClient.from_stored_credentials() is None
    assert stored["cloud_access_token"] is None
    assert stored["cloud_refresh_token"] is None


def test_loading_from_a_refresh_token_saves_when_nothing_else_wrote(stored, monkeypatch):
    stored.update({"cloud_access_token": None, "cloud_user_id": None})
    refreshed = _jwt("user-a")
    monkeypatch.setattr(
        cloud_sync.SporelyCloudClient,
        "refresh_login",
        classmethod(lambda cls, token: cls(refreshed, "user-a", refresh_token="refresh-a2")),
    )

    client = cloud_sync.SporelyCloudClient.from_stored_credentials()

    assert client is not None and client.access_token == refreshed
    assert stored["cloud_access_token"] == refreshed
    assert stored["cloud_refresh_token"] == "refresh-a2"


def test_a_fresh_sign_in_can_refresh_before_its_credentials_are_saved(stored, monkeypatch):
    stored.update({"cloud_access_token": None, "cloud_user_id": None, "cloud_refresh_token": None})
    fresh = cloud_sync.SporelyCloudClient(_jwt("user-a", -60), "user-a", refresh_token="refresh-new")
    refreshed = _refresh_that(monkeypatch, lambda: None)

    assert fresh._refresh_session_if_possible() is True
    assert fresh.access_token == refreshed


def test_concurrent_loaders_of_the_same_account_both_get_a_session(stored, monkeypatch):
    stored.update({"cloud_access_token": None, "cloud_user_id": None})
    other_loader = _jwt("user-a", exp_offset_seconds=7200)

    def refresh_login(cls, token):
        # Another loader finished first and saved its rotated session.
        cloud_sync.SporelyCloudClient(other_loader, "user-a", refresh_token="refresh-other").save_credentials()
        return cls(_jwt("user-a"), "user-a", refresh_token="refresh-mine")

    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "refresh_login", classmethod(refresh_login))

    client = cloud_sync.SporelyCloudClient.from_stored_credentials()

    assert client is not None and client.access_token == other_loader
    assert stored["cloud_access_token"] == other_loader
    assert stored["cloud_refresh_token"] == "refresh-other"


@pytest.mark.parametrize("client_class", ["password", "oauth"])
def test_a_signed_in_client_handed_to_workers_stops_after_sign_out(stored, monkeypatch, client_class):
    stored.update({"cloud_access_token": None, "cloud_user_id": None, "cloud_refresh_token": None})
    cls = cloud_sync.OAuthSporelyCloudClient if client_class == "oauth" else cloud_sync.SporelyCloudClient
    signed_in = cls(_jwt("user-a", -60), "user-a", refresh_token="refresh-a")
    signed_in.save_credentials()  # the sign-in flow saves, then reuses this client
    cloud_sync.SporelyCloudClient.clear_session()
    monkeypatch.setattr(
        cls, "refresh_login", classmethod(lambda klass, token: pytest.fail("signed out: no refresh"))
    )

    assert signed_in._refresh_session_if_possible() is False
    assert stored["cloud_access_token"] is None
