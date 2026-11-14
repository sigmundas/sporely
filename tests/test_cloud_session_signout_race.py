"""A token refresh that finishes after the user signed out or in saves nothing.

A sync worker refreshing its Sporely Cloud session holds the refresh lock
across the network call; signing out (or in) on the UI thread does not wait
for it. The refresh used to save its new tokens regardless, so a sign-out
during it was undone (the signed-out account's tokens came back) and a
sign-in to another account during it was overwritten. A refresh now saves
only if the stored session is still the one it started from, checked and
written under ``_CLOUD_SESSION_WRITE_LOCK``, which sign-in and sign-out also
take.

The settings store is a real dict behind ``get_app_settings`` /
``update_app_settings``; the sign-out and sign-in are the real
``clear_credentials`` and ``save_credentials``, run from inside the fake
refresh call, which is where the race happens.
"""
from __future__ import annotations

import base64
import json
import os
import stat
import threading
import time

import pytest

import utils.cloud_sync as cloud_sync


def _jwt(sub: str, *, exp_offset: int = 3600, kid: str = "k") -> str:
    def part(payload: dict) -> str:
        raw = json.dumps(payload).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    header = part({"alg": "HS256", "typ": "JWT", "kid": kid})
    body = part({"sub": sub, "exp": int(time.time()) + exp_offset})
    return f"{header}.{body}.sig"


A_STALE = _jwt("user-A", exp_offset=-60, kid="a-stale")
A_FRESH = _jwt("user-A", kid="a-fresh")
A_AGAIN = _jwt("user-A", kid="a-again")
B_TOKEN = _jwt("user-B", kid="b")


@pytest.fixture
def store(monkeypatch):
    data: dict[str, object] = {
        "cloud_access_token": A_STALE,
        "cloud_user_id": "user-A",
        "cloud_refresh_token": "RA1",
        "cloud_user_email": "a@example.com",
    }
    lock = threading.Lock()

    def get():
        with lock:
            return dict(data)

    def update(updates):
        with lock:
            data.update(updates)
            return dict(data)

    monkeypatch.setattr(cloud_sync, "get_app_settings", get)
    monkeypatch.setattr(cloud_sync, "update_app_settings", update)
    monkeypatch.setattr(cloud_sync, "clear_saved_cloud_password", lambda: [])
    return data


def _refresh_returns(monkeypatch, cls, during=None):
    """``cls.refresh_login`` returns A's new session after running ``during``."""

    def fake_refresh_login(_cls, refresh_token):
        if during is not None:
            during()  # the UI thread acts while the request is in flight
        return cls(A_FRESH, "user-A", "RA2")

    monkeypatch.setattr(cls, "refresh_login", classmethod(fake_refresh_login))


def _sign_out():
    cloud_sync.SporelyCloudClient.clear_credentials()


def _session(store):
    return store["cloud_access_token"], store["cloud_user_id"], store["cloud_refresh_token"]


def test_an_unchanged_session_is_saved(store, monkeypatch):
    _refresh_returns(monkeypatch, cloud_sync.SporelyCloudClient)
    client = cloud_sync.SporelyCloudClient(A_STALE, "user-A", "RA1")

    assert client._refresh_session_if_possible() is True
    assert _session(store) == (A_FRESH, "user-A", "RA2")


def test_signing_out_during_a_refresh_stays_signed_out(store, monkeypatch):
    _refresh_returns(monkeypatch, cloud_sync.SporelyCloudClient, during=_sign_out)
    client = cloud_sync.SporelyCloudClient(A_STALE, "user-A", "RA1")

    assert client._refresh_session_if_possible() is True  # this request may finish
    assert _session(store) == (None, None, None)
    assert cloud_sync.SporelyCloudClient.from_stored_credentials() is None


def test_signing_in_to_another_account_during_a_refresh_keeps_that_account(store, monkeypatch):
    def sign_in_as_b():
        cloud_sync.SporelyCloudClient(B_TOKEN, "user-B", "RB1").save_credentials(email="b@example.com")

    _refresh_returns(monkeypatch, cloud_sync.SporelyCloudClient, during=sign_in_as_b)
    client = cloud_sync.SporelyCloudClient(A_STALE, "user-A", "RA1")

    client._refresh_session_if_possible()

    assert _session(store) == (B_TOKEN, "user-B", "RB1")
    assert store["cloud_user_email"] == "b@example.com"


def test_signing_in_again_during_a_refresh_keeps_the_new_session(store, monkeypatch):
    def sign_in_as_a_again():
        cloud_sync.SporelyCloudClient(A_AGAIN, "user-A", "RA-new").save_credentials()

    _refresh_returns(monkeypatch, cloud_sync.SporelyCloudClient, during=sign_in_as_a_again)
    client = cloud_sync.SporelyCloudClient(A_STALE, "user-A", "RA1")

    client._refresh_session_if_possible()

    assert _session(store) == (A_AGAIN, "user-A", "RA-new")


def test_a_refresh_after_signing_out_saves_nothing(store, monkeypatch):
    """The worker still holds the signed-out account's refresh token."""
    _sign_out()
    _refresh_returns(monkeypatch, cloud_sync.SporelyCloudClient)
    client = cloud_sync.SporelyCloudClient(A_STALE, "user-A", "RA1")

    client._refresh_session_if_possible()

    assert _session(store) == (None, None, None)


def test_signing_out_during_an_oauth_refresh_stays_signed_out(store, monkeypatch):
    store["cloud_auth_method"] = "oauth"
    _refresh_returns(monkeypatch, cloud_sync.OAuthSporelyCloudClient, during=_sign_out)
    client = cloud_sync.OAuthSporelyCloudClient(A_STALE, "user-A", "RA1")

    assert client._refresh_session_if_possible() is True
    assert _session(store) == (None, None, None)
    assert store["cloud_auth_method"] is None


@pytest.mark.parametrize("cls", [cloud_sync.SporelyCloudClient, cloud_sync.OAuthSporelyCloudClient])
def test_loading_from_a_stored_refresh_token_honours_a_sign_out(store, monkeypatch, cls):
    """``from_stored_credentials`` with only a refresh token stored."""
    store.update({"cloud_access_token": None, "cloud_user_id": None})
    _refresh_returns(monkeypatch, cls, during=_sign_out)

    assert cls.from_stored_credentials() is None
    assert _session(store) == (None, None, None)


@pytest.mark.parametrize("cls", [cloud_sync.SporelyCloudClient, cloud_sync.OAuthSporelyCloudClient])
def test_loading_from_a_stored_refresh_token_still_saves(store, monkeypatch, cls):
    store.update({"cloud_access_token": None, "cloud_user_id": None})
    _refresh_returns(monkeypatch, cls)

    client = cls.from_stored_credentials()

    assert client is not None and client.access_token == A_FRESH
    assert _session(store) == (A_FRESH, "user-A", "RA2")


def test_a_sign_out_cannot_land_between_the_check_and_the_save(store, monkeypatch):
    """The sign-out runs on another thread right after the refresh's check.

    It reaches the lock while the save holds it, must not finish until the
    save is done, and then clears what the save wrote.
    """
    _refresh_returns(monkeypatch, cloud_sync.SporelyCloudClient)
    at_the_lock = threading.Event()  # clear_credentials takes the lock right after this
    monkeypatch.setattr(cloud_sync, "clear_saved_cloud_password", lambda: at_the_lock.set() or [])
    cleared = threading.Event()
    store_update = cloud_sync.update_app_settings

    def update(updates):
        result = store_update(updates)
        if "cloud_access_token" in updates and updates["cloud_access_token"] is None:
            cleared.set()
        return result

    monkeypatch.setattr(cloud_sync, "update_app_settings", update)
    real_read = cloud_sync._read_current_cloud_session_settings
    reads = []
    sign_out = threading.Thread(target=_sign_out)
    finished_during_the_save = []

    def read_then_sign_out():
        snapshot = real_read()
        reads.append(snapshot)
        if len(reads) == 2:  # the save's check (the first is the refresh's start)
            sign_out.start()
            assert at_the_lock.wait(timeout=5)
            finished_during_the_save.append(cleared.wait(timeout=0.3))
        return snapshot

    monkeypatch.setattr(cloud_sync, "_read_current_cloud_session_settings", read_then_sign_out)
    client = cloud_sync.SporelyCloudClient(A_STALE, "user-A", "RA1")

    client._refresh_session_if_possible()
    sign_out.join(timeout=5)

    assert finished_during_the_save == [False]
    assert not sign_out.is_alive()
    assert _session(store) == (None, None, None)


def test_signing_out_does_not_wait_for_a_refresh_in_flight(store, monkeypatch):
    """The refresh holds its lock across the network call; sign-out must not need it."""
    in_flight, answer = threading.Event(), threading.Event()

    def slow_refresh_login(cls, refresh_token):
        in_flight.set()
        answer.wait(timeout=5)
        return cls(A_FRESH, "user-A", "RA2")

    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "refresh_login", classmethod(slow_refresh_login))
    client = cloud_sync.SporelyCloudClient(A_STALE, "user-A", "RA1")
    refresh = threading.Thread(target=client._refresh_session_if_possible)
    refresh.start()
    try:
        assert in_flight.wait(timeout=5)
        sign_out = threading.Thread(target=_sign_out)
        sign_out.start()
        sign_out.join(timeout=2)
        assert not sign_out.is_alive(), "signing out waited for the network"
        assert _session(store) == (None, None, None)
    finally:
        answer.set()
        refresh.join(timeout=5)
    assert _session(store) == (None, None, None)  # and the refresh did not undo it


@pytest.mark.parametrize("cls", [cloud_sync.SporelyCloudClient, cloud_sync.OAuthSporelyCloudClient])
def test_two_loaders_spend_the_stored_refresh_token_once(store, monkeypatch, cls):
    """Codex: the second loader used to refresh too, then find the first one's session."""
    store.update({"cloud_access_token": None, "cloud_user_id": None})
    if cls is cloud_sync.OAuthSporelyCloudClient:
        store["cloud_auth_method"] = "oauth"
    calls = []
    second_at_the_lock = threading.Event()
    real_lock = cloud_sync._CLOUD_REFRESH_LOCK

    class WatchedLock:
        """The refresh lock, telling when the second loader starts waiting for it."""

        def __enter__(self):
            if threading.current_thread().name == "second":
                second_at_the_lock.set()
            return real_lock.__enter__()

        def __exit__(self, *exc):
            return real_lock.__exit__(*exc)

    monkeypatch.setattr(cloud_sync, "_CLOUD_REFRESH_LOCK", WatchedLock())

    def slow_refresh_login(_cls, refresh_token):
        calls.append(refresh_token)
        if threading.current_thread().name == "first":
            # The second loader has read the old session and waits for the
            # lock (without the lock it refreshes too, and calls has two).
            second_at_the_lock.wait(timeout=1)
            time.sleep(0.05)
        return _cls(A_FRESH, "user-A", "RA2")

    monkeypatch.setattr(cls, "refresh_login", classmethod(slow_refresh_login))
    loaded = {}
    threads = [
        threading.Thread(name=name, target=lambda name=name: loaded.__setitem__(name, cls.from_stored_credentials()))
        for name in ("first", "second")
    ]
    threads[0].start()
    threads[1].start()
    for thread in threads:
        thread.join(timeout=5)

    assert calls == ["RA1"]
    assert {name: client.access_token for name, client in loaded.items()} == {"first": A_FRESH, "second": A_FRESH}
    assert _session(store) == (A_FRESH, "user-A", "RA2")


# --- The settings file itself -------------------------------------------------


@pytest.fixture
def settings_file(tmp_path, monkeypatch):
    from database import schema

    path = tmp_path / "app_settings.json"
    monkeypatch.setattr(schema, "SETTINGS_PATH", path)
    monkeypatch.setattr(cloud_sync, "clear_saved_cloud_password", lambda: [])
    schema.save_app_settings({
        "cloud_access_token": A_STALE,
        "cloud_user_id": "user-A",
        "cloud_refresh_token": "RA1",
        "database_folder": "/data/sporely",
    })
    return schema, path


def test_another_settings_update_cannot_write_back_signed_out_tokens(settings_file, monkeypatch):
    """Codex: a sync worker saving its pull time had read the tokens just before sign-out."""
    schema, _path = settings_file
    real_load = schema._load_app_settings
    worker_read, cleared = threading.Event(), threading.Event()
    worker = None
    finished_during_the_update = []

    def load():
        settings = real_load()
        if threading.current_thread() is worker and not worker_read.is_set():
            worker_read.set()
            finished_during_the_update.append(cleared.wait(timeout=0.3))
        return settings

    monkeypatch.setattr(schema, "_load_app_settings", load)
    worker = threading.Thread(target=schema.update_app_settings, args=({"cloud_last_pull_at": "t1"},))
    worker.start()
    assert worker_read.wait(timeout=5)

    def sign_out():
        cloud_sync.SporelyCloudClient.clear_credentials()
        cleared.set()

    signer = threading.Thread(target=sign_out)
    signer.start()
    worker.join(timeout=5)
    signer.join(timeout=5)

    stored = schema.get_app_settings()
    assert finished_during_the_update == [False]  # the sign-out waited for the update
    assert (stored["cloud_access_token"], stored["cloud_refresh_token"]) == (None, None)
    assert stored["cloud_last_pull_at"] == "t1" and stored["database_folder"] == "/data/sporely"


def test_a_failed_settings_write_leaves_the_file_as_it_was(settings_file):
    schema, path = settings_file
    before = path.read_text(encoding="utf-8")

    with pytest.raises(TypeError):
        schema.update_app_settings({"cloud_last_pull_at": object()})  # not JSON

    assert path.read_text(encoding="utf-8") == before
    assert [p.name for p in path.parent.iterdir()] == ["app_settings.json"]


@pytest.mark.skipif(os.name == "nt", reason="POSIX permissions")
def test_a_settings_write_keeps_the_file_permissions(settings_file, monkeypatch):
    """The file holds tokens: replacing it must not loosen a tightened mode."""
    schema, path = settings_file
    os.chmod(path, 0o600)
    modes_while_writing = []
    real_fdopen = os.fdopen

    def fdopen(descriptor, *args, **kwargs):
        modes_while_writing.append(stat.S_IMODE(os.fstat(descriptor).st_mode))
        return real_fdopen(descriptor, *args, **kwargs)

    monkeypatch.setattr(schema.os, "fdopen", fdopen)
    schema.update_app_settings({"cloud_last_pull_at": "t2"})

    assert modes_while_writing == [0o600]  # the copy is never looser, even while written
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert schema.get_app_settings()["cloud_last_pull_at"] == "t2"


def test_a_settings_file_that_cannot_be_replaced_is_written_in_place(settings_file, monkeypatch):
    """Windows refuses to replace a file another program has open."""
    schema, path = settings_file

    def refuse(_source, _target):
        raise PermissionError("in use")

    monkeypatch.setattr(schema.os, "replace", refuse)
    schema.update_app_settings({"cloud_access_token": None})

    assert schema.get_app_settings()["cloud_access_token"] is None
    assert [p.name for p in path.parent.iterdir()] == ["app_settings.json"]


@pytest.mark.skipif(os.name == "nt", reason="symlinks need privileges on Windows")
def test_a_symlinked_settings_file_is_updated_through_the_link(settings_file, monkeypatch, tmp_path):
    schema, path = settings_file
    link = tmp_path / "linked_settings.json"
    link.symlink_to(path)
    monkeypatch.setattr(schema, "SETTINGS_PATH", link)

    schema.update_app_settings({"cloud_access_token": None})

    assert link.is_symlink()
    assert json.loads(path.read_text(encoding="utf-8"))["cloud_access_token"] is None
