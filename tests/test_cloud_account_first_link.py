"""First-link account binding: an unlinked DB must not adopt a foreign account."""

import sqlite3
from types import MethodType, SimpleNamespace

import pytest

from utils import cloud_sync


class _Client:
    def __init__(self, user_id, owned=()):
        self.access_token = None
        self.user_id = user_id
        self._owned = {str(value) for value in owned}
        self.ownership_queries = []

    def list_owned_observation_ids(self, cloud_ids):
        ids = list(cloud_ids)
        self.ownership_queries.append(ids)
        return {value for value in ids if value in self._owned}


@pytest.fixture
def local_db(monkeypatch, tmp_path):
    db_path = tmp_path / "mushrooms.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE observations (id INTEGER PRIMARY KEY, cloud_id TEXT, synced_at TEXT)")
    conn.commit()
    conn.close()
    settings = {}
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda updates: settings.update(updates))

    def add(*rows):
        conn = sqlite3.connect(db_path)
        conn.executemany("INSERT INTO observations (cloud_id, synced_at) VALUES (?, ?)", rows)
        conn.commit()
        conn.close()

    return SimpleNamespace(settings=settings, add=add)


def test_new_database_links_to_signed_in_account(local_db):
    local_db.add((None, None))
    client = _Client("acct-new")

    assert cloud_sync.ensure_database_linked_to_cloud_user(client) == "acct-new"
    assert local_db.settings["linked_cloud_user_id"] == "acct-new"
    assert client.ownership_queries == []


def test_unlinked_copy_of_another_accounts_data_is_refused(local_db):
    local_db.add(("101", "2026-10-01T00:00:00Z"), ("102", "2026-10-02T00:00:00Z"))
    client = _Client("acct-other", owned=())

    with pytest.raises(cloud_sync.AccountMismatchError):
        cloud_sync.ensure_database_linked_to_cloud_user(client)
    assert "linked_cloud_user_id" not in local_db.settings
    assert sorted(client.ownership_queries[0]) == ["101", "102"]


def test_unlinked_database_links_when_account_owns_its_observations(local_db):
    local_db.add(("101", "2026-10-01T00:00:00Z"), (None, None))
    client = _Client("acct-owner", owned={"101"})

    assert cloud_sync.ensure_database_linked_to_cloud_user(client) == "acct-owner"
    assert local_db.settings["linked_cloud_user_id"] == "acct-owner"


def test_ownership_lookup_failure_does_not_link(local_db):
    local_db.add(("101", "2026-10-01T00:00:00Z"))
    client = _Client("acct-owner")

    def fail(_ids):
        raise cloud_sync.CloudSyncError("network down")

    client.list_owned_observation_ids = fail
    with pytest.raises(cloud_sync.CloudSyncError):
        cloud_sync.ensure_database_linked_to_cloud_user(client)
    assert "linked_cloud_user_id" not in local_db.settings


def test_linked_database_skips_ownership_lookup(local_db):
    local_db.add(("101", "2026-10-01T00:00:00Z"))
    local_db.settings["linked_cloud_user_id"] = "acct-owner"
    client = _Client("acct-owner")

    assert cloud_sync.ensure_database_linked_to_cloud_user(client) == "acct-owner"
    assert client.ownership_queries == []
    with pytest.raises(cloud_sync.AccountMismatchError):
        cloud_sync.ensure_database_linked_to_cloud_user(_Client("acct-other"))


def test_list_owned_observation_ids_queries_only_numeric_ids_for_this_account(monkeypatch):
    client = cloud_sync.SporelyCloudClient("access-token", "acct-owner")
    paths = []

    def fake_get(self, path):
        paths.append(path)
        return [{"id": 101}]

    monkeypatch.setattr(client, "_get", MethodType(fake_get, client))

    assert client.list_owned_observation_ids(["101", " 102 ", "x)or(1", None]) == {"101"}
    assert paths == ["observations?select=id&user_id=eq.acct-owner&id=in.(101,102)&limit=2"]
    assert client.list_owned_observation_ids([None, "abc"]) == set()
    assert len(paths) == 1


@pytest.mark.parametrize(
    "error, taken",
    [
        ('{"code":"23505","message":"duplicate key value violates unique constraint \\"profiles_username_key\\""}', True),
        ('{"code":"23505","message":"duplicate key value violates unique constraint \\"other_key\\""}', False),
    ],
)
def test_profile_error_only_blames_username_for_username_conflicts(error, taken):
    from ui import main_window

    dialog = SimpleNamespace(tr=lambda text: text)
    message, details = main_window.SettingsHubDialog._profile_sync_error_notice(dialog, Exception(error))
    assert ("username is already taken" in message) is taken
    # Details carry the server's error only when the message does not say it all.
    assert (details is None) is taken


def test_refused_first_link_at_sign_in_saves_no_credentials(local_db, monkeypatch):
    from ui import main_window

    local_db.add(("101", "2026-10-01T00:00:00Z"))
    client = _Client("acct-other", owned=())
    saved = []
    client.save_credentials = lambda **kwargs: saved.append(kwargs)
    warnings = []
    monkeypatch.setattr(main_window.QMessageBox, "warning", lambda *args: warnings.append(args[2]))
    monkeypatch.setattr(main_window, "update_app_settings", lambda updates: saved.append(updates))
    dialog = SimpleNamespace(tr=lambda text: text, _cloud_client="previous")

    main_window.ArtsobservasjonerSettingsDialog._on_cloud_login_success(dialog, client, "other@example.com")

    assert saved == []
    assert dialog._cloud_client is None
    assert "linked_cloud_user_id" not in local_db.settings
    assert cloud_sync.ACCOUNT_MISMATCH_MESSAGE in warnings[0]
