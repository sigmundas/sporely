"""F10: an unlinked database binds only to an account that owns every sampled row.

Runs the real ``ensure_database_linked_to_cloud_user`` /
``_verify_unlinked_database_owner`` and the real
``SporelyCloudClient.list_owned_observation_ids``; only the REST ``_get`` is
replaced by a recorder that answers the owner-filtered query from a fake
cloud table.
"""

import re
import sqlite3
from types import MethodType, SimpleNamespace

import pytest

from utils import cloud_sync

ACCOUNT_A = "00000000-0000-4000-8000-00000000000a"
ACCOUNT_B = "00000000-0000-4000-8000-00000000000b"

# cloud observation id -> owner
CLOUD_OWNERS = {"101": ACCOUNT_A, "102": ACCOUNT_A, "201": ACCOUNT_B}


def _client(user_id):
    client = cloud_sync.SporelyCloudClient("access-token", user_id)
    client.queries = []

    def fake_get(self, path):
        self.queries.append(path)
        owner = re.search(r"user_id=eq\.([^&]+)", path).group(1)
        ids = re.search(r"id=in\.\(([^)]*)\)", path).group(1).split(",")
        # Postgres compares the bigint id, so "0101" matches row 101.
        return [{"id": int(i)} for i in ids if CLOUD_OWNERS.get(str(int(i))) == owner]

    client._get = MethodType(fake_get, client)
    return client


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

    def add(*cloud_ids):
        conn = sqlite3.connect(db_path)
        conn.executemany(
            "INSERT INTO observations (cloud_id, synced_at) VALUES (?, ?)",
            [(cid, "2026-10-01T00:00:00Z" if cid else None) for cid in cloud_ids],
        )
        conn.commit()
        conn.close()

    return SimpleNamespace(settings=settings, add=add)


@pytest.mark.parametrize("account", [ACCOUNT_A, ACCOUNT_B])
def test_database_with_rows_from_two_accounts_refuses_to_bind(local_db, account):
    local_db.add("101", "102", "201", None)
    client = _client(account)

    with pytest.raises(cloud_sync.AccountMismatchError):
        cloud_sync.ensure_database_linked_to_cloud_user(client)

    assert "linked_cloud_user_id" not in local_db.settings
    assert len(client.queries) == 1


def test_single_owner_database_still_binds(local_db):
    local_db.add("101", "102", None)
    client = _client(ACCOUNT_A)

    assert cloud_sync.ensure_database_linked_to_cloud_user(client) == ACCOUNT_A
    assert local_db.settings["linked_cloud_user_id"] == ACCOUNT_A


@pytest.mark.parametrize("bad_id", ["x)or(1", "12a", "²"])
def test_malformed_sampled_id_fails_closed_even_beside_an_owned_row(local_db, bad_id):
    local_db.add("101", bad_id)

    with pytest.raises(cloud_sync.AccountMismatchError):
        cloud_sync.ensure_database_linked_to_cloud_user(_client(ACCOUNT_A))
    assert "linked_cloud_user_id" not in local_db.settings


def test_zero_padded_local_id_matches_the_canonical_cloud_id(local_db):
    local_db.add("0101", "102")

    assert cloud_sync.ensure_database_linked_to_cloud_user(_client(ACCOUNT_A)) == ACCOUNT_A


def test_foreign_only_database_is_still_refused(local_db):
    local_db.add("201")

    with pytest.raises(cloud_sync.AccountMismatchError):
        cloud_sync.ensure_database_linked_to_cloud_user(_client(ACCOUNT_A))
    assert "linked_cloud_user_id" not in local_db.settings
