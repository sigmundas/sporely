"""Stage 2c: sync never silently widens location precision.

Builds before Stage 2c stored a cloud 'hidden'/'region' observation locally
as 'exact' while the sync snapshot kept the raw cloud value. These tests pin
the cloud-wins rule, the push guard, the full-row push after a real edit, the
dirty clear, the conflict auto-push and the one-time repair.
"""
from __future__ import annotations

import sqlite3

import pytest

from database import schema as db_schema
from database.models import ObservationDB
from utils import cloud_sync

CLOUD_ID = "obs-cloud-precision"


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "mushrooms.db"
    monkeypatch.setattr(db_schema, "get_database_path", lambda: path)
    db_schema.init_database()
    return path


def _remote(precision="hidden", **extra):
    row = {
        "id": CLOUD_ID, "date": "2026-08-05", "genus": "Mycena", "species": "galopus",
        "visibility": "public", "is_draft": False, "location_precision": precision,
        "notes": "n", "location": "Somewhere", "gps_latitude": 60.1234567,
        "gps_longitude": 10.1234567, "uncertain": False, "unspontaneous": False,
        "location_public": True, "interesting_comment": False, "publish_target": "artsobs_no",
        "source_type": "personal",
    }
    row.update(extra)
    return row


def _seed(db, *, local_precision="exact", baseline_precision="hidden", notes="n"):
    """A synced row as an old build left it: local 'exact', baseline hidden."""
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO observations (id, cloud_id, date, genus, species, sharing_scope, is_draft,"
        " location_precision, notes, location, gps_latitude, gps_longitude)"
        " VALUES (1, ?, '2026-08-05', 'Mycena', 'galopus', 'public', 0, ?, ?, 'Somewhere',"
        " 60.1234567, 10.1234567)",
        (CLOUD_ID, local_precision, notes),
    )
    conn.commit()
    conn.close()
    snapshot = cloud_sync._cloud_observation_snapshot(_remote(baseline_precision), [], [])
    cloud_sync._store_cloud_observation_snapshot(CLOUD_ID, snapshot)
    return ObservationDB.get_observation(1)


# --- a) cloud wins over an unconfirmed legacy 'exact' -----------------------------------

def test_push_diff_treats_legacy_exact_as_no_change(db):
    local = _seed(db)
    assert "location_precision" not in cloud_sync._observation_push_diff_fields(local, _remote())


def test_dirty_clear_still_clears_legacy_exact(db, monkeypatch):
    local = _seed(db)
    # Media is out of scope here: no stored and no current media signature.
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda _id: "sig")
    monkeypatch.setattr(cloud_sync, "_local_cloud_media_signature", lambda _id: "")
    assert cloud_sync._local_has_real_changes_since_snapshot(local, CLOUD_ID) is False


def test_conflict_detail_does_not_auto_push_legacy_exact(db):
    _seed(db)

    class Client:
        def get_observation(self, _cid):
            return _remote()

        def pull_image_metadata(self, _cid, include_deleted_for_sync=False):
            return []

    detail = cloud_sync.get_conflict_detail(Client(), 1, CLOUD_ID)
    fields = {entry["field"]: entry for entry in detail["automatic_decisions"]["fields"]}
    assert "location_precision" not in fields
    assert all(row["field"] != "location_precision" for row in detail["field_rows"])


def test_repair_restores_baseline_precision_once(db):
    _seed(db)
    assert cloud_sync.repair_legacy_location_precision() == 1
    assert ObservationDB.get_observation(1)["location_precision"] == "hidden"
    assert cloud_sync.repair_legacy_location_precision() == 0  # idempotent


def test_repair_skips_confirmed_choice_and_rows_without_hidden_baseline(db):
    _seed(db)
    cloud_sync.record_confirmed_location_precision(1, "exact")
    assert cloud_sync.repair_legacy_location_precision() == 0
    assert ObservationDB.get_observation(1)["location_precision"] == "exact"


def test_repair_ignores_fuzzed_baseline(db):
    _seed(db, baseline_precision="fuzzed")
    assert cloud_sync.repair_legacy_location_precision() == 0


# --- b/c) push guard incl. full-row push after a real edit ----------------------------

class _PushClient(cloud_sync.SporelyCloudClient):
    def __init__(self):
        super().__init__("token", "user-1")
        self.patches: list[dict] = []

    def _resolve_existing_observation_for_push(self, obs, remote_obs=None):
        return CLOUD_ID

    def _patch(self, path, payload):
        self.patches.append(dict(payload))
        return [{"id": CLOUD_ID}]

    def _sync_observation_selected_taxon(self, *args, **kwargs):
        return None


@pytest.fixture
def no_geography(monkeypatch):
    monkeypatch.setattr(cloud_sync, "_shape_geography_patch_payload", lambda *a, **k: None)


def test_full_row_push_after_notes_edit_keeps_cloud_precision(db, no_geography):
    local = dict(_seed(db), notes="edited")
    client = _PushClient()
    client.push_observation(local, _remote())
    assert client.patches and client.patches[0]["notes"] == "edited"
    assert client.patches[0]["location_precision"] == "hidden"


def test_push_without_remote_uses_snapshot_baseline(db, no_geography):
    local = dict(_seed(db, baseline_precision="region"), notes="edited")
    client = _PushClient()
    client.push_observation(local)
    assert client.patches[0]["location_precision"] == "region"


def test_confirmed_increase_is_pushed_and_consumed(db, no_geography):
    local = dict(_seed(db), notes="edited")
    cloud_sync.record_confirmed_location_precision(1, "exact")
    client = _PushClient()
    client.push_observation(local, _remote())
    assert client.patches[0]["location_precision"] == "exact"
    assert cloud_sync._confirmed_location_precision(1) is None


def test_decrease_is_always_pushed(db, no_geography):
    local = dict(_seed(db, local_precision="hidden", baseline_precision="exact"))
    client = _PushClient()
    client.push_observation(local, _remote("exact"))
    assert client.patches[0]["location_precision"] == "hidden"


def test_confirmation_for_a_different_level_does_not_count(db):
    local = _seed(db)
    cloud_sync.record_confirmed_location_precision(1, "fuzzed")
    guarded = cloud_sync._guard_local_location_precision(local, _remote())
    assert guarded["location_precision"] == "hidden"


# --- privacy slot parity with the server trigger ---------------------------------------

@pytest.mark.parametrize("row,expected", [
    ({"visibility": "public", "location_precision": "exact", "is_draft": False}, False),
    ({"visibility": "public", "location_precision": None, "is_draft": False}, False),
    ({"visibility": None, "location_precision": "exact", "is_draft": False}, False),
    ({"visibility": "public", "location_precision": "fuzzed", "is_draft": False}, True),
    ({"visibility": "public", "location_precision": "region", "is_draft": False}, True),
    ({"visibility": "public", "location_precision": "hidden", "is_draft": False}, True),
    ({"visibility": "friends", "location_precision": "exact", "is_draft": False}, True),
    ({"visibility": "private", "location_precision": "hidden", "is_draft": True}, False),
    ({"visibility": "private", "location_precision": "exact"}, True),  # missing draft = not draft
])
def test_privacy_slot_matches_server_trigger(row, expected):
    assert cloud_sync.cloud_observation_uses_privacy_slot(row) is expected


def test_remote_privacy_slot_query_matches_server_trigger(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-1")
    seen = {}

    class Resp:
        ok = True
        headers = {"Content-Range": "0-0/3"}

    def fake(method, url, **kwargs):
        seen["url"] = url
        return Resp()

    monkeypatch.setattr(client, "_request_with_refresh", fake)
    assert client.count_remote_privacy_slots() == 3
    assert "and=(or(is_draft.is.null,is_draft.eq.false)," in seen["url"]
    assert "or(visibility.neq.public,location_precision.in.(fuzzed,region,hidden)))" in seen["url"]
    assert "visibility.is.null" not in seen["url"]
