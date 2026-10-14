"""Desktop preservation of private ingestion and location provenance.

Contract: docs/supabase-sync-contract.md, "Private ingestion and location
provenance" (desktop requirements 1-9), against sporely-web migration
20261012105630.

The sync scenarios drive the real ``sync_all`` (push, then pull) against the
real SQLite schema and snapshot bookkeeping. Only the network is replaced: the
in-memory cloud of ``test_cloud_sync_remote_only_adoption`` extended with the
server's provenance rules (the coordinate-change invalidation trigger and the
``set_observation_location`` RPC). These are unit-level checks of the desktop
logic; the live server behavior is verified separately (manual gate G4).
"""
from __future__ import annotations

import copy
import json
import sqlite3

import pytest

from database import models, schema
from tests.test_cloud_sync_remote_only_adoption import (
    _FakeCloud,
    _baseline,
    _is_dirty,
    _local,
    _now_text,
    _sync,
    env,  # noqa: F401  (fixture)
)
from utils import cloud_sync
from utils.cloud_sync_impl import anchors
from utils.cloud_sync_impl.image_pull import _adopt_remote_ingest_method
from utils.cloud_sync_impl.pull_only import _PULL_ONLY_BLOCKED_CLIENT_METHODS
from utils.cloud_sync_impl.reconciliation.location_provenance import (
    attribute_coordinate_edit,
    location_pair_is_writable,
)

_BUNDLE = ("gps_latitude", "gps_longitude", "gps_altitude", "gps_accuracy")
_PAIR = ("location_source", "location_status")


class _ProvenanceCloud(_FakeCloud):
    """The in-memory cloud with the provenance migration applied."""

    def _post(self, path, payload):
        rows = super()._post(path, payload)
        row = self.rows[rows[0]["id"]]
        for field in _PAIR:
            row.setdefault(field, None)
        return [copy.deepcopy(row)]

    def _patch(self, path, payload):
        cloud_id = path.split("id=eq.", 1)[1].split("&", 1)[0]
        before = tuple(self.rows[cloud_id].get(field) for field in _BUNDLE)
        super()._patch(path, payload)
        row = self.rows[cloud_id]
        # trg_location_provenance_invalidate: any actual bundle change clears
        # the pair, even when the request resent the old enum labels.
        if tuple(row.get(field) for field in _BUNDLE) != before:
            row["location_source"] = None
            row["location_status"] = None

    def _rpc(self, function_name, payload=None):
        if function_name != "set_observation_location":
            return super()._rpc(function_name, payload)
        args = dict(payload or {})
        assert set(args) == {
            "p_observation_id", "p_gps_latitude", "p_gps_longitude",
            "p_gps_altitude", "p_gps_accuracy", "p_location_source", "p_location_status",
        }, "all seven arguments are sent"
        self.rpcs.append((function_name, args))
        row = self.rows[str(args["p_observation_id"])]
        wanted = (args["p_gps_latitude"], args["p_gps_longitude"], args["p_gps_altitude"], args["p_gps_accuracy"])
        if tuple(row.get(field) for field in _BUNDLE) != wanted:
            row.update(dict(zip(_BUNDLE, wanted)))
            row["location_source"] = row["location_status"] = None
        row["location_source"] = args["p_location_source"]
        row["location_status"] = args["p_location_status"]
        row["updated_at"] = _now_text()
        return args["p_observation_id"]

    def location_rpcs(self) -> list[dict]:
        return [args for name, args in self.rpcs if name == "set_observation_location"]


@pytest.fixture
def penv(env, monkeypatch):
    """``env`` (one linked, synced observation) on a migrated cloud."""
    old_cloud, local_id, cloud_id = env
    cloud = _ProvenanceCloud()
    cloud.rows = old_cloud.rows
    cloud._next_id = old_cloud._next_id
    for field in _PAIR:
        cloud.rows[cloud_id].setdefault(field, None)
    cloud.rows[cloud_id].update(gps_latitude=63.4, gps_longitude=10.4)
    cloud.rows[cloud_id]["updated_at"] = _now_text()
    assert _sync(cloud)["errors"] == []
    assert _local(local_id)["gps_latitude"] == 63.4
    assert not _is_dirty(local_id)
    cloud.patches.clear()
    cloud.posts.clear()
    cloud.rpcs.clear()
    return cloud, local_id, cloud_id


def _pair(row) -> tuple:
    return tuple(row.get(field) for field in _PAIR)


# ── Pure rules ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("pair", "lat", "lon", "ok"),
    [
        (("manual", "acquired"), 63.4, 10.4, True),
        (("image_exif", "acquired"), 63.4, 10.4, True),
        (("none", "skipped"), None, None, True),
        (("none", "acquired"), 63.4, 10.4, False),
        (("manual", "skipped"), None, None, False),
        (("manual", "acquired"), None, None, False),
        (("none", "skipped"), 63.4, 10.4, False),
        (("manual", "acquired"), 95.0, 10.4, False),
        ((None, None), 63.4, 10.4, False),
        (("bogus", "acquired"), 63.4, 10.4, False),
    ],
)
def test_location_pair_is_writable_mirrors_the_server_checks(pair, lat, lon, ok):
    assert location_pair_is_writable(*pair, lat, lon) is ok


def test_attribute_coordinate_edit():
    same = dict(before_lat=63.4, before_lon=10.4)
    assert attribute_coordinate_edit(**same, after_lat=63.4, after_lon=10.4) is None
    assert attribute_coordinate_edit(**same, after_lat=63.4000004, after_lon=10.4) is None, "spinbox rounding"
    assert attribute_coordinate_edit(**same, after_lat=60.0, after_lon=10.0) == ("manual", "acquired")
    assert attribute_coordinate_edit(
        **same, after_lat=60.0, after_lon=10.0, image_exif_coords=(60.0, 10.0),
    ) == ("image_exif", "acquired")
    assert attribute_coordinate_edit(**same, after_lat=None, after_lon=None) == ("none", "skipped")
    assert attribute_coordinate_edit(**same, after_lat=None, after_lon=None, allow_clear=False) is None
    assert attribute_coordinate_edit(**same, after_lat=60.0, after_lon=None) is None


# ── Local storage ───────────────────────────────────────────────────────────


def _columns(table) -> set[str]:
    conn = schema.get_connection()
    try:
        return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    finally:
        conn.close()


def test_schema_adds_nullable_columns_and_upgrades_existing_databases(env):
    _, local_id, _ = env
    assert set(_PAIR) <= _columns("observations")
    assert "ingest_method" in _columns("images")
    conn = schema.get_connection()
    try:
        conn.execute("ALTER TABLE observations DROP COLUMN location_source")
        conn.execute("ALTER TABLE observations DROP COLUMN location_status")
        conn.execute("ALTER TABLE images DROP COLUMN ingest_method")
        conn.commit()
    finally:
        conn.close()
    schema.init_database()
    assert set(_PAIR) <= _columns("observations")
    assert "ingest_method" in _columns("images")
    assert _pair(_local(local_id)) == (None, None)


def test_local_pair_is_invalidated_only_by_actual_unattributed_coordinate_changes(env):
    _, local_id, _ = env
    models.ObservationDB.update_observation(
        local_id, gps_latitude=63.4, gps_longitude=10.4,
        location_source="device_gps", location_status="acquired",
    )
    assert _pair(_local(local_id)) == ("device_gps", "acquired")

    models.ObservationDB.update_observation(local_id, notes="unrelated")
    models.ObservationDB.update_observation(local_id, gps_latitude=63.4, gps_longitude=10.4, allow_nulls=True)
    models.ObservationDB.update_observation(local_id, gps_latitude=63.4000004, gps_longitude=10.4)
    assert _pair(_local(local_id)) == ("device_gps", "acquired"), "unchanged coordinates preserve"

    models.ObservationDB.update_observation(local_id, gps_latitude=60.0, gps_longitude=10.0)
    assert _pair(_local(local_id)) == (None, None), "a stale pair never describes new coordinates"

    models.ObservationDB.update_observation(
        local_id, gps_latitude=None, gps_longitude=None, allow_nulls=True,
        location_source="none", location_status="skipped",
    )
    assert _pair(_local(local_id)) == ("none", "skipped")


def test_ingest_method_is_adopted_locally_as_received_without_dirtying(env):
    _, local_id, _ = env
    conn = schema.get_connection()
    try:
        cursor = conn.execute(
            "INSERT INTO images (observation_id, filepath, image_type) VALUES (?, '', 'field')",
            (local_id,),
        )
        image_id = cursor.lastrowid
        conn.commit()
    finally:
        conn.close()

    def _stored():
        conn = schema.get_connection()
        try:
            return conn.execute("SELECT ingest_method FROM images WHERE id = ?", (image_id,)).fetchone()[0]
        finally:
            conn.close()

    _adopt_remote_ingest_method(image_id, {"id": "img-1"})
    assert _stored() is None, "an absent key leaves the local value unchanged"
    _adopt_remote_ingest_method(image_id, {"ingest_method": "system_camera"})
    assert _stored() == "system_camera"
    _adopt_remote_ingest_method(image_id, {"ingest_method": "future_method"})
    assert _stored() == "future_method", "stored as received, not validated or remapped"
    _adopt_remote_ingest_method(image_id, {"id": "img-1"})
    assert _stored() == "future_method"
    _adopt_remote_ingest_method(image_id, {"ingest_method": None})
    assert _stored() is None, "an explicit NULL is stored"
    assert not _is_dirty(local_id)


# ── Write payloads never carry provenance ───────────────────────────────────


def test_ordinary_write_payloads_exclude_provenance():
    assert not set(_PAIR) & set(cloud_sync._OBS_PUSH_COLS)
    assert "ingest_method" not in cloud_sync._IMG_PUSH_COLS
    assert "ingest_method" not in anchors._METADATA_ONLY_IMG_FIELDS
    local_row = {"id": 7, "image_type": "microscope", "sort_order": 1, "ingest_method": "web_camera"}

    class _Client:
        user_id = "user-123"

    payload = anchors._metadata_only_microscope_image_payload(_Client(), "obs-1", local_row)
    assert "ingest_method" not in payload
    obs_payload = cloud_sync._observation_push_payload(
        {"gps_latitude": 1.0, "gps_longitude": 2.0, "location_source": "manual", "location_status": "acquired"},
        local=True,
    )
    assert not set(_PAIR) & set(obs_payload)


def test_download_only_blocks_the_location_writer():
    assert "set_observation_location" in _PULL_ONLY_BLOCKED_CLIENT_METHODS
    wrapped = cloud_sync.PullOnlyCloudClient(_ProvenanceCloud())
    with pytest.raises(cloud_sync.PullOnlyModeError):
        wrapped.set_observation_location("1", 1.0, 2.0, "manual", "acquired")


# ── Optional columns on reads ───────────────────────────────────────────────


def test_observation_reads_include_the_pair_and_drop_it_on_a_pre_migration_target():
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    paths: list[str] = []

    def _get(path):
        paths.append(path)
        if "location_source" in path:
            raise cloud_sync.CloudSyncError(
                'GET observations: {"code":"42703","message":"column observations.location_source does not exist"}'
            )
        return [{"id": "1"}]

    client._get = _get
    assert client.get_observation("1") == {"id": "1"}
    assert "location_source,location_status" in paths[0]
    assert "location_source" not in paths[1]
    assert "taxon_identity_state" in paths[1], "identity columns are kept"
    assert client.get_observation("1") == {"id": "1"}
    assert len(paths) == 3, "the drop is remembered per client"


def test_image_reads_include_ingest_method_and_drop_it_on_a_pre_migration_target():
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    paths: list[str] = []

    def _get(path):
        paths.append(path)
        if "ingest_method" in path:
            raise cloud_sync.CloudSyncError(
                'GET observation_images: {"message":"column observation_images.ingest_method does not exist"}'
            )
        return [{"id": "img-1", "deleted_at": None}]

    client._get = _get
    assert client.pull_image_metadata("obs-1") == [{"id": "img-1", "deleted_at": None}]
    assert paths[0].endswith(",ingest_method")
    assert "ingest_method" not in paths[1]


# ── Three-way bundle rules ──────────────────────────────────────────────────


def _row(lat=63.4, lon=10.4, pair=None, **extra):
    row = {"id": "c1", "desktop_id": 1, "date": "2026-09-20", "gps_latitude": lat, "gps_longitude": lon, **extra}
    if pair is not None:
        row.update(dict(zip(_PAIR, pair)))
    return row


def _analyze(local, remote, baseline):
    local = dict(local, cloud_id="c1", id=1)
    return cloud_sync._analyze_observation_field_changes(local, remote, baseline)


def test_old_snapshot_adopts_the_cloud_pair_without_a_local_change():
    changes = _analyze(_row(), _row(pair=("device_gps", "acquired")), _row())
    assert changes["local_only_fields"] == []
    assert changes["conflict_fields"] == []
    assert set(_PAIR) <= set(changes["remote_only_fields"])


def test_local_pair_alone_is_never_a_local_change():
    pair = ("device_gps", "acquired")
    changes = _analyze(_row(pair=("manual", "acquired")), _row(pair=pair), _row(pair=pair))
    assert changes["local_only_fields"] == []
    assert set(_PAIR) <= set(changes["remote_only_fields"]), "the local pair converges on the cloud"


def test_target_without_columns_compares_no_pair():
    changes = _analyze(_row(pair=("manual", "acquired")), _row(), _row())
    assert not set(_PAIR) & set(changes["remote_only_fields"] + changes["local_only_fields"])


def test_local_coordinate_change_against_a_cloud_pair_change_is_a_conflict():
    baseline = _row(pair=(None, None))
    remote = _row(pair=("device_gps", "acquired"))
    changes = _analyze(_row(lat=60.0, pair=(None, None)), remote, baseline)
    assert "gps_latitude" in changes["conflict_fields"]
    assert "gps_latitude" not in changes["local_only_fields"]


def test_split_coordinate_edits_move_as_one_bundle():
    baseline = _row(pair=(None, None))
    changes = _analyze(_row(lat=60.0), _row(lon=11.0, pair=(None, None)), baseline)
    assert {"gps_latitude", "gps_longitude"} <= set(changes["conflict_fields"])
    assert not {"gps_latitude", "gps_longitude"} & set(changes["local_only_fields"] + changes["remote_only_fields"])


def test_same_new_coordinates_on_both_sides_are_shared_and_take_the_cloud_pair():
    baseline = _row(pair=(None, None))
    changes = _analyze(
        _row(lat=60.0, pair=("manual", "acquired")),
        _row(lat=60.0, pair=("device_gps", "acquired")),
        baseline,
    )
    assert "gps_latitude" in changes["shared_same_fields"]
    assert changes["conflict_fields"] == []
    assert set(_PAIR) <= set(changes["remote_only_fields"])


# ── Sync scenarios ──────────────────────────────────────────────────────────


def test_cloud_pair_reaches_desktop_and_survives_an_unrelated_desktop_edit(penv):
    cloud, local_id, cloud_id = penv
    cloud.edit_from_other_device(cloud_id, location_source="device_gps", location_status="acquired")
    assert _sync(cloud)["errors"] == []
    assert cloud.writes == 0, "adopting provenance is not a cloud write"
    assert _pair(_local(local_id)) == ("device_gps", "acquired")
    assert _pair(_baseline(cloud_id)) == ("device_gps", "acquired")
    assert not _is_dirty(local_id)

    models.ObservationDB.update_observation(local_id, notes="desktop edit")
    assert _sync(cloud)["errors"] == []
    assert len(cloud.patches) == 1
    patch = cloud.patches[0]
    assert patch["notes"] == "desktop edit"
    assert not set(_PAIR) & set(patch), "an ordinary PATCH never sends provenance"
    assert not {"gps_latitude", "gps_longitude"} & set(patch), "unchanged coordinates are not resent"
    assert cloud.location_rpcs() == []
    assert _pair(cloud.rows[cloud_id]) == ("device_gps", "acquired")

    writes = cloud.writes
    assert _sync(cloud)["errors"] == []
    assert cloud.writes == writes, "no-op sync stays write-free"


def test_legacy_coordinate_change_patches_and_records_the_invalidated_pair(penv):
    cloud, local_id, cloud_id = penv
    cloud.edit_from_other_device(cloud_id, location_source="device_gps", location_status="acquired")
    assert _sync(cloud)["errors"] == []

    # Prepare Images style write: coordinates of unknown origin.
    models.ObservationDB.update_observation(local_id, gps_latitude=60.0, gps_longitude=10.0, allow_nulls=True)
    assert _pair(_local(local_id)) == (None, None)
    assert _sync(cloud)["errors"] == []
    assert cloud.location_rpcs() == []
    assert cloud.patches[-1]["gps_latitude"] == 60.0
    assert _pair(cloud.rows[cloud_id]) == (None, None), "server invalidation"
    assert _pair(_baseline(cloud_id)) == (None, None)
    assert not _is_dirty(local_id)

    writes = cloud.writes
    assert _sync(cloud)["errors"] == []
    assert cloud.writes == writes


@pytest.mark.parametrize(
    ("lat", "lon", "pair"),
    [
        (60.0, 10.0, ("manual", "acquired")),
        (60.0, 10.0, ("image_exif", "acquired")),
        (None, None, ("none", "skipped")),
    ],
)
def test_attributed_coordinate_change_uses_the_aware_operation(penv, lat, lon, pair):
    cloud, local_id, cloud_id = penv
    models.ObservationDB.update_observation(
        local_id, gps_latitude=lat, gps_longitude=lon, allow_nulls=True,
        location_source=pair[0], location_status=pair[1],
    )
    assert _sync(cloud)["errors"] == []
    assert cloud.location_rpcs() == [{
        "p_observation_id": int(cloud_id),
        "p_gps_latitude": lat,
        "p_gps_longitude": lon,
        "p_gps_altitude": None,
        "p_gps_accuracy": None,
        "p_location_source": pair[0],
        "p_location_status": pair[1],
    }]
    for patch in cloud.patches:
        assert not {"gps_latitude", "gps_longitude"} & set(patch), "coordinates go only through the RPC"
    row = cloud.rows[cloud_id]
    assert (row["gps_latitude"], row["gps_longitude"]) == (lat, lon)
    assert _pair(row) == pair
    assert _pair(_baseline(cloud_id)) == pair
    assert _pair(_local(local_id)) == pair
    assert not _is_dirty(local_id)

    writes = cloud.writes
    assert _sync(cloud)["errors"] == []
    assert cloud.writes == writes


def test_missing_rpc_falls_back_to_the_ordinary_coordinate_patch(penv, monkeypatch):
    cloud, local_id, cloud_id = penv
    real_rpc = cloud._rpc

    def _rpc(function_name, payload=None):
        if function_name == "set_observation_location":
            raise cloud_sync.CloudSyncError(
                'RPC set_observation_location: {"code":"PGRST202","message":"Could not find the function '
                'public.set_observation_location(...) in the schema cache"}'
            )
        return real_rpc(function_name, payload)

    monkeypatch.setattr(cloud, "_rpc", _rpc)
    models.ObservationDB.update_observation(
        local_id, gps_latitude=60.0, gps_longitude=10.0,
        location_source="manual", location_status="acquired",
    )
    assert _sync(cloud)["errors"] == []
    assert cloud.patches[-1]["gps_latitude"] == 60.0
    assert cloud.rows[cloud_id]["gps_latitude"] == 60.0


def test_old_snapshot_without_provenance_keys_adopts_and_writes_nothing(penv):
    cloud, local_id, cloud_id = penv
    cloud.rows[cloud_id].update(location_source="image_exif", location_status="acquired")
    raw = json.loads(cloud_sync._load_cloud_observation_snapshot(cloud_id))
    for field in _PAIR:
        raw["observation"].pop(field, None)
    cloud_sync._store_cloud_observation_snapshot(cloud_id, json.dumps(raw))
    cloud.rows[cloud_id]["updated_at"] = _now_text()

    assert _sync(cloud)["errors"] == []
    assert cloud.writes == 0
    assert _pair(_local(local_id)) == ("image_exif", "acquired")
    assert not _is_dirty(local_id)


def test_cloud_coordinate_change_brings_its_pair(penv):
    cloud, local_id, cloud_id = penv
    cloud.edit_from_other_device(
        cloud_id, gps_latitude=59.9, gps_longitude=10.7,
        location_source="device_gps", location_status="acquired",
    )
    assert _sync(cloud)["errors"] == []
    local = _local(local_id)
    assert (local["gps_latitude"], local["gps_longitude"]) == (59.9, 10.7)
    assert _pair(local) == ("device_gps", "acquired")
    assert cloud.writes == 0


def _download_only(cloud):
    return cloud_sync.sync_all(
        cloud, sync_images=False, materialize_remote_images=True, full_pull=False, pull_only=True,
    )


def test_download_only_adopts_provenance_with_zero_cloud_writes(penv):
    cloud, local_id, cloud_id = penv
    cloud.edit_from_other_device(cloud_id, location_source="device_gps", location_status="acquired")
    result = _download_only(cloud)
    assert cloud.writes == 0
    assert not result.get("blocked_write_attempts")
    assert _pair(_local(local_id)) == ("device_gps", "acquired")


def test_download_only_never_pushes_a_pending_attributed_change(penv):
    cloud, local_id, cloud_id = penv
    models.ObservationDB.update_observation(
        local_id, gps_latitude=60.0, gps_longitude=10.0,
        location_source="manual", location_status="acquired",
    )
    result = _download_only(cloud)
    assert cloud.writes == 0
    assert not result.get("blocked_write_attempts")
    assert cloud.rows[cloud_id]["gps_latitude"] == 63.4


def test_new_observation_insert_carries_a_known_pair_and_omits_an_unknown_one(penv):
    cloud, _, _ = penv
    attributed = models.ObservationDB.create_observation(
        date="2026-09-21", genus="Boletus", species="edulis",
        gps_latitude=61.0, gps_longitude=9.0,
        location_source="image_exif", location_status="acquired",
    )
    unknown = models.ObservationDB.create_observation(
        date="2026-09-22", genus="Russula", species="emetica",
        gps_latitude=62.0, gps_longitude=8.0,
    )
    assert _sync(cloud)["errors"] == []
    posts = {post["sync_key"]: post for post in cloud.posts}
    assert all("desktop_id" not in post for post in cloud.posts)
    attributed_key = models.ObservationDB.get_observation(attributed)["sync_key"]
    unknown_key = models.ObservationDB.get_observation(unknown)["sync_key"]
    assert _pair(posts[attributed_key]) == ("image_exif", "acquired")
    assert not set(_PAIR) & set(posts[unknown_key])
    assert cloud.location_rpcs() == []

    writes = cloud.writes
    assert _sync(cloud)["errors"] == []
    assert cloud.writes == writes


def test_insert_retries_without_the_pair_on_a_pre_migration_target(penv, monkeypatch):
    cloud, _, _ = penv
    real_post = cloud._post
    attempts: list[dict] = []

    def _post(path, payload):
        attempts.append(dict(payload))
        if "location_source" in payload:
            raise cloud_sync.CloudSyncError(
                'POST observations: {"code":"PGRST204","message":"Could not find the \'location_source\' '
                'column of \'observations\' in the schema cache"}'
            )
        return real_post(path, payload)

    monkeypatch.setattr(cloud, "_post", _post)
    models.ObservationDB.create_observation(
        date="2026-09-21", genus="Boletus", species="edulis",
        gps_latitude=61.0, gps_longitude=9.0,
        location_source="manual", location_status="acquired",
    )
    assert _sync(cloud)["errors"] == []
    assert len(attempts) == 2
    assert not set(_PAIR) & set(attempts[1])


# ── Review round 2: full bundle, conflict review/execution ──────────────────


@pytest.mark.parametrize("field", ["gps_altitude", "gps_accuracy"])
def test_cloud_only_altitude_or_accuracy_change_conflicts_with_a_local_coordinate_change(field):
    baseline = _row(pair=(None, None), gps_altitude=120.0, gps_accuracy=10.0)
    remote = dict(baseline, **{field: 3.0})
    changes = _analyze(_row(lat=60.0, pair=("manual", "acquired")), remote, baseline)
    assert "gps_latitude" in changes["conflict_fields"]
    assert changes["local_only_fields"] == []


def test_local_absence_of_altitude_and_accuracy_is_not_a_local_change():
    baseline = _row(pair=(None, None), gps_altitude=120.0, gps_accuracy=10.0)
    changes = _analyze(_row(notes="edit"), dict(baseline), baseline)
    assert not {"gps_altitude", "gps_accuracy"} & set(changes["local_only_fields"] + changes["conflict_fields"])
    assert "notes" in changes["local_only_fields"]


def test_baseline_without_altitude_and_accuracy_is_no_evidence_of_a_change():
    baseline = _row(pair=(None, None))
    remote = _row(pair=(None, None), gps_altitude=120.0, gps_accuracy=3.0)
    changes = _analyze(_row(lat=60.0), remote, baseline)
    assert changes["local_only_fields"] == ["gps_latitude"]


def test_observation_reads_and_snapshots_carry_altitude_and_accuracy():
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    assert "gps_altitude" in client._observation_select_columns()
    assert "gps_accuracy" in client._observation_select_columns()
    snapshot = json.loads(cloud_sync._cloud_observation_snapshot(
        _row(pair=(None, None), gps_altitude=120.0, gps_accuracy=4.0), [], [],
    ))["observation"]
    assert (snapshot["gps_altitude"], snapshot["gps_accuracy"]) == (120.0, 4.0)
    old = json.loads(cloud_sync._cloud_observation_snapshot(_row(), [], []))["observation"]
    assert "gps_accuracy" not in old


def test_concurrent_cloud_accuracy_change_blocks_an_attributed_local_push(penv):
    cloud, local_id, cloud_id = penv
    cloud.rows[cloud_id].update(gps_altitude=120.0, gps_accuracy=10.0)
    cloud.rows[cloud_id]["updated_at"] = _now_text()
    assert _sync(cloud)["errors"] == []
    assert _baseline(cloud_id)["gps_accuracy"] == 10.0

    cloud.edit_from_other_device(cloud_id, gps_accuracy=3.0)
    models.ObservationDB.update_observation(
        local_id, gps_latitude=60.0, gps_longitude=10.0,
        location_source="manual", location_status="acquired",
    )
    result = _sync(cloud)
    assert any("review" in str(error).lower() for error in result["errors"]), result["errors"]
    assert cloud.location_rpcs() == []
    assert cloud.rows[cloud_id]["gps_accuracy"] == 3.0, "the cloud change is not overwritten"
    assert cloud.rows[cloud_id]["gps_latitude"] == 63.4


def test_plan_resolves_both_coordinates_from_one_side():
    from utils.cloud_sync_impl.conflict_plan import _build_plan_operations

    ops = _build_plan_operations([{"kind": "field", "field": "gps_latitude", "choice": "local"}])
    assert {(op["op"], op["field"]) for op in ops} == {
        ("push_field", "gps_latitude"), ("push_field", "gps_longitude"),
    }
    with pytest.raises(cloud_sync.CloudSyncError):
        _build_plan_operations([
            {"kind": "field", "field": "gps_latitude", "choice": "local"},
            {"kind": "field", "field": "gps_longitude", "choice": "cloud"},
        ])


def _conflicting_location(cloud, local_id, cloud_id, **cloud_fields):
    cloud.edit_from_other_device(cloud_id, **cloud_fields)
    models.ObservationDB.update_observation(
        local_id, gps_latitude=60.0, gps_longitude=10.0,
        location_source="manual", location_status="acquired",
    )
    result = _sync(cloud)
    assert any("review" in str(error).lower() for error in result["errors"]), result["errors"]
    return cloud_sync.get_conflict_detail(cloud, local_id)


def test_split_coordinate_edits_are_one_bundle_row_in_conflict_review(penv):
    cloud, local_id, cloud_id = penv
    # Cloud moved only the longitude; the desktop moved both coordinates.
    detail = _conflicting_location(cloud, local_id, cloud_id, gps_longitude=11.0)
    rows = {row["field"]: row for row in detail["field_rows"]}
    assert {"gps_latitude", "gps_longitude"} <= set(rows)
    assert {rows["gps_latitude"]["bundle"], rows["gps_longitude"]["bundle"]} == {"location"}
    automatic = {entry["field"] for entry in detail["automatic_decisions"]["fields"]}
    assert not {"gps_latitude", "gps_longitude"} & automatic


def test_provenance_only_cloud_change_is_a_location_conflict_in_review(penv):
    cloud, local_id, cloud_id = penv
    detail = _conflicting_location(
        cloud, local_id, cloud_id, location_source="unknown", location_status="acquired",
    )
    assert {"gps_latitude", "gps_longitude"} <= {row["field"] for row in detail["field_rows"]}


def test_keeping_an_attributed_local_location_uses_the_aware_operation(penv):
    cloud, local_id, cloud_id = penv
    detail = _conflicting_location(cloud, local_id, cloud_id, gps_longitude=11.0)
    cloud.patches.clear()
    cloud_sync.resolve_conflict_plan(cloud, local_id, plan={
        "items": [{"kind": "field", "field": "gps_latitude", "choice": "local"}],
        "baseline": detail["plan_baseline"],
    })
    assert [(args["p_gps_latitude"], args["p_gps_longitude"], args["p_location_source"])
            for args in cloud.location_rpcs()] == [(60.0, 10.0, "manual")]
    for patch in cloud.patches:
        assert not {"gps_latitude", "gps_longitude"} & set(patch)
    row = cloud.rows[cloud_id]
    assert (row["gps_latitude"], row["gps_longitude"], *_pair(row)) == (60.0, 10.0, "manual", "acquired")


def test_taking_the_cloud_location_brings_both_coordinates_and_the_pair(penv):
    cloud, local_id, cloud_id = penv
    detail = _conflicting_location(
        cloud, local_id, cloud_id, gps_longitude=11.0,
        location_source="device_gps", location_status="acquired",
    )
    cloud_sync.resolve_conflict_plan(cloud, local_id, plan={
        "items": [{"kind": "field", "field": "gps_latitude", "choice": "cloud"}],
        "baseline": detail["plan_baseline"],
    })
    local = _local(local_id)
    assert (local["gps_latitude"], local["gps_longitude"]) == (63.4, 11.0)
    assert _pair(local) == ("device_gps", "acquired")
    assert cloud.location_rpcs() == []


# ── Review round 3: conflict-plan drift and retry cover the whole bundle ────


def _keep_local_plan(detail):
    return {
        "items": [{"kind": "field", "field": "gps_latitude", "choice": "local"}],
        "baseline": detail["plan_baseline"],
    }


@pytest.mark.parametrize(
    "drift",
    [
        {"location_source": "unknown"},
        {"location_status": "unknown", "location_source": "unknown"},
        {"gps_accuracy": 3.0},
        {"gps_altitude": 250.0},
    ],
)
def test_bundle_member_drift_after_review_is_refused_before_any_write(penv, drift):
    cloud, local_id, cloud_id = penv
    cloud.rows[cloud_id].update(gps_altitude=120.0, gps_accuracy=10.0)
    detail = _conflicting_location(cloud, local_id, cloud_id, gps_longitude=11.0,
                                   location_source="device_gps", location_status="acquired")
    cloud.edit_from_other_device(cloud_id, **drift)
    cloud.patches.clear()
    with pytest.raises(cloud_sync.CloudSyncError):
        cloud_sync.resolve_conflict_plan(cloud, local_id, plan=_keep_local_plan(detail))
    assert cloud.location_rpcs() == []
    assert cloud.patches == []


def test_local_pair_drift_after_review_is_refused(penv):
    cloud, local_id, cloud_id = penv
    detail = _conflicting_location(cloud, local_id, cloud_id, gps_longitude=11.0)
    models.ObservationDB.update_observation(local_id, location_source="image_exif", location_status="acquired")
    with pytest.raises(cloud_sync.CloudSyncError):
        cloud_sync.resolve_conflict_plan(cloud, local_id, plan=_keep_local_plan(detail))
    assert cloud.location_rpcs() == []


def _completed_location_ops(result):
    return [
        op for op in result["operations"]
        if op.get("op") == "push_field" and op.get("field") in {"gps_latitude", "gps_longitude"}
    ]


def test_retry_accepts_the_aware_push_effects_and_detects_altered_members(penv):
    cloud, local_id, cloud_id = penv
    cloud.rows[cloud_id].update(gps_altitude=120.0, gps_accuracy=10.0)
    detail = _conflicting_location(cloud, local_id, cloud_id, gps_longitude=11.0)
    plan = _keep_local_plan(detail)
    result = cloud_sync.resolve_conflict_plan(cloud, local_id, plan=plan)
    ops = _completed_location_ops(result)
    assert {op["expected_after"]["location_bundle"]["location_source"] for op in ops} == {"manual"}
    assert {op["expected_after"]["location_bundle"]["gps_accuracy"] for op in ops} == {None}
    rpcs = len(cloud.location_rpcs())

    retried = cloud_sync.resolve_conflict_plan(cloud, local_id, plan=plan, prior_result=result)
    assert retried["plan_applied"]
    assert len(cloud.location_rpcs()) == rpcs, "completed coordinate ops are not re-dispatched"

    cloud.edit_from_other_device(cloud_id, location_source="unknown")
    with pytest.raises(cloud_sync.CloudSyncError):
        cloud_sync.resolve_conflict_plan(cloud, local_id, plan=plan, prior_result=result)


def test_retry_accepts_the_servers_legacy_invalidation_and_detects_altered_members(penv):
    cloud, local_id, cloud_id = penv
    cloud.rows[cloud_id].update(gps_altitude=120.0, gps_accuracy=10.0)
    # Unattributed local coordinate change versus a cloud pair-only change.
    cloud.edit_from_other_device(cloud_id, location_source="device_gps", location_status="acquired")
    models.ObservationDB.update_observation(local_id, gps_latitude=60.0, gps_longitude=10.0)
    assert any("review" in str(e).lower() for e in _sync(cloud)["errors"])
    detail = cloud_sync.get_conflict_detail(cloud, local_id)
    plan = _keep_local_plan(detail)
    result = cloud_sync.resolve_conflict_plan(cloud, local_id, plan=plan)
    assert cloud.location_rpcs() == []
    assert _pair(cloud.rows[cloud_id]) == (None, None), "server invalidation"
    bundles = [op["expected_after"]["location_bundle"] for op in _completed_location_ops(result)]
    assert bundles and all(b["location_source"] is None and b["gps_accuracy"] == 10.0 for b in bundles)

    assert cloud_sync.resolve_conflict_plan(cloud, local_id, plan=plan, prior_result=result)["plan_applied"]
    cloud.edit_from_other_device(cloud_id, gps_accuracy=2.0)
    with pytest.raises(cloud_sync.CloudSyncError):
        cloud_sync.resolve_conflict_plan(cloud, local_id, plan=plan, prior_result=result)


# ── Review round 4: a completed op explains only the side it wrote ──────────


def test_retry_after_a_cloud_push_still_detects_local_pair_drift(penv):
    cloud, local_id, cloud_id = penv
    detail = _conflicting_location(cloud, local_id, cloud_id, gps_longitude=11.0)
    plan = _keep_local_plan(detail)
    result = cloud_sync.resolve_conflict_plan(cloud, local_id, plan=plan)
    assert cloud_sync.resolve_conflict_plan(cloud, local_id, plan=plan, prior_result=result)["plan_applied"]

    models.ObservationDB.update_observation(local_id, location_source="image_exif", location_status="acquired")
    with pytest.raises(cloud_sync.CloudSyncError):
        cloud_sync.resolve_conflict_plan(cloud, local_id, plan=plan, prior_result=result)


def test_retry_after_a_local_pull_accepts_its_effects_and_detects_cloud_pair_drift(penv):
    cloud, local_id, cloud_id = penv
    detail = _conflicting_location(
        cloud, local_id, cloud_id, gps_longitude=11.0,
        location_source="device_gps", location_status="acquired",
    )
    plan = {
        "items": [{"kind": "field", "field": "gps_latitude", "choice": "cloud"}],
        "baseline": detail["plan_baseline"],
    }
    result = cloud_sync.resolve_conflict_plan(cloud, local_id, plan=plan)
    pulled = [op for op in result["operations"] if op.get("op") == "pull_field"
              and op.get("field") in {"gps_latitude", "gps_longitude"}]
    assert {op["expected_after"]["location_bundle"]["location_source"] for op in pulled} == {"device_gps"}
    assert cloud_sync.resolve_conflict_plan(cloud, local_id, plan=plan, prior_result=result)["plan_applied"]

    cloud.edit_from_other_device(cloud_id, location_source="unknown")
    with pytest.raises(cloud_sync.CloudSyncError):
        cloud_sync.resolve_conflict_plan(cloud, local_id, plan=plan, prior_result=result)
