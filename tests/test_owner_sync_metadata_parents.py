"""Owner-sync metadata-only parents for byte-excluded microscope images.

Found in the observation-917 integrity round-trip: microscope image 7305 is
excluded from cloud byte storage and carries only three cheilocystidia
measurements. The only metadata-only parent the desktop created was the
public-spore anchor, so 7305 got no cloud row and its measurements never
reached the cloud or the owner's other devices (127 local vs 124 cloud).

Two separate intents may now require a parent:

* public spore data (`microscope_image_requires_public_spore_anchor`,
  unchanged), marked ``metadata_purpose = 'public_microscopy'``;
* the owner's own cross-device measurements of any type
  (`microscope_image_requires_owner_sync_anchor`), marked ``'owner_sync'`` —
  created only when the server confirms the capability (sporely-web migration
  20260925120000, which also keeps such parents out of every public surface).

Whether a parent is ever public is the server's decision (marker AND verified
public child data); the desktop records intent only. These tests run the real
push chain on the stateful cloud stand-in from
``test_cloud_media_measurement_mosaic_chain``.
"""
from __future__ import annotations

import sqlite3

import pytest

from database import models, schema
from utils import cloud_sync
from tests.test_cloud_media_measurement_mosaic_chain import (
    _ChainClient,
    _ChainHarness,
    _ID_FILTER,
    _add_microscope_image,
    _image_cloud_id,
    _observation_status,
    _write_microscope_source,
    db,  # noqa: F401  (fixture)
)

CHEILOCYSTIDIA = [
    # Observation 917's three rows on image 7305 (local ids 7683-7685).
    (7683, 31.673798209905865, 8.36010427786138, 2308.675419687702, 2883.823745237539),
    (7684, 33.3881450401931, 9.859852149021274, 2464.6590475702415, 3088.5244712055755),
    (7685, 38.556744606582484, 9.603788526838267, 1673.4249796983725, 2166.351959398218),
]


class _OwnerSyncChainClient(_ChainClient):
    """The chain stand-in on a server that has the owner-sync capability.

    Records PATCHes (idempotence), keeps each measurement's full payload
    (so a fresh profile can reconstruct type and geometry) and supports the
    canonical tombstone push.
    """

    def __init__(self, *args, capability: bool = True, **kwargs):
        super().__init__(*args, **kwargs)
        self.capability = capability
        self.patches: list[tuple[str, dict]] = []
        self.soft_deleted: list[str] = []

    def _observation_images_support_metadata_purpose(self):
        return self.capability

    def _patch(self, path, payload):
        self.patches.append((str(path), dict(payload or {})))
        return super()._patch(path, payload)

    def push_measurement(self, meas, cloud_image_id, remote_measurement_cache=None):
        cloud_id = super().push_measurement(meas, cloud_image_id, remote_measurement_cache)
        row = next(r for r in self.remote_measurements if r["id"] == cloud_id)
        for field in ("measurement_type", "p1_x", "p1_y", "p2_x", "p2_y",
                      "p3_x", "p3_y", "p4_x", "p4_y", "gallery_rotation", "measured_at"):
            row[field] = meas.get(field)
        return cloud_id

    def soft_delete_image(self, cloud_image_id, deleted_at=None):
        self.soft_deleted.append(str(cloud_image_id))
        row = self._row_for_cloud_id(cloud_image_id)
        if row is not None:
            row["deleted_at"] = deleted_at or "2026-09-25T00:00:00Z"


def _harness(monkeypatch, db_path, tmp_path, *, capability=True, real_tombstones=False):
    # The chain fixture's schema predates the real unique index the canonical
    # tombstone upsert relies on (database/schema.py
    # idx_image_tombstones_deleted_cloud_id).
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_image_tombstones_deleted_cloud_id "
        "ON image_tombstones(deleted_cloud_id)"
    )
    conn.commit()
    conn.close()
    real_tombstone_push = cloud_sync._push_pending_image_tombstones
    harness = _ChainHarness(monkeypatch, db_path, tmp_path)
    client = _OwnerSyncChainClient(
        harness.remote_obs, list(harness.client.remote_images), capability=capability,
    )
    harness.client = client
    if real_tombstones:
        monkeypatch.setattr(cloud_sync, "_push_pending_image_tombstones", real_tombstone_push)
    return harness


def _add_measurements(db_path, *, image_id, rows, measurement_type):
    conn = sqlite3.connect(db_path)
    try:
        for measurement_id, length, width, p1_x, p2_x in rows:
            conn.execute(
                """
                INSERT INTO spore_measurements (
                    id, image_id, length_um, width_um, measurement_type,
                    gallery_rotation, measured_at,
                    p1_x, p1_y, p2_x, p2_y, p3_x, p3_y, p4_x, p4_y
                ) VALUES (?, ?, ?, ?, ?, 0, '2026-09-15 17:37:31', ?, 10, ?, 10, ?, 5, ?, 15)
                """,
                (measurement_id, image_id, length, width, measurement_type,
                 p1_x, p2_x, (p1_x + p2_x) / 2, (p1_x + p2_x) / 2),
            )
        conn.commit()
    finally:
        conn.close()


def _set_spore_visibility(db_path, value):
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE observations SET spore_data_visibility = ? WHERE id = 1", (value,))
    conn.commit()
    conn.close()


def _mark_dirty(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE observations SET sync_status = 'dirty' WHERE id = 1")
    conn.commit()
    conn.close()


def _seed_917_image(db_path, tmp_path, image_id=12):
    """An excluded microscope image with only three cheilocystidia."""
    source = _write_microscope_source(tmp_path / f"P9150713-{image_id}.png")
    _add_microscope_image(db_path, image_id=image_id, filepath=source)
    _add_measurements(db_path, image_id=image_id, rows=CHEILOCYSTIDIA, measurement_type="cheilocystidia")
    cloud_sync._set_cloud_image_storage_intent_initialized_ids(1, {11, image_id})
    cloud_sync._set_cloud_image_storage_excluded_image_ids(1, {image_id})


# ── Predicates ───────────────────────────────────────────────────────────────


def test_owner_sync_predicate_is_independent_of_type_and_visibility(db):
    db_path, tmp_path = db
    _seed_917_image(db_path, tmp_path)
    assert cloud_sync.microscope_image_requires_public_spore_anchor(12) is False
    assert cloud_sync.microscope_image_requires_owner_sync_anchor(12) is True
    _set_spore_visibility(db_path, "private")
    assert cloud_sync.microscope_image_requires_owner_sync_anchor(12) is True


def test_capability_probe_fails_closed():
    class _NoProbe:
        pass

    class _Broken:
        def _observation_images_support_metadata_purpose(self):
            raise RuntimeError("probe failed")

    assert cloud_sync._owner_sync_parents_supported(_NoProbe()) is False
    assert cloud_sync._owner_sync_parents_supported(_Broken()) is False


# ── The 917 shape through the real push chain ────────────────────────────────


def test_excluded_cheilocystidia_image_gets_an_owner_sync_parent_and_all_three_upload(db, monkeypatch):
    db_path, tmp_path = db
    _seed_917_image(db_path, tmp_path)
    harness = _harness(monkeypatch, db_path, tmp_path)

    result = harness.run()

    assert result["errors"] == []
    assert harness.client.uploaded_bytes == [], "image bytes stay out of the cloud"
    [parent] = harness.client.rows_for_desktop_id(12)
    assert parent.get("storage_path") in (None, "")
    assert parent["image_type"] == "microscope"
    assert parent["metadata_purpose"] == cloud_sync.METADATA_PURPOSE_OWNER_SYNC
    assert _image_cloud_id(db_path, 12) == str(parent["id"])
    assert sorted(mid for mid, _ in harness.client.pushed_measurements) == [7683, 7684, 7685]
    assert {img for _, img in harness.client.pushed_measurements} == {str(parent["id"])}
    # Mosaic eligibility is unaffected: no cheilocystidia reached the mosaic
    # pipeline, so no public spore point or tile can derive from them.
    assert all(
        int(row["id"]) not in {7683, 7684, 7685}
        for rows in harness.mosaic_source_rows for row in rows
    )
    assert cloud_sync._current_local_mosaic_signature(1) == ""
    assert _observation_status(db_path) == "synced"


def test_private_observation_still_syncs_owner_measurements(db, monkeypatch):
    db_path, tmp_path = db
    _seed_917_image(db_path, tmp_path)
    _set_spore_visibility(db_path, "private")
    harness = _harness(monkeypatch, db_path, tmp_path)
    harness.run()
    [parent] = harness.client.rows_for_desktop_id(12)
    assert parent["metadata_purpose"] == cloud_sync.METADATA_PURPOSE_OWNER_SYNC
    assert len(harness.client.pushed_measurements) == 3


def test_without_the_server_capability_no_owner_sync_parent_is_created(db, monkeypatch):
    """An older server's public RPCs would expose such a row: fail closed."""
    db_path, tmp_path = db
    _seed_917_image(db_path, tmp_path)
    harness = _harness(monkeypatch, db_path, tmp_path, capability=False)
    harness.run()
    assert harness.client.rows_for_desktop_id(12) == []
    assert harness.client.pushed_measurements == []


def test_second_sync_is_idempotent(db, monkeypatch):
    db_path, tmp_path = db
    _seed_917_image(db_path, tmp_path)
    harness = _harness(monkeypatch, db_path, tmp_path)
    harness.run()
    created = list(harness.client.created_image_cloud_ids)
    remote_measurements = [dict(r) for r in harness.client.remote_measurements]

    _mark_dirty(db_path)
    harness.client.patches.clear()
    harness.run()

    assert harness.client.created_image_cloud_ids == created, "no second parent"
    assert len(harness.client.rows_for_desktop_id(12)) == 1
    assert [p for p in harness.client.patches if "metadata_purpose" in p[1]] == [], \
        "an unchanged purpose is never re-written"
    assert [
        {k: r[k] for k in ("id", "desktop_id", "image_id")} for r in harness.client.remote_measurements
    ] == [{k: r[k] for k in ("id", "desktop_id", "image_id")} for r in remote_measurements]
    assert harness.client.uploaded_bytes == []


def test_purpose_follows_the_public_spore_intent_both_ways(db, monkeypatch):
    db_path, tmp_path = db
    _seed_917_image(db_path, tmp_path)
    harness = _harness(monkeypatch, db_path, tmp_path)
    harness.run()
    [parent] = harness.client.rows_for_desktop_id(12)
    assert parent["metadata_purpose"] == "owner_sync"

    _add_measurements(db_path, image_id=12, rows=[(7700, 10.0, 6.0, 100.0, 140.0)], measurement_type="spores")
    _mark_dirty(db_path)
    harness.run()
    assert harness.client.rows_for_desktop_id(12)[0]["metadata_purpose"] == "public_microscopy"
    assert [int(r["id"]) for rows in harness.mosaic_source_rows for r in rows] == [7700], \
        "only the spore measurement is mosaic material"

    _delete_local_measurements(db_path, [7700])
    harness.client.remote_measurements = [
        r for r in harness.client.remote_measurements if int(r["desktop_id"]) != 7700
    ]
    _mark_dirty(db_path)
    harness.run()
    assert harness.client.rows_for_desktop_id(12)[0]["metadata_purpose"] == "owner_sync"


# ── Retirement ───────────────────────────────────────────────────────────────


def _delete_local_measurements(db_path, ids):
    conn = sqlite3.connect(db_path)
    conn.executemany("DELETE FROM spore_measurements WHERE id = ?", [(i,) for i in ids])
    conn.execute("UPDATE observations SET sync_status = 'dirty' WHERE id = 1")
    conn.commit()
    conn.close()


def _delete_local_cheilocystidia(db_path):
    _delete_local_measurements(db_path, [mid for mid, *_ in CHEILOCYSTIDIA])


def test_parent_is_retired_once_no_device_has_measurements(db, monkeypatch):
    db_path, tmp_path = db
    _seed_917_image(db_path, tmp_path)
    harness = _harness(monkeypatch, db_path, tmp_path, real_tombstones=True)
    harness.run()
    [parent] = harness.client.rows_for_desktop_id(12)

    _delete_local_cheilocystidia(db_path)
    harness.client.remote_measurements = []  # the cloud has none on it either
    harness.run()  # the parent pass queues the cloud-copy tombstone
    assert harness.client.soft_deleted == []
    assert [t["deleted_cloud_id"] for t in models.list_pending_image_tombstones()] == [str(parent["id"])]

    _mark_dirty(db_path)
    harness.run()  # the next sync's canonical tombstone push soft-deletes it

    assert harness.client.soft_deleted == [str(parent["id"])]
    assert models.list_pending_image_tombstones() == []
    assert harness.client.rows_for_desktop_id(12)[0]["deleted_at"]
    assert harness.client.uploaded_bytes == []


def test_parent_is_not_retired_while_the_cloud_still_carries_measurements(db, monkeypatch):
    """A device that has not downloaded the measurements has zero local rows;
    it must never retire the parent that carries them."""
    db_path, tmp_path = db
    _seed_917_image(db_path, tmp_path)
    harness = _harness(monkeypatch, db_path, tmp_path, real_tombstones=True)
    harness.run()

    _delete_local_cheilocystidia(db_path)
    _mark_dirty(db_path)
    harness.run()

    assert harness.client.soft_deleted == []
    assert not harness.client.rows_for_desktop_id(12)[0].get("deleted_at")


def test_public_microscopy_parent_is_never_retired_by_this_path(db, monkeypatch):
    db_path, tmp_path = db
    _seed_917_image(db_path, tmp_path)
    harness = _harness(monkeypatch, db_path, tmp_path, real_tombstones=True)
    harness.run()
    parent = harness.client.rows_for_desktop_id(12)[0]
    parent["metadata_purpose"] = "public_microscopy"  # e.g. a legacy spore anchor

    _delete_local_cheilocystidia(db_path)
    harness.client.remote_measurements = []
    harness.run()
    assert harness.client.soft_deleted == []


def test_explicit_tombstone_is_cancelled_while_owner_measurements_need_the_parent(db, monkeypatch):
    db_path, tmp_path = db
    _seed_917_image(db_path, tmp_path)
    harness = _harness(monkeypatch, db_path, tmp_path, real_tombstones=True)
    harness.run()
    cloud_image_id = _image_cloud_id(db_path, 12)
    monkeypatch.setattr(cloud_sync, "list_pending_image_tombstones", lambda: [{
        "deleted_cloud_id": cloud_image_id, "local_image_id": 12, "local_observation_id": 1,
    }])
    cancelled = []
    monkeypatch.setattr(
        cloud_sync.ImageDB, "clear_image_tombstone_by_deleted_cloud_id",
        lambda cid: cancelled.append(cid) or True,
    )
    monkeypatch.setattr(cloud_sync, "reconcile_legacy_publish_exclusion_tombstones", lambda: {})

    cloud_sync._push_pending_image_tombstones(harness.client)

    assert cancelled == [cloud_image_id]
    assert harness.client.soft_deleted == []


# ── Fresh profile ────────────────────────────────────────────────────────────


def test_fresh_profile_download_reconstructs_all_three(db, monkeypatch, tmp_path_factory):
    db_path, tmp_path = db
    _seed_917_image(db_path, tmp_path)
    harness = _harness(monkeypatch, db_path, tmp_path)
    harness.run()
    remote_images = [dict(r) for r in harness.client.remote_images]
    remote_measurements = [dict(r) for r in harness.client.remote_measurements]
    parent_cloud_id = str(harness.client.rows_for_desktop_id(12)[0]["id"])

    fresh_dir = tmp_path_factory.mktemp("fresh-profile")
    fresh_db = fresh_dir / "sporely.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: fresh_db)
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: fresh_dir / "ref.db")
    monkeypatch.setattr(schema, "get_bundled_reference_database_path", lambda: fresh_dir / "bundled.db")
    monkeypatch.setattr(schema, "_migrate_reference_values", lambda *a, **k: None)
    monkeypatch.setattr(schema, "_migrate_reference_mounts_and_stains", lambda *a, **k: None)
    schema.init_database()
    monkeypatch.setattr(cloud_sync, "get_connection", schema.get_connection)
    monkeypatch.setattr(models, "get_connection", schema.get_connection)
    monkeypatch.setattr(cloud_sync, "get_images_dir", lambda: fresh_dir)

    local_id = cloud_sync._create_local_from_remote(
        {**harness.remote_obs, "genus": "Conocybe", "species": "rugosa"},
        client=harness.client,
        remote_images=remote_images,
        remote_measurements=remote_measurements,
        materialize_remote_images=False,
    )

    conn = sqlite3.connect(fresh_db)
    try:
        rows = conn.execute(
            """
            SELECT m.measurement_type, m.length_um, m.width_um, i.image_type, i.cloud_id
            FROM spore_measurements m JOIN images i ON i.id = m.image_id
            WHERE i.observation_id = ? AND i.cloud_id = ?
            ORDER BY m.length_um
            """,
            (local_id, parent_cloud_id),
        ).fetchall()
    finally:
        conn.close()
    assert [(r[0], round(r[1], 6), round(r[2], 6), r[3]) for r in rows] == [
        ("cheilocystidia", round(length, 6), round(width, 6), "microscope")
        for _mid, length, width, *_ in CHEILOCYSTIDIA
    ]


# ── Request discipline ───────────────────────────────────────────────────────


class _ProbeClient(cloud_sync.SporelyCloudClient):
    def __init__(self, *, supported: bool):
        self.user_id = "user-123"
        self.paths: list[str] = []
        self._supported = supported

    def _get(self, path):
        self.paths.append(path)
        if "metadata_purpose" in path and not self._supported:
            raise cloud_sync.CloudSyncError(
                "GET failed (400): column observation_images.metadata_purpose does not exist"
            )
        return []


def test_capability_probe_is_user_scoped_and_cached():
    client = _ProbeClient(supported=True)
    assert client._observation_images_support_metadata_purpose() is True
    assert client._observation_images_support_metadata_purpose() is True
    assert client.paths == ["observation_images?user_id=eq.user-123&select=metadata_purpose&limit=1"]


def test_server_without_the_column_is_remembered_as_unsupported():
    client = _ProbeClient(supported=False)
    assert cloud_sync._owner_sync_parents_supported(client) is False
    assert cloud_sync._owner_sync_parents_supported(client) is False
    assert len(client.paths) == 1


def test_unrelated_probe_errors_fail_closed_without_caching():
    class _Down(_ProbeClient):
        def _get(self, path):
            raise cloud_sync.CloudSyncError("GET failed (500): boom")

    client = _Down(supported=True)
    assert cloud_sync._owner_sync_parents_supported(client) is False
    assert getattr(client, "_metadata_purpose_supported", None) is None


def test_observation_without_owner_sync_candidates_issues_no_probe(db, monkeypatch):
    """A byte-kept image with spores only: nothing needs an owner-sync parent."""
    db_path, tmp_path = db
    source = _write_microscope_source(tmp_path / "spores.png")
    _add_microscope_image(db_path, image_id=12, filepath=source)
    _add_measurements(db_path, image_id=12, rows=[(801, 10.0, 6.0, 100.0, 140.0)], measurement_type="spores")
    cloud_sync._set_cloud_image_storage_intent_initialized_ids(1, {11, 12})
    _set_spore_visibility(db_path, "private")

    harness = _harness(monkeypatch, db_path, tmp_path)
    probes = []
    monkeypatch.setattr(
        type(harness.client), "_observation_images_support_metadata_purpose",
        lambda self: probes.append(1) or True,
    )
    harness.run()
    assert probes == []


def test_public_spore_only_parent_is_marked_without_any_prior_probe(db, monkeypatch):
    """Independent review B3: a user whose excluded microscope images carry
    only public spores never triggered the capability probe, so their new
    parents stayed NULL — and NULL fails closed on the server, sporePoints
    included."""
    db_path, tmp_path = db
    source = _write_microscope_source(tmp_path / "spores.png")
    _add_microscope_image(db_path, image_id=12, filepath=source)
    _add_measurements(db_path, image_id=12, rows=[(801, 10.0, 6.0, 100.0, 140.0)], measurement_type="spores")
    cloud_sync._set_cloud_image_storage_intent_initialized_ids(1, {11, 12})
    cloud_sync._set_cloud_image_storage_excluded_image_ids(1, {12})

    harness = _harness(monkeypatch, db_path, tmp_path)
    assert getattr(harness.client, "_metadata_purpose_supported", None) is None
    harness.run()

    [parent] = harness.client.rows_for_desktop_id(12)
    assert parent.get("storage_path") in (None, "")
    assert parent["metadata_purpose"] == cloud_sync.METADATA_PURPOSE_PUBLIC_MICROSCOPY
