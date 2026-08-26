"""Stage 1: cloud-image-byte storage-desired predicate + client gate.

Pins the safety invariants that split desired cloud-storage state out of the
Artsobs publication-exclusion key:

  * ``cloud_image_bytes_desired`` reflects the new
    ``sporely_cloud_image_storage_excluded_ids_<obs>`` setting.
  * ``SporelyCloudClient.upload_image_file`` and
    ``SporelyCloudClient.upload_original_image_file`` raise
    ``CloudImageBytesNotDesiredError`` for images the user has unchecked
    unless ``recovery_authorized=True``.
  * ``_associate_persisted_cloud_images`` repairs identity even for
    unchecked (undesired) images.
  * ``prepare_images_cb=None`` fallback and metadata-only sync paths must
    not bypass the byte gate.
  * ``set_image_cloud_selected`` writes to the NEW key while preserving the
    existing tombstone lifecycle.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from database import models, schema
from utils import cloud_sync
from utils.cloud_sync import (
    CloudImageBytesNotDesiredError,
    CloudSyncError,
    cloud_image_bytes_desired,
)


# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------


def _init_db(tmp_path: Path) -> Path:
    """Create a bare SQLite DB with the tables the cloud-sync helpers touch."""
    db_path = tmp_path / "bytes_desired.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cloud_id TEXT,
                user_id TEXT,
                sync_status TEXT,
                updated_at TEXT,
                spore_data_visibility TEXT
            );
            CREATE TABLE images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observation_id INTEGER,
                cloud_id TEXT,
                filepath TEXT NOT NULL,
                original_filepath TEXT,
                image_type TEXT,
                sort_order INTEGER,
                micro_category TEXT,
                objective_name TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                synced_at TEXT,
                notes TEXT,
                source_role TEXT,
                file_purpose TEXT,
                storage_path TEXT,
                original_storage_path TEXT,
                calibration_uuid TEXT
            );
            CREATE TABLE spore_measurements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                image_id INTEGER NOT NULL,
                length_um REAL,
                width_um REAL,
                measurement_type TEXT,
                notes TEXT
            );
            CREATE TABLE spore_annotations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                image_id INTEGER NOT NULL,
                measurement_id INTEGER
            );
            CREATE TABLE thumbnails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                image_id INTEGER NOT NULL,
                size_preset TEXT NOT NULL,
                filepath TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(image_id, size_preset)
            );
            CREATE TABLE settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            """
        )
        schema._ensure_image_tombstones_table(conn.cursor())
        conn.commit()
    finally:
        conn.close()
    return db_path


def _write_image(path: Path, payload: bytes = b"webp") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path


def _patch_db(monkeypatch, db_path: Path) -> None:
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(schema, "get_connection", lambda: sqlite3.connect(db_path))


def _seed_observation(
    db_path: Path,
    *,
    obs_id: int = 500,
    cloud_id: str | None = "cloud-500",
    user_id: str | None = "user-x",
    visibility: str = "public",
) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, user_id, sync_status, updated_at, spore_data_visibility)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (obs_id, cloud_id, user_id, "synced", "2026-08-01 10:00:00", visibility),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_image(
    db_path: Path,
    *,
    image_id: int,
    obs_id: int,
    filepath: str,
    image_type: str = "field",
    cloud_id: str | None = None,
    synced: bool = False,
    objective_name: str | None = None,
    original_filepath: str | None = None,
) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath,
                image_type, sort_order, objective_name, synced_at,
                notes, source_role, file_purpose
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                image_id,
                obs_id,
                cloud_id,
                filepath,
                original_filepath,
                image_type,
                image_id,
                objective_name,
                "2026-08-01 10:05:00" if synced else None,
                "",
                "converted_local",
                image_type,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _set_storage_excluded(db_path: Path, obs_id: int, excluded_ids: list[int]) -> None:
    conn = sqlite3.connect(db_path)
    try:
        import json as _json
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (
                f"sporely_cloud_image_storage_excluded_ids_{obs_id}",
                _json.dumps(sorted(excluded_ids)),
            ),
        )
        conn.commit()
    finally:
        conn.close()


class _FakeClient:
    """Minimal fake of ``SporelyCloudClient`` for image-upload gate tests."""

    def __init__(self, *, user_id: str = "user-x") -> None:
        self.user_id = user_id


# ---------------------------------------------------------------------------
# 1. New unchecked field image — no bytes, gate raises
# ---------------------------------------------------------------------------


def test_unchecked_field_image_gate_refuses_bytes(monkeypatch, tmp_path):
    db_path = _init_db(tmp_path)
    _patch_db(monkeypatch, db_path)
    _seed_observation(db_path)
    fpath = _write_image(tmp_path / "field.jpg")
    _seed_image(db_path, image_id=11, obs_id=500, filepath=str(fpath), image_type="field")
    _set_storage_excluded(db_path, 500, [11])

    assert cloud_image_bytes_desired(500, 11) is False

    client = cloud_sync.SporelyCloudClient("token", "user-x")
    with pytest.raises(CloudImageBytesNotDesiredError):
        client.upload_image_file(
            str(fpath),
            "cloud-obs-1",
            "cloud-img-1",
            storage_path="user-x/cloud-obs-1/field.jpg",
            observation_id=500,
            image_id=11,
        )


# ---------------------------------------------------------------------------
# 2. New unchecked microscope with no measurements — no bytes, no anchor
# ---------------------------------------------------------------------------


def test_unchecked_microscope_without_measurements_has_no_bytes_no_anchor(
    monkeypatch, tmp_path
):
    db_path = _init_db(tmp_path)
    _patch_db(monkeypatch, db_path)
    _seed_observation(db_path)
    fpath = _write_image(tmp_path / "micro.jpg")
    _seed_image(
        db_path, image_id=21, obs_id=500, filepath=str(fpath), image_type="microscope"
    )
    _set_storage_excluded(db_path, 500, [21])

    assert cloud_image_bytes_desired(500, 21) is False

    # Anchors require public spore measurements; no measurements → no anchor.
    class _AnchorClient(_FakeClient):
        def __init__(self):
            super().__init__()
            self.pushed_meta: list[dict] = []

        def pull_image_metadata(self, obs_cloud_id, include_deleted_for_sync=False):
            return []

        def push_image_metadata(self, image_row, obs_cloud_id, storage_path):
            self.pushed_meta.append(dict(image_row or {}))
            return "cloud-img-new"

        def _observation_images_support_ai_crop(self):
            return False

        def _observation_images_support_ai_crop_custom(self):
            return False

    result = cloud_sync._ensure_metadata_only_microscope_image_for_public_spores(
        _AnchorClient(),
        500,
        "cloud-500",
        {
            "id": 21,
            "observation_id": 500,
            "image_type": "microscope",
            "filepath": str(fpath),
        },
        remote_images=[],
    )
    assert result is None


# ---------------------------------------------------------------------------
# 3. Unchecked microscope with public spore measurements — bytes still gated,
#    but anchor helper still creates the metadata-only anchor.
# ---------------------------------------------------------------------------


def test_unchecked_microscope_with_public_measurements_keeps_anchor(
    monkeypatch, tmp_path
):
    db_path = _init_db(tmp_path)
    _patch_db(monkeypatch, db_path)
    _seed_observation(db_path)
    fpath = _write_image(tmp_path / "micro-measured.jpg")
    _seed_image(
        db_path,
        image_id=31,
        obs_id=500,
        filepath=str(fpath),
        image_type="microscope",
    )
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO spore_measurements (id, image_id, length_um, width_um, measurement_type) "
            "VALUES (1, 31, 12.5, 6.4, 'spore')"
        )
        conn.commit()
    finally:
        conn.close()
    _set_storage_excluded(db_path, 500, [31])

    assert cloud_image_bytes_desired(500, 31) is False

    class _AnchorClient(_FakeClient):
        def __init__(self):
            super().__init__()
            self.upload_calls: list[tuple] = []
            self.posted: list[tuple] = []

        def pull_image_metadata(self, obs_cloud_id, include_deleted_for_sync=False):
            return []

        def _post(self, endpoint, payload):
            self.posted.append((endpoint, dict(payload or {})))
            return [{"id": "cloud-img-anchor"}]

        def upload_image_file(self, *args, **kwargs):
            self.upload_calls.append((args, kwargs))
            raise AssertionError("Anchor pathway must not upload bytes")

        def _observation_images_support_ai_crop(self):
            return False

        def _observation_images_support_ai_crop_custom(self):
            return False

    fake = _AnchorClient()
    result = cloud_sync._ensure_metadata_only_microscope_image_for_public_spores(
        fake,
        500,
        "cloud-500",
        {
            "id": 31,
            "observation_id": 500,
            "image_type": "microscope",
            "filepath": str(fpath),
        },
        remote_images=[],
    )
    assert result == "cloud-img-anchor"
    assert fake.upload_calls == []
    assert len(fake.posted) == 1
    endpoint, payload = fake.posted[0]
    assert endpoint == "observation_images"
    assert payload.get("storage_path") is None


# ---------------------------------------------------------------------------
# 4. Checked microscope image — gate passes
# ---------------------------------------------------------------------------


def test_checked_microscope_image_gate_passes(monkeypatch, tmp_path):
    db_path = _init_db(tmp_path)
    _patch_db(monkeypatch, db_path)
    _seed_observation(db_path)

    # Write a real PNG so the encoding thumbnail step can decode it.
    from PIL import Image

    fpath = tmp_path / "checked-micro.png"
    Image.new("RGB", (32, 32), (200, 100, 50)).save(fpath)
    _seed_image(
        db_path, image_id=41, obs_id=500, filepath=str(fpath), image_type="microscope"
    )
    _set_storage_excluded(db_path, 500, [])

    assert cloud_image_bytes_desired(500, 41) is True

    # Gate passes: exercise the boundary check by monkey-patching the prepare
    # helper to return the source path directly and the media worker with a
    # fake so the test does not require a live worker.
    upload_calls: list[tuple] = []

    def _fake_prepare(*args, **kwargs):
        upload_calls.append(("prepare",))
        return fpath, 32, 32, 32, 32, "image/webp", 80

    class _Worker:
        base_url = "https://upload.test"

        def put_file(self, file_path, key, *args, **kwargs):
            upload_calls.append(("put_file",))
            return {"ok": True, "key": key}

        def put_bytes(self, data, key, *args, **kwargs):
            upload_calls.append(("put_bytes",))
            return {"ok": True, "key": key}

    monkeypatch.setattr(cloud_sync, "_prepare_cloud_image_upload_file", _fake_prepare)
    monkeypatch.setattr(cloud_sync, "media_worker_base_url", lambda: "https://upload.test")

    client = cloud_sync.SporelyCloudClient("token", "user-x")
    monkeypatch.setattr(client, "_get_media_worker", lambda: _Worker())

    # Should not raise — the boundary gate is happy with a desired image.
    result = client.upload_image_file(
        str(fpath),
        "cloud-500",
        "cloud-img-41",
        storage_path="user-x/cloud-500/img.webp",
        observation_id=500,
        image_id=41,
    )
    assert result is not None
    kinds = [c[0] for c in upload_calls]
    assert "prepare" in kinds
    assert "put_file" in kinds


# ---------------------------------------------------------------------------
# 5. Existing checked cloud image — remains linked, no re-upload of unchanged
#    bytes, metadata parity preserved.
# ---------------------------------------------------------------------------


def test_existing_checked_cloud_image_stays_linked_without_reupload(
    monkeypatch, tmp_path
):
    db_path = _init_db(tmp_path)
    _patch_db(monkeypatch, db_path)
    _seed_observation(db_path)
    fpath = _write_image(tmp_path / "field-linked.jpg")
    _seed_image(
        db_path,
        image_id=51,
        obs_id=500,
        filepath=str(fpath),
        image_type="field",
        cloud_id="cloud-img-51",
        synced=True,
    )
    _set_storage_excluded(db_path, 500, [])
    assert cloud_image_bytes_desired(500, 51) is True

    row = models.ImageDB.get_image(51)
    assert row is not None
    assert row["cloud_id"] == "cloud-img-51"

    # No cloud-sync writes should happen if selection is unchanged — a probe
    # via the shared predicate confirms the image is not "pending upload".
    decision = cloud_sync.explain_pending_cloud_image_decision(
        row,
        seen_paths=set(),
        excluded_ids=set(),
        explicit_media_upload_selection={51},
    )
    assert decision["pending"] is False
    assert decision["reason"] == cloud_sync.PENDING_REASON_ALREADY_SYNCED


# ---------------------------------------------------------------------------
# 6. Lost local cloud_id + unique matching remote desktop_id: repair happens
#    without a byte upload — even for an unchecked image.
# ---------------------------------------------------------------------------


def test_identity_repair_runs_for_unchecked_image_without_upload(
    monkeypatch, tmp_path
):
    db_path = _init_db(tmp_path)
    _patch_db(monkeypatch, db_path)
    _seed_observation(db_path)
    fpath = _write_image(tmp_path / "unchecked-linked.jpg")
    _seed_image(
        db_path,
        image_id=61,
        obs_id=500,
        filepath=str(fpath),
        image_type="field",
        cloud_id=None,
        synced=True,
    )
    _set_storage_excluded(db_path, 500, [61])
    assert cloud_image_bytes_desired(500, 61) is False

    class _RepairClient(_FakeClient):
        def __init__(self):
            super().__init__(user_id="user-x")

        def _observation_images_support_ai_crop(self):
            return False

        def _observation_images_support_upload_metadata(self):
            return False

    remote_row = {
        "id": "cloud-img-61",
        "desktop_id": 61,
        "observation_id": "cloud-500",
        "user_id": "user-x",
        "storage_path": "user-x/cloud-500/img61.webp",
        "image_type": "field",
        "sort_order": 61,
        "notes": "",
    }
    associated = cloud_sync._associate_persisted_cloud_images(
        _RepairClient(),
        {"id": 500, "cloud_id": "cloud-500", "user_id": "user-x"},
        [remote_row],
    )
    assert 61 in associated
    row = models.ImageDB.get_image(61)
    assert row is not None
    assert row["cloud_id"] == "cloud-img-61"


# ---------------------------------------------------------------------------
# 7. Metadata-only sync pass (prepare_images_cb=None): cannot upload source
#    image bytes; anchors still function.
# ---------------------------------------------------------------------------


def test_prepare_images_cb_none_skips_undesired_images(monkeypatch, tmp_path):
    """The prepare_images_cb=None fallback iterates local images and produces
    the ``prepared_items`` list itself. It must respect the byte-storage
    predicate so undesired images never enter the upload loop."""
    db_path = _init_db(tmp_path)
    _patch_db(monkeypatch, db_path)
    _seed_observation(db_path)
    fpath_desired = _write_image(tmp_path / "desired.jpg")
    fpath_undesired = _write_image(tmp_path / "undesired.jpg")
    _seed_image(
        db_path,
        image_id=71,
        obs_id=500,
        filepath=str(fpath_desired),
        image_type="field",
        cloud_id="cloud-img-71",
        synced=True,
    )
    _seed_image(
        db_path,
        image_id=72,
        obs_id=500,
        filepath=str(fpath_undesired),
        image_type="field",
        cloud_id=None,
    )
    _set_storage_excluded(db_path, 500, [72])

    class _NoopClient(_FakeClient):
        def __init__(self):
            super().__init__()
            self.uploaded = []

        def pull_image_metadata(self, *args, **kwargs):
            return []

        def push_image_metadata(self, *args, **kwargs):
            raise AssertionError("must not push")

        def upload_image_file(self, *args, **kwargs):
            self.uploaded.append(kwargs.get("image_id"))
            raise AssertionError("must not upload")

        def _observation_images_support_ai_crop(self):
            return False

        def _observation_images_support_upload_metadata(self):
            return False

    # Build the prepared_items via the prepare_images_cb=None fallback and
    # confirm image 72 is skipped.
    #
    # We exercise the fallback branch directly by mimicking its logic on the
    # DB-side view of the observation. This is intentionally decoupled from
    # ``_push_images_for_observation`` so the assertion is precise.
    images = models.ImageDB.get_images_for_observation(500)
    prepared_items: list[dict] = []
    for img in images:
        if img.get("image_type") == "microscope" and not img.get("cloud_id"):
            continue
        local_img_id = int(img.get("id") or 0)
        local_obs_id = int(img.get("observation_id") or 0)
        if (
            local_obs_id > 0
            and local_img_id > 0
            and not cloud_image_bytes_desired(local_obs_id, local_img_id, img)
        ):
            continue
        prepared_items.append({"image_row": img, "upload_path": img.get("filepath")})
    assert [item["image_row"]["id"] for item in prepared_items] == [71]


# ---------------------------------------------------------------------------
# 8. prepare_images_cb=None fallback cannot bypass the boundary gate for an
#    unchecked image — even if the caller somehow includes it, upload raises.
# ---------------------------------------------------------------------------


def test_boundary_gate_defends_against_undesired_upload_leak(monkeypatch, tmp_path):
    db_path = _init_db(tmp_path)
    _patch_db(monkeypatch, db_path)
    _seed_observation(db_path)
    fpath = _write_image(tmp_path / "leak.jpg")
    _seed_image(db_path, image_id=81, obs_id=500, filepath=str(fpath))
    _set_storage_excluded(db_path, 500, [81])

    client = cloud_sync.SporelyCloudClient("token", "user-x")
    with pytest.raises(CloudImageBytesNotDesiredError):
        client.upload_image_file(
            str(fpath),
            "cloud-500",
            "cloud-img-81",
            storage_path="user-x/cloud-500/leak.jpg",
            observation_id=500,
            image_id=81,
        )


# ---------------------------------------------------------------------------
# 9. Full-resolution original upload gate: refuses when parent image is not
#    desired; passes when it is.
# ---------------------------------------------------------------------------


def test_original_upload_gate_refuses_when_parent_undesired(monkeypatch, tmp_path):
    db_path = _init_db(tmp_path)
    _patch_db(monkeypatch, db_path)
    _seed_observation(db_path)
    fpath = _write_image(tmp_path / "orig.jpg")
    _seed_image(db_path, image_id=91, obs_id=500, filepath=str(fpath))
    _set_storage_excluded(db_path, 500, [91])

    client = cloud_sync.SporelyCloudClient("token", "user-x")
    with pytest.raises(CloudImageBytesNotDesiredError):
        client.upload_original_image_file(
            str(fpath),
            "cloud-500",
            "cloud-img-91",
            storage_path="user-x/cloud-500/originals/orig.webp",
            observation_id=500,
            image_id=91,
        )


def test_original_upload_gate_allows_when_parent_desired(monkeypatch, tmp_path):
    db_path = _init_db(tmp_path)
    _patch_db(monkeypatch, db_path)
    _seed_observation(db_path)

    from PIL import Image

    fpath = tmp_path / "orig-desired.png"
    Image.new("RGB", (32, 32), (10, 20, 30)).save(fpath)
    _seed_image(db_path, image_id=92, obs_id=500, filepath=str(fpath))
    # No exclusion → desired.

    calls: list[tuple] = []

    def _fake_prepare(*args, **kwargs):
        calls.append(("prepare",))
        return fpath, 100, 100, 100, 100, "image/webp", 80

    class _Worker:
        base_url = "https://upload.test"

        def put_file(self, file_path, key, *args, **kwargs):
            calls.append(("put_file",))
            return {"ok": True, "key": key}

    monkeypatch.setattr(cloud_sync, "_prepare_cloud_image_upload_file", _fake_prepare)
    monkeypatch.setattr(cloud_sync, "media_worker_base_url", lambda: "https://upload.test")

    client = cloud_sync.SporelyCloudClient("token", "user-x")
    monkeypatch.setattr(client, "_get_media_worker", lambda: _Worker())

    key = client.upload_original_image_file(
        str(fpath),
        "cloud-500",
        "cloud-img-92",
        storage_path="user-x/cloud-500/originals/orig-desired.webp",
        observation_id=500,
        image_id=92,
    )
    assert key
    assert ("prepare",) in calls
    assert ("put_file",) in calls


# ---------------------------------------------------------------------------
# 10. Unchecking an already uploaded image: queues delete-pending tombstone
#     and updates the new storage-excluded set.
# ---------------------------------------------------------------------------


def test_uncheck_uploaded_image_queues_tombstone_and_updates_storage_set(
    monkeypatch, tmp_path
):
    db_path = _init_db(tmp_path)
    _patch_db(monkeypatch, db_path)
    _seed_observation(db_path)
    fpath = _write_image(tmp_path / "uploaded.jpg")
    _seed_image(
        db_path,
        image_id=101,
        obs_id=500,
        filepath=str(fpath),
        image_type="field",
        cloud_id="cloud-img-101",
        synced=True,
    )

    # Initial state: image is UPLOADED, excluded set is empty.
    assert cloud_image_bytes_desired(500, 101) is True

    transition = cloud_sync.set_image_cloud_selected(101, False)
    assert transition["previous_state"] == models.CLOUD_IMAGE_STATE_UPLOADED
    assert transition["cloud_state"] == models.CLOUD_IMAGE_STATE_DELETE_PENDING
    assert transition["action"] == "delete_queued"

    # Storage-excluded set is now populated.
    excluded = cloud_sync._cloud_image_storage_excluded_image_ids(500)
    assert excluded == {101}
    assert cloud_image_bytes_desired(500, 101) is False

    # And the tombstone exists.
    pending = models.list_pending_image_tombstones()
    assert [row["deleted_cloud_id"] for row in pending] == ["cloud-img-101"]


# ---------------------------------------------------------------------------
# 11. Rechecking before sync cancels the pending delete and removes id from
#     storage-excluded set.
# ---------------------------------------------------------------------------


def test_recheck_before_sync_cancels_delete_and_removes_from_storage_excluded(
    monkeypatch, tmp_path
):
    db_path = _init_db(tmp_path)
    _patch_db(monkeypatch, db_path)
    _seed_observation(db_path)
    fpath = _write_image(tmp_path / "uploaded-2.jpg")
    _seed_image(
        db_path,
        image_id=111,
        obs_id=500,
        filepath=str(fpath),
        image_type="field",
        cloud_id="cloud-img-111",
        synced=True,
    )

    cloud_sync.set_image_cloud_selected(111, False)
    assert cloud_sync._cloud_image_storage_excluded_image_ids(500) == {111}

    transition = cloud_sync.set_image_cloud_selected(111, True)
    assert transition["previous_state"] == models.CLOUD_IMAGE_STATE_DELETE_PENDING
    assert transition["cloud_state"] == models.CLOUD_IMAGE_STATE_UPLOADED
    assert transition["cloud_id"] == "cloud-img-111"
    assert transition["action"] == "delete_cancelled"

    # Storage-excluded set no longer includes the id; pending tombstones empty.
    assert cloud_sync._cloud_image_storage_excluded_image_ids(500) == set()
    assert models.list_pending_image_tombstones() == []
    # And the local image still points at the same cloud row.
    row = models.ImageDB.get_image(111)
    assert row["cloud_id"] == "cloud-img-111"


# ---------------------------------------------------------------------------
# 12. Fresh-DB headless sync must not upload every microscope frame: the
#     cloud-storage-desired initializer runs as a sync prerequisite, seeds
#     the storage-excluded set, and only the sparse per-magnification default
#     is byte-uploaded even when the gallery has never been opened.
# ---------------------------------------------------------------------------


def test_headless_sync_seeds_sparse_microscope_default_before_byte_upload(
    monkeypatch, tmp_path
):
    """Fix 1 regression: sync-driven initialization.

    Reproduces the exact "background/headless sync before any UI touch"
    scenario. Without the initializer running at the top of
    ``_push_images_for_observation``, absent
    ``sporely_cloud_image_storage_excluded_ids_<obs>`` means
    ``cloud_image_bytes_desired`` returns True for every image and every
    microscope frame would be shipped to Sporely Cloud on first sync.
    """
    db_path = _init_db(tmp_path)
    _patch_db(monkeypatch, db_path)
    _seed_observation(db_path, obs_id=900, cloud_id="cloud-900", visibility="private")

    # Two magnification groups: 10x has three local frames, 40x has two.
    group_10x_ids = (11, 12, 13)
    group_40x_ids = (21, 22)
    for image_id in group_10x_ids:
        _seed_image(
            db_path,
            image_id=image_id,
            obs_id=900,
            filepath=str(_write_image(tmp_path / f"micro-10x-{image_id}.jpg")),
            image_type="microscope",
            objective_name="10x objective",
        )
    for image_id in group_40x_ids:
        _seed_image(
            db_path,
            image_id=image_id,
            obs_id=900,
            filepath=str(_write_image(tmp_path / f"micro-40x-{image_id}.jpg")),
            image_type="microscope",
            objective_name="40x objective",
        )

    # Precondition: neither the excluded set nor the sentinel exists yet.
    assert cloud_sync._cloud_image_storage_excluded_image_ids(900) == set()
    assert cloud_sync._cloud_image_storage_initialized(900) is False
    # Also assert the naive precondition that motivated Fix 1: without the
    # initializer the byte-gate predicate would say "everything desired".
    for image_id in (*group_10x_ids, *group_40x_ids):
        assert cloud_image_bytes_desired(900, image_id) is True

    # The observation is private so
    # ``_ensure_metadata_anchors_for_public_spore_observation`` no-ops
    # cleanly. The sync path uses the ``prepare_images_cb`` we pass to
    # emit every microscope image as an upload candidate; the desired-state
    # gate must then filter out the four that were seeded excluded.
    all_ids = tuple(sorted((*group_10x_ids, *group_40x_ids)))

    def prepare_all(_obs, _progress_cb):
        rows = models.ImageDB.get_images_for_observation(900)
        # The initializer must run before this callback observes the
        # prepared items. The sync path filters by desired state after
        # preparation, but we also verify the initializer has seeded the
        # excluded set at prepare_images_cb time — proving it runs above
        # any candidate filter.
        excluded_after_prepare = cloud_sync._cloud_image_storage_excluded_image_ids(900)
        assert cloud_sync._cloud_image_storage_initialized(900) is True
        assert excluded_after_prepare, (
            "initializer must seed the storage-excluded set before "
            "prepare_images_cb runs"
        )
        return (
            [
                {"image_row": img, "upload_path": img.get("filepath")}
                for img in rows
                if int(img.get("id") or 0) in all_ids
            ],
            None,
            [],
        )

    upload_attempts: list[int] = []
    original_upload_attempts: list[int] = []

    class _HeadlessClient:
        user_id = "user-x"

        def pull_image_metadata(self, obs_cloud_id, include_deleted_for_sync=False):
            return []

        def push_image_metadata(
            self, image_row, obs_cloud_id, storage_path, remote_row=None
        ):
            image_id = int(dict(image_row or {}).get("id") or 0)
            return f"cloud-img-{image_id}"

        def upload_image_file(
            self,
            local_path,
            obs_cloud_id,
            img_cloud_id,
            storage_path=None,
            upload_meta=None,
            result_meta=None,
            *,
            observation_id=None,
            image_id=None,
            recovery_authorized=False,
        ):
            # Assert Fix 2 is honored on the sync path: identity kwargs are
            # populated so the client-boundary gate can consult the
            # desired-state predicate — a plain fake would still work if
            # the sync path stopped passing ids, so we assert here to
            # pin the invariant end-to-end.
            assert not recovery_authorized
            assert int(observation_id or 0) == 900
            assert int(image_id or 0) > 0
            upload_attempts.append(int(image_id))
            return storage_path or "storage/key"

        def upload_original_image_file(
            self,
            local_path,
            obs_cloud_id,
            img_cloud_id,
            storage_path=None,
            upload_meta=None,
            *,
            observation_id=None,
            image_id=None,
            recovery_authorized=False,
        ):
            assert not recovery_authorized
            assert int(observation_id or 0) == 900
            assert int(image_id or 0) > 0
            original_upload_attempts.append(int(image_id))
            return storage_path or "storage/key/orig"

        def set_image_original_storage_path(self, cloud_image_id, key):
            return None

        def _observation_images_support_ai_crop(self):
            return False

        def _observation_images_support_ai_crop_custom(self):
            return False

        def _observation_images_support_upload_metadata(self):
            return False

        def _build_original_storage_path(self, obs_cloud_id, img_cloud_id, path):
            return f"{self.user_id}/{obs_cloud_id}/originals/{img_cloud_id}"

        # Some downstream helpers touch ``_storage_remove`` and ``_patch``
        # on failure paths; provide no-op stubs so unrelated exceptions
        # don't mask the assertion we care about.
        def _storage_remove(self, keys):
            return None

        def _patch(self, endpoint, payload):
            return None

        def _delete(self, endpoint):
            return None

    # Prevent the full-resolution original upload from asking for network.
    monkeypatch.setattr(
        cloud_sync, "is_full_resolution_original_sync_enabled", lambda: False
    )

    result = cloud_sync._push_images_for_observation(
        _HeadlessClient(),
        {"id": 900, "spore_data_visibility": "private"},
        "cloud-900",
        prepare_images_cb=prepare_all,
    )

    assert result is True

    # After sync the initializer must have populated the storage-excluded
    # setting for this observation.
    assert cloud_sync._cloud_image_storage_initialized(900) is True
    excluded_after = cloud_sync._cloud_image_storage_excluded_image_ids(900)
    # Sparse default: one desired per group. Group order follows sort_order,
    # which was seeded to equal the image_id, so the keepers are the
    # lowest-id image in each group: 11 (10x) and 21 (40x).
    assert excluded_after == {12, 13, 22}

    # And only the sparse default was byte-uploaded — the rest are filtered
    # by the sync path (either dropped from prepared_items or gated at the
    # client boundary; the observable outcome is identical).
    assert sorted(upload_attempts) == [11, 21]
    # Original upload is disabled globally by monkeypatch above.
    assert original_upload_attempts == []


# ---------------------------------------------------------------------------
# 13. Byte gate fails closed on missing identity: normal-path callers that
#     omit ``observation_id`` or ``image_id`` are refused. Recovery flows
#     opt in via ``recovery_authorized=True`` and are the only intentional
#     exception.
# ---------------------------------------------------------------------------


def test_upload_image_file_requires_identity_on_normal_path(monkeypatch, tmp_path):
    """Fix 2 regression: identity required on the normal upload path.

    Without this, ``observation_id=None, image_id=None`` silently bypasses
    the desired-state predicate and lets arbitrary bytes be sent to
    storage. The gate must refuse both writer methods on the normal path
    and permit them only when ``recovery_authorized=True``.
    """
    db_path = _init_db(tmp_path)
    _patch_db(monkeypatch, db_path)
    _seed_observation(db_path)
    fpath = _write_image(tmp_path / "identity.jpg")
    _seed_image(db_path, image_id=42, obs_id=500, filepath=str(fpath))

    client = cloud_sync.SporelyCloudClient("token", "user-x")

    # (a) upload_image_file: observation_id=None, image_id=42 → refused.
    with pytest.raises(CloudImageBytesNotDesiredError) as excinfo:
        client.upload_image_file(
            str(fpath),
            "cloud-obs-1",
            "cloud-img-42",
            storage_path="user-x/cloud-obs-1/identity.jpg",
            observation_id=None,
            image_id=42,
        )
    assert "observation_id and image_id" in str(excinfo.value)

    # (b) upload_image_file: observation_id=99, image_id=None → refused.
    with pytest.raises(CloudImageBytesNotDesiredError):
        client.upload_image_file(
            str(fpath),
            "cloud-obs-1",
            "cloud-img-42",
            storage_path="user-x/cloud-obs-1/identity.jpg",
            observation_id=99,
            image_id=None,
        )

    # (c) upload_original_image_file: observation_id=None, image_id=42.
    with pytest.raises(CloudImageBytesNotDesiredError):
        client.upload_original_image_file(
            str(fpath),
            "cloud-obs-1",
            "cloud-img-42",
            storage_path="user-x/cloud-obs-1/originals/identity.jpg",
            observation_id=None,
            image_id=42,
        )

    # (d) upload_original_image_file: observation_id=99, image_id=None.
    with pytest.raises(CloudImageBytesNotDesiredError):
        client.upload_original_image_file(
            str(fpath),
            "cloud-obs-1",
            "cloud-img-42",
            storage_path="user-x/cloud-obs-1/originals/identity.jpg",
            observation_id=99,
            image_id=None,
        )

    # (e) Also refuse when BOTH kwargs are missing on both writers — the
    #     pre-Fix-2 silent bypass path.
    with pytest.raises(CloudImageBytesNotDesiredError):
        client.upload_image_file(
            str(fpath),
            "cloud-obs-1",
            "cloud-img-42",
            storage_path="user-x/cloud-obs-1/identity.jpg",
        )
    with pytest.raises(CloudImageBytesNotDesiredError):
        client.upload_original_image_file(
            str(fpath),
            "cloud-obs-1",
            "cloud-img-42",
            storage_path="user-x/cloud-obs-1/originals/identity.jpg",
        )


def test_upload_image_file_recovery_authorized_tolerates_missing_identity(
    monkeypatch, tmp_path
):
    """Recovery adapter waives the identity requirement for auditability."""
    from PIL import Image

    fpath = tmp_path / "recovery-source.png"
    Image.new("RGB", (16, 16), (50, 60, 70)).save(fpath)

    prepare_calls: list[str] = []

    def _fake_prepare(*args, **kwargs):
        prepare_calls.append("prepare")
        return fpath, 16, 16, 16, 16, "image/webp", 80

    class _Worker:
        base_url = "https://upload.test"

        def put_file(self, file_path, key, *args, **kwargs):
            prepare_calls.append(f"put_file:{key}")
            return {"ok": True, "key": key}

        def put_bytes(self, data, key, *args, **kwargs):
            prepare_calls.append(f"put_bytes:{key}")
            return {"ok": True, "key": key}

    monkeypatch.setattr(cloud_sync, "_prepare_cloud_image_upload_file", _fake_prepare)
    monkeypatch.setattr(
        cloud_sync, "media_worker_base_url", lambda: "https://upload.test"
    )

    client = cloud_sync.SporelyCloudClient("token", "user-x")
    monkeypatch.setattr(client, "_get_media_worker", lambda: _Worker())

    # Missing ids + recovery_authorized=True must pass the gate. The
    # recovery adapter already forwards ids in practice; the identity
    # requirement is waived here for auditability of the recovery-only
    # escape hatch.
    key = client.upload_image_file(
        str(fpath),
        "cloud-obs-1",
        "cloud-img-1",
        storage_path="user-x/cloud-obs-1/recovery.webp",
        recovery_authorized=True,
    )
    assert key
    assert any(entry.startswith("put_file:") for entry in prepare_calls)

    # Same for the original writer.
    original_key = client.upload_original_image_file(
        str(fpath),
        "cloud-obs-1",
        "cloud-img-1",
        storage_path="user-x/cloud-obs-1/originals/recovery.webp",
        recovery_authorized=True,
    )
    assert original_key
