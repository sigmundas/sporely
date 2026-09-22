"""Regression: an unchanged render signature must not imply uploaded media.

The local media signature describes whether local *render inputs* changed. It
carries no `cloud_id` and no cloud-storage intent, so it cannot prove that the
cloud identity or the bytes for the user's selected images exist. Before this
gate, an observation whose render inputs never changed but whose selected
images were never uploaded took the `image_render_unchanged` fast path on every
explicit `sync_images=True` run — the stranded-media state: dirty forever, bytes
never sent.

These tests lock in that image-prep fast paths (`image_render_unchanged`,
tombstone-only, metadata-only) are permitted only when no genuinely pending
cloud image remains, using the canonical per-image storage-intent ledger
(`_ensure_cloud_image_storage_intent_initialized`) and the canonical pending
predicate (`_pending_cloud_pushable_image_ids`) — not a second policy.

Refresh / background sync (`sync_images=False`) is explicitly unaffected.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from database import models
from utils import cloud_sync


def _init_db(tmp_path):
    db_path = tmp_path / "upload_completeness.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cloud_id TEXT,
            sync_status TEXT,
            synced_at TEXT,
            date TEXT,
            user_id TEXT,
            spore_data_visibility TEXT,
            sync_error_code TEXT,
            sync_error_message TEXT,
            sync_blocked_reason TEXT,
            sync_blocked_at TEXT,
            folder_path TEXT,
            artsdata_id INTEGER,
            publish_target TEXT
        );
        CREATE TABLE images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observation_id INTEGER,
            cloud_id TEXT,
            filepath TEXT,
            original_filepath TEXT,
            image_type TEXT,
            sort_order INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            micro_category TEXT,
            objective_name TEXT,
            scale_microns_per_pixel REAL,
            resample_scale_factor REAL,
            mount_medium TEXT,
            stain TEXT,
            sample_type TEXT,
            contrast TEXT,
            measure_color TEXT,
            crop_mode TEXT,
            notes TEXT,
            gps_source INTEGER,
            ai_crop_x1 REAL,
            ai_crop_y1 REAL,
            ai_crop_x2 REAL,
            ai_crop_y2 REAL,
            ai_crop_source_w INTEGER,
            ai_crop_source_h INTEGER,
            ai_crop_is_custom INTEGER,
            calibration_id INTEGER,
            synced_at TEXT,
            source_role TEXT,
            file_purpose TEXT
        );
        CREATE TABLE spore_measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            image_id INTEGER NOT NULL,
            length_um REAL,
            width_um REAL,
            measurement_type TEXT,
            notes TEXT,
            p1_x REAL, p1_y REAL, p2_x REAL, p2_y REAL,
            p3_x REAL, p3_y REAL, p4_x REAL, p4_y REAL,
            gallery_rotation INTEGER,
            measured_at TEXT,
            cloud_id TEXT
        );
        CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE image_tombstones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deleted_cloud_id TEXT NOT NULL,
            deleted_at TEXT NOT NULL DEFAULT '',
            delete_synced_at TEXT,
            deleted_storage_path TEXT,
            deleted_observation_cloud_id TEXT,
            local_observation_id INTEGER,
            local_image_id INTEGER,
            image_type TEXT,
            filepath TEXT,
            original_filepath TEXT
        );
        """
    )
    conn.commit()
    conn.close()
    return db_path


def _patch_connections(monkeypatch, db_path):
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))


def _seed_synced_observation(db_path, field_image_path: Path):
    """Observation 1: already in cloud, one synced field image."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at, date, "
            "user_id, spore_data_visibility) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                1, "cloud-obs-1", "dirty", "2026-05-01T00:00:00Z",
                "2026-05-01", "user-123", "public",
            ),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, image_type, sort_order,
                created_at, synced_at, crop_mode, source_role, file_purpose
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                11, 1, "cloud-image-11", str(field_image_path), "field", 0,
                "2026-05-01T00:00:00Z", "2026-05-01T00:00:00Z", "full",
                "local_canonical", "field",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _add_local_image(
    db_path,
    *,
    image_id: int,
    filepath: Path | str,
    image_type: str = "microscope",
    sort_order: int = 1,
    objective_name: str | None = "100X",
    source_role: str = "local_canonical",
    file_purpose: str | None = None,
    cloud_id: str | None = None,
):
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, image_type, sort_order,
                created_at, objective_name, crop_mode, source_role, file_purpose
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                image_id, 1, cloud_id, str(filepath), image_type, sort_order,
                "2026-05-02T00:00:00Z", objective_name, "full",
                source_role, file_purpose,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _cloud_id_for_image(db_path, image_id: int) -> str:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT cloud_id FROM images WHERE id = ?", (image_id,)
        ).fetchone()
    finally:
        conn.close()
    return str((row or [None])[0] or "")


def _mark_observation_dirty(db_path):
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("UPDATE observations SET sync_status = 'dirty' WHERE id = 1")
        conn.commit()
    finally:
        conn.close()


def _remote_image_row():
    return {
        "id": "cloud-image-11",
        "desktop_id": 11,
        "observation_id": "cloud-obs-1",
        "sort_order": 0,
        "image_type": "field",
        "crop_mode": "full",
        "notes": None,
        "storage_path": "user/cloud-obs-1/cloud-image-11.webp",
        "original_filename": "field.jpg",
        "ai_crop_x1": None,
        "ai_crop_y1": None,
        "ai_crop_x2": None,
        "ai_crop_y2": None,
        "ai_crop_source_w": None,
        "ai_crop_source_h": None,
        "ai_crop_is_custom": None,
        "calibration_uuid": None,
    }


def _remote_cache_image_row(image_id: str, desktop_id: int, sort_order: int):
    """Remote row for a locally cached, cloud-owned recovery copy."""
    row = _remote_image_row()
    row.update(
        {
            "id": image_id,
            "desktop_id": desktop_id,
            "sort_order": sort_order,
            "user_id": "user-123",
            "storage_path": f"user/cloud-obs-1/{image_id}.webp",
            "original_filename": f"{image_id}.jpg",
        }
    )
    return row


class _StubClient:
    """Minimal push_all client. Records nothing the gate depends on."""

    def __init__(self, remote_obs, remote_images):
        self.user_id = "user-123"
        self.remote_obs = dict(remote_obs)
        self.remote_images = [dict(row) for row in remote_images]
        self.uploaded_bytes: list[str] = []
        self.push_image_metadata_calls: list[dict] = []

    def push_observation(self, obs, remote_obs=None, **kwargs):
        return self.remote_obs["id"]

    def get_observation(self, cloud_id):
        return dict(self.remote_obs)

    def pull_image_metadata(self, obs_cloud_id, include_deleted_for_sync=False):
        return [dict(row) for row in self.remote_images]

    def pull_bulk_image_metadata(self, obs_cloud_ids):
        return [dict(row) for row in self.remote_images]

    def pull_measurements_for_images(self, image_cloud_ids):
        return []

    def push_image_metadata(self, img, obs_cloud_id, storage_path, *, remote_row=None):
        record = dict(img)
        self.push_image_metadata_calls.append(record)
        return str(record.get("cloud_id") or "cloud-image-new")

    def upload_image_file(self, local_path, *args, **kwargs):
        self.uploaded_bytes.append(str(local_path))
        return kwargs.get("storage_path") or "user/upload.webp"

    def set_desktop_id(self, *args, **kwargs):
        return None

    def _observation_images_support_ai_crop(self):
        return True

    def _observation_images_support_ai_crop_custom(self):
        return True

    def _observation_images_support_upload_metadata(self):
        return False

    def _observation_images_support_original_storage_path(self):
        return False

    def _find_cloud_image(self, desktop_id, obs_cloud_id=None, image_type=None):
        for row in self.remote_images:
            if int(row.get("desktop_id") or 0) == int(desktop_id):
                return {"id": str(row.get("id") or ""), "deleted_at": None}
        return None

    def _using_default_r2_loader(self):
        return False

    def _get(self, path):
        return []

    def _post(self, path, payload):
        return [{"id": 1}]

    def _patch(self, path, payload):
        return None

    def _delete(self, path):
        return None


class _PushHarness:
    """Wires push_all so only the image-prep decision is under test.

    ``real_image_push=True`` keeps the production
    ``_push_images_for_observation`` in the loop (identity repair, candidate
    filtering, the byte-upload boundary) and only *records* how it was called,
    so tests can assert on the real preparation/repair behavior instead of on
    a stub's arguments.
    """

    def __init__(self, monkeypatch, db_path, *, real_image_push=False, extra_remote_images=None):
        self.image_prep_calls: list[tuple[int, bool]] = []
        self.measurement_push_calls: list[int] = []
        # (observation id, image ids the callback was told to skip preparing)
        self.prepare_calls: list[tuple[int, list[int]]] = []
        remote_obs = {"id": "cloud-obs-1", "desktop_id": 1, "date": "2026-05-01"}
        remote_images = [_remote_image_row(), *(extra_remote_images or [])]
        self.client = _StubClient(remote_obs, remote_images)
        self.remote_obs = remote_obs

        # Stored signature == current signature: the render inputs are
        # unchanged, which is exactly the state the fast paths key on.
        baseline_signature = cloud_sync._local_cloud_media_signature(1)
        stored_snapshot = cloud_sync._cloud_observation_snapshot(
            remote_obs, remote_images, []
        )

        monkeypatch.setattr(
            cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None
        )
        monkeypatch.setattr(
            cloud_sync,
            "_mark_cloud_observations_dirty_for_pending_local_images",
            lambda **_kwargs: None,
        )
        monkeypatch.setattr(
            cloud_sync,
            "push_calibrations",
            lambda *a, **k: {"pushed": 0, "total": 0, "errors": []},
        )
        monkeypatch.setattr(
            cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: stored_snapshot
        )
        monkeypatch.setattr(
            cloud_sync,
            "_load_local_cloud_media_signature",
            lambda observation_id: baseline_signature,
        )
        monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *a, **k: None)
        monkeypatch.setattr(
            cloud_sync,
            "_refresh_local_cloud_media_signature",
            lambda observation_id: baseline_signature,
        )
        monkeypatch.setattr(cloud_sync, "_push_pending_image_tombstones", lambda client: [])

        real_push_images = cloud_sync._push_images_for_observation

        def recording_push_images(client_arg, obs, cloud_id, *, prepare_images_cb=None, **kwargs):
            self.image_prep_calls.append((int(obs["id"]), prepare_images_cb is not None))
            if not real_image_push:
                return True
            return real_push_images(
                client_arg, obs, cloud_id,
                prepare_images_cb=prepare_images_cb,
                **kwargs,
            )

        monkeypatch.setattr(
            cloud_sync, "_push_images_for_observation", recording_push_images
        )
        monkeypatch.setattr(
            cloud_sync,
            "_push_measurements_for_observation",
            lambda *args, **kwargs: self.measurement_push_calls.append(int(args[1])),
        )

    def run(self, *, sync_images: bool = True):
        def prepare_images_cb(obs, progress_cb):
            skip_ids = sorted(
                int(value)
                for value in (
                    obs.get(cloud_sync.CLOUD_SYNC_SKIP_PREPARE_IMAGE_IDS_KEY) or []
                )
            )
            self.prepare_calls.append((int(obs["id"]), skip_ids))
            return ([], None, [])

        return cloud_sync.push_all(
            self.client,
            remote_obs=[dict(self.remote_obs)],
            sync_images=sync_images,
            sync_calibrations=False,
            prepare_images_cb=prepare_images_cb,
        )

    @property
    def took_fast_path_skip(self) -> bool:
        """`image_render_unchanged` skips image prep entirely."""
        return self.image_prep_calls == []

    @property
    def ran_full_image_prep(self) -> bool:
        return self.image_prep_calls == [(1, True)]


def _observation_status(db_path) -> str:
    conn = sqlite3.connect(db_path)
    try:
        return str(
            conn.execute("SELECT sync_status FROM observations WHERE id = 1").fetchone()[0]
        )
    finally:
        conn.close()


@pytest.fixture
def db(tmp_path, monkeypatch):
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    field_image = tmp_path / "field.jpg"
    field_image.write_bytes(b"field-bytes")
    _seed_synced_observation(db_path, field_image)
    return db_path, tmp_path, field_image


# ---------------------------------------------------------------------------
# The stranded-media state
# ---------------------------------------------------------------------------


def test_pending_desired_image_forces_full_prep_despite_matching_signature(
    db, monkeypatch
):
    """Matching signature + desired, initialized image with cloud_id=NULL."""
    db_path, tmp_path, _field_image = db
    micro = tmp_path / "micro.jpg"
    micro.write_bytes(b"microscope-bytes")
    _add_local_image(db_path, image_id=12, filepath=micro)
    # Intent recorded, image desired (not in the excluded set).
    cloud_sync._set_cloud_image_storage_intent_initialized_ids(1, {11, 12})

    assert cloud_sync._pending_cloud_pushable_image_ids(1) == [12]

    harness = _PushHarness(monkeypatch, db_path)
    result = harness.run()

    assert result["errors"] == []
    assert harness.ran_full_image_prep, harness.image_prep_calls
    assert harness.measurement_push_calls == [1]


def test_no_pending_images_keeps_the_signature_skip_path(db, monkeypatch):
    """Everything already uploaded → the existing fast path is preserved."""
    db_path, _tmp_path, _field_image = db
    cloud_sync._set_cloud_image_storage_intent_initialized_ids(1, {11})

    assert cloud_sync._pending_cloud_pushable_image_ids(1) == []

    harness = _PushHarness(monkeypatch, db_path)
    result = harness.run()

    assert result["errors"] == []
    assert harness.took_fast_path_skip, harness.image_prep_calls
    assert harness.measurement_push_calls == [1]
    assert harness.client.uploaded_bytes == []


def test_storage_intent_is_initialized_before_pending_state_is_evaluated(
    db, monkeypatch
):
    """Ordering matters: an unseeded ledger makes every row look uninitialized.

    With no ledger entries, `_pending_cloud_pushable_image_ids` reports nothing
    pending (fail-closed on unknown intent) and the fast path would be taken.
    Seeding first is what makes image 12 visible as pending, so the full-prep
    outcome below is only reachable if initialization ran first.
    """
    db_path, tmp_path, _field_image = db
    micro = tmp_path / "micro.jpg"
    micro.write_bytes(b"microscope-bytes")
    _add_local_image(db_path, image_id=12, filepath=micro)

    # Nothing seeded: the pending predicate must refuse to guess.
    assert cloud_sync._cloud_image_storage_intent_initialized_ids(1) == set()
    assert cloud_sync._pending_cloud_pushable_image_ids(1) == []

    order: list[str] = []
    real_initialize = cloud_sync._ensure_cloud_image_storage_intent_initialized
    real_pending = cloud_sync._pending_cloud_pushable_image_ids

    def spy_initialize(observation_id):
        order.append(f"initialize:{cloud_sync._safe_int(observation_id)}")
        return real_initialize(observation_id)

    def spy_pending(observation_id, **kwargs):
        order.append(f"pending:{cloud_sync._safe_int(observation_id)}")
        return real_pending(observation_id, **kwargs)

    monkeypatch.setattr(
        cloud_sync, "_ensure_cloud_image_storage_intent_initialized", spy_initialize
    )
    monkeypatch.setattr(cloud_sync, "_pending_cloud_pushable_image_ids", spy_pending)

    harness = _PushHarness(monkeypatch, db_path)
    result = harness.run()

    assert result["errors"] == []
    assert order[0] == "initialize:1"
    assert order.index("initialize:1") < order.index("pending:1")
    # Seeding made the deterministic keeper desired, so it is genuinely pending.
    assert cloud_sync._pending_cloud_pushable_image_ids(1) == [12]
    assert harness.ran_full_image_prep, harness.image_prep_calls


# ---------------------------------------------------------------------------
# Rows that are intentionally not pushable must not force full prep
# ---------------------------------------------------------------------------


def test_excluded_microscope_image_does_not_force_full_prep(db, monkeypatch):
    db_path, tmp_path, _field_image = db
    micro = tmp_path / "micro.jpg"
    micro.write_bytes(b"microscope-bytes")
    _add_local_image(db_path, image_id=12, filepath=micro)
    cloud_sync._set_cloud_image_storage_intent_initialized_ids(1, {11, 12})
    # The user unchecked "Keep image in Sporely Cloud" for image 12.
    cloud_sync._set_cloud_image_storage_excluded_image_ids(1, {12})

    assert cloud_sync._pending_cloud_pushable_image_ids(1) == []

    harness = _PushHarness(monkeypatch, db_path)
    result = harness.run()

    assert result["errors"] == []
    assert harness.took_fast_path_skip, harness.image_prep_calls
    assert harness.client.uploaded_bytes == []


def test_missing_file_and_duplicate_rows_do_not_create_a_dirty_loop(db, monkeypatch):
    """Rows the upload path would skip anyway must not veto the fast path."""
    db_path, tmp_path, field_image = db
    _add_local_image(db_path, image_id=13, filepath=tmp_path / "gone.jpg")
    # Same local file as the already-synced image 11 → duplicate path.
    _add_local_image(
        db_path, image_id=14, filepath=field_image, image_type="field", sort_order=2
    )
    cloud_sync._set_cloud_image_storage_intent_initialized_ids(1, {11, 13, 14})

    assert cloud_sync._pending_cloud_pushable_image_ids(1) == []

    harness = _PushHarness(monkeypatch, db_path)
    result = harness.run()

    assert result["errors"] == []
    assert harness.took_fast_path_skip, harness.image_prep_calls
    # Steady state: the observation is not left dirty for unpushable rows.
    assert _observation_status(db_path) == "synced"


# ---------------------------------------------------------------------------
# Cloud recovery-cache rows: identity repair, never bytes
# ---------------------------------------------------------------------------
#
# `explain_pending_cloud_image_decision` deliberately treats a cache row that
# lost its `cloud_id` as pending, because it needs an identity repair (see
# tests/test_cloud_sync_image_upload_policy.py
# ::test_cache_row_without_cloud_id_is_pending_for_repair). The new gate
# therefore routes such an observation into full image preparation. These two
# tests run the *real* `_push_images_for_observation` so the repair and the
# byte-upload boundary are exercised, not mocked: the cache row's bytes are
# remote-owned and must never be prepared or re-uploaded, and a successful
# repair must reach a stable subsequent sync rather than a dirty loop.


def test_cache_row_without_cloud_id_repairs_identity_without_preparing_bytes(
    db, monkeypatch
):
    db_path, tmp_path, _field_image = db
    cache_file = tmp_path / "cache-15.webp"
    cache_file.write_bytes(b"remote-owned-bytes")
    _add_local_image(
        db_path,
        image_id=15,
        filepath=cache_file,
        image_type="field",
        sort_order=2,
        objective_name=None,
        source_role="cloud_recovery_cache",
        file_purpose="cache",
    )
    # Cache rows are cloud-owned: the storage-intent ledger does not govern
    # them, so seeding intent for the local canonical row only is realistic.
    cloud_sync._set_cloud_image_storage_intent_initialized_ids(1, {11})

    assert cloud_sync._pending_cloud_pushable_image_ids(1) == [15]

    harness = _PushHarness(
        monkeypatch,
        db_path,
        real_image_push=True,
        extra_remote_images=[_remote_cache_image_row("cloud-image-15", 15, 2)],
    )
    result = harness.run()

    assert result["errors"] == []
    # Full image preparation ran (the fast path would not call the callback).
    assert harness.image_prep_calls == [(1, True)]
    assert len(harness.prepare_calls) == 1
    # …and the cache row was handed to the callback as "do not prepare".
    assert 15 in harness.prepare_calls[0][1]
    # No bytes left the desktop for a remote-owned recovery copy.
    assert harness.client.uploaded_bytes == []
    # The repair the pending state existed for actually happened.
    assert _cloud_id_for_image(db_path, 15) == "cloud-image-15"

    # Steady state: nothing pending, and the next sync takes the shortcut.
    assert cloud_sync._pending_cloud_pushable_image_ids(1) == []
    _mark_observation_dirty(db_path)
    second = harness.run()

    assert second["errors"] == []
    assert harness.image_prep_calls == [(1, True)]
    assert len(harness.prepare_calls) == 1
    assert harness.client.uploaded_bytes == []
    assert _observation_status(db_path) == "synced"


def test_linked_cache_row_retains_the_fast_path_shortcut(db, monkeypatch):
    db_path, tmp_path, _field_image = db
    cache_file = tmp_path / "cache-16.webp"
    cache_file.write_bytes(b"remote-owned-bytes")
    _add_local_image(
        db_path,
        image_id=16,
        filepath=cache_file,
        image_type="field",
        sort_order=2,
        objective_name=None,
        source_role="cloud_recovery_cache",
        file_purpose="cache",
        cloud_id="cloud-image-16",
    )
    cloud_sync._set_cloud_image_storage_intent_initialized_ids(1, {11})

    assert cloud_sync._pending_cloud_pushable_image_ids(1) == []

    harness = _PushHarness(
        monkeypatch,
        db_path,
        real_image_push=True,
        extra_remote_images=[_remote_cache_image_row("cloud-image-16", 16, 2)],
    )
    result = harness.run()

    assert result["errors"] == []
    assert harness.image_prep_calls == []
    assert harness.prepare_calls == []
    assert harness.client.uploaded_bytes == []
    assert _observation_status(db_path) == "synced"


# ---------------------------------------------------------------------------
# Refresh / background sync is untouched
# ---------------------------------------------------------------------------


def test_sync_images_false_does_not_evaluate_upload_completeness(db, monkeypatch):
    db_path, tmp_path, _field_image = db
    micro = tmp_path / "micro.jpg"
    micro.write_bytes(b"microscope-bytes")
    _add_local_image(db_path, image_id=12, filepath=micro)
    cloud_sync._set_cloud_image_storage_intent_initialized_ids(1, {11, 12})

    calls: list[str] = []
    real_initialize = cloud_sync._ensure_cloud_image_storage_intent_initialized
    real_pending = cloud_sync._pending_cloud_pushable_image_ids

    monkeypatch.setattr(
        cloud_sync,
        "_ensure_cloud_image_storage_intent_initialized",
        lambda observation_id: (
            calls.append("initialize"), real_initialize(observation_id)
        )[1],
    )
    monkeypatch.setattr(
        cloud_sync,
        "_pending_cloud_pushable_image_ids",
        lambda observation_id, **kwargs: (
            calls.append("pending"), real_pending(observation_id, **kwargs)
        )[1],
    )

    harness = _PushHarness(monkeypatch, db_path)
    result = harness.run(sync_images=False)

    assert result["errors"] == []
    assert calls == []
    assert harness.image_prep_calls == []
    assert harness.client.uploaded_bytes == []
