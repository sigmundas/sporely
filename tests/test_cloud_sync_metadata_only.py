"""Regression tests for the metadata-only cloud sync paths.

Covers:
- AI crop / calibration reference / notes are treated as image metadata, not
  render/byte-affecting, and never trigger a manual conflict.
- Local-only AI crop edits push as metadata patches with no prepare_images_cb
  call (no WebP is prepared, no bytes are uploaded).
- Cloud-only AI crop edits pull and are applied to the local row without
  re-downloading the image bytes.
- Retroactive calibration reassignment marks the observation dirty and pushes
  the calibration reference change through the metadata-only path.
- Calibration is_active differences are per-device state and are not flagged
  as conflicts.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from database import models
from utils import cloud_sync


def _init_db(tmp_path):
    db_path = tmp_path / "sporely.db"
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
            sync_error_code TEXT,
            sync_error_message TEXT,
            sync_blocked_reason TEXT,
            sync_blocked_at TEXT,
            folder_path TEXT,
            artsdata_id INTEGER,
            publish_target TEXT
        );
        CREATE TABLE calibrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            calibration_uuid TEXT NOT NULL UNIQUE,
            objective_key TEXT NOT NULL,
            calibration_date TEXT NOT NULL,
            calibration_image_date TEXT,
            microns_per_pixel REAL NOT NULL,
            microns_per_pixel_std REAL,
            confidence_interval_low REAL,
            confidence_interval_high REAL,
            num_measurements INTEGER,
            measurements_json TEXT,
            image_filepath TEXT,
            camera TEXT,
            megapixels REAL,
            target_sampling_pct REAL,
            resample_scale_factor REAL,
            calibration_image_width INTEGER,
            calibration_image_height INTEGER,
            notes TEXT,
            is_active INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            p1_x REAL,
            p1_y REAL,
            p2_x REAL,
            p2_y REAL,
            p3_x REAL,
            p3_y REAL,
            p4_x REAL,
            p4_y REAL,
            gallery_rotation INTEGER,
            measured_at TEXT,
            cloud_id TEXT
        );
        CREATE TABLE settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
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


def _seed_observation_with_synced_image(db_path, image_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at, date, user_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01T00:00:00Z", "2026-05-01", "user-123"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, image_type, sort_order,
                created_at, synced_at, notes, crop_mode, source_role, file_purpose
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                11,
                1,
                "cloud-image-11",
                str(image_path),
                "field",
                0,
                "2026-05-01T00:00:00Z",
                "2026-05-01T00:00:00Z",
                "baseline note",
                "full",
                "local_canonical",
                "field",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _snapshot(remote_obs, remote_images, remote_measurements=None):
    return cloud_sync._cloud_observation_snapshot(
        remote_obs, remote_images, remote_measurements or []
    )


class _StubClient:
    def __init__(self, remote_obs, remote_images, remote_measurements=None):
        self.user_id = "user-123"
        self.remote_obs = dict(remote_obs)
        self.remote_images = [dict(row) for row in remote_images]
        self.remote_measurements = [dict(row) for row in (remote_measurements or [])]
        self.push_observation_calls: list[dict] = []
        self.push_image_metadata_calls: list[dict] = []
        self.uploaded_bytes: list[str] = []

    def push_observation(self, obs, remote_obs=None, **kwargs):
        self.push_observation_calls.append(dict(obs))
        return self.remote_obs["id"]

    def get_observation(self, cloud_id):
        return dict(self.remote_obs)

    def pull_image_metadata(self, obs_cloud_id, include_deleted_for_sync=False):
        return [dict(row) for row in self.remote_images]

    def pull_bulk_image_metadata(self, obs_cloud_ids):
        return [dict(row) for row in self.remote_images]

    def pull_measurements_for_images(self, image_cloud_ids):
        return [dict(row) for row in self.remote_measurements]

    def push_image_metadata(self, img, obs_cloud_id, storage_path):
        record = dict(img)
        record["_obs_cloud_id"] = obs_cloud_id
        record["_storage_path"] = storage_path
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

    def _find_cloud_image(self, desktop_id):
        for row in self.remote_images:
            if int(row.get("desktop_id") or 0) == int(desktop_id):
                return str(row.get("id") or "")
        return ""

    def _using_default_r2_loader(self):
        return False

    # ── Stage D spore-summary sync stubs ────────────────────────────────
    #
    # `_push_summary_for_current_observation` (utils/cloud_sync.py) uses
    # the REST primitives below. These tests do not exercise the summary
    # pipeline, so treat every request as a no-op that yields "no existing
    # remote rows" — the helper then decides there is nothing to reconcile
    # and the observation-level `result["errors"]` stays clean.
    def _get(self, path):
        return []

    def _post(self, path, payload):
        return [{"id": 1}]

    def _patch(self, path, payload):
        return None

    def _delete(self, path):
        return None


def _remote_image_row(*, ai_crop=None, calibration_uuid=None, notes="baseline note"):
    row = {
        "id": "cloud-image-11",
        "desktop_id": 11,
        "observation_id": "cloud-obs-1",
        "sort_order": 0,
        "image_type": "field",
        "crop_mode": "full",
        "notes": notes,
        "storage_path": "user/cloud-obs-1/cloud-image-11.webp",
        "original_filename": "image.jpg",
        "ai_crop_x1": None,
        "ai_crop_y1": None,
        "ai_crop_x2": None,
        "ai_crop_y2": None,
        "ai_crop_source_w": None,
        "ai_crop_source_h": None,
        "ai_crop_is_custom": None,
        "calibration_uuid": None,
    }
    if ai_crop is not None:
        (
            row["ai_crop_x1"],
            row["ai_crop_y1"],
            row["ai_crop_x2"],
            row["ai_crop_y2"],
            row["ai_crop_source_w"],
            row["ai_crop_source_h"],
        ) = ai_crop
        row["ai_crop_is_custom"] = 1
    if calibration_uuid is not None:
        row["calibration_uuid"] = calibration_uuid
    return row


# --- Task A: cloud-only AI crop pulls without conflict ------------------------


def test_pull_all_applies_cloud_only_ai_crop_without_conflict(monkeypatch, tmp_path):
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"unchanged-image-bytes")
    _seed_observation_with_synced_image(db_path, image_path)

    baseline_remote_obs = {
        "id": "cloud-obs-1",
        "desktop_id": 1,
        "date": "2026-05-01",
    }
    baseline_remote_image = _remote_image_row()
    stored_snapshot = _snapshot(baseline_remote_obs, [baseline_remote_image])

    updated_remote_image = _remote_image_row(ai_crop=(0.1, 0.1, 0.9, 0.9, 4000, 3000))

    client = _StubClient(baseline_remote_obs, [updated_remote_image])

    monkeypatch.setattr(cloud_sync, "_backfill_missing_exif_on_cloud_images", lambda: None)
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: stored_snapshot)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *args, **kwargs: [])
    monkeypatch.setattr(cloud_sync, "_detect_deleted_remote_observations", lambda remote_obs: [])
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *args, **kwargs: None)

    # Ensure the pull path does not fall through to file downloads.
    def _fake_sync_existing(client_arg, local_image, remote_image, materialize_remote_images=True):
        image_id = int(local_image["id"])
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                "UPDATE images SET ai_crop_x1 = ?, ai_crop_y1 = ?, ai_crop_x2 = ?, "
                "ai_crop_y2 = ?, ai_crop_source_w = ?, ai_crop_source_h = ?, "
                "ai_crop_is_custom = ? WHERE id = ?",
                (
                    remote_image["ai_crop_x1"],
                    remote_image["ai_crop_y1"],
                    remote_image["ai_crop_x2"],
                    remote_image["ai_crop_y2"],
                    remote_image["ai_crop_source_w"],
                    remote_image["ai_crop_source_h"],
                    remote_image["ai_crop_is_custom"],
                    image_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    monkeypatch.setattr(cloud_sync, "_sync_existing_remote_image_to_local", _fake_sync_existing)

    result = cloud_sync.pull_all(
        client,
        remote_obs=[dict(baseline_remote_obs)],
        sync_calibrations=False,
    )

    conn = sqlite3.connect(db_path)
    try:
        image_row = conn.execute(
            "SELECT ai_crop_x1, ai_crop_x2, ai_crop_is_custom FROM images WHERE id = 11"
        ).fetchone()
        obs_status = conn.execute(
            "SELECT sync_status FROM observations WHERE id = 1"
        ).fetchone()
    finally:
        conn.close()

    assert image_row == (0.1, 0.9, 1)
    # AI-crop-only remote edits must not surface as a conflict / needs-review.
    assert not any("needs review" in str(err) for err in result.get("errors", []))
    # And the observation is not left in a dirty state for a cloud-only edit.
    assert obs_status[0] == "synced"


# --- Task A/B: local-only AI crop pushes as metadata-only without conflict ----


def test_push_all_metadata_only_for_local_ai_crop_edit(monkeypatch, tmp_path):
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"stable-bytes")
    _seed_observation_with_synced_image(db_path, image_path)

    # Baseline signature reflects "no AI crop yet".
    baseline_signature = cloud_sync._local_cloud_media_signature(1)

    # Apply a local-only AI crop change and mark the observation dirty.
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE images SET ai_crop_x1 = ?, ai_crop_y1 = ?, ai_crop_x2 = ?, "
            "ai_crop_y2 = ?, ai_crop_source_w = ?, ai_crop_source_h = ?, "
            "ai_crop_is_custom = ? WHERE id = 11",
            (0.05, 0.05, 0.95, 0.95, 4000, 3000, 1),
        )
        conn.execute(
            "UPDATE observations SET sync_status = 'dirty' WHERE id = 1"
        )
        conn.commit()
    finally:
        conn.close()

    remote_obs = {"id": "cloud-obs-1", "desktop_id": 1, "date": "2026-05-01"}
    remote_image = _remote_image_row()
    stored_snapshot = _snapshot(remote_obs, [remote_image])

    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_pending_local_images", lambda: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: stored_snapshot)
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda observation_id: baseline_signature)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda observation_id: baseline_signature)

    client = _StubClient(remote_obs, [remote_image])

    prepare_calls: list[int] = []
    image_prep_calls: list[tuple[int, str, bool]] = []
    measurement_push_calls: list[int] = []

    def prepare_images_cb(obs, progress_cb):
        prepare_calls.append(int(obs["id"]))
        return ([], None, [])

    def fake_push_images_for_observation(client_arg, obs, cloud_id, *, prepare_images_cb=None, **kwargs):
        image_prep_calls.append((int(obs["id"]), str(cloud_id), prepare_images_cb is not None))
        return True

    monkeypatch.setattr(cloud_sync, "_push_images_for_observation", fake_push_images_for_observation)
    monkeypatch.setattr(
        cloud_sync,
        "_push_measurements_for_observation",
        lambda *args, **kwargs: measurement_push_calls.append(int(args[1])),
    )

    result = cloud_sync.push_all(
        client,
        remote_obs=[dict(remote_obs)],
        sync_images=True,
        sync_calibrations=False,
        prepare_images_cb=prepare_images_cb,
    )

    assert result["errors"] == []
    assert prepare_calls == []
    # _push_images_for_observation is invoked WITHOUT prepare_images_cb.
    assert image_prep_calls == [(1, "cloud-obs-1", False)]
    assert measurement_push_calls == [1]


# --- Task B: metadata-only sync does not prepare / upload WebP bytes ----------


def test_metadata_only_sync_does_not_upload_bytes(monkeypatch, tmp_path):
    """End-to-end: AI crop change goes through push_image_metadata only."""
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"stable-bytes")
    _seed_observation_with_synced_image(db_path, image_path)

    # Record a stored file signature so file_matches is True at push time.
    file_sig = cloud_sync._file_content_signature(str(image_path))
    cloud_sync._store_cloud_image_file_signature(1, 11, file_sig)

    baseline_signature = cloud_sync._local_cloud_media_signature(1)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE images SET ai_crop_x1 = ?, ai_crop_y1 = ?, ai_crop_x2 = ?, "
            "ai_crop_y2 = ?, ai_crop_source_w = ?, ai_crop_source_h = ?, "
            "ai_crop_is_custom = ? WHERE id = 11",
            (0.05, 0.05, 0.95, 0.95, 4000, 3000, 1),
        )
        conn.execute(
            "UPDATE observations SET sync_status = 'dirty' WHERE id = 1"
        )
        conn.commit()
    finally:
        conn.close()

    remote_obs = {"id": "cloud-obs-1", "desktop_id": 1, "date": "2026-05-01"}
    remote_image = _remote_image_row()
    stored_snapshot = _snapshot(remote_obs, [remote_image])

    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_pending_local_images", lambda: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: stored_snapshot)
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda observation_id: baseline_signature)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda observation_id: baseline_signature)
    monkeypatch.setattr(cloud_sync, "_push_measurements_for_observation", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_push_pending_image_tombstones", lambda client: [])
    monkeypatch.setattr(cloud_sync, "_local_tombstoned_cloud_image_ids", lambda ids: set())
    monkeypatch.setattr(cloud_sync, "_local_tombstoned_local_image_ids", lambda *args, **kwargs: set())
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: False)
    monkeypatch.setattr(cloud_sync, "resolve_full_original_upload_source", lambda img: None)

    client = _StubClient(remote_obs, [remote_image])

    def prepare_images_cb(obs, progress_cb):
        raise AssertionError("prepare_images_cb must not be called for metadata-only sync")

    result = cloud_sync.push_all(
        client,
        remote_obs=[dict(remote_obs)],
        sync_images=True,
        sync_calibrations=False,
        prepare_images_cb=prepare_images_cb,
    )

    assert result["errors"] == []
    assert client.uploaded_bytes == []
    # One metadata patch for the AI crop change — no WebP encoding involved.
    assert len(client.push_image_metadata_calls) == 1
    patched = client.push_image_metadata_calls[0]
    assert patched.get("ai_crop_x1") == 0.05
    assert patched.get("ai_crop_x2") == 0.95
    assert patched.get("ai_crop_is_custom") == 1


# --- Task D: retroactive recalibration marks obs dirty and syncs metadata ----


def test_recalculate_measurements_for_calibration_marks_observations_dirty(monkeypatch, tmp_path):
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)

    old_uuid = str(uuid.uuid4())
    new_uuid = str(uuid.uuid4())
    old_calibration_id = models.CalibrationDB.add_calibration(
        objective_key="100X",
        microns_per_pixel=0.0315,
        calibration_date="2026-05-01",
        set_active=False,
        calibration_uuid=old_uuid,
    )
    new_calibration_id = models.CalibrationDB.add_calibration(
        objective_key="100X",
        microns_per_pixel=0.0400,
        calibration_date="2026-06-01",
        set_active=True,
        calibration_uuid=new_uuid,
    )

    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"stable-bytes")
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at, date, user_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01T00:00:00Z", "2026-05-01", "user-123"),
        )
        conn.execute(
            "INSERT INTO images (id, observation_id, cloud_id, filepath, image_type, "
            "sort_order, calibration_id, scale_microns_per_pixel, synced_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                11,
                1,
                "cloud-image-11",
                str(image_path),
                "microscope",
                0,
                old_calibration_id,
                0.0315,
                "2026-05-01T00:00:00Z",
            ),
        )
        conn.execute(
            "INSERT INTO spore_measurements (id, image_id, length_um, width_um) VALUES (?, ?, ?, ?)",
            (21, 11, 10.0, 4.0),
        )
        conn.commit()
    finally:
        conn.close()

    updated = models.CalibrationDB.recalculate_measurements_for_calibration(
        old_calibration_id, new_calibration_id, new_scale=0.0400
    )
    assert updated == 1

    conn = sqlite3.connect(db_path)
    try:
        obs_status = conn.execute(
            "SELECT sync_status FROM observations WHERE id = 1"
        ).fetchone()
        image_row = conn.execute(
            "SELECT calibration_id, scale_microns_per_pixel FROM images WHERE id = 11"
        ).fetchone()
        measurement_row = conn.execute(
            "SELECT length_um, width_um FROM spore_measurements WHERE id = 21"
        ).fetchone()
    finally:
        conn.close()

    assert obs_status[0] == "dirty"
    assert image_row[0] == new_calibration_id
    assert image_row[1] == pytest.approx(0.0400)
    # length/width scaled by new/old ratio (~1.269).
    ratio = 0.0400 / 0.0315
    assert measurement_row[0] == pytest.approx(10.0 * ratio)
    assert measurement_row[1] == pytest.approx(4.0 * ratio)


def test_local_media_signature_reflects_calibration_uuid_change(monkeypatch, tmp_path):
    """A retroactive calibration reassignment must move the local signature."""
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)

    uuid_a = str(uuid.uuid4())
    uuid_b = str(uuid.uuid4())
    cal_a = models.CalibrationDB.add_calibration(
        objective_key="100X",
        microns_per_pixel=0.0315,
        calibration_date="2026-05-01",
        calibration_uuid=uuid_a,
    )
    cal_b = models.CalibrationDB.add_calibration(
        objective_key="100X",
        microns_per_pixel=0.0315,
        calibration_date="2026-06-01",
        calibration_uuid=uuid_b,
    )

    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"stable-bytes")
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, date, user_id) VALUES (?, ?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01", "user-123"),
        )
        conn.execute(
            "INSERT INTO images (id, observation_id, cloud_id, filepath, image_type, "
            "sort_order, calibration_id, scale_microns_per_pixel) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (11, 1, "cloud-image-11", str(image_path), "microscope", 0, cal_a, 0.0315),
        )
        conn.commit()
    finally:
        conn.close()

    signature_before = cloud_sync._local_cloud_media_signature(1)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("UPDATE images SET calibration_id = ? WHERE id = 11", (cal_b,))
        conn.commit()
    finally:
        conn.close()

    signature_after = cloud_sync._local_cloud_media_signature(1)
    assert signature_before != signature_after
    # And the change is scoped to the calibration_uuid slot.
    payload_before = json.loads(signature_before)
    payload_after = json.loads(signature_after)
    assert payload_before["images"][0]["calibration_uuid"] == uuid_a
    assert payload_after["images"][0]["calibration_uuid"] == uuid_b


# --- Task C: is_active is not a calibration conflict --------------------------


def test_calibration_is_active_only_difference_is_not_a_conflict(monkeypatch, tmp_path):
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    calibration_uuid = str(uuid.uuid4())

    models.CalibrationDB.add_calibration(
        objective_key="100X",
        microns_per_pixel=0.0315,
        calibration_date="2026-05-01",
        set_active=True,
        calibration_uuid=calibration_uuid,
    )

    remote_rows = [
        {
            "id": "cloud-cal-1",
            "calibration_uuid": calibration_uuid,
            "objective_key": "100X",
            "calibration_date": "2026-05-01",
            "microns_per_pixel": 0.0315,
            "is_active": False,
        }
    ]

    class DummyClient:
        user_id = "user-123"

        def list_remote_calibrations(self):
            return [dict(row) for row in remote_rows]

        def find_remote_calibration(self, calibration_uuid):
            return dict(remote_rows[0])

    client = DummyClient()

    assert cloud_sync.list_calibration_conflicts(client, remote_calibrations=remote_rows) == []
    push_result = cloud_sync.push_calibrations(client, remote_calibrations=remote_rows)
    pull_result = cloud_sync.pull_calibrations(client, remote_calibrations=remote_rows)
    assert push_result["conflicts"] == 0
    assert pull_result["conflicts"] == 0
    assert push_result["errors"] == []
    assert pull_result["errors"] == []
