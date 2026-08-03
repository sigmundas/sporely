"""Preflight tests for push_all's per-observation conflict guard.

pull_all appends a review-needed error whenever local+remote+baseline diverge
on the same observation. Historically push_all was one-sided: it consumed
`remote_only_fields` from :func:`_analyze_observation_field_changes` but
ignored `conflict_fields`, and it never checked measurement or image
conflicts at all. The real regression: obs 677 flipped from
``measurement_conflict`` (pull_all) to ``dirty→synced`` (push_all) on the
next sync, silently clobbering the remote.

These tests pin the mirrored contract:

* :func:`_analyze_observation_push_conflicts` runs the three-way
  comparison across observation metadata, images, measurements, and
  remote-removed media.
* When ``report.has_conflict`` is true, push_all stops before any
  mutation (``push_observation``, ``prepare_images_cb``,
  ``_push_measurements_for_observation``, ``_push_spore_mosaic_for_observation``,
  image PATCH / delete), stays dirty, marks
  ``sync_blocked_reason='conflict_review_pending'``, and emits one
  review-needed error covering every diverged category.
* Successful sync (no conflict) still pushes normally and clears the
  ``sync_blocked_reason`` marker via ``clear_sync_error_state=True``.
* User-driven resolution (Use this device / Use Sporely Cloud) clears the
  marker and advances the snapshot.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from database import models
from utils import cloud_sync


# ---------------------------------------------------------------------------
# Schema + seed helpers
# ---------------------------------------------------------------------------


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
            genus TEXT,
            species TEXT,
            common_name TEXT,
            notes TEXT,
            location TEXT,
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
            sample_source TEXT,
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


def _seed_observation(
    db_path,
    image_path: Path,
    *,
    obs_notes: str = "baseline note",
    obs_common: str = "Baseline common",
    image_notes: str = "baseline image note",
    image_type: str = "microscope",
    measurement: tuple[float, float] | None = None,
    measurement_cloud_id: str | None = "cloud-meas-21",
):
    """Seed a `synced` observation → image (+ optional measurement) row."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at, date, "
            "user_id, genus, species, common_name, notes, location) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                1,
                "cloud-obs-1",
                "synced",
                "2026-05-01T00:00:00Z",
                "2026-05-01",
                "user-123",
                "Amanita",
                "muscaria",
                obs_common,
                obs_notes,
                "Somewhere",
            ),
        )
        conn.execute(
            "INSERT INTO images ("
            "id, observation_id, cloud_id, filepath, image_type, sort_order, "
            "created_at, synced_at, notes, crop_mode, source_role, file_purpose"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                11,
                1,
                "cloud-image-11",
                str(image_path),
                image_type,
                0,
                "2026-05-01T00:00:00Z",
                "2026-05-01T00:00:00Z",
                image_notes,
                "full",
                "local_canonical",
                "field",
            ),
        )
        if measurement is not None:
            length_um, width_um = measurement
            conn.execute(
                "INSERT INTO spore_measurements ("
                "id, image_id, length_um, width_um, measurement_type, gallery_rotation, "
                "p1_x, p1_y, p2_x, p2_y, p3_x, p3_y, p4_x, p4_y, measured_at, cloud_id"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    21,
                    11,
                    length_um,
                    width_um,
                    "spore",
                    0,
                    1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
                    "2026-05-01T00:00:00Z",
                    measurement_cloud_id,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _remote_image_row(
    *,
    cloud_id: str = "cloud-image-11",
    desktop_id: int | None = 11,
    notes: str = "baseline image note",
    image_type: str = "microscope",
    sort_order: int = 0,
    ai_crop_x1: float | None = None,
    ai_crop_x2: float | None = None,
) -> dict:
    return {
        "id": cloud_id,
        "desktop_id": desktop_id,
        "observation_id": "cloud-obs-1",
        "sort_order": sort_order,
        "image_type": image_type,
        "crop_mode": "full",
        "notes": notes,
        "storage_path": f"user/cloud-obs-1/{cloud_id}.webp",
        "original_filename": "image.jpg",
        "ai_crop_x1": ai_crop_x1,
        "ai_crop_y1": None,
        "ai_crop_x2": ai_crop_x2,
        "ai_crop_y2": None,
        "ai_crop_source_w": None,
        "ai_crop_source_h": None,
        "ai_crop_is_custom": 1 if ai_crop_x1 is not None else None,
        "calibration_uuid": None,
    }


def _remote_measurement_row(
    *,
    cloud_id: str = "cloud-meas-21",
    image_cloud_id: str = "cloud-image-11",
    desktop_id: int | None = 21,
    length_um: float = 10.5,
    width_um: float = 5.2,
) -> dict:
    return {
        "id": cloud_id,
        "image_id": image_cloud_id,
        "desktop_id": desktop_id,
        "length_um": length_um,
        "width_um": width_um,
        "measurement_type": "spore",
        "gallery_rotation": 0,
        "p1_x": 1.0, "p1_y": 2.0,
        "p2_x": 3.0, "p2_y": 4.0,
        "p3_x": 5.0, "p3_y": 6.0,
        "p4_x": 7.0, "p4_y": 8.0,
        "measured_at": "2026-05-01T00:00:00Z",
    }


def _baseline_remote_obs(*, notes: str = "baseline note", common: str = "Baseline common") -> dict:
    return {
        "id": "cloud-obs-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "genus": "Amanita",
        "species": "muscaria",
        "common_name": common,
        "notes": notes,
        "location": "Somewhere",
    }


def _snapshot(remote_obs, remote_images, remote_measurements=None) -> str:
    return cloud_sync._cloud_observation_snapshot(
        remote_obs, remote_images, remote_measurements or []
    )


def _mark_observation_dirty(db_path, observation_id: int = 1):
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE observations SET sync_status = 'dirty' WHERE id = ?",
            (int(observation_id),),
        )
        conn.commit()
    finally:
        conn.close()


def _load_obs_sync_state(db_path, observation_id: int = 1) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT sync_status, sync_blocked_reason, sync_blocked_at, synced_at, cloud_id "
            "FROM observations WHERE id = ?",
            (int(observation_id),),
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else {}


class _StubClient:
    """Client stub matching the surface push_all + resolution paths touch."""

    def __init__(
        self,
        remote_obs: dict,
        remote_images: list[dict] | None = None,
        remote_measurements: list[dict] | None = None,
    ):
        self.user_id = "user-123"
        self.remote_obs = dict(remote_obs)
        self.remote_images = [dict(row or {}) for row in (remote_images or [])]
        self.remote_measurements = [dict(row or {}) for row in (remote_measurements or [])]
        self.push_observation_calls: list[dict] = []
        self.push_image_metadata_calls: list[dict] = []
        self.upload_image_calls: list[str] = []
        self.delete_calls: list[str] = []

    # --- observation endpoints -------------------------------------------------
    def push_observation(self, obs, remote_obs=None, **kwargs):
        self.push_observation_calls.append(dict(obs))
        return str(self.remote_obs.get("id") or "")

    def get_observation(self, cloud_id):
        return dict(self.remote_obs)

    # --- image endpoints -------------------------------------------------------
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
        self.upload_image_calls.append(str(local_path))
        return kwargs.get("storage_path") or "user/upload.webp"

    def delete_image(self, cloud_image_id):
        self.delete_calls.append(str(cloud_image_id))
        return None

    def soft_delete_image(self, cloud_image_id, *args, **kwargs):
        self.delete_calls.append(str(cloud_image_id))
        return None

    def set_desktop_id(self, *args, **kwargs):
        return None

    def set_measurement_desktop_id(self, *args, **kwargs):
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

    # --- summary endpoints (no-op) --------------------------------------------
    def _get(self, path):
        return []

    def _post(self, path, payload):
        return [{"id": 1}]

    def _patch(self, path, payload):
        return None

    def _delete(self, path):
        return None


# ---------------------------------------------------------------------------
# Common push_all patches (all tests bypass the same networking side-doors)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolate_side_effects(monkeypatch):
    """Stub summary/measurement reconciliation and byte-oriented paths."""
    monkeypatch.setattr(cloud_sync, "_push_summary_for_current_observation", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_reconcile_missing_spore_summaries", lambda *args, **kwargs: 0)
    monkeypatch.setattr(cloud_sync, "_reconcile_missing_spore_measurements", lambda *args, **kwargs: 0)
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_pending_local_images", lambda **_kwargs: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_push_pending_image_tombstones", lambda client: [])
    monkeypatch.setattr(cloud_sync, "_local_tombstoned_cloud_image_ids", lambda ids: set())
    monkeypatch.setattr(cloud_sync, "_local_tombstoned_local_image_ids", lambda *args, **kwargs: set())
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: False)
    monkeypatch.setattr(cloud_sync, "resolve_full_original_upload_source", lambda img: None)
    monkeypatch.setattr(cloud_sync, "_record_remote_image_tombstones", lambda *args, **kwargs: None)


def _stub_snapshot_and_signature(
    monkeypatch,
    *,
    stored_snapshot: str,
    baseline_signature: str,
):
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: stored_snapshot)
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda observation_id: baseline_signature)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda observation_id: baseline_signature)


def _track_push_calls(monkeypatch):
    """Return dict of lists that push_all's mutation entry points are patched to append to."""
    calls: dict[str, list] = {
        "images": [],
        "measurements": [],
        "mosaic": [],
        "prepare": [],
    }

    def fake_push_images_for_observation(client_arg, obs, cloud_id, *, prepare_images_cb=None, **kwargs):
        calls["images"].append((int(obs["id"]), str(cloud_id), prepare_images_cb is not None))
        return True

    def fake_push_measurements(client, obs_local_id):
        calls["measurements"].append(int(obs_local_id))

    def fake_push_mosaic(client, obs_local_id, cloud_id):
        calls["mosaic"].append((int(obs_local_id), str(cloud_id)))

    monkeypatch.setattr(cloud_sync, "_push_images_for_observation", fake_push_images_for_observation)
    monkeypatch.setattr(cloud_sync, "_push_measurements_for_observation", fake_push_measurements)
    monkeypatch.setattr(cloud_sync, "_push_spore_mosaic_for_observation", fake_push_mosaic)
    return calls


# ---------------------------------------------------------------------------
# 1. Pure-helper tests: _analyze_observation_push_conflicts
# ---------------------------------------------------------------------------


def _make_helper_inputs(
    *,
    local_notes: str = "baseline",
    remote_notes: str = "baseline",
    baseline_notes: str = "baseline",
    local_image_notes: str = "baseline image",
    remote_image_notes: str = "baseline image",
    baseline_image_notes: str = "baseline image",
    local_meas_length: float = 10.0,
    remote_meas_length: float = 10.0,
    baseline_meas_length: float = 10.0,
    include_measurements: bool = True,
    remote_removed_image: bool = False,
) -> dict:
    baseline_obs = {
        "id": "cloud-obs-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "genus": "Amanita",
        "species": "muscaria",
        "notes": baseline_notes,
    }
    local_obs = dict(baseline_obs, id=1, cloud_id="cloud-obs-1", notes=local_notes)
    remote_obs = dict(baseline_obs, notes=remote_notes)

    baseline_image = _remote_image_row(notes=baseline_image_notes)
    remote_image = _remote_image_row(notes=remote_image_notes)
    local_image = {
        "id": 11,
        "cloud_id": "cloud-image-11",
        "observation_id": 1,
        "filepath": "/tmp/image.jpg",
        "image_type": "microscope",
        "sort_order": 0,
        "crop_mode": "full",
        "notes": local_image_notes,
    }

    baseline_measurement = {
        "id": "cloud-meas-21",
        "image_id": "cloud-image-11",
        "desktop_id": 21,
        "length_um": baseline_meas_length,
        "width_um": 5.2,
        "measurement_type": "spore",
        "gallery_rotation": 0,
        "p1_x": 1.0, "p1_y": 2.0,
        "p2_x": 3.0, "p2_y": 4.0,
        "p3_x": 5.0, "p3_y": 6.0,
        "p4_x": 7.0, "p4_y": 8.0,
        "measured_at": "2026-05-01T00:00:00Z",
    }
    remote_measurement = dict(baseline_measurement, length_um=remote_meas_length)
    local_measurement = {
        "id": 21,
        "cloud_id": "cloud-meas-21",
        "image_id": 11,
        "image_cloud_id": "cloud-image-11",
        "length_um": local_meas_length,
        "width_um": 5.2,
        "measurement_type": "spore",
        "gallery_rotation": 0,
        "p1_x": 1.0, "p1_y": 2.0,
        "p2_x": 3.0, "p2_y": 4.0,
        "p3_x": 5.0, "p3_y": 6.0,
        "p4_x": 7.0, "p4_y": 8.0,
        "measured_at": "2026-05-01T00:00:00Z",
    }

    baseline_snapshot_dict = {
        "observation": baseline_obs,
        "images": [baseline_image],
        "measurements": [baseline_measurement] if include_measurements else [],
    }

    remote_images: list[dict] = [] if remote_removed_image else [remote_image]

    return {
        "local_obs": local_obs,
        "local_images": [local_image],
        "local_measurements_by_cloud_id": (
            {"cloud-meas-21": local_measurement} if include_measurements else {}
        ),
        "remote_obs": remote_obs,
        "remote_images": remote_images,
        "remote_measurements": (
            [remote_measurement] if include_measurements else []
        ),
        "baseline_snapshot": baseline_snapshot_dict,
    }


def test_helper_returns_no_conflict_when_only_remote_changed():
    report = cloud_sync._analyze_observation_push_conflicts(
        **_make_helper_inputs(
            local_notes="baseline",
            remote_notes="cloud edit",
            baseline_notes="baseline",
        )
    )
    assert report.has_conflict is False
    assert report.categories == []
    assert report.field_labels == []
    assert report.measurement_conflict_ids == []
    assert report.image_conflict_keys == []
    assert report.remote_removed_image_keys == []


def test_helper_returns_no_conflict_when_only_local_changed():
    report = cloud_sync._analyze_observation_push_conflicts(
        **_make_helper_inputs(
            local_notes="local edit",
            remote_notes="baseline",
            baseline_notes="baseline",
        )
    )
    assert report.has_conflict is False
    assert report.categories == []


def test_helper_flags_observation_metadata_conflict():
    report = cloud_sync._analyze_observation_push_conflicts(
        **_make_helper_inputs(
            local_notes="local edit",
            remote_notes="cloud edit",
            baseline_notes="baseline",
        )
    )
    assert report.has_conflict is True
    assert "observation" in report.categories
    assert any("Notes".lower() in label.lower() for label in report.field_labels)


def test_helper_flags_measurement_conflict():
    report = cloud_sync._analyze_observation_push_conflicts(
        **_make_helper_inputs(
            local_meas_length=12.0,
            remote_meas_length=15.0,
            baseline_meas_length=10.0,
        )
    )
    assert report.has_conflict is True
    assert "measurements" in report.categories
    assert 21 in report.measurement_conflict_ids


def test_helper_flags_mixed_metadata_and_measurement_conflict():
    report = cloud_sync._analyze_observation_push_conflicts(
        **_make_helper_inputs(
            local_notes="local edit",
            remote_notes="cloud edit",
            baseline_notes="baseline",
            local_meas_length=12.0,
            remote_meas_length=15.0,
            baseline_meas_length=10.0,
        )
    )
    assert report.has_conflict is True
    assert set(report.categories) >= {"observation", "measurements"}
    reasons = cloud_sync._format_push_conflict_review_reasons(report)
    joined = ' | '.join(reasons)
    assert "measurements" in joined.lower()
    assert any("Notes".lower() in reason.lower() for reason in reasons)


def test_helper_flags_remote_removed_media():
    report = cloud_sync._analyze_observation_push_conflicts(
        **_make_helper_inputs(remote_removed_image=True)
    )
    assert report.has_conflict is True
    assert "remote_removed_media" in report.categories
    assert report.remote_removed_image_keys


def test_helper_flags_shared_image_metadata_conflict():
    inputs = _make_helper_inputs(
        local_image_notes="local image note",
        remote_image_notes="cloud image note",
        baseline_image_notes="baseline image",
    )
    report = cloud_sync._analyze_observation_push_conflicts(**inputs)
    assert report.has_conflict is True
    assert "images" in report.categories
    assert report.image_conflict_keys


def test_helper_no_conflict_for_pure_local_measurement_addition():
    """Safe additions: local has an extra measurement with no cloud_id; remote is baseline."""
    inputs = _make_helper_inputs()
    # No local baseline measurement — this is a pure add.
    inputs["local_measurements_by_cloud_id"] = {}
    report = cloud_sync._analyze_observation_push_conflicts(**inputs)
    assert report.has_conflict is False


# ---------------------------------------------------------------------------
# 2. Integration tests: push_all guards mutation on conflict
# ---------------------------------------------------------------------------


def _run_push_all_with(client, monkeypatch, remote_obs, *, sync_images=True):
    return cloud_sync.push_all(
        client,
        remote_obs=[dict(remote_obs)],
        sync_images=sync_images,
        sync_calibrations=False,
        prepare_images_cb=lambda obs, progress_cb: monkeypatch,  # sentinel, must not be called
    )


def test_push_all_blocks_on_observation_metadata_conflict(monkeypatch, tmp_path):
    """Scenario 1: three-way metadata conflict must stop all mutation."""
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"bytes")
    _seed_observation(db_path, image_path, obs_notes="local edit")
    _mark_observation_dirty(db_path)

    baseline_remote_obs = _baseline_remote_obs(notes="baseline note")
    baseline_remote_image = _remote_image_row()
    stored_snapshot = _snapshot(baseline_remote_obs, [baseline_remote_image])
    baseline_signature = cloud_sync._local_cloud_media_signature(1)

    live_remote_obs = _baseline_remote_obs(notes="cloud edit")
    _stub_snapshot_and_signature(
        monkeypatch, stored_snapshot=stored_snapshot, baseline_signature=baseline_signature
    )
    calls = _track_push_calls(monkeypatch)

    prepare_calls: list[int] = []

    def prepare_images_cb(obs, progress_cb):
        prepare_calls.append(int(obs["id"]))
        return ([], None, [])

    client = _StubClient(live_remote_obs, [_remote_image_row(notes="baseline image note")])

    result = cloud_sync.push_all(
        client,
        remote_obs=[dict(live_remote_obs)],
        sync_images=True,
        sync_calibrations=False,
        prepare_images_cb=prepare_images_cb,
    )

    # Blocked: none of the mutation entry points fired.
    assert client.push_observation_calls == []
    assert prepare_calls == []
    assert calls["images"] == []
    assert calls["measurements"] == []
    assert calls["mosaic"] == []

    # Exactly one review-needed error for this observation.
    review_errors = [err for err in (result.get("errors") or []) if "needs review" in str(err)]
    assert len(review_errors) == 1
    assert "cloud-obs-1" in review_errors[0]

    state = _load_obs_sync_state(db_path)
    assert state["sync_status"] == "dirty"
    assert state["sync_blocked_reason"] == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER
    assert state["sync_blocked_at"]


def test_push_all_blocks_on_measurement_conflict(monkeypatch, tmp_path):
    """Scenario 2: measurement three-way conflict must stop push+prep+mosaic."""
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"bytes")
    _seed_observation(db_path, image_path, measurement=(12.0, 5.2))
    _mark_observation_dirty(db_path)

    baseline_remote_obs = _baseline_remote_obs()
    baseline_remote_image = _remote_image_row()
    baseline_remote_measurement = _remote_measurement_row(length_um=10.0)
    stored_snapshot = _snapshot(
        baseline_remote_obs,
        [baseline_remote_image],
        [baseline_remote_measurement],
    )
    baseline_signature = cloud_sync._local_cloud_media_signature(1)

    live_remote_measurement = _remote_measurement_row(length_um=15.0)  # differs from local 12.0
    _stub_snapshot_and_signature(
        monkeypatch, stored_snapshot=stored_snapshot, baseline_signature=baseline_signature
    )
    calls = _track_push_calls(monkeypatch)

    prepare_calls: list[int] = []

    def prepare_images_cb(obs, progress_cb):
        prepare_calls.append(int(obs["id"]))
        return ([], None, [])

    client = _StubClient(
        baseline_remote_obs,
        [baseline_remote_image],
        [live_remote_measurement],
    )

    result = cloud_sync.push_all(
        client,
        remote_obs=[dict(baseline_remote_obs)],
        sync_images=True,
        sync_calibrations=False,
        prepare_images_cb=prepare_images_cb,
    )

    assert client.push_observation_calls == []
    assert prepare_calls == []
    assert calls["images"] == []
    assert calls["measurements"] == []
    assert calls["mosaic"] == []

    review_errors = [err for err in (result.get("errors") or []) if "needs review" in str(err)]
    assert len(review_errors) == 1
    assert "measurements" in review_errors[0].lower()

    state = _load_obs_sync_state(db_path)
    assert state["sync_status"] == "dirty"
    assert state["sync_blocked_reason"] == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER


def test_push_all_blocks_on_image_metadata_conflict(monkeypatch, tmp_path):
    """Scenario 3: image metadata diverged on both sides — no upload, no PATCH, no delete."""
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"bytes")
    # Seed with baseline image notes so the baseline signature reflects the pre-edit state.
    _seed_observation(db_path, image_path, image_notes="baseline image note")

    baseline_remote_obs = _baseline_remote_obs()
    baseline_remote_image = _remote_image_row(notes="baseline image note")
    stored_snapshot = _snapshot(baseline_remote_obs, [baseline_remote_image])
    baseline_signature = cloud_sync._local_cloud_media_signature(1)

    # Now apply the local edit to the image and mark the observation dirty.
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("UPDATE images SET notes = ? WHERE id = 11", ("local image edit",))
        conn.commit()
    finally:
        conn.close()
    _mark_observation_dirty(db_path)

    live_remote_image = _remote_image_row(notes="cloud image edit")
    _stub_snapshot_and_signature(
        monkeypatch, stored_snapshot=stored_snapshot, baseline_signature=baseline_signature
    )
    calls = _track_push_calls(monkeypatch)

    client = _StubClient(baseline_remote_obs, [live_remote_image])

    def prepare_images_cb(obs, progress_cb):
        return ([], None, [])

    result = cloud_sync.push_all(
        client,
        remote_obs=[dict(baseline_remote_obs)],
        sync_images=True,
        sync_calibrations=False,
        prepare_images_cb=prepare_images_cb,
    )

    assert client.push_observation_calls == []
    assert client.push_image_metadata_calls == []
    assert client.upload_image_calls == []
    assert client.delete_calls == []
    assert calls["images"] == []
    assert calls["measurements"] == []
    assert calls["mosaic"] == []

    review_errors = [err for err in (result.get("errors") or []) if "needs review" in str(err)]
    assert len(review_errors) == 1
    assert "images changed" in review_errors[0]

    state = _load_obs_sync_state(db_path)
    assert state["sync_status"] == "dirty"
    assert state["sync_blocked_reason"] == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER


def test_push_all_blocks_on_mixed_metadata_and_measurement_conflict(monkeypatch, tmp_path):
    """Scenario 4: obs + measurement conflict → single review-needed error, both categories cited."""
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"bytes")
    _seed_observation(db_path, image_path, obs_notes="local edit", measurement=(12.0, 5.2))
    _mark_observation_dirty(db_path)

    baseline_remote_obs = _baseline_remote_obs(notes="baseline note")
    baseline_remote_image = _remote_image_row()
    baseline_remote_measurement = _remote_measurement_row(length_um=10.0)
    stored_snapshot = _snapshot(
        baseline_remote_obs,
        [baseline_remote_image],
        [baseline_remote_measurement],
    )
    baseline_signature = cloud_sync._local_cloud_media_signature(1)

    live_remote_obs = _baseline_remote_obs(notes="cloud edit")
    live_remote_measurement = _remote_measurement_row(length_um=15.0)
    _stub_snapshot_and_signature(
        monkeypatch, stored_snapshot=stored_snapshot, baseline_signature=baseline_signature
    )
    calls = _track_push_calls(monkeypatch)

    client = _StubClient(
        live_remote_obs,
        [baseline_remote_image],
        [live_remote_measurement],
    )

    def prepare_images_cb(obs, progress_cb):
        return ([], None, [])

    result = cloud_sync.push_all(
        client,
        remote_obs=[dict(live_remote_obs)],
        sync_images=True,
        sync_calibrations=False,
        prepare_images_cb=prepare_images_cb,
    )

    assert client.push_observation_calls == []
    assert calls["images"] == []
    assert calls["measurements"] == []
    assert calls["mosaic"] == []

    review_errors = [err for err in (result.get("errors") or []) if "needs review" in str(err)]
    assert len(review_errors) == 1
    err = review_errors[0]
    # Both categories mentioned in the single error message.
    assert "measurements" in err.lower()
    assert "notes" in err.lower()


def test_push_all_applies_remote_only_fields_without_conflict(monkeypatch, tmp_path):
    """Scenario 5 (regression guard): remote-only edit on one field merges into
    the push payload while a distinct local edit on another field still pushes."""
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"bytes")
    # Seed matches baseline so signatures line up.
    _seed_observation(db_path, image_path, obs_notes="baseline note", obs_common="Baseline common")

    baseline_remote_obs = _baseline_remote_obs(notes="baseline note", common="Baseline common")
    baseline_remote_image = _remote_image_row()
    stored_snapshot = _snapshot(baseline_remote_obs, [baseline_remote_image])
    baseline_signature = cloud_sync._local_cloud_media_signature(1)

    # Local edit on common_name only; remote edit on notes only — no overlap, no conflict.
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("UPDATE observations SET common_name = ? WHERE id = 1", ("Local common",))
        conn.commit()
    finally:
        conn.close()
    _mark_observation_dirty(db_path)

    live_remote_obs = _baseline_remote_obs(notes="cloud added note", common="Baseline common")
    _stub_snapshot_and_signature(
        monkeypatch, stored_snapshot=stored_snapshot, baseline_signature=baseline_signature
    )
    _track_push_calls(monkeypatch)

    client = _StubClient(live_remote_obs, [baseline_remote_image])

    def prepare_images_cb(obs, progress_cb):
        return ([], None, [])

    result = cloud_sync.push_all(
        client,
        remote_obs=[dict(live_remote_obs)],
        sync_images=True,
        sync_calibrations=False,
        prepare_images_cb=prepare_images_cb,
    )

    # No conflict error, observation was pushed with the remote-only merge applied.
    review_errors = [err for err in (result.get("errors") or []) if "needs review" in str(err)]
    assert review_errors == []
    assert client.push_observation_calls, "expected a push_observation call after successful preflight"
    pushed_payload = client.push_observation_calls[0]
    # Remote-only field merged into push payload.
    assert pushed_payload.get("notes") == "cloud added note"
    # Local-only field still present on the payload.
    assert pushed_payload.get("common_name") == "Local common"

    state = _load_obs_sync_state(db_path)
    assert state["sync_status"] == "synced"
    # Marker cleared via clear_sync_error_state=True.
    assert state["sync_blocked_reason"] in (None, "")


# ---------------------------------------------------------------------------
# 3. Deferral (Review later): second sync re-detects, still blocked
# ---------------------------------------------------------------------------


def test_review_later_leaves_observation_blocked_second_sync_reevaluates(monkeypatch, tmp_path):
    """Scenario 7: closing the review dialog without a decision defers, next sync repeats the block."""
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"bytes")
    _seed_observation(db_path, image_path, obs_notes="local edit")
    _mark_observation_dirty(db_path)

    baseline_remote_obs = _baseline_remote_obs(notes="baseline note")
    baseline_remote_image = _remote_image_row()
    stored_snapshot = _snapshot(baseline_remote_obs, [baseline_remote_image])
    baseline_signature = cloud_sync._local_cloud_media_signature(1)

    live_remote_obs = _baseline_remote_obs(notes="cloud edit")
    _stub_snapshot_and_signature(
        monkeypatch, stored_snapshot=stored_snapshot, baseline_signature=baseline_signature
    )
    calls = _track_push_calls(monkeypatch)

    client = _StubClient(live_remote_obs, [baseline_remote_image])

    def prepare_images_cb(obs, progress_cb):
        return ([], None, [])

    cloud_sync.push_all(
        client,
        remote_obs=[dict(live_remote_obs)],
        sync_images=True,
        sync_calibrations=False,
        prepare_images_cb=prepare_images_cb,
    )
    first_state = _load_obs_sync_state(db_path)
    assert first_state["sync_status"] == "dirty"
    assert first_state["sync_blocked_reason"] == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER

    # Simulate user closing the dialog with "Review later": no snapshot advance,
    # no local writes. Second sync must repeat the block.
    result2 = cloud_sync.push_all(
        client,
        remote_obs=[dict(live_remote_obs)],
        sync_images=True,
        sync_calibrations=False,
        prepare_images_cb=prepare_images_cb,
    )

    assert client.push_observation_calls == []
    assert calls["images"] == []
    assert calls["measurements"] == []
    assert calls["mosaic"] == []
    review_errors = [err for err in (result2.get("errors") or []) if "needs review" in str(err)]
    assert len(review_errors) == 1

    second_state = _load_obs_sync_state(db_path)
    assert second_state["sync_status"] == "dirty"
    assert second_state["sync_blocked_reason"] == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER


# ---------------------------------------------------------------------------
# 4. Resolution paths: Use this device / Use Sporely Cloud
# ---------------------------------------------------------------------------


def test_resolve_keep_local_clears_conflict_marker_and_advances_snapshot(monkeypatch, tmp_path):
    """Scenario 8: 'Use this device' resolution pushes local + clears sync_blocked_reason."""
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"bytes")
    _seed_observation(db_path, image_path, obs_notes="local edit")
    # Precondition: observation is already conflict-blocked from a prior sync.
    cloud_sync._set_observation_conflict_review_pending(1)

    remote_obs = _baseline_remote_obs(notes="cloud edit")
    remote_image = _remote_image_row(notes="baseline image note")

    # No image / measurement / mosaic side-effects for this focused resolution test.
    snapshot_writes: list[str] = []
    monkeypatch.setattr(cloud_sync, "_pull_remote_images_for_sync", lambda client, cloud_id: [dict(remote_image)])
    monkeypatch.setattr(cloud_sync, "_record_remote_image_tombstones", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_apply_remote_images_to_local", lambda *args, **kwargs: [])
    monkeypatch.setattr(cloud_sync, "_push_images_for_observation", lambda *args, **kwargs: True)
    monkeypatch.setattr(cloud_sync, "_push_measurements_for_observation", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_push_spore_mosaic_for_observation", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: snapshot_writes.append("keep_local"))
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda observation_id: "")
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: "")

    client = _StubClient(remote_obs, [remote_image])

    def prepare_images_cb(obs, progress_cb):
        return ([], None, [])

    result = cloud_sync.resolve_conflict_keep_local(
        client, local_id=1, prepare_images_cb=prepare_images_cb
    )
    assert result.get("cloud_id") == "cloud-obs-1"
    assert client.push_observation_calls  # local wrote through

    state = _load_obs_sync_state(db_path)
    assert state["sync_status"] == "synced"
    # Marker gone.
    assert state["sync_blocked_reason"] in (None, "")
    assert state["sync_blocked_at"] in (None, "")
    # Snapshot advanced via _store_remote_snapshot.
    assert snapshot_writes


def test_resolve_keep_cloud_clears_conflict_marker_without_local_overwrite(monkeypatch, tmp_path):
    """Scenario 9: 'Use Sporely Cloud' resolution applies remote locally + clears the marker."""
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"bytes")
    _seed_observation(db_path, image_path, obs_notes="local edit")
    cloud_sync._set_observation_conflict_review_pending(1)

    remote_obs = _baseline_remote_obs(notes="cloud edit")
    remote_image = _remote_image_row(notes="baseline image note")

    monkeypatch.setattr(cloud_sync, "_pull_remote_images_for_sync", lambda client, cloud_id: [dict(remote_image)])
    monkeypatch.setattr(cloud_sync, "_apply_remote_images_to_local", lambda *args, **kwargs: [])
    applied_fields: dict[str, dict] = {}

    def fake_apply(local_id, remote_row, fields=None):
        applied_fields["applied"] = dict(remote_row)

    monkeypatch.setattr(cloud_sync, "_apply_remote_observation_fields", fake_apply)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda observation_id: "")

    client = _StubClient(remote_obs, [remote_image])
    result = cloud_sync.resolve_conflict_keep_cloud(client, local_id=1)
    assert result.get("cloud_id") == "cloud-obs-1"
    # Remote applied locally; NO push_observation call to overwrite cloud.
    assert client.push_observation_calls == []
    assert applied_fields.get("applied", {}).get("notes") == "cloud edit"

    state = _load_obs_sync_state(db_path)
    assert state["sync_status"] == "synced"
    assert state["sync_blocked_reason"] in (None, "")
    assert state["sync_blocked_at"] in (None, "")


# ---------------------------------------------------------------------------
# 5. Sibling isolation: one conflict does not block a clean observation
# ---------------------------------------------------------------------------


def _seed_second_clean_observation(db_path, image_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at, date, "
            "user_id, genus, species, common_name, notes, location) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                2,
                None,
                "dirty",
                None,
                "2026-05-02",
                "user-123",
                "Boletus",
                "edulis",
                "Porcini",
                "sibling note",
                "Elsewhere",
            ),
        )
        conn.execute(
            "INSERT INTO images ("
            "id, observation_id, cloud_id, filepath, image_type, sort_order, "
            "created_at, notes, crop_mode, source_role, file_purpose"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                12,
                2,
                None,
                str(image_path),
                "field",
                0,
                "2026-05-02T00:00:00Z",
                "sibling image",
                "full",
                "local_canonical",
                "field",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def test_push_all_conflict_does_not_block_sibling_observation(monkeypatch, tmp_path):
    """Scenario 10: obs A blocked on conflict; obs B (fresh, no cloud_id) still pushes."""
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    image_path_a = tmp_path / "image_a.jpg"
    image_path_a.write_bytes(b"bytes-a")
    _seed_observation(db_path, image_path_a, obs_notes="local edit")
    _mark_observation_dirty(db_path, 1)

    image_path_b = tmp_path / "image_b.jpg"
    image_path_b.write_bytes(b"bytes-b")
    _seed_second_clean_observation(db_path, image_path_b)

    baseline_remote_obs = _baseline_remote_obs(notes="baseline note")
    baseline_remote_image = _remote_image_row()
    stored_snapshot_a = _snapshot(baseline_remote_obs, [baseline_remote_image])
    baseline_signature = cloud_sync._local_cloud_media_signature(1)

    live_remote_obs = _baseline_remote_obs(notes="cloud edit")

    # Only cloud-obs-1 has a stored snapshot; cloud-obs-2 is new (no snapshot).
    def stub_load_snapshot(cloud_id):
        return stored_snapshot_a if str(cloud_id or "") == "cloud-obs-1" else ""

    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", stub_load_snapshot)
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda observation_id: baseline_signature)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda observation_id: baseline_signature)

    calls = _track_push_calls(monkeypatch)

    class _SiblingClient(_StubClient):
        def push_observation(self, obs, remote_obs=None, **kwargs):
            self.push_observation_calls.append(dict(obs))
            # obs 2 has no cloud_id yet — return a fresh one.
            local_id = int(obs.get("id") or 0)
            if local_id == 2:
                return "cloud-obs-2"
            return "cloud-obs-1"

    client = _SiblingClient(live_remote_obs, [baseline_remote_image])

    def prepare_images_cb(obs, progress_cb):
        return ([], None, [])

    result = cloud_sync.push_all(
        client,
        remote_obs=[dict(live_remote_obs)],
        sync_images=True,
        sync_calibrations=False,
        prepare_images_cb=prepare_images_cb,
    )

    # obs 1 blocked, obs 2 pushed cleanly.
    review_errors = [err for err in (result.get("errors") or []) if "needs review" in str(err)]
    assert len(review_errors) == 1
    assert "cloud-obs-1" in review_errors[0]

    pushed_ids = {int(row.get("id") or 0) for row in client.push_observation_calls}
    assert pushed_ids == {2}

    state_a = _load_obs_sync_state(db_path, 1)
    state_b = _load_obs_sync_state(db_path, 2)
    assert state_a["sync_status"] == "dirty"
    assert state_a["sync_blocked_reason"] == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER
    assert state_b["sync_status"] == "synced"
    assert state_b["cloud_id"] == "cloud-obs-2"


# ---------------------------------------------------------------------------
# 6. Marker-clear semantics: successful subsequent sync clears the marker.
# ---------------------------------------------------------------------------


def test_conflict_marker_clears_on_next_clean_sync(monkeypatch, tmp_path):
    """After a conflict blocks obs 1, when the remote no longer diverges the next
    push_all succeeds and clears ``sync_blocked_reason``."""
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"bytes")
    _seed_observation(db_path, image_path, obs_notes="local edit")
    _mark_observation_dirty(db_path)
    # Simulate a prior conflict marker.
    cloud_sync._set_observation_conflict_review_pending(1)

    baseline_remote_obs = _baseline_remote_obs(notes="baseline note")
    baseline_remote_image = _remote_image_row()
    stored_snapshot = _snapshot(baseline_remote_obs, [baseline_remote_image])
    baseline_signature = cloud_sync._local_cloud_media_signature(1)

    # This time the remote matches the baseline (no cross-side divergence).
    live_remote_obs = _baseline_remote_obs(notes="baseline note")
    _stub_snapshot_and_signature(
        monkeypatch, stored_snapshot=stored_snapshot, baseline_signature=baseline_signature
    )
    _track_push_calls(monkeypatch)

    client = _StubClient(live_remote_obs, [baseline_remote_image])

    def prepare_images_cb(obs, progress_cb):
        return ([], None, [])

    result = cloud_sync.push_all(
        client,
        remote_obs=[dict(live_remote_obs)],
        sync_images=True,
        sync_calibrations=False,
        prepare_images_cb=prepare_images_cb,
    )

    review_errors = [err for err in (result.get("errors") or []) if "needs review" in str(err)]
    assert review_errors == []

    state = _load_obs_sync_state(db_path)
    assert state["sync_status"] == "synced"
    assert state["sync_blocked_reason"] in (None, "")
    assert state["sync_blocked_at"] in (None, "")
