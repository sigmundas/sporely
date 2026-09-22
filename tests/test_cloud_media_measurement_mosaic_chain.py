"""Integration regression: the repaired media path must converge completely.

Closing the render-signature fast-path hole is only useful if the *whole*
chain converges — cloud image identity, measurement synchronization and the
public spore mosaic — not merely "some bytes were uploaded". These tests run
the production chain end to end for one observation:

    push_all
      → _push_images_for_observation   (anchors, intent, byte upload)
      → _push_measurements_for_observation
      → _push_spore_mosaic_for_observation

Nothing in that chain is stubbed. The only interception point is the boundary
where the mosaic pusher hands its own eligibility-filtered rows to the mosaic
pipeline (``cloud_spore_mosaic.sources_from_measurement_rows``), so the rows
recorded here are the rows production selected — the assertion is about what
the mosaic pusher received, not about a harness's bookkeeping. Cloud state
lives in a small stateful stand-in for PostgREST/R2 so identity (one
``observation_images`` row per local image, no duplicates) is observable.

The second contract pinned here is the metadata anchor. A microscope image the
user excluded from cloud image storage keeps *no* cloud bytes, yet its
metadata-only anchor still carries its public spore measurements into the
cloud and into the mosaic. Byte-selection state and measurement/mosaic
participation are independent: this is what makes it impossible to "fix" a
future stranded-media incident by requiring every measured microscope source
image to upload its bytes.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest
from PIL import Image

from database import models
from utils import cloud_spore_mosaic, cloud_sync


# ---------------------------------------------------------------------------
# Fixture database
# ---------------------------------------------------------------------------


def _init_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "media_measurement_mosaic_chain.sqlite"
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
            publish_target TEXT,
            mosaic_signature TEXT
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


def _write_microscope_source(path: Path) -> Path:
    """A real raster so file signatures, existence checks and mosaic source
    rows behave the way they do in production."""
    Image.new("RGB", (240, 180), (90, 60, 40)).save(path, format="PNG")
    return path


def _seed_public_observation(db_path: Path, field_image_path: Path) -> None:
    """Observation 1: public spore data, already in cloud, one synced field image."""
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


def _add_microscope_image(
    db_path: Path,
    *,
    image_id: int,
    filepath: Path,
    sort_order: int = 1,
    cloud_id: str | None = None,
) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, image_type, sort_order,
                created_at, objective_name, scale_microns_per_pixel,
                resample_scale_factor, crop_mode, source_role
            ) VALUES (?, ?, ?, ?, 'microscope', ?, ?, ?, ?, ?, 'full', 'local_canonical')
            """,
            (
                image_id, 1, cloud_id, str(filepath), sort_order,
                "2026-05-02T00:00:00Z", "100X", 0.25, 1.0,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _add_spore_measurements(db_path: Path, *, image_id: int, ids: list[int]) -> None:
    """Public-eligible spore measurements: both dimensions, manual type, geometry."""
    conn = sqlite3.connect(db_path)
    try:
        for index, measurement_id in enumerate(ids):
            offset = 10.0 * (index + 1)
            conn.execute(
                """
                INSERT INTO spore_measurements (
                    id, image_id, length_um, width_um, measurement_type,
                    gallery_rotation, measured_at,
                    p1_x, p1_y, p2_x, p2_y, p3_x, p3_y, p4_x, p4_y
                ) VALUES (?, ?, ?, ?, 'manual', 0, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    measurement_id, image_id, 9.5 + index, 5.5 + index,
                    "2026-05-03T00:00:00Z",
                    offset, offset, offset + 20.0, offset,
                    offset + 10.0, offset - 8.0, offset + 10.0, offset + 8.0,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _patch_connections(monkeypatch, db_path: Path) -> None:
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))


def _image_cloud_id(db_path: Path, image_id: int) -> str:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT cloud_id FROM images WHERE id = ?", (image_id,)
        ).fetchone()
    finally:
        conn.close()
    return str((row or [None])[0] or "")


def _measurement_cloud_ids(db_path: Path) -> dict[int, str]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT id, cloud_id FROM spore_measurements ORDER BY id"
        ).fetchall()
    finally:
        conn.close()
    return {int(row[0]): str(row[1] or "") for row in rows}


def _observation_status(db_path: Path) -> str:
    conn = sqlite3.connect(db_path)
    try:
        return str(
            conn.execute(
                "SELECT sync_status FROM observations WHERE id = 1"
            ).fetchone()[0]
        )
    finally:
        conn.close()


def _mark_observation_dirty(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("UPDATE observations SET sync_status = 'dirty' WHERE id = 1")
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Stateful cloud stand-in
# ---------------------------------------------------------------------------


_REMOTE_IMAGE_FIELDS = (
    "sort_order", "image_type", "micro_category", "objective_name",
    "scale_microns_per_pixel", "resample_scale_factor", "mount_medium",
    "stain", "sample_type", "contrast", "measure_color", "crop_mode",
    "notes", "gps_source", "ai_crop_x1", "ai_crop_y1", "ai_crop_x2",
    "ai_crop_y2", "ai_crop_source_w", "ai_crop_source_h", "ai_crop_is_custom",
)

_ID_FILTER = re.compile(r"id=eq\.([^&]+)")


def _synced_field_image_row() -> dict:
    return {
        "id": "cloud-image-11",
        "desktop_id": 11,
        "observation_id": "cloud-obs-1",
        "user_id": "user-123",
        "sort_order": 0,
        "image_type": "field",
        "crop_mode": "full",
        "notes": None,
        "storage_path": "user/cloud-obs-1/cloud-image-11.webp",
        "original_filename": "field.jpg",
        "deleted_at": None,
        "calibration_uuid": None,
    }


class _ChainClient:
    """Stateful stand-in for `observation_images` / `spore_measurements` / R2.

    Remote rows are mutated exactly where production mutates them (create,
    metadata push, promotion reservation, byte upload), so cloud *identity*
    is observable: a duplicate upload or a second row for the same local
    image shows up as an extra entry here rather than being absorbed by a
    stateless stub.
    """

    def __init__(self, remote_obs: dict, remote_images: list[dict]):
        self.user_id = "user-123"
        self.remote_obs = dict(remote_obs)
        self.remote_images = [dict(row) for row in remote_images]
        self.remote_measurements: list[dict] = []
        # (local image id, storage path) per byte upload that left the desktop.
        self.uploaded_bytes: list[tuple[int, str]] = []
        # (local measurement id, cloud image id) per measurement push.
        self.pushed_measurements: list[tuple[int, str]] = []
        self.created_image_cloud_ids: list[str] = []
        self.promotion_reservations: list[tuple[str, str]] = []
        self._image_seq = 100
        self._measurement_seq = 500

    # ── observations ──────────────────────────────────────────────────────
    def push_observation(self, obs, remote_obs=None, **kwargs):
        return self.remote_obs["id"]

    def get_observation(self, cloud_id):
        return dict(self.remote_obs)

    def set_desktop_id(self, *args, **kwargs):
        return None

    def _find_cloud_observation(self, desktop_id):
        return self.remote_obs["id"]

    # ── image metadata ────────────────────────────────────────────────────
    def pull_image_metadata(self, obs_cloud_id, include_deleted_for_sync=False):
        return [dict(row) for row in self.remote_images]

    def pull_bulk_image_metadata(self, obs_cloud_ids):
        return [dict(row) for row in self.remote_images]

    def _row_for_cloud_id(self, cloud_id) -> dict | None:
        wanted = str(cloud_id or "").strip()
        if not wanted:
            return None
        for row in self.remote_images:
            if str(row.get("id") or "").strip() == wanted:
                return row
        return None

    def _row_for_desktop_id(self, desktop_id) -> dict | None:
        try:
            wanted = int(desktop_id)
        except (TypeError, ValueError):
            return None
        for row in self.remote_images:
            if int(row.get("desktop_id") or 0) == wanted:
                return row
        return None

    def rows_for_desktop_id(self, desktop_id) -> list[dict]:
        """Every remote row claiming this local image — duplicates included."""
        return [
            dict(row) for row in self.remote_images
            if int(row.get("desktop_id") or 0) == int(desktop_id)
        ]

    def _next_image_cloud_id(self) -> str:
        self._image_seq += 1
        return f"cloud-image-{self._image_seq}"

    def push_image_metadata(self, img, obs_cloud_id, storage_path, *, remote_row=None):
        row = self._row_for_cloud_id(img.get("cloud_id")) or self._row_for_cloud_id(
            (remote_row or {}).get("id")
        )
        if row is None:
            row = self._row_for_desktop_id(img.get("id"))
        if row is None:
            row = {
                "id": self._next_image_cloud_id(),
                "desktop_id": int(img.get("id") or 0),
                "observation_id": str(obs_cloud_id),
                "user_id": self.user_id,
                "deleted_at": None,
                "calibration_uuid": None,
            }
            self.remote_images.append(row)
            self.created_image_cloud_ids.append(str(row["id"]))
        for field in _REMOTE_IMAGE_FIELDS:
            if field in img:
                row[field] = img.get(field)
        row["original_filename"] = (
            str(img.get("original_filename") or "").strip()
            or Path(str(img.get("filepath") or "")).name
            or None
        )
        if str(storage_path or "").strip():
            row["storage_path"] = str(storage_path).strip()
        return str(row["id"])

    def set_image_desktop_id(self, cloud_image_id, desktop_id):
        row = self._row_for_cloud_id(cloud_image_id)
        if row is not None:
            row["desktop_id"] = int(desktop_id)

    def reserve_image_storage_path_for_promotion(self, cloud_image_id, storage_path):
        row = self._row_for_cloud_id(cloud_image_id)
        if row is None or str(row.get("storage_path") or "").strip():
            return False
        row["storage_path"] = str(storage_path)
        self.promotion_reservations.append((str(cloud_image_id), str(storage_path)))
        return True

    def release_image_storage_path_reservation(self, cloud_image_id, storage_path):
        row = self._row_for_cloud_id(cloud_image_id)
        if row is not None and str(row.get("storage_path") or "") == str(storage_path):
            row["storage_path"] = None
            return True
        return False

    def _find_cloud_image(self, desktop_id, obs_cloud_id=None, image_type=None):
        row = self._row_for_desktop_id(desktop_id)
        if row is None:
            return None
        return {"id": str(row.get("id") or ""), "deleted_at": row.get("deleted_at")}

    # ── bytes ─────────────────────────────────────────────────────────────
    def upload_image_file(
        self, local_path, obs_cloud_id=None, img_cloud_id=None, *,
        storage_path=None, upload_meta=None, observation_id=0, image_id=0,
        **kwargs,
    ):
        key = str(storage_path or "")
        self.uploaded_bytes.append((int(image_id or 0), key))
        row = self._row_for_cloud_id(img_cloud_id)
        if row is not None:
            row["storage_path"] = key
        return key

    def upload_original_image_file(self, *args, **kwargs):
        raise AssertionError("original upload is disabled in this harness")

    def set_image_original_storage_path(self, *args, **kwargs):
        return None

    def _build_original_storage_path(self, *args, **kwargs):
        return ""

    def _storage_remove(self, keys):
        return None

    def _using_default_r2_loader(self):
        return False

    # ── measurements ──────────────────────────────────────────────────────
    def pull_measurements_for_images(self, image_cloud_ids):
        wanted = {str(value or "").strip() for value in (image_cloud_ids or [])}
        return [
            dict(row) for row in self.remote_measurements
            if str(row.get("image_id") or "").strip() in wanted
        ]

    def push_measurement(self, meas, cloud_image_id, remote_measurement_cache=None):
        local_id = int(meas.get("id") or 0)
        row = next(
            (
                candidate for candidate in self.remote_measurements
                if int(candidate.get("desktop_id") or 0) == local_id
            ),
            None,
        )
        if row is None:
            self._measurement_seq += 1
            row = {
                "id": f"cloud-meas-{self._measurement_seq}",
                "desktop_id": local_id,
                "user_id": self.user_id,
            }
            self.remote_measurements.append(row)
        row["image_id"] = str(cloud_image_id)
        row["length_um"] = meas.get("length_um")
        row["width_um"] = meas.get("width_um")
        self.pushed_measurements.append((local_id, str(cloud_image_id)))
        return str(row["id"])

    def set_measurement_desktop_id(self, cloud_measurement_id, desktop_id):
        return None

    # ── capability flags ──────────────────────────────────────────────────
    def _observation_images_support_ai_crop(self):
        return True

    def _observation_images_support_ai_crop_custom(self):
        return True

    def _observation_images_support_upload_metadata(self):
        return False

    def _observation_images_support_original_storage_path(self):
        return False

    # ── raw PostgREST surface ─────────────────────────────────────────────
    def _get(self, path):
        text = str(path or "")
        if text.startswith("spore_measurements?"):
            return [dict(row) for row in self.remote_measurements]
        return []

    def _post(self, path, payload):
        if str(path or "") == "observation_images":
            row = {**dict(payload or {}), "id": self._next_image_cloud_id()}
            row.setdefault("deleted_at", None)
            self.remote_images.append(row)
            self.created_image_cloud_ids.append(str(row["id"]))
            return [{"id": row["id"]}]
        return [{"id": 1}]

    def _patch(self, path, payload):
        match = _ID_FILTER.search(str(path or ""))
        if match:
            row = self._row_for_cloud_id(match.group(1))
            if row is not None:
                row.update(dict(payload or {}))
        return None

    def _delete(self, path):
        match = _ID_FILTER.search(str(path or ""))
        if match:
            row = self._row_for_cloud_id(match.group(1))
            if row is not None:
                self.remote_images.remove(row)
        return None


# ---------------------------------------------------------------------------
# Harness: production chain, recorded only at the mosaic pipeline boundary
# ---------------------------------------------------------------------------


class _ChainHarness:
    def __init__(self, monkeypatch, db_path: Path, tmp_path: Path):
        self.db_path = db_path
        self.tmp_path = tmp_path
        # Which observation reached full image preparation (True) at all.
        self.image_prep_calls: list[int] = []
        # Rows the mosaic pusher's own eligibility query handed to the
        # mosaic pipeline, per invocation.
        self.mosaic_source_rows: list[list[dict]] = []
        remote_obs = {"id": "cloud-obs-1", "desktop_id": 1, "date": "2026-05-01"}
        self.remote_obs = remote_obs
        self.client = _ChainClient(remote_obs, [_synced_field_image_row()])

        # Stored signature == current signature: unchanged local render
        # inputs, which is the exact state the image-prep fast paths key on.
        baseline_signature = cloud_sync._local_cloud_media_signature(1)
        # Real remote snapshot bookkeeping (stored in `settings`), seeded with
        # the pre-sync cloud state. Keeping the production store/load in the
        # loop is what lets a second sync see a consistent baseline instead of
        # reporting a spurious both-sides measurement conflict.
        cloud_sync._store_remote_snapshot(
            self.client, "cloud-obs-1", remote_obs, [_synced_field_image_row()], [],
        )

        monkeypatch.setattr(cloud_sync, "get_images_dir", lambda: tmp_path)
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
            cloud_sync,
            "_load_local_cloud_media_signature",
            lambda observation_id: baseline_signature,
        )
        monkeypatch.setattr(
            cloud_sync,
            "_refresh_local_cloud_media_signature",
            lambda observation_id: baseline_signature,
        )
        monkeypatch.setattr(
            cloud_sync, "_push_pending_image_tombstones", lambda client: []
        )

        real_push_images = cloud_sync._push_images_for_observation

        def recording_push_images(client_arg, obs, cloud_id, **kwargs):
            if kwargs.get("prepare_images_cb") is not None:
                self.image_prep_calls.append(int(obs["id"]))
            return real_push_images(client_arg, obs, cloud_id, **kwargs)

        monkeypatch.setattr(
            cloud_sync, "_push_images_for_observation", recording_push_images
        )

        def recording_sources(rows, image_dir=None, **kwargs):
            self.mosaic_source_rows.append([dict(row) for row in rows])
            # Stop before Pillow/WebP/R2: the claim under test is which
            # measurements reached this boundary.
            return [], []

        monkeypatch.setattr(
            cloud_spore_mosaic, "sources_from_measurement_rows", recording_sources
        )

    def _prepare_images_cb(self, obs, progress_cb):
        """Stand in for the desktop's WebP preparation step.

        Mirrors production semantics: images the sync told us to skip are not
        prepared, and only images whose cloud bytes are desired produce an
        upload candidate.
        """
        observation_id = int(obs["id"])
        skip_ids = {
            int(value)
            for value in (obs.get(cloud_sync.CLOUD_SYNC_SKIP_PREPARE_IMAGE_IDS_KEY) or [])
        }
        prepared: list[dict] = []
        for image_row in models.ImageDB.get_images_for_observation(observation_id):
            image_id = int(image_row.get("id") or 0)
            if image_id <= 0 or image_id in skip_ids:
                continue
            if not cloud_sync.cloud_image_bytes_desired(
                observation_id, image_id, image_row
            ):
                continue
            upload_path = self.tmp_path / f"prepared-{image_id}.webp"
            upload_path.write_bytes(b"prepared-webp-bytes-%d" % image_id)
            prepared.append({"image_row": dict(image_row), "upload_path": str(upload_path)})
        return (prepared, None, [])

    def run(self, *, sync_images: bool = True) -> dict:
        return cloud_sync.push_all(
            self.client,
            remote_obs=[dict(self.remote_obs)],
            sync_images=sync_images,
            sync_calibrations=False,
            prepare_images_cb=self._prepare_images_cb,
        )

    @property
    def took_image_prep_fast_path(self) -> bool:
        return self.image_prep_calls == []

    def mosaic_measurement_ids(self, invocation: int = 0) -> list[int]:
        return [int(row["id"]) for row in self.mosaic_source_rows[invocation]]


@pytest.fixture
def db(tmp_path, monkeypatch):
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    field_image = tmp_path / "field.jpg"
    Image.new("RGB", (120, 90), (30, 90, 30)).save(field_image, format="JPEG")
    _seed_public_observation(db_path, field_image)
    return db_path, tmp_path


# ---------------------------------------------------------------------------
# The 917-shaped observation converges all the way to the mosaic
# ---------------------------------------------------------------------------


def test_stranded_selected_microscope_media_converges_to_mosaic(db, monkeypatch):
    """Full chain for the reported shape.

    Existing cloud observation, matching local render signature, a *desired*
    microscope image with ``cloud_id IS NULL``, eligible local spore
    measurements, explicit media sync. The run must reach the intended cloud
    image identity (exactly one remote row, byte-backed), make measurement
    synchronization eligible, and hand those cloud-linked measurements to the
    mosaic pusher.
    """
    db_path, tmp_path = db
    micro = _write_microscope_source(tmp_path / "micro-12.png")
    _add_microscope_image(db_path, image_id=12, filepath=micro)
    _add_spore_measurements(db_path, image_id=12, ids=[301, 302, 303])
    # Intent recorded for both images; neither excluded → image 12 is desired.
    cloud_sync._set_cloud_image_storage_intent_initialized_ids(1, {11, 12})

    # Pre-state: the stranded shape. The image is pending upload, and because
    # its `cloud_id` is NULL its measurements cannot reach cloud at all.
    assert cloud_sync._pending_cloud_pushable_image_ids(1) == [12]
    assert _image_cloud_id(db_path, 12) == ""
    assert set(_measurement_cloud_ids(db_path).values()) == {""}

    harness = _ChainHarness(monkeypatch, db_path, tmp_path)
    result = harness.run()

    assert result["errors"] == []
    # 1. Full image preparation ran instead of the render-signature fast path.
    assert harness.image_prep_calls == [1]

    # 2. Intended cloud image identity: one remote row for local image 12,
    #    byte-backed, and the local row points at exactly that row.
    image_cloud_id = _image_cloud_id(db_path, 12)
    assert image_cloud_id
    remote_rows = harness.client.rows_for_desktop_id(12)
    assert len(remote_rows) == 1, remote_rows
    assert str(remote_rows[0]["id"]) == image_cloud_id
    storage_path = str(remote_rows[0].get("storage_path") or "")
    assert storage_path
    # The bytes were sent once, for that image, under that key.
    assert harness.client.uploaded_bytes == [(12, storage_path)]

    # 3. Measurement synchronization became eligible and was carried out
    #    against the image's cloud identity.
    assert harness.client.pushed_measurements == [
        (301, image_cloud_id), (302, image_cloud_id), (303, image_cloud_id),
    ]
    assert _measurement_cloud_ids(db_path) == {
        301: "cloud-meas-501", 302: "cloud-meas-502", 303: "cloud-meas-503",
    }

    # 4. The mosaic pusher received those eligible, cloud-linked measurements.
    assert len(harness.mosaic_source_rows) == 1
    assert harness.mosaic_measurement_ids() == [301, 302, 303]
    for row in harness.mosaic_source_rows[0]:
        assert str(row["image_cloud_id"]) == image_cloud_id
        assert str(row["cloud_id"]).startswith("cloud-meas-")

    # 5. Convergence, not a loop: nothing is pending any more, and a second
    #    explicit sync re-takes the fast path without re-uploading bytes or
    #    creating a second cloud image row.
    assert cloud_sync._pending_cloud_pushable_image_ids(1) == []
    assert _observation_status(db_path) == "synced"

    _mark_observation_dirty(db_path)
    second = harness.run()

    assert second["errors"] == []
    assert harness.image_prep_calls == [1]
    assert harness.client.uploaded_bytes == [(12, storage_path)]
    assert len(harness.client.rows_for_desktop_id(12)) == 1
    assert _observation_status(db_path) == "synced"


# ---------------------------------------------------------------------------
# The metadata-anchor contract
# ---------------------------------------------------------------------------


def test_unselected_microscope_image_syncs_measurements_without_any_bytes(
    db, monkeypatch
):
    """A microscope image excluded from cloud image storage keeps no bytes.

    Its metadata-only anchor is still enough to carry its public spore
    measurements to cloud and into the mosaic. This is the contract that makes
    "upload every measured microscope source image" the wrong repair for a
    stranded-media incident.
    """
    db_path, tmp_path = db
    micro = _write_microscope_source(tmp_path / "micro-12.png")
    _add_microscope_image(db_path, image_id=12, filepath=micro)
    _add_spore_measurements(db_path, image_id=12, ids=[301, 302])
    cloud_sync._set_cloud_image_storage_intent_initialized_ids(1, {11, 12})
    # The user unchecked "Keep image in Sporely Cloud" for the microscope frame.
    cloud_sync._set_cloud_image_storage_excluded_image_ids(1, {12})

    # Byte work is complete by definition: nothing is pending for an image
    # whose bytes were never wanted.
    assert cloud_sync._pending_cloud_pushable_image_ids(1) == []
    assert not cloud_sync.cloud_image_bytes_desired(1, 12)

    harness = _ChainHarness(monkeypatch, db_path, tmp_path)
    result = harness.run()

    assert result["errors"] == []
    # No byte upload anywhere — the no-op image-prep fast path is preserved.
    assert harness.client.uploaded_bytes == []

    # A metadata-only anchor exists: cloud identity without cloud bytes.
    image_cloud_id = _image_cloud_id(db_path, 12)
    assert image_cloud_id
    remote_rows = harness.client.rows_for_desktop_id(12)
    assert len(remote_rows) == 1, remote_rows
    assert str(remote_rows[0]["id"]) == image_cloud_id
    assert remote_rows[0].get("storage_path") in (None, "")
    assert str(remote_rows[0].get("image_type")) == "microscope"

    # Measurement synchronization used that anchor…
    assert harness.client.pushed_measurements == [
        (301, image_cloud_id), (302, image_cloud_id),
    ]
    # …and the mosaic pusher received the cloud-linked measurements.
    assert harness.mosaic_measurement_ids() == [301, 302]
    for row in harness.mosaic_source_rows[0]:
        assert str(row["image_cloud_id"]) == image_cloud_id

    # Still no bytes after the anchor was established, and no dirty loop.
    assert cloud_sync._pending_cloud_pushable_image_ids(1) == []
    assert _observation_status(db_path) == "synced"


def test_byte_selection_and_mosaic_participation_are_independent(db, monkeypatch):
    """One selected and one excluded microscope image, same observation.

    Byte-selection state decides only whether cloud image bytes exist. Both
    images take their public spore measurements to cloud and into the mosaic,
    so measurement/mosaic participation cannot be made to depend on byte
    selection.
    """
    db_path, tmp_path = db
    selected = _write_microscope_source(tmp_path / "micro-12.png")
    excluded = _write_microscope_source(tmp_path / "micro-13.png")
    _add_microscope_image(db_path, image_id=12, filepath=selected, sort_order=1)
    _add_microscope_image(db_path, image_id=13, filepath=excluded, sort_order=2)
    _add_spore_measurements(db_path, image_id=12, ids=[301, 302])
    _add_spore_measurements(db_path, image_id=13, ids=[311, 312])
    cloud_sync._set_cloud_image_storage_intent_initialized_ids(1, {11, 12, 13})
    cloud_sync._set_cloud_image_storage_excluded_image_ids(1, {13})

    # Only the selected image is pending upload.
    assert cloud_sync._pending_cloud_pushable_image_ids(1) == [12]

    harness = _ChainHarness(monkeypatch, db_path, tmp_path)
    result = harness.run()

    assert result["errors"] == []
    selected_cloud_id = _image_cloud_id(db_path, 12)
    excluded_cloud_id = _image_cloud_id(db_path, 13)
    assert selected_cloud_id and excluded_cloud_id
    assert selected_cloud_id != excluded_cloud_id

    # Bytes: exactly one upload, for the selected image only.
    assert [image_id for image_id, _key in harness.client.uploaded_bytes] == [12]
    selected_row = harness.client.rows_for_desktop_id(12)[0]
    excluded_row = harness.client.rows_for_desktop_id(13)[0]
    assert str(selected_row.get("storage_path") or "")
    assert excluded_row.get("storage_path") in (None, "")

    # Measurements: both images participate, against their own cloud identity.
    assert sorted(harness.client.pushed_measurements) == sorted([
        (301, selected_cloud_id), (302, selected_cloud_id),
        (311, excluded_cloud_id), (312, excluded_cloud_id),
    ])

    # Mosaic: the pusher received every eligible measurement from both the
    # byte-backed image and the metadata-only anchor.
    assert harness.mosaic_measurement_ids() == [301, 302, 311, 312]
    mosaic_image_cloud_ids = {
        str(row["image_cloud_id"]) for row in harness.mosaic_source_rows[0]
    }
    assert mosaic_image_cloud_ids == {selected_cloud_id, excluded_cloud_id}

    # The excluded image is not re-dirtied for bytes it never wanted.
    assert cloud_sync._pending_cloud_pushable_image_ids(1) == []
    assert _observation_status(db_path) == "synced"
