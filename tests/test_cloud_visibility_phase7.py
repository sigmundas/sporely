import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from database import models, schema
from utils import cloud_sync


def _init_tombstone_sync_db(tmp_path):
    db_path = tmp_path / "sporely.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cloud_id TEXT,
            sync_status TEXT,
            synced_at TEXT,
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
            synced_at TEXT
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
            gallery_rotation INTEGER
        );
        CREATE TABLE settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """
    )
    schema._ensure_image_tombstones_table(conn.cursor())
    conn.commit()
    conn.close()
    return db_path


def _init_push_all_sync_db(tmp_path):
    db_path = _init_tombstone_sync_db(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("ALTER TABLE observations ADD COLUMN date TEXT")
        conn.execute("ALTER TABLE observations ADD COLUMN user_id TEXT")
        conn.execute("ALTER TABLE observations ADD COLUMN sync_error_code TEXT")
        conn.execute("ALTER TABLE observations ADD COLUMN sync_error_message TEXT")
        conn.execute("ALTER TABLE observations ADD COLUMN sync_blocked_reason TEXT")
        conn.execute("ALTER TABLE observations ADD COLUMN sync_blocked_at TEXT")
        conn.execute("ALTER TABLE images ADD COLUMN source_role TEXT")
        conn.execute("ALTER TABLE images ADD COLUMN file_purpose TEXT")
        conn.execute("ALTER TABLE spore_measurements ADD COLUMN cloud_id TEXT")
        conn.execute("ALTER TABLE spore_measurements ADD COLUMN measured_at TEXT")
        conn.commit()
    finally:
        conn.close()
    return db_path


class _PushAllImageClient(cloud_sync.SporelyCloudClient):
    def __init__(
        self,
        *,
        fail_derivative_upload: bool = False,
        fail_original_upload: bool = False,
        remote_images: list[dict] | None = None,
        remote_measurements: list[dict] | None = None,
    ):
        super().__init__("token", "user-123")
        self.fail_derivative_upload = bool(fail_derivative_upload)
        self.fail_original_upload = bool(fail_original_upload)
        self.remote_images = [dict(row or {}) for row in (remote_images or [])]
        self.remote_measurements = [dict(row or {}) for row in (remote_measurements or [])]
        self.upload_image_calls: list[dict] = []
        self.upload_original_calls: list[dict] = []
        self.push_metadata_calls: list[dict] = []
        self.original_patch_calls: list[dict] = []
        self.soft_delete_calls: list[tuple[str, str]] = []
        self.measurement_push_calls: list[tuple[int, str]] = []
        self.delete_calls: list[str] = []
        self.storage_remove_calls: list[list[str]] = []

    def push_observation(self, obs, remote_obs=None, **kwargs):
        return "cloud-obs-1"

    def pull_image_metadata(self, obs_cloud_id: str, include_deleted_for_sync: bool = False) -> list[dict]:
        cloud_value = str(obs_cloud_id or "").strip()
        rows = [
            dict(row)
            for row in self.remote_images
            if str(row.get("observation_id") or "").strip() == cloud_value
        ]
        if include_deleted_for_sync:
            return rows
        return [row for row in rows if not str(row.get("deleted_at") or "").strip()]

    def pull_measurements_for_images(self, image_cloud_ids: list[str]) -> list[dict]:
        image_ids = {
            str(image_id or "").strip()
            for image_id in (image_cloud_ids or [])
            if str(image_id or "").strip()
        }
        return [
            dict(row)
            for row in self.remote_measurements
            if str(row.get("image_id") or "").strip() in image_ids
        ]

    def _observation_images_support_ai_crop(self) -> bool:
        return False

    def _observation_images_support_ai_crop_custom(self) -> bool:
        return False

    def _observation_images_support_upload_metadata(self) -> bool:
        return False

    def _observation_images_support_original_storage_path(self) -> bool:
        return True

    def upload_image_file(
        self,
        local_path: str,
        obs_cloud_id: str,
        img_cloud_id: str,
        storage_path: str | None = None,
        upload_meta: dict | None = None,
    ) -> str | None:
        self.upload_image_calls.append(
            {
                "local_path": str(local_path),
                "obs_cloud_id": str(obs_cloud_id),
                "img_cloud_id": str(img_cloud_id),
                "storage_path": str(storage_path or ""),
                "upload_meta": dict(upload_meta or {}),
            }
        )
        if self.fail_derivative_upload:
            raise cloud_sync.CloudSyncError(cloud_sync.IMAGE_TOO_LARGE_FOR_PLAN_MESSAGE)
        return storage_path or self._build_storage_path(obs_cloud_id, img_cloud_id, local_path)

    def push_image_metadata(self, img: dict, obs_cloud_id: str, storage_path: str) -> str:
        cloud_id = str(img.get("cloud_id") or "").strip() or f"cloud-image-{len(self.push_metadata_calls) + 1}"
        self.push_metadata_calls.append(
            {
                "cloud_id": cloud_id,
                "storage_path": cloud_sync.normalize_media_key(storage_path),
                "desktop_id": img.get("id"),
            }
        )
        return cloud_id

    def upload_original_image_file(
        self,
        local_path: str,
        obs_cloud_id: str,
        img_cloud_id: str,
        storage_path: str | None = None,
        upload_meta: dict | None = None,
    ) -> str | None:
        self.upload_original_calls.append(
            {
                "local_path": str(local_path),
                "obs_cloud_id": str(obs_cloud_id),
                "img_cloud_id": str(img_cloud_id),
                "storage_path": str(storage_path or ""),
                "upload_meta": dict(upload_meta or {}),
            }
        )
        if self.fail_original_upload:
            raise cloud_sync.CloudSyncError("Original media upload failed: worker refused")
        return storage_path or self._build_original_storage_path(obs_cloud_id, img_cloud_id, local_path)

    def set_image_original_storage_path(self, cloud_image_id: str, original_storage_path: str) -> None:
        self.original_patch_calls.append(
            {
                "cloud_image_id": str(cloud_image_id),
                "original_storage_path": cloud_sync.normalize_media_key(original_storage_path),
            }
        )

    def soft_delete_image(self, cloud_image_id: str, deleted_at: str | None) -> None:
        deleted_cloud_id = str(cloud_image_id or "").strip()
        deleted_at_text = str(deleted_at or "").strip()
        self.soft_delete_calls.append((deleted_cloud_id, deleted_at_text))
        for row in self.remote_images:
            if str(row.get("id") or "").strip() == deleted_cloud_id:
                row["deleted_at"] = deleted_at_text
                break

    def push_measurement(self, meas: dict, cloud_image_id: str, remote_measurement_cache=None) -> str:
        self.measurement_push_calls.append((int(meas["id"]), str(cloud_image_id)))
        return f"cloud-measurement-{int(meas['id'])}"

    def _delete(self, path: str) -> None:
        self.delete_calls.append(str(path))

    def _storage_remove(self, paths: list[str]) -> None:
        self.storage_remove_calls.append([str(path) for path in paths])


def _seed_existing_cloud_media_observation(db_path, image_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO observations (
                id, cloud_id, sync_status, synced_at, sync_error_code,
                sync_error_message, sync_blocked_reason, sync_blocked_at, user_id, date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "cloud-obs-1",
                "dirty",
                "2026-05-01T00:00:00Z",
                None,
                None,
                None,
                None,
                "user-1",
                "2026-05-02",
            ),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath,
                image_type, source_role, file_purpose, sort_order, created_at,
                synced_at, notes, crop_mode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                11,
                1,
                "cloud-image-11",
                str(image_path),
                None,
                "field",
                "local_canonical",
                "field",
                0,
                "2026-05-01T10:00:00Z",
                "2026-05-01T10:05:00Z",
                "baseline note",
                "full",
            ),
        )
        conn.execute(
            """
            INSERT INTO spore_measurements (
                id, image_id, length_um, width_um, measurement_type, notes,
                p1_x, p1_y, p2_x, p2_y, p3_x, p3_y, p4_x, p4_y, gallery_rotation
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                21,
                11,
                12.3,
                4.5,
                "spore",
                "baseline measurement",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                0,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _setup_push_all_existing_media_case(tmp_path, monkeypatch):
    db_path = _init_push_all_sync_db(tmp_path)
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"image-bytes")
    _seed_existing_cloud_media_observation(db_path, image_path)

    remote_obs = {
        "id": "cloud-obs-1",
        "desktop_id": 1,
        "date": "2026-05-02",
    }
    remote_images = [
        {
            "id": "cloud-image-11",
            "desktop_id": 11,
            "sort_order": 0,
            "image_type": "field",
            "crop_mode": "full",
            "notes": "baseline note",
            "storage_path": "user/cloud-obs-1/cloud-image-11.webp",
            "original_filename": "image.jpg",
        }
    ]
    remote_measurements = [
        {
            "id": 21,
            "desktop_id": 21,
            "image_id": 11,
            "length_um": 12.3,
            "width_um": 4.5,
            "measurement_type": "spore",
            "gallery_rotation": 0,
            "p1_x": None,
            "p1_y": None,
            "p2_x": None,
            "p2_y": None,
            "p3_x": None,
            "p3_y": None,
            "p4_x": None,
            "p4_y": None,
            "measured_at": None,
        }
    ]
    stored_snapshot = cloud_sync._cloud_observation_snapshot(remote_obs, remote_images, remote_measurements)

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    stored_signature = cloud_sync._local_cloud_media_signature(1)
    progress_messages: list[str] = []
    refresh_calls: list[int] = []

    class DummyClient:
        def push_observation(self, obs, remote_obs=None, **kwargs):
            return "cloud-obs-1"

        def pull_image_metadata(self, obs_cloud_id, include_deleted_for_sync=False):
            return [dict(row) for row in remote_images]

        def pull_measurements_for_images(self, image_cloud_ids):
            return [dict(row) for row in remote_measurements]

    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_pending_local_images", lambda: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: stored_snapshot)
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda observation_id: stored_signature)
    monkeypatch.setattr(
        cloud_sync,
        "_refresh_local_cloud_media_signature",
        lambda observation_id: refresh_calls.append(int(observation_id)) or stored_signature,
    )

    return SimpleNamespace(
        db_path=db_path,
        image_path=image_path,
        remote_obs=remote_obs,
        remote_images=remote_images,
        remote_measurements=remote_measurements,
        stored_signature=stored_signature,
        stored_snapshot=stored_snapshot,
        progress_messages=progress_messages,
        refresh_calls=refresh_calls,
        client=DummyClient(),
    )


def _setup_push_all_tombstone_cleanup_case(
    tmp_path,
    monkeypatch,
    *,
    include_deleted_measurement: bool,
):
    db_path = _init_push_all_sync_db(tmp_path)
    tombstoned_image_path = tmp_path / "tombstoned-image.jpg"
    surviving_image_path = tmp_path / "surviving-image.jpg"
    tombstoned_image_path.write_bytes(b"tombstoned-image-bytes")
    surviving_image_path.write_bytes(b"surviving-image-bytes")

    tombstoned_created_at = "2026-05-01T10:00:00Z"
    surviving_created_at = "2026-05-01T10:05:00Z"
    tombstoned_image_type = "microscope" if include_deleted_measurement else "field"

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO observations (
                id, cloud_id, sync_status, synced_at, sync_error_code,
                sync_error_message, sync_blocked_reason, sync_blocked_at, user_id, date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "cloud-obs-1", "dirty", "2026-05-01T00:00:00Z", None, None, None, None, "user-1", "2026-05-02"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath,
                image_type, source_role, file_purpose, sort_order, created_at,
                synced_at, notes, crop_mode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                11,
                1,
                "cloud-image-11",
                str(tombstoned_image_path),
                None,
                tombstoned_image_type,
                "local_canonical",
                "field",
                0,
                tombstoned_created_at,
                tombstoned_created_at,
                "tombstoned note",
                "full",
            ),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath,
                image_type, source_role, file_purpose, sort_order, created_at,
                synced_at, notes, crop_mode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                12,
                1,
                "cloud-image-12",
                str(surviving_image_path),
                None,
                "field",
                "local_canonical",
                "field",
                1,
                surviving_created_at,
                surviving_created_at,
                "surviving note",
                "full",
            ),
        )
        if include_deleted_measurement:
            conn.execute(
                """
                INSERT INTO spore_measurements (
                    id, image_id, length_um, width_um, measurement_type, notes,
                    p1_x, p1_y, p2_x, p2_y, p3_x, p3_y, p4_x, p4_y, gallery_rotation
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    21,
                    11,
                    12.3,
                    4.5,
                    "spore",
                    "deleted-image measurement",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    0,
                ),
            )
        conn.commit()
    finally:
        conn.close()

    client = _PushAllImageClient()
    tombstoned_local_image = {
        "id": 11,
        "cloud_id": "cloud-image-11",
        "filepath": str(tombstoned_image_path),
        "image_type": tombstoned_image_type,
        "sort_order": 0,
        "created_at": tombstoned_created_at,
        "synced_at": tombstoned_created_at,
        "notes": "tombstoned note",
        "crop_mode": "full",
    }
    surviving_local_image = {
        "id": 12,
        "cloud_id": "cloud-image-12",
        "filepath": str(surviving_image_path),
        "image_type": "field",
        "sort_order": 1,
        "created_at": surviving_created_at,
        "synced_at": surviving_created_at,
        "notes": "surviving note",
        "crop_mode": "full",
    }
    remote_images = []
    for local_image in (tombstoned_local_image, surviving_local_image):
        storage_path = cloud_sync._build_worker_storage_path(
            client.user_id,
            "cloud-obs-1",
            local_image,
            str(local_image["filepath"]),
        )
        original_storage_path = client._build_original_storage_path(
            "cloud-obs-1",
            str(local_image["cloud_id"]),
            str(local_image["filepath"]),
        )
        remote_images.append(
            {
                "id": str(local_image["cloud_id"]),
                "observation_id": "cloud-obs-1",
                "desktop_id": int(local_image["id"]),
                "sort_order": int(local_image["sort_order"]),
                "image_type": str(local_image["image_type"]),
                "crop_mode": str(local_image["crop_mode"]),
                "notes": str(local_image["notes"]),
                "storage_path": storage_path,
                "original_storage_path": original_storage_path,
                "original_filename": Path(str(local_image["filepath"])).name,
                "deleted_at": "",
            }
        )

    remote_measurements = []
    if include_deleted_measurement:
        remote_measurements.append(
            {
                "id": "cloud-measurement-21",
                "desktop_id": 21,
                "image_id": "cloud-image-11",
                "length_um": 12.3,
                "width_um": 4.5,
                "measurement_type": "spore",
                "gallery_rotation": 0,
                "p1_x": None,
                "p1_y": None,
                "p2_x": None,
                "p2_y": None,
                "p3_x": None,
                "p3_y": None,
                "p4_x": None,
                "p4_y": None,
                "measured_at": None,
            }
        )

    client.remote_images = [dict(row) for row in remote_images]
    client.remote_measurements = [dict(row) for row in remote_measurements]

    remote_obs = {
        "id": "cloud-obs-1",
        "desktop_id": 1,
        "date": "2026-05-02",
    }
    stored_snapshot = cloud_sync._cloud_observation_snapshot(remote_obs, remote_images, remote_measurements)

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    stored_signature = cloud_sync._local_cloud_media_signature(1)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE images SET notes = ? WHERE id = ?",
            ("tombstoned note (cleanup)", 11),
        )
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?)
            """,
            ("cloud-image-11", "2026-05-01 10:10:00", 1, 11),
        )
        conn.commit()
    finally:
        conn.close()

    progress_messages: list[str] = []
    refresh_calls: list[int] = []

    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_pending_local_images", lambda: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: stored_snapshot)
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda observation_id: stored_signature)
    monkeypatch.setattr(
        cloud_sync,
        "_refresh_local_cloud_media_signature",
        lambda observation_id: refresh_calls.append(int(observation_id)) or stored_signature,
    )

    return SimpleNamespace(
        db_path=db_path,
        client=client,
        remote_obs=remote_obs,
        remote_images=remote_images,
        remote_measurements=remote_measurements,
        stored_signature=stored_signature,
        stored_snapshot=stored_snapshot,
        progress_messages=progress_messages,
        refresh_calls=refresh_calls,
    )


def test_phase7_visibility_normalization_maps_cloud_draft_to_local_private():
    assert cloud_sync._normalize_sharing_scope("draft") == "private"
    assert cloud_sync._cloud_visibility_to_sharing_scope("draft") == "private"
    assert cloud_sync._cloud_visibility_to_sharing_scope("friends") == "friends"
    assert cloud_sync._cloud_visibility_to_sharing_scope("public") == "public"


def test_phase7_visibility_normalization_sends_private_visibility_to_cloud():
    assert cloud_sync._sharing_scope_to_cloud_visibility("private") == "private"
    assert cloud_sync._sharing_scope_to_cloud_visibility("draft") == "private"
    assert cloud_sync._sharing_scope_to_cloud_visibility("friends") == "friends"
    assert cloud_sync._sharing_scope_to_cloud_visibility("public") == "public"


def test_push_observation_sends_private_visibility_for_local_private(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    posted_payloads = []

    monkeypatch.setattr(client, "_find_cloud_observation", lambda desktop_id: None)

    def fake_post(path, payload):
        posted_payloads.append((path, dict(payload)))
        return [{"id": "cloud-obs-1"}]

    monkeypatch.setattr(client, "_post", fake_post)

    cloud_id = client.push_observation(
        {
            "id": 7,
            "sharing_scope": "private",
            "location_public": 0,
            "is_draft": True,
            "location_precision": "fuzzed",
            "uncertain": 0,
            "unspontaneous": 0,
            "interesting_comment": 0,
        }
    )

    assert cloud_id == "cloud-obs-1"
    assert posted_payloads[0][0] == "observations"
    assert posted_payloads[0][1]["visibility"] == "private"
    assert posted_payloads[0][1]["is_draft"] is True
    assert posted_payloads[0][1]["location_precision"] == "fuzzed"
    assert posted_payloads[0][1]["user_id"] == "user-123"
    assert posted_payloads[0][1]["desktop_id"] == 7


def test_push_observation_preserves_public_visibility(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    patched_payloads = []

    monkeypatch.setattr(client, "_find_cloud_observation", lambda desktop_id: "cloud-obs-2")

    def fake_patch(path, payload):
        patched_payloads.append((path, dict(payload)))

    monkeypatch.setattr(client, "_patch", fake_patch)

    cloud_id = client.push_observation(
        {
            "id": 8,
            "sharing_scope": "public",
            "location_public": 1,
            "is_draft": False,
            "location_precision": "exact",
            "uncertain": 1,
            "unspontaneous": 0,
            "interesting_comment": 0,
        }
    )

    assert cloud_id == "cloud-obs-2"
    assert patched_payloads[0][0] == "observations?id=eq.cloud-obs-2"
    assert patched_payloads[0][1]["visibility"] == "public"
    assert patched_payloads[0][1]["location_public"] is True
    assert patched_payloads[0][1]["is_draft"] is False
    assert patched_payloads[0][1]["location_precision"] == "exact"


def test_observation_push_diff_fields_normalize_bool_float_json_and_visibility():
    local_obs = {
        "id": 7,
        "date": "2026-05-01T12:34:56Z",
        "genus": "Agaricus",
        "species": "campestris",
        "species_guess": "Agaricus campestris",
        "sharing_scope": "public",
        "location_public": "1",
        "is_draft": "0",
        "uncertain": "0",
        "unspontaneous": "1",
        "interesting_comment": "0",
        "location_precision": "Exact",
        "spore_data_visibility": "Public",
        "gps_latitude": "63.0000000001",
        "gps_longitude": "10.0",
        "ai_selected_probability": "0.9700000001",
        "spore_statistics": '{"b": 2, "a": 1}',
        "auto_threshold": "0.25",
    }
    remote_obs = {
        "id": "cloud-obs-1",
        "desktop_id": 7,
        "date": "2026-05-01",
        "genus": "Agaricus",
        "species": "campestris",
        "species_guess": "Agaricus campestris",
        "visibility": "public",
        "location_public": True,
        "is_draft": False,
        "uncertain": False,
        "unspontaneous": True,
        "interesting_comment": False,
        "location_precision": "exact",
        "spore_data_visibility": "public",
        "gps_latitude": 63.0,
        "gps_longitude": 10.0,
        "ai_selected_probability": 0.97,
        "spore_statistics": {"a": 1, "b": 2},
        "auto_threshold": 0.25,
    }

    assert cloud_sync._observation_push_diff_fields(local_obs, remote_obs) == []
    local_payload = cloud_sync._observation_compare_payload(local_obs, local=True)
    remote_payload = cloud_sync._observation_compare_payload(remote_obs, local=False)
    for field in (
        "visibility",
        "location_public",
        "is_draft",
        "uncertain",
        "unspontaneous",
        "interesting_comment",
        "location_precision",
        "spore_data_visibility",
        "gps_latitude",
        "gps_longitude",
        "ai_selected_probability",
        "spore_statistics",
        "auto_threshold",
    ):
        assert cloud_sync._observation_field_values_match(field, local_payload[field], remote_payload[field])


def test_push_observation_skips_noop_patch_when_remote_matches_after_normalization(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    patched_payloads = []

    monkeypatch.setattr(client, "_find_cloud_observation", lambda desktop_id: "cloud-obs-1")
    monkeypatch.setattr(client, "_patch", lambda path, payload: patched_payloads.append((path, dict(payload))))
    monkeypatch.setattr(client, "_post", lambda *args, **kwargs: pytest.fail("no-op observation sync should not POST"))
    summary = cloud_sync._new_sync_summary()
    with cloud_sync._cloud_sync_summary_scope(summary):
        cloud_id = client.push_observation(
            {
                "id": 7,
                "date": "2026-05-01T12:34:56Z",
                "genus": "Agaricus",
                "species": "campestris",
                "species_guess": "Agaricus campestris",
                "sharing_scope": "public",
                "location_public": "1",
                "is_draft": "0",
                "uncertain": "0",
                "unspontaneous": "1",
                "interesting_comment": "0",
                "location_precision": "Exact",
                "spore_data_visibility": "Public",
                "gps_latitude": "63.0000000001",
                "gps_longitude": "10.0",
                "ai_selected_probability": "0.9700000001",
                "spore_statistics": '{"b": 2, "a": 1}',
                "auto_threshold": "0.25",
            },
            remote_obs={
                "id": "cloud-obs-1",
                "desktop_id": 7,
                "date": "2026-05-01",
                "genus": "Agaricus",
                "species": "campestris",
                "species_guess": "Agaricus campestris",
                "visibility": "public",
                "location_public": True,
                "is_draft": False,
                "uncertain": False,
                "unspontaneous": True,
                "interesting_comment": False,
                "location_precision": "exact",
                "spore_data_visibility": "public",
                "gps_latitude": 63.0,
                "gps_longitude": 10.0,
                "ai_selected_probability": 0.97,
                "spore_statistics": {"a": 1, "b": 2},
                "auto_threshold": 0.25,
            },
        )

    assert cloud_id == "cloud-obs-1"
    assert patched_payloads == []
    assert summary["observations_skipped_noop"] == 1


def test_narrow_select_projection_constants_match_live_schema():
    assert "sharing_scope" not in cloud_sync._OBSERVATION_SELECT_COLUMNS

    assert "captured_at" not in cloud_sync._OBSERVATION_IMAGE_SELECT_COLUMNS
    assert "updated_at" not in cloud_sync._OBSERVATION_IMAGE_SELECT_COLUMNS
    assert "deleted_at" in cloud_sync._OBSERVATION_IMAGE_SELECT_COLUMNS

    assert "top_species_url" in cloud_sync._OBSERVATION_IDENTIFICATION_SELECT_COLUMNS
    assert "top_speciesUrl" not in cloud_sync._OBSERVATION_IDENTIFICATION_SELECT_COLUMNS
    assert "top_adbUrl" not in cloud_sync._OBSERVATION_IDENTIFICATION_SELECT_COLUMNS

    assert "gallery_rotation" in cloud_sync._SPORE_MEASUREMENT_SELECT_COLUMNS


def test_set_image_desktop_id_scopes_patch_to_user(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    patched_payloads = []

    monkeypatch.setattr(client, "_patch", lambda path, payload: patched_payloads.append((path, dict(payload))))

    client.set_image_desktop_id("cloud-img-1", 77)

    assert patched_payloads == [
        ("observation_images?id=eq.cloud-img-1&user_id=eq.user-123", {"desktop_id": 77})
    ]


def test_soft_delete_image_scopes_patch_to_user(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    fetched_paths = []
    patched_payloads = []

    monkeypatch.setattr(
        client,
        "_get",
        lambda path: fetched_paths.append(path) or [{"id": "cloud-img-1", "deleted_at": None}],
    )
    monkeypatch.setattr(client, "_patch", lambda path, payload: patched_payloads.append((path, dict(payload))))

    client.soft_delete_image("cloud-img-1", "2026-05-01 10:00:00")

    assert fetched_paths == [
        "observation_images?id=eq.cloud-img-1&user_id=eq.user-123&select=id,deleted_at&limit=1"
    ]
    assert patched_payloads == [
        (
            "observation_images?id=eq.cloud-img-1&user_id=eq.user-123",
            {"deleted_at": "2026-05-01 10:00:00"},
        )
    ]


def test_pull_image_metadata_filters_deleted_rows_by_default(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    fetched_paths = []

    monkeypatch.setattr(
        client,
        "_get",
        lambda path: fetched_paths.append(path) or [
            {"id": "cloud-img-1", "deleted_at": None, "storage_path": "user/cloud-obs-1/cloud-img-1.jpg"},
            {
                "id": "cloud-img-2",
                "deleted_at": "2026-05-01 10:00:00",
                "storage_path": "user/cloud-obs-1/cloud-img-2.jpg",
            },
        ],
    )

    rows = client.pull_image_metadata("cloud-obs-1")

    assert fetched_paths == [
        (
            "observation_images?observation_id=eq.cloud-obs-1&user_id=eq.user-123"
            "&deleted_at=is.null&select="
            f"{cloud_sync._OBSERVATION_IMAGE_SELECT_COLUMNS}"
        )
    ]
    assert [row["id"] for row in rows] == ["cloud-img-1"]
    assert rows[0]["deleted_at"] is None


def test_pull_image_metadata_include_deleted_for_sync_returns_deleted_rows(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    fetched_paths = []

    monkeypatch.setattr(
        client,
        "_get",
        lambda path: fetched_paths.append(path) or [
            {"id": "cloud-img-1", "deleted_at": None, "storage_path": "user/cloud-obs-1/cloud-img-1.jpg"},
            {
                "id": "cloud-img-2",
                "deleted_at": "2026-05-01 10:00:00",
                "storage_path": "user/cloud-obs-1/cloud-img-2.jpg",
            },
        ],
    )

    rows = client.pull_image_metadata("cloud-obs-1", include_deleted_for_sync=True)

    assert fetched_paths == [
        (
            "observation_images?observation_id=eq.cloud-obs-1&user_id=eq.user-123&select="
            f"{cloud_sync._OBSERVATION_IMAGE_SELECT_COLUMNS}"
        )
    ]
    assert [row["id"] for row in rows] == ["cloud-img-1", "cloud-img-2"]
    assert rows[1]["deleted_at"] == "2026-05-01 10:00:00"


def test_pull_image_metadata_does_not_change_local_db_rows(monkeypatch, tmp_path):
    db_path = tmp_path / "local.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE images (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        conn.execute("INSERT INTO images (name) VALUES (?)", ("kept",))
        conn.commit()
    finally:
        conn.close()

    client = cloud_sync.SporelyCloudClient("token", "user-123")
    monkeypatch.setattr(
        client,
        "_get",
        lambda path: [
            {"id": "cloud-img-1", "deleted_at": None, "storage_path": "user/cloud-obs-1/cloud-img-1.jpg"}
        ],
    )
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: pytest.fail("unexpected local DB access"))

    rows = client.pull_image_metadata("cloud-obs-1", include_deleted_for_sync=True)

    conn = sqlite3.connect(db_path)
    try:
        local_rows = conn.execute("SELECT id, name FROM images ORDER BY id").fetchall()
    finally:
        conn.close()

    assert rows == [
        {"id": "cloud-img-1", "deleted_at": None, "storage_path": "user/cloud-obs-1/cloud-img-1.jpg"}
    ]
    assert local_rows == [(1, "kept")]


def test_push_pending_image_tombstones_marks_delete_synced_at(monkeypatch, tmp_path):
    db_path = _init_tombstone_sync_db(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?)
            """,
            ("cloud-image-1", "2026-05-01 10:00:00", 1, 11),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    calls = []

    class DummyClient:
        def soft_delete_image(self, cloud_image_id, deleted_at):
            calls.append((cloud_image_id, deleted_at))

    warnings = cloud_sync._push_pending_image_tombstones(DummyClient())

    conn = sqlite3.connect(db_path)
    try:
        delete_synced_at = conn.execute(
            "SELECT delete_synced_at FROM image_tombstones WHERE deleted_cloud_id = ?",
            ("cloud-image-1",),
        ).fetchone()[0]
    finally:
        conn.close()

    assert warnings == []
    assert calls == [("cloud-image-1", "2026-05-01 10:00:00")]
    assert delete_synced_at is not None


def test_push_pending_image_tombstones_leaves_delete_synced_at_null_on_failure(monkeypatch, tmp_path):
    db_path = _init_tombstone_sync_db(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?)
            """,
            ("cloud-image-1", "2026-05-01 10:00:00", 1, 11),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    class DummyClient:
        def soft_delete_image(self, cloud_image_id, deleted_at):
            raise cloud_sync.CloudSyncError("boom")

    warnings = cloud_sync._push_pending_image_tombstones(DummyClient())

    conn = sqlite3.connect(db_path)
    try:
        delete_synced_at = conn.execute(
            "SELECT delete_synced_at FROM image_tombstones WHERE deleted_cloud_id = ?",
            ("cloud-image-1",),
        ).fetchone()[0]
    finally:
        conn.close()

    assert warnings and "could not sync cloud image tombstone" in warnings[0]
    assert delete_synced_at is None


def test_push_pending_image_tombstones_runs_before_active_image_push(monkeypatch, tmp_path):
    db_path = _init_tombstone_sync_db(tmp_path)
    images_root = tmp_path / "images"
    images_root.mkdir()
    image_path = images_root / "image.jpg"
    image_path.write_bytes(b"image-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, image_type, sort_order, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (11, 1, None, str(image_path), "field", 0, "2026-05-01T10:00:00Z"),
        )
        conn.commit()
    finally:
        conn.close()

    order = []
    client = cloud_sync.SporelyCloudClient("token", "user-123")

    monkeypatch.setattr(
        cloud_sync,
        "_push_pending_image_tombstones",
        lambda _client: order.append("tombstones") or [],
    )
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(client, "pull_image_metadata", lambda obs_cloud_id: order.append("existing_rows") or [])
    monkeypatch.setattr(client, "push_image_metadata", lambda img, obs_cloud_id, storage_path: order.append("push_metadata") or "cloud-image-1")
    monkeypatch.setattr(client, "upload_image_file", lambda local_path, obs_cloud_id, img_cloud_id, storage_path=None, upload_meta=None: order.append("upload_file") or storage_path)
    monkeypatch.setattr(client, "set_image_desktop_id", lambda cloud_image_id, desktop_id: order.append("set_desktop_id"))
    monkeypatch.setattr(client, "_patch", lambda *args, **kwargs: order.append("patch_storage"))
    monkeypatch.setattr(client, "_observation_images_support_ai_crop", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_upload_metadata", lambda: False)

    result = cloud_sync._push_images_for_observation(client, {"id": 1}, "cloud-obs-1")

    assert result is True
    assert order.index("tombstones") < order.index("existing_rows") < order.index("upload_file") < order.index("push_metadata")


def test_push_images_for_observation_leaves_image_pending_when_worker_upload_fails(monkeypatch, tmp_path):
    db_path = _init_tombstone_sync_db(tmp_path)
    images_root = tmp_path / "images"
    images_root.mkdir()
    image_path = images_root / "image.jpg"
    image_path.write_bytes(b"image-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, image_type, sort_order, created_at, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (11, 1, None, str(image_path), "field", 0, "2026-05-01T10:00:00Z", None),
        )
        conn.commit()
    finally:
        conn.close()

    order = []
    client = cloud_sync.SporelyCloudClient("token", "user-123")

    monkeypatch.setattr(
        cloud_sync,
        "_push_pending_image_tombstones",
        lambda _client: order.append("tombstones") or [],
    )
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(client, "pull_image_metadata", lambda obs_cloud_id: order.append("existing_rows") or [])
    monkeypatch.setattr(
        client,
        "upload_image_file",
        lambda *args, **kwargs: order.append("upload_file") or (_ for _ in ()).throw(cloud_sync.CloudSyncError("Worker upload failed")),
    )
    monkeypatch.setattr(client, "push_image_metadata", lambda *args, **kwargs: order.append("push_metadata") or "cloud-image-1")
    monkeypatch.setattr(client, "set_image_desktop_id", lambda *args, **kwargs: order.append("set_desktop_id"))
    monkeypatch.setattr(client, "_observation_images_support_ai_crop", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_upload_metadata", lambda: False)

    result = cloud_sync._push_images_for_observation(client, {"id": 1}, "cloud-obs-1")

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT cloud_id, synced_at FROM images WHERE id = ?",
            (11,),
        ).fetchone()
    finally:
        conn.close()

    assert result is False
    assert "push_metadata" not in order
    assert row == (None, None)


def test_push_images_for_observation_skips_already_synced_image_without_uploading(
    monkeypatch,
    tmp_path,
    capsys,
):
    db_path = _init_tombstone_sync_db(tmp_path)
    images_root = tmp_path / "images"
    images_root.mkdir()
    image_path = images_root / "synced.jpg"
    image_path.write_bytes(b"synced-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, image_type, sort_order, created_at, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (11, 1, "cloud-image-1", str(image_path), "field", 0, "2026-05-01T10:00:00Z", "2026-05-01T10:00:00Z"),
        )
        conn.commit()
    finally:
        conn.close()

    progress_messages: list[str] = []
    storage_path = "user-123/cloud-obs-1/cloud-image-1_synced.jpg"
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    monkeypatch.setattr(
        client,
        "pull_image_metadata",
        lambda obs_cloud_id: [
            {
                "id": "cloud-image-1",
                "desktop_id": 11,
                "storage_path": storage_path,
                "original_filename": "synced.jpg",
                "image_type": "field",
                "sort_order": 0,
            }
        ],
    )
    monkeypatch.setattr(client, "_observation_images_support_ai_crop", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_ai_crop_custom", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_upload_metadata", lambda: False)
    monkeypatch.setattr(cloud_sync, "_push_pending_image_tombstones", lambda _client: [])
    monkeypatch.setattr(cloud_sync, "_file_content_signature", lambda path: "sig-1")
    monkeypatch.setattr(cloud_sync, "_load_cloud_image_file_signature", lambda *args, **kwargs: "sig-1")
    monkeypatch.setattr(
        cloud_sync,
        "_prepared_item_remote_payload",
        lambda *args, **kwargs: {
            "desktop_id": 11,
            "storage_path": storage_path,
            "original_filename": "synced.jpg",
            "image_type": "field",
            "sort_order": 0,
        },
    )
    monkeypatch.setattr(
        cloud_sync,
        "_remote_image_payload",
        lambda *args, **kwargs: {
            "desktop_id": 11,
            "storage_path": storage_path,
            "original_filename": "synced.jpg",
            "image_type": "field",
            "sort_order": 0,
        },
    )
    monkeypatch.setattr(client, "upload_image_file", lambda *args, **kwargs: pytest.fail("upload should not run"))
    monkeypatch.setattr(client, "push_image_metadata", lambda *args, **kwargs: pytest.fail("metadata patch should not run"))
    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation", lambda observation_id: [
        {
            "id": 11,
            "observation_id": 1,
            "cloud_id": "cloud-image-1",
            "filepath": str(image_path),
            "image_type": "field",
            "sort_order": 0,
            "created_at": "2026-05-01T10:00:00Z",
            "synced_at": "2026-05-01T10:00:00Z",
        }
    ])
    monkeypatch.setattr(client, "_storage_remove", lambda *args, **kwargs: None)
    monkeypatch.setattr(client, "_delete", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    summary = cloud_sync._new_sync_summary()
    with cloud_sync._cloud_sync_summary_scope(summary):
        result = cloud_sync._push_images_for_observation(
            client,
            {"id": 1},
            "cloud-obs-1",
            progress_cb=lambda message, current, total: progress_messages.append(str(message)),
            progress_state={"done": 0, "total": 0},
            observation_index=1,
            observation_total=1,
        )

    output = capsys.readouterr().out

    assert result is True
    assert any("Checking cloud image" in message for message in progress_messages)
    assert not any("Uploading cloud image" in message for message in progress_messages)
    assert "skipped already synced cloud image" in output
    assert summary["images_checked"] == 1
    assert summary["images_skipped_already_synced"] == 1
    assert summary["images_uploaded"] == 0


def test_push_images_for_observation_metadata_patch_does_not_emit_uploading_progress(
    monkeypatch,
    tmp_path,
    capsys,
):
    db_path = _init_tombstone_sync_db(tmp_path)
    images_root = tmp_path / "images"
    images_root.mkdir()
    image_path = images_root / "metadata.jpg"
    image_path.write_bytes(b"metadata-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, image_type, sort_order,
                notes, created_at, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                11,
                1,
                "cloud-image-1",
                str(image_path),
                "field",
                0,
                "local note",
                "2026-05-01T10:00:00Z",
                "2026-05-01T10:00:00Z",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    progress_messages: list[str] = []
    upload_calls: list[tuple[str, str, str, str | None]] = []
    metadata_patch_calls: list[tuple[str, dict]] = []
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    monkeypatch.setattr(client, "pull_image_metadata", lambda obs_cloud_id: [
        {
            "id": "cloud-image-1",
            "observation_id": "cloud-obs-1",
            "desktop_id": 11,
            "storage_path": "user-123/cloud-obs-1/cloud-image-1_metadata.jpg",
            "original_filename": "metadata.jpg",
            "image_type": "field",
            "sort_order": 0,
            "notes": "remote note",
        }
    ])
    monkeypatch.setattr(client, "_observation_images_support_ai_crop", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_ai_crop_custom", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_upload_metadata", lambda: False)
    monkeypatch.setattr(cloud_sync, "_push_pending_image_tombstones", lambda _client: [])
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: False)
    monkeypatch.setattr(cloud_sync, "_file_content_signature", lambda path: "sig")
    monkeypatch.setattr(cloud_sync, "_load_cloud_image_file_signature", lambda *args, **kwargs: "sig")
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        cloud_sync.ImageDB,
        "get_images_for_observation",
        lambda observation_id: [
            {
                "id": 11,
                "observation_id": 1,
                "cloud_id": "cloud-image-1",
                "filepath": str(image_path),
                "image_type": "field",
                "sort_order": 0,
                "notes": "local note",
                "created_at": "2026-05-01T10:00:00Z",
                "synced_at": "2026-05-01T10:00:00Z",
            }
        ],
    )
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    def fake_upload_image_file(local_path, obs_cloud_id, img_cloud_id, storage_path=None, upload_meta=None):
        upload_calls.append((str(local_path), str(obs_cloud_id), str(img_cloud_id), storage_path))
        return storage_path

    def fake_push_image_metadata(img, obs_cloud_id, storage_path):
        metadata_patch_calls.append((str(obs_cloud_id), dict(img)))
        return "cloud-image-1"

    monkeypatch.setattr(client, "upload_image_file", fake_upload_image_file)
    monkeypatch.setattr(client, "push_image_metadata", fake_push_image_metadata)

    summary = cloud_sync._new_sync_summary()
    with cloud_sync._cloud_sync_summary_scope(summary):
        result = cloud_sync._push_images_for_observation(
            client,
            {"id": 1},
            "cloud-obs-1",
            progress_cb=lambda message, current, total: progress_messages.append(str(message)),
            progress_state={"done": 0, "total": 0},
            observation_index=1,
            observation_total=1,
        )

    output = capsys.readouterr().out

    assert result is True
    assert upload_calls == []
    assert metadata_patch_calls
    assert any("Checking cloud image" in message for message in progress_messages)
    assert not any("Uploading cloud image" in message for message in progress_messages)
    assert "metadata patch for cloud image" in output
    assert summary["images_checked"] == 1
    assert summary["images_uploaded"] == 0


def test_push_images_for_observation_emits_uploading_when_file_bytes_are_sent(
    monkeypatch,
    tmp_path,
    capsys,
):
    db_path = _init_tombstone_sync_db(tmp_path)
    images_root = tmp_path / "images"
    images_root.mkdir()
    image_path = images_root / "upload.jpg"
    image_path.write_bytes(b"upload-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, image_type, sort_order, created_at, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (11, 1, None, str(image_path), "field", 0, "2026-05-01T10:00:00Z", None),
        )
        conn.commit()
    finally:
        conn.close()

    progress_messages: list[str] = []
    upload_calls: list[tuple[str, str, str, str | None]] = []
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    monkeypatch.setattr(client, "pull_image_metadata", lambda obs_cloud_id: [])
    monkeypatch.setattr(client, "_observation_images_support_ai_crop", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_ai_crop_custom", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_upload_metadata", lambda: False)
    monkeypatch.setattr(cloud_sync, "_push_pending_image_tombstones", lambda _client: [])
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: False)
    monkeypatch.setattr(cloud_sync, "_file_content_signature", lambda path: "sig-2")
    monkeypatch.setattr(cloud_sync, "_load_cloud_image_file_signature", lambda *args, **kwargs: "")
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_prepared_item_remote_payload", lambda *args, **kwargs: {})
    monkeypatch.setattr(cloud_sync, "_remote_image_payload", lambda *args, **kwargs: {})
    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation", lambda observation_id: [
        {
            "id": 11,
            "observation_id": 1,
            "cloud_id": None,
            "filepath": str(image_path),
            "image_type": "field",
            "sort_order": 0,
            "created_at": "2026-05-01T10:00:00Z",
            "synced_at": None,
        }
    ])
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    def fake_upload_image_file(local_path, obs_cloud_id, img_cloud_id, storage_path=None, upload_meta=None):
        upload_calls.append((str(local_path), str(obs_cloud_id), str(img_cloud_id), storage_path))
        return storage_path

    monkeypatch.setattr(client, "upload_image_file", fake_upload_image_file)
    monkeypatch.setattr(client, "push_image_metadata", lambda img, obs_cloud_id, storage_path: "cloud-image-11")

    result = cloud_sync._push_images_for_observation(
        client,
        {"id": 1},
        "cloud-obs-1",
        progress_cb=lambda message, current, total: progress_messages.append(str(message)),
        progress_state={"done": 0, "total": 0},
        observation_index=1,
        observation_total=1,
    )

    output = capsys.readouterr().out

    assert result is True
    assert upload_calls
    assert any("Uploading cloud image" in message for message in progress_messages)
    assert "uploading cloud image" in output


def test_upload_image_file_records_summary_and_quota_rpc_calls(monkeypatch, tmp_path, capsys):
    from PIL import Image

    source_path = tmp_path / "source.jpg"
    Image.new("RGB", (8, 8), color="white").save(source_path, format="JPEG")

    client = cloud_sync.SporelyCloudClient("token", "user-123")
    upload_calls: list[tuple[str, str]] = []

    class _FakeWorker:
        base_url = "https://example.invalid"

        def put_file(self, prepared_path, storage_path, **kwargs):
            upload_calls.append(("put_file", str(storage_path)))
            return {"key": storage_path}

        def put_bytes(self, data, storage_path, **kwargs):
            upload_calls.append(("put_bytes", str(storage_path)))
            return {"key": storage_path}

    monkeypatch.setattr(cloud_sync, "direct_r2_runtime_available", lambda: False)
    monkeypatch.setattr(client, "_get_media_worker", lambda: _FakeWorker())
    monkeypatch.setattr(
        cloud_sync,
        "_prepare_cloud_image_upload_file",
        lambda source_path, temp_dir, image_id, meta: (
            Path(source_path),
            8,
            8,
            8,
            8,
            "image/jpeg",
            80,
        ),
    )

    summary = cloud_sync._new_sync_summary()
    with cloud_sync._cloud_sync_summary_scope(summary):
        result = client.upload_image_file(
            str(source_path),
            "cloud-obs-1",
            "cloud-image-1",
            storage_path="user-123/cloud-obs-1/cloud-image-1_source.jpg",
            upload_meta={"observation_id": 1, "image_id": 11},
        )

    output = capsys.readouterr().out

    assert result == "user-123/cloud-obs-1/cloud-image-1_source.jpg"
    assert upload_calls == [
        ("put_file", "user-123/cloud-obs-1/cloud-image-1_source.jpg"),
        ("put_bytes", "user-123/cloud-obs-1/thumb_cloud-image-1_source.jpg"),
    ]
    assert summary["images_uploaded"] == 1
    assert summary["storage_quota_delta_rpc_calls"] == 2
    assert "Uploading cloud image request" in output


def test_apply_remote_observation_fields_pulls_interesting_comment(monkeypatch):
    captured = {}

    def fake_update_observation(observation_id, **kwargs):
        captured["observation_id"] = observation_id
        captured["kwargs"] = dict(kwargs)

    monkeypatch.setattr(cloud_sync.ObservationDB, "update_observation", fake_update_observation)

    cloud_sync._apply_remote_observation_fields(
        42,
        {"interesting_comment": True},
        fields={"interesting_comment"},
    )

    assert captured == {
        "observation_id": 42,
        "kwargs": {
            "interesting_comment": True,
            "allow_nulls": True,
        },
    }


@pytest.mark.parametrize(
    ("remote_value", "expected"),
    [
        (True, True),
        (None, False),
    ],
)
def test_create_local_from_remote_preserves_interesting_comment(
    monkeypatch,
    tmp_path,
    remote_value,
    expected,
):
    db_path = tmp_path / "sporely.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY,
            cloud_id TEXT,
            sync_status TEXT,
            synced_at TEXT
        )
        """
    )
    conn.commit()
    conn.close()

    captured = {}

    def fake_create_observation(**kwargs):
        captured.update(kwargs)
        return 1

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync.ObservationDB, "create_observation", fake_create_observation)
    monkeypatch.setattr(cloud_sync, "_import_remote_images", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "from_stored_credentials", staticmethod(lambda: None))
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *args, **kwargs: None)

    remote = {
        "id": "cloud-obs-1",
        "date": "2026-05-01",
    }
    if remote_value is not None:
        remote["interesting_comment"] = remote_value

    local_id = cloud_sync._create_local_from_remote(remote, remote_images=[])

    assert local_id == 1
    assert captured["interesting_comment"] is expected


def test_mark_observation_dirty_clears_blocked_sync_state(tmp_path, monkeypatch):
    db_path = tmp_path / "cloud_dirty.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY,
            cloud_id TEXT,
            sync_status TEXT,
            synced_at TEXT,
            sync_error_code TEXT,
            sync_error_message TEXT,
            sync_blocked_reason TEXT,
            sync_blocked_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO observations (
            id, cloud_id, sync_status, synced_at,
            sync_error_code, sync_error_message, sync_blocked_reason, sync_blocked_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1,
            "cloud-obs-1",
            "blocked",
            "2026-01-01T00:00:00Z",
            "23514",
            'POST observations: {"code":"23514","message":"Free Sporely accounts can keep up to 20 privacy slot observations. Publish or use exact public location to continue."}',
            cloud_sync.privacy_slot_limit_user_message(),
            "2026-01-01T00:00:01Z",
        ),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))

    cloud_sync.mark_observation_dirty(1)

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT
                cloud_id, sync_status, synced_at,
                sync_error_code, sync_error_message,
                sync_blocked_reason, sync_blocked_at
            FROM observations
            WHERE id = ?
            """,
            (1,),
        ).fetchone()
    finally:
        conn.close()

    assert row == (
        "cloud-obs-1",
        "dirty",
        "2026-01-01T00:00:00Z",
        None,
        None,
        None,
        None,
    )


def test_privacy_slot_limit_error_classifier_matches_supabase_payload():
    assert cloud_sync.is_privacy_slot_limit_error(
        {
            "code": "23514",
            "message": "Free Sporely accounts can keep up to 20 privacy slot observations. Publish or use exact public location to continue.",
        }
    )
    assert cloud_sync.is_privacy_slot_limit_error(
        'POST observations: {"code":"23514","message":"Free Sporely accounts can keep up to 20 privacy slot observations. Publish or use exact public location to continue."}'
    )
    assert not cloud_sync.is_privacy_slot_limit_error(
        {
            "code": "23514",
            "message": "Some other check constraint failed.",
        }
    )


def test_fetch_cloud_usage_summary_counts_private_and_fuzzed_slots():
    client = SimpleNamespace(
        fetch_cloud_plan_profile=lambda: {"cloud_plan": "free", "is_pro": False},
        count_remote_privacy_slots=lambda: 2,
        list_remote_observations=lambda: pytest.fail("usage summary should not fetch the full observation list"),
    )

    summary = cloud_sync.fetch_cloud_usage_summary(client)

    assert summary["cloud_usage_loaded"] is True
    assert summary["privacy_slots_used"] == 2
    assert summary["privacy_slots_available"] == 18
    assert summary["cloud_plan"] == "free"


def test_fetch_cloud_usage_summary_keeps_plan_loaded_when_privacy_count_fails():
    client = SimpleNamespace(
        fetch_cloud_plan_profile=lambda: {
            "cloud_plan": "pro",
            "is_pro": True,
            "storage_quota_bytes": None,
            "full_res_storage_enabled": False,
        },
        count_remote_privacy_slots=lambda: (_ for _ in ()).throw(
            cloud_sync.CloudSyncError(
                'GET observations?user_id=eq.user-123&select=id status=503: {"message":"Service Unavailable"}'
            )
        ),
    )

    summary = cloud_sync.fetch_cloud_usage_summary(client)

    assert summary["cloud_profile_loaded"] is True
    assert summary["cloud_privacy_usage_loaded"] is False
    assert summary["cloud_usage_loaded"] is False
    assert summary["cloud_plan"] == "pro"
    assert summary["privacy_slots_used"] is None
    assert summary["privacy_slots_available"] is None
    assert "status=503" in summary["cloud_usage_error"]
    assert "Service Unavailable" in summary["cloud_usage_error"]


def test_push_all_blocks_privacy_slot_limit_and_continues_to_next_row(tmp_path, monkeypatch):
    db_path = tmp_path / "push_all_blocked.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY,
            date TEXT,
            cloud_id TEXT,
            sync_status TEXT,
            synced_at TEXT,
            user_id TEXT,
            visibility TEXT,
            location_precision TEXT,
            sync_error_code TEXT,
            sync_error_message TEXT,
            sync_blocked_reason TEXT,
            sync_blocked_at TEXT
        );
        INSERT INTO observations (
            id, date, cloud_id, sync_status, synced_at,
            user_id, visibility, location_precision
        ) VALUES
            (1, '2026-05-02', NULL, 'dirty', NULL, 'user-1', 'private', 'fuzzed'),
            (2, '2026-05-01', NULL, 'dirty', NULL, 'user-1', 'public', 'exact');
        """
    )
    conn.commit()
    conn.close()

    class DummyClient:
        def push_observation(self, obs, remote_obs=None, **kwargs):
            if int(obs.get("id") or 0) == 1:
                raise cloud_sync.CloudSyncError(
                    'POST observations: {"code":"23514","message":"Free Sporely accounts can keep up to 20 privacy slot observations. Publish or use exact public location to continue."}'
                )
            return "cloud-obs-2"

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)

    result = cloud_sync.push_all(
        DummyClient(),
        sync_images=False,
        sync_calibrations=False,
    )

    summary = cloud_sync.summarize_sync_issues(result["errors"])
    conn = sqlite3.connect(db_path)
    try:
        blocked_row = conn.execute(
            """
            SELECT
                cloud_id, sync_status, sync_error_code, sync_error_message,
                sync_blocked_reason, sync_blocked_at
            FROM observations
            WHERE id = 1
            """
        ).fetchone()
        synced_row = conn.execute(
            """
            SELECT cloud_id, sync_status
            FROM observations
            WHERE id = 2
            """
        ).fetchone()
    finally:
        conn.close()

    assert result["pushed"] == 1
    assert summary["blocked_count"] == 1
    assert summary["other_count"] == 0
    assert blocked_row[0] is None
    assert blocked_row[1] == "blocked"
    assert blocked_row[2] == "privacy_slot_limit"
    assert "privacy slot observations" in blocked_row[3]
    assert blocked_row[4] == cloud_sync.privacy_slot_limit_user_message()
    assert blocked_row[5] is not None
    assert synced_row == ("cloud-obs-2", "synced")


def test_push_all_marks_image_too_large_as_retryable_and_keeps_next_row_moving(tmp_path, monkeypatch):
    db_path = tmp_path / "push_all_retryable.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY,
            date TEXT,
            cloud_id TEXT,
            sync_status TEXT,
            synced_at TEXT,
            user_id TEXT,
            visibility TEXT,
            location_precision TEXT,
            sync_error_code TEXT,
            sync_error_message TEXT,
            sync_blocked_reason TEXT,
            sync_blocked_at TEXT
        );
        INSERT INTO observations (
            id, date, cloud_id, sync_status, synced_at,
            user_id, visibility, location_precision
        ) VALUES
            (1, '2026-05-02', NULL, 'dirty', NULL, 'user-1', 'public', 'exact'),
            (2, '2026-05-01', NULL, 'dirty', NULL, 'user-1', 'public', 'exact');
        """
    )
    conn.commit()
    conn.close()

    class DummyClient:
        def push_observation(self, obs, remote_obs=None, **kwargs):
            if int(obs.get("id") or 0) == 1:
                raise cloud_sync.CloudSyncError(
                    'Image is too large for your plan. Make it smaller or upgrade to Pro.'
                )
            return "cloud-obs-2"

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)

    result = cloud_sync.push_all(
        DummyClient(),
        sync_images=False,
        sync_calibrations=False,
    )

    summary = cloud_sync.summarize_sync_issues(result["errors"])
    conn = sqlite3.connect(db_path)
    try:
        retryable_row = conn.execute(
            """
            SELECT
                cloud_id, sync_status, sync_error_code, sync_error_message,
                sync_blocked_reason, sync_blocked_at
            FROM observations
            WHERE id = 1
            """
        ).fetchone()
        synced_row = conn.execute(
            """
            SELECT cloud_id, sync_status
            FROM observations
            WHERE id = 2
            """
        ).fetchone()
    finally:
        conn.close()

    assert result["pushed"] == 1
    assert summary["retryable_count"] == 1
    assert summary["blocked_count"] == 0
    assert summary["other_count"] == 0
    assert retryable_row[0] is None
    assert retryable_row[1] == "dirty"
    assert retryable_row[2] == "image_too_large_for_plan"
    assert "Image is too large for your plan" in retryable_row[3]
    assert retryable_row[4] is None
    assert retryable_row[5] is None
    assert synced_row == ("cloud-obs-2", "synced")


def test_push_all_marks_image_upload_plan_limit_retryable_and_keeps_observation_dirty(
    tmp_path,
    monkeypatch,
):
    db_path = _init_push_all_sync_db(tmp_path)
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"image-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO observations (
                id, cloud_id, sync_status, synced_at, sync_error_code,
                sync_error_message, sync_blocked_reason, sync_blocked_at, user_id, date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, None, "dirty", None, None, None, None, None, "user-1", "2026-05-02"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath,
                image_type, source_role, file_purpose, sort_order, created_at, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                11,
                1,
                None,
                str(image_path),
                None,
                "field",
                "local_canonical",
                "field",
                0,
                "2026-05-01T10:00:00Z",
                None,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    client = _PushAllImageClient(fail_derivative_upload=True)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_load_cloud_image_file_signature", lambda *args, **kwargs: "")
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *args, **kwargs: "")
    monkeypatch.setattr(cloud_sync, "_store_local_cloud_media_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_push_measurements_for_observation", lambda *args, **kwargs: None)

    result = cloud_sync.push_all(
        client,
        sync_images=True,
        sync_calibrations=False,
    )

    summary = cloud_sync.summarize_sync_issues(result["errors"])
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT cloud_id, sync_status, sync_error_code, sync_error_message,
                   sync_blocked_reason, sync_blocked_at
            FROM observations
            WHERE id = 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert result["pushed"] == 0
    assert summary["retryable_count"] == 1
    assert summary["blocked_count"] == 0
    assert summary["other_count"] == 0
    assert row[0] == "cloud-obs-1"
    assert row[1] == "dirty"
    assert row[2] == "image_too_large_for_plan"
    assert cloud_sync.IMAGE_TOO_LARGE_FOR_PLAN_MESSAGE in row[3]
    assert row[4] is None
    assert row[5] is None
    assert len(client.upload_image_calls) == 1
    assert client.upload_original_calls == []


def test_push_all_keeps_optional_original_upload_failure_out_of_top_level_errors_and_dirty_state(
    tmp_path,
    monkeypatch,
):
    db_path = _init_push_all_sync_db(tmp_path)
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"image-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO observations (
                id, cloud_id, sync_status, synced_at, sync_error_code,
                sync_error_message, sync_blocked_reason, sync_blocked_at, user_id, date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, None, "dirty", None, None, None, None, None, "user-1", "2026-05-02"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath,
                image_type, source_role, file_purpose, sort_order, created_at, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                11,
                1,
                None,
                str(image_path),
                None,
                "field",
                "local_canonical",
                "field",
                0,
                "2026-05-01T10:00:00Z",
                None,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    client = _PushAllImageClient(fail_original_upload=True)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_load_cloud_image_file_signature", lambda *args, **kwargs: "")
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *args, **kwargs: "")
    monkeypatch.setattr(cloud_sync, "_store_local_cloud_media_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: True)
    monkeypatch.setattr(cloud_sync, "_push_measurements_for_observation", lambda *args, **kwargs: None)

    result = cloud_sync.push_all(
        client,
        sync_images=True,
        sync_calibrations=False,
    )

    summary = cloud_sync.summarize_sync_issues(result["errors"])
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT cloud_id, sync_status, sync_error_code, sync_error_message,
                   sync_blocked_reason, sync_blocked_at
            FROM observations
            WHERE id = 1
            """
        ).fetchone()
        image_row = conn.execute(
            """
            SELECT cloud_id, synced_at
            FROM images
            WHERE id = 11
            """
        ).fetchone()
    finally:
        conn.close()

    assert result["pushed"] == 1
    assert result["errors"] == []
    assert summary["retryable_count"] == 0
    assert summary["blocked_count"] == 0
    assert summary["other_count"] == 0
    assert result["original_sync"]["failed_uploads"] == 1
    assert row[0] == "cloud-obs-1"
    assert row[1] == "synced"
    assert row[2] is None
    assert row[3] is None
    assert row[4] is None
    assert row[5] is None
    assert image_row[0] is not None
    assert image_row[1] is not None
    assert len(client.upload_image_calls) == 1
    assert len(client.upload_original_calls) == 1


def test_push_all_announces_cloud_media_check_before_skip_message(
    tmp_path,
    monkeypatch,
    capsys,
):
    ctx = _setup_push_all_existing_media_case(tmp_path, monkeypatch)
    measurement_push_calls: list[int] = []

    def fail_push_images(*args, **kwargs):
        pytest.fail("image prep should not run")

    monkeypatch.setattr(cloud_sync, "_push_images_for_observation", fail_push_images)
    monkeypatch.setattr(
        cloud_sync,
        "_push_measurements_for_observation",
        lambda *args, **kwargs: measurement_push_calls.append(int(args[1])),
    )

    result = cloud_sync.push_all(
        ctx.client,
        progress_cb=lambda text, current, total: ctx.progress_messages.append(text),
        remote_obs=[dict(ctx.remote_obs)],
        sync_images=True,
        sync_calibrations=False,
    )

    output = capsys.readouterr().out
    check_index = next(
        i for i, message in enumerate(ctx.progress_messages)
        if "Checking cloud media for observation 1/1" in message
    )
    skip_index = next(
        i for i, message in enumerate(ctx.progress_messages)
        if "Image/render media unchanged; skipping image prep for observation 1/1" in message
    )
    measurements_index = next(
        i for i, message in enumerate(ctx.progress_messages)
        if "Syncing measurements for observation 1/1" in message
    )

    assert result["pushed"] == 1
    assert result["errors"] == []
    assert measurement_push_calls == [1]
    assert ctx.refresh_calls == [1]
    assert check_index < skip_index < measurements_index
    assert any("no prepared upload candidates" in message for message in ctx.progress_messages)
    assert "measurements pushed" in output


def test_push_all_skips_image_prep_for_measurement_only_change_but_pushes_measurements(
    tmp_path,
    monkeypatch,
    capsys,
):
    ctx = _setup_push_all_existing_media_case(tmp_path, monkeypatch)
    with sqlite3.connect(ctx.db_path) as conn:
        conn.execute(
            "UPDATE spore_measurements SET length_um = ? WHERE id = ?",
            (13.7, 21),
        )
        conn.commit()

    measurement_push_calls: list[int] = []

    def fail_push_images(*args, **kwargs):
        pytest.fail("image prep should not run for measurement-only changes")

    monkeypatch.setattr(cloud_sync, "_push_images_for_observation", fail_push_images)
    monkeypatch.setattr(
        cloud_sync,
        "_push_measurements_for_observation",
        lambda *args, **kwargs: measurement_push_calls.append(int(args[1])),
    )

    result = cloud_sync.push_all(
        ctx.client,
        progress_cb=lambda text, current, total: ctx.progress_messages.append(text),
        remote_obs=[dict(ctx.remote_obs)],
        sync_images=True,
        sync_calibrations=False,
    )

    output = capsys.readouterr().out
    skip_index = next(
        i for i, message in enumerate(ctx.progress_messages)
        if "Image/render media unchanged; skipping image prep for observation 1/1" in message
    )
    measurements_index = next(
        i for i, message in enumerate(ctx.progress_messages)
        if "Syncing measurements for observation 1/1" in message
    )

    assert result["pushed"] == 1
    assert result["errors"] == []
    assert measurement_push_calls == [1]
    assert ctx.refresh_calls == [1]
    assert skip_index < measurements_index
    assert any("no prepared upload candidates" in message for message in ctx.progress_messages)
    assert "image prep diagnostics" not in output
    assert "measurements pushed" in output


def test_push_measurements_for_observation_logs_finalization_timing(
    tmp_path,
    monkeypatch,
    capsys,
):
    ctx = _setup_push_all_existing_media_case(tmp_path, monkeypatch)
    with sqlite3.connect(ctx.db_path) as conn:
        conn.execute("UPDATE images SET image_type = ? WHERE id = ?", ("microscope", 11))
        conn.commit()
    push_calls: list[tuple[int, str, bool]] = []

    def push_measurement(meas, cloud_image_id, remote_measurement_cache=None):
        push_calls.append((int(meas["id"]), str(cloud_image_id), bool(remote_measurement_cache)))
        return f"cloud-measurement-{int(meas['id'])}"

    monkeypatch.setattr(ctx.client, "push_measurement", push_measurement, raising=False)

    cloud_sync._push_measurements_for_observation(ctx.client, 1)

    output = capsys.readouterr().out
    assert push_calls == [(21, "cloud-image-11", True)]
    assert "measurement push finalized" in output
    assert "measurements=1" in output
    assert "pushed=1" in output


@pytest.mark.parametrize("mutation_name", ["metadata", "file", "crop", "render"])
def test_push_all_prepares_images_for_image_render_changes(
    tmp_path,
    monkeypatch,
    capsys,
    mutation_name,
):
    monkeypatch.setenv("SPORELY_DEBUG_CLOUD_SYNC", "1")
    ctx = _setup_push_all_existing_media_case(tmp_path, monkeypatch)
    if mutation_name == "metadata":
        with sqlite3.connect(ctx.db_path) as conn:
            conn.execute(
                "UPDATE images SET notes = ? WHERE id = ?",
                ("updated note", 11),
            )
            conn.commit()
    elif mutation_name == "file":
        ctx.image_path.write_bytes(b"changed-image-bytes")
    elif mutation_name == "crop":
        with sqlite3.connect(ctx.db_path) as conn:
            conn.execute(
                "UPDATE images SET crop_mode = ? WHERE id = ?",
                ("custom", 11),
            )
            conn.commit()
    elif mutation_name == "render":
        cloud_sync.SettingsDB.set_setting(cloud_sync._SETTING_IMAGE_LICENSE, "61")
    else:
        raise AssertionError(mutation_name)

    image_prep_calls: list[tuple[int, str]] = []
    prepare_cb_calls: list[int] = []
    measurement_push_calls: list[int] = []

    def prepare_images_cb(obs, progress_cb):
        prepare_cb_calls.append(int(obs["id"]))
        return (
            [
                {
                    "image_row": {
                        "id": 11,
                        "cloud_id": "cloud-image-11",
                        "filepath": str(ctx.image_path),
                        "image_type": "field",
                        "sort_order": 0,
                    },
                    "upload_path": str(ctx.image_path),
                }
            ],
            None,
            [],
        )

    def fake_push_images_for_observation(client, obs, cloud_id, *, prepare_images_cb=None, **kwargs):
        image_prep_calls.append((int(obs["id"]), str(cloud_id)))
        assert callable(prepare_images_cb)
        prepared_items, _cleanup, _warnings = prepare_images_cb(obs, kwargs.get("progress_cb"))
        assert prepared_items
        return True

    monkeypatch.setattr(cloud_sync, "_push_images_for_observation", fake_push_images_for_observation)
    monkeypatch.setattr(
        cloud_sync,
        "_push_measurements_for_observation",
        lambda *args, **kwargs: measurement_push_calls.append(int(args[1])),
    )

    result = cloud_sync.push_all(
        ctx.client,
        progress_cb=lambda text, current, total: ctx.progress_messages.append(text),
        remote_obs=[dict(ctx.remote_obs)],
        sync_images=True,
        sync_calibrations=False,
        prepare_images_cb=prepare_images_cb,
    )

    output = capsys.readouterr().out

    assert result["pushed"] == 1
    assert result["errors"] == []
    assert image_prep_calls == [(1, "cloud-obs-1")]
    assert prepare_cb_calls == [1]
    assert measurement_push_calls == [1]
    assert ctx.refresh_calls == [1]
    assert "image prep diagnostics" in output
    assert "decision=full image prep" in output
    assert "changed_keys=[" in output
    if mutation_name == "metadata":
        assert "image_file_signature_changed=False" in output
        assert "render_affecting_field_changed=False" in output
        assert "only_metadata_fields_changed=True" in output
        assert "notes" in output
    elif mutation_name == "file":
        assert "image_file_signature_changed=True" in output
    elif mutation_name in {"crop", "render"}:
        assert "render_affecting_field_changed=True" in output
    assert not any("no prepared upload candidates" in message for message in ctx.progress_messages)
    assert "measurements pushed" in output


def test_push_all_prepares_images_for_new_image(
    tmp_path,
    monkeypatch,
    capsys,
):
    ctx = _setup_push_all_existing_media_case(tmp_path, monkeypatch)
    new_image_path = tmp_path / "new-image.jpg"
    new_image_path.write_bytes(b"new-image-bytes")
    with sqlite3.connect(ctx.db_path) as conn:
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath,
                image_type, source_role, file_purpose, sort_order, created_at,
                synced_at, notes, crop_mode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                12,
                1,
                None,
                str(new_image_path),
                None,
                "field",
                "local_canonical",
                "field",
                1,
                "2026-05-01T10:10:00Z",
                None,
                None,
                "full",
            ),
        )
        conn.commit()

    image_prep_calls: list[tuple[int, str]] = []
    prepare_cb_calls: list[int] = []
    measurement_push_calls: list[int] = []

    def prepare_images_cb(obs, progress_cb):
        prepare_cb_calls.append(int(obs["id"]))
        return (
            [
                {
                    "image_row": {
                        "id": 11,
                        "cloud_id": "cloud-image-11",
                        "filepath": str(ctx.image_path),
                        "image_type": "field",
                        "sort_order": 0,
                    },
                    "upload_path": str(ctx.image_path),
                },
                {
                    "image_row": {
                        "id": 12,
                        "cloud_id": None,
                        "filepath": str(new_image_path),
                        "image_type": "field",
                        "sort_order": 1,
                    },
                    "upload_path": str(new_image_path),
                },
            ],
            None,
            [],
        )

    def fake_push_images_for_observation(client, obs, cloud_id, *, prepare_images_cb=None, **kwargs):
        image_prep_calls.append((int(obs["id"]), str(cloud_id)))
        assert callable(prepare_images_cb)
        prepared_items, _cleanup, _warnings = prepare_images_cb(obs, kwargs.get("progress_cb"))
        assert len(prepared_items) == 2
        return True

    monkeypatch.setattr(cloud_sync, "_push_images_for_observation", fake_push_images_for_observation)
    monkeypatch.setattr(
        cloud_sync,
        "_push_measurements_for_observation",
        lambda *args, **kwargs: measurement_push_calls.append(int(args[1])),
    )

    result = cloud_sync.push_all(
        ctx.client,
        progress_cb=lambda text, current, total: ctx.progress_messages.append(text),
        remote_obs=[dict(ctx.remote_obs)],
        sync_images=True,
        sync_calibrations=False,
        prepare_images_cb=prepare_images_cb,
    )

    output = capsys.readouterr().out

    assert result["pushed"] == 1
    assert result["errors"] == []
    assert image_prep_calls == [(1, "cloud-obs-1")]
    assert prepare_cb_calls == [1]
    assert measurement_push_calls == [1]
    assert ctx.refresh_calls == [1]
    assert not any("no prepared upload candidates" in message for message in ctx.progress_messages)
    assert "measurements pushed" in output


def test_push_all_skips_image_prep_for_tombstone_only_cleanup(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.setenv("SPORELY_DEBUG_CLOUD_SYNC", "1")
    ctx = _setup_push_all_tombstone_cleanup_case(
        tmp_path,
        monkeypatch,
        include_deleted_measurement=False,
    )

    measurement_push_calls: list[int] = []
    real_push_images_for_observation = cloud_sync._push_images_for_observation

    def fake_push_images_for_observation(client, obs, cloud_id, *, prepare_images_cb=None, **kwargs):
        assert prepare_images_cb is None
        return real_push_images_for_observation(
            client,
            obs,
            cloud_id,
            prepare_images_cb=prepare_images_cb,
            **kwargs,
        )

    monkeypatch.setattr(cloud_sync, "_push_images_for_observation", fake_push_images_for_observation)
    monkeypatch.setattr(
        cloud_sync,
        "_push_measurements_for_observation",
        lambda *args, **kwargs: measurement_push_calls.append(int(args[1])),
    )

    result = cloud_sync.push_all(
        ctx.client,
        progress_cb=lambda text, current, total: ctx.progress_messages.append(text),
        remote_obs=[dict(ctx.remote_obs)],
        sync_images=True,
        sync_calibrations=False,
    )

    output = capsys.readouterr().out
    conn = sqlite3.connect(ctx.db_path)
    try:
        tombstone_row = conn.execute(
            """
            SELECT delete_synced_at
            FROM image_tombstones
            WHERE deleted_cloud_id = ?
            """,
            ("cloud-image-11",),
        ).fetchone()
    finally:
        conn.close()

    assert result["pushed"] == 1
    assert result["errors"] == []
    assert measurement_push_calls == [1]
    assert ctx.refresh_calls == [1]
    assert ctx.client.soft_delete_calls and ctx.client.soft_delete_calls[0][0] == "cloud-image-11"
    assert ctx.client.upload_image_calls == []
    assert tombstone_row is not None and tombstone_row[0] is not None
    assert "image prep diagnostics" in output
    assert "decision=metadata-only image sync" in output
    assert "tombstone_aware_signature_matched=True" in output
    assert any("reason=tombstone_only" in message for message in ctx.progress_messages)
    assert any("metadata-only image sync" in message for message in ctx.progress_messages)
    assert "measurements pushed" in output


def test_push_all_skips_image_prep_for_deleted_image_measurement_cleanup(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.setenv("SPORELY_DEBUG_CLOUD_SYNC", "1")
    ctx = _setup_push_all_tombstone_cleanup_case(
        tmp_path,
        monkeypatch,
        include_deleted_measurement=True,
    )

    real_push_images_for_observation = cloud_sync._push_images_for_observation

    def fake_push_images_for_observation(client, obs, cloud_id, *, prepare_images_cb=None, **kwargs):
        assert prepare_images_cb is None
        return real_push_images_for_observation(
            client,
            obs,
            cloud_id,
            prepare_images_cb=prepare_images_cb,
            **kwargs,
        )

    monkeypatch.setattr(cloud_sync, "_push_images_for_observation", fake_push_images_for_observation)

    result = cloud_sync.push_all(
        ctx.client,
        progress_cb=lambda text, current, total: ctx.progress_messages.append(text),
        remote_obs=[dict(ctx.remote_obs)],
        sync_images=True,
        sync_calibrations=False,
    )

    output = capsys.readouterr().out
    conn = sqlite3.connect(ctx.db_path)
    try:
        tombstone_row = conn.execute(
            """
            SELECT delete_synced_at
            FROM image_tombstones
            WHERE deleted_cloud_id = ?
            """,
            ("cloud-image-11",),
        ).fetchone()
    finally:
        conn.close()

    assert result["pushed"] == 1
    assert result["errors"] == []
    assert ctx.client.upload_image_calls == []
    assert ctx.client.measurement_push_calls == []
    assert ctx.client.soft_delete_calls and ctx.client.soft_delete_calls[0][0] == "cloud-image-11"
    assert tombstone_row is not None and tombstone_row[0] is not None
    assert "image prep diagnostics" in output
    assert "decision=metadata-only image sync" in output
    assert "tombstone_aware_signature_matched=True" in output
    assert any("reason=tombstone_only" in message for message in ctx.progress_messages)
    assert "skipped cloud measurement 21 because cloud image cloud-image-11 has a local tombstone" in output
    assert "measurements pushed" in output


def test_get_conflict_detail_ignores_file_size_differences(monkeypatch):
    local_obs = {
        "id": 1,
        "cloud_id": "cloud-obs-1",
        "date": "2026-01-01",
        "genus": "Agaricus",
        "species": "campestris",
        "location": "Field edge",
        "sharing_scope": "private",
        "location_public": 0,
        "is_draft": True,
        "location_precision": "exact",
    }
    remote_obs = dict(local_obs)
    baseline_snapshot = {
        "observation": dict(remote_obs),
        "images": [
            {
                "desktop_id": 10,
                "sort_order": 0,
                "image_type": "field",
                "original_filename": "same.jpg",
                "stored_bytes": 1024,
            }
        ],
    }
    local_images = [
        {
            "id": 10,
            "cloud_id": "cloud-img-1",
            "sort_order": 0,
            "image_type": "field",
            "original_filename": "same.jpg",
            "stored_bytes": 2048,
            "filepath": "/tmp/same.jpg",
        }
    ]
    remote_images = [
        {
            "id": "cloud-img-1",
            "desktop_id": 10,
            "sort_order": 0,
            "image_type": "field",
            "original_filename": "same.jpg",
            "stored_bytes": 1024,
        }
    ]

    monkeypatch.setattr(cloud_sync.ObservationDB, "get_observation", lambda local_id: dict(local_obs))
    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation", lambda local_id: [dict(row) for row in local_images])
    monkeypatch.setattr(cloud_sync.MeasurementDB, "get_measurements_for_observation", lambda local_id: [])
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: json.dumps(baseline_snapshot))

    class DummyClient:
        def get_observation(self, cloud_id):
            return dict(remote_obs)

        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            return [dict(row) for row in remote_images]

    detail = cloud_sync.get_conflict_detail(DummyClient(), 1, "cloud-obs-1")

    assert detail["image_mismatches"] == []


def test_summarize_image_changes_ignores_upload_size_metadata_and_matches_cloud_id():
    baseline = [
        {
            "id": "cloud-img-1",
            "desktop_id": 10,
            "sort_order": 0,
            "image_type": "field",
            "original_filename": "same.jpg",
            "measure_color": "blue",
            "upload_mode": "full",
            "source_width": 4032,
            "source_height": 3024,
            "stored_width": 4032,
            "stored_height": 3024,
            "stored_bytes": 123456,
        }
    ]
    current = [
        {
            "id": "cloud-img-1",
            "desktop_id": 11,
            "sort_order": 0,
            "image_type": "field",
            "original_filename": "same.jpg",
            "measure_color": "blue",
            "upload_mode": "reduced",
            "source_width": 2000,
            "source_height": 1500,
            "stored_width": 2000,
            "stored_height": 1500,
            "stored_bytes": 45678,
        }
    ]

    assert cloud_sync._summarize_image_changes(current, baseline) == []


def test_import_remote_images_preserves_original_filename(monkeypatch, tmp_path):
    temp_root = tmp_path / "sync"
    temp_root.mkdir()
    captured = {}

    class DummyClient:
        def download_image_file(self, storage_path, dest_path):
            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"fake image")

    class DummyConn:
        def execute(self, *args, **kwargs):
            return self

        def commit(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "from_stored_credentials", lambda: DummyClient())
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_rename_to_detected_image_extension", lambda path: Path(path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: DummyConn())
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(":memory:"))
    monkeypatch.setattr(cloud_sync.tempfile, "mkdtemp", lambda prefix=None: str(temp_root))
    monkeypatch.setattr(
        cloud_sync.ImageDB,
        "add_image",
        lambda **kwargs: (captured.setdefault("filepath", kwargs["filepath"]) or 1) and 1,
    )

    cloud_sync._import_remote_images(
        {"id": "cloud-obs-1", "genus": "Flammulina", "species": "velutipes"},
        1,
        "cloud-obs-1",
        remote_images=[
            {
                "id": "cloud-img-1",
                "storage_path": "user/cloud-obs-1/cloud-img-1_cloud_1.jpg",
                "original_filename": "cloud_1.jpg",
                "image_type": "field",
            }
        ],
    )

    assert Path(captured["filepath"]).name == "cloud_1.jpg"


def test_import_remote_images_preserves_metadata_and_sets_desktop_id(monkeypatch, tmp_path):
    temp_root = tmp_path / "sync-meta"
    temp_root.mkdir()
    captured = {}
    desktop_id_calls = []
    conn_statements = []

    class DummyClient:
        def download_image_file(self, storage_path, dest_path):
            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"cloud image bytes")

        def set_image_desktop_id(self, cloud_image_id, desktop_id):
            desktop_id_calls.append((cloud_image_id, desktop_id))

    class DummyConn:
        def execute(self, sql, params=()):
            conn_statements.append((sql, tuple(params)))
            return self

        def commit(self):
            pass

        def close(self):
            pass

    def fake_add_image(**kwargs):
        captured.update(kwargs)
        return 23

    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "from_stored_credentials", lambda: DummyClient())
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_rename_to_detected_image_extension", lambda path: Path(path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: DummyConn())
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(":memory:"))
    monkeypatch.setattr(cloud_sync.tempfile, "mkdtemp", lambda prefix=None: str(temp_root))
    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", fake_add_image)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)

    cloud_sync._import_remote_images(
        {"id": "cloud-obs-1", "genus": "Flammulina", "species": "velutipes"},
        1,
        "cloud-obs-1",
        remote_images=[
            {
                "id": "cloud-img-1",
                "storage_path": "user/cloud-obs-1/cloud-img-1_cloud_1.jpg",
                "original_filename": "cloud_1.jpg",
                "image_type": "field",
                "sort_order": 4,
                "micro_category": "Spores",
                "objective_name": "100X Plan achro",
                "scale_microns_per_pixel": 0.0315,
                "resample_scale_factor": 2.0,
                "mount_medium": "Water",
                "stain": "Congo Red",
                "sample_type": "spores",
                "contrast": "BF",
                "measure_color": "blue",
                "crop_mode": "manual",
                "notes": "cloud note",
                "gps_source": True,
                "ai_crop_x1": 1.0,
                "ai_crop_y1": 2.0,
                "ai_crop_x2": 3.0,
                "ai_crop_y2": 4.0,
                "ai_crop_source_w": 640,
                "ai_crop_source_h": 480,
                "ai_crop_is_custom": True,
                "captured_at": "2026-05-01T12:34:56Z",
            }
        ],
    )

    assert captured["scale"] == 0.0315
    assert captured["notes"] == "cloud note"
    assert captured["micro_category"] == "Spores"
    assert captured["objective_name"] == "100X Plan achro"
    assert captured["measure_color"] == "blue"
    assert captured["mount_medium"] == "Water"
    assert captured["stain"] == "Congo Red"
    assert captured["sample_type"] == "spores"
    assert captured["contrast"] == "BF"
    assert captured["sort_order"] == 4
    assert captured["crop_mode"] == "manual"
    assert captured["gps_source"] is True
    assert captured["resample_scale_factor"] == 2.0
    assert captured["ai_crop_box"] == (1.0, 2.0, 3.0, 4.0)
    assert captured["ai_crop_source_size"] == (640, 480)
    assert captured["ai_crop_is_custom"] is True
    assert captured["captured_at"] == "2026-05-01T12:34:56Z"
    assert captured["copy_to_folder"] is True
    assert captured["mark_observation_dirty"] is False
    assert captured["source_role"] == "cloud_recovery_cache"
    assert captured["file_purpose"] == "cache"
    assert captured["original_mime_type"] is None
    assert captured["working_mime_type"] == "image/jpeg"
    assert desktop_id_calls == [("cloud-img-1", 23)]
    assert any(
        sql == "UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?"
        and params[0] == "cloud-img-1"
        and params[2] == 23
        for sql, params in conn_statements
    )


def test_import_remote_images_leaves_working_mime_type_null_for_unknown_extension(monkeypatch, tmp_path):
    temp_root = tmp_path / "sync-unknown-mime"
    temp_root.mkdir()
    captured = {}
    desktop_id_calls = []

    class DummyClient:
        def download_image_file(self, storage_path, dest_path):
            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"cloud image bytes")

        def set_image_desktop_id(self, cloud_image_id, desktop_id):
            desktop_id_calls.append((cloud_image_id, desktop_id))

    class DummyConn:
        def execute(self, *args, **kwargs):
            return self

        def commit(self):
            pass

        def close(self):
            pass

    def fake_add_image(**kwargs):
        captured.update(kwargs)
        return 24

    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "from_stored_credentials", lambda: DummyClient())
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_rename_to_detected_image_extension", lambda path: Path(path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: DummyConn())
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(":memory:"))
    monkeypatch.setattr(cloud_sync.tempfile, "mkdtemp", lambda prefix=None: str(temp_root))
    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", fake_add_image)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)

    cloud_sync._import_remote_images(
        {"id": "cloud-obs-1", "genus": "Flammulina", "species": "velutipes"},
        1,
        "cloud-obs-1",
        remote_images=[
            {
                "id": "cloud-img-2",
                "storage_path": "user/cloud-obs-1/cloud-img-2_cloud_1.unknown",
                "original_filename": "cloud_1.unknown",
                "image_type": "field",
            }
        ],
    )

    assert captured["source_role"] == "cloud_recovery_cache"
    assert captured["file_purpose"] == "cache"
    assert captured["original_mime_type"] is None
    assert captured["working_mime_type"] is None
    assert desktop_id_calls == [("cloud-img-2", 24)]


def test_pull_all_existing_observation_records_remote_deleted_rows(monkeypatch, tmp_path):
    db_path = _init_tombstone_sync_db(tmp_path)
    temp_root = tmp_path / "pull-all"
    temp_root.mkdir()

    existing_image_path = tmp_path / "images" / "observation-1" / "existing.jpg"
    existing_image_path.parent.mkdir(parents=True, exist_ok=True)
    existing_image_path.write_text("existing", encoding="utf-8")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath, image_type
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (11, 1, None, str(existing_image_path), None, "field"),
        )
        conn.commit()
    finally:
        conn.close()

    bulk_calls: list[tuple[str, ...]] = []
    download_calls: list[str] = []
    add_calls: list[dict] = []

    class DummyClient:
        def pull_bulk_image_metadata(self, obs_cloud_ids):
            bulk_calls.append(tuple(obs_cloud_ids))
            return [
                {
                    "id": "cloud-image-deleted",
                    "observation_id": "cloud-obs-1",
                    "deleted_at": "2026-05-01 10:00:00",
                    "storage_path": "user/cloud-obs-1/cloud-image-deleted.jpg",
                    "original_filename": "deleted.jpg",
                    "image_type": "field",
                    "sort_order": 0,
                },
                {
                    "id": "cloud-image-active",
                    "observation_id": "cloud-obs-1",
                    "deleted_at": None,
                    "storage_path": "user/cloud-obs-1/cloud-image-active.jpg",
                    "original_filename": "active.jpg",
                    "image_type": "field",
                    "sort_order": 1,
                },
            ]

        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            raise AssertionError("pull_image_metadata should not be called when bulk image metadata is available")

        def set_desktop_id(self, *args, **kwargs):
            pass

        def download_image_file(self, storage_path, dest_path):
            download_calls.append(storage_path)
            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"cloud image bytes")

    def fake_add_image(**kwargs):
        add_calls.append(dict(kwargs))
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.execute(
                """
                INSERT INTO images (observation_id, filepath, image_type, sort_order)
                VALUES (?, ?, ?, ?)
                """,
                (
                    kwargs["observation_id"],
                    kwargs["filepath"],
                    kwargs["image_type"],
                    kwargs.get("sort_order"),
                ),
            )
            local_id = cursor.lastrowid
            conn.commit()
            return local_id
        finally:
            conn.close()

    monkeypatch.setattr(cloud_sync, "_backfill_missing_exif_on_cloud_images", lambda: None)
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: "")
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda observation_id: "")
    monkeypatch.setattr(cloud_sync, "_local_cloud_media_signature", lambda observation_id: "")
    monkeypatch.setattr(cloud_sync, "_store_local_media_signature_if_equivalent", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *args, **kwargs: [])
    monkeypatch.setattr(cloud_sync, "_detect_deleted_remote_observations", lambda remote_obs: [])
    monkeypatch.setattr(cloud_sync, "_apply_remote_observation_fields", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_rename_to_detected_image_extension", lambda path: Path(path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync.tempfile, "mkdtemp", lambda prefix=None: str(temp_root))
    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", fake_add_image)

    result = cloud_sync.pull_all(
        DummyClient(),
        remote_obs=[
            {
                "id": "cloud-obs-1",
                "desktop_id": 1,
                "date": "2026-05-01",
                "genus": "Flammulina",
                "species": "velutipes",
            }
        ],
        sync_calibrations=False,
    )

    conn = sqlite3.connect(db_path)
    try:
        tombstone = conn.execute(
            """
            SELECT deleted_cloud_id, deleted_at, deleted_storage_path,
                   deleted_observation_cloud_id, local_observation_id
            FROM image_tombstones
            WHERE deleted_cloud_id = ?
            """,
            ("cloud-image-deleted",),
        ).fetchone()
        images = conn.execute(
            "SELECT id, cloud_id, filepath FROM images ORDER BY id"
        ).fetchall()
        observation = conn.execute(
            "SELECT cloud_id, sync_status FROM observations WHERE id = ?",
            (1,),
        ).fetchone()
    finally:
        conn.close()

    assert result["pulled"] == 1
    assert bulk_calls == [("cloud-obs-1",)]
    assert download_calls == ["user/cloud-obs-1/cloud-image-active.jpg"]
    assert len(add_calls) == 1
    assert add_calls[0]["source_role"] == "cloud_recovery_cache"
    assert add_calls[0]["file_purpose"] == "cache"
    assert add_calls[0]["original_mime_type"] is None
    assert add_calls[0]["working_mime_type"] == "image/jpeg"
    assert len(images) == 2
    assert tombstone == (
        "cloud-image-deleted",
        "2026-05-01 10:00:00",
        "user/cloud-obs-1/cloud-image-deleted.jpg",
        "cloud-obs-1",
        1,
    )
    assert images[0][0] == 11
    assert images[0][1] is None
    assert images[1][1] == "cloud-image-active"
    assert existing_image_path.exists()
    assert observation[0] == "cloud-obs-1"
    assert observation[1] == "synced"


@pytest.mark.parametrize("linked_by_cloud_id", [True, False])
def test_get_conflict_detail_ignores_tombstoned_image_changes_for_cloud_id_and_desktop_id(
    monkeypatch,
    linked_by_cloud_id,
):
    local_obs = {
        "id": 1,
        "cloud_id": "cloud-obs-1",
        "date": "2026-01-01",
        "genus": "Flammulina",
        "species": "velutipes",
        "location": "Field edge",
        "sharing_scope": "private",
        "location_public": 0,
        "is_draft": True,
        "location_precision": "exact",
    }
    remote_obs = dict(local_obs)
    baseline_snapshot = {
        "observation": dict(remote_obs),
        "images": [
            {
                "id": "cloud-image-1",
                "desktop_id": 11,
                "sort_order": 0,
                "image_type": "field",
                "original_filename": "image.jpg",
                "ai_crop_x1": 0,
                "ai_crop_y1": 0,
                "ai_crop_x2": 100,
                "ai_crop_y2": 100,
                "ai_crop_source_w": 100,
                "ai_crop_source_h": 100,
                "ai_crop_is_custom": False,
            }
        ],
    }
    local_images = [
        {
            "id": 11,
            "cloud_id": "cloud-image-1" if linked_by_cloud_id else None,
            "filepath": "/tmp/image.jpg",
            "image_type": "field",
            "sort_order": 0,
            "original_filename": "image.jpg",
            "ai_crop_x1": 5,
            "ai_crop_y1": 6,
            "ai_crop_x2": 95,
            "ai_crop_y2": 94,
            "ai_crop_source_w": 100,
            "ai_crop_source_h": 100,
            "ai_crop_is_custom": True,
        }
    ]
    remote_images = [
        {
            "id": "cloud-image-1",
            "desktop_id": 11,
            "sort_order": 0,
            "image_type": "field",
            "original_filename": "image.jpg",
            "deleted_at": "2026-05-01 10:00:00",
            "storage_path": "user/cloud-obs-1/0_1780049894442.webp",
        }
    ]

    monkeypatch.setattr(cloud_sync.ObservationDB, "get_observation", lambda local_id: dict(local_obs))
    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation", lambda local_id: [dict(row) for row in local_images])
    monkeypatch.setattr(cloud_sync.MeasurementDB, "get_measurements_for_observation", lambda local_id: [])
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: json.dumps(baseline_snapshot))

    class DummyClient:
        def get_observation(self, cloud_id):
            return dict(remote_obs)

        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            return [dict(row) for row in remote_images]

    detail = cloud_sync.get_conflict_detail(DummyClient(), 1, "cloud-obs-1")

    assert detail["field_rows"] == []
    assert detail["image_mismatches"] == []
    assert detail["local_image_changes"] == []
    assert detail["remote_image_changes"] == []


def test_pull_all_ignores_cloud_tombstone_even_when_local_media_changed(monkeypatch, tmp_path):
    db_path = _init_tombstone_sync_db(tmp_path)
    images_root = tmp_path / "images"
    images_root.mkdir()
    image_path = images_root / "image.jpg"
    image_path.write_bytes(b"image-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath, image_type, sort_order,
                ai_crop_x1, ai_crop_y1, ai_crop_x2, ai_crop_y2, ai_crop_source_w, ai_crop_source_h,
                ai_crop_is_custom, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                11,
                1,
                "cloud-image-1",
                str(image_path),
                None,
                "field",
                0,
                0,
                0,
                100,
                100,
                100,
                100,
                0,
                "2026-05-01T00:00:00Z",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    baseline_snapshot = {
        "observation": {
            "id": "cloud-obs-1",
            "desktop_id": 1,
            "date": "2026-01-01",
            "genus": "Flammulina",
            "species": "velutipes",
            "location": "Field edge",
            "sharing_scope": "private",
            "location_public": 0,
            "is_draft": True,
            "location_precision": "exact",
        },
        "images": [
            {
                "id": "cloud-image-1",
                "desktop_id": 11,
                "sort_order": 0,
                "image_type": "field",
                "original_filename": "image.jpg",
                "ai_crop_x1": 0,
                "ai_crop_y1": 0,
                "ai_crop_x2": 100,
                "ai_crop_y2": 100,
                "ai_crop_source_w": 100,
                "ai_crop_source_h": 100,
                "ai_crop_is_custom": False,
            }
        ],
        "measurements": [],
    }

    bulk_calls: list[tuple[str, ...]] = []

    class DummyClient:
        def pull_bulk_image_metadata(self, obs_cloud_ids):
            bulk_calls.append(tuple(obs_cloud_ids))
            return [
                {
                    "id": "cloud-image-1",
                    "desktop_id": 1,
                    "observation_id": "cloud-obs-1",
                    "deleted_at": "2026-05-29 10:22:16.824+00",
                    "storage_path": "8c471394-b274-4933-b830-59805820d93c/614/0_1780049894442.webp",
                    "original_filename": "image.jpg",
                    "image_type": "field",
                    "sort_order": 0,
                }
            ]

        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            raise AssertionError("pull_image_metadata should not be called when bulk image metadata is available")

        def set_desktop_id(self, *args, **kwargs):
            pass

    monkeypatch.setattr(cloud_sync, "_backfill_missing_exif_on_cloud_images", lambda: None)
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: json.dumps(baseline_snapshot))
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda observation_id: "old-signature")
    monkeypatch.setattr(cloud_sync, "_local_cloud_media_signature", lambda observation_id: "new-signature")
    monkeypatch.setattr(cloud_sync, "_store_local_media_signature_if_equivalent", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *args, **kwargs: [])
    monkeypatch.setattr(cloud_sync, "_detect_deleted_remote_observations", lambda remote_obs: [])
    monkeypatch.setattr(cloud_sync, "_apply_remote_observation_fields", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_rename_to_detected_image_extension", lambda path: Path(path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    result = cloud_sync.pull_all(
        DummyClient(),
        remote_obs=[
            {
                "id": "cloud-obs-1",
                "desktop_id": 1,
                "date": "2026-01-01",
                "genus": "Flammulina",
                "species": "velutipes",
            }
        ],
        sync_calibrations=False,
    )

    conn = sqlite3.connect(db_path)
    try:
        tombstone = conn.execute(
            """
            SELECT deleted_cloud_id, deleted_at, local_observation_id, local_image_id
            FROM image_tombstones
            WHERE deleted_cloud_id = ?
            """,
            ("cloud-image-1",),
        ).fetchone()
        image_row = conn.execute(
            "SELECT COUNT(*) FROM images WHERE id = ?",
            (11,),
        ).fetchone()[0]
        observation = conn.execute(
            "SELECT cloud_id, sync_status FROM observations WHERE id = ?",
            (1,),
        ).fetchone()
    finally:
        conn.close()

    assert result["errors"] == []
    assert result["pulled"] == 1
    assert bulk_calls == [("cloud-obs-1",)]
    assert tombstone == ("cloud-image-1", "2026-05-29 10:22:16", 1, 11)
    assert image_row == 1
    assert observation == ("cloud-obs-1", "dirty")


def test_pull_all_records_unmatched_cloud_tombstone_without_conflict(monkeypatch, tmp_path):
    db_path = _init_tombstone_sync_db(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.commit()
    finally:
        conn.close()

    baseline_snapshot = {
        "observation": {
            "id": "cloud-obs-1",
            "desktop_id": 1,
            "date": "2026-01-01",
            "genus": "Flammulina",
            "species": "velutipes",
            "location": "Field edge",
            "sharing_scope": "private",
            "location_public": 0,
            "is_draft": True,
            "location_precision": "exact",
        },
        "images": [
            {
                "id": "cloud-image-1",
                "desktop_id": 11,
                "sort_order": 0,
                "image_type": "field",
                "original_filename": "image.jpg",
            }
        ],
        "measurements": [],
    }

    bulk_calls: list[tuple[str, ...]] = []

    class DummyClient:
        def pull_bulk_image_metadata(self, obs_cloud_ids):
            bulk_calls.append(tuple(obs_cloud_ids))
            return [
                {
                    "id": "cloud-image-1",
                    "desktop_id": 11,
                    "observation_id": "cloud-obs-1",
                    "deleted_at": "2026-05-29 10:22:16.824+00",
                    "storage_path": "8c471394-b274-4933-b830-59805820d93c/614/0_1780049894442.webp",
                    "original_filename": "image.jpg",
                    "image_type": "field",
                    "sort_order": 0,
                }
            ]

        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            raise AssertionError("pull_image_metadata should not be called when bulk image metadata is available")

        def set_desktop_id(self, *args, **kwargs):
            pass

    monkeypatch.setattr(cloud_sync, "_backfill_missing_exif_on_cloud_images", lambda: None)
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: json.dumps(baseline_snapshot))
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda observation_id: "")
    monkeypatch.setattr(cloud_sync, "_local_cloud_media_signature", lambda observation_id: "")
    monkeypatch.setattr(cloud_sync, "_store_local_media_signature_if_equivalent", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *args, **kwargs: [])
    monkeypatch.setattr(cloud_sync, "_detect_deleted_remote_observations", lambda remote_obs: [])
    monkeypatch.setattr(cloud_sync, "_apply_remote_observation_fields", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_rename_to_detected_image_extension", lambda path: Path(path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    result = cloud_sync.pull_all(
        DummyClient(),
        remote_obs=[
            {
                "id": "cloud-obs-1",
                "desktop_id": 1,
                "date": "2026-01-01",
                "genus": "Flammulina",
                "species": "velutipes",
            }
        ],
        sync_calibrations=False,
    )

    conn = sqlite3.connect(db_path)
    try:
        tombstone = conn.execute(
            """
            SELECT deleted_cloud_id, deleted_at, local_observation_id, local_image_id
            FROM image_tombstones
            WHERE deleted_cloud_id = ?
            """,
            ("cloud-image-1",),
        ).fetchone()
    finally:
        conn.close()

    assert result["errors"] == []
    assert result["pulled"] == 1
    assert bulk_calls == [("cloud-obs-1",)]
    assert tombstone == ("cloud-image-1", "2026-05-29 10:22:16", 1, None)


def test_push_images_for_observation_skips_tombstoned_local_image_id(monkeypatch, tmp_path, capsys):
    db_path = _init_tombstone_sync_db(tmp_path)
    images_root = tmp_path / "images"
    images_root.mkdir()
    image_path = images_root / "image.jpg"
    image_path.write_bytes(b"image-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath, image_type, sort_order
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (11, 1, None, str(image_path), None, "field", 0),
        )
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?)
            """,
            ("cloud-image-1", "2026-05-01 10:00:00", 1, 11),
        )
        conn.commit()
    finally:
        conn.close()

    pushed_ids: list[int] = []
    uploaded_paths: list[str] = []

    client = cloud_sync.SporelyCloudClient("token", "user-123")
    monkeypatch.setattr(client, "pull_image_metadata", lambda obs_cloud_id: [])
    monkeypatch.setattr(client, "_observation_images_support_ai_crop", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_ai_crop_custom", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_upload_metadata", lambda: False)
    monkeypatch.setattr(client, "_patch", lambda *args, **kwargs: None)

    def fake_push_image_metadata(img, obs_cloud_id, storage_path):
        pushed_ids.append(int(img["id"]))
        return f"cloud-image-{int(img['id'])}"

    def fake_upload_image_file(local_path, obs_cloud_id, img_cloud_id, storage_path=None, upload_meta=None):
        uploaded_paths.append(str(local_path))
        return storage_path

    def prepare_images_cb(obs, progress_cb):
        return (
            [
                {
                    "image_row": {
                        "id": 11,
                        "cloud_id": None,
                        "filepath": str(image_path),
                        "image_type": "field",
                        "sort_order": 0,
                    },
                    "upload_path": str(image_path),
                }
            ],
            None,
            [],
        )

    monkeypatch.setattr(client, "push_image_metadata", fake_push_image_metadata)
    monkeypatch.setattr(client, "upload_image_file", fake_upload_image_file)
    monkeypatch.setattr(cloud_sync, "_push_pending_image_tombstones", lambda client: [])
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    result = cloud_sync._push_images_for_observation(
        client,
        {"id": 1},
        "cloud-obs-1",
        prepare_images_cb=prepare_images_cb,
    )

    output = capsys.readouterr().out

    assert result is True
    assert pushed_ids == []
    assert uploaded_paths == []
    assert "skipped local image 11 because it has a local tombstone" in output


def test_mark_cloud_observations_dirty_for_pending_local_images_counts_re_dirtied_observations(
    monkeypatch,
    tmp_path,
    capsys,
):
    db_path = _init_push_all_sync_db(tmp_path)
    pending_image = tmp_path / "pending.jpg"
    pending_image.write_bytes(b"pending-field-image")
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (434, "cloud-obs-434", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, image_type, source_role, file_purpose
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (1, 434, None, str(pending_image), "field", "local_canonical", "field"),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    summary = cloud_sync._new_sync_summary()
    with cloud_sync._cloud_sync_summary_scope(summary):
        cloud_sync._mark_cloud_observations_dirty_for_pending_local_images()

    output = capsys.readouterr().out
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT sync_status FROM observations WHERE id = 434"
        ).fetchone()
    finally:
        conn.close()

    assert row == ("dirty",)
    assert summary["observations_redirtied_pending_local_images"] == 1
    assert summary["images_uploaded"] == 0
    assert "re-dirtied because 1 cloud-eligible local image row(s) still have cloud_id IS NULL" in output


def test_summarize_sync_issues_still_flags_real_conflicts():
    summary = cloud_sync.summarize_sync_issues(
        [
            "cloud cloud-obs-1: skipped remote update because local observation 1 has unsynced desktop edits"
        ]
    )

    assert summary["conflict_count"] == 1
    assert summary["other_count"] == 0
    assert summary["conflicts"][0]["local_id"] == 1
    assert summary["conflicts"][0]["cloud_id"] == "cloud-obs-1"


def test_import_remote_images_records_deleted_rows_and_imports_only_active_rows(
    monkeypatch,
    tmp_path,
):
    db_path = _init_tombstone_sync_db(tmp_path)
    temp_root = tmp_path / "sync-tombstones"
    temp_root.mkdir()
    download_calls: list[str] = []
    fetched_calls: list[tuple[str, bool]] = []
    add_calls: list[dict] = []

    images_root = tmp_path / "images"
    image_path = images_root / "observation-1" / "image.jpg"
    original_path = images_root / "observation-1" / "originals" / "image-original.jpg"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    original_path.parent.mkdir(parents=True, exist_ok=True)
    image_path.write_text("image", encoding="utf-8")
    original_path.write_text("original", encoding="utf-8")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status) VALUES (?, ?, ?)",
            (1, "cloud-obs-1", "synced"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath, image_type
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (11, 1, "cloud-image-1", str(image_path), str(original_path), "field"),
        )
        conn.commit()
    finally:
        conn.close()

    class DummyClient:
        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            fetched_calls.append((cloud_id, include_deleted_for_sync))
            return [
                {
                    "id": "cloud-image-1",
                    "observation_id": "cloud-obs-1",
                    "deleted_at": "2026-05-01 10:00:00",
                    "storage_path": "user/cloud-obs-1/cloud-image-1_deleted.jpg",
                    "original_filename": "deleted.jpg",
                    "image_type": "field",
                },
                {
                    "id": "cloud-image-2",
                    "observation_id": "cloud-obs-1",
                    "deleted_at": None,
                    "storage_path": "user/cloud-obs-1/cloud-image-2_kept.jpg",
                    "original_filename": "kept.jpg",
                    "image_type": "field",
                },
            ]

        def download_image_file(self, storage_path, dest_path):
            download_calls.append(storage_path)
            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"cloud image bytes")

        def set_image_desktop_id(self, *args, **kwargs):
            pass

    def fake_add_image(**kwargs):
        add_calls.append(dict(kwargs))
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.execute(
                """
                INSERT INTO images (observation_id, filepath, image_type, sort_order)
                VALUES (?, ?, ?, ?)
                """,
                (
                    kwargs["observation_id"],
                    kwargs["filepath"],
                    kwargs["image_type"],
                    kwargs.get("sort_order"),
                ),
            )
            local_id = cursor.lastrowid
            conn.commit()
            return local_id
        finally:
            conn.close()

    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "from_stored_credentials", lambda: DummyClient())
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_rename_to_detected_image_extension", lambda path: Path(path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync.tempfile, "mkdtemp", lambda prefix=None: str(temp_root))
    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", fake_add_image)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)

    summary = cloud_sync._new_sync_summary()
    with cloud_sync._cloud_sync_summary_scope(summary):
        cloud_sync._import_remote_images(
            {"id": "cloud-obs-1", "genus": "Flammulina", "species": "velutipes"},
            1,
            "cloud-obs-1",
        )

    conn = sqlite3.connect(db_path)
    try:
        tombstone = conn.execute(
            """
            SELECT deleted_cloud_id, deleted_at, deleted_storage_path,
                   deleted_observation_cloud_id, local_observation_id, local_image_id
            FROM image_tombstones
            WHERE deleted_cloud_id = ?
            """,
            ("cloud-image-1",),
        ).fetchone()
        image_rows = conn.execute(
            "SELECT id, cloud_id FROM images ORDER BY id"
        ).fetchall()
    finally:
        conn.close()

    assert fetched_calls == [("cloud-obs-1", True)]
    assert len(add_calls) == 1
    assert download_calls == ["user/cloud-obs-1/cloud-image-2_kept.jpg"]
    assert tombstone == (
        "cloud-image-1",
        "2026-05-01 10:00:00",
        "user/cloud-obs-1/cloud-image-1_deleted.jpg",
        "cloud-obs-1",
        1,
        11,
    )
    assert image_rows == [
        (11, "cloud-image-1"),
        (12, "cloud-image-2"),
    ]
    assert image_path.exists()
    assert original_path.exists()
    assert summary["images_deleted_remote"] == 1


def test_record_remote_image_tombstones_counts_new_deletions_only(monkeypatch, tmp_path):
    db_path = _init_tombstone_sync_db(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, image_type, sort_order, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (11, 1, "cloud-image-1", "image.jpg", "field", 0, "2026-05-01 10:00:00"),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    remote_images = [
        {
            "id": "cloud-image-1",
            "observation_id": "cloud-obs-1",
            "deleted_at": "2026-05-01 10:00:00",
            "storage_path": "user/cloud-obs-1/cloud-image-1.jpg",
            "desktop_id": 11,
            "image_type": "field",
        }
    ]

    summary = cloud_sync._new_sync_summary()
    with cloud_sync._cloud_sync_summary_scope(summary):
        first = cloud_sync._record_remote_image_tombstones(
            remote_images,
            local_observation_id=1,
            cloud_observation_id="cloud-obs-1",
        )
        second = cloud_sync._record_remote_image_tombstones(
            remote_images,
            local_observation_id=1,
            cloud_observation_id="cloud-obs-1",
        )

    conn = sqlite3.connect(db_path)
    try:
        tombstone_count = conn.execute(
            "SELECT COUNT(*) FROM image_tombstones WHERE deleted_cloud_id = ?",
            ("cloud-image-1",),
        ).fetchone()[0]
    finally:
        conn.close()

    assert first == {"cloud-image-1"}
    assert second == {"cloud-image-1"}
    assert tombstone_count == 1
    assert summary["images_deleted_remote"] == 1


def test_import_remote_images_skips_tombstoned_cloud_image_and_keeps_unrelated_imports(
    monkeypatch,
    tmp_path,
    capsys,
):
    db_path = _init_tombstone_sync_db(tmp_path)
    temp_root = tmp_path / "sync-tombstones"
    temp_root.mkdir()
    download_calls: list[str] = []

    conn = sqlite3.connect(db_path)
    try:
        schema._ensure_image_tombstones_table(conn.cursor())
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?)
            """,
            ("cloud-image-1", "2026-05-01 10:00:00", 1, 11),
        )
        conn.commit()
    finally:
        conn.close()

    class DummyClient:
        def download_image_file(self, storage_path, dest_path):
            download_calls.append(storage_path)
            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"cloud image bytes")

        def set_image_desktop_id(self, *args, **kwargs):
            pass

    def fake_add_image(**kwargs):
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.execute(
                """
                INSERT INTO images (observation_id, filepath, image_type, sort_order)
                VALUES (?, ?, ?, ?)
                """,
                (
                    kwargs["observation_id"],
                    kwargs["filepath"],
                    kwargs["image_type"],
                    kwargs.get("sort_order"),
                ),
            )
            local_id = cursor.lastrowid
            conn.commit()
            return local_id
        finally:
            conn.close()

    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "from_stored_credentials", lambda: DummyClient())
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_rename_to_detected_image_extension", lambda path: Path(path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync.tempfile, "mkdtemp", lambda prefix=None: str(temp_root))
    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", fake_add_image)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)

    summary = cloud_sync._new_sync_summary()
    with cloud_sync._cloud_sync_summary_scope(summary):
        cloud_sync._import_remote_images(
            {"id": "cloud-obs-1", "genus": "Flammulina", "species": "velutipes"},
            1,
            "cloud-obs-1",
            remote_images=[
                {
                    "id": "cloud-image-1",
                    "storage_path": "user/cloud-obs-1/cloud-image-1_deleted.jpg",
                    "original_filename": "deleted.jpg",
                    "image_type": "field",
                },
                {
                    "id": "cloud-image-2",
                    "storage_path": "user/cloud-obs-1/cloud-image-2_kept.jpg",
                    "original_filename": "kept.jpg",
                    "image_type": "field",
                },
            ],
        )

    output = capsys.readouterr().out
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT cloud_id, image_type, filepath FROM images ORDER BY id"
        ).fetchall()
    finally:
        conn.close()

    assert download_calls == ["user/cloud-obs-1/cloud-image-2_kept.jpg"]
    assert len(rows) == 1
    assert rows[0][0] == "cloud-image-2"
    assert rows[0][1] == "field"
    assert "skipped cloud image cloud-image-1 because it has a local tombstone" in output
    assert summary["images_deleted_remote"] == 0


def test_apply_remote_images_to_local_skips_tombstoned_cloud_image_and_keeps_unrelated_imports(
    monkeypatch,
    tmp_path,
):
    db_path = _init_tombstone_sync_db(tmp_path)
    temp_root = tmp_path / "apply-tombstones"
    temp_root.mkdir()
    download_calls: list[str] = []

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status) VALUES (?, ?, ?)",
            (1, "cloud-obs-1", "synced"),
        )
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?)
            """,
            ("cloud-image-1", "2026-05-01 10:00:00", 1, 11),
        )
        conn.commit()
    finally:
        conn.close()

    class DummyClient:
        def download_image_file(self, storage_path, dest_path):
            download_calls.append(storage_path)
            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"cloud image bytes")

        def set_image_desktop_id(self, *args, **kwargs):
            pass

    def fake_add_image(**kwargs):
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.execute(
                """
                INSERT INTO images (observation_id, filepath, image_type, sort_order)
                VALUES (?, ?, ?, ?)
                """,
                (
                    kwargs["observation_id"],
                    kwargs["filepath"],
                    kwargs["image_type"],
                    kwargs.get("sort_order"),
                ),
            )
            local_id = cursor.lastrowid
            conn.commit()
            return local_id
        finally:
            conn.close()

    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_rename_to_detected_image_extension", lambda path: Path(path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync.tempfile, "mkdtemp", lambda prefix=None: str(temp_root))
    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", fake_add_image)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)

    warnings = cloud_sync._apply_remote_images_to_local(
        DummyClient(),
        1,
        [
            {
                "id": "cloud-image-1",
                "observation_id": "cloud-obs-1",
                "storage_path": "user/cloud-obs-1/cloud-image-1_deleted.jpg",
                "original_filename": "deleted.jpg",
                "image_type": "field",
                "sort_order": 0,
            },
            {
                "id": "cloud-image-2",
                "observation_id": "cloud-obs-1",
                "storage_path": "user/cloud-obs-1/cloud-image-2_kept.jpg",
                "original_filename": "kept.jpg",
                "image_type": "field",
                "sort_order": 1,
            },
        ],
        allow_delete=True,
    )

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT cloud_id, image_type FROM images ORDER BY id"
        ).fetchall()
    finally:
        conn.close()

    assert download_calls == ["user/cloud-obs-1/cloud-image-2_kept.jpg"]
    assert rows == [("cloud-image-2", "field")]
    assert any("local tombstone" in warning for warning in warnings)


def test_push_images_for_observation_skips_tombstoned_cloud_image_and_keeps_unrelated_uploads(
    monkeypatch,
    tmp_path,
    capsys,
):
    db_path = _init_tombstone_sync_db(tmp_path)
    images_root = tmp_path / "images"
    images_root.mkdir()

    tombstoned_path = images_root / "tombstoned.jpg"
    kept_path = images_root / "kept.jpg"
    tombstoned_path.write_bytes(b"tombstoned")
    kept_path.write_bytes(b"kept")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status) VALUES (?, ?, ?)",
            (1, "cloud-obs-1", "synced"),
        )
        cursor = conn.execute(
            """
            INSERT INTO images (
                observation_id, cloud_id, filepath, image_type, sort_order, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (1, "cloud-image-1", str(tombstoned_path), "field", 0, "2026-05-01 10:00:00"),
        )
        tombstoned_image_id = cursor.lastrowid
        cursor = conn.execute(
            """
            INSERT INTO images (
                observation_id, cloud_id, filepath, image_type, sort_order, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (1, None, str(kept_path), "field", 1, "2026-05-01 10:00:00"),
        )
        kept_image_id = cursor.lastrowid
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?)
            """,
            ("cloud-image-1", "2026-05-01 10:00:00", 1, tombstoned_image_id),
        )
        conn.commit()
    finally:
        conn.close()

    pushed_ids: list[int] = []
    uploaded_paths: list[str] = []

    client = cloud_sync.SporelyCloudClient("token", "user-123")
    monkeypatch.setattr(client, "pull_image_metadata", lambda obs_cloud_id: [])
    monkeypatch.setattr(client, "_observation_images_support_ai_crop", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_ai_crop_custom", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_upload_metadata", lambda: False)
    monkeypatch.setattr(client, "_patch", lambda *args, **kwargs: None)

    def fake_push_image_metadata(img, obs_cloud_id, storage_path):
        pushed_ids.append(int(img["id"]))
        return f"cloud-image-{int(img['id'])}"

    def fake_upload_image_file(local_path, obs_cloud_id, img_cloud_id, storage_path=None, upload_meta=None):
        uploaded_paths.append(str(local_path))
        return storage_path

    monkeypatch.setattr(client, "push_image_metadata", fake_push_image_metadata)
    monkeypatch.setattr(client, "upload_image_file", fake_upload_image_file)
    monkeypatch.setattr(cloud_sync, "_push_pending_image_tombstones", lambda client: [])
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)

    result = cloud_sync._push_images_for_observation(
        client,
        {"id": 1},
        "cloud-obs-1",
    )

    output = capsys.readouterr().out
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT id, cloud_id FROM images ORDER BY id"
        ).fetchall()
    finally:
        conn.close()

    assert result is True
    assert pushed_ids == [kept_image_id]
    assert uploaded_paths == [str(kept_path)]
    assert rows == [
        (tombstoned_image_id, "cloud-image-1"),
        (kept_image_id, f"cloud-image-{kept_image_id}"),
    ]
    assert "cloud-image-1 because it has a local tombstone" not in output


def test_push_images_for_observation_keeps_cloud_recovery_cache_image(monkeypatch, tmp_path):
    db_path = _init_push_all_sync_db(tmp_path)
    images_root = tmp_path / "images"
    images_root.mkdir()

    image_path = images_root / "cloud-imported.jpg"
    image_path.write_bytes(b"cloud-imported")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status) VALUES (?, ?, ?)",
            (1, "cloud-obs-1", "synced"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, image_type, sort_order,
                source_role, file_purpose, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                11,
                1,
                "cloud-image-1",
                str(image_path),
                "field",
                0,
                "cloud_recovery_cache",
                "cache",
                "2026-05-01 10:00:00",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    delete_calls: list[str] = []
    storage_remove_calls: list[list[str]] = []
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    monkeypatch.setattr(
        client,
        "pull_image_metadata",
        lambda obs_cloud_id: [
            {
                "id": "cloud-image-1",
                "desktop_id": 11,
                "storage_path": "user/cloud-obs-1/cloud-image-1.jpg",
            }
        ],
    )
    monkeypatch.setattr(client, "_observation_images_support_ai_crop", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_ai_crop_custom", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_upload_metadata", lambda: False)
    monkeypatch.setattr(client, "_patch", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        client,
        "_storage_remove",
        lambda paths: storage_remove_calls.append(list(paths)),
    )
    monkeypatch.setattr(
        client,
        "_delete",
        lambda path: delete_calls.append(str(path)),
    )
    monkeypatch.setattr(
        client,
        "upload_image_file",
        lambda local_path, obs_cloud_id, img_cloud_id, storage_path=None, upload_meta=None: (
            storage_path or client._build_storage_path(obs_cloud_id, img_cloud_id, local_path)
        ),
    )
    monkeypatch.setattr(
        client,
        "push_image_metadata",
        lambda img, obs_cloud_id, storage_path: "cloud-image-1",
    )
    monkeypatch.setattr(cloud_sync, "_push_pending_image_tombstones", lambda client: [])
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)

    result = cloud_sync._push_images_for_observation(
        client,
        {"id": 1},
        "cloud-obs-1",
    )

    conn = sqlite3.connect(db_path)
    try:
        image_row = conn.execute(
            "SELECT cloud_id FROM images WHERE id = ?",
            (11,),
        ).fetchone()
    finally:
        conn.close()

    assert result is True
    assert delete_calls == []
    assert storage_remove_calls == []
    assert image_row == ("cloud-image-1",)


def test_resolve_conflict_keep_local_records_deleted_cloud_images_before_push(
    monkeypatch,
    tmp_path,
):
    db_path = _init_tombstone_sync_db(tmp_path)
    images_root = tmp_path / "images"
    images_root.mkdir()
    image_path = images_root / "image.jpg"
    image_path.write_bytes(b"image-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "dirty", "2026-05-01T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, image_type, sort_order, created_at, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (11, 1, "cloud-image-1", str(image_path), "field", 0, "2026-05-01T10:00:00Z", "2026-05-01T10:00:00Z"),
        )
        conn.commit()
    finally:
        conn.close()

    pull_calls: list[bool] = []
    push_calls: list[tuple[str, str, str]] = []
    uploaded_paths: list[str] = []
    set_desktop_calls: list[tuple[str, int]] = []
    progress_messages: list[str] = []

    class DummyClient:
        def push_observation(self, local_obs, remote_obs=None, **kwargs):
            return "cloud-obs-1"

        def get_observation(self, cloud_id):
            return {"id": cloud_id}

        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            pull_calls.append(include_deleted_for_sync)
            if include_deleted_for_sync:
                return [
                    {
                        "id": "cloud-image-1",
                        "deleted_at": "2026-05-02 10:00:00",
                        "storage_path": "user/cloud-obs-1/cloud-image-1.jpg",
                    }
                ]
            return []

        def soft_delete_image(self, cloud_image_id, deleted_at):
            push_calls.append(("soft_delete", cloud_image_id, deleted_at))

        def push_image_metadata(self, *args, **kwargs):
            push_calls.append(("push_metadata", str(args[0].get("id")), str(args[1])))
            return "cloud-image-1"

        def upload_image_file(self, local_path, obs_cloud_id, img_cloud_id, storage_path=None, upload_meta=None):
            uploaded_paths.append(str(local_path))
            return storage_path

        def set_image_desktop_id(self, cloud_image_id, desktop_id):
            set_desktop_calls.append((cloud_image_id, desktop_id))

        def _observation_images_support_ai_crop(self):
            return False

        def _observation_images_support_upload_metadata(self):
            return False

    def prepare_images_cb(obs, progress_cb):
        return (
            [
                {
                    "image_row": {
                        "id": 11,
                        "cloud_id": "cloud-image-1",
                        "filepath": str(image_path),
                        "image_type": "field",
                        "sort_order": 0,
                    },
                    "upload_path": str(image_path),
                }
            ],
            None,
            [],
        )

    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    result = cloud_sync.resolve_conflict_keep_local(
        DummyClient(),
        1,
        prepare_images_cb=prepare_images_cb,
        progress_cb=lambda message, current, total: progress_messages.append(str(message)),
    )

    conn = sqlite3.connect(db_path)
    try:
        tombstone = conn.execute(
            """
            SELECT deleted_cloud_id, delete_synced_at
            FROM image_tombstones
            WHERE deleted_cloud_id = ?
            """,
            ("cloud-image-1",),
        ).fetchone()
        image_row = conn.execute(
            "SELECT COUNT(*) FROM images WHERE id = ?",
            (11,),
        ).fetchone()[0]
    finally:
        conn.close()

    assert result["cloud_id"] == "cloud-obs-1"
    assert pull_calls and pull_calls[0] is True
    assert tombstone is not None
    assert tombstone[0] == "cloud-image-1"
    assert tombstone[1] is not None
    assert image_row == 1
    assert push_calls == [("soft_delete", "cloud-image-1", "2026-05-02 10:00:00")]
    assert uploaded_paths == []
    assert set_desktop_calls == []
    assert image_path.exists()
    assert not any("Uploading cloud image" in message for message in progress_messages)


def test_sync_existing_remote_field_image_preserves_local_file(monkeypatch, tmp_path):
    local_file = tmp_path / "local_high_res.jpg"
    local_bytes = b"LOCAL-HIGH-RES-BYTES"
    remote_bytes = b"CLOUD-2MP-BYTES"
    local_file.write_bytes(local_bytes)

    class DummyClient:
        def download_image_file(self, storage_path, dest_path):
            Path(dest_path).write_bytes(remote_bytes)

        def set_image_desktop_id(self, *args, **kwargs):
            pass

    class DummyConn:
        def execute(self, *args, **kwargs):
            return self

        def commit(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: DummyConn())
    monkeypatch.setattr(cloud_sync.ImageDB, "update_image", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)

    cloud_sync._sync_existing_remote_image_to_local(
        DummyClient(),
        {
            "id": 19,
            "observation_id": 19,
            "filepath": str(local_file),
        },
        {
            "id": "cloud-img-19",
            "storage_path": "user/cloud-obs-1/cloud-img-19.jpg",
            "original_filename": "cloud-img-19.jpg",
            "image_type": "field",
        },
    )

    assert local_file.read_bytes() == local_bytes
