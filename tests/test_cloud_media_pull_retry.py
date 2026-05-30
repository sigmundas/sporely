import shutil
import sqlite3
from pathlib import Path

from database import models, schema
from utils import cloud_sync


def _create_retry_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cloud_id TEXT,
                sync_status TEXT,
                synced_at TEXT,
                folder_path TEXT
            );
            CREATE TABLE images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observation_id INTEGER,
                cloud_id TEXT,
                filepath TEXT,
                original_filepath TEXT,
                sort_order INTEGER,
                image_type TEXT,
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
                captured_at TEXT,
                synced_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE settings (
                key TEXT PRIMARY KEY,
                value TEXT
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
                cloud_id TEXT,
                measured_at TEXT
            );
            """
        )
        schema._ensure_image_tombstones_table(conn.cursor())
        conn.execute(
            """
            INSERT INTO observations (id, cloud_id, sync_status, synced_at, folder_path)
            VALUES (?, ?, ?, ?, ?)
            """,
            (1, "cloud-obs-1", "synced", "2026-05-01T00:00:00Z", str(db_path.parent / "images" / "obs-1")),
        )
        conn.commit()
    finally:
        conn.close()


class RetryingClient:
    def __init__(self, remote_images: list[dict]):
        self.remote_images = [dict(row or {}) for row in remote_images]
        self.remote_observation = {
            "id": "cloud-obs-1",
            "desktop_id": 1,
            "date": "2026-05-01",
            "genus": "Flammulina",
            "species": "velutipes",
        }
        self.download_attempts: list[str] = []
        self.failures_remaining = len(self.remote_images)

    def pull_bulk_image_metadata(self, obs_cloud_ids):
        if "cloud-obs-1" not in set(obs_cloud_ids or []):
            return []
        return [dict(row) for row in self.remote_images]

    def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
        if cloud_id != "cloud-obs-1":
            return []
        return [dict(row) for row in self.remote_images]

    def get_observation(self, cloud_id):
        if cloud_id != "cloud-obs-1":
            return None
        return dict(self.remote_observation)

    def download_image_file(self, storage_path, dest_path):
        self.download_attempts.append(str(storage_path))
        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            raise cloud_sync.CloudSyncError("Cloud image file is missing from storage")
        dest = Path(dest_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"cloud image bytes")

    def set_desktop_id(self, *args, **kwargs):
        return None


def test_pull_all_retries_missing_local_cloud_media_after_snapshot_stays_unchanged(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "sporely.db"
    _create_retry_db(db_path)
    images_root = tmp_path / "images" / "obs-1"
    images_root.mkdir(parents=True, exist_ok=True)

    remote_images = [
        {
            "id": "cloud-image-1",
            "desktop_id": 1,
            "observation_id": "cloud-obs-1",
            "storage_path": "8c471394-b274-4933-b830-59805820d93c/617/0_1780071867059.webp",
            "original_filename": "0_1780071867059.webp",
            "image_type": "field",
            "sort_order": 0,
            "deleted_at": None,
        },
        {
            "id": "cloud-image-2",
            "desktop_id": 1,
            "observation_id": "cloud-obs-1",
            "storage_path": "8c471394-b274-4933-b830-59805820d93c/617/1_1780071867059.webp",
            "original_filename": "1_1780071867059.webp",
            "image_type": "field",
            "sort_order": 1,
            "deleted_at": None,
        },
    ]

    client = RetryingClient(remote_images)

    def fake_add_image(**kwargs):
        source_path = Path(str(kwargs["filepath"]))
        conn = sqlite3.connect(db_path)
        try:
            obs_row = conn.execute(
                "SELECT folder_path FROM observations WHERE id = ?",
                (int(kwargs["observation_id"]),),
            ).fetchone()
            folder_path = Path(str(obs_row[0])) if obs_row and obs_row[0] else images_root
            folder_path.mkdir(parents=True, exist_ok=True)
            dest_path = folder_path / source_path.name
            counter = 1
            while dest_path.exists():
                dest_path = folder_path / f"{source_path.stem}_{counter}{source_path.suffix}"
                counter += 1
            shutil.copy2(source_path, dest_path)
            cursor = conn.execute(
                """
                INSERT INTO images (
                    observation_id, filepath, original_filepath, sort_order, image_type,
                    micro_category, objective_name, scale_microns_per_pixel, resample_scale_factor,
                    mount_medium, stain, sample_type, contrast, measure_color, crop_mode,
                    notes, gps_source, ai_crop_x1, ai_crop_y1, ai_crop_x2, ai_crop_y2,
                    ai_crop_source_w, ai_crop_source_h, ai_crop_is_custom, captured_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(kwargs["observation_id"]),
                    str(dest_path),
                    None,
                    kwargs.get("sort_order"),
                    kwargs.get("image_type"),
                    kwargs.get("micro_category"),
                    kwargs.get("objective_name"),
                    kwargs.get("scale"),
                    kwargs.get("resample_scale_factor"),
                    kwargs.get("mount_medium"),
                    kwargs.get("stain"),
                    kwargs.get("sample_type"),
                    kwargs.get("contrast"),
                    kwargs.get("measure_color"),
                    kwargs.get("crop_mode"),
                    kwargs.get("notes"),
                    kwargs.get("gps_source"),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    kwargs.get("ai_crop_is_custom"),
                    kwargs.get("captured_at"),
                ),
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    monkeypatch.setattr(cloud_sync, "_backfill_missing_exif_on_cloud_images", lambda: None)
    monkeypatch.setattr(cloud_sync, "_apply_remote_observation_fields", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_detect_deleted_remote_observations", lambda remote_obs: [])
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", fake_add_image)

    first_result = cloud_sync.pull_all(
        client,
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
        first_snapshot = conn.execute(
            "SELECT value FROM settings WHERE key = ?",
            ("sporely_cloud_snapshot_obs_cloud-obs-1",),
        ).fetchone()
        first_images = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
    finally:
        conn.close()

    assert first_result["pulled"] == 1
    assert len(client.download_attempts) == 2
    assert first_images == 0
    assert first_snapshot is not None
    assert str(first_snapshot[0] or "").strip()

    second_result = cloud_sync.pull_all(
        client,
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
        rows = conn.execute(
            "SELECT cloud_id, filepath FROM images ORDER BY id"
        ).fetchall()
        second_snapshot = conn.execute(
            "SELECT value FROM settings WHERE key = ?",
            ("sporely_cloud_snapshot_obs_cloud-obs-1",),
        ).fetchone()
    finally:
        conn.close()

    assert second_result["pulled"] == 1
    assert second_result["errors"] == []
    assert len(client.download_attempts) == 4
    assert [row[0] for row in rows] == ["cloud-image-1", "cloud-image-2"]
    assert all(Path(row[1]).exists() for row in rows)
    assert second_snapshot == first_snapshot
