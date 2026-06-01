import json
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
    def __init__(self, remote_images: list[dict], remote_measurements: list[dict] | None = None):
        self.remote_images = [dict(row or {}) for row in remote_images]
        self.remote_measurements = [dict(row or {}) for row in (remote_measurements or [])]
        self.remote_observation = {
            "id": "cloud-obs-1",
            "desktop_id": 1,
            "date": "2026-05-01",
            "genus": "Flammulina",
            "species": "velutipes",
        }
        self.download_attempts: list[str] = []
        self.pull_image_metadata_calls = 0
        self.pull_bulk_calls = 0
        self.pull_measurement_calls = 0
        self.set_image_desktop_id_calls: list[tuple[str, int]] = []
        self.set_measurement_desktop_id_calls: list[tuple[str, int]] = []
        self.failures_remaining = len(self.remote_images)

    def pull_bulk_image_metadata(self, obs_cloud_ids):
        self.pull_bulk_calls += 1
        if "cloud-obs-1" not in set(obs_cloud_ids or []):
            return []
        return [dict(row) for row in self.remote_images]

    def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
        self.pull_image_metadata_calls += 1
        if cloud_id != "cloud-obs-1":
            return []
        return [dict(row) for row in self.remote_images]

    def pull_measurements_for_images(self, image_cloud_ids):
        self.pull_measurement_calls += 1
        if "cloud-image-1" not in set(image_cloud_ids or []) and "cloud-image-2" not in set(image_cloud_ids or []):
            return []
        return [dict(row) for row in self.remote_measurements]

    def get_observation(self, cloud_id):
        if cloud_id != "cloud-obs-1":
            return None
        return dict(self.remote_observation)

    def set_image_desktop_id(self, cloud_image_id, desktop_id):
        self.set_image_desktop_id_calls.append((str(cloud_image_id), int(desktop_id)))

    def set_measurement_desktop_id(self, cloud_measurement_id, desktop_id):
        self.set_measurement_desktop_id_calls.append((str(cloud_measurement_id), int(desktop_id)))

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


def _copying_add_image_factory(db_path: Path, images_root: Path):
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

    return fake_add_image


def _store_snapshot(db_path: Path, cloud_id: str, payload: dict) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (f"sporely_cloud_snapshot_obs_{cloud_id}", json.dumps(payload)),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_local_image_row(
    db_path: Path,
    *,
    observation_id: int,
    filepath: Path,
    cloud_id: str | None = None,
    sort_order: int = 0,
    image_type: str = "field",
) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            """
            INSERT INTO images (
                observation_id, cloud_id, filepath, original_filepath, sort_order, image_type,
                micro_category, objective_name, scale_microns_per_pixel, resample_scale_factor,
                mount_medium, stain, sample_type, contrast, measure_color, crop_mode,
                notes, gps_source, ai_crop_x1, ai_crop_y1, ai_crop_x2, ai_crop_y2,
                ai_crop_source_w, ai_crop_source_h, ai_crop_is_custom, captured_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(observation_id),
                cloud_id,
                str(filepath),
                str(filepath),
                int(sort_order),
                image_type,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()


def _insert_local_measurement_row(
    db_path: Path,
    *,
    image_id: int,
    cloud_id: str | None = None,
    measurement_type: str = "manual",
) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            """
            INSERT INTO spore_measurements (
                image_id, length_um, width_um, measurement_type, notes,
                p1_x, p1_y, p2_x, p2_y, p3_x, p3_y, p4_x, p4_y,
                gallery_rotation, cloud_id, measured_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(image_id),
                12.5,
                7.5,
                measurement_type,
                None,
                1.0,
                2.0,
                3.0,
                4.0,
                5.0,
                6.0,
                7.0,
                8.0,
                90,
                cloud_id,
                "2026-05-01T12:00:00Z",
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()


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

    profiler = cloud_sync.CloudSyncProfiler()
    with cloud_sync._cloud_sync_profile_scope(profiler):
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

    assert profiler.retry_missing_cloud_media_branch_runs == 1


def test_pull_all_can_skip_materializing_remote_images_without_losing_snapshot_metadata(
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
    remote_measurements = [
        {
            "id": "cloud-measurement-1",
            "desktop_id": None,
            "image_id": "cloud-image-1",
            "length_um": 12.5,
            "width_um": 7.5,
            "measurement_type": "manual",
            "gallery_rotation": 90,
            "p1_x": 1.1,
            "p1_y": 2.2,
            "p2_x": 3.3,
            "p2_y": 4.4,
            "p3_x": 5.5,
            "p3_y": 6.6,
            "p4_x": 7.7,
            "p4_y": 8.8,
            "measured_at": "2026-05-01T12:00:00Z",
            "notes": "cloud note",
        }
    ]

    client = RetryingClient(remote_images, remote_measurements)
    generate_calls = []

    def fake_generate_all_sizes(*args, **kwargs):
        generate_calls.append((args, kwargs))

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
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", fake_generate_all_sizes)
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", fake_add_image)

    profiler = cloud_sync.CloudSyncProfiler()
    with cloud_sync._cloud_sync_profile_scope(profiler):
        result = cloud_sync.pull_all(
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
            materialize_remote_images=False,
        )

    conn = sqlite3.connect(db_path)
    try:
        observation = conn.execute(
            "SELECT cloud_id, sync_status FROM observations WHERE id = ?",
            (1,),
        ).fetchone()
        image_count = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
        measurement_count = conn.execute("SELECT COUNT(*) FROM spore_measurements").fetchone()[0]
        snapshot_row = conn.execute(
            "SELECT value FROM settings WHERE key = ?",
            ("sporely_cloud_snapshot_obs_cloud-obs-1",),
        ).fetchone()
    finally:
        conn.close()

    assert result["pulled"] == 1
    assert result["errors"] == []
    assert client.download_attempts == []
    assert generate_calls == []
    assert client.pull_bulk_calls == 1
    assert client.pull_measurement_calls == 1
    assert profiler.download_image_file_calls == 0
    assert profiler.generate_all_sizes_calls == 0
    assert profiler.store_remote_snapshot_fetch_images_count == 0
    assert profiler.store_remote_snapshot_fetch_measurements_count == 0
    assert profiler.retry_missing_cloud_media_branch_runs == 0
    assert observation == ("cloud-obs-1", "synced")
    assert image_count == 0
    assert measurement_count == 0
    assert snapshot_row is not None
    snapshot = json.loads(snapshot_row[0])
    assert len(snapshot["images"]) == len(remote_images)
    assert len(snapshot["measurements"]) == len(remote_measurements)


def test_materialize_cloud_media_for_observation_uses_snapshot_and_is_idempotent(
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
            "desktop_id": None,
            "observation_id": "cloud-obs-1",
            "storage_path": "8c471394-b274-4933-b830-59805820d93c/617/0_1780071867059.webp",
            "original_filename": "0_1780071867059.webp",
            "image_type": "field",
            "sort_order": 0,
            "deleted_at": None,
        }
    ]
    remote_measurements = [
        {
            "id": "cloud-measurement-1",
            "desktop_id": None,
            "image_id": "cloud-image-1",
            "length_um": 12.5,
            "width_um": 7.5,
            "measurement_type": "manual",
            "gallery_rotation": 90,
            "p1_x": 1.1,
            "p1_y": 2.2,
            "p2_x": 3.3,
            "p2_y": 4.4,
            "p3_x": 5.5,
            "p3_y": 6.6,
            "p4_x": 7.7,
            "p4_y": 8.8,
            "measured_at": "2026-05-01T12:00:00Z",
            "notes": "cloud note",
        }
    ]
    _store_snapshot(
        db_path,
        "cloud-obs-1",
        {
            "observation": {
                "id": "cloud-obs-1",
                "desktop_id": 1,
                "date": "2026-05-01",
                "genus": "Flammulina",
                "species": "velutipes",
            },
            "images": remote_images,
            "measurements": remote_measurements,
        },
    )

    client = RetryingClient(remote_images, remote_measurements)
    client.failures_remaining = 0
    generate_calls = []
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: generate_calls.append((args, kwargs)))
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", _copying_add_image_factory(db_path, images_root))

    profiler = cloud_sync.CloudSyncProfiler()
    with cloud_sync._cloud_sync_profile_scope(profiler):
        first_result = cloud_sync.materialize_cloud_media_for_observation(client, 1)
        second_result = cloud_sync.materialize_cloud_media_for_observation(client, 1)

    conn = sqlite3.connect(db_path)
    try:
        image_rows = conn.execute(
            "SELECT cloud_id, filepath FROM images ORDER BY id"
        ).fetchall()
        measurement_rows = conn.execute(
            "SELECT cloud_id, image_id FROM spore_measurements ORDER BY id"
        ).fetchall()
        snapshot_row = conn.execute(
            "SELECT value FROM settings WHERE key = ?",
            ("sporely_cloud_snapshot_obs_cloud-obs-1",),
        ).fetchone()
    finally:
        conn.close()

    assert first_result["status"] == "ok"
    assert first_result["used_snapshot_data"] is True
    assert first_result["used_live_fallback"] is False
    assert first_result["remote_images_considered"] == 1
    assert first_result["skipped_already_materialized"] == 0
    assert first_result["downloaded"] == 1
    assert first_result["failed"] == 0
    assert first_result["measurements_imported"] == 1
    assert first_result["errors"] == []
    assert second_result["status"] == "ok"
    assert second_result["used_snapshot_data"] is True
    assert second_result["used_live_fallback"] is False
    assert second_result["remote_images_considered"] == 1
    assert second_result["skipped_already_materialized"] == 1
    assert second_result["downloaded"] == 0
    assert second_result["failed"] == 0
    assert len(client.download_attempts) == 1
    assert client.pull_image_metadata_calls == 0
    assert client.pull_measurement_calls == 0
    assert len(client.set_image_desktop_id_calls) == 1
    assert len(client.set_measurement_desktop_id_calls) == 1
    assert len(generate_calls) == 1
    assert profiler.generate_all_sizes_calls == 1
    assert profiler.store_remote_snapshot_fetch_images_count == 0
    assert profiler.store_remote_snapshot_fetch_measurements_count == 0
    assert [row[0] for row in image_rows] == ["cloud-image-1"]
    assert Path(image_rows[0][1]).exists()
    assert [row[0] for row in measurement_rows] == ["cloud-measurement-1"]
    assert snapshot_row is not None
    snapshot = json.loads(snapshot_row[0])
    assert len(snapshot["images"]) == 1
    assert len(snapshot["measurements"]) == 1


def test_materialize_cloud_media_for_observation_falls_back_when_snapshot_missing(
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
            "desktop_id": None,
            "observation_id": "cloud-obs-1",
            "storage_path": "8c471394-b274-4933-b830-59805820d93c/617/0_1780071867059.webp",
            "original_filename": "0_1780071867059.webp",
            "image_type": "field",
            "sort_order": 0,
            "deleted_at": None,
        }
    ]
    remote_measurements = [
        {
            "id": "cloud-measurement-1",
            "desktop_id": None,
            "image_id": "cloud-image-1",
            "length_um": 12.5,
            "width_um": 7.5,
            "measurement_type": "manual",
            "gallery_rotation": 90,
            "p1_x": 1.1,
            "p1_y": 2.2,
            "p2_x": 3.3,
            "p2_y": 4.4,
            "p3_x": 5.5,
            "p3_y": 6.6,
            "p4_x": 7.7,
            "p4_y": 8.8,
            "measured_at": "2026-05-01T12:00:00Z",
            "notes": "cloud note",
        }
    ]

    client = RetryingClient(remote_images, remote_measurements)
    client.failures_remaining = 0
    generate_calls = []
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: generate_calls.append((args, kwargs)))
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", _copying_add_image_factory(db_path, images_root))

    profiler = cloud_sync.CloudSyncProfiler()
    with cloud_sync._cloud_sync_profile_scope(profiler):
        result = cloud_sync.materialize_cloud_media_for_observation(client, 1)

    conn = sqlite3.connect(db_path)
    try:
        image_rows = conn.execute("SELECT cloud_id FROM images ORDER BY id").fetchall()
        measurement_rows = conn.execute("SELECT cloud_id FROM spore_measurements ORDER BY id").fetchall()
    finally:
        conn.close()

    assert result["status"] == "ok"
    assert result["used_snapshot_data"] is False
    assert result["used_live_fallback"] is True
    assert result["remote_images_considered"] == 1
    assert result["downloaded"] == 1
    assert result["failed"] == 0
    assert len(client.download_attempts) == 1
    assert client.pull_image_metadata_calls == 1
    assert client.pull_measurement_calls == 1
    assert len(generate_calls) == 1
    assert profiler.generate_all_sizes_calls == 1
    assert profiler.store_remote_snapshot_fetch_images_count == 0
    assert profiler.store_remote_snapshot_fetch_measurements_count == 0
    assert [row[0] for row in image_rows] == ["cloud-image-1"]
    assert [row[0] for row in measurement_rows] == ["cloud-measurement-1"]


def test_materialize_cloud_media_for_observation_repairs_missing_file(
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
            "desktop_id": None,
            "observation_id": "cloud-obs-1",
            "storage_path": "8c471394-b274-4933-b830-59805820d93c/617/0_1780071867059.webp",
            "original_filename": "0_1780071867059.webp",
            "image_type": "field",
            "sort_order": 0,
            "deleted_at": None,
        }
    ]
    remote_measurements = [
        {
            "id": "cloud-measurement-1",
            "desktop_id": None,
            "image_id": "cloud-image-1",
            "length_um": 12.5,
            "width_um": 7.5,
            "measurement_type": "manual",
            "gallery_rotation": 90,
            "p1_x": 1.1,
            "p1_y": 2.2,
            "p2_x": 3.3,
            "p2_y": 4.4,
            "p3_x": 5.5,
            "p3_y": 6.6,
            "p4_x": 7.7,
            "p4_y": 8.8,
            "measured_at": "2026-05-01T12:00:00Z",
            "notes": "cloud note",
        }
    ]
    _store_snapshot(
        db_path,
        "cloud-obs-1",
        {
            "observation": {
                "id": "cloud-obs-1",
                "desktop_id": 1,
                "date": "2026-05-01",
                "genus": "Flammulina",
                "species": "velutipes",
            },
            "images": remote_images,
            "measurements": remote_measurements,
        },
    )

    existing_path = images_root / "0_1780071867059.webp"
    existing_path.write_bytes(b"old-bytes")
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath, sort_order, image_type,
                micro_category, objective_name, scale_microns_per_pixel, resample_scale_factor,
                mount_medium, stain, sample_type, contrast, measure_color, crop_mode, notes,
                gps_source, ai_crop_x1, ai_crop_y1, ai_crop_x2, ai_crop_y2, ai_crop_source_w,
                ai_crop_source_h, ai_crop_is_custom, captured_at, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                1,
                "cloud-image-1",
                str(existing_path),
                None,
                0,
                "field",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "cloud image",
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "2026-05-01T00:00:00Z",
                "2026-05-01T00:00:00Z",
            ),
        )
        conn.commit()
    finally:
        conn.close()
    existing_path.unlink()

    client = RetryingClient(remote_images, remote_measurements)
    client.failures_remaining = 0
    generate_calls = []
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: generate_calls.append((args, kwargs)))
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    def fail_add_image(*args, **kwargs):
        raise AssertionError("helper should repair the existing image row instead of creating a duplicate")

    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", fail_add_image)

    profiler = cloud_sync.CloudSyncProfiler()
    with cloud_sync._cloud_sync_profile_scope(profiler):
        result = cloud_sync.materialize_cloud_media_for_observation(client, 1)

    conn = sqlite3.connect(db_path)
    try:
        image_rows = conn.execute(
            "SELECT cloud_id, filepath FROM images ORDER BY id"
        ).fetchall()
        measurement_rows = conn.execute(
            "SELECT cloud_id, image_id FROM spore_measurements ORDER BY id"
        ).fetchall()
    finally:
        conn.close()

    assert result["status"] == "ok"
    assert result["downloaded"] == 1
    assert result["failed"] == 0
    assert result["skipped_already_materialized"] == 0
    assert len(client.download_attempts) == 1
    assert client.pull_image_metadata_calls == 0
    assert client.pull_measurement_calls == 0
    assert len(generate_calls) == 1
    assert profiler.generate_all_sizes_calls == 1
    assert [row[0] for row in image_rows] == ["cloud-image-1"]
    assert Path(image_rows[0][1]).exists()
    assert [row[0] for row in measurement_rows] == ["cloud-measurement-1"]


def test_materialize_cloud_media_for_observation_reports_partial_download_failure_without_broken_row(
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
            "desktop_id": None,
            "observation_id": "cloud-obs-1",
            "storage_path": "8c471394-b274-4933-b830-59805820d93c/617/0_1780071867059.webp",
            "original_filename": "0_1780071867059.webp",
            "image_type": "field",
            "sort_order": 0,
            "deleted_at": None,
        },
        {
            "id": "cloud-image-2",
            "desktop_id": None,
            "observation_id": "cloud-obs-1",
            "storage_path": "8c471394-b274-4933-b830-59805820d93c/617/1_1780071867059.webp",
            "original_filename": "1_1780071867059.webp",
            "image_type": "field",
            "sort_order": 1,
            "deleted_at": None,
        },
    ]
    remote_measurements = [
        {
            "id": "cloud-measurement-1",
            "desktop_id": None,
            "image_id": "cloud-image-2",
            "length_um": 12.5,
            "width_um": 7.5,
            "measurement_type": "manual",
            "gallery_rotation": 90,
            "p1_x": 1.1,
            "p1_y": 2.2,
            "p2_x": 3.3,
            "p2_y": 4.4,
            "p3_x": 5.5,
            "p3_y": 6.6,
            "p4_x": 7.7,
            "p4_y": 8.8,
            "measured_at": "2026-05-01T12:00:00Z",
            "notes": "cloud note",
        }
    ]
    _store_snapshot(
        db_path,
        "cloud-obs-1",
        {
            "observation": {
                "id": "cloud-obs-1",
                "desktop_id": 1,
                "date": "2026-05-01",
                "genus": "Flammulina",
                "species": "velutipes",
            },
            "images": remote_images,
            "measurements": remote_measurements,
        },
    )

    client = RetryingClient(remote_images, remote_measurements)
    client.failures_remaining = 1
    generate_calls = []
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: generate_calls.append((args, kwargs)))
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", _copying_add_image_factory(db_path, images_root))

    profiler = cloud_sync.CloudSyncProfiler()
    with cloud_sync._cloud_sync_profile_scope(profiler):
        result = cloud_sync.materialize_cloud_media_for_observation(client, 1)

    conn = sqlite3.connect(db_path)
    try:
        image_rows = conn.execute(
            "SELECT cloud_id, filepath FROM images ORDER BY id"
        ).fetchall()
        measurement_rows = conn.execute(
            "SELECT cloud_id, image_id FROM spore_measurements ORDER BY id"
        ).fetchall()
    finally:
        conn.close()

    assert result["status"] == "partial"
    assert result["downloaded"] == 1
    assert result["failed"] == 1
    assert result["remote_images_considered"] == 2
    assert result["errors"]
    assert len(client.download_attempts) == 2
    assert len(generate_calls) == 1
    assert profiler.generate_all_sizes_calls == 1
    assert [row[0] for row in image_rows] == ["cloud-image-2"]
    assert Path(image_rows[0][1]).exists()
    assert [row[0] for row in measurement_rows] == ["cloud-measurement-1"]


def test_cloud_media_materialization_state_detects_missing_and_ready_media(
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
        }
    ]
    remote_measurements = [
        {
            "id": "cloud-measurement-1",
            "desktop_id": None,
            "image_id": "cloud-image-1",
            "length_um": 12.5,
            "width_um": 7.5,
            "measurement_type": "manual",
            "gallery_rotation": 90,
            "p1_x": 1.1,
            "p1_y": 2.2,
            "p2_x": 3.3,
            "p2_y": 4.4,
            "p3_x": 5.5,
            "p3_y": 6.6,
            "p4_x": 7.7,
            "p4_y": 8.8,
            "measured_at": "2026-05-01T12:00:00Z",
            "notes": "cloud note",
        }
    ]
    _store_snapshot(
        db_path,
        "cloud-obs-1",
        {
            "observation": {
                "id": "cloud-obs-1",
                "desktop_id": 1,
                "date": "2026-05-01",
                "genus": "Flammulina",
                "species": "velutipes",
            },
            "images": remote_images,
            "measurements": remote_measurements,
        },
    )

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    missing_state = cloud_sync.cloud_media_materialization_state_for_observation(1)
    assert missing_state["status"] == "needs_materialization"
    assert missing_state["can_auto_start"] is True
    assert missing_state["remote_images_considered"] == 1
    assert missing_state["remote_measurements_considered"] == 1
    assert missing_state["local_images_missing_files"] == 1
    assert missing_state["local_measurements_missing"] == 1

    image_path = images_root / "cloud-materialized.jpg"
    image_path.write_bytes(b"materialized")
    local_image_id = _insert_local_image_row(
        db_path,
        observation_id=1,
        filepath=image_path,
        cloud_id="cloud-image-1",
    )
    _insert_local_measurement_row(
        db_path,
        image_id=local_image_id,
        cloud_id="cloud-measurement-1",
    )

    ready_state = cloud_sync.cloud_media_materialization_state_for_observation(1)
    assert ready_state["status"] == "already_materialized"
    assert ready_state["needs_materialization"] is False
    assert ready_state["local_images_ready"] == 1
    assert ready_state["local_measurements_linked"] == 1


def test_cloud_media_materialization_state_without_snapshot_is_conservative(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "sporely.db"
    _create_retry_db(db_path)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    state = cloud_sync.cloud_media_materialization_state_for_observation(1)
    assert state["status"] == "needs_materialization"
    assert state["can_auto_start"] is False
    assert state["reason"] == "snapshot_missing_media"
