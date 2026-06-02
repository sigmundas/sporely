import json
import sqlite3
import uuid

from database import models
from utils import cloud_sync


def _create_calibrations_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
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
            is_active INTEGER DEFAULT 0
        );
        """
    )


def _create_linkage_db(tmp_path):
    db_path = tmp_path / "linkage.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        _create_calibrations_table(conn)
        conn.executescript(
            """
            CREATE TABLE images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observation_id INTEGER,
                cloud_id TEXT,
                filepath TEXT,
                image_type TEXT,
                calibration_id INTEGER,
                synced_at TEXT,
                sort_order INTEGER,
                objective_name TEXT,
                scale_microns_per_pixel REAL,
                resample_scale_factor REAL,
                micro_category TEXT,
                notes TEXT,
                measure_color TEXT,
                mount_medium TEXT,
                stain TEXT,
                sample_type TEXT,
                contrast TEXT,
                crop_mode TEXT,
                gps_source INTEGER,
                ai_crop_x1 REAL,
                ai_crop_y1 REAL,
                ai_crop_x2 REAL,
                ai_crop_y2 REAL,
                ai_crop_source_w INTEGER,
                ai_crop_source_h INTEGER,
                ai_crop_is_custom INTEGER,
                captured_at TEXT,
                original_filepath TEXT
            );
            CREATE TABLE settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            """
        )
        conn.commit()
    finally:
        conn.close()
    return db_path


def _patch_linkage_connections(monkeypatch, db_path):
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))


def test_push_image_metadata_sends_calibration_uuid_instead_of_local_calibration_id(monkeypatch, tmp_path):
    db_path = _create_linkage_db(tmp_path)
    calibration_uuid = str(uuid.uuid4())

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO calibrations (
                calibration_uuid, objective_key, calibration_date, microns_per_pixel, is_active
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (calibration_uuid, "100X", "2026-05-08 08:30:00", 0.0315, 1),
        )
        conn.commit()
    finally:
        conn.close()

    _patch_linkage_connections(monkeypatch, db_path)

    client = cloud_sync.SporelyCloudClient("token", "user-123")
    posts: list[tuple[str, dict]] = []

    monkeypatch.setattr(client, "_find_cloud_image", lambda desktop_id: None)
    monkeypatch.setattr(client, "_post", lambda path, payload: posts.append((path, dict(payload))) or [{"id": "cloud-img-1"}])
    monkeypatch.setattr(client, "_patch", lambda *args, **kwargs: None)
    monkeypatch.setattr(client, "_set_observation_media_keys", lambda *args, **kwargs: None)
    monkeypatch.setattr(client, "_observation_images_support_ai_crop", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_ai_crop_custom", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_upload_metadata", lambda: False)

    cloud_id = client.push_image_metadata(
        {
            "id": 11,
            "filepath": "/tmp/image.jpg",
            "image_type": "field",
            "sort_order": 0,
            "objective_name": "100X",
            "calibration_id": 1,
        },
        "cloud-obs-1",
        "user-123/cloud-obs-1/cloud-img-1_image.jpg",
    )

    assert cloud_id == "cloud-img-1"
    assert posts[0][0] == "observation_images"
    assert posts[0][1]["calibration_uuid"] == calibration_uuid
    assert "calibration_id" not in posts[0][1]
    assert posts[0][1]["desktop_id"] == 11
    assert posts[0][1]["observation_id"] == "cloud-obs-1"


def test_import_remote_images_resolves_calibration_uuid_to_local_calibration_id(monkeypatch, tmp_path):
    db_path = _create_linkage_db(tmp_path)
    temp_root = tmp_path / "sync"
    temp_root.mkdir()
    calibration_uuid = str(uuid.uuid4())

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO calibrations (
                calibration_uuid, objective_key, calibration_date, microns_per_pixel, is_active
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (calibration_uuid, "100X", "2026-05-08 08:30:00", 0.0315, 1),
        )
        conn.commit()
    finally:
        conn.close()

    _patch_linkage_connections(monkeypatch, db_path)
    monkeypatch.setattr(cloud_sync.tempfile, "mkdtemp", lambda prefix=None: str(temp_root))
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_rename_to_detected_image_extension", lambda path: path)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)

    desktop_id_calls = []
    captured = {}

    def fake_add_image(**kwargs):
        captured["kwargs"] = dict(kwargs)
        return 23

    class DummyClient:
        def download_image_file(self, storage_path, dest_path):
            dest = dest_path
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"cloud image bytes")

        def set_image_desktop_id(self, cloud_image_id, desktop_id):
            desktop_id_calls.append((cloud_image_id, desktop_id))

    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "from_stored_credentials", lambda: DummyClient())
    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", fake_add_image)

    cloud_sync._import_remote_images(
        {"id": "cloud-obs-1"},
        1,
        "cloud-obs-1",
        remote_images=[
            {
                "id": "cloud-img-1",
                "storage_path": "user-123/cloud-obs-1/cloud-img-1_image.jpg",
                "original_filename": "image.jpg",
                "image_type": "field",
                "calibration_uuid": calibration_uuid,
            }
        ],
    )

    assert captured["kwargs"]["calibration_id"] == 1
    assert desktop_id_calls == [("cloud-img-1", 23)]


def test_pull_calibrations_reconciles_images_from_stored_snapshots(monkeypatch, tmp_path):
    db_path = _create_linkage_db(tmp_path)
    _patch_linkage_connections(monkeypatch, db_path)

    calibration_uuid = str(uuid.uuid4())
    remote_rows = [
        {
            "id": "cloud-cal-1",
            "calibration_uuid": calibration_uuid,
            "objective_key": "100X",
            "calibration_date": "2026-05-08",
            "calibration_image_date": None,
            "microns_per_pixel": 0.0315,
            "notes": "cloud note",
            "is_active": True,
        }
    ]
    snapshot = json.dumps(
        {
            "observation": {"id": "cloud-obs-1"},
            "images": [
                {
                    "id": "cloud-img-1",
                    "desktop_id": 11,
                    "sort_order": 0,
                    "image_type": "field",
                    "original_filename": "image.jpg",
                    "calibration_uuid": calibration_uuid,
                }
            ],
            "measurements": [],
        },
        sort_keys=True,
    )

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO images (id, observation_id, cloud_id, filepath, image_type, calibration_id) VALUES (?, ?, ?, ?, ?, ?)",
            (11, 1, "cloud-img-1", "/tmp/image.jpg", "field", None),
        )
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?)",
            ("sporely_cloud_snapshot_obs_cloud-obs-1", snapshot),
        )
        conn.commit()
    finally:
        conn.close()

    class DummyClient:
        def list_remote_calibrations(self):
            return [dict(row) for row in remote_rows]

    result = cloud_sync.pull_calibrations(DummyClient())

    conn = sqlite3.connect(db_path)
    try:
        image_row = conn.execute(
            "SELECT calibration_id FROM images WHERE id = ?",
            (11,),
        ).fetchone()
        calibration_row = conn.execute(
            "SELECT id, calibration_uuid FROM calibrations ORDER BY id",
        ).fetchone()
    finally:
        conn.close()

    assert result["pulled"] == 1
    assert result["errors"] == []
    assert calibration_row[1] == calibration_uuid
    assert image_row[0] == calibration_row[0]


def test_get_conflict_detail_uses_calibration_uuid_for_image_matching(monkeypatch):
    shared_uuid = str(uuid.uuid4())
    local_obs = {
        "id": 1,
        "cloud_id": "cloud-obs-1",
        "date": "2026-05-01",
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
                "id": "cloud-img-1",
                "desktop_id": 11,
                "sort_order": 0,
                "image_type": "field",
                "original_filename": "image.jpg",
                "calibration_uuid": shared_uuid,
            }
        ],
        "measurements": [],
    }
    local_images = [
        {
            "id": 11,
            "cloud_id": "cloud-img-1",
            "filepath": "/tmp/image.jpg",
            "image_type": "field",
            "sort_order": 0,
            "original_filename": "image.jpg",
            "calibration_id": 42,
        }
    ]

    monkeypatch.setattr(cloud_sync.ObservationDB, "get_observation", lambda local_id: dict(local_obs))
    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation", lambda local_id: [dict(row) for row in local_images])
    monkeypatch.setattr(cloud_sync.MeasurementDB, "get_measurements_for_observation", lambda local_id: [])
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: json.dumps(baseline_snapshot))
    monkeypatch.setattr(
        cloud_sync.CalibrationDB,
        "get_calibration",
        lambda calibration_id: {"id": 42, "calibration_uuid": shared_uuid} if calibration_id == 42 else None,
    )

    class DummyClient:
        def get_observation(self, cloud_id):
            return dict(remote_obs)

        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            return [
                {
                    "id": "cloud-img-1",
                    "desktop_id": 11,
                    "observation_id": "cloud-obs-1",
                    "image_type": "field",
                    "sort_order": 0,
                    "original_filename": "image.jpg",
                    "calibration_uuid": shared_uuid,
                }
            ]

    detail = cloud_sync.get_conflict_detail(DummyClient(), 1, "cloud-obs-1")

    assert detail["image_mismatches"] == []
    assert detail["local_image_changes"] == []
    assert detail["remote_image_changes"] == []
