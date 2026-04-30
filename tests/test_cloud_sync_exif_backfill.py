import sqlite3

from PIL import Image

from utils import cloud_sync


def test_backfill_missing_exif_uses_observation_fallback(monkeypatch, tmp_path):
    image_path = tmp_path / "field.jpg"
    Image.new("RGB", (8, 8), "white").save(image_path, format="JPEG")

    db_path = tmp_path / "sporely.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE images (
            id INTEGER PRIMARY KEY,
            filepath TEXT,
            observation_id INTEGER,
            image_type TEXT,
            cloud_id TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO images (id, filepath, observation_id, image_type, cloud_id)
        VALUES (1, ?, 42, 'field', 'cloud-image')
        """,
        (str(image_path),),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(
        cloud_sync.ObservationDB,
        "get_observation",
        lambda observation_id: {
            "id": observation_id,
            "gps_latitude": "59.9",
            "gps_longitude": "10.75",
            "gps_altitude": "12.5",
            "date": "2026-04-30",
        },
    )
    calls = []
    monkeypatch.setattr(
        cloud_sync,
        "_inject_obs_exif_into_field_image",
        lambda *args: calls.append(args),
    )

    cloud_sync._backfill_missing_exif_on_cloud_images()

    assert calls == [(image_path, 59.9, 10.75, 12.5, "2026-04-30")]
