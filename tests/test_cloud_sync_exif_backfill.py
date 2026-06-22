import sqlite3

from PIL import Image

from utils import cloud_sync


def _make_backfill_db(db_path, images):
    """Create images + settings tables and insert the given image rows.

    images: list of (id, filepath, observation_id, image_type, cloud_id)
    """
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
    conn.execute("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT)")
    conn.executemany(
        "INSERT INTO images (id, filepath, observation_id, image_type, cloud_id) VALUES (?, ?, ?, ?, ?)",
        images,
    )
    conn.commit()
    conn.close()


def test_backfill_returns_zero_when_no_cloud_images(monkeypatch, tmp_path):
    db_path = tmp_path / "sporely.db"
    _make_backfill_db(db_path, [])
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))

    opened = []
    real_open = Image.open
    monkeypatch.setattr(Image, "open", lambda *a, **k: opened.append(a) or real_open(*a, **k))

    counters = cloud_sync._backfill_missing_exif_on_cloud_images()

    assert counters["scanned"] == 0
    assert counters["opened"] == 0
    assert opened == []


def test_backfill_skips_unchanged_files_on_second_run(monkeypatch, tmp_path):
    image_path = tmp_path / "field.jpg"
    Image.new("RGB", (8, 8), "white").save(image_path, format="JPEG")
    db_path = tmp_path / "sporely.db"
    _make_backfill_db(db_path, [(1, str(image_path), 42, "field", "cloud-image")])

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(
        cloud_sync.ObservationDB,
        "get_observation",
        lambda observation_id: {
            "id": observation_id,
            "gps_latitude": "59.9",
            "gps_longitude": "10.75",
            "date": "2026-04-30",
        },
    )
    inject_calls = []
    monkeypatch.setattr(
        cloud_sync,
        "_inject_obs_exif_into_field_image",
        lambda *a, **k: inject_calls.append(a),
    )
    open_count = {"n": 0}
    real_open = Image.open

    def counting_open(*a, **k):
        open_count["n"] += 1
        return real_open(*a, **k)

    monkeypatch.setattr(Image, "open", counting_open)

    first = cloud_sync._backfill_missing_exif_on_cloud_images()
    assert first["opened"] == 1
    assert first["updated"] == 1
    assert len(inject_calls) == 1
    assert open_count["n"] == 1

    # Second run: the file version is unchanged, so it is skipped without
    # opening/decoding the image again.
    second = cloud_sync._backfill_missing_exif_on_cloud_images()
    assert second["skipped_cached"] == 1
    assert second["opened"] == 0
    assert len(inject_calls) == 1  # not re-injected
    assert open_count["n"] == 1  # not re-opened


def test_backfill_reopens_only_changed_files(monkeypatch, tmp_path):
    path_a = tmp_path / "a.jpg"
    path_b = tmp_path / "b.jpg"
    Image.new("RGB", (8, 8), "white").save(path_a, format="JPEG")
    Image.new("RGB", (8, 8), "white").save(path_b, format="JPEG")
    db_path = tmp_path / "sporely.db"
    _make_backfill_db(
        db_path,
        [
            (1, str(path_a), 42, "field", "cloud-a"),
            (2, str(path_b), 43, "field", "cloud-b"),
        ],
    )

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(
        cloud_sync.ObservationDB,
        "get_observation",
        lambda observation_id: {"id": observation_id, "gps_latitude": "1.0", "gps_longitude": "2.0", "date": "2026-01-01"},
    )
    inject_paths = []
    monkeypatch.setattr(
        cloud_sync,
        "_inject_obs_exif_into_field_image",
        lambda p, *a, **k: inject_paths.append(str(p)),
    )

    first = cloud_sync._backfill_missing_exif_on_cloud_images()
    assert first["opened"] == 2
    assert first["updated"] == 2
    assert sorted(inject_paths) == sorted([str(path_a), str(path_b)])

    # Change only file A (different size => different signature).
    inject_paths.clear()
    Image.new("RGB", (16, 16), "white").save(path_a, format="JPEG")

    second = cloud_sync._backfill_missing_exif_on_cloud_images()
    assert second["opened"] == 1  # only A reopened
    assert second["skipped_cached"] == 1  # B skipped
    assert inject_paths == [str(path_a)]


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
            "gps_accuracy": "7.5",
            "date": "2026-04-30",
        },
    )
    calls = []

    def fake_inject(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(
        cloud_sync,
        "_inject_obs_exif_into_field_image",
        fake_inject,
    )

    cloud_sync._backfill_missing_exif_on_cloud_images()

    assert calls == [
        (
            (image_path, 59.9, 10.75, 12.5, "2026-04-30"),
            {"gps_accuracy": 7.5},
        )
    ]
