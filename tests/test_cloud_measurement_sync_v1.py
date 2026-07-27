import json
import sqlite3

import pytest

from database import models, schema
from utils import cloud_sync


def _init_measurement_sync_db(tmp_path):
    db_path = tmp_path / "sporely.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cloud_id TEXT,
            sync_status TEXT,
            synced_at TEXT
        );
        CREATE TABLE images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observation_id INTEGER,
            cloud_id TEXT,
            filepath TEXT,
            image_type TEXT,
            sort_order INTEGER,
            created_at TEXT,
            micro_category TEXT,
            notes TEXT,
            scale_microns_per_pixel REAL,
            measure_color TEXT,
            mount_medium TEXT,
            stain TEXT,
            sample_type TEXT,
            contrast TEXT,
            objective_name TEXT,
            resample_scale_factor REAL,
            gps_source INTEGER,
            crop_mode TEXT,
            ai_crop_x1 REAL,
            ai_crop_y1 REAL,
            ai_crop_x2 REAL,
            ai_crop_y2 REAL,
            ai_crop_source_w INTEGER,
            ai_crop_source_h INTEGER,
            ai_crop_is_custom INTEGER
        );
        CREATE TABLE spore_measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            image_id INTEGER,
            cloud_id TEXT,
            desktop_id INTEGER,
            length_um REAL,
            width_um REAL,
            measurement_type TEXT,
            gallery_rotation INTEGER,
            p1_x REAL,
            p1_y REAL,
            p2_x REAL,
            p2_y REAL,
            p3_x REAL,
            p3_y REAL,
            p4_x REAL,
            p4_y REAL,
            measured_at TEXT,
            notes TEXT
        );
        """
    )
    schema._ensure_image_tombstones_table(conn.cursor())
    conn.commit()
    conn.close()
    return db_path


def _insert_image(db_path, **kwargs):
    conn = sqlite3.connect(db_path)
    try:
        columns = list(kwargs.keys())
        placeholders = ", ".join("?" for _ in columns)
        conn.execute(
            f"INSERT INTO images ({', '.join(columns)}) VALUES ({placeholders})",
            [kwargs[column] for column in columns],
        )
        conn.commit()
    finally:
        conn.close()


def _insert_measurement(db_path, **kwargs):
    conn = sqlite3.connect(db_path)
    try:
        columns = list(kwargs.keys())
        placeholders = ", ".join("?" for _ in columns)
        conn.execute(
            f"INSERT INTO spore_measurements ({', '.join(columns)}) VALUES ({placeholders})",
            [kwargs[column] for column in columns],
        )
        conn.commit()
    finally:
        conn.close()


def _patch_test_db_connections(monkeypatch, db_path):
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))


def test_create_local_from_remote_imports_measurements_from_cloud_anchored_image(monkeypatch, tmp_path):
    db_path = _init_measurement_sync_db(tmp_path)
    call_order = []
    desktop_id_calls = []
    image_file = tmp_path / "derived" / "cloud-image-1.jpg"
    image_file.parent.mkdir(parents=True, exist_ok=True)
    image_file.write_bytes(b"field image")

    class DummyClient:
        def pull_measurements_for_images(self, image_cloud_ids):
            raise AssertionError("remote measurements should be threaded through, not refetched")

        def set_measurement_desktop_id(self, cloud_measurement_id, desktop_id):
            desktop_id_calls.append((cloud_measurement_id, desktop_id))

    def fake_create_observation(**kwargs):
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("INSERT INTO observations DEFAULT VALUES")
            local_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.commit()
            return local_id
        finally:
            conn.close()

    def fake_import_remote_images(client, remote, local_id, cloud_id, **kwargs):
        call_order.append("images")
        _insert_image(
            db_path,
            id=7,
            observation_id=local_id,
            cloud_id="cloud-image-1",
            filepath=str(image_file),
            image_type="field",
            sort_order=0,
            created_at="2026-05-01T10:00:00Z",
            scale_microns_per_pixel=None,
        )
        return {
            "imported": 1,
            "metadata_applied": 0,
            "skipped_materialization": 0,
            "failed": 0,
            "warnings": [],
            "errors": [],
            "complete": True,
        }

    _patch_test_db_connections(monkeypatch, db_path)
    monkeypatch.setattr(cloud_sync.ObservationDB, "create_observation", fake_create_observation)
    monkeypatch.setattr(cloud_sync, "_import_remote_images", fake_import_remote_images)
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *args, **kwargs: None)

    remote = {
        "id": "cloud-obs-1",
        "date": "2026-05-01",
        "interesting_comment": True,
    }
    remote_images = [
        {
            "id": "cloud-image-1",
            "observation_id": "cloud-obs-1",
            "image_type": "field",
            "storage_path": "user/cloud-obs-1/cloud-image-1.jpg",
            "sort_order": 0,
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

    local_id = cloud_sync._create_local_from_remote(
        remote,
        client=DummyClient(),
        remote_images=remote_images,
        remote_measurements=remote_measurements,
    )

    conn = sqlite3.connect(db_path)
    try:
        observation = conn.execute(
            "SELECT cloud_id, sync_status, synced_at FROM observations WHERE id = ?",
            (local_id,),
        ).fetchone()
        image = conn.execute(
            "SELECT id, cloud_id, filepath, scale_microns_per_pixel FROM images WHERE observation_id = ?",
            (local_id,),
        ).fetchone()
        measurement = conn.execute(
            """
            SELECT image_id, cloud_id, desktop_id, length_um, width_um, measurement_type,
                   gallery_rotation, p1_x, p1_y, p2_x, p2_y, p3_x, p3_y, p4_x, p4_y,
                   measured_at, notes
            FROM spore_measurements
            WHERE image_id = ?
            """,
            (7,),
        ).fetchone()
    finally:
        conn.close()

    assert local_id == 1
    assert call_order == ["images"]
    assert observation[0] == "cloud-obs-1"
    assert observation[1] == "synced"
    assert image == (7, "cloud-image-1", str(image_file), None)
    assert measurement == (
        7,
        "cloud-measurement-1",
        None,
        12.5,
        7.5,
        "manual",
        90,
        1.1,
        2.2,
        3.3,
        4.4,
        5.5,
        6.6,
        7.7,
        8.8,
        "2026-05-01T12:00:00Z",
        None,
    )
    assert desktop_id_calls == [("cloud-measurement-1", 1)]


def test_import_remote_measurements_skips_when_image_missing_and_cannot_materialize(monkeypatch, tmp_path):
    db_path = _init_measurement_sync_db(tmp_path)
    warnings = []

    class DummyClient:
        def pull_measurements_for_images(self, image_cloud_ids):
            raise AssertionError("remote measurements should be supplied directly in this test")

        def set_measurement_desktop_id(self, *args, **kwargs):
            raise AssertionError("no measurement should be created when the image cannot be materialized")

    _patch_test_db_connections(monkeypatch, db_path)
    monkeypatch.setattr(cloud_sync, "_apply_remote_images_to_local", lambda *args, **kwargs: [])

    remote_images = [
        {
            "id": "cloud-image-missing",
            "observation_id": "cloud-obs-2",
            "image_type": "field",
            "storage_path": "user/cloud-obs-2/cloud-image-missing.jpg",
            "sort_order": 0,
        }
    ]
    remote_measurements = [
        {
            "id": "cloud-measurement-2",
            "image_id": "cloud-image-missing",
            "length_um": 20.0,
            "width_um": 10.0,
            "measurement_type": "manual",
            "measured_at": "2026-05-02T12:00:00Z",
        }
    ]

    result = cloud_sync._import_remote_measurements_for_observation(
        DummyClient(),
        local_id=1,
        cloud_id="cloud-obs-2",
        remote_images=remote_images,
        remote_measurements=remote_measurements,
    )
    warnings.extend(result["warnings"])

    conn = sqlite3.connect(db_path)
    try:
        measurement_count = conn.execute("SELECT COUNT(*) FROM spore_measurements").fetchone()[0]
    finally:
        conn.close()

    assert result["imported"] == 0
    assert result["conflict"] is False
    assert measurement_count == 0
    assert any("could not be materialized" in warning for warning in warnings)


def test_import_remote_measurements_skips_tombstoned_image_and_keeps_unrelated_measurements(
    monkeypatch,
    tmp_path,
):
    db_path = _init_measurement_sync_db(tmp_path)
    image_2_path = tmp_path / "image-2.jpg"
    image_2_path.write_bytes(b"image 2")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", None),
        )
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?)
            """,
            ("cloud-image-1", "2026-05-01 10:00:00", 1, 7),
        )
        conn.commit()
    finally:
        conn.close()

    _patch_test_db_connections(monkeypatch, db_path)
    _insert_image(
        db_path,
        id=11,
        observation_id=1,
        cloud_id="cloud-image-2",
        filepath=str(image_2_path),
        image_type="field",
        sort_order=0,
        created_at="2026-05-02T09:00:00Z",
        scale_microns_per_pixel=0.5,
    )

    class DummyClient:
        def pull_measurements_for_images(self, image_cloud_ids):
            raise AssertionError("remote measurements should be supplied directly in this test")

        def set_measurement_desktop_id(self, *args, **kwargs):
            pass

    remote_images = [
        {
            "id": "cloud-image-1",
            "observation_id": "cloud-obs-1",
            "image_type": "field",
            "storage_path": "user/cloud-obs-1/cloud-image-1.jpg",
            "sort_order": 0,
        },
        {
            "id": "cloud-image-2",
            "observation_id": "cloud-obs-1",
            "image_type": "field",
            "storage_path": "user/cloud-obs-1/cloud-image-2.jpg",
            "sort_order": 1,
        },
    ]
    remote_measurements = [
        {
            "id": "cloud-measurement-1",
            "image_id": "cloud-image-1",
            "length_um": 9.0,
            "width_um": 4.5,
            "measurement_type": "manual",
            "measured_at": "2026-05-01T12:00:00Z",
        },
        {
            "id": "cloud-measurement-2",
            "image_id": "cloud-image-2",
            "length_um": 13.0,
            "width_um": 6.5,
            "measurement_type": "manual",
            "measured_at": "2026-05-02T12:00:00Z",
        },
    ]

    result = cloud_sync._import_remote_measurements_for_observation(
        DummyClient(),
        local_id=1,
        cloud_id="cloud-obs-1",
        remote_images=remote_images,
        remote_measurements=remote_measurements,
    )

    conn = sqlite3.connect(db_path)
    try:
        measurement_rows = conn.execute(
            "SELECT image_id, cloud_id, length_um FROM spore_measurements ORDER BY id",
        ).fetchall()
    finally:
        conn.close()

    assert result["imported"] == 1
    assert result["conflict"] is False
    assert measurement_rows == [(11, "cloud-measurement-2", 13.0)]
    assert any("local tombstone" in warning for warning in result["warnings"])


def test_import_remote_measurements_does_not_anchor_to_unrelated_local_image_id(monkeypatch, tmp_path):
    db_path = _init_measurement_sync_db(tmp_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-2", "dirty", None),
        )
        conn.commit()
    finally:
        conn.close()

    _patch_test_db_connections(monkeypatch, db_path)
    _insert_image(
        db_path,
        id=42,
        observation_id=1,
        cloud_id="unrelated-cloud-image",
        filepath="/local/unrelated-image.jpg",
        image_type="field",
        sort_order=0,
        created_at="2026-05-02T09:00:00Z",
        scale_microns_per_pixel=0.5,
    )
    monkeypatch.setattr(cloud_sync, "_apply_remote_images_to_local", lambda *args, **kwargs: [])

    class DummyClient:
        def pull_measurements_for_images(self, image_cloud_ids):
            raise AssertionError("remote measurements should be supplied directly in this test")

        def set_measurement_desktop_id(self, *args, **kwargs):
            raise AssertionError("measurement should be skipped when the image cannot be materialized")

    remote_images = [
        {
            "id": "cloud-image-unsafe",
            "desktop_id": 42,
            "observation_id": "cloud-obs-2",
            "image_type": "field",
            "storage_path": "user/cloud-obs-2/cloud-image-unsafe.jpg",
            "sort_order": 0,
        }
    ]
    remote_measurements = [
        {
            "id": "cloud-measurement-unsafe",
            "image_id": "cloud-image-unsafe",
            "length_um": 20.0,
            "width_um": 10.0,
            "measurement_type": "manual",
            "measured_at": "2026-05-02T12:00:00Z",
        }
    ]

    result = cloud_sync._import_remote_measurements_for_observation(
        DummyClient(),
        local_id=1,
        cloud_id="cloud-obs-2",
        remote_images=remote_images,
        remote_measurements=remote_measurements,
    )

    conn = sqlite3.connect(db_path)
    try:
        measurement_rows = conn.execute(
            "SELECT image_id, cloud_id FROM spore_measurements ORDER BY id",
        ).fetchall()
    finally:
        conn.close()

    assert result["imported"] == 0
    assert result["conflict"] is False
    assert measurement_rows == []
    assert any("could not be materialized" in warning for warning in result["warnings"])


def test_import_remote_measurements_skips_conflicting_local_edit(monkeypatch, tmp_path):
    db_path = _init_measurement_sync_db(tmp_path)
    image_3_path = tmp_path / "image-3.jpg"
    image_3_path.write_bytes(b"image 3")

    _patch_test_db_connections(monkeypatch, db_path)

    _insert_image(
        db_path,
        id=11,
        observation_id=1,
        cloud_id="cloud-image-3",
        filepath=str(image_3_path),
        image_type="field",
        sort_order=0,
        created_at="2026-05-03T10:00:00Z",
        scale_microns_per_pixel=0.5,
    )
    _insert_measurement(
        db_path,
        id=22,
        image_id=11,
        cloud_id="cloud-measurement-3",
        desktop_id=22,
        length_um=10.0,
        width_um=5.0,
        measurement_type="manual",
        gallery_rotation=0,
        p1_x=1.0,
        p1_y=2.0,
        p2_x=3.0,
        p2_y=4.0,
        p3_x=5.0,
        p3_y=6.0,
        p4_x=7.0,
        p4_y=8.0,
        measured_at="2026-05-03T12:00:00Z",
        notes="local note",
    )

    class DummyClient:
        def pull_measurements_for_images(self, image_cloud_ids):
            raise AssertionError("remote measurements should be supplied directly in this test")

        def set_measurement_desktop_id(self, *args, **kwargs):
            raise AssertionError("conflicting measurements must not be overwritten")

    remote_images = [
        {
            "id": "cloud-image-3",
            "observation_id": "cloud-obs-3",
            "image_type": "field",
            "storage_path": "user/cloud-obs-3/cloud-image-3.jpg",
            "sort_order": 0,
        }
    ]
    remote_measurements = [
        {
            "id": "cloud-measurement-3",
            "desktop_id": 0,
            "image_id": "cloud-image-3",
            "length_um": 11.0,
            "width_um": 5.0,
            "measurement_type": "manual",
            "gallery_rotation": 0,
            "p1_x": 1.0,
            "p1_y": 2.0,
            "p2_x": 3.0,
            "p2_y": 4.0,
            "p3_x": 5.0,
            "p4_x": 7.0,
            "p4_y": 8.0,
            "p3_y": 6.0,
            "measured_at": "2026-05-03T12:00:00Z",
        }
    ]

    result = cloud_sync._import_remote_measurements_for_observation(
        DummyClient(),
        local_id=1,
        cloud_id="cloud-obs-3",
        remote_images=remote_images,
        remote_measurements=remote_measurements,
    )

    conn = sqlite3.connect(db_path)
    try:
        measurement = conn.execute(
            "SELECT length_um, width_um, cloud_id FROM spore_measurements WHERE id = ?",
            (22,),
        ).fetchone()
    finally:
        conn.close()

    assert result["imported"] == 0
    assert result["conflict"] is True
    assert measurement == (10.0, 5.0, "cloud-measurement-3")
    assert any("local copy changed" in warning for warning in result["warnings"])


def test_clear_observation_dirty_keeps_local_measurement_changes(monkeypatch, tmp_path):
    db_path = _init_measurement_sync_db(tmp_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-5", "dirty", None),
        )
        conn.commit()
    finally:
        conn.close()

    _insert_image(
        db_path,
        id=15,
        observation_id=1,
        cloud_id="cloud-image-5",
        filepath="/local/image-5.jpg",
        image_type="field",
        sort_order=0,
        created_at="2026-05-05T10:00:00Z",
        scale_microns_per_pixel=0.5,
    )
    _insert_measurement(
        db_path,
        id=35,
        image_id=15,
        cloud_id="cloud-measurement-5",
        desktop_id=35,
        length_um=11.0,
        width_um=5.0,
        measurement_type="manual",
        gallery_rotation=0,
        p1_x=1.0,
        p1_y=2.0,
        p2_x=3.0,
        p2_y=4.0,
        p3_x=5.0,
        p3_y=6.0,
        p4_x=7.0,
        p4_y=8.0,
        measured_at="2026-05-05T12:00:00Z",
        notes="edited locally",
    )

    _patch_test_db_connections(monkeypatch, db_path)
    monkeypatch.setattr(
        cloud_sync,
        "_load_cloud_observation_snapshot",
        lambda cloud_id: json.dumps(
            {
                "observation": {"id": "cloud-obs-5"},
                "images": [
                    {
                        "id": "cloud-image-5",
                        "desktop_id": 15,
                        "sort_order": 0,
                        "image_type": "field",
                    }
                ],
                "measurements": [
                    {
                        "id": "cloud-measurement-5",
                        "desktop_id": 35,
                        "image_id": "cloud-image-5",
                        "length_um": 10.0,
                        "width_um": 5.0,
                        "measurement_type": "manual",
                        "gallery_rotation": 0,
                        "p1_x": 1.0,
                        "p1_y": 2.0,
                        "p2_x": 3.0,
                        "p2_y": 4.0,
                        "p3_x": 5.0,
                        "p3_y": 6.0,
                        "p4_x": 7.0,
                        "p4_y": 8.0,
                        "measured_at": "2026-05-05T12:00:00Z",
                    }
                ],
            },
            sort_keys=True,
        ),
    )
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda *args, **kwargs: "sig")
    monkeypatch.setattr(cloud_sync, "_local_cloud_media_signature", lambda *args, **kwargs: "sig")
    monkeypatch.setattr(cloud_sync, "_store_local_media_signature_if_equivalent", lambda *args, **kwargs: None)

    assert cloud_sync._clear_observation_dirty_if_no_real_changes(1, "cloud-obs-5") is False

    conn = sqlite3.connect(db_path)
    try:
        sync_status = conn.execute("SELECT sync_status FROM observations WHERE id = 1").fetchone()[0]
    finally:
        conn.close()

    assert sync_status == "dirty"


def test_clear_observation_dirty_allows_matching_measurements_to_clear_dirty(monkeypatch, tmp_path):
    db_path = _init_measurement_sync_db(tmp_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-6", "dirty", None),
        )
        conn.commit()
    finally:
        conn.close()

    _insert_image(
        db_path,
        id=16,
        observation_id=1,
        cloud_id="cloud-image-6",
        filepath="/local/image-6.jpg",
        image_type="field",
        sort_order=0,
        created_at="2026-05-06T10:00:00Z",
        scale_microns_per_pixel=0.5,
    )
    _insert_measurement(
        db_path,
        id=36,
        image_id=16,
        cloud_id="cloud-measurement-6",
        desktop_id=36,
        length_um=11.0,
        width_um=5.0,
        measurement_type="manual",
        gallery_rotation=0,
        p1_x=1.0,
        p1_y=2.0,
        p2_x=3.0,
        p2_y=4.0,
        p3_x=5.0,
        p3_y=6.0,
        p4_x=7.0,
        p4_y=8.0,
        measured_at="2026-05-06T12:00:00Z",
    )

    remote_observation = {
        "id": "cloud-obs-6",
    }
    remote_image = {
        "id": "cloud-image-6",
        "observation_id": "cloud-obs-6",
        "image_type": "field",
        "sort_order": 0,
    }
    remote_measurement = {
        "id": "cloud-measurement-6",
        "desktop_id": 36,
        "image_id": "cloud-image-6",
        "length_um": 11.0,
        "width_um": 5.0,
        "measurement_type": "manual",
        "gallery_rotation": 0,
        "p1_x": 1.0,
        "p1_y": 2.0,
        "p2_x": 3.0,
        "p2_y": 4.0,
        "p3_x": 5.0,
        "p3_y": 6.0,
        "p4_x": 7.0,
        "p4_y": 8.0,
        "measured_at": "2026-05-06T12:00:00Z",
    }
    snapshot_json = cloud_sync._cloud_observation_snapshot(
        remote_observation,
        [remote_image],
        [remote_measurement],
    )

    _patch_test_db_connections(monkeypatch, db_path)
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: snapshot_json)
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda *args, **kwargs: "sig")
    monkeypatch.setattr(cloud_sync, "_local_cloud_media_signature", lambda *args, **kwargs: "sig")
    monkeypatch.setattr(cloud_sync, "_store_local_media_signature_if_equivalent", lambda *args, **kwargs: None)

    assert cloud_sync._clear_observation_dirty_if_no_real_changes(1, "cloud-obs-6") is True

    conn = sqlite3.connect(db_path)
    try:
        sync_status = conn.execute("SELECT sync_status FROM observations WHERE id = 1").fetchone()[0]
    finally:
        conn.close()

    assert sync_status == "synced"


def test_measurement_push_diff_fields_normalize_semantic_equivalents():
    local_row = {
        "id": 36,
        "cloud_id": "cloud-measurement-6",
        "image_cloud_id": "cloud-image-6",
        "length_um": "11.0000000001",
        "width_um": "5.0",
        "measurement_type": " Manual ",
        "gallery_rotation": "",
        "p1_x": "1.0",
        "p1_y": "2.0",
        "p2_x": "3.0",
        "p2_y": "4.0",
        "p3_x": "5.0",
        "p3_y": "6.0",
        "p4_x": "7.0",
        "p4_y": "8.0",
        "measured_at": "2026-05-06 12:00:00+00:00",
    }
    remote_row = {
        "id": "cloud-measurement-6",
        "desktop_id": 36,
        "image_id": "cloud-image-6",
        "length_um": 11.0,
        "width_um": "5",
        "measurement_type": "manual",
        "gallery_rotation": 0,
        "p1_x": 1.0,
        "p1_y": 2.0,
        "p2_x": 3.0,
        "p2_y": 4.0,
        "p3_x": 5.0,
        "p3_y": 6.0,
        "p4_x": 7.0,
        "p4_y": 8.0,
        "measured_at": "2026-05-06T12:00:00Z",
    }

    assert cloud_sync._measurement_payloads_match(
        local_row,
        remote_row,
        cloud_image_id="cloud-image-6",
    )
    assert cloud_sync._measurement_push_diff_fields(
        local_row,
        remote_row,
        cloud_image_id="cloud-image-6",
    ) == []


@pytest.mark.parametrize(
    ("field", "local_value", "remote_value"),
    [
        ("length_um", 10.0, 11.0),
        ("width_um", 5.0, 6.0),
        ("p1_x", 1.0, 1.25),
        ("measurement_type", "manual", "automatic"),
        ("measured_at", "2026-05-06T12:00:00Z", "2026-05-06T12:01:00Z"),
    ],
)
def test_measurement_push_diff_fields_reports_real_changes(field, local_value, remote_value):
    local_row = {
        "id": 36,
        "cloud_id": "cloud-measurement-6",
        "image_cloud_id": "cloud-image-6",
        "length_um": 11.0,
        "width_um": 5.0,
        "measurement_type": "manual",
        "gallery_rotation": 0,
        "p1_x": 1.0,
        "p1_y": 2.0,
        "p2_x": 3.0,
        "p2_y": 4.0,
        "p3_x": 5.0,
        "p3_y": 6.0,
        "p4_x": 7.0,
        "p4_y": 8.0,
        "measured_at": "2026-05-06T12:00:00Z",
    }
    remote_row = {
        "id": "cloud-measurement-6",
        "desktop_id": 36,
        "image_id": "cloud-image-6",
        "length_um": 11.0,
        "width_um": 5.0,
        "measurement_type": "manual",
        "gallery_rotation": 0,
        "p1_x": 1.0,
        "p1_y": 2.0,
        "p2_x": 3.0,
        "p2_y": 4.0,
        "p3_x": 5.0,
        "p3_y": 6.0,
        "p4_x": 7.0,
        "p4_y": 8.0,
        "measured_at": "2026-05-06T12:00:00Z",
    }
    local_row[field] = local_value
    remote_row[field] = remote_value

    assert cloud_sync._measurement_push_diff_fields(
        local_row,
        remote_row,
        cloud_image_id="cloud-image-6",
    ) == [field]


def test_push_measurement_logs_diff_fields_for_real_change(capsys):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    client._measurement_supports_media_keys = lambda: False

    patched_payloads = []

    def fake_patch(path, payload):
        patched_payloads.append((path, dict(payload)))

    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(client, "_patch", fake_patch)
        meas = {
            "id": 36,
            "cloud_id": "cloud-measurement-6",
            "image_id": 16,
            "length_um": 10.0,
            "width_um": 5.0,
            "measurement_type": "manual",
            "gallery_rotation": 0,
            "p1_x": 1.0,
            "p1_y": 2.0,
            "p2_x": 3.0,
            "p2_y": 4.0,
            "p3_x": 5.0,
            "p3_y": 6.0,
            "p4_x": 7.0,
            "p4_y": 8.0,
            "measured_at": "2026-05-06T12:00:00Z",
        }
        remote_measurement_cache = {
            "cloud:cloud-measurement-6": {
                "id": "cloud-measurement-6",
                "desktop_id": 36,
                "image_id": "cloud-image-6",
                "length_um": 11.0,
                "width_um": 5.0,
                "measurement_type": "manual",
                "gallery_rotation": 0,
                "p1_x": 1.0,
                "p1_y": 2.0,
                "p2_x": 3.0,
                "p2_y": 4.0,
                "p3_x": 5.0,
                "p3_y": 6.0,
                "p4_x": 7.0,
                "p4_y": 8.0,
                "measured_at": "2026-05-06T12:00:00Z",
            }
        }

        returned_id = client.push_measurement(
            meas,
            "cloud-image-6",
            remote_measurement_cache=remote_measurement_cache,
        )
    finally:
        monkeypatch.undo()

    output = capsys.readouterr().out
    assert returned_id == "cloud-measurement-6"
    assert patched_payloads and patched_payloads[0][0] == "spore_measurements?id=eq.cloud-measurement-6"
    assert "length_um" in output


def test_import_remote_measurements_imports_from_metadata_only_microscope_anchor(
    monkeypatch,
    tmp_path,
):
    db_path = _init_measurement_sync_db(tmp_path)

    _patch_test_db_connections(monkeypatch, db_path)
    download_calls: list[str] = []
    desktop_id_calls: list[tuple[str, int]] = []

    class DummyClient:
        def pull_measurements_for_images(self, image_cloud_ids):
            raise AssertionError("remote measurements should be supplied directly in this test")

        def download_image_file(self, storage_path, dest_path):
            download_calls.append(storage_path)
            raise AssertionError("metadata-only microscope anchors must not be downloaded")

        def set_measurement_desktop_id(self, cloud_measurement_id, desktop_id):
            desktop_id_calls.append((cloud_measurement_id, desktop_id))

    def fake_add_image(**kwargs):
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.execute(
                """
                INSERT INTO images (
                    observation_id, filepath, image_type, sort_order,
                    mount_medium, stain, sample_type, contrast
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    kwargs["observation_id"],
                    kwargs["filepath"],
                    kwargs["image_type"],
                    kwargs.get("sort_order"),
                    kwargs.get("mount_medium"),
                    kwargs.get("stain"),
                    kwargs.get("sample_type"),
                    kwargs.get("contrast"),
                ),
            )
            conn.commit()
            return int(cursor.lastrowid)
        finally:
            conn.close()

    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", fake_add_image)

    remote_images = [
        {
            "id": "cloud-image-4",
            "observation_id": "cloud-obs-4",
            "image_type": "microscope",
            "storage_path": None,
            "mount_medium": "KOH",
            "stain": "Melzer",
            "sample_type": "Fresh",
            "contrast": "DIC",
            "sort_order": 0,
        }
    ]
    remote_measurements = [
        {
            "id": "cloud-measurement-4",
            "image_id": "cloud-image-4",
            "length_um": 14.0,
            "width_um": 6.0,
            "measurement_type": "manual",
            "measured_at": "2026-05-04T12:00:00Z",
            "gallery_rotation": 0,
        }
    ]

    result = cloud_sync._import_remote_measurements_for_observation(
        DummyClient(),
        local_id=1,
        cloud_id="cloud-obs-4",
        remote_images=remote_images,
        remote_measurements=remote_measurements,
    )

    conn = sqlite3.connect(db_path)
    try:
        image_row = conn.execute(
            """
            SELECT id, cloud_id, filepath, image_type, mount_medium, stain, sample_type, contrast
            FROM images
            ORDER BY id
            """,
        ).fetchone()
        measurement_row = conn.execute(
            """
            SELECT id, image_id, cloud_id, length_um, width_um, measurement_type, gallery_rotation,
                   measured_at
            FROM spore_measurements
            ORDER BY id
            """,
        ).fetchone()
    finally:
        conn.close()

    assert result["imported"] == 1
    assert result["conflict"] is False
    assert result["warnings"] == []
    assert image_row == (
        1,
        "cloud-image-4",
        "",
        "microscope",
        "KOH",
        "Melzer",
        "Fresh",
        "DIC",
    )
    assert measurement_row == (
        1,
        1,
        "cloud-measurement-4",
        14.0,
        6.0,
        "manual",
        0,
        "2026-05-04T12:00:00Z",
    )
    assert download_calls == []
    assert desktop_id_calls == [("cloud-measurement-4", 1)]


def test_import_remote_measurements_groups_generated_mosaic_skips(monkeypatch, tmp_path):
    db_path = _init_measurement_sync_db(tmp_path)

    _patch_test_db_connections(monkeypatch, db_path)
    download_calls: list[str] = []

    class DummyClient:
        def pull_measurements_for_images(self, image_cloud_ids):
            raise AssertionError("remote measurements should be supplied directly in this test")

        def download_image_file(self, storage_path, dest_path):
            download_calls.append(storage_path)
            raise AssertionError("generated mosaic images must not be downloaded")

        def set_measurement_desktop_id(self, *args, **kwargs):
            raise AssertionError("generated mosaic measurements must be skipped")

    remote_images = [
        {
            "id": "cloud-generated-image",
            "observation_id": "cloud-obs-5",
            "image_type": "microscope",
            "notes": "generated media spore mosaic",
            "original_filename": "cloud_extra_mosaic.jpg",
            "sort_order": 0,
        }
    ]
    remote_measurements = [
        {
            "id": "cloud-measurement-5",
            "image_id": "cloud-generated-image",
            "length_um": 12.0,
            "width_um": 5.0,
            "measurement_type": "manual",
            "measured_at": "2026-05-05T12:00:00Z",
        },
        {
            "id": "cloud-measurement-6",
            "image_id": "cloud-generated-image",
            "length_um": 13.0,
            "width_um": 5.5,
            "measurement_type": "manual",
            "measured_at": "2026-05-05T12:05:00Z",
        },
    ]

    result = cloud_sync._import_remote_measurements_for_observation(
        DummyClient(),
        local_id=1,
        cloud_id="cloud-obs-5",
        remote_images=remote_images,
        remote_measurements=remote_measurements,
    )

    conn = sqlite3.connect(db_path)
    try:
        measurement_count = conn.execute("SELECT COUNT(*) FROM spore_measurements").fetchone()[0]
    finally:
        conn.close()

    assert result["imported"] == 0
    assert result["conflict"] is False
    assert measurement_count == 0
    assert len(result["warnings"]) == 1
    assert "skipped 2 cloud measurement(s) on 1 excluded image(s): cloud-generated-image" in result["warnings"][0]
    assert download_calls == []


def test_push_measurements_for_observation_skips_tombstoned_image_measurements(
    monkeypatch,
    tmp_path,
    capsys,
):
    db_path = _init_measurement_sync_db(tmp_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", None),
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

    _patch_test_db_connections(monkeypatch, db_path)
    _insert_image(
        db_path,
        id=11,
        observation_id=1,
        cloud_id="cloud-image-1",
        filepath="/local/micro-image-1.jpg",
        image_type="microscope",
        sort_order=0,
        created_at="2026-05-01T10:00:00Z",
        scale_microns_per_pixel=0.5,
    )
    _insert_image(
        db_path,
        id=12,
        observation_id=1,
        cloud_id="cloud-image-2",
        filepath="/local/micro-image-2.jpg",
        image_type="microscope",
        sort_order=1,
        created_at="2026-05-02T10:00:00Z",
        scale_microns_per_pixel=0.5,
    )
    _insert_measurement(
        db_path,
        id=21,
        image_id=11,
        cloud_id="cloud-measurement-1",
        desktop_id=21,
        length_um=10.0,
        width_um=5.0,
        measurement_type="manual",
        gallery_rotation=0,
        p1_x=1.0,
        p1_y=2.0,
        p2_x=3.0,
        p2_y=4.0,
        p3_x=5.0,
        p3_y=6.0,
        p4_x=7.0,
        p4_y=8.0,
        measured_at="2026-05-01T12:00:00Z",
    )
    _insert_measurement(
        db_path,
        id=22,
        image_id=12,
        cloud_id="cloud-measurement-2",
        desktop_id=22,
        length_um=11.0,
        width_um=5.5,
        measurement_type="manual",
        gallery_rotation=0,
        p1_x=1.0,
        p1_y=2.0,
        p2_x=3.0,
        p2_y=4.0,
        p3_x=5.0,
        p3_y=6.0,
        p4_x=7.0,
        p4_y=8.0,
        measured_at="2026-05-02T12:00:00Z",
    )

    pushed_calls: list[tuple[int, str]] = []

    class DummyClient:
        def pull_measurements_for_images(self, image_cloud_ids):
            return []

        def push_measurement(self, meas, cloud_image_id, remote_measurement_cache=None):
            pushed_calls.append((int(meas["id"]), str(cloud_image_id)))
            return f"cloud-measurement-{int(meas['id'])}"

    cloud_sync._push_measurements_for_observation(DummyClient(), 1)
    output = capsys.readouterr().out

    conn = sqlite3.connect(db_path)
    try:
        measurement_rows = conn.execute(
            "SELECT id, image_id, cloud_id FROM spore_measurements ORDER BY id"
        ).fetchall()
    finally:
        conn.close()

    assert pushed_calls == [(22, "cloud-image-2")]
    assert measurement_rows == [
        (21, 11, "cloud-measurement-1"),
        (22, 12, "cloud-measurement-22"),
    ]
    assert "skipped cloud measurement 21 because cloud image cloud-image-1 has a local tombstone" in output


def test_push_measurements_for_observation_prefetches_identity_cache_once(monkeypatch, tmp_path):
    db_path = _init_measurement_sync_db(tmp_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", None),
        )
        conn.commit()
    finally:
        conn.close()

    _patch_test_db_connections(monkeypatch, db_path)
    _insert_image(
        db_path,
        id=11,
        observation_id=1,
        cloud_id="cloud-image-1",
        filepath="/local/micro-image-1.jpg",
        image_type="microscope",
        sort_order=0,
        created_at="2026-05-01T10:00:00Z",
        scale_microns_per_pixel=0.5,
    )
    _insert_measurement(
        db_path,
        id=21,
        image_id=11,
        cloud_id="cloud-measurement-1",
        desktop_id=21,
        length_um=10.0,
        width_um=5.0,
        measurement_type="manual",
        gallery_rotation=0,
        p1_x=1.0,
        p1_y=2.0,
        p2_x=3.0,
        p2_y=4.0,
        p3_x=5.0,
        p3_y=6.0,
        p4_x=7.0,
        p4_y=8.0,
        measured_at="2026-05-01T12:00:00Z",
    )
    _insert_measurement(
        db_path,
        id=22,
        image_id=11,
        cloud_id=None,
        desktop_id=22,
        length_um=11.0,
        width_um=5.5,
        measurement_type="manual",
        gallery_rotation=0,
        p1_x=1.0,
        p1_y=2.0,
        p2_x=3.0,
        p2_y=4.0,
        p3_x=5.0,
        p3_y=6.0,
        p4_x=7.0,
        p4_y=8.0,
        measured_at="2026-05-01T13:00:00Z",
    )
    _insert_measurement(
        db_path,
        id=23,
        image_id=11,
        cloud_id=None,
        desktop_id=23,
        length_um=12.0,
        width_um=6.0,
        measurement_type="manual",
        gallery_rotation=0,
        p1_x=1.0,
        p1_y=2.0,
        p2_x=3.0,
        p2_y=4.0,
        p3_x=5.0,
        p3_y=6.0,
        p4_x=7.0,
        p4_y=8.0,
        measured_at="2026-05-01T14:00:00Z",
    )

    remote_measurements = [
        {
            "id": "cloud-measurement-1",
            "desktop_id": 21,
            "image_id": "cloud-image-1",
            "length_um": 10.0,
            "width_um": 5.0,
            "measurement_type": "manual",
            "gallery_rotation": 0,
            "p1_x": 1.0,
            "p1_y": 2.0,
            "p2_x": 3.0,
            "p2_y": 4.0,
            "p3_x": 5.0,
            "p4_x": 7.0,
            "p4_y": 8.0,
            "p3_y": 6.0,
            "measured_at": "2026-05-01T12:00:00Z",
        },
        {
            "id": "cloud-measurement-2",
            "desktop_id": 22,
            "image_id": "cloud-image-1",
            "length_um": 11.0,
            "width_um": 5.5,
            "measurement_type": "manual",
            "gallery_rotation": 0,
            "p1_x": 1.0,
            "p1_y": 2.0,
            "p2_x": 3.0,
            "p2_y": 4.0,
            "p3_x": 5.0,
            "p4_x": 7.0,
            "p4_y": 8.0,
            "p3_y": 6.0,
            "measured_at": "2026-05-01T13:00:00Z",
        },
    ]
    remote_measurements_state = [dict(row) for row in remote_measurements]

    class TrackingClient(cloud_sync.SporelyCloudClient):
        def __init__(self):
            super().__init__("access-token", "user-123")
            self.pull_calls = []
            self.patch_calls = []
            self.post_calls = []

        def pull_measurements_for_images(self, image_cloud_ids):
            self.pull_calls.append(list(image_cloud_ids))
            return [dict(row) for row in remote_measurements_state]

        def pull_image_metadata(self, obs_cloud_id, include_deleted_for_sync=False):
            return [{
                "id": "cloud-image-1",
                "observation_id": str(obs_cloud_id),
                "desktop_id": 11,
                "image_type": "microscope",
                "storage_path": "user-123/cloud-obs-1/micro.webp",
                "deleted_at": None,
            }]

    client = TrackingClient()
    client._measurement_supports_media_keys = lambda: False
    client._get = lambda path: pytest.fail(f"unexpected remote select: {path}")
    client._patch = lambda path, payload: client.patch_calls.append((path, dict(payload)))

    def fake_post(path, payload):
        client.post_calls.append((path, dict(payload)))
        new_cloud_id = f"cloud-post-{int(payload['desktop_id'])}"
        remote_measurements_state.append(
            {
                "id": new_cloud_id,
                "desktop_id": payload["desktop_id"],
                "image_id": payload["image_id"],
                "length_um": payload["length_um"],
                "width_um": payload["width_um"],
                "measurement_type": payload["measurement_type"],
                "gallery_rotation": payload["gallery_rotation"],
                "p1_x": payload["p1_x"],
                "p1_y": payload["p1_y"],
                "p2_x": payload["p2_x"],
                "p2_y": payload["p2_y"],
                "p3_x": payload["p3_x"],
                "p3_y": payload["p3_y"],
                "p4_x": payload["p4_x"],
                "p4_y": payload["p4_y"],
                "measured_at": payload["measured_at"],
            }
        )
        return [{"id": new_cloud_id}]

    client._post = fake_post

    cloud_sync._push_measurements_for_observation(client, 1)
    cloud_sync._push_measurements_for_observation(client, 1)

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT id, cloud_id FROM spore_measurements ORDER BY id",
        ).fetchall()
    finally:
        conn.close()

    assert client.pull_calls == [["cloud-image-1"], ["cloud-image-1"]]
    assert client.patch_calls == []
    assert client.post_calls == [
        (
            "spore_measurements",
            {
                "image_id": "cloud-image-1",
                "user_id": "user-123",
                "desktop_id": 23,
                "length_um": 12.0,
                "width_um": 6.0,
                "measurement_type": "manual",
                "gallery_rotation": 0,
                "p1_x": 1.0,
                "p1_y": 2.0,
                "p2_x": 3.0,
                "p2_y": 4.0,
                "p3_x": 5.0,
                "p3_y": 6.0,
                "p4_x": 7.0,
                "p4_y": 8.0,
                "measured_at": "2026-05-01T14:00:00+00:00",
            },
        )
    ]
    assert rows == [
        (21, "cloud-measurement-1"),
        (22, "cloud-measurement-2"),
        (23, "cloud-post-23"),
    ]


def test_push_measurements_for_observation_aborts_on_transient_failure(monkeypatch, tmp_path):
    db_path = _init_measurement_sync_db(tmp_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", None),
        )
        conn.commit()
    finally:
        conn.close()

    _patch_test_db_connections(monkeypatch, db_path)
    _insert_image(
        db_path,
        id=11,
        observation_id=1,
        cloud_id="cloud-image-1",
        filepath="/local/micro-image-1.jpg",
        image_type="microscope",
        sort_order=0,
        created_at="2026-05-01T10:00:00Z",
        scale_microns_per_pixel=0.5,
    )
    _insert_measurement(
        db_path,
        id=21,
        image_id=11,
        cloud_id="cloud-measurement-1",
        desktop_id=21,
        length_um=10.0,
        width_um=5.0,
        measurement_type="manual",
        gallery_rotation=0,
        p1_x=1.0,
        p1_y=2.0,
        p2_x=3.0,
        p2_y=4.0,
        p3_x=5.0,
        p3_y=6.0,
        p4_x=7.0,
        p4_y=8.0,
        measured_at="2026-05-01T12:00:00Z",
    )
    _insert_measurement(
        db_path,
        id=22,
        image_id=11,
        cloud_id=None,
        desktop_id=22,
        length_um=11.0,
        width_um=5.5,
        measurement_type="manual",
        gallery_rotation=0,
        p1_x=1.0,
        p1_y=2.0,
        p2_x=3.0,
        p2_y=4.0,
        p3_x=5.0,
        p3_y=6.0,
        p4_x=7.0,
        p4_y=8.0,
        measured_at="2026-05-01T13:00:00Z",
    )
    _insert_measurement(
        db_path,
        id=23,
        image_id=11,
        cloud_id=None,
        desktop_id=23,
        length_um=12.0,
        width_um=6.0,
        measurement_type="manual",
        gallery_rotation=0,
        p1_x=1.0,
        p1_y=2.0,
        p2_x=3.0,
        p2_y=4.0,
        p3_x=5.0,
        p3_y=6.0,
        p4_x=7.0,
        p4_y=8.0,
        measured_at="2026-05-01T14:00:00Z",
    )

    remote_measurements = [
        {
            "id": "cloud-measurement-1",
            "desktop_id": 21,
            "image_id": "cloud-image-1",
            "length_um": 10.0,
            "width_um": 5.0,
            "measurement_type": "manual",
            "gallery_rotation": 0,
            "p1_x": 1.0,
            "p1_y": 2.0,
            "p2_x": 3.0,
            "p2_y": 4.0,
            "p3_x": 5.0,
            "p4_x": 7.0,
            "p4_y": 8.0,
            "p3_y": 6.0,
            "measured_at": "2026-05-01T12:00:00Z",
        }
    ]

    class TrackingClient(cloud_sync.SporelyCloudClient):
        def __init__(self):
            super().__init__("access-token", "user-123")
            self.pull_calls = []
            self.patch_calls = []
            self.post_calls = []

        def pull_measurements_for_images(self, image_cloud_ids):
            self.pull_calls.append(list(image_cloud_ids))
            return [dict(row) for row in remote_measurements]

        def pull_image_metadata(self, obs_cloud_id, include_deleted_for_sync=False):
            return [{
                "id": "cloud-image-1",
                "observation_id": str(obs_cloud_id),
                "desktop_id": 11,
                "image_type": "microscope",
                "storage_path": "user-123/cloud-obs-1/micro.webp",
                "deleted_at": None,
            }]

    client = TrackingClient()
    client._measurement_supports_media_keys = lambda: False
    client._get = lambda path: pytest.fail(f"unexpected remote select: {path}")
    client._patch = lambda path, payload: client.patch_calls.append((path, dict(payload)))

    def fake_post(path, payload):
        client.post_calls.append((path, dict(payload)))
        raise cloud_sync.CloudTemporarilyUnavailableError(
            "Supabase/cloud sync is temporarily unavailable; local data was not overwritten."
        )

    client._post = fake_post

    with pytest.raises(cloud_sync.CloudTemporarilyUnavailableError):
        cloud_sync._push_measurements_for_observation(client, 1)

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT id, cloud_id FROM spore_measurements ORDER BY id",
        ).fetchall()
    finally:
        conn.close()

    assert client.pull_calls == [["cloud-image-1"]]
    assert client.patch_calls == []
    assert client.post_calls == [
        (
            "spore_measurements",
            {
                "image_id": "cloud-image-1",
                "user_id": "user-123",
                "desktop_id": 22,
                "length_um": 11.0,
                "width_um": 5.5,
                "measurement_type": "manual",
                "gallery_rotation": 0,
                "p1_x": 1.0,
                "p1_y": 2.0,
                "p2_x": 3.0,
                "p2_y": 4.0,
                "p3_x": 5.0,
                "p3_y": 6.0,
                "p4_x": 7.0,
                "p4_y": 8.0,
                "measured_at": "2026-05-01T13:00:00+00:00",
            },
        )
    ]
    assert rows == [
        (21, "cloud-measurement-1"),
        (22, None),
        (23, None),
    ]
