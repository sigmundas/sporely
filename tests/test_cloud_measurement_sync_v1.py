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

    def fake_import_remote_images(remote, local_id, cloud_id, **kwargs):
        call_order.append("images")
        _insert_image(
            db_path,
            id=7,
            observation_id=local_id,
            cloud_id="cloud-image-1",
            filepath="/derived/cloud-image-1.jpg",
            image_type="field",
            sort_order=0,
            created_at="2026-05-01T10:00:00Z",
            scale_microns_per_pixel=None,
        )

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
    assert image == (7, "cloud-image-1", "/derived/cloud-image-1.jpg", None)
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
        filepath="/local/image-2.jpg",
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
            "sort_order": 0,
        },
        {
            "id": "cloud-image-2",
            "observation_id": "cloud-obs-1",
            "image_type": "field",
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

    _patch_test_db_connections(monkeypatch, db_path)

    _insert_image(
        db_path,
        id=11,
        observation_id=1,
        cloud_id="cloud-image-3",
        filepath="/local/image-3.jpg",
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


def test_import_remote_measurements_skips_microscope_image(monkeypatch, tmp_path):
    db_path = _init_measurement_sync_db(tmp_path)

    _patch_test_db_connections(monkeypatch, db_path)

    _insert_image(
        db_path,
        id=13,
        observation_id=1,
        cloud_id="cloud-micro-image",
        filepath="/local/micro-image.jpg",
        image_type="microscope",
        sort_order=0,
        created_at="2026-05-04T10:00:00Z",
        scale_microns_per_pixel=None,
    )

    class DummyClient:
        def pull_measurements_for_images(self, image_cloud_ids):
            raise AssertionError("remote measurements should be supplied directly in this test")

        def set_measurement_desktop_id(self, *args, **kwargs):
            raise AssertionError("microscope-linked measurements must stay skipped")

    remote_images = [
        {
            "id": "cloud-micro-image",
            "observation_id": "cloud-obs-4",
            "image_type": "microscope",
            "sort_order": 0,
        }
    ]
    remote_measurements = [
        {
            "id": "cloud-measurement-4",
            "image_id": "cloud-micro-image",
            "length_um": 14.0,
            "width_um": 6.0,
            "measurement_type": "manual",
            "measured_at": "2026-05-04T12:00:00Z",
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
        measurement_count = conn.execute("SELECT COUNT(*) FROM spore_measurements").fetchone()[0]
    finally:
        conn.close()

    assert result["imported"] == 0
    assert measurement_count == 0
    assert any("excluded image" in warning for warning in result["warnings"])


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

    class TrackingClient(cloud_sync.SporelyCloudClient):
        def __init__(self):
            super().__init__("access-token", "user-123")
            self.pull_calls = []
            self.patch_calls = []
            self.post_calls = []

        def pull_measurements_for_images(self, image_cloud_ids):
            self.pull_calls.append(list(image_cloud_ids))
            return [dict(row) for row in remote_measurements]

    client = TrackingClient()
    client._measurement_supports_media_keys = lambda: False
    client._get = lambda path: pytest.fail(f"unexpected remote select: {path}")
    client._patch = lambda path, payload: client.patch_calls.append((path, dict(payload)))

    def fake_post(path, payload):
        client.post_calls.append((path, dict(payload)))
        return [{"id": f"cloud-post-{int(payload['desktop_id'])}"}]

    client._post = fake_post

    cloud_sync._push_measurements_for_observation(client, 1)

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT id, cloud_id FROM spore_measurements ORDER BY id",
        ).fetchall()
    finally:
        conn.close()

    assert client.pull_calls == [["cloud-image-1"]]
    assert client.patch_calls == [
        (
            "spore_measurements?id=eq.cloud-measurement-1",
            {
                "image_id": "cloud-image-1",
                "user_id": "user-123",
                "desktop_id": 21,
                "length_um": 10.0,
                "width_um": 5.0,
                "measurement_type": "manual",
                "gallery_rotation": None,
                "p1_x": 1.0,
                "p1_y": 2.0,
                "p2_x": 3.0,
                "p2_y": 4.0,
                "p3_x": 5.0,
                "p3_y": 6.0,
                "p4_x": 7.0,
                "p4_y": 8.0,
                "measured_at": "2026-05-01T12:00:00Z",
            },
        ),
        (
            "spore_measurements?id=eq.cloud-measurement-2",
            {
                "image_id": "cloud-image-1",
                "user_id": "user-123",
                "desktop_id": 22,
                "length_um": 11.0,
                "width_um": 5.5,
                "measurement_type": "manual",
                "gallery_rotation": None,
                "p1_x": 1.0,
                "p1_y": 2.0,
                "p2_x": 3.0,
                "p2_y": 4.0,
                "p3_x": 5.0,
                "p3_y": 6.0,
                "p4_x": 7.0,
                "p4_y": 8.0,
                "measured_at": "2026-05-01T13:00:00Z",
            },
        ),
    ]
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
                "gallery_rotation": None,
                "p1_x": 1.0,
                "p1_y": 2.0,
                "p2_x": 3.0,
                "p2_y": 4.0,
                "p3_x": 5.0,
                "p3_y": 6.0,
                "p4_x": 7.0,
                "p4_y": 8.0,
                "measured_at": "2026-05-01T14:00:00Z",
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
    assert client.patch_calls == [
        (
            "spore_measurements?id=eq.cloud-measurement-1",
            {
                "image_id": "cloud-image-1",
                "user_id": "user-123",
                "desktop_id": 21,
                "length_um": 10.0,
                "width_um": 5.0,
                "measurement_type": "manual",
                "gallery_rotation": None,
                "p1_x": 1.0,
                "p1_y": 2.0,
                "p2_x": 3.0,
                "p2_y": 4.0,
                "p3_x": 5.0,
                "p3_y": 6.0,
                "p4_x": 7.0,
                "p4_y": 8.0,
                "measured_at": "2026-05-01T12:00:00Z",
            },
        )
    ]
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
                "gallery_rotation": None,
                "p1_x": 1.0,
                "p1_y": 2.0,
                "p2_x": 3.0,
                "p2_y": 4.0,
                "p3_x": 5.0,
                "p3_y": 6.0,
                "p4_x": 7.0,
                "p4_y": 8.0,
                "measured_at": "2026-05-01T13:00:00Z",
            },
        )
    ]
    assert rows == [
        (21, "cloud-measurement-1"),
        (22, None),
        (23, None),
    ]
