import sqlite3
from pathlib import Path

import pytest

from database import models, schema
from utils import cloud_sync


def _create_image_tombstone_test_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cloud_id TEXT,
            sync_status TEXT,
            updated_at TEXT
        );
        CREATE TABLE images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observation_id INTEGER,
            cloud_id TEXT,
            filepath TEXT NOT NULL,
            original_filepath TEXT,
            image_type TEXT,
            sort_order INTEGER,
            micro_category TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE spore_measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            image_id INTEGER NOT NULL,
            notes TEXT
        );
        CREATE TABLE spore_annotations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            image_id INTEGER NOT NULL,
            measurement_id INTEGER
        );
        CREATE TABLE thumbnails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            image_id INTEGER NOT NULL,
            size_preset TEXT NOT NULL,
            filepath TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(image_id, size_preset)
        );
        """
    )
    schema._ensure_image_tombstones_table(conn.cursor())
    conn.commit()


def _seed_delete_fixture(
    db_path: Path,
    *,
    synced: bool,
    deleted_cloud_id: str = "cloud-image-1",
    observation_cloud_id: str | None = "cloud-obs-1",
):
    images_root = db_path.parent / "images"
    thumbnails_root = db_path.parent / "thumbnails"
    image_path = images_root / "observation-1" / "image.jpg"
    original_path = images_root / "observation-1" / "originals" / "image-original.jpg"
    thumbnail_path = thumbnails_root / "observation-1" / "image-small.jpg"

    image_path.parent.mkdir(parents=True, exist_ok=True)
    original_path.parent.mkdir(parents=True, exist_ok=True)
    thumbnail_path.parent.mkdir(parents=True, exist_ok=True)
    image_path.write_text("image", encoding="utf-8")
    original_path.write_text("original", encoding="utf-8")
    thumbnail_path.write_text("thumb", encoding="utf-8")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, updated_at) VALUES (?, ?, ?, ?)",
            (
                1,
                observation_cloud_id,
                "synced" if synced else "local",
                "2026-05-01 10:00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath, image_type
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                11,
                1,
                deleted_cloud_id if synced else None,
                str(image_path),
                str(original_path),
                "field",
            ),
        )
        conn.execute(
            "INSERT INTO spore_measurements (id, image_id, notes) VALUES (?, ?, ?)",
            (21, 11, "measurement"),
        )
        conn.execute(
            "INSERT INTO spore_annotations (id, image_id, measurement_id) VALUES (?, ?, ?)",
            (31, 11, None),
        )
        conn.execute(
            "INSERT INTO spore_annotations (id, image_id, measurement_id) VALUES (?, ?, ?)",
            (32, 11, 21),
        )
        conn.execute(
            "INSERT INTO thumbnails (id, image_id, size_preset, filepath) VALUES (?, ?, ?, ?)",
            (41, 11, "small", str(thumbnail_path)),
        )
        conn.commit()
    finally:
        conn.close()

    return {
        "images_root": images_root,
        "thumbnails_root": thumbnails_root,
        "image_path": image_path,
        "original_path": original_path,
        "thumbnail_path": thumbnail_path,
        "image_id": 11,
        "observation_id": 1,
        "measurement_id": 21,
        "image_cloud_id": deleted_cloud_id if synced else None,
        "observation_cloud_id": observation_cloud_id,
    }


def test_image_tombstones_helper_creates_table_and_indexes(tmp_path):
    db_path = tmp_path / "schema.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        schema._ensure_image_tombstones_table(conn.cursor())
        conn.commit()

        columns = {row[1] for row in conn.execute("PRAGMA table_info(image_tombstones)").fetchall()}
        assert {
            "deleted_cloud_id",
            "deleted_at",
            "delete_synced_at",
            "deleted_storage_path",
            "deleted_observation_cloud_id",
            "local_observation_id",
            "local_image_id",
            "image_type",
            "filepath",
            "original_filepath",
        }.issubset(columns)

        indexes = {row[1]: row[2] for row in conn.execute("PRAGMA index_list(image_tombstones)").fetchall()}
        assert indexes["idx_image_tombstones_deleted_cloud_id"] == 1
        assert "idx_image_tombstones_delete_synced_at" in indexes
        assert "idx_image_tombstones_deleted_observation_cloud_id" in indexes
    finally:
        conn.close()


def test_get_image_tombstones_by_deleted_cloud_id_filters_matching_ids(monkeypatch, tmp_path):
    db_path = tmp_path / "tombstones.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        schema._ensure_image_tombstones_table(conn.cursor())
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, deleted_observation_cloud_id, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?, ?)
            """,
            ("cloud-image-1", "2026-05-01 10:00:00", "cloud-obs-1", 1, 11),
        )
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, deleted_observation_cloud_id, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?, ?)
            """,
            ("cloud-image-2", "2026-05-02 10:00:00", "cloud-obs-2", 2, 12),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    tombstones = models.get_image_tombstones_by_deleted_cloud_id(
        ["cloud-image-1", "unrelated", "cloud-image-1"]
    )

    assert set(tombstones) == {"cloud-image-1"}
    assert tombstones["cloud-image-1"]["deleted_at"] == "2026-05-01 10:00:00"
    assert tombstones["cloud-image-1"]["deleted_observation_cloud_id"] == "cloud-obs-1"


def test_get_image_tombstones_by_local_image_id_filters_matching_ids(monkeypatch, tmp_path):
    db_path = tmp_path / "local_tombstones.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        schema._ensure_image_tombstones_table(conn.cursor())
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, deleted_observation_cloud_id, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?, ?)
            """,
            ("cloud-image-1", "2026-05-01 10:00:00", "cloud-obs-1", 1, 11),
        )
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, deleted_observation_cloud_id, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?, ?)
            """,
            ("cloud-image-2", "2026-05-02 10:00:00", "cloud-obs-2", 2, 12),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    tombstones = models.get_image_tombstones_by_local_image_id([11, "11", 99])

    assert set(tombstones) == {11}
    assert tombstones[11]["deleted_cloud_id"] == "cloud-image-1"
    assert tombstones[11]["deleted_at"] == "2026-05-01 10:00:00"


def test_list_pending_image_tombstones_returns_unsynced_rows(monkeypatch, tmp_path):
    db_path = tmp_path / "pending_tombstones.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        schema._ensure_image_tombstones_table(conn.cursor())
        conn.executemany(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, delete_synced_at, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                ("cloud-image-2", "2026-05-02 10:00:00", None, 2, 12),
                ("cloud-image-1", "2026-05-01 10:00:00", "2026-05-03 10:00:00", 1, 11),
                ("cloud-image-3", "2026-05-03 10:00:00", None, 3, 13),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    pending = models.list_pending_image_tombstones()

    assert [row["deleted_cloud_id"] for row in pending] == ["cloud-image-2", "cloud-image-3"]
    assert all(row["delete_synced_at"] is None for row in pending)


def test_mark_image_tombstone_synced_sets_delete_synced_at(monkeypatch, tmp_path):
    db_path = tmp_path / "mark_synced.sqlite"
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

    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    updated = models.mark_image_tombstone_synced("cloud-image-1")

    conn = sqlite3.connect(db_path)
    try:
        delete_synced_at = conn.execute(
            "SELECT delete_synced_at FROM image_tombstones WHERE deleted_cloud_id = ?",
            ("cloud-image-1",),
        ).fetchone()[0]
    finally:
        conn.close()

    assert updated is True
    assert delete_synced_at is not None


def test_queue_image_tombstone_for_local_image_keeps_local_row_visible(monkeypatch, tmp_path):
    db_path = tmp_path / "queue_tombstone.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        _create_image_tombstone_test_db(conn)
    finally:
        conn.close()

    fixture = _seed_delete_fixture(db_path, synced=True)
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    queued_cloud_id = models.ImageDB.queue_image_tombstone_for_local_image(fixture["image_id"])

    conn = sqlite3.connect(db_path)
    try:
        tombstone = conn.execute(
            """
            SELECT deleted_cloud_id, delete_synced_at, local_image_id
            FROM image_tombstones
            WHERE deleted_cloud_id = ?
            """,
            (fixture["image_cloud_id"],),
        ).fetchone()
        image_count = conn.execute("SELECT COUNT(*) FROM images WHERE id = ?", (fixture["image_id"],)).fetchone()[0]
    finally:
        conn.close()

    assert queued_cloud_id == fixture["image_cloud_id"]
    assert tombstone == (fixture["image_cloud_id"], None, None)
    assert image_count == 1
    assert models.ImageDB.get_images_for_observation(fixture["observation_id"])


def test_clear_image_cloud_sync_state_removes_cloud_link(monkeypatch, tmp_path):
    db_path = tmp_path / "clear_cloud_state.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        _create_image_tombstone_test_db(conn)
    finally:
        conn.close()

    fixture = _seed_delete_fixture(db_path, synced=True)
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    cleared = models.ImageDB.clear_image_cloud_sync_state(fixture["image_id"])

    conn = sqlite3.connect(db_path)
    try:
        image_row = conn.execute(
            "SELECT cloud_id, synced_at FROM images WHERE id = ?",
            (fixture["image_id"],),
        ).fetchone()
    finally:
        conn.close()

    assert cleared is True
    assert image_row == (None, None)


def test_record_remote_image_tombstones_ignores_active_rows_and_writes_deleted_remote_rows(
    monkeypatch,
    tmp_path,
):
    db_path = tmp_path / "remote_tombstones.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        _create_image_tombstone_test_db(conn)
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, updated_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01 10:00:00"),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))

    recorded = cloud_sync._record_remote_image_tombstones(
        [
            {
                "id": "cloud-image-1",
                "observation_id": "cloud-obs-1",
                "deleted_at": "2026-05-01 10:00:00",
                "storage_path": "user/cloud-obs-1/cloud-image-1.jpg",
            },
            {
                "id": "cloud-image-2",
                "observation_id": "cloud-obs-1",
                "deleted_at": None,
                "storage_path": "user/cloud-obs-1/cloud-image-2.jpg",
            },
        ],
        cloud_observation_id="cloud-obs-1",
    )

    conn = sqlite3.connect(db_path)
    try:
        tombstones = conn.execute(
            """
            SELECT deleted_cloud_id, deleted_at, delete_synced_at, deleted_storage_path,
                   deleted_observation_cloud_id, local_observation_id, local_image_id,
                   image_type, filepath, original_filepath
            FROM image_tombstones
            ORDER BY id
            """
        ).fetchall()
        image_count = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
    finally:
        conn.close()

    assert recorded == {"cloud-image-1"}
    assert len(tombstones) == 1
    deleted_cloud_id, deleted_at, delete_synced_at, deleted_storage_path, deleted_observation_cloud_id, local_observation_id, local_image_id, image_type, filepath, original_filepath = tombstones[0]
    assert deleted_cloud_id == "cloud-image-1"
    assert deleted_at == "2026-05-01 10:00:00"
    assert delete_synced_at is None
    assert deleted_storage_path == "user/cloud-obs-1/cloud-image-1.jpg"
    assert deleted_observation_cloud_id == "cloud-obs-1"
    assert local_observation_id is None
    assert local_image_id is None
    assert image_type is None
    assert filepath is None
    assert original_filepath is None
    assert image_count == 0


def test_record_remote_image_tombstones_records_local_metadata_and_keeps_files(
    monkeypatch,
    tmp_path,
):
    db_path = tmp_path / "remote_tombstone_match.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        _create_image_tombstone_test_db(conn)
        images_root = db_path.parent / "images"
        image_path = images_root / "observation-1" / "image.jpg"
        original_path = images_root / "observation-1" / "originals" / "image-original.jpg"
        image_path.parent.mkdir(parents=True, exist_ok=True)
        original_path.parent.mkdir(parents=True, exist_ok=True)
        image_path.write_text("image", encoding="utf-8")
        original_path.write_text("original", encoding="utf-8")
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, updated_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01 10:00:00"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath, image_type
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (11, 1, "cloud-image-1", str(image_path), str(original_path), "field"),
        )
        conn.execute(
            "INSERT INTO spore_measurements (id, image_id, notes) VALUES (?, ?, ?)",
            (21, 11, "measurement"),
        )
        conn.executemany(
            "INSERT INTO spore_annotations (id, image_id, measurement_id) VALUES (?, ?, ?)",
            [
                (31, 11, None),
                (32, 11, 21),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))

    cloud_sync._record_remote_image_tombstones(
        [
            {
                "id": "cloud-image-1",
                "observation_id": "cloud-obs-1",
                "deleted_at": "2026-05-01 10:00:00",
                "storage_path": "user/cloud-obs-1/cloud-image-1.jpg",
            }
        ],
        local_observation_id=1,
        cloud_observation_id="cloud-obs-1",
    )

    conn = sqlite3.connect(db_path)
    try:
        tombstone = conn.execute(
            """
            SELECT deleted_cloud_id, deleted_at, delete_synced_at, deleted_storage_path,
                   deleted_observation_cloud_id, local_observation_id, local_image_id,
                   image_type, filepath, original_filepath
            FROM image_tombstones
            WHERE deleted_cloud_id = ?
            """,
            ("cloud-image-1",),
        ).fetchone()
        image_row = conn.execute(
            "SELECT COUNT(*) FROM images WHERE id = ?",
            (11,),
        ).fetchone()[0]
        measurement_row = conn.execute(
            "SELECT COUNT(*) FROM spore_measurements WHERE id = ?",
            (21,),
        ).fetchone()[0]
        annotation_rows = conn.execute(
            "SELECT COUNT(*) FROM spore_annotations WHERE image_id = ? OR measurement_id = ?",
            (11, 21),
        ).fetchone()[0]
    finally:
        conn.close()

    assert tombstone is not None
    assert tombstone[0] == "cloud-image-1"
    assert tombstone[1] == "2026-05-01 10:00:00"
    assert tombstone[2] is None
    assert tombstone[3] == "user/cloud-obs-1/cloud-image-1.jpg"
    assert tombstone[4] == "cloud-obs-1"
    assert tombstone[5] == 1
    assert tombstone[6] == 11
    assert tombstone[7] == "field"
    assert tombstone[8] == str(image_path)
    assert tombstone[9] == str(original_path)
    assert image_row == 1
    assert measurement_row == 1
    assert annotation_rows == 2
    assert image_path.exists()
    assert original_path.exists()


def test_record_remote_image_tombstones_matches_local_image_by_desktop_id(
    monkeypatch,
    tmp_path,
):
    db_path = tmp_path / "remote_tombstone_desktop_id.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        _create_image_tombstone_test_db(conn)
        images_root = db_path.parent / "images"
        image_path = images_root / "observation-1" / "image.jpg"
        original_path = images_root / "observation-1" / "originals" / "image-original.jpg"
        image_path.parent.mkdir(parents=True, exist_ok=True)
        original_path.parent.mkdir(parents=True, exist_ok=True)
        image_path.write_text("image", encoding="utf-8")
        original_path.write_text("original", encoding="utf-8")
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, updated_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01 10:00:00"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath, image_type
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (11, 1, None, str(image_path), str(original_path), "field"),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))

    cloud_sync._record_remote_image_tombstones(
        [
            {
                "id": "cloud-image-1",
                "desktop_id": 11,
                "observation_id": "cloud-obs-1",
                "deleted_at": "2026-05-01 10:00:00",
                "storage_path": "user/cloud-obs-1/cloud-image-1.jpg",
            }
        ],
        local_observation_id=1,
        cloud_observation_id="cloud-obs-1",
    )

    conn = sqlite3.connect(db_path)
    try:
        tombstone = conn.execute(
            """
            SELECT deleted_cloud_id, deleted_at, deleted_storage_path,
                   deleted_observation_cloud_id, local_observation_id, local_image_id,
                   filepath, original_filepath
            FROM image_tombstones
            WHERE deleted_cloud_id = ?
            """,
            ("cloud-image-1",),
        ).fetchone()
    finally:
        conn.close()

    assert tombstone == (
        "cloud-image-1",
        "2026-05-01 10:00:00",
        "user/cloud-obs-1/cloud-image-1.jpg",
        "cloud-obs-1",
        1,
        11,
        str(image_path),
        str(original_path),
    )
    assert image_path.exists()
    assert original_path.exists()


def test_tombstoned_local_images_are_hidden_from_active_image_reads(monkeypatch, tmp_path):
    db_path = tmp_path / "hidden_tombstone.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        _create_image_tombstone_test_db(conn)
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, updated_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01 10:00:00"),
        )
        conn.executemany(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath, image_type
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (11, 1, "cloud-image-1", "/tmp/tombstoned.jpg", "/tmp/tombstoned-original.jpg", "field"),
                (12, 1, "cloud-image-2", "/tmp/active.jpg", "/tmp/active-original.jpg", "field"),
            ],
        )
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, deleted_observation_cloud_id, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?, ?)
            """,
            ("cloud-image-1", "2026-05-29 11:26:30", "cloud-obs-1", 1, 11),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    assert models.ImageDB.get_image(11) is None
    assert [row["id"] for row in models.ImageDB.get_images_for_observation(1)] == [12]
    assert [row["id"] for row in models.ImageDB.get_images_by_type(1, "field")] == [12]


def test_observation_table_thumbnail_map_skips_tombstoned_first_image(monkeypatch, tmp_path):
    import os

    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

    from ui.observations_tab import ObservationsTab
    import ui.observations_tab as observations_tab

    db_path = tmp_path / "thumbnail_map.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        _create_image_tombstone_test_db(conn)
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, updated_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01 10:00:00"),
        )

        tombstoned_thumb = tmp_path / "thumbs" / "tombstoned.webp"
        active_thumb = tmp_path / "thumbs" / "active.webp"
        tombstoned_thumb.parent.mkdir(parents=True, exist_ok=True)
        tombstoned_thumb.write_text("tombstoned", encoding="utf-8")
        active_thumb.write_text("active", encoding="utf-8")

        conn.executemany(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath, image_type, sort_order, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    11,
                    1,
                    "cloud-image-1",
                    "/tmp/tombstoned.jpg",
                    "/tmp/tombstoned-original.jpg",
                    "field",
                    0,
                    "2026-05-01 10:00:00",
                ),
                (
                    12,
                    1,
                    "cloud-image-2",
                    "/tmp/active.jpg",
                    "/tmp/active-original.jpg",
                    "field",
                    0,
                    "2026-05-01 10:00:01",
                ),
            ],
        )
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, deleted_observation_cloud_id, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?, ?)
            """,
            ("cloud-image-1", "2026-05-29 11:26:30", "cloud-obs-1", 1, 11),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(observations_tab, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(
        observations_tab,
        "get_thumbnail_path",
        lambda image_id, size: {
            11: str(tombstoned_thumb),
            12: str(active_thumb),
        }.get(image_id),
    )

    tab = ObservationsTab.__new__(ObservationsTab)
    thumbnail_map = tab._build_observation_thumbnail_map([1])

    assert thumbnail_map == {1: str(active_thumb)}


def test_delete_synced_image_writes_tombstone_before_hard_delete_and_marks_observation_dirty(
    monkeypatch,
    tmp_path,
):
    db_path = tmp_path / "delete_synced.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        _create_image_tombstone_test_db(conn)
    finally:
        conn.close()

    fixture = _seed_delete_fixture(db_path, synced=True)
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "_images_dir", lambda: fixture["images_root"])
    monkeypatch.setattr(models, "_thumbnails_dir", lambda: fixture["thumbnails_root"])

    original_upsert = models._upsert_image_tombstone
    tombstone_calls = []

    def wrapped_upsert(cursor, **kwargs):
        tombstone_calls.append(dict(kwargs))
        image_row = cursor.execute("SELECT 1 FROM images WHERE id = ?", (fixture["image_id"],)).fetchone()
        measurement_row = cursor.execute(
            "SELECT 1 FROM spore_measurements WHERE image_id = ?",
            (fixture["image_id"],),
        ).fetchone()
        annotation_rows = cursor.execute(
            "SELECT COUNT(*) FROM spore_annotations WHERE image_id = ? OR measurement_id = ?",
            (fixture["image_id"], fixture["measurement_id"]),
        ).fetchone()[0]
        thumb_row = cursor.execute(
            "SELECT 1 FROM thumbnails WHERE image_id = ?",
            (fixture["image_id"],),
        ).fetchone()
        assert image_row is not None
        assert measurement_row is not None
        assert annotation_rows == 2
        assert thumb_row is not None
        original_upsert(cursor, **kwargs)

    monkeypatch.setattr(models, "_upsert_image_tombstone", wrapped_upsert)

    models.ImageDB.delete_image(fixture["image_id"])

    conn = sqlite3.connect(db_path)
    try:
        tombstone = conn.execute(
            """
            SELECT deleted_cloud_id, deleted_at, delete_synced_at, deleted_storage_path,
                   deleted_observation_cloud_id, local_observation_id, local_image_id,
                   image_type, filepath, original_filepath
            FROM image_tombstones
            WHERE deleted_cloud_id = ?
            """,
            (fixture["image_cloud_id"],),
        ).fetchone()
        observation = conn.execute(
            "SELECT cloud_id, sync_status FROM observations WHERE id = ?",
            (fixture["observation_id"],),
        ).fetchone()
        image_row = conn.execute("SELECT COUNT(*) FROM images WHERE id = ?", (fixture["image_id"],)).fetchone()[0]
        measurement_row = conn.execute(
            "SELECT COUNT(*) FROM spore_measurements WHERE id = ?",
            (fixture["measurement_id"],),
        ).fetchone()[0]
        annotation_rows = conn.execute(
            "SELECT COUNT(*) FROM spore_annotations WHERE image_id = ? OR measurement_id = ?",
            (fixture["image_id"], fixture["measurement_id"]),
        ).fetchone()[0]
        thumbnail_row = conn.execute(
            "SELECT COUNT(*) FROM thumbnails WHERE image_id = ?",
            (fixture["image_id"],),
        ).fetchone()[0]
    finally:
        conn.close()

    assert len(tombstone_calls) == 1
    assert tombstone is not None
    assert tombstone[0] == fixture["image_cloud_id"]
    assert tombstone[2] is None
    assert tombstone[3] == str(fixture["image_path"])
    assert tombstone[4] == fixture["observation_cloud_id"]
    assert tombstone[5] == fixture["observation_id"]
    assert tombstone[6] == fixture["image_id"]
    assert tombstone[7] == "field"
    assert tombstone[8] == str(fixture["image_path"])
    assert tombstone[9] == str(fixture["original_path"])
    assert observation == (fixture["observation_cloud_id"], "dirty")
    assert image_row == 0
    assert measurement_row == 0
    assert annotation_rows == 0
    assert thumbnail_row == 0
    assert not fixture["image_path"].exists()
    assert not fixture["original_path"].exists()
    assert not fixture["thumbnail_path"].exists()


def test_delete_unsynced_image_keeps_hard_delete_without_tombstone(monkeypatch, tmp_path):
    db_path = tmp_path / "delete_unsynced.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        _create_image_tombstone_test_db(conn)
    finally:
        conn.close()

    fixture = _seed_delete_fixture(db_path, synced=False, observation_cloud_id=None)
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "_images_dir", lambda: fixture["images_root"])
    monkeypatch.setattr(models, "_thumbnails_dir", lambda: fixture["thumbnails_root"])
    monkeypatch.setattr(models, "_upsert_image_tombstone", lambda *args, **kwargs: pytest.fail("unexpected tombstone write"))

    models.ImageDB.delete_image(fixture["image_id"])

    conn = sqlite3.connect(db_path)
    try:
        tombstone_count = conn.execute("SELECT COUNT(*) FROM image_tombstones").fetchone()[0]
        observation = conn.execute(
            "SELECT cloud_id, sync_status FROM observations WHERE id = ?",
            (fixture["observation_id"],),
        ).fetchone()
        image_row = conn.execute("SELECT COUNT(*) FROM images WHERE id = ?", (fixture["image_id"],)).fetchone()[0]
    finally:
        conn.close()

    assert tombstone_count == 0
    assert observation == (None, "local")
    assert image_row == 0
    assert not fixture["image_path"].exists()
    assert not fixture["original_path"].exists()
    assert not fixture["thumbnail_path"].exists()


def test_delete_image_preserves_existing_tombstone_and_keeps_single_row(monkeypatch, tmp_path):
    db_path = tmp_path / "delete_duplicate.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        _create_image_tombstone_test_db(conn)
    finally:
        conn.close()

    fixture = _seed_delete_fixture(db_path, synced=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id,
                deleted_at,
                deleted_storage_path,
                local_image_id
            ) VALUES (?, ?, ?, ?)
            """,
            (
                fixture["image_cloud_id"],
                "2026-04-30 09:00:00",
                "/older/path.jpg",
                None,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "_images_dir", lambda: fixture["images_root"])
    monkeypatch.setattr(models, "_thumbnails_dir", lambda: fixture["thumbnails_root"])

    models.ImageDB.delete_image(fixture["image_id"])

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT deleted_cloud_id, deleted_at, deleted_storage_path, local_image_id,
                   local_observation_id, filepath, original_filepath
            FROM image_tombstones
            WHERE deleted_cloud_id = ?
            """,
            (fixture["image_cloud_id"],),
        ).fetchall()
    finally:
        conn.close()

    assert len(rows) == 1
    deleted_cloud_id, deleted_at, deleted_storage_path, local_image_id, local_observation_id, filepath, original_filepath = rows[0]
    assert deleted_cloud_id == fixture["image_cloud_id"]
    assert deleted_at == "2026-04-30 09:00:00"
    assert deleted_storage_path == "/older/path.jpg"
    assert local_image_id == fixture["image_id"]
    assert local_observation_id == fixture["observation_id"]
    assert filepath == str(fixture["image_path"])
    assert original_filepath == str(fixture["original_path"])
