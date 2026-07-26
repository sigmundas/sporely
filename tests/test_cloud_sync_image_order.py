from __future__ import annotations

import sqlite3
from pathlib import Path

from database import models
from utils import cloud_sync


def _create_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT
            );
            CREATE TABLE images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observation_id INTEGER,
                filepath TEXT,
                cloud_id TEXT,
                sort_order INTEGER,
                image_type TEXT,
                source_role TEXT,
                created_at TEXT
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def _seed_image(
    db_path: Path,
    *,
    observation_id: int,
    sort_order: int,
    image_type: str,
    source_role: str,
    created_at: str,
) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            """
            INSERT INTO images (
                observation_id, filepath, cloud_id, sort_order, image_type, source_role, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                observation_id,
                f"/tmp/{source_role}-{image_type}-{sort_order}.jpg",
                None,
                sort_order,
                image_type,
                source_role,
                created_at,
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()


def test_cloud_pulled_mixed_images_are_normalized_field_first(tmp_path, monkeypatch):
    db_path = tmp_path / "sporely.db"
    _create_db(db_path)

    _seed_image(
        db_path,
        observation_id=1,
        sort_order=0,
        image_type="microscope",
        source_role="converted_local",
        created_at="2026-07-18 10:00:00",
    )
    _seed_image(
        db_path,
        observation_id=1,
        sort_order=1,
        image_type="microscope",
        source_role="converted_local",
        created_at="2026-07-18 10:01:00",
    )
    _seed_image(
        db_path,
        observation_id=1,
        sort_order=0,
        image_type="field",
        source_role="cloud_recovery_cache",
        created_at="2026-07-18 09:00:00",
    )
    _seed_image(
        db_path,
        observation_id=1,
        sort_order=1,
        image_type="field",
        source_role="cloud_recovery_cache",
        created_at="2026-07-18 09:01:00",
    )

    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))

    cloud_sync._normalize_cloud_pulled_image_order(1)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT id, image_type, sort_order FROM images ORDER BY sort_order ASC, id ASC"
        ).fetchall()
    finally:
        conn.close()

    assert [row["image_type"] for row in rows] == [
        "field",
        "field",
        "microscope",
        "microscope",
    ]
    assert [row["sort_order"] for row in rows] == [0, 1, 2, 3]
