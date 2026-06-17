from __future__ import annotations

import sqlite3
from types import SimpleNamespace

from utils import cloud_sync


def _connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def test_mark_cloud_observations_dirty_for_pending_local_images_marks_synced_observations_with_unsynced_microscope_media(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "cloud_dirty.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY,
            cloud_id TEXT,
            sync_status TEXT
        );
        CREATE TABLE images (
            id INTEGER PRIMARY KEY,
            observation_id INTEGER,
            image_type TEXT,
            cloud_id TEXT,
            sort_order INTEGER,
            notes TEXT,
            source_role TEXT,
            file_purpose TEXT
        );
        INSERT INTO observations (id, cloud_id, sync_status) VALUES
            (389, '631', 'synced'),
            (390, '632', 'synced');
        INSERT INTO images (
            id, observation_id, image_type, cloud_id, sort_order, notes, source_role, file_purpose
        ) VALUES
            (1, 389, 'field', '1624', 0, '', 'cloud_recovery_cache', 'cache'),
            (2, 389, 'microscope', NULL, 1, '', 'local_canonical', 'microscope'),
            (3, 390, 'field', NULL, 0, '', 'cloud_recovery_cache', 'cache');
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: _connect(db_path))

    cloud_sync._mark_cloud_observations_dirty_for_pending_local_images()

    check_conn = _connect(db_path)
    try:
        rows = check_conn.execute(
            "SELECT id, sync_status FROM observations ORDER BY id"
        ).fetchall()
    finally:
        check_conn.close()

    assert [dict(row) for row in rows] == [
        {"id": 389, "sync_status": "dirty"},
        {"id": 390, "sync_status": "synced"},
    ]


def test_push_all_invokes_pending_local_image_dirty_scan(tmp_path, monkeypatch):
    db_path = tmp_path / "cloud_push.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY,
            date TEXT,
            cloud_id TEXT,
            sync_status TEXT
        );
        """
    )
    conn.commit()
    conn.close()

    calls: list[str] = []

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: _connect(db_path))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: calls.append("media"))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_pending_local_images", lambda: calls.append("pending"))
    monkeypatch.setattr(
        cloud_sync,
        "push_calibrations",
        lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []},
    )

    result = cloud_sync.push_all(SimpleNamespace(user_id="user-123"), sync_images=True, sync_calibrations=False)

    assert calls == ["media", "pending"]
    assert result["pushed"] == 0
    assert result["errors"] == []
