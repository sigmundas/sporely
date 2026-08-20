from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from database import models
from utils import cloud_sync


def _connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def test_mark_cloud_observations_dirty_for_pending_local_images_marks_synced_observations_with_pending_cloud_recovery_media_and_clears_stale_signatures(
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
        CREATE TABLE settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        INSERT INTO observations (id, cloud_id, sync_status) VALUES
            (389, '631', 'synced'),
            (390, '632', 'synced');
        INSERT INTO images (
            id, observation_id, image_type, cloud_id, sort_order, notes, source_role, file_purpose
        ) VALUES
            (1, 389, 'field', NULL, 0, '', 'cloud_recovery_cache', 'cache'),
            (3, 390, 'field', NULL, 0, 'generated media crop', 'cloud_recovery_cache', 'cache');
        INSERT INTO settings (key, value) VALUES
            ('sporely_cloud_local_media_sig_obs_389', 'stale-signature');
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: _connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: _connect(db_path))

    # Metadata-only sync (default): the pending scan is a no-op and must NOT
    # dirty observations, even for stub cloud-cache rows that lack cloud_id.
    cloud_sync._mark_cloud_observations_dirty_for_pending_local_images()

    check_conn = _connect(db_path)
    try:
        rows = check_conn.execute(
            "SELECT id, sync_status FROM observations ORDER BY id"
        ).fetchall()
        settings_rows = dict(
            check_conn.execute("SELECT key, value FROM settings").fetchall()
        )
    finally:
        check_conn.close()

    # Metadata-only mode: no observation is re-dirtied, no media signature cleared.
    assert [dict(row) for row in rows] == [
        {"id": 389, "sync_status": "synced"},
        {"id": 390, "sync_status": "synced"},
    ]
    assert settings_rows == {"sporely_cloud_local_media_sig_obs_389": "stale-signature"}

    # Explicit media-upload mode re-runs the scan and behaves like the old code:
    # cache rows without cloud_id trigger a re-dirty so the metadata patch can
    # repair them; the row without any pending image (obs 390) stays synced.
    cloud_sync._mark_cloud_observations_dirty_for_pending_local_images(
        include_pending_local_media_uploads=True,
    )
    check_conn = _connect(db_path)
    try:
        rows = check_conn.execute(
            "SELECT id, sync_status FROM observations ORDER BY id"
        ).fetchall()
        settings_rows = dict(
            check_conn.execute("SELECT key, value FROM settings").fetchall()
        )
    finally:
        check_conn.close()
    assert [dict(row) for row in rows] == [
        {"id": 389, "sync_status": "dirty"},
        {"id": 390, "sync_status": "synced"},
    ]
    # The media-mode scan also seeds the per-image storage-intent ledger for
    # every candidate observation before evaluating pending rows.
    assert settings_rows == {
        "sporely_cloud_local_media_sig_obs_389": "",
        "sporely_cloud_image_storage_intent_ids_389": "[1]",
        "sporely_cloud_image_storage_intent_ids_390": "[3]",
    }


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
    monkeypatch.setattr(models, "get_connection", lambda: _connect(db_path))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: calls.append("media"))
    monkeypatch.setattr(
        cloud_sync,
        "_cloud_pending_image_repair_scan_due",
        lambda: (True, "test_due"),
    )
    monkeypatch.setattr(
        cloud_sync,
        "_mark_cloud_observations_dirty_for_pending_local_images",
        lambda **kwargs: calls.append(f"pending:{kwargs.get('include_pending_local_media_uploads')}"),
    )
    monkeypatch.setattr(
        cloud_sync,
        "push_calibrations",
        lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []},
    )

    result = cloud_sync.push_all(SimpleNamespace(user_id="user-123"), sync_images=True, sync_calibrations=False)

    # sync_images=True must forward the explicit media-upload flag so the
    # dirty scan actually runs. Metadata-only sync (sync_images=False) would
    # skip the pending scan entirely.
    assert calls == ["media", "pending:True"]
    assert result["pushed"] == 0
    # The bare-minimum schema in this test drives some downstream reconciliation
    # paths to noise about missing tables; ignore those and only assert on the
    # scan-invocation invariant this test exists to pin.


def test_pending_image_repair_scan_cadence(monkeypatch):
    now = datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc)
    settings: dict[str, str] = {}
    monkeypatch.setattr(
        cloud_sync.SettingsDB,
        "get_setting",
        lambda key, default=None: settings.get(key, default),
    )

    due, reason = cloud_sync._cloud_pending_image_repair_scan_due(now)
    assert due is True
    assert reason.startswith("version_")

    settings[cloud_sync._CLOUD_PENDING_IMAGE_REPAIR_VERSION_SETTING] = str(
        cloud_sync._CLOUD_PENDING_IMAGE_REPAIR_VERSION
    )
    settings[cloud_sync._CLOUD_PENDING_IMAGE_REPAIR_AT_SETTING] = (
        now - timedelta(hours=1)
    ).isoformat()
    assert cloud_sync._cloud_pending_image_repair_scan_due(now) == (
        False,
        "fresh_watermark",
    )

    settings[cloud_sync._CLOUD_PENDING_IMAGE_REPAIR_AT_SETTING] = (
        now - timedelta(hours=25)
    ).isoformat()
    assert cloud_sync._cloud_pending_image_repair_scan_due(now) == (
        True,
        "stale_watermark",
    )


def test_push_all_skips_pending_image_repair_scan_with_fresh_watermark(
    tmp_path,
    monkeypatch,
    capsys,
):
    db_path = tmp_path / "cloud_push_fresh_repair.sqlite"
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

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: _connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: _connect(db_path))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(
        cloud_sync,
        "_cloud_pending_image_repair_scan_due",
        lambda: (False, "fresh_watermark"),
    )
    monkeypatch.setattr(
        cloud_sync,
        "_mark_cloud_observations_dirty_for_pending_local_images",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("repair scan must be skipped")),
    )
    monkeypatch.setattr(
        cloud_sync,
        "push_calibrations",
        lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []},
    )

    cloud_sync.push_all(
        SimpleNamespace(user_id="user-123"),
        sync_images=True,
        sync_calibrations=False,
    )

    assert "pending image dirty scan skipped reason=fresh_watermark" in capsys.readouterr().out


def test_push_all_metadata_only_skips_pending_local_image_scan(tmp_path, monkeypatch):
    """sync_images=False must NOT invoke the dirty scan — that's the whole
    point of gating it behind include_pending_local_media_uploads."""
    db_path = tmp_path / "cloud_push_meta_only.sqlite"
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
    monkeypatch.setattr(models, "get_connection", lambda: _connect(db_path))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: calls.append("media"))
    monkeypatch.setattr(
        cloud_sync,
        "_mark_cloud_observations_dirty_for_pending_local_images",
        lambda **kwargs: calls.append("pending"),
    )
    monkeypatch.setattr(
        cloud_sync,
        "push_calibrations",
        lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []},
    )

    cloud_sync.push_all(SimpleNamespace(user_id="user-123"), sync_images=False, sync_calibrations=False)

    assert calls == ["media"], (
        "Metadata-only sync must run the media-change scan but skip the "
        f"pending-local-images scan; got {calls!r}"
    )
