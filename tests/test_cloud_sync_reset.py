import sqlite3

from database import models


def test_reset_cloud_sync_state_clears_local_cloud_references(monkeypatch, tmp_path):
    db_path = tmp_path / "sporely.db"
    app_settings = {
        "cloud_last_pull_at": "2026-01-02T03:04:05Z",
        "linked_cloud_user_id": "old-user",
        "cloud_recent_import_local_ids": "[1]",
        "cloud_user_email": "keep@example.com",
    }
    saved_settings = []

    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY,
            cloud_id TEXT,
            sync_status TEXT,
            synced_at TEXT
        );
        CREATE TABLE images (
            id INTEGER PRIMARY KEY,
            observation_id INTEGER,
            filepath TEXT,
            cloud_id TEXT,
            synced_at TEXT
        );
        CREATE TABLE spore_measurements (
            id INTEGER PRIMARY KEY,
            cloud_id TEXT
        );
        CREATE TABLE settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        INSERT INTO observations (id, cloud_id, sync_status, synced_at)
        VALUES (1, 'cloud-obs', 'synced', '2026-01-02');
        INSERT INTO images (id, observation_id, filepath, cloud_id, synced_at)
        VALUES (10, 1, '/local/image.jpg', 'cloud-image', '2026-01-02');
        INSERT INTO spore_measurements (id, cloud_id)
        VALUES (20, 'cloud-measurement');
        INSERT INTO settings (key, value)
        VALUES
            ('sporely_cloud_snapshot_obs_cloud-obs', 'snapshot'),
            ('sporely_cloud_image_file_sig_1_10', 'file-sig'),
            ('sporely_cloud_local_media_sig_obs_1', 'media-sig'),
            ('sporely_cloud_media_signature_v1', 'global-sig'),
            ('profile_name', 'Local User');
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_app_settings", lambda: dict(app_settings))

    def save_app_settings(settings):
        saved_settings.append(dict(settings))
        app_settings.clear()
        app_settings.update(settings)

    monkeypatch.setattr(models, "save_app_settings", save_app_settings)

    summary = models.reset_cloud_sync_state()

    conn = sqlite3.connect(db_path)
    try:
        obs = conn.execute("SELECT cloud_id, sync_status, synced_at FROM observations").fetchone()
        image = conn.execute("SELECT filepath, cloud_id, synced_at FROM images").fetchone()
        measurement = conn.execute("SELECT cloud_id FROM spore_measurements").fetchone()
        settings_rows = dict(conn.execute("SELECT key, value FROM settings").fetchall())
    finally:
        conn.close()

    assert obs == (None, "dirty", None)
    assert image == ("/local/image.jpg", None, None)
    assert measurement == (None,)
    assert settings_rows == {"profile_name": "Local User"}
    assert "cloud_last_pull_at" not in app_settings
    assert "linked_cloud_user_id" not in app_settings
    assert "cloud_recent_import_local_ids" not in app_settings
    assert app_settings["cloud_user_email"] == "keep@example.com"
    assert saved_settings
    assert summary == {
        "observations": 1,
        "images": 1,
        "measurements": 1,
        "settings": 4,
        "app_settings": 3,
    }

