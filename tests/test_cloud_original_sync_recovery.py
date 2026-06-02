import json
import sqlite3
from pathlib import Path

import pytest

from database import models
from utils import cloud_sync
from utils import original_sync_policy as policy


def _create_sync_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "sporely.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cloud_id TEXT
            );
            CREATE TABLE images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observation_id INTEGER,
                cloud_id TEXT,
                filepath TEXT,
                original_filepath TEXT,
                source_role TEXT,
                file_purpose TEXT,
                image_type TEXT,
                micro_category TEXT,
                sort_order INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                synced_at TEXT
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


def _patch_db_connections(monkeypatch, db_path: Path) -> None:
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))


def _patch_app_data_dir(monkeypatch, tmp_path: Path) -> Path:
    app_root = tmp_path / "appdata"
    monkeypatch.setattr(cloud_sync, "app_data_dir", lambda: app_root)
    return app_root


def _enable_original_sync(db_path: Path, enabled: bool = True) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (policy.SYNC_FULL_RESOLUTION_ORIGINALS_SETTING, "true" if enabled else "false"),
        )
        conn.commit()
    finally:
        conn.close()


def _store_snapshot(db_path: Path, cloud_id: str, payload: dict) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (f"sporely_cloud_snapshot_obs_{cloud_id}", json.dumps(payload)),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_observation(db_path: Path, *, observation_id: int, cloud_id: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id) VALUES (?, ?)",
            (observation_id, cloud_id),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_image(
    db_path: Path,
    *,
    image_id: int,
    observation_id: int,
    filepath: Path,
    source_role: str,
    file_purpose: str,
    cloud_id: str | None = None,
    original_filepath: Path | None = None,
    image_type: str = "field",
) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath,
                source_role, file_purpose, image_type, micro_category, sort_order, created_at, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                image_id,
                observation_id,
                cloud_id,
                str(filepath),
                str(original_filepath) if original_filepath else None,
                source_role,
                file_purpose,
                image_type,
                None,
                0,
                "2026-05-01T10:00:00Z",
                None,
            ),
        )
        conn.commit()
    finally:
        conn.close()


class _RecoveryClient(cloud_sync.SporelyCloudClient):
    def __init__(self, remote_images: list[dict] | None = None, payload: bytes = b"cloud-original"):
        super().__init__("token", "user-123")
        self.remote_images = [dict(row or {}) for row in (remote_images or [])]
        self.payload = payload
        self.pull_calls = 0
        self.download_calls: list[str] = []
        self.fail_download = False
        self.fail_pull = False

    def pull_image_metadata(self, obs_cloud_id: str, include_deleted_for_sync: bool = False) -> list[dict]:
        self.pull_calls += 1
        if self.fail_pull:
            raise AssertionError("pull_image_metadata should not have been called")
        return [
            dict(row)
            for row in self.remote_images
            if str(row.get("observation_id") or "").strip() == str(obs_cloud_id or "").strip()
        ]

    def download_image_file(self, storage_path: str, dest_path: str | Path) -> Path:
        self.download_calls.append(str(storage_path))
        if self.fail_download:
            raise cloud_sync.CloudSyncError("Cloud image file is missing from storage")
        dest = Path(dest_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(self.payload)
        return dest


def _fetch_image_row(db_path: Path, image_id: int) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT * FROM images WHERE id = ?", (image_id,)).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def test_recovery_skips_when_setting_is_disabled(monkeypatch, tmp_path):
    db_path = _create_sync_db(tmp_path)
    _patch_db_connections(monkeypatch, db_path)
    _patch_app_data_dir(monkeypatch, tmp_path)
    _seed_observation(db_path, observation_id=1, cloud_id="cloud-obs-1")

    local_path = tmp_path / "working.jpg"
    local_path.write_text("working-bytes", encoding="utf-8")
    _seed_image(
        db_path,
        image_id=11,
        observation_id=1,
        filepath=local_path,
        source_role="converted_local",
        file_purpose="microscope",
        cloud_id="cloud-image-1",
    )

    client = _RecoveryClient(
        remote_images=[
            {
                "id": "cloud-image-1",
                "observation_id": "cloud-obs-1",
                "desktop_id": 11,
                "original_storage_path": "user-123/cloud-obs-1/originals/cloud-image-1/source.heic",
                "original_filename": "source.heic",
            }
        ]
    )

    result = cloud_sync.recover_full_original_for_image(client, 11)

    assert result["status"] == "skipped_disabled"
    assert result["skipped_reason"] == "disabled"
    assert client.pull_calls == 0
    assert client.download_calls == []


def test_recovery_skips_without_original_storage_path(monkeypatch, tmp_path):
    db_path = _create_sync_db(tmp_path)
    _patch_db_connections(monkeypatch, db_path)
    _patch_app_data_dir(monkeypatch, tmp_path)
    _enable_original_sync(db_path, True)
    _seed_observation(db_path, observation_id=1, cloud_id="cloud-obs-1")

    local_path = tmp_path / "working.jpg"
    local_path.write_text("working-bytes", encoding="utf-8")
    _seed_image(
        db_path,
        image_id=11,
        observation_id=1,
        filepath=local_path,
        source_role="converted_local",
        file_purpose="microscope",
        cloud_id="cloud-image-1",
    )

    client = _RecoveryClient(
        remote_images=[
            {
                "id": "cloud-image-1",
                "observation_id": "cloud-obs-1",
                "desktop_id": 11,
                "original_filename": "source.heic",
            }
        ]
    )

    result = cloud_sync.recover_full_original_for_image(client, 11)

    assert result["status"] == "skipped_missing_key"
    assert result["skipped_reason"] == "missing_original_storage_path"
    assert client.download_calls == []


def test_recovery_skips_when_local_canonical_file_exists(monkeypatch, tmp_path):
    db_path = _create_sync_db(tmp_path)
    _patch_db_connections(monkeypatch, db_path)
    _patch_app_data_dir(monkeypatch, tmp_path)
    _enable_original_sync(db_path, True)
    _seed_observation(db_path, observation_id=1, cloud_id="cloud-obs-1")

    local_path = tmp_path / "canonical.jpg"
    local_path.write_text("canonical-bytes", encoding="utf-8")
    _seed_image(
        db_path,
        image_id=11,
        observation_id=1,
        filepath=local_path,
        source_role="local_canonical",
        file_purpose="field",
        cloud_id="cloud-image-1",
    )

    client = _RecoveryClient(
        remote_images=[
            {
                "id": "cloud-image-1",
                "observation_id": "cloud-obs-1",
                "desktop_id": 11,
                "original_storage_path": "user-123/cloud-obs-1/originals/cloud-image-1/source.heic",
                "original_filename": "source.heic",
            }
        ]
    )
    client.fail_pull = True

    result = cloud_sync.recover_full_original_for_image(client, 11)

    assert result["status"] == "skipped_existing_local_original"
    assert result["skipped_reason"] == "existing_local_original"
    assert client.pull_calls == 0
    assert client.download_calls == []


def test_recovery_skips_when_converted_local_original_exists(monkeypatch, tmp_path):
    db_path = _create_sync_db(tmp_path)
    _patch_db_connections(monkeypatch, db_path)
    _patch_app_data_dir(monkeypatch, tmp_path)
    _enable_original_sync(db_path, True)
    _seed_observation(db_path, observation_id=1, cloud_id="cloud-obs-1")

    working_path = tmp_path / "working.jpg"
    working_path.write_text("working-bytes", encoding="utf-8")
    original_path = tmp_path / "source.heic"
    original_path.write_text("source-bytes", encoding="utf-8")
    _seed_image(
        db_path,
        image_id=11,
        observation_id=1,
        filepath=working_path,
        original_filepath=original_path,
        source_role="converted_local",
        file_purpose="microscope",
        cloud_id="cloud-image-1",
    )

    client = _RecoveryClient(
        remote_images=[
            {
                "id": "cloud-image-1",
                "observation_id": "cloud-obs-1",
                "desktop_id": 11,
                "original_storage_path": "user-123/cloud-obs-1/originals/cloud-image-1/source.heic",
                "original_filename": "source.heic",
            }
        ]
    )
    client.fail_pull = True

    result = cloud_sync.recover_full_original_for_image(client, 11)

    assert result["status"] == "skipped_existing_local_original"
    assert result["skipped_reason"] == "existing_local_original"
    assert client.pull_calls == 0
    assert client.download_calls == []


def test_recovery_downloads_to_cache_and_writes_sidecar_without_mutating_local_row(monkeypatch, tmp_path):
    db_path = _create_sync_db(tmp_path)
    _patch_db_connections(monkeypatch, db_path)
    _patch_app_data_dir(monkeypatch, tmp_path)
    _enable_original_sync(db_path, True)
    _seed_observation(db_path, observation_id=1, cloud_id="cloud-obs-1")

    working_path = tmp_path / "working.jpg"
    working_path.write_text("working-bytes", encoding="utf-8")
    _seed_image(
        db_path,
        image_id=11,
        observation_id=1,
        filepath=working_path,
        source_role="converted_local",
        file_purpose="microscope",
        cloud_id="cloud-image-1",
    )
    before_row = _fetch_image_row(db_path, 11)

    client = _RecoveryClient(
        remote_images=[
            {
                "id": "cloud-image-1",
                "observation_id": "cloud-obs-1",
                "desktop_id": 11,
                "original_storage_path": "user-123/cloud-obs-1/originals/cloud-image-1/source.heic",
                "original_filename": "source.heic",
            }
        ],
        payload=b"cloud-original-bytes",
    )

    result = cloud_sync.recover_full_original_for_image(client, 11)

    expected_cache_path = (
        tmp_path
        / "appdata"
        / "cloud_cache"
        / "originals"
        / "user-123"
        / "cloud-obs-1"
        / "cloud-image-1"
        / "source.heic"
    )
    expected_sidecar_path = expected_cache_path.with_name(f"{expected_cache_path.name}.json")

    assert result["status"] == "downloaded_to_cache"
    assert result["skipped_reason"] is None
    assert result["downloaded_path"] == expected_cache_path
    assert result["bytes"] == len(b"cloud-original-bytes")
    assert result["used_original_storage_path"] == "user-123/cloud-obs-1/originals/cloud-image-1/source.heic"
    assert result["warnings"] == []
    assert result["errors"] == []
    assert client.download_calls == ["user-123/cloud-obs-1/originals/cloud-image-1/source.heic"]
    assert expected_cache_path.exists()
    assert expected_cache_path.read_bytes() == b"cloud-original-bytes"
    assert expected_sidecar_path.exists()
    sidecar = json.loads(expected_sidecar_path.read_text(encoding="utf-8"))
    assert sidecar["source_role"] == "cloud_recovery_cache"
    assert sidecar["file_purpose"] == "cache"
    assert sidecar["original_storage_path"] == "user-123/cloud-obs-1/originals/cloud-image-1/source.heic"
    assert sidecar["downloaded_path"] == str(expected_cache_path)
    assert _fetch_image_row(db_path, 11) == before_row


def test_recovery_failed_download_does_not_mutate_local_row(monkeypatch, tmp_path):
    db_path = _create_sync_db(tmp_path)
    _patch_db_connections(monkeypatch, db_path)
    _patch_app_data_dir(monkeypatch, tmp_path)
    _enable_original_sync(db_path, True)
    _seed_observation(db_path, observation_id=1, cloud_id="cloud-obs-1")

    working_path = tmp_path / "working.jpg"
    working_path.write_text("working-bytes", encoding="utf-8")
    _seed_image(
        db_path,
        image_id=11,
        observation_id=1,
        filepath=working_path,
        source_role="converted_local",
        file_purpose="microscope",
        cloud_id="cloud-image-1",
    )
    before_row = _fetch_image_row(db_path, 11)

    client = _RecoveryClient(
        remote_images=[
            {
                "id": "cloud-image-1",
                "observation_id": "cloud-obs-1",
                "desktop_id": 11,
                "original_storage_path": "user-123/cloud-obs-1/originals/cloud-image-1/source.heic",
                "original_filename": "source.heic",
            }
        ]
    )
    client.fail_download = True

    result = cloud_sync.recover_full_original_for_image(client, 11)

    assert result["status"] == "download_failed"
    assert result["downloaded_path"] is None
    assert client.download_calls == ["user-123/cloud-obs-1/originals/cloud-image-1/source.heic"]
    assert _fetch_image_row(db_path, 11) == before_row
    expected_cache_path = (
        tmp_path
        / "appdata"
        / "cloud_cache"
        / "originals"
        / "user-123"
        / "cloud-obs-1"
        / "cloud-image-1"
        / "source.heic"
    )
    assert not expected_cache_path.exists()
    assert not expected_cache_path.with_name(f"{expected_cache_path.name}.json").exists()


def test_recovery_is_idempotent_when_cache_exists(monkeypatch, tmp_path):
    db_path = _create_sync_db(tmp_path)
    _patch_db_connections(monkeypatch, db_path)
    _patch_app_data_dir(monkeypatch, tmp_path)
    _enable_original_sync(db_path, True)
    _seed_observation(db_path, observation_id=1, cloud_id="cloud-obs-1")

    working_path = tmp_path / "working.jpg"
    working_path.write_text("working-bytes", encoding="utf-8")
    _seed_image(
        db_path,
        image_id=11,
        observation_id=1,
        filepath=working_path,
        source_role="converted_local",
        file_purpose="microscope",
        cloud_id="cloud-image-1",
    )

    client = _RecoveryClient(
        remote_images=[
            {
                "id": "cloud-image-1",
                "observation_id": "cloud-obs-1",
                "desktop_id": 11,
                "original_storage_path": "user-123/cloud-obs-1/originals/cloud-image-1/source.heic",
                "original_filename": "source.heic",
            }
        ],
        payload=b"cloud-original-bytes",
    )

    first_result = cloud_sync.recover_full_original_for_image(client, 11)
    second_result = cloud_sync.recover_full_original_for_image(client, 11)

    expected_cache_path = (
        tmp_path
        / "appdata"
        / "cloud_cache"
        / "originals"
        / "user-123"
        / "cloud-obs-1"
        / "cloud-image-1"
        / "source.heic"
    )

    assert first_result["status"] == "downloaded_to_cache"
    assert second_result["status"] == "skipped_existing_cache"
    assert second_result["skipped_reason"] == "existing_cache"
    assert second_result["downloaded_path"] == expected_cache_path
    assert client.download_calls == ["user-123/cloud-obs-1/originals/cloud-image-1/source.heic"]
    assert expected_cache_path.exists()


def test_recovery_uses_snapshot_original_storage_path_without_live_fetch(monkeypatch, tmp_path):
    db_path = _create_sync_db(tmp_path)
    _patch_db_connections(monkeypatch, db_path)
    _patch_app_data_dir(monkeypatch, tmp_path)
    _enable_original_sync(db_path, True)
    _seed_observation(db_path, observation_id=1, cloud_id="cloud-obs-1")

    working_path = tmp_path / "working.jpg"
    working_path.write_text("working-bytes", encoding="utf-8")
    _seed_image(
        db_path,
        image_id=11,
        observation_id=1,
        filepath=working_path,
        source_role="converted_local",
        file_purpose="microscope",
        cloud_id="cloud-image-1",
    )

    snapshot_payload = {
        "observation": {"id": "cloud-obs-1", "desktop_id": 1},
        "images": [
            {
                "id": "cloud-image-1",
                "observation_id": "cloud-obs-1",
                "desktop_id": 11,
                "original_storage_path": "user-123/cloud-obs-1/originals/cloud-image-1/source.heic",
                "original_filename": "source.heic",
            }
        ],
        "measurements": [],
    }
    _store_snapshot(db_path, "cloud-obs-1", snapshot_payload)

    client = _RecoveryClient(
        remote_images=[
            {
                "id": "cloud-image-1",
                "observation_id": "cloud-obs-1",
                "desktop_id": 11,
                "original_storage_path": "live/should/not/be/used.heic",
                "original_filename": "live.heic",
            }
        ],
        payload=b"snapshot-original-bytes",
    )
    client.fail_pull = True

    result = cloud_sync.recover_full_original_for_image(client, 11)

    expected_cache_path = (
        tmp_path
        / "appdata"
        / "cloud_cache"
        / "originals"
        / "user-123"
        / "cloud-obs-1"
        / "cloud-image-1"
        / "source.heic"
    )

    assert result["status"] == "downloaded_to_cache"
    assert result["used_original_storage_path"] == "user-123/cloud-obs-1/originals/cloud-image-1/source.heic"
    assert client.pull_calls == 0
    assert result["downloaded_path"] == expected_cache_path
    assert expected_cache_path.exists()
