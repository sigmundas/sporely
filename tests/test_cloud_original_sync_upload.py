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


def _seed_image(
    db_path: Path,
    *,
    image_id: int,
    observation_id: int,
    filepath: Path,
    source_role: str,
    file_purpose: str,
    original_filepath: Path | None = None,
    image_type: str = "field",
) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath,
                source_role, file_purpose, image_type, sort_order, created_at, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                image_id,
                observation_id,
                None,
                str(filepath),
                str(original_filepath) if original_filepath else None,
                source_role,
                file_purpose,
                image_type,
                0,
                "2026-05-01T10:00:00Z",
                None,
            ),
        )
        conn.commit()
    finally:
        conn.close()


class _MemoryOriginalSyncClient(cloud_sync.SporelyCloudClient):
    def __init__(self, remote_images: list[dict] | None = None):
        super().__init__("token", "user-123")
        self.remote_images = [dict(row or {}) for row in (remote_images or [])]
        self.upload_image_calls: list[dict] = []
        self.upload_original_calls: list[dict] = []
        self.push_metadata_calls: list[dict] = []
        self.original_patch_calls: list[dict] = []
        self.fail_original_upload = False

    def _observation_images_support_ai_crop(self) -> bool:
        return False

    def _observation_images_support_ai_crop_custom(self) -> bool:
        return False

    def _observation_images_support_upload_metadata(self) -> bool:
        return False

    def _observation_images_support_original_storage_path(self) -> bool:
        return True

    def pull_image_metadata(self, obs_cloud_id: str, include_deleted_for_sync: bool = False) -> list[dict]:
        return [
            dict(row)
            for row in self.remote_images
            if str(row.get("observation_id") or "").strip() == str(obs_cloud_id or "").strip()
        ]

    def pull_measurements_for_images(self, image_cloud_ids: list[str]) -> list[dict]:
        return []

    def upload_image_file(
        self,
        local_path: str,
        obs_cloud_id: str,
        img_cloud_id: str,
        storage_path: str | None = None,
        upload_meta: dict | None = None,
    ) -> str | None:
        self.upload_image_calls.append(
            {
                "local_path": str(local_path),
                "obs_cloud_id": str(obs_cloud_id),
                "img_cloud_id": str(img_cloud_id),
                "storage_path": str(storage_path or ""),
                "upload_meta": dict(upload_meta or {}),
            }
        )
        return storage_path

    def upload_original_image_file(
        self,
        local_path: str,
        obs_cloud_id: str,
        img_cloud_id: str,
        storage_path: str | None = None,
        upload_meta: dict | None = None,
    ) -> str | None:
        self.upload_original_calls.append(
            {
                "local_path": str(local_path),
                "obs_cloud_id": str(obs_cloud_id),
                "img_cloud_id": str(img_cloud_id),
                "storage_path": str(storage_path or ""),
                "upload_meta": dict(upload_meta or {}),
            }
        )
        if self.fail_original_upload:
            raise cloud_sync.CloudSyncError("Original media upload failed: worker refused")
        return storage_path or self._build_original_storage_path(obs_cloud_id, img_cloud_id, local_path)

    def push_image_metadata(self, img: dict, obs_cloud_id: str, storage_path: str) -> str:
        payload = {col: img.get(col) for col in cloud_sync._IMG_PUSH_COLS}
        calibration_uuid = cloud_sync._image_calibration_uuid(img)
        if calibration_uuid:
            payload["calibration_uuid"] = calibration_uuid
        else:
            payload.pop("calibration_uuid", None)
        payload["observation_id"] = obs_cloud_id
        payload["user_id"] = self.user_id
        payload["desktop_id"] = img["id"]
        payload["original_filename"] = (
            str(img.get("original_filename") or "").strip()
            or Path(img.get("filepath") or "").name
            or None
        )
        payload["storage_path"] = cloud_sync.normalize_media_key(storage_path)

        cloud_id = str(img.get("cloud_id") or "").strip() or f"cloud-image-{len(self.push_metadata_calls) + 1}"
        existing = next((row for row in self.remote_images if str(row.get("id") or "").strip() == cloud_id), None)
        if existing is None:
            existing = {"id": cloud_id}
            self.remote_images.append(existing)
        existing.update(payload)
        existing.setdefault("observation_id", obs_cloud_id)
        existing.setdefault("original_storage_path", None)
        self.push_metadata_calls.append({"payload": dict(payload), "storage_path": storage_path, "cloud_id": cloud_id})
        return cloud_id

    def set_image_original_storage_path(self, cloud_image_id: str, original_storage_path: str) -> None:
        normalized_key = cloud_sync.normalize_media_key(original_storage_path)
        self.original_patch_calls.append(
            {"cloud_image_id": str(cloud_image_id), "original_storage_path": normalized_key}
        )
        row = next((row for row in self.remote_images if str(row.get("id") or "").strip() == str(cloud_image_id).strip()), None)
        if row is None:
            raise AssertionError(f"Remote row {cloud_image_id!r} missing")
        row["original_storage_path"] = normalized_key


def test_original_upload_skips_when_setting_is_disabled_and_keeps_derivative_flow_unchanged(
    monkeypatch,
    tmp_path,
):
    db_path = _create_sync_db(tmp_path)
    source_path = tmp_path / "field.jpg"
    source_path.write_text("working-bytes", encoding="utf-8")
    _seed_image(
        db_path,
        image_id=11,
        observation_id=1,
        filepath=source_path,
        source_role="local_canonical",
        file_purpose="field",
    )
    _patch_db_connections(monkeypatch, db_path)
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: False)

    client = _MemoryOriginalSyncClient()
    result = cloud_sync._push_images_for_observation(client, {"id": 1}, "cloud-obs-1")

    assert result is True
    assert len(client.upload_image_calls) == 1
    assert len(client.push_metadata_calls) == 1
    assert client.upload_original_calls == []
    assert client.original_patch_calls == []
    assert client.remote_images[0]["original_storage_path"] is None


def test_raw_lineage_does_not_upload_originals_by_default(monkeypatch, tmp_path):
    db_path = _create_sync_db(tmp_path)
    working_path = tmp_path / "rendered_from_raw.jpg"
    original_path = tmp_path / "source.nef"
    working_path.write_text("working-bytes", encoding="utf-8")
    original_path.write_text("raw-bytes", encoding="utf-8")
    _seed_image(
        db_path,
        image_id=12,
        observation_id=1,
        filepath=working_path,
        source_role="converted_local",
        file_purpose="microscope",
        original_filepath=original_path,
    )
    _patch_db_connections(monkeypatch, db_path)
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: False)

    client = _MemoryOriginalSyncClient()
    result = cloud_sync._push_images_for_observation(client, {"id": 1}, "cloud-obs-1")

    assert result is True
    assert len(client.upload_image_calls) == 1
    assert client.upload_image_calls[0]["local_path"] == str(working_path)
    assert len(client.push_metadata_calls) == 1
    assert client.upload_original_calls == []
    assert client.original_patch_calls == []
    assert client.remote_images[0]["original_storage_path"] is None


def test_local_canonical_original_upload_updates_cloud_row_and_snapshot(monkeypatch, tmp_path):
    db_path = _create_sync_db(tmp_path)
    source_path = tmp_path / "field.jpg"
    source_path.write_text("working-bytes", encoding="utf-8")
    _seed_image(
        db_path,
        image_id=11,
        observation_id=1,
        filepath=source_path,
        source_role="local_canonical",
        file_purpose="field",
    )
    _patch_db_connections(monkeypatch, db_path)
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: True)

    client = _MemoryOriginalSyncClient()
    summary_warnings: list[str] = []
    result = cloud_sync._push_images_for_observation(
        client,
        {"id": 1},
        "cloud-obs-1",
        summary_warnings=summary_warnings,
    )

    expected_key = client._build_original_storage_path("cloud-obs-1", "cloud-image-1", source_path)
    snapshot_remote = {"id": "cloud-obs-1", "desktop_id": 1, "date": "2026-05-01"}

    cloud_sync._store_remote_snapshot(
        client,
        "cloud-obs-1",
        remote=snapshot_remote,
        remote_images=None,
        remote_measurements=[],
    )
    snapshot = json.loads(cloud_sync._load_cloud_observation_snapshot("cloud-obs-1"))

    assert result is True
    assert summary_warnings == []
    assert len(client.upload_image_calls) == 1
    assert len(client.push_metadata_calls) == 1
    assert len(client.upload_original_calls) == 1
    assert client.upload_original_calls[0]["local_path"] == str(source_path)
    assert client.upload_original_calls[0]["upload_meta"]["source_kind"] == "filepath"
    assert client.original_patch_calls == [
        {
            "cloud_image_id": "cloud-image-1",
            "original_storage_path": expected_key,
        }
    ]
    assert client.remote_images[0]["original_storage_path"] == expected_key
    assert snapshot["images"][0]["original_storage_path"] == expected_key


def test_converted_local_original_upload_prefers_original_filepath(monkeypatch, tmp_path):
    db_path = _create_sync_db(tmp_path)
    working_path = tmp_path / "working.jpg"
    original_path = tmp_path / "source.heic"
    working_path.write_text("working-bytes", encoding="utf-8")
    original_path.write_text("original-bytes", encoding="utf-8")
    _seed_image(
        db_path,
        image_id=11,
        observation_id=1,
        filepath=working_path,
        original_filepath=original_path,
        source_role="converted_local",
        file_purpose="microscope",
    )
    _patch_db_connections(monkeypatch, db_path)
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: True)

    client = _MemoryOriginalSyncClient()
    result = cloud_sync._push_images_for_observation(client, {"id": 1}, "cloud-obs-1")

    assert result is True
    assert len(client.upload_image_calls) == 1
    assert client.upload_image_calls[0]["local_path"] == str(working_path)
    assert len(client.upload_original_calls) == 1
    assert client.upload_original_calls[0]["local_path"] == str(original_path)
    assert client.upload_original_calls[0]["upload_meta"]["source_kind"] == "original_filepath"


@pytest.mark.parametrize(
    ("source_role", "file_purpose"),
    [
        ("cloud_derivative", "field"),
        ("cloud_recovery_cache", "cache"),
        ("generated_artifact", "plot"),
        ("generated_artifact", "thumbnail"),
        ("generated_artifact", "spore_crop"),
        ("local_canonical", "thumbnail"),
    ],
)
def test_ineligible_original_rows_are_skipped(monkeypatch, tmp_path, source_role, file_purpose):
    db_path = _create_sync_db(tmp_path)
    source_path = tmp_path / f"{source_role}_{file_purpose}.jpg"
    source_path.write_text("working-bytes", encoding="utf-8")
    _seed_image(
        db_path,
        image_id=11,
        observation_id=1,
        filepath=source_path,
        source_role=source_role,
        file_purpose=file_purpose,
    )
    _patch_db_connections(monkeypatch, db_path)
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: True)

    client = _MemoryOriginalSyncClient()
    result = cloud_sync._push_images_for_observation(client, {"id": 1}, "cloud-obs-1")

    assert result is True
    assert len(client.upload_image_calls) == 1
    assert client.upload_original_calls == []
    assert client.original_patch_calls == []
    assert client.remote_images[0]["original_storage_path"] is None


def test_original_upload_failure_does_not_write_original_storage_path(monkeypatch, tmp_path):
    db_path = _create_sync_db(tmp_path)
    source_path = tmp_path / "field.jpg"
    source_path.write_text("working-bytes", encoding="utf-8")
    _seed_image(
        db_path,
        image_id=11,
        observation_id=1,
        filepath=source_path,
        source_role="local_canonical",
        file_purpose="field",
    )
    _patch_db_connections(monkeypatch, db_path)
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: True)

    client = _MemoryOriginalSyncClient()
    client.fail_original_upload = True
    summary_warnings: list[str] = []

    result = cloud_sync._push_images_for_observation(
        client,
        {"id": 1},
        "cloud-obs-1",
        summary_warnings=summary_warnings,
    )

    assert result is True
    assert len(client.upload_image_calls) == 1
    assert len(client.push_metadata_calls) == 1
    assert len(client.upload_original_calls) == 1
    assert client.original_patch_calls == []
    assert client.remote_images[0]["original_storage_path"] is None
    assert any("original upload failed" in warning for warning in summary_warnings)


def test_too_large_original_is_skipped(monkeypatch, tmp_path):
    db_path = _create_sync_db(tmp_path)
    source_path = tmp_path / "field.jpg"
    source_path.write_text("working-bytes", encoding="utf-8")
    _seed_image(
        db_path,
        image_id=11,
        observation_id=1,
        filepath=source_path,
        source_role="local_canonical",
        file_purpose="field",
    )
    _patch_db_connections(monkeypatch, db_path)
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: True)
    monkeypatch.setattr(policy, "FULL_RESOLUTION_ORIGINAL_UPLOAD_MAX_BYTES", 1)
    monkeypatch.setattr(cloud_sync, "FULL_RESOLUTION_ORIGINAL_UPLOAD_MAX_BYTES", 1)

    client = _MemoryOriginalSyncClient()
    summary_warnings: list[str] = []
    result = cloud_sync._push_images_for_observation(
        client,
        {"id": 1},
        "cloud-obs-1",
        summary_warnings=summary_warnings,
    )

    assert result is True
    assert len(client.upload_image_calls) == 1
    assert client.upload_original_calls == []
    assert client.original_patch_calls == []
    assert any("too large" in warning for warning in summary_warnings)
