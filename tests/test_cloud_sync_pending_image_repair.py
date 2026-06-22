"""Regression tests for the cloud-sync pending-image repair path.

These cover the fix for observations that were re-dirtied forever because local
image rows kept ``cloud_id IS NULL``:

* the dirty-scan must only treat rows that cloud sync would actually push as
  pending (publish-excluded / duplicate-path / missing-file rows must not
  re-dirty an observation forever), and
* a metadata-only association of an existing remote cloud image must persist the
  local ``cloud_id`` (no bytes uploaded, no temporary WebP candidate encoded),
  so a second sync does not re-dirty the same observation.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from database import models
from utils import cloud_sync


def _create_sync_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "sporely.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE observations (
                id INTEGER PRIMARY KEY,
                cloud_id TEXT,
                sync_status TEXT,
                synced_at TEXT,
                date TEXT
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
                notes TEXT,
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


def _insert_image(db_path: Path, **columns) -> None:
    keys = ", ".join(columns.keys())
    placeholders = ", ".join("?" for _ in columns)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            f"INSERT INTO images ({keys}) VALUES ({placeholders})",
            tuple(columns.values()),
        )
        conn.commit()
    finally:
        conn.close()


def _sync_status(db_path: Path, observation_id: int) -> str | None:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT sync_status FROM observations WHERE id = ?", (observation_id,)
        ).fetchone()
    finally:
        conn.close()
    return None if row is None else row[0]


def _cloud_id(db_path: Path, image_id: int) -> str | None:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT cloud_id FROM images WHERE id = ?", (image_id,)
        ).fetchone()
    finally:
        conn.close()
    return None if row is None else row[0]


class _MemorySyncClient(cloud_sync.SporelyCloudClient):
    def __init__(self, remote_images: list[dict] | None = None):
        super().__init__("token", "user-123")
        self.remote_images = [dict(row or {}) for row in (remote_images or [])]
        self.upload_image_calls: list[dict] = []
        self.push_metadata_calls: list[dict] = []

    def _observation_images_support_ai_crop(self) -> bool:
        return False

    def _observation_images_support_ai_crop_custom(self) -> bool:
        return False

    def _observation_images_support_upload_metadata(self) -> bool:
        return False

    def _observation_images_support_original_storage_path(self) -> bool:
        return False

    def pull_image_metadata(self, obs_cloud_id: str, include_deleted_for_sync: bool = False) -> list[dict]:
        return [
            dict(row)
            for row in self.remote_images
            if str(row.get("observation_id") or "").strip() == str(obs_cloud_id or "").strip()
        ]

    def upload_image_file(self, local_path, obs_cloud_id, img_cloud_id, storage_path=None, upload_meta=None):
        self.upload_image_calls.append(
            {"local_path": str(local_path), "storage_path": str(storage_path or "")}
        )
        return storage_path

    def push_image_metadata(self, img: dict, obs_cloud_id: str, storage_path: str) -> str:
        desktop_id = img.get("id")
        # Mirror the real client: upsert by desktop_id, falling back to cloud_id.
        existing = next(
            (
                row
                for row in self.remote_images
                if str(row.get("desktop_id") or "") == str(desktop_id or "")
                and str(desktop_id or "")
            ),
            None,
        )
        if existing is None:
            existing = next(
                (
                    row
                    for row in self.remote_images
                    if str(row.get("id") or "").strip() == str(img.get("cloud_id") or "").strip()
                    and str(img.get("cloud_id") or "").strip()
                ),
                None,
            )
        if existing is None:
            cloud_id = (
                str(img.get("cloud_id") or "").strip()
                or f"cloud-image-{len(self.push_metadata_calls) + 1}"
            )
            existing = {"id": cloud_id}
            self.remote_images.append(existing)
        cloud_id = str(existing.get("id"))
        existing["observation_id"] = obs_cloud_id
        existing["desktop_id"] = desktop_id
        existing["storage_path"] = cloud_sync.normalize_media_key(storage_path)
        self.push_metadata_calls.append({"cloud_id": cloud_id, "storage_path": storage_path})
        return cloud_id


def test_dirty_scan_ignores_rows_cloud_sync_never_pushes(tmp_path, monkeypatch):
    """Publish-excluded / duplicate-path / missing-file NULL rows must not
    re-dirty a synced observation forever."""
    db_path = _create_sync_db(tmp_path)
    shared = tmp_path / "shared.jpg"
    shared.write_bytes(b"shared")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (10, "cloud-obs-10", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.commit()
    finally:
        conn.close()

    # Already synced canonical image (owns the shared.jpg path).
    _insert_image(
        db_path, id=1, observation_id=10, cloud_id="cloud-img-1", filepath=str(shared),
        source_role="local_canonical", file_purpose="field", image_type="field", sort_order=0,
    )
    # Publish-excluded NULL row.
    excluded_file = tmp_path / "excluded.jpg"
    excluded_file.write_bytes(b"excluded")
    _insert_image(
        db_path, id=2, observation_id=10, cloud_id=None, filepath=str(excluded_file),
        source_role="local_canonical", file_purpose="field", image_type="field", sort_order=1,
    )
    # Duplicate-path NULL row (same file as id=1, deduped away by sync).
    _insert_image(
        db_path, id=3, observation_id=10, cloud_id=None, filepath=str(shared),
        source_role="local_canonical", file_purpose="field", image_type="field", sort_order=2,
    )
    # Missing-file NULL row (sync skips it).
    _insert_image(
        db_path, id=4, observation_id=10, cloud_id=None, filepath=str(tmp_path / "gone.jpg"),
        source_role="local_canonical", file_purpose="field", image_type="field", sort_order=3,
    )

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?)",
            ("artsobs_publish_excluded_image_ids_10", json.dumps([2])),
        )
        conn.commit()
    finally:
        conn.close()

    _patch_db_connections(monkeypatch, db_path)

    cloud_sync._mark_cloud_observations_dirty_for_pending_local_images()

    assert _sync_status(db_path, 10) == "synced"


def test_dirty_scan_redirties_genuinely_pending_image(tmp_path, monkeypatch):
    db_path = _create_sync_db(tmp_path)
    pending = tmp_path / "pending.jpg"
    pending.write_bytes(b"pending")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (12, "cloud-obs-12", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.commit()
    finally:
        conn.close()
    _insert_image(
        db_path, id=1, observation_id=12, cloud_id=None, filepath=str(pending),
        source_role="local_canonical", file_purpose="field", image_type="field", sort_order=0,
    )

    _patch_db_connections(monkeypatch, db_path)

    cloud_sync._mark_cloud_observations_dirty_for_pending_local_images()

    assert _sync_status(db_path, 12) == "dirty"


def test_metadata_only_association_persists_local_cloud_id_without_upload(tmp_path, monkeypatch):
    """A previously-synced image whose local cloud_id was lost is re-linked to
    the matching remote image without uploading bytes, and the link persists so
    a second dirty-scan does not re-dirty the observation."""
    db_path = _create_sync_db(tmp_path)
    image_file = tmp_path / "field.jpg"
    image_file.write_bytes(b"field-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (377, "cloud-obs-377", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.commit()
    finally:
        conn.close()
    _insert_image(
        db_path, id=5, observation_id=377, cloud_id=None, filepath=str(image_file),
        source_role="local_canonical", file_purpose="field", image_type="field", sort_order=0,
        synced_at="2026-05-01T00:00:00Z",
    )

    _patch_db_connections(monkeypatch, db_path)
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: False)

    remote_row = {
        "id": "cloud-image-5",
        "desktop_id": 5,
        "observation_id": "cloud-obs-377",
        "storage_path": "users/user-123/cloud-obs-377/field.webp",
        "image_type": "field",
        "sort_order": 0,
    }
    client = _MemorySyncClient([remote_row])

    result = cloud_sync._push_images_for_observation(client, {"id": 377}, "cloud-obs-377")

    assert result is True
    # No image bytes were uploaded — only the local association was repaired.
    assert client.upload_image_calls == []
    # Local row now points at the existing remote cloud image.
    assert _cloud_id(db_path, 5) == "cloud-image-5"

    # A second dirty-scan must not re-dirty the observation.
    cloud_sync._mark_cloud_observations_dirty_for_pending_local_images()
    assert _sync_status(db_path, 377) == "synced"


def test_remote_first_pass_skips_temp_preparation_for_metadata_only_association(tmp_path, monkeypatch):
    """When a remote match exists for an orphaned local row, the upload
    preparation callback must not be asked to encode a temp WebP candidate for
    it (no bytes uploaded), and the local cloud_id is restored."""
    db_path = _create_sync_db(tmp_path)
    image_file = tmp_path / "field.jpg"
    image_file.write_bytes(b"field-bytes")

    _insert_image(
        db_path, id=7, observation_id=389, cloud_id=None, filepath=str(image_file),
        source_role="local_canonical", file_purpose="field", image_type="field", sort_order=0,
        synced_at="2026-05-01T00:00:00Z",
    )

    _patch_db_connections(monkeypatch, db_path)
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: False)

    remote_row = {
        "id": "cloud-image-7",
        "desktop_id": 7,
        "observation_id": "cloud-obs-389",
        "storage_path": "users/user-123/cloud-obs-389/field.webp",
        "image_type": "field",
        "sort_order": 0,
    }
    client = _MemorySyncClient([remote_row])

    prepare_calls: list[dict] = []

    def fake_prepare(observation, progress_cb=None):
        skip_ids = observation.get(cloud_sync.CLOUD_SYNC_SKIP_PREPARE_IMAGE_IDS_KEY)
        prepare_calls.append({"skip_ids": skip_ids})
        # The remote-first pass already associated image 7, so nothing is left
        # to encode/upload.
        return [], None, []

    result = cloud_sync._push_images_for_observation(
        client, {"id": 389}, "cloud-obs-389", prepare_images_cb=fake_prepare
    )

    assert result is True
    assert client.upload_image_calls == []
    assert _cloud_id(db_path, 7) == "cloud-image-7"
    # The prepare callback was told to skip image 7 (no temp candidate encoded).
    assert prepare_calls and prepare_calls[0]["skip_ids"] == [7]


def test_actual_upload_when_no_remote_match_exists(tmp_path, monkeypatch, capsys):
    """With no matching remote image, bytes are uploaded and the explicit
    upload log is emitted (actual_upload=True)."""
    db_path = _create_sync_db(tmp_path)
    image_file = tmp_path / "new.jpg"
    image_file.write_bytes(b"new-bytes")

    _insert_image(
        db_path, id=9, observation_id=433, cloud_id=None, filepath=str(image_file),
        source_role="local_canonical", file_purpose="field", image_type="field", sort_order=0,
    )

    _patch_db_connections(monkeypatch, db_path)
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: False)

    client = _MemorySyncClient([])
    result = cloud_sync._push_images_for_observation(client, {"id": 433}, "cloud-obs-433")
    output = capsys.readouterr().out

    assert result is True
    assert len(client.upload_image_calls) == 1
    assert _cloud_id(db_path, 9)  # got a fresh cloud id
    assert "actual_upload=True" in output
    assert "Uploading cloud image request" in output


def test_metadata_only_path_does_not_log_actual_upload(tmp_path, monkeypatch, capsys):
    db_path = _create_sync_db(tmp_path)
    image_file = tmp_path / "field.jpg"
    image_file.write_bytes(b"field-bytes")

    _insert_image(
        db_path, id=11, observation_id=434, cloud_id=None, filepath=str(image_file),
        source_role="local_canonical", file_purpose="field", image_type="field", sort_order=0,
        synced_at="2026-05-01T00:00:00Z",
    )

    _patch_db_connections(monkeypatch, db_path)
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: False)

    remote_row = {
        "id": "cloud-image-11",
        "desktop_id": 11,
        "observation_id": "cloud-obs-434",
        "storage_path": "users/user-123/cloud-obs-434/field.webp",
        "image_type": "field",
        "sort_order": 0,
    }
    client = _MemorySyncClient([remote_row])

    result = cloud_sync._push_images_for_observation(client, {"id": 434}, "cloud-obs-434")
    output = capsys.readouterr().out

    assert result is True
    assert client.upload_image_calls == []
    assert "actual_upload=True" not in output
    assert "Uploading cloud image request" not in output
