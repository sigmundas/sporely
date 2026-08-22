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

from database import models, schema
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
        self.storage_remove_calls: list[list[str]] = []
        self.delete_calls: list[str] = []

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

    def upload_image_file(
        self,
        local_path,
        obs_cloud_id,
        img_cloud_id,
        storage_path=None,
        upload_meta=None,
        result_meta=None,
        *,
        observation_id=None,
        image_id=None,
        recovery_authorized=False,
    ):
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

    def _storage_remove(self, storage_paths: list[str]) -> None:
        self.storage_remove_calls.append(list(storage_paths))

    def _delete(self, path: str) -> None:
        self.delete_calls.append(str(path))


def test_explicit_tombstone_restore_inserts_new_cloud_identity(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    patches: list[tuple[str, dict]] = []
    posts: list[tuple[str, dict]] = []
    cleared: list[int] = []

    monkeypatch.setattr(client, "_find_cloud_image", lambda desktop_id, obs_cloud_id, **kw: {"id": "cloud-image-4857", "deleted_at": None})
    monkeypatch.setattr(client, "_patch", lambda path, payload: patches.append((path, dict(payload))))
    monkeypatch.setattr(
        client,
        "_post",
        lambda path, payload: posts.append((path, dict(payload))) or [{"id": "cloud-image-new"}],
    )
    monkeypatch.setattr(client, "_observation_images_support_ai_crop", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_ai_crop_custom", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_upload_metadata", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_storage_exif_safe", lambda: False)
    monkeypatch.setattr(client, "_set_observation_media_keys", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_apply_image_sample_fields_to_push_payload", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        cloud_sync,
        "_explicit_image_restore_source",
        lambda image_id: "cloud-image-4857",
    )
    monkeypatch.setattr(
        cloud_sync,
        "_clear_explicit_image_restore_source",
        lambda image_id: cleared.append(int(image_id)),
    )

    cloud_id = client.push_image_metadata(
        {"id": 4997, "filepath": "/tmp/P8030011.jpg", "image_type": "field"},
        "cloud-observation-704",
        "user-123/cloud-observation-704/P8030011.webp",
    )

    assert cloud_id == "cloud-image-new"
    assert patches == [
        (
            "observation_images?id=eq.cloud-image-4857&user_id=eq.user-123",
            {"desktop_id": None},
        )
    ]
    assert len(posts) == 1
    assert posts[0][1]["desktop_id"] == 4997
    assert posts[0][1]["deleted_at"] is None
    assert cleared == [4997]


def test_explicit_checkbox_change_marks_dirty_without_invalidating_signature(monkeypatch):
    calls: list[tuple[str, int]] = []
    monkeypatch.setattr(
        cloud_sync,
        "_clear_local_cloud_media_signature",
        lambda observation_id: calls.append(("signature", int(observation_id))),
    )
    monkeypatch.setattr(
        cloud_sync,
        "mark_observation_dirty",
        lambda observation_id: calls.append(("dirty", int(observation_id))),
    )

    cloud_sync.mark_observation_media_dirty(704)

    assert calls == [("dirty", 704)]


def test_prepared_upload_omission_preserves_all_existing_remote_images(tmp_path, monkeypatch):
    db_path = _create_sync_db(tmp_path)
    first_path = tmp_path / "first.jpg"
    second_path = tmp_path / "second.jpg"
    first_path.write_bytes(b"first-new-bytes")
    second_path.write_bytes(b"second-existing-bytes")
    _insert_image(
        db_path, id=21, observation_id=500, cloud_id="cloud-image-21",
        filepath=str(first_path), source_role="local_canonical", file_purpose="field",
        image_type="field", sort_order=0,
    )
    _insert_image(
        db_path, id=22, observation_id=500, cloud_id="cloud-image-22",
        filepath=str(second_path), source_role="local_canonical", file_purpose="field",
        image_type="field", sort_order=1,
    )
    _patch_db_connections(monkeypatch, db_path)
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: False)
    monkeypatch.setattr(cloud_sync, "_file_content_signature", lambda path: "changed")
    monkeypatch.setattr(cloud_sync, "_load_cloud_image_file_signature", lambda *args: "old")
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args: None)

    remote_rows = [
        {
            "id": "cloud-image-21", "desktop_id": 21,
            "observation_id": "cloud-obs-500", "storage_path": "cloud/first.webp",
            "image_type": "field", "sort_order": 0,
        },
        {
            "id": "cloud-image-22", "desktop_id": 22,
            "observation_id": "cloud-obs-500", "storage_path": "cloud/second.webp",
            "image_type": "field", "sort_order": 1,
        },
    ]
    client = _MemorySyncClient(remote_rows)

    def prepare_only_first(observation, progress_cb=None):
        image_row = next(
            row for row in cloud_sync.ImageDB.get_images_for_observation(500)
            if int(row["id"]) == 21
        )
        return [{"image_row": image_row, "upload_path": str(first_path)}], None, []

    assert cloud_sync._push_images_for_observation(
        client, {"id": 500}, "cloud-obs-500", prepare_images_cb=prepare_only_first,
    ) is True
    assert len(client.upload_image_calls) == 1
    assert client.storage_remove_calls == []
    assert client.delete_calls == []
    assert _cloud_id(db_path, 21) == "cloud-image-21"
    assert _cloud_id(db_path, 22) == "cloud-image-22"
    assert [row["storage_path"] for row in client.remote_images] == [
        "cloud/first.webp", "cloud/second.webp",
    ]


def test_dirty_scan_ignores_rows_cloud_sync_never_pushes(tmp_path, monkeypatch):
    """The dirty-scan must exactly match the cloud-sync push predicate.

    Rows the sync intentionally skips MUST NOT re-dirty the observation:

    * unchecked gallery images (the checkbox is the source of truth for
      cloud-byte-upload consent — an unchecked image is intentionally
      metadata-only, so its presence alone must not force a rebuild),
    * duplicate-path rows (deduped away at upload time),
    * missing-file rows (sync skips them).

    Positive control (same observation, different image): a checked
    cloud-null image WITH a unique local file MUST still dirty the
    observation, so the test does not just prove inaction — a
    legitimately-pending image is still detected.
    """
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
    # Gallery-unchecked image — under the new checkbox contract this is
    # intentionally metadata-only and MUST NOT count as pending cloud media.
    unchecked_file = tmp_path / "unchecked.jpg"
    unchecked_file.write_bytes(b"unchecked")
    _insert_image(
        db_path, id=2, observation_id=10, cloud_id=None, filepath=str(unchecked_file),
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
        # Stage 1: cloud-storage-desired state lives under the dedicated
        # `sporely_cloud_image_storage_excluded_ids_<obs>` setting.
        # `_cloud_explicit_media_upload_selection` reads this and treats
        # every id NOT in the list as gallery-checked. Image 2 is excluded.
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?)",
            ("sporely_cloud_image_storage_excluded_ids_10", json.dumps([2])),
        )
        conn.commit()
    finally:
        conn.close()

    _patch_db_connections(monkeypatch, db_path)

    # ── Negative case: no genuinely-pending image, obs must stay synced ────
    # These tests exercise the dirty-scan itself; run in explicit media-upload
    # mode so the gate does not turn the call into a no-op.
    cloud_sync._mark_cloud_observations_dirty_for_pending_local_images(
        include_pending_local_media_uploads=True,
    )
    assert _sync_status(db_path, 10) == "synced", (
        "Only images 2 (unchecked), 3 (duplicate path), and 4 (missing "
        "file) are cloud_id-null; none should count as pending cloud "
        "media under the gallery-checkbox contract, so the observation "
        "must stay 'synced'."
    )

    # ── Positive control: add a checked, unique-path, cloud_id-null image → dirty ──
    checked_file = tmp_path / "checked-and-pending.jpg"
    checked_file.write_bytes(b"checked-and-pending")
    _insert_image(
        db_path, id=5, observation_id=10, cloud_id=None, filepath=str(checked_file),
        source_role="local_canonical", file_purpose="field", image_type="field", sort_order=4,
    )
    cloud_sync._mark_cloud_observations_dirty_for_pending_local_images(
        include_pending_local_media_uploads=True,
    )
    assert _sync_status(db_path, 10) == "dirty", (
        "Image 5 is checked (not in the excluded list), has a unique "
        "local file, and no cloud_id — the dirty-scan must detect it as "
        "genuinely pending. Without this control the negative-case "
        "assertion above is meaningless."
    )


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

    # These tests exercise the dirty-scan itself; run in explicit media-upload
    # mode so the gate does not turn the call into a no-op.
    cloud_sync._mark_cloud_observations_dirty_for_pending_local_images(
        include_pending_local_media_uploads=True,
    )

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
    # These tests exercise the dirty-scan itself; run in explicit media-upload
    # mode so the gate does not turn the call into a no-op.
    cloud_sync._mark_cloud_observations_dirty_for_pending_local_images(
        include_pending_local_media_uploads=True,
    )
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


def test_p11_recheck_prepares_and_uploads_only_the_tombstoned_target(
    tmp_path,
    monkeypatch,
):
    """Six checked live siblings must not widen one explicit restore event."""
    db_path = _create_sync_db(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        schema._ensure_image_tombstones_table(conn.cursor())
        conn.executescript(
            """
            CREATE TABLE spore_measurements (
                id INTEGER PRIMARY KEY, image_id INTEGER, notes TEXT
            );
            CREATE TABLE spore_annotations (
                id INTEGER PRIMARY KEY, image_id INTEGER, measurement_id INTEGER
            );
            """
        )
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (704, "cloud-obs-704", "synced", "2026-08-01T00:00:00Z"),
        )
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?)",
            ("artsobs_publish_excluded_image_ids_704", "[]"),
        )
        conn.commit()
    finally:
        conn.close()

    live_ids = [5102, 5103, 4992, 4993, 4994, 4996]
    remote_rows: list[dict] = []
    for index, image_id in enumerate(live_ids):
        image_path = tmp_path / f"live-{image_id}.jpg"
        image_path.write_bytes(f"live-{image_id}".encode())
        cloud_id = f"cloud-{image_id}"
        _insert_image(
            db_path,
            id=image_id,
            observation_id=704,
            cloud_id=cloud_id,
            filepath=str(image_path),
            original_filepath=str(image_path),
            source_role="local_canonical",
            file_purpose="field",
            image_type="field",
            sort_order=index,
            synced_at="2999-01-01T00:00:00Z",
        )
        remote_rows.append(
            {
                "id": cloud_id,
                "desktop_id": image_id,
                "observation_id": "cloud-obs-704",
                "storage_path": f"users/user-123/cloud-obs-704/{image_id}.webp",
                "image_type": "field",
                "sort_order": index,
                "original_filename": image_path.name,
            }
        )

    target_path = tmp_path / "P8030011.jpg"
    original_path = tmp_path / "original-P8030011.jpg"
    target_path.write_bytes(b"target-working-file")
    original_path.write_bytes(b"target-original-file")
    _insert_image(
        db_path,
        id=4990,
        observation_id=704,
        cloud_id="cloud-4857",
        filepath=str(target_path),
        original_filepath=str(original_path),
        source_role="local_canonical",
        file_purpose="field",
        image_type="field",
        sort_order=99,
        synced_at="2026-08-01T00:00:00Z",
    )
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO image_tombstones (deleted_cloud_id, deleted_at, delete_synced_at, "
            "local_observation_id, filepath, original_filepath) VALUES (?, ?, ?, ?, ?, ?)",
            (
                "cloud-4857",
                "2026-08-01T00:00:00Z",
                "2026-08-01T00:01:00Z",
                704,
                str(target_path),
                str(original_path),
            ),
        )
        conn.execute(
            "INSERT INTO spore_measurements (id, image_id, notes) VALUES (?, ?, ?)",
            (9001, 4990, "measurement"),
        )
        conn.execute(
            "INSERT INTO spore_annotations (id, image_id, measurement_id) VALUES (?, ?, ?)",
            (9002, 4990, 9001),
        )
        conn.commit()
    finally:
        conn.close()

    _patch_db_connections(monkeypatch, db_path)
    # Converged baseline excludes the still-linked tombstoned target.
    cloud_sync._store_local_cloud_media_signature(
        704,
        json.dumps(
            {
                "images": [
                    {
                        "id": image_id,
                        "filepath": cloud_sync._path_stat_signature(
                            str(tmp_path / f"live-{image_id}.jpg")
                        ),
                    }
                    for image_id in live_ids
                ]
            },
            sort_keys=True,
        ),
    )
    cloud_sync.remember_explicit_image_restore_source(4990, "cloud-4857")
    assert models.ImageDB.clear_image_cloud_sync_state(4990) is True
    cloud_sync.mark_observation_media_dirty(704)
    # The next reads use fresh SQLite connections, exercising the same state
    # boundary as closing the app after the click and syncing after restart.
    assert cloud_sync._explicit_image_restore_source(4990) == "cloud-4857"
    assert _cloud_id(db_path, 4990) is None

    encoded_ids: list[int] = []

    def prepare_only_requested(observation, progress_cb=None):
        skipped = {
            int(value)
            for value in observation.get(cloud_sync.CLOUD_SYNC_SKIP_PREPARE_IMAGE_IDS_KEY, [])
        }
        items = []
        for image in models.ImageDB.get_images_for_observation(704):
            image_id = int(image["id"])
            if image_id in skipped:
                continue
            encoded_ids.append(image_id)
            items.append({"image_row": image, "upload_path": image["filepath"]})
        return items, None, []

    client = _MemorySyncClient(remote_rows)
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: False)
    assert cloud_sync._push_images_for_observation(
        client,
        {"id": 704},
        "cloud-obs-704",
        prepare_images_cb=prepare_only_requested,
    ) is True

    assert encoded_ids == [4990]
    assert len(client.upload_image_calls) == 1
    assert client.upload_image_calls[0]["local_path"] == str(target_path)
    for image_id, expected_cloud_id in zip(live_ids, [f"cloud-{value}" for value in live_ids]):
        assert _cloud_id(db_path, image_id) == expected_cloud_id
    assert _cloud_id(db_path, 4990) not in {None, "cloud-4857"}
    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute(
            "SELECT deleted_cloud_id FROM image_tombstones WHERE deleted_cloud_id = ?",
            ("cloud-4857",),
        ).fetchone() == ("cloud-4857",)
        assert conn.execute(
            "SELECT id, notes FROM spore_measurements WHERE image_id = ?",
            (4990,),
        ).fetchone() == (9001, "measurement")
        assert conn.execute(
            "SELECT id, measurement_id FROM spore_annotations WHERE image_id = ?",
            (4990,),
        ).fetchone() == (9002, 9001)
    finally:
        conn.close()
    assert target_path.read_bytes() == b"target-working-file"
    assert original_path.read_bytes() == b"target-original-file"

    encoded_ids.clear()
    client.upload_image_calls.clear()
    assert cloud_sync._push_images_for_observation(
        client,
        {"id": 704},
        "cloud-obs-704",
        prepare_images_cb=prepare_only_requested,
    ) is True
    assert encoded_ids == []
    assert client.upload_image_calls == []


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
