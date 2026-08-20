"""Regression tests for the metadata-anchor → byte-backed promotion.

A local microscope image can be linked (valid ``cloud_id``) to an owned
remote ``observation_images`` row whose ``storage_path`` IS NULL — a
metadata-only anchor. When the user wants the bytes in cloud storage
(``cloud_image_bytes_desired`` is True) and the anchor is not intentionally
protected, sync must PROMOTE that existing cloud identity to byte-backed:

* keep the existing cloud image id — never POST a second row;
* reserve the intended Worker key on the existing row (owner-scoped,
  conditional on ``storage_path IS NULL``) before any bytes are sent, so
  the Worker's storage_path check passes;
* on upload failure, remove partial objects and restore ``storage_path``
  to NULL only when it still equals the key this attempt reserved;
* survive interruption at any point: a reserved-but-unconfirmed key must
  not be trusted as proof that bytes exist.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from database import models
from utils import cloud_sync


USER_ID = "user-123"


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
                objective_name TEXT,
                sort_order INTEGER,
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                synced_at TEXT
            );
            CREATE TABLE settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE image_tombstones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                deleted_cloud_id TEXT NOT NULL,
                deleted_at TEXT NOT NULL,
                delete_synced_at TEXT,
                deleted_storage_path TEXT,
                deleted_observation_cloud_id TEXT,
                local_observation_id INTEGER,
                local_image_id INTEGER,
                image_type TEXT,
                filepath TEXT,
                original_filepath TEXT
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


def _set_setting(db_path: Path, key: str, value: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        conn.commit()
    finally:
        conn.close()


def _get_setting(db_path: Path, key: str) -> str | None:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = ?", (key,)
        ).fetchone()
    finally:
        conn.close()
    return None if row is None else row[0]


class _PromotionSyncClient(cloud_sync.SporelyCloudClient):
    """In-memory client whose ``upload_image_file`` enforces the Worker's
    storage_path check: the named row must exist and must already carry
    exactly the key being uploaded (mirrors ``verifyImageRowMatchesUpload``).
    """

    def __init__(self, remote_images: list[dict] | None = None, *, fail_uploads: int = 0):
        super().__init__("token", USER_ID)
        self.remote_images = [dict(row or {}) for row in (remote_images or [])]
        self.upload_calls: list[dict] = []
        self.reserve_calls: list[tuple[str, str]] = []
        self.release_calls: list[tuple[str, str]] = []
        self.metadata_calls: list[dict] = []
        self.created_cloud_ids: list[str] = []
        self.storage_remove_calls: list[list[str]] = []
        self.delete_calls: list[str] = []
        self.fail_uploads = int(fail_uploads)
        self.storage_objects: set[str] = set()

    # Capability probes — keep payloads minimal.
    def _observation_images_support_ai_crop(self) -> bool:
        return False

    def _observation_images_support_ai_crop_custom(self) -> bool:
        return False

    def _observation_images_support_upload_metadata(self) -> bool:
        return False

    def _observation_images_support_original_storage_path(self) -> bool:
        return False

    def _row_by_id(self, cloud_id) -> dict | None:
        return next(
            (
                row
                for row in self.remote_images
                if str(row.get("id") or "") == str(cloud_id or "")
            ),
            None,
        )

    def pull_image_metadata(self, obs_cloud_id: str, include_deleted_for_sync: bool = False) -> list[dict]:
        rows = [
            dict(row)
            for row in self.remote_images
            if str(row.get("observation_id") or "").strip() == str(obs_cloud_id or "").strip()
        ]
        if include_deleted_for_sync:
            return rows
        return [row for row in rows if not str(row.get("deleted_at") or "").strip()]

    def reserve_image_storage_path_for_promotion(self, cloud_image_id: str, storage_path: str) -> bool:
        key = cloud_sync.normalize_media_key(storage_path)
        self.reserve_calls.append((str(cloud_image_id), key))
        row = self._row_by_id(cloud_image_id)
        if row is None or cloud_sync.normalize_media_key(row.get("storage_path")):
            return False
        row["storage_path"] = key
        return True

    def release_image_storage_path_reservation(self, cloud_image_id: str, reserved_key: str) -> bool:
        key = cloud_sync.normalize_media_key(reserved_key)
        self.release_calls.append((str(cloud_image_id), key))
        row = self._row_by_id(cloud_image_id)
        if row is None or cloud_sync.normalize_media_key(row.get("storage_path")) != key:
            return False
        row["storage_path"] = None
        return True

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
        key = cloud_sync.normalize_media_key(storage_path)
        row = self._row_by_id(img_cloud_id)
        # Worker parity: reject uploads whose key is not already bound to
        # the named owned row.
        if row is None:
            raise cloud_sync.CloudSyncError("image_not_found_or_not_owner")
        if cloud_sync.normalize_media_key(row.get("storage_path")) != key:
            raise cloud_sync.CloudSyncError("storage_path_mismatch")
        self.upload_calls.append({"cloud_id": str(img_cloud_id), "storage_path": key})
        if self.fail_uploads > 0:
            self.fail_uploads -= 1
            raise cloud_sync.CloudSyncError("simulated upload failure")
        self.storage_objects.add(key)
        return key

    def push_image_metadata(self, img: dict, obs_cloud_id: str, storage_path: str) -> str:
        desktop_id = img.get("id")
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
            cloud_id = f"cloud-image-new-{len(self.created_cloud_ids) + 1}"
            existing = {"id": cloud_id}
            self.remote_images.append(existing)
            self.created_cloud_ids.append(cloud_id)
        cloud_id = str(existing.get("id"))
        existing["observation_id"] = obs_cloud_id
        existing["desktop_id"] = desktop_id
        normalized_key = cloud_sync.normalize_media_key(storage_path)
        if normalized_key:
            existing["storage_path"] = normalized_key
        self.metadata_calls.append({"cloud_id": cloud_id, "storage_path": storage_path})
        return cloud_id

    def _storage_remove(self, storage_paths: list[str]) -> None:
        cleaned = [cloud_sync.normalize_media_key(p) for p in (storage_paths or [])]
        self.storage_remove_calls.append(cleaned)
        for key in cleaned:
            self.storage_objects.discard(key)

    def _delete(self, path: str) -> None:
        self.delete_calls.append(str(path))


def _setup_bare_anchor_case(tmp_path, monkeypatch, *, fail_uploads: int = 0):
    """One observation with a single linked microscope image whose remote
    row is a bare metadata anchor (storage_path NULL)."""
    db_path = _create_sync_db(tmp_path)
    image_path = tmp_path / "micro.jpg"
    image_path.write_bytes(b"microscope-bytes")
    _insert_image(
        db_path, id=1761, observation_id=465, cloud_id="3044",
        filepath=str(image_path), source_role="converted_local",
        file_purpose="microscope", image_type="microscope", sort_order=3,
        synced_at="2026-08-10T21:12:31+00:00",
        created_at="2026-07-01T10:57:54+00:00",
    )
    _patch_db_connections(monkeypatch, db_path)
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: False)

    remote_rows = [
        {
            "id": "3044", "desktop_id": 1761, "observation_id": "762",
            "storage_path": None, "original_storage_path": None,
            "image_type": "microscope", "sort_order": 3, "deleted_at": None,
        },
    ]
    client = _PromotionSyncClient(remote_rows, fail_uploads=fail_uploads)

    def prepare_anchor_image(observation, progress_cb=None):
        skip_ids = set(observation.get(cloud_sync.CLOUD_SYNC_SKIP_PREPARE_IMAGE_IDS_KEY) or [])
        items = [
            {"image_row": row, "upload_path": str(image_path)}
            for row in cloud_sync.ImageDB.get_images_for_observation(465)
            if int(row["id"]) not in skip_ids
        ]
        return items, None, []

    obs = {"id": 465, "spore_data_visibility": "private"}
    return db_path, image_path, client, prepare_anchor_image, obs


def test_bare_anchor_promotes_existing_row_without_new_identity(tmp_path, monkeypatch):
    db_path, _image_path, client, prepare_cb, obs = _setup_bare_anchor_case(tmp_path, monkeypatch)

    assert cloud_sync._push_images_for_observation(
        client, obs, "762", prepare_images_cb=prepare_cb,
    ) is True

    # The reservation ran against the existing row, conditional path first.
    assert client.reserve_calls and client.reserve_calls[0][0] == "3044"
    reserved_key = client.reserve_calls[0][1]
    assert reserved_key.startswith(f"{USER_ID}/762/")
    # Bytes were uploaded with the same cloud id and the reserved key.
    assert client.upload_calls == [{"cloud_id": "3044", "storage_path": reserved_key}]
    assert reserved_key in client.storage_objects
    # No new observation_images identity was created.
    assert client.created_cloud_ids == []
    assert client.delete_calls == []
    assert len(client.remote_images) == 1
    # The row kept its id and gained the key.
    row = client.remote_images[0]
    assert str(row["id"]) == "3044"
    assert cloud_sync.normalize_media_key(row["storage_path"]) == reserved_key
    # The pending marker is cleared after the confirmed upload.
    assert not cloud_sync._load_pending_image_promotion_key(465, 1761)
    # Local link retained.
    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT cloud_id FROM images WHERE id=1761").fetchone()[0] == "3044"
    finally:
        conn.close()


def test_promotion_failure_rolls_back_reservation_and_cleans_partials(tmp_path, monkeypatch):
    db_path, _image_path, client, prepare_cb, obs = _setup_bare_anchor_case(
        tmp_path, monkeypatch, fail_uploads=1,
    )

    assert cloud_sync._push_images_for_observation(
        client, obs, "762", prepare_images_cb=prepare_cb,
    ) is False

    reserved_key = client.reserve_calls[0][1]
    # Partial derivative + thumb cleanup ran for the reserved key.
    assert client.storage_remove_calls == [[
        reserved_key,
        cloud_sync.media_variant_key(reserved_key, "thumb"),
    ]]
    # The reservation was released back to NULL on the same row…
    assert client.release_calls == [("3044", reserved_key)]
    assert client.remote_images[0]["storage_path"] is None
    # …and the anchor row itself was never deleted, no new identity created.
    assert client.delete_calls == []
    assert client.created_cloud_ids == []
    assert len(client.remote_images) == 1
    # Marker is gone: the next sync sees a clean bare anchor again.
    assert not cloud_sync._load_pending_image_promotion_key(465, 1761)


def test_retry_after_failed_promotion_converges(tmp_path, monkeypatch):
    db_path, _image_path, client, prepare_cb, obs = _setup_bare_anchor_case(
        tmp_path, monkeypatch, fail_uploads=1,
    )

    assert cloud_sync._push_images_for_observation(
        client, obs, "762", prepare_images_cb=prepare_cb,
    ) is False
    assert cloud_sync._push_images_for_observation(
        client, obs, "762", prepare_images_cb=prepare_cb,
    ) is True

    # Two attempts, one surviving object, still exactly one remote row with
    # the original identity.
    assert client.created_cloud_ids == []
    assert len(client.remote_images) == 1
    assert str(client.remote_images[0]["id"]) == "3044"
    final_key = cloud_sync.normalize_media_key(client.remote_images[0]["storage_path"])
    assert final_key and final_key in client.storage_objects
    assert not cloud_sync._load_pending_image_promotion_key(465, 1761)


def test_interrupted_promotion_does_not_trust_reserved_key(tmp_path, monkeypatch):
    """Crash after the reservation PATCH but before upload: the remote row
    carries a key with no bytes behind it. The pending marker must force the
    byte upload instead of skipping on the non-NULL storage_path."""
    db_path, image_path, client, prepare_cb, obs = _setup_bare_anchor_case(tmp_path, monkeypatch)

    stale_key = f"{USER_ID}/762/3_1782903791000.webp"
    client.remote_images[0]["storage_path"] = stale_key
    cloud_sync._store_pending_image_promotion_key(465, 1761, stale_key)
    # Simulate the strongest skip signal: the stored file signature matches
    # the current bytes exactly.
    current_sig = cloud_sync._file_content_signature(str(image_path))
    cloud_sync._store_cloud_image_file_signature(465, 1761, current_sig)

    assert cloud_sync._push_images_for_observation(
        client, obs, "762", prepare_images_cb=prepare_cb,
    ) is True

    # Bytes were uploaded to the already-reserved key on the same row; no
    # second reservation, no new identity.
    assert client.upload_calls == [{"cloud_id": "3044", "storage_path": stale_key}]
    assert stale_key in client.storage_objects
    assert client.reserve_calls == []
    assert client.created_cloud_ids == []
    assert not cloud_sync._load_pending_image_promotion_key(465, 1761)


def test_interrupted_promotion_failure_rolls_back_to_clean_anchor(tmp_path, monkeypatch):
    """Resume of an interrupted promotion that fails again must still roll
    the reservation back to NULL (adopted reservation, same guarantees)."""
    db_path, image_path, client, prepare_cb, obs = _setup_bare_anchor_case(
        tmp_path, monkeypatch, fail_uploads=1,
    )
    stale_key = f"{USER_ID}/762/3_1782903791000.webp"
    client.remote_images[0]["storage_path"] = stale_key
    cloud_sync._store_pending_image_promotion_key(465, 1761, stale_key)

    assert cloud_sync._push_images_for_observation(
        client, obs, "762", prepare_images_cb=prepare_cb,
    ) is False

    assert client.release_calls == [("3044", stale_key)]
    assert client.remote_images[0]["storage_path"] is None
    assert client.delete_calls == []
    assert not cloud_sync._load_pending_image_promotion_key(465, 1761)


def test_unchecked_image_with_null_storage_path_stays_metadata_only(tmp_path, monkeypatch):
    db_path, _image_path, client, prepare_cb, obs = _setup_bare_anchor_case(tmp_path, monkeypatch)
    # The user unchecked this image for cloud byte storage. The explicit
    # decision is recorded in the per-image intent ledger so the initializer
    # never reseeds it.
    _set_setting(db_path, cloud_sync._cloud_image_storage_excluded_ids_key(465), json.dumps([1761]))
    _set_setting(db_path, cloud_sync._cloud_image_storage_intent_ledger_key(465), json.dumps([1761]))

    assert cloud_sync._push_images_for_observation(
        client, obs, "762", prepare_images_cb=prepare_cb,
    ) is True

    assert client.reserve_calls == []
    assert client.upload_calls == []
    assert client.storage_objects == set()
    assert client.created_cloud_ids == []
    assert client.remote_images[0]["storage_path"] is None
    assert not cloud_sync._load_pending_image_promotion_key(465, 1761)


def test_protected_metadata_only_anchor_is_never_promoted(tmp_path, monkeypatch):
    db_path, _image_path, client, prepare_cb, obs = _setup_bare_anchor_case(tmp_path, monkeypatch)
    monkeypatch.setattr(
        cloud_sync,
        "_ensure_metadata_anchors_for_public_spore_observation",
        lambda *args, **kwargs: {"metadata_only_cloud_ids": ["3044"]},
    )

    assert cloud_sync._push_images_for_observation(
        client, obs, "762", prepare_images_cb=prepare_cb,
    ) is True

    assert client.reserve_calls == []
    assert client.upload_calls == []
    assert client.remote_images[0]["storage_path"] is None
    assert not cloud_sync._load_pending_image_promotion_key(465, 1761)


def test_byte_backed_unchanged_row_stays_on_fast_path(tmp_path, monkeypatch):
    db_path, image_path, client, prepare_cb, obs = _setup_bare_anchor_case(tmp_path, monkeypatch)
    existing_key = f"{USER_ID}/762/3_1782903791000.webp"
    client.remote_images[0]["storage_path"] = existing_key
    client.storage_objects.add(existing_key)
    current_sig = cloud_sync._file_content_signature(str(image_path))
    cloud_sync._store_cloud_image_file_signature(465, 1761, current_sig)

    assert cloud_sync._push_images_for_observation(
        client, obs, "762", prepare_images_cb=prepare_cb,
    ) is True

    assert client.reserve_calls == []
    assert client.upload_calls == []
    assert cloud_sync.normalize_media_key(client.remote_images[0]["storage_path"]) == existing_key


def test_unchanged_source_bytes_do_not_block_promotion(tmp_path, monkeypatch):
    """`source bytes unchanged since last sync` is a fast-path reason for
    byte-backed rows, but a bare anchor with bytes desired must still
    promote even when the local file has not changed."""
    db_path, image_path, client, prepare_cb, obs = _setup_bare_anchor_case(tmp_path, monkeypatch)
    current_sig = cloud_sync._file_content_signature(str(image_path))
    cloud_sync._store_cloud_image_file_signature(465, 1761, current_sig)
    # A stored local-media signature that says the file is unchanged.
    monkeypatch.setattr(
        cloud_sync,
        "_local_image_source_bytes_unchanged",
        lambda stored_stats, image_row: True,
    )

    assert cloud_sync._push_images_for_observation(
        client, obs, "762", prepare_images_cb=prepare_cb,
    ) is True

    assert client.reserve_calls and client.reserve_calls[0][0] == "3044"
    assert len(client.upload_calls) == 1
    assert client.created_cloud_ids == []
    assert cloud_sync.normalize_media_key(client.remote_images[0]["storage_path"])


def test_reservation_conflict_fails_retryable_without_touching_row(tmp_path, monkeypatch):
    """When the conditional reservation matches no row (concurrent writer),
    the promotion fails retryable and never overwrites the other key."""
    db_path, _image_path, client, prepare_cb, obs = _setup_bare_anchor_case(tmp_path, monkeypatch)
    concurrent_key = f"{USER_ID}/762/other-writer.webp"

    original_reserve = client.reserve_image_storage_path_for_promotion

    def racing_reserve(cloud_image_id, storage_path):
        # Another writer claims the row between the metadata pull and our PATCH.
        client._row_by_id(cloud_image_id)["storage_path"] = concurrent_key
        return original_reserve(cloud_image_id, storage_path)

    monkeypatch.setattr(client, "reserve_image_storage_path_for_promotion", racing_reserve)

    assert cloud_sync._push_images_for_observation(
        client, obs, "762", prepare_images_cb=prepare_cb,
    ) is False

    assert client.upload_calls == []
    assert client.release_calls == []
    assert client.remote_images[0]["storage_path"] == concurrent_key
    assert not cloud_sync._load_pending_image_promotion_key(465, 1761)


def test_pending_marker_blocks_metadata_only_fast_path(tmp_path, monkeypatch):
    db_path = _create_sync_db(tmp_path)
    image_path = tmp_path / "micro.jpg"
    image_path.write_bytes(b"microscope-bytes")
    _insert_image(
        db_path, id=1761, observation_id=465, cloud_id="3044",
        filepath=str(image_path), source_role="converted_local",
        file_purpose="microscope", image_type="microscope", sort_order=3,
        synced_at="2026-08-10T21:12:31+00:00",
    )
    _patch_db_connections(monkeypatch, db_path)
    stale_key = f"{USER_ID}/762/3_1782903791000.webp"
    cloud_sync._store_pending_image_promotion_key(465, 1761, stale_key)
    monkeypatch.setattr(
        cloud_sync,
        "_local_image_source_bytes_unchanged",
        lambda stored_stats, image_row: True,
    )

    remote_rows = [
        {
            "id": "3044", "desktop_id": 1761, "observation_id": "762",
            "storage_path": stale_key, "image_type": "microscope",
            "sort_order": 3, "deleted_at": None,
        },
    ]
    client = _PromotionSyncClient(remote_rows)

    skip_ids = cloud_sync._reconcile_metadata_only_linked_images(
        client, {"id": 465}, "762", remote_rows,
    )

    # The unconfirmed promotion must not be swallowed by the fast path.
    assert 1761 not in skip_ids


def test_pull_only_client_blocks_promotion_writers():
    client = _PromotionSyncClient([])
    wrapper = cloud_sync.PullOnlyCloudClient(client)
    with pytest.raises(cloud_sync.PullOnlyModeError):
        wrapper.reserve_image_storage_path_for_promotion("3044", "user-123/762/x.webp")
    with pytest.raises(cloud_sync.PullOnlyModeError):
        wrapper.release_image_storage_path_reservation("3044", "user-123/762/x.webp")
    assert wrapper.write_attempts == [
        "reserve_image_storage_path_for_promotion",
        "release_image_storage_path_reservation",
    ]


def test_reserve_and_release_are_conditional_owner_scoped_patches(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", USER_ID)
    requests: list[tuple[str, str, dict]] = []

    class _Resp:
        ok = True

        def __init__(self, rows):
            self._rows = rows

        def json(self):
            return self._rows

    def fake_request(method, url, *, refresh_on_auth_error=True, **kwargs):
        requests.append((method, url, kwargs.get("json") or {}))
        return _Resp([{"id": "3044"}])

    monkeypatch.setattr(client, "_request_with_refresh", fake_request)

    key = f"{USER_ID}/762/3_1782903791000.webp"
    assert client.reserve_image_storage_path_for_promotion("3044", key) is True
    assert client.release_image_storage_path_reservation("3044", key) is True

    reserve_method, reserve_url, reserve_payload = requests[0]
    assert reserve_method == "PATCH"
    assert "id=eq.3044" in reserve_url
    assert f"user_id=eq.{USER_ID}" in reserve_url
    assert "storage_path=is.null" in reserve_url
    assert reserve_payload == {"storage_path": key}

    release_method, release_url, release_payload = requests[1]
    assert release_method == "PATCH"
    assert "id=eq.3044" in release_url
    assert f"user_id=eq.{USER_ID}" in release_url
    # Conditional on the exact reserved key (percent-encoded).
    assert f"storage_path=eq.{cloud_sync._encode_postgrest_filter_value(key)}" in release_url
    assert release_payload == {"storage_path": None}


def test_reserve_returns_false_when_no_row_matches(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", USER_ID)

    class _Resp:
        ok = True

        @staticmethod
        def json():
            return []

    monkeypatch.setattr(
        client, "_request_with_refresh", lambda *a, **k: _Resp()
    )
    assert client.reserve_image_storage_path_for_promotion(
        "3044", f"{USER_ID}/762/x.webp"
    ) is False
    assert client.release_image_storage_path_reservation(
        "3044", f"{USER_ID}/762/x.webp"
    ) is False


def test_worker_still_rejects_mismatched_keys(tmp_path, monkeypatch):
    """Worker-parity guard in the stub: an upload whose key is not bound to
    the row fails — the promotion path must therefore always reserve first."""
    client = _PromotionSyncClient([
        {"id": "3044", "desktop_id": 1761, "observation_id": "762",
         "storage_path": None, "image_type": "microscope", "deleted_at": None},
    ])
    with pytest.raises(cloud_sync.CloudSyncError, match="storage_path_mismatch"):
        client.upload_image_file(
            str(tmp_path / "missing.jpg"), "762", "3044",
            storage_path=f"{USER_ID}/762/unreserved.webp",
            recovery_authorized=True,
        )
