"""Image storage keys must not embed a time-derived component.

Keys are public media CDN paths; deriving them from created_at/captured_at
leaked photo time of day. New keys use a random suffix; the key is persisted
on the remote row before bytes are sent and reused on every later sync.
"""

import re

from utils import cloud_sync

USER = "user-1"
OBS = "obs-cloud-1"
KEY_RE = re.compile(rf"^{USER}/{OBS}/(\d+)_([0-9a-f]{{32}})\.jpg$")


def _row(**overrides):
    row = {
        "sort_order": 2,
        "created_at": "2026-06-01T07:42:13.123+00:00",
        "captured_at": "2026-06-01T07:40:00+00:00",
        "synced_at": "2026-06-02T10:00:00+00:00",
    }
    row.update(overrides)
    return row


def test_key_has_no_time_derived_component():
    row = _row()
    key = cloud_sync._build_worker_storage_path(USER, OBS, row, "/tmp/photo.JPG")
    match = KEY_RE.match(key)
    assert match, key
    assert match.group(1) == "2"
    for field in ("created_at", "captured_at", "synced_at"):
        ms = str(int(cloud_sync._parse_sync_timestamp(row[field]).timestamp() * 1000))
        assert ms not in key
        assert str(int(int(ms) / 1000)) not in key


def test_key_independent_of_row_timestamps():
    a = cloud_sync._build_worker_storage_path(USER, OBS, _row(), "/tmp/p.jpg")
    b = cloud_sync._build_worker_storage_path(USER, OBS, _row(), "/tmp/p.jpg")
    assert a != b  # random, not derived from (identical) row times
    assert KEY_RE.match(a) and KEY_RE.match(b)


def test_negative_sort_order_clamped():
    key = cloud_sync._build_worker_storage_path(USER, OBS, _row(sort_order=-3), "/tmp/p.jpg")
    assert KEY_RE.match(key).group(1) == "0"


def test_existing_legacy_key_is_reused_not_rebuilt():
    """The sync path keeps a remote row's existing storage_path verbatim
    (``existing_storage_path or _build_worker_storage_path(...)``), so legacy
    timestamp keys are untouched and retries after a reserved row reuse the
    persisted random key instead of minting a new one."""
    import inspect

    # The image push moved to utils/cloud_sync_impl/image_push.py (Stage S6 of
    # the cloud-sync extraction); inspect the function, not the facade module.
    src = inspect.getsource(cloud_sync._push_images_for_observation)
    assert "storage_path = existing_storage_path or _build_worker_storage_path(" in src
    legacy = f"{USER}/{OBS}/0_1717227733123.jpg"
    assert cloud_sync._normalize_cloud_media_key(legacy) == legacy
    assert not hasattr(cloud_sync, "_image_storage_timestamp_ms")


def _seed_linked_image_with_failed_listing(monkeypatch, tmp_path):
    import sqlite3

    from tests.test_cloud_original_sync_upload import (
        _MemoryOriginalSyncClient,
        _create_sync_db,
        _patch_db_connections,
        _seed_image,
    )

    db_path = _create_sync_db(tmp_path)
    source_path = tmp_path / "IMG_20260915_134721.jpg"
    source_path.write_text("working-bytes", encoding="utf-8")
    _seed_image(
        db_path, image_id=11, observation_id=1, filepath=source_path,
        source_role="local_canonical", file_purpose="field",
    )
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE images SET cloud_id = 'cloud-image-11' WHERE id = 11")
    conn.commit()
    conn.close()
    _patch_db_connections(monkeypatch, db_path)
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: False)

    legacy_key = "user-123/cloud-obs-1/0_1777629600000.jpg"

    class _ListingFails(_MemoryOriginalSyncClient):
        def pull_image_metadata(self, obs_cloud_id, include_deleted_for_sync=False):
            raise cloud_sync.CloudSyncError("transient listing failure")

        def _get(self, path):
            rows = [dict(r) for r in self.remote_images if f"id=eq.{r['id']}" in path]
            return rows

    client = _ListingFails(remote_images=[{
        "id": "cloud-image-11", "observation_id": "cloud-obs-1", "desktop_id": 11,
        "image_type": "field", "sort_order": 0, "storage_path": legacy_key,
        "deleted_at": None,
    }])
    return client, legacy_key


def test_failed_listing_keeps_existing_remote_storage_path(monkeypatch, tmp_path):
    """Regression for the mechanism: when the per-observation image listing
    fails (non-auth error -> existing_rows == []), the identity selector misses
    a live linked row. The sync must reuse that row's existing storage_path,
    not overwrite it with a freshly minted key (which would orphan the old
    object)."""
    client, legacy_key = _seed_linked_image_with_failed_listing(monkeypatch, tmp_path)
    cloud_sync._push_images_for_observation(client, {"id": 1}, "cloud-obs-1")
    written = [cloud_sync.normalize_media_key(c["payload"]["storage_path"]) for c in client.push_metadata_calls]
    uploaded = [c["storage_path"] for c in client.upload_image_calls]
    assert written and set(written) == {legacy_key}, written
    assert set(uploaded) <= {legacy_key}, uploaded


def test_fallback_storage_path_drops_filename_and_is_stable():
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    a = client._build_storage_path("obs-1", "img-1", "/x/IMG_20260915_134721.JPG")
    b = client._build_storage_path("obs-1", "img-1", "/y/IMG_20260916_080000.jpg")
    assert a == b  # stable per row across retries, independent of filename
    assert re.fullmatch(r"user-123/obs-1/img-1_[0-9a-f]{32}\.jpg", a), a
    assert "2026" not in a and "134721" not in a
    assert client._build_storage_path("obs-1", "img-2", "/x/a.jpg") != a


def test_recovery_key_drops_filename_and_is_deterministic():
    from utils.cloud_media_recovery import recovery_storage_key

    a = recovery_storage_key("user-1", "obs-1", 11, "/x/IMG_20260915_134721.jpg")
    b = recovery_storage_key("user-1", "obs-1", 11, "/x/IMG_20260915_134721.jpg")
    assert a == b
    assert re.fullmatch(r"user-1/obs-1/recovery/11_[0-9a-f]{32}\.jpg", a), a
    assert "134721" not in a and "20260915" not in a
