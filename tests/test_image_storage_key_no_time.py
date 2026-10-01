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

    src = inspect.getsource(cloud_sync)
    assert "storage_path = existing_storage_path or _build_worker_storage_path(" in src
    legacy = f"{USER}/{OBS}/0_1717227733123.jpg"
    assert cloud_sync._normalize_cloud_media_key(legacy) == legacy
    assert not hasattr(cloud_sync, "_image_storage_timestamp_ms")
