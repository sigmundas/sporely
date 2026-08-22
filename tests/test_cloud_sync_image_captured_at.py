from __future__ import annotations

import os
import time

import pytest

from utils import cloud_sync


def _client(monkeypatch, *, existing_id="cloud-image-1"):
    client = cloud_sync.SporelyCloudClient("token", "user-1")
    monkeypatch.setattr(client, "_resolve_existing_image_for_push", lambda img, obs_cloud_id, **kw: existing_id)
    monkeypatch.setattr(client, "_observation_images_support_ai_crop", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_ai_crop_custom", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_upload_metadata", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_storage_exif_safe", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_sample_source", lambda: False)
    monkeypatch.setattr(client, "_set_observation_media_keys", lambda *args, **kwargs: None)
    return client


def test_image_capture_time_is_in_explicit_sync_contract():
    assert "captured_at" in cloud_sync._IMG_PUSH_COLS
    assert "captured_at" in cloud_sync._SNAPSHOT_IMG_FIELDS
    assert "captured_at" in cloud_sync._OBSERVATION_IMAGE_SELECT_COLUMNS


def test_push_serializes_capture_time_without_touching_created_at(monkeypatch):
    client = _client(monkeypatch, existing_id=None)
    posts = []
    monkeypatch.setattr(
        client,
        "_post",
        lambda path, payload: posts.append(dict(payload)) or [{"id": "cloud-image-new"}],
    )

    client.push_image_metadata(
        {
            "id": 7,
            "image_type": "microscope",
            "captured_at": "2026-07-12T19:45:00+02:00",
        },
        "cloud-observation-1",
        "",
    )

    assert posts[0]["captured_at"] == "2026-07-12T17:45:00+00:00"
    assert "created_at" not in posts[0]


def test_push_omits_null_capture_time_instead_of_falling_back_or_erasing(monkeypatch):
    client = _client(monkeypatch)
    patches = []
    monkeypatch.setattr(client, "_patch", lambda path, payload: patches.append(dict(payload)))

    client.push_image_metadata(
        {"id": 7, "image_type": "microscope", "captured_at": None},
        "cloud-observation-1",
        "",
    )

    assert "captured_at" not in patches[0]
    assert "created_at" not in patches[0]


def test_pull_updates_local_capture_time_and_preserves_remote_null(monkeypatch):
    updates = []
    monkeypatch.setattr(cloud_sync.ImageDB, "update_image", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        cloud_sync,
        "_update_image_columns_without_touching_observation",
        lambda image_id, values: updates.append((image_id, dict(values))),
    )
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: _NoopConnection())

    cloud_sync._apply_remote_image_metadata_only_to_local(
        {"id": 3, "captured_at": None},
        {
            "id": "cloud-3",
            "image_type": "microscope",
            "captured_at": "2026-07-12T17:45:00+00:00",
        },
    )
    expected_local = cloud_sync._cloud_image_captured_at_to_local(
        "2026-07-12T17:45:00+00:00"
    )
    assert (3, {"captured_at": expected_local}) in updates

    updates.clear()
    cloud_sync._apply_remote_image_metadata_only_to_local(
        {"id": 3, "captured_at": "2025-01-02 03:04:05"},
        {"id": "cloud-3", "image_type": "microscope", "captured_at": None},
    )
    assert all("captured_at" not in values for _, values in updates)


def test_capture_timestamp_timezone_round_trip_preserves_instant(monkeypatch):
    if not hasattr(time, "tzset"):
        pytest.skip("timezone test requires time.tzset")
    previous_tz = os.environ.get("TZ")
    monkeypatch.setenv("TZ", "Europe/Oslo")
    time.tzset()
    try:
        cloud_value = cloud_sync._normalize_image_captured_at_for_cloud(
            "2026-07-12 19:45:00", local=True
        )
        assert cloud_value == "2026-07-12T17:45:00+00:00"
        assert cloud_sync._cloud_image_captured_at_to_local(cloud_value) == "2026-07-12 19:45:00"
    finally:
        if previous_tz is None:
            monkeypatch.delenv("TZ", raising=False)
        else:
            monkeypatch.setenv("TZ", previous_tz)
        time.tzset()


def test_snapshot_comparison_includes_capture_time():
    baseline = [{"id": "cloud-1", "desktop_id": 1, "captured_at": "2026-01-01T10:00:00Z"}]
    equal = [{"id": "cloud-1", "desktop_id": 1, "captured_at": "2026-01-01T11:00:00+01:00"}]
    changed = [{"id": "cloud-1", "desktop_id": 1, "captured_at": "2026-01-02T10:00:00Z"}]

    assert not cloud_sync._analyze_image_changes(
        [cloud_sync._remote_image_payload(row) for row in equal],
        [cloud_sync._remote_image_payload(row) for row in baseline],
    )["changed"]
    assert cloud_sync._analyze_image_changes(
        [cloud_sync._remote_image_payload(row) for row in changed],
        [cloud_sync._remote_image_payload(row) for row in baseline],
    )["metadata_changed_keys"] == ["cloud:cloud-1"]


def test_real_capture_time_disagreement_uses_image_conflict_detection():
    report = cloud_sync._analyze_observation_push_conflicts(
        local_obs={},
        local_images=[{
            "id": 1,
            "cloud_id": "cloud-1",
            "captured_at": "2026-01-02 10:00:00",
        }],
        local_measurements_by_cloud_id={},
        remote_obs={},
        remote_images=[{
            "id": "cloud-1",
            "desktop_id": 1,
            "captured_at": "2026-01-03T10:00:00Z",
        }],
        remote_measurements=[],
        baseline_snapshot={
            "observation": {},
            "images": [{
                "id": "cloud-1",
                "desktop_id": 1,
                "captured_at": "2026-01-01T10:00:00Z",
            }],
        },
    )
    assert report.image_conflict_keys == ["cloud:cloud-1"]
    assert "images" in report.categories


def test_old_signature_capture_time_gap_marks_observation_dirty(monkeypatch):
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: _CandidateConnection([(42,)]))
    monkeypatch.setattr(
        cloud_sync,
        "_load_local_cloud_media_signature",
        lambda _obs_id: '{"images":[{"id":7,"image_type":"microscope"}]}',
    )
    monkeypatch.setattr(
        cloud_sync,
        "_local_cloud_image_media_signature",
        lambda _obs_id: (
            '{"images":[{"id":7,"image_type":"microscope",'
            '"captured_at":"2026-07-12T17:45:00+00:00"}]}'
        ),
    )
    marked = []
    monkeypatch.setattr(cloud_sync, "mark_observation_dirty", lambda obs_id: marked.append(obs_id))

    assert cloud_sync._mark_cloud_observations_dirty_for_image_capture_time_changes() == 1
    assert marked == [42]


def test_existing_linked_image_backfill_uses_normal_metadata_patch(monkeypatch):
    local_image = {
        "id": 7,
        "cloud_id": "cloud-7",
        "filepath": "/tmp/existing-microscope.jpg",
        "image_type": "microscope",
        "captured_at": "2026-07-12T19:45:00+02:00",
        "synced_at": "2026-07-13T00:00:00Z",
    }
    remote_image = {
        "id": "cloud-7",
        "desktop_id": 7,
        "storage_path": "user/cloud-observation/cloud-7.jpg",
        "image_type": "microscope",
        "captured_at": None,
    }
    pushed = []

    class Client:
        def _observation_images_support_ai_crop(self):
            return False

        def _observation_images_support_upload_metadata(self):
            return False

        def push_image_metadata(self, image, observation_id, storage_path):
            pushed.append((dict(image), observation_id, storage_path))
            remote_image["captured_at"] = cloud_sync._normalize_image_captured_at_for_cloud(
                image.get("captured_at"), local=True
            )
            return "cloud-7"

    monkeypatch.setattr(
        cloud_sync.ImageDB,
        "get_images_for_observation",
        lambda _obs_id: [dict(local_image)],
    )
    monkeypatch.setattr(cloud_sync, "_stored_local_media_signature_image_stats", lambda _obs_id: {})
    monkeypatch.setattr(cloud_sync, "_local_image_source_bytes_unchanged", lambda *_args: True)

    skipped = cloud_sync._reconcile_metadata_only_linked_images(
        Client(),
        {"id": 42},
        "cloud-observation",
        [remote_image],
    )

    assert skipped == {7}
    assert len(pushed) == 1
    assert remote_image["captured_at"] == "2026-07-12T17:45:00+00:00"


class _NoopConnection:
    def execute(self, *_args, **_kwargs):
        return None

    def commit(self):
        return None

    def close(self):
        return None


class _CandidateConnection:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, sql, *_args, **_kwargs):
        if "PRAGMA table_info(images)" in sql:
            return _Rows([(0, "captured_at")])
        return _Rows(self.rows)

    def close(self):
        return None


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows
