"""Tests for canonical three-side image comparison in _analyze_observation_push_conflicts.

Verifies that representation differences between the local SQLite row, the stored
cloud baseline snapshot, and the live remote payload do not produce false conflicts,
while genuine field-value changes still trigger conflict detection.

Also covers push_skipped reporting for push-blocked review-needed errors.
"""

from __future__ import annotations

import pytest

from utils import cloud_sync


# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------

_CLOUD_OBS_ID = "cloud-obs-773"
_CLOUD_IMG_ID = "cloud-img-773-1"
_CLOUD_IMG_ID_2 = "cloud-img-773-2"
_CALIBRATION_UUID = "7a872549-0000-0000-0000-000000000001"
_CALIBRATION_UUID_OTHER = "7a872549-0000-0000-0000-000000000099"
_CAPTURED_AT_UTC = "2026-08-18T20:12:01+00:00"


def _baseline_obs() -> dict:
    return {
        "id": _CLOUD_OBS_ID,
        "desktop_id": 773,
        "date": "2026-08-18",
        "genus": "Russula",
        "species": "emetica",
        "notes": "baseline note",
    }


def _local_obs() -> dict:
    return {
        "id": 773,
        "cloud_id": _CLOUD_OBS_ID,
        "sync_status": "dirty",
        "date": "2026-08-18",
        "genus": "Russula",
        "species": "emetica",
        "notes": "baseline note",
    }


def _remote_obs(**overrides) -> dict:
    row = dict(_baseline_obs(), **overrides)
    return row


def _local_image_row(**overrides) -> dict:
    """Raw SQLite image row for the local side."""
    row = {
        "id": 773_001,
        "cloud_id": _CLOUD_IMG_ID,
        "observation_id": 773,
        "filepath": "/tmp/obs773/img1.jpg",
        "image_type": "microscope",
        "sort_order": 0,
        "crop_mode": "full",
        "notes": None,
        "sample_source": "Stipe",  # Desktop Title_Case
        # No calibration_uuid — will be resolved via CalibrationDB
        "calibration_id": 17,
        "captured_at": _CAPTURED_AT_UTC,  # UTC-aware so normalization is timezone-independent
    }
    row.update(overrides)
    return row


def _baseline_image_row(**overrides) -> dict:
    """Raw cloud baseline row as stored in the snapshot (cloud/remote format)."""
    row = {
        "id": _CLOUD_IMG_ID,
        "desktop_id": 773_001,
        "observation_id": _CLOUD_OBS_ID,
        "sort_order": 0,
        "image_type": "microscope",
        "crop_mode": "full",
        "notes": None,
        "sample_source": "stipe",  # Cloud lowercase form
        "calibration_uuid": _CALIBRATION_UUID,
        "captured_at": _CAPTURED_AT_UTC,
        "storage_path": f"user/{_CLOUD_OBS_ID}/{_CLOUD_IMG_ID}.webp",
        "original_filename": "img1.jpg",
    }
    row.update(overrides)
    return row


def _remote_image_row(**overrides) -> dict:
    """Normalized remote image row (same canonical values as baseline after normalization)."""
    row = {
        "id": _CLOUD_IMG_ID,
        "desktop_id": 773_001,
        "observation_id": _CLOUD_OBS_ID,
        "sort_order": 0,
        "image_type": "microscope",
        "crop_mode": "full",
        "notes": None,
        "sample_source": "stipe",  # Will be normalized to 'Stipe' by _remote_image_payload
        "calibration_uuid": _CALIBRATION_UUID,
        "captured_at": _CAPTURED_AT_UTC,
        "storage_path": f"user/{_CLOUD_OBS_ID}/{_CLOUD_IMG_ID}.webp",
        "original_filename": "img1.jpg",
    }
    row.update(overrides)
    return row


def _make_inputs(
    *,
    local_image: dict | None = None,
    baseline_image: dict | None = None,
    remote_image: dict | None = None,
    local_obs_overrides: dict | None = None,
    remote_obs_overrides: dict | None = None,
    baseline_obs_overrides: dict | None = None,
    baseline_images: list[dict] | None = None,
    remote_images: list[dict] | None = None,
) -> dict:
    b_obs = dict(_baseline_obs(), **(baseline_obs_overrides or {}))
    l_obs = dict(_local_obs(), **(local_obs_overrides or {}))
    r_obs = dict(_remote_obs(**(remote_obs_overrides or {})))

    b_img = baseline_image if baseline_image is not None else _baseline_image_row()
    r_img = remote_image if remote_image is not None else _remote_image_row()
    l_img = local_image if local_image is not None else _local_image_row()

    b_imgs = baseline_images if baseline_images is not None else [b_img]
    r_imgs = remote_images if remote_images is not None else [r_img]

    snapshot = {
        "observation": b_obs,
        "images": b_imgs,
        "measurements": [],
    }

    return {
        "local_obs": l_obs,
        "local_images": [l_img],
        "local_measurements_by_cloud_id": {},
        "remote_obs": r_obs,
        "remote_images": r_imgs,
        "remote_measurements": [],
        "baseline_snapshot": snapshot,
    }


def _patch_calibration(monkeypatch, calibration_id: int = 17, uuid: str = _CALIBRATION_UUID):
    """Monkeypatch CalibrationDB so _image_calibration_uuid resolves without a DB."""
    class _FakeCalibrationDB:
        @staticmethod
        def get_calibration(cid):
            if cid == calibration_id:
                return {"id": cid, "calibration_uuid": uuid}
            return None

    monkeypatch.setattr(cloud_sync, "CalibrationDB", _FakeCalibrationDB)


def _patch_tombstones(monkeypatch, tombstoned_ids: set[str] | None = None):
    """Monkeypatch _local_tombstoned_cloud_image_ids to avoid DB calls."""
    ids = tombstoned_ids or set()
    monkeypatch.setattr(cloud_sync, "_local_tombstoned_cloud_image_ids", lambda *_a, **_kw: ids)


# ---------------------------------------------------------------------------
# Test 1: Equivalent representations across sides — no false conflict
# ---------------------------------------------------------------------------


def test_obs773_equivalent_representations_no_conflict(monkeypatch):
    """sample_source case difference and calibration_id vs calibration_uuid must
    not cause a false image conflict after canonical normalization."""
    _patch_calibration(monkeypatch)
    _patch_tombstones(monkeypatch)

    inputs = _make_inputs()
    report = cloud_sync._analyze_observation_push_conflicts(**inputs)

    assert report.has_conflict is False, (
        f"Expected no conflict but got categories={report.categories}, "
        f"image_conflict_keys={report.image_conflict_keys}"
    )
    assert report.image_conflict_keys == []


# ---------------------------------------------------------------------------
# Test 2: Genuine remote sample_source change conflicts
# ---------------------------------------------------------------------------


def test_genuine_remote_sample_source_change_conflicts(monkeypatch):
    """A three-way divergence in sample_source (local changed to Cap, remote changed to
    Hymenium from Stipe baseline) must be detected as an image conflict."""
    _patch_calibration(monkeypatch)
    _patch_tombstones(monkeypatch)

    # Local edits sample_source to 'Cap'; baseline is 'Stipe'; remote changed to 'Hymenium'
    inputs = _make_inputs(
        local_image=_local_image_row(sample_source="Cap"),
        baseline_image=_baseline_image_row(sample_source="stipe"),
        remote_image=_remote_image_row(sample_source="hymenium"),
    )
    report = cloud_sync._analyze_observation_push_conflicts(**inputs)

    assert report.has_conflict is True
    assert "images" in report.categories
    assert report.image_conflict_keys


# ---------------------------------------------------------------------------
# Test 3: Genuine remote captured_at change conflicts
# ---------------------------------------------------------------------------


def test_genuine_remote_captured_at_change_conflicts(monkeypatch):
    """A three-way divergence in captured_at (local edited to +2h, remote edited to +1h
    from baseline) must be detected as an image conflict."""
    _patch_calibration(monkeypatch)
    _patch_tombstones(monkeypatch)

    # Local changed captured_at by +2h; remote changed by +1h; baseline is _CAPTURED_AT_UTC
    inputs = _make_inputs(
        local_image=_local_image_row(captured_at="2026-08-18T22:12:01+00:00"),
        baseline_image=_baseline_image_row(captured_at=_CAPTURED_AT_UTC),
        remote_image=_remote_image_row(captured_at="2026-08-18T21:12:01+00:00"),
    )
    report = cloud_sync._analyze_observation_push_conflicts(**inputs)

    assert report.has_conflict is True
    assert "images" in report.categories
    assert report.image_conflict_keys


# ---------------------------------------------------------------------------
# Test 4: Genuine remote calibration_uuid change conflicts
# ---------------------------------------------------------------------------


def test_genuine_remote_calibration_change_conflicts(monkeypatch):
    """A three-way divergence in calibration_uuid (local changed to uuid-A, remote changed
    to uuid-C, baseline is uuid-B) must be detected as an image conflict."""
    _patch_calibration(monkeypatch)
    _patch_tombstones(monkeypatch)

    _UUID_A = "aaaaaaaa-0000-0000-0000-000000000001"
    _UUID_B = "bbbbbbbb-0000-0000-0000-000000000002"
    _UUID_C = "cccccccc-0000-0000-0000-000000000003"

    # Local explicitly sets calibration_uuid (bypasses DB lookup)
    inputs = _make_inputs(
        local_image=_local_image_row(calibration_uuid=_UUID_A, calibration_id=None),
        baseline_image=_baseline_image_row(calibration_uuid=_UUID_B),
        remote_image=_remote_image_row(calibration_uuid=_UUID_C),
    )
    report = cloud_sync._analyze_observation_push_conflicts(**inputs)

    assert report.has_conflict is True
    assert "images" in report.categories
    assert report.image_conflict_keys


# ---------------------------------------------------------------------------
# Test 5: Tombstoned image + unchanged survivor — no false conflict
# ---------------------------------------------------------------------------


def test_same_run_tombstone_plus_surviving_unchanged_image_no_conflict(monkeypatch):
    """When baseline has 2 images (one tombstoned locally, one survivor) and remote
    has only the survivor, there must be no conflict."""
    _patch_calibration(monkeypatch)
    # Mark the first image as locally tombstoned
    _patch_tombstones(monkeypatch, tombstoned_ids={_CLOUD_IMG_ID})

    tombstoned_baseline = _baseline_image_row()  # id = _CLOUD_IMG_ID
    survivor_baseline = _baseline_image_row(
        id=_CLOUD_IMG_ID_2, storage_path=f"user/{_CLOUD_OBS_ID}/{_CLOUD_IMG_ID_2}.webp"
    )
    survivor_local = _local_image_row(
        id=773_002,
        cloud_id=_CLOUD_IMG_ID_2,
        calibration_id=17,
    )
    survivor_remote = _remote_image_row(
        id=_CLOUD_IMG_ID_2,
        desktop_id=773_002,
        storage_path=f"user/{_CLOUD_OBS_ID}/{_CLOUD_IMG_ID_2}.webp",
    )

    inputs = {
        "local_obs": _local_obs(),
        "local_images": [survivor_local],  # tombstoned image absent locally
        "local_measurements_by_cloud_id": {},
        "remote_obs": _remote_obs(),
        "remote_images": [survivor_remote],  # tombstoned image absent remotely
        "remote_measurements": [],
        "baseline_snapshot": {
            "observation": _baseline_obs(),
            "images": [tombstoned_baseline, survivor_baseline],
            "measurements": [],
        },
    }
    report = cloud_sync._analyze_observation_push_conflicts(**inputs)

    assert report.has_conflict is False, (
        f"Expected no conflict but got categories={report.categories}, "
        f"remote_removed_image_keys={report.remote_removed_image_keys}, "
        f"image_conflict_keys={report.image_conflict_keys}"
    )


# ---------------------------------------------------------------------------
# Test 6: Push-blocked review-needed error sets push_skipped=True
# ---------------------------------------------------------------------------


def test_review_required_push_block_sets_push_skipped():
    """A review-needed error emitted by the push path (with push_blocked marker)
    must result in push_skipped=True in summarize_sync_issues."""
    # Build the same error string that push_all emits
    error_text = cloud_sync._format_review_needed_error(
        773,
        _CLOUD_OBS_ID,
        ["push_blocked", "notes"],
    )

    summary = cloud_sync.summarize_sync_issues([error_text])

    assert summary["conflict_count"] == 1
    conflict = summary["conflicts"][0]
    assert conflict["local_id"] == 773
    assert conflict["push_skipped"] is True, (
        f"Expected push_skipped=True but got push_skipped={conflict['push_skipped']}"
    )


# ---------------------------------------------------------------------------
# Test 7: Real remote observation edit still blocks push
# ---------------------------------------------------------------------------


def test_real_remote_edit_still_blocks_push(monkeypatch):
    """A genuine three-way conflict on obs notes (local edits + remote edits diverge)
    must fire as a conflict and block push."""
    _patch_calibration(monkeypatch)
    _patch_tombstones(monkeypatch)

    inputs = _make_inputs(
        local_obs_overrides={"notes": "local note edit"},
        remote_obs_overrides={"notes": "remote note edit"},
        baseline_obs_overrides={"notes": "baseline note"},
    )
    report = cloud_sync._analyze_observation_push_conflicts(**inputs)

    assert report.has_conflict is True
    assert "observation" in report.categories
    assert any("notes" in label.lower() for label in report.field_labels)
