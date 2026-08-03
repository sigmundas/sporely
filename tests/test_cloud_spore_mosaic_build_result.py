"""Tests for `SporeMosaicBuildResult` + cloud-sync skip-status mapping.

Phase 2.D replaces the "None on any failure" return from
`build_spore_mosaic` with a structured `SporeMosaicBuildResult`, and the
sync layer maps the aggregate reason + per-item skips into specific
``MOSAIC_STATUS_*`` codes so operators see the real remediation instead
of a generic MISSING_SOURCE_IMAGES.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from PIL import Image

from utils import cloud_sync
from utils.cloud_spore_mosaic import (
    MOSAIC_BUILD_REASON_ALL_SKIPPED,
    MOSAIC_BUILD_REASON_NO_INPUT,
    MOSAIC_BUILD_REASON_NO_TILES_RENDERED,
    SporeCropSource,
    SporeMosaicBuildResult,
    build_spore_mosaic,
)


# ── SporeMosaicBuildResult contract (builder-level) ────────────────────────


def _write_source(path: Path, size=(200, 200), color=(120, 40, 40)) -> None:
    Image.new("RGB", size, color).save(path, format="PNG")


def _valid_source(path: Path, mid: int) -> SporeCropSource:
    return SporeCropSource(
        measurement_id=mid, image_id=1,
        cloud_measurement_id=str(mid), cloud_image_id="9",
        source_path=path, source_width=200, source_height=200,
        p1_x=95, p1_y=110, p2_x=95, p2_y=90,
        p3_x=90, p3_y=100, p4_x=100, p4_y=100,
        length_um=10.0, width_um=5.0,
    )


def _source_with_missing_calibration(path: Path, mid: int) -> SporeCropSource:
    """No length_um / width_um AND no scale — planner will skip with
    reason ``"missing_calibration"``."""
    return SporeCropSource(
        measurement_id=mid, image_id=1,
        cloud_measurement_id=str(mid), cloud_image_id="9",
        source_path=path, source_width=200, source_height=200,
        p1_x=95, p1_y=110, p2_x=95, p2_y=90,
        p3_x=90, p3_y=100, p4_x=100, p4_y=100,
        length_um=None, width_um=None, scale_um_per_px=None,
    )


def _source_with_invalid_dims(path: Path, mid: int) -> SporeCropSource:
    return SporeCropSource(
        measurement_id=mid, image_id=1,
        cloud_measurement_id=str(mid), cloud_image_id="9",
        source_path=path, source_width=0, source_height=0,
        p1_x=0, p1_y=0, p2_x=1, p2_y=0,
        p3_x=0, p3_y=-1, p4_x=1, p4_y=-1,
        length_um=10.0, width_um=5.0,
    )


def test_build_spore_mosaic_returns_result_with_none_reason_on_success(tmp_path):
    src = tmp_path / "src.png"
    _write_source(src)
    result = build_spore_mosaic([_valid_source(src, mid=1)], tile_size_px=96)
    assert isinstance(result, SporeMosaicBuildResult)
    assert result.reason is None
    assert result.manifest is not None
    assert result.skipped == []


def test_build_spore_mosaic_empty_input_returns_no_input_reason():
    result = build_spore_mosaic([], tile_size_px=96)
    assert result.manifest is None
    assert result.reason == MOSAIC_BUILD_REASON_NO_INPUT
    assert result.skipped == []


def test_build_spore_mosaic_all_missing_calibration_reason_all_skipped(tmp_path):
    src = tmp_path / "src.png"
    _write_source(src)
    result = build_spore_mosaic(
        [_source_with_missing_calibration(src, mid=1)],
        tile_size_px=96,
    )
    assert result.manifest is None
    assert result.reason == MOSAIC_BUILD_REASON_ALL_SKIPPED
    reasons = {reason for _mid, reason in result.skipped}
    assert reasons == {"missing_calibration"}


def test_build_spore_mosaic_all_invalid_dims_reason_all_skipped(tmp_path):
    result = build_spore_mosaic(
        [_source_with_invalid_dims(tmp_path / "no.png", mid=1)],
        tile_size_px=96,
    )
    assert result.manifest is None
    assert result.reason == MOSAIC_BUILD_REASON_ALL_SKIPPED
    reasons = {reason for _mid, reason in result.skipped}
    assert reasons == {"invalid source dims"}


def test_build_spore_mosaic_missing_source_file_reason_no_tiles(tmp_path):
    """Planner succeeds (row is valid) but the render loop's file open
    fails → `"no_tiles_rendered"` with per-item skips recording
    ``"source image missing"``."""
    ghost = tmp_path / "ghost.png"  # deliberately not created
    src = _valid_source(ghost, mid=42)
    result = build_spore_mosaic([src], tile_size_px=96)
    assert result.manifest is None
    assert result.reason == MOSAIC_BUILD_REASON_NO_TILES_RENDERED
    assert result.skipped == [(42, "source image missing")]


# ── cloud_sync skip-status classification (item 5, sync-side) ─────────────


def _classify(skips):
    return cloud_sync._classify_mosaic_build_skips(skips)


def test_classify_all_missing_source_returns_missing_source_images():
    assert _classify([(1, "source image missing"), (2, "source image missing")]) == (
        cloud_sync.MOSAIC_STATUS_SKIP_MISSING_SOURCE_IMAGES
    )


def test_classify_all_missing_calibration_returns_missing_calibration():
    assert _classify([(1, "missing_calibration"), (2, "missing_calibration")]) == (
        cloud_sync.MOSAIC_STATUS_SKIP_MISSING_CALIBRATION
    )


def test_classify_all_invalid_dims_returns_invalid_geometry():
    assert _classify([(1, "invalid source dims"), (2, "invalid source dims")]) == (
        cloud_sync.MOSAIC_STATUS_SKIP_INVALID_GEOMETRY
    )


def test_classify_render_failure_marker_wins_render_failure_bucket():
    assert _classify([(1, "render failed: boom"), (2, "source image missing")]) == (
        cloud_sync.MOSAIC_STATUS_SKIP_RENDER_FAILURE
    )


def test_classify_mixed_reasons_falls_back_to_no_usable_sources():
    """Brief item 5 explicit requirement: mixed skips must NOT collapse
    to the generic MISSING_SOURCE_IMAGES bucket."""
    code = _classify([
        (1, "source image missing"),
        (2, "missing_calibration"),
        (3, "invalid source dims"),
    ])
    assert code != cloud_sync.MOSAIC_STATUS_SKIP_MISSING_SOURCE_IMAGES
    assert code == cloud_sync.MOSAIC_STATUS_SKIP_NO_USABLE_SOURCES


def test_classify_empty_returns_no_usable_sources():
    assert _classify([]) == cloud_sync.MOSAIC_STATUS_SKIP_NO_USABLE_SOURCES


# ── Skip constants documented on cloud_sync ────────────────────────────────


def test_new_skip_constants_defined_and_distinct():
    """Ensure the three new constants exist and don't collide with the
    generic MISSING_SOURCE_IMAGES bucket the brief called out."""
    codes = {
        cloud_sync.MOSAIC_STATUS_SKIP_MISSING_SOURCE_IMAGES,
        cloud_sync.MOSAIC_STATUS_SKIP_MISSING_CALIBRATION,
        cloud_sync.MOSAIC_STATUS_SKIP_INVALID_GEOMETRY,
        cloud_sync.MOSAIC_STATUS_SKIP_RENDER_FAILURE,
        cloud_sync.MOSAIC_STATUS_SKIP_NO_USABLE_SOURCES,
    }
    assert len(codes) == 5
