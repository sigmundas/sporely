"""Tests for `utils.spore_thumbnail_render` — the PIL port of the desktop
`create_spore_thumbnail` pipeline.

These focus on the properties the mosaic + landing site rely on:

* orient rotation brings the length axis vertical (pointing up),
* p1..p4 are transformed by the same rotation as the pixels,
* padding matches the desktop constants,
* tile height equals the requested `height_px` (desktop parity),
* polygon coordinates land inside the tile bounds and reflect the
  measured rectangle after the same crop + scale transform.
"""

from __future__ import annotations

import math

import pytest
from PIL import Image

from utils.spore_thumbnail_render import (
    DESKTOP_PADDING_X,
    DESKTOP_PADDING_Y,
    SporeThumbnailInputs,
    plan_spore_thumbnail,
    render_spore_thumbnail,
    render_spore_thumbnail_common_crop,
    rotate_point_qt,
)


def _solid(size: tuple[int, int] = (800, 800), color=(80, 80, 80)) -> Image.Image:
    return Image.new("RGB", size, color)


# ── rotate_point_qt ─────────────────────────────────────────────────────────


def test_rotate_point_qt_zero_angle_identity():
    assert rotate_point_qt(3, 4, 0, 0, 0.0) == (3, 4)


def test_rotate_point_qt_matches_qt_math_matrix():
    # QTransform.rotate(-90) applied to (1, 0) → (0, -1) in Qt Y-down.
    x, y = rotate_point_qt(1, 0, 0, 0, -90.0)
    assert x == pytest.approx(0.0, abs=1e-9)
    assert y == pytest.approx(-1.0, abs=1e-9)


def test_rotate_point_qt_orient_swings_horizontal_axis_up():
    # (200, 100) → (300, 100) is a horizontal length axis. Orient rotation
    # of -90° around midpoint (250, 100) should swing it to vertical (up).
    p1 = rotate_point_qt(200, 100, 250, 100, -90.0)
    p2 = rotate_point_qt(300, 100, 250, 100, -90.0)
    dx = p2[0] - p1[0]
    dy = p2[1] - p1[1]
    assert abs(dx) < 1e-6
    assert dy < 0  # UP in Y-down


# ── render_spore_thumbnail — orientation + polygon ──────────────────────────


def test_render_orient_length_axis_becomes_vertical():
    """A horizontally-drawn measurement gets rotated so length is vertical."""
    src = _solid()
    inputs = SporeThumbnailInputs(
        p1_x=300, p1_y=400, p2_x=500, p2_y=400,   # length: horizontal 200 px
        p3_x=400, p3_y=380, p4_x=400, p4_y=420,   # width: vertical 40 px
        orient=True,
    )
    result = render_spore_thumbnail(src, inputs, height_px=256)
    assert result.polygon_tile_local is not None
    xs = [p[0] for p in result.polygon_tile_local]
    ys = [p[1] for p in result.polygon_tile_local]
    span_x = max(xs) - min(xs)
    span_y = max(ys) - min(ys)
    assert span_y > span_x, (
        "length axis must be vertical after orient "
        f"(got span_x={span_x:.1f}, span_y={span_y:.1f})"
    )
    # -90° swings a right-pointing axis to up.
    assert result.rotation_deg == pytest.approx(-90.0, abs=1e-6)


def test_render_orient_zero_when_axis_already_up():
    src = _solid()
    inputs = SporeThumbnailInputs(
        p1_x=400, p1_y=500, p2_x=400, p2_y=300,   # length axis points UP (Δy<0)
        p3_x=390, p3_y=400, p4_x=410, p4_y=400,
        orient=True,
    )
    result = render_spore_thumbnail(src, inputs, height_px=256)
    assert result.rotation_deg == pytest.approx(0.0, abs=1e-6)
    cx, cy, cw, ch = result.crop_rect_source_pixels
    assert cx <= 400 <= cx + cw
    assert cy <= 400 <= cy + ch


def test_render_polygon_none_when_p3p4_missing():
    src = _solid()
    inputs = SporeThumbnailInputs(
        p1_x=300, p1_y=400, p2_x=500, p2_y=400,
        p3_x=None, p3_y=None, p4_x=None, p4_y=None,
        orient=True,
    )
    result = render_spore_thumbnail(src, inputs, height_px=256)
    assert result.polygon_tile_local is None
    assert result.reason_no_polygon == "missing_p3p4"
    # But we still get a rendered tile (oriented crop around p1/p2).
    assert result.image.size == (result.tile_width_px, result.tile_height_px)
    assert result.tile_height_px == 256


def test_render_polygon_lands_inside_tile_bounds():
    src = _solid((1200, 1000))
    inputs = SporeThumbnailInputs(
        p1_x=600, p1_y=520, p2_x=600, p2_y=480,   # 40 px, up (Δy<0)
        p3_x=580, p3_y=500, p4_x=620, p4_y=500,   # 40 px, horizontal
        orient=True,
    )
    result = render_spore_thumbnail(src, inputs, height_px=320)
    assert result.polygon_tile_local is not None
    for x, y in result.polygon_tile_local:
        assert 0 <= x <= result.tile_width_px
        assert 0 <= y <= result.tile_height_px


# ── Desktop constants + tile geometry ───────────────────────────────────────


def test_render_uses_desktop_padding_constants():
    """Crop bounds must include padding_x/padding_y around the rectangle."""
    src = _solid((1000, 1000))
    # Rectangle 100×40 at centre (500, 500), length axis already pointing UP.
    inputs = SporeThumbnailInputs(
        p1_x=500, p1_y=550, p2_x=500, p2_y=450,   # length 100, up (Δy<0)
        p3_x=480, p3_y=500, p4_x=520, p4_y=500,   # width 40, horizontal
        orient=True,
    )
    result = render_spore_thumbnail(src, inputs, height_px=256)
    # Zero rotation so we can assert on the exact crop rect.
    assert result.rotation_deg == pytest.approx(0.0, abs=1e-6)
    crop_x, crop_y, crop_w, crop_h = result.crop_rect_source_pixels
    # Rectangle bounds in source: x in [480, 520], y in [450, 550].
    # Expected crop = (480-20, 450-15, 40+40, 100+30) = (460, 435, 80, 130).
    assert crop_x == 480 - int(DESKTOP_PADDING_X)
    assert crop_y == 450 - int(DESKTOP_PADDING_Y)
    assert crop_w == int(40 + 2 * DESKTOP_PADDING_X)
    assert crop_h == int(100 + 2 * DESKTOP_PADDING_Y)


def test_render_tile_height_matches_requested_and_width_follows_aspect():
    src = _solid((1000, 1000))
    inputs = SporeThumbnailInputs(
        p1_x=500, p1_y=550, p2_x=500, p2_y=450,
        p3_x=480, p3_y=500, p4_x=520, p4_y=500,
        orient=True,
    )
    result = render_spore_thumbnail(src, inputs, height_px=256)
    assert result.tile_height_px == 256
    # Crop is 80×130 → tile width = round(80 * 256 / 130) = 158.
    assert result.tile_width_px == round(80 * 256 / 130)
    assert result.image.size == (result.tile_width_px, 256)


def test_render_rejects_tiny_height():
    with pytest.raises(ValueError):
        render_spore_thumbnail(
            _solid(),
            SporeThumbnailInputs(p1_x=0, p1_y=0, p2_x=1, p2_y=0),
            height_px=4,
        )


# ── extra_rotation composes with orient ─────────────────────────────────────


def test_render_gallery_rotation_composes_with_orient():
    src = _solid((1000, 1000))
    # extra_rotation = 180 stacked on orient must keep length axis vertical
    # (just flipped end-for-end).
    inputs = SporeThumbnailInputs(
        p1_x=400, p1_y=500, p2_x=600, p2_y=500,   # horizontal
        p3_x=500, p3_y=480, p4_x=500, p4_y=520,
        orient=True,
        extra_rotation_deg=180.0,
    )
    result = render_spore_thumbnail(src, inputs, height_px=256)
    assert result.polygon_tile_local is not None
    xs = [p[0] for p in result.polygon_tile_local]
    ys = [p[1] for p in result.polygon_tile_local]
    assert (max(ys) - min(ys)) > (max(xs) - min(xs))
    # Rotation should be -90 + 180 = 90.
    assert result.rotation_deg == pytest.approx(90.0, abs=1e-6)


# ── Sanity: crop centred on measurement after orient ────────────────────────


def test_plan_matches_render_natural_crop_dimensions():
    """The plan's natural crop dims match the crop rect the single-shot
    renderer would produce for a source that is large enough for the
    natural crop to fit without clamping."""
    src = _solid((1000, 1000))
    inputs = SporeThumbnailInputs(
        p1_x=500, p1_y=550, p2_x=500, p2_y=450,   # length 100 UP
        p3_x=480, p3_y=500, p4_x=520, p4_y=500,   # width 40 horizontal
        orient=True,
    )
    plan = plan_spore_thumbnail(inputs, src.width, src.height)
    # Natural padded crop = width+2*px, length+2*py.
    assert plan.natural_crop_width == pytest.approx(40 + 2 * DESKTOP_PADDING_X, abs=1e-6)
    assert plan.natural_crop_height == pytest.approx(100 + 2 * DESKTOP_PADDING_Y, abs=1e-6)
    # Rotation is 0 → oriented dims match source.
    assert (plan.oriented_width, plan.oriented_height) == (src.width, src.height)


def test_common_crop_render_produces_exact_output_size():
    src = _solid((1000, 1000))
    inputs = SporeThumbnailInputs(
        p1_x=500, p1_y=550, p2_x=500, p2_y=450,
        p3_x=480, p3_y=500, p4_x=520, p4_y=500,
        orient=True,
    )
    plan = plan_spore_thumbnail(inputs, src.width, src.height)
    out = render_spore_thumbnail_common_crop(
        src, plan,
        common_crop_width=200, common_crop_height=300,
        output_width=213, output_height=320,
    )
    assert out.image.size == (213, 320)
    # Fits inside a large source → no padding.
    assert out.padded_x is False and out.padded_y is False
    # Polygon inside output bounds.
    assert out.polygon_tile_local is not None
    for x, y in out.polygon_tile_local:
        assert 0 <= x <= 213
        assert 0 <= y <= 320


def test_common_crop_pads_when_source_smaller():
    src = _solid((30, 30))
    inputs = SporeThumbnailInputs(
        p1_x=15, p1_y=18, p2_x=15, p2_y=12,
        p3_x=14, p3_y=15, p4_x=16, p4_y=15,
        orient=True,
    )
    plan = plan_spore_thumbnail(inputs, src.width, src.height)
    out = render_spore_thumbnail_common_crop(
        src, plan,
        common_crop_width=200, common_crop_height=200,
        output_width=200, output_height=200,
    )
    # Source is much smaller than the requested crop on both axes.
    assert out.padded_x is True
    assert out.padded_y is True
    assert out.image.size == (200, 200)


def test_render_polygon_centered_in_tile():
    """Rectangle centre should map to the tile centre — no drift due to
    padding maths or scale rounding beyond 1px."""
    src = _solid((1000, 1000))
    inputs = SporeThumbnailInputs(
        p1_x=500, p1_y=540, p2_x=500, p2_y=460,   # length 80, UP
        p3_x=480, p3_y=500, p4_x=520, p4_y=500,   # width 40, horizontal
        orient=True,
    )
    result = render_spore_thumbnail(src, inputs, height_px=256)
    assert result.polygon_tile_local is not None
    cx = sum(p[0] for p in result.polygon_tile_local) / 4
    cy = sum(p[1] for p in result.polygon_tile_local) / 4
    tcx = result.tile_width_px / 2
    tcy = result.tile_height_px / 2
    assert cx == pytest.approx(tcx, abs=1.0)
    assert cy == pytest.approx(tcy, abs=1.0)
