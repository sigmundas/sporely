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
    fast_render_tile,
    plan_spore_thumbnail,
    reference_render_tile,
    render_spore_thumbnail,
    render_spore_thumbnail_common_crop,
    resolve_common_crop_placement,
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


# ── Padded off-centre common crop ────────────────────────────────────────────


def test_placement_padded_x_centres_off_centre_measurement():
    """When the crop overflows the source on X and the measurement is
    off-centre in the source, the measurement centre must still land at
    the tile centre — padding falls on the appropriate side, source
    edges stay fully inside the canvas (no wrap-around)."""
    # 100×200 source, 200×200 crop, measurement centred at x=90 (near
    # the right edge of the source). Ideal paste_dx = 100 - 90 = 10, so
    # source (0..100) maps to (10..110) and the measurement (source x=90)
    # lands at canvas x=100 (canvas centre).
    placement = resolve_common_crop_placement(
        oriented_source_w=100, oriented_source_h=200,
        center_x=90.0, center_y=100.0,
        common_crop_width_px=200, common_crop_height_px=200,
        output_width=200, output_height=200,
    )
    assert placement.padded_x is True
    assert placement.padded_y is False
    assert placement.paste_dx == 10
    # Measurement lands at tile centre.
    cx, cy = placement.source_to_tile(90.0, 100.0)
    assert cx == pytest.approx(100.0)
    assert cy == pytest.approx(100.0)
    # Source stays fully inside the canvas.
    left, _ = placement.source_to_tile(0.0, 0.0)
    right, _ = placement.source_to_tile(100.0, 0.0)
    assert left >= 0
    assert right <= 200


def test_placement_padded_y_centres_off_centre_measurement():
    placement = resolve_common_crop_placement(
        oriented_source_w=200, oriented_source_h=100,
        center_x=100.0, center_y=10.0,
        common_crop_width_px=200, common_crop_height_px=200,
        output_width=200, output_height=200,
    )
    assert placement.padded_x is False
    assert placement.padded_y is True
    # Measurement centre → tile centre.
    cx, cy = placement.source_to_tile(100.0, 10.0)
    assert cy == pytest.approx(100.0, abs=1.0)
    assert cx == pytest.approx(100.0, abs=1.0)
    # Source stays fully inside canvas.
    _, top = placement.source_to_tile(0.0, 0.0)
    _, bottom = placement.source_to_tile(0.0, 100.0)
    assert top >= 0
    assert bottom <= 200


def test_placement_padded_both_axes_off_centre_measurement():
    placement = resolve_common_crop_placement(
        oriented_source_w=100, oriented_source_h=100,
        center_x=80.0, center_y=20.0,
        common_crop_width_px=200, common_crop_height_px=200,
        output_width=200, output_height=200,
    )
    assert placement.padded_x is True and placement.padded_y is True
    cx, cy = placement.source_to_tile(80.0, 20.0)
    assert cx == pytest.approx(100.0)
    assert cy == pytest.approx(100.0)


def test_placement_padded_clamps_when_measurement_near_source_edge():
    """When paste_dx would push the source outside the canvas, clamp so
    the source stays fully inside — measurement drifts off-centre but
    no source pixels are lost or wrapped."""
    # 100×100 source, 110×100 crop, measurement at x=0 (source left
    # edge). Ideal paste_dx = 110/2 - 0 = 55 → but that would push
    # source right edge to 155 (outside canvas 110). Clamp to
    # max_paste_dx = 110 - 100 = 10.
    placement = resolve_common_crop_placement(
        oriented_source_w=100, oriented_source_h=100,
        center_x=0.0, center_y=50.0,
        common_crop_width_px=110, common_crop_height_px=100,
        output_width=110, output_height=100,
    )
    assert placement.padded_x is True
    assert placement.paste_dx == 10  # clamped, not 55
    # Source stays inside canvas.
    left, _ = placement.source_to_tile(0.0, 0.0)
    right, _ = placement.source_to_tile(100.0, 0.0)
    assert 0 <= left
    assert right <= 110


def test_placement_padded_polygon_lands_at_measurement_not_source_centre():
    """Regression guard: when the measurement is far from the source
    centre and the crop is padded, the transformed polygon must land at
    the measurement, not at the source centre."""
    # Source 100×100, measurement centred at (10, 90), a 20×20 rectangle
    # around it. Common crop 200×200 (padded on both axes).
    polygon = [
        (0.0, 80.0),  # corner near source top-left
        (20.0, 80.0),
        (20.0, 100.0),
        (0.0, 100.0),
    ]
    placement = resolve_common_crop_placement(
        oriented_source_w=100, oriented_source_h=100,
        center_x=10.0, center_y=90.0,
        common_crop_width_px=200, common_crop_height_px=200,
        output_width=200, output_height=200,
    )
    transformed = placement.transform_polygon(polygon)
    assert transformed is not None
    poly_cx = sum(x for x, _ in transformed) / 4
    poly_cy = sum(y for _, y in transformed) / 4
    # Polygon centre lands at the tile centre (100, 100).
    assert poly_cx == pytest.approx(100.0)
    assert poly_cy == pytest.approx(100.0)


# ── Fast renderer parity (Phase 2.B) ─────────────────────────────────────────
#
# The fast renderer collapses rotate + crop + resize into one inverse
# affine. These tests assert both geometric parity (polygon coords land
# within 0.5 px of the reference) and image-similarity parity (mean
# absolute RGB diff below 3/255, max diff below 15/255).


def _rgb_diff_stats(a: Image.Image, b: Image.Image) -> tuple[float, int]:
    if a.mode != "RGB":
        a = a.convert("RGB")
    if b.mode != "RGB":
        b = b.convert("RGB")
    assert a.size == b.size, f"{a.size} vs {b.size}"
    apx = a.load()
    bpx = b.load()
    total = 0
    max_diff = 0
    for y in range(a.height):
        for x in range(a.width):
            ar, ag, ab = apx[x, y]
            br, bg, bb = bpx[x, y]
            diff = abs(ar - br) + abs(ag - bg) + abs(ab - bb)
            total += diff
            per_channel_max = max(abs(ar - br), abs(ag - bg), abs(ab - bb))
            if per_channel_max > max_diff:
                max_diff = per_channel_max
    mean_diff = total / (a.width * a.height * 3.0)
    return mean_diff, max_diff


def _structured_image(size: tuple[int, int]) -> Image.Image:
    """Smooth gradient with a mid-frequency component — approximates a
    real microscope frame better than a 2-px checkerboard, so the
    documented parity threshold (mean < 3, max < 15) applies without
    a pathological high-frequency test pattern.

    Real spore microscopy is mostly low-frequency (out-of-focus
    background) with a mid-frequency spore silhouette; resampling
    filter differences there are perceptually small.
    """
    img = Image.new("RGB", size, (60, 60, 80))
    px = img.load()
    w, h = size
    for y in range(h):
        for x in range(w):
            # Two overlapping gradients so the pattern isn't flat but
            # never spikes.  Deliberately avoid stride-2 alternation.
            r = int(30 + (x * 200) / max(1, w))
            g = int(30 + (y * 200) / max(1, h))
            b = int(30 + ((x + y) * 100) / max(1, w + h))
            px[x, y] = (r, g, b)
    return img


@pytest.mark.parametrize("rotation_deg", [0.0, -5.0, 5.0, -45.0, 45.0, -89.0, 180.0])
def test_fast_render_matches_reference_geometry(rotation_deg):
    """Polygon coords produced by both renderers must match within
    0.5 px across every documented rotation."""
    src = _structured_image((600, 600))
    # Length axis roughly vertical; extra_rotation forces the fast path
    # to exercise the affine.
    inputs = SporeThumbnailInputs(
        p1_x=300, p1_y=350, p2_x=300, p2_y=250,
        p3_x=290, p3_y=300, p4_x=310, p4_y=300,
        orient=False,  # only extra_rotation kicks in below
        extra_rotation_deg=float(rotation_deg),
        length_um=10.0, width_um=4.0,
    )
    plan = plan_spore_thumbnail(inputs, src.width, src.height)
    ref = reference_render_tile(
        src, plan,
        common_crop_width=200, common_crop_height=300,
        output_width=200, output_height=300,
    )
    fast = fast_render_tile(
        src, plan,
        common_crop_width=200, common_crop_height=300,
        output_width=200, output_height=300,
    )
    assert ref.polygon_tile_local is not None
    assert fast.polygon_tile_local is not None
    # Polygon coords are computed from the same placement helper in
    # both paths, so they should agree to machine precision.
    for (rx, ry), (fx, fy) in zip(ref.polygon_tile_local, fast.polygon_tile_local):
        assert abs(rx - fx) < 0.5
        assert abs(ry - fy) < 0.5


@pytest.mark.parametrize(
    "rotation_deg,common_crop,output_size",
    [
        (0.0, (240, 260), (240, 260)),
        (-45.0, (240, 260), (200, 200)),
        (30.0, (240, 260), (160, 200)),
    ],
)
def test_fast_render_image_diff_within_threshold(
    rotation_deg, common_crop, output_size,
):
    """Fast (BICUBIC) vs reference (BILINEAR + LANCZOS) must agree to
    a documented image-diff threshold: mean abs diff < 3.0/255, max
    channel diff < 15/255. Real spore tiles are noisier than the
    difference; the threshold is well inside the perceptual budget."""
    src = _structured_image((600, 600))
    inputs = SporeThumbnailInputs(
        p1_x=300, p1_y=340, p2_x=300, p2_y=260,
        p3_x=290, p3_y=300, p4_x=310, p4_y=300,
        orient=False,
        extra_rotation_deg=float(rotation_deg),
        length_um=8.0, width_um=2.0,
    )
    plan = plan_spore_thumbnail(inputs, src.width, src.height)
    ref = reference_render_tile(
        src, plan,
        common_crop_width=common_crop[0], common_crop_height=common_crop[1],
        output_width=output_size[0], output_height=output_size[1],
    )
    fast = fast_render_tile(
        src, plan,
        common_crop_width=common_crop[0], common_crop_height=common_crop[1],
        output_width=output_size[0], output_height=output_size[1],
    )
    mean_diff, max_diff = _rgb_diff_stats(ref.image, fast.image)
    # Doc threshold: mean < 3.0, max < 15 (out of 255 per channel).
    assert mean_diff < 3.0, (
        f"mean abs RGB diff too large: {mean_diff:.3f}"
    )
    assert max_diff < 15, (
        f"max channel diff too large: {max_diff}"
    )


def test_fast_render_padded_matches_reference_measurement_position():
    """Padded off-centre placement path — measurement centre must land
    at tile centre for both paths."""
    src = _structured_image((100, 100))
    inputs = SporeThumbnailInputs(
        p1_x=10, p1_y=55, p2_x=10, p2_y=45,
        p3_x=5, p3_y=50, p4_x=15, p4_y=50,
        orient=True,
        length_um=10.0, width_um=4.0,
    )
    plan = plan_spore_thumbnail(inputs, src.width, src.height)
    for renderer in (reference_render_tile, fast_render_tile):
        result = renderer(
            src, plan,
            common_crop_width=200, common_crop_height=200,
            output_width=200, output_height=200,
        )
        assert result.padded_x is True
        assert result.padded_y is True
        assert result.polygon_tile_local is not None
        cx = sum(p[0] for p in result.polygon_tile_local) / 4
        cy = sum(p[1] for p in result.polygon_tile_local) / 4
        assert cx == pytest.approx(100.0, abs=1.0)
        assert cy == pytest.approx(100.0, abs=1.0)


def test_fast_render_grayscale_source_converts_and_resamples():
    """Grayscale sources should be converted to RGB and sampled by the
    fast path without raising."""
    src = Image.new("L", (400, 400), 128)
    inputs = SporeThumbnailInputs(
        p1_x=200, p1_y=240, p2_x=200, p2_y=160,
        p3_x=190, p3_y=200, p4_x=210, p4_y=200,
        orient=True,
        length_um=8.0, width_um=2.0,
    )
    plan = plan_spore_thumbnail(inputs, src.width, src.height)
    fast = fast_render_tile(
        src, plan,
        common_crop_width=180, common_crop_height=200,
        output_width=180, output_height=200,
    )
    assert fast.image.mode == "RGB"
    assert fast.image.size == (180, 200)


def test_render_spore_thumbnail_common_crop_env_flag_switches_to_reference(
    monkeypatch,
):
    """`SPORELY_MOSAIC_USE_REFERENCE=1` forces the reference path so
    users can bisect fast-path regressions in production without a
    code change."""
    src = _structured_image((400, 400))
    inputs = SporeThumbnailInputs(
        p1_x=200, p1_y=240, p2_x=200, p2_y=160,
        p3_x=190, p3_y=200, p4_x=210, p4_y=200,
        orient=True,
        length_um=8.0, width_um=2.0,
    )
    plan = plan_spore_thumbnail(inputs, src.width, src.height)
    monkeypatch.setenv("SPORELY_MOSAIC_USE_REFERENCE", "1")
    dispatched = render_spore_thumbnail_common_crop(
        src, plan,
        common_crop_width=180, common_crop_height=200,
        output_width=180, output_height=200,
    )
    ref = reference_render_tile(
        src, plan,
        common_crop_width=180, common_crop_height=200,
        output_width=180, output_height=200,
    )
    # When the env flag routes to the reference, the two results are
    # byte-identical.
    assert dispatched.image.tobytes() == ref.image.tobytes()


def test_placement_padded_render_places_source_pixels_and_background():
    """Pillow raster path fills background on the correct sides when the
    source is smaller than the crop and the measurement is off-centre."""
    # Distinctive coloured source so we can detect where it sits on
    # the padded canvas. Measurement at (10, 50) → ideal paste_dx=90
    # (source left edge at canvas x=90, right edge at 190).
    src = Image.new("RGB", (100, 100), (200, 50, 50))
    inputs = SporeThumbnailInputs(
        p1_x=10, p1_y=55, p2_x=10, p2_y=45,   # length axis vertical
        p3_x=5,  p3_y=50, p4_x=15, p4_y=50,   # width axis horizontal
        orient=True,
        length_um=10.0, width_um=4.0,
    )
    plan = plan_spore_thumbnail(inputs, src.width, src.height)
    result = render_spore_thumbnail_common_crop(
        src, plan,
        common_crop_width=200, common_crop_height=200,
        output_width=200, output_height=200,
    )
    assert result.padded_x is True
    assert result.padded_y is True
    canvas = result.image
    # Left edge of canvas is background (source doesn't reach here).
    r, g, b = canvas.getpixel((5, 100))
    assert (r, g, b) == plan.inputs.background_rgb, (
        f"expected background at left edge, got ({r},{g},{b})"
    )
    # Canvas centre lands on source pixels (colour = 200,50,50).
    r, g, b = canvas.getpixel((100, 100))
    assert (r, g, b) == (200, 50, 50)
