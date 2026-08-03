"""Tests for the shared spore mosaic planning core.

The planning core (`utils.spore_mosaic_render`) drives every mosaic
backend — cloud atlas, live desktop preview, PNG/JPEG export, hybrid
SVG export. These tests guard the grid selector, the common physical
crop maths, and the per-tile layout independently of any raster/Qt
backend.
"""

from __future__ import annotations

import math
from pathlib import Path

import pytest

from utils.spore_mosaic_render import (
    GRID_EMPTY_FRACTION_PENALTY,
    MOSAIC_PLAN_REASON_ALL_SKIPPED,
    MOSAIC_PLAN_REASON_NO_INPUT,
    MosaicAnnotationSpec,
    MosaicGridPolicy,
    MosaicPlanningResult,
    SporeMosaicSource,
    plan_mosaic,
    select_grid_shape,
)
from utils.spore_thumbnail_render import (
    SporeThumbnailInputs,
    plan_common_crop,
    plan_spore_thumbnail,
)


# ── select_grid_shape ───────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "count",
    [1, 2, 7, 11, 13, 47],
)
def test_select_grid_shape_square_cells_square_target_produces_valid_grid(count):
    cols, rows = select_grid_shape(count, cell_w=100, cell_h=100, target_aspect=1.0)
    assert cols >= 1 and rows >= 1
    assert cols * rows >= count
    # Enough capacity but not two full extra rows / cols.
    assert (cols - 1) * rows < count or cols * (rows - 1) < count


def test_select_grid_shape_single_tile_is_1x1():
    assert select_grid_shape(1, 100, 100, 1.0) == (1, 1)


def test_select_grid_shape_target_4_3_with_tall_cells_drifts_wider():
    """Tall narrow cells with a 4:3 target must widen the grid."""
    cols_sq, rows_sq = select_grid_shape(24, 100, 100, 1.0)
    cols_43, rows_43 = select_grid_shape(24, 100, 100, 4.0 / 3.0)
    assert cols_43 >= cols_sq
    assert (cols_43, rows_43) != (cols_sq, rows_sq)


def test_select_grid_shape_target_1_with_wide_cells_drifts_taller():
    """Wide cells with a square-image target must add more rows."""
    tall_cols, tall_rows = select_grid_shape(24, 300, 100, 1.0)
    ref_cols, ref_rows = select_grid_shape(24, 100, 100, 1.0)
    assert tall_rows >= ref_rows
    assert tall_cols <= ref_cols


def test_select_grid_shape_partial_last_row_is_minimised():
    """9 tiles into square-image + square-cells should be 3×3 (no empties)
    rather than a wider grid with more empties."""
    assert select_grid_shape(9, 100, 100, 1.0) == (3, 3)


def test_select_grid_shape_scoring_no_gap():
    """Aspect error uses log; doubling and halving score identically.
    Ties are broken by a closer-to-square grid; iteration order picks
    the first candidate with the minimum penalty."""
    # For count=2 with square cells + square target, both (1,2) and (2,1)
    # score the same log-aspect error (|log 0.5| == |log 2|) and zero
    # empties, so the penalty is identical. Tie-break on abs(cols-rows)
    # also ties; iteration order picks (1, 2).
    assert select_grid_shape(2, 100, 100, 1.0) == (1, 2)


def test_select_grid_shape_penalty_prefers_zero_empties_when_aspect_is_close():
    """A grid with zero empties and small aspect error beats one with
    perfect aspect but wasted empty cells — as the penalty rule says."""
    # 4 tiles, square cells, square target:
    #   2×2: aspect 1.0, empties 0 → penalty 0.
    #   3×2: aspect 1.5, empties 2/6=0.333 → penalty log(1.5) + 1.5*0.333 = ~0.905
    # So (2, 2) must win.
    assert select_grid_shape(4, 100, 100, 1.0) == (2, 2)


def test_select_grid_shape_penalty_accepts_a_few_empties_for_much_better_aspect():
    """If a grid without empties has huge aspect error, the penalty rule
    should accept a modest empty fraction to gain a big aspect win."""
    # Take a count that forces the search: e.g. count=7 with square cells
    # and 4:3 target. 4×2 (1 empty) vs 7×1 (0 empties):
    #   4×2: aspect 4/2=2.0, empties 1/8=0.125 → penalty log(2/(4/3)) + K*0.125
    #        = |log(1.5)| + 1.5*0.125 ≈ 0.405 + 0.188 = 0.593
    #   7×1: aspect 7, empties 0 → penalty log(7/(4/3)) = log(5.25) ≈ 1.658
    # → 4×2 wins.
    cols, rows = select_grid_shape(7, 100, 100, 4.0 / 3.0)
    assert (cols, rows) == (4, 2)


def test_select_grid_shape_penalty_weight_is_documented_constant():
    """The exposed constant is what tests depend on — keep it discoverable."""
    assert GRID_EMPTY_FRACTION_PENALTY == 1.5


def test_select_grid_shape_rejects_bad_input():
    with pytest.raises(ValueError):
        select_grid_shape(0, 100, 100, 1.0)
    with pytest.raises(ValueError):
        select_grid_shape(3, 0, 100, 1.0)
    with pytest.raises(ValueError):
        select_grid_shape(3, 100, 0, 1.0)
    with pytest.raises(ValueError):
        select_grid_shape(3, 100, 100, 0)


def test_select_grid_shape_cells_do_not_overlap_and_stay_in_canvas():
    """Every produced grid must place `count` tiles within its own canvas
    without overlap."""
    for count in (1, 2, 7, 11, 13, 47):
        cell_w = 120
        cell_h = 80
        cols, rows = select_grid_shape(count, cell_w, cell_h, 4.0 / 3.0)
        canvas_w = cols * cell_w
        canvas_h = rows * cell_h
        seen: set[tuple[int, int]] = set()
        for index in range(count):
            row = index // cols
            col = index % cols
            x = col * cell_w
            y = row * cell_h
            assert 0 <= x <= canvas_w - cell_w
            assert 0 <= y <= canvas_h - cell_h
            assert (x, y) not in seen
            seen.add((x, y))


# ── plan_common_crop ────────────────────────────────────────────────────────


def _plan_from(
    length_um: float, width_um: float,
    length_px: int, width_px: int,
    source_size: int = 800,
):
    inputs = SporeThumbnailInputs(
        p1_x=source_size // 2, p1_y=source_size // 2 + length_px // 2,
        p2_x=source_size // 2, p2_y=source_size // 2 - length_px // 2,
        p3_x=source_size // 2 - width_px // 2, p3_y=source_size // 2,
        p4_x=source_size // 2 + width_px // 2, p4_y=source_size // 2,
        orient=True,
        length_um=length_um, width_um=width_um,
    )
    return plan_spore_thumbnail(inputs, source_size, source_size)


def test_plan_common_crop_picks_max_natural_dimensions():
    plans = [
        _plan_from(10, 4, 100, 20),   # 10 px/µm length, 5 px/µm width
        _plan_from(20, 8, 200, 40),   # 10 px/µm length, 5 px/µm width
    ]
    result = plan_common_crop(plans, output_height_px=320)
    assert result is not None
    assert result.common_crop_width_um == pytest.approx(
        max(p.natural_crop_width_um for p in plans)
    )
    assert result.common_crop_height_um == pytest.approx(
        max(p.natural_crop_height_um for p in plans)
    )
    assert result.output_tile_height == 320
    assert result.output_tile_width >= 1


def test_plan_common_crop_none_when_no_scale_available():
    plans = [_plan_from(0, 0, 100, 20)]  # length_um=0 → no physical scale
    assert plan_common_crop(plans, output_height_px=320) is None


def test_plan_common_crop_none_for_empty_input():
    assert plan_common_crop([], output_height_px=320) is None


def test_plan_common_crop_rejects_tiny_height():
    plans = [_plan_from(10, 4, 100, 20)]
    with pytest.raises(ValueError):
        plan_common_crop(plans, output_height_px=4)


# ── plan_mosaic ─────────────────────────────────────────────────────────────


def _source(
    item_id: int, *,
    p1=(300, 350), p2=(300, 250),
    p3=(290, 300), p4=(310, 300),
    length_um: float | None = 10.0,
    width_um: float | None = 4.0,
    extra_rotation_deg: float = 0.0,
    source_size: tuple[int, int] = (600, 600),
) -> SporeMosaicSource:
    return SporeMosaicSource(
        item_id=item_id,
        source_path=Path(f"/tmp/nonexistent-{item_id}.png"),
        source_width=source_size[0], source_height=source_size[1],
        p1_x=p1[0], p1_y=p1[1], p2_x=p2[0], p2_y=p2[1],
        p3_x=p3[0] if p3 else None, p3_y=p3[1] if p3 else None,
        p4_x=p4[0] if p4 else None, p4_y=p4[1] if p4 else None,
        length_um=length_um, width_um=width_um,
        extra_rotation_deg=extra_rotation_deg,
    )


def test_plan_mosaic_no_input_reason_and_empty_skipped():
    result = plan_mosaic(
        [], orient=True, grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
    )
    assert isinstance(result, MosaicPlanningResult)
    assert result.layout is None
    assert result.skipped == []
    assert result.reason == MOSAIC_PLAN_REASON_NO_INPUT


def test_plan_mosaic_all_skipped_reason_preserves_diagnostics():
    src = _source(1, length_um=None, width_um=None)
    result = plan_mosaic(
        [src], orient=True, grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
    )
    assert result.layout is None
    assert result.reason == MOSAIC_PLAN_REASON_ALL_SKIPPED
    # Even on total failure the caller keeps the per-item skip reasons.
    assert (1, "missing_calibration") in result.skipped


def test_plan_mosaic_records_invalid_source_dims_in_all_skipped_reason():
    bad = SporeMosaicSource(
        item_id=42,
        source_path=Path("/tmp/bad.png"),
        source_width=0, source_height=0,
        p1_x=0, p1_y=0, p2_x=1, p2_y=0,
        length_um=10.0, width_um=4.0,
    )
    result = plan_mosaic(
        [bad], orient=True, grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
    )
    assert result.layout is None
    assert result.reason == MOSAIC_PLAN_REASON_ALL_SKIPPED
    assert (42, "invalid source dims") in result.skipped


def test_plan_mosaic_uniform_tile_dims_across_measurements():
    """Two sources with equal physical length_um / width_um but different
    px/µm (source scales) must produce identical tile output dimensions."""
    # Low-res: length 100 px → 10 px/µm.
    low = SporeMosaicSource(
        item_id=1,
        source_path=Path("/tmp/lo.png"),
        source_width=400, source_height=400,
        p1_x=200, p1_y=250, p2_x=200, p2_y=150,
        p3_x=190, p3_y=200, p4_x=210, p4_y=200,
        length_um=10.0, width_um=4.0,
    )
    # Hi-res: length 200 px → 20 px/µm. Same physical dims.
    hi = SporeMosaicSource(
        item_id=2,
        source_path=Path("/tmp/hi.png"),
        source_width=800, source_height=800,
        p1_x=400, p1_y=500, p2_x=400, p2_y=300,
        p3_x=380, p3_y=400, p4_x=420, p4_y=400,
        length_um=10.0, width_um=4.0,
    )
    layout = plan_mosaic(
        [low, hi], orient=True,
        grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
    ).layout
    assert layout is not None
    assert len(layout.cells) == 2
    tiles = [cell.tile for cell in layout.cells]
    assert tiles[0].output_w_px == tiles[1].output_w_px
    assert tiles[0].output_h_px == tiles[1].output_h_px
    # Common physical crop is the same for both.
    assert tiles[0].common_crop_width_um == pytest.approx(tiles[1].common_crop_width_um)
    assert tiles[0].common_crop_height_um == pytest.approx(tiles[1].common_crop_height_um)
    # But the per-tile pixel crop DIFFERS because px/µm differs.
    assert tiles[0].common_crop_width_px != tiles[1].common_crop_width_px
    assert tiles[0].common_crop_height_px != tiles[1].common_crop_height_px


def test_plan_mosaic_grid_policy_square_image_targets_square_atlas():
    """A dozen slender spores under SQUARE_IMAGE policy should yield an
    atlas close to square (aspect < 25% off)."""
    sources = []
    for i in range(12):
        # Slender: q ~ 2.3 → length 46 px, width 20 px. Length=10 µm,
        # width=4.3 µm.
        sources.append(SporeMosaicSource(
            item_id=i + 1,
            source_path=Path(f"/tmp/{i}.png"),
            source_width=800, source_height=800,
            p1_x=400, p1_y=423, p2_x=400, p2_y=377,
            p3_x=390, p3_y=400, p4_x=410, p4_y=400,
            length_um=10.0, width_um=4.3,
        ))
    layout = plan_mosaic(
        sources, orient=True,
        grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
    ).layout
    assert layout is not None
    aspect = layout.mosaic_width_px / max(1, layout.mosaic_height_px)
    assert abs(aspect - 1.0) < 0.25, (
        f"SQUARE_IMAGE policy produced non-square atlas: "
        f"{layout.mosaic_width_px}x{layout.mosaic_height_px} "
        f"aspect={aspect:.3f}"
    )


def test_plan_mosaic_grid_policy_4_3_targets_wider_atlas():
    sources = [
        _source(i + 1, source_size=(800, 800))
        for i in range(12)
    ]
    sq = plan_mosaic(
        sources, orient=True, grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
    ).layout
    wide = plan_mosaic(
        sources, orient=True, grid_policy=MosaicGridPolicy.ASPECT_4_3,
        output_tile_height_px=320,
    ).layout
    assert sq is not None and wide is not None
    sq_aspect = sq.mosaic_width_px / sq.mosaic_height_px
    wide_aspect = wide.mosaic_width_px / wide.mosaic_height_px
    # 4:3 layout should be at least as wide (per unit height) as square.
    assert wide_aspect >= sq_aspect


def test_plan_mosaic_orient_off_keeps_source_orientation():
    """orient=False leaves rotation ~0 for horizontal length axes."""
    src = SporeMosaicSource(
        item_id=1, source_path=Path("/tmp/x.png"),
        source_width=800, source_height=800,
        p1_x=300, p1_y=400, p2_x=500, p2_y=400,
        p3_x=400, p3_y=380, p4_x=400, p4_y=420,
        length_um=20.0, width_um=4.0,
    )
    layout = plan_mosaic(
        [src], orient=False,
        grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
    ).layout
    assert layout is not None
    tile = layout.cells[0].tile
    assert tile.rotation_deg == pytest.approx(0.0, abs=0.1)


def test_plan_mosaic_orient_on_swings_length_axis_vertical():
    """orient=True rotates a horizontal length axis to vertical."""
    src = SporeMosaicSource(
        item_id=1, source_path=Path("/tmp/x.png"),
        source_width=800, source_height=800,
        p1_x=300, p1_y=400, p2_x=500, p2_y=400,
        p3_x=400, p3_y=380, p4_x=400, p4_y=420,
        length_um=20.0, width_um=4.0,
    )
    # Polygon needs explicit opt-in now — cloud path (annotation=None)
    # relies on the raster placement helper for its overlay geometry,
    # so the plan itself only carries polygon when a backend has asked
    # for it via `MosaicAnnotationSpec(draw_rectangle=True)`.
    layout = plan_mosaic(
        [src], orient=True,
        grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
        annotation=MosaicAnnotationSpec(draw_rectangle=True),
    ).layout
    assert layout is not None
    tile = layout.cells[0].tile
    assert tile.rotation_deg == pytest.approx(-90.0, abs=1.0)
    assert tile.oriented_polygon_tile_local is not None
    xs = [p[0] for p in tile.oriented_polygon_tile_local]
    ys = [p[1] for p in tile.oriented_polygon_tile_local]
    assert (max(ys) - min(ys)) > (max(xs) - min(xs))


def test_plan_mosaic_extra_rotation_composes_with_orient():
    """extra_rotation_deg=180 stacked on orient keeps length vertical."""
    src = SporeMosaicSource(
        item_id=1, source_path=Path("/tmp/x.png"),
        source_width=800, source_height=800,
        p1_x=300, p1_y=400, p2_x=500, p2_y=400,
        p3_x=400, p3_y=380, p4_x=400, p4_y=420,
        length_um=20.0, width_um=4.0,
        extra_rotation_deg=180.0,
    )
    layout = plan_mosaic(
        [src], orient=True,
        grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
        annotation=MosaicAnnotationSpec(draw_rectangle=True),
    ).layout
    assert layout is not None
    tile = layout.cells[0].tile
    # -90° (orient) + 180° (extra) = 90°.
    assert tile.rotation_deg == pytest.approx(90.0, abs=1.0)
    assert tile.oriented_polygon_tile_local is not None
    xs = [p[0] for p in tile.oriented_polygon_tile_local]
    ys = [p[1] for p in tile.oriented_polygon_tile_local]
    assert (max(ys) - min(ys)) > (max(xs) - min(xs))


def test_plan_mosaic_records_skipped_for_bad_dims():
    good = _source(1)
    bad = SporeMosaicSource(
        item_id=99,
        source_path=Path("/tmp/bad.png"),
        source_width=0, source_height=0,  # invalid
        p1_x=0, p1_y=0, p2_x=1, p2_y=0,
        length_um=10.0, width_um=4.0,
    )
    layout = plan_mosaic(
        [good, bad], orient=True,
        grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
    ).layout
    assert layout is not None
    assert [c.tile.source.item_id for c in layout.cells] == [1]
    assert (99, "invalid source dims") in layout.skipped


def test_plan_mosaic_records_skipped_for_missing_calibration():
    good = _source(1)
    unscaled = _source(2, length_um=None, width_um=None)
    layout = plan_mosaic(
        [good, unscaled], orient=True,
        grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
    ).layout
    assert layout is not None
    assert [c.tile.source.item_id for c in layout.cells] == [1]
    assert (2, "missing_calibration") in layout.skipped


def test_plan_mosaic_uses_scale_um_per_px_when_length_um_absent():
    """Authoritative image µm-per-pixel is enough on its own; the
    planner back-derives length_um and width_um from the pixel spans."""
    src = SporeMosaicSource(
        item_id=1,
        source_path=Path("/tmp/x.png"),
        source_width=800, source_height=800,
        p1_x=400, p1_y=450, p2_x=400, p2_y=350,   # 100 px length
        p3_x=380, p3_y=400, p4_x=420, p4_y=400,   # 40 px width
        length_um=None, width_um=None,
        scale_um_per_px=0.1,                        # 100 px = 10 µm
    )
    layout = plan_mosaic(
        [src], orient=True,
        grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
    ).layout
    assert layout is not None
    tile = layout.cells[0].tile
    assert tile.diagnostics["length_um"] == pytest.approx(10.0, abs=1e-6)
    assert tile.diagnostics["width_um"] == pytest.approx(4.0, abs=1e-6)


def test_plan_mosaic_prefers_scale_um_per_px_over_endpoint_derivation():
    """When both are present, scale_um_per_px is authoritative for
    width — the endpoint-derived width_um is overridden only if the
    caller left it None."""
    src = SporeMosaicSource(
        item_id=1,
        source_path=Path("/tmp/x.png"),
        source_width=800, source_height=800,
        p1_x=400, p1_y=450, p2_x=400, p2_y=350,   # 100 px
        p3_x=380, p3_y=400, p4_x=420, p4_y=400,   # 40 px
        length_um=10.0, width_um=None,             # width derived from scale
        scale_um_per_px=0.1,
    )
    layout = plan_mosaic(
        [src], orient=True,
        grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
    ).layout
    assert layout is not None
    tile = layout.cells[0].tile
    assert tile.diagnostics["length_um"] == pytest.approx(10.0)
    assert tile.diagnostics["width_um"] == pytest.approx(4.0, abs=1e-6)


def test_plan_mosaic_image_scale_wins_geometry_over_stored_length_um():
    """When image `scale_um_per_px` and stored `length_um` disagree,
    the render geometry follows the image scale — that's what actual
    source pixel spacing dictates."""
    # 100 px length line, image scale = 0.1 µm/px → image-derived length
    # = 10 µm. Stored length_um = 40 (much larger, e.g. user made an
    # inconsistent edit or the calibration was retuned after saving).
    src = SporeMosaicSource(
        item_id=1,
        source_path=Path("/tmp/x.png"),
        source_width=800, source_height=800,
        p1_x=400, p1_y=450, p2_x=400, p2_y=350,   # 100 px length
        p3_x=380, p3_y=400, p4_x=420, p4_y=400,   # 40 px width
        length_um=40.0, width_um=16.0,             # stored disagrees (4x)
        scale_um_per_px=0.1,                        # 100 px = 10 µm
    )
    layout = plan_mosaic(
        [src], orient=True,
        grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
        annotation=MosaicAnnotationSpec(draw_dimensions=True),
    ).layout
    assert layout is not None
    tile = layout.cells[0].tile
    # Geometry follows image scale: length_um in diagnostics is the
    # image-derived value, not the stored one.
    assert tile.diagnostics["length_um"] == pytest.approx(10.0, abs=1e-6)
    assert tile.diagnostics["width_um"] == pytest.approx(4.0, abs=1e-6)
    # px-per-µm on both axes agrees with 1/scale (isotropic).
    assert tile.diagnostics["length_axis_px_per_um"] == pytest.approx(10.0, abs=1e-6)
    assert tile.diagnostics["width_axis_px_per_um"] == pytest.approx(10.0, abs=1e-6)


def test_plan_mosaic_label_text_uses_stored_length_um_when_scale_disagrees():
    """Second half of the calibration contract: the label text still
    shows the user's stored length_um / width_um even when image
    calibration would derive a different µm value from the same pixels.
    Users see the number they saved; render geometry uses the
    authoritative image scale."""
    src = SporeMosaicSource(
        item_id=1,
        source_path=Path("/tmp/x.png"),
        source_width=800, source_height=800,
        p1_x=400, p1_y=450, p2_x=400, p2_y=350,
        p3_x=380, p3_y=400, p4_x=420, p4_y=400,
        length_um=40.0, width_um=16.0,             # stored — displayed
        scale_um_per_px=0.1,                        # image scale drives geometry
    )
    layout = plan_mosaic(
        [src], orient=True,
        grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
        annotation=MosaicAnnotationSpec(draw_dimensions=True),
    ).layout
    assert layout is not None
    tile = layout.cells[0].tile
    assert tile.label is not None
    # Label reads STORED values.
    assert tile.label["text"] == "40.0 x 16.0"
    # Diagnostics carry both, so downstream can detect the disagreement.
    assert tile.diagnostics["label_length_um"] == pytest.approx(40.0)
    assert tile.diagnostics["label_width_um"] == pytest.approx(16.0)


def test_plan_mosaic_skips_when_no_scale_and_no_length():
    """Without scale_um_per_px AND without length_um the planner refuses
    to render the tile — it never renders at an unknown scale."""
    src = SporeMosaicSource(
        item_id=99,
        source_path=Path("/tmp/x.png"),
        source_width=800, source_height=800,
        p1_x=400, p1_y=450, p2_x=400, p2_y=350,
        p3_x=380, p3_y=400, p4_x=420, p4_y=400,
        length_um=None, width_um=None,
        scale_um_per_px=None,
    )
    result = plan_mosaic(
        [src], orient=True,
        grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
    )
    assert result.layout is None
    assert result.reason == MOSAIC_PLAN_REASON_ALL_SKIPPED
    assert (99, "missing_calibration") in result.skipped


def test_plan_mosaic_places_cells_without_overlap():
    sources = [_source(i + 1) for i in range(7)]
    layout = plan_mosaic(
        sources, orient=True,
        grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
    ).layout
    assert layout is not None
    assert len(layout.cells) == 7
    seen: set[tuple[int, int]] = set()
    for cell in layout.cells:
        assert cell.w_px == layout.tile_width_px
        assert cell.h_px == layout.tile_height_px
        assert 0 <= cell.x_px <= layout.mosaic_width_px - cell.w_px
        assert 0 <= cell.y_px <= layout.mosaic_height_px - cell.h_px
        assert (cell.x_px, cell.y_px) not in seen
        seen.add((cell.x_px, cell.y_px))


def test_plan_mosaic_label_is_semantic_dict_when_dims_available():
    """Label is a semantic dict — text + anchor + align — with no
    font-metric offsets baked in. Each backend positions the glyphs
    itself."""
    layout = plan_mosaic(
        [_source(1, length_um=10.5, width_um=4.5)],
        orient=True, grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
        annotation=MosaicAnnotationSpec(draw_dimensions=True),
    ).layout
    assert layout is not None
    tile = layout.cells[0].tile
    assert tile.label is not None
    assert set(tile.label) >= {"text", "anchor", "align"}
    assert tile.label["text"] == "10.5 x 4.5"
    assert tile.label["align"] == "center"
    cx, baseline_y = tile.label["anchor"]
    assert cx == pytest.approx(tile.output_w_px / 2.0)
    assert 0 <= baseline_y <= tile.output_h_px


def test_plan_mosaic_label_absent_when_dims_missing():
    src = SporeMosaicSource(
        item_id=1,
        source_path=Path("/tmp/x.png"),
        source_width=600, source_height=600,
        p1_x=300, p1_y=350, p2_x=300, p2_y=250,
        p3_x=290, p3_y=300, p4_x=310, p4_y=300,
        length_um=10.0, width_um=None,
    )
    layout = plan_mosaic(
        [src], orient=True,
        grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
        annotation=MosaicAnnotationSpec(draw_dimensions=True),
    ).layout
    assert layout is not None
    tile = layout.cells[0].tile
    assert tile.label is None


def test_plan_mosaic_label_absent_when_annotation_flag_off():
    """When `draw_dimensions=False` (default for cloud), the plan carries
    no label — the semantic decision is centralised in the planner."""
    layout = plan_mosaic(
        [_source(1, length_um=10.0, width_um=4.0)],
        orient=True, grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
        annotation=MosaicAnnotationSpec(draw_dimensions=False),
    ).layout
    assert layout is not None
    assert layout.cells[0].tile.label is None


def test_plan_mosaic_polygon_absent_when_annotation_none():
    """`annotation=None` is the cloud path's default. Under Option A the
    planner does not attach polygon coords when no backend has asked
    for the rectangle. The raster path derives its own overlay polygon
    via `resolve_common_crop_placement`, so nothing downstream is lost."""
    layout = plan_mosaic(
        [_source(1)],
        orient=True, grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
        annotation=None,
    ).layout
    assert layout is not None
    assert layout.cells[0].tile.oriented_polygon_tile_local is None


def test_plan_mosaic_polygon_absent_when_draw_rectangle_flag_off():
    layout = plan_mosaic(
        [_source(1)],
        orient=True, grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
        annotation=MosaicAnnotationSpec(draw_rectangle=False),
    ).layout
    assert layout is not None
    assert layout.cells[0].tile.oriented_polygon_tile_local is None


def test_plan_mosaic_polygon_none_when_p3p4_missing():
    src = SporeMosaicSource(
        item_id=1,
        source_path=Path("/tmp/x.png"),
        source_width=600, source_height=600,
        p1_x=300, p1_y=350, p2_x=300, p2_y=250,
        p3_x=None, p3_y=None, p4_x=None, p4_y=None,
        length_um=10.0, width_um=4.0,
    )
    layout = plan_mosaic(
        [src], orient=True,
        grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
        annotation=MosaicAnnotationSpec(draw_rectangle=True),
    ).layout
    assert layout is not None
    tile = layout.cells[0].tile
    assert tile.oriented_polygon_tile_local is None


def test_plan_mosaic_polygon_stays_within_tile_bounds():
    layout = plan_mosaic(
        [_source(i + 1, source_size=(1200, 1200)) for i in range(5)],
        orient=True,
        grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
        annotation=MosaicAnnotationSpec(draw_rectangle=True),
    ).layout
    assert layout is not None
    for cell in layout.cells:
        tile = cell.tile
        assert tile.oriented_polygon_tile_local is not None
        for x, y in tile.oriented_polygon_tile_local:
            assert 0 <= x <= tile.output_w_px + 0.5
            assert 0 <= y <= tile.output_h_px + 0.5


def test_plan_mosaic_common_crop_matches_across_tiles():
    layout = plan_mosaic(
        [_source(1), _source(2), _source(3)],
        orient=True,
        grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=320,
    ).layout
    assert layout is not None
    tiles = [cell.tile for cell in layout.cells]
    common_w = {t.common_crop_width_um for t in tiles}
    common_h = {t.common_crop_height_um for t in tiles}
    assert len(common_w) == 1
    assert len(common_h) == 1
