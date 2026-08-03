"""Backend-agnostic planning core for spore mosaics.

Every mosaic pipeline (cloud WebP atlas, desktop live gallery preview,
desktop PNG/JPEG export, desktop hybrid SVG export) starts from the same
geometry: per-measurement rotation + crop plan, a single common physical
crop chosen across the observation, a grid shape driven by policy, and
per-tile placement + oriented polygon coordinates.

This module owns *planning* — it never opens a source image, never touches
Qt, and never produces pixels. It reuses the per-measurement math from
`utils.spore_thumbnail_render.plan_spore_thumbnail` and packages the
result into a `MosaicLayoutPlan` that all four output backends can consume.

Rendering stays in the backend that fits the output format:

* Cloud WebP atlas         — Pillow tiles, no baked annotations,
                             manifest overlays consumed by the landing site.
* Desktop live preview     — Qt tiles via `main_window.create_spore_thumbnail`.
* Desktop PNG / JPEG       — Qt tiles + `QPixmap.save`.
* Desktop hybrid SVG       — Pillow raster tiles embedded as base64
                             `<image>` + true vector `<polygon>` / `<text>`.

The one shared entry point is `plan_mosaic(sources, ...) -> MosaicLayoutPlan`.
Consumers never touch `select_grid_shape` or `plan_common_crop` directly.

Calibration resolution
----------------------
For every `SporeMosaicSource`, `plan_mosaic` derives µm-per-pixel in this
priority order:

1. **Authoritative image calibration.** If `scale_um_per_px` is set, use
   it for both axes (isotropic) and back-derive `length_um` / `width_um`
   from the pixel spans when the caller did not supply them.
2. **Endpoint-derived fallback.** Otherwise use `length_um` and
   `p1p2` pixel span to derive the length-axis scale; width-axis scale
   falls back to the length scale when `width_um` is absent (mirrors the
   existing `plan_spore_thumbnail` behaviour).
3. **Skip.** When neither is available, the tile is dropped and
   `MosaicLayoutPlan.skipped` records `(item_id, "missing_calibration")`.
   The planner never renders a tile at an unknown scale.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Sequence

from utils.spore_thumbnail_render import (
    DESKTOP_PADDING_X,
    DESKTOP_PADDING_Y,
    SporeThumbnailInputs,
    SporeThumbnailPlan,
    plan_common_crop,
    plan_spore_thumbnail,
    resolve_common_crop_placement,
)


# ── Neutral input model ─────────────────────────────────────────────────────


@dataclass(frozen=True)
class SporeMosaicSource:
    """One measurement's worth of input to any mosaic backend.

    All coordinates are in the source microscope image's native pixel
    space. No cloud IDs, no Qt objects, no PIL images — this is the
    portable geometry the shared planner needs and nothing more.
    Backends bind their own bookkeeping (cloud IDs, colour choices,
    tile ordering) around the `item_id`.

    Calibration: prefer `scale_um_per_px` (authoritative per-image µm
    per source pixel). Falls back to `length_um` + p1p2 pixel span if
    absent. Sources with neither are skipped by `plan_mosaic`.
    """

    item_id: int
    source_path: Path
    source_width: int
    source_height: int
    p1_x: float
    p1_y: float
    p2_x: float
    p2_y: float
    p3_x: float | None = None
    p3_y: float | None = None
    p4_x: float | None = None
    p4_y: float | None = None
    length_um: float | None = None
    width_um: float | None = None
    scale_um_per_px: float | None = None
    extra_rotation_deg: float = 0.0
    # Optional per-source overrides used by backends that draw
    # annotations (desktop export). Cloud path ignores these.
    annotation_colour_rgb: tuple[int, int, int] | None = None


# ── Annotation spec ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class MosaicAnnotationSpec:
    """Backend-agnostic description of what to draw on each tile.

    Cloud atlases pass `None` because landing renders overlays from the
    manifest. Desktop PNG/JPEG/SVG pass a populated spec to bake the
    rectangle + label onto the tile.
    """

    draw_rectangle: bool = False
    draw_dimensions: bool = False
    rectangle_style: str = "b"          # "a" | "b"
    rectangle_thickness: float = 2.0
    default_colour_rgb: tuple[int, int, int] = (0, 68, 170)
    selected_ids: frozenset[int] = frozenset()


class MosaicGridPolicy(str, Enum):
    """Preferred aspect ratio of the whole mosaic image."""

    SQUARE_IMAGE = "square_image"
    ASPECT_4_3 = "aspect_4_3"


# ── Per-tile + layout output ────────────────────────────────────────────────


@dataclass(frozen=True)
class MosaicTilePlan:
    """Everything the four backends need to render one tile identically.

    `oriented_polygon_tile_local` gives the measurement rectangle in the
    visible tile's pixel coordinate system, so vector SVG and raster
    backends can draw the same rectangle without re-deriving geometry.

    `label` carries the SEMANTIC placement of the dimensions label:
    `{text, anchor: (cx, baseline_y), align: "center"}`. Every backend
    positions the actual glyphs itself using its own font metrics —
    Qt via `QFontMetrics`, PIL via `ImageDraw.textbbox`, SVG via
    `text-anchor="middle"`. The neutral plan does not do exact-pixel
    layout of text.
    """

    source: SporeMosaicSource
    output_w_px: int
    output_h_px: int
    common_crop_width_um: float
    common_crop_height_um: float
    common_crop_width_px: int           # in oriented source pixels
    common_crop_height_px: int
    rotation_deg: float
    oriented_polygon_tile_local: list[tuple[float, float]] | None
    length_axis_px_per_um: float
    width_axis_px_per_um: float
    label: dict | None
    thumbnail_plan: SporeThumbnailPlan
    diagnostics: dict = field(default_factory=dict)


@dataclass(frozen=True)
class MosaicCell:
    """A tile placed inside the composite canvas."""

    tile: MosaicTilePlan
    x_px: int
    y_px: int
    w_px: int
    h_px: int


@dataclass
class MosaicLayoutPlan:
    """Final layout: every cell placed on a uniform grid + skipped items."""

    cells: list[MosaicCell]
    skipped: list[tuple[int, str]]      # (item_id, reason)
    mosaic_width_px: int
    mosaic_height_px: int
    cols: int
    rows: int
    tile_width_px: int
    tile_height_px: int
    common_crop_width_um: float
    common_crop_height_um: float


# ── Grid selection ──────────────────────────────────────────────────────────


# Weight for empty-cell penalty in `select_grid_shape`. A value of 1.5
# lets a grid with perfect aspect but a few empty slots beat a grid with
# 15–20% aspect error and no empty slots, while still rejecting grids
# that hoard empty cells to chase aspect (e.g. 5×3 when 12 tiles fit
# comfortably into 4×3).
GRID_EMPTY_FRACTION_PENALTY = 1.5


def select_grid_shape(
    count: int,
    cell_w: int,
    cell_h: int,
    target_aspect: float,
) -> tuple[int, int]:
    """Pick (cols, rows) so the mosaic image aspect nears `target_aspect`.

    Score = `abs(log(actual_aspect / target_aspect)) + K * empty_fraction`
    where `K = GRID_EMPTY_FRACTION_PENALTY`. Log aspect error makes
    doubling and halving score equally; the empty-fraction term stops
    the search from adding a full extra column just to chase aspect.
    Ties break on a closer-to-square grid.

    Callers assert what the rule says (lowest penalty wins), not that
    the atlas be exactly square — the aspect target itself is what
    steers geometry.
    """
    if count < 1:
        raise ValueError("count must be >= 1")
    if cell_w < 1 or cell_h < 1:
        raise ValueError("cell dimensions must be positive")
    target = float(target_aspect)
    if target <= 0:
        raise ValueError("target_aspect must be positive")

    log_target = math.log(target)
    best_key: tuple[float, int] | None = None
    best_cols = 1
    best_rows = count
    for cols in range(1, count + 1):
        rows = int(math.ceil(count / float(cols)))
        actual = (float(cols) * float(cell_w)) / (float(rows) * float(cell_h))
        err = abs(math.log(actual) - log_target)
        empties = cols * rows - count
        empty_frac = empties / float(cols * rows)
        penalty = err + GRID_EMPTY_FRACTION_PENALTY * empty_frac
        key = (penalty, abs(cols - rows))
        if best_key is None or key < best_key:
            best_key = key
            best_cols = cols
            best_rows = rows
    return best_cols, best_rows


def _target_aspect_for_policy(policy: MosaicGridPolicy) -> float:
    if policy is MosaicGridPolicy.ASPECT_4_3:
        return 4.0 / 3.0
    return 1.0


# ── Calibration resolution ──────────────────────────────────────────────────


def _resolve_calibration(
    src: SporeMosaicSource,
) -> tuple[float | None, float | None, str | None]:
    """Return (length_um, width_um, skip_reason).

    Follows the resolution order documented in the module docstring:
    explicit `scale_um_per_px` wins, else fall back to length_um +
    p1p2 pixel span, else the tile is skipped.
    """
    scale = src.scale_um_per_px
    line1_len_px = math.hypot(src.p2_x - src.p1_x, src.p2_y - src.p1_y)
    have_p34 = (
        src.p3_x is not None and src.p3_y is not None
        and src.p4_x is not None and src.p4_y is not None
    )
    line2_len_px = 0.0
    if have_p34:
        line2_len_px = math.hypot(
            float(src.p4_x) - float(src.p3_x),
            float(src.p4_y) - float(src.p3_y),
        )

    if scale is not None and scale > 0:
        if line1_len_px <= 0 and (
            src.length_um is None or src.length_um <= 0
        ):
            return None, None, "missing_calibration"
        length_um = (
            float(src.length_um)
            if src.length_um and src.length_um > 0
            else line1_len_px * scale
        )
        if src.width_um and src.width_um > 0:
            width_um = float(src.width_um)
        elif have_p34 and line2_len_px > 0:
            width_um = line2_len_px * scale
        else:
            width_um = None
        return length_um, width_um, None

    if src.length_um is not None and src.length_um > 0 and line1_len_px > 0:
        return float(src.length_um), (
            float(src.width_um) if src.width_um and src.width_um > 0 else None
        ), None

    return None, None, "missing_calibration"


# ── Label placement (semantic anchor only) ──────────────────────────────────


def _label_dict(
    src: SporeMosaicSource,
    length_um: float | None,
    width_um: float | None,
    *,
    output_width: int,
    output_height: int,
    margin: int = 4,
) -> dict | None:
    """Semantic label description; no font metrics involved.

    Every rendering backend positions the glyphs itself using its own
    font metrics — Qt via `QFontMetrics`, PIL via `ImageDraw.textbbox`,
    SVG via `text-anchor="middle"`. The plan just carries the intended
    text, the horizontal centre, and the baseline y.
    """
    _ = src  # kept in signature in case future backends want per-source label styling
    if length_um is None or width_um is None:
        return None
    try:
        text = f"{float(length_um):.1f} x {float(width_um):.1f}"
    except (TypeError, ValueError):
        return None
    safe_margin = max(4, int(margin))
    return {
        "text": text,
        "anchor": (float(output_width) / 2.0, float(output_height) - float(safe_margin)),
        "align": "center",
    }


# ── plan_mosaic entry point ─────────────────────────────────────────────────


def plan_mosaic(
    sources: Sequence[SporeMosaicSource],
    *,
    orient: bool,
    grid_policy: MosaicGridPolicy,
    output_tile_height_px: int,
    annotation: MosaicAnnotationSpec | None = None,
    background_rgb: tuple[int, int, int] = (18, 18, 22),
) -> MosaicLayoutPlan | None:
    """Plan a complete mosaic layout for one observation.

    * `orient` is threaded to `plan_spore_thumbnail`. `False` keeps the
      raw source orientation; `True` swings the length axis vertical.
    * `output_tile_height_px` fixes the visible tile height. Width is
      derived from the observation's common physical aspect ratio.
    * `annotation` is passed through so backends can decide whether to
      compute label anchors. Cloud path passes `None` and skips labels.
    * `grid_policy` selects between a near-square atlas and a 4:3
      composite; the aspect target drives `select_grid_shape`.

    Returns `None` when no source produces a usable tile (all skipped
    due to invalid dims, missing calibration, etc.).
    """
    if output_tile_height_px < 8:
        raise ValueError("output_tile_height_px too small")
    if not sources:
        return None

    _ = annotation  # only used by backend renderers; kept in signature
    # for symmetry and future planning (e.g. reserving label space).

    plans: list[tuple[SporeMosaicSource, SporeThumbnailPlan, float, float | None]] = []
    skipped: list[tuple[int, str]] = []
    for src in sources:
        if src.source_width < 1 or src.source_height < 1:
            skipped.append((src.item_id, "invalid source dims"))
            continue
        resolved_length_um, resolved_width_um, skip_reason = _resolve_calibration(src)
        if skip_reason is not None:
            skipped.append((src.item_id, skip_reason))
            continue
        inputs = SporeThumbnailInputs(
            p1_x=src.p1_x, p1_y=src.p1_y,
            p2_x=src.p2_x, p2_y=src.p2_y,
            p3_x=src.p3_x, p3_y=src.p3_y,
            p4_x=src.p4_x, p4_y=src.p4_y,
            orient=bool(orient),
            extra_rotation_deg=float(src.extra_rotation_deg or 0.0),
            padding_x_px=DESKTOP_PADDING_X,
            padding_y_px=DESKTOP_PADDING_Y,
            background_rgb=background_rgb,
            length_um=resolved_length_um,
            width_um=resolved_width_um,
        )
        try:
            plan = plan_spore_thumbnail(inputs, src.source_width, src.source_height)
        except Exception as exc:  # pragma: no cover — defensive
            skipped.append((src.item_id, f"plan failed: {exc}"))
            continue
        if (
            plan.length_axis_px_per_um is None
            or plan.width_axis_px_per_um is None
            or plan.natural_crop_width_um is None
            or plan.natural_crop_height_um is None
        ):
            skipped.append((src.item_id, "missing_calibration"))
            continue
        plans.append((src, plan, float(resolved_length_um), resolved_width_um))

    if not plans:
        return None

    crop_plan = plan_common_crop(
        [p for _s, p, _l, _w in plans], output_height_px=output_tile_height_px,
    )
    if crop_plan is None:
        return None

    common_w_um = crop_plan.common_crop_width_um
    common_h_um = crop_plan.common_crop_height_um
    out_w = crop_plan.output_tile_width
    out_h = crop_plan.output_tile_height

    # ── Tile plans ──────────────────────────────────────────────────────
    tile_plans: list[MosaicTilePlan] = []
    for src, plan, resolved_length_um, resolved_width_um in plans:
        length_scale = plan.length_axis_px_per_um or 0.0
        width_scale = plan.width_axis_px_per_um or 0.0
        crop_w_px = max(1, int(round(common_w_um * width_scale)))
        crop_h_px = max(1, int(round(common_h_um * length_scale)))
        # Delegate the shift + pad + polygon transform to the pure
        # resolver shared with `render_spore_thumbnail_common_crop` so
        # SVG and raster backends agree without divergent maths.
        placement = resolve_common_crop_placement(
            oriented_source_w=int(plan.oriented_width),
            oriented_source_h=int(plan.oriented_height),
            center_x=plan.center_x,
            center_y=plan.center_y,
            common_crop_width_px=crop_w_px,
            common_crop_height_px=crop_h_px,
            output_width=out_w,
            output_height=out_h,
        )
        polygon = placement.transform_polygon(plan.oriented_corners)
        label = _label_dict(
            src, resolved_length_um, resolved_width_um,
            output_width=out_w, output_height=out_h,
        )
        diagnostics = {
            "item_id": src.item_id,
            "rotation_deg": round(plan.rotation_deg, 3),
            "length_um": resolved_length_um,
            "width_um": resolved_width_um,
            "length_axis_px_per_um": (
                round(plan.length_axis_px_per_um, 4)
                if plan.length_axis_px_per_um is not None else None
            ),
            "width_axis_px_per_um": (
                round(plan.width_axis_px_per_um, 4)
                if plan.width_axis_px_per_um is not None else None
            ),
            "natural_crop_um": (
                round(plan.natural_crop_width_um or 0.0, 3),
                round(plan.natural_crop_height_um or 0.0, 3),
            ),
            "common_crop_um": (round(common_w_um, 3), round(common_h_um, 3)),
            "crop_px": (crop_w_px, crop_h_px),
            "output_tile": (out_w, out_h),
            "crop_rect_after_shift": (
                placement.crop_x_int, placement.crop_y_int,
                placement.common_crop_width_px, placement.common_crop_height_px,
            ),
            "padded_x": placement.padded_x,
            "padded_y": placement.padded_y,
            "polygon_present": polygon is not None,
            "reason_no_polygon": plan.reason_no_polygon,
            "scale_fallback_reason": plan.scale_fallback_reason,
        }
        tile_plans.append(MosaicTilePlan(
            source=src,
            output_w_px=out_w,
            output_h_px=out_h,
            common_crop_width_um=common_w_um,
            common_crop_height_um=common_h_um,
            common_crop_width_px=crop_w_px,
            common_crop_height_px=crop_h_px,
            rotation_deg=plan.rotation_deg,
            oriented_polygon_tile_local=polygon,
            length_axis_px_per_um=length_scale,
            width_axis_px_per_um=width_scale,
            label=label,
            thumbnail_plan=plan,
            diagnostics=diagnostics,
        ))

    # ── Layout ──────────────────────────────────────────────────────────
    target_aspect = _target_aspect_for_policy(grid_policy)
    cols, rows = select_grid_shape(len(tile_plans), out_w, out_h, target_aspect)
    mosaic_w = cols * out_w
    mosaic_h = rows * out_h
    cells: list[MosaicCell] = []
    for index, tp in enumerate(tile_plans):
        row = index // cols
        col = index % cols
        cells.append(MosaicCell(
            tile=tp,
            x_px=col * out_w,
            y_px=row * out_h,
            w_px=out_w,
            h_px=out_h,
        ))

    return MosaicLayoutPlan(
        cells=cells,
        skipped=skipped,
        mosaic_width_px=mosaic_w,
        mosaic_height_px=mosaic_h,
        cols=cols,
        rows=rows,
        tile_width_px=out_w,
        tile_height_px=out_h,
        common_crop_width_um=common_w_um,
        common_crop_height_um=common_h_um,
    )
