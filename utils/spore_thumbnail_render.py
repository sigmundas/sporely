"""Pure-PIL port of `main_window.create_spore_thumbnail` — the exact math
the desktop Analysis gallery uses to orient, crop, and outline a spore.

Extracted so cloud-side mosaic generation runs the SAME pipeline as the
desktop tile the user actually sees, instead of re-deriving it. When in
doubt about any behavior here, the source of truth is
`ui/main_window.py::create_spore_thumbnail` — the constants, the rotation
formula, and the corner-computation math are copied verbatim.

Two-phase interface:

* `plan_spore_thumbnail(inputs, source_width, source_height)` computes
  the oriented image size, the oriented measurement corners, the
  measurement centre, and the *natural* padded crop rectangle for one
  measurement — without opening the source image. Callers use these to
  pick a common crop size across an observation.
* `render_spore_thumbnail_common_crop(source, plan, common_crop_w,
  common_crop_h, output_w, output_h)` rotates the image, cuts a fixed
  `common_crop_w × common_crop_h` window centred on the measurement
  (edge-shifted to stay inside the source when possible), resizes to
  `output_w × output_h`, and transforms the polygon into the tile's
  local frame.

`render_spore_thumbnail` is the desktop-parity single-shot wrapper
(variable width, height-fixed) used by the desktop Analysis gallery and
by the older single-tile call path. Cloud mosaic generation now goes
through the two-phase interface so every tile in an observation shares
the same visible size.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Sequence

from PIL import Image


# Desktop constants — do not change without touching create_spore_thumbnail
# in main_window.py too. Values come straight from that function.
DESKTOP_PADDING_X = 20.0
DESKTOP_PADDING_Y = 15.0
DESKTOP_PADDING_Y_EXPORT = 10.0


@dataclass(frozen=True)
class SporeThumbnailInputs:
    """One measurement, in source-image pixel space.

    `length_um` / `width_um` are the physical measurements (µm). They are
    optional so the desktop single-shot path keeps working without them,
    but the cloud mosaic path uses them to plan a common physical crop
    frame — see `plan_spore_thumbnail`.
    """

    p1_x: float
    p1_y: float
    p2_x: float
    p2_y: float
    p3_x: float | None = None
    p3_y: float | None = None
    p4_x: float | None = None
    p4_y: float | None = None
    orient: bool = True
    extra_rotation_deg: float = 0.0
    padding_x_px: float = DESKTOP_PADDING_X
    padding_y_px: float = DESKTOP_PADDING_Y
    background_rgb: tuple[int, int, int] = (18, 18, 22)
    length_um: float | None = None
    width_um: float | None = None


@dataclass(frozen=True)
class SporeThumbnailPlan:
    """Per-measurement geometry needed to pick a common crop size.

    All coordinates in `oriented_*` and `natural_*` fields are in the
    *oriented* image frame (after the QT-parity rotation).

    `length_axis_px_per_um` / `width_axis_px_per_um` are derived per
    measurement from the pixel distance between p1/p2 (length) and
    p3/p4 (width) divided by the user's physical measurement in µm.
    Cross-image scale variance is why the mosaic pipeline needs a
    common *physical* crop rather than a common pixel crop.
    """

    inputs: SporeThumbnailInputs
    source_width: int
    source_height: int
    rotation_deg: float
    oriented_width: int
    oriented_height: int
    oriented_corners: list[tuple[float, float]] | None
    oriented_p1: tuple[float, float]
    oriented_p2: tuple[float, float]
    center_x: float
    center_y: float
    natural_crop_width: float
    natural_crop_height: float
    reason_no_polygon: str | None
    length_axis_px: float = 0.0
    width_axis_px: float = 0.0
    length_axis_px_per_um: float | None = None
    width_axis_px_per_um: float | None = None
    natural_crop_width_um: float | None = None
    natural_crop_height_um: float | None = None
    scale_fallback_reason: str | None = None


@dataclass(frozen=True)
class SporeThumbnailRenderResult:
    """Output of `render_spore_thumbnail`.

    `image` is `tile_width_px × tile_height_px` — height matches the
    requested `height_px`, width follows crop aspect (identical to the
    desktop `size × ~aspect` shape). `polygon_tile_local` is the
    measurement rectangle in the tile's pixel coordinates. It's `None`
    when p3/p4 are missing — we do NOT synthesise a rectangle in that
    case (see `reason_no_polygon`).
    """

    image: Image.Image
    tile_width_px: int
    tile_height_px: int
    polygon_tile_local: list[tuple[float, float]] | None
    crop_rect_source_pixels: tuple[int, int, int, int]
    rotation_deg: float
    reason_no_polygon: str | None


@dataclass(frozen=True)
class SporeThumbnailCommonCropResult:
    """Output of `render_spore_thumbnail_common_crop`.

    `image` is exactly `output_width × output_height` for every tile in
    the observation. `padded_x`/`padded_y` are True when the crop needed
    background fill on that axis (source smaller than the common crop);
    they should be rare because the common crop is chosen from the
    widest natural crop actually seen.
    """

    image: Image.Image
    output_width: int
    output_height: int
    polygon_tile_local: list[tuple[float, float]] | None
    crop_rect_before_shift: tuple[float, float, float, float]
    crop_rect_after_shift: tuple[int, int, int, int]
    padded_x: bool
    padded_y: bool
    reason_no_polygon: str | None


@dataclass(frozen=True)
class MosaicCropPlan:
    """Common visible tile geometry for an observation.

    Fields track the *physical* crop. Per-tile pixel dims are derived at
    render time from each measurement's own `px_per_um`.

    Lives here in `spore_thumbnail_render` — not in `spore_mosaic_render`
    — so backends that only pull single-tile helpers do not drag the
    mosaic-planning module in. `spore_mosaic_render` imports this
    upward; the reverse would introduce a circular dependency.
    """

    common_crop_width_um: float
    common_crop_height_um: float
    output_tile_width: int
    output_tile_height: int
    common_crop_width: int = 0   # legacy: representative pixel width
    common_crop_height: int = 0  # legacy: representative pixel height


def plan_common_crop(
    plans: "Sequence[SporeThumbnailPlan]",
    output_height_px: int,
) -> "MosaicCropPlan | None":
    """Pick a common crop size in µm from per-measurement natural crops.

    The plan is expressed in **micrometres**: the widest / tallest
    natural crop across all measurements, in µm, becomes the common
    physical crop for the observation. The visible tile height is
    fixed to `output_height_px`; the visible tile width follows the
    common physical aspect ratio. That guarantees a consistent
    µm-per-output-pixel mapping across every tile in the observation,
    even when the underlying microscope frames have different
    px-per-µm.

    Plans without a valid physical scale (missing length_um or a zero
    length axis) are silently ignored — the caller filters them.
    """
    if not plans:
        return None
    scaled = [
        p for p in plans
        if p.natural_crop_width_um and p.natural_crop_height_um
        and p.natural_crop_width_um > 0 and p.natural_crop_height_um > 0
    ]
    if not scaled:
        return None
    if output_height_px < 8:
        raise ValueError("output_height_px too small")

    common_w_um = max(p.natural_crop_width_um for p in scaled)
    common_h_um = max(p.natural_crop_height_um for p in scaled)
    out_h = int(output_height_px)
    out_w = max(1, int(round(common_w_um * out_h / common_h_um)))

    # Representative pixel dims (rounded off the widest measurement).
    # Kept only for backwards-compatible diagnostics; the per-tile
    # pixel crop is recomputed from each plan's own scale.
    rep_plan = max(scaled, key=lambda p: p.natural_crop_height_um)
    rep_w_px = max(1, int(math.ceil(common_w_um * (rep_plan.width_axis_px_per_um or 0))))
    rep_h_px = max(1, int(math.ceil(common_h_um * (rep_plan.length_axis_px_per_um or 0))))

    return MosaicCropPlan(
        common_crop_width_um=float(common_w_um),
        common_crop_height_um=float(common_h_um),
        output_tile_width=out_w,
        output_tile_height=out_h,
        common_crop_width=rep_w_px,
        common_crop_height=rep_h_px,
    )


# ── Rotation math (matches Qt) ──────────────────────────────────────────────


def rotate_point_qt(x: float, y: float, cx: float, cy: float, angle_deg: float) -> tuple[float, float]:
    """Rotate a point using Qt QTransform.rotate(angle_deg) semantics.

    Standard math rotation matrix around (cx, cy). In Y-down screen space
    a positive angle appears visually clockwise; a negative angle appears
    counter-clockwise. This is exactly what `transform.map(pt)` produces
    in Qt's Y-down QTransform.
    """
    a = math.radians(angle_deg)
    cos_a = math.cos(a)
    sin_a = math.sin(a)
    dx = x - cx
    dy = y - cy
    return cx + dx * cos_a - dy * sin_a, cy + dx * sin_a + dy * cos_a


def _rotated_source_offset(src_w: int, src_h: int, angle_deg: float) -> tuple[float, float]:
    """Offset that maps `rotate_point_qt(sp, W/2, H/2, angle)` into the
    rotated PIL image's coordinate frame.

    Mirrors Qt's:

        src_rect = transform.mapRect(QRectF(0, 0, W, H))
        rotated_points = [p + QPointF(-src_rect.x(), -src_rect.y()) …]
    """
    cx = src_w / 2.0
    cy = src_h / 2.0
    corners = [
        rotate_point_qt(0.0, 0.0, cx, cy, angle_deg),
        rotate_point_qt(float(src_w), 0.0, cx, cy, angle_deg),
        rotate_point_qt(float(src_w), float(src_h), cx, cy, angle_deg),
        rotate_point_qt(0.0, float(src_h), cx, cy, angle_deg),
    ]
    min_x = min(c[0] for c in corners)
    min_y = min(c[1] for c in corners)
    return -min_x, -min_y


def _rotated_bounding_box(src_w: int, src_h: int, angle_deg: float) -> tuple[int, int]:
    """Return the bounding-box size of an image after Qt-style rotation.

    Matches PIL's `img.rotate(-angle, expand=True)` output size.
    """
    cx = src_w / 2.0
    cy = src_h / 2.0
    corners = [
        rotate_point_qt(0.0, 0.0, cx, cy, angle_deg),
        rotate_point_qt(float(src_w), 0.0, cx, cy, angle_deg),
        rotate_point_qt(float(src_w), float(src_h), cx, cy, angle_deg),
        rotate_point_qt(0.0, float(src_h), cx, cy, angle_deg),
    ]
    xs = [c[0] for c in corners]
    ys = [c[1] for c in corners]
    width = int(math.ceil(max(xs) - min(xs)))
    height = int(math.ceil(max(ys) - min(ys)))
    return max(1, width), max(1, height)


def _rotate_pil_qt_style(
    img: Image.Image,
    angle_deg: float,
    background_rgb: tuple[int, int, int],
) -> Image.Image:
    """Rotate a PIL image so it lines up with `rotate_point_qt(…, angle_deg)`.

    Qt QTransform.rotate(A) applied to a point uses the math CCW rotation
    matrix. PIL `Image.rotate(A)` rotates the image visually CCW by A —
    which in Y-down screen space is the OPPOSITE direction to what the
    math matrix produces. So to get pixels lined up with points that were
    rotated via `rotate_point_qt(A)`, we call PIL with `-A`.
    """
    return img.rotate(
        -float(angle_deg),
        resample=Image.BILINEAR,
        expand=True,
        fillcolor=background_rgb,
    )


# ── Utilities ───────────────────────────────────────────────────────────────


def _to_rgb(img: Image.Image, background_rgb: tuple[int, int, int]) -> Image.Image:
    if img.mode == "RGB":
        return img
    if img.mode == "RGBA":
        flat = Image.new("RGB", img.size, background_rgb)
        flat.paste(img, mask=img.split()[3])
        return flat
    return img.convert("RGB")


def _compute_rotation_and_axes(
    inputs: SporeThumbnailInputs,
) -> tuple[float, tuple[float, float], tuple[float, float]]:
    """Return (rotation_deg, unit_length_axis, unit_width_axis) for the
    orient direction. Small `line1_len == 0` case returns 0-length axes;
    callers must handle it as a `zero_length_axis` degenerate."""
    line1_vx = inputs.p2_x - inputs.p1_x
    line1_vy = inputs.p2_y - inputs.p1_y
    line1_len = math.hypot(line1_vx, line1_vy)
    rotation_angle = float(inputs.extra_rotation_deg or 0.0)
    if inputs.orient and line1_len > 0:
        current_angle = math.atan2(line1_vx, -line1_vy)
        rotation_angle += -math.degrees(current_angle)
    return rotation_angle, (line1_vx, line1_vy), (line1_len, 0.0)


def _map_point_to_oriented(
    x: float, y: float,
    src_w: int, src_h: int,
    rotation_angle: float,
    offset: tuple[float, float],
) -> tuple[float, float]:
    rx, ry = rotate_point_qt(x, y, src_w / 2.0, src_h / 2.0, rotation_angle)
    return rx + offset[0], ry + offset[1]


# ── Plan phase ──────────────────────────────────────────────────────────────


def plan_spore_thumbnail(
    inputs: SporeThumbnailInputs,
    source_width: int,
    source_height: int,
) -> SporeThumbnailPlan:
    """Compute oriented geometry + natural padded crop without opening the source.

    The returned plan tells the caller:
    * the size of the rotated image (needed to bound the common crop),
    * the measurement centre in the oriented frame (the crop is centred here),
    * the oriented rectangle corners (or None + reason_no_polygon), and
    * the natural width/height of the padded crop for this measurement.

    Callers combine the natural dims across the observation to pick a
    single common crop size that fits the widest and tallest measurement.
    """
    if source_width < 1 or source_height < 1:
        raise ValueError("source_width and source_height must be positive")

    src_w = int(source_width)
    src_h = int(source_height)

    p1x, p1y = float(inputs.p1_x), float(inputs.p1_y)
    p2x, p2y = float(inputs.p2_x), float(inputs.p2_y)
    have_p34 = (
        inputs.p3_x is not None and inputs.p3_y is not None
        and inputs.p4_x is not None and inputs.p4_y is not None
    )
    p3x = float(inputs.p3_x) if have_p34 else 0.0
    p3y = float(inputs.p3_y) if have_p34 else 0.0
    p4x = float(inputs.p4_x) if have_p34 else 0.0
    p4y = float(inputs.p4_y) if have_p34 else 0.0

    line1_vx = p2x - p1x
    line1_vy = p2y - p1y
    line1_len = math.hypot(line1_vx, line1_vy)

    rotation_angle = float(inputs.extra_rotation_deg or 0.0)
    if inputs.orient and line1_len > 0:
        current_angle = math.atan2(line1_vx, -line1_vy)
        rotation_angle += -math.degrees(current_angle)

    if abs(rotation_angle) > 0.1:
        offset = _rotated_source_offset(src_w, src_h, rotation_angle)
        oriented_w, oriented_h = _rotated_bounding_box(src_w, src_h, rotation_angle)
        p1x_o, p1y_o = _map_point_to_oriented(p1x, p1y, src_w, src_h, rotation_angle, offset)
        p2x_o, p2y_o = _map_point_to_oriented(p2x, p2y, src_w, src_h, rotation_angle, offset)
        if have_p34:
            p3x_o, p3y_o = _map_point_to_oriented(p3x, p3y, src_w, src_h, rotation_angle, offset)
            p4x_o, p4y_o = _map_point_to_oriented(p4x, p4y, src_w, src_h, rotation_angle, offset)
        else:
            p3x_o = p3y_o = p4x_o = p4y_o = 0.0
    else:
        rotation_angle = 0.0
        oriented_w, oriented_h = src_w, src_h
        p1x_o, p1y_o = p1x, p1y
        p2x_o, p2y_o = p2x, p2y
        p3x_o, p3y_o = p3x, p3y
        p4x_o, p4y_o = p4x, p4y

    # Recompute in oriented frame.
    line1_vx = p2x_o - p1x_o
    line1_vy = p2y_o - p1y_o
    line1_len = math.hypot(line1_vx, line1_vy)

    reason_no_polygon: str | None = None
    corners_oriented: list[tuple[float, float]] | None = None
    if line1_len <= 0:
        reason_no_polygon = "zero_length_axis"
    elif not have_p34:
        reason_no_polygon = "missing_p3p4"
    else:
        line2_vx = p4x_o - p3x_o
        line2_vy = p4y_o - p3y_o
        width_px = math.hypot(line2_vx, line2_vy)
        if width_px <= 0:
            reason_no_polygon = "zero_width_axis"
        else:
            center_x = ((p1x_o + p2x_o) * 0.5 + (p3x_o + p4x_o) * 0.5) * 0.5
            center_y = ((p1y_o + p2y_o) * 0.5 + (p3y_o + p4y_o) * 0.5) * 0.5
            axis_length_x = -line1_vx / line1_len
            axis_length_y = -line1_vy / line1_len
            axis_width_x = -axis_length_y
            axis_width_y = axis_length_x
            hl = line1_len * 0.5
            hw = width_px * 0.5
            corners_oriented = [
                (center_x + axis_width_x * (-hw) + axis_length_x * (-hl),
                 center_y + axis_width_y * (-hw) + axis_length_y * (-hl)),
                (center_x + axis_width_x * (hw) + axis_length_x * (-hl),
                 center_y + axis_width_y * (hw) + axis_length_y * (-hl)),
                (center_x + axis_width_x * (hw) + axis_length_x * (hl),
                 center_y + axis_width_y * (hw) + axis_length_y * (hl)),
                (center_x + axis_width_x * (-hw) + axis_length_x * (hl),
                 center_y + axis_width_y * (-hw) + axis_length_y * (hl)),
            ]

    if corners_oriented is not None:
        source_points: Sequence[tuple[float, float]] = corners_oriented
    else:
        source_points = [(p1x_o, p1y_o), (p2x_o, p2y_o)]

    min_x = min(p[0] for p in source_points) - inputs.padding_x_px
    max_x = max(p[0] for p in source_points) + inputs.padding_x_px
    min_y = min(p[1] for p in source_points) - inputs.padding_y_px
    max_y = max(p[1] for p in source_points) + inputs.padding_y_px

    natural_w = max_x - min_x
    natural_h = max_y - min_y
    center_x = (min_x + max_x) * 0.5
    center_y = (min_y + max_y) * 0.5

    # Physical-scale planning. Distances are rotation-invariant, so we
    # can compute px_per_um from the *unoriented* points (line1_len /
    # line2_len were preserved through rotation, but working directly
    # from the unoriented axis lengths avoids any ambiguity if the
    # caller passed unoriented p1/p2 with a non-zero orient rotation
    # and the caller expected the raw axis measurements).
    length_axis_px = math.hypot(
        inputs.p2_x - inputs.p1_x, inputs.p2_y - inputs.p1_y,
    )
    if have_p34:
        width_axis_px = math.hypot(
            float(inputs.p4_x) - float(inputs.p3_x),
            float(inputs.p4_y) - float(inputs.p3_y),
        )
    else:
        width_axis_px = 0.0

    length_um = inputs.length_um if inputs.length_um and inputs.length_um > 0 else None
    width_um = inputs.width_um if inputs.width_um and inputs.width_um > 0 else None

    length_axis_px_per_um: float | None = None
    if length_um is not None and length_axis_px > 0:
        length_axis_px_per_um = length_axis_px / length_um

    width_axis_px_per_um: float | None = None
    scale_fallback_reason: str | None = None
    if width_um is not None and width_axis_px > 0:
        width_axis_px_per_um = width_axis_px / width_um
    elif length_axis_px_per_um is not None:
        width_axis_px_per_um = length_axis_px_per_um
        if width_um is None:
            scale_fallback_reason = "width_um_missing_use_length_scale"
        elif width_axis_px <= 0:
            scale_fallback_reason = "width_axis_px_missing_use_length_scale"

    natural_crop_width_um: float | None = None
    natural_crop_height_um: float | None = None
    if length_um is not None and length_axis_px_per_um and length_axis_px_per_um > 0:
        natural_crop_height_um = length_um + 2.0 * (
            inputs.padding_y_px / length_axis_px_per_um
        )
    if width_axis_px_per_um and width_axis_px_per_um > 0:
        # Prefer the measured width if we have it, otherwise fall back
        # to the width axis's pixel span (which is 0 without p3/p4 —
        # tile then relies on the length-derived width padding alone).
        effective_width_um = width_um if width_um is not None else 0.0
        natural_crop_width_um = effective_width_um + 2.0 * (
            inputs.padding_x_px / width_axis_px_per_um
        )

    return SporeThumbnailPlan(
        inputs=inputs,
        source_width=src_w,
        source_height=src_h,
        rotation_deg=rotation_angle,
        oriented_width=oriented_w,
        oriented_height=oriented_h,
        oriented_corners=corners_oriented,
        oriented_p1=(p1x_o, p1y_o),
        oriented_p2=(p2x_o, p2y_o),
        center_x=center_x,
        center_y=center_y,
        natural_crop_width=natural_w,
        natural_crop_height=natural_h,
        reason_no_polygon=reason_no_polygon,
        length_axis_px=length_axis_px,
        width_axis_px=width_axis_px,
        length_axis_px_per_um=length_axis_px_per_um,
        width_axis_px_per_um=width_axis_px_per_um,
        natural_crop_width_um=natural_crop_width_um,
        natural_crop_height_um=natural_crop_height_um,
        scale_fallback_reason=scale_fallback_reason,
    )


# ── Common-crop placement (pure, backend-neutral) ──────────────────────────


@dataclass(frozen=True)
class CommonCropPlacement:
    """Pure geometric plan for placing a common-sized crop onto a tile.

    Every backend that draws or plans a mosaic tile — Pillow raster,
    Qt raster, vector SVG — consumes this from the same helper so no
    axis-shift, pad or scale maths lives in more than one place.

    Fields:
        crop_x_int / crop_y_int
            Integer crop origin inside the oriented source. When the
            source is smaller than the requested crop on an axis, the
            origin snaps to 0 for that axis and `padded_*` reports
            that condition.
        common_crop_width_px / common_crop_height_px
            The requested crop size in oriented source pixels — echoed
            for callers that only capture the placement.
        padded_x / padded_y
            True when the requested crop overflows the source on that
            axis. Renderers must fill background pixels behind the
            source when so.
        paste_dx / paste_dy
            Where the source's (0, 0) lands in the crop canvas's
            coordinate frame. Renderers use this to composite the
            source onto a background-coloured canvas of size
            `common_crop_width_px × common_crop_height_px`. Any
            oriented-source point (x, y) maps to `(x + paste_dx,
            y + paste_dy)` on the canvas.
        scale_x / scale_y
            Crop canvas → visible tile scale factor.
    """

    crop_x_int: int
    crop_y_int: int
    common_crop_width_px: int
    common_crop_height_px: int
    padded_x: bool
    padded_y: bool
    paste_dx: int
    paste_dy: int
    scale_x: float
    scale_y: float

    def source_to_tile(self, x: float, y: float) -> tuple[float, float]:
        """Map an oriented source point to tile-local pixel coords."""
        return (
            (x + self.paste_dx) * self.scale_x,
            (y + self.paste_dy) * self.scale_y,
        )

    def transform_polygon(
        self,
        corners: Sequence[tuple[float, float]] | None,
    ) -> list[tuple[float, float]] | None:
        """Transform an oriented-source polygon into tile-local coords."""
        if corners is None:
            return None
        return [self.source_to_tile(x, y) for x, y in corners]


def resolve_common_crop_placement(
    oriented_source_w: int,
    oriented_source_h: int,
    center_x: float,
    center_y: float,
    common_crop_width_px: int,
    common_crop_height_px: int,
    output_width: int,
    output_height: int,
) -> CommonCropPlacement:
    """Pure resolver for the common-crop placement.

    Both the Pillow raster path (`render_spore_thumbnail_common_crop`)
    and the Qt raster path (`main_window.create_spore_thumbnail`'s plan
    branch) delegate here so they never diverge on shift / pad / scale
    math. Vector SVG uses the same result to place polygon and label
    coordinates identically to the raster.

    Semantics
    ---------
    * The measurement centre lands at the tile centre whenever
      geometrically possible.
    * When the requested crop overflows the source on an axis
      (``padded_x`` / ``padded_y``), the source is placed inside the
      canvas so the measurement stays centred; the source is then
      clamped to keep it fully inside the canvas (no wrap-around, no
      clipping). Background pixels fill the remainder on both sides as
      the geometry dictates.
    * When the requested crop fits the source, the crop origin is the
      measurement's own centre minus half the crop dims, edge-shifted
      to keep the crop inside the source.
    """
    if oriented_source_w < 1 or oriented_source_h < 1:
        raise ValueError("oriented source dims must be positive")
    if common_crop_width_px < 1 or common_crop_height_px < 1:
        raise ValueError("common crop dims must be positive")
    if output_width < 1 or output_height < 1:
        raise ValueError("output dims must be positive")

    crop_x_ideal = float(center_x) - float(common_crop_width_px) / 2.0
    crop_y_ideal = float(center_y) - float(common_crop_height_px) / 2.0

    padded_x = int(oriented_source_w) < int(common_crop_width_px)
    padded_y = int(oriented_source_h) < int(common_crop_height_px)

    if padded_x:
        crop_x_shifted = 0.0
    else:
        crop_x_shifted = max(
            0.0, min(crop_x_ideal, float(oriented_source_w - common_crop_width_px)),
        )
    if padded_y:
        crop_y_shifted = 0.0
    else:
        crop_y_shifted = max(
            0.0, min(crop_y_ideal, float(oriented_source_h - common_crop_height_px)),
        )

    crop_x_int = int(round(crop_x_shifted))
    crop_y_int = int(round(crop_y_shifted))

    if padded_x:
        # Ideal placement lands the measurement centre at the canvas
        # centre: measurement_centre_on_canvas = center_x + paste_dx
        # → paste_dx = canvas_centre - center_x. Clamp so the source
        # stays fully inside the canvas ([0, canvas_w - source_w]).
        ideal_paste_dx = (
            float(common_crop_width_px) / 2.0 - float(center_x)
        )
        max_paste_dx = float(int(common_crop_width_px) - int(oriented_source_w))
        paste_dx = int(round(max(0.0, min(ideal_paste_dx, max_paste_dx))))
    else:
        paste_dx = -crop_x_int
    if padded_y:
        ideal_paste_dy = (
            float(common_crop_height_px) / 2.0 - float(center_y)
        )
        max_paste_dy = float(int(common_crop_height_px) - int(oriented_source_h))
        paste_dy = int(round(max(0.0, min(ideal_paste_dy, max_paste_dy))))
    else:
        paste_dy = -crop_y_int

    scale_x = float(output_width) / float(common_crop_width_px)
    scale_y = float(output_height) / float(common_crop_height_px)

    return CommonCropPlacement(
        crop_x_int=crop_x_int,
        crop_y_int=crop_y_int,
        common_crop_width_px=int(common_crop_width_px),
        common_crop_height_px=int(common_crop_height_px),
        padded_x=padded_x,
        padded_y=padded_y,
        paste_dx=int(paste_dx),
        paste_dy=int(paste_dy),
        scale_x=scale_x,
        scale_y=scale_y,
    )


# ── Common-crop render ─────────────────────────────────────────────────────


def _rotate_source_if_needed(
    source: Image.Image,
    rotation_deg: float,
    background_rgb: tuple[int, int, int],
) -> Image.Image:
    if abs(rotation_deg) <= 0.1:
        return _to_rgb(source, background_rgb)
    rotated = _rotate_pil_qt_style(source, rotation_deg, background_rgb)
    return _to_rgb(rotated, background_rgb)


# ── Fast local-ROI renderer + reference (Phase 2.B) ────────────────────────
#
# The reference implementation (`reference_render_tile`) rotates the
# whole source before cropping — accurate but wasteful for large source
# frames because every pixel is resampled even though only the crop
# region ends up in the final tile.
#
# The fast renderer (`fast_render_tile`) collapses rotate + crop + resize
# into one inverse affine sampled directly at the output resolution. It
# consumes the same `SporeThumbnailPlan` + `CommonCropPlacement`, so it
# is guaranteed to land on the exact same tile-local polygon geometry
# as the reference path — parity tests assert an image-difference
# threshold and polygon delta.
#
# The public entry `render_spore_thumbnail_common_crop` picks between
# the two based on `SPORELY_MOSAIC_USE_REFERENCE`: any non-empty value
# forces the reference path so a user can bisect a suspected fast-path
# regression without changing code.


def _use_reference_renderer() -> bool:
    raw = os.environ.get("SPORELY_MOSAIC_USE_REFERENCE", "").strip().lower()
    return raw not in ("", "0", "false", "no")


def _run_placement(
    plan: SporeThumbnailPlan,
    *,
    common_crop_width: int,
    common_crop_height: int,
    output_width: int,
    output_height: int,
    oriented_width: int,
    oriented_height: int,
) -> CommonCropPlacement:
    return resolve_common_crop_placement(
        oriented_source_w=int(oriented_width),
        oriented_source_h=int(oriented_height),
        center_x=plan.center_x,
        center_y=plan.center_y,
        common_crop_width_px=int(common_crop_width),
        common_crop_height_px=int(common_crop_height),
        output_width=int(output_width),
        output_height=int(output_height),
    )


def reference_render_tile(
    source: Image.Image,
    plan: SporeThumbnailPlan,
    *,
    common_crop_width: int,
    common_crop_height: int,
    output_width: int,
    output_height: int,
) -> SporeThumbnailCommonCropResult:
    """Reference path: rotate whole source → crop → resize.

    Kept as the ground truth against which the fast local-ROI renderer
    is parity-tested. Prefer `render_spore_thumbnail_common_crop`
    (which dispatches by env flag) in production code.
    """
    if common_crop_width < 1 or common_crop_height < 1:
        raise ValueError("common crop dimensions must be positive")
    if output_width < 1 or output_height < 1:
        raise ValueError("output dimensions must be positive")

    background_rgb = plan.inputs.background_rgb
    oriented = _rotate_source_if_needed(source, plan.rotation_deg, background_rgb)
    working_w, working_h = oriented.size

    crop_rect_before = (
        plan.center_x - common_crop_width / 2.0,
        plan.center_y - common_crop_height / 2.0,
        float(common_crop_width),
        float(common_crop_height),
    )

    placement = _run_placement(
        plan,
        common_crop_width=common_crop_width,
        common_crop_height=common_crop_height,
        output_width=output_width,
        output_height=output_height,
        oriented_width=working_w,
        oriented_height=working_h,
    )
    padded_x = placement.padded_x
    padded_y = placement.padded_y
    crop_x_int = placement.crop_x_int
    crop_y_int = placement.crop_y_int

    if not padded_x and not padded_y:
        canvas = oriented.crop((
            crop_x_int, crop_y_int,
            crop_x_int + common_crop_width, crop_y_int + common_crop_height,
        ))
    else:
        canvas = Image.new(
            "RGB", (common_crop_width, common_crop_height), background_rgb,
        )
        canvas.paste(oriented, (placement.paste_dx, placement.paste_dy))

    crop_rect_after = (crop_x_int, crop_y_int, common_crop_width, common_crop_height)

    if (common_crop_width, common_crop_height) != (output_width, output_height):
        canvas = canvas.resize((output_width, output_height), Image.LANCZOS)

    polygon_tile_local = placement.transform_polygon(plan.oriented_corners)

    return SporeThumbnailCommonCropResult(
        image=canvas,
        output_width=output_width,
        output_height=output_height,
        polygon_tile_local=polygon_tile_local,
        crop_rect_before_shift=crop_rect_before,
        crop_rect_after_shift=crop_rect_after,
        padded_x=padded_x,
        padded_y=padded_y,
        reason_no_polygon=plan.reason_no_polygon,
    )


def _compute_tile_to_source_affine(
    plan: SporeThumbnailPlan,
    placement: CommonCropPlacement,
    source_w: int,
    source_h: int,
) -> tuple[float, float, float, float, float, float, tuple[float, float]]:
    """Return the affine coefficients (a, b, c, d, e, f) that map an
    output-tile pixel back to the source-image pixel, plus the
    ``(offset_x, offset_y)`` of `_rotated_source_offset` used to build
    the oriented frame.

    Forward chain (source → tile):
        rot_pt   = rotate_qt(src, cx, cy, angle)
        ori_pt   = rot_pt + rotated_source_offset
        tile_pt  = (ori_pt + (paste_dx, paste_dy)) * (scale_x, scale_y)

    The inverse walks tile → oriented → rotated → source and inlines
    the shared placement / scale constants so the whole thing is a
    single 2×3 affine. See the module docstring for the derivation.
    """
    rotation_deg = plan.rotation_deg
    if abs(rotation_deg) <= 0.1:
        # rotation ~ 0: offset (0, 0), cos=1, sin=0. Directly
        # sx = tx / scale_x - paste_dx, sy = ty / scale_y - paste_dy.
        a = 1.0 / placement.scale_x
        b = 0.0
        c = -float(placement.paste_dx)
        d = 0.0
        e = 1.0 / placement.scale_y
        f = -float(placement.paste_dy)
        return a, b, c, d, e, f, (0.0, 0.0)

    ang = math.radians(rotation_deg)
    cos_a = math.cos(ang)
    sin_a = math.sin(ang)
    offset = _rotated_source_offset(source_w, source_h, rotation_deg)
    cx = float(source_w) / 2.0
    cy = float(source_h) / 2.0
    kx = -float(placement.paste_dx) - offset[0]
    ky = -float(placement.paste_dy) - offset[1]
    inv_sx = 1.0 / placement.scale_x
    inv_sy = 1.0 / placement.scale_y

    a = cos_a * inv_sx
    b = sin_a * inv_sy
    c = cx + (kx - cx) * cos_a + (ky - cy) * sin_a
    d = -sin_a * inv_sx
    e = cos_a * inv_sy
    f = cy - (kx - cx) * sin_a + (ky - cy) * cos_a
    return a, b, c, d, e, f, offset


# Extra source pixels kept around the ROI to let the small-image
# rotation resampler blend without edge artefacts. 4 px is enough for
# BILINEAR sampling. Bigger sources with high rotation angles benefit
# from a slightly larger margin — kept conservative.
_FAST_ROI_MARGIN_PX = 8


def fast_render_tile(
    source: Image.Image,
    plan: SporeThumbnailPlan,
    *,
    common_crop_width: int,
    common_crop_height: int,
    output_width: int,
    output_height: int,
) -> SporeThumbnailCommonCropResult:
    """Local-ROI renderer: inverse-map the crop back into source coords,
    crop that small ROI, then rotate + resize only those pixels.

    Contract match with the reference path
    ---------------------------------------
    Uses the same filter combo the reference does — BILINEAR for the
    rotate step and LANCZOS for the resize — so per-pixel diff stays
    inside the documented parity budget (mean < 3.0, max < 15 out of
    255) even on high-frequency test patterns. What changes is the
    working image size: instead of rotating the whole source (up to
    hundreds of MPix), we rotate a ROI a few tens of KPix wide.

    Fallback: for very small sources (comparable to the crop size) the
    ROI approach costs almost as much as the reference. We degrade
    gracefully to the reference path in that case — it stays fast on
    small inputs while the ROI path pays only for what it actually
    saves.
    """
    if common_crop_width < 1 or common_crop_height < 1:
        raise ValueError("common crop dimensions must be positive")
    if output_width < 1 or output_height < 1:
        raise ValueError("output dimensions must be positive")

    background_rgb = plan.inputs.background_rgb
    working = _to_rgb(source, background_rgb)
    src_w, src_h = working.size

    # ROI approach only wins when the source is meaningfully larger than
    # the crop.  For small sources fall back to the reference path so
    # we don't add overhead without benefit.
    oriented_w, oriented_h = _rotated_bounding_box(src_w, src_h, plan.rotation_deg)
    if oriented_w <= 3 * common_crop_width and oriented_h <= 3 * common_crop_height:
        return reference_render_tile(
            source, plan,
            common_crop_width=common_crop_width,
            common_crop_height=common_crop_height,
            output_width=output_width,
            output_height=output_height,
        )

    placement = _run_placement(
        plan,
        common_crop_width=common_crop_width,
        common_crop_height=common_crop_height,
        output_width=output_width,
        output_height=output_height,
        oriented_width=oriented_w,
        oriented_height=oriented_h,
    )
    crop_rect_before = (
        plan.center_x - common_crop_width / 2.0,
        plan.center_y - common_crop_height / 2.0,
        float(common_crop_width),
        float(common_crop_height),
    )
    crop_rect_after = (
        placement.crop_x_int, placement.crop_y_int,
        common_crop_width, common_crop_height,
    )

    # If padding is required on either axis the source is smaller than
    # the crop on that axis — the ROI is the whole source anyway, so
    # fall back to the reference path.
    if placement.padded_x or placement.padded_y:
        return reference_render_tile(
            source, plan,
            common_crop_width=common_crop_width,
            common_crop_height=common_crop_height,
            output_width=output_width,
            output_height=output_height,
        )

    # ── Inverse-map the crop corners back to source pixel space ────────
    # Build the affine that maps oriented-frame crop pixel → source
    # pixel.  Feeding this into `Image.transform` on the ROI (BILINEAR)
    # gives us the exact equivalent of the reference's "rotate whole
    # source, then crop", but only on a small ROI.
    ang = math.radians(plan.rotation_deg)
    cos_a = math.cos(ang)
    sin_a = math.sin(ang)
    cx = float(src_w) / 2.0
    cy = float(src_h) / 2.0
    if abs(plan.rotation_deg) <= 0.1:
        # Fast path for un-rotated crops — offset is (0, 0).
        offset_x = 0.0
        offset_y = 0.0
    else:
        _off = _rotated_source_offset(src_w, src_h, plan.rotation_deg)
        offset_x, offset_y = _off[0], _off[1]

    kx = float(placement.crop_x_int) - offset_x - cx
    ky = float(placement.crop_y_int) - offset_y - cy

    # crop_pixel → source_pixel (whole-source):
    #   sx = cx + (cxp + kx) * cos + (cyp + ky) * sin
    #   sy = cy - (cxp + kx) * sin + (cyp + ky) * cos
    #
    # As a (a', b', c', d', e', f') affine acting on (cxp, cyp):
    a_full = cos_a
    b_full = sin_a
    c_full = cx + kx * cos_a + ky * sin_a
    d_full = -sin_a
    e_full = cos_a
    f_full = cy - kx * sin_a + ky * cos_a

    # ROI = source-space AABB of the crop corners, clamped to source
    # and expanded by a small margin so the BILINEAR sampler has neighbours.
    crop_corners = (
        (0.0, 0.0),
        (float(common_crop_width), 0.0),
        (float(common_crop_width), float(common_crop_height)),
        (0.0, float(common_crop_height)),
    )
    src_pts = [
        (a_full * cxp + b_full * cyp + c_full,
         d_full * cxp + e_full * cyp + f_full)
        for cxp, cyp in crop_corners
    ]
    min_sx = min(p[0] for p in src_pts) - _FAST_ROI_MARGIN_PX
    max_sx = max(p[0] for p in src_pts) + _FAST_ROI_MARGIN_PX
    min_sy = min(p[1] for p in src_pts) - _FAST_ROI_MARGIN_PX
    max_sy = max(p[1] for p in src_pts) + _FAST_ROI_MARGIN_PX
    roi_x0 = max(0, int(math.floor(min_sx)))
    roi_y0 = max(0, int(math.floor(min_sy)))
    roi_x1 = min(src_w, int(math.ceil(max_sx)))
    roi_y1 = min(src_h, int(math.ceil(max_sy)))
    if roi_x1 <= roi_x0 or roi_y1 <= roi_y0:
        # Crop is entirely outside the source — return background.
        tile = Image.new(
            "RGB", (int(output_width), int(output_height)),
            tuple(int(v) for v in background_rgb),
        )
        polygon_tile_local = placement.transform_polygon(plan.oriented_corners)
        return SporeThumbnailCommonCropResult(
            image=tile,
            output_width=output_width,
            output_height=output_height,
            polygon_tile_local=polygon_tile_local,
            crop_rect_before_shift=crop_rect_before,
            crop_rect_after_shift=crop_rect_after,
            padded_x=placement.padded_x,
            padded_y=placement.padded_y,
            reason_no_polygon=plan.reason_no_polygon,
        )
    roi = working.crop((roi_x0, roi_y0, roi_x1, roi_y1))

    # Adjust the affine for ROI-local source coords.
    a_roi = a_full
    b_roi = b_full
    c_roi = c_full - float(roi_x0)
    d_roi = d_full
    e_roi = e_full
    f_roi = f_full - float(roi_y0)

    # Step 1: crop→ROI transform via BILINEAR — mirrors the reference
    # path's `img.rotate(BILINEAR)` filter on just the pixels we need.
    crop_canvas = roi.transform(
        (int(common_crop_width), int(common_crop_height)),
        Image.AFFINE,
        (a_roi, b_roi, c_roi, d_roi, e_roi, f_roi),
        resample=Image.BILINEAR,
        fillcolor=tuple(int(v) for v in background_rgb),
    )
    # Step 2: LANCZOS resize to final tile dims — exact match with the
    # reference path's resize step.
    if (int(common_crop_width), int(common_crop_height)) != (
        int(output_width), int(output_height)
    ):
        tile = crop_canvas.resize(
            (int(output_width), int(output_height)), Image.LANCZOS,
        )
    else:
        tile = crop_canvas

    polygon_tile_local = placement.transform_polygon(plan.oriented_corners)

    return SporeThumbnailCommonCropResult(
        image=tile,
        output_width=output_width,
        output_height=output_height,
        polygon_tile_local=polygon_tile_local,
        crop_rect_before_shift=crop_rect_before,
        crop_rect_after_shift=crop_rect_after,
        padded_x=placement.padded_x,
        padded_y=placement.padded_y,
        reason_no_polygon=plan.reason_no_polygon,
    )


def render_spore_thumbnail_common_crop(
    source: Image.Image,
    plan: SporeThumbnailPlan,
    *,
    common_crop_width: int,
    common_crop_height: int,
    output_width: int,
    output_height: int,
) -> SporeThumbnailCommonCropResult:
    """Render a fixed-size crop centred on the measurement.

    Dispatches to `fast_render_tile` by default and to
    `reference_render_tile` when ``SPORELY_MOSAIC_USE_REFERENCE`` is
    set. Both paths honour the same `SporeThumbnailPlan` +
    `CommonCropPlacement` so callers cannot observe a semantic
    difference beyond the documented image-difference threshold.
    """
    if _use_reference_renderer():
        return reference_render_tile(
            source, plan,
            common_crop_width=common_crop_width,
            common_crop_height=common_crop_height,
            output_width=output_width,
            output_height=output_height,
        )
    return fast_render_tile(
        source, plan,
        common_crop_width=common_crop_width,
        common_crop_height=common_crop_height,
        output_width=output_width,
        output_height=output_height,
    )


# ── Desktop-parity single-shot renderer ─────────────────────────────────────


def render_spore_thumbnail(
    source: Image.Image,
    inputs: SporeThumbnailInputs,
    height_px: int,
) -> SporeThumbnailRenderResult:
    """PIL port of `create_spore_thumbnail`. Produces a non-square tile.

    * `height_px` maps to Qt's `size` — the tile's HEIGHT is fixed to
      `height_px`, width follows crop aspect (`crop_w * height_px / crop_h`).
    * When p3/p4 are missing, we still render an oriented, cropped tile
      using just p1/p2 for orientation and a fallback AABB (p1, p2), but
      `polygon_tile_local` is None with `reason_no_polygon='missing_p3p4'`.
      The caller can decide whether to keep or drop such tiles.
    """
    if height_px < 8:
        raise ValueError("height_px too small")

    src_w, src_h = source.size
    plan = plan_spore_thumbnail(inputs, src_w, src_h)

    oriented = _rotate_source_if_needed(source, plan.rotation_deg, inputs.background_rgb)
    working_w, working_h = oriented.size

    # Desktop behaviour: crop is AABB(natural crop) *clamped* to oriented
    # image bounds. That may make it smaller than the natural size, but
    # this preserves the single-shot desktop output. The common-crop
    # path above deliberately avoids clamping so cross-tile widths are
    # comparable.
    natural_min_x = plan.center_x - plan.natural_crop_width / 2.0
    natural_max_x = plan.center_x + plan.natural_crop_width / 2.0
    natural_min_y = plan.center_y - plan.natural_crop_height / 2.0
    natural_max_y = plan.center_y + plan.natural_crop_height / 2.0

    crop_x = max(0, int(math.floor(natural_min_x)))
    crop_y = max(0, int(math.floor(natural_min_y)))
    crop_x2 = min(working_w, int(math.ceil(natural_max_x)))
    crop_y2 = min(working_h, int(math.ceil(natural_max_y)))
    crop_w = max(1, crop_x2 - crop_x)
    crop_h = max(1, crop_y2 - crop_y)

    cropped = oriented.crop((crop_x, crop_y, crop_x + crop_w, crop_y + crop_h))

    scale_factor = float(height_px) / float(max(1, crop_h))
    tile_h = int(round(crop_h * scale_factor))
    tile_w = max(1, int(round(crop_w * scale_factor)))
    if (tile_w, tile_h) != cropped.size:
        cropped = cropped.resize((tile_w, tile_h), Image.LANCZOS)

    polygon_tile_local: list[tuple[float, float]] | None = None
    if plan.oriented_corners is not None:
        polygon_tile_local = [
            ((cx_ - crop_x) * scale_factor, (cy_ - crop_y) * scale_factor)
            for cx_, cy_ in plan.oriented_corners
        ]

    return SporeThumbnailRenderResult(
        image=cropped,
        tile_width_px=tile_w,
        tile_height_px=tile_h,
        polygon_tile_local=polygon_tile_local,
        crop_rect_source_pixels=(crop_x, crop_y, crop_w, crop_h),
        rotation_deg=plan.rotation_deg,
        reason_no_polygon=plan.reason_no_polygon,
    )
