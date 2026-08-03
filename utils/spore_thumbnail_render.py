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
        paste_dx = (int(common_crop_width_px) - int(oriented_source_w)) // 2
    else:
        paste_dx = -crop_x_int
    if padded_y:
        paste_dy = (int(common_crop_height_px) - int(oriented_source_h)) // 2
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

    * `common_crop_width` / `common_crop_height` are the size in oriented
      source-image pixels of the crop window. Chosen from
      `max(plan.natural_crop_*)` across the observation.
    * The window is centred on `plan.center_x/y` and edge-shifted so it
      stays inside the oriented source image where possible.
    * If the oriented source is smaller than the crop on either axis,
      background pixels fill the remainder (`padded_x` / `padded_y`
      report that condition).
    * The resulting `common_crop_width × common_crop_height` canvas is
      resized to `output_width × output_height` (LANCZOS).
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

    # Delegate the shift + pad + scale maths to the shared pure resolver
    # so this raster path never drifts from the Qt / SVG paths.
    placement = resolve_common_crop_placement(
        oriented_source_w=working_w,
        oriented_source_h=working_h,
        center_x=plan.center_x,
        center_y=plan.center_y,
        common_crop_width_px=common_crop_width,
        common_crop_height_px=common_crop_height,
        output_width=output_width,
        output_height=output_height,
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
        canvas = Image.new("RGB", (common_crop_width, common_crop_height), background_rgb)
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
