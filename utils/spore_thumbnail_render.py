"""Pure-PIL port of `main_window.create_spore_thumbnail` — the exact math
the desktop Analysis gallery uses to orient, crop, and outline a spore.

Extracted so cloud-side mosaic generation runs the SAME pipeline as the
desktop tile the user actually sees, instead of re-deriving it. When in
doubt about any behavior here, the source of truth is
`ui/main_window.py::create_spore_thumbnail` — the constants, the rotation
formula, and the corner-computation math are copied verbatim.

Deliberate simplifications relative to the Qt version:

* No painting inside the returned tile (no rectangle draw, no dimension
  text). The web frontend draws the overlay itself, so we return the
  polygon as data.
* No colour matching / stroke picking — that's a paint concern.
* No `selected` / `measurement_num` numbering.

Everything else — the orient rotation, the source-rect origin offset, the
crop bounds, and the tile-local polygon coordinates — matches the desktop
pipeline exactly.
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
    """One measurement, in source-image pixel space."""

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


# ── Renderer ────────────────────────────────────────────────────────────────


def _to_rgb(img: Image.Image, background_rgb: tuple[int, int, int]) -> Image.Image:
    if img.mode == "RGB":
        return img
    if img.mode == "RGBA":
        flat = Image.new("RGB", img.size, background_rgb)
        flat.paste(img, mask=img.split()[3])
        return flat
    return img.convert("RGB")


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

    p1x, p1y = inputs.p1_x, inputs.p1_y
    p2x, p2y = inputs.p2_x, inputs.p2_y
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

    # Rotation exactly as in create_spore_thumbnail.
    rotation_angle = float(inputs.extra_rotation_deg or 0.0)
    if inputs.orient and line1_len > 0:
        current_angle = math.atan2(line1_vx, -line1_vy)
        rotation_angle += -math.degrees(current_angle)

    applied_rotation = 0.0
    working_img = source
    working_w, working_h = src_w, src_h
    if abs(rotation_angle) > 0.1:
        rotated_img = _rotate_pil_qt_style(source, rotation_angle, inputs.background_rgb)
        off_x, off_y = _rotated_source_offset(src_w, src_h, rotation_angle)

        def _map(px: float, py: float) -> tuple[float, float]:
            rx, ry = rotate_point_qt(px, py, src_w / 2.0, src_h / 2.0, rotation_angle)
            return rx + off_x, ry + off_y

        p1x, p1y = _map(p1x, p1y)
        p2x, p2y = _map(p2x, p2y)
        if have_p34:
            p3x, p3y = _map(p3x, p3y)
            p4x, p4y = _map(p4x, p4y)
        # Recompute in rotated frame — matches lines 17444-17454 of main_window.py.
        line1_vx = p2x - p1x
        line1_vy = p2y - p1y
        line1_len = math.hypot(line1_vx, line1_vy)
        applied_rotation = rotation_angle
        working_img = rotated_img
        working_w, working_h = rotated_img.size

    # Compute the measurement rectangle in the (possibly rotated) frame.
    reason_no_polygon: str | None = None
    corners_img: list[tuple[float, float]] | None = None
    if line1_len <= 0:
        reason_no_polygon = "zero_length_axis"
    elif not have_p34:
        reason_no_polygon = "missing_p3p4"
    else:
        line2_vx = p4x - p3x
        line2_vy = p4y - p3y
        width_px = math.hypot(line2_vx, line2_vy)
        if width_px <= 0:
            reason_no_polygon = "zero_width_axis"
        else:
            center_x = ((p1x + p2x) * 0.5 + (p3x + p4x) * 0.5) * 0.5
            center_y = ((p1y + p2y) * 0.5 + (p3y + p4y) * 0.5) * 0.5
            axis_length_x = -line1_vx / line1_len
            axis_length_y = -line1_vy / line1_len
            axis_width_x = -axis_length_y
            axis_width_y = axis_length_x
            hl = line1_len * 0.5
            hw = width_px * 0.5
            corners_img = [
                (center_x + axis_width_x * (-hw) + axis_length_x * (-hl),
                 center_y + axis_width_y * (-hw) + axis_length_y * (-hl)),
                (center_x + axis_width_x * (hw) + axis_length_x * (-hl),
                 center_y + axis_width_y * (hw) + axis_length_y * (-hl)),
                (center_x + axis_width_x * (hw) + axis_length_x * (hl),
                 center_y + axis_width_y * (hw) + axis_length_y * (hl)),
                (center_x + axis_width_x * (-hw) + axis_length_x * (hl),
                 center_y + axis_width_y * (-hw) + axis_length_y * (hl)),
            ]

    # Crop bounds. Follow desktop math: AABB(corners) + padding, clamped.
    # When we can't build a rectangle, fall back to p1/p2 AABB so we still
    # produce a tile the caller can display without a measurement outline.
    if corners_img is not None:
        crop_source_points: Sequence[tuple[float, float]] = corners_img
    else:
        crop_source_points = [(p1x, p1y), (p2x, p2y)]

    min_x = min(p[0] for p in crop_source_points) - inputs.padding_x_px
    max_x = max(p[0] for p in crop_source_points) + inputs.padding_x_px
    min_y = min(p[1] for p in crop_source_points) - inputs.padding_y_px
    max_y = max(p[1] for p in crop_source_points) + inputs.padding_y_px

    crop_x = max(0, int(math.floor(min_x)))
    crop_y = max(0, int(math.floor(min_y)))
    crop_x2 = min(working_w, int(math.ceil(max_x)))
    crop_y2 = min(working_h, int(math.ceil(max_y)))
    crop_w = max(1, crop_x2 - crop_x)
    crop_h = max(1, crop_y2 - crop_y)

    cropped = working_img.crop((crop_x, crop_y, crop_x + crop_w, crop_y + crop_h))
    cropped = _to_rgb(cropped, inputs.background_rgb)

    # scale_factor = size / crop.height  — same as create_spore_thumbnail.
    scale_factor = float(height_px) / float(max(1, crop_h))
    tile_h = int(round(crop_h * scale_factor))
    tile_w = max(1, int(round(crop_w * scale_factor)))
    if (tile_w, tile_h) != cropped.size:
        cropped = cropped.resize((tile_w, tile_h), Image.LANCZOS)

    polygon_tile_local: list[tuple[float, float]] | None = None
    if corners_img is not None:
        polygon_tile_local = [
            ((cx_ - crop_x) * scale_factor, (cy_ - crop_y) * scale_factor)
            for cx_, cy_ in corners_img
        ]

    return SporeThumbnailRenderResult(
        image=cropped,
        tile_width_px=tile_w,
        tile_height_px=tile_h,
        polygon_tile_local=polygon_tile_local,
        crop_rect_source_pixels=(crop_x, crop_y, crop_w, crop_h),
        rotation_deg=applied_rotation,
        reason_no_polygon=reason_no_polygon,
    )
