"""Helpers for measurement rectangle overlay appearance."""

from PySide6.QtCore import QPointF
from PySide6.QtGui import QColor, QPolygonF


RECTANGLE_STYLE_A = "a"
RECTANGLE_STYLE_B = "b"
DEFAULT_RECTANGLE_STYLE = RECTANGLE_STYLE_A
DEFAULT_RECTANGLE_THICKNESS = 1.0
MIN_RECTANGLE_THICKNESS = 1.0
MAX_RECTANGLE_THICKNESS = 2.0
CORNER_OVERLAY_RATIO = 0.2
MIN_VISIBLE_STROKE_WIDTH = 0.2
STYLE_A_THIN_WIDTH_FACTOR = 0.5
STYLE_B_THIN_WIDTH_FACTOR = 0.3
NO_HALO_TEXT_COLORS = (
    "#4799cf",
    "#e48359",
    "#f2c75e",
    "#a269ae",
    "#9dc36a",
    "#7fd0f3",
    "#bc5669",
    "#ffffff",
)


def normalize_rectangle_style(style) -> str:
    text = str(style or "").strip().lower()
    if text == RECTANGLE_STYLE_B:
        return RECTANGLE_STYLE_B
    return RECTANGLE_STYLE_A


def clamp_rectangle_thickness(value, *, default: float = DEFAULT_RECTANGLE_THICKNESS) -> float:
    try:
        thickness = float(value)
    except (TypeError, ValueError):
        thickness = float(default)
    return max(MIN_RECTANGLE_THICKNESS, min(MAX_RECTANGLE_THICKNESS, thickness))


def clamp_stroke_width(value, *, default: float = MIN_VISIBLE_STROKE_WIDTH) -> float:
    try:
        width = float(value)
    except (TypeError, ValueError):
        width = float(default)
    return max(MIN_VISIBLE_STROKE_WIDTH, width)


def rectangle_thin_stroke_width(style, thick_width) -> float:
    resolved_style = normalize_rectangle_style(style)
    resolved_thick = clamp_rectangle_thickness(thick_width)
    factor = STYLE_B_THIN_WIDTH_FACTOR if resolved_style == RECTANGLE_STYLE_B else STYLE_A_THIN_WIDTH_FACTOR
    return clamp_stroke_width(resolved_thick * factor)


def measure_text_uses_halo(color) -> bool:
    base = QColor(color)
    if not base.isValid():
        return True
    if base.lightness() >= 245:
        return False

    def _dist2(left: QColor, right: QColor) -> int:
        dr = int(left.red()) - int(right.red())
        dg = int(left.green()) - int(right.green())
        db = int(left.blue()) - int(right.blue())
        return dr * dr + dg * dg + db * db

    no_halo_matches = tuple(QColor(value) for value in NO_HALO_TEXT_COLORS)
    nearest = min(no_halo_matches, key=lambda match: _dist2(base, match))
    return _dist2(base, nearest) > 4


def rectangle_corner_segments(
    polygon: QPolygonF | list[QPointF],
    *,
    fraction: float = CORNER_OVERLAY_RATIO,
) -> list[tuple[QPointF, QPointF]]:
    points = [QPointF(point) for point in polygon]
    count = len(points)
    if count < 2:
        return []

    clipped_fraction = max(0.0, min(0.5, float(fraction)))
    segments: list[tuple[QPointF, QPointF]] = []
    for idx in range(count):
        start = points[idx]
        end = points[(idx + 1) % count]
        dx = end.x() - start.x()
        dy = end.y() - start.y()
        length = (dx * dx + dy * dy) ** 0.5
        if length <= 0:
            continue
        seg_len = length * clipped_fraction
        ux = dx / length
        uy = dy / length
        segments.append(
            (
                QPointF(start.x(), start.y()),
                QPointF(start.x() + ux * seg_len, start.y() + uy * seg_len),
            )
        )
        segments.append(
            (
                QPointF(end.x() - ux * seg_len, end.y() - uy * seg_len),
                QPointF(end.x(), end.y()),
            )
        )
    return segments
