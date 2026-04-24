"""Species Plate — composite mycological illustration for a single observation."""
from __future__ import annotations

import json
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from PySide6.QtCore import QPoint, QPointF, QRectF, QSettings, QSizeF, Qt
from PySide6.QtGui import (
    QBrush, QColor, QFont, QFontMetrics, QImage, QLinearGradient,
    QPainter, QPainterPath, QPen, QPixmap, QPolygonF, QTransform,
)
from PySide6.QtCore import QSize
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import (
    QButtonGroup, QCheckBox, QDialog, QFileDialog, QFrame, QGroupBox,
    QHBoxLayout, QHeaderView, QLabel, QPushButton, QSizePolicy, QSlider,
    QTableWidget, QTableWidgetItem, QVBoxLayout, QWidget,
)

try:
    from PySide6.QtSvg import QSvgGenerator
    _SVG_OK = True
except ImportError:
    _SVG_OK = False

from database.models import ImageDB, MeasurementDB, SettingsDB, CalibrationDB
from database.database_tags import DatabaseTerms
from database.schema import get_app_settings, update_app_settings, get_database_path
from ui.hint_status import HintBar, HintStatusController
from app_identity import APP_NAME, SETTINGS_ORG
from ui.dialog_helpers import make_github_help_button

# ── Canvas geometry ───────────────────────────────────────────────────────────
_W, _H = 3200, 2000
_INS_R_DEFAULT, _INS_R_MIN, _INS_R_MAX = 280, 150, 840
_INS_M = 90           # edge → circle-centre gap
_INS_STROKE = 8
_OVERLAY_STROKE = 16        # thicker white ring for overlay layout
_OVERLAY_R_MIN = 420        # overlay: current max is the new minimum
_OVERLAY_R_MAX = 760        # overlay: upper end of remapped size range
_BORDER_HIT_PX = 14         # preview px from circle edge to register as "border" hit
_CIRCLE_ZOOM_MAX = 12.0
_PREVIEW_SCALE = 0.40
_TEXT_SCALE_DEFAULT = 1.0
_BG_SLOTS_MAX = 3

_SLOT_KEYS = ["TL", "TC", "TR", "BL", "BR"]
_OVERLAY_SLOT_KEYS = ["TL", "TC", "BR"]   # slots used by the 3_overlay layout

_SLOT_SAMPLES = {
    "TL": ["basidia", "basidium"],
    "TC": ["hymenium", "pileipellis", "context", "trama"],
    "TR": ["spore", "spores"],
    "BL": ["cheilocystidia", "cheilocystidium", "cystidia"],
    "BR": ["pleurocystidia", "pleurocystidium", "caulocystidia", "caulocystidium"],
}
_SLOT_FALLBACK_LABEL = {
    "TL": "Basidia",
    "TC": "Detail",
    "TR": "Spores",
    "BL": "Cystidia",
    "BR": "Cystidia",
}

def _del_hint() -> str:
    """Return the platform-appropriate keyboard shortcut hint for deleting an inset."""
    import sys as _sys
    return "Del / ⌘D" if _sys.platform == "darwin" else "Del / Ctrl+D"


_LICENSE_LABELS = {
    "10": "CC BY 4.0",
    "20": "CC BY-SA 4.0",
    "30": "CC BY-NC-SA 4.0",
    "60": "No reuse without permission",
}

# Cover-crop cache: (filepath, width, height) → QPixmap
_cover_cache: dict[tuple[str, int, int], QPixmap] = {}
# Raw pixmap cache: filepath → QPixmap
_raw_cache: dict[str, QPixmap] = {}


# ── Per-slot zoom/pan state ───────────────────────────────────────────────────
@dataclass
class SlotCrop:
    zoom: float = 1.0
    off_x: float = 0.0
    off_y: float = 0.0


# ── Image helpers ─────────────────────────────────────────────────────────────
def _load_pix(filepath: str) -> QPixmap:
    if filepath not in _raw_cache:
        _raw_cache[filepath] = QPixmap(filepath)
    return _raw_cache[filepath]


def _cover_source_rect(
    img_w: int,
    img_h: int,
    target_w: int,
    target_h: int,
    crop: SlotCrop,
) -> QRectF | None:
    """Return the original-image source rect for the current cover+zoom+pan state."""
    if img_w <= 0 or img_h <= 0 or target_w <= 0 or target_h <= 0:
        return None

    cover_scale = max(target_w / float(img_w), target_h / float(img_h))
    if cover_scale <= 0:
        return None

    cover_w = img_w * cover_scale
    cover_h = img_h * cover_scale
    z = max(float(crop.zoom), 1.0)

    vis_w = target_w / z
    vis_h = target_h / z
    # Clamp offset so the visible window never overruns the covered image area.
    # half_x/half_y is the maximum shift from centre in each axis.
    half_x = (cover_w - vis_w) / 2.0
    half_y = (cover_h - vis_h) / 2.0
    center_x = target_w / 2.0 + max(-half_x, min(half_x, float(crop.off_x)))
    center_y = target_h / 2.0 + max(-half_y, min(half_y, float(crop.off_y)))
    sub_x = center_x - vis_w / 2.0
    sub_y = center_y - vis_h / 2.0

    src_x = (sub_x + (cover_w - target_w) / 2.0) / cover_scale
    src_y = (sub_y + (cover_h - target_h) / 2.0) / cover_scale
    src_w = vis_w / cover_scale
    src_h = vis_h / cover_scale

    src_w = min(float(img_w), max(src_w, 1.0 / cover_scale))
    src_h = min(float(img_h), max(src_h, 1.0 / cover_scale))
    src_x = max(0.0, min(src_x, float(img_w) - src_w))
    src_y = max(0.0, min(src_y, float(img_h) - src_h))
    return QRectF(src_x, src_y, src_w, src_h)


def _render_cropped_pixmap(filepath: str, w: int, h: int, crop: SlotCrop) -> QPixmap:
    src = _load_pix(filepath)
    if src.isNull() or w <= 0 or h <= 0:
        return src

    src_rect = _cover_source_rect(src.width(), src.height(), w, h, crop)
    if src_rect is None:
        return src

    result = QPixmap(w, h)
    result.fill(Qt.transparent)
    painter = QPainter(result)
    painter.setRenderHint(QPainter.SmoothPixmapTransform)
    painter.drawPixmap(QRectF(0.0, 0.0, float(w), float(h)), src, src_rect)
    painter.end()
    return result


def _rect_image(filepath: str, w: int, h: int, crop: SlotCrop) -> QPixmap:
    return _render_cropped_pixmap(filepath, w, h, crop)


def _circular_image(filepath: str, diam: int, crop: SlotCrop,
                    shape_step: int = 0) -> QImage:
    """ARGB32 image with superellipse/rounded-rect alpha — embeds cleanly in SVG."""
    img = QImage(diam, diam, QImage.Format_ARGB32_Premultiplied)
    img.fill(Qt.transparent)
    src = _load_pix(filepath)
    src_rect = _cover_source_rect(src.width(), src.height(), diam, diam, crop)
    if src.isNull() or src_rect is None:
        return img
    p = QPainter(img)
    p.setRenderHint(QPainter.Antialiasing)
    p.setRenderHint(QPainter.SmoothPixmapTransform)
    half = diam / 2.0
    clip = _inset_shape_path(half, half, half - 0.5, shape_step)
    p.setClipPath(clip)
    p.drawPixmap(QRectF(0.0, 0.0, float(diam), float(diam)), src, src_rect)
    p.end()
    return img


def _slot_centre(r: int, slot: str) -> tuple[float, float]:
    m = _INS_M + r
    return {"TL": (m, m), "TC": (_W / 2, m), "TR": (_W - m, m),
            "BL": (m, _H - m), "BR": (_W - m, _H - m)}[slot]


def _slot_centre_pos(r: int, slot: str, t: float) -> tuple[float, float]:
    """Like _slot_centre but with inset position factor t in [0, 1].

    t=1 → default position (_INS_M + r from edge).
    t=0 → centre at the canvas edge (half the shape outside the frame).
    TC (top-centre) only moves vertically; left/right position is fixed at W/2.
    """
    m = t * (_INS_M + r)
    return {"TL": (m, m), "TC": (_W / 2, m), "TR": (_W - m, m),
            "BL": (m, _H - m), "BR": (_W - m, _H - m)}[slot]


_INS_R_MAX_OVERLAY_REF = 420   # overlay remap is anchored to original slider max

def _overlay_base_r(r: int) -> int:
    """Remap ins_r → overlay base radius [_OVERLAY_R_MIN.._OVERLAY_R_MAX].

    Uses the original upper reference (420 px) so that overlay circle sizes are
    independent of changes to _INS_R_MAX.  Values above the reference are clamped.
    """
    t = max(0.0, min(1.0, (r - _INS_R_MIN) / float(_INS_R_MAX_OVERLAY_REF - _INS_R_MIN)))
    return int(_OVERLAY_R_MIN + t * (_OVERLAY_R_MAX - _OVERLAY_R_MIN))


def _overlay_default_positions(base_r: int) -> dict[str, tuple[float, float]]:
    """Default circle centres for the 3_overlay layout at the given base radius."""
    r_tl = int(base_r * 0.85)
    r_br = int(base_r * 0.85)
    return {
        "TL": (r_tl * 0.65, r_tl * 0.65),
        "TC": (_W / 2.0, _H / 2.0),          # at the panel-intersection boundary
        "BR": (_W - r_br * 0.65, _H - r_br * 0.65),
    }


def _overlay_slot_params(
    r: int,
    positions: Optional[dict[str, Optional[tuple[float, float]]]] = None,
    radii_overrides: Optional[dict[str, Optional[int]]] = None,
) -> dict[str, tuple[float, float, int]]:
    """Return {slot: (cx, cy, radius)} for the 3_overlay layout.

    TL/BR: ~85 % of base radius, ~35 % extends outside the canvas corners.
    TC:    ~115 % of base radius — the largest circle, at the panel intersection.
    *positions* overrides the default centre for any slot (native canvas coords).
    *radii_overrides* overrides the computed radius for any slot (native px).
    """
    base_r = _overlay_base_r(r)
    default_radii = {"TL": int(base_r * 0.85), "TC": int(base_r * 1.15), "BR": int(base_r * 0.85)}
    defaults = _overlay_default_positions(base_r)
    result: dict[str, tuple[float, float, int]] = {}
    for slot in ("TL", "TC", "BR"):
        pos = (positions or {}).get(slot) if positions else None
        cx, cy = pos if pos else defaults[slot]
        slot_r = (radii_overrides or {}).get(slot) if radii_overrides else None
        slot_r = slot_r if slot_r is not None else default_radii[slot]
        result[slot] = (cx, cy, slot_r)
    return result


_BG_LAYOUTS = ("1", "2_50", "2_60", "3_50", "3", "3_overlay",
               "2_al", "2_ar", "3_al", "3_ar", "2_tilt")
_INSET_LAYOUTS = ("5", "3")  # 5-circle standard, 3-circle overlay

# Two tilted panels: (cx_frac, cy_frac, w_frac, h_frac) in native canvas coords
_TILT_PANEL_PARAMS = [
    (0.30, 0.50, 0.58, 0.90),   # left panel
    (0.70, 0.50, 0.58, 0.90),   # right panel
]
_TILT_LAYOUTS = frozenset({"2_tilt"})
# How much tilt-panel centers follow the content_rect when scaling (0=fixed, 1=proportional)
_TILT_CENTER_BLEND = 0.18


def _tilt_panel_geom(idx: int, content_rect: "QRectF") -> "tuple[float, float, float, float]":
    """Return (cx, cy, pw, ph) for a tilt panel.

    Sizes scale with content_rect so panels shrink when scaled down.
    Centers are mostly anchored to their canvas-fraction positions so the
    panels stay spread apart; only a small blend toward content_rect keeps
    them from drifting off-canvas.
    """
    cx_f, cy_f, w_f, h_f = _TILT_PANEL_PARAMS[idx]
    pw = w_f * content_rect.width()
    ph = h_f * content_rect.height()
    # Canvas-anchored centre
    cx_anchor = cx_f * _W
    cy_anchor = cy_f * _H
    # Content-rect-proportional centre
    cx_full = content_rect.x() + cx_f * content_rect.width()
    cy_full = content_rect.y() + cy_f * content_rect.height()
    t = _TILT_CENTER_BLEND
    cx = cx_anchor + t * (cx_full - cx_anchor)
    cy = cy_anchor + t * (cy_full - cy_anchor)
    return cx, cy, pw, ph

# Native-coord polygon vertices for angled split layouts.
# Each entry is a list of panels, each panel is a list of (x, y) tuples.
# Canvas: _W×_H = 3200×2000.
# 2_al: vertical divider leaning ~22° (top shifts 400 left, bottom 400 right of centre).
# 2_ar: mirror of 2_al.
# 3_al: left half + 2 right panels split by ~21° angled horizontal line (leans left-down).
# 3_ar: mirror of 3_al (leans right-down).
_BG_ANGLED_POLYS: dict[str, list[list[tuple[float, float]]]] = {
    "2_al": [
        [(0, 0), (1200, 0), (2000, _H), (0, _H)],
        [(1200, 0), (_W, 0), (_W, _H), (2000, _H)],
    ],
    "2_ar": [
        [(0, 0), (2000, 0), (1200, _H), (0, _H)],
        [(2000, 0), (_W, 0), (_W, _H), (1200, _H)],
    ],
    "3_al": [
        [(0, 0), (_W // 2, 0), (_W // 2, _H), (0, _H)],
        [(_W // 2, 0), (_W, 0), (_W, 1300), (_W // 2, 700)],
        [(_W // 2, 700), (_W, 1300), (_W, _H), (_W // 2, _H)],
    ],
    "3_ar": [
        [(0, 0), (_W // 2, 0), (_W // 2, _H), (0, _H)],
        [(_W // 2, 0), (_W, 0), (_W, 700), (_W // 2, 1300)],
        [(_W // 2, 1300), (_W, 700), (_W, _H), (_W // 2, _H)],
    ],
}
_ANGLED_LAYOUTS = frozenset(_BG_ANGLED_POLYS)


def _rotated_rect_path(cx: float, cy: float, w: float, h: float,
                       angle_deg: float) -> "QPainterPath":
    """Return a QPainterPath polygon for a rectangle rotated around its centre."""
    rad = math.radians(angle_deg)
    cos_a, sin_a = math.cos(rad), math.sin(rad)
    hw, hh = w / 2, h / 2
    corners = [(-hw, -hh), (hw, -hh), (hw, hh), (-hw, hh)]
    pts = [QPointF(cx + x * cos_a - y * sin_a, cy + x * sin_a + y * cos_a)
           for x, y in corners]
    path = QPainterPath()
    path.addPolygon(QPolygonF(pts))
    path.closeSubpath()
    return path


def _background_panel_rects(layout: str) -> list[QRectF]:
    """Return native-resolution panel rects for the given bg layout key.

    For angled layouts this returns the *bounding rect* of each polygon panel
    (used for panel count, image-crop reference size, and background-size slider).
    Actual clipping in the painter uses _scaled_bg_panel_shapes().
    """
    if layout in _ANGLED_LAYOUTS:
        return [
            QRectF(min(x for x, _ in poly), min(y for _, y in poly),
                   max(x for x, _ in poly) - min(x for x, _ in poly),
                   max(y for _, y in poly) - min(y for _, y in poly))
            for poly in _BG_ANGLED_POLYS[layout]
        ]
    if layout == "2_50":
        hw = _W // 2
        return [QRectF(0, 0, hw, _H), QRectF(hw, 0, _W - hw, _H)]
    if layout == "2_60":
        lw = int(_W * 0.60)
        return [QRectF(0, 0, lw, _H), QRectF(lw, 0, _W - lw, _H)]
    if layout == "3_50":
        lw = _W // 2
        rw = _W - lw
        hh = _H // 2
        return [QRectF(0, 0, lw, _H),
                QRectF(lw, 0, rw, hh),
                QRectF(lw, hh, rw, _H - hh)]
    if layout == "3":
        lw = int(_W * 0.60)
        rw = _W - lw
        hh = _H // 2
        return [QRectF(0, 0, lw, _H),
                QRectF(lw, 0, rw, hh),
                QRectF(lw, hh, rw, _H - hh)]
    if layout == "3_overlay":
        # Two full-height panels anchored to opposite corners.
        # Panel 0 — upper-right: right 55 % of canvas.
        # Panel 1 — lower-left: left 55 % of canvas.
        # Together they cover the whole canvas with a 10 % overlap in the centre.
        rw = int(_W * 0.55)
        return [QRectF(_W - rw, 0, rw, _H),   # upper-right
                QRectF(0, 0, rw, _H)]          # lower-left
    if layout == "2_tilt":
        # Approximate axis-aligned bounding rects — only used for panel count
        return [QRectF(0, _H * 0.05, _W * 0.58, _H * 0.90),
                QRectF(_W * 0.42, _H * 0.05, _W * 0.58, _H * 0.90)]
    # "1" or anything else
    return [QRectF(0, 0, _W, _H)]


def _content_rect_for_insets(r: int, inset_position: int) -> QRectF:
    """Background image area controlled by the Background size slider.

    Slider semantics:
    - `0` = minimum size, type-2 scaling (fixed top/bottom border, width only)
    - `50` = switch point where the side walls align with the inset centres
      and the background has equal border width on all sides
    - `100` = maximum background size (fills the frame)

    Geometry:
    - 0..50: type 2, fixed top/bottom border; only width changes.
    - 50..100: type 1, equal border on all sides; scales up to full frame.
    """
    t = max(0.0, min(1.0, float(inset_position) / 100.0))
    # Background sizing is intentionally independent of the current inset size.
    # Use the default inset radius as the layout reference.
    ref_r = float(_INS_R_DEFAULT)
    switch_margin = float(_INS_M + ref_r)
    switch_width = max(1.0, _W - 2.0 * switch_margin)
    switch_height = max(1.0, _H - 2.0 * switch_margin)
    min_width = max(1.0, _W - 2.0 * (_INS_M + (2.0 * ref_r)))

    if t <= 0.5:
        local_t = t / 0.5 if 0.5 > 0 else 1.0
        width = min_width + (switch_width - min_width) * local_t
        height = switch_height
        return QRectF((_W - width) / 2.0, (_H - height) / 2.0, width, height)

    local_t = (t - 0.5) / 0.5
    margin = switch_margin * (1.0 - local_t)
    width = max(1.0, _W - 2.0 * margin)
    height = max(1.0, _H - 2.0 * margin)
    return QRectF((_W - width) / 2.0, (_H - height) / 2.0, width, height)


def _scaled_background_panel_rects(layout: str, content_rect: QRectF) -> list[QRectF]:
    """Map logical background layout panels into the current content rect."""
    base = _background_panel_rects(layout)
    sx = content_rect.width() / float(_W)
    sy = content_rect.height() / float(_H)
    mapped: list[QRectF] = []
    for rect in base:
        mapped.append(
            QRectF(
                content_rect.x() + rect.x() * sx,
                content_rect.y() + rect.y() * sy,
                rect.width() * sx,
                rect.height() * sy,
            )
        )
    return mapped


def _scaled_bg_panel_shapes(
    layout: str, content_rect: QRectF,
    tilt_angles: Optional[list] = None,
) -> list[tuple[QRectF, "QPainterPath"]]:
    """Return (bounding_rect, clip_path) pairs for each panel, scaled to content_rect.

    Used by the painter so it can clip to the exact panel polygon (including
    angled dividers) while using the bounding rect to determine the image crop size.
    """
    sx = content_rect.width() / float(_W)
    sy = content_rect.height() / float(_H)
    ox, oy = content_rect.x(), content_rect.y()

    if layout in _ANGLED_LAYOUTS:
        result = []
        for poly_pts in _BG_ANGLED_POLYS[layout]:
            pts = [QPointF(ox + x * sx, oy + y * sy) for x, y in poly_pts]
            xs = [p.x() for p in pts]
            ys = [p.y() for p in pts]
            bounding = QRectF(min(xs), min(ys), max(xs) - min(xs), max(ys) - min(ys))
            path = QPainterPath()
            path.addPolygon(QPolygonF(pts))
            path.closeSubpath()
            result.append((bounding, path))
        return result

    if layout in _TILT_LAYOUTS:
        angles = list(tilt_angles) if tilt_angles else [0.0, 0.0]
        result = []
        for i in range(len(_TILT_PANEL_PARAMS)):
            cx, cy, pw, ph = _tilt_panel_geom(i, content_rect)
            angle = angles[i] if i < len(angles) else 0.0
            path = _rotated_rect_path(cx, cy, pw, ph, angle)
            result.append((path.boundingRect(), path))
        return result

    # Rect-based layouts: clip path = rect
    rects = _scaled_background_panel_rects(layout, content_rect)
    out = []
    for r in rects:
        path = QPainterPath()
        path.addRect(r)
        out.append((r, path))
    return out


def _make_layout_icon(layout: str, w: int = 52, h: int = 32) -> QPixmap:
    """Draw a small diagram icon for a background layout."""
    pm = QPixmap(w, h)
    pm.fill(Qt.transparent)
    p = QPainter(pm)
    p.setRenderHint(QPainter.Antialiasing)
    m = 2
    iw, ih = w - 2 * m, h - 2 * m
    panels: list[tuple[float, float, float, float]] = []
    if layout == "2_50":
        panels = [(0, 0, 0.5, 1), (0.5, 0, 0.5, 1)]
    elif layout == "2_60":
        panels = [(0, 0, 0.60, 1), (0.60, 0, 0.40, 1)]
    elif layout == "3_50":
        panels = [(0, 0, 0.50, 1), (0.50, 0, 0.50, 0.5), (0.50, 0.5, 0.50, 0.5)]
    elif layout == "3":
        panels = [(0, 0, 0.60, 1), (0.60, 0, 0.40, 0.5), (0.60, 0.5, 0.40, 0.5)]
    else:  # "1"
        panels = [(0, 0, 1, 1)]
    # Medium gray: visible on both light and dark backgrounds
    border = QColor(110, 125, 145, 230)

    # Angled-split icons: draw panels as filled polygons scaled to icon coords
    if layout in _ANGLED_LAYOUTS:
        fills = [QColor(110, 125, 145, 55), QColor(110, 125, 145, 110),
                 QColor(110, 125, 145, 55)]
        for idx, poly_pts in enumerate(_BG_ANGLED_POLYS[layout]):
            pts = [QPointF(m + x / _W * iw, m + y / _H * ih) for x, y in poly_pts]
            path = QPainterPath()
            path.addPolygon(QPolygonF(pts))
            path.closeSubpath()
            p.setPen(QPen(border, 1))
            p.setBrush(QBrush(fills[idx % len(fills)]))
            p.drawPath(path)
        p.end()
        return pm

    if layout == "3_overlay":
        # Draw two overlapping rects showing corner-anchored panels, plus three
        # small circles hinting at the overlay circle arrangement.
        p.setPen(QPen(border, 1))
        p.setBrush(Qt.NoBrush)
        rw2 = int(iw * 0.55)
        # upper-right panel
        p.drawRect(m + iw - rw2, m, rw2, ih)
        # lower-left panel (drawn on top)
        p.drawRect(m, m, rw2, ih)
        # Three circles: large TL, medium centre, large BR
        p.setBrush(QBrush(QColor(110, 125, 145, 80)))
        r_ul = int(min(iw, ih) * 0.30)
        r_c  = int(min(iw, ih) * 0.19)
        r_br = int(min(iw, ih) * 0.35)
        p.drawEllipse(m - r_ul // 2, m - r_ul // 2, r_ul * 2, r_ul * 2)
        p.drawEllipse(m + iw // 2 - r_c, m + ih // 2 - r_c, r_c * 2, r_c * 2)
        p.drawEllipse(m + iw - r_br // 2 - 2, m + ih - r_br // 2 - 2, r_br * 2, r_br * 2)
        p.end()
        return pm
    if layout == "2_tilt":
        # Two overlapping tilted rectangles, one left, one right
        p.setPen(QPen(border, 1))
        fills = [QColor(110, 125, 145, 60), QColor(110, 125, 145, 110)]
        cx = m + iw // 2
        cy = m + ih // 2
        pw = int(iw * 0.58)
        ph = int(ih * 0.88)
        for i, angle_deg in enumerate([-3.5, 3.5]):
            off_x = int(iw * 0.20 * (1 if i else -1))
            path = _rotated_rect_path(cx + off_x, cy, pw, ph, angle_deg)
            p.setBrush(QBrush(fills[i]))
            p.drawPath(path)
        p.end()
        return pm
    for nx, ny, nw, nh in panels:
        rx = m + int(nx * iw)
        ry = m + int(ny * ih)
        rw = int(nw * iw)
        rh = int(nh * ih)
        p.setPen(QPen(border, 1))
        p.setBrush(Qt.NoBrush)
        p.drawRect(rx, ry, rw, rh)
    p.end()
    return pm


def _get_layouts_dir() -> Path:
    """Return (and create) the folder where .mplate files and thumbnails are stored."""
    d = Path(get_database_path()).parent / "plate_layouts"
    d.mkdir(exist_ok=True)
    return d


def _swatch_icon(color_name: str, size: int = 20) -> "QIcon":
    """Solid black or white square icon for color-selector buttons."""
    color = QColor(245, 245, 245) if color_name == "white" else QColor(28, 28, 28)
    pm = QPixmap(size, size)
    pm.fill(color)
    p = QPainter(pm)
    p.setPen(QColor(160, 160, 160, 180))
    p.drawRect(0, 0, size - 1, size - 1)
    p.end()
    return QIcon(pm)


def _make_inset_layout_icon(layout: str, w: int = 52, h: int = 32) -> QPixmap:
    """Draw a small diagram icon for an inset layout ("5" or "3")."""
    pm = QPixmap(w, h)
    pm.fill(Qt.transparent)
    p = QPainter(pm)
    p.setRenderHint(QPainter.Antialiasing)
    border = QColor(110, 125, 145, 230)
    fill = QColor(110, 125, 145, 80)
    p.setPen(QPen(border, 1))
    p.setBrush(QBrush(fill))
    m = 2
    iw, ih = w - 2 * m, h - 2 * m
    if layout == "3":
        # Three circles: large TL, medium centre, large BR
        r_ul = int(min(iw, ih) * 0.30)
        r_c  = int(min(iw, ih) * 0.19)
        r_br = int(min(iw, ih) * 0.35)
        p.drawEllipse(m - r_ul // 2, m - r_ul // 2, r_ul * 2, r_ul * 2)
        p.drawEllipse(m + iw // 2 - r_c, m + ih // 2 - r_c, r_c * 2, r_c * 2)
        p.drawEllipse(m + iw - r_br // 2, m + ih - r_br // 2, r_br * 2, r_br * 2)
    else:
        # Five equal circles: TL TC TR BL BR
        r = int(min(iw, ih) * 0.14)
        for nx, ny in ((0.15, 0.25), (0.50, 0.25), (0.85, 0.25),
                       (0.25, 0.75), (0.75, 0.75)):
            cx = int(m + nx * iw)
            cy = int(m + ny * ih)
            p.drawEllipse(cx - r, cy - r, r * 2, r * 2)
    p.end()
    return pm


# ── Text helpers ──────────────────────────────────────────────────────────────
def _make_font(families: list[str], size_pt: float,
               bold=False, italic=False) -> QFont:
    f = QFont()
    f.setFamilies(families)
    f.setPointSizeF(size_pt)
    if bold:
        f.setWeight(QFont.Bold)
    if italic:
        f.setItalic(True)
    return f


def _draw_centred(painter: QPainter, text: str, cx: float, cy: float,
                  font: QFont, color: QColor, as_paths: bool) -> None:
    if not text:
        return
    if as_paths:
        probe = QPainterPath()
        probe.addText(0, 0, font, text)
        br = probe.boundingRect()
        path = QPainterPath()
        path.addText(cx - br.width() / 2 - br.x(), cy, font, text)
        painter.fillPath(path, QBrush(color))
    else:
        painter.setFont(font)
        painter.setPen(color)
        fm = painter.fontMetrics()
        painter.drawText(QPointF(cx - fm.horizontalAdvance(text) / 2, cy), text)


def _shadow_text(painter: QPainter, text: str, cx: float, cy: float,
                 font: QFont, color: QColor, as_paths: bool,
                 dx: float = 3.0, soft: bool = False) -> None:
    """Draw text with a drop shadow (soft=False) or soft shadow (soft=True).

    Drop shadow: semi-transparent black copy offset by (dx, dx), then coloured text.
    Soft shadow: filled+stroked shadow path at offset using sentinel colour #010000
    (so SVG post-processing can apply Gaussian blur), then full-opacity white text.
    """
    if soft:
        # stroke_w proportional to font size (ratio from reference SVG: ~0.12)
        stroke_w = max(1.0, font.pointSizeF() * 0.12)
        # Sentinel fill colour — visually near-black, identifiable by SVG post-processor
        shad = QColor(1, 0, 0, 180)
        half_w = QFontMetrics(font).horizontalAdvance(text) / 2.0
        offset_path = QPainterPath()
        offset_path.addText(cx - half_w + dx, cy + dx, font, text)
        painter.save()
        pen = QPen(shad, stroke_w)
        pen.setJoinStyle(Qt.RoundJoin)
        pen.setCapStyle(Qt.RoundCap)
        painter.setPen(pen)
        painter.setBrush(QBrush(shad))
        painter.drawPath(offset_path)
        painter.restore()
        full_color = QColor(color)
        full_color.setAlpha(255)
        _draw_centred(painter, text, cx, cy, font, full_color, as_paths)
    else:
        _draw_centred(painter, text, cx + dx, cy + dx,
                      font, QColor(0, 0, 0, 170), as_paths)
        _draw_centred(painter, text, cx, cy, font, color, as_paths)


def _fill_offset_shadow(
    painter: QPainter,
    path: QPainterPath,
    *,
    dx: float,
    dy: float,
    color: QColor,
    as_paths: bool,
    soft: bool = False,
) -> None:
    """Draw an offset shadow for a filled shape.

    For SVG, ``soft=True`` uses the sentinel shadow colour so post-processing
    can attach a Gaussian blur. Raster exports get a simple layered shadow.
    """
    if path.isEmpty():
        return
    painter.save()
    if soft and as_paths:
        shadow = QPainterPath(path)
        shadow.translate(dx, dy)
        shad = QColor(1, 0, 0, max(1, color.alpha()))
        painter.fillPath(shadow, QBrush(shad))
        painter.restore()
        return

    layers = 4 if soft else 1
    for i in range(layers):
        t = (i + 1) / float(layers)
        shadow = QPainterPath(path)
        shadow.translate(dx * t, dy * t)
        layer_color = QColor(color)
        layer_color.setAlpha(max(1, int(round(color.alpha() * (0.9 - 0.18 * i)))))
        painter.fillPath(shadow, QBrush(layer_color))
    painter.restore()


def _nice_label(raw: str | None) -> str:
    if not raw:
        return ""
    return raw.replace("_", " ").strip()


def _image_scale_mpp(img_rec: dict | None) -> float | None:
    """Resolve image scale, including calibration-backed microscope images."""
    if not img_rec:
        return None
    direct = img_rec.get("scale_microns_per_pixel")
    if isinstance(direct, (int, float)) and float(direct) > 0:
        return float(direct)
    calibration_id = img_rec.get("calibration_id")
    if calibration_id:
        cal = CalibrationDB.get_calibration(int(calibration_id))
        if cal:
            mpp = cal.get("microns_per_pixel")
            if isinstance(mpp, (int, float)) and float(mpp) > 0:
                return float(mpp)
    objective_name = str(img_rec.get("objective_name") or "").strip()
    if objective_name:
        cal = CalibrationDB.get_active_calibration(objective_name)
        if cal:
            mpp = cal.get("microns_per_pixel")
            if isinstance(mpp, (int, float)) and float(mpp) > 0:
                return float(mpp)
    return None


def _short_objective_label(name: str | None) -> str | None:
    if not name:
        return None
    text = str(name).strip()
    if not text or text.lower() == "custom":
        return None
    match = re.search(r"(\d+(?:\.\d+)?)\s*[xX]", text)
    if match:
        return f"{match.group(1)}X"
    match = re.search(r"(\d+(?:\.\d+)?)", text)
    if match:
        return f"{match.group(1)}X"
    return text


def _is_not_set(canonical: str | None) -> bool:
    """True for the "Not_set" sentinel that DB uses for unset dropdown fields."""
    return canonical is None or canonical == "Not_set"


def _microscope_badge_text(img_rec: dict) -> str:
    objective = (
        _short_objective_label(img_rec.get("objective_name"))
        or _short_objective_label(img_rec.get("magnification"))
        or ""
    )
    contrast_raw = img_rec.get("contrast")
    contrast = "" if _is_not_set(DatabaseTerms.canonicalize_contrast(contrast_raw)) \
        else DatabaseTerms.translate_contrast(contrast_raw)
    return " ".join(p for p in [objective, contrast] if p)


# ── Coordinate mapping ────────────────────────────────────────────────────────
def _map_point_to_circle(px: float, py: float, img_w: int, img_h: int,
                          diam: int, crop: SlotCrop) -> tuple[float, float]:
    """Map original image pixel (px, py) → circle-canvas pixel (0…diam).

    Uses the same source-rect transform as the inset image renderer so the
    overlay stays locked to the exact sampled pixels.
    """
    src_rect = _cover_source_rect(img_w, img_h, diam, diam, crop)
    if src_rect is None or src_rect.width() <= 0 or src_rect.height() <= 0:
        return 0.0, 0.0
    out_x = (float(px) - src_rect.x()) / src_rect.width() * diam
    out_y = (float(py) - src_rect.y()) / src_rect.height() * diam
    return out_x, out_y


# ── Text scale ────────────────────────────────────────────────────────────────
def _remap_ts(ts: float) -> float:
    """Pass-through — slider range [90, 200] ensures ts ≥ 0.90 always."""
    return ts


# ── Measure stroke helpers (shared style with Measure tab) ────────────────────
_MEASURE_PRESETS = (
    {"match": QColor("#1E90FF"), "thin": QColor("#0044aa"), "glow": QColor("#2a7fff"), "opacity": 0.576531, "blend": "screen"},
    {"match": QColor("#3498db"), "thin": QColor("#0044aa"), "glow": QColor("#2a7fff"), "opacity": 0.576531, "blend": "screen"},
    {"match": QColor("#FF3B30"), "thin": QColor("#d40000"), "glow": QColor("#d40000"), "opacity": 0.658163, "blend": "screen"},
    {"match": QColor("#2ECC71"), "thin": QColor("#00aa00"), "glow": QColor("#00aa00"), "opacity": 0.658163, "blend": "screen"},
    {"match": QColor("#E056FD"), "thin": QColor("#ff00ff"), "glow": QColor("#ff00ff"), "opacity": 0.433674, "blend": "screen"},
    {"match": QColor("#ECAF11"), "thin": QColor("#ffd42a"), "glow": QColor("#ffdd55"), "opacity": 0.658163, "blend": "overlay"},
    {"match": QColor("#1CEBEB"), "thin": QColor("#00ffff"), "glow": QColor("#00ffff"), "opacity": 0.658163, "blend": "overlay"},
    {"match": QColor("#000000"), "thin": QColor("#000000"), "glow": QColor("#000000"), "opacity": 0.658163, "blend": "overlay"},
)


def _measure_stroke_style(color=None) -> dict:
    base = QColor(color) if color is not None else QColor("#ffd42a")
    if not base.isValid():
        base = QColor("#ffd42a")
    def _d2(c1, c2):
        return (c1.red()-c2.red())**2 + (c1.green()-c2.green())**2 + (c1.blue()-c2.blue())**2
    chosen = min(_MEASURE_PRESETS, key=lambda p: _d2(base, p["match"]))
    thin = QColor(chosen["thin"])
    thin.setAlpha(max(1, base.alpha()))
    glow = QColor(chosen["glow"])
    glow.setAlpha(max(1, min(255, int(round(255 * float(chosen["opacity"]))))))
    return {"thin": thin, "glow": glow, "blend": str(chosen["blend"])}


def _set_composition_mode(painter: QPainter, blend: str) -> None:
    b = blend.lower().strip()
    if b == "overlay":
        painter.setCompositionMode(QPainter.CompositionMode_Overlay)
    elif b == "screen":
        painter.setCompositionMode(QPainter.CompositionMode_Screen)
    elif b == "plus":
        painter.setCompositionMode(QPainter.CompositionMode_Plus)
    elif b == "lighten":
        painter.setCompositionMode(QPainter.CompositionMode_Lighten)


def _dual_stroke_polygon(painter: QPainter, poly: QPolygonF, color=None,
                         thin_w: float = 1.0, wide_w: Optional[float] = None) -> None:
    style = _measure_stroke_style(color)
    thin_pen = QPen(style["thin"], max(1.0, thin_w))
    wide_pen = QPen(style["glow"], max(thin_w * 3.0, float(wide_w or 0.0), 1.0))
    painter.save()
    _set_composition_mode(painter, style["blend"])
    painter.setPen(wide_pen)
    painter.setBrush(Qt.NoBrush)
    painter.drawPolygon(poly)
    painter.restore()
    painter.setBrush(Qt.NoBrush)
    painter.setPen(thin_pen)
    painter.drawPolygon(poly)


def _dual_stroke_line(painter: QPainter, a: QPointF, b: QPointF, color=None,
                      thin_w: float = 1.0, wide_w: Optional[float] = None) -> None:
    style = _measure_stroke_style(color)
    thin_pen = QPen(style["thin"], max(1.0, thin_w))
    wide_pen = QPen(style["glow"], max(thin_w * 3.0, float(wide_w or 0.0), 1.0))
    painter.save()
    _set_composition_mode(painter, style["blend"])
    painter.setPen(wide_pen)
    painter.drawLine(a, b)
    painter.restore()
    painter.setPen(thin_pen)
    painter.drawLine(a, b)


def _draw_rotated_measure_label(painter: QPainter, text: str,
                                 a: QPointF, b: QPointF,
                                 text_color: QColor,
                                 center: Optional[QPointF] = None,
                                 padding: float = 4.0) -> None:
    """Draw text rotated along the a→b axis, offset outward from center."""
    dx = b.x() - a.x()
    dy = b.y() - a.y()
    length = math.sqrt(dx * dx + dy * dy)
    if length <= 0:
        return
    angle_deg = math.degrees(math.atan2(dy, dx))
    if angle_deg > 90 or angle_deg < -90:
        angle_deg += 180
    mid = QPointF((a.x() + b.x()) / 2, (a.y() + b.y()) / 2)
    perp_x = -dy / length
    perp_y = dx / length
    metrics = painter.fontMetrics()
    text_w = metrics.horizontalAdvance(text)
    text_h = metrics.height()
    offset = text_h / 2 + padding
    if center is not None:
        dot = (mid.x() - center.x()) * perp_x + (mid.y() - center.y()) * perp_y
        sign = 1 if dot >= 0 else -1
        lpos = QPointF(mid.x() + perp_x * offset * sign, mid.y() + perp_y * offset * sign)
    else:
        lpos = QPointF(mid.x() + perp_x * offset, mid.y() + perp_y * offset)
    # White outline underlay (same as Measure tab's _draw_measure_text_with_outline)
    path = QPainterPath()
    path.addText(float(-text_w / 2), float(text_h / 2), painter.font(), text)
    stroke_w = max(1.0, text_h * 0.4)
    halo_pen = QPen(QColor(255, 255, 255, 102), stroke_w)
    halo_pen.setJoinStyle(Qt.RoundJoin)
    halo_pen.setCapStyle(Qt.RoundCap)
    painter.save()
    painter.translate(lpos.x(), lpos.y())
    painter.rotate(angle_deg)
    painter.setBrush(Qt.NoBrush)
    painter.setPen(halo_pen)
    painter.drawPath(path)
    painter.setPen(text_color)
    painter.drawText(int(-text_w / 2), int(text_h / 2), text)
    painter.restore()


# ── SVG soft-shadow post-processor ────────────────────────────────────────────
def _softshadow_postprocess_svg(path: str) -> None:
    """Inject Gaussian blur into soft-shadow elements in an exported SVG.

    During soft-shadow rendering, shadow paths/lines are drawn with the sentinel
    fill/stroke colour #010000 (QColor(1,0,0,180)).  This function:
      1. Adds a <filter id="soft_shadow_blur"> with feGaussianBlur to <defs>.
      2. Changes every sentinel #010000 fill/stroke to #000000.
      3. Attaches the filter to those elements.
    """
    import re as _re
    import xml.etree.ElementTree as ET

    SENTINEL = "#010000"
    FILTER_ID = "soft_shadow_blur"
    # stdDeviation proportional to a typical label font size (24pt native canvas)
    std_dev = 24 * 0.16

    try:
        tree = ET.parse(path)
    except ET.ParseError:
        return

    root = tree.getroot()
    ns_match = _re.match(r'\{([^}]+)\}', root.tag)
    ns_uri = ns_match.group(1) if ns_match else "http://www.w3.org/2000/svg"
    ns = f"{{{ns_uri}}}"

    # Find or create <defs>
    defs = root.find(f"{ns}defs")
    if defs is None:
        defs = ET.Element(f"{ns}defs")
        root.insert(0, defs)

    # Add blur filter
    filt = ET.SubElement(defs, f"{ns}filter")
    filt.set("id", FILTER_ID)
    filt.set("x", "-20%")
    filt.set("y", "-20%")
    filt.set("width", "140%")
    filt.set("height", "140%")
    gaussian = ET.SubElement(filt, f"{ns}feGaussianBlur")
    gaussian.set("in", "SourceGraphic")
    gaussian.set("stdDeviation", f"{std_dev:.3f}")

    # Walk all elements and fix sentinel colour + apply filter
    found_any = False
    for elem in root.iter():
        style = elem.get("style", "")
        fill = elem.get("fill", "")
        stroke = elem.get("stroke", "")
        is_shadow = (
            fill == SENTINEL or stroke == SENTINEL or
            f"fill:{SENTINEL}" in style or f"stroke:{SENTINEL}" in style
        )
        if not is_shadow:
            continue
        found_any = True
        if fill == SENTINEL:
            elem.set("fill", "#000000")
        if stroke == SENTINEL:
            elem.set("stroke", "#000000")
        if style:
            style = style.replace(f"fill:{SENTINEL}", "fill:#000000")
            style = style.replace(f"stroke:{SENTINEL}", "stroke:#000000")
            elem.set("style", style)
        elem.set("filter", f"url(#{FILTER_ID})")

    if not found_any:
        # No sentinel elements — remove the filter we added to keep SVG clean
        defs.remove(filt)
        if len(defs) == 0:
            root.remove(defs)

    ET.register_namespace("", ns_uri)
    ET.register_namespace("xlink", "http://www.w3.org/1999/xlink")
    tree.write(path, encoding="unicode", xml_declaration=False)


# ── Scale bar ─────────────────────────────────────────────────────────────────
_NICE_BARS_UM = [1, 2, 5, 10, 20, 50, 100, 200, 500]
_NICE_BARS_MM = [0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50]       # in mm (= 100…50000 µm)

# Shape slider: 40 steps total.
#   Steps  0-19: superellipse (circle → squarish), exponents log-spaced 2→7.
#                n is capped at 7 — beyond that the single-Bézier-per-quadrant
#                approximation overshoots and creates a pincushion artefact.
#   Steps 20-39: rounded rectangle (large radius → sharp corners).
# Formula: |x|^n + |y|^n ≤ r^n.  n=2 → circle, n=4 → squircle.
_SE_STEPS = 20          # number of superellipse steps
_RR_STEPS = 20          # number of rounded-rect steps

# Log-spaced n values from 2.0 to 7.0 over 20 steps.
# k = (8·2^(-1/n) − 4)/3 stays below 1.2 throughout, keeping the Bézier well-behaved.
_SE_EXPS: list[float] = [2.0 * (7.0 / 2.0) ** (i / (_SE_STEPS - 1))
                         for i in range(_SE_STEPS)]
_SHAPE_STEPS = _SE_STEPS + _RR_STEPS   # total slider range = 40


def _draw_scale_bar_at(painter: QPainter, bar_cx: float, bar_by: float,
                        img_rec: dict, crop: SlotCrop,
                        ref_width: float, as_paths: bool, ts: float,
                        target_ratio: float = 0.20,
                        use_mm: bool = False,
                        left_align: bool = False,
                        soft_shadow: bool = False,
                        ref_height: float = 0.0) -> None:
    """Draw a scale bar horizontally centred at (bar_cx, bar_by).

    ``ref_width`` / ``ref_height`` are the display dimensions of the area being
    filled by the image (e.g. circle diameter × diameter, or panel width × height).
    They determine the cover-crop scale factor, so they must match whatever
    ``_cover_crop`` / ``_rect_image`` used.  If ``ref_height`` is 0, it falls back
    to ``ref_width`` (square areas such as circle insets).
    ``target_ratio`` controls the desired bar length as a fraction of ``ref_width``.
    ``use_mm`` selects mm nice-values and the "mm" unit label.
    When ``left_align`` is True, ``bar_cx`` is treated as the left edge of the bar.
    """
    spp = _image_scale_mpp(img_rec)
    if not spp:
        return
    spp = float(spp)
    if spp <= 0:
        return
    fp = img_rec.get("filepath", "")
    if not fp or not os.path.isfile(fp):
        return
    src = _load_pix(fp)
    if src.isNull():
        return
    rh = ref_height if ref_height > 0 else ref_width
    cover_s = max(ref_width / src.width(), rh / src.height()) if (src.width() and src.height()) else 1.0
    um_per_px = spp / (cover_s * max(crop.zoom, 1.0))

    target_um = (ref_width * target_ratio) * um_per_px
    if use_mm:
        target_mm = target_um / 1000.0
        bar_mm = min(_NICE_BARS_MM, key=lambda v: abs(v - target_mm))
        bar_um = bar_mm * 1000.0
        lbl = f"{bar_mm:g} mm"
    else:
        bar_um = min(_NICE_BARS_UM, key=lambda v: abs(v - target_um))
        lbl = f"{bar_um} µm"
    bar_px = bar_um / um_per_px

    bx = bar_cx if left_align else bar_cx - bar_px / 2
    lbl_cx = bx + bar_px / 2
    tick = 10

    painter.save()
    if soft_shadow:
        # Draw offset shadow bar first (sentinel colour #010000 → SVG blur post-proc)
        sdx = 2.0
        shad = QColor(1, 0, 0, 180)
        sh_pen = QPen(shad, 8)
        sh_pen.setCapStyle(Qt.FlatCap)
        painter.setPen(sh_pen)
        painter.drawLine(QPointF(bx + sdx, bar_by + sdx),
                         QPointF(bx + bar_px + sdx, bar_by + sdx))
        sh_tick_pen = QPen(shad, 3.5)
        sh_tick_pen.setCapStyle(Qt.FlatCap)
        painter.setPen(sh_tick_pen)
        for tx in (bx, bx + bar_px):
            painter.drawLine(QPointF(tx + sdx, bar_by - tick // 2 + sdx),
                             QPointF(tx + sdx, bar_by + tick // 2 + sdx))
    pen = QPen(QColor(255, 255, 255, 220), 5)
    pen.setCapStyle(Qt.FlatCap)
    painter.setPen(pen)
    painter.drawLine(QPointF(bx, bar_by), QPointF(bx + bar_px, bar_by))
    tick_pen = QPen(QColor(255, 255, 255, 220), 2)
    tick_pen.setCapStyle(Qt.FlatCap)
    painter.setPen(tick_pen)
    for tx in (bx, bx + bar_px):
        painter.drawLine(QPointF(tx, bar_by - tick // 2), QPointF(tx, bar_by + tick // 2))
    painter.restore()

    lf = _make_font(["Helvetica Neue", "Helvetica", "Arial"], 24 * _remap_ts(ts))
    _shadow_text(painter, lbl, lbl_cx, bar_by - 14, lf, QColor(255, 255, 255, 210),
                 as_paths, dx=1.5, soft=soft_shadow)


def _draw_scale_bar(painter: QPainter, cx: float, cy: float, r: int,
                    img_rec: dict, crop: SlotCrop,
                    as_paths: bool, ts: float,
                    soft_shadow: bool = False,
                    canvas_w: float = _W, canvas_h: float = _H) -> None:
    """Draw scale bar inside a circle inset (bottom area).

    When the inset extends outside the canvas the bar position is clamped so it
    remains visible.  The horizontal clamp assumes the bar is ≤ 50 % of the
    inset diameter (target_ratio = 0.20 → half-bar ≈ 0.20 * r).
    """
    use_mm = (str(img_rec.get("image_type") or "").strip().lower() == "field")
    bar_by = min(cy + r - 32, canvas_h - 48)
    bar_cx = max(r * 0.22, min(cx, canvas_w - r * 0.22))
    _draw_scale_bar_at(painter, bar_cx, bar_by, img_rec, crop,
                       r * 2, as_paths, ts, use_mm=use_mm, soft_shadow=soft_shadow)


# ── Superellipse path ─────────────────────────────────────────────────────────
def _superellipse_path(cx: float, cy: float, r: float, n: float) -> QPainterPath:
    """Return a QPainterPath for a superellipse centred at (cx, cy) with radius r.

    Uses 4 cubic Bézier segments (one per quadrant).  The control-point factor k
    is derived by matching the Bézier midpoint to the true superellipse midpoint
    at the 45° parameter angle:  k = (8·2^(-1/n) − 4) / 3.
    At n=2 this yields the standard circle approximation k ≈ 0.5523.
    """
    if n <= 2.0:
        path = QPainterPath()
        path.addEllipse(QPointF(cx, cy), r, r)
        return path
    k = (8.0 * pow(2.0, -1.0 / n) - 4.0) / 3.0
    kr = k * r
    path = QPainterPath()
    path.moveTo(cx + r, cy)
    path.cubicTo(cx + r,  cy + kr, cx + kr, cy + r,  cx,      cy + r)
    path.cubicTo(cx - kr, cy + r,  cx - r,  cy + kr, cx - r,  cy)
    path.cubicTo(cx - r,  cy - kr, cx - kr, cy - r,  cx,      cy - r)
    path.cubicTo(cx + kr, cy - r,  cx + r,  cy - kr, cx + r,  cy)
    path.closeSubpath()
    return path


def _inset_shape_path(cx: float, cy: float, r: float, step: int) -> QPainterPath:
    """Unified shape path for inset circles controlled by the Shape slider.

    Steps 0-19  → superellipse (n from _SE_EXPS: circle → near-square).
    Steps 20-39 → rounded rectangle (corner radius r·0.4 → 0 = sharp square).
    """
    if step < _SE_STEPS:
        return _superellipse_path(cx, cy, r, _SE_EXPS[step])
    # Rounded rectangle: t goes from 1.0 (step 20) to 0.0 (step 39)
    t = (_SHAPE_STEPS - 1 - step) / float(_RR_STEPS - 1)
    cr = r * t * 0.4
    path = QPainterPath()
    path.addRoundedRect(QRectF(cx - r, cy - r, r * 2, r * 2), cr, cr)
    return path


# ── Measurement overlay ───────────────────────────────────────────────────────
def _build_measurement_rect_corners(m: dict) -> Optional[list[QPointF]]:
    """Build 4 canvas-space corners of a measurement bounding rectangle.

    Measurements store two perpendicular lines:
      line1 = p1→p2 (length),  line2 = p3→p4 (width).
    Returns None if the measurement only has one line (line only).
    """
    vals = [m.get(k) for k in ("p1_x", "p1_y", "p2_x", "p2_y",
                                "p3_x", "p3_y", "p4_x", "p4_y")]
    if any(v is None for v in vals):
        return None
    p1 = QPointF(float(vals[0]), float(vals[1]))
    p2 = QPointF(float(vals[2]), float(vals[3]))
    p3 = QPointF(float(vals[4]), float(vals[5]))
    p4 = QPointF(float(vals[6]), float(vals[7]))
    length_vec = p2 - p1
    length_len = math.sqrt(length_vec.x() ** 2 + length_vec.y() ** 2)
    width_vec = p4 - p3
    width_len = math.sqrt(width_vec.x() ** 2 + width_vec.y() ** 2)
    if length_len < 0.001 or width_len < 0.001:
        return None
    length_dir = QPointF(length_vec.x() / length_len, length_vec.y() / length_len)
    width_dir = QPointF(-length_dir.y(), length_dir.x())
    line1_mid = QPointF((p1.x() + p2.x()) / 2, (p1.y() + p2.y()) / 2)
    line2_mid = QPointF((p3.x() + p4.x()) / 2, (p3.y() + p4.y()) / 2)
    center = QPointF((line1_mid.x() + line2_mid.x()) / 2,
                     (line1_mid.y() + line2_mid.y()) / 2)
    hl, hw = length_len / 2, width_len / 2
    return [
        center - width_dir * hw - length_dir * hl,
        center + width_dir * hw - length_dir * hl,
        center + width_dir * hw + length_dir * hl,
        center - width_dir * hw + length_dir * hl,
    ]


def _draw_measurements_on_circle(painter: QPainter, measurements: list[dict],
                                  img_w: int, img_h: int,
                                  diam: int, crop: SlotCrop,
                                  cx: float, cy: float, r: int,
                                  ts: float, color=None,
                                  shape_step: int = 0) -> None:
    """Draw measurement rectangles (or lines) clipped to the circle.

    Reuses the same dual-stroke style and rotated-label placement as the
    Measure tab (zoomable_image_widget._draw_rotated_label_outside).
    ``color`` is the image's ``measure_color`` string from ImageDB.
    """
    if not measurements or img_w == 0 or img_h == 0:
        return

    ts_c = _remap_ts(ts)

    def _map(px, py) -> QPointF:
        ox, oy = _map_point_to_circle(float(px), float(py), img_w, img_h, diam, crop)
        return QPointF(cx - r + ox, cy - r + oy)

    painter.save()
    clip = _inset_shape_path(cx, cy, r - _INS_STROKE / 2, shape_step)
    painter.setClipPath(clip)
    painter.setRenderHint(QPainter.Antialiasing)

    # Text colour: slightly darker than line colour for contrast
    style = _measure_stroke_style(color)
    thin_col = style["thin"]
    text_col = QColor(thin_col)
    h, s, v, a = (text_col.hsvHue(), text_col.hsvSaturation(),
                  text_col.value(), text_col.alpha())
    text_col.setHsv(h, min(255, int(s * 1.05)), max(0, int(v * 0.68)), a)

    # Use thin lines — same numeric value as the Measure tab (1.0 world px).
    # The preview painter has a 0.40 scale so 1.0 → 0.4px physical; the glow
    # at 3× = 1.2px physical.  Looks proportionally thin in the small circles.
    line_w = 1.0
    lbl_font = _make_font(["Helvetica Neue", "Helvetica", "Arial"], 20 * ts_c)
    pad = max(3.0, ts_c * 3.0)

    for m in measurements:
        raw_corners = _build_measurement_rect_corners(m)
        if raw_corners is not None:
            mapped = [_map(c.x(), c.y()) for c in raw_corners]
            poly = QPolygonF(mapped)
            _dual_stroke_polygon(painter, poly, color, thin_w=line_w)

            l_um = m.get("length_um")
            w_um = m.get("width_um")
            if l_um is not None:
                lv = float(l_um)
                wv = float(w_um) if w_um is not None else None
                cpt = QPointF(sum(p.x() for p in mapped) / 4,
                              sum(p.y() for p in mapped) / 4)
                # Find the rectangle edge most aligned with line1 (length axis)
                # — identical logic to zoomable_image_widget._draw_rotated_label_outside
                p1x, p1y = m.get("p1_x"), m.get("p1_y")
                p2x, p2y = m.get("p2_x"), m.get("p2_y")
                edges = [
                    (mapped[0], mapped[1]),
                    (mapped[1], mapped[2]),
                    (mapped[2], mapped[3]),
                    (mapped[3], mapped[0]),
                ]
                best = 0
                if p1x is not None and p2x is not None:
                    lv1 = QPointF(float(p2x) - float(p1x), float(p2y) - float(p1y))
                    ll = math.sqrt(lv1.x()**2 + lv1.y()**2)
                    if ll > 0:
                        lv1 = QPointF(lv1.x() / ll, lv1.y() / ll)
                        best_score = -1.0
                        for idx, (ea, eb) in enumerate(edges):
                            ev = QPointF(eb.x() - ea.x(), eb.y() - ea.y())
                            el = math.sqrt(ev.x()**2 + ev.y()**2)
                            if el <= 0:
                                continue
                            score = abs(ev.x() / el * lv1.x() + ev.y() / el * lv1.y())
                            if score > best_score:
                                best_score = score
                                best = idx
                len_edge = edges[best]
                wid_edge = edges[(best + 1) % 4]
                painter.setFont(lbl_font)
                # Only draw labels whose edge midpoint is inside the inset area.
                # This prevents labels from escaping the clip in SVG export
                # when measurements are partially or fully outside the visible crop.
                _label_r2 = (r * 1.15) ** 2
                def _mid_in(ea: QPointF, eb: QPointF) -> bool:
                    mx = (ea.x() + eb.x()) / 2 - cx
                    my = (ea.y() + eb.y()) / 2 - cy
                    return mx * mx + my * my <= _label_r2
                if _mid_in(len_edge[0], len_edge[1]):
                    _draw_rotated_measure_label(
                        painter, f"{lv:.1f}",
                        len_edge[0], len_edge[1], text_col, cpt, padding=pad)
                if wv is not None and _mid_in(wid_edge[0], wid_edge[1]):
                    _draw_rotated_measure_label(
                        painter, f"{wv:.1f}",
                        wid_edge[0], wid_edge[1], text_col, cpt, padding=pad)
        else:
            p1x, p1y = m.get("p1_x"), m.get("p1_y")
            p2x, p2y = m.get("p2_x"), m.get("p2_y")
            if p1x is None or p2x is None:
                continue
            a_pt, b_pt = _map(p1x, p1y), _map(p2x, p2y)
            _dual_stroke_line(painter, a_pt, b_pt, color, thin_w=line_w)
            l_um = m.get("length_um")
            if l_um is not None:
                mid_x = (a_pt.x() + b_pt.x()) / 2 - cx
                mid_y = (a_pt.y() + b_pt.y()) / 2 - cy
                if mid_x * mid_x + mid_y * mid_y <= (r * 1.15) ** 2:
                    painter.setFont(lbl_font)
                    _draw_rotated_measure_label(
                        painter, f"{float(l_um):.1f}",
                        a_pt, b_pt, text_col,
                        padding=pad)

    painter.restore()


# ── Interactive preview canvas ────────────────────────────────────────────────
class PlatePreviewCanvas(QWidget):

    _LABEL_BOX_W = 420
    _LABEL_BOX_H = 46
    _LABEL_GAP   = 26   # inset from circle top when label is drawn inside

    def __init__(self, dialog: "SpeciesPlateDialog", parent=None):
        super().__init__(parent)
        self._dlg = dialog
        self.setFixedSize(int(_W * _PREVIEW_SCALE), int(_H * _PREVIEW_SCALE))
        self.setMouseTracking(True)
        self.setFocusPolicy(Qt.StrongFocus)

        self._active: Optional[str] = None
        self._drag_start: Optional[QPoint] = None
        self._drag_base: Optional[SlotCrop] = None
        self._drag_circle_slot: Optional[str] = None          # overlay: dragging circle position
        self._drag_circle_base: Optional[tuple[float, float]] = None  # native cx,cy at drag start
        self._resize_slot: Optional[str] = None               # corner resize drag
        self._resize_corner: Optional[str] = None             # "TL"/"TR"/"BL"/"BR"
        self._resize_ins_r_base: int = 0
        self._resize_r_base: float = 0.0
        self._resize_cx: float = 0.0
        self._resize_cy: float = 0.0
        self._pm: Optional[QPixmap] = None

        # ── Inline label overlay ───────────────────────────────────────────
        from PySide6.QtWidgets import QLineEdit as _QLineEdit
        self._label_overlay = _QLineEdit(self)
        self._label_overlay.hide()
        self._label_overlay.setAlignment(Qt.AlignCenter)
        self._label_overlay_slot: Optional[str] = None
        self._label_overlay.returnPressed.connect(self._commit_label)
        # Note: do NOT connect editingFinished — it fires on every focus-loss
        # including when the dialog's key handler commits, causing double calls.

    def set_pixmap(self, pm: QPixmap) -> None:
        self._pm = pm
        self.update()

    def paintEvent(self, _) -> None:
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        if self._pm:
            p.drawPixmap(0, 0, self._pm)

        # ── Placeholder rings for empty circle slots ───────────────────────
        active_slots = self._active_slot_keys()
        for slot in active_slots:
            if self._dlg._slot_images.get(slot):
                continue
            cx, cy = self._preview_centre(slot)
            r = self._preview_slot_r(slot)
            is_active = (self._active == slot)
            color = QColor(255, 200, 50, 120) if is_active else QColor(255, 255, 255, 35)
            p.setPen(QPen(color, 2, Qt.DashLine))
            p.setBrush(Qt.NoBrush)
            p.drawPath(_inset_shape_path(cx, cy, r, self._dlg._se_step))

        # ── Active-selection highlight ─────────────────────────────────────
        active = self._active_key(self._active)  # strip any stale prefix
        if active:
            pen = QPen(QColor(255, 200, 50, 230), 3)
            p.setPen(pen)
            p.setBrush(Qt.NoBrush)
            if active.startswith("bg:"):
                bg_path = self._preview_background_path(active)
                if bg_path is not None:
                    p.save()
                    mask = QPainterPath()
                    mask.addRect(QRectF(self.rect()))
                    for slot in active_slots:
                        cx, cy = self._preview_centre(slot)
                        r = self._preview_slot_r(slot)
                        mask.addPath(_inset_shape_path(cx, cy, r + 3, self._dlg._se_step))
                    mask.setFillRule(Qt.OddEvenFill)
                    p.setClipPath(mask)
                    p.drawPath(bg_path)
                    p.restore()
            elif active in active_slots:
                cx, cy = self._preview_centre(active)
                r = self._preview_slot_r(active)
                p.drawPath(_inset_shape_path(cx, cy, r + 5, self._dlg._se_step))

        # ── Corner resize handles ───────────────────────────────────────────
        for slot in active_slots:
            is_active_slot = (self._active == slot)
            alpha = 200 if is_active_slot else 90
            for corner, rect in self._corner_handle_rects(slot).items():
                p.setPen(QPen(QColor(255, 255, 255, alpha), 1.5))
                p.setBrush(QBrush(QColor(220, 220, 220, alpha // 2)))
                p.drawRoundedRect(rect, 2, 2)

        # ── Red X buttons for filled circle slots ──────────────────────────
        for slot in active_slots:
            if not self._dlg._slot_images.get(slot):
                continue
            xr = self._clear_btn_rect_preview(slot)
            # Disc
            p.setPen(QPen(QColor(255, 255, 255, 200), 1.2))
            p.setBrush(QBrush(QColor(210, 45, 45, 235)))
            p.drawEllipse(xr)
            # × strokes — thick with round caps for clarity
            inset = xr.width() * 0.28
            pen_x = QPen(QColor(255, 255, 255, 245), 2.2)
            pen_x.setCapStyle(Qt.RoundCap)
            p.setPen(pen_x)
            p.drawLine(QPointF(xr.left() + inset, xr.top() + inset),
                       QPointF(xr.right() - inset, xr.bottom() - inset))
            p.drawLine(QPointF(xr.right() - inset, xr.top() + inset),
                       QPointF(xr.left() + inset, xr.bottom() - inset))

        p.end()

    def _is_overlay(self) -> bool:
        return self._dlg._inset_layout == "3"

    def _active_slot_keys(self) -> list[str]:
        return _OVERLAY_SLOT_KEYS if self._is_overlay() else _SLOT_KEYS

    def _preview_centre(self, slot: str) -> tuple[float, float]:
        if self._is_overlay():
            params = _overlay_slot_params(self._dlg._ins_r, self._dlg._overlay_positions, self._dlg._overlay_radii)
            cx, cy, _ = params.get(slot, (_W / 2, _H / 2, self._dlg._ins_r))
        else:
            t = self._dlg._ins_margin / 100.0
            cx, cy = _slot_centre_pos(self._dlg._ins_r, slot, t)
        return cx * _PREVIEW_SCALE, cy * _PREVIEW_SCALE

    def _preview_slot_r(self, slot: str) -> float:
        """Preview-scale radius for this slot (varies per slot in overlay mode)."""
        if self._is_overlay():
            params = _overlay_slot_params(self._dlg._ins_r, self._dlg._overlay_positions, self._dlg._overlay_radii)
            _, _, r = params.get(slot, (_W / 2, _H / 2, self._dlg._ins_r))
        else:
            r = self._dlg._ins_r
        return r * _PREVIEW_SCALE

    _CORNER_HS = 6    # corner handle half-size in preview px

    def _corner_handle_rects(self, slot: str) -> dict[str, QRectF]:
        """Return bounding-box corner resize handles (TL/TR/BL/BR) in preview px."""
        cx, cy = self._preview_centre(slot)
        r = self._preview_slot_r(slot)
        hs = self._CORNER_HS
        return {
            "TL": QRectF(cx - r - hs, cy - r - hs, hs * 2, hs * 2),
            "TR": QRectF(cx + r - hs, cy - r - hs, hs * 2, hs * 2),
            "BL": QRectF(cx - r - hs, cy + r - hs, hs * 2, hs * 2),
            "BR": QRectF(cx + r - hs, cy + r - hs, hs * 2, hs * 2),
        }

    def _clear_btn_rect_preview(self, slot: str) -> QRectF:
        """Small red-X button just left of the top-right corner resize handle.

        Falls back to clamping within the canvas when the TR corner is near the edge.
        """
        cx, cy = self._preview_centre(slot)
        r = self._preview_slot_r(slot)
        br = 9   # button half-size in preview px
        hs = self._CORNER_HS
        gap = 5
        # TR corner handle sits at (cx+r, cy-r); X goes to its left
        bx = cx + r + hs + gap + br   # default: right of TR handle (stays near the corner)
        by = cy - r
        # Clamp so the button stays inside the canvas
        cw, ch = self.width(), self.height()
        # If there's not enough room to the right, flip to left side of TR handle
        if bx + br > cw - 2:
            bx = cx + r - hs - gap - br
        bx = max(br + 2, min(bx, cw - br - 2))
        by = max(br + 2, min(by, ch - br - 2))
        return QRectF(bx - br, by - br, br * 2, br * 2)

    def _label_rect_preview(self, slot: str) -> QRectF:
        """Return the inside-top label rect in preview canvas coordinates."""
        cx, cy = self._preview_centre(slot)
        r = self._preview_slot_r(slot)
        bw = self._LABEL_BOX_W * _PREVIEW_SCALE
        bh = self._LABEL_BOX_H * _PREVIEW_SCALE
        gap = self._LABEL_GAP * _PREVIEW_SCALE
        return QRectF(cx - bw / 2, cy - r + gap, bw, bh)

    def _hit(self, pos: QPoint) -> Optional[str]:
        pt = QPointF(pos)
        active_slots = self._active_slot_keys()
        # 1. Red X clear buttons (only for filled circles)
        for slot in active_slots:
            if not self._dlg._slot_images.get(slot):
                continue
            if self._clear_btn_rect_preview(slot).contains(pt):
                return f"clear:{slot}"
        # 2. Corner resize handles (all slots, filled or empty)
        for slot in active_slots:
            for corner, rect in self._corner_handle_rects(slot).items():
                if rect.contains(pt):
                    return f"resize:{slot}:{corner}"
        # 3. Label boxes above filled circles
        for slot in active_slots:
            if not self._dlg._slot_images.get(slot):
                continue
            if self._label_rect_preview(slot).contains(pt):
                return f"label:{slot}"
        # 4. Border ring — only in overlay mode; triggers circle-position drag.
        # Use shape containment so the hit ring follows the actual shape boundary
        # (not just the bounding circle), which fixes detection on superellipses
        # and rectangles where the boundary is non-circular.
        if self._is_overlay():
            ss = self._dlg._se_step
            for slot in active_slots:
                cx, cy = self._preview_centre(slot)
                r = self._preview_slot_r(slot)
                outer = _inset_shape_path(cx, cy, r + _BORDER_HIT_PX, ss)
                inner = _inset_shape_path(cx, cy, max(1.0, r - _BORDER_HIT_PX), ss)
                if outer.contains(pt) and not inner.contains(pt):
                    return f"border:{slot}"
        # 5. Circle insets (filled and empty — both selectable)
        for slot in active_slots:
            cx, cy = self._preview_centre(slot)
            r = self._preview_slot_r(slot)
            dx, dy = pos.x() - cx, pos.y() - cy
            if dx * dx + dy * dy <= r * r:
                return slot
        # 6. Background panels (all, including empty)
        for key, path in self._preview_background_paths():
            if path.contains(pt):
                return key
        return None

    def _preview_background_paths(self) -> list[tuple[str, QPainterPath]]:
        """All bg panel paths at preview scale — includes empty slots."""
        content_rect = _content_rect_for_insets(self._dlg._ins_r, self._dlg._inset_position)
        shapes = _scaled_bg_panel_shapes(self._dlg._bg_layout, content_rect,
                                         self._dlg._tilt_angles)
        scale = QTransform.fromScale(_PREVIEW_SCALE, _PREVIEW_SCALE)
        return [(f"bg:{i}", scale.map(clip_path)) for i, (_, clip_path) in enumerate(shapes)]

    def _preview_background_path(self, key: str) -> Optional[QPainterPath]:
        for bg_key, path in self._preview_background_paths():
            if bg_key == key:
                return path
        return None

    def _active_crop(self) -> Optional[SlotCrop]:
        if not self._active:
            return None
        if self._active.startswith("bg:"):
            return self._dlg._background_crops.get(self._active)
        return self._dlg._crops.get(self._active)

    def _active_label(self, key: str) -> str:
        if key.startswith("bg:"):
            return self._dlg._background_label(key)
        return self._dlg._slot_labels.get(key, key)

    def show_label_overlay(self, slot: str) -> None:
        """Position and show the inline label editor over the circle's label box."""
        rect = self._label_rect_preview(slot)
        self._label_overlay.setGeometry(
            int(rect.x()), int(rect.y()), int(rect.width()), int(rect.height()))
        # Font size: match the rendered label (36pt native × text_scale × preview scale)
        pt = max(8, round(self._dlg._ts(36) * _PREVIEW_SCALE))
        self._label_overlay.setStyleSheet(
            f"QLineEdit {{ background: rgba(0,0,0,100); border: 2px solid #ffd700; "
            f"border-radius: 4px; color: white; font-size: {pt}pt; "
            f"font-family: 'Helvetica Neue', Arial, sans-serif; font-weight: bold; "
            f"padding: 0px 6px; }}"
        )
        self._label_overlay.setText(self._dlg._custom_labels.get(slot, ""))
        self._label_overlay.setPlaceholderText(self._dlg.tr("Type label…"))
        self._label_overlay_slot = slot
        self._label_overlay.show()
        self._label_overlay.raise_()
        self._label_overlay.setFocus()
        self._label_overlay.selectAll()

    def _commit_label(self) -> None:
        if self._label_overlay_slot:
            self._dlg._custom_labels[self._label_overlay_slot] = self._label_overlay.text()
            self._dlg._refresh_preview()
        self._label_overlay.hide()
        self._label_overlay_slot = None

    def mousePressEvent(self, event) -> None:
        if event.button() == Qt.LeftButton:
            self.setFocus()
            # Commit any open label editor before processing click
            if self._label_overlay.isVisible():
                self._commit_label()
            key = self._hit(event.pos())
            # Clear button → remove image from slot
            if key and key.startswith("clear:"):
                slot = key[6:]
                self._dlg._slot_images[slot] = None
                if self._active == slot:
                    self._active = None
                self._dlg._refresh_gallery()
                self._dlg._refresh_preview()
                self.update()
                return
            # Corner resize handle → resize inset
            if key and key.startswith("resize:"):
                _, slot, corner = key.split(":", 2)
                self._active = slot
                self._dlg._on_canvas_selection_changed(slot)
                self._resize_slot = slot
                self._resize_corner = corner
                self._resize_ins_r_base = self._dlg._ins_r
                self._resize_r_base = self._preview_slot_r(slot)
                self._resize_cx, self._resize_cy = self._preview_centre(slot)
                self._drag_start = event.pos()
                self._drag_base = None
                self.update()
                return
            # Border ring (overlay) → drag circle position
            if key and key.startswith("border:"):
                slot = key[7:]
                self._active = slot
                self._dlg._on_canvas_selection_changed(slot)
                ov = _overlay_slot_params(self._dlg._ins_r, self._dlg._overlay_positions, self._dlg._overlay_radii)
                cx, cy, _ = ov[slot]
                self._drag_circle_slot = slot
                self._drag_circle_base = (cx, cy)
                self._drag_start = event.pos()
                self._drag_base = None
                self._dlg._hint_ctrl.set_hint(
                    self._dlg.tr("Drag to reposition shape"), "info")
                self.update()
                return
            # Label rect → open inline editor, activate the circle slot
            if key and key.startswith("label:"):
                slot = key[6:]
                self._active = slot
                self._dlg._on_canvas_selection_changed(slot)
                self.show_label_overlay(slot)
                self.update()
                return
            self._active = key
            self._dlg._on_canvas_selection_changed(key)
            if key:
                sc = self._active_crop()
                if sc is None:
                    self._drag_start = None
                    self._drag_base = None
                    self.update()
                    return
                self._drag_start = event.pos()
                self._drag_base = SlotCrop(sc.zoom, sc.off_x, sc.off_y)
                self._dlg._hint_ctrl.set_hint(
                    self._dlg.tr("Pan / zoom active — {zoom:.1f}× — {del_hint} to delete").format(
                        zoom=sc.zoom, del_hint=_del_hint()),
                    "info",
                )
            else:
                self._drag_start = None
                self._drag_base = None
                self._dlg._hint_ctrl.set_hint(
                    self._dlg.tr("Click a circle or background panel to select, then pick from gallery or drag to pan"),
                    "info")
            self.update()

    def mouseMoveEvent(self, event) -> None:
        if event.buttons() & Qt.LeftButton and self._drag_start:
            d = event.pos() - self._drag_start
            if self._resize_slot and self._resize_corner:
                # Corner handle drag → resize inset
                # Project movement onto the corner's outward diagonal to get a 1-D resize delta.
                # Corner outward directions (screen y is +down):
                #   TR: (+1, -1)/√2,  TL: (-1, -1)/√2,  BR: (+1, +1)/√2,  BL: (-1, +1)/√2
                signs = {"TR": (1, -1), "TL": (-1, -1), "BR": (1, 1), "BL": (-1, 1)}
                sx, sy = signs.get(self._resize_corner, (1, -1))
                delta_preview = (d.x() * sx + d.y() * sy) / math.sqrt(2)
                if self._is_overlay():
                    # Per-slot resize for overlay layout
                    native_r_base = self._resize_r_base / _PREVIEW_SCALE
                    new_r = max(80, min(1400, int(native_r_base + delta_preview / _PREVIEW_SCALE)))
                    self._dlg._overlay_radii[self._resize_slot] = new_r
                    _cover_cache.clear()
                    self._dlg._refresh_preview()
                else:
                    new_ins_r = int(self._resize_ins_r_base + delta_preview / _PREVIEW_SCALE)
                    new_ins_r = max(_INS_R_MIN, min(_INS_R_MAX, new_ins_r))
                    if new_ins_r != self._dlg._ins_r:
                        self._dlg._ins_r = new_ins_r
                        _cover_cache.clear()
                        self._dlg._refresh_preview()
            elif self._drag_circle_slot and self._drag_circle_base:
                # Drag circle position in overlay mode
                base_cx, base_cy = self._drag_circle_base
                self._dlg._overlay_positions[self._drag_circle_slot] = (
                    base_cx + d.x() / _PREVIEW_SCALE,
                    base_cy + d.y() / _PREVIEW_SCALE,
                )
                self._dlg._refresh_preview()
            elif self._active and self._drag_base:
                # Pan the image inside the selected slot
                z = self._drag_base.zoom
                crop = self._active_crop()
                if crop is not None:
                    crop.off_x = self._drag_base.off_x - d.x() / _PREVIEW_SCALE / z
                    crop.off_y = self._drag_base.off_y - d.y() / _PREVIEW_SCALE / z
                self._dlg._refresh_preview()
        else:
            key = self._hit(event.pos())
            if key and (key.startswith("clear:") or key.startswith("label:")):
                self.setCursor(Qt.ArrowCursor)
            elif key and key.startswith("resize:"):
                corner = key.split(":", 2)[2]
                cur = (Qt.SizeFDiagCursor if corner in ("TL", "BR") else Qt.SizeBDiagCursor)
                self.setCursor(cur)
                self._dlg._hint_ctrl.set_hint(
                    self._dlg.tr("Drag to resize inset"), "info")
            elif key and key.startswith("border:"):
                self.setCursor(Qt.SizeAllCursor)
                self._dlg._hint_ctrl.set_hint(
                    self._dlg.tr("Drag border to reposition shape — {del_hint} to delete").format(
                        del_hint=_del_hint()),
                    "info")
            elif key:
                self.setCursor(Qt.OpenHandCursor)
            else:
                self.setCursor(Qt.ArrowCursor)

    def mouseReleaseEvent(self, event) -> None:
        if event.button() == Qt.LeftButton:
            self._drag_start = None
            self._drag_base = None
            self._drag_circle_slot = None
            self._drag_circle_base = None
            self._resize_slot = None
            self._resize_corner = None

    def keyPressEvent(self, event) -> None:
        """Del / Backspace / Cmd+D / Ctrl+D removes the image from the active inset slot."""
        mods = event.modifiers()
        is_delete = (
            event.key() in (Qt.Key_Delete, Qt.Key_Backspace)
            or (event.key() == Qt.Key_D
                and mods & (Qt.ControlModifier | Qt.MetaModifier))
        )
        if is_delete and self._active in self._active_slot_keys():
            self._dlg._slot_images[self._active] = None
            self._dlg._refresh_gallery()
            self._dlg._refresh_preview()
            self.update()
            return
        super().keyPressEvent(event)

    @staticmethod
    def _active_key(hit_key: Optional[str]) -> Optional[str]:
        """Strip 'label:' / 'clear:' / 'resize:' prefixes — _active only holds slot or bg keys."""
        if hit_key and (hit_key.startswith("label:") or hit_key.startswith("clear:")):
            return hit_key.split(":", 1)[1]
        if hit_key and hit_key.startswith("resize:"):
            # format: "resize:{slot}:{corner}" — extract the slot part
            return hit_key.split(":", 2)[1]
        return hit_key

    def wheelEvent(self, event) -> None:
        # Don't let scroll events through while a label is being typed
        if self._label_overlay.isVisible():
            event.ignore()
            return
        try:
            pos = event.position().toPoint()
        except AttributeError:
            pos = event.pos()
        raw = self._hit(pos)
        key = self._active_key(raw) or self._active
        crop = None
        if key:
            prev_active = self._active
            self._active = key
            if key != prev_active:
                self._dlg._on_canvas_selection_changed(key)
            crop = self._active_crop()
        if not key or crop is None:
            event.ignore()
            return
        delta = event.angleDelta().y()
        if delta == 0:
            event.ignore()
            return
        # Proportional: 15 % per standard 120-unit notch; smooth on Mac trackpad
        step = abs(delta) / 120.0 * 0.15
        factor = 1.0 + step if delta > 0 else 1.0 / (1.0 + step)
        equal_scale = (
            hasattr(self._dlg, "_equal_scale_chk")
            and self._dlg._equal_scale_chk.isChecked()
            and key in self._active_slot_keys()
        )
        if equal_scale:
            base_zooms, min_factor, max_factor = self._dlg._equal_scale_zoom_profile()
            active_base_zoom = base_zooms.get(key)
            if active_base_zoom:
                current_factor = crop.zoom / active_base_zoom if active_base_zoom > 0 else 1.0
                new_factor = max(min_factor, min(max_factor, current_factor * factor))
                for s, slot_base_zoom in base_zooms.items():
                    sc = self._dlg._crops.get(s)
                    if sc:
                        sc.zoom = max(1.0, slot_base_zoom * new_factor)
            else:
                crop.zoom = max(1.0, min(_CIRCLE_ZOOM_MAX, crop.zoom * factor))
        else:
            crop.zoom = max(1.0, min(_CIRCLE_ZOOM_MAX, crop.zoom * factor))
        self._dlg._hint_ctrl.set_hint(
            self._dlg.tr("Pan / zoom active — {zoom:.1f}× — {del_hint} to delete").format(
                zoom=crop.zoom, del_hint=_del_hint()),
            "info",
        )
        self._dlg._refresh_preview()
        self.update()
        event.accept()

    def leaveEvent(self, _) -> None:
        self.setCursor(Qt.ArrowCursor)
        self._dlg._hint_ctrl.set_hint(
            self._dlg.tr("Click a circle or background panel to select, then pick from gallery or drag to pan"),
            "info")


# ── Main dialog ───────────────────────────────────────────────────────────────
class SpeciesPlateDialog(QDialog):

    def __init__(self, observation: dict,
                 excluded_image_ids: set[int] | None = None,
                 parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Species Plate"))
        self._obs = observation
        self._obs_id = int(observation.get("id", 0))
        self._excluded_image_ids = {
            int(image_id)
            for image_id in (excluded_image_ids or set())
            if image_id is not None
        }

        # ── Load settings early so defaults are correct ────────────────────
        self._ins_r = _INS_R_DEFAULT
        self._text_scale = _TEXT_SCALE_DEFAULT
        self._se_step = 0   # index into _SE_EXPS; 0 = circle
        self._grad_opacity = 0.96
        self._grad_pos = 0.52
        self._bg_layout = "1"
        self._inset_position = 100
        self._ins_margin = 100
        self._plate_bg_color = "black"
        self._inset_layout = "5"
        self._bg_border = "none"
        self._inset_border = "white"
        self._show_tech_init = True
        self._show_sample_init = True
        self._show_measures_init = False
        self._equal_scale_init = False
        self._load_settings()

        # ── Load ALL images (no exclusion) ─────────────────────────────────
        all_imgs = [
            img
            for img in ImageDB.get_images_for_observation(self._obs_id)
            if int(img.get("id") or 0) not in self._excluded_image_ids
        ]
        self._all_images: list[dict] = all_imgs
        self._field_images = [i for i in all_imgs if i.get("image_type") == "field"]
        micro = [i for i in all_imgs if i.get("image_type") == "microscope"]

        # Assign micro images to circle slots by sample_type keyword
        by_sample: dict[str, list[dict]] = {}
        for img in micro:
            key = (img.get("sample_type") or "").strip().lower()
            by_sample.setdefault(key, []).append(img)

        self._slot_images: dict[str, Optional[dict]] = {}
        self._slot_labels: dict[str, str] = {}
        self._custom_labels: dict[str, str] = {s: "" for s in _SLOT_KEYS}
        used: set[int] = set()
        for slot in _SLOT_KEYS:
            found = None
            for kw in _SLOT_SAMPLES[slot]:
                for key, imgs in by_sample.items():
                    if kw in key:
                        for img in imgs:
                            if img["id"] not in used:
                                found = img
                                break
                    if found:
                        break
                if found:
                    break
            self._slot_images[slot] = found
            if found:
                used.add(found["id"])
                st = (found.get("sample_type") or "").strip()
                filtered = "" if _is_not_set(DatabaseTerms.canonicalize_sample(st)) else _nice_label(st)
                self._slot_labels[slot] = filtered or _SLOT_FALLBACK_LABEL[slot]
            else:
                self._slot_labels[slot] = _SLOT_FALLBACK_LABEL[slot]

        remaining_micro = [i for i in micro if i["id"] not in used]
        for slot in _SLOT_KEYS:
            if self._slot_images[slot] is None and remaining_micro:
                img = remaining_micro.pop(0)
                self._slot_images[slot] = img
                used.add(img["id"])
                st = (img.get("sample_type") or "").strip()
                filtered = "" if _is_not_set(DatabaseTerms.canonicalize_sample(st)) else _nice_label(st)
                self._slot_labels[slot] = filtered or _SLOT_FALLBACK_LABEL[slot]

        # ── Background slots (max 3, any image type) ───────────────────────
        self._bg_slots: list[Optional[dict]] = [None] * _BG_SLOTS_MAX
        # Auto-assign first unused field image to slot 0
        for img in self._field_images:
            if img.get("filepath") and os.path.isfile(img["filepath"]) and img["id"] not in used:
                self._bg_slots[0] = img
                used.add(img["id"])
                break
        # If no field image found, try any unused image
        if self._bg_slots[0] is None:
            for img in all_imgs:
                if img.get("filepath") and os.path.isfile(img["filepath"]) and img["id"] not in used:
                    self._bg_slots[0] = img
                    used.add(img["id"])
                    break

        self._background_crops: dict[str, SlotCrop] = {
            f"bg:{i}": SlotCrop() for i in range(_BG_SLOTS_MAX)
        }
        self._crops: dict[str, SlotCrop] = {s: SlotCrop() for s in _SLOT_KEYS}
        # Per-slot circle positions and radii for the 3_overlay layout (None = use default)
        self._overlay_positions: dict[str, Optional[tuple[float, float]]] = {
            s: None for s in _OVERLAY_SLOT_KEYS
        }
        self._overlay_radii: dict[str, Optional[int]] = {
            s: None for s in _OVERLAY_SLOT_KEYS
        }
        self._tilt_angles: list[float] = [0.0, 0.0]
        self._measurements_cache: dict[int, list[dict]] = {}
        self._stats_text = self._build_spore_stats()
        self._copyright_text = self._build_copyright()

        # Restore per-observation state (overrides defaults above)
        self._load_obs_state()

        self._setup_ui()
        # Equal scale: apply on open (setChecked fires toggled before connect)
        if self._equal_scale_chk.isChecked():
            self._equalize_circle_scales()
        self._refresh_gallery()
        self._refresh_preview()

    # ── Settings ──────────────────────────────────────────────────────────────

    def _load_settings(self) -> None:
        s = QSettings(SETTINGS_ORG, "SpeciesPlate")
        self._ins_r = max(_INS_R_MIN, min(_INS_R_MAX, int(s.value("ins_r", self._ins_r))))
        self._text_scale = max(0.90, float(s.value("text_scale", self._text_scale)))
        self._se_step = max(0, min(_SHAPE_STEPS - 1, int(s.value("se_step", self._se_step))))
        self._grad_opacity = float(s.value("grad_opacity", self._grad_opacity))
        self._grad_pos = float(s.value("grad_pos", self._grad_pos))
        raw_layout = s.value("bg_layout", self._bg_layout)
        self._bg_layout = raw_layout if raw_layout in _BG_LAYOUTS else "1"
        self._inset_position = max(0, min(100, int(s.value("inset_position", self._inset_position))))
        self._ins_margin = max(0, min(100, int(s.value("ins_margin", self._ins_margin))))
        raw_bg = str(s.value("plate_bg_color", self._plate_bg_color) or "").strip().lower()
        self._plate_bg_color = raw_bg if raw_bg in {"black", "white"} else "black"
        raw_inset = str(s.value("inset_layout", self._inset_layout) or "").strip()
        self._inset_layout = raw_inset if raw_inset in _INSET_LAYOUTS else "5"
        # Migration: old bg_layout "3_overlay" implies inset_layout "3"
        if self._bg_layout == "3_overlay" and not s.contains("inset_layout"):
            self._inset_layout = "3"
        raw_bg_border = str(s.value("bg_border", self._bg_border) or "").strip().lower()
        self._bg_border = raw_bg_border if raw_bg_border in {"white", "black", "none"} else "none"
        raw_ins_border = str(s.value("inset_border", self._inset_border) or "").strip().lower()
        self._inset_border = raw_ins_border if raw_ins_border in {"white", "black", "none"} else "white"
        self._show_tech_init = s.value("show_tech", True, type=bool)
        self._show_sample_init = s.value("show_sample", True, type=bool)
        self._show_measures_init = s.value("show_measures", False, type=bool)
        self._equal_scale_init = s.value("equal_scale", False, type=bool)

    def _save_settings(self) -> None:
        s = QSettings(SETTINGS_ORG, "SpeciesPlate")
        s.setValue("ins_r", self._ins_r)
        s.setValue("text_scale", self._text_scale)
        s.setValue("se_step", self._se_step)
        s.setValue("grad_opacity", self._grad_opacity)
        s.setValue("grad_pos", self._grad_pos)
        s.setValue("inset_position", self._inset_position)
        s.setValue("ins_margin", self._ins_margin)
        s.setValue("plate_bg_color", self._plate_bg_color)
        bg_layout = self._bg_layout
        if hasattr(self, "_layout_btn_group"):
            checked = self._layout_btn_group.checkedButton()
            if checked:
                bg_layout = checked.property("bg_layout")
        s.setValue("bg_layout", bg_layout)
        inset_layout = self._inset_layout
        if hasattr(self, "_inset_layout_btn_group"):
            checked = self._inset_layout_btn_group.checkedButton()
            if checked:
                inset_layout = checked.property("inset_layout")
        s.setValue("inset_layout", inset_layout)
        s.setValue("bg_border", self._bg_border)
        s.setValue("inset_border", self._inset_border)
        if hasattr(self, "_tech_badge_chk"):
            s.setValue("show_tech", self._tech_badge_chk.isChecked())
        if hasattr(self, "_sample_badge_chk"):
            s.setValue("show_sample", self._sample_badge_chk.isChecked())
        if hasattr(self, "_measures_chk"):
            s.setValue("show_measures", self._measures_chk.isChecked())
        if hasattr(self, "_equal_scale_chk"):
            s.setValue("equal_scale", self._equal_scale_chk.isChecked())
        self._save_obs_state()

    def _load_obs_state(self) -> None:
        """Restore per-observation plate state (image assignments, crops, labels)."""
        if not self._obs_id:
            return
        s = QSettings(SETTINGS_ORG, "SpeciesPlate")
        s.beginGroup(f"obs_{self._obs_id}")
        id_map = {img["id"]: img for img in self._all_images if img.get("id") is not None}
        for slot in _SLOT_KEYS:
            raw = s.value(f"slot_{slot}_id")
            if raw is not None:
                self._slot_images[slot] = id_map.get(int(raw))
        for i in range(_BG_SLOTS_MAX):
            raw = s.value(f"bg_{i}_id")
            if raw is not None:
                self._bg_slots[i] = id_map.get(int(raw))
        raw_layout = s.value("bg_layout")
        if raw_layout and raw_layout in _BG_LAYOUTS:
            self._bg_layout = raw_layout
        raw_inset = s.value("inset_layout")
        if raw_inset and raw_inset in _INSET_LAYOUTS:
            self._inset_layout = raw_inset
        elif raw_layout == "3_overlay" and not s.contains("inset_layout"):
            self._inset_layout = "3"
        for slot in _SLOT_KEYS:
            z = s.value(f"crop_{slot}_zoom")
            if z is not None:
                self._crops[slot].zoom = max(1.0, float(z))
                self._crops[slot].off_x = float(s.value(f"crop_{slot}_off_x", 0.0))
                self._crops[slot].off_y = float(s.value(f"crop_{slot}_off_y", 0.0))
        for i in range(_BG_SLOTS_MAX):
            key = f"bg:{i}"
            z = s.value(f"bgcrop_{i}_zoom")
            if z is not None:
                self._background_crops[key].zoom = max(1.0, float(z))
                self._background_crops[key].off_x = float(s.value(f"bgcrop_{i}_off_x", 0.0))
                self._background_crops[key].off_y = float(s.value(f"bgcrop_{i}_off_y", 0.0))
        for slot in _SLOT_KEYS:
            lbl = s.value(f"label_{slot}")
            if lbl is not None:
                self._custom_labels[slot] = str(lbl)
        for slot in _OVERLAY_SLOT_KEYS:
            x = s.value(f"ov_pos_{slot}_x")
            y = s.value(f"ov_pos_{slot}_y")
            if x is not None and y is not None:
                self._overlay_positions[slot] = (float(x), float(y))
            rv = s.value(f"ov_r_{slot}")
            if rv is not None:
                self._overlay_radii[slot] = int(rv)
        for i in range(2):
            a = s.value(f"tilt_angle_{i}")
            if a is not None:
                self._tilt_angles[i] = float(a)
        s.endGroup()

    def _save_obs_state(self) -> None:
        """Save per-observation plate state (image assignments, crops, labels)."""
        if not self._obs_id:
            return
        s = QSettings(SETTINGS_ORG, "SpeciesPlate")
        s.beginGroup(f"obs_{self._obs_id}")
        for slot in _SLOT_KEYS:
            img = self._slot_images.get(slot)
            s.setValue(f"slot_{slot}_id", img["id"] if img else None)
        for i in range(_BG_SLOTS_MAX):
            img = self._bg_slots[i]
            s.setValue(f"bg_{i}_id", img["id"] if img else None)
        bg_layout = self._bg_layout
        if hasattr(self, "_layout_btn_group"):
            checked = self._layout_btn_group.checkedButton()
            if checked:
                bg_layout = checked.property("bg_layout")
        s.setValue("bg_layout", bg_layout)
        inset_layout = self._inset_layout
        if hasattr(self, "_inset_layout_btn_group"):
            checked = self._inset_layout_btn_group.checkedButton()
            if checked:
                inset_layout = checked.property("inset_layout")
        s.setValue("inset_layout", inset_layout)
        for slot in _SLOT_KEYS:
            c = self._crops[slot]
            s.setValue(f"crop_{slot}_zoom", c.zoom)
            s.setValue(f"crop_{slot}_off_x", c.off_x)
            s.setValue(f"crop_{slot}_off_y", c.off_y)
        for i in range(_BG_SLOTS_MAX):
            key = f"bg:{i}"
            c = self._background_crops[key]
            s.setValue(f"bgcrop_{i}_zoom", c.zoom)
            s.setValue(f"bgcrop_{i}_off_x", c.off_x)
            s.setValue(f"bgcrop_{i}_off_y", c.off_y)
        for slot in _SLOT_KEYS:
            s.setValue(f"label_{slot}", self._custom_labels.get(slot, ""))
        for slot in _OVERLAY_SLOT_KEYS:
            pos = self._overlay_positions.get(slot)
            if pos:
                s.setValue(f"ov_pos_{slot}_x", pos[0])
                s.setValue(f"ov_pos_{slot}_y", pos[1])
            rv = self._overlay_radii.get(slot)
            if rv is not None:
                s.setValue(f"ov_r_{slot}", rv)
        for i, a in enumerate(self._tilt_angles):
            s.setValue(f"tilt_angle_{i}", a)
        s.endGroup()

    def keyPressEvent(self, event) -> None:
        # Never let Enter/Return trigger accept() — this dialog has no "OK" action.
        # QLineEdit handles Return itself via returnPressed; we must not let the
        # QDialog default-button machinery also fire (it would close the dialog).
        from PySide6.QtCore import Qt as _Qt
        if event.key() in (Qt.Key_Return, Qt.Key_Enter):
            event.ignore()
            return
        super().keyPressEvent(event)

    def accept(self) -> None:
        self._save_settings()
        super().accept()

    def reject(self) -> None:
        self._save_settings()
        super().reject()

    # ── UI ────────────────────────────────────────────────────────────────────

    def _setup_ui(self) -> None:
        from ui.image_gallery_widget import ImageGalleryWidget

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(6)

        # ── Main row: left controls panel + right canvas/gallery ─────────────
        main_row = QHBoxLayout()
        main_row.setSpacing(10)
        root.addLayout(main_row, 1)

        # Left control panel
        left = QFrame()
        left.setFrameShape(QFrame.StyledPanel)
        left.setFixedWidth(290)
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(8, 8, 8, 8)
        left_layout.setSpacing(6)

        def _add_slider_row(label: str, widget, layout) -> None:
            lbl = QLabel(label)
            lbl.setWordWrap(True)
            layout.addWidget(lbl)
            layout.addWidget(widget)

        _btn_style = (
            "QPushButton { background: transparent; border: 1px solid #555; border-radius: 3px; }"
            "QPushButton:checked { background: rgba(60,120,220,100); border: 1px solid #5090f0; }"
            "QPushButton:hover:!checked { background: rgba(60,120,220,45); }"
        )

        _color_btn_style = (
            "QPushButton { background: transparent; border: 2px solid transparent;"
            " border-radius: 4px; padding: 2px; }"
            "QPushButton:checked { border: 2px solid #c8c8c8; }"
            "QPushButton:hover:!checked { border: 2px solid #888; }"
        )

        def _color_swatch_widget(label: str, btn_group, options, slot,
                                 current_val: str = "") -> QWidget:
            """Build a label-above + swatch-buttons widget; return it for flexible placement."""
            w = QWidget()
            vbox = QVBoxLayout(w)
            vbox.setContentsMargins(0, 0, 0, 0)
            vbox.setSpacing(2)
            vbox.addWidget(QLabel(label))
            row = QHBoxLayout()
            row.setSpacing(4)
            for value, lbl_text in options:
                b = QPushButton()
                b.setProperty("color_val", value)
                b.setCheckable(True)
                b.setChecked(value == current_val)
                b.setIcon(_swatch_icon(value))
                b.setIconSize(QSize(18, 18))
                b.setFixedSize(30, 30)
                b.setStyleSheet(_color_btn_style)
                b.setToolTip(lbl_text)
                b.clicked.connect(lambda _c=False, v=value: slot(v))
                btn_group.addButton(b)
                row.addWidget(b)
            row.addStretch()
            vbox.addLayout(row)
            return w

        # ── Background images group ────────────────────────────────────────
        bg_grp = QGroupBox(self.tr("Background images"))
        bg_grp_layout = QVBoxLayout(bg_grp)
        bg_grp_layout.setContentsMargins(6, 6, 6, 6)
        bg_grp_layout.setSpacing(4)
        left_layout.addWidget(bg_grp)

        # Layout icons — two rows
        _bg_tooltips = {
            "1":         self.tr("Single background"),
            "2_50":      self.tr("Two panels, equal"),
            "2_60":      self.tr("Two panels, 60/40"),
            "3_50":      self.tr("Three panels, 50/50"),
            "3":         self.tr("Three panels, 60/40"),
            "3_overlay": self.tr("Two corner-anchored panels"),
            "2_al":      self.tr("Two panels, diagonal left"),
            "2_ar":      self.tr("Two panels, diagonal right"),
            "3_al":      self.tr("Three panels, angled split left"),
            "3_ar":      self.tr("Three panels, angled split right"),
            "2_tilt":    self.tr("Two tilted panels"),
        }
        self._layout_btn_group = QButtonGroup(self)
        self._layout_btn_group.setExclusive(True)
        for row_layouts in (("1", "2_50", "2_60", "3_50", "3"),
                            ("2_tilt", "2_al", "2_ar", "3_al", "3_ar")):
            layout_row = QHBoxLayout()
            layout_row.setSpacing(3)
            for bg_layout in row_layouts:
                btn = QPushButton()
                btn.setIcon(QIcon(_make_layout_icon(bg_layout, 40, 24)))
                btn.setIconSize(QSize(40, 24))
                btn.setCheckable(True)
                btn.setChecked(bg_layout == self._bg_layout)
                btn.setFixedSize(48, 32)
                btn.setStyleSheet(_btn_style)
                btn.setProperty("bg_layout", bg_layout)
                btn.setToolTip(_bg_tooltips.get(bg_layout, bg_layout))
                self._layout_btn_group.addButton(btn)
                layout_row.addWidget(btn)
            layout_row.addStretch()
            bg_grp_layout.addLayout(layout_row)
        self._layout_btn_group.buttonClicked.connect(
            lambda btn: self._on_bg_layout_changed(btn.property("bg_layout"))
        )

        # Background size
        self._inset_position_slider = QSlider(Qt.Horizontal)
        self._inset_position_slider.setRange(0, 100)
        self._inset_position_slider.setValue(self._inset_position)
        self._inset_position_slider.valueChanged.connect(self._on_inset_position)
        _add_slider_row(self.tr("Size:"), self._inset_position_slider, bg_grp_layout)

        # Gradient opacity
        self._grad_opacity_slider = QSlider(Qt.Horizontal)
        self._grad_opacity_slider.setRange(0, 100)
        self._grad_opacity_slider.setValue(int(self._grad_opacity * 100))
        self._grad_opacity_slider.valueChanged.connect(self._on_grad_opacity)
        _add_slider_row(self.tr("Gradient opacity:"), self._grad_opacity_slider, bg_grp_layout)

        # Gradient position
        self._grad_pos_slider = QSlider(Qt.Horizontal)
        self._grad_pos_slider.setRange(20, 90)
        self._grad_pos_slider.setValue(int(self._grad_pos * 100))
        self._grad_pos_slider.valueChanged.connect(self._on_grad_pos)
        _add_slider_row(self.tr("Gradient position:"), self._grad_pos_slider, bg_grp_layout)

        # Background color + border — side by side
        self._bg_color_group = QButtonGroup(self)
        self._bg_color_group.setExclusive(True)
        self._bg_border_group = QButtonGroup(self)
        self._bg_border_group.setExclusive(True)
        _bg_border_val = self._bg_border if self._bg_border != "none" else "white"
        _bg_color_pair = QHBoxLayout()
        _bg_color_pair.setSpacing(12)
        _bg_color_pair.addWidget(_color_swatch_widget(
            self.tr("Color:"), self._bg_color_group,
            [("white", self.tr("White")), ("black", self.tr("Black"))],
            self._on_plate_bg_color, current_val=self._plate_bg_color))
        _bg_color_pair.addWidget(_color_swatch_widget(
            self.tr("Border:"), self._bg_border_group,
            [("white", self.tr("White")), ("black", self.tr("Black"))],
            self._on_bg_border, current_val=_bg_border_val))
        _bg_color_pair.addStretch()
        bg_grp_layout.addLayout(_bg_color_pair)

        # ── Inset images group ─────────────────────────────────────────────
        ins_grp = QGroupBox(self.tr("Inset images"))
        ins_grp_layout = QVBoxLayout(ins_grp)
        ins_grp_layout.setContentsMargins(6, 6, 6, 6)
        ins_grp_layout.setSpacing(4)
        left_layout.addWidget(ins_grp)

        # Inset layout icons
        _inset_tooltips = {
            "5": self.tr("5 circles"),
            "3": self.tr("3 circles"),
        }
        self._inset_layout_btn_group = QButtonGroup(self)
        self._inset_layout_btn_group.setExclusive(True)
        inset_row = QHBoxLayout()
        inset_row.setSpacing(3)
        for ins_layout in _INSET_LAYOUTS:
            btn = QPushButton()
            btn.setIcon(QIcon(_make_inset_layout_icon(ins_layout, 40, 24)))
            btn.setIconSize(QSize(40, 24))
            btn.setCheckable(True)
            btn.setChecked(ins_layout == self._inset_layout)
            btn.setFixedSize(48, 32)
            btn.setStyleSheet(_btn_style)
            btn.setProperty("inset_layout", ins_layout)
            btn.setToolTip(_inset_tooltips.get(ins_layout, ins_layout))
            self._inset_layout_btn_group.addButton(btn)
            inset_row.addWidget(btn)
        inset_row.addStretch()
        ins_grp_layout.addLayout(inset_row)
        self._inset_layout_btn_group.buttonClicked.connect(
            lambda btn: self._on_inset_layout_changed(btn.property("inset_layout"))
        )

        # Inset position (hidden in overlay mode)
        self._ins_margin_label = QLabel(self.tr("Position:"))
        self._ins_margin_label.setWordWrap(True)
        ins_grp_layout.addWidget(self._ins_margin_label)
        self._ins_margin_slider = QSlider(Qt.Horizontal)
        self._ins_margin_slider.setRange(0, 100)
        self._ins_margin_slider.setValue(self._ins_margin)
        self._ins_margin_slider.valueChanged.connect(self._on_ins_margin)
        ins_grp_layout.addWidget(self._ins_margin_slider)

        # Shape
        self._se_slider = QSlider(Qt.Horizontal)
        self._se_slider.setRange(0, _SHAPE_STEPS - 1)
        self._se_slider.setValue(self._se_step)
        self._se_slider.valueChanged.connect(self._on_se_step)
        _add_slider_row(self.tr("Shape:"), self._se_slider, ins_grp_layout)

        # Text scale
        self._text_slider = QSlider(Qt.Horizontal)
        self._text_slider.setRange(90, 200)
        self._text_slider.setValue(int(self._text_scale * 100))
        self._text_slider.valueChanged.connect(self._on_text_scale)
        _add_slider_row(self.tr("Text:"), self._text_slider, ins_grp_layout)

        # Inset border
        self._inset_border_group = QButtonGroup(self)
        self._inset_border_group.setExclusive(True)
        _ins_border_val = self._inset_border if self._inset_border != "none" else "white"
        ins_grp_layout.addWidget(_color_swatch_widget(
            self.tr("Border:"), self._inset_border_group,
            [("white", self.tr("White")), ("black", self.tr("Black"))],
            self._on_inset_border, current_val=_ins_border_val))

        # Checkboxes — inside inset group
        self._tech_badge_chk = QCheckBox(self.tr("Show magnification && contrast"))
        self._tech_badge_chk.setChecked(self._show_tech_init)
        self._tech_badge_chk.toggled.connect(self._refresh_preview)
        ins_grp_layout.addWidget(self._tech_badge_chk)

        self._sample_badge_chk = QCheckBox(self.tr("Show mount, stain && sample type"))
        self._sample_badge_chk.setChecked(self._show_sample_init)
        self._sample_badge_chk.toggled.connect(self._refresh_preview)
        ins_grp_layout.addWidget(self._sample_badge_chk)

        self._measures_chk = QCheckBox(self.tr("Show measures"))
        self._measures_chk.setChecked(self._show_measures_init)
        self._measures_chk.toggled.connect(self._refresh_preview)
        ins_grp_layout.addWidget(self._measures_chk)

        self._equal_scale_chk = QCheckBox(self.tr("Same scale"))
        self._equal_scale_chk.setChecked(self._equal_scale_init)
        self._equal_scale_chk.toggled.connect(self._on_equal_scale_toggled)
        ins_grp_layout.addWidget(self._equal_scale_chk)

        # ── Save / restore group ───────────────────────────────────────────
        sr_grp = QGroupBox(self.tr("Save / restore"))
        sr_grp_layout = QVBoxLayout(sr_grp)
        sr_grp_layout.setContentsMargins(6, 6, 6, 6)
        sr_grp_layout.setSpacing(4)
        left_layout.addWidget(sr_grp)

        self._layout_table = QTableWidget(0, 4)
        self._layout_table.setHorizontalHeaderLabels(["", self.tr("Preview"), "BG", "○"])
        hdr = self._layout_table.horizontalHeader()
        for col in range(4):
            hdr.setSectionResizeMode(col, QHeaderView.Fixed)
        self._layout_table.setColumnWidth(0, 42)
        self._layout_table.setColumnWidth(1, 80)
        self._layout_table.setColumnWidth(2, 46)
        self._layout_table.setColumnWidth(3, 46)
        self._layout_table_paths: list[str] = []
        self._layout_table.verticalHeader().setVisible(False)
        self._layout_table.setSelectionBehavior(QTableWidget.SelectRows)
        self._layout_table.setSelectionMode(QTableWidget.SingleSelection)
        self._layout_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._layout_table.setShowGrid(False)
        self._layout_table.setMaximumHeight(200)
        self._layout_table.verticalHeader().setDefaultSectionSize(52)
        self._layout_table.cellClicked.connect(self._on_layout_table_clicked)
        sr_grp_layout.addWidget(self._layout_table)

        sr_btn_row = QHBoxLayout()
        sr_btn_row.setSpacing(4)
        save_layout_btn = QPushButton(self.tr("Save layout"))
        save_layout_btn.setToolTip(self.tr("Save all layout settings to a .mplate file"))
        save_layout_btn.clicked.connect(self._on_save_layout)
        sr_btn_row.addWidget(save_layout_btn)
        sr_btn_row.addStretch()
        self._reset_layout_btn = QPushButton(self.tr("Reset layout"))
        self._reset_layout_btn.clicked.connect(self._on_reset_layout)
        sr_btn_row.addWidget(self._reset_layout_btn)
        sr_grp_layout.addLayout(sr_btn_row)

        self._refresh_layout_table()

        left_layout.addStretch()
        main_row.addWidget(left)

        # Right side: canvas + gallery
        right_layout = QVBoxLayout()
        right_layout.setSpacing(6)

        self._canvas = PlatePreviewCanvas(self)
        right_layout.addWidget(self._canvas, 0, Qt.AlignHCenter)

        self._gallery = ImageGalleryWidget(
            self.tr("Images"),
            self,
            show_delete=False,
            show_badges=True,
        )
        self._gallery.setMaximumHeight(190)
        self._gallery.imageClicked.connect(self._on_gallery_image_clicked)
        right_layout.addWidget(self._gallery)

        main_row.addLayout(right_layout, 1)

        # ── Action row: hint bar + Export + Close ─────────────────────────────
        action_row = QHBoxLayout()
        action_row.setSpacing(8)

        self._hint_bar = HintBar(self)
        self._hint_ctrl = HintStatusController(self._hint_bar, self)
        self._hint_bar.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        action_row.addWidget(self._hint_bar, 1)
        action_row.addWidget(make_github_help_button(self, "species-plate-dialog.md"), 0, Qt.AlignRight | Qt.AlignVCenter)

        self._export_btn = QPushButton(self.tr("Export…"))
        self._export_btn.clicked.connect(self._on_export)
        action_row.addWidget(self._export_btn)

        close_btn = QPushButton(self.tr("Close"))
        close_btn.clicked.connect(self.reject)
        action_row.addWidget(close_btn)
        root.addLayout(action_row)

        # Hint registrations
        self._hint_ctrl.register_widget(self._inset_position_slider,
                                        self.tr("0-50 changes width only; 50-100 scales up with equal borders to full frame"))
        self._hint_ctrl.register_widget(self._ins_margin_slider,
                                        self.tr("100 = default position; lower values move insets toward the canvas edge"))
        # Set initial visibility of inset position slider
        _overlay = (self._inset_layout == "3")
        self._ins_margin_label.setVisible(not _overlay)
        self._ins_margin_slider.setVisible(not _overlay)
        self._hint_ctrl.register_widget(self._text_slider,
                                        self.tr("Scale all text up or down"))
        self._hint_ctrl.register_widget(self._grad_opacity_slider,
                                        self.tr("Adjust darkness of bottom gradient"))
        self._hint_ctrl.register_widget(self._grad_pos_slider,
                                        self.tr("Adjust how far up the gradient extends"))
        for btn in self._layout_btn_group.buttons():
            self._hint_ctrl.register_widget(btn,
                                            self.tr("Choose background panel layout"))
        for btn in self._inset_layout_btn_group.buttons():
            self._hint_ctrl.register_widget(btn,
                                            self.tr("Choose inset circle arrangement"))
        for btn in self._bg_color_group.buttons():
            self._hint_ctrl.register_widget(btn,
                                            self.tr("Choose the fill color around inset outsets"))
        for btn in self._bg_border_group.buttons():
            self._hint_ctrl.register_widget(btn,
                                            self.tr("Border drawn between background panels"))
        for btn in self._inset_border_group.buttons():
            self._hint_ctrl.register_widget(btn,
                                            self.tr("Border ring drawn around each inset circle"))
        self._hint_ctrl.register_widget(self._equal_scale_chk,
                                        self.tr("Show all microscope circles at the same physical µm/pixel scale"))
        self._hint_ctrl.register_widget(save_layout_btn, self.tr("Save all layout settings to a .mplate file"))
        self._hint_ctrl.register_widget(self._reset_layout_btn, self.tr("Reset inset size and position to defaults"))
        self._hint_ctrl.register_widget(self._export_btn, self.tr("Export plate to file"))
        self._hint_ctrl.register_widget(close_btn, self.tr("Close without exporting"))

        self._hint_ctrl.set_hint(
            self.tr("Click a circle or background panel to select, then pick from gallery or drag to pan"),
            "info")

    # ── Slot value handlers ───────────────────────────────────────────────────

    def _on_size(self, v: int) -> None:
        self._ins_r = v
        _cover_cache.clear()
        self._refresh_preview()

    def _on_inset_position(self, v: int) -> None:
        self._inset_position = max(0, min(100, int(v)))
        self._refresh_preview()

    def _on_ins_margin(self, v: int) -> None:
        self._ins_margin = max(0, min(100, int(v)))
        self._refresh_preview()

    def _on_reset_layout(self) -> None:
        """Reset inset size, position and overlay circle positions/radii to defaults."""
        self._ins_r = _INS_R_DEFAULT
        self._ins_margin = 100
        self._overlay_positions = {s: None for s in _OVERLAY_SLOT_KEYS}
        self._overlay_radii = {s: None for s in _OVERLAY_SLOT_KEYS}
        self._ins_margin_slider.setValue(100)
        _cover_cache.clear()
        self._refresh_preview()

    def _on_text_scale(self, v: int) -> None:
        self._text_scale = v / 100.0
        self._refresh_preview()

    def _on_se_step(self, v: int) -> None:
        self._se_step = max(0, min(_SHAPE_STEPS - 1, v))
        self._refresh_preview()

    def _on_grad_opacity(self, v: int) -> None:
        self._grad_opacity = v / 100.0
        self._refresh_preview()

    def _on_grad_pos(self, v: int) -> None:
        self._grad_pos = v / 100.0
        self._refresh_preview()

    def _on_plate_bg_color(self, value: str) -> None:
        normalized = str(value or "").strip().lower()
        if normalized not in {"black", "white"}:
            return
        self._plate_bg_color = normalized
        self._refresh_preview()

    def _on_equal_scale_toggled(self, checked: bool) -> None:
        if checked:
            self._equalize_circle_scales()
        self._refresh_preview()

    def _equal_scale_zoom_profile(self) -> tuple[dict[str, float], float, float]:
        """Return per-slot base zooms plus the shared-factor min/max range."""
        slots = _OVERLAY_SLOT_KEYS if self._inset_layout == "3" else _SLOT_KEYS
        overlay_params = None
        if self._inset_layout == "3":
            overlay_params = _overlay_slot_params(self._ins_r, self._overlay_positions, self._overlay_radii)

        coefficients: dict[str, float] = {}
        for slot in slots:
            img = self._slot_images.get(slot)
            if not img:
                continue
            if str(img.get("image_type") or "").strip().lower() != "microscope":
                continue
            spp = _image_scale_mpp(img)
            if not spp:
                continue
            fp = img.get("filepath", "")
            if not fp or not os.path.isfile(fp):
                continue
            pix = _load_pix(fp)
            if pix.isNull() or pix.width() == 0 or pix.height() == 0:
                continue
            if overlay_params and slot in overlay_params:
                diam = max(1, int(round(overlay_params[slot][2] * 2)))
            else:
                diam = max(1, int(self._ins_r * 2))
            cover_s = max(diam / pix.width(), diam / pix.height())
            coefficients[slot] = float(spp) / cover_s

        if not coefficients:
            return {}, 1.0, _CIRCLE_ZOOM_MAX

        reference = min(coefficients.values())
        if reference <= 0:
            return {}, 1.0, _CIRCLE_ZOOM_MAX

        base_zooms = {slot: coeff / reference for slot, coeff in coefficients.items()}
        min_factor = max((1.0 / zoom) for zoom in base_zooms.values()) if base_zooms else 1.0
        max_factor = _CIRCLE_ZOOM_MAX
        return base_zooms, min_factor, max_factor

    def _equalize_circle_scales(self) -> None:
        """Set per-circle zoom so all displayed circles share the same µm/pixel scale."""
        base_zooms, _min_factor, _max_factor = self._equal_scale_zoom_profile()
        if not base_zooms:
            return
        for slot, base_zoom in base_zooms.items():
            self._crops[slot].zoom = max(1.0, float(base_zoom))
            self._crops[slot].off_x = 0.0
            self._crops[slot].off_y = 0.0

    def _fit_new_slot_to_current_scale(self, new_slot: str) -> None:
        """Zoom the newly assigned slot to match the existing shared µm/pixel scale.

        Unlike _equalize_circle_scales(), this does NOT touch any other slot's zoom.
        If no existing slot has a known scale, falls back to _equalize_circle_scales().
        """
        slots = _OVERLAY_SLOT_KEYS if self._inset_layout == "3" else _SLOT_KEYS
        overlay_params = None
        if self._inset_layout == "3":
            overlay_params = _overlay_slot_params(self._ins_r, self._overlay_positions, self._overlay_radii)

        def _coefficient(slot: str) -> float | None:
            img = self._slot_images.get(slot)
            if not img:
                return None
            if str(img.get("image_type") or "").strip().lower() != "microscope":
                return None
            spp = _image_scale_mpp(img)
            if not spp:
                return None
            fp = img.get("filepath", "")
            if not fp or not os.path.isfile(fp):
                return None
            pix = _load_pix(fp)
            if pix.isNull() or pix.width() == 0 or pix.height() == 0:
                return None
            if overlay_params and slot in overlay_params:
                diam = max(1, int(round(overlay_params[slot][2] * 2)))
            else:
                diam = max(1, int(self._ins_r * 2))
            cover_s = max(diam / pix.width(), diam / pix.height())
            return float(spp) / cover_s

        # Find current display µm/pixel from any existing (non-new) equalized slot
        display_mpp: float | None = None
        for slot in slots:
            if slot == new_slot:
                continue
            coeff = _coefficient(slot)
            if coeff is None:
                continue
            zoom = self._crops[slot].zoom
            if zoom > 0:
                display_mpp = coeff / zoom
                break

        new_coeff = _coefficient(new_slot)
        if new_coeff is None or display_mpp is None:
            # No reference scale yet — equalize all as before
            self._equalize_circle_scales()
            return

        self._crops[new_slot].zoom = max(1.0, new_coeff / display_mpp)
        self._crops[new_slot].off_x = 0.0
        self._crops[new_slot].off_y = 0.0

    def _on_bg_layout_changed(self, layout: str) -> None:
        import random as _random
        old_count = len(_background_panel_rects(self._bg_layout))
        self._bg_layout = layout
        if layout in _TILT_LAYOUTS:
            self._tilt_angles = [_random.uniform(-5.0, 5.0),
                                  _random.uniform(-5.0, 5.0)]
        new_count = len(_background_panel_rects(layout))
        if new_count > old_count:
            in_use = self._in_use_image_ids()
            candidates = [i for i in self._field_images
                          if i.get("filepath") and i.get("id") not in in_use]
            if not candidates:
                candidates = [i for i in self._all_images
                              if i.get("filepath") and i.get("id") not in in_use]
            for idx in range(old_count, new_count):
                if self._bg_slots[idx] is None and candidates:
                    img = candidates.pop(0)
                    self._bg_slots[idx] = img
                    in_use.add(img["id"])
        self._refresh_gallery()
        self._refresh_preview()

    def _on_inset_layout_changed(self, layout: str) -> None:
        self._inset_layout = layout
        _overlay = (layout == "3")
        self._ins_margin_label.setVisible(not _overlay)
        self._ins_margin_slider.setVisible(not _overlay)
        _cover_cache.clear()
        self._refresh_gallery()
        self._refresh_preview()

    def _on_bg_border(self, value: str) -> None:
        self._bg_border = value
        self._refresh_preview()

    def _on_inset_border(self, value: str) -> None:
        self._inset_border = value
        self._refresh_preview()

    def _in_use_image_ids(self) -> set[int]:
        ids: set[int] = set()
        for img in self._slot_images.values():
            if img and img.get("id") is not None:
                ids.add(img["id"])
        n_bg = len(_background_panel_rects(self._bg_layout))
        for i in range(n_bg):
            img = self._bg_slots[i]
            if img and img.get("id") is not None:
                ids.add(img["id"])
        return ids

    def _active_plate_image_id(self) -> int | None:
        active = self._canvas._active if hasattr(self, "_canvas") else None
        if not active:
            return None
        if active.startswith("bg:"):
            try:
                idx = int(active.split(":", 1)[1])
            except (IndexError, ValueError):
                return None
            if 0 <= idx < len(self._bg_slots):
                img = self._bg_slots[idx]
                return img.get("id") if img else None
            return None
        img = self._slot_images.get(active)
        return img.get("id") if img else None

    def _sync_gallery_selection(self) -> None:
        if not hasattr(self, "_gallery"):
            return
        self._gallery.select_image(self._active_plate_image_id())

    def _on_canvas_selection_changed(self, key: str | None) -> None:
        """Sync hint and refresh preview for yellow label box."""
        if key and key.startswith("bg:"):
            self._hint_ctrl.set_hint(
                self.tr("Background {n} selected — pick an image from gallery below, or drag/scroll to adjust").format(
                    n=key[3:]),
                "info")
        elif key:
            crop = self._crops.get(key)
            zoom = crop.zoom if crop else 1.0
            self._hint_ctrl.set_hint(
                self.tr("Pan / zoom active — {zoom:.1f}× — {del_hint} to delete").format(
                    zoom=zoom, del_hint=_del_hint()),
                "info")
        self._sync_gallery_selection()
        # Refresh preview to update yellow/blue label rectangle highlight
        self._refresh_preview()

    def _on_gallery_image_clicked(self, item_dict, filepath: str) -> None:
        """Assign the gallery image to the active slot/background."""
        active = self._canvas._active
        if not active:
            self._hint_ctrl.set_hint(
                self.tr("Click a circle or background panel first, then pick an image"),
                "info")
            return
        img_id = item_dict.get("id") if isinstance(item_dict, dict) else None
        new_img: Optional[dict] = None
        if img_id is not None:
            for img in self._all_images:
                if img.get("id") == img_id:
                    new_img = img
                    break
        if new_img is None:
            for img in self._all_images:
                if img.get("filepath") == filepath:
                    new_img = img
                    break
        if new_img is None:
            return
        if active.startswith("bg:"):
            try:
                idx = int(active.split(":", 1)[1])
            except (IndexError, ValueError):
                return
            if 0 <= idx < _BG_SLOTS_MAX:
                self._bg_slots[idx] = new_img
        else:
            self._slot_images[active] = new_img
            st = (new_img.get("sample_type") or "").strip()
            filtered = "" if _is_not_set(DatabaseTerms.canonicalize_sample(st)) else _nice_label(st)
            self._slot_labels[active] = filtered or _SLOT_FALLBACK_LABEL.get(active, active)
            # Keep circles equalized when a new image is assigned
            if hasattr(self, "_equal_scale_chk") and self._equal_scale_chk.isChecked():
                self._fit_new_slot_to_current_scale(active)
        self._refresh_gallery()
        self._refresh_preview()

    def _get_measurements(self, image_id: int) -> list[dict]:
        if image_id not in self._measurements_cache:
            self._measurements_cache[image_id] = MeasurementDB.get_measurements_for_image(image_id)
        return self._measurements_cache[image_id]

    # ── Gallery management ────────────────────────────────────────────────────

    def _refresh_gallery(self) -> None:
        """Rebuild gallery with reuse enabled and used/selected highlights."""
        from ui.image_gallery_widget import ImageGalleryWidget as _IGW
        in_use = self._in_use_image_ids()
        items = []
        for img in self._all_images:
            if not img.get("filepath"):
                continue
            image_type = (img.get("image_type") or "field").strip().lower()
            obj_short = _short_objective_label(
                img.get("objective_name") or img.get("magnification"))
            scale_val = _image_scale_mpp(img)
            obj_name = img.get("objective_name")
            custom_scale = bool(scale_val) and (
                not obj_name or str(obj_name).strip().lower() == "custom")
            needs_scale = (
                image_type == "microscope"
                and not obj_name
                and not scale_val
            )
            badges = _IGW.build_image_type_badges(
                image_type=image_type,
                objective_name=obj_short,
                contrast=img.get("contrast"),
                scale_microns_per_pixel=scale_val,
                custom_scale=custom_scale,
                needs_scale=needs_scale,
                resize_to_optimal=False,
                translate=self.tr,
            )
            items.append({
                "id": img.get("id"),
                "filepath": img.get("filepath", ""),
                "image_type": image_type,
                "badges": badges,
                "frame_border_color": "#c0392b" if img.get("id") in in_use else None,
            })
        self._gallery.set_items(items)
        self._sync_gallery_selection()

    # ── Rendering ─────────────────────────────────────────────────────────────

    def _refresh_preview(self) -> None:
        pw, ph = int(_W * _PREVIEW_SCALE), int(_H * _PREVIEW_SCALE)
        img = QImage(pw, ph, QImage.Format_RGB32)
        img.fill(QColor("#111"))
        p = QPainter(img)
        p.save()
        p.scale(_PREVIEW_SCALE, _PREVIEW_SCALE)
        self._paint_native(p, as_paths=False)
        p.restore()
        p.end()
        self._canvas.set_pixmap(QPixmap.fromImage(img))

    def _ts(self, pt: float) -> float:
        """Scale for labels/tags/badges — remapped so slider-min = old slider-50%."""
        return pt * _remap_ts(self._text_scale)

    def _ts_title(self, pt: float) -> float:
        """Scale for the species name title — same range as labels."""
        return pt * self._text_scale

    def _paint_native(self, painter: QPainter, as_paths: bool,
                      for_export: bool = False) -> None:
        W, H = _W, _H
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setRenderHint(QPainter.SmoothPixmapTransform)
        bg_fill = QColor("#ffffff") if self._plate_bg_color == "white" else QColor("#111111")
        painter.fillRect(0, 0, W, H, bg_fill)

        is_overlay = (self._inset_layout == "3")

        # ── Field/background photos ────────────────────────────────────────
        content_rect = _content_rect_for_insets(self._ins_r, self._inset_position)
        all_shapes = _scaled_bg_panel_shapes(self._bg_layout, content_rect,
                                             self._tilt_angles)
        if not all_shapes:
            painter.fillRect(0, 0, W, H, bg_fill)
        else:
            for idx, (rect, clip_path) in enumerate(all_shapes):
                key = f"bg:{idx}"
                img_rec = self._bg_slots[idx] if idx < len(self._bg_slots) else None
                filepath = img_rec.get("filepath") if img_rec else None
                iw, ih = int(rect.width()), int(rect.height())
                is_angled = (self._bg_layout in _ANGLED_LAYOUTS)
                is_tilt = (self._bg_layout in _TILT_LAYOUTS)
                if filepath and os.path.isfile(filepath):
                    crop = self._background_crops[key]
                    if is_tilt:
                        # Draw panel with rotation using a canvas-sized ARGB buffer
                        cx, cy, pw_f, ph_f = _tilt_panel_geom(idx, content_rect)
                        pw = int(pw_f)
                        ph = int(ph_f)
                        angle = self._tilt_angles[idx] if idx < len(self._tilt_angles) else 0.0
                        panel_img = QImage(W, H, QImage.Format_ARGB32_Premultiplied)
                        panel_img.fill(Qt.transparent)
                        pp = QPainter(panel_img)
                        pp.setRenderHint(QPainter.Antialiasing)
                        pp.setRenderHint(QPainter.SmoothPixmapTransform)
                        pp.translate(cx, cy)
                        pp.rotate(angle)
                        pp.drawPixmap(-pw // 2, -ph // 2,
                                      _rect_image(filepath, pw, ph, crop))
                        if img_rec and _image_scale_mpp(img_rec):
                            use_mm = (img_rec.get("image_type") == "field")
                            _draw_scale_bar_at(pp, 0, ph // 2 - 80, img_rec, crop,
                                               pw, as_paths, self._text_scale,
                                               target_ratio=0.20, use_mm=use_mm,
                                               left_align=True, soft_shadow=True,
                                               ref_height=ph)
                        pp.end()
                        painter.drawImage(0, 0, panel_img)
                        # Border inline — so back panel's frame stays behind front panel
                        if self._bg_border != "none":
                            bdr_c = (QColor(255, 255, 255, 180) if self._bg_border == "white"
                                     else QColor(0, 0, 0, 180))
                            bdr_pen = QPen(bdr_c, 6)
                            bdr_pen.setJoinStyle(Qt.RoundJoin)
                            painter.setPen(bdr_pen)
                            painter.setBrush(Qt.NoBrush)
                            painter.drawPath(clip_path)
                            painter.setPen(Qt.NoPen)
                    elif is_angled:
                        # Bake the polygon clip into a canvas-sized QImage so that
                        # SVG export (which doesn't support setClipPath on rasters)
                        # receives a pre-composited image with correct alpha masking.
                        panel_img = QImage(W, H, QImage.Format_ARGB32_Premultiplied)
                        panel_img.fill(Qt.transparent)
                        pp = QPainter(panel_img)
                        pp.setRenderHint(QPainter.Antialiasing)
                        pp.setRenderHint(QPainter.SmoothPixmapTransform)
                        pp.setClipPath(clip_path)
                        pp.drawPixmap(int(rect.x()), int(rect.y()),
                                      _rect_image(filepath, iw, ih, crop))
                        if img_rec and _image_scale_mpp(img_rec):
                            use_mm = (img_rec.get("image_type") == "field")
                            _draw_scale_bar_at(pp, rect.x() + 60, rect.y() + 80,
                                               img_rec, crop, iw, as_paths,
                                               self._text_scale,
                                               target_ratio=0.20, use_mm=use_mm,
                                               left_align=True, soft_shadow=True,
                                               ref_height=ih)
                        pp.end()
                        painter.drawImage(0, 0, panel_img)
                    else:
                        painter.save()
                        painter.setClipPath(clip_path)
                        painter.drawPixmap(int(rect.x()), int(rect.y()),
                                           _rect_image(filepath, iw, ih, crop))
                        if img_rec and _image_scale_mpp(img_rec):
                            use_mm = (img_rec.get("image_type") == "field")
                            bar_left = rect.x() + 60
                            bar_by = rect.y() + 80
                            _draw_scale_bar_at(painter, bar_left, bar_by, img_rec, crop,
                                               iw, as_paths, self._text_scale,
                                               target_ratio=0.20, use_mm=use_mm,
                                               left_align=True, soft_shadow=True,
                                               ref_height=ih)
                        painter.restore()
                else:
                    painter.save()
                    painter.setClipPath(clip_path)
                    painter.fillRect(int(rect.x()), int(rect.y()), iw, ih, bg_fill)
                    if not for_export:
                        # Preview-only placeholder border
                        ph = QColor(80, 140, 220, 80)
                        pen = QPen(ph, 3, Qt.DashLine)
                        painter.setPen(pen)
                        painter.setBrush(Qt.NoBrush)
                        painter.drawPath(clip_path.translated(6, 6))
                        painter.setPen(Qt.NoPen)
                    painter.restore()

        # ── Background panel border ────────────────────────────────────────
        # Tilt panels draw their borders inline (for correct z-order), skip here.
        if self._bg_border != "none" and all_shapes and self._bg_layout not in _TILT_LAYOUTS:
            bdr_c = (QColor(255, 255, 255, 180) if self._bg_border == "white"
                     else QColor(0, 0, 0, 180))
            pen = QPen(bdr_c, 6)
            pen.setJoinStyle(Qt.RoundJoin)
            painter.setPen(pen)
            painter.setBrush(Qt.NoBrush)
            for _, clip_path in all_shapes:
                painter.drawPath(clip_path)
            painter.setPen(Qt.NoPen)

        # ── Bottom gradient ────────────────────────────────────────────────
        gh = int(H * self._grad_pos)
        g = QLinearGradient(0, H - gh, 0, H)
        g.setColorAt(0.0, QColor(0, 0, 0, 0))
        g.setColorAt(1.0, QColor(0, 0, 0, int(self._grad_opacity * 255)))
        painter.fillRect(0, H - gh, W, gh, g)

        # ── Inset circles ──────────────────────────────────────────────────
        show_tech = self._tech_badge_chk.isChecked() if hasattr(self, "_tech_badge_chk") else True
        show_sample = self._sample_badge_chk.isChecked() if hasattr(self, "_sample_badge_chk") else True
        show_measures = self._measures_chk.isChecked() if hasattr(self, "_measures_chk") else False
        active_slot = self._canvas._active if hasattr(self, "_canvas") else None

        # Resolve inset border stroke
        _ins_bdr = self._inset_border
        if _ins_bdr == "white":
            _ins_stroke_color: Optional[QColor] = QColor(255, 255, 255, 230)
        elif _ins_bdr == "black":
            _ins_stroke_color = QColor(30, 30, 30, 220)
        else:
            _ins_stroke_color = None  # "none" — width will be 0
        _ins_stroke_w = 0 if _ins_bdr == "none" else _INS_STROKE

        if is_overlay:
            # ── Overlay layout: 3 circles at corners + centre, clipped to canvas ──
            _ov_stroke_w = 0 if _ins_bdr == "none" else _OVERLAY_STROKE
            painter.save()
            painter.setClipRect(0, 0, W, H)
            ov = _overlay_slot_params(self._ins_r, self._overlay_positions, self._overlay_radii)
            # Z-order: TL then BR (background circles), then TC on top
            for slot in ("TL", "BR", "TC"):
                img_rec = self._slot_images.get(slot)
                if not img_rec:
                    continue
                cx, cy, r = ov[slot]
                is_active = (active_slot == slot)
                # Corner circles: suppress label (outside frame) for TL,
                # suppress tags (outside frame) for BR.
                show_label = (slot == "TC" or slot == "BR")
                show_tags  = (slot == "TC" or slot == "TL")
                self._draw_inset(painter, cx, cy, r, img_rec,
                                 self._slot_labels.get(slot, ""),
                                 self._crops[slot], as_paths,
                                 show_tech=show_tech, show_sample=show_sample,
                                 show_measures=show_measures,
                                 custom_label=self._custom_labels.get(slot, ""),
                                 is_active=is_active,
                                 stroke_width=_ov_stroke_w,
                                 stroke_color=_ins_stroke_color,
                                 show_label=show_label,
                                 show_tags=show_tags,
                                 for_export=for_export)
            painter.restore()
        else:
            _t = self._ins_margin / 100.0
            for slot in _SLOT_KEYS:
                img_rec = self._slot_images.get(slot)
                if not img_rec:
                    continue
                cx, cy = _slot_centre_pos(self._ins_r, slot, _t)
                is_active = (active_slot == slot)
                self._draw_inset(painter, cx, cy, self._ins_r, img_rec,
                                 self._slot_labels.get(slot, ""),
                                 self._crops[slot], as_paths,
                                 show_tech=show_tech, show_sample=show_sample,
                                 show_measures=show_measures,
                                 custom_label=self._custom_labels.get(slot, ""),
                                 is_active=is_active,
                                 stroke_width=_ins_stroke_w,
                                 stroke_color=_ins_stroke_color,
                                 for_export=for_export)

        # ── Title + copyright ─────────────────────────────────────────────
        self._draw_title(painter, W, H, as_paths)
        self._draw_copyright(painter, W, H, as_paths)

    def _draw_inset(self, painter: QPainter, cx: float, cy: float, r: int,
                    img_rec: dict, label: str, crop: SlotCrop,
                    as_paths: bool, *,
                    show_tech: bool = True, show_sample: bool = True,
                    show_measures: bool = False,
                    custom_label: str = "", is_active: bool = False,
                    stroke_width: int = _INS_STROKE,
                    stroke_color: Optional[QColor] = None,
                    show_label: bool = True, show_tags: bool = True,
                    for_export: bool = False) -> None:
        fp = img_rec.get("filepath", "")
        if not fp or not os.path.isfile(fp):
            return

        diam = r * 2
        ss = self._se_step

        inset_path = _inset_shape_path(cx, cy, r, ss)
        _fill_offset_shadow(
            painter,
            inset_path,
            dx=self._ts(13),
            dy=self._ts(13),
            color=QColor(0, 0, 0, 82),
            as_paths=as_paths,
            soft=True,
        )

        # Image alpha-masked to shape (works in SVG)
        painter.drawImage(int(cx - r), int(cy - r),
                          _circular_image(fp, diam, crop, ss))

        # Measurement overlay
        if show_measures:
            img_id = img_rec.get("id")
            if img_id is not None:
                measurements = self._get_measurements(img_id)
                if measurements:
                    pix = _load_pix(fp)
                    _draw_measurements_on_circle(
                        painter, measurements,
                        pix.width(), pix.height(),
                        diam, crop, cx, cy, r, self._text_scale,
                        color=img_rec.get("measure_color"),
                        shape_step=ss)

        # Border ring — skipped when stroke_width == 0 (border = "none")
        if stroke_width > 0:
            ring_color = stroke_color if stroke_color is not None else QColor(255, 255, 255, 230)
            pen = QPen(ring_color, stroke_width)
            pen.setJoinStyle(Qt.RoundJoin)
            painter.setPen(pen)
            painter.setBrush(Qt.NoBrush)
            painter.drawPath(inset_path)
            painter.setPen(Qt.NoPen)

        # Scale bar
        _draw_scale_bar(painter, cx, cy, r, img_rec, crop, as_paths, self._text_scale,
                        soft_shadow=True)

        # ── Custom label rectangle above the circle ────────────────────────
        if show_label:
            self._draw_circle_label(painter, custom_label, cx, cy, r, is_active, as_paths,
                                    for_export=for_export)

        # ── Tags line below the circle ─────────────────────────────────────
        if show_tags:
            parts: list[str] = []
            image_type = str(img_rec.get("image_type") or "").strip().lower()
            if image_type != "field" and show_tech:
                tech = _microscope_badge_text(img_rec)
                if tech:
                    parts.append(tech)
            if image_type != "field" and show_sample:
                st_raw = img_rec.get("sample_type")
                mt_raw = img_rec.get("mount_medium")
                stain_raw = img_rec.get("stain")
                stype = "" if _is_not_set(DatabaseTerms.canonicalize_sample(st_raw)) else _nice_label(st_raw)
                mount = "" if _is_not_set(DatabaseTerms.canonicalize_mount(mt_raw)) \
                    else _nice_label(mt_raw)
                stain = "" if _is_not_set(DatabaseTerms.canonicalize("stain", stain_raw)) \
                    else _nice_label(stain_raw)
                for p in [stype, mount, stain]:
                    if p and p not in parts:
                        parts.append(p)
            if parts:
                tag_text = "  ·  ".join(parts)
                tag_font = _make_font(["Helvetica Neue", "Helvetica", "Arial"], self._ts(30))
                # Baseline must be below the circle by at least one cap-height so that
                # the ascenders don't crash into the frame at high text-scale settings.
                tag_y = cy + r + int(self._ts(30) * 4 / 3) + 8
                # On export: skip tags whose baseline falls outside the canvas
                if for_export and (tag_y > _H or cx < 0 or cx > _W):
                    pass
                else:
                    _shadow_text(painter, tag_text, cx, tag_y, tag_font,
                                 QColor(220, 220, 220, 210), as_paths, dx=2.0,
                                 soft=True)

    def _draw_circle_label(self, painter: QPainter, text: str,
                            cx: float, cy: float, r: int,
                            is_active: bool, as_paths: bool,
                            for_export: bool = False) -> None:
        """Draw a custom text label inside the top of the inset.

        When *for_export* is True, container frames are suppressed so they
        don't appear in saved PNG / SVG files — only actual text is drawn.
        """
        box_w, box_h = PlatePreviewCanvas._LABEL_BOX_W, PlatePreviewCanvas._LABEL_BOX_H
        gap = PlatePreviewCanvas._LABEL_GAP
        bx = cx - box_w / 2
        by = cy - r + gap
        rect = QRectF(bx, by, box_w, box_h)

        painter.save()
        painter.setRenderHint(QPainter.Antialiasing)

        if text.strip():
            font = _make_font(["Helvetica Neue", "Helvetica", "Arial"], self._ts(36), bold=True)
            text_cy = by + box_h / 2 + self._ts(36) * 4 / 3 / 2 - 2
            if is_active and not for_export:
                border_color = QColor(255, 220, 50, 200)
                painter.setPen(QPen(border_color, 2, Qt.DashLine))
                painter.setBrush(Qt.NoBrush)
                painter.drawRoundedRect(rect, 4, 4)
            _shadow_text(painter, text, cx, text_cy, font,
                         QColor(255, 255, 255, 230), as_paths, dx=3.0,
                         soft=True)
        elif not for_export:
            # Preview-only placeholder frame — never appears in exports
            border_color = QColor(255, 220, 50, 180) if is_active else QColor(80, 140, 220, 120)
            pen = QPen(border_color, 2, Qt.DashLine)
            painter.setPen(pen)
            painter.setBrush(Qt.NoBrush)
            painter.drawRoundedRect(rect, 4, 4)

        painter.restore()

    def _draw_title(self, painter: QPainter, W: int, H: int, as_paths: bool) -> None:
        genus = (self._obs.get("genus") or "").strip()
        species = (self._obs.get("species") or "").strip()
        vernacular = (self._obs.get("vernacular_name") or
                      self._obs.get("common_name") or "").strip()
        sci_name = f"{genus} {species}".strip() if (genus or species) else ""
        subtitle = self._plate_subtitle_text()
        if vernacular:
            vernacular = vernacular[0].upper() + vernacular[1:]

        bottom_margin = 70
        line_gap = 18
        y = H - bottom_margin

        if self._stats_text:
            f = _make_font(["Menlo", "Consolas", "Courier New"], self._ts_title(22))
            f.setLetterSpacing(QFont.AbsoluteSpacing, 0.6)
            _shadow_text(painter, self._stats_text, W / 2, y, f,
                         QColor(200, 200, 200, 220), as_paths, soft=True)
            # (copyright is now drawn on the left edge by _draw_copyright)
            y -= (int(self._ts_title(22) * 4 / 3) + line_gap)

        if subtitle:
            f = _make_font(["Helvetica Neue", "Helvetica", "Arial"], self._ts_title(26))
            _shadow_text(painter, subtitle, W / 2, y, f,
                         QColor(235, 235, 235, 230), as_paths, soft=True)
            y -= (int(self._ts_title(26) * 4 / 3) + line_gap)

        if sci_name:
            f = _make_font(["Georgia", "Times New Roman"], self._ts_title(44), italic=True)
            _shadow_text(painter, sci_name, W / 2, y, f,
                         QColor(225, 225, 225, 240), as_paths, soft=True)
            y -= (int(self._ts_title(44) * 4 / 3) + line_gap)

        if vernacular:
            f = _make_font(["Helvetica Neue", "Helvetica", "Arial"],
                           self._ts_title(72), bold=True)
            _shadow_text(painter, vernacular, W / 2, y, f,
                         QColor(255, 255, 255, 255), as_paths, soft=True)

    def _draw_copyright(self, painter: QPainter, W: int, H: int,
                        as_paths: bool) -> None:
        """Draw copyright text on the left edge, vertically centred, rotated 90°
        (reads bottom-to-top)."""
        if not self._copyright_text:
            return
        f = _make_font(["Helvetica Neue", "Helvetica", "Arial"], self._ts(18))
        painter.setFont(f)
        fm = painter.fontMetrics()
        tw = fm.horizontalAdvance(self._copyright_text)
        # Baseline offset from the rotation origin so text is vertically centred
        half_tw = tw / 2.0
        # x from left edge: ascenders face left, baseline ~24px from edge
        edge_x = int(self._ts(18) * 4 / 3) + 10
        painter.save()
        painter.setRenderHint(QPainter.Antialiasing)
        painter.translate(edge_x, H / 2)
        painter.rotate(-90)
        _shadow_text(
            painter,
            self._copyright_text,
            0,
            0,
            f,
            QColor(220, 220, 220, 205),
            as_paths,
            dx=1.5,
            soft=False,
        )
        painter.restore()

    # ── Data helpers ──────────────────────────────────────────────────────────

    def _plate_subtitle_text(self) -> str:
        location = (
            str(self._obs.get("location") or "").strip()
            or str(self._obs.get("site_name") or "").strip()
        )
        date_text = self._format_plate_date(
            self._obs.get("date") or self._obs.get("observed_datetime")
        )
        parts = [part for part in (location, date_text) if part]
        return " • ".join(parts)

    @staticmethod
    def _format_plate_date(value: object) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
        ):
            try:
                return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(text).strftime("%Y-%m-%d")
        except Exception:
            return text[:10] if len(text) >= 10 else text

    def _build_spore_stats(self) -> str:
        import numpy as np

        spore_image_ids = {
            int(img.get("id"))
            for img in self._all_images
            if img.get("id") is not None
            and DatabaseTerms.canonicalize_sample(img.get("sample_type")) in {"spore", "spores"}
        }
        if not spore_image_ids:
            return ""

        measurements = [
            m for m in MeasurementDB.get_measurements_for_observation(self._obs_id)
            if int(m.get("image_id") or 0) in spore_image_ids
            and m.get("length_um") is not None
            and m.get("width_um") is not None
            and str(m.get("measurement_type") or "").strip().lower() != "calibration"
        ]
        if not measurements:
            return ""

        try:
            lengths = np.array([float(m["length_um"]) for m in measurements], dtype=float)
            widths = np.array([float(m["width_um"]) for m in measurements], dtype=float)
            ratios = lengths / widths
            prefix = self.tr("Spores") + ": "
            result = (
                f"{prefix}"
                f"{np.percentile(lengths, 5):.1f}-{np.percentile(lengths, 95):.1f}"
                f" × {np.percentile(widths, 5):.1f}-{np.percentile(widths, 95):.1f}"
                f"  Q = {np.percentile(ratios, 5):.1f}-{np.percentile(ratios, 95):.1f}"
                f"  n = {len(measurements)}"
            )
            return result
        except Exception:
            return ""

    def _build_copyright(self) -> str:
        try:
            profile = SettingsDB.get_profile()
            name = str((profile or {}).get("name") or "").strip()
        except Exception:
            name = ""
        if not name:
            name = str(self._obs.get("author") or "").strip()
        if not name:
            return ""
        # Use same settings key as the Online publishing dialog
        license_code = str(
            SettingsDB.get_setting("artsobs_publish_image_license", "60") or "60"
        ).strip()
        license_text = _LICENSE_LABELS.get(license_code, "No reuse without permission")
        return f"{name} \u2022 {license_text}"

    # ── Background helpers ────────────────────────────────────────────────────

    def _background_panels(self) -> list[tuple[str, str, QRectF]]:
        rects = _background_panel_rects(self._bg_count)
        panels: list[tuple[str, str, QRectF]] = []
        for idx in range(self._bg_count):
            img = self._bg_slots[idx] if idx < len(self._bg_slots) else None
            if not img:
                continue
            filepath = img.get("filepath")
            if not filepath or not os.path.isfile(filepath):
                continue
            if idx < len(rects):
                panels.append((f"bg:{idx}", filepath, rects[idx]))
        return panels

    def _bg_slot_for_key(self, key: str) -> Optional[dict]:
        try:
            idx = int(key.split(":", 1)[1])
        except (IndexError, ValueError):
            return None
        return self._bg_slots[idx] if 0 <= idx < len(self._bg_slots) else None

    def _background_label(self, key: str) -> str:
        try:
            idx = int(key.split(":", 1)[1])
        except (IndexError, ValueError):
            return self.tr("Background")
        img = self._bg_slots[idx] if 0 <= idx < len(self._bg_slots) else None
        itype = (img.get("image_type") or "") if img else ""
        if itype == "field":
            return self.tr("Field photo {0}").format(idx + 1)
        return self.tr("Background {0}").format(idx + 1)

    # ── Export folder helpers ─────────────────────────────────────────────────

    def _default_export_dir(self) -> str:
        settings = get_app_settings()
        last = settings.get("last_export_dir", "")
        if last and Path(last).is_dir():
            return last
        fp = (self._obs.get("folder_path") or "").strip()
        if fp and Path(fp).is_dir():
            return fp
        try:
            db_dir = get_database_path().parent
            if db_dir.is_dir():
                return str(db_dir)
        except Exception:
            pass
        return str(Path.home())

    def _save_export_dir(self, path: str) -> None:
        update_app_settings({"last_export_dir": str(Path(path).parent)})

    def _suggest_filename(self, ext: str) -> str:
        g = (self._obs.get("genus") or "").strip()
        s = (self._obs.get("species") or "").strip()
        base = f"{g}_{s}".strip("_") or "species_plate"
        return str(Path(self._default_export_dir()) / f"{base}_plate.{ext}")

    def _export_filter_specs(self) -> list[tuple[str, str, str]]:
        specs = [
            ("PNG", "png", "PNG Images (*.png)"),
            ("JPEG", "jpg", "JPEG Images (*.jpg *.jpeg)"),
        ]
        if _SVG_OK:
            specs.append(("SVG", "svg", "SVG Files (*.svg)"))
        return specs

    def _normalize_export_path(self, path: str, ext: str) -> str:
        suffixes = [s.lower() for s in Path(path).suffixes]
        valid = {"png": [".png"], "jpg": [".jpg", ".jpeg"], "svg": [".svg"]}[ext]
        if suffixes and suffixes[-1] in valid:
            return path
        return str(Path(path).with_suffix(f".{ext}"))

    def _ask_export_path(self) -> tuple[str | None, str | None]:
        specs = self._export_filter_specs()
        default_spec = specs[0]
        filters = ";;".join(spec[2] for spec in specs)
        path, selected_filter = QFileDialog.getSaveFileName(
            self,
            self.tr("Export Plate"),
            self._suggest_filename(default_spec[1]),
            filters,
            default_spec[2],
        )
        if not path:
            return None, None
        fmt, ext = next(
            ((spec[0], spec[1]) for spec in specs if spec[2] == selected_filter),
            (default_spec[0], default_spec[1]),
        )
        return self._normalize_export_path(path, ext), fmt

    # ── Layout save / load ────────────────────────────────────────────────────

    def _layout_state_dict(self) -> dict:
        return {
            "version": 1,
            "bg_layout":      self._bg_layout,
            "inset_layout":   self._inset_layout,
            "inset_position": self._inset_position,
            "ins_margin":     self._ins_margin,
            "plate_bg_color": self._plate_bg_color,
            "bg_border":      self._bg_border,
            "inset_border":   self._inset_border,
            "text_scale":     self._text_scale,
            "se_step":        self._se_step,
            "grad_opacity":   self._grad_opacity,
            "grad_pos":       self._grad_pos,
            "tilt_angles":    list(self._tilt_angles),
            "show_tech":     (self._tech_badge_chk.isChecked()
                              if hasattr(self, "_tech_badge_chk") else True),
            "show_sample":   (self._sample_badge_chk.isChecked()
                              if hasattr(self, "_sample_badge_chk") else True),
            "show_measures": (self._measures_chk.isChecked()
                              if hasattr(self, "_measures_chk") else False),
            "equal_scale":   (self._equal_scale_chk.isChecked()
                              if hasattr(self, "_equal_scale_chk") else False),
        }

    def _sync_ui_controls(self) -> None:
        """Push all state vars back into the UI widgets (used after load)."""
        for btn in self._layout_btn_group.buttons():
            btn.setChecked(btn.property("bg_layout") == self._bg_layout)
        for btn in self._inset_layout_btn_group.buttons():
            btn.setChecked(btn.property("inset_layout") == self._inset_layout)
        self._inset_position_slider.setValue(self._inset_position)
        self._ins_margin_slider.setValue(self._ins_margin)
        self._text_slider.setValue(int(self._text_scale * 100))
        self._se_slider.setValue(self._se_step)
        self._grad_opacity_slider.setValue(int(self._grad_opacity * 100))
        self._grad_pos_slider.setValue(int(self._grad_pos * 100))
        for btn in self._bg_color_group.buttons():
            btn.setChecked(btn.property("color_val") == self._plate_bg_color)
        for btn in self._bg_border_group.buttons():
            btn.setChecked(btn.property("color_val") == self._bg_border)
        for btn in self._inset_border_group.buttons():
            btn.setChecked(btn.property("color_val") == self._inset_border)
        _overlay = (self._inset_layout == "3")
        self._ins_margin_label.setVisible(not _overlay)
        self._ins_margin_slider.setVisible(not _overlay)

    def _apply_layout_state(self, data: dict) -> None:
        """Apply a layout state dict (e.g. from a .mplate file) without touching images."""
        raw_bg = data.get("bg_layout", "1")
        self._bg_layout = raw_bg if raw_bg in _BG_LAYOUTS else "1"
        raw_ins = data.get("inset_layout", "5")
        self._inset_layout = raw_ins if raw_ins in _INSET_LAYOUTS else "5"
        self._inset_position = max(0, min(100, int(data.get("inset_position", 100))))
        self._ins_margin = max(0, min(100, int(data.get("ins_margin", 100))))
        raw_bgc = str(data.get("plate_bg_color", "black")).strip().lower()
        self._plate_bg_color = raw_bgc if raw_bgc in {"black", "white"} else "black"
        raw_bgb = str(data.get("bg_border", "white")).strip().lower()
        self._bg_border = raw_bgb if raw_bgb in {"white", "black", "none"} else "white"
        raw_inb = str(data.get("inset_border", "white")).strip().lower()
        self._inset_border = raw_inb if raw_inb in {"white", "black", "none"} else "white"
        self._text_scale = max(0.9, float(data.get("text_scale", 1.0)))
        self._se_step = max(0, min(_SHAPE_STEPS - 1, int(data.get("se_step", 0))))
        self._grad_opacity = float(data.get("grad_opacity", 0.96))
        self._grad_pos = float(data.get("grad_pos", 0.52))
        ta = data.get("tilt_angles", [0.0, 0.0])
        self._tilt_angles = [float(ta[0]) if len(ta) > 0 else 0.0,
                             float(ta[1]) if len(ta) > 1 else 0.0]
        self._sync_ui_controls()
        if hasattr(self, "_tech_badge_chk"):
            self._tech_badge_chk.setChecked(bool(data.get("show_tech", True)))
            self._sample_badge_chk.setChecked(bool(data.get("show_sample", True)))
            self._measures_chk.setChecked(bool(data.get("show_measures", False)))
            eq = bool(data.get("equal_scale", False))
            self._equal_scale_chk.setChecked(eq)
            if eq:
                self._equalize_circle_scales()
        _cover_cache.clear()
        self._refresh_gallery()
        self._refresh_preview()

    def _render_plate_thumbnail(self, tw: int = 160, th: int = 100) -> QPixmap:
        """Render the current plate at small scale and return as QPixmap."""
        img = QImage(tw, th, QImage.Format_RGB32)
        img.fill(Qt.black)
        p = QPainter(img)
        p.scale(tw / _W, th / _H)
        self._paint_native(p, as_paths=False, for_export=True)
        p.end()
        return QPixmap.fromImage(img)

    def _refresh_layout_table(self) -> None:
        """Scan the layouts dir and repopulate the saved-layouts table."""
        layouts_dir = _get_layouts_dir()
        files = sorted(layouts_dir.glob("*.mplate"),
                       key=lambda p: p.stat().st_mtime, reverse=True)
        tbl = self._layout_table
        tbl.setRowCount(0)
        self._layout_table_paths = []
        for mplate in files:
            self._layout_table_paths.append(str(mplate))
            row = tbl.rowCount()
            tbl.insertRow(row)
            tbl.setRowHeight(row, 52)

            # Delete button — col 0 (red ✕)
            del_btn = QPushButton("✕")
            del_btn.setFixedSize(36, 36)
            del_btn.setStyleSheet(
                "QPushButton { color: #ffffff; background: #c0392b; border: none;"
                " border-radius: 4px; font-weight: bold; font-size: 16px; }"
                "QPushButton:hover { background: #e74c3c; }"
                "QPushButton:pressed { background: #922b21; }")
            del_btn.setToolTip(self.tr("Delete this saved layout"))
            del_btn.clicked.connect(
                lambda _c=False, p=mplate: self._on_delete_layout(p))
            tbl.setCellWidget(row, 0, del_btn)

            # Thumbnail — col 1
            thumb_path = mplate.with_suffix(".png")
            thumb_lbl = QLabel()
            thumb_lbl.setAlignment(Qt.AlignCenter)
            if thumb_path.exists():
                pm = QPixmap(str(thumb_path)).scaled(
                    76, 48, Qt.KeepAspectRatio, Qt.SmoothTransformation)
                thumb_lbl.setPixmap(pm)
            tbl.setCellWidget(row, 1, thumb_lbl)

            # Layout icons from the JSON — cols 2 (BG) and 3 (inset)
            try:
                with open(mplate, "r", encoding="utf-8") as f:
                    data = json.load(f)
                bg_layout = data.get("bg_layout", "1")
                ins_layout = data.get("inset_layout", "5")
            except Exception:
                bg_layout, ins_layout = "1", "5"

            bg_icon_lbl = QLabel()
            bg_icon_lbl.setAlignment(Qt.AlignCenter)
            bg_icon_lbl.setPixmap(_make_layout_icon(bg_layout, 40, 25))
            tbl.setCellWidget(row, 2, bg_icon_lbl)

            ins_icon_lbl = QLabel()
            ins_icon_lbl.setAlignment(Qt.AlignCenter)
            ins_icon_lbl.setPixmap(_make_inset_layout_icon(ins_layout, 34, 25))
            tbl.setCellWidget(row, 3, ins_icon_lbl)

    def _on_delete_layout(self, mplate_path: Path) -> None:
        mplate_path.unlink(missing_ok=True)
        mplate_path.with_suffix(".png").unlink(missing_ok=True)
        self._refresh_layout_table()

    def _on_layout_table_clicked(self, row: int, col: int) -> None:
        if col == 0:
            return
        if row >= len(self._layout_table_paths):
            return
        path = Path(self._layout_table_paths[row])
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._apply_layout_state(data)
            self._hint_ctrl.set_status(
                self.tr("Layout loaded — {filename}").format(filename=path.name),
                4000, "success")
        except Exception as e:
            self._hint_ctrl.set_status(
                self.tr("Load failed — {error}").format(error=str(e)), 5000, "warning")

    def _on_save_layout(self) -> None:
        layouts_dir = _get_layouts_dir()
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        stem = f"layout_{ts}"
        mplate_path = layouts_dir / f"{stem}.mplate"
        thumb_path = layouts_dir / f"{stem}.png"
        try:
            with open(mplate_path, "w", encoding="utf-8") as f:
                json.dump(self._layout_state_dict(), f, indent=2)
            thumb = self._render_plate_thumbnail()
            thumb.save(str(thumb_path))
            self._refresh_layout_table()
            self._hint_ctrl.set_status(
                self.tr("Layout saved — {filename}").format(filename=mplate_path.name),
                4000, "success")
        except Exception as e:
            self._hint_ctrl.set_status(
                self.tr("Save failed — {error}").format(error=str(e)), 5000, "warning")

    # ── Export ────────────────────────────────────────────────────────────────

    def _on_export(self) -> None:
        path, fmt = self._ask_export_path()
        # macOS: after the native file dialog closes, the Qt dialog can lose focus
        # and become unresponsive to clicks until reactivated.
        self.activateWindow()
        self.raise_()
        if not path or not fmt:
            return
        if fmt == "SVG":
            self._export_svg(path)
        else:
            self._export_raster(path, fmt)

    def _export_raster(self, path: str, fmt: str) -> None:
        img = QImage(_W, _H, QImage.Format_RGB32)
        img.fill(QColor("#1c1c1e"))
        p = QPainter(img)
        self._paint_native(p, as_paths=False, for_export=True)
        p.end()
        quality = 95 if fmt == "JPEG" else -1
        if img.save(path, fmt, quality):
            self._save_export_dir(path)
            self._hint_ctrl.set_status(
                self.tr("Saved — {filename}").format(filename=Path(path).name),
                6000, "success")
        else:
            self._hint_ctrl.set_status(
                self.tr("Export failed — could not save file"), 5000, "warning")

    def _export_svg(self, path: str) -> None:
        if not _SVG_OK:
            self._hint_ctrl.set_status(
                "SVG unavailable — PySide6.QtSvg not installed", 5000, "warning")
            return
        gen = QSvgGenerator()
        gen.setFileName(path)
        gen.setSize(QSizeF(_W, _H).toSize())
        gen.setViewBox(QRectF(0, 0, _W, _H))
        gen.setTitle(
            f"{self._obs.get('genus', '')} {self._obs.get('species', '')} plate")
        gen.setDescription(f"Generated by {APP_NAME}")
        p = QPainter(gen)
        self._paint_native(p, as_paths=True, for_export=True)
        p.end()
        _softshadow_postprocess_svg(path)
        self._save_export_dir(path)
        self._hint_ctrl.set_status(
            self.tr("SVG saved — {filename}").format(filename=Path(path).name),
            6000, "success")


def export_observation_plate_image(
    observation: dict,
    path: str | Path,
    excluded_image_ids: set[int] | None = None,
) -> bool:
    """Render the current plate state for an observation to a raster image."""
    dialog = SpeciesPlateDialog(observation, excluded_image_ids=excluded_image_ids)
    try:
        img = QImage(_W, _H, QImage.Format_RGB32)
        img.fill(QColor("#1c1c1e"))
        painter = QPainter(img)
        dialog._paint_native(painter, as_paths=False, for_export=True)
        painter.end()
        out_path = str(path)
        fmt = "PNG" if Path(out_path).suffix.lower() == ".png" else "JPEG"
        quality = 95 if fmt == "JPEG" else -1
        return bool(img.save(out_path, fmt, quality))
    finally:
        try:
            dialog.close()
        except Exception:
            pass
        dialog.deleteLater()
