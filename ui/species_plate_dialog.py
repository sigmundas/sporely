"""Species Plate — composite mycological illustration for a single observation."""
from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from PySide6.QtCore import QPoint, QPointF, QRectF, QSettings, QSizeF, Qt
from PySide6.QtGui import (
    QBrush, QColor, QFont, QImage, QLinearGradient,
    QPainter, QPainterPath, QPen, QPixmap, QPolygonF,
)
from PySide6.QtCore import QSize
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import (
    QButtonGroup, QCheckBox, QDialog, QFileDialog, QFrame, QHBoxLayout,
    QLabel, QPushButton, QSizePolicy, QSlider,
    QVBoxLayout, QWidget,
)

try:
    from PySide6.QtSvg import QSvgGenerator
    _SVG_OK = True
except ImportError:
    _SVG_OK = False

from database.models import ImageDB, MeasurementDB, SettingsDB
from database.database_tags import DatabaseTerms
from database.schema import get_app_settings, update_app_settings, get_database_path
from ui.hint_status import HintBar, HintStatusController

# ── Canvas geometry ───────────────────────────────────────────────────────────
_W, _H = 3200, 2000
_INS_R_DEFAULT, _INS_R_MIN, _INS_R_MAX = 280, 150, 420
_INS_M = 90           # edge → circle-centre gap
_INS_STROKE = 8
_PREVIEW_SCALE = 0.40
_TEXT_SCALE_DEFAULT = 1.0
_BG_SLOTS_MAX = 3

_SLOT_KEYS = ["TL", "TR", "BL", "BR"]

_SLOT_SAMPLES = {
    "TL": ["basidia", "basidium"],
    "TR": ["spore", "spores"],
    "BL": ["cheilocystidia", "cheilocystidium", "cystidia"],
    "BR": ["pleurocystidia", "pleurocystidium", "caulocystidia", "caulocystidium"],
}
_SLOT_FALLBACK_LABEL = {"TL": "Basidia", "TR": "Spores", "BL": "Cystidia", "BR": "Cystidia"}

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


def _cover_crop(pix: QPixmap, w: int, h: int) -> QPixmap:
    pw, ph = pix.width(), pix.height()
    if pw == 0 or ph == 0:
        return pix
    s = max(w / pw, h / ph)
    sw, sh = int(pw * s), int(ph * s)
    scaled = pix.scaled(sw, sh, Qt.IgnoreAspectRatio, Qt.SmoothTransformation)
    return scaled.copy((sw - w) // 2, (sh - h) // 2, w, h)


def _cover_crop_cached(filepath: str, w: int, h: int) -> QPixmap:
    key = (filepath, w, h)
    if key not in _cover_cache:
        _cover_cache[key] = _cover_crop(_load_pix(filepath), w, h)
    return _cover_cache[key]


def _zoomed_crop(base: QPixmap, w: int, h: int, crop: SlotCrop) -> QPixmap:
    if crop.zoom <= 1.0 and crop.off_x == 0 and crop.off_y == 0:
        return base
    z = max(crop.zoom, 1.0)
    vis_w = w / z
    vis_h = h / z
    cx = max(vis_w / 2, min(w - vis_w / 2, w / 2 + crop.off_x))
    cy = max(vis_h / 2, min(h - vis_h / 2, h / 2 + crop.off_y))
    sub = base.copy(int(cx - vis_w / 2), int(cy - vis_h / 2), int(vis_w), int(vis_h))
    return sub.scaled(w, h, Qt.IgnoreAspectRatio, Qt.SmoothTransformation)


def _rect_image(filepath: str, w: int, h: int, crop: SlotCrop) -> QPixmap:
    return _zoomed_crop(_cover_crop_cached(filepath, w, h), w, h, crop)


def _circular_image(filepath: str, diam: int, crop: SlotCrop) -> QImage:
    """ARGB32 image with circular alpha — embeds cleanly in SVG."""
    img = QImage(diam, diam, QImage.Format_ARGB32_Premultiplied)
    img.fill(Qt.transparent)
    p = QPainter(img)
    p.setRenderHint(QPainter.Antialiasing)
    p.setRenderHint(QPainter.SmoothPixmapTransform)
    clip = QPainterPath()
    clip.addEllipse(0.5, 0.5, diam - 1.0, diam - 1.0)
    p.setClipPath(clip)
    p.drawPixmap(0, 0, _zoomed_crop(_cover_crop_cached(filepath, diam, diam), diam, diam, crop))
    p.end()
    return img


def _slot_centre(r: int, slot: str) -> tuple[float, float]:
    m = _INS_M + r
    return {"TL": (m, m), "TR": (_W - m, m),
            "BL": (m, _H - m), "BR": (_W - m, _H - m)}[slot]


_BG_LAYOUTS = ("1", "2_50", "2_60", "3_50", "3")


def _background_panel_rects(layout: str) -> list[QRectF]:
    """Return native-resolution panel rects for the given bg layout key."""
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
    # "1" or anything else
    return [QRectF(0, 0, _W, _H)]


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
                 dx: float = 3.0) -> None:
    _draw_centred(painter, text, cx + dx, cy + dx,
                  font, QColor(0, 0, 0, 170), as_paths)
    _draw_centred(painter, text, cx, cy, font, color, as_paths)


def _nice_label(raw: str | None) -> str:
    if not raw:
        return ""
    return raw.replace("_", " ").strip()


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

    Minimum effective zoom is 1.0 — below 1.0 the image rendering shows the
    full cover-crop unchanged, so the overlay must agree.
    """
    s = max(diam / img_w, diam / img_h)
    # Position in cover-cropped (diam × diam) space
    cx0 = px * s - (img_w * s - diam) / 2
    cy0 = py * s - (img_h * s - diam) / 2
    # Zoom/pan — clamp to 1.0 minimum to match _zoomed_crop display behaviour
    z = max(crop.zoom, 1.0)
    vis_w = diam / z
    vis_h = diam / z
    cx_vis = max(vis_w / 2, min(diam - vis_w / 2, diam / 2 + crop.off_x))
    cy_vis = max(vis_h / 2, min(diam - vis_h / 2, diam / 2 + crop.off_y))
    sub_x0 = cx_vis - vis_w / 2
    sub_y0 = cy_vis - vis_h / 2
    out_x = (cx0 - sub_x0) / vis_w * diam
    out_y = (cy0 - sub_y0) / vis_h * diam
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


# ── Scale bar ─────────────────────────────────────────────────────────────────
_NICE_BARS_UM = [1, 2, 5, 10, 20, 50, 100, 200, 500]
_NICE_BARS_MM = [0.5, 1, 2, 5, 10, 20, 50]       # in mm (= 500…50000 µm)


def _draw_scale_bar_at(painter: QPainter, bar_cx: float, bar_by: float,
                        img_rec: dict, crop: SlotCrop,
                        ref_width: float, as_paths: bool, ts: float,
                        target_ratio: float = 0.20,
                        use_mm: bool = False,
                        left_align: bool = False) -> None:
    """Draw a scale bar horizontally centred at (bar_cx, bar_by).

    ``target_ratio`` controls the desired bar length as a fraction of
    ``ref_width`` (0.20 for circles, 0.40 for field backgrounds).
    ``use_mm`` selects mm nice-values and the "mm" unit label.
    When ``left_align`` is True, ``bar_cx`` is treated as the left edge of the bar.
    """
    spp = img_rec.get("scale_microns_per_pixel")
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
    cover_s = max(ref_width / src.width(), ref_width / src.height()) if (src.width() and src.height()) else 1.0
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
    _shadow_text(painter, lbl, lbl_cx, bar_by - 14, lf, QColor(255, 255, 255, 210), as_paths, dx=1.5)


def _draw_scale_bar(painter: QPainter, cx: float, cy: float, r: int,
                    img_rec: dict, crop: SlotCrop,
                    as_paths: bool, ts: float) -> None:
    """Draw scale bar inside a circle inset (bottom area)."""
    _draw_scale_bar_at(painter, cx, cy + r - 32, img_rec, crop,
                       r * 2, as_paths, ts)


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
                                  ts: float, color=None) -> None:
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
    clip = QPainterPath()
    clip.addEllipse(QPointF(cx, cy), r - _INS_STROKE / 2, r - _INS_STROKE / 2)
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
                _draw_rotated_measure_label(
                    painter, f"{lv:.1f}",
                    len_edge[0], len_edge[1], text_col, cpt, padding=pad)
                if wv is not None:
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
    _LABEL_GAP   = 22   # gap between label box bottom and circle top (native px)

    def __init__(self, dialog: "SpeciesPlateDialog", parent=None):
        super().__init__(parent)
        self._dlg = dialog
        self.setFixedSize(int(_W * _PREVIEW_SCALE), int(_H * _PREVIEW_SCALE))
        self.setMouseTracking(True)
        self.setFocusPolicy(Qt.StrongFocus)

        self._active: Optional[str] = None
        self._drag_start: Optional[QPoint] = None
        self._drag_base: Optional[SlotCrop] = None
        self._pm: Optional[QPixmap] = None

        # ── Inline label overlay ───────────────────────────────────────────
        from PySide6.QtWidgets import QLineEdit as _QLineEdit
        self._label_overlay = _QLineEdit(self)
        self._label_overlay.hide()
        self._label_overlay.setStyleSheet(
            "QLineEdit { background: rgba(0,0,0,80); border: 2px solid #ffd700; "
            "border-radius: 3px; color: white; font-size: 11pt; font-weight: bold; "
            "padding: 1px 6px; }"
        )
        self._label_overlay_slot: Optional[str] = None
        self._label_overlay.returnPressed.connect(self._commit_label)
        self._label_overlay.editingFinished.connect(self._commit_label)

    def set_pixmap(self, pm: QPixmap) -> None:
        self._pm = pm
        self.update()

    def paintEvent(self, _) -> None:
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        if self._pm:
            p.drawPixmap(0, 0, self._pm)

        # ── Placeholder rings for empty circle slots ───────────────────────
        for slot in _SLOT_KEYS:
            if self._dlg._slot_images.get(slot):
                continue
            cx, cy = self._preview_centre(slot)
            r = self._dlg._ins_r * _PREVIEW_SCALE
            is_active = (self._active == slot)
            color = QColor(255, 200, 50, 120) if is_active else QColor(255, 255, 255, 35)
            p.setPen(QPen(color, 2, Qt.DashLine))
            p.setBrush(Qt.NoBrush)
            p.drawEllipse(QPointF(cx, cy), r, r)

        # ── Active-selection highlight ─────────────────────────────────────
        active = self._active_key(self._active)  # strip any stale prefix
        if active:
            r = self._dlg._ins_r * _PREVIEW_SCALE
            pen = QPen(QColor(255, 200, 50, 230), 3)
            p.setPen(pen)
            p.setBrush(Qt.NoBrush)
            if active.startswith("bg:"):
                rect = self._preview_background_rect(active)
                if rect is not None:
                    p.drawRect(rect.adjusted(2, 2, -2, -2))
            elif active in _SLOT_KEYS:
                cx, cy = self._preview_centre(active)
                p.drawEllipse(QPointF(cx, cy), r + 5, r + 5)

        # ── Red X buttons for filled circle slots ──────────────────────────
        for slot in _SLOT_KEYS:
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

    def _preview_centre(self, slot: str) -> tuple[float, float]:
        cx, cy = _slot_centre(self._dlg._ins_r, slot)
        return cx * _PREVIEW_SCALE, cy * _PREVIEW_SCALE

    def _clear_btn_rect_preview(self, slot: str) -> QRectF:
        """Small red-X button fully outside the circle, at 45° top-right."""
        cx, cy = self._preview_centre(slot)
        r = self._dlg._ins_r * _PREVIEW_SCALE
        br = 9  # button half-size in preview px
        dist = r + br + 4  # centre of button past the circle edge
        dx = dist * math.cos(math.radians(45))
        dy = -dist * math.sin(math.radians(45))
        return QRectF(cx + dx - br, cy + dy - br, br * 2, br * 2)

    def _label_rect_preview(self, slot: str) -> QRectF:
        """Return the label box rect in preview canvas coordinates."""
        cx, cy = self._preview_centre(slot)
        r = self._dlg._ins_r * _PREVIEW_SCALE
        bw = self._LABEL_BOX_W * _PREVIEW_SCALE
        bh = self._LABEL_BOX_H * _PREVIEW_SCALE
        gap = self._LABEL_GAP * _PREVIEW_SCALE
        return QRectF(cx - bw / 2, cy - r - bh - gap, bw, bh)

    def _hit(self, pos: QPoint) -> Optional[str]:
        pt = QPointF(pos)
        # 1. Red X clear buttons (only for filled circles)
        for slot in _SLOT_KEYS:
            if not self._dlg._slot_images.get(slot):
                continue
            if self._clear_btn_rect_preview(slot).contains(pt):
                return f"clear:{slot}"
        # 2. Label boxes above filled circles
        for slot in _SLOT_KEYS:
            if not self._dlg._slot_images.get(slot):
                continue
            if self._label_rect_preview(slot).contains(pt):
                return f"label:{slot}"
        # 3. Circle insets (filled and empty — both selectable)
        for slot in _SLOT_KEYS:
            cx, cy = self._preview_centre(slot)
            r = self._dlg._ins_r * _PREVIEW_SCALE
            dx, dy = pos.x() - cx, pos.y() - cy
            if dx * dx + dy * dy <= r * r:
                return slot
        # 4. Background panels (all, including empty)
        for key, rect in self._preview_background_rects():
            if rect.contains(pt):
                return key
        return None

    def _preview_background_rects(self) -> list[tuple[str, QRectF]]:
        """All bg panel rects at preview scale — includes empty slots."""
        rects = _background_panel_rects(self._dlg._bg_layout)
        return [
            (f"bg:{i}", QRectF(r.x() * _PREVIEW_SCALE, r.y() * _PREVIEW_SCALE,
                               r.width() * _PREVIEW_SCALE, r.height() * _PREVIEW_SCALE))
            for i, r in enumerate(rects)
        ]

    def _preview_background_rect(self, key: str) -> Optional[QRectF]:
        for bg_key, rect in self._preview_background_rects():
            if bg_key == key:
                return rect
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
                name = self._active_label(key)
                self._dlg._hint_ctrl.set_hint(
                    self._dlg.tr("Active: {name} — scroll to zoom · drag to pan · zoom {zoom:.1f}×").format(
                        name=name, zoom=sc.zoom),
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
        if (self._drag_start and self._active and self._drag_base and
                event.buttons() & Qt.LeftButton):
            d = event.pos() - self._drag_start
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
            elif key:
                self.setCursor(Qt.OpenHandCursor)
            else:
                self.setCursor(Qt.ArrowCursor)

    def mouseReleaseEvent(self, event) -> None:
        if event.button() == Qt.LeftButton:
            self._drag_start = None
            self._drag_base = None

    @staticmethod
    def _active_key(hit_key: Optional[str]) -> Optional[str]:
        """Strip 'label:' / 'clear:' prefixes — _active only holds slot or bg keys."""
        if hit_key and (hit_key.startswith("label:") or hit_key.startswith("clear:")):
            return hit_key.split(":", 1)[1]
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
            and key in _SLOT_KEYS
        )
        if equal_scale:
            # Zoom all circle slots together
            for s in _SLOT_KEYS:
                sc = self._dlg._crops.get(s)
                if sc:
                    sc.zoom = max(1.0, min(12.0, sc.zoom * factor))
        else:
            crop.zoom = max(1.0, min(12.0, crop.zoom * factor))
        name = self._active_label(key)
        self._dlg._hint_ctrl.set_hint(
            self._dlg.tr("Active: {name} — zoom {zoom:.1f}×  (drag to pan)").format(
                name=name, zoom=crop.zoom),
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

        # ── Load settings early so defaults are correct ────────────────────
        self._ins_r = _INS_R_DEFAULT
        self._text_scale = _TEXT_SCALE_DEFAULT
        self._grad_opacity = 0.96
        self._grad_pos = 0.52
        self._bg_layout = "1"
        self._show_tech_init = True
        self._show_sample_init = True
        self._show_measures_init = False
        self._equal_scale_init = False
        self._load_settings()

        # ── Load ALL images (no exclusion) ─────────────────────────────────
        all_imgs = ImageDB.get_images_for_observation(self._obs_id)
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
        s = QSettings("MycoLog", "SpeciesPlate")
        self._ins_r = int(s.value("ins_r", self._ins_r))
        self._text_scale = max(0.90, float(s.value("text_scale", self._text_scale)))
        self._grad_opacity = float(s.value("grad_opacity", self._grad_opacity))
        self._grad_pos = float(s.value("grad_pos", self._grad_pos))
        raw_layout = s.value("bg_layout", self._bg_layout)
        self._bg_layout = raw_layout if raw_layout in _BG_LAYOUTS else "1"
        self._show_tech_init = s.value("show_tech", True, type=bool)
        self._show_sample_init = s.value("show_sample", True, type=bool)
        self._show_measures_init = s.value("show_measures", False, type=bool)
        self._equal_scale_init = s.value("equal_scale", False, type=bool)

    def _save_settings(self) -> None:
        s = QSettings("MycoLog", "SpeciesPlate")
        s.setValue("ins_r", self._ins_r)
        s.setValue("text_scale", self._text_scale)
        s.setValue("grad_opacity", self._grad_opacity)
        s.setValue("grad_pos", self._grad_pos)
        bg_layout = self._bg_layout
        if hasattr(self, "_layout_btn_group"):
            checked = self._layout_btn_group.checkedButton()
            if checked:
                bg_layout = checked.property("bg_layout")
        s.setValue("bg_layout", bg_layout)
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
        s = QSettings("MycoLog", "SpeciesPlate")
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
        s.endGroup()

    def _save_obs_state(self) -> None:
        """Save per-observation plate state (image assignments, crops, labels)."""
        if not self._obs_id:
            return
        s = QSettings("MycoLog", "SpeciesPlate")
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
        s.endGroup()

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
        left.setFixedWidth(270)
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(8, 8, 8, 8)
        left_layout.setSpacing(6)

        def _add_slider_row(label: str, widget) -> None:
            lbl = QLabel(label)
            lbl.setWordWrap(True)
            left_layout.addWidget(lbl)
            left_layout.addWidget(widget)

        # Gradient opacity
        self._grad_opacity_slider = QSlider(Qt.Horizontal)
        self._grad_opacity_slider.setRange(0, 100)
        self._grad_opacity_slider.setValue(int(self._grad_opacity * 100))
        self._grad_opacity_slider.valueChanged.connect(self._on_grad_opacity)
        _add_slider_row(self.tr("Gradient opacity:"), self._grad_opacity_slider)

        # Gradient position
        self._grad_pos_slider = QSlider(Qt.Horizontal)
        self._grad_pos_slider.setRange(20, 90)
        self._grad_pos_slider.setValue(int(self._grad_pos * 100))
        self._grad_pos_slider.valueChanged.connect(self._on_grad_pos)
        _add_slider_row(self.tr("Gradient position:"), self._grad_pos_slider)

        sep1 = QFrame()
        sep1.setFrameShape(QFrame.HLine)
        sep1.setFrameShadow(QFrame.Sunken)
        left_layout.addWidget(sep1)

        # Circle size
        self._size_slider = QSlider(Qt.Horizontal)
        self._size_slider.setRange(_INS_R_MIN, _INS_R_MAX)
        self._size_slider.setValue(self._ins_r)
        self._size_slider.valueChanged.connect(self._on_size)
        _add_slider_row(self.tr("Circles:"), self._size_slider)

        # Text scale
        self._text_slider = QSlider(Qt.Horizontal)
        self._text_slider.setRange(90, 200)
        self._text_slider.setValue(int(self._text_scale * 100))
        self._text_slider.valueChanged.connect(self._on_text_scale)
        _add_slider_row(self.tr("Text:"), self._text_slider)

        sep2 = QFrame()
        sep2.setFrameShape(QFrame.HLine)
        sep2.setFrameShadow(QFrame.Sunken)
        left_layout.addWidget(sep2)

        # Background layout selector
        left_layout.addWidget(QLabel(self.tr("Layout:")))
        layout_row = QHBoxLayout()
        layout_row.setSpacing(3)
        self._layout_btn_group = QButtonGroup(self)
        self._layout_btn_group.setExclusive(True)
        for bg_layout in _BG_LAYOUTS:
            btn = QPushButton()
            btn.setIcon(QIcon(_make_layout_icon(bg_layout, 40, 24)))
            btn.setIconSize(QSize(40, 24))
            btn.setCheckable(True)
            btn.setChecked(bg_layout == self._bg_layout)
            btn.setFixedSize(48, 32)
            btn.setStyleSheet(
                "QPushButton { background: transparent; border: 1px solid #555; border-radius: 3px; }"
                "QPushButton:checked { background: rgba(60,120,220,100); border: 1px solid #5090f0; }"
                "QPushButton:hover:!checked { background: rgba(60,120,220,45); }"
            )
            btn.setProperty("bg_layout", bg_layout)
            btn.setToolTip({
                "1": self.tr("Single background"),
                "2_50": self.tr("Two panels, equal"),
                "2_60": self.tr("Two panels, 60/40"),
                "3_50": self.tr("Three panels, 50/50"),
                "3": self.tr("Three panels, 60/40"),
            }.get(bg_layout, bg_layout))
            self._layout_btn_group.addButton(btn)
            layout_row.addWidget(btn)
        layout_row.addStretch()
        self._layout_btn_group.buttonClicked.connect(
            lambda btn: self._on_bg_layout_changed(btn.property("bg_layout"))
        )
        left_layout.addLayout(layout_row)

        sep3 = QFrame()
        sep3.setFrameShape(QFrame.HLine)
        sep3.setFrameShadow(QFrame.Sunken)
        left_layout.addWidget(sep3)

        # Checkboxes
        self._tech_badge_chk = QCheckBox(self.tr("Show magnification && contrast"))
        self._tech_badge_chk.setChecked(self._show_tech_init)
        self._tech_badge_chk.toggled.connect(self._refresh_preview)
        left_layout.addWidget(self._tech_badge_chk)

        self._sample_badge_chk = QCheckBox(self.tr("Show mount && sample type"))
        self._sample_badge_chk.setChecked(self._show_sample_init)
        self._sample_badge_chk.toggled.connect(self._refresh_preview)
        left_layout.addWidget(self._sample_badge_chk)

        self._measures_chk = QCheckBox(self.tr("Show measures"))
        self._measures_chk.setChecked(self._show_measures_init)
        self._measures_chk.toggled.connect(self._refresh_preview)
        left_layout.addWidget(self._measures_chk)

        self._equal_scale_chk = QCheckBox(self.tr("Equal scale (circles)"))
        self._equal_scale_chk.setChecked(self._equal_scale_init)
        self._equal_scale_chk.toggled.connect(self._on_equal_scale_toggled)
        left_layout.addWidget(self._equal_scale_chk)

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

        self._export_btn = QPushButton(self.tr("Export…"))
        self._export_btn.clicked.connect(self._on_export)
        action_row.addWidget(self._export_btn)

        close_btn = QPushButton(self.tr("Close"))
        close_btn.clicked.connect(self.reject)
        action_row.addWidget(close_btn)
        root.addLayout(action_row)

        # Hint registrations
        self._hint_ctrl.register_widget(self._size_slider,
                                        self.tr("Adjust the size of all inset circles"))
        self._hint_ctrl.register_widget(self._text_slider,
                                        self.tr("Scale all text up or down"))
        self._hint_ctrl.register_widget(self._grad_opacity_slider,
                                        self.tr("Adjust darkness of bottom gradient"))
        self._hint_ctrl.register_widget(self._grad_pos_slider,
                                        self.tr("Adjust how far up the gradient extends"))
        for btn in self._layout_btn_group.buttons():
            self._hint_ctrl.register_widget(btn,
                                            self.tr("Choose background panel layout"))
        self._hint_ctrl.register_widget(self._equal_scale_chk,
                                        self.tr("Show all microscope circles at the same physical µm/pixel scale"))
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

    def _on_text_scale(self, v: int) -> None:
        self._text_scale = v / 100.0
        self._refresh_preview()

    def _on_grad_opacity(self, v: int) -> None:
        self._grad_opacity = v / 100.0
        self._refresh_preview()

    def _on_grad_pos(self, v: int) -> None:
        self._grad_pos = v / 100.0
        self._refresh_preview()

    def _on_equal_scale_toggled(self, checked: bool) -> None:
        if checked:
            self._equalize_circle_scales()
        self._refresh_preview()

    def _equalize_circle_scales(self) -> None:
        """Set per-circle zoom so all displayed circles share the same µm/pixel scale."""
        diam = self._ins_r * 2
        entries: list[tuple[str, float, float]] = []
        for slot in _SLOT_KEYS:
            img = self._slot_images.get(slot)
            if not img:
                continue
            spp = img.get("scale_microns_per_pixel")
            if not spp:
                continue
            fp = img.get("filepath", "")
            if not fp or not os.path.isfile(fp):
                continue
            pix = _load_pix(fp)
            if pix.isNull() or pix.width() == 0 or pix.height() == 0:
                continue
            cover_s = max(diam / pix.width(), diam / pix.height())
            entries.append((slot, float(spp), cover_s))
        if not entries:
            return
        # C = min(spp/cover_s) → zoom IN all others to match closest-up view
        C = min(spp / s for _, spp, s in entries)
        for slot, spp, s in entries:
            z = spp / (s * C)
            self._crops[slot].zoom = max(0.5, min(12.0, z))
            self._crops[slot].off_x = 0.0
            self._crops[slot].off_y = 0.0

    def _on_bg_layout_changed(self, layout: str) -> None:
        old_count = len(_background_panel_rects(self._bg_layout))
        self._bg_layout = layout
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

    def _on_canvas_selection_changed(self, key: str | None) -> None:
        """Sync hint and refresh preview for yellow label box."""
        if key and key.startswith("bg:"):
            self._hint_ctrl.set_hint(
                self.tr("Background {n} selected — pick an image from gallery below, or drag/scroll to adjust").format(
                    n=key[3:]),
                "info")
        elif key:
            self._hint_ctrl.set_hint(
                self.tr("Active: {name} — click label box to type · scroll to zoom · drag to pan").format(
                    name=self._slot_labels.get(key, key)),
                "info")
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
                self._equalize_circle_scales()
        self._refresh_gallery()
        self._refresh_preview()

    def _get_measurements(self, image_id: int) -> list[dict]:
        if image_id not in self._measurements_cache:
            self._measurements_cache[image_id] = MeasurementDB.get_measurements_for_image(image_id)
        return self._measurements_cache[image_id]

    # ── Gallery management ────────────────────────────────────────────────────

    def _refresh_gallery(self) -> None:
        """Rebuild gallery showing only images not currently in use, with proper badges."""
        from ui.image_gallery_widget import ImageGalleryWidget as _IGW
        in_use = self._in_use_image_ids()
        items = []
        for img in self._all_images:
            if not img.get("filepath") or img.get("id") in in_use:
                continue
            image_type = (img.get("image_type") or "field").strip().lower()
            obj_short = _short_objective_label(
                img.get("objective_name") or img.get("magnification"))
            scale_val = img.get("scale_microns_per_pixel")
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
            })
        self._gallery.set_items(items)

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

    def _paint_native(self, painter: QPainter, as_paths: bool) -> None:
        W, H = _W, _H
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setRenderHint(QPainter.SmoothPixmapTransform)

        # ── Field/background photos ────────────────────────────────────────
        all_rects = _background_panel_rects(self._bg_layout)
        if not all_rects:
            painter.fillRect(0, 0, W, H, QColor("#1c1c1e"))
        else:
            for idx, rect in enumerate(all_rects):
                key = f"bg:{idx}"
                img_rec = self._bg_slots[idx] if idx < len(self._bg_slots) else None
                filepath = img_rec.get("filepath") if img_rec else None
                iw, ih = int(rect.width()), int(rect.height())
                if filepath and os.path.isfile(filepath):
                    crop = self._background_crops[key]
                    painter.drawPixmap(int(rect.x()), int(rect.y()),
                                       _rect_image(filepath, iw, ih, crop))
                    if img_rec and img_rec.get("scale_microns_per_pixel"):
                        use_mm = (img_rec.get("image_type") == "field")
                        bar_left = rect.x() + 60
                        bar_by = rect.y() + 80
                        _draw_scale_bar_at(painter, bar_left, bar_by, img_rec, crop,
                                           self._ins_r * 4, as_paths, self._text_scale,
                                           target_ratio=0.20, use_mm=use_mm,
                                           left_align=True)
                else:
                    # Empty slot placeholder
                    painter.fillRect(int(rect.x()), int(rect.y()), iw, ih, QColor("#1c1c1e"))
                    ph = QColor(80, 140, 220, 80)
                    pen = QPen(ph, 3, Qt.DashLine)
                    painter.setPen(pen)
                    painter.setBrush(Qt.NoBrush)
                    painter.drawRect(int(rect.x()) + 6, int(rect.y()) + 6, iw - 12, ih - 12)
                    painter.setPen(Qt.NoPen)

        # ── Bottom gradient ────────────────────────────────────────────────
        gh = int(H * self._grad_pos)
        g = QLinearGradient(0, H - gh, 0, H)
        g.setColorAt(0.0, QColor(0, 0, 0, 0))
        g.setColorAt(1.0, QColor(0, 0, 0, int(self._grad_opacity * 255)))
        painter.fillRect(0, H - gh, W, gh, g)

        # Soft top shadow
        tg = QLinearGradient(0, 0, 0, int(H * 0.16))
        tg.setColorAt(0.0, QColor(0, 0, 0, 120))
        tg.setColorAt(1.0, QColor(0, 0, 0, 0))
        painter.fillRect(0, 0, W, int(H * 0.16), tg)

        # ── Inset circles ──────────────────────────────────────────────────
        show_tech = self._tech_badge_chk.isChecked() if hasattr(self, "_tech_badge_chk") else True
        show_sample = self._sample_badge_chk.isChecked() if hasattr(self, "_sample_badge_chk") else True
        show_measures = self._measures_chk.isChecked() if hasattr(self, "_measures_chk") else False
        active_slot = self._canvas._active if hasattr(self, "_canvas") else None

        for slot in _SLOT_KEYS:
            img_rec = self._slot_images.get(slot)
            if not img_rec:
                continue
            cx, cy = _slot_centre(self._ins_r, slot)
            is_active = (active_slot == slot)
            self._draw_inset(painter, cx, cy, self._ins_r, img_rec,
                             self._slot_labels.get(slot, ""),
                             self._crops[slot], as_paths,
                             show_tech=show_tech, show_sample=show_sample,
                             show_measures=show_measures,
                             custom_label=self._custom_labels.get(slot, ""),
                             is_active=is_active)

        # ── Title + copyright ─────────────────────────────────────────────
        self._draw_title(painter, W, H, as_paths)

    def _draw_inset(self, painter: QPainter, cx: float, cy: float, r: int,
                    img_rec: dict, label: str, crop: SlotCrop,
                    as_paths: bool, *,
                    show_tech: bool = True, show_sample: bool = True,
                    show_measures: bool = False,
                    custom_label: str = "", is_active: bool = False) -> None:
        fp = img_rec.get("filepath", "")
        if not fp or not os.path.isfile(fp):
            return

        diam = r * 2

        # Soft shadow ring
        sp = QPainterPath()
        sp.addEllipse(QPointF(cx, cy), r + 18, r + 18)
        painter.fillPath(sp, QBrush(QColor(0, 0, 0, 100)))

        # Circular image (alpha-masked, works in SVG)
        painter.drawImage(int(cx - r), int(cy - r),
                          _circular_image(fp, diam, crop))

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
                        color=img_rec.get("measure_color"))

        # White ring
        pen = QPen(QColor(255, 255, 255, 230), _INS_STROKE)
        pen.setJoinStyle(Qt.RoundJoin)
        painter.setPen(pen)
        painter.setBrush(Qt.NoBrush)
        painter.drawEllipse(QPointF(cx, cy), r, r)
        painter.setPen(Qt.NoPen)

        # Scale bar
        _draw_scale_bar(painter, cx, cy, r, img_rec, crop, as_paths, self._text_scale)

        # ── Custom label rectangle above the circle ────────────────────────
        self._draw_circle_label(painter, custom_label, cx, cy, r, is_active, as_paths)

        # ── Tags line below the circle ─────────────────────────────────────
        parts: list[str] = []
        if show_tech:
            tech = _microscope_badge_text(img_rec)
            if tech:
                parts.append(tech)
        if show_sample:
            st_raw = img_rec.get("sample_type")
            mt_raw = img_rec.get("mount_medium")
            stype = ("" if _is_not_set(DatabaseTerms.canonicalize_sample(st_raw))
                     else _nice_label(st_raw)) or label
            mount = "" if _is_not_set(DatabaseTerms.canonicalize_mount(mt_raw)) \
                else _nice_label(mt_raw)
            for p in [stype, mount]:
                if p and p not in parts:
                    parts.append(p)
        if parts:
            tag_text = "  ·  ".join(parts)
            tag_font = _make_font(["Helvetica Neue", "Helvetica", "Arial"], self._ts(30))
            _shadow_text(painter, tag_text, cx, cy + r + 42, tag_font,
                         QColor(220, 220, 220, 210), as_paths, dx=2.0)

    def _draw_circle_label(self, painter: QPainter, text: str,
                            cx: float, cy: float, r: int,
                            is_active: bool, as_paths: bool) -> None:
        """Draw a text label above the circle using drop-shadow style.
        Shows a dashed outline placeholder when empty (clickable hint).
        """
        box_w, box_h = PlatePreviewCanvas._LABEL_BOX_W, PlatePreviewCanvas._LABEL_BOX_H
        gap = PlatePreviewCanvas._LABEL_GAP
        bx = cx - box_w / 2
        by = cy - r - box_h - gap
        rect = QRectF(bx, by, box_w, box_h)

        painter.save()
        painter.setRenderHint(QPainter.Antialiasing)

        if text.strip():
            # Shadow + bright text — same style as tags below circles
            font = _make_font(["Helvetica Neue", "Helvetica", "Arial"], self._ts(36), bold=True)
            text_cy = by + box_h / 2 + self._ts(36) * 4 / 3 / 2 - 2
            border_color = QColor(255, 220, 50, 200) if is_active else QColor(255, 255, 255, 0)
            if is_active:
                # Thin yellow border to indicate selected
                painter.setPen(QPen(border_color, 2, Qt.DashLine))
                painter.setBrush(Qt.NoBrush)
                painter.drawRoundedRect(rect, 4, 4)
            _shadow_text(painter, text, cx, text_cy, font,
                         QColor(255, 255, 255, 230), as_paths, dx=3.0)
        else:
            # Empty placeholder: dashed outline
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
        if vernacular:
            vernacular = vernacular[0].upper() + vernacular[1:]

        bottom_margin = 70
        line_gap = 18
        y = H - bottom_margin

        if self._stats_text:
            f = _make_font(["Menlo", "Consolas", "Courier New"], self._ts_title(22))
            f.setLetterSpacing(QFont.AbsoluteSpacing, 0.6)
            _shadow_text(painter, self._stats_text, W / 2, y, f,
                         QColor(200, 200, 200, 220), as_paths)
            # Draw copyright just below the stats line, centred
            if self._copyright_text:
                cf = _make_font(["Helvetica Neue", "Helvetica", "Arial"], self._ts(26))
                painter.setFont(cf)
                fm = painter.fontMetrics()
                tw = fm.horizontalAdvance(self._copyright_text)
                cx_pos = W / 2 - tw / 2
                cy_pos = y + int(self._ts(26) * 4 / 3) + 10
                if as_paths:
                    path = QPainterPath()
                    path.addText(cx_pos, cy_pos, cf, self._copyright_text)
                    painter.fillPath(path, QBrush(QColor(210, 210, 210, 190)))
                else:
                    painter.setFont(cf)
                    painter.setPen(QColor(200, 200, 200, 190))
                    painter.drawText(QPointF(cx_pos, cy_pos), self._copyright_text)
            y -= (int(self._ts_title(22) * 4 / 3) + line_gap)

        if sci_name:
            f = _make_font(["Georgia", "Times New Roman"], self._ts_title(44), italic=True)
            _shadow_text(painter, sci_name, W / 2, y, f,
                         QColor(225, 225, 225, 240), as_paths)
            y -= (int(self._ts_title(44) * 4 / 3) + line_gap)

        if vernacular:
            f = _make_font(["Helvetica Neue", "Helvetica", "Arial"],
                           self._ts_title(72), bold=True)
            _shadow_text(painter, vernacular, W / 2, y, f,
                         QColor(255, 255, 255, 255), as_paths)

    def _draw_copyright(self, painter: QPainter, W: int, H: int,
                        as_paths: bool) -> None:
        if not self._copyright_text:
            return
        f = _make_font(["Helvetica Neue", "Helvetica", "Arial"], self._ts(26))
        painter.setFont(f)
        fm = painter.fontMetrics()
        tw = fm.horizontalAdvance(self._copyright_text)
        x = W - tw - 14
        y = H - 14
        if as_paths:
            path = QPainterPath()
            path.addText(x, y, f, self._copyright_text)
            painter.fillPath(path, QBrush(QColor(210, 210, 210, 190)))
        else:
            painter.setFont(f)
            painter.setPen(QColor(200, 200, 200, 190))
            painter.drawText(QPointF(x, y), self._copyright_text)

    # ── Data helpers ──────────────────────────────────────────────────────────

    def _build_spore_stats(self) -> str:
        import re as _re
        stored = self._obs.get("spore_statistics")
        if stored:
            ml = _re.search(r"[Ss]pores?\s*:\s*([^,]+?)\s*um\s*x", stored)
            mw = _re.search(r"\s*x\s*([^,]+?)\s*um", stored)
            if ml and mw:
                def _nums(seg):
                    return _re.findall(r"[0-9]+(?:\.[0-9]+)?", seg)
                def _midpoint(seg):
                    nums = _nums(seg)
                    if len(nums) >= 2:
                        return f"{(float(nums[0]) + float(nums[-1])) / 2:.1f}"
                    return nums[0] if nums else None
                l_val = _midpoint(ml.group(1))
                w_val = _midpoint(mw.group(1))
                nm = _re.search(r"\bn\s*=\s*(\d+)", stored)
                n_str = f"  n={nm.group(1)}" if nm else ""
                mq5 = _re.search(r"\bQ\s*=\s*([0-9.]+)", stored)
                mq95 = _re.search(r"\bQ\s*=\s*[0-9.]+\s*[–-]\s*([0-9.]+)", stored)
                if mq5 and mq95:
                    q_str = f"  Q = {mq5.group(1)}–{mq95.group(1)}"
                elif mq5:
                    q_str = f"  Q = {mq5.group(1)}"
                else:
                    q_str = ""
                if l_val and w_val:
                    return f"Spores: {l_val} × {w_val} µm{q_str}{n_str}"

        stats = MeasurementDB.get_statistics_for_observation(
            self._obs_id, measurement_category="spores")
        if not stats:
            return ""
        try:
            l_mean = float(stats["length_mean"])
            w_mean = float(stats["width_mean"])
            n = int(stats.get("count", 0))
            n_str = f"  n={n}" if n else ""
            q5 = stats.get("ratio_p5")
            q95 = stats.get("ratio_p95")
            q_str = (f"  Q = {float(q5):.2f}–{float(q95):.2f}"
                     if q5 is not None and q95 is not None else "")
            return f"Spores: {l_mean:.1f} × {w_mean:.1f} µm{q_str}{n_str}"
        except (KeyError, TypeError, ValueError):
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

    # ── Export ────────────────────────────────────────────────────────────────

    def _on_export(self) -> None:
        path, fmt = self._ask_export_path()
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
        self._paint_native(p, as_paths=False)
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
        gen.setDescription("Generated by MycoLog")
        p = QPainter(gen)
        self._paint_native(p, as_paths=True)
        p.end()
        self._save_export_dir(path)
        self._hint_ctrl.set_status(
            self.tr("SVG saved — {filename}").format(filename=Path(path).name),
            6000, "success")
