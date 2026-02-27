"""Zoomable and pannable image widget with measurement overlays."""
from PySide6.QtWidgets import QLabel, QWidget, QVBoxLayout
from PySide6.QtGui import QPixmap, QPainter, QPen, QColor, QCursor, QTransform, QPolygonF, QPainterPath
from PySide6.QtCore import Qt, QPoint, QRect, QPointF, Signal, QRectF, QSize
from PySide6.QtSvg import QSvgGenerator
import math


class ZoomableImageLabel(QLabel):
    """Custom label that supports zoom, pan, and measurement overlays."""

    clicked = Signal(QPointF)  # Emits click position in original image coordinates
    cropChanged = Signal(object)  # Emits (x1, y1, x2, y2) in image coords or None
    cropPreviewChanged = Signal(object)  # Emits live crop preview box in image coords or None

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAlignment(Qt.AlignCenter)
        self.setMouseTracking(True)

        # Image data
        self.original_pixmap = None
        self._full_image_path = None
        self._preview_is_scaled = False
        self._preview_tag_text = ""
        self._corner_tag_text = ""
        self._corner_tag_color = QColor(149, 165, 166)
        self._full_loaded = False
        self.measurement_lines = []
        self.debug_line_layers = []
        self.measurement_rectangles = []
        self.preview_line = None  # Temporary line being drawn
        self.preview_rect = None  # Temporary rectangle preview
        self.objective_text = ""
        self.objective_color = QColor(52, 152, 219)
        self.measure_color = QColor("#0044aa")
        self.show_line_endcaps = True
        self.show_measure_labels = False
        self.measurement_labels = []
        self.show_scale_bar = False
        self.scale_bar_um = 10.0
        self.scale_bar_bg_alpha = 102
        self.export_measure_label_scale_multiplier = 1.0
        self.show_copyright = False
        self.copyright_text = ""
        self.microns_per_pixel = 0.5
        self.show_measure_overlays = True
        self.overlay_boxes = []
        self.hover_rect_index = -1
        self.hover_line_index = -1
        self.selected_rect_index = -1
        self.selected_line_indices = set()
        self.pan_without_shift = False
        self.pan_click_candidate = False
        self.measurement_active = False

        # Zoom and pan state
        self.zoom_level = 1.0
        self.min_zoom = 0.1
        self.max_zoom = 10.0
        self.pan_offset = QPointF(0, 0)
        self._auto_fit_pending = False

        # Pan interaction state
        self.is_panning = False
        self.pan_start_pos = QPointF()
        self.pan_start_offset = QPointF()

        # Mouse tracking for preview line
        self.current_mouse_pos = None
        self.crop_mode = False
        self.crop_start = None
        self.crop_box = None
        self.crop_preview = None
        self.crop_aspect_ratio = None
        self.crop_hovered = False
        self.crop_dragging = False
        self.crop_drag_start = None
        self.crop_drag_initial_box = None
        self.crop_corner_hover_index = -1
        self.crop_corner_dragging = False
        self.crop_corner_drag_index = -1
        self.crop_corner_drag_anchor = None

    def set_image_sources(self, pixmap, full_path=None, preview_scaled=False):
        """Set image with optional full-resolution source."""
        self.original_pixmap = pixmap
        self._full_image_path = str(full_path) if full_path else None
        self._preview_is_scaled = bool(preview_scaled)
        self._preview_tag_text = "Preview" if self._preview_is_scaled else ""
        self._corner_tag_text = ""
        self._full_loaded = not self._preview_is_scaled
        self.pan_offset = QPointF(0, 0)
        self.reset_view()
        self._auto_fit_pending = True

    def _measure_stroke_style(self, color=None):
        """Return SVG-inspired thin/glow colors and blend mode for a stroke color."""
        base = QColor(color) if color is not None else QColor(self.measure_color)
        if not base.isValid():
            base = QColor("#0044aa")

        presets = (
            # Representative (old palette/default) -> SVG thin/glow + blend
            {"match": QColor("#1E90FF"), "thin": QColor("#0044aa"), "glow": QColor("#2a7fff"), "opacity": 0.576531, "blend": "screen"},
            {"match": QColor("#FF3B30"), "thin": QColor("#d40000"), "glow": QColor("#d40000"), "opacity": 0.658163, "blend": "screen"},
            {"match": QColor("#2ECC71"), "thin": QColor("#00aa00"), "glow": QColor("#00aa00"), "opacity": 0.658163, "blend": "screen"},
            {"match": QColor("#E056FD"), "thin": QColor("#ff00ff"), "glow": QColor("#ff00ff"), "opacity": 0.433674, "blend": "screen"},
            {"match": QColor("#ECAF11"), "thin": QColor("#ffd42a"), "glow": QColor("#ffdd55"), "opacity": 0.658163, "blend": "overlay"},
            {"match": QColor("#1CEBEB"), "thin": QColor("#00ffff"), "glow": QColor("#00ffff"), "opacity": 0.658163, "blend": "overlay"},
            {"match": QColor("#000000"), "thin": QColor("#000000"), "glow": QColor("#000000"), "opacity": 0.658163, "blend": "overlay"},
            # Also match the older widget default blue if encountered
            {"match": QColor("#3498db"), "thin": QColor("#0044aa"), "glow": QColor("#2a7fff"), "opacity": 0.576531, "blend": "screen"},
        )

        def _dist2(c1: QColor, c2: QColor) -> int:
            dr = int(c1.red()) - int(c2.red())
            dg = int(c1.green()) - int(c2.green())
            db = int(c1.blue()) - int(c2.blue())
            return dr * dr + dg * dg + db * db

        chosen = min(presets, key=lambda p: _dist2(base, p["match"]))
        thin = QColor(chosen["thin"])
        thin.setAlpha(max(1, base.alpha()))
        glow = QColor(chosen["glow"])
        glow.setAlpha(max(1, min(255, int(round(255 * float(chosen["opacity"]))))))  # type: ignore[arg-type]
        return {
            "thin": thin,
            "glow": glow,
            "blend": str(chosen["blend"]),
        }

    def _light_stroke_color(self):
        """Backward-compatible glow color helper used by some preview/legacy paths."""
        return QColor(self._measure_stroke_style().get("glow"))

    @staticmethod
    def _set_named_composition_mode(painter: QPainter, composition: str | None) -> None:
        comp = (composition or "").strip().lower()
        if comp == "overlay":
            painter.setCompositionMode(QPainter.CompositionMode_Overlay)
        elif comp == "screen":
            painter.setCompositionMode(QPainter.CompositionMode_Screen)
        elif comp == "plus":
            painter.setCompositionMode(QPainter.CompositionMode_Plus)
        elif comp == "lighten":
            painter.setCompositionMode(QPainter.CompositionMode_Lighten)

    def _draw_dual_stroke_line(
        self,
        painter: QPainter,
        p1,
        p2,
        color=None,
        thin_width: float = 1.0,
        wide_width: float | None = None,
        dashed: bool = False,
    ) -> None:
        style = self._measure_stroke_style(color=color)
        thin_pen = QPen(QColor(style["thin"]), max(1.0, float(thin_width)))
        wide_pen = QPen(QColor(style["glow"]), max(max(1.0, float(thin_width) * 3.0), float(wide_width or 0.0)))
        if dashed:
            thin_pen.setStyle(Qt.DashLine)
            wide_pen.setStyle(Qt.DashLine)

        use_blend = True
        try:
            device = painter.device()
        except Exception:
            device = None
        if isinstance(device, QSvgGenerator):
            use_blend = False

        if use_blend:
            painter.save()
            self._set_named_composition_mode(painter, str(style.get("blend") or ""))
            painter.setPen(wide_pen)
            painter.drawLine(p1, p2)
            painter.restore()
        else:
            painter.setPen(wide_pen)
            painter.drawLine(p1, p2)

        painter.setPen(thin_pen)
        painter.drawLine(p1, p2)

    def _draw_dual_stroke_polygon(
        self,
        painter: QPainter,
        polygon: QPolygonF,
        color=None,
        thin_width: float = 1.0,
        wide_width: float | None = None,
        dashed: bool = False,
    ) -> None:
        style = self._measure_stroke_style(color=color)
        thin_pen = QPen(QColor(style["thin"]), max(1.0, float(thin_width)))
        wide_pen = QPen(QColor(style["glow"]), max(max(1.0, float(thin_width) * 3.0), float(wide_width or 0.0)))
        if dashed:
            thin_pen.setStyle(Qt.DashLine)
            wide_pen.setStyle(Qt.DashLine)

        use_blend = True
        try:
            device = painter.device()
        except Exception:
            device = None
        if isinstance(device, QSvgGenerator):
            use_blend = False

        if use_blend:
            painter.save()
            self._set_named_composition_mode(painter, str(style.get("blend") or ""))
            painter.setPen(wide_pen)
            painter.drawPolygon(polygon)
            painter.restore()
        else:
            painter.setPen(wide_pen)
            painter.drawPolygon(polygon)

        painter.setPen(thin_pen)
        painter.drawPolygon(polygon)

    def _compute_corners_from_lines(self, line1, line2):
        """Compute rectangle corners from two measurement lines."""
        p1 = QPointF(line1[0].x(), line1[0].y())
        p2 = QPointF(line1[1].x(), line1[1].y())
        p3 = QPointF(line2[0].x(), line2[0].y())
        p4 = QPointF(line2[1].x(), line2[1].y())

        length_vec = p2 - p1
        length_len = math.sqrt(length_vec.x() ** 2 + length_vec.y() ** 2)
        width_vec = p4 - p3
        width_len = math.sqrt(width_vec.x() ** 2 + width_vec.y() ** 2)
        if length_len <= 0 or width_len <= 0:
            return None

        length_dir = QPointF(length_vec.x() / length_len, length_vec.y() / length_len)
        width_dir = QPointF(-length_dir.y(), length_dir.x())

        line1_mid = QPointF((p1.x() + p2.x()) / 2, (p1.y() + p2.y()) / 2)
        line2_mid = QPointF((p3.x() + p4.x()) / 2, (p3.y() + p4.y()) / 2)
        center = QPointF((line1_mid.x() + line2_mid.x()) / 2,
                         (line1_mid.y() + line2_mid.y()) / 2)

        half_length = length_len / 2
        half_width = width_len / 2

        return [
            center - width_dir * half_width - length_dir * half_length,
            center + width_dir * half_width - length_dir * half_length,
            center + width_dir * half_width + length_dir * half_length,
            center - width_dir * half_width + length_dir * half_length,
        ]

    def _point_in_polygon(self, point, polygon):
        """Check if a point is inside a polygon using ray casting."""
        inside = False
        n = len(polygon)
        if n < 3:
            return False
        p1 = polygon[0]
        for i in range(1, n + 1):
            p2 = polygon[i % n]
            if point.y() > min(p1.y(), p2.y()):
                if point.y() <= max(p1.y(), p2.y()):
                    if point.x() <= max(p1.x(), p2.x()):
                        if p1.y() != p2.y():
                            xinters = (point.y() - p1.y()) * (p2.x() - p1.x()) / (p2.y() - p1.y()) + p1.x()
                        if p1.x() == p2.x() or point.x() <= xinters:
                            inside = not inside
            p1 = p2
        return inside

    def _draw_rotated_label_outside(self, painter, text, edge, center, padding_px):
        """Draw centered, rotated text along an edge, outside the rectangle."""
        a, b = edge
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
        offset = (text_h / 2) + padding_px

        dir_dot = (mid.x() - center.x()) * perp_x + (mid.y() - center.y()) * perp_y
        sign = 1 if dir_dot >= 0 else -1
        label_pos = QPointF(mid.x() + perp_x * offset * sign,
                            mid.y() + perp_y * offset * sign)

        painter.save()
        painter.translate(label_pos.x(), label_pos.y())
        painter.rotate(angle_deg)
        self._draw_measure_text_with_outline(
            painter,
            int(-text_w / 2),
            int(text_h / 2),
            text,
        )
        painter.restore()

    def _draw_rotated_label_on_line(self, painter, a, b, text, padding_px=4.0):
        """Draw centered, rotated text along a line with a small offset."""
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
        offset = (text_h / 2) + padding_px
        label_pos = QPointF(mid.x() + perp_x * offset, mid.y() + perp_y * offset)
        painter.save()
        painter.translate(label_pos.x(), label_pos.y())
        painter.rotate(angle_deg)
        self._draw_measure_text_with_outline(
            painter,
            int(-text_w / 2),
            int(text_h / 2),
            text,
        )
        painter.restore()

    def _draw_measure_text_with_outline(
        self,
        painter: QPainter,
        x: int,
        y: int,
        text: str,
        color: QColor | None = None,
    ) -> None:
        """Draw measure text with Inkscape-like white outline underlay.

        Approximation of: 2.5 mm text + 1 mm white stroke @ 40% opacity,
        implemented as a size-relative stroke (~40% of current font height).
        """
        if not text:
            return
        metrics = painter.fontMetrics()
        stroke_w = max(1.0, float(metrics.height()) * 0.4)
        path = QPainterPath()
        path.addText(float(x), float(y), painter.font(), text)
        outline_pen = QPen(QColor(255, 255, 255, 102), stroke_w)
        outline_pen.setJoinStyle(Qt.RoundJoin)
        outline_pen.setCapStyle(Qt.RoundCap)
        painter.save()
        painter.setBrush(Qt.NoBrush)
        painter.setPen(outline_pen)
        painter.drawPath(path)
        painter.restore()
        painter.setPen(QColor(color) if color is not None else QColor(self.measure_color))
        painter.drawText(int(x), int(y), text)

    def _draw_text_with_outline(
        self,
        painter: QPainter,
        x: float,
        y: float,
        text: str,
        fill_color: QColor,
        outline_color: QColor,
        outline_opacity: float = 0.4,
        outline_width_ratio: float = 0.4,
    ) -> None:
        """Draw text with a stroked outline underlay (Inkscape-like)."""
        if not text:
            return
        metrics = painter.fontMetrics()
        stroke_w = max(1.0, float(metrics.height()) * float(outline_width_ratio))
        alpha = max(0, min(255, int(round(255 * float(outline_opacity)))))
        path = QPainterPath()
        path.addText(float(x), float(y), painter.font(), text)
        pen = QPen(QColor(outline_color.red(), outline_color.green(), outline_color.blue(), alpha), stroke_w)
        pen.setJoinStyle(Qt.RoundJoin)
        pen.setCapStyle(Qt.RoundCap)
        painter.save()
        painter.setBrush(Qt.NoBrush)
        painter.setPen(pen)
        painter.drawPath(path)
        painter.restore()
        painter.setPen(fill_color)
        painter.drawText(int(x), int(y), text)

    def _label_positions_from_lines(self, line1, line2, center, offset):
        """Return label positions using the same logic as the preview widget."""
        corners = self._compute_corners_from_lines(line1, line2)
        if not corners:
            return None, None

        def _offset_from_edge(a, b, center_point):
            mid = QPointF((a.x() + b.x()) / 2, (a.y() + b.y()) / 2)
            dx = b.x() - a.x()
            dy = b.y() - a.y()
            length = math.sqrt(dx * dx + dy * dy)
            if length <= 0:
                return mid
            perp_x = -dy / length
            perp_y = dx / length
            candidate_a = QPointF(mid.x() + perp_x * offset, mid.y() + perp_y * offset)
            candidate_b = QPointF(mid.x() - perp_x * offset, mid.y() - perp_y * offset)
            dist_a = (candidate_a.x() - center_point.x()) ** 2 + (candidate_a.y() - center_point.y()) ** 2
            dist_b = (candidate_b.x() - center_point.x()) ** 2 + (candidate_b.y() - center_point.y()) ** 2
            chosen = candidate_a if dist_a >= dist_b else candidate_b
            if self._point_in_polygon(chosen, corners):
                chosen = QPointF(
                    chosen.x() + perp_x * offset,
                    chosen.y() + perp_y * offset
                )
            return chosen

        left_mid = _offset_from_edge(corners[0], corners[3], center)
        top_mid = _offset_from_edge(corners[0], corners[1], center)
        return left_mid, top_mid

    def set_image(self, pixmap, preserve_view: bool = False):
        """Set the image to display, optionally preserving zoom/pan."""
        self.original_pixmap = pixmap
        self._full_image_path = None
        self._preview_is_scaled = False
        self._preview_tag_text = ""
        self._corner_tag_text = ""
        self._full_loaded = True if pixmap else False
        if not pixmap:
            self.zoom_level = 1.0
            self.pan_offset = QPointF(0, 0)
            self.update()
            return
        if not preserve_view:
            self.pan_offset = QPointF(0, 0)
            # Fit image to screen by default
            self.reset_view()
            self._auto_fit_pending = True
        else:
            self.zoom_level = max(self.min_zoom, min(self.max_zoom, self.zoom_level))
            self.update()

    def set_measurement_lines(self, lines):
        """Set the measurement lines to draw."""
        self.measurement_lines = lines
        self.hover_line_index = -1
        self.update()

    def set_debug_lines(self, layers):
        """Set debug line layers to draw (list of {lines, color, width, show_endcaps})."""
        self.debug_line_layers = layers or []
        self.update()

    def set_measurement_color(self, color):
        """Set the color for measurement overlays."""
        self.measure_color = QColor(color)
        self.update()

    def set_show_line_endcaps(self, show_endcaps: bool):
        """Toggle perpendicular end marks for measurement lines."""
        self.show_line_endcaps = bool(show_endcaps)
        self.update()

    def set_microns_per_pixel(self, mpp):
        """Set scale for converting microns to pixels."""
        if mpp and mpp > 0:
            self.microns_per_pixel = mpp
        self.update()

    def set_scale_bar(self, show, microns):
        """Toggle and set scale bar size in microns."""
        self.show_scale_bar = bool(show)
        if microns and microns > 0:
            self.scale_bar_um = float(microns)
        self.update()

    def set_export_measure_label_scale_multiplier(self, multiplier):
        """Set export-only measurement label scale multiplier (does not affect app view)."""
        try:
            value = float(multiplier)
        except Exception:
            value = 1.0
        self.export_measure_label_scale_multiplier = max(0.5, min(6.0, value))
        self.update()

    def set_copyright(self, show, text):
        """Set copyright visibility and text."""
        self.show_copyright = bool(show)
        self.copyright_text = str(text).strip() if text else ""
        self.update()

    def set_measurement_rectangles(self, rectangles):
        """Set the measurement rectangles to draw."""
        self.measurement_rectangles = rectangles
        self.hover_rect_index = -1
        self.update()

    def set_selected_rect_index(self, index):
        self.selected_rect_index = index if index is not None else -1
        self.update()

    def set_selected_line_indices(self, indices):
        self.selected_line_indices = set(indices or [])
        self.update()

    def set_measurement_labels(self, labels):
        """Set label positions and values for measurements."""
        self.measurement_labels = labels
        self.update()

    def set_show_measure_overlays(self, show_overlays):
        """Toggle measurement overlay visibility."""
        self.show_measure_overlays = bool(show_overlays)
        self.update()

    def set_show_measure_labels(self, show_labels):
        """Toggle measurement label display."""
        self.show_measure_labels = show_labels
        self.update()

    def set_corner_tag(self, text, color=None):
        """Set a tag rendered in the lower-right corner."""
        self._corner_tag_text = str(text) if text else ""
        if color is not None:
            self._corner_tag_color = QColor(color)
        self.update()

    def set_pan_without_shift(self, enabled):
        """Allow panning with a plain left-drag (no Shift)."""
        self.pan_without_shift = bool(enabled)

    def set_measurement_active(self, active):
        """Toggle measurement-active border."""
        self.measurement_active = bool(active)
        self.update()

    def get_view_state(self):
        """Return current view center (image coords) and zoom."""
        if not self.original_pixmap or self.zoom_level <= 0:
            return None
        display_rect = self.get_display_rect()
        if display_rect.isNull():
            return None
        center_screen = QPointF(self.width() / 2, self.height() / 2)
        center_x = (center_screen.x() - display_rect.x()) / self.zoom_level
        center_y = (center_screen.y() - display_rect.y()) / self.zoom_level
        return {
            "center": QPointF(center_x, center_y),
            "zoom": float(self.zoom_level),
            "size": (self.original_pixmap.width(), self.original_pixmap.height()),
        }

    def set_view_state(self, center: QPointF, zoom: float):
        """Set view based on center (image coords) and zoom."""
        self._auto_fit_pending = False
        if not self.original_pixmap:
            return
        zoom = max(self.min_zoom, min(self.max_zoom, float(zoom)))
        width = self.original_pixmap.width()
        height = self.original_pixmap.height()
        cx = max(0.0, min(float(center.x()), float(width)))
        cy = max(0.0, min(float(center.y()), float(height)))
        self.zoom_level = zoom
        self.pan_offset = QPointF(
            (width / 2 - cx) * zoom,
            (height / 2 - cy) * zoom,
        )
        self.update()

    def set_overlay_boxes(self, boxes):
        """Set extra overlay boxes drawn on top of the image."""
        self.overlay_boxes = boxes or []
        self.update()

    def clear_overlay_boxes(self):
        """Clear overlay boxes."""
        self.overlay_boxes = []
        self.update()

    def set_crop_mode(self, enabled):
        """Enable crop selection mode."""
        self.crop_mode = bool(enabled)
        if not self.crop_mode:
            self.crop_start = None
            self.crop_preview = None
            self.crop_hovered = False
            self.crop_dragging = False
            self.crop_drag_start = None
            self.crop_drag_initial_box = None
            self.crop_corner_hover_index = -1
            self.crop_corner_dragging = False
            self.crop_corner_drag_index = -1
            self.crop_corner_drag_anchor = None
        self.setCursor(Qt.CrossCursor if self.crop_mode else Qt.ArrowCursor)
        self.update()

    def set_crop_box(self, box):
        """Set current crop box (x1, y1, x2, y2) in image coords."""
        self.crop_box = box
        if not box:
            self.crop_hovered = False
            self.crop_dragging = False
            self.crop_drag_start = None
            self.crop_drag_initial_box = None
            self.crop_corner_hover_index = -1
            self.crop_corner_dragging = False
            self.crop_corner_drag_index = -1
            self.crop_corner_drag_anchor = None
        self.cropPreviewChanged.emit(self.crop_box)
        self.update()

    def set_crop_aspect_ratio(self, ratio):
        """Set a fixed crop aspect ratio (width/height). Use None to disable."""
        self.crop_aspect_ratio = ratio if ratio and ratio > 0 else None

    def clear_crop_box(self):
        """Clear current crop box."""
        self.crop_box = None
        self.crop_preview = None
        self.crop_start = None
        self.crop_hovered = False
        self.crop_dragging = False
        self.crop_drag_start = None
        self.crop_drag_initial_box = None
        self.crop_corner_hover_index = -1
        self.crop_corner_dragging = False
        self.crop_corner_drag_index = -1
        self.crop_corner_drag_anchor = None
        self.cropPreviewChanged.emit(None)
        self.update()

    def set_preview_line(self, start_point):
        """Set the start point for a preview line that follows the mouse."""
        self.preview_line = start_point
        self.update()

    def clear_preview_line(self):
        """Clear the preview line."""
        self.preview_line = None
        self.update()

    def set_preview_rectangle(self, base_start, base_end, width_dir, moving_line):
        """Set preview rectangle data based on a fixed base line and moving side."""
        self.preview_rect = {
            "base_start": base_start,
            "base_end": base_end,
            "width_dir": width_dir,
            "moving_line": moving_line,
        }
        self.update()

    def clear_preview_rectangle(self):
        """Clear the preview rectangle."""
        self.preview_rect = None
        self.update()

    def get_current_mouse_pos(self):
        """Expose the current mouse position in image coordinates."""
        return self.current_mouse_pos

    def set_objective_text(self, text):
        """Set the objective tag text."""
        self.objective_text = text
        self.update()

    def set_objective_color(self, color):
        """Set the objective tag color."""
        self.objective_color = QColor(color)
        self.update()

    def reset_view(self):
        """Reset zoom to fit image within the window."""
        if not self.original_pixmap:
            self.zoom_level = 1.0
            self.pan_offset = QPointF(0, 0)
            self.update()
            return

        # Calculate zoom level to fit image within widget
        widget_width = self.width()
        widget_height = self.height()
        image_width = self.original_pixmap.width()
        image_height = self.original_pixmap.height()

        # Calculate scale to fit while maintaining aspect ratio
        scale_x = widget_width / image_width if image_width > 0 else 1.0
        scale_y = widget_height / image_height if image_height > 0 else 1.0
        self.zoom_level = min(scale_x, scale_y, 1.0)  # Don't zoom in beyond 1.0

        # Reset pan to center
        self.pan_offset = QPointF(0, 0)
        self.update()

    def set_zoom_1_to_1(self):
        """Set zoom so that 1 image pixel equals 1 screen pixel."""
        if not self.original_pixmap:
            return
        self._auto_fit_pending = False
        if self._preview_is_scaled and not self._full_loaded:
            self._load_full_resolution()
        self.zoom_level = 1.0
        self.pan_offset = QPointF(0, 0)
        self.update()

    def zoom_in(self):
        """Zoom in by 20%."""
        self._auto_fit_pending = False
        self.zoom_level = min(self.zoom_level * 1.2, self.max_zoom)
        if self.zoom_level > 1.0 and self._preview_is_scaled and not self._full_loaded:
            self._load_full_resolution()
        self.update()

    def zoom_out(self):
        """Zoom out by 20%."""
        self._auto_fit_pending = False
        self.zoom_level = max(self.zoom_level / 1.2, self.min_zoom)
        self.update()

    def wheelEvent(self, event):
        """Handle mouse wheel for zooming."""
        if not self.original_pixmap:
            return
        self._auto_fit_pending = False

        # Zoom toward mouse cursor position
        delta = event.angleDelta().y()
        if delta > 0:
            zoom_factor = 1.1
        else:
            zoom_factor = 0.9

        old_zoom = self.zoom_level
        self.zoom_level = max(self.min_zoom, min(self.max_zoom, self.zoom_level * zoom_factor))

        if old_zoom != self.zoom_level:
            # Get mouse position relative to widget center
            cursor_pos = event.position()
            widget_center = QPointF(self.width() / 2, self.height() / 2)

            # Calculate cursor position relative to center + current pan offset
            relative_pos = cursor_pos - widget_center - self.pan_offset

            # Scale the relative position by the zoom change
            zoom_ratio = self.zoom_level / old_zoom
            new_relative_pos = relative_pos * zoom_ratio

            # Update pan offset to keep point under cursor fixed
            self.pan_offset = cursor_pos - widget_center - new_relative_pos

        if self.zoom_level > 1.0 and self._preview_is_scaled and not self._full_loaded:
            self._load_full_resolution()
        self.update()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if self._auto_fit_pending and self.original_pixmap:
            if self.width() > 1 and self.height() > 1:
                self.reset_view()
                self._auto_fit_pending = False

    def _load_full_resolution(self):
        if not self._full_image_path:
            return
        try:
            full_pixmap = QPixmap(self._full_image_path)
        except Exception:
            return
        if full_pixmap.isNull() or not self.original_pixmap:
            return

        preview_pixmap = self.original_pixmap
        preview_width = max(1, preview_pixmap.width())
        preview_height = max(1, preview_pixmap.height())
        full_width = max(1, full_pixmap.width())
        full_height = max(1, full_pixmap.height())

        center_preview = self.screen_to_image(QPointF(self.width() / 2, self.height() / 2))
        if not center_preview:
            center_preview = QPointF(preview_width / 2, preview_height / 2)

        scale_x = full_width / preview_width
        scale_y = full_height / preview_height
        center_full = QPointF(center_preview.x() * scale_x, center_preview.y() * scale_y)

        new_zoom = self.zoom_level * (preview_width / full_width)
        new_zoom = max(self.min_zoom, min(self.max_zoom, new_zoom))

        if self.overlay_boxes:
            for item in self.overlay_boxes:
                box = item.get("box") if isinstance(item, dict) else None
                if not box:
                    continue
                item["box"] = (
                    box[0] * scale_x,
                    box[1] * scale_y,
                    box[2] * scale_x,
                    box[3] * scale_y,
                )
        if self.crop_box:
            self.crop_box = (
                self.crop_box[0] * scale_x,
                self.crop_box[1] * scale_y,
                self.crop_box[2] * scale_x,
                self.crop_box[3] * scale_y,
            )

        self.original_pixmap = full_pixmap
        self.zoom_level = new_zoom
        self._preview_is_scaled = False
        self._preview_tag_text = ""
        self._full_loaded = True

        scaled_width = self.original_pixmap.width() * self.zoom_level
        scaled_height = self.original_pixmap.height() * self.zoom_level
        self.pan_offset = QPointF(
            scaled_width / 2 - center_full.x() * self.zoom_level,
            scaled_height / 2 - center_full.y() * self.zoom_level,
        )
        self.update()

    def ensure_full_resolution(self):
        """Load full-resolution image if currently showing a preview."""
        if self._preview_is_scaled and not self._full_loaded:
            self._load_full_resolution()

    @staticmethod
    def _normalize_crop_box(box):
        if not box or len(box) != 4:
            return None
        x1, y1, x2, y2 = box
        return (
            min(float(x1), float(x2)),
            min(float(y1), float(y2)),
            max(float(x1), float(x2)),
            max(float(y1), float(y2)),
        )

    def _normalized_crop_box_from_points(self, start: QPointF, end: QPointF):
        if not self.original_pixmap or not start or not end:
            return None
        width = float(self.original_pixmap.width())
        height = float(self.original_pixmap.height())
        x1 = max(0.0, min(width, min(start.x(), end.x())))
        y1 = max(0.0, min(height, min(start.y(), end.y())))
        x2 = max(0.0, min(width, max(start.x(), end.x())))
        y2 = max(0.0, min(height, max(start.y(), end.y())))
        return (x1, y1, x2, y2)

    def _emit_crop_preview_changed(self, box) -> None:
        normalized = self._normalize_crop_box(box) if box else None
        self.cropPreviewChanged.emit(normalized)

    @staticmethod
    def _crop_corner_points(crop_box):
        normalized = ZoomableImageLabel._normalize_crop_box(crop_box)
        if not normalized:
            return []
        x1, y1, x2, y2 = normalized
        return [
            QPointF(x1, y1),  # top-left
            QPointF(x2, y1),  # top-right
            QPointF(x2, y2),  # bottom-right
            QPointF(x1, y2),  # bottom-left
        ]

    def _crop_corner_screen_points(self, crop_box):
        if not self.original_pixmap:
            return []
        display_rect = self.get_display_rect()
        points = []
        for pt in self._crop_corner_points(crop_box):
            points.append(
                QPointF(
                    display_rect.x() + pt.x() * self.zoom_level,
                    display_rect.y() + pt.y() * self.zoom_level,
                )
            )
        return points

    def _crop_corner_hit_test(self, screen_pos, crop_box=None) -> int:
        if crop_box is None:
            crop_box = self.crop_box
        if not crop_box or not self.original_pixmap:
            return -1
        pos = QPointF(screen_pos)
        radius = 8.0
        best_idx = -1
        best_dist_sq = radius * radius
        for idx, pt in enumerate(self._crop_corner_screen_points(crop_box)):
            dx = pos.x() - pt.x()
            dy = pos.y() - pt.y()
            dist_sq = dx * dx + dy * dy
            if dist_sq <= best_dist_sq:
                best_idx = idx
                best_dist_sq = dist_sq
        return best_idx

    @staticmethod
    def _crop_corner_cursor(index: int):
        if index in (0, 2):
            return Qt.SizeFDiagCursor
        if index in (1, 3):
            return Qt.SizeBDiagCursor
        return None

    def _update_crop_corner_hover(self, screen_pos) -> int:
        hovered = self._crop_corner_hit_test(screen_pos)
        if hovered != self.crop_corner_hover_index:
            self.crop_corner_hover_index = hovered
            self.update()
        return hovered

    def _corner_resize_box(self, anchor: QPointF, current: QPointF):
        if not self.original_pixmap or not anchor or not current:
            return None
        end = QPointF(current)
        if self.crop_aspect_ratio:
            end = self._constrain_crop_point(anchor, end, self.crop_aspect_ratio)
        return self._normalized_crop_box_from_points(anchor, end)

    def mousePressEvent(self, event):
        """Handle mouse press for panning or clicking."""
        if event.button() == Qt.LeftButton:
            if self.original_pixmap and self.crop_box:
                corner_index = self._crop_corner_hit_test(event.position())
                if corner_index >= 0:
                    corners = self._crop_corner_points(self.crop_box)
                    if len(corners) == 4:
                        self.crop_corner_dragging = True
                        self.crop_corner_drag_index = corner_index
                        self.crop_corner_drag_anchor = QPointF(corners[(corner_index + 2) % 4])
                        self.crop_corner_hover_index = corner_index
                        cursor = self._crop_corner_cursor(corner_index)
                        if cursor is not None:
                            self.setCursor(cursor)
                        self.update()
                        return
                start = self.screen_to_image(event.position())
                if start and self._point_in_crop_box(start, self.crop_box):
                    self.crop_dragging = True
                    self.crop_drag_start = QPointF(start)
                    self.crop_drag_initial_box = tuple(self.crop_box)
                    self.crop_hovered = True
                    self.setCursor(Qt.ClosedHandCursor)
                    self.update()
                    return
            if self.crop_mode:
                if self.original_pixmap:
                    start = self.screen_to_image(event.position())
                    if start:
                        if self.crop_start is None:
                            self.crop_start = start
                            self.crop_preview = (start, start)
                            self._emit_crop_preview_changed(None)
                            self.update()
                        else:
                            self._finalize_crop(start)
                return
            if event.modifiers() & Qt.ShiftModifier or self.pan_without_shift:
                # Start panning
                self.is_panning = True
                self.pan_start_pos = event.position()
                self.pan_start_offset = QPointF(self.pan_offset)
                self.setCursor(Qt.ClosedHandCursor)
                self.pan_click_candidate = self.pan_without_shift
            else:
                # Regular click - emit position in original image coordinates
                if self.original_pixmap:
                    orig_pos = self.screen_to_image(event.position())
                    if orig_pos:
                        self.clicked.emit(orig_pos)

    def mouseMoveEvent(self, event):
        """Handle mouse move for panning and preview line."""
        if self.crop_corner_dragging and self.original_pixmap:
            image_pos = self.screen_to_image(event.position())
            if image_pos and self.crop_corner_drag_anchor is not None:
                resized_box = self._corner_resize_box(self.crop_corner_drag_anchor, image_pos)
                if resized_box:
                    x1, y1, x2, y2 = resized_box
                    if (x2 - x1) >= 2 and (y2 - y1) >= 2 and resized_box != self.crop_box:
                        self.crop_box = resized_box
                        self._emit_crop_preview_changed(self.crop_box)
                        self.update()
            return
        if self.crop_dragging and self.original_pixmap:
            image_pos = self.screen_to_image(event.position())
            if image_pos and self.crop_drag_start and self.crop_drag_initial_box:
                x1, y1, x2, y2 = self.crop_drag_initial_box
                dx = image_pos.x() - self.crop_drag_start.x()
                dy = image_pos.y() - self.crop_drag_start.y()
                width = float(self.original_pixmap.width())
                height = float(self.original_pixmap.height())
                dx = max(-x1, min(dx, width - x2))
                dy = max(-y1, min(dy, height - y2))
                moved_box = (x1 + dx, y1 + dy, x2 + dx, y2 + dy)
                if moved_box != self.crop_box:
                    self.crop_box = moved_box
                    self._emit_crop_preview_changed(self.crop_box)
                    self.update()
            return
        if self.crop_mode:
            image_pos = self.screen_to_image(event.position()) if self.original_pixmap else None
            if self.crop_start and self.original_pixmap:
                current = image_pos
                if current:
                    if self.crop_aspect_ratio:
                        current = self._constrain_crop_point(self.crop_start, current, self.crop_aspect_ratio)
                    self.crop_preview = (self.crop_start, current)
                    self._emit_crop_preview_changed(self._normalized_crop_box_from_points(self.crop_start, current))
                    self.update()
                return
            corner_hover = self._update_crop_corner_hover(event.position())
            hovered = bool(image_pos and self.crop_box and self._point_in_crop_box(image_pos, self.crop_box))
            if hovered != self.crop_hovered:
                self.crop_hovered = hovered
                self.update()
            corner_cursor = self._crop_corner_cursor(corner_hover)
            if corner_cursor is not None:
                self.setCursor(corner_cursor)
            else:
                self.setCursor(Qt.OpenHandCursor if hovered else Qt.CrossCursor)
            return
        # Track mouse position for preview line
        if self.original_pixmap:
            self.current_mouse_pos = self.screen_to_image(event.position())
            self._update_crop_hover(event.position())
            self._update_crop_corner_hover(event.position())
            self._update_hover_rect(event.position())
            self._update_hover_lines(event.position())

        if self.is_panning:
            delta = event.position() - self.pan_start_pos
            self.pan_offset = self.pan_start_offset + delta
            if self.pan_click_candidate and delta.manhattanLength() > 3:
                self.pan_click_candidate = False
            self.update()
        else:
            # Update cursor for shift+hover
            if event.modifiers() & Qt.ShiftModifier:
                self.setCursor(Qt.OpenHandCursor)
            elif self.crop_corner_hover_index >= 0 and self.crop_box:
                cursor = self._crop_corner_cursor(self.crop_corner_hover_index)
                self.setCursor(cursor if cursor is not None else Qt.ArrowCursor)
            elif self.crop_hovered and self.crop_box:
                self.setCursor(Qt.OpenHandCursor)
            else:
                self.setCursor(Qt.ArrowCursor)

            # Update if we have a preview line
            if self.preview_line is not None or self.preview_rect is not None:
                self.update()

    def mouseReleaseEvent(self, event):
        """Handle mouse release."""
        if event.button() == Qt.LeftButton and self.crop_corner_dragging:
            self.crop_corner_dragging = False
            self.crop_corner_drag_index = -1
            self.crop_corner_drag_anchor = None
            if self.crop_box:
                self.cropChanged.emit(self.crop_box)
            if self.crop_mode:
                corner_cursor = self._crop_corner_cursor(self.crop_corner_hover_index)
                if corner_cursor is not None:
                    self.setCursor(corner_cursor)
                else:
                    self.setCursor(Qt.OpenHandCursor if self.crop_hovered else Qt.CrossCursor)
            else:
                corner_cursor = self._crop_corner_cursor(self.crop_corner_hover_index)
                if corner_cursor is not None:
                    self.setCursor(corner_cursor)
                else:
                    self.setCursor(Qt.OpenHandCursor if self.crop_hovered else Qt.ArrowCursor)
            self.update()
            return
        if event.button() == Qt.LeftButton and self.crop_dragging:
            self.crop_dragging = False
            self.crop_drag_start = None
            self.crop_drag_initial_box = None
            if self.crop_box:
                self.cropChanged.emit(self.crop_box)
            if self.crop_mode:
                self.setCursor(Qt.OpenHandCursor if self.crop_hovered else Qt.CrossCursor)
            else:
                self.setCursor(Qt.OpenHandCursor if self.crop_hovered else Qt.ArrowCursor)
            self.update()
            return
        if event.button() == Qt.LeftButton and self.is_panning:
            self.is_panning = False
            self.setCursor(Qt.ArrowCursor)
            if self.pan_click_candidate and self.original_pixmap:
                orig_pos = self.screen_to_image(event.position())
                if orig_pos:
                    self.clicked.emit(orig_pos)
            self.pan_click_candidate = False

    def _finalize_crop(self, end_point):
        if not self.crop_start or not self.original_pixmap:
            self.crop_start = None
            self.crop_preview = None
            self._emit_crop_preview_changed(self.crop_box)
            return
        if self.crop_aspect_ratio:
            end_point = self._constrain_crop_point(self.crop_start, end_point, self.crop_aspect_ratio)
        width = self.original_pixmap.width()
        height = self.original_pixmap.height()
        x1 = max(0.0, min(self.crop_start.x(), end_point.x()))
        y1 = max(0.0, min(self.crop_start.y(), end_point.y()))
        x2 = min(float(width), max(self.crop_start.x(), end_point.x()))
        y2 = min(float(height), max(self.crop_start.y(), end_point.y()))
        if (x2 - x1) >= 2 and (y2 - y1) >= 2:
            self.crop_box = (x1, y1, x2, y2)
        else:
            self.crop_box = None
        self.cropChanged.emit(self.crop_box)
        self.crop_start = None
        self.crop_preview = None
        self._emit_crop_preview_changed(self.crop_box)
        self.update()

    def _constrain_crop_point(self, start, current, ratio):
        dx = current.x() - start.x()
        dy = current.y() - start.y()
        if dx == 0 and dy == 0:
            return current
        abs_dx = abs(dx)
        abs_dy = abs(dy)
        if abs_dy == 0:
            abs_dy = 1e-6
        if abs_dx / abs_dy > ratio:
            target_w = abs_dx
            target_h = target_w / ratio
        else:
            target_h = abs_dy
            target_w = target_h * ratio
        sx = 1 if dx >= 0 else -1
        sy = 1 if dy >= 0 else -1
        new_x = start.x() + sx * target_w
        new_y = start.y() + sy * target_h
        if self.original_pixmap:
            w = float(self.original_pixmap.width())
            h = float(self.original_pixmap.height())
            new_x = max(0.0, min(w, new_x))
            new_y = max(0.0, min(h, new_y))
        return QPointF(new_x, new_y)

    @staticmethod
    def _point_in_crop_box(point: QPointF, crop_box: tuple[float, float, float, float]) -> bool:
        x1, y1, x2, y2 = crop_box
        left = min(x1, x2)
        top = min(y1, y2)
        right = max(x1, x2)
        bottom = max(y1, y2)
        return left <= point.x() <= right and top <= point.y() <= bottom

    def _update_crop_hover(self, screen_pos) -> None:
        if not self.original_pixmap or not self.crop_box:
            if self.crop_hovered:
                self.crop_hovered = False
                self.update()
            return
        image_pos = self.screen_to_image(screen_pos)
        hovered = bool(image_pos and self._point_in_crop_box(image_pos, self.crop_box))
        if hovered != self.crop_hovered:
            self.crop_hovered = hovered
            self.update()

    def _update_hover_rect(self, screen_pos):
        """Update which measurement rectangle is under the cursor."""
        if self.measurement_active:
            if self.hover_rect_index != -1:
                self.hover_rect_index = -1
                self.update()
            return
        if not self.measurement_rectangles or not self.original_pixmap:
            if self.hover_rect_index != -1:
                self.hover_rect_index = -1
                self.update()
            return

        image_pos = self.screen_to_image(screen_pos)
        if not image_pos:
            if self.hover_rect_index != -1:
                self.hover_rect_index = -1
                self.update()
            return

        hovered = -1
        for idx, rect in enumerate(self.measurement_rectangles):
            if self._point_in_polygon(image_pos, rect):
                hovered = idx
                break

        if hovered != self.hover_rect_index:
            self.hover_rect_index = hovered
            self.update()

    def _update_hover_lines(self, screen_pos):
        """Update which measurement line is under the cursor."""
        if self.measurement_active:
            if self.hover_line_index != -1:
                self.hover_line_index = -1
                self.update()
            return
        if not self.measurement_lines or not self.original_pixmap:
            if self.hover_line_index != -1:
                self.hover_line_index = -1
                self.update()
            return

        image_pos = self.screen_to_image(screen_pos)
        if not image_pos:
            if self.hover_line_index != -1:
                self.hover_line_index = -1
                self.update()
            return

        threshold = 6.0 / self.zoom_level if self.zoom_level else 6.0
        hovered = -1
        best_dist = threshold
        for idx, line in enumerate(self.measurement_lines):
            p1 = QPointF(line[0], line[1])
            p2 = QPointF(line[2], line[3])
            dist = self._distance_point_to_segment(image_pos, p1, p2)
            if dist <= best_dist:
                best_dist = dist
                hovered = idx

        if hovered != self.hover_line_index:
            self.hover_line_index = hovered
            self.update()

    def _distance_point_to_segment(self, point, a, b):
        """Return distance between a point and a line segment."""
        ab = b - a
        ap = point - a
        ab_len_sq = ab.x() * ab.x() + ab.y() * ab.y()
        if ab_len_sq == 0:
            return math.sqrt(ap.x() * ap.x() + ap.y() * ap.y())
        t = (ap.x() * ab.x() + ap.y() * ab.y()) / ab_len_sq
        t = max(0.0, min(1.0, t))
        closest = QPointF(a.x() + ab.x() * t, a.y() + ab.y() * t)
        dx = point.x() - closest.x()
        dy = point.y() - closest.y()
        return math.sqrt(dx * dx + dy * dy)

    def screen_to_image(self, screen_pos):
        """Convert screen position to original image coordinates."""
        if not self.original_pixmap:
            return None

        # Get the displayed image rect (centered, zoomed)
        display_rect = self.get_display_rect()

        # Check if click is within image
        if not display_rect.contains(screen_pos.toPoint()):
            return None

        # Convert to image coordinates
        x = (screen_pos.x() - display_rect.x()) / self.zoom_level
        y = (screen_pos.y() - display_rect.y()) / self.zoom_level

        # Clamp to image bounds
        if 0 <= x < self.original_pixmap.width() and 0 <= y < self.original_pixmap.height():
            return QPointF(x, y)
        return None

    def get_display_rect(self):
        """Get the rectangle where the image is displayed."""
        if not self.original_pixmap:
            return QRect()

        # Calculate scaled size
        scaled_width = self.original_pixmap.width() * self.zoom_level
        scaled_height = self.original_pixmap.height() * self.zoom_level

        # Center position with pan offset
        label_center = QPointF(self.width() / 2, self.height() / 2)
        x = label_center.x() - scaled_width / 2 + self.pan_offset.x()
        y = label_center.y() - scaled_height / 2 + self.pan_offset.y()

        return QRect(int(x), int(y), int(scaled_width), int(scaled_height))

    def export_annotated_pixmap(self):
        """Render annotations on the original image resolution."""
        if not self.original_pixmap:
            return None

        result = QPixmap(self.original_pixmap.size())
        result.fill(Qt.transparent)

        painter = QPainter(result)
        scale_factor = self._export_overlay_scale_factor()

        self._render_export(painter, scale_factor)

        painter.end()
        return result

    def export_annotated_svg(self, filename, target_size=None):
        """Export annotations to an SVG with the image embedded."""
        if not self.original_pixmap:
            return False

        base_size = self.original_pixmap.size()
        target_w = base_size.width()
        target_h = base_size.height()
        if target_size:
            target_w = max(1, int(target_size[0]))
            target_h = max(1, int(target_size[1]))

        generator = QSvgGenerator()
        generator.setFileName(filename)
        generator.setSize(QSize(target_w, target_h))
        generator.setViewBox(QRect(0, 0, base_size.width(), base_size.height()))
        generator.setTitle("MycoLog Export")
        generator.setDescription("Annotated image export")

        painter = QPainter(generator)
        scale_factor = self._export_overlay_scale_factor()
        self._render_export(painter, scale_factor)
        painter.end()
        return True

    def _export_overlay_scale_factor(self) -> float:
        """Scale export overlays by zoom-in only; never enlarge when view is zoomed out/fit."""
        scale_factor = 1.0
        if self.zoom_level and self.zoom_level > 0:
            try:
                scale_factor = 1.0 / float(self.zoom_level)
            except Exception:
                scale_factor = 1.0
        # Hidden/offscreen widgets often auto-fit images (zoom < 1), which used to inflate
        # export labels/strokes dramatically. Clamp so exports do not get larger overlays
        # just because the widget view was zoomed out.
        return max(0.25, min(1.0, scale_factor))

    def _draw_copyright_text(
        self,
        painter: QPainter,
        base_rect: QRectF,
        scale_factor: float = 1.0,
        max_baseline_y: float | None = None,
    ) -> None:
        if not self.show_copyright or not self.copyright_text:
            return
        if base_rect.width() <= 0 or base_rect.height() <= 0:
            return

        font = painter.font()
        # Match publish watermark sizing (about 1.2% of rendered image height).
        target_font_px = max(1, int(round(float(base_rect.height()) * 0.012)))
        font.setPixelSize(target_font_px)
        font.setBold(False)
        painter.setFont(font)
        metrics = painter.fontMetrics()

        margin = max(4, int(round(target_font_px * 0.35)))
        x = base_rect.left() + margin
        min_baseline = base_rect.top() + metrics.ascent() + margin
        baseline = base_rect.bottom() - margin
        if max_baseline_y is not None:
            baseline = min(baseline, max_baseline_y)
        baseline = max(min_baseline, baseline)

        self._draw_text_with_outline(
            painter,
            x,
            baseline,
            self.copyright_text,
            fill_color=QColor(255, 255, 255, 225),
            outline_color=QColor(0, 0, 0),
            outline_opacity=0.4,
            outline_width_ratio=0.4,
        )

    def _render_export(self, painter, scale_factor):
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setRenderHint(QPainter.SmoothPixmapTransform)
        painter.drawPixmap(0, 0, self.original_pixmap)

        thin_width = max(1.0, 1.0 * scale_factor)
        wide_width = max(1.0, 3.0 * scale_factor)

        # Draw measurement rectangles
        if self.show_measure_overlays and self.measurement_rectangles:
            for rect in self.measurement_rectangles:
                self._draw_dual_stroke_polygon(
                    painter,
                    QPolygonF(rect),
                    color=self.measure_color,
                    thin_width=thin_width,
                    wide_width=wide_width,
                )

        # Draw measurement lines
        if self.show_measure_overlays and self.measurement_lines:
            for line in self.measurement_lines:
                p1 = QPointF(line[0], line[1])
                p2 = QPointF(line[2], line[3])
                self._draw_dual_stroke_line(
                    painter,
                    p1,
                    p2,
                    color=self.measure_color,
                    thin_width=thin_width,
                    wide_width=wide_width,
                )

                dx = p2.x() - p1.x()
                dy = p2.y() - p1.y()
                length = math.sqrt(dx**2 + dy**2)
                if length > 0:
                    perp_x = -dy / length
                    perp_y = dx / length
                    mark_len = 5 * scale_factor
                    self._draw_dual_stroke_line(
                        painter,
                        QPointF(p1.x() - perp_x * mark_len, p1.y() - perp_y * mark_len),
                        QPointF(p1.x() + perp_x * mark_len, p1.y() + perp_y * mark_len),
                        color=self.measure_color,
                        thin_width=thin_width,
                        wide_width=wide_width,
                    )
                    self._draw_dual_stroke_line(
                        painter,
                        QPointF(p2.x() - perp_x * mark_len, p2.y() - perp_y * mark_len),
                        QPointF(p2.x() + perp_x * mark_len, p2.y() + perp_y * mark_len),
                        color=self.measure_color,
                        thin_width=thin_width,
                        wide_width=wide_width,
                    )

        # Draw debug line layers (used by calibration auto overlays)
        if self.show_measure_overlays and self.debug_line_layers:
            for layer in self.debug_line_layers:
                painter.save()
                lines = layer.get("lines") if isinstance(layer, dict) else None
                if not lines:
                    painter.restore()
                    continue
                color = layer.get("color", QColor(52, 152, 219)) if isinstance(layer, dict) else QColor(52, 152, 219)
                width = layer.get("width", 2) if isinstance(layer, dict) else 2
                dashed = bool(layer.get("dashed", False)) if isinstance(layer, dict) else False
                show_endcaps = bool(layer.get("show_endcaps", False)) if isinstance(layer, dict) else False
                composition = layer.get("composition") if isinstance(layer, dict) else None

                if isinstance(color, tuple):
                    if len(color) == 4:
                        color = QColor(*color)
                    elif len(color) == 3:
                        color = QColor(*color)
                elif isinstance(color, str):
                    color = QColor(color)

                # Composition modes can be unsupported/odd in some export backends (especially SVG).
                # Fall back to normal compositing for exports while keeping color/alpha visible.
                if composition in {"overlay", "screen", "plus", "lighten"}:
                    try:
                        device = painter.device()
                    except Exception:
                        device = None
                    if not isinstance(device, QSvgGenerator):
                        if composition == "overlay":
                            painter.setCompositionMode(QPainter.CompositionMode_Overlay)
                        elif composition == "screen":
                            painter.setCompositionMode(QPainter.CompositionMode_Screen)
                        elif composition == "plus":
                            painter.setCompositionMode(QPainter.CompositionMode_Plus)
                        elif composition == "lighten":
                            painter.setCompositionMode(QPainter.CompositionMode_Lighten)

                # SVG exports should use the authored stroke width directly; scaling by
                # widget zoom can inflate strokes dramatically (for example ~16pt).
                stroke_width = float(width)
                try:
                    device = painter.device()
                except Exception:
                    device = None
                if not isinstance(device, QSvgGenerator):
                    stroke_width = max(1.0, stroke_width * scale_factor)
                pen = QPen(color, stroke_width)
                if dashed:
                    pen.setStyle(Qt.DashLine)
                painter.setPen(pen)
                for line in lines:
                    p1 = QPointF(line[0], line[1])
                    p2 = QPointF(line[2], line[3])
                    painter.drawLine(p1, p2)

                    if show_endcaps:
                        dx = p2.x() - p1.x()
                        dy = p2.y() - p1.y()
                        length = math.sqrt(dx**2 + dy**2)
                        if length > 0:
                            perp_x = -dy / length
                            perp_y = dx / length
                            mark_len = 5 * scale_factor
                            painter.drawLine(
                                QPointF(p1.x() - perp_x * mark_len, p1.y() - perp_y * mark_len),
                                QPointF(p1.x() + perp_x * mark_len, p1.y() + perp_y * mark_len)
                            )
                            painter.drawLine(
                                QPointF(p2.x() - perp_x * mark_len, p2.y() - perp_y * mark_len),
                                QPointF(p2.x() + perp_x * mark_len, p2.y() + perp_y * mark_len)
                            )
                painter.restore()

        # Draw measurement labels
        if self.show_measure_labels and self.measurement_labels:
            export_label_ui_scale = max(0.5, float(getattr(self, "export_measure_label_scale_multiplier", 1.0)))
            font = painter.font()
            font.setPointSize(max(8, int(round(9 * 1.3 * scale_factor * export_label_ui_scale))))
            font.setBold(False)
            painter.setFont(font)
            painter.setPen(self.measure_color)
            offset = 12 * scale_factor
            for label in self.measurement_labels:
                length_um = label.get("length_um")
                width_um = label.get("width_um")
                if label.get("kind") == "line":
                    line = label.get("line")
                    unit = label.get("unit") or "\u03bcm"
                    value = label.get("length_value")
                    if value is None:
                        value = length_um
                    if line and length_um is not None:
                        p1 = QPointF(line[0], line[1])
                        p2 = QPointF(line[2], line[3])
                        self._draw_rotated_label_on_line(
                            painter,
                            p1,
                            p2,
                            f"{value:.1f}",
                            padding_px=max(3.0, 3.0 * scale_factor * export_label_ui_scale),
                        )
                    continue
                line1 = label.get("line1")
                line2 = label.get("line2")
                center = label.get("center")
                unit = label.get("unit") or "\u03bcm"
                length_value = label.get("length_value")
                width_value = label.get("width_value")
                if length_value is None:
                    length_value = length_um
                if width_value is None:
                    width_value = width_um
                if (length_um is None or width_um is None or
                        line1 is None or line2 is None or center is None):
                    continue
                p1 = QPointF(line1[0], line1[1])
                p2 = QPointF(line1[2], line1[3])
                p3 = QPointF(line2[0], line2[1])
                p4 = QPointF(line2[2], line2[3])
                corners = self._compute_corners_from_lines((p1, p2), (p3, p4))
                if corners:
                    line1_vec = QPointF(p2.x() - p1.x(), p2.y() - p1.y())
                    line1_len = math.sqrt(line1_vec.x() ** 2 + line1_vec.y() ** 2)
                    if line1_len <= 0:
                        continue
                    line1_dir = QPointF(line1_vec.x() / line1_len, line1_vec.y() / line1_len)

                    edges = [
                        (corners[0], corners[1]),
                        (corners[1], corners[2]),
                        (corners[2], corners[3]),
                        (corners[3], corners[0]),
                    ]
                    best_index = 0
                    best_score = -1.0
                    for idx, edge in enumerate(edges):
                        evec = QPointF(edge[1].x() - edge[0].x(), edge[1].y() - edge[0].y())
                        elen = math.sqrt(evec.x() ** 2 + evec.y() ** 2)
                        if elen <= 0:
                            continue
                        edir = QPointF(evec.x() / elen, evec.y() / elen)
                        score = abs(edir.x() * line1_dir.x() + edir.y() * line1_dir.y())
                        if score > best_score:
                            best_score = score
                            best_index = idx

                    length_edge = edges[best_index]
                    width_edge = edges[(best_index + 1) % 4]
                    self._draw_rotated_label_outside(
                        painter, f"{length_value:.1f}", length_edge, center, max(3.0, 3.0 * export_label_ui_scale)
                    )
                    self._draw_rotated_label_outside(
                        painter, f"{width_value:.1f}", width_edge, center, max(3.0, 3.0 * export_label_ui_scale)
                    )

        # Draw scale bar
        if self.show_scale_bar and self.microns_per_pixel > 0:
            bar_um = self.scale_bar_um
            bar_pixels = bar_um / self.microns_per_pixel
            bar_pixels = max(10.0, bar_pixels)
            bar_pixels = min(bar_pixels, self.original_pixmap.width() * 0.6)

            # Published/exported images are often downscaled by the target website.
            # Compensate for hidden-widget fit zoom so scale-bar UI (font/line/padding)
            # remains readable after upload while keeping the bar length itself accurate.
            scale_bar_ui_scale = 1.0
            if self.zoom_level and self.zoom_level > 0:
                try:
                    scale_bar_ui_scale = max(1.0, min(6.0, 1.0 / float(self.zoom_level)))
                except Exception:
                    scale_bar_ui_scale = 1.0

            font = painter.font()
            # Slightly smaller label text than the previous tuning.
            font.setPixelSize(max(10, int(round(12 * 0.8 * scale_bar_ui_scale))))
            font.setBold(False)
            painter.setFont(font)
            label = f"{bar_um:g} \u03bcm"
            metrics = painter.fontMetrics()
            label_w = metrics.horizontalAdvance(label)
            label_h = metrics.height()

            margin = max(8.0, 8.0 * scale_bar_ui_scale)
            pad = max(6.0, 6.0 * scale_bar_ui_scale)
            box_w = max(bar_pixels, label_w) + pad * 2
            box_h = label_h + pad * 2 + max(6.0, 6.0 * scale_bar_ui_scale)
            box_x = self.original_pixmap.width() - box_w - margin
            box_y = self.original_pixmap.height() - box_h - margin

            painter.setPen(Qt.NoPen)
            painter.setBrush(QColor(255, 255, 255, self.scale_bar_bg_alpha))
            radius = max(4.0, 4.0 * min(2.0, scale_bar_ui_scale))
            painter.drawRoundedRect(QRectF(box_x, box_y, box_w, box_h), radius, radius)

            bar_x1 = box_x + (box_w - bar_pixels) / 2
            bar_x2 = bar_x1 + bar_pixels
            bar_y = box_y + pad + max(4.0, 4.0 * scale_bar_ui_scale)
            painter.setPen(QPen(QColor(0, 0, 0), max(2.0, 2.0 * scale_bar_ui_scale)))
            painter.drawLine(QPointF(bar_x1, bar_y), QPointF(bar_x2, bar_y))

            text_x = box_x + (box_w - label_w) / 2
            text_y = box_y + pad + max(4.0, 4.0 * scale_bar_ui_scale) + label_h
            painter.setPen(QColor(0, 0, 0))
            painter.drawText(int(text_x), int(text_y), label)

        self._draw_copyright_text(
            painter,
            QRectF(0, 0, self.original_pixmap.width(), self.original_pixmap.height()),
            scale_factor=scale_factor,
        )

    def paintEvent(self, event):
        """Custom paint event to draw image, overlays, and measurements."""
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setRenderHint(QPainter.SmoothPixmapTransform)

        # Fill background — use dark gray in dark mode
        from PySide6.QtWidgets import QApplication as _QApp
        _app = _QApp.instance()
        _dark = _app.palette().window().color().lightness() < 128 if _app else False
        painter.fillRect(self.rect(), QColor(44, 44, 46) if _dark else QColor(236, 240, 241))

        if not self.original_pixmap:
            # Draw placeholder text
            painter.setPen(QColor(180, 180, 182) if _dark else QColor(127, 140, 141))
            painter.drawText(self.rect(), Qt.AlignCenter, "Load an image to begin")
            painter.end()
            return

        # Get display rectangle
        display_rect = self.get_display_rect()

        # Draw the image
        painter.drawPixmap(display_rect, self.original_pixmap)

        # Draw overlay boxes (e.g., AI bounding boxes)
        if self.overlay_boxes:
            for item in self.overlay_boxes:
                box = item.get("box") if isinstance(item, dict) else None
                if not box:
                    continue
                color = item.get("color", QColor(39, 174, 96)) if isinstance(item, dict) else QColor(39, 174, 96)
                width = item.get("width", 2) if isinstance(item, dict) else 2
                dashed = item.get("dashed", False) if isinstance(item, dict) else False
                x1, y1, x2, y2 = box
                left = display_rect.x() + min(x1, x2) * self.zoom_level
                top = display_rect.y() + min(y1, y2) * self.zoom_level
                right = display_rect.x() + max(x1, x2) * self.zoom_level
                bottom = display_rect.y() + max(y1, y2) * self.zoom_level
                pen = QPen(QColor(color), width)
                if dashed:
                    pen.setStyle(Qt.DashLine)
                painter.setPen(pen)
                painter.setBrush(Qt.NoBrush)
                painter.drawRect(QRectF(left, top, right - left, bottom - top))

        # Draw crop box (user-defined)
        crop_box = None
        if self.crop_preview and self.crop_start:
            start, end = self.crop_preview
            crop_box = (start.x(), start.y(), end.x(), end.y())
        elif self.crop_box:
            crop_box = self.crop_box
        if crop_box:
            x1, y1, x2, y2 = crop_box
            left = display_rect.x() + min(x1, x2) * self.zoom_level
            top = display_rect.y() + min(y1, y2) * self.zoom_level
            right = display_rect.x() + max(x1, x2) * self.zoom_level
            bottom = display_rect.y() + max(y1, y2) * self.zoom_level
            crop_color = QColor(243, 156, 18)
            is_highlighted = bool(self.crop_hovered or self.crop_dragging)
            if is_highlighted:
                glow_color = QColor(192, 57, 43, 110)
                outline_color = QColor(211, 84, 0)
                painter.setPen(QPen(glow_color, 6))
                painter.setBrush(QColor(192, 57, 43, 20))
                painter.drawRect(QRectF(left, top, right - left, bottom - top))
                crop_pen = QPen(outline_color, 2)
                crop_pen.setStyle(Qt.SolidLine)
                painter.setPen(crop_pen)
                painter.setBrush(Qt.NoBrush)
            else:
                crop_pen = QPen(crop_color, 2)
                crop_pen.setStyle(Qt.DashLine)
                painter.setPen(crop_pen)
                painter.setBrush(Qt.NoBrush)
            painter.drawRect(QRectF(left, top, right - left, bottom - top))

            tag_text = "Crop"
            metrics = painter.fontMetrics()
            text_w = metrics.horizontalAdvance(tag_text)
            text_h = metrics.height()
            tag_padding = 4
            tag_w = text_w + tag_padding * 2
            tag_h = text_h + tag_padding
            tag_x = max(display_rect.x(), min(left, display_rect.x() + display_rect.width() - tag_w))
            tag_y = max(0, top - tag_h - 4)
            painter.setPen(Qt.NoPen)
            painter.setBrush(crop_color)
            painter.drawRect(QRectF(tag_x, tag_y, tag_w, tag_h))
            painter.setPen(QColor(255, 255, 255))
            text_x = tag_x + tag_padding
            text_y = tag_y + tag_padding + metrics.ascent()
            painter.drawText(int(text_x), int(text_y), tag_text)

            # Corner handles for resizing crop box
            handle_points = self._crop_corner_screen_points(crop_box)
            if handle_points:
                active_handle = self.crop_corner_drag_index if self.crop_corner_dragging else self.crop_corner_hover_index
                for idx, pt in enumerate(handle_points):
                    radius = 5.0 if idx == active_handle else 4.0
                    fill = QColor(255, 255, 255)
                    outline = QColor(211, 84, 0) if idx == active_handle else crop_color
                    painter.setPen(QPen(outline, 2))
                    painter.setBrush(fill)
                    painter.drawEllipse(QRectF(pt.x() - radius, pt.y() - radius, radius * 2, radius * 2))

        # Draw measurement rectangles
        if self.show_measure_overlays and self.measurement_rectangles:
            hover_color = QColor(231, 76, 60)
            for idx, rect in enumerate(self.measurement_rectangles):
                screen_points = []
                for corner in rect:
                    x = display_rect.x() + corner.x() * self.zoom_level
                    y = display_rect.y() + corner.y() * self.zoom_level
                    screen_points.append(QPointF(x, y))
                polygon = QPolygonF(screen_points)
                self._draw_dual_stroke_polygon(painter, polygon, color=self.measure_color, thin_width=1.0, wide_width=3.0)
                if idx == self.hover_rect_index or (not self.measurement_active and idx == self.selected_rect_index):
                    self._draw_dual_stroke_polygon(painter, polygon, color=hover_color, thin_width=2.0, wide_width=6.0)

        # Draw measurement lines with perpendicular end marks
        if self.show_measure_overlays and self.measurement_lines:
            hover_color = QColor(231, 76, 60)

            for idx, line in enumerate(self.measurement_lines):
                # Convert original image coordinates to screen coordinates
                p1_x = display_rect.x() + line[0] * self.zoom_level
                p1_y = display_rect.y() + line[1] * self.zoom_level
                p2_x = display_rect.x() + line[2] * self.zoom_level
                p2_y = display_rect.y() + line[3] * self.zoom_level

                # Draw main line (wide + thin)
                p1_screen = QPointF(p1_x, p1_y)
                p2_screen = QPointF(p2_x, p2_y)
                self._draw_dual_stroke_line(
                    painter, p1_screen, p2_screen, color=self.measure_color, thin_width=1.0, wide_width=3.0
                )

                if self.show_line_endcaps:
                    # Calculate perpendicular direction
                    dx = p2_x - p1_x
                    dy = p2_y - p1_y
                    length = math.sqrt(dx**2 + dy**2)
                    if length > 0:
                        # Normalized perpendicular vector
                        perp_x = -dy / length
                        perp_y = dx / length

                        # End mark length (5 pixels on each side)
                        mark_len = 5

                        # Draw perpendicular marks at both ends
                        self._draw_dual_stroke_line(
                            painter,
                            QPointF(p1_x - perp_x * mark_len, p1_y - perp_y * mark_len),
                            QPointF(p1_x + perp_x * mark_len, p1_y + perp_y * mark_len),
                            color=self.measure_color,
                            thin_width=1.0,
                            wide_width=3.0,
                        )
                        self._draw_dual_stroke_line(
                            painter,
                            QPointF(p2_x - perp_x * mark_len, p2_y - perp_y * mark_len),
                            QPointF(p2_x + perp_x * mark_len, p2_y + perp_y * mark_len),
                            color=self.measure_color,
                            thin_width=1.0,
                            wide_width=3.0,
                        )

                if idx == self.hover_line_index or (
                    not self.measurement_active and idx in self.selected_line_indices
                ):
                    self._draw_dual_stroke_line(
                        painter, p1_screen, p2_screen, color=hover_color, thin_width=2.0, wide_width=6.0
                    )

        # Draw debug line layers (no hover/selection)
        if self.show_measure_overlays and self.debug_line_layers:
            for layer in self.debug_line_layers:
                painter.save()
                lines = layer.get("lines") if isinstance(layer, dict) else None
                if not lines:
                    painter.restore()
                    continue
                color = layer.get("color", QColor(52, 152, 219)) if isinstance(layer, dict) else QColor(52, 152, 219)
                width = layer.get("width", 2) if isinstance(layer, dict) else 2
                dashed = bool(layer.get("dashed", False)) if isinstance(layer, dict) else False
                show_endcaps = bool(layer.get("show_endcaps", False)) if isinstance(layer, dict) else False
                composition = layer.get("composition") if isinstance(layer, dict) else None

                if isinstance(color, tuple):
                    if len(color) == 4:
                        color = QColor(*color)
                    elif len(color) == 3:
                        color = QColor(*color)
                elif isinstance(color, str):
                    color = QColor(color)

                if composition == "overlay":
                    painter.setCompositionMode(QPainter.CompositionMode_Overlay)
                elif composition == "screen":
                    painter.setCompositionMode(QPainter.CompositionMode_Screen)
                elif composition == "plus":
                    painter.setCompositionMode(QPainter.CompositionMode_Plus)
                elif composition == "lighten":
                    painter.setCompositionMode(QPainter.CompositionMode_Lighten)

                pen = QPen(color, width)
                if dashed:
                    pen.setStyle(Qt.DashLine)
                painter.setPen(pen)
                for line in lines:
                    p1_x = display_rect.x() + line[0] * self.zoom_level
                    p1_y = display_rect.y() + line[1] * self.zoom_level
                    p2_x = display_rect.x() + line[2] * self.zoom_level
                    p2_y = display_rect.y() + line[3] * self.zoom_level
                    painter.drawLine(int(p1_x), int(p1_y), int(p2_x), int(p2_y))

                    if show_endcaps:
                        dx = p2_x - p1_x
                        dy = p2_y - p1_y
                        length = math.sqrt(dx**2 + dy**2)
                        if length > 0:
                            perp_x = -dy / length
                            perp_y = dx / length
                            mark_len = 5
                            painter.drawLine(
                                int(p1_x - perp_x * mark_len), int(p1_y - perp_y * mark_len),
                                int(p1_x + perp_x * mark_len), int(p1_y + perp_y * mark_len)
                            )
                            painter.drawLine(
                                int(p2_x - perp_x * mark_len), int(p2_y - perp_y * mark_len),
                                int(p2_x + perp_x * mark_len), int(p2_y + perp_y * mark_len)
                            )
                painter.restore()
        # Draw preview line (from last point to mouse cursor)
        if self.preview_line is not None and self.current_mouse_pos is not None:
            # Convert coordinates to screen
            p1_x = display_rect.x() + self.preview_line.x() * self.zoom_level
            p1_y = display_rect.y() + self.preview_line.y() * self.zoom_level
            p2_x = display_rect.x() + self.current_mouse_pos.x() * self.zoom_level
            p2_y = display_rect.y() + self.current_mouse_pos.y() * self.zoom_level

            self._draw_dual_stroke_line(
                painter,
                QPointF(p1_x, p1_y),
                QPointF(p2_x, p2_y),
                color=self.measure_color,
                thin_width=1.0,
                wide_width=3.0,
                dashed=True,
            )

        # Draw preview rectangle (based on fixed base line and mouse width)
        if self.preview_rect is not None and self.current_mouse_pos is not None:
            base_start = self.preview_rect["base_start"]
            base_end = self.preview_rect["base_end"]
            width_dir = self.preview_rect["width_dir"]
            moving_line = self.preview_rect["moving_line"]

            base_mid = QPointF(
                (base_start.x() + base_end.x()) / 2,
                (base_start.y() + base_end.y()) / 2
            )
            delta = self.current_mouse_pos - base_mid
            width_distance = delta.x() * width_dir.x() + delta.y() * width_dir.y()
            offset = width_dir * width_distance

            if moving_line == "line2":
                line1_start = base_start
                line1_end = base_end
                line2_start = base_start + offset
                line2_end = base_end + offset
            else:
                line2_start = base_start
                line2_end = base_end
                line1_start = base_start + offset
                line1_end = base_end + offset

            corners = [line1_start, line1_end, line2_end, line2_start]
            screen_points = []
            for corner in corners:
                x = display_rect.x() + corner.x() * self.zoom_level
                y = display_rect.y() + corner.y() * self.zoom_level
                screen_points.append(QPointF(x, y))

            self._draw_dual_stroke_polygon(
                painter,
                QPolygonF(screen_points),
                color=self.measure_color,
                thin_width=1.0,
                wide_width=3.0,
                dashed=True,
            )

        # Draw measurement labels
        if self.show_measure_labels and self.measurement_labels:
            painter.setPen(self.measure_color)
            font = painter.font()
            font.setPointSize(int(round(9 * 1.3)))
            font.setBold(False)
            painter.setFont(font)

            for label in self.measurement_labels:
                length_um = label.get("length_um")
                width_um = label.get("width_um")
                if label.get("kind") == "line":
                    line = label.get("line")
                    value = label.get("length_value")
                    if value is None:
                        value = length_um
                    if line and length_um is not None:
                        p1 = QPointF(display_rect.x() + line[0] * self.zoom_level,
                                     display_rect.y() + line[1] * self.zoom_level)
                        p2 = QPointF(display_rect.x() + line[2] * self.zoom_level,
                                     display_rect.y() + line[3] * self.zoom_level)
                        self._draw_rotated_label_on_line(painter, p1, p2, f"{value:.1f}", padding_px=4.0)
                    continue
                line1 = label.get("line1")
                line2 = label.get("line2")
                center = label.get("center")
                length_value = label.get("length_value")
                width_value = label.get("width_value")
                if length_value is None:
                    length_value = length_um
                if width_value is None:
                    width_value = width_um
                if (length_um is None or width_um is None or
                        line1 is None or line2 is None or center is None):
                    continue

                p1 = QPointF(display_rect.x() + line1[0] * self.zoom_level,
                             display_rect.y() + line1[1] * self.zoom_level)
                p2 = QPointF(display_rect.x() + line1[2] * self.zoom_level,
                             display_rect.y() + line1[3] * self.zoom_level)
                p3 = QPointF(display_rect.x() + line2[0] * self.zoom_level,
                             display_rect.y() + line2[1] * self.zoom_level)
                p4 = QPointF(display_rect.x() + line2[2] * self.zoom_level,
                             display_rect.y() + line2[3] * self.zoom_level)
                center_screen = QPointF(display_rect.x() + center.x() * self.zoom_level,
                                        display_rect.y() + center.y() * self.zoom_level)
                corners = self._compute_corners_from_lines((p1, p2), (p3, p4))
                if corners:
                    line1_vec = QPointF(p2.x() - p1.x(), p2.y() - p1.y())
                    line1_len = math.sqrt(line1_vec.x() ** 2 + line1_vec.y() ** 2)
                    if line1_len <= 0:
                        continue
                    line1_dir = QPointF(line1_vec.x() / line1_len, line1_vec.y() / line1_len)

                    edges = [
                        (corners[0], corners[1]),
                        (corners[1], corners[2]),
                        (corners[2], corners[3]),
                        (corners[3], corners[0]),
                    ]
                    best_index = 0
                    best_score = -1.0
                    for idx, edge in enumerate(edges):
                        evec = QPointF(edge[1].x() - edge[0].x(), edge[1].y() - edge[0].y())
                        elen = math.sqrt(evec.x() ** 2 + evec.y() ** 2)
                        if elen <= 0:
                            continue
                        edir = QPointF(evec.x() / elen, evec.y() / elen)
                        score = abs(edir.x() * line1_dir.x() + edir.y() * line1_dir.y())
                        if score > best_score:
                            best_score = score
                            best_index = idx

                    length_edge = edges[best_index]
                    width_edge = edges[(best_index + 1) % 4]
                    self._draw_rotated_label_outside(
                        painter, f"{length_value:.1f}", length_edge, center_screen, 3
                    )
                    self._draw_rotated_label_outside(
                        painter, f"{width_value:.1f}", width_edge, center_screen, 3
                    )

        def _draw_tag(text, y_offset, bg_color, font_size=10):
            tag_padding = 10
            tag_margin = 10
            font = painter.font()
            font.setPointSize(font_size)
            font.setBold(True)
            painter.setFont(font)
            metrics = painter.fontMetrics()
            text_width = metrics.horizontalAdvance(text)
            text_height = metrics.height()
            tag_rect = QRect(
                self.width() - text_width - tag_padding * 2 - tag_margin,
                y_offset,
                text_width + tag_padding * 2,
                text_height + tag_padding
            )
            painter.setPen(Qt.NoPen)
            tag_color = QColor(bg_color)
            tag_color.setAlpha(200)
            painter.setBrush(tag_color)
            painter.drawRoundedRect(tag_rect, 6, 6)
            painter.setPen(Qt.white)
            painter.drawText(tag_rect, Qt.AlignCenter, text)
            return tag_rect.bottom() + 6

        def _draw_corner_tag(text, bg_color, font_size=10, bottom_limit=None):
            tag_padding = 10
            tag_margin = 10
            font = painter.font()
            font.setPointSize(font_size)
            font.setBold(True)
            painter.setFont(font)
            metrics = painter.fontMetrics()
            text_width = metrics.horizontalAdvance(text)
            text_height = metrics.height()
            tag_w = text_width + tag_padding * 2
            tag_h = text_height + tag_padding
            right_edge = self.width() - tag_margin
            if bottom_limit is None:
                bottom_limit = self.height() - tag_margin
            tag_rect = QRect(
                int(right_edge - tag_w),
                int(bottom_limit - tag_h),
                int(tag_w),
                int(tag_h),
            )
            painter.setPen(Qt.NoPen)
            tag_color = QColor(bg_color)
            tag_color.setAlpha(210)
            painter.setBrush(tag_color)
            painter.drawRoundedRect(tag_rect, 6, 6)
            painter.setPen(Qt.white)
            painter.drawText(tag_rect, Qt.AlignCenter, text)

        next_tag_y = 10
        if self.objective_text:
            next_tag_y = _draw_tag(self.objective_text, next_tag_y, self.objective_color, 11)

        # Draw zoom info in lower left corner
        zoom_text = f"Zoom: {self.zoom_level * 100:.0f}%"
        font = painter.font()
        font.setPointSize(9)
        painter.setFont(font)
        zoom_metrics = painter.fontMetrics()
        zoom_height = zoom_metrics.height()

        zoom_margin = 10
        zoom_pad_x = 6
        zoom_pad_y = 3
        zoom_width = zoom_metrics.horizontalAdvance(zoom_text)
        zoom_rect = QRect(
            zoom_margin,
            self.height() - zoom_height - zoom_pad_y * 2 - zoom_margin,
            zoom_width + zoom_pad_x * 2,
            zoom_height + zoom_pad_y * 2,
        )
        painter.setPen(Qt.NoPen)
        painter.setBrush(QColor(255, 255, 255, 180))
        painter.drawRoundedRect(zoom_rect, 4, 4)
        painter.setPen(QColor(0, 0, 0))
        painter.setFont(font)
        painter.drawText(zoom_rect, Qt.AlignCenter, zoom_text)

        if self.measurement_active:
            painter.setPen(QPen(QColor("#e74c3c"), 3))
            painter.setBrush(Qt.NoBrush)
            painter.drawRect(self.rect().adjusted(1, 1, -2, -2))

        # Draw scale bar in lower right corner
        scale_bar_box = None
        if self.show_scale_bar and self.microns_per_pixel > 0:
            bar_um = self.scale_bar_um
            bar_pixels = bar_um / self.microns_per_pixel
            bar_screen = bar_pixels * self.zoom_level
            bar_screen = max(10.0, bar_screen)
            bar_screen = min(bar_screen, display_rect.width() * 0.6)

            font = painter.font()
            font.setPointSize(9)
            font.setBold(False)
            painter.setFont(font)
            label = f"{bar_um:g} \u03bcm"
            metrics = painter.fontMetrics()
            label_w = metrics.horizontalAdvance(label)
            label_h = metrics.height()

            margin = 8
            pad = 6
            box_w = max(bar_screen, label_w) + pad * 2
            box_h = label_h + pad * 2 + 6
            box_x = display_rect.right() - box_w - margin
            box_y = display_rect.bottom() - box_h - margin
            scale_bar_box = QRectF(box_x, box_y, box_w, box_h)

            painter.setPen(Qt.NoPen)
            painter.setBrush(QColor(255, 255, 255, self.scale_bar_bg_alpha))
            painter.drawRoundedRect(QRectF(box_x, box_y, box_w, box_h), 4, 4)

            bar_x1 = box_x + (box_w - bar_screen) / 2
            bar_x2 = bar_x1 + bar_screen
            bar_y = box_y + pad + 4
            painter.setPen(QPen(QColor(0, 0, 0), 2))
            painter.drawLine(int(bar_x1), int(bar_y), int(bar_x2), int(bar_y))

            text_x = box_x + (box_w - label_w) / 2
            text_y = box_y + pad + 4 + label_h
            painter.setPen(QColor(0, 0, 0))
            painter.drawText(int(text_x), int(text_y), label)

        self._draw_copyright_text(
            painter,
            QRectF(display_rect),
            scale_factor=1.0,
        )

        if self._corner_tag_text:
            bottom_limit = self.height() - 10
            if scale_bar_box is not None:
                bottom_limit = min(bottom_limit, int(scale_bar_box.top()) - 10)
            _draw_corner_tag(self._corner_tag_text, self._corner_tag_color, 9, bottom_limit=bottom_limit)

        painter.end()
