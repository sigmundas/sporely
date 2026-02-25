"""Magnified spore preview widget with draggable sides for fine-tuning."""
from PySide6.QtWidgets import QWidget, QVBoxLayout, QLabel
from PySide6.QtGui import QPixmap, QPainter, QPen, QColor, QPolygonF, QBrush, QPainterPath
from PySide6.QtCore import Qt, QPointF, QRectF, Signal
import math


class SporePreviewWidget(QWidget):
    """Widget that shows a magnified spore with draggable dimension overlay."""

    # Signal emitted when dimensions are adjusted (measurement_id, new_length_um, new_width_um, new_points)
    dimensions_changed = Signal(int, float, float, list)
    delete_requested = Signal(int)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumSize(340, 400)
        self.setMaximumHeight(500)
        self.setFocusPolicy(Qt.StrongFocus)

        # Data
        self.original_pixmap = None
        self.points = []  # 4 points defining the spore
        self.length_um = 0
        self.width_um = 0
        self.microns_per_pixel = 0.5
        self.measurement_id = None
        self.measure_color = QColor("#0044aa")
        self.show_dimension_labels = True

        # Adjustment offsets (in pixels in original image space)
        self.corner_offsets = [QPointF(0, 0), QPointF(0, 0), QPointF(0, 0), QPointF(0, 0)]

        # Fixed crop size (calculated once when spore is set, to prevent rescaling during drag)
        self.fixed_crop_size = 0
        self.fixed_center = QPointF(0, 0)

        # State for side dragging (captured at drag start)
        self.drag_start_center = QPointF()
        self.drag_start_length_dir = QPointF()
        self.drag_start_width_dir = QPointF()
        self.drag_start_half_length = 0.0
        self.drag_start_half_width = 0.0
        self.drag_start_has_state = False
        self.drag_start_side_normals = {}
        self.drag_start_side_is_length = {}

        self.init_ui()

    def init_ui(self):
        """Initialize the UI components."""
        layout = QVBoxLayout(self)
        layout.setSpacing(5)
        layout.setContentsMargins(5, 5, 5, 5)

        # Image display label
        self.image_label = PreviewImageLabel()
        self.image_label.setFocusPolicy(Qt.NoFocus)
        self.image_label.side_drag_started.connect(self.on_side_drag_started)
        self.image_label.side_dragged.connect(self.on_side_dragged)
        self.image_label.rotation_dragged.connect(self.on_rotation_dragged)
        self.image_label.rectangle_dragged.connect(self.on_rectangle_dragged)
        self.image_label.interaction_finished.connect(self.apply_changes)
        self.image_label.set_measure_color(self.measure_color)
        layout.addWidget(self.image_label, 1)

        # No action buttons; edits auto-apply on release. Delete via keyboard.

    def set_spore(self, pixmap, points, length_um, width_um, microns_per_pixel, measurement_id=None):
        """Set the spore to display."""
        self.original_pixmap = pixmap
        self.points = points
        self.length_um = length_um
        self.width_um = width_um
        self.microns_per_pixel = microns_per_pixel
        self.measurement_id = measurement_id

        # Calculate fixed crop size based on original dimensions
        line1_vec = QPointF(points[1].x() - points[0].x(), points[1].y() - points[0].y())
        line2_vec = QPointF(points[3].x() - points[2].x(), points[3].y() - points[2].y())
        line1_len = math.sqrt(line1_vec.x()**2 + line1_vec.y()**2)
        line2_len = math.sqrt(line2_vec.x()**2 + line2_vec.y()**2)
        max_dim = max(line1_len, line2_len)
        padding = max_dim * 0.15
        self.fixed_crop_size = max_dim + padding * 2

        # Calculate fixed center
        line1_mid = QPointF((points[0].x() + points[1].x()) / 2, (points[0].y() + points[1].y()) / 2)
        line2_mid = QPointF((points[2].x() + points[3].x()) / 2, (points[2].y() + points[3].y()) / 2)
        self.fixed_center = QPointF((line1_mid.x() + line2_mid.x()) / 2, (line1_mid.y() + line2_mid.y()) / 2)

        self.reset_adjustments()
        self.setFocus(Qt.OtherFocusReason)

    def set_measure_color(self, color):
        """Set the measurement color for the preview."""
        self.measure_color = QColor(color)
        self.image_label.set_measure_color(self.measure_color)
        self.update_preview()

    def set_show_dimension_labels(self, show: bool):
        """Toggle display of length/width labels."""
        self.show_dimension_labels = bool(show)
        self.image_label.set_show_dimension_labels(self.show_dimension_labels)
        self.update_preview()

    def clear(self):
        """Clear the preview."""
        self.original_pixmap = None
        self.points = []
        self.length_um = 0
        self.width_um = 0
        self.measurement_id = None
        self.fixed_crop_size = 0
        self.fixed_center = QPointF(0, 0)
        self.reset_adjustments()

    def _on_delete_clicked(self):
        if self.measurement_id is None:
            return
        self.delete_requested.emit(self.measurement_id)

    def keyPressEvent(self, event):
        if event.key() == Qt.Key_Delete:
            self._on_delete_clicked()
            event.accept()
            return
        super().keyPressEvent(event)

    def reset_adjustments(self):
        """Reset all adjustments to zero."""
        self.corner_offsets = [QPointF(0, 0), QPointF(0, 0), QPointF(0, 0), QPointF(0, 0)]
        self.update_preview()

    def on_side_drag_started(self, side_index):
        """Capture the rectangle state at the start of side dragging."""
        if not self.points or len(self.points) != 4:
            return

        adjusted_points = []
        for i in range(4):
            adjusted_points.append(QPointF(
                self.points[i].x() + self.corner_offsets[i].x(),
                self.points[i].y() + self.corner_offsets[i].y()
            ))

        line1_vec = adjusted_points[1] - adjusted_points[0]
        line2_vec = adjusted_points[3] - adjusted_points[2]
        line1_len = math.sqrt(line1_vec.x()**2 + line1_vec.y()**2)
        line2_len = math.sqrt(line2_vec.x()**2 + line2_vec.y()**2)

        if line1_len < 0.001 or line2_len < 0.001:
            return

        # Keep a stable orientation based on the first measurement line
        self.drag_start_length_dir = QPointF(line1_vec.x() / line1_len, line1_vec.y() / line1_len)
        self.drag_start_width_dir = QPointF(-self.drag_start_length_dir.y(), self.drag_start_length_dir.x())

        self.drag_start_half_length = line1_len / 2
        self.drag_start_half_width = line2_len / 2

        line1_mid = QPointF((adjusted_points[0].x() + adjusted_points[1].x()) / 2,
                            (adjusted_points[0].y() + adjusted_points[1].y()) / 2)
        line2_mid = QPointF((adjusted_points[2].x() + adjusted_points[3].x()) / 2,
                            (adjusted_points[2].y() + adjusted_points[3].y()) / 2)
        self.drag_start_center = QPointF((line1_mid.x() + line2_mid.x()) / 2,
                                         (line1_mid.y() + line2_mid.y()) / 2)

        axis_width = self.drag_start_width_dir
        axis_length = self.drag_start_length_dir  # Same direction as paintEvent uses
        hw = self.drag_start_half_width
        hl = self.drag_start_half_length
        # Build corners matching paintEvent() order:
        # Corner 0: center - width_dir * half_width - length_dir * half_length
        # Corner 1: center + width_dir * half_width - length_dir * half_length
        # Corner 2: center + width_dir * half_width + length_dir * half_length
        # Corner 3: center - width_dir * half_width + length_dir * half_length
        corners = [
            self.drag_start_center - axis_width * hw - axis_length * hl,
            self.drag_start_center + axis_width * hw - axis_length * hl,
            self.drag_start_center + axis_width * hw + axis_length * hl,
            self.drag_start_center - axis_width * hw + axis_length * hl,
        ]

        self.drag_start_side_normals = {}
        self.drag_start_side_is_length = {}
        for i in range(4):
            a = corners[i]
            b = corners[(i + 1) % 4]
            mid = QPointF((a.x() + b.x()) / 2, (a.y() + b.y()) / 2)
            normal = mid - self.drag_start_center
            normal_len = math.sqrt(normal.x()**2 + normal.y()**2)
            if normal_len > 0.001:
                normal = QPointF(normal.x() / normal_len, normal.y() / normal_len)
                self.drag_start_side_normals[i] = normal
                dot_len = abs(normal.x() * self.drag_start_length_dir.x() +
                              normal.y() * self.drag_start_length_dir.y())
                dot_wid = abs(normal.x() * self.drag_start_width_dir.x() +
                              normal.y() * self.drag_start_width_dir.y())
                self.drag_start_side_is_length[i] = dot_len >= dot_wid

        self.drag_start_has_state = True

    def on_side_dragged(self, side_index, delta):
        """Handle side dragging: move only perpendicular to the side."""
        if not self.drag_start_has_state:
            return

        normal = self.drag_start_side_normals.get(side_index)
        if normal is None:
            return

        is_length = self.drag_start_side_is_length.get(side_index, False)
        half_value = self.drag_start_half_length if is_length else self.drag_start_half_width

        delta_n = delta.x() * normal.x() + delta.y() * normal.y()
        min_half = 2.0
        new_half = half_value + (delta_n / 2)
        if new_half < min_half:
            delta_n = 2 * (min_half - half_value)
            new_half = min_half

        self.drag_start_center = self.drag_start_center + normal * (delta_n / 2)
        if is_length:
            self.drag_start_half_length = new_half
        else:
            self.drag_start_half_width = new_half

        center = self.drag_start_center
        half_length = self.drag_start_half_length
        half_width = self.drag_start_half_width

        length_dir = self.drag_start_length_dir
        width_dir = self.drag_start_width_dir
        line1_start = center - length_dir * half_length
        line1_end = center + length_dir * half_length
        line2_start = center - width_dir * half_width
        line2_end = center + width_dir * half_width

        new_points = [line1_start, line1_end, line2_start, line2_end]
        for i in range(4):
            self.corner_offsets[i] = QPointF(
                new_points[i].x() - self.points[i].x(),
                new_points[i].y() - self.points[i].y()
            )

        self.update_preview()

    def on_rotation_dragged(self, angle_delta):
        """Handle rotation arrow dragging - rotate all corner offsets around center."""
        if not self.points or len(self.points) != 4:
            return

        # Calculate the center of the original measurement
        line1_mid = QPointF((self.points[0].x() + self.points[1].x()) / 2,
                           (self.points[0].y() + self.points[1].y()) / 2)
        line2_mid = QPointF((self.points[2].x() + self.points[3].x()) / 2,
                           (self.points[2].y() + self.points[3].y()) / 2)
        center = QPointF((line1_mid.x() + line2_mid.x()) / 2,
                        (line1_mid.y() + line2_mid.y()) / 2)

        # Rotate each corner offset around the center
        for i in range(4):
            # Get the current adjusted point
            adjusted_point = QPointF(
                self.points[i].x() + self.corner_offsets[i].x(),
                self.points[i].y() + self.corner_offsets[i].y()
            )

            # Vector from center to adjusted point
            vec_x = adjusted_point.x() - center.x()
            vec_y = adjusted_point.y() - center.y()

            # Rotate the vector
            cos_a = math.cos(angle_delta)
            sin_a = math.sin(angle_delta)
            new_vec_x = vec_x * cos_a - vec_y * sin_a
            new_vec_y = vec_x * sin_a + vec_y * cos_a

            # Calculate new adjusted point
            new_adjusted_x = center.x() + new_vec_x
            new_adjusted_y = center.y() + new_vec_y

            # Update the offset
            self.corner_offsets[i] = QPointF(
                new_adjusted_x - self.points[i].x(),
                new_adjusted_y - self.points[i].y()
            )

        self.update_preview()

    def on_rectangle_dragged(self, delta):
        """Handle rectangle dragging - move all corners by the same delta."""
        # Move all corners by the same amount (no scaling)
        for i in range(4):
            self.corner_offsets[i] += delta

        self.update_preview()

    def apply_changes(self):
        """Apply the adjustments and emit signal to update database."""
        if not self.points or not self.measurement_id:
            return

        # Calculate adjusted points
        adjusted_points = []
        for i, point in enumerate(self.points):
            adjusted_points.append(QPointF(
                point.x() + self.corner_offsets[i].x(),
                point.y() + self.corner_offsets[i].y()
            ))

        # Calculate new line vectors
        line1_vec = QPointF(adjusted_points[1].x() - adjusted_points[0].x(),
                           adjusted_points[1].y() - adjusted_points[0].y())
        line2_vec = QPointF(adjusted_points[3].x() - adjusted_points[2].x(),
                           adjusted_points[3].y() - adjusted_points[2].y())

        line1_len = math.sqrt(line1_vec.x()**2 + line1_vec.y()**2)
        line2_len = math.sqrt(line2_vec.x()**2 + line2_vec.y()**2)

        # Calculate new measurements in microns
        new_length_um = max(line1_len, line2_len) * self.microns_per_pixel
        new_width_um = min(line1_len, line2_len) * self.microns_per_pixel

        # Emit signal with new values
        self.dimensions_changed.emit(self.measurement_id, new_length_um, new_width_um, adjusted_points)

    def update_preview(self):
        """Update the image label with current adjustments."""
        if not self.original_pixmap or len(self.points) != 4:
            self.image_label.clear_preview()
            return

        # Pass data to image label for rendering
        self.image_label.set_preview(
            self.original_pixmap,
            self.points,
            self.corner_offsets,
            self.microns_per_pixel,
            self.fixed_crop_size,
            self.fixed_center
        )
        # Force immediate repaint
        self.image_label.update()


class PreviewImageLabel(QLabel):
    """Label that displays the magnified spore image with draggable sides."""

    # Signal emitted when a side is dragged (side_index, delta)
    side_dragged = Signal(int, QPointF)
    # Signal emitted when side dragging starts (side_index)
    side_drag_started = Signal(int)
    # Signal emitted when rotation arrows are dragged (angle_delta_radians)
    rotation_dragged = Signal(float)
    # Signal emitted when rectangle is dragged (delta)
    rectangle_dragged = Signal(QPointF)
    # Signal emitted when a drag interaction ends
    interaction_finished = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAlignment(Qt.AlignCenter)
        self.setMinimumSize(340, 340)
        self.setMouseTracking(True)

        # Data
        self.original_pixmap = None
        self.points = []
        self.corner_offsets = [QPointF(0, 0), QPointF(0, 0), QPointF(0, 0), QPointF(0, 0)]
        self.microns_per_pixel = 0.5
        self.fixed_crop_size = 0
        self.fixed_center = QPointF(0, 0)
        self.show_dimension_labels = True

        # Interaction state
        self.dragging_side = -1
        self.dragging_rotation = False
        self.dragging_rectangle = False  # New: dragging entire rectangle
        self.last_mouse_pos = QPointF()
        self.hover_side = -1
        self.hover_rotation_arrow = -1  # -1 = none, 0 = left, 1 = right

        # Display coordinates (for hit testing)
        self.screen_corners = []
        self.rotation_arrow_positions = []  # Positions of rotation arrow handles
        self.preview_scale = 1.0
        self.measure_color = QColor("#0044aa")

    def set_show_dimension_labels(self, show: bool):
        self.show_dimension_labels = bool(show)
        self.update()

    def _measure_stroke_style(self, color=None):
        base = QColor(color) if color is not None else QColor(self.measure_color)
        if not base.isValid():
            base = QColor("#0044aa")
        presets = (
            {"match": QColor("#1E90FF"), "thin": QColor("#0044aa"), "glow": QColor("#2a7fff"), "opacity": 0.576531, "blend": "screen"},
            {"match": QColor("#3498db"), "thin": QColor("#0044aa"), "glow": QColor("#2a7fff"), "opacity": 0.576531, "blend": "screen"},
            {"match": QColor("#FF3B30"), "thin": QColor("#d40000"), "glow": QColor("#d40000"), "opacity": 0.658163, "blend": "screen"},
            {"match": QColor("#2ECC71"), "thin": QColor("#00aa00"), "glow": QColor("#00aa00"), "opacity": 0.658163, "blend": "screen"},
            {"match": QColor("#E056FD"), "thin": QColor("#ff00ff"), "glow": QColor("#ff00ff"), "opacity": 0.433674, "blend": "screen"},
            {"match": QColor("#ECAF11"), "thin": QColor("#ffd42a"), "glow": QColor("#ffdd55"), "opacity": 0.658163, "blend": "overlay"},
            {"match": QColor("#1CEBEB"), "thin": QColor("#00ffff"), "glow": QColor("#00ffff"), "opacity": 0.658163, "blend": "overlay"},
            {"match": QColor("#000000"), "thin": QColor("#000000"), "glow": QColor("#000000"), "opacity": 0.658163, "blend": "overlay"},
        )
        def _dist2(c1, c2):
            dr = c1.red() - c2.red()
            dg = c1.green() - c2.green()
            db = c1.blue() - c2.blue()
            return dr * dr + dg * dg + db * db
        chosen = min(presets, key=lambda p: _dist2(base, p["match"]))
        thin = QColor(chosen["thin"])
        thin.setAlpha(max(1, base.alpha()))
        glow = QColor(chosen["glow"])
        glow.setAlpha(max(1, min(255, int(round(255 * float(chosen["opacity"]))))))  # type: ignore[arg-type]
        return {"thin": thin, "glow": glow, "blend": str(chosen["blend"])}

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

    def _draw_dual_stroke_polygon(self, painter: QPainter, polygon: QPolygonF, color=None, thin_width=1.0, wide_width=None):
        style = self._measure_stroke_style(color=color)
        thin_pen = QPen(QColor(style["thin"]), max(1.0, float(thin_width)))
        wide_pen = QPen(QColor(style["glow"]), max(max(1.0, float(thin_width) * 3.0), float(wide_width or 0.0)))
        painter.save()
        self._set_named_composition_mode(painter, str(style.get("blend") or ""))
        painter.setPen(wide_pen)
        painter.setBrush(Qt.NoBrush)
        painter.drawPolygon(polygon)
        painter.restore()
        painter.setBrush(Qt.NoBrush)
        painter.setPen(thin_pen)
        painter.drawPolygon(polygon)

    def _draw_dual_stroke_line(self, painter: QPainter, a: QPointF, b: QPointF, color=None, thin_width=1.0, wide_width=None):
        style = self._measure_stroke_style(color=color)
        thin_pen = QPen(QColor(style["thin"]), max(1.0, float(thin_width)))
        wide_pen = QPen(QColor(style["glow"]), max(max(1.0, float(thin_width) * 3.0), float(wide_width or 0.0)))
        painter.save()
        self._set_named_composition_mode(painter, str(style.get("blend") or ""))
        painter.setPen(wide_pen)
        painter.drawLine(a, b)
        painter.restore()
        painter.setPen(thin_pen)
        painter.drawLine(a, b)

    def _draw_halo_text(self, painter: QPainter, x: int, y: int, text: str, color=None) -> None:
        style = self._measure_stroke_style(color=color)
        text_color = QColor(style["thin"])
        path = QPainterPath()
        path.addText(float(x), float(y), painter.font(), text)
        stroke_w = max(1.0, float(painter.fontMetrics().height()) * 0.4)
        pen = QPen(QColor(255, 255, 255, 102), stroke_w)
        pen.setJoinStyle(Qt.RoundJoin)
        pen.setCapStyle(Qt.RoundCap)
        painter.save()
        painter.setBrush(Qt.NoBrush)
        painter.setPen(pen)
        painter.drawPath(path)
        painter.restore()
        painter.setPen(text_color)
        painter.drawText(x, y, text)

    def set_measure_color(self, color):
        """Set the measurement color."""
        self.measure_color = QColor(color)
        self.update()

    def set_preview(self, pixmap, points, corner_offsets, mpp, fixed_crop_size, fixed_center):
        """Set preview data."""
        self.original_pixmap = pixmap
        self.points = points
        self.corner_offsets = corner_offsets
        self.microns_per_pixel = mpp
        self.fixed_crop_size = fixed_crop_size
        self.fixed_center = fixed_center
        self.update()

    def clear_preview(self):
        """Clear the preview."""
        self.original_pixmap = None
        self.points = []
        self.screen_corners = []
        self.fixed_crop_size = 0
        self.fixed_center = QPointF(0, 0)
        self.update()

    def is_point_inside_polygon(self, point, polygon_points):
        """Check if a point is inside a polygon using ray casting algorithm."""
        if len(polygon_points) < 3:
            return False

        inside = False
        n = len(polygon_points)
        p1x, p1y = polygon_points[0].x(), polygon_points[0].y()

        for i in range(1, n + 1):
            p2x, p2y = polygon_points[i % n].x(), polygon_points[i % n].y()
            if point.y() > min(p1y, p2y):
                if point.y() <= max(p1y, p2y):
                    if point.x() <= max(p1x, p2x):
                        if p1y != p2y:
                            xinters = (point.y() - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                        if p1x == p2x or point.x() <= xinters:
                            inside = not inside
            p1x, p1y = p2x, p2y

        return inside

    def point_to_segment_distance(self, point, a, b):
        """Calculate the distance from a point to a line segment."""
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

    def mousePressEvent(self, event):
        """Handle mouse press for side, rotation arrow, and rectangle dragging."""
        if event.button() == Qt.LeftButton:
            click_pos = event.position()

            # Check if clicking on a rotation arrow first (highest priority)
            for i, arrow_pos in enumerate(self.rotation_arrow_positions):
                if (arrow_pos - click_pos).manhattanLength() < 20:
                    self.dragging_rotation = True
                    self.last_mouse_pos = click_pos
                    self.setCursor(Qt.ClosedHandCursor)
                    return

            # Check if clicking on a side (second priority)
            if len(self.screen_corners) == 4:
                min_dist = 9999
                side_idx = -1
                for i in range(4):
                    a = self.screen_corners[i]
                    b = self.screen_corners[(i + 1) % 4]
                    dist = self.point_to_segment_distance(click_pos, a, b)
                    if dist < min_dist:
                        min_dist = dist
                        side_idx = i
                if min_dist < 10 and side_idx >= 0:
                    self.dragging_side = side_idx
                    self.last_mouse_pos = click_pos
                    self.setCursor(Qt.ClosedHandCursor)

                    # Notify parent to capture initial state for side dragging
                    self.side_drag_started.emit(side_idx)
                    return

            # Check if clicking inside the rectangle (lowest priority)
            if len(self.screen_corners) == 4 and self.is_point_inside_polygon(click_pos, self.screen_corners):
                self.dragging_rectangle = True
                self.last_mouse_pos = click_pos
                self.setCursor(Qt.ClosedHandCursor)

    def mouseMoveEvent(self, event):
        """Handle mouse move for corner/rotation/rectangle dragging and hover."""
        mouse_pos = event.position()

        if self.dragging_rotation:
            # Dragging rotation - calculate angle change
            screen_center = QPointF(self.width() / 2, self.height() / 2)

            # Calculate angles from center to old and new mouse positions
            old_angle = math.atan2(
                self.last_mouse_pos.y() - screen_center.y(),
                self.last_mouse_pos.x() - screen_center.x()
            )
            new_angle = math.atan2(
                mouse_pos.y() - screen_center.y(),
                mouse_pos.x() - screen_center.x()
            )

            angle_delta = new_angle - old_angle
            self.rotation_dragged.emit(angle_delta)
            self.last_mouse_pos = mouse_pos
        elif self.dragging_side >= 0:
            # Dragging a side
            delta = mouse_pos - self.last_mouse_pos
            if self.preview_scale > 0:
                delta = QPointF(delta.x() / self.preview_scale, delta.y() / self.preview_scale)
            self.side_dragged.emit(self.dragging_side, delta)
            self.last_mouse_pos = mouse_pos
        elif self.dragging_rectangle:
            # Dragging entire rectangle
            delta = mouse_pos - self.last_mouse_pos
            if self.preview_scale > 0:
                delta = QPointF(delta.x() / self.preview_scale, delta.y() / self.preview_scale)
            self.rectangle_dragged.emit(delta)
            self.last_mouse_pos = mouse_pos
        else:
            # Check for hover on rotation arrows
            self.hover_rotation_arrow = -1
            for i, arrow_pos in enumerate(self.rotation_arrow_positions):
                if (arrow_pos - mouse_pos).manhattanLength() < 20:
                    self.hover_rotation_arrow = i
                    self.setCursor(Qt.OpenHandCursor)
                    self.update()
                    return

            # Check for hover on sides
            self.hover_side = -1
            if len(self.screen_corners) == 4:
                min_dist = 9999
                side_idx = -1
                for i in range(4):
                    a = self.screen_corners[i]
                    b = self.screen_corners[(i + 1) % 4]
                    dist = self.point_to_segment_distance(mouse_pos, a, b)
                    if dist < min_dist:
                        min_dist = dist
                        side_idx = i
                if min_dist < 10 and side_idx >= 0:
                    self.hover_side = side_idx
                    self.setCursor(Qt.OpenHandCursor)
                    self.update()
                    return

            # Check if hovering inside rectangle
            if len(self.screen_corners) == 4 and self.is_point_inside_polygon(mouse_pos, self.screen_corners):
                self.setCursor(Qt.SizeAllCursor)
                self.update()
                return

            self.setCursor(Qt.ArrowCursor)
            if self.hover_side == -1 and self.hover_rotation_arrow == -1:
                self.update()

    def mouseReleaseEvent(self, event):
        """Handle mouse release."""
        if event.button() == Qt.LeftButton:
            was_dragging = self.dragging_side >= 0 or self.dragging_rotation or self.dragging_rectangle
            self.dragging_side = -1
            self.dragging_rotation = False
            self.dragging_rectangle = False
            self.setCursor(Qt.ArrowCursor)
            if was_dragging:
                self.interaction_finished.emit()

    def paintEvent(self, event):
        """Custom paint to show magnified spore with draggable corner nodes."""
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setRenderHint(QPainter.SmoothPixmapTransform)

        # Fill background
        painter.fillRect(self.rect(), QColor(236, 240, 241))

        if not self.original_pixmap or len(self.points) != 4:
            # Draw placeholder text
            painter.setPen(QColor(127, 140, 141))
            painter.drawText(self.rect(), Qt.AlignCenter, "Click a measurement\nto preview")
            painter.end()
            return

        # Get adjusted points
        adjusted_points = []
        for i, point in enumerate(self.points):
            adjusted_points.append(QPointF(
                point.x() + self.corner_offsets[i].x(),
                point.y() + self.corner_offsets[i].y()
            ))

        # Calculate line lengths
        line1_vec = QPointF(adjusted_points[1].x() - adjusted_points[0].x(),
                           adjusted_points[1].y() - adjusted_points[0].y())
        line2_vec = QPointF(adjusted_points[3].x() - adjusted_points[2].x(),
                           adjusted_points[3].y() - adjusted_points[2].y())
        line1_len = math.sqrt(line1_vec.x()**2 + line1_vec.y()**2)
        line2_len = math.sqrt(line2_vec.x()**2 + line2_vec.y()**2)

        # Keep a stable orientation based on the first measurement line
        length_vec = line1_vec
        length_px = line1_len
        width_px = line2_len

        # Calculate current center from adjusted points (so rectangle dragging works)
        line1_mid = QPointF((adjusted_points[0].x() + adjusted_points[1].x()) / 2,
                           (adjusted_points[0].y() + adjusted_points[1].y()) / 2)
        line2_mid = QPointF((adjusted_points[2].x() + adjusted_points[3].x()) / 2,
                           (adjusted_points[2].y() + adjusted_points[3].y()) / 2)
        current_center = QPointF((line1_mid.x() + line2_mid.x()) / 2,
                                (line1_mid.y() + line2_mid.y()) / 2)

        # Use fixed crop size but current center (allows rectangle dragging to move view)
        desired_crop_rect = QRectF(
            current_center.x() - self.fixed_crop_size / 2,
            current_center.y() - self.fixed_crop_size / 2,
            self.fixed_crop_size,
            self.fixed_crop_size
        )

        # Ensure crop rect is within image bounds
        crop_rect = desired_crop_rect.intersected(
            QRectF(0, 0, self.original_pixmap.width(), self.original_pixmap.height())
        )

        # Crop the pixmap (NO ROTATION)
        cropped = self.original_pixmap.copy(crop_rect.toRect())

        # Calculate scaling to fit widget
        scale = min(
            (self.width() - 20) / cropped.width(),
            (self.height() - 20) / cropped.height()
        )
        self.preview_scale = scale if scale > 0 else 1.0

        scaled_width = int(cropped.width() * scale)
        scaled_height = int(cropped.height() * scale)
        scaled_pixmap = cropped.scaled(
            scaled_width, scaled_height,
            Qt.KeepAspectRatio,
            Qt.SmoothTransformation
        )

        # Draw the scaled pixmap centered in the widget
        img_x = (self.width() - scaled_pixmap.width()) / 2
        img_y = (self.height() - scaled_pixmap.height()) / 2
        painter.drawPixmap(int(img_x), int(img_y), scaled_pixmap)

        # Calculate where the measurement center appears on screen
        # The measurement center in image coordinates is current_center
        # The crop_rect.topLeft() is the origin of our cropped image
        # So the measurement center relative to the cropped image is:
        center_in_crop_x = current_center.x() - crop_rect.x()
        center_in_crop_y = current_center.y() - crop_rect.y()

        # Then scale and offset to get screen coordinates
        screen_center_x = img_x + center_in_crop_x * scale
        screen_center_y = img_y + center_in_crop_y * scale

        # Calculate angle of length vector (line1 direction)
        angle_rad = math.atan2(length_vec.y(), length_vec.x())

        # Half dimensions in screen space
        half_length = (length_px * scale) / 2
        half_width = (width_px * scale) / 2

        # Build corners the same way as main_window.py:
        # length_dir is along line1, width_dir is perpendicular (90° counterclockwise)
        cos_a = math.cos(angle_rad)
        sin_a = math.sin(angle_rad)
        # length_dir = (cos_a, sin_a)
        # width_dir = (-sin_a, cos_a) which is perpendicular (90° counterclockwise)

        # Corners in screen space, matching main_window.py's build_measurement_rectangles():
        # corner = center ± width_dir * half_width ± length_dir * half_length
        self.screen_corners = [
            # center - width_dir * half_width - length_dir * half_length
            QPointF(screen_center_x - (-sin_a) * half_width - cos_a * half_length,
                    screen_center_y - cos_a * half_width - sin_a * half_length),
            # center + width_dir * half_width - length_dir * half_length
            QPointF(screen_center_x + (-sin_a) * half_width - cos_a * half_length,
                    screen_center_y + cos_a * half_width - sin_a * half_length),
            # center + width_dir * half_width + length_dir * half_length
            QPointF(screen_center_x + (-sin_a) * half_width + cos_a * half_length,
                    screen_center_y + cos_a * half_width + sin_a * half_length),
            # center - width_dir * half_width + length_dir * half_length
            QPointF(screen_center_x - (-sin_a) * half_width + cos_a * half_length,
                    screen_center_y - cos_a * half_width + sin_a * half_length),
        ]

        # Draw the rotated rectangle (SVG-style glow + thin stroke)
        polygon = QPolygonF(self.screen_corners)
        self._draw_dual_stroke_polygon(painter, polygon, color=self.measure_color, thin_width=1.0, wide_width=3.0)

        # Draw side highlight if hovered
        highlight_side = self.dragging_side if self.dragging_side >= 0 else self.hover_side
        if len(self.screen_corners) == 4 and highlight_side >= 0:
            a = self.screen_corners[highlight_side]
            b = self.screen_corners[(highlight_side + 1) % 4]
            self._draw_dual_stroke_line(painter, a, b, color=QColor(231, 76, 60), thin_width=1.0, wide_width=3.0)

        # Draw rotation arrows on the left and right sides
        self.rotation_arrow_positions = []
        arrow_distance = half_width + 30  # Distance from center

        # Left and right arrow positions (perpendicular to length axis)
        for side_idx, side_sign in enumerate([-1, 1]):  # left, right
            # Calculate position perpendicular to length axis
            # angle_rad is the length direction, so perpendicular is angle_rad + pi/2
            perp_angle = angle_rad + math.pi / 2
            arrow_x = screen_center_x + side_sign * arrow_distance * math.cos(perp_angle)
            arrow_y = screen_center_y + side_sign * arrow_distance * math.sin(perp_angle)
            arrow_pos = QPointF(arrow_x, arrow_y)
            self.rotation_arrow_positions.append(arrow_pos)

            # Draw curved arrow
            is_hovered = (side_idx == self.hover_rotation_arrow) or self.dragging_rotation
            arrow_color = QColor(231, 76, 60) if is_hovered else self.measure_color
            painter.setPen(QPen(arrow_color, 2))

            # Draw arc
            arc_radius = 12
            arc_rect = QRectF(arrow_x - arc_radius, arrow_y - arc_radius, arc_radius * 2, arc_radius * 2)

            # Arc angle depends on which side (clockwise or counterclockwise)
            start_angle = 45 if side_sign > 0 else 135
            span_angle = 270
            painter.drawArc(arc_rect, start_angle * 16, span_angle * 16)

            # Draw arrowhead
            arrow_tip_angle = math.radians(start_angle + span_angle) if side_sign > 0 else math.radians(start_angle)
            arrow_tip_x = arrow_x + arc_radius * math.cos(arrow_tip_angle)
            arrow_tip_y = arrow_y + arc_radius * math.sin(arrow_tip_angle)

            # Arrowhead points
            arrow_size = 6
            arrow_angle = arrow_tip_angle + (math.pi / 2 if side_sign > 0 else -math.pi / 2)
            p1 = QPointF(arrow_tip_x, arrow_tip_y)
            p2 = QPointF(
                arrow_tip_x - arrow_size * math.cos(arrow_angle - 0.4),
                arrow_tip_y - arrow_size * math.sin(arrow_angle - 0.4)
            )
            p3 = QPointF(
                arrow_tip_x - arrow_size * math.cos(arrow_angle + 0.4),
                arrow_tip_y - arrow_size * math.sin(arrow_angle + 0.4)
            )

            painter.setBrush(QBrush(arrow_color))
            painter.drawPolygon(QPolygonF([p1, p2, p3]))

        if self.show_dimension_labels:
            # Calculate current dimensions in microns
            current_length_um = length_px * self.microns_per_pixel
            current_width_um = width_px * self.microns_per_pixel

            # Draw dimension labels CLOSER to rectangle
            painter.setPen(self.measure_color)
            font = painter.font()
            font.setPointSize(10)
            font.setBold(True)
            painter.setFont(font)

            # Calculate midpoints of the sides for label placement
            left_mid = QPointF(
                (self.screen_corners[0].x() + self.screen_corners[3].x()) / 2,
                (self.screen_corners[0].y() + self.screen_corners[3].y()) / 2
            )
            top_mid = QPointF(
                (self.screen_corners[0].x() + self.screen_corners[1].x()) / 2,
                (self.screen_corners[0].y() + self.screen_corners[1].y()) / 2
            )

            # Calculate perpendicular offset for labels (just outside the rectangle)
            offset_distance = 15

            # Left side label (length) - perpendicular to left edge
            left_edge_vec = QPointF(
                self.screen_corners[3].x() - self.screen_corners[0].x(),
                self.screen_corners[3].y() - self.screen_corners[0].y()
            )
            left_edge_len = math.sqrt(left_edge_vec.x()**2 + left_edge_vec.y()**2)
            if left_edge_len > 0:
                # Perpendicular vector pointing left
                perp_x = -left_edge_vec.y() / left_edge_len
                perp_y = left_edge_vec.x() / left_edge_len
                label_pos = QPointF(
                    left_mid.x() + perp_x * offset_distance,
                    left_mid.y() + perp_y * offset_distance
                )
                self._draw_halo_text(
                    painter,
                    int(label_pos.x() - 25),
                    int(label_pos.y() + 5),
                    f"{current_length_um:.2f}",
                    color=self.measure_color,
                )

            # Top side label (width) - perpendicular to top edge
            top_edge_vec = QPointF(
                self.screen_corners[1].x() - self.screen_corners[0].x(),
                self.screen_corners[1].y() - self.screen_corners[0].y()
            )
            top_edge_len = math.sqrt(top_edge_vec.x()**2 + top_edge_vec.y()**2)
            if top_edge_len > 0:
                # Perpendicular vector pointing up
                perp_x = -top_edge_vec.y() / top_edge_len
                perp_y = top_edge_vec.x() / top_edge_len
                label_pos = QPointF(
                    top_mid.x() + perp_x * offset_distance,
                    top_mid.y() + perp_y * offset_distance
                )
                self._draw_halo_text(
                    painter,
                    int(label_pos.x() - 25),
                    int(label_pos.y() + 5),
                    f"{current_width_um:.2f}",
                    color=self.measure_color,
                )
        painter.end()
