"""Main application window with zoom, pan, and measurements table."""
from PySide6.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
                                QPushButton, QLabel, QFileDialog, QMessageBox,
                                QGroupBox, QTableWidget, QTableWidgetItem,
                                 QHeaderView, QAbstractItemView, QTabWidget,
                                 QRadioButton, QButtonGroup, QSplitter, QComboBox,
                                 QCheckBox, QDoubleSpinBox, QDialog, QFormLayout,
                                 QDialogButtonBox, QSpinBox, QSizePolicy, QToolButton,
                                 QStyle, QLineEdit, QApplication, QProgressDialog,
                                 QToolTip, QCompleter, QSplitterHandle, QFrame,
                                 QPlainTextEdit, QSlider, QGraphicsOpacityEffect,
                                 QListWidget, QListWidgetItem, QStackedWidget)
from PySide6.QtGui import (
    QPixmap,
    QAction,
    QActionGroup,
    QColor,
    QImage,
    QImageReader,
    QPainter,
    QPen,
    QIcon,
    QKeySequence,
    QShortcut,
    QDesktopServices,
    QStandardItemModel,
    QStandardItem,
    QFontDatabase,
    QPolygonF,
    QCursor,
)
from PySide6.QtCore import (
    Qt,
    QPointF,
    QRectF,
    QSize,
    QTimer,
    QThread,
    Signal,
    QPoint,
    QEvent,
    QStringListModel,
    QUrl,
    QStandardPaths,
    QModelIndex,
    QT_TRANSLATE_NOOP,
    QSysInfo,
)

_ALT_LABEL = "⌥" if QSysInfo.productType() == "macos" else "Alt"

# ── Corner icon SVGs ───────────────────────────────────────────────────────────
_SVG_SETTINGS = b"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none"
  stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
  <circle cx="12" cy="12" r="3"/>
  <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06
    a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09
    A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06
    A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09
    A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06
    A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09
    a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06
    A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09
    a1.65 1.65 0 0 0-1.51 1z"/>
</svg>"""

# Stage micrometer: tick-marked ruler bar inside a circle
_SVG_CALIBRATION = b"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none"
  stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
  <circle cx="12" cy="12" r="9"/>
  <line x1="5.5" y1="12" x2="18.5" y2="12"/>
  <line x1="7"   y1="12" x2="7"   y2="9.5"/>
  <line x1="9.5" y1="12" x2="9.5" y2="10.5"/>
  <line x1="12"  y1="12" x2="12"  y2="9.5"/>
  <line x1="14.5" y1="12" x2="14.5" y2="10.5"/>
  <line x1="17"  y1="12" x2="17"  y2="9.5"/>
</svg>"""


def _make_svg_icon(svg_bytes: bytes, color: str = "#c1c8c4") -> QIcon:
    """Render an SVG bytes payload into a QIcon at 64×64 px.

    QSvgRenderer does not resolve CSS 'currentColor', so we substitute the
    stroke colour directly into the SVG before rendering.
    """
    from PySide6.QtSvg import QSvgRenderer
    from PySide6.QtGui import QPainter
    svg = svg_bytes.replace(b"currentColor", color.encode())
    renderer = QSvgRenderer(svg)
    pixmap = QPixmap(64, 64)
    pixmap.fill(Qt.transparent)
    painter = QPainter(pixmap)
    renderer.render(painter)
    painter.end()
    return QIcon(pixmap)
from PySide6.QtNetwork import QNetworkAccessManager, QNetworkRequest, QNetworkReply
import json
import html
import numpy as np
import math
import sqlite3
import time
import os
import warnings
from pathlib import Path
import re
from PIL import Image, ExifTags
from database.models import (
    ObservationDB,
    ImageDB,
    MeasurementDB,
    SettingsDB,
    ReferenceDB,
    CalibrationDB,
    reset_cloud_sync_state,
)
from database.models import SpeciesDataAvailability
from database.database_tags import DatabaseTerms
from database.schema import (
    get_connection,
    get_app_settings,
    save_app_settings,
    update_app_settings,
    get_database_path,
    get_images_dir,
    init_database,
    load_objectives,
    objective_display_name,
    objective_sort_value,
    resolve_objective_key,
)
from utils.annotation_capture import save_spore_annotation
from utils.thumbnail_generator import generate_all_sizes
from utils.image_utils import cleanup_import_temp_file
from utils.heic_converter import maybe_convert_heic
from .delegates import SpeciesItemDelegate
from utils.vernacular_utils import (
    normalize_vernacular_language,
    vernacular_language_label,
    common_name_display_label,
    resolve_vernacular_db_path,
    list_available_vernacular_languages,
)
from utils.publish_targets import (
    PUBLISH_TARGET_ARTPORTALEN_SE,
    PUBLISH_TARGET_ARTSOBS_NO,
    SETTING_ACTIVE_REPORTING_TARGET,
    normalize_publish_target,
    publish_target_label,
)
from .image_gallery_widget import ImageGalleryWidget
from .dialog_helpers import ask_measurements_exist_delete, ask_wrapped_yes_no, make_github_help_button
from .calibration_dialog import CalibrationDialog
from .ingestion_hub_tab import IngestionHubTab
from .measurement_overlay_style import (
    DEFAULT_RECTANGLE_STYLE,
    DEFAULT_RECTANGLE_THICKNESS,
    clamp_rectangle_thickness,
    clamp_stroke_width,
    measure_text_uses_halo,
    normalize_rectangle_style,
    rectangle_thin_stroke_width,
    rectangle_corner_segments,
)
from .zoomable_image_widget import ZoomableImageLabel
from .spore_preview_widget import SporePreviewWidget
from .observations_tab import ObservationsTab
from .live_lab_tab import LiveLabTab
from .database_settings_dialog import DatabaseSettingsDialog
from .cloud_sync_dialog import CloudSyncDialog
from .cloud_reference_dialog import CloudReferenceDialog
from .styles import get_style, apply_palette, pt, _is_dark
from .window_state import GeometryMixin
from .hint_status import HintBar, HintStatusController
from .export_image_dialog import ExportImageDialog as SharedExportImageDialog, ExportPlotDialog, ExportGalleryDialog
from utils.db_share import export_database_bundle as export_db_bundle
from utils.db_share import import_database_bundle as import_db_bundle
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from matplotlib.patches import Ellipse
from matplotlib.ticker import MaxNLocator


def _draw_gallery_rectangle_overlay(
    painter: QPainter,
    polygon: QPolygonF,
    color: QColor,
    rectangle_style: str,
    wide_width: float = 4.0,
    thin_width: float = 1.5,
) -> None:
    if polygon.isEmpty():
        return
    thin_pen = QPen(QColor(color), clamp_stroke_width(thin_width))
    wide_pen = QPen(QColor(color), clamp_stroke_width(wide_width))
    painter.save()
    painter.setBrush(Qt.NoBrush)
    resolved_style = normalize_rectangle_style(rectangle_style)
    if resolved_style == DEFAULT_RECTANGLE_STYLE:
        painter.setPen(wide_pen)
        painter.drawPolygon(polygon)
        painter.setPen(thin_pen)
        painter.drawPolygon(polygon)
        painter.restore()
        return
    painter.setPen(thin_pen)
    painter.drawPolygon(polygon)
    painter.setPen(wide_pen)
    for seg_start, seg_end in rectangle_corner_segments(polygon):
        painter.drawLine(seg_start, seg_end)
    painter.restore()


class _CloudLoginWorker(QThread):
    ok = Signal(object, str)
    fail = Signal(str)

    def __init__(self, email: str, password: str, parent=None) -> None:
        super().__init__(parent)
        self._email = str(email or "").strip()
        self._password = password or ""

    def run(self) -> None:
        try:
            from utils.cloud_sync import SporelyCloudClient

            client = SporelyCloudClient.login(self._email, self._password)
            self.ok.emit(client, self._email)
        except Exception as exc:
            self.fail.emit(str(exc))


class AnalysisGalleryTile(QWidget):
    def __init__(self, rectangle_style: str, parent=None):
        super().__init__(parent)
        self.setMouseTracking(True)
        self.setAttribute(Qt.WA_Hover, True)
        self._rectangle_style = normalize_rectangle_style(rectangle_style)
        self._measurement_polygon = QPolygonF()
        self._hovered_measure = False
        self._selected_measure = False
        self._link_button: QToolButton | None = None
        self._rotate_button: QToolButton | None = None
        self._overlay = QWidget(self)
        self._overlay.setAttribute(Qt.WA_TransparentForMouseEvents, True)
        self._overlay.setAttribute(Qt.WA_NoSystemBackground, True)
        self._overlay.setGeometry(self.rect())
        self._overlay.raise_()

        def _overlay_paint(_event):
            if self._measurement_polygon.isEmpty():
                return
            if not (self._hovered_measure or self._selected_measure):
                return
            painter = QPainter(self._overlay)
            painter.setRenderHint(QPainter.Antialiasing)
            if self._selected_measure:
                _draw_gallery_rectangle_overlay(
                    painter,
                    self._measurement_polygon,
                    QColor(52, 152, 219, 150),
                    self._rectangle_style,
                    wide_width=3.0,
                    thin_width=1.2,
                )
            if self._hovered_measure:
                _draw_gallery_rectangle_overlay(
                    painter,
                    self._measurement_polygon,
                    QColor(231, 76, 60),
                    self._rectangle_style,
                    wide_width=4.0,
                    thin_width=1.5,
                )
            painter.end()

        self._overlay.paintEvent = _overlay_paint

    def set_measurement_polygon(self, polygon: QPolygonF | None) -> None:
        self._measurement_polygon = QPolygonF(polygon or [])
        self._reposition_overlay_buttons()
        self._overlay.update()

    def set_overlay_buttons(self, link_button: QToolButton, rotate_button: QToolButton) -> None:
        self._link_button = link_button
        self._rotate_button = rotate_button
        self._reposition_overlay_buttons()
        self._overlay.raise_()
        self._sync_overlay_buttons()

    def set_measure_selected(self, selected: bool) -> None:
        selected = bool(selected)
        if self._selected_measure == selected:
            return
        self._selected_measure = selected
        self._overlay.raise_()
        self._overlay.update()

    def set_measure_hovered(self, hovered: bool) -> None:
        hovered = bool(hovered)
        if self._hovered_measure == hovered:
            return
        self._hovered_measure = hovered
        self._overlay.raise_()
        self._sync_overlay_buttons()
        self._overlay.update()

    def _sync_overlay_buttons(self) -> None:
        self._overlay.raise_()
        visible = bool(not self._measurement_polygon.isEmpty())
        for button in (self._link_button, self._rotate_button):
            if button is not None:
                button.setVisible(visible)
                if visible:
                    button.raise_()

    def _reposition_overlay_buttons(self) -> None:
        if self._measurement_polygon.isEmpty():
            return
        buttons = [button for button in (self._link_button, self._rotate_button) if button is not None]
        if not buttons:
            return
        top_y = 2
        if self._link_button is not None:
            self._link_button.move(4, top_y)
        if self._rotate_button is not None:
            self._rotate_button.move(self.width() - self._rotate_button.width() - 4, top_y)

    def _cursor_inside_measurement(self, pos: QPointF) -> bool:
        if self._measurement_polygon.isEmpty():
            return False
        bounds = self._measurement_polygon.boundingRect().adjusted(-3.0, -3.0, 3.0, 3.0)
        return bounds.contains(pos) or self._measurement_polygon.containsPoint(pos, Qt.OddEvenFill)

    def event(self, event) -> bool:
        event_type = event.type()
        if event_type in (QEvent.HoverMove, QEvent.MouseMove):
            pos = event.position() if hasattr(event, "position") else QPointF()
            self.set_measure_hovered(self._cursor_inside_measurement(pos))
        elif event_type in (QEvent.HoverEnter, QEvent.Enter):
            pos = event.position() if hasattr(event, "position") else QPointF(self.mapFromGlobal(QCursor.pos()))
            self.set_measure_hovered(self._cursor_inside_measurement(pos))
        elif event_type in (QEvent.HoverLeave, QEvent.Leave):
            self.set_measure_hovered(False)
        return super().event(event)

    def mouseMoveEvent(self, event) -> None:
        self.set_measure_hovered(self._cursor_inside_measurement(event.position()))
        super().mouseMoveEvent(event)

    def enterEvent(self, event) -> None:
        pos = self.mapFromGlobal(QCursor.pos())
        self.set_measure_hovered(self._cursor_inside_measurement(QPointF(pos)))
        super().enterEvent(event)

    def leaveEvent(self, event) -> None:
        self.set_measure_hovered(False)
        super().leaveEvent(event)

    def resizeEvent(self, event) -> None:
        self._overlay.setGeometry(self.rect())
        self._overlay.raise_()
        self._reposition_overlay_buttons()
        super().resizeEvent(event)
from app_identity import APP_NAME, LEGACY_APP_NAME, app_data_dir


_REFERENCE_PLOT_PALETTE = [
    "#0072bd",
    "#d95319",
    "#edb120",
    "#7e2f8e",
    "#77ac30",
    "#4dbeee",
    "#a2142f",
]


def _blend_reference_palette_color(color: str, target: str, ratio: float) -> str:
    """Blend a palette colour toward black or white for darker/lighter variants."""
    base = QColor(color)
    mix = QColor(target)
    ratio = max(0.0, min(1.0, float(ratio)))
    red = round(base.red() + (mix.red() - base.red()) * ratio)
    green = round(base.green() + (mix.green() - base.green()) * ratio)
    blue = round(base.blue() + (mix.blue() - base.blue()) * ratio)
    return QColor(red, green, blue).name().lower()


def reference_plot_palette_groups(_dark: bool | None = None) -> list[tuple[str, list[str]]]:
    """Return darker, medium, and lighter reference colour rows."""
    medium = list(_REFERENCE_PLOT_PALETTE)
    dark = [_blend_reference_palette_color(color, "#000000", 0.22) for color in medium]
    light = [_blend_reference_palette_color(color, "#ffffff", 0.28) for color in medium]
    return [
        ("dark", dark),
        ("medium", medium),
        ("light", light),
    ]


def reference_plot_palette(_dark: bool | None = None) -> list[str]:
    """Return the medium palette used for automatic reference-series colours."""
    for name, colors in reference_plot_palette_groups(_dark):
        if name == "medium":
            return list(colors)
    return list(_REFERENCE_PLOT_PALETTE)


def measure_overlay_palette_groups(_dark: bool | None = None) -> list[tuple[str, list[str]]]:
    """Return the analysis-style palette with black/grey/white folded into dark/medium/light."""
    groups: list[tuple[str, list[str]]] = []
    for name, colors in reference_plot_palette_groups(_dark):
        extended = list(colors)
        if name == "dark":
            extended.append("#000000")
        elif name == "medium":
            extended.append("#808080")
        elif name == "light":
            extended.append("#ffffff")
        groups.append((name, extended))
    return groups


class JumpSlider(QSlider):
    """Slider that jumps to the clicked position instead of paging by step."""

    def mousePressEvent(self, event) -> None:
        if getattr(event, "button", lambda: None)() == Qt.LeftButton:
            if self.orientation() == Qt.Horizontal:
                span = max(1, self.width())
                pos = int(event.position().x())
            else:
                span = max(1, self.height())
                pos = span - int(event.position().y())
            value = QStyle.sliderValueFromPosition(
                self.minimum(),
                self.maximum(),
                pos,
                span,
                self.invertedAppearance(),
            )
            self.setValue(value)
        super().mousePressEvent(event)

class SpinnerWidget(QWidget):
    """Simple spinning doughnut indicator."""

    def __init__(self, parent=None, size=56):
        super().__init__(parent)
        self._angle = 0
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._tick)
        self._timer.start(60)
        self.setFixedSize(size, size)

    def _tick(self):
        self._angle = (self._angle + 30) % 360
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing, True)
        size = min(self.width(), self.height())
        rect = QRectF(6, 6, size - 12, size - 12)

        base_pen = QPen(QColor(220, 220, 220), 6)
        base_pen.setCapStyle(Qt.RoundCap)
        painter.setPen(base_pen)
        painter.drawEllipse(rect)

        arc_pen = QPen(QColor(52, 152, 219), 6)
        arc_pen.setCapStyle(Qt.RoundCap)
        painter.setPen(arc_pen)
        painter.drawArc(rect, int(self._angle * 16), int(120 * 16))


class LoadingDialog(QDialog):
    """Modal loading dialog with a spinner."""

    def __init__(self, parent=None, text="Loading..."):
        super().__init__(parent)
        self.setWindowFlags(self.windowFlags() | Qt.FramelessWindowHint)
        self.setModal(True)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(8)
        self.spinner = SpinnerWidget(self, size=60)
        layout.addWidget(self.spinner, alignment=Qt.AlignCenter)
        label = QLabel(text)
        label.setAlignment(Qt.AlignCenter)
        layout.addWidget(label)


class CollapsibleSplitterHandle(QSplitterHandle):
    """Splitter handle with a collapse/expand button."""

    def __init__(self, orientation, parent=None):
        super().__init__(orientation, parent)
        self._button = QToolButton(self)
        self._button.setAutoRaise(True)
        self._button.setFixedSize(18, 18)
        self._button.clicked.connect(self._on_clicked)
        self._button.setStyleSheet("QToolButton { border: none; }")
        self._update_icon()

    def _on_clicked(self):
        splitter = self.splitter()
        if hasattr(splitter, "toggle_collapse"):
            splitter.toggle_collapse()
        self._update_icon()

    def _update_icon(self):
        splitter = self.splitter()
        collapsed = bool(getattr(splitter, "_is_collapsed", False))
        if self.orientation() == Qt.Vertical:
            icon = self.style().standardIcon(QStyle.SP_ArrowUp if collapsed else QStyle.SP_ArrowDown)
        else:
            icon = self.style().standardIcon(QStyle.SP_ArrowLeft if collapsed else QStyle.SP_ArrowRight)
        self._button.setIcon(icon)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        size = self._button.size()
        self._button.move(
            (self.width() - size.width()) // 2,
            (self.height() - size.height()) // 2
        )


class CollapsibleSplitter(QSplitter):
    """Splitter that can collapse/expand a child with a handle button."""

    collapse_toggled = Signal(bool)

    def __init__(self, orientation, collapse_index=1, parent=None):
        super().__init__(orientation, parent)
        self._collapse_index = collapse_index
        self._last_sizes = None
        self._is_collapsed = False

    def createHandle(self):
        handle = CollapsibleSplitterHandle(self.orientation(), self)
        self.collapse_toggled.connect(lambda _state: handle._update_icon())
        return handle

    def toggle_collapse(self):
        sizes = self.sizes()
        if not sizes:
            return
        if not self._is_collapsed and sizes[self._collapse_index] > 0:
            self._last_sizes = sizes
            total = sum(sizes)
            sizes[self._collapse_index] = 0
            sizes[1 - self._collapse_index] = total
            self.setSizes(sizes)
            self._is_collapsed = True
        else:
            if self._last_sizes:
                self.setSizes(self._last_sizes)
            self._is_collapsed = False
        self.collapse_toggled.emit(self._is_collapsed)


class CollapsibleSection(QWidget):
    """Collapsible section with a header button."""

    def __init__(self, title, content, expanded=True, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        self._toggle_btn = QToolButton()
        self._toggle_btn.setObjectName("collapsibleToggle")
        self._toggle_btn.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        self._toggle_btn.setText(title)
        self._toggle_btn.setCheckable(True)
        self._toggle_btn.setChecked(bool(expanded))
        self._toggle_btn.setAutoRaise(True)
        self._toggle_btn.clicked.connect(self._on_toggled)
        layout.addWidget(self._toggle_btn)

        self._content = content
        layout.addWidget(self._content)

        self._on_toggled(self._toggle_btn.isChecked())

    def _on_toggled(self, checked):
        self._toggle_btn.setArrowType(Qt.DownArrow if checked else Qt.RightArrow)
        self._content.setVisible(bool(checked))
class ScaleBarCalibrationDialog(QDialog):
    """Simple scale bar calibration dialog for two-point measurement."""

    def __init__(
        self,
        main_window,
        initial_value: float | None = None,
        unit_label: str = "\u03bcm",
        unit_multiplier: float = 1.0,
        previous_key: str | None = None,
    ):
        super().__init__(main_window)
        self.setWindowTitle(self.tr("Scale bar"))
        self.setModal(False)
        self.main_window = main_window
        self.previous_key = previous_key
        self.scale_applied = False
        self.auto_apply = False
        self.unit_label = unit_label
        self.unit_multiplier = unit_multiplier
        if initial_value is None:
            initial_value = 10.0 if unit_label == "\u03bcm" else 1.0

        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.length_input = QDoubleSpinBox()
        if unit_label == "mm":
            self.length_input.setRange(1.0, 100000.0)
            self.length_input.setDecimals(0)
            self.length_input.setSingleStep(1.0)
        else:
            self.length_input.setRange(0.1, 100000.0)
            self.length_input.setDecimals(2)
        self.length_input.setValue(initial_value)
        self.length_input.setSuffix(f" {unit_label}")
        form.addRow(self.tr("Scale bar length:"), self.length_input)

        self.scale_label = QLabel("--")
        form.addRow(self.tr("Custom scale:"), self.scale_label)

        layout.addLayout(form)

        btn_row = QHBoxLayout()
        self.select_btn = QPushButton(self.tr("Select scale bar endpoints"))
        self.select_btn.clicked.connect(self._on_select)
        btn_row.addWidget(self.select_btn)
        btn_row.addStretch()

        close_btn = QPushButton(self.tr("Close"))
        close_btn.clicked.connect(self.close)
        btn_row.addWidget(close_btn)

        layout.addLayout(btn_row)

    def _on_select(self):
        if not self.main_window:
            return
        self.hide()
        self.main_window.enter_calibration_mode(self)

    def set_calibration_distance(self, distance_pixels: float):
        if not distance_pixels or distance_pixels <= 0:
            return
        length_um = float(self.length_input.value()) * self.unit_multiplier
        scale_um = length_um / distance_pixels
        scale_nm = scale_um * 1000.0
        self.scale_label.setText(f"{scale_nm:.2f} nm/px")
        self.show()
        self._pending_scale_um = scale_um

    def apply_scale(self, distance_pixels: float):
        if not distance_pixels or distance_pixels <= 0:
            return
        length_um = float(self.length_input.value()) * self.unit_multiplier
        scale_um = length_um / distance_pixels
        scale_nm = scale_um * 1000.0
        self.scale_label.setText(f"{scale_nm:.2f} nm/px")
        applied = False
        if self.main_window:
            applied = bool(self.main_window.set_custom_scale(scale_um))
        self.scale_applied = applied

    def closeEvent(self, event):
        if not self.scale_applied and self.previous_key:
            self.main_window._populate_scale_combo(self.previous_key)
        super().closeEvent(event)


class DatabaseBundleOptionsDialog(QDialog):
    """Dialog for choosing which database content to export/import."""

    def __init__(self, title: str, defaults: dict | None = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setModal(True)
        self.setMinimumWidth(420)

        defaults = defaults or {}

        layout = QVBoxLayout(self)
        prompt = QLabel(self.tr("Select what you want to include:"))
        layout.addWidget(prompt)

        self.observations_check = QCheckBox(
            self.tr("Observations (field and taxonomy metadata)")
        )
        self.images_check = QCheckBox(
            self.tr("Images of observations")
        )
        self.measurements_check = QCheckBox(self.tr("Spore measurements"))
        self.calibrations_check = QCheckBox(self.tr("Calibrations"))
        self.references_check = QCheckBox(self.tr("Reference values"))

        self.observations_check.setChecked(defaults.get("observations", True))
        self.images_check.setChecked(defaults.get("images", True))
        self.measurements_check.setChecked(defaults.get("measurements", True))
        self.calibrations_check.setChecked(defaults.get("calibrations", True))
        self.references_check.setChecked(defaults.get("reference_values", True))

        layout.addWidget(self.observations_check)
        layout.addWidget(self.images_check)
        layout.addWidget(self.measurements_check)
        layout.addWidget(self.calibrations_check)
        layout.addWidget(self.references_check)

        hint = QLabel(self.tr("Thumbnails are not included; they will be regenerated by the app."))
        hint.setWordWrap(True)
        hint.setStyleSheet(f"color: #7f8c8d; font-size: {pt(9)}pt;")
        layout.addWidget(hint)

        calibration_hint = QLabel(
            self.tr("Calibration bundles automatically include calibration images and objective profiles.")
        )
        calibration_hint.setWordWrap(True)
        calibration_hint.setStyleSheet(f"color: #7f8c8d; font-size: {pt(9)}pt;")
        layout.addWidget(calibration_hint)

        buttons = QDialogButtonBox(self)
        ok_btn = buttons.addButton(self.tr("OK"), QDialogButtonBox.AcceptRole)
        cancel_btn = buttons.addButton(self.tr("Cancel"), QDialogButtonBox.RejectRole)
        ok_btn.clicked.connect(self._on_accept)
        cancel_btn.clicked.connect(self.reject)
        layout.addWidget(buttons)

        self.images_check.toggled.connect(self._sync_bundle_dependencies)
        self.measurements_check.toggled.connect(self._sync_bundle_dependencies)
        self._sync_bundle_dependencies()

    def _sync_bundle_dependencies(self, _checked: bool | None = None) -> None:
        measurements_checked = self.measurements_check.isChecked()
        images_checked = self.images_check.isChecked()
        if measurements_checked:
            self.observations_check.setChecked(True)
            self.images_check.setChecked(True)
            self.observations_check.setEnabled(False)
            self.images_check.setEnabled(False)
            return
        if images_checked:
            self.observations_check.setChecked(True)
            self.observations_check.setEnabled(False)
            self.images_check.setEnabled(True)
        else:
            self.observations_check.setEnabled(True)
            self.images_check.setEnabled(True)

    def _on_accept(self) -> None:
        if not any([
            self.observations_check.isChecked(),
            self.images_check.isChecked(),
            self.measurements_check.isChecked(),
            self.calibrations_check.isChecked(),
            self.references_check.isChecked(),
        ]):
            QMessageBox.warning(self, self.tr("Nothing Selected"), self.tr("Select at least one item."))
            return
        self.accept()

    def get_options(self) -> dict:
        return {
            "observations": self.observations_check.isChecked(),
            "images": self.images_check.isChecked(),
            "measurements": self.measurements_check.isChecked(),
            "calibrations": self.calibrations_check.isChecked(),
            "reference_values": self.references_check.isChecked(),
        }


class LanguageSettingsDialog(QDialog):
    """Dialog for UI and vernacular language settings."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Language"))
        self.setModal(True)
        self.setMinimumWidth(420)
        self._ui_changed = False
        self._vernacular_changed = False
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        form = QFormLayout()

        restart_notice = QLabel(
            self.tr("Language change will apply after restart. Incomplete translations fall back to English.")
        )
        restart_notice.setWordWrap(True)
        restart_notice.setStyleSheet("color: #34495e;")
        layout.addWidget(restart_notice)

        self.ui_combo = QComboBox()
        self.ui_combo.addItem(self.tr("English"), "en")
        self.ui_combo.addItem(self.tr("Norwegian"), "nb_NO")
        self.ui_combo.addItem(self.tr("Swedish"), "sv_SE")
        self.ui_combo.addItem(self.tr("German"), "de_DE")
        form.addRow(self.tr("UI language:"), self.ui_combo)

        self.vernacular_combo = QComboBox()
        self._populate_vernacular_languages()
        form.addRow(self.tr("Vernacular names:"), self.vernacular_combo)

        layout.addLayout(form)

        buttons = QDialogButtonBox(self)
        ok_btn = buttons.addButton(self.tr("OK"), QDialogButtonBox.AcceptRole)
        cancel_btn = buttons.addButton(self.tr("Cancel"), QDialogButtonBox.RejectRole)
        ok_btn.clicked.connect(self._save)
        cancel_btn.clicked.connect(self.reject)
        layout.addWidget(buttons)

        self._load_settings()

    def _populate_vernacular_languages(self):
        self.vernacular_combo.blockSignals(True)
        self.vernacular_combo.clear()
        for code in list_available_vernacular_languages():
            label = vernacular_language_label(code) or code
            self.vernacular_combo.addItem(self.tr(label), code)
        self.vernacular_combo.blockSignals(False)

    def _load_settings(self):
        current_ui = str(SettingsDB.get_setting("ui_language", "en") or "en").replace("-", "_")
        current_prefix = current_ui.split("_", 1)[0].lower()
        if current_prefix == "de":
            current_ui = "de_DE"
        elif current_prefix in {"nb", "nn", "no"}:
            current_ui = "nb_NO"
        elif current_prefix == "sv":
            current_ui = "sv_SE"
        else:
            current_ui = "en"
        current_vern = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
        ui_index = self.ui_combo.findData(current_ui)
        if ui_index >= 0:
            self.ui_combo.setCurrentIndex(ui_index)
        vern_index = self.vernacular_combo.findData(current_vern)
        if vern_index >= 0:
            self.vernacular_combo.setCurrentIndex(vern_index)
        elif self.vernacular_combo.count():
            self.vernacular_combo.setCurrentIndex(0)

    def _save(self):
        old_ui = SettingsDB.get_setting("ui_language", "en")
        old_vern = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))

        new_ui = self.ui_combo.currentData()
        new_vern = normalize_vernacular_language(self.vernacular_combo.currentData())

        if new_ui and new_ui != old_ui:
            SettingsDB.set_setting("ui_language", new_ui)
            update_app_settings({"ui_language": new_ui})
            self._ui_changed = True

        if new_vern and new_vern != old_vern:
            SettingsDB.set_setting("vernacular_language", new_vern)
            update_app_settings({"vernacular_language": new_vern})
            self._vernacular_changed = True

        if self._vernacular_changed:
            parent = self.parent()
            if parent and hasattr(parent, "apply_vernacular_language_change"):
                parent.apply_vernacular_language_change()

        self.accept()


class AppearanceDialog(QDialog):
    """Dialog for colour-theme preference (Auto / Light / Dark)."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Appearance"))
        self.setModal(True)
        self.setMinimumWidth(320)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        label = QLabel(self.tr("Color theme:"))
        layout.addWidget(label)
        layout.addSpacing(4)

        self.btn_auto  = QRadioButton(self.tr("Auto (follow system)"))
        self.btn_light = QRadioButton(self.tr("Light"))
        self.btn_dark  = QRadioButton(self.tr("Dark"))
        layout.addWidget(self.btn_auto)
        layout.addWidget(self.btn_light)
        layout.addWidget(self.btn_dark)
        layout.addSpacing(8)

        buttons = QDialogButtonBox(self)
        ok_btn     = buttons.addButton(self.tr("OK"),     QDialogButtonBox.AcceptRole)
        cancel_btn = buttons.addButton(self.tr("Cancel"), QDialogButtonBox.RejectRole)
        ok_btn.clicked.connect(self._save)
        cancel_btn.clicked.connect(self.reject)
        layout.addWidget(buttons)

        self._load_settings()

    def _load_settings(self):
        theme = SettingsDB.get_setting("ui_theme", "auto")
        if theme == "dark":
            self.btn_dark.setChecked(True)
        elif theme == "light":
            self.btn_light.setChecked(True)
        else:
            self.btn_auto.setChecked(True)

    def _save(self):
        if self.btn_dark.isChecked():
            theme = "dark"
        elif self.btn_light.isChecked():
            theme = "light"
        else:
            theme = "auto"
        SettingsDB.set_setting("ui_theme", theme)
        parent = self.parent()
        if parent and hasattr(parent, "_apply_theme"):
            parent._apply_theme()
        self.accept()


class SettingsHubDialog(QDialog):
    """Single settings hub with left-nav pane and stacked content pages.

    Consolidates: User profile, Database, Online publishing, Language, Appearance.
    Calibration is intentionally excluded (it's a workflow, not a preference).
    """

    PAGE_PROFILE    = 0
    PAGE_DATABASE   = 1
    PAGE_PUBLISHING = 2
    PAGE_CLOUD      = 3
    PAGE_LANGUAGE   = 4
    PAGE_APPEARANCE = 5

    def __init__(self, parent=None, start_page: int = 0):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Preferences"))
        self.setModal(True)
        self.setMinimumSize(860, 580)
        self.resize(960, 680)

        # State that pages need to report back to the caller
        self._profile_changed     = False
        self._language_ui_changed  = False
        self._language_vern_changed = False
        self._publishing_changed   = False
        self._database_changed     = False

        self._build_ui()
        self._nav.setCurrentRow(start_page)
        self._stack.setCurrentIndex(start_page)

    # ── Layout ────────────────────────────────────────────────────────────────

    def _build_ui(self):
        root = QHBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # Create the artsobs dialog once; cloud/content pages borrow its section widgets
        self._artsobs_dialog = ArtsobservasjonerSettingsDialog(self)
        self._artsobs_dialog.setWindowFlags(Qt.Widget)

        # Left nav
        self._nav = QListWidget()
        self._nav.setObjectName("settingsNav")
        self._nav.setFixedWidth(180)
        self._nav.setFocusPolicy(Qt.NoFocus)
        for label in (
            self.tr("User profile"),
            self.tr("Database"),
            self.tr("Online publishing"),
            self.tr("Sporely Cloud"),
            self.tr("Language"),
            self.tr("Appearance"),
        ):
            item = QListWidgetItem(label)
            item.setSizeHint(QSize(180, 36))
            self._nav.addItem(item)
        # Stacked pages
        self._stack = QStackedWidget()
        self._stack.addWidget(self._build_profile_page())
        self._stack.addWidget(self._build_database_page())
        self._stack.addWidget(self._build_publishing_page())
        self._stack.addWidget(self._build_cloud_page())
        self._stack.addWidget(self._build_language_page())
        self._stack.addWidget(self._build_appearance_page())

        # Wire nav after stack exists
        self._nav.currentRowChanged.connect(self._stack.setCurrentIndex)

        root.addWidget(self._nav)

        right = QVBoxLayout()
        right.setContentsMargins(20, 16, 20, 12)
        right.setSpacing(12)
        right.addWidget(self._stack)

        btn_row = QHBoxLayout()
        btn_row.addStretch()
        close_btn = QPushButton(self.tr("Close"))
        close_btn.setDefault(True)
        close_btn.clicked.connect(self.accept)
        btn_row.addWidget(close_btn)
        right.addLayout(btn_row)

        right_widget = QWidget()
        right_widget.setLayout(right)
        root.addWidget(right_widget, 1)

    # ── Pages ─────────────────────────────────────────────────────────────────

    def _build_profile_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        profile = SettingsDB.get_profile()

        info = QLabel(self.tr(
            "Name is used for the copyright watermark on images.\n"
            "Name and email (optional) are added to observations in the database, "
            "useful if you share your observations with others."
        ))
        info.setWordWrap(True)
        layout.addWidget(info)

        form = QFormLayout()
        self._profile_name  = QLineEdit(profile.get("name", ""))
        self._profile_email = QLineEdit(profile.get("email", ""))
        form.addRow(self.tr("Name"), self._profile_name)
        form.addRow(self.tr("Email"), self._profile_email)
        layout.addLayout(form)

        save_btn = QPushButton(self.tr("Save"))
        save_btn.clicked.connect(self._save_profile)
        layout.addWidget(save_btn)

        copyright = self._artsobs_dialog._copyright_section
        copyright.setParent(page)
        copyright.show()
        layout.addWidget(copyright)
        layout.addStretch()
        return page

    def _build_database_page(self) -> QWidget:
        """Embed the DatabaseSettingsDialog's central widget."""
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        # DatabaseSettingsDialog is large — instantiate it hidden and reparent its
        # central widget so we avoid duplicating its UI code.
        self._db_dialog = DatabaseSettingsDialog(self)
        self._db_dialog.setWindowFlags(Qt.Widget)  # embed as widget, not window
        layout.addWidget(self._db_dialog)
        return page

    def _build_publishing_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        # Cloud and copyright sections live on their own pages; content checklist stays here
        self._artsobs_dialog._cloud_section.hide()
        self._artsobs_dialog._cloud_content_section.hide()
        self._artsobs_dialog._copyright_section.hide()
        layout.addWidget(self._artsobs_dialog)
        return page

    def _build_cloud_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)
        cloud = self._artsobs_dialog._cloud_section
        cloud.setParent(page)
        cloud.show()
        layout.addWidget(cloud)
        cloud_content = self._artsobs_dialog._cloud_content_section
        cloud_content.setParent(page)
        cloud_content.show()
        layout.addWidget(cloud_content)
        reset_group = QGroupBox(self.tr("Cloud account link"), page)
        reset_layout = QVBoxLayout(reset_group)
        reset_layout.setContentsMargins(10, 10, 10, 10)
        reset_layout.setSpacing(8)
        reset_note = QLabel(
            self.tr(
                "Reset only the local cloud link after you have deleted the old cloud account. "
                "Local observations and images remain in this database."
            )
        )
        reset_note.setWordWrap(True)
        reset_note.setStyleSheet("color: #6b7280; font-size: 11px;")
        reset_layout.addWidget(reset_note)
        reset_row = QHBoxLayout()
        self._reset_cloud_link_btn = QPushButton(self.tr("Reset Cloud Link..."))
        self._reset_cloud_link_btn.clicked.connect(self._reset_cloud_link)
        reset_row.addWidget(self._reset_cloud_link_btn)
        reset_row.addStretch()
        reset_layout.addLayout(reset_row)
        layout.addWidget(reset_group)
        layout.addStretch()
        return page

    def _reset_cloud_link(self) -> None:
        main_window = self.parent()
        observations_tab = getattr(main_window, "observations_tab", None)
        is_sync_running = getattr(observations_tab, "_is_cloud_sync_running", None)
        if callable(is_sync_running) and is_sync_running():
            QMessageBox.warning(
                self,
                self.tr("Sporely Cloud Sync"),
                self.tr("Wait for the current cloud sync to finish before resetting the cloud link."),
            )
            return

        box = QMessageBox(self)
        box.setIcon(QMessageBox.Critical)
        box.setWindowTitle(self.tr("CRITICAL: Reset Cloud Link"))
        box.setText(
            self.tr(
                "Resetting the cloud link will sever the connection to your current Sporely Cloud account. "
                "Your local data will remain safe, but the next time you sync, ALL local images and observations "
                "will be uploaded as brand new files.\n\n"
                "IMPORTANT: This action DOES NOT delete your old cloud data. To prevent duplicate storage, you MUST "
                "do the following first:\n"
                "1. Open the Sporely Web App.\n"
                "2. Log into your CURRENT account.\n"
                "3. Go to Profile -> Delete Account to erase your old data."
            )
        )
        cancel_btn = box.addButton(self.tr("Cancel"), QMessageBox.RejectRole)
        proceed_btn = box.addButton(
            self.tr("I have already deleted my old account, proceed with reset"),
            QMessageBox.DestructiveRole,
        )
        box.setDefaultButton(cancel_btn)
        box.exec()
        if box.clickedButton() is not proceed_btn:
            return

        try:
            reset_cloud_sync_state()
            from utils.cloud_sync import SporelyCloudClient

            SporelyCloudClient.clear_credentials()
            update_app_settings({"cloud_user_email": None})
            self._artsobs_dialog._cloud_client = None
            self._artsobs_dialog._update_cloud_controls()
            if observations_tab is not None and hasattr(observations_tab, "refresh_observations"):
                observations_tab.refresh_observations(show_status=False)
        except Exception as exc:
            QMessageBox.critical(
                self,
                self.tr("Reset Cloud Link Failed"),
                self.tr("Unable to reset cloud sync state.\n\n{error}").format(error=exc),
            )
            return

        QMessageBox.information(
            self,
            self.tr("Cloud Link Reset"),
            self.tr(
                "Cloud sync state was reset. You have been logged out of Sporely Cloud.\n\n"
                "Log in with the new account when you are ready to sync this local database again."
            ),
        )

    def _build_language_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        restart_notice = QLabel(self.tr(
            "Language change will apply after restart. "
            "Incomplete translations fall back to English."
        ))
        restart_notice.setWordWrap(True)
        layout.addWidget(restart_notice)

        form = QFormLayout()
        self._lang_ui_combo = QComboBox()
        for label, code in (
            (self.tr("English"),   "en"),
            (self.tr("Norwegian"), "nb_NO"),
            (self.tr("Swedish"),   "sv_SE"),
            (self.tr("German"),    "de_DE"),
        ):
            self._lang_ui_combo.addItem(label, code)

        self._lang_vern_combo = QComboBox()
        for code in list_available_vernacular_languages():
            lbl = vernacular_language_label(code) or code
            self._lang_vern_combo.addItem(self.tr(lbl), code)

        form.addRow(self.tr("UI language:"), self._lang_ui_combo)
        form.addRow(self.tr("Vernacular names:"), self._lang_vern_combo)
        layout.addLayout(form)

        save_btn = QPushButton(self.tr("Save"))
        save_btn.clicked.connect(self._save_language)
        layout.addWidget(save_btn)
        layout.addStretch()

        self._load_language_settings()
        return page

    def _build_appearance_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        layout.addWidget(QLabel(self.tr("Color theme:")))

        self._theme_auto  = QRadioButton(self.tr("Auto (follow system)"))
        self._theme_light = QRadioButton(self.tr("Light"))
        self._theme_dark  = QRadioButton(self.tr("Dark"))
        for btn in (self._theme_auto, self._theme_light, self._theme_dark):
            layout.addWidget(btn)
            btn.clicked.connect(self._save_appearance)

        layout.addStretch()
        self._load_appearance_settings()
        return page

    # ── Save helpers ──────────────────────────────────────────────────────────

    def _save_profile(self):
        SettingsDB.set_profile(
            self._profile_name.text().strip(),
            self._profile_email.text().strip(),
        )
        self._profile_changed = True
        parent = self.parent()
        if parent and hasattr(parent, "_update_measure_copyright_overlay"):
            parent._update_measure_copyright_overlay()

    def _load_language_settings(self):
        current_ui = str(SettingsDB.get_setting("ui_language", "en") or "en").replace("-", "_")
        prefix = current_ui.split("_", 1)[0].lower()
        if prefix == "de":       current_ui = "de_DE"
        elif prefix in {"nb","nn","no"}: current_ui = "nb_NO"
        elif prefix == "sv":     current_ui = "sv_SE"
        else:                    current_ui = "en"
        idx = self._lang_ui_combo.findData(current_ui)
        if idx >= 0: self._lang_ui_combo.setCurrentIndex(idx)

        current_vern = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
        idx = self._lang_vern_combo.findData(current_vern)
        if idx >= 0: self._lang_vern_combo.setCurrentIndex(idx)

    def _save_language(self):
        old_ui   = SettingsDB.get_setting("ui_language", "en")
        old_vern = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
        new_ui   = self._lang_ui_combo.currentData()
        new_vern = normalize_vernacular_language(self._lang_vern_combo.currentData())
        if new_ui and new_ui != old_ui:
            SettingsDB.set_setting("ui_language", new_ui)
            update_app_settings({"ui_language": new_ui})
            self._language_ui_changed = True
        if new_vern and new_vern != old_vern:
            SettingsDB.set_setting("vernacular_language", new_vern)
            update_app_settings({"vernacular_language": new_vern})
            self._language_vern_changed = True
        if self._language_vern_changed:
            parent = self.parent()
            if parent and hasattr(parent, "apply_vernacular_language_change"):
                parent.apply_vernacular_language_change()

    def _load_appearance_settings(self):
        theme = SettingsDB.get_setting("ui_theme", "auto")
        if theme == "dark":      self._theme_dark.setChecked(True)
        elif theme == "light":   self._theme_light.setChecked(True)
        else:                    self._theme_auto.setChecked(True)

    def _save_appearance(self):
        if self._theme_dark.isChecked():    theme = "dark"
        elif self._theme_light.isChecked(): theme = "light"
        else:                               theme = "auto"
        SettingsDB.set_setting("ui_theme", theme)
        parent = self.parent()
        if parent and hasattr(parent, "_apply_theme"):
            parent._apply_theme()

    # ── Public helpers ────────────────────────────────────────────────────────

    @property
    def publishing_changed(self) -> bool:
        return self._publishing_changed

    @property
    def database_changed(self) -> bool:
        return self._database_changed


class ArtsobservasjonerSettingsDialog(QDialog):
    """Dialog for Artsobservasjoner login and upload preferences."""

    SETTING_UPLOAD_TARGET = "artsobs_upload_target"
    SETTING_ENABLED_UPLOAD_TARGETS = "artsobs_enabled_upload_targets"
    SETTING_ACTIVE_REPORTING_TARGET = SETTING_ACTIVE_REPORTING_TARGET
    SETTING_INCLUDE_ANNOTATIONS = "artsobs_publish_include_annotations"
    SETTING_INCLUDE_SPORE_STATS = "artsobs_publish_include_spore_stats"
    SETTING_INCLUDE_MEASURE_PLOTS = "artsobs_publish_include_measure_plots"
    SETTING_INCLUDE_THUMBNAIL_GALLERY = "artsobs_publish_include_thumbnail_gallery"
    SETTING_INCLUDE_PLATE = "artsobs_publish_include_plate"
    SETTING_INCLUDE_COPYRIGHT = "artsobs_publish_include_copyright"
    SETTING_IMAGE_LICENSE = "artsobs_publish_image_license"
    SETTING_SHOW_SCALE_BAR = "artsobs_publish_show_scale_bar"
    SETTING_CLOUD_DEFAULT_SHARING_SCOPE = "sporely_cloud_default_sharing_scope"
    SETTING_CLOUD_IMAGE_SIZE_MODE = "sporely_cloud_image_size_mode"
    SETTING_CLOUD_INCLUDE_ANNOTATIONS = "sporely_cloud_include_annotations"
    SETTING_CLOUD_SHOW_SCALE_BAR = "sporely_cloud_show_scale_bar"
    SETTING_CLOUD_INCLUDE_MEASURE_PLOTS = "sporely_cloud_include_measure_plots"
    SETTING_CLOUD_INCLUDE_PLATE = "sporely_cloud_include_plate"
    SETTING_CLOUD_INCLUDE_COPYRIGHT = "sporely_cloud_include_copyright"
    SETTING_DEBUG_CLOUD_PLAN_OVERRIDE = "sporely_debug_cloud_plan_override"
    SETTING_SHOW_DEBUG_CLOUD_PLAN_OVERRIDE = "sporely_show_debug_cloud_plan_override"
    SETTING_INAT_CLIENT_ID = "inat_client_id"
    SETTING_INAT_CLIENT_SECRET = "inat_client_secret"
    SETTING_INAT_REDIRECT_URI = "inat_redirect_uri"
    SETTING_MO_APP_API_KEY = "mushroomobserver_app_api_key"
    SETTING_MO_USER_API_KEY = "mushroomobserver_user_api_key"
    UPLOADER_LABELS = {
        "mobile": QT_TRANSLATE_NOOP("ArtsobservasjonerSettingsDialog", "Artsobservasjoner"),
        "web": QT_TRANSLATE_NOOP("ArtsobservasjonerSettingsDialog", "Artsobservasjoner"),
        "artportalen": QT_TRANSLATE_NOOP("ArtsobservasjonerSettingsDialog", "Artportalen"),
        "inat": QT_TRANSLATE_NOOP("ArtsobservasjonerSettingsDialog", "iNaturalist"),
        "mo": QT_TRANSLATE_NOOP("ArtsobservasjonerSettingsDialog", "Mushroom Observer"),
    }
    ARTSOBS_MEDIA_LICENSE_OPTIONS = (
        (
            "10",
            QT_TRANSLATE_NOOP("ArtsobservasjonerSettingsDialog", "Creative Commons 4.0 (CC) BY"),
            QT_TRANSLATE_NOOP(
                "ArtsobservasjonerSettingsDialog",
                "Others can share, reuse, modify, and use commercially, as long as they give credit.",
            ),
        ),
        (
            "20",
            QT_TRANSLATE_NOOP("ArtsobservasjonerSettingsDialog", "Creative Commons 4.0 (CC) BY-SA"),
            QT_TRANSLATE_NOOP(
                "ArtsobservasjonerSettingsDialog",
                "Others can share, reuse, modify, and use commercially, as long as they give credit and keep the same license.",
            ),
        ),
        (
            "30",
            QT_TRANSLATE_NOOP("ArtsobservasjonerSettingsDialog", "Creative Commons 4.0 (CC) BY-NC-SA"),
            QT_TRANSLATE_NOOP(
                "ArtsobservasjonerSettingsDialog",
                "Others can share, reuse, and modify, but not commercially, and they must give credit and keep the same license.",
            ),
        ),
        (
            "60",
            QT_TRANSLATE_NOOP("ArtsobservasjonerSettingsDialog", "None (all rights reserved)"),
            QT_TRANSLATE_NOOP(
                "ArtsobservasjonerSettingsDialog",
                "No reuse or sharing without permission (except legal exceptions).",
            ),
        ),
    )

    def __init__(self, parent=None):
        super().__init__(parent)
        from utils.artsobs_uploaders import list_uploaders
        self.cookies_file = (
            app_data_dir() / "artsobservasjoner_cookies.json"
        )
        self._auth_widget = None
        self._uploaders = list_uploaders()
        self._target_status: dict[str, bool] = {}
        self._loading_settings = False
        self._hint_controller: HintStatusController | None = None
        self._inat_session_client_id = ""
        self._inat_session_client_secret = ""
        self._cloud_client = None
        self._cloud_login_worker: _CloudLoginWorker | None = None
        self._cloud_login_email = ""
        self._cloud_login_password = ""
        self._cloud_login_remember = False
        self._cloud_debug_tap_count = 0
        self._cloud_debug_tap_timer = QTimer(self)
        self._cloud_debug_tap_timer.setSingleShot(True)
        self._cloud_debug_tap_timer.timeout.connect(self._reset_cloud_debug_taps)
        self.setWindowTitle(self.tr("Online publishing"))
        self.setModal(True)
        self.setMinimumSize(700, 760)
        self.resize(760, 800)
        self._build_ui()
        self._load_settings()
        self._update_status()
        self._update_controls()

    def _inat_token_file(self) -> Path:
        return self.cookies_file.with_name("inaturalist_oauth_tokens.json")

    def _inat_credentials(self) -> tuple[str, str, str]:
        client_id = (
            (SettingsDB.get_setting(self.SETTING_INAT_CLIENT_ID, "") or "").strip()
            or (self._inat_session_client_id or "").strip()
        )
        client_secret = (
            (SettingsDB.get_setting(self.SETTING_INAT_CLIENT_SECRET, "") or "").strip()
            or (self._inat_session_client_secret or "").strip()
        )
        redirect_uri = (
            SettingsDB.get_setting(self.SETTING_INAT_REDIRECT_URI, "http://localhost:8000/callback")
            or "http://localhost:8000/callback"
        )
        if not client_id:
            client_id = (os.getenv("INAT_CLIENT_ID", "") or "").strip()
        if not client_id:
            client_id = "bJW2eDa8qF8GJIQbQbuG_LBgmOQYRGMh9-Ja58QBqmc"
        if not client_secret:
            client_secret = (os.getenv("INAT_CLIENT_SECRET", "") or "").strip()
        return client_id, client_secret, redirect_uri

    def _inat_oauth_client(self, require_credentials: bool = False):
        from utils.inat_oauth import INatOAuthClient

        client_id, client_secret, redirect_uri = self._inat_credentials()
        if require_credentials and not client_id:
            return None
        return INatOAuthClient(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
            token_file=self._inat_token_file(),
        )

    def _uploader_display_label(self, uploader) -> str:
        if not uploader:
            return ""
        label = self.UPLOADER_LABELS.get(
            getattr(uploader, "key", ""),
            getattr(uploader, "label", ""),
        )
        return self.tr(label) if label else ""

    def _mushroomobserver_credentials(self) -> tuple[str, str]:
        app_key = (SettingsDB.get_setting(self.SETTING_MO_APP_API_KEY, "") or "").strip()
        if not app_key:
            app_key = (os.getenv("MO_APP_API_KEY", "") or "").strip()
        if not app_key:
            app_key = (os.getenv("MUSHROOMOBSERVER_APP_API_KEY", "") or "").strip()

        user_key = (SettingsDB.get_setting(self.SETTING_MO_USER_API_KEY, "") or "").strip()
        if not user_key:
            user_key = (os.getenv("MO_USER_API_KEY", "") or "").strip()
        if not user_key:
            user_key = (os.getenv("MUSHROOMOBSERVER_USER_API_KEY", "") or "").strip()
        return app_key, user_key

    def _prompt_mushroomobserver_login(self, require_app_key: bool = False) -> bool:
        dialog = QDialog(self)
        dialog.setWindowTitle(self.tr("Mushroom Observer login"))
        dialog.setModal(True)
        dialog.setMinimumWidth(460)

        app_key, user_key = self._mushroomobserver_credentials()

        layout = QVBoxLayout(dialog)
        layout.addWidget(
            QLabel(
                self.tr(
                    "Enter your Mushroom Observer user API key."
                )
            )
        )

        form = QFormLayout()
        app_key_edit = None
        if require_app_key:
            app_key_edit = QLineEdit()
            app_key_edit.setPlaceholderText(self.tr("App API key"))
            app_key_edit.setEchoMode(QLineEdit.Password)
            app_key_edit.setText(app_key)
            form.addRow(self.tr("App API key:"), app_key_edit)

        user_key_edit = QLineEdit()
        user_key_edit.setPlaceholderText(self.tr("User API key"))
        user_key_edit.setEchoMode(QLineEdit.Password)
        user_key_edit.setText(user_key)
        form.addRow(self.tr("User API key:"), user_key_edit)
        layout.addLayout(form)

        buttons = QDialogButtonBox(dialog)
        ok_btn = buttons.addButton(self.tr("OK"), QDialogButtonBox.AcceptRole)
        cancel_btn = buttons.addButton(self.tr("Cancel"), QDialogButtonBox.RejectRole)
        layout.addWidget(buttons)

        def _accept_if_valid() -> None:
            if app_key_edit is not None and not app_key_edit.text().strip():
                QMessageBox.warning(
                    dialog,
                    self.tr("Missing Information"),
                    self.tr("Please enter the app API key and your user API key."),
                )
                return
            if not user_key_edit.text().strip():
                QMessageBox.warning(
                    dialog,
                    self.tr("Missing Information"),
                    self.tr("Please enter your user API key."),
                )
                return
            dialog.accept()

        ok_btn.clicked.connect(_accept_if_valid)
        cancel_btn.clicked.connect(dialog.reject)
        user_key_edit.returnPressed.connect(_accept_if_valid)
        if app_key_edit is not None:
            app_key_edit.returnPressed.connect(_accept_if_valid)
        user_key_edit.setFocus()

        if dialog.exec() != QDialog.Accepted:
            return False

        if app_key_edit is not None:
            SettingsDB.set_setting(self.SETTING_MO_APP_API_KEY, app_key_edit.text().strip())
        SettingsDB.set_setting(self.SETTING_MO_USER_API_KEY, user_key_edit.text().strip())
        return True

    def _build_ui(self):
        layout = QVBoxLayout(self)

        websites_group = QGroupBox(self.tr("Websites"), self)
        websites_layout = QVBoxLayout(websites_group)
        websites_layout.setContentsMargins(10, 10, 10, 10)
        websites_layout.setSpacing(8)

        reporting_row = QHBoxLayout()
        reporting_row.setContentsMargins(0, 0, 0, 0)
        reporting_row.setSpacing(12)
        reporting_row.addWidget(QLabel(self.tr("Reporting system:")))
        self.reporting_target_group = QButtonGroup(self)
        self.reporting_target_group.setExclusive(True)
        self.reporting_target_no_radio = QRadioButton(self.tr("Norway"))
        self.reporting_target_se_radio = QRadioButton(self.tr("Sweden"))
        self.reporting_target_group.addButton(self.reporting_target_no_radio)
        self.reporting_target_group.addButton(self.reporting_target_se_radio)
        self.reporting_target_no_radio.toggled.connect(self._on_reporting_target_changed)
        self.reporting_target_se_radio.toggled.connect(self._on_reporting_target_changed)
        reporting_row.addWidget(self.reporting_target_no_radio)
        reporting_row.addWidget(self.reporting_target_se_radio)
        reporting_row.addStretch(1)
        websites_layout.addLayout(reporting_row)

        reporting_note = QLabel(
            self.tr("This controls the biotope/substrate choices in the observation editor and which Nordic publish target appears on the Publish button.")
        )
        reporting_note.setWordWrap(True)
        reporting_note.setStyleSheet("color: #6b7280; font-size: 11px;")
        websites_layout.addWidget(reporting_note)

        self.targets_table = QTableWidget(0, 3, self)
        self.targets_table.setHorizontalHeaderLabels(
            [self.tr("Use"), self.tr("Publish target"), self.tr("Status")]
        )
        self.targets_table.verticalHeader().setVisible(False)
        self.targets_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.targets_table.setSelectionMode(QTableWidget.SingleSelection)
        self.targets_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.targets_table.setAlternatingRowColors(True)
        self.targets_table.setMinimumHeight(160)
        self.targets_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.targets_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.targets_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        for uploader in self._uploaders:
            row = self.targets_table.rowCount()
            self.targets_table.insertRow(row)
            use_item = QTableWidgetItem()
            use_item.setFlags(
                Qt.ItemIsEnabled
                | Qt.ItemIsSelectable
                | Qt.ItemIsUserCheckable
            )
            use_item.setCheckState(Qt.Checked)
            use_item.setData(Qt.UserRole, uploader.key)
            target_item = QTableWidgetItem(self._uploader_display_label(uploader))
            target_item.setData(Qt.UserRole, uploader.key)
            status_item = QTableWidgetItem(self.tr("Not logged in"))
            self.targets_table.setItem(row, 0, use_item)
            self.targets_table.setItem(row, 1, target_item)
            self.targets_table.setItem(row, 2, status_item)
        self.targets_table.itemChanged.connect(self._on_target_enabled_item_changed)
        self.targets_table.itemSelectionChanged.connect(self._on_target_selection_changed)
        websites_layout.addWidget(self.targets_table, 1)

        button_layout = QHBoxLayout()
        self.login_button = QPushButton(self.tr("Log in"))
        self.login_button.clicked.connect(self._open_login)
        button_layout.addWidget(self.login_button)

        self.logout_button = QPushButton(self.tr("Log out"))
        self.logout_button.clicked.connect(self._logout)
        button_layout.addWidget(self.logout_button)
        button_layout.addStretch()
        websites_layout.addLayout(button_layout)
        layout.addWidget(websites_group)

        cloud_group = QGroupBox(self.tr("Sporely Cloud"), self)
        cloud_layout = QVBoxLayout(cloud_group)
        cloud_layout.setContentsMargins(10, 10, 10, 10)
        cloud_layout.setSpacing(8)

        cloud_note = QLabel(
            self.tr(
                "Cloud backup uses the same image selection and image overlay options as online publishing. "
                "Choose whether synced images should keep full size or be reduced to 2 MP. "
                "Reduced is the planned low-cost default for future free cloud hosting, while full size keeps the door open for a later Pro backup tier."
            )
        )
        cloud_note.setWordWrap(True)
        cloud_note.setStyleSheet("color: #6b7280; font-size: 11px;")
        cloud_layout.addWidget(cloud_note)

        cloud_preferences_row = QHBoxLayout()
        cloud_preferences_row.setContentsMargins(0, 0, 0, 0)
        cloud_preferences_row.setSpacing(20)

        cloud_sharing_column = QVBoxLayout()
        cloud_sharing_column.setContentsMargins(0, 0, 0, 0)
        cloud_sharing_column.setSpacing(4)
        cloud_sharing_column.addWidget(QLabel(self.tr("Default sharing:")))
        self.cloud_sharing_group = QButtonGroup(self)
        self.cloud_sharing_private_radio = QRadioButton(self.tr("Private"))
        self.cloud_sharing_friends_radio = QRadioButton(self.tr("Friends"))
        self.cloud_sharing_public_radio = QRadioButton(self.tr("Public"))
        for radio, scope in (
            (self.cloud_sharing_private_radio, "private"),
            (self.cloud_sharing_friends_radio, "friends"),
            (self.cloud_sharing_public_radio, "public"),
        ):
            self.cloud_sharing_group.addButton(radio)
            radio.setProperty("sharing_scope", scope)
            radio.toggled.connect(self._on_cloud_sharing_changed)
            cloud_sharing_column.addWidget(radio)

        cloud_image_size_column = QVBoxLayout()
        cloud_image_size_column.setContentsMargins(0, 0, 0, 0)
        cloud_image_size_column.setSpacing(4)
        cloud_image_size_column.addWidget(QLabel(self.tr("Sync image size:")))
        self.cloud_image_size_group = QButtonGroup(self)
        self.cloud_image_size_reduced_radio = QRadioButton(self.tr("Reduced (2 MP)"))
        self.cloud_image_size_full_radio = QRadioButton(self.tr("Full size"))
        for radio, mode in (
            (self.cloud_image_size_reduced_radio, "reduced"),
            (self.cloud_image_size_full_radio, "full"),
        ):
            self.cloud_image_size_group.addButton(radio)
            radio.setProperty("cloud_image_size_mode", mode)
            radio.toggled.connect(self._on_cloud_image_size_changed)
            cloud_image_size_column.addWidget(radio)

        cloud_preferences_row.addLayout(cloud_sharing_column, 1)
        cloud_preferences_row.addLayout(cloud_image_size_column, 1)
        cloud_layout.addLayout(cloud_preferences_row)

        self.cloud_status_label = QLabel(self.tr("Not logged in"))
        self.cloud_status_label.setWordWrap(True)
        self.cloud_status_label.setCursor(Qt.PointingHandCursor)
        self.cloud_status_label.mousePressEvent = self._on_cloud_status_label_clicked
        cloud_layout.addWidget(self.cloud_status_label)

        self.cloud_debug_override_container = QWidget(self)
        cloud_debug_layout = QVBoxLayout(self.cloud_debug_override_container)
        cloud_debug_layout.setContentsMargins(0, 0, 0, 0)
        cloud_debug_layout.setSpacing(4)
        cloud_debug_layout.addWidget(QLabel(self.tr("Testing override:")))
        self.cloud_debug_plan_group = QButtonGroup(self)
        debug_row = QHBoxLayout()
        debug_row.setContentsMargins(0, 0, 0, 0)
        debug_row.setSpacing(12)
        self.cloud_debug_plan_server_radio = QRadioButton(self.tr("Server"))
        self.cloud_debug_plan_free_radio = QRadioButton(self.tr("Free"))
        self.cloud_debug_plan_pro_radio = QRadioButton(self.tr("Pro"))
        for radio, mode in (
            (self.cloud_debug_plan_server_radio, "server"),
            (self.cloud_debug_plan_free_radio, "free"),
            (self.cloud_debug_plan_pro_radio, "pro"),
        ):
            self.cloud_debug_plan_group.addButton(radio)
            radio.setProperty("cloud_debug_plan_override", mode)
            radio.toggled.connect(self._on_debug_cloud_plan_override_changed)
            debug_row.addWidget(radio)
        debug_row.addStretch(1)
        cloud_debug_layout.addLayout(debug_row)
        self.cloud_debug_note_label = QLabel(self.tr("Local testing only. Server uses your normal cloud settings."))
        self.cloud_debug_note_label.setWordWrap(True)
        self.cloud_debug_note_label.setStyleSheet("color: #6b7280; font-size: 11px;")
        cloud_debug_layout.addWidget(self.cloud_debug_note_label)
        self.cloud_debug_override_container.setVisible(False)
        cloud_layout.addWidget(self.cloud_debug_override_container)

        cloud_button_row = QHBoxLayout()
        self.cloud_login_button = QPushButton(self.tr("Log in"))
        self.cloud_login_button.clicked.connect(self._open_cloud_login)
        cloud_button_row.addWidget(self.cloud_login_button)

        self.cloud_logout_button = QPushButton(self.tr("Log out"))
        self.cloud_logout_button.clicked.connect(self._logout_cloud)
        cloud_button_row.addWidget(self.cloud_logout_button)
        cloud_button_row.addStretch()
        cloud_layout.addLayout(cloud_button_row)

        self._cloud_section = cloud_group
        layout.addWidget(cloud_group, 0)

        options_group = QGroupBox(self.tr("Publish content"), self)
        options_layout = QVBoxLayout(options_group)
        options_layout.setContentsMargins(10, 10, 10, 10)
        options_layout.setSpacing(6)
        self.include_annotations_checkbox = QCheckBox(self)
        self.show_scale_bar_checkbox = QCheckBox(self)
        self.include_spore_stats_checkbox = QCheckBox(self)
        self.include_measure_plots_checkbox = QCheckBox(self)
        self.include_thumbnail_gallery_checkbox = QCheckBox(self)
        self.include_plate_checkbox = QCheckBox(self)
        self.include_copyright_checkbox = QCheckBox(self)
        self.image_license_label = QLabel(self.tr("License"), self)
        self.image_license_combo = QComboBox(self)
        self.hint_bar = HintBar(self)
        self.hint_bar.set_wrap_mode(True)
        self._hint_controller = HintStatusController(self.hint_bar, self)
        wrapped_options = (
            (
                self.include_annotations_checkbox,
                self.tr("Show measures on images"),
                self.tr(
                    "Include measures on published images\n"
                    "Note: This will override current View settings for each image"
                ),
            ),
            (
                self.show_scale_bar_checkbox,
                self.tr("Show scale bar on images"),
                self.tr("Shows a scale bar, defined in the Measure module"),
            ),
            (
                self.include_spore_stats_checkbox,
                self.tr("Include spore stats in comment"),
                self.tr("Adds spore stats to your notes."),
            ),
            (
                self.include_measure_plots_checkbox,
                self.tr("Include measure plots"),
                self.tr("Uploads an image of the plot in the Analysis module."),
            ),
            (
                self.include_thumbnail_gallery_checkbox,
                self.tr("Include thumbnail gallery"),
                self.tr("Adds a mosaic gallery image (Same as Export gallery in Analysis)."),
            ),
            (
                self.include_plate_checkbox,
                self.tr("Include plate"),
                self.tr("Uploads the current species plate image."),
            ),
        )
        for checkbox, text, help_text in wrapped_options:
            checkbox.toggled.connect(self._save_settings)
            self._add_wrapped_checkbox_row(
                options_layout,
                checkbox,
                text,
                help_text,
            )

        copyright_group = QGroupBox(self.tr("Image copyright"), self)
        copyright_layout = QVBoxLayout(copyright_group)
        copyright_layout.setContentsMargins(10, 10, 10, 10)
        copyright_layout.setSpacing(6)

        watermark_help = self.tr(
            "Adds a visible watermark on published images. The selected license still applies even if watermark is off."
        )
        license_help = ""
        self.image_license_label.setWordWrap(True)
        self._register_hint_widget(self.image_license_label, license_help)
        self._register_hint_widget(self.image_license_combo, license_help)
        self.image_license_label.setToolTip(license_help)
        self.image_license_combo.setToolTip(license_help)
        copyright_layout.addWidget(self.image_license_label)

        self._populate_image_license_combo()
        self.image_license_combo.currentIndexChanged.connect(self._save_settings)
        self.image_license_combo.currentIndexChanged.connect(self._on_image_license_selection_changed)
        self.image_license_combo.highlighted.connect(self._on_image_license_highlighted)
        license_row = QHBoxLayout()
        license_row.setContentsMargins(24, 0, 0, 0)
        license_row.setSpacing(0)
        license_row.addWidget(self.image_license_combo, 1)
        copyright_layout.addLayout(license_row)

        self.include_copyright_checkbox.toggled.connect(self._save_settings)
        self.include_copyright_checkbox.toggled.connect(self._update_watermark_controls)
        self._add_wrapped_checkbox_row(
            copyright_layout,
            self.include_copyright_checkbox,
            self.tr("Include watermark"),
            watermark_help,
        )
        self._copyright_section = copyright_group

        layout.addWidget(options_group, 1)

        # ── Cloud publish content (subset — no spore stats, no thumbnail gallery) ──
        cloud_content_group = QGroupBox(self.tr("Sync content"), self)
        cloud_content_layout = QVBoxLayout(cloud_content_group)
        cloud_content_layout.setContentsMargins(10, 10, 10, 10)
        cloud_content_layout.setSpacing(6)
        self.cloud_include_annotations_checkbox = QCheckBox(self)
        self.cloud_show_scale_bar_checkbox = QCheckBox(self)
        self.cloud_include_measure_plots_checkbox = QCheckBox(self)
        self.cloud_include_plate_checkbox = QCheckBox(self)
        self.cloud_include_copyright_checkbox = QCheckBox(self)
        cloud_content_options = (
            (
                self.cloud_include_annotations_checkbox,
                self.tr("Show measures on images"),
                self.tr("Include measures on synced images"),
            ),
            (
                self.cloud_show_scale_bar_checkbox,
                self.tr("Show scale bar on images"),
                self.tr("Shows a scale bar on synced images"),
            ),
            (
                self.cloud_include_measure_plots_checkbox,
                self.tr("Include measure plots"),
                self.tr("Uploads an image of the plot in the Analysis module."),
            ),
            (
                self.cloud_include_plate_checkbox,
                self.tr("Include plate"),
                self.tr("Uploads the current species plate image."),
            ),
            (
                self.cloud_include_copyright_checkbox,
                self.tr("Include watermark"),
                self.tr("Adds a visible watermark on synced images."),
            ),
        )
        for checkbox, text, help_text in cloud_content_options:
            checkbox.toggled.connect(self._save_settings)
            self._add_wrapped_checkbox_row(cloud_content_layout, checkbox, text, help_text)
        self._cloud_content_section = cloud_content_group

        bottom_row = QHBoxLayout()
        bottom_row.setContentsMargins(0, 0, 0, 0)
        bottom_row.setSpacing(8)
        bottom_row.addWidget(self.hint_bar, 1)
        bottom_row.addWidget(
            make_github_help_button(self, "artsobservasjoner.md"),
            0,
            Qt.AlignRight | Qt.AlignVCenter,
        )
        buttons = QDialogButtonBox(self)
        close_button = buttons.addButton(self.tr("Close"), QDialogButtonBox.RejectRole)
        close_button.clicked.connect(self.reject)
        bottom_row.addWidget(buttons, 0, Qt.AlignRight | Qt.AlignVCenter)
        layout.addLayout(bottom_row)

    def _add_wrapped_checkbox_row(
        self,
        parent_layout: QVBoxLayout,
        checkbox: QCheckBox,
        text: str,
        help_text: str | None = None,
    ) -> None:
        row = QHBoxLayout()
        row.setSpacing(8)
        checkbox.setText("")
        self._register_hint_widget(checkbox, help_text)
        row.addWidget(checkbox, 0, Qt.AlignTop)
        text_row = QHBoxLayout()
        text_row.setContentsMargins(0, 0, 0, 0)
        text_row.setSpacing(4)

        label = QLabel(text, self)
        label.setWordWrap(True)
        label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        label.setTextInteractionFlags(Qt.NoTextInteraction)
        label.setCursor(Qt.PointingHandCursor)
        label.mousePressEvent = lambda _event, cb=checkbox: cb.click() if cb.isEnabled() else None
        self._register_hint_widget(label, help_text)
        text_row.addWidget(label, 1)
        row.addLayout(text_row, 1)
        parent_layout.addLayout(row)

    def _register_hint_widget(self, widget: QWidget, hint_text: str | None) -> None:
        if not widget:
            return
        hint = (hint_text or "").strip()
        if self._hint_controller is not None:
            self._hint_controller.register_widget(widget, hint)
            return
        widget.setProperty("_hint_text", hint)
        widget.setToolTip("")

    def set_hint(self, text: str | None, tone: str = "info") -> None:
        if self._hint_controller is not None:
            self._hint_controller.set_hint(text, tone=tone)

    def _on_target_selection_changed(self) -> None:
        self._update_controls()
        if not self._loading_settings:
            self._save_settings()

    def _on_reporting_target_changed(self) -> None:
        if not self._loading_settings:
            self._save_settings()

    def _on_target_enabled_item_changed(self, item: QTableWidgetItem) -> None:
        if self._loading_settings or item is None or item.column() != 0:
            return
        self._save_settings()

    def _selected_cloud_sharing_scope(self) -> str:
        for radio, scope in (
            (getattr(self, "cloud_sharing_private_radio", None), "private"),
            (getattr(self, "cloud_sharing_friends_radio", None), "friends"),
            (getattr(self, "cloud_sharing_public_radio", None), "public"),
        ):
            if radio is not None and radio.isChecked():
                return scope
        return "private"

    def _set_cloud_sharing_scope(self, scope: str | None) -> None:
        normalized = str(scope or "").strip().lower()
        if normalized not in {"private", "friends", "public"}:
            normalized = "private"
        radio_map = {
            "private": getattr(self, "cloud_sharing_private_radio", None),
            "friends": getattr(self, "cloud_sharing_friends_radio", None),
            "public": getattr(self, "cloud_sharing_public_radio", None),
        }
        radio = radio_map.get(normalized) or getattr(self, "cloud_sharing_private_radio", None)
        if radio is not None:
            radio.setChecked(True)

    def _on_cloud_sharing_changed(self, _checked: bool) -> None:
        if not self._loading_settings:
            self._save_settings()

    def _selected_cloud_image_size_mode(self) -> str:
        for radio, mode in (
            (getattr(self, "cloud_image_size_reduced_radio", None), "reduced"),
            (getattr(self, "cloud_image_size_full_radio", None), "full"),
        ):
            if radio is not None and radio.isChecked():
                return mode
        return "reduced"

    def _set_cloud_image_size_mode(self, mode: str | None) -> None:
        normalized = str(mode or "").strip().lower()
        if normalized not in {"reduced", "full"}:
            normalized = "reduced"
        radio_map = {
            "reduced": getattr(self, "cloud_image_size_reduced_radio", None),
            "full": getattr(self, "cloud_image_size_full_radio", None),
        }
        radio = radio_map.get(normalized) or getattr(self, "cloud_image_size_reduced_radio", None)
        if radio is not None:
            radio.setChecked(True)

    def _on_cloud_image_size_changed(self, _checked: bool) -> None:
        if not self._loading_settings:
            self._save_settings()

    @staticmethod
    def _normalize_debug_cloud_plan_override(value: str | None) -> str:
        raw = str(value or "").strip().lower()
        return raw if raw in {"server", "free", "pro"} else "server"

    def _debug_cloud_plan_controls_visible(self) -> bool:
        env_enabled = str(os.getenv("SPORELY_ENABLE_DEBUG_CLOUD_PLAN", "") or "").strip().lower() in {"1", "true", "yes", "on"}
        if env_enabled:
            return True
        raw = SettingsDB.get_setting(self.SETTING_SHOW_DEBUG_CLOUD_PLAN_OVERRIDE, "0")
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    def _set_debug_cloud_plan_controls_visible(self, visible: bool) -> None:
        SettingsDB.set_setting(
            self.SETTING_SHOW_DEBUG_CLOUD_PLAN_OVERRIDE,
            "1" if visible else "0",
        )

    def _selected_debug_cloud_plan_override(self) -> str:
        for radio, mode in (
            (getattr(self, "cloud_debug_plan_server_radio", None), "server"),
            (getattr(self, "cloud_debug_plan_free_radio", None), "free"),
            (getattr(self, "cloud_debug_plan_pro_radio", None), "pro"),
        ):
            if radio is not None and radio.isChecked():
                return mode
        return "server"

    def _set_debug_cloud_plan_override(self, mode: str | None) -> None:
        normalized = self._normalize_debug_cloud_plan_override(mode)
        radio_map = {
            "server": getattr(self, "cloud_debug_plan_server_radio", None),
            "free": getattr(self, "cloud_debug_plan_free_radio", None),
            "pro": getattr(self, "cloud_debug_plan_pro_radio", None),
        }
        radio = radio_map.get(normalized) or getattr(self, "cloud_debug_plan_server_radio", None)
        if radio is not None:
            radio.setChecked(True)

    def _reset_cloud_debug_taps(self) -> None:
        self._cloud_debug_tap_count = 0

    def _on_cloud_status_label_clicked(self, _event) -> None:
        self._cloud_debug_tap_count += 1
        self._cloud_debug_tap_timer.start(1500)
        if self._cloud_debug_tap_count < 5:
            return
        self._cloud_debug_tap_count = 0
        if str(os.getenv("SPORELY_ENABLE_DEBUG_CLOUD_PLAN", "") or "").strip().lower() in {"1", "true", "yes", "on"}:
            return
        next_visible = not self._debug_cloud_plan_controls_visible()
        if not next_visible:
            self._set_debug_cloud_plan_override("server")
        self._set_debug_cloud_plan_controls_visible(next_visible)
        self._update_cloud_debug_controls()
        self._save_settings()
        self.set_hint(
            self.tr("Testing override shown") if next_visible else self.tr("Testing override hidden"),
            tone="success" if next_visible else "info",
        )

    def _update_cloud_debug_controls(self) -> None:
        visible = self._debug_cloud_plan_controls_visible()
        override_mode = self._selected_debug_cloud_plan_override()
        if hasattr(self, "cloud_debug_override_container"):
            self.cloud_debug_override_container.setVisible(visible)
        manual_size_enabled = override_mode == "server"
        for radio in (
            getattr(self, "cloud_image_size_reduced_radio", None),
            getattr(self, "cloud_image_size_full_radio", None),
        ):
            if radio is not None:
                radio.setEnabled(manual_size_enabled)
        if hasattr(self, "cloud_debug_note_label"):
            self.cloud_debug_note_label.setText(
                self.tr("Local testing only. Server uses your normal cloud settings.")
                if override_mode == "server"
                else self.tr("Local testing only. This override currently replaces the normal cloud image-size policy.")
            )

    def _on_debug_cloud_plan_override_changed(self, _checked: bool) -> None:
        self._update_cloud_debug_controls()
        if not self._loading_settings:
            self._save_settings()

    @staticmethod
    def _setting_enabled(key: str, default: bool = False) -> bool:
        fallback = "1" if default else "0"
        raw = SettingsDB.get_setting(key, fallback)
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    def _image_license_help_text(self, index: int | None = None) -> str:
        if index is None:
            index = self.image_license_combo.currentIndex()
        if index is None or index < 0 or index >= self.image_license_combo.count():
            return self.tr("Select the image license for watermark/upload.")
        label = str(self.image_license_combo.itemText(index) or "").strip()
        desc = str(
            self.image_license_combo.itemData(index, Qt.UserRole + 1) or ""
        ).strip()
        if label and desc:
            return f"{label}: {desc}"
        return desc or label or self.tr("Select the image license for watermark/upload.")

    def _populate_image_license_combo(self) -> None:
        self.image_license_combo.clear()
        for code, label, description in self.ARTSOBS_MEDIA_LICENSE_OPTIONS:
            self.image_license_combo.addItem(self.tr(label), code)
            idx = self.image_license_combo.count() - 1
            self.image_license_combo.setItemData(idx, self.tr(description), Qt.UserRole + 1)
        self._update_image_license_hint_text()

    def _current_image_license_code(self) -> str:
        code = str(self.image_license_combo.currentData() or "").strip()
        valid_codes = {item[0] for item in self.ARTSOBS_MEDIA_LICENSE_OPTIONS}
        return code if code in valid_codes else "60"

    def _update_watermark_controls(self) -> None:
        # License applies to uploaded images whether or not the visible watermark is enabled.
        self.image_license_label.setEnabled(True)
        self.image_license_combo.setEnabled(True)

    def _update_image_license_hint_text(self, index: int | None = None) -> None:
        hint = self._image_license_help_text(index)
        for widget in (self.image_license_label, self.image_license_combo):
            widget.setProperty("_hint_text", hint)
            widget.setToolTip("")

    def _on_image_license_selection_changed(self, index: int) -> None:
        self._update_image_license_hint_text(index)

    def _on_image_license_highlighted(self, index: int) -> None:
        hover_hint = self._image_license_help_text(index)
        if self._hint_controller is not None and self.image_license_combo.isEnabled():
            self._hint_controller.set_hint(hover_hint)

    def _save_settings(self) -> None:
        selected_uploader = self._selected_uploader()
        if selected_uploader:
            SettingsDB.set_setting(self.SETTING_UPLOAD_TARGET, selected_uploader.key)
        SettingsDB.set_setting(
            self.SETTING_ENABLED_UPLOAD_TARGETS,
            json.dumps(self._enabled_target_keys_from_table()),
        )
        selected_reporting_target = (
            PUBLISH_TARGET_ARTPORTALEN_SE
            if getattr(self, "reporting_target_se_radio", None) is not None
            and self.reporting_target_se_radio.isChecked()
            else PUBLISH_TARGET_ARTSOBS_NO
        )
        SettingsDB.set_setting(
            self.SETTING_ACTIVE_REPORTING_TARGET,
            selected_reporting_target,
        )
        SettingsDB.set_setting(
            self.SETTING_INCLUDE_ANNOTATIONS,
            "1" if self.include_annotations_checkbox.isChecked() else "0",
        )
        SettingsDB.set_setting(
            self.SETTING_SHOW_SCALE_BAR,
            "1" if self.show_scale_bar_checkbox.isChecked() else "0",
        )
        SettingsDB.set_setting(
            self.SETTING_INCLUDE_SPORE_STATS,
            "1" if self.include_spore_stats_checkbox.isChecked() else "0",
        )
        SettingsDB.set_setting(
            self.SETTING_INCLUDE_MEASURE_PLOTS,
            "1" if self.include_measure_plots_checkbox.isChecked() else "0",
        )
        SettingsDB.set_setting(
            self.SETTING_INCLUDE_THUMBNAIL_GALLERY,
            "1" if self.include_thumbnail_gallery_checkbox.isChecked() else "0",
        )
        SettingsDB.set_setting(
            self.SETTING_INCLUDE_PLATE,
            "1" if self.include_plate_checkbox.isChecked() else "0",
        )
        SettingsDB.set_setting(
            self.SETTING_INCLUDE_COPYRIGHT,
            "1" if self.include_copyright_checkbox.isChecked() else "0",
        )
        SettingsDB.set_setting(
            self.SETTING_IMAGE_LICENSE,
            self._current_image_license_code(),
        )
        SettingsDB.set_setting(
            self.SETTING_CLOUD_DEFAULT_SHARING_SCOPE,
            self._selected_cloud_sharing_scope(),
        )
        SettingsDB.set_setting(
            self.SETTING_CLOUD_IMAGE_SIZE_MODE,
            self._selected_cloud_image_size_mode(),
        )
        SettingsDB.set_setting(
            self.SETTING_CLOUD_INCLUDE_ANNOTATIONS,
            "1" if self.cloud_include_annotations_checkbox.isChecked() else "0",
        )
        SettingsDB.set_setting(
            self.SETTING_CLOUD_SHOW_SCALE_BAR,
            "1" if self.cloud_show_scale_bar_checkbox.isChecked() else "0",
        )
        SettingsDB.set_setting(
            self.SETTING_CLOUD_INCLUDE_MEASURE_PLOTS,
            "1" if self.cloud_include_measure_plots_checkbox.isChecked() else "0",
        )
        SettingsDB.set_setting(
            self.SETTING_CLOUD_INCLUDE_PLATE,
            "1" if self.cloud_include_plate_checkbox.isChecked() else "0",
        )
        SettingsDB.set_setting(
            self.SETTING_CLOUD_INCLUDE_COPYRIGHT,
            "1" if self.cloud_include_copyright_checkbox.isChecked() else "0",
        )
        SettingsDB.set_setting(
            self.SETTING_DEBUG_CLOUD_PLAN_OVERRIDE,
            self._selected_debug_cloud_plan_override(),
        )

    def _selected_uploader(self):
        if not self._uploaders:
            return None
        rows = self.targets_table.selectionModel().selectedRows()
        if not rows:
            return self._uploaders[0]
        row = rows[0].row()
        item = self.targets_table.item(row, 1)
        selected_key = item.data(Qt.UserRole) if item else None
        for uploader in self._uploaders:
            if uploader.key == selected_key:
                return uploader
        return self._uploaders[0]

    def _enabled_target_keys_from_table(self) -> list[str]:
        enabled: list[str] = []
        for row in range(self.targets_table.rowCount()):
            use_item = self.targets_table.item(row, 0)
            if use_item is None or use_item.checkState() != Qt.Checked:
                continue
            key = str(use_item.data(Qt.UserRole) or "").strip().lower()
            if key:
                enabled.append(key)
        return enabled

    @classmethod
    def _stored_enabled_upload_target_keys(cls, uploaders: list | None = None) -> list[str]:
        available_keys = [
            str(getattr(uploader, "key", "")).strip().lower()
            for uploader in (uploaders or [])
            if getattr(uploader, "key", None)
        ]
        if not available_keys:
            return []
        raw = SettingsDB.get_setting(cls.SETTING_ENABLED_UPLOAD_TARGETS, "")
        enabled: list[str] = []
        if raw:
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = []
            if isinstance(parsed, list):
                enabled = [
                    str(value).strip().lower()
                    for value in parsed
                    if str(value).strip().lower() in available_keys
                ]
        if not enabled:
            enabled = list(available_keys)
        return enabled

    @classmethod
    def active_reporting_target(cls) -> str:
        return normalize_publish_target(
            SettingsDB.get_setting(cls.SETTING_ACTIVE_REPORTING_TARGET, PUBLISH_TARGET_ARTSOBS_NO),
            fallback=PUBLISH_TARGET_ARTSOBS_NO,
        )

    @classmethod
    def enabled_upload_target_keys(cls, uploaders: list | None = None) -> list[str]:
        available_keys = cls._stored_enabled_upload_target_keys(uploaders)
        if not available_keys:
            return []
        active_uploader = "artportalen" if cls.active_reporting_target() == PUBLISH_TARGET_ARTPORTALEN_SE else "web"
        enabled: list[str] = []
        for key in available_keys:
            if key in {"web", "mobile", "artportalen"}:
                if key == active_uploader:
                    enabled.append(key)
            else:
                enabled.append(key)
        return enabled

    def _refresh_target_status(self) -> None:
        status = {uploader.key: False for uploader in self._uploaders}

        try:
            from utils.artsobservasjoner_auto_login import ArtsObservasjonerAuth

            auth = ArtsObservasjonerAuth(cookies_file=self.cookies_file)
            for uploader in self._uploaders:
                if uploader.key == "inat":
                    try:
                        inat = self._inat_oauth_client(require_credentials=False)
                        status[uploader.key] = bool(inat and inat.get_valid_access_token())
                    except Exception:
                        status[uploader.key] = False
                elif uploader.key == "artportalen":
                    try:
                        from utils.artportalen_auth import ArtportalenAuth

                        status[uploader.key] = bool(ArtportalenAuth().get_valid_cookies())
                    except Exception:
                        status[uploader.key] = False
                elif uploader.key == "mo":
                    app_key, user_key = self._mushroomobserver_credentials()
                    status[uploader.key] = bool(app_key and user_key)
                else:
                    status[uploader.key] = bool(auth.get_valid_cookies(target=uploader.key))
        except Exception:
            # Keep conservative status when auth helper fails.
            for uploader in self._uploaders:
                if uploader.key == "inat":
                    try:
                        inat = self._inat_oauth_client(require_credentials=False)
                        status[uploader.key] = bool(inat and inat.get_valid_access_token())
                    except Exception:
                        status[uploader.key] = False
                elif uploader.key == "artportalen":
                    try:
                        from utils.artportalen_auth import ArtportalenAuth

                        status[uploader.key] = bool(ArtportalenAuth().get_valid_cookies())
                    except Exception:
                        status[uploader.key] = False
                elif uploader.key == "mo":
                    app_key, user_key = self._mushroomobserver_credentials()
                    status[uploader.key] = bool(app_key and user_key)
                else:
                    status[uploader.key] = False
        self._target_status = status

    def _is_logged_in(self, uploader_key: str | None = None) -> bool:
        if uploader_key:
            return bool(self._target_status.get(uploader_key))
        return any(self._target_status.values())

    def _update_status(self):
        self._refresh_target_status()
        logged_count = 0
        selected = self._selected_uploader()
        selected_logged_in = bool(selected and self._is_logged_in(selected.key))
        for row in range(self.targets_table.rowCount()):
            target_item = self.targets_table.item(row, 1)
            status_item = self.targets_table.item(row, 2)
            key = target_item.data(Qt.UserRole) if target_item else None
            logged_in = self._is_logged_in(key)
            if logged_in:
                logged_count += 1
            if status_item:
                status_item.setText(self.tr("Logged in") if logged_in else self.tr("Not logged in"))
                status_item.setForeground(QColor("#e8e8e8") if _is_dark("auto") else QColor("#2c3e50"))

        if selected_logged_in and selected:
            self.set_hint(
                self.tr("Logged in to {target}").format(
                    target=self._uploader_display_label(selected)
                ),
                tone="success",
            )
        elif logged_count:
            self.set_hint(self.tr("Logged in to one or more services"), tone="success")
        else:
            self.set_hint(self.tr("Not logged in"), tone="warning")

    def _refresh_cloud_status(self) -> None:
        try:
            from utils.cloud_sync import SporelyCloudClient

            self._cloud_client = SporelyCloudClient.from_stored_credentials()
        except Exception:
            self._cloud_client = None

    def _update_cloud_controls(self) -> None:
        self._refresh_cloud_status()
        email = str(get_app_settings().get("cloud_user_email") or "").strip()
        logged_in = bool(self._cloud_client)
        debug_override = self._selected_debug_cloud_plan_override()
        if hasattr(self, "cloud_status_label"):
            if logged_in:
                shown = email or f"{self._cloud_client.user_id[:8]}..."
                text = self.tr("Signed in as: {account}").format(account=shown)
            else:
                text = self.tr("Not logged in")
            if debug_override in {"free", "pro"}:
                text = f"{text}\n{self.tr('Testing override: {mode}').format(mode=self.tr('Free') if debug_override == 'free' else self.tr('Pro'))}"
            self.cloud_status_label.setText(text)
        if hasattr(self, "cloud_login_button"):
            self.cloud_login_button.setEnabled(self._cloud_login_worker is None and not logged_in)
            self.cloud_login_button.setText(
                self.tr("Signing in...") if self._cloud_login_worker is not None else self.tr("Log in")
            )
        if hasattr(self, "cloud_logout_button"):
            self.cloud_logout_button.setEnabled(self._cloud_login_worker is None and logged_in)
        self._update_cloud_debug_controls()

    def _update_controls(self):
        uploader = self._selected_uploader()
        selected_logged_in = self._is_logged_in(uploader.key if uploader else None)
        self.logout_button.setEnabled(bool(uploader) and selected_logged_in)
        self.login_button.setText(self.tr("Log in"))
        self._update_watermark_controls()
        self._update_cloud_controls()

    def _open_cloud_login(self) -> None:
        if self._cloud_login_worker is not None:
            return
        from utils.cloud_sync import load_saved_cloud_password

        dialog = QDialog(self)
        dialog.setWindowTitle(self.tr("Sporely Cloud login"))
        dialog.setModal(True)
        dialog.setMinimumWidth(420)

        saved_email, saved_password, can_store_password = load_saved_cloud_password()
        has_saved_password = bool(saved_password)
        password_edited = False
        submitted_password = None
        remember_login = False

        layout = QVBoxLayout(dialog)
        layout.addWidget(
            QLabel(
                self.tr("Sign in with your Sporely Cloud account.")
            )
        )

        form = QFormLayout()
        email_edit = QLineEdit()
        email_edit.setPlaceholderText(self.tr("Email"))
        email_edit.setText(saved_email or str(get_app_settings().get("cloud_user_email") or "").strip())
        password_edit = QLineEdit()
        password_edit.setPlaceholderText(self.tr("Password"))
        password_edit.setEchoMode(QLineEdit.Password)
        if has_saved_password:
            password_edit.setText("********")
        form.addRow(self.tr("Email:"), email_edit)
        form.addRow(self.tr("Password:"), password_edit)
        layout.addLayout(form)

        remember_checkbox = QCheckBox(self.tr("Save password on this device"))
        remember_checkbox.setChecked(bool(saved_email or has_saved_password))
        if not can_store_password:
            remember_checkbox.setChecked(False)
            remember_checkbox.setEnabled(False)
            remember_checkbox.setToolTip(self.tr("Install keyring to enable encrypted password storage."))
        layout.addWidget(remember_checkbox)
        if has_saved_password:
            layout.addWidget(QLabel(self.tr("Saved password loaded (shown masked).")))
        if not can_store_password:
            warning = QLabel(self.tr("Secure password storage unavailable: password will not be saved."))
            warning.setWordWrap(True)
            warning.setStyleSheet("color: #c05848;")
            layout.addWidget(warning)

        buttons = QDialogButtonBox(dialog)
        ok_btn = buttons.addButton(self.tr("Log in"), QDialogButtonBox.AcceptRole)
        cancel_btn = buttons.addButton(self.tr("Cancel"), QDialogButtonBox.RejectRole)
        layout.addWidget(buttons)

        def _on_password_edited(_text: str) -> None:
            nonlocal password_edited
            password_edited = True

        password_edit.textEdited.connect(_on_password_edited)

        def _accept_if_valid() -> None:
            nonlocal submitted_password, remember_login
            if not email_edit.text().strip() or not password_edit.text():
                QMessageBox.warning(
                    dialog,
                    self.tr("Missing Information"),
                    self.tr("Please enter your email and password."),
                )
                return
            if has_saved_password and not password_edited:
                submitted_password = saved_password
            else:
                submitted_password = password_edit.text()
            remember_login = bool(remember_checkbox.isChecked())
            if remember_login and not can_store_password:
                QMessageBox.warning(
                    dialog,
                    self.tr("Secure Storage Unavailable"),
                    self.tr("Password saving requires secure keyring support on this system."),
                )
                return
            dialog.accept()

        ok_btn.clicked.connect(_accept_if_valid)
        cancel_btn.clicked.connect(dialog.reject)
        email_edit.returnPressed.connect(_accept_if_valid)
        password_edit.returnPressed.connect(_accept_if_valid)

        if dialog.exec() != QDialog.Accepted:
            return

        self._cloud_login_email = email_edit.text().strip()
        self._cloud_login_password = submitted_password or password_edit.text()
        self._cloud_login_remember = bool(remember_login)
        self._cloud_login_worker = _CloudLoginWorker(
            self._cloud_login_email,
            self._cloud_login_password,
            parent=self,
        )
        self._cloud_login_worker.ok.connect(self._on_cloud_login_success)
        self._cloud_login_worker.fail.connect(self._on_cloud_login_failure)
        self._cloud_login_worker.finished.connect(self._on_cloud_login_finished)
        self._update_cloud_controls()
        self._cloud_login_worker.start()

    def _on_cloud_login_success(self, client, email: str) -> None:
        try:
            client.save_credentials(
                email=str(email or "").strip(),
                password=getattr(self, "_cloud_login_password", None),
                remember_password=bool(getattr(self, "_cloud_login_remember", False)),
            )
            update_app_settings({"cloud_user_email": str(email or "").strip()})
            self._cloud_client = client
            self.set_hint(self.tr("Logged in to Sporely Cloud"), tone="success")
            main_window = self.parent()
            if main_window is not None and hasattr(main_window, "observations_tab"):
                try:
                    main_window.observations_tab._start_cloud_sync(show_status=True, run_refresh_flow=True)
                except Exception:
                    pass
        except Exception as exc:
            QMessageBox.warning(
                self,
                self.tr("Login Failed"),
                self.tr("Unable to save cloud login.\n\n{error}").format(error=exc),
            )
            self._cloud_client = None

    def _on_cloud_login_failure(self, message: str) -> None:
        QMessageBox.warning(
            self,
            self.tr("Login Failed"),
            self.tr("Sporely Cloud login failed.\n\n{error}").format(error=message),
        )

    def _on_cloud_login_finished(self) -> None:
        self._cloud_login_worker = None
        self._cloud_login_email = ""
        self._cloud_login_password = ""
        self._cloud_login_remember = False
        self._update_cloud_controls()

    def _logout_cloud(self) -> None:
        try:
            from utils.cloud_sync import SporelyCloudClient

            SporelyCloudClient.clear_credentials()
            update_app_settings({"cloud_user_email": None})
        except Exception as exc:
            QMessageBox.warning(
                self,
                self.tr("Logout Failed"),
                self.tr("Unable to remove cloud login.\n\n{error}").format(error=exc),
            )
            return
        self._cloud_client = None
        self._update_cloud_controls()
        self._update_status()

    def _open_login(self):
        selected_uploader = self._selected_uploader()
        if not selected_uploader:
            QMessageBox.warning(
                self,
                self.tr("Login Unavailable"),
                self.tr("No upload targets are configured."),
            )
            return

        if selected_uploader.key == "inat":
            oauth = self._inat_oauth_client(require_credentials=True)
            if oauth is None:
                QMessageBox.warning(
                    self,
                    self.tr("Login Unavailable"),
                    self.tr("Missing iNaturalist Client ID."),
                )
                return
            try:
                oauth.authorize(open_browser=True, timeout=300)
                token = oauth.get_valid_access_token()
            except Exception as exc:
                QMessageBox.warning(
                    self,
                    self.tr("Login Failed"),
                    self.tr("iNaturalist login failed.\n\n{error}").format(error=exc),
                )
                return
            if token:
                self._on_login_success({"access_token": token}, target_key="inat", already_saved=True)
            return
        if selected_uploader.key == "mo":
            app_key, _user_key = self._mushroomobserver_credentials()
            if not self._prompt_mushroomobserver_login(require_app_key=not bool(app_key)):
                return
            app_key, user_key = self._mushroomobserver_credentials()
            if not app_key or not user_key:
                QMessageBox.warning(
                    self,
                    self.tr("Login Unavailable"),
                    self.tr("Missing Mushroom Observer app key or user API key."),
                )
                return
            self._on_login_success({"user_key": user_key}, target_key="mo", already_saved=True)
            return
        if selected_uploader.key == "artportalen":
            try:
                from utils.artportalen_auth import ArtportalenAuth
            except Exception as exc:
                QMessageBox.warning(
                    self,
                    self.tr("Login Unavailable"),
                    self.tr(f"Could not load the Artportalen login helper.\n\n{exc}"),
                )
                return
            try:
                cookies = ArtportalenAuth().login_with_gui(parent=self)
            except Exception as exc:
                QMessageBox.warning(
                    self,
                    self.tr("Login Failed"),
                    self.tr("Artportalen login failed.\n\n{error}").format(error=exc),
                )
                return
            if not cookies:
                return
            self._on_login_success(cookies, target_key="artportalen", already_saved=True)
            return

        try:
            from utils.artsobservasjoner_auto_login import ArtsObservasjonerAuth
        except Exception as exc:
            QMessageBox.warning(
                self,
                self.tr("Login Unavailable"),
                self.tr(f"Could not load the login module.\n\n{exc}")
            )
            return

        auth = ArtsObservasjonerAuth(cookies_file=self.cookies_file)

        if selected_uploader and selected_uploader.key in ("mobile", "web"):
            try:
                cookies = auth.login_web_with_gui(parent=self)
            except Exception as exc:
                QMessageBox.warning(
                    self,
                    self.tr("Login Failed"),
                    self.tr("Login failed.\n\n{error}").format(error=exc)
                )
                return
            if not cookies:
                return
            self._on_login_success(cookies, target_key="web", already_saved=True)
            return

    def _on_login_success(self, cookies: dict, target_key: str, already_saved: bool = False):
        if not already_saved:
            try:
                from utils.artsobservasjoner_auto_login import ArtsObservasjonerAuth

                auth = ArtsObservasjonerAuth(cookies_file=self.cookies_file)
                auth.save_cookies(cookies, target=target_key)
            except Exception as exc:
                QMessageBox.warning(
                    self,
                    self.tr("Login Failed"),
                    self.tr(f"Unable to save login cookies.\n\n{exc}")
                )
                return

        self._update_status()
        self._update_controls()
        # No modal dialog; status label updates in the settings dialog.

    def _logout(self):
        selected_uploader = self._selected_uploader()
        if selected_uploader and selected_uploader.key == "inat":
            try:
                oauth = self._inat_oauth_client(require_credentials=False)
                if oauth is not None:
                    oauth.clear_tokens()
                else:
                    token_file = self._inat_token_file()
                    if token_file.exists():
                        token_file.unlink()
            except Exception as exc:
                QMessageBox.warning(
                    self,
                    self.tr("Logout Failed"),
                    self.tr(f"Unable to remove login tokens.\n\n{exc}")
                )
                return
            self._update_status()
            self._update_controls()
            return
        if selected_uploader and selected_uploader.key == "mo":
            try:
                SettingsDB.set_setting(self.SETTING_MO_USER_API_KEY, "")
            except Exception as exc:
                QMessageBox.warning(
                    self,
                    self.tr("Logout Failed"),
                    self.tr(f"Unable to remove login credentials.\n\n{exc}")
                )
                return
            self._update_status()
            self._update_controls()
            return
        if selected_uploader and selected_uploader.key == "artportalen":
            try:
                from utils.artportalen_auth import ArtportalenAuth

                ArtportalenAuth().clear_cookies()
            except Exception as exc:
                QMessageBox.warning(
                    self,
                    self.tr("Logout Failed"),
                    self.tr(f"Unable to remove Artportalen session.\n\n{exc}")
                )
                return
            self._update_status()
            self._update_controls()
            return

        try:
            from utils.artsobservasjoner_auto_login import ArtsObservasjonerAuth

            auth = ArtsObservasjonerAuth(cookies_file=self.cookies_file)
            if selected_uploader and selected_uploader.key:
                target = "web" if selected_uploader.key in {"mobile", "web"} else selected_uploader.key
                auth.clear_cookies(target=target)
            else:
                return
        except Exception as exc:
            QMessageBox.warning(
                self,
                self.tr("Logout Failed"),
                self.tr(f"Unable to remove login cookies.\n\n{exc}")
            )
            return
        self._update_status()
        self._update_controls()

    def _load_settings(self):
        self._loading_settings = True
        try:
            selected_target = (SettingsDB.get_setting(self.SETTING_UPLOAD_TARGET, "") or "").strip().lower()
            if selected_target == "mobile":
                selected_target = "web"
            active_target = self.active_reporting_target()
            self.reporting_target_no_radio.blockSignals(True)
            self.reporting_target_se_radio.blockSignals(True)
            self.reporting_target_no_radio.setChecked(active_target == PUBLISH_TARGET_ARTSOBS_NO)
            self.reporting_target_se_radio.setChecked(active_target == PUBLISH_TARGET_ARTPORTALEN_SE)
            self.reporting_target_no_radio.blockSignals(False)
            self.reporting_target_se_radio.blockSignals(False)
            selected_row = -1
            if selected_target:
                for row in range(self.targets_table.rowCount()):
                    target_item = self.targets_table.item(row, 1)
                    target_key = str(target_item.data(Qt.UserRole) if target_item else "").strip().lower()
                    if target_key == selected_target:
                        selected_row = row
                        break
            enabled_keys = set(self._stored_enabled_upload_target_keys(self._uploaders))
            self.targets_table.blockSignals(True)
            for row in range(self.targets_table.rowCount()):
                use_item = self.targets_table.item(row, 0)
                key = str(use_item.data(Qt.UserRole) if use_item else "").strip().lower()
                if use_item is not None:
                    use_item.setCheckState(Qt.Checked if key in enabled_keys else Qt.Unchecked)
            self.targets_table.blockSignals(False)
            if selected_row >= 0:
                self.targets_table.selectRow(selected_row)
            elif self.targets_table.rowCount() > 0:
                self.targets_table.selectRow(0)

            scale_default = False
            parent_window = self.parent()
            if parent_window is not None:
                scale_toggle = getattr(parent_window, "show_scale_bar_checkbox", None)
                if scale_toggle is not None and hasattr(scale_toggle, "isChecked"):
                    scale_default = bool(scale_toggle.isChecked())

            check_states = (
                (
                    self.include_annotations_checkbox,
                    self.SETTING_INCLUDE_ANNOTATIONS,
                    False,
                ),
                (
                    self.show_scale_bar_checkbox,
                    self.SETTING_SHOW_SCALE_BAR,
                    scale_default,
                ),
                (
                    self.include_spore_stats_checkbox,
                    self.SETTING_INCLUDE_SPORE_STATS,
                    True,
                ),
                (
                    self.include_measure_plots_checkbox,
                    self.SETTING_INCLUDE_MEASURE_PLOTS,
                    False,
                ),
                (
                    self.include_thumbnail_gallery_checkbox,
                    self.SETTING_INCLUDE_THUMBNAIL_GALLERY,
                    False,
                ),
                (
                    self.include_plate_checkbox,
                    self.SETTING_INCLUDE_PLATE,
                    False,
                ),
                (
                    self.include_copyright_checkbox,
                    self.SETTING_INCLUDE_COPYRIGHT,
                    False,
                ),
                (
                    self.cloud_include_annotations_checkbox,
                    self.SETTING_CLOUD_INCLUDE_ANNOTATIONS,
                    False,
                ),
                (
                    self.cloud_show_scale_bar_checkbox,
                    self.SETTING_CLOUD_SHOW_SCALE_BAR,
                    False,
                ),
                (
                    self.cloud_include_measure_plots_checkbox,
                    self.SETTING_CLOUD_INCLUDE_MEASURE_PLOTS,
                    False,
                ),
                (
                    self.cloud_include_plate_checkbox,
                    self.SETTING_CLOUD_INCLUDE_PLATE,
                    False,
                ),
                (
                    self.cloud_include_copyright_checkbox,
                    self.SETTING_CLOUD_INCLUDE_COPYRIGHT,
                    False,
                ),
            )
            for checkbox, key, default in check_states:
                checkbox.blockSignals(True)
                checkbox.setChecked(self._setting_enabled(key, default=default))
                checkbox.blockSignals(False)

            image_license = str(
                SettingsDB.get_setting(self.SETTING_IMAGE_LICENSE, "60") or "60"
            ).strip()
            license_index = self.image_license_combo.findData(image_license)
            if license_index < 0:
                license_index = self.image_license_combo.findData("60")
            if license_index < 0 and self.image_license_combo.count():
                license_index = 0
            self.image_license_combo.blockSignals(True)
            if license_index >= 0:
                self.image_license_combo.setCurrentIndex(license_index)
            self.image_license_combo.blockSignals(False)
            self._update_image_license_hint_text()
            self._update_watermark_controls()
            self._set_cloud_sharing_scope(
                SettingsDB.get_setting(
                    self.SETTING_CLOUD_DEFAULT_SHARING_SCOPE,
                    "private",
                )
            )
            self._set_cloud_image_size_mode(
                SettingsDB.get_setting(
                    self.SETTING_CLOUD_IMAGE_SIZE_MODE,
                    "reduced",
                )
            )
            self._set_debug_cloud_plan_override(
                SettingsDB.get_setting(
                    self.SETTING_DEBUG_CLOUD_PLAN_OVERRIDE,
                    "server",
                )
            )
            self._update_cloud_debug_controls()

            if self.targets_table.rowCount() > 0:
                if not self.targets_table.selectionModel().hasSelection():
                    self.targets_table.selectRow(0)
        finally:
            self._loading_settings = False

    def closeEvent(self, event):
        try:
            self._save_settings()
        except Exception:
            pass
        super().closeEvent(event)


#
# Vernacular language helpers live in utils.vernacular_utils.
#


class VernacularDB:
    """Simple helper for vernacular name lookup."""

    def __init__(self, db_path: Path, language_code: str | None = None):
        self.db_path = db_path
        self.language_code = normalize_vernacular_language(language_code) if language_code else None
        self._has_language_column = None
        self._tables: set[str] | None = None

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _table_names(self) -> set[str]:
        if self._tables is None:
            with self._connect() as conn:
                cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                self._tables = {str(row[0] or "") for row in cur.fetchall()}
        return self._tables

    def _has_scientific_name_table(self) -> bool:
        return "scientific_name_min" in self._table_names()

    def _has_language(self) -> bool:
        if self._has_language_column is None:
            with self._connect() as conn:
                cur = conn.execute("PRAGMA table_info(vernacular_min)")
                self._has_language_column = any(row[1] == "language_code" for row in cur.fetchall())
        return bool(self._has_language_column)

    def _language_clause(self, language_code: str | None) -> tuple[str, list[str]]:
        if not self._has_language():
            return "", []
        raw = language_code or self.language_code
        if not raw:
            return "", []
        lang = normalize_vernacular_language(raw)
        if not lang:
            return "", []
        return " AND v.language_code = ? ", [lang]

    def list_languages(self) -> list[str]:
        if not self._has_language():
            return []
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT language_code
                FROM vernacular_min
                WHERE language_code IS NOT NULL AND language_code != ''
                ORDER BY language_code
                """
            )
            return [row[0] for row in cur.fetchall() if row and row[0]]

    def suggest_vernacular(self, prefix: str, genus: str | None = None, species: str | None = None) -> list[str]:
        prefix = prefix.strip()
        if not prefix:
            return []
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT v.vernacular_name
                FROM vernacular_min v
                JOIN taxon_min t ON t.taxon_id = v.taxon_id
                WHERE v.vernacular_name LIKE ? || '%'
                  AND (? IS NULL OR t.genus = ?)
                  AND (? IS NULL OR t.specific_epithet = ?)
                """
                + lang_clause
                + """
                ORDER BY v.vernacular_name
                LIMIT 200
                """,
                (prefix, genus, genus, species, species, *lang_params),
            )
            return [row[0] for row in cur.fetchall() if row and row[0]]

    def suggest_vernacular_for_taxon(
        self, genus: str | None = None, species: str | None = None, limit: int = 200
    ) -> list[str]:
        genus = genus.strip() if genus else None
        species = species.strip() if species else None
        if not genus and not species:
            return []
        resolved = self.taxon_from_scientific(genus or "", species or "") if genus and species else None
        if resolved:
            genus, species, _family = resolved
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT v.vernacular_name
                FROM vernacular_min v
                JOIN taxon_min t ON t.taxon_id = v.taxon_id
                WHERE (? IS NULL OR t.genus = ?)
                  AND (? IS NULL OR t.specific_epithet = ?)
                """
                + lang_clause
                + """
                ORDER BY v.is_preferred_name DESC, v.vernacular_name
                LIMIT ?
                """,
                (genus, genus, species, species, *lang_params, limit),
            )
            return [row[0] for row in cur.fetchall() if row and row[0]]

    def taxon_from_vernacular(self, name: str) -> tuple[str, str, str | None] | None:
        name = name.strip()
        if not name:
            return None
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT t.genus, t.specific_epithet, t.family
                FROM vernacular_min v
                JOIN taxon_min t ON t.taxon_id = v.taxon_id
                WHERE v.vernacular_name = ?
                """
                + lang_clause
                + """
                ORDER BY v.is_preferred_name DESC, v.vernacular_name
                LIMIT 1
                """,
                (name, *lang_params),
            )
            row = cur.fetchone()
            if not row:
                return None
            return row[0], row[1], row[2]

    def taxon_from_scientific(self, genus: str, species: str) -> tuple[str, str, str | None] | None:
        genus = (genus or "").strip()
        species = (species or "").strip()
        if not genus or not species:
            return None
        scientific_name = f"{genus} {species}".strip()
        with self._connect() as conn:
            cur = conn.cursor()
            if self._has_scientific_name_table():
                cur.execute(
                    """
                    SELECT t.genus, t.specific_epithet, t.family
                    FROM taxon_min t
                    LEFT JOIN scientific_name_min s ON s.taxon_id = t.taxon_id
                    WHERE (
                            t.genus = ? COLLATE NOCASE
                        AND t.specific_epithet = ? COLLATE NOCASE
                    )
                       OR (
                            t.canonical_scientific_name = ? COLLATE NOCASE
                    )
                       OR (
                            s.scientific_name = ? COLLATE NOCASE
                    )
                    ORDER BY
                        CASE
                            WHEN t.genus = ? COLLATE NOCASE AND t.specific_epithet = ? COLLATE NOCASE THEN 0
                            WHEN s.is_preferred_name = 1 THEN 1
                            ELSE 2
                        END,
                        t.genus,
                        t.specific_epithet
                    LIMIT 1
                    """,
                    (genus, species, scientific_name, scientific_name, genus, species),
                )
            else:
                cur.execute(
                    """
                    SELECT genus, specific_epithet, family
                    FROM taxon_min
                    WHERE genus = ? COLLATE NOCASE
                      AND specific_epithet = ? COLLATE NOCASE
                    ORDER BY genus, specific_epithet
                    LIMIT 1
                    """,
                    (genus, species),
                )
            row = cur.fetchone()
            if not row:
                return None
            return row[0], row[1], row[2]

    def vernacular_from_taxon(self, genus: str, species: str) -> str | None:
        if not genus or not species:
            return None
        resolved = self.taxon_from_scientific(genus, species)
        if resolved:
            genus, species, _family = resolved
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT v.vernacular_name
                FROM vernacular_min v
                JOIN taxon_min t ON t.taxon_id = v.taxon_id
                WHERE t.genus = ? COLLATE NOCASE
                  AND t.specific_epithet = ? COLLATE NOCASE
                """
                + lang_clause
                + """
                ORDER BY v.is_preferred_name DESC, v.vernacular_name
                LIMIT 1
                """,
                (genus, species, *lang_params),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def suggest_genus(self, prefix: str) -> list[str]:
        prefix = prefix.strip()
        if not prefix:
            return []
        with self._connect() as conn:
            cur = conn.cursor()
            values: list[str] = []
            seen: set[str] = set()
            cur.execute(
                """
                SELECT DISTINCT genus
                FROM taxon_min
                WHERE genus LIKE ? || '%'
                ORDER BY genus
                LIMIT 200
                """,
                (prefix,),
            )
            for row in cur.fetchall():
                genus = str(row[0] or "").strip()
                lowered = genus.casefold()
                if genus and lowered not in seen:
                    seen.add(lowered)
                    values.append(genus)
            if self._has_scientific_name_table():
                cur.execute(
                    """
                    SELECT DISTINCT scientific_name
                    FROM scientific_name_min
                    WHERE scientific_name LIKE ? || ' %'
                    ORDER BY scientific_name
                    LIMIT 400
                    """,
                    (prefix,),
                )
                for row in cur.fetchall():
                    scientific_name = str(row[0] or "").strip()
                    genus = scientific_name.split(" ", 1)[0].strip() if scientific_name else ""
                    lowered = genus.casefold()
                    if genus and lowered not in seen:
                        seen.add(lowered)
                        values.append(genus)
            return values[:200]

    def suggest_species(self, genus: str, prefix: str) -> list[str]:
        genus = genus.strip()
        prefix = prefix.strip()
        if not genus:
            return []
        with self._connect() as conn:
            cur = conn.cursor()
            values: list[str] = []
            seen: set[str] = set()
            cur.execute(
                """
                SELECT DISTINCT specific_epithet
                FROM taxon_min
                WHERE genus = ? COLLATE NOCASE
                  AND specific_epithet LIKE ? || '%'
                ORDER BY specific_epithet
                LIMIT 200
                """,
                (genus, prefix),
            )
            for row in cur.fetchall():
                species = str(row[0] or "").strip()
                lowered = species.casefold()
                if species and lowered not in seen:
                    seen.add(lowered)
                    values.append(species)
            if self._has_scientific_name_table():
                cur.execute(
                    """
                    SELECT DISTINCT scientific_name
                    FROM scientific_name_min
                    WHERE scientific_name LIKE ? || ' ' || ? || '%'
                    ORDER BY scientific_name
                    LIMIT 400
                    """,
                    (genus, prefix),
                )
                for row in cur.fetchall():
                    scientific_name = str(row[0] or "").strip()
                    parts = scientific_name.split()
                    if len(parts) < 2 or parts[0].casefold() != genus.casefold():
                        continue
                    species = parts[1].strip()
                    lowered = species.casefold()
                    if species and lowered not in seen:
                        seen.add(lowered)
                        values.append(species)
            return values[:200]

class ReferenceValuesDialog(QDialog):
    """Dialog for editing reference spore size values."""

    plot_requested = Signal(dict)
    save_requested = Signal(dict)

    def __init__(self, genus, species, ref_values=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Reference Values"))
        self.setModal(True)
        self.setMinimumSize(440, 280)
        self.genus = genus or ""
        self.species = species or ""
        self.ref_values = ref_values or {}
        self._suppress_taxon_autofill = False
        self._last_genus = ""
        self._last_species = ""

        layout = QVBoxLayout(self)
        form = QFormLayout()
        self.vernacular_input = QLineEdit()
        self.genus_input = QLineEdit()
        self.species_input = QLineEdit()
        self.source_input = QComboBox()
        self.source_input.setEditable(True)
        self.source_input.setInsertPolicy(QComboBox.NoInsert)
        self.mount_input = QLineEdit(self.ref_values.get("mount_medium") or "")
        self.stain_input = QLineEdit(self.ref_values.get("stain") or "")
        self.vernacular_label = QLabel(self._vernacular_label())
        form.addRow(self.vernacular_label, self.vernacular_input)
        form.addRow(self.tr("Genus:"), self.genus_input)
        form.addRow(self.tr("Species:"), self.species_input)
        form.addRow(self.tr("Source:"), self.source_input)
        form.addRow(self.tr("Mount medium:"), self.mount_input)
        form.addRow(self.tr("Stain:"), self.stain_input)
        layout.addLayout(form)

        self._genus_model = QStringListModel()
        self._species_model = QStringListModel()
        self._genus_completer = QCompleter(self._genus_model, self)
        self._genus_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._genus_completer.setCompletionMode(QCompleter.PopupCompletion)
        self._species_completer = QCompleter(self._species_model, self)
        self._species_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._species_completer.setCompletionMode(QCompleter.PopupCompletion)
        self.genus_input.setCompleter(self._genus_completer)
        self.species_input.setCompleter(self._species_completer)
        self._genus_completer.activated.connect(self._on_genus_selected)
        self._species_completer.activated.connect(self._on_species_selected)
        self.genus_input.textChanged.connect(self._on_genus_text_changed)
        self.species_input.textChanged.connect(self._on_species_text_changed)
        self.genus_input.editingFinished.connect(self._on_genus_editing_finished)
        self.species_input.editingFinished.connect(self._on_species_editing_finished)
        self.genus_input.installEventFilter(self)
        self.species_input.installEventFilter(self)

        self.vernacular_db = None
        lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
        db_path = resolve_vernacular_db_path(lang)
        if db_path:
            self.vernacular_db = VernacularDB(db_path, language_code=lang)
        self._vernacular_model = QStringListModel()
        self._vernacular_completer = QCompleter(self._vernacular_model, self)
        self._vernacular_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._vernacular_completer.setCompletionMode(QCompleter.PopupCompletion)
        self.vernacular_input.setCompleter(self._vernacular_completer)
        self._vernacular_completer.activated.connect(self._on_vernacular_selected)
        self.vernacular_input.textChanged.connect(self._on_vernacular_text_changed)
        self.vernacular_input.editingFinished.connect(self._on_vernacular_editing_finished)
        self.vernacular_input.installEventFilter(self)

        self.table = QTableWidget(3, 5)
        self.table.setHorizontalHeaderLabels(
            [self.tr("Min"), self.tr("5%"), self.tr("50%"), self.tr("95%"), self.tr("Max")]
        )
        self.table.setVerticalHeaderLabels([self.tr("Length"), self.tr("Width"), self.tr("Q")])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table.verticalHeader().setDefaultSectionSize(28)
        self.table.setMinimumHeight(120)
        layout.addWidget(self.table)

        info_row = QHBoxLayout()
        info_row.setContentsMargins(0, 0, 0, 0)
        info_row.setSpacing(8)
        info_label = QLabel(
            self.tr(
                "Percentiles assume an approximately normal distribution. "
                "The 50% column represents the median (middle value)."
            )
        )
        info_label.setStyleSheet(f"color: #7f8c8d; font-size: {pt(9)}pt;")
        info_label.setWordWrap(True)
        info_row.addWidget(info_label, 1)
        info_row.addWidget(make_github_help_button(self, "reference-data-dialog.md"), 0, Qt.AlignRight | Qt.AlignVCenter)
        layout.addLayout(info_row)

        def _set_cell(row, col, value):
            if value is None:
                return
            item = QTableWidgetItem(f"{value:g}")
            self.table.setItem(row, col, item)

        _set_cell(0, 0, self.ref_values.get("length_min"))
        _set_cell(0, 1, self.ref_values.get("length_p05"))
        _set_cell(0, 2, self.ref_values.get("length_p50"))
        _set_cell(0, 3, self.ref_values.get("length_p95"))
        _set_cell(0, 4, self.ref_values.get("length_max"))
        _set_cell(1, 0, self.ref_values.get("width_min"))
        _set_cell(1, 1, self.ref_values.get("width_p05"))
        _set_cell(1, 2, self.ref_values.get("width_p50"))
        _set_cell(1, 3, self.ref_values.get("width_p95"))
        _set_cell(1, 4, self.ref_values.get("width_max"))
        _set_cell(2, 0, self.ref_values.get("q_min"))
        _set_cell(2, 2, self.ref_values.get("q_p50"))
        _set_cell(2, 4, self.ref_values.get("q_max"))

        btn_row = QHBoxLayout()
        plot_btn = QPushButton(self.tr("Plot"))
        plot_btn.clicked.connect(self._on_plot_clicked)
        save_btn = QPushButton(self.tr("Save"))
        save_btn.clicked.connect(self._on_save_clicked)
        clear_btn = QPushButton(self.tr("Clear"))
        clear_btn.clicked.connect(self._on_clear_clicked)
        cancel_btn = QPushButton(self.tr("Cancel"))
        cancel_btn.clicked.connect(self.reject)
        btn_row.addStretch()
        btn_row.addWidget(plot_btn)
        btn_row.addWidget(save_btn)
        btn_row.addWidget(clear_btn)
        btn_row.addWidget(cancel_btn)
        layout.addLayout(btn_row)

        self._populate_genus(self.genus)
        self._populate_species(self.genus, self.species)
        self._populate_sources(self.genus, self.species, self.ref_values.get("source"))
        self._maybe_set_vernacular_from_taxon()
        self._maybe_load_reference()
        self._sync_taxon_cache()

        self.genus_input.textChanged.connect(self._on_genus_changed)
        self.species_input.textChanged.connect(self._on_species_changed)
        self.source_input.currentTextChanged.connect(self._on_source_changed)
        self.mount_input.textChanged.connect(self._on_mount_changed)
        self.stain_input.textChanged.connect(self._on_stain_changed)

    def _vernacular_label(self) -> str:
        lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
        base = self.tr("Common name")
        return f"{common_name_display_label(lang, base)}:"

    def apply_vernacular_language_change(self) -> None:
        if hasattr(self, "vernacular_label"):
            self.vernacular_label.setText(self._vernacular_label())
        lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
        db_path = resolve_vernacular_db_path(lang)
        if not db_path:
            return
        if self.vernacular_db and self.vernacular_db.db_path == db_path:
            self.vernacular_db.language_code = lang
        else:
            self.vernacular_db = VernacularDB(db_path, language_code=lang)
        self._maybe_set_vernacular_from_taxon()

    def _cell_value(self, row, col):
        item = self.table.item(row, col)
        if not item:
            return None
        try:
            return float(item.text().strip())
        except ValueError:
            return None


    def get_data(self):
        return {
            "genus": self.genus_input.text().strip() or None,
            "species": self.species_input.text().strip() or None,
            "source": self.source_input.currentText().strip() or None,
            "mount_medium": self.mount_input.text().strip() or None,
            "stain": self.stain_input.text().strip() or None,
            "length_min": self._cell_value(0, 0),
            "length_p05": self._cell_value(0, 1),
            "length_p50": self._cell_value(0, 2),
            "length_p95": self._cell_value(0, 3),
            "length_max": self._cell_value(0, 4),
            "width_min": self._cell_value(1, 0),
            "width_p05": self._cell_value(1, 1),
            "width_p50": self._cell_value(1, 2),
            "width_p95": self._cell_value(1, 3),
            "width_max": self._cell_value(1, 4),
            "q_min": self._cell_value(2, 0),
            "q_p50": self._cell_value(2, 2),
            "q_max": self._cell_value(2, 4),
        }

    def _has_species(self):
        data = self.get_data()
        return bool(data.get("genus") and data.get("species"))

    def _on_plot_clicked(self):
        data = self.get_data()
        self.plot_requested.emit(data)
        self.accept()

    def _on_save_clicked(self):
        data = self.get_data()
        if not (data.get("genus") and data.get("species")):
            QMessageBox.warning(
                self,
                self.tr("Missing Species"),
                self.tr("Please enter genus and species to save.")
            )
            return
        self.save_requested.emit(data)

    def _on_clear_clicked(self):
        self.vernacular_input.setText("")
        self.genus_input.setText("")
        self.species_input.setText("")
        self.source_input.setCurrentText("")
        self.mount_input.setText("")
        self.stain_input.setText("")
        self._clear_table()
        self.plot_requested.emit({})

    def _populate_combo(self, combo, values, current):
        combo.blockSignals(True)
        combo.clear()
        combo.addItem("")
        for value in values:
            combo.addItem(value)
        if current:
            idx = combo.findText(current)
            if idx >= 0:
                combo.setCurrentIndex(idx)
            else:
                combo.setCurrentText(current)
        else:
            combo.setCurrentIndex(-1)
            combo.setEditText("")
        combo.blockSignals(False)

    def _populate_genus(self, current):
        self.genus_input.blockSignals(True)
        self.genus_input.setText(current or "")
        self.genus_input.blockSignals(False)
        self._update_genus_suggestions(current or "")

    def _populate_species(self, genus, current):
        self.species_input.blockSignals(True)
        self.species_input.setText(current or "")
        self.species_input.blockSignals(False)
        self._update_species_suggestions(genus or "", current or "")

    def _populate_sources(self, genus, species, current):
        values = ReferenceDB.list_sources(genus or "", species or "", current or "")
        self._populate_combo(self.source_input, values, current)

    def _clear_table(self):
        self.table.clearContents()

    def _on_genus_text_changed(self, text):
        if self._suppress_taxon_autofill:
            return
        self._update_genus_suggestions(text or "")
        if not text.strip():
            self._suppress_taxon_autofill = True
            self.species_input.setText("")
            self._suppress_taxon_autofill = False
            self._species_model.setStringList([])
            # Reset species completer filtering
            if self._species_completer:
                self._species_completer.setCompletionPrefix("")
        else:
            # Reset species completer filtering when genus changes
            if self._species_completer and not self.species_input.hasFocus():
                self._species_completer.setCompletionPrefix("")

    def _on_species_text_changed(self, text):
        if self._suppress_taxon_autofill:
            return
        genus = self.genus_input.text().strip()
        if not genus:
            self._species_model.setStringList([])
            return
        self._update_species_suggestions(genus, text or "")
        if self._species_model.stringList() and self._species_completer:
            self._species_completer.setCompletionPrefix((text or "").strip())
            self._species_completer.complete()
        if text.strip():
            self._maybe_set_vernacular_from_taxon()

    def eventFilter(self, obj, event):
        if event.type() == QEvent.FocusIn:
            if obj == self.vernacular_input:
                if not self.vernacular_input.text().strip():
                    # Reset completer filtering when focusing empty field
                    if self._vernacular_completer:
                        self._vernacular_completer.setCompletionPrefix("")
                    self._update_vernacular_suggestions_for_taxon()
                    if self._vernacular_model.stringList():
                        self._vernacular_completer.complete()
            elif obj == self.genus_input:
                text = self.genus_input.text().strip()
                self._update_genus_suggestions(text)
                if self._genus_model.stringList():
                    self._genus_completer.complete()
            elif obj == self.species_input:
                genus = self.genus_input.text().strip()
                if genus:
                    text = self.species_input.text().strip()
                    self._update_species_suggestions(genus, text)
                    if self._species_model.stringList():
                        self._species_completer.complete()
        return super().eventFilter(obj, event)

    def _load_reference(self, genus, species, source, mount_medium=None, stain=None):
        ref = ReferenceDB.get_reference(genus, species, source, mount_medium, stain)
        if not ref:
            return
        self._clear_table()
        self.mount_input.setText(ref.get("mount_medium") or "")
        self.stain_input.setText(ref.get("stain") or "")

        def _set_cell(row, col, value):
            if value is None:
                return
            item = QTableWidgetItem(f"{value:g}")
            self.table.setItem(row, col, item)

        _set_cell(0, 0, ref.get("length_min"))
        _set_cell(0, 1, ref.get("length_p05"))
        _set_cell(0, 2, ref.get("length_p50"))
        _set_cell(0, 3, ref.get("length_p95"))
        _set_cell(0, 4, ref.get("length_max"))
        _set_cell(1, 0, ref.get("width_min"))
        _set_cell(1, 1, ref.get("width_p05"))
        _set_cell(1, 2, ref.get("width_p50"))
        _set_cell(1, 3, ref.get("width_p95"))
        _set_cell(1, 4, ref.get("width_max"))
        _set_cell(2, 0, ref.get("q_min"))
        _set_cell(2, 2, ref.get("q_p50"))
        _set_cell(2, 4, ref.get("q_max"))

    def _maybe_load_reference(self):
        genus = self.genus_input.text().strip()
        species = self.species_input.text().strip()
        source = self.source_input.currentText().strip()
        mount = self.mount_input.text().strip()
        stain = self.stain_input.text().strip()

        if not (genus and species):
            return

        if source:
            if mount or stain:
                ref = ReferenceDB.get_reference(genus, species, source, mount or None, stain or None)
                if ref:
                    self._load_reference(genus, species, source, mount or None, stain or None)
                    return
            mounts = ReferenceDB.list_mount_mediums(genus, species, source)
            stains = ReferenceDB.list_stains(genus, species, source)
            if len(mounts) == 1 and not mount:
                self.mount_input.setText(mounts[0] or "")
                mount = mounts[0] if mounts else ""
            if len(stains) == 1 and not stain:
                self.stain_input.setText(stains[0] or "")
                stain = stains[0] if stains else ""
            if (mount or stain) and (len(mounts) == 1 or len(stains) == 1):
                self._load_reference(genus, species, source, mount or None, stain or None)
                return
            ref = ReferenceDB.get_reference(genus, species, source)
            if ref:
                self._load_reference(genus, species, source)

    def _on_genus_changed(self, text):
        genus = text.strip()
        species = self.species_input.text().strip()
        self._populate_sources(genus, species, self.source_input.currentText().strip())
        self._clear_table()

    def _on_species_changed(self, text):
        genus = self.genus_input.text().strip()
        species = text.strip()
        sources = ReferenceDB.list_sources(genus, species, "")
        self._populate_sources(genus, species, self.source_input.currentText().strip())
        if len(sources) == 1:
            self.source_input.setCurrentText(sources[0])
        self._maybe_load_reference()

    def _on_source_changed(self, text):
        genus = self.genus_input.text().strip()
        species = self.species_input.text().strip()
        source = text.strip()
        if genus and species and source:
            self._maybe_load_reference()

    def _on_mount_changed(self, text):
        self._maybe_load_reference()

    def _on_stain_changed(self, text):
        self._maybe_load_reference()

    def _on_vernacular_text_changed(self, text):
        if not self.vernacular_db:
            return
        if self._suppress_taxon_autofill:
            return
        if not text.strip():
            self._update_vernacular_suggestions_for_taxon()
            return
        genus = self.genus_input.text().strip() or None
        species = self.species_input.text().strip() or None
        suggestions = self.vernacular_db.suggest_vernacular(text, genus=genus, species=species)
        
        # If text exactly matches any suggestion, clear the model to prevent popup
        text_lower = text.strip().lower()
        if any(s.lower() == text_lower for s in suggestions):
            self._vernacular_model.setStringList([])
            if self._vernacular_completer:
                self._vernacular_completer.popup().hide()
        else:
            self._vernacular_model.setStringList(suggestions)

    def _update_vernacular_suggestions_for_taxon(self):
        if not self.vernacular_db:
            return
        genus = self.genus_input.text().strip() or None
        species = self.species_input.text().strip() or None
        if not genus and not species:
            self._vernacular_model.setStringList([])
            self._set_vernacular_placeholder_from_suggestions([])
            return
        suggestions = self.vernacular_db.suggest_vernacular_for_taxon(genus=genus, species=species)
        self._vernacular_model.setStringList(suggestions)
        self._set_vernacular_placeholder_from_suggestions(suggestions)

    def _set_vernacular_placeholder_from_suggestions(self, suggestions: list[str]) -> None:
        if not hasattr(self, "vernacular_input"):
            return
        if not suggestions:
            self.vernacular_input.setPlaceholderText("")
            return
        preview = "; ".join(suggestions[:4])
        self.vernacular_input.setPlaceholderText(f"{self.tr('e.g.,')} {preview}")

    def _on_vernacular_selected(self, name):
        # Hide the popup after selection
        if self._vernacular_completer:
            self._vernacular_completer.popup().hide()
        
        if not self.vernacular_db:
            return
        taxon = self.vernacular_db.taxon_from_vernacular(name)
        if taxon:
            genus, species, _family = taxon
            self._suppress_taxon_autofill = True
            self.genus_input.setText(genus)
            self.species_input.setText(species)
            self._suppress_taxon_autofill = False
            self._sync_taxon_cache()

    def _on_vernacular_editing_finished(self):
        if not self.vernacular_db:
            return
        name = self.vernacular_input.text().strip()
        if not name:
            return
        taxon = self.vernacular_db.taxon_from_vernacular(name)
        if taxon:
            genus, species, _family = taxon
            self._suppress_taxon_autofill = True
            self.genus_input.setText(genus)
            self.species_input.setText(species)
            self._suppress_taxon_autofill = False
            self._sync_taxon_cache()

    def _on_genus_selected(self, genus):
        """Handle genus selection from completer."""
        # Hide the popup after selection
        if self._genus_completer:
            self._genus_completer.popup().hide()

    def _on_species_selected(self, species):
        """Handle species selection from completer."""
        # Hide the popup after selection
        if self._species_completer:
            self._species_completer.popup().hide()
        
        # Update vernacular name suggestions
        if self.vernacular_db:
            self._maybe_set_vernacular_from_taxon()

    def _on_genus_editing_finished(self):
        if not self.vernacular_db or self._suppress_taxon_autofill:
            return
        self._handle_taxon_change()
        self._maybe_set_vernacular_from_taxon()

    def _on_species_editing_finished(self):
        if not self.vernacular_db or self._suppress_taxon_autofill:
            return
        self._handle_taxon_change()
        self._maybe_set_vernacular_from_taxon()

    def _handle_taxon_change(self):
        if not hasattr(self, "_last_genus"):
            self._sync_taxon_cache()
            return
        genus = self.genus_input.text().strip()
        species = self.species_input.text().strip()
        if genus != self._last_genus or species != self._last_species:
            if genus and species and self.vernacular_input.text().strip():
                self._suppress_taxon_autofill = True
                self.vernacular_input.setText("")
                self._suppress_taxon_autofill = False
                # Reset vernacular completer filtering after clearing
                if self._vernacular_completer:
                    self._vernacular_completer.setCompletionPrefix("")
        self._last_genus = genus
        self._last_species = species

    def _sync_taxon_cache(self):
        self._last_genus = self.genus_input.text().strip()
        self._last_species = self.species_input.text().strip()

    def _maybe_set_vernacular_from_taxon(self):
        if not self.vernacular_db:
            return
        if self.vernacular_input.text().strip():
            return
        genus = self.genus_input.text().strip()
        species = self.species_input.text().strip()
        if not genus or not species:
            return
        suggestions = self.vernacular_db.suggest_vernacular_for_taxon(genus=genus, species=species)
        if not suggestions:
            self._set_vernacular_placeholder_from_suggestions([])
            return
        if len(suggestions) == 1:
            self._suppress_taxon_autofill = True
            self.vernacular_input.setText(suggestions[0])
            self._suppress_taxon_autofill = False
            self._set_vernacular_placeholder_from_suggestions([])
        else:
            self._set_vernacular_placeholder_from_suggestions(suggestions)

    def _update_genus_suggestions(self, text):
        if self.vernacular_db:
            values = self.vernacular_db.suggest_genus(text)
        else:
            values = ReferenceDB.list_genera(text or "")
        
        # If text exactly matches a single suggestion, clear the model to prevent popup
        text_stripped = text.strip()
        if len(values) == 1 and values[0].lower() == text_stripped.lower():
            self._genus_model.setStringList([])
            if self._genus_completer:
                self._genus_completer.popup().hide()
        else:
            self._genus_model.setStringList(values)

    def _update_species_suggestions(self, genus, text):
        if self.vernacular_db:
            values = self.vernacular_db.suggest_species(genus, text)
        else:
            values = ReferenceDB.list_species(genus or "", text or "")
        
        # Hide popup when text exactly matches any suggestion (covers multi-result cases)
        text_stripped = (text or "").strip()
        if text_stripped and any(v.lower() == text_stripped.lower() for v in values):
            self._species_model.setStringList([])
            if self._species_completer:
                self._species_completer.popup().hide()
        else:
            self._species_model.setStringList(values)

class SporeDataTable(QTableWidget):
    """Editable table for spore data with auto-added rows and Q calculation."""

    def __init__(self, parent=None):
        super().__init__(0, 3, parent)
        self.setHorizontalHeaderLabels([self.tr("Length (\u03bcm)"), self.tr("Width (\u03bcm)"), "Q"])
        header = self.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        header.setSectionResizeMode(1, QHeaderView.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.verticalHeader().setVisible(False)
        self.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.setEditTriggers(QAbstractItemView.AllEditTriggers)
        self.setStyleSheet("QTableWidget QLineEdit { padding: 0px; }")
        self._updating_q = False
        self.itemChanged.connect(self._on_item_changed)
        self._ensure_rows(1)

    def _ensure_rows(self, count):
        while self.rowCount() < count:
            row = self.rowCount()
            self.insertRow(row)
            q_item = QTableWidgetItem("")
            q_item.setFlags(q_item.flags() & ~Qt.ItemIsEditable)
            self.setItem(row, 2, q_item)

    def _on_item_changed(self, item):
        if self._updating_q:
            return
        if item.column() not in (0, 1):
            return
        self._update_q_for_row(item.row())

    def _update_q_for_row(self, row):
        if row < 0 or row >= self.rowCount():
            return
        length = self._cell_float(row, 0)
        width = self._cell_float(row, 1)
        q_value = None
        if length is not None and width is not None and width > 0:
            q_value = length / width
        self._updating_q = True
        try:
            if self.item(row, 2) is None:
                q_item = QTableWidgetItem("")
                q_item.setFlags(q_item.flags() & ~Qt.ItemIsEditable)
                self.setItem(row, 2, q_item)
            self.item(row, 2).setText(f"{q_value:.2f}" if q_value is not None else "")
        finally:
            self._updating_q = False

    def _cell_float(self, row, col):
        item = self.item(row, col)
        if not item:
            return None
        try:
            return float(item.text().strip())
        except ValueError:
            return None

    def keyPressEvent(self, event):
        if event.matches(QKeySequence.Paste):
            self._paste_from_clipboard()
            return
        if event.key() in (Qt.Key_Return, Qt.Key_Enter):
            current = self.currentIndex()
            if current.isValid() and current.row() == self.rowCount() - 1:
                self._ensure_rows(self.rowCount() + 1)
                self.setCurrentCell(self.rowCount() - 1, current.column())
                return
        text = event.text()
        if text and not text.isspace():
            item = self.currentItem()
            if item and (item.flags() & Qt.ItemIsEditable):
                if self.state() != QAbstractItemView.EditingState:
                    self.editItem(item)
        super().keyPressEvent(event)

    def _paste_from_clipboard(self):
        text = QApplication.clipboard().text()
        if not text:
            return
        rows = [line for line in text.splitlines() if line.strip()]
        if not rows:
            return
        start_row = max(0, self.currentRow())
        start_col = max(0, self.currentColumn())
        needed_rows = start_row + len(rows)
        self._ensure_rows(needed_rows)
        for r_index, line in enumerate(rows):
            cols = [c.strip() for c in re.split(r"[\t,;]", line) if c.strip()]
            for c_index, value in enumerate(cols[:2]):
                row = start_row + r_index
                col = start_col + c_index
                if col > 1:
                    break
                item = self.item(row, col)
                if item is None:
                    item = QTableWidgetItem()
                    self.setItem(row, col, item)
                item.setText(value)
            self._update_q_for_row(start_row + r_index)

    def get_points(self) -> list[dict]:
        points = []
        for row in range(self.rowCount()):
            length = self._cell_float(row, 0)
            width = self._cell_float(row, 1)
            if length is None or width is None or width <= 0:
                continue
            points.append({"length_um": float(length), "width_um": float(width)})
        return points


class ReferenceAddDialog(QDialog):
    """Dialog for adding reference min/max or spore data."""

    def __init__(
        self,
        parent,
        genus: str,
        species: str,
        vernacular: str | None = None,
        data: dict | None = None,
        title: str | None = None,
        allow_delete: bool = False,
    ):
        super().__init__(parent)
        self.setWindowTitle(title or parent.tr("Add reference data"))
        self.setModal(True)
        # Start compact (roughly half the old width), but keep the dialog resizable.
        self.setMinimumSize(420, 420)
        self.resize(440, 520)
        self._result = None
        self._genus = genus
        self._species = species
        self._prefill_data = data or {}
        self._hint_controller: HintStatusController | None = None
        self._plot_color = None
        self._allow_delete = bool(allow_delete)
        self._delete_requested = False
        self._default_hint_text = self.tr("Paste from Excel/csv or type values")
        self._minmax_header_hints = {
            0: self.tr("Min: Minimum value in the data set"),
            1: self.tr("5%: 5th percentile"),
            2: self.tr("50%: Median (50th percentile)"),
            3: self.tr("95%: 95th percentile"),
            4: self.tr("Max: Maximum value in the data set"),
        }

        layout = QVBoxLayout(self)
        sci_label = QLabel(f"{genus} {species}".strip())
        sci_label.setStyleSheet("font-weight: bold;")
        layout.addWidget(sci_label)
        if vernacular:
            vern_label = QLabel(vernacular)
            vern_label.setStyleSheet("color: #7f8c8d;")
            layout.addWidget(vern_label)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        minmax_tab = QWidget()
        minmax_layout = QVBoxLayout(minmax_tab)
        self.minmax_table = QTableWidget(3, 5)
        self.minmax_table.setHorizontalHeaderLabels(
            [self.tr("Min"), self.tr("5%"), self.tr("50%"), self.tr("95%"), self.tr("Max")]
        )
        self.minmax_table.setVerticalHeaderLabels([self.tr("Length"), self.tr("Width"), self.tr("Q")])
        minmax_header = self.minmax_table.horizontalHeader()
        minmax_header.setSectionResizeMode(QHeaderView.Stretch)
        minmax_header.setMouseTracking(True)
        minmax_header.sectionEntered.connect(self._on_minmax_header_entered)
        self._minmax_header_viewport = minmax_header.viewport()
        self._minmax_header_viewport.setMouseTracking(True)
        self._minmax_header_viewport.installEventFilter(self)
        self.minmax_table.verticalHeader().setDefaultSectionSize(30)
        self.minmax_table.setStyleSheet("QTableWidget QLineEdit { padding: 0px; }")
        self._formatting_minmax = False
        self.minmax_table.itemChanged.connect(self._on_minmax_item_changed)
        minmax_layout.addWidget(self.minmax_table)
        self.tabs.addTab(minmax_tab, self.tr("Min/max"))

        spore_tab = QWidget()
        spore_layout = QVBoxLayout(spore_tab)
        self.spore_table = SporeDataTable()
        spore_layout.addWidget(self.spore_table)
        self.tabs.addTab(spore_tab, self.tr("Spore data"))

        parmasto_tab = QWidget()
        parmasto_layout = QFormLayout(parmasto_tab)
        parmasto_layout.setContentsMargins(8, 8, 8, 8)
        parmasto_layout.setSpacing(6)
        parmasto_layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)
        self.parmasto_inputs: dict[str, QLineEdit] = {}
        def _math_label(html: str, description: str) -> QLabel:
            label = QLabel(f"{html}:")
            label.setTextFormat(Qt.RichText)
            label.setToolTip(description)
            return label
        parmasto_fields = [
            ("parmasto_length_mean", "<span style=\"text-decoration: overline;\">L</span>", self.tr("Species mean length")),
            ("parmasto_width_mean", "<span style=\"text-decoration: overline;\">W</span>", self.tr("Species mean width")),
            ("parmasto_q_mean", "<span style=\"text-decoration: overline;\">Q</span>", self.tr("Species mean quotient")),
            ("parmasto_v_sp_length", "V<sub>spL</sub>", self.tr("Inter-specimen CV for length means (%)")),
            ("parmasto_v_sp_width", "V<sub>spW</sub>", self.tr("Inter-specimen CV for width means (%)")),
            ("parmasto_v_sp_q", "V<sub>spQ</sub>", self.tr("Inter-specimen CV for quotient means (%)")),
            ("parmasto_v_ind_length", "<span style=\"text-decoration: overline;\">V</span><sub>indL</sub>", self.tr("Average intra-specimen variation for length (%)")),
            ("parmasto_v_ind_width", "<span style=\"text-decoration: overline;\">V</span><sub>indW</sub>", self.tr("Average intra-specimen variation for width (%)")),
            ("parmasto_v_ind_q", "<span style=\"text-decoration: overline;\">V</span><sub>indE</sub>", self.tr("Average intra-specimen variation for quotient (Parmasto VindE) (%)")),
        ]
        for key, math_label, description in parmasto_fields:
            line_edit = QLineEdit()
            line_edit.setPlaceholderText(description)
            line_edit.setToolTip(description)
            parmasto_layout.addRow(_math_label(math_label, description), line_edit)
            self.parmasto_inputs[key] = line_edit
        self.tabs.addTab(parmasto_tab, self.tr("Parmasto Biometrics"))

        source_row = QFormLayout()
        self.source_input = QLineEdit()
        source_prefill = (
            self._prefill_data.get("source")
            or self._prefill_data.get("points_label")
            or self._prefill_data.get("source_label")
            or ""
        )
        if source_prefill:
            self.source_input.setText(source_prefill)
        source_row.addRow(self.tr("Source:"), self.source_input)
        layout.addLayout(source_row)

        button_row = QHBoxLayout()
        self.hint_bar = HintBar(self)
        button_row.addWidget(self.hint_bar, 1)
        button_row.addWidget(make_github_help_button(self, "reference-data-dialog.md"), 0, Qt.AlignRight | Qt.AlignVCenter)
        self._hint_controller = HintStatusController(self.hint_bar, self)
        self._hint_controller.set_hint(self._default_hint_text)

        self.save_btn = QPushButton(self.tr("Save"))
        self.save_btn.clicked.connect(self._on_save)
        self.delete_btn = QPushButton(self.tr("Delete"))
        self.delete_btn.clicked.connect(self._on_delete)
        self.delete_btn.setVisible(self._allow_delete)
        self.cancel_btn = QPushButton(self.tr("Cancel"))
        self.cancel_btn.clicked.connect(self.reject)
        if self._allow_delete:
            button_row.addWidget(self.delete_btn)
        button_row.addWidget(self.save_btn)
        button_row.addWidget(self.cancel_btn)
        layout.addLayout(button_row)

        self._register_hint_widget(self.spore_table, self._default_hint_text)

        self._apply_prefill()

    def _register_hint_widget(self, widget: QWidget, hint_text: str | None, tone: str = "info") -> None:
        if not widget:
            return
        hint = (hint_text or "").strip()
        hint_tone = (tone or "info").strip().lower()
        widget.setProperty("_hint_text", hint)
        widget.setProperty("_hint_tone", hint_tone)
        widget.setToolTip("")
        if self._hint_controller is not None:
            self._hint_controller.register_widget(widget, hint, tone=hint_tone)

    def _set_hint(self, text: str | None, tone: str = "info") -> None:
        if self._hint_controller is not None:
            self._hint_controller.set_hint(text, tone=tone)

    def _on_minmax_header_entered(self, section: int) -> None:
        hint = self._minmax_header_hints.get(int(section))
        self._set_hint(hint or self._default_hint_text)

    def eventFilter(self, watched, event):
        if watched is getattr(self, "_minmax_header_viewport", None):
            if event.type() == QEvent.Leave:
                self._set_hint(self._default_hint_text)
            elif event.type() == QEvent.MouseMove:
                header = self.minmax_table.horizontalHeader()
                section = header.logicalIndexAt(event.pos())
                hint = self._minmax_header_hints.get(int(section))
                self._set_hint(hint or self._default_hint_text)
        return super().eventFilter(watched, event)

    def _table_value(self, row, col):
        item = self.minmax_table.item(row, col)
        if not item:
            return None
        try:
            return float(item.text().strip())
        except ValueError:
            return None

    def _on_minmax_item_changed(self, item: QTableWidgetItem):
        if self._formatting_minmax:
            return
        if not item:
            return
        text = item.text().strip()
        if not text:
            return
        try:
            value = float(text)
        except ValueError:
            return
        self._formatting_minmax = True
        try:
            item.setText(f"{value:.2f}")
        finally:
            self._formatting_minmax = False

    def _parmasto_value(self, key: str):
        widget = self.parmasto_inputs.get(key)
        if widget is None:
            return None
        text = widget.text().strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    def _reference_record_data(self):
        data = {
            "genus": self._genus,
            "species": self._species,
            "source": self.source_input.text().strip() or None,
            "plot_color": self._plot_color,
            "parmasto_length_mean": self._parmasto_value("parmasto_length_mean"),
            "parmasto_width_mean": self._parmasto_value("parmasto_width_mean"),
            "parmasto_q_mean": self._parmasto_value("parmasto_q_mean"),
            "parmasto_v_sp_length": self._parmasto_value("parmasto_v_sp_length"),
            "parmasto_v_sp_width": self._parmasto_value("parmasto_v_sp_width"),
            "parmasto_v_sp_q": self._parmasto_value("parmasto_v_sp_q"),
            "parmasto_v_ind_length": self._parmasto_value("parmasto_v_ind_length"),
            "parmasto_v_ind_width": self._parmasto_value("parmasto_v_ind_width"),
            "parmasto_v_ind_q": self._parmasto_value("parmasto_v_ind_q"),
            "length_min": self._table_value(0, 0),
            "length_p05": self._table_value(0, 1),
            "length_p50": self._table_value(0, 2),
            "length_p95": self._table_value(0, 3),
            "length_max": self._table_value(0, 4),
            "width_min": self._table_value(1, 0),
            "width_p05": self._table_value(1, 1),
            "width_p50": self._table_value(1, 2),
            "width_p95": self._table_value(1, 3),
            "width_max": self._table_value(1, 4),
            "q_min": self._table_value(2, 0),
            "q_p50": self._table_value(2, 2),
            "q_max": self._table_value(2, 4),
        }
        has_values = any(
            data.get(key) is not None
            for key in (
                "length_min",
                "length_p05",
                "length_p50",
                "length_p95",
                "length_max",
                "width_min",
                "width_p05",
                "width_p50",
                "width_p95",
                "width_max",
                "parmasto_length_mean",
                "parmasto_width_mean",
                "parmasto_q_mean",
                "parmasto_v_sp_length",
                "parmasto_v_sp_width",
                "parmasto_v_sp_q",
                "parmasto_v_ind_length",
                "parmasto_v_ind_width",
                "parmasto_v_ind_q",
            )
        )
        if not has_values:
            return None
        data["source_kind"] = "reference"
        return data

    def _points_data(self):
        points = self.spore_table.get_points()
        if not points:
            return None
        source_label = self.source_input.text().strip()
        if not source_label:
            source_label = self.tr("Reference points")
        return {
            "genus": self._genus,
            "species": self._species,
            "points": points,
            "points_label": source_label,
            "plot_color": self._plot_color,
            "source_kind": "points",
            "source_type": "custom",
        }

    def _on_save(self):
        if self.tabs.currentIndex() == 1:
            data = self._points_data()
            if not data:
                QMessageBox.warning(
                    self,
                    self.tr("Missing Data"),
                    self.tr("Enter at least one length and width value.")
                )
                return
        else:
            data = self._reference_record_data()
            if not data:
                QMessageBox.warning(
                    self,
                    self.tr("Missing Data"),
                    self.tr("Enter at least one reference or Parmasto value.")
                )
                return
        self._result = data
        self.accept()

    def result_data(self):
        return self._result

    def delete_requested(self) -> bool:
        return bool(self._delete_requested)

    def _set_plot_color(self, color: str | None) -> None:
        self._plot_color = str(color).strip().lower() if color else None

    def _on_delete(self) -> None:
        if not self._allow_delete:
            return
        answer = QMessageBox.question(
            self,
            self.tr("Delete Reference"),
            self.tr("Delete the selected stored reference?"),
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if answer != QMessageBox.Yes:
            return
        self._delete_requested = True
        self._result = None
        self.accept()

    def _apply_prefill(self):
        data = self._prefill_data
        if not data:
            self._set_plot_color(None)
            return

        def _set_cell(row, col, value):
            if value is None:
                return
            item = QTableWidgetItem(f"{value:.2f}")
            self.minmax_table.setItem(row, col, item)

        _set_cell(0, 0, data.get("length_min"))
        _set_cell(0, 1, data.get("length_p05"))
        _set_cell(0, 2, data.get("length_p50"))
        _set_cell(0, 3, data.get("length_p95"))
        _set_cell(0, 4, data.get("length_max"))
        _set_cell(1, 0, data.get("width_min"))
        _set_cell(1, 1, data.get("width_p05"))
        _set_cell(1, 2, data.get("width_p50"))
        _set_cell(1, 3, data.get("width_p95"))
        _set_cell(1, 4, data.get("width_max"))
        _set_cell(2, 0, data.get("q_min"))
        _set_cell(2, 2, data.get("q_p50"))
        _set_cell(2, 4, data.get("q_max"))

        points = data.get("points") or []
        if points:
            self.spore_table._ensure_rows(len(points))
            for row, point in enumerate(points):
                length = point.get("length_um")
                width = point.get("width_um")
                if length is not None:
                    self.spore_table.setItem(row, 0, QTableWidgetItem(f"{length:g}"))
                if width is not None:
                    self.spore_table.setItem(row, 1, QTableWidgetItem(f"{width:g}"))
                self.spore_table._update_q_for_row(row)
            self.tabs.setCurrentIndex(1)
        parmasto_has_values = False
        for key, widget in self.parmasto_inputs.items():
            value = data.get(key)
            if value is None:
                widget.clear()
                continue
            widget.setText(f"{float(value):g}")
            parmasto_has_values = True
        if parmasto_has_values and not points:
            self.tabs.setCurrentIndex(2)
        self._set_plot_color(data.get("plot_color"))

class MainWindow(GeometryMixin, QMainWindow):
    """Main application window with modern UI and measurement table."""

    _geometry_key = "MainWindow"
    SETTING_MEASURE_SHOW_LABELS = "measure_view_show_labels"
    SETTING_MEASURE_SHOW_OVERLAYS = "measure_view_show_overlays"
    SETTING_MEASURE_SHOW_SCALE_BAR = "measure_view_show_scale_bar"
    SETTING_MEASURE_SHOW_COPYRIGHT = "measure_view_show_copyright"
    SETTING_MEASURE_RECTANGLE_STYLE = "measure_view_rectangle_style"
    SETTING_MEASURE_RECTANGLE_THICKNESS = "measure_view_rectangle_thickness"

    def __init__(self, app_version: str | None = None):
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.setGeometry(100, 100, 1600, 900)
        self.app_version = app_version or ""
        self._update_check_started = False
        self._pixmap_cache: dict[str, QPixmap] = {}
        self._pixmap_cache_order: list[str] = []
        self._pixmap_cache_max = 6
        self._pixmap_cache_observation_id = None

        self.current_image_path = None
        self.current_image_id = None
        self.current_pixmap = None
        self.points = []  # Will store 4 points for two measurements
        self.measurement_lines = {}  # Dict mapping measurement_id -> [line1, line2]
        self.multiline_measurements = {}
        self.temp_lines = []  # Temporary lines for current measurement in progress
        self.measure_mode = "rectangle"
        self.measurements_cache = []
        self.rect_stage = 0
        self.rect_line1_start = None
        self.rect_line1_end = None
        self.rect_line2_start = None
        self.rect_line2_end = None
        self.rect_width_dir = None
        self.rect_length_dir = None
        self.rect_length_dir = None

        # Current objective settings
        self.current_objective = None
        self.current_objective_name = None
        self.microns_per_pixel = 0.5
        self.current_calibration_id = None
        self.current_image_type = None
        # Keep independent scale-bar overlay lengths per image type.
        self._scale_bar_overlay_field_mm = 10.0
        self._scale_bar_overlay_micro_um = 10.0
        self._scale_bar_overlay_field_is_manual = False
        self._current_measure_scale_bar_value_custom = False
        # Microscope overlay presets by objective; manual edits establish basis
        # used to derive values for other objectives.
        self._scale_bar_micro_manual_by_objective: dict[str, float] = {}
        self._scale_bar_micro_basis_objective: str | None = None
        self._measure_session_view_states: dict[int, dict] = {}
        self._loading_measure_image_note = False
        self._measure_image_note_save_timer = QTimer(self)
        self._measure_image_note_save_timer.setSingleShot(True)
        self._measure_image_note_save_timer.setInterval(350)
        self._measure_image_note_save_timer.timeout.connect(self._commit_measure_image_note)

        # Active observation tracking
        self.active_observation_id = None
        self.active_observation_name = None
        self.observation_images = []

        # Gallery thumbnail rotation tracking (measurement_id -> extra rotation in degrees)
        self.gallery_rotations = {}
        self.current_image_index = -1
        self.export_scale_percent = 100.0
        self.export_format = "png"
        self.default_measure_color = QColor("#000000")
        self.measure_color = QColor(self.default_measure_color)
        self.measurement_labels = []
        self.measurement_active = False
        self._auto_started_for_microscope = False
        self.auto_threshold = None
        self.auto_threshold_default = 0.12
        self.auto_gray_cache = None
        self.auto_gray_cache_id = None
        self.auto_max_radius = None
        self.gallery_filter_mode = None
        self.gallery_filter_value = None
        self.gallery_filter_ids = set()
        self._last_gallery_category = None
        self._pending_gallery_category = None
        self._suppress_gallery_update = False
        self._gallery_refresh_in_progress = False
        self._gallery_refresh_timer = None
        self._gallery_refresh_pending = False
        self._gallery_last_refresh_time = 0.0
        self._gallery_thumb_cache = {}
        self._gallery_thumb_geometry_cache = {}
        self._gallery_thumb_cache_observation_id = None
        self._gallery_pixmap_cache = {}
        self._gallery_render_timer = None
        self._gallery_render_queue = []
        self._gallery_render_state = None
        self._gallery_collapsed = False
        self._gallery_hint_controller: HintStatusController | None = None
        self._pending_gallery_hint_widgets: list[tuple[QWidget, str, str, bool]] = []
        self._gallery_scatter_axis = None
        self._gallery_hist_axes: set = set()
        self._gallery_hover_hint_key = ""
        self._gallery_busy_hint = ""
        self._gallery_pending_refresh_hint = ""
        self._gallery_render_total_items = 0
        self.gallery_selected_measurement_id = None
        self._gallery_thumbnail_frames: dict[int, QFrame] = {}
        self._gallery_thumbnail_labels: dict[int, QWidget] = {}
        self._gallery_measurement_lookup: dict[int, dict] = {}
        self._gallery_thumbnail_render_state: dict | None = None
        self._gallery_pan_active = False
        self._gallery_pan_axis = None
        self._gallery_pan_start = None
        self._gallery_pan_xlim = None
        self._gallery_pan_ylim = None
        self._gallery_pan_recently_dragged = False
        self._publish_excluded_image_ids_by_observation: dict[int, set[int]] = {}
        self.loading_dialog = None
        self.reference_values = {}
        self.species_availability = SpeciesDataAvailability()
        self._ref_completer_suppress = False
        self._ref_taxon_fill_from_vernacular = False
        self._ref_genus_summary_cache_key = None
        self._ref_genus_summary_cache = {}
        self.reference_series = []
        self.suppress_scale_prompt = False
        self._measure_category_sync = False
        self._measurement_type_by_id: dict[int, str] = {}
        self._show_calibration_overlay_for_scalebar = False
        self._prevent_cross_image_switch_from_table_selection = False

        # Calibration mode tracking
        self.calibration_mode = False
        self.calibration_dialog = None
        self.calibration_points = []

        # Apply theme (palette + stylesheet). Reads "ui_theme" from SettingsDB.
        self._apply_theme()

        # Re-apply theme automatically when the OS switches dark/light (Qt 6.5+).
        try:
            QApplication.instance().styleHints().colorSchemeChanged.connect(
                self._on_system_color_scheme_changed
            )
        except Exception:
            pass

        self.init_ui()
        self._populate_scale_combo()
        self.load_default_objective()
        self._restore_geometry()

    def closeEvent(self, event):
        self._save_current_image_measure_view_settings()
        self._save_geometry()
        if hasattr(self, "observations_tab") and self.observations_tab is not None:
            try:
                self.observations_tab.shutdown()
            except Exception:
                pass
        if hasattr(self, "live_lab_tab") and self.live_lab_tab is not None:
            try:
                self.live_lab_tab.shutdown()
            except Exception:
                pass
        super().closeEvent(event)

    def eventFilter(self, obj, event):
        """Show certain tooltips immediately on hover."""
        if obj.property("instant_tooltip"):
            if event.type() == QEvent.Enter:
                tip = obj.toolTip()
                if tip:
                    QToolTip.showText(obj.mapToGlobal(QPoint(0, obj.height())), tip, obj)
            elif event.type() == QEvent.Leave:
                QToolTip.hideText()
        if obj.property("gallery_fade_icon"):
            effect = obj.graphicsEffect()
            if isinstance(effect, QGraphicsOpacityEffect):
                if event.type() == QEvent.Enter:
                    effect.setOpacity(1.0)
                elif event.type() == QEvent.Leave:
                    effect.setOpacity(float(obj.property("gallery_idle_opacity") or 0.28))
        if event.type() == QEvent.FocusIn:
            if obj == getattr(self, "ref_genus_input", None):
                text = self._clean_ref_genus_text(self.ref_genus_input.text())
                self._update_ref_genus_suggestions(text)
                if self._ref_genus_model.stringList():
                    self._ref_genus_completer.complete()
            elif obj == getattr(self, "ref_species_input", None):
                genus = self._clean_ref_genus_text(self.ref_genus_input.text())
                if genus:
                    text = self._clean_ref_species_text(self.ref_species_input.text())
                    self._update_ref_species_suggestions(genus, text)
                    if self._ref_species_model.rowCount() > 0:
                        self._ref_species_completer.setCompletionPrefix(text)
                        self._ref_species_completer.complete()
            elif obj == getattr(self, "ref_vernacular_input", None):
                if not self.ref_vernacular_input.text().strip():
                    self._ref_vernacular_completer.setCompletionPrefix("")
                    self._update_ref_vernacular_suggestions_for_taxon()
                    if self._ref_vernacular_model.rowCount() > 0:
                        self._ref_vernacular_completer.complete()
        if event.type() == QEvent.MouseButtonPress:
            if obj == getattr(self, "ref_source_input", None):
                if self.ref_source_input.count() > 1:
                    self.ref_source_input.showPopup()
            if hasattr(self, "ref_source_input") and obj == self.ref_source_input.lineEdit():
                if self.ref_source_input.count() > 1:
                    self.ref_source_input.showPopup()
        return super().eventFilter(obj, event)

    def init_ui(self):
        """Initialize the user interface."""
        # Create menu bar
        self.create_menu_bar()

        # Central widget
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(10, 10, 10, 10)

        # Observation header
        self.observation_header_label = QLabel("")
        self.observation_header_label.setObjectName("observationHeaderLabel")
        self.observation_header_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        main_layout.addWidget(self.observation_header_label)

        # Main tabbed interface (takes full width)
        self.tab_widget = QTabWidget()
        self.tab_widget.setTabPosition(QTabWidget.North)
        self.tab_widget.currentChanged.connect(self.on_tab_changed)
        # Observations tab (new - first tab)
        self.observations_tab = ObservationsTab()
        self.observations_tab.observation_selected.connect(self.on_observation_selected)
        self.observations_tab.observation_highlighted.connect(self.update_observation_header)
        self.observations_tab.selection_count_changed.connect(self._on_multi_observation_selected)
        self.observations_tab.image_selected.connect(self.on_image_selected)
        self.observations_tab.observation_deleted.connect(self.on_observation_deleted)
        self.tab_widget.addTab(self.observations_tab, self.tr("Observations ({alt}O)").format(alt=_ALT_LABEL))

        # Measure tab (includes control panel on left and stats panel on right)
        measure_tab = self.create_measure_tab()
        self.tab_widget.addTab(measure_tab, self.tr("Measure ({alt}M)").format(alt=_ALT_LABEL))

        # Analysis tab
        gallery_tab = self.create_gallery_panel()
        self.analysis_tab = gallery_tab
        self.tab_widget.addTab(gallery_tab, self.tr("Analysis ({alt}A)").format(alt=_ALT_LABEL))
        self.refresh_gallery_filter_options()

        self.live_lab_tab = LiveLabTab(self)
        self.tab_widget.addTab(self.live_lab_tab, self.tr("Live Lab ({alt}L)").format(alt=_ALT_LABEL))

        self.ingestion_hub_tab = IngestionHubTab(self)
        self.tab_widget.addTab(self.ingestion_hub_tab, self.tr("Ingestion ({alt}I)").format(alt=_ALT_LABEL))

        main_layout.addWidget(self.tab_widget, 1)

        # Corner buttons: settings cog + calibration icon
        corner = QWidget()
        corner_layout = QHBoxLayout(corner)
        corner_layout.setContentsMargins(0, 0, 4, 0)
        corner_layout.setSpacing(2)

        self._calib_corner_btn = QToolButton()
        self._calib_corner_btn.setObjectName("cornerIconBtn")
        self._calib_corner_btn.setToolTip(self.tr("Calibration (Ctrl+K)"))
        self._calib_corner_btn.setIcon(_make_svg_icon(_SVG_CALIBRATION))
        self._calib_corner_btn.setIconSize(QSize(20, 20))
        self._calib_corner_btn.clicked.connect(self.open_calibration_dialog)
        corner_layout.addWidget(self._calib_corner_btn)

        self._settings_corner_btn = QToolButton()
        self._settings_corner_btn.setObjectName("cornerIconBtn")
        self._settings_corner_btn.setToolTip(self.tr("Settings (Ctrl+,)"))
        self._settings_corner_btn.setIcon(_make_svg_icon(_SVG_SETTINGS))
        self._settings_corner_btn.setIconSize(QSize(20, 20))
        self._settings_corner_btn.clicked.connect(self.open_settings_hub)
        corner_layout.addWidget(self._settings_corner_btn)

        self.tab_widget.setCornerWidget(corner, Qt.TopRightCorner)

        # Tab navigation shortcuts: Alt/Option on all platforms.
        # Cmd (Meta) deliberately excluded — Cmd+A/M/O conflict with system shortcuts on macOS.
        # From Observations → Measure (Alt+M), Analysis (Alt+A)
        # From Measure      → Observations (Alt+O), Analysis (Alt+A)
        # From Analysis     → Observations (Alt+O), Measure (Alt+M)
        _tab_shortcuts = [
            (self.observations_tab, Qt.Key_M, lambda: self._switch_tab_from_observations(1)),
            (self.observations_tab, Qt.Key_A, lambda: self._switch_tab_from_observations(2)),
            (self.observations_tab, Qt.Key_L, lambda: self._switch_tab_from_observations(3)),
            (self.observations_tab, Qt.Key_I, lambda: self._switch_tab_from_observations(4)),
            (self.measure_tab,      Qt.Key_O, lambda: self.tab_widget.setCurrentIndex(0)),
            (self.measure_tab,      Qt.Key_A, lambda: self.tab_widget.setCurrentIndex(2)),
            (self.measure_tab,      Qt.Key_L, lambda: self.tab_widget.setCurrentIndex(3)),
            (self.measure_tab,      Qt.Key_I, lambda: self.tab_widget.setCurrentIndex(4)),
            (self.analysis_tab,     Qt.Key_O, lambda: self.tab_widget.setCurrentIndex(0)),
            (self.analysis_tab,     Qt.Key_M, lambda: self.tab_widget.setCurrentIndex(1)),
            (self.analysis_tab,     Qt.Key_L, lambda: self.tab_widget.setCurrentIndex(3)),
            (self.analysis_tab,     Qt.Key_I, lambda: self.tab_widget.setCurrentIndex(4)),
            (self.live_lab_tab,     Qt.Key_O, lambda: self.tab_widget.setCurrentIndex(0)),
            (self.live_lab_tab,     Qt.Key_M, lambda: self.tab_widget.setCurrentIndex(1)),
            (self.live_lab_tab,     Qt.Key_A, lambda: self.tab_widget.setCurrentIndex(2)),
            (self.live_lab_tab,     Qt.Key_I, lambda: self.tab_widget.setCurrentIndex(4)),
            (self.ingestion_hub_tab, Qt.Key_O, lambda: self.tab_widget.setCurrentIndex(0)),
            (self.ingestion_hub_tab, Qt.Key_M, lambda: self.tab_widget.setCurrentIndex(1)),
            (self.ingestion_hub_tab, Qt.Key_A, lambda: self.tab_widget.setCurrentIndex(2)),
            (self.ingestion_hub_tab, Qt.Key_L, lambda: self.tab_widget.setCurrentIndex(3)),
        ]
        self._tab_nav_shortcuts = []
        for widget, key, slot in _tab_shortcuts:
            sc = QShortcut(QKeySequence(Qt.ALT | key), widget)
            sc.setContext(Qt.WidgetWithChildrenShortcut)
            sc.activated.connect(slot)
            self._tab_nav_shortcuts.append(sc)

    def create_menu_bar(self):
        """Create the menu bar."""
        menubar = self.menuBar()

        # File menu
        file_menu = menubar.addMenu(self.tr("File"))

        export_ml_action = QAction(self.tr("Export ML"), self)
        export_handler = getattr(self, "export_ml_dataset", None)
        if export_handler is None:
            export_handler = lambda: QMessageBox.warning(
                self,
                self.tr("Export Unavailable"),
                self.tr("Export ML is not available.")
            )
        export_ml_action.triggered.connect(export_handler)
        file_menu.addAction(export_ml_action)

        file_menu.addSeparator()

        exit_action = QAction(self.tr("Exit"), self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        settings_menu = menubar.addMenu(self.tr("Settings"))
        settings_action = QAction(self.tr("Preferences"), self)
        settings_action.setShortcut("Ctrl+,")
        settings_action.triggered.connect(self.open_settings_hub)
        settings_menu.addAction(settings_action)

        settings_menu.addSeparator()

        calib_action = QAction(self.tr("Calibration"), self)
        calib_action.setShortcut("Ctrl+K")
        calib_action.triggered.connect(self.open_calibration_dialog)
        settings_menu.addAction(calib_action)

        help_menu = menubar.addMenu(self.tr("Help"))
        version_text = self.tr("Version: {version}").format(
            version=self.app_version or self.tr("Unknown")
        )
        version_action = QAction(version_text, self)
        version_action.setEnabled(False)
        help_menu.addAction(version_action)

        release_action = QAction(self.tr("Open latest release"), self)
        release_action.triggered.connect(
            lambda: QDesktopServices.openUrl(
                QUrl("https://github.com/sigmundas/mycolog/releases/latest")
            )
        )
        help_menu.addAction(release_action)

    def start_update_check(self):
        """Check GitHub for newer releases without blocking the UI."""
        if self._update_check_started:
            return
        self._update_check_started = True
        current_version = self._parse_version(self.app_version)
        if current_version is None:
            return
        if not hasattr(self, "_update_network"):
            self._update_network = QNetworkAccessManager(self)
        
        # Use Atom feed instead of API - no rate limits!
        req = QNetworkRequest(QUrl("https://github.com/sigmundas/mycolog/releases.atom"))
        req.setHeader(QNetworkRequest.UserAgentHeader, f"{APP_NAME}/{self.app_version}")
        
        reply = self._update_network.get(req)
        reply.finished.connect(
            lambda: self._handle_atom_reply(reply, current_version)
        )

    def _handle_atom_reply(self, reply: QNetworkReply, current_version: tuple[int, ...]):
        """Handle Atom feed response from GitHub releases."""
        try:
            if reply.error() != QNetworkReply.NoError:
                return  # Silently fail - update check is non-critical
            
            payload = bytes(reply.readAll())
            
            # Parse XML properly
            from xml.etree import ElementTree as ET
            root = ET.fromstring(payload.decode("utf-8"))
            
            # Atom namespace
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            
            # Find first entry (latest release)
            entry = root.find('atom:entry', ns)
            if entry is None:
                return
            
            title_elem = entry.find('atom:title', ns)
            link_elem = entry.find('atom:link[@rel="alternate"]', ns)
            
            if title_elem is None or link_elem is None:
                return
            
            title = title_elem.text.strip()
            url = link_elem.get('href', '').strip()
            
            # Extract version from title (e.g., "v0.2.2" -> "0.2.2")
            version = title.lower().replace("release", "").strip().lstrip("v").strip()
            
            if self._is_newer_version(version, current_version):
                self._show_update_dialog(version, url)
                
        except Exception:
            pass  # Silently fail - update check is non-critical
        finally:
            reply.deleteLater()


    def _parse_version(self, version: str | None) -> tuple[int, ...] | None:
        if not version:
            return None
        raw = str(version).strip()
        if raw.startswith("v"):
            raw = raw[1:]
        raw = raw.split("-", 1)[0].split("+", 1)[0]
        parts = raw.split(".")
        if not parts:
            return None
        values = []
        for part in parts:
            if not part.isdigit():
                return None
            values.append(int(part))
        return tuple(values)

    def _is_newer_version(self, latest: str, current: tuple[int, ...]) -> bool:
        latest_parsed = self._parse_version(latest)
        if latest_parsed is None:
            return False
        max_len = max(len(latest_parsed), len(current))
        latest_padded = latest_parsed + (0,) * (max_len - len(latest_parsed))
        current_padded = current + (0,) * (max_len - len(current))
        return latest_padded > current_padded

    def _show_update_dialog(self, latest_version: str, url: str):
        current = self.app_version or self.tr("Unknown")
        box = QMessageBox(self)
        box.setIcon(QMessageBox.Information)
        box.setWindowTitle(self.tr("Update available"))
        box.setText(
            self.tr("A newer version of MycoLog is available.").replace(LEGACY_APP_NAME, APP_NAME)
        )
        box.setInformativeText(
            self.tr("Current version: {current}\nLatest version: {latest}").format(
                current=current,
                latest=latest_version
            )
        )
        open_btn = box.addButton(self.tr("Open download page"), QMessageBox.AcceptRole)
        box.addButton(self.tr("Later"), QMessageBox.RejectRole)
        box.exec()
        if box.clickedButton() == open_btn:
            QDesktopServices.openUrl(QUrl(url))

    def create_control_panel(self):
        """Create the left control panel."""
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setSpacing(10)

        # Image loading group
        # Import/Export now available from the top menus

        # Scale group
        calib_group = QGroupBox(self.tr("Scale"))
        self.calib_group = calib_group
        calib_layout = QVBoxLayout()

        self.scale_combo = QComboBox()
        self.scale_combo.currentIndexChanged.connect(self.on_scale_combo_changed)
        calib_layout.addWidget(self.scale_combo)

        self.set_from_scalebar_btn = QPushButton(self.tr("Set from scalebar"))
        self.set_from_scalebar_btn.clicked.connect(self._on_set_from_scalebar_clicked)
        calib_layout.addWidget(self.set_from_scalebar_btn)

        self.scale_bar_inline = QWidget()
        scale_inline_layout = QHBoxLayout(self.scale_bar_inline)
        scale_inline_layout.setContentsMargins(0, 0, 0, 0)
        scale_inline_layout.setSpacing(6)
        self.scale_bar_length_input = QDoubleSpinBox()
        self.scale_bar_length_input.setRange(0.1, 100000.0)
        self.scale_bar_length_input.setDecimals(1)
        self.scale_bar_length_input.setSingleStep(1.0)
        self.scale_bar_length_input.setValue(10.0)
        self.scale_bar_length_input.setSuffix(" µm")
        self.scale_bar_horizontal_checkbox = QCheckBox(self.tr("Horizontal"))
        self.scale_bar_horizontal_checkbox.toggled.connect(self._on_scale_bar_horizontal_toggled)
        scale_inline_layout.addWidget(self.scale_bar_length_input, 1)
        scale_inline_layout.addWidget(self.scale_bar_horizontal_checkbox, 0)
        calib_layout.addWidget(self.scale_bar_inline)

        self.calib_info_label = QLabel(self.tr("Calibration: --"))
        self.calib_info_label.setWordWrap(True)
        self.calib_info_label.setStyleSheet(f"color: #7f8c8d; font-size: {pt(9)}pt;")
        self.calib_info_label.setTextFormat(Qt.RichText)
        self.calib_info_label.setTextInteractionFlags(Qt.TextBrowserInteraction)
        self.calib_info_label.setOpenExternalLinks(False)
        self.calib_info_label.linkActivated.connect(self._on_calibration_link_clicked)
        calib_layout.addWidget(self.calib_info_label)

        self.scale_warning_label = QLabel("")
        self.scale_warning_label.setWordWrap(True)
        self.scale_warning_label.setStyleSheet(f"color: #e74c3c; font-weight: bold; font-size: {pt(9)}pt;")
        self.scale_warning_label.setVisible(False)
        calib_layout.addWidget(self.scale_warning_label)

        calib_group.setLayout(calib_layout)
        layout.addWidget(calib_group)

        # Measurement group (category dropdown lives at the top)
        measure_group = QGroupBox(self.tr("Measure"))
        measure_layout = QVBoxLayout()

        self.measure_category_combo = QComboBox()
        self._populate_measure_categories()
        self.measure_category_combo.currentIndexChanged.connect(self.on_measure_category_changed)
        measure_layout.addWidget(self.measure_category_combo)

        mode_row = QHBoxLayout()
        self.mode_group = QButtonGroup(self)
        self.mode_lines = QRadioButton(self.tr("Line"))
        self.mode_rect = QRadioButton(self.tr("Rectangle"))
        self.mode_multiline = QRadioButton(self.tr("Multi-line"))
        self.mode_rect.setChecked(True)
        self.mode_group.addButton(self.mode_lines)
        self.mode_group.addButton(self.mode_rect)
        self.mode_group.addButton(self.mode_multiline)
        self.mode_lines.toggled.connect(self.on_measure_mode_changed)
        self.mode_rect.toggled.connect(self.on_measure_mode_changed)
        self.mode_multiline.toggled.connect(self.on_measure_mode_changed)
        mode_row.addWidget(self.mode_lines)
        mode_row.addSpacing(16)
        mode_row.addWidget(self.mode_rect)
        mode_row.addSpacing(16)
        mode_row.addWidget(self.mode_multiline)
        mode_row.addStretch()
        measure_layout.addLayout(mode_row)

        self.measure_button = QPushButton(self.tr("Start measuring (M)"))
        self.measure_button.setCheckable(True)
        self.measure_button.setMinimumHeight(35)
        self.measure_button.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.measure_button.setStyleSheet("font-weight: bold; padding: 6px 10px;")
        self.measure_button.clicked.connect(self._on_measure_button_clicked)
        measure_layout.addWidget(self.measure_button)

        self.measure_status_label = QLabel("")
        self.measure_status_label.setWordWrap(True)
        self.measure_status_label.setStyleSheet(f"color: #27ae60; font-weight: bold; font-size: {pt(9)}pt;")
        self.measure_status_label.setVisible(True)
        measure_layout.addWidget(self.measure_status_label)

        measure_group.setLayout(measure_layout)
        layout.addWidget(measure_group)

        # Zoom controls
        zoom_group = QGroupBox(self.tr("View"))
        zoom_layout = QVBoxLayout()
        view_buttons_row = QHBoxLayout()

        reset_btn = QPushButton(self.tr("Reset"))
        reset_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        reset_btn.setMinimumHeight(35)
        reset_btn.clicked.connect(self.reset_view)
        view_buttons_row.addWidget(reset_btn)

        export_image_btn = QPushButton(self.tr("Export image"))
        export_image_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        export_image_btn.setMinimumHeight(35)
        export_image_btn.clicked.connect(self.export_annotated_image)
        view_buttons_row.addWidget(export_image_btn)
        view_buttons_row.setStretch(0, 1)
        view_buttons_row.setStretch(1, 1)
        zoom_layout.addLayout(view_buttons_row)

        color_row = QHBoxLayout()
        color_row.setContentsMargins(0, 0, 0, 0)
        color_row.setSpacing(8)
        self.measure_color_label = QLabel(self.tr("Color:"))
        self.measure_color_menu_button = QToolButton()
        self.measure_color_menu_button.setFixedSize(28, 22)
        self.measure_color_menu_button.setAutoRaise(False)
        self.measure_color_menu_button.clicked.connect(
            lambda _checked=False, btn=self.measure_color_menu_button: self._open_measure_color_menu(btn)
        )
        color_row.addWidget(self.measure_color_label)
        color_row.addWidget(self.measure_color_menu_button)
        color_row.addStretch()
        zoom_layout.addLayout(color_row)

        self.show_measures_checkbox = QCheckBox(self.tr("Show measures"))
        self.show_measures_checkbox.setChecked(
            self._measure_view_setting_enabled(self.SETTING_MEASURE_SHOW_LABELS, default=True)
        )
        self.show_measures_checkbox.toggled.connect(self.on_show_measures_toggled)
        zoom_layout.addWidget(self.show_measures_checkbox)

        self.show_rectangles_checkbox = QCheckBox(self.tr("Show rectangles"))
        self.show_rectangles_checkbox.setChecked(
            self._measure_view_setting_enabled(self.SETTING_MEASURE_SHOW_OVERLAYS, default=True)
        )
        self.show_rectangles_checkbox.toggled.connect(self.on_show_rectangles_toggled)
        zoom_layout.addWidget(self.show_rectangles_checkbox)

        self.rectangle_style_container = QWidget()
        rectangle_style_layout = QVBoxLayout(self.rectangle_style_container)
        rectangle_style_layout.setContentsMargins(18, 0, 0, 0)
        rectangle_style_layout.setSpacing(6)

        style_row = QHBoxLayout()
        style_row.setContentsMargins(0, 0, 0, 0)
        style_row.setSpacing(8)
        self.rectangle_style_label = QLabel(self.tr("Style"))
        self.rectangle_style_a_radio = QRadioButton("A")
        self.rectangle_style_b_radio = QRadioButton("B")
        self.rectangle_style_group = QButtonGroup(self.rectangle_style_container)
        self.rectangle_style_group.addButton(self.rectangle_style_a_radio)
        self.rectangle_style_group.addButton(self.rectangle_style_b_radio)
        self.rectangle_style_a_radio.setMinimumWidth(42)
        self.rectangle_style_b_radio.setMinimumWidth(42)
        style_value = normalize_rectangle_style(
            self._measure_view_setting_text(self.SETTING_MEASURE_RECTANGLE_STYLE, DEFAULT_RECTANGLE_STYLE)
        )
        thickness_value = int(round(clamp_rectangle_thickness(
            self._measure_view_setting_text(
                self.SETTING_MEASURE_RECTANGLE_THICKNESS,
                str(int(DEFAULT_RECTANGLE_THICKNESS)),
            )
        )))
        self.rectangle_thick_label = QLabel(self.tr("T:"))
        self.rectangle_thick_checkbox = QCheckBox()
        self.rectangle_thick_checkbox.setText("")
        self.rectangle_thick_checkbox.setToolTip(self.tr("Use thicker rectangle corners/stroke."))
        self.rectangle_style_a_radio.setChecked(style_value == "a")
        self.rectangle_style_b_radio.setChecked(style_value == "b")
        self.rectangle_thick_checkbox.setChecked(thickness_value >= 2)
        self.rectangle_style_a_radio.toggled.connect(self.on_measure_rectangle_style_changed)
        self.rectangle_style_b_radio.toggled.connect(self.on_measure_rectangle_style_changed)
        self.rectangle_thick_checkbox.toggled.connect(self.on_measure_rectangle_thickness_changed)
        style_row.addWidget(self.rectangle_style_label)
        style_row.addSpacing(6)
        style_row.addWidget(self.rectangle_style_a_radio)
        style_row.addWidget(self.rectangle_style_b_radio)
        style_row.addSpacing(10)
        style_row.addWidget(self.rectangle_thick_label)
        style_row.addWidget(self.rectangle_thick_checkbox)
        style_row.addStretch()
        rectangle_style_layout.addLayout(style_row)

        self.rectangle_style_container.setVisible(self.show_rectangles_checkbox.isChecked())
        zoom_layout.addWidget(self.rectangle_style_container)

        self.set_measure_color(self.measure_color)

        self.show_scale_bar_checkbox = QCheckBox(self.tr("Show scale bar"))
        self.show_scale_bar_checkbox.setChecked(
            self._measure_view_setting_enabled(self.SETTING_MEASURE_SHOW_SCALE_BAR, default=False)
        )
        self.show_scale_bar_checkbox.toggled.connect(self.on_show_scale_bar_toggled)
        zoom_layout.addWidget(self.show_scale_bar_checkbox)

        scale_bar_row = QHBoxLayout()
        scale_bar_row.setContentsMargins(0, 0, 0, 0)
        scale_bar_row.setSpacing(6)
        self.scale_bar_length_label = QLabel(self.tr("Length reference"))
        self.scale_bar_input = QDoubleSpinBox()
        self.scale_bar_input.setRange(0.1, 100000.0)
        self.scale_bar_input.setDecimals(2)
        self.scale_bar_input.setValue(10.0)
        self.scale_bar_input.setSingleStep(1.0)
        self.scale_bar_input.setSuffix(" \u03bcm")
        self.scale_bar_input.valueChanged.connect(self.on_scale_bar_value_changed)
        self.scale_bar_input.setToolTip(self.tr("Length of the displayed scale bar in micrometers."))
        self.scale_bar_length_label.setBuddy(self.scale_bar_input)
        scale_bar_row.addWidget(self.scale_bar_length_label)
        scale_bar_row.addWidget(self.scale_bar_input)
        scale_bar_row.addStretch()
        self.scale_bar_container = QWidget()
        scale_bar_layout = QHBoxLayout(self.scale_bar_container)
        scale_bar_layout.setContentsMargins(18, 0, 0, 0)
        scale_bar_layout.addLayout(scale_bar_row)
        self.scale_bar_container.setToolTip(self.tr("Settings for the 'Show scale bar' option."))
        self.scale_bar_container.setVisible(False)
        zoom_layout.addWidget(self.scale_bar_container)

        self.show_copyright_checkbox = QCheckBox(self.tr("Show copyright"))
        self.show_copyright_checkbox.setChecked(
            self._measure_view_setting_enabled(self.SETTING_MEASURE_SHOW_COPYRIGHT, default=False)
        )
        self.show_copyright_checkbox.toggled.connect(self.on_show_copyright_toggled)
        zoom_layout.addWidget(self.show_copyright_checkbox)

        zoom_group.setLayout(zoom_layout)
        layout.addWidget(zoom_group)

        info_group = QGroupBox(self.tr("Info"))
        info_layout = QVBoxLayout()
        self.exif_info_label = QLabel(self.tr("No image loaded"))
        self.exif_info_label.setWordWrap(True)
        self.exif_info_label.setStyleSheet(f"font-size: {pt(8)}pt;")
        self.exif_info_label.setTextFormat(Qt.RichText)
        self.exif_info_label.setTextInteractionFlags(Qt.TextBrowserInteraction)
        self.exif_info_label.setOpenExternalLinks(True)
        info_layout.addWidget(self.exif_info_label)
        self.measure_image_note_input = QPlainTextEdit()
        self.measure_image_note_input.setPlaceholderText(self.tr("Per-image note..."))
        self.measure_image_note_input.setMaximumHeight(78)
        self.measure_image_note_input.setEnabled(False)
        self.measure_image_note_input.textChanged.connect(self._queue_measure_image_note_save)
        info_layout.addWidget(self.measure_image_note_input)
        info_group.setLayout(info_layout)
        layout.addWidget(info_group)

        layout.addStretch()
        self.update_measurement_button_state()
        return panel

    @staticmethod
    def _measure_view_setting_enabled(key: str, default: bool = False) -> bool:
        raw = SettingsDB.get_setting(key, "1" if default else "0")
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, (int, float)):
            return raw != 0
        text = str(raw or "").strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
        return bool(default)

    @staticmethod
    def _measure_view_setting_text(key: str, default: str = "") -> str:
        raw = SettingsDB.get_setting(key, default)
        text = str(raw or "").strip()
        return text if text else str(default)

    def _current_measure_rectangle_style(self) -> str:
        if hasattr(self, "rectangle_style_b_radio") and self.rectangle_style_b_radio.isChecked():
            return "b"
        return "a"

    def _current_measure_rectangle_thickness(self) -> float:
        if hasattr(self, "rectangle_thick_checkbox") and self.rectangle_thick_checkbox.isChecked():
            return 2.0
        if hasattr(self, "rectangle_thick_checkbox"):
            return 1.0
        return DEFAULT_RECTANGLE_THICKNESS

    def _apply_measure_rectangle_appearance(self, *, refresh_gallery: bool = False) -> None:
        style = self._current_measure_rectangle_style()
        thickness = self._current_measure_rectangle_thickness()
        if hasattr(self, "image_label"):
            self.image_label.set_measurement_rectangle_appearance(style=style, thickness=thickness)
        if hasattr(self, "spore_preview"):
            self.spore_preview.set_measurement_rectangle_appearance(style=style, thickness=thickness)
        if refresh_gallery:
            self._gallery_thumb_cache = {}
            self._gallery_thumb_geometry_cache = {}
            self.schedule_gallery_refresh()

    def _measure_image_view_settings_key(self, image_id: int | None = None) -> str | None:
        target_id = int(image_id or 0)
        if target_id <= 0:
            return None
        return f"measure_image_view_settings_{target_id}"

    def _measure_observation_scale_bar_value_key(self, is_field: bool, observation_id: int | None = None) -> str | None:
        target_id = int(observation_id if observation_id is not None else self.active_observation_id or 0)
        if target_id <= 0:
            return None
        suffix = "field_mm" if is_field else "micro_um"
        return f"measure_observation_scale_bar_value_{suffix}_{target_id}"

    def _observation_scale_bar_fallback_value(self, is_field: bool, objective_key: str | None = None) -> float:
        key = self._measure_observation_scale_bar_value_key(is_field)
        if key:
            raw = SettingsDB.get_setting(key)
            try:
                value = float(raw)
            except (TypeError, ValueError):
                value = 0.0
            if value > 0:
                return value
        if is_field:
            return max(0.1, float(self._scale_bar_overlay_field_mm))
        suggested_um = self._suggest_microscope_scale_bar_um_for_objective(
            objective_key if objective_key is not None else self.current_objective_name
        )
        return max(0.1, float(suggested_um))

    def _save_observation_scale_bar_fallback_value(self, value: float, is_field: bool) -> None:
        key = self._measure_observation_scale_bar_value_key(is_field)
        if not key:
            return
        try:
            SettingsDB.set_setting(key, str(max(0.1, float(value))))
        except Exception:
            pass

    def _default_measure_view_settings(self) -> dict:
        is_field = (self.current_image_type or "").strip().lower() == "field"
        return {
            "show_labels": self._measure_view_setting_enabled(self.SETTING_MEASURE_SHOW_LABELS, default=True),
            "show_overlays": self._measure_view_setting_enabled(self.SETTING_MEASURE_SHOW_OVERLAYS, default=True),
            "show_scale_bar": self._measure_view_setting_enabled(self.SETTING_MEASURE_SHOW_SCALE_BAR, default=False),
            "show_copyright": self._measure_view_setting_enabled(self.SETTING_MEASURE_SHOW_COPYRIGHT, default=False),
            "rectangle_style": normalize_rectangle_style(
                self._measure_view_setting_text(self.SETTING_MEASURE_RECTANGLE_STYLE, DEFAULT_RECTANGLE_STYLE)
            ),
            "rectangle_thickness": clamp_rectangle_thickness(
                self._measure_view_setting_text(
                    self.SETTING_MEASURE_RECTANGLE_THICKNESS,
                    str(int(DEFAULT_RECTANGLE_THICKNESS)),
                )
            ),
            "scale_bar_value": self._observation_scale_bar_fallback_value(
                is_field,
                objective_key=self.current_objective_name,
            ),
            "scale_bar_value_custom": False,
        }

    def _load_measure_view_settings_for_image(self, image_id: int | None = None) -> dict:
        settings = self._default_measure_view_settings()
        key = self._measure_image_view_settings_key(image_id if image_id is not None else self.current_image_id)
        if not key:
            return settings
        raw = SettingsDB.get_setting(key)
        if not raw:
            return settings
        try:
            loaded = json.loads(raw)
        except Exception:
            return settings
        if not isinstance(loaded, dict):
            return settings
        for key_name in ("show_labels", "show_overlays", "show_scale_bar", "show_copyright"):
            if key_name in loaded:
                settings[key_name] = bool(loaded.get(key_name))
        if "rectangle_style" in loaded:
            settings["rectangle_style"] = normalize_rectangle_style(loaded.get("rectangle_style"))
        if "rectangle_thickness" in loaded:
            settings["rectangle_thickness"] = clamp_rectangle_thickness(loaded.get("rectangle_thickness"))
        scale_value = loaded.get("scale_bar_value")
        scale_custom = loaded.get("scale_bar_value_custom")
        if isinstance(scale_value, (int, float)) and scale_value > 0:
            settings["scale_bar_value"] = float(scale_value)
            settings["scale_bar_value_custom"] = bool(scale_custom) if scale_custom is not None else True
        return settings

    def _collect_current_measure_view_settings(self) -> dict:
        defaults = self._default_measure_view_settings()
        settings = {
            "show_labels": bool(self.show_measures_checkbox.isChecked()) if hasattr(self, "show_measures_checkbox") else defaults["show_labels"],
            "show_overlays": bool(self.show_rectangles_checkbox.isChecked()) if hasattr(self, "show_rectangles_checkbox") else defaults["show_overlays"],
            "show_scale_bar": bool(self.show_scale_bar_checkbox.isChecked()) if hasattr(self, "show_scale_bar_checkbox") else defaults["show_scale_bar"],
            "show_copyright": bool(self.show_copyright_checkbox.isChecked()) if hasattr(self, "show_copyright_checkbox") else defaults["show_copyright"],
            "rectangle_style": self._current_measure_rectangle_style() if hasattr(self, "rectangle_style_a_radio") else defaults["rectangle_style"],
            "rectangle_thickness": self._current_measure_rectangle_thickness() if hasattr(self, "rectangle_thick_checkbox") else defaults["rectangle_thickness"],
        }
        if bool(getattr(self, "_current_measure_scale_bar_value_custom", False)) and hasattr(self, "scale_bar_input"):
            settings["scale_bar_value"] = max(0.1, float(self.scale_bar_input.value()))
            settings["scale_bar_value_custom"] = True
        return settings

    def _save_current_image_measure_session_view(self) -> None:
        """Remember zoom/pan for the current image until the app closes."""
        image_id = int(self.current_image_id or 0)
        if image_id <= 0 or not hasattr(self, "image_label"):
            return
        state = self.image_label.get_view_state()
        if not isinstance(state, dict):
            self._measure_session_view_states.pop(image_id, None)
            return
        center = state.get("center")
        zoom = state.get("zoom")
        size = state.get("size")
        if center is None or zoom is None or not isinstance(size, (tuple, list)) or len(size) != 2:
            return
        self._measure_session_view_states[image_id] = {
            "center": (float(center.x()), float(center.y())),
            "zoom": float(zoom),
            "size": (int(size[0]), int(size[1])),
        }

    def _apply_measure_session_view_for_current_image(self) -> bool:
        """Restore session-only zoom/pan for the current image if available."""
        image_id = int(self.current_image_id or 0)
        if image_id <= 0 or not hasattr(self, "image_label") or not self.current_pixmap:
            return False
        state = self._measure_session_view_states.get(image_id)
        if not isinstance(state, dict):
            return False
        center = state.get("center")
        zoom = state.get("zoom")
        size = state.get("size")
        if (
            not isinstance(center, (tuple, list))
            or len(center) != 2
            or not isinstance(size, (tuple, list))
            or len(size) != 2
            or int(size[0]) != self.current_pixmap.width()
            or int(size[1]) != self.current_pixmap.height()
        ):
            self._measure_session_view_states.pop(image_id, None)
            return False
        try:
            self.image_label.set_view_state(QPointF(float(center[0]), float(center[1])), float(zoom))
        except Exception:
            self._measure_session_view_states.pop(image_id, None)
            return False
        return True

    def _save_current_image_measure_view_settings(self) -> None:
        key = self._measure_image_view_settings_key()
        if not key:
            return
        try:
            SettingsDB.set_setting(key, json.dumps(self._collect_current_measure_view_settings()))
        except Exception:
            pass

    def _apply_measure_view_settings_for_current_image(self) -> None:
        settings = self._load_measure_view_settings_for_image()
        self._current_measure_scale_bar_value_custom = bool(settings.get("scale_bar_value_custom", False))
        has_scale = self._current_image_has_scale()
        force_disable_overlays = bool((self.current_image_type or "").strip().lower() == "field" and not has_scale)

        if hasattr(self, "show_measures_checkbox"):
            self.show_measures_checkbox.blockSignals(True)
            self.show_measures_checkbox.setChecked(bool(settings.get("show_labels", True)) and not force_disable_overlays)
            self.show_measures_checkbox.blockSignals(False)
        if hasattr(self, "show_rectangles_checkbox"):
            self.show_rectangles_checkbox.blockSignals(True)
            self.show_rectangles_checkbox.setChecked(bool(settings.get("show_overlays", True)) and not force_disable_overlays)
            self.show_rectangles_checkbox.blockSignals(False)
        if hasattr(self, "show_scale_bar_checkbox"):
            self.show_scale_bar_checkbox.blockSignals(True)
            self.show_scale_bar_checkbox.setChecked(bool(settings.get("show_scale_bar", False)) and has_scale and not force_disable_overlays)
            self.show_scale_bar_checkbox.blockSignals(False)
        if hasattr(self, "show_copyright_checkbox"):
            self.show_copyright_checkbox.blockSignals(True)
            self.show_copyright_checkbox.setChecked(bool(settings.get("show_copyright", False)))
            self.show_copyright_checkbox.blockSignals(False)
        if hasattr(self, "scale_bar_input"):
            self.scale_bar_input.blockSignals(True)
            self.scale_bar_input.setValue(max(0.1, float(settings.get("scale_bar_value", 10.0))))
            self.scale_bar_input.blockSignals(False)
        rectangle_style = normalize_rectangle_style(settings.get("rectangle_style", DEFAULT_RECTANGLE_STYLE))
        if hasattr(self, "rectangle_style_a_radio") and hasattr(self, "rectangle_style_b_radio"):
            self.rectangle_style_a_radio.blockSignals(True)
            self.rectangle_style_b_radio.blockSignals(True)
            self.rectangle_style_a_radio.setChecked(rectangle_style == "a")
            self.rectangle_style_b_radio.setChecked(rectangle_style == "b")
            self.rectangle_style_a_radio.blockSignals(False)
            self.rectangle_style_b_radio.blockSignals(False)
        rectangle_thickness = int(round(clamp_rectangle_thickness(settings.get("rectangle_thickness", DEFAULT_RECTANGLE_THICKNESS))))
        if hasattr(self, "rectangle_thick_checkbox"):
            self.rectangle_thick_checkbox.blockSignals(True)
            self.rectangle_thick_checkbox.setChecked(rectangle_thickness >= 2)
            self.rectangle_thick_checkbox.blockSignals(False)

        if hasattr(self, "image_label"):
            self.image_label.set_show_measure_labels(
                bool(self.show_measures_checkbox.isChecked()) if hasattr(self, "show_measures_checkbox") else False
            )
            self.image_label.set_show_measure_overlays(
                bool(self.show_rectangles_checkbox.isChecked()) if hasattr(self, "show_rectangles_checkbox") else False
            )
        self._apply_measure_rectangle_appearance(refresh_gallery=False)
        self.on_show_scale_bar_toggled(
            bool(self.show_scale_bar_checkbox.isChecked()) if hasattr(self, "show_scale_bar_checkbox") else False
        )
        self._update_measure_copyright_overlay()
        self._update_measure_view_option_states()

    def create_measure_tab(self):
        """Create the measure tab with control panel, image panel, and stats panel."""
        tab = QWidget()
        layout = QHBoxLayout(tab)
        layout.setSpacing(10)
        layout.setContentsMargins(0, 0, 0, 0)

        self.measure_tab = tab

        # Left panel - controls (fixed width)
        left_panel = self.create_control_panel()
        left_panel.setMaximumWidth(400)
        left_panel.setMinimumWidth(400)
        layout.addWidget(left_panel)

        # Center - image panel
        image_panel = self.create_image_panel()
        layout.addWidget(image_panel, 1)

        # Right panel - Stats and measurements table (fixed width)
        right_panel = self.create_right_panel()
        right_panel.setMaximumWidth(340)
        right_panel.setMinimumWidth(340)
        layout.addWidget(right_panel)

        self.next_image_shortcut = QShortcut(QKeySequence(Qt.Key_N), tab)
        self.next_image_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.next_image_shortcut.activated.connect(self.goto_next_image)
        self.next_image_arrow_shortcut = QShortcut(QKeySequence(Qt.Key_Right), tab)
        self.next_image_arrow_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.next_image_arrow_shortcut.activated.connect(self.goto_next_image)

        self.prev_image_shortcut = QShortcut(QKeySequence(Qt.Key_P), tab)
        self.prev_image_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.prev_image_shortcut.activated.connect(self.goto_previous_image)
        self.prev_image_arrow_shortcut = QShortcut(QKeySequence(Qt.Key_Left), tab)
        self.prev_image_arrow_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.prev_image_arrow_shortcut.activated.connect(self.goto_previous_image)

        self.toggle_measurement_shortcut = QShortcut(QKeySequence(Qt.Key_M), tab)
        self.toggle_measurement_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.toggle_measurement_shortcut.activated.connect(self._on_measure_button_clicked)

        self.cancel_measurement_shortcut = QShortcut(QKeySequence(Qt.Key_Escape), tab)
        self.cancel_measurement_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.cancel_measurement_shortcut.activated.connect(self._cancel_measurement_shortcut)

        self.delete_measurement_shortcuts = []
        for seq in (QKeySequence(Qt.Key_Delete), QKeySequence("Alt+D"), QKeySequence("Meta+D")):
            shortcut = QShortcut(seq, tab)
            shortcut.setContext(Qt.WidgetWithChildrenShortcut)
            shortcut.activated.connect(self._delete_selected_measurement_shortcut)
            self.delete_measurement_shortcuts.append(shortcut)

        self.start_measurement()
        return tab

    def create_image_panel(self):
        """Create the image panel with zoomable image."""
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)

        self.image_label = ZoomableImageLabel()
        self.image_label.setObjectName("imageLabel")
        self.image_label.setMinimumSize(800, 400)
        self.image_label.clicked.connect(self.image_clicked)
        self.image_label.rightClicked.connect(self.image_right_clicked)
        self.image_label.set_measurement_color(self.measure_color)
        self.image_label.set_measurement_active(self.measurement_active)
        self.image_label.set_pan_without_shift(not self.measurement_active)
        if hasattr(self, "show_measures_checkbox"):
            self.image_label.set_show_measure_labels(self.show_measures_checkbox.isChecked())
        if hasattr(self, "show_rectangles_checkbox"):
            self.image_label.set_show_measure_overlays(self.show_rectangles_checkbox.isChecked())
        self.image_label.set_measurement_rectangle_appearance(
            style=self._current_measure_rectangle_style(),
            thickness=self._current_measure_rectangle_thickness(),
        )
        self._update_measure_copyright_overlay()

        self.measure_gallery = ImageGalleryWidget(
            self.tr("Images"),
            self,
            show_delete=True,
            show_badges=True,
            min_height=50,
            default_height=220,
            show_publish_checkbox=True,
            publish_checkbox_hint=self.tr("Select image for online publishing"),
        )
        self.measure_gallery.set_multi_select(True)
        self.measure_gallery.imageClicked.connect(self._on_measure_gallery_clicked)
        self.measure_gallery.deleteRequested.connect(self._on_measure_gallery_delete_requested)
        self.measure_gallery.publishSelectionChanged.connect(self._on_measure_gallery_publish_selection_changed)

        splitter = QSplitter(Qt.Vertical)
        splitter.setChildrenCollapsible(False)
        splitter.addWidget(self.image_label)
        splitter.addWidget(self.measure_gallery)
        splitter.setStretchFactor(0, 4)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([700, 220])

        layout.addWidget(splitter)
        return panel

    def create_gallery_panel(self):
        """Create the gallery panel showing all measured spores in a grid."""
        from PySide6.QtWidgets import QScrollArea, QGridLayout, QFormLayout

        panel = QWidget()
        main_layout = QVBoxLayout(panel)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(8)

        main_splitter = QSplitter(Qt.Horizontal)
        main_splitter.setChildrenCollapsible(False)

        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 4, 0)
        left_layout.setSpacing(8)

        category_row = QHBoxLayout()
        self.gallery_category_label = QLabel(self.tr("Category:"))
        self.gallery_category_label.setToolTip(self.tr("Select the measurement category you want to plot"))
        category_row.addWidget(self.gallery_category_label)
        self.gallery_filter_combo = QComboBox()
        self.gallery_filter_combo.setFixedWidth(190)
        self.gallery_filter_combo.currentIndexChanged.connect(self.on_gallery_thumbnail_setting_changed)
        category_row.addWidget(self.gallery_filter_combo)
        category_row.addStretch()
        left_layout.addLayout(category_row)

        self.gallery_plot_settings = {
            "bins": 8,
            "histogram": True,
            "plot_style": "ellipse",
            "ellipse_coverage_percent": 95,
            "ci": True,
            "kde": False,
            "kde_bandwidth": 1.0,
            "kde_contours": 6,
            "kde_coverage_percent": 95,
            "comparison_mode": "range",
            "reference_minmax": True,
            "reference_ci": True,
            "reference_shape": "ellipse",
            "image_color": False,
            "legend": False,
            "avg_q": False,
            "q_minmax": False,
            "q_extreme_minmax": False,
            "axis_equal": False,
            "x_min": None,
            "x_max": None,
            "y_min": None,
            "y_max": None,
        }

        plot_panel = QWidget()
        plot_layout = QFormLayout(plot_panel)
        plot_layout.setContentsMargins(8, 8, 8, 8)
        plot_layout.setSpacing(6)

        histogram_row = QWidget()
        histogram_row_layout = QHBoxLayout(histogram_row)
        histogram_row_layout.setContentsMargins(0, 0, 0, 0)
        histogram_row_layout.setSpacing(6)
        self.gallery_hist_checkbox = QCheckBox(self.tr("Histogram"))
        self.gallery_hist_checkbox.setChecked(bool(self.gallery_plot_settings.get("histogram", True)))
        self.gallery_hist_checkbox.stateChanged.connect(self.on_gallery_plot_setting_changed)
        histogram_row_layout.addWidget(self.gallery_hist_checkbox)
        self.gallery_bins_label = QLabel(self.tr("Bins:"))
        histogram_row_layout.addWidget(self.gallery_bins_label)
        self.gallery_bins_spin = QSpinBox()
        self.gallery_bins_spin.setRange(3, 50)
        self.gallery_bins_spin.setValue(int(self.gallery_plot_settings.get("bins", 8)))
        self.gallery_bins_spin.valueChanged.connect(self.on_gallery_plot_setting_changed)
        histogram_row_layout.addWidget(self.gallery_bins_spin)
        histogram_row_layout.addStretch()
        plot_layout.addRow("", histogram_row)

        plot_style_row = QWidget()
        plot_style_layout = QHBoxLayout(plot_style_row)
        plot_style_layout.setContentsMargins(0, 0, 0, 0)
        plot_style_layout.setSpacing(8)
        plot_style_label = QLabel(self.tr("Plot:"))
        plot_style_layout.addWidget(plot_style_label)
        self.gallery_plot_style_group = QButtonGroup(self)
        self.gallery_plot_style_ellipse_radio = QRadioButton(self.tr("Ellipse"))
        self.gallery_plot_style_kde_radio = QRadioButton(self.tr("Kernel density"))
        self.gallery_plot_style_mean_radio = QRadioButton(self.tr("Mean range"))
        for radio in (
            self.gallery_plot_style_ellipse_radio,
            self.gallery_plot_style_kde_radio,
            self.gallery_plot_style_mean_radio,
        ):
            self.gallery_plot_style_group.addButton(radio)
            plot_style_layout.addWidget(radio)
        plot_style_layout.addStretch()
        plot_layout.addRow("", plot_style_row)

        plot_style = self._gallery_plot_style(self.gallery_plot_settings)
        if plot_style == "kde":
            self.gallery_plot_style_kde_radio.setChecked(True)
        elif plot_style == "mean":
            self.gallery_plot_style_mean_radio.setChecked(True)
        else:
            self.gallery_plot_style_ellipse_radio.setChecked(True)
        for radio in (
            self.gallery_plot_style_ellipse_radio,
            self.gallery_plot_style_kde_radio,
            self.gallery_plot_style_mean_radio,
        ):
            radio.toggled.connect(self.on_gallery_plot_setting_changed)

        self._register_gallery_hint_widget(
            self.gallery_plot_style_ellipse_radio,
            self.tr("Show data ellipses for the current specimen and any spore-point reference sets. Coverage is set by the slider below."),
        )
        self._register_gallery_hint_widget(
            self.gallery_plot_style_kde_radio,
            self.tr("Kernel density estimate (Gaussian KDE) of the spore cloud. Filled bands and contour labels show enclosed probability mass."),
        )
        self._register_gallery_hint_widget(
            self.gallery_plot_style_mean_radio,
            self.tr("Parmasto-style mean comparison: show the mean point, mean Q line, and the expected mean range instead of the full spore cloud outline."),
        )

        self.gallery_ellipse_coverage_row = QWidget()
        ellipse_coverage_layout = QHBoxLayout(self.gallery_ellipse_coverage_row)
        ellipse_coverage_layout.setContentsMargins(0, 0, 0, 0)
        ellipse_coverage_layout.setSpacing(6)
        ellipse_coverage_layout.addSpacing(16)
        self.gallery_ellipse_coverage_label = QLabel(self.tr("Coverage:"))
        ellipse_coverage_layout.addWidget(self.gallery_ellipse_coverage_label)
        self.gallery_ellipse_coverage_slider = JumpSlider(Qt.Horizontal)
        self.gallery_ellipse_coverage_slider.setRange(50, 99)
        self.gallery_ellipse_coverage_slider.setSingleStep(1)
        self.gallery_ellipse_coverage_slider.setPageStep(5)
        self.gallery_ellipse_coverage_slider.setTracking(False)
        self.gallery_ellipse_coverage_slider.setValue(int(self.gallery_plot_settings.get("ellipse_coverage_percent", 95)))
        self.gallery_ellipse_coverage_slider.valueChanged.connect(self.on_gallery_plot_setting_changed)
        self.gallery_ellipse_coverage_slider.sliderMoved.connect(
            lambda value: self.gallery_ellipse_coverage_value.setText(f"{int(value)}%")
        )
        ellipse_coverage_layout.addWidget(self.gallery_ellipse_coverage_slider, 1)
        self.gallery_ellipse_coverage_value = QLabel()
        self.gallery_ellipse_coverage_value.setMinimumWidth(40)
        ellipse_coverage_layout.addWidget(self.gallery_ellipse_coverage_value)
        plot_layout.addRow("", self.gallery_ellipse_coverage_row)

        self._register_gallery_hint_widget(
            self.gallery_ellipse_coverage_slider,
            self.tr("Coverage of the data ellipse in percent. The same percentage is written on the ellipse itself."),
        )
        self._register_gallery_hint_widget(
            self.gallery_ellipse_coverage_label,
            self.tr("Coverage of the data ellipse in percent. The same percentage is written on the ellipse itself."),
        )

        self.gallery_kde_bandwidth_row = QWidget()
        kde_bandwidth_layout = QHBoxLayout(self.gallery_kde_bandwidth_row)
        kde_bandwidth_layout.setContentsMargins(0, 0, 0, 0)
        kde_bandwidth_layout.setSpacing(6)
        kde_bandwidth_layout.addSpacing(16)
        self.gallery_kde_bandwidth_label = QLabel(self.tr("Bandwidth:"))
        kde_bandwidth_layout.addWidget(self.gallery_kde_bandwidth_label)
        self.gallery_kde_bandwidth_slider = JumpSlider(Qt.Horizontal)
        self.gallery_kde_bandwidth_slider.setRange(50, 150)
        self.gallery_kde_bandwidth_slider.setSingleStep(1)
        self.gallery_kde_bandwidth_slider.setPageStep(5)
        self.gallery_kde_bandwidth_slider.setTracking(False)
        self.gallery_kde_bandwidth_slider.setValue(
            int(round(float(self.gallery_plot_settings.get("kde_bandwidth", 1.0)) * 100.0))
        )
        self.gallery_kde_bandwidth_slider.valueChanged.connect(self.on_gallery_plot_setting_changed)
        self.gallery_kde_bandwidth_slider.sliderMoved.connect(
            lambda value: self.gallery_kde_bandwidth_value.setText(f"{max(0.5, min(1.5, float(value) / 100.0)):.2f}")
        )
        kde_bandwidth_layout.addWidget(self.gallery_kde_bandwidth_slider, 1)
        self.gallery_kde_bandwidth_value = QLabel()
        self.gallery_kde_bandwidth_value.setMinimumWidth(36)
        kde_bandwidth_layout.addWidget(self.gallery_kde_bandwidth_value)
        plot_layout.addRow("", self.gallery_kde_bandwidth_row)

        self.gallery_kde_contours_row = QWidget()
        kde_contours_layout = QHBoxLayout(self.gallery_kde_contours_row)
        kde_contours_layout.setContentsMargins(0, 0, 0, 0)
        kde_contours_layout.setSpacing(6)
        kde_contours_layout.addSpacing(16)
        self.gallery_kde_contours_label = QLabel(self.tr("Contours:"))
        kde_contours_layout.addWidget(self.gallery_kde_contours_label)
        self.gallery_kde_contours_slider = JumpSlider(Qt.Horizontal)
        self.gallery_kde_contours_slider.setRange(1, 10)
        self.gallery_kde_contours_slider.setSingleStep(1)
        self.gallery_kde_contours_slider.setPageStep(1)
        self.gallery_kde_contours_slider.setTracking(False)
        self.gallery_kde_contours_slider.setValue(int(self.gallery_plot_settings.get("kde_contours", 6)))
        self.gallery_kde_contours_slider.valueChanged.connect(self.on_gallery_plot_setting_changed)
        self.gallery_kde_contours_slider.sliderMoved.connect(
            lambda value: self.gallery_kde_contours_value.setText(str(int(value)))
        )
        kde_contours_layout.addWidget(self.gallery_kde_contours_slider, 1)
        self.gallery_kde_contours_value = QLabel()
        self.gallery_kde_contours_value.setMinimumWidth(24)
        kde_contours_layout.addWidget(self.gallery_kde_contours_value)
        plot_layout.addRow("", self.gallery_kde_contours_row)

        self.gallery_kde_coverage_row = QWidget()
        kde_coverage_layout = QHBoxLayout(self.gallery_kde_coverage_row)
        kde_coverage_layout.setContentsMargins(0, 0, 0, 0)
        kde_coverage_layout.setSpacing(6)
        kde_coverage_layout.addSpacing(16)
        self.gallery_kde_coverage_label = QLabel(self.tr("Coverage:"))
        kde_coverage_layout.addWidget(self.gallery_kde_coverage_label)
        self.gallery_kde_coverage_slider = JumpSlider(Qt.Horizontal)
        self.gallery_kde_coverage_slider.setRange(50, 99)
        self.gallery_kde_coverage_slider.setSingleStep(1)
        self.gallery_kde_coverage_slider.setPageStep(5)
        self.gallery_kde_coverage_slider.setTracking(False)
        self.gallery_kde_coverage_slider.setValue(int(self.gallery_plot_settings.get("kde_coverage_percent", 95)))
        self.gallery_kde_coverage_slider.valueChanged.connect(self.on_gallery_plot_setting_changed)
        self.gallery_kde_coverage_slider.sliderMoved.connect(
            lambda value: self.gallery_kde_coverage_value.setText(f"{int(value)}%")
        )
        kde_coverage_layout.addWidget(self.gallery_kde_coverage_slider, 1)
        self.gallery_kde_coverage_value = QLabel()
        self.gallery_kde_coverage_value.setMinimumWidth(40)
        kde_coverage_layout.addWidget(self.gallery_kde_coverage_value)
        plot_layout.addRow("", self.gallery_kde_coverage_row)

        self._register_gallery_hint_widget(
            self.gallery_kde_bandwidth_slider,
            self.tr("Bandwidth controls KDE smoothing. Lower values follow local bumps more closely; higher values smooth the density into broader regions."),
        )
        self._register_gallery_hint_widget(
            self.gallery_kde_bandwidth_label,
            self.tr("Bandwidth controls KDE smoothing. Lower values follow local bumps more closely; higher values smooth the density into broader regions."),
        )
        self._register_gallery_hint_widget(
            self.gallery_kde_contours_slider,
            self.tr("Number of KDE contour rings. The rings are evenly spaced enclosed-mass levels up to the selected coverage."),
        )
        self._register_gallery_hint_widget(
            self.gallery_kde_contours_label,
            self.tr("Number of KDE contour rings. The rings are evenly spaced enclosed-mass levels up to the selected coverage."),
        )
        self._register_gallery_hint_widget(
            self.gallery_kde_coverage_slider,
            self.tr("Coverage of the outer KDE contour in percent. Contour labels show the enclosed density mass for each ring."),
        )
        self._register_gallery_hint_widget(
            self.gallery_kde_coverage_label,
            self.tr("Coverage of the outer KDE contour in percent. Contour labels show the enclosed density mass for each ring."),
        )

        self.gallery_image_color_checkbox = QCheckBox(self.tr("Image color"))
        self.gallery_image_color_checkbox.setChecked(bool(self.gallery_plot_settings.get("image_color", False)))
        self.gallery_image_color_checkbox.stateChanged.connect(self.on_gallery_plot_setting_changed)
        plot_layout.addRow("", self.gallery_image_color_checkbox)

        self.gallery_avg_q_checkbox = QCheckBox(self.tr("Plot Avg Q"))
        self.gallery_avg_q_checkbox.setChecked(bool(self.gallery_plot_settings.get("avg_q", False)))
        self.gallery_avg_q_checkbox.stateChanged.connect(self.on_gallery_plot_setting_changed)
        plot_layout.addRow("", self.gallery_avg_q_checkbox)

        self.gallery_q_minmax_checkbox = QCheckBox(self.tr("Plot Q 90% range (5%-95%)"))
        self.gallery_q_minmax_checkbox.setToolTip(self.tr("Show Q lines for the 5th to 95th percentile range"))
        self.gallery_q_minmax_checkbox.setChecked(bool(self.gallery_plot_settings.get("q_minmax", False)))
        self.gallery_q_minmax_checkbox.stateChanged.connect(self.on_gallery_plot_setting_changed)
        plot_layout.addRow("", self.gallery_q_minmax_checkbox)

        self.gallery_q_extreme_minmax_checkbox = QCheckBox(self.tr("Plot Q min/max"))
        self.gallery_q_extreme_minmax_checkbox.setToolTip(self.tr("Show Q lines for the true minimum and maximum values"))
        self.gallery_q_extreme_minmax_checkbox.setChecked(bool(self.gallery_plot_settings.get("q_extreme_minmax", False)))
        self.gallery_q_extreme_minmax_checkbox.stateChanged.connect(self.on_gallery_plot_setting_changed)
        plot_layout.addRow("", self.gallery_q_extreme_minmax_checkbox)

        self.gallery_axis_equal_checkbox = QCheckBox(self.tr("Axis equal"))
        self.gallery_axis_equal_checkbox.setToolTip(self.tr("Use the same scale on X and Y axes"))
        self.gallery_axis_equal_checkbox.setChecked(bool(self.gallery_plot_settings.get("axis_equal", False)))
        self.gallery_axis_equal_checkbox.stateChanged.connect(self.on_gallery_plot_setting_changed)
        plot_layout.addRow("", self.gallery_axis_equal_checkbox)

        self._sync_gallery_kde_controls()
        plot_section = CollapsibleSection(self.tr("Plot settings"), plot_panel, expanded=False)
        reference_section = CollapsibleSection(self.tr("Reference values"), self._build_reference_panel(), expanded=True)
        plot_section.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Maximum)
        reference_section.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        gallery_group = QGroupBox(self.tr("Gallery"))
        gallery_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Maximum)
        gallery_controls = QVBoxLayout(gallery_group)
        gallery_controls.setContentsMargins(8, 8, 8, 8)
        gallery_controls.setSpacing(6)

        gallery_row = QHBoxLayout()
        self.orient_checkbox = QCheckBox(self.tr("Orient"))
        self.orient_checkbox.setToolTip(self.tr("Rotate thumbnails so length axis is vertical"))
        self.orient_checkbox.stateChanged.connect(self.on_gallery_thumbnail_setting_changed)
        gallery_row.addWidget(self.orient_checkbox)

        self.uniform_scale_checkbox = QCheckBox(self.tr("Uniform scale"))
        self.uniform_scale_checkbox.setToolTip(self.tr("Use the same scale for all thumbnails"))
        self.uniform_scale_checkbox.stateChanged.connect(self.on_gallery_thumbnail_setting_changed)
        gallery_row.addWidget(self.uniform_scale_checkbox)

        gallery_row.addSpacing(8)
        gallery_row.addWidget(QLabel(self.tr("Sort:")))
        self.gallery_sort_combo = QComboBox()
        self.gallery_sort_combo.addItem(self.tr(""), "")
        self.gallery_sort_combo.addItem(self.tr("Images"), "images")
        self.gallery_sort_combo.addItem(self.tr("Width"), "width")
        self.gallery_sort_combo.addItem(self.tr("Length"), "length")
        self.gallery_sort_combo.addItem(self.tr("Q"), "q")
        self.gallery_sort_combo.setToolTip(self.tr("Sort thumbnails from smallest to largest"))
        self.gallery_sort_combo.currentIndexChanged.connect(self.on_gallery_thumbnail_setting_changed)
        gallery_row.addWidget(self.gallery_sort_combo)
        gallery_row.addStretch()
        gallery_controls.addLayout(gallery_row)

        self.gallery_filter_label = QLabel("")
        self.gallery_filter_label.setStyleSheet(f"color: #7f8c8d; font-size: {pt(9)}pt;")
        gallery_controls.addWidget(self.gallery_filter_label)

        top_sections = QWidget()
        top_sections.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Maximum)
        top_sections_layout = QVBoxLayout(top_sections)
        top_sections_layout.setContentsMargins(0, 0, 0, 0)
        top_sections_layout.setSpacing(8)
        top_sections_layout.addWidget(plot_section)
        top_sections_layout.addWidget(reference_section, 1)
        top_sections_layout.addWidget(self._build_spore_sharing_panel())

        left_layout.addWidget(top_sections, 0)
        left_layout.addStretch(1)
        left_layout.addWidget(gallery_group)

        analysis_button_height = max(35, QPushButton(self.tr("Plot")).sizeHint().height())

        self.gallery_plot_export_btn = QPushButton(self.tr("Export Plot"))
        self.gallery_plot_export_btn.setMinimumWidth(110)
        self.gallery_plot_export_btn.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.gallery_plot_export_btn.setToolTip(self.tr("Export the width vs length plot"))
        self.gallery_plot_export_btn.clicked.connect(self.export_graph_plot_svg)

        self.gallery_export_btn = QPushButton(self.tr("Export gallery"))
        self.gallery_export_btn.setMinimumWidth(110)
        self.gallery_export_btn.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.gallery_export_btn.setToolTip(self.tr("Export the thumbnail gallery as a mosaic"))
        self.gallery_export_btn.clicked.connect(self.export_gallery_composite)
        self.gallery_plot_export_btn.setFixedHeight(analysis_button_height)
        self.gallery_export_btn.setFixedHeight(analysis_button_height)
        gallery_export_row = QHBoxLayout()
        gallery_export_row.setContentsMargins(0, 0, 0, 0)
        gallery_export_row.setSpacing(0)
        gallery_export_row.addWidget(self.gallery_export_btn)
        gallery_export_row.addStretch()
        gallery_controls.addLayout(gallery_export_row)

        self.gallery_copy_stats_btn = QPushButton(self.tr("Export statistics"))
        self.gallery_copy_stats_btn.setToolTip(self.tr("Copy spore statistics and individual measurements to the clipboard"))
        self.gallery_copy_stats_btn.clicked.connect(self.copy_spore_stats)

        self.gallery_save_stats_btn = QPushButton(self.tr("Save statistics"))
        self.gallery_save_stats_btn.setToolTip(self.tr("Save spore statistics and individual measurements to a text file"))
        self.gallery_save_stats_btn.clicked.connect(self.save_spore_stats)
        self._sync_gallery_histogram_controls()

        left_panel.setMinimumWidth(400)
        left_panel.setMaximumWidth(400)
        left_scroll = QScrollArea()
        left_scroll.setWidgetResizable(True)
        left_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        left_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        left_scroll.setFrameShape(QFrame.NoFrame)
        left_scroll.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Expanding)
        left_scroll.setWidget(left_panel)
        left_scroll.setMinimumWidth(420)
        left_scroll.setMaximumWidth(420)

        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(8)

        self.gallery_splitter = CollapsibleSplitter(Qt.Vertical, collapse_index=1)
        self.gallery_splitter.setChildrenCollapsible(True)
        self.gallery_splitter.collapse_toggled.connect(self._on_gallery_collapse_toggled)
        self.gallery_splitter.splitterMoved.connect(self._on_gallery_splitter_moved)

        plot_panel = QWidget()
        plot_layout = QVBoxLayout(plot_panel)
        plot_layout.setContentsMargins(0, 0, 0, 0)
        plot_layout.setSpacing(6)

        plot_hint_row = QHBoxLayout()
        plot_hint_row.addWidget(self.gallery_plot_export_btn)
        self.gallery_clear_filter_btn = QPushButton(self.tr("Clear filter"))
        self.gallery_clear_filter_btn.setFixedHeight(analysis_button_height)
        self.gallery_clear_filter_btn.clicked.connect(self.clear_gallery_filter)
        plot_hint_row.addWidget(self.gallery_clear_filter_btn)
        self.gallery_reset_plot_btn = QPushButton(self.tr("Reset plot"))
        self.gallery_reset_plot_btn.setFixedHeight(analysis_button_height)
        self.gallery_reset_plot_btn.clicked.connect(self.reset_gallery_plot_view)
        plot_hint_row.addWidget(self.gallery_reset_plot_btn)
        plot_hint_row.addStretch()
        self.gallery_include_details_checkbox = QCheckBox(self.tr("Include details"))
        self.gallery_include_details_checkbox.setChecked(False)
        self.gallery_include_details_checkbox.toggled.connect(self.on_gallery_stats_setting_changed)
        plot_hint_row.addWidget(self.gallery_include_details_checkbox)
        for button in (self.gallery_copy_stats_btn, self.gallery_save_stats_btn):
            button.setFixedHeight(analysis_button_height)
            button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
            plot_hint_row.addWidget(button)
        plot_layout.addLayout(plot_hint_row)

        self.gallery_plot_figure = Figure(figsize=(6, 3.8))
        self.gallery_plot_canvas = FigureCanvas(self.gallery_plot_figure)
        self.gallery_plot_canvas.mpl_connect("pick_event", self.on_gallery_plot_pick)
        self.gallery_plot_canvas.mpl_connect("scroll_event", self._on_gallery_plot_scroll)
        self.gallery_plot_canvas.mpl_connect("button_press_event", self._on_gallery_plot_press)
        self.gallery_plot_canvas.mpl_connect("motion_notify_event", self._on_gallery_plot_motion)
        self.gallery_plot_canvas.mpl_connect("button_release_event", self._on_gallery_plot_release)
        self.gallery_plot_canvas.mpl_connect("figure_leave_event", self._on_gallery_plot_leave)

        plot_canvas_frame = QFrame()
        plot_canvas_frame.setFrameShape(QFrame.StyledPanel)
        plot_canvas_frame.setLineWidth(1)
        plot_canvas_layout = QVBoxLayout(plot_canvas_frame)
        plot_canvas_layout.setContentsMargins(0, 0, 0, 0)
        plot_canvas_layout.setSpacing(0)
        plot_canvas_layout.addWidget(self.gallery_plot_canvas)

        self.plot_width_splitter = QSplitter(Qt.Horizontal)
        self.plot_width_splitter.setHandleWidth(6)
        self.plot_width_splitter.addWidget(plot_canvas_frame)
        self.gallery_stats_preview = QPlainTextEdit()
        self.gallery_stats_preview.setReadOnly(True)
        self.gallery_stats_preview.setLineWrapMode(QPlainTextEdit.NoWrap)
        self.gallery_stats_preview.setMinimumWidth(180)
        self.gallery_stats_preview.setPlaceholderText(self.tr("No observation selected"))
        fixed_font = QFontDatabase.systemFont(QFontDatabase.FixedFont)
        fixed_font.setPointSize(max(9, fixed_font.pointSize()))
        self.gallery_stats_preview.setFont(fixed_font)
        self.gallery_stats_preview.setTabStopDistance(8 * self.gallery_stats_preview.fontMetrics().horizontalAdvance(" "))
        self.plot_width_splitter.addWidget(self.gallery_stats_preview)
        self.plot_width_splitter.setCollapsible(0, False)
        self.plot_width_splitter.setCollapsible(1, False)
        self.plot_width_splitter.setStretchFactor(0, 1)
        self.plot_width_splitter.setStretchFactor(1, 0)
        self.plot_width_splitter.setSizes([760, 280])
        self.plot_width_splitter.splitterMoved.connect(self._on_plot_width_splitter_moved)
        plot_layout.addWidget(self.plot_width_splitter)

        gallery_panel = QWidget()
        gallery_layout = QVBoxLayout(gallery_panel)
        gallery_layout.setContentsMargins(0, 0, 0, 0)
        gallery_layout.setSpacing(6)

        gallery_toolbar = QHBoxLayout()
        gallery_toolbar.addStretch()
        gallery_layout.addLayout(gallery_toolbar)

        self.gallery_scroll = QScrollArea()
        self.gallery_scroll.setWidgetResizable(True)
        self.gallery_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.gallery_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

        self.gallery_container = QWidget()
        self.gallery_container.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
        self.gallery_grid = QGridLayout(self.gallery_container)
        self.gallery_grid.setContentsMargins(0, 0, 0, 0)
        self.gallery_grid.setSpacing(0)
        self.gallery_grid.setAlignment(Qt.AlignTop | Qt.AlignLeft)

        self.gallery_scroll.setWidget(self.gallery_container)
        gallery_layout.addWidget(self.gallery_scroll)

        self.gallery_splitter.addWidget(plot_panel)
        self.gallery_splitter.addWidget(gallery_panel)
        self.gallery_splitter.setStretchFactor(0, 1)
        self.gallery_splitter.setStretchFactor(1, 0)
        self.gallery_splitter.setSizes([650, 220])
        right_layout.addWidget(self.gallery_splitter)

        main_splitter.addWidget(left_scroll)
        main_splitter.addWidget(right_panel)
        main_splitter.setStretchFactor(0, 0)
        main_splitter.setStretchFactor(1, 1)
        main_splitter.setSizes([380, 905])

        main_layout.addWidget(main_splitter, 1)

        bottom_row = QHBoxLayout()
        bottom_row.setContentsMargins(0, 0, 0, 0)
        bottom_row.setSpacing(8)

        self.gallery_hint_bar = HintBar(self)
        bottom_row.addWidget(self.gallery_hint_bar, 1)
        self._gallery_hint_controller = HintStatusController(self.gallery_hint_bar, self)
        if self._pending_gallery_hint_widgets:
            for widget, hint, tone, allow_when_disabled in self._pending_gallery_hint_widgets:
                if widget:
                    self._gallery_hint_controller.register_widget(
                        widget,
                        hint,
                        tone=tone,
                        allow_when_disabled=allow_when_disabled,
                    )
            self._pending_gallery_hint_widgets.clear()

        main_layout.addLayout(bottom_row)

        self._set_gallery_strip_height()
        return panel

    def _register_gallery_hint_widget(
        self,
        widget: QWidget,
        hint_text: str | None,
        tone: str = "info",
        allow_when_disabled: bool = False,
    ) -> None:
        if not widget:
            return
        hint = (hint_text or "").strip()
        hint_tone = (tone or "info").strip().lower()
        if self._gallery_hint_controller is not None:
            self._gallery_hint_controller.register_widget(
                widget,
                hint,
                tone=hint_tone,
                allow_when_disabled=allow_when_disabled,
            )
            return
        widget.setProperty("_hint_text", hint)
        widget.setProperty("_hint_tone", hint_tone)
        widget.setProperty("_hint_allow_disabled", bool(allow_when_disabled))
        widget.setToolTip("")
        if not any(existing is widget for existing, _, _, _ in self._pending_gallery_hint_widgets):
            self._pending_gallery_hint_widgets.append((widget, hint, hint_tone, bool(allow_when_disabled)))

    def _set_gallery_hint(self, text: str | None, tone: str = "info") -> None:
        if self._gallery_hint_controller is not None:
            self._gallery_hint_controller.set_hint(text, tone=tone)

    def _queue_gallery_refresh_hint(self, text: str | None) -> None:
        self._gallery_pending_refresh_hint = (text or "").strip()

    def _consume_gallery_refresh_hint(self, fallback: str) -> str:
        text = (self._gallery_pending_refresh_hint or "").strip()
        self._gallery_pending_refresh_hint = ""
        return text or fallback

    def _set_gallery_busy_hint(self, text: str | None, tone: str = "info") -> None:
        self._gallery_busy_hint = (text or "").strip()
        if self._gallery_busy_hint:
            self._set_gallery_hint(self._gallery_busy_hint, tone=tone)
            QApplication.processEvents()
        elif not self._gallery_hover_hint_key:
            self._set_gallery_hint("")

    def _on_gallery_plot_motion(self, event) -> None:
        if self._gallery_busy_hint:
            return
        hint_key = ""
        if event is not None:
            hovered_axis = getattr(event, "inaxes", None)
            if hovered_axis is getattr(self, "_gallery_scatter_axis", None):
                hint_key = "scatter"
            elif hovered_axis in getattr(self, "_gallery_hist_axes", set()):
                hint_key = "hist"
            if (
                self._gallery_pan_active
                and hovered_axis is self._gallery_pan_axis
                and event.xdata is not None
                and event.ydata is not None
                and self._gallery_pan_start is not None
                and self._gallery_pan_xlim is not None
                and self._gallery_pan_ylim is not None
            ):
                start_x, start_y = self._gallery_pan_start
                dx = float(event.xdata) - float(start_x)
                dy = float(event.ydata) - float(start_y)
                self._gallery_pan_axis.set_xlim(
                    float(self._gallery_pan_xlim[0]) - dx,
                    float(self._gallery_pan_xlim[1]) - dx,
                )
                self._gallery_pan_axis.set_ylim(
                    float(self._gallery_pan_ylim[0]) - dy,
                    float(self._gallery_pan_ylim[1]) - dy,
                )
                if hasattr(self, "gallery_plot_canvas"):
                    self.gallery_plot_canvas.draw_idle()
                if abs(dx) > 1e-9 or abs(dy) > 1e-9:
                    self._gallery_pan_recently_dragged = True
        if hint_key == self._gallery_hover_hint_key:
            return
        self._gallery_hover_hint_key = hint_key
        if hint_key == "scatter":
            self._set_gallery_hint(
                self.tr("Click to filter image gallery")
            )
        elif hint_key == "hist":
            self._set_gallery_hint(
                self.tr("Click to filter image gallery")
            )
        else:
            self._set_gallery_hint("")

    def _on_gallery_plot_leave(self, _event) -> None:
        if self._gallery_busy_hint:
            return
        if self._gallery_hover_hint_key:
            self._gallery_hover_hint_key = ""
            self._set_gallery_hint("")

    def _store_gallery_plot_view(self) -> None:
        axis = getattr(self, "_gallery_scatter_axis", None)
        if axis is None:
            return
        try:
            x0, x1 = axis.get_xlim()
            y0, y1 = axis.get_ylim()
            x0 = float(x0)
            x1 = float(x1)
            y0 = float(y0)
            y1 = float(y1)
        except Exception:
            return
        settings = dict(getattr(self, "gallery_plot_settings", {}) or {})
        settings["x_min"] = x0
        settings["x_max"] = x1
        settings["y_min"] = y0
        settings["y_max"] = y1
        self.gallery_plot_settings = settings

    def reset_gallery_plot_view(self) -> None:
        settings = dict(getattr(self, "gallery_plot_settings", {}) or {})
        settings["x_min"] = None
        settings["x_max"] = None
        settings["y_min"] = None
        settings["y_max"] = None
        self.gallery_plot_settings = settings
        self.update_graph_plots_only()

    def _on_gallery_plot_scroll(self, event) -> None:
        axis = getattr(event, "inaxes", None)
        if axis is None or event.xdata is None or event.ydata is None:
            return
        direction = getattr(event, "button", "")
        if direction not in {"up", "down"}:
            return
        scale = 1 / 1.2 if direction == "up" else 1.2
        x0, x1 = axis.get_xlim()
        y0, y1 = axis.get_ylim()
        xdata = float(event.xdata)
        ydata = float(event.ydata)
        axis.set_xlim(
            xdata - (xdata - float(x0)) * scale,
            xdata + (float(x1) - xdata) * scale,
        )
        axis.set_ylim(
            ydata - (ydata - float(y0)) * scale,
            ydata + (float(y1) - ydata) * scale,
        )
        self._store_gallery_plot_view()
        if hasattr(self, "gallery_plot_canvas"):
            self.gallery_plot_canvas.draw_idle()

    def _on_gallery_plot_press(self, event) -> None:
        axis = getattr(event, "inaxes", None)
        if axis is None or getattr(event, "button", None) != 1:
            return
        if event.xdata is None or event.ydata is None:
            return
        self._gallery_pan_active = True
        self._gallery_pan_axis = axis
        self._gallery_pan_start = (float(event.xdata), float(event.ydata))
        self._gallery_pan_xlim = axis.get_xlim()
        self._gallery_pan_ylim = axis.get_ylim()
        self._gallery_pan_recently_dragged = False

    def _on_gallery_plot_release(self, event) -> None:
        if getattr(event, "button", None) == 1 and self._gallery_pan_active:
            dragged = bool(self._gallery_pan_recently_dragged)
            self._gallery_pan_active = False
            self._gallery_pan_axis = None
            self._gallery_pan_start = None
            self._gallery_pan_xlim = None
            self._gallery_pan_ylim = None
            if dragged:
                self._store_gallery_plot_view()
                QTimer.singleShot(180, lambda: setattr(self, "_gallery_pan_recently_dragged", False))
            else:
                self._gallery_pan_recently_dragged = False

    def _set_gallery_strip_height(self):
        if not hasattr(self, "gallery_splitter"):
            return
        gallery_widget = self.gallery_splitter.widget(1)
        rendered_height = max(
            self._gallery_tile_metrics()["image_height"],
            int(getattr(self, "_gallery_render_max_height", 0)),
        )
        target_height = rendered_height + 8
        if hasattr(self, "gallery_scroll") and self.gallery_scroll is not None:
            target_height += int(self.gallery_scroll.frameWidth()) * 2
        if gallery_widget and gallery_widget.layout() is not None:
            layout = gallery_widget.layout()
            target_height += layout.contentsMargins().top() + layout.contentsMargins().bottom()
            target_height += layout.spacing()
        if gallery_widget:
            gallery_widget.setMaximumHeight(target_height)
            gallery_widget.setMinimumHeight(0)
        sizes = self.gallery_splitter.sizes()
        total = sum(sizes) if sizes else 0
        if total <= 0:
            total = self.gallery_splitter.height()
        if total <= 0:
            return
        self.gallery_splitter.setSizes([max(0, total - target_height), target_height])

    def _gallery_thumbnail_size(self):
        return 200

    def _gallery_tile_metrics(self, thumbnail_size: int | None = None) -> dict[str, int]:
        image_height = int(thumbnail_size or self._gallery_thumbnail_size())
        icon_size = 18
        return {
            "image_height": image_height,
            "icon_size": icon_size,
        }

    def _gallery_export_grid_shape(
        self,
        count: int,
        tile_width: int,
        tile_height: int,
        target_ratio: float = 4.0 / 3.0,
    ) -> tuple[int, int]:
        if count <= 0:
            return 0, 0
        best_cols = 1
        best_rows = count
        best_score = None
        for cols in range(1, count + 1):
            rows = int(math.ceil(count / float(cols)))
            ratio = (float(cols) * float(tile_width)) / max(1.0, float(rows) * float(tile_height))
            ratio_diff = abs(ratio - float(target_ratio))
            empty_slots = rows * cols - count
            score = (ratio_diff, empty_slots, rows * cols)
            if best_score is None or score < best_score:
                best_score = score
                best_cols = cols
                best_rows = rows
        return best_cols, best_rows

    def _on_gallery_collapse_toggled(self, collapsed):
        self._gallery_collapsed = bool(collapsed)
        if collapsed:
            self._cancel_gallery_render()
            self._gallery_refresh_in_progress = False
        else:
            self._set_gallery_strip_height()
            self.schedule_gallery_refresh()

    def _sync_gallery_histogram_controls(self):
        enabled = bool(self.gallery_hist_checkbox.isChecked()) if hasattr(self, "gallery_hist_checkbox") else True
        if hasattr(self, "gallery_bins_spin"):
            self.gallery_bins_spin.setEnabled(enabled)
        if hasattr(self, "gallery_bins_label"):
            self.gallery_bins_label.setEnabled(enabled)

    def _gallery_kde_bandwidth_value(self, slider_value: int | None = None) -> float:
        if slider_value is None:
            slider = getattr(self, "gallery_kde_bandwidth_slider", None)
            slider_value = int(slider.value()) if slider is not None else 100
        return max(0.5, min(1.5, float(slider_value) / 100.0))

    def _gallery_plot_style(self, settings: dict | None = None) -> str:
        source = settings if isinstance(settings, dict) else (getattr(self, "gallery_plot_settings", {}) or {})
        style = str(source.get("plot_style") or "").strip().lower()
        if style in {"ellipse", "kde", "mean"}:
            return style
        comparison_mode = str(source.get("comparison_mode", "range") or "range").strip().lower()
        if comparison_mode == "mean":
            return "mean"
        if bool(source.get("kde", False)):
            return "kde"
        return "ellipse"

    def _gallery_plot_style_from_controls(self) -> str:
        if hasattr(self, "gallery_plot_style_kde_radio") and self.gallery_plot_style_kde_radio.isChecked():
            return "kde"
        if hasattr(self, "gallery_plot_style_mean_radio") and self.gallery_plot_style_mean_radio.isChecked():
            return "mean"
        return "ellipse"

    def _apply_gallery_plot_style(self, settings: dict, plot_style: str) -> dict:
        style = str(plot_style or "ellipse").strip().lower()
        if style not in {"ellipse", "kde", "mean"}:
            style = "ellipse"
        settings["plot_style"] = style
        settings["ci"] = style == "ellipse"
        settings["kde"] = style == "kde"
        settings["comparison_mode"] = "mean" if style == "mean" else "range"
        settings["reference_ci"] = style != "mean"
        return settings

    def _sync_gallery_kde_controls(self) -> None:
        plot_style = self._gallery_plot_style()
        enabled = plot_style == "kde"
        ellipse_enabled = plot_style == "ellipse"
        ellipse_row = getattr(self, "gallery_ellipse_coverage_row", None)
        if ellipse_row is not None:
            ellipse_row.setVisible(ellipse_enabled)
        row = getattr(self, "gallery_kde_bandwidth_row", None)
        if row is not None:
            row.setVisible(enabled)
        contours_row = getattr(self, "gallery_kde_contours_row", None)
        if contours_row is not None:
            contours_row.setVisible(enabled)
        coverage_row = getattr(self, "gallery_kde_coverage_row", None)
        if coverage_row is not None:
            coverage_row.setVisible(enabled)
        value_label = getattr(self, "gallery_kde_bandwidth_value", None)
        if value_label is not None:
            value_label.setText(f"{self._gallery_kde_bandwidth_value():.2f}")
        contours_label = getattr(self, "gallery_kde_contours_value", None)
        contours_slider = getattr(self, "gallery_kde_contours_slider", None)
        if contours_label is not None and contours_slider is not None:
            contours_label.setText(str(int(contours_slider.value())))
        coverage_label = getattr(self, "gallery_kde_coverage_value", None)
        coverage_slider = getattr(self, "gallery_kde_coverage_slider", None)
        if coverage_label is not None and coverage_slider is not None:
            coverage_label.setText(f"{int(coverage_slider.value())}%")
        ellipse_value_label = getattr(self, "gallery_ellipse_coverage_value", None)
        ellipse_slider = getattr(self, "gallery_ellipse_coverage_slider", None)
        if ellipse_value_label is not None and ellipse_slider is not None:
            ellipse_value_label.setText(f"{int(ellipse_slider.value())}%")

    def _on_gallery_splitter_moved(self, _pos, _index):
        if not hasattr(self, "gallery_splitter"):
            return
        sizes = self.gallery_splitter.sizes()
        collapsed = bool(sizes and sizes[1] == 0)
        if collapsed != self._gallery_collapsed:
            self._gallery_collapsed = collapsed
            self.gallery_splitter._is_collapsed = collapsed
            self.gallery_splitter.collapse_toggled.emit(collapsed)
        self._save_gallery_settings()

    def _on_plot_width_splitter_moved(self, _pos, _index):
        if not hasattr(self, "plot_width_splitter"):
            return
        self._save_gallery_settings()

    def _build_spore_sharing_panel(self):
        panel = QWidget()
        panel.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Maximum)
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setSpacing(4)

        self.spore_sharing_group = QButtonGroup(self)
        self.spore_sharing_public_radio = QRadioButton(self.tr("Public (share with everyone)"))
        self.spore_sharing_friends_radio = QRadioButton(self.tr("Friends only"))
        self.spore_sharing_private_radio = QRadioButton(self.tr("Private (keep to myself)"))
        self.spore_sharing_public_radio.setChecked(True)

        self.spore_sharing_group.addButton(self.spore_sharing_public_radio)
        self.spore_sharing_group.addButton(self.spore_sharing_friends_radio)
        self.spore_sharing_group.addButton(self.spore_sharing_private_radio)

        layout.addWidget(self.spore_sharing_public_radio)
        layout.addWidget(self.spore_sharing_friends_radio)
        layout.addWidget(self.spore_sharing_private_radio)

        note = QLabel(self.tr("Controls who can find and use this observation's spore measurements in community search."))
        note.setWordWrap(True)
        note.setStyleSheet(f"color: #7f8c8d; font-size: {pt(8)}pt;")
        layout.addWidget(note)

        self.spore_sharing_group.buttonClicked.connect(self._on_spore_sharing_changed)

        section = CollapsibleSection(self.tr("Spore data sharing"), panel, expanded=False)
        section.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Maximum)
        return section

    def _update_spore_sharing_ui(self, observation_id: int | None) -> None:
        if not hasattr(self, 'spore_sharing_group'):
            return
        enabled = observation_id is not None
        for btn in self.spore_sharing_group.buttons():
            btn.setEnabled(enabled)
        if not enabled:
            self.spore_sharing_public_radio.setChecked(True)
            return
        obs = ObservationDB.get_observation(observation_id)
        vis = str(obs.get('spore_data_visibility') or 'public').strip().lower() if obs else 'public'
        if vis not in {'private', 'friends', 'public'}:
            vis = 'public'
        blocked = [b for b in self.spore_sharing_group.buttons()]
        for b in blocked:
            b.blockSignals(True)
        if vis == 'private':
            self.spore_sharing_private_radio.setChecked(True)
        elif vis == 'friends':
            self.spore_sharing_friends_radio.setChecked(True)
        else:
            self.spore_sharing_public_radio.setChecked(True)
        for b in blocked:
            b.blockSignals(False)

    def _on_spore_sharing_changed(self) -> None:
        obs_id = getattr(self, 'active_observation_id', None)
        if not obs_id:
            return
        if self.spore_sharing_private_radio.isChecked():
            vis = 'private'
        elif self.spore_sharing_friends_radio.isChecked():
            vis = 'friends'
        else:
            vis = 'public'
        ObservationDB.update_observation(obs_id, spore_data_visibility=vis)
        from utils.cloud_sync import mark_observation_dirty
        mark_observation_dirty(obs_id)

    def _build_reference_panel(self):
        panel = QWidget()
        panel.setMinimumWidth(0)
        panel.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(10)

        def _add_section_divider():
            divider = QFrame(panel)
            divider.setFrameShape(QFrame.HLine)
            divider.setFrameShadow(QFrame.Plain)
            divider.setStyleSheet("color: rgba(127, 140, 141, 0.35);")
            layout.addWidget(divider)

        form = QFormLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setSpacing(8)
        form.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        self.ref_vernacular_label = QLabel(self._reference_vernacular_label())
        self.ref_vernacular_input = QLineEdit()
        self.ref_genus_input = QLineEdit()
        self.ref_species_input = QLineEdit()
        self.ref_vernacular_input.setPlaceholderText(self._reference_vernacular_placeholder())
        self.ref_genus_input.setPlaceholderText(self.tr("e.g., Flammulina"))
        self.ref_species_input.setPlaceholderText(self.tr("e.g., velutipes"))
        self.ref_source_input = QComboBox()
        self.ref_source_input.setEditable(True)
        self.ref_source_input.setInsertPolicy(QComboBox.NoInsert)
        self.ref_source_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self._style_dropdown_popup_readability(self.ref_source_input.view(), self.ref_source_input)
        self.ref_cloud_btn = QPushButton(self.tr("Cloud..."))
        self.ref_cloud_btn.clicked.connect(self._on_reference_panel_cloud_clicked)
        self.ref_cloud_btn.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.ref_cloud_btn.setFixedHeight(self.ref_source_input.sizeHint().height())

        source_row = QWidget()
        source_layout = QHBoxLayout(source_row)
        source_layout.setContentsMargins(0, 0, 0, 0)
        source_layout.setSpacing(8)
        source_layout.addWidget(self.ref_source_input, 1)
        source_layout.addWidget(self.ref_cloud_btn, 0)

        form.addRow(self.ref_vernacular_label, self.ref_vernacular_input)
        form.addRow(self.tr("Genus:"), self.ref_genus_input)
        form.addRow(self.tr("Species:"), self.ref_species_input)
        form.addRow(self.tr("Source:"), source_row)
        layout.addLayout(form)
        _add_section_divider()


        btn_row = QHBoxLayout()
        btn_row.setSpacing(8)
        self.ref_plot_btn = QPushButton(self.tr("Plot"))
        self.ref_plot_btn.clicked.connect(self._on_reference_panel_plot_clicked)
        self._register_gallery_hint_widget(
            self.ref_plot_btn,
            self.tr("Plot this data"),
            allow_when_disabled=True,
        )
        self.ref_add_btn = QPushButton(self.tr("Add"))
        self.ref_add_btn.clicked.connect(self._on_reference_panel_add_clicked)
        self._register_gallery_hint_widget(
            self.ref_add_btn,
            self.tr("Add reference data for the selected species"),
            allow_when_disabled=True,
        )
        self.ref_edit_btn = QPushButton(self.tr("Edit"))
        self.ref_edit_btn.clicked.connect(self._on_reference_panel_edit_clicked)
        self._register_gallery_hint_widget(
            self.ref_edit_btn,
            self.tr("Edit reference data"),
            allow_when_disabled=True,
        )
        analysis_button_height = self.ref_plot_btn.sizeHint().height()
        for btn in (self.ref_plot_btn, self.ref_add_btn, self.ref_edit_btn):
            btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            btn.setFixedHeight(analysis_button_height)
        btn_row.addWidget(self.ref_plot_btn, 1)
        btn_row.addWidget(self.ref_add_btn, 1)
        btn_row.addWidget(self.ref_edit_btn, 1)
        layout.addLayout(btn_row)
        _add_section_divider()

        shape_row_widget = QWidget()
        shape_row = QHBoxLayout(shape_row_widget)
        shape_row.setContentsMargins(0, 0, 0, 0)
        shape_row.setSpacing(10)
        shape_label = QLabel(self.tr("Reference shape:"))
        shape_label.setSizePolicy(QSizePolicy.Maximum, QSizePolicy.Preferred)
        shape_row.addWidget(shape_label)
        self.ref_shape_group = QButtonGroup(self)
        self.ref_shape_ellipse_radio = QRadioButton(self.tr("Ellipse"))
        self.ref_shape_square_radio = QRadioButton(self.tr("Square"))
        self.ref_shape_group.addButton(self.ref_shape_ellipse_radio)
        self.ref_shape_group.addButton(self.ref_shape_square_radio)
        reference_shape = str(self.gallery_plot_settings.get("reference_shape", "ellipse") or "ellipse").strip().lower()
        if reference_shape == "square":
            self.ref_shape_square_radio.setChecked(True)
        else:
            self.ref_shape_ellipse_radio.setChecked(True)
        self.ref_shape_ellipse_radio.toggled.connect(self.on_reference_overlay_setting_changed)
        self.ref_shape_square_radio.toggled.connect(self.on_reference_overlay_setting_changed)
        self.ref_shape_ellipse_radio.setSizePolicy(QSizePolicy.Maximum, QSizePolicy.Fixed)
        self.ref_shape_square_radio.setSizePolicy(QSizePolicy.Maximum, QSizePolicy.Fixed)
        shape_row.addWidget(self.ref_shape_ellipse_radio)
        shape_row.addWidget(self.ref_shape_square_radio)
        shape_row.addSpacing(12)
        self.ref_show_minmax_checkbox = QCheckBox(self.tr("Min/Max"))
        self.ref_show_minmax_checkbox.setChecked(bool(self.gallery_plot_settings.get("reference_minmax", True)))
        self.ref_show_minmax_checkbox.toggled.connect(self.on_reference_overlay_setting_changed)
        self.ref_show_minmax_checkbox.setSizePolicy(QSizePolicy.Maximum, QSizePolicy.Fixed)
        shape_row.addWidget(self.ref_show_minmax_checkbox)
        shape_row.addStretch()
        layout.addWidget(shape_row_widget)
        _add_section_divider()

        self.ref_series_table = QTableWidget(0, 4)
        self.ref_series_table.setHorizontalHeaderLabels(
            [self.tr("Plot"), "", self.tr("Data set"), self.tr("Color")]
        )
        self.ref_series_table.verticalHeader().setVisible(False)
        self.ref_series_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.ref_series_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.ref_series_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.ref_series_table.horizontalHeader().setStretchLastSection(False)
        self.ref_series_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.ref_series_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.ref_series_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self.ref_series_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.ref_series_table.setShowGrid(False)
        self.ref_series_table.setMinimumHeight(240)
        self.ref_series_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.ref_series_table.cellClicked.connect(self._on_reference_series_row_clicked)
        layout.addWidget(self.ref_series_table)

        self._init_reference_panel_completers()
        self._populate_reference_panel_sources()
        self._apply_reference_panel_values(self.reference_values)
        self._refresh_reference_series_table()
        self._update_reference_add_state()
        self._sync_reference_overlay_controls_state()
        return panel

    def _style_dropdown_popup_readability(self, popup, font_source=None):
        """Match popup font to input and add slightly looser row spacing."""
        if popup is None:
            return
        if font_source is not None and hasattr(font_source, "font"):
            try:
                popup.setFont(font_source.font())
            except Exception:
                pass
        if hasattr(popup, "setSpacing"):
            try:
                popup.setSpacing(1)
            except Exception:
                pass
        popup.setStyleSheet(
            "QListView::item { padding: 2px 6px; }"
            "QAbstractItemView::item { padding: 2px 6px; }"
        )

    def _reference_vernacular_label(self) -> str:
        lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
        base = self.tr("Common name")
        return f"{common_name_display_label(lang, base)}:"

    def _reference_vernacular_placeholder(self) -> str:
        lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
        examples = {
            "no": "Kantarell",
            "de": "Pfifferling",
            "fr": "Girolle",
            "es": "Rebozuelo",
            "da": "Kantarel",
            "sv": "Kantarell",
            "fi": "Kantarelli",
            "pl": "Kurka",
            "pt": "Cantarelo",
            "it": "Gallinaccio",
        }
        return f"e.g., {examples.get(lang, 'Chanterelle')}"

    def _set_ref_vernacular_placeholder_from_suggestions(self, suggestions: list[str]) -> None:
        if not hasattr(self, "ref_vernacular_input"):
            return
        cleaned = [str(name).strip() for name in (suggestions or []) if str(name).strip()]
        if not cleaned:
            self.ref_vernacular_input.setPlaceholderText(self._reference_vernacular_placeholder())
            return
        preview = "; ".join(cleaned[:4])
        self.ref_vernacular_input.setPlaceholderText(f"e.g., {preview}")

    def _set_ref_species_placeholder_from_suggestions(self, suggestions: list[str]) -> None:
        if not hasattr(self, "ref_species_input"):
            return
        cleaned = [str(name).strip() for name in (suggestions or []) if str(name).strip()]
        if not cleaned:
            self.ref_species_input.setPlaceholderText(self.tr("e.g., velutipes"))
            return
        preview = "; ".join(cleaned[:4])
        self.ref_species_input.setPlaceholderText(f"e.g., {preview}")

    def _populate_ref_vernacular_model(self, suggestions: list[str]) -> None:
        self._ref_vernacular_model.clear()
        if not self.ref_vernacular_db:
            return
        exclude_id = self.active_observation_id if hasattr(self, "active_observation_id") else None
        for name in suggestions:
            display = name
            emojis = []
            if self.species_availability and name:
                taxon = self.ref_vernacular_db.taxon_from_vernacular(name)
                if taxon:
                    tax_genus, tax_species, _family = taxon
                    info = self._reference_species_info_case_insensitive(
                        tax_genus,
                        tax_species,
                        exclude_observation_id=exclude_id,
                    )
                    emojis.extend(self._reference_availability_emojis(info))
            if emojis:
                display = f"{name} {' '.join(emojis)}"
            item = QStandardItem(display)
            item.setData(name, Qt.UserRole)
            self._ref_vernacular_model.appendRow(item)

    def _update_ref_vernacular_suggestions_for_taxon(self) -> None:
        if not self.ref_vernacular_db:
            self._ref_vernacular_model.clear()
            self._set_ref_vernacular_placeholder_from_suggestions([])
            return
        genus = self._clean_ref_genus_text(self.ref_genus_input.text()) or None
        species = self._clean_ref_species_text(self.ref_species_input.text()) or None
        if not genus and not species:
            self._ref_vernacular_model.clear()
            self._set_ref_vernacular_placeholder_from_suggestions([])
            return
        suggestions = self.ref_vernacular_db.suggest_vernacular_for_taxon(genus=genus, species=species)
        self._populate_ref_vernacular_model(suggestions)
        self._set_ref_vernacular_placeholder_from_suggestions(suggestions)

    def _update_reference_add_state(self):
        if not hasattr(self, "ref_add_btn"):
            return
        genus = self._clean_ref_genus_text(self.ref_genus_input.text()) if hasattr(self, "ref_genus_input") else ""
        species = self._clean_ref_species_text(self.ref_species_input.text()) if hasattr(self, "ref_species_input") else ""
        has_species = bool(genus and species)
        self.ref_add_btn.setEnabled(has_species)
        add_hint = self.tr("Add reference data for the selected species")
        if not has_species:
            add_hint = self.tr("Enter a species first to add spore data")
        self._register_gallery_hint_widget(
            self.ref_add_btn,
            add_hint,
            allow_when_disabled=True,
        )
        if hasattr(self, "ref_edit_btn"):
            has_source = bool(self.ref_source_input.currentText().strip()) if hasattr(self, "ref_source_input") else False
            self.ref_edit_btn.setEnabled(has_species and has_source)
            edit_hint = self.tr("Edit reference data")
            if not has_species:
                edit_hint = self.tr("Enter a species first to edit reference data")
            elif not has_source:
                edit_hint = self.tr("Select a source to edit reference data")
            self._register_gallery_hint_widget(
                self.ref_edit_btn,
                edit_hint,
                allow_when_disabled=True,
            )
        if hasattr(self, "ref_cloud_btn"):
            self.ref_cloud_btn.setEnabled(has_species)
            cloud_hint = self.tr("Search community spore data")
            if not has_species:
                cloud_hint = self.tr("Enter a species first to search community spore data")
            self._register_gallery_hint_widget(
                self.ref_cloud_btn,
                cloud_hint,
                allow_when_disabled=True,
            )
        if hasattr(self, "ref_plot_btn"):
            source_text = self.ref_source_input.currentText().strip() if hasattr(self, "ref_source_input") else ""
            has_plot_data = self._reference_has_selected_source_data()
            self.ref_plot_btn.setEnabled(has_plot_data)
            plot_hint = self.tr("Plot this data")
            if not has_species:
                plot_hint = self.tr("Select species and source to plot.")
            elif not source_text or not has_plot_data:
                plot_hint = self.tr("Select a source for this species or Add a new source.")
            self._register_gallery_hint_widget(
                self.ref_plot_btn,
                plot_hint,
                allow_when_disabled=True,
            )
        if hasattr(self, "ref_clear_btn"):
            has_plot_data = bool(getattr(self, "reference_series", []))
            has_taxon_input = bool(genus or species)
            self.ref_clear_btn.setEnabled(has_plot_data or has_taxon_input)

    def _reference_has_selected_source_data(self) -> bool:
        if not hasattr(self, "ref_source_input"):
            return False
        genus = self._clean_ref_genus_text(self.ref_genus_input.text()) if hasattr(self, "ref_genus_input") else ""
        species = self._clean_ref_species_text(self.ref_species_input.text()) if hasattr(self, "ref_species_input") else ""
        if not genus or not species:
            return False
        source_text = self.ref_source_input.currentText().strip()
        if not source_text:
            return False
        source_data = self.ref_source_input.currentData()
        if isinstance(source_data, dict):
            kind = source_data.get("kind")
            if kind == "observation":
                return bool(source_data.get("observation_id"))
            if kind == "points":
                return True
            if kind == "reference":
                source = source_data.get("source") or source_text
                return bool(ReferenceDB.get_reference(genus, species, source))
        idx = self.ref_source_input.findText(source_text)
        if idx <= 0:
            return False
        item_data = self.ref_source_input.itemData(idx)
        if isinstance(item_data, dict):
            kind = item_data.get("kind")
            if kind == "observation":
                return bool(item_data.get("observation_id"))
            if kind == "points":
                return True
            if kind == "reference":
                source = item_data.get("source") or source_text
                return bool(ReferenceDB.get_reference(genus, species, source))
        return False

    def _reference_allow_points(self) -> bool:
        if not hasattr(self, "gallery_filter_combo"):
            return True
        category = self.gallery_filter_combo.currentData()
        if not category:
            return False
        normalized = self.normalize_measurement_category(category)
        return normalized == "spores"

    def _reference_series_key(self, data: dict) -> tuple | None:
        if not isinstance(data, dict):
            return None
        genus = (data.get("genus") or "").strip()
        species = (data.get("species") or "").strip()
        kind = data.get("source_kind") or ("points" if data.get("points") else "reference")
        if kind == "observation":
            obs_id = data.get("observation_id") or ""
            return ("observation", genus, species, str(obs_id))
        if kind == "points":
            source_type = (data.get("source_type") or "").strip()
            label = (data.get("points_label") or data.get("source_label") or "").strip()
            return ("points", genus, species, source_type, label)
        source = (data.get("source") or "").strip()
        mount = (data.get("mount_medium") or "").strip()
        stain = (data.get("stain") or "").strip()
        return ("reference", genus, species, source, mount, stain)

    def _format_reference_series_label(self, data: dict) -> str:
        genus = (data.get("genus") or "").strip()
        species = (data.get("species") or "").strip()
        kind = data.get("source_kind") or ("points" if data.get("points") else "reference")
        genus_label = f"{genus[0].upper()}." if genus else ""
        base = f"{genus_label} {species}".strip() if genus_label else (f"{genus} {species}".strip() or species)
        if kind == "observation":
            author = (data.get("author") or "").strip()
            if not author:
                profile = SettingsDB.get_profile()
                author = (profile.get("name") or "").strip()
            date_str = (data.get("date") or "").strip()
            info_parts = [p for p in (author, date_str) if p]
            info = ", ".join(info_parts)
            if info:
                return f"{base} ({info})".strip()
            return base or self.tr("Reference")
        source = (
            (data.get("source") or "")
            or (data.get("points_label") or "")
            or (data.get("source_label") or "")
        ).strip()
        mount = (data.get("mount_medium") or "").strip()
        stain = (data.get("stain") or "").strip()
        prep_parts = [part for part in (mount, stain) if part]
        prep = ", ".join(prep_parts).strip()
        if source:
            label = f"{base} ({source})".strip()
            return f"{label} [{prep}]".strip() if prep else label
        if prep:
            return f"{base} [{prep}]".strip()
        return base or self.tr("Reference")

    def _normalize_reference_series_entry(self, entry: dict | None) -> dict | None:
        if not isinstance(entry, dict) or not entry:
            return None
        has_wrapper = isinstance(entry.get("data"), dict)
        data = entry.get("data") if has_wrapper else entry
        if not isinstance(data, dict) or not data:
            return None
        key = entry.get("key") if has_wrapper else None
        if not key:
            key = self._reference_series_key(data)
        if not key:
            return None
        label = entry.get("label") if has_wrapper else None
        if not label:
            label = self._format_reference_series_label(data)
        enabled = bool(entry.get("enabled", True)) if has_wrapper else bool(entry.get("enabled", True))
        return {
            "key": key,
            "data": data,
            "label": label,
            "enabled": enabled,
        }

    def _ensure_reference_series_entries(self) -> list[dict]:
        if self.reference_series:
            return self.reference_series
        fallback = self._normalize_reference_series_entry(self.reference_values)
        if fallback:
            self.reference_series = [fallback]
        return self.reference_series

    def _resolved_reference_series_entries(self, dark: bool | None = None) -> list[dict]:
        resolved: list[dict] = []
        hist_color = "#4a90d9" if dark else "#3498db"
        ref_palette = reference_plot_palette(dark)
        source_entries = self.reference_series or ([self.reference_values] if self.reference_values else [])
        for entry_index, raw_entry in enumerate(source_entries):
            entry = self._normalize_reference_series_entry(raw_entry)
            if not entry:
                continue
            data = entry["data"]
            preferred_color = str(data.get("plot_color") or "").strip().lower()
            if preferred_color and not QColor(preferred_color).isValid():
                preferred_color = ""
            if preferred_color:
                color = QColor(preferred_color).name().lower()
            else:
                color = ref_palette[entry_index % len(ref_palette)]
                if color == hist_color and ref_palette:
                    color = ref_palette[(entry_index + 1) % len(ref_palette)]
            resolved.append({
                **entry,
                "color": color,
                "preferred_color": preferred_color or None,
            })
        return resolved

    def _style_reference_color_button(
        self,
        button: QPushButton | QToolButton,
        color: str,
        *,
        auto: bool = False,
        selected: bool = False,
    ) -> None:
        qcolor = QColor(color if QColor(color).isValid() else "#adb5bd")
        lum = (0.299 * qcolor.red() + 0.587 * qcolor.green() + 0.114 * qcolor.blue())
        border = "2px solid #3498db" if selected else "1px solid #666"
        inner = "#111" if lum > 160 else "#fff"
        text = self.tr("A") if auto else ""
        if hasattr(button, "setText"):
            button.setText(text)
        button.setToolTip(
            self.tr("Auto ({color})").format(color=qcolor.name())
            if auto
            else qcolor.name()
        )
        button.setStyleSheet(
            f"background-color: {qcolor.name()};"
            f"border: {border};"
            "border-radius: 6px;"
            f"color: {inner};"
            "font-weight: bold;"
        )

    def _open_reference_series_color_menu(self, key: tuple, button: QWidget) -> None:
        if not key or button is None:
            return
        entry = None
        for candidate in self._resolved_reference_series_entries(self._is_dark_theme()):
            if candidate.get("key") == key:
                entry = candidate
                break
        if not entry:
            return

        from PySide6.QtWidgets import QGridLayout, QMenu, QWidgetAction

        menu = QMenu(self)
        panel = QWidget(menu)
        grid = QGridLayout(panel)
        grid.setContentsMargins(8, 8, 8, 8)
        grid.setHorizontalSpacing(6)
        grid.setVerticalSpacing(6)

        current_preference = entry.get("preferred_color")

        def _apply(color: str | None) -> None:
            self._set_reference_series_color(key, color)
            menu.close()

        auto_btn = QPushButton(self.tr("Auto"), panel)
        auto_btn.setFixedHeight(24)
        auto_btn.clicked.connect(lambda _checked=False: _apply(None))
        auto_btn.setStyleSheet(
            "padding: 2px 8px; border-radius: 6px; "
            f"border: {'2px solid #3498db' if current_preference is None else '1px solid #777'};"
        )
        grid.addWidget(auto_btn, 0, 0, 1, 2)

        row_titles = {
            "dark": self.tr("Darker"),
            "medium": self.tr("Medium"),
            "light": self.tr("Lighter"),
        }
        for row_index, (name, colors) in enumerate(reference_plot_palette_groups(self._is_dark_theme()), start=1):
            grid.addWidget(QLabel(row_titles.get(name, name.title()), panel), row_index, 0)
            for col_index, color in enumerate(colors, start=1):
                swatch = QToolButton(panel)
                swatch.setFixedSize(22, 22)
                self._style_reference_color_button(
                    swatch,
                    color,
                    selected=(current_preference == color),
                )
                swatch.clicked.connect(lambda _checked=False, c=color: _apply(c))
                grid.addWidget(swatch, row_index, col_index)

        action = QWidgetAction(menu)
        action.setDefaultWidget(panel)
        menu.addAction(action)
        menu.exec(button.mapToGlobal(QPoint(0, button.height())))

    def _refresh_measure_color_button(self) -> None:
        if not hasattr(self, "measure_color_menu_button"):
            return
        color_name = QColor(self.measure_color).name().lower() if QColor(self.measure_color).isValid() else "#000000"
        self._style_reference_color_button(self.measure_color_menu_button, color_name, selected=False)

    def _open_measure_color_menu(self, button: QWidget) -> None:
        if button is None:
            return

        from PySide6.QtWidgets import QGridLayout, QMenu, QWidgetAction

        menu = QMenu(self)
        panel = QWidget(menu)
        grid = QGridLayout(panel)
        grid.setContentsMargins(8, 8, 8, 8)
        grid.setHorizontalSpacing(6)
        grid.setVerticalSpacing(6)

        current_color = QColor(self.measure_color).name().lower() if QColor(self.measure_color).isValid() else "#000000"
        row_titles = {
            "dark": self.tr("Darker"),
            "medium": self.tr("Medium"),
            "light": self.tr("Lighter"),
        }

        def _apply(color: str) -> None:
            self.set_measure_color(color)
            menu.close()

        for row_index, (name, colors) in enumerate(measure_overlay_palette_groups(self._is_dark_theme())):
            grid.addWidget(QLabel(row_titles.get(name, name.title()), panel), row_index, 0)
            for col_index, color in enumerate(colors, start=1):
                swatch = QToolButton(panel)
                swatch.setFixedSize(22, 22)
                self._style_reference_color_button(
                    swatch,
                    color,
                    selected=(current_color == QColor(color).name().lower()),
                )
                swatch.clicked.connect(lambda _checked=False, c=color: _apply(c))
                grid.addWidget(swatch, row_index, col_index)

        action = QWidgetAction(menu)
        action.setDefaultWidget(panel)
        menu.addAction(action)
        menu.exec(button.mapToGlobal(QPoint(0, button.height())))

    def _sync_reference_values_from_series_data(self, key: tuple, data: dict) -> None:
        current = self.reference_values if isinstance(self.reference_values, dict) else None
        if current and self._reference_series_key(current) == key:
            self.reference_values = dict(data)

    def _set_reference_series_enabled(self, key: tuple, enabled: bool) -> None:
        if not key:
            return
        changed = False
        for entry in self._ensure_reference_series_entries():
            if isinstance(entry, dict) and entry.get("key") == key:
                if bool(entry.get("enabled", True)) != bool(enabled):
                    entry["enabled"] = bool(enabled)
                    changed = True
                break
        if not changed:
            return
        self.update_graph_plots_only()
        self._save_gallery_settings()

    def _set_reference_series_color(self, key: tuple, color: str | None) -> None:
        if not key:
            return
        normalized_color = QColor(color).name().lower() if color and QColor(color).isValid() else None
        changed = False
        updated_data = None
        for entry in self._ensure_reference_series_entries():
            if not isinstance(entry, dict) or entry.get("key") != key:
                continue
            data = entry.get("data", {})
            if not isinstance(data, dict):
                break
            if data.get("plot_color") == normalized_color:
                return
            data["plot_color"] = normalized_color
            entry["data"] = data
            updated_data = data
            changed = True
            break
        if not changed or not isinstance(updated_data, dict):
            return
        self._sync_reference_values_from_series_data(key, updated_data)
        if updated_data.get("source_kind") == "reference":
            ReferenceDB.set_reference(updated_data)
        self._refresh_reference_series_table()
        self.update_graph_plots_only()
        self._save_gallery_settings()

    def _refresh_reference_series_table(self):
        if not hasattr(self, "ref_series_table"):
            return
        self.ref_series_table.setRowCount(0)
        self._ref_series_row_entries = []
        for entry in self._resolved_reference_series_entries(self._is_dark_theme()):
            data = entry.get("data", {})
            key = entry.get("key")
            label = entry.get("label") or self._format_reference_series_label(data)
            row = self.ref_series_table.rowCount()
            self.ref_series_table.insertRow(row)

            toggle_holder = QWidget()
            toggle_layout = QHBoxLayout(toggle_holder)
            toggle_layout.setContentsMargins(0, 0, 0, 0)
            toggle_layout.setAlignment(Qt.AlignCenter)
            toggle_checkbox = QCheckBox(toggle_holder)
            toggle_checkbox.setChecked(bool(entry.get("enabled", True)))
            toggle_checkbox.toggled.connect(lambda checked, k=key: self._set_reference_series_enabled(k, checked))
            toggle_layout.addWidget(toggle_checkbox)
            self._register_gallery_hint_widget(toggle_checkbox, self.tr("Show or hide this reference plot"))
            self.ref_series_table.setCellWidget(row, 0, toggle_holder)

            remove_btn = QToolButton()
            remove_btn.setText("X")
            remove_btn.setAutoRaise(True)
            remove_btn.setStyleSheet(f"color: #e74c3c; font-weight: bold; font-size: {pt(11)}pt;")
            remove_btn.clicked.connect(lambda _checked=False, k=key: self._remove_reference_series_key(k))
            self._register_gallery_hint_widget(remove_btn, self.tr("Remove this plot"))
            self.ref_series_table.setCellWidget(row, 1, remove_btn)

            label_item = QTableWidgetItem(label)
            label_item.setFlags(Qt.ItemIsEnabled)
            self.ref_series_table.setItem(row, 2, label_item)

            color_btn = QToolButton()
            color_btn.setFixedSize(28, 22)
            color_btn.setAutoRaise(False)
            self._style_reference_color_button(
                color_btn,
                str(entry.get("color") or "#adb5bd"),
                auto=entry.get("preferred_color") is None,
            )
            color_btn.clicked.connect(
                lambda _checked=False, k=key, btn=color_btn: self._open_reference_series_color_menu(k, btn)
            )
            self._register_gallery_hint_widget(color_btn, self.tr("Change the plot color"))
            self.ref_series_table.setCellWidget(row, 3, color_btn)

            self.ref_series_table.setRowHeight(row, 28)
            self._ref_series_row_entries.append(entry)
        self.ref_series_table.resizeColumnToContents(0)
        self.ref_series_table.resizeColumnToContents(1)
        self.ref_series_table.resizeColumnToContents(3)

    def _on_reference_series_row_clicked(self, row: int, col: int):
        if col in (0, 1, 3):
            return
        entries = getattr(self, "_ref_series_row_entries", []) or []
        if row < 0 or row >= len(entries):
            return
        entry = entries[row]
        data = entry.get("data", entry) if isinstance(entry, dict) else entry
        if not isinstance(data, dict):
            return
        genus = (data.get("genus") or "").strip()
        species = (data.get("species") or "").strip()
        if not genus or not species:
            return
        source = (
            (data.get("source") or "")
            or (data.get("points_label") or "")
            or (data.get("source_label") or "")
        ).strip()
        if hasattr(self, "ref_vernacular_input"):
            self.ref_vernacular_input.blockSignals(True)
            self.ref_vernacular_input.setText("")
            self.ref_vernacular_input.blockSignals(False)
        self.ref_genus_input.blockSignals(True)
        self.ref_species_input.blockSignals(True)
        self.ref_genus_input.setText(genus)
        self.ref_species_input.setText(species)
        self.ref_species_input.blockSignals(False)
        self.ref_genus_input.blockSignals(False)
        self._populate_reference_panel_sources(auto_select_single=False)
        if source:
            idx = self.ref_source_input.findText(source)
            if idx >= 0:
                self.ref_source_input.setCurrentIndex(idx)
            else:
                self.ref_source_input.setCurrentText(source)
        else:
            self.ref_source_input.setCurrentIndex(0)
        self._maybe_set_ref_vernacular_from_taxon()
        self.reference_values = dict(data)
        self._apply_reference_panel_values(self.reference_values)
        self._update_reference_add_state()

    def _set_reference_series(self, series: list[dict]):
        self.reference_series = []
        for item in series or []:
            normalized = self._normalize_reference_series_entry(item)
            if normalized:
                self.reference_series.append(normalized)
        self._refresh_reference_series_table()
        self._update_reference_add_state()
        self.update_graph_plots_only()

    def _add_reference_series_entry(self, data: dict) -> bool:
        normalized = self._normalize_reference_series_entry(data)
        if not normalized:
            return False
        key = normalized.get("key")
        for entry in self.reference_series:
            if isinstance(entry, dict) and entry.get("key") == key:
                entry["data"] = normalized.get("data")
                entry["label"] = normalized.get("label")
                self._refresh_reference_series_table()
                self.update_graph_plots_only()
                self._save_gallery_settings()
                return True
        self.reference_series.append(normalized)
        self._refresh_reference_series_table()
        self._update_reference_add_state()
        self.update_graph_plots_only()
        self._save_gallery_settings()
        return True

    def _remove_reference_series_key(self, key: tuple):
        if not key:
            return
        self.reference_series = [
            entry for entry in self.reference_series
            if not (isinstance(entry, dict) and entry.get("key") == key)
        ]
        current = self.reference_values if isinstance(self.reference_values, dict) else None
        if current and self._reference_series_key(current) == key:
            self.reference_values = {}
        if not self.reference_series:
            self.reference_values = {}
        self._refresh_reference_series_table()
        self._update_reference_add_state()
        self.update_graph_plots_only()
        self._save_gallery_settings()

    def _clean_ref_species_text(self, text: str | None) -> str:
        if not text:
            return ""
        cleaned = str(text).strip()
        if not cleaned:
            return ""
        availability_tokens = {
            str(getattr(self.species_availability, "DATA_POINT_EMOJI", "") or "").strip(),
            str(getattr(self.species_availability, "MINMAX_EMOJI", "") or "").strip(),
        }
        tokens = [token for token in cleaned.split() if token.strip() and token.strip() not in availability_tokens]
        return " ".join(tokens).strip()

    def _clean_ref_genus_text(self, text: str | None) -> str:
        if not text:
            return ""
        token = str(text).strip().split()
        return token[0].strip() if token else ""

    def _reference_availability_emojis(self, info: dict | None) -> list[str]:
        if not isinstance(info, dict):
            return []
        emojis = []
        if (
            info.get("has_personal_points")
            or info.get("has_shared_points")
            or info.get("has_published_points")
        ):
            emojis.append(self.species_availability.DATA_POINT_EMOJI)
        if info.get("has_reference_minmax"):
            emojis.append(self.species_availability.MINMAX_EMOJI)
        return emojis

    def _reference_species_info_case_insensitive(
        self,
        genus: str,
        species: str,
        exclude_observation_id: int | None = None,
    ) -> dict:
        genus = (genus or "").strip()
        species = (species or "").strip()
        if not genus or not species or not hasattr(self, "species_availability"):
            return {}
        info = self.species_availability.get_detailed_info(
            genus,
            species,
            exclude_observation_id=exclude_observation_id,
        )
        if info:
            return info
        genus_l = genus.lower()
        species_l = species.lower()
        cache = self.species_availability.get_cache()
        for (g, s), _ in cache.items():
            if str(g).strip().lower() == genus_l and str(s).strip().lower() == species_l:
                return self.species_availability.get_detailed_info(
                    g,
                    s,
                    exclude_observation_id=exclude_observation_id,
                )
        return {}

    def _reference_genus_availability_summary(
        self,
        exclude_observation_id: int | None = None,
    ) -> dict[str, dict]:
        if not hasattr(self, "species_availability"):
            return {}
        cache = self.species_availability.get_cache()
        cache_key = (id(cache), exclude_observation_id)
        if self._ref_genus_summary_cache_key == cache_key and isinstance(self._ref_genus_summary_cache, dict):
            return self._ref_genus_summary_cache

        summary: dict[str, dict] = {}
        for (g, s), _ in cache.items():
            genus_key = str(g).strip().lower()
            if not genus_key:
                continue
            info = self.species_availability.get_detailed_info(
                g,
                s,
                exclude_observation_id=exclude_observation_id,
            )
            combined = summary.setdefault(
                genus_key,
                {
                    "has_personal_points": False,
                    "has_shared_points": False,
                    "has_published_points": False,
                    "has_reference_minmax": False,
                },
            )
            if info.get("has_personal_points"):
                combined["has_personal_points"] = True
            if info.get("has_shared_points"):
                combined["has_shared_points"] = True
            if info.get("has_published_points"):
                combined["has_published_points"] = True
            if info.get("has_reference_minmax"):
                combined["has_reference_minmax"] = True

        self._ref_genus_summary_cache_key = cache_key
        self._ref_genus_summary_cache = summary
        return summary

    def _reference_genus_info(
        self,
        genus: str,
        exclude_observation_id: int | None = None,
    ) -> dict:
        genus = (genus or "").strip()
        if not genus or not hasattr(self, "species_availability"):
            return {}
        genus_l = genus.lower()
        combined = {
            "has_personal_points": False,
            "has_shared_points": False,
            "has_published_points": False,
            "has_reference_minmax": False,
        }
        cache = self.species_availability.get_cache()
        for (g, s), _ in cache.items():
            if str(g).strip().lower() != genus_l:
                continue
            info = self.species_availability.get_detailed_info(
                g,
                s,
                exclude_observation_id=exclude_observation_id,
            )
            if info.get("has_personal_points"):
                combined["has_personal_points"] = True
            if info.get("has_shared_points"):
                combined["has_shared_points"] = True
            if info.get("has_published_points"):
                combined["has_published_points"] = True
            if info.get("has_reference_minmax"):
                combined["has_reference_minmax"] = True
        return combined

    def _suppress_ref_completer_updates(self):
        if self._ref_completer_suppress:
            return
        self._ref_completer_suppress = True
        try:
            if hasattr(self, "_ref_genus_completer") and self._ref_genus_completer.popup():
                self._ref_genus_completer.popup().hide()
            if hasattr(self, "_ref_species_completer") and self._ref_species_completer.popup():
                self._ref_species_completer.popup().hide()
            if hasattr(self, "_ref_vernacular_completer") and self._ref_vernacular_completer.popup():
                self._ref_vernacular_completer.popup().hide()
        except Exception:
            pass
        QTimer.singleShot(0, lambda: (
            self._ref_genus_completer.popup().hide() if hasattr(self, "_ref_genus_completer") and self._ref_genus_completer.popup() else None,
            self._ref_species_completer.popup().hide() if hasattr(self, "_ref_species_completer") and self._ref_species_completer.popup() else None,
            self._ref_vernacular_completer.popup().hide() if hasattr(self, "_ref_vernacular_completer") and self._ref_vernacular_completer.popup() else None
        ))
        QTimer.singleShot(0, lambda: setattr(self, "_ref_completer_suppress", False))

    def _format_observation_legend_label(self) -> str:
        if not self.active_observation_id:
            return self.tr("Observation")
        obs = ObservationDB.get_observation(self.active_observation_id)
        if not obs:
            return self.tr("Observation")
        genus = (obs.get("genus") or "").strip()
        species = (obs.get("species") or obs.get("species_guess") or "").strip()
        date_value = (obs.get("date") or "").strip()
        if " " in date_value:
            date_value = date_value.split(" ")[0]
        if "T" in date_value:
            date_value = date_value.split("T")[0]
        genus_label = f"{genus[0].upper()}." if genus else ""
        name = f"{genus_label} {species}".strip()
        if date_value:
            return f"{name} {date_value}".strip()
        return name or self.tr("Observation")

    def _format_reference_label(self, kind: str, source: str | None) -> str:
        genus = (self.reference_values.get("genus") or "").strip() if isinstance(self.reference_values, dict) else ""
        species = (self.reference_values.get("species") or "").strip() if isinstance(self.reference_values, dict) else ""
        genus_label = f"{genus[0].upper()}." if genus else ""
        base = f"{genus_label} {species}".strip() if genus_label else (f"{genus} {species}".strip() or species)
        if source:
            return f"{base} ({source})".strip()
        return base or self.tr("Reference")

    def _init_reference_panel_completers(self):
        self._ref_genus_model = QStringListModel()
        self._ref_species_model = QStandardItemModel()
        self._ref_genus_completer = QCompleter(self._ref_genus_model, self)
        self._ref_genus_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._ref_genus_completer.setCompletionMode(QCompleter.PopupCompletion)
        self._ref_species_completer = QCompleter(self._ref_species_model, self)
        self._ref_species_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._ref_species_completer.setCompletionMode(QCompleter.PopupCompletion)
        self._ref_species_completer.setCompletionRole(Qt.UserRole)
        self._ref_species_completer.setFilterMode(Qt.MatchContains)
        self.ref_genus_input.setCompleter(self._ref_genus_completer)
        self.ref_species_input.setCompleter(self._ref_species_completer)
        self._style_dropdown_popup_readability(self._ref_genus_completer.popup(), self.ref_genus_input)
        species_popup = self._ref_species_completer.popup()
        self._style_dropdown_popup_readability(species_popup, self.ref_species_input)
        species_popup.setItemDelegate(
            SpeciesItemDelegate(
                self.species_availability,
                species_popup,
                exclude_observation_id=lambda: self.active_observation_id,
                genus_provider=lambda: self._clean_ref_genus_text(self.ref_genus_input.text()),
            )
        )
        self._ref_genus_completer.activated[QModelIndex].connect(self._on_ref_genus_selected)
        self._ref_species_completer.activated[QModelIndex].connect(self._on_ref_species_selected)
        self.ref_genus_input.textChanged.connect(self._on_ref_genus_text_changed)
        self.ref_species_input.textChanged.connect(self._on_ref_species_text_changed)
        self.ref_genus_input.editingFinished.connect(self._on_ref_taxon_editing_finished)
        self.ref_species_input.editingFinished.connect(self._on_ref_taxon_editing_finished)
        self.ref_source_input.currentTextChanged.connect(self._on_ref_source_changed)
        self.ref_genus_input.installEventFilter(self)
        self.ref_species_input.installEventFilter(self)
        self.ref_vernacular_input.installEventFilter(self)
        if self.ref_source_input.lineEdit():
            self.ref_source_input.lineEdit().installEventFilter(self)

        self.ref_vernacular_db = None
        lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
        db_path = resolve_vernacular_db_path(lang)
        if db_path:
            self.ref_vernacular_db = VernacularDB(db_path, language_code=lang)
        self._ref_vernacular_model = QStandardItemModel()
        self._ref_vernacular_completer = QCompleter(self._ref_vernacular_model, self)
        self._ref_vernacular_completer.setCaseSensitivity(Qt.CaseInsensitive)
        self._ref_vernacular_completer.setCompletionMode(QCompleter.PopupCompletion)
        self._ref_vernacular_completer.setCompletionRole(Qt.UserRole)
        self.ref_vernacular_input.setCompleter(self._ref_vernacular_completer)
        self._style_dropdown_popup_readability(self._ref_vernacular_completer.popup(), self.ref_vernacular_input)
        self._ref_vernacular_completer.activated[QModelIndex].connect(self._on_ref_vernacular_selected)
        self.ref_vernacular_input.textChanged.connect(self._on_ref_vernacular_text_changed)
        self.ref_vernacular_input.editingFinished.connect(self._on_ref_vernacular_editing_finished)

    def _update_ref_genus_suggestions(self, text, hide_on_exact: bool = False):
        values = ReferenceDB.list_genera(text or "")
        if getattr(self, "ref_vernacular_db", None):
            values.extend(self.ref_vernacular_db.suggest_genus(text or ""))
        values = sorted({value for value in values if value})
        exclude_id = self.active_observation_id if hasattr(self, "active_observation_id") else None
        genus_info_map = self._reference_genus_availability_summary(exclude_observation_id=exclude_id)
        display_values = []
        for genus in values:
            info = genus_info_map.get(genus.strip().lower(), {})
            emojis = self._reference_availability_emojis(info)
            display_values.append(f"{genus} {' '.join(emojis)}".strip() if emojis else genus)
        if hide_on_exact and text.strip():
            text_lower = text.strip().lower()
            if any(value.lower() == text_lower for value in values):
                self._ref_genus_model.setStringList([])
                if self._ref_genus_completer:
                    self._ref_genus_completer.popup().hide()
                return values
        if self._ref_genus_model.stringList() != display_values:
            self._ref_genus_model.setStringList(display_values)
        return values

    def _update_ref_species_suggestions(self, genus, text, hide_on_exact: bool = False):
        genus = (genus or "").strip()
        prefix = (text or "").strip()
        values = ReferenceDB.list_species(genus, prefix)
        if getattr(self, "ref_vernacular_db", None):
            values.extend(self.ref_vernacular_db.suggest_species(genus, prefix))
        exclude_id = self.active_observation_id if hasattr(self, "active_observation_id") else None
        if hasattr(self, "species_availability"):
            cache = self.species_availability.get_cache()
            for (g, s), _info in cache.items():
                if g != genus:
                    continue
                if prefix and not s.lower().startswith(prefix.lower()):
                    continue
                values.append(s)
        values = sorted({value for value in values if value})
        if hide_on_exact and prefix:
            prefix_lower = prefix.lower()
            if any(value.lower() == prefix_lower for value in values):
                self._ref_species_model.clear()
                if self._ref_species_completer:
                    self._ref_species_completer.popup().hide()
                return values
        self._ref_species_model.clear()
        for species in values:
            display, has_data = self.species_availability.get_species_display_name(
                genus,
                species,
                exclude_observation_id=exclude_id,
            )
            # Keep display focused on species while preserving emojis
            display_text = display
            if display.lower().startswith(genus.lower()):
                display_text = display[len(genus):].strip()
            item = QStandardItem(display_text)
            item.setData(species, Qt.UserRole)
            item.setData(genus, Qt.UserRole + 1)
            item.setData(species, Qt.UserRole + 2)
            item.setData(bool(has_data), Qt.UserRole + 3)
            self._ref_species_model.appendRow(item)
        return values

    def _on_ref_genus_text_changed(self, text):
        if self._ref_completer_suppress:
            return
        self._update_ref_genus_suggestions(text or "", hide_on_exact=True)
        if self.ref_genus_input.hasFocus() and not self._ref_taxon_fill_from_vernacular:
            if hasattr(self, "ref_vernacular_input") and self.ref_vernacular_input.text().strip():
                self.ref_vernacular_input.blockSignals(True)
                self.ref_vernacular_input.setText("")
                self.ref_vernacular_input.blockSignals(False)
            if self.ref_species_input.text().strip():
                self.ref_species_input.blockSignals(True)
                self.ref_species_input.setText("")
                self.ref_species_input.blockSignals(False)
                self._ref_species_model.clear()
            if hasattr(self, "ref_source_input") and self.ref_source_input.currentText().strip():
                self.ref_source_input.blockSignals(True)
                self.ref_source_input.setCurrentText("")
                self.ref_source_input.blockSignals(False)
        if not text.strip():
            self.ref_species_input.setText("")
            self._ref_species_model.clear()
            if self._ref_species_completer:
                self._ref_species_completer.setCompletionPrefix("")
            if hasattr(self, "ref_vernacular_input"):
                self.ref_vernacular_input.setText("")
            if self._ref_vernacular_completer:
                self._ref_vernacular_completer.setCompletionPrefix("")
            if hasattr(self, "ref_source_input"):
                self.ref_source_input.setCurrentText("")
            self._set_ref_species_placeholder_from_suggestions([])
            self._set_ref_vernacular_placeholder_from_suggestions([])
        genus = self._clean_ref_genus_text(text)
        if genus and not self.ref_species_input.text().strip():
            species_suggestions = self._update_ref_species_suggestions(genus, "")
            self._set_ref_species_placeholder_from_suggestions(species_suggestions)
        elif not genus:
            self._set_ref_species_placeholder_from_suggestions([])
        self._update_ref_vernacular_suggestions_for_taxon()
        if self._ref_species_completer and not self.ref_species_input.hasFocus():
            self._ref_species_completer.setCompletionPrefix("")
        if self._ref_vernacular_completer and not self.ref_vernacular_input.hasFocus():
            self._ref_vernacular_completer.setCompletionPrefix("")
        if not self.ref_genus_input.hasFocus() or self.ref_species_input.text().strip():
            self._populate_reference_panel_sources(auto_select_single=False)
        self._update_reference_add_state()

    def _on_ref_species_text_changed(self, text):
        if self._ref_completer_suppress:
            return
        genus = self._clean_ref_genus_text(self.ref_genus_input.text())
        clean_text = self._clean_ref_species_text(text)
        if self.ref_species_input.hasFocus() and hasattr(self, "ref_source_input") and self.ref_source_input.currentText().strip():
            self.ref_source_input.blockSignals(True)
            self.ref_source_input.setCurrentText("")
            self.ref_source_input.blockSignals(False)
        if genus:
            species_suggestions = self._update_ref_species_suggestions(genus, clean_text or "", hide_on_exact=True)
            if not clean_text:
                self._set_ref_species_placeholder_from_suggestions(species_suggestions)
            if self.ref_species_input.hasFocus() and self._ref_species_model.rowCount() > 0:
                self._ref_species_completer.setCompletionPrefix(clean_text or "")
                QTimer.singleShot(0, self._ref_species_completer.complete)
        else:
            self._ref_species_model.clear()
            self._set_ref_species_placeholder_from_suggestions([])
            if self._ref_species_completer and self._ref_species_completer.popup():
                self._ref_species_completer.popup().hide()
        if not clean_text:
            self._update_ref_vernacular_suggestions_for_taxon()
        self._populate_reference_panel_sources(auto_select_single=False)
        self._update_reference_add_state()

    def _on_ref_genus_selected(self, index: QModelIndex):
        if self._ref_completer_suppress:
            return
        self._suppress_ref_completer_updates()
        value = ""
        if isinstance(index, QModelIndex) and index.isValid():
            value = (index.data(Qt.DisplayRole) or "").strip()
        if not value:
            value = (self.ref_genus_input.text() or "").strip()
        if not value:
            return
        value = self._clean_ref_genus_text(value)
        self.ref_genus_input.blockSignals(True)
        self.ref_genus_input.setText(value)
        self.ref_genus_input.blockSignals(False)
        if hasattr(self, "ref_vernacular_input"):
            self.ref_vernacular_input.blockSignals(True)
            self.ref_vernacular_input.setText("")
            self.ref_vernacular_input.blockSignals(False)
        self.ref_species_input.blockSignals(True)
        self.ref_species_input.setText("")
        self.ref_species_input.blockSignals(False)
        if hasattr(self, "ref_source_input"):
            self.ref_source_input.blockSignals(True)
            self.ref_source_input.setCurrentText("")
            self.ref_source_input.blockSignals(False)
        if self._ref_genus_completer.popup():
            self._ref_genus_completer.popup().hide()
        self._ref_genus_model.setStringList([])
        self._ref_genus_completer.setCompletionPrefix("")
        if self._ref_species_completer:
            self._ref_species_completer.setCompletionPrefix("")
        if self._ref_vernacular_completer:
            self._ref_vernacular_completer.setCompletionPrefix("")
        self._update_ref_genus_suggestions(value)
        species_text = self._clean_ref_species_text(self.ref_species_input.text())
        species_suggestions = self._update_ref_species_suggestions(value, species_text or "")
        if not species_text:
            self._set_ref_species_placeholder_from_suggestions(species_suggestions)
        self._update_ref_vernacular_suggestions_for_taxon()
        self._populate_reference_panel_sources()
        self._update_reference_add_state()

    def _on_ref_species_selected(self, index: QModelIndex):
        if self._ref_completer_suppress:
            return
        self._suppress_ref_completer_updates()
        value = ""
        if isinstance(index, QModelIndex) and index.isValid():
            value = (index.data(Qt.UserRole) or index.data(Qt.DisplayRole) or "").strip()
        if not value:
            value = (self.ref_species_input.text() or "").strip()
        if not value:
            return
        value = self._clean_ref_species_text(value)
        self.ref_species_input.blockSignals(True)
        self.ref_species_input.setText(value)
        self.ref_species_input.blockSignals(False)
        if hasattr(self, "ref_source_input"):
            self.ref_source_input.blockSignals(True)
            self.ref_source_input.setCurrentText("")
            self.ref_source_input.blockSignals(False)
        if self._ref_species_completer.popup():
            self._ref_species_completer.popup().hide()
        self._ref_species_model.clear()
        self._ref_species_completer.setCompletionPrefix("")
        QTimer.singleShot(0, lambda: self._ref_species_completer.popup().hide() if self._ref_species_completer.popup() else None)
        self._populate_reference_panel_sources()
        self._maybe_load_reference_panel_reference()
        self._sync_ref_vernacular_from_taxon()
        self._update_reference_add_state()

    def _on_ref_vernacular_selected(self, index: QModelIndex):
        if self._ref_completer_suppress:
            return
        self._suppress_ref_completer_updates()
        value = ""
        if isinstance(index, QModelIndex) and index.isValid():
            value = (index.data(Qt.UserRole) or index.data(Qt.DisplayRole) or "").strip()
        if not value:
            value = (self.ref_vernacular_input.text() or "").strip()
        if not value:
            return
        self.ref_vernacular_input.blockSignals(True)
        self.ref_vernacular_input.setText(value)
        self.ref_vernacular_input.blockSignals(False)
        if self._ref_vernacular_completer.popup():
            self._ref_vernacular_completer.popup().hide()
        self._ref_vernacular_model.clear()
        self._ref_vernacular_completer.setCompletionPrefix("")

    def _on_ref_genus_popup_clicked(self, index: QModelIndex):
        self._on_ref_genus_selected(index)

    def _on_ref_species_popup_clicked(self, index: QModelIndex):
        self._on_ref_species_selected(index)

    def _on_ref_vernacular_popup_clicked(self, index: QModelIndex):
        self._on_ref_vernacular_selected(index)

    def _on_ref_taxon_editing_finished(self):
        genus_clean = self._clean_ref_genus_text(self.ref_genus_input.text())
        if genus_clean != self.ref_genus_input.text().strip():
            self.ref_genus_input.blockSignals(True)
            self.ref_genus_input.setText(genus_clean)
            self.ref_genus_input.blockSignals(False)
        self._populate_reference_panel_sources()
        self._maybe_load_reference_panel_reference()
        self._sync_ref_vernacular_from_taxon()
        self._update_reference_add_state()

    def _on_ref_source_changed(self, _text):
        self._maybe_load_reference_panel_reference()
        self._update_reference_add_state()

    def _on_ref_vernacular_text_changed(self, text):
        if self._ref_completer_suppress:
            return
        did_clear_dependencies = False
        if self.ref_vernacular_input.hasFocus() and text.strip():
            if self._clean_ref_genus_text(self.ref_genus_input.text()):
                self.ref_genus_input.blockSignals(True)
                self.ref_genus_input.setText("")
                self.ref_genus_input.blockSignals(False)
                did_clear_dependencies = True
            if self.ref_species_input.text().strip():
                self.ref_species_input.blockSignals(True)
                self.ref_species_input.setText("")
                self.ref_species_input.blockSignals(False)
                self._ref_species_model.clear()
                did_clear_dependencies = True
            if hasattr(self, "ref_source_input") and self.ref_source_input.currentText().strip():
                self.ref_source_input.blockSignals(True)
                self.ref_source_input.setCurrentText("")
                self.ref_source_input.blockSignals(False)
                did_clear_dependencies = True
            if did_clear_dependencies:
                self._populate_reference_panel_sources(auto_select_single=False)
        if not self.ref_vernacular_db:
            self._ref_vernacular_model.clear()
            return
        genus = self._clean_ref_genus_text(self.ref_genus_input.text()) or None
        species = self.ref_species_input.text().strip() or None
        suggestions = self.ref_vernacular_db.suggest_vernacular(text, genus=genus, species=species)
        text_lower = text.strip().lower()
        if text_lower and any(name.lower() == text_lower for name in suggestions):
            self._ref_vernacular_model.clear()
            if self._ref_vernacular_completer:
                self._ref_vernacular_completer.popup().hide()
            return
        self._populate_ref_vernacular_model(suggestions)
        if not text.strip():
            self._set_ref_vernacular_placeholder_from_suggestions(suggestions)

    def _on_ref_vernacular_editing_finished(self):
        if not self.ref_vernacular_db:
            return
        name = self.ref_vernacular_input.text().strip()
        if not name:
            return
        taxon = self.ref_vernacular_db.taxon_from_vernacular(name)
        if taxon:
            genus, species, _family = taxon
            self._ref_taxon_fill_from_vernacular = True
            try:
                self.ref_genus_input.setText(genus)
                self.ref_species_input.setText(species)
                self._populate_reference_panel_sources()
            finally:
                self._ref_taxon_fill_from_vernacular = False

    def _maybe_set_ref_vernacular_from_taxon(self):
        if not self.ref_vernacular_db:
            self._set_ref_vernacular_placeholder_from_suggestions([])
            return
        if self.ref_vernacular_input.text().strip():
            return
        genus = self._clean_ref_genus_text(self.ref_genus_input.text())
        species = self._clean_ref_species_text(self.ref_species_input.text())
        if not genus or not species:
            self._set_ref_vernacular_placeholder_from_suggestions([])
            return
        suggestions = self.ref_vernacular_db.suggest_vernacular_for_taxon(genus=genus, species=species)
        if not suggestions:
            self._set_ref_vernacular_placeholder_from_suggestions([])
            return
        if len(suggestions) == 1:
            self.ref_vernacular_input.setText(suggestions[0])
            self._set_ref_vernacular_placeholder_from_suggestions([])
        else:
            self._set_ref_vernacular_placeholder_from_suggestions(suggestions)

    def _sync_ref_vernacular_from_taxon(self) -> None:
        if not self.ref_vernacular_db:
            self._set_ref_vernacular_placeholder_from_suggestions([])
            return
        genus = self._clean_ref_genus_text(self.ref_genus_input.text())
        species = self._clean_ref_species_text(self.ref_species_input.text())
        current = self.ref_vernacular_input.text().strip()
        if not genus or not species:
            if current:
                self.ref_vernacular_input.blockSignals(True)
                self.ref_vernacular_input.setText("")
                self.ref_vernacular_input.blockSignals(False)
            self._set_ref_vernacular_placeholder_from_suggestions([])
            return

        suggestions = self.ref_vernacular_db.suggest_vernacular_for_taxon(genus=genus, species=species)
        current_taxon = self.ref_vernacular_db.taxon_from_vernacular(current) if current else None
        current_matches = bool(
            current_taxon
            and str(current_taxon[0] or "").strip().lower() == genus.lower()
            and str(current_taxon[1] or "").strip().lower() == species.lower()
        )
        if current_matches:
            self._set_ref_vernacular_placeholder_from_suggestions([])
            return

        new_value = suggestions[0] if suggestions else ""
        if current != new_value:
            self.ref_vernacular_input.blockSignals(True)
            self.ref_vernacular_input.setText(new_value)
            self.ref_vernacular_input.blockSignals(False)
        self._set_ref_vernacular_placeholder_from_suggestions([] if new_value else suggestions)

    def _populate_reference_panel_sources(self, auto_select_single: bool = True):
        if not hasattr(self, "ref_source_input"):
            return
        genus = self._clean_ref_genus_text(self.ref_genus_input.text())
        species = self._clean_ref_species_text(self.ref_species_input.text())
        current = self.ref_source_input.currentText().strip()
        current_data = self.ref_source_input.currentData()
        current_is_points = isinstance(current_data, dict) and current_data.get("kind") == "points"
        allow_points = self._reference_allow_points()
        values = self._reference_source_options(genus, species)
        self.ref_source_input.blockSignals(True)
        self.ref_source_input.clear()
        self.ref_source_input.addItem("")
        for label, data in values:
            self.ref_source_input.addItem(label)
            self.ref_source_input.setItemData(self.ref_source_input.count() - 1, data)
        self.ref_source_input.blockSignals(False)
        if current and (not current_is_points or allow_points):
            idx = self.ref_source_input.findText(current)
            if idx >= 0:
                self.ref_source_input.setCurrentIndex(idx)
            else:
                self.ref_source_input.setCurrentText(current)
        elif auto_select_single and len(values) == 1:
            self.ref_source_input.setCurrentIndex(1)
        else:
            self.ref_source_input.setCurrentIndex(0)
        self._update_reference_add_state()

    def _reference_source_options(self, genus: str, species: str) -> list[tuple[str, dict]]:
        options: list[tuple[str, dict]] = []
        if not genus or not species:
            return options
        allow_points = self._reference_allow_points()
        exclude_id = self.active_observation_id if hasattr(self, "active_observation_id") else None
        info = self.species_availability.get_detailed_info(
            genus,
            species,
            exclude_observation_id=exclude_id,
        )
        if allow_points:
            if info.get("personal_count", 0) > 0:
                options.append(
                    (self.tr("Personal measurements"), {"kind": "points", "source_type": "personal"})
                )
            if info.get("shared_count", 0) > 0:
                options.append(
                    (self.tr("Shared measurements"), {"kind": "points", "source_type": "shared"})
                )
            if info.get("published_count", 0) > 0:
                options.append(
                    (self.tr("Published measurements"), {"kind": "points", "source_type": "published"})
                )
        personal_obs = ObservationDB.get_personal_observations_for_species(
            genus, species, exclude_observation_id=exclude_id
        )
        for obs in personal_obs:
            date_str = (obs.get("date") or "").split(" ")[0].split("T")[0]
            author = (obs.get("author") or "").strip()
            label = f"{self.tr('My data')} {date_str}".strip() if date_str else self.tr("My data")
            options.append((
                label,
                {
                    "kind": "observation",
                    "observation_id": int(obs["id"]),
                    "date": date_str,
                    "author": author,
                    "genus": genus,
                    "species": species,
                },
            ))
        sources = ReferenceDB.list_sources(genus or "", species or "", "")
        for source in sources:
            if not source:
                continue
            options.append((source, {"kind": "reference", "source": source}))
        return options

    def _reference_stats_from_points(self, points: list[dict]) -> dict:
        if not points:
            return {}
        L = np.array([row["length_um"] for row in points if row.get("length_um") is not None], dtype=float)
        W = np.array([row["width_um"] for row in points if row.get("width_um") is not None], dtype=float)
        if L.size == 0 or W.size == 0:
            return {}
        Q = L / W
        return {
            "length_min": float(np.min(L)),
            "length_p05": float(np.percentile(L, 5)),
            "length_p50": float(np.percentile(L, 50)),
            "length_p95": float(np.percentile(L, 95)),
            "length_max": float(np.max(L)),
            "length_avg": float(np.mean(L)),
            "width_min": float(np.min(W)),
            "width_p05": float(np.percentile(W, 5)),
            "width_p50": float(np.percentile(W, 50)),
            "width_p95": float(np.percentile(W, 95)),
            "width_max": float(np.max(W)),
            "width_avg": float(np.mean(W)),
            "q_min": float(np.min(Q)),
            "q_p50": float(np.percentile(Q, 50)),
            "q_max": float(np.max(Q)),
            "q_avg": float(np.mean(Q)),
        }

    def _maybe_load_reference_panel_reference(self):
        genus = self._clean_ref_genus_text(self.ref_genus_input.text())
        species = self._clean_ref_species_text(self.ref_species_input.text())
        if not genus or not species:
            return
        source = self.ref_source_input.currentText().strip() or None
        data = self.ref_source_input.currentData()
        if isinstance(data, dict) and data.get("kind") == "points":
            if not self._reference_allow_points():
                self.reference_values = {}
                self._apply_reference_panel_values({})
                return
            source_type = data.get("source_type") or "personal"
            exclude_id = self.active_observation_id if hasattr(self, "active_observation_id") else None
            points = MeasurementDB.get_measurements_for_species(
                genus,
                species,
                source_type=source_type,
                measurement_category="spores",
                exclude_observation_id=exclude_id,
            )
            if points:
                stats = self._reference_stats_from_points(points)
                ref = {
                    **stats,
                    "points": points,
                    "points_label": self.ref_source_input.currentText().strip(),
                    "source_kind": "points",
                    "source_type": source_type,
                    "genus": genus,
                    "species": species,
                }
                self.reference_values = ref
                self._apply_reference_panel_values(ref)
                return
            self.reference_values = {}
            self._apply_reference_panel_values({})
            return
        if isinstance(data, dict) and data.get("kind") == "observation":
            obs_id = data.get("observation_id")
            if obs_id:
                raw = MeasurementDB.get_measurements_for_observation(obs_id)
                points = [
                    m for m in raw
                    if m.get("length_um") is not None
                    and m.get("width_um") is not None
                    and (m.get("measurement_type") in (None, "", "manual", "spore", "spores"))
                ]
                author = (data.get("author") or "").strip()
                if not author:
                    obs = ObservationDB.get_observation(obs_id)
                    if obs:
                        author = (obs.get("author") or "").strip()
                if points:
                    stats = self._reference_stats_from_points(points)
                    ref = {
                        **stats,
                        "points": points,
                        "source_kind": "observation",
                        "observation_id": obs_id,
                        "date": data.get("date") or "",
                        "author": author,
                        "genus": genus,
                        "species": species,
                    }
                    self.reference_values = ref
                    self._apply_reference_panel_values(ref)
                    return
            self.reference_values = {}
            self._apply_reference_panel_values({})
            return
        if isinstance(data, dict) and data.get("kind") == "reference":
            source = data.get("source") or source
        ref = ReferenceDB.get_reference(genus, species, source)
        if ref:
            ref["source_kind"] = "reference"
            self.reference_values = ref
            self._apply_reference_panel_values(ref)

    def _reference_panel_cell_value(self, row, col):
        if not hasattr(self, "ref_table"):
            return None
        item = self.ref_table.item(row, col)
        if not item:
            return None
        try:
            return float(item.text().strip())
        except ValueError:
            return None

    def _reference_panel_get_data(self):
        data = {
            "genus": self._clean_ref_genus_text(self.ref_genus_input.text()) or None,
            "species": self._clean_ref_species_text(self.ref_species_input.text()) or None,
            "source": self.ref_source_input.currentText().strip() or None,
        }
        if not hasattr(self, "ref_table"):
            return data
        data.update({
            "length_min": self._reference_panel_cell_value(0, 0),
            "length_p05": self._reference_panel_cell_value(0, 1),
            "length_p50": self._reference_panel_cell_value(0, 2),
            "length_p95": self._reference_panel_cell_value(0, 3),
            "length_max": self._reference_panel_cell_value(0, 4),
            "width_min": self._reference_panel_cell_value(1, 0),
            "width_p05": self._reference_panel_cell_value(1, 1),
            "width_p50": self._reference_panel_cell_value(1, 2),
            "width_p95": self._reference_panel_cell_value(1, 3),
            "width_max": self._reference_panel_cell_value(1, 4),
            "q_min": self._reference_panel_cell_value(2, 0),
            "q_p50": self._reference_panel_cell_value(2, 2),
            "q_max": self._reference_panel_cell_value(2, 4),
        })
        return data

    def _apply_reference_panel_values(self, ref_values):
        if not hasattr(self, "ref_table"):
            return
        self.ref_table.blockSignals(True)
        self.ref_table.clearContents()
        self.ref_table.blockSignals(False)
        if not ref_values:
            return

        def _set_cell(row, col, value):
            if value is None:
                return
            item = QTableWidgetItem(f"{value:g}")
            self.ref_table.setItem(row, col, item)

        _set_cell(0, 0, ref_values.get("length_min"))
        _set_cell(0, 1, ref_values.get("length_p05"))
        _set_cell(0, 2, ref_values.get("length_p50"))
        _set_cell(0, 3, ref_values.get("length_p95"))
        _set_cell(0, 4, ref_values.get("length_max"))
        _set_cell(1, 0, ref_values.get("width_min"))
        _set_cell(1, 1, ref_values.get("width_p05"))
        _set_cell(1, 2, ref_values.get("width_p50"))
        _set_cell(1, 3, ref_values.get("width_p95"))
        _set_cell(1, 4, ref_values.get("width_max"))
        _set_cell(2, 0, ref_values.get("q_min"))
        _set_cell(2, 2, ref_values.get("q_p50"))
        _set_cell(2, 4, ref_values.get("q_max"))

    def _on_reference_panel_plot_clicked(self):
        source_data = self.ref_source_input.currentData()
        if source_data:
            self._maybe_load_reference_panel_reference()
            data = self.reference_values
        elif isinstance(self.reference_values, dict) and self.reference_values:
            data = self.reference_values
        else:
            data = self._reference_panel_get_data()
        if not isinstance(data, dict) or not data:
            return
        if not (data.get("genus") and data.get("species")):
            return
        self.reference_values = data
        self._add_reference_series_entry(data)

    def _on_reference_panel_add_clicked(self):
        genus = self._clean_ref_genus_text(self.ref_genus_input.text())
        species = self._clean_ref_species_text(self.ref_species_input.text())
        if not genus or not species:
            return
        vernacular = self.ref_vernacular_input.text().strip() if hasattr(self, "ref_vernacular_input") else ""
        dialog = ReferenceAddDialog(self, genus, species, vernacular=vernacular)
        if dialog.exec() != QDialog.Accepted:
            return
        data = dialog.result_data()
        if not isinstance(data, dict) or not data:
            return
        if data.get("source_kind") == "reference":
            ReferenceDB.set_reference(data)
            self._refresh_reference_species_availability()
            self._populate_reference_panel_sources()
            if data.get("source"):
                idx = self.ref_source_input.findText(data.get("source"))
                if idx >= 0:
                    self.ref_source_input.setCurrentIndex(idx)
                else:
                    self.ref_source_input.setCurrentText(data.get("source"))
        self.reference_values = data
        self._add_reference_series_entry(data)

    def _on_reference_panel_edit_clicked(self):
        genus = self._clean_ref_genus_text(self.ref_genus_input.text())
        species = self._clean_ref_species_text(self.ref_species_input.text())
        if not genus or not species:
            return
        source_text = self.ref_source_input.currentText().strip()
        if not source_text:
            return
        self._maybe_load_reference_panel_reference()
        data = self.reference_values if isinstance(self.reference_values, dict) else {}
        source_data = self.ref_source_input.currentData()
        allow_delete = bool(isinstance(source_data, dict) and source_data.get("kind") == "reference")
        vernacular = self.ref_vernacular_input.text().strip() if hasattr(self, "ref_vernacular_input") else ""
        dialog = ReferenceAddDialog(
            self,
            genus,
            species,
            vernacular=vernacular,
            data=data,
            title=self.tr("Edit selected reference data"),
            allow_delete=allow_delete,
        )
        if dialog.exec() != QDialog.Accepted:
            return
        if dialog.delete_requested():
            ReferenceDB.delete_reference(
                genus,
                species,
                data.get("source"),
                data.get("mount_medium"),
                data.get("stain"),
            )
            key = self._reference_series_key(data)
            self._refresh_reference_species_availability()
            self.ref_source_input.blockSignals(True)
            self.ref_source_input.setCurrentText("")
            self.ref_source_input.blockSignals(False)
            self.reference_values = {}
            self._apply_reference_panel_values({})
            if key:
                self._remove_reference_series_key(key)
            self._populate_reference_panel_sources(auto_select_single=False)
            self._update_reference_add_state()
            return
        updated = dialog.result_data()
        if not isinstance(updated, dict) or not updated:
            return
        if updated.get("source_kind") == "reference":
            ReferenceDB.set_reference(updated)
            self._refresh_reference_species_availability()
            self._populate_reference_panel_sources()
            if updated.get("source"):
                idx = self.ref_source_input.findText(updated.get("source"))
                if idx >= 0:
                    self.ref_source_input.setCurrentIndex(idx)
                else:
                    self.ref_source_input.setCurrentText(updated.get("source"))
        self.reference_values = updated
        self._add_reference_series_entry(updated)

    def _on_reference_panel_save_clicked(self):
        data = self._reference_panel_get_data()
        if not (data.get("genus") and data.get("species")):
            QMessageBox.warning(
                self,
                self.tr("Missing Species"),
                self.tr("Please enter genus and species to save.")
            )
            return
        self._handle_reference_save(data)

    def _on_reference_panel_cloud_clicked(self):
        genus = self._clean_ref_genus_text(self.ref_genus_input.text())
        species = self._clean_ref_species_text(self.ref_species_input.text())
        if not genus or not species:
            return
        vernacular = self.ref_vernacular_input.text().strip() if hasattr(self, "ref_vernacular_input") else ""
        dialog = CloudReferenceDialog(
            self,
            genus=genus,
            species=species,
            vernacular=vernacular,
        )
        if dialog.exec() != QDialog.Accepted:
            return
        action = dialog.accepted_action()
        data = dialog.accepted_data()
        if not action or not isinstance(data, dict) or not data:
            return
        if action == "import_summary":
            ReferenceDB.set_reference(data)
            self._refresh_reference_species_availability()
            self._populate_reference_panel_sources()
            source = (data.get("source") or "").strip()
            if source:
                idx = self.ref_source_input.findText(source)
                if idx >= 0:
                    self.ref_source_input.setCurrentIndex(idx)
                else:
                    self.ref_source_input.setCurrentText(source)
            self.reference_values = data
            self._apply_reference_panel_values(data)
            self._add_reference_series_entry(data)
            return
        if action == "plot_points":
            self.reference_values = data
            self._apply_reference_panel_values(data)
            self._add_reference_series_entry(data)

    def _on_reference_panel_clear_clicked(self):
        self.ref_vernacular_input.setText("")
        self.ref_genus_input.setText("")
        self.ref_species_input.setText("")
        self.ref_source_input.setCurrentText("")
        self.reference_values = {}
        self._set_reference_series([])
        self._apply_reference_panel_values({})
        self._update_reference_add_state()
        self._save_gallery_settings()

    def create_right_panel(self):
        """Create the right panel with statistics, preview, and measurements table."""
        from PySide6.QtWidgets import QScrollArea, QGridLayout

        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setSpacing(5)
        layout.setContentsMargins(0, 0, 0, 0)

        # Measurement preview group
        self.preview_group = QGroupBox(self.tr("Measurement Fine tune"))
        preview_layout = QVBoxLayout()
        preview_layout.setContentsMargins(5, 5, 5, 5)

        self.spore_preview = SporePreviewWidget()
        self.spore_preview.dimensions_changed.connect(self.on_dimensions_changed)
        self.spore_preview.delete_requested.connect(self.delete_measurement)
        self.spore_preview.set_measure_color(self.measure_color)
        self.spore_preview.set_measurement_rectangle_appearance(
            style=self._current_measure_rectangle_style(),
            thickness=self._current_measure_rectangle_thickness(),
        )
        preview_layout.addWidget(self.spore_preview)

        self.preview_group.setLayout(preview_layout)
        layout.addWidget(self.preview_group)
        self._update_preview_title()

        # Measurements table group
        measurements_group = QGroupBox(self.tr("Measurements"))
        measurements_layout = QVBoxLayout()
        measurements_layout.setContentsMargins(5, 5, 5, 5)

        self.measurements_table = QTableWidget()
        self.measurements_table.setColumnCount(5)
        self.measurements_table.setHorizontalHeaderLabels(["Img", "Cat", "L", "W", "Q"])

        # Set column widths
        header = self.measurements_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)

        self.measurements_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.measurements_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.measurements_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.measurements_table.setAlternatingRowColors(True)
        self.measurements_table.setStyleSheet("""
            QTableWidget::item:selected {
                background-color: #d9e9f8;
                color: #1f2d3d;
            }
            QTableWidget::item:selected:!active {
                background-color: #eaf3ff;
                color: #1f2d3d;
            }
        """)
        self.measurements_table.itemSelectionChanged.connect(self.on_measurement_selected)

        measurements_layout.addWidget(self.measurements_table)

        measurements_group.setLayout(measurements_layout)
        layout.addWidget(measurements_group)

        return panel

    def load_default_objective(self):
        """Load the default or last used objective."""
        dialog = CalibrationDialog(self)
        objective = dialog.get_last_used_objective()
        if objective:
            self.apply_objective(objective)

    def open_calibration_dialog(self, select_custom=False, objective_key=None, calibration_id=None):
        """Open the calibration dialog."""
        dialog = CalibrationDialog(self)
        if select_custom:
            dialog.select_custom_tab()
        if objective_key:
            dialog.select_objective_key(objective_key)
        if calibration_id:
            dialog.select_calibration(calibration_id)
        dialog.calibration_saved.connect(self._on_calibration_saved_from_dialog)
        return dialog.exec() == QDialog.Accepted

    def _on_calibration_link_clicked(self, _link: str) -> None:
        objective_key = getattr(self, "_calib_link_objective_key", None)
        calibration_id = getattr(self, "_calib_link_calibration_id", None)
        if not objective_key or not calibration_id:
            return
        self.open_calibration_dialog(
            select_custom=False,
            objective_key=objective_key,
            calibration_id=calibration_id,
        )

    def _on_calibration_saved_from_dialog(self, objective: dict) -> None:
        if self.active_observation_id:
            self._refresh_active_observation_after_calibration()
            return
        self.apply_objective(objective)

    def _refresh_active_observation_after_calibration(self) -> None:
        if not self.active_observation_id:
            return
        display_name = self.active_observation_name
        if not display_name:
            obs = ObservationDB.get_observation(self.active_observation_id)
            if obs:
                display_name = obs.get("display_name") or obs.get("name") or obs.get("species") or ""
        if not display_name:
            display_name = f"Observation {self.active_observation_id}"
        self._on_observation_selected_impl(
            self.active_observation_id,
            display_name,
            switch_tab=False,
            schedule_gallery=True,
        )

    def _populate_scale_combo(self, selected_key=None):
        """Populate the scale combo with objectives and Custom."""
        if not hasattr(self, "scale_combo"):
            return
        objectives = self.load_objective_definitions()
        custom_key = "__from_scalebar__"

        def _sort_key(item):
            key, obj = item
            return objective_sort_value(obj, key)

        self.scale_combo.blockSignals(True)
        self.scale_combo.clear()
        self.scale_combo.addItem(self.tr("Not set"), "")
        self.scale_combo.addItem(self.tr("From scalebar"), custom_key)
        for key, obj in sorted(objectives.items(), key=_sort_key):
            label = objective_display_name(obj, key) or key
            self.scale_combo.addItem(label, key)
        if selected_key is None:
            selected_key = self.current_objective_name
        if selected_key == "Custom":
            selected_key = custom_key
        if selected_key:
            idx = self.scale_combo.findData(selected_key)
            if idx >= 0:
                self.scale_combo.setCurrentIndex(idx)
        self.scale_combo.blockSignals(False)
        self._last_scale_combo_key = self.scale_combo.currentData()

    def on_scale_combo_changed(self):
        """Handle objective selection from the scale combo."""
        if not hasattr(self, "scale_combo"):
            return
        previous_key = getattr(self, "_last_scale_combo_key", None)
        selected_key = self.scale_combo.currentData()
        custom_key = "__from_scalebar__"
        if not selected_key:
            self.current_objective = None
            self.current_objective_name = None
            self.microns_per_pixel = 0.0
            self._last_scale_combo_key = ""
            self._set_custom_scale_info(None)
            self.image_label.set_microns_per_pixel(self.microns_per_pixel)
            if self.current_pixmap:
                self.image_label.set_objective_text("")
            if self.current_image_id:
                ImageDB.update_image(
                    self.current_image_id,
                    scale=0.0,
                    objective_name="",
                    calibration_id=None,
                )
            self._update_scalebar_controls_visibility()
            return
        if selected_key == custom_key:
            image_data = ImageDB.get_image(self.current_image_id) if self.current_image_id else None
            image_scale = image_data.get("scale_microns_per_pixel") if image_data else None
            try:
                resolved_scale = float(image_scale) if image_scale is not None and float(image_scale) > 0 else 0.0
            except Exception:
                resolved_scale = 0.0
            if resolved_scale <= 0:
                resolved_scale = float(self.microns_per_pixel or 0.0)
            if resolved_scale > 0:
                self.set_custom_scale(resolved_scale)
            else:
                self.scale_combo.blockSignals(True)
                self.scale_combo.setCurrentIndex(0)
                self.scale_combo.blockSignals(False)
                self._last_scale_combo_key = ""
            self._update_scalebar_controls_visibility()
            return

        objectives = self.load_objective_definitions()
        objective = objectives.get(selected_key)
        if objective:
            objective_data = dict(objective)
            objective_data["key"] = selected_key
            if not self.apply_objective(objective_data):
                self.scale_combo.blockSignals(True)
                if previous_key is None:
                    self.scale_combo.setCurrentIndex(0)
                else:
                    idx = self.scale_combo.findData(previous_key)
                    if idx >= 0:
                        self.scale_combo.setCurrentIndex(idx)
                self.scale_combo.blockSignals(False)
                return
            self._last_scale_combo_key = selected_key
        self._update_scalebar_controls_visibility()

    def _set_calibration_info(self, objective_key: str | None, calibration_id: int | None = None) -> None:
        if not hasattr(self, "calib_info_label"):
            return
        self._calib_link_objective_key = None
        self._calib_link_calibration_id = None
        self.current_calibration_id = None
        objective_name = (objective_key or "").strip()
        if not objective_name:
            self.calib_info_label.setText(self.tr("Calibration: --"))
            return

        active_calibration_id = calibration_id
        if not active_calibration_id:
            active_calibration_id = CalibrationDB.get_active_calibration_id(objective_name)
        self.current_calibration_id = active_calibration_id

        calib_obj_key = objective_name
        calib_date = None
        if active_calibration_id:
            calibration = CalibrationDB.get_calibration(active_calibration_id)
            if calibration:
                raw_date = calibration.get("calibration_date", "")
                calib_date = raw_date[:10] if raw_date else None
                calib_obj_key = calibration.get("objective_key") or calib_obj_key
        if calib_date and active_calibration_id:
            self._calib_link_objective_key = calib_obj_key
            self._calib_link_calibration_id = active_calibration_id
            self.calib_info_label.setText(
                self.tr("Calibration: <a href=\"calibration\">{date}</a>").format(date=calib_date)
            )
        else:
            self.calib_info_label.setText(self.tr("Calibration: --"))

    def _set_custom_scale_info(self, scale_um: float | None) -> None:
        if not hasattr(self, "calib_info_label"):
            return
        self._calib_link_objective_key = None
        self._calib_link_calibration_id = None
        self.current_calibration_id = None
        if scale_um and scale_um > 0:
            scale_nm = float(scale_um) * 1000.0
            self.calib_info_label.setText(self.tr("Scale: {scale:.1f} nm/px").format(scale=scale_nm))
        else:
            self.calib_info_label.setText(self.tr("Scale: -- nm/px"))

    def _has_objective_calibration_selected(self) -> bool:
        key = (self.current_objective_name or "").strip()
        if not key:
            return False
        if key.lower() == "custom":
            return False
        return True

    def _update_scalebar_controls_visibility(self) -> None:
        hide_scalebar_controls = self._has_objective_calibration_selected()
        if hasattr(self, "set_from_scalebar_btn"):
            self.set_from_scalebar_btn.setVisible(not hide_scalebar_controls)
        if hasattr(self, "scale_bar_inline"):
            self.scale_bar_inline.setVisible(not hide_scalebar_controls)

    def apply_objective(self, objective):
        """Apply an objective's settings."""
        old_scale = self.microns_per_pixel
        previous_key = self.current_objective_name
        new_scale = objective.get("microns_per_pixel", 0.5)
        if not self._maybe_rescale_current_image(old_scale, new_scale):
            self._populate_scale_combo(previous_key)
            return False
        objective_key = objective.get("key") or objective.get("objective_key")
        if not objective_key:
            objective_key = objective.get("magnification") or objective.get("name")
        self.current_objective = objective
        self.current_objective_name = objective_key
        self.microns_per_pixel = new_scale

        # Update calibration info
        display_name = objective_display_name(objective, objective_key) or str(objective_key or "Unknown")
        tag_text = self._objective_tag_text(display_name, objective_key)
        self._set_calibration_info(objective_key)
        self._update_field_scale_label()

        # Update image overlay
        if self.current_pixmap:
            self.image_label.set_objective_text(tag_text)
            self.image_label.set_objective_color(self._objective_color_for_name(tag_text))
        self.image_label.set_microns_per_pixel(self.microns_per_pixel)

        if self.current_image_id:
            calibration_id = CalibrationDB.get_active_calibration_id(objective_key) if objective_key else None
            ImageDB.update_image(
                self.current_image_id,
                scale=self.microns_per_pixel,
                objective_name=self.current_objective_name,
                calibration_id=calibration_id,
            )
        self._populate_scale_combo(self.current_objective_name)
        self._last_scale_combo_key = self.current_objective_name
        self._update_scale_mismatch_warning()
        self._update_measure_view_option_states()
        self._update_scalebar_controls_visibility()
        return True

    def load_objective_definitions(self):
        """Load objective definitions from the calibration database."""
        return load_objectives()

    def set_custom_scale(self, scale, warning_text=None):
        """Apply a custom scale and optionally warn about mismatches."""
        old_scale = self.microns_per_pixel
        previous_key = self.current_objective_name
        if not self._maybe_rescale_current_image(old_scale, scale):
            self._populate_scale_combo(previous_key)
            return False
        self.current_objective = {
            "name": "Custom",
            "magnification": "Custom",
            "microns_per_pixel": scale
        }
        self.current_objective_name = "Custom"
        self.microns_per_pixel = scale
        self._set_custom_scale_info(scale)
        self._update_field_scale_label()

        if self.current_pixmap:
            self.image_label.set_objective_text("Scale bar")
            self.image_label.set_objective_color(QColor("#7f8c8d"))
        self.image_label.set_microns_per_pixel(self.microns_per_pixel)

        if warning_text:
            self.measure_status_label.setText(warning_text)
            self.measure_status_label.setStyleSheet(
                f"color: #e67e22; font-weight: bold; font-size: {pt(9)}pt;"
            )

        if self.current_image_id:
            ImageDB.update_image(
                self.current_image_id,
                scale=scale,
                objective_name=self.current_objective_name,
                calibration_id=None,
            )
        self._populate_scale_combo("Custom")
        self._last_scale_combo_key = self.scale_combo.currentData() if hasattr(self, "scale_combo") else "Custom"
        self._update_scale_mismatch_warning()
        self._update_measure_view_option_states()
        self._update_scalebar_controls_visibility()
        return True

    def apply_image_scale(self, image_data):
        """Apply scale/objective metadata from an image record."""
        scale = image_data.get('scale_microns_per_pixel')
        if scale is not None and scale <= 0:
            scale = None
        objective_name = image_data.get('objective_name')
        calibration_id = image_data.get('calibration_id')
        resample_factor = image_data.get("resample_scale_factor")
        if not isinstance(resample_factor, (int, float)) or resample_factor <= 0:
            resample_factor = 1.0
        objective_lookup = self.load_objective_definitions()
        show_old_calibration_warning = False

        resolved_key = resolve_objective_key(objective_name, objective_lookup)
        if resolved_key and resolved_key != objective_name:
            objective_name = resolved_key
            calibration_id = CalibrationDB.get_active_calibration_id(resolved_key)
            ImageDB.update_image(
                image_data.get("id"),
                objective_name=resolved_key,
                calibration_id=calibration_id,
            )

        self.current_calibration_id = calibration_id
        self.suppress_scale_prompt = True
        if objective_name and objective_name in objective_lookup:
            objective = objective_lookup[objective_name]
            objective_data = dict(objective)
            objective_data["key"] = objective_name
            objective_scale = objective.get('microns_per_pixel', 0)
            expected_scale = objective_scale
            if resample_factor < 0.999 and objective_scale:
                expected_scale = float(objective_scale) / float(resample_factor)
            if scale and objective_scale:
                diff_ratio = abs(expected_scale - scale) / max(1e-9, float(expected_scale))
            else:
                diff_ratio = 0 if scale is None else 1
            if scale and diff_ratio > 0.01 and resample_factor >= 0.999:
                self.current_objective = objective_data
                self.current_objective_name = objective_name
                self.microns_per_pixel = scale
                show_old_calibration_warning = True

                mag = objective_display_name(objective, objective_name) or objective_name
                tag_text = self._objective_tag_text(mag, objective_name)
                self._set_calibration_info(objective_name, calibration_id)

                if self.current_pixmap:
                    self.image_label.set_objective_text(tag_text)
                    self.image_label.set_objective_color(self._objective_color_for_name(tag_text))
                self.image_label.set_microns_per_pixel(self.microns_per_pixel)

                self.measure_status_label.setText(self.tr("Warning: Older calibration standard used."))
                self.measure_status_label.setStyleSheet(
                    f"color: #e67e22; font-weight: bold; font-size: {pt(9)}pt;"
                )

                self._populate_scale_combo(objective_name)
            elif scale and resample_factor < 0.999:
                self.current_objective = objective_data
                self.current_objective_name = objective_name
                self.microns_per_pixel = scale

                mag = objective_display_name(objective, objective_name) or objective_name
                tag_text = self._objective_tag_text(mag, objective_name)
                self._set_calibration_info(objective_name, calibration_id)

                if self.current_pixmap:
                    self.image_label.set_objective_text(tag_text)
                    self.image_label.set_objective_color(self._objective_color_for_name(tag_text))
                self.image_label.set_microns_per_pixel(self.microns_per_pixel)

                self._populate_scale_combo(objective_name)
            else:
                self.apply_objective(objective_data)
                if scale:
                    self.microns_per_pixel = scale
        elif scale:
            self.set_custom_scale(scale)
        elif objective_name:
            self.current_objective_name = objective_name
            self._set_calibration_info(objective_name, calibration_id)
            if self.current_pixmap:
                tag_text = self._objective_tag_text(objective_name, objective_name)
                self.image_label.set_objective_text(tag_text)
                self.image_label.set_objective_color(self._objective_color_for_name(tag_text))
            self._populate_scale_combo(objective_name)
        else:
            self._populate_scale_combo()
        if (
            not show_old_calibration_warning
            and hasattr(self, "measure_status_label")
            and self.measure_status_label.text() == self.tr("Warning: Older calibration standard used.")
        ):
            self.measure_status_label.setText("")
            self.measure_status_label.setStyleSheet("")
        self.suppress_scale_prompt = False
        self._update_scale_mismatch_warning()
        self._update_measure_view_option_states()
        self._update_scalebar_controls_visibility()
        if image_data.get("image_type") == "field" and (scale is None or scale <= 0):
            self.microns_per_pixel = 0.0
        self._update_field_scale_label()

    def _format_megapixels(self, mp_value: float) -> str:
        text = f"{mp_value:.1f}"
        return text.rstrip("0").rstrip(".")

    def _current_image_megapixels(self) -> float | None:
        if not self.current_pixmap or self.current_pixmap.isNull():
            return None
        width = self.current_pixmap.width()
        height = self.current_pixmap.height()
        if width <= 0 or height <= 0:
            return None
        return (width * height) / 1_000_000.0

    def _megapixels_from_path(self, path: str | None) -> float | None:
        if not path:
            return None
        reader = QImageReader(path)
        size = reader.size()
        if not size.isValid():
            return None
        width = size.width()
        height = size.height()
        if width <= 0 or height <= 0:
            return None
        return (width * height) / 1_000_000.0

    def _estimate_calibration_megapixels(self, cal: dict | None) -> float | None:
        if not cal:
            return None
        values: list[float] = []
        measurements_json = cal.get("measurements_json")
        if measurements_json:
            try:
                loaded = json.loads(measurements_json)
            except Exception:
                loaded = None
            if isinstance(loaded, dict):
                for info in loaded.get("images") or []:
                    mp = self._megapixels_from_path(info.get("path"))
                    if mp:
                        values.append(mp)
        if not values:
            mp = self._megapixels_from_path(cal.get("image_filepath"))
            if mp:
                values.append(mp)
        if values:
            return float(sum(values) / len(values))
        return None

    def _effective_calibration_megapixels(self, mp_value: float | None, cal: dict | None) -> float | None:
        if not mp_value:
            return mp_value
        return float(mp_value)

    def _current_calibration_megapixels(self) -> float | None:
        calibration_id = getattr(self, "current_calibration_id", None) or getattr(
            self, "_calib_link_calibration_id", None
        )
        if calibration_id:
            cal = CalibrationDB.get_calibration(calibration_id)
            mp_value = cal.get("megapixels") if cal else None
            estimate = self._estimate_calibration_megapixels(cal)
            if isinstance(mp_value, (int, float)) and mp_value > 0:
                if estimate:
                    diff_ratio = abs(float(mp_value) - float(estimate)) / max(1e-6, float(estimate))
                    if diff_ratio > 0.01:
                        return self._effective_calibration_megapixels(float(estimate), cal)
                return self._effective_calibration_megapixels(float(mp_value), cal)
            if estimate:
                return self._effective_calibration_megapixels(float(estimate), cal)
        if self.current_objective_name:
            cal = CalibrationDB.get_active_calibration(self.current_objective_name)
            mp_value = cal.get("megapixels") if cal else None
            estimate = self._estimate_calibration_megapixels(cal)
            if isinstance(mp_value, (int, float)) and mp_value > 0:
                if estimate:
                    diff_ratio = abs(float(mp_value) - float(estimate)) / max(1e-6, float(estimate))
                    if diff_ratio > 0.01:
                        return self._effective_calibration_megapixels(float(estimate), cal)
                return self._effective_calibration_megapixels(float(mp_value), cal)
            if estimate:
                return self._effective_calibration_megapixels(float(estimate), cal)
        return None

    def _update_scale_mismatch_warning(self) -> None:
        if not hasattr(self, "scale_warning_label"):
            return
        if not self.current_image_id or not self.current_pixmap:
            self.scale_warning_label.setText("")
            self.scale_warning_label.setToolTip("")
            self.scale_warning_label.setVisible(False)
            return
        if self.current_image_type != "microscope":
            self.scale_warning_label.setText("")
            self.scale_warning_label.setToolTip("")
            self.scale_warning_label.setVisible(False)
            return
        image_mp = self._current_image_megapixels()
        calibration_mp = self._current_calibration_megapixels()
        if not image_mp or not calibration_mp:
            self.scale_warning_label.setText("")
            self.scale_warning_label.setToolTip("")
            self.scale_warning_label.setVisible(False)
            return
        effective_mp = float(image_mp)
        image_data = ImageDB.get_image(self.current_image_id) if self.current_image_id else None
        factor = image_data.get("resample_scale_factor") if image_data else None
        if isinstance(factor, (int, float)) and factor > 0 and factor < 0.999:
            effective_mp = float(image_mp) / (float(factor) * float(factor))
        diff_ratio = abs(effective_mp - calibration_mp) / max(1e-6, calibration_mp)
        if diff_ratio <= 0.01:
            self.scale_warning_label.setText("")
            self.scale_warning_label.setToolTip("")
            self.scale_warning_label.setVisible(False)
            return
        ratio = max(effective_mp, calibration_mp) / max(1e-6, min(effective_mp, calibration_mp))
        if ratio < 1.5:
            self.scale_warning_label.setText("")
            self.scale_warning_label.setToolTip("")
            self.scale_warning_label.setVisible(False)
            return
        self.scale_warning_label.setText(self.tr("Warning: Image resolution mismatch!"))
        self.scale_warning_label.setToolTip(
            self.tr(
                "Calibration image: {cal}MP. This image: {img}MP. "
                "This is ok if you are working on a cropped image."
            ).format(
                cal=self._format_megapixels(calibration_mp),
                img=self._format_megapixels(effective_mp),
            )
        )
        self.scale_warning_label.setVisible(True)

    def update_controls_for_image_type(self, image_type):
        """Adjust calibration and category controls based on image type."""
        is_field = (image_type == "field")
        if hasattr(self, "scale_combo"):
            self.scale_combo.setEnabled(True)
        self._sync_scale_bar_unit_for_image_type(is_field)
        if is_field and self.measurement_active:
            self.stop_measurement()
        if hasattr(self, "measure_category_combo"):
            self.measure_category_combo.setEnabled(True)
        if hasattr(self, "measure_button"):
            self.measure_button.setEnabled(True)
        if hasattr(self, "mode_lines"):
            self.mode_lines.setEnabled(True)
        if hasattr(self, "mode_rect"):
            self.mode_rect.setEnabled(True)
        if hasattr(self, "mode_multiline"):
            self.mode_multiline.setEnabled(True)
        if hasattr(self, "measure_category_combo"):
            if not self.measurements_table.selectedIndexes() and not self.measurement_active:
                target = "field" if is_field else "spores"
                idx = self.measure_category_combo.findData(target)
                if idx >= 0:
                    self.measure_category_combo.blockSignals(True)
                    self.measure_category_combo.setCurrentIndex(idx)
                    self.measure_category_combo.blockSignals(False)
        if is_field:
            image_data = None
            if self.current_image_id:
                image_data = ImageDB.get_image(self.current_image_id)
            has_scale = False
            if image_data:
                has_scale = bool(
                    image_data.get("objective_name") or image_data.get("scale_microns_per_pixel")
                )
            if not has_scale:
                self.current_objective_name = None
                self.image_label.set_objective_text("")
        if not is_field:
            if not self.measurement_active and not self._auto_started_for_microscope:
                self.start_measurement()
                self._auto_started_for_microscope = True
        if hasattr(self, "measure_status_label"):
            if is_field and not has_scale:
                self.measure_status_label.setText(self.tr("Field photo - no scale set"))
                self.measure_status_label.setStyleSheet(f"color: #e67e22; font-weight: bold; font-size: {pt(9)}pt;")
            elif self.measure_status_label.text() == self.tr("Field photo - no scale set"):
                self.measure_status_label.setText("")
                self.measure_status_label.setStyleSheet("")
        self._update_measure_view_option_states()
        self._update_scalebar_controls_visibility()

    def _ensure_field_measure_category(self):
        if not hasattr(self, "measure_category_combo"):
            return
        idx = self.measure_category_combo.findData("field")
        if idx >= 0 and self.measure_category_combo.currentIndex() != idx:
            self.measure_category_combo.blockSignals(True)
            self.measure_category_combo.setCurrentIndex(idx)
            self.measure_category_combo.blockSignals(False)

    def _update_field_scale_label(self):
        if not hasattr(self, "calib_info_label"):
            return
        if self.current_image_type != "field":
            return
        scale_um = self.microns_per_pixel if self.microns_per_pixel and self.microns_per_pixel > 0 else None
        if scale_um:
            scale_mm = scale_um / 1000.0
            self.calib_info_label.setText(self.tr("Scale: {scale:.3f} mm/px").format(scale=scale_mm))
        else:
            self.calib_info_label.setText(self.tr("Scale: -- mm/px"))

    def _current_image_has_scale(self) -> bool:
        image_data = ImageDB.get_image(self.current_image_id) if self.current_image_id else None
        scale = image_data.get("scale_microns_per_pixel") if image_data else None
        if isinstance(scale, (int, float)) and scale > 0:
            return True
        return bool(self.microns_per_pixel and self.microns_per_pixel > 0)

    def _update_measure_view_option_states(self) -> None:
        has_scale = self._current_image_has_scale()
        is_field = (self.current_image_type or "").strip().lower() == "field"
        force_disable_overlays = bool(is_field and not has_scale)

        overlay_checkbox_names = (
            "show_measures_checkbox",
            "show_rectangles_checkbox",
            "show_scale_bar_checkbox",
        )
        for widget_name in overlay_checkbox_names:
            widget = getattr(self, widget_name, None)
            if widget is None:
                continue
            widget.setEnabled(not force_disable_overlays)
            if force_disable_overlays and widget.isChecked():
                widget.blockSignals(True)
                widget.setChecked(False)
                widget.blockSignals(False)

        copyright_widget = getattr(self, "show_copyright_checkbox", None)
        if copyright_widget is not None:
            copyright_widget.setEnabled(True)

        if force_disable_overlays and hasattr(self, "image_label"):
            self.image_label.set_show_measure_labels(False)
            self.image_label.set_show_measure_overlays(False)
        if hasattr(self, "scale_bar_input"):
            self.scale_bar_input.setEnabled(has_scale and not force_disable_overlays)
        if hasattr(self, "scale_bar_length_label"):
            self.scale_bar_length_label.setEnabled(has_scale and not force_disable_overlays)
        rectangles_enabled = bool(
            hasattr(self, "show_rectangles_checkbox")
            and self.show_rectangles_checkbox.isChecked()
            and not force_disable_overlays
        )
        if hasattr(self, "rectangle_style_container"):
            self.rectangle_style_container.setVisible(rectangles_enabled)
        for widget_name in (
            "rectangle_style_label",
            "rectangle_style_a_radio",
            "rectangle_style_b_radio",
            "rectangle_thick_label",
            "rectangle_thick_checkbox",
        ):
            widget = getattr(self, widget_name, None)
            if widget is not None:
                widget.setEnabled(rectangles_enabled)
        if hasattr(self, "scale_bar_container"):
            show_container = bool(
                (has_scale and not force_disable_overlays)
                and hasattr(self, "show_scale_bar_checkbox")
                and self.show_scale_bar_checkbox.isChecked()
            )
            self.scale_bar_container.setVisible(show_container)
        if not has_scale and hasattr(self, "image_label"):
            unit = "mm" if (self.current_image_type or "").strip().lower() == "field" else "\u03bcm"
            self.image_label.set_scale_bar(False, 0.0, unit=unit)

    @staticmethod
    def _snap_scale_bar_length_um(value_um: float) -> float:
        """Snap microscope scale-bar lengths to user-friendly whole increments."""
        value_um = max(0.1, float(value_um))
        multipliers = (1.0, 2.0, 2.5, 3.0, 5.0)
        candidates: list[float] = []
        for exp in range(0, 6):  # 1 .. 500000
            base = 10.0 ** exp
            for mul in multipliers:
                candidate = mul * base
                if 0.1 <= candidate <= 500000.0:
                    candidates.append(candidate)
        if not candidates:
            return value_um
        return min(candidates, key=lambda candidate: abs(candidate - value_um))

    def _objective_microns_per_pixel_for_key(self, objective_key: str | None) -> float | None:
        key = (objective_key or "").strip()
        if not key:
            return None
        if key == (self.current_objective_name or "") and self.microns_per_pixel and self.microns_per_pixel > 0:
            return float(self.microns_per_pixel)
        objectives = self.load_objective_definitions()
        objective = objectives.get(key)
        if not objective:
            return None
        scale = objective.get("microns_per_pixel")
        if isinstance(scale, (int, float)) and scale > 0:
            return float(scale)
        return None

    def _suggest_microscope_scale_bar_um_for_objective(self, objective_key: str | None) -> float:
        key = (objective_key or "").strip()
        if key and key in self._scale_bar_micro_manual_by_objective:
            return float(self._scale_bar_micro_manual_by_objective[key])
        basis_key = (self._scale_bar_micro_basis_objective or "").strip()
        target_mpp = self._objective_microns_per_pixel_for_key(key)
        basis_mpp = self._objective_microns_per_pixel_for_key(basis_key)
        if (
            basis_key
            and basis_key in self._scale_bar_micro_manual_by_objective
            and target_mpp
            and basis_mpp
            and basis_mpp > 0
        ):
            basis_um = float(self._scale_bar_micro_manual_by_objective[basis_key])
            derived_um = basis_um * (float(target_mpp) / float(basis_mpp))
            return float(self._snap_scale_bar_length_um(derived_um))
        return float(self._snap_scale_bar_length_um(self._scale_bar_overlay_micro_um))

    def _register_microscope_scale_bar_manual_value(self, value_um: float) -> None:
        key = (self.current_objective_name or "").strip()
        if not key or key.lower() == "custom":
            self._scale_bar_overlay_micro_um = float(self._snap_scale_bar_length_um(value_um))
            return
        snapped_um = float(self._snap_scale_bar_length_um(value_um))
        self._scale_bar_micro_manual_by_objective[key] = snapped_um
        self._scale_bar_micro_basis_objective = key
        self._scale_bar_overlay_micro_um = snapped_um

    def _sync_scale_bar_unit_for_image_type(self, is_field: bool) -> None:
        if not hasattr(self, "scale_bar_length_input"):
            return
        if is_field:
            self.scale_bar_length_input.setDecimals(1)
            self.scale_bar_length_input.setSingleStep(1.0)
            self.scale_bar_length_input.setSuffix(" mm")
            if self.scale_bar_length_input.value() <= 0:
                self.scale_bar_length_input.setValue(10.0)
        else:
            self.scale_bar_length_input.setDecimals(1)
            self.scale_bar_length_input.setSingleStep(1.0)
            self.scale_bar_length_input.setSuffix(" µm")
            if self.scale_bar_length_input.value() <= 0:
                self.scale_bar_length_input.setValue(10.0)

        if hasattr(self, "scale_bar_input"):
            if is_field:
                display_value = self._observation_scale_bar_fallback_value(True)
                suffix = " mm"
                tooltip = self.tr("Length of the displayed scale bar in millimeters.")
                current_um = display_value * 1000.0
            else:
                display_value = self._observation_scale_bar_fallback_value(
                    False,
                    objective_key=self.current_objective_name,
                )
                suffix = " \u03bcm"
                tooltip = self.tr("Length of the displayed scale bar in micrometers.")
                current_um = display_value
            self.scale_bar_input.blockSignals(True)
            self.scale_bar_input.setSuffix(suffix)
            self.scale_bar_input.setToolTip(tooltip)
            self.scale_bar_input.setValue(float(display_value))
            self.scale_bar_input.blockSignals(False)
            if (
                self._current_image_has_scale()
                and hasattr(self, "show_scale_bar_checkbox")
                and self.show_scale_bar_checkbox.isChecked()
            ):
                unit = "mm" if is_field else "\u03bcm"
                self.image_label.set_scale_bar(True, current_um, unit=unit)

    def _current_scale_bar_length_um(self) -> float:
        if not hasattr(self, "scale_bar_length_input"):
            return 10.0
        raw_value = float(self.scale_bar_length_input.value())
        if (self.current_image_type or "").strip().lower() == "field":
            return raw_value * 1000.0
        return raw_value

    def _on_set_from_scalebar_clicked(self) -> None:
        if not self.current_pixmap:
            self.measure_status_label.setText(self.tr("Load an image first to calibrate"))
            self.measure_status_label.setStyleSheet(f"color: #e74c3c; font-weight: bold; font-size: {pt(9)}pt;")
            return
        self.enter_calibration_mode(None)

    def _on_scale_bar_horizontal_toggled(self, checked: bool) -> None:
        if self.calibration_mode and hasattr(self, "image_label"):
            self.image_label.preview_line_horizontal = bool(checked)

    def _reset_calibration_interaction_state(self) -> None:
        """Reset transient scalebar-calibration interaction state."""
        self.calibration_mode = False
        self.calibration_dialog = None
        self.calibration_points = []
        self._show_calibration_overlay_for_scalebar = False
        if hasattr(self, "calibration_distance_pixels"):
            self.calibration_distance_pixels = 0.0
        self.temp_lines = []
        if hasattr(self, "image_label"):
            self.image_label.clear_preview_line()
            self.image_label.clear_preview_rectangle()

    def _default_measure_category_for_image(self, image_id: int | None, image_type: str | None) -> str:
        """Choose default category when switching images."""
        normalized_type = (image_type or "").strip().lower()
        default_category = "field" if normalized_type == "field" else "spores"
        if not image_id:
            return default_category
        measurements = MeasurementDB.get_measurements_for_image(image_id)
        if not measurements:
            return default_category
        last = measurements[-1]
        last_category = self.normalize_measurement_category(last.get("measurement_type"))
        if normalized_type == "microscope" and last_category == "calibration":
            return "spores"
        return last_category or default_category

    def _set_measure_category_for_current_image(self) -> None:
        if not hasattr(self, "measure_category_combo"):
            return
        target = self._default_measure_category_for_image(self.current_image_id, self.current_image_type)
        idx = self.measure_category_combo.findData(target)
        if idx < 0:
            idx = self.measure_category_combo.findData("spores")
        if idx < 0:
            return
        self._measure_category_sync = True
        self.measure_category_combo.blockSignals(True)
        self.measure_category_combo.setCurrentIndex(idx)
        self.measure_category_combo.blockSignals(False)
        self._measure_category_sync = False
        self._update_preview_title()
        self.update_display_lines()

    def load_image_record(self, image_data, display_name=None, refresh_table=True):
        """Load an image record into the viewer."""
        self._flush_measure_image_note()
        incoming_image_id = int(image_data.get("id") or 0)
        current_image_id = int(self.current_image_id or 0)
        if current_image_id > 0 and incoming_image_id > 0 and current_image_id != incoming_image_id:
            self._save_current_image_measure_session_view()
            self._save_current_image_measure_view_settings()

        original_path = image_data['filepath']
        output_dir = Path(__file__).parent.parent / "data" / "imports"
        converted_path = maybe_convert_heic(original_path, output_dir)
        if converted_path is None:
            QMessageBox.warning(
                self,
                "HEIC Conversion Failed",
                f"Could not convert {Path(original_path).name} to JPEG."
            )
            return

        if converted_path != original_path:
            ImageDB.update_image(image_data['id'], filepath=converted_path)
            image_data = dict(image_data)
            image_data['filepath'] = converted_path

        self.current_image_path = image_data['filepath']
        self.current_image_id = image_data['id']
        self.current_image_type = image_data.get("image_type")
        self._set_measure_image_note_text(image_data.get("notes"), enabled=True)
        self._reset_calibration_interaction_state()
        self.auto_gray_cache = None
        self.auto_gray_cache_id = None
        self._update_measure_copyright_overlay()

        self.current_pixmap = self._load_pixmap_cached(self.current_image_path)
        self.image_label.set_image(self.current_pixmap)
        self.update_exif_panel(self.current_image_path)
        QTimer.singleShot(
            0,
            lambda: self.image_label.reset_view()
            if not self._apply_measure_session_view_for_current_image()
            else None,
        )

        filename = Path(self.current_image_path).name
        if hasattr(self, "image_info_label"):
            if display_name:
                self.image_info_label.setText(f"{display_name}\n{filename}")
            elif self.active_observation_name:
                self.image_info_label.setText(f"{self.active_observation_name}\n{filename}")
            else:
                self.image_info_label.setText(f"Loaded: {filename}")

        self.apply_image_scale(image_data)
        self.image_label.set_microns_per_pixel(self.microns_per_pixel)
        self.update_controls_for_image_type(image_data.get("image_type"))
        self._apply_measure_view_settings_for_current_image()
        self._update_scale_mismatch_warning()
        if self.current_objective_name:
            self.image_label.set_objective_color(
                self._objective_color_for_name(self.current_objective_name)
            )
        stored_color = image_data.get('measure_color')
        if stored_color:
            self.set_measure_color(QColor(stored_color))
        else:
            self.set_measure_color(self.measure_color or self.default_measure_color)
        self.refresh_observation_images(select_image_id=self.current_image_id)
        self.measurement_lines = {}
        self.temp_lines = []
        self.points = []
        self.load_measurement_lines()
        self.update_display_lines()
        self.update_statistics()
        if refresh_table:
            self.update_measurements_table()
            self.measurements_table.clearSelection()
            self.spore_preview.clear()
        self._set_measure_category_for_current_image()
        if not self._suppress_gallery_update:
            self.schedule_gallery_refresh()

        self._prefetch_adjacent_images()

    def refresh_observation_images(self, select_image_id=None):
        """Refresh the image list for the active observation."""
        if not self.active_observation_id:
            self.observation_images = []
            self.current_image_index = -1
            self._pixmap_cache.clear()
            self._pixmap_cache_order.clear()
            self._pixmap_cache_observation_id = None
            self.update_image_navigation_ui()
            if hasattr(self, "measure_gallery"):
                self.measure_gallery.clear()
            return

        if self._pixmap_cache_observation_id != self.active_observation_id:
            self._pixmap_cache.clear()
            self._pixmap_cache_order.clear()
            self._pixmap_cache_observation_id = self.active_observation_id

        self.observation_images = ImageDB.get_images_for_observation(self.active_observation_id)
        if hasattr(self, "measure_gallery"):
            self.measure_gallery.set_observation_id(self.active_observation_id)
            self._apply_measure_gallery_publish_selection()
        if select_image_id:
            for idx, image in enumerate(self.observation_images):
                if image['id'] == select_image_id:
                    self.current_image_index = idx
                    break
            else:
                self.current_image_index = -1
        elif self.current_image_id:
            for idx, image in enumerate(self.observation_images):
                if image['id'] == self.current_image_id:
                    self.current_image_index = idx
                    break
            else:
                self.current_image_index = 0 if self.observation_images else -1
        else:
            self.current_image_index = 0 if self.observation_images else -1

        self.update_image_navigation_ui()
        if hasattr(self, "measure_gallery"):
            self.measure_gallery.select_image(self.current_image_id)

    def _publish_excluded_images_setting_key(self, observation_id: int | None) -> str:
        return f"artsobs_publish_excluded_image_ids_{int(observation_id or 0)}"

    def _get_publish_excluded_image_ids_for_observation(self, observation_id: int | None) -> set[int]:
        if not observation_id:
            return set()
        obs_id = int(observation_id)
        key = self._publish_excluded_images_setting_key(obs_id)
        raw = SettingsDB.get_setting(key, "[]")
        parsed: set[int] = set()
        try:
            loaded = json.loads(raw or "[]")
            if isinstance(loaded, list):
                parsed = {int(v) for v in loaded}
        except Exception:
            parsed = set()
        self._publish_excluded_image_ids_by_observation[obs_id] = set(parsed)
        return parsed

    def _set_publish_excluded_image_ids_for_observation(self, observation_id: int | None, excluded_ids: set[int]) -> None:
        if not observation_id:
            return
        obs_id = int(observation_id)
        normalized = {int(v) for v in (excluded_ids or set())}
        key = self._publish_excluded_images_setting_key(obs_id)
        self._publish_excluded_image_ids_by_observation[obs_id] = set(normalized)
        try:
            SettingsDB.set_setting(
                key,
                json.dumps(sorted(normalized)),
            )
        except Exception:
            pass

    def _apply_measure_gallery_publish_selection(self) -> None:
        if not hasattr(self, "measure_gallery"):
            return
        if not self.active_observation_id:
            return
        all_image_ids = {
            int(image.get("id"))
            for image in (self.observation_images or [])
            if image.get("id") is not None
        }
        excluded = self._get_publish_excluded_image_ids_for_observation(self.active_observation_id)
        excluded = {img_id for img_id in excluded if img_id in all_image_ids}
        selected = set(all_image_ids) - set(excluded)
        self.measure_gallery.set_publish_selected_ids(selected, emit_signal=False)

    def _on_measure_gallery_publish_selection_changed(self, selected_ids) -> None:
        if not self.active_observation_id:
            return
        all_image_ids = {
            int(image.get("id"))
            for image in (self.observation_images or [])
            if image.get("id") is not None
        }
        try:
            selected_set = {int(v) for v in (selected_ids or set())}
        except Exception:
            selected_set = set()
        excluded = set(all_image_ids) - set(selected_set)
        self._set_publish_excluded_image_ids_for_observation(self.active_observation_id, excluded)
        if hasattr(self, "_sync_observations_tab_publish_state"):
            self._sync_observations_tab_publish_state(self.active_observation_id, excluded)

    def _sync_observations_tab_publish_state(self, observation_id: int | None, excluded_ids: set[int] | None = None) -> None:
        """Keep Observations tab publish checkboxes in sync with Measure tab edits."""
        if not observation_id or not hasattr(self, "observations_tab"):
            return
        obs_id = int(observation_id)
        tab = self.observations_tab

        if excluded_ids is not None and hasattr(tab, "_set_publish_excluded_image_ids"):
            try:
                tab._set_publish_excluded_image_ids(obs_id, set(excluded_ids))
            except Exception:
                pass

        if not hasattr(tab, "table") or not hasattr(tab, "_find_table_row_for_observation"):
            if hasattr(tab, "refresh_publish_checkbox_state"):
                tab.refresh_publish_checkbox_state(obs_id)
            return

        current_selected = int(getattr(tab, "selected_observation_id", 0) or 0)
        if current_selected == obs_id:
            if hasattr(tab, "refresh_publish_checkbox_state"):
                tab.refresh_publish_checkbox_state(obs_id)
            return

        # When Observations tab is visible, align table selection with active Measure observation.
        if hasattr(self, "tab_widget") and self.tab_widget.currentIndex() == 0:
            try:
                row = int(tab._find_table_row_for_observation(obs_id))
            except Exception:
                row = -1
            if row >= 0:
                try:
                    tab.table.selectRow(row)
                except Exception:
                    pass
            if hasattr(tab, "refresh_publish_checkbox_state"):
                tab.refresh_publish_checkbox_state(obs_id)

    def update_image_navigation_ui(self):
        """Update navigation button state and label."""
        total = len(self.observation_images)
        if total <= 0 or self.current_image_index < 0:
            if hasattr(self, "image_group"):
                self.image_group.setTitle("Image (0/0)")
            if hasattr(self, "prev_image_btn"):
                self.prev_image_btn.setEnabled(False)
            if hasattr(self, "next_image_btn"):
                self.next_image_btn.setEnabled(False)
            return

        label_text = f"({self.current_image_index + 1}/{total})"
        if hasattr(self, "image_group"):
            self.image_group.setTitle(f"Image {label_text}")
        if hasattr(self, "prev_image_btn"):
            self.prev_image_btn.setEnabled(self.current_image_index > 0)
        if hasattr(self, "next_image_btn"):
            self.next_image_btn.setEnabled(self.current_image_index < total - 1)

    def _cache_pixmap(self, path: str, pixmap: QPixmap) -> None:
        if not path or pixmap is None or pixmap.isNull():
            return
        if path in self._pixmap_cache_order:
            self._pixmap_cache_order.remove(path)
        self._pixmap_cache[path] = pixmap
        self._pixmap_cache_order.append(path)
        while len(self._pixmap_cache_order) > self._pixmap_cache_max:
            oldest = self._pixmap_cache_order.pop(0)
            self._pixmap_cache.pop(oldest, None)

    def _load_pixmap_cached(self, path: str) -> QPixmap:
        if path in self._pixmap_cache:
            return self._pixmap_cache[path]
        pixmap = QPixmap(path)
        self._cache_pixmap(path, pixmap)
        return pixmap

    def _prefetch_adjacent_images(self) -> None:
        if not self.observation_images:
            return
        if self.current_image_index < 0:
            idx = -1
            if self.current_image_id:
                for i, image in enumerate(self.observation_images):
                    if image.get("id") == self.current_image_id:
                        idx = i
                        break
            if idx < 0:
                return
        else:
            idx = self.current_image_index

        targets = [idx - 1, idx + 1]
        for target in targets:
            if target < 0 or target >= len(self.observation_images):
                continue
            path = self.observation_images[target].get("filepath")
            if not path:
                continue
            if path in self._pixmap_cache:
                continue
            QTimer.singleShot(0, lambda p=path: self._load_pixmap_cached(p))

    def goto_previous_image(self):
        """Navigate to the previous image."""
        if self.current_image_index <= 0:
            return
        self.goto_image_index(self.current_image_index - 1)

    def goto_next_image(self):
        """Navigate to the next image."""
        if self.current_image_index < 0 or self.current_image_index >= len(self.observation_images) - 1:
            return
        self.goto_image_index(self.current_image_index + 1)

    def _on_measure_gallery_clicked(self, image_id, _filepath):
        if not image_id:
            return
        for idx, image in enumerate(self.observation_images):
            if image.get("id") == image_id:
                self.current_image_index = idx
                self.goto_image_index(idx)
                self.update_image_navigation_ui()
                if hasattr(self, "measure_gallery"):
                    self.measure_gallery.select_image(image_id)
                return

    def _on_measure_gallery_delete_requested(self, image_key):
        image_id = None
        if isinstance(image_key, int):
            image_id = image_key
        elif image_key:
            for image in self.observation_images:
                if image.get("filepath") == image_key:
                    image_id = image.get("id")
                    break
        if not image_id:
            return

        measurements = MeasurementDB.get_measurements_for_image(image_id)
        if measurements:
            confirmed = ask_measurements_exist_delete(self, count=1)
        else:
            confirmed = self._question_yes_no(self.tr("Confirm Delete"), self.tr("Delete image?"), default_yes=False)
        if not confirmed:
            return

        current_id = self.current_image_id
        ids = [img.get("id") for img in self.observation_images if img.get("id") is not None]
        next_id = None
        if current_id and image_id != current_id and current_id in ids:
            next_id = current_id
        elif image_id in ids and len(ids) > 1:
            idx = ids.index(image_id)
            next_idx = idx + 1 if idx + 1 < len(ids) else idx - 1
            next_id = ids[next_idx] if next_idx >= 0 else None

        try:
            ImageDB.delete_image(image_id)
        except Exception as exc:
            QMessageBox.critical(
                self,
                self.tr("Delete failed"),
                self.tr("Could not delete image: {error}").format(error=exc),
            )
            return

        if next_id:
            next_image = ImageDB.get_image(next_id)
            if next_image:
                self.load_image_record(next_image, display_name=self.active_observation_name, refresh_table=True)
                return

        self.clear_current_image_display()
        self.refresh_observation_images()
        self.update_measurements_table()
        self.update_statistics()
        if not self._suppress_gallery_update:
            self.schedule_gallery_refresh()

    def goto_image_index(self, index):
        """Load an image by index from the active observation."""
        if index < 0 or index >= len(self.observation_images):
            return
        image_data = self.observation_images[index]
        self.load_image_record(image_data, display_name=self.active_observation_name, refresh_table=True)

    def get_objective_name_for_storage(self):
        """Return the objective name to store with an image."""
        if self.current_objective_name:
            return self.current_objective_name
        if self.current_objective:
            key = self.current_objective.get("key") or self.current_objective.get("objective_key")
            if key:
                return key
            if self.current_objective.get("magnification"):
                return self.current_objective["magnification"]
        if self.microns_per_pixel:
            return "Custom"
        return None

    def update_observation_header(self, observation_id):
        """Update the observation header label."""
        if not observation_id:
            self.observation_header_label.setText("")
            return

        observation = ObservationDB.get_observation(observation_id)
        if not observation:
            self.observation_header_label.setText("")
            return

        genus = observation.get('genus') or ''
        species = observation.get('species') or observation.get('species_guess') or 'sp.'
        uncertain = observation.get('uncertain', 0)
        display_name = f"{genus} {species}".strip() or "Unknown"
        if uncertain:
            display_name = f"? {display_name}"
        date = observation.get('date') or "Unknown date"
        self.observation_header_label.setText(f"{display_name} - {date}")

    def _on_multi_observation_selected(self, count: int):
        """Update the observation header label when multiple rows are selected."""
        self.observation_header_label.setText(self.tr("{n} observations selected").format(n=count))

    def clear_current_image_display(self):
        """Clear the current image and overlays."""
        self._flush_measure_image_note()
        self._save_current_image_measure_session_view()
        self._save_current_image_measure_view_settings()
        self.current_image_id = None
        self.current_image_path = None
        self.current_pixmap = None
        self.current_image_type = None
        self.auto_gray_cache = None
        self.auto_gray_cache_id = None
        self.points = []
        self.measurement_lines = {}
        self.multiline_measurements = {}
        self.temp_lines = []
        self.image_label.set_image(None)
        self.image_label.set_objective_text("")
        self.update_exif_panel(None)
        self.image_label.clear_preview_line()
        self.image_label.clear_preview_rectangle()
        self._update_measure_copyright_overlay()
        self._set_measure_image_note_text(None, enabled=False)
        self.spore_preview.clear()
        self.update_display_lines()
        self._update_scale_mismatch_warning()

    def load_image(self):
        """Load a microscope image."""
        paths, _ = QFileDialog.getOpenFileNames(
            self, "Open Microscope Image", self._get_default_import_dir(),
            "Images (*.png *.jpg *.jpeg *.tif *.tiff *.heic *.heif);;All Files (*)"
        )

        if not paths:
            return
        self._remember_import_dir(paths[0])

        output_dir = get_images_dir() / "imports"
        output_dir.mkdir(parents=True, exist_ok=True)
        last_image_data = None
        for path in paths:
            converted_path = maybe_convert_heic(path, output_dir)
            if converted_path is None:
                QMessageBox.warning(
                    self,
                    "HEIC Conversion Failed",
                    f"Could not convert {Path(path).name} to JPEG."
                )
                continue

            objective_name = self.get_objective_name_for_storage()
            calibration_id = CalibrationDB.get_active_calibration_id(objective_name) if objective_name else None
            contrast_fallback = SettingsDB.get_list_setting(
                "contrast_options",
                DatabaseTerms.CONTRAST_METHODS,
            )
            contrast_value = SettingsDB.get_setting(DatabaseTerms.last_used_key("contrast"), None)
            if not contrast_value:
                contrast_value = SettingsDB.get_setting(
                    "contrast_default",
                    contrast_fallback[0] if contrast_fallback else DatabaseTerms.CONTRAST_METHODS[0],
                )
            contrast_value = DatabaseTerms.canonicalize("contrast", contrast_value)
            if not contrast_value:
                contrast_value = contrast_fallback[0] if contrast_fallback else DatabaseTerms.CONTRAST_METHODS[0]
            image_id = ImageDB.add_image(
                observation_id=self.active_observation_id,
                filepath=converted_path,
                image_type='microscope',
                scale=self.microns_per_pixel,
                objective_name=objective_name,
                contrast=contrast_value,
                calibration_id=calibration_id,
                resample_scale_factor=1.0,
            )

            image_data = ImageDB.get_image(image_id)
            stored_path = image_data.get("filepath") if image_data else converted_path

            # Generate thumbnails for ML training
            try:
                generate_all_sizes(stored_path, image_id)
            except Exception as e:
                print(f"Warning: Could not generate thumbnails: {e}")

            last_image_data = ImageDB.get_image(image_id)
            cleanup_import_temp_file(path, converted_path, stored_path, output_dir)

        if last_image_data:
            self.load_image_record(last_image_data, refresh_table=True)
            self.refresh_observation_images(select_image_id=last_image_data['id'])

    def zoom_in(self):
        """Zoom in the image."""
        self.image_label.zoom_in()

    def zoom_out(self):
        """Zoom out the image."""
        self.image_label.zoom_out()

    def reset_view(self):
        """Reset zoom and pan."""
        self.image_label.reset_view()

    def set_measure_color(self, color):
        """Set the active measurement color and update palette."""
        self.measure_color = QColor(color)
        if hasattr(self, "image_label"):
            self.image_label.set_measurement_color(self.measure_color)
        if hasattr(self, "spore_preview"):
            self.spore_preview.set_measure_color(self.measure_color)
        if self.current_image_id:
            ImageDB.update_image(
                self.current_image_id,
                measure_color=self.measure_color.name()
            )
        self._refresh_measure_color_button()

    def on_show_measures_toggled(self, checked):
        """Toggle measurement labels on the main image."""
        SettingsDB.set_setting(self.SETTING_MEASURE_SHOW_LABELS, "1" if checked else "0")
        if hasattr(self, "image_label"):
            self.image_label.set_show_measure_labels(checked)
        self._save_current_image_measure_view_settings()

    def on_show_rectangles_toggled(self, checked):
        """Toggle measurement overlays on the main image."""
        SettingsDB.set_setting(self.SETTING_MEASURE_SHOW_OVERLAYS, "1" if checked else "0")
        if hasattr(self, "image_label"):
            self.image_label.set_show_measure_overlays(checked)
        self.update_display_lines()
        self._update_measure_view_option_states()
        self._save_current_image_measure_view_settings()
        return

    def on_measure_rectangle_style_changed(self, checked):
        if not checked:
            return
        style = self._current_measure_rectangle_style()
        SettingsDB.set_setting(self.SETTING_MEASURE_RECTANGLE_STYLE, style)
        self._apply_measure_rectangle_appearance(refresh_gallery=True)
        self._save_current_image_measure_view_settings()

    def on_measure_rectangle_thickness_changed(self, checked):
        thickness = self._current_measure_rectangle_thickness()
        SettingsDB.set_setting(self.SETTING_MEASURE_RECTANGLE_THICKNESS, str(int(round(thickness))))
        self._apply_measure_rectangle_appearance(refresh_gallery=True)
        self._save_current_image_measure_view_settings()

    def on_show_scale_bar_toggled(self, checked):
        """Toggle scale bar display."""
        if checked and not self._current_image_has_scale():
            checked = False
        if hasattr(self, "scale_bar_container"):
            self.scale_bar_container.setVisible(bool(checked) and self._current_image_has_scale())
        scale_value = float(self.scale_bar_input.value()) if hasattr(self, "scale_bar_input") else 10.0
        is_field = (self.current_image_type or "").strip().lower() == "field"
        scale_um = scale_value * 1000.0 if is_field else scale_value
        unit = "mm" if is_field else "\u03bcm"
        self.image_label.set_scale_bar(checked, scale_um, unit=unit)
        self._save_current_image_measure_view_settings()

    def on_scale_bar_value_changed(self, value):
        """Update scale bar size."""
        if not self._current_image_has_scale():
            if hasattr(self, "image_label"):
                unit = "mm" if (self.current_image_type or "").strip().lower() == "field" else "\u03bcm"
                self.image_label.set_scale_bar(False, 0.0, unit=unit)
            return
        is_field = (self.current_image_type or "").strip().lower() == "field"
        default_value = self._observation_scale_bar_fallback_value(
            is_field,
            objective_key=self.current_objective_name,
        )
        if is_field:
            value = max(0.1, float(value))
        else:
            snapped = float(self._snap_scale_bar_length_um(float(value)))
            if hasattr(self, "scale_bar_input") and abs(float(value) - snapped) > 1e-9:
                self.scale_bar_input.blockSignals(True)
                self.scale_bar_input.setValue(snapped)
                self.scale_bar_input.blockSignals(False)
            value = snapped
        self._save_observation_scale_bar_fallback_value(float(value), is_field)
        self._current_measure_scale_bar_value_custom = abs(float(value) - float(default_value)) > 1e-9
        if hasattr(self, "show_scale_bar_checkbox") and self.show_scale_bar_checkbox.isChecked():
            scale_um = float(value) * 1000.0 if is_field else float(value)
            unit = "mm" if is_field else "\u03bcm"
            self.image_label.set_scale_bar(True, scale_um, unit=unit)
        self._save_current_image_measure_view_settings()

    def on_show_copyright_toggled(self, _checked):
        """Toggle copyright text on the Measure image view/export."""
        SettingsDB.set_setting(
            self.SETTING_MEASURE_SHOW_COPYRIGHT,
            "1" if bool(self.show_copyright_checkbox.isChecked()) else "0",
        )
        self._update_measure_copyright_overlay()
        self._save_current_image_measure_view_settings()

    def _observation_year_for_copyright(self, observation_date) -> int:
        """Return a year extracted from observation date input."""
        value = observation_date
        if hasattr(value, "year") and callable(getattr(value, "year", None)):
            try:
                return int(value.year())
            except Exception:
                pass
        if hasattr(value, "date") and callable(getattr(value, "date", None)):
            try:
                return int(value.date().year())
            except Exception:
                pass
        text = str(observation_date or "").strip()
        match = re.match(r"^(\d{4})", text)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                pass
        return int(time.localtime().tm_year)

    def _measure_publish_license_watermark_text(self) -> str:
        code = str(
            SettingsDB.get_setting(ArtsobservasjonerSettingsDialog.SETTING_IMAGE_LICENSE, "60") or "60"
        ).strip()
        if code == "10":
            return "CC BY 4.0"
        if code == "20":
            return "CC BY-SA 4.0"
        if code == "30":
            return "CC BY-NC-SA 4.0"
        return "No reuse without permission"

    def _measure_copyright_text(self) -> str | None:
        """Build watermark line from profile name + selected online-publishing license."""
        if not self.active_observation_id:
            return None
        observation = ObservationDB.get_observation(self.active_observation_id)
        if not observation:
            return None
        profile = SettingsDB.get_profile() if hasattr(SettingsDB, "get_profile") else {}
        name = str((profile or {}).get("name") or "").strip()
        if not name:
            name = str(observation.get("author") or "").strip()
        if not name:
            return None
        return f"{name} \u2022 {self._measure_publish_license_watermark_text()}"

    def _update_measure_copyright_overlay(self) -> None:
        if not hasattr(self, "image_label"):
            return
        show = bool(
            hasattr(self, "show_copyright_checkbox")
            and self.show_copyright_checkbox.isChecked()
        )
        text = self._measure_copyright_text() if show else None
        self.image_label.set_copyright(show and bool(text), text or "")

    def start_measurement(self):
        """Enable measurement mode."""
        if hasattr(self, "show_rectangles_checkbox") and not self.show_rectangles_checkbox.isChecked():
            self.show_rectangles_checkbox.setChecked(True)
        self.measurement_active = True
        self.update_measurement_button_state()
        if hasattr(self, "measurements_table"):
            self.measurements_table.clearSelection()
        if hasattr(self, "spore_preview"):
            self.spore_preview.clear()
        self._clear_measurement_highlight()
        if hasattr(self, "image_label"):
            self.image_label.set_pan_without_shift(True)
            self.image_label.set_measurement_active(True)
        self.on_measure_mode_changed()

    def stop_measurement(self):
        """Disable measurement mode and clear any in-progress points."""
        if not self.measurement_active:
            return
        self.measurement_active = False
        self.abort_measurement(show_status=False)
        self.update_measurement_button_state()
        if hasattr(self, "image_label"):
            self.image_label.set_pan_without_shift(True)
            self.image_label.set_measurement_active(False)
        self.measure_status_label.setText(self.tr("Stopped - Start measuring"))
        self.measure_status_label.setStyleSheet(f"color: #7f8c8d; font-weight: bold; font-size: {pt(9)}pt;")

    def update_measurement_button_state(self):
        """Update Start/Stop button state based on measurement mode."""
        if hasattr(self, "measure_button"):
            self.measure_button.blockSignals(True)
            self.measure_button.setChecked(self.measurement_active)
            if self.measurement_active:
                self.measure_button.setText(self.tr("Stop measuring (M)"))
                self.measure_button.setStyleSheet(
                    "font-weight: bold; padding: 6px 10px; background-color: #e74c3c; color: white;"
                )
            else:
                self.measure_button.setText(self.tr("Start measuring (M)"))
                self.measure_button.setStyleSheet("font-weight: bold; padding: 6px 10px;")
            self.measure_button.blockSignals(False)

    def _on_measure_button_clicked(self):
        """Handle measure mode button click."""
        if self.measurement_active:
            self.stop_measurement()
        else:
            if not self._check_scale_before_measure():
                return
            self.start_measurement()

    def _check_scale_before_measure(self):
        """Ensure scale exists before measuring."""
        if not self.current_image_id:
            return True
        image_data = ImageDB.get_image(self.current_image_id)
        if not image_data:
            return True
        image_type = (image_data.get("image_type") or "").strip().lower()
        if image_type == "field":
            self._ensure_field_measure_category()
        scale = image_data.get("scale_microns_per_pixel")
        if scale is not None and scale > 0:
            return True

        if image_type not in ("field", "microscope"):
            return True
        if image_type == "field":
            self.measure_status_label.setText(self.tr("Set scale first using 'Set from scalebar'."))
            self.measure_status_label.setStyleSheet(f"color: #e67e22; font-weight: bold; font-size: {pt(9)}pt;")
            return False

        self.measure_status_label.setText(self.tr("Set scale first (objective or 'Set from scalebar')."))
        self.measure_status_label.setStyleSheet(f"color: #e67e22; font-weight: bold; font-size: {pt(9)}pt;")
        return False

    def on_measure_category_changed(self):
        """Update category for selected measurement."""
        self._update_preview_title()
        if self._measure_category_sync:
            return
        selected_rows = self.measurements_table.selectedIndexes()
        if not selected_rows:
            self.update_display_lines()
            return
        row = selected_rows[0].row()
        if row >= len(self.measurements_cache):
            return
        measurement = self.measurements_cache[row]
        measurement_id = measurement.get("id")
        new_type = self.measure_category_combo.currentData()
        if not measurement_id or not new_type:
            return
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            'UPDATE spore_measurements SET measurement_type = ? WHERE id = ?',
            (new_type, measurement_id)
        )
        conn.commit()
        conn.close()
        self.measurements_cache[row]["measurement_type"] = new_type
        self.update_measurements_table()
        if measurement_id:
            self.select_measurement_in_table(measurement_id)
        self.update_display_lines()

    def abort_measurement(self, show_status=True):
        """Abort the current measurement."""
        self.points = []
        self.temp_lines = []
        self.image_label.clear_preview_line()
        self.image_label.clear_preview_rectangle()
        self.rect_stage = 0
        self.rect_line1_start = None
        self.rect_line1_end = None
        self.rect_line2_start = None
        self.rect_line2_end = None
        self.rect_width_dir = None
        self.update_display_lines()
        if show_status:
            self.measure_status_label.setText(self.tr("Aborted - Start measuring"))
            self.measure_status_label.setStyleSheet(f"color: #e67e22; font-weight: bold; font-size: {pt(9)}pt;")

    def _measurement_in_progress(self) -> bool:
        """Return whether a line/rectangle measurement is mid-click."""
        return bool(self.points or self.rect_stage > 0 or self.temp_lines)

    def _reset_measure_prompt(self) -> None:
        """Restore the first-click prompt for the active measure mode."""
        if not self.measurement_active:
            self.measure_status_label.setText(self.tr("Start measuring to begin"))
            self.measure_status_label.setStyleSheet(f"color: #7f8c8d; font-weight: bold; font-size: {pt(9)}pt;")
            return
        if self.measure_mode == "rectangle":
            self.measure_status_label.setText(self.tr("Rectangle: Click point 1"))
            self.measure_status_label.setStyleSheet(f"color: #3498db; font-weight: bold; font-size: {pt(9)}pt;")
        elif self.measure_mode == "multiline":
            self.measure_status_label.setText(self.tr("Multi-line: Click start point"))
            self.measure_status_label.setStyleSheet(f"color: #9b59b6; font-weight: bold; font-size: {pt(9)}pt;")
        else:
            self.measure_status_label.setText(self.tr("Line: Click start point"))
            self.measure_status_label.setStyleSheet(f"color: #27ae60; font-weight: bold; font-size: {pt(9)}pt;")

    def _cancel_measurement_shortcut(self) -> None:
        """Cancel the current in-progress measurement without leaving measure mode."""
        if not self.measurement_active:
            self._clear_measurement_selection()
            return
        if not self._measurement_in_progress():
            return
        self.abort_measurement(show_status=False)
        self._reset_measure_prompt()

    def on_measure_mode_changed(self):
        """Switch between line, rectangle, and multi-line measurement modes."""
        if self.mode_lines.isChecked():
            self.measure_mode = "lines"
        elif hasattr(self, "mode_multiline") and self.mode_multiline.isChecked():
            self.measure_mode = "multiline"
        else:
            self.measure_mode = "rectangle"
        self.abort_measurement(show_status=False)
        self._reset_measure_prompt()

    def image_clicked(self, pos):
        """Handle image clicks for measurement or calibration."""
        # Handle calibration mode first
        if self.calibration_mode:
            self.handle_calibration_click(pos)
            return

        # If idle, allow clicking existing measurement overlays to select
        if not self.measurement_active and self.rect_stage == 0 and len(self.points) == 0:
            measurement_id = self.find_measurement_at_point(pos)
            if measurement_id:
                self.select_measurement_in_table(measurement_id)
                return
        if not self.measurement_active:
            return

        if self.measure_mode == "rectangle":
            self.handle_rectangle_measurement(pos)
            return

        if self.measure_mode == "multiline":
            self.points.append(pos)
            if len(self.points) == 1:
                self.image_label.set_preview_line(pos)
                self.measure_status_label.setText(self.tr("Multi-line: Click next point, Right-click to finish"))
                self.measure_status_label.setStyleSheet(f"color: #9b59b6; font-weight: bold; font-size: {pt(9)}pt;")
            else:
                line = [self.points[-2].x(), self.points[-2].y(), pos.x(), pos.y()]
                self.temp_lines.append(line)
                self.update_display_lines()
                self.image_label.set_preview_line(pos)
            return

        # Auto-start measurement if we have an image but no active measurement
        if len(self.points) == 0 and self.current_image_path:
            # Automatically start a new measurement
            self.points = []
            self.temp_lines = []
            self.measure_status_label.setText(self.tr("Line: Click start point"))
            self.measure_status_label.setStyleSheet(f"color: #3498db; font-weight: bold; font-size: {pt(9)}pt;")

        # Add point
        self.points.append(pos)

        # Update status and preview line
        if len(self.points) == 1:
            # Start preview line from point 1
            self.image_label.set_preview_line(self.points[0])
            self.measure_status_label.setText(self.tr("Line: Click end point"))
        elif len(self.points) == 2:
            # Complete first line, clear preview
            self.image_label.clear_preview_line()
            line1 = [
                self.points[0].x(), self.points[0].y(),
                self.points[1].x(), self.points[1].y()
            ]
            self.temp_lines.append(line1)
            self.update_display_lines()
            self.complete_measurement()

    def image_right_clicked(self, pos):
        """Handle right clicks on the image (e.g., to finish multi-line)."""
        if not self.measurement_active or self.measure_mode != "multiline":
            return
        if len(self.points) >= 2:
            self.complete_multiline_measurement()
        else:
            self.abort_measurement()

    def complete_multiline_measurement(self):
        """Complete a multi-line measurement and store it."""
        if len(self.points) < 2:
            self.abort_measurement()
            return
            
        total_length_pixels = 0
        for i in range(len(self.points) - 1):
            dx = self.points[i+1].x() - self.points[i].x()
            dy = self.points[i+1].y() - self.points[i].y()
            total_length_pixels += math.sqrt(dx**2 + dy**2)
            
        length_microns = total_length_pixels * self.microns_per_pixel
        width_microns = None
        
        measurement_category = self.measure_category_combo.currentData()
        notes_json = json.dumps({"multiline": [[p.x(), p.y()] for p in self.points]})
        measurement_id = MeasurementDB.add_measurement(
            self.current_image_id,
            length=length_microns,
            width=width_microns,
            measurement_type=measurement_category,
            notes=notes_json,
            points=self.points[:2]  # Fallback for DB schema
        )
        
        ImageDB.update_image(
            self.current_image_id,
            scale=self.microns_per_pixel,
            objective_name=self.current_objective_name
        )
        
        saved_lines = self.temp_lines.copy()
        self.multiline_measurements[measurement_id] = saved_lines
        
        self.measurement_labels.append(
            self._build_multiline_measurement_label(
                measurement_id,
                length_microns,
                QPointF(self.points[-1].x(), self.points[-1].y()),
                measurement_category,
            )
        )
        
        self.temp_lines = []
        self.update_display_lines()
        self.points = []
        self.image_label.clear_preview_line()
        
        self.measure_status_label.setText(self.tr("Click to measure next"))
        self.measure_status_label.setStyleSheet(f"color: #27ae60; font-weight: bold; font-size: {pt(9)}pt;")
        
        self.update_measurements_table()
        self.update_statistics()

    def complete_measurement(self):
        """Complete a measurement and store it."""
        if self.measure_mode == "lines" and len(self.points) >= 2:
            dx = self.points[1].x() - self.points[0].x()
            dy = self.points[1].y() - self.points[0].y()
            dist_pixels = math.sqrt(dx**2 + dy**2)
            length_microns = dist_pixels * self.microns_per_pixel
            width_microns = None
            q_value = None
        else:
            # Calculate first distance
            dx1 = self.points[1].x() - self.points[0].x()
            dy1 = self.points[1].y() - self.points[0].y()
            dist1_pixels = np.sqrt(dx1**2 + dy1**2)
            dist1_microns = dist1_pixels * self.microns_per_pixel

            # Calculate second distance
            dx2 = self.points[3].x() - self.points[2].x()
            dy2 = self.points[3].y() - self.points[2].y()
            dist2_pixels = np.sqrt(dx2**2 + dy2**2)
            dist2_microns = dist2_pixels * self.microns_per_pixel

            # Auto-detect length (longer) and width (shorter)
            if dist1_microns >= dist2_microns:
                length_microns = dist1_microns
                width_microns = dist2_microns
            else:
                length_microns = dist2_microns
                width_microns = dist1_microns

            # Calculate Q (length/width ratio)
            q_value = length_microns / width_microns if width_microns > 0 else 0

        # Save to database with point coordinates and get measurement ID
        measurement_category = self.measure_category_combo.currentData()
        measurement_id = MeasurementDB.add_measurement(
            self.current_image_id,
            length=length_microns,
            width=width_microns,
            measurement_type=measurement_category,
            notes=f"Q={q_value:.1f}" if q_value is not None else None,
            points=self.points[:2] if self.measure_mode == "lines" else self.points
        )

        ImageDB.update_image(
            self.current_image_id,
            scale=self.microns_per_pixel,
            objective_name=self.current_objective_name
        )

        # Save ML annotation with bounding box
        if (
            self.current_pixmap
            and self.normalize_measurement_category(measurement_category) == "spores"
            and width_microns is not None
        ):
            image_shape = (self.current_pixmap.height(), self.current_pixmap.width())
            try:
                save_spore_annotation(
                    image_id=self.current_image_id,
                    measurement_id=measurement_id,
                    points=self.points,
                    length_um=length_microns,
                    width_um=width_microns,
                    image_shape=image_shape
                )
            except Exception as e:
                print(f"Warning: Could not save ML annotation: {e}")

        # Store the lines associated with this measurement
        saved_lines = self.temp_lines.copy()
        self.measurement_lines[measurement_id] = saved_lines
        if len(saved_lines) >= 2 and width_microns is not None:
            self.measurement_labels.append(
                self._build_measurement_label(
                    measurement_id,
                    saved_lines[0],
                    saved_lines[1],
                    length_microns,
                    width_microns,
                    measurement_category,
                )
            )
        elif len(saved_lines) == 1 and length_microns is not None:
            self.measurement_labels.append(
                self._build_line_measurement_label(
                    measurement_id,
                    saved_lines[0],
                    length_microns,
                    measurement_category,
                )
            )
        self.temp_lines = []
        self.update_display_lines()

        # Update display
        self.measure_status_label.setText(self.tr("Click to measure next"))
        self.measure_status_label.setStyleSheet(f"color: #27ae60; font-weight: bold; font-size: {pt(9)}pt;")

        # Update table and statistics
        self.update_measurements_table()
        self.update_statistics()

        # Auto-show preview for the just-completed measurement
        measurements = MeasurementDB.get_measurements_for_image(self.current_image_id)
        if measurements:
            # Get the last measurement (the one we just added)
            last_measurement = measurements[-1]
            self.show_measurement_preview(last_measurement)

        # Reset for next measurement - ready for next click
        self.points = []

    def handle_rectangle_measurement(self, pos):
        """Handle rectangle-based interactive measurement."""
        if not self.current_image_path:
            return

        if self.rect_stage == 0:
            self.rect_line1_start = pos
            self.image_label.set_preview_line(pos)
            self.rect_stage = 1
            self.measure_status_label.setText(self.tr("Rectangle: Click point 2"))
            self.measure_status_label.setStyleSheet(f"color: #3498db; font-weight: bold; font-size: {pt(9)}pt;")
            return

        if self.rect_stage == 1:
            self.rect_line1_end = pos
            self.image_label.clear_preview_line()

            dx = self.rect_line1_end.x() - self.rect_line1_start.x()
            dy = self.rect_line1_end.y() - self.rect_line1_start.y()
            length = math.sqrt(dx**2 + dy**2)
            if length < 0.001:
                return

            self.rect_width_dir = QPointF(-dy / length, dx / length)
            self.rect_length_dir = QPointF(dx / length, dy / length)
            self.image_label.set_preview_rectangle(
                self.rect_line1_start,
                self.rect_line1_end,
                self.rect_width_dir,
                "line2"
            )
            self.rect_stage = 2
            self.measure_status_label.setText(self.tr("Rectangle: Set width, click point 3"))
            self.measure_status_label.setStyleSheet(f"color: #3498db; font-weight: bold; font-size: {pt(9)}pt;")
            return

        if self.rect_stage == 2:
            if not self.rect_width_dir:
                return
            line1_mid = QPointF(
                (self.rect_line1_start.x() + self.rect_line1_end.x()) / 2,
                (self.rect_line1_start.y() + self.rect_line1_end.y()) / 2
            )
            delta = pos - line1_mid
            width_distance = delta.x() * self.rect_width_dir.x() + delta.y() * self.rect_width_dir.y()
            self.rect_line2_start = self.rect_line1_start + self.rect_width_dir * width_distance
            self.rect_line2_end = self.rect_line1_end + self.rect_width_dir * width_distance

            self.image_label.set_preview_rectangle(
                self.rect_line2_start,
                self.rect_line2_end,
                self.rect_width_dir,
                "line1",
                reference_start=self.rect_line1_start,
                reference_end=self.rect_line1_end,
            )
            self.rect_stage = 3
            self.measure_status_label.setText(self.tr("Rectangle: Adjust start line, click point 4"))
            self.measure_status_label.setStyleSheet(f"color: #3498db; font-weight: bold; font-size: {pt(9)}pt;")
            return

        if self.rect_stage == 3:
            if not self.rect_width_dir or not self.rect_length_dir:
                return
            line1_ref_mid = QPointF(
                (self.rect_line1_start.x() + self.rect_line1_end.x()) / 2,
                (self.rect_line1_start.y() + self.rect_line1_end.y()) / 2
            )
            current_distance = (
                (pos.x() - line1_ref_mid.x()) * self.rect_width_dir.x()
                + (pos.y() - line1_ref_mid.y()) * self.rect_width_dir.y()
            )
            line1_start = self.rect_line1_start + self.rect_width_dir * current_distance
            line1_end = self.rect_line1_end + self.rect_width_dir * current_distance

            self.image_label.clear_preview_rectangle()

            line1_mid = QPointF((line1_start.x() + line1_end.x()) / 2,
                                (line1_start.y() + line1_end.y()) / 2)
            line2_mid = QPointF((self.rect_line2_start.x() + self.rect_line2_end.x()) / 2,
                                (self.rect_line2_start.y() + self.rect_line2_end.y()) / 2)
            center = QPointF((line1_mid.x() + line2_mid.x()) / 2,
                             (line1_mid.y() + line2_mid.y()) / 2)

            length_len = math.sqrt((line1_end.x() - line1_start.x())**2 +
                                   (line1_end.y() - line1_start.y())**2)
            half_length = length_len / 2
            center_line_start = center - self.rect_length_dir * half_length
            center_line_end = center + self.rect_length_dir * half_length

            width_vec = line1_mid - line2_mid
            width = abs(width_vec.x() * self.rect_width_dir.x() + width_vec.y() * self.rect_width_dir.y())
            width_half = width / 2
            width_line_start = center - self.rect_width_dir * width_half
            width_line_end = center + self.rect_width_dir * width_half

            self.points = [center_line_start, center_line_end, width_line_start, width_line_end]
            self.temp_lines = [
                [center_line_start.x(), center_line_start.y(), center_line_end.x(), center_line_end.y()],
                [width_line_start.x(), width_line_start.y(), width_line_end.x(), width_line_end.y()]
            ]
            self.update_display_lines()
            self.complete_measurement()

            self.rect_stage = 0
            self.rect_line1_start = None
            self.rect_line1_end = None
            self.rect_line2_start = None
            self.rect_line2_end = None
            self.rect_width_dir = None
            self.rect_length_dir = None

    def update_display_lines(self):
        """Update the display with all lines (saved + temporary)."""
        show_saved = True
        if hasattr(self, "show_rectangles_checkbox"):
            show_saved = self.show_rectangles_checkbox.isChecked()
        show_calibration = self._should_show_calibration_overlay()
        rectangles, self._rect_index_map = self._build_measurement_rectangles_with_ids() if show_saved else ([], {})
        all_lines = []
        self._line_index_map = {}
        if show_saved:
            for measurement_id, lines_list in self.measurement_lines.items():
                measurement_type = self.normalize_measurement_category(
                    self._measurement_type_by_id.get(measurement_id, "")
                )
                if measurement_type == "calibration" and not show_calibration:
                    continue
                if len(lines_list) != 1:
                    continue
                line = lines_list[0]
                idx = len(all_lines)
                all_lines.append(line)
                self._line_index_map.setdefault(measurement_id, []).append(idx)
            
            for measurement_id, lines_list in getattr(self, "multiline_measurements", {}).items():
                measurement_type = self.normalize_measurement_category(
                    self._measurement_type_by_id.get(measurement_id, "")
                )
                if measurement_type == "calibration" and not show_calibration:
                    continue
                if lines_list:
                    idx = len(all_lines)
                    all_lines.append(lines_list)
                    self._line_index_map.setdefault(measurement_id, []).append(idx)
                    
        if self.measure_mode == "multiline" and self.temp_lines:
            all_lines.append(self.temp_lines)
        else:
            all_lines.extend(self.temp_lines)
        visible_labels = self.measurement_labels
        if show_saved and not show_calibration:
            visible_labels = [
                label
                for label in self.measurement_labels
                if self.normalize_measurement_category(
                    self._measurement_type_by_id.get(label.get("id"), "")
                ) != "calibration"
            ]
        self.image_label.set_measurement_rectangles(rectangles)
        self.image_label.set_measurement_lines(all_lines)
        self.image_label.set_measurement_labels(visible_labels)

    def build_measurement_rectangles(self):
        """Build rectangle corner lists from saved measurement lines."""
        rectangles, _ = self._build_measurement_rectangles_with_ids()
        return rectangles

    def _build_measurement_rectangles_with_ids(self):
        """Build rectangle corner lists and an index map keyed by measurement id."""
        rectangles = []
        rect_index_map = {}
        show_calibration = self._should_show_calibration_overlay()
        for measurement_id, lines_list in self.measurement_lines.items():
            measurement_type = self.normalize_measurement_category(
                self._measurement_type_by_id.get(measurement_id, "")
            )
            if measurement_type == "calibration" and not show_calibration:
                continue
            if len(lines_list) < 2:
                continue
            line1 = lines_list[0]
            line2 = lines_list[1]
            p1 = QPointF(line1[0], line1[1])
            p2 = QPointF(line1[2], line1[3])
            p3 = QPointF(line2[0], line2[1])
            p4 = QPointF(line2[2], line2[3])

            length_vec = p2 - p1
            length_len = math.sqrt(length_vec.x()**2 + length_vec.y()**2)
            width_vec = p4 - p3
            width_len = math.sqrt(width_vec.x()**2 + width_vec.y()**2)
            if length_len < 0.001 or width_len < 0.001:
                continue

            length_dir = QPointF(length_vec.x() / length_len, length_vec.y() / length_len)
            width_dir = QPointF(-length_dir.y(), length_dir.x())

            line1_mid = QPointF((p1.x() + p2.x()) / 2, (p1.y() + p2.y()) / 2)
            line2_mid = QPointF((p3.x() + p4.x()) / 2, (p3.y() + p4.y()) / 2)
            center = QPointF((line1_mid.x() + line2_mid.x()) / 2,
                             (line1_mid.y() + line2_mid.y()) / 2)

            half_length = length_len / 2
            half_width = width_len / 2

            corners = [
                center - width_dir * half_width - length_dir * half_length,
                center + width_dir * half_width - length_dir * half_length,
                center + width_dir * half_width + length_dir * half_length,
                center - width_dir * half_width + length_dir * half_length,
            ]
            rect_index_map[measurement_id] = len(rectangles)
            rectangles.append(corners)
        return rectangles, rect_index_map

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

    def find_measurement_at_point(self, pos, threshold=6.0):
        """Return measurement_id if click is near a measurement overlay."""
        if not self.measurement_lines:
            return None

        best_id = None
        best_dist = threshold
        for measurement_id, lines_list in self.measurement_lines.items():
            if not lines_list:
                continue
            line1 = lines_list[0]
            p1 = QPointF(line1[0], line1[1])
            p2 = QPointF(line1[2], line1[3])

            # Check distance to the measurement line(s)
            dist = self._distance_point_to_segment(pos, p1, p2)

            if len(lines_list) >= 2:
                line2 = lines_list[1]
                p3 = QPointF(line2[0], line2[1])
                p4 = QPointF(line2[2], line2[3])
                dist = min(dist, self._distance_point_to_segment(pos, p3, p4))

                # Check rectangle edges for better selection on rectangle view
                corners = self.build_measurement_rectangles_for_lines(line1, line2)
                if corners:
                    for i in range(4):
                        a = corners[i]
                        b = corners[(i + 1) % 4]
                        dist = min(dist, self._distance_point_to_segment(pos, a, b))
                    if self._point_in_polygon(pos, corners):
                        dist = 0.0

            if dist <= best_dist:
                best_dist = dist
                best_id = measurement_id
                
        if hasattr(self, "multiline_measurements"):
            for measurement_id, lines_list in self.multiline_measurements.items():
                for line in lines_list:
                    p1 = QPointF(line[0], line[1])
                    p2 = QPointF(line[2], line[3])
                    dist = self._distance_point_to_segment(pos, p1, p2)
                    if dist <= best_dist:
                        best_dist = dist
                        best_id = measurement_id

        return best_id

    def build_measurement_rectangles_for_lines(self, line1, line2):
        """Build rectangle corners for a specific measurement."""
        p1 = QPointF(line1[0], line1[1])
        p2 = QPointF(line1[2], line1[3])
        p3 = QPointF(line2[0], line2[1])
        p4 = QPointF(line2[2], line2[3])

        length_vec = p2 - p1
        length_len = math.sqrt(length_vec.x()**2 + length_vec.y()**2)
        width_vec = p4 - p3
        width_len = math.sqrt(width_vec.x()**2 + width_vec.y()**2)
        if length_len < 0.001 or width_len < 0.001:
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

    def _objective_color_for_name(self, name):
        """Return the objective tag color based on magnification."""
        if not name:
            return QColor(52, 152, 219)
        match = re.search(r"(\d+)", str(name))
        mag = int(match.group(1)) if match else None
        if mag in (10,):
            return QColor("#f1c40f")
        if mag in (16, 20, 25, 32):
            return QColor("#2ecc71")
        if mag in (40, 50):
            return QColor("#3498db")
        if mag in (63,):
            return QColor("#1f4ea8")
        if mag in (4, 5):
            return QColor("#e74c3c")
        if mag in (6,):
            return QColor("#f39c12")
        return QColor("#3498db")

    def _objective_tag_text(self, display_name: str | None, objective_key: str | None = None) -> str:
        text = display_name or objective_key or ""
        short = ImageGalleryWidget._short_objective_label(text, self.tr)
        return short or text

    def _format_exposure_time(self, value):
        """Format exposure time for display."""
        if value is None:
            return "-"
        try:
            if isinstance(value, tuple):
                value = value[0] / value[1] if value[1] else 0
            if value >= 1:
                text = f"{value:.1f}".rstrip("0").rstrip(".")
                return f"{text}s"
            if value > 0:
                denom = round(1 / value)
                return f"1/{denom}"
        except Exception:
            return "-"
        return "-"

    def _format_fstop(self, value):
        """Format f-stop for display."""
        if value is None:
            return "-"
        try:
            if isinstance(value, tuple):
                value = value[0] / value[1] if value[1] else 0
            return f"f/{value:.1f}".rstrip("0").rstrip(".")
        except Exception:
            return "-"

    def _extract_exif_lines(self, image_path):
        """Extract EXIF info lines for overlay display."""
        path = Path(image_path) if image_path else None
        if not path or not path.exists():
            return []
        lines = []
        lines.append(f"File: {path.name}")

        try:
            with Image.open(path) as img:
                exif = img.getexif()
                if not exif:
                    return lines
                exif_data = {ExifTags.TAGS.get(k, k): v for k, v in exif.items()}
        except Exception:
            return lines

        date = exif_data.get("DateTimeOriginal") or exif_data.get("DateTime")
        if date:
            lines.append(f"Date: {date}")

        iso = exif_data.get("ISOSpeedRatings")
        if iso is None:
            iso = exif_data.get("PhotographicSensitivity")
        if isinstance(iso, tuple):
            iso = iso[0]
        if iso is not None:
            lines.append(f"ISO: {iso}")

        fstop = self._format_fstop(exif_data.get("FNumber") or exif_data.get("ApertureValue"))
        if fstop != "-":
            lines.append(f"F-stop: {fstop}")

        shutter = self._format_exposure_time(exif_data.get("ExposureTime"))
        if shutter == "-":
            shutter = self._format_exposure_time(exif_data.get("ShutterSpeedValue"))
        if shutter != "-":
            lines.append(f"Shutter: {shutter}")

        make = exif_data.get("Make", "")
        model = exif_data.get("Model", "")
        camera = " ".join(part for part in (make, model) if part).strip()
        if camera:
            lines.append(f"Camera: {camera}")

        return lines

    def update_exif_panel(self, image_path):
        """Update the Info panel with EXIF data."""
        if not hasattr(self, "exif_info_label"):
            return
        lines = self._extract_exif_lines(image_path)
        if not lines:
            self.exif_info_label.setText(self.tr("No image loaded"))
            return
        mp_value = self._megapixels_from_path(image_path) or self._current_image_megapixels()
        if mp_value:
            mp_text = f"Image resolution: {self._format_megapixels(float(mp_value))}MP"
        else:
            mp_text = "Image resolution: --"
        scale_factor = None
        if self.current_image_id:
            image_data = ImageDB.get_image(self.current_image_id)
            if image_data:
                scale_factor = image_data.get("resample_scale_factor")
        if not isinstance(scale_factor, (int, float)) or scale_factor <= 0:
            scale_factor = 1.0
        lines.append(mp_text)
        lines.append(f"Resize scale factor: {float(scale_factor):.2f}")
        folder_path = Path(image_path).resolve().parent
        folder_uri = folder_path.as_uri()
        html_lines = [html.escape(line) for line in lines]
        html_lines.append(
            f'Folder: <a href="{folder_uri}">Folder location</a>'
        )
        self.exif_info_label.setText("<br>".join(html_lines))

    def _set_measure_image_note_text(self, text: str | None, *, enabled: bool) -> None:
        if not hasattr(self, "measure_image_note_input"):
            return
        self._loading_measure_image_note = True
        try:
            self.measure_image_note_input.blockSignals(True)
            self.measure_image_note_input.setPlainText(str(text or ""))
            self.measure_image_note_input.setEnabled(bool(enabled))
        finally:
            self.measure_image_note_input.blockSignals(False)
            self._loading_measure_image_note = False

    def _queue_measure_image_note_save(self) -> None:
        if self._loading_measure_image_note:
            return
        if not self.current_image_id:
            return
        if hasattr(self, "_measure_image_note_save_timer"):
            self._measure_image_note_save_timer.start()

    def _commit_measure_image_note(self) -> None:
        if self._loading_measure_image_note:
            return
        image_id = int(self.current_image_id or 0)
        if image_id <= 0 or not hasattr(self, "measure_image_note_input"):
            return
        note_text = str(self.measure_image_note_input.toPlainText() or "").strip() or None
        try:
            current_image = ImageDB.get_image(image_id) or {}
            existing_note = str(current_image.get("notes") or "").strip() or None
        except Exception:
            existing_note = None
        if existing_note == note_text:
            return
        ImageDB.update_image(image_id, notes=note_text)

    def _flush_measure_image_note(self) -> None:
        if not hasattr(self, "_measure_image_note_save_timer"):
            return
        if not self._measure_image_note_save_timer.isActive():
            return
        self._measure_image_note_save_timer.stop()
        self._commit_measure_image_note()

    def _get_gray_image(self):
        """Return a cached grayscale numpy array of the current image."""
        if not self.current_pixmap or not self.current_image_id:
            return None
        if self.auto_gray_cache_id == self.current_image_id and self.auto_gray_cache is not None:
            return self.auto_gray_cache

        image = self.current_pixmap.toImage().convertToFormat(QImage.Format.Format_Grayscale8)
        width = image.width()
        height = image.height()
        buf = image.constBits() if hasattr(image, "constBits") else image.bits()
        arr = np.frombuffer(buf, dtype=np.uint8)
        bytes_per_line = image.bytesPerLine()
        gray = arr.reshape((height, bytes_per_line))[:, :width].copy()
        self.auto_gray_cache = gray
        self.auto_gray_cache_id = self.current_image_id
        return gray

    def _update_auto_threshold_from_points(self, points):
        """Update the auto threshold based on a refined measurement."""
        if not self.active_observation_id or len(points) != 4:
            return
        gray = self._get_gray_image()
        if gray is None:
            return
        h, w = gray.shape
        line1_mid = QPointF((points[0].x() + points[1].x()) / 2,
                            (points[0].y() + points[1].y()) / 2)
        line2_mid = QPointF((points[2].x() + points[3].x()) / 2,
                            (points[2].y() + points[3].y()) / 2)
        center = QPointF((line1_mid.x() + line2_mid.x()) / 2,
                         (line1_mid.y() + line2_mid.y()) / 2)
        cx = int(round(center.x()))
        cy = int(round(center.y()))
        if cx < 0 or cy < 0 or cx >= w or cy >= h:
            return
        center_intensity = float(gray[cy, cx])
        edge_samples = []
        for pt in points:
            x = int(round(pt.x()))
            y = int(round(pt.y()))
            if 0 <= x < w and 0 <= y < h:
                edge_samples.append(float(gray[y, x]))
        if not edge_samples:
            return
        edge_mean = float(np.mean(edge_samples))
        max_radius = self._update_auto_max_radius_from_points(points)
        max_radius = max_radius or min(h, w) / 2
        ring_radius = int(min(max_radius * 1.2, min(h, w) / 2))
        ring_samples = []
        for angle in range(0, 360, 30):
            rad = math.radians(angle)
            rx = int(round(cx + math.cos(rad) * ring_radius))
            ry = int(round(cy + math.sin(rad) * ring_radius))
            if 0 <= rx < w and 0 <= ry < h:
                ring_samples.append(float(gray[ry, rx]))
        bg_mean = float(np.mean(ring_samples)) if ring_samples else center_intensity
        threshold = abs(bg_mean - edge_mean) / 255.0
        threshold = max(0.02, min(0.6, threshold))
        self.auto_threshold = threshold
        ObservationDB.set_auto_threshold(self.active_observation_id, threshold)

    def _auto_find_radii(self, cx, cy, gray, background_mean,
                         threshold, max_radius, angle_step=10):
        """Return radii at sampled angles using inward search."""
        height, width = gray.shape
        delta = max(2.0, threshold * 255.0)
        def hit(val):
            return abs(val - background_mean) >= delta

        radii = {}
        for angle in range(0, 180, angle_step):
            rad = math.radians(angle)
            dx = math.cos(rad)
            dy = math.sin(rad)
            for r in range(max_radius, 0, -1):
                x = int(round(cx + dx * r))
                y = int(round(cy + dy * r))
                if x < 0 or y < 0 or x >= width or y >= height:
                    break
                if hit(float(gray[y, x])):
                    radii[angle] = r
                    break
        return radii

    def _update_auto_max_radius_from_points(self, points):
        """Update max recorded spore radius (pixels) from measurement points."""
        if len(points) != 4:
            return self.auto_max_radius
        line1_mid = QPointF((points[0].x() + points[1].x()) / 2,
                            (points[0].y() + points[1].y()) / 2)
        line2_mid = QPointF((points[2].x() + points[3].x()) / 2,
                            (points[2].y() + points[3].y()) / 2)
        center = QPointF((line1_mid.x() + line2_mid.x()) / 2,
                         (line1_mid.y() + line2_mid.y()) / 2)
        max_radius = 0.0
        for pt in points:
            dx = pt.x() - center.x()
            dy = pt.y() - center.y()
            max_radius = max(max_radius, math.hypot(dx, dy))
        if max_radius > 0:
            if self.auto_max_radius is None or max_radius > self.auto_max_radius:
                self.auto_max_radius = max_radius
        return self.auto_max_radius

    def _compute_observation_max_radius(self, observation_id):
        """Initialize auto max radius from stored measurements."""
        if not observation_id:
            self.auto_max_radius = None
            return
        measurements = MeasurementDB.get_measurements_for_observation(observation_id)
        max_radius = None
        for measurement in measurements:
            if not all(measurement.get(f'p{i}_{axis}') is not None
                       for i in range(1, 5) for axis in ['x', 'y']):
                continue
            points = [
                QPointF(measurement['p1_x'], measurement['p1_y']),
                QPointF(measurement['p2_x'], measurement['p2_y']),
                QPointF(measurement['p3_x'], measurement['p3_y']),
                QPointF(measurement['p4_x'], measurement['p4_y'])
            ]
            line1_mid = QPointF((points[0].x() + points[1].x()) / 2,
                                (points[0].y() + points[1].y()) / 2)
            line2_mid = QPointF((points[2].x() + points[3].x()) / 2,
                                (points[2].y() + points[3].y()) / 2)
            center = QPointF((line1_mid.x() + line2_mid.x()) / 2,
                             (line1_mid.y() + line2_mid.y()) / 2)
            for pt in points:
                radius = math.hypot(pt.x() - center.x(), pt.y() - center.y())
                if max_radius is None or radius > max_radius:
                    max_radius = radius
        self.auto_max_radius = max_radius

    def auto_measure_at_click(self, pos):
        """Auto-detect spore axes based on intensity drop from a click point."""
        if not self.current_pixmap or not self.current_image_id:
            return
        gray = self._get_gray_image()
        if gray is None:
            return
        height, width = gray.shape
        cx = int(round(pos.x()))
        cy = int(round(pos.y()))
        if cx < 0 or cy < 0 or cx >= width or cy >= height:
            return

        center_intensity = float(gray[cy, cx])
        threshold = self.auto_threshold if self.auto_threshold is not None else self.auto_threshold_default
        max_radius = self.auto_max_radius if self.auto_max_radius else min(width, height) / 2
        max_radius = int(min(max_radius * 1.2, min(width, height) / 2))
        max_radius = max(10, max_radius)
        ring_samples = []
        for angle in range(0, 360, 30):
            rad = math.radians(angle)
            rx = int(round(cx + math.cos(rad) * max_radius))
            ry = int(round(cy + math.sin(rad) * max_radius))
            if 0 <= rx < width and 0 <= ry < height:
                ring_samples.append(float(gray[ry, rx]))
        bg_mean = float(np.mean(ring_samples)) if ring_samples else center_intensity

        radii = self._auto_find_radii(cx, cy, gray, bg_mean, threshold, max_radius, angle_step=10)

        if len(radii) < 4:
            self.show_auto_debug_dialog(
                pos, radii, None, None, threshold, center_intensity, bg_mean, max_radius
            )
            self.measure_status_label.setText(self.tr("Auto: Edge not found"))
            self.measure_status_label.setStyleSheet(f"color: #e67e22; font-weight: bold; font-size: {pt(9)}pt;")
            return

        major_angle = max(radii, key=lambda a: radii[a])
        major_radius = radii[major_angle]
        target_minor = (major_angle + 90) % 180
        minor_angle = min(
            radii.keys(),
            key=lambda a: min(
                abs(a - target_minor),
                abs(a - target_minor + 180),
                abs(a - target_minor - 180)
            )
        )
        minor_radius = radii.get(minor_angle, min(radii.values()))

        major_rad = math.radians(major_angle)
        minor_rad = math.radians(minor_angle)
        center = QPointF(cx, cy)
        major_dir = QPointF(math.cos(major_rad), math.sin(major_rad))
        minor_dir = QPointF(math.cos(minor_rad), math.sin(minor_rad))

        p1 = center - major_dir * major_radius
        p2 = center + major_dir * major_radius
        p3 = center - minor_dir * minor_radius
        p4 = center + minor_dir * minor_radius

        self.points = [p1, p2, p3, p4]
        self.temp_lines = [
            [p1.x(), p1.y(), p2.x(), p2.y()],
            [p3.x(), p3.y(), p4.x(), p4.y()]
        ]
        self.update_display_lines()
        self._update_auto_max_radius_from_points(self.points)
        self.show_auto_debug_dialog(
            pos, radii, major_angle, minor_angle, threshold, center_intensity, bg_mean, max_radius
        )
        self.complete_measurement()

    def show_auto_debug_dialog(self, pos, radii, major_angle, minor_angle,
                               threshold, center_intensity, background_mean,
                               max_radius):
        """Show a debug popup with auto-measure traces and stats."""
        if not self.current_pixmap:
            return
        dialog = QDialog(self)
        dialog.setWindowTitle(self.tr("Auto Measure Debug"))
        layout = QVBoxLayout(dialog)

        center = QPointF(pos.x(), pos.y())
        crop_size = 300
        half = crop_size / 2
        left = max(0, int(center.x() - half))
        top = max(0, int(center.y() - half))
        right = min(self.current_pixmap.width(), int(center.x() + half))
        bottom = min(self.current_pixmap.height(), int(center.y() + half))
        crop_rect = QRectF(left, top, right - left, bottom - top)

        gray = self._get_gray_image()
        gray_crop = None
        if gray is not None:
            gray_crop = gray[int(crop_rect.y()):int(crop_rect.y() + crop_rect.height()),
                            int(crop_rect.x()):int(crop_rect.x() + crop_rect.width())].copy()

        image_label = QLabel()
        layout.addWidget(image_label)

        controls_row = QHBoxLayout()
        threshold_label = QLabel(self.tr("Threshold:"))
        threshold_input = QDoubleSpinBox()
        threshold_input.setRange(0.02, 0.6)
        threshold_input.setDecimals(3)
        threshold_input.setSingleStep(0.01)
        threshold_input.setValue(float(threshold))
        show_gray_checkbox = QCheckBox(self.tr("Show grayscale"))
        controls_row.addWidget(threshold_label)
        controls_row.addWidget(threshold_input)
        controls_row.addStretch()
        controls_row.addWidget(show_gray_checkbox)
        layout.addLayout(controls_row)

        stats_label = QLabel()
        stats_label.setStyleSheet(f"font-size: {pt(9)}pt; color: #2c3e50;")
        layout.addWidget(stats_label)

        plot_label = QLabel()
        layout.addWidget(plot_label)

        def render_overlay(current_threshold):
            base_radii = self._auto_find_radii(
                int(round(center.x())),
                int(round(center.y())),
                gray,
                background_mean,
                current_threshold,
                max_radius,
                angle_step=10
            ) if gray is not None else {}

            full_radii = dict(base_radii)
            for angle, radius in base_radii.items():
                full_radii[(angle + 180) % 360] = radius

            major = None
            minor = None
            if base_radii:
                major = max(base_radii, key=lambda a: base_radii[a])
                target_minor = (major + 90) % 180
                minor = min(
                    base_radii.keys(),
                    key=lambda a: min(
                        abs(a - target_minor),
                        abs(a - target_minor + 180),
                        abs(a - target_minor - 180)
                    )
                )

            if show_gray_checkbox.isChecked() and gray_crop is not None:
                h, w = gray_crop.shape
                gray_img = QImage(gray_crop.data, w, h, w, QImage.Format.Format_Grayscale8)
                base_pixmap = QPixmap.fromImage(gray_img.copy())
            else:
                base_pixmap = self.current_pixmap.copy(crop_rect.toRect())

            target_size = 360
            scale = min(target_size / max(1, base_pixmap.width()),
                        target_size / max(1, base_pixmap.height()))
            scaled = base_pixmap.scaled(
                int(base_pixmap.width() * scale),
                int(base_pixmap.height() * scale),
                Qt.KeepAspectRatio,
                Qt.SmoothTransformation
            )

            overlay = QPixmap(scaled.size())
            overlay.fill(Qt.transparent)
            painter = QPainter(overlay)
            painter.setRenderHint(QPainter.Antialiasing)
            painter.drawPixmap(0, 0, scaled)

            center_local = QPointF((center.x() - crop_rect.x()) * scale,
                                   (center.y() - crop_rect.y()) * scale)

            for angle, radius in full_radii.items():
                rad = math.radians(angle)
                dx = math.cos(rad)
                dy = math.sin(rad)
                end = QPointF(center_local.x() + dx * radius * scale,
                              center_local.y() + dy * radius * scale)
                pen_color = QColor(200, 200, 200)
                if major is not None and angle == major:
                    pen_color = QColor(46, 204, 113)
                elif minor is not None and angle % 180 == minor:
                    pen_color = QColor(231, 76, 60)
                painter.setPen(QPen(pen_color, 2))
                painter.drawLine(center_local, end)
                painter.setBrush(pen_color)
                painter.drawEllipse(end, 3, 3)

            painter.setPen(QPen(QColor(52, 152, 219), 2))
            painter.setBrush(Qt.NoBrush)
            painter.drawEllipse(center_local, 4, 4)
            painter.end()

            image_label.setPixmap(overlay)

            direction = "darker" if background_mean < center_intensity else "brighter"
            stats_text = (
                f"Threshold: {current_threshold:.3f} ({direction} edge)\n"
                f"Center intensity: {center_intensity:.1f}  Background: {background_mean:.1f}\n"
                f"Rays found: {len(full_radii)}  Max radius: {max_radius}px"
            )
            stats_label.setText(stats_text)

            if self.active_observation_id:
                self.auto_threshold = current_threshold
                ObservationDB.set_auto_threshold(self.active_observation_id, current_threshold)

            if gray is None:
                plot_label.clear()
                return

            plot_w = 360
            plot_h = 200
            plot_pixmap = QPixmap(plot_w, plot_h)
            plot_pixmap.fill(QColor(255, 255, 255))
            plot_painter = QPainter(plot_pixmap)
            plot_painter.setRenderHint(QPainter.Antialiasing)

            left_pad = 30
            right_pad = 10
            top_pad = 10
            bottom_pad = 25
            axis_w = plot_w - left_pad - right_pad
            axis_h = plot_h - top_pad - bottom_pad

            plot_painter.setPen(QPen(QColor(200, 200, 200), 1))
            for i in range(1, 5):
                y = top_pad + axis_h * (i / 5)
                plot_painter.drawLine(left_pad, int(y), left_pad + axis_w, int(y))
            for i in range(1, 5):
                x = left_pad + axis_w * (i / 5)
                plot_painter.drawLine(int(x), top_pad, int(x), top_pad + axis_h)

            plot_painter.setPen(QPen(QColor(120, 120, 120), 1))
            plot_painter.drawLine(left_pad, top_pad, left_pad, top_pad + axis_h)
            plot_painter.drawLine(left_pad, top_pad + axis_h, left_pad + axis_w, top_pad + axis_h)

            max_radius_local = max(1, max_radius)

            def map_point(r, intensity):
                x = left_pad + ((max_radius_local - r) / max_radius_local) * axis_w
                y = top_pad + axis_h - (intensity / 255.0) * axis_h
                return QPointF(x, y)

            def line_color(angle):
                if major is not None and angle == major:
                    return QColor(46, 204, 113)
                if minor is not None and angle % 180 == minor:
                    return QColor(231, 76, 60)
                return QColor(120, 120, 120, 140)

            cx = int(round(center.x()))
            cy = int(round(center.y()))
            for angle in range(0, 180, 10):
                rad = math.radians(angle)
                dx = math.cos(rad)
                dy = math.sin(rad)
                points = []
                for r in range(max_radius_local, -1, -1):
                    x = int(round(cx + dx * r))
                    y = int(round(cy + dy * r))
                    if x < 0 or y < 0 or x >= gray.shape[1] or y >= gray.shape[0]:
                        break
                    points.append(map_point(r, float(gray[y, x])))
                if len(points) > 1:
                    plot_painter.setPen(QPen(line_color(angle), 1))
                    for i in range(1, len(points)):
                        plot_painter.drawLine(points[i - 1], points[i])

            plot_painter.setPen(QPen(QColor(120, 120, 120), 1))
            plot_painter.drawText(5, top_pad + 10, "I")
            plot_painter.drawText(plot_w - 24, plot_h - 8, "r")
            plot_painter.end()
            plot_label.setPixmap(plot_pixmap)

        threshold_input.valueChanged.connect(lambda value: render_overlay(value))
        show_gray_checkbox.toggled.connect(lambda _: render_overlay(threshold_input.value()))
        render_overlay(threshold_input.value())

        dialog.setLayout(layout)
        dialog.exec()

    def select_measurement_in_table(self, measurement_id):
        """Select a measurement row by id."""
        for row in range(self.measurements_table.rowCount()):
            item = self.measurements_table.item(row, 0)
            if item and item.data(Qt.UserRole) == measurement_id:
                self.measurements_table.selectRow(row)
                self.on_measurement_selected()
                return

    def _get_default_export_dir(self) -> str:
        settings = get_app_settings()
        last_dir = settings.get("last_export_dir")
        if last_dir and Path(last_dir).exists():
            return last_dir
        docs = QStandardPaths.writableLocation(QStandardPaths.DocumentsLocation)
        if docs:
            return docs
        return str(Path.home())

    def _remember_export_dir(self, filepath: str | None) -> None:
        if not filepath:
            return
        export_dir = str(Path(filepath).parent)
        update_app_settings({"last_export_dir": export_dir})

    def _get_default_import_dir(self) -> str:
        settings = get_app_settings()
        last_dir = settings.get("last_import_dir")
        if last_dir and Path(last_dir).exists():
            return last_dir
        docs = QStandardPaths.writableLocation(QStandardPaths.DocumentsLocation)
        if docs:
            return docs
        return str(Path.home())

    def _remember_import_dir(self, filepath: str | None) -> None:
        if not filepath:
            return
        import_dir = str(Path(filepath).parent)
        update_app_settings({"last_import_dir": import_dir})

    def export_annotated_image(self):
        """Export the current image view with annotations."""
        if not self.current_pixmap:
            QMessageBox.warning(self, "No image", "Load an image before exporting.")
            return

        default_name = "annotated_image"
        if self.active_observation_id:
            obs = ObservationDB.get_observation(self.active_observation_id)
            if obs:
                genus = obs.get("genus") or ""
                species = obs.get("species") or obs.get("species_guess") or ""
                date = obs.get("date") or ""
                vernacular = (obs.get("common_name") or "").strip()
                if not vernacular and genus and species:
                    lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
                    db_path = resolve_vernacular_db_path(lang)
                    if db_path and Path(db_path).exists():
                        vernacular_db = VernacularDB(db_path, language_code=lang)
                        vernacular = vernacular_db.vernacular_from_taxon(genus, species) or ""
                sci = " ".join([p for p in [genus, species] if p]).strip()
                parts = [p for p in [vernacular, sci, date] if p]
                name = " - ".join(parts).strip()
                name = name.replace(":", "-")
                name = re.sub(r'[<>:"/\\\\|?*]', "_", name)
                name = re.sub(r"\\s+", " ", name).strip()
                if name:
                    default_name = name

        current_view_crop = self.image_label.get_current_view_crop_rect()
        dialog = SharedExportImageDialog(
            self.current_pixmap.width(),
            self.current_pixmap.height(),
            self.export_scale_percent,
            parent=self,
            crop_width=(max(1, current_view_crop[2] - current_view_crop[0]) if current_view_crop else None),
            crop_height=(max(1, current_view_crop[3] - current_view_crop[1]) if current_view_crop else None),
            crop_enabled=bool(current_view_crop),
        )
        self.image_label.set_export_crop_preview(False)
        if hasattr(dialog, "crop_to_view_checkbox"):
            dialog.crop_to_view_checkbox.toggled.connect(self.image_label.set_export_crop_preview)
        if dialog.exec() != QDialog.Accepted:
            self.image_label.set_export_crop_preview(False)
            return
        self.image_label.set_export_crop_preview(False)

        export_settings = dialog.get_settings()
        self.export_scale_percent = export_settings["scale_percent"]
        ext_map = {
            "jpg": ".jpg",
            "png": ".png",
            "svg": ".svg",
        }
        default_ext = ext_map.get(self.export_format, ".png")
        default_path = str(Path(self._get_default_export_dir()) / f"{default_name}{default_ext}")
        filename, selected_filter = QFileDialog.getSaveFileName(
            self,
            "Export Image",
            default_path,
            "PNG Images (*.png);;JPEG Images (*.jpg);;SVG Files (*.svg)"
        )
        if not filename:
            return
        filter_map = {
            "PNG Images (*.png)": "png",
            "JPEG Images (*.jpg)": "jpg",
            "SVG Files (*.svg)": "svg",
        }
        selected_format = filter_map.get(selected_filter)
        suffix = Path(filename).suffix.lower()
        if selected_format is None:
            if suffix == ".svg":
                selected_format = "svg"
            elif suffix in {".jpg", ".jpeg"}:
                selected_format = "jpg"
            else:
                selected_format = "png"
        final_ext = ext_map.get(selected_format, ".png")
        if not re.search(r"\.(png|jpe?g|svg)$", filename, re.IGNORECASE):
            filename += final_ext
        self._remember_export_dir(filename)
        self.export_format = selected_format
        crop_rect = current_view_crop if export_settings.get("crop_to_view") else None

        target_w = export_settings["width"]
        target_h = export_settings["height"]
        if selected_format == "svg":
            if self.image_label.export_annotated_svg(
                filename,
                (target_w, target_h),
                crop_rect=crop_rect,
            ):
                self.export_format = "svg"
            return

        exported = self.image_label.export_annotated_pixmap(
            crop_rect=crop_rect,
            match_screen_appearance=True,
        )
        if exported:
            if target_w and target_h:
                exported = exported.scaled(
                    target_w,
                    target_h,
                    Qt.KeepAspectRatio,
                    Qt.SmoothTransformation
                )
            fmt = "JPEG" if selected_format == "jpg" else "PNG"
            quality = export_settings["quality"]
            if fmt == "JPEG":
                exported.save(filename, fmt, quality)
                self.export_format = "jpg"
            else:
                exported.save(filename, fmt)
                self.export_format = "png"


    def _compute_measurement_center(self, line1, line2):
        """Compute center point for a measurement from two lines."""
        p1 = QPointF(line1[0], line1[1])
        p2 = QPointF(line1[2], line1[3])
        p3 = QPointF(line2[0], line2[1])
        p4 = QPointF(line2[2], line2[3])
        line1_mid = QPointF((p1.x() + p2.x()) / 2, (p1.y() + p2.y()) / 2)
        line2_mid = QPointF((p3.x() + p4.x()) / 2, (p3.y() + p4.y()) / 2)
        return QPointF(
            (line1_mid.x() + line2_mid.x()) / 2,
            (line1_mid.y() + line2_mid.y()) / 2
        )

    def _measurement_unit_for_display(self, measurement_type: str | None = None) -> tuple[str, float]:
        """Return (unit, divisor) for displaying measurements based on image type.

        Field images use no unit label — the calibration defines the scale.
        The divisor still converts µm storage values to mm-scale for display.
        """
        image_type = (self.current_image_type or "").strip().lower()
        if image_type == "field":
            return "", 1000.0
        if image_type == "microscope":
            return "\u03bcm", 1.0
        if measurement_type and self.normalize_measurement_category(measurement_type) == "field":
            return "", 1000.0
        return "\u03bcm", 1.0

    def _build_measurement_label(self, measurement_id, line1, line2, length_um, width_um, measurement_type=None):
        """Build a label entry for measurement overlays."""
        center = self._compute_measurement_center(line1, line2)
        unit, divisor = self._measurement_unit_for_display(measurement_type)
        length_value = (length_um / divisor) if length_um is not None else None
        width_value = (width_um / divisor) if width_um is not None else None
        return {
            "id": measurement_id,
            "center": center,
            "length_um": length_um,
            "width_um": width_um,
            "length_value": length_value,
            "width_value": width_value,
            "unit": unit,
            "line1": line1,
            "line2": line2
        }

    def _build_line_measurement_label(self, measurement_id, line1, length_um, measurement_type=None):
        """Build a label entry for line-only measurements."""
        unit, divisor = self._measurement_unit_for_display(measurement_type)
        length_value = (length_um / divisor) if length_um is not None else None
        center = QPointF(
            (line1[0] + line1[2]) / 2,
            (line1[1] + line1[3]) / 2,
        )
        return {
            "id": measurement_id,
            "kind": "line",
            "center": center,
            "length_um": length_um,
            "length_value": length_value,
            "unit": unit,
            "line": line1,
        }

    def _build_multiline_measurement_label(self, measurement_id, length_um, last_point, measurement_type=None):
        """Build a label entry for multiline measurements."""
        unit, divisor = self._measurement_unit_for_display(measurement_type)
        length_value = (length_um / divisor) if length_um is not None else None
        return {
            "id": measurement_id,
            "kind": "multiline",
            "center": last_point,
            "length_um": length_um,
            "length_value": length_value,
            "unit": unit,
        }

    def load_measurement_lines(self):
        """Load measurement lines from database for current image.

        Note: We don't have the original point coordinates stored,
        so we can't reconstruct the exact lines. For now, keep existing
        lines when loading an image. In future, could store point coordinates
        in database.
        """
        self.measurement_lines = {}
        self.multiline_measurements = {}
        self.temp_lines = []
        self.measurement_labels = []
        self._measurement_type_by_id = {}
        self.image_label.set_measurement_lines([])
        self.image_label.set_measurement_rectangles([])
        self.image_label.set_measurement_labels([])

        if not self.current_image_id:
            return

        measurements = MeasurementDB.get_measurements_for_image(self.current_image_id)
        for measurement in measurements:
            notes = measurement.get("notes") or ""
            is_multiline = False
            if '{"multiline"' in notes:
                try:
                    data = json.loads(notes)
                    pts = data.get("multiline")
                    if pts and len(pts) >= 2:
                        lines = []
                        for i in range(len(pts) - 1):
                            lines.append([pts[i][0], pts[i][1], pts[i+1][0], pts[i+1][1]])
                        self.multiline_measurements[measurement['id']] = lines
                        self._measurement_type_by_id[int(measurement['id'])] = self.normalize_measurement_category(
                            measurement.get("measurement_type")
                        )
                        length_um = measurement.get('length_um')
                        if length_um is not None:
                            last_point = QPointF(pts[-1][0], pts[-1][1])
                            self.measurement_labels.append(
                                self._build_multiline_measurement_label(
                                    measurement['id'],
                                    length_um,
                                    last_point,
                                    measurement.get("measurement_type"),
                                )
                            )
                        is_multiline = True
                except Exception:
                    pass
            if is_multiline:
                continue

            if not all(measurement.get(f'p{i}_{axis}') is not None
                       for i in range(1, 3) for axis in ['x', 'y']):
                continue
            line1 = [
                measurement['p1_x'], measurement['p1_y'],
                measurement['p2_x'], measurement['p2_y']
            ]
            lines = [line1]
            if all(measurement.get(f'p{i}_{axis}') is not None for i in range(3, 5) for axis in ['x', 'y']):
                line2 = [
                    measurement['p3_x'], measurement['p3_y'],
                    measurement['p4_x'], measurement['p4_y']
                ]
                lines.append(line2)
            self.measurement_lines[measurement['id']] = lines
            self._measurement_type_by_id[int(measurement['id'])] = self.normalize_measurement_category(
                measurement.get("measurement_type")
            )
            length_um = measurement.get('length_um')
            width_um = measurement.get('width_um')
            if len(lines) >= 2 and (length_um is None or width_um is None):
                line2 = lines[1]
                dx1 = line1[2] - line1[0]
                dy1 = line1[3] - line1[1]
                dx2 = line2[2] - line2[0]
                dy2 = line2[3] - line2[1]
                dist1 = math.sqrt(dx1**2 + dy1**2) * self.microns_per_pixel
                dist2 = math.sqrt(dx2**2 + dy2**2) * self.microns_per_pixel
                length_um = max(dist1, dist2)
                width_um = min(dist1, dist2)
            if len(lines) == 1 and length_um is None:
                dx = line1[2] - line1[0]
                dy = line1[3] - line1[1]
                length_um = math.sqrt(dx**2 + dy**2) * self.microns_per_pixel
            if len(lines) >= 2 and length_um is not None and width_um is not None:
                self.measurement_labels.append(
                    self._build_measurement_label(
                        measurement['id'],
                        line1,
                        lines[1],
                        length_um,
                        width_um,
                        measurement.get("measurement_type"),
                    )
                )
            elif len(lines) == 1 and length_um is not None:
                self.measurement_labels.append(
                    self._build_line_measurement_label(
                        measurement['id'],
                        line1,
                        length_um,
                        measurement.get("measurement_type"),
                    )
                )

        self.update_display_lines()

    def on_dimensions_changed(self, measurement_id, new_length_um, new_width_um, new_points):
        """Handle dimension changes from the preview widget."""
        is_line = len(new_points) == 2
        has_width = (not is_line) and (new_width_um is not None and new_width_um > 0)
        q_value = new_length_um / new_width_um if has_width else None

        # Update the database
        conn = get_connection()
        cursor = conn.cursor()
        if is_line:
            cursor.execute('''
                UPDATE spore_measurements
                SET length_um = ?, width_um = ?, notes = ?,
                    p1_x = ?, p1_y = ?, p2_x = ?, p2_y = ?,
                    p3_x = NULL, p3_y = NULL, p4_x = NULL, p4_y = NULL
                WHERE id = ?
            ''', (
                new_length_um,
                None,
                None,
                new_points[0].x(), new_points[0].y(),
                new_points[1].x(), new_points[1].y(),
                measurement_id,
            ))
        else:
            cursor.execute('''
                UPDATE spore_measurements
                SET length_um = ?, width_um = ?, notes = ?,
                    p1_x = ?, p1_y = ?, p2_x = ?, p2_y = ?,
                    p3_x = ?, p3_y = ?, p4_x = ?, p4_y = ?
                WHERE id = ?
            ''', (
                new_length_um,
                new_width_um,
                f"Q={q_value:.1f}" if q_value is not None else None,
                new_points[0].x(), new_points[0].y(),
                new_points[1].x(), new_points[1].y(),
                new_points[2].x(), new_points[2].y(),
                new_points[3].x(), new_points[3].y(),
                measurement_id,
            ))

        conn.commit()
        conn.close()

        self._invalidate_gallery_thumbnail_cache(measurement_id)

        # Update the UI
        self.update_measurements_table()
        self.update_statistics()

        # Update the measurement lines on the main image
        line1 = [new_points[0].x(), new_points[0].y(), new_points[1].x(), new_points[1].y()]
        if is_line:
            self.measurement_lines[measurement_id] = [line1]
        else:
            line2 = [new_points[2].x(), new_points[2].y(), new_points[3].x(), new_points[3].y()]
            self.measurement_lines[measurement_id] = [line1, line2]
        measurement_type = None
        for cached in self.measurements_cache:
            if cached.get("id") == measurement_id:
                measurement_type = cached.get("measurement_type")
                break
        if measurement_type is None:
            record = self._get_measurement_by_id(measurement_id)
            if record:
                measurement_type = record.get("measurement_type")

        for idx, label in enumerate(self.measurement_labels):
            if label.get("id") == measurement_id:
                if is_line:
                    self.measurement_labels[idx] = self._build_line_measurement_label(
                        measurement_id, line1, new_length_um, measurement_type
                    )
                else:
                    self.measurement_labels[idx] = self._build_measurement_label(
                        measurement_id, line1, line2, new_length_um, new_width_um, measurement_type
                    )
                break
        else:
            if is_line:
                self.measurement_labels.append(
                    self._build_line_measurement_label(
                        measurement_id, line1, new_length_um, measurement_type
                    )
                )
            else:
                self.measurement_labels.append(
                    self._build_measurement_label(
                        measurement_id, line1, line2, new_length_um, new_width_um, measurement_type
                    )
                )
        self.update_display_lines()

        self.measure_status_label.setText(self.tr("Click to measure next"))
        self.measure_status_label.setStyleSheet(f"color: #27ae60; font-weight: bold; font-size: {pt(9)}pt;")

        # If calibration measurement was edited, offer to rescale all other measurements
        if (not is_line) and self.normalize_measurement_category(measurement_type) == "calibration":
            self._on_calibration_measurement_edited(measurement_id, new_length_um, new_points)

    def _on_calibration_measurement_edited(self, measurement_id, new_length_um, new_points):
        """Handle edit of a calibration measurement — offer to rescale others."""
        # Compute pixel distance from p1–p2 points (scale bar endpoints)
        dx = new_points[1].x() - new_points[0].x()
        dy = new_points[1].y() - new_points[0].y()
        pixel_dist = (dx * dx + dy * dy) ** 0.5
        if pixel_dist <= 0 or new_length_um <= 0:
            return
        new_scale_mpp = new_length_um / pixel_dist

        # Get the image_id for this measurement
        image_id = None
        for m in self.measurements_cache:
            if m.get("id") == measurement_id:
                image_id = m.get("image_id")
                break
        if not image_id:
            record = self._get_measurement_by_id(measurement_id)
            if record:
                image_id = record.get("image_id")
        if not image_id:
            return

        # Get old scale from the images table
        conn = get_connection()
        row = conn.execute(
            "SELECT scale_microns_per_pixel FROM images WHERE id = ?", (image_id,)
        ).fetchone()
        conn.close()
        old_scale_mpp = row[0] if row and row[0] else None
        if not old_scale_mpp or abs(new_scale_mpp - old_scale_mpp) < 1e-9:
            return

        # Ask user
        box = QMessageBox(self)
        box.setIcon(QMessageBox.Question)
        box.setWindowTitle(self.tr("Changing calibration"))
        box.setText(self.tr(
            "You changed the calibration measurement. "
            "Do you want to rescale all other measurements for this image to match the new scale?"
        ))
        ok_btn = box.addButton(self.tr("Rescale"), QMessageBox.AcceptRole)
        box.addButton(self.tr("Keep as-is"), QMessageBox.RejectRole)
        box.exec()
        if box.clickedButton() != ok_btn:
            return

        ratio = new_scale_mpp / old_scale_mpp
        conn = get_connection()
        measurements = conn.execute(
            "SELECT id, length_um, width_um FROM spore_measurements WHERE image_id = ? AND measurement_type != 'calibration'",
            (image_id,),
        ).fetchall()
        for m in measurements:
            m_id, length, width = m
            if length is None:
                continue
            new_l = float(length) * ratio
            new_w = float(width) * ratio if width is not None else None
            q = new_l / new_w if new_w and new_w > 0 else 0
            conn.execute(
                "UPDATE spore_measurements SET length_um = ?, width_um = ?, notes = ? WHERE id = ?",
                (new_l, new_w, f"Q={q:.1f}", m_id),
            )
        conn.execute(
            "UPDATE images SET scale_microns_per_pixel = ? WHERE id = ?",
            (new_scale_mpp, image_id),
        )
        conn.commit()
        conn.close()

        # Refresh UI
        self.update_measurements_table()
        self.update_statistics()
        if hasattr(self, "microns_per_pixel"):
            self.microns_per_pixel = new_scale_mpp
        self.update_display_lines()

    def show_measurement_preview(self, measurement):
        """Show preview for a specific measurement."""
        notes = measurement.get("notes") or ""
        if '{"multiline"' in notes:
            self.spore_preview.clear()
            return

        has_line = (
            measurement.get('p1_x') is not None and
            measurement.get('p1_y') is not None and
            measurement.get('p2_x') is not None and
            measurement.get('p2_y') is not None
        )
        has_rect = (
            has_line and
            measurement.get('p3_x') is not None and
            measurement.get('p3_y') is not None and
            measurement.get('p4_x') is not None and
            measurement.get('p4_y') is not None
        )

        if has_rect:

            # Reconstruct points
            from PySide6.QtCore import QPointF
            points = [
                QPointF(measurement['p1_x'], measurement['p1_y']),
                QPointF(measurement['p2_x'], measurement['p2_y']),
                QPointF(measurement['p3_x'], measurement['p3_y']),
                QPointF(measurement['p4_x'], measurement['p4_y'])
            ]

            # Update preview with measurement ID for editing
            is_field = (self.current_image_type or "").strip().lower() == "field"
            self.spore_preview.set_spore(
                self.current_pixmap,
                points,
                measurement['length_um'],
                measurement['width_um'] or 0,
                self.microns_per_pixel,
                measurement['id'],
                display_unit="mm" if is_field else "\u03bcm",
                display_divisor=1000.0 if is_field else 1.0,
            )
        elif has_line:
            from PySide6.QtCore import QPointF
            points = [
                QPointF(measurement['p1_x'], measurement['p1_y']),
                QPointF(measurement['p2_x'], measurement['p2_y']),
            ]
            is_field = (self.current_image_type or "").strip().lower() == "field"
            self.spore_preview.set_line(
                self.current_pixmap,
                points,
                measurement['length_um'] or 0,
                self.microns_per_pixel,
                measurement['id'],
                display_unit="mm" if is_field else "\u03bcm",
                display_divisor=1000.0 if is_field else 1.0,
            )
        else:
            # No point data available for this measurement
            self.spore_preview.clear()

    def on_measurement_selected(self):
        """Handle measurement selection from table."""
        selected_rows = self.measurements_table.selectedIndexes()
        if not selected_rows:
            self.spore_preview.clear()
            self._clear_measurement_highlight()
            return

        row = selected_rows[0].row()
        if row < len(self.measurements_cache):
            measurement = self.measurements_cache[row]
            measurement_type = self.normalize_measurement_category(measurement.get("measurement_type") or "spores")
            if hasattr(self, "measure_category_combo"):
                idx = self.measure_category_combo.findData(measurement_type)
                if idx >= 0:
                    self._measure_category_sync = True
                    self.measure_category_combo.setCurrentIndex(idx)
            self._measure_category_sync = False
            self.update_display_lines()
            image_id = measurement.get('image_id')
            if image_id and image_id != self.current_image_id:
                if getattr(self, "_prevent_cross_image_switch_from_table_selection", False):
                    return
                image_data = ImageDB.get_image(image_id)
                if image_data:
                    self.load_image_record(image_data, refresh_table=False)
            self.show_measurement_preview(measurement)
            self._highlight_selected_measurement(measurement)

    def update_measurements_table(self):
        """Update the measurements table."""
        if not self.current_image_id and not self.active_observation_id:
            self.measurements_table.setRowCount(0)
            self.spore_preview.clear()
            self.measurements_cache = []
            return

        image_labels = {}
        if self.active_observation_id:
            images = ImageDB.get_images_for_observation(self.active_observation_id)
            image_labels = {img['id']: f"Image {idx + 1}" for idx, img in enumerate(images)}
            measurements = MeasurementDB.get_measurements_for_observation(self.active_observation_id)
        else:
            measurements = MeasurementDB.get_measurements_for_image(self.current_image_id)
            if self.current_image_id:
                image_labels[self.current_image_id] = "Image 1"

        measurements = [
            measurement
            for measurement in (measurements or [])
            if not self._is_calibration_measurement(measurement)
        ]

        self.measurements_cache = measurements
        self.measurements_table.setRowCount(len(measurements))

        for row, measurement in enumerate(measurements):
            image_label = image_labels.get(measurement.get('image_id'), "Image ?")
            image_num = image_label.replace("Image ", "")
            image_item = QTableWidgetItem(image_num)
            image_item.setData(Qt.UserRole, measurement['id'])
            self.measurements_table.setItem(row, 0, image_item)

            category = self.normalize_measurement_category(measurement.get("measurement_type"))
            category_label = self.format_measurement_category(category)
            self.measurements_table.setItem(row, 1, QTableWidgetItem(category_label))

            # Length
            length = measurement['length_um']
            self.measurements_table.setItem(row, 2, QTableWidgetItem(f"{length:.1f}"))

            # Width
            width = measurement.get('width_um')
            if width is not None and width > 0:
                self.measurements_table.setItem(row, 3, QTableWidgetItem(f"{width:.1f}"))
                q = length / width if width > 0 else 0
                self.measurements_table.setItem(row, 4, QTableWidgetItem(f"{q:.1f}"))
            else:
                self.measurements_table.setItem(row, 3, QTableWidgetItem("-"))
                self.measurements_table.setItem(row, 4, QTableWidgetItem("-"))

        # Update gallery view only when visible
        if self.is_analysis_visible() and not self._suppress_gallery_update:
            self.refresh_gallery_filter_options()
            self.schedule_gallery_refresh()
        self.update_statistics()

    def normalize_measurement_category(self, category):
        """Normalize measurement categories for filtering."""
        if not category:
            return "spores"
        canonical = DatabaseTerms.canonicalize_measure(category)
        if canonical:
            value = str(canonical).strip().lower()
        else:
            value = str(category).strip().lower()
        if value in ("manual", "spore", "spores"):
            return "spores"
        if value == "calibration":
            return "calibration"
        return value

    def _is_calibration_measurement(self, measurement: dict | None) -> bool:
        """Return whether a measurement row is the hidden scale-bar calibration helper."""
        if not measurement:
            return False
        return self.normalize_measurement_category(measurement.get("measurement_type")) == "calibration"

    def format_measurement_category(self, category):
        """Format measurement categories for display."""
        if str(category or "").strip().lower() == "all_except_spores":
            return self.tr("All except spores")
        canonical = DatabaseTerms.canonicalize_measure(category)
        if canonical:
            return DatabaseTerms.translate("measure", canonical)
        return str(category).replace("_", " ").title()

    def refresh_gallery_filter_options(self):
        """Refresh gallery filter dropdown based on observation measurements."""
        if not hasattr(self, "gallery_filter_combo"):
            return

        current = self.gallery_filter_combo.currentData()
        saved_measurement_type = None
        if self.active_observation_id:
            saved_settings = self._load_gallery_settings()
            if isinstance(saved_settings, dict):
                saved_measurement_type = saved_settings.get("measurement_type")
        self.gallery_filter_combo.blockSignals(True)
        self.gallery_filter_combo.clear()
        self.gallery_filter_combo.addItem(self.tr("All except spores"), "all_except_spores")

        if self.active_observation_id:
            raw_types = MeasurementDB.get_measurement_types_for_observation(self.active_observation_id)
            normalized = []
            for entry in raw_types:
                category = self.normalize_measurement_category(entry)
                if category == "calibration":
                    continue
                if category not in normalized:
                    normalized.append(category)

            order = []
            for canonical in DatabaseTerms.default_values("measure"):
                normalized_category = self.normalize_measurement_category(canonical)
                if normalized_category and normalized_category not in order:
                    order.append(normalized_category)
            ordered = [cat for cat in order if cat in normalized]
            for category in normalized:
                if category not in ordered:
                    ordered.append(category)

            for category in ordered:
                self.gallery_filter_combo.addItem(
                    self.format_measurement_category(category),
                    category
                )

        self.gallery_filter_combo.blockSignals(False)
        pending_category = self._pending_gallery_category
        desired = pending_category
        if desired is None:
            desired = saved_measurement_type
        if str(desired).strip().lower() == "spore":
            desired = "spores"
        if str(desired).strip().lower() == "all":
            desired = "all_except_spores"
        selected_category = None
        if desired and self.gallery_filter_combo.findData(desired) >= 0:
            self.gallery_filter_combo.setCurrentIndex(self.gallery_filter_combo.findData(desired))
            selected_category = desired
        else:
            idx = self.gallery_filter_combo.findData("spores")
            if idx >= 0:
                self.gallery_filter_combo.setCurrentIndex(idx)
                selected_category = "spores"
            else:
                idx = self.gallery_filter_combo.findData("all_except_spores")
                if idx >= 0:
                    self.gallery_filter_combo.setCurrentIndex(idx)
                    selected_category = "all_except_spores"
                elif current and self.gallery_filter_combo.findData(current) >= 0:
                    self.gallery_filter_combo.setCurrentIndex(self.gallery_filter_combo.findData(current))
                    selected_category = current
        should_persist_default = (
            bool(self.active_observation_id)
            and pending_category is None
            and not saved_measurement_type
            and bool(selected_category)
        )
        if should_persist_default:
            self._save_gallery_settings()
        self._pending_gallery_category = None

    def is_analysis_visible(self):
        """Return True if the Analysis tab is active."""
        return hasattr(self, "tab_widget") and self.tab_widget.currentIndex() == 2

    def _switch_tab_from_observations(self, target_index: int) -> None:
        """Switch tabs only when Observations tab is currently active."""
        if not hasattr(self, "tab_widget"):
            return
        if self.tab_widget.currentIndex() != 0:
            return
        if target_index < 0 or target_index >= self.tab_widget.count():
            return
        self.tab_widget.setCurrentIndex(int(target_index))

    def on_tab_changed(self, index):
        """Handle tab changes for analysis/measure."""
        _t0 = time.perf_counter()
        if index == 0 and hasattr(self, "_sync_observations_tab_publish_state"):
            self._sync_observations_tab_publish_state(self.active_observation_id)
        if index in (1, 2, 3, 4) and hasattr(self, "observations_tab"):
            selected = self.observations_tab.get_selected_observation()
            if selected:
                obs_id, display_name = selected
                if self.active_observation_id != obs_id:
                    self.on_observation_selected(
                        obs_id,
                        display_name,
                        switch_tab=False,
                        suppress_gallery=True
                    )
                else:
                    self.active_observation_name = display_name
                    self.refresh_observation_images(select_image_id=self.current_image_id)
                    self.update_measurements_table()
                    if (
                        not self.current_image_id
                        and getattr(self, "observation_images", None)
                    ):
                        self.goto_image_index(0)
        if index == 3 and hasattr(self, "live_lab_tab"):
            self.live_lab_tab.sync_from_active_observation()
        if index == 4 and hasattr(self, "ingestion_hub_tab"):
            try:
                self.ingestion_hub_tab.refresh_observation_queue(
                    select_observation_id=int(self.active_observation_id or 0) or None
                )
            except Exception:
                pass
            self.ingestion_hub_tab.sync_from_active_observation()
        if index == 1 and hasattr(self, "_apply_measure_gallery_publish_selection"):
            self._apply_measure_gallery_publish_selection()
        if index == 1 and hasattr(self, "measure_button"):
            self.measure_button.setEnabled(True)
        if index == 2:
            self.apply_gallery_settings()
            self.refresh_gallery_filter_options()
            self._refresh_reference_species_availability()
            self.schedule_gallery_refresh()

    def schedule_gallery_refresh(self):
        """Coalesce multiple refresh requests into a single gallery update."""
        self._gallery_refresh_pending = True
        if self._gallery_refresh_timer is None:
            self._gallery_refresh_timer = QTimer(self)
            self._gallery_refresh_timer.setSingleShot(True)
            self._gallery_refresh_timer.timeout.connect(self._run_scheduled_gallery_refresh)
        # debounce rapid callers
        self._gallery_refresh_timer.start(50)

    def _run_scheduled_gallery_refresh(self):
        if not self._gallery_refresh_pending:
            return
        if self._gallery_refresh_in_progress:
            return
        now = time.perf_counter()
        if now - self._gallery_last_refresh_time < 0.2:
            self._gallery_refresh_timer.start(100)
            return
        self._gallery_refresh_pending = False
        self.update_gallery()

    def get_gallery_measurements(self):
        """Get measurements to show in the gallery."""
        if self.active_observation_id:
            measurements = MeasurementDB.get_measurements_for_observation(self.active_observation_id)
        elif self.current_image_id:
            measurements = MeasurementDB.get_measurements_for_image(self.current_image_id)
        else:
            return []

        category = None
        if hasattr(self, "gallery_filter_combo"):
            category = self.gallery_filter_combo.currentData()

        # Always exclude calibration measurements from the Analysis tab
        measurements = [
            m for m in measurements
            if self.normalize_measurement_category(m.get("measurement_type")) != "calibration"
        ]

        if category == "all_except_spores":
            measurements = [
                m for m in measurements
                if self.normalize_measurement_category(m.get("measurement_type")) != "spores"
            ]
        elif category:
            measurements = [
                m for m in measurements
                if self.normalize_measurement_category(m.get("measurement_type")) == category
            ]

        return measurements

    def get_measurement_pixmap(self, measurement, pixmap_cache):
        """Get the pixmap for a measurement, cached by path."""
        image_path = measurement.get('image_filepath') or self.current_image_path
        if not image_path:
            return None

        if image_path == self.current_image_path and self.current_pixmap:
            return self.current_pixmap

        if image_path not in pixmap_cache:
            pixmap_cache[image_path] = QPixmap(image_path)
        return pixmap_cache[image_path]

    def update_gallery(self):
        """Update the gallery grid with all measured items."""
        if not self.is_analysis_visible():
            return
        if self._gallery_refresh_in_progress:
            return

        self._cancel_gallery_render()
        self._gallery_refresh_in_progress = True
        self._set_gallery_busy_hint(
            self._consume_gallery_refresh_hint(self.tr("Refreshing spore plot and gallery..."))
        )

        category = self.gallery_filter_combo.currentData() if hasattr(self, "gallery_filter_combo") else None
        if category != self._last_gallery_category:
            self.gallery_filter_mode = None
            self.gallery_filter_value = None
            self.gallery_filter_ids = set()
            self._last_gallery_category = category

        image_labels = {}
        if self.active_observation_id:
            images = ImageDB.get_images_for_observation(self.active_observation_id)
            image_labels = {img['id']: f"Image {idx + 1}" for idx, img in enumerate(images)}
        self.gallery_image_labels = image_labels

        all_measurements = self.get_gallery_measurements()
        all_measurement_ids = {int(m.get("id")) for m in all_measurements if m.get("id")}
        if self.gallery_selected_measurement_id and self.gallery_selected_measurement_id not in all_measurement_ids:
            self.gallery_selected_measurement_id = None
        self.update_graph_plots(all_measurements)

        if self._gallery_collapsed:
            self._complete_gallery_refresh()
            return

        # Clear existing gallery items
        while self.gallery_grid.count():
            item = self.gallery_grid.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        self._gallery_thumbnail_frames = {}
        self._gallery_thumbnail_labels = {}
        self._gallery_measurement_lookup = {}
        self._gallery_thumbnail_render_state = None
        self._update_gallery_container_size(0)

        measurements = self._filter_gallery_measurements(all_measurements)
        measurements = self._sort_gallery_measurements(measurements)
        if not measurements:
            self._complete_gallery_refresh()
            return

        self._reset_gallery_cache_if_needed()

        orient = hasattr(self, 'orient_checkbox') and self.orient_checkbox.isChecked()
        uniform_scale = hasattr(self, 'uniform_scale_checkbox') and self.uniform_scale_checkbox.isChecked()
        thumbnail_size = self._gallery_thumbnail_size()
        total = len(measurements)
        self._gallery_render_total_items = total
        items_per_row = max(1, total)

        uniform_length_um = None
        if uniform_scale:
            for measurement in all_measurements:
                length_um = measurement.get("length_um")
                if length_um is None:
                    continue
                if uniform_length_um is None or length_um > uniform_length_um:
                    uniform_length_um = length_um

        render_state = {
            "items_per_row": items_per_row,
            "thumbnail_size": thumbnail_size,
            "image_labels": image_labels,
            "orient": orient,
            "uniform_scale": uniform_scale,
            "uniform_length_um": uniform_length_um,
            "image_color_cache": {},
            "display_index": 1,
            "running_width": 0,
            "max_height": 0,
        }
        self._gallery_thumbnail_render_state = {
            "thumbnail_size": thumbnail_size,
            "orient": orient,
            "uniform_scale": uniform_scale,
            "uniform_length_um": uniform_length_um,
            "image_color_cache": render_state["image_color_cache"],
        }
        self._gallery_render_total_width = 0
        self._gallery_render_max_height = thumbnail_size

        immediate_count = min(7, total)
        for measurement in measurements[:immediate_count]:
            self._add_gallery_item(measurement, render_state)

        if total > immediate_count:
            self._gallery_render_queue = list(measurements[immediate_count:])
            self._gallery_render_state = render_state
            self._gallery_render_timer = QTimer(self)
            self._gallery_render_timer.timeout.connect(self._render_gallery_batch)
            self._gallery_render_timer.start(10)
        else:
            self._complete_gallery_refresh()

    def _cancel_gallery_render(self):
        if self._gallery_render_timer is not None:
            self._gallery_render_timer.stop()
            self._gallery_render_timer.deleteLater()
            self._gallery_render_timer = None
        self._gallery_render_queue = []
        self._gallery_render_state = None
        self._gallery_render_total_items = 0

    def _render_gallery_batch(self):
        if not self._gallery_render_queue or not self._gallery_render_state:
            self._finish_gallery_render()
            return
        remaining = len(self._gallery_render_queue)
        total = max(remaining, int(self._gallery_render_total_items or 0))
        rendered = max(0, total - remaining)
        self._set_gallery_busy_hint(
            self.tr("Rendering spore thumbnails {done}/{total}...").format(
                done=rendered,
                total=max(1, total),
            )
        )
        batch_size = 4
        for _ in range(min(batch_size, len(self._gallery_render_queue))):
            measurement = self._gallery_render_queue.pop(0)
            self._add_gallery_item(measurement, self._gallery_render_state)
        QApplication.processEvents()
        if not self._gallery_render_queue:
            self._finish_gallery_render()

    def _finish_gallery_render(self):
        self._cancel_gallery_render()
        self._complete_gallery_refresh()

    def _complete_gallery_refresh(self):
        self._set_gallery_strip_height()
        self._gallery_last_refresh_time = time.perf_counter()
        self._gallery_refresh_in_progress = False
        self._set_gallery_busy_hint("")
        if self._gallery_refresh_pending:
            self.schedule_gallery_refresh()

    def _reset_gallery_cache_if_needed(self):
        if self._gallery_thumb_cache_observation_id != self.active_observation_id:
            self._gallery_thumb_cache_observation_id = self.active_observation_id
            self._gallery_thumb_cache = {}
            self._gallery_thumb_geometry_cache = {}
            self._gallery_pixmap_cache = {}

    def _analysis_gallery_frame_style(self, selected: bool = False) -> str:
        return "QWidget { border: none; background: transparent; }"

    def _apply_analysis_gallery_frame_glow(self, frame: QFrame, selected: bool, hovered: bool = False) -> None:
        frame.setGraphicsEffect(None)

    def _refresh_analysis_gallery_frame_state(self, measurement_id: int | None = None, hovered: bool = False) -> None:
        if measurement_id is None:
            for existing_id in list(self._gallery_thumbnail_frames.keys()):
                self._refresh_analysis_gallery_frame_state(existing_id, hovered=False)
            return
        frame = self._gallery_thumbnail_frames.get(int(measurement_id))
        if frame is None:
            return
        is_selected = int(measurement_id) == int(self.gallery_selected_measurement_id or 0)
        frame.setStyleSheet(self._analysis_gallery_frame_style(selected=is_selected))
        if hasattr(frame, "set_measure_selected"):
            frame.set_measure_selected(is_selected)
        if hasattr(frame, "set_measure_hovered"):
            frame.set_measure_hovered(hovered and not is_selected)
        self._apply_analysis_gallery_frame_glow(frame, is_selected, hovered=hovered and not is_selected)

    def _select_analysis_gallery_measurement(self, measurement_id: int | None, update_plot: bool = True) -> None:
        previous_selected = int(self.gallery_selected_measurement_id) if self.gallery_selected_measurement_id else None
        normalized_id = int(measurement_id) if measurement_id else None
        if normalized_id and previous_selected == normalized_id:
            self.gallery_selected_measurement_id = None
        else:
            self.gallery_selected_measurement_id = normalized_id
        if previous_selected != self.gallery_selected_measurement_id:
            self._refresh_analysis_gallery_frame_state(previous_selected, hovered=False)
            self._refresh_analysis_gallery_frame_state(self.gallery_selected_measurement_id, hovered=False)
        if update_plot and hasattr(self, "gallery_plot_figure"):
            self.update_graph_plots_only()

    def _prepare_analysis_gallery_for_tab_switch(self) -> None:
        """Clear transient Analysis gallery UI effects before hiding the tab."""
        self._gallery_hover_hint_key = ""
        self._set_gallery_hint("")
        for frame in list(self._gallery_thumbnail_frames.values()):
            try:
                frame.setGraphicsEffect(None)
            except Exception:
                pass
        if hasattr(self, "gallery_scroll") and self.gallery_scroll is not None:
            try:
                self.gallery_scroll.clearFocus()
            except Exception:
                pass
        self.setFocus(Qt.OtherFocusReason)

    def _invalidate_gallery_thumbnail_cache(self, measurement_id):
        if not self._gallery_thumb_cache:
            return
        keys_to_remove = [key for key in self._gallery_thumb_cache if key[0] == measurement_id]
        for key in keys_to_remove:
            self._gallery_thumb_cache.pop(key, None)
            self._gallery_thumb_geometry_cache.pop(key, None)

    def _gallery_thumbnail_cache_key(
        self,
        measurement_id,
        orient,
        uniform_scale,
        uniform_length_um,
        thumbnail_size,
        extra_rotation,
        color_key,
        rectangle_style,
        rectangle_thickness,
        selected,
    ):
        if uniform_scale:
            uniform_key = round(float(uniform_length_um or 0.0), 6)
        else:
            uniform_key = None
        return (
            measurement_id,
            orient,
            uniform_scale,
            uniform_key,
            thumbnail_size,
            extra_rotation,
            color_key,
            normalize_rectangle_style(rectangle_style),
            round(clamp_rectangle_thickness(rectangle_thickness), 3),
            bool(selected),
        )

    def _add_gallery_item(self, measurement, render_state):
        from PySide6.QtWidgets import QLabel as QLabel2, QToolButton

        if not all(measurement.get(f'p{i}_{axis}') is not None
                   for i in range(1, 5) for axis in ['x', 'y']):
            return

        thumbnail_meta: dict = {}
        thumbnail = self._get_gallery_thumbnail(measurement, render_state, thumbnail_meta)
        if thumbnail is None:
            return

        thumbnail_size = render_state["thumbnail_size"]
        orient = render_state["orient"]
        image_labels = render_state["image_labels"]
        image_id = measurement.get('image_id')
        measurement_id = measurement.get('id')
        if measurement_id is None:
            return
        self._gallery_measurement_lookup[int(measurement_id)] = measurement

        container = AnalysisGalleryTile(self._current_measure_rectangle_style())
        tile_width = thumbnail.width()
        tile_height = thumbnail.height()
        container.setFixedSize(tile_width, tile_height)
        container.setCursor(Qt.PointingHandCursor)
        self._gallery_thumbnail_frames[int(measurement_id)] = container
        container.set_measurement_polygon(thumbnail_meta.get("polygon"))

        label = QLabel2()
        label.setPixmap(thumbnail)
        label.setFixedSize(tile_width, tile_height)
        label.setStyleSheet("border: none; background: transparent;")
        label.setAttribute(Qt.WA_TransparentForMouseEvents, True)
        label.setParent(container)
        label.move(0, 0)
        label.show()
        self._gallery_thumbnail_labels[int(measurement_id)] = label

        link_btn = QToolButton(container)
        link_btn.setIcon(QIcon(str(Path(__file__).parent.parent / "assets" / "icons" / "link.svg")))
        link_label = image_labels.get(image_id, "Image ?")
        link_btn.setToolTip(link_label)
        link_btn.setProperty("instant_tooltip", True)
        link_btn.setProperty("gallery_fade_icon", True)
        link_btn.setProperty("gallery_idle_opacity", 0.28)
        link_btn.installEventFilter(self)
        link_btn.setFixedSize(18, 18)
        link_btn.setIconSize(QSize(18, 18))
        link_btn.setStyleSheet(
            "QToolButton { background: transparent; border: none; }"
            "QToolButton:hover { background-color: rgba(0, 0, 0, 0.08); }"
        )
        link_opacity = QGraphicsOpacityEffect(link_btn)
        link_opacity.setOpacity(0.28)
        link_btn.setGraphicsEffect(link_opacity)
        link_btn.clicked.connect(
            lambda checked, mid=measurement_id: self.open_measurement_from_gallery(mid)
        )
        link_btn.raise_()

        rotate_btn = QToolButton(container)
        rotate_btn.setIcon(QIcon(str(Path(__file__).parent.parent / "assets" / "icons" / "rotate.svg")))
        rotate_btn.setToolTip(self.tr("Rotate 180"))
        rotate_btn.setProperty("instant_tooltip", True)
        rotate_btn.setProperty("gallery_fade_icon", True)
        rotate_btn.setProperty("gallery_idle_opacity", 0.28)
        rotate_btn.installEventFilter(self)
        rotate_btn.setFixedSize(18, 18)
        rotate_btn.setIconSize(QSize(18, 18))
        rotate_btn.setStyleSheet(
            "QToolButton { background: transparent; border: none; }"
            "QToolButton:hover { background-color: rgba(0, 0, 0, 0.08); }"
        )
        rotate_opacity = QGraphicsOpacityEffect(rotate_btn)
        rotate_opacity.setOpacity(0.28)
        rotate_btn.setGraphicsEffect(rotate_opacity)
        rotate_btn.clicked.connect(
            lambda checked, mid=measurement_id: self.rotate_gallery_thumbnail(mid)
        )
        rotate_btn.raise_()
        container.set_overlay_buttons(link_btn, rotate_btn)

        display_index = render_state["display_index"]
        items_per_row = render_state["items_per_row"]
        row = (display_index - 1) // items_per_row
        col = (display_index - 1) % items_per_row
        self.gallery_grid.addWidget(container, row, col)
        container.mousePressEvent = lambda _event, mid=int(measurement_id): self._select_analysis_gallery_measurement(mid)
        self._refresh_analysis_gallery_frame_state(int(measurement_id))
        render_state["display_index"] = display_index + 1
        render_state["running_width"] = int(render_state.get("running_width", 0)) + tile_width
        render_state["max_height"] = max(int(render_state.get("max_height", 0)), tile_height)
        self._gallery_render_total_width = render_state["running_width"]
        self._gallery_render_max_height = render_state["max_height"]
        self._update_gallery_container_size(render_state["display_index"] - 1)

    def _get_gallery_thumbnail(self, measurement, render_state, metadata_out: dict | None = None):
        from PySide6.QtCore import QPointF

        pixmap = self.get_measurement_pixmap(measurement, self._gallery_pixmap_cache)
        if not pixmap or pixmap.isNull():
            return None

        measurement_id = measurement['id']
        extra_rotation = measurement.get("gallery_rotation") or self.gallery_rotations.get(measurement_id, 0)
        image_id = measurement.get('image_id')

        image_color_cache = render_state["image_color_cache"]
        stored_color = None
        mpp = None
        if image_id:
            if image_id not in image_color_cache:
                image_data = ImageDB.get_image(image_id)
                image_color_cache[image_id] = (
                    {
                        "measure_color": image_data.get('measure_color') if image_data else None,
                        "mpp": image_data.get('scale_microns_per_pixel') if image_data else None
                    }
                )
            cached = image_color_cache[image_id]
            stored_color = cached.get("measure_color") if cached else None
            mpp = cached.get("mpp") if cached else None

        measure_color = QColor(stored_color) if stored_color else self.default_measure_color
        color_key = measure_color.name()
        rectangle_style = self._current_measure_rectangle_style()
        rectangle_thickness = self._current_measure_rectangle_thickness()
        cache_key = self._gallery_thumbnail_cache_key(
            measurement_id,
            render_state["orient"],
            render_state["uniform_scale"],
            render_state["uniform_length_um"],
            render_state["thumbnail_size"],
            extra_rotation,
            color_key,
            rectangle_style,
            rectangle_thickness,
            False,
        )
        if cache_key in self._gallery_thumb_cache:
            if metadata_out is not None:
                metadata_out.update(self._gallery_thumb_geometry_cache.get(cache_key, {}))
            return self._gallery_thumb_cache[cache_key]

        points = [
            QPointF(measurement['p1_x'], measurement['p1_y']),
            QPointF(measurement['p2_x'], measurement['p2_y']),
            QPointF(measurement['p3_x'], measurement['p3_y']),
            QPointF(measurement['p4_x'], measurement['p4_y'])
        ]

        uniform_length_px = None
        if render_state["uniform_scale"] and render_state["uniform_length_um"]:
            if not mpp or mpp <= 0:
                p1 = points[0]
                p2 = points[1]
                p3 = points[2]
                p4 = points[3]
                line1_len = math.hypot(p2.x() - p1.x(), p2.y() - p1.y())
                line2_len = math.hypot(p4.x() - p3.x(), p4.y() - p3.y())
                length_px = max(line1_len, line2_len)
                length_um = measurement.get("length_um")
                if length_px > 0 and length_um:
                    mpp = float(length_um) / float(length_px)
            if mpp and mpp > 0:
                uniform_length_px = float(render_state["uniform_length_um"]) / float(mpp)

        thumbnail_meta: dict = {}
        thumbnail = self.create_spore_thumbnail(
            pixmap,
            points,
            measurement['length_um'],
            measurement['width_um'] or 0,
            render_state["thumbnail_size"],
            0,
            orient=render_state["orient"],
            extra_rotation=extra_rotation,
            uniform_length_px=uniform_length_px,
            color=measure_color,
            rectangle_style=rectangle_style,
            rectangle_thickness=rectangle_thickness,
            selected=False,
            metadata=thumbnail_meta,
        )
        if thumbnail:
            self._gallery_thumb_cache[cache_key] = thumbnail
            self._gallery_thumb_geometry_cache[cache_key] = dict(thumbnail_meta)
            if metadata_out is not None:
                metadata_out.update(thumbnail_meta)
        return thumbnail

    def _update_gallery_container_size(self, count):
        if not hasattr(self, "gallery_container") or not hasattr(self, "gallery_grid"):
            return
        if count <= 0:
            self.gallery_container.setMinimumWidth(0)
            return
        margins = self.gallery_grid.contentsMargins()
        width = int(getattr(self, "_gallery_render_total_width", 0))
        height = int(getattr(self, "_gallery_render_max_height", self._gallery_tile_metrics()["image_height"]))
        width += margins.left() + margins.right()
        height += margins.top() + margins.bottom()
        self.gallery_container.setMinimumWidth(width)
        self.gallery_container.setMinimumHeight(height)

    def _sort_gallery_measurements(self, measurements):
        """Sort measurements by the selected gallery sort key (ascending)."""
        if not hasattr(self, "gallery_sort_combo"):
            return measurements
        sort_key = self.gallery_sort_combo.currentData() or ""
        if not sort_key:
            return measurements
        image_order_map = {}
        if sort_key == "images" and self.active_observation_id:
            try:
                ordered_images = ImageDB.get_images_for_observation(self.active_observation_id)
                image_order_map = {
                    int(image.get("id")): idx
                    for idx, image in enumerate(ordered_images)
                    if image.get("id") is not None
                }
            except Exception:
                image_order_map = {}
        def _key(m):
            l = m.get("length_um") or 0.0
            w = m.get("width_um") or 0.0
            if sort_key == "images":
                image_id = int(m.get("image_id") or 0)
                measurement_id = int(m.get("id") or 0)
                return (
                    image_order_map.get(image_id, 10**9),
                    measurement_id,
                )
            if sort_key == "length":
                return float(l)
            if sort_key == "width":
                return float(w)
            if sort_key == "q":
                return float(l) / float(w) if w else 0.0
            return 0.0
        return sorted(measurements, key=_key)

    def _filter_gallery_measurements(self, measurements):
        """Apply gallery selection filter to measurements."""
        if not measurements:
            return measurements
        if self.gallery_filter_mode == "points" and self.gallery_filter_ids:
            return [m for m in measurements if m.get("id") in self.gallery_filter_ids]
        if self.gallery_filter_mode == "bin" and self.gallery_filter_value:
            metric, min_val, max_val = self.gallery_filter_value
            filtered = []
            for m in measurements:
                length = m.get("length_um")
                width = m.get("width_um")
                if length is None or width is None or width <= 0:
                    continue
                if metric == "L":
                    value = length
                elif metric == "W":
                    value = width
                else:
                    value = length / width
                if min_val <= value <= max_val:
                    filtered.append(m)
            return filtered
        return measurements

    def clear_gallery_filter(self):
        """Clear the gallery selection filter."""
        self.gallery_filter_mode = None
        self.gallery_filter_value = None
        self.gallery_filter_ids = set()
        self.gallery_selected_measurement_id = None
        self._update_gallery_filter_label()
        self.schedule_gallery_refresh()

    def _is_dark_theme(self) -> bool:
        """Return True if the current palette is dark."""
        return self.palette().window().color().lightness() < 128

    def _apply_plot_dark_theme(self, fig, axes: list) -> None:
        """Apply dark colours to a matplotlib Figure and its Axes list."""
        fig_bg  = "#1c1c1e"
        ax_bg   = "#2b2b2d"
        text_c  = "#e8e8e8"
        spine_c = "#555557"
        tick_c  = "#8e8e93"
        grid_c  = "#3a3a3c"
        fig.patch.set_facecolor(fig_bg)
        for ax in axes:
            if ax is None:
                continue
            ax.set_facecolor(ax_bg)
            ax.tick_params(colors=tick_c, which="both")
            ax.xaxis.label.set_color(text_c)
            ax.yaxis.label.set_color(text_c)
            for spine in ax.spines.values():
                spine.set_edgecolor(spine_c)
            leg = ax.get_legend()
            if leg is not None:
                leg.get_frame().set_facecolor("#3a3a3c")
                leg.get_frame().set_edgecolor("#555557")
                for txt in leg.get_texts():
                    txt.set_color(text_c)

    def _apply_plot_light_theme(self, fig, axes: list) -> None:
        """Reset matplotlib Figure and Axes to light colours."""
        fig.patch.set_facecolor("#f5f5f5")
        for ax in axes:
            if ax is None:
                continue
            ax.set_facecolor("white")
            ax.tick_params(colors="#555555", which="both")
            ax.xaxis.label.set_color("#2c3e50")
            ax.yaxis.label.set_color("#2c3e50")
            for spine in ax.spines.values():
                spine.set_edgecolor("#cccccc")
            leg = ax.get_legend()
            if leg is not None:
                leg.get_frame().set_facecolor("white")
                leg.get_frame().set_edgecolor("#cccccc")
                for txt in leg.get_texts():
                    txt.set_color("#2c3e50")

    def _gallery_highlighted_measurement_ids(self, measurements) -> set[int]:
        """Return measurement ids highlighted by the active plot selection."""
        highlighted_ids = set()
        if self.gallery_selected_measurement_id:
            highlighted_ids.add(int(self.gallery_selected_measurement_id))
        if self.gallery_filter_mode == "points" and self.gallery_filter_ids:
            highlighted_ids.update(int(measurement_id) for measurement_id in self.gallery_filter_ids if measurement_id)
            return highlighted_ids
        if self.gallery_filter_mode != "bin" or not self.gallery_filter_value:
            return highlighted_ids
        metric, min_val, max_val = self.gallery_filter_value
        for measurement in measurements or []:
            measurement_id = measurement.get("id")
            length = measurement.get("length_um")
            width = measurement.get("width_um")
            if not measurement_id or length is None or width is None or width <= 0:
                continue
            if metric == "L":
                value = length
            elif metric == "W":
                value = width
            else:
                value = length / width
            if min_val <= value <= max_val:
                highlighted_ids.add(int(measurement_id))
        return highlighted_ids

    def _gallery_plot_width_ratios(self, histogram_count_max: int | None) -> tuple[float, float]:
        """Choose scatter/histogram width ratios based on available plot width."""
        canvas_width = 0
        if hasattr(self, "gallery_plot_canvas") and self.gallery_plot_canvas is not None:
            try:
                canvas_width = int(self.gallery_plot_canvas.width())
            except Exception:
                canvas_width = 0
        canvas_width = max(420, canvas_width)

        digits = len(str(max(0, int(histogram_count_max or 0))))
        digits = max(2, digits)
        hist_min_px = max(150, 72 + digits * 11)
        hist_fraction = min(0.42, max(0.24, hist_min_px / float(canvas_width)))
        scatter_fraction = max(0.58, 1.0 - hist_fraction)
        hist_fraction = min(hist_fraction, 1.0 - scatter_fraction)
        return (scatter_fraction, hist_fraction)

    def _gallery_histogram_y_tick_settings(self, show_q: bool) -> tuple[int, float]:
        """Choose y-axis tick density/length based on the available histogram height."""
        canvas_height = 0
        if hasattr(self, "gallery_plot_canvas") and self.gallery_plot_canvas is not None:
            try:
                canvas_height = int(self.gallery_plot_canvas.height())
            except Exception:
                canvas_height = 0
        canvas_height = max(300, canvas_height)
        histogram_count = 3 if show_q else 2
        axis_height = max(80.0, (canvas_height - 36.0) / float(histogram_count))
        max_ticks = max(3, min(6, int(axis_height / 26.0)))
        tick_length = max(2.0, min(4.5, axis_height / 42.0))
        return max_ticks, tick_length

    def update_graph_plots(self, measurements):
        """Update analysis graphs from measurement data."""
        if not hasattr(self, "gallery_plot_figure"):
            return

        plot_settings = getattr(self, "gallery_plot_settings", {}) or {}
        bins = int(plot_settings.get("bins", 8))
        show_hist = bool(plot_settings.get("histogram", True))
        plot_style = self._gallery_plot_style(plot_settings)
        show_ci = plot_style == "ellipse"
        show_kde = plot_style == "kde"
        ellipse_coverage_percent = max(50, min(99, int(plot_settings.get("ellipse_coverage_percent", 95) or 95)))
        kde_bandwidth = max(0.5, min(1.5, float(plot_settings.get("kde_bandwidth", 1.0) or 1.0)))
        kde_contours = max(1, min(10, int(plot_settings.get("kde_contours", 6) or 6)))
        kde_coverage_percent = max(50, min(99, int(plot_settings.get("kde_coverage_percent", 95) or 95)))
        mean_comparison = plot_style == "mean"
        show_reference_minmax = bool(plot_settings.get("reference_minmax", True))
        show_reference_ci = plot_style != "mean"
        reference_shape = str(plot_settings.get("reference_shape", "ellipse") or "ellipse").strip().lower()
        if reference_shape not in {"ellipse", "square"}:
            reference_shape = "ellipse"
        show_image_color = bool(plot_settings.get("image_color", False))
        show_legend = bool(plot_settings.get("legend", False))
        show_avg_q = bool(plot_settings.get("avg_q", True))
        show_q_minmax = bool(plot_settings.get("q_minmax", True))
        show_q_extreme_minmax = bool(plot_settings.get("q_extreme_minmax", False))
        axis_equal = bool(plot_settings.get("axis_equal", False))

        all_lengths = []
        all_image_ids = []
        lengths = []
        widths = []
        measurement_ids = []
        measurement_image_ids = []
        for m in measurements or []:
            length = m.get("length_um")
            width = m.get("width_um")
            if length is None:
                continue
            all_lengths.append(float(length))
            all_image_ids.append(m.get("image_id"))
            if width is not None and float(width) > 0:
                lengths.append(float(length))
                widths.append(float(width))
                measurement_ids.append(m.get("id"))
                measurement_image_ids.append(m.get("image_id"))

        histogram_count_max = None
        if all_lengths:
            try:
                length_counts, _ = np.histogram(all_lengths, bins=bins)
                count_max = max(
                    int(length_counts.max()) if len(length_counts) else 0,
                )
                if widths:
                    width_counts, _ = np.histogram(widths, bins=bins)
                    count_max = max(count_max, int(width_counts.max()) if len(width_counts) else 0)
                    q_values = [l / w for l, w in zip(lengths, widths) if w > 0]
                    if q_values:
                        q_counts, _ = np.histogram(q_values, bins=bins)
                        count_max = max(count_max, int(q_counts.max()) if len(q_counts) else 0)
                histogram_count_max = count_max
            except Exception:
                histogram_count_max = None

        self.gallery_plot_figure.clear()
        if show_hist:
            width_ratios = self._gallery_plot_width_ratios(histogram_count_max)
            gs = self.gallery_plot_figure.add_gridspec(
                3, 2, width_ratios=width_ratios, hspace=0.7, wspace=0.30
            )
            ax_scatter = self.gallery_plot_figure.add_subplot(gs[:, 0])
            ax_len = self.gallery_plot_figure.add_subplot(gs[0, 1])
            ax_wid = self.gallery_plot_figure.add_subplot(gs[1, 1])
            ax_q = self.gallery_plot_figure.add_subplot(gs[2, 1])
        else:
            gs = self.gallery_plot_figure.add_gridspec(1, 1)
            ax_scatter = self.gallery_plot_figure.add_subplot(gs[0, 0])
            ax_len = None
            ax_wid = None
            ax_q = None
        self._gallery_scatter_axis = ax_scatter
        self._gallery_hist_axes = {axis for axis in (ax_len, ax_wid, ax_q) if axis is not None}
        self._gallery_hover_hint_key = ""

        stats = self._stats_from_measurements(all_lengths, lengths, widths)

        dark = self._is_dark_theme()
        hist_color_dark = "#4a90d9"   # brighter blue for dark bg
        q_line_style = (0, (3.5, 2.0))
        q_line_width = 1.5 if dark else 1.35
        q_line_alpha = 0.95

        L_all = np.asarray(all_lengths, dtype=float)
        L = np.asarray(lengths, dtype=float)
        W = np.asarray(widths, dtype=float)

        if L_all.size == 0:
            self.gallery_scatter_id_map = {}
            self.gallery_hist_patches = {}
            all_axes = [ax_scatter, ax_len if show_hist else None,
                        ax_wid if show_hist else None, ax_q if show_hist else None]
            ax_scatter.text(0.5, 0.5, "No measurements", ha="center", va="center",
                            color="#e8e8e8" if dark else "#2c3e50")
            ax_scatter.set_axis_off()
            if show_hist and ax_len and ax_wid and ax_q:
                ax_len.set_axis_off()
                ax_wid.set_axis_off()
                ax_q.set_axis_off()
            if dark:
                self._apply_plot_dark_theme(self.gallery_plot_figure, all_axes)
            else:
                self._apply_plot_light_theme(self.gallery_plot_figure, all_axes)
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        message="This figure includes Axes that are not compatible with tight_layout",
                        category=UserWarning,
                    )
                    self.gallery_plot_figure.tight_layout(pad=0.5, rect=(0.02, 0.02, 0.995, 0.995))
                self.gallery_plot_figure.subplots_adjust(
                    left=max(0.075, float(self.gallery_plot_figure.subplotpars.left))
                )
                canvas = getattr(self.gallery_plot_figure, "canvas", None)
                if canvas is not None:
                    canvas.draw()
                    renderer = canvas.get_renderer()
                    tight_bbox = ax_scatter.get_tightbbox(renderer)
                    fig_bbox = self.gallery_plot_figure.bbox
                    if tight_bbox is not None and fig_bbox is not None:
                        clip_px = max(0.0, 6.0 - float(tight_bbox.x0))
                        if clip_px > 0 and float(fig_bbox.width) > 0:
                            extra_left = (clip_px / float(fig_bbox.width)) + 0.005
                            self.gallery_plot_figure.subplots_adjust(
                                left=min(0.28, float(self.gallery_plot_figure.subplotpars.left) + extra_left)
                            )
            except Exception:
                pass
            self.gallery_plot_canvas.draw()
            self._update_gallery_stats_preview()
            return

        category = self.gallery_filter_combo.currentData() if hasattr(self, "gallery_filter_combo") else None
        normalized = self.normalize_measurement_category(category) if category else None
        show_q = normalized in (None, "spores")
        Q = L / W
        specimen_parmasto = self._parmasto_specimen_metrics(L, W)
        category_label = self._format_observation_legend_label()

        def _append_mean_q_label(label, q_value):
            try:
                q_value = float(q_value)
            except (TypeError, ValueError):
                return label
            if q_value <= 0:
                return label
            base_label = str(label or "").strip()
            q_label = f"Qₘ={q_value:.2f}"
            return f"{base_label}, {q_label}" if base_label else q_label

        if show_q and (show_avg_q or mean_comparison) and stats and stats.get("ratio_mean") is not None:
            category_label = _append_mean_q_label(category_label, stats.get("ratio_mean"))

        self.gallery_hist_patches = {}
        self.gallery_scatter = None
        self.gallery_scatter_id_map = {}
        ax_scatter.set_xlabel(self.tr("Length (\u03bcm)"))
        ax_scatter.set_ylabel(self.tr("Width (\u03bcm)"))
        highlighted_ids = self._gallery_highlighted_measurement_ids(measurements)

        image_labels = getattr(self, "gallery_image_labels", {}) or {}
        image_color_map = {}
        hist_color = hist_color_dark if dark else "#3498db"
        specimen_point_alpha = 0.22 if mean_comparison else 0.8
        specimen_point_size = 14 if mean_comparison else 20
        reference_point_alpha = 0.20 if mean_comparison else 0.7
        reference_point_size = 14 if mean_comparison else 18

        if show_image_color:
            grouped = {}
            for length, width, measurement_id, image_id in zip(
                L, W, measurement_ids, measurement_image_ids
            ):
                grouped.setdefault(image_id, {"L": [], "W": [], "ids": []})
                grouped[image_id]["L"].append(length)
                grouped[image_id]["W"].append(width)
                grouped[image_id]["ids"].append(measurement_id)

            ordered_image_ids = list(image_labels.keys())
            ordered_image_ids.extend(
                image_id for image_id in grouped.keys()
                if image_id not in image_labels
            )
            for image_id in ordered_image_ids:
                if image_id not in grouped:
                    continue
                label = image_labels.get(image_id, f"Image {image_id}")
                color = image_color_map.get(image_id) or ax_scatter._get_lines.get_next_color()
                image_color_map[image_id] = color
                data = grouped[image_id]
                collection = ax_scatter.scatter(
                    data["L"],
                    data["W"],
                    s=specimen_point_size,
                    alpha=specimen_point_alpha,
                    picker=5,
                    label=label if show_legend else "_nolegend_",
                    color=color,
                )
                self.gallery_scatter_id_map[collection] = data["ids"]
            if show_legend and category_label:
                ax_scatter.plot([], [], marker="o", color=hist_color, linestyle="", label=category_label)
        else:
            self.gallery_scatter = ax_scatter.scatter(
                L,
                W,
                s=specimen_point_size,
                alpha=specimen_point_alpha,
                picker=5,
                color=hist_color,
                label=category_label,
            )
            self.gallery_scatter_id_map[self.gallery_scatter] = measurement_ids

        def _q_segment_for_box(q_value, x_left, x_right, y_bottom, y_top):
            try:
                q_value = float(q_value)
                x_left = float(x_left)
                x_right = float(x_right)
                y_bottom = float(y_bottom)
                y_top = float(y_top)
            except (TypeError, ValueError):
                return None
            if q_value <= 0:
                return None
            left = min(x_left, x_right)
            right = max(x_left, x_right)
            bottom = min(y_bottom, y_top)
            top = max(y_bottom, y_top)

            start_y = bottom
            end_y = top
            start_x = q_value * start_y
            end_x = q_value * end_y

            if start_x < left:
                start_x = left
                start_y = left / q_value
            if end_x > right:
                end_x = right
                end_y = right / q_value

            if end_x < left - 1e-9 or start_x > right + 1e-9:
                return None
            if start_y < bottom - 1e-9 or start_y > top + 1e-9:
                return None
            if end_y < bottom - 1e-9 or end_y > top + 1e-9:
                return None

            start = (float(start_x), float(start_y))
            end = (float(end_x), float(end_y))
            if abs(start[0] - end[0]) < 1e-9 and abs(start[1] - end[1]) < 1e-9:
                return None
            return start, end

        def _plot_q_guideline(q_value, x_left, x_right, y_bottom, y_top, color, label=None):
            segment = _q_segment_for_box(q_value, x_left, x_right, y_bottom, y_top)
            if segment is None:
                return
            (x_start, y_start), (x_end, y_end) = segment
            ax_scatter.plot(
                [x_start, x_end],
                [y_start, y_end],
                color=color,
                linestyle=q_line_style,
                linewidth=q_line_width,
                alpha=q_line_alpha,
                dash_capstyle="round",
                zorder=2.3,
                label=label if label else "_nolegend_",
            )

        def _plot_data_ellipse(x_values, y_values, color_value, linewidth=1.6):
            if len(x_values) < 3 or len(y_values) < 3:
                return
            ellipse = self._confidence_ellipse_points(
                x_values,
                y_values,
                confidence=float(ellipse_coverage_percent) / 100.0,
            )
            if ellipse is None:
                return
            ex, ey = ellipse
            ax_scatter.plot(
                ex,
                ey,
                color=color_value,
                linewidth=linewidth,
                alpha=0.98,
                zorder=6.05,
            )
            try:
                label_index = int(np.argmax(ey))
                ax_scatter.text(
                    float(ex[label_index]),
                    float(ey[label_index]),
                    f"{ellipse_coverage_percent}%",
                    color=color_value,
                    fontsize=7,
                    alpha=0.78,
                    ha="center",
                    va="bottom",
                    zorder=6.15,
                )
            except Exception:
                pass

        def _rgba_with_alpha(color_value, alpha_value):
            qcolor = QColor(color_value)
            return (
                qcolor.redF(),
                qcolor.greenF(),
                qcolor.blueF(),
                max(0.0, min(1.0, float(alpha_value))),
            )

        def _plot_kde_overlay(x_values, y_values, color_value):
            if not show_kde or len(x_values) < 3 or len(y_values) < 3:
                return
            kde_grid = self._gaussian_kde_grid(x_values, y_values, bw_method=kde_bandwidth)
            if kde_grid is None:
                return
            xx, yy, density, cell_area = kde_grid
            max_density = float(np.nanmax(density))
            if not np.isfinite(max_density):
                return
            density = density / max_density
            contour_info = self._kde_contour_levels(
                density,
                cell_area,
                contour_count=kde_contours,
                coverage_percent=kde_coverage_percent,
            )
            if not contour_info:
                return
            kde_levels, contour_label_map = contour_info
            fill_levels = list(kde_levels)
            top_level = 1.0 + 1e-6
            if top_level <= fill_levels[-1]:
                top_level = fill_levels[-1] + max(abs(fill_levels[-1]) * 1e-6, 1e-12)
            fill_levels.append(top_level)
            fill_alphas = np.linspace(
                0.06 if mean_comparison else 0.08,
                0.16 if mean_comparison else 0.22,
                num=max(1, len(fill_levels) - 1),
            )
            fill_colors = [_rgba_with_alpha(color_value, alpha) for alpha in fill_alphas]
            ax_scatter.contourf(
                xx,
                yy,
                density,
                levels=fill_levels,
                colors=fill_colors,
                zorder=1.7,
            )
            contour_widths = np.linspace(0.8, 1.15, num=len(kde_levels))
            contour_alpha = 0.45 if mean_comparison else 0.55
            contour_set = ax_scatter.contour(
                xx,
                yy,
                density,
                levels=kde_levels,
                colors=[color_value],
                linewidths=contour_widths.tolist(),
                alpha=contour_alpha,
                zorder=2.02,
            )
            for collection in getattr(contour_set, "collections", []):
                collection.set_alpha(contour_alpha)
            try:
                contour_labels = ax_scatter.clabel(
                    contour_set,
                    contour_set.levels,
                    fmt=contour_label_map,
                    inline=True,
                    inline_spacing=3,
                    fontsize=7,
                    colors=[color_value],
                )
                for text in contour_labels:
                    text.set_alpha(max(0.72, contour_alpha))
            except Exception:
                pass

        if highlighted_ids:
            highlight_x = []
            highlight_y = []
            selected_x = []
            selected_y = []
            selected_measurement_id = int(self.gallery_selected_measurement_id or 0)
            for length, width, measurement_id in zip(L, W, measurement_ids):
                if measurement_id in highlighted_ids:
                    highlight_x.append(length)
                    highlight_y.append(width)
                if selected_measurement_id and measurement_id == selected_measurement_id:
                    selected_x.append(length)
                    selected_y.append(width)
            if highlight_x:
                highlight_edge = "#fff27a" if dark else "#2b124c"
                ax_scatter.scatter(
                    highlight_x,
                    highlight_y,
                    s=74,
                    marker="s",
                    facecolors="none",
                    edgecolors=highlight_edge,
                    alpha=0.95,
                    linewidths=1.5,
                    zorder=6,
                )
            if selected_x and selected_y and self.gallery_filter_mode in {"bin", "points"}:
                ax_scatter.scatter(
                    selected_x,
                    selected_y,
                    s=92,
                    marker="s",
                    facecolors="none",
                    edgecolors="#d93025",
                    alpha=0.98,
                    linewidths=1.2,
                    zorder=7,
                )

        max_len = float(np.max(L)) if L.size > 0 else float(np.max(L_all))
        min_len = float(np.min(L)) if L.size > 0 else float(np.min(L_all))
        min_w = float(np.min(W)) if W.size > 0 else 0.0
        max_w = float(np.max(W)) if W.size > 0 else 0.0
        own_l_p05 = float(stats.get("length_p5")) if stats and stats.get("length_p5") is not None else min_len
        own_l_p95 = float(stats.get("length_p95")) if stats and stats.get("length_p95") is not None else max_len
        own_w_p05 = float(stats.get("width_p5")) if stats and stats.get("width_p5") is not None else min_w
        own_w_p95 = float(stats.get("width_p95")) if stats and stats.get("width_p95") is not None else max_w
        if show_q and (show_avg_q or mean_comparison):
            avg_q = float(stats.get("ratio_mean", np.mean(Q)))
            _plot_q_guideline(
                avg_q,
                own_l_p05,
                own_l_p95,
                own_w_p05,
                own_w_p95,
                hist_color,
            )

        if show_q and show_q_minmax and not mean_comparison:
            q_min = float(stats.get("ratio_p5", stats.get("ratio_min", np.min(Q))))
            q_max = float(stats.get("ratio_p95", stats.get("ratio_max", np.max(Q))))
            _plot_q_guideline(
                q_min,
                own_l_p05,
                own_l_p95,
                own_w_p05,
                own_w_p95,
                hist_color,
            )
            _plot_q_guideline(
                q_max,
                own_l_p05,
                own_l_p95,
                own_w_p05,
                own_w_p95,
                hist_color,
            )

        if show_q and show_q_extreme_minmax and not mean_comparison:
            q_extreme_min = float(stats.get("ratio_min", np.min(Q)))
            q_extreme_max = float(stats.get("ratio_max", np.max(Q)))
            _plot_q_guideline(
                q_extreme_min,
                min_len,
                max_len,
                min_w,
                max_w,
                hist_color,
                label=f"Q min/max={q_extreme_min:.1f}-{q_extreme_max:.1f}",
            )
            _plot_q_guideline(
                q_extreme_max,
                min_len,
                max_len,
                min_w,
                max_w,
                hist_color,
            )

        if show_kde and len(L) >= 3:
            _plot_kde_overlay(L, W, hist_color)

        if show_ci and len(L) >= 3 and not mean_comparison:
            _plot_data_ellipse(L, W, hist_color, linewidth=1.8)

        if mean_comparison and specimen_parmasto:
            ax_scatter.scatter(
                [specimen_parmasto["L_m"]],
                [specimen_parmasto["W_m"]],
                marker="+",
                s=260,
                color=hist_color,
                linewidths=2.4,
                zorder=6.2,
            )

        x_min = plot_settings.get("x_min")
        x_max = plot_settings.get("x_max")
        y_min = plot_settings.get("y_min")
        y_max = plot_settings.get("y_max")
        if x_min is not None or x_max is not None:
            ax_scatter.set_xlim(left=x_min, right=x_max)
        if y_min is not None or y_max is not None:
            ax_scatter.set_ylim(bottom=y_min, top=y_max)
        if axis_equal:
            ax_scatter.set_aspect("equal", adjustable="box")
        else:
            ax_scatter.set_aspect("auto")

        reference_series = self._resolved_reference_series_entries(dark)

        def _fallback(low, mid_low, mid_high, high):
            left = low if low is not None else mid_low
            right = high if high is not None else mid_high
            if left is None and right is not None:
                left = right
            if right is None and left is not None:
                right = left
            return left, right

        def _clip_polygon_to_q_constraint(points, q_value, keep_greater):
            try:
                q_value = float(q_value)
            except (TypeError, ValueError):
                return points
            if q_value <= 0 or not points:
                return points

            def _metric(point):
                return float(point[0]) - q_value * float(point[1])

            def _inside(point):
                value = _metric(point)
                return value >= -1e-9 if keep_greater else value <= 1e-9

            clipped = []
            previous = points[-1]
            previous_inside = _inside(previous)
            previous_metric = _metric(previous)
            for current in points:
                current_inside = _inside(current)
                current_metric = _metric(current)
                if current_inside != previous_inside:
                    denominator = previous_metric - current_metric
                    if abs(denominator) > 1e-12:
                        t = previous_metric / denominator
                        intersection = (
                            float(previous[0]) + (float(current[0]) - float(previous[0])) * t,
                            float(previous[1]) + (float(current[1]) - float(previous[1])) * t,
                        )
                        clipped.append(intersection)
                if current_inside:
                    clipped.append((float(current[0]), float(current[1])))
                previous = current
                previous_inside = current_inside
                previous_metric = current_metric
            return clipped

        def _constrained_box_polygon(x_left, x_right, y_bottom, y_top, q_low=None, q_high=None):
            if x_left is None or x_right is None or y_bottom is None or y_top is None:
                return None
            left = float(min(x_left, x_right))
            right = float(max(x_left, x_right))
            bottom = float(min(y_bottom, y_top))
            top = float(max(y_bottom, y_top))
            if right <= left or top <= bottom:
                return None
            polygon = [
                (left, bottom),
                (right, bottom),
                (right, top),
                (left, top),
            ]
            polygon = _clip_polygon_to_q_constraint(polygon, q_low, keep_greater=True)
            polygon = _clip_polygon_to_q_constraint(polygon, q_high, keep_greater=False)
            if len(polygon) < 3:
                return None
            return polygon

        def _plot_reference_range_shape(x_left, x_right, y_bottom, y_top, edge_color, linestyle, q_low=None, q_high=None):
            if x_left is None or x_right is None or y_bottom is None or y_top is None:
                return
            if reference_shape == "square":
                polygon = _constrained_box_polygon(x_left, x_right, y_bottom, y_top, q_low=q_low, q_high=q_high)
                if not polygon:
                    return
                xs = [point[0] for point in polygon] + [polygon[0][0]]
                ys = [point[1] for point in polygon] + [polygon[0][1]]
                ax_scatter.plot(xs, ys, color=edge_color, linewidth=1.5, linestyle=linestyle)
                return
            width = abs(x_right - x_left)
            height = abs(y_top - y_bottom)
            if width <= 0 or height <= 0:
                return
            center = ((x_left + x_right) / 2.0, (y_bottom + y_top) / 2.0)
            ellipse = Ellipse(
                center,
                width=width,
                height=height,
                fill=False,
                edgecolor=edge_color,
                linewidth=1.5,
                linestyle=linestyle,
            )
            ax_scatter.add_patch(ellipse)

        def _plot_reference_mean_shape(
            x_left,
            x_right,
            y_bottom,
            y_top,
            color,
            mean_q=None,
            mean_x=None,
            mean_y=None,
            center_q=None,
            fill_alpha=0.10,
            show_center_marker=True,
        ):
            polygon = _constrained_box_polygon(
                x_left,
                x_right,
                y_bottom,
                y_top,
                q_low=mean_q[0] if isinstance(mean_q, tuple) else None,
                q_high=mean_q[1] if isinstance(mean_q, tuple) else None,
            )
            if polygon:
                xs = [point[0] for point in polygon] + [polygon[0][0]]
                ys = [point[1] for point in polygon] + [polygon[0][1]]
                ax_scatter.fill(xs, ys, color=color, alpha=fill_alpha, zorder=1.1)
                ax_scatter.plot(xs, ys, color=color, linewidth=1.25, alpha=0.9, zorder=1.8)
            if show_center_marker and mean_x is not None and mean_y is not None:
                ax_scatter.scatter(
                    [mean_x],
                    [mean_y],
                    marker="+",
                    s=250,
                    color=color,
                    linewidths=2.2,
                    zorder=6,
                )
            if show_q:
                try:
                    center_q = float(center_q) if center_q is not None else None
                except (TypeError, ValueError):
                    center_q = None
                if center_q is None:
                    try:
                        center_q = float(mean_x) / float(mean_y) if mean_x and mean_y else None
                    except (TypeError, ValueError, ZeroDivisionError):
                        center_q = None
                if center_q is not None:
                    _plot_q_guideline(center_q, x_left, x_right, y_bottom, y_top, color)

        reference_hist_overlays = []
        reference_point_hist = []
        parmasto_reference_present = False

        for entry in reference_series:
            if not bool(entry.get("enabled", True)):
                continue
            data = entry.get("data", {})
            if not isinstance(data, dict) or not data:
                continue
            kind = data.get("source_kind") or ("points" if data.get("points") else "reference")
            if kind == "points" and not self._reference_allow_points():
                continue
            if kind == "observation" and not (data.get("points") or data.get("observation_id")):
                continue
            color = str(entry.get("color") or "#adb5bd")
            label = entry.get("label") or self._format_reference_series_label(data)

            if kind == "observation":
                points = data.get("points") or []
                ref_L = np.array([p.get("length_um") for p in points if p.get("length_um") is not None], dtype=float)
                ref_W = np.array([p.get("width_um") for p in points if p.get("width_um") is not None], dtype=float)
                if ref_L.size and ref_W.size:
                    ref_Q = ref_L / ref_W
                    _plot_kde_overlay(ref_L, ref_W, color)
                    if show_q and (show_avg_q or mean_comparison):
                        label = _append_mean_q_label(label, np.mean(ref_Q))
                    if mean_comparison:
                        ax_scatter.scatter(
                            ref_L,
                            ref_W,
                            s=reference_point_size,
                            alpha=reference_point_alpha,
                            facecolors="none",
                            edgecolors=color,
                            linewidths=0.9,
                            label=label,
                        )
                        ref_Q = ref_L / ref_W
                        _plot_reference_mean_shape(
                            float(np.percentile(ref_L, 5)),
                            float(np.percentile(ref_L, 95)),
                            float(np.percentile(ref_W, 5)),
                            float(np.percentile(ref_W, 95)),
                            color,
                            mean_q=(
                                float(np.percentile(ref_Q, 5)),
                                float(np.percentile(ref_Q, 95)),
                            ),
                            mean_x=float(np.mean(ref_L)),
                            mean_y=float(np.mean(ref_W)),
                            center_q=float(np.mean(ref_Q)),
                            fill_alpha=0.07,
                        )
                    else:
                        ax_scatter.scatter(
                            ref_L, ref_W, s=22, alpha=0.85,
                            facecolors="none", edgecolors=color, linewidths=1.2,
                            label=label,
                        )
                        if show_ci and ref_L.size >= 3:
                            _plot_data_ellipse(ref_L, ref_W, color, linewidth=1.6)
                        if show_q and show_avg_q and ref_Q.size:
                            avg_q = float(np.mean(ref_Q))
                            l_p05 = float(np.percentile(ref_L, 5))
                            l_p95 = float(np.percentile(ref_L, 95))
                            w_p05 = float(np.percentile(ref_W, 5))
                            w_p95 = float(np.percentile(ref_W, 95))
                            _plot_q_guideline(avg_q, l_p05, l_p95, w_p05, w_p95, color)
                        if show_q and show_q_minmax and ref_Q.size:
                            l_p05 = float(np.percentile(ref_L, 5))
                            l_p95 = float(np.percentile(ref_L, 95))
                            w_p05 = float(np.percentile(ref_W, 5))
                            w_p95 = float(np.percentile(ref_W, 95))
                            _plot_q_guideline(float(np.percentile(ref_Q, 5)), l_p05, l_p95, w_p05, w_p95, color)
                            _plot_q_guideline(float(np.percentile(ref_Q, 95)), l_p05, l_p95, w_p05, w_p95, color)
                        if show_q and show_q_extreme_minmax and ref_Q.size:
                            _plot_q_guideline(float(np.min(ref_Q)), float(np.min(ref_L)), float(np.max(ref_L)), float(np.min(ref_W)), float(np.max(ref_W)), color)
                            _plot_q_guideline(float(np.max(ref_Q)), float(np.min(ref_L)), float(np.max(ref_L)), float(np.min(ref_W)), float(np.max(ref_W)), color)
                    if show_hist:
                        reference_point_hist.append({
                            "L": ref_L,
                            "W": ref_W,
                            "Q": ref_Q if show_q else None,
                            "color": color,
                        })
                continue

            if kind == "points":
                points = data.get("points") or []
                ref_L = np.array([p.get("length_um") for p in points if p.get("length_um") is not None], dtype=float)
                ref_W = np.array([p.get("width_um") for p in points if p.get("width_um") is not None], dtype=float)
                if ref_L.size and ref_W.size:
                    ref_Q = ref_L / ref_W
                    _plot_kde_overlay(ref_L, ref_W, color)
                    if mean_comparison:
                        if show_q and ref_Q.size:
                            label = _append_mean_q_label(label, np.mean(ref_Q))
                        ax_scatter.scatter(
                            ref_L,
                            ref_W,
                            s=reference_point_size,
                            alpha=reference_point_alpha,
                            color=color,
                            label=label,
                        )
                        _plot_reference_mean_shape(
                            float(np.percentile(ref_L, 5)),
                            float(np.percentile(ref_L, 95)),
                            float(np.percentile(ref_W, 5)),
                            float(np.percentile(ref_W, 95)),
                            color,
                            mean_q=(
                                float(np.percentile(ref_Q, 5)),
                                float(np.percentile(ref_Q, 95)),
                            ) if ref_Q.size else None,
                            mean_x=float(np.mean(ref_L)),
                            mean_y=float(np.mean(ref_W)),
                            center_q=float(np.mean(ref_Q)) if ref_Q.size else None,
                            fill_alpha=0.07,
                        )
                    else:
                        ax_scatter.scatter(ref_L, ref_W, s=18, alpha=0.7, color=color, label=label)
                        if show_ci and ref_L.size >= 3:
                            _plot_data_ellipse(ref_L, ref_W, color, linewidth=1.6)
                        if show_q and show_avg_q and ref_Q.size:
                            l_p05 = float(np.percentile(ref_L, 5))
                            l_p95 = float(np.percentile(ref_L, 95))
                            w_p05 = float(np.percentile(ref_W, 5))
                            w_p95 = float(np.percentile(ref_W, 95))
                            _plot_q_guideline(float(np.mean(ref_Q)), l_p05, l_p95, w_p05, w_p95, color)
                        if show_q and show_q_minmax and ref_Q.size:
                            l_p05 = float(np.percentile(ref_L, 5))
                            l_p95 = float(np.percentile(ref_L, 95))
                            w_p05 = float(np.percentile(ref_W, 5))
                            w_p95 = float(np.percentile(ref_W, 95))
                            _plot_q_guideline(float(np.percentile(ref_Q, 5)), l_p05, l_p95, w_p05, w_p95, color)
                            _plot_q_guideline(float(np.percentile(ref_Q, 95)), l_p05, l_p95, w_p05, w_p95, color)
                        if show_q and show_q_extreme_minmax and ref_Q.size:
                            _plot_q_guideline(float(np.min(ref_Q)), float(np.min(ref_L)), float(np.max(ref_L)), float(np.min(ref_W)), float(np.max(ref_W)), color)
                            _plot_q_guideline(float(np.max(ref_Q)), float(np.min(ref_L)), float(np.max(ref_L)), float(np.min(ref_W)), float(np.max(ref_W)), color)
                    if show_hist:
                        reference_point_hist.append({
                            "L": ref_L,
                            "W": ref_W,
                            "Q": ref_Q if show_q else None,
                            "color": color,
                        })
                continue

            ref_l_min = data.get("length_min")
            ref_l_max = data.get("length_max")
            ref_l_avg = data.get("length_p50")
            ref_l_p05 = data.get("length_p05")
            ref_l_p50 = data.get("length_p50")
            ref_l_p95 = data.get("length_p95")
            ref_w_min = data.get("width_min")
            ref_w_max = data.get("width_max")
            ref_w_avg = data.get("width_p50")
            ref_w_p05 = data.get("width_p05")
            ref_w_p50 = data.get("width_p50")
            ref_w_p95 = data.get("width_p95")
            ref_q_min = data.get("q_p05")
            if ref_q_min is None:
                ref_q_min = data.get("q_p5")
            if ref_q_min is None:
                ref_q_min = data.get("q_min")
            ref_q_max = data.get("q_p95")
            if ref_q_max is None:
                ref_q_max = data.get("q_max")
            ref_q_avg = data.get("q_p50")
            if show_q and (show_avg_q or mean_comparison) and ref_q_avg is not None:
                label = _append_mean_q_label(label, ref_q_avg)
            parmasto_ref = self._parmasto_reference_metrics(data)
            if parmasto_ref:
                parmasto_reference_present = True

            l_left, l_right = _fallback(ref_l_min, ref_l_p05, ref_l_p95, ref_l_max)
            w_bottom, w_top = _fallback(ref_w_min, ref_w_p05, ref_w_p95, ref_w_max)

            any_reference = any(
                value is not None
                for value in (
                    l_left, l_right, w_bottom, w_top,
                    ref_l_p05, ref_l_p95, ref_w_p05, ref_w_p95,
                    parmasto_ref.get("L_bar") if parmasto_ref else None,
                    parmasto_ref.get("W_bar") if parmasto_ref else None,
                )
            )

            def _validated_q_pair(low_value, high_value):
                try:
                    low = float(low_value) if low_value is not None else None
                    high = float(high_value) if high_value is not None else None
                except (TypeError, ValueError):
                    return (None, None)
                if low is None or high is None or low >= high:
                    return (None, None)
                return (low, high)

            q_range_low, q_range_high = _validated_q_pair(ref_q_min, ref_q_max)
            q_extreme_low, q_extreme_high = _validated_q_pair(data.get("q_min"), data.get("q_max"))
            range_shape_q_low = q_range_low if q_range_low is not None else q_extreme_low
            range_shape_q_high = q_range_high if q_range_high is not None else q_extreme_high
            minmax_shape_q_low = q_extreme_low if q_extreme_low is not None else q_range_low
            minmax_shape_q_high = q_extreme_high if q_extreme_high is not None else q_range_high

            if (
                not mean_comparison
                and show_reference_minmax
                and l_left is not None and l_right is not None and w_bottom is not None and w_top is not None
            ):
                _plot_reference_range_shape(
                    l_left,
                    l_right,
                    w_bottom,
                    w_top,
                    color,
                    ":",
                    q_low=minmax_shape_q_low,
                    q_high=minmax_shape_q_high,
                )
            if (
                not mean_comparison
                and show_reference_ci
                and ref_l_p05 is not None and ref_l_p95 is not None and ref_w_p05 is not None and ref_w_p95 is not None
            ):
                _plot_reference_range_shape(
                    ref_l_p05,
                    ref_l_p95,
                    ref_w_p05,
                    ref_w_p95,
                    color,
                    "-",
                    q_low=range_shape_q_low,
                    q_high=range_shape_q_high,
                )

            if any_reference:
                legend_style = "-" if show_reference_ci else ":"
                ax_scatter.plot([], [], color=color, linestyle=legend_style, label=label)

            if mean_comparison:
                mean_x = parmasto_ref.get("L_bar") if parmasto_ref and parmasto_ref.get("L_bar") is not None else ref_l_avg
                mean_y = parmasto_ref.get("W_bar") if parmasto_ref and parmasto_ref.get("W_bar") is not None else ref_w_avg
                mean_q_range = None
                x_left = ref_l_p05 if ref_l_p05 is not None else l_left
                x_right = ref_l_p95 if ref_l_p95 is not None else l_right
                y_bottom = ref_w_p05 if ref_w_p05 is not None else w_bottom
                y_top = ref_w_p95 if ref_w_p95 is not None else w_top
                if parmasto_ref:
                    parmasto_l_range = self._parmasto_expected_range(
                        parmasto_ref.get("L_bar"),
                        parmasto_ref.get("V_spL"),
                    )
                    parmasto_w_range = self._parmasto_expected_range(
                        parmasto_ref.get("W_bar"),
                        parmasto_ref.get("V_spW"),
                    )
                    parmasto_q_range = self._parmasto_expected_range(
                        parmasto_ref.get("Q_bar"),
                        parmasto_ref.get("V_spQ"),
                    )
                    if parmasto_l_range:
                        x_left, x_right = parmasto_l_range
                    if parmasto_w_range:
                        y_bottom, y_top = parmasto_w_range
                    mean_q_range = parmasto_q_range
                elif ref_q_min is not None and ref_q_max is not None:
                    try:
                        mean_q_range = (float(ref_q_min), float(ref_q_max))
                    except (TypeError, ValueError):
                        mean_q_range = None
                _plot_reference_mean_shape(
                    x_left,
                    x_right,
                    y_bottom,
                    y_top,
                    color,
                    mean_q=mean_q_range,
                    mean_x=mean_x,
                    mean_y=mean_y,
                    center_q=parmasto_ref.get("Q_bar") if parmasto_ref and parmasto_ref.get("Q_bar") is not None else ref_q_avg,
                    fill_alpha=0.10,
                    show_center_marker=not bool(parmasto_ref),
                )

            mean_x = parmasto_ref.get("L_bar") if parmasto_ref and parmasto_ref.get("L_bar") is not None else ref_l_avg
            mean_y = parmasto_ref.get("W_bar") if parmasto_ref and parmasto_ref.get("W_bar") is not None else ref_w_avg
            if not mean_comparison and not parmasto_ref and mean_x is not None and mean_y is not None:
                ax_scatter.scatter(
                    [mean_x],
                    [mean_y],
                    marker="+",
                    s=260,
                    color=color,
                    linewidths=2.2,
                    zorder=6,
                )

            if show_q:
                ref_q_l_left = ref_l_p05 if ref_l_p05 is not None else l_left
                ref_q_l_right = ref_l_p95 if ref_l_p95 is not None else l_right
                ref_q_w_bottom = ref_w_p05 if ref_w_p05 is not None else w_bottom
                ref_q_w_top = ref_w_p95 if ref_w_p95 is not None else w_top
                if (show_avg_q or mean_comparison) and ref_q_avg is not None and ref_q_avg > 0 and not mean_comparison:
                    _plot_q_guideline(
                        ref_q_avg,
                        ref_q_l_left,
                        ref_q_l_right,
                        ref_q_w_bottom,
                        ref_q_w_top,
                        color,
                    )

            reference_hist_overlays.append({
                "color": color,
                "length_min": ref_l_min,
                "length_max": ref_l_max,
                "length_p05": ref_l_p05,
                "length_p95": ref_l_p95,
                "width_min": ref_w_min,
                "width_max": ref_w_max,
                "width_p05": ref_w_p05,
                "width_p95": ref_w_p95,
            })
        if parmasto_reference_present and specimen_parmasto and not mean_comparison:
            specimen_lm = specimen_parmasto.get("L_m")
            specimen_wm = specimen_parmasto.get("W_m")
            if specimen_lm is not None and specimen_wm is not None:
                ax_scatter.scatter(
                    [specimen_lm],
                    [specimen_wm],
                    marker="+",
                    s=300,
                    color=hist_color,
                    linewidths=2.4,
                    zorder=6,
                )
        handles, labels = ax_scatter.get_legend_handles_labels()
        if labels:
            ordered = sorted(
                zip(labels, handles),
                key=lambda item: str(item[0]).casefold(),
            )
            ax_scatter.legend(
                [handle for _, handle in ordered],
                [label for label, _ in ordered],
                loc="best",
                fontsize=8,
            )

        if show_hist:
            hist_tick_max, hist_tick_length = self._gallery_histogram_y_tick_settings(show_q)
            l_bins = np.histogram_bin_edges(L, bins=bins)
            w_bins = np.histogram_bin_edges(W, bins=bins)
            q_bins = np.histogram_bin_edges(Q, bins=bins) if show_q else None

            if show_legend and image_labels:
                grouped = {}
                for length, width, measurement_id, image_id in zip(
                    L, W, measurement_ids, measurement_image_ids
                ):
                    grouped.setdefault(image_id, {"L": [], "W": [], "Q": [], "L_all": []})
                    grouped[image_id]["L"].append(length)
                    grouped[image_id]["W"].append(width)
                if show_q:
                    for length, width, image_id in zip(L, W, measurement_image_ids):
                        grouped.setdefault(image_id, {"L": [], "W": [], "Q": [], "L_all": []})
                        grouped[image_id]["Q"].append(length / width)
                
                for length, image_id in zip(L_all, all_image_ids):
                    grouped.setdefault(image_id, {"L": [], "W": [], "Q": [], "L_all": []})
                    grouped[image_id]["L_all"].append(length)

                for image_id in image_labels.keys():
                    if image_id not in grouped:
                        continue
                    color = image_color_map.get(image_id) or ax_scatter._get_lines.get_next_color()
                    image_color_map[image_id] = color
                    data = grouped[image_id]
                    if data["L_all"]:
                        _, l_bins, l_patches = ax_len.hist(data["L_all"], bins=l_bins, color=color, alpha=0.35)
                        for i, patch in enumerate(l_patches):
                            patch.set_picker(True)
                            self.gallery_hist_patches[patch] = ("L", l_bins[i], l_bins[i + 1])
                    if data["W"]:
                        _, w_bins, w_patches = ax_wid.hist(data["W"], bins=w_bins, color=color, alpha=0.35)
                        for i, patch in enumerate(w_patches):
                            patch.set_picker(True)
                            self.gallery_hist_patches[patch] = ("W", w_bins[i], w_bins[i + 1])
                    if show_q:
                        _, q_bins, q_patches = ax_q.hist(data["Q"], bins=q_bins, color=color, alpha=0.35)
                        for i, patch in enumerate(q_patches):
                            patch.set_picker(True)
                            self.gallery_hist_patches[patch] = ("Q", q_bins[i], q_bins[i + 1])
            else:
                _, l_bins, l_patches = ax_len.hist(L_all, bins=l_bins, color=hist_color)
                ax_len.set_ylabel("Count")
                for i, patch in enumerate(l_patches):
                    patch.set_picker(True)
                    self.gallery_hist_patches[patch] = ("L", l_bins[i], l_bins[i + 1])

                _, w_bins, w_patches = ax_wid.hist(W, bins=w_bins, color=hist_color)
                for i, patch in enumerate(w_patches):
                    patch.set_picker(True)
                    self.gallery_hist_patches[patch] = ("W", w_bins[i], w_bins[i + 1])

                if show_q:
                    _, q_bins, q_patches = ax_q.hist(Q, bins=q_bins, color=hist_color)
                    for i, patch in enumerate(q_patches):
                        patch.set_picker(True)
                        self.gallery_hist_patches[patch] = ("Q", q_bins[i], q_bins[i + 1])
            ax_len.set_ylabel("Count")
            ax_wid.set_ylabel("Count")
            ax_len.yaxis.set_major_locator(MaxNLocator(integer=True, nbins=hist_tick_max, min_n_ticks=3))
            ax_wid.yaxis.set_major_locator(MaxNLocator(integer=True, nbins=hist_tick_max, min_n_ticks=3))
            ax_len.tick_params(axis="y", length=hist_tick_length)
            ax_wid.tick_params(axis="y", length=hist_tick_length)
            if show_q:
                ax_q.set_ylabel("Count")
                ax_q.yaxis.set_major_locator(MaxNLocator(integer=True, nbins=hist_tick_max, min_n_ticks=3))
                ax_q.tick_params(axis="y", length=hist_tick_length)
            for entry in reference_point_hist:
                ref_L = entry.get("L")
                ref_W = entry.get("W")
                ref_Q = entry.get("Q") if show_q else None
                color = entry.get("color") or "#95a5a6"
                if ref_L is not None and len(ref_L):
                    ax_len.hist(ref_L, bins=l_bins, color=color, alpha=0.25)
                if ref_W is not None and len(ref_W):
                    ax_wid.hist(ref_W, bins=w_bins, color=color, alpha=0.25)
                if show_q and ref_Q is not None and len(ref_Q):
                    ax_q.hist(ref_Q, bins=q_bins, color=color, alpha=0.25)
            for overlay in reference_hist_overlays:
                color = overlay.get("color") or "#2c3e50"
                if overlay.get("length_p05") is not None:
                    ax_len.axvline(overlay["length_p05"], color=color, linestyle="--", linewidth=1.2)
                if overlay.get("length_p95") is not None:
                    ax_len.axvline(overlay["length_p95"], color=color, linestyle="--", linewidth=1.2)
                if overlay.get("width_p05") is not None:
                    ax_wid.axvline(overlay["width_p05"], color=color, linestyle="--", linewidth=1.2)
                if overlay.get("width_p95") is not None:
                    ax_wid.axvline(overlay["width_p95"], color=color, linestyle="--", linewidth=1.2)
                if overlay.get("length_min") is not None:
                    ax_len.axvline(overlay["length_min"], color=color, linestyle=":", linewidth=1.0)
                if overlay.get("length_max") is not None:
                    ax_len.axvline(overlay["length_max"], color=color, linestyle=":", linewidth=1.0)
                if overlay.get("width_min") is not None:
                    ax_wid.axvline(overlay["width_min"], color=color, linestyle=":", linewidth=1.0)
                if overlay.get("width_max") is not None:
                    ax_wid.axvline(overlay["width_max"], color=color, linestyle=":", linewidth=1.0)
            ax_len.set_xlabel(self.tr("Length (\u03bcm)"))
            ax_len.set_ylabel("Count")
            ax_wid.set_xlabel(self.tr("Width (\u03bcm)"))
            if show_q:
                ax_q.set_xlabel("Q (L/W)")
            else:
                ax_q.set_axis_off()
        else:
            if ax_len:
                ax_len.set_axis_off()
            if ax_wid:
                ax_wid.set_axis_off()
            if ax_q:
                ax_q.set_axis_off()

        all_axes = [ax_scatter, ax_len, ax_wid, ax_q]
        if dark:
            self._apply_plot_dark_theme(self.gallery_plot_figure, all_axes)
        else:
            self._apply_plot_light_theme(self.gallery_plot_figure, all_axes)
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message="This figure includes Axes that are not compatible with tight_layout",
                    category=UserWarning,
                )
                self.gallery_plot_figure.tight_layout(pad=0.5, rect=(0.02, 0.02, 0.995, 0.995))
            self.gallery_plot_figure.subplots_adjust(
                left=max(0.075, float(self.gallery_plot_figure.subplotpars.left))
            )
            canvas = getattr(self.gallery_plot_figure, "canvas", None)
            if canvas is not None:
                canvas.draw()
                renderer = canvas.get_renderer()
                tight_bbox = ax_scatter.get_tightbbox(renderer)
                fig_bbox = self.gallery_plot_figure.bbox
                if tight_bbox is not None and fig_bbox is not None:
                    clip_px = max(0.0, 6.0 - float(tight_bbox.x0))
                    if clip_px > 0 and float(fig_bbox.width) > 0:
                        extra_left = (clip_px / float(fig_bbox.width)) + 0.005
                        self.gallery_plot_figure.subplots_adjust(
                            left=min(0.28, float(self.gallery_plot_figure.subplotpars.left) + extra_left)
                        )
        except Exception:
            pass
        self.gallery_plot_canvas.draw()
        self._update_gallery_stats_preview()

    def export_graph_plot_svg(self):
        """Export analysis graphs to SVG, PNG, or JPEG."""
        if not hasattr(self, "gallery_plot_figure"):
            return

        dark = self._is_dark_theme()
        dialog = ExportPlotDialog(current_dark=dark, parent=self)
        if dialog.exec() != QDialog.Accepted:
            return
        settings = dialog.get_settings()
        export_format = settings["format"]
        export_theme = settings["theme"]   # "light" or "dark"
        export_quality = settings["quality"]

        default_name = "spore_plot"
        if self.active_observation_id:
            obs = ObservationDB.get_observation(self.active_observation_id)
            if obs:
                parts = [
                    obs.get("genus") or "",
                    obs.get("species") or obs.get("species_guess") or "",
                    obs.get("date") or ""
                ]
                name = " ".join([p for p in parts if p]).strip()
                name = name.replace(":", "-")
                name = re.sub(r'[<>:"/\\\\|?*]', "_", name)
                name = re.sub(r"\s+", " ", name).strip()
                if name:
                    default_name = f"{name} - plot"

        ext_map = {"svg": ".svg", "png": ".png", "jpg": ".jpg"}
        default_ext = ext_map.get(export_format, ".svg")
        filter_map = {
            "svg": "SVG Files (*.svg)",
            "png": "PNG Images (*.png)",
            "jpg": "JPEG Images (*.jpg)",
        }
        default_path = str(Path(self._get_default_export_dir()) / f"{default_name}{default_ext}")
        filename, _ = QFileDialog.getSaveFileName(
            self,
            "Export Plot",
            default_path,
            f"{filter_map.get(export_format, 'SVG Files (*.svg)')};;All Files (*)"
        )
        if not filename:
            return
        if not re.search(r"\.(svg|png|jpe?g)$", filename, re.IGNORECASE):
            filename += default_ext
        self._remember_export_dir(filename)

        fig = self.gallery_plot_figure
        axes = fig.get_axes()
        needs_restore = (export_theme == "dark") != dark
        try:
            if export_theme == "dark" and not dark:
                self._apply_plot_dark_theme(fig, axes)
            elif export_theme == "light" and dark:
                self._apply_plot_light_theme(fig, axes)

            fmt = "jpeg" if export_format == "jpg" else export_format
            canvas = getattr(fig, "canvas", None)
            if canvas is not None:
                canvas.draw()

            save_kwargs = {
                "format": fmt,
                "facecolor": fig.get_facecolor(),
                "edgecolor": "none",
                "transparent": False,
            }
            if export_format == "svg":
                save_kwargs["bbox_inches"] = "tight"
                save_kwargs["pad_inches"] = 0.04
            if export_format == "jpg":
                save_kwargs["pil_kwargs"] = {"quality": export_quality}
            fig.savefig(filename, **save_kwargs)
            self.measure_status_label.setText(f"\u2713 Plot exported to {Path(filename).name}")
            self.measure_status_label.setStyleSheet(f"color: #27ae60; font-weight: bold; font-size: {pt(9)}pt;")
        except Exception as exc:
            QMessageBox.warning(self, "Export Failed", str(exc))
        finally:
            if needs_restore:
                if dark:
                    self._apply_plot_dark_theme(fig, axes)
                else:
                    self._apply_plot_light_theme(fig, axes)
                canvas = getattr(fig, "canvas", None)
                if canvas:
                    canvas.draw_idle()

    def export_publish_measure_plot_png(self, observation_id: int, out_path: Path | str) -> bool:
        """Render and export a publish PNG using the same analysis plot pipeline."""
        if not observation_id or not hasattr(self, "gallery_plot_figure"):
            return False
        target_path = Path(out_path)
        previous_observation_id = self.active_observation_id
        previous_observation_name = self.active_observation_name
        previous_tab_index = self.tab_widget.currentIndex() if hasattr(self, "tab_widget") else None
        previous_suppress = getattr(self, "_suppress_gallery_update", False)
        previous_fig_size = None
        switched_observation = False

        try:
            if previous_observation_id != observation_id:
                obs = ObservationDB.get_observation(observation_id)
                if not obs:
                    return False
                genus = (obs.get("genus") or "").strip()
                species = (obs.get("species") or obs.get("species_guess") or "").strip()
                display_name = f"{genus} {species}".strip() or f"Observation {observation_id}"
                self.on_observation_selected(
                    observation_id,
                    display_name,
                    switch_tab=False,
                    suppress_gallery=True,
                )
                switched_observation = True
                self.apply_gallery_settings()
                self.refresh_gallery_filter_options()

            if self.active_observation_id:
                images = ImageDB.get_images_for_observation(self.active_observation_id)
                self.gallery_image_labels = {img['id']: f"Image {idx + 1}" for idx, img in enumerate(images)}
            else:
                self.gallery_image_labels = {}

            measurements = self.get_gallery_measurements()
            try:
                previous_fig_size = tuple(self.gallery_plot_figure.get_size_inches())
                # Wider export canvas so scatter panel + stacked histograms have room for labels.
                self.gallery_plot_figure.set_size_inches(10.5, 6.0, forward=False)
            except Exception:
                previous_fig_size = None
            self.update_graph_plots(measurements)
            self.gallery_plot_figure.savefig(str(target_path), format="png", dpi=140)
            return target_path.exists()
        except Exception:
            return False
        finally:
            if previous_fig_size and hasattr(self, "gallery_plot_figure"):
                try:
                    self.gallery_plot_figure.set_size_inches(
                        float(previous_fig_size[0]),
                        float(previous_fig_size[1]),
                        forward=False,
                    )
                    if hasattr(self, "gallery_plot_canvas"):
                        self.gallery_plot_canvas.draw_idle()
                except Exception:
                    pass
            self._suppress_gallery_update = previous_suppress
            if switched_observation and previous_observation_id:
                self.on_observation_selected(
                    previous_observation_id,
                    previous_observation_name or f"Observation {previous_observation_id}",
                    switch_tab=False,
                    suppress_gallery=True,
                )
            if previous_tab_index is not None and hasattr(self, "tab_widget"):
                self.tab_widget.setCurrentIndex(previous_tab_index)

    def on_gallery_plot_pick(self, event):
        """Handle pick events from gallery plots."""
        if self._gallery_pan_recently_dragged:
            return
        mouse_event = getattr(event, "mouseevent", None)
        if (
            mouse_event is None
            or getattr(mouse_event, "name", "") != "button_press_event"
            or getattr(mouse_event, "button", None) != 1
        ):
            return
        scatter_map = getattr(self, "gallery_scatter_id_map", {})
        if event.artist in scatter_map:
            indices = getattr(event, "ind", [])
            selected_ids = set()
            ids = scatter_map.get(event.artist, [])
            for idx in indices:
                if idx < len(ids):
                    measurement_id = ids[idx]
                    if measurement_id:
                        selected_ids.add(measurement_id)
            if selected_ids:
                self._store_gallery_plot_view()
                self.gallery_filter_mode = "points"
                self.gallery_filter_value = None
                self.gallery_filter_ids = selected_ids
                if len(selected_ids) == 1:
                    self.gallery_selected_measurement_id = next(iter(selected_ids))
                self._update_gallery_filter_label()
                self.schedule_gallery_refresh()
            return

        if hasattr(self, "gallery_hist_patches") and event.artist in self.gallery_hist_patches:
            self._store_gallery_plot_view()
            metric, min_val, max_val = self.gallery_hist_patches[event.artist]
            self.gallery_filter_mode = "bin"
            self.gallery_filter_value = (metric, min_val, max_val)
            self.gallery_filter_ids = set()
            self._update_gallery_filter_label()
            self.schedule_gallery_refresh()

    def _update_gallery_filter_label(self):
        if not hasattr(self, "gallery_filter_label"):
            return
        label = ""
        if self.gallery_filter_mode == "bin" and self.gallery_filter_value:
            metric, min_val, max_val = self.gallery_filter_value
            name_map = {"L": self.tr("Length"), "W": self.tr("Width"), "Q": "Q"}
            name = name_map.get(metric, metric)
            label = f"{name}: {min_val:.2f} - {max_val:.2f}"
        self.gallery_filter_label.setText(label)

    def _measurement_stats_records_for_category(self, measurements: list[dict], category: str | None = None) -> list[dict]:
        records: list[dict] = []
        normalized = self.normalize_measurement_category(category) if category else None
        for measurement in measurements or []:
            measurement_category = self.normalize_measurement_category(measurement.get("measurement_type"))
            if measurement_category == "calibration":
                continue
            if normalized == "all_except_spores" and measurement_category == "spores":
                continue
            if normalized and measurement_category != normalized:
                continue
            length = measurement.get("length_um")
            if length is None:
                continue
            records.append(measurement)
        return records

    def _stats_from_measurement_records(self, measurements: list[dict]) -> tuple[dict | None, list[float], list[float]]:
        all_lengths = []
        paired_lengths = []
        paired_widths = []
        for m in measurements:
            l = m.get("length_um")
            w = m.get("width_um")
            if l is not None:
                all_lengths.append(float(l))
                if w is not None and float(w) > 0:
                    paired_lengths.append(float(l))
                    paired_widths.append(float(w))
        stats = self._stats_from_measurements(all_lengths, paired_lengths, paired_widths) if all_lengths else None
        return stats, paired_lengths, paired_widths

    def _format_measurement_stats_string(self, stats: dict | None, category: str | None = "spores") -> str:
        label = self.format_measurement_category(category or "spores") if category and category != "spores" else self.tr("Spores")
        if not stats:
            return f"{label}: -"
        stats_text = (
            f"{label}: ({stats['length_min']:.1f}-){stats['length_p5']:.1f}-"
            f"{stats['length_p95']:.1f}(-{stats['length_max']:.1f}) um"
        )
        if "width_mean" in stats and stats.get("width_mean", 0) > 0:
            stats_text += (
                f" x ({stats['width_min']:.1f}-){stats['width_p5']:.1f}-"
                f"{stats['width_p95']:.1f}(-{stats['width_max']:.1f}) um"
            )
            stats_text += (
                f", Q = ({stats['ratio_min']:.1f}-){stats['ratio_p5']:.1f}-"
                f"{stats['ratio_p95']:.1f}(-{stats['ratio_max']:.1f})"
            )
            stats_text += f", Qm = {stats['ratio_mean']:.1f}"
        stats_text += f", n = {stats['count']}"
        return stats_text

    def _build_measurement_stats_table_rows(
        self,
        measurements: list[dict],
        include_details: bool,
        image_labels: dict,
        image_details_cache: dict[int, dict],
    ) -> list[str]:
        rows = ["Width\tLength\tQ"]
        if include_details:
            rows[0] += "\tImage\tContrast\tMount\tStain\tSample\tObjective"
        for measurement in measurements:
            length = float(measurement.get("length_um"))
            width_val = measurement.get("width_um")
            if width_val is not None and float(width_val) > 0:
                width = float(width_val)
                q = length / width
                row = f"{width:.2f}\t{length:.2f}\t{q:.2f}"
            else:
                row = f"-\t{length:.2f}\t-"
            if include_details:
                image_id = measurement.get("image_id")
                image_data = {}
                if image_id:
                    image_id = int(image_id)
                    if image_id not in image_details_cache:
                        image_details_cache[image_id] = ImageDB.get_image(image_id) or {}
                    image_data = image_details_cache.get(image_id, {})
                image_label = image_labels.get(image_id) if image_id else None
                image_display = ""
                if image_label:
                    parts = str(image_label).split()
                    image_display = parts[-1] if parts else str(image_label)
                elif image_id:
                    image_display = str(image_id)
                contrast = DatabaseTerms.translate("contrast", image_data.get("contrast"))
                mount = DatabaseTerms.translate("mount", image_data.get("mount_medium"))
                stain = DatabaseTerms.translate("stain", image_data.get("stain"))
                sample = DatabaseTerms.translate("sample", image_data.get("sample_type"))
                objective = str(image_data.get("objective_name") or "").strip()
                row += "\t" + "\t".join([image_display, contrast, mount, stain, sample, objective])
            rows.append(row)
        return rows

    def _ordered_stats_categories(self, categories: list[str]) -> list[str]:
        normalized = [self.normalize_measurement_category(category) for category in categories if category]
        ordered: list[str] = []
        if "spores" in normalized:
            ordered.append("spores")
        for category in normalized:
            if category not in ordered and category != "calibration":
                ordered.append(category)
        return ordered

    def _build_stats_text(self):
        if not self.active_observation_id:
            return None, None, None
        obs = ObservationDB.get_observation(self.active_observation_id)
        if not obs:
            return None, None, None
        genus = obs.get("genus") or ""
        species = obs.get("species") or obs.get("species_guess") or ""
        display_name = f"{genus} {species}".strip()
        if not display_name:
            display_name = obs.get("display_name") or obs.get("name") or self.tr("Observation")
        date_value = obs.get("date") or ""
        first_line = f"{display_name}\t{date_value}".rstrip()

        category = self.gallery_filter_combo.currentData() if hasattr(self, "gallery_filter_combo") else None
        normalized = self.normalize_measurement_category(category) if category else None
        measurements = MeasurementDB.get_measurements_for_observation(self.active_observation_id)
        include_details = bool(
            hasattr(self, "gallery_include_details_checkbox")
            and self.gallery_include_details_checkbox.isChecked()
        )
        image_labels = getattr(self, "gallery_image_labels", {}) or {}
        image_details_cache: dict[int, dict] = {}
        body_lines: list[str] = []

        if category is None:
            categories = self._ordered_stats_categories(
                [
                    self.normalize_measurement_category(measurement.get("measurement_type"))
                    for measurement in (measurements or [])
                    if self.normalize_measurement_category(measurement.get("measurement_type")) != "calibration"
                ]
            )
            if not categories:
                body_lines.append(self.tr("No measurements"))
            for index, section_category in enumerate(categories):
                section_measurements = self._measurement_stats_records_for_category(measurements, section_category)
                section_stats, section_lengths, section_widths = self._stats_from_measurement_records(section_measurements)
                if index > 0:
                    body_lines.append("")
                body_lines.append(self._format_measurement_stats_string(section_stats, section_category))
                if section_category == "spores":
                    parmasto_lines = self._parmasto_fit_summary_lines(
                        self._parmasto_specimen_metrics(section_lengths, section_widths),
                        self._active_reference_data_for_stats(),
                    )
                    if parmasto_lines:
                        body_lines.extend(parmasto_lines)
                body_lines.extend(
                    self._build_measurement_stats_table_rows(
                        section_measurements,
                        include_details,
                        image_labels,
                        image_details_cache,
                    )
                )
        else:
            filtered_measurements = self._measurement_stats_records_for_category(measurements, normalized)
            filtered_stats, filtered_lengths, filtered_widths = self._stats_from_measurement_records(filtered_measurements)
            body_lines.append(self._format_measurement_stats_string(filtered_stats, normalized))
            if normalized == "spores":
                parmasto_lines = self._parmasto_fit_summary_lines(
                    self._parmasto_specimen_metrics(filtered_lengths, filtered_widths),
                    self._active_reference_data_for_stats(),
                )
                if parmasto_lines:
                    body_lines.extend(parmasto_lines)
            body_lines.extend(
                self._build_measurement_stats_table_rows(
                    filtered_measurements,
                    include_details,
                    image_labels,
                    image_details_cache,
                )
            )

        text = "\n".join([first_line] + body_lines)
        return text, display_name, date_value

    def _update_gallery_stats_preview(self):
        if not hasattr(self, "gallery_stats_preview"):
            return
        text, _name, _date = self._build_stats_text()
        self.gallery_stats_preview.setPlainText(text or "")

    def copy_spore_stats(self):
        """Copy stats + table to clipboard."""
        text, _name, _date = self._build_stats_text()
        if not text:
            QMessageBox.warning(self, self.tr("No Observation"), self.tr("Select an observation first."))
            return
        QApplication.clipboard().setText(text)

    def save_spore_stats(self):
        """Save stats + table to a text file."""
        text, name, date_value = self._build_stats_text()
        if not text:
            QMessageBox.warning(self, self.tr("No Observation"), self.tr("Select an observation first."))
            return
        default_name = name or "stats"
        safe_name = re.sub(r'[<>:"/\\\\|?*]', "_", str(default_name))
        safe_date = re.sub(r'[<>:"/\\\\|?*]', "_", str(date_value)).strip()
        if safe_date:
            suggested = f"{safe_name} {safe_date} stats.txt"
        else:
            suggested = f"{safe_name} stats.txt"
        default_path = str(Path(self._get_default_export_dir()) / suggested)
        filename, _ = QFileDialog.getSaveFileName(
            self,
            self.tr("Save stats"),
            default_path,
            "Text Files (*.txt)"
        )
        if not filename:
            return
        if not filename.lower().endswith(".txt"):
            filename = f"{filename}.txt"
        self._remember_export_dir(filename)
        try:
            Path(filename).write_text(text, encoding="utf-8")
        except Exception as exc:
            QMessageBox.warning(self, self.tr("Save Failed"), str(exc))

    def _get_measurement_by_id(self, measurement_id):
        """Load a measurement record by id."""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM spore_measurements WHERE id = ?', (measurement_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def open_measurement_from_gallery(self, measurement_id):
        """Open a measurement in the Measure tab from the gallery."""
        measurement_id = int(measurement_id or 0)
        if not measurement_id:
            return
        self._prepare_analysis_gallery_for_tab_switch()
        QTimer.singleShot(75, lambda mid=measurement_id: self._open_measurement_from_gallery_impl(mid))

    def _open_measurement_from_gallery_impl(self, measurement_id: int):
        """Deferred gallery-to-measure navigation to avoid tab-switch crashes mid-click."""
        if self.measurement_active:
            self.stop_measurement()
        measurement = self._get_measurement_by_id(measurement_id)
        if not measurement:
            return
        self._suppress_gallery_update = True
        try:
            image_id = measurement.get("image_id")
            if image_id and image_id != self.current_image_id:
                image_data = ImageDB.get_image(image_id)
                if image_data:
                    self.load_image_record(image_data, refresh_table=True)
            else:
                self.update_measurements_table()
        finally:
            self._suppress_gallery_update = False
        self.select_measurement_in_table(measurement_id)
        if hasattr(self, "tab_widget"):
            self.tab_widget.setCurrentIndex(1)

    def _highlight_selected_measurement(self, measurement):
        if not measurement:
            self._clear_measurement_highlight()
            return
        measurement_id = measurement.get("id")
        if not measurement_id:
            self._clear_measurement_highlight()
            return
        lines_list = self.measurement_lines.get(measurement_id, [])
        if measurement_id in getattr(self, "multiline_measurements", {}):
            indices = getattr(self, "_line_index_map", {}).get(measurement_id, [])
            self.image_label.set_selected_line_indices(indices)
            self.image_label.set_selected_rect_index(-1)
            return
        if len(lines_list) == 1:
            indices = getattr(self, "_line_index_map", {}).get(measurement_id, [])
            self.image_label.set_selected_line_indices(indices)
            self.image_label.set_selected_rect_index(-1)
            return
        if self.measure_mode == "lines":
            indices = getattr(self, "_line_index_map", {}).get(measurement_id, [])
            self.image_label.set_selected_line_indices(indices)
            self.image_label.set_selected_rect_index(-1)
        else:
            rect_index = getattr(self, "_rect_index_map", {}).get(measurement_id, -1)
            self.image_label.set_selected_rect_index(rect_index)
            self.image_label.set_selected_line_indices([])

    def _clear_measurement_highlight(self):
        if hasattr(self, "image_label"):
            self.image_label.set_selected_rect_index(-1)
            self.image_label.set_selected_line_indices([])

    def _clear_measurement_selection(self) -> None:
        if hasattr(self, "measurements_table"):
            self.measurements_table.clearSelection()
        if hasattr(self, "spore_preview"):
            self.spore_preview.clear()
        self._clear_measurement_highlight()

    def _confidence_ellipse_points(self, x, y, confidence=0.95, n_points=300):
        """Return ellipse points for a data ellipse of the observed distribution."""
        if len(x) < 3 or len(y) < 3:
            return None
        try:
            confidence = float(confidence)
        except (TypeError, ValueError):
            return None
        confidence = max(0.01, min(0.999, confidence))
        chi2_val = -2.0 * math.log(max(1e-12, 1.0 - confidence))

        mean = np.array([np.mean(x), np.mean(y)])
        cov = np.cov(x, y, ddof=1)
        eigvals, eigvecs = np.linalg.eigh(cov)
        order = np.argsort(eigvals)[::-1]
        eigvals = eigvals[order]
        eigvecs = eigvecs[:, order]
        axis_lengths = np.sqrt(eigvals * chi2_val)

        t = np.linspace(0, 2 * math.pi, n_points)
        circle = np.vstack((np.cos(t), np.sin(t)))
        ellipse = (eigvecs @ (axis_lengths[:, None] * circle)) + mean[:, None]
        return ellipse[0, :], ellipse[1, :]

    def _gaussian_kde_grid(self, x, y, bw_method=1.0, grid_size=180):
        """Evaluate a 2D Gaussian KDE on a regular grid for contour plotting."""
        bw_method = max(0.5, min(1.5, float(bw_method)))

        try:
            from scipy.stats import gaussian_kde
        except Exception:
            gaussian_kde = None

        x = np.asarray(x, dtype=float)
        y = np.asarray(y, dtype=float)
        valid = np.isfinite(x) & np.isfinite(y)
        if np.count_nonzero(valid) < 3:
            return None
        x = x[valid]
        y = y[valid]
        if x.size < 3 or y.size < 3:
            return None

        x_span = float(np.ptp(x))
        y_span = float(np.ptp(y))
        if x_span <= 0 or y_span <= 0:
            return None

        x_pad = max(x_span * 0.08, 0.2)
        y_pad = max(y_span * 0.08, 0.1)
        grid_x = np.linspace(float(np.min(x)) - x_pad, float(np.max(x)) + x_pad, int(grid_size))
        grid_y = np.linspace(float(np.min(y)) - y_pad, float(np.max(y)) + y_pad, int(grid_size))
        xx, yy = np.meshgrid(grid_x, grid_y)
        positions = np.vstack([xx.ravel(), yy.ravel()])
        if gaussian_kde is not None:
            try:
                kde = gaussian_kde(np.vstack([x, y]), bw_method=bw_method)
                density = np.reshape(kde(positions), xx.shape)
            except Exception:
                return None
        else:
            samples = np.column_stack((x, y))
            sample_count = samples.shape[0]
            dims = samples.shape[1]
            if sample_count < 3 or dims != 2:
                return None
            covariance = np.cov(samples, rowvar=False, ddof=1)
            if covariance.shape != (2, 2):
                return None
            scott_factor = float(sample_count) ** (-1.0 / (dims + 4.0))
            scaled_covariance = covariance * (scott_factor * bw_method) ** 2
            scaled_covariance = scaled_covariance + np.eye(dims) * 1e-12
            try:
                inv_covariance = np.linalg.inv(scaled_covariance)
                det_covariance = float(np.linalg.det(scaled_covariance))
            except np.linalg.LinAlgError:
                return None
            if not np.isfinite(det_covariance) or det_covariance <= 0:
                return None
            normalizer = float(sample_count) * math.sqrt(((2.0 * math.pi) ** dims) * det_covariance)
            if normalizer <= 0 or not np.isfinite(normalizer):
                return None

            grid_points = np.column_stack((xx.ravel(), yy.ravel()))
            density_flat = np.empty(grid_points.shape[0], dtype=float)
            chunk_size = 2048
            for start in range(0, grid_points.shape[0], chunk_size):
                stop = min(start + chunk_size, grid_points.shape[0])
                chunk = grid_points[start:stop]
                diff = chunk[:, None, :] - samples[None, :, :]
                exponent = np.einsum("...i,ij,...j->...", diff, inv_covariance, diff)
                density_flat[start:stop] = np.exp(-0.5 * exponent).sum(axis=1) / normalizer
            density = density_flat.reshape(xx.shape)

        if grid_x.size < 2 or grid_y.size < 2:
            return None
        cell_area = float(grid_x[1] - grid_x[0]) * float(grid_y[1] - grid_y[0])
        if not np.isfinite(cell_area) or cell_area <= 0:
            return None
        return xx, yy, density, cell_area

    def _kde_contour_levels(self, density, cell_area, contour_count: int = 6, coverage_percent: int = 95):
        """Convert KDE density into contour thresholds enclosing target probability mass."""
        finite = np.asarray(density, dtype=float)
        finite = finite[np.isfinite(finite)]
        if finite.size == 0:
            return None
        ordered = np.sort(finite)[::-1]
        cumulative = np.cumsum(ordered) * float(cell_area)
        total = float(cumulative[-1]) if cumulative.size else 0.0
        if total <= 0:
            return None

        contour_count = max(1, min(10, int(contour_count)))
        coverage_percent = max(50, min(99, int(coverage_percent)))
        outer_mass = float(coverage_percent) / 100.0
        if contour_count == 1:
            mass_levels = np.asarray([outer_mass], dtype=float)
        else:
            mass_levels = np.linspace(outer_mass / float(contour_count), outer_mass, contour_count)
        thresholds = []
        for level in mass_levels:
            idx = int(np.searchsorted(cumulative, total * float(level), side="left"))
            idx = max(0, min(idx, ordered.size - 1))
            thresholds.append(float(ordered[idx]))

        threshold_labels: dict[float, str] = {}
        for threshold, level in zip(thresholds, mass_levels):
            if np.isfinite(threshold) and threshold > 0:
                threshold_labels[float(threshold)] = f"{int(round(float(level) * 100.0))}%"

        contour_levels = sorted({value for value in thresholds if np.isfinite(value) and value > 0})
        if not contour_levels:
            return None
        label_map = {float(level): threshold_labels.get(float(level), "") for level in contour_levels}
        return contour_levels, label_map

    def _stats_from_measurements(self, all_lengths, paired_lengths, paired_widths):
        """Compute stats dictionary from length/width lists."""
        if all_lengths is None:
            return None
        all_lengths = np.asarray(all_lengths, dtype=float)
        if all_lengths.size == 0:
            return None
        paired_lengths = np.asarray(paired_lengths, dtype=float) if paired_lengths is not None else np.asarray([], dtype=float)
        paired_widths = np.asarray(paired_widths, dtype=float) if paired_widths is not None else np.asarray([], dtype=float)
        stats = {
            "count": int(len(all_lengths)),
            "length_mean": float(np.mean(all_lengths)),
            "length_std": float(np.std(all_lengths)),
            "length_min": float(np.min(all_lengths)),
            "length_max": float(np.max(all_lengths)),
            "length_p5": float(np.percentile(all_lengths, 5)),
            "length_p95": float(np.percentile(all_lengths, 95)),
        }
        if paired_widths.size > 0 and paired_lengths.size == paired_widths.size:
            ratios = paired_lengths / paired_widths
            stats.update({
                "width_mean": float(np.mean(paired_widths)),
                "width_std": float(np.std(paired_widths)),
                "width_min": float(np.min(paired_widths)),
                "width_max": float(np.max(paired_widths)),
                "width_p5": float(np.percentile(paired_widths, 5)),
                "width_p95": float(np.percentile(paired_widths, 95)),
                "ratio_mean": float(np.mean(ratios)),
                "ratio_min": float(np.min(ratios)),
                "ratio_max": float(np.max(ratios)),
                "ratio_p5": float(np.percentile(ratios, 5)),
                "ratio_p95": float(np.percentile(ratios, 95)),
            })
        return stats

    def _parmasto_specimen_metrics(self, lengths, widths):
        """Compute specimen-level Parmasto metrics from raw spore measurements."""
        if lengths is None or widths is None:
            return None
        L = np.asarray(lengths, dtype=float)
        W = np.asarray(widths, dtype=float)
        if L.size == 0 or W.size == 0:
            return None
        valid = W > 0
        if not np.any(valid):
            return None
        L = L[valid]
        W = W[valid]
        Q = L / W

        def _cv_percent(values):
            values = np.asarray(values, dtype=float)
            if values.size == 0:
                return None
            mean = float(np.mean(values))
            if mean == 0:
                return None
            if values.size < 2:
                return 0.0
            std = float(np.std(values, ddof=1))
            return (std / mean) * 100.0

        return {
            "L_m": float(np.mean(L)),
            "W_m": float(np.mean(W)),
            "Q_m": float(np.mean(Q)),
            "V_indL": _cv_percent(L),
            "V_indW": _cv_percent(W),
            "V_indQ": _cv_percent(Q),
        }

    def _parmasto_expected_range(self, mean_value, cv_percent):
        """Return the approximate 95% Parmasto range around a species mean."""
        try:
            mean_value = float(mean_value)
            cv_percent = float(cv_percent)
        except (TypeError, ValueError):
            return None
        spread = 2.0 * (cv_percent / 100.0) * mean_value
        return (mean_value - spread, mean_value + spread)

    def _parmasto_reference_metrics(self, data: dict | None):
        """Extract persisted Parmasto biometrics from a reference record."""
        if not isinstance(data, dict) or not data:
            return None
        keys = (
            "parmasto_length_mean",
            "parmasto_width_mean",
            "parmasto_q_mean",
            "parmasto_v_sp_length",
            "parmasto_v_sp_width",
            "parmasto_v_sp_q",
            "parmasto_v_ind_length",
            "parmasto_v_ind_width",
            "parmasto_v_ind_q",
        )
        if not any(data.get(key) is not None for key in keys):
            return None
        return {
            "L_bar": data.get("parmasto_length_mean"),
            "W_bar": data.get("parmasto_width_mean"),
            "Q_bar": data.get("parmasto_q_mean"),
            "V_spL": data.get("parmasto_v_sp_length"),
            "V_spW": data.get("parmasto_v_sp_width"),
            "V_spQ": data.get("parmasto_v_sp_q"),
            "V_indL_bar": data.get("parmasto_v_ind_length"),
            "V_indW_bar": data.get("parmasto_v_ind_width"),
            "V_indE_bar": data.get("parmasto_v_ind_q"),
        }

    def _active_reference_data_for_stats(self) -> dict | None:
        genus = self._clean_ref_genus_text(self.ref_genus_input.text()) if hasattr(self, "ref_genus_input") else ""
        species = self._clean_ref_species_text(self.ref_species_input.text()) if hasattr(self, "ref_species_input") else ""
        if genus and species and hasattr(self, "ref_source_input"):
            selected_source = self.ref_source_input.currentText().strip() or None
            selected_data = self.ref_source_input.currentData()
            if isinstance(selected_data, dict):
                selected_kind = (selected_data.get("kind") or "").strip().lower()
                if selected_kind == "reference":
                    selected_source = selected_data.get("source") or selected_source
            if selected_source:
                selected_reference = ReferenceDB.get_reference(genus, species, selected_source)
                if isinstance(selected_reference, dict) and self._parmasto_reference_metrics(selected_reference):
                    selected_reference["source_kind"] = "reference"
                    return selected_reference

        current = self.reference_values if isinstance(self.reference_values, dict) else None
        if current and self._parmasto_reference_metrics(current):
            return current
        for entry in self.reference_series or []:
            if not isinstance(entry, dict):
                continue
            if not bool(entry.get("enabled", True)):
                continue
            data = entry.get("data")
            if isinstance(data, dict) and self._parmasto_reference_metrics(data):
                return data
        for entry in self.reference_series or []:
            if not isinstance(entry, dict):
                continue
            data = entry.get("data")
            if isinstance(data, dict) and self._parmasto_reference_metrics(data):
                return data
        return current if isinstance(current, dict) and current else None

    def _parmasto_fit_summary_lines(self, specimen_metrics: dict | None, reference_data: dict | None) -> list[str]:
        """Summarize Parmasto fit checks for the current specimen vs reference."""
        specimen = specimen_metrics or {}
        reference = self._parmasto_reference_metrics(reference_data)
        if not specimen:
            return []

        def _fmt(value):
            try:
                return f"{float(value):.2f}"
            except (TypeError, ValueError):
                return "-"

        lines = [
            self.tr(
                "Parmasto specimen: Lm={lm}, Wm={wm}, Qm={qm}"
            ).format(
                lm=_fmt(specimen.get("L_m")),
                wm=_fmt(specimen.get("W_m")),
                qm=_fmt(specimen.get("Q_m")),
            ),
            self.tr(
                "Parmasto variation: VindL={vindl}%, VindW={vindw}%, VindE={vinde}%"
            ).format(
                vindl=_fmt(specimen.get("V_indL")),
                vindw=_fmt(specimen.get("V_indW")),
                vinde=_fmt(specimen.get("V_indQ")),
            ),
        ]

        if not reference:
            return lines

        fit_parts = []
        comparisons = [
            ("L", specimen.get("L_m"), reference.get("L_bar"), reference.get("V_spL")),
            ("W", specimen.get("W_m"), reference.get("W_bar"), reference.get("V_spW")),
            ("Q", specimen.get("Q_m"), reference.get("Q_bar"), reference.get("V_spQ")),
        ]
        for name, specimen_value, ref_mean, ref_cv in comparisons:
            expected = self._parmasto_expected_range(ref_mean, ref_cv)
            if expected is None or specimen_value is None:
                continue
            low, high = expected
            status = self.tr("OK") if low <= float(specimen_value) <= high else self.tr("Outlier")
            fit_parts.append(
                f"{name}={status} [{low:.2f}-{high:.2f}]"
            )
        if fit_parts:
            lines.append(f"{self.tr('Parmasto fit')}: " + "; ".join(fit_parts))

        ref_v_ind_e = reference.get("V_indE_bar")
        specimen_v_ind_e = specimen.get("V_indQ")
        try:
            if ref_v_ind_e is not None and specimen_v_ind_e is not None and float(specimen_v_ind_e) > float(ref_v_ind_e):
                lines.append(
                    self.tr(
                        "Parmasto warning: specimen spore shape is more variable than typical "
                        "(VindE {specimen}% > reference {reference}%)."
                    ).format(
                        specimen=_fmt(specimen_v_ind_e),
                        reference=_fmt(ref_v_ind_e),
                    )
                )
        except (TypeError, ValueError):
            pass

        return lines

    def rotate_gallery_thumbnail(self, measurement_id):
        """Rotate a gallery thumbnail by 180 degrees."""
        current = self.gallery_rotations.get(measurement_id, 0)
        new_rotation = (current + 180) % 360
        self.gallery_rotations[measurement_id] = new_rotation
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            'UPDATE spore_measurements SET gallery_rotation = ? WHERE id = ?',
            (new_rotation, measurement_id)
        )
        conn.commit()
        conn.close()
        self.schedule_gallery_refresh()


    def create_spore_thumbnail(self, pixmap, points, length_um, width_um, size,
                               measurement_num=0, orient=False, extra_rotation=0,
                               uniform_length_px=None, color=None,
                               rectangle_style=None, rectangle_thickness=None,
                               selected: bool = False,
                               export_mode: bool = False,
                               metadata: dict | None = None):
        """Create a thumbnail image of a single measurement.

        Args:
            pixmap: Source image
            points: List of 4 QPointF measurement points
            length_um: Length in microns
            width_um: Width in microns
            size: Output thumbnail size (square)
            measurement_num: Number to display on thumbnail
            orient: If True, rotate so length axis is vertical
            extra_rotation: Additional rotation in degrees (e.g., 180 for flip)
        """
        from PySide6.QtGui import QPainter, QColor, QPolygonF, QPen, QTransform, QPainterPath
        from PySide6.QtCore import QPointF, QRectF
        import math

        def _measure_stroke_style(stroke_color):
            base = QColor(stroke_color) if stroke_color is not None else QColor("#0044aa")
            presets = (
                {"match": QColor("#005993"), "thin": QColor("#005993"), "glow": QColor("#0072bd"), "opacity": 0.52, "blend": "screen"},
                {"match": QColor("#a94114"), "thin": QColor("#a94114"), "glow": QColor("#d95319"), "opacity": 0.58, "blend": "screen"},
                {"match": QColor("#b98a19"), "thin": QColor("#b98a19"), "glow": QColor("#edb120"), "opacity": 0.58, "blend": "screen"},
                {"match": QColor("#62256f"), "thin": QColor("#62256f"), "glow": QColor("#7e2f8e"), "opacity": 0.48, "blend": "screen"},
                {"match": QColor("#5d8625"), "thin": QColor("#5d8625"), "glow": QColor("#77ac30"), "opacity": 0.58, "blend": "screen"},
                {"match": QColor("#3c94ba"), "thin": QColor("#3c94ba"), "glow": QColor("#4dbeee"), "opacity": 0.58, "blend": "screen"},
                {"match": QColor("#7e1025"), "thin": QColor("#7e1025"), "glow": QColor("#a2142f"), "opacity": 0.58, "blend": "screen"},
                {"match": QColor("#0072bd"), "thin": QColor("#0072bd"), "glow": QColor("#0072bd"), "opacity": 0.52, "blend": "screen"},
                {"match": QColor("#d95319"), "thin": QColor("#d95319"), "glow": QColor("#d95319"), "opacity": 0.58, "blend": "screen"},
                {"match": QColor("#edb120"), "thin": QColor("#edb120"), "glow": QColor("#edb120"), "opacity": 0.58, "blend": "screen"},
                {"match": QColor("#7e2f8e"), "thin": QColor("#7e2f8e"), "glow": QColor("#7e2f8e"), "opacity": 0.5, "blend": "screen"},
                {"match": QColor("#77ac30"), "thin": QColor("#77ac30"), "glow": QColor("#77ac30"), "opacity": 0.58, "blend": "screen"},
                {"match": QColor("#4dbeee"), "thin": QColor("#4dbeee"), "glow": QColor("#4dbeee"), "opacity": 0.58, "blend": "screen"},
                {"match": QColor("#a2142f"), "thin": QColor("#a2142f"), "glow": QColor("#a2142f"), "opacity": 0.58, "blend": "screen"},
                {"match": QColor("#4799cf"), "thin": QColor("#4799cf"), "glow": QColor("#4799cf"), "opacity": 0.5, "blend": "screen"},
                {"match": QColor("#e48359"), "thin": QColor("#e48359"), "glow": QColor("#e48359"), "opacity": 0.54, "blend": "screen"},
                {"match": QColor("#f2c75e"), "thin": QColor("#f2c75e"), "glow": QColor("#f2c75e"), "opacity": 0.54, "blend": "screen"},
                {"match": QColor("#a269ae"), "thin": QColor("#a269ae"), "glow": QColor("#a269ae"), "opacity": 0.46, "blend": "screen"},
                {"match": QColor("#9dc36a"), "thin": QColor("#9dc36a"), "glow": QColor("#9dc36a"), "opacity": 0.54, "blend": "screen"},
                {"match": QColor("#7fd0f3"), "thin": QColor("#7fd0f3"), "glow": QColor("#7fd0f3"), "opacity": 0.54, "blend": "screen"},
                {"match": QColor("#bc5669"), "thin": QColor("#bc5669"), "glow": QColor("#bc5669"), "opacity": 0.54, "blend": "screen"},
                {"match": QColor("#1E90FF"), "thin": QColor("#0044aa"), "glow": QColor("#2a7fff"), "opacity": 0.576531, "blend": "screen"},
                {"match": QColor("#3498db"), "thin": QColor("#0044aa"), "glow": QColor("#2a7fff"), "opacity": 0.576531, "blend": "screen"},
                {"match": QColor("#FF3B30"), "thin": QColor("#d40000"), "glow": QColor("#d40000"), "opacity": 0.658163, "blend": "screen"},
                {"match": QColor("#2ECC71"), "thin": QColor("#00aa00"), "glow": QColor("#00aa00"), "opacity": 0.658163, "blend": "screen"},
                {"match": QColor("#E056FD"), "thin": QColor("#ff00ff"), "glow": QColor("#ff00ff"), "opacity": 0.433674, "blend": "screen"},
                {"match": QColor("#ECAF11"), "thin": QColor("#ffd42a"), "glow": QColor("#ffdd55"), "opacity": 0.658163, "blend": "overlay"},
                {"match": QColor("#1CEBEB"), "thin": QColor("#00ffff"), "glow": QColor("#00ffff"), "opacity": 0.658163, "blend": "overlay"},
                {"match": QColor("#808080"), "thin": QColor("#808080"), "glow": QColor("#a8a8a8"), "opacity": 0.5, "blend": "screen"},
                {"match": QColor("#ffffff"), "thin": QColor("#ffffff"), "glow": QColor("#ffffff"), "opacity": 0.42, "blend": "screen"},
                {"match": QColor("#000000"), "thin": QColor("#000000"), "glow": QColor("#7a7a7a"), "opacity": 0.5, "blend": "screen"},
            )
            def _dist2(c1, c2):
                dr = c1.red() - c2.red()
                dg = c1.green() - c2.green()
                db = c1.blue() - c2.blue()
                return dr * dr + dg * dg + db * db
            chosen = min(presets, key=lambda p: _dist2(base, p["match"]))
            thin = QColor(chosen["thin"])
            glow = QColor(chosen["glow"])
            glow.setAlpha(max(1, min(255, int(round(255 * float(chosen["opacity"]))))))  # type: ignore[arg-type]
            return {"thin": thin, "glow": glow, "blend": str(chosen["blend"])}

        def _set_comp(p: QPainter, comp_name: str):
            if comp_name == "overlay":
                p.setCompositionMode(QPainter.CompositionMode_Overlay)
            elif comp_name == "screen":
                p.setCompositionMode(QPainter.CompositionMode_Screen)
            elif comp_name == "plus":
                p.setCompositionMode(QPainter.CompositionMode_Plus)
            elif comp_name == "lighten":
                p.setCompositionMode(QPainter.CompositionMode_Lighten)

        def _draw_dual_polygon(
            p: QPainter,
            polygon: QPolygonF,
            stroke_color,
            thin_width: float,
            wide_width: float,
            use_blend: bool = True,
        ):
            style = _measure_stroke_style(stroke_color)
            thin_pen = QPen(QColor(style["thin"]), clamp_stroke_width(thin_width))
            wide_pen = QPen(QColor(style["glow"]), clamp_stroke_width(wide_width))
            if use_blend:
                p.save()
                _set_comp(p, str(style["blend"]))
                p.setPen(wide_pen)
                p.setBrush(Qt.NoBrush)
                p.drawPolygon(polygon)
                p.restore()
            else:
                p.setPen(wide_pen)
                p.setBrush(Qt.NoBrush)
                p.drawPolygon(polygon)
            p.setBrush(Qt.NoBrush)
            p.setPen(thin_pen)
            p.drawPolygon(polygon)

        def _draw_dual_line(
            p: QPainter,
            a: QPointF,
            b: QPointF,
            stroke_color,
            thin_width: float,
            wide_width: float,
            use_blend: bool = True,
        ):
            style = _measure_stroke_style(stroke_color)
            thin_pen = QPen(QColor(style["thin"]), clamp_stroke_width(thin_width))
            wide_pen = QPen(QColor(style["glow"]), clamp_stroke_width(wide_width))
            if use_blend:
                p.save()
                _set_comp(p, str(style["blend"]))
                p.setPen(wide_pen)
                p.drawLine(a, b)
                p.restore()
            else:
                p.setPen(wide_pen)
                p.drawLine(a, b)
            p.setPen(thin_pen)
            p.drawLine(a, b)

        def _draw_polygon_outline(p: QPainter, polygon: QPolygonF, stroke_color, thin_width: float):
            style = _measure_stroke_style(stroke_color)
            thin_pen = QPen(QColor(style["thin"]), clamp_stroke_width(thin_width))
            p.setBrush(Qt.NoBrush)
            p.setPen(thin_pen)
            p.drawPolygon(polygon)

        def _draw_measurement_rectangle(
            p: QPainter,
            polygon: QPolygonF,
            stroke_color,
            style_name: str,
            wide_width: float,
        ):
            resolved_style_name = normalize_rectangle_style(style_name)
            thin_width = rectangle_thin_stroke_width(resolved_style_name, wide_width)
            if resolved_style_name == DEFAULT_RECTANGLE_STYLE:
                _draw_dual_polygon(p, polygon, stroke_color, thin_width, wide_width)
                return

            _draw_polygon_outline(p, polygon, stroke_color, thin_width)
            for seg_start, seg_end in rectangle_corner_segments(polygon):
                _draw_dual_line(
                    p,
                    seg_start,
                    seg_end,
                    stroke_color,
                    thin_width,
                    wide_width,
                    use_blend=False,
                )

        def _draw_halo_text(p: QPainter, x: int, y: int, text: str, stroke_color):
            style = _measure_stroke_style(stroke_color)
            text_color = QColor(style["thin"])
            path = QPainterPath()
            path.addText(float(x), float(y), p.font(), text)
            if not measure_text_uses_halo(text_color):
                p.save()
                p.setPen(Qt.NoPen)
                p.setBrush(text_color)
                p.drawPath(path)
                p.restore()
                return
            stroke_w = max(1.0, float(p.fontMetrics().height()) * 0.4)
            pen = QPen(QColor(255, 255, 255, 102), stroke_w)
            pen.setJoinStyle(Qt.RoundJoin)
            pen.setCapStyle(Qt.RoundCap)
            p.save()
            p.setBrush(Qt.NoBrush)
            p.setPen(pen)
            p.drawPath(path)
            p.restore()
            p.save()
            p.setPen(Qt.NoPen)
            p.setBrush(text_color)
            p.drawPath(path)
            p.restore()

        if not pixmap:
            return None

        resolved_rectangle_style = normalize_rectangle_style(rectangle_style)
        resolved_rectangle_thickness = clamp_rectangle_thickness(rectangle_thickness)

        # Calculate center and dimensions
        line1_mid = QPointF((points[0].x() + points[1].x()) / 2, (points[0].y() + points[1].y()) / 2)
        line2_mid = QPointF((points[2].x() + points[3].x()) / 2, (points[2].y() + points[3].y()) / 2)
        center = QPointF((line1_mid.x() + line2_mid.x()) / 2, (line1_mid.y() + line2_mid.y()) / 2)

        # Calculate line lengths
        line1_vec = QPointF(points[1].x() - points[0].x(), points[1].y() - points[0].y())
        line2_vec = QPointF(points[3].x() - points[2].x(), points[3].y() - points[2].y())
        line1_len = math.sqrt(line1_vec.x()**2 + line1_vec.y()**2)
        line2_len = math.sqrt(line2_vec.x()**2 + line2_vec.y()**2)

        # Keep stable orientation based on the first measurement line
        length_px = line1_len
        width_px = line2_len

        # Calculate rotation angle if orient is enabled
        rotation_angle = extra_rotation  # Start with any manual extra rotation
        if orient and line1_len > 0:
            # line1_vec IS the length axis (points[0] to points[1] is the center/length line)
            # We want this axis to be vertical (pointing up or down)
            # atan2(x, -y) gives angle from negative y-axis (up direction)
            current_angle = math.atan2(line1_vec.x(), -line1_vec.y())
            rotation_angle += -math.degrees(current_angle)

        # If we rotate for orient, rotate the crop source and points too
        if abs(rotation_angle) > 0.1:
            center_src = QPointF(pixmap.width() / 2, pixmap.height() / 2)
            transform = QTransform()
            transform.translate(center_src.x(), center_src.y())
            transform.rotate(rotation_angle)
            transform.translate(-center_src.x(), -center_src.y())
            rotated_pixmap = pixmap.transformed(transform, Qt.SmoothTransformation)

            # Transform points into rotated pixmap space
            rotated_points = [transform.map(p) for p in points]

            # Offset if the rotated pixmap origin changed
            src_rect = transform.mapRect(QRectF(0, 0, pixmap.width(), pixmap.height()))
            offset = QPointF(-src_rect.x(), -src_rect.y())
            rotated_points = [p + offset for p in rotated_points]
            pixmap = rotated_pixmap
            points = rotated_points

            # Recompute vectors/center with rotated points
            line1_vec = QPointF(points[1].x() - points[0].x(), points[1].y() - points[0].y())
            line2_vec = QPointF(points[3].x() - points[2].x(), points[3].y() - points[2].y())
            line1_len = math.sqrt(line1_vec.x()**2 + line1_vec.y()**2)
            line2_len = math.sqrt(line2_vec.x()**2 + line2_vec.y()**2)
            line1_mid = QPointF((points[0].x() + points[1].x()) / 2, (points[0].y() + points[1].y()) / 2)
            line2_mid = QPointF((points[2].x() + points[3].x()) / 2, (points[2].y() + points[3].y()) / 2)
            center = QPointF((line1_mid.x() + line2_mid.x()) / 2, (line1_mid.y() + line2_mid.y()) / 2)
            length_px = line1_len
            width_px = line2_len
            rotation_angle = 0

        padding_x = 20.0
        padding_y = 10.0 if export_mode else 15.0

        axis_length_img = QPointF(-line1_vec.x() / line1_len, -line1_vec.y() / line1_len)
        axis_width_img = QPointF(-(axis_length_img.y()), axis_length_img.x())
        corners_img = [
            center + axis_width_img * (-width_px / 2.0) + axis_length_img * (-length_px / 2.0),
            center + axis_width_img * (width_px / 2.0) + axis_length_img * (-length_px / 2.0),
            center + axis_width_img * (width_px / 2.0) + axis_length_img * (length_px / 2.0),
            center + axis_width_img * (-width_px / 2.0) + axis_length_img * (length_px / 2.0),
        ]
        min_x = min(point.x() for point in corners_img) - padding_x
        max_x = max(point.x() for point in corners_img) + padding_x
        min_y = min(point.y() for point in corners_img) - padding_y
        max_y = max(point.y() for point in corners_img) + padding_y

        if uniform_length_px:
            desired_height = max(max_y - min_y, float(uniform_length_px) + padding_y * 2.0)
            center_y = (min_y + max_y) / 2.0
            min_y = center_y - desired_height / 2.0
            max_y = center_y + desired_height / 2.0

        crop_rect = QRectF(
            max(0.0, min_x),
            max(0.0, min_y),
            max(1.0, min(float(pixmap.width()), max_x) - max(0.0, min_x)),
            max(1.0, min(float(pixmap.height()), max_y) - max(0.0, min_y)),
        ).intersected(QRectF(0, 0, pixmap.width(), pixmap.height()))

        cropped = pixmap.copy(crop_rect.toRect())
        scale_factor = float(size) / max(1.0, crop_rect.height())
        tile_width = max(1, int(round(crop_rect.width() * scale_factor)))
        tile_height = max(1, int(round(crop_rect.height() * scale_factor)))
        scaled = cropped.scaled(
            tile_width,
            tile_height,
            Qt.IgnoreAspectRatio,
            Qt.SmoothTransformation,
        )

        result = QPixmap(tile_width, tile_height)
        result.fill(Qt.transparent)

        painter = QPainter(result)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setRenderHint(QPainter.SmoothPixmapTransform)

        painter.drawPixmap(0, 0, scaled)

        # Draw rectangle overlay
        length_dir = QPointF(line1_vec.x() / line1_len, line1_vec.y() / line1_len)
        width_dir = QPointF(-length_dir.y(), length_dir.x())
        half_length = length_px / 2
        half_width = width_px / 2

        scale_x = float(tile_width) / max(1.0, crop_rect.width())
        scale_y = float(tile_height) / max(1.0, crop_rect.height())

        # Calculate where the measurement center appears on screen
        center_in_crop_x = center.x() - crop_rect.x()
        center_in_crop_y = center.y() - crop_rect.y()
        screen_center = QPointF(
            center_in_crop_x * scale_x,
            center_in_crop_y * scale_y
        )

        # Apply rotation to the rectangle overlay as well
        if abs(rotation_angle) > 0.1:
            # Rotate the screen_center around the image center
            rad = math.radians(rotation_angle)
            cos_a, sin_a = math.cos(rad), math.sin(rad)
            cx = tile_width / 2
            cy = tile_height / 2
            dx = screen_center.x() - cx
            dy = screen_center.y() - cy
            screen_center = QPointF(
                cx + dx * cos_a - dy * sin_a,
                cy + dx * sin_a + dy * cos_a
            )
            # Also rotate the axis directions
            new_length_dir = QPointF(
                length_dir.x() * cos_a - length_dir.y() * sin_a,
                length_dir.x() * sin_a + length_dir.y() * cos_a
            )
            new_width_dir = QPointF(
                width_dir.x() * cos_a - width_dir.y() * sin_a,
                width_dir.x() * sin_a + width_dir.y() * cos_a
            )
            length_dir = new_length_dir
            width_dir = new_width_dir

        axis_length = QPointF(-length_dir.x(), -length_dir.y())
        axis_width = width_dir
        corners = [
            screen_center + QPointF(axis_width.x() * (-half_width * scale_x) + axis_length.x() * (-half_length * scale_x),
                                    axis_width.y() * (-half_width * scale_y) + axis_length.y() * (-half_length * scale_y)),
            screen_center + QPointF(axis_width.x() * (half_width * scale_x) + axis_length.x() * (-half_length * scale_x),
                                    axis_width.y() * (half_width * scale_y) + axis_length.y() * (-half_length * scale_y)),
            screen_center + QPointF(axis_width.x() * (half_width * scale_x) + axis_length.x() * (half_length * scale_x),
                                    axis_width.y() * (half_width * scale_y) + axis_length.y() * (half_length * scale_y)),
            screen_center + QPointF(axis_width.x() * (-half_width * scale_x) + axis_length.x() * (half_length * scale_x),
                                    axis_width.y() * (-half_width * scale_y) + axis_length.y() * (half_length * scale_y)),
        ]

        stroke_color = QColor(color) if color else QColor("#0044aa")
        _draw_measurement_rectangle(
            painter,
            QPolygonF(corners),
            stroke_color,
            resolved_rectangle_style,
            resolved_rectangle_thickness,
        )
        if selected:
            _draw_measurement_rectangle(
                painter,
                QPolygonF(corners),
                QColor(231, 76, 60),
                DEFAULT_RECTANGLE_STYLE,
                6.0,
            )

        rect_min_x = min(point.x() for point in corners)
        rect_max_x = max(point.x() for point in corners)
        rect_min_y = min(point.y() for point in corners)
        rect_max_y = max(point.y() for point in corners)
        if metadata is not None:
            metadata["polygon"] = QPolygonF(corners)
            metadata["bounds"] = QRectF(rect_min_x, rect_min_y, rect_max_x - rect_min_x, rect_max_y - rect_min_y)

        # Draw dimensions on the image, just below the rectangle.
        font = painter.font()
        font.setPointSize(max(8, int(tile_height * 0.055)))
        painter.setFont(font)
        metrics = painter.fontMetrics()
        dim_text = f"{float(length_um):.1f} x {float(width_um):.1f}"
        text_width = metrics.horizontalAdvance(dim_text)
        text_x = int(max(4.0, min(float(tile_width - text_width - 4), ((rect_min_x + rect_max_x) / 2.0) - (text_width / 2.0))))
        text_y = int(min(float(tile_height - 4), rect_max_y + 8.0 + metrics.ascent()))
        _draw_halo_text(painter, text_x, text_y, dim_text, stroke_color)

        painter.end()
        return result

    def export_gallery_composite(self):
        """Export all spore thumbnails as a single composite image."""
        from PySide6.QtWidgets import QFileDialog
        from PySide6.QtGui import QPainter, QColor
        from PySide6.QtCore import QPointF

        measurements = self.get_gallery_measurements()
        if not measurements:
            return

        valid_measurements = [
            m for m in measurements
            if all(m.get(f'p{i}_{axis}') is not None for i in range(1, 5) for axis in ['x', 'y'])
        ]

        if not valid_measurements:
            return

        # Ask user for format options
        fmt_dialog = ExportGalleryDialog(parent=self)
        if fmt_dialog.exec() != QDialog.Accepted:
            return
        fmt_settings = fmt_dialog.get_settings()
        export_format = fmt_settings["format"]
        export_quality = fmt_settings["quality"]

        # Ask user for save location
        default_name = "spore_gallery"
        if self.active_observation_id:
            obs = ObservationDB.get_observation(self.active_observation_id)
            if obs:
                parts = [
                    obs.get("genus") or "",
                    obs.get("species") or obs.get("species_guess") or "",
                    obs.get("date") or ""
                ]
                name = " ".join([p for p in parts if p]).strip()
                name = name.replace(":", "-")
                name = re.sub(r'[<>:"/\\\\|?*]', "_", name)
                name = re.sub(r"\\s+", " ", name).strip()
                if name:
                    default_name = f"{name} - gallery"

        ext_map = {"png": ".png", "jpg": ".jpg", "svg": ".svg"}
        default_ext = ext_map.get(export_format, ".png")
        filter_map = {
            "png": "PNG Images (*.png)",
            "jpg": "JPEG Images (*.jpg)",
            "svg": "SVG Files (*.svg)",
        }
        default_path = str(Path(self._get_default_export_dir()) / f"{default_name}{default_ext}")
        filename, _ = QFileDialog.getSaveFileName(
            self,
            "Export Gallery Composite",
            default_path,
            f"{filter_map.get(export_format, 'PNG Images (*.png)')};;All Files (*)"
        )

        if not filename:
            return
        self._remember_export_dir(filename)

        # Create composite
        thumbnail_size = self._gallery_thumbnail_size()
        thumbnails = []
        image_color_cache = {}
        pixmap_cache = {}

        # Match gallery settings
        orient = hasattr(self, 'orient_checkbox') and self.orient_checkbox.isChecked()
        uniform_scale = hasattr(self, 'uniform_scale_checkbox') and self.uniform_scale_checkbox.isChecked()
        filtered_measurements = self._filter_gallery_measurements(valid_measurements)
        filtered_measurements = self._sort_gallery_measurements(filtered_measurements)
        if not filtered_measurements:
            return

        uniform_length_um = None
        if uniform_scale:
            for measurement in filtered_measurements:
                length_um = measurement.get("length_um")
                if length_um is None:
                    continue
                if uniform_length_um is None or length_um > uniform_length_um:
                    uniform_length_um = length_um

        for measurement in filtered_measurements:
            pixmap = self.get_measurement_pixmap(measurement, pixmap_cache)
            if not pixmap or pixmap.isNull():
                continue

            measurement_id = measurement['id']
            extra_rotation = measurement.get("gallery_rotation") or self.gallery_rotations.get(measurement_id, 0)

            points = [
                QPointF(measurement['p1_x'], measurement['p1_y']),
                QPointF(measurement['p2_x'], measurement['p2_y']),
                QPointF(measurement['p3_x'], measurement['p3_y']),
                QPointF(measurement['p4_x'], measurement['p4_y'])
            ]

            image_id = measurement.get('image_id')
            stored_color = None
            mpp = None
            if image_id:
                if image_id not in image_color_cache:
                    image_data = ImageDB.get_image(image_id)
                    image_color_cache[image_id] = (
                        {
                            "measure_color": image_data.get('measure_color') if image_data else None,
                            "mpp": image_data.get('scale_microns_per_pixel') if image_data else None
                        }
                    )
                cached = image_color_cache[image_id]
                stored_color = cached.get("measure_color") if cached else None
                mpp = cached.get("mpp") if cached else None
            measure_color = QColor(stored_color) if stored_color else self.default_measure_color
            rectangle_style = self._current_measure_rectangle_style()
            rectangle_thickness = self._current_measure_rectangle_thickness()
            uniform_length_px = None
            if uniform_scale and uniform_length_um:
                if not mpp or mpp <= 0:
                    p1 = QPointF(measurement['p1_x'], measurement['p1_y'])
                    p2 = QPointF(measurement['p2_x'], measurement['p2_y'])
                    p3 = QPointF(measurement['p3_x'], measurement['p3_y'])
                    p4 = QPointF(measurement['p4_x'], measurement['p4_y'])
                    line1_len = math.hypot(p2.x() - p1.x(), p2.y() - p1.y())
                    line2_len = math.hypot(p4.x() - p3.x(), p4.y() - p3.y())
                    length_px = max(line1_len, line2_len)
                    length_um = measurement.get("length_um")
                    if length_px > 0 and length_um:
                        mpp = float(length_um) / float(length_px)
                if mpp and mpp > 0:
                    uniform_length_px = float(uniform_length_um) / float(mpp)

            thumbnail = self.create_spore_thumbnail(
                pixmap,
                points,
                measurement['length_um'],
                measurement['width_um'] or 0,
                thumbnail_size,
                len(thumbnails) + 1,
                orient=orient,
                extra_rotation=extra_rotation,
                uniform_length_px=uniform_length_px,
                color=measure_color,
                rectangle_style=rectangle_style,
                rectangle_thickness=rectangle_thickness,
                selected=False,
                export_mode=True,
            )

            if thumbnail:
                thumbnails.append(thumbnail)

        if not thumbnails:
            return

        num_items = len(thumbnails)
        average_tile_width = int(round(sum(thumbnail.width() for thumbnail in thumbnails) / max(1, num_items)))
        tile_height = max(thumbnail.height() for thumbnail in thumbnails)
        items_per_row, num_rows = self._gallery_export_grid_shape(
            num_items,
            average_tile_width,
            tile_height,
        )

        spacing = 0
        row_widths = []
        for row in range(num_rows):
            row_items = thumbnails[row * items_per_row:(row + 1) * items_per_row]
            row_width = sum(thumbnail.width() for thumbnail in row_items)
            if row_items:
                row_width += max(0, len(row_items) - 1) * spacing
            row_widths.append(row_width)
        composite_width = max(row_widths) if row_widths else 0
        composite_height = num_rows * tile_height + (num_rows - 1) * spacing

        if export_format == "svg":
            from PySide6.QtSvg import QSvgGenerator
            from PySide6.QtCore import QRect, QSize as _QSize
            generator = QSvgGenerator()
            generator.setFileName(filename)
            generator.setSize(_QSize(composite_width, composite_height))
            generator.setViewBox(QRect(0, 0, composite_width, composite_height))
            painter = QPainter(generator)
            for idx, thumbnail in enumerate(thumbnails):
                row = idx // items_per_row
                x = sum(
                    thumbnails[row * items_per_row + col_index].width() + spacing
                    for col_index in range(idx % items_per_row)
                )
                y = row * (tile_height + spacing)
                painter.drawPixmap(x, y, thumbnail)
            painter.end()
        else:
            composite = QPixmap(composite_width, composite_height)
            composite.fill(QColor(255, 255, 255))
            painter = QPainter(composite)
            for idx, thumbnail in enumerate(thumbnails):
                row = idx // items_per_row
                x = sum(
                    thumbnails[row * items_per_row + col_index].width() + spacing
                    for col_index in range(idx % items_per_row)
                )
                y = row * (tile_height + spacing)
                painter.drawPixmap(x, y, thumbnail)
            painter.end()
            if export_format == "jpg":
                composite.save(filename, "JPEG", export_quality)
            else:
                composite.save(filename)

        self.measure_status_label.setText(f"\u2713 Gallery exported to {Path(filename).name}")
        self.measure_status_label.setStyleSheet(f"color: #27ae60; font-weight: bold; font-size: {pt(9)}pt;")

    def export_ml_dataset(self):
        """Trigger ML export from the observations tab."""
        if hasattr(self, "observations_tab"):
            self.observations_tab.export_for_ml()
            return
        QMessageBox.warning(
            self,
            "Export Unavailable",
            "The observations tab is not ready yet."
        )

    def open_settings_hub(self, page: int = 0):
        """Open the unified Settings hub dialog."""
        dialog = SettingsHubDialog(self, start_page=page)
        dialog.exec()
        # Propagate side-effects from embedded sub-dialogs
        if dialog.database_changed:
            self._populate_measure_categories()
        if dialog.publishing_changed:
            if hasattr(self, "observations_tab"):
                try:
                    self.observations_tab._build_publish_menu()
                    self.observations_tab._invalidate_publish_login_status_cache()
                    self.observations_tab._update_publish_controls()
                    self.observations_tab.refresh_observations(show_status=False)
                except Exception:
                    pass

    def open_profile_dialog(self):
        """Open profile settings dialog."""
        profile = SettingsDB.get_profile()
        dialog = QDialog(self)
        dialog.setWindowTitle(self.tr("Profile"))
        form = QFormLayout(dialog)
        info_label = QLabel(
            self.tr(
                "Name is used for the copyright watermark on images.\n"
                "Name and email (optional) are added to observations in the database, "
                "useful if you share your observations with others."
            )
        )
        info_label.setWordWrap(True)
        name_input = QLineEdit(profile.get("name", ""))
        email_input = QLineEdit(profile.get("email", ""))
        form.addRow(info_label)
        form.addRow(self.tr("Name"), name_input)
        form.addRow(self.tr("Email"), email_input)

        buttons = QDialogButtonBox(dialog)
        ok_btn = buttons.addButton(self.tr("OK"), QDialogButtonBox.AcceptRole)
        cancel_btn = buttons.addButton(self.tr("Cancel"), QDialogButtonBox.RejectRole)
        ok_btn.clicked.connect(dialog.accept)
        cancel_btn.clicked.connect(dialog.reject)
        form.addRow(buttons)
        dialog.resize(560, dialog.sizeHint().height())

        if dialog.exec() == QDialog.Accepted:
            SettingsDB.set_profile(name_input.text().strip(), email_input.text().strip())
            self._update_measure_copyright_overlay()

    def open_cloud_sync_dialog(self):
        """Open Sporely Cloud Sync dialog."""
        dialog = CloudSyncDialog(
            self,
            prepare_images_cb=getattr(self, "prepare_cloud_sync_image_uploads", None),
        )
        dialog.exec()

    def prepare_cloud_sync_image_uploads(self, observation: dict, progress_cb=None):
        if not hasattr(self, "observations_tab") or self.observations_tab is None:
            return [], None, []
        try:
            return self.observations_tab.prepare_cloud_sync_image_uploads(
                observation,
                progress_cb=progress_cb,
            )
        except Exception as exc:
            return [], None, [str(exc)]

    def open_database_settings_dialog(self):
        """Open database settings dialog."""
        dialog = DatabaseSettingsDialog(self)
        if dialog.exec() == QDialog.Accepted:
            self._populate_measure_categories()

    def open_artsobservasjoner_settings_dialog(self):
        """Open online publishing settings dialog."""
        dialog = ArtsobservasjonerSettingsDialog(self)
        dialog.exec()
        if hasattr(self, "observations_tab"):
            try:
                self.observations_tab._build_publish_menu()
                self.observations_tab._invalidate_publish_login_status_cache()
                self.observations_tab._update_publish_controls()
                self.observations_tab.refresh_observations(show_status=False)
            except Exception:
                pass

    def open_language_settings_dialog(self):
        """Open language settings dialog."""
        dialog = LanguageSettingsDialog(self)
        dialog.exec()

    def open_appearance_dialog(self):
        """Open appearance (theme) settings dialog."""
        dialog = AppearanceDialog(self)
        dialog.exec()

    def _set_ui_theme(self, theme: str) -> None:
        SettingsDB.set_setting("ui_theme", theme)
        self._apply_theme()

    def _sync_appearance_menu(self) -> None:
        actions = getattr(self, "_appearance_actions", None)
        if not actions:
            return
        theme = SettingsDB.get_setting("ui_theme", "auto")
        if theme not in actions:
            theme = "auto"
        actions[theme].setChecked(True)

    def _apply_theme(self):
        """Read the stored theme preference and apply palette + stylesheet."""
        theme = SettingsDB.get_setting("ui_theme", "auto")
        apply_palette(theme)
        self.setStyleSheet(get_style(theme))
        self._sync_appearance_menu()
        if hasattr(self, "gallery_plot_figure") and hasattr(self, "gallery_plot_canvas"):
            self.update_graph_plots_only()

    def _on_system_color_scheme_changed(self):
        if SettingsDB.get_setting("ui_theme", "auto") == "auto":
            self._apply_theme()

    def apply_vernacular_language_change(self):
        if hasattr(self, "observations_tab"):
            self.observations_tab.apply_vernacular_language_change()
        if hasattr(self, "ref_vernacular_label"):
            self.ref_vernacular_label.setText(self._reference_vernacular_label())
        if hasattr(self, "ref_vernacular_input"):
            lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
            db_path = resolve_vernacular_db_path(lang)
            if db_path:
                self.ref_vernacular_db = VernacularDB(db_path, language_code=lang)
            if not self.ref_vernacular_input.text().strip():
                self._set_ref_vernacular_placeholder_from_suggestions([])
                self._update_ref_vernacular_suggestions_for_taxon()
        if hasattr(self, "ref_species_input"):
            genus = self._clean_ref_genus_text(self.ref_genus_input.text()) if hasattr(self, "ref_genus_input") else ""
            if genus and not self._clean_ref_species_text(self.ref_species_input.text()):
                species_suggestions = self._update_ref_species_suggestions(genus, "")
                self._set_ref_species_placeholder_from_suggestions(species_suggestions)
            elif not self._clean_ref_species_text(self.ref_species_input.text()):
                self._set_ref_species_placeholder_from_suggestions([])
        for widget in QApplication.topLevelWidgets():
            if widget is self:
                continue
            if hasattr(widget, "apply_vernacular_language_change"):
                try:
                    widget.apply_vernacular_language_change()
                except Exception:
                    pass

    def set_ui_language(self, code):
        """Persist the UI language setting."""
        SettingsDB.set_setting("ui_language", code)
        update_app_settings({"ui_language": code})

    def _populate_measure_categories(self):
        setting_key = DatabaseTerms.setting_key("measure")
        defaults = DatabaseTerms.default_values("measure")
        saved_categories = SettingsDB.get_list_setting(setting_key, defaults)
        categories = DatabaseTerms.canonicalize_list("measure", saved_categories)
        categories = list(categories)
        changed = categories != saved_categories
        for default_category in defaults:
            if default_category not in categories:
                categories.append(default_category)
                changed = True
        filtered_categories = []
        for category in categories:
            normalized_category = self.normalize_measurement_category(category)
            if normalized_category == "calibration":
                changed = True
                continue
            filtered_categories.append(category)
        categories = filtered_categories
        if changed:
            SettingsDB.set_list_setting(setting_key, categories)
        if not hasattr(self, "measure_category_combo"):
            return
        current = self.measure_category_combo.currentData()
        self.measure_category_combo.blockSignals(True)
        self.measure_category_combo.clear()
        for category in categories:
            if not category:
                continue
            normalized_category = self.normalize_measurement_category(category)
            self.measure_category_combo.addItem(
                self.format_measurement_category(category),
                normalized_category,
            )
        self.measure_category_combo.blockSignals(False)
        if current:
            idx = self.measure_category_combo.findData(current)
            if idx >= 0:
                self.measure_category_combo.setCurrentIndex(idx)

    def on_gallery_thumbnail_setting_changed(self):
        """Persist gallery settings and refresh thumbnails."""
        if hasattr(self, "ref_source_input"):
            self._populate_reference_panel_sources()
        sender = self.sender()
        hint = self.tr("Refreshing spore plot and gallery...")
        if sender is getattr(self, "gallery_sort_combo", None):
            hint = self.tr("Resorting spore thumbnails...")
        elif sender is getattr(self, "orient_checkbox", None):
            hint = self.tr("Rotating spore thumbnails...")
        elif sender is getattr(self, "uniform_scale_checkbox", None):
            hint = self.tr("Rescaling spore thumbnails...")
        elif sender is getattr(self, "gallery_filter_combo", None):
            hint = self.tr("Filtering measurements and refreshing gallery...")
        self._queue_gallery_refresh_hint(hint)
        self._save_gallery_settings()
        self.schedule_gallery_refresh()

    def on_gallery_plot_setting_changed(self):
        """Persist gallery settings and refresh plots only."""
        self._queue_gallery_refresh_hint(self.tr("Updating spore plot..."))
        plot_style = self._gallery_plot_style_from_controls()
        self.gallery_plot_settings = self._apply_gallery_plot_style({
            "bins": int(self.gallery_bins_spin.value()) if hasattr(self, "gallery_bins_spin") else 8,
            "histogram": bool(self.gallery_hist_checkbox.isChecked()) if hasattr(self, "gallery_hist_checkbox") else True,
            "ellipse_coverage_percent": int(self.gallery_ellipse_coverage_slider.value()) if hasattr(self, "gallery_ellipse_coverage_slider") else int(getattr(self, "gallery_plot_settings", {}).get("ellipse_coverage_percent", 95)),
            "kde_bandwidth": self._gallery_kde_bandwidth_value() if hasattr(self, "gallery_kde_bandwidth_slider") else float(getattr(self, "gallery_plot_settings", {}).get("kde_bandwidth", 1.0)),
            "kde_contours": int(self.gallery_kde_contours_slider.value()) if hasattr(self, "gallery_kde_contours_slider") else int(getattr(self, "gallery_plot_settings", {}).get("kde_contours", 6)),
            "kde_coverage_percent": int(self.gallery_kde_coverage_slider.value()) if hasattr(self, "gallery_kde_coverage_slider") else int(getattr(self, "gallery_plot_settings", {}).get("kde_coverage_percent", 95)),
            "reference_minmax": bool(self.ref_show_minmax_checkbox.isChecked()) if hasattr(self, "ref_show_minmax_checkbox") else bool(getattr(self, "gallery_plot_settings", {}).get("reference_minmax", True)),
            "reference_shape": (
                "square"
                if hasattr(self, "ref_shape_square_radio") and self.ref_shape_square_radio.isChecked()
                else "ellipse"
            ),
            "image_color": bool(self.gallery_image_color_checkbox.isChecked()) if hasattr(self, "gallery_image_color_checkbox") else False,
            "legend": bool(getattr(self, "gallery_plot_settings", {}).get("legend", False)),
            "avg_q": bool(self.gallery_avg_q_checkbox.isChecked()) if hasattr(self, "gallery_avg_q_checkbox") else False,
            "q_minmax": bool(self.gallery_q_minmax_checkbox.isChecked()) if hasattr(self, "gallery_q_minmax_checkbox") else False,
            "q_extreme_minmax": bool(self.gallery_q_extreme_minmax_checkbox.isChecked()) if hasattr(self, "gallery_q_extreme_minmax_checkbox") else False,
            "axis_equal": bool(self.gallery_axis_equal_checkbox.isChecked()) if hasattr(self, "gallery_axis_equal_checkbox") else False,
            "x_min": None,
            "x_max": None,
            "y_min": None,
            "y_max": None,
        }, plot_style)
        self._sync_gallery_histogram_controls()
        self._sync_gallery_kde_controls()
        self._sync_reference_overlay_controls_state()
        self._save_gallery_settings()
        self.update_graph_plots_only()

    def on_reference_overlay_setting_changed(self):
        """Persist reference overlay visibility and refresh plots."""
        self._queue_gallery_refresh_hint(self.tr("Updating reference overlays..."))
        settings = dict(getattr(self, "gallery_plot_settings", {}) or {})
        settings["reference_minmax"] = bool(self.ref_show_minmax_checkbox.isChecked()) if hasattr(self, "ref_show_minmax_checkbox") else True
        settings["reference_shape"] = (
            "square"
            if hasattr(self, "ref_shape_square_radio") and self.ref_shape_square_radio.isChecked()
            else "ellipse"
        )
        self.gallery_plot_settings = settings
        self._sync_reference_overlay_controls_state()
        self._save_gallery_settings()
        self.update_graph_plots_only()

    def _sync_reference_overlay_controls_state(self) -> None:
        range_mode = self._gallery_plot_style() != "mean"
        for widget_name in ("ref_show_minmax_checkbox", "ref_shape_ellipse_radio", "ref_shape_square_radio"):
            widget = getattr(self, widget_name, None)
            if widget is not None:
                widget.setEnabled(range_mode)

    def on_gallery_stats_setting_changed(self):
        """Persist stats-view settings and refresh the text preview."""
        self._save_gallery_settings()
        self._update_gallery_stats_preview()

    def _collect_reference_panel_state(self) -> dict:
        if not hasattr(self, "ref_genus_input"):
            return {}
        return {
            "vernacular": self.ref_vernacular_input.text().strip() if hasattr(self, "ref_vernacular_input") else "",
            "genus": self._clean_ref_genus_text(self.ref_genus_input.text()) if hasattr(self, "ref_genus_input") else "",
            "species": self._clean_ref_species_text(self.ref_species_input.text()) if hasattr(self, "ref_species_input") else "",
            "source": self.ref_source_input.currentText().strip() if hasattr(self, "ref_source_input") else "",
        }

    def _serialize_reference_data_for_settings(self, data: dict | None) -> dict:
        if not isinstance(data, dict) or not data:
            return {}
        serialized: dict = {}
        for key, value in data.items():
            if key == "points":
                continue
            if isinstance(value, np.generic):
                value = value.item()
            if isinstance(value, (str, int, float, bool)) or value is None:
                serialized[key] = value
            elif isinstance(value, (list, tuple)):
                simple = []
                ok = True
                for item in value:
                    if isinstance(item, np.generic):
                        item = item.item()
                    if isinstance(item, (str, int, float, bool)) or item is None:
                        simple.append(item)
                    else:
                        ok = False
                        break
                if ok:
                    serialized[key] = simple
        if "kind" in serialized and "source_kind" not in serialized:
            serialized["source_kind"] = serialized.get("kind")
        return serialized

    def _restore_reference_data_from_settings(self, saved: dict | None) -> dict | None:
        if not isinstance(saved, dict) or not saved:
            return None
        data = dict(saved)
        kind = (data.get("source_kind") or data.get("kind") or "").strip()
        if kind:
            data["source_kind"] = kind
        genus = (data.get("genus") or "").strip()
        species = (data.get("species") or "").strip()
        if not genus or not species:
            return data if data else None

        if kind == "points":
            source_type = (data.get("source_type") or "personal").strip().lower()
            if source_type not in {"personal", "shared", "published"}:
                source_type = "personal"
            exclude_id = self.active_observation_id if hasattr(self, "active_observation_id") else None
            points = MeasurementDB.get_measurements_for_species(
                genus,
                species,
                source_type=source_type,
                measurement_category="spores",
                exclude_observation_id=exclude_id,
            )
            if not points:
                return None
            stats = self._reference_stats_from_points(points)
            return {
                **stats,
                "points": points,
                "points_label": (data.get("points_label") or data.get("source_label") or ""),
                "plot_color": data.get("plot_color"),
                "source_kind": "points",
                "source_type": source_type,
                "genus": genus,
                "species": species,
            }

        if kind == "observation":
            try:
                obs_id = int(data.get("observation_id") or 0)
            except (TypeError, ValueError):
                obs_id = 0
            if not obs_id:
                return None
            raw = MeasurementDB.get_measurements_for_observation(obs_id)
            points = [
                m for m in raw
                if m.get("length_um") is not None
                and m.get("width_um") is not None
                and (m.get("measurement_type") in (None, "", "manual", "spore", "spores"))
            ]
            if not points:
                return None
            stats = self._reference_stats_from_points(points)
            author = (data.get("author") or "").strip()
            if not author:
                obs = ObservationDB.get_observation(obs_id)
                if obs:
                    author = (obs.get("author") or "").strip()
            return {
                **stats,
                "points": points,
                "plot_color": data.get("plot_color"),
                "source_kind": "observation",
                "observation_id": obs_id,
                "date": data.get("date") or "",
                "author": author,
                "genus": genus,
                "species": species,
            }

        if kind == "reference":
            source = (data.get("source") or "").strip() or None
            ref = ReferenceDB.get_reference(genus, species, source)
            if ref:
                ref["source_kind"] = "reference"
                if "plot_color" in data:
                    ref["plot_color"] = data.get("plot_color")
                return ref
        return data

    def _apply_saved_reference_state(self, settings: dict) -> None:
        if not isinstance(settings, dict):
            return
        panel_state = settings.get("reference_panel")
        if isinstance(panel_state, dict) and hasattr(self, "ref_genus_input"):
            self.ref_vernacular_input.blockSignals(True)
            self.ref_genus_input.blockSignals(True)
            self.ref_species_input.blockSignals(True)
            self.ref_source_input.blockSignals(True)
            self.ref_vernacular_input.setText((panel_state.get("vernacular") or "").strip())
            self.ref_genus_input.setText((panel_state.get("genus") or "").strip())
            self.ref_species_input.setText((panel_state.get("species") or "").strip())
            self.ref_source_input.blockSignals(False)
            self.ref_species_input.blockSignals(False)
            self.ref_genus_input.blockSignals(False)
            self.ref_vernacular_input.blockSignals(False)
            self._populate_reference_panel_sources()
            source = (panel_state.get("source") or "").strip()
            if source:
                idx = self.ref_source_input.findText(source)
                if idx >= 0:
                    self.ref_source_input.setCurrentIndex(idx)
                else:
                    self.ref_source_input.setCurrentText(source)

        restored_series = []
        saved_series = settings.get("reference_series")
        if isinstance(saved_series, list):
            for item in saved_series:
                enabled = True
                saved_item = item
                if isinstance(item, dict) and isinstance(item.get("data"), dict):
                    enabled = bool(item.get("enabled", True))
                    saved_item = item.get("data")
                restored = self._restore_reference_data_from_settings(saved_item)
                if isinstance(restored, dict) and restored:
                    restored_series.append({
                        "data": restored,
                        "enabled": enabled,
                    })

        if restored_series:
            last_entry = restored_series[-1]
            self.reference_values = dict(last_entry.get("data", {}))
            self._set_reference_series(restored_series)
            self._apply_reference_panel_values(self.reference_values)
            self._update_reference_add_state()
            return

        restored_values = self._restore_reference_data_from_settings(settings.get("reference_values"))
        if isinstance(restored_values, dict) and restored_values:
            self.reference_values = restored_values
            self._set_reference_series([restored_values])
            self._apply_reference_panel_values(restored_values)
            self._update_reference_add_state()

    def open_gallery_plot_settings(self):
        """Open plot settings dialog for analysis charts."""
        dialog = QDialog(self)
        dialog.setWindowTitle(self.tr("Plot settings"))
        dialog.setModal(True)

        layout = QFormLayout(dialog)
        layout.setLabelAlignment(Qt.AlignLeft)

        settings = getattr(self, "gallery_plot_settings", {}) or {}

        bins_spin = QSpinBox()
        bins_spin.setRange(3, 50)
        bins_spin.setValue(int(settings.get("bins", 8)))
        layout.addRow(self.tr("Bins:"), bins_spin)

        plot_style_row = QWidget()
        plot_style_layout = QHBoxLayout(plot_style_row)
        plot_style_layout.setContentsMargins(0, 0, 0, 0)
        plot_style_layout.setSpacing(10)
        plot_style_layout.addWidget(QLabel(self.tr("Plot:")))
        plot_style_group = QButtonGroup(dialog)
        plot_style_ellipse_radio = QRadioButton(self.tr("Ellipse"))
        plot_style_kde_radio = QRadioButton(self.tr("Kernel density"))
        plot_style_mean_radio = QRadioButton(self.tr("Mean range"))
        plot_style_ellipse_radio.setToolTip(
            self.tr("Show data ellipses for the current specimen and any spore-point reference sets. Coverage is set by the slider below.")
        )
        plot_style_kde_radio.setToolTip(
            self.tr("Show Gaussian KDE filled density bands for the measured spore distribution.")
        )
        plot_style_mean_radio.setToolTip(
            self.tr("Parmasto-style mean comparison with mean point, mean Q line, and expected mean range.")
        )
        for radio in (plot_style_ellipse_radio, plot_style_kde_radio, plot_style_mean_radio):
            plot_style_group.addButton(radio)
            plot_style_layout.addWidget(radio)
        plot_style_layout.addStretch()
        layout.addRow("", plot_style_row)

        plot_style = self._gallery_plot_style(settings)
        if plot_style == "kde":
            plot_style_kde_radio.setChecked(True)
        elif plot_style == "mean":
            plot_style_mean_radio.setChecked(True)
        else:
            plot_style_ellipse_radio.setChecked(True)

        ellipse_coverage_row = QWidget()
        ellipse_coverage_layout = QHBoxLayout(ellipse_coverage_row)
        ellipse_coverage_layout.setContentsMargins(0, 0, 0, 0)
        ellipse_coverage_layout.setSpacing(6)
        ellipse_coverage_layout.addSpacing(16)
        ellipse_coverage_layout.addWidget(QLabel(self.tr("Coverage:")))
        ellipse_coverage_slider = JumpSlider(Qt.Horizontal)
        ellipse_coverage_slider.setRange(50, 99)
        ellipse_coverage_slider.setSingleStep(1)
        ellipse_coverage_slider.setPageStep(5)
        ellipse_coverage_slider.setValue(int(settings.get("ellipse_coverage_percent", 95)))
        ellipse_coverage_layout.addWidget(ellipse_coverage_slider, 1)
        ellipse_coverage_value = QLabel()
        ellipse_coverage_value.setMinimumWidth(40)
        ellipse_coverage_layout.addWidget(ellipse_coverage_value)
        layout.addRow("", ellipse_coverage_row)

        kde_bandwidth_row = QWidget()
        kde_bandwidth_layout = QHBoxLayout(kde_bandwidth_row)
        kde_bandwidth_layout.setContentsMargins(0, 0, 0, 0)
        kde_bandwidth_layout.setSpacing(8)
        kde_bandwidth_layout.addSpacing(16)
        kde_bandwidth_layout.addWidget(QLabel(self.tr("Bandwidth:")))
        kde_bandwidth_slider = JumpSlider(Qt.Horizontal)
        kde_bandwidth_slider.setRange(50, 150)
        kde_bandwidth_slider.setSingleStep(1)
        kde_bandwidth_slider.setPageStep(5)
        kde_bandwidth_slider.setValue(
            int(round(max(0.5, min(1.5, float(settings.get("kde_bandwidth", 1.0)))) * 100.0))
        )
        kde_bandwidth_layout.addWidget(kde_bandwidth_slider, 1)
        kde_bandwidth_value = QLabel()
        kde_bandwidth_value.setMinimumWidth(36)
        kde_bandwidth_layout.addWidget(kde_bandwidth_value)
        layout.addRow("", kde_bandwidth_row)

        kde_contours_row = QWidget()
        kde_contours_layout = QHBoxLayout(kde_contours_row)
        kde_contours_layout.setContentsMargins(0, 0, 0, 0)
        kde_contours_layout.setSpacing(6)
        kde_contours_layout.addSpacing(16)
        kde_contours_layout.addWidget(QLabel(self.tr("Contours:")))
        kde_contours_slider = JumpSlider(Qt.Horizontal)
        kde_contours_slider.setRange(1, 10)
        kde_contours_slider.setSingleStep(1)
        kde_contours_slider.setPageStep(1)
        kde_contours_slider.setValue(int(settings.get("kde_contours", 6)))
        kde_contours_layout.addWidget(kde_contours_slider, 1)
        kde_contours_value = QLabel()
        kde_contours_value.setMinimumWidth(24)
        kde_contours_layout.addWidget(kde_contours_value)
        layout.addRow("", kde_contours_row)

        kde_coverage_row = QWidget()
        kde_coverage_layout = QHBoxLayout(kde_coverage_row)
        kde_coverage_layout.setContentsMargins(0, 0, 0, 0)
        kde_coverage_layout.setSpacing(6)
        kde_coverage_layout.addSpacing(16)
        kde_coverage_layout.addWidget(QLabel(self.tr("Coverage:")))
        kde_coverage_slider = JumpSlider(Qt.Horizontal)
        kde_coverage_slider.setRange(50, 99)
        kde_coverage_slider.setSingleStep(1)
        kde_coverage_slider.setPageStep(5)
        kde_coverage_slider.setValue(int(settings.get("kde_coverage_percent", 95)))
        kde_coverage_layout.addWidget(kde_coverage_slider, 1)
        kde_coverage_value = QLabel()
        kde_coverage_value.setMinimumWidth(40)
        kde_coverage_layout.addWidget(kde_coverage_value)
        layout.addRow("", kde_coverage_row)

        def _sync_dialog_kde_controls() -> None:
            ellipse_enabled = bool(plot_style_ellipse_radio.isChecked())
            ellipse_coverage_row.setVisible(ellipse_enabled)
            ellipse_coverage_value.setText(f"{int(ellipse_coverage_slider.value())}%")
            bandwidth = max(0.5, min(1.5, float(kde_bandwidth_slider.value()) / 100.0))
            kde_bandwidth_value.setText(f"{bandwidth:.2f}")
            kde_enabled = bool(plot_style_kde_radio.isChecked())
            kde_bandwidth_row.setVisible(kde_enabled)
            kde_contours_row.setVisible(kde_enabled)
            kde_contours_value.setText(str(int(kde_contours_slider.value())))
            kde_coverage_row.setVisible(kde_enabled)
            kde_coverage_value.setText(f"{int(kde_coverage_slider.value())}%")
            reference_shape_row.setEnabled(not bool(plot_style_mean_radio.isChecked()))

        plot_style_ellipse_radio.toggled.connect(_sync_dialog_kde_controls)
        plot_style_kde_radio.toggled.connect(_sync_dialog_kde_controls)
        plot_style_mean_radio.toggled.connect(_sync_dialog_kde_controls)
        ellipse_coverage_slider.valueChanged.connect(_sync_dialog_kde_controls)
        kde_bandwidth_slider.valueChanged.connect(_sync_dialog_kde_controls)
        kde_contours_slider.valueChanged.connect(_sync_dialog_kde_controls)
        kde_coverage_slider.valueChanged.connect(_sync_dialog_kde_controls)

        image_color_checkbox = QCheckBox(self.tr("Image color"))
        image_color_checkbox.setChecked(bool(settings.get("image_color", False)))
        layout.addRow("", image_color_checkbox)

        avg_q_checkbox = QCheckBox(self.tr("Plot Avg Q"))
        avg_q_checkbox.setChecked(bool(settings.get("avg_q", False)))
        layout.addRow("", avg_q_checkbox)

        q_minmax_checkbox = QCheckBox(self.tr("Plot Q 90% range (5%-95%)"))
        q_minmax_checkbox.setToolTip(self.tr("Show Q lines for the 5th to 95th percentile range"))
        q_minmax_checkbox.setChecked(bool(settings.get("q_minmax", False)))
        layout.addRow("", q_minmax_checkbox)

        q_extreme_minmax_checkbox = QCheckBox(self.tr("Plot Q min/max"))
        q_extreme_minmax_checkbox.setToolTip(self.tr("Show Q lines for the true minimum and maximum values"))
        q_extreme_minmax_checkbox.setChecked(bool(settings.get("q_extreme_minmax", False)))
        layout.addRow("", q_extreme_minmax_checkbox)

        reference_shape_row = QWidget()
        reference_shape_layout = QHBoxLayout(reference_shape_row)
        reference_shape_layout.setContentsMargins(0, 0, 0, 0)
        reference_shape_layout.setSpacing(8)
        reference_shape_layout.addWidget(QLabel(self.tr("Reference shape:")))
        reference_shape_group = QButtonGroup(dialog)
        reference_shape_ellipse_radio = QRadioButton(self.tr("Ellipse"))
        reference_shape_square_radio = QRadioButton(self.tr("Square"))
        reference_shape_group.addButton(reference_shape_ellipse_radio)
        reference_shape_group.addButton(reference_shape_square_radio)
        if str(settings.get("reference_shape", "ellipse") or "ellipse").strip().lower() == "square":
            reference_shape_square_radio.setChecked(True)
        else:
            reference_shape_ellipse_radio.setChecked(True)
        reference_shape_layout.addWidget(reference_shape_ellipse_radio)
        reference_shape_layout.addWidget(reference_shape_square_radio)
        reference_shape_layout.addStretch()
        layout.addRow("", reference_shape_row)
        _sync_dialog_kde_controls()

        axis_equal_checkbox = QCheckBox(self.tr("Axis equal"))
        axis_equal_checkbox.setToolTip(self.tr("Use the same scale on X and Y axes"))
        axis_equal_checkbox.setChecked(bool(settings.get("axis_equal", False)))
        layout.addRow("", axis_equal_checkbox)

        buttons = QDialogButtonBox(dialog)
        ok_btn = buttons.addButton(self.tr("OK"), QDialogButtonBox.AcceptRole)
        cancel_btn = buttons.addButton(self.tr("Cancel"), QDialogButtonBox.RejectRole)
        ok_btn.clicked.connect(dialog.accept)
        cancel_btn.clicked.connect(dialog.reject)
        layout.addRow(buttons)

        if dialog.exec() != QDialog.Accepted:
            return

        new_settings = {
            "bins": int(bins_spin.value()),
            "histogram": bool(settings.get("histogram", self.gallery_plot_settings.get("histogram", True))),
            "ellipse_coverage_percent": int(ellipse_coverage_slider.value()),
            "kde_bandwidth": max(0.5, min(1.5, float(kde_bandwidth_slider.value()) / 100.0)),
            "kde_contours": int(kde_contours_slider.value()),
            "kde_coverage_percent": int(kde_coverage_slider.value()),
            "reference_minmax": bool(settings.get("reference_minmax", self.gallery_plot_settings.get("reference_minmax", True))),
            "reference_shape": "square" if reference_shape_square_radio.isChecked() else "ellipse",
            "image_color": bool(image_color_checkbox.isChecked()),
            "legend": bool(settings.get("legend", False)),
            "avg_q": bool(avg_q_checkbox.isChecked()),
            "q_minmax": bool(q_minmax_checkbox.isChecked()),
            "q_extreme_minmax": bool(q_extreme_minmax_checkbox.isChecked()),
            "axis_equal": bool(axis_equal_checkbox.isChecked()),
            "x_min": None,
            "x_max": None,
            "y_min": None,
            "y_max": None,
        }
        selected_plot_style = "ellipse"
        if plot_style_kde_radio.isChecked():
            selected_plot_style = "kde"
        elif plot_style_mean_radio.isChecked():
            selected_plot_style = "mean"
        self.gallery_plot_settings = self._apply_gallery_plot_style(new_settings, selected_plot_style)

        self._save_gallery_settings()
        self.update_graph_plots_only()

    def _gallery_settings_key(self):
        if not self.active_observation_id:
            return None
        return f"gallery_settings_{self.active_observation_id}"

    def _load_gallery_settings(self):
        key = self._gallery_settings_key()
        if not key:
            return {}
        raw = SettingsDB.get_setting(key)
        if not raw:
            return {}
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return data if isinstance(data, dict) else {}

    def _collect_gallery_settings(self):
        plot_settings = getattr(self, "gallery_plot_settings", {}) or {}
        serialized_reference_series = []
        for entry in self.reference_series or []:
            data = entry.get("data", entry) if isinstance(entry, dict) else entry
            serialized = self._serialize_reference_data_for_settings(data)
            if serialized:
                serialized_reference_series.append({
                    "enabled": bool(entry.get("enabled", True)) if isinstance(entry, dict) else True,
                    "data": serialized,
                })
        return {
            "measurement_type": self.gallery_filter_combo.currentData() if hasattr(self, "gallery_filter_combo") else None,
            "bins": int(plot_settings.get("bins", 8)),
            "histogram": bool(plot_settings.get("histogram", True)),
            "plot_style": self._gallery_plot_style(plot_settings),
            "ellipse_coverage_percent": int(plot_settings.get("ellipse_coverage_percent", 95)),
            "ci": bool(plot_settings.get("ci", True)),
            "kde": bool(plot_settings.get("kde", False)),
            "kde_bandwidth": float(plot_settings.get("kde_bandwidth", 1.0)),
            "kde_contours": int(plot_settings.get("kde_contours", 6)),
            "kde_coverage_percent": int(plot_settings.get("kde_coverage_percent", 95)),
            "comparison_mode": str(plot_settings.get("comparison_mode", "range") or "range"),
            "reference_minmax": bool(plot_settings.get("reference_minmax", True)),
            "reference_ci": bool(plot_settings.get("reference_ci", True)),
            "reference_shape": str(plot_settings.get("reference_shape", "ellipse") or "ellipse"),
            "image_color": bool(plot_settings.get("image_color", False)),
            "legend": bool(plot_settings.get("legend", False)),
            "avg_q": bool(plot_settings.get("avg_q", True)),
            "q_minmax": bool(plot_settings.get("q_minmax", True)),
            "q_extreme_minmax": bool(plot_settings.get("q_extreme_minmax", False)),
            "axis_equal": bool(plot_settings.get("axis_equal", False)),
            "x_min": None,
            "x_max": None,
            "y_min": None,
            "y_max": None,
            "orient": bool(self.orient_checkbox.isChecked()) if hasattr(self, "orient_checkbox") else False,
            "uniform_scale": bool(self.uniform_scale_checkbox.isChecked()) if hasattr(self, "uniform_scale_checkbox") else False,
            "gallery_sort": self.gallery_sort_combo.currentData() if hasattr(self, "gallery_sort_combo") else "",
            "include_details": bool(self.gallery_include_details_checkbox.isChecked()) if hasattr(self, "gallery_include_details_checkbox") else False,
            "reference_panel": self._collect_reference_panel_state(),
            "reference_values": self._serialize_reference_data_for_settings(self.reference_values),
            "reference_series": serialized_reference_series,
            "plot_width_splitter_sizes": self.plot_width_splitter.sizes() if hasattr(self, "plot_width_splitter") else [],
        }

    def _save_gallery_settings(self):
        key = self._gallery_settings_key()
        if not key:
            return
        settings = self._collect_gallery_settings()
        SettingsDB.set_setting(key, json.dumps(settings))

    def apply_gallery_settings(self):
        settings = self._load_gallery_settings()
        if not settings:
            return
        loaded_plot_settings = {
            "bins": int(settings.get("bins", self.gallery_plot_settings.get("bins", 8))),
            "histogram": bool(settings.get("histogram", self.gallery_plot_settings.get("histogram", True))),
            "ellipse_coverage_percent": int(settings.get("ellipse_coverage_percent", self.gallery_plot_settings.get("ellipse_coverage_percent", 95))),
            "kde_bandwidth": float(settings.get("kde_bandwidth", self.gallery_plot_settings.get("kde_bandwidth", 1.0))),
            "kde_contours": int(settings.get("kde_contours", self.gallery_plot_settings.get("kde_contours", 6))),
            "kde_coverage_percent": int(settings.get("kde_coverage_percent", self.gallery_plot_settings.get("kde_coverage_percent", 95))),
            "reference_minmax": bool(settings.get("reference_minmax", self.gallery_plot_settings.get("reference_minmax", True))),
            "reference_shape": str(settings.get("reference_shape", self.gallery_plot_settings.get("reference_shape", "ellipse")) or "ellipse"),
            "image_color": bool(settings.get("image_color", self.gallery_plot_settings.get("image_color", False))),
            "legend": bool(settings.get("legend", self.gallery_plot_settings.get("legend", False))),
            "avg_q": bool(settings.get("avg_q", self.gallery_plot_settings.get("avg_q", False))),
            "q_minmax": bool(settings.get("q_minmax", self.gallery_plot_settings.get("q_minmax", False))),
            "q_extreme_minmax": bool(settings.get("q_extreme_minmax", self.gallery_plot_settings.get("q_extreme_minmax", False))),
            "axis_equal": bool(settings.get("axis_equal", self.gallery_plot_settings.get("axis_equal", False))),
            "x_min": None,
            "x_max": None,
            "y_min": None,
            "y_max": None,
        }
        plot_style = self._gallery_plot_style({
            **self.gallery_plot_settings,
            **settings,
        })
        self.gallery_plot_settings = self._apply_gallery_plot_style(loaded_plot_settings, plot_style)
        if hasattr(self, "gallery_bins_spin"):
            self.gallery_bins_spin.blockSignals(True)
            self.gallery_bins_spin.setValue(int(self.gallery_plot_settings.get("bins", 8)))
            self.gallery_bins_spin.blockSignals(False)
        if hasattr(self, "gallery_hist_checkbox"):
            self.gallery_hist_checkbox.blockSignals(True)
            self.gallery_hist_checkbox.setChecked(bool(self.gallery_plot_settings.get("histogram", True)))
            self.gallery_hist_checkbox.blockSignals(False)
        plot_style = self._gallery_plot_style(self.gallery_plot_settings)
        if hasattr(self, "gallery_plot_style_ellipse_radio") and hasattr(self, "gallery_plot_style_kde_radio") and hasattr(self, "gallery_plot_style_mean_radio"):
            self.gallery_plot_style_ellipse_radio.blockSignals(True)
            self.gallery_plot_style_kde_radio.blockSignals(True)
            self.gallery_plot_style_mean_radio.blockSignals(True)
            self.gallery_plot_style_ellipse_radio.setChecked(plot_style == "ellipse")
            self.gallery_plot_style_kde_radio.setChecked(plot_style == "kde")
            self.gallery_plot_style_mean_radio.setChecked(plot_style == "mean")
            self.gallery_plot_style_mean_radio.blockSignals(False)
            self.gallery_plot_style_kde_radio.blockSignals(False)
            self.gallery_plot_style_ellipse_radio.blockSignals(False)
        if hasattr(self, "gallery_ellipse_coverage_slider"):
            self.gallery_ellipse_coverage_slider.blockSignals(True)
            self.gallery_ellipse_coverage_slider.setValue(int(self.gallery_plot_settings.get("ellipse_coverage_percent", 95)))
            self.gallery_ellipse_coverage_slider.blockSignals(False)
        if hasattr(self, "gallery_kde_bandwidth_slider"):
            self.gallery_kde_bandwidth_slider.blockSignals(True)
            self.gallery_kde_bandwidth_slider.setValue(
                int(round(max(0.5, min(1.5, float(self.gallery_plot_settings.get("kde_bandwidth", 1.0)))) * 100.0))
            )
            self.gallery_kde_bandwidth_slider.blockSignals(False)
        if hasattr(self, "gallery_kde_contours_slider"):
            self.gallery_kde_contours_slider.blockSignals(True)
            self.gallery_kde_contours_slider.setValue(int(self.gallery_plot_settings.get("kde_contours", 6)))
            self.gallery_kde_contours_slider.blockSignals(False)
        if hasattr(self, "gallery_kde_coverage_slider"):
            self.gallery_kde_coverage_slider.blockSignals(True)
            self.gallery_kde_coverage_slider.setValue(int(self.gallery_plot_settings.get("kde_coverage_percent", 95)))
            self.gallery_kde_coverage_slider.blockSignals(False)
        if hasattr(self, "ref_show_minmax_checkbox"):
            self.ref_show_minmax_checkbox.blockSignals(True)
            self.ref_show_minmax_checkbox.setChecked(bool(self.gallery_plot_settings.get("reference_minmax", True)))
            self.ref_show_minmax_checkbox.blockSignals(False)
        if hasattr(self, "ref_shape_ellipse_radio") and hasattr(self, "ref_shape_square_radio"):
            reference_shape = str(self.gallery_plot_settings.get("reference_shape", "ellipse") or "ellipse").strip().lower()
            self.ref_shape_ellipse_radio.blockSignals(True)
            self.ref_shape_square_radio.blockSignals(True)
            self.ref_shape_square_radio.setChecked(reference_shape == "square")
            self.ref_shape_ellipse_radio.setChecked(reference_shape != "square")
            self.ref_shape_square_radio.blockSignals(False)
            self.ref_shape_ellipse_radio.blockSignals(False)
        self._sync_reference_overlay_controls_state()
        if hasattr(self, "gallery_image_color_checkbox"):
            self.gallery_image_color_checkbox.blockSignals(True)
            self.gallery_image_color_checkbox.setChecked(bool(self.gallery_plot_settings.get("image_color", False)))
            self.gallery_image_color_checkbox.blockSignals(False)
        if hasattr(self, "gallery_avg_q_checkbox"):
            self.gallery_avg_q_checkbox.blockSignals(True)
            self.gallery_avg_q_checkbox.setChecked(bool(self.gallery_plot_settings.get("avg_q", False)))
            self.gallery_avg_q_checkbox.blockSignals(False)
        if hasattr(self, "gallery_q_minmax_checkbox"):
            self.gallery_q_minmax_checkbox.blockSignals(True)
            self.gallery_q_minmax_checkbox.setChecked(bool(self.gallery_plot_settings.get("q_minmax", False)))
            self.gallery_q_minmax_checkbox.blockSignals(False)
        if hasattr(self, "gallery_q_extreme_minmax_checkbox"):
            self.gallery_q_extreme_minmax_checkbox.blockSignals(True)
            self.gallery_q_extreme_minmax_checkbox.setChecked(bool(self.gallery_plot_settings.get("q_extreme_minmax", False)))
            self.gallery_q_extreme_minmax_checkbox.blockSignals(False)
        if hasattr(self, "gallery_axis_equal_checkbox"):
            self.gallery_axis_equal_checkbox.blockSignals(True)
            self.gallery_axis_equal_checkbox.setChecked(bool(self.gallery_plot_settings.get("axis_equal", False)))
            self.gallery_axis_equal_checkbox.blockSignals(False)
        self._sync_gallery_histogram_controls()
        self._sync_gallery_kde_controls()
        if hasattr(self, "orient_checkbox"):
            self.orient_checkbox.blockSignals(True)
            self.orient_checkbox.setChecked(bool(settings.get("orient", False)))
            self.orient_checkbox.blockSignals(False)
        if hasattr(self, "uniform_scale_checkbox"):
            self.uniform_scale_checkbox.blockSignals(True)
            self.uniform_scale_checkbox.setChecked(bool(settings.get("uniform_scale", False)))
            self.uniform_scale_checkbox.blockSignals(False)
        if hasattr(self, "gallery_sort_combo"):
            self.gallery_sort_combo.blockSignals(True)
            sort_val = settings.get("gallery_sort", "") or ""
            idx = self.gallery_sort_combo.findData(sort_val)
            if idx >= 0:
                self.gallery_sort_combo.setCurrentIndex(idx)
            self.gallery_sort_combo.blockSignals(False)
        if hasattr(self, "gallery_include_details_checkbox"):
            self.gallery_include_details_checkbox.blockSignals(True)
            self.gallery_include_details_checkbox.setChecked(bool(settings.get("include_details", False)))
            self.gallery_include_details_checkbox.blockSignals(False)
        if hasattr(self, "plot_width_splitter"):
            splitter_sizes = settings.get("plot_width_splitter_sizes")
            if (
                isinstance(splitter_sizes, list)
                and len(splitter_sizes) >= 2
                and all(isinstance(value, (int, float)) for value in splitter_sizes[:2])
            ):
                self.plot_width_splitter.setSizes([max(0, int(splitter_sizes[0])), max(0, int(splitter_sizes[1]))])
        if settings.get("measurement_type"):
            self._pending_gallery_category = settings.get("measurement_type")
        self._set_gallery_strip_height()
        self._apply_saved_reference_state(settings)
        self._update_gallery_stats_preview()

    def update_graph_plots_only(self):
        """Update analysis graphs without rebuilding thumbnails."""
        if not self.is_analysis_visible():
            return
        self._set_gallery_busy_hint(
            self._consume_gallery_refresh_hint(self.tr("Updating spore plot..."))
        )
        try:
            all_measurements = self.get_gallery_measurements()
            self.update_graph_plots(all_measurements)
        finally:
            self._set_gallery_busy_hint("")

    def _set_observations_status(self, message: str, level: str = "info", auto_clear_ms: int = 10000) -> None:
        if hasattr(self, "observations_tab") and hasattr(self.observations_tab, "set_status_message"):
            self.observations_tab.set_status_message(message, level=level, auto_clear_ms=auto_clear_ms)

    def export_database_bundle(self):
        """Export DB and data folders as a zip file."""
        options_dialog = DatabaseBundleOptionsDialog(self.tr("Export Options"), parent=self)
        if options_dialog.exec() != QDialog.Accepted:
            return
        options = options_dialog.get_options()
        default_path = str(Path(self._get_default_export_dir()) / "Sporely_DB.zip")
        filename, _ = QFileDialog.getSaveFileName(
            self,
            "Export Database",
            default_path,
            "Zip Files (*.zip)"
        )
        if not filename:
            return
        if not filename.lower().endswith(".zip"):
            filename += ".zip"
        self._remember_export_dir(filename)
        status_tab = getattr(self, "observations_tab", None)
        progress_visible = False

        def set_export_progress(text: str, current: int, total: int) -> None:
            nonlocal progress_visible
            if status_tab is not None and hasattr(status_tab, "_set_status_progress_visible"):
                if not progress_visible:
                    status_tab._set_status_progress_visible(True)
                    progress_visible = True
                if hasattr(status_tab, "_set_status_progress"):
                    status_tab._set_status_progress(text, current=current, total=total)
            QApplication.processEvents()

        try:
            export_db_bundle(
                filename,
                include_observations=options["observations"],
                include_images=options["images"],
                include_measurements=options["measurements"],
                include_calibrations=options["calibrations"],
                include_reference_values=options["reference_values"],
                progress_cb=set_export_progress,
            )
            self._set_observations_status(
                self.tr("Export complete: {name}.").format(name=Path(filename).name),
                level="success",
            )
        except Exception as exc:
            self._set_observations_status(
                self.tr("Export failed: {error}").format(error=exc),
                level="error",
                auto_clear_ms=12000,
            )
        finally:
            if progress_visible and status_tab is not None:
                if hasattr(status_tab, "_set_status_progress_visible"):
                    status_tab._set_status_progress_visible(False)
                if hasattr(status_tab, "_set_status_progress"):
                    status_tab._set_status_progress("", current=0, total=1)

    def import_database_bundle(self):
        """Import DB and data from a shared zip file."""
        filename, _ = QFileDialog.getOpenFileName(
            self,
            "Import Database",
            self._get_default_import_dir(),
            "Zip Files (*.zip)"
        )
        if not filename:
            return
        self._remember_import_dir(filename)
        options_dialog = DatabaseBundleOptionsDialog(self.tr("Import Options"), parent=self)
        if options_dialog.exec() != QDialog.Accepted:
            return
        options = options_dialog.get_options()
        try:
            summary = import_db_bundle(
                filename,
                include_observations=options["observations"],
                include_images=options["images"],
                include_measurements=options["measurements"],
                include_calibrations=options["calibrations"],
                include_reference_values=options["reference_values"],
            )
            lines = []
            if options["observations"]:
                lines.append(f"Observations: {summary.get('observations', 0)}")
            if options["images"]:
                lines.append(f"Images: {summary.get('images', 0)}")
            if options["measurements"]:
                lines.append(f"Spore measurements: {summary.get('measurements', 0)}")
            if options["calibrations"]:
                lines.append(f"Calibrations: {summary.get('calibrations', 0)}")
                objective_count = int(summary.get("objectives", 0) or 0)
                if objective_count:
                    lines.append(f"Objective profiles: {objective_count}")
            if options["reference_values"]:
                lines.append(f"Reference values: {summary.get('reference_values', 0)}")
            status_message = self.tr("Updated DB.")
            if lines:
                status_message += " " + "; ".join(lines)
            warning_messages = [str(item or "").strip() for item in (summary.get("warnings") or []) if str(item or "").strip()]
            if warning_messages:
                preview = "; ".join(warning_messages[:2])
                if len(warning_messages) > 2:
                    preview += self.tr("; and {count} more").format(count=len(warning_messages) - 2)
                status_message += " " + self.tr("Warnings: {text}").format(text=preview)
            if hasattr(self, "observations_tab"):
                self.observations_tab.refresh_observations(
                    status_message=None if warning_messages else status_message
                )
                if warning_messages:
                    self.observations_tab.set_status_message(
                        status_message,
                        level="warning",
                        auto_clear_ms=12000,
                    )
            else:
                self._set_observations_status(
                    status_message,
                    level="warning" if warning_messages else "success",
                )
        except Exception as exc:
            self._set_observations_status(
                self.tr("Import failed: {error}").format(error=exc),
                level="error",
                auto_clear_ms=12000,
            )



    def delete_measurement(self, measurement_id):
        """Delete a measurement and its associated lines."""
        previous_row = None
        for row, measurement in enumerate(self.measurements_cache or []):
            if int(measurement.get("id") or 0) == int(measurement_id):
                previous_row = row
                break
        current_image_id = self.current_image_id

        MeasurementDB.delete_measurement(measurement_id)

        # Remove only the lines for this measurement
        if measurement_id in self.measurement_lines:
            del self.measurement_lines[measurement_id]
        if hasattr(self, "multiline_measurements") and measurement_id in self.multiline_measurements:
            del self.multiline_measurements[measurement_id]
        self.measurement_labels = [
            label for label in self.measurement_labels
            if label.get("id") != measurement_id
        ]

        previous_guard = bool(getattr(self, "_prevent_cross_image_switch_from_table_selection", False))
        self._prevent_cross_image_switch_from_table_selection = True
        self.update_display_lines()
        self.update_measurements_table()
        try:
            # Keep selection on the current image after delete; do not jump to another image.
            same_image_rows = [
                idx
                for idx, measurement in enumerate(self.measurements_cache or [])
                if int(measurement.get("image_id") or 0) == int(current_image_id or 0)
            ]
            if same_image_rows:
                if previous_row is not None:
                    target_row = next((idx for idx in same_image_rows if idx >= previous_row), same_image_rows[-1])
                else:
                    target_row = same_image_rows[0]
                self.measurements_table.selectRow(int(target_row))
                self.on_measurement_selected()
            else:
                self.measurements_table.clearSelection()
                self._clear_measurement_highlight()
        finally:
            self._prevent_cross_image_switch_from_table_selection = previous_guard
        self.update_statistics()
        self.spore_preview.clear()
        self.measure_status_label.setText(self.tr("Measurement deleted"))
        self.measure_status_label.setStyleSheet(f"color: #e67e22; font-weight: bold; font-size: {pt(9)}pt;")

    def _delete_selected_measurement_shortcut(self) -> None:
        """Delete the previewed measurement in measure mode, else selected field measurement."""
        if self.measurement_active and hasattr(self, "spore_preview"):
            preview_measurement_id = getattr(self.spore_preview, "measurement_id", None)
            if preview_measurement_id:
                self.delete_measurement(int(preview_measurement_id))
                return

        image_type = (self.current_image_type or "").strip().lower()
        if image_type != "field":
            return
        if not hasattr(self, "measurements_table") or not getattr(self, "measurements_cache", None):
            return
        selected_rows = self.measurements_table.selectedIndexes()
        if not selected_rows:
            return
        row = selected_rows[0].row()
        if row < 0 or row >= len(self.measurements_cache):
            return
        measurement_id = self.measurements_cache[row].get("id")
        if not measurement_id:
            return
        self.delete_measurement(int(measurement_id))

    def update_statistics(self):
        """Update the statistics display."""
        stats = {}
        if self.current_image_id:
            stats = MeasurementDB.get_statistics_for_image(
                self.current_image_id,
                measurement_category='spores'
            )
        elif self.active_observation_id:
            stats = MeasurementDB.get_statistics_for_observation(
                self.active_observation_id,
                measurement_category='spores'
            )

        if hasattr(self, "stats_table"):
            self.stats_table.update_stats(stats)

        if self.active_observation_id:
            obs_stats = MeasurementDB.get_statistics_for_observation(
                self.active_observation_id,
                measurement_category='spores'
            )
            self._update_observation_spore_statistics(self.active_observation_id, obs_stats)

    def _update_observation_spore_statistics(self, observation_id: int, stats: dict) -> None:
        if not observation_id:
            return
        if not hasattr(self, "_stats_retry_pending"):
            self._stats_retry_pending = False
        try:
            if stats:
                ObservationDB.update_spore_statistics(
                    observation_id,
                    self.format_literature_string(stats)
                )
            else:
                ObservationDB.update_spore_statistics(observation_id, None)
            self._stats_retry_pending = False
        except sqlite3.OperationalError as exc:
            if "locked" in str(exc).lower():
                if not self._stats_retry_pending:
                    self._stats_retry_pending = True
                    QTimer.singleShot(250, self.update_statistics)
                return
            raise

    def format_literature_string(self, stats, label: str | None = None):
        """Format the literature string for spore statistics."""
        if not stats:
            return ""

        lit_format = (
            f"{label or self.tr('Spores:')} ({stats['length_min']:.1f}-){stats['length_p5']:.1f}-"
            f"{stats['length_p95']:.1f}(-{stats['length_max']:.1f}) um"
        )

        if 'width_mean' in stats and stats.get('width_mean', 0) > 0:
            lit_format += (
                f" x ({stats['width_min']:.1f}-){stats['width_p5']:.1f}-"
                f"{stats['width_p95']:.1f}(-{stats['width_max']:.1f}) um"
            )
            lit_format += (
                f", Q = ({stats['ratio_min']:.1f}-){stats['ratio_p5']:.1f}-"
                f"{stats['ratio_p95']:.1f}(-{stats['ratio_max']:.1f})"
            )
            lit_format += f", Qm = {stats['ratio_mean']:.1f}"

        lit_format += f", n = {stats['count']}"
        return lit_format

    def _update_preview_title(self):
        if not hasattr(self, "preview_group"):
            return
        label = "Measurement Fine tune"
        if hasattr(self, "measure_category_combo"):
            category = self.measure_category_combo.currentData()
            if category:
                label = f"{self.format_measurement_category(category)} Fine tune"
        self.preview_group.setTitle(label)

    def _show_loading(self, message="Loading..."):
        """Show a blocking loading indicator."""
        if self.loading_dialog is None:
            dlg = QProgressDialog(message, None, 0, 0, self)
            dlg.setWindowTitle(message)
            dlg.setWindowModality(Qt.ApplicationModal)
            dlg.setCancelButton(None)
            dlg.setMinimumDuration(0)
            dlg.setAutoClose(False)
            dlg.setAutoReset(False)
            self.loading_dialog = dlg
        else:
            self.loading_dialog.setLabelText(message)
            self.loading_dialog.setWindowTitle(message)
        self.loading_dialog.show()
        QApplication.processEvents()

    def _hide_loading(self):
        """Hide the loading indicator."""
        if self.loading_dialog is not None:
            self.loading_dialog.hide()

    def _question_yes_no(self, title, text, default_yes=True):
        """Show a localized Yes/No confirmation dialog."""
        return ask_wrapped_yes_no(self, title, text, default_yes=default_yes)

    def _maybe_rescale_current_image(self, old_scale, new_scale):
        """Prompt to rescale previous measurements for the current image."""
        if self.suppress_scale_prompt:
            return True
        if not self.current_image_id or not old_scale or not new_scale:
            return True
        if abs(new_scale - old_scale) < 1e-6:
            return True
        measurements = MeasurementDB.get_measurements_for_image(self.current_image_id)
        if not measurements:
            return True
        has_points = any(
            all(m.get(f"p{i}_{axis}") is not None for i in range(1, 5) for axis in ("x", "y"))
            for m in measurements
        )
        if not has_points:
            return True
        box = QMessageBox(self)
        box.setIcon(QMessageBox.Question)
        box.setWindowTitle(self.tr("Changing image scale"))
        box.setText(self.tr("Changing image scale: This will update previous measurements to match the new scale."))
        ok_btn = box.addButton(self.tr("OK"), QMessageBox.AcceptRole)
        box.addButton(self.tr("Cancel"), QMessageBox.RejectRole)
        box.exec()
        if box.clickedButton() != ok_btn:
            return False

        conn = get_connection()
        cursor = conn.cursor()
        for m in measurements:
            if not all(m.get(f"p{i}_{axis}") is not None for i in range(1, 5) for axis in ("x", "y")):
                continue
            p1 = QPointF(m["p1_x"], m["p1_y"])
            p2 = QPointF(m["p2_x"], m["p2_y"])
            p3 = QPointF(m["p3_x"], m["p3_y"])
            p4 = QPointF(m["p4_x"], m["p4_y"])
            dist1 = math.hypot(p2.x() - p1.x(), p2.y() - p1.y()) * new_scale
            dist2 = math.hypot(p4.x() - p3.x(), p4.y() - p3.y()) * new_scale
            length_um = max(dist1, dist2)
            width_um = min(dist1, dist2)
            q_value = length_um / width_um if width_um > 0 else 0
            cursor.execute(
                'UPDATE spore_measurements SET length_um = ?, width_um = ?, notes = ? WHERE id = ?',
                (length_um, width_um, f"Q={q_value:.1f}", m["id"])
            )
        conn.commit()
        conn.close()

        self.load_measurement_lines()
        self.update_measurements_table()
        self.update_statistics()
        return True

    def _handle_reference_plot(self, data):
        """Plot reference values without saving."""
        self.reference_values = data or {}
        if data:
            self._set_reference_series([data])
        else:
            self._set_reference_series([])

    def _handle_reference_save(self, data):
        """Save reference values and update plot."""
        if not data.get("genus") or not data.get("species"):
            QMessageBox.warning(self, "Missing Species", "Please enter genus and species to save.")
            return
        ReferenceDB.set_reference(data)
        self._refresh_reference_species_availability()
        return

    def _refresh_reference_species_availability(self, force_refresh: bool = True) -> None:
        if not hasattr(self, "species_availability"):
            return
        self.species_availability.get_cache(force_refresh=force_refresh)
        if force_refresh:
            self._ref_genus_summary_cache_key = None
            self._ref_genus_summary_cache = {}
        if hasattr(self, "ref_genus_input") and hasattr(self, "ref_species_input"):
            genus = self._clean_ref_genus_text(self.ref_genus_input.text())
            species_text = self.ref_species_input.text().strip()
            if genus:
                self._update_ref_species_suggestions(genus, species_text)

    def load_reference_values(self):
        """Load reference values for the active observation."""
        self.reference_values = {}
        if not self.active_observation_id:
            return
        obs = ObservationDB.get_observation(self.active_observation_id)
        if not obs:
            return
        genus = obs.get("genus")
        species = obs.get("species")
        if not (genus and species):
            if hasattr(self, "ref_genus_input"):
                self.ref_genus_input.setText("")
            if hasattr(self, "ref_species_input"):
                self.ref_species_input.setText("")
            if hasattr(self, "ref_source_input"):
                self.ref_source_input.setCurrentText("")
            if hasattr(self, "ref_vernacular_input"):
                self.ref_vernacular_input.setText("")
            self._apply_reference_panel_values({})
            return
        ref = ReferenceDB.get_reference(genus, species)
        if ref:
            self.reference_values = ref
        if hasattr(self, "ref_genus_input"):
            self.ref_genus_input.setText(genus or "")
        if hasattr(self, "ref_species_input"):
            self.ref_species_input.setText(species or "")
        if hasattr(self, "ref_source_input"):
            self._populate_reference_panel_sources()
            source = self.reference_values.get("source") if self.reference_values else None
            if source:
                idx = self.ref_source_input.findText(source)
                if idx >= 0:
                    self.ref_source_input.setCurrentIndex(idx)
                else:
                    self.ref_source_input.setCurrentText(source)
        self._apply_reference_panel_values(self.reference_values)
        self._maybe_set_ref_vernacular_from_taxon()
        if self.reference_values:
            self._set_reference_series([self.reference_values])
        else:
            self._set_reference_series([])

    def open_reference_values_dialog(self):
        """Open the reference values dialog and save data."""
        if not self.active_observation_id:
            QMessageBox.warning(self, "No Observation", "Select an observation first.")
            return
        ref_data = dict(self.reference_values) if self.reference_values else {}
        dialog = ReferenceValuesDialog(
            ref_data.get("genus") or "",
            ref_data.get("species") or "",
            ref_data,
            self
        )
        dialog.plot_requested.connect(self._handle_reference_plot)
        dialog.save_requested.connect(self._handle_reference_save)
        dialog.exec()


    def on_observation_selected(self, observation_id, display_name, switch_tab=True, suppress_gallery=False):
        """Handle observation selection from the Observations tab."""
        _t0 = time.perf_counter()
        previous_suppress = self._suppress_gallery_update
        if suppress_gallery:
            self._suppress_gallery_update = True
        try:
            self._on_observation_selected_impl(
                observation_id,
                display_name,
                switch_tab=switch_tab,
                schedule_gallery=not suppress_gallery
            )
        finally:
            if suppress_gallery:
                self._suppress_gallery_update = previous_suppress
        if suppress_gallery and self.is_analysis_visible():
            self.schedule_gallery_refresh()

    def on_observation_deleted(self, observation_id: int):
        if observation_id != getattr(self, "active_observation_id", None):
            return
        self.active_observation_id = None
        self.active_observation_name = None
        if hasattr(self, "live_lab_tab") and self.live_lab_tab is not None:
            try:
                self.live_lab_tab.set_target_observation(None)
            except Exception:
                pass
        if hasattr(self, "ingestion_hub_tab") and self.ingestion_hub_tab is not None:
            try:
                self.ingestion_hub_tab.refresh_observation_queue()
            except Exception:
                pass
        self._update_spore_sharing_ui(None)
        if hasattr(self, "image_info_label"):
            self.image_info_label.setText("")
        self.update_observation_header(None)
        self.clear_current_image_display()
        self._update_measure_copyright_overlay()
        self.refresh_observation_images()
        self.update_measurements_table()
        self._update_gallery_stats_preview()

    def _on_observation_selected_impl(self, observation_id, display_name, switch_tab=True, schedule_gallery=True):
        """Internal handler for observation selection."""
        self.active_observation_id = observation_id
        self.active_observation_name = display_name
        if hasattr(self, "live_lab_tab") and self.live_lab_tab is not None:
            try:
                self.live_lab_tab.set_target_observation(observation_id, display_name=display_name)
            except Exception:
                pass
        if hasattr(self, "ingestion_hub_tab") and self.ingestion_hub_tab is not None:
            try:
                self.ingestion_hub_tab.sync_from_active_observation()
            except Exception:
                pass
        self._update_measure_copyright_overlay()

        # Update the image info label to show active observation
        if hasattr(self, "image_info_label"):
            self.image_info_label.setText(f"Active: {display_name}")
        self.clear_current_image_display()
        self.update_observation_header(observation_id)
        observation = ObservationDB.get_observation(observation_id)
        self.auto_threshold = observation.get("auto_threshold") if observation else None
        self._update_spore_sharing_ui(observation_id)
        self.load_reference_values()
        self._compute_observation_max_radius(observation_id)
        self.apply_gallery_settings()
        self._update_gallery_stats_preview()
        self.refresh_gallery_filter_options()
        if schedule_gallery and self.is_analysis_visible():
            self.schedule_gallery_refresh()
        self.update_measurements_table()
        self.refresh_observation_images()
        if hasattr(self, "measure_button"):
            self.measure_button.setEnabled(True)
        if self.observation_images:
            self.goto_image_index(0)

        # Switch to the Measure tab
        if switch_tab:
            self.tab_widget.setCurrentIndex(1)

    def load_image_for_observation(self):
        """Load microscope images and link them to the active observation."""
        paths, _ = QFileDialog.getOpenFileNames(
            self, "Open Microscope Image", self._get_default_import_dir(),
            "Images (*.png *.jpg *.jpeg *.tif *.tiff *.heic *.heif);;All Files (*)"
        )

        if not paths:
            return
        self._remember_import_dir(paths[0])

        output_dir = get_images_dir() / "imports"
        output_dir.mkdir(parents=True, exist_ok=True)
        last_image_data = None
        for path in paths:
            converted_path = maybe_convert_heic(path, output_dir)
            if converted_path is None:
                QMessageBox.warning(
                    self,
                    "HEIC Conversion Failed",
                    f"Could not convert {Path(path).name} to JPEG."
                )
                continue

            objective_name = self.get_objective_name_for_storage()
            calibration_id = CalibrationDB.get_active_calibration_id(objective_name) if objective_name else None
            contrast_fallback = SettingsDB.get_list_setting(
                "contrast_options",
                DatabaseTerms.CONTRAST_METHODS,
            )
            contrast_value = SettingsDB.get_setting(DatabaseTerms.last_used_key("contrast"), None)
            if not contrast_value:
                contrast_value = SettingsDB.get_setting(
                    "contrast_default",
                    contrast_fallback[0] if contrast_fallback else DatabaseTerms.CONTRAST_METHODS[0],
                )
            contrast_value = DatabaseTerms.canonicalize("contrast", contrast_value)
            if not contrast_value:
                contrast_value = contrast_fallback[0] if contrast_fallback else DatabaseTerms.CONTRAST_METHODS[0]
            image_id = ImageDB.add_image(
                observation_id=self.active_observation_id,
                filepath=converted_path,
                image_type='microscope',
                scale=self.microns_per_pixel,
                objective_name=objective_name,
                contrast=contrast_value,
                calibration_id=calibration_id,
                resample_scale_factor=1.0,
            )

            image_data = ImageDB.get_image(image_id)
            stored_path = image_data.get("filepath") if image_data else converted_path

            try:
                generate_all_sizes(stored_path, image_id)
            except Exception as e:
                print(f"Warning: Could not generate thumbnails: {e}")

            last_image_data = ImageDB.get_image(image_id)
            cleanup_import_temp_file(path, converted_path, stored_path, output_dir)

        if last_image_data:
            self.load_image_record(last_image_data, refresh_table=True)
            self.refresh_observation_images(select_image_id=last_image_data['id'])

    def on_image_selected(self, image_id, observation_id, display_name):
        """Handle image selection from the Observations tab - load the image."""
        image_id = int(image_id or 0)
        observation_id = int(observation_id or 0)
        if not image_id or not observation_id:
            return
        if hasattr(self, "observations_tab") and hasattr(self.observations_tab, "gallery_widget"):
            try:
                self.observations_tab.gallery_widget.prepare_for_tab_switch()
            except Exception:
                pass
        QTimer.singleShot(
            75,
            lambda img_id=image_id, obs_id=observation_id, name=display_name:
                self._on_image_selected_impl(img_id, obs_id, name),
        )

    def _on_image_selected_impl(self, image_id, observation_id, display_name):
        """Deferred image-selection handler to avoid crashes during tab hiding."""
        self.active_observation_id = observation_id
        self.active_observation_name = display_name
        self.update_observation_header(observation_id)
        observation = ObservationDB.get_observation(observation_id)
        self.auto_threshold = observation.get("auto_threshold") if observation else None
        self._compute_observation_max_radius(observation_id)
    
        # Get image data from database
        image_data = ImageDB.get_image(image_id)
        if not image_data:
            return
    
        self.load_image_record(image_data, display_name=display_name, refresh_table=True)
        filename = Path(self.current_image_path).name
    
        # Switch to Measure tab
        self.tab_widget.setCurrentIndex(1)
        if hasattr(self, "measure_button"):
            self.measure_button.setEnabled(True)

        self.measure_status_label.setText("")
        if hasattr(self, "measure_gallery"):
            self.measure_gallery.select_image(image_id)
    
    def enter_calibration_mode(self, dialog):
        """Enter calibration mode for 2-point scale calibration."""
        if not self.current_pixmap:
            self.measure_status_label.setText(self.tr("Load an image first to calibrate"))
            self.measure_status_label.setStyleSheet(f"color: #e74c3c; font-weight: bold; font-size: {pt(9)}pt;")
            return

        self.calibration_mode = True
        self.calibration_dialog = dialog
        self.calibration_points = []
        self._show_calibration_overlay_for_scalebar = True
        self.update_display_lines()

        # Clear any existing preview
        self.image_label.clear_preview_line()

        self.measure_status_label.setText(self.tr("CALIBRATION: Click first point on scale bar"))
        self.measure_status_label.setStyleSheet(f"color: #e67e22; font-weight: bold; font-size: {pt(9)}pt;")

    def handle_calibration_click(self, pos):
        """Handle clicks during calibration mode."""
        self.calibration_points.append(pos)

        if len(self.calibration_points) == 1:
            # First point - show preview line
            constrain_horizontal = bool(
                hasattr(self, "scale_bar_horizontal_checkbox")
                and self.scale_bar_horizontal_checkbox.isChecked()
            )
            self.image_label.set_preview_line(pos, horizontal=constrain_horizontal)
            self.measure_status_label.setText(self.tr("CALIBRATION: Click second point on scale bar"))
            self.measure_status_label.setStyleSheet(f"color: #e67e22; font-weight: bold; font-size: {pt(9)}pt;")

        elif len(self.calibration_points) == 2:
            # Second point - calculate distance
            p1 = self.calibration_points[0]
            p2 = self.calibration_points[1]
            if (
                hasattr(self, "scale_bar_horizontal_checkbox")
                and self.scale_bar_horizontal_checkbox.isChecked()
            ):
                p2 = QPointF(p2.x(), p1.y())
                self.calibration_points[1] = p2
            dx = p2.x() - p1.x()
            dy = p2.y() - p1.y()
            distance_pixels = math.sqrt(dx**2 + dy**2)
            if distance_pixels <= 0:
                self.calibration_points = []
                self.image_label.clear_preview_line()
                self.measure_status_label.setText(self.tr("Calibration failed: zero-length line. Try again."))
                self.measure_status_label.setStyleSheet(f"color: #e74c3c; font-weight: bold; font-size: {pt(9)}pt;")
                return

            # Store the calibration line for display
            self.calibration_distance_pixels = distance_pixels

            calib_line = [p1.x(), p1.y(), p2.x(), p2.y()]
            self.temp_lines = [calib_line]
            self.update_display_lines()
            self.image_label.clear_preview_line()
            self.apply_calibration_scale()

    def apply_calibration_scale(self):
        """Apply the calibration scale from preview."""
        if not hasattr(self, "calibration_distance_pixels"):
            return
        if not self.calibration_points or len(self.calibration_points) < 2:
            return
        distance_pixels = float(self.calibration_distance_pixels or 0.0)
        if distance_pixels <= 0:
            return
        total_um = self._current_scale_bar_length_um()
        if total_um <= 0:
            return
        scale_um_per_px = total_um / distance_pixels
        applied = bool(self.set_custom_scale(scale_um_per_px))
        if applied and self.current_image_id:
            p1 = self.calibration_points[0]
            p2 = self.calibration_points[1]
            scale_bar_selection = ((float(p1.x()), float(p1.y())), (float(p2.x()), float(p2.y())))
            ImageDB.update_image(
                self.current_image_id,
                scale_bar_selection=scale_bar_selection,
            )
            self._upsert_calibration_measurement(
                self.current_image_id,
                scale_bar_selection,
                total_um,
                distance_pixels,
            )
            self.load_measurement_lines()
            self.update_measurements_table()
            self.update_display_lines()
        if (self.current_image_type or "").strip().lower() == "field":
            mm_per_px = scale_um_per_px / 1000.0
            self.measure_status_label.setText(self.tr("Scale set: {scale:.4f} mm/px").format(scale=mm_per_px))
        else:
            self.measure_status_label.setText(self.tr("Scale set: {scale:.2f} nm/px").format(scale=scale_um_per_px * 1000.0))
        self.measure_status_label.setStyleSheet(f"color: #27ae60; font-weight: bold; font-size: {pt(9)}pt;")
        self.calibration_mode = False
        self.calibration_dialog = None
        self.calibration_points = []
        self.temp_lines = []
        self._show_calibration_overlay_for_scalebar = False
        self.update_display_lines()

    def _upsert_calibration_measurement(
        self,
        image_id: int,
        scale_bar_selection: tuple[tuple[float, float], tuple[float, float]],
        total_um: float,
        pixel_dist: float,
    ) -> None:
        (x1, y1), (x2, y2) = scale_bar_selection
        dx = x2 - x1
        dy = y2 - y1
        length_px = (dx * dx + dy * dy) ** 0.5
        if length_px <= 0:
            return
        perp_x = -dy / length_px
        perp_y = dx / length_px
        half_width_px = pixel_dist / 20.0
        mx = (x1 + x2) / 2
        my = (y1 + y2) / 2
        p1 = QPointF(x1, y1)
        p2 = QPointF(x2, y2)
        p3 = QPointF(mx - perp_x * half_width_px, my - perp_y * half_width_px)
        p4 = QPointF(mx + perp_x * half_width_px, my + perp_y * half_width_px)
        width_um = total_um / 10.0

        conn = get_connection()
        conn.execute(
            "DELETE FROM spore_measurements WHERE image_id = ? AND measurement_type = 'calibration'",
            (image_id,),
        )
        conn.commit()
        conn.close()

        MeasurementDB.add_measurement(
            image_id=image_id,
            length=total_um,
            width=width_um,
            measurement_type="calibration",
            notes=f"Scale bar: {total_um:.1f} µm",
            points=[p1, p2, p3, p4],
        )

    def _should_show_calibration_overlay(self) -> bool:
        if self.calibration_mode or self._show_calibration_overlay_for_scalebar:
            return True
        return False
