"""Live microscopy session tab."""
from __future__ import annotations

import copy
import json
import os
import time
import tempfile
from dataclasses import dataclass, replace
from datetime import datetime
from uuid import uuid4
from pathlib import Path

import numpy as np
from PIL import Image

from PySide6.QtCore import QSize, Qt, QTimer, QSignalBlocker, QEvent, QPointF, QRectF
from PySide6.QtGui import (
    QColor,
    QIcon,
    QImage,
    QImageReader,
    QKeySequence,
    QPainter,
    QPainterPath,
    QPen,
    QPixmap,
    QPalette,
    QShortcut,
)
from PySide6.QtWidgets import (
    QApplication,
    QAbstractButton,
    QAbstractSlider,
    QAbstractSpinBox,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFrame,
    QFormLayout,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSplitter,
    QSlider,
    QPlainTextEdit,
    QTextEdit,
    QVBoxLayout,
    QToolButton,
    QWidget,
)

from config import (
    RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG,
    RAW_COMPANION_SOURCE_PREFERENCE_PREFER_RAW,
    SETTING_RAW_PROCESSING_BRIGHT_CUTOFF,
    SETTING_RAW_PROCESSING_DARK_CUTOFF,
    SETTING_RAW_PROCESSING_SHADOW_LIFT_ENABLED,
    SETTING_RAW_PROCESSING_SHADOW_LIFT_MAX,
    SETTING_RAW_COMPANION_SOURCE_PREFERENCE,
)
from database.database_tags import DatabaseTerms
from database.models import CalibrationDB, ImageDB, ObservationDB, SessionLogDB, SettingsDB
from database.schema import (
    get_images_dir,
    load_objectives,
    objective_display_name,
    objective_sort_value,
    resolve_objective_key,
)
from utils.image_utils import cleanup_import_temp_file
from utils.image_companion_grouping import (
    companion_group_key,
    normalize_raw_companion_source_preference,
    select_preferred_companion_path,
)
from utils.image_metadata_merge import merge_image_lab_metadata
from utils.image_processing_pipeline import (
    ProcessingDebugInfo,
    apply_post_decode_processing,
    compute_post_decode_transfer_curve,
    raw_basic_controls_from_settings,
    raw_settings_from_basic_controls,
)
from utils.local_image_ingest import RawRenderingUnavailableError, prepare_local_ingest_image
from utils.lab_watcher import LabWatcherWorker
from utils.raw_detection import SUPPORTED_RAW_SUFFIXES, is_raw_image_path
from utils.raw_render import (
    RawRenderSettings,
    compute_auto_level_adjusted_settings_from_source,
    build_raw_processing_metadata,
    RAW_PREVIEW_MAX_DIM,
    render_raw_image,
    render_raw_preview,
    render_raw_preview_proxy_rgb,
    save_raw_preview_jpeg,
)
from utils.raw_white_balance import estimate_white_balance_from_background
from utils.thumbnail_generator import generate_all_sizes, get_thumbnail_path

from .hint_status import HintBar, HintStatusController
from .combo_alerts import combo_is_unset, lab_state_combo_alert_stylesheet, update_combo_alert, update_combo_alerts
from .image_gallery_widget import ImageGalleryWidget
from .adaptive_choice_selector import (
    AdaptiveChoiceSelector,
    objective_color,
    objective_short_label,
    objective_is_macro_profile,
    stain_color,
)
from .raw_processing_controls import RawProcessingControls
from .section_card import create_section_card
from .segmented_selector import SegmentedSelector
from .splitter_state import (
    GALLERY_DEFAULT_HEIGHT,
    GALLERY_MIN_HEIGHT,
    SIDEBAR_DEFAULT_WIDTH,
    SIDEBAR_MIN_WIDTH,
    configure_sidebar_scroll,
    configure_splitter_pane,
    install_persistent_splitter,
)
from .zoomable_image_widget import ZoomableImageLabel


_RAW_DEBUG_TIMING = True


def _raw_timing_log(label: str, start: float | None, *, detail: str | None = None) -> None:
    if not _RAW_DEBUG_TIMING or start is None:
        return
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    message = f"[raw-timing] {label}: {elapsed_ms:.1f} ms"
    if detail:
        message = f"{message} | {detail}"
    print(message, flush=True)


@dataclass(slots=True)
class PendingRawCapture:
    """Session-local RAW capture waiting for review and commit."""

    source_path: Path
    companion_jpeg_path: Path | None
    lab_metadata: dict[str, object]
    raw_settings: RawRenderSettings
    preview_path: Path | None = None
    preview_rgb: np.ndarray | None = None
    wb_sample_base_preview_path: Path | None = None
    wb_sample_base_pixmap: QPixmap | None = None
    status: str = "pending"
    group_key: str | None = None
    observation_id: int | None = None
    created_at: datetime | None = None


@dataclass(slots=True)
class RawEditSession:
    """Session-local RAW re-render state for one committed image."""

    image_id: int
    source_raw_path: Path
    current_derivative_path: Path
    original_settings: RawRenderSettings
    working_settings: RawRenderSettings
    image_lab_metadata: dict[str, object]
    source_capture_datetime: str | None = None
    previous_session_settings: RawRenderSettings | None = None
    preview_path: Path | None = None
    preview_rgb: np.ndarray | None = None
    wb_sample_base_preview_path: Path | None = None
    wb_sample_base_pixmap: QPixmap | None = None
    dirty: bool = False


def _resize_preview_rgb(rgb: np.ndarray, max_dim: int = RAW_PREVIEW_MAX_DIM) -> np.ndarray:
    arr = np.asarray(rgb, dtype=np.float32)
    if arr.size == 0:
        return arr.copy()
    if arr.ndim != 3 or arr.shape[-1] < 3:
        return arr.copy()

    height, width = arr.shape[:2]
    limit = max(1, int(max_dim))
    long_edge = max(int(width), int(height))
    image = np.clip(arr[..., :3], 0.0, 1.0)
    if long_edge <= limit:
        return image.copy()

    scale = float(limit) / float(long_edge)
    new_width = max(1, int(round(float(width) * scale)))
    new_height = max(1, int(round(float(height) * scale)))
    rgb8 = np.rint(image * 255.0).astype(np.uint8)
    resample = getattr(getattr(Image, "Resampling", Image), "LANCZOS", Image.LANCZOS)
    resized = Image.fromarray(rgb8, mode="RGB").resize((new_width, new_height), resample)
    return np.asarray(resized, dtype=np.float32) / np.float32(255.0)


_TONE_CURVE_PREVIEW_ANALYSIS_MAX_DIM = 192
_TONE_CURVE_PREVIEW_HISTOGRAM_BINS = 96


class RawCurvePreviewWidget(QWidget):
    """Compact preview of the active RAW transfer curve and image histogram."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._curve = None
        self._histogram: np.ndarray | None = None
        self._theme_colors: dict[str, QColor] = {}
        self.setFixedSize(190, 190)
        self.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.setToolTip("Effective transfer curve")
        self._refresh_theme_colors()

    @property
    def current_curve(self):
        return self._curve

    def current_histogram(self) -> np.ndarray | None:
        if self._histogram is None:
            return None
        return self._histogram.copy()

    def set_curve(self, curve, histogram: np.ndarray | None = None) -> None:
        self._curve = curve
        self._histogram = None if histogram is None else np.asarray(histogram, dtype=np.float32).copy()
        self.update()

    def _refresh_theme_colors(self) -> None:
        palette = self.palette()
        window_color = QColor(palette.window().color())
        plot_color = QColor(palette.base().color())
        border_color = QColor(palette.mid().color())
        text_color = QColor(palette.text().color())
        accent_color = QColor("#16a085")
        if palette.highlight().color().isValid():
            highlight = QColor(palette.highlight().color())
            if highlight.lightness() > 200:
                accent_color = highlight.darker(140)
            elif highlight.lightness() < 80:
                accent_color = highlight.lighter(120)
            else:
                accent_color = highlight

        dark_theme = window_color.lightness() < 128
        histogram_color = QColor(border_color)
        histogram_color.setAlpha(84 if dark_theme else 96)
        identity_color = QColor(border_color)
        identity_color.setAlpha(170 if dark_theme else 150)
        curve_color = QColor(accent_color)
        curve_color.setAlpha(240)
        node_fill = QColor(plot_color)
        node_fill.setAlpha(255)
        node_dark = QColor(text_color)
        node_dark.setAlpha(235)
        node_light = QColor(curve_color)
        node_light.setAlpha(255)

        self._theme_colors = {
            "window": window_color,
            "plot": plot_color,
            "border": border_color,
            "histogram": histogram_color,
            "identity": identity_color,
            "curve": curve_color,
            "node_fill": node_fill,
            "node_dark": node_dark,
            "node_light": node_light,
        }

    def changeEvent(self, event) -> None:  # type: ignore[override]
        event_type = event.type()
        if event_type in {
            getattr(QEvent, "PaletteChange", None),
            getattr(QEvent, "ApplicationPaletteChange", None),
            getattr(QEvent, "StyleChange", None),
            getattr(QEvent, "ThemeChange", None),
        }:
            self._refresh_theme_colors()
            self.update()
        super().changeEvent(event)

    @staticmethod
    def _map_point(plot: QRectF, x: float, y: float) -> QPointF:
        px = plot.left() + float(np.clip(float(x), 0.0, 1.0)) * plot.width()
        py = plot.bottom() - float(np.clip(float(y), 0.0, 1.0)) * plot.height()
        return QPointF(px, py)

    @staticmethod
    def _draw_node(painter: QPainter, center: QPointF, *, fill: QColor, stroke: QColor) -> None:
        painter.setPen(QPen(stroke, 1.0))
        painter.setBrush(fill)
        painter.drawEllipse(center, 3.8, 3.8)

    def paintEvent(self, event) -> None:  # type: ignore[override]
        del event
        if not self._theme_colors:
            self._refresh_theme_colors()

        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        painter.fillRect(self.rect(), self._theme_colors["window"])

        outer = self.rect().adjusted(1, 1, -1, -1)
        painter.setPen(QPen(self._theme_colors["border"], 1))
        painter.setBrush(Qt.NoBrush)
        painter.drawRoundedRect(outer, 8, 8)

        inset = 12
        plot_side = max(1, min(int(outer.width() - inset * 2), int(outer.height() - inset * 2)))
        plot_left = float(outer.center().x()) - float(plot_side) / 2.0
        plot_top = float(outer.center().y()) - float(plot_side) / 2.0
        plot = QRectF(plot_left, plot_top, float(plot_side), float(plot_side))

        painter.setPen(QPen(self._theme_colors["border"], 1))
        painter.setBrush(self._theme_colors["plot"])
        painter.drawRoundedRect(plot, 6, 6)

        histogram = self._histogram
        if histogram is not None and histogram.size:
            hist = np.asarray(histogram, dtype=np.float32)
            peak = float(np.max(hist)) if hist.size else 0.0
            if np.isfinite(peak) and peak > 0.0:
                hist = np.clip(hist / peak, 0.0, 1.0)
                hist_path = QPainterPath()
                step = plot.width() / float(hist.size)
                hist_path.moveTo(plot.left(), plot.bottom())
                for index, value in enumerate(hist):
                    x_left = plot.left() + float(index) * step
                    x_right = x_left + step
                    y_top = plot.bottom() - float(value) * plot.height()
                    hist_path.lineTo(x_left, y_top)
                    hist_path.lineTo(x_right, y_top)
                hist_path.lineTo(plot.right(), plot.bottom())
                hist_path.closeSubpath()
                painter.setPen(Qt.NoPen)
                painter.setBrush(self._theme_colors["histogram"])
                painter.drawPath(hist_path)

        painter.setPen(QPen(self._theme_colors["identity"], 1.1, Qt.DashLine))
        painter.setBrush(Qt.NoBrush)
        painter.drawLine(self._map_point(plot, 0.0, 0.0), self._map_point(plot, 1.0, 1.0))

        curve = self._curve
        if curve is None:
            return

        path = QPainterPath()
        first_point = True
        for x_value, y_value in zip(curve.input_values, curve.final_output, strict=False):
            point = self._map_point(plot, float(x_value), float(y_value))
            if first_point:
                path.moveTo(point)
                first_point = False
            else:
                path.lineTo(point)
        painter.setPen(QPen(self._theme_colors["curve"], 2.2, Qt.SolidLine, Qt.RoundCap, Qt.RoundJoin))
        painter.setBrush(Qt.NoBrush)
        painter.drawPath(path)

        debug = getattr(curve, "debug", None)
        black_level = getattr(debug, "black_level", None)
        white_level = getattr(debug, "white_level", None)
        dark_point = getattr(debug, "dark_point", None)
        light_point = getattr(debug, "light_point", None)
        if dark_point is None and black_level is not None:
            dark_point = black_level
        if light_point is None and white_level is not None:
            light_point = white_level
        if dark_point is not None and np.isfinite(float(dark_point)):
            self._draw_node(
                painter,
                self._map_point(plot, float(dark_point), 0.0),
                fill=self._theme_colors["node_fill"],
                stroke=self._theme_colors["node_dark"],
            )
        if light_point is not None and np.isfinite(float(light_point)):
            self._draw_node(
                painter,
                self._map_point(plot, float(light_point), 1.0),
                fill=self._theme_colors["node_light"],
                stroke=self._theme_colors["node_dark"],
            )


def _compute_combined_rgb_histogram(rgb: np.ndarray, *, bins: int = _TONE_CURVE_PREVIEW_HISTOGRAM_BINS) -> np.ndarray:
    arr = np.asarray(rgb, dtype=np.float32)
    bin_count = max(1, int(bins))
    if arr.size == 0:
        return np.zeros(bin_count, dtype=np.float32)

    values = np.clip(arr[..., :3], 0.0, 1.0).reshape(-1)
    hist, _edges = np.histogram(values, bins=bin_count, range=(0.0, 1.0))
    hist = hist.astype(np.float32, copy=False)
    if hist.size == 0:
        return np.zeros(bin_count, dtype=np.float32)

    peak = float(hist.max())
    if peak <= 0.0 or not np.isfinite(peak):
        return np.zeros(bin_count, dtype=np.float32)
    hist = np.sqrt(hist / np.float32(peak)).astype(np.float32, copy=False)
    return hist


class LiveLabTab(QWidget):
    """Watch a capture folder and ingest new microscope images into one observation."""

    SETTING_WATCH_DIR = "live_lab_watch_dir"
    SETTING_LAST_OBJECTIVE = "live_lab_last_objective"
    SETTING_SESSION_MODE = "live_lab_session_mode"
    SETTING_MAIN_SPLITTER = "live_lab_main_splitter_sizes"
    SETTING_CONTENT_SPLITTER = "live_lab_content_splitter_sizes"
    SETTING_RAW_PROCESSING_PRESET_PREFIX = "live_lab_raw_processing_preset"
    SETTING_RAW_CAPTURE_MODE = "live_lab_raw_capture_mode"
    RAW_CAPTURE_MODE_AUTO_SAVE = "auto_save"
    RAW_CAPTURE_MODE_REVIEW = "review"
    SESSION_MODE_LIVE = "live"
    SESSION_MODE_OFFLINE = "offline"
    VIEWER_PREVIEW_MAX_DIM = 1600
    VIEWER_SCALE_BAR_UM = 10.0
    RAW_BACKGROUND_WB_SAMPLE_SIZE = 10
    OBSERVATION_PREVIEW_SIZE = 116
    SESSION_BUTTON_BASE_STYLE = "font-weight: bold; padding: 6px 10px;"
    SESSION_BUTTON_ACTIVE_STYLE = (
        "font-weight: bold; padding: 6px 10px; background-color: #e74c3c; color: white;"
    )

    def __init__(self, main_window, parent=None) -> None:
        super().__init__(parent)
        self._main_window = main_window
        self._target_observation_id: int | None = None
        self._target_observation: dict | None = None
        self._session_active = False
        self._session_id: str | None = None
        self._active_session_mode: str | None = None
        self._session_observation_id: int | None = None
        self._session_observation_snapshot: dict | None = None
        self._watcher: LabWatcherWorker | None = None
        self._session_image_ids: list[int] = []
        self._selected_session_image_id: int | None = None
        self._raw_render_settings = RawRenderSettings.default()
        self._raw_capture_mode = self.RAW_CAPTURE_MODE_AUTO_SAVE
        self._raw_companion_source_preference = RAW_COMPANION_SOURCE_PREFERENCE_PREFER_RAW
        self._pending_raw_captures: list[PendingRawCapture] = []
        self._selected_pending_raw_index = -1
        self._pending_raw_preview_timer = QTimer(self)
        self._pending_raw_preview_timer.setSingleShot(True)
        self._pending_raw_preview_timer.timeout.connect(self._refresh_selected_pending_raw_preview)
        self._pending_raw_preview_timer.setInterval(24)
        self._raw_edit_session: RawEditSession | None = None
        self._raw_edit_background_wb_armed = False
        self._raw_edit_preview_timer = QTimer(self)
        self._raw_edit_preview_timer.setSingleShot(True)
        self._raw_edit_preview_timer.timeout.connect(self._refresh_raw_edit_preview)
        self._raw_edit_preview_timer.setInterval(24)
        self._pending_raw_background_wb_armed = False
        self._pending_raw_preview_proxy_cache: dict[tuple[str, int, int, str], np.ndarray] = {}
        self._raw_curve_preview_analysis_signature: tuple[str, int, int, str] | None = None
        self._raw_curve_preview_analysis_rgb: np.ndarray | None = None
        self._raw_curve_preview_histogram: np.ndarray | None = None
        self._raw_edit_preview_save_target: RawEditSession | None = None
        self._pending_raw_preview_save_target: PendingRawCapture | None = None
        self._raw_edit_preview_save_timer = QTimer(self)
        self._raw_edit_preview_save_timer.setSingleShot(True)
        self._raw_edit_preview_save_timer.timeout.connect(self._persist_raw_edit_preview)
        self._raw_edit_preview_save_timer.setInterval(300)
        self._pending_raw_preview_save_timer = QTimer(self)
        self._pending_raw_preview_save_timer.setSingleShot(True)
        self._pending_raw_preview_save_timer.timeout.connect(self._persist_pending_raw_preview)
        self._pending_raw_preview_save_timer.setInterval(300)
        self._raw_review_shortcuts: list[QShortcut] = []
        self._raw_copied_settings: RawRenderSettings | None = None
        self._seen_source_paths: set[str] = set()
        self._pending_companion_groups: dict[str, dict[str, object]] = {}
        self._consumed_companion_groups: set[str] = set()
        self._raw_companion_hold_ms = 2000
        self._session_import_count = 0
        self._session_stop_pending = False
        self._pending_stop_status: tuple[str, str, int] | None = None
        self._recording_tab_icon = self._build_recording_tab_icon()
        self._default_hint_text = self.tr(
            "Watch your microscope capture folder here. Scroll to zoom, drag to pan, and click a session thumbnail to inspect it."
        )
        self._build_ui()
        self._install_raw_review_shortcuts()
        self._populate_objective_combo()
        self._restore_term_selection(self.contrast_combo, "contrast")
        self._restore_term_selection(self.mount_combo, "mount")
        self._restore_term_selection(self.stain_combo, "stain")
        self._restore_term_selection(self.sample_combo, "sample")
        self._restore_raw_capture_mode()
        self._restore_raw_companion_source_preference()
        self._load_raw_processing_settings_for_current_context()
        self._restore_watch_dir()
        self._restore_session_mode()
        self._connect_session_logging_signals()
        self._clear_session_viewer()
        self._update_target_display()
        self._update_session_controls()
        update_alerts = getattr(self, "_update_lab_state_combo_alerts", None)
        if callable(update_alerts):
            update_alerts()
        self._register_hint_widgets()
        self._set_hint(self._default_hint_text)

    def _build_ui(self) -> None:
        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(10, 10, 10, 10)
        root_layout.setSpacing(8)

        main_splitter = QSplitter(Qt.Horizontal)
        main_splitter.setChildrenCollapsible(False)
        root_layout.addWidget(main_splitter, 1)

        left_panel = QWidget()
        left_panel.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(10)

        current_group, current_layout = create_section_card(
            self.tr("Current observation"),
            body_margins=(10, 12, 10, 10),
        )
        current_row = QHBoxLayout()
        current_row.setContentsMargins(0, 0, 0, 0)
        current_row.setSpacing(10)
        self.current_observation_thumb_label = QLabel(self.tr("No image"))
        self.current_observation_thumb_label.setAlignment(Qt.AlignCenter)
        self.current_observation_thumb_label.setFixedSize(
            self.OBSERVATION_PREVIEW_SIZE,
            self.OBSERVATION_PREVIEW_SIZE,
        )
        self.current_observation_thumb_label.setStyleSheet(
            "border: 1px solid #d1d5db; border-radius: 8px; background-color: #f3f4f6; color: #6b7280;"
        )
        current_row.addWidget(self.current_observation_thumb_label, 0, Qt.AlignTop)
        current_text_layout = QVBoxLayout()
        current_text_layout.setContentsMargins(0, 0, 0, 0)
        current_text_layout.setSpacing(6)
        self.current_observation_name_label = QLabel("\u2014")
        self.current_observation_name_label.setWordWrap(True)
        self.current_observation_name_label.setStyleSheet("font-weight: 600; font-size: 15px;")
        current_text_layout.addWidget(self.current_observation_name_label)
        self.current_observation_scientific_label = QLabel("\u2014")
        self.current_observation_scientific_label.setWordWrap(True)
        self.current_observation_scientific_label.setStyleSheet("font-style: italic; color: #6b7280;")
        current_text_layout.addWidget(self.current_observation_scientific_label)
        self.current_observation_date_label = QLabel(self.tr("Date: \u2014"))
        self.current_observation_date_label.setWordWrap(True)
        self.current_observation_date_label.setStyleSheet("color: #6b7280;")
        current_text_layout.addWidget(self.current_observation_date_label)
        self.start_stop_btn = QPushButton(self.tr("Start Session"))
        self.start_stop_btn.setMinimumHeight(36)
        self.start_stop_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.start_stop_btn.setStyleSheet(self.SESSION_BUTTON_BASE_STYLE)
        self.start_stop_btn.clicked.connect(self._toggle_session)
        current_text_layout.addWidget(self.start_stop_btn)
        self.session_mode_selector = SegmentedSelector(self, compact=True)
        self.session_mode_live_radio = self.session_mode_selector.add_option(
            self.tr("Live capture (watch folder)"),
            self.SESSION_MODE_LIVE,
            checked=True,
        )
        self.session_mode_offline_radio = self.session_mode_selector.add_option(
            self.tr("Offline (log only)"),
            self.SESSION_MODE_OFFLINE,
        )
        self.session_mode_combo = self.session_mode_selector
        self.session_mode_selector.selectionChanged.connect(lambda _value: self._on_session_mode_changed())
        current_text_layout.addWidget(self.session_mode_selector)

        self.watch_group = QWidget()
        self.watch_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        watch_layout = QHBoxLayout(self.watch_group)
        watch_layout.setContentsMargins(0, 4, 0, 0)
        watch_layout.setSpacing(8)
        self.watch_dir_input = QLineEdit()
        self.watch_dir_input.setPlaceholderText(self.tr("Choose the microscope capture folder"))
        self.watch_dir_input.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.watch_dir_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.watch_dir_input.textChanged.connect(self._on_watch_dir_changed)
        watch_layout.addWidget(self.watch_dir_input)
        self.browse_btn = QPushButton(self.tr("Browse"))
        self.browse_btn.clicked.connect(self._choose_watch_dir)
        self.browse_btn.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.browse_btn.setMinimumWidth(72)
        watch_layout.addWidget(self.browse_btn, 0, Qt.AlignVCenter)
        self.rescan_btn = QPushButton(self.tr("Rescan folder"))
        self.rescan_btn.clicked.connect(self._on_rescan_watch_folder)
        self.rescan_btn.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.rescan_btn.setMinimumWidth(104)
        watch_layout.addWidget(self.rescan_btn, 0, Qt.AlignVCenter)
        current_text_layout.addWidget(self.watch_group)
        current_text_layout.addStretch(1)
        current_row.addLayout(current_text_layout, 1)
        current_layout.addLayout(current_row)
        left_layout.addWidget(current_group)

        tag_group, tag_form = create_section_card(
            self.tr("Microscope"),
            QFormLayout,
            body_margins=(10, 12, 10, 10),
        )
        tag_form.setSpacing(8)
        self.objective_combo = self._make_combo()
        self.objective_combo.set_unselected_border_visible(False)
        self.objective_combo.currentIndexChanged.connect(self._save_objective_selection)
        self.contrast_combo = self._build_term_combo("contrast")
        self.mount_combo = self._build_term_combo("mount")
        self.stain_combo = self._build_term_combo("stain")
        self.sample_combo = self._build_term_combo("sample")
        tag_form.addRow(self.tr("Objective:"), self.objective_combo)
        tag_form.addRow(self.tr("Contrast:"), self.contrast_combo)
        tag_form.addRow(self.tr("Mount:"), self.mount_combo)
        tag_form.addRow(self.tr("Stain:"), self.stain_combo)
        tag_form.addRow(self.tr("Sample:"), self.sample_combo)
        note_row = QWidget()
        note_row_layout = QHBoxLayout(note_row)
        note_row_layout.setContentsMargins(0, 0, 0, 0)
        note_row_layout.setSpacing(6)
        self.session_note_input = QLineEdit()
        self.session_note_input.setPlaceholderText(self.tr("Add a timestamped session note"))
        self.session_note_input.returnPressed.connect(self._add_session_note)
        note_row_layout.addWidget(self.session_note_input, 1)
        self.add_note_btn = QPushButton(self.tr("Add"))
        self.add_note_btn.clicked.connect(self._add_session_note)
        note_row_layout.addWidget(self.add_note_btn, 0)
        tag_form.addRow(self.tr("Note:"), note_row)
        left_layout.addWidget(tag_group)

        self.raw_processing_card = QFrame()
        self.raw_processing_card.setObjectName("sectionCard")
        self.raw_processing_card.setFrameShape(QFrame.NoFrame)
        raw_card_layout = QVBoxLayout(self.raw_processing_card)
        raw_card_layout.setContentsMargins(0, 0, 0, 0)
        raw_card_layout.setSpacing(0)
        self.raw_processing_toggle_btn = QToolButton()
        self.raw_processing_toggle_btn.setCheckable(True)
        self.raw_processing_toggle_btn.setChecked(False)
        self.raw_processing_toggle_btn.setToolButtonStyle(Qt.ToolButtonTextOnly)
        self.raw_processing_toggle_btn.setCursor(Qt.PointingHandCursor)
        self.raw_processing_toggle_btn.setStyleSheet(
            "QToolButton { border: none; padding: 10px 12px; text-align: left; font-weight: 600; }"
        )
        self.raw_processing_toggle_btn.toggled.connect(self._on_raw_processing_toggle_changed)
        raw_card_layout.addWidget(self.raw_processing_toggle_btn)

        self.raw_processing_body = QWidget()
        raw_body_layout = QVBoxLayout(self.raw_processing_body)
        raw_body_layout.setContentsMargins(12, 4, 12, 12)
        raw_body_layout.setSpacing(10)

        self.raw_controls = RawProcessingControls(
            self.raw_processing_body,
            show_tone_controls_when_disabled=True,
        )
        self.raw_controls.settingsChanged.connect(self._on_raw_processing_controls_changed)
        self.raw_controls.pickWhiteBalanceToggled.connect(self._toggle_active_raw_background_wb_pick)
        raw_body_layout.addWidget(self.raw_controls)
        self.raw_curve_preview_widget = RawCurvePreviewWidget(self.raw_processing_body)
        raw_body_layout.addWidget(self.raw_curve_preview_widget, 0, Qt.AlignHCenter)

        self.pending_raw_frame = QFrame()
        self.pending_raw_frame.setFrameShape(QFrame.NoFrame)
        self.pending_raw_frame.setObjectName("sectionCard")
        self.pending_raw_frame.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        pending_raw_layout = QVBoxLayout(self.pending_raw_frame)
        pending_raw_layout.setContentsMargins(12, 8, 12, 8)
        pending_raw_layout.setSpacing(8)

        pending_raw_top_row = QWidget()
        pending_raw_top_row_layout = QHBoxLayout(pending_raw_top_row)
        pending_raw_top_row_layout.setContentsMargins(0, 0, 0, 0)
        pending_raw_top_row_layout.setSpacing(8)

        self.pending_raw_count_label = QLabel(self.tr("Pending RAW captures: 0"), pending_raw_top_row)
        self.pending_raw_count_label.setStyleSheet("font-weight: 600;")
        self.pending_raw_count_label.setVisible(False)
        pending_raw_top_row_layout.addStretch(1)

        self.pending_raw_save_btn = QPushButton(self.tr("Save current"))
        self.pending_raw_save_btn.clicked.connect(self._commit_selected_pending_raw_capture)
        pending_raw_top_row_layout.addWidget(self.pending_raw_save_btn)
        self.pending_raw_save_all_btn = QPushButton(self.tr("Save all"))
        self.pending_raw_save_all_btn.clicked.connect(self._commit_all_pending_raw_captures)
        pending_raw_top_row_layout.addWidget(self.pending_raw_save_all_btn)
        self.pending_raw_apply_all_btn = QPushButton(self.tr("Apply settings to all pending"))
        self.pending_raw_apply_all_btn.clicked.connect(self._apply_current_raw_settings_to_all_pending)
        pending_raw_top_row_layout.addWidget(self.pending_raw_apply_all_btn)

        pending_raw_layout.addWidget(pending_raw_top_row)

        self.pending_raw_shortcuts_label = QLabel("")
        self.pending_raw_shortcuts_label.setWordWrap(True)
        self.pending_raw_shortcuts_label.setStyleSheet("color: #6b7280; font-size: 11px;")
        pending_raw_layout.addWidget(self.pending_raw_shortcuts_label)
        self.pending_raw_frame.setVisible(False)

        self.raw_processing_body.setVisible(False)
        raw_card_layout.addWidget(self.raw_processing_body)
        self._sync_raw_processing_controls_from_settings(self._raw_render_settings)
        self._restore_raw_capture_mode()
        self._update_pending_raw_controls()
        self._update_raw_processing_section_label(False)
        left_layout.addWidget(self.raw_processing_card)
        left_layout.addStretch(1)

        left_scroll = QScrollArea()
        left_scroll.setWidgetResizable(True)
        left_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        left_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        left_scroll.setFrameShape(QFrame.NoFrame)
        left_scroll.setWidget(left_panel)
        configure_sidebar_scroll(left_scroll, left_panel, SIDEBAR_MIN_WIDTH)
        main_splitter.addWidget(left_scroll)

        right_panel = QWidget()
        configure_splitter_pane(right_panel, min_width=360)
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(8)

        viewer_panel = QWidget()
        self.viewer_panel = viewer_panel
        viewer_layout = QVBoxLayout(viewer_panel)
        viewer_layout.setContentsMargins(0, 0, 0, 0)
        viewer_layout.setSpacing(8)

        viewer_header = QWidget()
        viewer_header_layout = QHBoxLayout(viewer_header)
        viewer_header_layout.setContentsMargins(0, 0, 0, 0)
        viewer_header_layout.setSpacing(8)
        viewer_text_layout = QVBoxLayout()
        viewer_text_layout.setContentsMargins(0, 0, 0, 0)
        viewer_text_layout.setSpacing(2)
        self.viewer_title_label = QLabel(self.tr("Last import"))
        self.viewer_title_label.setStyleSheet("font-weight: 600; font-size: 15px;")
        viewer_text_layout.addWidget(self.viewer_title_label)
        self.viewer_meta_label = QLabel("")
        self.viewer_meta_label.setWordWrap(True)
        self.viewer_meta_label.setStyleSheet("color: #6b7280;")
        viewer_text_layout.addWidget(self.viewer_meta_label)
        viewer_header_layout.addLayout(viewer_text_layout, 1)
        self.reset_view_btn = QPushButton(self.tr("Reset view"))
        self.reset_view_btn.setEnabled(False)
        self.reset_view_btn.clicked.connect(self._reset_viewer)
        viewer_header_layout.addWidget(self.reset_view_btn, 0, Qt.AlignTop)
        viewer_layout.addWidget(viewer_header)

        # Committed RAW images behave like normal images; the in-place RAW
        # re-render panel has been removed. Users review/edit RAW settings once
        # before saving via the pending RAW review flow.

        viewer_layout.addWidget(self.pending_raw_frame)

        self.live_image_label = ZoomableImageLabel()
        self.live_image_label.setObjectName("liveLabImageLabel")
        self.live_image_label.setMinimumSize(320, 240)
        self.live_image_label.set_pan_without_shift(True)
        self.live_image_label.set_measurement_active(False)
        self.live_image_label.set_show_measure_labels(False)
        self.live_image_label.set_show_measure_overlays(False)
        self.live_image_label.clicked.connect(self._on_live_image_clicked_for_background_wb)
        viewer_layout.addWidget(self.live_image_label, 1)

        self.session_gallery = ImageGalleryWidget(
            self.tr("Session Gallery"),
            parent=self,
            show_delete=True,
            show_badges=True,
            thumbnail_size=132,
            default_height=GALLERY_DEFAULT_HEIGHT,
            min_height=GALLERY_MIN_HEIGHT,
        )
        self.session_gallery.set_multi_select(True)
        self.session_gallery.imageClicked.connect(self._on_session_gallery_clicked)
        self.session_gallery.selectionChanged.connect(self._on_session_gallery_selection_changed)
        self.session_gallery.deleteRequested.connect(self._on_session_gallery_delete_requested)

        content_splitter = QSplitter(Qt.Vertical)
        content_splitter.setObjectName("gallerySplitter")
        content_splitter.setChildrenCollapsible(False)
        content_splitter.addWidget(viewer_panel)
        content_splitter.addWidget(self.session_gallery)
        content_splitter.setStretchFactor(0, 4)
        content_splitter.setStretchFactor(1, 1)
        install_persistent_splitter(
            content_splitter,
            key=self.SETTING_CONTENT_SPLITTER,
            default_sizes=[760, GALLERY_DEFAULT_HEIGHT],
            minimum_sizes=[240, GALLERY_MIN_HEIGHT],
        )
        right_layout.addWidget(content_splitter, 1)

        main_splitter.addWidget(right_panel)
        main_splitter.setStretchFactor(0, 0)
        main_splitter.setStretchFactor(1, 1)
        install_persistent_splitter(
            main_splitter,
            key=self.SETTING_MAIN_SPLITTER,
            default_sizes=[SIDEBAR_DEFAULT_WIDTH, 1180],
            minimum_sizes=[SIDEBAR_MIN_WIDTH, 360],
        )

        self.hint_bar = HintBar(self)
        self.hint_bar.set_wrap_mode(True)
        self._hint_controller = HintStatusController(self.hint_bar, self)
        root_layout.addWidget(self.hint_bar)

    def _register_hint_widgets(self) -> None:
        self._register_hint_widget(
            self.session_mode_combo,
            self.tr("Choose between live folder watching and retrospective log-only capture."),
            disabled_hint=self.tr("Stop the current session before changing capture mode."),
        )
        self._register_hint_widget(
            self.watch_dir_input,
            self.tr("Folder that Sporely watches for new microscope captures."),
        )
        self._register_hint_widget(
            self.browse_btn,
            self.tr("Choose the folder your microscope camera saves into."),
        )
        self._register_hint_widget(
            self.objective_combo,
            self.tr("Objective stored on newly imported microscope images."),
        )
        self._register_hint_widget(
            self.contrast_combo,
            self.tr("Contrast method stored on newly imported microscope images."),
        )
        self._register_hint_widget(
            self.mount_combo,
            self.tr("Mount medium stored on newly imported microscope images."),
        )
        self._register_hint_widget(
            self.stain_combo,
            self.tr("Stain stored on newly imported microscope images."),
        )
        self._register_hint_widget(
            self.sample_combo,
            self.tr("Sample type stored on newly imported microscope images."),
        )
        self._register_hint_widget(
            self.session_note_input,
            self.tr("Write a timestamped note into the current lab session log."),
            disabled_hint=self.tr("Start a Live Lab session before adding notes."),
        )
        self._register_hint_widget(
            self.raw_processing_toggle_btn,
            self.tr("Show or hide the simplified RAW processing controls for future session-level captures."),
        )
        self._register_hint_widget(
            self.raw_controls,
            self.tr("Shared RAW processing controls for future session-level captures."),
        )
        self._register_hint_widget(
            self.raw_controls.white_balance_selector,
            self.tr("Choose the white balance mode for future RAW captures."),
        )
        self._register_hint_widget(
            self.raw_controls.auto_levels_checkbox,
            self.tr("Apply automatic levels to future RAW captures."),
        )
        self._register_hint_widget(
            self.raw_controls.tone_curve_checkbox,
            self.tr("Enable the luminance tone curve for future RAW captures."),
        )
        self._register_hint_widget(
            self.raw_controls.curve_strength_slider,
            self.tr("Adjust the tone curve strength."),
        )
        self._register_hint_widget(
            self.raw_controls.curve_midpoint_slider,
            self.tr("Adjust the tone curve midpoint. Lower values automatically increase dark boost."),
        )
        self._register_hint_widget(
            self.raw_controls.pick_button,
            self.tr("Sample a neutral point from the preview and set the current RAW white balance."),
        )
        self._register_hint_widget(
            self.pending_raw_save_btn,
            self.tr("Commit the selected pending RAW capture using the current RAW settings."),
            disabled_hint=self.tr("Choose a pending RAW capture first."),
        )
        self._register_hint_widget(
            self.pending_raw_save_all_btn,
            self.tr("Commit every pending RAW capture using its current RAW settings."),
            disabled_hint=self.tr("No pending RAW captures are waiting to be saved."),
        )
        self._register_hint_widget(
            self.pending_raw_apply_all_btn,
            self.tr("Update all pending RAW captures to use the current RAW settings."),
            disabled_hint=self.tr("No pending RAW captures are waiting to be reviewed."),
        )
        self._register_hint_widget(
            self.add_note_btn,
            self.tr("Append the note to the current session log with the current timestamp."),
            disabled_hint=self.tr("Start a Live Lab session before adding notes."),
        )
        self._register_hint_widget(
            self.start_stop_btn,
            self.tr("Start or stop watching the folder for new microscope captures."),
            disabled_hint=self.tr("Choose a current observation and an existing watched folder first."),
        )
        self._register_hint_widget(
            self.reset_view_btn,
            self.tr("Fit the selected session image back into view."),
        )
        self._register_hint_widget(
            self.session_gallery,
            self.tr(
                "Click a thumbnail to inspect it here. Pending RAW items appear here with an UNSAVED badge and use their preview render."
            ),
        )

    def _register_hint_widget(
        self,
        widget: QWidget,
        hint_text: str | None,
        tone: str = "info",
        allow_when_disabled: bool = False,
        disabled_hint: str | None = None,
    ) -> None:
        if self._hint_controller is None:
            return
        self._hint_controller.register_widget(
            widget,
            hint_text,
            tone=tone,
            allow_when_disabled=allow_when_disabled,
            disabled_hint=disabled_hint,
        )

    def _raw_processing_preset_context(self) -> dict[str, str | None]:
        """Return the context key that future RAW presets will use."""
        objective_value = self._selected_combo_value(self.objective_combo)
        objective_key = str(objective_value).strip() if objective_value is not None else ""
        objective_key = objective_key or None
        objective_label = str(self.objective_combo.currentText() or "").strip() or None
        return {
            "capture_source": "live_lab",
            "instrument": "microscope",
            "objective_name": objective_key,
            "objective_label": objective_label,
            "contrast": DatabaseTerms.canonicalize("contrast", self._selected_combo_value(self.contrast_combo)),
            "mount_medium": DatabaseTerms.canonicalize("mount", self._selected_combo_value(self.mount_combo)),
            "stain": DatabaseTerms.canonicalize("stain", self._selected_combo_value(self.stain_combo)),
            "sample_type": DatabaseTerms.canonicalize("sample", self._selected_combo_value(self.sample_combo)),
        }

    def _raw_processing_settings_key(self, context: dict[str, str | None] | None = None) -> str:
        context = dict(context or self._raw_processing_preset_context())
        prefix = getattr(self, "SETTING_RAW_PROCESSING_PRESET_PREFIX", None) or LiveLabTab.SETTING_RAW_PROCESSING_PRESET_PREFIX
        key_context = {
            "capture_source": context.get("capture_source"),
            "instrument": context.get("instrument"),
            "objective_name": context.get("objective_name"),
            "contrast": context.get("contrast"),
            "mount_medium": context.get("mount_medium"),
            "stain": context.get("stain"),
            "sample_type": context.get("sample_type"),
        }
        serialized_context = json.dumps(key_context, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return f"{prefix}::{serialized_context}"

    def _load_raw_processing_settings_for_current_context(self) -> RawRenderSettings:
        raw_value = str(SettingsDB.get_setting(self._raw_processing_settings_key(), "") or "").strip()
        if not raw_value:
            return self._raw_render_settings
        try:
            payload = json.loads(raw_value)
        except Exception:
            return self._raw_render_settings
        settings = RawRenderSettings.from_dict(payload)
        sync_helper = getattr(self, "_sync_raw_processing_controls_from_settings", None)
        if callable(sync_helper):
            sync_helper(settings)
        else:
            LiveLabTab._sync_raw_processing_controls_from_settings(self, settings)
        return settings

    def _save_raw_processing_settings_for_current_context(self, settings: RawRenderSettings | None = None) -> None:
        resolved_settings = RawRenderSettings.from_dict(settings or self._raw_settings_from_controls())
        SettingsDB.set_setting(
            self._raw_processing_settings_key(),
            json.dumps(resolved_settings.to_dict(), sort_keys=True),
        )
        self._raw_render_settings = resolved_settings

    def _normalize_raw_companion_source_preference(self, value: str | None) -> str:
        return normalize_raw_companion_source_preference(value)

    def _selected_raw_companion_source_preference(self) -> str:
        selector = getattr(self, "raw_companion_source_selector", None)
        fallback = getattr(self, "_raw_companion_source_preference", RAW_COMPANION_SOURCE_PREFERENCE_PREFER_RAW)
        if selector is not None:
            fallback = selector.selected_value(fallback)
        return self._normalize_raw_companion_source_preference(fallback)

    def _restore_raw_companion_source_preference(self) -> str:
        saved = self._normalize_raw_companion_source_preference(
            SettingsDB.get_setting(
                SETTING_RAW_COMPANION_SOURCE_PREFERENCE,
                RAW_COMPANION_SOURCE_PREFERENCE_PREFER_RAW,
            )
        )
        self._raw_companion_source_preference = saved
        selector = getattr(self, "raw_companion_source_selector", None)
        if selector is not None:
            selector.set_selected_value(saved)
        return saved

    def _save_raw_companion_source_preference(self, preference: str | None = None) -> str:
        normalized = self._normalize_raw_companion_source_preference(
            preference or self._selected_raw_companion_source_preference()
        )
        self._raw_companion_source_preference = normalized
        SettingsDB.set_setting(SETTING_RAW_COMPANION_SOURCE_PREFERENCE, normalized)
        return normalized

    def _on_raw_companion_source_preference_changed(self, value) -> None:
        self._save_raw_companion_source_preference(str(value or ""))

    @staticmethod
    def _raw_processing_setting_float(key: str, default: float, minimum: float, maximum: float) -> float:
        try:
            value = float(SettingsDB.get_setting(key, default))
        except Exception:
            value = float(default)
        if not np.isfinite(value):
            value = float(default)
        return float(np.clip(value, float(minimum), float(maximum)))

    @staticmethod
    def _raw_processing_setting_bool(key: str, default: bool) -> bool:
        value = SettingsDB.get_setting(key, default)
        if isinstance(value, bool):
            return value
        if value is None:
            return bool(default)
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if not text:
            return bool(default)
        return text in {"1", "true", "yes", "on", "enabled"}

    def _raw_processing_preferences(self) -> dict[str, float | bool]:
        return {
            "dark_cutoff": self._raw_processing_setting_float(
                SETTING_RAW_PROCESSING_DARK_CUTOFF,
                0.0,
                0.0,
                0.02,
            ),
            "bright_cutoff": self._raw_processing_setting_float(
                SETTING_RAW_PROCESSING_BRIGHT_CUTOFF,
                0.0,
                0.0,
                0.02,
            ),
        }

    def _raw_settings_from_controls(self, *, update_session_settings: bool = True) -> RawRenderSettings:
        controls = getattr(self, "raw_controls", None)
        if controls is None:
            settings = RawRenderSettings.from_dict(getattr(self, "_raw_render_settings", None))
        else:
            settings = controls.settings()
        if update_session_settings:
            self._raw_render_settings = settings
        return settings

    def _raw_auto_level_settings_for_source(
        self,
        source_path: str | Path,
        settings: RawRenderSettings | None,
    ) -> RawRenderSettings:
        return compute_auto_level_adjusted_settings_from_source(source_path, settings=settings)

    def _sync_raw_processing_controls_from_settings(
        self,
        settings: RawRenderSettings | None = None,
        *,
        update_session_settings: bool = True,
        auto_level_settings: RawRenderSettings | None = None,
    ) -> None:
        settings = RawRenderSettings.from_dict(settings or getattr(self, "_raw_render_settings", None))
        if update_session_settings:
            self._raw_render_settings = settings
        controls = getattr(self, "raw_controls", None)
        if controls is not None:
            if auto_level_settings is not None:
                controls.set_auto_level_settings(auto_level_settings)
            controls.set_settings(settings)
        self._set_raw_tone_controls_enabled(bool(settings.tone_curve_enabled))
        self._refresh_raw_processing_context_ui()
        self._update_raw_processing_section_label(bool(getattr(self, "raw_processing_toggle_btn", None) and self.raw_processing_toggle_btn.isChecked()))

    def _set_raw_tone_controls_enabled(self, enabled: bool) -> None:
        controls = getattr(self, "raw_controls", None)
        if controls is not None:
            controls.set_tone_controls_enabled(bool(enabled))
        return

    def _raw_settings_for_copy(self, settings: RawRenderSettings) -> RawRenderSettings:
        resolved = RawRenderSettings.from_dict(settings)
        if resolved.wb_multipliers is not None:
            return replace(
                resolved,
                wb_selection=None,
                wb_sample_point=None,
                wb_sample_size=None,
                wb_selection_space="inherited_multipliers",
            )
        return resolved

    def _raw_white_balance_label(self, settings: RawRenderSettings | None) -> str:
        resolved = RawRenderSettings.from_dict(settings)
        wb_mode = str(resolved.white_balance_mode or "camera").strip().lower() or "camera"
        if wb_mode == "auto":
            return self.tr("Auto WB")
        if wb_mode == "custom" or resolved.wb_multipliers is not None:
            return self.tr("Custom WB")
        return self.tr("Camera WB")

    def _raw_white_balance_readout_text(self, settings: RawRenderSettings | None) -> str:
        resolved = RawRenderSettings.from_dict(settings)
        wb_mode = str(resolved.white_balance_mode or "camera").strip().lower() or "camera"
        if wb_mode == "auto":
            return self.tr("Auto WB")
        if resolved.wb_multipliers is not None:
            return self.tr("Custom WB {r:.2f} / {g:.2f} / {b:.2f}").format(
                r=float(resolved.wb_multipliers[0]),
                g=float(resolved.wb_multipliers[1]),
                b=float(resolved.wb_multipliers[2]),
            )
        if wb_mode == "custom":
            return self.tr("Custom WB set")
        return self.tr("Camera WB")

    def _raw_settings_summary_text(self, settings: RawRenderSettings | None) -> str:
        resolved = RawRenderSettings.from_dict(settings)
        wb_label = self._raw_white_balance_label(resolved)
        levels_label = self.tr("Auto levels") if resolved.auto_levels else self.tr("levels off")
        if resolved.tone_curve_enabled:
            curve_label = self.tr("Curve {strength} / mid {midpoint}").format(
                strength=int(round(float(resolved.tone_curve_strength) * 100.0)),
                midpoint=int(round(float(resolved.tone_curve_midpoint) * 100.0)),
            )
        else:
            curve_label = self.tr("Curve off")
        return " \u00b7 ".join([wb_label, levels_label, curve_label])

    @staticmethod
    def _format_raw_percent(value: float | int | None, *, decimals: int = 1) -> str:
        try:
            numeric = float(value)
        except Exception:
            numeric = 0.0
        if not np.isfinite(numeric):
            numeric = 0.0
        return f"{max(0.0, numeric) * 100.0:.{decimals}f}%"

    def _raw_settings_info_text(self, settings: RawRenderSettings | None) -> str:
        resolved = RawRenderSettings.from_dict(settings)
        dark_cutoff = LiveLabTab._format_raw_percent(resolved.black_percentile, decimals=2)
        bright_cutoff = LiveLabTab._format_raw_percent(1.0 - float(resolved.white_percentile), decimals=2)
        soft_tails_label = self.tr("on") if resolved.auto_levels_soft_tails else self.tr("off")
        curve_strength = LiveLabTab._format_raw_percent(resolved.tone_curve_strength, decimals=0)
        curve_midpoint = LiveLabTab._format_raw_percent(resolved.tone_curve_midpoint, decimals=0)
        return " \u00b7 ".join(
            [
                self._raw_white_balance_label(resolved),
                self.tr("Dark cutoff {value}").format(value=dark_cutoff),
                self.tr("Bright cutoff {value}").format(value=bright_cutoff),
                self.tr("Soft tails {state}").format(state=soft_tails_label),
                self.tr("Curve strength {value}").format(value=curve_strength),
                self.tr("Curve midpoint {value}").format(value=curve_midpoint),
            ]
        )

    def _raw_processing_summary_text(self) -> str:
        return self._raw_settings_summary_text(self._raw_settings_from_controls(update_session_settings=False))

    def _raw_processing_hint_text(self) -> str:
        return self.tr(
            "Pick Camera, Auto, or Custom WB, then click Pick to sample a neutral patch from the preview. "
            "Low midpoint values lift shadows."
        )

    def _pending_raw_review_hint_text(self) -> str:
        return self.tr(
            "Review mode: use ←/→ to move between RAW captures, Delete/Backspace/Cmd/Ctrl+D to remove the current image, "
            "and Enter to save current."
        )

    def _committed_raw_review_hint_text(self) -> str:
        return self.tr("Delete/Backspace/Cmd/Ctrl+D remove the selected saved image.")

    def _raw_edit_hint_text(self) -> str:
        return self.tr(
            "Editing mode: choose a WB mode, use Pick to sample the preview, then click Apply re-render when ready."
        )

    def _current_raw_hint_text(self) -> str:
        if getattr(self, "_raw_edit_session", None) is not None:
            return self._raw_edit_hint_text()
        if self._current_pending_raw_capture() is not None:
            return self._pending_raw_review_hint_text()
        selected_targets = self._selected_raw_processing_targets()
        if selected_targets:
            if len(selected_targets) > 1:
                return self.tr("Settings will be applied to selected images.")
            return self._raw_processing_hint_text()
        if self._selected_committed_image_id() is not None:
            return self._committed_raw_review_hint_text()
        raw_body = getattr(self, "raw_processing_body", None)
        if raw_body is not None and raw_body.isVisible():
            return self._raw_processing_hint_text()
        return self._default_hint_text

    @staticmethod
    def _metadata_text(value) -> str | None:
        text = str(value or "").strip()
        return text or None

    @staticmethod
    def _selected_combo_value(combo) -> object | None:
        if combo is None:
            return None
        unset_texts = {"not_set", "not set", "-", "—"}
        selected_item_fn = getattr(combo, "selected_item", None)
        if callable(selected_item_fn):
            try:
                item = selected_item_fn()
            except Exception:
                item = None
            if item is not None:
                value = getattr(item, "value", None)
                if value is not None:
                    if isinstance(value, str):
                        value = value.strip()
                        if value:
                            return value
                    else:
                        return value
                for attr in ("display_text", "pill_text", "tooltip"):
                    text = str(getattr(item, attr, "") or "").strip()
                    if text and text.lower() not in unset_texts:
                        return text
                return None
        try:
            value = combo.currentData()
        except Exception:
            value = None
        if value is not None:
            if isinstance(value, str):
                value = value.strip()
                if value:
                    return value
                return None
            return value
        try:
            text = str(combo.currentText() or "").strip()
        except Exception:
            text = ""
        if not text or text.lower() in unset_texts:
            return None
        return text

    def _objective_key_from_metadata(self, metadata: dict | None) -> str | None:
        data = dict(metadata or {})
        lab_metadata = data.get("lab_metadata")
        lab_data = dict(lab_metadata) if isinstance(lab_metadata, dict) else {}
        candidates = (
            data.get("objective_name"),
            data.get("objective_label"),
            lab_data.get("objective_name"),
            lab_data.get("objective_label"),
        )
        objectives = load_objectives()
        for candidate in candidates:
            text = LiveLabTab._metadata_text(candidate)
            if not text:
                continue
            return resolve_objective_key(text, objectives) or text
        return None

    def _microscope_state_from_metadata(self, metadata: dict | None) -> dict[str, str | None]:
        data = dict(metadata or {})
        objective_key = LiveLabTab._objective_key_from_metadata(self, data)
        return {
            "objective_name": objective_key,
            "contrast": DatabaseTerms.canonicalize("contrast", data.get("contrast")),
            "mount_medium": DatabaseTerms.canonicalize("mount", data.get("mount_medium")),
            "stain": DatabaseTerms.canonicalize("stain", data.get("stain")),
            "sample_type": DatabaseTerms.canonicalize("sample", data.get("sample_type")),
        }

    def _microscope_tag_for_metadata(self, metadata: dict | None) -> tuple[str | None, str | None]:
        data = dict(metadata or {})
        objective_name = LiveLabTab._objective_key_from_metadata(self, data)
        if not objective_name:
            return None, None

        objectives = load_objectives()
        objective = objectives.get(str(objective_name))
        tag_text = objective_short_label(objective, str(objective_name))
        if not tag_text:
            tag_text = objective_display_name(objective, str(objective_name)) if objective else str(objective_name)
        contrast = DatabaseTerms.canonicalize("contrast", data.get("contrast"))
        if contrast and str(contrast).strip().lower() not in {"not_set", "not set"}:
            tag_text = f"{tag_text} {DatabaseTerms.translate('contrast', contrast)}"
        return tag_text, objective_color(objective, str(objective_name))

    def _apply_microscope_state_to_controls(self, metadata: dict | None) -> None:
        state = self._microscope_state_from_metadata(metadata)
        objective_combo = getattr(self, "objective_combo", None)
        if objective_combo is not None:
            objective_value = state.get("objective_name")
            if objective_value is not None and str(objective_value).strip():
                with QSignalBlocker(objective_combo):
                    if hasattr(objective_combo, "set_selected_value"):
                        objective_combo.set_selected_value(objective_value, emit=False)
                    elif hasattr(objective_combo, "findData"):
                        index = objective_combo.findData(objective_value)
                        if index >= 0:
                            objective_combo.setCurrentIndex(index)

        for combo_name, value in (
            ("contrast_combo", state.get("contrast")),
            ("mount_combo", state.get("mount_medium")),
            ("stain_combo", state.get("stain")),
            ("sample_combo", state.get("sample_type")),
        ):
            combo = getattr(self, combo_name, None)
            if combo is None:
                continue
            with QSignalBlocker(combo):
                try:
                    index = combo.findData(value)
                except Exception:
                    index = -1
                if index < 0:
                    index = 0 if getattr(combo, "count", lambda: 0)() else -1
                if index >= 0:
                    combo.setCurrentIndex(index)
        update_alerts = getattr(self, "_update_lab_state_combo_alerts", None)
        if callable(update_alerts):
            update_alerts()
        refresh_viewer_objective_tag = getattr(self, "_refresh_viewer_objective_tag_from_current_state", None)
        if callable(refresh_viewer_objective_tag):
            refresh_viewer_objective_tag()

    def _refresh_viewer_objective_tag_from_current_state(self, *_args) -> None:
        update_viewer_objective_tag = getattr(self, "_update_viewer_objective_tag", None)
        if callable(update_viewer_objective_tag):
            update_viewer_objective_tag(None, use_current_state=True)

    def _update_viewer_objective_tag(self, metadata: dict | None, *, use_current_state: bool = False) -> None:
        label = getattr(self, "live_image_label", None)
        if label is None:
            return
        if use_current_state:
            current_lab_metadata = getattr(self, "_current_lab_metadata", None)
            if callable(current_lab_metadata):
                try:
                    metadata = current_lab_metadata()
                except Exception:
                    metadata = None
        set_text = getattr(label, "set_objective_text", None)
        set_color = getattr(label, "set_objective_color", None)
        tag_text, tag_color = LiveLabTab._microscope_tag_for_metadata(self, metadata)
        if callable(set_text):
            try:
                set_text(tag_text or "")
            except Exception:
                pass
        if callable(set_color):
            try:
                set_color(tag_color or objective_color(None, None))
            except Exception:
                pass

    def _session_gallery_selected_keys(self) -> list[object]:
        gallery = getattr(self, "session_gallery", None)
        if gallery is not None:
            selected_keys_fn = getattr(gallery, "selected_keys", None)
            if callable(selected_keys_fn):
                try:
                    keys = [key for key in list(selected_keys_fn()) if key is not None]
                except Exception:
                    keys = []
                if keys:
                    return keys
            selected_paths_fn = getattr(gallery, "selected_paths", None)
            if callable(selected_paths_fn):
                try:
                    selected_paths = [str(path) for path in selected_paths_fn() if path]
                except Exception:
                    selected_paths = []
                if selected_paths:
                    return selected_paths

        current_pending = self._current_pending_raw_capture()
        if current_pending is not None:
            pending_key = self._pending_raw_gallery_key(current_pending)
            if pending_key is not None:
                return [pending_key]

        selected_image_id = self._selected_committed_image_id()
        if selected_image_id is not None:
            return [selected_image_id]
        return []

    def _selected_raw_processing_targets(self) -> list[tuple[str, object, object]]:
        targets: list[tuple[str, object, object]] = []
        captures = getattr(self, "_pending_raw_captures", [])
        selected_keys = self._session_gallery_selected_keys()
        selected_key_set = {key for key in selected_keys if key is not None}

        current_pending = self._current_pending_raw_capture()
        if current_pending is not None:
            pending_key = self._pending_raw_gallery_key(current_pending)
            if pending_key is not None and pending_key not in selected_key_set:
                targets = [("pending", self._selected_pending_raw_index_value(), current_pending)]

        selected_image_id = self._selected_committed_image_id()
        if not targets and selected_image_id is not None and selected_image_id not in selected_key_set:
            image = ImageDB.get_image(selected_image_id)
            if image is not None and self._raw_editable_image_session(image) is not None:
                targets = [("image", selected_image_id, image)]

        if not targets:
            for key in selected_keys:
                pending_index = self._pending_raw_capture_index_for_key(key)
                if pending_index is not None and 0 <= pending_index < len(captures):
                    capture = captures[pending_index]
                    if isinstance(capture, PendingRawCapture):
                        targets.append(("pending", pending_index, capture))
                        continue
                try:
                    image_id = int(key)
                except Exception:
                    continue
                image = ImageDB.get_image(image_id)
                if image is None:
                    continue
                if self._raw_editable_image_session(image) is not None:
                    targets.append(("image", image_id, image))

        visible_target = self._visible_raw_processing_target()
        if visible_target is not None:
            if not targets:
                return [visible_target]
            if len(targets) == 1 and (targets[0][0], targets[0][1]) != (visible_target[0], visible_target[1]):
                return [visible_target]
        return targets

    def _visible_raw_processing_target(self) -> tuple[str, object, object] | None:
        label = getattr(self, "live_image_label", None)
        if label is None:
            return None

        visible_path = str(getattr(label, "_full_image_path", "") or "").strip()
        if not visible_path:
            return None

        try:
            visible_path_obj = Path(visible_path)
        except Exception:
            visible_path_obj = None

        current_pending = self._current_pending_raw_capture()
        if current_pending is not None:
            for candidate in (current_pending.preview_path, current_pending.source_path):
                candidate_text = str(candidate or "").strip()
                if not candidate_text:
                    continue
                try:
                    if visible_path_obj is not None and Path(candidate_text) == visible_path_obj:
                        pending_index = self._selected_pending_raw_index_value()
                        return ("pending", pending_index if pending_index >= 0 else 0, current_pending)
                except Exception:
                    if candidate_text == visible_path:
                        pending_index = self._selected_pending_raw_index_value()
                        return ("pending", pending_index if pending_index >= 0 else 0, current_pending)

        for image_id in reversed(list(getattr(self, "_session_image_ids", []) or [])):
            image = ImageDB.get_image(image_id)
            if image is None:
                continue
            image_path = str(image.get("filepath") or "").strip()
            if not image_path:
                continue
            try:
                matches_visible = visible_path_obj is not None and Path(image_path) == visible_path_obj
            except Exception:
                matches_visible = image_path == visible_path
            if not matches_visible:
                continue
            if self._raw_editable_image_session(image) is None:
                return None
            return ("image", image_id, image)

        return None

    def _raw_processing_controls_visible(self) -> bool:
        if self._raw_edit_session is not None:
            return True
        if self._current_pending_raw_capture() is not None:
            return True
        return bool(self._selected_raw_processing_targets())

    def _update_raw_processing_visibility(self) -> None:
        card = getattr(self, "raw_processing_card", None)
        body = getattr(self, "raw_processing_body", None)
        visible = self._raw_processing_controls_visible()
        if card is not None:
            card.setVisible(bool(visible))
        if body is None:
            return
        if not visible:
            body.setVisible(False)
            return
        toggle = getattr(self, "raw_processing_toggle_btn", None)
        body.setVisible(bool(toggle.isChecked()) if toggle is not None else False)

    @staticmethod
    def _preserve_raw_wb_fields(base_settings: RawRenderSettings, resolved_settings: RawRenderSettings) -> RawRenderSettings:
        wb_mode = str(resolved_settings.white_balance_mode or "camera").strip().lower() or "camera"
        if wb_mode == "custom":
            if base_settings.wb_multipliers is not None:
                return replace(
                    resolved_settings,
                    wb_multipliers=base_settings.wb_multipliers,
                    wb_selection=base_settings.wb_selection,
                    wb_multiplier_space=base_settings.wb_multiplier_space,
                    wb_sample_point=base_settings.wb_sample_point,
                    wb_sample_size=base_settings.wb_sample_size,
                    wb_sample_base_mode=base_settings.wb_sample_base_mode,
                    wb_selection_space=base_settings.wb_selection_space,
                )
            return replace(
                resolved_settings,
                wb_multipliers=None,
                wb_selection=None,
                wb_multiplier_space=None,
                wb_sample_point=None,
                wb_sample_size=None,
                wb_sample_base_mode=None,
                wb_selection_space=None,
            )
        return replace(
            resolved_settings,
            wb_multipliers=None,
            wb_selection=None,
            wb_multiplier_space=None,
            wb_sample_point=None,
            wb_sample_size=None,
            wb_sample_base_mode=None,
            wb_selection_space=None,
        )

    def _apply_raw_settings_to_pending_capture(self, capture: PendingRawCapture, settings: RawRenderSettings) -> bool:
        if capture is None:
            return False
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        resolved_settings = RawRenderSettings.from_dict(settings)
        base_settings = RawRenderSettings.from_dict(capture.raw_settings)
        updated_settings = self._preserve_raw_wb_fields(base_settings, resolved_settings)
        capture.raw_settings = updated_settings
        if self._normalize_raw_capture_mode(self._selected_raw_capture_mode()) != self.RAW_CAPTURE_MODE_REVIEW:
            return True

        try:
            preview_rgb, _processing_settings = self._raw_preview_rgb_for_source(capture.source_path, updated_settings)
            capture.preview_rgb = preview_rgb
            preview_dir = self._pending_raw_preview_dir()
            preview_dir.mkdir(parents=True, exist_ok=True)
            preview_path = capture.preview_path
            if preview_path is None:
                preview_path = preview_dir / f"{capture.source_path.stem}_{uuid4().hex}.jpg"
            capture.preview_path = Path(preview_path)
            capture.status = "pending"
            _raw_timing_log(
                "pending RAW capture settings applied",
                start,
                detail=f"{capture.source_path.name} preview={preview_rgb.shape[1]}x{preview_rgb.shape[0]}",
            )
            return True
        except RawRenderingUnavailableError as exc:
            capture.status = "failed"
            self._show_status(
                self.tr("RAW preview unavailable for {name}: {error}").format(
                    name=capture.source_path.name,
                    error=str(exc),
                ),
                tone="warning",
                timeout_ms=6000,
            )
        except RuntimeError as exc:
            capture.status = "failed"
            self._show_status(
                self.tr("Could not render RAW preview for {name}: {error}").format(
                    name=capture.source_path.name,
                    error=str(exc),
                ),
                tone="warning",
                timeout_ms=6000,
            )
        return False

    def _commit_raw_edit_session(self, session: RawEditSession) -> bool:
        if session is None:
            return False

        temp_dir = self._raw_edit_preview_dir()
        temp_dir.mkdir(parents=True, exist_ok=True)
        temp_output = temp_dir / f"{session.source_raw_path.stem}_{session.image_id}_{uuid4().hex}.jpg"
        final_path = Path(session.current_derivative_path)
        final_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            rendered_path = render_raw_image(
                session.source_raw_path,
                settings=session.working_settings,
                output_path=temp_output,
                source_capture_datetime=session.source_capture_datetime,
            )
            reader = QImageReader(str(rendered_path))
            size = reader.size()
            width = int(size.width()) if size.isValid() else 0
            height = int(size.height()) if size.isValid() else 0
            if width <= 0 or height <= 0:
                width = height = 0
            os.replace(rendered_path, final_path)
            updated_lab_metadata = copy.deepcopy(session.image_lab_metadata)
            source_metadata = dict((updated_lab_metadata.get("raw_processing") or {}).get("source") or {})
            source_mime_type = source_metadata.get("mime_type")
            raw_processing = build_raw_processing_metadata(
                session.source_raw_path,
                final_path,
                session.working_settings,
                width=width,
                height=height,
                source_mime_type=str(source_mime_type or "") or None,
                source_capture_datetime=session.source_capture_datetime,
                rendered_at=datetime.now(),
            )
            updated_lab_metadata["raw_processing"] = raw_processing
            ImageDB.update_image(
                session.image_id,
                filepath=str(final_path),
                lab_metadata=updated_lab_metadata,
            )
            try:
                generate_all_sizes(str(final_path), int(session.image_id))
            except Exception as exc:
                self._show_status(
                    self.tr("Thumbnail refresh warning for {name}: {error}").format(
                        name=final_path.name,
                        error=str(exc),
                    ),
                    tone="warning",
                    timeout_ms=5000,
                )
            self._invalidate_thumbnail_caches_for_raw_image(int(session.image_id), final_path)
            return True
        except RawRenderingUnavailableError as exc:
            self._show_status(
                self.tr("RAW re-render unavailable for {name}: {error}").format(
                    name=session.source_raw_path.name,
                    error=str(exc),
                ),
                tone="warning",
                timeout_ms=6000,
            )
        except Exception as exc:
            self._show_status(
                self.tr("Could not re-render RAW image {name}: {error}").format(
                    name=session.source_raw_path.name,
                    error=str(exc),
                ),
                tone="warning",
                timeout_ms=6000,
            )
        finally:
            try:
                temp_output.unlink(missing_ok=True)
            except Exception:
                pass
        return False

    def _refresh_raw_processing_context_ui(self) -> None:
        updater = getattr(self, "_update_raw_processing_visibility", None)
        if callable(updater):
            updater()
        updater = getattr(self, "_update_raw_processing_pick_button", None)
        if callable(updater):
            updater()
        updater = getattr(self, "_refresh_raw_curve_preview", None)
        if callable(updater):
            updater()
        hint_setter = getattr(self, "_set_hint", None)
        if callable(hint_setter):
            hint_setter(self._current_raw_hint_text())

    @staticmethod
    def _raw_preview_decode_mode(settings: RawRenderSettings) -> str:
        sample_base_mode = str(settings.wb_sample_base_mode or "").strip().lower() or None
        if sample_base_mode not in {"camera", "auto"}:
            sample_base_mode = "auto" if settings.white_balance_mode == "auto" else "camera"
        return sample_base_mode if sample_base_mode in {"camera", "auto"} else "camera"

    def _raw_preview_proxy_cache_key(
        self,
        source_path: str | Path,
        settings: RawRenderSettings | dict | None,
    ) -> tuple[str, int, int, str] | None:
        path = Path(source_path)
        try:
            stat = path.stat()
        except Exception:
            return None
        render_settings = RawRenderSettings.from_dict(settings)
        decode_mode = self._raw_preview_decode_mode(render_settings)
        try:
            source_key = str(path.resolve())
        except Exception:
            source_key = str(path)
        return (source_key, int(stat.st_mtime_ns), int(stat.st_size), decode_mode)

    def _raw_preview_proxy_for_source(
        self,
        source_path: str | Path,
        settings: RawRenderSettings | dict | None,
    ) -> np.ndarray:
        cache = getattr(self, "_pending_raw_preview_proxy_cache", None)
        if cache is None:
            cache = {}
            self._pending_raw_preview_proxy_cache = cache
        cache_key = self._raw_preview_proxy_cache_key(source_path, settings)
        if cache_key is None:
            proxy = render_raw_preview_proxy_rgb(source_path, settings=settings)
            return np.asarray(proxy, dtype=np.float32)
        cached = cache.get(cache_key)
        if isinstance(cached, np.ndarray):
            return cached
        proxy = render_raw_preview_proxy_rgb(source_path, settings=settings)
        result = np.asarray(proxy, dtype=np.float32)
        cache[cache_key] = result
        return result

    @staticmethod
    def _rgb_to_pixmap(rgb: np.ndarray) -> QPixmap:
        arr = np.asarray(rgb)
        if arr.dtype == np.uint8:
            image8 = np.ascontiguousarray(arr[..., :3])
        else:
            image = np.asarray(arr[..., :3], dtype=np.float32)
            image = np.clip(image, 0.0, 1.0)
            image8 = np.ascontiguousarray(np.rint(image * 255.0).astype(np.uint8))
        image8 = np.ascontiguousarray(image8)
        height, width = image8.shape[:2]
        qimage = QImage(
            image8.data,
            width,
            height,
            int(image8.strides[0]),
            QImage.Format.Format_RGB888,
        ).copy()
        return QPixmap.fromImage(qimage)

    def _raw_preview_rgb_for_source(
        self,
        source_path: str | Path,
        settings: RawRenderSettings | dict | None,
    ) -> tuple[np.ndarray, RawRenderSettings]:
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        source_name = Path(source_path).name
        resolved_settings = RawRenderSettings.from_dict(settings)
        proxy_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        preview_rgb = self._raw_preview_proxy_for_source(source_path, resolved_settings)
        preview_rgb = _resize_preview_rgb(preview_rgb, RAW_PREVIEW_MAX_DIM)
        _raw_timing_log(
            "RAW preview proxy decode/resize",
            proxy_start,
            detail=f"{source_name} -> {preview_rgb.shape[1]}x{preview_rgb.shape[0]}",
        )
        processing_settings = resolved_settings
        if (
            processing_settings.white_balance_mode in {"background", "custom"}
            and processing_settings.wb_multipliers is None
            and processing_settings.wb_selection is not None
        ):
            wb_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            background_wb = estimate_white_balance_from_background(
                preview_rgb,
                rect=processing_settings.wb_selection,
            )
            _raw_timing_log("background WB estimate", wb_start, detail=source_name)
            processing_settings = replace(
                processing_settings,
                white_balance_mode="custom",
                wb_multipliers=(
                    float(background_wb[0]),
                    float(background_wb[1]),
                    float(background_wb[2]),
                ),
                wb_multiplier_space="post_decode_rgb",
                wb_sample_base_mode=self._raw_preview_decode_mode(resolved_settings),
            )
        process_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        preview_float = apply_post_decode_processing(preview_rgb, processing_settings)
        _raw_timing_log("RAW preview post-decode processing", process_start, detail=source_name)
        _raw_timing_log("RAW preview total", start, detail=source_name)
        return np.asarray(preview_float, dtype=np.float32), processing_settings

    def _present_raw_preview(
        self,
        *,
        pixmap: QPixmap,
        image_path: str,
        image: dict | None,
        title: str,
        meta: str,
        preview_scaled: bool = True,
        preserve_view: bool = False,
    ) -> None:
        if pixmap is None or pixmap.isNull():
            return
        actual_preserve_view = bool(preserve_view)
        if not actual_preserve_view:
            preserve_view_checker = getattr(self, "_should_preserve_live_image_view", None)
            if callable(preserve_view_checker):
                try:
                    actual_preserve_view = bool(preserve_view_checker(image_path))
                except Exception:
                    actual_preserve_view = False
        self.live_image_label.set_image_sources(
            pixmap,
            image_path,
            preview_scaled,
            preserve_view=actual_preserve_view,
        )
        self.reset_view_btn.setEnabled(True)
        if image is not None:
            mpp_value = self._image_microns_per_pixel(image)
            self.live_image_label.set_microns_per_pixel(mpp_value or 0.0)
            if mpp_value:
                scale_bar_value, scale_bar_unit = self._viewer_scale_bar_config(image)
                scale_bar_microns = float(scale_bar_value) * 1000.0 if scale_bar_unit == "mm" else float(scale_bar_value)
                self.live_image_label.set_scale_bar(True, scale_bar_microns, unit=scale_bar_unit)
            else:
                self.live_image_label.set_scale_bar(False, 0.0)
        else:
            self.live_image_label.set_microns_per_pixel(0.0)
            self.live_image_label.set_scale_bar(False, 0.0)
        self.viewer_title_label.setText(title)
        self.viewer_meta_label.setText(meta)

    def _schedule_raw_edit_preview_save(self, session: RawEditSession | None = None) -> None:
        self._raw_edit_preview_save_target = session or getattr(self, "_raw_edit_session", None)
        timer = getattr(self, "_raw_edit_preview_save_timer", None)
        if isinstance(timer, QTimer):
            timer.stop()
            timer.start()

    def _schedule_pending_raw_preview_save(self, capture: PendingRawCapture | None = None) -> None:
        self._pending_raw_preview_save_target = capture or self._current_pending_raw_capture()
        timer = getattr(self, "_pending_raw_preview_save_timer", None)
        if isinstance(timer, QTimer):
            timer.stop()
            timer.start()

    def _persist_raw_edit_preview(self) -> None:
        session = getattr(self, "_raw_edit_preview_save_target", None) or getattr(self, "_raw_edit_session", None)
        if session is None:
            return
        preview_rgb = getattr(session, "preview_rgb", None)
        preview_path = getattr(session, "preview_path", None)
        if preview_rgb is None or not preview_path:
            return
        preview_path = Path(str(preview_path))
        try:
            save_raw_preview_jpeg(
                preview_rgb,
                preview_path,
                session.source_raw_path,
                source_capture_datetime=session.source_capture_datetime,
            )
        except Exception:
            pass
        finally:
            if getattr(self, "_raw_edit_preview_save_target", None) is session:
                self._raw_edit_preview_save_target = None

    def _persist_pending_raw_preview(self) -> None:
        capture = getattr(self, "_pending_raw_preview_save_target", None) or self._current_pending_raw_capture()
        if capture is None:
            return
        preview_rgb = getattr(capture, "preview_rgb", None)
        preview_path = getattr(capture, "preview_path", None)
        if preview_rgb is None or not preview_path:
            return
        preview_path = Path(str(preview_path))
        try:
            save_raw_preview_jpeg(
                preview_rgb,
                preview_path,
                capture.source_path,
            )
            gallery = getattr(self, "session_gallery", None)
            if gallery is not None:
                invalidate_gallery = getattr(gallery, "invalidate_pixmap_cache", None)
                if callable(invalidate_gallery):
                    invalidate_gallery(preview_path)
        except Exception:
            pass
        finally:
            if self._current_pending_raw_capture() is not capture:
                capture.preview_rgb = None
            if getattr(self, "_pending_raw_preview_save_target", None) is capture:
                self._pending_raw_preview_save_target = None

    def _prune_pending_raw_preview_buffers(
        self,
        keep_capture: PendingRawCapture | None = None,
        *,
        clear_proxy_cache: bool = False,
    ) -> None:
        captures = getattr(self, "_pending_raw_captures", []) or []
        for capture in captures:
            if capture is keep_capture:
                continue
            if getattr(capture, "preview_rgb", None) is not None:
                capture.preview_rgb = None
        if clear_proxy_cache:
            cache = getattr(self, "_pending_raw_preview_proxy_cache", None)
            if isinstance(cache, dict):
                cache.clear()

    def _raw_curve_preview_source_path(self) -> Path | None:
        session = getattr(self, "_raw_edit_session", None)
        if session is not None:
            source_path = getattr(session, "source_raw_path", None)
            if source_path:
                path = Path(str(source_path))
                if path.exists():
                    return path

        capture = self._current_pending_raw_capture()
        if capture is not None:
            source_path = getattr(capture, "source_path", None)
            if source_path:
                path = Path(str(source_path))
                if path.exists():
                    return path

        live_image_label = getattr(self, "live_image_label", None)
        preview_path = getattr(live_image_label, "_full_image_path", None)
        if preview_path:
            path = Path(str(preview_path))
            if path.exists():
                return path
        return None

    @staticmethod
    def _raw_curve_preview_fallback_rgb() -> np.ndarray:
        ramp = np.linspace(0.0, 1.0, 256, dtype=np.float32)
        return np.repeat(ramp[:, None, None], 3, axis=2)

    def _raw_curve_preview_analysis_from_path(
        self,
        source_path: Path | None,
        settings: RawRenderSettings | None = None,
    ) -> tuple[np.ndarray, np.ndarray] | None:
        if source_path is None:
            return None
        try:
            stat = source_path.stat()
        except Exception:
            return None

        resolved_settings = RawRenderSettings.from_dict(settings or getattr(self, "_raw_render_settings", None))
        signature = (str(source_path), int(stat.st_mtime_ns), int(stat.st_size), self._raw_preview_decode_mode(resolved_settings))
        cached_signature = getattr(self, "_raw_curve_preview_analysis_signature", None)
        if signature == cached_signature:
            cached_rgb = getattr(self, "_raw_curve_preview_analysis_rgb", None)
            cached_histogram = getattr(self, "_raw_curve_preview_histogram", None)
            if isinstance(cached_rgb, np.ndarray) and isinstance(cached_histogram, np.ndarray):
                return cached_rgb, cached_histogram

        try:
            if is_raw_image_path(source_path):
                rgb = self._raw_preview_proxy_for_source(source_path, resolved_settings)
                rgb = _resize_preview_rgb(rgb, _TONE_CURVE_PREVIEW_ANALYSIS_MAX_DIM)
            else:
                with Image.open(source_path) as image:
                    image.load()
                    image = image.convert("RGB")
                    max_dim = max(int(image.width), int(image.height))
                    if max_dim > _TONE_CURVE_PREVIEW_ANALYSIS_MAX_DIM:
                        scale = float(_TONE_CURVE_PREVIEW_ANALYSIS_MAX_DIM) / float(max_dim)
                        resized_size = (
                            max(1, int(round(float(image.width) * scale))),
                            max(1, int(round(float(image.height) * scale))),
                        )
                        resample = getattr(getattr(Image, "Resampling", Image), "BILINEAR", Image.BILINEAR)
                        image = image.resize(resized_size, resample)
                    rgb = np.asarray(image, dtype=np.float32) / np.float32(255.0)
        except Exception:
            return None

        histogram = _compute_combined_rgb_histogram(rgb, bins=_TONE_CURVE_PREVIEW_HISTOGRAM_BINS)
        self._raw_curve_preview_analysis_signature = signature
        self._raw_curve_preview_analysis_rgb = rgb
        self._raw_curve_preview_histogram = histogram
        return rgb, histogram

    def _refresh_raw_curve_preview(self) -> None:
        widget = getattr(self, "raw_curve_preview_widget", None)
        if widget is None:
            return

        controls = getattr(self, "raw_controls", None)
        if controls is not None:
            try:
                settings = RawRenderSettings.from_dict(controls.settings())
            except Exception:
                settings = RawRenderSettings.from_dict(getattr(self, "_raw_render_settings", None))
        else:
            settings = RawRenderSettings.from_dict(getattr(self, "_raw_render_settings", None))

        source_path = self._raw_curve_preview_source_path()
        analysis = self._raw_curve_preview_analysis_from_path(source_path, settings)
        if analysis is None:
            rgb = self._raw_curve_preview_fallback_rgb()
            histogram = None
            self._raw_curve_preview_analysis_signature = None
            self._raw_curve_preview_analysis_rgb = rgb
            self._raw_curve_preview_histogram = None
        else:
            rgb, histogram = analysis

        try:
            curve = compute_post_decode_transfer_curve(rgb, settings)
        except Exception:
            curve = None
        widget.set_curve(curve, histogram)

    def _raw_processing_pick_target(self) -> str | None:
        if getattr(self, "_raw_edit_session", None) is not None:
            return "edit"
        if self._current_pending_raw_capture() is not None:
            return "pending"
        return None

    def _update_raw_processing_pick_button(self) -> None:
        controls = getattr(self, "raw_controls", None)
        if controls is None:
            return
        target = self._raw_processing_pick_target()
        armed = False
        if target == "edit":
            armed = bool(getattr(self, "_raw_edit_background_wb_armed", False))
        elif target == "pending":
            armed = bool(getattr(self, "_pending_raw_background_wb_armed", False))
        controls.set_pick_enabled(bool(target))
        controls.set_pick_checked(armed)

    def _toggle_active_raw_background_wb_pick(self, checked: bool) -> None:
        target = self._raw_processing_pick_target()
        controls = getattr(self, "raw_controls", None)
        if not checked:
            if target is not None:
                self._cancel_raw_background_wb_selection(target=target)
            self._refresh_raw_processing_context_ui()
            return
        if target is None:
            if controls is not None:
                controls.set_pick_checked(False)
            self._show_status(
                self.tr("Choose a pending RAW capture or a RAW edit session before sampling background WB."),
                tone="warning",
                timeout_ms=4000,
            )
            self._refresh_raw_processing_context_ui()
            return
        self._toggle_raw_background_wb_pick(True, target=target)

    def _update_raw_processing_section_label(self, expanded: bool) -> None:
        if not hasattr(self, "raw_processing_toggle_btn"):
            return
        arrow = "▾" if expanded else "▸"
        summary = self._raw_processing_summary_text()
        self.raw_processing_toggle_btn.setText(
            self.tr("RAW processing {arrow} {summary}").format(arrow=arrow, summary=summary)
        )

    def _on_raw_processing_toggle_changed(self, checked: bool) -> None:
        self._update_raw_processing_visibility()
        self._update_raw_processing_section_label(bool(checked))
        self._set_hint(self._current_raw_hint_text())

    def _on_raw_processing_controls_changed(self, *_args) -> None:
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        sender_fn = getattr(self, "sender", None)
        sender = sender_fn() if callable(sender_fn) else None
        editing_session = getattr(self, "_raw_edit_session", None)
        settings = self._raw_settings_from_controls(update_session_settings=editing_session is None)
        if editing_session is not None:
            current_edit_settings = RawRenderSettings.from_dict(editing_session.working_settings)
            settings = self._preserve_raw_wb_fields(current_edit_settings, settings)
            editing_session.working_settings = settings
            editing_session.dirty = settings != editing_session.original_settings
            self._refresh_raw_edit_preview()
            self._set_raw_tone_controls_enabled(bool(settings.tone_curve_enabled))
            self._update_pending_raw_controls()
            self._update_raw_edit_controls()
            self._update_raw_processing_section_label(bool(getattr(self, "raw_processing_toggle_btn", None) and self.raw_processing_toggle_btn.isChecked()))
            self._refresh_raw_processing_context_ui()
            _raw_timing_log(
                "RAW controls changed (edit session)",
                start,
                detail=f"image_id={editing_session.image_id} sender={type(sender).__name__ if sender is not None else 'unknown'}",
            )
            return

        selected_targets = self._selected_raw_processing_targets()
        if selected_targets:
            active_pending_index = self._selected_pending_raw_index_value()
            active_pending_capture = self._current_pending_raw_capture()
            active_committed_image_id = self._selected_committed_image_id()
            active_pending_applied = False
            active_committed_applied = False
            any_applied = False
            for kind, target_key, target in selected_targets:
                if kind == "pending" and isinstance(target, PendingRawCapture):
                    if active_pending_capture is target:
                        active_pending_applied = True
                    if self._apply_raw_settings_to_pending_capture(target, settings):
                        any_applied = True
                    else:
                        any_applied = True
                elif kind == "image":
                    try:
                        image_id = int(target_key)
                    except Exception:
                        continue
                    editable_session = self._raw_editable_image_session(target, settings=settings if isinstance(target, dict) else None)
                    if editable_session is None:
                        continue
                    editable_session.working_settings = self._preserve_raw_wb_fields(editable_session.original_settings, settings)
                    editable_session.dirty = editable_session.working_settings != editable_session.original_settings
                    if self._commit_raw_edit_session(editable_session):
                        any_applied = True
                        if active_committed_image_id is not None and int(active_committed_image_id) == image_id:
                            active_committed_applied = True
            if any_applied:
                self._set_raw_tone_controls_enabled(bool(settings.tone_curve_enabled))
                self._update_pending_raw_controls()
                self._update_raw_edit_controls()
                self._update_raw_processing_section_label(bool(getattr(self, "raw_processing_toggle_btn", None) and self.raw_processing_toggle_btn.isChecked()))
                if active_pending_applied and active_pending_capture is not None:
                    try:
                        pending_index = active_pending_index
                        current_capture = self._current_pending_raw_capture()
                        if current_capture is active_pending_capture:
                            pending_index = self._selected_pending_raw_index_value()
                        self._show_pending_raw_capture(pending_index)
                    except Exception:
                        pass
                elif active_committed_applied and active_committed_image_id is not None:
                    self._show_session_image(int(active_committed_image_id))
                self._refresh_raw_processing_context_ui()
                _raw_timing_log(
                    "RAW controls changed (selected targets)",
                    start,
                    detail=(
                        f"targets={len(selected_targets)} "
                        f"pending={active_pending_applied} committed={active_committed_applied} "
                        f"sender={type(sender).__name__ if sender is not None else 'unknown'}"
                    ),
                )
                return

        save_helper = getattr(self, "_save_raw_processing_settings_for_current_context", None)
        if callable(save_helper):
            save_helper(settings)
        else:
            LiveLabTab._save_raw_processing_settings_for_current_context(self, settings)
        self._set_raw_tone_controls_enabled(bool(settings.tone_curve_enabled))
        self._update_pending_raw_controls()
        self._update_raw_edit_controls()
        self._update_raw_processing_section_label(bool(getattr(self, "raw_processing_toggle_btn", None) and self.raw_processing_toggle_btn.isChecked()))
        self._refresh_raw_processing_context_ui()
        _raw_timing_log(
            "RAW controls changed (no targets)",
            start,
            detail=f"sender={type(sender).__name__ if sender is not None else 'unknown'}",
        )

    def _current_raw_render_settings(self) -> RawRenderSettings:
        _preset_context = self._raw_processing_preset_context()
        del _preset_context
        # TODO: resolve a saved preset first using _raw_processing_preset_context().
        return self._raw_settings_from_controls(update_session_settings=False)

    @staticmethod
    def _raw_settings_has_sampled_background_wb(settings: RawRenderSettings | None) -> bool:
        resolved = RawRenderSettings.from_dict(settings)
        wb_mode = str(resolved.white_balance_mode or "").strip().lower()
        if wb_mode == "auto":
            return False
        return bool(resolved.wb_multipliers is not None)

    def _pending_raw_action_hint_text(self, capture: PendingRawCapture | None = None) -> str:
        if self._pending_raw_background_wb_armed:
            return self.tr("Click neutral background to set WB")
        active_capture = capture or self._current_pending_raw_capture()
        if self._raw_settings_has_sampled_background_wb(active_capture.raw_settings if active_capture is not None else None):
            readout = self._raw_white_balance_readout_text(active_capture.raw_settings if active_capture is not None else None)
            return self.tr(
                "{readout} · ←/→ select · Delete/Backspace/Cmd/Ctrl+D remove current image · Enter save current"
            ).format(readout=readout)
        return self.tr("←/→ select · Delete/Backspace/Cmd/Ctrl+D remove current image · Enter save current")

    def _set_pending_raw_background_wb_armed(self, armed: bool) -> None:
        self._set_raw_background_wb_armed(armed, target="pending")

    def _cancel_pending_raw_background_wb_selection(self) -> None:
        self._cancel_raw_background_wb_selection(target="pending")

    def _pending_raw_settings_for_copy(self, settings: RawRenderSettings) -> RawRenderSettings:
        return self._raw_settings_for_copy(settings)

    def _raw_edit_preview_dir(self) -> Path:
        base_dir = Path(tempfile.gettempdir()) / "sporely_raw_edit_previews"
        session_id = str(self._session_id or "no_session").strip() or "no_session"
        observation_id = int(self._session_observation_id or 0)
        return base_dir / session_id / f"obs_{observation_id}"

    def _selected_committed_image_id(self) -> int | None:
        selected_id = int(self._selected_session_image_id or 0)
        if selected_id <= 0:
            return None
        if selected_id not in set(int(image_id) for image_id in self._session_image_ids):
            return None
        return selected_id

    def _selected_committed_image(self) -> dict | None:
        image_id = self._selected_committed_image_id()
        if image_id is None:
            return None
        return ImageDB.get_image(image_id)

    def _raw_editable_image_session(
        self,
        image: dict | None = None,
        *,
        settings: RawRenderSettings | None = None,
    ) -> RawEditSession | None:
        image_data = dict(image or self._selected_committed_image() or {})
        try:
            image_id = int(image_data.get("id") or 0)
        except Exception:
            image_id = 0
        if image_id <= 0:
            return None

        lab_metadata = copy.deepcopy(image_data.get("lab_metadata") or {})
        raw_processing = dict(lab_metadata.get("raw_processing") or {})
        source = dict(raw_processing.get("source") or {})
        if str(source.get("kind") or "").strip().lower() != "camera_raw":
            return None

        source_path_text = str(source.get("path") or "").strip()
        current_derivative_path_text = str(image_data.get("filepath") or "").strip()
        if not source_path_text or not current_derivative_path_text:
            return None

        source_path = Path(source_path_text)
        current_derivative_path = Path(current_derivative_path_text)
        try:
            if not source_path.exists() or not source_path.is_file():
                return None
            if not current_derivative_path.exists() or not current_derivative_path.is_file():
                return None
        except Exception:
            return None

        raw_settings = RawRenderSettings.from_dict(raw_processing.get("settings"))
        resolved_settings = RawRenderSettings.from_dict(settings or raw_settings)
        if source_path.exists() and bool(resolved_settings.auto_levels):
            resolved_settings = self._raw_auto_level_settings_for_source(source_path, resolved_settings)
        source_capture_datetime = str(source.get("captured_at") or image_data.get("captured_at") or "").strip() or None

        return RawEditSession(
            image_id=image_id,
            source_raw_path=source_path,
            current_derivative_path=current_derivative_path,
            original_settings=raw_settings,
            working_settings=resolved_settings,
            image_lab_metadata=lab_metadata,
            source_capture_datetime=source_capture_datetime,
            previous_session_settings=RawRenderSettings.from_dict(getattr(self, "_raw_render_settings", RawRenderSettings.default())),
            dirty=resolved_settings != raw_settings,
        )

    def _update_raw_edit_controls(self) -> None:
        session = getattr(self, "_raw_edit_session", None)
        selected_image = self._selected_committed_image()
        editable_session = self._raw_editable_image_session(selected_image) if selected_image is not None else None
        frame = getattr(self, "raw_edit_frame", None)
        if frame is not None:
            frame.setVisible(bool(session is not None or editable_session is not None))

        open_btn = getattr(self, "raw_edit_open_btn", None)
        use_copied_btn = getattr(self, "raw_edit_use_copied_btn", None)
        apply_btn = getattr(self, "raw_edit_apply_btn", None)
        copy_btn = getattr(self, "raw_edit_copy_btn", None)
        pick_wb_btn = getattr(self, "raw_edit_pick_wb_btn", None)
        cancel_btn = getattr(self, "raw_edit_cancel_btn", None)
        summary_label = getattr(self, "raw_edit_summary_label", None)

        has_copied_settings = getattr(self, "_raw_copied_settings", None) is not None
        can_edit_selected = editable_session is not None
        editing = session is not None

        if open_btn is not None:
            open_btn.setEnabled(bool(can_edit_selected and not editing))
        if use_copied_btn is not None:
            use_copied_btn.setEnabled(bool(can_edit_selected and has_copied_settings and not editing))
        if apply_btn is not None:
            apply_btn.setEnabled(bool(editing))
        if copy_btn is not None:
            copy_btn.setEnabled(bool(editing or can_edit_selected))
        if pick_wb_btn is not None:
            pick_wb_btn.setEnabled(bool(editing))
        if cancel_btn is not None:
            cancel_btn.setEnabled(bool(editing))

        summary_text = ""
        if editing:
            session_name = Path(str(session.source_raw_path)).name
            summary_text = self.tr("Editing RAW: {name} · {summary}").format(
                name=session_name,
                summary=self._raw_settings_info_text(session.working_settings),
            )
            if session.dirty:
                summary_text = f"{summary_text} {self.tr('· modified')}"
        elif editable_session is not None:
            summary_text = self.tr("RAW-backed image: {name} · {summary}").format(
                name=Path(str(editable_session.current_derivative_path)).name,
                summary=self._raw_settings_info_text(editable_session.original_settings),
            )
            if has_copied_settings:
                summary_text = f"{summary_text} {self.tr('· copied settings available')}"
        elif selected_image is not None and frame is not None:
            summary_text = self.tr("Select a RAW-backed image to re-render it in place.")

        if summary_label is not None:
            summary_label.setText(summary_text)
        self._refresh_raw_processing_context_ui()

    def _begin_raw_edit_for_selected_image(
        self,
        settings: RawRenderSettings | None = None,
        *,
        source_image: dict | None = None,
    ) -> bool:
        if self._raw_edit_session is not None:
            self._cancel_raw_edit_session(restore_selection=False)
        image = dict(source_image or self._selected_committed_image() or {})
        editable_session = self._raw_editable_image_session(image, settings=settings)
        if editable_session is None:
            self._update_raw_edit_controls()
            self._show_status(
                self.tr("Choose a committed RAW-backed image with an available RAW source first."),
                tone="warning",
                timeout_ms=5000,
            )
            return False

        self._raw_edit_session = editable_session
        self._selected_session_image_id = int(editable_session.image_id)
        self.session_gallery.select_image(int(editable_session.image_id))
        auto_settings = None
        if editable_session.source_raw_path.exists():
            auto_settings = self._raw_auto_level_settings_for_source(
                editable_session.source_raw_path,
                editable_session.working_settings,
            )
        self._sync_raw_processing_controls_from_settings(
            editable_session.working_settings,
            update_session_settings=False,
            auto_level_settings=auto_settings,
        )
        self._update_raw_edit_controls()
        if not self._refresh_raw_edit_preview():
            self._cancel_raw_edit_session(restore_selection=False)
            return False
        self._show_session_image(int(editable_session.image_id))
        self._show_status(
            self.tr("Editing RAW-backed image {name}.").format(name=Path(str(editable_session.source_raw_path)).name),
            tone="info",
            timeout_ms=2500,
        )
        return True

    def _begin_raw_edit_with_copied_settings(self) -> bool:
        copied_settings = getattr(self, "_raw_copied_settings", None)
        if copied_settings is None:
            self._show_status(
                self.tr("Copy RAW settings from another RAW-backed image first."),
                tone="warning",
                timeout_ms=4000,
            )
            return False
        return self._begin_raw_edit_for_selected_image(self._raw_settings_for_copy(copied_settings))

    def _cancel_raw_edit_session(self, restore_selection: bool = True) -> None:
        session = getattr(self, "_raw_edit_session", None)
        if session is None:
            return

        self._cancel_raw_background_wb_selection(target="edit")
        preview_path = Path(session.preview_path) if session.preview_path else None
        if preview_path is not None:
            try:
                preview_path.unlink(missing_ok=True)
            except Exception:
                pass

        previous_settings = RawRenderSettings.from_dict(
            session.previous_session_settings or getattr(self, "_raw_render_settings", RawRenderSettings.default())
        )
        self._raw_edit_session = None
        self._raw_edit_background_wb_armed = False
        self._sync_raw_processing_controls_from_settings(previous_settings, update_session_settings=True)
        self._update_raw_edit_controls()
        self._update_pending_raw_controls()
        if restore_selection and int(session.image_id) > 0:
            self._show_session_image(int(session.image_id))

    def _copy_raw_edit_settings(self) -> None:
        session = getattr(self, "_raw_edit_session", None)
        settings: RawRenderSettings | None = None
        source_name = None
        if session is not None:
            settings = session.working_settings
            source_name = Path(str(session.source_raw_path)).name
        else:
            selected_image = self._selected_committed_image()
            if selected_image is None:
                self._show_status(
                    self.tr("Select a RAW-backed image before copying RAW settings."),
                    tone="warning",
                    timeout_ms=4000,
                )
                return
            editable_session = self._raw_editable_image_session(selected_image)
            if editable_session is None:
                self._show_status(
                    self.tr("The selected image is not a RAW-backed image that can be copied."),
                    tone="warning",
                    timeout_ms=5000,
                )
                return
            settings = editable_session.original_settings
            source_name = Path(str(editable_session.source_raw_path)).name

        if settings is None:
            return

        self._raw_copied_settings = self._raw_settings_for_copy(settings)
        self._update_raw_edit_controls()
        self._show_status(
            self.tr("Copied RAW settings from {name}.").format(name=source_name or self.tr("the selected image")),
            tone="success",
            timeout_ms=2500,
        )

    def _schedule_raw_edit_preview_refresh(self) -> None:
        timer = getattr(self, "_raw_edit_preview_timer", None)
        if isinstance(timer, QTimer):
            timer.stop()
            timer.start()

    def _refresh_raw_edit_preview(self) -> bool:
        session = getattr(self, "_raw_edit_session", None)
        if session is None:
            return False
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None

        try:
            preview_rgb, _processing_settings = self._raw_preview_rgb_for_source(session.source_raw_path, session.working_settings)
            session.preview_rgb = preview_rgb
            preview_dir = self._raw_edit_preview_dir()
            preview_dir.mkdir(parents=True, exist_ok=True)
            preview_path = session.preview_path
            if preview_path is None:
                preview_path = preview_dir / f"{session.source_raw_path.stem}_{session.image_id}.jpg"
            session.preview_path = Path(preview_path)
            session.dirty = session.working_settings != session.original_settings
            self._update_raw_edit_controls()
            if self._selected_committed_image_id() == session.image_id:
                self._show_session_image(session.image_id)
                action = "displayed"
            else:
                self._schedule_raw_edit_preview_save(session)
                action = "scheduled-save"
            _raw_timing_log(
                "RAW edit preview refresh",
                start,
                detail=f"{session.source_raw_path.name} {preview_rgb.shape[1]}x{preview_rgb.shape[0]} {action}",
            )
            return True
        except RawRenderingUnavailableError as exc:
            self._show_status(
                self.tr("RAW preview unavailable for {name}: {error}").format(
                    name=session.source_raw_path.name,
                    error=str(exc),
                ),
                tone="warning",
                timeout_ms=6000,
            )
        except RuntimeError as exc:
            self._show_status(
                self.tr("Could not render RAW preview for {name}: {error}").format(
                    name=session.source_raw_path.name,
                    error=str(exc),
                ),
                tone="warning",
                timeout_ms=6000,
            )
        return False

    def _apply_raw_edit_session(self) -> bool:
        session = getattr(self, "_raw_edit_session", None)
        if session is None:
            return False

        temp_dir = self._raw_edit_preview_dir()
        temp_dir.mkdir(parents=True, exist_ok=True)
        temp_output = temp_dir / f"{session.source_raw_path.stem}_{session.image_id}_{uuid4().hex}.jpg"
        final_path = Path(session.current_derivative_path)
        final_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            rendered_path = render_raw_image(
                session.source_raw_path,
                settings=session.working_settings,
                output_path=temp_output,
                source_capture_datetime=session.source_capture_datetime,
            )
            reader = QImageReader(str(rendered_path))
            size = reader.size()
            width = int(size.width()) if size.isValid() else 0
            height = int(size.height()) if size.isValid() else 0
            if width <= 0 or height <= 0:
                width = height = 0
            os.replace(rendered_path, final_path)
            updated_lab_metadata = copy.deepcopy(session.image_lab_metadata)
            source_metadata = dict((updated_lab_metadata.get("raw_processing") or {}).get("source") or {})
            source_mime_type = source_metadata.get("mime_type")
            raw_processing = build_raw_processing_metadata(
                session.source_raw_path,
                final_path,
                session.working_settings,
                width=width,
                height=height,
                source_mime_type=str(source_mime_type or "") or None,
                source_capture_datetime=session.source_capture_datetime,
                rendered_at=datetime.now(),
            )
            updated_lab_metadata["raw_processing"] = raw_processing
            ImageDB.update_image(
                session.image_id,
                filepath=str(final_path),
                lab_metadata=updated_lab_metadata,
            )
            try:
                generate_all_sizes(str(final_path), int(session.image_id))
            except Exception as exc:
                self._show_status(
                    self.tr("Thumbnail refresh warning for {name}: {error}").format(
                        name=final_path.name,
                        error=str(exc),
                    ),
                    tone="warning",
                    timeout_ms=5000,
                )
            self._invalidate_thumbnail_caches_for_raw_image(int(session.image_id), final_path)
            preview_cleanup = Path(session.preview_path) if session.preview_path else None
            self._raw_edit_session = None
            self._raw_edit_background_wb_armed = False
            self._sync_raw_processing_controls_from_settings(
                RawRenderSettings.from_dict(session.previous_session_settings or self._raw_render_settings),
                update_session_settings=True,
            )
            self._update_raw_edit_controls()
            self._update_pending_raw_controls()
            self._refresh_session_gallery()
            self._show_session_image(session.image_id)
            self._refresh_main_window_after_import(session.image_id)
            self._update_observation_thumbnail()
            if preview_cleanup is not None:
                try:
                    preview_cleanup.unlink(missing_ok=True)
                except Exception:
                    pass
            self._show_status(
                self.tr("Updated RAW rendering for {name}.").format(name=final_path.name),
                tone="success",
                timeout_ms=4000,
            )
            return True
        except RawRenderingUnavailableError as exc:
            self._show_status(
                self.tr("RAW re-render unavailable for {name}: {error}").format(
                    name=session.source_raw_path.name,
                    error=str(exc),
                ),
                tone="warning",
                timeout_ms=6000,
            )
        except Exception as exc:
            self._show_status(
                self.tr("Could not re-render RAW image {name}: {error}").format(
                    name=session.source_raw_path.name,
                    error=str(exc),
                ),
                tone="warning",
                timeout_ms=6000,
            )
        finally:
            try:
                temp_output.unlink(missing_ok=True)
            except Exception:
                pass
        return False

    def _invalidate_thumbnail_caches_for_raw_image(self, image_id: int, final_path: Path) -> None:
        thumb_path = str(get_thumbnail_path(int(image_id), "small") or "").strip()
        if thumb_path:
            gallery = getattr(self, "session_gallery", None)
            invalidate_gallery = getattr(gallery, "invalidate_pixmap_cache", None)
            if callable(invalidate_gallery):
                try:
                    invalidate_gallery(thumb_path)
                except Exception:
                    pass

        main_window = getattr(self, "_main_window", None)
        if main_window is None:
            return

        invalidate_main_window = getattr(main_window, "invalidate_pixmap_cache", None)
        if callable(invalidate_main_window):
            try:
                invalidate_main_window(str(final_path))
            except Exception:
                pass
            return

        target = str(final_path)
        cache = getattr(main_window, "_pixmap_cache", None)
        if isinstance(cache, dict):
            cache.pop(target, None)
        order = getattr(main_window, "_pixmap_cache_order", None)
        if isinstance(order, list) and target in order:
            order[:] = [item for item in order if item != target]
        gallery_cache = getattr(main_window, "_gallery_pixmap_cache", None)
        if isinstance(gallery_cache, dict):
            gallery_cache.pop(target, None)

    def _raw_background_wb_selection_state(self, target: str) -> tuple[bool, QPushButton | None]:
        if target == "edit":
            return bool(self._raw_edit_background_wb_armed), getattr(self, "raw_edit_pick_wb_btn", None)
        return bool(self._pending_raw_background_wb_armed), getattr(self, "pending_raw_pick_wb_btn", None)

    def _set_raw_background_wb_armed(self, armed: bool, *, target: str = "pending") -> None:
        armed = bool(armed)
        if target == "edit":
            self._raw_edit_background_wb_armed = armed
            button = getattr(self, "raw_edit_pick_wb_btn", None)
        else:
            self._pending_raw_background_wb_armed = armed
            button = getattr(self, "pending_raw_pick_wb_btn", None)

        if button is not None:
            with QSignalBlocker(button):
                button.setChecked(armed)
            button.setText(self.tr("Cancel background WB") if armed else self.tr("Pick background WB"))

        label = getattr(self, "pending_raw_shortcuts_label", None) if target == "pending" else getattr(self, "raw_edit_summary_label", None)
        if label is not None:
            if target == "pending":
                label.setText(self._pending_raw_action_hint_text())
            elif self._raw_edit_session is not None:
                if armed:
                    label.setText(self.tr("Click neutral background to set WB"))
                else:
                    label.setText(
                        self.tr("Editing RAW: {name} · {summary}").format(
                            name=Path(str(self._raw_edit_session.source_raw_path)).name,
                            summary=self._raw_settings_info_text(self._raw_edit_session.working_settings),
                        )
                    )

        if hasattr(self, "live_image_label") and self.live_image_label is not None:
            try:
                self.live_image_label.clear_crop_box()
            except Exception:
                pass
            try:
                self.live_image_label.set_crop_mode(False)
            except Exception:
                pass
            try:
                self.live_image_label.setCursor(Qt.CrossCursor if armed else Qt.ArrowCursor)
            except Exception:
                pass
        self._refresh_raw_processing_context_ui()

    def _cancel_raw_background_wb_selection(self, *, target: str = "pending") -> None:
        self._set_raw_background_wb_armed(False, target=target)

    def _cancel_active_raw_background_wb_selection(self) -> None:
        active_target = self._active_raw_background_target()
        if active_target is not None:
            self._cancel_raw_background_wb_selection(target=active_target)

    def _toggle_raw_background_wb_pick(self, checked: bool, *, target: str = "pending") -> None:
        if checked:
            if target == "edit":
                self._cancel_raw_background_wb_selection(target="edit")
            else:
                self._cancel_pending_raw_background_wb_selection()
        self._set_raw_background_wb_armed(bool(checked), target=target)

    def _active_raw_background_target(self) -> str | None:
        if self._raw_edit_background_wb_armed:
            return "edit"
        if self._pending_raw_background_wb_armed:
            return "pending"
        return None

    def _raw_background_wb_sample_base_mode(self, settings: RawRenderSettings | None) -> str:
        resolved = RawRenderSettings.from_dict(settings)
        base_mode = str(resolved.wb_sample_base_mode or "").strip().lower() or None
        if base_mode in {"camera", "auto"}:
            return base_mode
        wb_mode = str(resolved.white_balance_mode or "camera").strip().lower() or "camera"
        if wb_mode == "auto":
            return "auto"
        return "camera"

    def _raw_background_wb_sampling_settings(self, settings: RawRenderSettings | None) -> RawRenderSettings:
        resolved = RawRenderSettings.from_dict(settings)
        base_mode = self._raw_background_wb_sample_base_mode(resolved)
        return replace(
            resolved,
            white_balance_mode=base_mode,
            wb_multipliers=None,
            wb_selection=None,
            wb_multiplier_space=None,
            wb_sample_point=None,
            wb_sample_size=None,
            wb_sample_base_mode=base_mode,
            wb_selection_space=None,
            auto_levels=False,
            tone_curve_enabled=False,
        )

    def _raw_background_wb_sampling_pixmap(self, target: str = "pending") -> QPixmap | None:
        if target == "edit":
            session = getattr(self, "_raw_edit_session", None)
            if session is None:
                return None
            cached_pixmap = getattr(session, "wb_sample_base_pixmap", None)
            if cached_pixmap is not None and not cached_pixmap.isNull():
                return cached_pixmap
            source_path = Path(session.source_raw_path)
            preview_dir = self._raw_edit_preview_dir()
            preview_dir.mkdir(parents=True, exist_ok=True)
            cached_path = getattr(session, "wb_sample_base_preview_path", None)
            if cached_path is None:
                cached_path = preview_dir / f"{source_path.stem}_{session.image_id}_wb_base.jpg"
            try:
                preview_path = render_raw_preview(
                    source_path,
                    settings=self._raw_background_wb_sampling_settings(session.working_settings),
                    output_path=cached_path,
                    output_dir=preview_dir,
                )
            except Exception:
                return None
            session.wb_sample_base_preview_path = Path(preview_path)
            pixmap, _preview_scaled = self._load_viewer_pixmap(str(preview_path))
            if pixmap is not None and not pixmap.isNull():
                session.wb_sample_base_pixmap = pixmap
                return pixmap
            return None

        capture = self._current_pending_raw_capture()
        if capture is None:
            return None
        cached_pixmap = getattr(capture, "wb_sample_base_pixmap", None)
        if cached_pixmap is not None and not cached_pixmap.isNull():
            return cached_pixmap
        source_path = Path(capture.source_path)
        preview_dir = self._pending_raw_preview_dir()
        preview_dir.mkdir(parents=True, exist_ok=True)
        cached_path = getattr(capture, "wb_sample_base_preview_path", None)
        if cached_path is None:
            cached_path = preview_dir / f"{source_path.stem}_wb_base.jpg"
        try:
            preview_path = render_raw_preview(
                source_path,
                settings=self._raw_background_wb_sampling_settings(capture.raw_settings),
                output_path=cached_path,
                output_dir=preview_dir,
            )
        except Exception:
            return None
        capture.wb_sample_base_preview_path = Path(preview_path)
        pixmap, _preview_scaled = self._load_viewer_pixmap(str(preview_path))
        if pixmap is not None and not pixmap.isNull():
            capture.wb_sample_base_pixmap = pixmap
            return pixmap
        return None

    def _raw_background_wb_sampling_view(
        self,
        target: str = "pending",
    ) -> tuple[QPixmap | None, tuple[float, float, float, float] | None, tuple[float, float] | None, tuple[float, float] | None]:
        base_pixmap = self._raw_background_wb_sampling_pixmap(target)
        if base_pixmap is None or base_pixmap.isNull():
            return None, None, None, None

        display_pixmap = getattr(self.live_image_label, "original_pixmap", None)
        if display_pixmap is None or display_pixmap.isNull():
            display_pixmap = base_pixmap

        display_width = float(max(1, int(display_pixmap.width())))
        display_height = float(max(1, int(display_pixmap.height())))
        base_width = float(max(1, int(base_pixmap.width())))
        base_height = float(max(1, int(base_pixmap.height())))
        source_scale_x = base_width / display_width
        source_scale_y = base_height / display_height

        crop_left = 0.0
        crop_top = 0.0
        crop_width = base_width
        crop_height = base_height
        view_getter = getattr(self.live_image_label, "get_current_view_crop_rect", None)
        if callable(view_getter):
            try:
                view_rect = view_getter()
            except Exception:
                view_rect = None
            if view_rect:
                try:
                    view_left, view_top, view_width, view_height = (
                        float(view_rect[0]),
                        float(view_rect[1]),
                        float(view_rect[2]),
                        float(view_rect[3]),
                    )
                except Exception:
                    view_left = view_top = view_width = view_height = 0.0
                if view_width > 0 and view_height > 0:
                    crop_left = max(0.0, min(base_width - 1.0, view_left * source_scale_x))
                    crop_top = max(0.0, min(base_height - 1.0, view_top * source_scale_y))
                    crop_width = max(1.0, min(base_width - crop_left, view_width * source_scale_x))
                    crop_height = max(1.0, min(base_height - crop_top, view_height * source_scale_y))

        crop_x = max(0, min(int(crop_left), int(base_width) - 1))
        crop_y = max(0, min(int(crop_top), int(base_height) - 1))
        crop_w = max(1, min(int(round(crop_width)), int(base_width) - crop_x))
        crop_h = max(1, min(int(round(crop_height)), int(base_height) - crop_y))
        working_pixmap = base_pixmap.copy(crop_x, crop_y, crop_w, crop_h)

        widget_size = getattr(self.live_image_label, "size", None)
        if callable(widget_size):
            try:
                widget_size = widget_size()
            except Exception:
                widget_size = None
        if widget_size is not None:
            try:
                target_w = int(widget_size.width())
                target_h = int(widget_size.height())
            except Exception:
                target_w = target_h = 0
            if target_w > 0 and target_h > 0 and (working_pixmap.width() > target_w or working_pixmap.height() > target_h):
                scale = min(float(target_w) / float(working_pixmap.width()), float(target_h) / float(working_pixmap.height()))
                if scale < 1.0:
                    working_pixmap = working_pixmap.scaled(
                        max(1, int(round(working_pixmap.width() * scale))),
                        max(1, int(round(working_pixmap.height() * scale))),
                        Qt.IgnoreAspectRatio,
                        Qt.SmoothTransformation,
                    )

        working_scale_x = float(working_pixmap.width()) / float(crop_w)
        working_scale_y = float(working_pixmap.height()) / float(crop_h)
        return (
            working_pixmap,
            (float(crop_x), float(crop_y), float(crop_w), float(crop_h)),
            (source_scale_x, source_scale_y),
            (working_scale_x, working_scale_y),
        )

    def _raw_background_wb_sample_rect_from_point(
        self,
        point: QPointF,
        sample_size: int | None = None,
        pixmap: QPixmap | None = None,
    ) -> tuple[float, float, float, float] | None:
        pixmap = pixmap or getattr(self.live_image_label, "original_pixmap", None)
        if pixmap is None or pixmap.isNull():
            return None
        width = float(int(pixmap.width()))
        height = float(int(pixmap.height()))
        if width <= 0 or height <= 0:
            return None

        size = int(sample_size or self.RAW_BACKGROUND_WB_SAMPLE_SIZE)
        size = max(1, size)
        half = float(size) / 2.0
        center_x = float(point.x())
        center_y = float(point.y())
        x1 = max(0.0, center_x - half)
        y1 = max(0.0, center_y - half)
        x2 = min(width, center_x + half)
        y2 = min(height, center_y + half)
        if x2 <= x1 or y2 <= y1:
            return None
        return (x1, y1, x2 - x1, y2 - y1)

    def _apply_raw_background_wb_result(
        self,
        base_settings: RawRenderSettings,
        multipliers: tuple[float, float, float],
        selection: tuple[float, float, float, float],
        *,
        target: str = "pending",
        sample_point: tuple[float, float] | None = None,
        sample_size: int | None = None,
    ) -> bool:
        resolved_base_settings = RawRenderSettings.from_dict(base_settings)
        base_mode = self._raw_background_wb_sample_base_mode(resolved_base_settings)
        updated_settings = replace(
            resolved_base_settings,
            white_balance_mode="custom",
            wb_multipliers=(float(multipliers[0]), float(multipliers[1]), float(multipliers[2])),
            wb_selection=(float(selection[0]), float(selection[1]), float(selection[2]), float(selection[3])),
            wb_multiplier_space="post_decode_rgb",
            wb_sample_point=(float(sample_point[0]), float(sample_point[1])) if sample_point is not None else None,
            wb_sample_size=int(sample_size) if sample_size is not None else None,
            wb_sample_base_mode=base_mode,
            wb_selection_space="preview_pixels",
        )

        source_path: Path | None = None
        if target == "edit":
            session = getattr(self, "_raw_edit_session", None)
            if session is None:
                return False
            source_path = Path(session.source_raw_path)
        else:
            capture = self._current_pending_raw_capture()
            if capture is None:
                self._show_status(
                    self.tr("Choose a pending RAW capture before sampling background WB."),
                    tone="warning",
                    timeout_ms=4000,
                )
                return False
            source_path = Path(capture.source_path)

        if source_path.exists() and bool(updated_settings.auto_levels):
            updated_settings = self._raw_auto_level_settings_for_source(source_path, updated_settings)

        armed, _button = self._raw_background_wb_selection_state(target)
        if not armed:
            return False

        if target == "edit":
            session.working_settings = updated_settings
            session.dirty = updated_settings != session.original_settings
            self._cancel_raw_background_wb_selection(target=target)
            auto_settings = None
            if source_path.exists():
                auto_settings = self._raw_auto_level_settings_for_source(source_path, updated_settings)
            self._sync_raw_processing_controls_from_settings(
                updated_settings,
                update_session_settings=False,
                auto_level_settings=auto_settings,
            )
            self._schedule_raw_edit_preview_refresh()
            self._update_raw_edit_controls()
            self._show_status(
                self._raw_white_balance_readout_text(updated_settings),
                tone="success",
                timeout_ms=3000,
            )
            return True

        capture.raw_settings = updated_settings
        self._cancel_raw_background_wb_selection(target=target)
        auto_settings = None
        if source_path.exists():
            auto_settings = self._raw_auto_level_settings_for_source(source_path, updated_settings)
        self._sync_raw_processing_controls_from_settings(
            updated_settings,
            update_session_settings=True,
            auto_level_settings=auto_settings,
        )
        self._refresh_selected_pending_raw_preview()
        self._show_status(
            self._raw_white_balance_readout_text(updated_settings),
            tone="success",
            timeout_ms=3000,
        )
        return True

    def _apply_raw_background_wb_selection(self, crop_box, *, target: str = "pending") -> bool:
        armed, _button = self._raw_background_wb_selection_state(target)
        if not armed:
            return False

        pixmap, crop_rect, source_scale, working_scale = self._raw_background_wb_sampling_view(target)
        if pixmap is None or crop_rect is None or source_scale is None or working_scale is None:
            self._show_status(
                self.tr("The stable preview is not available for background WB sampling."),
                tone="warning",
                timeout_ms=4000,
            )
            return False

        rgb = self._pixmap_rgb_array(pixmap)
        if rgb is None:
            self._show_status(
                self.tr("The stable preview is not available for background WB sampling."),
                tone="warning",
                timeout_ms=4000,
            )
            return False

        normalized = None
        if crop_box is not None:
            try:
                if isinstance(crop_box, (tuple, list)) and len(crop_box) == 2:
                    start, end = crop_box
                    normalized = self.live_image_label._normalized_crop_box_from_points(start, end)
                else:
                    normalized = ZoomableImageLabel._normalize_crop_box(crop_box)
            except Exception:
                normalized = None
        if not normalized:
            return False
        x1, y1, x2, y2 = normalized
        width = float(max(0.0, x2 - x1))
        height = float(max(0.0, y2 - y1))
        if width < 1.0 or height < 1.0:
            return False
        source_x1 = float(x1) * float(source_scale[0])
        source_y1 = float(y1) * float(source_scale[1])
        source_x2 = float(x2) * float(source_scale[0])
        source_y2 = float(y2) * float(source_scale[1])
        local_x1 = max(0.0, source_x1 - float(crop_rect[0]))
        local_y1 = max(0.0, source_y1 - float(crop_rect[1]))
        local_x2 = min(float(crop_rect[2]), source_x2 - float(crop_rect[0]))
        local_y2 = min(float(crop_rect[3]), source_y2 - float(crop_rect[1]))
        if local_x2 <= local_x1 or local_y2 <= local_y1:
            return False
        sample_rect = (
            float(local_x1) * float(working_scale[0]),
            float(local_y1) * float(working_scale[1]),
            max(1.0, float(local_x2 - local_x1) * float(working_scale[0])),
            max(1.0, float(local_y2 - local_y1) * float(working_scale[1])),
        )
        try:
            multipliers = estimate_white_balance_from_background(rgb, rect=sample_rect)
        except Exception as exc:
            self._show_status(
                self.tr("Could not sample background WB from the selected region: {error}").format(error=str(exc)),
                tone="warning",
                timeout_ms=5000,
            )
            return False

        base_settings = RawRenderSettings.default()
        if target == "edit":
            session = getattr(self, "_raw_edit_session", None)
            if session is None:
                return False
            base_settings = RawRenderSettings.from_dict(session.working_settings)
        else:
            capture = self._current_pending_raw_capture()
            if capture is None:
                self._show_status(
                    self.tr("Choose a pending RAW capture before sampling background WB."),
                    tone="warning",
                    timeout_ms=4000,
                )
                return False
            base_settings = RawRenderSettings.from_dict(capture.raw_settings)

        return self._apply_raw_background_wb_result(
            base_settings,
            (float(multipliers[0]), float(multipliers[1]), float(multipliers[2])),
            (float(crop_rect[0] + local_x1), float(crop_rect[1] + local_y1), float(local_x2 - local_x1), float(local_y2 - local_y1)),
            target=target,
            sample_size=None,
        )

    def _apply_raw_background_wb_selection_from_point(self, point: QPointF, *, target: str = "pending") -> bool:
        armed, _button = self._raw_background_wb_selection_state(target)
        if not armed:
            return False

        pixmap, crop_rect, source_scale, working_scale = self._raw_background_wb_sampling_view(target)
        if pixmap is None or crop_rect is None or source_scale is None or working_scale is None:
            self._show_status(
                self.tr("The stable preview is not available for background WB sampling."),
                tone="warning",
                timeout_ms=4000,
            )
            return False

        rgb = self._pixmap_rgb_array(pixmap)
        if rgb is None:
            self._show_status(
                self.tr("The stable preview is not available for background WB sampling."),
                tone="warning",
                timeout_ms=4000,
            )
            return False

        sample_rect = self._raw_background_wb_sample_rect_from_point(
            QPointF(
                max(0.0, (float(point.x()) * float(source_scale[0])) - float(crop_rect[0])) * float(working_scale[0]),
                max(0.0, (float(point.y()) * float(source_scale[1])) - float(crop_rect[1])) * float(working_scale[1]),
            ),
            self.RAW_BACKGROUND_WB_SAMPLE_SIZE,
            pixmap=pixmap,
        )
        if sample_rect is None:
            self._show_status(
                self.tr("Could not sample background WB from the clicked point."),
                tone="warning",
                timeout_ms=4000,
            )
            return False

        try:
            multipliers = estimate_white_balance_from_background(rgb, rect=sample_rect)
        except Exception as exc:
            self._show_status(
                self.tr("Could not sample background WB from the clicked point: {error}").format(error=str(exc)),
                tone="warning",
                timeout_ms=5000,
            )
            return False

        sample_point = (float(point.x()), float(point.y()))
        if target == "edit":
            session = getattr(self, "_raw_edit_session", None)
            if session is None:
                return False
            base_settings = RawRenderSettings.from_dict(session.working_settings)
        else:
            capture = self._current_pending_raw_capture()
            if capture is None:
                self._show_status(
                    self.tr("Choose a pending RAW capture before sampling background WB."),
                    tone="warning",
                    timeout_ms=4000,
                )
                return False
            base_settings = RawRenderSettings.from_dict(capture.raw_settings)

        return self._apply_raw_background_wb_result(
            base_settings,
            (float(multipliers[0]), float(multipliers[1]), float(multipliers[2])),
            (
                float(crop_rect[0] + (float(sample_rect[0]) / float(working_scale[0]))),
                float(crop_rect[1] + (float(sample_rect[1]) / float(working_scale[1]))),
                float(sample_rect[2]) / float(working_scale[0]),
                float(sample_rect[3]) / float(working_scale[1]),
            ),
            target=target,
            sample_point=sample_point,
            sample_size=self.RAW_BACKGROUND_WB_SAMPLE_SIZE,
        )

    def _on_live_image_clicked_for_background_wb(self, point: QPointF) -> None:
        target = self._active_raw_background_target()
        if target is None:
            return
        self._apply_raw_background_wb_selection_from_point(point, target=target)

    def _on_raw_background_wb_crop_changed(self, crop_box, *, target: str = "pending") -> None:
        if self._active_raw_background_target() != target:
            return
        self._apply_raw_background_wb_selection(crop_box, target=target)

    def _finalize_raw_background_wb_from_preview(self, *, target: str = "pending") -> None:
        if target == "edit":
            if not self._raw_edit_background_wb_armed:
                return
            crop_preview = getattr(self.live_image_label, "crop_preview", None)
            if crop_preview and self._apply_raw_background_wb_selection(crop_preview, target=target):
                return
            crop_box = getattr(self.live_image_label, "crop_box", None)
            if crop_box:
                self._apply_raw_background_wb_selection(crop_box, target=target)
            return

        if not self._pending_raw_background_wb_armed:
            return
        crop_preview = getattr(self.live_image_label, "crop_preview", None)
        if crop_preview and self._apply_raw_background_wb_selection(crop_preview, target=target):
            return
        crop_box = getattr(self.live_image_label, "crop_box", None)
        if crop_box:
            self._apply_raw_background_wb_selection(crop_box, target=target)

    def _pixmap_rgb_array(self, pixmap: QPixmap | None) -> np.ndarray | None:
        if pixmap is None or pixmap.isNull():
            return None
        image = pixmap.toImage().convertToFormat(QImage.Format.Format_RGBA8888)
        if image.isNull():
            return None
        width = int(image.width())
        height = int(image.height())
        if width <= 0 or height <= 0:
            return None
        bytes_per_line = int(image.bytesPerLine())
        buffer = image.constBits() if hasattr(image, "constBits") else image.bits()
        array = np.frombuffer(buffer, dtype=np.uint8)
        try:
            array = array.reshape((height, bytes_per_line))
        except Exception:
            return None
        rgb = array[:, : width * 4]
        try:
            rgb = rgb.reshape((height, width, 4))
        except Exception:
            return None
        return np.asarray(rgb[:, :, :3], dtype=np.float64).copy() / 255.0

    def _apply_pending_raw_background_wb_selection(self, crop_box) -> bool:
        return self._apply_raw_background_wb_selection(crop_box, target="pending")

    def _on_pending_raw_background_wb_crop_changed(self, crop_box) -> None:
        self._on_raw_background_wb_crop_changed(crop_box, target="pending")

    def _toggle_pending_raw_background_wb_pick(self, checked: bool) -> None:
        if checked:
            capture = self._current_pending_raw_capture()
            if self._normalize_raw_capture_mode(self._selected_raw_capture_mode()) != self.RAW_CAPTURE_MODE_REVIEW:
                self._set_raw_background_wb_armed(False, target="pending")
                self._show_status(
                    self.tr("Background WB sampling is only available in RAW review mode."),
                    tone="warning",
                    timeout_ms=4000,
                )
                return
            preview_path = str(getattr(capture, "preview_path", None) or "").strip() if capture is not None else ""
            if capture is None or not preview_path or not Path(preview_path).exists():
                self._set_raw_background_wb_armed(False, target="pending")
                self._show_status(
                    self.tr("Choose a pending RAW capture with a preview before sampling background WB."),
                    tone="warning",
                    timeout_ms=4000,
                )
                return
            self._cancel_raw_background_wb_selection(target="edit")
            self._set_raw_background_wb_armed(True, target="pending")
            self._show_status(
                self.tr("Click neutral background to set WB"),
                tone="info",
                timeout_ms=0,
            )
            return
        self._cancel_raw_background_wb_selection(target="pending")

    @staticmethod
    def _is_text_input_widget(widget: QWidget | None) -> bool:
        return isinstance(widget, (QLineEdit, QTextEdit, QPlainTextEdit, QAbstractSpinBox, QAbstractSlider, QComboBox, QAbstractButton))

    def _raw_review_shortcut_allowed(self) -> bool:
        if self._raw_edit_session is not None:
            return False
        if self._current_pending_raw_capture() is None and self._selected_committed_image_id() is None:
            return False
        if self._pending_raw_background_wb_armed:
            return False
        focus_widget = QApplication.focusWidget()
        if focus_widget is not None and self._is_text_input_widget(focus_widget):
            return False
        return True

    def _handle_raw_review_shortcut(self, action: str) -> None:
        if not self._raw_review_shortcut_allowed():
            return
        normalized = str(action or "").strip().lower()
        if normalized in {"previous", "left"}:
            self._show_previous_pending_raw_capture()
        elif normalized in {"next", "right"}:
            self._show_next_pending_raw_capture()
        elif normalized in {"discard", "delete", "backspace"}:
            self._delete_current_raw_review_item()
        elif normalized in {"save", "enter", "return"}:
            self._commit_selected_pending_raw_capture()

    def _install_raw_review_shortcuts(self) -> None:
        for shortcut in getattr(self, "_raw_review_shortcuts", []):
            try:
                shortcut.setParent(None)
            except Exception:
                pass
        self._raw_review_shortcuts = []
        shortcut_defs = [
            (QKeySequence(Qt.Key_Left), "previous"),
            (QKeySequence(Qt.Key_Right), "next"),
            (QKeySequence(Qt.Key_Delete), "discard"),
            (QKeySequence(Qt.Key_Backspace), "discard"),
            (QKeySequence("Ctrl+D"), "discard"),
            (QKeySequence("Meta+D"), "discard"),
            (QKeySequence(Qt.Key_Return), "save"),
            (QKeySequence(Qt.Key_Enter), "save"),
        ]
        for sequence, action in shortcut_defs:
            shortcut = QShortcut(sequence, self)
            shortcut.setContext(Qt.WidgetWithChildrenShortcut)
            shortcut.activated.connect(lambda _checked=False, act=action: self._handle_raw_review_shortcut(act))
            self._raw_review_shortcuts.append(shortcut)
        escape_shortcut = QShortcut(QKeySequence(Qt.Key_Escape), self)
        escape_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        escape_shortcut.activated.connect(lambda _checked=False: self._cancel_active_raw_background_wb_selection())
        self._raw_review_shortcuts.append(escape_shortcut)

    def eventFilter(self, obj, event) -> bool:
        if obj is getattr(self, "live_image_label", None):
            active_target = self._active_raw_background_target()
            if event.type() == QEvent.MouseButtonRelease and getattr(event, "button", lambda: None)() == Qt.LeftButton:
                if active_target and getattr(self.live_image_label, "crop_mode", False):
                    QTimer.singleShot(
                        0,
                        lambda target=active_target: self._finalize_raw_background_wb_from_preview(target=target),
                    )
            elif event.type() == QEvent.KeyPress and getattr(event, "key", lambda: None)() == Qt.Key_Escape:
                if active_target:
                    self._cancel_raw_background_wb_selection(target=active_target)
                    return True
        return super().eventFilter(obj, event)

    def _finalize_pending_raw_background_wb_from_preview(self) -> None:
        self._finalize_raw_background_wb_from_preview(target="pending")

    def _normalize_raw_capture_mode(self, value: str | None) -> str:
        mode = str(value or self.RAW_CAPTURE_MODE_AUTO_SAVE).strip().lower()
        return mode if mode in {self.RAW_CAPTURE_MODE_AUTO_SAVE, self.RAW_CAPTURE_MODE_REVIEW} else self.RAW_CAPTURE_MODE_AUTO_SAVE

    def _selected_raw_capture_mode(self) -> str:
        selector = getattr(self, "raw_capture_mode_selector", None)
        if selector is not None:
            return self._normalize_raw_capture_mode(selector.selected_value(self.RAW_CAPTURE_MODE_AUTO_SAVE))
        # The simplified Live Lab UI always reviews RAW captures.
        return self.RAW_CAPTURE_MODE_REVIEW

    def _raw_capture_mode_label(self, mode: str | None = None, *, short: bool = False) -> str:
        normalized = self._normalize_raw_capture_mode(mode or self._selected_raw_capture_mode())
        if normalized == self.RAW_CAPTURE_MODE_REVIEW:
            return self.tr("Review") if short else self.tr("Review RAW before saving")
        return self.tr("Auto-save") if short else self.tr("Auto-save RAW captures")

    def _restore_raw_capture_mode(self) -> None:
        saved = self._normalize_raw_capture_mode(
            SettingsDB.get_setting(self.SETTING_RAW_CAPTURE_MODE, self.RAW_CAPTURE_MODE_AUTO_SAVE)
        )
        selector = getattr(self, "raw_capture_mode_selector", None)
        if selector is not None:
            self._raw_capture_mode = saved
            selector.set_selected_value(saved)
        else:
            self._raw_capture_mode = self.RAW_CAPTURE_MODE_REVIEW
        self._update_pending_raw_controls()
        self._update_raw_processing_section_label(bool(getattr(self, "raw_processing_toggle_btn", None) and self.raw_processing_toggle_btn.isChecked()))

    def _save_raw_capture_mode(self, mode: str | None = None) -> None:
        normalized = self._normalize_raw_capture_mode(mode or self._selected_raw_capture_mode())
        if getattr(self, "raw_capture_mode_selector", None) is not None:
            self._raw_capture_mode = normalized
        else:
            self._raw_capture_mode = self.RAW_CAPTURE_MODE_REVIEW
        SettingsDB.set_setting(self.SETTING_RAW_CAPTURE_MODE, normalized)

    def _on_raw_capture_mode_changed(self, value) -> None:
        self._save_raw_capture_mode(str(value or ""))
        self._update_pending_raw_controls()
        self._update_raw_processing_section_label(bool(getattr(self, "raw_processing_toggle_btn", None) and self.raw_processing_toggle_btn.isChecked()))

    def _pending_raw_preview_dir(self) -> Path:
        session_token = str(self._session_id or "session").strip() or "session"
        preview_dir = Path(tempfile.gettempdir()) / "sporely" / "raw_previews" / session_token
        preview_dir.mkdir(parents=True, exist_ok=True)
        return preview_dir

    def _pending_raw_capture_count(self) -> int:
        return len(getattr(self, "_pending_raw_captures", []) or [])

    def _selected_pending_raw_index_value(self) -> int:
        try:
            return int(getattr(self, "_selected_pending_raw_index", -1))
        except Exception:
            return -1

    def _current_pending_raw_capture(self) -> PendingRawCapture | None:
        captures = getattr(self, "_pending_raw_captures", [])
        index = self._selected_pending_raw_index_value()
        if index < 0 or index >= len(captures):
            return None
        capture = captures[index]
        return capture if isinstance(capture, PendingRawCapture) else None

    def _selected_pending_raw_captures(self) -> list[PendingRawCapture]:
        captures = getattr(self, "_pending_raw_captures", []) or []
        selected: list[PendingRawCapture] = []
        seen_indices: set[int] = set()
        for key in self._session_gallery_selected_keys():
            pending_index = self._pending_raw_capture_index_for_key(key)
            if pending_index is None or pending_index in seen_indices:
                continue
            if 0 <= pending_index < len(captures):
                capture = captures[pending_index]
                if isinstance(capture, PendingRawCapture):
                    selected.append(capture)
                    seen_indices.add(pending_index)
        if selected:
            return selected
        current_pending = self._current_pending_raw_capture()
        return [current_pending] if current_pending is not None else []

    def _sync_selected_pending_raw_metadata_from_controls(self) -> bool:
        if getattr(self, "_raw_edit_session", None) is not None:
            return False
        selected_captures = self._selected_pending_raw_captures()
        if not selected_captures:
            return False

        current_lab_metadata = getattr(self, "_current_lab_metadata", None)
        if not callable(current_lab_metadata):
            return False
        try:
            metadata = dict(current_lab_metadata() or {})
        except Exception:
            return False

        changed = False
        for capture in selected_captures:
            merged = merge_image_lab_metadata(capture.lab_metadata, metadata)
            if merged != capture.lab_metadata:
                capture.lab_metadata = merged
                changed = True
        if changed:
            self._refresh_session_gallery()
            refresh_viewer_objective_tag = getattr(self, "_refresh_viewer_objective_tag_from_current_state", None)
            if callable(refresh_viewer_objective_tag):
                refresh_viewer_objective_tag()
        return changed

    def _pending_raw_gallery_key(self, capture: PendingRawCapture | None) -> str | None:
        if capture is None:
            return None
        try:
            source = str(capture.source_path).strip()
        except Exception:
            return None
        if not source:
            return None
        return f"pending:{source}"

    def _pending_raw_capture_index_for_key(self, key) -> int | None:
        selected_key = str(key or "").strip()
        if not selected_key:
            return None
        for index, capture in enumerate(getattr(self, "_pending_raw_captures", []) or []):
            if not isinstance(capture, PendingRawCapture):
                continue
            candidate_keys = {
                self._pending_raw_gallery_key(capture),
                str(capture.source_path),
                str(capture.preview_path or ""),
            }
            if selected_key in {str(candidate) for candidate in candidate_keys if str(candidate).strip()}:
                return index
        return None

    def _pending_raw_capture_exists(self, source_path: str | Path, *, group_key: str | None = None) -> bool:
        source_text = str(source_path or "").strip()
        if not source_text:
            return False
        candidate_group_key = str(group_key or "").strip() or companion_group_key(source_text)
        for capture in getattr(self, "_pending_raw_captures", []) or []:
            if not isinstance(capture, PendingRawCapture):
                continue
            if str(capture.source_path).strip() == source_text:
                return True
            if candidate_group_key and str(capture.group_key or "").strip() == candidate_group_key:
                return True
        return False

    def _pending_raw_gallery_items(self) -> tuple[list[dict[str, object]], str | int | None]:
        items: list[dict[str, object]] = []
        selected_key: str | int | None = None
        current_pending = self._current_pending_raw_capture()
        for index, capture in enumerate(getattr(self, "_pending_raw_captures", []) or []):
            if not isinstance(capture, PendingRawCapture):
                continue
            preview_path = None
            if capture.preview_path:
                try:
                    preview_candidate = Path(capture.preview_path)
                    if preview_candidate.exists():
                        preview_path = str(preview_candidate)
                except Exception:
                    preview_path = None
            is_raw_source = is_raw_image_path(capture.source_path)
            badges: list[str] = [self.tr("UNSAVED RAW") if is_raw_source else self.tr("UNSAVED")]
            if capture.status == "failed":
                badges.append(self.tr("Preview failed"))
            elif preview_path is None:
                badges.append(self.tr("Preview pending"))
            microscope_tag_text, microscope_tag_color = LiveLabTab._microscope_tag_for_metadata(self, capture.lab_metadata)
            item = {
                "id": self._pending_raw_gallery_key(capture),
                "filepath": str(capture.source_path),
                "preview_path": preview_path,
                "image_number": f"P{index + 1}",
                "badges": badges,
                "frame_border_color": "#e74c3c" if capture.status == "failed" else "#e67e22",
                "raw_halo_color": "#e74c3c",
                "microscope_tag_text": microscope_tag_text,
                "microscope_tag_color": microscope_tag_color,
                "gps_tag_text": microscope_tag_text,
                "gps_tag_color": microscope_tag_color,
            }
            items.append(item)
            if current_pending is capture:
                selected_key = item["id"]
        return items, selected_key

    def _session_gallery_items(self) -> tuple[list[dict[str, object]], str | int | None]:
        items: list[dict[str, object]] = []
        pending_items, selected_pending_key = self._pending_raw_gallery_items()
        items.extend(pending_items)
        objectives = load_objectives()
        for idx, image_id in enumerate(self._session_image_ids, start=1):
            image = ImageDB.get_image(image_id)
            if not image:
                continue
            objective_name = LiveLabTab._objective_key_from_metadata(self, image)
            objective_label = None
            if objective_name:
                objective_obj = objectives.get(str(objective_name))
                objective_label = (
                    objective_display_name(objective_obj, str(objective_name))
                    if objective_obj
                    else str(objective_name)
                )
            badges = ImageGalleryWidget.build_gallery_badges(
                image_type=image.get("image_type"),
                objective_name=objective_label,
                contrast=image.get("contrast"),
                scale_microns_per_pixel=image.get("scale_microns_per_pixel"),
                custom_scale=bool(str(objective_name or "").strip().lower() == "custom"),
                needs_scale=(
                    str(image.get("image_type") or "").strip().lower() == "microscope"
                    and not objective_name
                    and not image.get("scale_microns_per_pixel")
                ),
                resize_to_optimal=bool(
                    isinstance(image.get("resample_scale_factor"), (int, float))
                    and image.get("resample_scale_factor") is not None
                    and float(image.get("resample_scale_factor")) < 0.999
                ),
                lab_metadata=image.get("lab_metadata"),
                translate=self.tr,
            )
            microscope_tag_text, microscope_tag_color = LiveLabTab._microscope_tag_for_metadata(self, image)
            if microscope_tag_text and badges and str(image.get("image_type") or "").strip().lower() == "microscope":
                badges = badges[1:]
            items.append(
                {
                    "id": image_id,
                    "filepath": image.get("filepath"),
                    "image_number": idx,
                    "has_measurements": False,
                    "badges": badges,
                    "microscope_tag_text": microscope_tag_text,
                    "microscope_tag_color": microscope_tag_color,
                    "gps_tag_text": microscope_tag_text,
                    "gps_tag_color": microscope_tag_color,
                }
            )
        selected_key: str | int | None = selected_pending_key
        if selected_key is None and self._selected_session_image_id is not None:
            selected_key = self._selected_session_image_id
        return items, selected_key

    def _refresh_pending_raw_gallery(self) -> None:
        self._refresh_session_gallery()

    def _on_pending_raw_gallery_clicked(self, image_id, path) -> None:
        self._on_session_gallery_clicked(image_id, path)

    def _update_pending_raw_controls(self) -> None:
        captures = getattr(self, "_pending_raw_captures", [])
        count = len(captures)
        selected_index = self._selected_pending_raw_index_value()
        if count == 0:
            selected_index = -1
            self._cancel_pending_raw_background_wb_selection()
        elif selected_index >= count or selected_index < -1:
            selected_index = count - 1
        self._selected_pending_raw_index = selected_index
        selected_mode = self._normalize_raw_capture_mode(self._selected_raw_capture_mode())

        frame = getattr(self, "pending_raw_frame", None)
        if frame is not None:
            frame.setVisible(bool(count))

        raw_toggle = getattr(self, "raw_processing_toggle_btn", None)
        should_expand = bool(count and selected_mode == self.RAW_CAPTURE_MODE_REVIEW)
        if raw_toggle is not None and should_expand and not raw_toggle.isChecked():
            with QSignalBlocker(raw_toggle):
                raw_toggle.setChecked(True)
            if hasattr(self, "raw_processing_body"):
                self.raw_processing_body.setVisible(True)
        elif raw_toggle is not None and hasattr(self, "raw_processing_body"):
            self.raw_processing_body.setVisible(bool(raw_toggle.isChecked()))

        count_label = getattr(self, "pending_raw_count_label", None)
        if count_label is not None:
            count_label.setText(self.tr("Pending RAW captures: {count}").format(count=count))

        self._refresh_pending_raw_gallery()

        current_capture = self._current_pending_raw_capture()
        has_selection = current_capture is not None
        preview_available = bool(
            has_selection
            and getattr(current_capture, "preview_path", None)
            and Path(str(current_capture.preview_path)).exists()
        )

        save_btn = getattr(self, "pending_raw_save_btn", None)
        if save_btn is not None:
            save_btn.setEnabled(bool(has_selection and not self._pending_raw_background_wb_armed))

        save_all_btn = getattr(self, "pending_raw_save_all_btn", None)
        if save_all_btn is not None:
            save_all_btn.setEnabled(bool(count and not self._pending_raw_background_wb_armed))

        apply_btn = getattr(self, "pending_raw_apply_all_btn", None)
        if apply_btn is not None:
            apply_btn.setEnabled(bool(count and not self._pending_raw_background_wb_armed))

        wb_btn = getattr(self, "pending_raw_pick_wb_btn", None)
        if wb_btn is not None:
            wb_btn.setEnabled(bool(preview_available and selected_mode == self.RAW_CAPTURE_MODE_REVIEW))
            if not self._pending_raw_background_wb_armed:
                wb_btn.setText(self.tr("Pick background WB"))

        hint_label = getattr(self, "pending_raw_shortcuts_label", None)
        if hint_label is not None:
            hint_label.setText(self._pending_raw_action_hint_text(current_capture))

        self._update_raw_processing_section_label(bool(getattr(self, "raw_processing_toggle_btn", None) and self.raw_processing_toggle_btn.isChecked()))
        self._refresh_raw_processing_context_ui()

    def _show_pending_raw_capture(self, index: int | None = None) -> None:
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        captures = getattr(self, "_pending_raw_captures", [])
        if not captures:
            self._cancel_pending_raw_background_wb_selection()
            self._selected_session_image_id = None
            self._clear_session_viewer(
                title=self.tr("Waiting for first import"),
                meta=self.tr("New microscope captures from the watched folder will appear here automatically."),
            )
            self._update_pending_raw_controls()
            return
        if index is None:
            index = self._selected_pending_raw_index_value()
        try:
            index = int(index)
        except Exception:
            index = -1
        if index < 0:
            index = len(captures) - 1
        if index >= len(captures):
            index = len(captures) - 1
        self._selected_pending_raw_index = index
        capture = self._current_pending_raw_capture()
        if capture is None:
            self._update_pending_raw_controls()
            return
        self._cancel_pending_raw_background_wb_selection()
        self._selected_session_image_id = None
        gallery = getattr(self, "session_gallery", None)
        if gallery is not None:
            is_multi_select = False
            selected_keys: list[object] = []
            is_multi_select_fn = getattr(gallery, "is_multi_select", None)
            if callable(is_multi_select_fn):
                try:
                    is_multi_select = bool(is_multi_select_fn())
                except Exception:
                    is_multi_select = False
            selected_keys_fn = getattr(gallery, "selected_keys", None)
            if callable(selected_keys_fn):
                try:
                    selected_keys = [key for key in list(selected_keys_fn()) if key is not None]
                except Exception:
                    selected_keys = []
            select_image = getattr(gallery, "select_image", None)
            pending_key = self._pending_raw_gallery_key(capture)
            if callable(select_image) and pending_key is not None and (not is_multi_select or not selected_keys):
                try:
                    select_image(pending_key)
                except Exception:
                    pass
        apply_microscope_state = getattr(self, "_apply_microscope_state_to_controls", None)
        if callable(apply_microscope_state):
            apply_microscope_state(capture.lab_metadata)
        auto_settings = None
        if capture.source_path.exists():
            auto_settings = self._raw_auto_level_settings_for_source(capture.source_path, capture.raw_settings)
        self._sync_raw_processing_controls_from_settings(
            capture.raw_settings,
            auto_level_settings=auto_settings,
        )
        preview_rgb = getattr(capture, "preview_rgb", None)
        preview_path = str(capture.preview_path or capture.source_path)
        title = self.tr("Pending RAW {current} of {total}: {name}").format(
            current=self._selected_pending_raw_index_value() + 1,
            total=len(captures),
            name=capture.source_path.name,
        )
        meta = self._raw_settings_info_text(capture.raw_settings)
        if isinstance(preview_rgb, np.ndarray) and preview_rgb.size:
            preview_dir = self._pending_raw_preview_dir()
            preview_dir.mkdir(parents=True, exist_ok=True)
            preview_path_obj = Path(capture.preview_path) if capture.preview_path else preview_dir / f"{capture.source_path.stem}_{uuid4().hex}.jpg"
            capture.preview_path = preview_path_obj
            preview_path = str(preview_path_obj)
            self._present_raw_preview(
                pixmap=self._rgb_to_pixmap(preview_rgb),
                image_path=preview_path,
                image=None,
                title=title,
                meta=meta,
                preview_scaled=True,
                preserve_view=True,
            )
            self._schedule_pending_raw_preview_save(capture)
            self._prune_pending_raw_preview_buffers(capture)
            _raw_timing_log(
                "show pending RAW capture",
                start,
                detail=f"{capture.source_path.name} source=memory preview={preview_rgb.shape[1]}x{preview_rgb.shape[0]}",
            )
        else:
            self._prune_pending_raw_preview_buffers(clear_proxy_cache=True)
            if not preview_path or not Path(preview_path).exists():
                _raw_timing_log(
                    "show pending RAW capture",
                    start,
                    detail=f"{capture.source_path.name} source=refresh-needed",
                )
                self._refresh_selected_pending_raw_preview()
                return
            pixmap, _preview_scaled = self._load_viewer_pixmap(preview_path)
            if pixmap is None or pixmap.isNull():
                self._clear_session_viewer(
                    title=self.tr("Pending RAW preview unavailable"),
                    meta=self.tr("Could not load the temporary preview for {name}.").format(name=capture.source_path.name),
                )
                self._show_status(
                    self.tr("Could not load the temporary preview for {name}.").format(name=capture.source_path.name),
                    tone="warning",
                    timeout_ms=5000,
                )
                self._update_pending_raw_controls()
                return
            self._present_raw_preview(
                pixmap=pixmap,
                image_path=preview_path,
                image=None,
                title=title,
                meta=meta,
                preview_scaled=True,
                preserve_view=True,
            )
            self._prune_pending_raw_preview_buffers(clear_proxy_cache=True)
            _raw_timing_log(
                "show pending RAW capture",
                start,
                detail=f"{capture.source_path.name} source=disk",
            )
        self._update_pending_raw_controls()

    def _refresh_selected_pending_raw_preview(self) -> None:
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        capture = self._current_pending_raw_capture()
        if capture is None:
            return
        if capture.status not in {"pending", "failed"}:
            return
        try:
            preview_rgb, _processing_settings = self._raw_preview_rgb_for_source(capture.source_path, capture.raw_settings)
            capture.preview_rgb = preview_rgb
            preview_dir = self._pending_raw_preview_dir()
            preview_dir.mkdir(parents=True, exist_ok=True)
            preview_path = capture.preview_path
            if preview_path is None:
                preview_path = preview_dir / f"{capture.source_path.stem}_{uuid4().hex}.jpg"
            capture.preview_path = Path(preview_path)
            capture.status = "pending"
            _raw_timing_log(
                "refresh selected pending RAW preview",
                start,
                detail=f"{capture.source_path.name} preview={preview_rgb.shape[1]}x{preview_rgb.shape[0]}",
            )
            self._show_pending_raw_capture(self._selected_pending_raw_index)
        except RawRenderingUnavailableError as exc:
            companion_jpeg_path = getattr(capture, "companion_jpeg_path", None)
            if companion_jpeg_path and Path(str(companion_jpeg_path)).exists():
                try:
                    if self._ingest_detected_image(str(companion_jpeg_path)):
                        self._remove_pending_raw_capture(capture, status="discarded", refresh_ui=False)
                        self._update_pending_raw_controls()
                        return
                except Exception:
                    pass
            capture.status = "failed"
            _raw_timing_log(
                "refresh selected pending RAW preview failed",
                start,
                detail=f"{capture.source_path.name}: {exc}",
            )
            self._show_status(
                self.tr("RAW preview unavailable for {name}: {error}").format(name=capture.source_path.name, error=str(exc)),
                tone="warning",
                timeout_ms=6000,
            )
        except RuntimeError as exc:
            companion_jpeg_path = getattr(capture, "companion_jpeg_path", None)
            if companion_jpeg_path and Path(str(companion_jpeg_path)).exists():
                try:
                    if self._ingest_detected_image(str(companion_jpeg_path)):
                        self._remove_pending_raw_capture(capture, status="discarded", refresh_ui=False)
                        self._update_pending_raw_controls()
                        return
                except Exception:
                    pass
            capture.status = "failed"
            self._show_status(
                self.tr("Could not render preview for {name}: {error}").format(name=capture.source_path.name, error=str(exc)),
                tone="warning",
                timeout_ms=6000,
            )

    def _schedule_pending_raw_preview_refresh(self) -> None:
        timer = getattr(self, "_pending_raw_preview_timer", None)
        if isinstance(timer, QTimer):
            timer.stop()
            timer.start()

    def _create_pending_raw_capture(
        self,
        source_path: str | Path,
        *,
        group_key: str | None = None,
        companion_jpeg_path: str | Path | None = None,
        raw_settings: RawRenderSettings | None = None,
        lab_metadata: dict[str, object] | None = None,
    ) -> PendingRawCapture:
        source = Path(source_path)
        resolved_lab_metadata = merge_image_lab_metadata(self._current_lab_metadata(), lab_metadata)
        resolved_lab_metadata["image_type"] = "microscope"
        resolved_settings = RawRenderSettings.from_dict(raw_settings or self._current_raw_render_settings())
        if source.exists() and bool(resolved_settings.auto_levels):
            resolved_settings = self._raw_auto_level_settings_for_source(source, resolved_settings)
        pending = PendingRawCapture(
            source_path=source,
            companion_jpeg_path=Path(companion_jpeg_path) if companion_jpeg_path else None,
            lab_metadata=resolved_lab_metadata,
            raw_settings=resolved_settings,
            group_key=group_key,
            observation_id=int(self._session_observation_id or 0) or None,
            created_at=datetime.now(),
        )
        preview_dir = self._pending_raw_preview_dir()
        preview_dir.mkdir(parents=True, exist_ok=True)
        pending.preview_path = preview_dir / f"{source.stem}_{uuid4().hex}.jpg"
        return pending

    def _add_pending_raw_capture(self, pending: PendingRawCapture) -> None:
        captures = getattr(self, "_pending_raw_captures", [])
        captures.append(pending)
        self._pending_raw_captures = captures
        self._selected_pending_raw_index = len(captures) - 1
        self._show_pending_raw_capture(self._selected_pending_raw_index)

    def _show_previous_pending_raw_capture(self) -> None:
        count = self._pending_raw_capture_count()
        if count <= 0:
            return
        index = self._selected_pending_raw_index_value() - 1
        if index < 0:
            index = count - 1
        self._show_pending_raw_capture(index)

    def _show_next_pending_raw_capture(self) -> None:
        count = self._pending_raw_capture_count()
        if count <= 0:
            return
        index = self._selected_pending_raw_index_value() + 1
        if index >= count:
            index = 0
        self._show_pending_raw_capture(index)

    def _commit_pending_raw_capture(self, pending: PendingRawCapture) -> bool:
        if pending is None:
            return False
        try:
            observation_id = int(getattr(pending, "observation_id", 0) or self._session_observation_id or 0)
            committed = self._ingest_detected_image(
                str(pending.source_path),
                raw_settings=pending.raw_settings,
                lab_metadata=pending.lab_metadata,
                observation_id=observation_id,
            )
        except Exception:
            committed = False
        if committed:
            pending.status = "committed"
        return committed

    def _remove_pending_raw_capture(self, pending: PendingRawCapture, *, status: str, refresh_ui: bool = True) -> bool:
        captures = getattr(self, "_pending_raw_captures", [])
        if pending not in captures:
            return False
        index = captures.index(pending)
        preview_path = Path(pending.preview_path) if pending.preview_path else None
        pending.status = status
        if preview_path is not None:
            try:
                preview_path.unlink(missing_ok=True)
            except Exception:
                pass
        captures.pop(index)
        self._pending_raw_captures = captures
        if not refresh_ui:
            if captures:
                self._selected_pending_raw_index = min(index, len(captures) - 1)
            else:
                self._selected_pending_raw_index = -1
            return True
        if captures:
            self._selected_pending_raw_index = min(index, len(captures) - 1)
            self._show_pending_raw_capture(self._selected_pending_raw_index)
        else:
            self._selected_pending_raw_index = -1
            self._update_pending_raw_controls()
            if self._session_image_ids:
                self._show_session_image(self._session_image_ids[-1])
            else:
                self._clear_session_viewer(
                    title=self.tr("Waiting for first import"),
                    meta=self.tr("New microscope captures from the watched folder will appear here automatically."),
                )
        return True

    def _commit_selected_pending_raw_capture(self) -> bool:
        pending = self._current_pending_raw_capture()
        if pending is None:
            return False
        self._cancel_pending_raw_background_wb_selection()
        if self._commit_pending_raw_capture(pending):
            self._remove_pending_raw_capture(pending, status="committed", refresh_ui=True)
            self._show_status(
                self.tr("Saved pending RAW capture {name}.").format(name=pending.source_path.name),
                tone="success",
                timeout_ms=3500,
            )
            return True
        self._show_status(
            self.tr("Could not save pending RAW capture {name}.").format(name=pending.source_path.name),
            tone="warning",
            timeout_ms=5000,
        )
        return False

    def _commit_all_pending_raw_captures(self) -> bool:
        captures = list(getattr(self, "_pending_raw_captures", []) or [])
        if not captures:
            return False
        saved = 0
        failed = 0
        for pending in captures:
            if pending not in getattr(self, "_pending_raw_captures", []):
                continue
            if self._commit_pending_raw_capture(pending):
                saved += 1
                self._remove_pending_raw_capture(pending, status="committed", refresh_ui=False)
            else:
                failed += 1
        self._update_pending_raw_controls()
        if self._current_pending_raw_capture() is not None:
            self._show_pending_raw_capture(self._selected_pending_raw_index)
        elif self._session_image_ids:
            self._show_session_image(self._session_image_ids[-1])
        else:
            self._clear_session_viewer(
                title=self.tr("Waiting for first import"),
                meta=self.tr("New microscope captures from the watched folder will appear here automatically."),
            )
        if saved == 0:
            self._show_status(
                self.tr("Could not save any pending RAW captures."),
                tone="warning",
                timeout_ms=5000,
            )
            return False
        if failed:
            self._show_status(
                self.tr("Saved {saved} RAW capture(s), but {failed} failed.").format(saved=saved, failed=failed),
                tone="warning",
                timeout_ms=5000,
            )
        else:
            self._show_status(
                self.tr("Saved {count} RAW capture(s).").format(count=saved),
                tone="success",
                timeout_ms=3500,
            )
        return True

    def _discard_selected_pending_raw_capture(self) -> bool:
        pending = self._current_pending_raw_capture()
        if pending is None:
            return False
        self._cancel_pending_raw_background_wb_selection()
        if self._remove_pending_raw_capture(pending, status="discarded", refresh_ui=True):
            self._show_status(
                self.tr("Removed pending RAW capture {name}.").format(name=pending.source_path.name),
                tone="info",
                timeout_ms=3500,
            )
            return True
        return False

    def _delete_current_raw_review_item(self) -> bool:
        if self._raw_edit_session is not None:
            return False

        pending = self._current_pending_raw_capture()
        if pending is not None:
            return self._discard_selected_pending_raw_capture()

        image_id = self._selected_committed_image_id()
        if image_id is None:
            return False

        image = ImageDB.get_image(image_id)
        image_name = Path(str((image or {}).get("filepath") or "")).name or self.tr("selected image")
        session_image_ids = list(getattr(self, "_session_image_ids", []) or [])
        try:
            image_index = session_image_ids.index(image_id)
        except ValueError:
            image_index = -1

        try:
            ImageDB.delete_image(image_id)
        except Exception as exc:
            self._show_status(
                self.tr("Could not delete local processed image {name}: {error}").format(
                    name=image_name,
                    error=str(exc),
                ),
                tone="warning",
                timeout_ms=6000,
            )
            return False

        self._session_image_ids = [existing_id for existing_id in session_image_ids if int(existing_id) != int(image_id)]
        if self._session_image_ids:
            next_index = image_index if image_index >= 0 else 0
            if next_index >= len(self._session_image_ids):
                next_index = len(self._session_image_ids) - 1
            next_image_id = int(self._session_image_ids[next_index])
            self._selected_session_image_id = next_image_id
            self._refresh_session_gallery()
            self._show_session_image(next_image_id)
        else:
            self._selected_session_image_id = None
            self._refresh_session_gallery()
            self._clear_session_viewer(
                title=self.tr("Waiting for first import"),
                meta=self.tr("New microscope captures from the watched folder will appear here automatically."),
            )
        self._update_pending_raw_controls()

        observation_id = int(self._session_observation_id or 0)
        if observation_id > 0 and int(getattr(self._main_window, "active_observation_id", 0) or 0) == observation_id:
            try:
                if hasattr(self._main_window, "observations_tab"):
                    self._main_window.observations_tab.refresh_observations(show_status=False)
            except Exception:
                pass
            try:
                self._main_window.refresh_observation_images(select_image_id=self._selected_session_image_id)
                self._main_window.update_measurements_table()
                if getattr(self._main_window, "is_analysis_visible", None) and self._main_window.is_analysis_visible():
                    self._main_window.schedule_gallery_refresh()
            except Exception:
                pass

        self._show_status(
            self.tr("Deleted local processed image {name}.").format(name=image_name),
            tone="success",
            timeout_ms=3500,
        )
        return True

    def _apply_current_raw_settings_to_all_pending(self) -> None:
        captures = list(getattr(self, "_pending_raw_captures", []) or [])
        if not captures:
            return
        current_capture = self._current_pending_raw_capture()
        settings = RawRenderSettings.from_dict(current_capture.raw_settings if current_capture is not None else self._current_raw_render_settings())
        for capture in captures:
            copied_settings = self._pending_raw_settings_for_copy(settings)
            capture.raw_settings = copied_settings
            try:
                preview_rgb, _processing_settings = self._raw_preview_rgb_for_source(capture.source_path, copied_settings)
                capture.preview_rgb = preview_rgb
                preview_dir = self._pending_raw_preview_dir()
                preview_dir.mkdir(parents=True, exist_ok=True)
                preview_path = capture.preview_path
                if preview_path is None:
                    preview_path = preview_dir / f"{capture.source_path.stem}_{uuid4().hex}.jpg"
                capture.preview_path = Path(preview_path)
                try:
                    save_raw_preview_jpeg(
                        preview_rgb,
                        capture.preview_path,
                        capture.source_path,
                    )
                except Exception:
                    pass
                capture.status = "pending"
            except Exception as exc:
                capture.status = "failed"
                self._show_status(
                    self.tr("Could not update preview for {name}: {error}").format(
                        name=capture.source_path.name,
                        error=str(exc),
                    ),
                    tone="warning",
                    timeout_ms=6000,
                )
        self._update_pending_raw_controls()
        self._prune_pending_raw_preview_buffers(self._current_pending_raw_capture(), clear_proxy_cache=True)
        self._refresh_session_gallery()
        if self._current_pending_raw_capture() is not None:
            self._show_pending_raw_capture(self._selected_pending_raw_index)

    def _reset_companion_dedupe_state(self) -> None:
        pending = getattr(self, "_pending_companion_groups", {})
        for state in list(pending.values()):
            timer = state.get("timer") if isinstance(state, dict) else None
            if isinstance(timer, QTimer):
                try:
                    timer.stop()
                except Exception:
                    pass
                try:
                    timer.deleteLater()
                except Exception:
                    pass
        pending.clear()
        getattr(self, "_consumed_companion_groups", set()).clear()
        self._cancel_pending_raw_background_wb_selection()
        timer = getattr(self, "_pending_raw_preview_timer", None)
        if isinstance(timer, QTimer):
            timer.stop()

    def _same_stem_companion_paths(self, source_path: str) -> list[str]:
        source_text = str(source_path or "").strip()
        if not source_text:
            return []
        try:
            source = Path(source_text)
        except Exception:
            return [source_text]
        try:
            if not source.exists() or not source.is_file():
                return [str(source)]
        except Exception:
            return [source_text]

        supported_suffixes = {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".heic", ".heif"} | set(SUPPORTED_RAW_SUFFIXES)
        candidates: list[str] = []
        try:
            for child in source.parent.iterdir():
                if not child.is_file():
                    continue
                if child.stem.casefold() != source.stem.casefold():
                    continue
                if child.suffix.lower() not in supported_suffixes:
                    continue
                try:
                    candidates.append(str(child.resolve()))
                except Exception:
                    candidates.append(str(child))
        except Exception:
            return [str(source)]

        if not candidates:
            return [str(source)]
        seen: set[str] = set()
        unique: list[str] = []
        for candidate in sorted(candidates, key=lambda path: Path(path).name.casefold()):
            if candidate in seen:
                continue
            seen.add(candidate)
            unique.append(candidate)
        return unique

    def _fallback_companion_path(self, source_path: str, *, exclude_path: str | None = None) -> str | None:
        companion_paths = self._same_stem_companion_paths(source_path)
        non_raw_paths = [
            path
            for path in companion_paths
            if path and path != exclude_path and not is_raw_image_path(path)
        ]
        if not non_raw_paths:
            return None
        return select_preferred_companion_path(non_raw_paths) or non_raw_paths[0]

    def _companion_state_for_path(self, source_path: str) -> tuple[str, dict[str, object]]:
        group_key = companion_group_key(source_path)
        state = self._pending_companion_groups.get(group_key)
        if state is None:
            state = {
                "paths": set(),
                "timer": None,
                "raw_failed": False,
                "failed_raw_path": None,
            }
            self._pending_companion_groups[group_key] = state
        paths = state.setdefault("paths", set())
        if isinstance(paths, set):
            paths.add(str(source_path))
        return group_key, state

    def _clear_companion_group(self, group_key: str) -> None:
        state = self._pending_companion_groups.pop(group_key, None)
        if isinstance(state, dict):
            timer = state.get("timer")
            if isinstance(timer, QTimer):
                try:
                    timer.stop()
                except Exception:
                    pass
                try:
                    timer.deleteLater()
                except Exception:
                    pass
        self._consumed_companion_groups.add(group_key)

    def _handle_raw_companion_source(self, source: str, *, group_key: str, state: dict[str, object]) -> bool:
        source_path = str(source or "").strip()
        if not source_path:
            return False
        companion_jpeg_path = self._fallback_companion_path(source_path, exclude_path=source_path)
        selected_mode = self._normalize_raw_capture_mode(self._selected_raw_capture_mode())

        if selected_mode == self.RAW_CAPTURE_MODE_REVIEW:
            try:
                pending = self._create_pending_raw_capture(
                    source_path,
                    group_key=group_key,
                    companion_jpeg_path=companion_jpeg_path,
                    raw_settings=self._current_raw_render_settings(),
                    lab_metadata=self._current_lab_metadata(),
                )
            except RawRenderingUnavailableError as exc:
                if companion_jpeg_path and self._ingest_detected_image(companion_jpeg_path):
                    self._clear_companion_group(group_key)
                    return True
                state["raw_failed"] = True
                state["failed_raw_path"] = source_path
                self._show_status(
                    self.tr("RAW preview unavailable for {name}: {error}").format(
                        name=Path(source_path).name,
                        error=str(exc),
                    ),
                    tone="warning",
                    timeout_ms=6000,
                )
                return False
            except RuntimeError as exc:
                if companion_jpeg_path and self._ingest_detected_image(companion_jpeg_path):
                    self._clear_companion_group(group_key)
                    return True
                state["raw_failed"] = True
                state["failed_raw_path"] = source_path
                self._show_status(
                    self.tr("Could not render preview for {name}: {error}").format(
                        name=Path(source_path).name,
                        error=str(exc),
                    ),
                    tone="warning",
                    timeout_ms=6000,
                )
                return False

            self._add_pending_raw_capture(pending)
            self._clear_companion_group(group_key)
            return True

        if self._ingest_detected_image(source_path):
            self._clear_companion_group(group_key)
            return True
        if companion_jpeg_path and companion_jpeg_path != source_path and self._ingest_detected_image(companion_jpeg_path):
            self._clear_companion_group(group_key)
            return True
        state["raw_failed"] = True
        state["failed_raw_path"] = source_path
        return False

    def _queue_companion_source(self, source_path: str) -> bool:
        source = str(source_path or "").strip()
        if not source:
            return False
        if source in self._seen_source_paths:
            return False
        if not Path(source).exists():
            return False
        group_key = companion_group_key(source)
        if self._pending_raw_capture_exists(source, group_key=group_key):
            self._seen_source_paths.add(source)
            return False
        self._seen_source_paths.add(source)
        group_key, state = self._companion_state_for_path(source)
        if group_key in self._consumed_companion_groups:
            return False

        source_preference = self._selected_raw_companion_source_preference()
        if is_raw_image_path(source) and source_preference == RAW_COMPANION_SOURCE_PREFERENCE_PREFER_RAW:
            timer = state.get("timer")
            if isinstance(timer, QTimer):
                try:
                    timer.stop()
                except Exception:
                    pass
            return self._handle_raw_companion_source(source, group_key=group_key, state=state)

        if bool(state.get("raw_failed")):
            if self._ingest_detected_image(source):
                self._clear_companion_group(group_key)
                return True
            return False

        timer = state.get("timer")
        if not isinstance(timer, QTimer):
            timer = QTimer(self)
            timer.setSingleShot(True)
            timer.timeout.connect(lambda key=group_key: self._flush_companion_group(key))
            state["timer"] = timer
        timer.stop()
        timer.start(int(self._raw_companion_hold_ms))
        return True

    def _flush_companion_group(self, group_key: str) -> None:
        if group_key in self._consumed_companion_groups:
            return
        state = self._pending_companion_groups.get(group_key)
        if not isinstance(state, dict):
            return
        timer = state.get("timer")
        if isinstance(timer, QTimer):
            try:
                timer.stop()
            except Exception:
                pass
        paths = [str(path) for path in (state.get("paths") or set()) if str(path or "").strip()]
        if not paths:
            return
        primary_path = paths[0]
        if bool(state.get("raw_failed")):
            fallback = self._fallback_companion_path(primary_path, exclude_path=str(state.get("failed_raw_path") or ""))
            if fallback and self._ingest_detected_image(fallback):
                self._clear_companion_group(group_key)
            return

        candidate_paths = self._same_stem_companion_paths(primary_path)
        source_preference = self._selected_raw_companion_source_preference()
        preferred = select_preferred_companion_path(
            candidate_paths,
            source_preference=source_preference,
        ) or select_preferred_companion_path(paths, source_preference=source_preference)
        if not preferred:
            return
        if is_raw_image_path(preferred):
            if self._handle_raw_companion_source(preferred, group_key=group_key, state=state):
                return
            return
        if self._ingest_detected_image(preferred):
            self._clear_companion_group(group_key)

    def _set_hint(self, text: str | None, tone: str = "info") -> None:
        controller = getattr(self, "_hint_controller", None)
        if controller is not None:
            controller.set_hint(text, tone=tone)

    def _show_status(self, text: str | None, tone: str = "info", timeout_ms: int = 4000) -> None:
        controller = getattr(self, "_hint_controller", None)
        if controller is not None:
            controller.set_status(text, timeout_ms=timeout_ms, tone=tone)

    def changeEvent(self, event) -> None:
        super().changeEvent(event)
        event_type = event.type() if event is not None else None
        palette_change = getattr(QEvent, "PaletteChange", None)
        app_palette_change = getattr(QEvent, "ApplicationPaletteChange", None)
        if event_type in {palette_change, app_palette_change}:
            self._update_lab_state_combo_alerts()

    def _make_combo(self):
        return AdaptiveChoiceSelector(self, compact=True)

    def _add_choice_item(
        self,
        combo,
        text: str,
        value,
        *,
        pill_text: str | None = None,
        tooltip: str | None = None,
        color: str | None = None,
    ) -> None:
        if isinstance(combo, AdaptiveChoiceSelector):
            combo.addItem(text, value, pillText=pill_text, tooltip=tooltip, color=color)
        else:
            combo.addItem(text, value)

    def _build_term_combo(self, category: str):
        combo = self._make_combo()
        if category == "stain" and hasattr(combo, "set_unselected_border_visible"):
            combo.set_unselected_border_visible(False)
        adder = getattr(self, "_add_choice_item", None)
        values = DatabaseTerms.canonicalize_list(
            category,
            SettingsDB.get_list_setting(DatabaseTerms.setting_key(category), DatabaseTerms.default_values(category)),
        )
        for value in values:
            display = DatabaseTerms.translate(category, value)
            if callable(adder):
                adder(
                    combo,
                    display,
                    value,
                    pill_text="—" if value == "Not_set" else display,
                    color=stain_color(value) if category == "stain" else None,
                    tooltip=display,
                )
            else:
                combo.addItem(display, value)
        combo.currentIndexChanged.connect(
            lambda _idx, cat=category, c=combo: self._remember_last_used_term(cat, c.currentData())
        )
        return combo

    def _refresh_term_combo(self, combo, category: str) -> None:
        if combo is None:
            return
        current_value = combo.currentData()
        combo.blockSignals(True)
        combo.clear()
        adder = getattr(self, "_add_choice_item", None)
        values = DatabaseTerms.canonicalize_list(
            category,
            SettingsDB.get_list_setting(DatabaseTerms.setting_key(category), DatabaseTerms.default_values(category)),
        )
        for value in values:
            display = DatabaseTerms.translate(category, value)
            if callable(adder):
                adder(
                    combo,
                    display,
                    value,
                    pill_text="—" if value == "Not_set" else display,
                    color=stain_color(value) if category == "stain" else None,
                    tooltip=display,
                )
            else:
                combo.addItem(display, value)
        index = combo.findData(current_value) if current_value is not None else -1
        if index < 0:
            index = 0 if combo.count() else -1
        if index >= 0:
            combo.setCurrentIndex(index)
        combo.blockSignals(False)

    def refresh_microscope_tag_preferences(self) -> None:
        for category, combo in (
            ("contrast", getattr(self, "contrast_combo", None)),
            ("mount", getattr(self, "mount_combo", None)),
            ("stain", getattr(self, "stain_combo", None)),
            ("sample", getattr(self, "sample_combo", None)),
        ):
            self._refresh_term_combo(combo, category)
        self._update_lab_state_combo_alerts()

    def _restore_term_selection(self, combo, category: str) -> None:
        saved = DatabaseTerms.canonicalize(category, SettingsDB.get_setting(DatabaseTerms.last_used_key(category), ""))
        if not saved:
            return
        index = combo.findData(saved)
        if index >= 0:
            combo.setCurrentIndex(index)

    def _remember_last_used_term(self, category: str, value: str | None) -> None:
        if value:
            SettingsDB.set_setting(DatabaseTerms.last_used_key(category), str(value))

    def _lab_state_combo_alert_stylesheet(self) -> str:
        return lab_state_combo_alert_stylesheet(True)

    @staticmethod
    def _combo_is_unset(combo) -> bool:
        return combo_is_unset(combo)

    def _set_lab_state_combo_alert(self, combo, alert: bool) -> None:
        update_combo_alert(combo, alert)

    def _update_lab_state_combo_alerts(self, *_args) -> None:
        update_combo_alerts(
            (
                getattr(self, "objective_combo", None),
                getattr(self, "contrast_combo", None),
                getattr(self, "mount_combo", None),
                getattr(self, "stain_combo", None),
                getattr(self, "sample_combo", None),
            )
        )

    def _should_preserve_live_image_view(self, image_path: str | None) -> bool:
        label = getattr(self, "live_image_label", None)
        if label is None or not image_path:
            return False
        current_path = str(getattr(label, "_full_image_path", "") or "").strip()
        if not current_path:
            return False
        try:
            return Path(current_path) == Path(str(image_path))
        except Exception:
            return current_path == str(image_path)

    def _populate_objective_combo(self) -> None:
        self.objective_combo.clear()
        adder = getattr(self, "_add_choice_item", None)
        objectives = load_objectives()
        rows = []
        for key, obj in objectives.items():
            if objective_is_macro_profile(obj, key):
                continue
            rows.append((key, obj))
        rows.sort(key=lambda item: objective_sort_value(item[1], item[0]))
        for key, obj in rows:
            label = objective_display_name(obj, key) or str(key)
            if callable(adder):
                adder(
                    self.objective_combo,
                    label,
                    key,
                    pill_text=objective_short_label(obj, key) or label,
                    color=objective_color(obj, key),
                    tooltip=label,
                )
            else:
                self.objective_combo.addItem(label, key)
        saved = str(SettingsDB.get_setting(self.SETTING_LAST_OBJECTIVE, "") or "").strip()
        if saved:
            index = self.objective_combo.findData(saved)
            if index >= 0:
                self.objective_combo.setCurrentIndex(index)
        self._update_lab_state_combo_alerts()

    def _save_objective_selection(self) -> None:
        objective_value = self._selected_combo_value(self.objective_combo)
        SettingsDB.set_setting(self.SETTING_LAST_OBJECTIVE, "" if objective_value is None else str(objective_value))
        refresh_viewer_objective_tag = getattr(self, "_refresh_viewer_objective_tag_from_current_state", None)
        if callable(refresh_viewer_objective_tag):
            refresh_viewer_objective_tag()

    def _normalize_session_mode(self, value: str | None) -> str:
        mode = str(value or self.SESSION_MODE_LIVE).strip().lower()
        return mode if mode in {self.SESSION_MODE_LIVE, self.SESSION_MODE_OFFLINE} else self.SESSION_MODE_LIVE

    def _selected_session_mode(self) -> str:
        selector = getattr(self, "session_mode_selector", None)
        if selector is not None:
            return self._normalize_session_mode(selector.selected_value(self.SESSION_MODE_LIVE))
        return self.SESSION_MODE_LIVE

    def _session_mode_label(self, mode: str | None = None) -> str:
        normalized = self._normalize_session_mode(mode or self._selected_session_mode())
        if normalized == self.SESSION_MODE_OFFLINE:
            return self.tr("Offline")
        return self.tr("Live capture")

    def _restore_session_mode(self) -> None:
        saved = self._normalize_session_mode(SettingsDB.get_setting(self.SETTING_SESSION_MODE, self.SESSION_MODE_LIVE))
        selector = getattr(self, "session_mode_selector", None)
        if selector is not None:
            selector.set_selected_value(saved)

    def _on_session_mode_changed(self) -> None:
        SettingsDB.set_setting(self.SETTING_SESSION_MODE, self._selected_session_mode())
        self._update_session_controls()

    def _connect_session_logging_signals(self) -> None:
        self.objective_combo.currentIndexChanged.connect(
            lambda _idx: self._log_dropdown_change(
                "objective_name",
                self._selected_combo_value(self.objective_combo),
                self.objective_combo.currentText(),
            )
        )
        self.objective_combo.currentIndexChanged.connect(self._update_lab_state_combo_alerts)
        self.objective_combo.currentIndexChanged.connect(self._refresh_viewer_objective_tag_from_current_state)
        self.objective_combo.currentIndexChanged.connect(self._sync_selected_pending_raw_metadata_from_controls)
        self.contrast_combo.currentIndexChanged.connect(
            lambda _idx: self._log_dropdown_change(
                "contrast",
                self._selected_combo_value(self.contrast_combo),
                self.contrast_combo.currentText(),
            )
        )
        self.contrast_combo.currentIndexChanged.connect(self._update_lab_state_combo_alerts)
        self.contrast_combo.currentIndexChanged.connect(self._refresh_viewer_objective_tag_from_current_state)
        self.contrast_combo.currentIndexChanged.connect(self._sync_selected_pending_raw_metadata_from_controls)
        self.mount_combo.currentIndexChanged.connect(
            lambda _idx: self._log_dropdown_change(
                "mount_medium",
                self._selected_combo_value(self.mount_combo),
                self.mount_combo.currentText(),
            )
        )
        self.mount_combo.currentIndexChanged.connect(self._update_lab_state_combo_alerts)
        self.mount_combo.currentIndexChanged.connect(self._refresh_viewer_objective_tag_from_current_state)
        self.mount_combo.currentIndexChanged.connect(self._sync_selected_pending_raw_metadata_from_controls)
        self.stain_combo.currentIndexChanged.connect(
            lambda _idx: self._log_dropdown_change(
                "stain",
                self._selected_combo_value(self.stain_combo),
                self.stain_combo.currentText(),
            )
        )
        self.stain_combo.currentIndexChanged.connect(self._update_lab_state_combo_alerts)
        self.stain_combo.currentIndexChanged.connect(self._refresh_viewer_objective_tag_from_current_state)
        self.stain_combo.currentIndexChanged.connect(self._sync_selected_pending_raw_metadata_from_controls)
        self.sample_combo.currentIndexChanged.connect(
            lambda _idx: self._log_dropdown_change(
                "sample_type",
                self._selected_combo_value(self.sample_combo),
                self.sample_combo.currentText(),
            )
        )
        self.sample_combo.currentIndexChanged.connect(self._update_lab_state_combo_alerts)
        self.sample_combo.currentIndexChanged.connect(self._refresh_viewer_objective_tag_from_current_state)
        self.sample_combo.currentIndexChanged.connect(self._sync_selected_pending_raw_metadata_from_controls)

    def _restore_watch_dir(self) -> None:
        saved = str(SettingsDB.get_setting(self.SETTING_WATCH_DIR, "") or "").strip()
        if saved:
            self.watch_dir_input.setText(saved)
            self.watch_dir_input.setCursorPosition(len(saved))
            self.watch_dir_input.setToolTip(saved)

    def _on_watch_dir_changed(self, text: str) -> None:
        SettingsDB.set_setting(self.SETTING_WATCH_DIR, str(text or "").strip())
        if hasattr(self, "watch_dir_input") and self.watch_dir_input is not None:
            self.watch_dir_input.setToolTip(str(text or "").strip())
        self._update_session_controls()

    def _choose_watch_dir(self) -> None:
        current = str(self.watch_dir_input.text() or "").strip()
        start_dir = current if current and Path(current).exists() else str(Path.home())
        chosen = QFileDialog.getExistingDirectory(self, self.tr("Choose microscope capture folder"), start_dir)
        if chosen:
            self.watch_dir_input.setText(chosen)
            self.watch_dir_input.setCursorPosition(len(chosen))
            self.watch_dir_input.setToolTip(chosen)

    def _on_rescan_watch_folder(self) -> None:
        queued = self.rescan_watch_folder()
        if queued > 0:
            self._show_status(
                self.tr("Rescanned the watched folder and queued {count} image(s).").format(count=queued),
                tone="info",
                timeout_ms=4000,
            )
        else:
            self._show_status(
                self.tr("No supported images were ready to rescan."),
                tone="info",
                timeout_ms=3000,
            )

    def _open_observations_tab(self) -> None:
        self._main_window.tab_widget.setCurrentIndex(0)
        table = getattr(getattr(self._main_window, "observations_tab", None), "table", None)
        if table is not None:
            try:
                table.setFocus()
            except Exception:
                pass

    def _reset_viewer(self) -> None:
        self.live_image_label.reset_view()

    def is_session_running(self) -> bool:
        return bool(self._session_active or (self._watcher is not None and self._watcher.isRunning()))

    def sync_from_active_observation(self) -> None:
        if self.is_session_running():
            return
        obs_id = int(getattr(self._main_window, "active_observation_id", 0) or 0)
        if obs_id > 0:
            self.set_target_observation(obs_id)

    def set_target_observation(self, observation_id: int | None, display_name: str | None = None) -> None:
        del display_name
        if self.is_session_running():
            return
        try:
            obs_id = int(observation_id or 0)
        except Exception:
            obs_id = 0
        observation = ObservationDB.get_observation(obs_id) if obs_id > 0 else None
        if observation:
            self._target_observation_id = obs_id
            self._target_observation = observation
        else:
            self._target_observation_id = None
            self._target_observation = None
        self._update_target_display()
        self._update_session_controls()

    def _scientific_name_text(self, observation: dict | None) -> str:
        obs = dict(observation or {})
        scientific = " ".join(
            part
            for part in (str(obs.get("genus") or "").strip(), str(obs.get("species") or "").strip())
            if part
        ).strip()
        if scientific:
            return scientific
        return str(obs.get("species_guess") or "").strip()

    def _vernacular_name_text(self, observation: dict | None) -> str:
        obs = dict(observation or {})
        return str(obs.get("common_name") or "").strip()

    def _observation_summary_text(self, observation: dict | None) -> str:
        vernacular = self._vernacular_name_text(observation)
        scientific = self._scientific_name_text(observation)
        if vernacular and scientific:
            return self.tr("{vernacular} \u2014 {scientific}").format(
                vernacular=vernacular,
                scientific=scientific,
            )
        return vernacular or scientific or self.tr("Unknown observation")

    def _update_target_display(self) -> None:
        observation = self._target_observation
        if observation:
            vernacular = self._vernacular_name_text(observation) or "\u2014"
            scientific = self._scientific_name_text(observation) or "\u2014"
            date_text = str(observation.get("date") or "").strip() or "\u2014"
            self.current_observation_name_label.setText(vernacular)
            self.current_observation_scientific_label.setText(scientific)
            self.current_observation_date_label.setText(
                self.tr("Date: {date}").format(date=date_text)
            )
            self._update_observation_thumbnail()
        else:
            self.current_observation_name_label.setText(self.tr("No current observation selected"))
            self.current_observation_scientific_label.setText("\u2014")
            self.current_observation_date_label.setText(self.tr("Date: \u2014"))
            self._clear_observation_thumbnail()

    def _build_recording_tab_icon(self) -> QIcon:
        pixmap = QPixmap(14, 14)
        pixmap.fill(Qt.transparent)
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.Antialiasing, True)
        painter.setPen(Qt.NoPen)
        painter.setBrush(QColor("#e74c3c"))
        painter.drawEllipse(2, 2, 10, 10)
        painter.end()
        return QIcon(pixmap)

    def _update_tab_recording_indicator(self, active: bool) -> None:
        tab_widget = getattr(self._main_window, "tab_widget", None)
        if tab_widget is None:
            return
        tab_index = tab_widget.indexOf(self)
        if tab_index < 0:
            return
        tab_widget.setTabIcon(tab_index, self._recording_tab_icon if active else QIcon())

    def _set_session_button_style(self, active: bool) -> None:
        self.start_stop_btn.setStyleSheet(
            self.SESSION_BUTTON_ACTIVE_STYLE if active else self.SESSION_BUTTON_BASE_STYLE
        )

    def _clear_observation_thumbnail(self) -> None:
        self.current_observation_thumb_label.clear()
        self.current_observation_thumb_label.setText(self.tr("No image"))

    def _update_observation_thumbnail(self) -> None:
        observation_id = int(self._target_observation_id or 0)
        if observation_id <= 0:
            self._clear_observation_thumbnail()
            return

        images = ImageDB.get_images_for_observation(observation_id)
        if not images:
            self._clear_observation_thumbnail()
            return

        image = images[-1]
        image_id = int(image.get("id") or 0)
        candidate_path = ""
        if image_id > 0:
            candidate_path = str(get_thumbnail_path(image_id, "small") or "").strip()
        if not candidate_path:
            candidate_path = str(image.get("filepath") or "").strip()
        if not candidate_path or not Path(candidate_path).exists():
            self._clear_observation_thumbnail()
            return

        pixmap = QPixmap(candidate_path)
        if pixmap.isNull():
            reader = QImageReader(candidate_path)
            reader.setAutoTransform(True)
            image_data = reader.read()
            pixmap = QPixmap.fromImage(image_data) if not image_data.isNull() else QPixmap()
        if pixmap.isNull():
            self._clear_observation_thumbnail()
            return

        scaled = pixmap.scaled(
            self.current_observation_thumb_label.size(),
            Qt.KeepAspectRatio,
            Qt.SmoothTransformation,
        )
        self.current_observation_thumb_label.setPixmap(scaled)
        self.current_observation_thumb_label.setText("")

    def _current_lab_metadata(self) -> dict:
        return {
            "session_id": str(self._session_id or "").strip() or None,
            "session_kind": self._normalize_session_mode(self._active_session_mode or self._selected_session_mode()),
            "objective_name": self._selected_combo_value(self.objective_combo),
            "objective_label": str(self.objective_combo.currentText() or "").strip() or None,
            "contrast": DatabaseTerms.canonicalize("contrast", self._selected_combo_value(self.contrast_combo)),
            "contrast_label": str(self.contrast_combo.currentText() or "").strip() or None,
            "mount_medium": DatabaseTerms.canonicalize("mount", self._selected_combo_value(self.mount_combo)),
            "mount_label": str(self.mount_combo.currentText() or "").strip() or None,
            "stain": DatabaseTerms.canonicalize("stain", self._selected_combo_value(self.stain_combo)),
            "stain_label": str(self.stain_combo.currentText() or "").strip() or None,
            "sample_type": DatabaseTerms.canonicalize("sample", self._selected_combo_value(self.sample_combo)),
            "sample_label": str(self.sample_combo.currentText() or "").strip() or None,
        }

    def _log_session_event(
        self,
        event_type: str,
        *,
        attribute_name: str | None = None,
        value: str | None = None,
        metadata: dict | None = None,
    ) -> int:
        observation_id = int(self._session_observation_id or 0)
        session_id = str(self._session_id or "").strip()
        if observation_id <= 0 or not session_id:
            return 0
        merged_metadata = dict(metadata or {})
        if not merged_metadata:
            merged_metadata = {}
        if event_type != "manual_note":
            merged_metadata.setdefault("lab_metadata", self._current_lab_metadata())
        return SessionLogDB.add_event(
            observation_id,
            session_id,
            event_type,
            session_kind=self._active_session_mode or self._selected_session_mode(),
            attribute_name=attribute_name,
            value=value,
            metadata_json=merged_metadata or None,
        )

    def _log_dropdown_change(self, attribute_name: str, raw_value, display_value: str | None = None) -> None:
        if not self.is_session_running():
            return
        if raw_value is None:
            value = None
        else:
            value = str(raw_value).strip() or None
        metadata = {
            "display_value": str(display_value or "").strip() or None,
        }
        self._log_session_event(
            "dropdown_change",
            attribute_name=attribute_name,
            value=value,
            metadata=metadata,
        )

    def _log_initial_lab_state(self) -> None:
        self._log_dropdown_change("objective_name", self._selected_combo_value(self.objective_combo), self.objective_combo.currentText())
        self._log_dropdown_change("contrast", self._selected_combo_value(self.contrast_combo), self.contrast_combo.currentText())
        self._log_dropdown_change("mount_medium", self._selected_combo_value(self.mount_combo), self.mount_combo.currentText())
        self._log_dropdown_change("stain", self._selected_combo_value(self.stain_combo), self.stain_combo.currentText())
        self._log_dropdown_change("sample_type", self._selected_combo_value(self.sample_combo), self.sample_combo.currentText())

    def _add_session_note(self) -> None:
        note_text = str(self.session_note_input.text() or "").strip()
        if not note_text:
            return
        if not self.is_session_running():
            self._show_status(
                self.tr("Start a Live Lab session before adding notes."),
                tone="warning",
                timeout_ms=4000,
            )
            return
        self._log_session_event("manual_note", value=note_text, metadata={"note_length": len(note_text)})
        self.session_note_input.clear()
        self._show_status(
            self.tr("Added session note."),
            tone="success",
            timeout_ms=2500,
        )

    def _update_session_controls(self) -> None:
        running = self.is_session_running()
        stopping = bool(self._session_stop_pending and self._watcher is not None)
        selected_mode = self._selected_session_mode()
        active_mode = self._normalize_session_mode(self._active_session_mode or selected_mode)
        mode_is_live = active_mode == self.SESSION_MODE_LIVE
        watch_dir = str(self.watch_dir_input.text() or "").strip()
        watch_path = Path(watch_dir) if watch_dir else None
        watch_ok = bool(watch_path and watch_path.exists() and watch_path.is_dir())
        can_start = bool(
            self._target_observation_id
            and not stopping
            and (watch_ok if selected_mode == self.SESSION_MODE_LIVE else True)
        )

        self.session_mode_combo.setEnabled(not running and not stopping)
        self.session_mode_combo.setProperty(
            "_hint_disabled_text",
            self.tr("Stop the current session before changing capture mode."),
        )
        self.watch_group.setVisible(mode_is_live if running else selected_mode == self.SESSION_MODE_LIVE)
        self.watch_dir_input.setReadOnly(running or stopping or selected_mode != self.SESSION_MODE_LIVE)
        self.browse_btn.setEnabled(not running and not stopping and selected_mode == self.SESSION_MODE_LIVE)
        rescan_btn = getattr(self, "rescan_btn", None)
        if rescan_btn is not None:
            rescan_btn.setEnabled(
                bool(running and not stopping and selected_mode == self.SESSION_MODE_LIVE and watch_ok)
            )
        self.start_stop_btn.setEnabled(bool(running or can_start))
        self.session_note_input.setEnabled(bool(running and not stopping))
        self.add_note_btn.setEnabled(bool(running and not stopping))

        if stopping:
            self.start_stop_btn.setText(self.tr("Stopping..."))
        elif running:
            self.start_stop_btn.setText(self.tr("Stop Session"))
        else:
            self.start_stop_btn.setText(
                self.tr("Start Log Session")
                if selected_mode == self.SESSION_MODE_OFFLINE
                else self.tr("Start Session")
            )
        if not running and not stopping:
            if selected_mode == self.SESSION_MODE_OFFLINE:
                disabled_hint = self.tr("Choose a current observation before starting the session.")
            else:
                disabled_hint = self.tr("Choose a current observation and an existing watched folder first.")
                if self._target_observation_id and not watch_ok:
                    disabled_hint = self.tr("Choose an existing watched folder before starting the session.")
                elif not self._target_observation_id and watch_ok:
                    disabled_hint = self.tr("Choose a current observation before starting the session.")
            self.start_stop_btn.setProperty("_hint_disabled_text", disabled_hint)

        self._set_session_button_style(bool(running or stopping))
        self._update_tab_recording_indicator(bool(running or stopping))

    def _toggle_session(self) -> None:
        if self.is_session_running():
            self.stop_session()
        else:
            self.start_session()

    def start_session(self) -> None:
        if self.is_session_running():
            return
        self._cancel_raw_edit_session(restore_selection=False)
        if not self._target_observation_id or not self._target_observation:
            self._show_status(
                self.tr("Choose a current observation before starting a Live Lab session."),
                tone="warning",
                timeout_ms=5000,
            )
            return
        selected_mode = self._selected_session_mode()
        watch_dir = str(self.watch_dir_input.text() or "").strip()
        if (
            selected_mode == self.SESSION_MODE_LIVE
            and (not watch_dir or not Path(watch_dir).exists() or not Path(watch_dir).is_dir())
        ):
            self._show_status(
                self.tr("Choose an existing microscope capture folder before starting the session."),
                tone="warning",
                timeout_ms=5000,
            )
            return

        self._session_observation_id = int(self._target_observation_id)
        self._session_observation_snapshot = dict(self._target_observation)
        self._session_image_ids = []
        self._selected_session_image_id = None
        self._seen_source_paths = set()
        self._reset_companion_dedupe_state()
        self._session_import_count = 0
        self._session_active = True
        self._session_id = uuid4().hex
        self._active_session_mode = selected_mode
        self._session_stop_pending = False
        self._pending_stop_status = None
        self.session_gallery.clear()
        if selected_mode == self.SESSION_MODE_LIVE:
            self._clear_session_viewer(
                title=self.tr("Waiting for first import"),
                meta=self.tr("New microscope captures from the watched folder will appear here automatically."),
            )
            self._watcher = LabWatcherWorker(watch_dir, parent=self)
            self._watcher.new_image_detected.connect(self._on_new_image_detected)
            self._watcher.error_occurred.connect(self._on_watcher_error)
            self._watcher.finished.connect(self._on_watcher_finished)
            self._watcher.start()
            status_text = self.tr("Live capture session started for {name}.").format(
                name=self._observation_summary_text(self._session_observation_snapshot),
            )
        else:
            self._clear_session_viewer(
                title=self.tr("Offline session"),
                meta=self.tr("Log microscope-state changes now and match images later in the Ingestion Hub."),
            )
            status_text = self.tr("Offline session started for {name}.").format(
                name=self._observation_summary_text(self._session_observation_snapshot),
            )

        self._log_session_event(
            "session_started",
            value=selected_mode,
            metadata={
                "mode_label": self._session_mode_label(selected_mode),
                "watch_dir": watch_dir if selected_mode == self.SESSION_MODE_LIVE else None,
            },
        )
        self._log_initial_lab_state()
        self._show_status(status_text, tone="success", timeout_ms=4000)
        self._update_session_controls()
        self._update_pending_raw_controls()
        if self._current_pending_raw_capture() is not None:
            self._show_pending_raw_capture(self._selected_pending_raw_index)

    def stop_session(self) -> None:
        if not self.is_session_running():
            return
        watcher = self._watcher
        if watcher is None:
            self._finalize_session_stop()
            return

        self._session_stop_pending = True
        try:
            watcher.stop()
        except Exception:
            pass

        if watcher.isRunning():
            if self._pending_stop_status is None:
                self._show_status(self.tr("Stopping Live Lab session..."), tone="info", timeout_ms=0)
            self._update_session_controls()
            return

        self._watcher = None
        self._finalize_session_stop()

    def _finalize_session_stop(self) -> None:
        had_session = bool(self._session_active and self._session_observation_id and self._session_id)
        import_count = int(self._session_import_count or 0)
        session_mode = self._active_session_mode or self._selected_session_mode()
        if had_session:
            self._log_session_event(
                "session_stopped",
                value=session_mode,
                metadata={"import_count": import_count},
            )
        self._session_active = False
        self._session_id = None
        self._active_session_mode = None
        self._session_stop_pending = False
        self._cancel_raw_edit_session(restore_selection=False)
        self._reset_companion_dedupe_state()
        self._update_session_controls()
        self._update_pending_raw_controls()
        if self._current_pending_raw_capture() is not None:
            self._show_pending_raw_capture(self._selected_pending_raw_index)

        if self._pending_stop_status is not None:
            message, tone, timeout_ms = self._pending_stop_status
            self._pending_stop_status = None
            self._show_status(message, tone=tone, timeout_ms=timeout_ms)
            return

        if had_session:
            self._show_status(
                (
                    self.tr("Offline session stopped. Logged {count} imported image(s).")
                    if session_mode == self.SESSION_MODE_OFFLINE
                    else self.tr("Live Lab session stopped. Imported {count} image(s).")
                ).format(count=import_count),
                tone="success" if import_count else "info",
                timeout_ms=4000,
            )

    def shutdown(self) -> None:
        self.stop_session()

    def _on_watcher_finished(self) -> None:
        sender_fn = getattr(self, "sender", None)
        if callable(sender_fn) and sender_fn() is not self._watcher:
            return
        self._watcher = None
        if self._session_stop_pending or self._session_observation_id:
            self._finalize_session_stop()
        else:
            self._update_session_controls()

    def _on_watcher_error(self, message: str) -> None:
        text = str(message or "").strip() or self.tr("The Live Lab watcher stopped unexpectedly.")
        self._pending_stop_status = (text, "warning", 6000)
        self.stop_session()

    def _on_new_image_detected(self, source_path: str) -> None:
        if not self.is_session_running() or self._active_session_mode != self.SESSION_MODE_LIVE:
            return
        source = str(source_path or "").strip()
        if not source or source in self._seen_source_paths:
            return
        self._queue_companion_source(source)

    def rescan_watch_folder(self) -> int:
        if not self.is_session_running() or self._active_session_mode != self.SESSION_MODE_LIVE:
            return 0
        watch_dir = str(self.watch_dir_input.text() or "").strip()
        if not watch_dir:
            return 0
        watch_path = Path(watch_dir)
        try:
            if not watch_path.exists() or not watch_path.is_dir():
                return 0
        except Exception:
            return 0

        supported_suffixes = {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".heic", ".heif"} | set(
            SUPPORTED_RAW_SUFFIXES
        )
        try:
            children = sorted(
                watch_path.iterdir(),
                key=lambda path: (path.name.casefold(), str(path).casefold()),
            )
        except Exception:
            return 0

        queued = 0
        for child in children:
            try:
                if not child.is_file():
                    continue
            except Exception:
                continue
            if child.suffix.lower() not in supported_suffixes:
                continue
            try:
                candidate = str(child.resolve())
            except Exception:
                candidate = str(child)
            if self._queue_companion_source(candidate):
                queued += 1
        return queued

    def _finalize_local_ingest(
        self,
        source_path: str,
        ingest,
        *,
        raw_settings: RawRenderSettings | None = None,
        lab_metadata: dict[str, object] | None = None,
    ) -> bool:
        observation_id = int(self._session_observation_id or 0)
        if observation_id <= 0:
            return False

        ingest_lab_metadata = merge_image_lab_metadata(
            self._current_lab_metadata(),
            lab_metadata,
            getattr(ingest, "lab_metadata", None),
        )
        objective_key = ingest_lab_metadata.get("objective_name")
        if objective_key is None or not str(objective_key).strip():
            objective_key = self._selected_combo_value(self.objective_combo)
        contrast_value = ingest_lab_metadata.get("contrast")
        mount_value = ingest_lab_metadata.get("mount_medium")
        stain_value = ingest_lab_metadata.get("stain")
        sample_value = ingest_lab_metadata.get("sample_type")
        if not str(contrast_value or "").strip():
            contrast_value = self._selected_combo_value(self.contrast_combo)
        if not str(mount_value or "").strip():
            mount_value = self._selected_combo_value(self.mount_combo)
        if not str(stain_value or "").strip():
            stain_value = self._selected_combo_value(self.stain_combo)
        if not str(sample_value or "").strip():
            sample_value = self._selected_combo_value(self.sample_combo)
        objective_key = str(objective_key or "").strip() or None
        objective = load_objectives().get(objective_key) if objective_key else None
        scale = objective.get("microns_per_pixel") if isinstance(objective, dict) else None
        calibration_id = CalibrationDB.get_active_calibration_id(objective_key) if objective_key else None
        original_filepath = ingest.original_path if ingest.original_path != ingest.working_path else None

        image_id = ImageDB.add_image(
            observation_id=observation_id,
            filepath=ingest.working_path,
            image_type="microscope",
            scale=scale,
            objective_name=objective_key,
            contrast=DatabaseTerms.canonicalize("contrast", contrast_value),
            mount_medium=DatabaseTerms.canonicalize("mount", mount_value),
            stain=DatabaseTerms.canonicalize("stain", stain_value),
            sample_type=DatabaseTerms.canonicalize("sample", sample_value),
            calibration_id=calibration_id,
            resample_scale_factor=1.0,
            original_filepath=original_filepath,
            lab_metadata=ingest_lab_metadata or None,
            **ingest.provenance_kwargs(),
        )

        image_data = ImageDB.get_image(image_id)
        stored_path = str((image_data or {}).get("filepath") or ingest.working_path)
        output_dir = get_images_dir() / "imports"
        warning_text = ""
        try:
            generate_all_sizes(stored_path, image_id)
        except Exception as exc:
            warning_text = self.tr("Thumbnail generation warning for {name}: {error}").format(
                name=Path(stored_path).name,
                error=str(exc),
            )
        cleanup_import_temp_file(source_path, ingest.working_path, stored_path, output_dir)

        self._session_image_ids.append(int(image_id))
        self._selected_session_image_id = int(image_id)
        self._session_import_count += 1
        self._update_observation_thumbnail()
        self._refresh_session_gallery()
        self._show_session_image(image_id)
        self._update_session_controls()

        status_text = self.tr("Imported {name} into the current observation.").format(
            name=Path(stored_path).name,
        )
        if warning_text:
            status_text = f"{status_text} {warning_text}"
        self._show_status(
            status_text,
            tone="warning" if warning_text else "success",
            timeout_ms=6000 if warning_text else 3500,
        )
        self._log_session_event(
            "image_imported",
            value=Path(stored_path).name,
            metadata={
                "image_id": int(image_id),
                "filepath": stored_path,
                "warning_text": warning_text or None,
            },
        )
        if getattr(ingest, "raw_render_snapshot", None) is not None:
            save_helper = getattr(self, "_save_raw_processing_settings_for_current_context", None)
            if callable(save_helper):
                save_helper(raw_settings or self._current_raw_render_settings())
            else:
                LiveLabTab._save_raw_processing_settings_for_current_context(
                    self,
                    raw_settings or self._current_raw_render_settings(),
                )
        self._refresh_main_window_after_import(image_id)
        return True

    def _ingest_detected_image(
        self,
        source_path: str,
        *,
        raw_settings: RawRenderSettings | None = None,
        lab_metadata: dict[str, object] | None = None,
        observation_id: int | None = None,
    ) -> bool:
        observation_id = int(observation_id or self._session_observation_id or 0)
        if observation_id <= 0:
            return False

        output_dir = get_images_dir() / "imports"
        output_dir.mkdir(parents=True, exist_ok=True)
        ingest_context = merge_image_lab_metadata(self._current_lab_metadata(), lab_metadata)
        ingest_context["image_type"] = "microscope"
        resolved_raw_settings = RawRenderSettings.from_dict(raw_settings or self._current_raw_render_settings())
        try:
            ingest = prepare_local_ingest_image(
                source_path,
                raw_settings=resolved_raw_settings,
                lab_metadata=ingest_context,
                output_dir=output_dir,
            )
        except RawRenderingUnavailableError as exc:
            self._show_status(
                self.tr("RAW image {name} cannot be imported yet: {error}").format(
                    name=Path(source_path).name,
                    error=str(exc),
                ),
                tone="warning",
                timeout_ms=6000,
            )
            return False
        except RuntimeError as exc:
            self._show_status(
                self.tr("Could not prepare {name}: {error}").format(
                    name=Path(source_path).name,
                    error=str(exc),
                ),
                tone="warning",
                timeout_ms=6000,
            )
            return False

        finalize_helper = getattr(self, "_finalize_local_ingest", None)
        if callable(finalize_helper):
            return finalize_helper(
                source_path,
                ingest,
                raw_settings=resolved_raw_settings,
                lab_metadata=ingest_context,
            )
        return LiveLabTab._finalize_local_ingest(
            self,
            source_path,
            ingest,
            raw_settings=resolved_raw_settings,
            lab_metadata=ingest_context,
        )

    def _refresh_session_gallery(self) -> None:
        gallery = getattr(self, "session_gallery", None)
        if gallery is None:
            return
        selected_paths: list[str] = []
        selected_paths_fn = getattr(gallery, "selected_paths", None)
        if callable(selected_paths_fn):
            try:
                selected_paths = [str(path) for path in selected_paths_fn() if path]
            except Exception:
                selected_paths = []
        items, selected_key = self._session_gallery_items()
        gallery.set_items(items)
        is_multi_select = False
        is_multi_select_fn = getattr(gallery, "is_multi_select", None)
        if callable(is_multi_select_fn):
            try:
                is_multi_select = bool(is_multi_select_fn())
            except Exception:
                is_multi_select = False
        if is_multi_select and selected_paths:
            select_paths = getattr(gallery, "select_paths", None)
            if callable(select_paths):
                select_paths(selected_paths)
                selected_paths_fn = getattr(gallery, "selected_paths", None)
                restored_paths = []
                if callable(selected_paths_fn):
                    try:
                        restored_paths = [str(path) for path in selected_paths_fn() if path]
                    except Exception:
                        restored_paths = []
                if not restored_paths and selected_key is not None:
                    fallback_paths = []
                    for item in items:
                        item_key = item.get("id") if item.get("id") is not None else item.get("filepath")
                        if item_key == selected_key and item.get("filepath"):
                            fallback_paths = [str(item.get("filepath"))]
                            break
                    if fallback_paths:
                        select_paths(fallback_paths)
        elif selected_key is not None:
            gallery.select_image(selected_key)

    def _on_session_gallery_clicked(self, image_id, _path: str) -> None:
        gallery = getattr(self, "session_gallery", None)
        is_multi_select = False
        if gallery is not None:
            is_multi_select_fn = getattr(gallery, "is_multi_select", None)
            if callable(is_multi_select_fn):
                try:
                    is_multi_select = bool(is_multi_select_fn())
                except Exception:
                    is_multi_select = False
        current_edit_session = getattr(self, "_raw_edit_session", None)
        try:
            clicked_image_id = int(image_id or 0)
        except Exception:
            clicked_image_id = 0
        if current_edit_session is not None and clicked_image_id != int(current_edit_session.image_id):
            self._cancel_raw_edit_session(restore_selection=False)
        pending_index = self._pending_raw_capture_index_for_key(image_id)
        if pending_index is None:
            pending_index = self._pending_raw_capture_index_for_key(_path)
        if pending_index is not None:
            capture = None
            captures = getattr(self, "_pending_raw_captures", [])
            if 0 <= int(pending_index) < len(captures):
                maybe_capture = captures[int(pending_index)]
                if isinstance(maybe_capture, PendingRawCapture):
                    capture = maybe_capture
            if capture is not None:
                apply_microscope_state = getattr(self, "_apply_microscope_state_to_controls", None)
                if callable(apply_microscope_state):
                    apply_microscope_state(capture.lab_metadata)
            if is_multi_select and gallery is not None:
                center_on_key = getattr(gallery, "center_on_key", None)
                if callable(center_on_key):
                    center_on_key(image_id if image_id is not None else _path)
            self._show_pending_raw_capture(pending_index)
            return
        resolved_image_id = clicked_image_id
        if resolved_image_id <= 0:
            return
        self._selected_pending_raw_index = -1
        self._selected_session_image_id = resolved_image_id
        self._update_pending_raw_controls()
        if is_multi_select:
            image = ImageDB.get_image(resolved_image_id)
            if image is not None:
                apply_microscope_state = getattr(self, "_apply_microscope_state_to_controls", None)
                if callable(apply_microscope_state):
                    apply_microscope_state(image)
            if gallery is not None:
                center_on_key = getattr(gallery, "center_on_key", None)
                if callable(center_on_key):
                    center_on_key(resolved_image_id)
        else:
            if gallery is not None:
                select_image = getattr(gallery, "select_image", None)
                if callable(select_image):
                    select_image(resolved_image_id)
        self._show_session_image(resolved_image_id)

    def _on_session_gallery_selection_changed(self, selected_paths: list[str]) -> None:
        try:
            selected_count = len([path for path in selected_paths or [] if path])
        except Exception:
            selected_count = 0
        if selected_count > 1:
            self._show_status(
                self.tr("Settings will be applied to selected images."),
                tone="info",
                timeout_ms=3500,
            )
        self._update_raw_processing_visibility()

    def _on_session_gallery_delete_requested(self, key) -> None:
        if self._delete_session_gallery_item(key):
            return

    def _delete_session_gallery_item(self, key) -> bool:
        pending_index = self._pending_raw_capture_index_for_key(key)
        if pending_index is not None:
            return self._delete_session_gallery_pending_capture(int(pending_index))
        try:
            image_id = int(key or 0)
        except Exception:
            image_id = 0
        if image_id <= 0:
            return False
        return self._delete_session_gallery_committed_image(image_id)

    def _delete_session_gallery_pending_capture(self, pending_index: int) -> bool:
        captures = list(getattr(self, "_pending_raw_captures", []) or [])
        if pending_index < 0 or pending_index >= len(captures):
            return False
        capture = captures[pending_index]
        if not isinstance(capture, PendingRawCapture):
            return False

        current_capture = self._current_pending_raw_capture()
        current_index = self._selected_pending_raw_index_value()
        current_committed_id = self._selected_committed_image_id()
        if current_capture is capture and current_index == pending_index:
            return self._discard_selected_pending_raw_capture()

        preview_path = Path(capture.preview_path) if capture.preview_path else None
        capture.status = "discarded"
        if preview_path is not None:
            try:
                preview_path.unlink(missing_ok=True)
            except Exception:
                pass
        captures.pop(pending_index)
        self._pending_raw_captures = captures
        if current_capture is not None and current_index > pending_index:
            current_index -= 1
        if current_capture is not None and captures:
            self._selected_pending_raw_index = max(0, min(current_index, len(captures) - 1))
        elif current_capture is not None:
            self._selected_pending_raw_index = -1

        self._refresh_session_gallery()
        if current_capture is not None and captures:
            self._show_pending_raw_capture(self._selected_pending_raw_index)
        elif current_committed_id is not None and current_committed_id in set(self._session_image_ids or []):
            self._show_session_image(int(current_committed_id))
        elif self._session_image_ids:
            self._show_session_image(int(self._session_image_ids[-1]))
        else:
            self._clear_session_viewer(
                title=self.tr("Waiting for first import"),
                meta=self.tr("New microscope captures from the watched folder will appear here automatically."),
            )

        self._show_status(
            self.tr("Removed pending RAW capture {name}.").format(name=capture.source_path.name),
            tone="info",
            timeout_ms=3500,
        )
        return True

    def _delete_session_gallery_committed_image(self, image_id: int) -> bool:
        image = ImageDB.get_image(image_id)
        image_name = Path(str((image or {}).get("filepath") or "")).name or self.tr("selected image")
        session_image_ids = list(getattr(self, "_session_image_ids", []) or [])
        try:
            image_index = session_image_ids.index(image_id)
        except ValueError:
            image_index = -1

        current_pending_capture = self._current_pending_raw_capture()
        current_pending_index = self._selected_pending_raw_index_value()
        current_committed_id = self._selected_committed_image_id()

        try:
            ImageDB.delete_image(image_id)
        except Exception as exc:
            self._show_status(
                self.tr("Could not delete local processed image {name}: {error}").format(
                    name=image_name,
                    error=str(exc),
                ),
                tone="warning",
                timeout_ms=6000,
            )
            return False

        self._session_image_ids = [existing_id for existing_id in session_image_ids if int(existing_id) != int(image_id)]
        self._refresh_session_gallery()

        if current_pending_capture is not None:
            if current_pending_index >= 0:
                self._show_pending_raw_capture(current_pending_index)
            else:
                self._show_pending_raw_capture(self._selected_pending_raw_index)
        elif current_committed_id is not None and int(current_committed_id) in set(int(item) for item in self._session_image_ids):
            self._selected_session_image_id = int(current_committed_id)
            self._show_session_image(int(current_committed_id))
        elif self._session_image_ids:
            next_index = image_index if image_index >= 0 else 0
            if next_index >= len(self._session_image_ids):
                next_index = len(self._session_image_ids) - 1
            next_image_id = int(self._session_image_ids[next_index])
            self._selected_session_image_id = next_image_id
            self._show_session_image(next_image_id)
        else:
            self._selected_session_image_id = None
            self._clear_session_viewer(
                title=self.tr("Waiting for first import"),
                meta=self.tr("New microscope captures from the watched folder will appear here automatically."),
            )

        self._update_pending_raw_controls()

        observation_id = int(self._session_observation_id or 0)
        if observation_id > 0 and int(getattr(self._main_window, "active_observation_id", 0) or 0) == observation_id:
            try:
                if hasattr(self._main_window, "observations_tab"):
                    self._main_window.observations_tab.refresh_observations(show_status=False)
            except Exception:
                pass
            try:
                self._main_window.refresh_observation_images(select_image_id=self._selected_session_image_id)
                self._main_window.update_measurements_table()
                if getattr(self._main_window, "is_analysis_visible", None) and self._main_window.is_analysis_visible():
                    self._main_window.schedule_gallery_refresh()
            except Exception:
                pass

        self._show_status(
            self.tr("Deleted local processed image {name}.").format(name=image_name),
            tone="success",
            timeout_ms=3500,
        )
        return True

    def _clear_session_viewer(self, title: str | None = None, meta: str | None = None) -> None:
        self.viewer_title_label.setText(title or self.tr("Last import"))
        self.viewer_meta_label.setText(meta or self.tr("The next imported microscope image will appear here."))
        self.reset_view_btn.setEnabled(False)
        self.live_image_label.set_microns_per_pixel(0.0)
        self.live_image_label.set_scale_bar(False, 0.0)
        self.live_image_label.set_image(None)
        update_viewer_objective_tag = getattr(self, "_update_viewer_objective_tag", None)
        if callable(update_viewer_objective_tag):
            update_viewer_objective_tag(None)
        update_raw_processing_visibility = getattr(self, "_update_raw_processing_visibility", None)
        if callable(update_raw_processing_visibility):
            update_raw_processing_visibility()

    def _show_session_image(self, image_id: int) -> None:
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        image = ImageDB.get_image(image_id)
        if not image:
            self._clear_session_viewer()
            self._update_raw_edit_controls()
            _raw_timing_log("show session image", start, detail=f"image_id={image_id} missing")
            return
        self._selected_session_image_id = int(image_id)

        image_path = str(image.get("filepath") or "").strip()
        if not image_path:
            self._clear_session_viewer(
                title=self.tr("Selected image unavailable"),
                meta=self.tr("This session image does not have a stored file path."),
            )
            self._update_raw_edit_controls()
            _raw_timing_log("show session image", start, detail=f"image_id={image_id} missing-path")
            return

        apply_microscope_state = getattr(self, "_apply_microscope_state_to_controls", None)
        if callable(apply_microscope_state):
            apply_microscope_state(image)
        edit_session = getattr(self, "_raw_edit_session", None)
        edit_mode = edit_session is not None and int(edit_session.image_id) == int(image_id)
        preview_scaled = False
        pixmap: QPixmap | None = None
        if edit_mode:
            preview_dir = self._raw_edit_preview_dir()
            preview_dir.mkdir(parents=True, exist_ok=True)
            preview_path_obj = Path(edit_session.preview_path) if edit_session.preview_path else preview_dir / f"{edit_session.source_raw_path.stem}_{edit_session.image_id}.jpg"
            edit_session.preview_path = preview_path_obj
            preview_path = str(preview_path_obj)
            image_path = preview_path
            preview_rgb = getattr(edit_session, "preview_rgb", None)
            if isinstance(preview_rgb, np.ndarray) and preview_rgb.size:
                pixmap = self._rgb_to_pixmap(preview_rgb)
                preview_scaled = True
                self._schedule_raw_edit_preview_save(edit_session)
            elif not preview_path or not Path(preview_path).exists():
                self._clear_session_viewer(
                    title=self.tr("RAW edit preview unavailable"),
                    meta=self.tr("The temporary RAW edit preview for {name} is not available.").format(
                        name=edit_session.source_raw_path.name
                    ),
                )
                self._update_raw_edit_controls()
                _raw_timing_log(
                    "show session image",
                    start,
                    detail=f"{Path(image_path).name} edit={edit_mode} preview-missing",
                )
                return

        if pixmap is None:
            pixmap, preview_scaled = self._load_viewer_pixmap(image_path)
            if pixmap is None or pixmap.isNull():
                self._clear_session_viewer(
                    title=self.tr("Selected image unavailable"),
                    meta=self.tr("Could not load {name}.").format(name=Path(image_path).name),
                )
                self._show_status(
                    self.tr("Could not load {name}.").format(name=Path(image_path).name),
                    tone="warning",
                    timeout_ms=5000,
                )
                self._update_raw_edit_controls()
                _raw_timing_log("show session image", start, detail=f"{Path(image_path).name} load-failed")
                return

        preserve_view = False
        preserve_view_checker = getattr(self, "_should_preserve_live_image_view", None)
        if edit_mode:
            preserve_view = True
        elif callable(preserve_view_checker):
            try:
                preserve_view = bool(preserve_view_checker(image_path))
            except Exception:
                preserve_view = False
        if edit_mode:
            preview_scaled = True
        self.live_image_label.set_image_sources(
            pixmap,
            image_path,
            preview_scaled,
            preserve_view=preserve_view,
        )
        self._prune_pending_raw_preview_buffers(clear_proxy_cache=True)
        self.reset_view_btn.setEnabled(True)

        mpp_value = self._image_microns_per_pixel(image)
        self.live_image_label.set_microns_per_pixel(mpp_value or 0.0)
        if mpp_value:
            scale_bar_value, scale_bar_unit = self._viewer_scale_bar_config(image)
            scale_bar_microns = float(scale_bar_value) * 1000.0 if scale_bar_unit == "mm" else float(scale_bar_value)
            self.live_image_label.set_scale_bar(True, scale_bar_microns, unit=scale_bar_unit)
        else:
            self.live_image_label.set_scale_bar(False, 0.0)

        if edit_mode:
            self.viewer_title_label.setText(
                self.tr("Editing RAW: {name}").format(name=edit_session.source_raw_path.name)
            )
            self.viewer_meta_label.setText(self._raw_settings_info_text(edit_session.working_settings))
            auto_settings = None
            if edit_session.source_raw_path.exists():
                auto_settings = self._raw_auto_level_settings_for_source(
                    edit_session.source_raw_path,
                    edit_session.working_settings,
                )
            self._sync_raw_processing_controls_from_settings(
                edit_session.working_settings,
                update_session_settings=False,
                auto_level_settings=auto_settings,
            )
        else:
            title_prefix = self.tr("Last import")
            if self._session_image_ids and int(image_id) != int(self._session_image_ids[-1]):
                title_prefix = self.tr("Selected image")
            self.viewer_title_label.setText(
                self.tr("{prefix}: {name}").format(
                    prefix=title_prefix,
                    name=Path(image_path).name,
                )
            )
            self.viewer_meta_label.setText(self._viewer_meta_text(image))
            editable_session = self._raw_editable_image_session(image)
            if editable_session is not None:
                auto_settings = None
                if editable_session.source_raw_path.exists():
                    auto_settings = self._raw_auto_level_settings_for_source(
                        editable_session.source_raw_path,
                        editable_session.working_settings,
                    )
                self._sync_raw_processing_controls_from_settings(
                    editable_session.working_settings,
                    update_session_settings=False,
                    auto_level_settings=auto_settings,
                )
        self._update_raw_edit_controls()
        _raw_timing_log(
            "show session image",
            start,
            detail=f"{Path(image_path).name} edit={edit_mode} preserve_view={preserve_view} preview_scaled={preview_scaled}",
        )

    def _load_viewer_pixmap(self, path: str) -> tuple[QPixmap | None, bool]:
        reader = QImageReader(path)
        reader.setAutoTransform(True)
        preview_scaled = False
        size = reader.size()
        if size.isValid():
            max_dim = max(int(size.width() or 0), int(size.height() or 0))
            if max_dim > self.VIEWER_PREVIEW_MAX_DIM:
                scale = float(self.VIEWER_PREVIEW_MAX_DIM) / float(max_dim)
                reader.setScaledSize(
                    QSize(
                        max(1, int(round(size.width() * scale))),
                        max(1, int(round(size.height() * scale))),
                    )
                )
                preview_scaled = True
        image = reader.read()
        if image.isNull():
            pixmap = QPixmap(path)
            return (pixmap if not pixmap.isNull() else None), False
        return QPixmap.fromImage(image), preview_scaled

    def _image_microns_per_pixel(self, image: dict | None) -> float | None:
        if not image:
            return None
        value = image.get("scale_microns_per_pixel")
        try:
            scale = float(value or 0.0)
        except Exception:
            scale = 0.0
        return scale if scale > 0 else None

    def _viewer_scale_bar_config(self, image: dict | None) -> tuple[float, str]:
        image_type = str((image or {}).get("image_type") or "").strip().lower()
        objective_key = str((image or {}).get("objective_name") or "").strip() or None

        observation_fallback = getattr(self._main_window, "_observation_scale_bar_fallback_value", None)
        if callable(observation_fallback):
            try:
                if image_type == "field":
                    return float(observation_fallback(True)), "mm"
                return float(observation_fallback(False, objective_key=objective_key)), "\u03bcm"
            except Exception:
                pass

        objective_fallback = getattr(self._main_window, "_suggest_microscope_scale_bar_um_for_objective", None)
        if callable(objective_fallback):
            try:
                return float(objective_fallback(objective_key)), "\u03bcm"
            except Exception:
                pass

        return float(self.VIEWER_SCALE_BAR_UM), "\u03bcm"

    def _viewer_meta_text(self, image: dict | None) -> str:
        if not image:
            return ""
        objectives = load_objectives()
        parts: list[str] = []
        objective_name = LiveLabTab._objective_key_from_metadata(self, image)
        if objective_name:
            objective = objectives.get(objective_name)
            parts.append(objective_display_name(objective, objective_name) if objective else objective_name)
        contrast = str(image.get("contrast") or "").strip()
        if contrast:
            parts.append(DatabaseTerms.translate("contrast", contrast))
        mount_medium = str(image.get("mount_medium") or "").strip()
        if mount_medium:
            parts.append(DatabaseTerms.translate("mount", mount_medium))
        stain = str(image.get("stain") or "").strip()
        if stain:
            parts.append(DatabaseTerms.translate("stain", stain))
        sample_type = str(image.get("sample_type") or "").strip()
        if sample_type:
            parts.append(DatabaseTerms.translate("sample", sample_type))
        mpp_value = self._image_microns_per_pixel(image)
        if mpp_value:
            parts.append(self.tr("{scale:.4g} \u03bcm/px").format(scale=mpp_value))
        return " \u2022 ".join(parts) if parts else self.tr("Scroll to zoom and drag to pan.")

    def _refresh_main_window_after_import(self, image_id: int) -> None:
        observation_id = int(self._session_observation_id or 0)
        try:
            if hasattr(self._main_window, "observations_tab"):
                self._main_window.observations_tab.refresh_observations(show_status=False)
        except Exception:
            pass
        if int(getattr(self._main_window, "active_observation_id", 0) or 0) != observation_id:
            return
        try:
            self._main_window.refresh_observation_images(select_image_id=image_id)
            self._main_window.update_measurements_table()
            if getattr(self._main_window, "is_analysis_visible", None) and self._main_window.is_analysis_visible():
                self._main_window.schedule_gallery_refresh()
        except Exception:
            pass

    def closeEvent(self, event) -> None:
        self.shutdown()
        super().closeEvent(event)
