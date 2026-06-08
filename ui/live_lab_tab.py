"""Live microscopy session tab."""
from __future__ import annotations

import copy
import json
import os
import tempfile
from dataclasses import dataclass, replace
from datetime import datetime
from uuid import uuid4
from pathlib import Path

import numpy as np

from PySide6.QtCore import QSize, Qt, QUrl, QTimer, QSignalBlocker, QEvent, QPointF
from PySide6.QtGui import (
    QColor,
    QDesktopServices,
    QIcon,
    QImage,
    QImageReader,
    QKeySequence,
    QPainter,
    QPainterPath,
    QPen,
    QPixmap,
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
    SETTING_RAW_COMPANION_SOURCE_PREFERENCE,
)
from database.database_tags import DatabaseTerms
from database.models import CalibrationDB, ImageDB, ObservationDB, SessionLogDB, SettingsDB
from database.schema import get_images_dir, load_objectives, objective_display_name, objective_sort_value
from utils.image_utils import cleanup_import_temp_file
from utils.image_companion_grouping import (
    companion_group_key,
    normalize_raw_companion_source_preference,
    select_preferred_companion_path,
)
from utils.image_processing_pipeline import (
    ProcessingDebugInfo,
    compute_post_decode_transfer_curve,
    raw_basic_controls_from_settings,
    raw_settings_from_basic_controls,
)
from utils.local_image_ingest import RawRenderingUnavailableError, prepare_local_ingest_image
from utils.lab_watcher import LabWatcherWorker
from utils.raw_detection import SUPPORTED_RAW_SUFFIXES, is_raw_image_path
from utils.raw_render import (
    RawRenderSettings,
    build_raw_processing_metadata,
    render_raw_image,
    render_raw_preview,
)
from utils.raw_white_balance import estimate_white_balance_from_background
from utils.thumbnail_generator import generate_all_sizes, get_thumbnail_path

from .hint_status import HintBar, HintStatusController
from .image_gallery_widget import ImageGalleryWidget
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


@dataclass(slots=True)
class PendingRawCapture:
    """Session-local RAW capture waiting for review and commit."""

    source_path: Path
    companion_jpeg_path: Path | None
    lab_metadata: dict[str, object]
    raw_settings: RawRenderSettings
    preview_path: Path | None = None
    wb_sample_base_preview_path: Path | None = None
    wb_sample_base_pixmap: QPixmap | None = None
    status: str = "pending"
    group_key: str | None = None
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
    wb_sample_base_preview_path: Path | None = None
    wb_sample_base_pixmap: QPixmap | None = None
    dirty: bool = False


class RawCurvePreviewWidget(QWidget):
    """Tiny QPainter-only preview of the current simplified RAW transfer."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._settings = RawRenderSettings.default()
        self._debug: ProcessingDebugInfo | None = None
        self._curve = None
        self.setMinimumSize(120, 68)
        self.setMaximumHeight(76)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.setToolTip("Curve preview")

    @property
    def current_curve(self):
        return self._curve

    def set_curve_state(
        self,
        settings: RawRenderSettings | None,
        *,
        debug: ProcessingDebugInfo | None = None,
    ) -> None:
        self._settings = RawRenderSettings.from_dict(settings or RawRenderSettings.default())
        self._debug = debug if isinstance(debug, ProcessingDebugInfo) else None
        self._curve = self._build_curve()
        self.update()

    def _build_curve(self):
        debug = self._debug
        if debug is None:
            debug = ProcessingDebugInfo(
                input_min=0.0,
                input_max=1.0,
                black_level=0.20,
                white_level=0.80,
                settings=self._settings.to_dict(),
            )
        return compute_post_decode_transfer_curve(
            np.zeros((1, 1, 3), dtype=np.float64),
            self._settings,
            debug=debug,
        )

    def paintEvent(self, event) -> None:  # type: ignore[override]
        super().paintEvent(event)
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing, True)
        painter.fillRect(self.rect(), QColor("#fbfbfc"))

        outer = self.rect().adjusted(1, 1, -1, -1)
        painter.setPen(QPen(QColor("#d1d5db"), 1))
        painter.drawRoundedRect(outer, 6, 6)

        curve = self._curve
        if curve is None:
            return

        plot = outer.adjusted(6, 6, -6, -6)
        if plot.width() <= 1 or plot.height() <= 1:
            return

        def _map_point(x: float, y: float) -> QPointF:
            px = plot.left() + float(np.clip(x, 0.0, 1.0)) * plot.width()
            py = plot.bottom() - float(np.clip(y, 0.0, 1.0)) * plot.height()
            return QPointF(px, py)

        painter.setPen(QPen(QColor("#d1d5db"), 1, Qt.DashLine))
        painter.drawLine(_map_point(0.0, 0.0), _map_point(1.0, 1.0))

        black_level = getattr(curve.debug, "black_level", None)
        white_level = getattr(curve.debug, "white_level", None)
        if black_level is not None and np.isfinite(float(black_level)):
            x = plot.left() + float(np.clip(float(black_level), 0.0, 1.0)) * plot.width()
            painter.setPen(QPen(QColor(44, 62, 80, 100), 1, Qt.DotLine))
            painter.drawLine(QPointF(x, plot.top()), QPointF(x, plot.bottom()))
        if white_level is not None and np.isfinite(float(white_level)):
            x = plot.left() + float(np.clip(float(white_level), 0.0, 1.0)) * plot.width()
            painter.setPen(QPen(QColor(192, 57, 43, 100), 1, Qt.DotLine))
            painter.drawLine(QPointF(x, plot.top()), QPointF(x, plot.bottom()))

        path = QPainterPath()
        first_point = True
        for x_value, y_value in zip(curve.input_values, curve.final_output, strict=False):
            point = _map_point(float(x_value), float(y_value))
            if first_point:
                path.moveTo(point)
                first_point = False
            else:
                path.lineTo(point)
        painter.setPen(QPen(QColor("#2563eb"), 2))
        painter.drawPath(path)


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
    VIEWER_PREVIEW_MAX_DIM = 2400
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
        self._raw_edit_session: RawEditSession | None = None
        self._raw_edit_background_wb_armed = False
        self._raw_edit_preview_timer = QTimer(self)
        self._raw_edit_preview_timer.setSingleShot(True)
        self._raw_edit_preview_timer.timeout.connect(self._refresh_raw_edit_preview)
        self._pending_raw_background_wb_armed = False
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
        current_text_layout.addStretch(1)
        self.change_observation_btn = QPushButton(self.tr("Change observation"))
        self.change_observation_btn.clicked.connect(self._open_observations_tab)
        current_text_layout.addWidget(self.change_observation_btn, 0, Qt.AlignLeft)
        current_row.addLayout(current_text_layout, 1)
        current_layout.addLayout(current_row)
        left_layout.addWidget(current_group)

        self.start_stop_btn = QPushButton(self.tr("Start Session"))
        self.start_stop_btn.setMinimumHeight(36)
        self.start_stop_btn.setStyleSheet(self.SESSION_BUTTON_BASE_STYLE)
        self.start_stop_btn.clicked.connect(self._toggle_session)
        left_layout.addWidget(self.start_stop_btn)

        self.session_status_label = QLabel("")
        self.session_status_label.setWordWrap(True)
        self.session_status_label.setStyleSheet("color: #4b5563;")
        left_layout.addWidget(self.session_status_label)

        self.session_count_label = QLabel(self.tr("Imported this session: 0"))
        self.session_count_label.setWordWrap(True)
        self.session_count_label.setStyleSheet("color: #6b7280;")
        left_layout.addWidget(self.session_count_label)

        mode_group, mode_layout = create_section_card(
            self.tr("Capture mode"),
            body_margins=(10, 12, 10, 10),
        )
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
        mode_layout.addWidget(self.session_mode_selector)
        left_layout.addWidget(mode_group)

        watch_group, watch_layout = create_section_card(
            self.tr("Watched folder"),
            body_margins=(10, 12, 10, 10),
        )
        self.watch_group = watch_group
        self.watch_dir_input = QLineEdit()
        self.watch_dir_input.setPlaceholderText(self.tr("Choose the microscope capture folder"))
        self.watch_dir_input.textChanged.connect(self._on_watch_dir_changed)
        watch_layout.addWidget(self.watch_dir_input)
        watch_buttons = QHBoxLayout()
        watch_buttons.setContentsMargins(0, 0, 0, 0)
        watch_buttons.setSpacing(8)
        self.browse_btn = QPushButton(self.tr("Browse"))
        self.browse_btn.clicked.connect(self._choose_watch_dir)
        watch_buttons.addWidget(self.browse_btn)
        self.open_folder_btn = QPushButton(self.tr("Open folder"))
        self.open_folder_btn.clicked.connect(self._open_watch_dir)
        watch_buttons.addWidget(self.open_folder_btn)
        watch_layout.addLayout(watch_buttons)
        left_layout.addWidget(watch_group)

        tag_group, tag_form = create_section_card(
            self.tr("Current Lab State"),
            QFormLayout,
            body_margins=(10, 12, 10, 10),
        )
        tag_form.setSpacing(8)
        self.objective_combo = self._make_combo()
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
        raw_body_layout.setContentsMargins(12, 0, 12, 12)
        raw_body_layout.setSpacing(8)
        self.raw_processing_details_label = QLabel(
            self.tr("Applies to future RAW captures in this Live Lab session.")
        )
        self.raw_processing_details_label.setWordWrap(True)
        self.raw_processing_details_label.setStyleSheet("color: #6b7280;")
        raw_body_layout.addWidget(self.raw_processing_details_label)

        self.raw_capture_mode_selector = SegmentedSelector(self, compact=True)
        self.raw_capture_mode_selector.add_option(
            self.tr("Auto-save RAW captures"),
            self.RAW_CAPTURE_MODE_AUTO_SAVE,
            checked=True,
        )
        self.raw_capture_mode_selector.add_option(
            self.tr("Review RAW before saving"),
            self.RAW_CAPTURE_MODE_REVIEW,
        )
        self.raw_capture_mode_selector.selectionChanged.connect(self._on_raw_capture_mode_changed)

        raw_form = QFormLayout()
        raw_form.setContentsMargins(0, 0, 0, 0)
        raw_form.setHorizontalSpacing(8)
        raw_form.setVerticalSpacing(8)
        raw_form.setLabelAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        raw_form.addRow(self.tr("Capture mode:"), self.raw_capture_mode_selector)

        self.raw_companion_source_selector = SegmentedSelector(self, compact=True)
        self.raw_companion_source_selector.add_option(
            self.tr("Prefer RAW"),
            RAW_COMPANION_SOURCE_PREFERENCE_PREFER_RAW,
            checked=True,
        )
        self.raw_companion_source_selector.add_option(
            self.tr("Use camera JPEG"),
            RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG,
        )
        self.raw_companion_source_selector.selectionChanged.connect(
            self._on_raw_companion_source_preference_changed
        )
        raw_form.addRow(self.tr("Companion source:"), self.raw_companion_source_selector)

        self.raw_white_balance_combo = QComboBox()
        self.raw_white_balance_combo.addItem(self.tr("Camera WB"), "camera")
        self.raw_white_balance_combo.addItem(self.tr("Auto WB"), "auto")
        self.raw_white_balance_combo.addItem(self.tr("Custom WB"), "custom")
        self.raw_white_balance_combo.currentIndexChanged.connect(self._on_raw_processing_controls_changed)
        raw_form.addRow(self.tr("White balance:"), self.raw_white_balance_combo)

        self.raw_auto_levels_checkbox = QCheckBox(self.tr("Auto levels"))
        self.raw_auto_levels_checkbox.toggled.connect(self._on_raw_processing_controls_changed)
        raw_form.addRow(self.tr("Levels:"), self.raw_auto_levels_checkbox)

        self.raw_tone_curve_checkbox = QCheckBox(self.tr("Tone curve"))
        self.raw_tone_curve_checkbox.toggled.connect(self._on_raw_processing_controls_changed)
        raw_form.addRow(self.tr("Tone curve:"), self.raw_tone_curve_checkbox)

        self.raw_curve_strength_row = QWidget()
        raw_strength_row_layout = QHBoxLayout(self.raw_curve_strength_row)
        raw_strength_row_layout.setContentsMargins(0, 0, 0, 0)
        raw_strength_row_layout.setSpacing(8)
        self.raw_curve_strength_slider = QSlider(Qt.Horizontal)
        self.raw_curve_strength_slider.setRange(0, 100)
        self.raw_curve_strength_slider.setSingleStep(1)
        self.raw_curve_strength_slider.setPageStep(5)
        self.raw_curve_strength_slider.setValue(int(round(self._raw_render_settings.tone_curve_strength * 100.0)))
        self.raw_curve_strength_slider.valueChanged.connect(self._on_raw_processing_controls_changed)
        self.raw_curve_strength_value_label = QLabel("")
        self.raw_curve_strength_value_label.setMinimumWidth(28)
        self.raw_curve_strength_value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        raw_strength_row_layout.addWidget(self.raw_curve_strength_slider, 1)
        raw_strength_row_layout.addWidget(self.raw_curve_strength_value_label, 0)
        raw_form.addRow(self.tr("Curve strength:"), self.raw_curve_strength_row)

        self.raw_curve_midpoint_row = QWidget()
        raw_midpoint_row_layout = QHBoxLayout(self.raw_curve_midpoint_row)
        raw_midpoint_row_layout.setContentsMargins(0, 0, 0, 0)
        raw_midpoint_row_layout.setSpacing(8)
        self.raw_curve_midpoint_slider = QSlider(Qt.Horizontal)
        self.raw_curve_midpoint_slider.setRange(0, 100)
        self.raw_curve_midpoint_slider.setSingleStep(1)
        self.raw_curve_midpoint_slider.setPageStep(5)
        self.raw_curve_midpoint_slider.setValue(int(round(self._raw_render_settings.tone_curve_midpoint * 100.0)))
        self.raw_curve_midpoint_slider.valueChanged.connect(self._on_raw_processing_controls_changed)
        self.raw_curve_midpoint_value_label = QLabel("")
        self.raw_curve_midpoint_value_label.setMinimumWidth(28)
        self.raw_curve_midpoint_value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        raw_midpoint_row_layout.addWidget(self.raw_curve_midpoint_slider, 1)
        raw_midpoint_row_layout.addWidget(self.raw_curve_midpoint_value_label, 0)
        raw_form.addRow(self.tr("Curve midpoint:"), self.raw_curve_midpoint_row)

        raw_body_layout.addLayout(raw_form)

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
        self.pending_raw_apply_all_btn = QPushButton(self.tr("Apply settings to all pending"))
        self.pending_raw_apply_all_btn.clicked.connect(self._apply_current_raw_settings_to_all_pending)
        pending_raw_top_row_layout.addWidget(self.pending_raw_apply_all_btn)
        self.pending_raw_pick_wb_btn = QPushButton(self.tr("Pick background WB"))
        self.pending_raw_pick_wb_btn.setCheckable(True)
        self.pending_raw_pick_wb_btn.toggled.connect(self._toggle_pending_raw_background_wb_pick)
        pending_raw_top_row_layout.addWidget(self.pending_raw_pick_wb_btn)

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

        self.raw_edit_frame = QFrame()
        self.raw_edit_frame.setObjectName("sectionCard")
        self.raw_edit_frame.setFrameShape(QFrame.NoFrame)
        self.raw_edit_frame.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        raw_edit_layout = QVBoxLayout(self.raw_edit_frame)
        raw_edit_layout.setContentsMargins(12, 8, 12, 8)
        raw_edit_layout.setSpacing(8)

        self.raw_edit_summary_label = QLabel("")
        self.raw_edit_summary_label.setWordWrap(True)
        self.raw_edit_summary_label.setStyleSheet("color: #6b7280;")
        raw_edit_layout.addWidget(self.raw_edit_summary_label)

        self.raw_edit_note_label = QLabel(
            self.tr("Re-render the selected RAW-backed image in place. The original RAW source stays untouched.")
        )
        self.raw_edit_note_label.setWordWrap(True)
        self.raw_edit_note_label.setStyleSheet("color: #6b7280;")
        raw_edit_layout.addWidget(self.raw_edit_note_label)

        raw_edit_button_row = QWidget()
        raw_edit_button_layout = QGridLayout(raw_edit_button_row)
        raw_edit_button_layout.setContentsMargins(0, 0, 0, 0)
        raw_edit_button_layout.setHorizontalSpacing(8)
        raw_edit_button_layout.setVerticalSpacing(8)

        self.raw_edit_open_btn = QPushButton(self.tr("Edit RAW settings"))
        self.raw_edit_open_btn.clicked.connect(self._begin_raw_edit_for_selected_image)
        raw_edit_button_layout.addWidget(self.raw_edit_open_btn, 0, 0)

        self.raw_edit_use_copied_btn = QPushButton(self.tr("Use copied RAW settings"))
        self.raw_edit_use_copied_btn.clicked.connect(self._begin_raw_edit_with_copied_settings)
        raw_edit_button_layout.addWidget(self.raw_edit_use_copied_btn, 0, 1)

        self.raw_edit_apply_btn = QPushButton(self.tr("Apply re-render"))
        self.raw_edit_apply_btn.clicked.connect(self._apply_raw_edit_session)
        raw_edit_button_layout.addWidget(self.raw_edit_apply_btn, 0, 2)

        self.raw_edit_copy_btn = QPushButton(self.tr("Copy settings"))
        self.raw_edit_copy_btn.clicked.connect(self._copy_raw_edit_settings)
        raw_edit_button_layout.addWidget(self.raw_edit_copy_btn, 1, 0)

        self.raw_edit_pick_wb_btn = QPushButton(self.tr("Pick background WB"))
        self.raw_edit_pick_wb_btn.setCheckable(True)
        self.raw_edit_pick_wb_btn.toggled.connect(
            lambda checked: self._toggle_raw_background_wb_pick(checked, target="edit")
        )
        raw_edit_button_layout.addWidget(self.raw_edit_pick_wb_btn, 1, 1)

        self.raw_edit_cancel_btn = QPushButton(self.tr("Cancel"))
        self.raw_edit_cancel_btn.clicked.connect(self._cancel_raw_edit_session)
        raw_edit_button_layout.addWidget(self.raw_edit_cancel_btn, 1, 2)

        raw_edit_layout.addWidget(raw_edit_button_row)
        self.raw_edit_frame.setVisible(False)
        viewer_layout.addWidget(self.raw_edit_frame)

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
            show_delete=False,
            show_badges=True,
            thumbnail_size=132,
            default_height=GALLERY_DEFAULT_HEIGHT,
            min_height=GALLERY_MIN_HEIGHT,
        )
        self.session_gallery.imageClicked.connect(self._on_session_gallery_clicked)

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
            self.change_observation_btn,
            self.tr("Switch back to Observations to choose a different current observation."),
            disabled_hint=self.tr("Stop the current Live Lab session before changing observation."),
        )
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
            self.open_folder_btn,
            self.tr("Open the watched folder in Finder."),
            disabled_hint=self.tr("Choose an existing watched folder first."),
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
            self.tr("Show or hide the RAW processing controls for future session-level captures."),
        )
        self._register_hint_widget(
            self.raw_capture_mode_selector,
            self.tr("Choose whether RAW captures are committed immediately or held for review."),
        )
        self._register_hint_widget(
            self.raw_companion_source_selector,
            self.tr("Choose whether RAW files or the companion camera JPEG should win when both exist."),
        )
        self._register_hint_widget(
            self.raw_white_balance_combo,
            self.tr("Choose the default white balance mode for future RAW captures."),
        )
        self._register_hint_widget(
            self.raw_auto_levels_checkbox,
            self.tr("Apply automatic levels to future RAW captures."),
        )
        self._register_hint_widget(
            self.raw_tone_curve_checkbox,
            self.tr("Enable the luminance tone curve for future RAW captures."),
        )
        self._register_hint_widget(
            self.raw_curve_strength_slider,
            self.tr("Adjust the tone curve strength."),
        )
        self._register_hint_widget(
            self.raw_curve_midpoint_slider,
            self.tr("Adjust the tone curve midpoint."),
        )
        self._register_hint_widget(
            self.raw_edit_open_btn,
            self.tr("Open the selected RAW-backed image for in-place re-rendering."),
            disabled_hint=self.tr("Select a committed RAW-backed session image first."),
        )
        self._register_hint_widget(
            self.raw_edit_use_copied_btn,
            self.tr("Start editing the selected RAW-backed image using the copied RAW settings."),
            disabled_hint=self.tr("Copy RAW settings first, then select a compatible RAW-backed image."),
        )
        self._register_hint_widget(
            self.raw_edit_apply_btn,
            self.tr("Render the selected RAW-backed image again using the current settings."),
            disabled_hint=self.tr("Start a RAW edit session first."),
        )
        self._register_hint_widget(
            self.raw_edit_copy_btn,
            self.tr("Copy the current RAW settings so they can be reused on another RAW-backed image."),
            disabled_hint=self.tr("Select or edit a RAW-backed image first."),
        )
        self._register_hint_widget(
            self.raw_edit_pick_wb_btn,
            self.tr("Click a neutral background point in the current RAW edit preview."),
            disabled_hint=self.tr("Start a RAW edit session with a preview first."),
        )
        self._register_hint_widget(
            self.raw_edit_cancel_btn,
            self.tr("Cancel the current RAW edit session without changing the committed image."),
            disabled_hint=self.tr("Start a RAW edit session first."),
        )
        self._register_hint_widget(
            self.pending_raw_save_btn,
            self.tr("Commit the selected pending RAW capture using the current RAW settings."),
            disabled_hint=self.tr("Choose a pending RAW capture first."),
        )
        self._register_hint_widget(
            self.pending_raw_apply_all_btn,
            self.tr("Update all pending RAW captures to use the current RAW settings."),
            disabled_hint=self.tr("No pending RAW captures are waiting to be reviewed."),
        )
        self._register_hint_widget(
            self.pending_raw_pick_wb_btn,
            self.tr("Click a neutral background point in the current pending RAW preview."),
            disabled_hint=self.tr("Choose a pending RAW capture with a preview first."),
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
        objective_key = str(self.objective_combo.currentData() or "").strip() or None
        objective_label = str(self.objective_combo.currentText() or "").strip() or None
        return {
            "capture_source": "live_lab",
            "instrument": "microscope",
            "objective_name": objective_key,
            "objective_label": objective_label,
            "contrast": DatabaseTerms.canonicalize("contrast", self.contrast_combo.currentData()),
            "mount_medium": DatabaseTerms.canonicalize("mount", self.mount_combo.currentData()),
            "stain": DatabaseTerms.canonicalize("stain", self.stain_combo.currentData()),
            "sample_type": DatabaseTerms.canonicalize("sample", self.sample_combo.currentData()),
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

    def _raw_settings_from_controls(self, *, update_session_settings: bool = True) -> RawRenderSettings:
        base_settings = getattr(self, "_raw_render_settings", RawRenderSettings.default())
        white_balance_combo = getattr(self, "raw_white_balance_combo", None)
        auto_levels_checkbox = getattr(self, "raw_auto_levels_checkbox", None)
        tone_curve_checkbox = getattr(self, "raw_tone_curve_checkbox", None)
        strength_slider = getattr(self, "raw_curve_strength_slider", None)
        midpoint_slider = getattr(self, "raw_curve_midpoint_slider", None)

        white_balance_mode = "camera"
        if white_balance_combo is not None:
            white_balance_mode = str(white_balance_combo.currentData() or "camera").strip().lower() or "camera"

        auto_levels = True
        if auto_levels_checkbox is not None:
            auto_levels = bool(auto_levels_checkbox.isChecked())

        tone_curve_enabled = False
        if tone_curve_checkbox is not None:
            tone_curve_enabled = bool(tone_curve_checkbox.isChecked())

        tone_curve_strength = float(getattr(base_settings, "tone_curve_strength", 0.5))
        if strength_slider is not None:
            tone_curve_strength = max(0.0, min(1.0, float(strength_slider.value()) / 100.0))

        tone_curve_midpoint = float(getattr(base_settings, "tone_curve_midpoint", 0.5))
        if midpoint_slider is not None:
            tone_curve_midpoint = max(0.0, min(1.0, float(midpoint_slider.value()) / 100.0))

        settings = RawRenderSettings(
            white_balance_mode=white_balance_mode if white_balance_mode in {"camera", "auto", "custom"} else "camera",
            auto_levels=auto_levels,
            tone_curve_enabled=tone_curve_enabled,
            tone_curve_strength=tone_curve_strength,
            tone_curve_midpoint=tone_curve_midpoint,
        )
        if update_session_settings:
            self._raw_render_settings = settings
        return settings

    def _sync_raw_processing_controls_from_settings(
        self,
        settings: RawRenderSettings | None = None,
        *,
        update_session_settings: bool = True,
    ) -> None:
        settings = RawRenderSettings.from_dict(settings or getattr(self, "_raw_render_settings", None))
        if update_session_settings:
            self._raw_render_settings = settings
        combo = getattr(self, "raw_white_balance_combo", None)
        auto_levels_checkbox = getattr(self, "raw_auto_levels_checkbox", None)
        tone_curve_checkbox = getattr(self, "raw_tone_curve_checkbox", None)
        strength_slider = getattr(self, "raw_curve_strength_slider", None)
        midpoint_slider = getattr(self, "raw_curve_midpoint_slider", None)
        strength_label = getattr(self, "raw_curve_strength_value_label", None)
        midpoint_label = getattr(self, "raw_curve_midpoint_value_label", None)

        if combo is not None:
            target_index = combo.findData(settings.white_balance_mode)
            if target_index < 0:
                target_index = combo.findData("camera")
            if target_index >= 0:
                with QSignalBlocker(combo):
                    combo.setCurrentIndex(target_index)
        if auto_levels_checkbox is not None:
            with QSignalBlocker(auto_levels_checkbox):
                auto_levels_checkbox.setChecked(bool(settings.auto_levels))
        if tone_curve_checkbox is not None:
            with QSignalBlocker(tone_curve_checkbox):
                tone_curve_checkbox.setChecked(bool(settings.tone_curve_enabled))
        if strength_slider is not None:
            with QSignalBlocker(strength_slider):
                strength_slider.setValue(int(round(float(settings.tone_curve_strength) * 100.0)))
        if midpoint_slider is not None:
            with QSignalBlocker(midpoint_slider):
                midpoint_slider.setValue(int(round(float(settings.tone_curve_midpoint) * 100.0)))

        if strength_label is not None:
            strength_label.setText(str(int(round(float(settings.tone_curve_strength) * 100.0))))
        if midpoint_label is not None:
            midpoint_label.setText(str(int(round(float(settings.tone_curve_midpoint) * 100.0))))

        self._set_raw_tone_controls_enabled(bool(settings.tone_curve_enabled))
        self._update_raw_processing_section_label(bool(getattr(self, "raw_processing_toggle_btn", None) and self.raw_processing_toggle_btn.isChecked()))

    def _set_raw_tone_controls_enabled(self, enabled: bool) -> None:
        for attr in (
            "raw_curve_strength_row",
            "raw_curve_midpoint_row",
            "raw_curve_strength_slider",
            "raw_curve_midpoint_slider",
            "raw_curve_strength_value_label",
            "raw_curve_midpoint_value_label",
        ):
            widget = getattr(self, attr, None)
            if widget is not None:
                widget.setEnabled(bool(enabled))

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

    def _raw_processing_summary_text(self) -> str:
        return self._raw_settings_summary_text(self._raw_settings_from_controls(update_session_settings=False))

    def _update_raw_processing_section_label(self, expanded: bool) -> None:
        if not hasattr(self, "raw_processing_toggle_btn"):
            return
        arrow = "▾" if expanded else "▸"
        summary = self._raw_processing_summary_text()
        self.raw_processing_toggle_btn.setText(
            self.tr("RAW processing {arrow} {summary}").format(arrow=arrow, summary=summary)
        )

    def _on_raw_processing_toggle_changed(self, checked: bool) -> None:
        if hasattr(self, "raw_processing_body"):
            self.raw_processing_body.setVisible(bool(checked))
        self._update_raw_processing_section_label(bool(checked))

    def _on_raw_processing_controls_changed(self, *_args) -> None:
        editing_session = getattr(self, "_raw_edit_session", None)
        settings = self._raw_settings_from_controls(update_session_settings=editing_session is None)
        selected_capture = None if editing_session is not None else self._current_pending_raw_capture()

        def _apply_current_wb_fields(base_settings: RawRenderSettings, resolved_settings: RawRenderSettings) -> RawRenderSettings:
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

        if selected_capture is not None and self._normalize_raw_capture_mode(self._selected_raw_capture_mode()) == self.RAW_CAPTURE_MODE_REVIEW:
            current_settings = RawRenderSettings.from_dict(selected_capture.raw_settings)
            settings = _apply_current_wb_fields(current_settings, settings)
        if editing_session is not None:
            current_edit_settings = RawRenderSettings.from_dict(editing_session.working_settings)
            settings = _apply_current_wb_fields(current_edit_settings, settings)
            editing_session.working_settings = settings
            editing_session.dirty = settings != editing_session.original_settings
            self._schedule_raw_edit_preview_refresh()
            self._update_raw_edit_controls()
        else:
            save_helper = getattr(self, "_save_raw_processing_settings_for_current_context", None)
            if callable(save_helper):
                save_helper(settings)
            else:
                LiveLabTab._save_raw_processing_settings_for_current_context(self, settings)
            if selected_capture is not None and self._normalize_raw_capture_mode(self._selected_raw_capture_mode()) == self.RAW_CAPTURE_MODE_REVIEW:
                selected_capture.raw_settings = settings
                self._schedule_pending_raw_preview_refresh()
        self._set_raw_tone_controls_enabled(bool(settings.tone_curve_enabled))
        self._update_pending_raw_controls()
        self._update_raw_edit_controls()
        self._update_raw_processing_section_label(bool(getattr(self, "raw_processing_toggle_btn", None) and self.raw_processing_toggle_btn.isChecked()))

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
            return self.tr("{readout} · ←/→ select · Delete discard · Enter save").format(readout=readout)
        return self.tr("←/→ select · Delete discard · Enter save")

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
                summary=self._raw_settings_summary_text(session.working_settings),
            )
            if session.dirty:
                summary_text = f"{summary_text} {self.tr('· modified')}"
        elif editable_session is not None:
            summary_text = self.tr("RAW-backed image: {name} · {summary}").format(
                name=Path(str(editable_session.current_derivative_path)).name,
                summary=self._raw_settings_summary_text(editable_session.original_settings),
            )
            if has_copied_settings:
                summary_text = f"{summary_text} {self.tr('· copied settings available')}"
        elif selected_image is not None and frame is not None:
            summary_text = self.tr("Select a RAW-backed image to re-render it in place.")

        if summary_label is not None:
            summary_label.setText(summary_text)

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
        self._sync_raw_processing_controls_from_settings(
            editable_session.working_settings,
            update_session_settings=False,
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
            timer.start(200)

    def _refresh_raw_edit_preview(self) -> bool:
        session = getattr(self, "_raw_edit_session", None)
        if session is None:
            return False

        preview_dir = self._raw_edit_preview_dir()
        preview_dir.mkdir(parents=True, exist_ok=True)
        preview_path = session.preview_path
        if preview_path is None:
            preview_path = preview_dir / f"{session.source_raw_path.stem}_{session.image_id}.jpg"
        try:
            rendered_path = render_raw_preview(
                session.source_raw_path,
                settings=session.working_settings,
                output_path=preview_path,
                output_dir=preview_dir,
                source_capture_datetime=session.source_capture_datetime,
            )
            session.preview_path = Path(rendered_path)
            session.dirty = session.working_settings != session.original_settings
            self._update_raw_edit_controls()
            if self._selected_committed_image_id() == session.image_id:
                self._show_session_image(session.image_id)
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
                            summary=self._raw_settings_summary_text(self._raw_edit_session.working_settings),
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

        armed, _button = self._raw_background_wb_selection_state(target)
        if not armed:
            return False

        if target == "edit":
            session = getattr(self, "_raw_edit_session", None)
            if session is None:
                return False
            session.working_settings = updated_settings
            session.dirty = updated_settings != session.original_settings
            self._cancel_raw_background_wb_selection(target=target)
            self._sync_raw_processing_controls_from_settings(updated_settings, update_session_settings=False)
            self._schedule_raw_edit_preview_refresh()
            self._update_raw_edit_controls()
            self._show_status(
                self._raw_white_balance_readout_text(updated_settings),
                tone="success",
                timeout_ms=3000,
            )
            return True

        capture = self._current_pending_raw_capture()
        if capture is None:
            self._show_status(
                self.tr("Choose a pending RAW capture before sampling background WB."),
                tone="warning",
                timeout_ms=4000,
            )
            return False
        capture.raw_settings = updated_settings
        self._cancel_raw_background_wb_selection(target=target)
        self._sync_raw_processing_controls_from_settings(updated_settings, update_session_settings=True)
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

        pixmap = self._raw_background_wb_sampling_pixmap(target)
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
        try:
            multipliers = estimate_white_balance_from_background(rgb, rect=(x1, y1, width, height))
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
            (float(x1), float(y1), float(width), float(height)),
            target=target,
            sample_size=None,
        )

    def _apply_raw_background_wb_selection_from_point(self, point: QPointF, *, target: str = "pending") -> bool:
        armed, _button = self._raw_background_wb_selection_state(target)
        if not armed:
            return False

        pixmap = self._raw_background_wb_sampling_pixmap(target)
        rgb = self._pixmap_rgb_array(pixmap)
        if rgb is None:
            self._show_status(
                self.tr("The stable preview is not available for background WB sampling."),
                tone="warning",
                timeout_ms=4000,
            )
            return False

        sample_rect = self._raw_background_wb_sample_rect_from_point(
            point,
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
            sample_rect,
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
        if not self.is_session_running():
            return False
        if self._current_pending_raw_capture() is None:
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
            self._discard_selected_pending_raw_capture()
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
        return self._normalize_raw_capture_mode(getattr(self, "_raw_capture_mode", self.RAW_CAPTURE_MODE_AUTO_SAVE))

    def _raw_capture_mode_label(self, mode: str | None = None, *, short: bool = False) -> str:
        normalized = self._normalize_raw_capture_mode(mode or self._selected_raw_capture_mode())
        if normalized == self.RAW_CAPTURE_MODE_REVIEW:
            return self.tr("Review") if short else self.tr("Review RAW before saving")
        return self.tr("Auto-save") if short else self.tr("Auto-save RAW captures")

    def _restore_raw_capture_mode(self) -> None:
        saved = self._normalize_raw_capture_mode(
            SettingsDB.get_setting(self.SETTING_RAW_CAPTURE_MODE, self.RAW_CAPTURE_MODE_AUTO_SAVE)
        )
        self._raw_capture_mode = saved
        selector = getattr(self, "raw_capture_mode_selector", None)
        if selector is not None:
            selector.set_selected_value(saved)
        self._update_pending_raw_controls()
        self._update_raw_processing_section_label(bool(getattr(self, "raw_processing_toggle_btn", None) and self.raw_processing_toggle_btn.isChecked()))

    def _save_raw_capture_mode(self, mode: str | None = None) -> None:
        normalized = self._normalize_raw_capture_mode(mode or self._selected_raw_capture_mode())
        self._raw_capture_mode = normalized
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
            item = {
                "id": self._pending_raw_gallery_key(capture),
                "filepath": str(capture.source_path),
                "preview_path": preview_path,
                "image_number": f"P{index + 1}",
                "badges": badges,
                "frame_border_color": "#e74c3c" if capture.status == "failed" else "#e67e22",
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
            objective_name = image.get("objective_name")
            objective_label = None
            if objective_name:
                objective_obj = objectives.get(str(objective_name))
                objective_label = (
                    objective_display_name(objective_obj, str(objective_name))
                    if objective_obj
                    else str(objective_name)
                )
            badges = ImageGalleryWidget.build_image_type_badges(
                image_type=image.get("image_type"),
                objective_name=objective_label,
                contrast=image.get("contrast"),
                scale_microns_per_pixel=image.get("scale_microns_per_pixel"),
                custom_scale=bool(str(image.get("objective_name") or "").strip().lower() == "custom"),
                needs_scale=(
                    str(image.get("image_type") or "").strip().lower() == "microscope"
                    and not image.get("objective_name")
                    and not image.get("scale_microns_per_pixel")
                ),
                resize_to_optimal=bool(
                    isinstance(image.get("resample_scale_factor"), (int, float))
                    and image.get("resample_scale_factor") is not None
                    and float(image.get("resample_scale_factor")) < 0.999
                ),
                translate=self.tr,
            )
            badges.extend(ImageGalleryWidget.build_raw_source_badges(image.get("lab_metadata"), translate=self.tr))
            items.append(
                {
                    "id": image_id,
                    "filepath": image.get("filepath"),
                    "image_number": idx,
                    "has_measurements": False,
                    "badges": badges,
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

    def _show_pending_raw_capture(self, index: int | None = None) -> None:
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
        self._sync_raw_processing_controls_from_settings(capture.raw_settings)
        preview_path = str(capture.preview_path or "").strip()
        if not preview_path or not Path(preview_path).exists():
            self._clear_session_viewer(
                title=self.tr("Pending RAW preview unavailable"),
                meta=self.tr("The temporary preview for {name} is not available.").format(name=capture.source_path.name),
            )
            self._update_pending_raw_controls()
            return
        pixmap, preview_scaled = self._load_viewer_pixmap(preview_path)
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

        self.live_image_label.set_image_sources(pixmap, preview_path, preview_scaled)
        self.reset_view_btn.setEnabled(True)
        self.live_image_label.set_microns_per_pixel(0.0)
        self.live_image_label.set_scale_bar(False, 0.0)
        self.viewer_title_label.setText(
            self.tr("Pending RAW {current} of {total}: {name}").format(
                current=self._selected_pending_raw_index_value() + 1,
                total=len(captures),
                name=capture.source_path.name,
            )
        )
        self.viewer_meta_label.setText(self._raw_processing_summary_text())
        self._update_pending_raw_controls()

    def _refresh_selected_pending_raw_preview(self) -> None:
        capture = self._current_pending_raw_capture()
        if capture is None:
            return
        if capture.status not in {"pending", "failed"}:
            return
        try:
            preview_path = render_raw_preview(
                capture.source_path,
                settings=capture.raw_settings,
                output_path=capture.preview_path if capture.preview_path and Path(capture.preview_path).exists() else None,
                output_dir=self._pending_raw_preview_dir(),
            )
            capture.preview_path = Path(preview_path)
            capture.status = "pending"
            self._refresh_session_gallery()
            self._show_pending_raw_capture(self._selected_pending_raw_index)
        except RawRenderingUnavailableError as exc:
            capture.status = "failed"
            self._show_status(
                self.tr("RAW preview unavailable for {name}: {error}").format(name=capture.source_path.name, error=str(exc)),
                tone="warning",
                timeout_ms=6000,
            )
        except RuntimeError as exc:
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
            timer.start(250)

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
        resolved_lab_metadata = dict(lab_metadata or self._current_lab_metadata())
        resolved_lab_metadata["image_type"] = "microscope"
        resolved_settings = RawRenderSettings.from_dict(raw_settings or self._current_raw_render_settings())
        pending = PendingRawCapture(
            source_path=source,
            companion_jpeg_path=Path(companion_jpeg_path) if companion_jpeg_path else None,
            lab_metadata=resolved_lab_metadata,
            raw_settings=resolved_settings,
            group_key=group_key,
            created_at=datetime.now(),
        )
        preview_path = render_raw_preview(
            source,
            settings=resolved_settings,
            output_dir=self._pending_raw_preview_dir(),
        )
        pending.preview_path = Path(preview_path)
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
            committed = self._ingest_detected_image(
                str(pending.source_path),
                raw_settings=pending.raw_settings,
                lab_metadata=pending.lab_metadata,
            )
        except Exception:
            committed = False
        if committed:
            pending.status = "committed"
        return committed

    def _commit_selected_pending_raw_capture(self) -> None:
        pending = self._current_pending_raw_capture()
        if pending is None:
            return
        self._cancel_pending_raw_background_wb_selection()
        captures = getattr(self, "_pending_raw_captures", [])
        index = self._selected_pending_raw_index_value()
        preview_path = Path(pending.preview_path) if pending.preview_path else None
        if self._commit_pending_raw_capture(pending):
            if preview_path is not None:
                try:
                    preview_path.unlink(missing_ok=True)
                except Exception:
                    pass
            if 0 <= index < len(captures):
                captures.pop(index)
            self._pending_raw_captures = captures
            if captures:
                self._selected_pending_raw_index = min(index, len(captures) - 1)
            else:
                self._selected_pending_raw_index = -1
            self._update_pending_raw_controls()
            if captures:
                self._show_pending_raw_capture(self._selected_pending_raw_index)
            else:
                if self._session_image_ids:
                    self._show_session_image(self._session_image_ids[-1])
                else:
                    self._clear_session_viewer(
                        title=self.tr("Waiting for first import"),
                        meta=self.tr("New microscope captures from the watched folder will appear here automatically."),
                    )

    def _discard_selected_pending_raw_capture(self) -> None:
        pending = self._current_pending_raw_capture()
        if pending is None:
            return
        self._cancel_pending_raw_background_wb_selection()
        captures = getattr(self, "_pending_raw_captures", [])
        index = self._selected_pending_raw_index_value()
        preview_path = Path(pending.preview_path) if pending.preview_path else None
        pending.status = "discarded"
        if preview_path is not None:
            try:
                preview_path.unlink(missing_ok=True)
            except Exception:
                pass
        if 0 <= index < len(captures):
            captures.pop(index)
        self._pending_raw_captures = captures
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
                capture.preview_path = Path(
                    render_raw_preview(
                        capture.source_path,
                        settings=copied_settings,
                        output_path=capture.preview_path if capture.preview_path and Path(capture.preview_path).exists() else None,
                        output_dir=self._pending_raw_preview_dir(),
                    )
                )
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
        for capture in list(getattr(self, "_pending_raw_captures", []) or []):
            preview_path = Path(capture.preview_path) if capture.preview_path else None
            if preview_path is not None:
                try:
                    preview_path.unlink(missing_ok=True)
                except Exception:
                    pass
        self._pending_raw_captures = []
        self._selected_pending_raw_index = -1
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
        self._seen_source_paths.add(source)

        if not Path(source).exists():
            return False
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
        if self._hint_controller is not None:
            self._hint_controller.set_hint(text, tone=tone)

    def _show_status(self, text: str | None, tone: str = "info", timeout_ms: int = 4000) -> None:
        if self._hint_controller is not None:
            self._hint_controller.set_status(text, timeout_ms=timeout_ms, tone=tone)

    def _make_combo(self):
        from PySide6.QtWidgets import QComboBox

        combo = QComboBox()
        combo.setSizeAdjustPolicy(combo.SizeAdjustPolicy.AdjustToContents)
        return combo

    def _build_term_combo(self, category: str):
        combo = self._make_combo()
        values = DatabaseTerms.canonicalize_list(
            category,
            SettingsDB.get_list_setting(DatabaseTerms.setting_key(category), DatabaseTerms.default_values(category)),
        )
        for value in values:
            combo.addItem(DatabaseTerms.translate(category, value), value)
        combo.currentIndexChanged.connect(
            lambda _idx, cat=category, c=combo: self._remember_last_used_term(cat, c.currentData())
        )
        return combo

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

    def _populate_objective_combo(self) -> None:
        self.objective_combo.clear()
        self.objective_combo.addItem(self.tr("Not set"), None)
        objectives = load_objectives()
        rows = []
        for key, obj in objectives.items():
            if str(obj.get("optics_type") or "microscope").strip().lower() == "macro":
                continue
            rows.append((key, obj))
        rows.sort(key=lambda item: objective_sort_value(item[1], item[0]))
        for key, obj in rows:
            self.objective_combo.addItem(objective_display_name(obj, key) or str(key), key)
        saved = str(SettingsDB.get_setting(self.SETTING_LAST_OBJECTIVE, "") or "").strip()
        if saved:
            index = self.objective_combo.findData(saved)
            if index >= 0:
                self.objective_combo.setCurrentIndex(index)

    def _save_objective_selection(self) -> None:
        SettingsDB.set_setting(self.SETTING_LAST_OBJECTIVE, str(self.objective_combo.currentData() or ""))

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
                self.objective_combo.currentData(),
                self.objective_combo.currentText(),
            )
        )
        self.contrast_combo.currentIndexChanged.connect(
            lambda _idx: self._log_dropdown_change(
                "contrast",
                self.contrast_combo.currentData(),
                self.contrast_combo.currentText(),
            )
        )
        self.mount_combo.currentIndexChanged.connect(
            lambda _idx: self._log_dropdown_change(
                "mount_medium",
                self.mount_combo.currentData(),
                self.mount_combo.currentText(),
            )
        )
        self.stain_combo.currentIndexChanged.connect(
            lambda _idx: self._log_dropdown_change(
                "stain",
                self.stain_combo.currentData(),
                self.stain_combo.currentText(),
            )
        )
        self.sample_combo.currentIndexChanged.connect(
            lambda _idx: self._log_dropdown_change(
                "sample_type",
                self.sample_combo.currentData(),
                self.sample_combo.currentText(),
            )
        )

    def _restore_watch_dir(self) -> None:
        saved = str(SettingsDB.get_setting(self.SETTING_WATCH_DIR, "") or "").strip()
        if saved:
            self.watch_dir_input.setText(saved)

    def _on_watch_dir_changed(self, text: str) -> None:
        SettingsDB.set_setting(self.SETTING_WATCH_DIR, str(text or "").strip())
        self._update_session_controls()

    def _choose_watch_dir(self) -> None:
        current = str(self.watch_dir_input.text() or "").strip()
        start_dir = current if current and Path(current).exists() else str(Path.home())
        chosen = QFileDialog.getExistingDirectory(self, self.tr("Choose microscope capture folder"), start_dir)
        if chosen:
            self.watch_dir_input.setText(chosen)

    def _open_watch_dir(self) -> None:
        path = str(self.watch_dir_input.text() or "").strip()
        if path and Path(path).exists():
            QDesktopServices.openUrl(QUrl.fromLocalFile(path))

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
            "objective_name": self.objective_combo.currentData(),
            "objective_label": str(self.objective_combo.currentText() or "").strip() or None,
            "contrast": DatabaseTerms.canonicalize("contrast", self.contrast_combo.currentData()),
            "contrast_label": str(self.contrast_combo.currentText() or "").strip() or None,
            "mount_medium": DatabaseTerms.canonicalize("mount", self.mount_combo.currentData()),
            "mount_label": str(self.mount_combo.currentText() or "").strip() or None,
            "stain": DatabaseTerms.canonicalize("stain", self.stain_combo.currentData()),
            "stain_label": str(self.stain_combo.currentText() or "").strip() or None,
            "sample_type": DatabaseTerms.canonicalize("sample", self.sample_combo.currentData()),
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
        value = str(raw_value or "").strip() or None
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
        self._log_dropdown_change("objective_name", self.objective_combo.currentData(), self.objective_combo.currentText())
        self._log_dropdown_change("contrast", self.contrast_combo.currentData(), self.contrast_combo.currentText())
        self._log_dropdown_change("mount_medium", self.mount_combo.currentData(), self.mount_combo.currentText())
        self._log_dropdown_change("stain", self.stain_combo.currentData(), self.stain_combo.currentText())
        self._log_dropdown_change("sample_type", self.sample_combo.currentData(), self.sample_combo.currentText())

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

        self.change_observation_btn.setEnabled(not running and not stopping)
        self.change_observation_btn.setProperty(
            "_hint_disabled_text",
            self.tr("Stop the current Live Lab session before changing observation."),
        )
        self.session_mode_combo.setEnabled(not running and not stopping)
        self.session_mode_combo.setProperty(
            "_hint_disabled_text",
            self.tr("Stop the current session before changing capture mode."),
        )
        self.watch_group.setVisible(mode_is_live if running else selected_mode == self.SESSION_MODE_LIVE)
        self.watch_dir_input.setReadOnly(running or stopping or selected_mode != self.SESSION_MODE_LIVE)
        self.browse_btn.setEnabled(not running and not stopping and selected_mode == self.SESSION_MODE_LIVE)
        self.open_folder_btn.setEnabled(watch_ok and selected_mode == self.SESSION_MODE_LIVE)
        self.open_folder_btn.setProperty(
            "_hint_disabled_text",
            self.tr("Choose an existing watched folder first."),
        )
        self.start_stop_btn.setEnabled(bool(running or can_start))
        self.session_note_input.setEnabled(bool(running and not stopping))
        self.add_note_btn.setEnabled(bool(running and not stopping))

        if stopping:
            self.start_stop_btn.setText(self.tr("Stopping..."))
            status_text = (
                self.tr("Stopping the retrospective session...")
                if active_mode == self.SESSION_MODE_OFFLINE
                else self.tr("Stopping the Live Lab session...")
            )
        elif running:
            self.start_stop_btn.setText(self.tr("Stop Session"))
            status_text = (
                self.tr("Recording lab-state changes for retrospective matching.")
                if active_mode == self.SESSION_MODE_OFFLINE
                else self.tr("Watching for new microscope captures.")
            )
        else:
            self.start_stop_btn.setText(
                self.tr("Start Log Session")
                if selected_mode == self.SESSION_MODE_OFFLINE
                else self.tr("Start Session")
            )
            if not self._target_observation_id:
                status_text = self.tr("Choose a current observation in Observations before starting a Live Lab session.")
            elif selected_mode == self.SESSION_MODE_LIVE and not watch_ok:
                status_text = self.tr("Choose an existing microscope capture folder to watch.")
            elif selected_mode == self.SESSION_MODE_OFFLINE:
                status_text = self.tr("Ready to start a retrospective log-only session.")
            else:
                status_text = self.tr("Ready to start a Live Lab session.")

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
        self.session_status_label.setText(status_text)
        self.session_count_label.setText(
            self.tr("Imported this session: {count}").format(count=self._session_import_count)
        )

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
        self._session_observation_id = None
        self._session_observation_snapshot = None
        self._cancel_raw_edit_session(restore_selection=False)
        self._reset_companion_dedupe_state()
        self._update_session_controls()

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
        if self.sender() is not self._watcher:
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

        ingest_lab_metadata = dict(lab_metadata or getattr(ingest, "lab_metadata", None) or self._current_lab_metadata())
        objective_key = ingest_lab_metadata.get("objective_name") or self.objective_combo.currentData()
        contrast_value = ingest_lab_metadata.get("contrast")
        mount_value = ingest_lab_metadata.get("mount_medium")
        stain_value = ingest_lab_metadata.get("stain")
        sample_value = ingest_lab_metadata.get("sample_type")
        if not str(contrast_value or "").strip():
            contrast_value = self.contrast_combo.currentData()
        if not str(mount_value or "").strip():
            mount_value = self.mount_combo.currentData()
        if not str(stain_value or "").strip():
            stain_value = self.stain_combo.currentData()
        if not str(sample_value or "").strip():
            sample_value = self.sample_combo.currentData()
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
            lab_metadata=getattr(ingest, "lab_metadata", None) or ingest_lab_metadata or self._current_lab_metadata(),
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
    ) -> bool:
        observation_id = int(self._session_observation_id or 0)
        if observation_id <= 0:
            return False

        output_dir = get_images_dir() / "imports"
        output_dir.mkdir(parents=True, exist_ok=True)
        ingest_context = dict(lab_metadata or self._current_lab_metadata())
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
        items, selected_key = self._session_gallery_items()
        gallery.set_items(items)
        if selected_key is not None:
            gallery.select_image(selected_key)

    def _on_session_gallery_clicked(self, image_id, _path: str) -> None:
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
            self._show_pending_raw_capture(pending_index)
            return
        resolved_image_id = clicked_image_id
        if resolved_image_id <= 0:
            return
        self._selected_pending_raw_index = -1
        self._selected_session_image_id = resolved_image_id
        self._update_pending_raw_controls()
        self.session_gallery.select_image(resolved_image_id)
        self._show_session_image(resolved_image_id)

    def _clear_session_viewer(self, title: str | None = None, meta: str | None = None) -> None:
        self.viewer_title_label.setText(title or self.tr("Last import"))
        self.viewer_meta_label.setText(meta or self.tr("The next imported microscope image will appear here."))
        self.reset_view_btn.setEnabled(False)
        self.live_image_label.set_microns_per_pixel(0.0)
        self.live_image_label.set_scale_bar(False, 0.0)
        self.live_image_label.set_image(None)

    def _show_session_image(self, image_id: int) -> None:
        image = ImageDB.get_image(image_id)
        if not image:
            self._clear_session_viewer()
            self._update_raw_edit_controls()
            return
        self._selected_session_image_id = int(image_id)

        image_path = str(image.get("filepath") or "").strip()
        if not image_path:
            self._clear_session_viewer(
                title=self.tr("Selected image unavailable"),
                meta=self.tr("This session image does not have a stored file path."),
            )
            self._update_raw_edit_controls()
            return

        edit_session = getattr(self, "_raw_edit_session", None)
        if edit_session is not None and int(edit_session.image_id) == int(image_id):
            preview_path = str(edit_session.preview_path or "").strip()
            if not preview_path or not Path(preview_path).exists():
                self._clear_session_viewer(
                    title=self.tr("RAW edit preview unavailable"),
                    meta=self.tr("The temporary RAW edit preview for {name} is not available.").format(
                        name=edit_session.source_raw_path.name
                    ),
                )
                self._update_raw_edit_controls()
                return
            image_path = preview_path

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
            return

        self.live_image_label.set_image_sources(pixmap, image_path, preview_scaled)
        self.reset_view_btn.setEnabled(True)

        mpp_value = self._image_microns_per_pixel(image)
        self.live_image_label.set_microns_per_pixel(mpp_value or 0.0)
        if mpp_value:
            scale_bar_value, scale_bar_unit = self._viewer_scale_bar_config(image)
            scale_bar_microns = float(scale_bar_value) * 1000.0 if scale_bar_unit == "mm" else float(scale_bar_value)
            self.live_image_label.set_scale_bar(True, scale_bar_microns, unit=scale_bar_unit)
        else:
            self.live_image_label.set_scale_bar(False, 0.0)

        if edit_session is not None and int(edit_session.image_id) == int(image_id):
            self.viewer_title_label.setText(
                self.tr("Editing RAW: {name}").format(name=edit_session.source_raw_path.name)
            )
            self.viewer_meta_label.setText(self._raw_settings_summary_text(edit_session.working_settings))
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
        self._update_raw_edit_controls()

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
        objective_name = str(image.get("objective_name") or "").strip()
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
