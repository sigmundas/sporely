"""Live microscopy session tab."""
from __future__ import annotations

import time
from datetime import datetime
import tempfile
from dataclasses import field
from uuid import uuid4
from pathlib import Path

import numpy as np

from PySide6.QtCore import (
    QSize,
    Qt,
    QThread,
    QTimer,
    QSignalBlocker,
    QEvent,
    QPointF,
    QRect,
    Signal,
)
from PySide6.QtGui import (
    QColor,
    QIcon,
    QImage,
    QImageReader,
    QPainter,
    QPixmap,
    QShortcut,
    QKeySequence,
    QWheelEvent,
)
from PySide6.QtWidgets import (
    QApplication,
    QFileDialog,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSplitter,
    QProgressBar,
    QStackedWidget,
    QVBoxLayout,
    QToolButton,
    QWidget,
    QAbstractSpinBox,
    QComboBox,
    QPlainTextEdit,
    QTextEdit,
)

from config import (
    RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG,
    RAW_COMPANION_SOURCE_PREFERENCE_PREFER_RAW,
    SETTING_RAW_COMPANION_SOURCE_PREFERENCE,
)
from database.database_tags import DatabaseTerms
from database.models import CalibrationDB, ImageDB, ObservationDB, SessionLogDB, SettingsDB
from database.schema import (
    get_images_dir,
    load_objectives,
    objective_display_name,
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
    PostDecodeTransferCurve,
    apply_post_decode_processing_fast,
)
from utils.local_image_ingest import LocalIngestResult, RawRenderingUnavailableError, prepare_local_ingest_image
from utils.lab_watcher import LabWatcherWorker
from utils.raw_detection import SUPPORTED_RAW_SUFFIXES, is_raw_image_path
from utils.raw_render import (
    RawRenderSettings,
    render_raw_image,
    render_raw_preview_proxy_rgb,
    save_raw_preview_jpeg,
)
from utils.thumbnail_generator import generate_all_sizes, get_thumbnail_path

from .hint_status import HintBar, HintStatusController, style_progress_widgets
from .image_gallery_widget import ImageGalleryWidget
from .adaptive_choice_selector import objective_color, objective_short_label
from .raw_processing_controls import RawProcessingControls
from .collapsible_section import ElidedLabel
from .section_card import create_section_card
from .segmented_selector import SegmentedSelector
from .sync_shot_dialog import SyncShotDialog, SyncShotQrView
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
from .capture_state_bar import CaptureStateBar, readable_text_color
from .styles import HUD_COLORS, STAGING_BORDER, objective_ring_outline
from .live_lab_prep_panel import LiveLabPrepPanelMixin
from .live_lab_raw import LiveLabRawMixin
# The RAW edit/preview machinery lives in ui/live_lab_raw.py; these names stay
# importable from this module for existing callers and tests.
from .live_lab_raw import (  # noqa: F401
    _RAW_DEBUG_TIMING,
    PENDING_RAW_COMMIT_MAX_WORKERS,
    _raw_timing_log,
    PendingRawCapture,
    RawEditSession,
    _RAW_PREVIEW_WB_CACHE_MAX,
    _RawPreviewCacheEntry,
    _pending_raw_source_signature,
    _pending_raw_settings_signature,
    _pending_raw_preview_decode_mode,
    _PendingRawPreviewJobRequest,
    _PendingRawPreviewJobResult,
    _pending_raw_preview_cache_has_entry,
    _build_pending_raw_preview_job_result,
    _PendingRawPreviewWorker,
    _PendingRawCommitJobRequest,
    _PendingRawCommitJobResult,
    _build_pending_raw_commit_job_result,
    _PendingRawCommitWorker,
    _resize_preview_rgb,
    _TONE_CURVE_PREVIEW_ANALYSIS_MAX_DIM,
    _TONE_CURVE_PREVIEW_HISTOGRAM_BINS,
    RawCurvePreviewWidget,
    _compute_combined_rgb_histogram,
)


class _LiveAwayStrip:
    """Live Lab's view of the shared strip while View or Measure holds it (stage 9d).

    Live Lab keeps working in the background (captures arrive, previews
    render). Its writes must not touch the strip another page shows, and its
    reads must see Live Lab's own selection, never the other page's: the
    capture bar and the RAW panel edit what Live Lab has selected. Thumbnail
    cache and the per-observation unsaved images still go to the real strip.
    """

    def __init__(self, strip, keys=(), paths=()):
        self._strip = strip
        self._keys = set(keys)
        self._paths = [str(path) for path in paths if path]
        self.stale = False

    # reads: Live Lab's own selection
    def selected_keys(self):
        return set(self._keys)

    def selected_paths(self):
        return list(self._paths)

    def is_multi_select(self):
        return True

    # writes: remembered, applied when Live Lab takes the strip back
    def select_image(self, image_id, center=True):
        del center
        self._keys = {image_id} if image_id is not None else set()
        self._paths = []
        self.stale = True

    def select_paths(self, paths, center=True):
        del center
        self._paths = [str(path) for path in paths or [] if path]
        self._keys = set()
        self.stale = True

    def set_items(self, *args, **kwargs):
        self.stale = True

    def clear(self):
        self._keys, self._paths = set(), []
        self.stale = True

    def exit_edit_selection(self):
        self._keys, self._paths = set(), []
        self.stale = True

    def center_on_key(self, *args, **kwargs):
        pass

    # shared state that is not the page's
    def invalidate_pixmap_cache(self, *args, **kwargs):
        return self._strip.invalidate_pixmap_cache(*args, **kwargs)

    def set_staged_items(self, observation_id, items, *, render=True):
        self._strip.set_staged_items(observation_id, items, render=render)

    def __getattr__(self, name):
        if name.startswith("set_"):
            return lambda *args, **kwargs: None  # page behaviour, applied on reclaim
        return getattr(self._strip, name)


class LiveLabTab(LiveLabPrepPanelMixin, LiveLabRawMixin, QWidget):
    """Watch a capture folder and ingest new microscope images into one observation."""

    # Unsaved captures per observation changed (the observation list shows them).
    unsavedCountsChanged = Signal(object)
    # “Live 00:14” / “Logging 00:14” while a session runs, "" otherwise (the mode bar).
    sessionBadgeChanged = Signal(str)
    # Bilder's source: "live" or "files" (stage 9f).
    sourceChanged = Signal(str)

    SETTING_WATCH_DIR = "live_lab_watch_dir"
    SETTING_LAST_OBJECTIVE = "live_lab_last_objective"
    SETTING_SESSION_MODE = "live_lab_session_mode"
    SETTING_MAIN_SPLITTER = "live_lab_main_splitter_sizes"
    SETTING_CONTENT_SPLITTER = "live_lab_content_splitter_sizes"
    SETTING_RAW_MORE_SECTION_EXPANDED = "live_lab_raw_more_expanded"
    SESSION_MODE_LIVE = "live"
    SESSION_MODE_OFFLINE = "offline"
    VIEWER_PREVIEW_MAX_DIM = 1600
    VIEWER_SCALE_BAR_UM = 10.0
    OBSERVATION_PREVIEW_SIZE = 116
    # Keyboard shortcuts on the capture-state bar (decided 2026-11-09): 1-6
    # choose the objectives in magnification order, letters the contrast
    # methods. They act only when no text field has focus.
    OBJECTIVE_SHORTCUT_KEYS = ("1", "2", "3", "4", "5", "6")
    CONTRAST_SHORTCUT_KEYS = {"BF": "B", "DF": "D", "DIC": "I", "Phase": "P"}
    SESSION_BUTTON_BASE_STYLE = "font-weight: bold; padding: 4px 10px;"
    SESSION_BUTTON_ACTIVE_STYLE = (
        "font-weight: bold; padding: 4px 10px; background-color: #e74c3c; color: white;"
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
        # Sidebar-metadata binding rules:
        #   - False: sidebar changes represent a NEW microscope state to be
        #     applied to the NEXT capture. Never mutates already-captured
        #     images. `_session_microscope_state` shadows the sidebar.
        #   - True: user explicitly clicked a thumbnail; sidebar changes are
        #     an edit of that image (or the currently multi-selected images).
        self._sidebar_binds_to_selection: bool = False
        # What the viewer shows: ("image", image_id), ("pending", capture) or
        # None (empty: waiting for the next capture). The live-view label, the
        # scale bar and the calibration notice describe this subject.
        self._viewer_subject: tuple | None = None
        # The shown image's DB row, read when it is shown and after edits of its
        # metadata, so control changes do not hit the database.
        self._viewer_subject_metadata: dict | None = None
        # Snapshot of the microscope state used for the next capture. Kept in
        # sync with the sidebar while unbound, and reset to the most recent
        # capture's metadata after each ingest.
        self._session_microscope_state: dict = {}
        self._raw_render_settings = RawRenderSettings.default()
        # True after an objective or contrast change: the RAW panel shows and
        # edits the next capture's settings rather than the image on screen.
        self._raw_panel_shows_next_capture = False
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
        self._pending_raw_preview_proxy_cache: dict[
            tuple[str, int, int, str], "_RawPreviewCacheEntry"
        ] = {}
        self._pending_raw_preview_job_counter = 0
        self._pending_raw_preview_active_request: _PendingRawPreviewJobRequest | None = None
        self._pending_raw_preview_job_queue: list[_PendingRawPreviewJobRequest] = []
        self._pending_raw_preview_worker_thread: QThread | None = None
        self._pending_raw_preview_worker: _PendingRawPreviewWorker | None = None
        self._pending_raw_commit_job_counter = 0
        self._pending_raw_commit_active_request: _PendingRawCommitJobRequest | None = None
        self._pending_raw_commit_job_queue: list[_PendingRawCommitJobRequest] = []
        self._pending_raw_commit_worker_thread: QThread | None = None
        self._pending_raw_commit_worker: _PendingRawCommitWorker | None = None
        self._pending_raw_commit_batch_mode: str | None = None
        self._pending_raw_commit_batch_total = 0
        self._pending_raw_commit_batch_completed = 0
        self._pending_raw_commit_batch_saved = 0
        self._pending_raw_commit_batch_failed = 0
        self._pending_raw_commit_batch_skipped = 0
        self._pending_raw_commit_cancel_requested = False
        self._pending_raw_commit_defer_start = False
        self._session_stop_processed_raw_capture_ids: set[int] = set()
        self._raw_curve_preview_analysis_signature: tuple[str, int, int, str] | None = None
        self._raw_curve_preview_analysis_rgb: np.ndarray | None = None
        self._raw_curve_preview_histogram: np.ndarray | None = None
        self._raw_preview_last_curve: PostDecodeTransferCurve | None = None
        self._raw_preview_last_curve_signature: tuple[object, ...] | None = None
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
        self._pending_raw_background_render_queue: list[PendingRawCapture] = []
        self._pending_raw_background_render_timer = QTimer(self)
        self._pending_raw_background_render_timer.setSingleShot(True)
        self._pending_raw_background_render_timer.setInterval(50)
        self._pending_raw_background_render_timer.timeout.connect(
            self._process_pending_raw_background_render_queue
        )
        self._raw_review_shortcuts: list[QShortcut] = []
        self._raw_copied_settings: RawRenderSettings | None = None
        self._pending_raw_copy_source_path: str | None = None
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
        self._install_capture_shortcuts()
        self._populate_objective_combo()
        self._restore_term_selection(self.contrast_combo, "contrast")
        self._restore_term_selection(self.mount_combo, "mount")
        self._restore_term_selection(self.stain_combo, "stain")
        self._restore_term_selection(self.sample_combo, "sample")
        self._restore_term_selection(self.sample_source_combo, "sample_source")
        self._restore_raw_capture_mode()
        self._restore_raw_companion_source_preference()
        self._load_raw_processing_settings_for_current_context()
        self._restore_watch_dir()
        self._restore_session_mode()
        self._connect_session_logging_signals()
        # After the logging handlers, so an objective change is logged before
        # the contrast it brings back.
        self.objective_combo.currentIndexChanged.connect(self._restore_contrast_for_objective)
        self.contrast_combo.currentIndexChanged.connect(self._remember_contrast_for_objective)
        # RAW settings follow the objective + contrast combination (stage 5).
        self.objective_combo.currentIndexChanged.connect(self._apply_raw_settings_for_combination)
        self.contrast_combo.currentIndexChanged.connect(self._apply_raw_settings_for_combination)
        self.capture_state_bar.contrast_segments.chosen.connect(self._on_contrast_chosen)
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
        self._root_layout = root_layout
        self._live_content = main_splitter
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
        current_text_layout.setSpacing(4)
        # Stage 9d: a running session keeps its observation while another is
        # open in View, Measure or Analysis; say so here.
        self.session_target_notice = QLabel(self.tr("Session's observation: captures are saved here"))
        self.session_target_notice.setWordWrap(True)
        self.session_target_notice.setStyleSheet(f"font-weight: 600; color: {STAGING_BORDER};")
        self.session_target_notice.setVisible(False)
        current_text_layout.addWidget(self.session_target_notice)
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
        self.current_observation_location_label = QLabel("")
        self.current_observation_location_label.setWordWrap(True)
        self.current_observation_location_label.setStyleSheet("color: #6b7280;")
        self.current_observation_location_label.setVisible(False)
        current_text_layout.addWidget(self.current_observation_location_label)
        current_text_layout.addStretch(1)
        # Starting and stopping the session belongs to the observation, not
        # the image: the viewer shows only what is needed at the microscope.
        self.start_stop_btn = QPushButton(self.tr("Start\nSession").replace("\n", " "))
        self.start_stop_btn.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.start_stop_btn.setStyleSheet(self.SESSION_BUTTON_BASE_STYLE)
        self.start_stop_btn.clicked.connect(self._toggle_session)
        current_row.addLayout(current_text_layout, 1)
        current_layout.addLayout(current_row)
        current_layout.addWidget(self._build_session_box())
        left_layout.addWidget(current_group)

        # These controls remain as the runtime binding for session settings,
        # while their user-facing counterparts live in Preferences → Image import.
        session_settings = QWidget(self)
        session_settings.hide()
        self.session_mode_selector = SegmentedSelector(session_settings, compact=True, fill_width=True)
        self.session_mode_live_radio = self.session_mode_selector.add_option(
            self.tr("Camera connected"),
            self.SESSION_MODE_LIVE,
            checked=True,
        )
        self.session_mode_offline_radio = self.session_mode_selector.add_option(
            self.tr("Log only"),
            self.SESSION_MODE_OFFLINE,
        )
        self.session_mode_combo = self.session_mode_selector
        self.session_mode_selector.selectionChanged.connect(lambda _value: self._on_session_mode_changed())

        self.watch_group = QWidget(session_settings)
        self.watch_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        watch_layout = QHBoxLayout(self.watch_group)
        watch_layout.setContentsMargins(0, 0, 0, 0)
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

        # Objective and contrast (the optical path) are chosen on the
        # capture-state bar over the viewer; PREPARATION holds everything about
        # the mounted specimen: mount medium, stain, condition and source.
        self._build_microscope_selectors()
        self._build_slide_prep_card()
        left_layout.addWidget(self.slide_prep_group)

        self._build_notes_card()
        left_layout.addWidget(self.notes_group)

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
            more_section_key=self.SETTING_RAW_MORE_SECTION_EXPANDED,
        )
        self._push_raw_processing_cutoffs_to_controls()
        self.raw_controls.settingsChanged.connect(self._on_raw_processing_controls_changed)
        self.raw_controls.pickWhiteBalanceToggled.connect(self._toggle_active_raw_background_wb_pick)
        raw_body_layout.addWidget(self.raw_controls)
        self.raw_curve_preview_widget = RawCurvePreviewWidget(self.raw_processing_body)
        # The curve and histogram belong with the tone curve in "More".
        self.raw_controls.more_section.body_layout.addWidget(self.raw_curve_preview_widget, 0, Qt.AlignHCenter)

        self.pending_raw_frame = QFrame()
        self.pending_raw_frame.setFrameShape(QFrame.NoFrame)
        self.pending_raw_frame.setObjectName("pendingRawSaveBar")
        self.pending_raw_frame.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        pending_raw_layout = QHBoxLayout(self.pending_raw_frame)
        pending_raw_layout.setContentsMargins(8, 6, 8, 6)
        pending_raw_layout.setSpacing(6)

        # Stage 9d: saving happens in the strip under the viewer, not on the
        # image: “N not saved · Save selected · Save to observation”.
        self.pending_raw_count_label = QLabel(self.pending_raw_frame)
        self.pending_raw_count_label.setStyleSheet(f"font-weight: 600; color: {STAGING_BORDER};")
        pending_raw_layout.addWidget(self.pending_raw_count_label)
        pending_raw_layout.addStretch(1)

        self.pending_raw_save_btn = QPushButton(self.tr("Save current"))
        self.pending_raw_save_btn.setObjectName("outlineButton")
        self.pending_raw_save_btn.clicked.connect(self._commit_selected_pending_raw_capture)
        pending_raw_layout.addWidget(self.pending_raw_save_btn)
        self.pending_raw_save_all_btn = QPushButton(self.tr("Save to observation"))
        self.pending_raw_save_all_btn.clicked.connect(self._commit_all_pending_raw_captures)
        pending_raw_layout.addWidget(self.pending_raw_save_all_btn)
        # Copy/paste of RAW settings lives in the RAW panel (stage 5): the image
        # frame shows only what you need while looking through the microscope.
        raw_copy_row = QWidget(self.raw_processing_body)
        raw_copy_layout = QHBoxLayout(raw_copy_row)
        raw_copy_layout.setContentsMargins(0, 0, 0, 0)
        raw_copy_layout.setSpacing(6)
        self.pending_raw_copy_btn = QPushButton(self.tr("Copy settings"), raw_copy_row)
        self.pending_raw_copy_btn.clicked.connect(self._copy_selected_pending_raw_settings)
        raw_copy_layout.addWidget(self.pending_raw_copy_btn)
        self.pending_raw_paste_btn = QPushButton(self.tr("Paste settings"), raw_copy_row)
        self.pending_raw_paste_btn.clicked.connect(self._paste_copied_settings_to_selected_pending)
        self.pending_raw_paste_btn.setVisible(False)
        raw_copy_layout.addWidget(self.pending_raw_paste_btn)
        raw_copy_layout.addStretch(1)
        raw_body_layout.addWidget(raw_copy_row)

        self.pending_raw_frame.setVisible(False)

        # Separate floating action bar for "Use for calibration" — visible
        # whenever any image is on screen (RAW pending capture, JPEG session
        # image, etc.), unlike the pending-RAW bar above.
        self.calibration_action_frame = QFrame()
        self.calibration_action_frame.setFrameShape(QFrame.NoFrame)
        self.calibration_action_frame.setObjectName("sectionCard")
        self.calibration_action_frame.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        calibration_action_layout = QHBoxLayout(self.calibration_action_frame)
        calibration_action_layout.setContentsMargins(8, 6, 8, 6)
        calibration_action_layout.setSpacing(6)
        self.use_for_calibration_btn = QPushButton(self.tr("Use for calibration"))
        self.use_for_calibration_btn.clicked.connect(self._on_use_current_image_for_calibration)
        calibration_action_layout.addWidget(self.use_for_calibration_btn)
        self.calibration_action_frame.setVisible(False)

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

        self.viewer_title_label = QLabel(self.tr("Last import"), viewer_panel)
        self.viewer_title_label.setStyleSheet("font-weight: 600; font-size: 15px;")
        self.viewer_title_label.setVisible(False)
        self.viewer_meta_label = QLabel("", viewer_panel)
        self.viewer_meta_label.setWordWrap(True)
        self.viewer_meta_label.setStyleSheet("color: #6b7280;")
        self.viewer_meta_label.setVisible(False)
        self.reset_view_btn = QPushButton(self.tr("Reset view"), viewer_panel)
        self.reset_view_btn.setEnabled(False)
        self.reset_view_btn.clicked.connect(self._reset_viewer)
        self.reset_view_btn.setVisible(False)
        # Kun logg (stage 9e): a photo chosen in the strip shows here; this goes back to the page.
        self.back_to_log_btn = QPushButton(self.tr("Back to the log"), viewer_panel)
        self.back_to_log_btn.setObjectName("outlineButton")
        self.back_to_log_btn.clicked.connect(self._back_to_log_page)
        self.back_to_log_btn.setVisible(False)
        viewer_layout.addWidget(self.back_to_log_btn, 0, Qt.AlignLeft)

        self.live_image_label = ZoomableImageLabel()
        self.live_image_label.setObjectName("liveLabImageLabel")
        self.live_image_label.setMinimumSize(320, 240)
        self.live_image_label.set_pan_without_shift(True)
        self.live_image_label.set_measurement_active(False)
        self.live_image_label.set_show_measure_labels(False)
        self.live_image_label.set_show_measure_overlays(False)
        self.live_image_label.clicked.connect(self._on_live_image_clicked_for_background_wb)
        viewer_layout.addWidget(self.live_image_label, 1)

        self.live_image_label.installEventFilter(self)
        # Pin the "Use for calibration" bar to the image viewer as well; it
        # sits at the top-right so it doesn't collide with the pending-RAW
        # controls at the bottom.
        self.calibration_action_frame.setParent(self.live_image_label)
        self.calibration_action_frame.setAttribute(Qt.WA_StyledBackground, True)
        self.calibration_action_frame.setStyleSheet(
            "QFrame#sectionCard {"
            " background-color: rgba(255, 255, 255, 215);"
            " border: 1px solid rgba(0, 0, 0, 40);"
            " border-radius: 8px;"
            "}"
        )
        self.calibration_action_frame.raise_()
        # Capture-state bar (stage 2): objective and contrast as large segments
        # at the bottom of the viewer, and a label at the top-left showing the
        # state of what is on screen, coloured by the objective's ring colour.
        self.capture_state_bar = CaptureStateBar(
            self.objective_combo,
            self.contrast_combo,
            objective_key=lambda _value, position: str(position + 1) if position < len(self.OBJECTIVE_SHORTCUT_KEYS) else None,
            contrast_key=lambda value, _position: self._contrast_shortcut_key(value),
            parent=self.live_image_label,
        )
        self.capture_state_bar.raise_()
        self.live_state_label = QLabel(self.live_image_label)
        self.live_state_label.setObjectName("liveStateLabel")
        self.live_state_label.setAttribute(Qt.WA_TransparentForMouseEvents, True)
        self.live_state_label.setVisible(False)
        # "Not calibrated · 63x  Calibrate…" bottom-left, above the zoom readout.
        self.calibration_notice = QFrame(self.live_image_label)
        self.calibration_notice.setObjectName("calibrationNotice")
        self.calibration_notice.setAttribute(Qt.WA_StyledBackground, True)
        notice_layout = QHBoxLayout(self.calibration_notice)
        notice_layout.setContentsMargins(12, 7, 10, 7)
        notice_layout.setSpacing(10)
        self.calibration_notice_label = QLabel(self.calibration_notice)
        self.calibration_notice_label.setObjectName("calibrationNoticeText")
        self.calibrate_link_btn = QPushButton(self.tr("Calibrate…"), self.calibration_notice)
        self.calibrate_link_btn.setObjectName("calibrationNoticeLink")
        self.calibrate_link_btn.setCursor(Qt.PointingHandCursor)
        self.calibrate_link_btn.setFlat(True)
        self.calibrate_link_btn.clicked.connect(self._on_calibrate_notice_clicked)
        notice_layout.addWidget(self.calibration_notice_label)
        notice_layout.addWidget(self.calibrate_link_btn)
        self.calibration_notice.setStyleSheet(
            "QFrame#calibrationNotice {"
            f" background-color: {HUD_COLORS['alert_background']};"
            f" border: 1px solid {HUD_COLORS['alert_border']}; border-radius: 8px;"
            " }"
            "QLabel#calibrationNoticeText {"
            f" color: {HUD_COLORS['alert_text']}; font-size: 13px; font-weight: 700; background: transparent; border: none;"
            " }"
            "QPushButton#calibrationNoticeLink {"
            f" color: {HUD_COLORS['alert_text']}; font-size: 13px; text-decoration: underline; background: transparent;"
            " border: none; padding: 0px;"
            " }"
        )
        self.calibration_notice.setVisible(False)
        # Forward wheel and native pinch-gesture events that land on either
        # floating action bar down to the image viewer beneath, so zooming
        # while the controls are visible still works.
        self._install_viewer_gesture_forwarding(self.calibration_action_frame)
        self._install_viewer_gesture_forwarding(self.capture_state_bar)
        # New buttons (objectives loaded, contrast list changed in Preferences)
        # need the forwarding too, and a new size means a new position.
        self.capture_state_bar.rebuilt.connect(self._on_capture_state_bar_rebuilt)

        self.session_gallery = ImageGalleryWidget(
            self.tr("Session Gallery"),
            parent=self,
            show_delete=True,
            show_badges=True,
            show_edit=True,
            thumbnail_size=132,
            default_height=GALLERY_DEFAULT_HEIGHT,
            min_height=GALLERY_MIN_HEIGHT,
        )
        self.session_gallery.set_center_reveal_mode("nudge")
        self.session_gallery.set_multi_select(True)
        self.session_gallery.set_toggle_selection_on_plain_click(True)
        self.session_gallery.set_clear_selection_on_background_click(True)
        self.session_gallery.set_reorderable(True)
        self.session_gallery.imageClicked.connect(self._on_session_gallery_clicked)
        self.session_gallery.selectionChanged.connect(self._on_session_gallery_selection_changed)
        self.session_gallery.itemsReordered.connect(self._on_session_gallery_items_reordered)
        self.session_gallery.deleteImagesRequested.connect(self._on_session_gallery_delete_images_requested)
        self.session_gallery.editRequested.connect(self._on_session_gallery_edit_requested)

        content_splitter = QSplitter(Qt.Vertical)
        content_splitter.setObjectName("gallerySplitter")
        content_splitter.setChildrenCollapsible(False)
        # Stage 9e: a Kun logg session has no live view; its page takes the
        # viewer's place, with the settings first.
        self.viewer_stack = QStackedWidget()
        self.viewer_stack.addWidget(viewer_panel)
        self.viewer_stack.addWidget(self._build_log_page())
        content_splitter.addWidget(self.viewer_stack)
        # The strip's slot: the save row, then the strip (stage 9d). In the
        # main window the strip is the one shared with View and Measure.
        self.strip_slot = QWidget()
        self.strip_slot.setObjectName("imageStripSlot")
        self.strip_slot.setMinimumHeight(GALLERY_MIN_HEIGHT)
        strip_slot_layout = QVBoxLayout(self.strip_slot)
        strip_slot_layout.setContentsMargins(0, 0, 0, 0)
        strip_slot_layout.setSpacing(4)
        strip_slot_layout.addWidget(self.pending_raw_frame)
        strip_slot_layout.addWidget(self.session_gallery, 1)
        content_splitter.addWidget(self.strip_slot)
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

        hint_area = QWidget(self)
        hint_area_layout = QVBoxLayout(hint_area)
        hint_area_layout.setContentsMargins(0, 0, 0, 0)
        hint_area_layout.setSpacing(4)

        self.hint_bar = HintBar(self)
        self.hint_bar.set_wrap_mode(True)
        self._hint_controller = HintStatusController(self.hint_bar, self)

        self.pending_raw_preview_progress_widget = QWidget(self)
        pending_progress_layout = QGridLayout(self.pending_raw_preview_progress_widget)
        pending_progress_layout.setContentsMargins(0, 0, 0, 0)
        pending_progress_layout.setHorizontalSpacing(6)
        pending_progress_layout.setVerticalSpacing(2)
        self.pending_raw_preview_progress_label = QLabel("")
        self.pending_raw_preview_progress_label.setWordWrap(False)
        self.pending_raw_preview_progress_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.pending_raw_preview_progress_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.pending_raw_preview_progress_bar = QProgressBar(self.pending_raw_preview_progress_widget)
        self.pending_raw_preview_progress_bar.setRange(0, 0)
        self.pending_raw_preview_progress_bar.setTextVisible(False)
        self.pending_raw_preview_progress_bar.setFormat("")
        self.pending_raw_preview_progress_bar.setFixedHeight(5)
        self.pending_raw_preview_progress_bar.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.pending_raw_commit_cancel_btn = QToolButton(self.pending_raw_preview_progress_widget)
        self.pending_raw_commit_cancel_btn.setText("✕")
        self.pending_raw_commit_cancel_btn.setToolTip(self.tr("Stop RAW save after current file"))
        self.pending_raw_commit_cancel_btn.setCursor(Qt.PointingHandCursor)
        self.pending_raw_commit_cancel_btn.setFixedSize(22, 22)
        self.pending_raw_commit_cancel_btn.setVisible(False)
        self.pending_raw_commit_cancel_btn.setStyleSheet(
            "QToolButton {"
            " border: 1px solid #b91c1c;"
            " border-radius: 4px;"
            " color: #ffffff;"
            " background-color: #dc2626;"
            " font-weight: 700;"
            " padding: 0px;"
            "}"
            "QToolButton:hover { background-color: #b91c1c; }"
            "QToolButton:pressed { background-color: #991b1b; }"
        )
        self.pending_raw_commit_cancel_btn.clicked.connect(self._cancel_pending_raw_commit_jobs)
        style_progress_widgets(
            self.pending_raw_preview_progress_bar,
            self.pending_raw_preview_progress_label,
        )
        pending_progress_layout.addWidget(self.pending_raw_preview_progress_label, 0, 0, 1, 1)
        pending_progress_layout.addWidget(self.pending_raw_preview_progress_bar, 1, 0, 1, 1)
        pending_progress_layout.addWidget(self.pending_raw_commit_cancel_btn, 0, 1, 2, 1, Qt.AlignRight | Qt.AlignVCenter)
        pending_progress_layout.setColumnStretch(0, 1)
        # Keep hint and progress in one fixed-height stack so RAW worker
        # progress never makes the Live Lab viewer/gallery jump vertically.
        self.hint_progress_stack = QStackedWidget(self)
        self.hint_progress_stack.addWidget(self.hint_bar)
        self.hint_progress_stack.addWidget(self.pending_raw_preview_progress_widget)
        stable_hint_height = max(
            int(self.hint_bar.sizeHint().height() or 0),
            int(self.pending_raw_preview_progress_widget.sizeHint().height() or 0),
            32,
        )
        self.hint_progress_stack.setFixedHeight(stable_hint_height)
        self.hint_progress_stack.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        hint_area_layout.addWidget(self.hint_progress_stack)
        self._hint_area = hint_area
        root_layout.addWidget(hint_area)

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
            self.tr("Specimen condition (Fresh/Dried) stored on newly imported microscope images."),
        )
        self._register_hint_widget(
            self.sample_source_combo,
            self.tr("Where the observed material came from (spore print, stipe, hymenium, ...)."),
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
            self.raw_controls.auto_levels_btn,
            self.tr("Apply automatic levels to the current RAW capture."),
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
            self.use_for_calibration_btn,
            self.tr("Open the currently displayed image in the Calibration dialog."),
        )
        self._register_hint_widget(
            self.pending_raw_copy_btn,
            self.tr("Copy the current RAW processing settings so you can paste them onto another pending capture."),
            disabled_hint=self.tr("Choose a pending RAW capture first."),
        )
        self._register_hint_widget(
            self.pending_raw_paste_btn,
            self.tr("Apply the copied RAW processing settings to this pending capture."),
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
            self.live_image_label,
            self._pending_raw_action_hint_text(),
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
            "sample_source": DatabaseTerms.canonicalize("sample_source", data.get("sample_source")),
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

    _MICROSCOPE_SIDEBAR_FIELDS = (
        ("objective_combo", "objective_name"),
        ("contrast_combo", "contrast"),
        ("mount_combo", "mount_medium"),
        ("stain_combo", "stain"),
        ("sample_combo", "sample_type"),
        ("sample_source_combo", "sample_source"),
    )

    def _apply_microscope_state_to_controls(self, metadata: dict | None) -> None:
        LiveLabTab._apply_microscope_state_to_controls_now(self, metadata)
        update_log_page = getattr(self, "_update_log_page", None)
        if callable(update_log_page):
            update_log_page()  # the selectors changed with their signals blocked

    def _apply_microscope_state_to_controls_now(self, metadata: dict | None) -> None:
        if getattr(self, "_sidebar_binds_to_selection", False):
            try:
                selected_keys = LiveLabTab._session_gallery_selected_keys(self)
            except Exception:
                selected_keys = []
            if len(selected_keys) > 1:
                try:
                    metadatas = LiveLabTab._collect_multi_select_metadatas(self)
                except Exception:
                    metadatas = []
                if len(metadatas) > 1:
                    LiveLabTab._apply_microscope_multi_state_to_controls(self, metadatas)
                    return
        note_loader = getattr(self, "_load_image_note_from_metadata", None)
        if callable(note_loader):
            note_loader(metadata)
        state = self._microscope_state_from_metadata(metadata)
        objective_combo = getattr(self, "objective_combo", None)
        if objective_combo is not None:
            clear_mixed = getattr(objective_combo, "clear_mixed", None)
            if callable(clear_mixed):
                clear_mixed()
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
            ("sample_source_combo", state.get("sample_source")),
        ):
            combo = getattr(self, combo_name, None)
            if combo is None:
                continue
            clear_mixed = getattr(combo, "clear_mixed", None)
            if callable(clear_mixed):
                clear_mixed()
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

    def _apply_microscope_multi_state_to_controls(
        self, metadatas: list[dict | None]
    ) -> None:
        """Reflect a multi-selection in the sidebar.

        Fields where every selected image agrees are set to that shared
        value as usual. Fields where the images disagree are put into a
        "mixed" visual state — no pill is checked, but each value present
        across the selection is rendered dimmed. See
        AdaptiveChoiceSelector.set_mixed_values.
        """
        if not metadatas:
            return
        notes_loader = getattr(self, "_load_image_note_from_metadatas", None)
        if callable(notes_loader):
            notes_loader(metadatas)
        states = [self._microscope_state_from_metadata(m) for m in metadatas]
        # Collect per-field value sets, preserving insertion order so the
        # dimmed pills mirror the order the user encounters them. For the
        # term categories a missing / None value maps to the "Not_set"
        # option so its "—" pill lights up alongside the others.
        field_values: dict[str, list] = {
            "objective_name": [],
            "contrast": [],
            "mount_medium": [],
            "stain": [],
            "sample_type": [],
            "sample_source": [],
        }
        term_fields = {"contrast", "mount_medium", "stain", "sample_type", "sample_source"}
        for state in states:
            for field, seen in field_values.items():
                value = state.get(field)
                if value in (None, "") and field in term_fields:
                    value = "Not_set"
                if value not in seen:
                    seen.append(value)

        for combo_name, field_key in self._MICROSCOPE_SIDEBAR_FIELDS:
            combo = getattr(self, combo_name, None)
            if combo is None:
                continue
            values = field_values.get(field_key, [])
            with QSignalBlocker(combo):
                if len(values) <= 1:
                    clear_mixed = getattr(combo, "clear_mixed", None)
                    if callable(clear_mixed):
                        clear_mixed()
                    value = values[0] if values else None
                    if value is not None and str(value).strip():
                        try:
                            index = combo.findData(value)
                        except Exception:
                            index = -1
                        if index < 0:
                            index = 0 if getattr(combo, "count", lambda: 0)() else -1
                        if index >= 0:
                            combo.setCurrentIndex(index)
                else:
                    set_mixed = getattr(combo, "set_mixed_values", None)
                    if callable(set_mixed):
                        set_mixed(values)
        update_alerts = getattr(self, "_update_lab_state_combo_alerts", None)
        if callable(update_alerts):
            update_alerts()
        refresh_viewer_objective_tag = getattr(self, "_refresh_viewer_objective_tag_from_current_state", None)
        if callable(refresh_viewer_objective_tag):
            refresh_viewer_objective_tag()

    def _is_sidebar_field_mixed(self, combo_name: str) -> bool:
        combo = getattr(self, combo_name, None)
        if combo is None:
            return False
        is_mixed = getattr(combo, "is_mixed", None)
        try:
            return bool(is_mixed()) if callable(is_mixed) else False
        except Exception:
            return False

    def _mixed_sidebar_field_keys(self) -> set[str]:
        """Return the metadata field keys whose sidebar selector is currently
        showing a mixed multi-select state — those must not be written back
        to any image, only the fields the user actually picked (which auto-
        exit mixed) should propagate."""
        keys: set[str] = set()
        # objective_name has both a canonical key and a label variant that
        # gets rebuilt from the combo. Treat any objective-linked metadata
        # as mixed when the objective row is mixed.
        objective_mixed = self._is_sidebar_field_mixed("objective_combo")
        if objective_mixed:
            keys.update({"objective_name", "objective_label"})
        for combo_name, field_key in self._MICROSCOPE_SIDEBAR_FIELDS:
            if combo_name == "objective_combo":
                continue
            if self._is_sidebar_field_mixed(combo_name):
                keys.add(field_key)
                keys.add(f"{field_key.split('_')[0]}_label")
        return keys

    def _collect_multi_select_metadatas(self) -> list[dict]:
        """Metadata dicts for all currently multi-selected gallery items
        (pending RAW captures + committed session images).
        """
        metadatas: list[dict] = []
        seen_pending: set[int] = set()
        seen_images: set[int] = set()
        captures = getattr(self, "_pending_raw_captures", []) or []
        for key in self._session_gallery_selected_keys():
            pending_index = self._pending_raw_capture_index_for_key(key)
            if pending_index is not None:
                if pending_index in seen_pending:
                    continue
                seen_pending.add(pending_index)
                if 0 <= pending_index < len(captures):
                    capture = captures[pending_index]
                    if isinstance(capture, PendingRawCapture):
                        metadatas.append(dict(capture.lab_metadata or {}))
                continue
            try:
                image_id = int(key)
            except (TypeError, ValueError):
                continue
            if image_id <= 0 or image_id in seen_images:
                continue
            seen_images.add(image_id)
            try:
                image = ImageDB.get_image(image_id)
            except Exception:
                image = None
            if image:
                metadatas.append(dict(image))
        return metadatas

    def _refresh_viewer_objective_tag_from_current_state(self, *_args) -> None:
        # Kept for its callers: the overlays now follow what the viewer shows.
        self._refresh_viewer_overlays()

    def _displayed_lab_metadata(self) -> dict | None:
        """Lab metadata of what the viewer shows; the current state when empty."""
        subject = getattr(self, "_viewer_subject", None)
        if subject and subject[0] == "image":
            cached = getattr(self, "_viewer_subject_metadata", None)
            if cached is None:
                try:
                    cached = ImageDB.get_image(int(subject[1])) or {}
                except Exception:
                    cached = {}
                self._viewer_subject_metadata = cached
            return cached or None
        if subject and subject[0] == "pending":
            # Its preview stays on screen while a batch save removes it.
            return dict(getattr(subject[1], "lab_metadata", None) or {})
        current_lab_metadata = getattr(self, "_current_lab_metadata", None)
        if callable(current_lab_metadata):
            try:
                return current_lab_metadata()
            except Exception:
                return None
        return None

    def _refresh_viewer_overlays(self, *_args) -> None:
        """Live-view label and calibration notice for what the viewer shows."""
        update_log_page = getattr(self, "_update_log_page", None)
        if callable(update_log_page):
            update_log_page()  # Kun logg: the page, or the photo chosen in the strip
        metadata = self._displayed_lab_metadata()
        update_viewer_objective_tag = getattr(self, "_update_viewer_objective_tag", None)
        if callable(update_viewer_objective_tag):
            update_viewer_objective_tag(metadata)
        update_notice = getattr(self, "_update_calibration_notice", None)
        if callable(update_notice):
            update_notice(metadata)

    def _subject_is_uncalibrated(self, metadata: dict | None) -> tuple[bool, str | None]:
        """(no scale for what the viewer shows, its objective key)."""
        data = dict(metadata or {})
        objective_key = LiveLabTab._objective_key_from_metadata(self, data)
        subject = getattr(self, "_viewer_subject", None)
        if subject and subject[0] == "image":
            return self._image_microns_per_pixel(data) is None, objective_key
        if not objective_key:
            return False, None
        objective = load_objectives().get(str(objective_key)) or {}
        try:
            scale = float(objective.get("microns_per_pixel") or 0.0)
        except (TypeError, ValueError):
            scale = 0.0
        return scale <= 0, objective_key

    def _update_calibration_notice(self, metadata: dict | None) -> None:
        notice = getattr(self, "calibration_notice", None)
        if notice is None:
            return
        uncalibrated, objective_key = self._subject_is_uncalibrated(metadata)
        if not uncalibrated:
            notice.setVisible(False)
            return
        objective = load_objectives().get(str(objective_key)) if objective_key else None
        if objective_key:
            name = objective_short_label(objective, str(objective_key)) or str(objective_key)
            text = self.tr("Not calibrated · {objective}").format(objective=name)
        else:
            text = self.tr("Not calibrated")
        # "Calibrate…" only for an objective that exists: the dialog would
        # otherwise open on another one.
        self._calibration_notice_objective = objective_key if objective is not None else None
        self.calibration_notice_label.setText(text)
        self.calibrate_link_btn.setVisible(objective is not None)
        notice.setVisible(True)
        notice.adjustSize()
        self._reposition_pending_raw_floating_tab()

    def _on_calibrate_notice_clicked(self) -> None:
        objective_key = getattr(self, "_calibration_notice_objective", None)
        opener = getattr(self._main_window, "open_calibration_dialog", None)
        if not callable(opener):
            return
        opener(objective_key=objective_key)
        # A new calibration changes the objectives' scales; keep what is chosen.
        self._populate_objective_combo(keep_current=True)
        if self._viewer_subject and self._viewer_subject[0] == "image":
            self._viewer_subject_metadata = None  # re-read: its scale may be stored now
        self._refresh_viewer_overlays()

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
        # The viewer's own corner tag is replaced by the live-state label.
        set_text = getattr(label, "set_objective_text", None)
        if callable(set_text):
            try:
                set_text("")
            except Exception:
                pass
        state_label = getattr(self, "live_state_label", None)
        if state_label is None:
            return
        text, color = LiveLabTab._live_state_label_parts(self, metadata)
        if not text:
            state_label.setVisible(False)
            return
        outline = objective_ring_outline(color)
        state_label.setText(text)
        state_label.setStyleSheet(
            "QLabel#liveStateLabel {"
            f" background-color: {color}; color: {readable_text_color(color)};"
            f" border: 1px solid {outline or color};"
            " border-radius: 8px; padding: 6px 12px; font-size: 18px; font-weight: 700;"
            " }"
        )
        state_label.setVisible(True)
        self._reposition_pending_raw_floating_tab()

    def _live_state_label_parts(self, metadata: dict | None) -> tuple[str | None, str | None]:
        """Text ("63x · DIC · Water") and ring colour of the live-view label."""
        data = dict(metadata or {})
        objective_name = LiveLabTab._objective_key_from_metadata(self, data)
        if not objective_name:
            return None, None
        objective = load_objectives().get(str(objective_name))
        parts = [
            objective_short_label(objective, str(objective_name))
            or (objective_display_name(objective, str(objective_name)) if objective else str(objective_name))
        ]
        for category, key in (("contrast", "contrast"), ("mount", "mount_medium")):
            value = DatabaseTerms.canonicalize(category, data.get(key))
            if value and str(value).strip().lower() not in {"not_set", "not set"}:
                parts.append(DatabaseTerms.translate(category, value))
        return " · ".join(part for part in parts if part), objective_color(objective, str(objective_name))

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

    def _update_calibration_action_visibility(self) -> None:
        """Show the 'Use for calibration' overlay whenever an image is loaded."""
        frame = getattr(self, "calibration_action_frame", None)
        label = getattr(self, "live_image_label", None)
        if frame is None or label is None:
            return
        pixmap = getattr(label, "original_pixmap", None)
        has_image = bool(pixmap is not None and not pixmap.isNull())
        frame.setVisible(has_image)
        if has_image:
            self._reposition_pending_raw_floating_tab()

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

    def eventFilter(self, obj, event) -> bool:
        if obj is getattr(self, "log_bar_slot", None) and event.type() == QEvent.Resize:
            self._reposition_pending_raw_floating_tab()
        if obj is getattr(self, "live_image_label", None):
            if event.type() == QEvent.MouseButtonPress:
                self._clear_session_gallery_edit_selection()
            if event.type() == QEvent.Resize:
                self._reposition_pending_raw_floating_tab()
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
        # Wheel/gesture events on the pending-RAW floating overlay would
        # otherwise be swallowed by the frame or its child buttons; forward
        # them to the image viewer so zoom keeps working while the overlay
        # is on screen.
        if event.type() in (QEvent.Wheel, QEvent.NativeGesture):
            forwarded = self._forward_gesture_event_to_viewer(obj, event)
            if forwarded:
                return True
        return super().eventFilter(obj, event)

    def _install_viewer_gesture_forwarding(self, widget) -> None:
        if widget is None:
            return
        widget.installEventFilter(self)
        try:
            widget.grabGesture(Qt.PinchGesture)
        except Exception:
            pass
        for child in widget.findChildren(QWidget):
            child.installEventFilter(self)

    def _forward_gesture_event_to_viewer(self, obj, event) -> bool:
        image_label = getattr(self, "live_image_label", None)
        if image_label is None:
            return False
        frames = [
            getattr(self, "pending_raw_frame", None),
            getattr(self, "calibration_action_frame", None),
            getattr(self, "capture_state_bar", None),
        ]
        for frame in frames:
            if frame is None:
                continue
            if obj is frame or (isinstance(obj, QWidget) and frame.isAncestorOf(obj)):
                if event.type() == QEvent.Wheel and isinstance(obj, QWidget):
                    # Re-anchor the wheel at the same point in viewer
                    # coordinates, so zoom centres where the pointer is.
                    position = QPointF(image_label.mapFromGlobal(obj.mapToGlobal(event.position().toPoint())))
                    event = QWheelEvent(
                        position,
                        event.globalPosition(),
                        event.pixelDelta(),
                        event.angleDelta(),
                        event.buttons(),
                        event.modifiers(),
                        event.phase(),
                        event.inverted(),
                        event.source(),
                    )
                QApplication.sendEvent(image_label, event)
                return True
        return False

    def _on_capture_state_bar_rebuilt(self) -> None:
        self._install_viewer_gesture_forwarding(self.capture_state_bar)
        self._reposition_pending_raw_floating_tab()

    def _contrast_shortcut_key(self, value) -> str | None:
        return self.CONTRAST_SHORTCUT_KEYS.get(str(value or ""))

    def _install_capture_shortcuts(self) -> None:
        """1-6 choose objectives, B/D/I/P contrast methods (the capture-state bar)."""
        self._capture_shortcuts = []
        for position, key in enumerate(self.OBJECTIVE_SHORTCUT_KEYS):
            shortcut = QShortcut(QKeySequence(key), self)
            shortcut.setContext(Qt.WidgetWithChildrenShortcut)
            shortcut.activated.connect(lambda _checked=False, p=position: self._choose_objective_by_position(p))
            self._capture_shortcuts.append(shortcut)
        for value, key in self.CONTRAST_SHORTCUT_KEYS.items():
            shortcut = QShortcut(QKeySequence(key), self)
            shortcut.setContext(Qt.WidgetWithChildrenShortcut)
            shortcut.activated.connect(lambda _checked=False, v=value: self._choose_contrast_by_shortcut(v))
            self._capture_shortcuts.append(shortcut)

    def _capture_shortcut_allowed(self) -> bool:
        focus = QApplication.focusWidget()
        if isinstance(focus, (QLineEdit, QTextEdit, QPlainTextEdit, QAbstractSpinBox)):
            return False
        if isinstance(focus, QComboBox) and focus.isEditable():
            return False
        return True

    def _choose_objective_by_position(self, position: int) -> None:
        if not self._capture_shortcut_allowed():
            return
        selector = self.objective_combo
        values = [
            selector.itemData(index)
            for index in range(selector.count())
            if not selector.is_unset_item(index)
        ]
        if 0 <= position < len(values) and (selector.selected_value() != values[position] or selector.is_mixed()):
            selector.set_selected_value(values[position], emit=True)

    def _choose_contrast_by_shortcut(self, value: str) -> None:
        if not self._capture_shortcut_allowed():
            return
        selector = self.contrast_combo
        if selector.findData(value) < 0:
            return
        if selector.selected_value() != value or selector.is_mixed():
            selector.set_selected_value(value, emit=True)
        self._on_contrast_chosen()

    def _reposition_pending_raw_floating_tab(self) -> None:
        image_label = getattr(self, "live_image_label", None)
        if image_label is None:
            return
        log_slot = getattr(self, "log_bar_slot", None)
        page_bar = getattr(self, "capture_state_bar", None)
        if log_slot is not None and page_bar is not None and page_bar.parentWidget() is log_slot:
            page_bar.fit_to_width(max(0, log_slot.width() - 8))  # Kun logg page: stack when narrow
        # Capture-state bar: bottom centre. The pending-RAW bar sits just above it.
        bottom = image_label.height() - 12
        bar = getattr(self, "capture_state_bar", None)
        if bar is not None and bar.parentWidget() is image_label:
            bar.fit_to_width(max(0, image_label.width() - 16))
            hint = bar.sizeHint()
            x = max(0, (image_label.width() - hint.width()) // 2)
            y = max(0, bottom - hint.height())
            readout_rect = getattr(image_label, "bottom_left_readout_rect", None)
            if callable(readout_rect):
                readout = readout_rect()
                if QRect(x, y, hint.width(), hint.height()).intersects(readout.adjusted(-6, -6, 6, 6)):
                    y = max(0, readout.top() - 8 - hint.height())  # sit above the zoom readout
            bar.setGeometry(x, y, hint.width(), hint.height())
            bar.raise_()
            if not bar.isHidden():
                bottom = y - 8
        frame = getattr(self, "pending_raw_frame", None)
        if frame is not None and frame.parentWidget() is image_label:
            hint = frame.sizeHint()
            width = max(hint.width(), frame.minimumSizeHint().width())
            height = max(hint.height(), frame.minimumSizeHint().height())
            x = max(0, (image_label.width() - width) // 2)
            y = max(0, bottom - height)
            frame.setGeometry(x, y, width, height)
            frame.raise_()
        calibration_frame = getattr(self, "calibration_action_frame", None)
        if calibration_frame is not None and calibration_frame.parentWidget() is image_label:
            hint = calibration_frame.sizeHint()
            width = max(hint.width(), calibration_frame.minimumSizeHint().width())
            height = max(hint.height(), calibration_frame.minimumSizeHint().height())
            # Top-right corner with a small inset.
            x = max(0, image_label.width() - width - 8)
            y = 8
            calibration_frame.setGeometry(x, y, width, height)
            calibration_frame.raise_()
        notice = getattr(self, "calibration_notice", None)
        if notice is not None and notice.parentWidget() is image_label:
            hint = notice.sizeHint()
            top = image_label.height() - 12 - hint.height()
            readout_rect = getattr(image_label, "bottom_left_readout_rect", None)
            if callable(readout_rect):
                top = readout_rect().top() - 8 - hint.height()
            notice.setGeometry(8, max(0, top), hint.width(), hint.height())
            notice.raise_()
            # A narrow viewer: lift the notice above the capture bar and the
            # pending-RAW bar, whichever it would cover.
            obstacles = [
                widget
                for widget in (getattr(self, "capture_state_bar", None), getattr(self, "pending_raw_frame", None))
                if widget is not None and widget.parentWidget() is image_label and not widget.isHidden()
            ]
            moved = True
            while moved and not notice.isHidden():
                moved = False
                for widget in obstacles:
                    if notice.geometry().intersects(widget.geometry()):
                        notice.move(8, max(0, widget.geometry().top() - 8 - hint.height()))
                        moved = notice.geometry().top() > 0
                        break
        state_label = getattr(self, "live_state_label", None)
        if state_label is not None and state_label.parentWidget() is image_label:
            state_label.adjustSize()
            state_label.move(8, 8)
            state_label.raise_()

    _SESSION_IMAGE_TOP_LEVEL_METADATA_KEYS = (
        ("objective_name", "objective_name"),
        ("contrast", "contrast"),
        ("mount_medium", "mount_medium"),
        ("stain", "stain"),
        ("sample_type", "sample_type"),
        ("sample_source", "sample_source"),
    )

    def _selected_session_image_ids_for_metadata_sync(self) -> list[int]:
        session_ids = set(int(v) for v in getattr(self, "_session_image_ids", []) or [])
        if not session_ids:
            return []
        collected: list[int] = []
        seen: set[int] = set()
        for key in self._session_gallery_selected_keys():
            # Skip pending-RAW keys — those are handled by the pending sync.
            if self._pending_raw_capture_index_for_key(key) is not None:
                continue
            try:
                image_id = int(key)
            except (TypeError, ValueError):
                continue
            if image_id <= 0 or image_id in seen or image_id not in session_ids:
                continue
            collected.append(image_id)
            seen.add(image_id)
        if collected:
            return collected
        # Fall back to the currently viewed session image when nothing is
        # multi-selected in the gallery.
        fallback = self._selected_committed_image_id()
        if fallback and fallback not in seen:
            return [int(fallback)]
        return []

    def _sync_selected_session_image_metadata_from_controls(self) -> bool:
        """Persist sidebar metadata changes to the selected saved session images."""
        # See _sync_selected_pending_raw_metadata_from_controls: without an
        # explicit user selection the sidebar is a staging area for the next
        # capture, not an edit surface for existing images.
        if not getattr(self, "_sidebar_binds_to_selection", False):
            refresh_state = getattr(self, "_refresh_session_microscope_state_from_sidebar", None)
            if callable(refresh_state):
                refresh_state()
            return False
        if getattr(self, "_raw_edit_session", None) is not None:
            return False
        image_ids = self._selected_session_image_ids_for_metadata_sync()
        if not image_ids:
            return False

        current_lab_metadata = getattr(self, "_current_lab_metadata", None)
        if not callable(current_lab_metadata):
            return False
        try:
            metadata_updates = dict(current_lab_metadata() or {})
        except Exception:
            return False
        # Skip fields the user hasn't disambiguated yet — a mixed selector
        # is showing "these images disagree on this field", and applying the
        # sidebar's placeholder value would silently collapse that spread.
        mixed_keys_fn = getattr(self, "_mixed_sidebar_field_keys", None)
        mixed_keys = mixed_keys_fn() if callable(mixed_keys_fn) else set()
        for key in mixed_keys:
            metadata_updates.pop(key, None)

        changed_any = False
        for image_id in image_ids:
            try:
                image = ImageDB.get_image(image_id)
            except Exception:
                image = None
            if not image:
                continue
            existing_metadata = image.get("lab_metadata") or {}
            merged = merge_image_lab_metadata(existing_metadata, metadata_updates)
            top_level_updates: dict[str, object] = {}
            for metadata_key, column_key in self._SESSION_IMAGE_TOP_LEVEL_METADATA_KEYS:
                if metadata_key in mixed_keys:
                    continue
                new_value = metadata_updates.get(metadata_key)
                if new_value is None:
                    continue
                if image.get(column_key) == new_value:
                    continue
                top_level_updates[column_key] = new_value
            metadata_changed = merged != existing_metadata
            if not metadata_changed and not top_level_updates:
                continue
            update_kwargs: dict[str, object] = dict(top_level_updates)
            if metadata_changed:
                update_kwargs["lab_metadata"] = merged
            try:
                ImageDB.update_image(int(image_id), **update_kwargs)
                changed_any = True
            except Exception:
                continue

        if changed_any:
            self._refresh_session_gallery()
            refresh_viewer_objective_tag = getattr(self, "_refresh_viewer_objective_tag_from_current_state", None)
            if callable(refresh_viewer_objective_tag):
                refresh_viewer_objective_tag()
            refresh_main = getattr(self, "_refresh_main_window_after_import", None)
            if callable(refresh_main):
                try:
                    refresh_main(image_ids[0])
                except Exception:
                    pass
        if changed_any:
            self._viewer_subject_metadata = None  # its row changed: read it again
            self._refresh_viewer_overlays()
        return changed_any

    def _session_gallery_items(self) -> tuple[list[dict[str, object]], str | int | None]:
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        # Chronological ordering: the oldest committed image sits on the left,
        # newer committed captures fill in to the right of it, and pending RAW
        # captures — the freshest photos in the session, not yet saved — hang
        # off the right edge in the order they arrived.
        items: list[dict[str, object]] = []
        pending_items, selected_pending_key = self._pending_raw_gallery_items()
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
        items.extend(pending_items)
        selected_key: str | int | None = selected_pending_key
        if selected_key is None and self._selected_session_image_id is not None:
            selected_key = self._selected_session_image_id
        _raw_timing_log("session gallery items rebuild", start, detail=f"items={len(items)}")
        return items, selected_key

    def _pending_raw_source_exif(
        self, pending: "PendingRawCapture"
    ) -> tuple[str | None, str | None, str | None]:
        """Return (make, model, capture_datetime) for a pending RAW capture.

        rawpy does not expose camera Make/Model, so we read them from the
        companion JPEG when present. Falls back to the pending capture's
        recorded source_capture_datetime for the timestamp.
        """
        make: str | None = None
        model: str | None = None
        capture_dt: str | None = str(getattr(pending, "source_capture_datetime", "") or "").strip() or None

        companion = getattr(pending, "companion_jpeg_path", None)
        companion_text = str(companion or "").strip()
        if companion_text and Path(companion_text).exists():
            try:
                from utils.exif_reader import get_exif_data
                exif = get_exif_data(companion_text)
            except Exception:
                exif = {}
            if isinstance(exif, dict):
                raw_make = exif.get("Make")
                raw_model = exif.get("Model")
                if isinstance(raw_make, bytes):
                    raw_make = raw_make.decode("utf-8", errors="ignore")
                if isinstance(raw_model, bytes):
                    raw_model = raw_model.decode("utf-8", errors="ignore")
                make = str(raw_make or "").strip() or None
                model = str(raw_model or "").strip() or None
                if not capture_dt:
                    for field in ("DateTimeOriginal", "DateTimeDigitized", "DateTime"):
                        value = exif.get(field)
                        if isinstance(value, bytes):
                            value = value.decode("utf-8", errors="ignore")
                        text = str(value or "").strip()
                        if text:
                            capture_dt = text
                            break

        return make, model, capture_dt

    def _prepare_calibration_source_image(self) -> tuple[str | None, bool]:
        """Return `(path, is_temp)` for the full-resolution image to calibrate.

        For pending RAW captures we render the RAW at full resolution using
        the user's current live settings (so their exposure, white balance,
        curve, etc. are applied) into a temporary JPEG. For saved session
        images we return the on-disk path directly. `is_temp` indicates
        whether the caller should delete the file after the dialog closes.
        """
        pending = self._current_pending_raw_capture()
        if pending is not None:
            source = getattr(pending, "source_path", None)
            source_text = str(source or "").strip()
            if source_text and Path(source_text).exists():
                try:
                    output_dir = Path(tempfile.gettempdir()) / "sporely" / "calibration_from_livelab"
                    output_dir.mkdir(parents=True, exist_ok=True)
                    from utils.raw_render import render_raw_image
                    make, model, capture_dt = self._pending_raw_source_exif(pending)
                    rendered = render_raw_image(
                        source_text,
                        settings=getattr(pending, "raw_settings", None),
                        output_dir=output_dir,
                        preview=False,
                        source_capture_datetime=capture_dt,
                        source_camera_make=make,
                        source_camera_model=model,
                    )
                    return str(rendered), True
                except Exception as exc:
                    self._show_status(
                        self.tr("Could not render RAW for calibration: {error}").format(error=exc),
                        tone="warning",
                        timeout_ms=5000,
                    )
                    return None, False
        image = self._selected_committed_image()
        if isinstance(image, dict):
            for key in ("filepath", "original_filepath"):
                candidate = str(image.get(key) or "").strip()
                if candidate and Path(candidate).exists():
                    return candidate, False
        label = getattr(self, "live_image_label", None)
        fallback = str(getattr(label, "_full_image_path", "") or "").strip()
        if fallback and Path(fallback).exists() and not is_raw_image_path(fallback):
            return fallback, False
        return None, False

    def _on_use_current_image_for_calibration(self) -> None:
        source_path, is_temp = self._prepare_calibration_source_image()
        if not source_path:
            self._show_status(
                self.tr("No image to send to the Calibration dialog."),
                tone="warning",
                timeout_ms=4000,
            )
            return

        # Preselect the objective the user has currently chosen in Live Lab so
        # the calibration lands on the right row without extra clicks.
        try:
            current_metadata = dict(self._current_lab_metadata() or {})
        except Exception:
            current_metadata = {}
        objective_key = str(current_metadata.get("objective_name") or "").strip() or None

        from .calibration_dialog import CalibrationDialog

        host = self.window()
        dialog = CalibrationDialog(host or self)
        dialog.select_custom_tab()
        if objective_key:
            dialog.select_objective_key(objective_key)
        else:
            dialog.clear_objective_selection()
        dialog._load_calibration_image_candidates([source_path])
        if host is not None and hasattr(host, "_on_calibration_saved_from_dialog"):
            try:
                dialog.calibration_saved.connect(host._on_calibration_saved_from_dialog)
            except Exception:
                pass
        try:
            dialog.exec()
        finally:
            if is_temp:
                try:
                    Path(source_path).unlink(missing_ok=True)
                except Exception:
                    pass

    def _selected_committed_session_image_ids_for_delete(self) -> list[int]:
        session_image_ids = list(getattr(self, "_session_image_ids", []) or [])
        session_id_set = {int(value) for value in session_image_ids}
        collected: list[int] = []
        seen: set[int] = set()
        for key in self._session_gallery_selected_keys():
            if self._pending_raw_capture_index_for_key(key) is not None:
                continue
            try:
                image_id = int(key)
            except (TypeError, ValueError):
                continue
            if image_id <= 0 or image_id in seen or image_id not in session_id_set:
                continue
            collected.append(image_id)
            seen.add(image_id)
        if collected:
            return collected
        selected_id = self._selected_committed_image_id()
        if selected_id is not None and int(selected_id) in session_id_set:
            return [int(selected_id)]
        return []

    def _delete_committed_session_images(self, image_ids: list[int]) -> bool:
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        session_image_ids = [int(value) for value in list(getattr(self, "_session_image_ids", []) or [])]
        requested_ids: list[int] = []
        seen: set[int] = set()
        for image_id in image_ids:
            try:
                normalized = int(image_id)
            except Exception:
                continue
            if normalized <= 0 or normalized in seen or normalized not in session_image_ids:
                continue
            requested_ids.append(normalized)
            seen.add(normalized)
        if not requested_ids:
            return False

        first_deleted_index = min((session_image_ids.index(image_id) for image_id in requested_ids), default=-1)
        current_pending_capture = self._current_pending_raw_capture()
        current_pending_index = self._selected_pending_raw_index_value()
        current_committed_id = self._selected_committed_image_id()
        image_names: list[str] = []
        deleted_ids: list[int] = []

        for image_id in requested_ids:
            image_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            get_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            image = ImageDB.get_image(image_id)
            _raw_timing_log("delete session image DB get_image", get_start, detail=f"image_id={image_id}")
            image_name = Path(str((image or {}).get("filepath") or "")).name or self.tr("selected image")
            try:
                delete_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
                ImageDB.delete_image(image_id)
                _raw_timing_log("delete session image DB/file delete", delete_start, detail=f"image_id={image_id}")
            except Exception as exc:
                self._show_status(
                    self.tr("Could not delete local processed image {name}: {error}").format(
                        name=image_name,
                        error=str(exc),
                    ),
                    tone="warning",
                    timeout_ms=6000,
                )
                continue
            image_names.append(image_name)
            deleted_ids.append(image_id)
            _raw_timing_log("delete session image item", image_start, detail=f"image_id={image_id}")

        if not deleted_ids:
            return False

        deleted_set = set(deleted_ids)
        self._session_image_ids = [existing_id for existing_id in session_image_ids if int(existing_id) not in deleted_set]

        if current_pending_capture is not None:
            if current_pending_index >= 0:
                self._selected_pending_raw_index = current_pending_index
            selected_image_id = None
        elif current_committed_id is not None and int(current_committed_id) in set(int(item) for item in self._session_image_ids):
            selected_image_id = int(current_committed_id)
            self._selected_session_image_id = selected_image_id
        elif self._session_image_ids:
            next_index = first_deleted_index if first_deleted_index >= 0 else 0
            if next_index >= len(self._session_image_ids):
                next_index = len(self._session_image_ids) - 1
            selected_image_id = int(self._session_image_ids[next_index])
            self._selected_session_image_id = selected_image_id
        else:
            selected_image_id = None
            self._selected_session_image_id = None

        gallery_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        self._refresh_session_gallery()
        _raw_timing_log("delete session images gallery refresh", gallery_start, detail=f"deleted={len(deleted_ids)}")

        if current_pending_capture is not None:
            show_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            if current_pending_index >= 0:
                self._show_pending_raw_capture(current_pending_index)
            else:
                self._show_pending_raw_capture(self._selected_pending_raw_index)
            _raw_timing_log("delete session images show pending", show_start)
        elif selected_image_id is not None:
            show_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            self._show_session_image(selected_image_id)
            _raw_timing_log("delete session images show selected", show_start, detail=f"image_id={selected_image_id}")
        else:
            clear_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            self._clear_session_viewer(
                title=self.tr("Waiting for first import"),
                meta=self.tr("New microscope captures from the watched folder will appear here automatically."),
            )
            _raw_timing_log("delete session images clear viewer", clear_start)

        controls_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        self._suppress_pending_raw_gallery_refresh = True
        try:
            self._update_pending_raw_controls()
        finally:
            self._suppress_pending_raw_gallery_refresh = False
        _raw_timing_log("delete session images pending controls", controls_start)

        main_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        self._refresh_main_window_for_live_lab_image_change(self._selected_session_image_id)
        _raw_timing_log("delete session images main window light refresh", main_start, detail=f"deleted={len(deleted_ids)}")

        if len(deleted_ids) == 1:
            message = self.tr("Deleted local processed image {name}.").format(name=image_names[0])
        else:
            message = self.tr("Deleted {count} local processed images.").format(count=len(deleted_ids))
        self._show_status(message, tone="success", timeout_ms=3500)
        _raw_timing_log("delete committed session images", start, detail=f"deleted={len(deleted_ids)}")
        return True

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
        pending_raw_exists = getattr(self, "_pending_raw_capture_exists", None)
        if callable(pending_raw_exists) and pending_raw_exists(source, group_key=group_key):
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
            return self.tr("Log only")
        return self.tr("Camera connected")

    def _restore_session_mode(self) -> None:
        saved = self._normalize_session_mode(SettingsDB.get_setting(self.SETTING_SESSION_MODE, self.SESSION_MODE_LIVE))
        selector = getattr(self, "session_mode_selector", None)
        if selector is not None:
            selector.set_selected_value(saved)
        update_notes = getattr(self, "_update_notes_summary", None)
        if callable(update_notes):
            update_notes()  # Notes differs between live and offline

    def _on_session_mode_changed(self) -> None:
        SettingsDB.set_setting(self.SETTING_SESSION_MODE, self._selected_session_mode())
        self._update_session_controls()
        update_notes = getattr(self, "_update_notes_summary", None)
        if callable(update_notes):
            update_notes()

    def _connect_session_logging_signals(self) -> None:
        # The Kun logg page shows the current settings (stage 9e).
        for combo in (self.objective_combo, self.contrast_combo, self.mount_combo, self.stain_combo):
            combo.currentIndexChanged.connect(lambda _index: self._update_log_page())
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
        self.objective_combo.currentIndexChanged.connect(self._sync_selected_session_image_metadata_from_controls)
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
        self.contrast_combo.currentIndexChanged.connect(self._sync_selected_session_image_metadata_from_controls)
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
        self.mount_combo.currentIndexChanged.connect(self._sync_selected_session_image_metadata_from_controls)
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
        self.stain_combo.currentIndexChanged.connect(self._sync_selected_session_image_metadata_from_controls)
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
        self.sample_combo.currentIndexChanged.connect(self._sync_selected_session_image_metadata_from_controls)
        self.sample_source_combo.currentIndexChanged.connect(
            lambda _idx: self._log_dropdown_change(
                "sample_source",
                self._selected_combo_value(self.sample_source_combo),
                self.sample_source_combo.currentText(),
            )
        )
        self.sample_source_combo.currentIndexChanged.connect(self._update_lab_state_combo_alerts)
        self.sample_source_combo.currentIndexChanged.connect(self._refresh_viewer_objective_tag_from_current_state)
        self.sample_source_combo.currentIndexChanged.connect(self._sync_selected_pending_raw_metadata_from_controls)
        self.sample_source_combo.currentIndexChanged.connect(self._sync_selected_session_image_metadata_from_controls)

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
            self._update_session_target_notice()
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
        if self.holds_shared_strip():
            self._refresh_session_gallery(reveal="preserve")

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

    def _update_session_target_notice(self) -> None:
        notice = getattr(self, "session_target_notice", None)
        if notice is None:
            return
        active = int(getattr(getattr(self, "_main_window", None), "active_observation_id", 0) or 0)
        target = int(self._target_observation_id or 0)
        notice.setVisible(bool(self.is_session_running() and target and active and active != target))

    def _update_target_display(self) -> None:
        self._update_session_target_notice()
        observation = self._target_observation
        location_label = getattr(self, "current_observation_location_label", None)
        if observation:
            vernacular = self._vernacular_name_text(observation) or "\u2014"
            scientific = self._scientific_name_text(observation) or "\u2014"
            date_text = str(observation.get("date") or "").strip() or "\u2014"
            location_text = str(observation.get("location") or "").strip()
            self.current_observation_name_label.setText(vernacular)
            self.current_observation_scientific_label.setText(scientific)
            self.current_observation_date_label.setText(
                self.tr("Date: {date}").format(date=date_text)
            )
            if location_label is not None:
                if location_text:
                    location_label.setText(self.tr("Location: {location}").format(location=location_text))
                    location_label.setVisible(True)
                else:
                    location_label.clear()
                    location_label.setVisible(False)
            self._update_observation_thumbnail()
        else:
            self.current_observation_name_label.setText(self.tr("No current observation selected"))
            self.current_observation_scientific_label.setText("\u2014")
            self.current_observation_date_label.setText(self.tr("Date: \u2014"))
            if location_label is not None:
                location_label.clear()
                location_label.setVisible(False)
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
        icon = self._recording_tab_icon if active else QIcon()
        tab_widget.setTabIcon(tab_index, icon)
        # The tab bar is hidden since stage 9a: the dot shows on the mode bar's Images.
        mode_bar = getattr(self._main_window, "mode_bar", None)
        if mode_bar is not None:
            mode_bar.set_page_icon(tab_index, icon)

    def _set_session_button_style(self, active: bool) -> None:
        self.start_stop_btn.setStyleSheet(
            self.SESSION_BUTTON_ACTIVE_STYLE if active else self.SESSION_BUTTON_BASE_STYLE
        )

    def _clear_observation_thumbnail(self) -> None:
        self.current_observation_thumb_label.clear()
        self.current_observation_thumb_label.setText(self.tr("No image"))

    def _update_observation_thumbnail(self) -> None:
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        observation_id = int(self._target_observation_id or 0)
        if observation_id <= 0:
            self._clear_observation_thumbnail()
            _raw_timing_log("update observation thumbnail", start, detail="no-observation")
            return

        db_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        images = ImageDB.get_images_for_observation(observation_id)
        _raw_timing_log("update observation thumbnail DB images", db_start, detail=f"observation_id={observation_id}")
        if not images:
            self._clear_observation_thumbnail()
            _raw_timing_log("update observation thumbnail", start, detail="no-images")
            return

        field_images = [img for img in images if str(img.get("image_type") or "").strip().lower() == "field"]
        image = field_images[-1] if field_images else images[-1]
        image_id = int(image.get("id") or 0)
        candidate_path = ""
        if image_id > 0:
            candidate_path = str(get_thumbnail_path(image_id, "small") or "").strip()
        if not candidate_path:
            candidate_path = str(image.get("filepath") or "").strip()
        if not candidate_path or not Path(candidate_path).exists():
            self._clear_observation_thumbnail()
            _raw_timing_log("update observation thumbnail", start, detail="missing-thumbnail")
            return

        pixmap_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        pixmap = QPixmap(candidate_path)
        if pixmap.isNull():
            reader = QImageReader(candidate_path)
            reader.setAutoTransform(True)
            image_data = reader.read()
            pixmap = QPixmap.fromImage(image_data) if not image_data.isNull() else QPixmap()
        _raw_timing_log("update observation thumbnail load pixmap", pixmap_start, detail=Path(candidate_path).name)
        if pixmap.isNull():
            self._clear_observation_thumbnail()
            _raw_timing_log("update observation thumbnail", start, detail="null-pixmap")
            return

        scale_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        scaled = pixmap.scaled(
            self.current_observation_thumb_label.size(),
            Qt.KeepAspectRatio,
            Qt.SmoothTransformation,
        )
        self.current_observation_thumb_label.setPixmap(scaled)
        self.current_observation_thumb_label.setText("")
        _raw_timing_log("update observation thumbnail scale/set", scale_start, detail=Path(candidate_path).name)
        _raw_timing_log("update observation thumbnail", start, detail=Path(candidate_path).name)

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
            "sample_source": DatabaseTerms.canonicalize(
                "sample_source",
                self._selected_combo_value(getattr(self, "sample_source_combo", None)),
            ),
            "sample_source_label": (
                str(self.sample_source_combo.currentText() or "").strip() or None
                if getattr(self, "sample_source_combo", None) is not None
                else None
            ),
        }

    def _refresh_session_microscope_state_from_sidebar(self) -> None:
        """Snapshot the current sidebar as the state to use for the next capture."""
        try:
            self._session_microscope_state = dict(self._current_lab_metadata() or {})
        except Exception:
            pass

    def _capture_time_metadata(self) -> dict:
        """Return the metadata to bake into a newly-captured image.

        Falls back to the sidebar when no session snapshot has been taken yet
        (e.g. the very first capture in a fresh session).
        """
        state = dict(getattr(self, "_session_microscope_state", None) or {})
        if state:
            return state
        try:
            return dict(self._current_lab_metadata() or {})
        except Exception:
            return {}

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
        if getattr(self, "_restoring_remembered_contrast", False) and attribute_name == "contrast":
            metadata["remembered"] = True
        self._log_session_event(
            "dropdown_change",
            attribute_name=attribute_name,
            value=value,
            metadata=metadata,
        )
        self._log_settings_since = datetime.now()
        self._update_log_page()
        update_notes = getattr(self, "_update_notes_summary", None)
        if callable(update_notes):
            update_notes(refresh_log=True)

    def _log_initial_lab_state(self) -> None:
        self._log_dropdown_change("objective_name", self._selected_combo_value(self.objective_combo), self.objective_combo.currentText())
        self._log_dropdown_change("contrast", self._selected_combo_value(self.contrast_combo), self.contrast_combo.currentText())
        self._log_dropdown_change("mount_medium", self._selected_combo_value(self.mount_combo), self.mount_combo.currentText())
        self._log_dropdown_change("stain", self._selected_combo_value(self.stain_combo), self.stain_combo.currentText())
        self._log_dropdown_change("sample_type", self._selected_combo_value(self.sample_combo), self.sample_combo.currentText())

    # -- session type, folder and running time (stage 9e) ---------------------

    def _build_session_box(self) -> QWidget:
        """Session type, the watched folder, running time and Start/End, in the observation card."""
        box = QWidget(self)
        layout = QVBoxLayout(box)
        layout.setContentsMargins(0, 8, 0, 0)
        layout.setSpacing(6)
        self.session_type_selector = SegmentedSelector(box, compact=True, fill_width=True)
        self.session_type_selector.add_option(self.tr("Camera connected"), self.SESSION_MODE_LIVE, checked=True)
        self.session_type_selector.add_option(self.tr("Log only"), self.SESSION_MODE_OFFLINE)
        self.session_type_selector.setToolTip(
            self.tr("The session type; the default is set in Preferences › Image import.")
        )
        self.session_type_selector.selectionChanged.connect(self._on_session_type_chosen)
        layout.addWidget(self.session_type_selector)

        self.session_folder_row = QWidget(box)
        folder_layout = QHBoxLayout(self.session_folder_row)
        folder_layout.setContentsMargins(0, 0, 0, 0)
        folder_layout.setSpacing(6)
        # A path keeps its start and end (the folder's name) when it is too long.
        self.session_folder_label = ElidedLabel(self.session_folder_row, text_setter=True, elide_mode=Qt.ElideMiddle)
        folder_layout.addWidget(self.session_folder_label, 1)
        self.session_folder_change_btn = QPushButton(self.tr("Change…"), self.session_folder_row)
        self.session_folder_change_btn.setObjectName("outlineButton")
        self.session_folder_change_btn.setToolTip(self.tr("Choose the folder the microscope camera saves into"))
        self.session_folder_change_btn.clicked.connect(self._choose_watch_dir)
        folder_layout.addWidget(self.session_folder_change_btn, 0)
        layout.addWidget(self.session_folder_row)

        control_row = QHBoxLayout()
        control_row.setContentsMargins(0, 0, 0, 0)
        control_row.setSpacing(8)
        self.session_clock_label = QLabel(box)
        self.session_clock_label.setStyleSheet("font-weight: 700;")
        self.session_clock_label.setVisible(False)
        control_row.addWidget(self.session_clock_label, 1)
        control_row.addWidget(self.start_stop_btn, 0, Qt.AlignRight)
        layout.addLayout(control_row)

        self._session_clock_started: float | None = None
        self._session_clock_timer = QTimer(self)
        self._session_clock_timer.setInterval(1000)
        self._session_clock_timer.timeout.connect(self._update_session_clock)
        return box

    # -- the Kun logg page (stage 9e) ------------------------------------------

    def _build_log_page(self) -> QWidget:
        page = QWidget(self)
        page.setObjectName("logOnlyPage")
        self.log_page = page
        page.setFocusPolicy(Qt.StrongFocus)  # a click here gives the 1–6 and B/D/I/P keys back
        layout = QVBoxLayout(page)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)

        self.log_settings_summary = QLabel(page)
        self.log_settings_summary.setStyleSheet("font-size: 20px; font-weight: 700;")
        self.log_settings_summary.setWordWrap(True)
        layout.addWidget(self.log_settings_summary)
        self.log_applies_label = QLabel(page)
        self.log_applies_label.setStyleSheet("font-weight: 600;")
        self.log_applies_label.setWordWrap(True)
        layout.addWidget(self.log_applies_label)
        hint = QLabel(
            self.tr("The next change is logged with its time and applied to the photos afterwards."), page
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: palette(placeholder-text);")
        layout.addWidget(hint)

        # The capture-state bar (objective, contrast) moves here from the image.
        self.log_bar_slot = QWidget(page)
        self.log_bar_slot.installEventFilter(self)
        bar_layout = QHBoxLayout(self.log_bar_slot)
        bar_layout.setContentsMargins(0, 4, 0, 4)
        bar_layout.addStretch(1)
        bar_layout.addStretch(1)
        layout.addWidget(self.log_bar_slot)

        cards = QHBoxLayout()
        cards.setSpacing(12)
        offset_card, offset_layout = create_section_card(self.tr("Camera time offset"), body_margins=(12, 10, 12, 12))
        self.log_time_offset_card = offset_card
        offset_row = QHBoxLayout()
        offset_row.setSpacing(12)
        self.log_qr_view = SyncShotQrView(180, offset_card)
        offset_row.addWidget(self.log_qr_view, 0, Qt.AlignTop)
        offset_text = QVBoxLayout()
        offset_hint = QLabel(
            self.tr(
                "Photograph the code with the microscope camera. Camera import then knows how far the camera clock is off."
            ),
            offset_card,
        )
        offset_hint.setWordWrap(True)
        offset_text.addWidget(offset_hint)
        self.log_full_screen_btn = QPushButton(self.tr("Full screen"), offset_card)
        self.log_full_screen_btn.setObjectName("outlineButton")
        self.log_full_screen_btn.clicked.connect(self._open_sync_shot_full_screen)
        offset_text.addWidget(self.log_full_screen_btn, 0, Qt.AlignLeft)
        offset_text.addStretch(1)
        offset_row.addLayout(offset_text, 1)
        offset_layout.addLayout(offset_row)
        cards.addWidget(offset_card, 1)

        after_card, after_layout = create_section_card(self.tr("Afterwards"), body_margins=(12, 10, 12, 12))
        for number, text in (
            (1, self.tr("End the session and insert the camera's SD card.")),
            (2, self.tr("Open Camera import: photos taken during the session are matched to this observation.")),
            (3, self.tr("Check the matches and add them to the observation.")),
        ):
            step = QLabel(f"<b>{number}</b>&nbsp;&nbsp;{text}", after_card)
            step.setWordWrap(True)
            after_layout.addWidget(step)
        after_layout.addStretch(1)
        cards.addWidget(after_card, 1)
        layout.addLayout(cards)
        layout.addStretch(1)
        return page

    def _open_sync_shot_full_screen(self) -> None:
        dialog = SyncShotDialog(self)
        dialog.showMaximized()
        dialog.exec()

    def _log_mode(self) -> bool:
        running = self.is_session_running() or bool(self._session_stop_pending)
        mode = self._active_session_mode if running else self._selected_session_mode()
        return self._normalize_session_mode(mode) == self.SESSION_MODE_OFFLINE

    def _log_page_wanted(self) -> bool:
        # A photo chosen in the strip is shown in the viewer, also in Kun logg.
        return self._log_mode() and getattr(self, "_viewer_subject", None) is None

    def _back_to_log_page(self) -> None:
        gallery = getattr(self, "session_gallery", None)
        if gallery is not None:
            gallery.select_image(None, center=False)
        self._sidebar_binds_to_selection = False
        self._clear_session_viewer()
        self._update_log_page()

    def _log_settings_summary_text(self) -> str:
        parts: list[str] = []
        objective_key = self._selected_combo_value(self.objective_combo)
        if objective_key:
            objective = load_objectives().get(str(objective_key))
            parts.append(objective_short_label(objective, str(objective_key)) or str(objective_key))
        for combo in (self.contrast_combo, self.mount_combo, self.stain_combo):
            value = self._selected_combo_value(combo)
            if value and str(value) != "Not_set":
                parts.append(combo.currentText())
        return " · ".join(parts) or self.tr("No settings chosen")

    def _update_log_page(self) -> None:
        stack = getattr(self, "viewer_stack", None)
        if stack is None:
            return
        wanted = self._log_page_wanted()
        bar = getattr(self, "capture_state_bar", None)
        if wanted:
            stack.setCurrentWidget(self.log_page)
            if bar is not None and bar.parentWidget() is not self.log_bar_slot:
                self.log_bar_slot.layout().insertWidget(1, bar)
                bar.show()
        else:
            stack.setCurrentWidget(self.viewer_panel)
            if bar is not None and bar.parentWidget() is not self.live_image_label:
                self.log_bar_slot.layout().removeWidget(bar)
                bar.setParent(self.live_image_label)
                bar.show()
                self._reposition_pending_raw_floating_tab()
        self.back_to_log_btn.setVisible(self._log_mode() and not wanted)
        running = self.is_session_running()
        self.log_settings_summary.setText(self._log_settings_summary_text())
        since = getattr(self, "_log_settings_since", None)
        if running and since is not None:
            self.log_applies_label.setText(
                self.tr("Applies to photos taken from {time}").format(time=since.strftime("%H:%M"))
            )
        else:
            self.log_applies_label.setText(self.tr("Start the session to log these settings with their time."))
        self.log_time_offset_card.setVisible(running)

    def _on_session_type_chosen(self, value) -> None:
        """Bilder's choice is the session mode (and the remembered default)."""
        if self.is_session_running() or self._session_stop_pending:
            return
        self.session_mode_selector.set_selected_value(value, emit=True)

    def _session_clock_text(self) -> str:
        started = getattr(self, "_session_clock_started", None)
        if started is None:
            return ""
        seconds = max(0, int(time.monotonic() - started))
        return f"{seconds // 3600:02d}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}"

    def _update_session_clock(self) -> None:
        label = getattr(self, "session_clock_label", None)
        running = self.is_session_running()
        clock = self._session_clock_text() if running else ""
        logging = self._normalize_session_mode(self._active_session_mode) == self.SESSION_MODE_OFFLINE
        if label is not None:
            label.setVisible(bool(clock))
            if clock:
                label.setText(
                    self.tr("Logging · {time}").format(time=clock) if logging else self.tr("Live · {time}").format(time=clock)
                )
        short = clock[:5] if clock else ""
        badge = ""
        if short:
            badge = self.tr("Logging {time}").format(time=short) if logging else self.tr("Live {time}").format(time=short)
        if badge != getattr(self, "_session_badge_text", ""):
            self._session_badge_text = badge
            self.sessionBadgeChanged.emit(badge)

    def session_badge_text(self) -> str:
        """“Live 00:14” / “Logging 00:14” while a session runs (the mode bar shows it)."""
        return getattr(self, "_session_badge_text", "")

    def _update_session_box(self, running: bool, stopping: bool, selected_mode: str, watch_ok: bool) -> None:
        selector = getattr(self, "session_type_selector", None)
        if selector is None:
            return
        with QSignalBlocker(selector):
            selector.set_selected_value(selected_mode)
        selector.setVisible(not running and not stopping)
        folder_shown = not running and not stopping and selected_mode == self.SESSION_MODE_LIVE
        self.session_folder_row.setVisible(folder_shown)
        folder = str(self.watch_dir_input.text() or "").strip()
        if folder and watch_ok:
            self.session_folder_label.setText(self.tr("Folder: {path}").format(path=folder))
            self.session_folder_label.setToolTip(folder)
            self.session_folder_label.setStyleSheet("color: palette(placeholder-text);")
        else:
            self.session_folder_label.setText(
                self.tr("Choose the folder the camera saves into") if not folder else self.tr("The folder no longer exists")
            )
            self.session_folder_label.setToolTip(folder)
            self.session_folder_label.setStyleSheet(f"color: {STAGING_BORDER}; font-weight: 600;")
        if running and self._session_clock_started is None:
            self._session_clock_started = time.monotonic()
        if running and not self._session_clock_timer.isActive():
            self._session_clock_timer.start()
        if not running:
            self._session_clock_timer.stop()
            self._session_clock_started = None
        self._update_session_clock()

    def _update_session_controls(self) -> None:
        running = self.is_session_running()
        stopping = bool(self._session_stop_pending)
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
        self.start_stop_btn.setEnabled(bool((running and not stopping) or can_start))
        self.session_note_input.setEnabled(bool(running and not stopping))
        self.add_note_btn.setEnabled(bool(running and not stopping))

        if stopping:
            self.start_stop_btn.setText(self.tr("Stopping..."))
        elif running:
            self.start_stop_btn.setText(self.tr("End session"))
        else:
            self.start_stop_btn.setText(
                self.tr("Start Log\nSession").replace("\n", " ")
                if selected_mode == self.SESSION_MODE_OFFLINE
                else self.tr("Start\nSession").replace("\n", " ")
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
        self._update_session_box(running, stopping, selected_mode, watch_ok)
        self._update_log_page()
        self._update_tab_recording_indicator(bool(running or stopping))
        self._reposition_pending_raw_floating_tab()

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
        # Seed the microscope-state shadow from the current sidebar; every
        # subsequent capture will refresh it. Sidebar starts unbound.
        self._sidebar_binds_to_selection = False
        try:
            self._session_microscope_state = dict(self._current_lab_metadata() or {})
        except Exception:
            self._session_microscope_state = {}
        self._session_id = uuid4().hex
        self._active_session_mode = selected_mode
        self._session_stop_pending = False
        self._session_stop_processed_raw_capture_ids.clear()
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
            status_text = self.tr("Camera session started for {name}.").format(
                name=self._observation_summary_text(self._session_observation_snapshot),
            )
        else:
            self._clear_session_viewer(
                title=self.tr("Log-only session"),
                meta=self.tr("Log microscope-state changes now and match images later in Camera import."),
            )
            status_text = self.tr("Log-only session started for {name}.").format(
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
        self._update_notes_summary(refresh_log=True)
        self._update_pending_raw_controls()
        if self._current_pending_raw_capture() is not None:
            self._show_pending_raw_capture(self._selected_pending_raw_index)

    def stop_session(self) -> None:
        if not self.is_session_running():
            return
        edit_session = getattr(self, "_raw_edit_session", None)
        if edit_session is not None and bool(getattr(edit_session, "dirty", False)):
            if not self._apply_raw_edit_session():
                return

        self._session_stop_pending = True
        cancel_btn = getattr(self, "pending_raw_commit_cancel_btn", None)
        if cancel_btn is not None:
            cancel_btn.setEnabled(False)
        if self._pending_raw_commit_is_busy():
            for capture in list(getattr(self, "_pending_raw_captures", []) or []):
                if self._pending_raw_commit_has_request_for_capture(capture):
                    self._session_stop_processed_raw_capture_ids.add(id(capture))
        watcher = self._watcher
        if watcher is None:
            self._continue_session_stop_after_raw_saves()
            return

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
        self._continue_session_stop_after_raw_saves()

    def _continue_session_stop_after_raw_saves(self) -> None:
        """Finish stopping only after every pending RAW capture has been processed."""
        if not bool(getattr(self, "_session_stop_pending", False)):
            return
        if self._watcher is not None:
            return
        if self._pending_raw_commit_is_busy():
            return
        processed_ids = getattr(self, "_session_stop_processed_raw_capture_ids", set())
        captures = [
            capture
            for capture in list(getattr(self, "_pending_raw_captures", []) or [])
            if id(capture) not in processed_ids
        ]
        if captures:
            processed_ids.update(id(capture) for capture in captures)
            if self._commit_pending_raw_capture_batch(captures):
                self._update_session_controls()
                return
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
        self._session_stop_processed_raw_capture_ids.clear()
        self._cancel_raw_edit_session(restore_selection=False)
        self._reset_companion_dedupe_state()
        self._update_session_controls()
        self._update_notes_summary(refresh_log=True)
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
                    self.tr("Log-only session ended. Logged {count} imported image(s).")
                    if session_mode == self.SESSION_MODE_OFFLINE
                    else self.tr("Session ended. Imported {count} image(s).")
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
            self._continue_session_stop_after_raw_saves()
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
        capture_time_state: dict[str, object] | None = None,
        observation_id: int | None = None,
    ) -> bool:
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        observation_id = int(observation_id or self._session_observation_id or 0)
        if observation_id <= 0:
            return False

        if capture_time_state is None:
            capture_time_helper = getattr(self, "_capture_time_metadata", None)
            if callable(capture_time_helper):
                try:
                    capture_time_state = capture_time_helper() or {}
                except Exception:
                    capture_time_state = self._current_lab_metadata()
            else:
                capture_time_state = self._current_lab_metadata()
        else:
            capture_time_state = dict(capture_time_state or {})
        ingest_lab_metadata = merge_image_lab_metadata(
            capture_time_state,
            lab_metadata,
            getattr(ingest, "lab_metadata", None),
        )
        objective_key = ingest_lab_metadata.get("objective_name")
        if objective_key is None or not str(objective_key).strip():
            objective_key = capture_time_state.get("objective_name") or self._selected_combo_value(self.objective_combo)
        contrast_value = ingest_lab_metadata.get("contrast")
        mount_value = ingest_lab_metadata.get("mount_medium")
        stain_value = ingest_lab_metadata.get("stain")
        sample_value = ingest_lab_metadata.get("sample_type")
        sample_source_value = ingest_lab_metadata.get("sample_source")
        if not str(contrast_value or "").strip():
            contrast_value = capture_time_state.get("contrast") or self._selected_combo_value(self.contrast_combo)
        if not str(mount_value or "").strip():
            mount_value = capture_time_state.get("mount_medium") or self._selected_combo_value(self.mount_combo)
        if not str(stain_value or "").strip():
            stain_value = capture_time_state.get("stain") or self._selected_combo_value(self.stain_combo)
        if not str(sample_value or "").strip():
            sample_value = capture_time_state.get("sample_type") or self._selected_combo_value(self.sample_combo)
        if not str(sample_source_value or "").strip():
            sample_source_value = capture_time_state.get("sample_source") or self._selected_combo_value(
                getattr(self, "sample_source_combo", None)
            )
        objective_key = str(objective_key or "").strip() or None
        objective = load_objectives().get(objective_key) if objective_key else None
        scale = objective.get("microns_per_pixel") if isinstance(objective, dict) else None
        calibration_id = CalibrationDB.get_active_calibration_id(objective_key) if objective_key else None
        original_filepath = ingest.original_path if ingest.original_path != ingest.working_path else None

        db_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
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
            sample_source=DatabaseTerms.canonicalize("sample_source", sample_source_value),
            calibration_id=calibration_id,
            resample_scale_factor=1.0,
            original_filepath=original_filepath,
            lab_metadata=ingest_lab_metadata or None,
            # The note typed for the capture becomes the image's own (shown in Mål).
            notes=str(ingest_lab_metadata.get("notes") or "").strip() or None,
            **ingest.provenance_kwargs(),
        )
        _raw_timing_log("RAW finalize DB add_image", db_start, detail=Path(str(source_path)).name)

        db_read_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        image_data = ImageDB.get_image(image_id)
        _raw_timing_log("RAW finalize DB get_image", db_read_start, detail=f"image_id={image_id}")
        stored_path = str((image_data or {}).get("filepath") or ingest.working_path)
        output_dir = get_images_dir() / "imports"
        warning_text = ""
        try:
            thumb_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            generate_all_sizes(stored_path, image_id)
            _raw_timing_log("RAW thumbnail generation", thumb_start, detail=Path(stored_path).name)
        except Exception as exc:
            warning_text = self.tr("Thumbnail generation warning for {name}: {error}").format(
                name=Path(stored_path).name,
                error=str(exc),
            )
        cleanup_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        cleanup_import_temp_file(source_path, ingest.working_path, stored_path, output_dir)
        _raw_timing_log("RAW finalize temp cleanup", cleanup_start, detail=Path(str(source_path)).name)

        self._session_image_ids.append(int(image_id))
        self._selected_session_image_id = int(image_id)
        self._session_import_count += 1
        defer_ui_refresh = self._pending_raw_commit_batch_mode == "save_all"
        # After a fresh capture the sidebar is no longer bound to any thumbnail
        # the user was inspecting — subsequent sidebar edits must be treated as
        # preparation for the next capture, not as edits to this new image.
        try:
            self._sidebar_binds_to_selection = False
            # Track the state this capture was actually taken with so it seeds
            # the next capture (and matches whatever `_show_session_image` will
            # now populate into the sidebar).
            self._session_microscope_state = dict(ingest_lab_metadata or {})
        except Exception:
            pass
        # A fresh capture ends any "select to edit" multi-selection — the
        # user's next action almost certainly targets the new thumbnail, not
        # the batch they were editing. Dropping the multi here also clears
        # the persistent hint via the selectionChanged emission.
        gallery = getattr(self, "session_gallery", None)
        if gallery is not None:
            exit_edit = getattr(gallery, "exit_edit_selection", None)
            if callable(exit_edit):
                exit_edit()
        # Ask the next gallery refresh to scroll so the fresh capture is
        # visible at the right edge of the strip.
        self._pending_reveal_new_at_end = True
        if not defer_ui_refresh:
            observation_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            self._update_observation_thumbnail()
            _raw_timing_log("RAW finalize observation thumbnail", observation_start, detail=Path(stored_path).name)
            gallery_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            self._refresh_session_gallery()
            _raw_timing_log("RAW finalize session gallery refresh", gallery_start, detail=Path(stored_path).name)
            show_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            self._show_session_image(image_id)
            _raw_timing_log("RAW finalize show session image", show_start, detail=f"image_id={image_id}")
            controls_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            self._update_session_controls()
            _raw_timing_log("RAW finalize session controls", controls_start, detail=Path(stored_path).name)

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
            settings_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            # Under the capture's own objective + contrast, not whatever the
            # side panel shows by the time a deferred save completes.
            capture_metadata = capture_time_state or lab_metadata or None
            save_helper = getattr(self, "_save_raw_processing_settings_for_current_context", None)
            if callable(save_helper):
                save_helper(raw_settings or self._current_raw_render_settings(), lab_metadata=capture_metadata)
            else:
                LiveLabTab._save_raw_processing_settings_for_current_context(
                    self,
                    raw_settings or self._current_raw_render_settings(),
                    lab_metadata=capture_metadata,
                )
            _raw_timing_log("RAW finalize settings save", settings_start, detail=Path(stored_path).name)
        if not defer_ui_refresh:
            main_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            self._refresh_main_window_after_import(image_id)
            _raw_timing_log("RAW finalize main window refresh", main_start, detail=f"image_id={image_id}")
        _raw_timing_log("RAW finalize local ingest", start, detail=Path(str(source_path)).name)
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
        capture_time_helper = getattr(self, "_capture_time_metadata", None)
        if callable(capture_time_helper):
            try:
                capture_time_state = capture_time_helper() or {}
            except Exception:
                capture_time_state = self._current_lab_metadata()
        else:
            capture_time_state = self._current_lab_metadata()
        ingest_context = merge_image_lab_metadata(capture_time_state, lab_metadata)
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
                observation_id=observation_id,
            )
        return LiveLabTab._finalize_local_ingest(
            self,
            source_path,
            ingest,
            raw_settings=resolved_raw_settings,
            lab_metadata=ingest_context,
            observation_id=observation_id,
        )

    # -- Bilder's sources: Live and Files and SD card (stage 9f) ----------------

    SOURCE_LIVE = "live"
    SOURCE_FILES = "files"

    def add_files_source(self, page: QWidget) -> None:
        """Bilder gets a second source next to Live: Files and SD card (``page``)."""
        root = self._root_layout
        root.removeWidget(self._live_content)
        self.source_selector = SegmentedSelector(self, compact=True)
        self.source_selector.add_option(self.tr("Live (microscope)"), self.SOURCE_LIVE, checked=True)
        self.source_selector.add_option(self.tr("Files and SD card"), self.SOURCE_FILES)
        self.source_selector.selectionChanged.connect(self._on_source_chosen)
        source_row = QHBoxLayout()
        source_row.setContentsMargins(0, 0, 0, 0)
        source_row.addWidget(self.source_selector, 0)
        self._files_header_actions = list(getattr(page, "header_actions", lambda: [])())
        for widget in self._files_header_actions:
            source_row.addWidget(widget, 0)
            widget.setVisible(False)
        source_row.addStretch(1)
        root.insertLayout(0, source_row)
        self.source_stack = QStackedWidget(self)
        self.source_stack.addWidget(self._live_content)
        self.source_stack.addWidget(page)
        root.insertWidget(1, self.source_stack, 1)
        self._files_source_page = page
        strip = getattr(self, "_shared_strip", None)
        if strip is not None and hasattr(page, "use_shared_strip"):
            page.use_shared_strip(strip)

    def current_source(self) -> str:
        selector = getattr(self, "source_selector", None)
        return selector.selected_value(self.SOURCE_LIVE) if selector is not None else self.SOURCE_LIVE

    def show_source(self, source: str) -> None:
        selector = getattr(self, "source_selector", None)
        if selector is None:
            return
        selector.set_selected_value(source, emit=True)

    def _on_source_chosen(self, value) -> None:
        files = value == self.SOURCE_FILES
        self.source_stack.setCurrentIndex(1 if files else 0)
        for widget in getattr(self, "_files_header_actions", []):
            widget.setVisible(files)
        self._hint_area.setVisible(not files)  # the Files editor has its own hint bar
        # Live's keys (RAW review: arrows, Delete, Enter, Esc; objectives 1-6,
        # contrast B/D/I/P) are bound to the whole page: with Files shown they
        # would act on Live's captures and make the editor's keys ambiguous.
        for shortcut in [*getattr(self, "_raw_review_shortcuts", []), *getattr(self, "_capture_shortcuts", [])]:
            shortcut.setEnabled(not files)
        page = getattr(self, "_files_source_page", None)
        if page is not None and (self.holds_shared_strip() or page.holds_shared_strip()):
            self.reclaim_shared_strip()  # the strip follows the source shown
        self.sourceChanged.emit(str(value))

    # -- the shared strip (stage 9d) ------------------------------------------

    def adopt_shared_strip(self, strip) -> None:
        """Use the main window's strip (shared with View and Measure) from now on."""
        own = self.session_gallery
        self._own_session_gallery = own
        self._shared_strip = strip
        channel = strip.channel("live", accepts_staged=True)
        channel.imageClicked.connect(self._on_session_gallery_clicked)
        channel.selectionChanged.connect(self._on_session_gallery_selection_changed)
        channel.itemsReordered.connect(self._on_session_gallery_items_reordered)
        channel.deleteImagesRequested.connect(self._on_session_gallery_delete_images_requested)
        channel.editRequested.connect(self._on_session_gallery_edit_requested)
        layout = self.strip_slot.layout()
        layout.removeWidget(own)
        own.hide()
        if self._hint_controller is not None:
            self._register_hint_widget(
                strip,
                self.tr("Unsaved captures come first, with an orange border; save them with Save to observation."),
            )
        self.session_gallery = _LiveAwayStrip(strip)

    def _unsaved_count_text(self, count: int) -> str:
        # Here, not in the RAW mixin: a plural tr() from a mixin method finds no translation.
        return self.tr("%n not saved", "", int(count))

    def unsaved_counts(self) -> dict[int, int]:
        """Unsaved captures per observation (the observation list shows them)."""
        counts: dict[int, int] = {}
        for capture in getattr(self, "_pending_raw_captures", []) or []:
            observation_id = int(getattr(capture, "observation_id", 0) or 0)
            if observation_id > 0:
                counts[observation_id] = counts.get(observation_id, 0) + 1
        return counts

    def holds_shared_strip(self) -> bool:
        strip = getattr(self, "_shared_strip", None)
        return strip is not None and self.session_gallery is strip

    def reclaim_shared_strip(self) -> None:
        """Images shows: the strip shows the session's observation, unsaved captures first.

        With Files and SD card as the source, Files holds it instead (stage 9f3).
        """
        strip = getattr(self, "_shared_strip", None)
        if strip is None:
            return
        files = getattr(self, "_files_source_page", None)
        if files is not None and self.current_source() == self.SOURCE_FILES:
            self._release_live_strip()
            files.claim_shared_strip()
            return
        if files is not None:
            files.release_shared_strip()
        away = self.session_gallery if isinstance(self.session_gallery, _LiveAwayStrip) else None
        strip.set_active_channel("live")
        strip.set_context_actions(
            change_settings=False, move_to_observation=False, delete_cloud_copy=False, publish_checkbox=False,
        )
        strip.set_page_behaviour(
            toggle_on_plain_click=True, clear_on_background_click=True, reveal_mode="nudge", reorderable=False,
        )
        layout = self.strip_slot.layout()
        if layout.indexOf(strip) < 0:
            layout.addWidget(strip, 1)
        strip.show()
        self.session_gallery = strip
        # Live Lab's own selection, not the one the other page left.
        if away is not None and away.selected_paths():
            strip.select_paths(away.selected_paths(), center=False)
        else:
            keys = list(away.selected_keys()) if away is not None else []
            strip.select_image(keys[0] if len(keys) == 1 else None, center=False)
        self._refresh_session_gallery(reveal="preserve")

    def release_shared_strip(self) -> None:
        """Another page takes the strip from Bilder (Live or Files)."""
        files = getattr(self, "_files_source_page", None)
        if files is not None:
            files.release_shared_strip()
        self._release_live_strip()

    def _release_live_strip(self) -> None:
        """Remember Live Lab's selection, leave none behind."""
        strip = getattr(self, "_shared_strip", None)
        if strip is None or self.session_gallery is not strip:
            return
        self.session_gallery = _LiveAwayStrip(strip, strip.selected_keys(), strip.selected_paths())
        strip.select_image(None, center=False)

    def _refresh_shared_strip(self, *, reveal: str) -> None:
        """The session's observation, its unsaved captures first (shared strip)."""
        strip = self._shared_strip
        pending_items, selected_pending_key = self._pending_raw_gallery_items()
        observation_of = {
            self._pending_raw_gallery_key(capture): int(getattr(capture, "observation_id", 0) or 0)
            for capture in getattr(self, "_pending_raw_captures", []) or []
        }
        by_observation: dict[int, list[dict]] = {}
        for item in pending_items:
            observation_id = observation_of.get(item.get("id"), 0) or int(self._target_observation_id or 0)
            if observation_id > 0:
                by_observation.setdefault(observation_id, []).append(item)
        # When Images holds the strip it renders once below, after the update.
        render = not self.holds_shared_strip()
        known = set(getattr(self, "_staged_observation_ids", set()))
        for observation_id in known - set(by_observation):
            self.session_gallery.set_staged_items(observation_id, [], render=render)
        for observation_id, items in by_observation.items():
            self.session_gallery.set_staged_items(observation_id, items, render=render)
        self._staged_observation_ids = set(by_observation)
        emitter = getattr(self, "unsavedCountsChanged", None)
        if emitter is not None:
            emitter.emit(self.unsaved_counts())
        if not self.holds_shared_strip():
            self.session_gallery.set_items()  # marks it to refresh when Images shows
            return
        target = int(self._target_observation_id or 0)
        if target <= 0:
            strip.clear()
            return
        # Saved images change too (a save adds one), so reload them each time.
        strip.set_observation_id(target, reveal="preserve")
        if len(strip.selected_keys()) > 1:
            return  # a multi-selection survives the reload (set_items keeps it)
        selected_key = selected_pending_key
        if selected_key is None and self._selected_session_image_id is not None:
            selected_key = self._selected_session_image_id
        if selected_key is not None and getattr(self, "_sidebar_binds_to_selection", False):
            strip.select_image(selected_key, center=reveal == "new_at_end")
        elif selected_pending_key is not None and reveal == "new_at_end":
            strip.select_image(selected_pending_key, center=True)

    def _refresh_session_gallery(self, *, reveal: str = "preserve") -> None:
        # A pending "new capture arrived" flag overrides the caller's mode.
        # It's set once by the ingest paths and consumed on the next refresh
        # — subsequent refreshes fall back to the caller's reveal (or the
        # default "preserve").
        if getattr(self, "_pending_reveal_new_at_end", False):
            reveal = "new_at_end"
            self._pending_reveal_new_at_end = False
        if getattr(self, "_shared_strip", None) is not None:
            self._refresh_shared_strip(reveal=reveal)
            update_hint = getattr(self, "_update_edit_selection_hint", None)
            if callable(update_hint):
                update_hint()
            return
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
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
        items_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        items, selected_key = self._session_gallery_items()
        _raw_timing_log("refresh session gallery build items", items_start, detail=f"items={len(items)}")
        set_items_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        # Metadata-only refreshes preserve the horizontal scroll offset so
        # the view doesn't jitter every time a badge updates. Ingest paths
        # override this with reveal="new_at_end" to scroll the strip so the
        # fresh capture is visible on the right edge.
        set_items_fn = getattr(gallery, "set_items", None)
        if callable(set_items_fn):
            try:
                set_items_fn(items, reveal=reveal)
            except TypeError:
                # Legacy signature without the reveal kwarg (test mocks).
                set_items_fn(items, preserve_scroll=(reveal == "preserve"))
        _raw_timing_log("refresh session gallery set_items", set_items_start, detail=f"items={len(items)}")
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
                select_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
                select_paths(selected_paths, center=False)
                _raw_timing_log("refresh session gallery restore selected paths", select_start, detail=f"paths={len(selected_paths)}")
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
                        select_paths(fallback_paths, center=False)
        elif selected_key is not None and getattr(self, "_sidebar_binds_to_selection", False):
            select_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            gallery.select_image(selected_key, center=False)
            _raw_timing_log("refresh session gallery select image", select_start, detail=str(selected_key))
        update_hint = getattr(self, "_update_edit_selection_hint", None)
        if callable(update_hint):
            update_hint()
        _raw_timing_log("refresh session gallery", start, detail=f"items={len(items)}")

    def _on_session_gallery_clicked(self, image_id, _path: str) -> None:
        gallery = getattr(self, "session_gallery", None)
        is_multi_select = False
        clicked_key = image_id if image_id is not None else _path
        clicked_selected = True
        selected_keys: set[object] = set()
        if gallery is not None:
            is_multi_select_fn = getattr(gallery, "is_multi_select", None)
            if callable(is_multi_select_fn):
                try:
                    is_multi_select = bool(is_multi_select_fn())
                except Exception:
                    is_multi_select = False
            selected_keys_fn = getattr(gallery, "selected_keys", None)
            if callable(selected_keys_fn):
                try:
                    selected_keys = set(selected_keys_fn())
                    clicked_selected = clicked_key in selected_keys
                except Exception:
                    clicked_selected = True
        # A user click is an explicit selection; sidebar edits from now on
        # should apply to the clicked image(s) rather than the next capture.
        self._sidebar_binds_to_selection = (
            bool(selected_keys) if is_multi_select else bool(clicked_selected)
        ) and LiveLabTab._selection_is_this_sessions(self)  # images saved earlier are view-only (stage 9d)
        self._raw_panel_shows_next_capture = False
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
                multi_metadatas = (
                    self._collect_multi_select_metadatas() if is_multi_select else []
                )
                if len(multi_metadatas) > 1:
                    self._apply_microscope_multi_state_to_controls(multi_metadatas)
                    self._apply_raw_multi_state_to_controls()
                else:
                    apply_microscope_state = getattr(self, "_apply_microscope_state_to_controls", None)
                    if callable(apply_microscope_state):
                        apply_microscope_state(capture.lab_metadata)
            # ImageGalleryWidget has already updated selection and queued its
            # edge nudge before emitting imageClicked. Rebuilding the gallery
            # from _update_pending_raw_controls here would cancel that nudge
            # and restore a stale pixel offset after thumbnail geometry moves.
            selected_keys_fn = getattr(gallery, "selected_keys", None) if gallery is not None else None
            selected_keys: set[object] = set()
            if callable(selected_keys_fn):
                try:
                    selected_keys = set(selected_keys_fn())
                except Exception:
                    selected_keys = set()
            suppress_refresh = clicked_key in selected_keys
            previous_suppression = bool(getattr(self, "_suppress_pending_raw_gallery_refresh", False))
            if suppress_refresh:
                self._suppress_pending_raw_gallery_refresh = True
            try:
                self._show_pending_raw_capture(pending_index)
            finally:
                if suppress_refresh:
                    self._suppress_pending_raw_gallery_refresh = previous_suppression
            return
        resolved_image_id = clicked_image_id
        if resolved_image_id <= 0:
            return
        self._selected_pending_raw_index = -1
        self._selected_session_image_id = resolved_image_id
        self._update_pending_raw_controls()
        if is_multi_select:
            multi_metadatas = self._collect_multi_select_metadatas()
            if len(multi_metadatas) > 1:
                self._apply_microscope_multi_state_to_controls(multi_metadatas)
                self._apply_raw_multi_state_to_controls()
            else:
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

    def _clear_session_gallery_edit_selection(self) -> None:
        gallery = getattr(self, "session_gallery", None)
        if gallery is not None:
            exit_edit = getattr(gallery, "exit_edit_selection", None)
            if callable(exit_edit):
                exit_edit()
        self._sidebar_binds_to_selection = False

    def _on_session_gallery_edit_requested(self, _image_id, filepath: str) -> None:
        # Route to the Observations tab's Prepare Images dialog, same as the
        # Observations-panel "Edit photo" menu.
        path = (filepath or "").strip() or None
        if not path:
            return
        observations_tab = getattr(self._main_window, "observations_tab", None) if getattr(self, "_main_window", None) else None
        if observations_tab is None:
            return
        opener = getattr(observations_tab, "open_edit_images_direct", None)
        if callable(opener):
            opener(selected_image_path=path)

    def _selection_is_this_sessions(self) -> bool:
        """Shared strip: the selection holds an unsaved capture or one saved this session.

        Images also shows images saved earlier (stage 9d); they are view-only
        here, so the capture bar keeps editing the next capture.
        """
        if getattr(self, "_shared_strip", None) is None:
            return True
        session_ids = {int(value) for value in getattr(self, "_session_image_ids", []) or []}
        for key in self._session_gallery_selected_keys():
            if isinstance(key, int):
                if key in session_ids:
                    return True
            elif self._pending_raw_capture_index_for_key(key) is not None:
                return True
        return False

    def _on_session_gallery_selection_changed(self, selected_paths: list[str]) -> None:
        try:
            selected_count = len([path for path in selected_paths or [] if path])
        except Exception:
            selected_count = 0
        if selected_count and not LiveLabTab._selection_is_this_sessions(self):
            selected_count = 0  # view-only: edits go to the next capture
        # Multi-selection or clearing the selection are both user actions.
        # Any surviving selection keeps the sidebar bound to those images; an
        # empty selection releases it so future edits queue for the next
        # capture instead of mutating whatever was previously highlighted.
        self._sidebar_binds_to_selection = selected_count > 0
        if selected_count > 0:
            self._raw_panel_shows_next_capture = False
        self._update_edit_selection_hint(selected_count)
        if selected_count > 1:
            metadatas = self._collect_multi_select_metadatas()
            if len(metadatas) > 1:
                self._apply_microscope_multi_state_to_controls(metadatas)
            self._apply_raw_multi_state_to_controls()
        else:
            # Any lingering "mixed" state from a prior multi-select must be
            # cleared once we're down to zero or one item.
            for combo_name, _field in self._MICROSCOPE_SIDEBAR_FIELDS:
                combo = getattr(self, combo_name, None)
                if combo is None:
                    continue
                clear_mixed = getattr(combo, "clear_mixed", None)
                if callable(clear_mixed):
                    clear_mixed()
            controls = getattr(self, "raw_controls", None)
            if controls is not None:
                clear_mixed_raw = getattr(controls, "_clear_mixed_state", None)
                if callable(clear_mixed_raw):
                    clear_mixed_raw()
                set_multi_selection_mode = getattr(controls, "set_multi_selection_mode", None)
                if callable(set_multi_selection_mode):
                    set_multi_selection_mode(False)
        self._update_raw_processing_visibility()

    def _update_edit_selection_hint(self, selected_count: int | None = None) -> None:
        """Show a persistent hint whenever the gallery has 2+ thumbnails
        selected for editing. Clear the hint as soon as the selection
        collapses back to 0 or 1."""
        controller = getattr(self, "_hint_controller", None)
        if controller is None:
            return
        if selected_count is None:
            try:
                selected_count = len(self._session_gallery_selected_keys())
            except Exception:
                selected_count = 0
        active = int(selected_count or 0) > 1
        previous = bool(getattr(self, "_edit_selection_hint_active", False))
        if active == previous:
            return
        self._edit_selection_hint_active = active
        if active:
            controller.set_hint(self.tr("Selected images will be edited"), tone="info")
        else:
            controller.set_hint("")

    def _on_session_gallery_items_reordered(self, ordered_keys) -> None:
        # Session gallery mixes committed session images (int DB ids) with
        # pending RAW captures (string keys "pending:..."). Committed images
        # get persisted through ImageDB; pending captures get their in-memory
        # list rebuilt so a subsequent commit lands them in the desired order.
        committed_ids: list[int] = []
        pending_keys: list[str] = []
        for key in ordered_keys or []:
            if isinstance(key, int):
                committed_ids.append(key)
                continue
            key_text = str(key or "")
            if key_text.startswith("pending:"):
                pending_keys.append(key_text)
                continue
            try:
                committed_ids.append(int(key_text))
            except (TypeError, ValueError):
                continue

        if committed_ids:
            obs_id = int(self._session_observation_id or 0)
            if obs_id > 0:
                try:
                    ImageDB.reorder_images(obs_id, committed_ids)
                except Exception:
                    pass
            existing = set(int(v) for v in getattr(self, "_session_image_ids", []) or [])
            reordered_committed = [int(v) for v in committed_ids if int(v) in existing]
            for image_id in getattr(self, "_session_image_ids", []) or []:
                if int(image_id) not in reordered_committed:
                    reordered_committed.append(int(image_id))
            self._session_image_ids = reordered_committed

        if pending_keys:
            captures_by_key = {}
            for capture in getattr(self, "_pending_raw_captures", []) or []:
                if not isinstance(capture, PendingRawCapture):
                    continue
                key = self._pending_raw_gallery_key(capture)
                if key is not None:
                    captures_by_key[str(key)] = capture
            reordered_captures = []
            seen = set()
            for pending_key in pending_keys:
                capture = captures_by_key.get(pending_key)
                if capture is None or id(capture) in seen:
                    continue
                reordered_captures.append(capture)
                seen.add(id(capture))
            for capture in getattr(self, "_pending_raw_captures", []) or []:
                if isinstance(capture, PendingRawCapture) and id(capture) not in seen:
                    reordered_captures.append(capture)
                    seen.add(id(capture))
            if reordered_captures:
                self._pending_raw_captures = reordered_captures
                current = self._current_pending_raw_capture()
                if current is not None and current in reordered_captures:
                    self._selected_pending_raw_index = reordered_captures.index(current)

        self._refresh_session_gallery()

    def _on_session_gallery_delete_images_requested(self, keys) -> None:
        keys_list = list(keys or [])
        if not keys_list:
            return
        if len(keys_list) > 1:
            self._delete_session_gallery_items(keys_list)
            return
        self._on_session_gallery_delete_requested(keys_list[0])

    def _on_session_gallery_delete_requested(self, key) -> None:
        if isinstance(key, (list, tuple, set)):
            if self._delete_session_gallery_items(list(key)):
                return
        if self._delete_session_gallery_item(key):
            return

    def _delete_session_gallery_items(self, keys: list[object]) -> bool:
        pending_indices: list[int] = []
        committed_ids: list[int] = []
        for key in keys or []:
            pending_index = self._pending_raw_capture_index_for_key(key)
            if pending_index is not None:
                if pending_index not in pending_indices:
                    pending_indices.append(int(pending_index))
                continue
            try:
                image_id = int(key or 0)
            except Exception:
                image_id = 0
            if image_id > 0 and image_id not in committed_ids:
                committed_ids.append(image_id)

        deleted = False
        for pending_index in sorted(pending_indices, reverse=True):
            deleted = self._delete_session_gallery_pending_capture(int(pending_index)) or deleted
        self._note_earlier_images_not_deleted(committed_ids)
        if committed_ids:
            deleted = self._delete_committed_session_images(committed_ids) or deleted
        return deleted

    def _note_earlier_images_not_deleted(self, image_ids) -> None:
        """Images shows images saved before this session too; it deletes only this session's."""
        session_ids = {int(value) for value in getattr(self, "_session_image_ids", []) or []}
        if any(int(image_id) not in session_ids for image_id in image_ids or []):
            self._show_status(
                self.tr("Images saved before this session are deleted in View or Measure."),
                tone="info",
                timeout_ms=5000,
            )

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
        self._note_earlier_images_not_deleted([image_id])
        return self._delete_session_gallery_committed_image(image_id)

    def _delete_session_gallery_pending_capture(self, pending_index: int) -> bool:
        captures = list(getattr(self, "_pending_raw_captures", []) or [])
        if pending_index < 0 or pending_index >= len(captures):
            return False
        capture = captures[pending_index]
        if not isinstance(capture, PendingRawCapture):
            return False
        if self._pending_raw_commit_capture_is_busy(capture):
            self._show_status(
                self.tr("Wait for RAW saving to finish before removing this capture."),
                tone="info",
                timeout_ms=3000,
            )
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
        return self._delete_committed_session_images([int(image_id)])

    def _clear_session_viewer(self, title: str | None = None, meta: str | None = None) -> None:
        self.viewer_title_label.setText(title or self.tr("Last import"))
        self.viewer_meta_label.setText(meta or self.tr("The next imported microscope image will appear here."))
        self.reset_view_btn.setEnabled(False)
        self.live_image_label.set_microns_per_pixel(0.0)
        self.live_image_label.set_scale_bar(False, 0.0)
        self.live_image_label.set_image(None)
        frame = getattr(self, "calibration_action_frame", None)
        if frame is not None:
            frame.setVisible(False)
        # An empty viewer waits for the next capture: its overlays and the RAW
        # panel describe the next capture's state.
        self._viewer_subject = None
        apply_raw = getattr(self, "_apply_raw_settings_for_combination", None)
        if callable(apply_raw):
            apply_raw()
        self._viewer_subject_metadata = None
        refresh_overlays = getattr(self, "_refresh_viewer_overlays", None)
        if callable(refresh_overlays):
            refresh_overlays()
        update_raw_processing_visibility = getattr(self, "_update_raw_processing_visibility", None)
        if callable(update_raw_processing_visibility):
            update_raw_processing_visibility()

    def _show_session_image(self, image_id: int) -> None:
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        self._raw_panel_shows_next_capture = False  # the panel follows the image on screen
        db_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        image = ImageDB.get_image(image_id)
        _raw_timing_log("show session image DB get_image", db_start, detail=f"image_id={image_id}")
        if not image:
            self._clear_session_viewer()
            self._update_raw_edit_controls()
            _raw_timing_log("show session image", start, detail=f"image_id={image_id} missing")
            return
        self._selected_session_image_id = int(image_id)
        self._viewer_subject = ("image", int(image_id))
        self._viewer_subject_metadata = dict(image)

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
            apply_state_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            apply_microscope_state(image)
            _raw_timing_log("show session image apply microscope state", apply_state_start, detail=f"image_id={image_id}")
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
            load_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            pixmap, preview_scaled = self._load_viewer_pixmap(image_path)
            _raw_timing_log("show session image load viewer pixmap", load_start, detail=Path(image_path).name)
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
        set_image_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        self.live_image_label.set_image_sources(
            pixmap,
            image_path,
            preview_scaled,
            preserve_view=preserve_view,
        )
        _raw_timing_log("show session image set viewer image", set_image_start, detail=Path(image_path).name)
        prune_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        self._prune_pending_raw_preview_buffers(clear_proxy_cache=not edit_mode)
        _raw_timing_log("show session image prune RAW buffers", prune_start, detail=Path(image_path).name)
        self.reset_view_btn.setEnabled(True)
        update_calibration_action_visibility = getattr(self, "_update_calibration_action_visibility", None)
        if callable(update_calibration_action_visibility):
            update_calibration_action_visibility()

        mpp_value = self._image_microns_per_pixel(image)
        self.live_image_label.set_microns_per_pixel(mpp_value or 0.0)
        if mpp_value:
            scale_bar_value, scale_bar_unit = self._viewer_scale_bar_config(image)
            scale_bar_microns = float(scale_bar_value) * 1000.0 if scale_bar_unit == "mm" else float(scale_bar_value)
            self.live_image_label.set_scale_bar(True, scale_bar_microns, unit=scale_bar_unit)
        else:
            self.live_image_label.set_scale_bar(False, 0.0)
        self._refresh_viewer_overlays()

        if edit_mode:
            self.viewer_title_label.setText(
                self.tr("Editing RAW: {name}").format(name=edit_session.source_raw_path.name)
            )
            self.viewer_meta_label.setText(self._raw_settings_info_text(edit_session.working_settings))
            auto_settings = None
            edit_cache_key = self._raw_preview_proxy_cache_key(
                edit_session.source_raw_path,
                edit_session.working_settings,
            )
            edit_cache = getattr(self, "_pending_raw_preview_proxy_cache", None)
            if (
                edit_session.source_raw_path.exists()
                and _pending_raw_preview_cache_has_entry(edit_cache, edit_cache_key)
            ):
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
                sync_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
                self._sync_raw_processing_controls_from_settings(
                    editable_session.working_settings,
                    update_session_settings=False,
                    auto_level_settings=None,
                )
                _raw_timing_log("show session image sync RAW controls", sync_start, detail=Path(image_path).name)
        controls_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        self._update_raw_edit_controls()
        _raw_timing_log("show session image update RAW edit controls", controls_start, detail=Path(image_path).name)
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

    def _refresh_main_window_for_live_lab_image_change(self, image_id: int | None) -> None:
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        observation_id = int(self._session_observation_id or 0)
        if int(getattr(self._main_window, "active_observation_id", 0) or 0) != observation_id:
            _raw_timing_log("main window light image refresh", start, detail="inactive-observation")
            return
        try:
            refresh_images = getattr(self._main_window, "refresh_observation_images", None)
            if callable(refresh_images):
                images_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
                on_measure = getattr(self._main_window, "_strip_on_measure", None)
                if callable(on_measure) and on_measure() and getattr(self, "_shared_strip", None) is not None:
                    # Measure holds the shared strip (stage 9d): keep what is open and selected there.
                    refresh_images(
                        select_image_id=getattr(self._main_window, "current_image_id", None),
                        preserve_selection=True,
                    )
                else:
                    refresh_images(select_image_id=image_id)
                _raw_timing_log("main window refresh observation images", images_start, detail=f"image_id={image_id}")
            update_measurements = getattr(self._main_window, "update_measurements_table", None)
            if callable(update_measurements):
                measurements_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
                update_measurements()
                _raw_timing_log("main window update measurements", measurements_start, detail=f"image_id={image_id}")
            is_analysis_visible = getattr(self._main_window, "is_analysis_visible", None)
            schedule_refresh = getattr(self._main_window, "schedule_gallery_refresh", None)
            if callable(is_analysis_visible) and is_analysis_visible() and callable(schedule_refresh):
                schedule_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
                schedule_refresh()
                _raw_timing_log("main window schedule analysis gallery", schedule_start, detail=f"image_id={image_id}")
        except Exception:
            pass
        _raw_timing_log("main window light image refresh", start, detail=f"image_id={image_id}")

    def _refresh_main_window_after_import(self, image_id: int, *, full_observations_refresh: bool = False) -> None:
        if full_observations_refresh:
            start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            try:
                observations_tab = getattr(self._main_window, "observations_tab", None)
                refresh_observations = getattr(observations_tab, "refresh_observations", None)
                if callable(refresh_observations):
                    obs_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
                    refresh_observations(show_status=False)
                    _raw_timing_log("main window refresh observations", obs_start, detail=f"image_id={image_id}")
            except Exception:
                pass
            _raw_timing_log("main window full observations refresh", start, detail=f"image_id={image_id}")
        self._refresh_main_window_for_live_lab_image_change(image_id)

    def closeEvent(self, event) -> None:
        self.shutdown()
        super().closeEvent(event)
