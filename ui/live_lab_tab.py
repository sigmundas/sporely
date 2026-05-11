"""Live microscopy session tab."""
from __future__ import annotations

from uuid import uuid4
from pathlib import Path

from PySide6.QtCore import QSize, Qt, QUrl
from PySide6.QtGui import QColor, QDesktopServices, QIcon, QImageReader, QPainter, QPixmap
from PySide6.QtWidgets import (
    QFileDialog,
    QFrame,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSplitter,
    QVBoxLayout,
    QWidget,
)

from database.database_tags import DatabaseTerms
from database.models import CalibrationDB, ImageDB, ObservationDB, SessionLogDB, SettingsDB
from database.schema import get_images_dir, load_objectives, objective_display_name, objective_sort_value
from utils.heic_converter import maybe_convert_heic
from utils.image_utils import cleanup_import_temp_file
from utils.lab_watcher import LabWatcherWorker
from utils.thumbnail_generator import generate_all_sizes, get_thumbnail_path

from .hint_status import HintBar, HintStatusController
from .image_gallery_widget import ImageGalleryWidget
from .section_card import create_section_card
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


class LiveLabTab(QWidget):
    """Watch a capture folder and ingest new microscope images into one observation."""

    SETTING_WATCH_DIR = "live_lab_watch_dir"
    SETTING_LAST_OBJECTIVE = "live_lab_last_objective"
    SETTING_SESSION_MODE = "live_lab_session_mode"
    SETTING_MAIN_SPLITTER = "live_lab_main_splitter_sizes"
    SETTING_CONTENT_SPLITTER = "live_lab_content_splitter_sizes"
    SESSION_MODE_LIVE = "live"
    SESSION_MODE_OFFLINE = "offline"
    VIEWER_PREVIEW_MAX_DIM = 2400
    VIEWER_SCALE_BAR_UM = 10.0
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
        self._seen_source_paths: set[str] = set()
        self._session_import_count = 0
        self._session_stop_pending = False
        self._pending_stop_status: tuple[str, str, int] | None = None
        self._recording_tab_icon = self._build_recording_tab_icon()
        self._default_hint_text = self.tr(
            "Watch your microscope capture folder here. Scroll to zoom, drag to pan, and click a session thumbnail to inspect it."
        )
        self._build_ui()
        self._populate_objective_combo()
        self._restore_term_selection(self.contrast_combo, "contrast")
        self._restore_term_selection(self.mount_combo, "mount")
        self._restore_term_selection(self.stain_combo, "stain")
        self._restore_term_selection(self.sample_combo, "sample")
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
        self.session_mode_combo = self._make_combo()
        self.session_mode_combo.addItem(self.tr("Live capture (watch folder)"), self.SESSION_MODE_LIVE)
        self.session_mode_combo.addItem(self.tr("Retrospective session (log only)"), self.SESSION_MODE_OFFLINE)
        self.session_mode_combo.currentIndexChanged.connect(self._on_session_mode_changed)
        mode_layout.addWidget(self.session_mode_combo)
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

        self.live_image_label = ZoomableImageLabel()
        self.live_image_label.setObjectName("liveLabImageLabel")
        self.live_image_label.setMinimumSize(320, 240)
        self.live_image_label.set_pan_without_shift(True)
        self.live_image_label.set_measurement_active(False)
        self.live_image_label.set_show_measure_labels(False)
        self.live_image_label.set_show_measure_overlays(False)
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
            self.tr("Click a thumbnail to inspect it here. The newest import is highlighted automatically."),
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
        if hasattr(self, "session_mode_combo"):
            return self._normalize_session_mode(self.session_mode_combo.currentData())
        return self.SESSION_MODE_LIVE

    def _session_mode_label(self, mode: str | None = None) -> str:
        normalized = self._normalize_session_mode(mode or self._selected_session_mode())
        if normalized == self.SESSION_MODE_OFFLINE:
            return self.tr("Retrospective session")
        return self.tr("Live capture")

    def _restore_session_mode(self) -> None:
        saved = self._normalize_session_mode(SettingsDB.get_setting(self.SETTING_SESSION_MODE, self.SESSION_MODE_LIVE))
        index = self.session_mode_combo.findData(saved)
        if index >= 0:
            self.session_mode_combo.setCurrentIndex(index)

    def _on_session_mode_changed(self, _index: int) -> None:
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
                title=self.tr("Retrospective session"),
                meta=self.tr("Log microscope-state changes now and match images later in the Ingestion Hub."),
            )
            status_text = self.tr("Retrospective session started for {name}.").format(
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
        self._update_session_controls()

        if self._pending_stop_status is not None:
            message, tone, timeout_ms = self._pending_stop_status
            self._pending_stop_status = None
            self._show_status(message, tone=tone, timeout_ms=timeout_ms)
            return

        if had_session:
            self._show_status(
                (
                    self.tr("Retrospective session stopped. Logged {count} imported image(s).")
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
        self._seen_source_paths.add(source)
        self._ingest_detected_image(source)

    def _ingest_detected_image(self, source_path: str) -> None:
        observation_id = int(self._session_observation_id or 0)
        if observation_id <= 0:
            return

        output_dir = get_images_dir() / "imports"
        output_dir.mkdir(parents=True, exist_ok=True)
        converted_path = maybe_convert_heic(source_path, output_dir)
        if converted_path is None:
            self._show_status(
                self.tr("HEIC conversion failed for {name}.").format(name=Path(source_path).name),
                tone="warning",
                timeout_ms=6000,
            )
            return

        objective_key = self.objective_combo.currentData()
        objective = load_objectives().get(objective_key) if objective_key else None
        scale = objective.get("microns_per_pixel") if isinstance(objective, dict) else None
        calibration_id = CalibrationDB.get_active_calibration_id(objective_key) if objective_key else None

        image_id = ImageDB.add_image(
            observation_id=observation_id,
            filepath=converted_path,
            image_type="microscope",
            scale=scale,
            objective_name=objective_key,
            contrast=DatabaseTerms.canonicalize("contrast", self.contrast_combo.currentData()),
            mount_medium=DatabaseTerms.canonicalize("mount", self.mount_combo.currentData()),
            stain=DatabaseTerms.canonicalize("stain", self.stain_combo.currentData()),
            sample_type=DatabaseTerms.canonicalize("sample", self.sample_combo.currentData()),
            calibration_id=calibration_id,
            resample_scale_factor=1.0,
            lab_metadata=self._current_lab_metadata(),
        )

        image_data = ImageDB.get_image(image_id)
        stored_path = str((image_data or {}).get("filepath") or converted_path)
        warning_text = ""
        try:
            generate_all_sizes(stored_path, image_id)
        except Exception as exc:
            warning_text = self.tr("Thumbnail generation warning for {name}: {error}").format(
                name=Path(stored_path).name,
                error=str(exc),
            )
        cleanup_import_temp_file(source_path, converted_path, stored_path, output_dir)

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
        self._refresh_main_window_after_import(image_id)

    def _refresh_session_gallery(self) -> None:
        objectives = load_objectives()
        items = []
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
            items.append(
                {
                    "id": image_id,
                    "filepath": image.get("filepath"),
                    "image_number": idx,
                    "has_measurements": False,
                    "badges": badges,
                }
            )
        self.session_gallery.set_items(items)
        if self._selected_session_image_id is not None:
            self.session_gallery.select_image(self._selected_session_image_id)

    def _on_session_gallery_clicked(self, image_id, _path: str) -> None:
        try:
            resolved_image_id = int(image_id or 0)
        except Exception:
            resolved_image_id = 0
        if resolved_image_id <= 0:
            return
        self._selected_session_image_id = resolved_image_id
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
            return

        image_path = str(image.get("filepath") or "").strip()
        if not image_path:
            self._clear_session_viewer(
                title=self.tr("Selected image unavailable"),
                meta=self.tr("This session image does not have a stored file path."),
            )
            return

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
