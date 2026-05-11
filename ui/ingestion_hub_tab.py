"""Retrospective image ingestion tab driven by session logs."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from PySide6.QtCore import QSize, Qt, QUrl
from PySide6.QtGui import QDesktopServices, QImageReader, QPixmap
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
    QFileDialog,
    QDoubleSpinBox,
    QFrame,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSplitter,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from database.database_tags import DatabaseTerms
from database.models import CalibrationDB, ImageDB, ObservationDB, SessionLogDB, SettingsDB
from database.schema import get_images_dir, load_objectives, objective_display_name, resolve_objective_key
from utils.heic_converter import maybe_convert_heic
from utils.image_utils import cleanup_import_temp_file
from utils.sync_shot_qr import choose_sync_shot_offset, decode_sync_shot_qr
from utils.temporal_matcher import TemporalMatcher
from utils.thumbnail_generator import generate_all_sizes

from .hint_status import HintBar, HintStatusController, style_progress_widgets
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
from .sync_shot_dialog import SyncShotDialog
from .zoomable_image_widget import ZoomableImageLabel


class IngestionHubTab(QWidget):
    """Match mixed field and microscope imports to existing observations."""

    SETTING_SCAN_DIR = "ingestion_hub_scan_dir"
    SETTING_FIELD_MATCH_TOLERANCE_SECONDS = "ingestion_hub_field_match_tolerance_seconds"
    SETTING_OFFSET_SECONDS = "ingestion_hub_offset_seconds"
    SETTING_MAIN_SPLITTER = "ingestion_hub_main_splitter_sizes"
    SETTING_CONTENT_SPLITTER = "ingestion_hub_content_splitter_sizes"
    VIEWER_PREVIEW_MAX_DIM = 2400

    def __init__(self, main_window, parent=None) -> None:
        super().__init__(parent)
        self._main_window = main_window
        self._matcher = TemporalMatcher(session_kind="offline")
        self._sessions: list = []
        self._batch_images: list[dict] = []
        self._matches_by_observation: dict[int, list[dict]] = {}
        self._match_result: dict = {"matches": [], "unmatched": [], "observation_counts": {}}
        self._excluded_paths: set[str] = set()
        self._sync_shot_record: dict | None = None
        self._sync_shot_image_path: str | None = None
        self._selected_match_path: str | None = None

        self._build_ui()
        self._restore_scan_dir()
        self._restore_offset()
        self.refresh_observation_queue()
        self.sync_from_active_observation()
        self._update_sync_shot_summary()
        self._update_action_state()
        self._set_hint(
            self.tr(
                "Scan a folder with field and microscope images, optionally create a Sync Shot QR, "
                "then let Sporely auto-check the first and last image from each folder before matching "
                "field photos to nearby observation times and microscope photos to retrospective sessions."
            )
        )

    def _build_ui(self) -> None:
        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(10, 10, 10, 10)
        root_layout.setSpacing(8)

        main_splitter = QSplitter(Qt.Horizontal)
        main_splitter.setChildrenCollapsible(False)
        root_layout.addWidget(main_splitter, 1)

        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(10)

        queue_group, queue_layout = create_section_card(
            self.tr("Microscope sessions"),
            body_margins=(10, 12, 10, 10),
        )
        self.observation_queue = QListWidget()
        self.observation_queue.currentItemChanged.connect(self._on_observation_changed)
        queue_layout.addWidget(self.observation_queue, 1)
        self.queue_summary_label = QLabel("")
        self.queue_summary_label.setWordWrap(True)
        self.queue_summary_label.setStyleSheet("color: #6b7280;")
        queue_layout.addWidget(self.queue_summary_label)
        left_layout.addWidget(queue_group, 1)

        folder_group, folder_layout = create_section_card(
            self.tr("Import folder"),
            body_margins=(10, 12, 10, 10),
        )
        self.scan_dir_input = QLineEdit()
        self.scan_dir_input.setPlaceholderText(self.tr("Choose a folder of field and microscope images"))
        self.scan_dir_input.textChanged.connect(self._on_scan_dir_changed)
        folder_layout.addWidget(self.scan_dir_input)
        folder_button_row = QHBoxLayout()
        folder_button_row.setContentsMargins(0, 0, 0, 0)
        folder_button_row.setSpacing(8)
        self.browse_scan_dir_btn = QPushButton(self.tr("Browse"))
        self.browse_scan_dir_btn.clicked.connect(self._choose_scan_dir)
        folder_button_row.addWidget(self.browse_scan_dir_btn)
        self.scan_folder_btn = QPushButton(self.tr("Scan folder"))
        self.scan_folder_btn.clicked.connect(self._scan_folder)
        folder_button_row.addWidget(self.scan_folder_btn)
        folder_layout.addLayout(folder_button_row)
        left_layout.addWidget(folder_group)

        sync_group, sync_layout = create_section_card(
            self.tr("Sync Shot"),
            body_margins=(10, 12, 10, 10),
        )
        self.sync_shot_summary_label = QLabel("")
        self.sync_shot_summary_label.setWordWrap(True)
        self.sync_shot_summary_label.setStyleSheet("color: #4b5563;")
        sync_layout.addWidget(self.sync_shot_summary_label)
        offset_row = QHBoxLayout()
        offset_row.setContentsMargins(0, 0, 0, 0)
        offset_row.setSpacing(8)
        offset_row.addWidget(QLabel(self.tr("Offset (s):")))
        self.offset_spin = QDoubleSpinBox()
        self.offset_spin.setRange(-86400.0, 86400.0)
        self.offset_spin.setDecimals(1)
        self.offset_spin.setSingleStep(0.5)
        self.offset_spin.valueChanged.connect(self._on_offset_changed)
        offset_row.addWidget(self.offset_spin, 1)
        sync_layout.addLayout(offset_row)
        sync_button_row = QHBoxLayout()
        sync_button_row.setContentsMargins(0, 0, 0, 0)
        sync_button_row.setSpacing(8)
        self.new_sync_shot_btn = QPushButton(self.tr("New Sync Shot"))
        self.new_sync_shot_btn.clicked.connect(self._create_sync_shot)
        sync_button_row.addWidget(self.new_sync_shot_btn)
        self.apply_sync_shot_btn = QPushButton(self.tr("Use image..."))
        self.apply_sync_shot_btn.clicked.connect(self._apply_sync_shot_from_batch_image)
        sync_button_row.addWidget(self.apply_sync_shot_btn)
        self.clear_sync_shot_btn = QPushButton(self.tr("Clear"))
        self.clear_sync_shot_btn.clicked.connect(self._clear_sync_shot)
        sync_button_row.addWidget(self.clear_sync_shot_btn)
        sync_layout.addLayout(sync_button_row)
        left_layout.addWidget(sync_group)

        actions_group, actions_layout = create_section_card(
            self.tr("Actions"),
            body_margins=(10, 12, 10, 10),
        )
        self.batch_summary_label = QLabel("")
        self.batch_summary_label.setWordWrap(True)
        self.batch_summary_label.setStyleSheet("color: #4b5563;")
        actions_layout.addWidget(self.batch_summary_label)
        self.refresh_matches_btn = QPushButton(self.tr("Refresh matches"))
        self.refresh_matches_btn.clicked.connect(self._recompute_matches)
        actions_layout.addWidget(self.refresh_matches_btn)
        self.commit_matches_btn = QPushButton(self.tr("Add selected image"))
        self.commit_matches_btn.clicked.connect(self._commit_selected_matches)
        actions_layout.addWidget(self.commit_matches_btn)
        self.add_all_images_btn = QPushButton(self.tr("Add all images"))
        self.add_all_images_btn.clicked.connect(self._add_all_matches)
        actions_layout.addWidget(self.add_all_images_btn)
        left_layout.addWidget(actions_group)
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
        self.viewer_title_label = QLabel(self.tr("Matched image"))
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

        self.image_viewer = ZoomableImageLabel()
        self.image_viewer.setMinimumSize(320, 240)
        self.image_viewer.set_pan_without_shift(True)
        self.image_viewer.set_measurement_active(False)
        self.image_viewer.set_show_measure_labels(False)
        self.image_viewer.set_show_measure_overlays(False)
        viewer_layout.addWidget(self.image_viewer, 1)

        self.staging_gallery = ImageGalleryWidget(
            self.tr("Staging grid"),
            parent=self,
            show_delete=False,
            show_badges=True,
            thumbnail_size=132,
            default_height=GALLERY_DEFAULT_HEIGHT,
            min_height=GALLERY_MIN_HEIGHT,
        )
        self.staging_gallery.set_multi_select(True)
        self.staging_gallery.imageClicked.connect(self._on_gallery_clicked)
        self.staging_gallery.selectionChanged.connect(self._on_gallery_selection_changed)

        content_splitter = QSplitter(Qt.Vertical)
        content_splitter.setObjectName("gallerySplitter")
        content_splitter.setChildrenCollapsible(False)
        content_splitter.addWidget(viewer_panel)
        content_splitter.addWidget(self.staging_gallery)
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

        self.hint_progress_widget = QWidget(self)
        hint_progress_layout = QHBoxLayout(self.hint_progress_widget)
        hint_progress_layout.setContentsMargins(0, 0, 0, 0)
        hint_progress_layout.setSpacing(0)
        progress_stack = QWidget(self.hint_progress_widget)
        progress_stack_layout = QVBoxLayout(progress_stack)
        progress_stack_layout.setContentsMargins(0, 0, 0, 0)
        progress_stack_layout.setSpacing(4)
        self.hint_progress_bar = QProgressBar(self)
        self.hint_progress_bar.setRange(0, 100)
        self.hint_progress_bar.setValue(0)
        self.hint_progress_bar.setTextVisible(True)
        self.hint_progress_bar.setFixedHeight(18)
        self.hint_progress_bar.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.hint_progress_status = QLabel("")
        self.hint_progress_status.setWordWrap(True)
        self.hint_progress_status.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        self.hint_progress_status.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        style_progress_widgets(self.hint_progress_bar, self.hint_progress_status)
        progress_stack_layout.addWidget(self.hint_progress_bar, 0)
        progress_stack_layout.addWidget(self.hint_progress_status, 0)
        hint_progress_layout.addWidget(progress_stack, 1)
        self.hint_progress_widget.setVisible(False)
        root_layout.addWidget(self.hint_progress_widget)

        self._clear_viewer()

    def _set_hint(self, text: str | None, tone: str = "info") -> None:
        if self._hint_controller is not None:
            self._hint_controller.set_hint(text, tone=tone)

    def _show_status(self, text: str | None, tone: str = "info", timeout_ms: int = 4000) -> None:
        if self._hint_controller is not None:
            self._hint_controller.set_status(text, timeout_ms=timeout_ms, tone=tone)

    def _set_hint_progress_visible(self, visible: bool) -> None:
        if hasattr(self, "hint_bar"):
            self.hint_bar.setVisible(not bool(visible))
        if hasattr(self, "hint_progress_widget"):
            self.hint_progress_widget.setVisible(bool(visible))

    def _set_hint_progress(self, status_text: str | None, value: int | None = None) -> None:
        if hasattr(self, "hint_progress_status"):
            self.hint_progress_status.setText((status_text or "").strip())
        if value is not None and hasattr(self, "hint_progress_bar"):
            self.hint_progress_bar.setValue(int(max(0, min(100, value))))
        app = QApplication.instance()
        if app is not None:
            app.processEvents()

    def _restore_scan_dir(self) -> None:
        saved = str(SettingsDB.get_setting(self.SETTING_SCAN_DIR, "") or "").strip()
        if saved:
            self.scan_dir_input.setText(saved)

    def _restore_offset(self) -> None:
        raw = SettingsDB.get_setting(self.SETTING_OFFSET_SECONDS, 0.0)
        try:
            offset = float(raw or 0.0)
        except (TypeError, ValueError):
            offset = 0.0
        self.offset_spin.blockSignals(True)
        self.offset_spin.setValue(offset)
        self.offset_spin.blockSignals(False)

    def _on_scan_dir_changed(self, text: str) -> None:
        SettingsDB.set_setting(self.SETTING_SCAN_DIR, str(text or "").strip())
        self._update_action_state()

    def _field_match_tolerance_seconds(self) -> float:
        raw = SettingsDB.get_setting(self.SETTING_FIELD_MATCH_TOLERANCE_SECONDS, 60)
        try:
            return max(0.0, float(raw))
        except (TypeError, ValueError):
            return 60.0

    @staticmethod
    def _parse_observation_datetime(value) -> datetime | None:
        if isinstance(value, datetime):
            return value
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text)
        except Exception:
            pass
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None

    def sync_from_active_observation(self) -> None:
        obs_id = int(getattr(self._main_window, "active_observation_id", 0) or 0)
        if obs_id > 0:
            self._set_selected_observation(obs_id)

    def refresh_observation_queue(self, select_observation_id: int | None = None) -> None:
        self._sessions = self._matcher.load_sessions()
        current_obs_id = select_observation_id if select_observation_id is not None else self._selected_observation_id()
        current_match_counts = dict(self._match_result.get("observation_counts") or {})

        grouped: dict[int, dict] = {}
        for session in self._sessions:
            summary = grouped.setdefault(
                session.observation_id,
                {
                    "session_count": 0,
                    "latest": None,
                },
            )
            summary["session_count"] += 1
            latest = session.last_recorded_at or session.ended_at or session.started_at
            if latest and (summary["latest"] is None or latest > summary["latest"]):
                summary["latest"] = latest
        for obs_id in current_match_counts:
            summary = grouped.setdefault(
                int(obs_id),
                {
                    "session_count": 0,
                    "latest": None,
                },
            )
            if summary.get("latest") is None:
                observation = ObservationDB.get_observation(int(obs_id))
                observation_dt = self._parse_observation_datetime((observation or {}).get("date"))
                if observation_dt is not None:
                    summary["latest"] = observation_dt
        observation_ids = list(grouped.keys())
        observation_ids.sort(
            key=lambda obs_id: (
                -(int(current_match_counts.get(obs_id, 0) or 0)),
                -(grouped.get(obs_id, {}).get("latest") or datetime.min).timestamp()
                if grouped.get(obs_id, {}).get("latest")
                else float("-inf"),
                -obs_id,
            )
        )

        self.observation_queue.blockSignals(True)
        self.observation_queue.clear()
        for obs_id in observation_ids:
            observation = ObservationDB.get_observation(obs_id)
            if not observation:
                continue
            summary = grouped.get(obs_id, {})
            session_count = int(summary.get("session_count") or 0)
            match_count = int(current_match_counts.get(obs_id, 0) or 0)
            title = self._observation_title(observation)
            subtitle_parts = [str(observation.get("date") or "").strip() or self.tr("No date")]
            subtitle_parts.append(
                self.tr("{count} session(s)").format(count=session_count)
            )
            subtitle_parts.append(
                self.tr("{count} match(es)").format(count=match_count)
            )
            item = QListWidgetItem()
            item.setData(Qt.UserRole, obs_id)
            self.observation_queue.addItem(item)
            row_widget = self._make_observation_queue_row(
                obs_id,
                f"{title}\n" + " • ".join(subtitle_parts),
                session_count,
            )
            item.setSizeHint(row_widget.sizeHint())
            self.observation_queue.setItemWidget(item, row_widget)
        self.observation_queue.blockSignals(False)

        if self.observation_queue.count() == 0:
            self.queue_summary_label.setText(
                self.tr("No matching observations or retrospective microscope sessions were found yet.")
            )
            self._refresh_gallery()
            self._update_action_state()
            return

        self.queue_summary_label.setText("")
        if current_obs_id:
            self._set_selected_observation(current_obs_id)
        elif self.observation_queue.currentItem() is None and self.observation_queue.count() > 0:
            self.observation_queue.setCurrentRow(0)
        self._update_action_state()

    def _make_observation_queue_row(self, observation_id: int, text: str, session_count: int) -> QWidget:
        row_widget = QWidget(self.observation_queue)
        row_layout = QHBoxLayout(row_widget)
        row_layout.setContentsMargins(2, 4, 2, 4)
        row_layout.setSpacing(8)

        delete_btn = QToolButton(row_widget)
        delete_btn.setText("x")
        delete_btn.setFixedSize(22, 22)
        delete_btn.setToolTip(self.tr("Delete microscope session(s) for this row"))
        delete_btn.setEnabled(session_count > 0)
        delete_btn.clicked.connect(
            lambda _checked=False, obs_id=int(observation_id): self._delete_sessions_for_observation(obs_id)
        )
        row_layout.addWidget(delete_btn, 0, Qt.AlignTop)

        label = QLabel(text, row_widget)
        label.setWordWrap(True)
        row_layout.addWidget(label, 1)

        def select_row(_event=None, obs_id: int = int(observation_id)) -> None:
            self._set_selected_observation(obs_id)

        row_widget.mousePressEvent = select_row
        label.mousePressEvent = select_row
        return row_widget

    def _selected_observation_id(self) -> int | None:
        item = self.observation_queue.currentItem()
        if item is None:
            return None
        try:
            obs_id = int(item.data(Qt.UserRole) or 0)
        except Exception:
            obs_id = 0
        return obs_id if obs_id > 0 else None

    def _set_selected_observation(self, observation_id: int | None) -> None:
        try:
            target_id = int(observation_id or 0)
        except Exception:
            target_id = 0
        if target_id <= 0:
            return
        for row in range(self.observation_queue.count()):
            item = self.observation_queue.item(row)
            try:
                item_id = int(item.data(Qt.UserRole) or 0)
            except Exception:
                item_id = 0
            if item_id == target_id:
                self.observation_queue.setCurrentItem(item)
                return

    def _observation_title(self, observation: dict | None) -> str:
        obs = dict(observation or {})
        vernacular = str(obs.get("common_name") or "").strip()
        scientific = " ".join(
            part
            for part in (str(obs.get("genus") or "").strip(), str(obs.get("species") or "").strip())
            if part
        ).strip()
        guess = str(obs.get("species_guess") or "").strip()
        if vernacular and scientific:
            return f"{vernacular} — {scientific}"
        return vernacular or scientific or guess or self.tr("Unknown observation")

    def _choose_scan_dir(self) -> None:
        current = str(self.scan_dir_input.text() or "").strip()
        start_dir = current if current and Path(current).exists() else str(Path.home())
        chosen = QFileDialog.getExistingDirectory(self, self.tr("Choose import folder"), start_dir)
        if chosen:
            self.scan_dir_input.setText(chosen)

    def _scan_folder(self) -> None:
        folder = str(self.scan_dir_input.text() or "").strip()
        if not folder or not Path(folder).exists():
            self._show_status(
                self.tr("Choose an existing import folder first."),
                tone="warning",
                timeout_ms=5000,
            )
            return
        files = sorted(
            str(path)
            for path in Path(folder).rglob("*")
            if path.is_file()
        )
        total_files = len(files)
        show_progress = total_files > 1
        prepared_rows: list[dict] = []
        if show_progress:
            self._set_hint_progress_visible(True)
            self._set_hint_progress(self.tr("Scanning import folder..."), 0)
        try:
            for index, file_path in enumerate(files, start=1):
                prepared_rows.extend(self._matcher.prepare_image_rows([file_path]))
                if show_progress:
                    progress_value = int(round((index / max(1, total_files)) * 70))
                    self._set_hint_progress(
                        self.tr("Scanning image {current} of {total}...").format(
                            current=index,
                            total=total_files,
                        ),
                        progress_value,
                    )
            prepared_rows.sort(
                key=lambda row: (
                    row.get("captured_at") or datetime.max,
                    str(row.get("filename") or "").casefold(),
                )
            )
            self._batch_images = prepared_rows
            if show_progress:
                self._set_hint_progress(self.tr("Matching scanned images..."), 75)
            self._excluded_paths = {
                path for path in self._excluded_paths if any(row.get("filepath") == path for row in self._batch_images)
            }
            if self._sync_shot_image_path and self._sync_shot_image_path not in self._excluded_paths:
                self._sync_shot_image_path = None
            self._recompute_matches()
            if show_progress:
                self._set_hint_progress(self.tr("Checking Sync Shot QR..."), 88)
            auto_applied = self._attempt_auto_apply_sync_shot()
            clock_offset_applied = False
            if not auto_applied:
                clock_offset_applied = self._auto_apply_clock_offset_if_helpful()
            if show_progress:
                self._set_hint_progress(self.tr("Scan complete."), 100)
        finally:
            if show_progress:
                self._set_hint_progress_visible(False)
                self._set_hint_progress("", 0)
        status_text = self.tr("Scanned {count} image(s) from the import folder.").format(
            count=len(self._batch_images)
        )
        if auto_applied:
            status_text = self.tr(
                "Scanned {count} image(s) from the import folder and auto-detected the Sync Shot."
            ).format(count=len(self._batch_images))
        elif clock_offset_applied:
            status_text = self.tr(
                "Scanned {count} image(s) from the import folder and restored a likely camera clock offset."
            ).format(count=len(self._batch_images))
        self._show_status(
            status_text,
            tone="success" if self._batch_images else "info",
            timeout_ms=5000,
        )

    def _on_offset_changed(self, _value: float) -> None:
        SettingsDB.set_setting(self.SETTING_OFFSET_SECONDS, float(self.offset_spin.value()))
        self._recompute_matches()
        self._update_sync_shot_summary()

    def _recompute_matches(self) -> None:
        if not self._batch_images:
            self._match_result = {"matches": [], "unmatched": [], "observation_counts": {}}
            self._matches_by_observation = {}
            self.refresh_observation_queue()
            self._refresh_gallery()
            self._update_action_state()
            return
        self._match_result = self._matcher.match_images(
            self._batch_images,
            offset_seconds=float(self.offset_spin.value()),
            observation_tolerance_seconds=self._field_match_tolerance_seconds(),
            exclude_paths=self._excluded_paths,
        )
        self._match_result = self._filter_already_imported_matches(self._match_result)
        self._matches_by_observation = {}
        for row in self._match_result.get("matches", []):
            try:
                obs_id = int(row.get("observation_id") or 0)
            except Exception:
                obs_id = 0
            if obs_id <= 0:
                continue
            self._matches_by_observation.setdefault(obs_id, []).append(row)
        current_obs_id = self._selected_observation_id()
        if current_obs_id is None:
            active_obs_id = int(getattr(self._main_window, "active_observation_id", 0) or 0)
            if active_obs_id in self._matches_by_observation:
                current_obs_id = active_obs_id
            elif self._matches_by_observation:
                current_obs_id = next(iter(self._matches_by_observation.keys()))
        self.refresh_observation_queue(select_observation_id=current_obs_id)
        self._refresh_gallery()
        self._update_action_state()

    def _auto_apply_clock_offset_if_helpful(self) -> bool:
        if not self._batch_images or self._match_result.get("matches"):
            return False
        try:
            current_offset = float(self.offset_spin.value())
        except Exception:
            current_offset = 0.0
        if abs(current_offset) > 0.01:
            return False

        candidates = [3600.0, -3600.0, 7200.0, -7200.0, 10800.0, -10800.0]
        best_offset = 0.0
        best_result: dict | None = None
        best_count = 0
        for offset in candidates:
            result = self._matcher.match_images(
                self._batch_images,
                offset_seconds=offset,
                observation_tolerance_seconds=self._field_match_tolerance_seconds(),
                exclude_paths=self._excluded_paths,
            )
            result = self._filter_already_imported_matches(result)
            match_count = len(result.get("matches", []) or [])
            if match_count > best_count:
                best_count = match_count
                best_offset = offset
                best_result = result

        if best_result is None or best_count <= 0:
            return False

        self.offset_spin.blockSignals(True)
        self.offset_spin.setValue(best_offset)
        self.offset_spin.blockSignals(False)
        SettingsDB.set_setting(self.SETTING_OFFSET_SECONDS, best_offset)
        self._match_result = best_result
        self._matches_by_observation = {}
        for row in self._match_result.get("matches", []):
            try:
                obs_id = int(row.get("observation_id") or 0)
            except Exception:
                obs_id = 0
            if obs_id <= 0:
                continue
            self._matches_by_observation.setdefault(obs_id, []).append(row)
        self.refresh_observation_queue(select_observation_id=self._selected_observation_id())
        self._refresh_gallery()
        self._update_action_state()
        return True

    def _filter_already_imported_matches(self, match_result: dict) -> dict:
        result = dict(match_result or {})
        matches = list(result.get("matches") or [])
        if not matches:
            return result

        existing_by_observation: dict[int, set[tuple[str, str | None]]] = {}
        for row in matches:
            try:
                obs_id = int(row.get("observation_id") or 0)
            except Exception:
                obs_id = 0
            if obs_id <= 0 or obs_id in existing_by_observation:
                continue
            signatures: set[tuple[str, str | None]] = set()
            for image in ImageDB.get_images_for_observation(obs_id):
                captured_key = self._timestamp_key(image.get("captured_at"))
                for key in ("filepath", "original_filepath"):
                    filename = Path(str(image.get(key) or "")).name.casefold()
                    if filename:
                        signatures.add((filename, captured_key))
            existing_by_observation[obs_id] = signatures

        annotated_matches: list[dict] = []
        for row in matches:
            try:
                obs_id = int(row.get("observation_id") or 0)
            except Exception:
                obs_id = 0
            filename = Path(str(row.get("filepath") or "")).name.casefold()
            captured_key = self._timestamp_key(row.get("captured_at"))
            existing = existing_by_observation.get(obs_id, set())
            row_copy = dict(row)
            if (filename, captured_key) in existing:
                row_copy["already_imported"] = True
            elif (filename, None) in existing:
                row_copy["already_imported"] = True
            else:
                row_copy["already_imported"] = False
            annotated_matches.append(row_copy)

        result["matches"] = annotated_matches
        observation_counts: dict[int, int] = {}
        session_counts: dict[str, int] = {}
        already_imported_count = 0
        for row in annotated_matches:
            if bool(row.get("already_imported")):
                already_imported_count += 1
                continue
            try:
                obs_id = int(row.get("observation_id") or 0)
            except Exception:
                obs_id = 0
            if obs_id > 0:
                observation_counts[obs_id] = observation_counts.get(obs_id, 0) + 1
            session_id = str(row.get("session_id") or "").strip()
            if session_id:
                session_counts[session_id] = session_counts.get(session_id, 0) + 1
        result["observation_counts"] = observation_counts
        result["session_counts"] = session_counts
        result["already_imported_count"] = already_imported_count
        return result

    @staticmethod
    def _timestamp_key(value) -> str | None:
        if isinstance(value, datetime):
            return value.replace(microsecond=0).isoformat(sep=" ")
        text = str(value or "").strip()
        if not text:
            return None
        parsed = IngestionHubTab._parse_observation_datetime(text)
        if parsed is None:
            return text
        return parsed.replace(microsecond=0).isoformat(sep=" ")

    def _on_observation_changed(self, _current, _previous) -> None:
        self._refresh_gallery()
        self._update_action_state()

    def _matches_for_current_observation(self) -> list[dict]:
        obs_id = self._selected_observation_id()
        if obs_id is None:
            return []
        return list(self._matches_by_observation.get(obs_id, []))

    def _refresh_gallery(self) -> None:
        matches = self._matches_for_current_observation()
        existing_selection = set(self.staging_gallery.selected_paths())
        items = []
        objectives = load_objectives()
        for index, row in enumerate(matches, start=1):
            image_type = str(row.get("image_type") or "field").strip().lower() or "field"
            state = dict(row.get("state") or {})
            objective_key = str(state.get("objective_name") or "").strip() or None
            objective_label = None
            if objective_key:
                objective = objectives.get(objective_key)
                objective_label = objective_display_name(objective, objective_key) if objective else objective_key
            badges = ImageGalleryWidget.build_image_type_badges(
                image_type=image_type,
                objective_name=objective_label,
                contrast=state.get("contrast"),
                scale_microns_per_pixel=self._matched_scale_microns_per_pixel(state),
                needs_scale=image_type == "microscope" and not bool(objective_key),
                translate=self.tr,
            )
            items.append(
                {
                    "id": row.get("filepath"),
                    "filepath": row.get("filepath"),
                    "image_number": index,
                    "has_measurements": False,
                    "badges": badges,
                    "center_badge": self.tr("Imported") if bool(row.get("already_imported")) else None,
                }
            )
        self.staging_gallery.set_items(items)
        if items:
            addable_paths = [
                str(item["filepath"])
                for item, row in zip(items, matches, strict=False)
                if not bool(row.get("already_imported"))
            ]
            selected_paths = [
                item["filepath"]
                for item, row in zip(items, matches, strict=False)
                if item["filepath"] in existing_selection and not bool(row.get("already_imported"))
            ]
            if not selected_paths:
                selected_paths = [addable_paths[0]] if addable_paths else [items[0]["filepath"]]
            self.staging_gallery.select_paths(selected_paths)
            self._on_gallery_selection_changed(selected_paths)
        else:
            self._selected_match_path = None
            self._clear_viewer(
                title=self.tr("No matched images"),
                meta=self.tr("Scan a folder and choose an observation with matched field or microscope imports."),
            )
        total_scanned = len(self._batch_images)
        all_matches = list(self._match_result.get("matches", []) or [])
        already_imported_total = sum(1 for row in all_matches if bool((row or {}).get("already_imported")))
        matched_total = len(all_matches) - already_imported_total
        unmatched_total = len(self._match_result.get("unmatched", []))
        excluded_total = len(self._excluded_paths)
        microscope_total = sum(
            1 for row in all_matches
            if str((row or {}).get("image_type") or "").strip().lower() == "microscope"
            and not bool((row or {}).get("already_imported"))
        )
        field_total = matched_total - microscope_total
        self.batch_summary_label.setText(
            self.tr(
                "Scanned: {scanned} • New matches: {matched} • Field: {field} • Microscope: {microscope} • Already imported: {imported} • Unmatched: {unmatched} • Excluded: {excluded}"
            ).format(
                scanned=total_scanned,
                matched=matched_total,
                field=field_total,
                microscope=microscope_total,
                imported=already_imported_total,
                unmatched=unmatched_total,
                excluded=excluded_total,
            )
        )

    def _on_gallery_clicked(self, image_key, _path: str) -> None:
        target_path = str(image_key or "").strip()
        if not target_path:
            return
        if target_path not in set(self.staging_gallery.selected_paths()):
            return
        self._selected_match_path = target_path
        match = self._match_for_path(target_path)
        if match:
            self._show_match(match)

    def _on_gallery_selection_changed(self, paths: list[str]) -> None:
        if not paths:
            return
        target_path = str(paths[0] or "").strip()
        if not target_path:
            return
        self._selected_match_path = target_path
        match = self._match_for_path(target_path)
        if match:
            self._show_match(match)

    def _match_for_path(self, filepath: str | None) -> dict | None:
        target = str(filepath or "").strip()
        if not target:
            return None
        for row in self._matches_for_current_observation():
            if str(row.get("filepath") or "").strip() == target:
                return row
        return None

    def _reset_viewer(self) -> None:
        self.image_viewer.reset_view()

    def _clear_viewer(self, title: str | None = None, meta: str | None = None) -> None:
        self.viewer_title_label.setText(title or self.tr("Matched image"))
        self.viewer_meta_label.setText(meta or self.tr("Select a matched image to inspect it here."))
        self.reset_view_btn.setEnabled(False)
        self.image_viewer.set_microns_per_pixel(0.0)
        self.image_viewer.set_scale_bar(False, 0.0)
        self.image_viewer.set_image(None)

    def _show_match(self, match_row: dict) -> None:
        image_path = str(match_row.get("filepath") or "").strip()
        if not image_path:
            self._clear_viewer()
            return
        pixmap, preview_scaled = self._load_viewer_pixmap(image_path)
        if pixmap is None or pixmap.isNull():
            self._clear_viewer(
                title=self.tr("Selected image unavailable"),
                meta=self.tr("Could not load {name}.").format(name=Path(image_path).name),
            )
            return
        self.image_viewer.set_image_sources(pixmap, image_path, preview_scaled)
        self.reset_view_btn.setEnabled(True)

        scale_mpp = self._matched_scale_microns_per_pixel(match_row.get("state") or {})
        self.image_viewer.set_microns_per_pixel(scale_mpp or 0.0)
        if scale_mpp:
            scale_bar_value = self._suggest_scale_bar_um(match_row.get("state") or {})
            self.image_viewer.set_scale_bar(True, float(scale_bar_value), unit="µm")
        else:
            self.image_viewer.set_scale_bar(False, 0.0)

        self.viewer_title_label.setText(
            self.tr("Matched image: {name}").format(name=Path(image_path).name)
        )
        self.viewer_meta_label.setText(self._viewer_meta_text(match_row))

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
        else:
            pixmap = QPixmap.fromImage(image)
        return (pixmap if not pixmap.isNull() else None, preview_scaled)

    def _matched_scale_microns_per_pixel(self, state: dict) -> float | None:
        objective_key = str((state or {}).get("objective_name") or "").strip()
        if not objective_key:
            return None
        objectives = load_objectives()
        resolved_key = resolve_objective_key(objective_key, objectives) or objective_key
        objective = objectives.get(resolved_key)
        try:
            scale = float((objective or {}).get("microns_per_pixel") or 0.0)
        except Exception:
            scale = 0.0
        return scale if scale > 0 else None

    def _suggest_scale_bar_um(self, state: dict) -> float:
        objective_key = str((state or {}).get("objective_name") or "").strip() or None
        observation_fallback = getattr(self._main_window, "_observation_scale_bar_fallback_value", None)
        if callable(observation_fallback):
            try:
                return float(observation_fallback(False, objective_key=objective_key))
            except Exception:
                pass
        objective_fallback = getattr(self._main_window, "_suggest_microscope_scale_bar_um_for_objective", None)
        if callable(objective_fallback):
            try:
                return float(objective_fallback(objective_key))
            except Exception:
                pass
        return 10.0

    def _viewer_meta_text(self, match_row: dict) -> str:
        parts: list[str] = []
        match_kind = str(match_row.get("match_kind") or "").strip().lower()
        if match_kind == "session":
            parts.append(self.tr("Microscope session match"))
        elif match_kind == "observation_window":
            parts.append(self.tr("Observation time match"))
        captured_at = match_row.get("captured_at")
        adjusted_at = match_row.get("adjusted_at")
        if isinstance(captured_at, datetime):
            parts.append(self.tr("Capture: {value}").format(value=captured_at.strftime("%Y-%m-%d %H:%M:%S")))
        if isinstance(adjusted_at, datetime):
            parts.append(self.tr("Matched: {value}").format(value=adjusted_at.strftime("%Y-%m-%d %H:%M:%S")))
        window_start_at = match_row.get("window_start_at")
        window_end_at = match_row.get("window_end_at")
        if isinstance(window_start_at, datetime) and isinstance(window_end_at, datetime):
            if window_start_at == window_end_at:
                parts.append(
                    self.tr("Observation: {value}").format(value=window_start_at.strftime("%Y-%m-%d %H:%M:%S"))
                )
            else:
                parts.append(
                    self.tr("Observation span: {start} - {end}").format(
                        start=window_start_at.strftime("%Y-%m-%d %H:%M:%S"),
                        end=window_end_at.strftime("%Y-%m-%d %H:%M:%S"),
                    )
                )
        state = dict(match_row.get("state") or {})
        objective_key = str(state.get("objective_name") or "").strip()
        if objective_key:
            objectives = load_objectives()
            objective = objectives.get(resolve_objective_key(objective_key, objectives) or objective_key)
            parts.append(objective_display_name(objective, objective_key) if objective else objective_key)
        for category, key in (
            ("contrast", "contrast"),
            ("mount", "mount_medium"),
            ("stain", "stain"),
            ("sample", "sample_type"),
        ):
            value = str(state.get(key) or "").strip()
            if value:
                parts.append(DatabaseTerms.translate(category, value))
        scale_mpp = self._matched_scale_microns_per_pixel(state)
        if scale_mpp:
            parts.append(self.tr("{scale:.4g} µm/px").format(scale=scale_mpp))
        note_text = str(match_row.get("notes") or "").strip()
        if note_text:
            parts.append(self.tr("Note: {value}").format(value=note_text.replace("\n", " | ")))
        return " • ".join(parts) if parts else self.tr("Scroll to zoom and drag to pan.")

    def _create_sync_shot(self) -> None:
        dialog = SyncShotDialog(self)
        if dialog.exec() != QDialog.Accepted or not dialog.shot_record:
            return
        self._sync_shot_record = dict(dialog.shot_record)
        self._sync_shot_image_path = None
        self._update_sync_shot_summary()
        auto_applied = self._attempt_auto_apply_sync_shot()
        if auto_applied:
            self._update_action_state()
            return
        self._show_status(
            self.tr(
                "Sync Shot created. Photograph it, close the dialog, then scan the import folder. "
                "Sporely will auto-check the first and last image from each folder."
            ),
            tone="success",
            timeout_ms=5000,
        )
        self._update_action_state()

    def _sync_shot_candidate_rows(self) -> list[dict]:
        grouped: dict[str, list[dict]] = {}
        for row in self._batch_images:
            filepath = str(row.get("filepath") or "").strip()
            if not filepath:
                continue
            folder_key = str(Path(filepath).parent)
            grouped.setdefault(folder_key, []).append(row)
        candidates: list[dict] = []
        seen_paths: set[str] = set()
        for folder_key in sorted(grouped.keys(), key=str.casefold):
            rows = sorted(
                grouped.get(folder_key) or [],
                key=lambda row: str(row.get("filepath") or "").casefold(),
            )
            for candidate in (rows[0], rows[-1]):
                filepath = str(candidate.get("filepath") or "").strip()
                if not filepath or filepath in seen_paths:
                    continue
                seen_paths.add(filepath)
                candidates.append(candidate)
        return candidates

    def _attempt_auto_apply_sync_shot(self) -> bool:
        if not self._batch_images or self._sync_shot_image_path:
            return False
        for row in self._sync_shot_candidate_rows():
            if self._apply_sync_shot_row(row, show_status=False):
                return True
        return False

    def _apply_sync_shot_row(self, row: dict, *, show_status: bool = True) -> bool:
        if not row:
            return False
        captured_at = row.get("captured_at")
        if not isinstance(captured_at, datetime):
            return False
        filepath = str(row.get("filepath") or "").strip()
        if not filepath:
            return False
        try:
            decode_result = decode_sync_shot_qr(filepath)
        except ImportError:
            if show_status:
                self._show_status(
                    self.tr("QR decoding dependencies are not installed yet."),
                    tone="warning",
                    timeout_ms=5000,
                )
            return False
        except Exception:
            if show_status:
                self._show_status(
                    self.tr("Sporely could not decode a Sync Shot QR code from that image."),
                    tone="warning",
                    timeout_ms=5000,
                )
            return False
        matches = list(decode_result.get("matches") or [])
        if not matches or decode_result.get("multiple"):
            if show_status and decode_result.get("multiple"):
                self._show_status(
                    self.tr("Multiple Sync Shot QR timestamps were detected. Choose a clearer image."),
                    tone="warning",
                    timeout_ms=6000,
                )
            return False
        parsed = matches[0]
        session_id = str(parsed.get("session_id") or "").strip()
        expected_session_id = str((self._sync_shot_record or {}).get("session_id") or "").strip()
        if expected_session_id and session_id != expected_session_id:
            return False
        qr_utc_dt = parsed.get("utc_dt")
        if not isinstance(qr_utc_dt, datetime):
            return False
        offset_info = choose_sync_shot_offset(captured_at, qr_utc_dt)
        chosen_offset = float(offset_info.get("offset_seconds") or 0.0)
        basis = self.tr("local clock") if str(offset_info.get("basis") or "") == "local" else self.tr("UTC clock")
        if not self._sync_shot_record:
            self._sync_shot_record = {
                "shot_id": session_id,
                "mode": "qr",
                "session_id": session_id,
                "created_utc_text": str(parsed.get("utc_text") or ""),
            }
        self._sync_shot_record["applied_utc_text"] = str(parsed.get("utc_text") or "")
        self.offset_spin.blockSignals(True)
        self.offset_spin.setValue(chosen_offset)
        self.offset_spin.blockSignals(False)
        SettingsDB.set_setting(self.SETTING_OFFSET_SECONDS, chosen_offset)
        if self._sync_shot_image_path:
            self._excluded_paths.discard(self._sync_shot_image_path)
        self._sync_shot_image_path = filepath
        self._excluded_paths.add(filepath)
        self._recompute_matches()
        self._update_sync_shot_summary()
        if show_status:
            self._show_status(
                self.tr("Applied Sync Shot offset from {name} using QR time {time} and the {basis}.").format(
                    name=str(row.get("filename") or Path(filepath).name),
                    time=str(parsed.get("utc_text") or ""),
                    basis=basis,
                ),
                tone="success",
                timeout_ms=5000,
            )
        return True

    def _apply_sync_shot_from_batch_image(self) -> None:
        if not self._batch_images:
            self._show_status(
                self.tr("Scan an import folder before choosing the photographed Sync Shot image."),
                tone="warning",
                timeout_ms=5000,
            )
            return
        choices: list[str] = []
        choice_map: dict[str, dict] = {}
        for row in self._batch_images:
            filepath = str(row.get("filepath") or "").strip()
            if not filepath:
                continue
            filename = str(row.get("filename") or Path(filepath).name)
            captured_at = row.get("captured_at")
            if isinstance(captured_at, datetime):
                label = f"{filename} — {captured_at.strftime('%Y-%m-%d %H:%M:%S')}"
            else:
                label = f"{filename} — {self.tr('No capture time')}"
            choices.append(label)
            choice_map[label] = row
        if not choices:
            self._show_status(
                self.tr("No batch images are available for Sync Shot calibration."),
                tone="warning",
                timeout_ms=4000,
            )
            return
        selected_label, accepted = QInputDialog.getItem(
            self,
            self.tr("Choose Sync Shot image"),
            self.tr("Photographed Sync Shot image:"),
            choices,
            0,
            False,
        )
        if not accepted or not selected_label:
            return
        row = choice_map.get(str(selected_label))
        if not row:
            return
        if self._apply_sync_shot_row(row, show_status=True):
            return
        if not isinstance(row.get("captured_at"), datetime):
            self._show_status(
                self.tr("The chosen image does not have a capture timestamp in EXIF."),
                tone="warning",
                timeout_ms=5000,
            )
            return
        self._show_status(
            self.tr("No Sync Shot QR code was found in the chosen image."),
            tone="warning",
            timeout_ms=5000,
        )

    def _clear_sync_shot(self) -> None:
        if self._sync_shot_image_path:
            self._excluded_paths.discard(self._sync_shot_image_path)
        self._sync_shot_record = None
        self._sync_shot_image_path = None
        self.offset_spin.blockSignals(True)
        self.offset_spin.setValue(0.0)
        self.offset_spin.blockSignals(False)
        SettingsDB.set_setting(self.SETTING_OFFSET_SECONDS, 0.0)
        self._recompute_matches()
        self._update_sync_shot_summary()
        self._show_status(
            self.tr("Cleared Sync Shot calibration."),
            tone="info",
            timeout_ms=3000,
        )

    def _update_sync_shot_summary(self) -> None:
        if not self._sync_shot_record:
            self.sync_shot_summary_label.setText(
                self.tr("No Sync Shot created for this batch yet.")
            )
            return
        session_text = str(
            self._sync_shot_record.get("applied_utc_text")
            or self._sync_shot_record.get("created_utc_text")
            or ""
        ).strip()
        parts = [
            self.tr("QR session: {value}").format(value=session_text),
            self.tr("Offset: {value:+.1f}s").format(value=float(self.offset_spin.value())),
        ]
        if self._sync_shot_image_path:
            parts.append(
                self.tr("Using: {name}").format(name=Path(self._sync_shot_image_path).name)
            )
        self.sync_shot_summary_label.setText(" • ".join(parts))

    def _current_commit_rows(self) -> list[dict]:
        matches = self._matches_for_current_observation()
        if not matches:
            return []
        selected_paths = set(self.staging_gallery.selected_paths())
        if not selected_paths:
            return []
        return [
            row
            for row in matches
            if str(row.get("filepath") or "").strip() in selected_paths
            and not bool(row.get("already_imported"))
        ]

    def _commit_selected_matches(self) -> None:
        self._add_match_rows(self._current_commit_rows())

    def _add_all_matches(self) -> None:
        self._add_match_rows(
            [row for row in self._matches_for_current_observation() if not bool(row.get("already_imported"))]
        )

    def _add_match_rows(self, rows: list[dict]) -> None:
        observation_id = self._selected_observation_id()
        if observation_id is None:
            self._show_status(
                self.tr("Choose an observation before adding matched images."),
                tone="warning",
                timeout_ms=5000,
            )
            return
        if not rows:
            self._show_status(
                self.tr("No new matched images are selected to add."),
                tone="warning",
                timeout_ms=4000,
            )
            return
        imported_ids: list[int] = []
        total_rows = len(rows)
        show_progress = total_rows > 1
        if show_progress:
            self._set_hint_progress_visible(True)
            self._set_hint_progress(self.tr("Adding matched images..."), 0)
        try:
            for index, row in enumerate(rows, start=1):
                if show_progress:
                    self._set_hint_progress(
                        self.tr("Adding image {current} of {total}...").format(
                            current=index,
                            total=total_rows,
                        ),
                        int(round(((index - 1) / max(1, total_rows)) * 100)),
                    )
                image_id = self._import_match(observation_id, row)
                if image_id:
                    imported_ids.append(int(image_id))
                if show_progress:
                    self._set_hint_progress(
                        self.tr("Adding image {current} of {total}...").format(
                            current=index,
                            total=total_rows,
                        ),
                        int(round((index / max(1, total_rows)) * 100)),
                    )
        finally:
            if show_progress:
                self._set_hint_progress_visible(False)
                self._set_hint_progress("", 0)
        if not imported_ids:
            self._show_status(
                self.tr("No images were added."),
                tone="warning",
                timeout_ms=4000,
            )
            return
        self._recompute_matches()
        self._refresh_main_window_after_commit(observation_id, imported_ids[-1])
        self._show_status(
            self.tr("Added {count} image(s) to the observation.").format(
                count=len(imported_ids)
            ),
            tone="success",
            timeout_ms=5000,
        )

    def _import_match(self, observation_id: int, match_row: dict) -> int:
        source_path = str(match_row.get("filepath") or "").strip()
        if not source_path:
            return 0
        output_dir = get_images_dir() / "imports"
        output_dir.mkdir(parents=True, exist_ok=True)
        converted_path = maybe_convert_heic(source_path, output_dir)
        if converted_path is None:
            return 0

        state = dict(match_row.get("state") or {})
        matched_image_type = str(match_row.get("image_type") or "field").strip().lower() or "field"
        objectives = load_objectives()
        objective_key = str(state.get("objective_name") or "").strip() or None
        resolved_objective_key = (
            resolve_objective_key(objective_key, objectives) or objective_key
            if objective_key
            else None
        )
        objective = objectives.get(resolved_objective_key) if resolved_objective_key else None
        try:
            scale = float((objective or {}).get("microns_per_pixel") or 0.0)
        except Exception:
            scale = 0.0
        calibration_id = (
            CalibrationDB.get_active_calibration_id(resolved_objective_key)
            if resolved_objective_key
            else None
        )
        lab_metadata = None
        if matched_image_type == "microscope":
            lab_metadata = {
                "session_id": str(match_row.get("session_id") or "").strip() or None,
                "session_kind": str(match_row.get("session_kind") or "").strip() or None,
                "objective_name": resolved_objective_key,
                "contrast": (
                    DatabaseTerms.canonicalize("contrast", state.get("contrast"))
                    if state.get("contrast")
                    else None
                ),
                "mount_medium": (
                    DatabaseTerms.canonicalize("mount", state.get("mount_medium"))
                    if state.get("mount_medium")
                    else None
                ),
                "stain": (
                    DatabaseTerms.canonicalize("stain", state.get("stain"))
                    if state.get("stain")
                    else None
                ),
                "sample_type": (
                    DatabaseTerms.canonicalize("sample", state.get("sample_type"))
                    if state.get("sample_type")
                    else None
                ),
                "matched_at": (
                    match_row.get("adjusted_at").isoformat()
                    if isinstance(match_row.get("adjusted_at"), datetime)
                    else None
                ),
            }
        image_id = ImageDB.add_image(
            observation_id=observation_id,
            filepath=converted_path,
            image_type="microscope" if matched_image_type == "microscope" else "field",
            scale=scale if matched_image_type == "microscope" and scale > 0 else None,
            objective_name=resolved_objective_key if matched_image_type == "microscope" else None,
            contrast=(
                DatabaseTerms.canonicalize("contrast", state.get("contrast"))
                if matched_image_type == "microscope"
                else None
            ),
            mount_medium=(
                DatabaseTerms.canonicalize("mount", state.get("mount_medium"))
                if matched_image_type == "microscope"
                else None
            ),
            stain=(
                DatabaseTerms.canonicalize("stain", state.get("stain"))
                if matched_image_type == "microscope"
                else None
            ),
            sample_type=(
                DatabaseTerms.canonicalize("sample", state.get("sample_type"))
                if matched_image_type == "microscope"
                else None
            ),
            notes=str(match_row.get("notes") or "").strip() or None,
            captured_at=match_row.get("captured_at"),
            calibration_id=calibration_id if matched_image_type == "microscope" else None,
            resample_scale_factor=1.0,
            lab_metadata=lab_metadata,
        )
        image_data = ImageDB.get_image(image_id)
        stored_path = str((image_data or {}).get("filepath") or converted_path)
        try:
            generate_all_sizes(stored_path, image_id)
        except Exception:
            pass
        cleanup_import_temp_file(source_path, converted_path, stored_path, output_dir)
        return int(image_id or 0)

    def _refresh_main_window_after_commit(self, observation_id: int, image_id: int) -> None:
        try:
            if hasattr(self._main_window, "observations_tab"):
                self._main_window.observations_tab.refresh_observations(show_status=False)
        except Exception:
            pass
        if int(getattr(self._main_window, "active_observation_id", 0) or 0) != int(observation_id or 0):
            return
        try:
            self._main_window.refresh_observation_images(select_image_id=image_id)
            self._main_window.update_measurements_table()
        except Exception:
            pass

    def _delete_sessions_for_observation(self, observation_id: int) -> None:
        deleted = SessionLogDB.delete_sessions_for_observation(
            observation_id,
            session_kind="offline",
        )
        self.refresh_observation_queue(select_observation_id=observation_id)
        self._recompute_matches()
        if deleted:
            self._show_status(
                self.tr("Deleted {count} microscope session event(s).").format(count=deleted),
                tone="success",
                timeout_ms=4000,
            )
        else:
            self._show_status(
                self.tr("No microscope sessions were found for that row."),
                tone="info",
                timeout_ms=3000,
            )

    def _update_action_state(self) -> None:
        has_batch = bool(self._batch_images)
        has_addable_matches = any(
            not bool(row.get("already_imported"))
            for row in self._matches_for_current_observation()
        )
        has_selection = bool(self._selected_observation_id())
        self.scan_folder_btn.setEnabled(bool(str(self.scan_dir_input.text() or "").strip()))
        self.refresh_matches_btn.setEnabled(has_batch)
        self.commit_matches_btn.setEnabled(has_selection and has_addable_matches)
        self.add_all_images_btn.setEnabled(has_selection and has_addable_matches)
        self.apply_sync_shot_btn.setEnabled(has_batch)
        self.clear_sync_shot_btn.setEnabled(bool(self._sync_shot_record or self._sync_shot_image_path))
