"""Retrospective image ingestion tab driven by session logs."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from PySide6.QtCore import QSize, Qt, QUrl
from PySide6.QtGui import QDesktopServices, QImageReader, QPixmap
from PySide6.QtWidgets import (
    QDialog,
    QFileDialog,
    QDoubleSpinBox,
    QFrame,
    QGroupBox,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSpinBox,
    QSplitter,
    QVBoxLayout,
    QWidget,
)

from database.database_tags import DatabaseTerms
from database.models import CalibrationDB, ImageDB, ObservationDB, SettingsDB
from database.schema import get_images_dir, load_objectives, objective_display_name, resolve_objective_key
from utils.heic_converter import maybe_convert_heic
from utils.image_utils import cleanup_import_temp_file
from utils.sync_shot_qr import choose_sync_shot_offset, decode_sync_shot_qr
from utils.temporal_matcher import TemporalMatcher
from utils.thumbnail_generator import generate_all_sizes

from .hint_status import HintBar, HintStatusController
from .image_gallery_widget import ImageGalleryWidget
from .sync_shot_dialog import SyncShotDialog
from .zoomable_image_widget import ZoomableImageLabel


class IngestionHubTab(QWidget):
    """Match mixed field and microscope imports to existing observations."""

    SETTING_SCAN_DIR = "ingestion_hub_scan_dir"
    SETTING_FIELD_MATCH_TOLERANCE_SECONDS = "ingestion_hub_field_match_tolerance_seconds"
    SETTING_MICROSCOPE_MATCH_TOLERANCE_SECONDS = "ingestion_hub_microscope_match_tolerance_seconds"
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

        queue_group = QGroupBox(self.tr("Matched observations"))
        queue_layout = QVBoxLayout(queue_group)
        queue_layout.setContentsMargins(10, 12, 10, 10)
        queue_layout.setSpacing(8)
        self.observation_queue = QListWidget()
        self.observation_queue.currentItemChanged.connect(self._on_observation_changed)
        queue_layout.addWidget(self.observation_queue, 1)
        self.queue_summary_label = QLabel("")
        self.queue_summary_label.setWordWrap(True)
        self.queue_summary_label.setStyleSheet("color: #6b7280;")
        queue_layout.addWidget(self.queue_summary_label)
        left_layout.addWidget(queue_group, 1)

        folder_group = QGroupBox(self.tr("Import folder"))
        folder_layout = QVBoxLayout(folder_group)
        folder_layout.setContentsMargins(10, 12, 10, 10)
        folder_layout.setSpacing(8)
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

        sync_group = QGroupBox(self.tr("Sync Shot"))
        sync_layout = QVBoxLayout(sync_group)
        sync_layout.setContentsMargins(10, 12, 10, 10)
        sync_layout.setSpacing(8)
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

        match_group = QGroupBox(self.tr("Matching"))
        match_layout = QVBoxLayout(match_group)
        match_layout.setContentsMargins(10, 12, 10, 10)
        match_layout.setSpacing(8)
        match_hint = QLabel(
            self.tr(
                "Field images match nearby observation times. Microscope images match retrospective "
                "session logs and use the Sync Shot offset when present."
            )
        )
        match_hint.setWordWrap(True)
        match_hint.setStyleSheet("color: #4b5563;")
        match_layout.addWidget(match_hint)
        field_tol_row = QHBoxLayout()
        field_tol_row.setContentsMargins(0, 0, 0, 0)
        field_tol_row.setSpacing(8)
        field_tol_row.addWidget(QLabel(self.tr("Field tolerance (s):")))
        self.field_tolerance_spin = QSpinBox()
        self.field_tolerance_spin.setRange(0, 3600)
        self.field_tolerance_spin.setSingleStep(10)
        self.field_tolerance_spin.setValue(int(round(self._field_match_tolerance_seconds())))
        self.field_tolerance_spin.valueChanged.connect(self._on_match_tolerance_changed)
        field_tol_row.addWidget(self.field_tolerance_spin, 1)
        match_layout.addLayout(field_tol_row)
        microscope_tol_row = QHBoxLayout()
        microscope_tol_row.setContentsMargins(0, 0, 0, 0)
        microscope_tol_row.setSpacing(8)
        microscope_tol_row.addWidget(QLabel(self.tr("Microscope tolerance (s):")))
        self.microscope_tolerance_spin = QSpinBox()
        self.microscope_tolerance_spin.setRange(0, 300)
        self.microscope_tolerance_spin.setSingleStep(1)
        self.microscope_tolerance_spin.setValue(int(round(self._microscope_match_tolerance_seconds())))
        self.microscope_tolerance_spin.valueChanged.connect(self._on_match_tolerance_changed)
        microscope_tol_row.addWidget(self.microscope_tolerance_spin, 1)
        match_layout.addLayout(microscope_tol_row)
        left_layout.addWidget(match_group)

        actions_group = QGroupBox(self.tr("Actions"))
        actions_layout = QVBoxLayout(actions_group)
        actions_layout.setContentsMargins(10, 12, 10, 10)
        actions_layout.setSpacing(8)
        self.batch_summary_label = QLabel("")
        self.batch_summary_label.setWordWrap(True)
        self.batch_summary_label.setStyleSheet("color: #4b5563;")
        actions_layout.addWidget(self.batch_summary_label)
        self.refresh_matches_btn = QPushButton(self.tr("Refresh matches"))
        self.refresh_matches_btn.clicked.connect(self._recompute_matches)
        actions_layout.addWidget(self.refresh_matches_btn)
        self.commit_matches_btn = QPushButton(self.tr("Commit selected"))
        self.commit_matches_btn.clicked.connect(self._commit_selected_matches)
        actions_layout.addWidget(self.commit_matches_btn)
        self.open_observation_btn = QPushButton(self.tr("Open observation"))
        self.open_observation_btn.clicked.connect(self._open_selected_observation)
        actions_layout.addWidget(self.open_observation_btn)
        left_layout.addWidget(actions_group)
        left_layout.addStretch(1)

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
        main_splitter.addWidget(left_scroll)

        right_panel = QWidget()
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
        self.image_viewer.setMinimumSize(800, 420)
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
            default_height=220,
            min_height=80,
        )
        self.staging_gallery.set_multi_select(True)
        self.staging_gallery.imageClicked.connect(self._on_gallery_clicked)
        self.staging_gallery.selectionChanged.connect(self._on_gallery_selection_changed)

        content_splitter = QSplitter(Qt.Vertical)
        content_splitter.setChildrenCollapsible(False)
        content_splitter.addWidget(viewer_panel)
        content_splitter.addWidget(self.staging_gallery)
        content_splitter.setStretchFactor(0, 4)
        content_splitter.setStretchFactor(1, 1)
        content_splitter.setSizes([760, 220])
        right_layout.addWidget(content_splitter, 1)

        main_splitter.addWidget(right_panel)
        main_splitter.setStretchFactor(0, 0)
        main_splitter.setStretchFactor(1, 1)
        main_splitter.setSizes([420, 1180])

        self.hint_bar = HintBar(self)
        self.hint_bar.set_wrap_mode(True)
        self._hint_controller = HintStatusController(self.hint_bar, self)
        root_layout.addWidget(self.hint_bar)

        self._clear_viewer()

    def _set_hint(self, text: str | None, tone: str = "info") -> None:
        if self._hint_controller is not None:
            self._hint_controller.set_hint(text, tone=tone)

    def _show_status(self, text: str | None, tone: str = "info", timeout_ms: int = 4000) -> None:
        if self._hint_controller is not None:
            self._hint_controller.set_status(text, timeout_ms=timeout_ms, tone=tone)

    def _restore_scan_dir(self) -> None:
        saved = str(SettingsDB.get_setting(self.SETTING_SCAN_DIR, "") or "").strip()
        if saved:
            self.scan_dir_input.setText(saved)

    def _on_scan_dir_changed(self, text: str) -> None:
        SettingsDB.set_setting(self.SETTING_SCAN_DIR, str(text or "").strip())
        self._update_action_state()

    def _field_match_tolerance_seconds(self) -> float:
        widget = getattr(self, "field_tolerance_spin", None)
        if widget is not None:
            try:
                return max(0.0, float(widget.value()))
            except Exception:
                pass
        raw = SettingsDB.get_setting(self.SETTING_FIELD_MATCH_TOLERANCE_SECONDS, 60)
        try:
            return max(0.0, float(raw))
        except (TypeError, ValueError):
            return 60.0

    def _microscope_match_tolerance_seconds(self) -> float:
        widget = getattr(self, "microscope_tolerance_spin", None)
        if widget is not None:
            try:
                return max(0.0, float(widget.value()))
            except Exception:
                pass
        raw = SettingsDB.get_setting(self.SETTING_MICROSCOPE_MATCH_TOLERANCE_SECONDS, 5)
        try:
            return max(0.0, float(raw))
        except (TypeError, ValueError):
            return 5.0

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
            image_count = len(ImageDB.get_images_for_observation(obs_id))
            title = self._observation_title(observation)
            subtitle_parts = [str(observation.get("date") or "").strip() or self.tr("No date")]
            subtitle_parts.append(
                self.tr("{count} session(s)").format(count=session_count)
            )
            subtitle_parts.append(
                self.tr("{count} match(es)").format(count=match_count)
            )
            subtitle_parts.append(
                self.tr("{count} image(s)").format(count=image_count)
            )
            item = QListWidgetItem(f"{title}\n" + " • ".join(subtitle_parts))
            item.setData(Qt.UserRole, obs_id)
            self.observation_queue.addItem(item)
        self.observation_queue.blockSignals(False)

        if self.observation_queue.count() == 0:
            self.queue_summary_label.setText(
                self.tr("No matching observations or retrospective microscope sessions were found yet.")
            )
            self._refresh_gallery()
            self._update_action_state()
            return

        self.queue_summary_label.setText(
            self.tr("{count} observation(s) with matched imports or retrospective sessions.").format(
                count=self.observation_queue.count()
            )
        )
        if current_obs_id:
            self._set_selected_observation(current_obs_id)
        elif self.observation_queue.currentItem() is None and self.observation_queue.count() > 0:
            self.observation_queue.setCurrentRow(0)
        self._update_action_state()

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
        self._batch_images = self._matcher.prepare_image_rows(files)
        self._excluded_paths = {
            path for path in self._excluded_paths if any(row.get("filepath") == path for row in self._batch_images)
        }
        if self._sync_shot_image_path and self._sync_shot_image_path not in self._excluded_paths:
            self._sync_shot_image_path = None
        self._recompute_matches()
        auto_applied = self._attempt_auto_apply_sync_shot()
        status_text = self.tr("Scanned {count} image(s) from the import folder.").format(
            count=len(self._batch_images)
        )
        if auto_applied:
            status_text = self.tr(
                "Scanned {count} image(s) from the import folder and auto-detected the Sync Shot."
            ).format(count=len(self._batch_images))
        self._show_status(
            status_text,
            tone="success" if self._batch_images else "info",
            timeout_ms=5000,
        )

    def _on_offset_changed(self, _value: float) -> None:
        self._recompute_matches()
        self._update_sync_shot_summary()

    def _on_match_tolerance_changed(self, _value: int) -> None:
        SettingsDB.set_setting(
            self.SETTING_FIELD_MATCH_TOLERANCE_SECONDS,
            int(self.field_tolerance_spin.value()),
        )
        SettingsDB.set_setting(
            self.SETTING_MICROSCOPE_MATCH_TOLERANCE_SECONDS,
            int(self.microscope_tolerance_spin.value()),
        )
        self._recompute_matches()

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
            session_grace_seconds=self._microscope_match_tolerance_seconds(),
            exclude_paths=self._excluded_paths,
        )
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
                }
            )
        self.staging_gallery.set_items(items)
        if items:
            selected_paths = [item["filepath"] for item in items if item["filepath"] in existing_selection]
            if not selected_paths:
                selected_paths = [item["filepath"] for item in items]
            self.staging_gallery.select_paths(selected_paths)
            self._on_gallery_selection_changed(selected_paths)
        else:
            self._selected_match_path = None
            self._clear_viewer(
                title=self.tr("No matched images"),
                meta=self.tr("Scan a folder and choose an observation with matched field or microscope imports."),
            )
        total_scanned = len(self._batch_images)
        matched_total = len(self._match_result.get("matches", []))
        unmatched_total = len(self._match_result.get("unmatched", []))
        excluded_total = len(self._excluded_paths)
        microscope_total = sum(
            1 for row in (self._match_result.get("matches", []) or [])
            if str((row or {}).get("image_type") or "").strip().lower() == "microscope"
        )
        field_total = matched_total - microscope_total
        self.batch_summary_label.setText(
            self.tr(
                "Scanned: {scanned} • Matched: {matched} • Field: {field} • Microscope: {microscope} • Unmatched: {unmatched} • Excluded: {excluded}"
            ).format(
                scanned=total_scanned,
                matched=matched_total,
                field=field_total,
                microscope=microscope_total,
                unmatched=unmatched_total,
                excluded=excluded_total,
            )
        )

    def _on_gallery_clicked(self, image_key, _path: str) -> None:
        target_path = str(image_key or "").strip()
        if not target_path:
            return
        self._selected_match_path = target_path
        self.staging_gallery.select_paths([target_path])
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
        if not self._sync_shot_record or not self._batch_images or self._sync_shot_image_path:
            return False
        for row in self._sync_shot_candidate_rows():
            if self._apply_sync_shot_row(row, show_status=False):
                return True
        return False

    def _apply_sync_shot_row(self, row: dict, *, show_status: bool = True) -> bool:
        if not self._sync_shot_record:
            return False
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
        expected_session_id = str(self._sync_shot_record.get("session_id") or "").strip()
        if expected_session_id and session_id != expected_session_id:
            return False
        qr_utc_dt = parsed.get("utc_dt")
        if not isinstance(qr_utc_dt, datetime):
            return False
        offset_info = choose_sync_shot_offset(captured_at, qr_utc_dt)
        chosen_offset = float(offset_info.get("offset_seconds") or 0.0)
        basis = self.tr("local clock") if str(offset_info.get("basis") or "") == "local" else self.tr("UTC clock")
        self._sync_shot_record["applied_utc_text"] = str(parsed.get("utc_text") or "")
        self.offset_spin.blockSignals(True)
        self.offset_spin.setValue(chosen_offset)
        self.offset_spin.blockSignals(False)
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
        if not self._sync_shot_record:
            self._show_status(
                self.tr("Create a Sync Shot first."),
                tone="warning",
                timeout_ms=4000,
            )
            return
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
            return matches
        return [row for row in matches if str(row.get("filepath") or "").strip() in selected_paths]

    def _commit_selected_matches(self) -> None:
        observation_id = self._selected_observation_id()
        if observation_id is None:
            self._show_status(
                self.tr("Choose an observation before committing matched images."),
                tone="warning",
                timeout_ms=5000,
            )
            return
        rows = self._current_commit_rows()
        if not rows:
            self._show_status(
                self.tr("No matched images are selected for commit."),
                tone="warning",
                timeout_ms=4000,
            )
            return
        imported_ids: list[int] = []
        imported_paths: list[str] = []
        for row in rows:
            image_id = self._import_match(observation_id, row)
            if image_id:
                imported_ids.append(int(image_id))
                imported_paths.append(str(row.get("filepath") or "").strip())
        if not imported_ids:
            self._show_status(
                self.tr("No images were committed."),
                tone="warning",
                timeout_ms=4000,
            )
            return
        imported_path_set = set(imported_paths)
        self._batch_images = [
            row for row in self._batch_images if str(row.get("filepath") or "").strip() not in imported_path_set
        ]
        for path in imported_path_set:
            self._excluded_paths.discard(path)
        if self._sync_shot_image_path and self._sync_shot_image_path in imported_path_set:
            self._sync_shot_image_path = None
        self._recompute_matches()
        self._refresh_main_window_after_commit(observation_id, imported_ids[-1])
        self._show_status(
            self.tr("Committed {count} image(s) to the observation.").format(
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

    def _open_selected_observation(self) -> None:
        observation_id = self._selected_observation_id()
        if observation_id is None:
            return
        observation = ObservationDB.get_observation(observation_id)
        display_name = self._observation_title(observation)
        try:
            self._main_window.on_observation_selected(
                observation_id,
                display_name,
                switch_tab=False,
            )
            self._main_window.tab_widget.setCurrentIndex(0)
        except Exception:
            pass

    def _update_action_state(self) -> None:
        has_batch = bool(self._batch_images)
        has_matches = bool(self._matches_for_current_observation())
        has_selection = bool(self._selected_observation_id())
        self.scan_folder_btn.setEnabled(bool(str(self.scan_dir_input.text() or "").strip()))
        self.refresh_matches_btn.setEnabled(has_batch)
        self.commit_matches_btn.setEnabled(has_selection and has_matches)
        self.open_observation_btn.setEnabled(has_selection)
        self.apply_sync_shot_btn.setEnabled(has_batch and bool(self._sync_shot_record))
        self.clear_sync_shot_btn.setEnabled(bool(self._sync_shot_record or self._sync_shot_image_path))
