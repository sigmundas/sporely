"""Dialog for preparing images before creating an observation."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import math
import json
import shutil

from PySide6.QtCore import (
    Qt,
    QDateTime,
    QDate,
    QTime,
    Signal,
    QPointF,
    QCoreApplication,
    QThread,
    QTimer,
    QEvent,
    QSize,
)
from PySide6.QtGui import QPixmap, QKeySequence, QShortcut, QImageReader, QColor, QIcon, QFont
from PySide6.QtWidgets import (
    QButtonGroup,
    QComboBox,
    QDateTimeEdit,
    QDialog,
    QDoubleSpinBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QListView,
    QPushButton,
    QRadioButton,
    QStackedLayout,
    QSizePolicy,
    QSplitter,
    QVBoxLayout,
    QWidget,
    QProgressBar,
    QMessageBox,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QAbstractItemView,
    QCheckBox,
    QToolButton,
)

from database.schema import (
    load_objectives,
    save_objectives,
    get_images_dir,
    get_connection,
    objective_display_name,
    objective_sort_value,
)
from database.models import SettingsDB, ImageDB, MeasurementDB, CalibrationDB
from database.database_tags import DatabaseTerms
from utils.vernacular_utils import normalize_vernacular_language
from utils.exif_reader import get_image_metadata, get_exif_data, get_gps_coordinates
from utils.heic_converter import maybe_convert_heic
from .image_gallery_widget import ImageGalleryWidget
from .zoomable_image_widget import ZoomableImageLabel
from .spore_preview_widget import SporePreviewWidget
from .calibration_dialog import get_resolution_status
from .hint_status import HintBar, HintLabel, HintStatusController
from .dialog_helpers import ask_measurements_exist_delete
from .styles import pt
from .window_state import GeometryMixin


@dataclass
class ImageImportResult:
    filepath: str
    preview_path: Optional[str] = None
    image_id: Optional[int] = None
    image_type: str = "field"
    objective: Optional[str] = None
    custom_scale: Optional[float] = None
    contrast: Optional[str] = None
    mount_medium: Optional[str] = None
    sample_type: Optional[str] = None
    captured_at: Optional[QDateTime] = None
    gps_latitude: Optional[float] = None
    gps_longitude: Optional[float] = None
    needs_scale: bool = False
    exif_has_gps: bool = False
    calibration_id: Optional[int] = None
    ai_crop_box: Optional[tuple[float, float, float, float]] = None
    ai_crop_source_size: Optional[tuple[int, int]] = None
    gps_source: bool = False
    resample_scale_factor: Optional[float] = None
    resize_to_optimal: bool = False
    store_original: bool = False
    original_filepath: Optional[str] = None


class AIGuessWorker(QThread):
    resultReady = Signal(list, list, object, object, list)
    error = Signal(list, str)

    def __init__(
        self,
        requests: list[dict],
        temp_dir: Path,
        max_dim: int = 1600,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.requests: list[dict] = []
        for request in requests or []:
            if not request:
                continue
            index = request.get("index")
            image_path = request.get("image_path")
            if index is None or not image_path:
                continue
            self.requests.append(
                {
                    "index": int(index),
                    "image_path": str(image_path),
                    "crop_box": request.get("crop_box"),
                }
            )
        self.indices = [req["index"] for req in self.requests]
        self.temp_dir = temp_dir
        self.max_dim = max_dim

    def _prepare_image(
        self,
        image_path: str,
        crop_box: tuple[float, float, float, float] | None,
    ) -> Path:
        from uuid import uuid4
        from PIL import Image

        with Image.open(image_path) as img:
            orig_w, orig_h = img.size
            if crop_box:
                x1, y1, x2, y2 = crop_box
                x1n = max(0.0, min(1.0, float(min(x1, x2))))
                y1n = max(0.0, min(1.0, float(min(y1, y2))))
                x2n = max(0.0, min(1.0, float(max(x1, x2))))
                y2n = max(0.0, min(1.0, float(max(y1, y2))))
                crop_x1 = int(max(0, min(orig_w, round(x1n * orig_w))))
                crop_y1 = int(max(0, min(orig_h, round(y1n * orig_h))))
                crop_x2 = int(max(0, min(orig_w, round(x2n * orig_w))))
                crop_y2 = int(max(0, min(orig_h, round(y2n * orig_h))))
                if crop_x2 <= crop_x1:
                    crop_x2 = min(orig_w, crop_x1 + 1)
                    crop_x1 = max(0, crop_x2 - 1)
                if crop_y2 <= crop_y1:
                    crop_y2 = min(orig_h, crop_y1 + 1)
                    crop_y1 = max(0, crop_y2 - 1)
                img = img.crop((crop_x1, crop_y1, crop_x2, crop_y2))

            if img.mode not in {"RGB", "L"}:
                img = img.convert("RGB")

            self.temp_dir.mkdir(parents=True, exist_ok=True)
            temp_path = self.temp_dir / f"ai_guess_{uuid4().hex}.jpg"
            img.save(temp_path, "JPEG", quality=90)
            return temp_path

    def run(self) -> None:
        if not self.requests:
            return
        try:
            import requests

            send_paths: list[Path] = []
            temp_paths: list[str] = []
            for request in self.requests:
                temp_path = self._prepare_image(request["image_path"], request.get("crop_box"))
                send_paths.append(temp_path)
                temp_paths.append(str(temp_path))

            url = "https://ai.artsdatabanken.no"
            handles: list[object] = []
            response = None
            try:
                files = []
                for path in send_paths:
                    handle = open(path, "rb")
                    handles.append(handle)
                    files.append(("image", (path.name, handle, "image/jpeg")))
                response = requests.post(
                    url,
                    files=files,
                    headers={"User-Agent": "MycoLog/AI"},
                    timeout=30,
                )
                if response.status_code != 200 and len(send_paths) == 1:
                    for handle in handles:
                        handle.close()
                    handles = []
                    with open(send_paths[0], "rb") as handle:
                        response = requests.post(
                            url,
                            files={"file": (send_paths[0].name, handle, "image/jpeg")},
                            headers={"User-Agent": "MycoLog/AI"},
                            timeout=30,
                        )
            finally:
                for handle in handles:
                    try:
                        handle.close()
                    except Exception:
                        pass

            if response is None or response.status_code != 200:
                detail = (response.text or "").strip() if response is not None else ""
                if detail:
                    detail = detail.replace("\n", " ").strip()
                    detail = detail[:200]
                suffix = f" - {detail}" if detail else ""
                status = response.status_code if response is not None else "no response"
                raise Exception(f"API request failed: {status}{suffix}")

            data = response.json()
            predictions = [
                p for p in data.get("predictions", [])
                if p.get("taxon", {}).get("vernacularName") != "*** Utdatert versjon ***"
            ]

            warnings = data.get("warnings")
            self.resultReady.emit(self.indices, predictions, None, warnings, temp_paths)
        except Exception as exc:
            self.error.emit(self.indices, str(exc))
        finally:
            for path in send_paths:
                try:
                    path.unlink(missing_ok=True)
                except Exception:
                    pass


class ScaleBarImportDialog(QDialog):
    """Scale bar calibration dialog used from Prepare Images."""

    def __init__(self, image_dialog, initial_um: float = 10.0, previous_key: str | None = None):
        super().__init__(image_dialog)
        self.setWindowTitle(self.tr("Scale bar"))
        self.setModal(False)
        self.image_dialog = image_dialog
        self.previous_key = previous_key
        self.scale_applied = False
        self.auto_apply = False
        self._pending_distance_px: float | None = None

        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.length_input = QDoubleSpinBox()
        self.length_input.setRange(0.1, 100000.0)
        self.length_input.setDecimals(2)
        self.length_input.setValue(initial_um)
        self.length_input.setSuffix(" um")
        self.length_input.valueChanged.connect(self._update_scale_label)
        form.addRow(self.tr("Scale bar length:"), self.length_input)

        self.scale_label = QLabel("--")
        form.addRow(self.tr("Custom scale:"), self.scale_label)
        layout.addLayout(form)

        self.preview = SporePreviewWidget()
        self.preview.set_show_dimension_labels(False)
        self.preview.dimensions_changed.connect(self._on_preview_changed)
        layout.addWidget(self.preview, 1)

        btn_row = QHBoxLayout()
        self.select_btn = QPushButton(self.tr("Select scale bar endpoints"))
        self.select_btn.clicked.connect(self._on_select)
        btn_row.addWidget(self.select_btn)

        self.apply_btn = QPushButton(self.tr("Apply scale"))
        self.apply_btn.clicked.connect(self._on_apply)
        btn_row.addWidget(self.apply_btn)
        btn_row.addStretch()

        close_btn = QPushButton(self.tr("Close"))
        close_btn.clicked.connect(self.close)
        btn_row.addWidget(close_btn)
        layout.addLayout(btn_row)

    def _on_select(self) -> None:
        if not self.image_dialog:
            return
        self.hide()
        self.image_dialog.enter_calibration_mode(self)

    def _on_apply(self) -> None:
        if not self._pending_distance_px:
            return
        scale_um = float(self.length_input.value()) / self._pending_distance_px
        self.scale_applied = True
        if self.image_dialog:
            self.image_dialog.apply_scale_bar(scale_um)

    def _on_preview_changed(self, _measurement_id, new_length_um, new_width_um, _points):
        length_px = max(float(new_length_um), float(new_width_um))
        if length_px <= 0:
            return
        self._pending_distance_px = length_px
        self._update_scale_label()

    def _update_scale_label(self) -> None:
        if not self._pending_distance_px:
            self.scale_label.setText("--")
            return
        scale_um = float(self.length_input.value()) / self._pending_distance_px
        scale_nm = scale_um * 1000.0
        self.scale_label.setText(f"{scale_nm:.2f} nm/px")

    def set_calibration_distance(self, distance_pixels: float):
        if not distance_pixels or distance_pixels <= 0:
            return
        self._pending_distance_px = float(distance_pixels)
        self._update_scale_label()
        self.show()

    def set_calibration_preview(self, pixmap, points):
        if not pixmap or not points or len(points) != 4:
            return
        length_px = self._pending_distance_px or 0.0
        width_px = max(2.0, length_px * 0.1) if length_px > 0 else 2.0
        self.preview.set_spore(pixmap, points, length_px, width_px, 1.0, measurement_id=1)

    def apply_scale(self, distance_pixels: float):
        self.set_calibration_distance(distance_pixels)
        if self._pending_distance_px:
            self._on_apply()

    def closeEvent(self, event):
        if not self.scale_applied and self.image_dialog:
            self.image_dialog._populate_objectives(selected_key=self.previous_key)
            if self.previous_key is None:
                self.image_dialog.objective_combo.setCurrentIndex(0)
        super().closeEvent(event)
class ImageImportDialog(GeometryMixin, QDialog):
    """Prepare images before creating or editing an observation."""

    continueRequested = Signal(list)
    _geometry_key = "ImageImportDialog"
    CUSTOM_OBJECTIVE_KEY = "__custom__"

    def __init__(
        self,
        parent=None,
        image_paths: Optional[list[str]] = None,
        import_results: Optional[list[ImageImportResult]] = None,
        observation_datetime: QDateTime | None = None,
        observation_lat: float | None = None,
        observation_lon: float | None = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle(self.tr("Prepare Images"))
        self.setModal(True)
        self.setMinimumSize(1200, 800)
        self.resize(1500, 900)

        self.objectives = self._load_objectives()
        self.default_objective = self._get_default_objective()
        self.contrast_options = self._load_tag_options("contrast")
        self.mount_options = self._load_tag_options("mount")
        self.sample_options = self._load_tag_options("sample")
        self.contrast_default = self._preferred_tag_value(
            "contrast",
            self.contrast_options,
            DatabaseTerms.CONTRAST_METHODS[0],
        )
        self.mount_default = self._preferred_tag_value(
            "mount",
            self.mount_options,
            DatabaseTerms.MOUNT_MEDIA[0],
        )
        self.sample_default = self._preferred_tag_value(
            "sample",
            self.sample_options,
            DatabaseTerms.SAMPLE_TYPES[0],
        )
        self.resize_to_optimal_default = bool(
            SettingsDB.get_setting("resize_to_optimal_sampling", False)
        )
        storage_mode = SettingsDB.get_setting("original_storage_mode")
        if not storage_mode:
            storage_mode = (
                "observation" if SettingsDB.get_setting("store_original_images", False) else "none"
            )
        self.store_original_default = storage_mode != "none"
        self.target_sampling_pct = float(
            SettingsDB.get_setting("target_sampling_pct", 120.0)
        )
        self._resize_preview_enabled = bool(self.resize_to_optimal_default)

        self.image_paths: list[str] = []
        self.import_results: list[ImageImportResult] = []
        self.selected_index: int | None = None
        self.selected_indices: list[int] = []
        self.primary_index: int | None = None
        self._loading_form = False
        self._temp_preview_paths: set[str] = set()
        self._custom_scale: float | None = None
        self._current_exif_datetime: QDateTime | None = None
        self._current_exif_lat: float | None = None
        self._current_exif_lon: float | None = None
        self._current_exif_path: str | None = None
        self._missing_exif_paths: set[str] = set()
        self._missing_exif_warning_scheduled = False
        self._pixmap_cache: dict[str, QPixmap] = {}
        self._pixmap_cache_is_preview: dict[str, bool] = {}
        self._max_preview_dim = 1600
        self._unset_datetime = QDateTime(QDate(1900, 1, 1), QTime(0, 0))
        if observation_datetime is not None and not isinstance(observation_datetime, QDateTime):
            try:
                observation_datetime = QDateTime(observation_datetime)
            except Exception:
                observation_datetime = None
        self._observation_datetime: QDateTime | None = observation_datetime
        self._observation_lat: float | None = observation_lat
        self._observation_lon: float | None = observation_lon
        self._observation_source_index: int | None = None
        self._converted_import_paths: set[str] = set()
        self._accepted = False
        self._setting_from_image_source = False
        self._last_settings_action: str | None = None
        self._ai_predictions_by_index: dict[int, list[dict]] = {}
        self._ai_selected_by_index: dict[int, dict] = {}
        self._ai_selected_taxon: dict | None = None
        self._ai_crop_boxes: dict[int, tuple[float, float, float, float]] = {}
        self._ai_crop_active = False
        self._ai_thread: QThread | None = None
        self._scale_bar_dialog: ScaleBarImportDialog | None = None
        self._last_objective_key: str | None = None
        self._hint_controller: HintStatusController | None = None
        self._pending_hint_widgets: list[tuple[QWidget, str, str]] = []

        self._build_ui()
        if hasattr(self, "objective_combo"):
            self._last_objective_key = self.objective_combo.currentData()
        if import_results:
            self.set_import_results(import_results)
        elif image_paths:
            self.add_images(image_paths)
        self._restore_geometry()
        self.finished.connect(self._save_geometry)

    def _build_ui(self) -> None:
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(8)

        content_row = QHBoxLayout()
        content_row.setSpacing(10)

        left_panel = self._build_left_panel()
        left_panel.setFixedWidth(280)
        left_panel.setStyleSheet(
            "QPushButton { padding: 4px 8px; }"
            "QLineEdit, QComboBox, QSpinBox, QDoubleSpinBox { padding: 4px 6px; }"
        )
        left_panel.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Expanding)
        content_row.addWidget(left_panel, 0)

        center_container = QWidget()
        center_layout = QVBoxLayout(center_container)
        center_layout.setContentsMargins(0, 0, 0, 0)
        center_layout.setSpacing(8)

        self.gallery = ImageGalleryWidget(
            self.tr("Images"),
            self,
            show_delete=True,
            show_badges=True,
            min_height=60,
            default_height=180,
            thumbnail_size=140,
        )
        self.gallery.set_multi_select(True)
        self.gallery.imageClicked.connect(self._on_gallery_clicked)
        self.gallery.selectionChanged.connect(self._on_gallery_selection_changed)
        self.gallery.deleteRequested.connect(self._on_gallery_delete_requested)
        self.delete_shortcut = QShortcut(QKeySequence.Delete, self)
        self.delete_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.delete_shortcut.activated.connect(self._on_remove_selected)
        self.resize_preview_shortcut = QShortcut(QKeySequence("R"), self)
        self.resize_preview_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.resize_preview_shortcut.activated.connect(self._toggle_resize_preview)
        self.ai_crop_shortcut = QShortcut(QKeySequence("C"), self)
        self.ai_crop_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.ai_crop_shortcut.activated.connect(self._on_ai_crop_clicked)
        self.field_shortcut = QShortcut(QKeySequence("F"), self)
        self.field_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.field_shortcut.activated.connect(lambda: self._apply_image_type_shortcut("field"))
        self.micro_shortcut = QShortcut(QKeySequence("M"), self)
        self.micro_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.micro_shortcut.activated.connect(lambda: self._apply_image_type_shortcut("micro"))
        self.apply_all_shortcut = QShortcut(QKeySequence("A"), self)
        self.apply_all_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.apply_all_shortcut.activated.connect(self._apply_to_all)
        self.scale_shortcut = QShortcut(QKeySequence("S"), self)
        self.scale_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.scale_shortcut.activated.connect(self._on_scale_shortcut)

        center_splitter = QSplitter(Qt.Vertical)
        center_splitter.setChildrenCollapsible(False)
        center_splitter.addWidget(self._build_center_panel())
        center_splitter.addWidget(self.gallery)
        center_splitter.setStretchFactor(0, 4)
        center_splitter.setStretchFactor(1, 1)
        center_splitter.setSizes([700, 220])

        center_layout.addWidget(center_splitter, 1)
        self.details_panel = self._build_right_panel()
        main_splitter = QSplitter(Qt.Horizontal)
        main_splitter.setChildrenCollapsible(False)
        main_splitter.addWidget(center_container)
        main_splitter.addWidget(self.details_panel)
        main_splitter.setStretchFactor(0, 1)
        main_splitter.setStretchFactor(1, 0)
        main_splitter.setSizes([980, 400])
        content_row.addWidget(main_splitter, 1)
        main_layout.addLayout(content_row, 1)

        bottom_row = QHBoxLayout()
        bottom_row.setContentsMargins(0, 0, 0, 0)
        bottom_row.setSpacing(8)
        self.hint_bar = HintBar(self)
        bottom_row.addWidget(self.hint_bar, 1)
        self._hint_controller = HintStatusController(self.hint_bar, self)
        if self._pending_hint_widgets:
            for widget, hint, tone in self._pending_hint_widgets:
                if widget is not None:
                    self._hint_controller.register_widget(widget, hint, tone=tone)
            self._pending_hint_widgets.clear()

        self.cancel_btn = QPushButton(self.tr("Cancel"))
        self.cancel_btn.setMinimumHeight(35)
        self.cancel_btn.clicked.connect(self.reject)
        bottom_row.addWidget(self.cancel_btn)

        self.next_btn = QPushButton(self.tr("Continue"))
        self.next_btn.setMinimumHeight(35)
        self.next_btn.clicked.connect(self._accept_continue)
        bottom_row.addWidget(self.next_btn)
        main_layout.addLayout(bottom_row)

    def showEvent(self, event) -> None:
        super().showEvent(event)
        QTimer.singleShot(0, self._ensure_initial_preview)

    def _ensure_initial_preview(self) -> None:
        if not self.image_paths:
            return
        if getattr(self, "selected_indices", None) and len(self.selected_indices) > 1:
            return
        current_pixmap = getattr(self.preview, "original_pixmap", None) if hasattr(self, "preview") else None
        if current_pixmap is None or current_pixmap.isNull():
            index = self.selected_index if self.selected_index is not None else 0
            self._select_image(index)
        elif hasattr(self, "preview_stack") and self.preview_stack.currentWidget() != self.preview:
            self.preview_stack.setCurrentWidget(self.preview)

    def _apply_combo_popup_style(self, combo: QComboBox) -> None:
        view = QListView()
        view.setSpacing(3)
        view.setStyleSheet(
            "QListView { background: white; color: #2c3e50; }"
            "QListView::item { color: #2c3e50; background: white; padding: 4px 8px; min-height: 24px; }"
            "QListView::item:hover { background: #d9e9f8; color: #2c3e50; }"
            "QListView::item:selected { background: #3498db; color: white; }"
        )
        combo.setView(view)

    def _load_tag_options(self, category: str) -> list[str]:
        setting_key = DatabaseTerms.setting_key(category)
        defaults = DatabaseTerms.default_values(category)
        options = SettingsDB.get_list_setting(setting_key, defaults)
        return DatabaseTerms.canonicalize_list(category, options)

    def _canonicalize_tag(self, category: str, value: str | None) -> str | None:
        return DatabaseTerms.canonicalize(category, value)

    def _preferred_tag_value(self, category: str, options: list[str], fallback: str) -> str:
        options = options or [fallback]
        legacy_default_key = {
            "contrast": "contrast_default",
            "mount": "mount_default",
            "sample": "sample_default",
        }.get(category, "")
        preferred = SettingsDB.get_setting(DatabaseTerms.last_used_key(category), None)
        if not preferred and legacy_default_key:
            preferred = SettingsDB.get_setting(legacy_default_key, None)
        preferred = self._canonicalize_tag(category, preferred)
        if preferred and preferred in options:
            return preferred
        if preferred and preferred not in options:
            options.insert(0, preferred)
        return options[0] if options else fallback

    def _populate_tag_combo(self, combo: QComboBox, category: str, options: list[str]) -> None:
        combo.clear()
        for canonical in options:
            combo.addItem(DatabaseTerms.translate(category, canonical), canonical)

    def _set_combo_tag_value(self, combo: QComboBox, category: str, value: str | None) -> None:
        canonical = self._canonicalize_tag(category, value)
        if not canonical:
            return
        idx = combo.findData(canonical)
        if idx >= 0:
            combo.setCurrentIndex(idx)
            return
        combo.addItem(DatabaseTerms.translate(category, canonical), canonical)
        combo.setCurrentIndex(combo.count() - 1)

    def _get_combo_tag_value(self, combo: QComboBox, category: str) -> str | None:
        value = combo.currentData()
        if value is None:
            value = combo.currentText()
        return self._canonicalize_tag(category, value)

    def _build_left_panel(self) -> QWidget:
        container = QWidget()
        outer = QVBoxLayout(container)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(8)

        add_btn = QPushButton(self.tr("Add Images..."))
        add_btn.clicked.connect(self._on_add_images_clicked)
        outer.addWidget(add_btn)
        self.import_progress = QProgressBar()
        self.import_progress.setVisible(False)
        self.import_progress.setRange(0, 1)
        self.import_progress.setFormat(self.tr("Loading images... %p%"))
        outer.addWidget(self.import_progress)

        panel = QGroupBox(self.tr("Image settings"))
        layout = QVBoxLayout(panel)
        layout.setSpacing(8)

        type_group = QGroupBox(self.tr("Image type"))
        type_layout = QHBoxLayout(type_group)
        self.image_type_group = QButtonGroup(self)
        self.field_radio = QRadioButton(self.tr("Field (F)"))
        self.micro_radio = QRadioButton(self.tr("Micro (M)"))
        self.image_type_group.addButton(self.field_radio)
        self.image_type_group.addButton(self.micro_radio)
        self.field_radio.setChecked(True)
        self.image_type_group.buttonClicked.connect(self._on_settings_changed)
        type_layout.addWidget(self.field_radio)
        type_layout.addWidget(self.micro_radio)
        layout.addWidget(type_group)

        self.scale_group = QGroupBox(self.tr("Scale"))
        scale_layout = QVBoxLayout(self.scale_group)
        self.objective_combo = QComboBox()
        self._apply_combo_popup_style(self.objective_combo)
        self._populate_objectives()
        self.objective_combo.currentIndexChanged.connect(self._on_settings_changed)
        scale_layout.addWidget(self.objective_combo)
        self.calibrate_btn = QPushButton(self.tr("Set from scalebar"))
        self.calibrate_btn.clicked.connect(self._open_calibration_dialog)
        scale_layout.addWidget(self.calibrate_btn)
        self.scale_warning_label = QLabel("")
        self.scale_warning_label.setWordWrap(True)
        self.scale_warning_label.setStyleSheet(f"color: #e74c3c; font-weight: bold; font-size: {pt(9)}pt;")
        self.scale_warning_label.setVisible(False)
        scale_layout.addWidget(self.scale_warning_label)
        layout.addWidget(self.scale_group)

        self.contrast_group = QGroupBox(self.tr("Contrast"))
        contrast_layout = QVBoxLayout(self.contrast_group)
        self.contrast_combo = QComboBox()
        self._apply_combo_popup_style(self.contrast_combo)
        self._populate_tag_combo(self.contrast_combo, "contrast", self.contrast_options)
        self._set_combo_tag_value(self.contrast_combo, "contrast", self.contrast_default)
        self.contrast_combo.currentIndexChanged.connect(self._on_settings_changed)
        contrast_layout.addWidget(self.contrast_combo)
        layout.addWidget(self.contrast_group)

        self.mount_group = QGroupBox(self.tr("Mount"))
        mount_layout = QVBoxLayout(self.mount_group)
        self.mount_combo = QComboBox()
        self._apply_combo_popup_style(self.mount_combo)
        self._populate_tag_combo(self.mount_combo, "mount", self.mount_options)
        self._set_combo_tag_value(self.mount_combo, "mount", self.mount_default)
        self.mount_combo.currentIndexChanged.connect(self._on_settings_changed)
        mount_layout.addWidget(self.mount_combo)
        layout.addWidget(self.mount_group)

        self.sample_group = QGroupBox(self.tr("Sample type"))
        sample_layout = QVBoxLayout(self.sample_group)
        self.sample_combo = QComboBox()
        self._apply_combo_popup_style(self.sample_combo)
        self._populate_tag_combo(self.sample_combo, "sample", self.sample_options)
        self._set_combo_tag_value(self.sample_combo, "sample", self.sample_default)
        self.sample_combo.currentIndexChanged.connect(self._on_settings_changed)
        sample_layout.addWidget(self.sample_combo)
        layout.addWidget(self.sample_group)

        layout.addStretch()
        apply_row = QHBoxLayout()
        self.apply_all_btn = QPushButton(self.tr("Apply to all (A)"))
        self.apply_all_btn.clicked.connect(self._apply_to_all)
        apply_row.addWidget(self.apply_all_btn)
        layout.addLayout(apply_row)
        outer.addWidget(panel, 1)
        return container

    def _build_center_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        self.preview_stack = QStackedLayout()

        self.preview = ZoomableImageLabel()
        self.preview.setMinimumHeight(420)
        self.preview.setAlignment(Qt.AlignCenter)
        self.preview.set_pan_without_shift(True)
        self.preview.clicked.connect(self._on_preview_clicked)
        self.preview.cropChanged.connect(self._on_ai_crop_changed)
        self.preview.cropPreviewChanged.connect(self._on_ai_crop_preview_changed)
        self.preview.installEventFilter(self)

        self.rotate_preview_btn = QToolButton(self.preview)
        self.rotate_preview_btn.setIcon(QIcon(str(Path(__file__).parent.parent / "assets" / "icons" / "rotate.svg")))
        self.rotate_preview_btn.setIconSize(QSize(20, 20))
        self.rotate_preview_btn.setToolTip(self.tr("Rotate 90 deg clockwise"))
        self.rotate_preview_btn.setStyleSheet(
            "QToolButton { background-color: rgba(255, 255, 255, 200); border: 1px solid #d0d0d0; border-radius: 4px; }"
            "QToolButton:hover { background-color: rgba(236, 240, 241, 230); }"
            "QToolButton:disabled { background-color: rgba(220, 220, 220, 140); }"
        )
        self.rotate_preview_btn.setFixedSize(26, 26)
        self.rotate_preview_btn.move(8, 8)
        self.rotate_preview_btn.clicked.connect(self._on_rotate_current_image_clicked)
        self.rotate_preview_btn.raise_()

        self.preview_message = QLabel(self.tr("Multiple images selected"))
        self.preview_message.setAlignment(Qt.AlignCenter)
        self.preview_message.setStyleSheet(f"color: #7f8c8d; font-size: {pt(12)}pt;")

        self.preview_stack.addWidget(self.preview)
        self.preview_stack.addWidget(self.preview_message)
        layout.addLayout(self.preview_stack, 1)
        self._update_rotate_button_state()
        return panel

    def _build_right_panel(self) -> QWidget:
        panel = QGroupBox(self.tr("Import details"))
        panel.setMinimumWidth(340)
        panel.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Expanding)
        layout = QVBoxLayout(panel)

        current_group = QGroupBox(self.tr("Current image"))
        current_layout = QFormLayout(current_group)
        self.exif_datetime_label = QLabel("--")
        self.exif_camera_label = QLabel("--")
        self.exif_iso_label = QLabel("--")
        self.exif_shutter_label = QLabel("--")
        self.exif_aperture_label = QLabel("--")
        self.exif_lat_label = QLabel("Lat: --")
        self.exif_lon_label = QLabel("Lon: --")
        self.exif_map_btn = QPushButton(self.tr("Map"))
        self.exif_map_btn.clicked.connect(self._open_current_image_map)
        self.exif_map_btn.setEnabled(False)

        current_layout.addRow(self.tr("Date & time:"), self.exif_datetime_label)
        current_layout.addRow(self.tr("Camera:"), self.exif_camera_label)
        current_layout.addRow(self.tr("ISO:"), self.exif_iso_label)
        current_layout.addRow(self.tr("Shutter:"), self.exif_shutter_label)
        current_layout.addRow(self.tr("F-stop:"), self.exif_aperture_label)

        gps_values = QVBoxLayout()
        gps_values.addWidget(self.exif_lat_label)
        gps_values.addWidget(self.exif_lon_label)
        gps_row = QHBoxLayout()
        gps_row.addLayout(gps_values, 1)
        gps_row.addWidget(self.exif_map_btn)
        current_layout.addRow(self.tr("GPS:"), gps_row)
        layout.addWidget(current_group)

        obs_group = QGroupBox(self.tr("Time and GPS"))
        obs_layout = QFormLayout(obs_group)

        self.datetime_input = QDateTimeEdit()
        self.datetime_input.setMinimumDateTime(self._unset_datetime)
        self.datetime_input.setSpecialValueText("--")
        self.datetime_input.setCalendarPopup(True)
        self.datetime_input.setDisplayFormat("yyyy-MM-dd HH:mm")
        self.datetime_input.dateTimeChanged.connect(self._on_metadata_changed)
        self.datetime_input.setDateTime(self._unset_datetime)
        obs_layout.addRow(self.tr("Date & time:"), self.datetime_input)

        gps_container = QWidget()
        gps_container_layout = QVBoxLayout(gps_container)
        gps_container_layout.setContentsMargins(0, 0, 0, 0)
        gps_container_layout.setSpacing(4)

        gps_lat_row = QHBoxLayout()
        gps_lat_row.setContentsMargins(0, 0, 0, 0)
        gps_lat_row.setSpacing(6)
        self.lat_input = QDoubleSpinBox()
        self.lat_input.setRange(-90.0, 90.0)
        self.lat_input.setDecimals(6)
        self.lat_input.setAlignment(Qt.AlignLeft)
        self.lat_input.setSpecialValueText("--")
        self.lat_input.setValue(self.lat_input.minimum())
        self.lat_input.valueChanged.connect(self._on_metadata_changed)
        gps_lat_row.addWidget(QLabel(self.tr("Lat:")))
        gps_lat_row.addWidget(self.lat_input, 0)
        gps_lat_row.addStretch(1)
        gps_container_layout.addLayout(gps_lat_row)

        gps_lon_row = QHBoxLayout()
        gps_lon_row.setContentsMargins(0, 0, 0, 0)
        gps_lon_row.setSpacing(6)
        self.lon_input = QDoubleSpinBox()
        self.lon_input.setRange(-180.0, 180.0)
        self.lon_input.setDecimals(6)
        self.lon_input.setAlignment(Qt.AlignLeft)
        self.lon_input.setSpecialValueText("--")
        self.lon_input.setValue(self.lon_input.minimum())
        self.lon_input.valueChanged.connect(self._on_metadata_changed)
        gps_lon_row.addWidget(QLabel(self.tr("Lon:")))
        gps_lon_row.addWidget(self.lon_input, 0)
        gps_lon_row.addStretch(1)
        gps_container_layout.addLayout(gps_lon_row)

        gps_container_layout.addSpacing(4)
        self.set_from_image_btn = QPushButton(self.tr("Set from current image"))
        self.set_from_image_btn.clicked.connect(self._set_observation_gps_from_image)
        self.set_from_image_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        gps_container_layout.addWidget(self.set_from_image_btn)
        obs_layout.addRow(self.tr("GPS:"), gps_container)

        layout.addWidget(obs_group)

        resize_group = QGroupBox(self.tr("Image resize"))
        self.resize_group = resize_group
        resize_layout = QVBoxLayout(resize_group)
        resize_layout.setContentsMargins(6, 6, 6, 6)
        self.resize_optimal_checkbox = QCheckBox(
            self.tr("Resize to optimal sampling (R)")
        )
        self.resize_optimal_checkbox.setChecked(self.resize_to_optimal_default)
        self.resize_optimal_checkbox.toggled.connect(self._on_resize_settings_changed)
        resize_hint = self.tr(
            "Enable optimal downsampling for microscope images. Keyboard shortcut R toggles resize."
        )
        self._register_hint_widget(self.resize_optimal_checkbox, resize_hint)
        resize_layout.addWidget(self.resize_optimal_checkbox)

        resize_info = QFormLayout()
        self.target_sampling_input = QDoubleSpinBox()
        self.target_sampling_input.setRange(50.0, 300.0)
        self.target_sampling_input.setDecimals(0)
        self.target_sampling_input.setSuffix("%")
        self.target_sampling_input.setValue(float(self.target_sampling_pct))
        self.target_sampling_input.valueChanged.connect(self._on_target_sampling_changed)
        self.target_sampling_input.installEventFilter(self)
        self.current_resolution_title = HintLabel(
            self.tr("Current resolution:"),
            self.tr("Current pixel dimensions and megapixels for this image."),
            self.set_hint,
            self,
        )
        self.target_resolution_title = HintLabel(
            self.tr("Ideal resolution:"),
            self.tr("Ideal output resolution from objective scale, NA, and target sampling."),
            self.set_hint,
            self,
        )
        self.current_resolution_label = QLabel("--")
        self.target_resolution_label = QLabel("--")
        resize_info.addRow(
            self.tr("Ideal sampling (% Nyquist):"),
            self.target_sampling_input,
        )
        resize_info.addRow(self.current_resolution_title, self.current_resolution_label)
        resize_info.addRow(self.target_resolution_title, self.target_resolution_label)
        resize_layout.addLayout(resize_info)
        self._sync_resize_controls()

        layout.addWidget(resize_group)

        ai_crop_group = QGroupBox(self.tr("AI crop"))
        ai_crop_layout = QVBoxLayout(ai_crop_group)
        ai_crop_layout.setContentsMargins(6, 6, 6, 6)
        self.ai_crop_btn = QPushButton(self.tr("Crop (C)"))
        crop_hint = self.tr(
            "Draw a crop area for Artsorakelet. Keyboard shortcut C."
        )
        self._register_hint_widget(self.ai_crop_btn, crop_hint)
        self.ai_crop_btn.clicked.connect(self._on_ai_crop_clicked)
        self.ai_crop_btn.setEnabled(False)
        ai_crop_layout.addWidget(self.ai_crop_btn)
        self.ai_crop_size_label = QLabel("")
        self.ai_crop_size_label.setStyleSheet("color: #7f8c8d;")
        self.ai_crop_size_label.setVisible(False)
        ai_crop_layout.addWidget(self.ai_crop_size_label)
        self._set_ai_crop_active(False)

        layout.addWidget(ai_crop_group)
        layout.addStretch(1)
        return panel

    def _populate_objectives(self, selected_key: str | None = None) -> None:
        self.objective_combo.blockSignals(True)
        self.objective_combo.clear()
        self.objective_combo.addItem(self.tr("Not set"), None)
        self.objective_combo.addItem(self.tr("Scale bar"), self.CUSTOM_OBJECTIVE_KEY)
        for key, obj in sorted(self.objectives.items(), key=lambda item: objective_sort_value(item[1], item[0])):
            label = objective_display_name(obj, key) or key
            self.objective_combo.addItem(label, key)
        if selected_key is None:
            selected_key = self.default_objective
        if selected_key:
            idx = self.objective_combo.findData(selected_key)
            if idx >= 0:
                self.objective_combo.setCurrentIndex(idx)
        self._last_objective_key = self.objective_combo.currentData()
        self.objective_combo.blockSignals(False)

    def _open_calibration_dialog(self, previous_key: str | None = None) -> None:
        if previous_key is None:
            previous_key = self.objective_combo.currentData() if hasattr(self, "objective_combo") else None
        dialog = getattr(self, "_scale_bar_dialog", None)
        if dialog and dialog.isVisible():
            dialog.raise_()
            dialog.activateWindow()
            return
        dialog = ScaleBarImportDialog(self, previous_key=previous_key)
        dialog.finished.connect(lambda _result: setattr(self, "_scale_bar_dialog", None))
        self._scale_bar_dialog = dialog
        dialog.show()

    def apply_scale_bar(self, scale_um: float) -> None:
        if not scale_um or scale_um <= 0:
            return
        self._custom_scale = float(scale_um)
        self._populate_objectives(selected_key=self.CUSTOM_OBJECTIVE_KEY)
        idx = self.objective_combo.findData(self.CUSTOM_OBJECTIVE_KEY)
        self._loading_form = True
        if idx >= 0:
            self.objective_combo.setCurrentIndex(idx)
        self._loading_form = False
        indices = self.selected_indices or ([self.selected_index] if self.selected_index is not None else [])
        if indices:
            self._apply_settings_to_indices(indices, action="scale")

    def _on_calibration_saved(self, objective: dict) -> None:
        if not isinstance(objective, dict):
            return
        custom_scale = objective.get("microns_per_pixel")
        key = objective.get("key") or objective.get("objective_key") or objective.get("magnification") or ""
        is_custom = str(key).lower() == "custom" or key == self.CUSTOM_OBJECTIVE_KEY
        if is_custom and isinstance(custom_scale, (int, float)):
            self._custom_scale = float(custom_scale)
            self._populate_objectives(selected_key=self.CUSTOM_OBJECTIVE_KEY)
            idx = self.objective_combo.findData(self.CUSTOM_OBJECTIVE_KEY)
            if idx >= 0:
                self.objective_combo.setCurrentIndex(idx)

    def _load_objectives(self):
        return load_objectives()

    def _get_default_objective(self):
        for key, obj in self.objectives.items():
            if obj.get("is_default"):
                return key
        if self.objectives:
            return sorted(self.objectives.keys())[0]
        return None

    def _current_selection_indices(self) -> list[int]:
        if self.selected_indices:
            return [idx for idx in self.selected_indices if idx is not None]
        if self.selected_index is not None:
            return [self.selected_index]
        return []

    def _is_resized_image(self, result: ImageImportResult | None) -> bool:
        if not result:
            return False
        orig = getattr(result, "original_filepath", None)
        path = getattr(result, "filepath", None)
        if orig and path and orig != path:
            return True
        factor = getattr(result, "resample_scale_factor", None)
        if result.image_id and isinstance(factor, (int, float)) and factor > 0 and factor < 0.999:
            return True
        return False

    def _update_scale_group_state(self) -> None:
        if not hasattr(self, "scale_group"):
            return
        indices = self._current_selection_indices()
        if not indices:
            self.scale_group.setEnabled(False)
            self._update_micro_settings_state(False)
            self._update_scale_mismatch_warning()
            return
        enable = all(
            self.import_results[idx].image_type == "microscope"
            for idx in indices
            if 0 <= idx < len(self.import_results)
        )
        self.scale_group.setEnabled(enable)
        self._update_micro_settings_state(enable)
        self._update_resize_group_state()
        self._update_scale_mismatch_warning()

    def _update_resize_group_state(self) -> None:
        if not hasattr(self, "resize_group"):
            return
        indices = self._current_selection_indices()
        if not indices:
            self.resize_group.setEnabled(False)
            return
        all_micro = all(
            self.import_results[idx].image_type == "microscope"
            for idx in indices
            if 0 <= idx < len(self.import_results)
        )
        if not all_micro:
            self.resize_group.setEnabled(False)
            return
        selected_objective = self.objective_combo.currentData() if hasattr(self, "objective_combo") else None
        if selected_objective == self.CUSTOM_OBJECTIVE_KEY:
            self.resize_group.setEnabled(False)
            return
        any_resized = any(
            self._is_resized_image(self.import_results[idx])
            for idx in indices
            if 0 <= idx < len(self.import_results)
        )
        self.resize_group.setEnabled(not any_resized)
        self._sync_resize_controls()

    def _sync_resize_controls(self) -> None:
        if not hasattr(self, "resize_optimal_checkbox"):
            return
        return

    def _update_micro_settings_state(self, enable: bool | None = None) -> None:
        if not hasattr(self, "contrast_combo"):
            return
        if enable is None:
            indices = self._current_selection_indices()
            if not indices:
                enable = False
            else:
                enable = all(
                    self.import_results[idx].image_type == "microscope"
                    for idx in indices
                    if 0 <= idx < len(self.import_results)
                )
        self.contrast_combo.setEnabled(enable)
        self.mount_combo.setEnabled(enable)
        self.sample_combo.setEnabled(enable)
        if hasattr(self, "contrast_group"):
            self.contrast_group.setEnabled(enable)
        if hasattr(self, "mount_group"):
            self.mount_group.setEnabled(enable)
        if hasattr(self, "sample_group"):
            self.sample_group.setEnabled(enable)

    def _update_set_from_image_button_state(self) -> None:
        if not hasattr(self, "set_from_image_btn"):
            return
        indices = self._current_selection_indices()
        if len(indices) != 1:
            self.set_from_image_btn.setEnabled(False)
            self.set_from_image_btn.setStyleSheet(
                "background-color: #bdc3c7; color: #7f8c8d;"
            )
            return
        idx = indices[0]
        if idx < 0 or idx >= len(self.import_results):
            self.set_from_image_btn.setEnabled(False)
            self.set_from_image_btn.setStyleSheet(
                "background-color: #bdc3c7; color: #7f8c8d;"
            )
            return
        if self.import_results[idx].image_type == "microscope":
            self.set_from_image_btn.setEnabled(False)
            self.set_from_image_btn.setStyleSheet(
                "background-color: #bdc3c7; color: #7f8c8d;"
            )
            return
        has_exif_data = (
            self._current_exif_datetime is not None
            or self._current_exif_lat is not None
            or self._current_exif_lon is not None
        )
        enable = bool(has_exif_data)
        self.set_from_image_btn.setEnabled(enable)
        if not enable:
            self.set_from_image_btn.setStyleSheet(
                "background-color: #bdc3c7; color: #7f8c8d;"
            )
            return
        is_source = self._observation_source_index == idx
        if is_source:
            self.set_from_image_btn.setStyleSheet(
                "background-color: #27ae60; color: white; font-weight: bold;"
            )
        else:
            self.set_from_image_btn.setStyleSheet("")

    def _current_single_index(self) -> int | None:
        indices = self._current_selection_indices()
        if len(indices) == 1:
            return indices[0]
        return None

    def _update_rotate_button_state(self) -> None:
        if not hasattr(self, "rotate_preview_btn"):
            return
        idx = self._current_single_index()
        enabled = (
            idx is not None
            and 0 <= idx < len(self.import_results)
            and bool(self.import_results[idx].filepath)
        )
        self.rotate_preview_btn.setEnabled(enabled)
        self.rotate_preview_btn.setVisible(self.preview_stack.currentWidget() is self.preview)

    def _position_rotate_button(self) -> None:
        if not hasattr(self, "rotate_preview_btn"):
            return
        self.rotate_preview_btn.move(8, 8)

    def _matches_observation_datetime(self, dt: QDateTime | None) -> bool:
        if not dt or not self._observation_datetime:
            return False
        if not dt.isValid() or not self._observation_datetime.isValid():
            return False
        obs_minutes = int(self._observation_datetime.toSecsSinceEpoch() / 60)
        img_minutes = int(dt.toSecsSinceEpoch() / 60)
        return obs_minutes == img_minutes

    def _update_observation_source_index(self) -> None:
        self._observation_source_index = None
        source_idx = None
        for idx, result in enumerate(self.import_results):
            if getattr(result, "gps_source", False):
                source_idx = idx
                break
        if source_idx is not None:
            for i, result in enumerate(self.import_results):
                result.gps_source = i == source_idx
            self._observation_source_index = source_idx
            return
        if not self._observation_datetime:
            return
        for idx, result in enumerate(self.import_results):
            if result.exif_has_gps and self._matches_observation_datetime(result.captured_at):
                self._observation_source_index = idx
                return

    def _update_ai_controls_state(self) -> None:
        if hasattr(self, "ai_crop_btn"):
            index = self._current_single_index()
            enable = False
            if index is not None and 0 <= index < len(self.import_results):
                image_type = (self.import_results[index].image_type or "field").strip().lower()
                enable = image_type == "field"
            if self._ai_thread is not None:
                enable = False
            self.ai_crop_btn.setEnabled(enable)
            if not enable and self._ai_crop_active:
                self._set_ai_crop_active(False)
            self._set_ai_crop_button_style(enable)
        if hasattr(self, "ai_guess_btn"):
            indices = self._current_selection_indices()
            enable_guess = False
            if indices:
                indices = [idx for idx in indices if 0 <= idx < len(self.import_results)]
                enable_guess = bool(indices) and all(
                    (self.import_results[idx].image_type or "field").strip().lower() == "field"
                    for idx in indices
                )
            if self._ai_thread is not None:
                enable_guess = False
            self.ai_guess_btn.setEnabled(enable_guess)
        self._update_rotate_button_state()
        self._update_ai_crop_size_label()

    def _update_ai_table(self) -> None:
        if not hasattr(self, "ai_table"):
            return
        index = self._current_single_index()
        self.ai_table.setRowCount(0)
        if index is None:
            return
        predictions = self._ai_predictions_by_index.get(index, [])
        for row, pred in enumerate(predictions):
            taxon = pred.get("taxon", {})
            display_name = self._format_ai_taxon_name(taxon)
            confidence = pred.get("probability", 0.0)
            name_item = QTableWidgetItem(display_name)
            name_item.setData(Qt.UserRole, pred)
            conf_item = QTableWidgetItem(f"{confidence:.1%}")
            link_widget = self._build_adb_link_widget(self._ai_prediction_link(pred, taxon))
            self.ai_table.insertRow(row)
            self.ai_table.setItem(row, 0, name_item)
            self.ai_table.setItem(row, 1, conf_item)
            if link_widget:
                self.ai_table.setCellWidget(row, 2, link_widget)
        if predictions:
            selected = self._ai_selected_by_index.get(index)
            if selected:
                for row in range(self.ai_table.rowCount()):
                    item = self.ai_table.item(row, 0)
                    if item and item.data(Qt.UserRole) == selected:
                        self.ai_table.selectRow(row)
                        break
            else:
                self.ai_table.selectRow(0)
        else:
            self._ai_selected_taxon = None

    def _update_ai_overlay(self) -> None:
        if not hasattr(self, "preview"):
            return
        index = self._current_single_index()
        preview_pixmap = getattr(self.preview, "original_pixmap", None)
        if index is not None and preview_pixmap:
            width = preview_pixmap.width()
            height = preview_pixmap.height()
            crop_box = self._ai_crop_boxes.get(index)
            if crop_box and width > 0 and height > 0:
                self.preview.set_crop_box(
                    (crop_box[0] * width, crop_box[1] * height, crop_box[2] * width, crop_box[3] * height)
                )
            else:
                self.preview.set_crop_box(None)
        else:
            self.preview.set_crop_box(None)
        self.preview.set_overlay_boxes([])
        self._update_ai_crop_size_label()

    def _format_ai_taxon_name(self, taxon: dict) -> str:
        scientific = taxon.get("scientificName") or taxon.get("scientific_name") or taxon.get("name") or ""
        vernacular = ""
        vernacular_names = taxon.get("vernacularNames") or {}
        lang = normalize_vernacular_language(SettingsDB.get_setting("vernacular_language", "no"))
        if isinstance(vernacular_names, dict) and lang:
            vernacular = vernacular_names.get(lang, "")
        if not vernacular:
            vernacular = taxon.get("vernacularName") or ""
        return vernacular or scientific or self.tr("Unknown")

    def _ai_prediction_link(self, pred: dict, taxon: dict) -> str | None:
        if isinstance(pred, dict):
            for key in ("infoURL", "infoUrl", "info_url"):
                value = pred.get(key)
                if isinstance(value, str) and value.startswith("http"):
                    return value
        if not isinstance(taxon, dict):
            return None
        for key in ("infoURL", "infoUrl", "info_url"):
            value = taxon.get(key)
            if isinstance(value, str) and value.startswith("http"):
                return value
        for key in ("url", "link", "href", "uri"):
            value = taxon.get(key)
            if isinstance(value, str) and value.startswith("http"):
                return value
        taxon_id = (
            taxon.get("taxonId")
            or taxon.get("taxon_id")
            or taxon.get("TaxonId")
            or taxon.get("id")
        )
        if taxon_id:
            return f"https://artsdatabanken.no/arter/takson/{taxon_id}"
        return "https://artsdatabanken.no"

    def _build_adb_link_widget(self, url: str | None) -> QLabel | None:
        if not url:
            return None
        label = QLabel(f'<a href="{url}">AdB</a>')
        label.setTextFormat(Qt.RichText)
        label.setTextInteractionFlags(Qt.TextBrowserInteraction)
        label.setOpenExternalLinks(True)
        label.setAlignment(Qt.AlignCenter)
        label.setStyleSheet("QLabel { padding: 2px 6px; }")
        return label

    def _extract_genus_species(self, taxon: dict) -> tuple[str | None, str | None]:
        scientific = taxon.get("scientificName") or taxon.get("scientific_name") or taxon.get("name") or ""
        parts = [p for p in scientific.replace("/", " ").split() if p]
        if len(parts) >= 2:
            return parts[0], parts[1]
        return None, None

    def get_ai_selected_taxon(self) -> dict | None:
        if not self._ai_selected_taxon:
            return None
        genus, species = self._extract_genus_species(self._ai_selected_taxon)
        if not genus or not species:
            return None
        return {
            "genus": genus,
            "species": species,
            "taxon": self._ai_selected_taxon,
        }

    def _on_ai_selection_changed(self) -> None:
        index = self._current_single_index()
        if index is None:
            return
        selected_items = self.ai_table.selectedItems()
        if not selected_items:
            self._ai_selected_taxon = None
            self._set_ai_status(None)
            return
        row_item = self.ai_table.item(self.ai_table.currentRow(), 0)
        if not row_item:
            return
        pred = row_item.data(Qt.UserRole) or {}
        self._ai_selected_by_index[index] = pred
        self._ai_selected_taxon = pred.get("taxon") or {}
        self._set_ai_status(self.tr("Applied selected species."), "#27ae60")

    def _set_ai_crop_active(self, active: bool) -> None:
        self._ai_crop_active = bool(active)
        if hasattr(self, "preview"):
            self.preview.set_crop_mode(self._ai_crop_active)
            self.preview.set_crop_aspect_ratio(None)
        self._set_ai_crop_button_style()

    def _set_ai_crop_button_style(self, enabled: bool | None = None) -> None:
        if not hasattr(self, "ai_crop_btn"):
            return
        if enabled is None:
            enabled = self.ai_crop_btn.isEnabled()
        if not enabled:
            self.ai_crop_btn.setStyleSheet(
                "background-color: #bdc3c7; color: #7f8c8d; font-weight: bold;"
            )
            return
        if self._ai_crop_active:
            self.ai_crop_btn.setStyleSheet(
                "background-color: #e74c3c; color: white; font-weight: bold;"
            )
        else:
            self.ai_crop_btn.setStyleSheet(
                "background-color: #3498db; color: white; font-weight: bold;"
            )

    def _on_ai_crop_clicked(self) -> None:
        index = self._current_single_index()
        if index is None:
            return
        if index in self._ai_crop_boxes:
            self._ai_crop_boxes.pop(index, None)
            if hasattr(self, "preview"):
                self.preview.set_crop_box(None)
            if 0 <= index < len(self.import_results):
                self.import_results[index].ai_crop_box = None
                self.import_results[index].ai_crop_source_size = None
                if self.import_results[index].image_id:
                    ImageDB.update_image(
                        self.import_results[index].image_id,
                        ai_crop_box=None,
                        ai_crop_source_size=None,
                    )
            self._set_ai_crop_active(True)
            self._update_ai_controls_state()
            return
        if self._ai_crop_active:
            self._set_ai_crop_active(False)
            return
        self._set_ai_crop_active(True)

    def _on_ai_crop_changed(self, box: tuple[float, float, float, float] | None) -> None:
        index = self._current_single_index()
        if index is None:
            return
        if box and getattr(self.preview, "original_pixmap", None):
            width = self.preview.original_pixmap.width()
            height = self.preview.original_pixmap.height()
            if width > 0 and height > 0:
                x1, y1, x2, y2 = box
                norm_box = (
                    max(0.0, min(1.0, x1 / width)),
                    max(0.0, min(1.0, y1 / height)),
                    max(0.0, min(1.0, x2 / width)),
                    max(0.0, min(1.0, y2 / height)),
                )
                self._ai_crop_boxes[index] = norm_box
                if 0 <= index < len(self.import_results):
                    self.import_results[index].ai_crop_box = norm_box
                    self.import_results[index].ai_crop_source_size = (width, height)
                    if self.import_results[index].image_id:
                        ImageDB.update_image(
                            self.import_results[index].image_id,
                            ai_crop_box=norm_box,
                            ai_crop_source_size=(width, height),
                        )
            else:
                self._ai_crop_boxes.pop(index, None)
                if 0 <= index < len(self.import_results):
                    self.import_results[index].ai_crop_box = None
                    self.import_results[index].ai_crop_source_size = None
                    if self.import_results[index].image_id:
                        ImageDB.update_image(
                            self.import_results[index].image_id,
                            ai_crop_box=None,
                            ai_crop_source_size=None,
                        )
        else:
            self._ai_crop_boxes.pop(index, None)
            if 0 <= index < len(self.import_results):
                self.import_results[index].ai_crop_box = None
                self.import_results[index].ai_crop_source_size = None
                if self.import_results[index].image_id:
                    ImageDB.update_image(
                        self.import_results[index].image_id,
                        ai_crop_box=None,
                        ai_crop_source_size=None,
                    )
        if self._ai_crop_active:
            self._set_ai_crop_active(False)
        self._update_ai_controls_state()
        self._update_ai_crop_size_label()

    def _on_ai_crop_preview_changed(self, box: tuple[float, float, float, float] | None) -> None:
        if not hasattr(self, "ai_crop_size_label"):
            return
        if (
            box
            and isinstance(box, (tuple, list))
            and len(box) == 4
        ):
            try:
                x1, y1, x2, y2 = (float(v) for v in box)
                crop_w = max(1, int(round(abs(x2 - x1))))
                crop_h = max(1, int(round(abs(y2 - y1))))
                text = self.tr("Crop: {w}x{h}").format(w=crop_w, h=crop_h)
                self.ai_crop_size_label.setText(text)
                self.ai_crop_size_label.setVisible(True)
                return
            except Exception:
                pass
        self._update_ai_crop_size_label()

    def _update_ai_crop_size_label(self) -> None:
        if not hasattr(self, "ai_crop_size_label"):
            return
        index = self._current_single_index()
        text = ""
        if index is not None:
            crop_box = self._ai_crop_boxes.get(index)
            source_size = None
            if 0 <= index < len(self.import_results):
                result = self.import_results[index]
                if crop_box is None:
                    crop_box = getattr(result, "ai_crop_box", None)
                source_size = getattr(result, "ai_crop_source_size", None)
            if (
                (not source_size or len(source_size) != 2)
                and hasattr(self, "preview")
                and getattr(self.preview, "original_pixmap", None)
            ):
                pixmap = self.preview.original_pixmap
                source_size = (pixmap.width(), pixmap.height())
            if (
                crop_box
                and isinstance(crop_box, (tuple, list))
                and len(crop_box) == 4
                and source_size
                and len(source_size) == 2
            ):
                try:
                    src_w = max(1, int(source_size[0]))
                    src_h = max(1, int(source_size[1]))
                    x1 = max(0.0, min(1.0, float(min(crop_box[0], crop_box[2]))))
                    y1 = max(0.0, min(1.0, float(min(crop_box[1], crop_box[3]))))
                    x2 = max(0.0, min(1.0, float(max(crop_box[0], crop_box[2]))))
                    y2 = max(0.0, min(1.0, float(max(crop_box[1], crop_box[3]))))
                    crop_w = max(1, int(round((x2 - x1) * src_w)))
                    crop_h = max(1, int(round((y2 - y1) * src_h)))
                    text = self.tr("Crop: {w}x{h}").format(w=crop_w, h=crop_h)
                except Exception:
                    text = ""
        self.ai_crop_size_label.setText(text)
        self.ai_crop_size_label.setVisible(bool(text))

    def _on_ai_guess_clicked(self) -> None:
        if not hasattr(self, "ai_guess_btn"):
            return
        indices = self._current_selection_indices()
        if not indices:
            return
        indices = [idx for idx in indices if 0 <= idx < len(self.import_results)]
        if not indices:
            return
        if any(self.import_results[idx].image_type != "field" for idx in indices):
            self._set_ai_status(self.tr("AI guess only works for field photos"), "#e74c3c")
            return
        requests = []
        for idx in indices:
            result = self.import_results[idx]
            image_path = result.filepath
            if not image_path:
                continue
            crop_box = self._ai_crop_boxes.get(idx) or getattr(result, "ai_crop_box", None)
            requests.append(
                {
                    "index": idx,
                    "image_path": image_path,
                    "crop_box": crop_box,
                }
            )
        if not requests:
            return
        if self._ai_thread is not None:
            return
        self.ai_guess_btn.setEnabled(False)
        self.ai_guess_btn.setText(self.tr("AI guessing..."))
        count = len(requests)
        self._set_ai_status(
            self.tr("Sending {count} image(s) to Artsdatabanken AI...").format(count=count),
            "#3498db",
        )
        temp_dir = get_images_dir() / "imports"
        self._ai_thread = AIGuessWorker(requests, temp_dir, max_dim=1600, parent=self)
        self._ai_thread.resultReady.connect(self._on_ai_guess_finished)
        self._ai_thread.error.connect(self._on_ai_guess_error)
        self._ai_thread.finished.connect(self._ai_thread.deleteLater)
        self._ai_thread.finished.connect(self._on_ai_thread_finished)
        self._ai_thread.start()

    def _on_ai_thread_finished(self) -> None:
        self._ai_thread = None
        if hasattr(self, "ai_guess_btn"):
            self.ai_guess_btn.setText(self.tr("AI guess"))
        self._update_ai_controls_state()

    def _on_ai_guess_finished(
        self,
        indices: list,
        predictions: list,
        _box: object,
        _warnings: object,
        temp_paths: list,
    ) -> None:
        for temp_path in temp_paths or []:
            if not temp_path:
                continue
            try:
                Path(temp_path).unlink(missing_ok=True)
            except Exception:
                self._temp_preview_paths.add(temp_path)
        for index in indices or []:
            self._ai_predictions_by_index[index] = predictions or []
        self._update_ai_table()
        self._update_ai_overlay()
        if predictions:
            self._set_ai_status(self.tr("AI suggestion updated"), "#27ae60")
        else:
            self._set_ai_status(self.tr("No AI suggestions found"), "#7f8c8d")
        self._update_ai_controls_state()

    def _on_ai_guess_error(self, _indices: list, message: str) -> None:
        if "500" in message:
            hint = self.tr("AI guess failed: server error (500). Try again later.")
        else:
            hint = self.tr("AI guess failed: {message}").format(message=message)
        self._set_ai_status(hint, "#e74c3c")
        self._update_ai_controls_state()

    def _seed_observation_metadata(self) -> None:
        if self._observation_datetime is None:
            for result in self.import_results:
                if result.captured_at:
                    self._observation_datetime = result.captured_at
                    break
        if self._observation_lat is None or self._observation_lon is None:
            for result in self.import_results:
                if result.gps_latitude is not None or result.gps_longitude is not None:
                    self._observation_lat = result.gps_latitude
                    self._observation_lon = result.gps_longitude
                    break

    def _sync_observation_metadata_inputs(self) -> None:
        self._loading_form = True
        if self._observation_datetime:
            self.datetime_input.setDateTime(self._observation_datetime)
        else:
            self.datetime_input.setDateTime(self._unset_datetime)
        if self._observation_lat is not None:
            self.lat_input.setValue(self._observation_lat)
        else:
            self.lat_input.setValue(self.lat_input.minimum())
        if self._observation_lon is not None:
            self.lon_input.setValue(self._observation_lon)
        else:
            self.lon_input.setValue(self.lon_input.minimum())
        self._loading_form = False

    def _update_observation_metadata_from_inputs(self) -> None:
        dt_value = self.datetime_input.dateTime()
        self._observation_datetime = None if dt_value == self._unset_datetime else dt_value
        lat = self.lat_input.value()
        self._observation_lat = None if lat == self.lat_input.minimum() else lat
        lon = self.lon_input.value()
        self._observation_lon = None if lon == self.lon_input.minimum() else lon
        self._update_observation_source_index()
        self._update_set_from_image_button_state()

    def _register_hint_widget(
        self,
        widget: QWidget,
        hint_text: str | None,
        tone: str = "info",
    ) -> None:
        if not widget:
            return
        hint = (hint_text or "").strip()
        hint_tone = (tone or "info").strip().lower()
        if self._hint_controller is not None:
            self._hint_controller.register_widget(widget, hint, tone=hint_tone)
            return
        widget.setProperty("_hint_text", hint)
        widget.setProperty("_hint_tone", hint_tone)
        widget.setToolTip("")
        if not any(existing is widget for existing, _, _ in self._pending_hint_widgets):
            self._pending_hint_widgets.append((widget, hint, hint_tone))

    def _set_hintable_label_text(
        self,
        label: QLabel,
        text: str,
        hint_text: str | None = None,
        hint_tone: str = "info",
    ) -> None:
        label.setText(text)
        hint = (hint_text or "").strip()
        if isinstance(label, HintLabel):
            label.set_hint_text(hint)
            label.setToolTip("")
        elif hint:
            self._register_hint_widget(label, hint, tone=hint_tone)
        else:
            label.setProperty("_hint_text", "")
            label.setProperty("_hint_tone", "info")
            label.setToolTip("")

    def _set_resize_resolution_hints(
        self,
        current_hint: str | None = None,
        target_hint: str | None = None,
    ) -> None:
        current_text = (current_hint or "").strip()
        target_text = (target_hint or "").strip()
        if hasattr(self, "current_resolution_title") and isinstance(self.current_resolution_title, HintLabel):
            self.current_resolution_title.set_hint_text(current_text)
        if hasattr(self, "target_resolution_title") and isinstance(self.target_resolution_title, HintLabel):
            self.target_resolution_title.set_hint_text(target_text)

    def set_hint(self, text: str | None) -> None:
        if self._hint_controller is not None:
            self._hint_controller.set_hint(text)

    def set_status(
        self,
        text: str | None,
        timeout_ms: int = 4000,
        tone: str = "info",
    ) -> None:
        if self._hint_controller is not None:
            self._hint_controller.set_status(text, timeout_ms=timeout_ms, tone=tone)

    @staticmethod
    def _status_tone_from_color(color: str | None, default: str = "info") -> str:
        value = (color or "").strip().lower()
        if not value:
            return default
        if value in {"#27ae60", "#2ecc71", "green"}:
            return "success"
        if value in {"#e74c3c", "#e67e22", "#f39c12", "red", "orange"}:
            return "warning"
        return default

    def _set_settings_hint(self, text: str | None, color: str) -> None:
        self.set_status(text, tone=self._status_tone_from_color(color, default="info"))

    def _set_ai_status(self, text: str | None, color: str = "#7f8c8d") -> None:
        self.set_status(
            text,
            timeout_ms=6000,
            tone=self._status_tone_from_color(color, default="info"),
        )

    @staticmethod
    def _rotate_normalized_crop_box_clockwise(
        box: tuple[float, float, float, float],
    ) -> tuple[float, float, float, float]:
        x1, y1, x2, y2 = box
        corners = [
            (x1, y1),
            (x2, y1),
            (x2, y2),
            (x1, y2),
        ]
        rotated = [(1.0 - y, x) for x, y in corners]
        xs = [p[0] for p in rotated]
        ys = [p[1] for p in rotated]
        return (
            max(0.0, min(1.0, min(xs))),
            max(0.0, min(1.0, min(ys))),
            max(0.0, min(1.0, max(xs))),
            max(0.0, min(1.0, max(ys))),
        )

    def _invalidate_cached_pixmap(self, path: str | None) -> None:
        if not path:
            return
        self._pixmap_cache.pop(path, None)
        self._pixmap_cache_is_preview.pop(path, None)

    def _prepare_rotate_source_path(self, index: int) -> str | None:
        if index < 0 or index >= len(self.import_results):
            return None
        result = self.import_results[index]
        source = (result.filepath or "").strip()
        if not source:
            return None
        source_path = Path(source)
        if not source_path.exists():
            return None
        if result.image_id:
            return str(source_path)
        imports_dir = get_images_dir() / "imports"
        imports_dir.mkdir(parents=True, exist_ok=True)
        try:
            in_imports = source_path.resolve().is_relative_to(imports_dir.resolve())
        except Exception:
            in_imports = False
        if in_imports:
            return str(source_path)
        dest = imports_dir / source_path.name
        counter = 1
        while dest.exists():
            dest = imports_dir / f"{source_path.stem}_prep_{counter}{source_path.suffix}"
            counter += 1
        try:
            shutil.copy2(source_path, dest)
        except Exception:
            return None
        result.filepath = str(dest)
        result.preview_path = str(dest)
        if not result.original_filepath:
            result.original_filepath = str(source_path)
        self.image_paths[index] = str(dest)
        self._converted_import_paths.add(str(dest))
        return str(dest)

    def _rotate_image_file_clockwise(self, path: str) -> bool:
        from PIL import Image, ImageOps

        source = Path(path)
        if not source.exists():
            return False
        tmp = source.with_name(f"{source.stem}_rotating{source.suffix}")
        try:
            with Image.open(source) as img:
                normalized = ImageOps.exif_transpose(img)
                rotated = normalized.rotate(-90, expand=True)
                exif = normalized.getexif()
                if exif is not None:
                    exif[274] = 1  # Orientation = Normal
                fmt = img.format
                save_kwargs = {}
                if fmt and fmt.upper() in {"JPEG", "JPG"} and rotated.mode not in {"RGB", "L"}:
                    rotated = rotated.convert("RGB")
                if fmt and fmt.upper() in {"JPEG", "JPG"}:
                    save_kwargs["quality"] = 95
                if exif is not None and len(exif) > 0:
                    save_kwargs["exif"] = exif.tobytes()
                try:
                    rotated.save(tmp, format=fmt, **save_kwargs)
                except Exception:
                    save_kwargs.pop("exif", None)
                    rotated.save(tmp, format=fmt, **save_kwargs)
            tmp.replace(source)
            return True
        except Exception:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
            return False

    def _on_rotate_current_image_clicked(self) -> None:
        index = self._current_single_index()
        if index is None or index < 0 or index >= len(self.import_results):
            return
        result = self.import_results[index]
        old_filepath = result.filepath
        old_preview = result.preview_path
        rotate_path = self._prepare_rotate_source_path(index)
        if not rotate_path:
            self._set_settings_hint(self.tr("Failed to rotate image"), "#e74c3c")
            return
        if not self._rotate_image_file_clockwise(rotate_path):
            self._set_settings_hint(self.tr("Failed to rotate image"), "#e74c3c")
            return
        result.filepath = rotate_path
        result.preview_path = rotate_path
        if result.original_filepath == old_filepath:
            result.original_filepath = rotate_path
        self.image_paths[index] = rotate_path
        self._invalidate_cached_pixmap(old_filepath)
        if old_preview and old_preview != old_filepath:
            self._invalidate_cached_pixmap(old_preview)
        self._invalidate_cached_pixmap(rotate_path)
        if result.ai_crop_box:
            rotated_box = self._rotate_normalized_crop_box_clockwise(result.ai_crop_box)
            result.ai_crop_box = rotated_box
            self._ai_crop_boxes[index] = rotated_box
            new_size = self._get_image_size(rotate_path)
            if new_size:
                result.ai_crop_source_size = (int(new_size[0]), int(new_size[1]))
            if result.image_id:
                try:
                    ImageDB.update_image(
                        result.image_id,
                        ai_crop_box=result.ai_crop_box,
                        ai_crop_source_size=result.ai_crop_source_size,
                    )
                except Exception:
                    pass
        self._refresh_gallery()
        self._select_image(index)
        self._set_settings_hint(self.tr("Image rotated 90 deg"), "#27ae60")

    def _update_settings_hint_for_indices(self, indices: list[int], action: str | None = None) -> None:
        if not indices:
            return
        action_map = {
            "scale": (self.tr("Scale applied"), "to"),
            "contrast": (self.tr("Contrast changed"), "for"),
            "mount": (self.tr("Mount changed"), "for"),
            "sample": (self.tr("Sample type changed"), "for"),
            "image_type": (self.tr("Image type changed"), "for"),
        }
        base, prep = action_map.get(action, (self.tr("Settings applied"), "to"))
        total = len(self.import_results)
        if total > 0 and len(indices) == total and total > 1:
            message = self.tr("{base} {prep} all images").format(base=base, prep=prep)
        elif len(indices) > 1:
            message = self.tr("{base} {prep} selected images").format(base=base, prep=prep)
        else:
            index = indices[0]
            message = self.tr("{base} {prep} image {num}").format(base=base, prep=prep, num=index + 1)
        self._set_settings_hint(message, "#27ae60")

    def _on_add_images_clicked(self) -> None:
        paths, _ = QFileDialog.getOpenFileNames(
            self,
            self.tr("Select Images"),
            "",
            self.tr("Images (*.png *.jpg *.jpeg *.tif *.tiff *.heic *.heif);;All Files (*)"),
        )
        if paths:
            self.add_images(paths)

    def add_images(self, paths: list[str]) -> None:
        import_dir = get_images_dir() / "imports"
        import_dir.mkdir(parents=True, exist_ok=True)
        first_new_index = len(self.import_results)
        if getattr(self, "import_progress", None) and len(paths) > 1:
            self.import_progress.setRange(0, len(paths))
            self.import_progress.setValue(0)
            self.import_progress.setVisible(True)
            QCoreApplication.processEvents()
        for path in paths:
            if not path:
                continue
            converted_path = maybe_convert_heic(path, import_dir)
            if converted_path and converted_path != path:
                self._converted_import_paths.add(converted_path)
                path = converted_path
            self.image_paths.append(path)
            meta = get_image_metadata(path)
            if meta.get("missing"):
                self._register_missing_exif_path(path)
            captured_at = None
            dt = meta.get("datetime")
            if dt:
                captured_at = QDateTime(dt)
            lat = meta.get("latitude")
            lon = meta.get("longitude")
            if (lat is None or lon is None) and path:
                lat2, lon2 = get_gps_coordinates(path)
                if lat is None:
                    lat = lat2
                if lon is None:
                    lon = lon2
            preview_path = path
            has_exif_gps = lat is not None or lon is not None
            result = ImageImportResult(
                filepath=path,
                preview_path=preview_path or path,
                captured_at=captured_at,
                gps_latitude=lat,
                gps_longitude=lon,
                gps_source=False,
                exif_has_gps=has_exif_gps,
                resize_to_optimal=self.resize_to_optimal_default,
                store_original=self.store_original_default,
                original_filepath=path,
            )
            self.import_results.append(result)
            if getattr(self, "import_progress", None) and self.import_progress.isVisible():
                self.import_progress.setValue(self.import_progress.value() + 1)
                QCoreApplication.processEvents()
        if getattr(self, "import_progress", None) and self.import_progress.isVisible():
            self.import_progress.setVisible(False)
        self._update_summary()
        self._seed_observation_metadata()
        self._update_observation_source_index()
        self._sync_observation_metadata_inputs()
        self._refresh_gallery()
        self._update_scale_group_state()
        self._update_set_from_image_button_state()
        self._update_ai_controls_state()
        if self.image_paths:
            last_new_index = len(self.import_results) - 1
            if first_new_index <= last_new_index:
                self._select_image(last_new_index)
            elif self.selected_index is None:
                self._select_image(0)

    def select_image_by_path(self, path: str) -> None:
        """Select and preview a specific image by its file path."""
        if not path:
            return
        try:
            index = self.image_paths.index(path)
        except ValueError:
            return
        self._select_image(index)

    def set_import_results(self, results: list[ImageImportResult]) -> None:
        self.import_results = []
        self.image_paths = []
        self._ai_crop_boxes = {}
        for result in results:
            if not result:
                continue
            if not result.preview_path:
                result.preview_path = result.filepath
            result.gps_source = bool(getattr(result, "gps_source", False))
            if result.original_filepath is None:
                result.original_filepath = result.filepath
            if not hasattr(result, "resize_to_optimal"):
                result.resize_to_optimal = self.resize_to_optimal_default
            if not hasattr(result, "store_original"):
                result.store_original = self.store_original_default
            if result.filepath and (
                not getattr(result, "exif_has_gps", False)
                or not result.captured_at
                or result.gps_latitude is None
                or result.gps_longitude is None
            ):
                meta = get_image_metadata(result.filepath)
                if meta.get("missing"):
                    self._register_missing_exif_path(result.filepath)
                lat = meta.get("latitude")
                lon = meta.get("longitude")
                if (lat is None or lon is None):
                    lat2, lon2 = get_gps_coordinates(result.filepath)
                    if lat is None:
                        lat = lat2
                    if lon is None:
                        lon = lon2
                if result.gps_latitude is None:
                    result.gps_latitude = lat
                if result.gps_longitude is None:
                    result.gps_longitude = lon
                result.exif_has_gps = (
                    bool(getattr(result, "exif_has_gps", False))
                    or lat is not None
                    or lon is not None
                )
                if not result.captured_at:
                    dt = meta.get("datetime")
                    if dt:
                        result.captured_at = QDateTime(dt)
            if result.ai_crop_box:
                self._ai_crop_boxes[len(self.import_results)] = result.ai_crop_box
            self.import_results.append(result)
            self.image_paths.append(result.filepath)
        self._update_summary()
        self._seed_observation_metadata()
        self._update_observation_source_index()
        self._sync_observation_metadata_inputs()
        self._refresh_gallery()
        self._update_scale_group_state()
        self._update_set_from_image_button_state()
        self._update_ai_controls_state()
        if self.image_paths:
            self._select_image(0)

    def _on_gallery_clicked(self, _, path: str) -> None:
        if not path:
            return
        if len(self.selected_indices) > 1:
            self._show_multi_selection_state()
            return
        try:
            index = self.image_paths.index(path)
        except ValueError:
            return
        self._select_image(index)

    def _on_gallery_selection_changed(self, paths: list[str]) -> None:
        indices = []
        for path in paths:
            try:
                indices.append(self.image_paths.index(path))
            except ValueError:
                continue
        self.selected_indices = sorted(set(indices))
        if len(self.selected_indices) > 1:
            self.selected_index = None
            self._show_multi_selection_state()
            self._update_scale_group_state()
            self._update_set_from_image_button_state()
            self._update_ai_controls_state()
            self._update_ai_table()
            self._update_ai_overlay()
        elif len(self.selected_indices) == 1:
            self._select_image(self.selected_indices[0], sync_gallery=False)
        else:
            self._update_scale_group_state()
            self._update_set_from_image_button_state()
            self._update_ai_controls_state()
            self._update_ai_table()
            self._update_ai_overlay()

    def _resolve_preview_pixmap(self, result: ImageImportResult) -> tuple[QPixmap | None, bool]:
        preview_path = result.preview_path or result.filepath
        pixmap = self._get_cached_pixmap(preview_path) if preview_path else None
        preview_scaled = self._pixmap_cache_is_preview.get(preview_path or "", False)
        if (pixmap is None or pixmap.isNull()) and result.filepath and preview_path != result.filepath:
            preview_path = result.filepath
            result.preview_path = preview_path
            pixmap = self._get_cached_pixmap(preview_path)
            preview_scaled = self._pixmap_cache_is_preview.get(preview_path or "", False)
        if (pixmap is None or pixmap.isNull()) and preview_path:
            converted_path = maybe_convert_heic(preview_path, get_images_dir() / "imports")
            if converted_path and converted_path != preview_path:
                self._converted_import_paths.add(converted_path)
                result.preview_path = converted_path
                preview_path = converted_path
                pixmap = self._get_cached_pixmap(preview_path)
                preview_scaled = self._pixmap_cache_is_preview.get(preview_path or "", False)
        return pixmap, preview_scaled

    def _apply_resize_preview(self, pixmap: QPixmap, result: ImageImportResult) -> tuple[QPixmap, bool]:
        if not self._resize_preview_enabled:
            return pixmap, False
        if getattr(self, "_calibration_mode", False):
            return pixmap, False
        factor = self._compute_resample_scale_factor(result, respect_toggle=False)
        if factor >= 0.999:
            return pixmap, False
        target_w = max(1, int(round(pixmap.width() * factor)))
        target_h = max(1, int(round(pixmap.height() * factor)))
        resized = pixmap.scaled(
            target_w,
            target_h,
            Qt.KeepAspectRatio,
            Qt.SmoothTransformation,
        )
        return resized, True

    def _update_resize_preview_tag(
        self,
        result: ImageImportResult | None,
        preview_resized: bool,
        preview_pixmap: QPixmap | None = None,
    ) -> None:
        if not hasattr(self, "preview"):
            return
        if result is None:
            self.preview.set_corner_tag("")
            return
        if preview_resized:
            mp_value = None
            size = self._get_image_size(result.original_filepath or result.filepath or result.preview_path)
            if size:
                width, height = size
                # Recompute from current objective/sampling settings so the tag
                # reflects the actual preview resize target, not a stale cached factor.
                factor = self._compute_resample_scale_factor(result, respect_toggle=False)
                target_w = max(1, int(round(width * factor)))
                target_h = max(1, int(round(height * factor)))
                mp_value = (target_w * target_h) / 1_000_000.0
            elif preview_pixmap is not None:
                mp_value = (preview_pixmap.width() * preview_pixmap.height()) / 1_000_000.0
            mp_text = self._format_significant(float(mp_value), 2) if mp_value is not None else "--"
            self.preview.set_corner_tag(
                self.tr("Resized to {mp} MP").format(mp=mp_text),
                QColor(39, 174, 96),
            )
        else:
            if (result.image_type or "field").strip().lower() == "field":
                self.preview.set_corner_tag("")
            else:
                self.preview.set_corner_tag(self.tr("Original image"), QColor(149, 165, 166))

    def _set_preview_for_result(self, result: ImageImportResult, preserve_view: bool = False) -> None:
        old_state = None
        if preserve_view and hasattr(self, "preview"):
            old_state = self.preview.get_view_state()
        pixmap, preview_scaled = self._resolve_preview_pixmap(result)
        preview_resized = False
        if pixmap and not pixmap.isNull():
            pixmap, preview_resized = self._apply_resize_preview(pixmap, result)
            preview_scaled_for_view = preview_scaled if not preview_resized else False
            self.preview.set_image_sources(pixmap, result.filepath, preview_scaled_for_view)
        else:
            self.preview.set_image(None)
        self.preview_stack.setCurrentWidget(self.preview)
        if (
            old_state
            and pixmap
            and not pixmap.isNull()
            and old_state.get("size")
        ):
            old_w, old_h = old_state["size"]
            new_w, new_h = pixmap.width(), pixmap.height()
            if old_w and old_h:
                ratio_x = float(new_w) / float(old_w)
                ratio_y = float(new_h) / float(old_h)
                center = old_state["center"]
                new_center = QPointF(center.x() * ratio_x, center.y() * ratio_y)
                zoom_ratio = ratio_x if ratio_x > 0 else 1.0
                new_zoom = float(old_state["zoom"]) / zoom_ratio
                self.preview.set_view_state(new_center, new_zoom)
        if pixmap and not pixmap.isNull():
            self._update_resize_preview_tag(result, preview_resized, pixmap)
        else:
            self._update_resize_preview_tag(None, False)

    def _refresh_resize_preview(self, force: bool = False, preserve_view: bool = False) -> None:
        if not self._resize_preview_enabled and not force:
            return
        index = self._current_single_index()
        if index is None:
            if hasattr(self, "preview"):
                self.preview.set_corner_tag("")
            return
        if index < 0 or index >= len(self.import_results):
            return
        self._set_preview_for_result(self.import_results[index], preserve_view=preserve_view)
        self._update_ai_overlay()

    def _toggle_resize_preview(self) -> None:
        if not hasattr(self, "resize_optimal_checkbox"):
            return
        if not self.resize_optimal_checkbox.isEnabled():
            return
        self.resize_optimal_checkbox.toggle()

    def _on_scale_shortcut(self) -> None:
        if not hasattr(self, "calibrate_btn"):
            return
        if not self.calibrate_btn.isEnabled():
            return
        self._open_calibration_dialog()

    def _apply_image_type_shortcut(self, image_type: str) -> None:
        if self.selected_index is None and not self.selected_indices:
            return
        if getattr(self, "_loading_form", False):
            return
        target = (image_type or "").strip().lower()
        if target == "micro":
            self.micro_radio.setChecked(True)
        else:
            self.field_radio.setChecked(True)
        indices = self.selected_indices or [self.selected_index]
        self._last_settings_action = "image_type"
        self._apply_settings_to_indices(indices, action="image_type")

    def eventFilter(self, obj, event):
        if obj is getattr(self, "preview", None) and event.type() == QEvent.Resize:
            self._position_rotate_button()
            return False
        if (
            obj is getattr(self, "target_sampling_input", None)
            and event.type() == QEvent.KeyPress
            and event.key() == Qt.Key_R
            and event.modifiers() in (Qt.NoModifier, Qt.ShiftModifier)
        ):
            self._toggle_resize_preview()
            return True
        return super().eventFilter(obj, event)

    def _select_image(self, index: int, sync_gallery: bool = True) -> None:
        if index < 0 or index >= len(self.image_paths):
            return
        if self._ai_crop_active:
            self._set_ai_crop_active(False)
        self._set_ai_status(None)
        self.selected_index = index
        self.primary_index = index
        result = self.import_results[index]
        if sync_gallery:
            self.gallery.select_paths([result.filepath])
        self._set_preview_for_result(result)
        self._load_result_into_form(result)
        self._update_current_image_exif(result)
        self._update_scale_group_state()
        self._update_set_from_image_button_state()
        self._update_ai_controls_state()
        self._update_ai_table()
        self._update_ai_overlay()

    def _load_result_into_form(self, result: ImageImportResult) -> None:
        self._loading_form = True
        if result.image_type == "microscope":
            self.micro_radio.setChecked(True)
        else:
            self.field_radio.setChecked(True)
        self._custom_scale = float(result.custom_scale) if result.custom_scale else None
        self._populate_objectives(
            selected_key=self.CUSTOM_OBJECTIVE_KEY if result.custom_scale else result.objective
        )
        if result.custom_scale:
            idx = self.objective_combo.findData(self.CUSTOM_OBJECTIVE_KEY)
            if idx >= 0:
                self.objective_combo.setCurrentIndex(idx)
        elif result.objective:
            idx = self.objective_combo.findText(result.objective)
            if idx >= 0:
                self.objective_combo.setCurrentIndex(idx)
        else:
            self.objective_combo.setCurrentIndex(0)
        if result.contrast:
            self._set_combo_tag_value(self.contrast_combo, "contrast", result.contrast)
        if result.mount_medium:
            self._set_combo_tag_value(self.mount_combo, "mount", result.mount_medium)
        if result.sample_type:
            self._set_combo_tag_value(self.sample_combo, "sample", result.sample_type)
        self._loading_form = False
        self._last_objective_key = self.objective_combo.currentData()
        self._sync_observation_metadata_inputs()

    def _on_settings_changed(self) -> None:
        if self.selected_index is None and not self.selected_indices:
            return
        if getattr(self, "_loading_form", False):
            return
        sender = self.sender()
        action = None
        if sender is self.objective_combo:
            action = "scale"
            previous_key = self._last_objective_key
            current_key = self.objective_combo.currentData()
            if current_key == self.CUSTOM_OBJECTIVE_KEY and self._custom_scale is None:
                self._update_resize_group_state()
                self._open_calibration_dialog(previous_key=previous_key)
                return
        elif sender is self.contrast_combo:
            action = "contrast"
        elif sender is self.mount_combo:
            action = "mount"
        elif sender is self.sample_combo:
            action = "sample"
        elif sender in (self.field_radio, self.micro_radio):
            action = "image_type"
        self._last_settings_action = action
        indices = self.selected_indices or [self.selected_index]
        self._apply_settings_to_indices(indices, action, previous_key if action == "scale" else None)

    def _on_metadata_changed(self, *_args) -> None:
        if self.selected_index is None and not self.selected_indices:
            return
        if getattr(self, "_loading_form", False):
            return
        if getattr(self, "_setting_from_image_source", False):
            return
        self._setting_from_image_source = False
        indices = self.selected_indices or [self.selected_index]
        self._apply_metadata_to_indices(indices)

    def _apply_settings_to_index(
        self,
        index: int,
        action: str | None = None,
        rescale_targets: dict[int, tuple[float, float, list[dict]]] | None = None,
    ) -> bool:
        if index < 0 or index >= len(self.import_results):
            return True
        result = self.import_results[index]
        result.image_type = "microscope" if self.micro_radio.isChecked() else "field"
        selected_objective = self.objective_combo.currentData()
        if action == "scale" and result.image_id and result.image_type == "microscope":
            old_scale = None
            img = ImageDB.get_image(result.image_id)
            if img:
                old_scale = img.get("scale_microns_per_pixel")
            new_scale = self._resolve_selected_scale_value(selected_objective)
            factor = getattr(result, "resample_scale_factor", None)
            if (
                new_scale is not None
                and isinstance(factor, (int, float))
                and factor > 0
                and factor < 0.999
            ):
                new_scale = float(new_scale) / float(factor)
            if (
                old_scale is not None
                and new_scale is not None
                and abs(float(new_scale) - float(old_scale)) > 1e-6
            ):
                if rescale_targets is not None:
                    payload = rescale_targets.pop(result.image_id, None)
                    if payload:
                        rescale_old, rescale_new, measurements = payload
                        self._rescale_measurements_for_image(
                            result.image_id, rescale_old, rescale_new, measurements
                        )
                else:
                    if not self._maybe_rescale_measurements_for_image(
                        result.image_id, float(old_scale), float(new_scale)
                    ):
                        return False
        if selected_objective == self.CUSTOM_OBJECTIVE_KEY and self._custom_scale:
            result.custom_scale = self._custom_scale
            result.objective = None
        else:
            result.custom_scale = None
            result.objective = selected_objective or None
        result.contrast = self._get_combo_tag_value(self.contrast_combo, "contrast")
        result.mount_medium = self._get_combo_tag_value(self.mount_combo, "mount")
        result.sample_type = self._get_combo_tag_value(self.sample_combo, "sample")
        result.needs_scale = (
            result.image_type == "microscope"
            and not result.objective
            and not result.custom_scale
        )
        if hasattr(self, "resize_optimal_checkbox"):
            result.resize_to_optimal = bool(self.resize_optimal_checkbox.isChecked())
        result.store_original = self._store_originals_enabled()
        if not result.image_id:
            result.resample_scale_factor = self._compute_resample_scale_factor(result)
        self._refresh_gallery()
        self._update_summary()
        if index == self.selected_index and len(self.selected_indices) <= 1:
            self._update_current_image_sampling(result)
            self._refresh_resize_preview(preserve_view=True)
        return True

    def _apply_settings_to_indices(
        self,
        indices: list[int | None],
        action: str | None = None,
        previous_key: str | None = None,
    ) -> None:
        applied = []
        rescale_targets: dict[int, tuple[float, float, list[dict]]] | None = None
        if action == "scale":
            rescale_targets = {}
            targets = self._collect_rescale_targets(indices)
            if targets:
                decision = self._confirm_rescale_for_targets(len(targets))
                if decision == "cancel":
                    if action == "scale":
                        self.objective_combo.blockSignals(True)
                        if previous_key is None:
                            self.objective_combo.setCurrentIndex(0)
                        else:
                            combo_idx = self.objective_combo.findData(previous_key)
                            if combo_idx >= 0:
                                self.objective_combo.setCurrentIndex(combo_idx)
                        self.objective_combo.blockSignals(False)
                    return
                if decision == "all":
                    rescale_targets = {t[0]: t[1:] for t in targets}
                else:
                    image_id, old_scale, new_scale, measurements = targets[0]
                    rescale_targets = {image_id: (old_scale, new_scale, measurements)}

        for idx in indices:
            if idx is None:
                continue
            if not self._apply_settings_to_index(idx, action, rescale_targets):
                if action == "scale":
                    self.objective_combo.blockSignals(True)
                    if previous_key is None:
                        self.objective_combo.setCurrentIndex(0)
                    else:
                        combo_idx = self.objective_combo.findData(previous_key)
                        if combo_idx >= 0:
                            self.objective_combo.setCurrentIndex(combo_idx)
                    self.objective_combo.blockSignals(False)
                return
            applied.append(idx)
        if applied:
            self._update_settings_hint_for_indices(applied, action or self._last_settings_action)
            self._update_scale_group_state()
            self._update_ai_controls_state()
        if action == "scale" and hasattr(self, "objective_combo"):
            self._last_objective_key = self.objective_combo.currentData()
        self._update_scale_group_state()
        self._update_set_from_image_button_state()
        self._update_ai_controls_state()

    def _resolve_selected_scale_value(self, selected_objective) -> float | None:
        if selected_objective == self.CUSTOM_OBJECTIVE_KEY and self._custom_scale:
            return float(self._custom_scale)
        if selected_objective and selected_objective in self.objectives:
            return self.objectives[selected_objective].get("microns_per_pixel")
        return None

    def _collect_rescale_targets(
        self,
        indices: list[int | None],
    ) -> list[tuple[int, float, float, list[dict]]]:
        targets: list[tuple[int, float, float, list[dict]]] = []
        selected_objective = self.objective_combo.currentData()
        new_scale = self._resolve_selected_scale_value(selected_objective)
        if new_scale is None or not self.micro_radio.isChecked():
            return targets

        for idx in indices:
            if idx is None or idx < 0 or idx >= len(self.import_results):
                continue
            result = self.import_results[idx]
            if not result.image_id:
                continue
            img = ImageDB.get_image(result.image_id)
            if not img:
                continue
            old_scale = img.get("scale_microns_per_pixel")
            if old_scale is None:
                continue
            factor = getattr(result, "resample_scale_factor", None)
            effective_new_scale = new_scale
            if (
                effective_new_scale is not None
                and isinstance(factor, (int, float))
                and factor > 0
                and factor < 0.999
            ):
                effective_new_scale = float(effective_new_scale) / float(factor)
            if effective_new_scale is None:
                continue
            if abs(float(effective_new_scale) - float(old_scale)) <= 1e-6:
                continue
            measurements = MeasurementDB.get_measurements_for_image(result.image_id)
            if not measurements:
                continue
            has_points = any(
                all(m.get(f"p{i}_{axis}") is not None for i in range(1, 5) for axis in ("x", "y"))
                for m in measurements
            )
            if not has_points:
                continue
            targets.append(
                (result.image_id, float(old_scale), float(effective_new_scale), measurements)
            )
        return targets

    def _confirm_rescale_for_targets(self, target_count: int) -> str:
        box = QMessageBox(self)
        box.setIcon(QMessageBox.Question)
        box.setWindowTitle(self.tr("Changing image scale"))
        if target_count > 1:
            box.setText(
                self.tr(
                    "Changing image scale: This will update previous measurements to match the new scale.\n"
                    "Apply to all selected images?"
                )
            )
            apply_all_btn = box.addButton(self.tr("Apply to all"), QMessageBox.AcceptRole)
            ok_btn = box.addButton(self.tr("OK"), QMessageBox.AcceptRole)
            cancel_btn = box.addButton(self.tr("Cancel"), QMessageBox.RejectRole)
        else:
            box.setText(
                self.tr("Changing image scale: This will update previous measurements to match the new scale.")
            )
            apply_all_btn = None
            ok_btn = box.addButton(self.tr("OK"), QMessageBox.AcceptRole)
            cancel_btn = box.addButton(self.tr("Cancel"), QMessageBox.RejectRole)
        box.exec()
        clicked = box.clickedButton()
        if clicked == cancel_btn:
            return "cancel"
        if apply_all_btn and clicked == apply_all_btn:
            return "all"
        if target_count > 1 and clicked == ok_btn:
            return "one"
        return "one"

    def _maybe_rescale_measurements_for_image(
        self,
        image_id: int,
        old_scale: float,
        new_scale: float,
    ) -> bool:
        if not image_id or not old_scale or not new_scale:
            return True
        if abs(new_scale - old_scale) < 1e-6:
            return True
        measurements = MeasurementDB.get_measurements_for_image(image_id)
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
        return self._rescale_measurements_for_image(image_id, old_scale, new_scale, measurements)

    def _rescale_measurements_for_image(
        self,
        image_id: int,
        old_scale: float,
        new_scale: float,
        measurements: list[dict] | None = None,
    ) -> bool:
        if not image_id or not old_scale or not new_scale:
            return True
        if abs(new_scale - old_scale) < 1e-6:
            return True
        measurements = measurements or MeasurementDB.get_measurements_for_image(image_id)
        if not measurements:
            return True
        has_points = any(
            all(m.get(f"p{i}_{axis}") is not None for i in range(1, 5) for axis in ("x", "y"))
            for m in measurements
        )
        if not has_points:
            return True
        conn = get_connection()
        cursor = conn.cursor()
        for m in measurements:
            if not all(m.get(f"p{i}_{axis}") is not None for i in range(1, 5) for axis in ("x", "y")):
                continue
            dx1 = m["p2_x"] - m["p1_x"]
            dy1 = m["p2_y"] - m["p1_y"]
            dx2 = m["p4_x"] - m["p3_x"]
            dy2 = m["p4_y"] - m["p3_y"]
            dist1 = math.hypot(dx1, dy1) * new_scale
            dist2 = math.hypot(dx2, dy2) * new_scale
            length_um = max(dist1, dist2)
            width_um = min(dist1, dist2)
            q_value = length_um / width_um if width_um > 0 else 0
            cursor.execute(
                "UPDATE spore_measurements SET length_um = ?, width_um = ?, notes = ? WHERE id = ?",
                (length_um, width_um, f"Q={q_value:.1f}", m["id"]),
            )
        conn.commit()
        conn.close()
        return True

    def _apply_metadata_to_index(self, index: int) -> None:
        if index < 0 or index >= len(self.import_results):
            return
        result = self.import_results[index]
        dt_value = self.datetime_input.dateTime()
        result.captured_at = None if dt_value == self._unset_datetime else dt_value
        lat = self.lat_input.value()
        result.gps_latitude = None if lat == self.lat_input.minimum() else lat
        lon = self.lon_input.value()
        result.gps_longitude = None if lon == self.lon_input.minimum() else lon

    def _apply_metadata_to_indices(self, indices: list[int | None]) -> None:
        self._update_observation_metadata_from_inputs()
        if not self._setting_from_image_source:
            self._observation_source_index = None
        for idx in indices:
            if idx is None:
                continue
            self._apply_metadata_to_index(idx)
        if indices:
            self._refresh_gallery()

    def _cache_pixmap(self, path: str) -> None:
        if not path or path in self._pixmap_cache:
            return
        pixmap = QPixmap(path)
        if pixmap.isNull():
            return
        w = pixmap.width()
        h = pixmap.height()
        max_dim = self._max_preview_dim
        is_preview = False
        if max(w, h) > max_dim:
            pixmap = pixmap.scaled(
                max_dim,
                max_dim,
                Qt.KeepAspectRatio,
                Qt.SmoothTransformation
            )
            is_preview = True
        self._pixmap_cache[path] = pixmap
        self._pixmap_cache_is_preview[path] = is_preview

    def _get_cached_pixmap(self, path: str) -> QPixmap | None:
        if not path:
            return None
        pixmap = self._pixmap_cache.get(path)
        if pixmap is not None:
            if path not in self._pixmap_cache_is_preview:
                self._pixmap_cache_is_preview[path] = False
            return pixmap
        self._cache_pixmap(path)
        return self._pixmap_cache.get(path)

    def _apply_to_all(self) -> None:
        if not self.import_results:
            return
        self._apply_settings_to_indices(list(range(len(self.import_results))))

    def _apply_to_selected(self) -> None:
        indices = self.selected_indices or ([self.selected_index] if self.selected_index is not None else [])
        if not indices:
            return
        self._apply_settings_to_indices(indices)
        self._apply_metadata_to_indices(indices)

    def _set_observation_gps_from_image(self) -> None:
        if (
            self._current_exif_lat is None
            and self._current_exif_lon is None
            and self._current_exif_datetime is None
        ):
            return
        self._setting_from_image_source = True
        if self._current_exif_datetime is not None:
            self._observation_datetime = self._current_exif_datetime
            self.datetime_input.setDateTime(self._current_exif_datetime)
        if self._current_exif_lat is not None:
            self.lat_input.setValue(self._current_exif_lat)
        else:
            self.lat_input.setValue(self.lat_input.minimum())
        if self._current_exif_lon is not None:
            self.lon_input.setValue(self._current_exif_lon)
        else:
            self.lon_input.setValue(self.lon_input.minimum())
        self._update_observation_metadata_from_inputs()
        indices = self.selected_indices or ([self.selected_index] if self.selected_index is not None else [])
        if indices:
            self._observation_source_index = indices[0]
            for i, result in enumerate(self.import_results):
                result.gps_source = i == indices[0]
            self._apply_metadata_to_indices(indices)
        self._setting_from_image_source = False
        self._set_settings_hint(
            self.tr("Observation date and GPS set based on current image"),
            "#27ae60",
        )

    def _update_summary(self) -> None:
        if not hasattr(self, "summary_label"):
            return
        total = len(self.import_results)
        if total == 0:
            self.summary_label.setText(self.tr("No images added."))
            return
        microscope_count = sum(1 for item in self.import_results if item.image_type == "microscope")
        missing_scale = sum(1 for item in self.import_results if item.needs_scale)
        self.summary_label.setText(
            self.tr("Images: {total}\nMicroscope: {micro}\nMissing scale: {missing}").format(
                total=total,
                micro=microscope_count,
                missing=missing_scale,
            )
        )

    def _register_missing_exif_path(self, path: str | None) -> None:
        if not path:
            return
        self._missing_exif_paths.add(path)
        if not self._missing_exif_warning_scheduled:
            self._missing_exif_warning_scheduled = True
            QTimer.singleShot(0, self._show_missing_exif_warning)

    def _show_missing_exif_warning(self) -> None:
        self._missing_exif_warning_scheduled = False
        if not self._missing_exif_paths:
            return
        paths = sorted(self._missing_exif_paths)
        self._missing_exif_paths.clear()
        names = [Path(p).name for p in paths if p]
        preview = "\n".join(f"- {name}" for name in names[:5])
        extra = ""
        if len(names) > 5:
            extra = self.tr("\n...and {count} more.").format(count=len(names) - 5)
        QMessageBox.warning(
            self,
            self.tr("Missing image files"),
            self.tr(
                "Some image files are missing or were moved. EXIF data could not be read."
            )
            + "\n\n"
            + self.tr("Missing files:")
            + "\n"
            + preview
            + extra
            + "\n\n"
            + self.tr("Please relink or remove the missing images."),
        )

    def _update_current_image_exif(self, result: ImageImportResult) -> None:
        path = result.filepath
        self._current_exif_path = path
        exif = get_exif_data(path) if path else {}
        meta = get_image_metadata(path) if path else {}
        if meta.get("missing"):
            self._register_missing_exif_path(path)
        dt = meta.get("datetime")
        if not dt and result.preview_path and result.preview_path != result.filepath:
            meta_preview = get_image_metadata(result.preview_path)
            if meta_preview.get("missing"):
                self._register_missing_exif_path(result.preview_path)
            dt = meta_preview.get("datetime")
        if dt:
            self._current_exif_datetime = QDateTime(dt)
            self.exif_datetime_label.setText(self._current_exif_datetime.toString("yyyy-MM-dd HH:mm"))
        else:
            self._current_exif_datetime = None
            self.exif_datetime_label.setText("--")

        make = exif.get("Make") or ""
        model = exif.get("Model") or ""
        camera = " ".join(str(part).strip() for part in (make, model) if part).strip()
        self.exif_camera_label.setText(camera if camera else "--")

        iso = exif.get("ISOSpeedRatings") or exif.get("PhotographicSensitivity")
        if isinstance(iso, (list, tuple)):
            iso = iso[0] if iso else None
        self.exif_iso_label.setText(str(iso) if iso else "--")

        exposure = exif.get("ExposureTime") or exif.get("ShutterSpeedValue")
        exposure_text = self._format_exposure(exposure)
        self.exif_shutter_label.setText(exposure_text or "--")

        fnum = exif.get("FNumber") or exif.get("ApertureValue")
        fnum_text = self._format_aperture(fnum)
        self.exif_aperture_label.setText(fnum_text or "--")

        lat = meta.get("latitude")
        lon = meta.get("longitude")
        if (lat is None or lon is None) and path:
            lat2, lon2 = get_gps_coordinates(path)
            lat = lat if lat is not None else lat2
            lon = lon if lon is not None else lon2
        self._current_exif_lat = lat
        self._current_exif_lon = lon
        lat_text = f"Lat: {lat:.6f}" if lat is not None else "Lat: --"
        lon_text = f"Lon: {lon:.6f}" if lon is not None else "Lon: --"
        self.exif_lat_label.setText(lat_text)
        self.exif_lon_label.setText(lon_text)
        self.exif_map_btn.setEnabled(lat is not None and lon is not None)
        self._update_set_from_image_button_state()
        self._update_current_image_sampling(result)

    def _update_current_image_sampling(self, result: ImageImportResult | None) -> None:
        self._update_resize_info(result)

    def _get_image_size(self, path: str | None) -> tuple[int, int] | None:
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
        return width, height

    def _get_objective_scale_mpp(self, objective_key: str | None) -> float | None:
        if not objective_key:
            return None
        active_cal = CalibrationDB.get_active_calibration(objective_key)
        if active_cal:
            scale = active_cal.get("microns_per_pixel")
            if scale and scale > 0:
                return float(scale)
        history = CalibrationDB.get_calibrations_for_objective(objective_key)
        if history:
            scale = history[0].get("microns_per_pixel")
            if scale and scale > 0:
                return float(scale)
        return None

    def _compute_resample_scale_factor(
        self,
        result: ImageImportResult | None,
        respect_toggle: bool = True,
    ) -> float:
        if not result or result.image_type != "microscope":
            return 1.0
        if (
            respect_toggle
            and (
                not hasattr(self, "resize_optimal_checkbox")
                or not self.resize_optimal_checkbox.isChecked()
            )
        ):
            return 1.0
        objective = None
        if result.objective and result.objective in self.objectives:
            objective = self.objectives[result.objective]
        scale_mpp = result.custom_scale
        if scale_mpp is None:
            scale_mpp = self._get_objective_scale_mpp(result.objective)
        if not scale_mpp or scale_mpp <= 0:
            return 1.0
        na_value = objective.get("na") if objective else None
        if not na_value:
            return 1.0

        target_pct = float(
            objective.get("target_sampling_pct", self.target_sampling_pct or 120.0)
            if objective
            else (self.target_sampling_pct or 120.0)
        )
        pixels_per_micron = 1.0 / float(scale_mpp)
        result_info = get_resolution_status(pixels_per_micron, float(na_value))
        ideal_pixels_per_micron = float(result_info.get("ideal_pixels_per_micron", 0.0))
        if not ideal_pixels_per_micron or ideal_pixels_per_micron <= 0:
            return 1.0
        target_pixels_per_micron = ideal_pixels_per_micron * (target_pct / 100.0)
        factor = target_pixels_per_micron / pixels_per_micron
        if factor > 1.0:
            factor = 1.0
        # Skip resize when ideal area is close to current (>= 90% of current MP).
        if (float(factor) * float(factor)) >= 0.90:
            return 1.0
        return max(0.01, float(factor))

    def _update_resize_info(self, result: ImageImportResult | None) -> None:
        if not hasattr(self, "current_resolution_label") or not hasattr(self, "target_resolution_label"):
            return
        if not result or result.image_type != "microscope":
            self.current_resolution_label.setText("--")
            self.target_resolution_label.setText("--")
            self._set_resize_resolution_hints("", "")
            return
        size = self._get_image_size(result.filepath or result.preview_path or result.original_filepath)
        if not size:
            self.current_resolution_label.setText("--")
            self.target_resolution_label.setText("--")
            self._set_resize_resolution_hints("", "")
            return
        width, height = size
        current_mp = (width * height) / 1_000_000.0
        self.current_resolution_label.setText(f"{self._format_megapixels(current_mp)} MP ({width} x {height})")
        current_hint = self.tr("Current pixel dimensions and megapixels for this image.")
        objective = None
        if result.objective and result.objective in self.objectives:
            objective = self.objectives[result.objective]
        scale_mpp = result.custom_scale
        if scale_mpp is None:
            scale_mpp = self._get_objective_scale_mpp(result.objective)
        na_value = objective.get("na") if objective else None
        if hasattr(self, "target_sampling_input"):
            target_pct = float(
                objective.get("target_sampling_pct", self.target_sampling_pct)
                if objective
                else self.target_sampling_pct
            )
            self.target_sampling_input.blockSignals(True)
            self.target_sampling_input.setValue(float(target_pct))
            self.target_sampling_input.blockSignals(False)
        if self._is_resized_image(result):
            self.target_resolution_label.setText(self.tr("Already resized"))
            self._set_resize_resolution_hints(
                current_hint,
                self.tr("This image is already stored in a resized form."),
            )
            return
        if not scale_mpp or not na_value:
            self.target_resolution_label.setText("--")
            self._set_resize_resolution_hints(
                current_hint,
                self.tr("Ideal resolution requires both scale and objective NA."),
            )
            return
        factor = self._compute_resample_scale_factor(result)
        if result.image_id is None and not self._is_resized_image(result):
            result.resample_scale_factor = factor
        if not hasattr(self, "resize_optimal_checkbox") or not self.resize_optimal_checkbox.isChecked():
            self.target_resolution_label.setText("--")
            self._set_resize_resolution_hints(
                current_hint,
                self.tr("Enable resize to preview ideal output resolution."),
            )
            return
        target_pct = float(
            objective.get("target_sampling_pct", self.target_sampling_pct)
            if objective
            else self.target_sampling_pct
        )
        hint_text = self.tr(
            "Ideal resolution from objective scale, NA, and target sampling ({pct:.0f}% Nyquist)."
        ).format(pct=target_pct)
        if factor >= 0.999:
            self.target_resolution_label.setText(self.tr("Current (no resize)"))
            self._set_resize_resolution_hints(
                current_hint,
                self.tr("Ideal area is close to current area (>=90%), so resize is skipped."),
            )
            return
        target_w = max(1, int(round(width * factor)))
        target_h = max(1, int(round(height * factor)))
        target_mp = (target_w * target_h) / 1_000_000.0
        self.target_resolution_label.setText(
            f"{self._format_megapixels(target_mp)} MP ({target_w} x {target_h})"
        )
        self._set_resize_resolution_hints(current_hint, hint_text)

    def _on_resize_settings_changed(self) -> None:
        if not hasattr(self, "resize_optimal_checkbox"):
            return
        resize_enabled = bool(self.resize_optimal_checkbox.isChecked())
        self._resize_preview_enabled = resize_enabled
        self._sync_resize_controls()
        SettingsDB.set_setting("resize_to_optimal_sampling", resize_enabled)
        store_original = self._store_originals_enabled() if resize_enabled else False
        for result in self.import_results:
            result.resize_to_optimal = resize_enabled
            result.store_original = store_original
            if not result.image_id:
                result.resample_scale_factor = self._compute_resample_scale_factor(result)
        if self.selected_index is not None and 0 <= self.selected_index < len(self.import_results):
            self._update_current_image_sampling(self.import_results[self.selected_index])
        self._refresh_resize_preview(force=True, preserve_view=True)

    def _on_target_sampling_changed(self, value: float) -> None:
        self.target_sampling_pct = float(value)
        SettingsDB.set_setting("target_sampling_pct", float(value))
        selected_objective = self.objective_combo.currentData() if hasattr(self, "objective_combo") else None
        if selected_objective and selected_objective in self.objectives:
            self.objectives[selected_objective]["target_sampling_pct"] = float(value)
            save_objectives(self.objectives)
        for result in self.import_results:
            if not result.image_id:
                result.resample_scale_factor = self._compute_resample_scale_factor(result)
        if self.selected_index is not None and 0 <= self.selected_index < len(self.import_results):
            self._update_current_image_sampling(self.import_results[self.selected_index])
        self._refresh_resize_preview(preserve_view=True)

    def _store_originals_enabled(self) -> bool:
        storage_mode = SettingsDB.get_setting("original_storage_mode")
        if not storage_mode:
            return bool(SettingsDB.get_setting("store_original_images", False))
        return storage_mode != "none"

    def _format_megapixels(self, mp_value: float) -> str:
        text = f"{mp_value:.1f}"
        return text.rstrip("0").rstrip(".")

    def _format_significant(self, value: float, digits: int = 2) -> str:
        if not math.isfinite(value):
            return "--"
        if value == 0:
            return "0"
        digits = max(1, int(digits))
        order = int(math.floor(math.log10(abs(value))))
        decimals = max(0, digits - 1 - order)
        text = f"{value:.{decimals}f}"
        return text.rstrip("0").rstrip(".")

    def _get_image_megapixels(self, path: str | None) -> float | None:
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
                    mp = self._get_image_megapixels(info.get("path"))
                    if mp:
                        values.append(mp)
        if not values:
            mp = self._get_image_megapixels(cal.get("image_filepath"))
            if mp:
                values.append(mp)
        if values:
            return float(sum(values) / len(values))
        return None

    def _update_scale_mismatch_warning(self) -> None:
        if not hasattr(self, "scale_warning_label"):
            return
        def _hide_warning() -> None:
            self._set_hintable_label_text(self.scale_warning_label, "", "")
            self.scale_warning_label.setVisible(False)

        if not hasattr(self, "scale_group") or not self.scale_group.isEnabled():
            _hide_warning()
            return
        idx = self._current_single_index()
        if idx is None or idx < 0 or idx >= len(self.import_results):
            _hide_warning()
            return
        result = self.import_results[idx]
        if result.image_type != "microscope":
            _hide_warning()
            return
        selected_objective = self.objective_combo.currentData()
        if not selected_objective or selected_objective == self.CUSTOM_OBJECTIVE_KEY:
            _hide_warning()
            return
        calibration = CalibrationDB.get_active_calibration(selected_objective)
        calibration_mp = calibration.get("megapixels") if calibration else None
        estimate = self._estimate_calibration_megapixels(calibration)
        if isinstance(calibration_mp, (int, float)) and calibration_mp > 0:
            if estimate:
                diff_ratio = abs(float(calibration_mp) - float(estimate)) / max(1e-6, float(estimate))
                if diff_ratio > 0.01:
                    calibration_mp = estimate
        elif estimate:
            calibration_mp = estimate
        if not isinstance(calibration_mp, (int, float)) or calibration_mp <= 0:
            _hide_warning()
            return
        image_mp = self._get_image_megapixels(
            result.filepath or result.preview_path or result.original_filepath
        )
        if not image_mp:
            _hide_warning()
            return
        effective_mp = float(image_mp)
        factor = getattr(result, "resample_scale_factor", None)
        if self._is_resized_image(result) and isinstance(factor, (int, float)) and factor > 0:
            effective_mp = float(image_mp) / (float(factor) * float(factor))
        diff_ratio = abs(float(effective_mp) - float(calibration_mp)) / max(1e-6, float(calibration_mp))
        if diff_ratio <= 0.01:
            _hide_warning()
            return
        ratio = max(effective_mp, calibration_mp) / max(1e-6, min(effective_mp, calibration_mp))
        if ratio < 1.5:
            _hide_warning()
            return
        hint_text = self.tr(
            "Calibration image: {cal}MP. This image: {img}MP. "
            "This is ok if you are working on a cropped image."
        ).format(
            cal=self._format_megapixels(float(calibration_mp)),
            img=self._format_megapixels(float(effective_mp)),
        )
        self._set_hintable_label_text(
            self.scale_warning_label,
            self.tr("Resolution mismatch!"),
            hint_text,
            hint_tone="warning",
        )
        self.scale_warning_label.setVisible(True)


    def _clear_current_image_exif(self) -> None:
        self._current_exif_path = None
        self._current_exif_datetime = None
        self._current_exif_lat = None
        self._current_exif_lon = None
        self.exif_datetime_label.setText("--")
        self.exif_camera_label.setText("--")
        self.exif_iso_label.setText("--")
        self.exif_shutter_label.setText("--")
        self.exif_aperture_label.setText("--")
        self.exif_lat_label.setText("Lat: --")
        self.exif_lon_label.setText("Lon: --")
        self.exif_map_btn.setEnabled(False)
        if hasattr(self, "exif_sampling_label"):
            self._set_hintable_label_text(self.exif_sampling_label, "--", "")
        if hasattr(self, "current_resolution_label"):
            self.current_resolution_label.setText("--")
        if hasattr(self, "target_resolution_label"):
            self.target_resolution_label.setText("--")
        self._set_resize_resolution_hints("", "")

    def _show_multi_selection_state(self) -> None:
        self.preview.set_image(None)
        self.preview.set_corner_tag("")
        self.preview_stack.setCurrentWidget(self.preview_message)
        self._clear_current_image_exif()
        self._update_set_from_image_button_state()
        if self._ai_crop_active:
            self._set_ai_crop_active(False)
        self._set_ai_status(None)
        self._update_ai_controls_state()
        self._update_ai_table()
        self._update_ai_overlay()

    def _format_exposure(self, value) -> str | None:
        if value is None:
            return None
        num, den = self._split_ratio(value)
        if num is None or den in (None, 0):
            try:
                val = float(value)
            except Exception:
                return None
            return f"{val:.3f}s" if val >= 0.01 else f"1/{int(round(1 / val))}"
        if num == 0:
            return None
        if num < den:
            return f"1/{int(round(den / num))}"
        return f"{num / den:.2f}s"

    def _format_aperture(self, value) -> str | None:
        if value is None:
            return None
        num, den = self._split_ratio(value)
        if num is None or den in (None, 0):
            try:
                val = float(value)
            except Exception:
                return None
            return f"f/{val:.1f}"
        return f"f/{(num / den):.1f}"

    @staticmethod
    def _split_ratio(value):
        if isinstance(value, tuple) and len(value) == 2:
            return value[0], value[1]
        if hasattr(value, "numerator") and hasattr(value, "denominator"):
            return value.numerator, value.denominator
        return None, None

    def _open_current_image_map(self) -> None:
        if self._current_exif_lat is None or self._current_exif_lon is None:
            return
        from .observations_tab import MapServiceHelper
        if not hasattr(self, "_map_helper"):
            self._map_helper = MapServiceHelper(self)
        self._map_helper.show_map_service_dialog(
            self._current_exif_lat, self._current_exif_lon
        )

    def _refresh_gallery(self) -> None:
        selected = self.gallery.selected_paths() if hasattr(self, "gallery") else []
        items = []
        for idx, result in enumerate(self.import_results):
            objective_label = result.objective
            if result.objective and result.objective in self.objectives:
                objective_label = objective_display_name(
                    self.objectives[result.objective],
                    result.objective,
                ) or result.objective
            badges = ImageGalleryWidget.build_image_type_badges(
                image_type=result.image_type,
                objective_name=objective_label,
                contrast=result.contrast,
                custom_scale=bool(result.custom_scale),
                needs_scale=bool(result.needs_scale),
                translate=self.tr,
            )
            has_measurements = False
            if result.image_id:
                has_measurements = bool(MeasurementDB.get_measurements_for_image(result.image_id))
            is_source = self._observation_source_index == idx
            gps_highlight = is_source and result.exif_has_gps
            gps_tag = self.tr("GPS") if gps_highlight else None
            items.append(
                {
                    "id": result.image_id,
                    "filepath": result.filepath,
                    "preview_path": result.preview_path or result.filepath,
                    "image_number": idx + 1,
                    "badges": badges,
                    "gps_tag_text": gps_tag,
                    "gps_tag_highlight": gps_highlight,
                    "has_measurements": has_measurements,
                }
            )
        self.gallery.set_items(items)
        if selected:
            self.gallery.select_paths(selected)

    def _on_remove_selected(self) -> None:
        if not self.selected_indices:
            return
        measurement_indices = []
        for idx in self.selected_indices:
            if idx is None or idx < 0 or idx >= len(self.import_results):
                continue
            image_id = self.import_results[idx].image_id
            if not image_id:
                continue
            try:
                if MeasurementDB.get_measurements_for_image(image_id):
                    measurement_indices.append(idx)
            except Exception:
                continue
        if measurement_indices:
            count = len(measurement_indices)
            if not ask_measurements_exist_delete(self, count=count):
                return
        removed_numbers = [idx + 1 for idx in self.selected_indices if idx is not None]
        removed_indices = sorted(idx for idx in self.selected_indices if idx is not None)
        for idx in sorted(self.selected_indices, reverse=True):
            if 0 <= idx < len(self.import_results):
                del self.import_results[idx]
                del self.image_paths[idx]
        if removed_indices:
            self._remap_ai_indices(removed_indices)
        self.selected_indices = []
        self.selected_index = None
        self.primary_index = None
        self._refresh_gallery()
        self._update_summary()
        if removed_numbers:
            if len(removed_numbers) == 1:
                message = self.tr("Image {num} deleted").format(num=removed_numbers[0])
            else:
                message = self.tr("Deleted {count} images").format(count=len(removed_numbers))
            self._set_settings_hint(message, "#e74c3c")
        if self.image_paths:
            self._select_image(0)

    def _on_gallery_delete_requested(self, image_key) -> None:
        if image_key is None:
            return
        index = None
        for idx, result in enumerate(self.import_results):
            if image_key == result.image_id or image_key == result.filepath:
                index = idx
                break
        if index is None:
            return
        self.selected_indices = [index]
        self.selected_index = index
        self._on_remove_selected()

    def _remap_ai_indices(self, removed_indices: list[int]) -> None:
        def new_index(old_index: int) -> int:
            shift = 0
            for removed in removed_indices:
                if removed < old_index:
                    shift += 1
            return old_index - shift

        def remap_dict(source: dict[int, object]) -> dict[int, object]:
            remapped = {}
            for old_index, value in source.items():
                if old_index in removed_indices:
                    continue
                remapped[new_index(old_index)] = value
            return remapped

        self._ai_predictions_by_index = remap_dict(self._ai_predictions_by_index)
        self._ai_selected_by_index = remap_dict(self._ai_selected_by_index)
        self._ai_crop_boxes = remap_dict(self._ai_crop_boxes)
        self._ai_selected_taxon = None

    def _accept_continue(self) -> None:
        self._apply_to_selected()
        self._save_last_used_tag_settings()
        self._accepted = True
        self.continueRequested.emit(self.import_results)
        self.accept()

    def _save_last_used_tag_settings(self) -> None:
        SettingsDB.set_setting(
            DatabaseTerms.last_used_key("contrast"),
            self._get_combo_tag_value(self.contrast_combo, "contrast"),
        )
        SettingsDB.set_setting(
            DatabaseTerms.last_used_key("mount"),
            self._get_combo_tag_value(self.mount_combo, "mount"),
        )
        SettingsDB.set_setting(
            DatabaseTerms.last_used_key("sample"),
            self._get_combo_tag_value(self.sample_combo, "sample"),
        )

    def get_observation_gps(self) -> tuple[float | None, float | None]:
        return self._observation_lat, self._observation_lon

    def enter_calibration_mode(self, dialog):
        if not getattr(self, "preview", None) or not self.preview.original_pixmap:
            return
        if hasattr(self, "ai_crop_btn") and self.ai_crop_btn.isChecked():
            self.ai_crop_btn.setChecked(False)
        if hasattr(self.preview, "ensure_full_resolution"):
            self.preview.ensure_full_resolution()
        self.calibration_dialog = dialog
        self.calibration_points = []
        self._calibration_mode = True
        self.preview.clear_preview_line()

    def _on_preview_clicked(self, pos):
        if not getattr(self, "_calibration_mode", False):
            return
        self.calibration_points.append(pos)
        if len(self.calibration_points) == 1:
            self.preview.set_preview_line(pos)
            return
        if len(self.calibration_points) == 2:
            p1, p2 = self.calibration_points
            dx = p2.x() - p1.x()
            dy = p2.y() - p1.y()
            distance = (dx * dx + dy * dy) ** 0.5
            if distance <= 0:
                self.calibration_points = []
                return
            perp_x = -dy / distance
            perp_y = dx / distance
            half_width = max(6.0, distance * 0.05)
            mid = QPointF((p1.x() + p2.x()) / 2, (p1.y() + p2.y()) / 2)
            p3 = QPointF(mid.x() - perp_x * half_width, mid.y() - perp_y * half_width)
            p4 = QPointF(mid.x() + perp_x * half_width, mid.y() + perp_y * half_width)
            self.preview.set_measurement_lines([[p1.x(), p1.y(), p2.x(), p2.y()]])
            self.preview.clear_preview_line()
            self._calibration_mode = False
            if self.calibration_dialog:
                self.calibration_dialog.set_calibration_distance(distance)
                self.calibration_dialog.set_calibration_preview(
                    self.preview.original_pixmap,
                    [p1, p2, p3, p4],
                )
            self.calibration_points = []

    def closeEvent(self, event):
        if self._ai_thread is not None:
            try:
                self._ai_thread.quit()
                self._ai_thread.wait(1000)
            except Exception:
                pass
        for path in list(self._temp_preview_paths):
            try:
                Path(path).unlink(missing_ok=True)
            except Exception:
                pass
        self._temp_preview_paths.clear()
        if not self._accepted:
            for path in list(self._converted_import_paths):
                try:
                    Path(path).unlink(missing_ok=True)
                except Exception:
                    pass
            self._converted_import_paths.clear()
        super().closeEvent(event)
