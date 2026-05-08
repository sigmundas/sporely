"""Dialog for preparing images before creating an observation."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import math
import json
import shutil
import os
import time

from PySide6.QtCore import (
    Qt,
    QDateTime,
    QDate,
    QTime,
    QSettings,
    QStandardPaths,
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
    QApplication,
    QAbstractSpinBox,
    QButtonGroup,
    QComboBox,
    QDateTimeEdit,
    QDialog,
    QDoubleSpinBox,
    QFileDialog,
    QFormLayout,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QListView,
    QLineEdit,
    QPlainTextEdit,
    QPushButton,
    QStackedLayout,
    QSizePolicy,
    QSplitter,
    QTextEdit,
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
    QFrame,
)

from app_identity import APP_NAME, SETTINGS_APP, SETTINGS_ORG
from database.schema import (
    load_objectives,
    save_objectives,
    get_images_dir,
    get_connection,
    get_app_settings,
    update_app_settings,
    objective_display_name,
    objective_sort_value,
)
from database.models import SettingsDB, ImageDB, MeasurementDB, CalibrationDB
from database.database_tags import DatabaseTerms
from utils.vernacular_utils import normalize_vernacular_language
from utils.exif_reader import get_image_metadata, get_exif_data, get_gps_coordinates, get_camera_model
from utils.heic_converter import maybe_convert_heic
from .image_gallery_widget import ImageGalleryWidget
from .zoomable_image_widget import ZoomableImageLabel
from .spore_preview_widget import SporePreviewWidget
from .calibration_dialog import get_resolution_status
from .hint_status import HintBar, HintLabel, HintStatusController, style_progress_widgets
from .dialog_helpers import ask_measurements_exist_delete, make_github_help_button
from .section_card import create_section_card
from .styles import pt, _is_dark
from .window_state import GeometryMixin

SUPPORTED_IMAGE_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".tif",
    ".tiff",
    ".heic",
    ".heif",
    ".orf",
    ".nef",
}


def _debug_import_flow_enabled() -> bool:
    value = (
        os.environ.get("SPORELY_DEBUG_IMPORT_FLOW", "")
        or os.environ.get("MYCOLOG_DEBUG_IMPORT_FLOW", "")
    )
    return str(value).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _debug_import_flow(message: str) -> None:
    if _debug_import_flow_enabled():
        print(f"[{APP_NAME} debug][prepare-images] {message}", flush=True)


def dropped_image_paths_from_mime_data(mime_data) -> list[str]:
    """Return unique local image paths from a drag/drop payload."""
    if mime_data is None or not mime_data.hasUrls():
        return []
    paths: list[str] = []
    seen: set[str] = set()
    for url in mime_data.urls() or []:
        if url is None or not url.isLocalFile():
            continue
        local_path = str(Path(url.toLocalFile()).expanduser())
        if not local_path or local_path in seen:
            continue
        suffix = Path(local_path).suffix.lower()
        if suffix not in SUPPORTED_IMAGE_EXTENSIONS:
            continue
        if not Path(local_path).is_file():
            continue
        seen.add(local_path)
        paths.append(local_path)
    return paths


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
    stain: Optional[str] = None
    sample_type: Optional[str] = None
    notes: Optional[str] = None
    captured_at: Optional[QDateTime] = None
    gps_latitude: Optional[float] = None
    gps_longitude: Optional[float] = None
    needs_scale: bool = False
    exif_has_gps: bool = False
    calibration_id: Optional[int] = None
    ai_crop_box: Optional[tuple[float, float, float, float]] = None
    ai_crop_source_size: Optional[tuple[int, int]] = None
    crop_mode: Optional[str] = None
    pending_image_crop_offset: Optional[tuple[int, int]] = None
    gps_source: bool = False
    resample_scale_factor: Optional[float] = None
    resize_to_optimal: bool = False
    store_original: bool = False
    original_filepath: Optional[str] = None
    scale_bar_selection: Optional[tuple] = None  # ((p1x, p1y), (p2x, p2y)) in image coords
    scale_bar_length_um: Optional[float] = None  # µm value entered by user for the scale bar


@dataclass
class ImageCropUndoState:
    backup_path: str
    filepath: str
    preview_path: Optional[str]
    image_path: str
    ai_crop_box: Optional[tuple[float, float, float, float]]
    ai_crop_source_size: Optional[tuple[int, int]]
    crop_mode: Optional[str]
    pending_image_crop_offset: Optional[tuple[int, int]]
    scale_bar_selection: Optional[tuple]
    had_ai_crop_entry: bool
    ai_crop_entry: Optional[tuple[float, float, float, float]]
    had_crop_mode_entry: bool
    crop_mode_entry: Optional[str]


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
            url = "https://ai.artsdatabanken.no"
            for request in self.requests:
                if self.isInterruptionRequested():
                    break
                index = request.get("index")
                if index is None:
                    continue
                temp_path: Path | None = None
                response = None
                try:
                    temp_path = self._prepare_image(request["image_path"], request.get("crop_box"))
                    with open(temp_path, "rb") as handle:
                        response = requests.post(
                            url,
                            files={"image": (temp_path.name, handle, "image/jpeg")},
                            headers={"User-Agent": f"{APP_NAME}/AI"},
                            timeout=30,
                        )
                    if self.isInterruptionRequested():
                        break
                    if response.status_code != 200:
                        with open(temp_path, "rb") as handle:
                            response = requests.post(
                                url,
                                files={"file": (temp_path.name, handle, "image/jpeg")},
                                headers={"User-Agent": f"{APP_NAME}/AI"},
                                timeout=30,
                            )
                    if self.isInterruptionRequested():
                        break
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
                    self.resultReady.emit([int(index)], predictions, None, warnings, [str(temp_path)])
                except Exception as exc:
                    if self.isInterruptionRequested():
                        break
                    self.error.emit([int(index)], str(exc))
                finally:
                    try:
                        if response is not None:
                            response.close()
                    except Exception:
                        pass
                    if temp_path is not None:
                        try:
                            temp_path.unlink(missing_ok=True)
                        except Exception:
                            pass
        except Exception as exc:
            self.error.emit(self.indices, str(exc))


class ScaleBarImportDialog(QDialog):
    """Scale bar calibration dialog used from Prepare Images."""

    def __init__(
        self,
        image_dialog,
        initial_um: float = 10.0,
        previous_key: str | None = None,
        is_field: bool = False,
    ):
        super().__init__(image_dialog)
        self.setWindowTitle(self.tr("Scale bar"))
        self.setModal(False)
        self.image_dialog = image_dialog
        self.previous_key = previous_key
        self.is_field = bool(is_field)
        self.scale_applied = False
        self.auto_apply = False
        self._pending_distance_px: float | None = None

        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.length_input = QDoubleSpinBox()
        self.length_input.setRange(0.1, 100000.0)
        self.length_input.setDecimals(2)
        initial_value = float(initial_um) / 1000.0 if self.is_field else float(initial_um)
        self.length_input.setValue(initial_value)
        self.length_input.setSuffix(" mm" if self.is_field else " \u03bcm")
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
        length_um = float(self.length_input.value()) * 1000.0 if self.is_field else float(self.length_input.value())
        scale_um = length_um / self._pending_distance_px
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
        length_um = float(self.length_input.value()) * 1000.0 if self.is_field else float(self.length_input.value())
        scale_um = length_um / self._pending_distance_px
        if self.is_field:
            self.scale_label.setText(f"{(scale_um / 1000.0):.4f} mm/px")
        else:
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
    _gallery_splitter_key = "splitter/ImageImportDialogBottom"
    CUSTOM_OBJECTIVE_KEY = "__custom__"
    _FIELD_TAG_DEFAULTS = {
        "contrast": DatabaseTerms.CONTRAST_METHODS[0],
        "mount": DatabaseTerms.MOUNT_MEDIA[0],
        "stain": DatabaseTerms.STAIN_TYPES[0],
        "sample": DatabaseTerms.SAMPLE_TYPES[0],
    }

    def __init__(
        self,
        parent=None,
        image_paths: Optional[list[str]] = None,
        import_results: Optional[list[ImageImportResult]] = None,
        observation_datetime: QDateTime | None = None,
        observation_lat: float | None = None,
        observation_lon: float | None = None,
        continue_to_observation_details: bool = True,
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
        self.stain_options = self._load_tag_options("stain")
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
        self.stain_default = self._preferred_tag_value(
            "stain",
            self.stain_options,
            DatabaseTerms.STAIN_TYPES[0],
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
        self._image_size_cache: dict[str, tuple[int, int]] = {}
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
        self._close_cleanup_done = False
        self._setting_from_image_source = False
        self._last_settings_action: str | None = None
        self._ai_predictions_by_index: dict[int, list[dict]] = {}
        self._ai_selected_by_index: dict[int, dict] = {}
        self._ai_selected_taxon: dict | None = None
        self._ai_crop_boxes: dict[int, tuple[float, float, float, float]] = {}
        self._crop_mode_by_index: dict[int, str] = {}
        self._image_crop_undo_by_index: dict[int, ImageCropUndoState] = {}
        self._crop_active_mode: str | None = None
        self._ai_thread: QThread | None = None
        self._scale_bar_dialog: ScaleBarImportDialog | None = None
        self._scale_bar_pixel_distance: float | None = None  # pixel length of last scale bar selection
        self._last_objective_key: str | None = None
        self._hint_controller: HintStatusController | None = None
        self._pending_hint_widgets: list[tuple[QWidget, str, str]] = []
        self._continue_to_observation_details = bool(continue_to_observation_details)
        self._dialog_gallery_splitter_syncing = False

        self._build_ui()
        self._setup_drop_targets()
        if hasattr(self, "objective_combo"):
            self._last_objective_key = self.objective_combo.currentData()
        if import_results:
            self.set_import_results(import_results)
        elif image_paths:
            self.add_images(image_paths)
        self._restore_geometry()
        self.finished.connect(self._save_geometry)

    def _dialog_gallery_default_height(self) -> int:
        gallery = getattr(self, "gallery", None)
        if gallery is None:
            return 175
        return max(gallery.minimumHeight(), min(220, gallery.preferred_single_row_height() + 20))

    def _dialog_gallery_max_height(self) -> int:
        return 360

    def _apply_dialog_gallery_splitter_height(self, gallery_height: int | None = None) -> None:
        splitter = getattr(self, "dialog_gallery_splitter", None)
        gallery = getattr(self, "gallery", None)
        if splitter is None or gallery is None:
            return
        target = self._dialog_gallery_default_height() if gallery_height is None else int(gallery_height)
        target = max(gallery.minimumHeight(), min(self._dialog_gallery_max_height(), target))
        gallery.setMaximumHeight(self._dialog_gallery_max_height())
        sizes = splitter.sizes()
        total = sum(sizes) if sizes else 0
        if total <= 0:
            total = splitter.height()
        if total <= 0:
            total = max(self.height(), target + 600)
        if self._dialog_gallery_splitter_syncing:
            return
        self._dialog_gallery_splitter_syncing = True
        try:
            splitter.setSizes([max(0, total - target), target])
        finally:
            self._dialog_gallery_splitter_syncing = False
        if os.environ.get("SPORELY_DEBUG_GALLERY_HEIGHT"):
            print(
                "Prepare Images gallery height applied:",
                {
                    "requested": gallery_height,
                    "target": target,
                    "splitter_sizes": splitter.sizes(),
                    "min": gallery.minimumHeight(),
                    "max": gallery.maximumHeight(),
                },
            )

    def _restore_dialog_gallery_splitter(self) -> None:
        settings = QSettings(SETTINGS_ORG, SETTINGS_APP)
        raw_sizes = settings.value(self._gallery_splitter_key)
        parsed: list[int] = []
        if isinstance(raw_sizes, (list, tuple)):
            for value in raw_sizes[:2]:
                try:
                    parsed.append(max(0, int(value)))
                except Exception:
                    parsed.append(0)
        gallery_height = parsed[1] if len(parsed) >= 2 else None
        if gallery_height is None:
            try:
                gallery_height = int(raw_sizes)
            except Exception:
                gallery_height = None
        self._apply_dialog_gallery_splitter_height(gallery_height)

    def _save_dialog_gallery_splitter(self) -> None:
        splitter = getattr(self, "dialog_gallery_splitter", None)
        if splitter is None:
            return
        settings = QSettings(SETTINGS_ORG, SETTINGS_APP)
        settings.setValue(self._gallery_splitter_key, splitter.sizes())

    def _on_dialog_gallery_splitter_moved(self, _pos: int, _index: int) -> None:
        splitter = getattr(self, "dialog_gallery_splitter", None)
        if splitter is None or self._dialog_gallery_splitter_syncing:
            return
        if os.environ.get("SPORELY_DEBUG_GALLERY_HEIGHT"):
            print("Prepare Images gallery splitter moved:", {"sizes": splitter.sizes()})
        self._save_dialog_gallery_splitter()

    def _restore_geometry(self) -> None:
        super()._restore_geometry()
        QTimer.singleShot(0, self._restore_dialog_gallery_splitter)

    def _save_geometry(self) -> None:
        self._save_dialog_gallery_splitter()
        super()._save_geometry()

    def _build_ui(self) -> None:
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(8)

        self.left_panel = self._build_left_panel()
        self.left_panel.setMinimumWidth(320)
        self.left_panel.setMaximumWidth(460)
        self.left_panel.setStyleSheet(
            "QPushButton { padding: 4px 8px; }"
            "QLineEdit, QComboBox, QSpinBox, QDoubleSpinBox { padding: 4px 6px; }"
        )
        self.left_panel.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Expanding)

        self.gallery = ImageGalleryWidget(
            self.tr("Images"),
            self,
            show_delete=True,
            show_badges=True,
            min_height=125,
            default_height=175,
            thumbnail_size=110,
        )
        self.gallery.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.gallery.set_reorderable(True)
        self.gallery.set_multi_select(True)
        self.gallery.setMaximumHeight(self._dialog_gallery_max_height())
        self.gallery.imageClicked.connect(self._on_gallery_clicked)
        self.gallery.selectionChanged.connect(self._on_gallery_selection_changed)
        self.gallery.deleteRequested.connect(self._on_gallery_delete_requested)
        self.gallery.itemsReordered.connect(self._on_gallery_items_reordered)
        self.delete_shortcut = QShortcut(QKeySequence.Delete, self)
        self.delete_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.delete_shortcut.activated.connect(self._on_remove_selected)
        self.delete_shortcut_alt = QShortcut(QKeySequence(Qt.ALT | Qt.Key_D), self)
        self.delete_shortcut_alt.setContext(Qt.WidgetWithChildrenShortcut)
        self.delete_shortcut_alt.activated.connect(self._on_remove_selected)
        self.delete_shortcut_cmd = QShortcut(QKeySequence(Qt.CTRL | Qt.Key_D), self)
        self.delete_shortcut_cmd.setContext(Qt.WidgetWithChildrenShortcut)
        self.delete_shortcut_cmd.activated.connect(self._on_remove_selected)
        self.resize_preview_shortcut = QShortcut(QKeySequence("R"), self)
        self.resize_preview_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.resize_preview_shortcut.activated.connect(self._toggle_resize_preview)
        self.crop_shortcut = QShortcut(QKeySequence("C"), self)
        self.crop_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.crop_shortcut.activated.connect(self._on_image_crop_clicked)
        self.field_shortcut = QShortcut(QKeySequence("F"), self)
        self.field_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.field_shortcut.activated.connect(lambda: self._apply_image_type_shortcut("field"))
        self.micro_shortcut = QShortcut(QKeySequence("M"), self)
        self.micro_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.micro_shortcut.activated.connect(lambda: self._apply_image_type_shortcut("micro"))
        self.scale_shortcut = QShortcut(QKeySequence("S"), self)
        self.scale_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.scale_shortcut.activated.connect(self._on_scale_shortcut)
        self.next_image_shortcut = QShortcut(QKeySequence("N"), self)
        self.next_image_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.next_image_shortcut.activated.connect(self._on_next_image_shortcut)
        self.previous_image_shortcut = QShortcut(QKeySequence("P"), self)
        self.previous_image_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.previous_image_shortcut.activated.connect(self._on_previous_image_shortcut)
        self.next_image_arrow_shortcut = QShortcut(QKeySequence(Qt.Key_Right), self)
        self.next_image_arrow_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.next_image_arrow_shortcut.activated.connect(self._on_next_image_shortcut)
        self.previous_image_arrow_shortcut = QShortcut(QKeySequence(Qt.Key_Left), self)
        self.previous_image_arrow_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self.previous_image_arrow_shortcut.activated.connect(self._on_previous_image_shortcut)
        self.center_panel = self._build_center_panel()

        self.dialog_gallery_splitter = QSplitter(Qt.Vertical)
        self.dialog_gallery_splitter.setChildrenCollapsible(False)
        self.dialog_gallery_splitter.setHandleWidth(10)
        self.dialog_gallery_splitter.setStyleSheet(
            "QSplitter::handle:vertical {"
            " background: rgba(88, 96, 100, 0.35);"
            " margin: 3px 0px;"
            " border-radius: 2px;"
            "}"
            "QSplitter::handle:vertical:hover { background: rgba(71, 100, 92, 0.8); }"
        )
        self.dialog_gallery_splitter.addWidget(self.center_panel)
        self.dialog_gallery_splitter.addWidget(self.gallery)
        self.dialog_gallery_splitter.setStretchFactor(0, 1)
        self.dialog_gallery_splitter.setStretchFactor(1, 0)
        self.dialog_gallery_splitter.setSizes([760, self._dialog_gallery_default_height()])
        self.dialog_gallery_splitter.splitterMoved.connect(self._on_dialog_gallery_splitter_moved)

        self.details_panel = self._build_right_panel()
        self.details_panel.setMinimumWidth(340)
        self.details_panel.setMaximumWidth(520)
        self.details_panel.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Expanding)

        self.main_splitter = QSplitter(Qt.Horizontal)
        self.main_splitter.setChildrenCollapsible(False)
        self.main_splitter.addWidget(self.left_panel)
        self.main_splitter.addWidget(self.dialog_gallery_splitter)
        self.main_splitter.addWidget(self.details_panel)
        self.main_splitter.setStretchFactor(0, 0)
        self.main_splitter.setStretchFactor(1, 1)
        self.main_splitter.setStretchFactor(2, 0)
        self.main_splitter.setSizes([360, 980, 420])
        main_layout.addWidget(self.main_splitter, 1)

        bottom_row = QHBoxLayout()
        bottom_row.setContentsMargins(0, 0, 0, 0)
        bottom_row.setSpacing(8)
        hint_area = QWidget(self)
        hint_area_layout = QVBoxLayout(hint_area)
        hint_area_layout.setContentsMargins(0, 0, 0, 0)
        hint_area_layout.setSpacing(4)
        self.hint_bar = HintBar(self)
        hint_area_layout.addWidget(self.hint_bar)
        self.hint_progress_widget = QWidget(self)
        hint_progress_layout = QHBoxLayout(self.hint_progress_widget)
        hint_progress_layout.setContentsMargins(0, 0, 0, 0)
        hint_progress_layout.setSpacing(0)
        progress_stack = QWidget(self.hint_progress_widget)
        progress_stack_layout = QVBoxLayout(progress_stack)
        progress_stack_layout.setContentsMargins(0, 0, 0, 0)
        progress_stack_layout.setSpacing(4)
        self.hint_progress_status = QLabel("")
        self.hint_progress_status.setWordWrap(True)
        self.hint_progress_status.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        self.hint_progress_status.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.hint_progress_bar = QProgressBar(self)
        self.hint_progress_bar.setRange(0, 100)
        self.hint_progress_bar.setValue(0)
        self.hint_progress_bar.setTextVisible(True)
        self.hint_progress_bar.setFixedHeight(18)
        self.hint_progress_bar.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        style_progress_widgets(self.hint_progress_bar, self.hint_progress_status)
        progress_stack_layout.addWidget(self.hint_progress_bar, 0)
        progress_stack_layout.addWidget(self.hint_progress_status, 0)
        hint_progress_layout.addWidget(progress_stack, 1)
        self.hint_progress_widget.setVisible(False)
        hint_area_layout.addWidget(self.hint_progress_widget)
        bottom_row.addWidget(hint_area, 1)
        bottom_row.addWidget(make_github_help_button(self, "prepare-images-dialog.md"), 0, Qt.AlignRight | Qt.AlignVCenter)
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

        next_label = self.tr("Continue") if self._continue_to_observation_details else self.tr("Close")
        self.next_btn = QPushButton(next_label)
        self.next_btn.setMinimumHeight(35)
        self.next_btn.clicked.connect(self._accept_and_close)
        bottom_row.addWidget(self.next_btn)
        self._update_action_buttons_state()
        main_layout.addLayout(bottom_row)

    def _register_drop_target(self, widget: QWidget | None) -> None:
        if widget is None:
            return
        widget.setAcceptDrops(True)
        widget.installEventFilter(self)

    def _setup_drop_targets(self) -> None:
        targets = [
            self,
            getattr(self, "left_panel", None),
            getattr(self, "center_panel", None),
            getattr(self, "main_splitter", None),
            getattr(self, "gallery", None),
            getattr(self, "preview", None),
            getattr(self, "details_panel", None),
        ]
        targets.extend(self.findChildren(QWidget))
        seen: set[int] = set()
        for target in targets:
            if target is None:
                continue
            marker = id(target)
            if marker in seen:
                continue
            seen.add(marker)
            self._register_drop_target(target)

    def _accept_image_drag(self, event) -> bool:
        if not dropped_image_paths_from_mime_data(event.mimeData()):
            return False
        event.acceptProposedAction()
        return True

    def _handle_image_drop(self, event) -> bool:
        paths = dropped_image_paths_from_mime_data(event.mimeData())
        if not paths:
            return False
        self.add_images(paths)
        event.acceptProposedAction()
        return True

    def dragEnterEvent(self, event) -> None:
        if self._accept_image_drag(event):
            return
        super().dragEnterEvent(event)

    def dragMoveEvent(self, event) -> None:
        if self._accept_image_drag(event):
            return
        super().dragMoveEvent(event)

    def dropEvent(self, event) -> None:
        if self._handle_image_drop(event):
            return
        super().dropEvent(event)

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
        dark = _is_dark("auto")
        view = QListView()
        view.setSpacing(0)
        view.setUniformItemSizes(True)
        if dark:
            view.setStyleSheet(
                "QListView { background: #2b2b2d; color: #e8e8e8; }"
                "QListView::item { color: #e8e8e8; background: #2b2b2d; padding: 4px 8px; min-height: 24px; margin: 0px; }"
                "QListView::item:hover { background: #1c3a5e; color: #c0deff; }"
                "QListView::item:selected { background: #4a90d9; color: white; }"
            )
        else:
            view.setStyleSheet(
                "QListView { background: white; color: #2c3e50; }"
                "QListView::item { color: #2c3e50; background: white; padding: 4px 8px; min-height: 24px; margin: 0px; }"
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
            "stain": "stain_default",
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

    def _set_tag_combo_neutral_display(self, combo: QComboBox, category: str, blank: bool) -> None:
        idx = combo.findData(self._field_tag_value(category))
        if idx < 0:
            return
        combo.setItemText(idx, "" if blank else DatabaseTerms.translate(category, self._field_tag_value(category)))

    def _sync_field_tag_display(self, blank: bool) -> None:
        self._set_tag_combo_neutral_display(self.contrast_combo, "contrast", blank)
        self._set_tag_combo_neutral_display(self.mount_combo, "mount", blank)
        self._set_tag_combo_neutral_display(self.stain_combo, "stain", blank)
        self._set_tag_combo_neutral_display(self.sample_combo, "sample", blank)

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

    def _field_tag_value(self, category: str) -> str:
        return self._FIELD_TAG_DEFAULTS[category]

    def _set_field_tag_defaults_in_form(self) -> None:
        combos = (
            (self.contrast_combo, "contrast"),
            (self.mount_combo, "mount"),
            (self.stain_combo, "stain"),
            (self.sample_combo, "sample"),
        )
        for combo, _category in combos:
            combo.blockSignals(True)
        try:
            self._set_combo_tag_value(self.contrast_combo, "contrast", self._field_tag_value("contrast"))
            self._set_combo_tag_value(self.mount_combo, "mount", self._field_tag_value("mount"))
            self._set_combo_tag_value(self.stain_combo, "stain", self._field_tag_value("stain"))
            self._set_combo_tag_value(self.sample_combo, "sample", self._field_tag_value("sample"))
        finally:
            for combo, _category in combos:
                combo.blockSignals(False)

    def _build_left_panel(self) -> QWidget:
        container = QWidget()
        outer = QVBoxLayout(container)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(8)

        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        self.image_type_group = QButtonGroup(self)
        self.image_type_group.setExclusive(True)
        segmented_control_height = 52
        image_type_pill = QFrame()
        image_type_pill.setObjectName("segmentedControl")
        image_type_pill.setFixedHeight(segmented_control_height)
        image_type_pill.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        image_type_layout = QHBoxLayout(image_type_pill)
        image_type_layout.setContentsMargins(4, 4, 4, 4)
        image_type_layout.setSpacing(4)

        self.field_radio = QPushButton(self.tr("Field Image (F)"))
        self.field_radio.setObjectName("segmentedButton")
        self.field_radio.setCheckable(True)
        self.field_radio.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.micro_radio = QPushButton(self.tr("Microscope Image (M)"))
        self.micro_radio.setObjectName("segmentedButton")
        self.micro_radio.setCheckable(True)
        self.micro_radio.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.image_type_group.addButton(self.field_radio)
        self.image_type_group.addButton(self.micro_radio)
        self.field_radio.setChecked(True)
        self.image_type_group.buttonClicked.connect(self._on_settings_changed)
        image_type_layout.addWidget(self.field_radio)
        image_type_layout.addWidget(self.micro_radio)
        layout.addWidget(image_type_pill)

        add_btn = QPushButton(self.tr("Add Images..."))
        add_btn.setFixedHeight(segmented_control_height)
        add_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        add_btn.clicked.connect(self._on_add_images_clicked)
        add_btn_row = QWidget()
        add_btn_grid = QGridLayout(add_btn_row)
        add_btn_grid.setContentsMargins(0, 0, 0, 0)
        add_btn_grid.setHorizontalSpacing(0)
        add_btn_grid.setVerticalSpacing(0)
        add_btn_grid.addWidget(add_btn, 0, 0)
        add_btn_grid.addWidget(QWidget(), 0, 1)
        add_btn_grid.setColumnStretch(0, 1)
        add_btn_grid.setColumnStretch(1, 1)
        outer.addWidget(add_btn_row)

        self.scale_group, scale_layout = create_section_card(self.tr("Scale"))
        self.objective_combo = QComboBox()
        self._apply_combo_popup_style(self.objective_combo)
        self._populate_objectives()
        self.objective_combo.currentIndexChanged.connect(self._on_settings_changed)
        scale_layout.addWidget(self.objective_combo)
        self.calibrate_btn = QPushButton(self.tr("Set from scalebar"))
        calibrate_hint = self.tr("Select start and end on the image")
        self._register_hint_widget(self.calibrate_btn, calibrate_hint)
        self.calibrate_btn.clicked.connect(self._start_scale_bar_selection)
        scale_layout.addWidget(self.calibrate_btn)
        # Inline controls mirror Measure tab behavior and remain available
        # whenever objective calibration is not selected.
        self._scale_bar_inline = QWidget()
        self._scale_bar_inline.setVisible(True)
        _inline_layout = QVBoxLayout(self._scale_bar_inline)
        _inline_layout.setContentsMargins(0, 4, 0, 0)
        _inline_layout.setSpacing(4)
        _length_row = QHBoxLayout()
        self.scale_bar_length_input = QDoubleSpinBox()
        self.scale_bar_length_input.setRange(0.1, 100000.0)
        self.scale_bar_length_input.setDecimals(1)
        self.scale_bar_length_input.setSuffix(" µm")
        self.scale_bar_length_input.setValue(10.0)
        self.scale_bar_length_input.editingFinished.connect(self._apply_scale_bar_inline)
        _length_row.addWidget(self.scale_bar_length_input)
        self.scale_bar_horizontal_checkbox = QCheckBox(self.tr("Horizontal"))
        self.scale_bar_horizontal_checkbox.toggled.connect(self._on_scale_bar_horizontal_toggled)
        _length_row.addWidget(self.scale_bar_horizontal_checkbox)
        _inline_layout.addLayout(_length_row)
        scale_layout.addWidget(self._scale_bar_inline)
        self.scale_warning_label = QLabel("")
        self.scale_warning_label.setWordWrap(True)
        self.scale_warning_label.setStyleSheet(f"color: #e74c3c; font-weight: bold; font-size: {pt(9)}pt;")
        self.scale_warning_label.setVisible(False)
        scale_layout.addWidget(self.scale_warning_label)
        layout.addWidget(self.scale_group)

        # ── Microscope details group (disabled for field images) ───────────
        self.micro_settings_group, micro_form = create_section_card(
            self.tr("Microscope"),
            QFormLayout,
            body_margins=(8, 8, 8, 8),
        )
        micro_form.setSpacing(6)
        micro_form.setLabelAlignment(Qt.AlignLeft)
        micro_form.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        self.contrast_combo = QComboBox()
        self._apply_combo_popup_style(self.contrast_combo)
        self._populate_tag_combo(self.contrast_combo, "contrast", self.contrast_options)
        self._set_combo_tag_value(self.contrast_combo, "contrast", self.contrast_default)
        self.contrast_combo.currentIndexChanged.connect(self._on_settings_changed)
        micro_form.addRow(self.tr("Contrast:"), self.contrast_combo)

        self.mount_combo = QComboBox()
        self._apply_combo_popup_style(self.mount_combo)
        self._populate_tag_combo(self.mount_combo, "mount", self.mount_options)
        self._set_combo_tag_value(self.mount_combo, "mount", self.mount_default)
        self.mount_combo.currentIndexChanged.connect(self._on_settings_changed)
        micro_form.addRow(self.tr("Mount:"), self.mount_combo)

        self.stain_combo = QComboBox()
        self._apply_combo_popup_style(self.stain_combo)
        self._populate_tag_combo(self.stain_combo, "stain", self.stain_options)
        self._set_combo_tag_value(self.stain_combo, "stain", self.stain_default)
        self.stain_combo.currentIndexChanged.connect(self._on_settings_changed)
        micro_form.addRow(self.tr("Stain:"), self.stain_combo)

        self.sample_combo = QComboBox()
        self._apply_combo_popup_style(self.sample_combo)
        self._populate_tag_combo(self.sample_combo, "sample", self.sample_options)
        self._set_combo_tag_value(self.sample_combo, "sample", self.sample_default)
        self.sample_combo.currentIndexChanged.connect(self._on_settings_changed)
        micro_form.addRow(self.tr("Sample type:"), self.sample_combo)

        layout.addWidget(self.micro_settings_group)

        notes_group, notes_layout = create_section_card(
            self.tr("Image note"),
            body_margins=(8, 8, 8, 8),
            body_spacing=6,
        )
        self.image_note_input = QPlainTextEdit()
        self.image_note_input.setObjectName("imageNoteInput")
        self.image_note_input.setPlaceholderText(self.tr("Optional note for the selected image"))
        self.image_note_input.setFixedHeight(56)
        self.image_note_input.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.image_note_input.textChanged.connect(self._on_settings_changed)
        self._register_hint_widget(
            self.image_note_input,
            self.tr("Store a per-image note alongside the selected image."),
        )
        notes_layout.addWidget(self.image_note_input)
        layout.addWidget(notes_group)

        self._set_field_tag_defaults_in_form()
        self._sync_field_tag_display(True)

        layout.addStretch()

        outer.addWidget(panel, 1)
        return container

    def _build_center_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        self.preview_stack = QStackedLayout()

        self.preview = ZoomableImageLabel()
        self.preview.setMinimumHeight(260)
        self.preview.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.preview.setAlignment(Qt.AlignCenter)
        self.preview.set_pan_without_shift(True)
        self.preview.clicked.connect(self._on_preview_clicked)
        self.preview.cropChanged.connect(self._on_crop_changed)
        self.preview.cropPreviewChanged.connect(self._on_crop_preview_changed)
        self.preview.scaleBarChanged.connect(self._on_scale_bar_endpoint_moved)
        self.preview.installEventFilter(self)

        self.rotate_preview_btn = QToolButton(self.preview)
        self.rotate_preview_btn.setIcon(QIcon(str(Path(__file__).parent.parent / "assets" / "icons" / "rotate.svg")))
        self.rotate_preview_btn.setIconSize(QSize(20, 20))
        self.rotate_preview_btn.setToolTip(self.tr("Rotate 90 deg counter-clockwise"))
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
        panel = QWidget()
        panel.setMinimumWidth(340)
        panel.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Expanding)
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 4, 0, 0)

        current_group, current_layout = create_section_card(self.tr("Current image"), QFormLayout)
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

        obs_group, obs_layout = create_section_card(self.tr("Time and GPS"), QFormLayout)

        self.datetime_input = QDateTimeEdit()
        self.datetime_input.setMinimumDateTime(self._unset_datetime)
        self.datetime_input.setSpecialValueText("--")
        self.datetime_input.setCalendarPopup(True)
        _cal = self.datetime_input.calendarWidget()
        _cal.setHorizontalHeaderFormat(_cal.HorizontalHeaderFormat.SingleLetterDayNames)
        _cal.setMinimumSize(300, 240)
        self.datetime_input.setDisplayFormat("yyyy-MM-dd HH:mm")
        self.datetime_input.dateTimeChanged.connect(self._on_metadata_changed)
        self.datetime_input.setDateTime(self._unset_datetime)
        obs_layout.addRow(self.tr("Date & time:"), self.datetime_input)

        gps_container = QWidget()
        gps_container_layout = QVBoxLayout(gps_container)
        gps_container_layout.setContentsMargins(0, 0, 0, 0)
        gps_container_layout.setSpacing(4)
        gps_label_width = max(
            QLabel(self.tr("Lat:")).sizeHint().width(),
            QLabel(self.tr("Lon:")).sizeHint().width(),
        )
        gps_input_width = 132

        gps_lat_row = QHBoxLayout()
        gps_lat_row.setContentsMargins(0, 0, 0, 0)
        gps_lat_row.setSpacing(6)
        gps_lat_label = QLabel(self.tr("Lat:"))
        gps_lat_label.setFixedWidth(gps_label_width)
        self.lat_input = QDoubleSpinBox()
        self.lat_input.setRange(-90.0, 90.0)
        self.lat_input.setDecimals(6)
        self.lat_input.setAlignment(Qt.AlignLeft)
        self.lat_input.setSpecialValueText("--")
        self.lat_input.setFixedWidth(gps_input_width)
        self.lat_input.setValue(self.lat_input.minimum())
        self.lat_input.valueChanged.connect(self._on_metadata_changed)
        gps_lat_row.addWidget(gps_lat_label)
        gps_lat_row.addWidget(self.lat_input, 0)
        gps_lat_row.addStretch(1)
        gps_container_layout.addLayout(gps_lat_row)

        gps_lon_row = QHBoxLayout()
        gps_lon_row.setContentsMargins(0, 0, 0, 0)
        gps_lon_row.setSpacing(6)
        gps_lon_label = QLabel(self.tr("Lon:"))
        gps_lon_label.setFixedWidth(gps_label_width)
        self.lon_input = QDoubleSpinBox()
        self.lon_input.setRange(-180.0, 180.0)
        self.lon_input.setDecimals(6)
        self.lon_input.setAlignment(Qt.AlignLeft)
        self.lon_input.setSpecialValueText("--")
        self.lon_input.setFixedWidth(gps_input_width)
        self.lon_input.setValue(self.lon_input.minimum())
        self.lon_input.valueChanged.connect(self._on_metadata_changed)
        gps_lon_row.addWidget(gps_lon_label)
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

        resize_group, resize_layout = create_section_card(
            self.tr("Resize and crop"),
            body_margins=(6, 6, 6, 6),
        )
        self.resize_group = resize_group
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

        crop_button_row = QHBoxLayout()
        crop_button_row.setContentsMargins(0, 0, 0, 0)
        crop_button_row.setSpacing(6)
        self.image_crop_btn = QPushButton(self.tr("Crop (C)"))
        self.image_crop_btn.setObjectName("cropActionButton")
        self.image_crop_btn.setProperty("cropRole", "image")
        self.image_crop_btn.setProperty("active", False)
        self.image_crop_btn.setProperty("hasUndo", False)
        image_crop_hint = self.tr(
            "Draw an image crop area. Keyboard shortcut C."
        )
        self._register_hint_widget(self.image_crop_btn, image_crop_hint)
        self.image_crop_btn.clicked.connect(self._on_image_crop_clicked)
        self.image_crop_btn.setEnabled(False)
        crop_button_row.addWidget(self.image_crop_btn)

        self.ai_crop_btn = QPushButton(self.tr("AI crop"))
        self.ai_crop_btn.setObjectName("cropActionButton")
        self.ai_crop_btn.setProperty("cropRole", "ai")
        self.ai_crop_btn.setProperty("active", False)
        self.ai_crop_btn.setProperty("hasUndo", False)
        crop_hint = self.tr(
            "Draw a crop area for Artsorakelet."
        )
        self._register_hint_widget(self.ai_crop_btn, crop_hint)
        self.ai_crop_btn.clicked.connect(self._on_ai_crop_clicked)
        self.ai_crop_btn.setEnabled(False)
        crop_button_row.addWidget(self.ai_crop_btn)
        resize_layout.addLayout(crop_button_row)
        self._set_crop_active_mode(None)

        layout.addWidget(resize_group)
        layout.addStretch(1)

        return panel

    def _populate_objectives(self, selected_key: str | None = None) -> None:
        self.objective_combo.blockSignals(True)
        self.objective_combo.clear()
        self.objective_combo.addItem(self.tr("Not set"), None)
        self.objective_combo.addItem(self.tr("From scalebar"), self.CUSTOM_OBJECTIVE_KEY)
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
        is_field = bool(hasattr(self, "field_radio") and self.field_radio.isChecked())
        dialog = ScaleBarImportDialog(self, previous_key=previous_key, is_field=is_field)
        dialog.finished.connect(lambda _result: setattr(self, "_scale_bar_dialog", None))
        self._scale_bar_dialog = dialog
        # Pre-load previous scale bar selection if one exists for this image
        result = (
            self.import_results[self.selected_index]
            if self.selected_index is not None and self.selected_index < len(self.import_results)
            else None
        )
        sel = getattr(result, "scale_bar_selection", None) if result else None
        if sel and len(sel) == 2 and getattr(self.preview, "original_pixmap", None):
            (x1, y1), (x2, y2) = sel
            dx = x2 - x1; dy = y2 - y1
            distance = (dx * dx + dy * dy) ** 0.5
            if distance > 0:
                perp_x = -dy / distance; perp_y = dx / distance
                half_w = max(6.0, distance * 0.05)
                mx = (x1 + x2) / 2; my = (y1 + y2) / 2
                p1 = QPointF(x1, y1); p2 = QPointF(x2, y2)
                p3 = QPointF(mx - perp_x * half_w, my - perp_y * half_w)
                p4 = QPointF(mx + perp_x * half_w, my + perp_y * half_w)
                dialog.set_calibration_distance(distance)
                dialog.set_calibration_preview(self.preview.original_pixmap, [p1, p2, p3, p4])
        dialog.show()

    def _start_scale_bar_selection(self) -> None:
        """Enter calibration mode directly — user clicks start/end on the preview."""
        if not getattr(self, "preview", None) or not self.preview.original_pixmap:
            return
        if self._crop_active_mode:
            self._set_crop_active_mode(None)
        if hasattr(self.preview, "ensure_full_resolution"):
            self.preview.ensure_full_resolution()
        self.calibration_dialog = None
        self.calibration_points = []
        self._calibration_mode = True
        self.preview.set_scale_bar_draggable(False)
        self.preview.clear_preview_line()
        self._sync_scale_bar_length_unit_for_image_type()
        self._update_scalebar_controls_visibility()
        self.set_hint(self.tr("Click the start point of the scale bar, then the end point"))

    def _on_scale_bar_horizontal_toggled(self, checked: bool) -> None:
        """Update preview line horizontal constraint when checkbox changes."""
        if getattr(self, "preview", None):
            self.preview.preview_line_horizontal = checked
            self.preview.update()

    def _on_scale_bar_endpoint_moved(self, line: list) -> None:
        """Update scale_bar_selection when user drags an endpoint."""
        if self.selected_index is None or self.selected_index >= len(self.import_results):
            return
        result = self.import_results[self.selected_index]
        result.scale_bar_selection = ((line[0], line[1]), (line[2], line[3]))
        dx = line[2] - line[0]; dy = line[3] - line[1]
        px_dist = (dx * dx + dy * dy) ** 0.5
        if px_dist > 0:
            self._scale_bar_pixel_distance = px_dist
        # Persist to DB
        if result.image_id:
            ImageDB.update_image(result.image_id, scale_bar_selection=result.scale_bar_selection)

    def _apply_scale_bar_inline(self) -> None:
        """Apply the scale bar using the value in the inline length input."""
        entered_length = float(self.scale_bar_length_input.value())
        is_field = bool(getattr(self, "field_radio", None) and self.field_radio.isChecked())
        total_um = entered_length * 1000.0 if is_field else entered_length
        pixel_dist = self._scale_bar_pixel_distance
        if total_um <= 0 or not pixel_dist or pixel_dist <= 0:
            return
        scale_mpp = total_um / pixel_dist  # µm per pixel
        self.apply_scale_bar(scale_mpp)
        if self.selected_index is not None and self.selected_index < len(self.import_results):
            result = self.import_results[self.selected_index]
            result.scale_bar_length_um = total_um
            # Persist scale bar selection to DB so it survives dialog close/reopen
            if result.image_id and result.scale_bar_selection:
                ImageDB.update_image(result.image_id, scale_bar_selection=result.scale_bar_selection)
            # Create / update calibration measurement (1:10 aspect ratio)
            if result.image_id and result.scale_bar_selection:
                self._upsert_calibration_measurement(result.image_id, result.scale_bar_selection, total_um, pixel_dist)
            # Re-apply overlay after scale updates, because scale application can refresh the preview widget.
            self._restore_scale_bar_overlay(result)
        self.set_hint(None)

    def _upsert_calibration_measurement(
        self,
        image_id: int,
        scale_bar_selection: tuple,
        total_um: float,
        pixel_dist: float,
    ) -> None:
        """Create or replace the calibration measurement for an image (1:10 aspect ratio)."""
        (x1, y1), (x2, y2) = scale_bar_selection
        dx = x2 - x1
        dy = y2 - y1
        length_px = (dx * dx + dy * dy) ** 0.5
        if length_px <= 0:
            return
        perp_x = -dy / length_px
        perp_y = dx / length_px
        half_width_px = pixel_dist / 20.0  # 1:10 ratio: width = length/10, half = length/20
        mx = (x1 + x2) / 2
        my = (y1 + y2) / 2
        p1 = QPointF(x1, y1)
        p2 = QPointF(x2, y2)
        p3 = QPointF(mx - perp_x * half_width_px, my - perp_y * half_width_px)
        p4 = QPointF(mx + perp_x * half_width_px, my + perp_y * half_width_px)
        width_um = total_um / 10.0

        # Delete existing calibration measurement(s) for this image
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
            self._update_scalebar_controls_visibility()
            self._update_scale_mismatch_warning()
            return
        all_micro = all(
            self.import_results[idx].image_type == "microscope"
            for idx in indices
            if 0 <= idx < len(self.import_results)
        )
        # Scale can be set for both field and microscope images.
        self.scale_group.setEnabled(True)
        # Contrast/mount/stain/sample remain microscope-only controls.
        self._update_micro_settings_state(all_micro)
        self._update_resize_group_state()
        self._update_scalebar_controls_visibility()
        self._update_scale_mismatch_warning()

    def _has_objective_calibration_selected(self) -> bool:
        selected_objective = self.objective_combo.currentData() if hasattr(self, "objective_combo") else None
        if not selected_objective:
            return False
        if selected_objective == self.CUSTOM_OBJECTIVE_KEY:
            return False
        return True

    def _update_scalebar_controls_visibility(self) -> None:
        hide_scalebar_controls = self._has_objective_calibration_selected()
        if hasattr(self, "calibrate_btn"):
            self.calibrate_btn.setVisible(not hide_scalebar_controls)
        if hasattr(self, "_scale_bar_inline"):
            self._scale_bar_inline.setVisible(not hide_scalebar_controls)

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
        if self._objective_optics_type(selected_objective) == "macro":
            self.resize_group.setEnabled(False)
            return
        any_resized = any(
            self._is_resized_image(self.import_results[idx])
            for idx in indices
            if 0 <= idx < len(self.import_results)
        )
        self.resize_group.setEnabled(not any_resized)

    def _has_pending_resize_operations(self) -> bool:
        for result in self.import_results:
            if not isinstance(result, ImageImportResult):
                continue
            if result.image_type != "microscope":
                continue
            if not bool(getattr(result, "resize_to_optimal", False)):
                continue
            if self._is_resized_image(result):
                continue
            if self._compute_resample_scale_factor(result) < 0.999:
                return True
        return False

    def _update_action_buttons_state(self) -> None:
        if not hasattr(self, "next_btn") or not hasattr(self, "cancel_btn"):
            return
        if self._continue_to_observation_details:
            self.cancel_btn.setVisible(True)
            self.next_btn.setText(self.tr("Continue"))
            return
        if self._has_pending_resize_operations():
            self.cancel_btn.setVisible(True)
            self.next_btn.setText(self.tr("Apply"))
        else:
            self.cancel_btn.setVisible(False)
            self.next_btn.setText(self.tr("Close"))

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
        if hasattr(self, "micro_settings_group"):
            self.micro_settings_group.setEnabled(enable)
        self._sync_field_tag_display(not enable and bool(getattr(self, "field_radio", None) and self.field_radio.isChecked()))

    def _sync_scale_bar_length_unit_for_image_type(self) -> None:
        if not hasattr(self, "scale_bar_length_input"):
            return
        is_field = bool(hasattr(self, "field_radio") and self.field_radio.isChecked())
        target_suffix = " mm" if is_field else " \u03bcm"
        current_suffix = (self.scale_bar_length_input.suffix() or "").strip().lower()
        current_is_mm = current_suffix == "mm"
        if current_is_mm == is_field:
            return
        current_value = float(self.scale_bar_length_input.value())
        current_um = current_value * 1000.0 if current_is_mm else current_value
        next_value = current_um / 1000.0 if is_field else current_um
        self.scale_bar_length_input.blockSignals(True)
        self.scale_bar_length_input.setSuffix(target_suffix)
        self.scale_bar_length_input.setValue(max(0.1, float(next_value)))
        self.scale_bar_length_input.blockSignals(False)

    def _update_set_from_image_button_state(self) -> None:
        if not hasattr(self, "set_from_image_btn"):
            return
        indices = self._current_selection_indices()
        if len(indices) != 1:
            self.set_from_image_btn.setEnabled(False)
            self.set_from_image_btn.setProperty("sourceActive", False)
            self.set_from_image_btn.style().unpolish(self.set_from_image_btn)
            self.set_from_image_btn.style().polish(self.set_from_image_btn)
            return
        idx = indices[0]
        if idx < 0 or idx >= len(self.import_results):
            self.set_from_image_btn.setEnabled(False)
            self.set_from_image_btn.setProperty("sourceActive", False)
            self.set_from_image_btn.style().unpolish(self.set_from_image_btn)
            self.set_from_image_btn.style().polish(self.set_from_image_btn)
            return
        if self.import_results[idx].image_type == "microscope":
            self.set_from_image_btn.setEnabled(False)
            self.set_from_image_btn.setProperty("sourceActive", False)
            self.set_from_image_btn.style().unpolish(self.set_from_image_btn)
            self.set_from_image_btn.style().polish(self.set_from_image_btn)
            return
        has_exif_data = (
            self._current_exif_datetime is not None
            or self._current_exif_lat is not None
            or self._current_exif_lon is not None
        )
        enable = bool(has_exif_data)
        self.set_from_image_btn.setEnabled(enable)
        is_source = enable and self._observation_source_index == idx
        self.set_from_image_btn.setProperty("sourceActive", is_source)
        self.set_from_image_btn.style().unpolish(self.set_from_image_btn)
        self.set_from_image_btn.style().polish(self.set_from_image_btn)

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
        if hasattr(self, "image_crop_btn"):
            index = self._current_single_index()
            enable_image_crop = index is not None and 0 <= index < len(self.import_results)
            if self._ai_thread is not None:
                enable_image_crop = False
            self.image_crop_btn.setEnabled(enable_image_crop)
            if not enable_image_crop and self._crop_active_mode == "image":
                self._set_crop_active_mode(None)
        if hasattr(self, "ai_crop_btn"):
            index = self._current_single_index()
            enable = False
            if index is not None and 0 <= index < len(self.import_results):
                image_type = (self.import_results[index].image_type or "field").strip().lower()
                enable = image_type == "field"
            if self._ai_thread is not None:
                enable = False
            self.ai_crop_btn.setEnabled(enable)
            if not enable and self._crop_active_mode == "ai":
                self._set_crop_active_mode(None)
            self._update_crop_button_styles()
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
        if self._crop_active_mode == "image":
            self._apply_crop_overlay_style("image")
            self.preview.set_crop_box(None)
            self.preview.set_overlay_boxes([])
            self._update_ai_crop_size_label()
            return
        index = self._current_single_index()
        preview_pixmap = getattr(self.preview, "original_pixmap", None)
        if index is not None and preview_pixmap:
            width = preview_pixmap.width()
            height = preview_pixmap.height()
            crop_box = self._ai_crop_boxes.get(index)
            crop_mode = self._stored_crop_mode_for_index(index)
            self._apply_crop_overlay_style(crop_mode)
            if crop_box and width > 0 and height > 0:
                self.preview.set_crop_box(
                    (crop_box[0] * width, crop_box[1] * height, crop_box[2] * width, crop_box[3] * height)
                )
            elif crop_mode == "image" and width > 0 and height > 0:
                self.preview.set_crop_box((0.0, 0.0, float(width), float(height)))
            else:
                self.preview.set_crop_box(None)
        else:
            self._apply_crop_overlay_style(None)
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

    def _crop_mode_label(self, mode: str | None) -> str:
        return self.tr("AI crop") if mode == "ai" else self.tr("Image crop")

    def _crop_overlay_label(
        self,
        mode: str | None,
        dimensions: tuple[int, int] | None = None,
    ) -> str:
        label = self._crop_mode_label(mode)
        if mode == "ai" and dimensions:
            return self.tr("{label}: {w}x{h} px").format(
                label=label,
                w=max(1, int(dimensions[0])),
                h=max(1, int(dimensions[1])),
            )
        return label

    def _stored_crop_mode_for_index(self, index: int | None) -> str | None:
        if index is None:
            return None
        mode = self._crop_mode_by_index.get(index)
        if mode in {"ai", "image"}:
            return mode
        if 0 <= index < len(self.import_results):
            mode = str(getattr(self.import_results[index], "crop_mode", "") or "").strip().lower()
            if mode in {"ai", "image"}:
                return mode
        return None

    @staticmethod
    def _normalize_crop_box(
        box: tuple[float, float, float, float] | None,
    ) -> tuple[float, float, float, float] | None:
        if not box or len(box) != 4:
            return None
        try:
            x1, y1, x2, y2 = (float(v) for v in box)
        except Exception:
            return None
        left = max(0.0, min(1.0, min(x1, x2)))
        top = max(0.0, min(1.0, min(y1, y2)))
        right = max(0.0, min(1.0, max(x1, x2)))
        bottom = max(0.0, min(1.0, max(y1, y2)))
        if right <= left or bottom <= top:
            return None
        return (left, top, right, bottom)

    def _normalize_crop_box_from_preview_pixels(
        self,
        box: tuple[float, float, float, float] | None,
    ) -> tuple[float, float, float, float] | None:
        if not box or len(box) != 4 or not hasattr(self, "preview"):
            return None
        preview_pixmap = getattr(self.preview, "original_pixmap", None)
        if preview_pixmap is None or preview_pixmap.isNull():
            return None
        width = preview_pixmap.width()
        height = preview_pixmap.height()
        if width <= 0 or height <= 0:
            return None
        try:
            x1, y1, x2, y2 = (float(v) for v in box)
        except Exception:
            return None
        return self._normalize_crop_box(
            (
                x1 / float(width),
                y1 / float(height),
                x2 / float(width),
                y2 / float(height),
            )
        )

    @classmethod
    def _translate_normalized_crop_box_for_crop(
        cls,
        box: tuple[float, float, float, float] | None,
        crop_box: tuple[float, float, float, float] | None,
    ) -> tuple[float, float, float, float] | None:
        normalized_box = cls._normalize_crop_box(box)
        normalized_crop = cls._normalize_crop_box(crop_box)
        if not normalized_box or not normalized_crop:
            return normalized_box
        crop_left, crop_top, crop_right, crop_bottom = normalized_crop
        crop_width = crop_right - crop_left
        crop_height = crop_bottom - crop_top
        if crop_width <= 0 or crop_height <= 0:
            return None
        left = (normalized_box[0] - crop_left) / crop_width
        top = (normalized_box[1] - crop_top) / crop_height
        right = (normalized_box[2] - crop_left) / crop_width
        bottom = (normalized_box[3] - crop_top) / crop_height
        translated = cls._normalize_crop_box((left, top, right, bottom))
        if not translated:
            return None
        if translated[2] - translated[0] <= 0.001 or translated[3] - translated[1] <= 0.001:
            return None
        return translated

    @staticmethod
    def _translate_scale_bar_selection_for_crop(
        selection: tuple | None,
        crop_pixels: tuple[int, int, int, int] | None,
    ) -> tuple | None:
        if not selection or len(selection) != 2 or not crop_pixels:
            return selection
        try:
            (x1, y1), (x2, y2) = selection
            crop_x1, crop_y1, crop_x2, crop_y2 = crop_pixels
            crop_w = max(1, int(crop_x2 - crop_x1))
            crop_h = max(1, int(crop_y2 - crop_y1))
            nx1 = float(x1) - float(crop_x1)
            ny1 = float(y1) - float(crop_y1)
            nx2 = float(x2) - float(crop_x1)
            ny2 = float(y2) - float(crop_y1)
        except Exception:
            return None
        points = ((nx1, ny1), (nx2, ny2))
        for px, py in points:
            if px < 0 or py < 0 or px > crop_w or py > crop_h:
                return None
        return points

    def _apply_crop_overlay_style(
        self,
        mode: str | None = None,
        dimensions: tuple[int, int] | None = None,
    ) -> None:
        if not hasattr(self, "preview"):
            return
        overlay_mode = mode or self._stored_crop_mode_for_index(self._current_single_index()) or "ai"
        if overlay_mode == "image":
            self.preview.set_crop_overlay_style(
                self._crop_overlay_label("image", dimensions),
                QColor("#d93025"),
                QColor("#b3261e"),
            )
        else:
            self.preview.set_crop_overlay_style(
                self._crop_overlay_label("ai", dimensions),
                QColor("#f39c12"),
                QColor("#d35400"),
            )

    def _set_crop_active_mode(self, mode: str | None) -> None:
        normalized = str(mode or "").strip().lower() or None
        if normalized not in {"ai", "image"}:
            normalized = None
        self._crop_active_mode = normalized
        if hasattr(self, "preview"):
            self.preview.set_crop_mode(bool(normalized))
            self.preview.set_crop_aspect_ratio(None)
        self._apply_crop_overlay_style(normalized)
        self._update_crop_button_styles()

    def _set_crop_button_state(
        self,
        button: QPushButton | None,
        *,
        enabled: bool,
        active: bool,
        role: str,
        has_undo: bool = False,
    ) -> None:
        if button is None:
            return
        button.setProperty("cropRole", role)
        button.setProperty("active", bool(active))
        button.setProperty("hasUndo", bool(has_undo))
        button.setEnabled(enabled)
        style = button.style()
        style.unpolish(button)
        style.polish(button)
        button.update()

    def _update_crop_button_styles(self) -> None:
        image_enabled = bool(hasattr(self, "image_crop_btn") and self.image_crop_btn.isEnabled())
        ai_enabled = bool(hasattr(self, "ai_crop_btn") and self.ai_crop_btn.isEnabled())
        index = self._current_single_index()
        has_image_undo = index in self._image_crop_undo_by_index if index is not None else False
        if hasattr(self, "image_crop_btn"):
            if self._crop_active_mode == "image":
                self.image_crop_btn.setText(self.tr("Cancel Crop"))
            elif has_image_undo:
                self.image_crop_btn.setText(self.tr("Undo Crop"))
            else:
                self.image_crop_btn.setText(self.tr("Crop (C)"))
        self._set_crop_button_state(
            getattr(self, "image_crop_btn", None),
            enabled=image_enabled,
            active=self._crop_active_mode == "image",
            role="image",
            has_undo=has_image_undo,
        )
        self._set_crop_button_state(
            getattr(self, "ai_crop_btn", None),
            enabled=ai_enabled,
            active=self._crop_active_mode == "ai",
            role="ai",
        )

    def _toggle_crop_mode(self, mode: str) -> None:
        index = self._current_single_index()
        if index is None:
            return
        normalized = str(mode or "").strip().lower()
        if normalized not in {"ai", "image"}:
            return
        if normalized == "image":
            if self._crop_active_mode == normalized:
                self._set_crop_active_mode(None)
                return
            self._set_crop_active_mode(normalized)
            self._set_preview_for_result(self.import_results[index], preserve_view=True)
            if hasattr(self.preview, "ensure_full_resolution"):
                self.preview.ensure_full_resolution()
            self._update_ai_overlay()
            self._update_ai_crop_size_label()
            return
        current_mode = self._stored_crop_mode_for_index(index)
        if index in self._ai_crop_boxes and current_mode == normalized:
            self._ai_crop_boxes.pop(index, None)
            self._crop_mode_by_index.pop(index, None)
            if hasattr(self, "preview"):
                self.preview.set_crop_box(None)
            if 0 <= index < len(self.import_results):
                self.import_results[index].ai_crop_box = None
                self.import_results[index].ai_crop_source_size = None
                self.import_results[index].crop_mode = None
                if self.import_results[index].image_id:
                    ImageDB.update_image(
                        self.import_results[index].image_id,
                        ai_crop_box=None,
                        ai_crop_source_size=None,
                        crop_mode=None,
                    )
            self._set_crop_active_mode(normalized)
            self._update_ai_controls_state()
            return
        if self._crop_active_mode == normalized:
            self._set_crop_active_mode(None)
            return
        self._set_crop_active_mode(normalized)

    def _on_image_crop_clicked(self) -> None:
        index = self._current_single_index()
        if (
            index is not None
            and self._crop_active_mode != "image"
            and index in self._image_crop_undo_by_index
        ):
            self._undo_image_crop(index)
            return
        self._toggle_crop_mode("image")

    def _on_ai_crop_clicked(self) -> None:
        self._toggle_crop_mode("ai")

    def _discard_image_crop_undo_state(self, index: int) -> None:
        state = self._image_crop_undo_by_index.pop(index, None)
        if state is None:
            return
        backup_path = state.backup_path
        self._temp_preview_paths.discard(backup_path)
        try:
            Path(backup_path).unlink(missing_ok=True)
        except Exception:
            pass

    def _create_image_crop_undo_state(self, index: int, source_path: str) -> bool:
        if index < 0 or index >= len(self.import_results):
            return False
        source = Path(source_path)
        if not source.exists():
            return False
        self._discard_image_crop_undo_state(index)
        backup = source.with_name(
            f"{source.stem}_before_crop_{int(time.time() * 1000)}{source.suffix}"
        )
        counter = 1
        while backup.exists():
            backup = source.with_name(
                f"{source.stem}_before_crop_{int(time.time() * 1000)}_{counter}{source.suffix}"
            )
            counter += 1
        try:
            shutil.copy2(source, backup)
        except Exception:
            return False
        result = self.import_results[index]
        self._image_crop_undo_by_index[index] = ImageCropUndoState(
            backup_path=str(backup),
            filepath=str(result.filepath or source_path),
            preview_path=result.preview_path,
            image_path=self.image_paths[index] if 0 <= index < len(self.image_paths) else str(result.filepath or source_path),
            ai_crop_box=result.ai_crop_box,
            ai_crop_source_size=result.ai_crop_source_size,
            crop_mode=result.crop_mode,
            pending_image_crop_offset=result.pending_image_crop_offset,
            scale_bar_selection=result.scale_bar_selection,
            had_ai_crop_entry=index in self._ai_crop_boxes,
            ai_crop_entry=self._ai_crop_boxes.get(index),
            had_crop_mode_entry=index in self._crop_mode_by_index,
            crop_mode_entry=self._crop_mode_by_index.get(index),
        )
        self._temp_preview_paths.add(str(backup))
        return True

    def _undo_image_crop(self, index: int) -> None:
        state = self._image_crop_undo_by_index.get(index)
        if state is None or index < 0 or index >= len(self.import_results):
            return
        backup = Path(state.backup_path)
        target = Path(state.filepath)
        if not backup.exists():
            self._discard_image_crop_undo_state(index)
            self._update_crop_button_styles()
            return
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(backup, target)
        except Exception:
            self._set_settings_hint(self.tr("Could not undo crop"), "#e74c3c")
            return
        result = self.import_results[index]
        old_filepath = result.filepath
        old_preview = result.preview_path
        result.filepath = state.filepath
        result.preview_path = state.preview_path
        result.ai_crop_box = state.ai_crop_box
        result.ai_crop_source_size = state.ai_crop_source_size
        result.crop_mode = state.crop_mode
        result.pending_image_crop_offset = state.pending_image_crop_offset
        result.scale_bar_selection = state.scale_bar_selection
        if 0 <= index < len(self.image_paths):
            self.image_paths[index] = state.image_path
        if state.had_ai_crop_entry:
            self._ai_crop_boxes[index] = state.ai_crop_entry
        else:
            self._ai_crop_boxes.pop(index, None)
        if state.had_crop_mode_entry:
            self._crop_mode_by_index[index] = state.crop_mode_entry
        else:
            self._crop_mode_by_index.pop(index, None)
        for path in {old_filepath, old_preview, state.filepath, state.preview_path, state.backup_path}:
            self._invalidate_cached_pixmap(path)
        self._discard_image_crop_undo_state(index)
        if self._crop_active_mode:
            self._set_crop_active_mode(None)
        self._refresh_gallery()
        self._select_image(index)
        self._set_settings_hint(self.tr("Image crop undone"), "#27ae60")

    def _on_crop_changed(self, box: tuple[float, float, float, float] | None) -> None:
        index = self._current_single_index()
        if index is None:
            return
        mode = self._crop_active_mode or self._stored_crop_mode_for_index(index) or "ai"
        if mode == "image":
            result = self.import_results[index] if 0 <= index < len(self.import_results) else None
            normalized_box = self._normalize_crop_box_from_preview_pixels(box)
            source_path = (
                self._prepare_working_copy_path(index, copy_existing=True)
                if normalized_box
                else None
            )
            undo_ready = (
                self._create_image_crop_undo_state(index, source_path)
                if source_path and normalized_box
                else False
            )
            if (
                result is None
                or not source_path
                or not normalized_box
                or not undo_ready
                or not self._crop_image_file_in_place(index, source_path, normalized_box)
            ):
                self._discard_image_crop_undo_state(index)
                if self._crop_active_mode:
                    self._set_crop_active_mode(None)
                self._update_ai_controls_state()
                self._update_ai_crop_size_label()
                return
            if result is not None:
                if getattr(result, "ai_crop_box", None):
                    self._crop_mode_by_index[index] = "ai"
                    result.crop_mode = "ai"
                else:
                    self._crop_mode_by_index[index] = "image"
                    result.crop_mode = "image"
            if self._crop_active_mode:
                self._set_crop_active_mode(None)
            self._refresh_gallery()
            self._select_image(index)
            self._set_settings_hint(self.tr("Image crop applied"), "#27ae60")
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
                self._crop_mode_by_index[index] = mode
                if 0 <= index < len(self.import_results):
                    self.import_results[index].ai_crop_box = norm_box
                    self.import_results[index].ai_crop_source_size = (width, height)
                    self.import_results[index].crop_mode = mode
                    if self.import_results[index].image_id:
                        ImageDB.update_image(
                            self.import_results[index].image_id,
                            ai_crop_box=norm_box,
                            ai_crop_source_size=(width, height),
                            crop_mode=mode,
                        )
            else:
                self._ai_crop_boxes.pop(index, None)
                self._crop_mode_by_index.pop(index, None)
                if 0 <= index < len(self.import_results):
                    self.import_results[index].ai_crop_box = None
                    self.import_results[index].ai_crop_source_size = None
                    self.import_results[index].crop_mode = None
                    if self.import_results[index].image_id:
                        ImageDB.update_image(
                            self.import_results[index].image_id,
                            ai_crop_box=None,
                            ai_crop_source_size=None,
                            crop_mode=None,
                        )
        else:
            self._ai_crop_boxes.pop(index, None)
            self._crop_mode_by_index.pop(index, None)
            if 0 <= index < len(self.import_results):
                self.import_results[index].ai_crop_box = None
                self.import_results[index].ai_crop_source_size = None
                self.import_results[index].crop_mode = None
                if self.import_results[index].image_id:
                    ImageDB.update_image(
                        self.import_results[index].image_id,
                        ai_crop_box=None,
                        ai_crop_source_size=None,
                        crop_mode=None,
                    )
        if self._crop_active_mode:
            self._set_crop_active_mode(None)
        self._update_ai_controls_state()
        self._update_ai_crop_size_label()

    def _on_crop_preview_changed(self, box: tuple[float, float, float, float] | None) -> None:
        index = self._current_single_index()
        mode = self._crop_active_mode or self._stored_crop_mode_for_index(index) or "ai"
        if (
            box
            and isinstance(box, (tuple, list))
            and len(box) == 4
        ):
            try:
                x1, y1, x2, y2 = (float(v) for v in box)
                crop_w = abs(x2 - x1)
                crop_h = abs(y2 - y1)
                preview_pixmap = getattr(self.preview, "original_pixmap", None) if hasattr(self, "preview") else None
                source_size = self._source_image_size_for_index(index, prefer_ai_size=(mode == "ai"))
                if (
                    preview_pixmap is not None
                    and source_size
                    and len(source_size) == 2
                    and preview_pixmap.width() > 0
                    and preview_pixmap.height() > 0
                ):
                    crop_w *= float(source_size[0]) / float(preview_pixmap.width())
                    crop_h *= float(source_size[1]) / float(preview_pixmap.height())
                crop_w = max(1, int(round(crop_w)))
                crop_h = max(1, int(round(crop_h)))
                self._apply_crop_overlay_style(mode, (crop_w, crop_h))
                return
            except Exception:
                pass
        self._update_ai_crop_size_label()

    def _update_ai_crop_size_label(self) -> None:
        if not hasattr(self, "preview"):
            return
        index = self._current_single_index()
        dimensions: tuple[int, int] | None = None
        crop_mode = self._stored_crop_mode_for_index(index) or self._crop_active_mode or "ai"
        if index is not None:
            crop_box = self._ai_crop_boxes.get(index)
            crop_mode = self._stored_crop_mode_for_index(index) or self._crop_active_mode or "ai"
            if 0 <= index < len(self.import_results):
                result = self.import_results[index]
                if crop_box is None:
                    crop_box = getattr(result, "ai_crop_box", None)
            source_size = self._source_image_size_for_index(index, prefer_ai_size=(crop_mode == "ai"))
            if crop_mode == "image" and not crop_box and source_size and len(source_size) == 2:
                try:
                    dimensions = (max(1, int(source_size[0])), max(1, int(source_size[1])))
                except Exception:
                    dimensions = None
            if (
                dimensions is None
                and
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
                    dimensions = (crop_w, crop_h)
                except Exception:
                    dimensions = None
        self._apply_crop_overlay_style(crop_mode, dimensions)

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

    def _park_thread_until_finished(self, thread: QThread | None) -> None:
        """Reparent a still-running worker so dialog close won't destroy it."""
        if thread is None:
            return
        app = QApplication.instance()
        if app is None:
            try:
                if thread.parent() is self:
                    thread.setParent(None)
            except Exception:
                pass
            return
        try:
            if thread.parent() is self or thread.parent() is None:
                thread.setParent(app)
        except Exception:
            pass
        parked = getattr(app, "_sporely_parked_threads", None)
        if parked is None:
            parked = set()
            setattr(app, "_sporely_parked_threads", parked)
        try:
            parked.add(thread)
        except Exception:
            pass

        def _release_thread(t=thread, a=app):
            try:
                parked_threads = getattr(a, "_sporely_parked_threads", None)
                if parked_threads is not None:
                    parked_threads.discard(t)
            except Exception:
                pass

        try:
            thread.finished.connect(thread.deleteLater)
        except Exception:
            pass
        try:
            thread.finished.connect(_release_thread)
        except Exception:
            pass

    def _cleanup_dialog_threads(self) -> None:
        if getattr(self, "_close_cleanup_done", False):
            return
        self._close_cleanup_done = True
        if self._ai_thread is not None:
            try:
                self._ai_thread.requestInterruption()
            except Exception:
                pass
            try:
                self._ai_thread.wait(1000)
                if self._ai_thread.isRunning():
                    self._park_thread_until_finished(self._ai_thread)
            except Exception:
                pass

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

    def _set_hint_progress_visible(self, visible: bool) -> None:
        if hasattr(self, "hint_bar"):
            self.hint_bar.setVisible(not visible)
        if hasattr(self, "hint_progress_widget"):
            self.hint_progress_widget.setVisible(bool(visible))

    def _set_hint_progress(self, status_text: str | None, value: int | None = None) -> None:
        if hasattr(self, "hint_progress_status"):
            self.hint_progress_status.setText((status_text or "").strip())
        if value is not None and hasattr(self, "hint_progress_bar"):
            self.hint_progress_bar.setValue(int(max(0, min(100, value))))

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
    def _rotate_normalized_crop_box_counterclockwise(
        box: tuple[float, float, float, float],
    ) -> tuple[float, float, float, float]:
        x1, y1, x2, y2 = box
        corners = [
            (x1, y1),
            (x2, y1),
            (x2, y2),
            (x1, y2),
        ]
        rotated = [(y, 1.0 - x) for x, y in corners]
        xs = [p[0] for p in rotated]
        ys = [p[1] for p in rotated]
        return (
            max(0.0, min(1.0, min(xs))),
            max(0.0, min(1.0, min(ys))),
            max(0.0, min(1.0, max(xs))),
            max(0.0, min(1.0, max(ys))),
        )

    def _load_pixmap_with_orientation(self, path: str) -> QPixmap:
        """Load preview pixmap honoring EXIF orientation."""
        reader = QImageReader(path)
        reader.setAutoTransform(True)
        image = reader.read()
        if image.isNull():
            return QPixmap(path)
        return QPixmap.fromImage(image)

    def _invalidate_cached_pixmap(self, path: str | None) -> None:
        if not path:
            return
        self._pixmap_cache.pop(path, None)
        self._pixmap_cache_is_preview.pop(path, None)
        self._image_size_cache.pop(path, None)

    def _source_image_size_for_index(
        self,
        index: int | None,
        *,
        prefer_ai_size: bool = False,
    ) -> tuple[int, int] | None:
        if index is None or index < 0 or index >= len(self.import_results):
            preview_pixmap = getattr(self.preview, "original_pixmap", None) if hasattr(self, "preview") else None
            if preview_pixmap is not None and not preview_pixmap.isNull():
                return (preview_pixmap.width(), preview_pixmap.height())
            return None
        result = self.import_results[index]
        if prefer_ai_size:
            ai_size = getattr(result, "ai_crop_source_size", None)
            if ai_size and len(ai_size) == 2:
                try:
                    return (max(1, int(ai_size[0])), max(1, int(ai_size[1])))
                except (TypeError, ValueError):
                    pass
        for candidate in (
            result.filepath,
            result.preview_path,
            result.original_filepath,
        ):
            size = self._get_image_size(candidate)
            if size:
                return size
        preview_pixmap = getattr(self.preview, "original_pixmap", None) if hasattr(self, "preview") else None
        if preview_pixmap is not None and not preview_pixmap.isNull():
            return (preview_pixmap.width(), preview_pixmap.height())
        return None

    def _prepare_working_copy_path(self, index: int, *, copy_existing: bool = False) -> str | None:
        if index < 0 or index >= len(self.import_results):
            return None
        result = self.import_results[index]
        source = (result.filepath or "").strip()
        if not source:
            return None
        source_path = Path(source)
        if not source_path.exists():
            return None
        if result.image_id and not copy_existing:
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

    def _prepare_rotate_source_path(self, index: int) -> str | None:
        return self._prepare_working_copy_path(index, copy_existing=False)

    def _crop_image_file_in_place(
        self,
        index: int,
        path: str,
        crop_box: tuple[float, float, float, float],
    ) -> bool:
        from PIL import Image, ImageOps

        if index < 0 or index >= len(self.import_results):
            return False
        result = self.import_results[index]
        source = Path(path)
        if not source.exists():
            return False
        normalized = self._normalize_crop_box(crop_box)
        if not normalized:
            return False
        tmp = source.with_name(f"{source.stem}_cropping{source.suffix}")
        crop_pixels: tuple[int, int, int, int] | None = None
        new_size: tuple[int, int] | None = None
        try:
            with Image.open(source) as img:
                normalized_img = ImageOps.exif_transpose(img)
                width, height = normalized_img.size
                crop_x1 = int(max(0, min(width, round(normalized[0] * width))))
                crop_y1 = int(max(0, min(height, round(normalized[1] * height))))
                crop_x2 = int(max(0, min(width, round(normalized[2] * width))))
                crop_y2 = int(max(0, min(height, round(normalized[3] * height))))
                if crop_x2 <= crop_x1:
                    crop_x2 = min(width, crop_x1 + 1)
                    crop_x1 = max(0, crop_x2 - 1)
                if crop_y2 <= crop_y1:
                    crop_y2 = min(height, crop_y1 + 1)
                    crop_y1 = max(0, crop_y2 - 1)
                cropped = normalized_img.crop((crop_x1, crop_y1, crop_x2, crop_y2))
                exif = normalized_img.getexif()
                if exif is not None:
                    exif[274] = 1
                fmt = img.format
                save_kwargs = {}
                if fmt and fmt.upper() in {"JPEG", "JPG"} and cropped.mode not in {"RGB", "L"}:
                    cropped = cropped.convert("RGB")
                if fmt and fmt.upper() in {"JPEG", "JPG"}:
                    save_kwargs["quality"] = 95
                if exif is not None and len(exif) > 0:
                    save_kwargs["exif"] = exif.tobytes()
                try:
                    cropped.save(tmp, format=fmt, **save_kwargs)
                except Exception:
                    save_kwargs.pop("exif", None)
                    cropped.save(tmp, format=fmt, **save_kwargs)
                crop_pixels = (crop_x1, crop_y1, crop_x2, crop_y2)
                new_size = (crop_x2 - crop_x1, crop_y2 - crop_y1)
            tmp.replace(source)
        except Exception:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
            return False

        old_filepath = result.filepath
        old_preview = result.preview_path
        result.filepath = path
        result.preview_path = path
        self.image_paths[index] = path
        self._invalidate_cached_pixmap(old_filepath)
        if old_preview and old_preview != old_filepath:
            self._invalidate_cached_pixmap(old_preview)
        self._invalidate_cached_pixmap(path)
        if new_size:
            translated_ai_box = self._translate_normalized_crop_box_for_crop(
                result.ai_crop_box,
                normalized,
            )
            if translated_ai_box:
                result.ai_crop_box = translated_ai_box
                result.ai_crop_source_size = new_size
                self._ai_crop_boxes[index] = translated_ai_box
                self._crop_mode_by_index[index] = "ai"
                result.crop_mode = "ai"
            else:
                result.ai_crop_box = None
                result.ai_crop_source_size = None
                self._ai_crop_boxes.pop(index, None)
                self._crop_mode_by_index.pop(index, None)
                result.crop_mode = None
            result.scale_bar_selection = self._translate_scale_bar_selection_for_crop(
                getattr(result, "scale_bar_selection", None),
                crop_pixels,
            )
        if crop_pixels:
            offset_x, offset_y = crop_pixels[0], crop_pixels[1]
            pending_offset = getattr(result, "pending_image_crop_offset", None) or (0, 0)
            result.pending_image_crop_offset = (
                int(pending_offset[0]) + int(offset_x),
                int(pending_offset[1]) + int(offset_y),
            )
        return True

    def _rotate_image_file_counterclockwise(self, path: str) -> bool:
        from PIL import Image, ImageOps

        source = Path(path)
        if not source.exists():
            return False
        tmp = source.with_name(f"{source.stem}_rotating{source.suffix}")
        try:
            with Image.open(source) as img:
                normalized = ImageOps.exif_transpose(img)
                rotated = normalized.rotate(90, expand=True)
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
        if not self._rotate_image_file_counterclockwise(rotate_path):
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
            rotated_box = self._rotate_normalized_crop_box_counterclockwise(result.ai_crop_box)
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
                        crop_mode=getattr(result, "crop_mode", None),
                    )
                except Exception:
                    pass
        self._refresh_gallery()
        self._select_image(index)
        self._set_settings_hint(self.tr("Image rotated 90 deg counter-clockwise"), "#27ae60")

    def _update_settings_hint_for_indices(self, indices: list[int], action: str | None = None) -> None:
        if not indices:
            return
        action_map = {
            "scale": (self.tr("Scale applied"), "to"),
            "contrast": (self.tr("Contrast changed"), "for"),
            "mount": (self.tr("Mount changed"), "for"),
            "stain": (self.tr("Stain changed"), "for"),
            "sample": (self.tr("Sample type changed"), "for"),
            "notes": (self.tr("Image note changed"), "for"),
            "image_type": (self.tr("Image type changed"), "for"),
            "resize": (self.tr("Resize setting changed"), "for"),
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
        update_app_settings({"last_import_dir": str(Path(filepath).parent)})

    def _on_add_images_clicked(self) -> None:
        paths, _ = QFileDialog.getOpenFileNames(
            self,
            self.tr("Select Images"),
            self._get_default_import_dir(),
            self.tr("Images (*.png *.jpg *.jpeg *.tif *.tiff *.heic *.heif);;All Files (*)"),
        )
        if paths:
            self._remember_import_dir(paths[0])
            self.add_images(paths)

    def add_images(self, paths: list[str]) -> None:
        start_time = time.perf_counter()
        import_dir = get_images_dir() / "imports"
        import_dir.mkdir(parents=True, exist_ok=True)
        first_new_index = len(self.import_results)
        show_progress = len(paths) > 1
        total_paths = len(paths)
        if show_progress:
            self._set_hint_progress_visible(True)
            self._set_hint_progress(
                self.tr("Loading images... ({current}/{total})").format(current=0, total=total_paths),
                0,
            )
            QCoreApplication.processEvents()
        processed = 0
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
                contrast=self._field_tag_value("contrast"),
                mount_medium=self._field_tag_value("mount"),
                stain=self._field_tag_value("stain"),
                sample_type=self._field_tag_value("sample"),
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
            if show_progress:
                processed += 1
                progress_value = int(round((processed / total_paths) * 100)) if total_paths > 0 else 100
                self._set_hint_progress(
                    self.tr("Loading images... ({current}/{total})").format(
                        current=processed,
                        total=total_paths,
                    ),
                    progress_value,
                )
                QCoreApplication.processEvents()
        if show_progress:
            self._set_hint_progress_visible(False)
            self._set_hint_progress("", 0)
        self._update_summary()
        self._seed_observation_metadata()
        self._update_observation_source_index()
        self._sync_observation_metadata_inputs()
        self._refresh_gallery()
        self._update_scale_group_state()
        self._update_set_from_image_button_state()
        self._update_ai_controls_state()
        added_count = max(0, len(self.import_results) - first_new_index)
        added_names = [Path(p).name for p in (paths or []) if p]
        _debug_import_flow(
            f"add_images finished in {time.perf_counter() - start_time:.3f}s; "
            f"added={added_count}; total={len(self.import_results)}; "
            f"files={added_names}"
        )
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
        self._crop_mode_by_index = {}
        for result in results:
            if not result:
                continue
            if not result.preview_path:
                result.preview_path = result.filepath
            result.gps_source = bool(getattr(result, "gps_source", False))
            if result.original_filepath is None:
                result.original_filepath = result.filepath
            if not hasattr(result, "pending_image_crop_offset"):
                result.pending_image_crop_offset = None
            if not hasattr(result, "notes"):
                result.notes = None
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
            crop_mode = str(getattr(result, "crop_mode", "") or "").strip().lower()
            if result.ai_crop_box and crop_mode != "image":
                self._ai_crop_boxes[len(self.import_results)] = result.ai_crop_box
            if crop_mode in {"ai", "image"}:
                self._crop_mode_by_index[len(self.import_results)] = crop_mode
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

    @staticmethod
    def _image_result_key(result: ImageImportResult):
        if result.image_id is not None:
            return result.image_id
        return str(result.filepath or "")

    def _remap_index_dict_after_reorder(
        self,
        source: dict[int, object],
        old_results: list[ImageImportResult],
        new_index_by_key: dict[object, int],
    ) -> dict[int, object]:
        remapped: dict[int, object] = {}
        for old_index, value in source.items():
            if old_index < 0 or old_index >= len(old_results):
                continue
            key = self._image_result_key(old_results[old_index])
            new_index = new_index_by_key.get(key)
            if new_index is None:
                continue
            remapped[int(new_index)] = value
        return remapped

    def _on_gallery_items_reordered(self, ordered_keys: list[object]) -> None:
        old_results = list(self.import_results)
        if len(old_results) < 2:
            return

        ordered_results: list[ImageImportResult] = []
        seen: set[object] = set()
        for key in ordered_keys or []:
            for result in old_results:
                result_key = self._image_result_key(result)
                if result_key != key or result_key in seen:
                    continue
                ordered_results.append(result)
                seen.add(result_key)
                break
        for result in old_results:
            result_key = self._image_result_key(result)
            if result_key in seen:
                continue
            ordered_results.append(result)
            seen.add(result_key)
        if ordered_results == old_results:
            return

        selected_keys = []
        for index in self.selected_indices:
            if 0 <= index < len(old_results):
                selected_keys.append(self._image_result_key(old_results[index]))
        current_key = None
        if self.selected_index is not None and 0 <= self.selected_index < len(old_results):
            current_key = self._image_result_key(old_results[self.selected_index])
        primary_key = None
        if self.primary_index is not None and 0 <= self.primary_index < len(old_results):
            primary_key = self._image_result_key(old_results[self.primary_index])

        self.import_results[:] = ordered_results
        self.image_paths = [result.filepath for result in self.import_results]
        new_index_by_key = {
            self._image_result_key(result): idx
            for idx, result in enumerate(self.import_results)
        }
        self._ai_predictions_by_index = self._remap_index_dict_after_reorder(
            self._ai_predictions_by_index,
            old_results,
            new_index_by_key,
        )
        self._ai_selected_by_index = self._remap_index_dict_after_reorder(
            self._ai_selected_by_index,
            old_results,
            new_index_by_key,
        )
        self._ai_crop_boxes = self._remap_index_dict_after_reorder(
            self._ai_crop_boxes,
            old_results,
            new_index_by_key,
        )
        self._crop_mode_by_index = self._remap_index_dict_after_reorder(
            self._crop_mode_by_index,
            old_results,
            new_index_by_key,
        )
        self._image_crop_undo_by_index = self._remap_index_dict_after_reorder(
            self._image_crop_undo_by_index,
            old_results,
            new_index_by_key,
        )
        self._ai_selected_taxon = None

        self.selected_indices = sorted(
            new_index_by_key[key] for key in selected_keys if key in new_index_by_key
        )
        self.selected_index = new_index_by_key.get(current_key)
        self.primary_index = new_index_by_key.get(primary_key)
        self._update_observation_source_index()
        self._refresh_gallery()

        if len(self.selected_indices) > 1:
            selected_paths = [
                self.import_results[idx].filepath
                for idx in self.selected_indices
                if 0 <= idx < len(self.import_results)
            ]
            self.gallery.select_paths(selected_paths)
            self._show_multi_selection_state()
            self._update_scale_group_state()
            self._update_set_from_image_button_state()
            self._update_ai_controls_state()
            self._update_ai_table()
            self._update_ai_overlay()
        elif self.selected_index is not None and 0 <= self.selected_index < len(self.import_results):
            self._select_image(self.selected_index)
        elif self.import_results:
            self._select_image(0)

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
        if not bool(getattr(result, "resize_to_optimal", False)):
            return pixmap, False
        if getattr(self, "_calibration_mode", False):
            return pixmap, False
        if self._crop_active_mode == "image":
            return pixmap, False
        if self._is_resized_image(result):
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
                if self._is_resized_image(result):
                    self.preview.set_corner_tag(self.tr("Already resized"), QColor(39, 174, 96))
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
        index = self._current_single_index()
        if index is None:
            if hasattr(self, "preview"):
                self.preview.set_corner_tag("")
            return
        if index < 0 or index >= len(self.import_results):
            return
        result = self.import_results[index]
        if not force and not bool(getattr(result, "resize_to_optimal", False)):
            return
        self._set_preview_for_result(result, preserve_view=preserve_view)
        factor = self._compute_resample_scale_factor(result, respect_toggle=False)
        # If resize preview is ON but effective factor is ~1.0 (no resize),
        # still force full-resolution so users do not see the cached
        # downscaled preview and think a resize happened.
        if (
            bool(getattr(result, "resize_to_optimal", False))
            and factor >= 0.999
            and hasattr(self, "preview")
        ):
            try:
                self.preview.ensure_full_resolution()
            except Exception:
                pass
        # When toggling resize preview off, show the true original immediately.
        # Without this, the widget may stay on a cached downscaled preview until
        # the next zoom action triggers full-resolution loading.
        if (
            force
            and not bool(getattr(result, "resize_to_optimal", False))
            and hasattr(self, "preview")
        ):
            try:
                self.preview.ensure_full_resolution()
            except Exception:
                pass
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

    def _is_gallery_navigation_shortcut_blocked(self) -> bool:
        focus_widget = QApplication.focusWidget()
        if focus_widget is None:
            return False
        if isinstance(focus_widget, (QLineEdit, QTextEdit, QPlainTextEdit, QAbstractSpinBox)):
            return True
        current = focus_widget
        while current is not None:
            if isinstance(current, QComboBox):
                return True
            current = current.parentWidget()
        return False

    def _select_adjacent_gallery_image(self, step: int) -> None:
        if step == 0 or not self.import_results:
            return
        indices = self._current_selection_indices()
        if indices:
            base_index = max(indices) if step > 0 else min(indices)
        elif self.selected_index is not None:
            base_index = self.selected_index
        else:
            base_index = -1 if step > 0 else len(self.import_results)
        target_index = max(0, min(len(self.import_results) - 1, base_index + step))
        if target_index == base_index and self.selected_index == target_index:
            return
        self._select_image(target_index)

    def _on_next_image_shortcut(self) -> None:
        if self._is_gallery_navigation_shortcut_blocked():
            return
        self._select_adjacent_gallery_image(1)

    def _on_previous_image_shortcut(self) -> None:
        if self._is_gallery_navigation_shortcut_blocked():
            return
        self._select_adjacent_gallery_image(-1)

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
        self._sync_scale_bar_length_unit_for_image_type()
        indices = self.selected_indices or [self.selected_index]
        self._last_settings_action = "image_type"
        self._apply_settings_to_indices(indices, action="image_type")

    def eventFilter(self, obj, event):
        if event.type() == QEvent.DragEnter and self._accept_image_drag(event):
            return True
        if event.type() == QEvent.DragMove and self._accept_image_drag(event):
            return True
        if event.type() == QEvent.Drop and self._handle_image_drop(event):
            return True
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
        if self._crop_active_mode:
            self._set_crop_active_mode(None)
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
        self._restore_scale_bar_overlay(result)

    def _load_result_into_form(self, result: ImageImportResult) -> None:
        self._loading_form = True
        is_micro = result.image_type == "microscope"
        if is_micro:
            self.micro_radio.setChecked(True)
        else:
            self.field_radio.setChecked(True)
        self._sync_scale_bar_length_unit_for_image_type()
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
        self._set_combo_tag_value(
            self.contrast_combo,
            "contrast",
            result.contrast or (self.contrast_default if is_micro else self._field_tag_value("contrast")),
        )
        self._set_combo_tag_value(
            self.mount_combo,
            "mount",
            result.mount_medium or (self.mount_default if is_micro else self._field_tag_value("mount")),
        )
        self._set_combo_tag_value(
            self.stain_combo,
            "stain",
            result.stain or (self.stain_default if is_micro else self._field_tag_value("stain")),
        )
        self._set_combo_tag_value(
            self.sample_combo,
            "sample",
            result.sample_type or (self.sample_default if is_micro else self._field_tag_value("sample")),
        )
        if hasattr(self, "image_note_input"):
            self.image_note_input.blockSignals(True)
            self.image_note_input.setPlainText(str(result.notes or ""))
            self.image_note_input.blockSignals(False)
        if result.scale_bar_length_um:
            display_len = (
                float(result.scale_bar_length_um) / 1000.0
                if result.image_type == "field"
                else float(result.scale_bar_length_um)
            )
            self.scale_bar_length_input.setValue(display_len)
        if hasattr(self, "resize_optimal_checkbox"):
            self.resize_optimal_checkbox.blockSignals(True)
            self.resize_optimal_checkbox.setChecked(bool(getattr(result, "resize_to_optimal", False)))
            self.resize_optimal_checkbox.blockSignals(False)
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
            self._update_scalebar_controls_visibility()
            if current_key == self.CUSTOM_OBJECTIVE_KEY and self._custom_scale is None:
                self._update_resize_group_state()
                self._start_scale_bar_selection()
                return
        elif sender is self.contrast_combo:
            action = "contrast"
        elif sender is self.mount_combo:
            action = "mount"
        elif sender is self.stain_combo:
            action = "stain"
        elif sender is self.sample_combo:
            action = "sample"
        elif sender is self.image_note_input:
            action = "notes"
        elif sender in (self.field_radio, self.micro_radio, self.image_type_group):
            action = "image_type"
            self._sync_scale_bar_length_unit_for_image_type()
            if self.field_radio.isChecked():
                self._set_field_tag_defaults_in_form()
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

    def _current_image_note_text(self) -> str | None:
        if not hasattr(self, "image_note_input"):
            return None
        text = str(self.image_note_input.toPlainText() or "").strip()
        return text or None

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
        result.stain = self._get_combo_tag_value(self.stain_combo, "stain")
        result.sample_type = self._get_combo_tag_value(self.sample_combo, "sample")
        result.notes = self._current_image_note_text()
        if result.image_type != "microscope":
            result.contrast = self._field_tag_value("contrast")
            result.mount_medium = self._field_tag_value("mount")
            result.stain = self._field_tag_value("stain")
            result.sample_type = self._field_tag_value("sample")
        result.needs_scale = (
            result.image_type == "microscope"
            and not result.objective
            and not result.custom_scale
        )
        if action == "resize" and hasattr(self, "resize_optimal_checkbox"):
            result.resize_to_optimal = bool(self.resize_optimal_checkbox.isChecked())
        if action == "resize":
            result.store_original = self._store_originals_enabled() if result.resize_to_optimal else False
        if not result.image_id:
            result.resample_scale_factor = self._compute_resample_scale_factor(result)
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
            self._refresh_gallery()
            self._update_summary()
            if (
                self.selected_index is not None
                and self.selected_index in applied
                and len(self.selected_indices) <= 1
            ):
                selected_result = self.import_results[self.selected_index]
                self._update_current_image_sampling(selected_result)
                if action == "scale":
                    self._refresh_resize_preview(preserve_view=True)
                elif action == "image_type":
                    preview_pixmap = getattr(self.preview, "original_pixmap", None) if hasattr(self, "preview") else None
                    self._update_resize_preview_tag(selected_result, False, preview_pixmap)
                elif action == "resize":
                    self._refresh_resize_preview(force=True, preserve_view=True)
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
            # Prefer the in-memory value to avoid stale DB reads during repeated rescales in the same session
            old_scale = result.custom_scale if result.custom_scale is not None else img.get("scale_microns_per_pixel")
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
        if not any(m.get("length_um") is not None for m in measurements):
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
        if not any(m.get("length_um") is not None for m in measurements):
            return True
        ratio = float(new_scale) / float(old_scale)
        conn = get_connection()
        cursor = conn.cursor()
        for m in measurements:
            old_length = m.get("length_um")
            if old_length is None:
                continue
            new_length_um = float(old_length) * ratio
            old_width = m.get("width_um")
            new_width_um = float(old_width) * ratio if old_width is not None else None
            q_value = new_length_um / new_width_um if new_width_um and new_width_um > 0 else 0
            cursor.execute(
                "UPDATE spore_measurements SET length_um = ?, width_um = ?, notes = ? WHERE id = ?",
                (new_length_um, new_width_um, f"Q={q_value:.1f}", m["id"]),
            )
        # Also update the image's scale so _apply_import_results_to_observation won't double-rescale
        cursor.execute(
            "UPDATE images SET scale_microns_per_pixel = ? WHERE id = ?",
            (float(new_scale), image_id),
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
        pixmap = self._load_pixmap_with_orientation(path)
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
        _debug_import_flow(
            "set observation metadata from current image; "
            f"datetime={(self._observation_datetime.toString('yyyy-MM-dd HH:mm:ss') if self._observation_datetime and self._observation_datetime.isValid() else None)}; "
            f"gps=({self._observation_lat}, {self._observation_lon}); "
            f"source_index={self._observation_source_index}"
        )
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

        camera = get_camera_model(exif)
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
        cached = self._image_size_cache.get(path)
        if cached:
            return cached
        reader = QImageReader(path)
        size = reader.size()
        if not size.isValid():
            return None
        width = size.width()
        height = size.height()
        if width <= 0 or height <= 0:
            return None
        dimensions = (width, height)
        self._image_size_cache[path] = dimensions
        return dimensions

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
        objective = self.objectives.get(objective_key) if hasattr(self, "objectives") else None
        if isinstance(objective, dict):
            provisional = objective.get("provisional_microns_per_pixel")
            if isinstance(provisional, (int, float)) and provisional > 0:
                return float(provisional)
        return None

    def _objective_optics_type(self, objective_key: str | None) -> str:
        if not objective_key or objective_key == self.CUSTOM_OBJECTIVE_KEY:
            return "microscope"
        objective = self.objectives.get(objective_key)
        if not isinstance(objective, dict):
            return "microscope"
        return "macro" if str(objective.get("optics_type") or "").strip().lower() == "macro" else "microscope"

    def _compute_resample_scale_factor(
        self,
        result: ImageImportResult | None,
        respect_toggle: bool = True,
    ) -> float:
        if not result or result.image_type != "microscope":
            return 1.0
        if (
            respect_toggle
            and not bool(getattr(result, "resize_to_optimal", False))
        ):
            return 1.0
        objective = None
        if result.objective and result.objective in self.objectives:
            objective = self.objectives[result.objective]
        if self._objective_optics_type(result.objective) == "macro":
            return 1.0
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
        is_macro_profile = self._objective_optics_type(result.objective) == "macro"
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
            self.target_sampling_input.setEnabled(not is_macro_profile)
        if is_macro_profile:
            self.target_resolution_label.setText("--")
            self._set_resize_resolution_hints(
                current_hint,
                self.tr("Ideal Nyquist-based resize is available only for microscope objectives."),
            )
            return
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
        if not bool(getattr(result, "resize_to_optimal", False)):
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
        if getattr(self, "_loading_form", False):
            return
        resize_enabled = bool(self.resize_optimal_checkbox.isChecked())
        SettingsDB.set_setting("resize_to_optimal_sampling", resize_enabled)
        indices = self.selected_indices or ([self.selected_index] if self.selected_index is not None else [])
        if not indices:
            return
        self._last_settings_action = "resize"
        self._apply_settings_to_indices(indices, action="resize")

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
        self._refresh_gallery()
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
        if self._objective_optics_type(result.objective) == "macro":
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
        if self._crop_active_mode:
            self._set_crop_active_mode(None)
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
        image_ids = [
            int(result.image_id)
            for result in self.import_results
            if isinstance(result.image_id, int) and result.image_id > 0
        ]
        measured_image_ids: set[int] = set()
        if image_ids:
            conn = None
            try:
                conn = get_connection()
                cursor = conn.cursor()
                placeholders = ",".join("?" for _ in image_ids)
                cursor.execute(
                    f"SELECT DISTINCT image_id FROM spore_measurements WHERE image_id IN ({placeholders})",
                    tuple(image_ids),
                )
                measured_image_ids = {int(row[0]) for row in cursor.fetchall() if row and row[0] is not None}
            except Exception:
                measured_image_ids = set()
            finally:
                if conn is not None:
                    conn.close()
        items = []
        for idx, result in enumerate(self.import_results):
            objective_label = result.objective
            if result.objective and result.objective in self.objectives:
                objective_label = objective_display_name(
                    self.objectives[result.objective],
                    result.objective,
                ) or result.objective
            resize_suggested = (
                bool(getattr(result, "resize_to_optimal", False))
                and self._compute_resample_scale_factor(result) < 0.999
            )
            badges = ImageGalleryWidget.build_image_type_badges(
                image_type=result.image_type,
                objective_name=objective_label,
                contrast=result.contrast,
                custom_scale=bool(result.custom_scale),
                needs_scale=bool(result.needs_scale),
                resize_to_optimal=resize_suggested,
                translate=self.tr,
            )
            has_measurements = bool(result.image_id and int(result.image_id) in measured_image_ids)
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
        self._update_action_buttons_state()

    def _on_remove_selected(self) -> None:
        if not self.selected_indices:
            return
        selected_indices = sorted(
            {
                idx
                for idx in self.selected_indices
                if idx is not None and 0 <= idx < len(self.import_results)
            }
        )
        if not selected_indices:
            return
        measurement_indices = []
        for idx in selected_indices:
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
        removed_numbers = [idx + 1 for idx in selected_indices]
        removed_indices = list(selected_indices)
        removed_results = [
            self.import_results[idx]
            for idx in removed_indices
            if 0 <= idx < len(self.import_results)
        ]
        next_index = removed_indices[0]
        for idx in sorted(selected_indices, reverse=True):
            if 0 <= idx < len(self.import_results):
                del self.import_results[idx]
                del self.image_paths[idx]
        if removed_indices:
            self._remap_ai_indices(removed_indices)
        for result in removed_results:
            self._invalidate_cached_pixmap(getattr(result, "filepath", None))
            self._invalidate_cached_pixmap(getattr(result, "preview_path", None))
            self._invalidate_cached_pixmap(getattr(result, "original_filepath", None))
            for path in {
                getattr(result, "filepath", None),
                getattr(result, "preview_path", None),
                getattr(result, "original_filepath", None),
            }:
                if not path:
                    continue
                self._temp_preview_paths.discard(path)
                self._converted_import_paths.discard(path)
                self._missing_exif_paths.discard(path)
        self.selected_indices = []
        self.selected_index = None
        self.primary_index = None
        self._seed_observation_metadata()
        self._update_observation_source_index()
        self._sync_observation_metadata_inputs()
        self._refresh_gallery()
        self._update_summary()
        if removed_numbers:
            if len(removed_numbers) == 1:
                message = self.tr("Image {num} deleted").format(num=removed_numbers[0])
            else:
                message = self.tr("Deleted {count} images").format(count=len(removed_numbers))
            self._set_settings_hint(message, "#e74c3c")
        if self.image_paths:
            target_index = max(0, min(next_index, len(self.image_paths) - 1))
            self._select_image(target_index)
        else:
            self._show_multi_selection_state()
            self.preview.set_measurement_lines([])
            self.preview.set_scale_bar_draggable(False)

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
                    if source is self._image_crop_undo_by_index and isinstance(value, ImageCropUndoState):
                        self._temp_preview_paths.discard(value.backup_path)
                        try:
                            Path(value.backup_path).unlink(missing_ok=True)
                        except Exception:
                            pass
                    continue
                remapped[new_index(old_index)] = value
            return remapped

        self._ai_predictions_by_index = remap_dict(self._ai_predictions_by_index)
        self._ai_selected_by_index = remap_dict(self._ai_selected_by_index)
        self._ai_crop_boxes = remap_dict(self._ai_crop_boxes)
        self._crop_mode_by_index = remap_dict(self._crop_mode_by_index)
        self._image_crop_undo_by_index = remap_dict(self._image_crop_undo_by_index)
        self._ai_selected_taxon = None

    def _accept_and_close(self) -> None:
        start_time = time.perf_counter()
        self._apply_to_selected()
        self._save_last_used_tag_settings()
        self._accepted = True
        _debug_import_flow(
            "accept requested; "
            f"continue_to_details={self._continue_to_observation_details}; "
            f"images={len(self.import_results)}; "
            f"selected_indices={list(self.selected_indices)}; "
            f"gps=({self._observation_lat}, {self._observation_lon}); "
            f"elapsed_before_accept={time.perf_counter() - start_time:.3f}s"
        )
        if self._continue_to_observation_details:
            self.continueRequested.emit(self.import_results)
        self.accept()

    def _save_last_used_tag_settings(self) -> None:
        selected_indices = [
            idx
            for idx in self._current_selection_indices()
            if idx is not None and 0 <= idx < len(self.import_results)
        ]
        if selected_indices and not any(
            (self.import_results[idx].image_type or "field") == "microscope"
            for idx in selected_indices
        ):
            return
        SettingsDB.set_setting(
            DatabaseTerms.last_used_key("contrast"),
            self._get_combo_tag_value(self.contrast_combo, "contrast"),
        )
        SettingsDB.set_setting(
            DatabaseTerms.last_used_key("mount"),
            self._get_combo_tag_value(self.mount_combo, "mount"),
        )
        SettingsDB.set_setting(
            DatabaseTerms.last_used_key("stain"),
            self._get_combo_tag_value(self.stain_combo, "stain"),
        )
        SettingsDB.set_setting(
            DatabaseTerms.last_used_key("sample"),
            self._get_combo_tag_value(self.sample_combo, "sample"),
        )

    def get_observation_gps(self) -> tuple[float | None, float | None]:
        return self._observation_lat, self._observation_lon

    def _restore_scale_bar_overlay(self, result: "ImageImportResult") -> None:
        """Draw the stored scale bar selection line on the preview, if any."""
        sel = getattr(result, "scale_bar_selection", None)
        if sel and len(sel) == 2:
            (x1, y1), (x2, y2) = sel
            pixmap = getattr(self.preview, "original_pixmap", None)
            if pixmap and not pixmap.isNull():
                width = float(pixmap.width())
                height = float(pixmap.height())
                in_bounds = (
                    0.0 <= float(x1) <= width
                    and 0.0 <= float(y1) <= height
                    and 0.0 <= float(x2) <= width
                    and 0.0 <= float(y2) <= height
                )
                if not in_bounds:
                    self.preview.set_measurement_lines([])
                    self.preview.set_scale_bar_draggable(False)
                    self._update_scalebar_controls_visibility()
                    return
            self.preview.set_measurement_lines([[x1, y1, x2, y2]])
            # Restore pixel distance so Apply can recompute µm/pixel
            dx = x2 - x1; dy = y2 - y1
            px_dist = (dx * dx + dy * dy) ** 0.5
            if px_dist > 0:
                self._scale_bar_pixel_distance = px_dist
            self.preview.set_scale_bar_draggable(True)
            self._update_scalebar_controls_visibility()
            length_um = getattr(result, "scale_bar_length_um", None)
            if length_um:
                is_field = (result.image_type or "").strip().lower() == "field"
                self.scale_bar_length_input.setValue(float(length_um) / 1000.0 if is_field else float(length_um))
        else:
            self.preview.set_measurement_lines([])
            self.preview.set_scale_bar_draggable(False)
            self._update_scalebar_controls_visibility()

    def enter_calibration_mode(self, dialog):
        if not getattr(self, "preview", None) or not self.preview.original_pixmap:
            return
        if self._crop_active_mode:
            self._set_crop_active_mode(None)
        if hasattr(self.preview, "ensure_full_resolution"):
            self.preview.ensure_full_resolution()
        self.calibration_dialog = dialog
        self.calibration_points = []
        self._calibration_mode = True
        self.preview.clear_preview_line()

    def _on_preview_clicked(self, pos):
        if not getattr(self, "_calibration_mode", False):
            # If the user clicks near an existing scale bar selection, open the dialog
            result = (
                self.import_results[self.selected_index]
                if self.selected_index is not None and self.selected_index < len(self.import_results)
                else None
            )
            sel = getattr(result, "scale_bar_selection", None) if result else None
            if sel and len(sel) == 2:
                (x1, y1), (x2, y2) = sel
                dx = x2 - x1; dy = y2 - y1
                seg_len = (dx * dx + dy * dy) ** 0.5
                if seg_len > 0:
                    t = max(0.0, min(1.0, ((pos.x() - x1) * dx + (pos.y() - y1) * dy) / (seg_len * seg_len)))
                    cx = x1 + t * dx; cy = y1 + t * dy
                    if ((pos.x() - cx) ** 2 + (pos.y() - cy) ** 2) ** 0.5 <= 20:
                        # Restore pixel distance so _apply_scale_bar_inline can recompute µm/pixel
                        self._scale_bar_pixel_distance = seg_len
                        self._update_scalebar_controls_visibility()
                        length_um = getattr(result, "scale_bar_length_um", None)
                        if length_um:
                            is_field = (result.image_type or "").strip().lower() == "field"
                            value = float(length_um) / 1000.0 if is_field else float(length_um)
                            self.scale_bar_length_input.setValue(value)
                        self.set_hint(self.tr("Adjust length and click again to re-apply scale"))
                        return
            return
        self.calibration_points.append(pos)
        if len(self.calibration_points) == 1:
            _horiz = hasattr(self, "scale_bar_horizontal_checkbox") and self.scale_bar_horizontal_checkbox.isChecked()
            self.preview.set_preview_line(pos, horizontal=_horiz)
            self.set_hint(self.tr("Now click the end point of the scale bar"))
            return
        if len(self.calibration_points) == 2:
            p1, p2 = self.calibration_points
            # Constrain to horizontal line if checkbox is set
            if hasattr(self, "scale_bar_horizontal_checkbox") and self.scale_bar_horizontal_checkbox.isChecked():
                p2 = QPointF(p2.x(), p1.y())
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
            # Store pixel distance so _apply_scale_bar_inline can compute µm/pixel
            self._scale_bar_pixel_distance = distance
            # Enable endpoint dragging
            self.preview.set_scale_bar_draggable(True)
            # Persist the selection on the current result so the overlay survives image switching
            if self.selected_index is not None and self.selected_index < len(self.import_results):
                self.import_results[self.selected_index].scale_bar_selection = (
                    (p1.x(), p1.y()), (p2.x(), p2.y())
                )
            self.calibration_points = []
            # Apply scale immediately — 2nd click is the trigger
            self._apply_scale_bar_inline()

    def done(self, result: int) -> None:  # noqa: N802 - Qt API
        self._cleanup_dialog_threads()
        super().done(result)

    def closeEvent(self, event):
        dialog = getattr(self, "_scale_bar_dialog", None)
        if dialog and dialog.isVisible():
            dialog.close()
        self._scale_bar_dialog = None
        self._cleanup_dialog_threads()
        for path in list(self._temp_preview_paths):
            try:
                Path(path).unlink(missing_ok=True)
            except Exception:
                pass
        self._temp_preview_paths.clear()
        self._image_crop_undo_by_index.clear()
        if not self._accepted:
            for path in list(self._converted_import_paths):
                try:
                    Path(path).unlink(missing_ok=True)
                except Exception:
                    pass
            self._converted_import_paths.clear()
        super().closeEvent(event)
