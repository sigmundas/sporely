"""Calibration dialog for setting microscope objective scales."""
from __future__ import annotations

import json
import base64
import io
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional
from uuid import uuid4

import numpy as np
from PIL import Image, ImageDraw, ImageFont
from PySide6.QtCore import Qt, Signal, QPointF, QStandardPaths, QObject, QThread, Slot, QLocale
from PySide6.QtGui import QPixmap, QKeySequence, QShortcut, QIntValidator, QDoubleValidator
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, QPushButton,
    QComboBox, QFormLayout, QGroupBox, QTabWidget, QWidget, QDoubleSpinBox,
    QSplitter, QListWidget, QListWidgetItem, QTableWidget, QTableWidgetItem,
    QHeaderView, QAbstractItemView, QFileDialog, QMessageBox, QSizePolicy,
    QCheckBox, QProgressBar, QGridLayout, QLayout, QDialogButtonBox, QStyle,
)

from database.schema import (
    load_objectives, save_objectives, get_last_objective_path,
    get_calibrations_dir, get_app_settings, update_app_settings,
    format_objective_display, objective_display_name, objective_sort_value,
)
from database.models import CalibrationDB, ObservationDB, SettingsDB
import utils.slide_calibration as slide_calibration
from utils.exif_reader import get_exif_data
from .hint_status import HintBar, HintLabel, HintStatusController
from .styles import pt
from .window_state import GeometryMixin
from .zoomable_image_widget import ZoomableImageLabel
from .image_gallery_widget import ImageGalleryWidget
from .export_image_dialog import ExportImageDialog


def calculate_calibration_stats(measurements: list[tuple[float, float]]):
    """
    Calculate calibration statistics from measurements.

    Args:
        measurements: list of (known_um, measured_px) tuples

    Returns:
        tuple: (mean_um_per_px, std, ci_low, ci_high)
    """
    if not measurements:
        return None, None, None, None

    um_per_px = [um / px for um, px in measurements if px > 0]

    if not um_per_px:
        return None, None, None, None

    if len(um_per_px) == 1:
        return um_per_px[0], None, None, None

    mean = float(np.mean(um_per_px))
    std = float(np.std(um_per_px, ddof=1))
    n = len(um_per_px)
    sem = std / np.sqrt(n)

    # 95% confidence interval using t-distribution
    # t-values for 95% CI (two-tailed) by degrees of freedom
    t_values = {
        1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
        6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228,
        15: 2.131, 20: 2.086, 25: 2.060, 30: 2.042, 40: 2.021,
        50: 2.009, 100: 1.984, 1000: 1.962,
    }
    df = n - 1
    # Find closest t-value
    if df in t_values:
        t_val = t_values[df]
    else:
        # Interpolate or use closest available
        available = sorted(t_values.keys())
        if df < available[0]:
            t_val = t_values[available[0]]
        elif df > available[-1]:
            t_val = 1.96  # Approximate for large df
        else:
            # Find surrounding values and interpolate
            lower = max(k for k in available if k <= df)
            upper = min(k for k in available if k >= df)
            if lower == upper:
                t_val = t_values[lower]
            else:
                ratio = (df - lower) / (upper - lower)
                t_val = t_values[lower] + ratio * (t_values[upper] - t_values[lower])

    margin = t_val * sem
    ci_low = mean - margin
    ci_high = mean + margin

    return mean, std, float(ci_low), float(ci_high)


def um_to_nm(um: float) -> float:
    """Convert micrometers to nanometers."""
    return um * 1000


def nm_to_um(nm: float) -> float:
    """Convert nanometers to micrometers."""
    return nm / 1000


def get_resolution_status(pixels_per_micron, numerical_aperture, wavelength_um=0.405):
    """Assess sampling relative to Nyquist (ideal pixel = lambda / (4 * NA))."""
    if not pixels_per_micron or not numerical_aperture or numerical_aperture <= 0:
        return {
            "status": "Unknown",
            "quality": "unknown",
            "sampling_pct": 0.0,
            "ideal_pixel_um": 0.0,
            "ideal_pixels_per_micron": 0.0,
            "downsample_advice": None,
        }

    ideal_pixel_um = float(wavelength_um) / (4.0 * float(numerical_aperture))
    ideal_pixels_per_micron = 1.0 / ideal_pixel_um if ideal_pixel_um > 0 else 0.0
    sampling_pct = (
        (float(pixels_per_micron) / ideal_pixels_per_micron) * 100.0
        if ideal_pixels_per_micron > 0
        else 0.0
    )

    if sampling_pct < 80.0:
        status = "Undersampled"
        quality = "undersampled"
        downsample = None
    elif sampling_pct <= 150.0:
        status = "Good"
        quality = "good"
        downsample = None
    elif sampling_pct < 200.0:
        status = "Oversampled"
        quality = "oversampled"
        reduce_pct = max(1.0, min(100.0, 10000.0 / sampling_pct))
        downsample = f"Consider downsampling to {reduce_pct:.0f}%"
    elif sampling_pct <= 260.0:
        status = "Oversampled"
        quality = "oversampled"
        reduce_pct = max(1.0, min(100.0, 10000.0 / sampling_pct))
        downsample = f"Consider downsampling to {reduce_pct:.0f}%"
    else:
        status = "Heavily oversampled"
        quality = "heavy_oversample"
        reduce_pct = max(1.0, min(100.0, 10000.0 / sampling_pct))
        downsample = f"Consider downsampling to {reduce_pct:.0f}%"

    return {
        "status": status,
        "quality": quality,
        "sampling_pct": float(sampling_pct),
        "ideal_pixel_um": float(ideal_pixel_um),
        "ideal_pixels_per_micron": float(ideal_pixels_per_micron),
        "downsample_advice": downsample,
    }


def format_resolution_summary(pixels_per_micron, numerical_aperture, wavelength_um=0.405):
    """Generate a compact multi-line summary of sampling status."""
    result = get_resolution_status(pixels_per_micron, numerical_aperture, wavelength_um)
    ideal_nm = result["ideal_pixel_um"] * 1000.0
    summary = (
        f"Calibration: {pixels_per_micron:.2f} px/um\n"
        f"Nyquist pixel: {ideal_nm:.1f} nm\n"
        f"Sampling: {result['status']} ({result['sampling_pct']:.0f}% of Nyquist)"
    )
    if result["downsample_advice"]:
        summary += f"\n{result['downsample_advice']}"
    return summary


class NewObjectiveDialog(QDialog):
    """Dialog for creating or editing a microscope objective."""

    def __init__(
        self,
        parent=None,
        existing_keys: list[str] | None = None,
        objective_data: dict | None = None,
        objective_key: str | None = None,
        edit_mode: bool = False,
    ):
        super().__init__(parent)
        self.edit_mode = bool(edit_mode)
        self.original_key = objective_key
        self.setWindowTitle(self.tr("Edit Objective") if self.edit_mode else self.tr("New Objective"))
        self.setModal(True)
        self.setMinimumWidth(400)
        self.existing_keys = existing_keys or []
        self._objective_data = dict(objective_data) if isinstance(objective_data, dict) else {}

        self._init_ui()
        if self._objective_data:
            self._populate_form(self._objective_data)

    def _make_key(self, display_name: str) -> str:
        if self.edit_mode and self.original_key:
            return self.original_key
        key = re.sub(r"[^A-Za-z0-9._-]+", "_", display_name).strip("_")
        return key or display_name

    def _init_ui(self):
        layout = QVBoxLayout(self)

        form = QFormLayout()

        # Magnification
        self.magnification_input = QLineEdit()
        self.magnification_input.setPlaceholderText(self.tr("e.g., 40"))
        self.magnification_input.setValidator(QIntValidator(1, 1000, self))
        self.magnification_input.setMinimumHeight(26)
        self.magnification_input.setStyleSheet("padding: 4px 6px;")
        form.addRow(self.tr("Magnification (X):"), self.magnification_input)

        # Numerical aperture
        self.na_input = QLineEdit()
        self.na_input.setPlaceholderText(self.tr("e.g., 0.75"))
        na_validator = QDoubleValidator(0.01, 2.0, 2, self)
        na_validator.setNotation(QDoubleValidator.StandardNotation)
        na_validator.setLocale(QLocale.system())
        self.na_input.setValidator(na_validator)
        self.na_input.setMinimumHeight(26)
        self.na_input.setStyleSheet("padding: 4px 6px;")
        form.addRow(self.tr("NA:"), self.na_input)

        # Objective name
        self.objective_name_input = QLineEdit()
        self.objective_name_input.setPlaceholderText(self.tr("e.g., Plan achro"))
        form.addRow(self.tr("Objective name:"), self.objective_name_input)

        # Notes (microscope and camera description)
        self.notes_input = QLineEdit()
        self.notes_input.setPlaceholderText(self.tr("e.g., Leica DM2000, Olympus MFT 1:1"))
        form.addRow(self.tr("Notes:"), self.notes_input)

        layout.addLayout(form)

        # Buttons
        button_row = QHBoxLayout()
        button_row.addStretch()

        cancel_btn = QPushButton(self.tr("Cancel"))
        cancel_btn.clicked.connect(self.reject)
        button_row.addWidget(cancel_btn)

        self.ok_btn = QPushButton(self.tr("Save") if self.edit_mode else self.tr("Create"))
        self.ok_btn.clicked.connect(self._on_create)
        self.ok_btn.setDefault(True)
        button_row.addWidget(self.ok_btn)

        layout.addLayout(button_row)

    def _populate_form(self, data: dict) -> None:
        magnification = data.get("magnification")
        if magnification is not None:
            self.magnification_input.setText(str(int(magnification)) if float(magnification).is_integer() else str(magnification))
        na_value = data.get("na")
        if na_value is not None:
            self.na_input.setText(str(na_value))
        self.objective_name_input.setText(str(data.get("objective_name") or ""))
        self.notes_input.setText(str(data.get("notes") or ""))

    def _on_create(self):
        objective_name = self.objective_name_input.text().strip()
        mag_text = self.magnification_input.text().strip()
        na_text = self.na_input.text().strip()

        try:
            magnification = int(mag_text)
        except (TypeError, ValueError):
            magnification = 0

        try:
            na_value = float(na_text.replace(",", "."))
        except (TypeError, ValueError):
            na_value = 0.0

        if not objective_name:
            QMessageBox.warning(self, self.tr("Missing Name"), self.tr("Please enter an objective name."))
            return

        if magnification <= 0:
            QMessageBox.warning(self, self.tr("Missing Magnification"), self.tr("Please enter a magnification."))
            return

        if na_value <= 0:
            QMessageBox.warning(self, self.tr("Missing NA"), self.tr("Please enter a numerical aperture (NA)."))
            return

        display_name = format_objective_display(magnification, na_value, objective_name)
        if not display_name:
            QMessageBox.warning(self, self.tr("Invalid Name"), self.tr("Please enter valid objective details."))
            return

        key = self._make_key(display_name)
        if key in self.existing_keys or display_name in self.existing_keys:
            QMessageBox.warning(
                self,
                self.tr("Duplicate"),
                self.tr("An objective with this name already exists."),
            )
            return

        self.accept()

    def get_objective_data(self) -> dict:
        """Get the objective data from the dialog."""
        mag_text = self.magnification_input.text().strip()
        na_text = self.na_input.text().strip()
        magnification = int(mag_text) if mag_text else 0
        na_value = float(na_text.replace(",", ".")) if na_text else 0.0
        objective_name = self.objective_name_input.text().strip()
        display_name = format_objective_display(magnification, na_value, objective_name)
        key = self._make_key(display_name)
        return {
            "key": key,
            "name": display_name,
            "objective_name": objective_name,
            "magnification": magnification,
            "na": na_value,
            "microns_per_pixel": 0.1,  # Default, will be set by calibration
            "notes": self.notes_input.text().strip(),
        }


class ObservationSelectionDialog(QDialog):
    """Dialog for selecting observations to update when calibration changes."""

    def __init__(self, parent=None, observations: list[dict] = None, old_scale: float = 0, new_scale: float = 0):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Update Observations"))
        self.setModal(True)
        self.setMinimumSize(700, 500)
        self.observations = observations or []
        self.old_scale = old_scale
        self.new_scale = new_scale
        self.selected_observation_ids = []

        self._init_ui()

    def _init_ui(self):
        layout = QVBoxLayout(self)

        # Info
        diff_percent = ((self.new_scale - self.old_scale) / self.old_scale * 100) if self.old_scale > 0 else 0
        sign = "+" if diff_percent >= 0 else ""
        info_label = QLabel(
            self.tr(
                "This calibration is used for the following observations.\n"
                "Scale change: {old:.4f} → {new:.4f} nm/px ({sign}{diff:.2f}%)\n\n"
                "Select the observations you would like to update:"
            ).format(
                old=um_to_nm(self.old_scale),
                new=um_to_nm(self.new_scale),
                sign=sign,
                diff=diff_percent,
            )
        )
        info_label.setWordWrap(True)
        layout.addWidget(info_label)

        # Table with multi-select
        self.table = QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels([
            self.tr("Species"),
            self.tr("Common Name"),
            self.tr("Date"),
            self.tr("Images"),
            self.tr("Measurements"),
        ])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        for col in range(2, 5):
            self.table.horizontalHeader().setSectionResizeMode(col, QHeaderView.ResizeToContents)
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.MultiSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)

        # Populate table
        for obs in self.observations:
            row = self.table.rowCount()
            self.table.insertRow(row)

            species = f"{obs.get('genus', '')} {obs.get('species', '')}".strip() or "--"
            self.table.setItem(row, 0, QTableWidgetItem(species))

            common = obs.get("common_name", "") or "--"
            self.table.setItem(row, 1, QTableWidgetItem(common))

            date = obs.get("date", "")[:10] if obs.get("date") else "--"
            self.table.setItem(row, 2, QTableWidgetItem(date))

            img_count = obs.get("image_count", 0)
            self.table.setItem(row, 3, QTableWidgetItem(str(img_count)))

            measure_count = obs.get("measurement_count", 0)
            self.table.setItem(row, 4, QTableWidgetItem(str(measure_count)))

        layout.addWidget(self.table, 1)

        # Select all / none buttons
        select_row = QHBoxLayout()
        select_all_btn = QPushButton(self.tr("Select All"))
        select_all_btn.clicked.connect(self.table.selectAll)
        select_row.addWidget(select_all_btn)

        select_none_btn = QPushButton(self.tr("Select None"))
        select_none_btn.clicked.connect(self.table.clearSelection)
        select_row.addWidget(select_none_btn)

        select_row.addStretch()
        layout.addLayout(select_row)

        # Buttons
        button_row = QHBoxLayout()
        button_row.addStretch()

        skip_btn = QPushButton(self.tr("Skip (Don't Update)"))
        skip_btn.clicked.connect(self.reject)
        button_row.addWidget(skip_btn)

        update_btn = QPushButton(self.tr("Update Selected"))
        update_btn.clicked.connect(self._on_update)
        button_row.addWidget(update_btn)

        layout.addLayout(button_row)

    def _on_update(self):
        selected_rows = set(idx.row() for idx in self.table.selectedIndexes())
        self.selected_observation_ids = [
            self.observations[row].get("observation_id")
            for row in selected_rows
            if row < len(self.observations) and self.observations[row].get("observation_id")
        ]
        self.accept()


class _AutoCalibrationWorker(QObject):
    progress = Signal(str, float)
    finished = Signal(object, float, object, object)
    failed = Signal(str)

    def __init__(
        self,
        image_path: str,
        spacing_um: float,
        use_edges: bool,
        crop_box: tuple[float, float, float, float] | None = None,
    ) -> None:
        super().__init__()
        self.image_path = image_path
        self.spacing_um = float(spacing_um)
        self.use_edges = bool(use_edges)
        self.crop_box = crop_box

    @Slot()
    def run(self) -> None:
        crop_offset = (0.0, 0.0)
        crop_size = None
        pil_img = None
        if self.crop_box:
            try:
                pil_img = Image.open(self.image_path).convert("RGB")
                w, h = pil_img.size
                x1 = max(0, min(w, int(self.crop_box[0] * w)))
                y1 = max(0, min(h, int(self.crop_box[1] * h)))
                x2 = max(0, min(w, int(self.crop_box[2] * w)))
                y2 = max(0, min(h, int(self.crop_box[3] * h)))
                if x2 - x1 >= 2 and y2 - y1 >= 2:
                    pil_img = pil_img.crop((x1, y1, x2, y2))
                    crop_offset = (float(x1), float(y1))
                    crop_size = (int(x2 - x1), int(y2 - y1))
                else:
                    pil_img = None
            except Exception:
                pil_img = None
                crop_offset = (0.0, 0.0)
                crop_size = None
        try:
            result = slide_calibration.calibrate_image(
                pil_img if pil_img is not None else self.image_path,
                spacing_um=self.spacing_um,
                axis_hint=None,
                use_edges=self.use_edges,
                use_large_angles=True,
                progress_cb=self.progress.emit,
            )
        except Exception as exc:
            self.failed.emit(str(exc))
            return
        self.finished.emit(result, self.spacing_um, crop_offset, crop_size)


class _ExportImageWorker(QObject):
    progress = Signal(str, int)
    finished = Signal(str)
    failed = Signal(str)

    def __init__(
        self,
        image_path: str,
        save_path: str,
        suffix: str,
        target_size: tuple[int, int] | None,
        jpeg_quality: int,
        crop_box: tuple[float, float, float, float] | None,
        layers: list[dict],
        result_lines: list[tuple[str, tuple[int, int, int, int]]],
    ) -> None:
        super().__init__()
        self.image_path = image_path
        self.save_path = save_path
        self.suffix = suffix.lower()
        self.target_size = target_size if isinstance(target_size, tuple) and len(target_size) == 2 else None
        self.jpeg_quality = max(1, min(100, int(jpeg_quality or 75)))
        self.crop_box = crop_box
        self.layers = list(layers or [])
        self.result_lines = list(result_lines or [])

    @staticmethod
    def _overlay_color_rgba(color, alpha: int = 180) -> tuple[int, int, int, int]:
        if isinstance(color, tuple):
            if len(color) == 4:
                return color
            if len(color) == 3:
                return (color[0], color[1], color[2], alpha)
        if isinstance(color, str) and color.startswith("#") and len(color) == 7:
            try:
                r = int(color[1:3], 16)
                g = int(color[3:5], 16)
                b = int(color[5:7], 16)
                return (r, g, b, alpha)
            except ValueError:
                pass
        return (231, 76, 60, alpha)

    @Slot()
    def run(self) -> None:
        try:
            if self.suffix == ".svg":
                self._run_svg()
                return
            self.progress.emit("export", 5)
            img = Image.open(self.image_path).convert("RGBA")
            w, h = img.size
            crop_offset = (0, 0)

            self.progress.emit("export", 15)
            if self.crop_box:
                x1 = max(0, min(w, int(min(self.crop_box[0], self.crop_box[2]) * w)))
                y1 = max(0, min(h, int(min(self.crop_box[1], self.crop_box[3]) * h)))
                x2 = max(0, min(w, int(max(self.crop_box[0], self.crop_box[2]) * w)))
                y2 = max(0, min(h, int(max(self.crop_box[1], self.crop_box[3]) * h)))
                if x2 - x1 >= 2 and y2 - y1 >= 2:
                    img = img.crop((x1, y1, x2, y2))
                    crop_offset = (x1, y1)

            if self.layers:
                overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
                px_count = int(img.size[0]) * int(img.size[1])
                if px_count <= 2_000_000:
                    aa_factor = 4
                elif px_count <= 8_000_000:
                    aa_factor = 2
                else:
                    aa_factor = 1
                if aa_factor > 1:
                    overlay_hi = Image.new(
                        "RGBA",
                        (max(1, img.size[0] * aa_factor), max(1, img.size[1] * aa_factor)),
                        (0, 0, 0, 0),
                    )
                    draw = ImageDraw.Draw(overlay_hi, "RGBA")
                else:
                    overlay_hi = None
                    draw = ImageDraw.Draw(overlay, "RGBA")
                total_lines = sum(len(layer.get("lines", [])) for layer in self.layers)
                total_lines = max(1, int(total_lines))
                processed = 0
                self.progress.emit("export", 30)
                for layer in self.layers:
                    lines = layer.get("lines", [])
                    if not lines:
                        continue
                    color = self._overlay_color_rgba(layer.get("color"))
                    width = int(layer.get("width", 3)) if isinstance(layer, dict) else 3
                    show_endcaps = bool(layer.get("show_endcaps", False)) if isinstance(layer, dict) else False
                    for line in lines:
                        if len(line) != 4:
                            continue
                        x1, y1, x2, y2 = line
                        p1 = (float(x1 - crop_offset[0]), float(y1 - crop_offset[1]))
                        p2 = (float(x2 - crop_offset[0]), float(y2 - crop_offset[1]))
                        if aa_factor > 1:
                            p1 = (p1[0] * aa_factor, p1[1] * aa_factor)
                            p2 = (p2[0] * aa_factor, p2[1] * aa_factor)
                        draw.line([p1, p2], fill=color, width=max(1, width * aa_factor))
                        if show_endcaps:
                            dx = p2[0] - p1[0]
                            dy = p2[1] - p1[1]
                            length = (dx * dx + dy * dy) ** 0.5
                            if length > 0:
                                perp_x = -dy / length
                                perp_y = dx / length
                                mark_len = max(5.0 * aa_factor, float(width * aa_factor) * 1.6)
                                for px, py in (p1, p2):
                                    a = (px - perp_x * mark_len, py - perp_y * mark_len)
                                    b = (px + perp_x * mark_len, py + perp_y * mark_len)
                                    draw.line([a, b], fill=color, width=max(1, width * aa_factor))
                        processed += 1
                        if processed % 50 == 0:
                            pct = 30 + int(50 * (processed / total_lines))
                            self.progress.emit("export", max(30, min(80, pct)))
                if overlay_hi is not None:
                    overlay = overlay_hi.resize(img.size, Image.LANCZOS)
                img = Image.alpha_composite(img, overlay)

            self.progress.emit("export", 85)
            CalibrationDialog._draw_export_results_box_from_lines(img, self.result_lines)

            out = img.convert("RGB")
            if self.target_size:
                target_w = max(1, int(self.target_size[0]))
                target_h = max(1, int(self.target_size[1]))
                if out.size != (target_w, target_h):
                    out = out.resize((target_w, target_h), Image.LANCZOS)
            self.progress.emit("export", 95)
            if self.suffix in {".jpg", ".jpeg"}:
                out.save(self.save_path, "JPEG", quality=self.jpeg_quality)
            else:
                out.save(self.save_path, "PNG")
            self.progress.emit("export", 100)
            self.finished.emit(self.save_path)
        except Exception as exc:
            self.failed.emit(str(exc))

    def _run_svg(self) -> None:
        self.progress.emit("export", 5)
        with Image.open(self.image_path) as src:
            img = src.convert("RGBA")
        w, h = img.size
        crop_offset = (0, 0)

        self.progress.emit("export", 15)
        if self.crop_box:
            x1 = max(0, min(w, int(min(self.crop_box[0], self.crop_box[2]) * w)))
            y1 = max(0, min(h, int(min(self.crop_box[1], self.crop_box[3]) * h)))
            x2 = max(0, min(w, int(max(self.crop_box[0], self.crop_box[2]) * w)))
            y2 = max(0, min(h, int(max(self.crop_box[1], self.crop_box[3]) * h)))
            if x2 - x1 >= 2 and y2 - y1 >= 2:
                img = img.crop((x1, y1, x2, y2))
                crop_offset = (x1, y1)

        img_w, img_h = img.size
        target_w, target_h = (img_w, img_h)
        if self.target_size:
            target_w = max(1, int(self.target_size[0]))
            target_h = max(1, int(self.target_size[1]))

        self.progress.emit("export", 30)
        png_buffer = io.BytesIO()
        img.save(png_buffer, format="PNG")
        png_buffer.seek(0)

        total_lines = 0
        for layer in self.layers:
            if isinstance(layer, dict):
                total_lines += len(layer.get("lines", []) or [])
        total_lines = max(1, int(total_lines))
        processed = 0

        def _fmt(v: float) -> str:
            try:
                return f"{float(v):.3f}".rstrip("0").rstrip(".")
            except Exception:
                return "0"

        def _line_svg(x1: float, y1: float, x2: float, y2: float, stroke: str, opacity: float, width: float, dashed: bool) -> str:
            dash_attr = ' stroke-dasharray="6 4"' if dashed else ""
            opacity_attr = ""
            if opacity < 0.999:
                opacity_attr = f' stroke-opacity="{_fmt(opacity)}"'
            return (
                f'<line x1="{_fmt(x1)}" y1="{_fmt(y1)}" x2="{_fmt(x2)}" y2="{_fmt(y2)}" '
                f'stroke="{stroke}"{opacity_attr} stroke-width="{_fmt(width)}" '
                f'stroke-linecap="round"{dash_attr}/>'
            )

        def _write_text(fh, text: str) -> None:
            fh.write(text.encode("utf-8"))

        self.progress.emit("export", 45)
        with Path(self.save_path).open("wb") as fh:
            _write_text(fh, '<?xml version="1.0" encoding="UTF-8"?>\n')
            _write_text(
                fh,
                (
                    f'<svg xmlns="http://www.w3.org/2000/svg" '
                    f'width="{target_w}" height="{target_h}" '
                    f'viewBox="0 0 {img_w} {img_h}">\n'
                ),
            )
            _write_text(
                fh,
                f'<image x="0" y="0" width="{img_w}" height="{img_h}" href="data:image/png;base64,',
            )
            # Stream base64 to avoid building a very large temporary SVG string in memory.
            base64.encode(png_buffer, fh)
            _write_text(fh, '"/>\n')

            for layer in self.layers:
                if not isinstance(layer, dict):
                    continue
                lines = layer.get("lines", []) or []
                if not lines:
                    continue
                rgba = self._overlay_color_rgba(layer.get("color"))
                stroke = f"rgb({int(rgba[0])},{int(rgba[1])},{int(rgba[2])})"
                opacity = max(0.0, min(1.0, float(rgba[3]) / 255.0))
                width = max(0.5, float(layer.get("width", 2.5)))
                dashed = bool(layer.get("dashed", False))
                show_endcaps = bool(layer.get("show_endcaps", False))
                for line in lines:
                    try:
                        if len(line) != 4:
                            continue
                        x1, y1, x2, y2 = (float(line[0]), float(line[1]), float(line[2]), float(line[3]))
                    except Exception:
                        continue
                    x1 -= float(crop_offset[0]); y1 -= float(crop_offset[1])
                    x2 -= float(crop_offset[0]); y2 -= float(crop_offset[1])
                    _write_text(fh, _line_svg(x1, y1, x2, y2, stroke, opacity, width, dashed))
                    _write_text(fh, "\n")

                    if show_endcaps:
                        dx = x2 - x1
                        dy = y2 - y1
                        length = (dx * dx + dy * dy) ** 0.5
                        if length > 0:
                            perp_x = -dy / length
                            perp_y = dx / length
                            mark_len = max(5.0, width * 1.6)
                            for px, py in ((x1, y1), (x2, y2)):
                                ax = px - perp_x * mark_len
                                ay = py - perp_y * mark_len
                                bx = px + perp_x * mark_len
                                by = py + perp_y * mark_len
                                _write_text(fh, _line_svg(ax, ay, bx, by, stroke, opacity, width, dashed=False))
                                _write_text(fh, "\n")

                    processed += 1
                    if processed % 50 == 0:
                        pct = 45 + int(50 * (processed / total_lines))
                        self.progress.emit("export", max(45, min(95, pct)))

            _write_text(fh, "</svg>\n")
        self.progress.emit("export", 100)
        self.finished.emit(self.save_path)


class CalibrationDialog(GeometryMixin, QDialog):
    """Dialog for managing microscope objectives and calibration."""

    calibration_saved = Signal(dict)  # Emits the selected objective data
    _geometry_key = "CalibrationDialog"

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Calibrate Objective"))
        # Ensure modal dialog instances are destroyed after closing; otherwise hidden
        # child dialogs can survive and prompt again during app shutdown.
        self.setAttribute(Qt.WA_DeleteOnClose, True)
        self.setMinimumSize(1300, 900)
        self.resize(1400, 960)

        self.objectives = load_objectives()
        self.target_sampling_pct = float(
            SettingsDB.get_setting("target_sampling_pct", 120.0)
        )
        self.current_objective_key: str | None = None
        self.calibration_images: list[dict] = []  # [{path, pixmap, measurements}]
        self.current_image_index: int = -1
        self._preserve_image_zoom = False
        self.measurement_points: list[QPointF] = []  # Points being drawn
        self.is_measuring = False
        self._modified = False  # Track if user made changes
        self.manual_measure_color = "#3498db"
        self.auto_measure_color = "#e74c3c"
        self.auto_parabola_color = "#b455ff"
        self._auto_crop_active = False
        self._show_auto_debug_overlays = True
        self._auto_manual_notice_shown = False
        self._hint_controller: HintStatusController | None = None
        self._pending_hint_widgets: list[tuple[QWidget, str, str, bool]] = []
        self._auto_worker_thread: QThread | None = None
        self._auto_worker: _AutoCalibrationWorker | None = None
        self._auto_calibration_running = False
        self._export_worker_thread: QThread | None = None
        self._export_worker: _ExportImageWorker | None = None
        self._export_running = False
        self.export_scale_percent: float = 100.0
        self.export_format: str = "png"
        self._close_prompt_bypass = False

        self._init_ui()
        self._load_objectives_combo()
        self._update_history_table()
        self._restore_geometry()
        self.finished.connect(self._save_geometry)

    def _init_ui(self):
        """Initialize the user interface."""
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(8)

        # Top row: Objective selector + Load images button
        top_row = self._build_top_row()
        main_layout.addLayout(top_row)

        # Tab widget for calibration methods
        self.tab_widget = QTabWidget()

        # Tab 1: Calibrate from Image
        image_tab = self._build_image_calibration_tab()
        self.tab_widget.addTab(image_tab, self.tr("Calibrate from Image"))

        # Tab 2: Manual Entry
        manual_tab = self._build_manual_entry_tab()
        self.tab_widget.addTab(manual_tab, self.tr("Manual Entry"))

        main_layout.addWidget(self.tab_widget, 1)

        # Bottom: Calibration history table (give it more space)
        history_group = self._build_history_section()
        main_layout.addWidget(history_group, 0)

        # Action buttons
        button_row = QHBoxLayout()
        button_row.setContentsMargins(0, 0, 0, 0)
        button_row.setSpacing(8)
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
        self.hint_progress_status.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Preferred)
        self.hint_progress_status.setStyleSheet(f"color: #2980b9; font-size: {pt(9)}pt;")
        self.hint_progress_bar = QProgressBar(self)
        self.hint_progress_bar.setRange(0, 100)
        self.hint_progress_bar.setValue(0)
        self.hint_progress_bar.setTextVisible(True)
        self.hint_progress_bar.setFixedHeight(18)
        self.hint_progress_bar.setMinimumWidth(220)
        self.hint_progress_status.setMinimumWidth(220)
        progress_stack_layout.addWidget(self.hint_progress_bar, 0)
        progress_stack_layout.addWidget(self.hint_progress_status, 0)
        hint_progress_layout.addWidget(progress_stack, 0)
        hint_progress_layout.addStretch(1)
        self.hint_progress_widget.setVisible(False)
        hint_area_layout.addWidget(self.hint_progress_widget)

        button_row.addWidget(hint_area, 1)
        self._hint_controller = HintStatusController(self.hint_bar, self)
        if self._pending_hint_widgets:
            for widget, hint, tone, allow_when_disabled in self._pending_hint_widgets:
                if widget is not None:
                    self._hint_controller.register_widget(
                        widget,
                        hint,
                        tone=tone,
                        allow_when_disabled=allow_when_disabled,
                    )
                    widget.setProperty("_hint_registered", True)
            self._pending_hint_widgets.clear()

        self.set_active_btn = QPushButton(self.tr("Set as Active"))
        self.set_active_btn.setMinimumHeight(35)
        self.set_active_btn.clicked.connect(self._on_set_active_calibration)
        button_row.addWidget(self.set_active_btn, 0, Qt.AlignVCenter)
        self._register_hint_widget(self.set_active_btn, self.set_active_btn.text())

        self.delete_cal_btn = QPushButton(self.tr("Delete calibration"))
        self.delete_cal_btn.setMinimumHeight(35)
        self._apply_danger_button_style(self.delete_cal_btn)
        self.delete_cal_btn.clicked.connect(self._delete_selected_calibration)
        button_row.addWidget(self.delete_cal_btn, 0, Qt.AlignVCenter)
        self._register_hint_widget(self.delete_cal_btn, self.delete_cal_btn.text())

        self.save_calibration_btn = QPushButton(self.tr("Save Calibration"))
        self.save_calibration_btn.setMinimumHeight(35)
        self.save_calibration_btn.clicked.connect(self._on_save_calibration)
        button_row.addWidget(self.save_calibration_btn, 0, Qt.AlignVCenter)
        self._register_hint_widget(self.save_calibration_btn, self.save_calibration_btn.text())

        close_btn = QPushButton(self.tr("Close"))
        close_btn.setMinimumHeight(35)
        close_btn.clicked.connect(self.accept)
        button_row.addWidget(close_btn, 0, Qt.AlignVCenter)
        self._register_hint_widget(close_btn, close_btn.text())

        main_layout.addLayout(button_row)
        self._register_existing_tooltips()

        # Delete shortcut - handles both measurement list and history table
        self.delete_shortcut = QShortcut(QKeySequence.Delete, self)
        self.delete_shortcut.activated.connect(self._on_delete_pressed)

    def _build_top_row(self) -> QHBoxLayout:
        """Build the top row with objective selector and load button."""
        row = QHBoxLayout()

        row.addWidget(QLabel(self.tr("Objective:")))

        self.objective_combo = QComboBox()
        self.objective_combo.setMinimumWidth(200)
        self.objective_combo.currentIndexChanged.connect(self._on_objective_changed)
        row.addWidget(self.objective_combo)

        new_objective_btn = QPushButton(self.tr("New Objective..."))
        new_objective_btn.clicked.connect(self._on_new_objective)
        row.addWidget(new_objective_btn)
        self._register_hint_widget(new_objective_btn, self.tr("Add a new objective."))

        edit_objective_btn = QPushButton(self.tr("Edit Objective..."))
        edit_objective_btn.clicked.connect(self._on_edit_objective)
        row.addWidget(edit_objective_btn)
        self._register_hint_widget(edit_objective_btn, self.tr("Edit the selected objective."))

        # Load images button (moved here from left panel)
        load_btn = QPushButton(self.tr("Load image(s)..."))
        load_btn.clicked.connect(self._on_load_images)
        row.addWidget(load_btn)
        self._register_hint_widget(load_btn, self.tr("Load one or more images of calibration slides."))

        delete_objective_btn = QPushButton(self.tr("Delete objective"))
        self._apply_danger_button_style(delete_objective_btn)
        delete_objective_btn.clicked.connect(self._on_delete_objective)
        row.addWidget(delete_objective_btn)

        self.export_btn = QPushButton(self.tr("Export Image"))
        self.export_btn.clicked.connect(self._on_export_image)
        row.addWidget(self.export_btn)
        self._register_hint_widget(self.export_btn, self.tr("Export the current view, with calibration stats."))

        row.addStretch()

        # Active calibration info
        self.active_cal_label = QLabel()
        self.active_cal_label.setStyleSheet("color: #27ae60; font-weight: bold;")
        row.addWidget(self.active_cal_label)

        return row

    def _build_image_calibration_tab(self) -> QWidget:
        """Build the image calibration tab."""
        tab = QWidget()
        layout = QHBoxLayout(tab)
        layout.setSpacing(10)

        # Left panel: Image viewer and image gallery
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)

        # Image viewer (expands with dialog)
        self.image_viewer = ZoomableImageLabel()
        self.image_viewer.setMinimumSize(450, 280)
        self.image_viewer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.image_viewer.set_pan_without_shift(True)
        self.image_viewer.clicked.connect(self._on_image_clicked)
        self.image_viewer.cropChanged.connect(self._on_crop_changed)

        image_container = QWidget()
        image_layout = QGridLayout(image_container)
        image_layout.setContentsMargins(0, 0, 0, 0)
        image_layout.addWidget(self.image_viewer, 0, 0)

        self.zoom_1to1_btn = QPushButton(self.tr("[ 1:1 ]"))
        self.zoom_1to1_btn.setCursor(Qt.PointingHandCursor)
        self.zoom_1to1_btn.setStyleSheet(
            "QPushButton { background-color: rgba(0, 0, 0, 128); color: white; "
            "border: none; border-radius: 4px; padding: 2px 8px; }"
            "QPushButton:hover { background-color: rgba(0, 0, 0, 170); }"
        )
        self.zoom_1to1_btn.clicked.connect(self.image_viewer.set_zoom_1_to_1)
        image_layout.addWidget(self.zoom_1to1_btn, 0, 0, alignment=Qt.AlignTop | Qt.AlignRight)

        left_layout.addWidget(image_container, 1)

        # Gallery for loaded calibration images (fixed height, just above thumbnail size)
        self.image_gallery = ImageGalleryWidget(
            self.tr("Loaded Images"),
            self,
            show_delete=True,
            show_badges=True,
            min_height=100,
            default_height=100,
            thumbnail_size=80,
        )
        self.image_gallery.setFocusPolicy(Qt.StrongFocus)
        self.image_gallery.setFixedHeight(120)  # Thumbnail (80) + title bar + margins
        self.image_gallery.imageClicked.connect(self._on_gallery_image_clicked)
        self.image_gallery.deleteRequested.connect(self._on_gallery_image_deleted)
        left_layout.addWidget(self.image_gallery)

        layout.addWidget(left_panel, 2)

        # Right panel: Auto/manual tabs, results, notes
        right_panel = QWidget()
        right_panel.setFixedWidth(320)
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)

        self.image_mode_tabs = QTabWidget()
        self.image_mode_tabs.currentChanged.connect(self._on_image_mode_tab_changed)

        # Automatic tab
        auto_tab = self._build_auto_calibration_tab()
        self.image_mode_tabs.addTab(auto_tab, self.tr("Automatic"))

        # Manual tab
        manual_tab = QWidget()
        manual_layout = QVBoxLayout(manual_tab)
        manual_layout.setContentsMargins(0, 0, 0, 0)

        # Measurements group
        measurements_group = QGroupBox(self.tr("Calibration Measurements"))
        measurements_layout = QVBoxLayout(measurements_group)

        # Known distance input (mm)
        distance_row = QHBoxLayout()
        distance_row.addWidget(QLabel(self.tr("Known distance:")))
        self.known_distance_input = QDoubleSpinBox()
        self.known_distance_input.setRange(0.01, 1000.0)
        self.known_distance_input.setValue(0.10)
        self.known_distance_input.setSuffix(" mm")
        self.known_distance_input.setDecimals(2)
        self.known_distance_input.setSingleStep(0.01)
        self.known_distance_input.valueChanged.connect(self._update_results)
        distance_row.addWidget(self.known_distance_input)
        distance_row.addStretch()
        measurements_layout.addLayout(distance_row)

        # Measurement list
        self.measurement_list = QListWidget()
        self.measurement_list.setMaximumHeight(90)
        self._register_hint_widget(
            self.measurement_list,
            self.tr("Press Del to remove selected measurement"),
        )
        measurements_layout.addWidget(self.measurement_list)

        # Measurement controls
        controls_row = QHBoxLayout()

        self.add_measurement_btn = QPushButton(self.tr("Add Measurement"))
        self.add_measurement_btn.clicked.connect(self._start_measurement)
        self._register_hint_widget(
            self.add_measurement_btn,
            self.tr(
                "Decide on a distance to measure, then select a start point and end point. "
                "Pick 3 or more distances."
            ),
        )
        controls_row.addWidget(self.add_measurement_btn)
        controls_row.addStretch()

        measurements_layout.addLayout(controls_row)

        manual_layout.addWidget(measurements_group)

        # Results group
        results_group = QGroupBox(self.tr("Results"))
        results_layout = QFormLayout(results_group)

        self.result_average_label = QLabel("--")
        self.result_average_label.setStyleSheet(f"font-weight: bold; font-size: {pt(14)}pt;")
        results_layout.addRow(self.tr("Average:"), self.result_average_label)

        self.result_std_label = QLabel("--")
        results_layout.addRow(self.tr("Std Dev:"), self.result_std_label)

        self.result_ci_label = QLabel("--")
        results_layout.addRow(self.tr("95% CI:"), self.result_ci_label)

        self.result_count_label = QLabel("0")

        # Comparison with auto calibration
        self.comparison_label = QLabel("")
        self.comparison_label.setWordWrap(True)
        results_layout.addRow(self.tr("Compared to auto:"), self.comparison_label)
        self.auto_used_label = QLabel("")
        self.auto_used_label.setStyleSheet("color: #c0392b;")
        results_layout.addRow("", self.auto_used_label)

        manual_layout.addWidget(results_group)
        manual_layout.addStretch()

        self.image_mode_tabs.addTab(manual_tab, self.tr("Manual"))

        right_layout.addWidget(self.image_mode_tabs, 1)

        resize_group = self._build_resize_options_group()
        right_layout.addWidget(resize_group)

        # Notes
        notes_group = QGroupBox(self.tr("Notes"))
        notes_layout = QVBoxLayout(notes_group)
        self.notes_input = QLineEdit()
        self.notes_input.setPlaceholderText(self.tr("Optional notes about this calibration..."))
        notes_layout.addWidget(self.notes_input)
        right_layout.addWidget(notes_group)

        right_layout.addStretch()

        layout.addWidget(right_panel, 1)

        return tab

    def _build_auto_calibration_tab(self) -> QWidget:
        """Build the automatic calibration tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(0, 0, 0, 0)

        input_group = QGroupBox(self.tr("Automatic Calibration"))
        input_layout = QFormLayout(input_group)

        self.auto_division_input = QComboBox()
        self.auto_division_input.addItem(self.tr("0.01 mm (10 µm)"), 0.01)
        self.auto_division_input.addItem(self.tr("0.1 mm (100 µm)"), 0.1)
        self.auto_division_input.setCurrentIndex(0)
        self.auto_division_input.currentIndexChanged.connect(self._on_auto_division_changed)
        input_layout.addRow(self.tr("Division distance:"), self.auto_division_input)
        self._register_hint_widget(
            self.auto_division_input,
            self.tr("The line distance for your calibration slide."),
        )

        # Progress/status are shown in the bottom hint/progress area instead of inside this box.
        self.auto_status_label = QLabel(self.tr("Ready."), input_group)
        self.auto_status_label.setWordWrap(True)
        self.auto_status_label.hide()
        self.auto_progress = QProgressBar(input_group)
        self.auto_progress.setRange(0, 100)
        self.auto_progress.setValue(0)
        self.auto_progress.hide()

        run_row = QHBoxLayout()
        self.auto_crop_btn = QPushButton(self.tr("Crop"))
        self.auto_crop_btn.clicked.connect(self._on_crop_button_clicked)
        self._register_hint_widget(
            self.auto_crop_btn,
            self.tr("If auto detect fails, select an area with horizontal or vertical lines."),
        )
        self.auto_clear_btn = QPushButton(self.tr("Clear"))
        self.auto_clear_btn.clicked.connect(self._on_clear_auto_calibration)
        self._register_hint_widget(
            self.auto_clear_btn,
            self.tr("Remove this. Calibration will run a new calibration."),
        )
        btn_width = max(self.auto_crop_btn.sizeHint().width(), self.auto_clear_btn.sizeHint().width())
        self.auto_crop_btn.setFixedWidth(btn_width)
        self.auto_clear_btn.setFixedWidth(btn_width)
        run_row.addWidget(self.auto_crop_btn)
        run_row.addStretch(1)

        self.auto_run_btn = QPushButton(self.tr("Calibrate"))
        self.auto_run_btn.clicked.connect(self._on_run_auto_calibration)
        self._register_hint_widget(self.auto_run_btn, self.tr("Automatic calibration."))
        run_row.addWidget(self.auto_run_btn)
        run_row.addStretch(1)
        run_row.addWidget(self.auto_clear_btn)

        input_layout.addRow(run_row)

        layout.addWidget(input_group)

        results_group = QGroupBox(self.tr("Results"))
        results_layout = QFormLayout(results_group)

        self.auto_scale_title = QLabel(self.tr("Scale (this image):"))
        self.auto_scale_label = QLabel("--")
        self.auto_scale_label.setStyleSheet("font-weight: bold;")
        results_layout.addRow(self.auto_scale_title, self.auto_scale_label)

        self.auto_scale_current_title = QLabel(self.tr("Scale (this image):"))
        self.auto_scale_current_label = QLabel("--")
        self.auto_scale_current_title.setVisible(False)
        self.auto_scale_current_label.setVisible(False)
        results_layout.addRow(self.auto_scale_current_title, self.auto_scale_current_label)

        scatter_mad_hint = " ".join(self.tr(
            "Median of deviations - how consistent is the spacing between lines?\n"
            "<1%: Manufacturing quality is excellent\n"
            "1-2%: Good, typical for real slides\n"
            "2-5%: Acceptable but check focus issues\n"
            ">5%: Warning - detection errors or poor slide quality"
        ).split())
        self.auto_scatter_mad_title = HintLabel(
            text=self.tr("Scatter MAD:"),
            hint_text=scatter_mad_hint,
            set_hint_callback=self.set_hint,
        )
        self.auto_scatter_mad_label = QLabel("--")
        results_layout.addRow(
            self.auto_scatter_mad_title,
            self._make_value_with_info(
                self.auto_scatter_mad_label,
                scatter_mad_hint,
                title_label=self.auto_scatter_mad_title,
            ),
        )

        scatter_iqr_hint = " ".join(self.tr(
            "IQR is more sensitive to outliers than MAD.\n"
            "<1%: Manufacturing quality is excellent\n"
            "1-2%: Good, typical for real slides\n"
            "2-5%: Acceptable but check focus issues\n"
            ">5%: Warning - detection errors or poor slide quality"
        ).split())
        self.auto_scatter_iqr_title = HintLabel(
            text=self.tr("Scatter IQR:"),
            hint_text=scatter_iqr_hint,
            set_hint_callback=self.set_hint,
        )
        self.auto_scatter_iqr_label = QLabel("--")
        results_layout.addRow(
            self.auto_scatter_iqr_title,
            self._make_value_with_info(
                self.auto_scatter_iqr_label,
                scatter_iqr_hint,
                title_label=self.auto_scatter_iqr_title,
            ),
        )

        residual_hint = " ".join(self.tr(
            "Residual tilt after rotation (close to 0 is best).\n"
            ">0.5 deg suggests rotation mismatch or artifacts."
        ).split())
        self.auto_residual_title = HintLabel(
            text=self.tr("Residual tilt:"),
            hint_text=residual_hint,
            set_hint_callback=self.set_hint,
        )
        self.auto_residual_label = QLabel("--")
        results_layout.addRow(
            self.auto_residual_title,
            self._make_value_with_info(
                self.auto_residual_label,
                residual_hint,
                title_label=self.auto_residual_title,
            ),
        )

        drift_hint = " ".join(self.tr(
            "Does spacing gradually increase/decrease across the image?\n"
            "Slope near 0: constant spacing (good)\n"
            "Positive slope: lines getting farther apart\n"
            "Negative slope: lines getting closer together\n"
            "Unit: px/px (relative change in spacing per pixel across the image)"
        ).split())
        self.auto_drift_title = HintLabel(
            text=self.tr("Drift slope:"),
            hint_text=drift_hint,
            set_hint_callback=self.set_hint,
        )
        self.auto_drift_label = QLabel("--")
        results_layout.addRow(
            self.auto_drift_title,
            self._make_value_with_info(
                self.auto_drift_label,
                drift_hint,
                title_label=self.auto_drift_title,
            ),
        )

        self.auto_angle_label = QLabel("--")
        results_layout.addRow(self.tr("Angle:"), self.auto_angle_label)

        self.auto_dev_title = QLabel(self.tr("Max deviation:"))
        self.auto_dev_label = QLabel("--")
        results_layout.addRow(self.auto_dev_title, self.auto_dev_label)

        self.auto_spread_title = QLabel(self.tr("Image spread:"))
        self.auto_spread_label = QLabel("--")
        results_layout.addRow(self.auto_spread_title, self.auto_spread_label)

        layout.addWidget(results_group)

        layout.addStretch()

        return tab

    def _build_resize_options_group(self) -> QGroupBox:
        group = QGroupBox(self.tr("Image resize"))
        layout = QVBoxLayout(group)
        layout.setContentsMargins(6, 6, 6, 6)

        self.sampling_status_title = HintLabel(
            text=self.tr("Resolution:"),
            hint_text="",
            set_hint_callback=self.set_hint,
            parent=self,
        )
        self.sampling_status_label = QLabel("--")
        self.sampling_status_label.setWordWrap(True)
        layout.addWidget(self._label_with_widget(self.sampling_status_title, self.sampling_status_label))

        self.target_sampling_input = QDoubleSpinBox()
        self.target_sampling_input.setRange(50.0, 300.0)
        self.target_sampling_input.setDecimals(0)
        self.target_sampling_input.setSuffix("%")
        self.target_sampling_input.setValue(float(self.target_sampling_pct))
        self.target_sampling_input.setStyleSheet(
            "QDoubleSpinBox {"
            " background-color: white;"
            " border: 2px solid #e0e0e0;"
            " border-radius: 6px;"
            " padding: 2px 6px;"
            "}"
            "QDoubleSpinBox:focus {"
            " border: 2px solid #3498db;"
            "}"
        )
        self.target_sampling_input.valueChanged.connect(self._on_target_sampling_changed)
        layout.addWidget(
            self._label_with_widget(self.tr("Ideal sampling (% Nyquist):"), self.target_sampling_input)
        )

        info = QFormLayout()
        self.current_resolution_title = QLabel(self.tr("Current resolution:"))
        self.target_resolution_title = QLabel(self.tr("Ideal resolution:"))
        self.current_resolution_label = QLabel("--")
        self.target_resolution_label = QLabel("--")
        info.addRow(self.current_resolution_title, self.current_resolution_label)
        info.addRow(self.target_resolution_title, self.target_resolution_label)
        layout.addLayout(info)
        return group

    def _build_manual_entry_tab(self) -> QWidget:
        """Build the manual entry tab for direct nm/pixel input."""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # Instructions
        instructions = QLabel(
            self.tr(
                "Enter the scale value directly if you know the exact nm/pixel value "
                "for this objective. This will be saved as a calibration record."
            )
        )
        instructions.setWordWrap(True)
        instructions.setStyleSheet("color: #7f8c8d; padding: 10px;")
        layout.addWidget(instructions)

        # Form
        form_group = QGroupBox(self.tr("Scale Value"))
        form_layout = QFormLayout(form_group)

        self.manual_scale_input = QDoubleSpinBox()
        self.manual_scale_input.setRange(1, 100000)
        self.manual_scale_input.setValue(100)
        self.manual_scale_input.setDecimals(2)
        self.manual_scale_input.setSuffix(" nm/pixel")
        form_layout.addRow(self.tr("Scale:"), self.manual_scale_input)

        self.manual_notes_input = QLineEdit()
        self.manual_notes_input.setPlaceholderText(self.tr("Optional notes..."))
        form_layout.addRow(self.tr("Notes:"), self.manual_notes_input)

        layout.addWidget(form_group)

        # Save button for manual entry
        save_manual_btn = QPushButton(self.tr("Save Manual Calibration"))
        save_manual_btn.clicked.connect(self._on_save_manual_calibration)
        layout.addWidget(save_manual_btn)

        layout.addStretch()

        return tab


    def _on_image_mode_tab_changed(self, _index: int):
        """Refresh overlays when switching between auto/manual modes."""
        self._apply_current_overlay()

    def _is_auto_tab_active(self) -> bool:
        if not hasattr(self, "image_mode_tabs"):
            return False
        return self.image_mode_tabs.currentIndex() == 0

    def _auto_use_edges(self) -> bool:
        return True

    def _current_auto_data(self) -> Optional[dict]:
        if self.current_image_index < 0 or self.current_image_index >= len(self.calibration_images):
            return None
        return self.calibration_images[self.current_image_index].get("auto")

    def _auto_overlay_geometry(self, img_data: dict) -> tuple[tuple[int, int], tuple[float, float]]:
        image_size = (0, 0)
        crop_offset = (0.0, 0.0)
        pixmap = img_data.get("pixmap")
        if pixmap:
            image_size = (pixmap.width(), pixmap.height())
        crop_box = img_data.get("crop_box")
        if crop_box and image_size[0] > 0 and image_size[1] > 0:
            source_size = img_data.get("crop_source_size") or image_size
            sw, sh = source_size
            x1 = max(0, min(sw, int(min(crop_box[0], crop_box[2]) * sw)))
            y1 = max(0, min(sh, int(min(crop_box[1], crop_box[3]) * sh)))
            x2 = max(0, min(sw, int(max(crop_box[0], crop_box[2]) * sw)))
            y2 = max(0, min(sh, int(max(crop_box[1], crop_box[3]) * sh)))
            if x2 - x1 >= 2 and y2 - y1 >= 2:
                crop_offset = (float(x1), float(y1))
                image_size = (int(x2 - x1), int(y2 - y1))
        return image_size, crop_offset

    def _ensure_auto_overlays(self, auto_data: dict, img_data: dict) -> None:
        result = auto_data.get("result")
        if not result:
            return
        image_size, crop_offset = self._auto_overlay_geometry(img_data)
        if image_size[0] <= 0 or image_size[1] <= 0:
            return
        if not auto_data.get("overlay_parabola"):
            auto_data["overlay_parabola"] = slide_calibration.build_overlay_lines(
                result, image_size, use_edges=False, origin_offset=crop_offset
            )
        if not auto_data.get("overlay_edges"):
            auto_data["overlay_edges"] = slide_calibration.build_overlay_lines(
                result, image_size, use_edges=True, origin_offset=crop_offset
            )
        if auto_data.get("overlay_edges_50") is None or not auto_data.get("overlay_edges_50"):
            auto_data["overlay_edges_50"] = slide_calibration.build_overlay_edge_lines(
                result, image_size, origin_offset=crop_offset
            )

    def _collect_auto_values(self, use_edges: bool) -> list[float]:
        values: list[float] = []
        for img_data in self.calibration_images:
            auto_data = img_data.get("auto")
            if not auto_data:
                continue
            result = auto_data.get("result")
            if not result:
                continue
            value = result.nm_per_px_edges if use_edges else result.nm_per_px
            if value > 0:
                values.append(float(value))
        return values

    def _update_auto_summary(self):
        if not hasattr(self, "auto_scale_label"):
            return
        values = self._collect_auto_values(self._auto_use_edges())
        if not values:
            self.auto_scale_title.setText(self.tr("Scale (this image):"))
            self.auto_scale_label.setText("--")
            self.auto_dev_label.setText("--")
            if hasattr(self, "auto_spread_label"):
                self.auto_spread_label.setText("--")
                self.auto_spread_label.setStyleSheet("")
            if hasattr(self, "sampling_status_label"):
                self.sampling_status_label.setText("--")
                self._set_hint_for_label_widget(getattr(self, "sampling_status_title", None), "")
            if hasattr(self, "auto_scale_current_title"):
                self.auto_scale_current_title.setVisible(False)
                self.auto_scale_current_label.setVisible(False)
            return
        if len(values) == 1:
            self.auto_scale_title.setText(self.tr("Scale (this image):"))
            self.auto_scale_label.setText(f"{values[0]:.2f} nm/px")
            self.auto_dev_label.setText("--")
            if hasattr(self, "auto_spread_label"):
                self.auto_spread_label.setText("--")
                self.auto_spread_label.setStyleSheet("")
            if hasattr(self, "sampling_status_label"):
                self._update_sampling_label(self.sampling_status_label, values[0])
            if hasattr(self, "auto_scale_current_title"):
                self.auto_scale_current_title.setVisible(False)
                self.auto_scale_current_label.setVisible(False)
            return
        mean = float(np.mean(values))
        max_dev = float(np.max(np.abs(np.array(values) - mean))) if values else 0.0
        self.auto_scale_title.setText(self.tr("Scale (average):"))
        self.auto_scale_label.setText(f"{mean:.2f} nm/px")
        self.auto_dev_label.setText(f"+/-{max_dev:.2f} nm/px")
        if hasattr(self, "auto_scale_current_title"):
            current = None
            current_auto = self._current_auto_data()
            if current_auto:
                result = current_auto.get("result")
                if result:
                    current = result.nm_per_px_edges if self._auto_use_edges() else result.nm_per_px
            if current and current > 0:
                self.auto_scale_current_title.setText(self.tr("Scale (this image):"))
                self.auto_scale_current_label.setText(f"{float(current):.2f} nm/px")
                self.auto_scale_current_title.setVisible(True)
                self.auto_scale_current_label.setVisible(True)
            else:
                self.auto_scale_current_title.setVisible(False)
                self.auto_scale_current_label.setVisible(False)
        if hasattr(self, "auto_spread_label"):
            spread_pct = 100.0 * (max_dev / mean) if mean > 0 else 0.0
            self.auto_spread_label.setText(f"{spread_pct:.2f}%")
            color = "#27ae60" if spread_pct <= 0.5 else "#c0392b"
            self.auto_spread_label.setStyleSheet(f"color: {color}; font-weight: bold;")
        if hasattr(self, "sampling_status_label"):
            self._update_sampling_label(self.sampling_status_label, mean)

    def _make_value_with_info(
        self,
        value_label: QLabel,
        tooltip: str,
        title_label: QLabel | None = None,
    ) -> QWidget:
        wrapper = QWidget()
        layout = QHBoxLayout(wrapper)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)
        layout.addWidget(value_label)
        layout.addStretch()
        if title_label is not None:
            self._register_hint_widget(title_label, tooltip)
        return wrapper

    def _set_hint_for_label_widget(
        self,
        widget: QWidget | None,
        hint_text: str | None,
        tone: str = "info",
    ) -> None:
        if widget is None:
            return
        hint = " ".join((hint_text or "").split())
        if isinstance(widget, HintLabel):
            widget.set_hint_text(hint)
            return
        if hint:
            self._register_hint_widget(widget, hint, tone=tone)
        else:
            widget.setProperty("_hint_text", "")
            widget.setProperty("_hint_tone", (tone or "info").strip().lower())
            widget.setToolTip("")

    def _register_hint_widget(
        self,
        widget: QWidget,
        hint_text: str | None,
        tone: str = "info",
        allow_when_disabled: bool = False,
    ) -> None:
        if not widget:
            return
        hint = " ".join((hint_text or "").split())
        hint_tone = (tone or "info").strip().lower()
        widget.setProperty("_hint_text", hint)
        widget.setProperty("_hint_tone", hint_tone)
        widget.setProperty("_hint_allow_disabled", bool(allow_when_disabled))
        widget.setToolTip("")
        if self._hint_controller is not None:
            already_registered = bool(widget.property("_hint_registered"))
            if not already_registered:
                self._hint_controller.register_widget(
                    widget,
                    hint,
                    tone=hint_tone,
                    allow_when_disabled=allow_when_disabled,
                )
                widget.setProperty("_hint_registered", True)
            return
        if not any(existing is widget for existing, _, _, _ in self._pending_hint_widgets):
            self._pending_hint_widgets.append((widget, hint, hint_tone, bool(allow_when_disabled)))

    def set_hint(self, text: str | None, tone: str = "info") -> None:
        if self._hint_controller is not None:
            self._hint_controller.set_hint(text, tone=tone)

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

    def _register_existing_tooltips(self) -> None:
        for widget in self.findChildren(QWidget):
            tip = (widget.toolTip() or "").strip()
            if tip:
                self._register_hint_widget(widget, tip)

    def _on_auto_division_changed(self, _index: int) -> None:
        if not hasattr(self, "auto_division_input"):
            return
        spacing_mm = float(self.auto_division_input.currentData() or 0.0)
        if spacing_mm <= 0:
            return
        spacing_um = spacing_mm * 1000.0
        updated = False
        for img_data in self.calibration_images:
            auto_data = img_data.get("auto")
            if not auto_data:
                continue
            result = auto_data.get("result")
            if not result:
                continue
            if hasattr(result, "spacing_median_px") and result.spacing_median_px and result.spacing_median_px > 0:
                result.nm_per_px = (spacing_um * 1000.0) / float(result.spacing_median_px)
            if hasattr(result, "spacing_median_edges_px") and result.spacing_median_edges_px and result.spacing_median_edges_px > 0:
                result.nm_per_px_edges = (spacing_um * 1000.0) / float(result.spacing_median_edges_px)
            auto_data["spacing_um"] = spacing_um
            updated = True

        if not updated:
            return

        self._modified = True
        self._render_auto_results(self._current_auto_data())
        self._update_auto_summary()
        self._update_resize_info()
        if hasattr(self, "auto_status_label"):
            self.auto_status_label.setText(self.tr("Division distance updated."))
            self.auto_status_label.setStyleSheet("color: #2980b9;")

    def _label_with_widget(self, label_widget: str | QWidget, widget: QWidget) -> QWidget:
        wrapper = QWidget()
        layout = QHBoxLayout(wrapper)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)
        if isinstance(label_widget, QWidget):
            label = label_widget
        else:
            label = QLabel(label_widget)
        layout.addWidget(label)
        layout.addWidget(widget, 1)
        return wrapper

    def _update_sampling_label(self, label: QLabel, scale_nm_per_px: float) -> None:
        na_value = None
        if self.current_objective_key:
            obj = self.objectives.get(self.current_objective_key, {})
            na_value = obj.get("na")
        if not na_value or not scale_nm_per_px or scale_nm_per_px <= 0:
            label.setText(self.tr("NA not set") if not na_value else "--")
            self._set_hint_for_label_widget(getattr(self, "sampling_status_title", None), "")
            return
        pixels_per_micron = 1000.0 / float(scale_nm_per_px)
        result = get_resolution_status(pixels_per_micron, float(na_value))
        sampling_pct = float(result.get("sampling_pct", 0.0))
        if not np.isfinite(sampling_pct) or sampling_pct <= 0:
            label.setText("--")
            self._set_hint_for_label_widget(getattr(self, "sampling_status_title", None), "")
            return

        score_text = f"{sampling_pct:.0f}%"

        if sampling_pct < 80.0:
            status = self.tr("Undersampled")
            tooltip = self.tr("Camera resolution is too low to resolve all details from this objective.")
        elif sampling_pct <= 150.0:
            status = self.tr("Good")
            tooltip = self.tr("Sampling is close to ideal for this objective.")
        elif sampling_pct < 200.0:
            status = self.tr("Oversampled")
            reduce_pct = max(1.0, min(100.0, 10000.0 / sampling_pct))
            tooltip = self.tr(
                "Image contains more pixels than optical detail.\n"
                "Can likely be scaled to {pct:.0f}% without losing information."
            ).format(pct=reduce_pct)
        elif sampling_pct <= 260.0:
            status = self.tr("Oversampled")
            reduce_pct = max(1.0, min(100.0, 10000.0 / sampling_pct))
            tooltip = self.tr(
                "Image contains more pixels than optical detail.\n"
                "Image can likely be scaled to {pct:.0f}% without losing optical detail."
            ).format(pct=reduce_pct)
        else:
            status = self.tr("Heavily oversampled")
            reduce_pct = max(1.0, min(100.0, 10000.0 / sampling_pct))
            tooltip = self.tr(
                "Image can be reduced to {pct:.0f}% of current size without losing information."
            ).format(pct=reduce_pct)

        summary = format_resolution_summary(pixels_per_micron, float(na_value))
        label.setText(self.tr("Sampling: {status} ({score})").format(status=status, score=score_text))
        self._set_hint_for_label_widget(getattr(self, "sampling_status_title", None), f"{summary}\n\n{tooltip}")

    def _compute_resample_scale_factor(self, scale_um_per_px: float | None) -> float:
        if not scale_um_per_px or scale_um_per_px <= 0:
            return 1.0
        na_value = None
        if self.current_objective_key:
            obj = self.objectives.get(self.current_objective_key, {})
            na_value = obj.get("na")
        if not na_value:
            return 1.0
        target_pct = float(obj.get("target_sampling_pct", self.target_sampling_pct or 120.0))
        pixels_per_micron = 1.0 / float(scale_um_per_px)
        result = get_resolution_status(pixels_per_micron, float(na_value))
        ideal_pixels_per_micron = float(result.get("ideal_pixels_per_micron", 0.0))
        if not ideal_pixels_per_micron or ideal_pixels_per_micron <= 0:
            return 1.0
        target_pixels_per_micron = ideal_pixels_per_micron * (target_pct / 100.0)
        factor = target_pixels_per_micron / pixels_per_micron
        if factor > 1.0:
            factor = 1.0
        return max(0.01, float(factor))

    def _get_resize_scale_um(self) -> float | None:
        if self.current_image_index < 0 or self.current_image_index >= len(self.calibration_images):
            return None
        img_data = self.calibration_images[self.current_image_index]
        auto_data = img_data.get("auto")
        if auto_data:
            result = auto_data.get("result")
            if result:
                value = result.nm_per_px
                if self._auto_use_edges():
                    value = result.nm_per_px_edges or value
                if value and value > 0:
                    return nm_to_um(float(value))
        if not img_data.get("measurements"):
            return None
        all_measurements = self._get_all_measurements()
        if not all_measurements:
            return None
        measurement_tuples = [(m["known_um"], m["measured_px"]) for m in all_measurements]
        mean, _std, _ci_low, _ci_high = calculate_calibration_stats(measurement_tuples)
        if mean and mean > 0:
            return mean
        return None

    def _update_resize_info(self) -> None:
        if not hasattr(self, "current_resolution_label") or not hasattr(self, "target_resolution_label"):
            return
        if self.current_image_index < 0 or self.current_image_index >= len(self.calibration_images):
            self.current_resolution_label.setText("--")
            self.target_resolution_label.setText("--")
            return
        img_data = self.calibration_images[self.current_image_index]
        pixmap = img_data.get("pixmap")
        if not pixmap:
            self.current_resolution_label.setText("--")
            self.target_resolution_label.setText("--")
            return
        width = pixmap.width()
        height = pixmap.height()
        if width <= 0 or height <= 0:
            self.current_resolution_label.setText("--")
            self.target_resolution_label.setText("--")
            return
        mp = (width * height) / 1_000_000.0
        self.current_resolution_label.setText(f"{mp:.1f} MP ({width} × {height})")

        scale_um = self._get_resize_scale_um()
        na_value = None
        if self.current_objective_key:
            obj = self.objectives.get(self.current_objective_key, {})
            na_value = obj.get("na")
        if not scale_um or not na_value:
            self.target_resolution_label.setText("--")
            return
        factor = self._compute_resample_scale_factor(scale_um)
        if factor >= 0.999:
            self.target_resolution_label.setText(self.tr("Same as current"))
            return
        target_w = max(1, int(round(width * factor)))
        target_h = max(1, int(round(height * factor)))
        target_mp = (target_w * target_h) / 1_000_000.0
        self.target_resolution_label.setText(f"{target_mp:.1f} MP ({target_w} × {target_h})")

    def _on_target_sampling_changed(self, value: float) -> None:
        self.target_sampling_pct = float(value)
        SettingsDB.set_setting("target_sampling_pct", float(value))
        if self.current_objective_key and self.current_objective_key in self.objectives:
            self.objectives[self.current_objective_key]["target_sampling_pct"] = float(value)
            save_objectives(self.objectives)
        if hasattr(self, "target_sampling_label"):
            self.target_sampling_label.setText(
                self.tr("{pct:.0f}% of Nyquist").format(pct=float(value))
            )
        self._update_resize_info()

    def _quality_color(self, value: float, good: float, warn: float) -> str:
        if value is None or not np.isfinite(value):
            return ""
        if value < good:
            return "color: #27ae60;"
        if value < warn:
            return "color: #f39c12;"
        return "color: #c0392b;"

    def _quality_color_abs(self, value: float, good: float, warn: float) -> str:
        if value is None or not np.isfinite(value):
            return ""
        v = abs(value)
        if v < good:
            return "color: #27ae60;"
        if v < warn:
            return "color: #f39c12;"
        return "color: #c0392b;"

    def _result_from_dict(self, data: dict) -> slide_calibration.CalibrationResult:
        return slide_calibration.CalibrationResult(
            axis=data.get("axis", "horizontal"),
            angle_deg=float(data.get("angle_deg", 0.0)),
            centers_px=np.array(data.get("centers_px", []), dtype=np.float64),
            centers_edges_px=np.array(data.get("centers_edges_px", []), dtype=np.float64),
            edges_px=np.array(data.get("edges_px", []), dtype=np.float64),
            spacing_median_px=float(data.get("spacing_median_px", float("nan"))),
            spacing_median_edges_px=float(data.get("spacing_median_edges_px", float("nan"))),
            nm_per_px=float(data.get("nm_per_px", float("nan"))),
            nm_per_px_edges=float(data.get("nm_per_px_edges", float("nan"))),
            agreement_pct=float(data.get("agreement_pct", float("nan"))),
            rel_scatter_mad_pct=float(data.get("rel_scatter_mad_pct", float("nan"))),
            rel_scatter_iqr_pct=float(data.get("rel_scatter_iqr_pct", float("nan"))),
            drift_slope=float(data.get("drift_slope", float("nan"))),
            residual_slope_deg=float(data.get("residual_slope_deg", float("nan"))),
        )

    def _render_auto_results(self, auto_data: Optional[dict]):
        if not hasattr(self, "auto_scale_label"):
            return

        if not auto_data:
            self.auto_scale_title.setText(self.tr("Scale (this image):"))
            self.auto_scale_label.setText("--")
            self.auto_scatter_mad_label.setText("--")
            self.auto_scatter_mad_label.setStyleSheet("")
            self.auto_scatter_iqr_label.setText("--")
            self.auto_scatter_iqr_label.setStyleSheet("")
            self.auto_residual_label.setText("--")
            self.auto_residual_label.setStyleSheet("")
            self.auto_drift_label.setText("--")
            self.auto_drift_label.setStyleSheet("")
            self.auto_angle_label.setText("--")
            if hasattr(self, "auto_status_label"):
                self.auto_status_label.setText(self.tr("Ready."))
                self.auto_status_label.setStyleSheet("")
            if hasattr(self, "auto_progress"):
                self.auto_progress.setValue(0)
            self._update_auto_summary()
            return

        result = auto_data["result"]
        self.auto_scatter_mad_label.setText(f"{result.rel_scatter_mad_pct:.2f}%")
        self.auto_scatter_mad_label.setStyleSheet(
            self._quality_color(result.rel_scatter_mad_pct, good=1.0, warn=2.0)
        )
        self.auto_scatter_iqr_label.setText(f"{result.rel_scatter_iqr_pct:.2f}%")
        self.auto_scatter_iqr_label.setStyleSheet(
            self._quality_color(result.rel_scatter_iqr_pct, good=1.0, warn=2.0)
        )
        self.auto_residual_label.setText(f"{result.residual_slope_deg:.3f} deg")
        self.auto_residual_label.setStyleSheet(
            self._quality_color_abs(result.residual_slope_deg, good=0.2, warn=0.5)
        )
        self.auto_drift_label.setText(f"{result.drift_slope:.4g} px/px")
        self.auto_drift_label.setStyleSheet(
            self._quality_color_abs(result.drift_slope, good=0.001, warn=0.003)
        )
        self.auto_angle_label.setText(f"{result.angle_deg:.3f} deg")

        self._update_auto_summary()

    def _apply_current_overlay(self):
        """Apply the correct overlay based on the selected tab and method."""
        if self.current_image_index < 0 or self.current_image_index >= len(self.calibration_images):
            self.image_viewer.set_measurement_lines([])
            self.image_viewer.set_debug_lines([])
            return

        if self._is_auto_tab_active():
            auto_data = self._current_auto_data()
            if not auto_data:
                self.image_viewer.set_measurement_lines([])
                self.image_viewer.set_debug_lines([])
                return
            img_data = self.calibration_images[self.current_image_index]
            self._ensure_auto_overlays(auto_data, img_data)
            use_edges = self._auto_use_edges()
            self.image_viewer.set_measurement_lines([])
            self.image_viewer.set_debug_lines([])
            if self._show_auto_debug_overlays:
                layers = []
                if use_edges:
                    edge_lines = auto_data.get("overlay_edges_50")
                    if edge_lines is None:
                        edge_lines = auto_data.get("overlay_edges", [])
                    if edge_lines:
                        layers.append({
                            "lines": edge_lines,
                            "color": (241, 196, 15, 200),
                            "width": 2,
                            "composition": "overlay",
                        })
                    edge_center_lines = auto_data.get("overlay_edges", [])
                    if edge_center_lines:
                        layers.append({
                            "lines": edge_center_lines,
                            "color": (255, 0, 0, 200),
                            "width": 3,
                            "composition": "screen",
                        })
                else:
                    parabola_lines = auto_data.get("overlay_parabola", [])
                    if parabola_lines:
                        layers.append({
                            "lines": parabola_lines,
                            "color": (186, 85, 255, 170),
                            "width": 3,
                            "composition": "screen",
                        })
                self.image_viewer.set_debug_lines(layers)
            else:
                self.image_viewer.set_measurement_color(
                    self.auto_measure_color if use_edges else self.auto_parabola_color
                )
                self.image_viewer.set_show_line_endcaps(False)
                lines = auto_data["overlay_edges"] if use_edges else auto_data["overlay_parabola"]
                self.image_viewer.set_measurement_lines(lines)
            return

        self.image_viewer.set_debug_lines([])
        self.image_viewer.set_measurement_color(self.manual_measure_color)
        self.image_viewer.set_show_line_endcaps(True)
        self._update_measurement_lines()

    def _reset_auto_results(
        self,
        status_text: Optional[str] = None,
        status_color: Optional[str] = None,
    ):
        """Clear automatic calibration results and overlays."""
        if self.current_image_index >= 0 and self.current_image_index < len(self.calibration_images):
            img_data = self.calibration_images[self.current_image_index]
            if "auto" in img_data:
                img_data.pop("auto", None)
                self._modified = True

        if hasattr(self, "auto_scale_label"):
            self.auto_scale_title.setText(self.tr("Scale (this image):"))
            self.auto_scale_label.setText("--")
            self.auto_scatter_mad_label.setText("--")
            self.auto_scatter_iqr_label.setText("--")
            self.auto_residual_label.setText("--")
            self.auto_drift_label.setText("--")
            self.auto_angle_label.setText("--")

        if hasattr(self, "auto_progress"):
            self.auto_progress.setValue(0)

        if hasattr(self, "auto_status_label"):
            self.auto_status_label.setText(status_text or self.tr("Ready."))
            if status_color:
                self.auto_status_label.setStyleSheet(f"color: {status_color};")
            else:
                self.auto_status_label.setStyleSheet("")

        if not self._auto_calibration_running:
            self._set_hint_progress_visible(False)
            self._set_hint_progress("", 0)

        self._update_auto_summary()
        self._update_resize_info()
        self._apply_current_overlay()

    def _set_auto_results(
        self,
        result: slide_calibration.CalibrationResult,
        spacing_um: float,
        crop_offset: tuple[float, float] = (0.0, 0.0),
        crop_size: Optional[tuple[int, int]] = None,
    ):
        """Populate automatic calibration UI and overlays."""
        img_data = self.calibration_images[self.current_image_index]
        pixmap = img_data["pixmap"]
        if crop_size is None:
            image_size = (pixmap.width(), pixmap.height())
        else:
            image_size = crop_size

        auto_data = {
            "result": result,
            "spacing_um": spacing_um,
            "overlay_parabola": slide_calibration.build_overlay_lines(
                result, image_size, use_edges=False, origin_offset=crop_offset
            ),
            "overlay_edges": slide_calibration.build_overlay_lines(
                result, image_size, use_edges=True, origin_offset=crop_offset
            ),
            "overlay_edges_50": slide_calibration.build_overlay_edge_lines(
                result, image_size, origin_offset=crop_offset
            ),
        }
        img_data["auto"] = auto_data
        self._modified = True
        self._render_auto_results(auto_data)

        self.auto_status_label.setText(self.tr("Calibration complete."))
        self.auto_status_label.setStyleSheet("color: #27ae60;")
        if hasattr(self, "auto_progress"):
            self.auto_progress.setValue(100)
        self._update_resize_info()
        self._apply_current_overlay()

    def _on_clear_auto_calibration(self):
        """Clear auto calibration results for the current image."""
        if self._auto_calibration_running:
            return
        if not self.calibration_images or self.current_image_index < 0:
            return
        img_data = self.calibration_images[self.current_image_index]
        img_data.pop("crop_box", None)
        img_data.pop("crop_source_size", None)
        self.image_viewer.set_crop_box(None)
        self._set_auto_crop_active(False)
        self._refresh_image_gallery()
        self._reset_auto_results(status_text=self.tr("Auto calibration cleared."))

    def _on_run_auto_calibration(self):
        """Run automatic calibration on the current image."""
        if self._export_running:
            return
        if self._auto_calibration_running:
            return
        if not self.calibration_images or self.current_image_index < 0:
            QMessageBox.information(
                self,
                self.tr("No Image"),
                self.tr("Please load a calibration image first."),
            )
            return

        spacing_mm = float(self.auto_division_input.currentData())
        if spacing_mm <= 0:
            QMessageBox.warning(
                self,
                self.tr("Invalid Distance"),
                self.tr("Please enter a valid division distance."),
            )
            return

        img_data = self.calibration_images[self.current_image_index]
        image_path = img_data.get("path")
        if not image_path or not Path(image_path).exists():
            QMessageBox.warning(
                self,
                self.tr("Missing Image"),
                self.tr("The selected image could not be found on disk."),
            )
            return

        self._reset_auto_results(status_text=self.tr("Running..."), status_color="#2980b9")
        self._set_hint_progress_visible(True)
        self._set_hint_progress(self.tr("Running..."), 0)
        spacing_um = float(spacing_mm) * 1000.0
        crop_box = img_data.get("crop_box")
        normalized_crop_box = None
        if isinstance(crop_box, (tuple, list)) and len(crop_box) == 4:
            try:
                normalized_crop_box = tuple(float(v) for v in crop_box)
            except (TypeError, ValueError):
                normalized_crop_box = None
        self._set_auto_controls_enabled(False)
        self._auto_calibration_running = True

        thread = QThread(self)
        worker = _AutoCalibrationWorker(
            image_path=image_path,
            spacing_um=spacing_um,
            use_edges=self._auto_use_edges(),
            crop_box=normalized_crop_box,
        )
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.progress.connect(self._update_auto_progress)
        worker.finished.connect(self._on_auto_calibration_finished)
        worker.failed.connect(self._on_auto_calibration_failed)
        worker.finished.connect(thread.quit)
        worker.failed.connect(thread.quit)
        worker.finished.connect(worker.deleteLater)
        worker.failed.connect(worker.deleteLater)
        thread.finished.connect(self._on_auto_worker_thread_finished)
        thread.finished.connect(thread.deleteLater)

        self._auto_worker_thread = thread
        self._auto_worker = worker
        thread.start()

    @Slot(object, float, object, object)
    def _on_auto_calibration_finished(
        self,
        result: object,
        spacing_um: float,
        crop_offset: object,
        crop_size: object,
    ) -> None:
        self._set_hint_progress(self.tr("Calibration complete."), 100)
        offset = crop_offset if isinstance(crop_offset, tuple) and len(crop_offset) == 2 else (0.0, 0.0)
        size = crop_size if isinstance(crop_size, tuple) and len(crop_size) == 2 else None
        self._set_auto_results(result, float(spacing_um), crop_offset=offset, crop_size=size)

    @Slot(str)
    def _on_auto_calibration_failed(self, error_text: str) -> None:
        self._set_hint_progress(
            self.tr("Auto calibration failed: {err}").format(err=error_text),
            0,
        )
        self._reset_auto_results(
            status_text=self.tr("Auto calibration failed: {err}").format(err=error_text),
            status_color="#c0392b",
        )

    @Slot()
    def _on_auto_worker_thread_finished(self) -> None:
        self._auto_worker = None
        self._auto_worker_thread = None
        self._auto_calibration_running = False
        self._set_auto_controls_enabled(True)
        self._set_hint_progress_visible(False)

    def _set_auto_controls_enabled(self, enabled: bool) -> None:
        if hasattr(self, "auto_run_btn"):
            self.auto_run_btn.setEnabled(enabled)
        if hasattr(self, "auto_crop_btn"):
            self.auto_crop_btn.setEnabled(enabled)
        if hasattr(self, "auto_clear_btn"):
            self.auto_clear_btn.setEnabled(enabled)
        if hasattr(self, "auto_division_input"):
            self.auto_division_input.setEnabled(enabled)
        if hasattr(self, "image_gallery"):
            self.image_gallery.setEnabled(enabled)
        if hasattr(self, "objective_combo"):
            self.objective_combo.setEnabled(enabled)
        if hasattr(self, "export_btn"):
            self.export_btn.setEnabled(enabled)

    def _build_history_section(self) -> QGroupBox:
        """Build the calibration history table section."""
        group = QGroupBox(self.tr("Calibration History"))
        layout = QVBoxLayout(group)

        self.history_table = QTableWidget(0, 12)
        self.history_table.setHorizontalHeaderLabels([
            self.tr("Date"),
            self.tr("nm/px"),
            self.tr("MP"),
            self.tr("n"),
            self.tr("Diff%"),
            self.tr("MAD%"),
            self.tr("IQR%"),
            self.tr("Residual tilt"),
            self.tr("Observations"),
            self.tr("Camera"),
            self.tr("Active"),
            self.tr("Notes"),
        ])
        # Set column resize modes
        header = self.history_table.horizontalHeader()
        # Date - fixed width
        header.setSectionResizeMode(0, QHeaderView.Fixed)
        self.history_table.setColumnWidth(0, 120)
        # Data columns - resize to contents
        for col in range(1, 11):
            header.setSectionResizeMode(col, QHeaderView.ResizeToContents)
        # Notes - stretch to fill remaining space
        header.setSectionResizeMode(11, QHeaderView.Stretch)
        self.history_table.verticalHeader().setVisible(False)
        self.history_table.verticalHeader().setDefaultSectionSize(26)
        self.history_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.history_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.history_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.history_table.setMinimumHeight(110)
        self.history_table.setMaximumHeight(160)
        self.history_table.setStyleSheet(
            "QTableWidget::item:selected { background: #d9e9f8; color: #1f2d3d; }"
            "QTableWidget::item:selected:!active { background: #eaf3ff; color: #1f2d3d; }"
        )
        # Click row to view calibration
        self.history_table.cellClicked.connect(self._on_history_row_clicked)

        layout.addWidget(self.history_table)

        return group

    def _load_objectives_combo(self):
        """Load objectives into the combo box."""
        self.objective_combo.clear()
        for key, obj in sorted(self.objectives.items(), key=lambda item: objective_sort_value(item[1], item[0])):
            display_name = objective_display_name(obj, key)
            self.objective_combo.addItem(display_name or key, key)

        if self.objective_combo.count() > 0:
            self.objective_combo.setCurrentIndex(0)
            self._on_objective_changed()

    def _ask_unsaved_changes(self) -> QMessageBox.StandardButton:
        dialog = QDialog(self)
        dialog.setWindowTitle(self.tr("Unsaved Changes"))
        dialog.setModal(True)
        dialog.setWindowFlags(
            Qt.Dialog
            | Qt.CustomizeWindowHint
            | Qt.WindowTitleHint
            | Qt.WindowCloseButtonHint
        )
        dialog.setStyleSheet(
            "QPushButton { min-width: 92px; min-height: 35px; padding: 6px 10px; }"
        )

        outer = QVBoxLayout(dialog)
        outer.setContentsMargins(16, 14, 16, 12)
        outer.setSpacing(12)
        outer.setSizeConstraint(QLayout.SetMinimumSize)

        message_row = QHBoxLayout()
        message_row.setContentsMargins(0, 0, 0, 0)
        message_row.setSpacing(12)

        icon_label = QLabel(dialog)
        icon = self.style().standardIcon(QStyle.SP_MessageBoxQuestion)
        icon_label.setPixmap(icon.pixmap(48, 48))
        icon_label.setAlignment(Qt.AlignHCenter | Qt.AlignTop)
        icon_label.setFixedWidth(56)
        message_row.addWidget(icon_label, 0, Qt.AlignTop)

        text_label = QLabel(
            self.tr("You have unsaved calibration measurements. What would you like to do?"),
            dialog,
        )
        text_label.setWordWrap(True)
        text_label.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        text_label.setMinimumWidth(460)
        text_label.setMaximumWidth(640)
        text_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        message_row.addWidget(text_label, 1)
        outer.addLayout(message_row)

        buttons = QDialogButtonBox(dialog)
        discard_btn = buttons.addButton(self.tr("Discard"), QDialogButtonBox.DestructiveRole)
        cancel_btn = buttons.addButton(self.tr("Cancel"), QDialogButtonBox.RejectRole)
        save_btn = buttons.addButton(self.tr("Save"), QDialogButtonBox.AcceptRole)
        save_btn.setDefault(True)
        save_btn.setAutoDefault(True)
        outer.addWidget(buttons)

        choice = {"value": QMessageBox.Cancel}

        def _choose(value: QMessageBox.StandardButton) -> None:
            choice["value"] = value
            dialog.done(int(QDialog.Accepted))

        save_btn.clicked.connect(lambda: _choose(QMessageBox.Save))
        discard_btn.clicked.connect(lambda: _choose(QMessageBox.Discard))
        cancel_btn.clicked.connect(lambda: _choose(QMessageBox.Cancel))
        buttons.rejected.connect(dialog.reject)

        dialog.setMinimumWidth(620)
        dialog.setMinimumHeight(185)
        dialog.adjustSize()
        hint = dialog.sizeHint()
        dialog.resize(max(620, min(780, hint.width() + 12)), max(190, hint.height() + 8))

        if dialog.exec() != QDialog.Accepted:
            return QMessageBox.Cancel
        return choice["value"]

    def _show_wrapped_info_dialog(
        self,
        title: str,
        text: str,
        min_width: int = 560,
        max_width: int = 700,
        min_height: int = 240,
    ) -> None:
        """Show an information dialog with reliable wrapping/sizing on Linux."""
        box = QMessageBox(self)
        box.setIcon(QMessageBox.Information)
        box.setWindowTitle(title)
        box.setText(text)
        ok_btn = box.addButton(QMessageBox.Ok)
        if ok_btn is not None:
            ok_btn.setMinimumHeight(35)
        box.setDefaultButton(ok_btn)
        box.setEscapeButton(ok_btn)
        box.setStyleSheet(
            "QLabel#qt_msgboxex_icon_label { min-width: 0px; max-width: 56px; }"
            "QLabel#qt_msgbox_label { min-width: 380px; max-width: 520px; }"
            "QLabel#qt_msgbox_informativelabel { min-width: 380px; max-width: 520px; }"
        )
        text_label = box.findChild(QLabel, "qt_msgbox_label")
        if text_label is not None:
            text_label.setWordWrap(True)
            text_label.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        info_label = box.findChild(QLabel, "qt_msgbox_informativelabel")
        if info_label is not None:
            info_label.setWordWrap(True)
            info_label.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        box.setMinimumWidth(min_width)
        box.setMaximumWidth(max_width)
        box.setMinimumHeight(min_height)
        box.exec()

    def _apply_danger_button_style(self, button: QPushButton | None) -> None:
        """Apply a red destructive-button style with hover/pressed states."""
        if button is None:
            return
        button.setStyleSheet(
            "QPushButton {"
            " background-color: #e74c3c;"
            " color: white;"
            " border: none;"
            " border-radius: 6px;"
            "}"
            "QPushButton:hover { background-color: #c0392b; }"
            "QPushButton:pressed { background-color: #a93226; }"
            "QPushButton:disabled { background-color: #bdc3c7; color: #7f8c8d; }"
        )

    def _ask_wrapped_confirm_dialog(
        self,
        title: str,
        text: str,
        *,
        parent_widget: QWidget | None = None,
        accept_text: str | None = None,
        cancel_text: str | None = None,
        destructive: bool = False,
        min_width: int = 620,
        max_width: int = 780,
        min_height: int = 210,
    ) -> bool:
        """Show a wrapped Yes/Cancel confirmation dialog with reliable Linux sizing."""
        host = parent_widget if parent_widget is not None else self
        dialog = QDialog(host)
        dialog.setWindowTitle(title)
        dialog.setModal(True)
        dialog.setWindowFlags(
            Qt.Dialog
            | Qt.CustomizeWindowHint
            | Qt.WindowTitleHint
            | Qt.WindowCloseButtonHint
        )
        dialog.setStyleSheet(
            "QPushButton { min-width: 92px; min-height: 35px; padding: 6px 10px; }"
        )

        outer = QVBoxLayout(dialog)
        outer.setContentsMargins(16, 14, 16, 12)
        outer.setSpacing(12)
        outer.setSizeConstraint(QLayout.SetMinimumSize)

        row = QHBoxLayout()
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(12)

        icon_label = QLabel(dialog)
        icon = self.style().standardIcon(QStyle.SP_MessageBoxQuestion)
        icon_label.setPixmap(icon.pixmap(48, 48))
        icon_label.setAlignment(Qt.AlignHCenter | Qt.AlignTop)
        icon_label.setFixedWidth(56)
        row.addWidget(icon_label, 0, Qt.AlignTop)

        text_label = QLabel(text, dialog)
        text_label.setWordWrap(True)
        text_label.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        text_label.setMinimumWidth(460)
        text_label.setMaximumWidth(max(460, max_width - 120))
        text_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        row.addWidget(text_label, 1)
        outer.addLayout(row)

        buttons = QDialogButtonBox(dialog)
        cancel_btn = buttons.addButton(cancel_text or self.tr("Cancel"), QDialogButtonBox.RejectRole)
        accept_role = QDialogButtonBox.DestructiveRole if destructive else QDialogButtonBox.AcceptRole
        accept_btn = buttons.addButton(accept_text or self.tr("Yes"), accept_role)
        accept_btn.setDefault(True)
        accept_btn.setAutoDefault(True)
        outer.addWidget(buttons)

        accepted = {"value": False}

        def _accept() -> None:
            accepted["value"] = True
            dialog.accept()

        accept_btn.clicked.connect(_accept)
        cancel_btn.clicked.connect(dialog.reject)
        buttons.rejected.connect(dialog.reject)

        dialog.setMinimumWidth(min_width)
        dialog.setMinimumHeight(min_height)
        dialog.adjustSize()
        hint = dialog.sizeHint()
        dialog.resize(max(min_width, min(max_width, hint.width() + 12)), max(min_height, hint.height() + 8))
        dialog.exec()
        return bool(accepted["value"])

    def _on_objective_changed(self):
        """Handle objective selection change."""
        new_objective_key = self.objective_combo.currentData()
        if not new_objective_key:
            return

        # Check for unsaved changes before switching
        if self._has_unsaved_changes() and new_objective_key != self.current_objective_key:
            reply = self._ask_unsaved_changes()
            if reply == QMessageBox.Save:
                self._on_save_calibration()
                # After saving, continue to switch objective
            elif reply == QMessageBox.Cancel:
                # Revert to previous selection
                self.objective_combo.blockSignals(True)
                for i in range(self.objective_combo.count()):
                    if self.objective_combo.itemData(i) == self.current_objective_key:
                        self.objective_combo.setCurrentIndex(i)
                        break
                self.objective_combo.blockSignals(False)
                return
            # Discard: continue to switch

        self.current_objective_key = new_objective_key

        # Update active calibration label (in nm/px)
        active_cal = CalibrationDB.get_active_calibration(self.current_objective_key)
        if active_cal:
            scale_um = active_cal.get("microns_per_pixel", 0)
            scale_nm = um_to_nm(scale_um)
            date = active_cal.get("calibration_date", "")[:10]
            self.active_cal_label.setText(
                self.tr("Active: {scale:.2f} nm/px ({date})").format(scale=scale_nm, date=date)
            )
            # Set manual entry to current value (in nm)
            self.manual_scale_input.setValue(scale_nm)
        else:
            # Fall back to objectives.json value
            obj = self.objectives.get(self.current_objective_key, {})
            scale_um = obj.get("microns_per_pixel", 0)
            scale_nm = um_to_nm(scale_um)
            self.active_cal_label.setText(
                self.tr("From config: {scale:.2f} nm/px").format(scale=scale_nm)
            )
            self.manual_scale_input.setValue(scale_nm)

        # Update history table
        self._update_history_table()

        # Clear current calibration state
        self._clear_all()

        if self.current_objective_key and self.current_objective_key in self.objectives:
            obj = self.objectives.get(self.current_objective_key, {})
            self.target_sampling_pct = float(obj.get("target_sampling_pct", self.target_sampling_pct))
            if hasattr(self, "target_sampling_input"):
                self.target_sampling_input.blockSignals(True)
                self.target_sampling_input.setValue(float(self.target_sampling_pct))
                self.target_sampling_input.blockSignals(False)
            if hasattr(self, "target_sampling_label"):
                self.target_sampling_label.setText(
                    self.tr("{pct:.0f}% of Nyquist").format(pct=float(self.target_sampling_pct))
                )
            self._update_resize_info()

    def _on_new_objective(self):
        """Create a new objective using the full dialog."""
        existing_names = set(self.objectives.keys())
        for key, obj in self.objectives.items():
            display_name = objective_display_name(obj, key)
            if display_name:
                existing_names.add(display_name)
        dialog = NewObjectiveDialog(self, sorted(existing_names))
        if dialog.exec() == QDialog.Accepted:
            data = dialog.get_objective_data()
            key = data["key"]

            self.objectives[key] = {
                "name": data["name"],
                "objective_name": data["objective_name"],
                "magnification": data["magnification"],
                "na": data["na"],
                "microns_per_pixel": data["microns_per_pixel"],
                "notes": data["notes"],
            }
            save_objectives(self.objectives)
            self._load_objectives_combo()

            # Select the new objective
            idx = self.objective_combo.findData(key)
            if idx >= 0:
                self.objective_combo.setCurrentIndex(idx)

    def _on_edit_objective(self) -> None:
        """Edit the currently selected objective definition."""
        if not self.current_objective_key:
            return
        obj = self.objectives.get(self.current_objective_key)
        if not obj:
            return

        existing_names = set(self.objectives.keys())
        existing_names.discard(self.current_objective_key)
        for key, entry in self.objectives.items():
            if key == self.current_objective_key:
                continue
            display_name = objective_display_name(entry, key)
            if display_name:
                existing_names.add(display_name)

        dialog = NewObjectiveDialog(
            self,
            sorted(existing_names),
            objective_data=obj,
            objective_key=self.current_objective_key,
            edit_mode=True,
        )
        if dialog.exec() == QDialog.Accepted:
            data = dialog.get_objective_data()
            key = self.current_objective_key
            self.objectives[key] = {
                "name": data["name"],
                "objective_name": data["objective_name"],
                "magnification": data["magnification"],
                "na": data["na"],
                "microns_per_pixel": self.objectives.get(key, {}).get("microns_per_pixel", data["microns_per_pixel"]),
                "notes": data["notes"],
            }
            save_objectives(self.objectives)
            self._load_objectives_combo()
            idx = self.objective_combo.findData(key)
            if idx >= 0:
                self.objective_combo.setCurrentIndex(idx)

    def _on_delete_objective(self) -> None:
        """Delete the currently selected objective definition."""
        if not self.current_objective_key:
            return

        images_using = CalibrationDB.get_images_using_objective(self.current_objective_key)
        if images_using:
            if not self._show_objective_in_use_dialog(self.current_objective_key, images_using):
                return

        if not images_using:
            if not self._ask_wrapped_confirm_dialog(
                self.tr("Delete Objective"),
                self.tr("Objective will be deleted.\n\nThis action cannot be undone."),
                destructive=True,
                accept_text=self.tr("Yes"),
            ):
                return

        CalibrationDB.clear_objective_usage(self.current_objective_key)
        failures = CalibrationDB.delete_calibrations_for_objective(self.current_objective_key)
        self._warn_delete_failures(failures)
        self.objectives.pop(self.current_objective_key, None)
        save_objectives(self.objectives)
        self._clear_all()
        self._load_objectives_combo()
        if self.objective_combo.count() == 0:
            self.current_objective_key = None
            self.active_cal_label.setText("")
            self._update_history_table()

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

    def _on_load_images(self):
        """Load calibration target images (multi-select)."""
        paths, _ = QFileDialog.getOpenFileNames(
            self,
            self.tr("Select Calibration Images"),
            "",
            self.tr("Images (*.png *.jpg *.jpeg *.tif *.tiff);;All Files (*)"),
        )
        for path in paths:
            self._add_calibration_image(path)

    def _overlay_color_rgba(self, color, alpha: int = 180) -> tuple[int, int, int, int]:
        if isinstance(color, tuple):
            if len(color) == 4:
                return color
            if len(color) == 3:
                return (color[0], color[1], color[2], alpha)
        if isinstance(color, str) and color.startswith("#") and len(color) == 7:
            try:
                r = int(color[1:3], 16)
                g = int(color[3:5], 16)
                b = int(color[5:7], 16)
                return (r, g, b, alpha)
            except ValueError:
                pass
        return (231, 76, 60, alpha)

    @staticmethod
    def _label_color_rgba(label: QLabel, default: tuple[int, int, int, int] = (44, 62, 80, 255)) -> tuple[int, int, int, int]:
        style = label.styleSheet() if hasattr(label, "styleSheet") else ""
        if not style:
            return default
        match = re.search(r"color\s*:\s*([^;]+)", style, flags=re.IGNORECASE)
        if not match:
            return default
        token = (match.group(1) or "").strip().lower()
        if token.startswith("#") and len(token) == 7:
            try:
                return (int(token[1:3], 16), int(token[3:5], 16), int(token[5:7], 16), 255)
            except ValueError:
                return default
        named = {
            "black": (0, 0, 0, 255),
            "white": (255, 255, 255, 255),
            "red": (255, 0, 0, 255),
            "green": (0, 128, 0, 255),
            "orange": (255, 165, 0, 255),
        }
        return named.get(token, default)

    def _collect_export_result_lines(self) -> list[tuple[str, tuple[int, int, int, int]]]:
        lines: list[tuple[str, tuple[int, int, int, int]]] = []

        def _append(title: str, value_label: QLabel) -> None:
            value = (value_label.text() or "").strip()
            if not value or value == "--":
                return
            color = self._label_color_rgba(value_label)
            lines.append((f"{title} {value}".strip(), color))

        if self._is_auto_tab_active():
            _append(self.auto_scale_title.text(), self.auto_scale_label)
            if self.auto_scale_current_title.isVisible():
                _append(self.auto_scale_current_title.text(), self.auto_scale_current_label)
            _append(self.auto_scatter_mad_title.text(), self.auto_scatter_mad_label)
            _append(self.auto_scatter_iqr_title.text(), self.auto_scatter_iqr_label)
            _append(self.auto_residual_title.text(), self.auto_residual_label)
            _append(self.auto_drift_title.text(), self.auto_drift_label)
            _append(self.tr("Angle:"), self.auto_angle_label)
            _append(self.auto_dev_title.text(), self.auto_dev_label)
            _append(self.auto_spread_title.text(), self.auto_spread_label)
        else:
            _append(self.tr("Average:"), self.result_average_label)
            _append(self.tr("Std Dev:"), self.result_std_label)
            _append(self.tr("95% CI:"), self.result_ci_label)
            if hasattr(self, "result_count_label"):
                value = (self.result_count_label.text() or "").strip()
                if value:
                    lines.append((f"{self.tr('Count:')} {value}", self._label_color_rgba(self.result_count_label)))
            if hasattr(self, "comparison_label"):
                value = (self.comparison_label.text() or "").strip()
                if value and value != "--":
                    lines.append(
                        (
                            f"{self.tr('Compared to auto:')} {value}",
                            self._label_color_rgba(self.comparison_label),
                        )
                    )
            if hasattr(self, "auto_used_label"):
                value = (self.auto_used_label.text() or "").strip()
                if value:
                    lines.append((value, self._label_color_rgba(self.auto_used_label)))
        return lines

    @staticmethod
    def _export_font(size_px: int, bold: bool = False):
        size = max(10, int(size_px))
        try:
            face = "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"
            return ImageFont.truetype(face, size=size)
        except Exception:
            return ImageFont.load_default()

    @staticmethod
    def _draw_export_results_box_from_lines(
        image: Image.Image,
        lines: list[tuple[str, tuple[int, int, int, int]]],
    ) -> None:
        if not lines:
            return
        draw = ImageDraw.Draw(image, "RGBA")
        heading = "MycoLab calibration"
        min_side = max(1, min(image.size))
        font = CalibrationDialog._export_font(int(min_side * 0.018))
        heading_font = CalibrationDialog._export_font(int(min_side * 0.020), bold=True)
        pad = max(8, int(min_side * 0.012))
        line_gap = max(4, int(min_side * 0.004))
        outer_margin = max(8, int(min_side * 0.018))

        def _text_size(text: str, current_font) -> tuple[int, int]:
            l, t, r, b = draw.textbbox((0, 0), text, font=current_font)
            return max(1, int(r - l)), max(1, int(b - t))

        heading_w, heading_h = _text_size(heading, heading_font)
        max_w = heading_w
        total_h = heading_h
        sized_lines: list[tuple[str, tuple[int, int, int, int], int, int]] = []
        for text, color in lines:
            w, h = _text_size(text, font)
            sized_lines.append((text, color, w, h))
            max_w = max(max_w, w)
            total_h += line_gap + h

        box_w = max_w + pad * 2
        box_h = total_h + pad * 2
        box_x = outer_margin
        box_y = outer_margin
        draw.rounded_rectangle(
            (box_x, box_y, box_x + box_w, box_y + box_h),
            radius=max(6, int(min_side * 0.008)),
            fill=(255, 255, 255, 128),
            outline=(255, 255, 255, 128),
        )

        text_x = box_x + pad
        text_y = box_y + pad
        draw.text((text_x, text_y), heading, font=heading_font, fill=(44, 62, 80, 255))
        text_y += heading_h + line_gap
        for text, color, _w, h in sized_lines:
            draw.text((text_x, text_y), text, font=font, fill=color)
            text_y += h + line_gap

    def _draw_export_results_box(self, image: Image.Image) -> None:
        lines = self._collect_export_result_lines()
        self._draw_export_results_box_from_lines(image, lines)

    def _collect_export_layers(self, img_data: dict) -> list[dict]:
        if self._is_auto_tab_active():
            auto_data = img_data.get("auto")
            if not auto_data:
                return []
            use_edges = self._auto_use_edges()
            if self._show_auto_debug_overlays:
                layers = []
                if use_edges:
                    edge_lines = auto_data.get("overlay_edges_50")
                    if edge_lines is None:
                        edge_lines = auto_data.get("overlay_edges", [])
                    if edge_lines:
                        layers.append({
                            "lines": edge_lines,
                            "color": (241, 196, 15, 200),
                            "width": 2.5,
                        })
                    edge_center_lines = auto_data.get("overlay_edges", [])
                    if edge_center_lines:
                        layers.append({
                            "lines": edge_center_lines,
                            "color": (255, 0, 0, 140),
                            "width": 2.5,
                        })
                else:
                    parabola_lines = auto_data.get("overlay_parabola", [])
                    if parabola_lines:
                        layers.append({
                            "lines": parabola_lines,
                            "color": (186, 85, 255, 170),
                            "width": 2.5,
                            "composition": "screen",
                        })
                return layers
            lines = auto_data["overlay_edges"] if use_edges else auto_data["overlay_parabola"]
            color = self.auto_measure_color if use_edges else self.auto_parabola_color
            return [{"lines": lines, "color": color}]

        lines = []
        for m in img_data.get("measurements", []):
            coords = m.get("line_coords", [])
            if len(coords) == 4:
                lines.append(coords)
        return [{"lines": lines, "color": self.manual_measure_color, "width": 4, "show_endcaps": True}]

    @staticmethod
    def _crop_rect_from_normalized(
        width: int,
        height: int,
        crop_box: tuple[float, float, float, float] | None,
    ) -> tuple[int, int, int, int] | None:
        if not crop_box or width <= 0 or height <= 0:
            return None
        try:
            x1n, y1n, x2n, y2n = (float(v) for v in crop_box)
        except Exception:
            return None
        x1 = max(0, min(width, int(min(x1n, x2n) * width)))
        y1 = max(0, min(height, int(min(y1n, y2n) * height)))
        x2 = max(0, min(width, int(max(x1n, x2n) * width)))
        y2 = max(0, min(height, int(max(y1n, y2n) * height)))
        if x2 - x1 < 2 or y2 - y1 < 2:
            return None
        return (x1, y1, x2, y2)

    @staticmethod
    def _shift_lines_for_crop(lines: list, crop_offset: tuple[int, int]) -> list[list[float]]:
        shifted: list[list[float]] = []
        ox, oy = crop_offset
        for line in lines or []:
            if not isinstance(line, (list, tuple)) or len(line) != 4:
                continue
            try:
                x1, y1, x2, y2 = (float(v) for v in line)
            except Exception:
                continue
            shifted.append([x1 - ox, y1 - oy, x2 - ox, y2 - oy])
        return shifted

    def _export_calibration_svg_with_widget(
        self,
        image_path: str,
        crop_box: tuple[float, float, float, float] | None,
        img_data: dict,
        filename: str,
        target_size: tuple[int, int] | None = None,
    ) -> bool:
        pixmap = QPixmap(str(image_path))
        if pixmap.isNull():
            return False
        crop_offset = (0, 0)
        crop_rect = self._crop_rect_from_normalized(pixmap.width(), pixmap.height(), crop_box)
        if crop_rect is not None:
            x1, y1, x2, y2 = crop_rect
            pixmap = pixmap.copy(x1, y1, x2 - x1, y2 - y1)
            crop_offset = (x1, y1)

        export_view = ZoomableImageLabel()
        export_view.set_image(pixmap, preserve_view=False)
        export_view.set_measurement_rectangles([])
        export_view.set_measurement_labels([])
        export_view.set_overlay_boxes([])
        export_view.set_preview_line(None)
        export_view.set_debug_lines([])
        export_view.set_measurement_lines([])

        if self._is_auto_tab_active():
            auto_data = img_data.get("auto")
            if auto_data:
                use_edges = self._auto_use_edges()
                if self._show_auto_debug_overlays:
                    layers = self._collect_export_layers(img_data)
                    debug_layers = []
                    for layer in layers:
                        debug_layers.append(
                            {
                                **layer,
                                "lines": self._shift_lines_for_crop(layer.get("lines", []), crop_offset),
                            }
                        )
                    export_view.set_debug_lines(debug_layers)
                else:
                    lines = auto_data["overlay_edges"] if use_edges else auto_data["overlay_parabola"]
                    export_view.set_measurement_color(self.auto_measure_color if use_edges else self.auto_parabola_color)
                    export_view.set_show_line_endcaps(False)
                    export_view.set_measurement_lines(self._shift_lines_for_crop(lines, crop_offset))
        else:
            export_view.set_measurement_color(self.manual_measure_color)
            export_view.set_show_line_endcaps(True)
            manual_lines = []
            for m in img_data.get("measurements", []):
                coords = m.get("line_coords", [])
                if len(coords) == 4:
                    manual_lines.append(coords)
            export_view.set_measurement_lines(self._shift_lines_for_crop(manual_lines, crop_offset))

        return export_view.export_annotated_svg(str(filename), target_size)

    def _on_export_image(self) -> None:
        """Export the current image with overlay lines."""
        if self._export_running:
            return
        if self._auto_calibration_running:
            QMessageBox.information(
                self,
                self.tr("Auto calibration"),
                self.tr("Auto calibration is still running. Please wait until it finishes."),
            )
            return
        if self.current_image_index < 0 or self.current_image_index >= len(self.calibration_images):
            QMessageBox.information(
                self,
                self.tr("No Image"),
                self.tr("Please load a calibration image first."),
            )
            return

        img_data = self.calibration_images[self.current_image_index]
        image_path = img_data.get("path")
        if not image_path or not Path(image_path).exists():
            QMessageBox.warning(
                self,
                self.tr("Missing Image"),
                self.tr("The selected image could not be found on disk."),
            )
            return

        obj = self.objectives.get(self.current_objective_key or "", {})
        objective_name = (
            str(obj.get("objective_name") or "").strip()
            or objective_display_name(obj, self.current_objective_key or "")
            or str(self.current_objective_key or "").strip()
        )
        if not objective_name:
            objective_name = Path(image_path).stem
        objective_name = re.sub(r'[\\/:*?"<>|]+', "-", objective_name)
        objective_name = re.sub(r"\s+", "_", objective_name).strip(" ._-")
        if not objective_name:
            objective_name = "calibration"
        date_stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        crop_box = img_data.get("crop_box")
        base_w = int(img_data["pixmap"].width()) if img_data.get("pixmap") else 1
        base_h = int(img_data["pixmap"].height()) if img_data.get("pixmap") else 1
        crop_rect = self._crop_rect_from_normalized(base_w, base_h, crop_box)
        if crop_rect is not None:
            base_w = max(1, crop_rect[2] - crop_rect[0])
            base_h = max(1, crop_rect[3] - crop_rect[1])

        export_dialog = ExportImageDialog(
            base_w,
            base_h,
            self.export_scale_percent,
            self.export_format,
            parent=self,
        )
        if export_dialog.exec() != QDialog.Accepted:
            return
        export_settings = export_dialog.get_settings()
        self.export_scale_percent = float(export_settings["scale_percent"])
        export_format = str(export_settings["format"] or "png")
        self.export_format = export_format

        ext_map = {"png": ".png", "jpg": ".jpg", "svg": ".svg"}
        default_ext = ext_map.get(export_format, ".png")
        default_name = f"{objective_name}_{date_stamp}{default_ext}"
        default_path = str(Path(self._get_default_export_dir()) / default_name)
        save_path, _selected_filter = QFileDialog.getSaveFileName(
            self,
            self.tr("Export Image"),
            default_path,
            self.tr("PNG Image (*.png);;JPEG Image (*.jpg *.jpeg);;SVG Files (*.svg)"),
        )
        if not save_path:
            return

        suffix = Path(save_path).suffix.lower()
        if not suffix:
            suffix = default_ext
            save_path = f"{save_path}{suffix}"
        self._remember_export_dir(save_path)

        layers = self._collect_export_layers(img_data)
        result_lines = self._collect_export_result_lines()
        normalized_crop_box = None
        if isinstance(crop_box, (tuple, list)) and len(crop_box) == 4:
            try:
                normalized_crop_box = tuple(float(v) for v in crop_box)
            except (TypeError, ValueError):
                normalized_crop_box = None

        self._set_export_controls_enabled(False)
        self._export_running = True
        self._set_hint_progress_visible(True)
        self._set_hint_progress(self.tr("Export Image"), 0)

        thread = QThread(self)
        worker = _ExportImageWorker(
            image_path=str(image_path),
            save_path=str(save_path),
            suffix=str(suffix),
            target_size=(int(export_settings["width"]), int(export_settings["height"])),
            jpeg_quality=int(export_settings["quality"]),
            crop_box=normalized_crop_box,
            layers=layers,
            result_lines=result_lines,
        )
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.progress.connect(self._on_export_progress)
        worker.finished.connect(self._on_export_finished)
        worker.failed.connect(self._on_export_failed)
        worker.finished.connect(thread.quit)
        worker.failed.connect(thread.quit)
        worker.finished.connect(worker.deleteLater)
        worker.failed.connect(worker.deleteLater)
        thread.finished.connect(self._on_export_worker_thread_finished)
        thread.finished.connect(thread.deleteLater)

        self._export_worker_thread = thread
        self._export_worker = worker
        thread.start()

    def _set_export_controls_enabled(self, enabled: bool) -> None:
        self._set_auto_controls_enabled(enabled)
        if hasattr(self, "tab_widget"):
            self.tab_widget.setEnabled(enabled)

    @Slot(str, int)
    def _on_export_progress(self, _step: str, value: int) -> None:
        self._set_hint_progress_visible(True)
        self._set_hint_progress(self.tr("Export Image"), value)

    @Slot(str)
    def _on_export_finished(self, _save_path: str) -> None:
        self._set_hint_progress(self.tr("Export Image"), 100)

    @Slot(str)
    def _on_export_failed(self, error_text: str) -> None:
        self._set_hint_progress(self.tr("Export Image"), 0)
        QMessageBox.warning(
            self,
            self.tr("Export Image"),
            error_text,
        )

    @Slot()
    def _on_export_worker_thread_finished(self) -> None:
        self._export_worker = None
        self._export_worker_thread = None
        self._export_running = False
        self._set_export_controls_enabled(True)
        self._set_hint_progress_visible(False)

    def _add_calibration_image(self, path: str):
        """Add a calibration image."""
        pixmap = QPixmap(path)
        if pixmap.isNull():
            QMessageBox.warning(
                self,
                self.tr("Error"),
                self.tr("Could not load image: {path}").format(path=Path(path).name),
            )
            return

        camera_text = self._extract_camera_text(path)
        self.calibration_images.append({
            "path": path,
            "pixmap": pixmap,
            "measurements": [],  # Measurements for this specific image
            "crop_box": None,
            "crop_source_size": None,
            "camera": camera_text,
        })
        self._modified = True  # User added a new image
        self._refresh_image_gallery()

        # Select the new image
        self.current_image_index = len(self.calibration_images) - 1
        self._show_current_image()

    def _refresh_image_gallery(self):
        """Refresh the image gallery with loaded calibration images."""
        items = []
        for i, img_data in enumerate(self.calibration_images):
            n_measurements = len(img_data.get("measurements", []))
            badge = f"{n_measurements} meas" if n_measurements > 0 else ""
            items.append({
                "id": f"cal_{i}",  # Use string ID to avoid thumbnail cache collision with db image IDs
                "filepath": img_data["path"],
                "image_number": i + 1,
                "badges": [badge] if badge else [],
                "crop_box": img_data.get("crop_box"),
                "crop_source_size": img_data.get("crop_source_size"),
            })
        self.image_gallery.set_items(items)

    def _extract_camera_text(self, path: str) -> str | None:
        if not path:
            return None
        exif = get_exif_data(path)
        make = exif.get("Make") or ""
        model = exif.get("Model") or ""
        camera = " ".join(str(part).strip() for part in (make, model) if part).strip()
        return camera or None

    def _collect_camera_summary(self) -> str | None:
        cameras = []
        for img in self.calibration_images:
            cam = img.get("camera")
            if cam:
                cameras.append(cam)
        if not cameras:
            return None
        unique = []
        for cam in cameras:
            if cam not in unique:
                unique.append(cam)
        if len(unique) == 1:
            return unique[0]
        return "; ".join(unique)

    def _selected_camera_text(self) -> str | None:
        if self.current_image_index < 0 or self.current_image_index >= len(self.calibration_images):
            return None
        img = self.calibration_images[self.current_image_index]
        cam = img.get("camera")
        if cam:
            return cam
        path = img.get("path")
        return self._extract_camera_text(path) if path else None

    def _compute_megapixels(self, img_data: dict) -> float | None:
        if not img_data:
            return None
        crop_source = img_data.get("crop_source_size")
        if crop_source and len(crop_source) == 2:
            try:
                source_w = float(crop_source[0])
                source_h = float(crop_source[1])
            except (TypeError, ValueError):
                source_w = source_h = 0
            if source_w > 0 and source_h > 0:
                return (source_w * source_h) / 1_000_000.0

        pixmap = img_data.get("pixmap")
        if pixmap:
            width = float(pixmap.width())
            height = float(pixmap.height())
            if width > 0 and height > 0:
                return (width * height) / 1_000_000.0
        return None

    def _collect_megapixels_summary(self) -> float | None:
        values = []
        for img in self.calibration_images:
            mp = self._compute_megapixels(img)
            if mp and mp > 0:
                values.append(mp)
        if not values:
            return None
        return float(np.mean(values))

    def _collect_image_dimensions_summary(self) -> tuple[int | None, int | None]:
        widths = []
        heights = []
        for img in self.calibration_images:
            crop_source = img.get("crop_source_size")
            if crop_source and len(crop_source) == 2:
                try:
                    source_w = int(crop_source[0])
                    source_h = int(crop_source[1])
                except (TypeError, ValueError):
                    source_w = source_h = 0
                if source_w > 0 and source_h > 0:
                    widths.append(source_w)
                    heights.append(source_h)
                    continue
            pixmap = img.get("pixmap")
            if pixmap:
                width = pixmap.width()
                height = pixmap.height()
                if width > 0 and height > 0:
                    widths.append(int(width))
                    heights.append(int(height))
        if not widths or not heights:
            return None, None
        return int(round(float(np.mean(widths)))), int(round(float(np.mean(heights))))

    def _estimate_megapixels_from_calibration(self, cal: dict) -> float | None:
        if not cal:
            return None
        mp_value = cal.get("megapixels")
        if isinstance(mp_value, (int, float)) and mp_value > 0:
            return float(mp_value)
        measurements_json = cal.get("measurements_json")
        if measurements_json:
            try:
                loaded = json.loads(measurements_json)
            except Exception:
                loaded = None
            if isinstance(loaded, dict):
                image_entries = loaded.get("images") or []
                values = []
                for info in image_entries:
                    crop_source = info.get("crop_source_size")
                    if crop_source and len(crop_source) == 2:
                        try:
                            source_w = float(crop_source[0])
                            source_h = float(crop_source[1])
                        except (TypeError, ValueError):
                            source_w = source_h = 0
                        if source_w > 0 and source_h > 0:
                            values.append((source_w * source_h) / 1_000_000.0)
                            continue
                    path = info.get("path")
                    if path and Path(path).exists():
                        try:
                            with Image.open(path) as img:
                                values.append((img.width * img.height) / 1_000_000.0)
                        except Exception:
                            pass
                if values:
                    return float(np.mean(values))
            elif isinstance(loaded, list):
                image_path = cal.get("image_filepath")
                if image_path and Path(image_path).exists():
                    try:
                        with Image.open(image_path) as img:
                            return float((img.width * img.height) / 1_000_000.0)
                    except Exception:
                        return None
        image_path = cal.get("image_filepath")
        if image_path and Path(image_path).exists():
            try:
                with Image.open(image_path) as img:
                    return float((img.width * img.height) / 1_000_000.0)
            except Exception:
                return None
        return None

    def _effective_megapixels(self, mp_value: float | None, cal: dict | None) -> float | None:
        if not mp_value:
            return mp_value
        return float(mp_value)

    def _on_gallery_image_clicked(self, image_id, path: str):
        """Handle click on an image in the gallery."""
        self.image_gallery.setFocus()
        # Find image by path since we use string IDs
        for i, img_data in enumerate(self.calibration_images):
            if img_data.get("path") == path:
                self.current_image_index = i
                self._show_current_image()
                return
            self._show_current_image()

    def _on_gallery_image_deleted(self, image_id):
        """Handle deletion of an image from the gallery."""
        # Parse index from string ID like "cal_0"
        if isinstance(image_id, str) and image_id.startswith("cal_"):
            try:
                idx = int(image_id.split("_")[1])
            except (ValueError, IndexError):
                return
            if 0 <= idx < len(self.calibration_images):
                del self.calibration_images[idx]
                self._modified = True  # User deleted an image
                if self.current_image_index > idx:
                    self.current_image_index -= 1
                self._refresh_image_gallery()
                if self.current_image_index >= len(self.calibration_images):
                    self.current_image_index = len(self.calibration_images) - 1
                if self.current_image_index >= 0:
                    self._show_current_image()
                else:
                    self.image_viewer.set_image(None)
                    self.image_viewer.set_measurement_lines([])
                    self._preserve_image_zoom = False
                self._update_measurement_list()
                self._update_results()
                self._update_auto_summary()

    def _delete_selected_gallery_image(self):
        """Delete the selected image in the gallery."""
        if not self.calibration_images:
            return

        selected_paths = self.image_gallery.selected_paths()
        idx = None
        if selected_paths:
            selected_path = selected_paths[0]
            for i, img_data in enumerate(self.calibration_images):
                if img_data.get("path") == selected_path:
                    idx = i
                    break

        if idx is None and self.current_image_index >= 0:
            idx = self.current_image_index

        if idx is None:
            return

        self._on_gallery_image_deleted(f"cal_{idx}")

    def _show_current_image(self):
        """Show the currently selected image."""
        if self.current_image_index < 0 or self.current_image_index >= len(self.calibration_images):
            self.image_viewer.set_image(None)
            self._preserve_image_zoom = False
            self._update_resize_info()
            return

        img_data = self.calibration_images[self.current_image_index]
        self.image_viewer.set_image(img_data["pixmap"], preserve_view=self._preserve_image_zoom)
        if not self._preserve_image_zoom:
            self._preserve_image_zoom = True
        crop_box = img_data.get("crop_box")
        if crop_box and self.image_viewer.original_pixmap:
            width = float(self.image_viewer.original_pixmap.width())
            height = float(self.image_viewer.original_pixmap.height())
            x1 = crop_box[0] * width
            y1 = crop_box[1] * height
            x2 = crop_box[2] * width
            y2 = crop_box[3] * height
            self.image_viewer.set_crop_box((x1, y1, x2, y2))
        else:
            self.image_viewer.set_crop_box(None)
        self._set_auto_crop_active(False)
        self._render_auto_results(self._current_auto_data())
        self._apply_current_overlay()
        self._update_resize_info()

    def _start_measurement(self):
        """Start a new measurement."""
        if not self.calibration_images:
            QMessageBox.information(
                self,
                self.tr("No Image"),
                self.tr("Please load a calibration image first."),
            )
            return

        self.is_measuring = True
        self.measurement_points = []
        self.add_measurement_btn.setText(self.tr("Click start point..."))
        self.add_measurement_btn.setEnabled(False)
        self.image_viewer.set_preview_line(None)
        self.image_viewer.setCursor(Qt.CrossCursor)

    def _on_image_clicked(self, pos: QPointF):
        """Handle click on the image."""
        if not self.is_measuring:
            return

        self.measurement_points.append(pos)

        if len(self.measurement_points) == 1:
            # First point - show preview line
            self.image_viewer.set_preview_line(pos)
            self.add_measurement_btn.setText(self.tr("Click end point..."))

        elif len(self.measurement_points) == 2:
            # Second point - complete measurement
            p1, p2 = self.measurement_points
            dx = p2.x() - p1.x()
            dy = p2.y() - p1.y()
            distance_px = (dx * dx + dy * dy) ** 0.5

            # Reset measurement state first (before any potential errors)
            self.is_measuring = False
            self.measurement_points = []
            self.add_measurement_btn.setText(self.tr("Add Measurement"))
            self.add_measurement_btn.setEnabled(True)
            self.image_viewer.clear_preview_line()
            self.image_viewer.setCursor(Qt.ArrowCursor)

            if distance_px > 0 and self.current_image_index >= 0:
                if self._collect_auto_values(self._auto_use_edges()) and not self._auto_manual_notice_shown:
                    self._show_wrapped_info_dialog(
                        self.tr("Auto Calibration Available"),
                        self.tr(
                            "Auto calibration results are available. Manual measurements are still recorded, "
                            "but auto results will be used unless they are cleared."
                        ),
                    )
                    self._auto_manual_notice_shown = True
                known_um = self.known_distance_input.value() * 1000.0
                measurement = {
                    "known_um": known_um,
                    "measured_px": distance_px,
                    "line_coords": [p1.x(), p1.y(), p2.x(), p2.y()],
                    "image_index": self.current_image_index,
                }
                self.calibration_images[self.current_image_index]["measurements"].append(measurement)
                self._modified = True  # User added a measurement
                try:
                    self._update_measurement_list()
                    self._update_results()
                    self._apply_current_overlay()
                    self._refresh_image_gallery()
                except Exception as e:
                    print(f"Error updating calibration results: {e}")

    def _delete_selected_measurement(self):
        """Delete the selected measurement from the list."""
        current_item = self.measurement_list.currentItem()
        if not current_item:
            return

        data = current_item.data(Qt.UserRole)
        if not data:
            return

        img_idx = data.get("image_index")
        meas_idx = data.get("measurement_index")

        if img_idx is not None and meas_idx is not None:
            if 0 <= img_idx < len(self.calibration_images):
                measurements = self.calibration_images[img_idx].get("measurements", [])
                if 0 <= meas_idx < len(measurements):
                    del measurements[meas_idx]
                    self._modified = True  # User deleted a measurement
                    self._update_measurement_list()
                    self._update_results()
                    self._apply_current_overlay()
                    self._refresh_image_gallery()

    def _on_delete_pressed(self):
        """Handle Del key - delete from measurement list or history table based on focus."""
        focus_widget = self.focusWidget()
        if focus_widget and self.image_gallery.isAncestorOf(focus_widget):
            self._delete_selected_gallery_image()
            return
        # Check if history table has focus
        if self.history_table.hasFocus():
            self._delete_selected_calibration()
        else:
            # Default to measurement list
            self._delete_selected_measurement()

    def _delete_selected_calibration(self):
        """Delete the selected calibration from the history table."""
        selected_rows = self.history_table.selectionModel().selectedRows()
        if not selected_rows:
            return

        rows = [row.row() for row in selected_rows]
        if not hasattr(self, '_history_calibration_ids'):
            return
        calibration_ids = [
            self._history_calibration_ids[r]
            for r in rows
            if 0 <= r < len(self._history_calibration_ids)
        ]
        if not calibration_ids:
            return

        if len(calibration_ids) == 1:
            calibration_id = calibration_ids[0]
            cal = CalibrationDB.get_calibration(calibration_id)
            if not cal:
                return

            # Check if this calibration is being used
            usage_summary = CalibrationDB.get_calibration_usage_summary(self.current_objective_key)
            usage = next((u for u in usage_summary if u["calibration_id"] == calibration_id), {})
            image_count = usage.get("image_count", 0)
            measurement_count = usage.get("measurement_count", 0)

            if image_count > 0 or measurement_count > 0:
                self._show_calibration_in_use_dialog(calibration_id, image_count, measurement_count)
                return

            # Confirm deletion
            date_str = cal.get("calibration_date", "")[:16]
            scale_nm = um_to_nm(cal.get("microns_per_pixel", 0))

            if not self._ask_wrapped_confirm_dialog(
                self.tr("Delete Calibration"),
                self.tr(
                    "Delete calibration from {date}?\n\n"
                    "Scale: {scale:.2f} nm/px\n\n"
                    "This action cannot be undone."
                ).format(date=date_str, scale=scale_nm),
                destructive=True,
                accept_text=self.tr("Yes"),
            ):
                return

            failures = CalibrationDB.delete_calibration(calibration_id)
            self._warn_delete_failures(failures)
            self._update_history_table()
            return

        usage_summary = CalibrationDB.get_calibration_usage_summary(self.current_objective_key)
        in_use = []
        for calibration_id in calibration_ids:
            usage = next((u for u in usage_summary if u["calibration_id"] == calibration_id), {})
            if usage.get("image_count", 0) > 0 or usage.get("measurement_count", 0) > 0:
                in_use.append(calibration_id)
        if in_use:
            QMessageBox.warning(
                self,
                self.tr("Delete Calibration"),
                self.tr(
                    "Some selected calibrations are in use. Delete them individually to review usage details."
                ),
            )
            return

        if not self._ask_wrapped_confirm_dialog(
            self.tr("Delete Calibration"),
            self.tr(
                "Delete {count} calibrations?\n\n"
                "This action cannot be undone."
            ).format(count=len(calibration_ids)),
            destructive=True,
            accept_text=self.tr("Yes"),
        ):
            return

        failures: list[str] = []
        for calibration_id in calibration_ids:
            failures.extend(CalibrationDB.delete_calibration(calibration_id))
        self._warn_delete_failures(failures)
        self._update_history_table()

    def _show_calibration_in_use_dialog(
        self,
        calibration_id: int,
        image_count: int,
        measurement_count: int,
    ) -> None:
        dialog = QDialog(self)
        dialog.setWindowTitle(self.tr("Warning - calibration in use"))
        dialog.setModal(True)
        dialog.setMinimumWidth(400)
        dialog.resize(400, 320)

        layout = QVBoxLayout(dialog)
        message = QLabel(
            self.tr(
                "This calibration is used by {images} images and {measurements} measurements."
            ).format(images=image_count, measurements=measurement_count)
        )
        message.setWordWrap(True)
        layout.addWidget(message)

        table = QTableWidget(0, 5)
        table.setHorizontalHeaderLabels([
            self.tr("ID"),
            self.tr("Genus"),
            self.tr("Species"),
            self.tr("Vernacular name"),
            self.tr("Date"),
        ])
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.setSelectionMode(QAbstractItemView.SingleSelection)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.verticalHeader().setVisible(False)
        header = table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.Stretch)
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        table.setSortingEnabled(False)

        rows = CalibrationDB.get_images_by_calibration(calibration_id)
        obs_map: dict[int, dict] = {}
        for row in rows:
            obs_id = row.get("observation_id")
            if obs_id is None:
                continue
            if obs_id not in obs_map:
                obs_map[obs_id] = {
                    "id": obs_id,
                    "genus": row.get("genus") or "",
                    "species": row.get("species") or "",
                    "common_name": row.get("common_name") or "",
                    "date": row.get("date") or "",
                }
        obs_list = list(obs_map.values())
        obs_list.sort(key=lambda o: (o.get("date") or "", o.get("genus") or "", o.get("species") or ""))

        table.setRowCount(len(obs_list))
        for i, obs in enumerate(obs_list):
            id_item = QTableWidgetItem(str(obs.get("id", "")))
            id_item.setData(Qt.UserRole, obs.get("id"))
            table.setItem(i, 0, id_item)
            table.setItem(i, 1, QTableWidgetItem(obs.get("genus", "")))
            table.setItem(i, 2, QTableWidgetItem(obs.get("species", "")))
            table.setItem(i, 3, QTableWidgetItem(obs.get("common_name", "")))
            table.setItem(i, 4, QTableWidgetItem(obs.get("date", "")))

        if obs_list:
            table.selectRow(0)

        layout.addWidget(table)

        button_row = QHBoxLayout()
        go_btn = QPushButton(self.tr("Go to observation"))
        delete_btn = QPushButton(self.tr("Delete calibration"))
        self._apply_danger_button_style(delete_btn)
        close_btn = QPushButton(self.tr("Close"))
        button_row.addWidget(delete_btn)
        button_row.addWidget(go_btn)
        button_row.addStretch()
        button_row.addWidget(close_btn)
        layout.addLayout(button_row)

        def _open_selected_observation():
            row = table.currentRow()
            if row < 0:
                return
            if row >= len(obs_list):
                return
            obs_id = obs_list[row].get("id")
            if not obs_id:
                return
            obs = ObservationDB.get_observation(obs_id)
            genus = obs.get("genus") if obs else ""
            species = obs.get("species") if obs else ""
            date = (obs.get("date") or "")[:10] if obs else ""
            display_name = f"{genus} {species} {date}".strip() or f"Observation {obs_id}"
            parent = self.parent()
            if parent and hasattr(parent, "on_observation_selected"):
                dialog.accept()
                self.close()
                parent.on_observation_selected(obs_id, display_name, switch_tab=True, suppress_gallery=False)
                return
            if parent and hasattr(parent, "_on_observation_selected_impl"):
                dialog.accept()
                self.close()
                parent._on_observation_selected_impl(obs_id, display_name, switch_tab=True, schedule_gallery=True)

        def _delete_calibration_in_use():
            if not self._ask_wrapped_confirm_dialog(
                self.tr("Delete Calibration"),
                self.tr(
                    "Delete this calibration and remove scale data from all observations that use it?\n\n"
                    "This action cannot be undone."
                ),
                parent_widget=dialog,
                destructive=True,
                accept_text=self.tr("Yes"),
            ):
                return
            CalibrationDB.clear_calibration_usage(calibration_id, clear_objective=True)
            failures = CalibrationDB.delete_calibration(calibration_id)
            self._warn_delete_failures(failures)
            dialog.accept()
            self._update_history_table()

        go_btn.clicked.connect(_open_selected_observation)
        delete_btn.clicked.connect(_delete_calibration_in_use)
        close_btn.clicked.connect(dialog.reject)

        dialog.exec()

    def _show_objective_in_use_dialog(
        self,
        objective_key: str,
        rows: list[dict],
    ) -> bool:
        dialog = QDialog(self)
        dialog.setWindowTitle(self.tr("Warning - objective in use"))
        dialog.setModal(True)
        dialog.setMinimumWidth(620)
        dialog.resize(760, 360)

        layout = QVBoxLayout(dialog)
        obs_map: dict[int, dict] = {}
        for row in rows:
            obs_id = row.get("observation_id")
            if obs_id is None:
                continue
            if obs_id not in obs_map:
                obs_map[obs_id] = {
                    "id": obs_id,
                    "genus": row.get("genus") or "",
                    "species": row.get("species") or "",
                    "common_name": row.get("common_name") or "",
                    "date": row.get("date") or "",
                }
        obs_list = list(obs_map.values())
        obs_list.sort(key=lambda o: (o.get("date") or "", o.get("genus") or "", o.get("species") or ""))

        message = QLabel(
            self.tr(
                "This objective is used by {images} images across {observations} observations."
            ).format(images=len(rows), observations=len(obs_list))
        )
        message.setWordWrap(True)
        layout.addWidget(message)

        table = QTableWidget(0, 5)
        table.setHorizontalHeaderLabels([
            self.tr("ID"),
            self.tr("Genus"),
            self.tr("Species"),
            self.tr("Vernacular name"),
            self.tr("Date"),
        ])
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.setSelectionMode(QAbstractItemView.SingleSelection)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.verticalHeader().setVisible(False)
        header = table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.Stretch)
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        table.setSortingEnabled(False)

        table.setRowCount(len(obs_list))
        for i, obs in enumerate(obs_list):
            id_item = QTableWidgetItem(str(obs.get("id", "")))
            id_item.setData(Qt.UserRole, obs.get("id"))
            table.setItem(i, 0, id_item)
            table.setItem(i, 1, QTableWidgetItem(obs.get("genus", "")))
            table.setItem(i, 2, QTableWidgetItem(obs.get("species", "")))
            table.setItem(i, 3, QTableWidgetItem(obs.get("common_name", "")))
            table.setItem(i, 4, QTableWidgetItem(obs.get("date", "")))

        if obs_list:
            table.selectRow(0)

        layout.addWidget(table)

        should_delete = {"value": False}

        button_row = QHBoxLayout()
        go_btn = QPushButton(self.tr("Go to observation"))
        delete_btn = QPushButton(self.tr("Delete objective"))
        self._apply_danger_button_style(delete_btn)
        close_btn = QPushButton(self.tr("Close"))
        button_row.addWidget(delete_btn)
        button_row.addWidget(go_btn)
        button_row.addStretch()
        button_row.addWidget(close_btn)
        layout.addLayout(button_row)

        def _open_selected_observation():
            row = table.currentRow()
            if row < 0:
                return
            if row >= len(obs_list):
                return
            obs_id = obs_list[row].get("id")
            if not obs_id:
                return
            obs = ObservationDB.get_observation(obs_id)
            genus = obs.get("genus") if obs else ""
            species = obs.get("species") if obs else ""
            date = (obs.get("date") or "")[:10] if obs else ""
            display_name = f"{genus} {species} {date}".strip() or f"Observation {obs_id}"
            parent = self.parent()
            if parent and hasattr(parent, "on_observation_selected"):
                dialog.accept()
                self.close()
                parent.on_observation_selected(obs_id, display_name, switch_tab=True, suppress_gallery=False)
                return
            if parent and hasattr(parent, "_on_observation_selected_impl"):
                dialog.accept()
                self.close()
                parent._on_observation_selected_impl(obs_id, display_name, switch_tab=True, schedule_gallery=True)

        def _delete_objective_in_use():
            if not self._ask_wrapped_confirm_dialog(
                self.tr("Delete Objective"),
                self.tr(
                    "Delete this objective and remove scale data from all observations that use it?\n\n"
                    "This will also delete all calibrations for this objective.\n\n"
                    "This action cannot be undone."
                ),
                parent_widget=dialog,
                destructive=True,
                accept_text=self.tr("Yes"),
                min_height=230,
            ):
                return
            should_delete["value"] = True
            dialog.accept()

        go_btn.clicked.connect(_open_selected_observation)
        delete_btn.clicked.connect(_delete_objective_in_use)
        close_btn.clicked.connect(dialog.reject)

        dialog.exec()
        return should_delete["value"]

    def _warn_delete_failures(self, failures: list[str]) -> None:
        if not failures:
            return
        paths = [p for p in failures if p]
        if not paths:
            return
        names = [Path(p).name for p in paths]
        preview = "\n".join(f"- {name}" for name in names[:5])
        extra = ""
        if len(names) > 5:
            extra = self.tr("\n...and {count} more.").format(count=len(names) - 5)
        QMessageBox.warning(
            self,
            self.tr("Delete Failed"),
            self.tr("Some files or folders could not be deleted.")
            + "\n\n"
            + preview
            + extra,
        )

    def _on_set_active_calibration(self):
        """Set the selected calibration as active for this objective."""
        if not hasattr(self, "_history_calibration_ids"):
            return
        row = self.history_table.currentRow()
        if row < 0 or row >= len(self._history_calibration_ids):
            QMessageBox.information(
                self,
                self.tr("No Selection"),
                self.tr("Select a calibration in the history table first."),
            )
            return
        calibration_id = self._history_calibration_ids[row]
        cal = CalibrationDB.get_calibration(calibration_id)
        if not cal:
            return
        CalibrationDB.set_active_calibration(calibration_id)

        scale_um = cal.get("microns_per_pixel", 0)
        if self.current_objective_key in self.objectives:
            self.objectives[self.current_objective_key]["microns_per_pixel"] = scale_um
            save_objectives(self.objectives)

        scale_nm = um_to_nm(scale_um)
        date = cal.get("calibration_date", "")[:10]
        self.active_cal_label.setText(
            self.tr("Active: {scale:.2f} nm/px ({date})").format(scale=scale_nm, date=date)
        )
        self.manual_scale_input.setValue(scale_nm)
        self._update_history_table()

    def _clear_measurements(self):
        """Clear all measurements from all images."""
        for img_data in self.calibration_images:
            img_data["measurements"] = []
        self.measurement_points = []
        self.is_measuring = False
        self.add_measurement_btn.setText(self.tr("Add Measurement"))
        self.add_measurement_btn.setEnabled(True)
        self.image_viewer.clear_preview_line()
        self.image_viewer.setCursor(Qt.ArrowCursor)
        self._update_measurement_list()
        self._update_results()
        self._refresh_image_gallery()
        self._apply_current_overlay()

    def _update_auto_progress(self, step: str, frac: float):
        """Update progress UI from calibration steps."""
        if hasattr(self, "auto_progress"):
            value = int(max(0.0, min(1.0, frac)) * 100)
            self.auto_progress.setValue(value)
        else:
            value = int(max(0.0, min(1.0, frac)) * 100)
        if hasattr(self, "auto_status_label"):
            self.auto_status_label.setText(step)
            self.auto_status_label.setStyleSheet("color: #2980b9;")
        self._set_hint_progress_visible(True)
        self._set_hint_progress(step, value)

    def _set_auto_crop_active(self, active: bool):
        self._auto_crop_active = bool(active)
        self.image_viewer.set_crop_mode(self._auto_crop_active)
        if hasattr(self, "auto_crop_btn"):
            if self._auto_crop_active:
                self.auto_crop_btn.setStyleSheet("background-color: #f39c12; color: white;")
            else:
                self.auto_crop_btn.setStyleSheet("")

    def _on_crop_button_clicked(self):
        if not self.calibration_images or self.current_image_index < 0:
            return
        self._set_auto_crop_active(not getattr(self, "_auto_crop_active", False))

    def _on_crop_changed(self, box: tuple[float, float, float, float] | None) -> None:
        if self.current_image_index < 0 or self.current_image_index >= len(self.calibration_images):
            return
        img_data = self.calibration_images[self.current_image_index]
        if not self.image_viewer.original_pixmap:
            return
        width = float(self.image_viewer.original_pixmap.width())
        height = float(self.image_viewer.original_pixmap.height())
        if box and width > 0 and height > 0:
            x1, y1, x2, y2 = box
            norm_box = (
                max(0.0, min(1.0, x1 / width)),
                max(0.0, min(1.0, y1 / height)),
                max(0.0, min(1.0, x2 / width)),
                max(0.0, min(1.0, y2 / height)),
            )
            img_data["crop_box"] = norm_box
            img_data["crop_source_size"] = (int(width), int(height))
        else:
            img_data.pop("crop_box", None)
            img_data.pop("crop_source_size", None)
        self._modified = True
        self._refresh_image_gallery()
        self._set_auto_crop_active(False)
        # Crop changes invalidate auto results for this image.
        self._reset_auto_results(status_text=self.tr("Crop updated. Run auto calibration."), status_color="#2980b9")
    def _clear_all(self):
        """Clear all images and measurements."""
        self.calibration_images = []
        self.current_image_index = -1
        self.measurement_points = []
        self.is_measuring = False
        self._modified = False  # Reset modified flag
        self._preserve_image_zoom = False
        self.add_measurement_btn.setText(self.tr("Add Measurement"))
        self.add_measurement_btn.setEnabled(True)
        self.image_viewer.clear_preview_line()
        self.image_viewer.set_measurement_lines([])
        self.image_viewer.set_image(None)
        self.image_viewer.set_crop_box(None)
        self._set_auto_crop_active(False)
        self.image_viewer.setCursor(Qt.ArrowCursor)
        self._update_resize_info()
        self._update_measurement_list()
        self._update_results()
        self._refresh_image_gallery()
        self._reset_auto_results()

    def _get_all_measurements(self) -> list[dict]:
        """Get all measurements from all images."""
        all_measurements = []
        for img_idx, img_data in enumerate(self.calibration_images):
            for meas_idx, m in enumerate(img_data.get("measurements", [])):
                m_copy = dict(m)
                m_copy["image_index"] = img_idx
                m_copy["measurement_index"] = meas_idx
                all_measurements.append(m_copy)
        return all_measurements

    def _update_measurement_list(self):
        """Update the measurement list widget with all measurements."""
        self.measurement_list.clear()
        all_measurements = self._get_all_measurements()
        for i, m in enumerate(all_measurements):
            known = m["known_um"]
            px = m["measured_px"]
            um_per_px = known / px if px > 0 else 0
            nm_per_px = um_to_nm(um_per_px)
            img_num = m.get("image_index", 0) + 1
            known_mm = known / 1000.0
            text = f"#{i+1} (img{img_num}): {known_mm:.2f} mm = {px:.1f} px → {nm_per_px:.2f} nm/px"
            item = QListWidgetItem(text)
            item.setData(Qt.UserRole, {
                "image_index": m.get("image_index"),
                "measurement_index": m.get("measurement_index"),
            })
            self.measurement_list.addItem(item)

    def _update_measurement_lines(self):
        """Update measurement line overlays for the current image."""
        if self._is_auto_tab_active():
            return
        if self.current_image_index < 0 or self.current_image_index >= len(self.calibration_images):
            self.image_viewer.set_measurement_lines([])
            return

        img_data = self.calibration_images[self.current_image_index]
        lines = []
        for m in img_data.get("measurements", []):
            coords = m.get("line_coords", [])
            if len(coords) == 4:
                lines.append(coords)
        self.image_viewer.set_measurement_lines(lines)

    def _update_results(self):
        """Update the results display."""
        all_measurements = self._get_all_measurements()

        if not all_measurements:
            self.result_average_label.setText("--")
            self.result_std_label.setText("--")
            self.result_ci_label.setText("--")
            self.result_count_label.setText("0")
            if hasattr(self, "sampling_status_label"):
                self.sampling_status_label.setText("--")
                self._set_hint_for_label_widget(getattr(self, "sampling_status_title", None), "")
            self.comparison_label.setText("--")
            if hasattr(self, "auto_used_label"):
                self.auto_used_label.setText("")
            return

        # Calculate statistics
        measurement_tuples = [(m["known_um"], m["measured_px"]) for m in all_measurements]
        mean, std, ci_low, ci_high = calculate_calibration_stats(measurement_tuples)

        self.result_count_label.setText(str(len(all_measurements)))

        if mean is not None:
            mean_nm = um_to_nm(mean)
            self.result_average_label.setText(f"{mean_nm:.2f} nm/px")
        else:
            self.result_average_label.setText("--")

        if std is not None:
            std_nm = um_to_nm(std)
            self.result_std_label.setText(f"+/-{std_nm:.2f} nm/px")
        else:
            self.result_std_label.setText("--")

        if ci_low is not None and ci_high is not None:
            ci_low_nm = um_to_nm(ci_low)
            ci_high_nm = um_to_nm(ci_high)
            self.result_ci_label.setText(f"[{ci_low_nm:.2f}, {ci_high_nm:.2f}]")
        else:
            self.result_ci_label.setText("--")

        if hasattr(self, "sampling_status_label"):
            na_value = None
            if self.current_objective_key:
                obj = self.objectives.get(self.current_objective_key, {})
                na_value = obj.get("na")
            if mean and na_value:
                scale_nm_per_px = um_to_nm(mean)
                self._update_sampling_label(self.sampling_status_label, scale_nm_per_px)
            else:
                self.sampling_status_label.setText(self.tr("NA not set") if not na_value else "--")
                self._set_hint_for_label_widget(getattr(self, "sampling_status_title", None), "")

        auto_values = self._collect_auto_values(self._auto_use_edges())
        if mean and auto_values:
            auto_mean_nm = float(np.mean(auto_values))
            if auto_mean_nm > 0:
                mean_nm = um_to_nm(mean)
                diff_percent = ((mean_nm - auto_mean_nm) / auto_mean_nm) * 100
                sign = "+" if diff_percent >= 0 else ""
                color = "#27ae60" if abs(diff_percent) < 1 else "#e74c3c"
                self.comparison_label.setText(
                    f'<span style="color: {color};">{sign}{diff_percent:.2f}%</span>'
                )
                if hasattr(self, "auto_used_label"):
                    self.auto_used_label.setText(self.tr("Automatic calibration is used"))
                return
        self.comparison_label.setText("--")
        if hasattr(self, "auto_used_label"):
            self.auto_used_label.setText("")

    def _update_history_table(self):
        """Update the calibration history table."""
        self.history_table.setRowCount(0)
        self._history_calibration_ids = []  # Store calibration IDs for row lookup

        if not self.current_objective_key:
            return

        history = CalibrationDB.get_calibration_history(self.current_objective_key)
        usage_summary = CalibrationDB.get_calibration_usage_summary(self.current_objective_key)

        # Create a map of calibration_id to usage stats
        usage_map = {u["calibration_id"]: u for u in usage_summary}

        for row_idx, cal in enumerate(history):
            self.history_table.insertRow(row_idx)
            cal_id = cal.get("id")
            self._history_calibration_ids.append(cal_id)
            usage = usage_map.get(cal_id, {})

            # Date
            date_str = cal.get("calibration_date", "")[:16]
            self.history_table.setItem(row_idx, 0, QTableWidgetItem(date_str))

            # nm/px
            scale_um = cal.get("microns_per_pixel", 0)
            scale_nm = um_to_nm(scale_um)
            self.history_table.setItem(row_idx, 1, QTableWidgetItem(f"{scale_nm:.2f}"))

            # MP (megapixels used)
            mp_value = cal.get("megapixels")
            estimate = self._estimate_megapixels_from_calibration(cal)
            if isinstance(mp_value, (int, float)) and mp_value > 0:
                if estimate:
                    diff_ratio = abs(float(mp_value) - float(estimate)) / max(1e-6, float(estimate))
                    if diff_ratio > 0.01:
                        mp_value = estimate
            elif estimate:
                mp_value = estimate
            mp_value = self._effective_megapixels(mp_value, cal)
            mp_text = f"{float(mp_value):.3f}" if isinstance(mp_value, (int, float)) and mp_value > 0 else "--"
            self.history_table.setItem(row_idx, 2, QTableWidgetItem(mp_text))

            # n (calibration measurements)
            n = cal.get("num_measurements", 0)
            n_text = str(n) if n else "man"
            self.history_table.setItem(row_idx, 3, QTableWidgetItem(n_text))

            # Diff%
            diff = cal.get("diff_from_first_percent")
            if diff is not None:
                sign = "+" if diff >= 0 else ""
                diff_text = f"{sign}{diff:.2f}%"
            else:
                diff_text = "--"
            self.history_table.setItem(row_idx, 4, QTableWidgetItem(diff_text))

            # Auto quality metrics (if available)
            mad_text = "--"
            iqr_text = "--"
            residual_text = "--"
            measurements_json = cal.get("measurements_json")
            if measurements_json:
                try:
                    parsed = json.loads(measurements_json)
                except Exception:
                    parsed = None
                if isinstance(parsed, dict):
                    auto_images = parsed.get("auto_images", [])
                    if auto_images:
                        mad_vals = []
                        iqr_vals = []
                        residual_vals = []
                        for info in auto_images:
                            res = info.get("result", {}) or {}
                            mad = res.get("rel_scatter_mad_pct")
                            iqr = res.get("rel_scatter_iqr_pct")
                            residual = res.get("residual_slope_deg")
                            if isinstance(mad, (int, float)):
                                mad_vals.append(float(mad))
                            if isinstance(iqr, (int, float)):
                                iqr_vals.append(float(iqr))
                            if isinstance(residual, (int, float)):
                                residual_vals.append(abs(float(residual)))
                        if mad_vals:
                            mad_text = f"{float(np.mean(mad_vals)):.2f}%"
                        if iqr_vals:
                            iqr_text = f"{float(np.mean(iqr_vals)):.2f}%"
                        if residual_vals:
                            residual_text = f"{float(np.mean(residual_vals)):.2f} deg"

            self.history_table.setItem(row_idx, 5, QTableWidgetItem(mad_text))
            self.history_table.setItem(row_idx, 6, QTableWidgetItem(iqr_text))
            self.history_table.setItem(row_idx, 7, QTableWidgetItem(residual_text))

            # Observations count
            obs_count = usage.get("observation_count", 0)
            obs_item = QTableWidgetItem(str(obs_count))
            obs_item.setTextAlignment(Qt.AlignCenter)
            self.history_table.setItem(row_idx, 8, obs_item)

            # Camera
            camera_text = cal.get("camera") or "--"
            self.history_table.setItem(row_idx, 9, QTableWidgetItem(camera_text))

            # Active
            is_active = cal.get("is_active", 0)
            active_text = "✓" if is_active else ""
            active_item = QTableWidgetItem(active_text)
            active_item.setTextAlignment(Qt.AlignCenter)
            self.history_table.setItem(row_idx, 10, active_item)

            # Notes
            notes = cal.get("notes", "") or ""
            self.history_table.setItem(row_idx, 11, QTableWidgetItem(notes))

    def _on_history_row_clicked(self, row: int, column: int):
        """Handle click on a history table row to view that calibration."""
        if not hasattr(self, '_history_calibration_ids') or row >= len(self._history_calibration_ids):
            return

        calibration_id = self._history_calibration_ids[row]

        # Check for unsaved changes
        if self._has_unsaved_changes():
            reply = self._ask_unsaved_changes()
            if reply == QMessageBox.Save:
                self._on_save_calibration()
                # After saving, continue to view the historical calibration
            elif reply != QMessageBox.Discard:
                return

        self._on_view_calibration(calibration_id)

    def _has_unsaved_changes(self) -> bool:
        """Check if user made changes that haven't been saved."""
        return self._modified

    def _confirm_close_if_needed(self) -> bool:
        """Return True if dialog can close now, prompting for unsaved changes if needed."""
        if self._export_running:
            QMessageBox.information(
                self,
                self.tr("Export Image"),
                self.tr("Export is still running. Please wait until it finishes."),
            )
            return False
        if self._auto_calibration_running:
            QMessageBox.information(
                self,
                self.tr("Auto calibration"),
                self.tr("Auto calibration is still running. Please wait until it finishes."),
            )
            return False
        if not self._has_unsaved_changes():
            return True
        reply = self._ask_unsaved_changes()
        if reply == QMessageBox.Save:
            self._on_save_calibration()
            return not self._has_unsaved_changes()
        if reply == QMessageBox.Discard:
            return True
        return False

    def done(self, result: int) -> None:  # noqa: N802 - Qt API
        # Some dialog close paths can end up calling done(...) directly (bypassing
        # accept/reject overrides). Route them through the same close guard.
        if self._close_prompt_bypass:
            super().done(result)
            return
        if not self._confirm_close_if_needed():
            return
        self._close_prompt_bypass = True
        try:
            super().done(result)
        finally:
            self._close_prompt_bypass = False

    def accept(self) -> None:  # noqa: N802 - Qt API
        if not self._confirm_close_if_needed():
            return
        self._close_prompt_bypass = True
        try:
            super().accept()
        finally:
            self._close_prompt_bypass = False

    def reject(self) -> None:  # noqa: N802 - Qt API
        if not self._confirm_close_if_needed():
            return
        self._close_prompt_bypass = True
        try:
            super().reject()
        finally:
            self._close_prompt_bypass = False

    def closeEvent(self, event):
        """Handle dialog close, checking for unsaved changes."""
        if self._close_prompt_bypass:
            event.accept()
            return
        if self._confirm_close_if_needed():
            event.accept()
        else:
            event.ignore()

    def _on_view_calibration(self, calibration_id: int):
        """View a previous calibration."""
        cal = CalibrationDB.get_calibration(calibration_id)
        if not cal:
            return

        self._clear_all()

        # Load the calibration data
        measurements_json = cal.get("measurements_json")
        loaded_data = None
        if measurements_json:
            try:
                loaded_data = json.loads(measurements_json)

                # Check if new format (dict with images) or old format (list of measurements)
                if isinstance(loaded_data, dict) and "images" in loaded_data:
                    # New format: multiple images with per-image measurements
                    for img_info in loaded_data.get("images", []):
                        img_path = img_info.get("path")
                        if img_path and Path(img_path).exists():
                            pixmap = QPixmap(img_path)
                            if not pixmap.isNull():
                                self.calibration_images.append({
                                    "path": img_path,
                                    "pixmap": pixmap,
                                    "measurements": img_info.get("measurements", []),
                                    "crop_box": img_info.get("crop_box"),
                                    "crop_source_size": img_info.get("crop_source_size"),
                                })
                else:
                    # Old format: single image with all measurements
                    image_path = cal.get("image_filepath")
                    if image_path and Path(image_path).exists():
                        pixmap = QPixmap(image_path)
                        if not pixmap.isNull():
                            measurements = loaded_data if isinstance(loaded_data, list) else []
                            self.calibration_images.append({
                                "path": image_path,
                                "pixmap": pixmap,
                                "measurements": measurements,
                                "crop_box": None,
                                "crop_source_size": None,
                            })

            except json.JSONDecodeError:
                # Fallback: try loading single image
                image_path = cal.get("image_filepath")
                if image_path and Path(image_path).exists():
                    pixmap = QPixmap(image_path)
                    if not pixmap.isNull():
                        self.calibration_images.append({
                            "path": image_path,
                            "pixmap": pixmap,
                            "measurements": [],
                            "crop_box": None,
                            "crop_source_size": None,
                        })

        # Attach auto calibration data if available
        if isinstance(loaded_data, dict):
            auto_images = loaded_data.get("auto_images", [])
            if auto_images:
                for auto_info in auto_images:
                    idx = auto_info.get("index")
                    target = None
                    if isinstance(idx, int) and 0 <= idx < len(self.calibration_images):
                        target = self.calibration_images[idx]
                    else:
                        path = auto_info.get("path")
                        if path:
                            for img_data in self.calibration_images:
                                if img_data.get("path") == path:
                                    target = img_data
                                    break
                    if not target:
                        continue
                    result_dict = auto_info.get("result", {}) or {}
                    target["auto"] = {
                        "result": self._result_from_dict(result_dict),
                        "spacing_um": auto_info.get("spacing_um"),
                        "overlay_parabola": auto_info.get("overlay_parabola", []),
                        "overlay_edges": auto_info.get("overlay_edges", []),
                        "overlay_edges_50": auto_info.get("overlay_edges_50", []),
                    }
            elif "auto" in loaded_data and self.calibration_images:
                auto_info = loaded_data.get("auto") or {}
                result_dict = auto_info.get("result", auto_info)
                self.calibration_images[0]["auto"] = {
                    "result": self._result_from_dict(result_dict),
                    "spacing_um": auto_info.get("spacing_um"),
                    "overlay_parabola": auto_info.get("overlay_parabola", []),
                    "overlay_edges": auto_info.get("overlay_edges", []),
                    "overlay_edges_50": auto_info.get("overlay_edges_50", []),
                }

        # Update UI
        if self.calibration_images:
            self.current_image_index = 0
            self._show_current_image()
            self._refresh_image_gallery()
            self._update_measurement_list()
            self._update_results()
            self._update_auto_summary()
            if any("auto" in img for img in self.calibration_images):
                self.image_mode_tabs.setCurrentIndex(0)

        # Show notes
        self.notes_input.setText(cal.get("notes", ""))

    def _on_save_calibration(self):
        """Save the current calibration."""
        if not self.current_objective_key:
            QMessageBox.warning(
                self,
                self.tr("No Objective"),
                self.tr("Please select an objective first."),
            )
            return

        auto_values = self._collect_auto_values(self._auto_use_edges())
        using_auto = bool(auto_values)

        if using_auto:
            mean_nm = float(np.mean(auto_values))
            if mean_nm <= 0:
                QMessageBox.warning(
                    self,
                    self.tr("Invalid Result"),
                    self.tr("Auto calibration result is invalid."),
                )
                return
            std_nm = float(np.std(auto_values, ddof=1)) if len(auto_values) > 1 else None
            scale_um = nm_to_um(mean_nm)

            # Get previous active calibration for comparison
            old_calibration = CalibrationDB.get_active_calibration(self.current_objective_key)
            old_scale = old_calibration.get("microns_per_pixel") if old_calibration else None
            old_calibration_id = old_calibration.get("id") if old_calibration else None

            image_entries = []
            first_saved_path = None
            auto_images = []
            all_measurements = self._get_all_measurements()
            for idx, img_data in enumerate(self.calibration_images):
                saved_path = self._save_calibration_image(img_data["path"])
                if saved_path and first_saved_path is None:
                    first_saved_path = saved_path
                image_entries.append({
                    "index": idx,
                    "path": saved_path,
                    "measurements": img_data.get("measurements", []),
                    "crop_box": img_data.get("crop_box"),
                    "crop_source_size": img_data.get("crop_source_size"),
                })
                auto_data = img_data.get("auto")
                if not auto_data:
                    continue
                result = auto_data["result"]
                auto_images.append({
                    "index": idx,
                    "path": saved_path,
                    "crop_box": img_data.get("crop_box"),
                    "crop_source_size": img_data.get("crop_source_size"),
                    "spacing_um": auto_data.get("spacing_um"),
                    "result": {
                        "axis": result.axis,
                        "angle_deg": result.angle_deg,
                        "spacing_median_px": result.spacing_median_px,
                        "spacing_median_edges_px": result.spacing_median_edges_px,
                        "nm_per_px": result.nm_per_px,
                        "nm_per_px_edges": result.nm_per_px_edges,
                        "agreement_pct": result.agreement_pct,
                        "rel_scatter_mad_pct": result.rel_scatter_mad_pct,
                        "rel_scatter_iqr_pct": result.rel_scatter_iqr_pct,
                        "drift_slope": result.drift_slope,
                        "residual_slope_deg": result.residual_slope_deg,
                        "edges_px": result.edges_px.tolist() if hasattr(result, "edges_px") else [],
                    },
                    "overlay_parabola": auto_data.get("overlay_parabola", []),
                    "overlay_edges": auto_data.get("overlay_edges", []),
                    "overlay_edges_50": auto_data.get("overlay_edges_50", []),
                })

            calibration_data = {
                "images": image_entries,
                "measurements": all_measurements,
                "auto_images": auto_images,
                "auto_summary": {
                    "method": "edges" if self._auto_use_edges() else "parabola",
                    "average_nm_per_px": mean_nm,
                    "max_deviation_nm_per_px": float(np.max(np.abs(np.array(auto_values) - mean_nm))),
                    "n_images": len(auto_values),
                },
            }
            notes = self.tr("Automatic image calibration")
            image_filepath = first_saved_path
            cal_width, cal_height = self._collect_image_dimensions_summary()
            resample_factor = 1.0

            calibration_id = CalibrationDB.add_calibration(
                objective_key=self.current_objective_key,
                microns_per_pixel=scale_um,
                microns_per_pixel_std=nm_to_um(std_nm) if std_nm is not None else None,
                num_measurements=len(auto_images),
                measurements_json=json.dumps(calibration_data),
                image_filepath=image_filepath,
                camera=self._selected_camera_text(),
                megapixels=self._collect_megapixels_summary(),
                target_sampling_pct=float(self.target_sampling_pct),
                resample_scale_factor=resample_factor,
                calibration_image_width=cal_width,
                calibration_image_height=cal_height,
                notes=notes,
                set_active=True,
            )

            if self.current_objective_key in self.objectives:
                self.objectives[self.current_objective_key]["microns_per_pixel"] = scale_um
                self.objectives[self.current_objective_key]["target_sampling_pct"] = float(self.target_sampling_pct)
                self.objectives[self.current_objective_key]["resample_scale_factor"] = resample_factor
                save_objectives(self.objectives)

            self._prompt_recalculate_measurements(old_calibration_id, old_scale, calibration_id, scale_um)

            self._clear_all()
            self._on_objective_changed()
            return

        # Manual image calibration (no auto results)
        all_measurements = self._get_all_measurements()
        if not all_measurements:
            QMessageBox.warning(
                self,
                self.tr("No Measurements"),
                self.tr("Please add at least one measurement."),
            )
            return

        # Calculate statistics
        measurement_tuples = [(m["known_um"], m["measured_px"]) for m in all_measurements]
        mean, std, ci_low, ci_high = calculate_calibration_stats(measurement_tuples)

        if mean is None:
            QMessageBox.warning(
                self,
                self.tr("Error"),
                self.tr("Could not calculate scale from measurements."),
            )
            return

        # Get previous active calibration for comparison
        old_calibration = CalibrationDB.get_active_calibration(self.current_objective_key)
        old_scale = old_calibration.get("microns_per_pixel") if old_calibration else None
        old_calibration_id = old_calibration.get("id") if old_calibration else None

        # Save ALL calibration images and build calibration data
        saved_image_paths = []
        calibration_data = {
            "images": [],
            "measurements": all_measurements,
        }
        for idx, img_data in enumerate(self.calibration_images):
            saved_path = self._save_calibration_image(img_data["path"])
            if saved_path:
                saved_image_paths.append(saved_path)
                calibration_data["images"].append({
                    "index": idx,
                    "path": saved_path,
                    "measurements": img_data.get("measurements", []),
                    "crop_box": img_data.get("crop_box"),
                    "crop_source_size": img_data.get("crop_source_size"),
                })

        # First image filepath for backward compatibility
        image_filepath = saved_image_paths[0] if saved_image_paths else None

        notes = self.notes_input.text().strip()
        if not notes:
            notes = self.tr("Manual image calibration")
        elif "Manual image calibration" not in notes:
            notes = f"{notes} | {self.tr('Manual image calibration')}"

        cal_width, cal_height = self._collect_image_dimensions_summary()
        resample_factor = 1.0

        # Save to database
        calibration_id = CalibrationDB.add_calibration(
            objective_key=self.current_objective_key,
            microns_per_pixel=mean,
            microns_per_pixel_std=std,
            confidence_interval_low=ci_low,
            confidence_interval_high=ci_high,
            num_measurements=len(all_measurements),
            measurements_json=json.dumps(calibration_data),
            image_filepath=image_filepath,
            camera=self._selected_camera_text(),
            megapixels=self._collect_megapixels_summary(),
            target_sampling_pct=float(self.target_sampling_pct),
            resample_scale_factor=resample_factor,
            calibration_image_width=cal_width,
            calibration_image_height=cal_height,
            notes=notes,
            set_active=True,
        )

        # Update objectives.json
        if self.current_objective_key in self.objectives:
            self.objectives[self.current_objective_key]["microns_per_pixel"] = mean
            self.objectives[self.current_objective_key]["target_sampling_pct"] = float(self.target_sampling_pct)
            self.objectives[self.current_objective_key]["resample_scale_factor"] = resample_factor
            save_objectives(self.objectives)

        # Prompt to update existing measurements if scale changed
        self._prompt_recalculate_measurements(old_calibration_id, old_scale, calibration_id, mean)

        # Clear the current calibration state and refresh
        self._clear_all()
        self._on_objective_changed()
    def _on_save_manual_calibration(self):
        """Save a manual calibration entry."""
        if not self.current_objective_key:
            QMessageBox.warning(
                self,
                self.tr("No Objective"),
                self.tr("Please select an objective first."),
            )
            return

        if self._collect_auto_values(self._auto_use_edges()):
            self._show_wrapped_info_dialog(
                self.tr("Auto Calibration Available"),
                self.tr("Auto calibration results are available. Use Save Calibration in the image tab."),
                min_height=220,
            )
            return

        scale_nm = self.manual_scale_input.value()
        if scale_nm <= 0:
            QMessageBox.warning(
                self,
                self.tr("Invalid Scale"),
                self.tr("Please enter a valid scale value."),
            )
            return

        # Convert nm to um for storage
        scale_um = nm_to_um(scale_nm)

        # Get previous active calibration for comparison
        old_calibration = CalibrationDB.get_active_calibration(self.current_objective_key)
        old_scale = old_calibration.get("microns_per_pixel") if old_calibration else None
        old_calibration_id = old_calibration.get("id") if old_calibration else None

        notes = self.manual_notes_input.text().strip()
        if not notes:
            notes = self.tr("Manually entered scale")
        elif "Manually entered scale" not in notes:
            notes = f"{notes} | {self.tr('Manually entered scale')}"

        cal_width, cal_height = self._collect_image_dimensions_summary()
        resample_factor = 1.0

        # Save to database
        calibration_id = CalibrationDB.add_calibration(
            objective_key=self.current_objective_key,
            microns_per_pixel=scale_um,
            num_measurements=0,  # Manual entry
            camera=self._selected_camera_text(),
            megapixels=self._collect_megapixels_summary(),
            target_sampling_pct=float(self.target_sampling_pct),
            resample_scale_factor=resample_factor,
            calibration_image_width=cal_width,
            calibration_image_height=cal_height,
            notes=notes,
            set_active=True,
        )

        # Update objectives.json
        if self.current_objective_key in self.objectives:
            self.objectives[self.current_objective_key]["microns_per_pixel"] = scale_um
            self.objectives[self.current_objective_key]["target_sampling_pct"] = float(self.target_sampling_pct)
            self.objectives[self.current_objective_key]["resample_scale_factor"] = resample_factor
            save_objectives(self.objectives)

        # Prompt to update existing measurements if scale changed
        self._prompt_recalculate_measurements(old_calibration_id, old_scale, calibration_id, scale_um)

        self._on_objective_changed()
    def _prompt_recalculate_measurements(
        self,
        old_calibration_id: Optional[int],
        old_scale: Optional[float],
        new_calibration_id: int,
        new_scale: float
    ):
        """Prompt user to recalculate measurements if calibration scale changed significantly."""
        if old_calibration_id is None or old_scale is None or old_scale <= 0:
            return

        # Check if there are images using the old calibration
        usage_summary = CalibrationDB.get_calibration_usage_summary(self.current_objective_key)
        old_usage = next((u for u in usage_summary if u["calibration_id"] == old_calibration_id), None)

        if not old_usage:
            return

        image_count = old_usage.get("image_count", 0)
        measurement_count = old_usage.get("measurement_count", 0)

        if image_count == 0 and measurement_count == 0:
            return

        # Calculate percentage difference
        diff_percent = ((new_scale - old_scale) / old_scale) * 100
        if abs(diff_percent) < 0.2:
            return
        sign = "+" if diff_percent >= 0 else ""

        old_nm = um_to_nm(old_scale)
        new_nm = um_to_nm(new_scale)

        # Show dialog asking if user wants to update measurements
        msg = self.tr(
            "The calibration scale has changed from {old:.2f} to {new:.2f} nm/px ({sign}{diff:.2f}%).\n\n"
            "There are {images} images with {measurements} spore measurements using the old calibration.\n\n"
            "Would you like to update these images to use the new calibration and recalculate the measurements?"
        ).format(
            old=old_nm,
            new=new_nm,
            sign=sign,
            diff=diff_percent,
            images=image_count,
            measurements=measurement_count,
        )

        reply = QMessageBox.question(
            self,
            self.tr("Update Measurements?"),
            msg,
            QMessageBox.Yes | QMessageBox.No,
        )

        if reply == QMessageBox.Yes:
            updated = CalibrationDB.recalculate_measurements_for_calibration(
                old_calibration_id, new_calibration_id, new_scale
            )
            QMessageBox.information(
                self,
                self.tr("Measurements Updated"),
                self.tr("Updated {count} spore measurements to use the new calibration.").format(count=updated),
            )

    def _save_calibration_image(self, source_path: str) -> Optional[str]:
        """Save a calibration image to the calibrations directory."""
        if not source_path or not self.current_objective_key:
            return None

        # Create directory
        cal_dir = get_calibrations_dir() / self.current_objective_key
        cal_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename
        date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        source = Path(source_path)
        filename = f"{date_str}_{uuid4().hex[:8]}{source.suffix}"
        dest_path = cal_dir / filename

        try:
            shutil.copy2(source_path, dest_path)
            return str(dest_path)
        except Exception as e:
            print(f"Warning: Could not copy calibration image: {e}")
            return None

    def _emit_calibration(self, microns_per_pixel: float):
        """Emit the calibration_saved signal with objective data."""
        if not self.current_objective_key:
            return

        obj = self.objectives.get(self.current_objective_key, {})
        objective_data = {
            "key": self.current_objective_key,
            "objective_key": self.current_objective_key,
            "name": objective_display_name(obj, self.current_objective_key),
            "objective_name": obj.get("objective_name"),
            "magnification": obj.get("magnification"),
            "na": obj.get("na"),
            "microns_per_pixel": microns_per_pixel,
            "notes": obj.get("notes", ""),
        }
        self.calibration_saved.emit(objective_data)

    def select_custom_tab(self):
        """Select the calibration tab (for compatibility)."""
        self.tab_widget.setCurrentIndex(0)

    def select_objective_key(self, objective_key: str) -> None:
        """Select an objective by key in the combo."""
        if not objective_key or not hasattr(self, "objective_combo"):
            return
        idx = self.objective_combo.findData(objective_key)
        if idx >= 0:
            self.objective_combo.setCurrentIndex(idx)

    def select_calibration(self, calibration_id: int) -> None:
        """Select a calibration row in the history table and load it."""
        if not calibration_id or not hasattr(self, "history_table"):
            return
        if not hasattr(self, "_history_calibration_ids"):
            self._update_history_table()
        if not hasattr(self, "_history_calibration_ids"):
            return
        try:
            row = self._history_calibration_ids.index(calibration_id)
        except ValueError:
            return
        self.history_table.selectRow(row)
        item = self.history_table.item(row, 0)
        if item is not None:
            self.history_table.scrollToItem(item)
        self._on_history_row_clicked(row, 0)

    # Backward compatibility methods for in-place calibration
    # The new dialog handles calibration internally, so these are stubs

    def set_calibration_distance(self, distance_pixels: float):
        """Backward compatibility stub. New dialog handles this internally."""
        pass

    def set_calibration_preview(self, pixmap: QPixmap, points: list):
        """Backward compatibility stub. New dialog handles this internally."""
        pass

    def get_last_used_objective(self):
        """Get the last used objective data, or the default objective if set."""
        # First check for a default objective
        for key, obj in self.objectives.items():
            if obj.get("is_default", False):
                # Get active calibration scale
                active_cal = CalibrationDB.get_active_calibration(key)
                if active_cal:
                    obj = dict(obj)
                    obj["microns_per_pixel"] = active_cal.get("microns_per_pixel", obj.get("microns_per_pixel", 0))
                obj = dict(obj)
                obj["key"] = key
                return obj

        # Fall back to last used
        last_used_file = get_last_objective_path()
        if last_used_file.exists():
            try:
                with open(last_used_file, 'r') as f:
                    last_used = json.load(f)
                    key = last_used.get("objective_key") or last_used.get("key") or last_used.get("magnification", "")
                    if key in self.objectives:
                        obj = self.objectives[key]
                        # Get active calibration scale
                        active_cal = CalibrationDB.get_active_calibration(key)
                        if active_cal:
                            obj = dict(obj)
                            obj["microns_per_pixel"] = active_cal.get("microns_per_pixel", obj.get("microns_per_pixel", 0))
                        obj = dict(obj)
                        obj["key"] = key
                        return obj
            except (json.JSONDecodeError, IOError):
                pass

        # Fall back to first objective
        if self.objectives:
            key = sorted(self.objectives.keys())[0]
            obj = dict(self.objectives[key])
            active_cal = CalibrationDB.get_active_calibration(key)
            if active_cal:
                obj = dict(obj)
                obj["microns_per_pixel"] = active_cal.get("microns_per_pixel", obj.get("microns_per_pixel", 0))
            obj["key"] = key
            return obj

        return None
