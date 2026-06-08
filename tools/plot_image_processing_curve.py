#!/usr/bin/env python3
"""Inspect Sporely's post-decode curve, either as a snapshot or interactively."""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path

import numpy as np
from PIL import Image
from PySide6.QtCore import QPointF, Qt, QSignalBlocker, QTimer
from PySide6.QtGui import QImage, QPixmap
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QFileDialog,
    QLabel,
    QMainWindow,
    QPushButton,
    QSizePolicy,
    QSpinBox,
    QSlider,
    QSplitter,
    QVBoxLayout,
    QWidget,
)
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

if __package__ in {None, ""}:
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from utils.image_processing_pipeline import (  # noqa: E402
    apply_post_decode_processing,
    compute_post_decode_transfer_curve,
    to_float_rgb,
)
from config import LOCAL_IMPORT_IMAGE_FILTER  # noqa: E402
from utils.raw_detection import is_raw_image_path  # noqa: E402
from utils.raw_render import RawRenderSettings, RawRenderingUnavailableError, render_raw_preview  # noqa: E402
from utils.raw_white_balance import estimate_white_balance_from_background  # noqa: E402
from ui.zoomable_image_widget import ZoomableImageLabel  # noqa: E402

_CURVE_CUTOFF_SCALE = 50
_CURVE_STRENGTH_SCALE = 100
_CURVE_TONE_SCALE = 100


@dataclass(slots=True)
class LoadedSource:
    kind: str
    rgb: np.ndarray
    raw_base_mode: str | None = None


def _load_raster_rgb(path: Path) -> np.ndarray:
    with Image.open(path) as image:
        image.load()
        return to_float_rgb(np.array(image))


def _load_raw_rgb(path: Path, *, base_mode: str, scratch_dir: Path) -> np.ndarray:
    scratch_dir.mkdir(parents=True, exist_ok=True)
    preview_path = scratch_dir / f"{path.stem}_{base_mode}_base.jpg"
    preview_settings = RawRenderSettings(
        white_balance_mode=base_mode,
        wb_sample_base_mode=base_mode,
        auto_levels=False,
        tone_curve_enabled=False,
    )
    try:
        rendered_preview = render_raw_preview(
            path,
            settings=preview_settings,
            output_path=preview_path,
            output_dir=scratch_dir,
        )
    except RawRenderingUnavailableError as exc:
        raise RuntimeError(f"RAW preview unavailable for {path.name}: {exc}") from exc
    with Image.open(rendered_preview) as image:
        image.load()
        return to_float_rgb(np.array(image))


def _load_source_image(path: Path, *, raw_base_mode: str = "camera", scratch_dir: Path | None = None) -> LoadedSource:
    if is_raw_image_path(path):
        scratch = scratch_dir or Path(tempfile.gettempdir()) / "sporely_curve_inspector"
        return LoadedSource("raw", _load_raw_rgb(path, base_mode=raw_base_mode, scratch_dir=scratch), raw_base_mode)
    return LoadedSource("raster", _load_raster_rgb(path))


def _save_preview(path: Path, rgb: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    image = np.clip(np.asarray(rgb, dtype=np.float64)[..., :3], 0.0, 1.0)
    image8 = np.rint(image * 255.0).astype(np.uint8)
    Image.fromarray(image8, mode="RGB").save(path, format="PNG")


def _rgb_to_pixmap(rgb: np.ndarray) -> QPixmap:
    image = np.clip(np.asarray(rgb, dtype=np.float64)[..., :3], 0.0, 1.0)
    image8 = np.rint(image * 255.0).astype(np.uint8)
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


def _build_settings(args: argparse.Namespace) -> RawRenderSettings:
    return RawRenderSettings(
        auto_levels=bool(args.auto_levels),
        black_percentile=float(args.dark_cutoff),
        white_percentile=1.0 - float(args.bright_cutoff),
        auto_levels_strength=float(args.auto_levels_strength),
        auto_levels_soft_tails=bool(args.soft_tails),
        auto_levels_tail_size=float(args.tail_size),
        auto_levels_shadow_lift=max(0.0, min(0.25, float(args.shadow_lift))),
        tone_curve_enabled=bool(args.tone_curve),
        tone_curve_strength=float(args.curve_strength),
        tone_curve_midpoint=float(args.curve_midpoint),
    )


def _open_file_filter() -> str:
    return f"{LOCAL_IMPORT_IMAGE_FILTER};;All Files (*)"


def _draw_processing_figure(
    figure: Figure,
    source_rgb: np.ndarray,
    settings: RawRenderSettings,
    *,
    debug=None,
) -> None:
    curve = compute_post_decode_transfer_curve(source_rgb, settings, debug=debug)

    figure.clear()
    ax = figure.add_subplot(111)

    input_values = curve.input_values
    ax.plot(input_values, input_values, label="identity", linewidth=1.0, color="#95a5a6", linestyle="--")
    ax.plot(input_values, curve.hard_target, label="hard auto-level target", linewidth=1.8, color="#7f8c8d", linestyle=":")
    if (
        settings.auto_levels
        and settings.auto_levels_soft_tails
        and curve.debug.black_level is not None
        and curve.debug.white_level is not None
    ):
        ax.plot(input_values, curve.soft_target, label="soft-tail target", linewidth=2.0, color="#e67e22", linestyle="--")
    ax.plot(
        input_values,
        curve.auto_levels_output,
        label="strength-blended auto-level output",
        linewidth=2.4,
        color="#2980b9",
    )
    ax.plot(
        input_values,
        curve.final_output,
        label="final output after tone curve",
        linewidth=2.4,
        color="#16a085",
    )
    if curve.debug.black_level is not None:
        ax.axvline(curve.debug.black_level, color="#2c3e50", linestyle="--", linewidth=1.0, alpha=0.75, label="black level")
    if curve.debug.white_level is not None:
        ax.axvline(curve.debug.white_level, color="#c0392b", linestyle="--", linewidth=1.0, alpha=0.75, label="white level")
    if (
        settings.auto_levels
        and settings.auto_levels_soft_tails
        and curve.debug.black_level is not None
        and curve.debug.white_level is not None
        and curve.debug.input_min < float(curve.debug.black_level)
    ):
        ax.axvspan(curve.debug.input_min, float(curve.debug.black_level), color="#2c3e50", alpha=0.06)
    if (
        settings.auto_levels
        and settings.auto_levels_soft_tails
        and curve.debug.black_level is not None
        and curve.debug.white_level is not None
        and curve.debug.input_max > float(curve.debug.white_level)
    ):
        ax.axvspan(float(curve.debug.white_level), curve.debug.input_max, color="#c0392b", alpha=0.06)
    ax.set_xlim(0.0, 1.0)
    ax.set_ylim(0.0, 1.0)
    ax.set_xlabel("Input luminance")
    ax.set_ylabel("Output luminance")
    ax.set_title("Processing curve")
    ax.grid(True, alpha=0.2)
    ax.legend(loc="best", ncols=2)


def _build_processing_figure(source_rgb: np.ndarray, settings: RawRenderSettings, *, debug=None) -> Figure:
    figure = Figure(figsize=(8.4, 6.1), dpi=120, constrained_layout=True)
    _draw_processing_figure(figure, source_rgb, settings, debug=debug)
    return figure


def _save_plot(path: Path, source_rgb: np.ndarray, settings: RawRenderSettings, *, debug=None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    figure = _build_processing_figure(source_rgb, settings, debug=debug)
    figure.savefig(path, dpi=160)


def _settings_summary(settings: RawRenderSettings) -> str:
    wb_desc = str(settings.white_balance_mode or "camera").strip() or "camera"
    if settings.wb_multipliers is not None:
        wb_desc = f"custom {settings.wb_multipliers[0]:.2f}/{settings.wb_multipliers[1]:.2f}/{settings.wb_multipliers[2]:.2f}"
    dark_cutoff = max(0.0, min(0.5, float(settings.black_percentile) * 100.0))
    bright_cutoff = max(0.0, min(0.5, (1.0 - float(settings.white_percentile)) * 100.0))
    auto_levels_strength = max(0.0, min(100.0, float(settings.auto_levels_strength) * 100.0))
    shadow_lift = max(0.0, min(25.0, float(settings.auto_levels_shadow_lift) * 100.0))
    parts = [
        f"WB {wb_desc}",
        "auto levels on" if settings.auto_levels else "auto levels off",
        f"strength {auto_levels_strength:.0f}%",
        "soft tails on" if settings.auto_levels_soft_tails else "soft tails off",
        f"shadow lift {shadow_lift:.1f}%",
        f"dark cutoff {dark_cutoff:.2f}%",
        f"bright cutoff {bright_cutoff:.2f}%",
        "tone curve on" if settings.tone_curve_enabled else "tone curve off",
        f"curve strength {float(settings.tone_curve_strength):.2f}",
        f"curve midpoint {float(settings.tone_curve_midpoint):.2f}",
    ]
    if settings.auto_levels_soft_tails:
        parts.append(f"tail {float(settings.auto_levels_tail_size):.2f}")
    if settings.wb_sample_base_mode:
        parts.append(f"base {settings.wb_sample_base_mode}")
    return " · ".join(parts)


class ProcessingCurveWindow(QMainWindow):
    """Interactive inspector for Sporely's post-decode pipeline."""

    def __init__(
        self,
        source_path: Path,
        *,
        plot_out: Path | None = None,
        preview_out: Path | None = None,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._source_path = source_path
        self._plot_out = plot_out
        self._preview_out = preview_out
        self._is_raw = is_raw_image_path(source_path)
        self._raw_base_mode = "camera"
        self._raw_tempdir = tempfile.TemporaryDirectory(prefix="sporely_curve_") if self._is_raw else None
        self._source = self._load_source()
        self._settings = RawRenderSettings.default()
        self._wb_pick_armed = False
        self._refresh_timer = QTimer(self)
        self._refresh_timer.setSingleShot(True)
        self._refresh_timer.timeout.connect(self._refresh_outputs)
        self._build_ui()
        self._refresh_outputs()

    def closeEvent(self, event) -> None:  # type: ignore[override]
        try:
            self._refresh_timer.stop()
            if self._raw_tempdir is not None:
                self._raw_tempdir.cleanup()
        finally:
            super().closeEvent(event)

    def _load_source(self) -> LoadedSource:
        if self._is_raw:
            scratch_dir = Path(self._raw_tempdir.name) if self._raw_tempdir is not None else None
            return _load_source_image(self._source_path, raw_base_mode=self._raw_base_mode, scratch_dir=scratch_dir)
        return _load_source_image(self._source_path)

    def _build_ui(self) -> None:
        self.setWindowTitle(f"Sporely curve inspector - {self._source_path.name}")
        self.resize(1500, 920)

        central = QWidget(self)
        self.setCentralWidget(central)
        root_layout = QHBoxLayout(central)
        root_layout.setContentsMargins(12, 12, 12, 12)
        root_layout.setSpacing(12)

        root_layout.addWidget(self._build_controls_panel(), 0)
        root_layout.addWidget(self._build_display_panel(), 1)

    def _build_controls_panel(self) -> QWidget:
        panel = QWidget(self)
        panel.setMinimumWidth(330)
        panel.setMaximumWidth(420)
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        source_group = QGroupBox("Source")
        source_form = QFormLayout(source_group)
        source_form.setLabelAlignment(Qt.AlignRight)
        self.source_path_label = QLabel(str(self._source_path))
        self.source_path_label.setWordWrap(True)
        self.source_kind_label = QLabel("RAW" if self._is_raw else "Raster")
        self.source_kind_label.setWordWrap(True)
        source_form.addRow("Image", self.source_path_label)
        source_form.addRow("Kind", self.source_kind_label)
        self.raw_base_mode_label = QLabel("Raw base")
        self.raw_base_mode_combo = QComboBox()
        self.raw_base_mode_combo.addItem("Camera", "camera")
        self.raw_base_mode_combo.addItem("Auto", "auto")
        self._set_combo_data(self.raw_base_mode_combo, self._raw_base_mode)
        self.raw_base_mode_combo.currentIndexChanged.connect(self._on_raw_base_mode_changed)
        source_form.addRow(self.raw_base_mode_label, self.raw_base_mode_combo)

        self.open_file_btn = QPushButton("Open file...")
        self.open_file_btn.clicked.connect(self._open_source_dialog)
        source_form.addRow(self.open_file_btn)
        layout.addWidget(source_group)
        self._update_source_widgets()

        wb_group = QGroupBox("White balance")
        wb_form = QFormLayout(wb_group)
        wb_form.setLabelAlignment(Qt.AlignRight)

        self.wb_mode_combo = QComboBox()
        self.wb_mode_combo.addItem("Camera", "camera")
        self.wb_mode_combo.addItem("Auto", "auto")
        self.wb_mode_combo.addItem("Custom", "custom")
        self._set_combo_data(self.wb_mode_combo, "camera")
        self.wb_mode_combo.currentIndexChanged.connect(self._on_wb_mode_changed)
        wb_form.addRow("Mode", self.wb_mode_combo)

        self.wb_sample_size_spin = QSpinBox()
        self.wb_sample_size_spin.setRange(1, 128)
        self.wb_sample_size_spin.setValue(10)
        self.wb_sample_size_spin.valueChanged.connect(self._queue_refresh)
        wb_form.addRow("Sample size", self.wb_sample_size_spin)

        self.wb_pick_btn = QPushButton("Pick background WB")
        self.wb_pick_btn.setCheckable(True)
        self.wb_pick_btn.toggled.connect(self._toggle_wb_pick)
        wb_form.addRow(self.wb_pick_btn)

        self.wb_readout_label = QLabel("Camera WB")
        self.wb_readout_label.setWordWrap(True)
        wb_form.addRow("Readout", self.wb_readout_label)
        layout.addWidget(wb_group)

        controls_group = QGroupBox("Adjustments")
        controls_form = QFormLayout(controls_group)
        controls_form.setLabelAlignment(Qt.AlignRight)

        self.auto_levels_checkbox = QCheckBox("Enable auto-levels")
        self.auto_levels_checkbox.setChecked(True)
        self.auto_levels_checkbox.toggled.connect(self._queue_refresh)
        controls_form.addRow(self.auto_levels_checkbox)

        auto_levels_strength_row, self.auto_levels_strength_slider, self.auto_levels_strength_value_label = self._create_percent_slider_row(
            value=1.0,
            label_formatter=lambda value: f"{value:.0f}%",
        )
        controls_form.addRow("Auto-level strength", auto_levels_strength_row)

        self.soft_tails_checkbox = QCheckBox("Enable soft tails")
        self.soft_tails_checkbox.setChecked(False)
        self.soft_tails_checkbox.toggled.connect(self._queue_refresh)
        controls_form.addRow(self.soft_tails_checkbox)

        shadow_lift_row, self.shadow_lift_slider, self.shadow_lift_value_label = self._create_shadow_lift_slider_row(
            value=0.0,
        )
        controls_form.addRow("Shadow lift", shadow_lift_row)

        dark_cutoff_row, self.dark_cutoff_slider, self.dark_cutoff_value_label = self._create_cutoff_slider_row(
            cutoff_percent=0.05,
        )
        self.black_quantile_slider = self.dark_cutoff_slider
        self.black_quantile_value_label = self.dark_cutoff_value_label
        controls_form.addRow("Dark cutoff", dark_cutoff_row)

        bright_cutoff_row, self.bright_cutoff_slider, self.bright_cutoff_value_label = self._create_cutoff_slider_row(
            cutoff_percent=0.05,
        )
        self.white_quantile_slider = self.bright_cutoff_slider
        self.white_quantile_value_label = self.bright_cutoff_value_label
        controls_form.addRow("Bright cutoff", bright_cutoff_row)

        self.tone_curve_checkbox = QCheckBox("Enable tone curve")
        self.tone_curve_checkbox.setChecked(False)
        self.tone_curve_checkbox.toggled.connect(self._queue_refresh)
        controls_form.addRow(self.tone_curve_checkbox)

        curve_strength_row, self.curve_strength_slider, self.curve_strength_value_label = self._create_float_slider_row(
            value=0.50,
            scale=_CURVE_TONE_SCALE,
            decimals=2,
        )
        controls_form.addRow("Curve strength", curve_strength_row)

        curve_midpoint_row, self.curve_midpoint_slider, self.curve_midpoint_value_label = self._create_float_slider_row(
            value=0.50,
            scale=_CURVE_TONE_SCALE,
            decimals=2,
        )
        controls_form.addRow("Curve midpoint", curve_midpoint_row)
        layout.addWidget(controls_group)

        stats_group = QGroupBox("Current output")
        stats_form = QFormLayout(stats_group)
        stats_form.setLabelAlignment(Qt.AlignRight)
        self.input_min_label = QLabel("—")
        self.input_max_label = QLabel("—")
        self.black_level_label = QLabel("—")
        self.white_level_label = QLabel("—")
        self.settings_label = QLabel("—")
        self.settings_label.setWordWrap(True)
        stats_form.addRow("Input min", self.input_min_label)
        stats_form.addRow("Input max", self.input_max_label)
        stats_form.addRow("Black level", self.black_level_label)
        stats_form.addRow("White level", self.white_level_label)
        stats_form.addRow("Settings", self.settings_label)
        layout.addWidget(stats_group)

        layout.addStretch(1)
        return panel

    def _build_display_panel(self) -> QWidget:
        panel = QWidget(self)
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        splitter = QSplitter(Qt.Vertical)
        splitter.setChildrenCollapsible(False)
        splitter.addWidget(self._build_preview_panel())
        splitter.addWidget(self._build_graph_panel())
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 1)
        layout.addWidget(splitter, 1)
        return panel

    def _build_preview_panel(self) -> QWidget:
        panel = QWidget(self)
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        heading = QLabel("Processed preview")
        heading.setStyleSheet("font-weight: 600;")
        layout.addWidget(heading, 0)

        self.preview_label = ZoomableImageLabel(self)
        self.preview_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.preview_label.setMinimumSize(320, 240)
        self.preview_label.set_pan_without_shift(True)
        self.preview_label.clicked.connect(self._on_preview_clicked)
        layout.addWidget(self.preview_label, 1)
        return panel

    def _build_graph_panel(self) -> QWidget:
        panel = QWidget(self)
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        heading = QLabel("Processing curves")
        heading.setStyleSheet("font-weight: 600;")
        layout.addWidget(heading, 0)

        self.figure = Figure(figsize=(8.4, 6.1), dpi=120, constrained_layout=True)
        self.canvas = FigureCanvas(self.figure)
        self.canvas.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.canvas.setMinimumHeight(280)
        layout.addWidget(self.canvas, 1)
        return panel

    def _create_percent_slider_row(
        self,
        *,
        value: float,
        label_formatter,
    ) -> tuple[QWidget, QSlider, QLabel]:
        row = QWidget(self)
        row_layout = QHBoxLayout(row)
        row_layout.setContentsMargins(0, 0, 0, 0)
        row_layout.setSpacing(8)

        slider = QSlider(Qt.Horizontal, row)
        slider.setRange(0, _CURVE_STRENGTH_SCALE)
        slider.setSingleStep(1)
        slider.setPageStep(10)
        slider.setValue(self._float_to_slider_value(value, _CURVE_STRENGTH_SCALE))
        row_layout.addWidget(slider, 1)

        value_label = QLabel(row)
        value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        value_label.setMinimumWidth(72)
        value_label.setText(label_formatter(self._slider_value_float(slider.value(), _CURVE_STRENGTH_SCALE) * 100.0))
        row_layout.addWidget(value_label, 0)

        def _sync_slider_value(slider_value: int) -> None:
            percent_value = self._slider_value_float(slider_value, _CURVE_STRENGTH_SCALE) * 100.0
            value_label.setText(label_formatter(percent_value))
            self._queue_refresh()

        slider.valueChanged.connect(_sync_slider_value)
        return row, slider, value_label

    def _create_cutoff_slider_row(self, *, cutoff_percent: float) -> tuple[QWidget, QSlider, QLabel]:
        row = QWidget(self)
        row_layout = QHBoxLayout(row)
        row_layout.setContentsMargins(0, 0, 0, 0)
        row_layout.setSpacing(8)

        slider = QSlider(Qt.Horizontal, row)
        slider.setRange(0, _CURVE_CUTOFF_SCALE)
        slider.setSingleStep(1)
        slider.setPageStep(5)
        slider.setValue(self._cutoff_percent_to_slider_value(cutoff_percent))
        row_layout.addWidget(slider, 1)

        value_label = QLabel(row)
        value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        value_label.setMinimumWidth(72)
        value_label.setText(self._cutoff_slider_value_text(slider.value()))
        row_layout.addWidget(value_label, 0)

        def _sync_slider_value(slider_value: int) -> None:
            value_label.setText(self._cutoff_slider_value_text(slider_value))
            self._queue_refresh()

        slider.valueChanged.connect(_sync_slider_value)
        return row, slider, value_label

    def _create_shadow_lift_slider_row(self, *, value: float) -> tuple[QWidget, QSlider, QLabel]:
        row = QWidget(self)
        row_layout = QHBoxLayout(row)
        row_layout.setContentsMargins(0, 0, 0, 0)
        row_layout.setSpacing(8)

        slider = QSlider(Qt.Horizontal, row)
        slider.setRange(0, 200)
        slider.setSingleStep(1)
        slider.setPageStep(10)
        slider.setValue(self._shadow_lift_fraction_to_slider_value(value))
        row_layout.addWidget(slider, 1)

        value_label = QLabel(row)
        value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        value_label.setMinimumWidth(72)
        value_label.setText(self._shadow_lift_slider_value_text(slider.value()))
        row_layout.addWidget(value_label, 0)

        def _sync_slider_value(slider_value: int) -> None:
            value_label.setText(self._shadow_lift_slider_value_text(slider_value))
            self._queue_refresh()

        slider.valueChanged.connect(_sync_slider_value)
        return row, slider, value_label

    def _create_float_slider_row(self, *, value: float, scale: int, decimals: int) -> tuple[QWidget, QSlider, QLabel]:
        row = QWidget(self)
        row_layout = QHBoxLayout(row)
        row_layout.setContentsMargins(0, 0, 0, 0)
        row_layout.setSpacing(8)

        slider = QSlider(Qt.Horizontal, row)
        slider.setRange(0, scale)
        slider.setSingleStep(1)
        slider.setPageStep(max(1, scale // 20))
        slider.setValue(self._float_to_slider_value(value, scale))
        row_layout.addWidget(slider, 1)

        value_label = QLabel(row)
        value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        value_label.setMinimumWidth(72)
        value_label.setText(self._slider_value_text(slider.value(), scale, decimals))
        row_layout.addWidget(value_label, 0)

        def _sync_slider_value(slider_value: int) -> None:
            value_label.setText(self._slider_value_text(slider_value, scale, decimals))
            self._queue_refresh()

        slider.valueChanged.connect(_sync_slider_value)
        return row, slider, value_label

    @staticmethod
    def _float_to_slider_value(value: float, scale: int) -> int:
        return int(round(max(0.0, min(1.0, float(value))) * float(scale)))

    @staticmethod
    def _slider_value_float(value: int, scale: int) -> float:
        return max(0.0, min(1.0, float(value) / float(scale)))

    @classmethod
    def _slider_value_text(cls, value: int, scale: int, decimals: int) -> str:
        return f"{cls._slider_value_float(value, scale):.{decimals}f}"

    @staticmethod
    def _cutoff_percent_to_slider_value(cutoff_percent: float) -> int:
        return int(round(max(0.0, min(0.5, float(cutoff_percent))) * 100.0))

    @staticmethod
    def _cutoff_slider_value_to_percent(value: int) -> float:
        return max(0.0, min(0.5, float(value) / 100.0))

    @classmethod
    def _cutoff_slider_value_text(cls, value: int) -> str:
        return f"{cls._cutoff_slider_value_to_percent(value):.2f}%"

    @staticmethod
    def _shadow_lift_fraction_to_slider_value(value: float) -> int:
        return int(round(max(0.0, min(0.2, float(value))) * 1000.0))

    @staticmethod
    def _shadow_lift_slider_value_to_fraction(value: int) -> float:
        return max(0.0, min(0.2, float(value) / 1000.0))

    @classmethod
    def _shadow_lift_slider_value_text(cls, value: int) -> str:
        return f"{cls._shadow_lift_slider_value_to_fraction(value) * 100.0:.1f}%"

    def _set_combo_data(self, combo: QComboBox, data: str) -> None:
        index = combo.findData(data)
        if index >= 0:
            with QSignalBlocker(combo):
                combo.setCurrentIndex(index)

    def _sync_wb_controls_from_settings(self, settings: RawRenderSettings) -> None:
        self._set_combo_data(self.wb_mode_combo, settings.white_balance_mode if settings.white_balance_mode in {"camera", "auto", "custom"} else "camera")
        if self._is_raw and hasattr(self, "raw_base_mode_combo"):
            base_mode = settings.wb_sample_base_mode or self._raw_base_mode
            self._set_combo_data(self.raw_base_mode_combo, base_mode)
        self.wb_sample_size_spin.setValue(int(settings.wb_sample_size or 10))
        self._update_wb_readout(settings)

    def _update_source_widgets(self) -> None:
        self.setWindowTitle(f"Sporely curve inspector - {self._source_path.name}")
        self.source_path_label.setText(str(self._source_path))
        self.source_kind_label.setText("RAW" if self._is_raw else "Raster")
        self.raw_base_mode_label.setVisible(self._is_raw)
        self.raw_base_mode_combo.setVisible(self._is_raw)
        if self._is_raw:
            self._set_combo_data(self.raw_base_mode_combo, self._raw_base_mode)

    def _reset_white_balance_controls(self) -> None:
        self._set_combo_data(self.wb_mode_combo, "camera")
        if self._is_raw:
            self._set_combo_data(self.raw_base_mode_combo, self._raw_base_mode)
        self._settings = replace(
            self._settings,
            white_balance_mode="camera",
            wb_multipliers=None,
            wb_selection=None,
            wb_multiplier_space=None,
            wb_sample_point=None,
            wb_selection_space=None,
            wb_sample_base_mode=self._raw_base_mode if self._is_raw else None,
        )
        self._update_wb_readout(self._settings)
        if self.wb_pick_btn.isChecked():
            self.wb_pick_btn.setChecked(False)

    def _update_wb_readout(self, settings: RawRenderSettings | None = None) -> None:
        resolved = RawRenderSettings.from_dict(settings or self._settings)
        self.wb_readout_label.setText(self._white_balance_readout_text(resolved))

    def _white_balance_readout_text(self, settings: RawRenderSettings) -> str:
        mode = str(settings.white_balance_mode or "camera").strip().lower() or "camera"
        if mode == "custom" and settings.wb_multipliers is not None:
            return "Custom WB {r:.2f} / {g:.2f} / {b:.2f}".format(
                r=float(settings.wb_multipliers[0]),
                g=float(settings.wb_multipliers[1]),
                b=float(settings.wb_multipliers[2]),
            )
        if mode == "custom":
            return "Custom WB set"
        if mode == "auto":
            return "Auto WB"
        return "Camera WB"

    def _settings_from_controls(self) -> RawRenderSettings:
        base_settings = self._settings if isinstance(self._settings, RawRenderSettings) else RawRenderSettings.default()
        wb_mode = str(self.wb_mode_combo.currentData() or "camera").strip().lower() or "camera"
        raw_base_mode = self._raw_base_mode if self._is_raw else None
        if self._is_raw and wb_mode in {"camera", "auto"}:
            raw_base_mode = wb_mode
        settings = replace(
            base_settings,
            white_balance_mode=wb_mode if wb_mode in {"camera", "auto", "custom"} else "camera",
            auto_levels=bool(self.auto_levels_checkbox.isChecked()),
            black_percentile=self._cutoff_slider_value_to_percent(self.dark_cutoff_slider.value()) / 100.0,
            white_percentile=1.0 - (self._cutoff_slider_value_to_percent(self.bright_cutoff_slider.value()) / 100.0),
            auto_levels_strength=self._slider_value_float(self.auto_levels_strength_slider.value(), _CURVE_STRENGTH_SCALE),
            auto_levels_soft_tails=bool(self.soft_tails_checkbox.isChecked()),
            auto_levels_tail_size=float(base_settings.auto_levels_tail_size),
            auto_levels_shadow_lift=self._shadow_lift_slider_value_to_fraction(self.shadow_lift_slider.value()),
            tone_curve_enabled=bool(self.tone_curve_checkbox.isChecked()),
            tone_curve_strength=self._slider_value_float(self.curve_strength_slider.value(), _CURVE_TONE_SCALE),
            tone_curve_midpoint=self._slider_value_float(self.curve_midpoint_slider.value(), _CURVE_TONE_SCALE),
            wb_sample_size=int(self.wb_sample_size_spin.value()),
            wb_sample_base_mode=raw_base_mode,
        )
        if wb_mode in {"camera", "auto"}:
            settings = replace(
                settings,
                wb_multipliers=None,
                wb_selection=None,
                wb_multiplier_space=None,
                wb_sample_point=None,
                wb_selection_space=None,
            )
        elif settings.wb_multipliers is not None:
            settings = replace(settings, wb_multiplier_space="post_decode_rgb")
        return settings

    def _refresh_source(self) -> None:
        if not self._is_raw:
            return
        self._source = self._load_source()

    def _load_source_path(self, source_path: Path) -> None:
        resolved_path = source_path.expanduser().resolve()
        if not resolved_path.exists():
            raise FileNotFoundError(f"Input image not found: {resolved_path}")

        previous_path = self._source_path
        previous_is_raw = self._is_raw
        previous_raw_base_mode = self._raw_base_mode
        previous_source = self._source
        created_raw_tempdir = False

        try:
            self._source_path = resolved_path
            self._is_raw = is_raw_image_path(resolved_path)
            if self._is_raw and not previous_is_raw:
                self._raw_base_mode = "camera"
            if self._is_raw and self._raw_tempdir is None:
                self._raw_tempdir = tempfile.TemporaryDirectory(prefix="sporely_curve_")
                created_raw_tempdir = True
            self._source = self._load_source()
        except Exception:
            self._source_path = previous_path
            self._is_raw = previous_is_raw
            self._raw_base_mode = previous_raw_base_mode
            self._source = previous_source
            if created_raw_tempdir and self._raw_tempdir is not None:
                self._raw_tempdir.cleanup()
                self._raw_tempdir = None
            raise

        self._update_source_widgets()
        self._reset_white_balance_controls()
        self._refresh_outputs()

    def _open_source_dialog(self) -> None:
        file_path, _selected_filter = QFileDialog.getOpenFileName(
            self,
            "Open image",
            str(self._source_path.parent),
            _open_file_filter(),
        )
        if not file_path:
            return
        try:
            self._load_source_path(Path(file_path))
        except Exception as exc:
            self.statusBar().showMessage(str(exc), 5000)

    def _load_source(self) -> LoadedSource:
        if self._is_raw:
            scratch_dir = Path(self._raw_tempdir.name) if self._raw_tempdir is not None else None
            return _load_source_image(self._source_path, raw_base_mode=self._raw_base_mode, scratch_dir=scratch_dir)
        return _load_source_image(self._source_path)

    def _queue_refresh(self, *_args) -> None:
        self._refresh_timer.stop()
        self._refresh_timer.start(0)

    def _on_raw_base_mode_changed(self, *_args) -> None:
        if not self._is_raw:
            return
        self._raw_base_mode = str(self.raw_base_mode_combo.currentData() or "camera").strip().lower() or "camera"
        if str(self.wb_mode_combo.currentData() or "camera").strip().lower() in {"camera", "auto"}:
            self._set_combo_data(self.wb_mode_combo, self._raw_base_mode)
        self._source = self._load_source()
        self._queue_refresh()

    def _on_wb_mode_changed(self, *_args) -> None:
        mode = str(self.wb_mode_combo.currentData() or "camera").strip().lower() or "camera"
        if self._is_raw and mode in {"camera", "auto"} and hasattr(self, "raw_base_mode_combo"):
            self._set_combo_data(self.raw_base_mode_combo, mode)
            self._raw_base_mode = mode
            self._source = self._load_source()
        if mode != "custom":
            self._settings = replace(
                self._settings,
                white_balance_mode=mode,
                wb_multipliers=None,
                wb_selection=None,
                wb_multiplier_space=None,
                wb_sample_point=None,
                wb_selection_space=None,
                wb_sample_size=int(self.wb_sample_size_spin.value()),
                wb_sample_base_mode=self._raw_base_mode if self._is_raw else None,
            )
        else:
            self._settings = replace(
                self._settings,
                white_balance_mode="custom",
                wb_sample_size=int(self.wb_sample_size_spin.value()),
                wb_sample_base_mode=self._raw_base_mode if self._is_raw else None,
            )
        self._update_wb_readout(self._settings)
        self._queue_refresh()

    def _toggle_wb_pick(self, checked: bool) -> None:
        self._wb_pick_armed = bool(checked)
        self.preview_label.setCursor(Qt.CrossCursor if checked else Qt.ArrowCursor)
        self.wb_pick_btn.setText("Cancel background WB" if checked else "Pick background WB")
        if checked:
            self.statusBar().showMessage("Click neutral background to sample WB")
        else:
            self.statusBar().clearMessage()

    def _sample_rect_from_point(self, point: QPointF, *, sample_size: int | None = None) -> tuple[float, float, float, float] | None:
        if self._source.rgb.size == 0:
            return None
        width = float(self._source.rgb.shape[1])
        height = float(self._source.rgb.shape[0])
        size = int(sample_size or self.wb_sample_size_spin.value() or 10)
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

    def _apply_wb_pick_from_point(self, point: QPointF) -> bool:
        if not self._wb_pick_armed:
            return False
        sample_rect = self._sample_rect_from_point(point)
        if sample_rect is None:
            self.statusBar().showMessage("Could not sample WB from that point", 4000)
            return False
        try:
            multipliers = estimate_white_balance_from_background(self._source.rgb, rect=sample_rect)
        except Exception as exc:
            self.statusBar().showMessage(f"Could not sample background WB: {exc}", 5000)
            return False

        base_mode = self._raw_base_mode if self._is_raw else None
        self._settings = replace(
            self._settings,
            white_balance_mode="custom",
            wb_multipliers=(float(multipliers[0]), float(multipliers[1]), float(multipliers[2])),
            wb_selection=(float(sample_rect[0]), float(sample_rect[1]), float(sample_rect[2]), float(sample_rect[3])),
            wb_multiplier_space="post_decode_rgb",
            wb_sample_point=(float(point.x()), float(point.y())),
            wb_sample_size=int(self.wb_sample_size_spin.value()),
            wb_sample_base_mode=base_mode,
            wb_selection_space="preview_pixels",
        )
        self._set_combo_data(self.wb_mode_combo, "custom")
        self._update_wb_readout(self._settings)
        if self.wb_pick_btn.isChecked():
            self.wb_pick_btn.setChecked(False)
        self._refresh_outputs()
        return True

    def _on_preview_clicked(self, point: QPointF) -> None:
        self._apply_wb_pick_from_point(point)

    def _refresh_outputs(self) -> None:
        self._settings = self._settings_from_controls()
        source_rgb = self._source.rgb
        processed_rgb, debug = apply_post_decode_processing(source_rgb, self._settings, return_debug=True)

        preview_pixmap = _rgb_to_pixmap(processed_rgb)
        self.preview_label.set_image_sources(preview_pixmap, preserve_view=True)
        self.preview_label.set_pan_without_shift(True)

        self.figure.clear()
        _draw_processing_figure(self.figure, source_rgb, self._settings, debug=debug)
        self.canvas.draw()

        self.input_min_label.setText(f"{debug.input_min:.6f}")
        self.input_max_label.setText(f"{debug.input_max:.6f}")
        self.black_level_label.setText("none" if debug.black_level is None else f"{debug.black_level:.6f}")
        self.white_level_label.setText("none" if debug.white_level is None else f"{debug.white_level:.6f}")
        self.settings_label.setText(_settings_summary(self._settings))
        self._update_wb_readout(self._settings)
        self.statusBar().showMessage(_settings_summary(self._settings))

        if self._preview_out is not None:
            _save_preview(self._preview_out, processed_rgb)
        if self._plot_out is not None:
            _save_plot(self._plot_out, source_rgb, self._settings, debug=debug)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_path", type=Path, help="Source image to analyze")
    parser.add_argument("--plot-out", type=Path, help="Path for the curve plot PNG snapshot")
    parser.add_argument("--preview-out", type=Path, help="Path for the processed preview PNG snapshot")
    parser.add_argument("--interactive", action="store_true", help="Open an interactive window with live controls")
    parser.add_argument(
        "--auto-levels",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable or disable the auto-level stage",
    )
    parser.add_argument("--auto-levels-strength", type=float, default=1.0, help="Blend between original and auto-leveled luminance")
    parser.add_argument("--soft-tails", action="store_true", help="Compress dark and bright tails instead of hard clipping")
    parser.add_argument("--tail-size", type=float, default=0.03, help="Output tail range reserved when soft tails are enabled")
    parser.add_argument("--shadow-lift", type=float, default=0.0, help="Output black lift applied after auto-levels")
    parser.add_argument(
        "--dark-cutoff",
        "--black-quantile",
        dest="dark_cutoff",
        type=float,
        default=0.001,
        help="Dark cutoff quantile used by auto-levels",
    )
    parser.add_argument(
        "--bright-cutoff",
        "--white-quantile",
        dest="bright_cutoff",
        type=float,
        default=0.001,
        help="Bright cutoff quantile used by auto-levels",
    )
    parser.add_argument("--tone-curve", action="store_true", help="Apply the tone curve stage")
    parser.add_argument("--curve-strength", type=float, default=0.5, help="Tone curve strength")
    parser.add_argument("--curve-midpoint", type=float, default=0.5, help="Tone curve midpoint")
    args = parser.parse_args(argv)
    if not args.interactive and (args.plot_out is None or args.preview_out is None):
        parser.error("--plot-out and --preview-out are required unless --interactive is set")
    return args


def _run_snapshot_mode(args: argparse.Namespace) -> int:
    source_path = args.input_path.expanduser().resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"Input image not found: {source_path}")

    settings = _build_settings(args)
    source = _load_source_image(source_path)
    processed_rgb, debug = apply_post_decode_processing(source.rgb, settings, return_debug=True)

    preview_out = args.preview_out.expanduser().resolve()
    plot_out = args.plot_out.expanduser().resolve()
    _save_preview(preview_out, processed_rgb)
    _save_plot(plot_out, source.rgb, settings)

    print(f"input min luminance: {debug.input_min:.6f}")
    print(f"input max luminance: {debug.input_max:.6f}")
    print(f"detected black level: {debug.black_level if debug.black_level is not None else 'none'}")
    print(f"detected white level: {debug.white_level if debug.white_level is not None else 'none'}")
    print("settings JSON:")
    print(json.dumps(debug.settings, indent=2, sort_keys=True))
    print(f"plot out: {plot_out}")
    print(f"preview out: {preview_out}")
    return 0


def _run_interactive_mode(args: argparse.Namespace) -> int:
    source_path = args.input_path.expanduser().resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"Input image not found: {source_path}")

    app = QApplication.instance() or QApplication([])
    window = ProcessingCurveWindow(
        source_path,
        plot_out=args.plot_out.expanduser().resolve() if args.plot_out is not None else None,
        preview_out=args.preview_out.expanduser().resolve() if args.preview_out is not None else None,
    )
    window.show()
    return int(app.exec())


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.interactive:
        return _run_interactive_mode(args)
    return _run_snapshot_mode(args)


if __name__ == "__main__":
    raise SystemExit(main())
