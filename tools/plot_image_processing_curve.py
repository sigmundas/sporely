#!/usr/bin/env python3
"""Inspect Sporely's post-decode curve, either as a snapshot or interactively."""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image
from PySide6.QtCore import QEvent, QPointF, Qt, QSignalBlocker, QTimer
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
from utils.raw_tone_curve import (  # noqa: E402
    apply_luminance_contrast_curve,
    apply_luminance_shadow_highlights,
    normalized_sigmoid_curve,
)
from ui.raw_processing_controls import RawProcessingControls  # noqa: E402
from ui.zoomable_image_widget import ZoomableImageLabel  # noqa: E402

_CURVE_CUTOFF_SCALE = 20
_EXPOSURE_SCALE = 20
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
    if args.light_ev is None and args.dark_ev is None and args.exposure_ev is not None:
        light_ev = max(0.0, float(args.exposure_ev))
        dark_ev = min(0.0, float(args.exposure_ev))
    else:
        light_ev = 0.0 if args.light_ev is None else float(args.light_ev)
        dark_ev = 0.0 if args.dark_ev is None else float(args.dark_ev)
    return RawRenderSettings(
        exposure_ev=light_ev + dark_ev,
        light_ev=light_ev,
        dark_ev=dark_ev,
        auto_levels=bool(args.auto_levels),
        black_percentile=float(args.dark_cutoff),
        white_percentile=1.0 - float(args.bright_cutoff),
        auto_levels_strength=float(args.auto_levels_strength),
        auto_levels_soft_tails=bool(args.soft_tails),
        auto_levels_tail_size=float(args.tail_size),
        shadow_lift=max(0.0, min(0.10, float(args.shadow_lift))),
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
    ax.plot(
        input_values,
        curve.auto_levels_output,
        label="after auto-levels",
        linewidth=2.0,
        color="#2980b9",
    )
    ax.plot(
        input_values,
        curve.manual_levels_output,
        label="after light / dark",
        linewidth=2.4,
        color="#8e44ad",
    )
    ax.plot(
        input_values,
        curve.shadow_toe_output,
        label="after dark boost",
        linewidth=2.4,
        color="#e67e22",
    )
    ax.plot(
        input_values,
        curve.final_output,
        label="final after tone curve",
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
    dark_cutoff = max(0.0, min(0.02, float(settings.black_percentile) * 100.0))
    bright_cutoff = max(0.0, min(0.02, (1.0 - float(settings.white_percentile)) * 100.0))
    auto_levels_strength = max(0.0, min(100.0, float(settings.auto_levels_strength) * 100.0))
    dark_ev = max(0.0, min(0.5, -float(settings.dark_ev)))
    parts = [
        f"WB {wb_desc}",
        f"light {float(settings.light_ev):.3f}",
        f"dark {dark_ev:.3f}",
        "auto levels on" if settings.auto_levels else "auto levels off",
        f"strength {auto_levels_strength:.0f}%",
        "soft tails on" if settings.auto_levels_soft_tails else "soft tails off",
        f"dark boost {float(settings.shadow_lift) * 100.0:.1f}%",
        f"dark cutoff {dark_cutoff:.3f}%",
        f"bright cutoff {bright_cutoff:.3f}%",
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
        source_path: Path | None = None,
        *,
        plot_out: Path | None = None,
        preview_out: Path | None = None,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._source_path = source_path.expanduser().resolve() if source_path is not None else None
        self._plot_out = plot_out
        self._preview_out = preview_out
        self._is_raw = bool(self._source_path is not None and is_raw_image_path(self._source_path))
        self._raw_base_mode = "camera"
        self._raw_tempdir = tempfile.TemporaryDirectory(prefix="sporely_curve_") if self._is_raw else None
        self._has_source = self._source_path is not None
        self._source = self._load_source() if self._has_source else LoadedSource("blank", np.zeros((0, 0, 3), dtype=np.float64))
        self._settings = RawRenderSettings.default()
        self._wb_pick_armed = False
        self._refresh_timer = QTimer(self)
        self._refresh_timer.setSingleShot(True)
        self._refresh_timer.timeout.connect(self._refresh_outputs)
        self._build_ui()
        if self._has_source:
            self._refresh_outputs()
        else:
            self._set_blank_outputs()

    def closeEvent(self, event) -> None:  # type: ignore[override]
        try:
            self._refresh_timer.stop()
            if self._raw_tempdir is not None:
                self._raw_tempdir.cleanup()
        finally:
            super().closeEvent(event)

    def _load_source(self) -> LoadedSource:
        if self._source_path is None:
            return LoadedSource("blank", np.zeros((0, 0, 3), dtype=np.float64))
        if self._is_raw:
            scratch_dir = Path(self._raw_tempdir.name) if self._raw_tempdir is not None else None
            return _load_source_image(self._source_path, raw_base_mode=self._raw_base_mode, scratch_dir=scratch_dir)
        return _load_source_image(self._source_path)

    def _open_source_dialog(self) -> None:
        file_path, _selected_filter = QFileDialog.getOpenFileName(
            self,
            "Open image",
            str(self._source_path.parent if self._source_path is not None else Path.cwd()),
            _open_file_filter(),
        )
        if not file_path:
            return
        try:
            self._load_source_path(Path(file_path))
        except Exception as exc:
            self.statusBar().showMessage(str(exc), 5000)

    def _toggle_wb_pick(self, checked: bool) -> None:
        self._wb_pick_armed = bool(checked)
        self.preview_label.setCursor(Qt.CrossCursor if checked else Qt.ArrowCursor)
        if checked:
            self.statusBar().showMessage("Click neutral background to sample WB")
        else:
            self.statusBar().clearMessage()

    def _build_ui(self) -> None:
        if self._source_path is not None:
            self.setWindowTitle(f"Sporely curve inspector - {self._source_path.name}")
        else:
            self.setWindowTitle("Sporely curve inspector")
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
        self.source_kind_label = QLabel("RAW" if self._is_raw else ("Raster" if self._has_source else "Blank"))
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

        self.wb_sample_size_spin = QSpinBox()
        self.wb_sample_size_spin.setRange(1, 128)
        self.wb_sample_size_spin.setValue(10)
        self.wb_sample_size_spin.valueChanged.connect(self._queue_refresh)
        source_form.addRow("WB sample size", self.wb_sample_size_spin)

        self.open_file_btn = QPushButton("Open file...")
        self.open_file_btn.clicked.connect(self._open_source_dialog)
        source_form.addRow(self.open_file_btn)
        self.wb_readout_label = QLabel("Camera WB")
        self.wb_readout_label.setWordWrap(True)
        source_form.addRow("Readout", self.wb_readout_label)
        layout.addWidget(source_group)
        self._update_source_widgets()

        raw_group = QGroupBox("RAW processing")
        raw_layout = QVBoxLayout(raw_group)
        raw_layout.setContentsMargins(8, 8, 8, 8)
        raw_layout.setSpacing(8)
        self.raw_controls = RawProcessingControls(raw_group, show_shadow_lift=True)
        self.raw_controls.settingsChanged.connect(self._queue_refresh)
        self.raw_controls.pickWhiteBalanceToggled.connect(self._toggle_wb_pick)
        raw_layout.addWidget(self.raw_controls)
        layout.addWidget(raw_group)

        self.wb_pick_btn = self.raw_controls.pick_button

        self.light_slider = self.raw_controls.light_slider
        self.light_value_label = self.raw_controls.light_value_label
        self.dark_slider = self.raw_controls.dark_slider
        self.dark_value_label = self.raw_controls.dark_value_label
        self.exposure_slider = self.light_slider
        self.exposure_value_label = self.light_value_label
        self.auto_levels_checkbox = self.raw_controls.auto_levels_checkbox
        self.tone_curve_checkbox = self.raw_controls.tone_curve_checkbox
        self.shadow_lift_label = self.raw_controls.shadow_lift_label
        self.shadow_lift_slider = self.raw_controls.shadow_lift_slider
        self.shadow_lift_value_label = self.raw_controls.shadow_lift_value_label
        self.shadows_label = self.raw_controls.shadow_lift_label
        self.shadows_slider = self.raw_controls.shadow_lift_slider
        self.shadows_value_label = self.raw_controls.shadow_lift_value_label
        self.curve_strength_row = self.raw_controls.curve_strength_row
        self.curve_strength_slider = self.raw_controls.curve_strength_slider
        self.curve_strength_value_label = self.raw_controls.curve_strength_value_label
        self.contrast_label = QLabel("Contrast")
        self.contrast_slider = self.curve_strength_slider
        self.contrast_value_label = self.curve_strength_value_label

        self.curve_midpoint_row = self.raw_controls.curve_midpoint_row
        self.curve_midpoint_slider = self.raw_controls.curve_midpoint_slider
        self.curve_midpoint_value_label = self.raw_controls.curve_midpoint_value_label
        self.midpoint_label = QLabel("Midpoint")
        self.midpoint_slider = self.curve_midpoint_slider
        self.midpoint_value_label = self.curve_midpoint_value_label

        advanced_group = QGroupBox("Advanced auto-levels")
        advanced_form = QFormLayout(advanced_group)
        advanced_form.setLabelAlignment(Qt.AlignRight)

        auto_levels_strength_row, self.auto_levels_strength_slider, self.auto_levels_strength_value_label = self._create_percent_slider_row(
            value=1.0,
            label_formatter=lambda value: f"{value:.0f}%",
        )
        advanced_form.addRow("Auto-level strength", auto_levels_strength_row)

        dark_cutoff_row, self.dark_cutoff_slider, self.dark_cutoff_value_label = self._create_cutoff_slider_row(
            cutoff_percent=0.01,
        )
        self.black_quantile_slider = self.dark_cutoff_slider
        self.black_quantile_value_label = self.dark_cutoff_value_label
        advanced_form.addRow("Dark cutoff", dark_cutoff_row)

        bright_cutoff_row, self.bright_cutoff_slider, self.bright_cutoff_value_label = self._create_cutoff_slider_row(
            cutoff_percent=0.01,
        )
        self.white_quantile_slider = self.bright_cutoff_slider
        self.white_quantile_value_label = self.bright_cutoff_value_label
        advanced_form.addRow("Bright cutoff", bright_cutoff_row)
        layout.addWidget(advanced_group)
        self.raw_controls.set_tone_controls_enabled(bool(self.tone_curve_checkbox.isChecked()))

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
        self._set_processing_controls_enabled(self._has_source)
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

    def _create_ev_slider_row(self, *, value: float) -> tuple[QWidget, QSlider, QLabel]:
        row = QWidget(self)
        row_layout = QHBoxLayout(row)
        row_layout.setContentsMargins(0, 0, 0, 0)
        row_layout.setSpacing(8)

        slider = QSlider(Qt.Horizontal, row)
        slider.setRange(-20, 30)
        slider.setSingleStep(1)
        slider.setPageStep(5)
        slider.setValue(self._ev_to_slider_value(value))
        row_layout.addWidget(slider, 1)

        value_label = QLabel(row)
        value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        value_label.setMinimumWidth(72)
        value_label.setText(self._ev_slider_value_text(slider.value()))
        row_layout.addWidget(value_label, 0)

        def _sync_slider_value(slider_value: int) -> None:
            value_label.setText(self._ev_slider_value_text(slider_value))
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
        slider.setPageStep(1)
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
        slider.setRange(0, 100)
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
    def _ev_to_slider_value(value: float) -> int:
        return int(round(max(-2.0, min(2.0, float(value))) * 20.0))

    @staticmethod
    def _slider_value_float(value: int, scale: int) -> float:
        return max(0.0, min(1.0, float(value) / float(scale)))

    @staticmethod
    def _slider_value_ev(value: int) -> float:
        return max(-2.0, min(2.0, float(value) / 20.0))

    @classmethod
    def _slider_value_text(cls, value: int, scale: int, decimals: int) -> str:
        return f"{cls._slider_value_float(value, scale):.{decimals}f}"

    @staticmethod
    def _cutoff_percent_to_slider_value(cutoff_percent: float) -> int:
        return int(round(max(0.0, min(0.02, float(cutoff_percent))) * 1000.0))

    @staticmethod
    def _cutoff_slider_value_to_percent(value: int) -> float:
        return max(0.0, min(0.02, float(value) / 1000.0))

    @classmethod
    def _cutoff_slider_value_text(cls, value: int) -> str:
        return f"{cls._cutoff_slider_value_to_percent(value):.3f}%"

    @staticmethod
    def _shadow_lift_fraction_to_slider_value(value: float) -> int:
        return int(round(max(0.0, min(0.10, float(value))) * 1000.0))

    @staticmethod
    def _shadow_lift_slider_value_to_fraction(value: int) -> float:
        return max(0.0, min(0.10, float(value) / 1000.0))

    @classmethod
    def _shadow_lift_slider_value_text(cls, value: int) -> str:
        return f"{cls._shadow_lift_slider_value_to_fraction(value) * 100.0:.1f}%"

    @classmethod
    def _ev_slider_value_text(cls, value: int) -> str:
        return f"{cls._slider_value_ev(value):.3f}"

    def _set_combo_data(self, combo: QComboBox, data: str) -> None:
        index = combo.findData(data)
        if index >= 0:
            with QSignalBlocker(combo):
                combo.setCurrentIndex(index)

    def _sync_wb_controls_from_settings(self, settings: RawRenderSettings) -> None:
        resolved = RawRenderSettings.from_dict(settings)
        self.raw_controls.set_settings(resolved)
        if self._is_raw and hasattr(self, "raw_base_mode_combo"):
            base_mode = resolved.wb_sample_base_mode or self._raw_base_mode
            self._set_combo_data(self.raw_base_mode_combo, base_mode)
        self.wb_sample_size_spin.setValue(int(resolved.wb_sample_size or 10))
        self._update_wb_readout(resolved)

    def _update_source_widgets(self) -> None:
        if self._source_path is None:
            self.setWindowTitle("Sporely curve inspector")
            self.source_path_label.setText("No file loaded")
            self.source_kind_label.setText("Blank")
            self.raw_base_mode_label.setVisible(False)
            self.raw_base_mode_combo.setVisible(False)
            self.wb_sample_size_spin.setEnabled(False)
            self.wb_readout_label.setText("No file loaded")
            return

        self.setWindowTitle(f"Sporely curve inspector - {self._source_path.name}")
        self.source_path_label.setText(str(self._source_path))
        self.source_kind_label.setText("RAW" if self._is_raw else "Raster")
        self.raw_base_mode_label.setVisible(self._is_raw)
        self.raw_base_mode_combo.setVisible(self._is_raw)
        self.wb_sample_size_spin.setEnabled(True)
        if self._is_raw:
            self._set_combo_data(self.raw_base_mode_combo, self._raw_base_mode)

    def _reset_white_balance_controls(self) -> None:
        if self._is_raw:
            self._set_combo_data(self.raw_base_mode_combo, self._raw_base_mode)
        current = RawRenderSettings.from_dict(self.raw_controls.settings())
        reset = replace(
            current,
            white_balance_mode="camera",
            wb_multipliers=None,
            wb_selection=None,
            wb_multiplier_space=None,
            wb_sample_point=None,
            wb_selection_space=None,
            wb_sample_base_mode=self._raw_base_mode if self._is_raw else None,
            wb_sample_size=int(self.wb_sample_size_spin.value()),
        )
        self.raw_controls.set_settings(reset)
        self._settings = reset
        self._update_wb_readout(reset)
        if self.wb_pick_btn.isChecked():
            self.raw_controls.set_pick_checked(False)
        self._wb_pick_armed = False

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
        base_settings = RawRenderSettings.from_dict(self.raw_controls.settings())
        wb_mode = str(base_settings.white_balance_mode or "camera").strip().lower() or "camera"
        raw_base_mode = self._raw_base_mode if self._is_raw else None
        if self._is_raw and wb_mode in {"camera", "auto"}:
            raw_base_mode = wb_mode
        settings = replace(
            base_settings,
            white_balance_mode=wb_mode if wb_mode in {"camera", "auto", "custom"} else "camera",
            black_percentile=self._cutoff_slider_value_to_percent(self.dark_cutoff_slider.value()) / 100.0,
            white_percentile=1.0 - (self._cutoff_slider_value_to_percent(self.bright_cutoff_slider.value()) / 100.0),
            auto_levels_strength=self._slider_value_float(self.auto_levels_strength_slider.value(), _CURVE_STRENGTH_SCALE),
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

    def _set_processing_controls_enabled(self, enabled: bool) -> None:
        for widget in (
            self.raw_controls,
            self.raw_base_mode_combo,
            self.wb_sample_size_spin,
            self.light_slider,
            self.dark_slider,
            self.auto_levels_strength_slider,
            self.dark_cutoff_slider,
            self.bright_cutoff_slider,
            self.curve_strength_slider,
            self.curve_midpoint_slider,
        ):
            widget.setEnabled(bool(enabled))

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
        self._set_processing_controls_enabled(True)
        self._refresh_outputs()

    def _open_source_dialog(self) -> None:
        file_path, _selected_filter = QFileDialog.getOpenFileName(
            self,
            "Open image",
            str(self._source_path.parent if self._source_path is not None else Path.cwd()),
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
        self._refresh_timer.start()

    def _on_raw_base_mode_changed(self, *_args) -> None:
        if not self._is_raw:
            return
        self._raw_base_mode = str(self.raw_base_mode_combo.currentData() or "camera").strip().lower() or "camera"
        current = RawRenderSettings.from_dict(self.raw_controls.settings())
        if str(current.white_balance_mode or "camera").strip().lower() in {"camera", "auto"}:
            self.raw_controls.set_settings(
                replace(current, white_balance_mode=self._raw_base_mode, wb_sample_base_mode=self._raw_base_mode)
            )
        self._source = self._load_source()
        self._queue_refresh()

    def _toggle_wb_pick(self, checked: bool) -> None:
        self._wb_pick_armed = bool(checked)
        self.preview_label.setCursor(Qt.CrossCursor if checked else Qt.ArrowCursor)
        if checked:
            self.statusBar().showMessage("Click a neutral background to sample WB")
        else:
            self.statusBar().clearMessage()

    def _toggle_wb_pick(self, checked: bool) -> None:
        self._wb_pick_armed = bool(checked)
        self.preview_label.setCursor(Qt.CrossCursor if checked else Qt.ArrowCursor)
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
        updated = replace(
            RawRenderSettings.from_dict(self.raw_controls.settings()),
            white_balance_mode="custom",
            wb_multipliers=(float(multipliers[0]), float(multipliers[1]), float(multipliers[2])),
            wb_selection=(float(sample_rect[0]), float(sample_rect[1]), float(sample_rect[2]), float(sample_rect[3])),
            wb_multiplier_space="post_decode_rgb",
            wb_sample_point=(float(point.x()), float(point.y())),
            wb_sample_size=int(self.wb_sample_size_spin.value()),
            wb_sample_base_mode=base_mode,
            wb_selection_space="preview_pixels",
        )
        self.raw_controls.set_settings(updated)
        self._settings = updated
        self._update_wb_readout(self._settings)
        if self.wb_pick_btn.isChecked():
            self.raw_controls.set_pick_checked(False)
        self._wb_pick_armed = False
        self._refresh_outputs()
        return True

    def _on_preview_clicked(self, point: QPointF) -> None:
        self._apply_wb_pick_from_point(point)

    def _refresh_outputs(self) -> None:
        if self._source_path is None:
            self._set_blank_outputs()
            return
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

    def _set_blank_outputs(self) -> None:
        self.preview_label.set_image_sources(QPixmap())
        self.figure.clear()
        self.canvas.draw()
        self.input_min_label.setText("—")
        self.input_max_label.setText("—")
        self.black_level_label.setText("—")
        self.white_level_label.setText("—")
        self.settings_label.setText("Open a file to inspect RAW processing")
        self.wb_readout_label.setText("No file loaded")
        self.statusBar().showMessage("Open a file to inspect RAW processing")


_TONE_LUMA_WEIGHTS = np.array([0.2126, 0.7152, 0.0722], dtype=np.float64)
_TONE_EPSILON = np.finfo(np.float64).eps
_TONE_POINT_SCALE = 1000
_TONE_SIGNED_SCALE = 100
_TONE_CUTOFF_SCALE = 20
_TONE_PREVIEW_BUCKET_NORMAL = 1600
_TONE_PREVIEW_BUCKET_ZOOMED_OUT = 1100
_TONE_PREVIEW_BUCKET_ZOOMED_IN = 2400
_TONE_ZOOM_OUT_THRESHOLD = 0.85
_TONE_ZOOM_IN_THRESHOLD = 1.75


@dataclass(slots=True)
class TestToneSettings:
    dark_point: float = 0.0
    light_point: float = 1.0
    auto_levels: bool = True
    dark_cutoff: float = 0.01
    bright_cutoff: float = 0.01
    contrast: float = 0.0
    shadows: float = 0.0
    highlights: float = 0.0
    tone_curve_enabled: bool = False
    tone_curve_strength: float = 0.5
    tone_curve_midpoint: float = 0.5

    @classmethod
    def default(cls) -> "TestToneSettings":
        return cls()

    @classmethod
    def from_dict(cls, mapping: Any | None) -> "TestToneSettings":
        data = dict(mapping or {}) if isinstance(mapping, dict) else {}
        if not data and mapping is not None:
            to_dict = getattr(mapping, "to_dict", None)
            if callable(to_dict):
                try:
                    data = dict(to_dict())
                except Exception:
                    data = {}
        contrast_value = data.get("contrast", data.get("curve_strength", 0.0))
        shadows_value = data.get("shadows", data.get("shadow_lift", 0.0))
        highlights_value = data.get("highlights", 0.0)
        return cls(
            dark_point=float(data.get("dark_point", 0.0)),
            light_point=float(data.get("light_point", 1.0)),
            auto_levels=bool(data.get("auto_levels", True)),
            dark_cutoff=float(data.get("dark_cutoff", 0.01)),
            bright_cutoff=float(data.get("bright_cutoff", 0.01)),
            contrast=float(contrast_value),
            shadows=float(shadows_value),
            highlights=float(highlights_value),
            tone_curve_enabled=bool(data.get("tone_curve_enabled", False)),
            tone_curve_strength=float(data.get("tone_curve_strength", 0.5)),
            tone_curve_midpoint=float(data.get("tone_curve_midpoint", 0.5)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "dark_point": float(self.dark_point),
            "light_point": float(self.light_point),
            "auto_levels": bool(self.auto_levels),
            "dark_cutoff": float(self.dark_cutoff),
            "bright_cutoff": float(self.bright_cutoff),
            "contrast": float(self.contrast),
            "shadows": float(self.shadows),
            "highlights": float(self.highlights),
            "tone_curve_enabled": bool(self.tone_curve_enabled),
            "tone_curve_strength": float(self.tone_curve_strength),
            "tone_curve_midpoint": float(self.tone_curve_midpoint),
        }


@dataclass(slots=True)
class TestToneDebugInfo:
    input_min: float
    input_max: float
    auto_black_level: float | None
    auto_white_level: float | None
    dark_point: float
    light_point: float
    preview_bucket: int | None
    manual_level_override: bool
    settings: dict[str, Any]


@dataclass(slots=True)
class TestToneTransferCurve:
    input_values: np.ndarray
    levels_output: np.ndarray
    contrast_output: np.ndarray
    shadow_highlight_output: np.ndarray
    final_output: np.ndarray
    debug: TestToneDebugInfo


def _tone_coerce_settings(settings: Any | None) -> TestToneSettings:
    if isinstance(settings, TestToneSettings):
        return settings
    if settings is None:
        return TestToneSettings.default()
    if isinstance(settings, dict):
        return TestToneSettings.from_dict(settings)
    to_dict = getattr(settings, "to_dict", None)
    if callable(to_dict):
        try:
            return TestToneSettings.from_dict(to_dict())
        except Exception:
            return TestToneSettings.default()
    return TestToneSettings.default()


def _tone_clamp01(values: Any) -> np.ndarray:
    return np.clip(np.asarray(values, dtype=np.float64), 0.0, 1.0)


def _tone_smoothstep(edge0: Any, edge1: Any | None = None, x: Any | None = None) -> np.ndarray:
    if x is None and edge1 is None:
        values = _tone_clamp01(edge0)
    else:
        if edge1 is None or x is None:
            raise TypeError("smoothstep expects either 1 or 3 arguments")
        start = float(edge0)
        stop = float(edge1)
        span = max(stop - start, _TONE_EPSILON)
        values = _tone_clamp01((np.asarray(x, dtype=np.float64) - start) / span)
    return values * values * (3.0 - 2.0 * values)


def _tone_compute_luminance(rgb: Any) -> np.ndarray:
    arr = to_float_rgb(rgb, clip=False)
    return np.tensordot(arr[..., :3], _TONE_LUMA_WEIGHTS, axes=([-1], [0]))


def _tone_apply_luminance_transfer(rgb: Any, source_luminance: Any, target_luminance: Any) -> np.ndarray:
    arr = to_float_rgb(rgb, clip=False)
    source = np.asarray(source_luminance, dtype=np.float64)
    target = np.asarray(target_luminance, dtype=np.float64)
    if source.shape != target.shape:
        raise ValueError("Source and target luminance must have the same shape")

    scale = np.ones_like(source, dtype=np.float64)
    usable = source > _TONE_EPSILON
    scale[usable] = target[usable] / source[usable]
    balanced = np.maximum(arr[..., :3] * scale[..., None], 0.0)
    if np.any(~usable):
        balanced[~usable] = np.clip(target[~usable][..., None], 0.0, 1.0)
    if arr.shape[-1] > 3:
        alpha = np.clip(arr[..., 3:], 0.0, 1.0)
        return np.concatenate([balanced, alpha], axis=-1)
    return balanced


def _tone_apply_channel_gains(rgb: Any, multipliers: tuple[float, float, float] | None) -> np.ndarray:
    arr = to_float_rgb(rgb, clip=False)
    if multipliers is None:
        return arr
    gains = np.asarray([float(multipliers[0]), float(multipliers[1]), float(multipliers[2])], dtype=np.float64)
    balanced = np.maximum(arr[..., :3] * gains, 0.0)
    if arr.shape[-1] > 3:
        alpha = np.clip(arr[..., 3:], 0.0, 1.0)
        return np.concatenate([balanced, alpha], axis=-1)
    return balanced


def _tone_resolve_level_points(dark_point: float, light_point: float) -> tuple[float, float]:
    dark = float(np.clip(float(dark_point), 0.0, 1.0))
    light = float(np.clip(float(light_point), 0.0, 1.0))
    if not np.isfinite(dark):
        dark = 0.0
    if not np.isfinite(light):
        light = 1.0
    if light <= dark + 1e-4:
        if dark >= 1.0 - 1e-4:
            dark = max(0.0, 1.0 - 1e-3)
            light = 1.0
        else:
            light = min(1.0, dark + 1e-3)
        if light <= dark + 1e-4:
            dark = 0.0
            light = 1.0
    return dark, light


def _tone_compute_auto_level_bounds(
    rgb: Any,
    dark_cutoff: float,
    bright_cutoff: float,
) -> tuple[float | None, float | None]:
    luminance = _tone_compute_luminance(rgb)
    if luminance.size == 0:
        return None, None

    black_quantile = float(np.clip(float(dark_cutoff), 0.0, 0.49))
    white_quantile = float(np.clip(1.0 - float(bright_cutoff), 0.51, 1.0))
    if white_quantile <= black_quantile:
        white_quantile = min(1.0, black_quantile + 1e-3)

    try:
        black_level = float(np.quantile(luminance, black_quantile))
        white_level = float(np.quantile(luminance, white_quantile))
    except Exception:
        return None, None

    if not np.isfinite(black_level) or not np.isfinite(white_level):
        return None, None
    if white_level <= black_level:
        black_level = float(np.min(luminance))
        white_level = float(np.max(luminance))
    if white_level <= black_level:
        white_level = min(1.0, black_level + 1e-3)
    return black_level, white_level


def _tone_normalize_levels(values: Any, dark_point: float, light_point: float) -> np.ndarray:
    dark, light = _tone_resolve_level_points(dark_point, light_point)
    span = max(light - dark, _TONE_EPSILON)
    return _tone_clamp01((np.asarray(values, dtype=np.float64) - dark) / span)


def _tone_apply_contrast(values: Any, contrast: float) -> np.ndarray:
    return _tone_clamp01(apply_luminance_contrast_curve(values, contrast))


def _tone_apply_shadow_highlights(values: Any, shadows: float, highlights: float) -> np.ndarray:
    return _tone_clamp01(apply_luminance_shadow_highlights(values, shadows, highlights))


def _tone_apply_tone_curve(values: Any, strength: float, midpoint: float) -> np.ndarray:
    return _tone_clamp01(normalized_sigmoid_curve(values, strength, midpoint))


def _tone_build_debug(
    rgb: Any,
    settings: TestToneSettings,
    *,
    manual_level_override: bool = False,
    preview_bucket: int | None = None,
    extra_settings: dict[str, Any] | None = None,
) -> TestToneDebugInfo:
    source = to_float_rgb(rgb, clip=False)
    luminance = _tone_compute_luminance(source)
    input_min = float(np.min(luminance)) if luminance.size else 0.0
    input_max = float(np.max(luminance)) if luminance.size else 0.0
    auto_black_level, auto_white_level = _tone_compute_auto_level_bounds(
        source,
        settings.dark_cutoff,
        settings.bright_cutoff,
    )
    dark_point, light_point = _tone_resolve_level_points(settings.dark_point, settings.light_point)
    payload = settings.to_dict()
    payload.update(
        {
            "input_min": input_min,
            "input_max": input_max,
            "auto_black_level": auto_black_level,
            "auto_white_level": auto_white_level,
            "resolved_dark_point": dark_point,
            "resolved_light_point": light_point,
            "manual_level_override": bool(manual_level_override),
        }
    )
    if preview_bucket is not None:
        payload["preview_bucket"] = int(preview_bucket)
    if extra_settings:
        payload.update(extra_settings)
    return TestToneDebugInfo(
        input_min=input_min,
        input_max=input_max,
        auto_black_level=auto_black_level,
        auto_white_level=auto_white_level,
        dark_point=dark_point,
        light_point=light_point,
        preview_bucket=preview_bucket,
        manual_level_override=bool(manual_level_override),
        settings=payload,
    )


def apply_post_decode_processing(
    rgb: Any,
    settings: Any,
    *,
    return_debug: bool = False,
    manual_level_override: bool = False,
    preview_bucket: int | None = None,
) -> np.ndarray | tuple[np.ndarray, TestToneDebugInfo]:
    """Apply the local tone prototype to an RGB array."""
    resolved = _tone_coerce_settings(settings)
    input_rgb = to_float_rgb(rgb, clip=False)
    luminance = _tone_compute_luminance(input_rgb)
    dark_point, light_point = _tone_resolve_level_points(resolved.dark_point, resolved.light_point)

    working_luminance = _tone_normalize_levels(luminance, dark_point, light_point)
    working_rgb = _tone_apply_luminance_transfer(input_rgb, luminance, working_luminance)

    contrast_output = _tone_apply_contrast(working_luminance, resolved.contrast)
    working_rgb = _tone_apply_luminance_transfer(working_rgb, working_luminance, contrast_output)

    shadow_highlight_output = _tone_apply_shadow_highlights(contrast_output, resolved.shadows, resolved.highlights)
    working_rgb = _tone_apply_luminance_transfer(working_rgb, contrast_output, shadow_highlight_output)

    final_output = shadow_highlight_output
    if resolved.tone_curve_enabled:
        final_output = _tone_apply_tone_curve(
            shadow_highlight_output,
            resolved.tone_curve_strength,
            resolved.tone_curve_midpoint,
        )
        working_rgb = _tone_apply_luminance_transfer(working_rgb, shadow_highlight_output, final_output)

    working_rgb = np.clip(working_rgb, 0.0, 1.0)
    if not return_debug:
        return working_rgb
    debug = _tone_build_debug(
        input_rgb,
        resolved,
        manual_level_override=manual_level_override,
        preview_bucket=preview_bucket,
    )
    return working_rgb, debug


def compute_post_decode_transfer_curve(
    rgb: Any,
    settings: Any,
    *,
    samples: int = 2048,
    debug: TestToneDebugInfo | None = None,
) -> TestToneTransferCurve:
    """Return the tone transfer curve used by the local inspector."""
    resolved = _tone_coerce_settings(settings)
    resolved_debug = debug or _tone_build_debug(rgb, resolved)
    sample_count = max(2, int(samples))
    input_values = np.linspace(0.0, 1.0, sample_count, dtype=np.float64)

    dark_point, light_point = _tone_resolve_level_points(resolved_debug.dark_point, resolved_debug.light_point)
    levels_output = _tone_normalize_levels(input_values, dark_point, light_point)
    contrast_output = _tone_apply_contrast(levels_output, resolved.contrast)
    shadow_highlight_output = _tone_apply_shadow_highlights(contrast_output, resolved.shadows, resolved.highlights)
    final_output = shadow_highlight_output
    if resolved.tone_curve_enabled:
        final_output = _tone_apply_tone_curve(
            shadow_highlight_output,
            resolved.tone_curve_strength,
            resolved.tone_curve_midpoint,
        )

    return TestToneTransferCurve(
        input_values=input_values,
        levels_output=levels_output,
        contrast_output=contrast_output,
        shadow_highlight_output=shadow_highlight_output,
        final_output=final_output,
        debug=resolved_debug,
    )


def _tone_format_point(value: float) -> str:
    return f"{float(value):.3f}"


def _tone_format_signed(value: float) -> str:
    return f"{int(round(float(value) * 100.0)):+d}"


def _tone_format_percent(value: float) -> str:
    return f"{float(value):.3f}%"


def _draw_processing_figure(
    figure: Figure,
    source_rgb: np.ndarray,
    settings: Any,
    *,
    debug: TestToneDebugInfo | None = None,
) -> None:
    resolved = _tone_coerce_settings(settings)
    curve = compute_post_decode_transfer_curve(source_rgb, resolved, debug=debug)

    figure.clear()
    ax = figure.add_subplot(111)

    input_values = curve.input_values
    ax.plot(input_values, input_values, label="identity", linewidth=1.0, color="#95a5a6", linestyle="--")
    ax.plot(input_values, curve.levels_output, label="after levels", linewidth=2.2, color="#2980b9")
    ax.plot(input_values, curve.contrast_output, label="after contrast", linewidth=2.2, color="#8e44ad")
    ax.plot(
        input_values,
        curve.shadow_highlight_output,
        label="after shadows/highlights",
        linewidth=2.4,
        color="#e67e22",
    )
    final_label = "final with tone curve" if resolved.tone_curve_enabled else "final after shadows/highlights"
    ax.plot(input_values, curve.final_output, label=final_label, linewidth=2.6, color="#16a085")

    plotted_levels: list[float] = []

    def _plot_level(value: float | None, *, label: str, color: str, linestyle: str, alpha: float = 0.75) -> None:
        if value is None or not np.isfinite(float(value)):
            return
        for existing in plotted_levels:
            if abs(existing - float(value)) < 1e-6:
                return
        plotted_levels.append(float(value))
        ax.axvline(float(value), color=color, linestyle=linestyle, linewidth=1.0, alpha=alpha, label=label)

    _plot_level(curve.debug.auto_black_level, label="auto black", color="#2c3e50", linestyle="--", alpha=0.65)
    _plot_level(curve.debug.auto_white_level, label="auto white", color="#c0392b", linestyle="--", alpha=0.65)
    _plot_level(curve.debug.dark_point, label="dark point", color="#7f8c8d", linestyle="-", alpha=0.55)
    _plot_level(curve.debug.light_point, label="light point", color="#7f8c8d", linestyle="-", alpha=0.55)
    ax.plot(
        [curve.debug.dark_point, curve.debug.light_point],
        [0.0, 1.0],
        label="tone nodes",
        linestyle="None",
        marker="o",
        markersize=6.5,
        markerfacecolor="#f1c40f",
        markeredgecolor="#2c3e50",
        markeredgewidth=1.0,
        zorder=6,
    )

    ax.set_xlim(0.0, 1.0)
    ax.set_ylim(0.0, 1.0)
    ax.set_xlabel("Input luminance")
    ax.set_ylabel("Output luminance")
    ax.set_title("Processing curve")
    ax.grid(True, alpha=0.2)
    ax.legend(loc="best", ncols=2)


def _build_processing_figure(source_rgb: np.ndarray, settings: Any, *, debug: TestToneDebugInfo | None = None) -> Figure:
    figure = Figure(figsize=(8.4, 6.1), dpi=120, constrained_layout=True)
    _draw_processing_figure(figure, source_rgb, settings, debug=debug)
    return figure


def _save_plot(path: Path, source_rgb: np.ndarray, settings: Any, *, debug: TestToneDebugInfo | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    figure = _build_processing_figure(source_rgb, settings, debug=debug)
    figure.savefig(path, dpi=160)


def _settings_summary(
    settings: Any,
    *,
    debug: TestToneDebugInfo | None = None,
    wb_text: str | None = None,
    preview_bucket: int | None = None,
) -> str:
    resolved = _tone_coerce_settings(settings)
    parts: list[str] = []
    if wb_text:
        parts.append(wb_text)
    parts.append("auto levels on" if resolved.auto_levels else "auto levels off")
    if debug is not None and debug.auto_black_level is not None and debug.auto_white_level is not None:
        parts.append(
            f"auto black {debug.auto_black_level:.3f} / auto white {debug.auto_white_level:.3f}"
        )
    parts.append(f"dark point {_tone_format_point(resolved.dark_point)}")
    parts.append(f"light point {_tone_format_point(resolved.light_point)}")
    parts.append(f"contrast {_tone_format_signed(resolved.contrast)}")
    parts.append(f"shadows/highlights {_tone_format_signed(resolved.shadows)} / {_tone_format_signed(resolved.highlights)}")
    parts.append("tone curve on" if resolved.tone_curve_enabled else "tone curve off")
    if debug is not None and debug.manual_level_override and resolved.auto_levels:
        parts.append("manual override")
    if preview_bucket is not None:
        parts.append(f"preview {int(preview_bucket)}px")
    return " · ".join(parts)


def _preview_bucket_for_zoom(source_width: int, zoom_level: float) -> int:
    width = max(1, int(source_width))
    zoom = float(zoom_level)
    if zoom < _TONE_ZOOM_OUT_THRESHOLD:
        target = _TONE_PREVIEW_BUCKET_ZOOMED_OUT
    elif zoom > _TONE_ZOOM_IN_THRESHOLD:
        target = _TONE_PREVIEW_BUCKET_ZOOMED_IN
    else:
        target = _TONE_PREVIEW_BUCKET_NORMAL
    return max(1, min(width, int(target)))


def _resize_rgb_preview(rgb: np.ndarray, max_width: int) -> np.ndarray:
    arr = np.asarray(rgb, dtype=np.float64)
    if arr.size == 0:
        return arr.copy()
    width = int(arr.shape[1])
    height = int(arr.shape[0])
    bucket = max(1, int(max_width))
    if width <= bucket:
        return arr.copy()
    new_height = max(1, int(round(float(height) * float(bucket) / float(width))))
    image8 = np.rint(np.clip(arr[..., :3], 0.0, 1.0) * 255.0).astype(np.uint8)
    resized = Image.fromarray(image8, mode="RGB").resize((bucket, new_height), Image.Resampling.LANCZOS)
    return np.asarray(resized, dtype=np.float64) / 255.0


def _apply_preview_bucket_rgb(rgb: np.ndarray, zoom_level: float) -> tuple[np.ndarray, int]:
    bucket = _preview_bucket_for_zoom(int(rgb.shape[1]), zoom_level)
    return _resize_rgb_preview(rgb, bucket), bucket


def _tone_settings_from_args(args: argparse.Namespace, source_rgb: np.ndarray) -> TestToneSettings:
    dark_cutoff = float(getattr(args, "dark_cutoff", 0.01))
    bright_cutoff = float(getattr(args, "bright_cutoff", 0.01))
    auto_levels = bool(getattr(args, "auto_levels", True))
    auto_black, auto_white = _tone_compute_auto_level_bounds(source_rgb, dark_cutoff, bright_cutoff)
    dark_point = getattr(args, "dark_point", None)
    light_point = getattr(args, "light_point", None)
    if dark_point is None or light_point is None:
        if auto_levels and auto_black is not None and auto_white is not None:
            dark_point = auto_black if dark_point is None else dark_point
            light_point = auto_white if light_point is None else light_point
        else:
            dark_point = 0.0 if dark_point is None else dark_point
            light_point = 1.0 if light_point is None else light_point

    dark_point, light_point = _tone_resolve_level_points(float(dark_point), float(light_point))
    return TestToneSettings(
        dark_point=dark_point,
        light_point=light_point,
        auto_levels=auto_levels,
        dark_cutoff=dark_cutoff,
        bright_cutoff=bright_cutoff,
        contrast=float(getattr(args, "contrast", 0.0)) / 100.0,
        shadows=float(getattr(args, "shadows", 0.0)) / 100.0,
        highlights=float(getattr(args, "highlights", 0.0)) / 100.0,
        tone_curve_enabled=bool(getattr(args, "tone_curve", False)),
        tone_curve_strength=float(getattr(args, "curve_strength", 0.5)),
        tone_curve_midpoint=float(getattr(args, "curve_midpoint", 0.5)),
    )


class ProcessingCurveWindow(QMainWindow):
    """Interactive inspector for the local tone prototype."""

    def __init__(
        self,
        source_path: Path | None = None,
        *,
        plot_out: Path | None = None,
        preview_out: Path | None = None,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._source_path = source_path.expanduser().resolve() if source_path is not None else None
        self._plot_out = plot_out
        self._preview_out = preview_out
        self._is_raw = bool(self._source_path is not None and is_raw_image_path(self._source_path))
        self._raw_base_mode = "camera"
        self._raw_tempdir = tempfile.TemporaryDirectory(prefix="sporely_curve_") if self._is_raw else None
        self._has_source = self._source_path is not None
        self._source = self._load_source()
        self._analysis_rgb = np.zeros((0, 0, 3), dtype=np.float64)
        self._analysis_luminance = np.zeros((0, 0), dtype=np.float64)
        self._preview_source_cache: dict[int, np.ndarray] = {}
        self._preview_bucket_value = 1
        self._manual_level_override = False
        self._auto_black_point: float | None = None
        self._auto_white_point: float | None = None
        self._wb_signature: tuple[Any, ...] | None = None
        self._settings = TestToneSettings.default()
        self._wb_pick_armed = False
        self._refresh_timer = QTimer(self)
        self._refresh_timer.setSingleShot(True)
        self._refresh_timer.setInterval(16)
        self._refresh_timer.timeout.connect(self._refresh_outputs)
        self._build_ui()
        if self._has_source:
            self._rebuild_analysis_source(force_auto=True)
            self._refresh_outputs()
        else:
            self._set_blank_outputs()

    def closeEvent(self, event) -> None:  # type: ignore[override]
        try:
            self._refresh_timer.stop()
            if self._raw_tempdir is not None:
                self._raw_tempdir.cleanup()
        finally:
            super().closeEvent(event)

    def eventFilter(self, obj, event) -> bool:  # type: ignore[override]
        if obj is self.preview_label and event.type() in {QEvent.Type.Wheel, QEvent.Type.Resize}:
            self._queue_refresh()
        return super().eventFilter(obj, event)

    def _load_source(self) -> LoadedSource:
        if self._source_path is None:
            return LoadedSource("blank", np.zeros((0, 0, 3), dtype=np.float64))
        if self._is_raw:
            scratch_dir = Path(self._raw_tempdir.name) if self._raw_tempdir is not None else None
            return _load_source_image(self._source_path, raw_base_mode=self._raw_base_mode, scratch_dir=scratch_dir)
        return _load_source_image(self._source_path)

    def _build_ui(self) -> None:
        if self._source_path is not None:
            self.setWindowTitle(f"Sporely curve inspector - {self._source_path.name}")
        else:
            self.setWindowTitle("Sporely curve inspector")
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
        panel.setMinimumWidth(350)
        panel.setMaximumWidth(450)
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        source_group = QGroupBox("Source")
        source_form = QFormLayout(source_group)
        source_form.setLabelAlignment(Qt.AlignRight)
        self.source_path_label = QLabel(str(self._source_path) if self._source_path is not None else "No file loaded")
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

        self.wb_sample_size_spin = QSpinBox()
        self.wb_sample_size_spin.setRange(1, 128)
        self.wb_sample_size_spin.setValue(10)
        self.wb_sample_size_spin.valueChanged.connect(self._queue_refresh)
        source_form.addRow("WB sample size", self.wb_sample_size_spin)

        self.open_file_btn = QPushButton("Open file...")
        self.open_file_btn.clicked.connect(self._open_source_dialog)
        source_form.addRow(self.open_file_btn)
        layout.addWidget(source_group)

        wb_group = QGroupBox("White Balance")
        wb_layout = QVBoxLayout(wb_group)
        wb_layout.setContentsMargins(8, 8, 8, 8)
        wb_layout.setSpacing(8)
        self.raw_controls = RawProcessingControls(wb_group, show_shadow_lift=True)
        self.raw_controls.settingsChanged.connect(self._on_wb_controls_changed)
        self.raw_controls.pickWhiteBalanceToggled.connect(self._toggle_wb_pick)
        wb_layout.addWidget(self.raw_controls)
        self.wb_pick_btn = self.raw_controls.pick_button
        self.wb_readout_label = QLabel("Camera WB")
        self.wb_readout_label.setWordWrap(True)
        wb_layout.addWidget(self.wb_readout_label)
        self._hide_wb_helper_rows()
        layout.addWidget(wb_group)

        tone_group = QGroupBox("Tone")
        tone_form = QFormLayout(tone_group)
        tone_form.setLabelAlignment(Qt.AlignRight)

        self.auto_levels_checkbox = QCheckBox("Auto levels")
        self.auto_levels_checkbox.setChecked(True)
        self.auto_levels_checkbox.toggled.connect(self._on_auto_levels_toggled)
        tone_form.addRow(self.auto_levels_checkbox)

        dark_point_row, self.dark_slider, self.dark_value_label = self._create_point_slider_row(value=0.0)
        tone_form.addRow("Dark point", dark_point_row)
        light_point_row, self.light_slider, self.light_value_label = self._create_point_slider_row(value=1.0)
        tone_form.addRow("Light point", light_point_row)
        self.exposure_slider = self.light_slider
        self.exposure_value_label = self.light_value_label

        dark_cutoff_row, self.dark_cutoff_slider, self.dark_cutoff_value_label = self._create_cutoff_slider_row(value=0.01)
        tone_form.addRow("Dark cutoff", dark_cutoff_row)
        bright_cutoff_row, self.bright_cutoff_slider, self.bright_cutoff_value_label = self._create_cutoff_slider_row(value=0.01)
        tone_form.addRow("Bright cutoff", bright_cutoff_row)

        contrast_row, self.contrast_slider, self.contrast_value_label = self._create_signed_slider_row(value=0.0)
        tone_form.addRow("Contrast", contrast_row)

        shadows_row, self.shadows_slider, self.shadows_value_label = self._create_signed_slider_row(value=0.0)
        tone_form.addRow("Shadows", shadows_row)
        self.shadow_lift_slider = self.shadows_slider
        self.shadow_lift_value_label = self.shadows_value_label

        highlights_row, self.highlights_slider, self.highlights_value_label = self._create_signed_slider_row(value=0.0)
        tone_form.addRow("Highlights", highlights_row)

        layout.addWidget(tone_group)

        advanced_group = QGroupBox("Advanced / Tone Curve")
        advanced_form = QFormLayout(advanced_group)
        advanced_form.setLabelAlignment(Qt.AlignRight)

        self.tone_curve_checkbox = QCheckBox("Tone curve")
        self.tone_curve_checkbox.setChecked(False)
        self.tone_curve_checkbox.toggled.connect(self._queue_refresh)
        advanced_form.addRow(self.tone_curve_checkbox)

        curve_strength_row, self.curve_strength_slider, self.curve_strength_value_label = self._create_fraction_slider_row(value=0.5)
        advanced_form.addRow("Curve strength", curve_strength_row)
        curve_midpoint_row, self.curve_midpoint_slider, self.curve_midpoint_value_label = self._create_fraction_slider_row(value=0.5)
        advanced_form.addRow("Curve midpoint", curve_midpoint_row)
        self.midpoint_slider = self.curve_midpoint_slider
        self.midpoint_value_label = self.curve_midpoint_value_label
        self.contrast_label = QLabel("Contrast")
        self.midpoint_label = QLabel("Midpoint")
        layout.addWidget(advanced_group)

        stats_group = QGroupBox("Current output")
        stats_form = QFormLayout(stats_group)
        stats_form.setLabelAlignment(Qt.AlignRight)
        self.input_min_label = QLabel("—")
        self.input_max_label = QLabel("—")
        self.auto_black_label = QLabel("—")
        self.auto_white_label = QLabel("—")
        self.dark_point_stats_label = QLabel("—")
        self.light_point_stats_label = QLabel("—")
        self.settings_label = QLabel("—")
        self.settings_label.setWordWrap(True)
        stats_form.addRow("Input min", self.input_min_label)
        stats_form.addRow("Input max", self.input_max_label)
        stats_form.addRow("Auto black", self.auto_black_label)
        stats_form.addRow("Auto white", self.auto_white_label)
        stats_form.addRow("Dark point", self.dark_point_stats_label)
        stats_form.addRow("Light point", self.light_point_stats_label)
        stats_form.addRow("Settings", self.settings_label)
        layout.addWidget(stats_group)

        layout.addStretch(1)
        self._set_processing_controls_enabled(self._has_source)
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
        self.preview_label.installEventFilter(self)
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

    def _hide_wb_helper_rows(self) -> None:
        for row_name in (
            "light_row",
            "dark_row",
            "auto_levels_checkbox",
            "tone_curve_checkbox",
            "curve_strength_row",
            "curve_midpoint_row",
            "shadow_lift_row",
        ):
            widget = getattr(self.raw_controls, row_name, None)
            if widget is not None:
                widget.setVisible(False)

    def _create_point_slider_row(self, *, value: float) -> tuple[QWidget, QSlider, QLabel]:
        row = QWidget(self)
        row_layout = QHBoxLayout(row)
        row_layout.setContentsMargins(0, 0, 0, 0)
        row_layout.setSpacing(8)

        slider = QSlider(Qt.Horizontal, row)
        slider.setRange(0, _TONE_POINT_SCALE)
        slider.setSingleStep(1)
        slider.setPageStep(10)
        slider.setValue(self._point_to_slider_value(value))
        row_layout.addWidget(slider, 1)

        value_label = QLabel(row)
        value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        value_label.setMinimumWidth(72)
        value_label.setText(self._point_slider_value_text(slider.value()))
        row_layout.addWidget(value_label, 0)

        def _sync_slider_value(slider_value: int) -> None:
            value_label.setText(self._point_slider_value_text(slider_value))
            if slider is self.dark_slider or slider is self.light_slider:
                if self.auto_levels_checkbox.isChecked():
                    self._manual_level_override = True
            self._queue_refresh()

        slider.valueChanged.connect(_sync_slider_value)
        return row, slider, value_label

    def _create_cutoff_slider_row(self, *, value: float) -> tuple[QWidget, QSlider, QLabel]:
        row = QWidget(self)
        row_layout = QHBoxLayout(row)
        row_layout.setContentsMargins(0, 0, 0, 0)
        row_layout.setSpacing(8)

        slider = QSlider(Qt.Horizontal, row)
        slider.setRange(0, _TONE_CUTOFF_SCALE)
        slider.setSingleStep(1)
        slider.setPageStep(1)
        slider.setValue(self._cutoff_percent_to_slider_value(value))
        row_layout.addWidget(slider, 1)

        value_label = QLabel(row)
        value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        value_label.setMinimumWidth(72)
        value_label.setText(self._cutoff_slider_value_text(slider.value()))
        row_layout.addWidget(value_label, 0)

        def _sync_slider_value(slider_value: int) -> None:
            value_label.setText(self._cutoff_slider_value_text(slider_value))
            self._apply_auto_level_points(force=True)
            self._queue_refresh()

        slider.valueChanged.connect(_sync_slider_value)
        return row, slider, value_label

    def _create_signed_slider_row(self, *, value: float) -> tuple[QWidget, QSlider, QLabel]:
        row = QWidget(self)
        row_layout = QHBoxLayout(row)
        row_layout.setContentsMargins(0, 0, 0, 0)
        row_layout.setSpacing(8)

        slider = QSlider(Qt.Horizontal, row)
        slider.setRange(-_TONE_SIGNED_SCALE, _TONE_SIGNED_SCALE)
        slider.setSingleStep(1)
        slider.setPageStep(5)
        slider.setValue(self._signed_to_slider_value(value))
        row_layout.addWidget(slider, 1)

        value_label = QLabel(row)
        value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        value_label.setMinimumWidth(72)
        value_label.setText(self._signed_slider_value_text(slider.value()))
        row_layout.addWidget(value_label, 0)

        def _sync_slider_value(slider_value: int) -> None:
            value_label.setText(self._signed_slider_value_text(slider_value))
            self._queue_refresh()

        slider.valueChanged.connect(_sync_slider_value)
        return row, slider, value_label

    def _create_fraction_slider_row(self, *, value: float) -> tuple[QWidget, QSlider, QLabel]:
        row = QWidget(self)
        row_layout = QHBoxLayout(row)
        row_layout.setContentsMargins(0, 0, 0, 0)
        row_layout.setSpacing(8)

        slider = QSlider(Qt.Horizontal, row)
        slider.setRange(0, 100)
        slider.setSingleStep(1)
        slider.setPageStep(5)
        slider.setValue(self._fraction_to_slider_value(value))
        row_layout.addWidget(slider, 1)

        value_label = QLabel(row)
        value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        value_label.setMinimumWidth(72)
        value_label.setText(self._fraction_slider_value_text(slider.value()))
        row_layout.addWidget(value_label, 0)

        def _sync_slider_value(slider_value: int) -> None:
            value_label.setText(self._fraction_slider_value_text(slider_value))
            self._queue_refresh()

        slider.valueChanged.connect(_sync_slider_value)
        return row, slider, value_label

    @staticmethod
    def _point_to_slider_value(value: float) -> int:
        return int(round(float(np.clip(float(value), 0.0, 1.0)) * float(_TONE_POINT_SCALE)))

    @staticmethod
    def _point_slider_value_float(value: int) -> float:
        return float(np.clip(float(value) / float(_TONE_POINT_SCALE), 0.0, 1.0))

    @classmethod
    def _point_slider_value_text(cls, value: int) -> str:
        return _tone_format_point(cls._point_slider_value_float(value))

    @staticmethod
    def _signed_to_slider_value(value: float) -> int:
        return int(round(float(np.clip(float(value), -1.0, 1.0)) * float(_TONE_SIGNED_SCALE)))

    @staticmethod
    def _signed_slider_value_float(value: int) -> float:
        return float(np.clip(float(value) / float(_TONE_SIGNED_SCALE), -1.0, 1.0))

    @classmethod
    def _signed_slider_value_text(cls, value: int) -> str:
        return f"{int(round(cls._signed_slider_value_float(value) * 100.0)):+d}"

    @staticmethod
    def _fraction_to_slider_value(value: float) -> int:
        return int(round(float(np.clip(float(value), 0.0, 1.0)) * 100.0))

    @staticmethod
    def _fraction_slider_value_float(value: int) -> float:
        return float(np.clip(float(value) / 100.0, 0.0, 1.0))

    @classmethod
    def _fraction_slider_value_text(cls, value: int) -> str:
        return f"{cls._fraction_slider_value_float(value):.2f}"

    @staticmethod
    def _cutoff_percent_to_slider_value(cutoff_percent: float) -> int:
        return int(round(float(np.clip(float(cutoff_percent), 0.0, 0.02)) * 1000.0))

    @staticmethod
    def _cutoff_slider_value_to_percent(value: int) -> float:
        return float(np.clip(float(value) / 1000.0, 0.0, 0.02))

    @classmethod
    def _cutoff_slider_value_text(cls, value: int) -> str:
        return _tone_format_percent(cls._cutoff_slider_value_to_percent(value))

    def _set_combo_data(self, combo: QComboBox, data: str) -> None:
        index = combo.findData(data)
        if index >= 0:
            with QSignalBlocker(combo):
                combo.setCurrentIndex(index)

    def _update_wb_readout(self, settings: RawRenderSettings | None = None) -> None:
        resolved = RawRenderSettings.from_dict(settings or self.raw_controls.settings())
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

    def _analysis_source_rgb(self) -> np.ndarray:
        source_rgb = self._source.rgb
        wb_settings = RawRenderSettings.from_dict(self.raw_controls.settings())
        mode = str(wb_settings.white_balance_mode or "camera").strip().lower() or "camera"
        if mode == "custom" and wb_settings.wb_multipliers is not None:
            return _tone_apply_channel_gains(source_rgb, wb_settings.wb_multipliers)
        return source_rgb

    def _rebuild_analysis_source(self, *, force_auto: bool = False) -> None:
        self._analysis_rgb = self._analysis_source_rgb()
        self._analysis_luminance = _tone_compute_luminance(self._analysis_rgb)
        self._preview_source_cache.clear()
        self._preview_bucket_value = _preview_bucket_for_zoom(
            int(self._analysis_rgb.shape[1]) if self._analysis_rgb.size else 1,
            getattr(self.preview_label, "zoom_level", 1.0),
        )
        self._apply_auto_level_points(force=force_auto)

    def _apply_auto_level_points(self, *, force: bool = False) -> None:
        auto_black, auto_white = _tone_compute_auto_level_bounds(
            self._analysis_rgb,
            self._cutoff_slider_value_to_percent(self.dark_cutoff_slider.value()),
            self._cutoff_slider_value_to_percent(self.bright_cutoff_slider.value()),
        )
        self._auto_black_point = auto_black
        self._auto_white_point = auto_white
        if self.auto_levels_checkbox.isChecked() and (force or not self._manual_level_override):
            if auto_black is not None and auto_white is not None:
                self._set_point_slider(self.dark_slider, self.dark_value_label, auto_black)
                self._set_point_slider(self.light_slider, self.light_value_label, auto_white)
            self._manual_level_override = False
        self._update_auto_level_stats()

    def _set_point_slider(self, slider: QSlider, label: QLabel, value: float) -> None:
        with QSignalBlocker(slider):
            slider.setValue(self._point_to_slider_value(value))
        label.setText(self._point_slider_value_text(slider.value()))

    def _update_auto_level_stats(self) -> None:
        self.auto_black_label.setText("none" if self._auto_black_point is None else _tone_format_point(self._auto_black_point))
        self.auto_white_label.setText("none" if self._auto_white_point is None else _tone_format_point(self._auto_white_point))
        self.dark_point_stats_label.setText(_tone_format_point(self._point_slider_value_float(self.dark_slider.value())))
        self.light_point_stats_label.setText(_tone_format_point(self._point_slider_value_float(self.light_slider.value())))

    def _settings_from_controls(self) -> TestToneSettings:
        dark_point = self._point_slider_value_float(self.dark_slider.value())
        light_point = self._point_slider_value_float(self.light_slider.value())
        if self.auto_levels_checkbox.isChecked() and not self._manual_level_override:
            if self._auto_black_point is not None and self._auto_white_point is not None:
                dark_point = self._auto_black_point
                light_point = self._auto_white_point
        dark_point, light_point = _tone_resolve_level_points(dark_point, light_point)
        return TestToneSettings(
            dark_point=dark_point,
            light_point=light_point,
            auto_levels=bool(self.auto_levels_checkbox.isChecked()),
            dark_cutoff=self._cutoff_slider_value_to_percent(self.dark_cutoff_slider.value()),
            bright_cutoff=self._cutoff_slider_value_to_percent(self.bright_cutoff_slider.value()),
            contrast=self._signed_slider_value_float(self.contrast_slider.value()),
            shadows=self._signed_slider_value_float(self.shadows_slider.value()),
            highlights=self._signed_slider_value_float(self.highlights_slider.value()),
            tone_curve_enabled=bool(self.tone_curve_checkbox.isChecked()),
            tone_curve_strength=self._fraction_slider_value_float(self.curve_strength_slider.value()),
            tone_curve_midpoint=self._fraction_slider_value_float(self.curve_midpoint_slider.value()),
        )

    def _set_processing_controls_enabled(self, enabled: bool) -> None:
        for widget in (
            self.raw_controls,
            self.raw_base_mode_combo,
            self.wb_sample_size_spin,
            self.auto_levels_checkbox,
            self.dark_slider,
            self.light_slider,
            self.dark_cutoff_slider,
            self.bright_cutoff_slider,
            self.contrast_slider,
            self.shadows_slider,
            self.highlights_slider,
            self.tone_curve_checkbox,
            self.curve_strength_slider,
            self.curve_midpoint_slider,
        ):
            widget.setEnabled(bool(enabled))
        self.open_file_btn.setEnabled(True)

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
        self._rebuild_analysis_source(force_auto=True)
        self._set_processing_controls_enabled(True)
        self._refresh_outputs()

    def _update_source_widgets(self) -> None:
        if self._source_path is None:
            self.setWindowTitle("Sporely curve inspector")
            self.source_path_label.setText("No file loaded")
            self.source_kind_label.setText("Blank")
            self.raw_base_mode_label.setVisible(False)
            self.raw_base_mode_combo.setVisible(False)
            self.wb_sample_size_spin.setEnabled(False)
            self.wb_readout_label.setText("No file loaded")
            return

        self.setWindowTitle(f"Sporely curve inspector - {self._source_path.name}")
        self.source_path_label.setText(str(self._source_path))
        self.source_kind_label.setText("RAW" if self._is_raw else "Raster")
        self.raw_base_mode_label.setVisible(self._is_raw)
        self.raw_base_mode_combo.setVisible(self._is_raw)
        self.wb_sample_size_spin.setEnabled(True)
        if self._is_raw:
            self._set_combo_data(self.raw_base_mode_combo, self._raw_base_mode)

    def _reset_white_balance_controls(self) -> None:
        if self._is_raw:
            self._set_combo_data(self.raw_base_mode_combo, self._raw_base_mode)
        current = RawRenderSettings.from_dict(self.raw_controls.settings())
        reset = replace(
            current,
            white_balance_mode="camera",
            wb_multipliers=None,
            wb_selection=None,
            wb_multiplier_space=None,
            wb_sample_point=None,
            wb_selection_space=None,
            wb_sample_base_mode=self._raw_base_mode if self._is_raw else None,
            wb_sample_size=int(self.wb_sample_size_spin.value()),
        )
        self.raw_controls.set_settings(reset)
        self._update_wb_readout(reset)
        self._wb_signature = self._wb_signature_for_settings(reset)
        if self.wb_pick_btn.isChecked():
            self.raw_controls.set_pick_checked(False)
        self._wb_pick_armed = False

    def _wb_signature_for_settings(self, settings: RawRenderSettings | None = None) -> tuple[Any, ...]:
        resolved = RawRenderSettings.from_dict(settings or self.raw_controls.settings())
        mode = str(resolved.white_balance_mode or "camera").strip().lower() or "camera"
        multipliers = None
        if resolved.wb_multipliers is not None:
            multipliers = tuple(float(value) for value in resolved.wb_multipliers)
        return (
            mode,
            multipliers,
            self._raw_base_mode if self._is_raw else None,
            self._source_path,
        )

    def _sync_wb_from_controls(self, *, force_rebuild: bool = False) -> None:
        resolved = RawRenderSettings.from_dict(self.raw_controls.settings())
        mode = str(resolved.white_balance_mode or "camera").strip().lower() or "camera"
        if self._is_raw and mode in {"camera", "auto"} and mode != self._raw_base_mode:
            self._raw_base_mode = mode
            self._set_combo_data(self.raw_base_mode_combo, mode)
            self._source = self._load_source()
            force_rebuild = True
        signature = self._wb_signature_for_settings(resolved)
        if force_rebuild or signature != self._wb_signature:
            self._wb_signature = signature
            self._rebuild_analysis_source(force_auto=True)
        self._update_wb_readout(resolved)
        self._queue_refresh()

    def _on_wb_controls_changed(self, *_args) -> None:
        self._sync_wb_from_controls(force_rebuild=True)

    def _on_raw_base_mode_changed(self, *_args) -> None:
        if not self._is_raw:
            return
        self._raw_base_mode = str(self.raw_base_mode_combo.currentData() or "camera").strip().lower() or "camera"
        current = RawRenderSettings.from_dict(self.raw_controls.settings())
        if str(current.white_balance_mode or "camera").strip().lower() in {"camera", "auto"}:
            self.raw_controls.set_settings(
                replace(current, white_balance_mode=self._raw_base_mode, wb_sample_base_mode=self._raw_base_mode)
            )
        self._source = self._load_source()
        self._sync_wb_from_controls(force_rebuild=True)

    def _on_auto_levels_toggled(self, checked: bool) -> None:
        if checked:
            self._manual_level_override = False
            self._apply_auto_level_points(force=True)
        self._queue_refresh()

    def _queue_refresh(self, *_args) -> None:
        self._refresh_timer.stop()
        self._refresh_timer.start()

    def _current_preview_bucket(self) -> int:
        if self._analysis_rgb.size == 0:
            return 1
        return _preview_bucket_for_zoom(
            int(self._analysis_rgb.shape[1]),
            getattr(self.preview_label, "zoom_level", 1.0),
        )

    def _preview_source_for_bucket(self, bucket: int) -> np.ndarray:
        bucket = max(1, int(bucket))
        cached = self._preview_source_cache.get(bucket)
        if cached is not None:
            return cached
        preview_source = _resize_rgb_preview(self._analysis_rgb, bucket)
        self._preview_source_cache[bucket] = preview_source
        return preview_source

    def _sample_rect_from_point(
        self,
        point: QPointF,
        *,
        image_size: tuple[int, int],
        sample_size: int | None = None,
    ) -> tuple[float, float, float, float] | None:
        width = float(image_size[0])
        height = float(image_size[1])
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
        if self._analysis_rgb.size == 0:
            return False
        preview_pixmap = self.preview_label.original_pixmap
        if preview_pixmap is None or preview_pixmap.isNull():
            return False

        source_width = float(self._analysis_rgb.shape[1])
        source_height = float(self._analysis_rgb.shape[0])
        preview_width = max(1.0, float(preview_pixmap.width()))
        preview_height = max(1.0, float(preview_pixmap.height()))
        mapped_point = QPointF(
            float(point.x()) * source_width / preview_width,
            float(point.y()) * source_height / preview_height,
        )
        sample_rect = self._sample_rect_from_point(mapped_point, image_size=(int(source_width), int(source_height)))
        if sample_rect is None:
            self.statusBar().showMessage("Could not sample WB from that point", 4000)
            return False
        try:
            multipliers = estimate_white_balance_from_background(self._analysis_rgb, rect=sample_rect)
        except Exception as exc:
            self.statusBar().showMessage(f"Could not sample background WB: {exc}", 5000)
            return False

        base_mode = self._raw_base_mode if self._is_raw else None
        updated = replace(
            RawRenderSettings.from_dict(self.raw_controls.settings()),
            white_balance_mode="custom",
            wb_multipliers=(float(multipliers[0]), float(multipliers[1]), float(multipliers[2])),
            wb_selection=(float(sample_rect[0]), float(sample_rect[1]), float(sample_rect[2]), float(sample_rect[3])),
            wb_multiplier_space="post_decode_rgb",
            wb_sample_point=(float(point.x()), float(point.y())),
            wb_sample_size=int(self.wb_sample_size_spin.value()),
            wb_sample_base_mode=base_mode,
            wb_selection_space="preview_pixels",
        )
        self.raw_controls.set_settings(updated)
        self._update_wb_readout(updated)
        if self.wb_pick_btn.isChecked():
            self.raw_controls.set_pick_checked(False)
        self._wb_pick_armed = False
        self._sync_wb_from_controls(force_rebuild=True)
        return True

    def _on_preview_clicked(self, point: QPointF) -> None:
        self._apply_wb_pick_from_point(point)

    def _refresh_outputs(self) -> None:
        if self._source_path is None:
            self._set_blank_outputs()
            return
        if self._analysis_rgb.size == 0:
            self._rebuild_analysis_source(force_auto=True)

        self._settings = self._settings_from_controls()
        preview_bucket = self._current_preview_bucket()
        preview_source = self._preview_source_for_bucket(preview_bucket)
        processed_rgb = apply_post_decode_processing(
            preview_source,
            self._settings,
            manual_level_override=self._manual_level_override,
            preview_bucket=preview_bucket,
        )
        debug = _tone_build_debug(
            self._analysis_rgb,
            self._settings,
            manual_level_override=self._manual_level_override,
            preview_bucket=preview_bucket,
            extra_settings={
                "white_balance_mode": str(RawRenderSettings.from_dict(self.raw_controls.settings()).white_balance_mode or "camera").strip().lower() or "camera",
                "raw_base_mode": self._raw_base_mode if self._is_raw else None,
                "wb_multipliers": list(RawRenderSettings.from_dict(self.raw_controls.settings()).wb_multipliers)
                if RawRenderSettings.from_dict(self.raw_controls.settings()).wb_multipliers is not None
                else None,
            },
        )
        self._debug = debug
        curve = compute_post_decode_transfer_curve(self._analysis_rgb, self._settings, debug=debug)

        preview_pixmap = _rgb_to_pixmap(processed_rgb)
        self.preview_label.set_image_sources(preview_pixmap, preserve_view=True)
        self.preview_label.set_pan_without_shift(True)

        self.figure.clear()
        _draw_processing_figure(self.figure, self._analysis_rgb, self._settings, debug=debug)
        self.canvas.draw()

        self.input_min_label.setText(f"{debug.input_min:.6f}")
        self.input_max_label.setText(f"{debug.input_max:.6f}")
        self.auto_black_label.setText("none" if debug.auto_black_level is None else f"{debug.auto_black_level:.6f}")
        self.auto_white_label.setText("none" if debug.auto_white_level is None else f"{debug.auto_white_level:.6f}")
        self.dark_point_stats_label.setText(f"{debug.dark_point:.6f}")
        self.light_point_stats_label.setText(f"{debug.light_point:.6f}")
        wb_text = self._white_balance_readout_text(RawRenderSettings.from_dict(self.raw_controls.settings()))
        summary = _settings_summary(
            self._settings,
            debug=debug,
            wb_text=wb_text,
            preview_bucket=preview_bucket,
        )
        self.settings_label.setText(summary)
        self._update_wb_readout()
        self.statusBar().showMessage(summary)

        if self._preview_out is not None:
            _save_preview(self._preview_out, processed_rgb)
        if self._plot_out is not None:
            _save_plot(self._plot_out, self._analysis_rgb, self._settings, debug=debug)

    def _set_blank_outputs(self) -> None:
        self.preview_label.set_image_sources(QPixmap())
        self.figure.clear()
        self.canvas.draw()
        self.input_min_label.setText("—")
        self.input_max_label.setText("—")
        self.auto_black_label.setText("—")
        self.auto_white_label.setText("—")
        self.dark_point_stats_label.setText("—")
        self.light_point_stats_label.setText("—")
        self.settings_label.setText("Open a file to inspect tone processing")
        self.wb_readout_label.setText("No file loaded")
        self.statusBar().showMessage("Open a file to inspect tone processing")


class _ProcessingCurveWindowCompat(ProcessingCurveWindow):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if not getattr(self, "_has_source", False):
            self._update_source_widgets()

    def _open_source_dialog(self) -> None:
        file_path, _selected_filter = QFileDialog.getOpenFileName(
            self,
            "Open image",
            str(self._source_path.parent if self._source_path is not None else Path.cwd()),
            _open_file_filter(),
        )
        if not file_path:
            return
        try:
            self._load_source_path(Path(file_path))
        except Exception as exc:
            self.statusBar().showMessage(str(exc), 5000)

    def _toggle_wb_pick(self, checked: bool) -> None:
        self._wb_pick_armed = bool(checked)
        self.preview_label.setCursor(Qt.CrossCursor if checked else Qt.ArrowCursor)
        if checked:
            self.statusBar().showMessage("Click neutral background to sample WB")
        else:
            self.statusBar().clearMessage()


ProcessingCurveWindow = _ProcessingCurveWindowCompat


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_path", type=Path, nargs="?", default=None, help="Source image to analyze")
    parser.add_argument("--plot-out", type=Path, help="Path for the curve plot PNG snapshot")
    parser.add_argument("--preview-out", type=Path, help="Path for the processed preview PNG snapshot")
    parser.add_argument("--interactive", action="store_true", help="Open an interactive window with live controls")
    parser.add_argument("--dark-point", type=float, default=None, help="Manual dark point in normalized luminance")
    parser.add_argument("--light-point", type=float, default=None, help="Manual light point in normalized luminance")
    parser.add_argument("--contrast", type=float, default=0.0, help="Contrast from -100 to 100")
    parser.add_argument("--shadows", type=float, default=0.0, help="Shadow lift from -100 to 100")
    parser.add_argument("--highlights", type=float, default=0.0, help="Highlight recovery from -100 to 100")
    parser.add_argument(
        "--auto-levels",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable or disable automatic level detection",
    )
    parser.add_argument(
        "--dark-cutoff",
        "--black-quantile",
        dest="dark_cutoff",
        type=float,
        default=0.01,
        help="Dark cutoff quantile used by auto levels",
    )
    parser.add_argument(
        "--bright-cutoff",
        "--white-quantile",
        dest="bright_cutoff",
        type=float,
        default=0.01,
        help="Bright cutoff quantile used by auto levels",
    )
    parser.add_argument("--tone-curve", action=argparse.BooleanOptionalAction, default=False, help="Apply the tone curve stage")
    parser.add_argument("--curve-strength", type=float, default=0.5, help="Tone curve strength")
    parser.add_argument("--curve-midpoint", type=float, default=0.5, help="Tone curve midpoint")
    parser.add_argument("--light-ev", type=float, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--dark-ev", type=float, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--exposure-ev", type=float, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--auto-levels-strength", type=float, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--soft-tails", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--tail-size", type=float, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--shadow-lift", type=float, default=None, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.input_path is None:
        if args.plot_out is not None or args.preview_out is not None:
            parser.error("input_path is required when using --plot-out or --preview-out")
        args.interactive = True
    if not args.interactive and (args.plot_out is None or args.preview_out is None):
        parser.error("--plot-out and --preview-out are required unless --interactive is set")
    return args


def _run_snapshot_mode(args: argparse.Namespace) -> int:
    source_path = Path(args.input_path).expanduser().resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"Input image not found: {source_path}")

    source = _load_source_image(source_path)
    settings = _tone_settings_from_args(args, source.rgb)
    preview_bucket = _preview_bucket_for_zoom(int(source.rgb.shape[1]), 1.0)
    preview_rgb = _resize_rgb_preview(source.rgb, preview_bucket)
    processed_rgb = apply_post_decode_processing(
        preview_rgb,
        settings,
        preview_bucket=preview_bucket,
    )
    debug = _tone_build_debug(
        source.rgb,
        settings,
        preview_bucket=preview_bucket,
        extra_settings={
            "source_path": str(source_path),
            "source_kind": source.kind,
            "preview_mode": "preview-resized",
        },
    )
    curve = compute_post_decode_transfer_curve(source.rgb, settings, debug=debug)

    preview_out = args.preview_out.expanduser().resolve()
    plot_out = args.plot_out.expanduser().resolve()
    _save_preview(preview_out, processed_rgb)
    _save_plot(plot_out, source.rgb, settings, debug=debug)

    print(f"input min luminance: {debug.input_min:.6f}")
    print(f"input max luminance: {debug.input_max:.6f}")
    print(f"detected auto black: {debug.auto_black_level if debug.auto_black_level is not None else 'none'}")
    print(f"detected auto white: {debug.auto_white_level if debug.auto_white_level is not None else 'none'}")
    print("settings JSON:")
    print(json.dumps(debug.settings, indent=2, sort_keys=True))
    print(f"plot out: {plot_out}")
    print(f"preview out: {preview_out}")
    print(f"snapshot preview bucket: {preview_bucket}px")
    print(f"final curve points: {curve.input_values.size}")
    return 0


def _run_interactive_mode(args: argparse.Namespace) -> int:
    source_path = Path(args.input_path).expanduser().resolve() if args.input_path is not None else None
    if source_path is not None and not source_path.exists():
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
