"""RAW edit and preview machinery for the Live Lab tab (split out of ``ui/live_lab_tab.py``)."""
from __future__ import annotations

import copy
import json
import os
import time
import tempfile
from dataclasses import dataclass, field, replace
from datetime import datetime
from uuid import uuid4
from pathlib import Path

import numpy as np
from PIL import Image

from PySide6.QtCore import (
    QObject,
    Qt,
    QThread,
    QTimer,
    QSignalBlocker,
    QEvent,
    QPointF,
    QRectF,
    Signal,
    Slot,
)
from PySide6.QtGui import (
    QColor,
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
    QComboBox,
    QLineEdit,
    QPushButton,
    QSizePolicy,
    QPlainTextEdit,
    QStackedWidget,
    QTextEdit,
    QWidget,
)

from config import SETTING_RAW_PROCESSING_BRIGHT_CUTOFF, SETTING_RAW_PROCESSING_DARK_CUTOFF
from database.database_tags import DatabaseTerms
from database.models import ImageDB, SettingsDB
from database.schema import get_images_dir
from utils.image_companion_grouping import companion_group_key
from utils.image_metadata_merge import merge_image_lab_metadata
from utils.image_processing_pipeline import (
    PostDecodeTransferCurve,
    apply_post_decode_processing_fast,
    compute_auto_level_bounds_from_luminance,
    compute_post_decode_transfer_curve,
    compute_pre_levels_working_rgb,
    prepare_post_decode_fast_inputs,
)
from utils.local_image_ingest import LocalIngestResult, RawRenderingUnavailableError, prepare_local_ingest_image
from utils.raw_detection import is_raw_image_path
from utils.raw_render import (
    RawRenderSettings,
    apply_auto_level_bounds_to_settings,
    compute_auto_level_adjusted_settings_from_source,
    build_raw_processing_metadata,
    RAW_PREVIEW_MAX_DIM,
    render_raw_image,
    render_raw_preview,
    render_raw_preview_proxy_rgb,
    save_raw_preview_jpeg,
)
from utils.raw_white_balance import estimate_white_balance_from_background
from utils.thumbnail_generator import generate_all_sizes, get_thumbnail_path

from .styles import STAGING_BORDER
from .zoomable_image_widget import ZoomableImageLabel

_RAW_DEBUG_TIMING = str(os.environ.get("SPORELY_DEBUG_RAW_TIMING") or "").strip().lower() in {"1", "true", "yes", "on"}
PENDING_RAW_COMMIT_MAX_WORKERS = 1
# Keep RAW commit concurrency at one worker until output path reservation is
# atomic, 20 MP RAW memory use is profiled, and packaged rawpy/LibRaw
# thread-safety is verified instead of assumed.


def _raw_timing_log(label: str, start: float | None, *, detail: str | None = None) -> None:
    if not _RAW_DEBUG_TIMING or start is None:
        return
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    message = f"[raw-timing] {label}: {elapsed_ms:.1f} ms"
    if detail:
        message = f"{message} | {detail}"
    print(message, flush=True)


@dataclass(slots=True)
class PendingRawCapture:
    """Session-local RAW capture waiting for review and commit."""

    source_path: Path
    companion_jpeg_path: Path | None
    lab_metadata: dict[str, object]
    raw_settings: RawRenderSettings
    preview_path: Path | None = None
    preview_rgb: np.ndarray | None = None
    wb_sample_base_preview_path: Path | None = None
    wb_sample_base_pixmap: QPixmap | None = None
    status: str = "pending"
    group_key: str | None = None
    observation_id: int | None = None
    created_at: datetime | None = None
    # Settings the current ``preview_rgb`` / on-disk preview JPEG were
    # rendered with. Used to detect staleness when multi-select applied new
    # settings to this capture without re-rendering it.
    rendered_settings: RawRenderSettings | None = None
    # Manual Light/Dark equivalents of the exact auto-level bounds used for
    # this capture's preview. Keeping these per capture prevents one image's
    # slider positions leaking into another when navigating the review queue.
    auto_level_settings: RawRenderSettings | None = None


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
    preview_rgb: np.ndarray | None = None
    wb_sample_base_preview_path: Path | None = None
    wb_sample_base_pixmap: QPixmap | None = None
    dirty: bool = False


_RAW_PREVIEW_WB_CACHE_MAX = 4


@dataclass(slots=True)
class _RawPreviewCacheEntry:
    """Per-source proxy cache shared by every preview path.

    The half-size LibRaw decode is by far the dominant cost; everything
    derived from it (resized preview, source auto-level bounds, WB-applied
    RGB + luminance) is kept here so that slider events don't redo any of
    it.
    """

    raw_rgb: np.ndarray
    preview_rgb: np.ndarray | None = None
    source_auto_bounds: tuple[float | None, float | None] | None = None
    wb_processed: dict[
        tuple[float, float, float] | None, tuple[np.ndarray, np.ndarray]
    ] = field(default_factory=dict)


def _pending_raw_source_signature(source_path: str | Path) -> tuple[str, int, int]:
    path = Path(source_path)
    try:
        stat = path.stat()
    except Exception:
        return (str(path), -1, -1)
    try:
        resolved = str(path.resolve())
    except Exception:
        resolved = str(path)
    return (resolved, int(stat.st_mtime_ns), int(stat.st_size))


def _pending_raw_settings_signature(settings: RawRenderSettings | dict | None) -> str:
    resolved = RawRenderSettings.from_dict(settings)
    return json.dumps(resolved.to_dict(), sort_keys=True, separators=(",", ":"))


def _pending_raw_preview_decode_mode(settings: RawRenderSettings) -> str:
    sample_base_mode = str(settings.wb_sample_base_mode or "").strip().lower() or None
    if sample_base_mode not in {"camera", "auto"}:
        sample_base_mode = "auto" if settings.white_balance_mode == "auto" else "camera"
    return sample_base_mode if sample_base_mode in {"camera", "auto"} else "camera"


@dataclass(slots=True)
class _PendingRawPreviewJobRequest:
    job_id: int
    kind: str
    capture: PendingRawCapture
    source_path: Path
    raw_settings: RawRenderSettings
    source_signature: tuple[str, int, int]
    settings_signature: str


@dataclass(slots=True)
class _PendingRawPreviewJobResult:
    job_id: int
    kind: str
    source_signature: tuple[str, int, int]
    settings_signature: str
    raw_rgb: np.ndarray | None = None
    preview_rgb: np.ndarray | None = None
    processed_settings: RawRenderSettings | None = None
    auto_level_settings: RawRenderSettings | None = None
    curve: PostDecodeTransferCurve | None = None
    preview_path: str | None = None
    error: str | None = None


def _pending_raw_preview_cache_has_entry(
    cache: dict[tuple[str, int, int, str], object] | None,
    cache_key: tuple[str, int, int, str] | None,
) -> bool:
    if cache is None or cache_key is None:
        return False
    cached = cache.get(cache_key)
    return isinstance(cached, (_RawPreviewCacheEntry, np.ndarray))


def _build_pending_raw_preview_job_result(
    request: _PendingRawPreviewJobRequest,
) -> _PendingRawPreviewJobResult:
    resolved_settings = RawRenderSettings.from_dict(request.raw_settings)
    raw_rgb: np.ndarray | None = None
    preview_rgb: np.ndarray | None = None
    processed_settings = resolved_settings
    auto_level_settings = resolved_settings
    curve: PostDecodeTransferCurve | None = None
    preview_path: str | None = None
    error: str | None = None
    try:
        raw_rgb = np.asarray(
            render_raw_preview_proxy_rgb(request.source_path, settings=resolved_settings),
            dtype=np.float32,
        )
        preview_proxy_rgb = np.asarray(_resize_preview_rgb(raw_rgb, RAW_PREVIEW_MAX_DIM), dtype=np.float32)
        preview_rgb = preview_proxy_rgb
        if (
            processed_settings.white_balance_mode in {"background", "custom"}
            and processed_settings.wb_multipliers is None
            and processed_settings.wb_selection is not None
        ):
            background_wb = estimate_white_balance_from_background(
                preview_proxy_rgb,
                rect=processed_settings.wb_selection,
            )
            processed_settings = replace(
                processed_settings,
                white_balance_mode="custom",
                wb_multipliers=(
                    float(background_wb[0]),
                    float(background_wb[1]),
                    float(background_wb[2]),
                ),
                wb_multiplier_space="post_decode_rgb",
                wb_sample_base_mode=_pending_raw_preview_decode_mode(resolved_settings),
            )
        if (
            processed_settings.auto_levels
            and processed_settings.auto_levels_method == "b"
            and processed_settings.auto_black_level is None
        ):
            analysis_luminance = raw_rgb @ np.asarray(
                [0.2126, 0.7152, 0.0722], dtype=np.float32
            )
            black_level, white_level = compute_auto_level_bounds_from_luminance(
                analysis_luminance,
                processed_settings.black_percentile,
                processed_settings.white_percentile,
            )
            processed_settings = apply_auto_level_bounds_to_settings(
                processed_settings, black_level, white_level
            )
        prepared = prepare_post_decode_fast_inputs(preview_proxy_rgb, processed_settings)
        fast_result = apply_post_decode_processing_fast(
            preview_proxy_rgb,
            processed_settings,
            prepared_inputs=prepared,
        )
        preview_rgb = np.ascontiguousarray(np.asarray(fast_result.rgb, dtype=np.float32))
        curve = fast_result.curve
        # Derive the manual slider equivalents from the exact bounds used by
        # the render above. A second analysis (especially one done before
        # custom WB or with different percentile cutoffs) makes the first
        # manual slider movement jump visibly.
        auto_level_settings = apply_auto_level_bounds_to_settings(
            resolved_settings,
            fast_result.debug.black_level,
            fast_result.debug.white_level,
        )
        if request.capture.preview_path is not None:
            preview_path = str(request.capture.preview_path)
            save_raw_preview_jpeg(
                preview_rgb,
                request.capture.preview_path,
                request.source_path,
            )
    except RawRenderingUnavailableError as exc:
        error = str(exc)
    except Exception as exc:
        error = str(exc)
    return _PendingRawPreviewJobResult(
        job_id=request.job_id,
        kind=request.kind,
        source_signature=request.source_signature,
        settings_signature=request.settings_signature,
        raw_rgb=raw_rgb,
        preview_rgb=preview_rgb,
        processed_settings=processed_settings,
        auto_level_settings=auto_level_settings,
        curve=curve,
        preview_path=preview_path,
        error=error,
    )


class _PendingRawPreviewWorker(QObject):
    finished = Signal(object)

    def __init__(self, request: _PendingRawPreviewJobRequest) -> None:
        super().__init__()
        self.request = request

    @Slot()
    def run(self) -> None:
        self.finished.emit(_build_pending_raw_preview_job_result(self.request))


@dataclass(slots=True)
class _PendingRawCommitJobRequest:
    job_id: int
    kind: str
    capture: PendingRawCapture
    source_path: Path
    raw_settings: RawRenderSettings
    lab_metadata: dict[str, object]
    observation_id: int
    source_signature: tuple[str, int, int]


@dataclass(slots=True)
class _PendingRawCommitJobResult:
    job_id: int
    kind: str
    source_signature: tuple[str, int, int]
    source_path: Path
    pending_capture_id: int
    pending_capture_source_path: str
    prepared_ingest: LocalIngestResult | None = None
    error: str | None = None


def _build_pending_raw_commit_job_result(
    request: _PendingRawCommitJobRequest,
) -> _PendingRawCommitJobResult:
    start = time.perf_counter() if _RAW_DEBUG_TIMING else None
    resolved_settings = RawRenderSettings.from_dict(request.raw_settings)
    prepared_ingest: LocalIngestResult | None = None
    error: str | None = None
    try:
        prepared_ingest = prepare_local_ingest_image(
            request.source_path,
            raw_settings=resolved_settings,
            lab_metadata=copy.deepcopy(request.lab_metadata),
            output_dir=get_images_dir() / "imports",
        )
    except RawRenderingUnavailableError as exc:
        error = str(exc)
    except Exception as exc:
        error = str(exc)
    _raw_timing_log("pending RAW commit prepare", start, detail=request.source_path.name)
    return _PendingRawCommitJobResult(
        job_id=request.job_id,
        kind=request.kind,
        source_signature=request.source_signature,
        source_path=request.source_path,
        pending_capture_id=id(request.capture),
        pending_capture_source_path=str(request.capture.source_path),
        prepared_ingest=prepared_ingest,
        error=error,
    )


class _PendingRawCommitWorker(QObject):
    finished = Signal(object)

    def __init__(self, request: _PendingRawCommitJobRequest) -> None:
        super().__init__()
        self.request = request

    @Slot()
    def run(self) -> None:
        self.finished.emit(_build_pending_raw_commit_job_result(self.request))


def _resize_preview_rgb(rgb: np.ndarray, max_dim: int = RAW_PREVIEW_MAX_DIM) -> np.ndarray:
    arr = np.asarray(rgb, dtype=np.float32)
    if arr.size == 0:
        return arr.copy()
    if arr.ndim != 3 or arr.shape[-1] < 3:
        return arr.copy()

    height, width = arr.shape[:2]
    limit = max(1, int(max_dim))
    long_edge = max(int(width), int(height))
    image = np.clip(arr[..., :3], 0.0, 1.0)
    if long_edge <= limit:
        return image.copy()

    scale = float(limit) / float(long_edge)
    new_width = max(1, int(round(float(width) * scale)))
    new_height = max(1, int(round(float(height) * scale)))
    rgb8 = np.rint(image * 255.0).astype(np.uint8)
    resample = getattr(getattr(Image, "Resampling", Image), "LANCZOS", Image.LANCZOS)
    resized = Image.fromarray(rgb8, mode="RGB").resize((new_width, new_height), resample)
    return np.asarray(resized, dtype=np.float32) / np.float32(255.0)


_TONE_CURVE_PREVIEW_ANALYSIS_MAX_DIM = 192
_TONE_CURVE_PREVIEW_HISTOGRAM_BINS = 96


class RawCurvePreviewWidget(QWidget):
    """Compact preview of the active RAW transfer curve and image histogram."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._curve = None
        self._histogram: np.ndarray | None = None
        self._theme_colors: dict[str, QColor] = {}
        self.setFixedSize(190, 190)
        self.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.setToolTip("Effective transfer curve")
        self._refresh_theme_colors()

    @property
    def current_curve(self):
        return self._curve

    def current_histogram(self) -> np.ndarray | None:
        if self._histogram is None:
            return None
        return self._histogram.copy()

    def set_curve(self, curve, histogram: np.ndarray | None = None) -> None:
        self._curve = curve
        self._histogram = None if histogram is None else np.asarray(histogram, dtype=np.float32).copy()
        self.update()

    def _refresh_theme_colors(self) -> None:
        palette = self.palette()
        window_color = QColor(palette.window().color())
        plot_color = QColor(palette.base().color())
        border_color = QColor(palette.mid().color())
        text_color = QColor(palette.text().color())
        accent_color = QColor("#16a085")
        if palette.highlight().color().isValid():
            highlight = QColor(palette.highlight().color())
            if highlight.lightness() > 200:
                accent_color = highlight.darker(140)
            elif highlight.lightness() < 80:
                accent_color = highlight.lighter(120)
            else:
                accent_color = highlight

        dark_theme = window_color.lightness() < 128
        histogram_color = QColor(border_color)
        histogram_color.setAlpha(84 if dark_theme else 96)
        histogram_clipped_color = QColor(231, 76, 60)
        histogram_clipped_color.setAlpha(150 if dark_theme else 170)
        identity_color = QColor(border_color)
        identity_color.setAlpha(170 if dark_theme else 150)
        curve_color = QColor(accent_color)
        curve_color.setAlpha(240)
        node_fill = QColor(plot_color)
        node_fill.setAlpha(255)
        node_dark = QColor(text_color)
        node_dark.setAlpha(235)
        node_light = QColor(curve_color)
        node_light.setAlpha(255)

        self._theme_colors = {
            "window": window_color,
            "plot": plot_color,
            "border": border_color,
            "histogram": histogram_color,
            "histogram_clipped": histogram_clipped_color,
            "identity": identity_color,
            "curve": curve_color,
            "node_fill": node_fill,
            "node_dark": node_dark,
            "node_light": node_light,
        }

    def changeEvent(self, event) -> None:  # type: ignore[override]
        event_type = event.type()
        if event_type in {
            getattr(QEvent, "PaletteChange", None),
            getattr(QEvent, "ApplicationPaletteChange", None),
            getattr(QEvent, "StyleChange", None),
            getattr(QEvent, "ThemeChange", None),
        }:
            self._refresh_theme_colors()
            self.update()
        super().changeEvent(event)

    @staticmethod
    def _map_point(plot: QRectF, x: float, y: float) -> QPointF:
        px = plot.left() + float(np.clip(float(x), 0.0, 1.0)) * plot.width()
        py = plot.bottom() - float(np.clip(float(y), 0.0, 1.0)) * plot.height()
        return QPointF(px, py)

    @staticmethod
    def _draw_node(painter: QPainter, center: QPointF, *, fill: QColor, stroke: QColor) -> None:
        painter.setPen(QPen(stroke, 1.0))
        painter.setBrush(fill)
        painter.drawEllipse(center, 3.8, 3.8)

    def paintEvent(self, event) -> None:  # type: ignore[override]
        del event
        if not self._theme_colors:
            self._refresh_theme_colors()

        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        painter.fillRect(self.rect(), self._theme_colors["window"])

        outer = self.rect().adjusted(1, 1, -1, -1)
        painter.setPen(QPen(self._theme_colors["border"], 1))
        painter.setBrush(Qt.NoBrush)
        painter.drawRoundedRect(outer, 8, 8)

        inset = 12
        plot_side = max(1, min(int(outer.width() - inset * 2), int(outer.height() - inset * 2)))
        plot_left = float(outer.center().x()) - float(plot_side) / 2.0
        plot_top = float(outer.center().y()) - float(plot_side) / 2.0
        plot = QRectF(plot_left, plot_top, float(plot_side), float(plot_side))

        painter.setPen(QPen(self._theme_colors["border"], 1))
        painter.setBrush(self._theme_colors["plot"])
        painter.drawRoundedRect(plot, 6, 6)

        curve = self._curve
        dark_bound = 0.0
        light_bound = 1.0
        if curve is not None:
            inputs = np.asarray(curve.input_values, dtype=np.float64)
            outputs = np.asarray(curve.final_output, dtype=np.float64)
            if inputs.size and outputs.size and inputs.size == outputs.size:
                eps = 1e-4
                dark_mask = outputs <= eps
                light_mask = outputs >= 1.0 - eps
                if dark_mask.any():
                    dark_bound = float(inputs[dark_mask].max())
                if light_mask.any():
                    light_bound = float(inputs[light_mask].min())
        if light_bound < dark_bound:
            dark_bound, light_bound = light_bound, dark_bound

        histogram = self._histogram
        if histogram is not None and histogram.size:
            hist = np.asarray(histogram, dtype=np.float32)
            peak = float(np.max(hist)) if hist.size else 0.0
            if np.isfinite(peak) and peak > 0.0:
                hist = np.clip(hist / peak, 0.0, 1.0)
                step = plot.width() / float(hist.size)
                painter.setPen(Qt.NoPen)
                normal_color = self._theme_colors["histogram"]
                clipped_color = self._theme_colors.get("histogram_clipped", normal_color)
                for index, value in enumerate(hist):
                    if value <= 0.0:
                        continue
                    bin_center = (float(index) + 0.5) / float(hist.size)
                    is_clipped = bin_center < dark_bound or bin_center > light_bound
                    x_left = plot.left() + float(index) * step
                    y_top = plot.bottom() - float(value) * plot.height()
                    bar_rect = QRectF(x_left, y_top, step, plot.bottom() - y_top)
                    painter.setBrush(clipped_color if is_clipped else normal_color)
                    painter.drawRect(bar_rect)

        painter.setPen(QPen(self._theme_colors["identity"], 1.1, Qt.DashLine))
        painter.setBrush(Qt.NoBrush)
        painter.drawLine(self._map_point(plot, 0.0, 0.0), self._map_point(plot, 1.0, 1.0))

        if curve is None:
            return

        path = QPainterPath()
        first_point = True
        for x_value, y_value in zip(curve.input_values, curve.final_output, strict=False):
            point = self._map_point(plot, float(x_value), float(y_value))
            if first_point:
                path.moveTo(point)
                first_point = False
            else:
                path.lineTo(point)
        painter.setPen(QPen(self._theme_colors["curve"], 2.2, Qt.SolidLine, Qt.RoundCap, Qt.RoundJoin))
        painter.setBrush(Qt.NoBrush)
        painter.drawPath(path)
        if dark_bound > 0.0:
            self._draw_node(
                painter,
                self._map_point(plot, dark_bound, 0.0),
                fill=self._theme_colors["node_fill"],
                stroke=self._theme_colors["node_dark"],
            )
        if light_bound < 1.0:
            self._draw_node(
                painter,
                self._map_point(plot, light_bound, 1.0),
                fill=self._theme_colors["node_light"],
                stroke=self._theme_colors["node_dark"],
            )


def _compute_combined_rgb_histogram(rgb: np.ndarray, *, bins: int = _TONE_CURVE_PREVIEW_HISTOGRAM_BINS) -> np.ndarray:
    arr = np.asarray(rgb, dtype=np.float32)
    bin_count = max(1, int(bins))
    if arr.size == 0:
        return np.zeros(bin_count, dtype=np.float32)

    values = np.clip(arr[..., :3], 0.0, 1.0).reshape(-1)
    hist, _edges = np.histogram(values, bins=bin_count, range=(0.0, 1.0))
    hist = hist.astype(np.float32, copy=False)
    if hist.size == 0:
        return np.zeros(bin_count, dtype=np.float32)

    peak = float(hist.max())
    if peak <= 0.0 or not np.isfinite(peak):
        return np.zeros(bin_count, dtype=np.float32)
    hist = np.sqrt(hist / np.float32(peak)).astype(np.float32, copy=False)
    return hist


def _live_lab_tab_class():
    """Return ``LiveLabTab`` at call time (``ui.live_lab_tab`` imports this module)."""
    from .live_lab_tab import LiveLabTab

    return LiveLabTab


class LiveLabRawMixin:
    """RAW review and edit for ``LiveLabTab``: pending captures, previews, commits, white balance.

    Mixed into ``LiveLabTab``; the methods use the tab's widgets and state through ``self``.
    """

    SETTING_RAW_PROCESSING_PRESET_PREFIX = "live_lab_raw_processing_preset"
    SETTING_RAW_CAPTURE_MODE = "live_lab_raw_capture_mode"
    RAW_CAPTURE_MODE_AUTO_SAVE = "auto_save"
    RAW_CAPTURE_MODE_REVIEW = "review"
    RAW_BACKGROUND_WB_SAMPLE_SIZE = 10

    def _raw_processing_preset_context(self) -> dict[str, str | None]:
        """Return the context key that future RAW presets will use."""
        objective_value = self._selected_combo_value(self.objective_combo)
        objective_key = str(objective_value).strip() if objective_value is not None else ""
        objective_key = objective_key or None
        objective_label = str(self.objective_combo.currentText() or "").strip() or None
        return {
            "capture_source": "live_lab",
            "instrument": "microscope",
            "objective_name": objective_key,
            "objective_label": objective_label,
            "contrast": DatabaseTerms.canonicalize("contrast", self._selected_combo_value(self.contrast_combo)),
            "mount_medium": DatabaseTerms.canonicalize("mount", self._selected_combo_value(self.mount_combo)),
            "stain": DatabaseTerms.canonicalize("stain", self._selected_combo_value(self.stain_combo)),
            "sample_type": DatabaseTerms.canonicalize("sample", self._selected_combo_value(self.sample_combo)),
        }

    def _raw_processing_settings_key(self, context: dict[str, str | None] | None = None) -> str:
        context = dict(context or self._raw_processing_preset_context())
        prefix = getattr(self, "SETTING_RAW_PROCESSING_PRESET_PREFIX", None) or LiveLabRawMixin.SETTING_RAW_PROCESSING_PRESET_PREFIX
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
            LiveLabRawMixin._sync_raw_processing_controls_from_settings(self, settings)
        return settings

    def _save_raw_processing_settings_for_current_context(self, settings: RawRenderSettings | None = None) -> None:
        resolved_settings = RawRenderSettings.from_dict(settings or self._raw_settings_from_controls())
        SettingsDB.set_setting(
            self._raw_processing_settings_key(),
            json.dumps(resolved_settings.to_dict(), sort_keys=True),
        )
        self._raw_render_settings = resolved_settings

    @staticmethod
    def _raw_processing_setting_float(key: str, default: float, minimum: float, maximum: float) -> float:
        try:
            value = float(SettingsDB.get_setting(key, default))
        except Exception:
            value = float(default)
        if not np.isfinite(value):
            value = float(default)
        return float(np.clip(value, float(minimum), float(maximum)))

    @staticmethod
    def _raw_processing_setting_bool(key: str, default: bool) -> bool:
        value = SettingsDB.get_setting(key, default)
        if isinstance(value, bool):
            return value
        if value is None:
            return bool(default)
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if not text:
            return bool(default)
        return text in {"1", "true", "yes", "on", "enabled"}

    def _raw_processing_preferences(self) -> dict[str, float | bool]:
        return {
            "dark_cutoff": self._raw_processing_setting_float(
                SETTING_RAW_PROCESSING_DARK_CUTOFF,
                0.0,
                0.0,
                0.02,
            ),
            "bright_cutoff": self._raw_processing_setting_float(
                SETTING_RAW_PROCESSING_BRIGHT_CUTOFF,
                0.0,
                0.0,
                0.02,
            ),
        }

    def _push_raw_processing_cutoffs_to_controls(self) -> None:
        """Forward Preferences dark/bright cutoffs into the shared widget."""
        controls = getattr(self, "raw_controls", None)
        if controls is None:
            return
        prefs = self._raw_processing_preferences()
        controls.set_auto_level_cutoffs(
            float(prefs.get("dark_cutoff", 0.0)),
            float(prefs.get("bright_cutoff", 0.0)),
        )

    def refresh_raw_processing_preferences(self) -> None:
        """Public entry point for the Settings hub to invoke after a save.

        Preferences can change while the Live Lab panel remains open (the
        Settings hub lives in the same window), so we re-push the cutoffs
        whenever the user saves Preferences. Safe to call before controls
        exist — no-ops if the widget hasn't been built yet.
        """
        self._push_raw_processing_cutoffs_to_controls()

    def _raw_settings_from_controls(self, *, update_session_settings: bool = True) -> RawRenderSettings:
        controls = getattr(self, "raw_controls", None)
        if controls is None:
            settings = RawRenderSettings.from_dict(getattr(self, "_raw_render_settings", None))
        else:
            settings = controls.settings()
        if update_session_settings:
            self._raw_render_settings = settings
        return settings

    def _raw_auto_level_settings_for_source(
        self,
        source_path: str | Path,
        settings: RawRenderSettings | None,
    ) -> RawRenderSettings:
        resolved = RawRenderSettings.from_dict(settings)
        if resolved.auto_black_level is not None and resolved.auto_white_level is not None:
            return resolved
        prefs = self._raw_processing_preferences()
        dark_cutoff = float(prefs.get("dark_cutoff", 0.0))
        bright_cutoff = float(prefs.get("bright_cutoff", 0.0))
        clipping_enabled = bool(resolved.auto_levels_clipping)
        black_percentile = max(0.0, min(1.0, dark_cutoff)) if clipping_enabled else 0.0
        white_percentile = max(0.0, min(1.0, 1.0 - bright_cutoff)) if clipping_enabled else 1.0
        try:
            entry = LiveLabRawMixin._raw_preview_cache_entry(self, source_path, resolved)
            if resolved.auto_levels_method == "b":
                luminance = entry.raw_rgb @ np.asarray(
                    [0.2126, 0.7152, 0.0722], dtype=np.float32
                )
            else:
                analysis_rgb = LiveLabRawMixin._raw_preview_resized_for_entry(entry)
                _, luminance = prepare_post_decode_fast_inputs(analysis_rgb, resolved)
            black_level, white_level = compute_auto_level_bounds_from_luminance(
                luminance, black_percentile, white_percentile
            )
        except Exception:
            return compute_auto_level_adjusted_settings_from_source(
                source_path,
                settings=resolved,
                black_percentile=black_percentile,
                white_percentile=white_percentile,
            )
        if black_level is None or white_level is None:
            return resolved
        return apply_auto_level_bounds_to_settings(resolved, black_level, white_level)

    def _sync_raw_processing_controls_from_settings(
        self,
        settings: RawRenderSettings | None = None,
        *,
        update_session_settings: bool = True,
        auto_level_settings: RawRenderSettings | None = None,
    ) -> None:
        settings = RawRenderSettings.from_dict(settings or getattr(self, "_raw_render_settings", None))
        if update_session_settings:
            self._raw_render_settings = settings
        controls = getattr(self, "raw_controls", None)
        if controls is not None:
            controls.set_auto_level_settings(auto_level_settings)
            controls.set_settings(settings)
            if getattr(self, "_sidebar_binds_to_selection", False):
                try:
                    selected_keys = _live_lab_tab_class()._session_gallery_selected_keys(self)
                except Exception:
                    selected_keys = []
                if len(selected_keys) > 1:
                    LiveLabRawMixin._apply_raw_multi_state_to_controls(self)
                else:
                    controls.set_multi_selection_mode(False)
            else:
                controls.set_multi_selection_mode(False)
        self._set_raw_tone_controls_enabled(bool(settings.tone_curve_enabled))
        self._refresh_raw_processing_context_ui()
        self._update_raw_processing_section_label(bool(getattr(self, "raw_processing_toggle_btn", None) and self.raw_processing_toggle_btn.isChecked()))

    def _set_raw_tone_controls_enabled(self, enabled: bool) -> None:
        controls = getattr(self, "raw_controls", None)
        if controls is not None:
            controls.set_tone_controls_enabled(bool(enabled))
        return

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

    @staticmethod
    def _format_raw_percent(value: float | int | None, *, decimals: int = 1) -> str:
        try:
            numeric = float(value)
        except Exception:
            numeric = 0.0
        if not np.isfinite(numeric):
            numeric = 0.0
        return f"{max(0.0, numeric) * 100.0:.{decimals}f}%"

    def _raw_settings_info_text(self, settings: RawRenderSettings | None) -> str:
        resolved = RawRenderSettings.from_dict(settings)
        dark_cutoff = LiveLabRawMixin._format_raw_percent(resolved.black_percentile, decimals=2)
        bright_cutoff = LiveLabRawMixin._format_raw_percent(1.0 - float(resolved.white_percentile), decimals=2)
        soft_tails_label = self.tr("on") if resolved.auto_levels_soft_tails else self.tr("off")
        curve_strength = LiveLabRawMixin._format_raw_percent(resolved.tone_curve_strength, decimals=0)
        curve_midpoint = LiveLabRawMixin._format_raw_percent(resolved.tone_curve_midpoint, decimals=0)
        return " \u00b7 ".join(
            [
                self._raw_white_balance_label(resolved),
                self.tr("Dark cutoff {value}").format(value=dark_cutoff),
                self.tr("Bright cutoff {value}").format(value=bright_cutoff),
                self.tr("Soft tails {state}").format(state=soft_tails_label),
                self.tr("Curve strength {value}").format(value=curve_strength),
                self.tr("Curve midpoint {value}").format(value=curve_midpoint),
            ]
        )

    def _raw_processing_summary_text(self) -> str:
        return self._raw_settings_summary_text(self._raw_settings_from_controls(update_session_settings=False))

    def _raw_processing_hint_text(self) -> str:
        return self.tr(
            "Pick Camera, Auto, or Custom WB, then click Pick to sample a neutral patch from the preview. "
            "Low midpoint values lift shadows."
        )

    def _pending_raw_review_hint_text(self) -> str:
        return self.tr(
            "Review mode: use ←/→ to move between RAW captures, Delete/Backspace/Cmd/Ctrl+D to remove the current image, "
            "and Enter to save current."
        )

    def _committed_raw_review_hint_text(self) -> str:
        return self.tr("Delete/Backspace/Cmd/Ctrl+D remove the selected saved image.")

    def _raw_edit_hint_text(self) -> str:
        return self.tr(
            "Editing mode: choose a WB mode, use Pick to sample the preview, then click Apply re-render when ready."
        )

    def _current_raw_hint_text(self) -> str:
        if getattr(self, "_raw_edit_session", None) is not None:
            return self._raw_edit_hint_text()
        if self._current_pending_raw_capture() is not None:
            return self._pending_raw_review_hint_text()
        selected_targets = self._selected_raw_processing_targets()
        if selected_targets:
            if len(selected_targets) > 1:
                return self.tr("Settings will be applied to selected images.")
            return self._raw_processing_hint_text()
        if self._selected_committed_image_id() is not None:
            return self._committed_raw_review_hint_text()
        raw_body = getattr(self, "raw_processing_body", None)
        if raw_body is not None and raw_body.isVisible():
            return self._raw_processing_hint_text()
        return self._default_hint_text

    def _apply_raw_multi_state_to_controls(self) -> None:
        """Populate the RAW processing controls with a mixed-state view of
        the currently selected pending captures / committed images.
        Sliders whose values disagree show dual ghost handles at min/max;
        agreeing fields display the shared value.
        """
        controls = getattr(self, "raw_controls", None)
        if controls is None:
            return
        try:
            targets = self._selected_raw_processing_targets()
        except Exception:
            targets = []
        settings_list = []
        for kind, _key, obj in targets:
            if kind == "pending" and isinstance(obj, PendingRawCapture):
                visible_settings = RawRenderSettings.from_dict(obj.raw_settings)
                if bool(visible_settings.auto_levels) and obj.auto_level_settings is not None:
                    visible_settings = RawRenderSettings.from_dict(obj.auto_level_settings)
                settings_list.append(visible_settings)
            elif kind == "image" and isinstance(obj, dict):
                helper = getattr(self, "_raw_editable_image_session", None)
                edit_session = helper(obj) if callable(helper) else None
                if edit_session is not None:
                    settings_list.append(RawRenderSettings.from_dict(edit_session.working_settings))
                else:
                    settings_list.append(RawRenderSettings.from_dict(obj.get("raw_settings")))
        if len(settings_list) > 1:
            cancel_wb_pick = getattr(self, "_cancel_active_raw_background_wb_selection", None)
            if callable(cancel_wb_pick):
                cancel_wb_pick()
        set_multi_selection_mode = getattr(controls, "set_multi_selection_mode", None)
        if callable(set_multi_selection_mode):
            set_multi_selection_mode(len(settings_list) > 1)
        if len(settings_list) > 1:
            controls.set_mixed_settings(settings_list)
        elif settings_list:
            controls.set_settings(settings_list[0])

    def _selected_raw_processing_targets(self) -> list[tuple[str, object, object]]:
        targets: list[tuple[str, object, object]] = []
        captures = getattr(self, "_pending_raw_captures", [])
        selected_keys = self._session_gallery_selected_keys()
        selected_key_set = {key for key in selected_keys if key is not None}

        current_pending = self._current_pending_raw_capture()
        if current_pending is not None:
            pending_key = self._pending_raw_gallery_key(current_pending)
            if pending_key is not None and pending_key not in selected_key_set:
                targets = [("pending", self._selected_pending_raw_index_value(), current_pending)]

        selected_image_id = self._selected_committed_image_id()
        if not targets and selected_image_id is not None and selected_image_id not in selected_key_set:
            image = ImageDB.get_image(selected_image_id)
            if image is not None and self._raw_editable_image_session(image) is not None:
                targets = [("image", selected_image_id, image)]

        if not targets:
            for key in selected_keys:
                pending_index = self._pending_raw_capture_index_for_key(key)
                if pending_index is not None and 0 <= pending_index < len(captures):
                    capture = captures[pending_index]
                    if isinstance(capture, PendingRawCapture):
                        targets.append(("pending", pending_index, capture))
                        continue
                try:
                    image_id = int(key)
                except Exception:
                    continue
                image = ImageDB.get_image(image_id)
                if image is None:
                    continue
                if self._raw_editable_image_session(image) is not None:
                    targets.append(("image", image_id, image))

        visible_target = self._visible_raw_processing_target()
        if visible_target is not None:
            if not targets:
                return [visible_target]
            if len(targets) == 1 and (targets[0][0], targets[0][1]) != (visible_target[0], visible_target[1]):
                return [visible_target]
        return targets

    def _visible_raw_processing_target(self) -> tuple[str, object, object] | None:
        label = getattr(self, "live_image_label", None)
        if label is None:
            return None

        visible_path = str(getattr(label, "_full_image_path", "") or "").strip()
        if not visible_path:
            return None

        try:
            visible_path_obj = Path(visible_path)
        except Exception:
            visible_path_obj = None

        current_pending = self._current_pending_raw_capture()
        if current_pending is not None:
            for candidate in (current_pending.preview_path, current_pending.source_path):
                candidate_text = str(candidate or "").strip()
                if not candidate_text:
                    continue
                try:
                    if visible_path_obj is not None and Path(candidate_text) == visible_path_obj:
                        pending_index = self._selected_pending_raw_index_value()
                        return ("pending", pending_index if pending_index >= 0 else 0, current_pending)
                except Exception:
                    if candidate_text == visible_path:
                        pending_index = self._selected_pending_raw_index_value()
                        return ("pending", pending_index if pending_index >= 0 else 0, current_pending)

        for image_id in reversed(list(getattr(self, "_session_image_ids", []) or [])):
            image = ImageDB.get_image(image_id)
            if image is None:
                continue
            image_path = str(image.get("filepath") or "").strip()
            if not image_path:
                continue
            try:
                matches_visible = visible_path_obj is not None and Path(image_path) == visible_path_obj
            except Exception:
                matches_visible = image_path == visible_path
            if not matches_visible:
                continue
            if self._raw_editable_image_session(image) is None:
                return None
            return ("image", image_id, image)

        return None

    def _raw_processing_controls_visible(self) -> bool:
        if self._raw_edit_session is not None:
            return True
        # Multi-select with any non-RAW item: hide the panel so the user
        # isn't led to think RAW sliders also affect the JPEG/16-bit-TIFF
        # rows in the selection.
        if self._selection_contains_non_raw_item():
            return False
        if self._current_pending_raw_capture() is not None:
            return True
        return bool(self._selected_raw_processing_targets())

    def _selection_contains_non_raw_item(self) -> bool:
        """True if any selected gallery item is not RAW-editable."""
        selected_keys = self._session_gallery_selected_keys()
        if len(selected_keys) <= 1:
            return False
        for key in selected_keys:
            if self._pending_raw_capture_index_for_key(key) is not None:
                continue  # pending captures are always RAW
            try:
                image_id = int(key)
            except Exception:
                continue
            image = ImageDB.get_image(image_id)
            if image is None:
                continue
            if self._raw_editable_image_session(image) is None:
                return True
        return False

    def _update_raw_processing_visibility(self) -> None:
        card = getattr(self, "raw_processing_card", None)
        body = getattr(self, "raw_processing_body", None)
        visible = self._raw_processing_controls_visible()
        if card is not None:
            card.setVisible(bool(visible))
        if body is None:
            return
        if not visible:
            body.setVisible(False)
            return
        toggle = getattr(self, "raw_processing_toggle_btn", None)
        body.setVisible(bool(toggle.isChecked()) if toggle is not None else False)

    @staticmethod
    def _preserve_raw_wb_fields(base_settings: RawRenderSettings, resolved_settings: RawRenderSettings) -> RawRenderSettings:
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

    @staticmethod
    def _merge_raw_form_into_target(
        target_settings,
        form_settings: RawRenderSettings,
        mixed_fields,
    ) -> RawRenderSettings:
        """Return a settings object where fields still in the form's mixed
        state are taken from ``target_settings`` (the capture's existing
        values) and everything else is taken from the form. This is how
        multi-select edits propagate the touched slider(s) without
        clobbering fields the user hasn't reconciled yet.
        """
        base = RawRenderSettings.from_dict(target_settings)
        merged = RawRenderSettings.from_dict(form_settings)
        if not mixed_fields:
            return merged
        overrides = {}
        for name in mixed_fields:
            if name == "white_balance_mode":
                overrides["white_balance_mode"] = base.white_balance_mode
                overrides["wb_multipliers"] = base.wb_multipliers
                overrides["wb_multiplier_space"] = base.wb_multiplier_space
                overrides["wb_selection"] = base.wb_selection
                overrides["wb_sample_point"] = base.wb_sample_point
                overrides["wb_selection_space"] = base.wb_selection_space
                continue
            if hasattr(base, name):
                overrides[name] = getattr(base, name)
        if overrides:
            merged = replace(merged, **overrides)
        return merged

    @staticmethod
    def _propagate_raw_wb_for_multi_select(
        resolved_settings: RawRenderSettings,
    ) -> RawRenderSettings:
        """Return ``resolved_settings`` with selection-coordinate WB metadata stripped.

        Multi-select propagates the active capture's WB to its siblings. The
        WB multipliers and mode are valid for any source (they live in
        post-decode RGB space), but the sample rectangle and click point are
        in the active capture's preview coordinates and don't translate.
        """
        return replace(
            resolved_settings,
            wb_selection=None,
            wb_sample_point=None,
            wb_sample_size=None,
            wb_selection_space=None,
        )

    @staticmethod
    def _replace_capture_wb(
        capture_settings: RawRenderSettings,
        source_settings: RawRenderSettings,
    ) -> RawRenderSettings:
        """Return ``capture_settings`` with WB fields replaced by ``source_settings``."""
        wb_mode = str(source_settings.white_balance_mode or "camera").strip().lower() or "camera"
        if wb_mode == "custom" and source_settings.wb_multipliers is not None:
            return replace(
                capture_settings,
                white_balance_mode="custom",
                wb_multipliers=source_settings.wb_multipliers,
                wb_multiplier_space=source_settings.wb_multiplier_space or "post_decode_rgb",
                wb_sample_base_mode=source_settings.wb_sample_base_mode,
                wb_selection=None,
                wb_sample_point=None,
                wb_sample_size=None,
                wb_selection_space=None,
            )
        return replace(
            capture_settings,
            white_balance_mode=wb_mode if wb_mode in {"camera", "auto"} else "camera",
            wb_multipliers=None,
            wb_multiplier_space=None,
            wb_sample_base_mode=None,
            wb_selection=None,
            wb_sample_point=None,
            wb_sample_size=None,
            wb_selection_space=None,
        )

    def _propagate_raw_wb_to_selected_pending(
        self,
        source_settings: RawRenderSettings,
        *,
        exclude: PendingRawCapture | None = None,
    ) -> None:
        """Copy ``source_settings``' WB onto every other selected pending capture."""
        targets_fn = getattr(self, "_selected_raw_processing_targets", None)
        if not callable(targets_fn):
            return
        try:
            targets = targets_fn()
        except Exception:
            return
        changed = False
        for kind, _key, target in targets:
            if kind != "pending" or not isinstance(target, PendingRawCapture):
                continue
            if target is exclude:
                continue
            merged = LiveLabRawMixin._replace_capture_wb(target.raw_settings, source_settings)
            if merged == target.raw_settings:
                continue
            target.raw_settings = merged
            target.auto_level_settings = None
            target.preview_rgb = None
            target.rendered_settings = None
            target.status = "pending"
            changed = True
        if not changed:
            return
        queue_helper = getattr(self, "_queue_background_pending_raw_renders", None)
        if callable(queue_helper):
            queue_helper()

    def _apply_raw_settings_to_pending_capture(
        self,
        capture: PendingRawCapture,
        settings: RawRenderSettings,
        *,
        render_preview: bool = True,
    ) -> bool:
        if capture is None:
            return False
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        resolved_settings = RawRenderSettings.from_dict(settings)
        base_settings = RawRenderSettings.from_dict(capture.raw_settings)
        if render_preview:
            updated_settings = self._preserve_raw_wb_fields(base_settings, resolved_settings)
        else:
            # Multi-select non-active: take the active's WB (with its
            # sample-coord metadata stripped) along with the other settings.
            updated_settings = self._propagate_raw_wb_for_multi_select(resolved_settings)
        capture.raw_settings = updated_settings
        capture.auto_level_settings = None
        if self._normalize_raw_capture_mode(self._selected_raw_capture_mode()) != self.RAW_CAPTURE_MODE_REVIEW:
            return True

        if not render_preview:
            # Multi-select: only the active capture is re-rendered on each
            # slider event. For the rest, just stash the new settings and
            # release the stale preview so the next activation forces a
            # re-render via _show_pending_raw_capture's staleness check.
            capture.preview_rgb = None
            capture.status = "pending"
            _raw_timing_log(
                "pending RAW capture settings applied",
                start,
                detail=f"{capture.source_path.name} deferred (multi-select)",
            )
            return True

        cache = getattr(self, "_pending_raw_preview_proxy_cache", None)
        cache_key = self._raw_preview_proxy_cache_key(capture.source_path, updated_settings)
        if _pending_raw_preview_cache_has_entry(cache, cache_key):
            try:
                preview_rgb, _processing_settings = self._raw_preview_rgb_for_source(capture.source_path, updated_settings)
                capture.preview_rgb = preview_rgb
                capture.rendered_settings = updated_settings
                preview_dir = self._pending_raw_preview_dir()
                preview_dir.mkdir(parents=True, exist_ok=True)
                preview_path = capture.preview_path
                if preview_path is None:
                    preview_path = preview_dir / f"{capture.source_path.stem}_{uuid4().hex}.jpg"
                capture.preview_path = Path(preview_path)
                capture.status = "pending"
                _raw_timing_log(
                    "pending RAW capture settings applied",
                    start,
                    detail=f"{capture.source_path.name} preview={preview_rgb.shape[1]}x{preview_rgb.shape[0]}",
                )
                return True
            except RawRenderingUnavailableError as exc:
                capture.status = "failed"
                self._show_status(
                    self.tr("RAW preview unavailable for {name}: {error}").format(
                        name=capture.source_path.name,
                        error=str(exc),
                    ),
                    tone="warning",
                    timeout_ms=6000,
                )
            except RuntimeError as exc:
                capture.status = "failed"
                self._show_status(
                    self.tr("Could not render RAW preview for {name}: {error}").format(
                        name=capture.source_path.name,
                        error=str(exc),
                    ),
                    tone="warning",
                    timeout_ms=6000,
                )
            return False

        request_builder = getattr(self, "_pending_raw_preview_request_for_capture", None)
        enqueue_request = getattr(self, "_enqueue_pending_raw_preview_job", None)
        if callable(request_builder) and callable(enqueue_request):
            request = request_builder(capture, kind="selected")
            if request is not None:
                enqueue_request(request)
                _raw_timing_log(
                    "pending RAW capture settings applied",
                    start,
                    detail=f"{capture.source_path.name} queued job={request.job_id}",
                )
                return True
        return False

    def _commit_raw_edit_session(self, session: RawEditSession) -> bool:
        if session is None:
            return False

        temp_dir = self._raw_edit_preview_dir()
        temp_dir.mkdir(parents=True, exist_ok=True)
        final_path = Path(session.current_derivative_path)
        final_path.parent.mkdir(parents=True, exist_ok=True)
        temp_suffix = final_path.suffix.lower() or ".jpg"
        temp_output = temp_dir / f"{session.source_raw_path.stem}_{session.image_id}_{uuid4().hex}{temp_suffix}"

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
            self._invalidate_thumbnail_caches_for_raw_image(int(session.image_id), final_path)
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

    def _refresh_raw_processing_context_ui(self) -> None:
        updater = getattr(self, "_update_raw_processing_visibility", None)
        if callable(updater):
            updater()
        updater = getattr(self, "_update_raw_processing_pick_button", None)
        if callable(updater):
            updater()
        updater = getattr(self, "_refresh_raw_curve_preview", None)
        if callable(updater):
            updater()
        hint_setter = getattr(self, "_set_hint", None)
        if callable(hint_setter):
            hint_setter(self._current_raw_hint_text())

    @staticmethod
    def _raw_preview_decode_mode(settings: RawRenderSettings) -> str:
        sample_base_mode = str(settings.wb_sample_base_mode or "").strip().lower() or None
        if sample_base_mode not in {"camera", "auto"}:
            sample_base_mode = "auto" if settings.white_balance_mode == "auto" else "camera"
        return sample_base_mode if sample_base_mode in {"camera", "auto"} else "camera"

    def _raw_preview_proxy_cache_key(
        self,
        source_path: str | Path,
        settings: RawRenderSettings | dict | None,
    ) -> tuple[str, int, int, str] | None:
        path = Path(source_path)
        try:
            stat = path.stat()
        except Exception:
            return None
        render_settings = RawRenderSettings.from_dict(settings)
        decode_mode = self._raw_preview_decode_mode(render_settings)
        try:
            source_key = str(path.resolve())
        except Exception:
            source_key = str(path)
        return (source_key, int(stat.st_mtime_ns), int(stat.st_size), decode_mode)

    def _raw_preview_proxy_for_source(
        self,
        source_path: str | Path,
        settings: RawRenderSettings | dict | None,
    ) -> np.ndarray:
        cache = getattr(self, "_pending_raw_preview_proxy_cache", None)
        if cache is None:
            cache = {}
            self._pending_raw_preview_proxy_cache = cache
        cache_key = self._raw_preview_proxy_cache_key(source_path, settings)
        if cache_key is not None:
            cached = cache.get(cache_key)
            if isinstance(cached, _RawPreviewCacheEntry):
                return cached.raw_rgb
            if isinstance(cached, np.ndarray):
                return cached
        proxy = render_raw_preview_proxy_rgb(source_path, settings=settings)
        result = np.asarray(proxy, dtype=np.float32)
        if cache_key is not None:
            cache[cache_key] = _RawPreviewCacheEntry(raw_rgb=result)
        return result

    def _evict_raw_preview_proxy_cache(self, source_path: str | Path) -> None:
        """Drop every cache entry tied to ``source_path`` (across decode modes)."""
        cache = getattr(self, "_pending_raw_preview_proxy_cache", None)
        if not isinstance(cache, dict) or not cache:
            return
        try:
            target = str(Path(source_path).resolve())
        except Exception:
            target = str(source_path)
        stale = [key for key in cache.keys() if isinstance(key, tuple) and key and key[0] == target]
        for key in stale:
            cache.pop(key, None)

    def _raw_preview_cache_entry(
        self,
        source_path: str | Path,
        settings: RawRenderSettings | dict | None,
    ) -> "_RawPreviewCacheEntry":
        cache = getattr(self, "_pending_raw_preview_proxy_cache", None)
        if cache is None:
            cache = {}
            self._pending_raw_preview_proxy_cache = cache
        cache_key = self._raw_preview_proxy_cache_key(source_path, settings)
        if cache_key is not None:
            cached = cache.get(cache_key)
            if isinstance(cached, _RawPreviewCacheEntry):
                return cached
            if isinstance(cached, np.ndarray):
                entry = _RawPreviewCacheEntry(raw_rgb=np.asarray(cached, dtype=np.float32))
                cache[cache_key] = entry
                return entry
        raw_rgb = self._raw_preview_proxy_for_source(source_path, settings)
        entry = _RawPreviewCacheEntry(raw_rgb=np.asarray(raw_rgb, dtype=np.float32))
        if cache_key is not None:
            cache[cache_key] = entry
        return entry

    @staticmethod
    def _raw_preview_resized_for_entry(entry: "_RawPreviewCacheEntry") -> np.ndarray:
        if entry.preview_rgb is None:
            resized = _resize_preview_rgb(entry.raw_rgb, RAW_PREVIEW_MAX_DIM)
            entry.preview_rgb = np.ascontiguousarray(resized.astype(np.float32, copy=False))
        return entry.preview_rgb

    @staticmethod
    def _raw_preview_source_auto_bounds(
        entry: "_RawPreviewCacheEntry",
    ) -> tuple[float | None, float | None]:
        if entry.source_auto_bounds is None:
            preview = LiveLabRawMixin._raw_preview_resized_for_entry(entry)
            luminance = preview @ np.asarray([0.2126, 0.7152, 0.0722], dtype=np.float32)
            entry.source_auto_bounds = compute_auto_level_bounds_from_luminance(
                luminance, 0.0, 1.0
            )
        return entry.source_auto_bounds

    @staticmethod
    def _raw_preview_wb_inputs(
        entry: "_RawPreviewCacheEntry",
        settings: RawRenderSettings,
    ) -> tuple[np.ndarray, np.ndarray]:
        preview = LiveLabRawMixin._raw_preview_resized_for_entry(entry)
        wb_key: tuple[float, float, float] | None = None
        wb_multipliers = settings.wb_multipliers
        wb_space = (settings.wb_multiplier_space or "").strip().lower() or None
        if wb_multipliers is not None and wb_space in {None, "post_decode_rgb"}:
            try:
                wb_key = (
                    float(wb_multipliers[0]),
                    float(wb_multipliers[1]),
                    float(wb_multipliers[2]),
                )
            except Exception:
                wb_key = None
        cached = entry.wb_processed.get(wb_key)
        if cached is not None:
            return cached
        prepared = prepare_post_decode_fast_inputs(preview, settings)
        if len(entry.wb_processed) >= _RAW_PREVIEW_WB_CACHE_MAX:
            oldest_key = next(iter(entry.wb_processed), None)
            if oldest_key is not None:
                entry.wb_processed.pop(oldest_key, None)
        entry.wb_processed[wb_key] = prepared
        return prepared

    def _raw_preview_rgb_for_source(
        self,
        source_path: str | Path,
        settings: RawRenderSettings | dict | None,
    ) -> tuple[np.ndarray, RawRenderSettings]:
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        source_name = Path(source_path).name
        resolved_settings = RawRenderSettings.from_dict(settings)
        proxy_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        entry = LiveLabRawMixin._raw_preview_cache_entry(self, source_path, resolved_settings)
        preview_rgb = LiveLabRawMixin._raw_preview_resized_for_entry(entry)
        _raw_timing_log(
            "RAW preview proxy decode/resize",
            proxy_start,
            detail=f"{source_name} -> {preview_rgb.shape[1]}x{preview_rgb.shape[0]}",
        )
        processing_settings = resolved_settings
        if (
            processing_settings.white_balance_mode in {"background", "custom"}
            and processing_settings.wb_multipliers is None
            and processing_settings.wb_selection is not None
        ):
            wb_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            background_wb = estimate_white_balance_from_background(
                preview_rgb,
                rect=processing_settings.wb_selection,
            )
            _raw_timing_log("background WB estimate", wb_start, detail=source_name)
            processing_settings = replace(
                processing_settings,
                white_balance_mode="custom",
                wb_multipliers=(
                    float(background_wb[0]),
                    float(background_wb[1]),
                    float(background_wb[2]),
                ),
                wb_multiplier_space="post_decode_rgb",
                wb_sample_base_mode=self._raw_preview_decode_mode(resolved_settings),
            )
        if (
            processing_settings.auto_levels
            and processing_settings.auto_levels_method == "b"
            and processing_settings.auto_black_level is None
        ):
            analysis_luminance = entry.raw_rgb @ np.asarray(
                [0.2126, 0.7152, 0.0722], dtype=np.float32
            )
            black_level, white_level = compute_auto_level_bounds_from_luminance(
                analysis_luminance,
                processing_settings.black_percentile,
                processing_settings.white_percentile,
            )
            processing_settings = apply_auto_level_bounds_to_settings(
                processing_settings, black_level, white_level
            )
        process_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        prepared = LiveLabRawMixin._raw_preview_wb_inputs(entry, processing_settings)
        fast_result = apply_post_decode_processing_fast(
            preview_rgb,
            processing_settings,
            prepared_inputs=prepared,
        )
        _raw_timing_log("RAW preview post-decode processing", process_start, detail=source_name)
        self._raw_preview_last_curve = fast_result.curve
        self._raw_preview_last_curve_signature = LiveLabRawMixin._raw_curve_signature_for_source(
            source_path, processing_settings
        )
        controls = getattr(self, "raw_controls", None)
        if controls is not None:
            controls.sync_from_live_bounds(
                fast_result.debug.black_level,
                fast_result.debug.white_level,
            )
        _raw_timing_log("RAW preview total", start, detail=source_name)
        return fast_result.rgb, processing_settings

    @staticmethod
    def _raw_curve_signature_for_source(
        source_path: str | Path,
        settings: RawRenderSettings,
    ) -> tuple[object, ...]:
        try:
            stat = Path(source_path).stat()
            mtime_ns = int(stat.st_mtime_ns)
            size = int(stat.st_size)
        except Exception:
            mtime_ns = 0
            size = 0
        return (str(source_path), mtime_ns, size, settings)

    def _present_raw_preview(
        self,
        *,
        pixmap: QPixmap,
        image_path: str,
        image: dict | None,
        title: str,
        meta: str,
        preview_scaled: bool = True,
        preserve_view: bool = False,
    ) -> None:
        if pixmap is None or pixmap.isNull():
            return
        if image is None:
            # A pending capture's preview: from now on the overlays describe it
            # (not earlier, while the previous image is still on screen).
            capture = self._current_pending_raw_capture()
            if capture is not None:
                self._viewer_subject = ("pending", capture)
                self._viewer_subject_metadata = None
        actual_preserve_view = bool(preserve_view)
        if not actual_preserve_view:
            preserve_view_checker = getattr(self, "_should_preserve_live_image_view", None)
            if callable(preserve_view_checker):
                try:
                    actual_preserve_view = bool(preserve_view_checker(image_path))
                except Exception:
                    actual_preserve_view = False
        self.live_image_label.set_image_sources(
            pixmap,
            image_path,
            preview_scaled,
            preserve_view=actual_preserve_view,
        )
        self.reset_view_btn.setEnabled(True)
        if image is not None:
            mpp_value = self._image_microns_per_pixel(image)
            self.live_image_label.set_microns_per_pixel(mpp_value or 0.0)
            if mpp_value:
                scale_bar_value, scale_bar_unit = self._viewer_scale_bar_config(image)
                scale_bar_microns = float(scale_bar_value) * 1000.0 if scale_bar_unit == "mm" else float(scale_bar_value)
                self.live_image_label.set_scale_bar(True, scale_bar_microns, unit=scale_bar_unit)
            else:
                self.live_image_label.set_scale_bar(False, 0.0)
        else:
            self.live_image_label.set_microns_per_pixel(0.0)
            self.live_image_label.set_scale_bar(False, 0.0)
        refresh_overlays = getattr(self, "_refresh_viewer_overlays", None)
        if callable(refresh_overlays):
            refresh_overlays()
        self.viewer_title_label.setText(title)
        self.viewer_meta_label.setText(meta)
        update_calibration_action_visibility = getattr(self, "_update_calibration_action_visibility", None)
        if callable(update_calibration_action_visibility):
            update_calibration_action_visibility()

    def _schedule_raw_edit_preview_save(self, session: RawEditSession | None = None) -> None:
        self._raw_edit_preview_save_target = session or getattr(self, "_raw_edit_session", None)
        timer = getattr(self, "_raw_edit_preview_save_timer", None)
        if isinstance(timer, QTimer):
            timer.stop()
            timer.start()

    def _schedule_pending_raw_preview_save(self, capture: PendingRawCapture | None = None) -> None:
        self._pending_raw_preview_save_target = capture or self._current_pending_raw_capture()
        timer = getattr(self, "_pending_raw_preview_save_timer", None)
        if isinstance(timer, QTimer):
            timer.stop()
            timer.start()

    def _persist_raw_edit_preview(self) -> None:
        session = getattr(self, "_raw_edit_preview_save_target", None) or getattr(self, "_raw_edit_session", None)
        if session is None:
            return
        preview_rgb = getattr(session, "preview_rgb", None)
        preview_path = getattr(session, "preview_path", None)
        if preview_rgb is None or not preview_path:
            return
        preview_path = Path(str(preview_path))
        try:
            save_raw_preview_jpeg(
                preview_rgb,
                preview_path,
                session.source_raw_path,
                source_capture_datetime=session.source_capture_datetime,
            )
        except Exception:
            pass
        finally:
            if getattr(self, "_raw_edit_preview_save_target", None) is session:
                self._raw_edit_preview_save_target = None

    def _persist_pending_raw_preview(self) -> None:
        capture = getattr(self, "_pending_raw_preview_save_target", None) or self._current_pending_raw_capture()
        if capture is None:
            return
        preview_rgb = getattr(capture, "preview_rgb", None)
        preview_path = getattr(capture, "preview_path", None)
        if preview_rgb is None or not preview_path:
            return
        preview_path = Path(str(preview_path))
        save_succeeded = False
        try:
            save_raw_preview_jpeg(
                preview_rgb,
                preview_path,
                capture.source_path,
            )
            capture.rendered_settings = capture.raw_settings
            save_succeeded = True
            gallery = getattr(self, "session_gallery", None)
            if gallery is not None:
                invalidate_gallery = getattr(gallery, "invalidate_pixmap_cache", None)
                if callable(invalidate_gallery):
                    invalidate_gallery(preview_path)
        except Exception:
            pass
        finally:
            if self._current_pending_raw_capture() is not capture:
                capture.preview_rgb = None
            if getattr(self, "_pending_raw_preview_save_target", None) is capture:
                self._pending_raw_preview_save_target = None
        if save_succeeded:
            self._refresh_session_gallery()
            # The slider has settled (300 ms idle); kick off background
            # rendering for any captures left stale by multi-select.
            queue_helper = getattr(self, "_queue_background_pending_raw_renders", None)
            if callable(queue_helper):
                queue_helper()

    def _prune_pending_raw_preview_buffers(
        self,
        keep_capture: PendingRawCapture | None = None,
        *,
        clear_proxy_cache: bool = False,
    ) -> None:
        captures = getattr(self, "_pending_raw_captures", []) or []
        for capture in captures:
            if capture is keep_capture:
                continue
            if getattr(capture, "preview_rgb", None) is not None:
                capture.preview_rgb = None
        if clear_proxy_cache:
            cache = getattr(self, "_pending_raw_preview_proxy_cache", None)
            if isinstance(cache, dict):
                cache.clear()

    def _pending_raw_preview_has_request_for_capture(
        self,
        capture: PendingRawCapture,
        *,
        kind: str | None = None,
    ) -> bool:
        active_request = getattr(self, "_pending_raw_preview_active_request", None)
        if isinstance(active_request, _PendingRawPreviewJobRequest):
            if active_request.capture is capture and (kind is None or active_request.kind == kind):
                return True
        queue = getattr(self, "_pending_raw_preview_job_queue", None)
        for request in list(queue or []):
            if not isinstance(request, _PendingRawPreviewJobRequest):
                continue
            if request.capture is not capture:
                continue
            if kind is not None and request.kind != kind:
                continue
            return True
        return False

    def _pending_raw_commit_has_request_for_capture(
        self,
        capture: PendingRawCapture,
        *,
        kind: str | None = None,
    ) -> bool:
        active_request = getattr(self, "_pending_raw_commit_active_request", None)
        if isinstance(active_request, _PendingRawCommitJobRequest):
            if active_request.capture is capture and (kind is None or active_request.kind == kind):
                return True
        queue = getattr(self, "_pending_raw_commit_job_queue", None)
        for request in list(queue or []):
            if not isinstance(request, _PendingRawCommitJobRequest):
                continue
            if request.capture is not capture:
                continue
            if kind is not None and request.kind != kind:
                continue
            return True
        return False

    def _pending_raw_commit_is_busy(self) -> bool:
        return bool(
            getattr(self, "_pending_raw_commit_active_request", None)
            or getattr(self, "_pending_raw_commit_job_queue", None)
        )

    def _pending_raw_commit_active_worker_count(self) -> int:
        return 1 if getattr(self, "_pending_raw_commit_worker_thread", None) is not None else 0

    def _pending_raw_commit_capture_is_busy(self, capture: PendingRawCapture | None) -> bool:
        if capture is None:
            return False
        return self._pending_raw_commit_has_request_for_capture(capture)

    @staticmethod
    def _cleanup_local_ingest_result(ingest: LocalIngestResult | None) -> None:
        if ingest is None:
            return
        for attr in ("working_path",):
            candidate = str(getattr(ingest, attr, "") or "").strip()
            if not candidate:
                continue
            try:
                Path(candidate).unlink(missing_ok=True)
            except Exception:
                pass

    @staticmethod
    def _cleanup_preview_result_path(result: "_PendingRawPreviewJobResult | None") -> None:
        preview_path = str(getattr(result, "preview_path", "") or "").strip()
        if not preview_path:
            return
        try:
            Path(preview_path).unlink(missing_ok=True)
        except Exception:
            pass

    @staticmethod
    def _pending_raw_capture_preview_is_current(capture: PendingRawCapture) -> bool:
        """Return True when the on-disk preview matches the capture's current settings."""
        rendered = getattr(capture, "rendered_settings", None)
        if not isinstance(rendered, RawRenderSettings):
            return False
        if rendered != capture.raw_settings:
            return False
        preview_path = getattr(capture, "preview_path", None)
        if not preview_path:
            return False
        try:
            return Path(str(preview_path)).exists()
        except Exception:
            return False

    def _queue_background_pending_raw_renders(self) -> None:
        """Queue every pending capture whose on-disk preview is missing or stale."""
        queue = getattr(self, "_pending_raw_background_render_queue", None)
        timer = getattr(self, "_pending_raw_background_render_timer", None)
        if queue is None or timer is None:
            return
        active = self._current_pending_raw_capture()
        queued_ids = {id(capture) for capture in queue}
        added = False
        for capture in list(getattr(self, "_pending_raw_captures", []) or []):
            if capture is active or id(capture) in queued_ids:
                continue
            if getattr(capture, "status", "pending") != "pending":
                continue
            if LiveLabRawMixin._pending_raw_capture_preview_is_current(capture):
                continue
            if self._pending_raw_preview_has_request_for_capture(capture, kind="selected"):
                continue
            queue.append(capture)
            added = True
        if (added or queue) and not timer.isActive():
            timer.start()

    def _process_pending_raw_background_render_queue(self) -> None:
        """Render and persist the preview JPEG for one queued pending capture."""
        queue = getattr(self, "_pending_raw_background_render_queue", None)
        timer = getattr(self, "_pending_raw_background_render_timer", None)
        if not queue:
            return
        active_captures = list(getattr(self, "_pending_raw_captures", []) or [])
        capture: PendingRawCapture | None = None
        while queue:
            candidate = queue.pop(0)
            if candidate not in active_captures:
                continue
            if getattr(candidate, "status", "pending") != "pending":
                continue
            if LiveLabRawMixin._pending_raw_capture_preview_is_current(candidate):
                continue
            if self._pending_raw_preview_has_request_for_capture(candidate, kind="selected"):
                continue
            capture = candidate
            break
        try:
            if capture is None:
                return
            request_builder = getattr(self, "_pending_raw_preview_request_for_capture", None)
            enqueue_request = getattr(self, "_enqueue_pending_raw_preview_job", None)
            if callable(request_builder) and callable(enqueue_request):
                request = request_builder(capture, kind="background")
                if request is not None:
                    enqueue_request(request)
                return
            return
        finally:
            if isinstance(timer, QTimer) and queue:
                timer.start()

    def _raw_curve_preview_source_path(self) -> Path | None:
        session = getattr(self, "_raw_edit_session", None)
        if session is not None:
            source_path = getattr(session, "source_raw_path", None)
            if source_path:
                path = Path(str(source_path))
                if path.exists():
                    return path

        capture = self._current_pending_raw_capture()
        if capture is not None:
            source_path = getattr(capture, "source_path", None)
            if source_path:
                path = Path(str(source_path))
                if path.exists():
                    return path

        live_image_label = getattr(self, "live_image_label", None)
        preview_path = getattr(live_image_label, "_full_image_path", None)
        if preview_path:
            path = Path(str(preview_path))
            if path.exists():
                return path
        return None

    @staticmethod
    def _raw_curve_preview_fallback_rgb() -> np.ndarray:
        ramp = np.linspace(0.0, 1.0, 256, dtype=np.float32)
        return np.repeat(ramp[:, None, None], 3, axis=2)

    def _raw_curve_preview_rgb_from_active_preview(
        self,
        source_path: Path,
    ) -> np.ndarray | None:
        def _same_path(left: str | Path | None, right: Path) -> bool:
            if left is None:
                return False
            try:
                return Path(str(left)).resolve() == right.resolve()
            except Exception:
                return str(left) == str(right)

        live_image_label = getattr(self, "live_image_label", None)
        current_view_path = getattr(live_image_label, "_full_image_path", None)

        session = getattr(self, "_raw_edit_session", None)
        if session is not None and _same_path(getattr(session, "source_raw_path", None), source_path):
            preview_rgb = getattr(session, "preview_rgb", None)
            if isinstance(preview_rgb, np.ndarray) and preview_rgb.size:
                return np.asarray(preview_rgb, dtype=np.float32)
            # If the viewer is already showing the temporary edit preview JPEG,
            # reuse that pixmap instead of falling back to the synthetic ramp.
            if _same_path(current_view_path, getattr(session, "preview_path", None)):
                rgb = self._pixmap_rgb_array(getattr(live_image_label, "original_pixmap", None))
                if isinstance(rgb, np.ndarray) and rgb.size:
                    return np.asarray(rgb, dtype=np.float32)

        capture = self._current_pending_raw_capture()
        if capture is not None and _same_path(getattr(capture, "source_path", None), source_path):
            preview_rgb = getattr(capture, "preview_rgb", None)
            if isinstance(preview_rgb, np.ndarray) and preview_rgb.size:
                return np.asarray(preview_rgb, dtype=np.float32)
            # Same for the pending-import preview: the active viewer often
            # shows the rendered JPEG before the in-memory RGB buffer exists.
            if _same_path(current_view_path, getattr(capture, "preview_path", None)):
                rgb = self._pixmap_rgb_array(getattr(live_image_label, "original_pixmap", None))
                if isinstance(rgb, np.ndarray) and rgb.size:
                    return np.asarray(rgb, dtype=np.float32)

        if _same_path(current_view_path, source_path):
            rgb = self._pixmap_rgb_array(getattr(live_image_label, "original_pixmap", None))
            if isinstance(rgb, np.ndarray) and rgb.size:
                return np.asarray(rgb, dtype=np.float32)

        return None

    def _raw_curve_preview_analysis_from_path(
        self,
        source_path: Path | None,
        settings: RawRenderSettings | None = None,
    ) -> tuple[np.ndarray, np.ndarray] | None:
        if source_path is None:
            return None
        try:
            stat = source_path.stat()
        except Exception:
            return None

        resolved_settings = RawRenderSettings.from_dict(settings or getattr(self, "_raw_render_settings", None))
        decode_mode = self._raw_preview_decode_mode(resolved_settings)
        settings_signature = json.dumps(
            resolved_settings.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        cached_signature = getattr(self, "_raw_curve_preview_analysis_signature", None)
        base_signature = (str(source_path), int(stat.st_mtime_ns), int(stat.st_size), decode_mode)
        cache_signature = base_signature + (settings_signature,)
        if cached_signature == cache_signature:
            cached_rgb = getattr(self, "_raw_curve_preview_analysis_rgb", None)
            cached_histogram = getattr(self, "_raw_curve_preview_histogram", None)
            if isinstance(cached_rgb, np.ndarray) and isinstance(cached_histogram, np.ndarray):
                return cached_rgb, cached_histogram

        try:
            if is_raw_image_path(source_path):
                cache = getattr(self, "_pending_raw_preview_proxy_cache", None)
                cache_key = self._raw_preview_proxy_cache_key(source_path, resolved_settings)
                cached_proxy_rgb = None
                if cache_key is not None and isinstance(cache, dict):
                    cached_proxy = cache.get(cache_key)
                    if isinstance(cached_proxy, _RawPreviewCacheEntry):
                        cached_proxy_rgb = cached_proxy.raw_rgb
                    elif isinstance(cached_proxy, np.ndarray):
                        cached_proxy_rgb = cached_proxy
                if isinstance(cached_proxy_rgb, np.ndarray) and cached_proxy_rgb.size:
                    rgb = _resize_preview_rgb(cached_proxy_rgb, _TONE_CURVE_PREVIEW_ANALYSIS_MAX_DIM)
                    signature = cache_signature
                else:
                    preview_rgb = self._raw_curve_preview_rgb_from_active_preview(source_path)
                    if isinstance(preview_rgb, np.ndarray) and preview_rgb.size:
                        rgb = _resize_preview_rgb(preview_rgb, _TONE_CURVE_PREVIEW_ANALYSIS_MAX_DIM)
                        signature = ("preview",) + cache_signature
                    else:
                        rgb = self._raw_curve_preview_fallback_rgb()
                        signature = ("fallback",) + cache_signature
            else:
                with Image.open(source_path) as image:
                    image.load()
                    image = image.convert("RGB")
                    max_dim = max(int(image.width), int(image.height))
                    if max_dim > _TONE_CURVE_PREVIEW_ANALYSIS_MAX_DIM:
                        scale = float(_TONE_CURVE_PREVIEW_ANALYSIS_MAX_DIM) / float(max_dim)
                        resized_size = (
                            max(1, int(round(float(image.width) * scale))),
                            max(1, int(round(float(image.height) * scale))),
                        )
                        resample = getattr(getattr(Image, "Resampling", Image), "BILINEAR", Image.BILINEAR)
                        image = image.resize(resized_size, resample)
                    rgb = np.asarray(image, dtype=np.float32) / np.float32(255.0)
                signature = cache_signature
        except Exception:
            return None

        try:
            # Histogram bins live on the same axis as `curve.input_values`
            # (WB-applied, pre-levels luminance) so the widget's clipped-bin
            # markers line up with the curve saturation points.
            histogram_rgb = compute_pre_levels_working_rgb(rgb, resolved_settings)
        except Exception:
            histogram_rgb = rgb

        histogram = _compute_combined_rgb_histogram(histogram_rgb, bins=_TONE_CURVE_PREVIEW_HISTOGRAM_BINS)
        self._raw_curve_preview_analysis_signature = signature
        self._raw_curve_preview_analysis_rgb = rgb
        self._raw_curve_preview_histogram = histogram
        return rgb, histogram

    def _refresh_raw_curve_preview(self) -> None:
        widget = getattr(self, "raw_curve_preview_widget", None)
        if widget is None:
            return

        controls = getattr(self, "raw_controls", None)
        if controls is not None:
            try:
                settings = RawRenderSettings.from_dict(controls.settings())
            except Exception:
                settings = RawRenderSettings.from_dict(getattr(self, "_raw_render_settings", None))
        else:
            settings = RawRenderSettings.from_dict(getattr(self, "_raw_render_settings", None))

        source_path = self._raw_curve_preview_source_path()

        cached_curve = getattr(self, "_raw_preview_last_curve", None)
        cached_signature = getattr(self, "_raw_preview_last_curve_signature", None)
        if cached_curve is not None and cached_signature is not None and source_path is not None:
            if cached_signature == LiveLabRawMixin._raw_curve_signature_for_source(source_path, settings):
                histogram = getattr(self, "_raw_curve_preview_histogram", None)
                widget.set_curve(cached_curve, histogram)
                return

        analysis = self._raw_curve_preview_analysis_from_path(source_path, settings)
        if analysis is None:
            rgb = self._raw_curve_preview_fallback_rgb()
            histogram = None
            self._raw_curve_preview_analysis_signature = None
            self._raw_curve_preview_analysis_rgb = rgb
            self._raw_curve_preview_histogram = None
        else:
            rgb, histogram = analysis

        try:
            curve = compute_post_decode_transfer_curve(rgb, settings)
        except Exception:
            curve = None
        widget.set_curve(curve, histogram)

    def _raw_processing_pick_target(self) -> str | None:
        if getattr(self, "_raw_edit_session", None) is not None:
            return "edit"
        if self._current_pending_raw_capture() is not None:
            return "pending"
        return None

    def _update_raw_processing_pick_button(self) -> None:
        controls = getattr(self, "raw_controls", None)
        if controls is None:
            return
        target = self._raw_processing_pick_target()
        armed = False
        if target == "edit":
            armed = bool(getattr(self, "_raw_edit_background_wb_armed", False))
        elif target == "pending":
            armed = bool(getattr(self, "_pending_raw_background_wb_armed", False))
        controls.set_pick_enabled(bool(target))
        controls.set_pick_checked(armed)

    def _toggle_active_raw_background_wb_pick(self, checked: bool) -> None:
        target = self._raw_processing_pick_target()
        controls = getattr(self, "raw_controls", None)
        if not checked:
            if target is not None:
                self._cancel_raw_background_wb_selection(target=target)
            self._refresh_raw_processing_context_ui()
            return
        if target is None:
            if controls is not None:
                controls.set_pick_checked(False)
            self._show_status(
                self.tr("Choose a pending RAW capture or a RAW edit session before sampling background WB."),
                tone="warning",
                timeout_ms=4000,
            )
            self._refresh_raw_processing_context_ui()
            return
        self._toggle_raw_background_wb_pick(True, target=target)

    def _update_raw_processing_section_label(self, expanded: bool) -> None:
        if not hasattr(self, "raw_processing_toggle_btn"):
            return
        arrow = "▾" if expanded else "▸"
        summary = self._raw_processing_summary_text()
        self.raw_processing_toggle_btn.setText(
            self.tr("RAW processing {arrow} {summary}").format(arrow=arrow, summary=summary)
        )

    def _on_raw_processing_toggle_changed(self, checked: bool) -> None:
        self._update_raw_processing_visibility()
        self._update_raw_processing_section_label(bool(checked))
        self._set_hint(self._current_raw_hint_text())

    def _on_raw_processing_controls_changed(self, *_args) -> None:
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        sender_fn = getattr(self, "sender", None)
        sender = sender_fn() if callable(sender_fn) else None
        editing_session = getattr(self, "_raw_edit_session", None)
        settings = self._raw_settings_from_controls(update_session_settings=editing_session is None)
        if editing_session is not None:
            current_edit_settings = RawRenderSettings.from_dict(editing_session.working_settings)
            settings = self._preserve_raw_wb_fields(current_edit_settings, settings)
            editing_session.working_settings = settings
            editing_session.dirty = settings != editing_session.original_settings
            self._refresh_raw_edit_preview()
            self._set_raw_tone_controls_enabled(bool(settings.tone_curve_enabled))
            self._update_pending_raw_controls()
            self._update_raw_edit_controls()
            self._update_raw_processing_section_label(bool(getattr(self, "raw_processing_toggle_btn", None) and self.raw_processing_toggle_btn.isChecked()))
            self._refresh_raw_processing_context_ui()
            _raw_timing_log(
                "RAW controls changed (edit session)",
                start,
                detail=f"image_id={editing_session.image_id} sender={type(sender).__name__ if sender is not None else 'unknown'}",
            )
            return

        selected_targets = self._selected_raw_processing_targets()
        if selected_targets:
            active_pending_index = self._selected_pending_raw_index_value()
            active_pending_capture = self._current_pending_raw_capture()
            active_committed_image_id = self._selected_committed_image_id()
            active_pending_applied = False
            active_committed_applied = False
            any_applied = False
            image_target = None
            mixed_fields_fn = getattr(getattr(self, "raw_controls", None), "mixed_fields", None)
            mixed_fields = set(mixed_fields_fn()) if callable(mixed_fields_fn) else set()
            merge_fn = getattr(self, "_merge_raw_form_into_target", None)
            for kind, target_key, target in selected_targets:
                if kind == "pending" and isinstance(target, PendingRawCapture):
                    is_active_pending = active_pending_capture is target
                    if is_active_pending:
                        active_pending_applied = True
                    if callable(merge_fn):
                        target_settings = merge_fn(target.raw_settings, settings, mixed_fields)
                    else:
                        target_settings = settings
                    if self._apply_raw_settings_to_pending_capture(
                        target,
                        target_settings,
                        render_preview=is_active_pending,
                    ):
                        any_applied = True
                    else:
                        any_applied = True
                elif kind == "image":
                    if image_target is None:
                        image_target = target
            if any_applied:
                self._set_raw_tone_controls_enabled(bool(settings.tone_curve_enabled))
                self._update_pending_raw_controls()
                self._update_raw_edit_controls()
                self._update_raw_processing_section_label(bool(getattr(self, "raw_processing_toggle_btn", None) and self.raw_processing_toggle_btn.isChecked()))
                if active_pending_applied and active_pending_capture is not None:
                    try:
                        pending_index = active_pending_index
                        current_capture = self._current_pending_raw_capture()
                        if current_capture is active_pending_capture:
                            pending_index = self._selected_pending_raw_index_value()
                        self._show_pending_raw_capture(pending_index)
                    except Exception:
                        pass
                elif active_committed_applied and active_committed_image_id is not None:
                    self._show_session_image(int(active_committed_image_id))
                self._refresh_raw_processing_context_ui()
                _raw_timing_log(
                    "RAW controls changed (selected targets)",
                    start,
                    detail=(
                        f"targets={len(selected_targets)} "
                        f"pending={active_pending_applied} committed={active_committed_applied} "
                        f"sender={type(sender).__name__ if sender is not None else 'unknown'}"
                    ),
                )
                return
            if image_target is not None:
                begin_helper = getattr(self, "_begin_raw_edit_for_selected_image", None)
                if callable(begin_helper):
                    begin_helper(settings, source_image=image_target)
                _raw_timing_log(
                    "RAW controls changed (selected targets)",
                    start,
                    detail=f"targets={len(selected_targets)} pending={active_pending_applied} committed=True sender={type(sender).__name__ if sender is not None else 'unknown'}",
                )
                return

        save_helper = getattr(self, "_save_raw_processing_settings_for_current_context", None)
        if callable(save_helper):
            save_helper(settings)
        else:
            LiveLabRawMixin._save_raw_processing_settings_for_current_context(self, settings)
        self._set_raw_tone_controls_enabled(bool(settings.tone_curve_enabled))
        self._update_pending_raw_controls()
        self._update_raw_edit_controls()
        self._update_raw_processing_section_label(bool(getattr(self, "raw_processing_toggle_btn", None) and self.raw_processing_toggle_btn.isChecked()))
        self._refresh_raw_processing_context_ui()
        _raw_timing_log(
            "RAW controls changed (no targets)",
            start,
            detail=f"sender={type(sender).__name__ if sender is not None else 'unknown'}",
        )

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

    def _refresh_live_image_hover_hint(self, capture: PendingRawCapture | None = None) -> None:
        widget = getattr(self, "live_image_label", None)
        if widget is None:
            return
        text = self._pending_raw_action_hint_text(capture)
        set_property = getattr(widget, "setProperty", None)
        if callable(set_property):
            set_property("_hint_text", text)
        controller = getattr(self, "_hint_controller", None)
        under_mouse = getattr(widget, "underMouse", None)
        if controller is not None and callable(under_mouse) and under_mouse():
            try:
                controller.set_hint(text, tone="info")
            except Exception:
                pass

    def _pending_raw_action_hint_text(self, capture: PendingRawCapture | None = None) -> str:
        if self._pending_raw_background_wb_armed:
            return self.tr("Click neutral background to set WB")
        active_capture = capture or self._current_pending_raw_capture()
        if self._raw_settings_has_sampled_background_wb(active_capture.raw_settings if active_capture is not None else None):
            readout = self._raw_white_balance_readout_text(active_capture.raw_settings if active_capture is not None else None)
            return self.tr(
                "{readout} · ←/→ select · Delete/Backspace/Cmd/Ctrl+D remove current image · Enter save current"
            ).format(readout=readout)
        return self.tr("←/→ select · Delete/Backspace/Cmd/Ctrl+D remove current image · Enter save current")

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
                summary=self._raw_settings_info_text(session.working_settings),
            )
            if session.dirty:
                summary_text = f"{summary_text} {self.tr('· preview only')}"
        elif editable_session is not None:
            summary_text = self.tr("RAW-backed image: {name} · {summary}").format(
                name=Path(str(editable_session.current_derivative_path)).name,
                summary=self._raw_settings_info_text(editable_session.original_settings),
            )
            if has_copied_settings:
                summary_text = f"{summary_text} {self.tr('· copied settings available')}"
        elif selected_image is not None and frame is not None:
            summary_text = self.tr("Select a RAW-backed image to re-render it in place.")

        if summary_label is not None:
            summary_label.setText(summary_text)
        self._refresh_raw_processing_context_ui()

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
        auto_settings = None
        if editable_session.source_raw_path.exists():
            auto_settings = self._raw_auto_level_settings_for_source(
                editable_session.source_raw_path,
                editable_session.working_settings,
            )
        self._sync_raw_processing_controls_from_settings(
            editable_session.working_settings,
            update_session_settings=False,
            auto_level_settings=auto_settings,
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

        was_dirty = bool(getattr(session, "dirty", False))
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
        if was_dirty:
            self._show_status(
                self.tr("RAW preview changes discarded."),
                tone="info",
                timeout_ms=3000,
            )

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
            timer.start()

    def _refresh_raw_edit_preview(self) -> bool:
        session = getattr(self, "_raw_edit_session", None)
        if session is None:
            return False
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None

        cache_key = self._raw_preview_proxy_cache_key(session.source_raw_path, session.working_settings)
        cache = getattr(self, "_pending_raw_preview_proxy_cache", None)
        cached_proxy_available = _pending_raw_preview_cache_has_entry(cache, cache_key)
        existing_preview_rgb = getattr(session, "preview_rgb", None)
        if not cached_proxy_available and isinstance(existing_preview_rgb, np.ndarray) and existing_preview_rgb.size:
            session.dirty = session.working_settings != session.original_settings
            self._update_raw_edit_controls()
            if self._selected_committed_image_id() == session.image_id:
                self._show_session_image(session.image_id)
            else:
                self._schedule_raw_edit_preview_save(session)
            _raw_timing_log(
                "RAW edit preview refresh",
                start,
                detail=f"{session.source_raw_path.name} reused existing preview (proxy cache cold)",
            )
            return True

        try:
            preview_rgb, _processing_settings = self._raw_preview_rgb_for_source(session.source_raw_path, session.working_settings)
            session.preview_rgb = preview_rgb
            preview_dir = self._raw_edit_preview_dir()
            preview_dir.mkdir(parents=True, exist_ok=True)
            preview_path = session.preview_path
            if preview_path is None:
                preview_path = preview_dir / f"{session.source_raw_path.stem}_{session.image_id}.jpg"
            session.preview_path = Path(preview_path)
            session.dirty = session.working_settings != session.original_settings
            self._update_raw_edit_controls()
            if self._selected_committed_image_id() == session.image_id:
                self._show_session_image(session.image_id)
                action = "displayed"
            else:
                self._schedule_raw_edit_preview_save(session)
                action = "scheduled-save"
            _raw_timing_log(
                "RAW edit preview refresh",
                start,
                detail=f"{session.source_raw_path.name} {preview_rgb.shape[1]}x{preview_rgb.shape[0]} {action}",
            )
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
        final_path = Path(session.current_derivative_path)
        final_path.parent.mkdir(parents=True, exist_ok=True)
        temp_suffix = final_path.suffix.lower() or ".jpg"
        temp_output = temp_dir / f"{session.source_raw_path.stem}_{session.image_id}_{uuid4().hex}{temp_suffix}"

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
            self._invalidate_thumbnail_caches_for_raw_image(int(session.image_id), final_path)
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
            self._update_observation_thumbnail()
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

    def _invalidate_thumbnail_caches_for_raw_image(self, image_id: int, final_path: Path) -> None:
        thumb_path = str(get_thumbnail_path(int(image_id), "small") or "").strip()
        if thumb_path:
            gallery = getattr(self, "session_gallery", None)
            invalidate_gallery = getattr(gallery, "invalidate_pixmap_cache", None)
            if callable(invalidate_gallery):
                try:
                    invalidate_gallery(thumb_path)
                except Exception:
                    pass

        main_window = getattr(self, "_main_window", None)
        if main_window is None:
            return

        invalidate_main_window = getattr(main_window, "invalidate_pixmap_cache", None)
        if callable(invalidate_main_window):
            try:
                invalidate_main_window(str(final_path))
            except Exception:
                pass
            return

        target = str(final_path)
        cache = getattr(main_window, "_pixmap_cache", None)
        if isinstance(cache, dict):
            cache.pop(target, None)
        order = getattr(main_window, "_pixmap_cache_order", None)
        if isinstance(order, list) and target in order:
            order[:] = [item for item in order if item != target]
        gallery_cache = getattr(main_window, "_gallery_pixmap_cache", None)
        if isinstance(gallery_cache, dict):
            gallery_cache.pop(target, None)

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

        if target == "pending":
            refresh_hover_hint = getattr(self, "_refresh_live_image_hover_hint", None)
            if callable(refresh_hover_hint):
                refresh_hover_hint()
        else:
            label = getattr(self, "raw_edit_summary_label", None)
            if label is not None and self._raw_edit_session is not None:
                if armed:
                    label.setText(self.tr("Click neutral background to set WB"))
                else:
                    label.setText(
                        self.tr("Editing RAW: {name} · {summary}").format(
                            name=Path(str(self._raw_edit_session.source_raw_path)).name,
                            summary=self._raw_settings_info_text(self._raw_edit_session.working_settings),
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
        self._refresh_raw_processing_context_ui()

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

    def _raw_background_wb_sampling_view(
        self,
        target: str = "pending",
    ) -> tuple[QPixmap | None, tuple[float, float, float, float] | None, tuple[float, float] | None, tuple[float, float] | None]:
        base_pixmap = self._raw_background_wb_sampling_pixmap(target)
        if base_pixmap is None or base_pixmap.isNull():
            return None, None, None, None

        display_pixmap = getattr(self.live_image_label, "original_pixmap", None)
        if display_pixmap is None or display_pixmap.isNull():
            display_pixmap = base_pixmap

        display_width = float(max(1, int(display_pixmap.width())))
        display_height = float(max(1, int(display_pixmap.height())))
        base_width = float(max(1, int(base_pixmap.width())))
        base_height = float(max(1, int(base_pixmap.height())))
        source_scale_x = base_width / display_width
        source_scale_y = base_height / display_height

        crop_left = 0.0
        crop_top = 0.0
        crop_width = base_width
        crop_height = base_height
        view_getter = getattr(self.live_image_label, "get_current_view_crop_rect", None)
        if callable(view_getter):
            try:
                view_rect = view_getter()
            except Exception:
                view_rect = None
            if view_rect:
                try:
                    view_left, view_top, view_width, view_height = (
                        float(view_rect[0]),
                        float(view_rect[1]),
                        float(view_rect[2]),
                        float(view_rect[3]),
                    )
                except Exception:
                    view_left = view_top = view_width = view_height = 0.0
                if view_width > 0 and view_height > 0:
                    crop_left = max(0.0, min(base_width - 1.0, view_left * source_scale_x))
                    crop_top = max(0.0, min(base_height - 1.0, view_top * source_scale_y))
                    crop_width = max(1.0, min(base_width - crop_left, view_width * source_scale_x))
                    crop_height = max(1.0, min(base_height - crop_top, view_height * source_scale_y))

        crop_x = max(0, min(int(crop_left), int(base_width) - 1))
        crop_y = max(0, min(int(crop_top), int(base_height) - 1))
        crop_w = max(1, min(int(round(crop_width)), int(base_width) - crop_x))
        crop_h = max(1, min(int(round(crop_height)), int(base_height) - crop_y))
        working_pixmap = base_pixmap.copy(crop_x, crop_y, crop_w, crop_h)

        widget_size = getattr(self.live_image_label, "size", None)
        if callable(widget_size):
            try:
                widget_size = widget_size()
            except Exception:
                widget_size = None
        if widget_size is not None:
            try:
                target_w = int(widget_size.width())
                target_h = int(widget_size.height())
            except Exception:
                target_w = target_h = 0
            if target_w > 0 and target_h > 0 and (working_pixmap.width() > target_w or working_pixmap.height() > target_h):
                scale = min(float(target_w) / float(working_pixmap.width()), float(target_h) / float(working_pixmap.height()))
                if scale < 1.0:
                    working_pixmap = working_pixmap.scaled(
                        max(1, int(round(working_pixmap.width() * scale))),
                        max(1, int(round(working_pixmap.height() * scale))),
                        Qt.IgnoreAspectRatio,
                        Qt.SmoothTransformation,
                    )

        working_scale_x = float(working_pixmap.width()) / float(crop_w)
        working_scale_y = float(working_pixmap.height()) / float(crop_h)
        return (
            working_pixmap,
            (float(crop_x), float(crop_y), float(crop_w), float(crop_h)),
            (source_scale_x, source_scale_y),
            (working_scale_x, working_scale_y),
        )

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

        source_path: Path | None = None
        if target == "edit":
            session = getattr(self, "_raw_edit_session", None)
            if session is None:
                return False
            source_path = Path(session.source_raw_path)
        else:
            capture = self._current_pending_raw_capture()
            if capture is None:
                self._show_status(
                    self.tr("Choose a pending RAW capture before sampling background WB."),
                    tone="warning",
                    timeout_ms=4000,
                )
                return False
            source_path = Path(capture.source_path)

        if source_path.exists() and bool(updated_settings.auto_levels):
            updated_settings = self._raw_auto_level_settings_for_source(source_path, updated_settings)

        armed, _button = self._raw_background_wb_selection_state(target)
        if not armed:
            return False

        if target == "edit":
            session.working_settings = updated_settings
            session.dirty = updated_settings != session.original_settings
            self._cancel_raw_background_wb_selection(target=target)
            auto_settings = None
            if source_path.exists():
                auto_settings = self._raw_auto_level_settings_for_source(source_path, updated_settings)
            self._sync_raw_processing_controls_from_settings(
                updated_settings,
                update_session_settings=False,
                auto_level_settings=auto_settings,
            )
            self._schedule_raw_edit_preview_refresh()
            self._update_raw_edit_controls()
            self._show_status(
                self._raw_white_balance_readout_text(updated_settings),
                tone="success",
                timeout_ms=3000,
            )
            return True

        capture.raw_settings = updated_settings
        capture.rendered_settings = None
        self._cancel_raw_background_wb_selection(target=target)
        auto_settings = None
        if source_path.exists():
            auto_settings = self._raw_auto_level_settings_for_source(source_path, updated_settings)
        capture.auto_level_settings = auto_settings
        self._sync_raw_processing_controls_from_settings(
            updated_settings,
            update_session_settings=True,
            auto_level_settings=auto_settings,
        )
        propagate_wb = getattr(self, "_propagate_raw_wb_to_selected_pending", None)
        if callable(propagate_wb):
            propagate_wb(updated_settings, exclude=capture)
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

        pixmap, crop_rect, source_scale, working_scale = self._raw_background_wb_sampling_view(target)
        if pixmap is None or crop_rect is None or source_scale is None or working_scale is None:
            self._show_status(
                self.tr("The stable preview is not available for background WB sampling."),
                tone="warning",
                timeout_ms=4000,
            )
            return False

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
        source_x1 = float(x1) * float(source_scale[0])
        source_y1 = float(y1) * float(source_scale[1])
        source_x2 = float(x2) * float(source_scale[0])
        source_y2 = float(y2) * float(source_scale[1])
        local_x1 = max(0.0, source_x1 - float(crop_rect[0]))
        local_y1 = max(0.0, source_y1 - float(crop_rect[1]))
        local_x2 = min(float(crop_rect[2]), source_x2 - float(crop_rect[0]))
        local_y2 = min(float(crop_rect[3]), source_y2 - float(crop_rect[1]))
        if local_x2 <= local_x1 or local_y2 <= local_y1:
            return False
        sample_rect = (
            float(local_x1) * float(working_scale[0]),
            float(local_y1) * float(working_scale[1]),
            max(1.0, float(local_x2 - local_x1) * float(working_scale[0])),
            max(1.0, float(local_y2 - local_y1) * float(working_scale[1])),
        )
        try:
            multipliers = estimate_white_balance_from_background(rgb, rect=sample_rect)
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
            (float(crop_rect[0] + local_x1), float(crop_rect[1] + local_y1), float(local_x2 - local_x1), float(local_y2 - local_y1)),
            target=target,
            sample_size=None,
        )

    def _apply_raw_background_wb_selection_from_point(self, point: QPointF, *, target: str = "pending") -> bool:
        armed, _button = self._raw_background_wb_selection_state(target)
        if not armed:
            return False

        pixmap, crop_rect, source_scale, working_scale = self._raw_background_wb_sampling_view(target)
        if pixmap is None or crop_rect is None or source_scale is None or working_scale is None:
            self._show_status(
                self.tr("The stable preview is not available for background WB sampling."),
                tone="warning",
                timeout_ms=4000,
            )
            return False

        rgb = self._pixmap_rgb_array(pixmap)
        if rgb is None:
            self._show_status(
                self.tr("The stable preview is not available for background WB sampling."),
                tone="warning",
                timeout_ms=4000,
            )
            return False

        sample_rect = self._raw_background_wb_sample_rect_from_point(
            QPointF(
                max(0.0, (float(point.x()) * float(source_scale[0])) - float(crop_rect[0])) * float(working_scale[0]),
                max(0.0, (float(point.y()) * float(source_scale[1])) - float(crop_rect[1])) * float(working_scale[1]),
            ),
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
            (
                float(crop_rect[0] + (float(sample_rect[0]) / float(working_scale[0]))),
                float(crop_rect[1] + (float(sample_rect[1]) / float(working_scale[1]))),
                float(sample_rect[2]) / float(working_scale[0]),
                float(sample_rect[3]) / float(working_scale[1]),
            ),
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
        if self._raw_edit_session is not None:
            return False
        if self._current_pending_raw_capture() is None and self._selected_committed_image_id() is None:
            return False
        if self._pending_raw_background_wb_armed:
            return False
        focus_widget = QApplication.focusWidget()
        if focus_widget is not None and self._is_text_input_widget(focus_widget):
            # Only treat a focused text-input as blocking when it actually
            # belongs to this tab. A foreign focus widget left behind by
            # another dialog (for example a QPushButton from an unrelated
            # window) must not silently swallow the review shortcut — Qt's
            # WidgetWithChildrenShortcut context already scopes shortcut
            # delivery in production; this mirrors that scoping in the
            # Python-side gate so unit tests and stray focus don't gate the
            # action off.
            is_ancestor = getattr(self, "isAncestorOf", None)
            if focus_widget is self or (callable(is_ancestor) and bool(is_ancestor(focus_widget))):
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
            self._delete_current_raw_review_item()
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
            (QKeySequence("Ctrl+D"), "discard"),
            (QKeySequence("Meta+D"), "discard"),
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

    def _finalize_pending_raw_background_wb_from_preview(self) -> None:
        self._finalize_raw_background_wb_from_preview(target="pending")

    def _normalize_raw_capture_mode(self, value: str | None) -> str:
        mode = str(value or self.RAW_CAPTURE_MODE_AUTO_SAVE).strip().lower()
        return mode if mode in {self.RAW_CAPTURE_MODE_AUTO_SAVE, self.RAW_CAPTURE_MODE_REVIEW} else self.RAW_CAPTURE_MODE_AUTO_SAVE

    def _selected_raw_capture_mode(self) -> str:
        selector = getattr(self, "raw_capture_mode_selector", None)
        if selector is not None:
            return self._normalize_raw_capture_mode(selector.selected_value(self.RAW_CAPTURE_MODE_AUTO_SAVE))
        # The simplified Live Lab UI always reviews RAW captures.
        return self.RAW_CAPTURE_MODE_REVIEW

    def _raw_capture_mode_label(self, mode: str | None = None, *, short: bool = False) -> str:
        normalized = self._normalize_raw_capture_mode(mode or self._selected_raw_capture_mode())
        if normalized == self.RAW_CAPTURE_MODE_REVIEW:
            return self.tr("Review") if short else self.tr("Review RAW before saving")
        return self.tr("Auto-save") if short else self.tr("Auto-save RAW captures")

    def _restore_raw_capture_mode(self) -> None:
        saved = self._normalize_raw_capture_mode(
            SettingsDB.get_setting(self.SETTING_RAW_CAPTURE_MODE, self.RAW_CAPTURE_MODE_AUTO_SAVE)
        )
        selector = getattr(self, "raw_capture_mode_selector", None)
        if selector is not None:
            self._raw_capture_mode = saved
            selector.set_selected_value(saved)
        else:
            self._raw_capture_mode = self.RAW_CAPTURE_MODE_REVIEW
        self._update_pending_raw_controls()
        self._update_raw_processing_section_label(bool(getattr(self, "raw_processing_toggle_btn", None) and self.raw_processing_toggle_btn.isChecked()))

    def _save_raw_capture_mode(self, mode: str | None = None) -> None:
        normalized = self._normalize_raw_capture_mode(mode or self._selected_raw_capture_mode())
        if getattr(self, "raw_capture_mode_selector", None) is not None:
            self._raw_capture_mode = normalized
        else:
            self._raw_capture_mode = self.RAW_CAPTURE_MODE_REVIEW
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

    def _selected_pending_raw_captures(self) -> list[PendingRawCapture]:
        captures = getattr(self, "_pending_raw_captures", []) or []
        selected: list[PendingRawCapture] = []
        seen_indices: set[int] = set()
        for key in self._session_gallery_selected_keys():
            pending_index = self._pending_raw_capture_index_for_key(key)
            if pending_index is None or pending_index in seen_indices:
                continue
            if 0 <= pending_index < len(captures):
                capture = captures[pending_index]
                if isinstance(capture, PendingRawCapture):
                    selected.append(capture)
                    seen_indices.add(pending_index)
        if selected:
            return selected
        current_pending = self._current_pending_raw_capture()
        return [current_pending] if current_pending is not None else []

    def _sync_selected_pending_raw_metadata_from_controls(self) -> bool:
        # Sidebar changes only mutate already-captured images when the user
        # has explicitly bound the sidebar to a thumbnail via a click. When
        # unbound the change is a preparation for the next capture — keep the
        # session state shadow in sync so the next ingest picks it up.
        if not getattr(self, "_sidebar_binds_to_selection", False):
            refresh_state = getattr(self, "_refresh_session_microscope_state_from_sidebar", None)
            if callable(refresh_state):
                refresh_state()
            return False
        if getattr(self, "_raw_edit_session", None) is not None:
            return False
        selected_captures = self._selected_pending_raw_captures()
        if not selected_captures:
            return False

        current_lab_metadata = getattr(self, "_current_lab_metadata", None)
        if not callable(current_lab_metadata):
            return False
        try:
            metadata = dict(current_lab_metadata() or {})
        except Exception:
            return False
        # Skip fields the user hasn't disambiguated yet — a mixed selector
        # is showing "these images disagree on this field", and applying the
        # sidebar's placeholder value would silently collapse that spread.
        mixed_keys_fn = getattr(self, "_mixed_sidebar_field_keys", None)
        mixed_keys = mixed_keys_fn() if callable(mixed_keys_fn) else set()
        for key in mixed_keys:
            metadata.pop(key, None)

        changed = False
        for capture in selected_captures:
            merged = merge_image_lab_metadata(capture.lab_metadata, metadata)
            if merged != capture.lab_metadata:
                capture.lab_metadata = merged
                changed = True
        if changed:
            self._refresh_session_gallery()
            refresh_viewer_objective_tag = getattr(self, "_refresh_viewer_objective_tag_from_current_state", None)
            if callable(refresh_viewer_objective_tag):
                refresh_viewer_objective_tag()
        if changed:
            self._refresh_viewer_overlays()
        return changed

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

    def _pending_raw_capture_exists(self, source_path: str | Path, *, group_key: str | None = None) -> bool:
        source_text = str(source_path or "").strip()
        if not source_text:
            return False
        candidate_group_key = str(group_key or "").strip() or companion_group_key(source_text)
        for capture in getattr(self, "_pending_raw_captures", []) or []:
            if not isinstance(capture, PendingRawCapture):
                continue
            if str(capture.source_path).strip() == source_text:
                return True
            if candidate_group_key and str(capture.group_key or "").strip() == candidate_group_key:
                return True
        return False

    def _pending_raw_gallery_items(self) -> tuple[list[dict[str, object]], str | int | None]:
        items: list[dict[str, object]] = []
        selected_key: str | int | None = None
        current_pending = self._current_pending_raw_capture()
        # Continue numbering from the last committed session image so the
        # newest pending capture — the freshest photo in the session — carries
        # the highest visible number.
        number_offset = len(getattr(self, "_session_image_ids", None) or [])
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
            microscope_tag_text, microscope_tag_color = _live_lab_tab_class()._microscope_tag_for_metadata(self, capture.lab_metadata)
            item = {
                "id": self._pending_raw_gallery_key(capture),
                "filepath": str(capture.source_path),
                "preview_path": preview_path,
                # Continue the visual sequence from the last committed image so
                # the newest capture always shows the highest number.
                "image_number": f"P{number_offset + index + 1}",
                "badges": badges,
                "frame_border_color": "#e74c3c" if capture.status == "failed" else STAGING_BORDER,
                "raw_halo_color": "#e74c3c",
                "microscope_tag_text": microscope_tag_text,
                "microscope_tag_color": microscope_tag_color,
                "gps_tag_text": microscope_tag_text,
                "gps_tag_color": microscope_tag_color,
            }
            items.append(item)
            if current_pending is capture:
                selected_key = item["id"]
        return items, selected_key

    def _update_pending_raw_controls(self) -> None:
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
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
            reposition = getattr(self, "_reposition_pending_raw_floating_tab", None)
            if callable(reposition):
                reposition()

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

        if not bool(getattr(self, "_suppress_pending_raw_gallery_refresh", False)):
            gallery_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            self._refresh_session_gallery()
            _raw_timing_log("update pending RAW controls gallery refresh", gallery_start, detail=f"captures={count}")

        current_capture = self._current_pending_raw_capture()
        has_selection = current_capture is not None
        preview_available = bool(
            has_selection
            and getattr(current_capture, "preview_path", None)
            and Path(str(current_capture.preview_path)).exists()
        )
        commit_busy = bool(
            getattr(self, "_pending_raw_commit_active_request", None)
            or getattr(self, "_pending_raw_commit_job_queue", None)
        )

        save_btn = getattr(self, "pending_raw_save_btn", None)
        if save_btn is not None:
            save_btn.setEnabled(bool(has_selection and not self._pending_raw_background_wb_armed and not commit_busy))

        save_all_btn = getattr(self, "pending_raw_save_all_btn", None)
        if save_all_btn is not None:
            save_all_btn.setEnabled(bool(count and not self._pending_raw_background_wb_armed and not commit_busy))

        copy_btn = getattr(self, "pending_raw_copy_btn", None)
        if copy_btn is not None:
            copy_btn.setEnabled(bool(has_selection and not self._pending_raw_background_wb_armed))

        paste_btn = getattr(self, "pending_raw_paste_btn", None)
        if paste_btn is not None:
            copied_settings = getattr(self, "_raw_copied_settings", None)
            copied_source = getattr(self, "_pending_raw_copy_source_path", None)
            current_source = (
                str(current_capture.source_path)
                if current_capture is not None and current_capture.source_path is not None
                else None
            )
            paste_applicable = bool(
                copied_settings is not None
                and has_selection
                and copied_source is not None
                and current_source is not None
                and copied_source != current_source
                and not self._pending_raw_background_wb_armed
            )
            paste_btn.setVisible(paste_applicable)
            paste_btn.setEnabled(paste_applicable)
            reposition = getattr(self, "_reposition_pending_raw_floating_tab", None)
            if callable(reposition):
                reposition()

        wb_btn = getattr(self, "pending_raw_pick_wb_btn", None)
        if wb_btn is not None:
            wb_btn.setEnabled(bool(preview_available and selected_mode == self.RAW_CAPTURE_MODE_REVIEW))
            if not self._pending_raw_background_wb_armed:
                wb_btn.setText(self.tr("Pick background WB"))

        refresh_hover_hint = getattr(self, "_refresh_live_image_hover_hint", None)
        if callable(refresh_hover_hint):
            refresh_hover_hint(current_capture)

        self._update_raw_processing_section_label(bool(getattr(self, "raw_processing_toggle_btn", None) and self.raw_processing_toggle_btn.isChecked()))
        self._refresh_raw_processing_context_ui()
        _raw_timing_log("update pending RAW controls", start, detail=f"captures={count}")

    def _show_pending_raw_capture(self, index: int | None = None) -> None:
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
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
        gallery = getattr(self, "session_gallery", None)
        if gallery is not None:
            is_multi_select = False
            selected_keys: list[object] = []
            is_multi_select_fn = getattr(gallery, "is_multi_select", None)
            if callable(is_multi_select_fn):
                try:
                    is_multi_select = bool(is_multi_select_fn())
                except Exception:
                    is_multi_select = False
            selected_keys_fn = getattr(gallery, "selected_keys", None)
            if callable(selected_keys_fn):
                try:
                    selected_keys = [key for key in list(selected_keys_fn()) if key is not None]
                except Exception:
                    selected_keys = []
            select_image = getattr(gallery, "select_image", None)
            pending_key = self._pending_raw_gallery_key(capture)
            if (
                getattr(self, "_sidebar_binds_to_selection", False)
                and callable(select_image)
                and pending_key is not None
                and (not is_multi_select or not selected_keys)
            ):
                try:
                    select_image(pending_key, center=False)
                except Exception:
                    pass
        apply_microscope_state = getattr(self, "_apply_microscope_state_to_controls", None)
        if callable(apply_microscope_state):
            apply_microscope_state(capture.lab_metadata)
        self._sync_raw_processing_controls_from_settings(
            capture.raw_settings,
            auto_level_settings=capture.auto_level_settings,
        )
        preview_rgb = getattr(capture, "preview_rgb", None)
        rendered_settings = getattr(capture, "rendered_settings", None)
        memory_preview_is_current = (
            isinstance(rendered_settings, RawRenderSettings)
            and rendered_settings == capture.raw_settings
        )
        disk_preview_is_current = LiveLabRawMixin._pending_raw_capture_preview_is_current(capture)
        if not memory_preview_is_current:
            # Multi-select left this capture with new raw_settings but no
            # fresh preview. Force a re-render so the viewer reflects the
            # current sliders rather than the old JPEG on disk.
            preview_rgb = None
        preview_path = str(capture.preview_path or capture.source_path)
        title = self.tr("Pending RAW {current} of {total}: {name}").format(
            current=self._selected_pending_raw_index_value() + 1,
            total=len(captures),
            name=capture.source_path.name,
        )
        meta = self._raw_settings_info_text(capture.raw_settings)
        if isinstance(preview_rgb, np.ndarray) and preview_rgb.size:
            preview_dir = self._pending_raw_preview_dir()
            preview_dir.mkdir(parents=True, exist_ok=True)
            preview_path_obj = Path(capture.preview_path) if capture.preview_path else preview_dir / f"{capture.source_path.stem}_{uuid4().hex}.jpg"
            capture.preview_path = preview_path_obj
            preview_path = str(preview_path_obj)
            self._present_raw_preview(
                pixmap=self._rgb_to_pixmap(preview_rgb),
                image_path=preview_path,
                image=None,
                title=title,
                meta=meta,
                preview_scaled=True,
                preserve_view=True,
            )
            self._schedule_pending_raw_preview_save(capture)
            self._prune_pending_raw_preview_buffers(capture)
            _raw_timing_log(
                "show pending RAW capture",
                start,
                detail=f"{capture.source_path.name} source=memory preview={preview_rgb.shape[1]}x{preview_rgb.shape[0]}",
            )
        else:
            self._prune_pending_raw_preview_buffers(clear_proxy_cache=False)
            needs_refresh = not disk_preview_is_current
            if needs_refresh:
                active_request = getattr(self, "_pending_raw_preview_active_request", None)
                if (
                    isinstance(active_request, _PendingRawPreviewJobRequest)
                    and active_request.capture is capture
                    and active_request.kind == "selected"
                ):
                    self._update_pending_raw_controls()
                    return
                busy_setter = getattr(self, "_set_pending_raw_preview_busy", None)
                if callable(busy_setter):
                    busy_setter(True, self.tr("Preparing RAW preview…"))
                _raw_timing_log(
                    "show pending RAW capture",
                    start,
                    detail=f"{capture.source_path.name} source=refresh-needed",
                )
                self._refresh_selected_pending_raw_preview()
                return
            pixmap, _preview_scaled = self._load_viewer_pixmap(preview_path)
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
            self._present_raw_preview(
                pixmap=pixmap,
                image_path=preview_path,
                image=None,
                title=title,
                meta=meta,
                preview_scaled=True,
                preserve_view=True,
            )
            # Keep the proxy cache: the curve preview refresh that runs as
            # part of _sync_raw_processing_controls_from_settings re-uses it,
            # and so does the next slider event. Cache eviction now happens
            # when captures are removed, not on every disk-load.
            self._prune_pending_raw_preview_buffers(clear_proxy_cache=False)
            _raw_timing_log(
                "show pending RAW capture",
                start,
                detail=f"{capture.source_path.name} source=disk",
            )
        self._update_pending_raw_controls()

    def _refresh_selected_pending_raw_preview(self) -> None:
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        capture = self._current_pending_raw_capture()
        if capture is None:
            return
        if capture.status not in {"pending", "failed"}:
            return
        if LiveLabRawMixin._pending_raw_capture_preview_is_current(capture):
            return
        if self._pending_raw_preview_has_request_for_capture(capture, kind="selected"):
            return
        request_builder = getattr(self, "_pending_raw_preview_request_for_capture", None)
        enqueue_request = getattr(self, "_enqueue_pending_raw_preview_job", None)
        if callable(request_builder) and callable(enqueue_request):
            request = request_builder(capture, kind="selected")
            if request is not None:
                busy_setter = getattr(self, "_set_pending_raw_preview_busy", None)
                if callable(busy_setter):
                    busy_setter(True, self.tr("Preparing RAW preview…"))
                enqueue_request(request)
                _raw_timing_log(
                    "refresh selected pending RAW preview",
                    start,
                    detail=f"{capture.source_path.name} queued job={request.job_id}",
                )
                return
        busy_setter = getattr(self, "_set_pending_raw_preview_busy", None)
        if callable(busy_setter):
            busy_setter(False, "")
        _raw_timing_log(
            "refresh selected pending RAW preview",
            start,
            detail=f"{capture.source_path.name} skipped (no worker request)",
        )

    def _set_pending_raw_preview_busy(
        self,
        visible: bool,
        text: str | None = None,
        *,
        value: int | None = None,
        maximum: int | None = None,
    ) -> None:
        stack = getattr(self, "hint_progress_stack", None)
        if isinstance(stack, QStackedWidget):
            stack.setCurrentWidget(
                self.pending_raw_preview_progress_widget if bool(visible) else self.hint_bar
            )
        widget = getattr(self, "pending_raw_preview_progress_widget", None)
        if widget is not None:
            widget.setVisible(True)
        progress_text = str(text or "").strip()
        bar = getattr(self, "pending_raw_preview_progress_bar", None)
        if bar is not None:
            if visible:
                if value is not None and maximum is not None and int(maximum) > 0:
                    bar.setRange(0, int(maximum))
                    bar.setValue(int(max(0, min(int(value), int(maximum)))))
                else:
                    bar.setRange(0, 0)
                    bar.setValue(0)
                bar.setFormat("")
            else:
                bar.setRange(0, 100)
                bar.setValue(0)
                bar.setFormat("")
        label = getattr(self, "pending_raw_preview_progress_label", None)
        if label is not None:
            label.setText(progress_text)
        cancel_btn = getattr(self, "pending_raw_commit_cancel_btn", None)
        if cancel_btn is not None:
            cancel_btn.setVisible(False)

    def _set_pending_raw_commit_busy(
        self,
        visible: bool,
        text: str | None = None,
        *,
        value: int | None = None,
        maximum: int | None = None,
    ) -> None:
        busy_setter = getattr(self, "_set_pending_raw_preview_busy", None)
        if callable(busy_setter):
            busy_setter(visible, text, value=value, maximum=maximum)
        cancel_btn = getattr(self, "pending_raw_commit_cancel_btn", None)
        if cancel_btn is not None:
            cancel_btn.setVisible(bool(visible))
            cancel_btn.setEnabled(bool(visible and not getattr(self, "_pending_raw_commit_cancel_requested", False)))

    def _pending_raw_preview_request_for_capture(
        self,
        capture: PendingRawCapture | None,
        *,
        kind: str = "selected",
    ) -> _PendingRawPreviewJobRequest | None:
        if capture is None:
            return None
        job_id = int(getattr(self, "_pending_raw_preview_job_counter", 0)) + 1
        self._pending_raw_preview_job_counter = job_id
        source_path = Path(capture.source_path)
        source_signature = _pending_raw_source_signature(source_path)
        settings = RawRenderSettings.from_dict(capture.raw_settings)
        settings_signature = _pending_raw_settings_signature(settings)
        if capture.preview_path is None:
            preview_dir = self._pending_raw_preview_dir()
            preview_dir.mkdir(parents=True, exist_ok=True)
            capture.preview_path = preview_dir / f"{source_path.stem}_{uuid4().hex}.jpg"
        request_kind = "background" if str(kind or "").strip().lower() == "background" else "selected"
        return _PendingRawPreviewJobRequest(
            job_id=job_id,
            kind=request_kind,
            capture=capture,
            source_path=source_path,
            raw_settings=settings,
            source_signature=source_signature,
            settings_signature=settings_signature,
        )

    def _enqueue_pending_raw_preview_job(self, request: _PendingRawPreviewJobRequest | None) -> None:
        if request is None:
            return
        queue = getattr(self, "_pending_raw_preview_job_queue", None)
        if queue is None:
            queue = []
            self._pending_raw_preview_job_queue = queue
        active = getattr(self, "_pending_raw_preview_active_request", None)
        if isinstance(active, _PendingRawPreviewJobRequest) and active.job_id == request.job_id:
            return
        if request.kind == "selected":
            queue[:] = [
                queued
                for queued in queue
                if not (
                    isinstance(queued, _PendingRawPreviewJobRequest)
                    and (
                        queued.kind == "selected"
                        or (queued.kind == "background" and queued.capture is request.capture)
                    )
                )
            ]
            queue.insert(0, request)
        else:
            if self._pending_raw_preview_has_request_for_capture(request.capture, kind="selected"):
                return
            if self._pending_raw_preview_has_request_for_capture(request.capture, kind="background"):
                return
            queue.append(request)
        if request.kind == "selected":
            self._set_pending_raw_preview_busy(True, self.tr("Preparing RAW preview…"))
        if getattr(self, "_pending_raw_preview_worker_thread", None) is None:
            self._start_next_pending_raw_preview_job()

    def _start_next_pending_raw_preview_job(self) -> None:
        if getattr(self, "_pending_raw_preview_worker_thread", None) is not None:
            return
        queue = getattr(self, "_pending_raw_preview_job_queue", None)
        if not queue:
            self._set_pending_raw_preview_busy(False, "")
            return
        request = queue.pop(0)
        self._start_pending_raw_preview_job(request)

    def _start_pending_raw_preview_job(self, request: _PendingRawPreviewJobRequest) -> None:
        self._pending_raw_preview_active_request = request
        if request.kind == "selected":
            self._set_pending_raw_preview_busy(True, self.tr("Preparing RAW preview…"))
        thread = QThread(self)
        worker = _PendingRawPreviewWorker(request)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.finished.connect(self._on_pending_raw_preview_job_finished)
        worker.finished.connect(thread.quit)
        worker.finished.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(self._finish_pending_raw_preview_job)
        self._pending_raw_preview_worker_thread = thread
        self._pending_raw_preview_worker = worker
        thread.start()
        QApplication.processEvents()

    def _on_pending_raw_preview_job_finished(self, result: object) -> None:
        thread = getattr(self, "_pending_raw_preview_worker_thread", None)
        try:
            request = getattr(self, "_pending_raw_preview_active_request", None)
            if not isinstance(request, _PendingRawPreviewJobRequest):
                return
            if not isinstance(result, _PendingRawPreviewJobResult):
                return
            if result.job_id != request.job_id:
                LiveLabRawMixin._cleanup_preview_result_path(result)
                return
            current_request = request
            capture = current_request.capture
            pending_captures = list(getattr(self, "_pending_raw_captures", []) or [])
            if capture not in pending_captures:
                LiveLabRawMixin._cleanup_preview_result_path(result)
                return
            if current_request.kind == "selected" and self._current_pending_raw_capture() is not capture:
                LiveLabRawMixin._cleanup_preview_result_path(result)
                return
            if _pending_raw_source_signature(capture.source_path) != current_request.source_signature:
                LiveLabRawMixin._cleanup_preview_result_path(result)
                return
            if _pending_raw_settings_signature(capture.raw_settings) != current_request.settings_signature:
                LiveLabRawMixin._cleanup_preview_result_path(result)
                return

            if isinstance(result.raw_rgb, np.ndarray) and result.raw_rgb.size:
                cache = getattr(self, "_pending_raw_preview_proxy_cache", None)
                if cache is None:
                    cache = {}
                    self._pending_raw_preview_proxy_cache = cache
                cache_key = self._raw_preview_proxy_cache_key(capture.source_path, current_request.raw_settings)
                if cache_key is not None:
                    raw_rgb = np.ascontiguousarray(np.asarray(result.raw_rgb, dtype=np.float32))
                    cache[cache_key] = _RawPreviewCacheEntry(
                        raw_rgb=raw_rgb,
                        preview_rgb=np.ascontiguousarray(_resize_preview_rgb(raw_rgb).astype(np.float32, copy=False)),
                    )

            if result.preview_rgb is None:
                if current_request.kind != "selected":
                    capture.status = "failed"
                    return
                companion_jpeg_path = getattr(capture, "companion_jpeg_path", None)
                if companion_jpeg_path and Path(str(companion_jpeg_path)).exists():
                    try:
                        if self._ingest_detected_image(str(companion_jpeg_path)):
                            self._remove_pending_raw_capture(capture, status="discarded", refresh_ui=False)
                            self._update_pending_raw_controls()
                            return
                    except Exception:
                        pass
                capture.status = "failed"
                error_text = result.error or self.tr("RAW preview failed.")
                self._show_status(
                    self.tr("RAW preview unavailable for {name}: {error}").format(name=capture.source_path.name, error=error_text),
                    tone="warning",
                    timeout_ms=6000,
                )
                return

            capture.preview_rgb = result.preview_rgb
            capture.rendered_settings = capture.raw_settings
            capture.auto_level_settings = result.auto_level_settings
            preview_path = result.preview_path or str(capture.preview_path or "")
            if not preview_path:
                preview_dir = self._pending_raw_preview_dir()
                preview_dir.mkdir(parents=True, exist_ok=True)
                preview_path_obj = preview_dir / f"{capture.source_path.stem}_{uuid4().hex}.jpg"
                capture.preview_path = preview_path_obj
                preview_path = str(preview_path_obj)
            else:
                capture.preview_path = Path(preview_path)
            if result.curve is not None:
                self._raw_preview_last_curve = result.curve
                curve_settings = result.processed_settings or capture.raw_settings
                self._raw_preview_last_curve_signature = LiveLabRawMixin._raw_curve_signature_for_source(
                    capture.source_path,
                    curve_settings,
                )
            capture.status = "pending"
            if current_request.kind == "selected":
                self._sync_raw_processing_controls_from_settings(
                    capture.raw_settings,
                    auto_level_settings=result.auto_level_settings,
                )
                self._present_raw_preview(
                    pixmap=self._rgb_to_pixmap(result.preview_rgb),
                    image_path=preview_path,
                    image=None,
                    title=self.tr("Pending RAW {current} of {total}: {name}").format(
                        current=self._selected_pending_raw_index_value() + 1,
                        total=len(pending_captures),
                        name=capture.source_path.name,
                    ),
                    meta=self._raw_settings_info_text(capture.raw_settings),
                    preview_scaled=True,
                    preserve_view=True,
                )
                self._prune_pending_raw_preview_buffers(capture)
            gallery = getattr(self, "session_gallery", None)
            if gallery is not None:
                invalidate_gallery = getattr(gallery, "invalidate_pixmap_cache", None)
                if callable(invalidate_gallery):
                    invalidate_gallery(capture.preview_path)
            if current_request.kind == "selected":
                self._update_pending_raw_controls()
                self._refresh_raw_processing_context_ui()
            self._refresh_session_gallery()
        finally:
            pass

    def _finish_pending_raw_preview_job(self) -> None:
        self._pending_raw_preview_worker = None
        self._pending_raw_preview_worker_thread = None
        self._pending_raw_preview_active_request = None
        queue = getattr(self, "_pending_raw_preview_job_queue", None)
        next_request = queue[0] if queue else None
        if not (isinstance(next_request, _PendingRawPreviewJobRequest) and next_request.kind == "selected"):
            busy_setter = getattr(self, "_set_pending_raw_preview_busy", None)
            if callable(busy_setter):
                busy_setter(False, "")
        self._start_next_pending_raw_preview_job()

    def _schedule_pending_raw_preview_refresh(self) -> None:
        timer = getattr(self, "_pending_raw_preview_timer", None)
        if isinstance(timer, QTimer):
            timer.stop()
            timer.start()

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
        resolved_lab_metadata = merge_image_lab_metadata(self._current_lab_metadata(), lab_metadata)
        resolved_lab_metadata["image_type"] = "microscope"
        resolved_settings = RawRenderSettings.from_dict(raw_settings or self._current_raw_render_settings())
        pending = PendingRawCapture(
            source_path=source,
            companion_jpeg_path=Path(companion_jpeg_path) if companion_jpeg_path else None,
            lab_metadata=resolved_lab_metadata,
            raw_settings=resolved_settings,
            group_key=group_key,
            observation_id=int(self._session_observation_id or 0) or None,
            created_at=datetime.now(),
        )
        preview_dir = self._pending_raw_preview_dir()
        preview_dir.mkdir(parents=True, exist_ok=True)
        pending.preview_path = preview_dir / f"{source.stem}_{uuid4().hex}.jpg"
        return pending

    def _add_pending_raw_capture(self, pending: PendingRawCapture) -> None:
        captures = getattr(self, "_pending_raw_captures", [])
        captures.append(pending)
        self._pending_raw_captures = captures
        self._selected_pending_raw_index = len(captures) - 1
        # Save all is a draining batch: if another RAW capture finishes
        # arriving while earlier files render, include it in the same batch.
        if (
            getattr(self, "_pending_raw_commit_batch_mode", None) == "save_all"
            and self._pending_raw_commit_is_busy()
            and not bool(getattr(self, "_pending_raw_commit_cancel_requested", False))
        ):
            request = self._pending_raw_commit_request_for_capture(pending, kind="save_all")
            if request is not None:
                if bool(getattr(self, "_session_stop_pending", False)):
                    self._session_stop_processed_raw_capture_ids.add(id(pending))
                self._pending_raw_commit_batch_total = int(self._pending_raw_commit_batch_total or 0) + 1
                self._enqueue_pending_raw_commit_job(request)
        # A fresh pending RAW arriving ends any "select to edit" multi-
        # selection so the user's next click targets the new capture, not
        # the batch they were reviewing.
        gallery = getattr(self, "session_gallery", None)
        if gallery is not None:
            exit_edit = getattr(gallery, "exit_edit_selection", None)
            if callable(exit_edit):
                exit_edit()
        # Ask the next gallery refresh (triggered from _show_pending_raw_capture
        # via _update_pending_raw_controls) to scroll so the fresh capture
        # lands on the right edge of the strip.
        self._pending_reveal_new_at_end = True
        self._show_pending_raw_capture(self._selected_pending_raw_index)
        queue_helper = getattr(self, "_queue_background_pending_raw_renders", None)
        if callable(queue_helper):
            queue_helper()

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
        raw_settings = RawRenderSettings.from_dict(pending.raw_settings)
        if bool(raw_settings.auto_levels) and pending.auto_level_settings is not None:
            raw_settings = RawRenderSettings.from_dict(pending.auto_level_settings)
        try:
            observation_id = int(getattr(pending, "observation_id", 0) or self._session_observation_id or 0)
            committed = self._ingest_detected_image(
                str(pending.source_path),
                raw_settings=raw_settings,
                lab_metadata=pending.lab_metadata,
                observation_id=observation_id,
            )
        except Exception:
            committed = False
        if committed:
            pending.status = "committed"
        return committed

    def _pending_raw_commit_request_for_capture(
        self,
        pending: PendingRawCapture | None,
        *,
        kind: str = "save_current",
    ) -> _PendingRawCommitJobRequest | None:
        if pending is None:
            return None
        job_id = int(getattr(self, "_pending_raw_commit_job_counter", 0)) + 1
        self._pending_raw_commit_job_counter = job_id
        source_path = Path(pending.source_path)
        source_signature = _pending_raw_source_signature(source_path)
        raw_settings = RawRenderSettings.from_dict(pending.raw_settings)
        if bool(raw_settings.auto_levels) and pending.auto_level_settings is not None:
            raw_settings = RawRenderSettings.from_dict(pending.auto_level_settings)
        lab_metadata = copy.deepcopy(getattr(pending, "lab_metadata", None) or {})
        observation_id = int(getattr(pending, "observation_id", 0) or getattr(self, "_session_observation_id", 0) or 0)
        request_kind = "save_all" if str(kind or "").strip().lower() == "save_all" else "save_current"
        return _PendingRawCommitJobRequest(
            job_id=job_id,
            kind=request_kind,
            capture=pending,
            source_path=source_path,
            raw_settings=raw_settings,
            lab_metadata=lab_metadata,
            observation_id=observation_id,
            source_signature=source_signature,
        )

    def _pending_raw_commit_begin_batch(self, *, kind: str, total: int) -> None:
        self._pending_raw_commit_batch_mode = "save_all" if str(kind or "").strip().lower() == "save_all" else "save_current"
        self._pending_raw_commit_batch_total = max(0, int(total))
        self._pending_raw_commit_batch_completed = 0
        self._pending_raw_commit_batch_saved = 0
        self._pending_raw_commit_batch_failed = 0
        self._pending_raw_commit_batch_skipped = 0
        self._pending_raw_commit_cancel_requested = False

    def _cancel_pending_raw_commit_jobs(self) -> None:
        if bool(getattr(self, "_session_stop_pending", False)):
            return
        if not self._pending_raw_commit_is_busy():
            return
        self._pending_raw_commit_cancel_requested = True
        cancel_btn = getattr(self, "pending_raw_commit_cancel_btn", None)
        if cancel_btn is not None:
            cancel_btn.setEnabled(False)
        queue = getattr(self, "_pending_raw_commit_job_queue", None)
        if getattr(self, "_pending_raw_commit_worker_thread", None) is None:
            skipped = len(list(queue or []))
            if isinstance(queue, list):
                queue.clear()
            self._pending_raw_commit_batch_completed += skipped
            self._pending_raw_commit_batch_skipped += skipped
            self._finish_pending_raw_commit_job()

    def _enqueue_pending_raw_commit_job(self, request: _PendingRawCommitJobRequest | None) -> None:
        if request is None:
            return
        queue = getattr(self, "_pending_raw_commit_job_queue", None)
        if queue is None:
            queue = []
            self._pending_raw_commit_job_queue = queue
        active = getattr(self, "_pending_raw_commit_active_request", None)
        if isinstance(active, _PendingRawCommitJobRequest) and active.job_id == request.job_id:
            return
        if request.kind == "save_current":
            queue[:] = [
                queued
                for queued in queue
                if not (
                    isinstance(queued, _PendingRawCommitJobRequest)
                    and (
                        queued.kind == "save_current"
                        or (queued.kind == "save_all" and queued.capture is request.capture)
                    )
                )
            ]
            queue.insert(0, request)
        else:
            if self._pending_raw_commit_has_request_for_capture(request.capture):
                return
            queue.append(request)
        if not bool(getattr(self, "_pending_raw_commit_defer_start", False)):
            self._update_pending_raw_controls()
        if self._pending_raw_commit_active_worker_count() < PENDING_RAW_COMMIT_MAX_WORKERS and not bool(
            getattr(self, "_pending_raw_commit_defer_start", False)
        ):
            self._start_next_pending_raw_commit_job()

    def _start_next_pending_raw_commit_job(self) -> None:
        if self._pending_raw_commit_active_worker_count() >= PENDING_RAW_COMMIT_MAX_WORKERS:
            return
        queue = getattr(self, "_pending_raw_commit_job_queue", None)
        if bool(getattr(self, "_pending_raw_commit_cancel_requested", False)):
            skipped = len(list(queue or []))
            if isinstance(queue, list):
                queue.clear()
            self._pending_raw_commit_batch_completed += skipped
            self._pending_raw_commit_batch_skipped += skipped
            self._finish_pending_raw_commit_job()
            return
        if not queue:
            self._finish_pending_raw_commit_job()
            return
        while queue:
            request = queue.pop(0)
            if not isinstance(request, _PendingRawCommitJobRequest):
                continue
            if request.capture not in list(getattr(self, "_pending_raw_captures", []) or []):
                self._pending_raw_commit_batch_completed += 1
                self._pending_raw_commit_batch_skipped += 1
                continue
            if _pending_raw_source_signature(request.capture.source_path) != request.source_signature:
                self._pending_raw_commit_batch_completed += 1
                self._pending_raw_commit_batch_skipped += 1
                continue
            self._start_pending_raw_commit_job(request)
            return
        self._finish_pending_raw_commit_job()

    def _start_pending_raw_commit_job(self, request: _PendingRawCommitJobRequest) -> None:
        self._pending_raw_commit_active_request = request
        batch_mode = self._pending_raw_commit_batch_mode or request.kind
        total = int(self._pending_raw_commit_batch_total or 0)
        if batch_mode == "save_all" and total > 0:
            current = min(int(self._pending_raw_commit_batch_completed) + 1, total)
            completed = max(0, min(int(self._pending_raw_commit_batch_completed), total))
            self._set_pending_raw_commit_busy(
                True,
                self.tr("Saving RAW {current} of {total}: {name}").format(
                    current=current,
                    total=total,
                    name=request.source_path.name,
                ),
                value=completed,
                maximum=total,
            )
        else:
            self._set_pending_raw_commit_busy(
                True,
                self.tr("Saving RAW: {name}").format(name=request.source_path.name),
            )
        if batch_mode != "save_all":
            self._update_pending_raw_controls()
        thread = QThread(self)
        worker = _PendingRawCommitWorker(request)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.finished.connect(self._on_pending_raw_commit_job_finished)
        worker.finished.connect(thread.quit)
        worker.finished.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        thread.finished.connect(self._finish_pending_raw_commit_job)
        self._pending_raw_commit_worker_thread = thread
        self._pending_raw_commit_worker = worker
        thread.start()
        QApplication.processEvents()

    def _on_pending_raw_commit_job_finished(self, result: object) -> None:
        thread = getattr(self, "_pending_raw_commit_worker_thread", None)
        try:
            request = getattr(self, "_pending_raw_commit_active_request", None)
            if not isinstance(request, _PendingRawCommitJobRequest):
                return
            if not isinstance(result, _PendingRawCommitJobResult):
                return
            if result.job_id != request.job_id:
                return

            pending_captures = list(getattr(self, "_pending_raw_captures", []) or [])
            capture = request.capture
            stale_request = capture not in pending_captures
            if not stale_request:
                try:
                    stale_request = _pending_raw_source_signature(capture.source_path) != request.source_signature
                except Exception:
                    stale_request = True

            if stale_request:
                self._pending_raw_commit_batch_completed += 1
                self._pending_raw_commit_batch_skipped += 1
                LiveLabRawMixin._cleanup_local_ingest_result(result.prepared_ingest)
                return

            if result.prepared_ingest is None:
                self._pending_raw_commit_batch_completed += 1
                self._pending_raw_commit_batch_failed += 1
                capture.status = "failed"
                if result.error:
                    self._show_status(
                        self.tr("Could not save pending RAW capture {name}: {error}").format(
                            name=capture.source_path.name,
                            error=result.error,
                        ),
                        tone="warning",
                        timeout_ms=6000,
                    )
                return

            finalize_helper = getattr(self, "_finalize_local_ingest", None)
            try:
                if callable(finalize_helper):
                    committed = finalize_helper(
                        str(request.source_path),
                        result.prepared_ingest,
                        raw_settings=request.raw_settings,
                        lab_metadata=request.lab_metadata,
                        capture_time_state=request.lab_metadata,
                        observation_id=request.observation_id,
                    )
                else:
                    committed = _live_lab_tab_class()._finalize_local_ingest(
                        self,
                        str(request.source_path),
                        result.prepared_ingest,
                        raw_settings=request.raw_settings,
                        lab_metadata=request.lab_metadata,
                        capture_time_state=request.lab_metadata,
                        observation_id=request.observation_id,
                    )
            except Exception as exc:
                committed = False
                capture.status = "failed"
                self._pending_raw_commit_batch_completed += 1
                self._pending_raw_commit_batch_failed += 1
                LiveLabRawMixin._cleanup_local_ingest_result(result.prepared_ingest)
                self._show_status(
                    self.tr("Could not save pending RAW capture {name}: {error}").format(
                        name=capture.source_path.name,
                        error=str(exc),
                    ),
                    tone="warning",
                    timeout_ms=6000,
                )
                return

            self._pending_raw_commit_batch_completed += 1
            if committed:
                self._pending_raw_commit_batch_saved += 1
                if request.kind == "save_current":
                    self._remove_pending_raw_capture(capture, status="committed", refresh_ui=True)
                else:
                    self._remove_pending_raw_capture(capture, status="committed", refresh_ui=False)
                return

            self._pending_raw_commit_batch_failed += 1
            LiveLabRawMixin._cleanup_local_ingest_result(result.prepared_ingest)
            capture.status = "failed"
            error_text = result.error or self.tr("RAW save failed.")
            self._show_status(
                self.tr("Could not save pending RAW capture {name}: {error}").format(
                    name=capture.source_path.name,
                    error=error_text,
                ),
                tone="warning",
                timeout_ms=6000,
            )
        finally:
            pass

    def _finish_pending_raw_commit_job(self) -> None:
        active_request = getattr(self, "_pending_raw_commit_active_request", None)
        self._pending_raw_commit_worker = None
        self._pending_raw_commit_worker_thread = None
        self._pending_raw_commit_active_request = None
        queue = getattr(self, "_pending_raw_commit_job_queue", None)
        cancel_requested = bool(getattr(self, "_pending_raw_commit_cancel_requested", False))
        if cancel_requested and queue:
            skipped = len(list(queue or []))
            if isinstance(queue, list):
                queue.clear()
            self._pending_raw_commit_batch_completed += skipped
            self._pending_raw_commit_batch_skipped += skipped
        if queue:
            if self._pending_raw_commit_batch_mode != "save_all":
                self._update_pending_raw_controls()
            self._start_next_pending_raw_commit_job()
            return

        batch_mode = self._pending_raw_commit_batch_mode
        self._set_pending_raw_commit_busy(False, "")
        if batch_mode == "save_all":
            self._sync_pending_raw_commit_batch_ui()
        else:
            self._update_pending_raw_controls()
        total = int(self._pending_raw_commit_batch_total or 0)
        saved = int(self._pending_raw_commit_batch_saved or 0)
        failed = int(self._pending_raw_commit_batch_failed or 0)
        skipped = int(getattr(self, "_pending_raw_commit_batch_skipped", 0) or 0)
        if cancel_requested:
            message = self.tr("RAW save cancelled. Saved {saved}, skipped {skipped}.").format(
                saved=saved,
                skipped=skipped,
            )
            self._show_status(message, tone="warning", timeout_ms=5000)
        elif batch_mode == "save_all":
            message = self.tr("Saved {saved} RAW captures.").format(saved=saved)
            tone = "success"
            timeout_ms = 3500
            if failed and skipped:
                message = self.tr("Saved {saved} RAW captures, {failed} failed, {skipped} skipped.").format(
                    saved=saved,
                    failed=failed,
                    skipped=skipped,
                )
                tone = "warning"
                timeout_ms = 5000
            elif failed:
                message = self.tr("Saved {saved} RAW captures, {failed} failed.").format(saved=saved, failed=failed)
                tone = "warning"
                timeout_ms = 5000
            elif skipped:
                message = self.tr("Saved {saved} RAW captures, {skipped} skipped.").format(saved=saved, skipped=skipped)
                tone = "warning"
                timeout_ms = 5000
            self._show_status(
                message,
                tone=tone,
                timeout_ms=timeout_ms,
            )
        elif batch_mode == "save_current":
            if saved:
                name = None
                if isinstance(active_request, _PendingRawCommitJobRequest):
                    name = str(active_request.capture.source_path.name or "").strip() or None
                if not name:
                    pending = self._current_pending_raw_capture()
                    name = pending.source_path.name if pending is not None else self.tr("selected capture")
                self._show_status(
                    self.tr("Saved pending RAW capture {name}.").format(name=name),
                    tone="success",
                    timeout_ms=3500,
                )
        self._pending_raw_commit_batch_mode = None
        self._pending_raw_commit_batch_total = 0
        self._pending_raw_commit_batch_completed = 0
        self._pending_raw_commit_batch_saved = 0
        self._pending_raw_commit_batch_failed = 0
        self._pending_raw_commit_batch_skipped = 0
        self._pending_raw_commit_cancel_requested = False
        if bool(getattr(self, "_session_stop_pending", False)):
            self._continue_session_stop_after_raw_saves()

    def _sync_pending_raw_commit_batch_ui(self) -> None:
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        observation_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        self._update_observation_thumbnail()
        _raw_timing_log("RAW batch UI observation thumbnail", observation_start)
        controls_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        self._update_pending_raw_controls()
        _raw_timing_log("RAW batch UI pending controls", controls_start)
        selected_id = self._selected_session_image_id
        if selected_id is not None:
            show_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            self._show_session_image(int(selected_id))
            _raw_timing_log("RAW batch UI show selected image", show_start, detail=f"image_id={selected_id}")
        elif self._session_image_ids:
            selected_id = int(self._session_image_ids[-1])
            self._selected_session_image_id = selected_id
            show_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            self._show_session_image(selected_id)
            _raw_timing_log("RAW batch UI show fallback image", show_start, detail=f"image_id={selected_id}")
        else:
            clear_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            self._clear_session_viewer(
                title=self.tr("Waiting for first import"),
                meta=self.tr("New microscope captures from the watched folder will appear here automatically."),
            )
            _raw_timing_log("RAW batch UI clear viewer", clear_start)
        controls_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        self._update_session_controls()
        _raw_timing_log("RAW batch UI session controls", controls_start)
        if selected_id is not None:
            main_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
            self._refresh_main_window_after_import(int(selected_id))
            _raw_timing_log("RAW batch UI main window refresh", main_start, detail=f"image_id={selected_id}")
        _raw_timing_log("RAW batch UI sync", start)

    def _remove_pending_raw_capture(self, pending: PendingRawCapture, *, status: str, refresh_ui: bool = True) -> bool:
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        captures = getattr(self, "_pending_raw_captures", [])
        if pending not in captures:
            return False
        index = captures.index(pending)
        preview_path = Path(pending.preview_path) if pending.preview_path else None
        pending.status = status
        if preview_path is not None:
            try:
                preview_start = time.perf_counter() if _RAW_DEBUG_TIMING else None
                preview_path.unlink(missing_ok=True)
                _raw_timing_log("pending RAW capture preview unlink", preview_start, detail=preview_path.name)
            except Exception:
                pass
        captures.pop(index)
        self._pending_raw_captures = captures
        evict = getattr(self, "_evict_raw_preview_proxy_cache", None)
        if callable(evict):
            evict(pending.source_path)
        if not refresh_ui:
            if captures:
                self._selected_pending_raw_index = min(index, len(captures) - 1)
            else:
                self._selected_pending_raw_index = -1
            _raw_timing_log("remove pending RAW capture", start, detail=f"{pending.source_path.name} refresh=False")
            return True
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
        _raw_timing_log("remove pending RAW capture", start, detail=f"{pending.source_path.name} refresh=True")
        return True

    def _commit_selected_pending_raw_capture(self) -> bool:
        pending = self._current_pending_raw_capture()
        if pending is None:
            return False
        self._cancel_pending_raw_background_wb_selection()
        if self._pending_raw_commit_is_busy():
            return False
        request = self._pending_raw_commit_request_for_capture(pending, kind="save_current")
        if request is None:
            return False
        self._pending_raw_commit_begin_batch(kind="save_current", total=1)
        self._enqueue_pending_raw_commit_job(request)
        return True

    def _commit_all_pending_raw_captures(self) -> bool:
        captures = list(getattr(self, "_pending_raw_captures", []) or [])
        return self._commit_pending_raw_capture_batch(captures)

    def _commit_pending_raw_capture_batch(self, captures: list[PendingRawCapture]) -> bool:
        if not captures:
            return False
        if self._pending_raw_commit_is_busy():
            return False
        requests: list[_PendingRawCommitJobRequest] = []
        for pending in captures:
            request = self._pending_raw_commit_request_for_capture(pending, kind="save_all")
            if request is not None:
                requests.append(request)
        if not requests:
            return False
        self._pending_raw_commit_begin_batch(kind="save_all", total=len(requests))
        self._pending_raw_commit_defer_start = True
        try:
            for request in requests:
                self._enqueue_pending_raw_commit_job(request)
        finally:
            self._pending_raw_commit_defer_start = False
        self._update_pending_raw_controls()
        if self._pending_raw_commit_active_worker_count() < PENDING_RAW_COMMIT_MAX_WORKERS:
            self._start_next_pending_raw_commit_job()
        return True

    def _discard_selected_pending_raw_capture(self) -> bool:
        pending = self._current_pending_raw_capture()
        if pending is None:
            return False
        if self._pending_raw_commit_capture_is_busy(pending):
            self._show_status(
                self.tr("Wait for RAW saving to finish before removing this capture."),
                tone="info",
                timeout_ms=3000,
            )
            return False
        self._cancel_pending_raw_background_wb_selection()
        if self._remove_pending_raw_capture(pending, status="discarded", refresh_ui=True):
            self._show_status(
                self.tr("Removed pending RAW capture {name}.").format(name=pending.source_path.name),
                tone="info",
                timeout_ms=3500,
            )
            return True
        return False

    def _delete_current_raw_review_item(self) -> bool:
        start = time.perf_counter() if _RAW_DEBUG_TIMING else None
        if self._raw_edit_session is not None:
            return False

        pending = self._current_pending_raw_capture()
        if pending is not None:
            return self._discard_selected_pending_raw_capture()

        image_ids = self._selected_committed_session_image_ids_for_delete()
        if not image_ids:
            return False
        deleted = self._delete_committed_session_images(image_ids)
        _raw_timing_log("delete current RAW review item", start, detail=f"images={len(image_ids)} deleted={deleted}")
        return deleted

    def _copy_selected_pending_raw_settings(self) -> None:
        capture = self._current_pending_raw_capture()
        if capture is None:
            return
        settings = RawRenderSettings.from_dict(capture.raw_settings)
        self._raw_copied_settings = self._raw_settings_for_copy(settings)
        self._pending_raw_copy_source_path = str(capture.source_path)
        self._update_pending_raw_controls()
        self._show_status(
            self.tr("Copied RAW settings from {name}.").format(name=capture.source_path.name),
            tone="success",
            timeout_ms=2500,
        )

    def _paste_copied_settings_to_selected_pending(self) -> None:
        copied = getattr(self, "_raw_copied_settings", None)
        if copied is None:
            return
        capture = self._current_pending_raw_capture()
        if capture is None:
            return
        target_settings = RawRenderSettings.from_dict(capture.raw_settings)
        pasted = self._pending_raw_settings_for_copy(RawRenderSettings.from_dict(copied))
        # WB stays with the target image; only tone/levels and other RAW
        # processing parameters come from the copied capture.
        pasted = replace(
            pasted,
            white_balance_mode=target_settings.white_balance_mode,
            wb_multipliers=target_settings.wb_multipliers,
            wb_multiplier_space=target_settings.wb_multiplier_space,
            wb_sample_point=target_settings.wb_sample_point,
            wb_sample_size=target_settings.wb_sample_size,
            wb_sample_base_mode=target_settings.wb_sample_base_mode,
            wb_selection=target_settings.wb_selection,
            wb_selection_space=target_settings.wb_selection_space,
        )
        capture.raw_settings = pasted
        capture.auto_level_settings = None
        capture.rendered_settings = None
        capture.preview_rgb = None
        capture.status = "pending"
        self._sync_raw_processing_controls_from_settings(pasted)
        self._refresh_selected_pending_raw_preview()
        self._update_pending_raw_controls()
        self._show_status(
            self.tr("Pasted RAW settings to {name}.").format(name=capture.source_path.name),
            tone="success",
            timeout_ms=2500,
        )
