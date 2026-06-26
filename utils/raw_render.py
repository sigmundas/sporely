"""RAW render settings and rawpy-backed rendering helpers."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
import math
import os
from pathlib import Path
from typing import Any, Mapping

import numpy as np
from PIL import Image

from utils.image_processing_pipeline import (
    apply_auto_levels_from_bounds,
    apply_post_decode_processing,
    compute_auto_level_bounds,
    to_float_rgb,
)
from utils.raw_detection import raw_mime_type_for_path
from utils.raw_white_balance import estimate_white_balance_from_background
from utils.rawpy_import import import_rawpy, read_rawpy_capture_datetime

RAW_DERIVATIVE_FORMAT = "jpeg"
RAW_DERIVATIVE_MIME_TYPE = "image/jpeg"
RAW_DERIVATIVE_QUALITY = 95
RAW_DERIVATIVE_SUBSAMPLING = 0
RAW_DERIVATIVE_OPTIMIZE = True
RAW_PREVIEW_MAX_DIM = 1600
_EPSILON = np.finfo(np.float64).eps


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "on", "enabled"}


def _coerce_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _coerce_float_in_range(value: Any, default: float, minimum: float, maximum: float) -> float:
    coerced = _coerce_float(value, default)
    if not np.isfinite(coerced):
        return float(default)
    return float(np.clip(coerced, float(minimum), float(maximum)))


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _coerce_float_tuple(value: Any, length: int) -> tuple[float, ...] | None:
    if value is None:
        return None
    if isinstance(value, tuple):
        items = list(value)
    elif isinstance(value, list):
        items = list(value)
    else:
        return None
    if len(items) != length:
        return None
    try:
        return tuple(float(item) for item in items)
    except Exception:
        return None


def _coerce_point_tuple(value: Any, length: int) -> tuple[float, ...] | None:
    if value is None:
        return None
    if isinstance(value, tuple):
        items = list(value)
    elif isinstance(value, list):
        items = list(value)
    else:
        return None
    if len(items) != length:
        return None
    try:
        return tuple(float(item) for item in items)
    except Exception:
        return None


def _normalize_white_balance_mode(mode: Any, *, wb_multipliers: tuple[float, float, float] | None = None) -> str:
    normalized = str(mode or "camera").strip().lower() or "camera"
    if normalized in {"background", "user"}:
        return "custom"
    if normalized == "camera" and wb_multipliers is not None:
        return "custom"
    return normalized


def _normalize_white_balance_multiplier_space(
    value: Any,
    *,
    wb_multipliers: tuple[float, float, float] | None = None,
) -> str | None:
    normalized = str(value or "").strip().lower() or None
    if normalized == "post_decode_rgb":
        return normalized
    if wb_multipliers is not None:
        return "post_decode_rgb"
    return normalized


def _normalize_wb_sample_base_mode(
    value: Any,
    *,
    white_balance_mode: str,
    wb_multipliers: tuple[float, float, float] | None = None,
) -> str | None:
    normalized = str(value or "").strip().lower() or None
    if normalized in {"camera", "auto"}:
        return normalized
    if wb_multipliers is None:
        return None
    mode = str(white_balance_mode or "camera").strip().lower() or "camera"
    if mode == "auto":
        return "auto"
    return "camera"


def _format_capture_datetime(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        normalized = value
        if normalized.tzinfo is not None:
            normalized = normalized.astimezone().replace(tzinfo=None)
        return normalized.strftime("%Y:%m:%d %H:%M:%S")
    text = str(value).strip()
    return text or None


@dataclass(frozen=True, slots=True, init=False)
class RawRenderSettings:
    """Serializable RAW rendering parameters."""

    white_balance_mode: str = "camera"
    wb_multipliers: tuple[float, float, float] | None = None
    wb_selection: tuple[float, float, float, float] | None = None
    wb_multiplier_space: str | None = None
    wb_sample_point: tuple[float, float] | None = None
    wb_sample_size: int | None = None
    wb_sample_base_mode: str | None = None
    wb_selection_space: str | None = None
    exposure_ev: float = 0.0
    light_ev: float = 0.0
    dark_ev: float = 0.0
    auto_levels: bool = True
    black_percentile: float = 0.0
    white_percentile: float = 1.0
    auto_levels_strength: float = 1.0
    auto_levels_soft_tails: bool = False
    auto_levels_tail_size: float = 0.03
    auto_levels_shadow_lift: float = 0.0
    tone_curve_enabled: bool = False
    tone_curve_strength: float = 0.5
    tone_curve_midpoint: float = 0.5
    tone_shadows: float = 0.0
    tone_highlights: float = 0.0
    output_bps: int = 16

    def __init__(
        self,
        white_balance_mode: str = "camera",
        wb_multipliers: tuple[float, float, float] | None = None,
        wb_selection: tuple[float, float, float, float] | None = None,
        wb_multiplier_space: str | None = None,
        wb_sample_point: tuple[float, float] | None = None,
        wb_sample_size: int | None = None,
        wb_sample_base_mode: str | None = None,
        wb_selection_space: str | None = None,
        exposure_ev: float = 0.0,
        light_ev: float | None = None,
        dark_ev: float | None = None,
        auto_levels: bool = True,
        black_percentile: float = 0.0,
        white_percentile: float = 1.0,
        auto_levels_strength: float = 1.0,
        auto_levels_soft_tails: bool = False,
        auto_levels_tail_size: float = 0.03,
        auto_levels_shadow_lift: float = 0.0,
        shadow_lift: float | None = None,
        tone_curve_enabled: bool = False,
        tone_curve_strength: float = 0.5,
        tone_curve_midpoint: float = 0.5,
        tone_shadows: float = 0.0,
        tone_highlights: float = 0.0,
        output_bps: int = 16,
    ) -> None:
        resolved_shadow_lift = auto_levels_shadow_lift if shadow_lift is None else shadow_lift
        if light_ev is None and dark_ev is None:
            resolved_light_ev = max(0.0, float(exposure_ev))
            resolved_dark_ev = min(0.0, float(exposure_ev))
            resolved_exposure_ev = float(exposure_ev)
        else:
            resolved_light_ev = 0.0 if light_ev is None else float(light_ev)
            resolved_dark_ev = 0.0 if dark_ev is None else float(dark_ev)
            resolved_exposure_ev = resolved_light_ev + resolved_dark_ev
        object.__setattr__(self, "white_balance_mode", str(white_balance_mode or "camera"))
        object.__setattr__(self, "wb_multipliers", wb_multipliers)
        object.__setattr__(self, "wb_selection", wb_selection)
        object.__setattr__(self, "wb_multiplier_space", wb_multiplier_space)
        object.__setattr__(self, "wb_sample_point", wb_sample_point)
        object.__setattr__(self, "wb_sample_size", wb_sample_size)
        object.__setattr__(self, "wb_sample_base_mode", wb_sample_base_mode)
        object.__setattr__(self, "wb_selection_space", wb_selection_space)
        object.__setattr__(self, "exposure_ev", _coerce_float_in_range(resolved_exposure_ev, 0.0, -2.0, 2.0))
        object.__setattr__(self, "light_ev", _coerce_float_in_range(resolved_light_ev, 0.0, 0.0, 2.0))
        object.__setattr__(self, "dark_ev", _coerce_float_in_range(resolved_dark_ev, 0.0, -2.0, 0.0))
        object.__setattr__(self, "auto_levels", bool(auto_levels))
        object.__setattr__(self, "black_percentile", _coerce_float_in_range(black_percentile, 0.0, 0.0, 1.0))
        object.__setattr__(self, "white_percentile", _coerce_float_in_range(white_percentile, 1.0, 0.0, 1.0))
        object.__setattr__(self, "auto_levels_strength", _coerce_float_in_range(auto_levels_strength, 1.0, 0.0, 1.0))
        object.__setattr__(self, "auto_levels_soft_tails", bool(auto_levels_soft_tails))
        object.__setattr__(self, "auto_levels_tail_size", _coerce_float_in_range(auto_levels_tail_size, 0.03, 0.0, 0.5))
        object.__setattr__(self, "auto_levels_shadow_lift", _coerce_float_in_range(resolved_shadow_lift, 0.0, 0.0, 0.10))
        object.__setattr__(self, "tone_curve_enabled", bool(tone_curve_enabled))
        object.__setattr__(self, "tone_curve_strength", _coerce_float_in_range(tone_curve_strength, 0.5, 0.0, 1.0))
        object.__setattr__(self, "tone_curve_midpoint", _coerce_float_in_range(tone_curve_midpoint, 0.5, 0.0, 1.0))
        object.__setattr__(self, "tone_shadows", _coerce_float_in_range(tone_shadows, 0.0, -1.0, 1.0))
        object.__setattr__(self, "tone_highlights", _coerce_float_in_range(tone_highlights, 0.0, -1.0, 1.0))
        object.__setattr__(self, "output_bps", _coerce_int(output_bps, 16))

    @classmethod
    def default(cls) -> "RawRenderSettings":
        return cls()

    @property
    def shadow_lift(self) -> float:
        return float(self.auto_levels_shadow_lift)

    def to_dict(self) -> dict[str, Any]:
        return {
            "white_balance_mode": self.white_balance_mode,
            "wb_multipliers": list(self.wb_multipliers) if self.wb_multipliers is not None else None,
            "wb_selection": list(self.wb_selection) if self.wb_selection is not None else None,
            "wb_multiplier_space": self.wb_multiplier_space,
            "wb_sample_point": list(self.wb_sample_point) if self.wb_sample_point is not None else None,
            "wb_sample_size": int(self.wb_sample_size) if self.wb_sample_size is not None else None,
            "wb_sample_base_mode": self.wb_sample_base_mode,
            "wb_selection_space": self.wb_selection_space,
            "exposure_ev": float(self.light_ev + self.dark_ev),
            "light_ev": float(self.light_ev),
            "dark_ev": float(self.dark_ev),
            "auto_levels": bool(self.auto_levels),
            "black_percentile": float(self.black_percentile),
            "white_percentile": float(self.white_percentile),
            "auto_levels_strength": float(self.auto_levels_strength),
            "auto_levels_soft_tails": bool(self.auto_levels_soft_tails),
            "auto_levels_tail_size": float(self.auto_levels_tail_size),
            "auto_levels_shadow_lift": float(self.auto_levels_shadow_lift),
            "shadow_lift": float(self.shadow_lift),
            "tone_curve_enabled": bool(self.tone_curve_enabled),
            "tone_curve_strength": float(self.tone_curve_strength),
            "tone_curve_midpoint": float(self.tone_curve_midpoint),
            "tone_shadows": float(self.tone_shadows),
            "tone_highlights": float(self.tone_highlights),
            "output_bps": int(self.output_bps),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any] | dict[str, Any] | None) -> "RawRenderSettings":
        if value is None:
            return cls.default()
        if isinstance(value, cls):
            mapping = dict(value.to_dict())
        else:
            mapping = dict(value) if isinstance(value, Mapping) else {}
        wb_multipliers = _coerce_float_tuple(mapping.get("wb_multipliers"), 3)
        wb_selection = _coerce_float_tuple(mapping.get("wb_selection"), 4)
        white_balance_mode = _normalize_white_balance_mode(mapping.get("white_balance_mode") or "camera", wb_multipliers=wb_multipliers)
        wb_multiplier_space = _normalize_white_balance_multiplier_space(
            mapping.get("wb_multiplier_space"),
            wb_multipliers=wb_multipliers,
        )
        wb_sample_point = _coerce_point_tuple(mapping.get("wb_sample_point"), 2)
        sample_size_raw = mapping.get("wb_sample_size")
        wb_sample_size = None
        if sample_size_raw is not None:
            try:
                sample_size = int(sample_size_raw)
            except Exception:
                sample_size = 0
            if sample_size > 0:
                wb_sample_size = sample_size
        wb_sample_base_mode = _normalize_wb_sample_base_mode(
            mapping.get("wb_sample_base_mode"),
            white_balance_mode=white_balance_mode,
            wb_multipliers=wb_multipliers,
        )
        wb_selection_space = str(mapping.get("wb_selection_space") or "").strip() or None
        shadow_lift_value = mapping.get("shadow_lift")
        if shadow_lift_value is None:
            shadow_lift_value = mapping.get("auto_levels_shadow_lift", 0.0)
        tone_shadows_value = mapping.get("tone_shadows")
        if tone_shadows_value is None:
            tone_shadows_value = mapping.get("shadows", 0.0)
        tone_highlights_value = mapping.get("tone_highlights")
        if tone_highlights_value is None:
            tone_highlights_value = mapping.get("highlights", 0.0)
        has_light_dark = "light_ev" in mapping or "dark_ev" in mapping
        light_ev_value = mapping.get("light_ev") if has_light_dark else None
        dark_ev_value = mapping.get("dark_ev") if has_light_dark else None
        if not has_light_dark:
            legacy_exposure_ev = _coerce_float_in_range(mapping.get("exposure_ev"), 0.0, -2.0, 2.0)
            light_ev_value = max(0.0, legacy_exposure_ev)
            dark_ev_value = min(0.0, legacy_exposure_ev)
        return cls(
            white_balance_mode=white_balance_mode,
            wb_multipliers=wb_multipliers,  # type: ignore[arg-type]
            wb_selection=wb_selection,  # type: ignore[arg-type]
            wb_multiplier_space=wb_multiplier_space,
            wb_sample_point=wb_sample_point,  # type: ignore[arg-type]
            wb_sample_size=wb_sample_size,
            wb_sample_base_mode=wb_sample_base_mode,
            wb_selection_space=wb_selection_space,
            exposure_ev=_coerce_float_in_range(mapping.get("exposure_ev"), 0.0, -2.0, 2.0),
            light_ev=_coerce_float_in_range(light_ev_value, 0.0, 0.0, 2.0),
            dark_ev=_coerce_float_in_range(dark_ev_value, 0.0, -2.0, 0.0),
            auto_levels=_coerce_bool(mapping.get("auto_levels"), True),
            black_percentile=_coerce_float(mapping.get("black_percentile"), 0.0),
            white_percentile=_coerce_float(mapping.get("white_percentile"), 1.0),
            auto_levels_strength=_coerce_float_in_range(mapping.get("auto_levels_strength"), 1.0, 0.0, 1.0),
            auto_levels_soft_tails=_coerce_bool(mapping.get("auto_levels_soft_tails"), False),
            auto_levels_tail_size=_coerce_float_in_range(mapping.get("auto_levels_tail_size"), 0.03, 0.0, 0.5),
            auto_levels_shadow_lift=_coerce_float_in_range(shadow_lift_value, 0.0, 0.0, 0.10),
            tone_curve_enabled=_coerce_bool(mapping.get("tone_curve_enabled"), False),
            tone_curve_strength=_coerce_float(mapping.get("tone_curve_strength"), 0.5),
            tone_curve_midpoint=_coerce_float(mapping.get("tone_curve_midpoint"), 0.5),
            tone_shadows=_coerce_float_in_range(tone_shadows_value, 0.0, -1.0, 1.0),
            tone_highlights=_coerce_float_in_range(tone_highlights_value, 0.0, -1.0, 1.0),
            output_bps=_coerce_int(mapping.get("output_bps"), 16),
        )


class RawRenderingUnavailableError(RuntimeError):
    """Raised when RAW rendering is requested before the renderer exists."""


def _unique_output_path(output_dir: Path, base_name: str, suffix: str) -> Path:
    output_path = output_dir / f"{base_name}{suffix}"
    counter = 1
    while output_path.exists():
        output_path = output_dir / f"{base_name}_{counter}{suffix}"
        counter += 1
    return output_path


def _coerce_user_wb(multipliers: tuple[float, float, float] | None) -> list[float] | None:
    if multipliers is None:
        return None
    if len(multipliers) != 3:
        return None
    try:
        red, green, blue = (float(value) for value in multipliers)
    except Exception:
        return None
    return [red, green, blue, green]


def _build_postprocess_kwargs(
    settings: RawRenderSettings,
    *,
    preview: bool = False,
    wb_mode: str | None = None,
    user_wb: list[float] | None = None,
) -> dict[str, Any]:
    mode = str(wb_mode or settings.white_balance_mode or "camera").strip().lower() or "camera"
    kwargs: dict[str, Any] = {
        "output_bps": int(settings.output_bps),
        "no_auto_bright": True,
    }
    if preview:
        kwargs["half_size"] = True

    if user_wb is not None:
        kwargs["use_camera_wb"] = False
        kwargs["use_auto_wb"] = False
        kwargs["user_wb"] = user_wb
        return kwargs

    if mode == "auto":
        kwargs["use_camera_wb"] = False
        kwargs["use_auto_wb"] = True
        return kwargs

    if mode == "user":
        user_wb = _coerce_user_wb(settings.wb_multipliers)
        if user_wb is not None:
            kwargs["use_camera_wb"] = False
            kwargs["use_auto_wb"] = False
            kwargs["user_wb"] = user_wb
            return kwargs

    kwargs["use_camera_wb"] = True
    kwargs["use_auto_wb"] = False
    return kwargs


def _resolve_preview_rawpy_mode(settings: RawRenderSettings) -> str:
    sample_base_mode = str(settings.wb_sample_base_mode or "").strip().lower() or None
    if sample_base_mode not in {"camera", "auto"}:
        sample_base_mode = "auto" if settings.white_balance_mode == "auto" else "camera"
    return sample_base_mode if sample_base_mode in {"camera", "auto"} else "camera"


def _to_float_rgb(rgb: Any) -> np.ndarray:
    return to_float_rgb(rgb)


def _apply_auto_levels(
    rgb: np.ndarray,
    black_percentile: float,
    white_percentile: float,
    *,
    shadow_lift: float = 0.0,
) -> np.ndarray:
    black_level, white_level = compute_auto_level_bounds(rgb, black_percentile, white_percentile)
    return apply_auto_levels_from_bounds(rgb, black_level, white_level, shadow_lift=shadow_lift)


def _auto_level_analysis_settings(settings: RawRenderSettings | Mapping[str, Any] | dict[str, Any] | None) -> RawRenderSettings:
    resolved = RawRenderSettings.from_dict(settings)
    return replace(
        resolved,
        exposure_ev=0.0,
        light_ev=0.0,
        dark_ev=0.0,
        auto_levels=False,
        black_percentile=0.0,
        white_percentile=1.0,
        auto_levels_strength=1.0,
        auto_levels_soft_tails=False,
        auto_levels_tail_size=0.03,
        auto_levels_shadow_lift=0.0,
        tone_curve_enabled=False,
        tone_shadows=0.0,
        tone_highlights=0.0,
    )


def apply_auto_level_bounds_to_settings(
    settings: RawRenderSettings | Mapping[str, Any] | dict[str, Any] | None,
    black_level: float | None,
    white_level: float | None,
) -> RawRenderSettings:
    """Return a RAW settings snapshot with auto levels snapped to the given bounds."""
    resolved = RawRenderSettings.from_dict(settings)
    if (
        black_level is None
        or white_level is None
        or not np.isfinite(float(black_level))
        or not np.isfinite(float(white_level))
    ):
        return resolved

    dark_point = float(np.clip(float(black_level), 0.0, 1.0))
    light_point = float(np.clip(float(white_level), 0.0, 1.0))
    if light_point <= dark_point + _EPSILON:
        return resolved

    dark_ev = float(np.log2(max(_EPSILON, 1.0 - dark_point)))
    light_ev = float(-math.log2(max(_EPSILON, light_point)))
    dark_ev = float(np.clip(dark_ev, -2.0, 0.0))
    light_ev = float(np.clip(light_ev, 0.0, 2.0))
    return replace(
        resolved,
        auto_levels=True,
        black_percentile=0.0,
        white_percentile=1.0,
        auto_levels_strength=1.0,
        auto_levels_soft_tails=False,
        auto_levels_tail_size=0.03,
        auto_levels_shadow_lift=0.0,
        exposure_ev=light_ev + dark_ev,
        light_ev=light_ev,
        dark_ev=dark_ev,
    )


def compute_auto_level_adjusted_settings_from_source(
    source_path: str | Path,
    *,
    settings: RawRenderSettings | Mapping[str, Any] | dict[str, Any] | None = None,
    black_percentile: float = 0.0,
    white_percentile: float = 1.0,
) -> RawRenderSettings:
    """Compute the auto-level-snapped settings for a RAW source preview."""
    resolved = RawRenderSettings.from_dict(settings)
    try:
        analysis_settings = _auto_level_analysis_settings(resolved)
        analysis_rgb = render_raw_preview_proxy_rgb(source_path, settings=analysis_settings)
        black_level, white_level = compute_auto_level_bounds(analysis_rgb, black_percentile, white_percentile)
    except Exception:
        return resolved
    return apply_auto_level_bounds_to_settings(resolved, black_level, white_level)


def _render_raw_array(
    rawpy_module: Any,
    source_path: str | Path,
    settings: RawRenderSettings,
    *,
    preview: bool = False,
    wb_mode: str | None = None,
    user_wb: list[float] | None = None,
) -> np.ndarray:
    kwargs = _build_postprocess_kwargs(settings, preview=preview, wb_mode=wb_mode, user_wb=user_wb)
    with rawpy_module.imread(str(source_path)) as raw:
        rgb = raw.postprocess(**kwargs)
    return np.asarray(rgb)


def _save_local_derivative_jpeg(
    rgb: np.ndarray,
    destination: Path,
    source_path: Path,
    source_capture_datetime: datetime | str | None = None,
) -> None:
    rgb8 = np.asarray(rgb, dtype=np.float64)
    if rgb8.ndim == 2:
        rgb8 = np.repeat(rgb8[..., None], 3, axis=2)
    if rgb8.ndim != 3 or rgb8.shape[-1] < 3:
        raise ValueError("Expected an RGB image with at least 3 channels")
    rgb8 = np.clip(rgb8[..., :3], 0.0, 1.0)
    rgb8 = np.rint(rgb8 * 255.0).astype(np.uint8)
    image = Image.fromarray(rgb8, mode="RGB")
    existed_before = destination.exists()
    try:
        save_kwargs = {
            "quality": RAW_DERIVATIVE_QUALITY,
            "subsampling": RAW_DERIVATIVE_SUBSAMPLING,
            "optimize": RAW_DERIVATIVE_OPTIMIZE,
        }
        exif_timestamp = _format_capture_datetime(source_capture_datetime)
        if exif_timestamp:
            exif_factory = getattr(Image, "Exif", None)
            if callable(exif_factory):
                exif = exif_factory()
                for tag_id in (306, 36867, 36868):
                    exif[tag_id] = exif_timestamp
                try:
                    save_kwargs["exif"] = exif.tobytes()
                except Exception:
                    pass
        image.save(destination, "JPEG", **save_kwargs)
        source_stat = source_path.stat()
        os.utime(destination, (source_stat.st_atime, source_stat.st_mtime))
    except Exception:
        if not existed_before:
            try:
                destination.unlink(missing_ok=True)
            except Exception:
                pass
        raise


def _resize_rgb_preview(rgb: np.ndarray, max_dim: int = RAW_PREVIEW_MAX_DIM) -> np.ndarray:
    arr = to_float_rgb(rgb, clip=False)
    if arr.ndim != 3 or arr.shape[-1] < 3:
        raise ValueError("Expected an RGB image with at least 3 channels")
    height, width = arr.shape[:2]
    limit = max(1, int(max_dim))
    long_edge = max(int(width), int(height))
    image = np.clip(arr[..., :3], 0.0, 1.0)
    if long_edge <= limit:
        return image

    scale = limit / float(long_edge)
    new_width = max(1, int(round(float(width) * scale)))
    new_height = max(1, int(round(float(height) * scale)))
    rgb8 = np.rint(image * 255.0).astype(np.uint8)
    resized = Image.fromarray(rgb8, mode="RGB").resize((new_width, new_height), Image.Resampling.LANCZOS)
    return np.asarray(resized, dtype=np.float64) / 255.0


def build_raw_processing_metadata(
    source_path: str | Path,
    local_derivative_path: str | Path,
    settings: RawRenderSettings | Mapping[str, Any] | dict[str, Any] | None,
    *,
    width: int,
    height: int,
    source_mime_type: str | None = None,
    source_capture_datetime: datetime | str | None = None,
    rendered_at: datetime | str | None = None,
) -> dict[str, Any]:
    """Build the metadata snapshot for a rendered-from-RAW local derivative."""
    source = Path(source_path)
    derivative = Path(local_derivative_path)
    render_settings = RawRenderSettings.from_dict(settings).to_dict()
    captured_at = _format_capture_datetime(source_capture_datetime)
    rendered_at_text = _format_capture_datetime(rendered_at)
    return {
        "engine": "rawpy",
        "source": {
            "kind": "camera_raw",
            "path": str(source),
            "mime_type": source_mime_type or raw_mime_type_for_path(source),
            **({"captured_at": captured_at} if captured_at else {}),
        },
        "local_derivative": {
            "kind": "rendered_from_raw",
            "format": RAW_DERIVATIVE_FORMAT,
            "mime_type": RAW_DERIVATIVE_MIME_TYPE,
            "path": str(derivative),
            "quality": RAW_DERIVATIVE_QUALITY,
            "subsampling": RAW_DERIVATIVE_SUBSAMPLING,
            "width": int(width),
            "height": int(height),
            **({"rendered_at": rendered_at_text} if rendered_at_text else {}),
        },
        "settings": render_settings,
    }


def _cleanup_partial_output(destination: Path, had_file_before: bool) -> None:
    if had_file_before:
        return
    try:
        destination.unlink(missing_ok=True)
    except Exception:
        pass


def render_raw_image(
    source_path: str | Path,
    *,
    settings: RawRenderSettings | Mapping[str, Any] | dict[str, Any] | None = None,
    output_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    preview: bool = False,
    source_capture_datetime: datetime | str | None = None,
    **_kwargs: Any,
) -> Path:
    """Render a RAW source file to a high-quality local JPEG derivative."""
    source = Path(source_path)
    if not str(source).strip():
        raise ValueError("source_path is required")

    rawpy_module = import_rawpy()
    render_settings = RawRenderSettings.from_dict(settings)

    if output_path is not None:
        destination = Path(output_path)
    else:
        target_dir = Path(output_dir) if output_dir is not None else source.parent
        target_dir.mkdir(parents=True, exist_ok=True)
        destination = _unique_output_path(target_dir, source.stem or "rendered_from_raw", ".jpg")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination_had_file = destination.exists()

    try:
        rawpy_mode = _resolve_preview_rawpy_mode(render_settings)
        rgb = _render_raw_array(
            rawpy_module,
            source,
            render_settings,
            preview=preview,
            wb_mode=rawpy_mode,
        )

        processing_settings = render_settings
        if (
            render_settings.white_balance_mode in {"background", "custom"}
            and render_settings.wb_multipliers is None
            and render_settings.wb_selection is not None
        ):
            background_wb = estimate_white_balance_from_background(
                to_float_rgb(rgb),
                rect=render_settings.wb_selection,
            )
            processing_settings = replace(
                render_settings,
                white_balance_mode="custom",
                wb_multipliers=(float(background_wb[0]), float(background_wb[1]), float(background_wb[2])),
                wb_multiplier_space="post_decode_rgb",
                wb_sample_base_mode=rawpy_mode,
            )

        rgb_float = apply_post_decode_processing(rgb, processing_settings)
        if preview:
            rgb_float = _resize_rgb_preview(rgb_float)
        _save_local_derivative_jpeg(
            rgb_float,
            destination,
            source,
            source_capture_datetime=source_capture_datetime,
        )
        return destination
    except RawRenderingUnavailableError:
        raise
    except Exception as exc:
        _cleanup_partial_output(destination, destination_had_file)
        raise RuntimeError(f"RAW rendering failed for {source.name}: {exc}") from exc


def render_raw_preview_proxy_rgb(
    source_path: str | Path,
    *,
    settings: RawRenderSettings | Mapping[str, Any] | dict[str, Any] | None = None,
) -> np.ndarray:
    """Decode a low-resolution RAW preview frame for reuse across preview updates."""
    rawpy_module = import_rawpy()
    render_settings = RawRenderSettings.from_dict(settings)
    rawpy_mode = _resolve_preview_rawpy_mode(render_settings)
    rgb = _render_raw_array(rawpy_module, source_path, render_settings, preview=True, wb_mode=rawpy_mode)
    return to_float_rgb(rgb)


def save_raw_preview_jpeg(
    rgb: np.ndarray,
    destination: str | Path,
    source_path: str | Path,
    source_capture_datetime: datetime | str | None = None,
) -> None:
    """Persist a processed RAW preview frame as a JPEG file."""
    rgb = _resize_rgb_preview(rgb)
    _save_local_derivative_jpeg(
        rgb,
        Path(destination),
        Path(source_path),
        source_capture_datetime=source_capture_datetime,
    )


def render_raw_preview(
    source_path: str | Path,
    *,
    settings: RawRenderSettings | Mapping[str, Any] | dict[str, Any] | None = None,
    output_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    **kwargs: Any,
) -> Path:
    """Render a temporary RAW preview using the same engine as the final derivative."""
    source_capture_datetime = kwargs.pop("source_capture_datetime", None)
    if source_capture_datetime is None:
        source_capture_datetime = read_rawpy_capture_datetime(source_path)
    return render_raw_image(
        source_path,
        settings=settings,
        output_path=output_path,
        output_dir=output_dir,
        preview=True,
        source_capture_datetime=source_capture_datetime,
        **kwargs,
    )


def render_raw_sampling_rgb(
    source_path: str | Path,
    *,
    settings: RawRenderSettings | Mapping[str, Any] | dict[str, Any] | None = None,
) -> np.ndarray:
    """Render a RAW source to a preview-sized RGB array suitable for sampling."""
    rawpy_module = import_rawpy()
    render_settings = RawRenderSettings.from_dict(settings)
    rgb = _render_raw_array(rawpy_module, source_path, render_settings, preview=True)
    return to_float_rgb(rgb)


__all__ = [
    "RAW_DERIVATIVE_FORMAT",
    "RAW_DERIVATIVE_MIME_TYPE",
    "RAW_DERIVATIVE_QUALITY",
    "RAW_DERIVATIVE_SUBSAMPLING",
    "RAW_PREVIEW_MAX_DIM",
    "RawRenderSettings",
    "RawRenderingUnavailableError",
    "apply_auto_level_bounds_to_settings",
    "build_raw_processing_metadata",
    "compute_auto_level_adjusted_settings_from_source",
    "render_raw_image",
    "render_raw_preview",
    "render_raw_preview_proxy_rgb",
    "render_raw_sampling_rgb",
    "save_raw_preview_jpeg",
]
