"""Shared post-decode image-processing helpers."""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Mapping

import numpy as np

from utils.raw_tone_curve import apply_luminance_tone_curve, normalized_sigmoid_curve

_LUMA_WEIGHTS = np.array([0.2126, 0.7152, 0.0722], dtype=np.float64)
_EPSILON = np.finfo(np.float64).eps


@dataclass(frozen=True, slots=True)
class ProcessingDebugInfo:
    """Debug snapshot for the shared post-decode processing pipeline.

    `input_min` and `input_max` are measured after post-decode white balance and
    before auto-levels, which is the luminance range the auto-level stage sees.
    """

    input_min: float
    input_max: float
    black_level: float | None
    white_level: float | None
    settings: dict[str, Any]


@dataclass(frozen=True, slots=True)
class PostDecodeTransferCurve:
    """Curve data for visualizing the post-decode transfer function."""

    input_values: np.ndarray
    hard_target: np.ndarray
    soft_target: np.ndarray
    auto_levels_output: np.ndarray
    final_output: np.ndarray
    debug: ProcessingDebugInfo


@dataclass(frozen=True, slots=True)
class RawBasicControlState:
    """Simplified RAW controls used by the normal Live Lab UI."""

    white_balance_mode: str
    contrast: float
    midpoint: float
    preserve_tails: bool


def _clamp_unit(value: Any, default: float = 0.0) -> float:
    try:
        numeric = float(value)
    except Exception:
        numeric = float(default)
    if not np.isfinite(numeric):
        numeric = float(default)
    return float(np.clip(numeric, 0.0, 1.0))


def _clamp_range(value: Any, default: float, minimum: float, maximum: float) -> float:
    try:
        numeric = float(value)
    except Exception:
        numeric = float(default)
    if not np.isfinite(numeric):
        numeric = float(default)
    return float(np.clip(numeric, float(minimum), float(maximum)))


def _raw_settings_class():
    from utils.raw_render import RawRenderSettings

    return RawRenderSettings


def raw_basic_controls_from_settings(settings: Any) -> RawBasicControlState:
    """Approximate simplified Live Lab controls from a full RAW settings snapshot."""
    resolved = _raw_settings_class().from_dict(_settings_to_dict(settings))
    white_balance_mode = str(resolved.white_balance_mode or "camera").strip().lower() or "camera"

    midpoint = 0.5
    midpoint_raw = (float(resolved.tone_curve_midpoint) - 0.18) / 0.64
    if np.isfinite(midpoint_raw):
        midpoint = float(np.clip(midpoint_raw, 0.0, 1.0))

    contrast = 0.0
    if bool(resolved.tone_curve_enabled):
        strength = float(np.clip(float(resolved.tone_curve_strength), 0.08, 0.80))
        contrast = float(np.clip((strength - 0.08) / 0.72, 0.0, 1.0)) ** (1.0 / 1.35)
    preserve_tails = bool(resolved.auto_levels_soft_tails)

    return RawBasicControlState(
        white_balance_mode=white_balance_mode if white_balance_mode in {"camera", "auto", "custom"} else "camera",
        contrast=float(np.clip(contrast, 0.0, 1.0)),
        midpoint=midpoint,
        preserve_tails=preserve_tails,
    )


def raw_settings_from_basic_controls(
    *,
    white_balance_mode: str,
    wb_multipliers: tuple[float, float, float] | None = None,
    contrast: float,
    midpoint: float,
    preserve_tails: bool,
    dark_cutoff: float = 0.0005,
    bright_cutoff: float = 0.0005,
    shadow_lift_enabled: bool = True,
    shadow_lift_max: float = 0.05,
    existing_settings: Any | None = None,
) -> Any:
    """Map simplified Live Lab controls into a full RAW settings snapshot."""
    RawRenderSettings = _raw_settings_class()
    base_settings = RawRenderSettings.from_dict(existing_settings)

    mode = str(white_balance_mode or "camera").strip().lower() or "camera"
    if mode not in {"camera", "auto", "custom"}:
        mode = "camera"

    resolved_wb_multipliers = None
    resolved_wb_selection = None
    resolved_wb_multiplier_space = None
    resolved_wb_sample_point = None
    resolved_wb_sample_size = None
    resolved_wb_sample_base_mode = None
    resolved_wb_selection_space = None
    if mode == "custom":
        resolved_wb_multipliers = wb_multipliers if wb_multipliers is not None else base_settings.wb_multipliers
        resolved_wb_selection = base_settings.wb_selection
        resolved_wb_multiplier_space = base_settings.wb_multiplier_space or "post_decode_rgb"
        resolved_wb_sample_point = base_settings.wb_sample_point
        resolved_wb_sample_size = base_settings.wb_sample_size
        resolved_wb_sample_base_mode = base_settings.wb_sample_base_mode
        resolved_wb_selection_space = base_settings.wb_selection_space

    contrast_value = _clamp_unit(contrast, 0.0)
    midpoint_value = _clamp_unit(midpoint, 0.5)
    curve_midpoint = 0.18 + midpoint_value * 0.64
    curve_strength = 0.08 + 0.72 * (contrast_value ** 1.35)
    dark_cutoff_value = _clamp_range(dark_cutoff, 0.0005, 0.0, 0.02)
    bright_cutoff_value = _clamp_range(bright_cutoff, 0.0005, 0.0, 0.02)
    lift_enabled = bool(shadow_lift_enabled)
    lift_max = _clamp_range(shadow_lift_max, 0.05, 0.0, 0.05)
    if lift_enabled and midpoint_value < 0.20:
        lift_ratio = float(smoothstep((0.20 - midpoint_value) / 0.20))
        shadow_lift = float(np.clip(lift_max * lift_ratio, 0.0, lift_max))
    else:
        shadow_lift = 0.0

    resolved = replace(
        base_settings,
        white_balance_mode=mode,
        wb_multipliers=resolved_wb_multipliers,
        wb_selection=resolved_wb_selection,
        wb_multiplier_space=resolved_wb_multiplier_space,
        wb_sample_point=resolved_wb_sample_point,
        wb_sample_size=resolved_wb_sample_size,
        wb_sample_base_mode=resolved_wb_sample_base_mode,
        wb_selection_space=resolved_wb_selection_space,
        auto_levels=True,
        black_percentile=float(dark_cutoff_value),
        white_percentile=float(max(0.0, 1.0 - bright_cutoff_value)),
        auto_levels_strength=1.0,
        auto_levels_soft_tails=bool(preserve_tails),
        auto_levels_tail_size=0.03,
        auto_levels_shadow_lift=float(np.clip(shadow_lift, 0.0, 0.05)),
        tone_curve_enabled=bool(contrast_value > 0.02),
        tone_curve_strength=float(np.clip(curve_strength, 0.0, 1.0)),
        tone_curve_midpoint=float(np.clip(curve_midpoint, 0.0, 1.0)),
    )
    return resolved


def _settings_to_dict(settings: Any) -> dict[str, Any]:
    if settings is None:
        mapping: dict[str, Any] = {}
    elif isinstance(settings, Mapping):
        mapping = dict(settings)
    else:
        to_dict = getattr(settings, "to_dict", None)
        if callable(to_dict):
            try:
                mapping = dict(to_dict())
            except Exception:
                mapping = {}
        else:
            mapping = {}

    try:
        from utils.raw_render import RawRenderSettings

        return RawRenderSettings.from_dict(mapping).to_dict()
    except Exception:
        return mapping


def to_float_rgb(rgb: Any) -> np.ndarray:
    """Normalize an RGB array to float64 in the 0..1 range."""
    arr = np.asarray(rgb)
    if arr.ndim == 2:
        arr = np.repeat(arr[..., None], 3, axis=2)
    if arr.ndim != 3 or arr.shape[-1] < 3:
        raise ValueError("Expected an RGB image with at least 3 channels")

    arr = np.asarray(arr[..., :3], dtype=np.float64)
    arr = np.nan_to_num(arr, nan=0.0, posinf=1.0, neginf=0.0)
    source = np.asarray(rgb)
    if np.issubdtype(source.dtype, np.integer):
        max_value = float(np.iinfo(source.dtype).max)
        if max_value > 0:
            arr = arr / max_value
    elif arr.max(initial=0.0) > 1.0:
        arr = arr / float(arr.max(initial=1.0))
    return np.clip(arr, 0.0, 1.0)


def compute_luminance(rgb: Any) -> np.ndarray:
    """Return the luminance channel for an RGB array."""
    arr = to_float_rgb(rgb)
    return np.tensordot(arr, _LUMA_WEIGHTS, axes=([-1], [0]))


def apply_custom_white_balance(rgb: Any, multipliers: tuple[float, float, float] | None) -> np.ndarray:
    """Apply post-decode RGB channel gains."""
    arr = to_float_rgb(rgb)
    if multipliers is None:
        return arr
    if len(multipliers) != 3:
        raise ValueError("Expected three RGB multipliers")
    gains = np.asarray([float(multipliers[0]), float(multipliers[1]), float(multipliers[2])], dtype=np.float64)
    balanced = np.clip(arr[..., :3] * gains, 0.0, 1.0)
    if arr.shape[-1] > 3:
        alpha = np.clip(arr[..., 3:], 0.0, 1.0)
        return np.concatenate([balanced, alpha], axis=-1)
    return balanced


def _clamp01(values: Any) -> np.ndarray:
    return np.clip(np.asarray(values, dtype=np.float64), 0.0, 1.0)


def smoothstep(values: Any) -> np.ndarray:
    """Return a smoothstep interpolation over the 0..1 range."""
    z = _clamp01(values)
    return z * z * (3.0 - 2.0 * z)


def _resolve_auto_level_output_anchors(
    input_min: float | None,
    input_max: float | None,
    black_level: float | None,
    white_level: float | None,
    *,
    shadow_lift: float,
    tail_size: float,
) -> tuple[float, float, float, float, float, bool, bool]:
    output_min = float(np.clip(shadow_lift, 0.0, 0.25))
    output_max = 1.0
    has_dark_tail = (
        black_level is not None
        and input_min is not None
        and np.isfinite(float(black_level))
        and np.isfinite(float(input_min))
        and float(black_level) > float(input_min) + _EPSILON
    )
    has_bright_tail = (
        white_level is not None
        and input_max is not None
        and np.isfinite(float(white_level))
        and np.isfinite(float(input_max))
        and float(input_max) > float(white_level) + _EPSILON
    )

    tail = float(np.clip(tail_size, 0.0, 0.5))
    available = max(0.0, output_max - output_min)
    tail = min(tail, max(0.0, (available - _EPSILON) / 2.0))

    output_black = output_min + tail if has_dark_tail else output_min
    output_white = output_max - tail if has_bright_tail else output_max
    if output_white <= output_black + _EPSILON:
        output_black = output_min
        output_white = output_max
        tail = 0.0

    return output_min, output_black, output_white, output_max, tail, has_dark_tail, has_bright_tail


def hard_luminance_levels(
    x: Any,
    black_level: float | None,
    white_level: float | None,
    *,
    shadow_lift: float = 0.0,
) -> np.ndarray:
    """Return the hard auto-level luminance transfer."""
    values = np.asarray(x, dtype=np.float64)
    if black_level is None or white_level is None:
        return _clamp01(values)
    black = float(black_level)
    white = float(white_level)
    if not np.isfinite(black) or not np.isfinite(white):
        return _clamp01(values)
    span = white - black
    if span <= _EPSILON:
        return _clamp01(values)
    output_min = float(np.clip(shadow_lift, 0.0, 0.25))
    mapped = _clamp01((values - black) / span)
    return _clamp01(output_min + mapped * (1.0 - output_min))


def soft_luminance_levels(
    x: Any,
    input_min: float | None,
    black_level: float | None,
    white_level: float | None,
    input_max: float | None,
    tail_size: float,
    *,
    shadow_lift: float = 0.0,
) -> np.ndarray:
    """Return the soft-tail luminance transfer."""
    values = np.asarray(x, dtype=np.float64)
    if black_level is None or white_level is None:
        return _clamp01(values)

    black = float(black_level)
    white = float(white_level)
    if not np.isfinite(black) or not np.isfinite(white) or white <= black:
        return _clamp01(values)

    minimum = float(input_min) if input_min is not None else (float(np.min(values)) if values.size else 0.0)
    maximum = float(input_max) if input_max is not None else (float(np.max(values)) if values.size else 1.0)
    if not np.isfinite(minimum):
        minimum = float(np.min(values)) if values.size else 0.0
    if not np.isfinite(maximum):
        maximum = float(np.max(values)) if values.size else 1.0

    output_min, output_black, output_white, output_max, tail, has_dark_tail, has_bright_tail = _resolve_auto_level_output_anchors(
        minimum,
        maximum,
        black,
        white,
        shadow_lift=shadow_lift,
        tail_size=tail_size,
    )
    output = np.empty_like(values, dtype=np.float64)

    low_mask = values < black
    mid_mask = (values >= black) & (values <= white)
    high_mask = values > white

    if np.any(low_mask):
        if has_dark_tail and black > minimum + _EPSILON:
            output[low_mask] = output_min + tail * smoothstep((values[low_mask] - minimum) / (black - minimum))
        else:
            output[low_mask] = output_min
    if np.any(mid_mask):
        span = white - black
        if span <= _EPSILON:
            output[mid_mask] = output_black
        else:
            progress = _clamp01((values[mid_mask] - black) / span)
            output[mid_mask] = output_black + (output_white - output_black) * progress
    if np.any(high_mask):
        if has_bright_tail and maximum > white + _EPSILON:
            output[high_mask] = output_white + tail * smoothstep((values[high_mask] - white) / (maximum - white))
        else:
            output[high_mask] = output_max

    return _clamp01(output)


def blend_luminance_levels(original: Any, target: Any, strength: float) -> np.ndarray:
    """Blend between the original and target luminance values."""
    base = np.asarray(original, dtype=np.float64)
    mapped = np.asarray(target, dtype=np.float64)
    blend = float(np.clip(strength, 0.0, 1.0))
    return _clamp01((1.0 - blend) * base + blend * mapped)


def apply_luminance_transfer(rgb: Any, source_luminance: Any, target_luminance: Any) -> np.ndarray:
    """Apply a luminance transfer while preserving RGB hue."""
    arr = to_float_rgb(rgb)
    source = np.asarray(source_luminance, dtype=np.float64)
    target = np.asarray(target_luminance, dtype=np.float64)
    if source.shape != target.shape:
        raise ValueError("Source and target luminance must have the same shape")

    scale = np.ones_like(source, dtype=np.float64)
    usable = source > _EPSILON
    scale[usable] = target[usable] / source[usable]
    balanced = np.clip(arr[..., :3] * scale[..., None], 0.0, 1.0)
    if arr.shape[-1] > 3:
        alpha = np.clip(arr[..., 3:], 0.0, 1.0)
        return np.concatenate([balanced, alpha], axis=-1)
    return balanced


def compute_auto_level_bounds(
    rgb: Any,
    black_percentile: float,
    white_percentile: float,
) -> tuple[float | None, float | None]:
    """Return the luminance bounds used by the auto-level stage."""
    arr = to_float_rgb(rgb)
    luminance = compute_luminance(arr)
    black_percentile = float(np.clip(black_percentile, 0.0, 1.0))
    white_percentile = float(np.clip(white_percentile, 0.0, 1.0))
    if white_percentile <= black_percentile:
        return None, None

    black_level = float(np.quantile(luminance, black_percentile))
    white_level = float(np.quantile(luminance, white_percentile))
    if not np.isfinite(black_level) or not np.isfinite(white_level) or white_level <= black_level:
        return None, None
    return black_level, white_level


def apply_auto_levels_from_bounds(
    rgb: Any,
    black_level: float | None,
    white_level: float | None,
    *,
    shadow_lift: float = 0.0,
) -> np.ndarray:
    """Apply the same luminance auto-level mapping used by Sporely."""
    arr = to_float_rgb(rgb)
    if black_level is None or white_level is None or not np.isfinite(black_level) or not np.isfinite(white_level):
        return np.clip(arr, 0.0, 1.0)
    if white_level <= black_level:
        return np.clip(arr, 0.0, 1.0)

    luminance = compute_luminance(arr)
    mapped = hard_luminance_levels(luminance, black_level, white_level, shadow_lift=shadow_lift)
    return apply_luminance_transfer(arr, luminance, mapped)


def compute_auto_levels_transfer(
    luminance: Any,
    *,
    input_min: float | None,
    input_max: float | None,
    black_level: float | None,
    white_level: float | None,
    strength: float,
    soft_tails: bool,
    tail_size: float,
    shadow_lift: float = 0.0,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Compute hard, soft, and strength-blended luminance transfers."""
    values = np.asarray(luminance, dtype=np.float64)
    hard = hard_luminance_levels(values, black_level, white_level, shadow_lift=shadow_lift)
    soft = soft_luminance_levels(
        values,
        input_min,
        black_level,
        white_level,
        input_max,
        tail_size,
        shadow_lift=shadow_lift,
    )
    target = soft if soft_tails else hard
    blended = blend_luminance_levels(values, target, strength)
    return hard, soft, blended


def apply_post_decode_processing(
    rgb: Any,
    settings: Any,
    *,
    return_debug: bool = False,
) -> np.ndarray | tuple[np.ndarray, ProcessingDebugInfo]:
    """Apply post-decode custom WB, auto-levels, and tone curve in order."""
    normalized_settings = _settings_to_dict(settings)
    input_rgb = to_float_rgb(rgb)

    working = input_rgb
    wb_multipliers = normalized_settings.get("wb_multipliers")
    wb_space = str(normalized_settings.get("wb_multiplier_space") or "").strip().lower() or None
    if wb_multipliers is not None and wb_space in {None, "post_decode_rgb"}:
        try:
            gains = tuple(float(value) for value in wb_multipliers)
        except Exception:
            gains = None
        if gains is not None and len(gains) == 3:
            working = apply_custom_white_balance(working, gains)

    working_luminance = compute_luminance(working)
    input_min = float(np.min(working_luminance)) if working_luminance.size else 0.0
    input_max = float(np.max(working_luminance)) if working_luminance.size else 0.0

    black_level = None
    white_level = None
    if bool(normalized_settings.get("auto_levels", True)):
        black_level, white_level = compute_auto_level_bounds(
            working,
            float(normalized_settings.get("black_percentile", 0.001)),
            float(normalized_settings.get("white_percentile", 0.999)),
        )
        if black_level is not None and white_level is not None:
            _hard_target, _soft_target, auto_levels_output = compute_auto_levels_transfer(
                working_luminance,
                input_min=input_min,
                input_max=input_max,
                black_level=black_level,
                white_level=white_level,
                strength=float(normalized_settings.get("auto_levels_strength", 1.0)),
                soft_tails=bool(normalized_settings.get("auto_levels_soft_tails", False)),
                tail_size=float(normalized_settings.get("auto_levels_tail_size", 0.03)),
                shadow_lift=float(normalized_settings.get("auto_levels_shadow_lift", 0.0)),
            )
            working = apply_luminance_transfer(working, working_luminance, auto_levels_output)

    if bool(normalized_settings.get("tone_curve_enabled", False)):
        working = apply_luminance_tone_curve(
            working,
            float(normalized_settings.get("tone_curve_strength", 0.5)),
            float(normalized_settings.get("tone_curve_midpoint", 0.5)),
        )

    if not return_debug:
        return working

    debug = ProcessingDebugInfo(
        input_min=input_min,
        input_max=input_max,
        black_level=black_level,
        white_level=white_level,
        settings=normalized_settings,
    )
    return working, debug


def compute_post_decode_transfer_curve(
    rgb: Any,
    settings: Any,
    *,
    samples: int = 2048,
    debug: ProcessingDebugInfo | None = None,
) -> PostDecodeTransferCurve:
    """Return the luminance transfer curves used by post-decode processing."""
    normalized_settings = _settings_to_dict(settings)
    resolved_debug = debug
    if resolved_debug is None:
        _, resolved_debug = apply_post_decode_processing(rgb, normalized_settings, return_debug=True)

    if not isinstance(resolved_debug, ProcessingDebugInfo):
        raise TypeError("Expected ProcessingDebugInfo for debug data")

    sample_count = max(2, int(samples))
    ramp = np.linspace(0.0, 1.0, sample_count, dtype=np.float64)
    black_level = resolved_debug.black_level if bool(normalized_settings.get("auto_levels", True)) else None
    white_level = resolved_debug.white_level if bool(normalized_settings.get("auto_levels", True)) else None

    if (
        black_level is not None
        and white_level is not None
        and np.isfinite(black_level)
        and np.isfinite(white_level)
        and float(white_level) > float(black_level)
    ):
        ramp = np.unique(
            np.concatenate(
                [
                    ramp,
                    np.asarray([float(black_level), float(white_level)], dtype=np.float64),
                ]
            )
        )
        hard_target, soft_target, auto_levels_output = compute_auto_levels_transfer(
            ramp,
            input_min=resolved_debug.input_min,
            input_max=resolved_debug.input_max,
            black_level=black_level,
            white_level=white_level,
            strength=float(normalized_settings.get("auto_levels_strength", 1.0)),
            soft_tails=bool(normalized_settings.get("auto_levels_soft_tails", False)),
            tail_size=float(normalized_settings.get("auto_levels_tail_size", 0.03)),
            shadow_lift=float(normalized_settings.get("auto_levels_shadow_lift", 0.0)),
        )
    else:
        hard_target = ramp.copy()
        soft_target = ramp.copy()
        auto_levels_output = ramp.copy()

    final_output = auto_levels_output.copy()
    if bool(normalized_settings.get("tone_curve_enabled", False)):
        final_output = normalized_sigmoid_curve(
            auto_levels_output,
            float(normalized_settings.get("tone_curve_strength", 0.5)),
            float(normalized_settings.get("tone_curve_midpoint", 0.5)),
        )

    return PostDecodeTransferCurve(
        input_values=ramp,
        hard_target=hard_target,
        soft_target=soft_target,
        auto_levels_output=auto_levels_output,
        final_output=final_output,
        debug=resolved_debug,
    )


__all__ = [
    "RawBasicControlState",
    "ProcessingDebugInfo",
    "PostDecodeTransferCurve",
    "apply_auto_levels_from_bounds",
    "apply_custom_white_balance",
    "apply_luminance_transfer",
    "apply_post_decode_processing",
    "blend_luminance_levels",
    "compute_auto_level_bounds",
    "compute_auto_levels_transfer",
    "compute_post_decode_transfer_curve",
    "compute_luminance",
    "hard_luminance_levels",
    "smoothstep",
    "soft_luminance_levels",
    "raw_basic_controls_from_settings",
    "raw_settings_from_basic_controls",
    "to_float_rgb",
]
