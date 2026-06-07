"""Pure NumPy helpers for RAW luminance tone curves."""
from __future__ import annotations

import numpy as np


_LUMA_WEIGHTS = np.array([0.2126, 0.7152, 0.0722], dtype=np.float64)
_EPSILON = np.finfo(np.float64).eps


def normalized_sigmoid_curve(x, strength: float, midpoint: float):
    """Map values through a normalized sigmoid that stays within 0..1."""
    values = np.asarray(x, dtype=np.float64)
    clipped = np.clip(values, 0.0, 1.0)
    strength = float(strength or 0.0)
    if strength <= 0.0:
        return clipped

    midpoint = float(np.clip(midpoint if midpoint is not None else 0.5, 1e-6, 1.0 - 1e-6))
    slope = max(1e-6, 8.0 * strength)

    def _sigmoid(positions):
        return 1.0 / (1.0 + np.exp(-slope * (positions - midpoint)))

    lo = _sigmoid(0.0)
    hi = _sigmoid(1.0)
    denom = hi - lo
    if abs(float(denom)) <= _EPSILON:
        return clipped
    return np.clip((_sigmoid(clipped) - lo) / denom, 0.0, 1.0)


def apply_luminance_tone_curve(rgb, strength: float, midpoint: float):
    """Apply a luminance-only tone curve while preserving hue."""
    arr = np.asarray(rgb, dtype=np.float64)
    if arr.ndim < 1 or arr.shape[-1] < 3:
        raise ValueError("Expected an RGB array with at least 3 channels")

    rgb_only = arr[..., :3]
    luminance = np.tensordot(rgb_only, _LUMA_WEIGHTS, axes=([-1], [0]))
    mapped_luminance = normalized_sigmoid_curve(luminance, strength, midpoint)

    scale = np.ones_like(luminance, dtype=np.float64)
    usable = luminance > _EPSILON
    scale[usable] = mapped_luminance[usable] / luminance[usable]

    balanced_rgb = np.clip(rgb_only * scale[..., None], 0.0, 1.0)
    if arr.shape[-1] > 3:
        alpha = np.clip(arr[..., 3:], 0.0, 1.0)
        return np.concatenate([balanced_rgb, alpha], axis=-1)
    return balanced_rgb


__all__ = [
    "apply_luminance_tone_curve",
    "normalized_sigmoid_curve",
]
