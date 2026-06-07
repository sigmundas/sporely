"""Pure NumPy helpers for estimating RAW white balance from a background ROI."""
from __future__ import annotations

import numpy as np


_LUMA_WEIGHTS = np.array([0.2126, 0.7152, 0.0722], dtype=np.float64)
_BLACK_THRESHOLD = 0.02
_CLIP_THRESHOLD = 0.98
_EPSILON = 1e-8


def _normalize_rect(rect) -> tuple[int, int, int, int]:
    values = tuple(rect or ())
    if len(values) != 4:
        raise ValueError("rect must be [x, y, w, h]")
    try:
        x, y, w, h = (float(values[0]), float(values[1]), float(values[2]), float(values[3]))
    except Exception as exc:  # pragma: no cover - defensive conversion
        raise ValueError("rect must be numeric") from exc
    if w <= 0 or h <= 0:
        raise ValueError("rect must have positive width and height")
    return (
        int(np.floor(x)),
        int(np.floor(y)),
        int(np.ceil(x + w)),
        int(np.ceil(y + h)),
    )


def estimate_white_balance_from_background(rgb, rect=None):
    """Return RGB multipliers estimated from a background selection."""
    arr = np.asarray(rgb, dtype=np.float64)
    if arr.ndim < 2 or arr.shape[-1] < 3:
        raise ValueError("Expected an RGB image array")

    region = arr[..., :3]
    if rect is not None:
        x0, y0, x1, y1 = _normalize_rect(rect)
        x0 = max(0, x0)
        y0 = max(0, y0)
        x1 = min(region.shape[1], x1)
        y1 = min(region.shape[0], y1)
        if x1 <= x0 or y1 <= y0:
            raise ValueError("rect is outside the image bounds")
        region = region[y0:y1, x0:x1, :]

    if region.size == 0:
        raise ValueError("Background selection is empty")

    pixels = region.reshape(-1, 3)
    finite_mask = np.isfinite(pixels).all(axis=1)
    if not np.any(finite_mask):
        raise ValueError("Background selection contains no finite pixels")

    pixels = pixels[finite_mask]
    luminance = pixels @ _LUMA_WEIGHTS
    usable = (
        (luminance > _BLACK_THRESHOLD)
        & (luminance < _CLIP_THRESHOLD)
        & (pixels.max(axis=1) > _BLACK_THRESHOLD)
    )
    if not np.any(usable):
        raise ValueError("Background selection is unusable")

    usable_pixels = pixels[usable]
    channel_means = usable_pixels.mean(axis=0)
    green = float(channel_means[1])
    if not np.isfinite(green) or green <= _EPSILON:
        raise ValueError("Background selection has no usable green channel")

    r_gain = green / max(float(channel_means[0]), _EPSILON)
    b_gain = green / max(float(channel_means[2]), _EPSILON)
    return np.array([r_gain, 1.0, b_gain], dtype=np.float64)


__all__ = ["estimate_white_balance_from_background"]
