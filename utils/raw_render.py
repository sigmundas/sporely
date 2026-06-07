"""RAW render settings and rawpy-backed rendering helpers."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import os
from pathlib import Path
from typing import Any, Mapping

import numpy as np
from PIL import Image

from utils.raw_detection import raw_mime_type_for_path
from utils.raw_tone_curve import apply_luminance_tone_curve
from utils.raw_white_balance import estimate_white_balance_from_background
from utils.rawpy_import import import_rawpy

RAW_DERIVATIVE_FORMAT = "jpeg"
RAW_DERIVATIVE_MIME_TYPE = "image/jpeg"
RAW_DERIVATIVE_QUALITY = 95
RAW_DERIVATIVE_SUBSAMPLING = 0
RAW_DERIVATIVE_OPTIMIZE = True


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


@dataclass(frozen=True, slots=True)
class RawRenderSettings:
    """Serializable RAW rendering parameters."""

    white_balance_mode: str = "camera"
    wb_multipliers: tuple[float, float, float] | None = None
    wb_selection: tuple[float, float, float, float] | None = None
    auto_levels: bool = True
    black_percentile: float = 0.001
    white_percentile: float = 0.999
    tone_curve_enabled: bool = False
    tone_curve_strength: float = 0.5
    tone_curve_midpoint: float = 0.5
    output_bps: int = 16

    @classmethod
    def default(cls) -> "RawRenderSettings":
        return cls()

    def to_dict(self) -> dict[str, Any]:
        return {
            "white_balance_mode": self.white_balance_mode,
            "wb_multipliers": list(self.wb_multipliers) if self.wb_multipliers is not None else None,
            "wb_selection": list(self.wb_selection) if self.wb_selection is not None else None,
            "auto_levels": bool(self.auto_levels),
            "black_percentile": float(self.black_percentile),
            "white_percentile": float(self.white_percentile),
            "tone_curve_enabled": bool(self.tone_curve_enabled),
            "tone_curve_strength": float(self.tone_curve_strength),
            "tone_curve_midpoint": float(self.tone_curve_midpoint),
            "output_bps": int(self.output_bps),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any] | dict[str, Any] | None) -> "RawRenderSettings":
        if value is None:
            return cls.default()
        if isinstance(value, cls):
            return value
        mapping = dict(value) if isinstance(value, Mapping) else {}
        white_balance_mode = str(mapping.get("white_balance_mode") or "camera").strip().lower() or "camera"
        wb_multipliers = _coerce_float_tuple(mapping.get("wb_multipliers"), 3)
        wb_selection = _coerce_float_tuple(mapping.get("wb_selection"), 4)
        return cls(
            white_balance_mode=white_balance_mode,
            wb_multipliers=wb_multipliers,  # type: ignore[arg-type]
            wb_selection=wb_selection,  # type: ignore[arg-type]
            auto_levels=_coerce_bool(mapping.get("auto_levels"), True),
            black_percentile=_coerce_float(mapping.get("black_percentile"), 0.001),
            white_percentile=_coerce_float(mapping.get("white_percentile"), 0.999),
            tone_curve_enabled=_coerce_bool(mapping.get("tone_curve_enabled"), False),
            tone_curve_strength=_coerce_float(mapping.get("tone_curve_strength"), 0.5),
            tone_curve_midpoint=_coerce_float(mapping.get("tone_curve_midpoint"), 0.5),
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


def _to_float_rgb(rgb: Any) -> np.ndarray:
    arr = np.asarray(rgb)
    if arr.ndim == 2:
        arr = np.repeat(arr[..., None], 3, axis=2)
    if arr.ndim != 3 or arr.shape[-1] < 3:
        raise ValueError("Expected an RGB image with at least 3 channels")
    arr = np.asarray(arr[..., :3], dtype=np.float64)
    arr = np.nan_to_num(arr, nan=0.0, posinf=1.0, neginf=0.0)
    if np.issubdtype(np.asarray(rgb).dtype, np.integer):
        max_value = float(np.iinfo(np.asarray(rgb).dtype).max)
        if max_value > 0:
            arr = arr / max_value
    elif arr.max(initial=0.0) > 1.0:
        arr = arr / float(arr.max(initial=1.0))
    return np.clip(arr, 0.0, 1.0)


def _apply_auto_levels(rgb: np.ndarray, black_percentile: float, white_percentile: float) -> np.ndarray:
    arr = np.asarray(rgb, dtype=np.float64)
    if arr.ndim != 3 or arr.shape[-1] < 3:
        raise ValueError("Expected an RGB image with at least 3 channels")
    arr = arr[..., :3]
    black_percentile = float(np.clip(black_percentile, 0.0, 1.0))
    white_percentile = float(np.clip(white_percentile, 0.0, 1.0))
    if white_percentile <= black_percentile:
        return np.clip(arr, 0.0, 1.0)

    luminance = (
        arr[..., 0] * 0.2126
        + arr[..., 1] * 0.7152
        + arr[..., 2] * 0.0722
    )
    low = float(np.quantile(luminance, black_percentile))
    high = float(np.quantile(luminance, white_percentile))
    if not np.isfinite(low) or not np.isfinite(high) or high <= low:
        return np.clip(arr, 0.0, 1.0)

    mapped = np.clip((luminance - low) / (high - low), 0.0, 1.0)
    scale = np.ones_like(luminance, dtype=np.float64)
    usable = luminance > np.finfo(np.float64).eps
    scale[usable] = mapped[usable] / luminance[usable]
    return np.clip(arr * scale[..., None], 0.0, 1.0)


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


def build_raw_processing_metadata(
    source_path: str | Path,
    local_derivative_path: str | Path,
    settings: RawRenderSettings | Mapping[str, Any] | dict[str, Any] | None,
    *,
    width: int,
    height: int,
    source_mime_type: str | None = None,
    source_capture_datetime: datetime | str | None = None,
) -> dict[str, Any]:
    """Build the metadata snapshot for a rendered-from-RAW local derivative."""
    source = Path(source_path)
    derivative = Path(local_derivative_path)
    render_settings = RawRenderSettings.from_dict(settings).to_dict()
    captured_at = _format_capture_datetime(source_capture_datetime)
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
        if render_settings.white_balance_mode == "background" and render_settings.wb_selection is not None:
            preview_rgb = _render_raw_array(
                rawpy_module,
                source,
                render_settings,
                preview=preview,
                wb_mode="camera",
            )
            background_wb = estimate_white_balance_from_background(
                _to_float_rgb(preview_rgb),
                rect=render_settings.wb_selection,
            )
            user_wb = [float(background_wb[0]), float(background_wb[1]), float(background_wb[2]), float(background_wb[1])]
            rgb = _render_raw_array(
                rawpy_module,
                source,
                render_settings,
                preview=preview,
                user_wb=user_wb,
            )
        else:
            rgb = _render_raw_array(
                rawpy_module,
                source,
                render_settings,
                preview=preview,
            )
        rgb_float = _to_float_rgb(rgb)
        if render_settings.auto_levels:
            rgb_float = _apply_auto_levels(
                rgb_float,
                render_settings.black_percentile,
                render_settings.white_percentile,
            )
        if render_settings.tone_curve_enabled:
            rgb_float = apply_luminance_tone_curve(
                rgb_float,
                render_settings.tone_curve_strength,
                render_settings.tone_curve_midpoint,
            )
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


__all__ = [
    "RAW_DERIVATIVE_FORMAT",
    "RAW_DERIVATIVE_MIME_TYPE",
    "RAW_DERIVATIVE_QUALITY",
    "RAW_DERIVATIVE_SUBSAMPLING",
    "RawRenderSettings",
    "RawRenderingUnavailableError",
    "build_raw_processing_metadata",
    "render_raw_image",
]
