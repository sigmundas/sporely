"""Shared helpers for preparing AI image requests."""
from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from PIL import Image, ImageOps

_KEEP_TEMP_ENV = "SPORELY_KEEP_AI_ID_TEMP"
_DEBUG_ENV = "SPORELY_DEBUG_AI_ID"


def _env_flag(name: str) -> bool:
    return str(os.environ.get(name, "")).strip().lower() in {"1", "true", "yes", "on"}


def should_keep_ai_id_temp() -> bool:
    return _env_flag(_KEEP_TEMP_ENV)


def debug_ai_id_enabled() -> bool:
    return _env_flag(_DEBUG_ENV)


def _sanitize_prefix(prefix: str | None) -> str:
    text = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(prefix or "").strip())
    return text.strip("._-") or "ai_id"


def _normalize_crop_box(
    crop_box: tuple[float, float, float, float] | list[float] | None,
) -> tuple[float, float, float, float] | None:
    if crop_box is None:
        return None
    try:
        x1, y1, x2, y2 = (float(value) for value in crop_box)
    except Exception:
        return None
    x1, x2 = sorted((x1, x2))
    y1, y2 = sorted((y1, y2))
    x1 = max(0.0, min(1.0, x1))
    y1 = max(0.0, min(1.0, y1))
    x2 = max(0.0, min(1.0, x2))
    y2 = max(0.0, min(1.0, y2))
    return x1, y1, x2, y2


def _normalized_crop_pixels(
    normalized_crop_box: tuple[float, float, float, float],
    size: tuple[int, int],
) -> tuple[int, int, int, int] | None:
    width, height = (int(size[0]), int(size[1]))
    if width <= 0 or height <= 0:
        return None
    x1, y1, x2, y2 = normalized_crop_box
    crop_x1 = max(0, min(width, int(round(x1 * width))))
    crop_y1 = max(0, min(height, int(round(y1 * height))))
    crop_x2 = max(0, min(width, int(round(x2 * width))))
    crop_y2 = max(0, min(height, int(round(y2 * height))))
    if crop_x2 <= crop_x1:
        crop_x2 = min(width, crop_x1 + 1)
        crop_x1 = max(0, crop_x2 - 1)
    if crop_y2 <= crop_y1:
        crop_y2 = min(height, crop_y1 + 1)
        crop_y1 = max(0, crop_y2 - 1)
    return crop_x1, crop_y1, crop_x2, crop_y2


@dataclass(frozen=True, slots=True)
class PreparedAIRequestImage:
    path: Path
    original_size: tuple[int, int]
    crop_box: tuple[float, float, float, float] | None
    crop_pixels: tuple[int, int, int, int] | None
    final_size: tuple[int, int]
    sha256: str
    byte_size: int


def prepare_ai_request_image(
    image_path: str | Path,
    crop_box: tuple[float, float, float, float] | list[float] | None,
    temp_dir: str | Path,
    prefix: str,
    max_dim: int = 1600,
    jpeg_quality: int = 90,
) -> PreparedAIRequestImage:
    source_path = Path(image_path)
    temp_dir_path = Path(temp_dir)
    normalized_crop_box = _normalize_crop_box(crop_box)
    crop_pixels: tuple[int, int, int, int] | None = None

    with Image.open(source_path) as img:
        img = ImageOps.exif_transpose(img)
        original_size = (int(img.size[0]), int(img.size[1]))
        if normalized_crop_box is not None:
            crop_pixels = _normalized_crop_pixels(normalized_crop_box, original_size)
            if crop_pixels is not None:
                img = img.crop(crop_pixels)
        if max_dim and max_dim > 0 and max(img.size) > max_dim:
            resample = getattr(getattr(Image, "Resampling", Image), "LANCZOS")
            img.thumbnail((max_dim, max_dim), resample)
        if img.mode not in {"RGB", "L"}:
            img = img.convert("RGB")
        final_size = (int(img.size[0]), int(img.size[1]))
        temp_dir_path.mkdir(parents=True, exist_ok=True)
        temp_path = temp_dir_path / f"{_sanitize_prefix(prefix)}_{uuid4().hex}.jpg"
        img.save(temp_path, "JPEG", quality=int(jpeg_quality))

    byte_size = int(temp_path.stat().st_size)
    sha256 = hashlib.sha256(temp_path.read_bytes()).hexdigest()
    return PreparedAIRequestImage(
        path=temp_path,
        original_size=original_size,
        crop_box=normalized_crop_box,
        crop_pixels=crop_pixels,
        final_size=final_size,
        sha256=sha256,
        byte_size=byte_size,
    )


def debug_log_prepared_ai_request_image(
    *,
    provider: str,
    source_path: str | Path,
    prepared: PreparedAIRequestImage | object,
    max_dim: int,
    jpeg_quality: int,
) -> None:
    if not debug_ai_id_enabled():
        return
    print(
        "[SPORELY_DEBUG_AI_ID] "
        f"provider={provider} "
        f"source_path={Path(source_path)} "
        f"final_temp_path={getattr(prepared, 'path', None)} "
        f"original_size={getattr(prepared, 'original_size', None)} "
        f"crop_box={getattr(prepared, 'crop_box', None)} "
        f"crop_pixels={getattr(prepared, 'crop_pixels', None)} "
        f"final_size={getattr(prepared, 'final_size', None)} "
        f"sha256={getattr(prepared, 'sha256', None)} "
        f"byte_size={getattr(prepared, 'byte_size', None)} "
        f"max_dim={max_dim} "
        f"jpeg_quality={jpeg_quality}",
        flush=True,
    )
