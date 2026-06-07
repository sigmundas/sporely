"""Shared local image ingestion façade for HEIC, raster, and future RAW files."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from utils.heic_converter import build_local_image_provenance, maybe_convert_heic
from utils.raw_detection import is_raw_image_path
from utils.raw_render import (
    RawRenderSettings,
    RawRenderingUnavailableError,
    build_raw_processing_metadata,
    render_raw_image,
)
from utils.rawpy_import import read_rawpy_capture_datetime


def _default_import_dir() -> Path:
    from database.schema import get_images_dir

    return get_images_dir() / "imports"


def _normalize_raw_settings(raw_settings) -> dict[str, Any] | None:
    if raw_settings is None:
        return None
    if isinstance(raw_settings, RawRenderSettings):
        return raw_settings.to_dict()
    if isinstance(raw_settings, Mapping):
        return RawRenderSettings.from_dict(raw_settings).to_dict()
    to_dict = getattr(raw_settings, "to_dict", None)
    if callable(to_dict):
        try:
            return RawRenderSettings.from_dict(to_dict()).to_dict()
        except Exception:
            return None
    try:
        return RawRenderSettings.from_dict(raw_settings).to_dict()
    except Exception:
        return None


def _infer_image_type(lab_metadata: Mapping[str, Any] | None) -> str | None:
    if not isinstance(lab_metadata, Mapping):
        return None
    for key in ("image_type", "file_purpose"):
        value = lab_metadata.get(key)
        text = str(value or "").strip().lower()
        if text:
            return text
    return None


@dataclass(slots=True)
class LocalIngestResult:
    """The shared result returned by the local ingest façade."""

    source_path: str
    working_path: str
    original_path: str | None
    source_role: str | None
    file_purpose: str | None
    original_mime_type: str | None
    working_mime_type: str | None
    raw_render_snapshot: dict[str, Any] | None = None
    lab_metadata: dict[str, Any] | None = None

    def provenance_kwargs(self) -> dict[str, str | None]:
        return {
            "source_role": self.source_role,
            "file_purpose": self.file_purpose,
            "original_mime_type": self.original_mime_type,
            "working_mime_type": self.working_mime_type,
        }


def prepare_local_ingest_image(
    source_path,
    *,
    raw_settings=None,
    lab_metadata=None,
    output_dir=None,
) -> LocalIngestResult:
    """Prepare a local file for ingestion without changing non-RAW behavior."""
    source_text = str(source_path or "").strip()
    if not source_text:
        raise ValueError("source_path is required")

    raw_render_snapshot = None
    image_type = _infer_image_type(lab_metadata)
    lab_metadata_dict = dict(lab_metadata) if isinstance(lab_metadata, Mapping) else {}
    source = Path(source_text)

    if is_raw_image_path(source):
        raw_settings_source = raw_settings
        existing_raw_processing = lab_metadata_dict.get("raw_processing")
        if raw_settings_source is None and isinstance(existing_raw_processing, Mapping):
            raw_settings_source = existing_raw_processing.get("settings")
        render_settings = RawRenderSettings.from_dict(_normalize_raw_settings(raw_settings_source))
        source_capture_datetime = read_rawpy_capture_datetime(source_text)
        resolved_output_dir = Path(output_dir) if output_dir is not None else _default_import_dir()
        resolved_output_dir.mkdir(parents=True, exist_ok=True)
        working_path = render_raw_image(
            source_text,
            settings=render_settings,
            output_dir=resolved_output_dir,
            source_capture_datetime=source_capture_datetime,
        )
        try:
            from PIL import Image

            with Image.open(working_path) as rendered_image:
                width, height = rendered_image.size
        except Exception:
            width = height = 0
        raw_render_snapshot = build_raw_processing_metadata(
            source_text,
            working_path,
            render_settings,
            width=width,
            height=height,
            source_capture_datetime=source_capture_datetime,
        )
        lab_metadata_dict["raw_processing"] = raw_render_snapshot
    elif source.suffix.lower() in {".heic", ".heif"}:
        resolved_output_dir = Path(output_dir) if output_dir is not None else _default_import_dir()
        resolved_output_dir.mkdir(parents=True, exist_ok=True)
        working_path = maybe_convert_heic(source_text, resolved_output_dir)
        if working_path is None:
            raise RuntimeError(f"HEIC conversion failed for {source.name}")
    else:
        working_path = source_text

    provenance = build_local_image_provenance(source_text, working_path, image_type=image_type)
    return LocalIngestResult(
        source_path=source_text,
        working_path=str(working_path),
        original_path=source_text,
        raw_render_snapshot=raw_render_snapshot,
        lab_metadata=lab_metadata_dict or None,
        **provenance,
    )


__all__ = [
    "LocalIngestResult",
    "prepare_local_ingest_image",
]
