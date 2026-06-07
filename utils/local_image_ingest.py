"""Shared local image ingestion façade for HEIC, raster, and future RAW files."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from utils.heic_converter import build_local_image_provenance, maybe_convert_heic
from utils.raw_detection import is_raw_image_path
from utils.raw_render import RawRenderSettings, RawRenderingUnavailableError


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

    raw_render_snapshot = _normalize_raw_settings(raw_settings)
    image_type = _infer_image_type(lab_metadata)
    source = Path(source_text)

    if is_raw_image_path(source):
        raise RawRenderingUnavailableError(
            f"RAW rendering is not enabled yet for {source.name}; rawpy-based processing will be added in a later pass."
        )

    if source.suffix.lower() in {".heic", ".heif"}:
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
        **provenance,
    )


__all__ = [
    "LocalIngestResult",
    "prepare_local_ingest_image",
]
