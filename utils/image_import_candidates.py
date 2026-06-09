"""Staged image import candidate helpers.

This module models content-aware import candidates without committing anything to
the database. It preserves companion file relationships so later UI work can
decide how to present and commit them.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from config import (
    RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG,
    RAW_COMPANION_SOURCE_PREFERENCE_PREFER_RAW,
)
from utils.exif_reader import get_image_metadata
from utils.image_companion_grouping import companion_group_key, normalize_raw_companion_source_preference
from utils.image_metadata_merge import merge_image_lab_metadata
from utils.local_image_ingest import prepare_local_ingest_image
from utils.raw_detection import SUPPORTED_RAW_SUFFIXES, is_raw_image_path
from utils.raw_render import RawRenderSettings, RawRenderingUnavailableError
from utils.rawpy_import import read_rawpy_capture_datetime

SUPPORTED_IMPORT_SUFFIXES = frozenset(
    {
        ".png",
        ".jpg",
        ".jpeg",
        ".tif",
        ".tiff",
        ".webp",
        ".heic",
        ".heif",
    }
) | set(SUPPORTED_RAW_SUFFIXES)

SUPPORTED_RASTER_SUFFIXES = frozenset({".png", ".jpg", ".jpeg", ".tif", ".tiff", ".webp"})
SUPPORTED_JPEG_SUFFIXES = frozenset({".jpg", ".jpeg"})


class ImageImportStatus(str, Enum):
    STAGED = "staged"
    READY = "ready"
    FAILED = "failed"
    SKIPPED = "skipped"
    COMMITTED = "committed"


class ImageImportSourceKind(str, Enum):
    RAW = "raw"
    RASTER = "raster"
    HEIC = "heic"
    CAMERA_JPEG = "camera_jpeg"
    PROCESSED_RASTER = "processed_raster"


IMAGE_IMPORT_STATUS_STAGED = ImageImportStatus.STAGED.value
IMAGE_IMPORT_STATUS_READY = ImageImportStatus.READY.value
IMAGE_IMPORT_STATUS_FAILED = ImageImportStatus.FAILED.value
IMAGE_IMPORT_STATUS_SKIPPED = ImageImportStatus.SKIPPED.value
IMAGE_IMPORT_STATUS_COMMITTED = ImageImportStatus.COMMITTED.value

IMAGE_IMPORT_SOURCE_KIND_RAW = ImageImportSourceKind.RAW.value
IMAGE_IMPORT_SOURCE_KIND_RASTER = ImageImportSourceKind.RASTER.value
IMAGE_IMPORT_SOURCE_KIND_HEIC = ImageImportSourceKind.HEIC.value
IMAGE_IMPORT_SOURCE_KIND_CAMERA_JPEG = ImageImportSourceKind.CAMERA_JPEG.value
IMAGE_IMPORT_SOURCE_KIND_PROCESSED_RASTER = ImageImportSourceKind.PROCESSED_RASTER.value


def _enum_value_text(value: Any) -> str:
    if isinstance(value, Enum):
        return str(value.value)
    return str(value or "")


def normalize_image_import_status(value: str | ImageImportStatus | None) -> str:
    text = _enum_value_text(value).strip().lower()
    if text in {
        IMAGE_IMPORT_STATUS_STAGED,
        IMAGE_IMPORT_STATUS_READY,
        IMAGE_IMPORT_STATUS_FAILED,
        IMAGE_IMPORT_STATUS_SKIPPED,
        IMAGE_IMPORT_STATUS_COMMITTED,
    }:
        return text
    return IMAGE_IMPORT_STATUS_STAGED


def normalize_image_import_source_kind(value: str | ImageImportSourceKind | None) -> str:
    text = _enum_value_text(value).strip().lower().replace("-", "_").replace(" ", "_")
    if text in {
        IMAGE_IMPORT_SOURCE_KIND_RAW,
        IMAGE_IMPORT_SOURCE_KIND_RASTER,
        IMAGE_IMPORT_SOURCE_KIND_HEIC,
        IMAGE_IMPORT_SOURCE_KIND_CAMERA_JPEG,
        IMAGE_IMPORT_SOURCE_KIND_PROCESSED_RASTER,
    }:
        return text
    if text in {"camera_raw", "raw_file", "rawimage", "raw_image"}:
        return IMAGE_IMPORT_SOURCE_KIND_RAW
    if text in {"camera_jpeg", "camerajpeg", "jpeg", "jpg", "camera_jpg"}:
        return IMAGE_IMPORT_SOURCE_KIND_CAMERA_JPEG
    if text in {"heif"}:
        return IMAGE_IMPORT_SOURCE_KIND_HEIC
    return IMAGE_IMPORT_SOURCE_KIND_RASTER


def _coerce_path(value: str | Path | None) -> Path | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return Path(text).expanduser().resolve()
    except Exception:
        try:
            return Path(text).expanduser()
        except Exception:
            return None


def _coerce_mapping(value: Mapping[str, Any] | dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    try:
        return deepcopy(dict(value))
    except Exception:
        return dict(value)


def _dedupe_paths(paths: Iterable[Path]) -> tuple[Path, ...]:
    seen: set[str] = set()
    unique: list[Path] = []
    for path in paths:
        normalized = _coerce_path(path)
        if normalized is None:
            continue
        key = str(normalized)
        if key in seen:
            continue
        seen.add(key)
        unique.append(normalized)
    return tuple(unique)


def _is_supported_import_suffix(path: Path) -> bool:
    return path.suffix.lower() in SUPPORTED_IMPORT_SUFFIXES


def _sort_paths(paths: Iterable[Path]) -> list[Path]:
    return sorted(
        (path for path in paths if path is not None),
        key=lambda path: (path.name.casefold(), str(path).casefold()),
    )


def _raw_paths(paths: Iterable[Path]) -> list[Path]:
    return [path for path in paths if is_raw_image_path(path)]


def _jpeg_paths(paths: Iterable[Path]) -> list[Path]:
    return [path for path in paths if path.suffix.lower() in SUPPORTED_JPEG_SUFFIXES]


def _non_raw_supported_paths(paths: Iterable[Path]) -> list[Path]:
    return [path for path in paths if _is_supported_import_suffix(path) and not is_raw_image_path(path)]


def _select_group_path(paths: Iterable[Path], *, source_preference: str | None = None) -> Path | None:
    ordered_paths = _sort_paths(paths)
    if not ordered_paths:
        return None

    supported_paths = [path for path in ordered_paths if _is_supported_import_suffix(path)]
    raw_paths = _raw_paths(supported_paths)
    jpeg_paths = _jpeg_paths(supported_paths)
    non_raw_paths = _non_raw_supported_paths(supported_paths)
    normalized_preference = normalize_raw_companion_source_preference(source_preference)

    if normalized_preference == RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG:
        if jpeg_paths:
            return jpeg_paths[0]
        if non_raw_paths:
            return non_raw_paths[0]
        if raw_paths:
            return raw_paths[0]
    else:
        if raw_paths:
            return raw_paths[0]
        if supported_paths:
            return supported_paths[0]

    return ordered_paths[0]


def _selected_source_kind(selected_path: Path, *, has_raw_companion: bool) -> str:
    suffix = selected_path.suffix.lower()
    if is_raw_image_path(selected_path):
        return IMAGE_IMPORT_SOURCE_KIND_RAW
    if suffix in {".heic", ".heif"}:
        return IMAGE_IMPORT_SOURCE_KIND_HEIC
    if suffix in SUPPORTED_JPEG_SUFFIXES and has_raw_companion:
        return IMAGE_IMPORT_SOURCE_KIND_CAMERA_JPEG
    if suffix in SUPPORTED_IMPORT_SUFFIXES:
        return IMAGE_IMPORT_SOURCE_KIND_RASTER
    return IMAGE_IMPORT_SOURCE_KIND_PROCESSED_RASTER


def _capture_metadata_for_paths(
    paths: Iterable[Path],
) -> tuple[datetime | None, float | None, float | None, Path | None, str | None]:
    candidate_paths = [_coerce_path(path) for path in paths or []]
    seen: set[str] = set()
    has_raw_companion = any(is_raw_image_path(path) for path in candidate_paths if path is not None)
    for path in candidate_paths:
        if path is None:
            continue
        path_text = str(path)
        if path_text in seen:
            continue
        seen.add(path_text)
        if is_raw_image_path(path):
            try:
                captured_at = read_rawpy_capture_datetime(str(path))
            except Exception:
                captured_at = None
            if captured_at is not None:
                return captured_at, None, None, path, IMAGE_IMPORT_SOURCE_KIND_RAW
            continue

        metadata = get_image_metadata(str(path))
        if not metadata:
            continue
        captured_at = metadata.get("datetime")
        latitude = metadata.get("latitude")
        longitude = metadata.get("longitude")
        if captured_at is not None or latitude is not None or longitude is not None:
            if path.suffix.lower() in {".heic", ".heif"}:
                source_kind = IMAGE_IMPORT_SOURCE_KIND_HEIC
            elif path.suffix.lower() in SUPPORTED_JPEG_SUFFIXES and has_raw_companion:
                source_kind = IMAGE_IMPORT_SOURCE_KIND_CAMERA_JPEG
            else:
                source_kind = IMAGE_IMPORT_SOURCE_KIND_RASTER
            return captured_at, latitude, longitude, path, source_kind
    return None, None, None, None, None


def _final_source_path(selected_path: Path, raw_path: Path | None) -> Path:
    return raw_path or selected_path


def _candidate_processing_metadata(
    candidate: "ImageImportCandidate",
    *,
    selected_kind: str | None = None,
) -> dict[str, Any]:
    selected = selected_kind or candidate.source_kind
    return {
        "status": candidate.status,
        "group": {
            "paths": [str(path) for path in candidate.companion_paths],
        },
        "source": {
            "path": str(candidate.source_path),
            "kind": candidate.source_kind,
            **({"captured_at": candidate.captured_at} if candidate.captured_at is not None else {}),
            **({"gps_latitude": candidate.gps_latitude} if candidate.gps_latitude is not None else {}),
            **({"gps_longitude": candidate.gps_longitude} if candidate.gps_longitude is not None else {}),
        },
        "captured_at_source": {
            **(
                {"path": str(candidate.captured_at_source_path)}
                if candidate.captured_at_source_path is not None
                else {}
            ),
            **(
                {"kind": candidate.captured_at_source_kind}
                if candidate.captured_at_source_kind
                else {}
            ),
        },
        "selected": {
            "path": str(candidate.selected_path),
            "kind": selected,
            **(
                {"policy": candidate.selected_source_policy}
                if candidate.selected_source_policy
                else {}
            ),
        },
        "companions": {
            "paths": [str(path) for path in candidate.companion_paths],
            **({"raw_path": str(candidate.raw_path)} if candidate.raw_path is not None else {}),
            **(
                {"camera_jpeg_path": str(candidate.camera_jpeg_path)}
                if candidate.camera_jpeg_path is not None
                else {}
            ),
            "has_raw_companion": candidate.has_raw_companion,
        },
    }


def _build_candidate(
    *,
    source_path: Path,
    selected_path: Path,
    source_kind: str,
    status: str,
    companion_paths: tuple[Path, ...],
    raw_path: Path | None,
    camera_jpeg_path: Path | None,
    has_raw_companion: bool,
    selected_source_policy: str,
    captured_at: datetime | None,
    gps_latitude: float | None,
    gps_longitude: float | None,
    captured_at_source_path: Path | None,
    captured_at_source_kind: str | None,
    failure_reason: str | None = None,
    error_detail: str | None = None,
) -> ImageImportCandidate:
    candidate = ImageImportCandidate(
        source_path=source_path,
        selected_path=selected_path,
        source_kind=source_kind,
        status=status,
        companion_paths=companion_paths,
        raw_path=raw_path,
        camera_jpeg_path=camera_jpeg_path,
        has_raw_companion=has_raw_companion,
        selected_source_policy=selected_source_policy,
        captured_at=captured_at,
        gps_latitude=gps_latitude,
        gps_longitude=gps_longitude,
        captured_at_source_path=captured_at_source_path,
        captured_at_source_kind=captured_at_source_kind,
        failure_reason=failure_reason,
        error_detail=error_detail,
    )
    candidate.processing_metadata = _candidate_processing_metadata(
        candidate,
        selected_kind=_selected_source_kind(selected_path, has_raw_companion=has_raw_companion),
    )
    return candidate


def _working_dimensions_from_result(result: Any, working_path: Path) -> tuple[int | None, int | None]:
    raw_render_snapshot = getattr(result, "raw_render_snapshot", None)
    if isinstance(raw_render_snapshot, Mapping):
        local_derivative = raw_render_snapshot.get("local_derivative")
        if isinstance(local_derivative, Mapping):
            width = local_derivative.get("width")
            height = local_derivative.get("height")
            try:
                width_value = int(width) if width is not None else None
            except Exception:
                width_value = None
            try:
                height_value = int(height) if height is not None else None
            except Exception:
                height_value = None
            if width_value is not None or height_value is not None:
                return width_value, height_value

    try:
        from PIL import Image

        with Image.open(working_path) as image:
            width, height = image.size
            return int(width), int(height)
    except Exception:
        return None, None


def _prepare_local_ingest_result(
    source_path: Path,
    *,
    raw_settings: RawRenderSettings | Mapping[str, Any] | None = None,
    lab_metadata: Mapping[str, Any] | None = None,
    output_dir: Path | None = None,
) -> Any:
    return prepare_local_ingest_image(
        str(source_path),
        raw_settings=raw_settings,
        lab_metadata=lab_metadata,
        output_dir=output_dir,
    )


def _best_fallback_path(candidate: "ImageImportCandidate") -> Path | None:
    if candidate.camera_jpeg_path is not None and candidate.camera_jpeg_path != candidate.selected_path:
        return candidate.camera_jpeg_path
    for path in _sort_paths(candidate.companion_paths):
        if path == candidate.selected_path:
            continue
        if path.suffix.lower() in SUPPORTED_IMPORT_SUFFIXES and not is_raw_image_path(path):
            return path
    return None


def _failure_reason_for_exception(exc: Exception, *, selected_path: Path) -> str:
    if isinstance(exc, RawRenderingUnavailableError):
        return "raw rendering unavailable"
    text = str(exc or "").strip()
    suffix = selected_path.suffix.lower()
    if is_raw_image_path(selected_path):
        return "raw rendering failed"
    if suffix in {".heic", ".heif"}:
        return "heic conversion failed"
    if suffix in SUPPORTED_RASTER_SUFFIXES:
        return "raster preparation failed"
    if text:
        return text
    return "image preparation failed"


def _failure_detail(exc: Exception) -> str:
    text = str(exc or "").strip()
    return text or exc.__class__.__name__


@dataclass(slots=True)
class ImageImportCandidate:
    """A staged import candidate that preserves companion-file context."""

    source_path: Path
    selected_path: Path
    working_path: Path | None = None
    preview_path: Path | None = None

    source_kind: str = IMAGE_IMPORT_SOURCE_KIND_RASTER
    status: str = IMAGE_IMPORT_STATUS_STAGED

    companion_paths: tuple[Path, ...] = ()
    raw_path: Path | None = None
    camera_jpeg_path: Path | None = None
    has_raw_companion: bool = False
    selected_source_policy: str | None = None

    captured_at: datetime | None = None
    gps_latitude: float | None = None
    gps_longitude: float | None = None
    captured_at_source_path: Path | None = None
    captured_at_source_kind: str | None = None
    working_width: int | None = None
    working_height: int | None = None

    processing_settings: RawRenderSettings | None = None
    processing_metadata: dict[str, Any] | None = None
    lab_metadata: dict[str, Any] = field(default_factory=dict)

    fallback_used: bool = False
    fallback_reason: str | None = None
    failure_reason: str | None = None
    error_detail: str | None = None

    def __post_init__(self) -> None:
        source_path = _coerce_path(self.source_path)
        selected_path = _coerce_path(self.selected_path)
        working_path = _coerce_path(self.working_path)
        preview_path = _coerce_path(self.preview_path)
        raw_path = _coerce_path(self.raw_path)
        camera_jpeg_path = _coerce_path(self.camera_jpeg_path)
        captured_at_source_path = _coerce_path(self.captured_at_source_path)

        self.source_path = source_path or Path(".")
        self.selected_path = selected_path or self.source_path
        self.working_path = working_path
        self.preview_path = preview_path
        self.raw_path = raw_path
        self.camera_jpeg_path = camera_jpeg_path
        self.captured_at_source_path = captured_at_source_path
        self.companion_paths = _dedupe_paths(self.companion_paths)
        self.source_kind = normalize_image_import_source_kind(self.source_kind)
        self.status = normalize_image_import_status(self.status)
        if self.selected_source_policy is not None:
            self.selected_source_policy = normalize_raw_companion_source_preference(self.selected_source_policy)
        if self.captured_at_source_kind is not None:
            self.captured_at_source_kind = normalize_image_import_source_kind(self.captured_at_source_kind)

        self.lab_metadata = _coerce_mapping(self.lab_metadata)
        self.processing_metadata = _coerce_mapping(self.processing_metadata) or None

        if isinstance(self.processing_settings, Mapping):
            try:
                self.processing_settings = RawRenderSettings.from_dict(self.processing_settings)
            except Exception:
                self.processing_settings = None
        elif self.processing_settings is not None and not isinstance(self.processing_settings, RawRenderSettings):
            to_dict = getattr(self.processing_settings, "to_dict", None)
            if callable(to_dict):
                try:
                    self.processing_settings = RawRenderSettings.from_dict(to_dict())
                except Exception:
                    self.processing_settings = None
            else:
                self.processing_settings = None

    @property
    def is_failed(self) -> bool:
        return self.status == IMAGE_IMPORT_STATUS_FAILED

    @property
    def is_pending(self) -> bool:
        return self.status in {IMAGE_IMPORT_STATUS_STAGED, IMAGE_IMPORT_STATUS_READY}

    @property
    def is_raw_backed(self) -> bool:
        return bool(self.raw_path is not None or self.source_kind == IMAGE_IMPORT_SOURCE_KIND_RAW or self.has_raw_companion)

    @property
    def is_processable(self) -> bool:
        return self.status in {IMAGE_IMPORT_STATUS_STAGED, IMAGE_IMPORT_STATUS_READY}

    @property
    def display_name(self) -> str:
        path = self.selected_path or self.source_path
        return path.name if path else ""

    @property
    def primary_preview_path(self) -> Path | None:
        return self.preview_path or self.working_path or self.selected_path or self.source_path

    @property
    def all_source_paths(self) -> tuple[Path, ...]:
        ordered: list[Path] = []
        for path in (
            self.source_path,
            self.selected_path,
            self.raw_path,
            self.camera_jpeg_path,
            *self.companion_paths,
        ):
            if path is None:
                continue
            if any(existing == path for existing in ordered):
                continue
            ordered.append(path)
        return tuple(ordered)


def build_image_import_candidates(
    paths: Iterable[str | Path],
    *,
    source_preference: str,
) -> list[ImageImportCandidate]:
    """Group local files into staged candidates without collapsing companions."""
    normalized_preference = normalize_raw_companion_source_preference(source_preference)
    grouped_paths: dict[str, list[Path]] = {}
    order: list[str] = []
    seen_paths: set[str] = set()

    for raw_path in paths or []:
        normalized = _coerce_path(raw_path)
        if normalized is None:
            continue
        normalized_text = str(normalized)
        if normalized_text in seen_paths:
            continue
        seen_paths.add(normalized_text)
        group_key = companion_group_key(normalized)
        if group_key not in grouped_paths:
            grouped_paths[group_key] = []
            order.append(group_key)
        grouped_paths[group_key].append(normalized)

    candidates: list[ImageImportCandidate] = []
    for group_key in order:
        group_paths = _sort_paths(_dedupe_paths(grouped_paths.get(group_key, [])))
        if not group_paths:
            continue

        supported_paths = [path for path in group_paths if _is_supported_import_suffix(path)]
        selected_path = _select_group_path(group_paths, source_preference=normalized_preference) or group_paths[0]
        raw_paths = _raw_paths(supported_paths)
        jpeg_paths = _jpeg_paths(supported_paths)
        has_raw_companion = bool(raw_paths)
        source_path = _final_source_path(selected_path, raw_paths[0] if raw_paths else None)
        source_kind = _selected_source_kind(source_path, has_raw_companion=False)
        (
            captured_at,
            gps_latitude,
            gps_longitude,
            captured_at_source_path,
            captured_at_source_kind,
        ) = _capture_metadata_for_paths(
            [selected_path, source_path, *group_paths]
        )
        status = IMAGE_IMPORT_STATUS_STAGED if supported_paths else IMAGE_IMPORT_STATUS_SKIPPED
        failure_reason = None
        error_detail = None
        if not supported_paths:
            failure_reason = "unsupported file suffix"
            error_detail = f"Unsupported image suffix for {selected_path.name}"

        candidates.append(
            _build_candidate(
                source_path=source_path,
                selected_path=selected_path,
                source_kind=source_kind,
                status=status,
                companion_paths=tuple(group_paths),
                raw_path=raw_paths[0] if raw_paths else None,
                camera_jpeg_path=jpeg_paths[0] if jpeg_paths else None,
                has_raw_companion=has_raw_companion,
                selected_source_policy=normalized_preference,
                captured_at=captured_at,
                gps_latitude=gps_latitude,
                gps_longitude=gps_longitude,
                captured_at_source_path=captured_at_source_path,
                captured_at_source_kind=captured_at_source_kind,
                failure_reason=failure_reason,
                error_detail=error_detail,
            )
        )
    return candidates


def prepare_image_import_candidate(
    candidate: ImageImportCandidate,
    *,
    raw_settings: RawRenderSettings | None = None,
    lab_metadata: Mapping[str, Any] | None = None,
    output_dir: Path | None = None,
    allow_raw_render: bool = True,
    allow_heic_convert: bool = True,
) -> ImageImportCandidate:
    """Prepare one candidate without committing it."""
    prepared = deepcopy(candidate)
    merged_lab_metadata = merge_image_lab_metadata(prepared.lab_metadata, lab_metadata)
    prepared.lab_metadata = merged_lab_metadata

    if prepared.status in {IMAGE_IMPORT_STATUS_FAILED, IMAGE_IMPORT_STATUS_SKIPPED, IMAGE_IMPORT_STATUS_COMMITTED}:
        prepared.lab_metadata = merged_lab_metadata
        if prepared.processing_metadata is None:
            prepared.processing_metadata = _candidate_processing_metadata(prepared)
        else:
            prepared.processing_metadata = merge_image_lab_metadata(
                prepared.processing_metadata,
                _candidate_processing_metadata(prepared),
            )
        return prepared

    selected_path = _coerce_path(prepared.selected_path) or prepared.selected_path
    if selected_path is None:
        prepared.status = IMAGE_IMPORT_STATUS_FAILED
        prepared.failure_reason = "missing source file"
        prepared.error_detail = "No source path was provided"
        prepared.processing_metadata = merge_image_lab_metadata(
            prepared.processing_metadata,
            _candidate_processing_metadata(prepared),
            {"failure": {"reason": prepared.failure_reason, "detail": prepared.error_detail}},
        )
        return prepared

    selected_exists = False
    try:
        selected_exists = bool(selected_path.exists() and selected_path.is_file())
    except Exception:
        selected_exists = False

    if not selected_exists:
        prepared.status = IMAGE_IMPORT_STATUS_FAILED
        prepared.failure_reason = "missing source file"
        prepared.error_detail = str(selected_path)
        prepared.processing_metadata = merge_image_lab_metadata(
            prepared.processing_metadata,
            _candidate_processing_metadata(prepared),
            {"failure": {"reason": prepared.failure_reason, "detail": prepared.error_detail}},
        )
        return prepared

    effective_raw_settings = raw_settings
    if effective_raw_settings is None:
        effective_raw_settings = prepared.processing_settings

    if is_raw_image_path(selected_path):
        if allow_raw_render:
            try:
                ingest_result = _prepare_local_ingest_result(
                    selected_path,
                    raw_settings=effective_raw_settings,
                    lab_metadata=prepared.lab_metadata,
                    output_dir=output_dir,
                )
            except Exception as exc:
                fallback_path = _best_fallback_path(prepared)
                if fallback_path is None:
                    prepared.status = IMAGE_IMPORT_STATUS_FAILED
                    prepared.failure_reason = _failure_reason_for_exception(exc, selected_path=selected_path)
                    prepared.error_detail = _failure_detail(exc)
                    prepared.processing_metadata = merge_image_lab_metadata(
                        prepared.processing_metadata,
                        _candidate_processing_metadata(prepared, selected_kind=prepared.source_kind),
                        {
                            "failure": {
                                "reason": prepared.failure_reason,
                                "detail": prepared.error_detail,
                                "selected_path": str(selected_path),
                            }
                        },
                    )
                    return prepared

                fallback_reason = _failure_reason_for_exception(exc, selected_path=selected_path)
                try:
                    ingest_result = _prepare_local_ingest_result(
                        fallback_path,
                        lab_metadata=prepared.lab_metadata,
                        output_dir=output_dir,
                    )
                except Exception as fallback_exc:
                    prepared.status = IMAGE_IMPORT_STATUS_FAILED
                    prepared.failure_reason = _failure_reason_for_exception(fallback_exc, selected_path=fallback_path)
                    prepared.error_detail = _failure_detail(fallback_exc)
                    prepared.processing_metadata = merge_image_lab_metadata(
                        prepared.processing_metadata,
                        _candidate_processing_metadata(prepared, selected_kind=prepared.source_kind),
                        {
                            "fallback": {
                                "used": True,
                                "reason": fallback_reason,
                                "source_path": str(selected_path),
                                "fallback_path": str(fallback_path),
                                "raw_failure": _failure_detail(exc),
                            },
                            "failure": {
                                "reason": prepared.failure_reason,
                                "detail": prepared.error_detail,
                                "selected_path": str(fallback_path),
                            },
                        },
                    )
                    return prepared

                prepared.fallback_used = True
                prepared.fallback_reason = fallback_reason
                selected_path = fallback_path
                prepared.selected_path = fallback_path
                prepared.selected_source_policy = prepared.selected_source_policy or RAW_COMPANION_SOURCE_PREFERENCE_PREFER_RAW
            else:
                prepared.selected_path = selected_path
        else:
            fallback_path = _best_fallback_path(prepared)
            if fallback_path is None:
                prepared.status = IMAGE_IMPORT_STATUS_SKIPPED
                prepared.failure_reason = "raw rendering disabled"
                prepared.error_detail = str(selected_path)
                prepared.processing_metadata = merge_image_lab_metadata(
                    prepared.processing_metadata,
                    _candidate_processing_metadata(prepared, selected_kind=prepared.source_kind),
                    {
                        "failure": {
                            "reason": prepared.failure_reason,
                            "detail": prepared.error_detail,
                            "selected_path": str(selected_path),
                        }
                    },
                )
                return prepared
            try:
                ingest_result = _prepare_local_ingest_result(
                    fallback_path,
                    lab_metadata=prepared.lab_metadata,
                    output_dir=output_dir,
                )
            except Exception as exc:
                prepared.status = IMAGE_IMPORT_STATUS_FAILED
                prepared.failure_reason = _failure_reason_for_exception(exc, selected_path=fallback_path)
                prepared.error_detail = _failure_detail(exc)
                prepared.processing_metadata = merge_image_lab_metadata(
                    prepared.processing_metadata,
                    _candidate_processing_metadata(prepared, selected_kind=prepared.source_kind),
                    {
                        "fallback": {
                            "used": True,
                            "reason": "raw rendering disabled",
                            "source_path": str(selected_path),
                            "fallback_path": str(fallback_path),
                        },
                        "failure": {
                            "reason": prepared.failure_reason,
                            "detail": prepared.error_detail,
                            "selected_path": str(fallback_path),
                        },
                    },
                )
                return prepared

            prepared.fallback_used = True
            prepared.fallback_reason = "raw rendering disabled"
            selected_path = fallback_path
            prepared.selected_path = fallback_path
    elif selected_path.suffix.lower() in {".heic", ".heif"}:
        if not allow_heic_convert:
            prepared.status = IMAGE_IMPORT_STATUS_SKIPPED
            prepared.failure_reason = "heic conversion disabled"
            prepared.error_detail = str(selected_path)
            prepared.processing_metadata = merge_image_lab_metadata(
                prepared.processing_metadata,
                _candidate_processing_metadata(prepared, selected_kind=prepared.source_kind),
                {
                    "failure": {
                        "reason": prepared.failure_reason,
                        "detail": prepared.error_detail,
                        "selected_path": str(selected_path),
                    }
                },
            )
            return prepared
        try:
            ingest_result = _prepare_local_ingest_result(
                selected_path,
                lab_metadata=prepared.lab_metadata,
                output_dir=output_dir,
            )
        except Exception as exc:
            prepared.status = IMAGE_IMPORT_STATUS_FAILED
            prepared.failure_reason = _failure_reason_for_exception(exc, selected_path=selected_path)
            prepared.error_detail = _failure_detail(exc)
            prepared.processing_metadata = merge_image_lab_metadata(
                prepared.processing_metadata,
                _candidate_processing_metadata(prepared, selected_kind=prepared.source_kind),
                {
                    "failure": {
                        "reason": prepared.failure_reason,
                        "detail": prepared.error_detail,
                        "selected_path": str(selected_path),
                    }
                },
            )
            return prepared
    elif selected_path.suffix.lower() in SUPPORTED_IMPORT_SUFFIXES:
        try:
            ingest_result = _prepare_local_ingest_result(
                selected_path,
                lab_metadata=prepared.lab_metadata,
                output_dir=output_dir,
            )
        except Exception as exc:
            prepared.status = IMAGE_IMPORT_STATUS_FAILED
            prepared.failure_reason = _failure_reason_for_exception(exc, selected_path=selected_path)
            prepared.error_detail = _failure_detail(exc)
            prepared.processing_metadata = merge_image_lab_metadata(
                prepared.processing_metadata,
                _candidate_processing_metadata(prepared, selected_kind=prepared.source_kind),
                {
                    "failure": {
                        "reason": prepared.failure_reason,
                        "detail": prepared.error_detail,
                        "selected_path": str(selected_path),
                    }
                },
            )
            return prepared
    else:
        prepared.status = IMAGE_IMPORT_STATUS_SKIPPED
        prepared.failure_reason = "unsupported file suffix"
        prepared.error_detail = str(selected_path)
        prepared.processing_metadata = merge_image_lab_metadata(
            prepared.processing_metadata,
            _candidate_processing_metadata(prepared, selected_kind=prepared.source_kind),
            {
                "failure": {
                    "reason": prepared.failure_reason,
                    "detail": prepared.error_detail,
                    "selected_path": str(selected_path),
                }
            },
        )
        return prepared

    working_path = _coerce_path(getattr(ingest_result, "working_path", None))
    if working_path is None:
        prepared.status = IMAGE_IMPORT_STATUS_FAILED
        prepared.failure_reason = "missing working path"
        prepared.error_detail = "prepare_local_ingest_image() did not return a working path"
        prepared.processing_metadata = merge_image_lab_metadata(
            prepared.processing_metadata,
            _candidate_processing_metadata(prepared, selected_kind=prepared.source_kind),
            {
                "failure": {
                    "reason": prepared.failure_reason,
                    "detail": prepared.error_detail,
                    "selected_path": str(selected_path),
                }
            },
        )
        return prepared

    prepared.status = IMAGE_IMPORT_STATUS_READY
    prepared.working_path = working_path
    if prepared.preview_path is None:
        prepared.preview_path = working_path

    result_lab_metadata = getattr(ingest_result, "lab_metadata", None)
    prepared.lab_metadata = merge_image_lab_metadata(prepared.lab_metadata, result_lab_metadata)
    provenance_kwargs = {}
    provenance_helper = getattr(ingest_result, "provenance_kwargs", None)
    if callable(provenance_helper):
        try:
            provenance_kwargs = dict(provenance_helper() or {})
        except Exception:
            provenance_kwargs = {}

    working_width, working_height = _working_dimensions_from_result(ingest_result, working_path)
    prepared.working_width = working_width
    prepared.working_height = working_height

    final_kind = _selected_source_kind(selected_path, has_raw_companion=prepared.has_raw_companion)
    if prepared.fallback_used and prepared.fallback_reason:
        fallback_payload = {
            "used": True,
            "reason": prepared.fallback_reason,
            "source_path": str(prepared.source_path),
            "fallback_path": str(selected_path),
        }
    else:
        fallback_payload = {}

    processing_update: dict[str, Any] = {
        "status": prepared.status,
        "source": {
            "path": str(prepared.source_path),
            "kind": prepared.source_kind,
            **({"captured_at": prepared.captured_at} if prepared.captured_at is not None else {}),
            **({"gps_latitude": prepared.gps_latitude} if prepared.gps_latitude is not None else {}),
            **({"gps_longitude": prepared.gps_longitude} if prepared.gps_longitude is not None else {}),
        },
        "captured_at_source": {
            **(
                {"path": str(prepared.captured_at_source_path)}
                if prepared.captured_at_source_path is not None
                else {}
            ),
            **(
                {"kind": prepared.captured_at_source_kind}
                if prepared.captured_at_source_kind
                else {}
            ),
        },
        "selected": {
            "path": str(selected_path),
            "kind": final_kind,
            **(
                {"policy": prepared.selected_source_policy}
                if prepared.selected_source_policy
                else {}
            ),
        },
        "working": {
            "path": str(working_path),
            **({"width": working_width} if working_width is not None else {}),
            **({"height": working_height} if working_height is not None else {}),
        },
    }
    if provenance_kwargs:
        processing_update["provenance"] = provenance_kwargs
    raw_render_snapshot = getattr(ingest_result, "raw_render_snapshot", None)
    if isinstance(raw_render_snapshot, Mapping):
        processing_update["raw_processing"] = dict(raw_render_snapshot)
        try:
            prepared.processing_settings = RawRenderSettings.from_dict(raw_render_snapshot.get("settings"))
        except Exception:
            prepared.processing_settings = prepared.processing_settings
    if fallback_payload:
        processing_update["fallback"] = fallback_payload
    prepared.processing_metadata = merge_image_lab_metadata(
        prepared.processing_metadata,
        _candidate_processing_metadata(prepared, selected_kind=final_kind),
        processing_update,
    )
    if prepared.fallback_used:
        prepared.lab_metadata.pop("raw_processing", None)
        if isinstance(prepared.processing_metadata, dict):
            prepared.processing_metadata.pop("raw_processing", None)
    prepared.failure_reason = None
    prepared.error_detail = None
    return prepared


def prepare_image_import_candidates(
    candidates: Iterable[ImageImportCandidate],
    *,
    raw_settings: RawRenderSettings | None = None,
    lab_metadata: Mapping[str, Any] | None = None,
    output_dir: Path | None = None,
    allow_raw_render: bool = True,
    allow_heic_convert: bool = True,
) -> list[ImageImportCandidate]:
    """Prepare a batch of candidates without dropping failed items."""
    prepared_candidates: list[ImageImportCandidate] = []
    for candidate in candidates or []:
        try:
            prepared_candidate = prepare_image_import_candidate(
                candidate,
                raw_settings=raw_settings,
                lab_metadata=lab_metadata,
                output_dir=output_dir,
                allow_raw_render=allow_raw_render,
                allow_heic_convert=allow_heic_convert,
            )
        except Exception as exc:
            prepared_candidate = deepcopy(candidate)
            prepared_candidate.status = IMAGE_IMPORT_STATUS_FAILED
            prepared_candidate.failure_reason = "candidate preparation failed"
            prepared_candidate.error_detail = _failure_detail(exc)
            prepared_candidate.processing_metadata = merge_image_lab_metadata(
                prepared_candidate.processing_metadata,
                _candidate_processing_metadata(prepared_candidate),
                {
                    "failure": {
                        "reason": prepared_candidate.failure_reason,
                        "detail": prepared_candidate.error_detail,
                    }
                },
            )
        prepared_candidates.append(prepared_candidate)
    return prepared_candidates


__all__ = [
    "IMAGE_IMPORT_SOURCE_KIND_CAMERA_JPEG",
    "IMAGE_IMPORT_SOURCE_KIND_HEIC",
    "IMAGE_IMPORT_SOURCE_KIND_PROCESSED_RASTER",
    "IMAGE_IMPORT_SOURCE_KIND_RASTER",
    "IMAGE_IMPORT_SOURCE_KIND_RAW",
    "IMAGE_IMPORT_STATUS_COMMITTED",
    "IMAGE_IMPORT_STATUS_FAILED",
    "IMAGE_IMPORT_STATUS_READY",
    "IMAGE_IMPORT_STATUS_SKIPPED",
    "IMAGE_IMPORT_STATUS_STAGED",
    "ImageImportCandidate",
    "ImageImportSourceKind",
    "ImageImportStatus",
    "SUPPORTED_IMPORT_SUFFIXES",
    "SUPPORTED_JPEG_SUFFIXES",
    "SUPPORTED_RASTER_SUFFIXES",
    "build_image_import_candidates",
    "normalize_image_import_source_kind",
    "normalize_image_import_status",
    "prepare_image_import_candidate",
    "prepare_image_import_candidates",
]
