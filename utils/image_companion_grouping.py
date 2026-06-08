"""Helpers for grouping same-stem local image companions."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from config import (
    RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG,
    RAW_COMPANION_SOURCE_PREFERENCE_PREFER_RAW,
)
from utils.exif_reader import get_image_datetime
from utils.raw_detection import is_raw_image_path
from utils.rawpy_import import read_rawpy_capture_datetime


@dataclass(frozen=True, slots=True)
class CompanionGroup:
    """A same-stem group of local image files."""

    key: str
    paths: tuple[str, ...]
    preferred_path: str
    captured_at: datetime | None
    has_raw: bool


def companion_group_key(path: str | Path) -> str:
    """Return a stable grouping key for files that share a stem and parent folder."""
    candidate = Path(path)
    try:
        parent = str(candidate.resolve().parent)
    except Exception:
        parent = str(candidate.parent)
    return f"{parent}\0{candidate.stem.casefold()}"


def _normalize_path(path: str | Path | None) -> str:
    if path is None:
        return ""
    text = str(path).strip()
    if not text:
        return ""
    try:
        return str(Path(text).expanduser())
    except Exception:
        return text


def normalize_raw_companion_source_preference(value: str | None) -> str:
    text = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if text in {
        RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG,
        "camerajpeg",
        "camera_jpg",
        "jpeg",
        "jpg",
        "use_camera_jpeg",
    }:
        return RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG
    return RAW_COMPANION_SOURCE_PREFERENCE_PREFER_RAW


def _preferred_companion_path(paths: Iterable[str], *, source_preference: str | None = None) -> str | None:
    candidate_list = [str(path) for path in paths or [] if str(path).strip()]
    if not candidate_list:
        return None
    preference = normalize_raw_companion_source_preference(source_preference)
    if preference == RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG:
        for candidate in candidate_list:
            if not is_raw_image_path(candidate):
                return candidate
        return candidate_list[0]
    for candidate in candidate_list:
        if is_raw_image_path(candidate):
            return candidate
    return candidate_list[0]


def _capture_time_for_paths(paths: Iterable[str], preferred_path: str | None = None) -> datetime | None:
    ordered_paths = [str(path) for path in paths or [] if str(path).strip()]
    if not ordered_paths:
        return None
    if preferred_path:
        ordered_paths = [preferred_path] + [path for path in ordered_paths if path != preferred_path]

    for candidate in ordered_paths:
        captured_at = get_image_datetime(candidate)
        if captured_at is not None:
            return captured_at
        if is_raw_image_path(candidate):
            try:
                captured_at = read_rawpy_capture_datetime(candidate)
            except Exception:
                captured_at = None
            if captured_at is not None:
                return captured_at
    return None


def group_companion_paths(
    paths: Iterable[str | Path],
    *,
    source_preference: str | None = None,
) -> list[CompanionGroup]:
    """Collapse same-stem files into a single preferred companion candidate."""
    grouped: dict[str, list[str]] = {}
    order: list[str] = []
    seen_paths: set[str] = set()

    for raw_path in paths or []:
        normalized_path = _normalize_path(raw_path)
        if not normalized_path or normalized_path in seen_paths:
            continue
        path_obj = Path(normalized_path)
        if not path_obj.exists() or not path_obj.is_file():
            continue
        key = companion_group_key(path_obj)
        if key not in grouped:
            grouped[key] = []
            order.append(key)
        grouped[key].append(normalized_path)
        seen_paths.add(normalized_path)

    groups: list[CompanionGroup] = []
    for key in order:
        group_paths = tuple(grouped.get(key, []))
        preferred_path = _preferred_companion_path(group_paths, source_preference=source_preference)
        if preferred_path is None:
            continue
        groups.append(
            CompanionGroup(
                key=key,
                paths=group_paths,
                preferred_path=preferred_path,
                captured_at=_capture_time_for_paths(group_paths, preferred_path),
                has_raw=any(is_raw_image_path(path) for path in group_paths),
            )
        )
    return groups


def select_preferred_companion_path(
    paths: Iterable[str | Path],
    *,
    source_preference: str | None = None,
) -> str | None:
    """Return the preferred same-stem companion path using the requested source preference."""
    normalized_paths = [_normalize_path(path) for path in paths or []]
    normalized_paths = [path for path in normalized_paths if path]
    return _preferred_companion_path(normalized_paths, source_preference=source_preference)


__all__ = [
    "CompanionGroup",
    "companion_group_key",
    "group_companion_paths",
    "normalize_raw_companion_source_preference",
    "select_preferred_companion_path",
]
