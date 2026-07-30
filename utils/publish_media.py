"""Shared preparation and lifetime management for external publish media.

Files managed here are disposable derivatives.  They are deliberately kept
outside the observation image collection and must never be treated as source
media.
"""
from __future__ import annotations

import math
import shutil
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

from utils.publish_media_cache import (
    PublishMediaCache,
    publish_media_signature,
    validate_cached_image,
)


MOSAIC_RENDERER_VERSION = "1"
ANNOTATED_IMAGE_RENDERER_VERSION = "1"
TARGET_VARIANT_RENDERER_VERSION = "1"

_CACHE_CLEANUP_LOCK = threading.Lock()
_CLEANED_CACHE_ROOTS: set[str] = set()


def cleanup_publish_media_cache_once(
    cache: PublishMediaCache,
) -> dict[str, int] | None:
    """Run the cache's modest expiry policy once per root and process.

    Cleanup is maintenance only: failures are deliberately swallowed so a
    cache permission problem or malformed stale entry can never block
    publishing.
    """
    try:
        root_key = str(cache.root.expanduser().resolve(strict=False))
    except Exception:
        root_key = str(cache.root)
    with _CACHE_CLEANUP_LOCK:
        if root_key in _CLEANED_CACHE_ROOTS:
            return None
        _CLEANED_CACHE_ROOTS.add(root_key)
    try:
        return cache.cleanup()
    except Exception:
        return None


def normalize_measurement_category(category: str | None) -> str:
    text = str(category or "").strip().lower()
    if not text:
        return "spores"
    if text in {"spore", "spores", "manual"}:
        return "spores"
    return text


def filter_mosaic_measurements(
    measurements: Iterable[dict],
    category: str | None,
) -> list[dict]:
    """Return category-eligible, renderable measurements.

    Callers may apply an additional UI-specific selection first (for example
    the Analysis point/bin filter).  This shared stage intentionally handles
    the rules that must agree between Analysis export and external publishing.
    """
    normalized_category = str(category or "all").strip().lower() or "all"
    result: list[dict] = []
    for measurement in measurements or ():
        measurement_type = normalize_measurement_category(
            measurement.get("measurement_type")
        )
        if measurement_type == "calibration":
            continue
        if normalized_category not in {"all", ""}:
            wanted = normalize_measurement_category(normalized_category)
            if measurement_type != wanted:
                continue
        if not all(
            measurement.get(f"p{point}_{axis}") is not None
            for point in range(1, 5)
            for axis in ("x", "y")
        ):
            continue
        result.append(measurement)
    return result


def _stable_int(value, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _stable_float(value) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def ordered_mosaic_measurements(
    measurements: Iterable[dict],
    *,
    sort_key: str | None,
    image_order: Iterable[int] = (),
) -> list[dict]:
    """Return canonical mosaic order with deterministic tie-breaking."""
    rows = list(measurements or ())
    image_positions = {
        _stable_int(image_id, -1): index
        for index, image_id in enumerate(image_order or ())
    }
    selected_sort = str(sort_key or "").strip().lower()

    def stable_tail(measurement: dict, input_index: int) -> tuple[int, int, int]:
        image_id = _stable_int(measurement.get("image_id"), 2**63 - 1)
        measurement_id = _stable_int(measurement.get("id"), 2**63 - 1)
        return measurement_id, image_id, input_index

    indexed = list(enumerate(rows))

    def key(item: tuple[int, dict]):
        input_index, measurement = item
        tail = stable_tail(measurement, input_index)
        length = _stable_float(measurement.get("length_um"))
        width = _stable_float(measurement.get("width_um"))
        if selected_sort == "images":
            image_id = _stable_int(measurement.get("image_id"), -1)
            return (image_positions.get(image_id, 2**63 - 1),) + tail
        if selected_sort == "length":
            return (length,) + tail
        if selected_sort == "width":
            return (width,) + tail
        if selected_sort == "q":
            return ((length / width) if width else 0.0,) + tail
        return (str(measurement.get("measured_at") or ""),) + tail

    return [measurement for _index, measurement in sorted(indexed, key=key)]


def prepare_ordered_mosaic_inputs(
    measurements: Iterable[dict],
    *,
    category: str | None,
    sort_key: str | None,
    image_order: Iterable[int] = (),
) -> list[dict]:
    """Canonical eligible-input and ordering pipeline for every mosaic."""
    return ordered_mosaic_measurements(
        filter_mosaic_measurements(measurements, category),
        sort_key=sort_key,
        image_order=image_order,
    )


def source_file_identity(path: str | Path, *, media_id=None) -> dict:
    """Return a stable, inexpensive identity for a local source file.

    Canonical observation images do not currently store a durable content
    checksum.  The disposable cache therefore uses the database media id plus
    file size and nanosecond mtime.  Replacing bytes while deliberately
    preserving both stat values can produce a stale cache hit; clearing the
    cache recovers safely, and normal image edits/replacements change mtime.
    """
    candidate = Path(path)
    identity = {"media_id": media_id}
    try:
        stat = candidate.stat()
    except OSError:
        identity["missing"] = True
        return identity
    identity.update(
        {
            "size": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
        }
    )
    return identity


def mosaic_dependencies(
    *,
    observation_id: int,
    measurements: Iterable[dict],
    image_rows: dict[int, dict],
    settings: dict,
    render_options: dict,
) -> dict:
    """Build deterministic dependencies for a rendered publish mosaic."""
    measurement_dependencies = []
    for measurement in measurements:
        image_id = _stable_int(measurement.get("image_id"), 0)
        image_row = image_rows.get(image_id, {})
        source_path = (
            measurement.get("image_filepath")
            or image_row.get("filepath")
            or image_row.get("original_filepath")
            or ""
        )
        measurement_dependencies.append(
            {
                "id": measurement.get("id"),
                "image_id": image_id,
                "source": source_file_identity(source_path, media_id=image_id),
                "points": [
                    [
                        measurement.get(f"p{point}_x"),
                        measurement.get(f"p{point}_y"),
                    ]
                    for point in range(1, 5)
                ],
                "length_um": measurement.get("length_um"),
                "width_um": measurement.get("width_um"),
                "gallery_rotation": int(measurement.get("gallery_rotation") or 0),
                "scale_microns_per_pixel": image_row.get(
                    "scale_microns_per_pixel"
                ),
                "measure_color": image_row.get("measure_color"),
            }
        )
    return {
        "observation_id": int(observation_id),
        "measurements": measurement_dependencies,
        "settings": {
            "measurement_type": settings.get("measurement_type", "all"),
            "gallery_sort": settings.get("gallery_sort", ""),
            "orient": bool(settings.get("orient", True)),
            "uniform_scale": bool(settings.get("uniform_scale", False)),
        },
        "render": dict(render_options),
    }


def annotated_image_dependencies(
    *,
    image_row: dict,
    source_path: str | Path,
    measurements: Iterable[dict],
    preferences: dict,
    render_options: dict,
) -> dict:
    """Build deterministic dependencies for one baked annotation image."""
    overlay_dependencies = []
    for measurement in measurements:
        overlay_dependencies.append(
            {
                "id": measurement.get("id"),
                "measurement_type": normalize_measurement_category(
                    measurement.get("measurement_type")
                ),
                "points": [
                    [
                        measurement.get(f"p{point}_x"),
                        measurement.get(f"p{point}_y"),
                    ]
                    for point in range(1, 5)
                ],
                "length_um": measurement.get("length_um"),
                "width_um": measurement.get("width_um"),
            }
        )
    overlay_dependencies.sort(
        key=lambda row: _stable_int(row.get("id"), 2**63 - 1)
    )
    return {
        "image_id": image_row.get("id"),
        "source": source_file_identity(
            source_path,
            media_id=image_row.get("id"),
        ),
        "calibration": {
            "scale_microns_per_pixel": image_row.get("scale_microns_per_pixel"),
            "scale_bar_length": image_row.get("scale_bar_length"),
            "scale_bar_unit": image_row.get("scale_bar_unit"),
        },
        "measure_color": image_row.get("measure_color"),
        "measurements": overlay_dependencies,
        "preferences": {
            "show_overlays": bool(preferences.get("show_overlays")),
            "show_labels": bool(preferences.get("show_labels")),
            "show_scale_bar": bool(preferences.get("show_scale_bar")),
            "scale_bar_um": preferences.get("scale_bar_um"),
        },
        "render": dict(render_options),
    }


@dataclass(frozen=True)
class ResolvedPublishAsset:
    path: Path
    signature: str
    from_cache: bool


class PublishMediaBundle:
    """Operation view over persistent shared assets and temporary variants."""

    def __init__(
        self,
        observation_id: int,
        *,
        cache: PublishMediaCache | None = None,
    ):
        self.observation_id = int(observation_id)
        self.cache = cache or PublishMediaCache()
        cleanup_publish_media_cache_once(self.cache)
        self._operation_dir: Path | None = None
        self._resolved: dict[tuple[str, str, str], ResolvedPublishAsset] = {}

    def __enter__(self) -> "PublishMediaBundle":
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.close()

    @property
    def operation_dir(self) -> Path:
        if self._operation_dir is None:
            self._operation_dir = Path(
                tempfile.mkdtemp(prefix=f"sporely_publish_{self.observation_id}_")
            )
        return self._operation_dir

    def temporary_path(self, filename: str) -> Path:
        safe_name = Path(filename).name
        if safe_name != filename or safe_name in {"", ".", ".."}:
            raise ValueError("Temporary publish filename must be a safe basename")
        return self.operation_dir / safe_name

    def resolve_cached_image(
        self,
        *,
        asset_kind: str,
        renderer_version: str,
        dependencies: dict,
        extension: str,
        render: Callable[[Path], bool | None],
    ) -> ResolvedPublishAsset:
        signature = publish_media_signature(
            asset_kind,
            renderer_version,
            dependencies,
        )
        memo_key = (asset_kind, signature, extension)
        memoized = self._resolved.get(memo_key)
        if memoized is not None and validate_cached_image(memoized.path):
            return memoized

        cached = self.cache.lookup(
            asset_kind,
            signature,
            extension,
            validator=validate_cached_image,
        )
        if cached is not None:
            resolved = ResolvedPublishAsset(cached, signature, True)
            self._resolved[memo_key] = resolved
            return resolved

        render_path = self.temporary_path(
            f"{asset_kind}_{signature}.{str(extension).lstrip('.')}"
        )
        rendered = render(render_path)
        if rendered is False or not validate_cached_image(render_path):
            raise ValueError(f"Publish-media renderer produced no valid {asset_kind}")
        stored = self.cache.store_file(
            asset_kind,
            signature,
            extension,
            render_path,
            validator=validate_cached_image,
        )
        resolved = ResolvedPublishAsset(stored, signature, False)
        self._resolved[memo_key] = resolved
        return resolved

    def close(self) -> None:
        operation_dir = self._operation_dir
        self._operation_dir = None
        if operation_dir is not None:
            shutil.rmtree(operation_dir, ignore_errors=True)
