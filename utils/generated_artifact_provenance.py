"""Generated artifact provenance descriptors."""
from __future__ import annotations

import re
from typing import Any

_GENERATED_ARTIFACT_SOURCE_ROLE = "generated_artifact"
_VALID_FILE_PURPOSES = {
    "thumbnail",
    "spore_crop",
    "plot",
    "plate",
    "calibration_overlay",
}


def _coerce_optional_int(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer or None")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer or None") from exc


def _coerce_optional_float(value: Any, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a number or None")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a number or None") from exc


def normalize_generated_artifact_file_purpose(file_purpose: Any) -> str:
    """Normalize a generated-artifact purpose name.

    Accepts common whitespace and hyphen variants, then validates against the
    supported vocabulary.
    """
    purpose_text = str(file_purpose or "").strip().lower()
    purpose_text = re.sub(r"[\s-]+", "_", purpose_text)
    purpose_text = re.sub(r"_+", "_", purpose_text).strip("_")
    if purpose_text not in _VALID_FILE_PURPOSES:
        raise ValueError(f"Unknown generated artifact file_purpose: {file_purpose!r}")
    return purpose_text


def _normalize_optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalize_crop_bbox(crop_bbox: Any) -> tuple[float, float, float, float] | None:
    if crop_bbox is None:
        return None
    try:
        x1, y1, x2, y2 = crop_bbox
    except Exception as exc:
        raise ValueError("crop_bbox must be a 4-item sequence or None") from exc
    normalized_values = (
        _coerce_optional_float(x1, "crop_bbox[0]"),
        _coerce_optional_float(y1, "crop_bbox[1]"),
        _coerce_optional_float(x2, "crop_bbox[2]"),
        _coerce_optional_float(y2, "crop_bbox[3]"),
    )
    if any(value is None for value in normalized_values):
        raise ValueError("crop_bbox values must be numbers")
    return (
        normalized_values[0],
        normalized_values[1],
        normalized_values[2],
        normalized_values[3],
    )


def build_generated_artifact_provenance(
    *,
    file_purpose: Any,
    source_image_id: Any = None,
    measurement_id: Any = None,
    annotation_id: Any = None,
    source_width: Any = None,
    source_height: Any = None,
    crop_bbox: Any = None,
    rotation_angle: Any = None,
    render_preset: Any = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a normalized provenance descriptor for a generated artifact."""
    normalized_metadata = dict(metadata or {})
    return {
        "source_role": _GENERATED_ARTIFACT_SOURCE_ROLE,
        "file_purpose": normalize_generated_artifact_file_purpose(file_purpose),
        "source_image_id": _coerce_optional_int(source_image_id, "source_image_id"),
        "measurement_id": _coerce_optional_int(measurement_id, "measurement_id"),
        "annotation_id": _coerce_optional_int(annotation_id, "annotation_id"),
        "source_width": _coerce_optional_int(source_width, "source_width"),
        "source_height": _coerce_optional_int(source_height, "source_height"),
        "crop_bbox": _normalize_crop_bbox(crop_bbox),
        "rotation_angle": _coerce_optional_float(rotation_angle, "rotation_angle"),
        "render_preset": _normalize_optional_text(render_preset),
        "metadata": normalized_metadata,
    }


__all__ = [
    "build_generated_artifact_provenance",
    "normalize_generated_artifact_file_purpose",
]
