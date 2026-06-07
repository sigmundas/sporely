"""Deterministic RAW preset key helpers."""
from __future__ import annotations

import re
from collections.abc import Mapping


_CONTEXT_KEY_ORDER = (
    "camera_model",
    "microscope",
    "contrast_mode",
    "stain",
    "mountant",
    "sample_type",
    "objective",
    "magnification",
)

_CONTEXT_ALIASES = {
    "camera_model": ("camera_model", "camera", "camera_name"),
    "microscope": ("microscope", "microscope_model", "microscope_name"),
    "contrast_mode": ("contrast_mode", "contrast", "contrast_label"),
    "stain": ("stain", "stain_label"),
    "mountant": ("mountant", "mount_medium", "mount_medium_label"),
    "sample_type": ("sample_type", "sample", "sample_label"),
    "objective": ("objective", "objective_name", "objective_label"),
    "magnification": ("magnification", "objective_magnification", "objective_power"),
}

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _clean_text(value) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return re.sub(r"\s+", " ", text)


def _slugify(value: str | None) -> str | None:
    cleaned = _clean_text(value)
    if cleaned is None:
        return None
    slug = _SLUG_RE.sub("_", cleaned.casefold()).strip("_")
    return slug or None


def normalize_raw_context(context: Mapping[str, object] | None) -> dict[str, str]:
    """Normalize context fields that should participate in the preset key."""
    if not isinstance(context, Mapping):
        return {}

    normalized: dict[str, str] = {}
    for canonical_key in _CONTEXT_KEY_ORDER:
        for alias in _CONTEXT_ALIASES[canonical_key]:
            if alias not in context:
                continue
            cleaned = _clean_text(context.get(alias))
            if cleaned is None:
                continue
            normalized[canonical_key] = cleaned
            break
    return normalized


def build_raw_preset_key(context: Mapping[str, object] | None) -> str:
    """Build a deterministic preset key from normalized context fields."""
    normalized = normalize_raw_context(context)
    parts: list[str] = []
    for canonical_key in _CONTEXT_KEY_ORDER:
        slug = _slugify(normalized.get(canonical_key))
        if slug:
            parts.append(f"{canonical_key}={slug}")
    suffix = "|".join(parts) if parts else "default"
    return f"raw-preset:v1:{suffix}"


__all__ = [
    "build_raw_preset_key",
    "normalize_raw_context",
]
