"""Helpers for combining image metadata dictionaries."""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any


def _clone_metadata_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _clone_metadata_value(subvalue) for key, subvalue in value.items() if subvalue is not None}
    return deepcopy(value)


def _merge_two_image_metadata_dicts(base: Mapping[str, Any], incoming: Mapping[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for key, value in base.items():
        if value is None:
            continue
        merged[key] = _clone_metadata_value(value)

    for key, value in incoming.items():
        if value is None:
            continue
        existing = merged.get(key)
        if isinstance(existing, Mapping) and isinstance(value, Mapping):
            merged[key] = _merge_two_image_metadata_dicts(existing, value)
        elif isinstance(value, Mapping):
            merged[key] = _merge_two_image_metadata_dicts({}, value)
        else:
            merged[key] = deepcopy(value)
    return merged


def merge_image_lab_metadata(*metadata_dicts: Mapping[str, Any] | None) -> dict[str, Any]:
    """Deep-merge image metadata dictionaries without mutating inputs.

    Earlier mappings provide defaults; later mappings override scalar values and
    recursively merge nested dictionaries. ``None`` inputs and non-mapping values
    are ignored safely.
    """

    merged: dict[str, Any] = {}
    for metadata in metadata_dicts:
        if not isinstance(metadata, Mapping):
            continue
        merged = _merge_two_image_metadata_dicts(merged, metadata)
    return merged


__all__ = ["merge_image_lab_metadata"]
