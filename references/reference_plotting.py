"""Qt-free translation from persisted observation reference-use snapshots
into Analysis reference-series wrapper entries.

An ``ObservationReferenceUse`` stores a canonical, public-safe snapshot
that captures the exact core/exceptional length/width bounds, any
supplied means, raw points (only for ``raw_points`` data kind), and the
work's short label. This module maps that snapshot to the wrapper shape
consumed by ``MainWindow.reference_series``:

    {"key": <use-uuid>, "label": <short_label>, "data": {...}}

Range/summary snapshots map core bounds to ``length_p05/p95`` etc. and
exceptional bounds to ``length_min/max`` exactly. ``length_p50`` and
``width_p50`` are populated only when the snapshot supplies the
matching ``length_mean``/``width_mean``. Range/summary entries never
include a ``points`` key.

``raw_points`` snapshots translate only genuine paired numeric points
into the existing ``length_um``/``width_um`` point structure.
"""
from __future__ import annotations

import json
from typing import Any, Iterable


_RANGE_LIKE_DATA_KINDS = frozenset({"range", "summary"})


def _decode_snapshot(snapshot_json: str) -> dict | None:
    if not isinstance(snapshot_json, str) or not snapshot_json.strip():
        return None
    try:
        parsed = json.loads(snapshot_json)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed


def _coerce_use(use: Any) -> tuple[str, str, int, dict] | None:
    """Extract ``(use_id, role, reference_revision, snapshot)`` from either
    an ``ObservationReferenceUse`` dataclass or a plain dict with the
    same field names. Returns ``None`` if any required piece is missing.
    """
    if use is None:
        return None
    if isinstance(use, dict):
        use_id = use.get("id")
        role = use.get("role")
        revision = use.get("reference_revision")
        snapshot_raw = use.get("snapshot_json")
        snapshot_dict = use.get("snapshot")
    else:
        use_id = getattr(use, "id", None)
        role = getattr(use, "role", None)
        revision = getattr(use, "reference_revision", None)
        snapshot_raw = getattr(use, "snapshot_json", None)
        snapshot_dict = None
    if not use_id or not role:
        return None
    if isinstance(snapshot_dict, dict) and snapshot_dict:
        snapshot = snapshot_dict
    elif isinstance(snapshot_raw, str):
        snapshot = _decode_snapshot(snapshot_raw)
    else:
        snapshot = None
    if not isinstance(snapshot, dict) or not snapshot:
        return None
    try:
        revision_int = int(revision) if revision is not None else 0
    except (TypeError, ValueError):
        revision_int = 0
    return (str(use_id), str(role), revision_int, snapshot)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _split_scientific_name(name: str | None) -> tuple[str, str]:
    if not name:
        return "", ""
    parts = str(name).strip().split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def _translate_range_or_summary(
    use_id: str,
    role: str,
    revision: int,
    snapshot: dict,
) -> dict:
    measurements = snapshot.get("measurements") or {}
    if not isinstance(measurements, dict):
        measurements = {}
    length_p05 = _float_or_none(measurements.get("length_core_min"))
    length_p95 = _float_or_none(measurements.get("length_core_max"))
    length_min = _float_or_none(measurements.get("length_min"))
    length_max = _float_or_none(measurements.get("length_max"))
    width_p05 = _float_or_none(measurements.get("width_core_min"))
    width_p95 = _float_or_none(measurements.get("width_core_max"))
    width_min = _float_or_none(measurements.get("width_min"))
    width_max = _float_or_none(measurements.get("width_max"))
    length_mean = _float_or_none(measurements.get("length_mean"))
    width_mean = _float_or_none(measurements.get("width_mean"))
    q_mean = _float_or_none(measurements.get("q_mean"))
    q_min = _float_or_none(measurements.get("q_min"))
    q_max = _float_or_none(measurements.get("q_max"))
    sample_size = measurements.get("sample_size")

    short_label = str(snapshot.get("short_label") or "").strip()
    name_as_published = str(snapshot.get("name_as_published") or "").strip()
    genus, species = _split_scientific_name(name_as_published)

    data_kind = str(snapshot.get("data_kind") or "").strip() or "range"
    data: dict[str, Any] = {
        "source_kind": "reference",
        "observation_reference_use_id": use_id,
        "reference_measurement_set_id": str(
            snapshot.get("reference_measurement_set_id") or ""
        ),
        "reference_treatment_id": str(
            snapshot.get("reference_treatment_id") or ""
        ),
        "reference_work_id": str(snapshot.get("reference_work_id") or ""),
        "role": role,
        "reference_revision": revision,
        "reference_data_kind": data_kind,
        "short_label": short_label,
        "source": short_label,
        "name_as_published": name_as_published,
        "genus": genus,
        "species": species,
    }
    if snapshot.get("raw_text"):
        data["raw_text"] = str(snapshot.get("raw_text"))
    if snapshot.get("locator_text"):
        data["locator_text"] = str(snapshot.get("locator_text"))

    # Range/summary bounds: exceptional (min/max) stay verbatim, core
    # bounds map to p05/p95. Only include a p50 when the snapshot itself
    # supplied a mean (never synthesise a midpoint).
    if length_p05 is not None:
        data["length_p05"] = length_p05
    if length_p95 is not None:
        data["length_p95"] = length_p95
    if length_min is not None:
        data["length_min"] = length_min
    if length_max is not None:
        data["length_max"] = length_max
    if width_p05 is not None:
        data["width_p05"] = width_p05
    if width_p95 is not None:
        data["width_p95"] = width_p95
    if width_min is not None:
        data["width_min"] = width_min
    if width_max is not None:
        data["width_max"] = width_max
    if length_mean is not None:
        data["length_p50"] = length_mean
        data["length_mean"] = length_mean
    if width_mean is not None:
        data["width_p50"] = width_mean
        data["width_mean"] = width_mean
    if q_mean is not None:
        data["q_p50"] = q_mean
    if q_min is not None:
        data["q_min"] = q_min
    if q_max is not None:
        data["q_max"] = q_max
    if isinstance(sample_size, int) and sample_size > 0:
        data["sample_size"] = int(sample_size)

    return {
        "key": use_id,
        "label": short_label or name_as_published or "",
        "data": data,
        "enabled": True,
    }


def _translate_raw_points(
    use_id: str,
    role: str,
    revision: int,
    snapshot: dict,
) -> dict | None:
    raw_points = snapshot.get("raw_points")
    if not isinstance(raw_points, list) or not raw_points:
        return None
    points: list[dict] = []
    for entry in raw_points:
        if not isinstance(entry, dict):
            continue
        length = entry.get("length")
        if length is None:
            length = entry.get("l")
        width = entry.get("width")
        if width is None:
            width = entry.get("w")
        length_float = _float_or_none(length)
        width_float = _float_or_none(width)
        # Keep only genuine paired numeric points; discard incomplete pairs.
        if length_float is None or width_float is None:
            continue
        if length_float <= 0 or width_float <= 0:
            continue
        points.append({"length_um": length_float, "width_um": width_float})
    if not points:
        return None

    short_label = str(snapshot.get("short_label") or "").strip()
    name_as_published = str(snapshot.get("name_as_published") or "").strip()
    genus, species = _split_scientific_name(name_as_published)
    data: dict[str, Any] = {
        "source_kind": "points",
        "observation_reference_use_id": use_id,
        "reference_measurement_set_id": str(
            snapshot.get("reference_measurement_set_id") or ""
        ),
        "reference_treatment_id": str(
            snapshot.get("reference_treatment_id") or ""
        ),
        "reference_work_id": str(snapshot.get("reference_work_id") or ""),
        "role": role,
        "reference_revision": revision,
        "reference_data_kind": "raw_points",
        "short_label": short_label,
        "points": points,
        "points_label": short_label,
        "source_type": "reference_library",
        "genus": genus,
        "species": species,
    }
    if snapshot.get("locator_text"):
        data["locator_text"] = str(snapshot.get("locator_text"))
    return {
        "key": use_id,
        "label": short_label or name_as_published or "",
        "data": data,
        "enabled": True,
    }


def translate_observation_reference_use(use: Any) -> dict | None:
    """Translate a single ``ObservationReferenceUse`` snapshot into an
    Analysis reference-series wrapper entry.

    Returns ``None`` for malformed or unsupported snapshots so the caller
    can surface a warning without crashing observation opening.
    """
    coerced = _coerce_use(use)
    if coerced is None:
        return None
    use_id, role, revision, snapshot = coerced
    data_kind = str(snapshot.get("data_kind") or "").strip().lower()
    if data_kind == "raw_points":
        return _translate_raw_points(use_id, role, revision, snapshot)
    if data_kind in _RANGE_LIKE_DATA_KINDS or not data_kind:
        # Range and summary share the range grammar. Empty data_kind is
        # treated as a range for legacy snapshots that omitted the field.
        result = _translate_range_or_summary(use_id, role, revision, snapshot)
        # If none of the L/W bounds landed, the snapshot is degenerate.
        data = result["data"]
        has_bounds = any(
            key in data
            for key in (
                "length_p05",
                "length_p95",
                "length_min",
                "length_max",
                "width_p05",
                "width_p95",
                "width_min",
                "width_max",
                "length_mean",
                "width_mean",
            )
        )
        if not has_bounds:
            return None
        return result
    # ``parmasto`` and any future kinds are not part of this vertical
    # slice; skip cleanly so the caller can log a warning.
    return None


def translate_observation_reference_uses(uses: Iterable[Any]) -> list[dict]:
    """Best-effort bulk translator that drops entries producing None."""
    results: list[dict] = []
    for use in uses or []:
        entry = translate_observation_reference_use(use)
        if entry is not None:
            results.append(entry)
    return results


__all__ = [
    "translate_observation_reference_use",
    "translate_observation_reference_uses",
]
