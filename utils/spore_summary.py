"""Compute structured per-observation spore statistics summaries.

Stage C of the spore-statistics-species-profiles plan
(sporely-py/docs/spore-statistics-species-profiles.md). This module produces
payloads matching the public.observation_spore_summaries table contract
created by the Stage B migration.

Stage C scope is *pure computation*. It does not:
  * sync to Supabase (Stage D),
  * expose public RPCs (Stage E),
  * aggregate species profiles (Stage G),
  * or replace the legacy `observations.spore_statistics` literature string.

Percentile convention: numpy.percentile default (linear interpolation),
matching the legacy literature-string formatter which uses
`np.percentile(x, 5)` / `np.percentile(x, 95)`.

Standard-deviation convention: *sample* SD (ddof=1). If n < 2, sd is None.
This is a deliberate departure from the legacy `np.std(...)` (population SD,
ddof=0) used only inside the display-only literature string; that string is
untouched.

`q_mean` is the arithmetic mean of individual `length_i / width_i` ratios
across paired measurements. It is NOT `length_mean_um / width_mean_um`.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

import numpy as np

SPORE_SUMMARY_STATS_VERSION = 1
SPORE_SUMMARY_SOURCE_APP = "sporely-py"

# Fixed key order for the canonical context object. This order is part of
# the wire contract that drives context_hash; do not reorder. Stage B's
# observation_spore_summaries_measurement_context_idx uses the same column
# order so partial-prefix filters still hit the index.
CONTEXT_KEYS: tuple[str, ...] = (
    "measurement_type",
    "sample_type",
    "mount_reagent",
    "stain_reagent",
    "contrast_method",
)

# Rows from the local `spore_measurements` table whose `measurement_type`
# falls in this set are treated as spore biometric measurements — matches
# the filter used by MeasurementDB.get_statistics_for_observation and the
# legacy literature-string generator. `None` is also accepted (defaults to
# 'manual' at write time but older rows may be NULL).
_SPORE_ROW_MEASUREMENT_TYPES = frozenset({"", "manual", "spore", "spores"})

_WHITESPACE_RE = re.compile(r"\s+")


def normalize_context_value(value: Any) -> str | None:
    """Normalize a raw context field: coerce->strip->casefold->collapse WS.

    Empty strings become None. No alias/synonym lookup — the Stage A note
    documents that a broad synonym dictionary is deferred until we have
    grepped actual user-facing values.
    """
    if value is None:
        return None
    text = value if isinstance(value, str) else str(value)
    text = text.strip().casefold()
    text = _WHITESPACE_RE.sub(" ", text)
    return text or None


def build_context(
    *,
    measurement_type: Any = "spore",
    sample_type: Any = None,
    mount_reagent: Any = None,
    stain_reagent: Any = None,
    contrast_method: Any = None,
) -> dict[str, str | None]:
    """Return the canonical, normalized context dict in fixed key order.

    `measurement_type` defaults to 'spore' when the caller passes None/empty
    — this is the *summary* type (kind of structure), not the raw-row
    provenance stored in spore_measurements.measurement_type.
    """
    values = {
        "measurement_type": normalize_context_value(measurement_type) or "spore",
        "sample_type": normalize_context_value(sample_type),
        "mount_reagent": normalize_context_value(mount_reagent),
        "stain_reagent": normalize_context_value(stain_reagent),
        "contrast_method": normalize_context_value(contrast_method),
    }
    return {key: values[key] for key in CONTEXT_KEYS}


def serialize_context(context: Mapping[str, str | None]) -> str:
    """Canonical JSON for hashing.

    Uses the fixed CONTEXT_KEYS order (measurement_type, sample_type,
    mount_reagent, stain_reagent, contrast_method) — NOT alphabetical
    sort_keys. Passing any Mapping is safe: we rebuild the payload from
    CONTEXT_KEYS ourselves, so caller-side dict ordering does not affect
    the hash. Tight separators; unicode preserved via ensure_ascii=False.

    Do not switch to sort_keys=True — the wire contract is the fixed key
    order, so the resulting string is *not* alphabetical (alphabetical
    would start with `contrast_method`).
    """
    payload = {key: context.get(key) for key in CONTEXT_KEYS}
    return json.dumps(payload, sort_keys=False, separators=(",", ":"), ensure_ascii=False)


def compute_context_hash(context: Mapping[str, str | None]) -> str:
    """SHA-256 hex digest of the canonical context JSON."""
    return hashlib.sha256(serialize_context(context).encode("utf-8")).hexdigest()


def _is_spore_row(measurement_type: Any) -> bool:
    if measurement_type is None:
        return True
    if not isinstance(measurement_type, str):
        return False
    return measurement_type.strip().lower() in _SPORE_ROW_MEASUREMENT_TYPES


def _row_context(row: Mapping[str, Any]) -> dict[str, str | None]:
    """Derive the normalized summary context from a measurement row.

    Rows are expected to expose image context under either the local
    `images` column names (mount_medium / stain / contrast / sample_type)
    or the summary field names (mount_reagent / stain_reagent /
    contrast_method / sample_type). When both are present the summary
    field name wins.
    """
    def _pick(*keys: str) -> Any:
        for key in keys:
            if key in row and row[key] not in (None, ""):
                return row[key]
        return None

    return build_context(
        measurement_type="spore",
        sample_type=_pick("sample_type"),
        mount_reagent=_pick("mount_reagent", "mount_medium"),
        stain_reagent=_pick("stain_reagent", "stain"),
        contrast_method=_pick("contrast_method", "contrast"),
    )


def _finite_positive(value: Any) -> float | None:
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v) or v <= 0.0:
        return None
    return v


def _percentile_bundle(values: list[float]) -> dict[str, float | None]:
    """Compute min / p05 / mean / median / p95 / max / sd for `values`.

    Returns all-None if `values` is empty. sd is None when n < 2.
    """
    if not values:
        return {"min": None, "p05": None, "mean": None, "median": None,
                "p95": None, "max": None, "sd": None}
    arr = np.asarray(values, dtype=float)
    return {
        "min": float(np.min(arr)),
        "p05": float(np.percentile(arr, 5)),
        "mean": float(np.mean(arr)),
        "median": float(np.percentile(arr, 50)),
        "p95": float(np.percentile(arr, 95)),
        "max": float(np.max(arr)),
        "sd": float(np.std(arr, ddof=1)) if arr.size >= 2 else None,
    }


def _summary_row_from_group(
    *,
    observation_id: int,
    context: dict[str, str | None],
    rows: list[Mapping[str, Any]],
    computed_at_iso: str,
    source_app_version: str | None,
) -> dict[str, Any]:
    lengths: list[float] = []
    widths: list[float] = []
    paired_lengths: list[float] = []
    paired_widths: list[float] = []
    ratios: list[float] = []
    for row in rows:
        length = _finite_positive(row.get("length_um"))
        width = _finite_positive(row.get("width_um"))
        if length is not None:
            lengths.append(length)
        if width is not None:
            widths.append(width)
        if length is not None and width is not None:
            paired_lengths.append(length)
            paired_widths.append(width)
            ratios.append(length / width)

    # Canonical length/width/q stats all come from the same paired rows so
    # Lm, Wm and Qm share a single denominator. `n_length` / `n_width` still
    # report totals for all valid positive length/width values — those are
    # transparency counters only; adding length-only / width-only stats
    # would be a schema extension, not something to silently mix into the
    # canonical means. (Plan: Stage C patch.)
    length_stats = _percentile_bundle(paired_lengths)
    width_stats = _percentile_bundle(paired_widths)
    q_stats = _percentile_bundle(ratios)

    context_json = dict(context)  # already fixed-order

    payload: dict[str, Any] = {
        "observation_id": observation_id,
        "context_hash": compute_context_hash(context),
        "context_json": context_json,
        "measurement_type": context["measurement_type"],
        "sample_type": context["sample_type"],
        "mount_reagent": context["mount_reagent"],
        "stain_reagent": context["stain_reagent"],
        "contrast_method": context["contrast_method"],
        "n_spores": len(rows),
        "n_paired": len(ratios),
        "n_length": len(lengths),
        "n_width": len(widths),
        "length_min_um": length_stats["min"],
        "length_p05_um": length_stats["p05"],
        "length_mean_um": length_stats["mean"],
        "length_median_um": length_stats["median"],
        "length_p95_um": length_stats["p95"],
        "length_max_um": length_stats["max"],
        "length_sd_um": length_stats["sd"],
        "width_min_um": width_stats["min"],
        "width_p05_um": width_stats["p05"],
        "width_mean_um": width_stats["mean"],
        "width_median_um": width_stats["median"],
        "width_p95_um": width_stats["p95"],
        "width_max_um": width_stats["max"],
        "width_sd_um": width_stats["sd"],
        "q_min": q_stats["min"],
        "q_p05": q_stats["p05"],
        "q_mean": q_stats["mean"],
        "q_median": q_stats["median"],
        "q_p95": q_stats["p95"],
        "q_max": q_stats["max"],
        "q_sd": q_stats["sd"],
        "stats_version": SPORE_SUMMARY_STATS_VERSION,
        "computed_at": computed_at_iso,
        "source_app": SPORE_SUMMARY_SOURCE_APP,
        "source_app_version": source_app_version,
    }
    return payload


def compute_observation_spore_summaries(
    *,
    observation_id: int,
    measurements: Iterable[Mapping[str, Any]],
    computed_at: datetime | None = None,
    source_app_version: str | None = None,
) -> list[dict[str, Any]]:
    """Compute summary-row payloads for one observation.

    Each row in `measurements` must expose:
      * `length_um` (float | None)
      * `width_um` (float | None)
      * `measurement_type` (str | None) — row-level provenance from the
        `spore_measurements` table (e.g. 'manual', 'spore'). Non-spore
        rows are excluded.
      * image context fields (either `mount_medium`/`stain`/`contrast`
        /`sample_type` from the local `images` table, or the summary-side
        names `mount_reagent`/`stain_reagent`/`contrast_method`
        /`sample_type`).

    Returns one payload dict per distinct normalized context, keyed by
    `context_hash`. Never returns None; returns an empty list if there
    are no eligible measurements.

    `computed_at` defaults to now(UTC). `source_app_version` is not
    imported from main.py to avoid dragging in PySide6; Stage D callers
    should pass main.APP_VERSION.
    """
    if computed_at is None:
        computed_at = datetime.now(timezone.utc)
    computed_at_iso = computed_at.isoformat()

    groups: dict[str, dict[str, Any]] = {}
    for row in measurements:
        if not _is_spore_row(row.get("measurement_type")):
            continue
        context = _row_context(row)
        key = compute_context_hash(context)
        bucket = groups.setdefault(key, {"context": context, "rows": []})
        bucket["rows"].append(row)

    summaries = [
        _summary_row_from_group(
            observation_id=observation_id,
            context=bucket["context"],
            rows=bucket["rows"],
            computed_at_iso=computed_at_iso,
            source_app_version=source_app_version,
        )
        for bucket in groups.values()
    ]
    summaries.sort(key=lambda payload: payload["context_hash"])
    return summaries


def load_measurements_with_context(observation_id: int) -> list[dict[str, Any]]:
    """Load raw spore_measurements rows for `observation_id` with their
    parent image's context fields injected.

    Returned rows expose:
      id, image_id, length_um, width_um, measurement_type, measured_at,
      mount_medium, stain, sample_type, contrast, micro_category

    Stage C keeps this as a thin loader so the pure computation above
    stays trivially testable. Callers may substitute their own row list.
    """
    # Imported here to avoid a hard dependency on the app's database module
    # when this function is not called (e.g. from unit tests).
    from database.models import get_connection
    import sqlite3

    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                m.id,
                m.image_id,
                m.length_um,
                m.width_um,
                m.measurement_type,
                m.measured_at,
                i.mount_medium,
                i.stain,
                i.sample_type,
                i.contrast,
                i.micro_category
            FROM spore_measurements m
            JOIN images i ON i.id = m.image_id
            WHERE i.observation_id = ?
            ORDER BY m.measured_at
            """,
            (observation_id,),
        )
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()
