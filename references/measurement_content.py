"""Shared typed contract for reference measurement content.

This module owns the rules and the codec of the measurement content contract
frozen in ``docs/reference-data/measurement-content-contract.md`` (Stage 1).
It owns no transactions: repositories, importers, reconciliation and the
parser call it; nothing here reads or writes a database.

Three responsibilities live here:

1. **Details object, version 1** (contract section 1) — the typed
   :class:`MeasurementDetails` tree, its JSON codec
   (:func:`decode_measurement_details`, :func:`encode_measurement_details`) and
   decoded-object equality (:func:`measurement_details_equal`).
2. **Validation of complete candidate content** (sections 1–2) —
   :func:`validate_measurement_content` checks the numeric columns and the
   descriptors of a :class:`MeasurementContent` together, in one of the two
   modes defined by the contract (``"edit"`` or ``"authoritative"``).
3. **Explicit edit operations** (plan: clear / switch / swap) — pure
   transitions on :class:`MeasurementContent` that return a new value. They
   are deliberately separate from validation: an editor applies transitions,
   then validates the result with ``mode="edit"``; an import path validates
   the transported state with ``mode="authoritative"`` and never applies
   transitions.

The module imports nothing heavier than ``json``, ``math`` and ``dataclasses``
so that ``database/`` may depend on it without a cycle.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, fields, replace
from typing import Any, Literal, Mapping

# --- Constants (contract section 3 and the section 14 spec block) ------------

MEASUREMENT_DETAILS_SCHEMA_VERSION = 1
SUPPORTED_DETAILS_VERSIONS: frozenset[int] = frozenset({1})
MEASUREMENT_DETAILS_MAX_BYTES = 4096  # canonical encoding

EXTENSION_FIELDS: tuple[str, str, str] = (
    "measurement_details_json",
    "q_core_min",
    "q_core_max",
)

METRICS: tuple[str, str, str] = ("length", "width", "q")
METRIC_KEYS: frozenset[str] = frozenset(
    {"outer_range", "core_range", "mean_interval", "median", "sd"}
)

OUTER_RANGE_KINDS: frozenset[str] = frozenset({"reported_extremes"})
CORE_RANGE_KINDS: frozenset[str] = frozenset(
    {"unspecified", "typical_range", "reported_range", "percentile_interval"}
)
INTERVAL_KINDS: frozenset[str] = frozenset({"reported_range", "typical_range"})

SCIENTIFIC_CONTENT_FIELDS: frozenset[str] = frozenset(
    {
        "character",
        "data_kind",
        "raw_text",
        "length_min",
        "length_core_min",
        "length_core_max",
        "length_max",
        "width_min",
        "width_core_min",
        "width_core_max",
        "width_max",
        "q_min",
        "q_core_min",
        "q_core_max",
        "q_max",
        "q_mean",
        "length_mean",
        "width_mean",
        "sample_size",
        "specimen_count",
        "mount_medium",
        "stain",
        "preparation",
        "measurement_method",
        "raw_points_json",
        "measurement_details_json",
    }
)

# Column pairs each descriptor describes (contract section 2).
OUTER_PAIR: Mapping[str, tuple[str, str]] = {
    "length": ("length_min", "length_max"),
    "width": ("width_min", "width_max"),
    "q": ("q_min", "q_max"),
}
CORE_PAIR: Mapping[str, tuple[str, str]] = {
    "length": ("length_core_min", "length_core_max"),
    "width": ("width_core_min", "width_core_max"),
    "q": ("q_core_min", "q_core_max"),
}
SCALAR_MEAN: Mapping[str, str] = {
    "length": "length_mean",
    "width": "width_mean",
    "q": "q_mean",
}

_NUMERIC_COLUMNS: tuple[str, ...] = tuple(
    column
    for metric in METRICS
    for column in (*OUTER_PAIR[metric], *CORE_PAIR[metric], SCALAR_MEAN[metric])
)

ValidationMode = Literal["edit", "authoritative"]
_VALIDATION_MODES: frozenset[str] = frozenset({"edit", "authoritative"})


class MeasurementContentError(ValueError):
    """Raised when measurement content violates the frozen contract."""


# --- Typed details ------------------------------------------------------------


@dataclass(frozen=True)
class RangeDescriptor:
    """Meaning of an existing column pair; never carries the pair's values."""

    kind: str
    percentile_bounds: tuple[float, float] | None = None


@dataclass(frozen=True)
class IntervalStatistic:
    """A statistic reported as an interval. Equal endpoints stay an interval."""

    lower: float
    upper: float
    kind: str


@dataclass(frozen=True)
class ScalarStatistic:
    value: float


@dataclass(frozen=True)
class MetricDetails:
    outer_range: RangeDescriptor | None = None
    core_range: RangeDescriptor | None = None
    mean_interval: IntervalStatistic | None = None
    median: ScalarStatistic | IntervalStatistic | None = None
    sd: ScalarStatistic | None = None

    def is_empty(self) -> bool:
        return all(getattr(self, f.name) is None for f in fields(self))


@dataclass(frozen=True)
class MeasurementDetails:
    """Version-1 details object as a typed tree (contract section 1)."""

    metrics: Mapping[str, MetricDetails]

    def is_empty(self) -> bool:
        return all(metric.is_empty() for metric in self.metrics.values())


@dataclass(frozen=True)
class UnsupportedMeasurementDetails:
    """A decoded details object whose version this binary does not know.

    Preserved opaquely on authoritative paths; every edit operation and
    ``mode="edit"`` validation rejects it (contract section 3).
    """

    schema_version: int
    raw: Mapping[str, Any]


Details = MeasurementDetails | UnsupportedMeasurementDetails | None


@dataclass
class MeasurementContent:
    """Typed intermediate for the 26 scientific-content fields (section 5).

    ``measurement_details_json`` is represented decoded, as ``details``;
    :func:`content_from_row` and :func:`content_row_updates` translate to and
    from the stored column. Every field is optional so that the parser can
    emit content without knowing the owning row's ``character``.
    """

    character: str | None = None
    data_kind: str | None = None
    raw_text: str | None = None
    length_min: float | None = None
    length_core_min: float | None = None
    length_core_max: float | None = None
    length_max: float | None = None
    width_min: float | None = None
    width_core_min: float | None = None
    width_core_max: float | None = None
    width_max: float | None = None
    q_min: float | None = None
    q_core_min: float | None = None
    q_core_max: float | None = None
    q_max: float | None = None
    q_mean: float | None = None
    length_mean: float | None = None
    width_mean: float | None = None
    sample_size: int | None = None
    specimen_count: int | None = None
    mount_medium: str | None = None
    stain: str | None = None
    preparation: str | None = None
    measurement_method: str | None = None
    raw_points_json: str | None = None
    details: Details = None


_CONTENT_COLUMN_FIELDS: tuple[str, ...] = tuple(
    f.name for f in fields(MeasurementContent) if f.name != "details"
)


# --- Codec --------------------------------------------------------------------


def _canonical_json(obj: Any) -> str:
    """The single contract codec (same settings as ``_canonical_json`` in
    ``database/reference_sync_state.py``)."""
    return json.dumps(obj, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _descriptor_from_object(obj: Any, *, where: str) -> RangeDescriptor:
    if not isinstance(obj, dict):
        raise MeasurementContentError(f"{where} must be an object")
    keys = set(obj)
    if keys == {"kind"}:
        return RangeDescriptor(kind=obj["kind"])
    if keys == {"kind", "percentile_bounds"}:
        bounds = obj["percentile_bounds"]
        if not isinstance(bounds, (list, tuple)) or len(bounds) != 2:
            raise MeasurementContentError(
                f"{where}.percentile_bounds must be a two-element list"
            )
        return RangeDescriptor(kind=obj["kind"], percentile_bounds=(bounds[0], bounds[1]))
    raise MeasurementContentError(f"{where} has unexpected keys {sorted(keys)}")


def _interval_from_object(obj: Any, *, where: str) -> IntervalStatistic:
    if not isinstance(obj, dict) or set(obj) != {"lower", "upper", "kind"}:
        raise MeasurementContentError(
            f"{where} must be an object with exactly lower, upper and kind"
        )
    return IntervalStatistic(lower=obj["lower"], upper=obj["upper"], kind=obj["kind"])


def _scalar_from_object(obj: Any, *, where: str) -> ScalarStatistic:
    if not isinstance(obj, dict) or set(obj) != {"value"}:
        raise MeasurementContentError(f"{where} must be an object with exactly value")
    return ScalarStatistic(value=obj["value"])


def _metric_from_object(obj: Any, *, metric: str) -> MetricDetails:
    where = f"metrics.{metric}"
    if not isinstance(obj, dict):
        raise MeasurementContentError(f"{where} must be an object")
    unknown = set(obj) - METRIC_KEYS
    if unknown:
        raise MeasurementContentError(f"{where} has unknown keys {sorted(unknown)}")
    median_obj = obj.get("median")
    median: ScalarStatistic | IntervalStatistic | None = None
    if median_obj is not None:
        if isinstance(median_obj, dict) and set(median_obj) == {"value"}:
            median = _scalar_from_object(median_obj, where=f"{where}.median")
        else:
            # Anything that is not exactly a scalar must be exactly an
            # interval: ``value`` mixed with ``lower``/``upper`` is rejected.
            median = _interval_from_object(median_obj, where=f"{where}.median")
    return MetricDetails(
        outer_range=(
            _descriptor_from_object(obj["outer_range"], where=f"{where}.outer_range")
            if "outer_range" in obj
            else None
        ),
        core_range=(
            _descriptor_from_object(obj["core_range"], where=f"{where}.core_range")
            if "core_range" in obj
            else None
        ),
        mean_interval=(
            _interval_from_object(obj["mean_interval"], where=f"{where}.mean_interval")
            if "mean_interval" in obj
            else None
        ),
        median=median,
        sd=_scalar_from_object(obj["sd"], where=f"{where}.sd") if "sd" in obj else None,
    )


def _details_from_object(obj: Mapping[str, Any]) -> MeasurementDetails:
    if set(obj) != {"schema_version", "metrics"}:
        raise MeasurementContentError(
            "measurement details must have exactly schema_version and metrics"
        )
    metrics_obj = obj["metrics"]
    if not isinstance(metrics_obj, dict):
        raise MeasurementContentError("measurement details metrics must be an object")
    unknown = set(metrics_obj) - set(METRICS)
    if unknown:
        raise MeasurementContentError(
            f"measurement details has unknown metrics {sorted(unknown)}"
        )
    metrics = {
        metric: _metric_from_object(metrics_obj[metric], metric=metric)
        for metric in METRICS
        if metric in metrics_obj
    }
    return MeasurementDetails(metrics=metrics)


def decode_measurement_details(
    text: str | None,
) -> MeasurementDetails | UnsupportedMeasurementDetails | None:
    """Decode stored ``measurement_details_json``.

    ``None``, empty text and JSON ``null`` decode to ``None``. A well-formed
    object whose ``schema_version`` is not supported decodes to
    :class:`UnsupportedMeasurementDetails` carrying the raw object. Malformed
    JSON or a version-1 object of the wrong *shape* raises
    :class:`MeasurementContentError`; semantic rules (enum values, ordering,
    signs, emptiness) are checked by :func:`validate_measurement_content`.
    """
    if text is None:
        return None
    if not isinstance(text, str):
        raise MeasurementContentError("measurement details must be JSON text")
    if not text.strip():
        return None
    try:
        obj = json.loads(text)
    except ValueError as exc:
        raise MeasurementContentError("measurement details are not valid JSON") from exc
    if obj is None:
        return None
    if not isinstance(obj, dict):
        raise MeasurementContentError("measurement details must be a JSON object")
    version = obj.get("schema_version")
    if isinstance(version, bool) or not isinstance(version, int):
        raise MeasurementContentError("measurement details schema_version must be an integer")
    if version not in SUPPORTED_DETAILS_VERSIONS:
        return UnsupportedMeasurementDetails(schema_version=version, raw=obj)
    return _details_from_object(obj)


def _descriptor_to_object(descriptor: RangeDescriptor) -> dict[str, Any]:
    obj: dict[str, Any] = {"kind": descriptor.kind}
    if descriptor.percentile_bounds is not None:
        obj["percentile_bounds"] = list(descriptor.percentile_bounds)
    return obj


def _statistic_to_object(statistic: ScalarStatistic | IntervalStatistic) -> dict[str, Any]:
    if isinstance(statistic, ScalarStatistic):
        return {"value": statistic.value}
    return {"lower": statistic.lower, "upper": statistic.upper, "kind": statistic.kind}


def details_to_object(details: MeasurementDetails) -> dict[str, Any] | None:
    """Plain JSON-ready object for supported details; ``None`` when empty."""
    unknown = set(details.metrics) - set(METRICS)
    if unknown:
        raise MeasurementContentError(
            f"measurement details has unknown metrics {sorted(unknown)}"
        )
    metrics: dict[str, Any] = {}
    for metric in METRICS:
        body = details.metrics.get(metric)
        if body is None or body.is_empty():
            continue
        obj: dict[str, Any] = {}
        if body.outer_range is not None:
            obj["outer_range"] = _descriptor_to_object(body.outer_range)
        if body.core_range is not None:
            obj["core_range"] = _descriptor_to_object(body.core_range)
        if body.mean_interval is not None:
            obj["mean_interval"] = _statistic_to_object(body.mean_interval)
        if body.median is not None:
            obj["median"] = _statistic_to_object(body.median)
        if body.sd is not None:
            obj["sd"] = _statistic_to_object(body.sd)
        metrics[metric] = obj
    if not metrics:
        return None
    return {"schema_version": MEASUREMENT_DETAILS_SCHEMA_VERSION, "metrics": metrics}


def encode_measurement_details(details: Details) -> str | None:
    """Canonical text for storage. ``None`` for ``None`` or semantically empty
    details; unsupported details re-encode their raw object unchanged."""
    if details is None:
        return None
    if isinstance(details, UnsupportedMeasurementDetails):
        return _canonical_json(details.raw)
    if not isinstance(details, MeasurementDetails):
        raise MeasurementContentError("details must be MeasurementDetails or None")
    obj = details_to_object(details)
    if obj is None:
        return None
    return _canonical_json(obj)


def measurement_details_equal(a: str | None, b: str | None) -> bool:
    """Decoded-object equality; NULL equals NULL. Never rewrites either side."""
    if a is None or b is None:
        return a is None and b is None
    try:
        return json.loads(a) == json.loads(b)
    except ValueError:
        return a == b


# --- Row helpers --------------------------------------------------------------


def content_from_row(row: Mapping[str, Any]) -> MeasurementContent:
    """Typed content from a row mapping (``asdict(MeasurementSet)``, a
    ``sqlite3.Row`` converted to ``dict``, an import row).

    Missing keys read as ``None``. Acknowledgement (key presence) must be
    decided with :func:`acknowledgement_state` *before* calling this, because
    this normalizes absence to ``None``.
    """
    values = {name: row.get(name) for name in _CONTENT_COLUMN_FIELDS}
    return MeasurementContent(
        **values, details=decode_measurement_details(row.get("measurement_details_json"))
    )


def content_row_updates(content: MeasurementContent) -> dict[str, Any]:
    """The 26 scientific-content columns of ``content`` as a row mapping."""
    updates: dict[str, Any] = {
        name: getattr(content, name) for name in _CONTENT_COLUMN_FIELDS
    }
    updates["measurement_details_json"] = encode_measurement_details(content.details)
    return updates


def is_enhanced_row(row: Mapping[str, Any]) -> bool:
    return any(row.get(name) is not None for name in EXTENSION_FIELDS)


def acknowledgement_state(row: Mapping[str, Any]) -> Literal["absent", "complete", "partial"]:
    present = sum(1 for name in EXTENSION_FIELDS if name in row)
    if present == 0:
        return "absent"
    if present == len(EXTENSION_FIELDS):
        return "complete"
    return "partial"


def acknowledges_extension(row: Mapping[str, Any]) -> bool:
    return acknowledgement_state(row) == "complete"


# --- Import policy (contract section 8) ---------------------------------------

IMPORT_DECISIONS: tuple[str, ...] = (
    "reject_partial_extension",
    "skip_stale",
    "skip_unacknowledged",
    "equivalent_if_content_equal",
    "reject_unacknowledged_extension",
    "replace",
)

_JSON_CONTENT_FIELDS: frozenset[str] = frozenset({"raw_points_json", "measurement_details_json"})


def _json_content_equal(a: Any, b: Any) -> bool:
    """Decoded-object equality for JSON text columns; NULL equals NULL and
    unparsable text falls back to text equality. Never rewrites either side."""
    if a is None or b is None:
        return a is None and b is None
    try:
        return json.loads(a) == json.loads(b)
    except (TypeError, ValueError):
        return a == b


def scientific_content_equal(a: Mapping[str, Any], b: Mapping[str, Any]) -> bool:
    """Whether two rows carry the same scientific-content group (section 5).

    JSON columns compare by decoded object, everything else by value. A key
    missing on either side reads as NULL: an omitting historical source
    compares as the recognized NULL baseline (section 8). Acknowledgement is
    decided separately by :func:`import_decision`; this never inspects it.
    """
    for name in SCIENTIFIC_CONTENT_FIELDS:
        left, right = a.get(name), b.get(name)
        if name in _JSON_CONTENT_FIELDS:
            if not _json_content_equal(left, right):
                return False
        elif left != right:
            return False
    return True


def import_decision(
    incoming: Mapping[str, Any], destination: Mapping[str, Any] | None
) -> str:
    """Section 8 decision for a row arriving by bundle or portable import.

    ``incoming`` must be the source row *before* any key normalization so
    that acknowledgement can be read from key presence. ``destination`` is
    the existing row with the same id, or ``None``. The result is one of
    :data:`IMPORT_DECISIONS`; ``"equivalent_if_content_equal"`` leaves the
    group comparison (:func:`scientific_content_equal`) to the caller.
    Validation of accepted content is also the caller's step, in
    ``mode="authoritative"``.
    """
    if acknowledgement_state(incoming) == "partial":
        return "reject_partial_extension"
    if destination is None:
        return "replace"
    incoming_revision = int(incoming.get("revision") or 1)
    destination_revision = int(destination.get("revision") or 1)
    if incoming_revision < destination_revision:
        return "skip_stale"
    acknowledged = acknowledges_extension(incoming)
    destination_enhanced = is_enhanced_row(destination)
    if incoming_revision == destination_revision:
        if not acknowledged and destination_enhanced:
            return "skip_unacknowledged"
        return "equivalent_if_content_equal"
    if not acknowledged and destination_enhanced:
        return "reject_unacknowledged_extension"
    return "replace"


# --- Validation ---------------------------------------------------------------


def _is_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _require_number(value: Any, what: str) -> float:
    if not _is_number(value):
        raise MeasurementContentError(f"{what} must be a finite number")
    return value


def _require_positive(value: Any, what: str) -> float:
    _require_number(value, what)
    if value <= 0:
        raise MeasurementContentError(f"{what} must be positive")
    return value


def _validate_interval(interval: Any, what: str) -> None:
    if not isinstance(interval, IntervalStatistic):
        raise MeasurementContentError(f"{what} must be an interval statistic")
    if not isinstance(interval.kind, str) or interval.kind not in INTERVAL_KINDS:
        raise MeasurementContentError(f"{what}.kind must be one of {sorted(INTERVAL_KINDS)}")
    _require_positive(interval.lower, f"{what}.lower")
    _require_positive(interval.upper, f"{what}.upper")
    if interval.lower > interval.upper:
        raise MeasurementContentError(f"{what} endpoints must be ordered")


def _validate_metric_details(metric: str, body: MetricDetails, content: MeasurementContent) -> None:
    where = f"metrics.{metric}"
    if not isinstance(body, MetricDetails):
        raise MeasurementContentError(f"{where} must be MetricDetails")
    if body.is_empty():
        raise MeasurementContentError(f"{where} is empty")

    outer = body.outer_range
    if outer is not None:
        if not isinstance(outer, RangeDescriptor) or outer.kind not in OUTER_RANGE_KINDS:
            raise MeasurementContentError(
                f"{where}.outer_range.kind must be one of {sorted(OUTER_RANGE_KINDS)}"
            )
        if outer.percentile_bounds is not None:
            raise MeasurementContentError(f"{where}.outer_range takes no percentile bounds")
        # Rule 1: the outer pair is complete and ordered.
        lo_name, hi_name = OUTER_PAIR[metric]
        lo = _require_positive(getattr(content, lo_name), lo_name)
        hi = _require_positive(getattr(content, hi_name), hi_name)
        if lo > hi:
            raise MeasurementContentError(f"{lo_name} must not exceed {hi_name}")

    core = body.core_range
    if core is not None:
        if not isinstance(core, RangeDescriptor) or core.kind not in CORE_RANGE_KINDS:
            raise MeasurementContentError(
                f"{where}.core_range.kind must be one of {sorted(CORE_RANGE_KINDS)}"
            )
        if core.kind == "percentile_interval":
            bounds = core.percentile_bounds
            if bounds is None or len(bounds) != 2:
                raise MeasurementContentError(
                    f"{where}.core_range.percentile_bounds are required for percentile_interval"
                )
            p_lo = _require_number(bounds[0], f"{where}.core_range.percentile_bounds[0]")
            p_hi = _require_number(bounds[1], f"{where}.core_range.percentile_bounds[1]")
            if not (0 <= p_lo < p_hi <= 100):
                raise MeasurementContentError(
                    f"{where}.core_range.percentile_bounds must satisfy 0 <= lo < hi <= 100"
                )
        elif core.percentile_bounds is not None:
            raise MeasurementContentError(
                f"{where}.core_range.percentile_bounds are allowed only for percentile_interval"
            )
        # Rule 2: the core pair is complete and ordered.
        lo_name, hi_name = CORE_PAIR[metric]
        lo = _require_positive(getattr(content, lo_name), lo_name)
        hi = _require_positive(getattr(content, hi_name), hi_name)
        if lo > hi:
            raise MeasurementContentError(f"{lo_name} must not exceed {hi_name}")
        # Rule 3: explicit extremes enclose the core pair.
        if outer is not None:
            o_lo_name, o_hi_name = OUTER_PAIR[metric]
            if getattr(content, o_lo_name) > lo or hi > getattr(content, o_hi_name):
                raise MeasurementContentError(
                    f"{metric} outer range must enclose its core range"
                )

    if body.mean_interval is not None:
        _validate_interval(body.mean_interval, f"{where}.mean_interval")
        # Rule 4: a mean interval excludes the scalar mean.
        mean_name = SCALAR_MEAN[metric]
        if getattr(content, mean_name) is not None:
            raise MeasurementContentError(
                f"{mean_name} must be NULL when {metric} has a mean interval"
            )

    if body.median is not None:
        # Rule 5: scalar or interval, exclusively (enforced by type).
        if isinstance(body.median, ScalarStatistic):
            _require_positive(body.median.value, f"{where}.median.value")
        else:
            _validate_interval(body.median, f"{where}.median")

    if body.sd is not None:
        if not isinstance(body.sd, ScalarStatistic):
            raise MeasurementContentError(f"{where}.sd must be a scalar statistic")
        _require_number(body.sd.value, f"{where}.sd.value")
        if body.sd.value < 0:
            raise MeasurementContentError(f"{where}.sd.value must be non-negative")


def _validate_supported_details(details: MeasurementDetails, content: MeasurementContent) -> None:
    if not isinstance(details.metrics, Mapping) or not details.metrics:
        raise MeasurementContentError("measurement details must describe at least one metric")
    unknown = set(details.metrics) - set(METRICS)
    if unknown:
        raise MeasurementContentError(
            f"measurement details has unknown metrics {sorted(unknown)}"
        )
    for metric in METRICS:
        body = details.metrics.get(metric)
        if body is not None:
            _validate_metric_details(metric, body, content)


def _validate_columns(content: MeasurementContent) -> None:
    """Descriptor-independent numeric rules: finite, non-boolean, positive
    dimension and Q values (contract section 1) and ordered pairs. Applied in
    both modes to every details state."""
    for name in _NUMERIC_COLUMNS:
        value = getattr(content, name)
        if value is not None:
            _require_positive(value, name)
    for metric in METRICS:
        for lo_name, hi_name in (OUTER_PAIR[metric], CORE_PAIR[metric]):
            lo = getattr(content, lo_name)
            hi = getattr(content, hi_name)
            if lo is not None and hi is not None and lo > hi:
                raise MeasurementContentError(f"{lo_name} must not exceed {hi_name}")


def _check_size(encoded: str | None) -> None:
    if encoded is not None and len(encoded.encode("utf-8")) > MEASUREMENT_DETAILS_MAX_BYTES:
        raise MeasurementContentError(
            f"measurement details exceed {MEASUREMENT_DETAILS_MAX_BYTES} bytes"
        )


def validate_measurement_content(content: MeasurementContent, *, mode: ValidationMode) -> None:
    """Validate the complete candidate content (contract sections 1–2).

    ``mode="edit"`` (repository writes, editors, parser output) rejects
    :class:`UnsupportedMeasurementDetails`. ``mode="authoritative"`` (pull,
    bundle and portable import) accepts it opaquely and then enforces only the
    descriptor-independent numeric rules and the size limit.
    """
    if mode not in _VALIDATION_MODES:
        raise MeasurementContentError(f"unknown validation mode {mode!r}")
    if not isinstance(content, MeasurementContent):
        raise MeasurementContentError("content must be MeasurementContent")

    _validate_columns(content)

    details = content.details
    if details is None:
        return
    if isinstance(details, UnsupportedMeasurementDetails):
        if mode == "edit":
            raise MeasurementContentError("unsupported measurement details version")
        _check_size(encode_measurement_details(details))
        return
    if not isinstance(details, MeasurementDetails):
        raise MeasurementContentError("details must be MeasurementDetails or None")
    _validate_supported_details(details, content)
    _check_size(encode_measurement_details(details))


# --- Explicit edit operations -------------------------------------------------


def _require_editable(content: MeasurementContent) -> MeasurementDetails | None:
    if isinstance(content.details, UnsupportedMeasurementDetails):
        raise MeasurementContentError("unsupported measurement details version")
    if content.details is not None and not isinstance(content.details, MeasurementDetails):
        raise MeasurementContentError("details must be MeasurementDetails or None")
    return content.details


def _require_metric(metric: str) -> str:
    if metric not in METRICS:
        raise MeasurementContentError(f"unknown metric {metric!r}")
    return metric


def _with_metric(
    content: MeasurementContent, metric: str, body: MetricDetails, **column_updates: Any
) -> MeasurementContent:
    """Return ``content`` with ``metric``'s details replaced by ``body`` and
    columns updated. Empty metric objects and empty details normalize away."""
    details = _require_editable(content)
    metrics = dict(details.metrics) if details is not None else {}
    if body.is_empty():
        metrics.pop(metric, None)
    else:
        metrics[metric] = body
    new_details: MeasurementDetails | None = MeasurementDetails(metrics=metrics) if metrics else None
    return replace(content, details=new_details, **column_updates)


def _metric_body(content: MeasurementContent, metric: str) -> MetricDetails:
    details = _require_editable(content)
    if details is None:
        return MetricDetails()
    return details.metrics.get(metric) or MetricDetails()


def clear_pair(
    content: MeasurementContent, metric: str, which: Literal["outer", "core"]
) -> MeasurementContent:
    """Clear a column pair and its descriptor together."""
    _require_metric(metric)
    if which == "outer":
        lo_name, hi_name = OUTER_PAIR[metric]
        body = replace(_metric_body(content, metric), outer_range=None)
    elif which == "core":
        lo_name, hi_name = CORE_PAIR[metric]
        body = replace(_metric_body(content, metric), core_range=None)
    else:
        raise MeasurementContentError(f"unknown pair {which!r}")
    return _with_metric(content, metric, body, **{lo_name: None, hi_name: None})


def clear_statistic(
    content: MeasurementContent, metric: str, statistic: Literal["mean", "median", "sd"]
) -> MeasurementContent:
    """Clear a statistic. ``"mean"`` clears both the scalar column and the
    mean interval so no form of the statistic survives."""
    _require_metric(metric)
    body = _metric_body(content, metric)
    if statistic == "mean":
        return _with_metric(
            content, metric, replace(body, mean_interval=None), **{SCALAR_MEAN[metric]: None}
        )
    if statistic == "median":
        return _with_metric(content, metric, replace(body, median=None))
    if statistic == "sd":
        return _with_metric(content, metric, replace(body, sd=None))
    raise MeasurementContentError(f"unknown statistic {statistic!r}")


def set_scalar_mean(content: MeasurementContent, metric: str, value: float) -> MeasurementContent:
    """Switch the mean to a scalar; removes any mean interval."""
    _require_metric(metric)
    _require_positive(value, SCALAR_MEAN[metric])
    body = replace(_metric_body(content, metric), mean_interval=None)
    return _with_metric(content, metric, body, **{SCALAR_MEAN[metric]: value})


def set_mean_interval(
    content: MeasurementContent, metric: str, lower: float, upper: float, kind: str
) -> MeasurementContent:
    """Switch the mean to an interval; clears the scalar mean column."""
    _require_metric(metric)
    interval = IntervalStatistic(lower=lower, upper=upper, kind=kind)
    _validate_interval(interval, f"metrics.{metric}.mean_interval")
    body = replace(_metric_body(content, metric), mean_interval=interval)
    return _with_metric(content, metric, body, **{SCALAR_MEAN[metric]: None})


def swap_length_width(content: MeasurementContent) -> MeasurementContent:
    """Swap length and width: pairs, scalar means and descriptors move
    together so numbers never part from their interpretation. Q is unchanged."""
    details = _require_editable(content)
    swapped_columns: dict[str, Any] = {}
    for length_name, width_name in (
        *zip(OUTER_PAIR["length"], OUTER_PAIR["width"]),
        *zip(CORE_PAIR["length"], CORE_PAIR["width"]),
        (SCALAR_MEAN["length"], SCALAR_MEAN["width"]),
    ):
        swapped_columns[length_name] = getattr(content, width_name)
        swapped_columns[width_name] = getattr(content, length_name)
    new_details: MeasurementDetails | None = None
    if details is not None:
        metrics = dict(details.metrics)
        length_body = metrics.pop("length", None)
        width_body = metrics.pop("width", None)
        if width_body is not None and not width_body.is_empty():
            metrics["length"] = width_body
        if length_body is not None and not length_body.is_empty():
            metrics["width"] = length_body
        new_details = MeasurementDetails(metrics=metrics) if metrics else None
    return replace(content, details=new_details, **swapped_columns)


__all__ = (
    "CORE_PAIR",
    "CORE_RANGE_KINDS",
    "EXTENSION_FIELDS",
    "IMPORT_DECISIONS",
    "INTERVAL_KINDS",
    "MEASUREMENT_DETAILS_MAX_BYTES",
    "MEASUREMENT_DETAILS_SCHEMA_VERSION",
    "METRICS",
    "METRIC_KEYS",
    "OUTER_PAIR",
    "OUTER_RANGE_KINDS",
    "SCALAR_MEAN",
    "SCIENTIFIC_CONTENT_FIELDS",
    "SUPPORTED_DETAILS_VERSIONS",
    "IntervalStatistic",
    "MeasurementContent",
    "MeasurementContentError",
    "MeasurementDetails",
    "MetricDetails",
    "RangeDescriptor",
    "ScalarStatistic",
    "UnsupportedMeasurementDetails",
    "acknowledgement_state",
    "acknowledges_extension",
    "clear_pair",
    "clear_statistic",
    "content_from_row",
    "content_row_updates",
    "decode_measurement_details",
    "details_to_object",
    "encode_measurement_details",
    "import_decision",
    "is_enhanced_row",
    "measurement_details_equal",
    "scientific_content_equal",
    "set_mean_interval",
    "set_scalar_mean",
    "swap_length_width",
    "validate_measurement_content",
)
