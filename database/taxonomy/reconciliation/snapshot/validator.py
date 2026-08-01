"""Schema + privacy validator for the W2D anonymised snapshot contract.

The validator has two entry points:

* :func:`validate_record` — checks a single ``ReconciliationInput``-shaped dict
  and returns a list of :class:`ValidationError` records (empty on success).
* :func:`validate_snapshot` — walks a JSONL file, aggregates errors, and
  refuses the snapshot if any prohibited private field is present or the
  pseudonymous references are not unique.

A snapshot that passes structural + privacy validation is still not
authorised for reconciliation until an authorised human has inspected the
export column list and a small sample. Pattern-based privacy validation is
a backstop, not a substitute for review.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from .pseudonym import is_pseudonym


SCHEMA_VERSION = "w2d-input-1.0.0"


REQUIRED_FIELDS: tuple[str, ...] = (
    "observation_id",
    "signals",
    "manual_identification_flag",
    "stored_scientific_name",
    "stored_vernacular_name",
    "stored_rank",
    "source_release_or_timestamp",
)

ALLOWED_FIELDS: frozenset[str] = frozenset(REQUIRED_FIELDS)

SIGNAL_REQUIRED_FIELDS: tuple[str, ...] = (
    "kind",
    "source_system",
    "namespace",
    "external_id",
    "origin_field",
    "raw_value",
)
SIGNAL_ALLOWED_FIELDS: frozenset[str] = frozenset(SIGNAL_REQUIRED_FIELDS + ("notes", "rule_id"))
SIGNAL_KINDS: frozenset[str] = frozenset({"exact", "text-only"})

PROHIBITED_FIELD_NAMES: frozenset[str] = frozenset(
    {
        "email",
        "email_address",
        "user_email",
        "display_name",
        "user_display_name",
        "user_id",
        "auth_user_id",
        "user_uuid",
        "profile_id",
        "device_id",
        "device_uuid",
        "session_id",
        "access_token",
        "refresh_token",
        "password",
        "photo_url",
        "photo_urls",
        "image_url",
        "image_urls",
        "media_url",
        "media_urls",
        "storage_path",
        "photo_paths",
        "latitude",
        "longitude",
        "lat",
        "lon",
        "lng",
        "geom",
        "geohash",
        "locality",
        "locality_text",
        "place_name",
        "location_name",
        "notes",
        "observation_notes",
        "private_habitat",
        "habitat_notes",
    }
)


EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
MEDIA_URL_PATTERN = re.compile(
    r"(?:https?://|storage://|s3://|gs://|file://|/storage/v1/object/)",
    re.IGNORECASE,
)
COORDINATE_KEY_PATTERN = re.compile(
    r"(?:^|[_\W])(?:lat|lon|lng|latitude|longitude|coord(?:s|inates)?|geohash|geom)(?:$|[_\W])",
    re.IGNORECASE,
)
RAW_UUID_PATTERN = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


@dataclass(frozen=True, slots=True)
class ValidationError:
    """A single defect discovered by the validator."""

    kind: str
    location: str
    detail: str


@dataclass
class ValidationReport:
    schema_version_ok: bool = False
    record_count: int = 0
    unique_observation_ids: int = 0
    errors: list[ValidationError] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.schema_version_ok and not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "schema_version_ok": self.schema_version_ok,
            "record_count": self.record_count,
            "unique_observation_ids": self.unique_observation_ids,
            "errors": [
                {"kind": e.kind, "location": e.location, "detail": e.detail}
                for e in sorted(
                    self.errors, key=lambda e: (e.kind, e.location, e.detail)
                )
            ],
        }


def _scan_prohibited_values(location: str, value: Any) -> Iterable[ValidationError]:
    if isinstance(value, str):
        if EMAIL_PATTERN.search(value):
            yield ValidationError(
                "prohibited_email_like_value",
                location,
                "value matches an email address pattern",
            )
        if MEDIA_URL_PATTERN.search(value):
            yield ValidationError(
                "prohibited_media_url",
                location,
                "value matches a media or storage URL pattern",
            )
        if RAW_UUID_PATTERN.match(value) and location.endswith("observation_id"):
            yield ValidationError(
                "raw_uuid_observation_id",
                location,
                "observation_id looks like a raw UUID; expected a keyed pseudonym",
            )
    elif isinstance(value, dict):
        for k, v in value.items():
            if k.lower() in PROHIBITED_FIELD_NAMES:
                yield ValidationError(
                    "prohibited_field_name",
                    f"{location}.{k}",
                    "field name appears in the prohibited-field list",
                )
            if COORDINATE_KEY_PATTERN.search(f"_{k}_"):
                yield ValidationError(
                    "coordinate_key",
                    f"{location}.{k}",
                    "field name suggests coordinates",
                )
            yield from _scan_prohibited_values(f"{location}.{k}", v)
    elif isinstance(value, list):
        for i, item in enumerate(value):
            yield from _scan_prohibited_values(f"{location}[{i}]", item)


def _validate_signal(location: str, signal: Any) -> list[ValidationError]:
    errors: list[ValidationError] = []
    if not isinstance(signal, dict):
        errors.append(ValidationError("signal_not_object", location, "signal must be an object"))
        return errors
    unexpected = set(signal.keys()) - SIGNAL_ALLOWED_FIELDS
    for k in sorted(unexpected):
        errors.append(
            ValidationError("unexpected_signal_field", f"{location}.{k}", "field not in signal contract")
        )
    for k in SIGNAL_REQUIRED_FIELDS:
        if k not in signal:
            errors.append(
                ValidationError("missing_signal_field", f"{location}.{k}", "required signal field")
            )
    kind = signal.get("kind")
    if kind is not None and kind not in SIGNAL_KINDS:
        errors.append(
            ValidationError("invalid_signal_kind", f"{location}.kind", f"kind must be one of {sorted(SIGNAL_KINDS)}")
        )
    if signal.get("external_id") is not None and not isinstance(signal["external_id"], str):
        errors.append(
            ValidationError(
                "external_id_not_string",
                f"{location}.external_id",
                "external_id must be stored as a string (no numeric coercion)",
            )
        )
    return errors


def validate_record(record: Any, *, index: int) -> list[ValidationError]:
    """Return every defect in ``record``. Empty list means the record is valid."""

    errors: list[ValidationError] = []
    loc = f"records[{index}]"

    if not isinstance(record, dict):
        errors.append(ValidationError("record_not_object", loc, "record must be a JSON object"))
        return errors

    unexpected = set(record.keys()) - ALLOWED_FIELDS
    for k in sorted(unexpected):
        errors.append(
            ValidationError("unexpected_field", f"{loc}.{k}", "field not in snapshot contract")
        )
        if k.lower() in PROHIBITED_FIELD_NAMES:
            errors.append(
                ValidationError(
                    "prohibited_field_name",
                    f"{loc}.{k}",
                    "field name appears in the prohibited-field list",
                )
            )
    for k in REQUIRED_FIELDS:
        if k not in record:
            errors.append(
                ValidationError("missing_required_field", f"{loc}.{k}", "required snapshot field")
            )

    obs_id = record.get("observation_id")
    if isinstance(obs_id, str):
        if not is_pseudonym(obs_id):
            if RAW_UUID_PATTERN.match(obs_id):
                errors.append(
                    ValidationError(
                        "raw_uuid_observation_id",
                        f"{loc}.observation_id",
                        "observation_id is a raw UUID; expected a keyed pseudonym (obs_<24 hex>)",
                    )
                )
            elif not obs_id.startswith("synthetic_"):
                errors.append(
                    ValidationError(
                        "unpseudonymised_observation_id",
                        f"{loc}.observation_id",
                        "observation_id does not match the pseudonym or 'synthetic_' prefix shape",
                    )
                )

    signals = record.get("signals")
    if signals is not None:
        if not isinstance(signals, list):
            errors.append(
                ValidationError("signals_not_list", f"{loc}.signals", "signals must be a JSON array")
            )
        else:
            for i, s in enumerate(signals):
                errors.extend(_validate_signal(f"{loc}.signals[{i}]", s))

    errors.extend(_scan_prohibited_values(loc, record))
    return errors


def validate_snapshot(snapshot_path: Path, *, expect_schema_version: str = SCHEMA_VERSION) -> ValidationReport:
    """Read ``snapshot_path`` as JSONL and produce a :class:`ValidationReport`.

    The first non-empty line MUST be a header object of the form
    ``{"__snapshot_header__": true, "schema_version": "...", "record_count": <n>}``.
    Every subsequent non-empty line MUST be a reconciliation-input record.
    """

    report = ValidationReport()
    seen_ids: dict[str, int] = {}
    header_seen = False

    with snapshot_path.open("r", encoding="utf-8") as f:
        for line_number, raw_line in enumerate(f, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError as exc:
                report.errors.append(
                    ValidationError("invalid_json", f"line:{line_number}", f"{exc.msg} at column {exc.colno}")
                )
                continue
            if not header_seen:
                if isinstance(doc, dict) and doc.get("__snapshot_header__") is True:
                    header_seen = True
                    schema_ok = doc.get("schema_version") == expect_schema_version
                    report.schema_version_ok = bool(schema_ok)
                    if not schema_ok:
                        report.errors.append(
                            ValidationError(
                                "schema_version_mismatch",
                                f"line:{line_number}",
                                f"expected {expect_schema_version}, got {doc.get('schema_version')!r}",
                            )
                        )
                    continue
                else:
                    report.errors.append(
                        ValidationError(
                            "missing_header",
                            f"line:{line_number}",
                            "first non-empty line must be the __snapshot_header__ object",
                        )
                    )
                    header_seen = True  # Only report once.
            errors = validate_record(doc, index=report.record_count)
            report.errors.extend(errors)
            report.record_count += 1
            if isinstance(doc, dict):
                obs_id = doc.get("observation_id")
                if isinstance(obs_id, str):
                    if obs_id in seen_ids:
                        report.errors.append(
                            ValidationError(
                                "duplicate_observation_reference",
                                f"records[{report.record_count - 1}].observation_id",
                                f"observation_id {obs_id!r} first seen at record index {seen_ids[obs_id]}",
                            )
                        )
                    else:
                        seen_ids[obs_id] = report.record_count - 1

    report.unique_observation_ids = len(seen_ids)
    return report
