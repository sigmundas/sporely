"""Identity-safe import of portable databases and managed archive assets."""

from __future__ import annotations

import hashlib
import base64
import json
import math
import os
import re
import shutil
import sqlite3
import tempfile
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable
from zipfile import BadZipFile, ZipFile

from utils.archive.checksums import sha256_file
from utils.archive.manifest import ArchiveManifest
from utils.archive.paths import safe_staging_destination, validate_zip_entries
from utils.archive.validation import ArchiveValidationError, validate_portable_observations


class PortableImportError(RuntimeError):
    """Raised when portable identities cannot be imported safely."""


class PortableIdentityConflictError(PortableImportError):
    """Raised when a stable identity resolves to incompatible content."""


@dataclass(frozen=True)
class _ManagedAsset:
    archive_path: str
    staged_path: Path
    destination_path: Path
    sha256: str


def _canonical_uuid(value: Any, *, label: str) -> str:
    try:
        normalized = str(uuid.UUID(str(value or "").strip()))
    except (ValueError, TypeError, AttributeError) as exc:
        raise PortableImportError(f"{label} has an invalid stable UUID") from exc
    if str(value or "").strip().lower() != normalized:
        raise PortableImportError(f"{label} has a non-canonical stable UUID")
    return normalized


@dataclass(frozen=True)
class PortableImportResult:
    observation_id_map: dict[int, int]
    image_id_map: dict[int, int]
    measurement_id_map: dict[int, int]
    annotation_id_map: dict[int, int]
    session_log_id_map: dict[int, int]
    calibration_id_map: dict[int, int]
    calibration_asset_id_map: dict[int, int]
    reference_value_id_map: dict[int, int]
    reference_work_id_map: dict[str, str]
    reference_treatment_id_map: dict[str, str]
    reference_measurement_set_id_map: dict[str, str]
    reference_use_id_map: dict[str, str]
    session_id_map: dict[str, str]
    new_item_counts: dict[str, int]
    reused_item_counts: dict[str, int]


@dataclass(frozen=True)
class PortableObservationPreview:
    observation_id: int
    name: str
    date: str
    image_count: int


@dataclass(frozen=True)
class PortableClosureCounts:
    observations: int
    images: int
    measurements: int
    calibrations: int
    references: int


@dataclass(frozen=True)
class PortableArchivePreview:
    archive_path: Path
    archive_sha256: str
    archive_id: str
    created_at: str
    app_version: str
    source_platform: str
    observations: tuple[PortableObservationPreview, ...]
    full_counts: PortableClosureCounts

    def closure_counts(self, observation_ids: set[int]) -> PortableClosureCounts:
        if observation_ids == {item.observation_id for item in self.observations}:
            return self.full_counts
        return _portable_closure_counts(
            self.archive_path, observation_ids, expected_sha256=self.archive_sha256
        )


class _IntegerAllocator:
    def __init__(self, connection: sqlite3.Connection, table: str, source_ids: set[int]) -> None:
        destination_max = int(connection.execute(
            f"SELECT COALESCE(MAX(id), 0) FROM {table}"
        ).fetchone()[0])
        if "." in table:
            schema_name, table_name = table.split(".", 1)
            sequence_table = f"{schema_name}.sqlite_sequence"
        else:
            table_name = table
            sequence_table = "sqlite_sequence"
        try:
            sequence_row = connection.execute(
                f"SELECT seq FROM {sequence_table} WHERE name=?", (table_name,)
            ).fetchone()
        except sqlite3.OperationalError:
            sequence_row = None
        sequence_max = int(sequence_row[0]) if sequence_row is not None else 0
        self._next = max(destination_max, sequence_max, max(source_ids, default=0)) + 1

    def allocate(self) -> int:
        value = self._next
        self._next += 1
        return value


def _rows(connection: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY id")]


_MAIN_PROVENANCE_TABLES = {
    "observation": "observations",
    "image": "images",
    "measurement": "spore_measurements",
    "annotation": "spore_annotations",
    "session_log": "session_logs",
    "calibration": "calibrations",
    "calibration_asset": "calibration_assets",
    "reference_use": "observation_reference_uses",
}
_REFERENCE_PROVENANCE_TABLES = {
    "reference_value": "reference_values",
    "reference_work": "reference_works",
    "reference_treatment": "reference_taxon_treatments",
    "reference_measurement_set": "reference_measurement_sets",
}


def _ensure_provenance_schema(connection: sqlite3.Connection) -> None:
    from database.schema import ensure_portable_import_schema

    ensure_portable_import_schema(connection)


def _fingerprint_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"bytes_sha256": hashlib.sha256(value).hexdigest()}
    if isinstance(value, dict):
        return {key: _fingerprint_value(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_fingerprint_value(item) for item in value]
    return value


def _source_fingerprint(row: dict[str, Any]) -> str:
    payload = json.dumps(
        _fingerprint_value(row), ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _source_provenance_items(
    source_main: sqlite3.Connection,
    source_reference: sqlite3.Connection,
) -> dict[tuple[str, str], str]:
    items: dict[tuple[str, str], str] = {}
    for item_type, table in _MAIN_PROVENANCE_TABLES.items():
        for row in _rows(source_main, table):
            items[(item_type, str(row["id"]))] = _source_fingerprint(row)
    for item_type, table in _REFERENCE_PROVENANCE_TABLES.items():
        for row in _rows(source_reference, table):
            items[(item_type, str(row["id"]))] = _source_fingerprint(row)

    session_rows: dict[str, list[dict[str, Any]]] = {}
    for row in _rows(source_main, "session_logs"):
        identity = str(row.get("session_id") or "")
        if identity.strip():
            session_rows.setdefault(identity, []).append(row)
    for row in _rows(source_main, "images"):
        for identity in _embedded_values(row.get("lab_metadata"), "session_id"):
            session_rows.setdefault(identity, [])
    for table, field in (("observations", "ai_state_json"), ("session_logs", "metadata_json")):
        for row in _rows(source_main, table):
            for identity in _embedded_values(row.get(field), "session_id"):
                session_rows.setdefault(identity, [])
    for identity, rows in session_rows.items():
        items[("session", identity)] = _source_fingerprint(
            {"session_id": identity, "session_logs": rows}
        )
    return items


def _destination_exists(
    connection: sqlite3.Connection,
    item_type: str,
    destination_item_id: str,
) -> bool:
    if item_type == "session":
        return bool(destination_item_id.strip())
    if item_type in _REFERENCE_PROVENANCE_TABLES:
        table = f"portable_reference.{_REFERENCE_PROVENANCE_TABLES[item_type]}"
    else:
        table = _MAIN_PROVENANCE_TABLES.get(item_type)
    if table is None:
        raise PortableImportError(f"unknown provenance item type {item_type}")
    return connection.execute(
        f"SELECT 1 FROM {table} WHERE id=?", (destination_item_id,)
    ).fetchone() is not None


def _preflight_provenance(
    connection: sqlite3.Connection,
    *,
    archive_id: str,
    source_items: dict[tuple[str, str], str],
    archive_inventory_fingerprint: str | None = None,
) -> dict[tuple[str, str], str]:
    inventory_fingerprint = (
        archive_inventory_fingerprint or _archive_inventory_fingerprint(source_items)
    )
    archive_row = connection.execute(
        "SELECT destination_item_id, source_content_sha256 "
        "FROM portable_import_provenance WHERE archive_id=? "
        "AND source_item_type='archive' AND source_item_id='inventory'",
        (archive_id,),
    ).fetchone()
    if archive_row is not None and str(archive_row[1]) != inventory_fingerprint:
        raise PortableImportError(
            f"archive {archive_id} has conflicting source content inventory"
        )
    existing = {
        (str(row[0]), str(row[1])): (str(row[2]), str(row[3]))
        for row in connection.execute(
            "SELECT source_item_type, source_item_id, destination_item_id, "
            "source_content_sha256 FROM portable_import_provenance WHERE archive_id=?",
            (archive_id,),
        )
        if str(row[0]) != "archive"
    }
    if existing and archive_row is None:
        raise PortableImportError(f"archive {archive_id} has provenance without inventory identity")
    stale = sorted(set(existing) - set(source_items))
    if stale and archive_inventory_fingerprint is None:
        item_type, source_id = stale[0]
        raise PortableImportError(
            f"archive {archive_id} is missing previously imported {item_type} {source_id}"
        )
    mappings: dict[tuple[str, str], str] = {}
    for key, fingerprint in source_items.items():
        recorded = existing.get(key)
        if recorded is None:
            continue
        destination_id, recorded_fingerprint = recorded
        if recorded_fingerprint != fingerprint:
            raise PortableImportError(
                f"{key[0]} {key[1]} has conflicting source content for archive {archive_id}"
            )
        if not _destination_exists(connection, key[0], destination_id):
            raise PortableImportError(
                f"{key[0]} {key[1]} has a missing destination mapping {destination_id}"
            )
        mappings[key] = destination_id
    return mappings


def _archive_inventory_fingerprint(
    source_items: dict[tuple[str, str], str],
    manifest: ArchiveManifest | None = None,
) -> str:
    inventory: dict[str, Any] = {
        f"{item_type}\0{source_id}": fingerprint
        for (item_type, source_id), fingerprint in source_items.items()
    }
    if manifest is not None:
        inventory["archive_files"] = {
            entry.path: {
                "status": entry.status,
                "size": entry.size,
                "sha256": entry.sha256,
            }
            for entry in manifest.files
        }
    return _source_fingerprint(inventory)


def _record_provenance(
    connection: sqlite3.Connection,
    *,
    archive_id: str,
    item_type: str,
    source_id: str,
    destination_id: str,
    source_fingerprint: str,
) -> None:
    connection.execute(
        "INSERT INTO portable_import_provenance "
        "(archive_id, source_item_type, source_item_id, destination_item_id, "
        "source_content_sha256) VALUES (?, ?, ?, ?, ?)",
        (archive_id, item_type, source_id, destination_id, source_fingerprint),
    )


def _validate_replay_relationships(
    source_main: sqlite3.Connection,
    source_reference: sqlite3.Connection,
    destination: sqlite3.Connection,
    replay: dict[tuple[str, str], str],
) -> None:
    def mapped(item_type: str, source_id: Any) -> str | None:
        return replay.get((item_type, str(source_id)))

    def validate_embedded(
        *, item_type: str, source_row: dict[str, Any], destination_table: str, field: str
    ) -> None:
        destination_id = mapped(item_type, source_row["id"])
        if destination_id is None:
            return
        destination_row = destination.execute(
            f"SELECT {field} FROM {destination_table} WHERE id=?", (destination_id,)
        ).fetchone()
        if destination_row is None:
            return
        source_images, source_sessions = _embedded_identity_values(source_row.get(field))
        expected_images = {mapped("image", value) for value in source_images}
        expected_sessions = {mapped("session", value) for value in source_sessions}
        if None in expected_images or None in expected_sessions:
            raise PortableImportError(
                f"{item_type} {source_row['id']} has incomplete embedded provenance"
            )
        actual_images, actual_sessions = _embedded_identity_values(destination_row[0])
        if not expected_images.issubset(actual_images) or not expected_sessions.issubset(actual_sessions):
            raise PortableImportError(
                f"{item_type} {source_row['id']} has conflicting embedded destination relationships"
            )

    relationship_specs = (
        (source_main, "image", "images", "observation_id", "observation", "images"),
        (source_main, "image", "images", "calibration_id", "calibration", "images"),
        (source_main, "measurement", "spore_measurements", "image_id", "image", "spore_measurements"),
        (source_main, "annotation", "spore_annotations", "image_id", "image", "spore_annotations"),
        (source_main, "annotation", "spore_annotations", "measurement_id", "measurement", "spore_annotations"),
        (source_main, "session_log", "session_logs", "observation_id", "observation", "session_logs"),
        (source_main, "session_log", "session_logs", "session_id", "session", "session_logs"),
        (source_main, "reference_use", "observation_reference_uses", "observation_id", "observation", "observation_reference_uses"),
        (source_main, "reference_use", "observation_reference_uses", "reference_measurement_set_id", "reference_measurement_set", "observation_reference_uses"),
        (source_reference, "reference_treatment", "reference_taxon_treatments", "reference_work_id", "reference_work", "portable_reference.reference_taxon_treatments"),
        (source_reference, "reference_measurement_set", "reference_measurement_sets", "taxon_treatment_id", "reference_treatment", "portable_reference.reference_measurement_sets"),
        (source_reference, "reference_measurement_set", "reference_measurement_sets", "legacy_reference_value_id", "reference_value", "portable_reference.reference_measurement_sets"),
    )
    for source_connection, child_type, source_table, source_fk, parent_type, destination_table in relationship_specs:
        for row in _rows(source_connection, source_table):
            child_destination = mapped(child_type, row["id"])
            source_parent = row.get(source_fk)
            if child_destination is None or source_parent is None:
                continue
            parent_destination = mapped(parent_type, source_parent)
            if parent_destination is None:
                raise PortableImportError(
                    f"{child_type} {row['id']} has provenance without mapped {parent_type} {source_parent}"
                )
            actual = destination.execute(
                f"SELECT {source_fk} FROM {destination_table} WHERE id=?",
                (child_destination,),
            ).fetchone()
            if actual is None or str(actual[0]) != str(parent_destination):
                raise PortableImportError(
                    f"{child_type} {row['id']} has conflicting destination relationship {source_fk}"
                )

    for row in _rows(source_main, "calibration_assets"):
        child_destination = mapped("calibration_asset", row["id"])
        source_parent = row.get("calibration_id")
        if child_destination is None:
            continue
        parent_destination = mapped("calibration", source_parent) if source_parent is not None else None
        if parent_destination is None and str(row.get("calibration_uuid") or "").strip():
            source_calibration = source_main.execute(
                "SELECT id FROM calibrations WHERE lower(calibration_uuid)=lower(?)",
                (row["calibration_uuid"],),
            ).fetchone()
            if source_calibration is not None:
                parent_destination = mapped("calibration", source_calibration[0])
        actual = destination.execute(
            "SELECT calibration_id, calibration_uuid FROM calibration_assets WHERE id=?",
            (child_destination,),
        ).fetchone()
        destination_parent = destination.execute(
            "SELECT calibration_uuid FROM calibrations WHERE id=?", (parent_destination,)
        ).fetchone() if parent_destination is not None else None
        numeric_present = actual is not None and actual[0] is not None
        uuid_present = actual is not None and bool(str(actual[1] or "").strip())
        numeric_conflict = numeric_present and str(actual[0]) != parent_destination
        uuid_conflict = (
            uuid_present and (
                destination_parent is None
                or str(actual[1]).lower() != str(destination_parent[0] or "").lower()
            )
        )
        if (
            parent_destination is None or actual is None
            or not (numeric_present or uuid_present)
            or numeric_conflict or uuid_conflict
        ):
            raise PortableImportError(
                f"calibration_asset {row['id']} has conflicting destination relationship calibration_id"
            )

    for row in _rows(source_main, "observations"):
        validate_embedded(
            item_type="observation", source_row=row,
            destination_table="observations", field="ai_state_json",
        )
    for row in _rows(source_main, "images"):
        validate_embedded(
            item_type="image", source_row=row,
            destination_table="images", field="lab_metadata",
        )
    for row in _rows(source_main, "session_logs"):
        validate_embedded(
            item_type="session_log", source_row=row,
            destination_table="session_logs", field="metadata_json",
        )

    for row in _rows(source_main, "observation_reference_uses"):
        destination_id = mapped("reference_use", row["id"])
        if destination_id is None:
            continue
        destination_snapshot = destination.execute(
            "SELECT snapshot_json FROM observation_reference_uses WHERE id=?",
            (destination_id,),
        ).fetchone()
        source_snapshot = _canonical_json(row.get("snapshot_json"))
        target_snapshot = _canonical_json(destination_snapshot[0]) if destination_snapshot else None
        for key in (
            "reference_work_id", "reference_treatment_id",
            "reference_measurement_set_id", "reference_revision",
        ):
            if not isinstance(source_snapshot, dict) or not isinstance(target_snapshot, dict):
                raise PortableImportError(
                    f"reference use {row['id']} has invalid destination snapshot"
                )
            if source_snapshot.get(key) != target_snapshot.get(key):
                raise PortableImportError(
                    f"reference use {row['id']} has conflicting destination snapshot {key}"
                )


def _validate_replay_root_completeness(
    source_main: sqlite3.Connection,
    replay: dict[tuple[str, str], str],
) -> None:
    observations = _rows(source_main, "observations")
    images = _rows(source_main, "images")
    measurements = _rows(source_main, "spore_measurements")
    annotations = _rows(source_main, "spore_annotations")
    logs = _rows(source_main, "session_logs")
    uses = _rows(source_main, "observation_reference_uses")
    images_by_observation: dict[int, set[int]] = {}
    for row in images:
        images_by_observation.setdefault(int(row["observation_id"]), set()).add(int(row["id"]))
    for observation in observations:
        observation_id = int(observation["id"])
        image_ids = images_by_observation.get(observation_id, set())
        closure = {("observation", str(observation_id))}
        closure.update(("image", str(row["id"])) for row in images if int(row["id"]) in image_ids)
        closure.update(
            ("measurement", str(row["id"]))
            for row in measurements if int(row["image_id"]) in image_ids
        )
        closure.update(
            ("annotation", str(row["id"]))
            for row in annotations if int(row["image_id"]) in image_ids
        )
        closure.update(
            ("session_log", str(row["id"]))
            for row in logs if int(row["observation_id"]) == observation_id
        )
        closure.update(
            ("reference_use", str(row["id"]))
            for row in uses if int(row["observation_id"]) == observation_id
        )
        session_keys: set[tuple[str, str]] = set()
        for row in images:
            if int(row["id"]) in image_ids:
                session_keys.update(
                    ("session", value)
                    for value in _embedded_values(row.get("lab_metadata"), "session_id")
                )
        for row in logs:
            if int(row["observation_id"]) == observation_id:
                session_keys.add(("session", str(row["session_id"])))
                session_keys.update(
                    ("session", value)
                    for value in _embedded_values(row.get("metadata_json"), "session_id")
                )
        session_keys.update(
            ("session", value)
            for value in _embedded_values(observation.get("ai_state_json"), "session_id")
        )
        mapped_count = sum(key in replay for key in closure)
        if mapped_count not in (0, len(closure)):
            raise PortableImportError(
                f"observation {observation_id} has incomplete portable import provenance"
            )
        if ("observation", str(observation_id)) in replay:
            missing_sessions = session_keys - set(replay)
            if missing_sessions:
                raise PortableImportError(
                    f"observation {observation_id} has incomplete session provenance"
                )


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    if "." in table:
        schema_name, table_name = table.split(".", 1)
        pragma = f"PRAGMA {schema_name}.table_info({table_name})"
    else:
        pragma = f"PRAGMA table_info({table})"
    return {str(row[1]) for row in connection.execute(pragma)}


def _insert_row(connection: sqlite3.Connection, table: str, row: dict[str, Any]) -> None:
    allowed = _columns(connection, table)
    data = {key: value for key, value in row.items() if key in allowed}
    names = list(data)
    placeholders = ", ".join("?" for _name in names)
    connection.execute(
        f"INSERT INTO {table} ({', '.join(names)}) VALUES ({placeholders})",
        [data[name] for name in names],
    )


def _canonical_json(value: Any) -> Any:
    if value is None or value == "":
        return None
    try:
        loaded = json.loads(value) if isinstance(value, str) else value
    except (json.JSONDecodeError, TypeError):
        return value
    return loaded


_LOCATION_METADATA_KEYS = {
    "source_path", "source_filepath", "working_path", "original_path",
    "local_path", "selected_path", "path", "companion_paths",
    "image_storage_path", "cloud_storage_path", "storage_path",
    "original_storage_path", "metadata_sha256",
}


def _portable_metadata(value: Any, *, preserve_local_paths: bool = False) -> Any:
    loaded = _canonical_json(value)
    if isinstance(loaded, dict):
        stripped = {
            "image_storage_path", "cloud_storage_path", "storage_path",
            "original_storage_path", "metadata_sha256",
        }
        if not preserve_local_paths:
            stripped |= _LOCATION_METADATA_KEYS
        return {
            key: _portable_metadata(item, preserve_local_paths=preserve_local_paths)
            for key, item in sorted(loaded.items())
            if key not in stripped
        }
    if isinstance(loaded, list):
        return [
            _portable_metadata(item, preserve_local_paths=preserve_local_paths)
            for item in loaded
        ]
    return loaded


def _normalized_value(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 12)
    if isinstance(value, str):
        return value.strip()
    return value


def _equivalent_rows(
    source: dict[str, Any],
    destination: dict[str, Any],
    *,
    ignored: set[str],
    json_fields: set[str] = frozenset(),
    metadata_fields: set[str] = frozenset(),
) -> bool:
    keys = (set(source) & set(destination)) - ignored
    for key in keys:
        if key in metadata_fields:
            if _portable_metadata(source.get(key)) != _portable_metadata(destination.get(key)):
                return False
        elif key in json_fields:
            if _canonical_json(source.get(key)) != _canonical_json(destination.get(key)):
                return False
        else:
            left = _normalized_value(source.get(key))
            right = _normalized_value(destination.get(key))
            if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                if not math.isclose(float(left), float(right), rel_tol=1e-9, abs_tol=1e-12):
                    return False
            elif left != right:
                return False
    return True


_CALIBRATION_FLOAT_FIELDS = {
    "microns_per_pixel", "microns_per_pixel_std", "confidence_interval_low",
    "confidence_interval_high", "megapixels", "target_sampling_pct",
    "resample_scale_factor",
}
_CALIBRATION_INT_FIELDS = {
    "num_measurements", "calibration_image_width", "calibration_image_height",
}
_CALIBRATION_TEXT_FIELDS = {"objective_key", "camera", "notes"}


def _calibration_value(field: str, value: Any) -> Any:
    if field in _CALIBRATION_FLOAT_FIELDS:
        if value is None or str(value).strip() == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    if field in _CALIBRATION_INT_FIELDS:
        if value is None or str(value).strip() == "":
            return None
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None
    if field in {"calibration_date", "calibration_image_date"}:
        text = str(value or "").strip()
        return text[:10] if text else None
    if field == "measurements_json":
        normalized = _portable_metadata(value)
        return None if normalized in (None, "", [], {}) else normalized
    if field == "calibration_uuid":
        try:
            return str(uuid.UUID(str(value or "").strip()))
        except (TypeError, ValueError, AttributeError):
            return None
    if field in _CALIBRATION_TEXT_FIELDS:
        text = str(value or "").strip()
        return text or None
    return value


def _calibrations_equivalent(
    source: dict[str, Any], destination: dict[str, Any]
) -> bool:
    fields = {
        "calibration_uuid", "objective_key", "calibration_date",
        "calibration_image_date", "microns_per_pixel", "microns_per_pixel_std",
        "confidence_interval_low", "confidence_interval_high", "num_measurements",
        "measurements_json", "camera", "megapixels", "target_sampling_pct",
        "resample_scale_factor", "calibration_image_width",
        "calibration_image_height", "notes",
    }
    for field in fields:
        left = _calibration_value(field, source.get(field))
        right = _calibration_value(field, destination.get(field))
        if field in _CALIBRATION_FLOAT_FIELDS and left is not None and right is not None:
            if not math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-9):
                return False
        elif left != right:
            return False
    return True


def _next_uuid(existing: set[str]) -> str:
    while True:
        candidate = str(uuid.uuid4())
        if candidate not in existing:
            existing.add(candidate)
            return candidate


def _rewrite_embedded_identity(
    value: Any,
    *,
    image_id_map: dict[int, int],
    session_id_map: dict[str, str],
    managed_assets_root: Path | None = None,
) -> Any:
    loaded = _canonical_json(value)
    if not isinstance(loaded, (dict, list)):
        return value

    def rewrite(item: Any) -> Any:
        if isinstance(item, list):
            return [rewrite(child) for child in item]
        if not isinstance(item, dict):
            return item
        result: dict[str, Any] = {}
        for key, child in item.items():
            if key == "image_id" and child is not None:
                try:
                    source_image_id = int(child)
                except (TypeError, ValueError):
                    raise PortableImportError("embedded image_id is not an integer")
                if source_image_id not in image_id_map:
                    raise PortableImportError(
                        f"embedded image_id {source_image_id} is outside the portable graph"
                    )
                result[key] = image_id_map[source_image_id]
            elif key == "image_ids" and isinstance(child, list):
                rewritten_ids: list[int] = []
                for value in child:
                    try:
                        source_image_id = int(value)
                    except (TypeError, ValueError):
                        raise PortableImportError("embedded image_ids contains a non-integer")
                    if source_image_id not in image_id_map:
                        raise PortableImportError(
                            f"embedded image_id {source_image_id} is outside the portable graph"
                        )
                    rewritten_ids.append(image_id_map[source_image_id])
                result[key] = rewritten_ids
            elif key == "session_id" and str(child or "") in session_id_map:
                result[key] = session_id_map[str(child)]
            elif (
                isinstance(child, str)
                and (
                    key.lower() in {"path", "filepath", "watch_dir"}
                    or key.lower().endswith(("_path", "_filepath", "_dir"))
                )
                and (
                    Path(child).is_absolute()
                    or re.match(r"^[A-Za-z]:[\\/]", child) is not None
                    or child.startswith(("\\\\", "//"))
                )
            ):
                candidate = Path(child).resolve()
                allowed = (
                    managed_assets_root is not None
                    and managed_assets_root.resolve() in candidate.parents
                )
                result[key] = child if allowed else None
            else:
                result[key] = rewrite(child)
        return result

    return json.dumps(rewrite(loaded), ensure_ascii=False, sort_keys=True)


def _embedded_values(value: Any, key_name: str) -> set[str]:
    loaded = _canonical_json(value)
    found: set[str] = set()

    def visit(item: Any) -> None:
        if isinstance(item, list):
            for child in item:
                visit(child)
        elif isinstance(item, dict):
            for key, child in item.items():
                if key == key_name and str(child or "").strip():
                    found.add(str(child))
                visit(child)

    visit(loaded)
    return found


def _embedded_identity_values(value: Any) -> tuple[set[str], set[str]]:
    loaded = _canonical_json(value)
    image_ids: set[str] = set()
    session_ids: set[str] = set()

    def visit(item: Any) -> None:
        if isinstance(item, list):
            for child in item:
                visit(child)
            return
        if not isinstance(item, dict):
            return
        for key, child in item.items():
            if key == "image_id" and child is not None:
                image_ids.add(str(child))
            elif key == "image_ids" and isinstance(child, list):
                image_ids.update(str(value) for value in child if value is not None)
            elif key == "session_id" and child is not None:
                session_ids.add(str(child))
            visit(child)

    visit(loaded)
    return image_ids, session_ids


def _validate_reference_snapshot(
    row: dict[str, Any],
    destination_reference: sqlite3.Connection,
    *,
    destination_schema: str,
) -> None:
    snapshot = _canonical_json(row.get("snapshot_json"))
    if not isinstance(snapshot, dict):
        raise PortableImportError(f"reference use {row.get('id')} has an invalid snapshot")
    allowed_keys = {
        "schema_version", "reference_work_id", "reference_measurement_set_id",
        "reference_treatment_id", "reference_revision", "short_label",
        "full_citation", "work_type", "year", "doi", "isbn", "taxon_id",
        "name_as_published", "locator_text", "page_from", "page_to",
        "character", "data_kind", "raw_text", "measurements", "method",
        "raw_points",
    }
    if snapshot.get("schema_version") != 1 or set(snapshot) != allowed_keys:
        raise PortableImportError(
            f"reference use {row.get('id')} has a noncanonical snapshot"
        )
    set_id = str(row.get("reference_measurement_set_id") or "").strip()
    graph = destination_reference.execute(
        """
        SELECT measurement_set.id AS set_id,
               treatment.id AS treatment_id,
               work.id AS work_id
        FROM {destination_schema}.reference_measurement_sets AS measurement_set
        JOIN {destination_schema}.reference_taxon_treatments AS treatment
          ON treatment.id = measurement_set.taxon_treatment_id
        JOIN {destination_schema}.reference_works AS work
          ON work.id = treatment.reference_work_id
        WHERE measurement_set.id = ?
        """.format(destination_schema=destination_schema),
        (set_id,),
    ).fetchone()
    if graph is None:
        raise PortableImportError(f"reference use {row.get('id')} has an unresolved snapshot graph")
    expected = {
        "reference_measurement_set_id": str(graph["set_id"]),
        "reference_treatment_id": str(graph["treatment_id"]),
        "reference_work_id": str(graph["work_id"]),
    }
    for key, value in expected.items():
        if str(snapshot.get(key) or "").strip() != value:
            raise PortableImportError(
                f"reference use {row.get('id')} snapshot has crossed {key}"
            )
    snapshot_revision = snapshot.get("reference_revision")
    row_revision = row.get("reference_revision")
    if snapshot_revision is None or int(snapshot_revision) <= 0:
        raise PortableImportError(
            f"reference use {row.get('id')} snapshot has no valid revision"
        )
    if int(snapshot_revision) != int(row_revision or 0):
        raise PortableImportError(
            f"reference use {row.get('id')} snapshot revision disagrees with its link"
        )


def _merge_reference_entity(
    source: dict[str, Any],
    destination: sqlite3.Connection,
    *,
    table: str,
    immutable_fields: set[str],
    json_fields: set[str] = frozenset(),
) -> str:
    identity = str(source.get("id") or "").strip()
    if not identity:
        raise PortableImportError(f"{table} row has no stable identity")
    existing_row = destination.execute(
        f"SELECT * FROM {table} WHERE id=?", (identity,)
    ).fetchone()
    data = dict(source)
    if "owner_id" in data:
        data["owner_id"] = None
    if existing_row is None:
        _insert_row(destination, table, data)
        return identity
    existing = dict(existing_row)
    enrich_legacy_reference_id = None
    if table.rsplit(".", 1)[-1] == "reference_measurement_sets":
        source_legacy_id = data.get("legacy_reference_value_id")
        destination_legacy_id = existing.get("legacy_reference_value_id")
        if source_legacy_id is not None and destination_legacy_id is not None:
            if int(source_legacy_id) != int(destination_legacy_id):
                raise PortableIdentityConflictError(
                    f"{table} {identity} has conflicting legacy reference provenance"
                )
        elif source_legacy_id is None:
            data["legacy_reference_value_id"] = destination_legacy_id
        else:
            existing["legacy_reference_value_id"] = source_legacy_id
            enrich_legacy_reference_id = int(source_legacy_id)
    for field in immutable_fields:
        if _normalized_value(data.get(field)) != _normalized_value(existing.get(field)):
            raise PortableIdentityConflictError(
                f"{table} {identity} has conflicting immutable field {field}"
            )
    source_revision = int(data.get("revision") or 1)
    destination_revision = int(existing.get("revision") or 1)
    ignored = {"created_at", "updated_at", "owner_id"}
    if table.rsplit(".", 1)[-1] == "reference_works":
        ignored.update({"verification_status", "visibility"})
    if source_revision == destination_revision:
        if not _equivalent_rows(data, existing, ignored=ignored, json_fields=json_fields):
            raise PortableIdentityConflictError(
                f"{table} {identity} has conflicting content at revision {source_revision}"
            )
    elif source_revision > destination_revision:
        allowed = _columns(destination, table)
        updates = {
            key: value for key, value in data.items()
            if key in allowed and key not in {"id", "created_at", "updated_at", "owner_id"}
        }
        assignments = ", ".join(f"{key}=?" for key in updates)
        destination.execute(
            f"UPDATE {table} SET {assignments} WHERE id=?",
            [*updates.values(), identity],
        )
    if enrich_legacy_reference_id is not None:
        destination.execute(
            f"UPDATE {table} SET legacy_reference_value_id=? WHERE id=?",
            (enrich_legacy_reference_id, identity),
        )
    return identity


def _merge_reference_graph(
    source: sqlite3.Connection,
    destination: sqlite3.Connection,
    *,
    destination_schema: str,
    existing_maps: dict[str, dict[Any, Any]] | None = None,
) -> tuple[dict[int, int], dict[str, str], dict[str, str], dict[str, str]]:
    def destination_table(name: str) -> str:
        return f"{destination_schema}.{name}"

    source_values = _rows(source, "reference_values")
    value_ids = {int(row["id"]) for row in source_values}
    allocator = _IntegerAllocator(
        destination, destination_table("reference_values"), value_ids
    )
    existing_maps = existing_maps or {}
    value_map: dict[int, int] = dict(existing_maps.get("reference_value", {}))
    for row in source_values:
        source_id = int(row["id"])
        if source_id in value_map:
            continue
        comparable = {key: value for key, value in row.items() if key not in {"id", "updated_at"}}
        matches = []
        for candidate in _rows(destination, destination_table("reference_values")):
            if _equivalent_rows(
                comparable,
                candidate,
                ignored={"id", "updated_at"},
                json_fields={"metadata_json"},
            ):
                matches.append(candidate)
        if len(matches) > 1:
            raise PortableIdentityConflictError(
                f"reference_values {source_id} has ambiguous destination matches"
            )
        if matches:
            value_map[source_id] = int(matches[0]["id"])
        else:
            business_key = tuple(
                _normalized_value(row.get(field))
                for field in ("genus", "species", "source", "mount_medium", "stain")
            )
            conflicting = [
                candidate
                for candidate in _rows(
                    destination, destination_table("reference_values")
                )
                if tuple(
                    _normalized_value(candidate.get(field))
                    for field in ("genus", "species", "source", "mount_medium", "stain")
                ) == business_key
            ]
            if conflicting:
                raise PortableIdentityConflictError(
                    f"reference_values {source_id} conflicts with an existing business key"
                )
            new_id = allocator.allocate()
            row["id"] = new_id
            _insert_row(destination, destination_table("reference_values"), row)
            value_map[source_id] = new_id

    work_map: dict[str, str] = dict(existing_maps.get("reference_work", {}))
    for row in _rows(source, "reference_works"):
        if str(row["id"]) in work_map:
            continue
        identity = _merge_reference_entity(
            row,
            destination,
            table=destination_table("reference_works"),
            immutable_fields=set(),
            json_fields={"authors_json", "editors_json"},
        )
        work_map[str(row["id"])] = identity

    treatment_map: dict[str, str] = dict(existing_maps.get("reference_treatment", {}))
    for row in _rows(source, "reference_taxon_treatments"):
        if str(row["id"]) in treatment_map:
            continue
        source_parent = str(row["reference_work_id"])
        if source_parent not in work_map:
            raise PortableImportError(f"reference treatment {row['id']} has no mapped work")
        row["reference_work_id"] = work_map[source_parent]
        identity = _merge_reference_entity(
            row,
            destination,
            table=destination_table("reference_taxon_treatments"),
            immutable_fields={"reference_work_id"},
        )
        treatment_map[str(row["id"])] = identity

    set_map: dict[str, str] = dict(existing_maps.get("reference_measurement_set", {}))
    source_sets = _rows(source, "reference_measurement_sets")
    for row in source_sets:
        if str(row["id"]) in set_map:
            continue
        source_parent = str(row["taxon_treatment_id"])
        if source_parent not in treatment_map:
            raise PortableImportError(f"reference measurement set {row['id']} has no mapped treatment")
        row["taxon_treatment_id"] = treatment_map[source_parent]
        legacy_id = row.get("legacy_reference_value_id")
        if legacy_id is not None:
            if int(legacy_id) not in value_map:
                raise PortableImportError(
                    f"reference measurement set {row['id']} has no mapped legacy value"
                )
            row["legacy_reference_value_id"] = value_map[int(legacy_id)]
        identity = _merge_reference_entity(
            row,
            destination,
            table=destination_table("reference_measurement_sets"),
            immutable_fields={
                "taxon_treatment_id", "supersedes_id",
            },
            json_fields={"raw_points_json"},
        )
        set_map[str(row["id"])] = identity
    return value_map, work_map, treatment_map, set_map


def _merge_calibrations(
    source: sqlite3.Connection,
    destination: sqlite3.Connection,
    existing_map: dict[int, int] | None = None,
) -> dict[int, int]:
    source_rows = _rows(source, "calibrations")
    allocator = _IntegerAllocator(
        destination, "calibrations", {int(row["id"]) for row in source_rows}
    )
    result: dict[int, int] = dict(existing_map or {})
    for row in source_rows:
        source_id = int(row["id"])
        if source_id in result:
            continue
        identity = _canonical_uuid(
            row.get("calibration_uuid"), label=f"calibration {source_id}"
        )
        row["calibration_uuid"] = identity
        existing_rows = destination.execute(
            "SELECT * FROM calibrations WHERE lower(calibration_uuid)=?", (identity,)
        ).fetchall()
        if len(existing_rows) > 1:
            raise PortableIdentityConflictError(
                f"calibration {identity} has ambiguous destination identities"
            )
        existing_row = existing_rows[0] if existing_rows else None
        if existing_row is not None:
            if not _calibrations_equivalent(row, dict(existing_row)):
                raise PortableIdentityConflictError(
                    f"calibration {identity} has conflicting immutable content"
                )
            result[source_id] = int(existing_row["id"])
            continue
        new_id = allocator.allocate()
        row["id"] = new_id
        row["is_active"] = 0
        _insert_row(destination, "calibrations", row)
        result[source_id] = new_id
    return result


def _merge_calibration_assets(
    source: sqlite3.Connection,
    destination: sqlite3.Connection,
    calibration_id_map: dict[int, int],
    existing_map: dict[int, int] | None = None,
    *,
    preserve_managed_asset_paths: bool = False,
) -> dict[int, int]:
    source_rows = _rows(source, "calibration_assets")
    allocator = _IntegerAllocator(
        destination, "calibration_assets", {int(row["id"]) for row in source_rows}
    )
    result: dict[int, int] = dict(existing_map or {})
    for row in source_rows:
        source_id = int(row["id"])
        if source_id in result:
            continue
        identity = _canonical_uuid(
            row.get("asset_uuid"), label=f"calibration asset {source_id}"
        )
        row["asset_uuid"] = identity
        source_calibration_id = row.get("calibration_id")
        linked_source_calibration = None
        if source_calibration_id is not None:
            linked_source_calibration = source.execute(
                "SELECT calibration_uuid FROM calibrations WHERE id=?",
                (int(source_calibration_id),),
            ).fetchone()
            if linked_source_calibration is None:
                raise PortableImportError(
                    f"calibration asset {source_id} has no source calibration"
                )
        explicit_uuid = str(row.get("calibration_uuid") or "").strip()
        linked_uuid = str(
            linked_source_calibration["calibration_uuid"]
            if linked_source_calibration is not None else ""
        ).strip()
        if explicit_uuid and linked_uuid:
            if _canonical_uuid(
                explicit_uuid, label=f"calibration asset {source_id} calibration"
            ) != _canonical_uuid(
                linked_uuid, label=f"calibration asset {source_id} parent"
            ):
                raise PortableIdentityConflictError(
                    f"calibration asset {source_id} has conflicting calibration identities"
                )
        calibration_uuid = _canonical_uuid(
            explicit_uuid or linked_uuid,
            label=f"calibration asset {source_id} calibration",
        )
        row["calibration_uuid"] = calibration_uuid
        mapped_by_uuid_row = destination.execute(
            "SELECT id FROM calibrations WHERE lower(calibration_uuid)=?", (calibration_uuid,)
        ).fetchone()
        if mapped_by_uuid_row is None:
            raise PortableImportError(
                f"calibration asset {source_id} has no mapped calibration UUID"
            )
        mapped_by_uuid = int(mapped_by_uuid_row["id"])
        if source_calibration_id is not None:
            mapped_calibration_id = calibration_id_map.get(int(source_calibration_id))
            if mapped_calibration_id is None:
                raise PortableImportError(
                    f"calibration asset {source_id} has no mapped calibration"
                )
            if mapped_calibration_id != mapped_by_uuid:
                raise PortableIdentityConflictError(
                    f"calibration asset {source_id} has conflicting calibration identities"
                )
        row["calibration_id"] = mapped_by_uuid
        existing_rows = destination.execute(
            "SELECT * FROM calibration_assets WHERE lower(asset_uuid)=?", (identity,)
        ).fetchall()
        if len(existing_rows) > 1:
            raise PortableIdentityConflictError(
                f"calibration asset {identity} has ambiguous destination identities"
            )
        existing_row = existing_rows[0] if existing_rows else None
        if existing_row is not None:
            existing = dict(existing_row)
            if existing.get("calibration_id") is not None:
                if int(existing["calibration_id"]) != mapped_by_uuid:
                    raise PortableIdentityConflictError(
                        f"calibration asset {identity} has conflicting calibration_id"
                    )
            if str(existing.get("calibration_uuid") or "").strip():
                if _canonical_uuid(
                    existing["calibration_uuid"],
                    label=f"destination calibration asset {identity} calibration",
                ) != calibration_uuid:
                    raise PortableIdentityConflictError(
                        f"calibration asset {identity} has conflicting calibration_uuid"
                    )
            if (
                existing.get("calibration_id") is None
                and not str(existing.get("calibration_uuid") or "").strip()
            ):
                raise PortableIdentityConflictError(
                    f"calibration asset {identity} has no stable calibration parent"
                )
            if not _equivalent_rows(
                row,
                existing,
                ignored={
                    "id", "calibration_id", "calibration_uuid", "local_path",
                    "original_path", "cloud_storage_path", "created_at",
                },
                metadata_fields={"metadata_json"},
            ):
                raise PortableIdentityConflictError(
                    f"calibration asset {identity} has conflicting immutable content"
                )
            result[source_id] = int(existing_row["id"])
            continue
        new_id = allocator.allocate()
        row["id"] = new_id
        row["cloud_storage_path"] = None
        if row.get("metadata_json"):
            row["metadata_json"] = json.dumps(
                _portable_metadata(
                    row["metadata_json"],
                    preserve_local_paths=preserve_managed_asset_paths,
                ),
                # Archive orchestration has already replaced every retained local
                # location with a destination-managed path. Raw payload imports
                # still strip source-machine paths as in Phase 5.
                ensure_ascii=False, sort_keys=True,
            )
        _insert_row(destination, "calibration_assets", row)
        result[source_id] = new_id
    return result


def _allocate_rows(
    source: sqlite3.Connection,
    destination: sqlite3.Connection,
    table: str,
    existing_map: dict[int, int] | None = None,
) -> tuple[list[dict[str, Any]], dict[int, int]]:
    rows = _rows(source, table)
    source_ids = {int(row["id"]) for row in rows}
    allocator = _IntegerAllocator(destination, table, source_ids)
    result = dict(existing_map or {})
    for source_id in sorted(source_ids):
        if source_id not in result:
            result[source_id] = allocator.allocate()
    return rows, result


def _validate_replayed_stable_content(
    source_main: sqlite3.Connection,
    source_reference: sqlite3.Connection,
    destination: sqlite3.Connection,
    replay: dict[tuple[str, str], str],
) -> None:
    for row in _rows(source_main, "calibrations"):
        destination_id = replay.get(("calibration", str(row["id"])))
        if destination_id is None:
            continue
        existing = destination.execute(
            "SELECT * FROM calibrations WHERE id=?", (destination_id,)
        ).fetchone()
        if existing is None or not _calibrations_equivalent(row, dict(existing)):
            raise PortableIdentityConflictError(
                f"calibration {row['id']} has conflicting destination content"
            )

    for row in _rows(source_main, "calibration_assets"):
        destination_id = replay.get(("calibration_asset", str(row["id"])))
        if destination_id is None:
            continue
        comparable = dict(row)
        comparable["asset_uuid"] = _canonical_uuid(
            comparable.get("asset_uuid"), label=f"calibration asset {row['id']}"
        )
        existing = destination.execute(
            "SELECT * FROM calibration_assets WHERE id=?", (destination_id,)
        ).fetchone()
        if existing is None or not _equivalent_rows(
            comparable,
            dict(existing),
            ignored={
                "id", "calibration_id", "calibration_uuid", "local_path",
                "original_path", "cloud_storage_path", "created_at",
            },
            metadata_fields={"metadata_json"},
        ):
            raise PortableIdentityConflictError(
                f"calibration asset {row['id']} has conflicting destination content"
            )

    for row in _rows(source_reference, "reference_values"):
        destination_id = replay.get(("reference_value", str(row["id"])))
        if destination_id is None:
            continue
        existing = destination.execute(
            "SELECT * FROM portable_reference.reference_values WHERE id=?",
            (destination_id,),
        ).fetchone()
        if existing is None or not _equivalent_rows(
            row, dict(existing), ignored={"id", "updated_at"},
            json_fields={"metadata_json"},
        ):
            raise PortableIdentityConflictError(
                f"reference value {row['id']} has conflicting destination content"
            )

    reference_specs = (
        ("reference_work", "reference_works", {"authors_json", "editors_json"}),
        ("reference_treatment", "reference_taxon_treatments", set()),
        ("reference_measurement_set", "reference_measurement_sets", {"raw_points_json"}),
    )
    for item_type, table, json_fields in reference_specs:
        for row in _rows(source_reference, table):
            destination_id = replay.get((item_type, str(row["id"])))
            if destination_id is None:
                continue
            existing_row = destination.execute(
                f"SELECT * FROM portable_reference.{table} WHERE id=?",
                (destination_id,),
            ).fetchone()
            if existing_row is None:
                raise PortableIdentityConflictError(
                    f"{item_type} {row['id']} has missing destination content"
                )
            existing = dict(existing_row)
            if item_type == "reference_measurement_set" and _normalized_value(
                row.get("supersedes_id")
            ) != _normalized_value(existing.get("supersedes_id")):
                raise PortableIdentityConflictError(
                    f"{item_type} {row['id']} has conflicting immutable destination identity"
                )
            source_revision = int(row.get("revision") or 1)
            destination_revision = int(existing.get("revision") or 1)
            if destination_revision > source_revision:
                continue
            comparable = dict(row)
            if item_type == "reference_measurement_set":
                legacy_id = comparable.get("legacy_reference_value_id")
                if legacy_id is not None:
                    mapped_legacy_id = replay.get(("reference_value", str(legacy_id)))
                    comparable["legacy_reference_value_id"] = (
                        int(mapped_legacy_id) if mapped_legacy_id is not None else None
                    )
                else:
                    comparable["legacy_reference_value_id"] = existing.get(
                        "legacy_reference_value_id"
                    )
            ignored = {"created_at", "updated_at", "owner_id"}
            if item_type == "reference_work":
                ignored.update({"verification_status", "visibility"})
            if not _equivalent_rows(
                comparable, existing, ignored=ignored, json_fields=json_fields
            ):
                raise PortableIdentityConflictError(
                    f"{item_type} {row['id']} has conflicting destination content"
                )


def _asset_entry(
    manifest: ArchiveManifest,
    *,
    prefix: str,
    label: str,
    old_path: object,
    excluded: bool = False,
) -> object:
    """Return the one manifest entry for a populated database asset slot."""
    if not str(old_path or "").strip():
        return None
    marker = f"{prefix}/{label}."
    matches = [entry for entry in manifest.files if entry.path.startswith(marker)]
    if len(matches) != 1:
        raise PortableImportError(f"missing or ambiguous portable asset slot: {prefix}/{label}")
    entry = matches[0]
    if excluded and entry.status != "excluded_by_policy":
        raise PortableImportError(f"cache asset was not excluded by policy: {entry.path}")
    if not excluded and entry.status == "excluded_by_policy":
        raise PortableImportError(
            f"authoritative asset was excluded by policy: {entry.path}"
        )
    return entry


def _managed_asset_path(
    assets_root: Path,
    *,
    archive_id: str,
    archive_path: str,
    sha256: str,
    category: str,
) -> Path:
    suffix = Path(archive_path).suffix.lower()
    if not suffix or len(suffix) > 12 or not suffix[1:].isalnum():
        suffix = ".bin"
    identity = uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"sporely-portable:{archive_id}:{archive_path}:{sha256}",
    )
    destination = (assets_root / category / f"{identity}{suffix}").resolve()
    root = assets_root.resolve()
    if root not in destination.parents:
        raise PortableImportError("managed asset destination escapes its root")
    return destination


def _rewrite_json_asset_paths(
    value: object,
    *,
    manifest: ArchiveManifest,
    staging: Path,
    assets_root: Path,
    archive_id: str,
    prefix: str,
    category: str,
    nested_images: bool,
    excluded: bool = False,
    planned: list[_ManagedAsset],
    consumed: set[str],
) -> object:
    if value is None or value == "":
        return value
    try:
        loaded = json.loads(value) if isinstance(value, str) else value
    except (json.JSONDecodeError, TypeError) as exc:
        raise PortableImportError("calibration asset metadata is not valid JSON") from exc
    if not isinstance(loaded, dict):
        raise PortableImportError("calibration asset metadata must be a JSON object")
    if nested_images:
        image_records = loaded.get("images")
        auto_records = loaded.get("auto_images") or []
        if not isinstance(image_records, list) or not isinstance(auto_records, list):
            raise PortableImportError("calibration measurements images must be a JSON array")
        records = [
            (str(index), record) for index, record in enumerate(image_records)
        ] + [
            (f"auto-{index}", record) for index, record in enumerate(auto_records)
        ]
    else:
        records = [("0", loaded)]
    if not isinstance(records, list):
        raise PortableImportError("calibration measurements images must be a JSON array")
    keys = (
        ("source_path", "source_filepath", "original_path", "selected_path", "path", "working_path")
        if nested_images else
        ("source_path", "working_path", "original_path", "local_path")
    )
    for index, record in records:
        if not isinstance(record, dict):
            raise PortableImportError("calibration measurements images must contain objects")
        record_excluded = excluded or (
            str(record.get("source_role") or "").lower() == "cloud_recovery_cache"
            or str(record.get("file_purpose") or "").lower() == "cache"
        )
        for key in keys:
            if not str(record.get(key) or "").strip():
                continue
            label = f"metadata-{index}-{key}" if nested_images else f"metadata-{key}"
            record[key] = _resolve_asset_slot(
                manifest=manifest, staging=staging, assets_root=assets_root,
                archive_id=archive_id, prefix=prefix, label=label,
                old_path=record[key], category=category, excluded=record_excluded,
                planned=planned, consumed=consumed,
            )
        companions = record.get("companion_paths")
        if isinstance(companions, list):
            for companion_index, old_path in enumerate(companions):
                if not str(old_path or "").strip():
                    continue
                label = (
                    f"metadata-{index}-companion_{companion_index}"
                    if nested_images else f"metadata-companion-{companion_index}"
                )
                companions[companion_index] = _resolve_asset_slot(
                    manifest=manifest, staging=staging, assets_root=assets_root,
                    archive_id=archive_id, prefix=prefix, label=label,
                    old_path=old_path, category=category, excluded=record_excluded,
                    planned=planned, consumed=consumed,
                )
    return json.dumps(loaded, ensure_ascii=False, sort_keys=True)


def _resolve_asset_slot(
    *,
    manifest: ArchiveManifest,
    staging: Path,
    assets_root: Path,
    archive_id: str,
    prefix: str,
    label: str,
    old_path: object,
    category: str,
    planned: list[_ManagedAsset],
    consumed: set[str],
    excluded: bool = False,
) -> str | None:
    entry = _asset_entry(
        manifest, prefix=prefix, label=label, old_path=old_path, excluded=excluded
    )
    if entry is None:
        return None
    consumed.add(entry.path)
    if entry.status != "included":
        return None
    assert entry.sha256 is not None
    staged = safe_staging_destination(staging, entry.path)
    destination = _managed_asset_path(
        assets_root, archive_id=archive_id, archive_path=entry.path,
        sha256=entry.sha256, category=category,
    )
    planned.append(_ManagedAsset(entry.path, staged, destination, entry.sha256))
    return str(destination)


def _source_paths_equivalent(left: str, right: str) -> bool:
    if not left or not right:
        return False
    left_path = Path(left)
    right_path = Path(right)
    if left_path.is_absolute() == right_path.is_absolute():
        return os.path.normcase(os.path.normpath(left)) == os.path.normcase(os.path.normpath(right))
    absolute, relative = (
        (left_path, right_path) if left_path.is_absolute() else (right_path, left_path)
    )
    relative_parts = relative.parts
    return bool(relative_parts) and tuple(absolute.parts[-len(relative_parts):]) == relative_parts


def _prepare_portable_assets(
    database: Path,
    *,
    manifest: ArchiveManifest,
    staging: Path,
    assets_root: Path,
) -> list[_ManagedAsset]:
    planned: list[_ManagedAsset] = []
    consumed: set[str] = set()
    image_path_candidates: dict[str, set[str | None]] = {}
    image_paths_by_id: dict[int, dict[str, str | None]] = {}
    with sqlite3.connect(database) as connection:
        connection.row_factory = sqlite3.Row
        for row in connection.execute(
            "SELECT id, filepath, original_filepath, source_role, file_purpose FROM images"
        ):
            excluded = (
                str(row["source_role"] or "").lower() == "cloud_recovery_cache"
                or str(row["file_purpose"] or "").lower() == "cache"
            )
            updates: dict[str, str | None] = {}
            for field, root, label, category in (
                ("filepath", "images", "working", "images"),
                ("original_filepath", "originals", "original", "originals"),
            ):
                updates[field] = _resolve_asset_slot(
                    manifest=manifest, staging=staging, assets_root=assets_root,
                    archive_id=manifest.archive_id,
                    prefix=f"portable/assets/{root}/{row['id']}", label=label,
                    old_path=row[field], category=category, excluded=excluded,
                    planned=planned, consumed=consumed,
                )
                if str(row[field] or "").strip():
                    old_path = str(row[field])
                    image_path_candidates.setdefault(old_path, set()).add(updates[field])
                    image_paths_by_id.setdefault(int(row["id"]), {})[old_path] = updates[field]
            connection.execute(
                "UPDATE images SET filepath=?, original_filepath=? WHERE id=?",
                (updates["filepath"] or "", updates["original_filepath"], row["id"]),
            )

        unambiguous_rewrites = {
            old_path: next(iter(destinations)) if len(destinations) == 1 else None
            for old_path, destinations in image_path_candidates.items()
        }

        def rewrite_embedded_paths(
            value: object, *, image_id: int | None = None
        ) -> object:
            try:
                loaded = json.loads(value) if isinstance(value, str) else value
            except (json.JSONDecodeError, TypeError):
                return value

            def rewrite(item: object) -> object:
                if isinstance(item, dict):
                    return {key: rewrite(child) for key, child in item.items()}
                if isinstance(item, list):
                    return [rewrite(child) for child in item]
                if isinstance(item, str):
                    image_rewrites = image_paths_by_id.get(image_id or -1, {})
                    if item in image_rewrites:
                        return image_rewrites[item]
                    if item in unambiguous_rewrites:
                        return unambiguous_rewrites[item]
                return item

            return json.dumps(rewrite(loaded), ensure_ascii=False, sort_keys=True)

        for table, field in (("session_logs", "metadata_json"), ("observations", "ai_state_json")):
            for row in connection.execute(
                f"SELECT id, {field} FROM {table} WHERE {field} IS NOT NULL"
            ):
                embedded_image_id = None
                if table == "session_logs":
                    try:
                        metadata = json.loads(row[field])
                        if isinstance(metadata, dict) and metadata.get("image_id") is not None:
                            embedded_image_id = int(metadata["image_id"])
                    except (json.JSONDecodeError, TypeError, ValueError):
                        pass
                connection.execute(
                    f"UPDATE {table} SET {field}=? WHERE id=?",
                    (rewrite_embedded_paths(row[field], image_id=embedded_image_id), row["id"]),
                )

        excluded_calibration_paths: set[str] = set()
        for row in connection.execute(
            "SELECT local_path, original_path, source_role, file_purpose, metadata_json "
            "FROM calibration_assets"
        ):
            excluded = (
                str(row["source_role"] or "").lower() == "cloud_recovery_cache"
                or str(row["file_purpose"] or "").lower() == "cache"
            )
            if excluded:
                for value in (row["local_path"], row["original_path"]):
                    if str(value or "").strip():
                        excluded_calibration_paths.add(str(value))
                try:
                    metadata = json.loads(row["metadata_json"] or "null")
                except (json.JSONDecodeError, TypeError):
                    metadata = None
                if isinstance(metadata, dict):
                    for key in ("source_path", "working_path", "original_path", "local_path"):
                        if str(metadata.get(key) or "").strip():
                            excluded_calibration_paths.add(str(metadata[key]))
                    excluded_calibration_paths.update(
                        str(value) for value in (metadata.get("companion_paths") or []) if value
                    )

        for row in connection.execute(
            "SELECT id, image_filepath, measurements_json FROM calibrations"
        ):
            prefix = f"portable/assets/calibrations/records/{row['id']}"
            calibration_path = str(row["image_filepath"] or "")
            excluded = any(
                _source_paths_equivalent(calibration_path, cache_path)
                for cache_path in excluded_calibration_paths
            )
            image_path = _resolve_asset_slot(
                manifest=manifest, staging=staging, assets_root=assets_root,
                archive_id=manifest.archive_id, prefix=prefix, label="working",
                old_path=row["image_filepath"], category="calibrations/records",
                excluded=excluded, planned=planned, consumed=consumed,
            )
            metadata = _rewrite_json_asset_paths(
                row["measurements_json"], manifest=manifest, staging=staging,
                assets_root=assets_root, archive_id=manifest.archive_id,
                prefix=prefix, category="calibrations/records", nested_images=True,
                planned=planned, consumed=consumed,
            )
            connection.execute(
                "UPDATE calibrations SET image_filepath=?, measurements_json=? WHERE id=?",
                (image_path, metadata, row["id"]),
            )

        for row in connection.execute(
            "SELECT id, local_path, original_path, source_role, file_purpose, metadata_json "
            "FROM calibration_assets"
        ):
            prefix = f"portable/assets/calibrations/assets/{row['id']}"
            excluded = (
                str(row["source_role"] or "").lower() == "cloud_recovery_cache"
                or str(row["file_purpose"] or "").lower() == "cache"
            )
            local_path = _resolve_asset_slot(
                manifest=manifest, staging=staging, assets_root=assets_root,
                archive_id=manifest.archive_id, prefix=prefix, label="local",
                old_path=row["local_path"], category="calibrations/assets",
                excluded=excluded, planned=planned, consumed=consumed,
            )
            original_path = _resolve_asset_slot(
                manifest=manifest, staging=staging, assets_root=assets_root,
                archive_id=manifest.archive_id, prefix=prefix, label="original",
                old_path=row["original_path"], category="calibrations/assets",
                excluded=excluded, planned=planned, consumed=consumed,
            )
            metadata = _rewrite_json_asset_paths(
                row["metadata_json"], manifest=manifest, staging=staging,
                assets_root=assets_root, archive_id=manifest.archive_id,
                prefix=prefix, category="calibrations/assets", nested_images=False,
                excluded=excluded, planned=planned, consumed=consumed,
            )
            connection.execute(
                "UPDATE calibration_assets SET local_path=?, original_path=?, metadata_json=? "
                "WHERE id=?", (local_path, original_path, metadata, row["id"]),
            )
        connection.commit()

    declared_assets = {
        entry.path for entry in manifest.files if entry.path.startswith("portable/assets/")
    }
    if declared_assets != consumed:
        unexpected = sorted(declared_assets - consumed)
        raise PortableImportError(
            "portable archive contains unmapped asset entries: " + ", ".join(unexpected)
        )
    destinations: dict[str, str] = {}
    unique: list[_ManagedAsset] = []
    for item in planned:
        folded = str(item.destination_path).casefold()
        existing = destinations.get(folded)
        if existing is not None and existing != item.sha256:
            raise PortableImportError("managed asset destination collision")
        if existing is None:
            destinations[folded] = item.sha256
            unique.append(item)
    return unique


def _metadata_asset_slots(value: object, *, nested_images: bool) -> dict[str, str]:
    loaded = _canonical_json(value)
    if not isinstance(loaded, dict):
        return {}
    records: list[tuple[str, object]] = []
    if nested_images:
        for collection in ("images", "auto_images"):
            items = loaded.get(collection) or []
            if isinstance(items, list):
                records.extend(
                    (str(index) if collection == "images" else f"auto-{index}", item)
                    for index, item in enumerate(items)
                )
    else:
        records = [("0", loaded)]
    keys = (
        ("source_path", "source_filepath", "original_path", "selected_path", "path", "working_path")
        if nested_images else
        ("source_path", "working_path", "original_path", "local_path")
    )
    slots: dict[str, str] = {}
    for token, record in records:
        if not isinstance(record, dict):
            continue
        for key in keys:
            if str(record.get(key) or "").strip():
                slots[f"{token}:{key}"] = str(record[key])
        for index, path in enumerate(record.get("companion_paths") or []):
            if str(path or "").strip():
                slots[f"{token}:companion_{index}"] = str(path)
    return slots


def _validate_reused_stable_asset_bytes(
    source_database: Path,
    destination_database: Path,
    planned: list[_ManagedAsset],
) -> None:
    planned_by_path = {str(item.destination_path): item.sha256 for item in planned}

    def require_matching(
        source_slots: dict[str, str], destination_slots: dict[str, str], identity: str
    ) -> None:
        for slot, source_path in source_slots.items():
            expected = planned_by_path.get(source_path)
            if expected is None:
                continue
            destination_path = Path(destination_slots.get(slot, ""))
            if not destination_path.is_file() or sha256_file(destination_path) != expected:
                raise PortableIdentityConflictError(
                    f"{identity} has conflicting authoritative asset bytes"
                )

    with sqlite3.connect(source_database) as source, sqlite3.connect(
        destination_database
    ) as destination:
        source.row_factory = sqlite3.Row
        destination.row_factory = sqlite3.Row
        for source_row in source.execute("SELECT * FROM calibrations"):
            existing = destination.execute(
                "SELECT * FROM calibrations WHERE lower(calibration_uuid)=lower(?)",
                (source_row["calibration_uuid"],),
            ).fetchone()
            if existing is None:
                continue
            source_slots = {"working": str(source_row["image_filepath"] or "")}
            source_slots.update({
                f"metadata:{key}": value for key, value in _metadata_asset_slots(
                    source_row["measurements_json"], nested_images=True
                ).items()
            })
            destination_slots = {"working": str(existing["image_filepath"] or "")}
            destination_slots.update({
                f"metadata:{key}": value for key, value in _metadata_asset_slots(
                    existing["measurements_json"], nested_images=True
                ).items()
            })
            require_matching(
                source_slots, destination_slots,
                f"calibration {source_row['calibration_uuid']}",
            )

        for source_row in source.execute("SELECT * FROM calibration_assets"):
            existing = destination.execute(
                "SELECT * FROM calibration_assets WHERE lower(asset_uuid)=lower(?)",
                (source_row["asset_uuid"],),
            ).fetchone()
            if existing is None:
                continue
            source_slots = {
                "local": str(source_row["local_path"] or ""),
                "original": str(source_row["original_path"] or ""),
            }
            source_slots.update({
                f"metadata:{key}": value for key, value in _metadata_asset_slots(
                    source_row["metadata_json"], nested_images=False
                ).items()
            })
            destination_slots = {
                "local": str(existing["local_path"] or ""),
                "original": str(existing["original_path"] or ""),
            }
            destination_slots.update({
                f"metadata:{key}": value for key, value in _metadata_asset_slots(
                    existing["metadata_json"], nested_images=False
                ).items()
            })
            require_matching(
                source_slots, destination_slots,
                f"calibration asset {source_row['asset_uuid']}",
            )


def _extract_portable_archive(
    archive_path: Path, staging: Path, manifest: ArchiveManifest
) -> None:
    included = {entry.path: entry for entry in manifest.files if entry.status == "included"}
    try:
        with ZipFile(archive_path) as archive:
            names = validate_zip_entries(archive.infolist())
            if set(names) - {"manifest.json"} != set(included):
                raise PortableImportError("manifest and ZIP members are not a bijection")
            for name in names:
                if name == "manifest.json":
                    continue
                destination = safe_staging_destination(staging, name)
                destination.parent.mkdir(parents=True, exist_ok=True)
                digest = hashlib.sha256()
                size = 0
                with archive.open(name) as source, destination.open("xb") as target:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        target.write(chunk)
                        digest.update(chunk)
                        size += len(chunk)
                entry = included[name]
                if size != entry.size or digest.hexdigest() != entry.sha256:
                    raise PortableImportError(
                        f"size or checksum mismatch during extraction: {name}"
                    )
    except (BadZipFile, KeyError, OSError) as exc:
        raise PortableImportError(f"portable archive extraction failed: {exc}") from exc


def _promote_staged_asset(source: Path, destination: Path) -> bool:
    """Install one immutable asset without overwriting an existing path."""
    if destination.exists():
        if not destination.is_file() or sha256_file(destination) != sha256_file(source):
            raise PortableImportError(f"managed asset destination conflict: {destination}")
        return False
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.parent / f".{destination.name}.{uuid.uuid4().hex}.tmp"
    try:
        with source.open("rb") as input_file, temporary.open("xb") as output_file:
            shutil.copyfileobj(input_file, output_file, length=1024 * 1024)
            output_file.flush()
            os.fsync(output_file.fileno())
        try:
            os.link(temporary, destination)
        except FileExistsError:
            if sha256_file(destination) != sha256_file(source):
                raise PortableImportError(
                    f"managed asset destination conflict: {destination}"
                )
            return False
        _fsync_directory(destination.parent)
        return True
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            # The immutable final link is authoritative once created. A stale
            # dot-temp is safer than masking ownership of the final path.
            pass


def _remove_empty_parents(path: Path, stop: Path) -> None:
    parent = path.parent
    while parent != stop and stop in parent.parents:
        try:
            parent.rmdir()
        except OSError:
            break
        parent = parent.parent


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _merged_objectives_payload(
    staged_main: Path, incoming_path: Path, destination_path: Path
) -> bytes | None:
    try:
        incoming = json.loads(incoming_path.read_text(encoding="utf-8"))
        current = (
            json.loads(destination_path.read_text(encoding="utf-8"))
            if destination_path.is_file() else {}
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PortableImportError("objective profiles are not valid JSON") from exc
    if not isinstance(incoming, dict) or not isinstance(current, dict):
        raise PortableImportError("objective profiles must be JSON objects")
    with sqlite3.connect(staged_main) as connection:
        used = {
            str(row[0]).strip()
            for row in connection.execute(
                "SELECT objective_name FROM images WHERE objective_name IS NOT NULL "
                "UNION SELECT objective_key FROM calibrations WHERE objective_key IS NOT NULL"
            )
            if str(row[0]).strip()
        }
    selected = {
        key: value for key, value in incoming.items()
        if key in used or (isinstance(value, dict) and str(value.get("name") or "") in used)
    }
    merged = dict(current)
    for key, value in selected.items():
        if key in merged and _normalized_value(merged[key]) != _normalized_value(value):
            raise PortableIdentityConflictError(
                f"objective profile {key} conflicts with destination content"
            )
        merged[key] = value
    if merged == current:
        return None
    return (json.dumps(
        merged, ensure_ascii=False, sort_keys=True, indent=2
    ) + "\n").encode("utf-8")


def _normalize_selected_observations(
    database: Path, observation_ids: set[int], *, allow_empty: bool
) -> set[int]:
    try:
        normalized = {int(value) for value in observation_ids}
    except (TypeError, ValueError) as exc:
        raise PortableImportError("observation IDs must be integers") from exc
    if any(value <= 0 for value in normalized) or (not normalized and not allow_empty):
        raise PortableImportError("at least one positive observation ID is required")
    with sqlite3.connect(f"{database.resolve().as_uri()}?mode=ro", uri=True) as connection:
        available = {int(row[0]) for row in connection.execute("SELECT id FROM observations")}
    missing = normalized - available
    if missing:
        raise PortableImportError(
            "selected observations do not exist in the archive: "
            + ", ".join(str(value) for value in sorted(missing))
        )
    return normalized


def _apply_portable_selection(
    main_database: Path, reference_database: Path, observation_ids: set[int]
) -> None:
    normalized = _normalize_selected_observations(
        main_database, observation_ids, allow_empty=False
    )
    from utils.archive.portable_export import (
        _prune_main_database,
        _prune_reference_database,
    )

    _prune_main_database(main_database, normalized)
    _prune_reference_database(main_database, reference_database)


def _manifest_for_staged_closure(
    manifest: ArchiveManifest, main_database: Path
) -> ArchiveManifest:
    with sqlite3.connect(f"{main_database.resolve().as_uri()}?mode=ro", uri=True) as connection:
        image_ids = {str(row[0]) for row in connection.execute("SELECT id FROM images")}
        calibration_ids = {
            str(row[0]) for row in connection.execute("SELECT id FROM calibrations")
        }
        calibration_asset_ids = {
            str(row[0]) for row in connection.execute("SELECT id FROM calibration_assets")
        }

    def retained(path: str) -> bool:
        parts = path.split("/")
        if parts[:3] == ["portable", "assets", "images"]:
            return len(parts) > 3 and parts[3] in image_ids
        if parts[:3] == ["portable", "assets", "originals"]:
            return len(parts) > 3 and parts[3] in image_ids
        if parts[:4] == ["portable", "assets", "calibrations", "records"]:
            return len(parts) > 4 and parts[4] in calibration_ids
        if parts[:4] == ["portable", "assets", "calibrations", "assets"]:
            return len(parts) > 4 and parts[4] in calibration_asset_ids
        return not path.startswith("portable/assets/")

    return replace(manifest, files=tuple(entry for entry in manifest.files if retained(entry.path)))


def _preview_staging(
    archive: Path,
    manifest: ArchiveManifest,
    staging: Path,
    observation_ids: set[int] | None = None,
) -> tuple[Path, Path, ArchiveManifest]:
    _extract_portable_archive(archive, staging, manifest)
    main_database = staging / "portable/mushrooms.db"
    reference_database = staging / "portable/reference_values.db"
    selected_manifest = manifest
    if observation_ids is not None:
        if not observation_ids:
            return main_database, reference_database, selected_manifest
        _apply_portable_selection(main_database, reference_database, observation_ids)
        selected_manifest = _manifest_for_staged_closure(manifest, main_database)
    else:
        from utils.archive.portable_export import _contents

        before = _contents(main_database, reference_database)
        with sqlite3.connect(
            f"{main_database.resolve().as_uri()}?mode=ro", uri=True
        ) as connection:
            all_observation_ids = {
                int(row[0]) for row in connection.execute("SELECT id FROM observations")
            }
        if not all_observation_ids:
            raise PortableImportError("portable archive contains no observations")
        _apply_portable_selection(
            main_database, reference_database, all_observation_ids
        )
        after = _contents(main_database, reference_database)
        if before != after:
            raise PortableImportError(
                "portable archive contains data outside its observation dependency closure"
            )
    _prepare_portable_assets(
        main_database,
        manifest=selected_manifest,
        staging=staging,
        assets_root=staging / "preview-assets",
    )
    return main_database, reference_database, selected_manifest


def preview_portable_archive(archive_path: str | Path) -> PortableArchivePreview:
    """Validate an archive and build a temporary, non-actionable preview model."""
    archive = Path(archive_path)
    try:
        manifest = validate_portable_observations(archive)
        with tempfile.TemporaryDirectory(prefix="sporely-portable-preview-") as temporary:
            main_database, _reference_database, _manifest = _preview_staging(
                archive, manifest, Path(temporary)
            )
            with sqlite3.connect(
                f"{main_database.resolve().as_uri()}?mode=ro", uri=True
            ) as connection:
                connection.row_factory = sqlite3.Row
                observations = tuple(
                    PortableObservationPreview(
                        observation_id=int(row["id"]),
                        name=(
                            " ".join(
                                part for part in (
                                    str(row["genus"] or "").strip(),
                                    str(row["species"] or "").strip(),
                                ) if part
                            )
                            or f"Observation {row['id']}"
                        ),
                        date=str(row["date"] or ""),
                        image_count=int(row["image_count"]),
                    )
                    for row in connection.execute(
                        "SELECT observation.id, observation.genus, observation.species, "
                        "observation.date, COUNT(image.id) AS image_count "
                        "FROM observations AS observation "
                        "LEFT JOIN images AS image ON image.observation_id=observation.id "
                        "GROUP BY observation.id ORDER BY observation.date DESC, observation.id"
                    )
                )
    except Exception as exc:
        if isinstance(exc, PortableImportError):
            raise
        raise PortableImportError(f"portable archive validation failed: {exc}") from exc
    return PortableArchivePreview(
        archive_path=archive,
        archive_sha256=sha256_file(archive),
        archive_id=manifest.archive_id,
        created_at=manifest.created_at,
        app_version=manifest.app_version,
        source_platform=manifest.source_platform,
        observations=observations,
        full_counts=PortableClosureCounts(
            observations=manifest.contents["observations"],
            images=manifest.contents["images"],
            measurements=manifest.contents["measurements"],
            calibrations=manifest.contents["calibrations"],
            references=manifest.contents["reference_works"],
        ),
    )


def _portable_closure_counts(
    archive_path: Path,
    observation_ids: set[int],
    *,
    expected_sha256: str,
) -> PortableClosureCounts:
    if not observation_ids:
        return PortableClosureCounts(0, 0, 0, 0, 0)
    try:
        if sha256_file(archive_path) != expected_sha256:
            raise PortableImportError("portable archive changed after it was previewed")
        with tempfile.TemporaryDirectory(prefix="sporely-portable-preview-") as temporary:
            root = Path(temporary)
            main_database = root / "portable/mushrooms.db"
            reference_database = root / "portable/reference_values.db"
            main_database.parent.mkdir(parents=True)
            with ZipFile(archive_path) as archive:
                for name, destination in (
                    ("portable/mushrooms.db", main_database),
                    ("portable/reference_values.db", reference_database),
                ):
                    with archive.open(name) as source, destination.open("xb") as target:
                        shutil.copyfileobj(source, target, length=1024 * 1024)
            _apply_portable_selection(main_database, reference_database, observation_ids)
            from utils.archive.portable_export import _contents

            counts = _contents(main_database, reference_database)
    except Exception as exc:
        if isinstance(exc, PortableImportError):
            raise
        raise PortableImportError(f"portable archive validation failed: {exc}") from exc
    return PortableClosureCounts(
        observations=counts["observations"],
        images=counts["images"],
        measurements=counts["measurements"],
        calibrations=counts["calibrations"],
        references=counts["reference_works"],
    )


def _recover_import_journals(
    assets_root: Path, connection: sqlite3.Connection
) -> None:
    journal_root = assets_root / ".portable-import-journals"
    if not journal_root.is_dir():
        return
    referenced = _referenced_asset_paths(connection)
    for journal in sorted(journal_root.glob("*.json")):
        try:
            values = json.loads(journal.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        objective_state = None
        archive_id = None
        if isinstance(values, dict):
            paths = values.get("created_paths")
            objective_state = values.get("objective")
            archive_id = values.get("archive_id")
        else:
            paths = values
        if not isinstance(paths, list) or not all(isinstance(value, str) for value in paths):
            continue
        committed = bool(
            archive_id
            and connection.execute(
                "SELECT 1 FROM portable_import_provenance WHERE archive_id=? "
                "AND source_item_type='archive' AND source_item_id='inventory'",
                (archive_id,),
            ).fetchone()
        )
        if isinstance(objective_state, dict) and not committed:
            destination = Path(str(objective_state.get("path") or ""))
            previous = objective_state.get("previous_base64")
            if objective_state.get("was_present") and isinstance(previous, str):
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(base64.b64decode(previous, validate=True))
            elif destination.name == "objectives.json":
                destination.unlink(missing_ok=True)
        for value in paths:
            candidate = Path(value).resolve()
            if assets_root not in candidate.parents or str(candidate) in referenced:
                continue
            candidate.unlink(missing_ok=True)
            _remove_empty_parents(candidate, assets_root)
        journal.unlink(missing_ok=True)
        _fsync_directory(journal_root)
    try:
        journal_root.rmdir()
    except OSError:
        pass


def _write_import_journal(
    assets_root: Path,
    paths: list[Path],
    *,
    archive_id: str,
    objective_path: Path | None = None,
    objective_previous: bytes | None = None,
    objective_was_present: bool = False,
) -> Path:
    journal_root = assets_root / ".portable-import-journals"
    journal_root.mkdir(parents=True, exist_ok=True)
    journal = journal_root / f"{uuid.uuid4()}.json"
    payload_value: dict[str, object] = {
        "archive_id": archive_id,
        "created_paths": [str(path) for path in paths],
    }
    if objective_path is not None:
        payload_value["objective"] = {
            "path": str(objective_path),
            "was_present": objective_was_present,
            "previous_base64": (
                base64.b64encode(objective_previous).decode("ascii")
                if objective_previous is not None else None
            ),
        }
    payload = (json.dumps(payload_value, sort_keys=True) + "\n").encode("utf-8")
    with journal.open("xb") as output:
        output.write(payload)
        output.flush()
        os.fsync(output.fileno())
    _fsync_directory(journal_root)
    return journal


def _referenced_asset_paths(connection: sqlite3.Connection) -> set[str]:
    referenced: set[str] = set()
    for table, fields in (
        ("images", ("filepath", "original_filepath")),
        ("calibrations", ("image_filepath", "measurements_json")),
        ("calibration_assets", ("local_path", "original_path", "metadata_json")),
    ):
        columns = ", ".join(fields)
        for row in connection.execute(f"SELECT {columns} FROM {table}"):
            for value in row:
                if not isinstance(value, str) or not value:
                    continue
                if value.startswith(("{", "[")):
                    try:
                        loaded = json.loads(value)
                    except json.JSONDecodeError:
                        continue
                    stack = [loaded]
                    while stack:
                        current = stack.pop()
                        if isinstance(current, dict):
                            stack.extend(current.values())
                        elif isinstance(current, list):
                            stack.extend(current)
                        elif isinstance(current, str) and current:
                            referenced.add(current)
                else:
                    referenced.add(value)
    return referenced


def import_portable_archive(
    archive_path: str | Path,
    *,
    destination_main_database: str | Path,
    destination_reference_database: str | Path,
    destination_assets_root: str | Path,
    destination_objectives_path: str | Path | None = None,
    observation_ids: set[int] | None = None,
    expected_archive_sha256: str | None = None,
) -> PortableImportResult:
    """Validate and import one portable archive with managed local assets."""
    archive = Path(archive_path)
    if expected_archive_sha256 is not None and sha256_file(archive) != expected_archive_sha256:
        raise PortableImportError("portable archive changed after it was previewed")
    try:
        manifest = validate_portable_observations(archive)
    except (ArchiveValidationError, ValueError) as exc:
        raise PortableImportError(f"portable archive validation failed: {exc}") from exc
    assets_root = Path(destination_assets_root).resolve()
    created: list[Path] = []
    database_phase_started = False
    journal: Path | None = None
    objective_destination = (
        Path(destination_objectives_path).resolve()
        if destination_objectives_path is not None
        else Path(destination_main_database).resolve().parent / "objectives.json"
    )
    objective_previous: bytes | None = None
    objective_was_present = False
    objective_written = False
    try:
        with tempfile.TemporaryDirectory(prefix="sporely-portable-import-") as temporary:
            staging = Path(temporary)
            _extract_portable_archive(archive, staging, manifest)
            staged_main = staging / "portable/mushrooms.db"
            staged_reference = staging / "portable/reference_values.db"
            with sqlite3.connect(
                f"{staged_main.resolve().as_uri()}?mode=ro", uri=True
            ) as source_main, sqlite3.connect(
                f"{staged_reference.resolve().as_uri()}?mode=ro", uri=True
            ) as source_reference:
                source_main.row_factory = sqlite3.Row
                source_reference.row_factory = sqlite3.Row
                full_source_items = _source_provenance_items(source_main, source_reference)
            inventory_fingerprint_override = _archive_inventory_fingerprint(
                full_source_items, manifest
            )
            if observation_ids is not None:
                _apply_portable_selection(staged_main, staged_reference, observation_ids)
                manifest = _manifest_for_staged_closure(manifest, staged_main)
            with sqlite3.connect(
                f"{staged_main.resolve().as_uri()}?mode=ro", uri=True
            ) as source_main, sqlite3.connect(
                f"{staged_reference.resolve().as_uri()}?mode=ro", uri=True
            ) as source_reference:
                source_main.row_factory = sqlite3.Row
                source_reference.row_factory = sqlite3.Row
                source_items = _source_provenance_items(source_main, source_reference)
            for key in tuple(source_items):
                if key[0] == "session" and key in full_source_items:
                    source_items[key] = full_source_items[key]
            objective_payload = _merged_objectives_payload(
                staged_main,
                staging / "portable/objectives.json",
                objective_destination,
            )
            planned = _prepare_portable_assets(
                staged_main, manifest=manifest, staging=staging,
                assets_root=assets_root,
            )
            _validate_reused_stable_asset_bytes(
                staged_main, Path(destination_main_database), planned
            )
            def promote_before_commit(
                _result: PortableImportResult, connection: sqlite3.Connection
            ) -> None:
                nonlocal journal, objective_previous, objective_was_present, objective_written
                _recover_import_journals(assets_root, connection)
                referenced = _referenced_asset_paths(connection)
                for destination_id in _result.image_id_map.values():
                    row = connection.execute(
                        "SELECT filepath, original_filepath FROM images WHERE id=?",
                        (destination_id,),
                    ).fetchone()
                    if row is None:
                        continue
                    for value in row:
                        if not value:
                            continue
                        resolved = Path(value).resolve()
                        if assets_root not in resolved.parents:
                            raise PortableImportError(
                                "portable replay destination asset root does not match"
                            )
                selected = [
                    item for item in planned if str(item.destination_path) in referenced
                ]
                newly_owned = [item for item in selected if not item.destination_path.exists()]
                if objective_payload is not None:
                    objective_was_present = objective_destination.is_file()
                    objective_previous = (
                        objective_destination.read_bytes()
                        if objective_was_present else None
                    )
                if newly_owned or objective_payload is not None:
                    journal = _write_import_journal(
                        assets_root,
                        [item.destination_path for item in newly_owned],
                        archive_id=manifest.archive_id,
                        objective_path=(
                            objective_destination if objective_payload is not None else None
                        ),
                        objective_previous=objective_previous,
                        objective_was_present=objective_was_present,
                    )
                for item in selected:
                    if sha256_file(item.staged_path) != item.sha256:
                        raise PortableImportError(
                            f"staged asset checksum changed: {item.archive_path}"
                        )
                    if _promote_staged_asset(item.staged_path, item.destination_path):
                        created.append(item.destination_path)
                if objective_payload is not None:
                    objective_destination.parent.mkdir(parents=True, exist_ok=True)
                    descriptor, temporary_name = tempfile.mkstemp(
                        prefix=".objectives.", suffix=".tmp",
                        dir=objective_destination.parent,
                    )
                    temporary_path = Path(temporary_name)
                    try:
                        with os.fdopen(descriptor, "wb") as handle:
                            handle.write(objective_payload)
                            handle.flush()
                            os.fsync(handle.fileno())
                        os.replace(temporary_path, objective_destination)
                        _fsync_directory(objective_destination.parent)
                        objective_written = True
                    finally:
                        temporary_path.unlink(missing_ok=True)

            def cleanup_before_rollback() -> None:
                nonlocal objective_written
                for path in reversed(created):
                    path.unlink(missing_ok=True)
                    _remove_empty_parents(path, assets_root)
                created.clear()
                if objective_written:
                    if objective_was_present and objective_previous is not None:
                        objective_destination.write_bytes(objective_previous)
                    else:
                        objective_destination.unlink(missing_ok=True)
                    objective_written = False

            database_phase_started = True
            result = import_portable_payload(
                staged_main, staged_reference,
                destination_main_database=destination_main_database,
                destination_reference_database=destination_reference_database,
                archive_id=manifest.archive_id,
                _before_commit=promote_before_commit,
                _preserve_managed_asset_paths=True,
                _source_items_override=source_items,
                _archive_inventory_fingerprint_override=inventory_fingerprint_override,
                _on_rollback=cleanup_before_rollback,
                _managed_assets_root=assets_root,
            )
            if journal is not None:
                journal.unlink(missing_ok=True)
                _fsync_directory(journal.parent)
                try:
                    journal.parent.rmdir()
                except OSError:
                    pass
            return result
    except Exception as exc:
        if not database_phase_started:
            for path in reversed(created):
                path.unlink(missing_ok=True)
                _remove_empty_parents(path, assets_root)
        if journal is not None and not created:
            journal.unlink(missing_ok=True)
        if isinstance(exc, PortableImportError):
            raise
        raise PortableImportError(f"portable asset import failed: {exc}") from exc


def import_portable_payload(
    source_main_database: str | Path,
    source_reference_database: str | Path,
    *,
    destination_main_database: str | Path,
    destination_reference_database: str | Path,
    archive_id: str,
    _before_commit: Callable[[PortableImportResult, sqlite3.Connection], None] | None = None,
    _preserve_managed_asset_paths: bool = False,
    _source_items_override: dict[tuple[str, str], str] | None = None,
    _archive_inventory_fingerprint_override: str | None = None,
    _on_rollback: Callable[[], None] | None = None,
    _managed_assets_root: Path | None = None,
) -> PortableImportResult:
    """Import a validated portable payload with replay-safe identity remapping."""
    normalized_archive_id = str(archive_id)
    if not normalized_archive_id.strip():
        raise PortableImportError("portable archive_id must not be empty")
    source_main = sqlite3.connect(f"{Path(source_main_database).resolve().as_uri()}?mode=ro", uri=True)
    source_reference = sqlite3.connect(
        f"{Path(source_reference_database).resolve().as_uri()}?mode=ro", uri=True
    )
    destination_main = sqlite3.connect(destination_main_database)
    destination_main.execute(
        "ATTACH DATABASE ? AS portable_reference",
        (str(Path(destination_reference_database).resolve()),),
    )
    destination_reference = destination_main
    for connection in (source_main, source_reference, destination_main):
        connection.row_factory = sqlite3.Row
    try:
        destination_main.execute("BEGIN IMMEDIATE")
        _ensure_provenance_schema(destination_main)

        source_items = (
            dict(_source_items_override)
            if _source_items_override is not None
            else _source_provenance_items(source_main, source_reference)
        )
        replay = _preflight_provenance(
            destination_main,
            archive_id=normalized_archive_id,
            source_items=source_items,
            archive_inventory_fingerprint=_archive_inventory_fingerprint_override,
        )
        _validate_replay_root_completeness(source_main, replay)
        _validate_replay_relationships(
            source_main, source_reference, destination_main, replay
        )
        _validate_replayed_stable_content(
            source_main, source_reference, destination_main, replay
        )

        def integer_map(item_type: str) -> dict[int, int]:
            return {
                int(source_id): int(destination_id)
                for (kind, source_id), destination_id in replay.items()
                if kind == item_type
            }

        def text_map(item_type: str) -> dict[str, str]:
            return {
                source_id: destination_id
                for (kind, source_id), destination_id in replay.items()
                if kind == item_type
            }

        reference_existing = {
            item_type: integer_map(item_type) if item_type == "reference_value"
            else text_map(item_type)
            for item_type in _REFERENCE_PROVENANCE_TABLES
        }

        value_map, work_map, treatment_map, set_map = _merge_reference_graph(
            source_reference,
            destination_reference,
            destination_schema="portable_reference",
            existing_maps=reference_existing,
        )
        calibration_map = _merge_calibrations(
            source_main, destination_main, integer_map("calibration")
        )
        calibration_asset_map = _merge_calibration_assets(
            source_main, destination_main, calibration_map,
            integer_map("calibration_asset"),
            preserve_managed_asset_paths=_preserve_managed_asset_paths,
        )

        observation_rows, observation_map = _allocate_rows(
            source_main, destination_main, "observations", integer_map("observation")
        )
        image_rows, image_map = _allocate_rows(source_main, destination_main, "images")
        image_map.update(integer_map("image"))
        session_log_rows = _rows(source_main, "session_logs")
        source_session_ids = {
            str(row["session_id"])
            for row in session_log_rows
            if str(row.get("session_id") or "").strip()
        }
        for image_row in image_rows:
            source_session_ids.update(
                _embedded_values(image_row.get("lab_metadata"), "session_id")
            )
        for observation_row in observation_rows:
            source_session_ids.update(
                _embedded_values(observation_row.get("ai_state_json"), "session_id")
            )
        for session_log_row in session_log_rows:
            source_session_ids.update(
                _embedded_values(session_log_row.get("metadata_json"), "session_id")
            )
        existing_session_ids = {
            str(row[0]) for row in destination_main.execute(
                "SELECT DISTINCT session_id FROM session_logs WHERE session_id IS NOT NULL"
            )
        }
        session_id_map = text_map("session")
        session_id_map.update({
            source_id: _next_uuid(existing_session_ids)
            for source_id in sorted(source_session_ids)
            if source_id not in session_id_map
        })
        for row in observation_rows:
            source_id = int(row["id"])
            if ("observation", str(source_id)) in replay:
                continue
            row["id"] = observation_map[source_id]
            for field in (
                "cloud_id", "synced_at", "sync_error_code", "sync_error_message",
                "sync_blocked_reason", "sync_blocked_at", "mosaic_signature", "region_id",
            ):
                if field in row:
                    row[field] = None
            row["sync_status"] = "local"
            row["folder_path"] = None
            row["portable_cloud_identity_pending"] = 1
            if row.get("ai_state_json"):
                row["ai_state_json"] = _rewrite_embedded_identity(
                    row["ai_state_json"],
                    image_id_map=image_map,
                    session_id_map=session_id_map,
                    managed_assets_root=_managed_assets_root,
                )
            _insert_row(destination_main, "observations", row)

        for row in image_rows:
            source_id = int(row["id"])
            if ("image", str(source_id)) in replay:
                continue
            source_observation_id = int(row["observation_id"])
            if source_observation_id not in observation_map:
                raise PortableImportError(f"image {source_id} has no mapped observation")
            row["id"] = image_map[source_id]
            row["observation_id"] = observation_map[source_observation_id]
            calibration_id = row.get("calibration_id")
            if calibration_id is not None:
                if int(calibration_id) not in calibration_map:
                    raise PortableImportError(f"image {source_id} has no mapped calibration")
                row["calibration_id"] = calibration_map[int(calibration_id)]
            row["cloud_id"] = None
            row["synced_at"] = None
            if row.get("lab_metadata"):
                row["lab_metadata"] = _rewrite_embedded_identity(
                    row["lab_metadata"],
                    image_id_map=image_map,
                    session_id_map=session_id_map,
                    managed_assets_root=_managed_assets_root,
                )
            _insert_row(destination_main, "images", row)

        measurement_rows, measurement_map = _allocate_rows(
            source_main, destination_main, "spore_measurements",
            integer_map("measurement"),
        )
        for row in measurement_rows:
            source_id = int(row["id"])
            if ("measurement", str(source_id)) in replay:
                continue
            source_image_id = int(row["image_id"])
            if source_image_id not in image_map:
                raise PortableImportError(f"measurement {source_id} has no mapped image")
            row["id"] = measurement_map[source_id]
            row["image_id"] = image_map[source_image_id]
            row["cloud_id"] = None
            _insert_row(destination_main, "spore_measurements", row)

        annotation_rows, annotation_map = _allocate_rows(
            source_main, destination_main, "spore_annotations",
            integer_map("annotation"),
        )
        for row in annotation_rows:
            source_id = int(row["id"])
            if ("annotation", str(source_id)) in replay:
                continue
            source_image_id = int(row["image_id"])
            if source_image_id not in image_map:
                raise PortableImportError(f"annotation {source_id} has no mapped image")
            row["id"] = annotation_map[source_id]
            row["image_id"] = image_map[source_image_id]
            measurement_id = row.get("measurement_id")
            if measurement_id is not None:
                if int(measurement_id) not in measurement_map:
                    raise PortableImportError(
                        f"annotation {source_id} has no mapped measurement"
                    )
                row["measurement_id"] = measurement_map[int(measurement_id)]
            _insert_row(destination_main, "spore_annotations", row)

        session_log_allocator = _IntegerAllocator(
            destination_main,
            "session_logs",
            {int(row["id"]) for row in session_log_rows},
        )
        session_log_map = integer_map("session_log")
        session_log_map.update({
            source_id: session_log_allocator.allocate()
            for source_id in sorted(int(row["id"]) for row in session_log_rows)
            if source_id not in session_log_map
        })
        for row in session_log_rows:
            source_id = int(row["id"])
            if ("session_log", str(source_id)) in replay:
                continue
            source_observation_id = int(row["observation_id"])
            source_session_id = str(row["session_id"])
            if source_observation_id not in observation_map:
                raise PortableImportError(f"session log {source_id} has no mapped observation")
            row["id"] = session_log_map[source_id]
            row["observation_id"] = observation_map[source_observation_id]
            row["session_id"] = session_id_map[source_session_id]
            if row.get("metadata_json"):
                row["metadata_json"] = _rewrite_embedded_identity(
                    row["metadata_json"],
                    image_id_map=image_map,
                    session_id_map=session_id_map,
                    managed_assets_root=_managed_assets_root,
                )
            _insert_row(destination_main, "session_logs", row)

        existing_use_ids = {
            str(row[0]) for row in destination_main.execute(
                "SELECT id FROM observation_reference_uses"
            )
        }
        reference_use_map: dict[str, str] = text_map("reference_use")
        for row in _rows(source_main, "observation_reference_uses"):
            source_id = str(row["id"])
            if ("reference_use", source_id) in replay:
                continue
            source_observation_id = int(row["observation_id"])
            source_set_id = str(row["reference_measurement_set_id"])
            if source_observation_id not in observation_map or source_set_id not in set_map:
                raise PortableImportError(f"reference use {source_id} has an unresolved dependency")
            destination_id = _next_uuid(existing_use_ids)
            reference_use_map[source_id] = destination_id
            row["id"] = destination_id
            row["observation_id"] = observation_map[source_observation_id]
            row["reference_measurement_set_id"] = set_map[source_set_id]
            _validate_reference_snapshot(
                row,
                destination_reference,
                destination_schema="portable_reference",
            )
            _insert_row(destination_main, "observation_reference_uses", row)

        destination_maps: dict[str, dict[Any, Any]] = {
            "observation": observation_map,
            "image": image_map,
            "measurement": measurement_map,
            "annotation": annotation_map,
            "session_log": session_log_map,
            "calibration": calibration_map,
            "calibration_asset": calibration_asset_map,
            "reference_value": value_map,
            "reference_work": work_map,
            "reference_treatment": treatment_map,
            "reference_measurement_set": set_map,
            "reference_use": reference_use_map,
            "session": session_id_map,
        }
        if destination_main.execute(
            "SELECT 1 FROM portable_import_provenance WHERE archive_id=? "
            "AND source_item_type='archive' AND source_item_id='inventory'",
            (normalized_archive_id,),
        ).fetchone() is None:
            _record_provenance(
                destination_main,
                archive_id=normalized_archive_id,
                item_type="archive",
                source_id="inventory",
                destination_id=normalized_archive_id,
                source_fingerprint=(
                    _archive_inventory_fingerprint_override
                    or _archive_inventory_fingerprint(source_items)
                ),
            )
        for (item_type, source_id), fingerprint in source_items.items():
            if (item_type, source_id) in replay:
                continue
            destination_id = destination_maps[item_type].get(
                int(source_id) if item_type in {
                    "observation", "image", "measurement", "annotation",
                    "session_log", "calibration", "calibration_asset",
                    "reference_value",
                } else source_id
            )
            if destination_id is None:
                raise PortableImportError(
                    f"{item_type} {source_id} has no destination mapping"
                )
            _record_provenance(
                destination_main,
                archive_id=normalized_archive_id,
                item_type=item_type,
                source_id=source_id,
                destination_id=str(destination_id),
                source_fingerprint=fingerprint,
            )

        new_counts: dict[str, int] = {}
        reused_counts: dict[str, int] = {}
        for item_type, _source_id in source_items:
            target = reused_counts if (item_type, _source_id) in replay else new_counts
            target[item_type] = target.get(item_type, 0) + 1
        result = PortableImportResult(
            observation_id_map=observation_map,
            image_id_map=image_map,
            measurement_id_map=measurement_map,
            annotation_id_map=annotation_map,
            session_log_id_map=session_log_map,
            calibration_id_map=calibration_map,
            calibration_asset_id_map=calibration_asset_map,
            reference_value_id_map=value_map,
            reference_work_id_map=work_map,
            reference_treatment_id_map=treatment_map,
            reference_measurement_set_id_map=set_map,
            reference_use_id_map=reference_use_map,
            session_id_map=session_id_map,
            new_item_counts=new_counts,
            reused_item_counts=reused_counts,
        )
        if _before_commit is not None:
            _before_commit(result, destination_main)
        destination_main.commit()
        return result
    except Exception as exc:
        cleanup_error: Exception | None = None
        try:
            if destination_main.in_transaction and _on_rollback is not None:
                _on_rollback()
        except Exception as cleanup_exc:
            cleanup_error = cleanup_exc
        destination_main.rollback()
        if cleanup_error is not None:
            raise PortableImportError(
                f"portable import rollback cleanup failed: {cleanup_error}"
            ) from exc
        if isinstance(exc, PortableImportError):
            raise
        raise PortableImportError(f"portable identity import failed: {exc}") from exc
    finally:
        destination_main.close()
        source_reference.close()
        source_main.close()
