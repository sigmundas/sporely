"""Read-only cloud-media incident inventory and deterministic serializers.

The audit never imports recovery code or calls sync reconciliation. SQLite is
opened with ``mode=ro`` and cloud access is restricted to ``get_read_only`` and
fixed-token media probes.
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import quote

from utils.cloud_sync import (
    CloudReauthRequiredError,
    SporelyCloudClient,
    measurement_qualifies_for_public_spore_anchor,
)


INCIDENT_BOUNDARY = "2026-08-03T00:00:00+00:00"
STORAGE_EXISTS = "exists"
STORAGE_MISSING = "confirmed_missing"
STORAGE_UNAUTHORIZED = "unauthorized"
STORAGE_UNAVAILABLE = "temporarily_unavailable"
STORAGE_UNSUPPORTED = "verification_unsupported"
STORAGE_NOT_CHECKED = "not_checked"
STORAGE_NOT_APPLICABLE = "not_applicable"

BATCH_SIZE = 75
PAGE_SIZE = 200

PRIMARY_STATES = (
    "healthy_cloud_image", "local_only_not_selected", "new_local_pending_upload",
    "legitimate_metadata_only_anchor", "suspicious_metadata_only_anchor",
    "active_cloud_row_missing_storage_path",
    "cloud_row_missing_local_file_available", "cloud_row_missing_local_file_missing",
    "cloud_derivative_missing", "cloud_original_missing",
    "soft_deleted_with_local_tombstone", "soft_deleted_without_local_tombstone",
    "local_tombstone_without_cloud_delete", "cloud_row_without_local_match",
    "possible_duplicate", "possible_match_requires_review",
    "unable_to_verify_storage", "healthy_intentional_deletion",
)

LOCAL_IMAGE_FIELDS = (
    "id", "observation_id", "cloud_id", "image_type", "sort_order", "filepath",
    "original_filepath", "source_role", "file_purpose", "synced_at",
)
CLOUD_IMAGE_FIELDS = (
    "id", "desktop_id", "observation_id", "image_type", "sort_order",
    "storage_path", "original_storage_path", "deleted_at", "purged_at",
    "original_filename", "upload_mode", "upload_variant", "stored_bytes",
    "source_width", "source_height", "stored_width", "stored_height",
    "encoding_format", "encoding_quality", "storage_exif_safe",
)
TOMBSTONE_FIELDS = (
    "local_image_id", "local_observation_id", "deleted_cloud_id",
    "deleted_at", "delete_synced_at", "deleted_storage_path",
)

CSV_FIELDS = (
    "primary_state", "local_observation_id", "cloud_observation_id",
    "local_image_id", "cloud_image_id", "match_method", "possible_match_ids",
    "image_type", "sort_order", "filepath", "filepath_exists", "filepath_readable",
    "filepath_size", "original_filepath", "original_filepath_exists",
    "original_filepath_readable", "original_filepath_size", "source_role",
    "file_purpose", "local_image_synced_at", "signature_key_exists",
    "signature_value_present", "signature_value_empty", "tombstone_created_at",
    "tombstone_synced_at", "deleted_cloud_id", "storage_path", "derivative_status",
    "original_storage_path", "original_status", "deleted_at", "purged_at",
    "original_filename", "upload_mode", "upload_variant", "local_measurement_count",
    "qualifying_local_measurement_count", "cloud_measurement_count",
    "public_spore_anchor_required", "evidence_flags", "flags",
)


class CloudInventoryIncompleteError(RuntimeError):
    pass


@dataclass(frozen=True)
class AuditFilters:
    observation_id: int | None = None
    cloud_observation_id: str | None = None
    names: tuple[str, ...] = ()
    since: str | None = None
    max_observations: int | None = None


@dataclass(frozen=True)
class ReadCollection:
    rows: tuple[dict, ...]
    requested_ids: int
    returned_rows: int
    batches: int
    pages: int
    complete: bool = True


def _text(value: Any) -> str:
    return str(value or "").strip()


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _pick(row: dict | None, fields: tuple[str, ...]) -> dict | None:
    return None if row is None else {field: row.get(field) for field in fields}


def _chunks(values: Iterable[str], size: int = BATCH_SIZE) -> list[list[str]]:
    ordered = sorted({_text(value) for value in values if _text(value)})
    return [ordered[index:index + size] for index in range(0, len(ordered), size)]


def _iso_timestamp(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _during_incident(value: Any) -> bool:
    parsed = _iso_timestamp(value)
    boundary = _iso_timestamp(INCIDENT_BOUNDARY)
    return bool(parsed and boundary and parsed >= boundary)


def validate_since(value: str | None) -> str | None:
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValueError("--since must be an ISO date in YYYY-MM-DD format") from exc


class ReadOnlyCloudAuditReader:
    """Bounded, paginated reader using a fixed, non-refreshing access token."""

    def __init__(
        self,
        client: SporelyCloudClient,
        *,
        batch_size: int = BATCH_SIZE,
        page_size: int = PAGE_SIZE,
        storage_timeout: int = 15,
    ):
        self._client = client
        self.user_id = _text(client.user_id)
        self.batch_size = max(1, min(int(batch_size), 100))
        self.page_size = max(1, int(page_size))
        self.storage_timeout = max(1, int(storage_timeout))

    @staticmethod
    def _encoded_ids(values: list[str]) -> str:
        return ",".join(quote(value, safe="-") for value in values)

    def _collect(
        self,
        table: str,
        filter_column: str,
        ids: list[str],
        *,
        select: str,
        stable_key: Callable[[dict], str],
    ) -> ReadCollection:
        batches = _chunks(ids, self.batch_size)
        rows_by_id: dict[str, dict] = {}
        page_count = 0
        for batch in batches:
            offset = 0
            while True:
                path = (
                    f"{table}?{filter_column}=in.({self._encoded_ids(batch)})"
                    f"&user_id=eq.{quote(self.user_id, safe='-')}&select={select}"
                    f"&order=id.asc&limit={self.page_size}&offset={offset}"
                )
                try:
                    page = self._client.get_read_only(path)
                except CloudReauthRequiredError:
                    raise
                except Exception as exc:
                    raise CloudInventoryIncompleteError(
                        f"Cloud inventory fetch failed for {table} batch {len(batch)} offset {offset}: {exc}"
                    ) from exc
                if not isinstance(page, list):
                    raise CloudInventoryIncompleteError(f"Cloud inventory returned a non-list page for {table}")
                page_count += 1
                for row in page:
                    row_dict = dict(row or {})
                    key = stable_key(row_dict)
                    if key:
                        rows_by_id.setdefault(key, row_dict)
                if len(page) < self.page_size:
                    break
                offset += self.page_size
        rows = tuple(rows_by_id[key] for key in sorted(rows_by_id))
        return ReadCollection(rows, len(set(ids)), len(rows), len(batches), page_count, True)

    def fetch_observations(self, cloud_ids: list[str]) -> ReadCollection:
        return self._collect(
            "observations", "id", cloud_ids, select="*",
            stable_key=lambda row: _text(row.get("id")),
        )

    def fetch_images(self, observation_ids: list[str]) -> ReadCollection:
        return self._collect(
            "observation_images", "observation_id", observation_ids, select="*",
            stable_key=lambda row: _text(row.get("id")),
        )

    def fetch_measurements(self, image_ids: list[str]) -> ReadCollection:
        return self._collect(
            "spore_measurements", "image_id", image_ids, select="id,image_id",
            stable_key=lambda row: _text(row.get("id")),
        )

    def check_storage(self, storage_path: str) -> str:
        key = _text(storage_path)
        if not key:
            return STORAGE_NOT_APPLICABLE
        try:
            status = self._client._get_media_worker().probe_object_status(
                key, timeout=self.storage_timeout,
            )
        except Exception as exc:
            text = _text(exc).lower()
            if any(term in text for term in ("401", "403", "unauthor", "forbidden")):
                return STORAGE_UNAUTHORIZED
            if any(term in text for term in ("timeout", "tempor", "unavailable")):
                return STORAGE_UNAVAILABLE
            return STORAGE_UNSUPPORTED
        if status in {200, 206}:
            return STORAGE_EXISTS
        if status in {404, 410}:
            return STORAGE_MISSING
        if status in {401, 403}:
            return STORAGE_UNAUTHORIZED
        if status in {408, 425, 429, 500, 502, 503, 504}:
            return STORAGE_UNAVAILABLE
        return STORAGE_UNSUPPORTED


def _required_rows(conn: sqlite3.Connection, table: str, sql: str, params: tuple = ()) -> list[dict]:
    try:
        return [dict(row) for row in conn.execute(sql, params).fetchall()]
    except sqlite3.OperationalError as exc:
        if "no such table" in _text(exc).lower():
            raise RuntimeError(f"Required local table is unavailable: {table}") from exc
        raise


def _optional_rows(
    conn: sqlite3.Connection,
    table: str,
    sql: str,
    params: tuple,
    unavailable: list[str],
) -> list[dict]:
    try:
        return [dict(row) for row in conn.execute(sql, params).fetchall()]
    except sqlite3.OperationalError as exc:
        if "no such table" in _text(exc).lower():
            unavailable.append(table)
            return []
        raise


def _name_blob(row: dict) -> str:
    return " ".join(_text(row.get(key)) for key in (
        "genus", "species", "common_name", "species_guess", "name",
    )).casefold()


def _date_text(row: dict) -> str:
    for key in ("date", "observation_date", "observed_at", "created_at"):
        if _text(row.get(key)):
            return _text(row.get(key))
    return ""


def _matches_filters(row: dict, filters: AuditFilters) -> bool:
    if filters.observation_id and _int(row.get("id")) != filters.observation_id:
        return False
    if filters.cloud_observation_id and _text(row.get("cloud_id")) != filters.cloud_observation_id:
        return False
    if filters.names and not any(name.casefold() in _name_blob(row) for name in filters.names):
        return False
    if filters.since and _date_text(row)[:10] < filters.since:
        return False
    return bool(_text(row.get("cloud_id")))


def _placeholders(values: list[Any]) -> str:
    return ",".join("?" for _ in values)


def load_local_inventory(db_path: Path, filters: AuditFilters) -> dict[str, Any]:
    """Read only selected observations and their relevant child evidence."""
    uri = f"file:{quote(str(Path(db_path).resolve()))}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    unavailable: list[str] = []
    try:
        observations = [
            row for row in _required_rows(conn, "observations", "SELECT * FROM observations")
            if _matches_filters(row, filters)
        ]
        observations.sort(key=lambda row: _int(row.get("id")))
        if filters.max_observations is not None:
            observations = observations[:max(0, filters.max_observations)]
        observation_ids = [_int(row.get("id")) for row in observations]
        if not observation_ids:
            return {
                "observations": [], "images": [], "measurements": [], "tombstones": [],
                "settings": [], "optional_tables_unavailable": unavailable,
                "filters": asdict(filters),
            }
        obs_marks = _placeholders(observation_ids)
        images = _required_rows(
            conn, "images", f"SELECT * FROM images WHERE observation_id IN ({obs_marks})", tuple(observation_ids),
        )
        image_ids = [_int(row.get("id")) for row in images if _int(row.get("id"))]
        cloud_ids = [_text(row.get("cloud_id")) for row in images if _text(row.get("cloud_id"))]
        if image_ids:
            image_marks = _placeholders(image_ids)
            measurements = _optional_rows(
                conn, "spore_measurements",
                f"SELECT * FROM spore_measurements WHERE image_id IN ({image_marks})",
                tuple(image_ids), unavailable,
            )
            tombstone_sql = f"SELECT * FROM image_tombstones WHERE local_image_id IN ({image_marks})"
            tombstone_params: tuple[Any, ...] = tuple(image_ids)
            if cloud_ids:
                cloud_marks = _placeholders(cloud_ids)
                tombstone_sql += f" OR deleted_cloud_id IN ({cloud_marks})"
                tombstone_params += tuple(cloud_ids)
            tombstones = _optional_rows(
                conn, "image_tombstones", tombstone_sql, tombstone_params, unavailable,
            )
        else:
            measurements, tombstones = [], []
        setting_clauses = []
        setting_params: list[Any] = []
        for obs_id in observation_ids:
            setting_clauses.extend(["key LIKE ?", "key = ?", "key = ?"])
            setting_params.extend([
                f"sporely_cloud_image_file_sig_{obs_id}_%",
                f"sporely_cloud_local_media_sig_obs_{obs_id}",
                f"artsobs_publish_excluded_image_ids_{obs_id}",
            ])
        settings = _optional_rows(
            conn, "settings",
            f"SELECT key, value FROM settings WHERE {' OR '.join(setting_clauses)}",
            tuple(setting_params), unavailable,
        )
    finally:
        conn.close()
    return {
        "observations": observations, "images": images, "measurements": measurements,
        "tombstones": tombstones, "settings": settings,
        "optional_tables_unavailable": sorted(set(unavailable)),
        "filters": asdict(filters),
    }


def _path_state(value: Any) -> dict[str, Any]:
    text = _text(value)
    result = {"path": text or None, "exists": False, "readable": False, "size": None}
    if not text:
        return result
    path = Path(text)
    try:
        result["exists"] = path.is_file()
        result["readable"] = bool(result["exists"] and os.access(path, os.R_OK))
        if result["exists"]:
            result["size"] = path.stat().st_size
    except OSError:
        pass
    return result


def _settings_evidence(settings: list[dict]) -> dict[str, Any]:
    result = {
        "signature_key_image_ids": set(), "signature_present_image_ids": set(),
        "signature_empty_image_ids": set(), "observation_signature_ids": set(),
        "publication_excluded_image_ids": set(),
    }
    for row in settings:
        key, value = _text(row.get("key")), _text(row.get("value"))
        match = re.fullmatch(r"sporely_cloud_image_file_sig_(\d+)_(\d+)", key)
        if match:
            image_id = _int(match.group(2))
            result["signature_key_image_ids"].add(image_id)
            result["signature_present_image_ids" if value else "signature_empty_image_ids"].add(image_id)
            continue
        match = re.fullmatch(r"sporely_cloud_local_media_sig_obs_(\d+)", key)
        if match:
            result["observation_signature_ids"].add(_int(match.group(1)))
            continue
        if key.startswith("artsobs_publish_excluded_image_ids_"):
            try:
                values = json.loads(value or "[]")
            except (TypeError, ValueError, json.JSONDecodeError):
                values = []
            if isinstance(values, list):
                result["publication_excluded_image_ids"].update(_int(item) for item in values if _int(item))
    return result


def _tombstone_maps(rows: list[dict]) -> tuple[dict[int, dict], dict[str, dict]]:
    by_local, by_cloud = {}, {}
    for row in rows:
        if _int(row.get("local_image_id")):
            by_local[_int(row.get("local_image_id"))] = row
        if _text(row.get("deleted_cloud_id")):
            by_cloud[_text(row.get("deleted_cloud_id"))] = row
    return by_local, by_cloud


def _fallback_candidates(local: dict, cloud_rows: list[dict]) -> list[dict]:
    filename = Path(_text(local.get("filepath") or local.get("original_filepath"))).name.casefold()
    return sorted([
        row for row in cloud_rows
        if _text(row.get("image_type")).lower() == _text(local.get("image_type")).lower()
        and _int(row.get("sort_order")) == _int(local.get("sort_order"))
        and filename and filename == _text(row.get("original_filename")).casefold()
    ], key=lambda row: _text(row.get("id")))


def _storage_status(reader: ReadOnlyCloudAuditReader, path: str, enabled: bool) -> str:
    if not path:
        return STORAGE_NOT_APPLICABLE
    if not enabled:
        return STORAGE_NOT_CHECKED
    status = reader.check_storage(path)
    return status if status in {
        STORAGE_EXISTS, STORAGE_MISSING, STORAGE_UNAUTHORIZED,
        STORAGE_UNAVAILABLE, STORAGE_UNSUPPORTED,
    } else STORAGE_UNSUPPORTED


def _qualifying_counts(measurements: list[dict]) -> tuple[Counter, Counter]:
    total = Counter(_int(row.get("image_id")) for row in measurements if _int(row.get("image_id")))
    qualifying = Counter(
        _int(row.get("image_id")) for row in measurements
        if _int(row.get("image_id")) and measurement_qualifies_for_public_spore_anchor(row)
    )
    return total, qualifying


def _public_anchor_required(local: dict, observation: dict, qualifying_count: int) -> bool:
    return bool(
        str(local.get("image_type") or "") == "microscope"
        and str(observation.get("spore_data_visibility") or "public").lower() == "public"
        and qualifying_count > 0
    )


def _classify(record: dict) -> str:
    local, cloud, tombstone = record.get("local"), record.get("cloud"), record.get("tombstone")
    if record.get("duplicate_cloud_ids") or len(record.get("possible_match_ids") or []) > 1:
        return "possible_duplicate"
    if cloud and _text(cloud.get("deleted_at")):
        return "soft_deleted_with_local_tombstone" if tombstone else "soft_deleted_without_local_tombstone"
    if tombstone and cloud:
        return "local_tombstone_without_cloud_delete"
    if local and not cloud:
        if tombstone:
            return "healthy_intentional_deletion" if record.get("tombstone_synced_at") else "local_tombstone_without_cloud_delete"
        if record.get("prior_cloud_evidence"):
            return "cloud_row_missing_local_file_available" if record["local_source_available"] else "cloud_row_missing_local_file_missing"
        return "new_local_pending_upload" if record["local_source_available"] else "local_only_not_selected"
    if cloud and not local:
        if record.get("possible_match_ids"):
            return "possible_match_requires_review"
        return "cloud_row_without_local_match"
    if not cloud:
        return "local_only_not_selected"
    if not _text(cloud.get("storage_path")):
        if record.get("metadata_only_suspicious"):
            return "suspicious_metadata_only_anchor"
        if record.get("public_spore_anchor_required") and _text(cloud.get("image_type")).lower() == "microscope":
            return "legitimate_metadata_only_anchor"
        return "active_cloud_row_missing_storage_path"
    if record["derivative_status"] == STORAGE_MISSING:
        return "cloud_derivative_missing"
    if record["derivative_status"] in {STORAGE_UNAUTHORIZED, STORAGE_UNAVAILABLE, STORAGE_UNSUPPORTED}:
        return "unable_to_verify_storage"
    if record["original_status"] == STORAGE_MISSING:
        return "cloud_original_missing"
    return "healthy_cloud_image"


def _collection_meta(collection: ReadCollection) -> dict[str, Any]:
    return {key: value for key, value in asdict(collection).items() if key != "rows"}


def build_audit_report(
    local_inventory: dict[str, Any],
    reader: ReadOnlyCloudAuditReader,
    *,
    check_storage: bool = True,
    generated_at: str | None = None,
) -> dict[str, Any]:
    observations = list(local_inventory.get("observations") or [])
    local_images = list(local_inventory.get("images") or [])
    cloud_obs_ids = [_text(row.get("cloud_id")) for row in observations if _text(row.get("cloud_id"))]
    obs_collection = reader.fetch_observations(cloud_obs_ids)
    image_collection = reader.fetch_images(cloud_obs_ids)
    measurement_collection = reader.fetch_measurements([
        _text(row.get("id")) for row in image_collection.rows if _text(row.get("id"))
    ])
    collections = {
        "observations": obs_collection, "observation_images": image_collection,
        "spore_measurements": measurement_collection,
    }
    if not all(collection.complete for collection in collections.values()):
        raise CloudInventoryIncompleteError("Cloud inventory completeness was not proven; classification aborted")

    cloud_observations = list(obs_collection.rows)
    cloud_images = list(image_collection.rows)
    cloud_measurement_counts = Counter(_text(row.get("image_id")) for row in measurement_collection.rows)
    cloud_obs_by_id = {_text(row.get("id")): row for row in cloud_observations}
    cloud_by_obs: dict[str, list[dict]] = defaultdict(list)
    for row in cloud_images:
        cloud_by_obs[_text(row.get("observation_id"))].append(row)
    evidence = _settings_evidence(local_inventory.get("settings") or [])
    total_counts, qualifying_counts = _qualifying_counts(local_inventory.get("measurements") or [])
    tomb_by_local, tomb_by_cloud = _tombstone_maps(local_inventory.get("tombstones") or [])
    results: list[dict] = []
    derivative_totals = Counter()
    original_totals = Counter()

    for observation in observations:
        obs_id, cloud_obs_id = _int(observation.get("id")), _text(observation.get("cloud_id"))
        obs_cloud_rows = sorted(cloud_by_obs.get(cloud_obs_id, []), key=lambda row: _text(row.get("id")))
        by_cloud_id = {_text(row.get("id")): row for row in obs_cloud_rows}
        by_desktop: dict[int, list[dict]] = defaultdict(list)
        by_shape: dict[tuple[str, int], list[dict]] = defaultdict(list)
        for row in obs_cloud_rows:
            if _int(row.get("desktop_id")):
                by_desktop[_int(row.get("desktop_id"))].append(row)
            by_shape[(_text(row.get("image_type")).lower(), _int(row.get("sort_order")))].append(row)
        used: set[str] = set()
        obs_local = sorted(
            [row for row in local_images if _int(row.get("observation_id")) == obs_id],
            key=lambda row: (_int(row.get("sort_order")), _int(row.get("id"))),
        )
        for local in obs_local:
            local_id, linked_id = _int(local.get("id")), _text(local.get("cloud_id"))
            linked_row = by_cloud_id.get(linked_id) if linked_id else None
            desktop_rows = by_desktop.get(local_id, [])
            duplicate_ids = sorted({
                _text(row.get("id")) for row in desktop_rows
                if _text(row.get("id")) and (len(desktop_rows) > 1 or (linked_row and row is not linked_row))
            })
            cloud, match_method = linked_row, "cloud_id" if linked_row else None
            if cloud is None and len(desktop_rows) == 1:
                cloud, match_method = desktop_rows[0], "desktop_id"
            fallback_rows = [] if cloud else _fallback_candidates(local, obs_cloud_rows)
            possible_ids = [_text(row.get("id")) for row in fallback_rows]
            if cloud:
                used.add(_text(cloud.get("id")))
            filepath, original_filepath = _path_state(local.get("filepath")), _path_state(local.get("original_filepath"))
            source_available = filepath["exists"] or original_filepath["exists"]
            tombstone = tomb_by_local.get(local_id) or tomb_by_cloud.get(linked_id)
            evidence_flags = set()
            if linked_id:
                evidence_flags.add("previous_cloud_id")
            if local_id in evidence["signature_present_image_ids"]:
                evidence_flags.add("nonempty_media_signature")
            if local_id in evidence["signature_empty_image_ids"]:
                evidence_flags.add("empty_media_signature_key")
            if obs_id in evidence["observation_signature_ids"]:
                evidence_flags.add("observation_level_signature")
            if local_id in evidence["publication_excluded_image_ids"]:
                evidence_flags.add("publication_excluded")
            for timestamp in (local.get("synced_at"), observation.get("synced_at")):
                if _during_incident(timestamp):
                    evidence_flags.add("synced_during_incident_window")
            if tombstone and _during_incident(tombstone.get("deleted_at")):
                evidence_flags.add("tombstone_created_during_incident_window")
            source_app_version = _text(
                observation.get("source_app_version") or observation.get("app_version")
            )
            if source_app_version:
                evidence_flags.add("source_app_version_recorded")
            qualifying = qualifying_counts.get(local_id, 0)
            anchor_required = _public_anchor_required(local, observation, qualifying)
            positive_byte_evidence = bool({
                "nonempty_media_signature", "synced_during_incident_window",
                "tombstone_created_during_incident_window",
            } & evidence_flags)
            metadata_suspicious = bool(
                cloud and not _text(cloud.get("deleted_at")) and not _text(cloud.get("storage_path"))
                and source_available and positive_byte_evidence
            )
            derivative = _storage_status(reader, _text((cloud or {}).get("storage_path")), check_storage)
            original = _storage_status(reader, _text((cloud or {}).get("original_storage_path")), check_storage)
            derivative_totals.update([derivative])
            original_totals.update([original])
            record = {
                "observation": {
                    "local_observation_id": obs_id, "cloud_observation_id": cloud_obs_id,
                    "date": _date_text(observation) or None,
                    "genus": _text(observation.get("genus")) or None,
                    "species": _text(observation.get("species")) or None,
                    "common_name": _text(observation.get("common_name")) or None,
                    "species_guess": _text(observation.get("species_guess")) or None,
                    "sync_status": _text(observation.get("sync_status")) or None,
                    "synced_at": _text(observation.get("synced_at")) or None,
                    "visibility": _text(observation.get("visibility") or observation.get("observation_visibility")) or None,
                    "is_draft": bool(observation.get("is_draft") or observation.get("draft")),
                    "spore_data_visibility": _text(observation.get("spore_data_visibility") or "public"),
                    "source_app_version": source_app_version or None,
                    "cloud_row_found": cloud_obs_id in cloud_obs_by_id,
                },
                "local": _pick(local, LOCAL_IMAGE_FIELDS), "cloud": _pick(cloud, CLOUD_IMAGE_FIELDS),
                "local_image_id": local_id, "cloud_image_id": _text((cloud or {}).get("id")) or None,
                "match_method": match_method, "possible_match_ids": possible_ids,
                "duplicate_cloud_ids": duplicate_ids,
                "filepath": filepath, "original_filepath": original_filepath,
                "local_source_available": source_available,
                "prior_cloud_evidence": bool(linked_id or _text(local.get("synced_at")) or evidence_flags),
                "signature_key_exists": local_id in evidence["signature_key_image_ids"],
                "signature_value_present": local_id in evidence["signature_present_image_ids"],
                "signature_value_empty": local_id in evidence["signature_empty_image_ids"],
                "tombstone": _pick(tombstone, TOMBSTONE_FIELDS),
                "has_local_tombstone": bool(tombstone),
                "tombstone_created_at": _text((tombstone or {}).get("deleted_at")) or None,
                "tombstone_synced_at": _text((tombstone or {}).get("delete_synced_at")) or None,
                "deleted_cloud_id": _text((tombstone or {}).get("deleted_cloud_id")) or None,
                "local_measurement_count": total_counts.get(local_id, 0),
                "qualifying_local_measurement_count": qualifying,
                "cloud_measurement_count": cloud_measurement_counts.get(_text((cloud or {}).get("id")), 0),
                "public_spore_anchor_required": anchor_required,
                "metadata_only_suspicious": metadata_suspicious,
                "derivative_status": derivative, "original_status": original,
                "evidence_flags": sorted(evidence_flags), "flags": [],
            }
            record["primary_state"] = _classify(record)
            record["flags"] = sorted({
                flag for flag, enabled in {
                    "local_source_missing": not source_available,
                    "cloud_derivative_missing": derivative == STORAGE_MISSING,
                    "cloud_original_missing": original == STORAGE_MISSING,
                    "cloud_derivative_unverified": derivative in {STORAGE_UNAUTHORIZED, STORAGE_UNAVAILABLE, STORAGE_UNSUPPORTED},
                    "cloud_original_unverified": original in {STORAGE_UNAUTHORIZED, STORAGE_UNAVAILABLE, STORAGE_UNSUPPORTED},
                    "multiple_fallback_candidates": len(possible_ids) > 1,
                    "duplicate_desktop_id": bool(duplicate_ids),
                    "duplicate_observation_type_order": len(by_shape[(_text(local.get("image_type")).lower(), _int(local.get("sort_order")))]) > 1,
                }.items() if enabled
            })
            results.append(record)

        for cloud in obs_cloud_rows:
            cloud_id = _text(cloud.get("id"))
            if cloud_id in used:
                continue
            fallback_locals = [local for local in obs_local if cloud in _fallback_candidates(local, [cloud])]
            derivative = _storage_status(reader, _text(cloud.get("storage_path")), check_storage)
            original = _storage_status(reader, _text(cloud.get("original_storage_path")), check_storage)
            derivative_totals.update([derivative])
            original_totals.update([original])
            duplicate_ids = sorted({
                _text(row.get("id")) for row in by_desktop.get(_int(cloud.get("desktop_id")), [])
                if _text(row.get("id")) != cloud_id
            }) if _int(cloud.get("desktop_id")) else []
            shape_duplicates = [
                row for row in by_shape[(_text(cloud.get("image_type")).lower(), _int(cloud.get("sort_order")))]
                if _text(row.get("id")) != cloud_id
            ]
            if shape_duplicates:
                duplicate_ids = sorted(set(duplicate_ids) | {_text(row.get("id")) for row in shape_duplicates})
            tombstone = tomb_by_cloud.get(cloud_id)
            record = {
                "observation": {"local_observation_id": obs_id, "cloud_observation_id": cloud_obs_id},
                "local": None, "cloud": _pick(cloud, CLOUD_IMAGE_FIELDS),
                "local_image_id": None, "cloud_image_id": cloud_id, "match_method": None,
                "possible_match_ids": sorted(_int(row.get("id")) for row in fallback_locals),
                "duplicate_cloud_ids": duplicate_ids,
                "filepath": _path_state(None), "original_filepath": _path_state(None),
                "local_source_available": False, "prior_cloud_evidence": False,
                "signature_key_exists": False, "signature_value_present": False, "signature_value_empty": False,
                "tombstone": _pick(tombstone, TOMBSTONE_FIELDS), "has_local_tombstone": bool(tombstone),
                "tombstone_created_at": _text((tombstone or {}).get("deleted_at")) or None,
                "tombstone_synced_at": _text((tombstone or {}).get("delete_synced_at")) or None,
                "deleted_cloud_id": cloud_id if tombstone else None,
                "local_measurement_count": 0, "qualifying_local_measurement_count": 0,
                "cloud_measurement_count": cloud_measurement_counts.get(cloud_id, 0),
                "public_spore_anchor_required": False, "metadata_only_suspicious": False,
                "derivative_status": derivative, "original_status": original,
                "evidence_flags": [], "flags": [],
            }
            record["primary_state"] = _classify(record)
            record["flags"] = sorted({
                flag for flag, enabled in {
                    "cloud_derivative_missing": derivative == STORAGE_MISSING,
                    "cloud_original_missing": original == STORAGE_MISSING,
                    "cloud_derivative_unverified": derivative in {STORAGE_UNAUTHORIZED, STORAGE_UNAVAILABLE, STORAGE_UNSUPPORTED},
                    "cloud_original_unverified": original in {STORAGE_UNAUTHORIZED, STORAGE_UNAVAILABLE, STORAGE_UNSUPPORTED},
                    "duplicate_cloud_row": bool(duplicate_ids),
                    "multiple_fallback_candidates": len(fallback_locals) > 1,
                }.items() if enabled
            })
            results.append(record)

    results.sort(key=lambda row: (
        _int(row.get("observation", {}).get("local_observation_id")),
        _int(row.get("local_image_id")), _text(row.get("cloud_image_id")),
    ))
    return {
        "schema_version": 2, "read_only": True,
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
        "filters": local_inventory.get("filters") or {},
        "storage_checks_enabled": bool(check_storage),
        "storage_verification_skipped": not check_storage,
        "cloud_inventory_complete": True,
        "cloud_inventory": {name: _collection_meta(collection) for name, collection in collections.items()},
        "optional_local_tables_unavailable": local_inventory.get("optional_tables_unavailable") or [],
        "observations_audited": len(observations), "images_audited": len(results),
        "counts_by_primary_state": dict(sorted(Counter(row["primary_state"] for row in results).items())),
        "derivative_status_totals": dict(sorted(derivative_totals.items())),
        "original_status_totals": dict(sorted(original_totals.items())),
        "images": results,
        "notes": [
            "Fallback matches are diagnostic suggestions and never authoritative.",
            "delete_cloud_observation() requires a separate deletion-safety follow-up.",
        ],
    }


def report_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _csv_row(record: dict[str, Any]) -> dict[str, Any]:
    local, cloud, observation = record.get("local") or {}, record.get("cloud") or {}, record.get("observation") or {}
    filepath, original = record.get("filepath") or {}, record.get("original_filepath") or {}
    return {
        "primary_state": record.get("primary_state"),
        "local_observation_id": observation.get("local_observation_id"),
        "cloud_observation_id": observation.get("cloud_observation_id"),
        "local_image_id": record.get("local_image_id"), "cloud_image_id": record.get("cloud_image_id"),
        "match_method": record.get("match_method"), "possible_match_ids": ";".join(map(str, record.get("possible_match_ids") or [])),
        "image_type": local.get("image_type") or cloud.get("image_type"),
        "sort_order": local.get("sort_order") if local else cloud.get("sort_order"),
        "filepath": filepath.get("path"), "filepath_exists": filepath.get("exists"),
        "filepath_readable": filepath.get("readable"), "filepath_size": filepath.get("size"),
        "original_filepath": original.get("path"), "original_filepath_exists": original.get("exists"),
        "original_filepath_readable": original.get("readable"), "original_filepath_size": original.get("size"),
        "source_role": local.get("source_role"), "file_purpose": local.get("file_purpose"),
        "local_image_synced_at": local.get("synced_at"),
        "signature_key_exists": record.get("signature_key_exists"),
        "signature_value_present": record.get("signature_value_present"),
        "signature_value_empty": record.get("signature_value_empty"),
        "tombstone_created_at": record.get("tombstone_created_at"),
        "tombstone_synced_at": record.get("tombstone_synced_at"), "deleted_cloud_id": record.get("deleted_cloud_id"),
        "storage_path": cloud.get("storage_path"), "derivative_status": record.get("derivative_status"),
        "original_storage_path": cloud.get("original_storage_path"), "original_status": record.get("original_status"),
        "deleted_at": cloud.get("deleted_at"), "purged_at": cloud.get("purged_at"),
        "original_filename": cloud.get("original_filename"), "upload_mode": cloud.get("upload_mode"),
        "upload_variant": cloud.get("upload_variant"), "local_measurement_count": record.get("local_measurement_count"),
        "qualifying_local_measurement_count": record.get("qualifying_local_measurement_count"),
        "cloud_measurement_count": record.get("cloud_measurement_count"),
        "public_spore_anchor_required": record.get("public_spore_anchor_required"),
        "evidence_flags": ";".join(record.get("evidence_flags") or []),
        "flags": ";".join(record.get("flags") or []),
    }


def report_csv(report: dict[str, Any]) -> str:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=CSV_FIELDS, lineterminator="\n")
    writer.writeheader()
    for record in report.get("images") or []:
        writer.writerow(_csv_row(record))
    return output.getvalue()


def terminal_summary(report: dict[str, Any]) -> str:
    lines = [
        "READ-ONLY cloud-media incident audit",
        f"Observations audited: {report.get('observations_audited', 0):,}",
        f"Images audited: {report.get('images_audited', 0):,}",
        f"Cloud inventory complete: {'yes' if report.get('cloud_inventory_complete') else 'no'}",
        f"Storage checks: {'enabled' if report.get('storage_checks_enabled') else 'skipped'}", "",
    ]
    lines.extend(
        f"{state.replace('_', ' ').title()}: {count:,}"
        for state, count in sorted((report.get("counts_by_primary_state") or {}).items())
    )
    missing_derivatives = (report.get("derivative_status_totals") or {}).get(STORAGE_MISSING, 0)
    missing_originals = (report.get("original_status_totals") or {}).get(STORAGE_MISSING, 0)
    unverified_originals = sum(
        (report.get("original_status_totals") or {}).get(status, 0)
        for status in (STORAGE_UNAUTHORIZED, STORAGE_UNAVAILABLE, STORAGE_UNSUPPORTED)
    )
    lines.extend([
        "", f"Confirmed missing derivative files: {missing_derivatives:,}",
        f"Confirmed missing original files: {missing_originals:,}",
        f"Unverified original files: {unverified_originals:,}",
    ])
    return "\n".join(lines)
