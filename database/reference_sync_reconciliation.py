"""Atomic whole-graph pull reconciliation for normalized references."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from database.reference_library_schema import init_reference_library_schema
from database.reference_library_schema import (
    REFERENCE_MEASUREMENT_CHARACTERS,
    REFERENCE_MEASUREMENT_DATA_KINDS,
    REFERENCE_WORK_TYPES,
)
from database.reference_sync_state import (
    ReferenceCloudSyncStateError,
    canonical_library_payload,
)
from database.schema import get_reference_connection
from database import schema as database_schema


_KINDS = ("work", "treatment", "measurement_set")
_TABLES = {
    "work": "reference_works",
    "treatment": "reference_taxon_treatments",
    "measurement_set": "reference_measurement_sets",
}
_PAYLOAD_COLUMNS = {
    "work": (
        "id", "type", "citation_key", "authors_json", "editors_json", "title",
        "container_title", "year", "edition", "publisher", "place", "volume",
        "issue", "pages", "doi", "isbn", "url", "language", "short_label",
        "citation_override", "revision",
    ),
    "treatment": (
        "id", "reference_work_id", "taxon_id", "name_as_published", "page_from",
        "page_to", "locator_text", "treatment_notes", "revision",
    ),
    "measurement_set": (
        "id", "taxon_treatment_id", "character", "raw_text", "data_kind",
        "length_min", "length_core_min", "length_core_max", "length_max",
        "width_min", "width_core_min", "width_core_max", "width_max", "q_min",
        "q_max", "q_mean", "length_mean", "width_mean", "sample_size",
        "specimen_count", "mount_medium", "stain", "preparation",
        "measurement_method", "notes", "raw_points_json", "revision",
        "supersedes_id",
    ),
}
_IDENTITY_FIELDS = {
    "work": {"id"},
    "treatment": {"id", "reference_work_id"},
    "measurement_set": {"id", "taxon_treatment_id", "supersedes_id"},
}
_JSON_COLUMNS = {"authors_json", "editors_json", "raw_points_json"}


class ReferencePullReconciliationError(ReferenceCloudSyncStateError):
    """A complete remote library feed cannot be safely reconciled."""


class ReferencePullRetryableError(ReferencePullReconciliationError):
    """A coherent complete graph could not be observed or applied yet."""


@dataclass(frozen=True, slots=True)
class StagedReferenceLibraryFeed:
    works: tuple[dict[str, Any], ...]
    treatments: tuple[dict[str, Any], ...]
    measurement_sets: tuple[dict[str, Any], ...]


@dataclass(frozen=True, slots=True)
class ReferencePullApplyResult:
    applied: int = 0
    conflicts: tuple[str, ...] = ()
    blocked: tuple[str, ...] = ()


def _canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _parse_json(value: str | None) -> dict[str, Any] | None:
    if value is None:
        return None
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ReferencePullReconciliationError("stored baseline is not an object")
    return parsed


def _rows(feed: StagedReferenceLibraryFeed, kind: str) -> tuple[dict[str, Any], ...]:
    return {
        "work": feed.works,
        "treatment": feed.treatments,
        "measurement_set": feed.measurement_sets,
    }[kind]


def stage_reference_library_feed(
    cloud_user_id: str,
    works: tuple[dict[str, Any], ...],
    treatments: tuple[dict[str, Any], ...],
    measurement_sets: tuple[dict[str, Any], ...],
) -> StagedReferenceLibraryFeed:
    account_id = str(cloud_user_id or "").strip()
    staged: dict[str, tuple[dict[str, Any], ...]] = {}
    for kind, source_rows in zip(_KINDS, (works, treatments, measurement_sets)):
        seen: set[str] = set()
        normalized = []
        for source in source_rows:
            row = dict(source)
            entity_id = str(row.get("id") or "").strip()
            if not entity_id or entity_id in seen:
                raise ReferencePullReconciliationError(
                    f"duplicate or missing remote {kind} identity"
                )
            seen.add(entity_id)
            if str(row.get("user_id") or "").strip() != account_id:
                raise ReferencePullReconciliationError(
                    f"remote {kind} belongs to another account"
                )
            version = row.get("row_version")
            if isinstance(version, bool) or not isinstance(version, int) or version < 1:
                raise ReferencePullReconciliationError(
                    f"remote {kind} has invalid row_version"
                )
            if not str(row.get("updated_at") or "").strip():
                raise ReferencePullReconciliationError(
                    f"remote {kind} has no updated_at cursor"
                )
            try:
                payload = canonical_library_payload(kind, row)
            except (KeyError, TypeError, ValueError, ReferenceCloudSyncStateError) as exc:
                raise ReferencePullReconciliationError(
                    f"remote {kind} is missing canonical fields"
                ) from exc
            revision = payload.get("revision")
            if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
                raise ReferencePullReconciliationError(
                    f"remote {kind} has invalid domain revision"
                )
            if not str(row.get("created_at") or "").strip():
                raise ReferencePullReconciliationError(
                    f"remote {kind} has no created_at"
                )
            if row.get("deleted_at") is not None and not str(row["deleted_at"]).strip():
                raise ReferencePullReconciliationError(
                    f"remote {kind} has invalid deleted_at"
                )
            if kind == "work":
                if payload["type"] not in REFERENCE_WORK_TYPES:
                    raise ReferencePullReconciliationError("remote work has invalid type")
                if not str(payload["title"] or "").strip() or not str(
                    payload["short_label"] or ""
                ).strip():
                    raise ReferencePullReconciliationError("remote work is incomplete")
                if not isinstance(payload["authors_json"], list) or not isinstance(
                    payload["editors_json"], list
                ):
                    raise ReferencePullReconciliationError(
                        "remote work author fields must be arrays"
                    )
            elif kind == "treatment":
                if not str(payload["reference_work_id"] or "").strip() or not str(
                    payload["name_as_published"] or ""
                ).strip():
                    raise ReferencePullReconciliationError(
                        "remote treatment is incomplete"
                    )
            else:
                if payload["character"] not in REFERENCE_MEASUREMENT_CHARACTERS:
                    raise ReferencePullReconciliationError(
                        "remote measurement set has invalid character"
                    )
                if payload["data_kind"] not in REFERENCE_MEASUREMENT_DATA_KINDS:
                    raise ReferencePullReconciliationError(
                        "remote measurement set has invalid data kind"
                    )
                if payload["raw_points_json"] is not None and not isinstance(
                    payload["raw_points_json"], list
                ):
                    raise ReferencePullReconciliationError(
                        "remote measurement points must be an array"
                    )
            normalized.append({**row, "_payload": payload})
        staged[kind] = tuple(normalized)
    result = StagedReferenceLibraryFeed(
        staged["work"], staged["treatment"], staged["measurement_set"]
    )
    _validate_graph(result)
    return result


def _validate_graph(feed: StagedReferenceLibraryFeed) -> None:
    works = {row["id"]: row for row in feed.works}
    treatments = {row["id"]: row for row in feed.treatments}
    sets = {row["id"]: row for row in feed.measurement_sets}
    for row in feed.treatments:
        parent = works.get(row["reference_work_id"])
        if not row["deleted_at"] and (parent is None or parent["deleted_at"]):
            raise ReferencePullRetryableError("live treatment has no live work")
    for row in feed.measurement_sets:
        parent = treatments.get(row["taxon_treatment_id"])
        if not row["deleted_at"] and (parent is None or parent["deleted_at"]):
            raise ReferencePullRetryableError(
                "live measurement set has no live treatment"
            )
        predecessor_id = row.get("supersedes_id")
        if predecessor_id:
            predecessor = sets.get(predecessor_id)
            if predecessor is None or predecessor_id == row["id"]:
                raise ReferencePullRetryableError(
                    "measurement-set successor has an invalid predecessor"
                )
            if not row["deleted_at"] and predecessor["deleted_at"]:
                raise ReferencePullRetryableError(
                    "live successor has a deleted predecessor"
                )
    live_successors: dict[str, int] = {}
    for row in feed.measurement_sets:
        if not row["deleted_at"] and row.get("supersedes_id"):
            predecessor_id = row["supersedes_id"]
            live_successors[predecessor_id] = live_successors.get(predecessor_id, 0) + 1
            if live_successors[predecessor_id] > 1:
                raise ReferencePullRetryableError("measurement-set successor fork")
    for entity_id in sets:
        seen: set[str] = set()
        current = entity_id
        while current:
            if current in seen:
                raise ReferencePullRetryableError("measurement-set successor cycle")
            seen.add(current)
            current = str(sets[current].get("supersedes_id") or "") if current in sets else ""


def _payload_from_local(connection: sqlite3.Connection, kind: str, entity_id: str):
    row = connection.execute(
        f"SELECT * FROM {_TABLES[kind]} WHERE id=?", (entity_id,)
    ).fetchone()
    if row is None:
        return None
    mapping = dict(row)
    for key in _JSON_COLUMNS:
        if key in mapping and isinstance(mapping[key], str):
            mapping[key] = json.loads(mapping[key])
    mapping["deleted_at"] = None
    return canonical_library_payload(kind, mapping)


def _state_row(connection: sqlite3.Connection, kind: str, entity_id: str):
    return connection.execute(
        "SELECT * FROM reference_cloud_sync_state WHERE entity_type=? AND entity_id=?",
        (kind, entity_id),
    ).fetchone()


def _pending_tombstone(connection: sqlite3.Connection, kind: str, entity_id: str):
    return connection.execute(
        "SELECT * FROM reference_cloud_tombstones WHERE entity_type=? AND entity_id=?",
        (kind, entity_id),
    ).fetchone()


def _remote_tombstone_marker(
    connection: sqlite3.Connection, cloud_user_id: str, kind: str, entity_id: str
):
    return connection.execute(
        "SELECT * FROM reference_cloud_remote_tombstone_markers "
        "WHERE cloud_user_id=? AND entity_type=? AND entity_id=?",
        (cloud_user_id, kind, entity_id),
    ).fetchone()


def _domain_values(kind: str, payload: dict[str, Any]) -> list[Any]:
    values = []
    for column in _PAYLOAD_COLUMNS[kind]:
        value = payload[column]
        if column in _JSON_COLUMNS and value is not None:
            value = json.dumps(value, ensure_ascii=True, separators=(",", ":"))
        values.append(value)
    return values


def _write_domain(
    connection: sqlite3.Connection,
    kind: str,
    payload: dict[str, Any],
    row: dict[str, Any],
    *,
    preserve_updated_at: bool = False,
) -> None:
    columns = list(_PAYLOAD_COLUMNS[kind])
    extras: list[tuple[str, Any]] = []
    if kind == "work":
        extras.append(("owner_id", row["user_id"]))
    extras.extend(
        [
            ("created_at", row.get("created_at") or row["updated_at"]),
            (
                "updated_at",
                None if preserve_updated_at else row["updated_at"],
            ),
        ]
    )
    existing = connection.execute(
        f"SELECT updated_at FROM {_TABLES[kind]} WHERE id=?", (payload["id"],)
    ).fetchone()
    if existing is not None and preserve_updated_at:
        extras[-1] = ("updated_at", existing["updated_at"])
    values = _domain_values(kind, payload) + [value for _, value in extras]
    all_columns = columns + [name for name, _ in extras]
    placeholders = ",".join("?" for _ in all_columns)
    updates = ",".join(f"{name}=excluded.{name}" for name in all_columns if name != "id")
    connection.execute(
        f"INSERT INTO {_TABLES[kind]} ({','.join(all_columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT(id) DO UPDATE SET {updates}",
        values,
    )


def _save_acknowledged_state(
    connection: sqlite3.Connection,
    kind: str,
    entity_id: str,
    cloud_user_id: str,
    remote_payload: dict[str, Any],
    row_version: int,
    status: str,
) -> None:
    connection.execute(
        """
        UPDATE reference_cloud_sync_state
        SET cloud_user_id=?, remote_identity_state='acknowledged',
            cloud_row_version=?, accepted_payload_json=?, sync_status=?,
            conflict_json=NULL, retry_count=0, last_error=NULL,
            last_attempted_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP
        WHERE entity_type=? AND entity_id=?
        """,
        (
            cloud_user_id,
            row_version,
            _canonical_json(remote_payload),
            status,
            kind,
            entity_id,
        ),
    )


def _record_conflict(
    connection: sqlite3.Connection,
    kind: str,
    entity_id: str,
    *,
    reason: str,
    baseline: dict[str, Any] | None,
    local: dict[str, Any] | None,
    remote: dict[str, Any],
    remote_row_version: int,
    overlapping_fields: set[str] | None = None,
) -> None:
    conflict = {
        "reason": reason,
        "baseline": baseline,
        "local": local,
        "remote": remote,
        "remote_row_version": remote_row_version,
    }
    if overlapping_fields is not None:
        conflict["overlapping_fields"] = sorted(overlapping_fields)
    connection.execute(
        """
        UPDATE reference_cloud_sync_state
        SET sync_status='conflict', conflict_json=?,
            last_error=?, last_attempted_at=CURRENT_TIMESTAMP,
            updated_at=CURRENT_TIMESTAMP
        WHERE entity_type=? AND entity_id=?
        """,
        (_canonical_json(conflict), reason, kind, entity_id),
    )


def _upsert_remote_tombstone(
    connection: sqlite3.Connection,
    cloud_user_id: str,
    kind: str,
    row: dict[str, Any],
) -> bool:
    payload = row["_payload"]
    existing = connection.execute(
        "SELECT cloud_row_version, accepted_payload_json, deleted_at "
        "FROM reference_cloud_remote_tombstone_markers "
        "WHERE cloud_user_id=? AND entity_type=? AND entity_id=?",
        (cloud_user_id, kind, row["id"]),
    ).fetchone()
    encoded = _canonical_json(payload)
    changed = existing is None or (
        existing["cloud_row_version"], existing["accepted_payload_json"], existing["deleted_at"]
    ) != (row["row_version"], encoded, row["deleted_at"])
    connection.execute(
        """
        INSERT INTO reference_cloud_remote_tombstone_markers(
            cloud_user_id, entity_type, entity_id, cloud_row_version,
            accepted_payload_json, deleted_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(cloud_user_id, entity_type, entity_id) DO UPDATE SET
            cloud_row_version=excluded.cloud_row_version,
            accepted_payload_json=excluded.accepted_payload_json,
            deleted_at=excluded.deleted_at
        """,
        (cloud_user_id, kind, row["id"], row["row_version"], encoded, row["deleted_at"]),
    )
    return changed


def _reconcile_live(
    connection: sqlite3.Connection,
    cloud_user_id: str,
    kind: str,
    row: dict[str, Any],
) -> tuple[int, str | None]:
    entity_id = row["id"]
    remote = row["_payload"]
    local = _payload_from_local(connection, kind, entity_id)
    state = _state_row(connection, kind, entity_id)
    tombstone = _pending_tombstone(connection, kind, entity_id)
    marker = _remote_tombstone_marker(connection, cloud_user_id, kind, entity_id)
    if marker is not None and row["row_version"] < marker["cloud_row_version"]:
        raise ReferencePullRetryableError("remote row version moved backwards")
    if tombstone is not None:
        expected = tombstone["expected_row_version"]
        if expected is not None and row["row_version"] < expected:
            raise ReferencePullRetryableError("remote row version moved backwards")
        baseline = _parse_json(tombstone["accepted_payload_json"])
        if tombstone["sync_status"] == "conflict":
            return 0, f"{kind}:{entity_id}"
        if tombstone["remote_identity_state"] == "create_outcome_unknown" or baseline == remote:
            connection.execute(
                """
                UPDATE reference_cloud_tombstones
                SET remote_identity_state='acknowledged', expected_row_version=?,
                    accepted_payload_json=?, sync_status='dirty', conflict_json=NULL,
                    retry_count=0, last_error=NULL, updated_at=CURRENT_TIMESTAMP
                WHERE entity_type=? AND entity_id=? AND cloud_user_id=?
                """,
                (row["row_version"], _canonical_json(remote), kind, entity_id, cloud_user_id),
            )
            return 1, None
        conflict = {
            "reason": "remote_change_local_delete",
            "baseline": baseline,
            "remote": remote,
            "remote_row_version": row["row_version"],
        }
        connection.execute(
            "UPDATE reference_cloud_tombstones SET sync_status='conflict', "
            "conflict_json=?, last_error='remote_change_local_delete', "
            "last_attempted_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP "
            "WHERE entity_type=? AND entity_id=? AND cloud_user_id=?",
            (_canonical_json(conflict), kind, entity_id, cloud_user_id),
        )
        return 0, f"{kind}:{entity_id}"
    connection.execute(
        "DELETE FROM reference_cloud_remote_tombstone_markers "
        "WHERE cloud_user_id=? AND entity_type=? AND entity_id=?",
        (cloud_user_id, kind, entity_id),
    )
    if local is None:
        _write_domain(connection, kind, remote, row)
        _save_acknowledged_state(
            connection, kind, entity_id, cloud_user_id, remote, row["row_version"], "clean"
        )
        return 1, None
    if state is None:
        raise ReferencePullReconciliationError("local library row has no sync state")
    if state["cloud_user_id"] not in {None, cloud_user_id}:
        raise ReferencePullReconciliationError("local library row is bound to another account")
    if (
        state["cloud_row_version"] is not None
        and row["row_version"] < state["cloud_row_version"]
    ):
        raise ReferencePullRetryableError("remote row version moved backwards")
    if state["sync_status"] == "conflict":
        return 0, f"{kind}:{entity_id}"
    baseline = _parse_json(state["accepted_payload_json"])
    if state["remote_identity_state"] != "acknowledged" or baseline is None:
        if local == remote:
            _save_acknowledged_state(
                connection, kind, entity_id, cloud_user_id, remote, row["row_version"], "clean"
            )
            return 1, None
        _record_conflict(
            connection, kind, entity_id,
            reason="unacknowledged_identity_divergence", baseline=None,
            local=local, remote=remote, remote_row_version=row["row_version"],
        )
        return 0, f"{kind}:{entity_id}"
    if local == remote:
        changed = state["cloud_row_version"] != row["row_version"] or state["sync_status"] != "clean"
        if not changed and baseline == remote:
            return 0, None
        _save_acknowledged_state(
            connection, kind, entity_id, cloud_user_id, remote, row["row_version"], "clean"
        )
        return int(changed), None

    compare_fields = set(remote) - {"revision", "deleted"}
    local_changes = {key for key in compare_fields if local.get(key) != baseline.get(key)}
    remote_changes = {key for key in compare_fields if remote.get(key) != baseline.get(key)}
    identity_change = remote_changes & _IDENTITY_FIELDS[kind]
    overlap = local_changes & remote_changes
    if identity_change or any(local.get(key) != remote.get(key) for key in overlap):
        _record_conflict(
            connection, kind, entity_id,
            reason="overlapping_remote_change", baseline=baseline, local=local,
            remote=remote, remote_row_version=row["row_version"],
            overlapping_fields=identity_change | {
                key for key in overlap if local.get(key) != remote.get(key)
            },
        )
        return 0, f"{kind}:{entity_id}"
    if not local_changes:
        if int(local.get("revision") or 0) > int(remote.get("revision") or 0):
            _save_acknowledged_state(
                connection, kind, entity_id, cloud_user_id, remote, row["row_version"], "dirty"
            )
            return 1, None
        _write_domain(connection, kind, remote, row)
        _save_acknowledged_state(
            connection, kind, entity_id, cloud_user_id, remote, row["row_version"], "clean"
        )
        return 1, None
    if not remote_changes:
        _save_acknowledged_state(
            connection, kind, entity_id, cloud_user_id, remote, row["row_version"], "dirty"
        )
        return 1, None
    merged = dict(remote)
    for key in local_changes:
        merged[key] = local[key]
    merged["revision"] = max(
        int(baseline.get("revision") or 0),
        int(local.get("revision") or 0),
        int(remote.get("revision") or 0),
    ) + 1
    _write_domain(connection, kind, merged, row, preserve_updated_at=True)
    _save_acknowledged_state(
        connection, kind, entity_id, cloud_user_id, remote, row["row_version"], "dirty"
    )
    return 1, None


def _live_dependency_blocked(
    connection: sqlite3.Connection, kind: str, row: dict[str, Any]
) -> str | None:
    dependencies: list[tuple[str, str]] = []
    if kind == "treatment":
        dependencies.append(("work", row["reference_work_id"]))
    elif kind == "measurement_set":
        dependencies.append(("treatment", row["taxon_treatment_id"]))
        if row.get("supersedes_id"):
            dependencies.append(("measurement_set", row["supersedes_id"]))
    for dependency_kind, dependency_id in dependencies:
        state = _state_row(connection, dependency_kind, dependency_id)
        if state is None:
            return "missing_local_dependency"
        if state["sync_status"] == "conflict":
            return "dependency_conflict"
        if state["remote_identity_state"] != "acknowledged":
            return "dependency_not_acknowledged"
    return None


def _reconcile_tombstone(
    connection: sqlite3.Connection,
    cloud_user_id: str,
    kind: str,
    row: dict[str, Any],
) -> tuple[int, str | None]:
    entity_id = row["id"]
    remote = row["_payload"]
    local = _payload_from_local(connection, kind, entity_id)
    state = _state_row(connection, kind, entity_id)
    pending = _pending_tombstone(connection, kind, entity_id)
    marker = _remote_tombstone_marker(connection, cloud_user_id, kind, entity_id)
    if marker is not None and row["row_version"] < marker["cloud_row_version"]:
        raise ReferencePullRetryableError("remote row version moved backwards")
    if pending is not None:
        expected = pending["expected_row_version"]
        if expected is not None and row["row_version"] < expected:
            raise ReferencePullRetryableError("remote row version moved backwards")
        connection.execute(
            "DELETE FROM reference_cloud_tombstones "
            "WHERE entity_type=? AND entity_id=? AND cloud_user_id=?",
            (kind, entity_id, cloud_user_id),
        )
        changed = _upsert_remote_tombstone(connection, cloud_user_id, kind, row)
        return int(changed or True), None
    if local is None:
        return int(_upsert_remote_tombstone(connection, cloud_user_id, kind, row)), None
    if state is None:
        raise ReferencePullReconciliationError("local library row has no sync state")
    if state["cloud_user_id"] not in {None, cloud_user_id}:
        raise ReferencePullReconciliationError("local library row is bound to another account")
    if (
        state["cloud_row_version"] is not None
        and row["row_version"] < state["cloud_row_version"]
    ):
        raise ReferencePullRetryableError("remote row version moved backwards")
    if state["sync_status"] == "conflict":
        return 0, f"{kind}:{entity_id}"
    baseline = _parse_json(state["accepted_payload_json"])
    if (
        state["remote_identity_state"] == "acknowledged"
        and baseline is not None
        and baseline.get("deleted") is True
    ):
        _save_acknowledged_state(
            connection, kind, entity_id, cloud_user_id, remote, row["row_version"], "dirty"
        )
        return 0, None
    if state["remote_identity_state"] != "acknowledged" or baseline is None or local != baseline:
        _record_conflict(
            connection, kind, entity_id,
            reason="remote_delete_local_change", baseline=baseline, local=local,
            remote=remote, remote_row_version=row["row_version"],
        )
        return 0, f"{kind}:{entity_id}"
    if kind == "measurement_set":
        active_use = connection.execute(
            "SELECT id FROM observation_db.observation_reference_uses "
            "WHERE reference_measurement_set_id=? LIMIT 1",
            (entity_id,),
        ).fetchone()
        if active_use is not None:
            _record_conflict(
                connection, kind, entity_id,
                reason="remote_delete_local_use", baseline=baseline, local=local,
                remote=remote, remote_row_version=row["row_version"],
            )
            return 0, f"{kind}:{entity_id}"
    dependent = None
    if kind == "treatment":
        dependent = connection.execute(
            "SELECT id FROM reference_measurement_sets WHERE taxon_treatment_id=? LIMIT 1",
            (entity_id,),
        ).fetchone()
    elif kind == "work":
        dependent = connection.execute(
            "SELECT id FROM reference_taxon_treatments WHERE reference_work_id=? LIMIT 1",
            (entity_id,),
        ).fetchone()
    if dependent is not None:
        _record_conflict(
            connection, kind, entity_id,
            reason="remote_delete_local_dependency", baseline=baseline, local=local,
            remote=remote, remote_row_version=row["row_version"],
        )
        return 0, f"{kind}:{entity_id}"
    connection.execute(f"DELETE FROM {_TABLES[kind]} WHERE id=?", (entity_id,))
    connection.execute(
        "DELETE FROM reference_cloud_sync_state WHERE entity_type=? AND entity_id=?",
        (kind, entity_id),
    )
    connection.execute(
        "DELETE FROM reference_cloud_tombstones WHERE entity_type=? AND entity_id=?",
        (kind, entity_id),
    )
    _upsert_remote_tombstone(connection, cloud_user_id, kind, row)
    return 1, None


def _set_depth(row: dict[str, Any], by_id: dict[str, dict[str, Any]]) -> int:
    depth = 0
    current = row
    while current.get("supersedes_id"):
        depth += 1
        current = by_id[current["supersedes_id"]]
    return depth


def _advance_cursors(
    connection: sqlite3.Connection,
    cloud_user_id: str,
    feed: StagedReferenceLibraryFeed,
) -> None:
    for kind in _KINDS:
        rows = _rows(feed, kind)
        if not rows:
            continue
        updated_at, entity_id = max(
            (str(row["updated_at"]), str(row["id"])) for row in rows
        )
        connection.execute(
            """
            INSERT INTO reference_cloud_pull_cursors(
                cloud_user_id, entity_type, updated_at, entity_id
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(cloud_user_id, entity_type) DO UPDATE SET
                updated_at=excluded.updated_at, entity_id=excluded.entity_id
            WHERE (excluded.updated_at, excluded.entity_id) >
                  (reference_cloud_pull_cursors.updated_at,
                   reference_cloud_pull_cursors.entity_id)
            """,
            (cloud_user_id, kind, updated_at, entity_id),
        )


def _claim_remote_restore_markers(
    connection: sqlite3.Connection, cloud_user_id: str
) -> None:
    rows = connection.execute(
        """
        SELECT marker.*
        FROM reference_cloud_remote_tombstone_markers AS marker
        JOIN reference_cloud_sync_state AS state
          ON state.entity_type=marker.entity_type
         AND state.entity_id=marker.entity_id
        WHERE marker.cloud_user_id=?
        ORDER BY marker.entity_type, marker.entity_id
        """,
        (cloud_user_id,),
    ).fetchall()
    for row in rows:
        connection.execute(
            """
            UPDATE reference_cloud_sync_state
            SET cloud_user_id=?, remote_identity_state='acknowledged',
                cloud_row_version=?, accepted_payload_json=?, sync_status='dirty',
                conflict_json=NULL, retry_count=0, last_error=NULL,
                last_attempted_at=NULL, updated_at=CURRENT_TIMESTAMP
            WHERE entity_type=? AND entity_id=?
            """,
            (
                cloud_user_id,
                row["cloud_row_version"],
                row["accepted_payload_json"],
                row["entity_type"],
                row["entity_id"],
            ),
        )
        connection.execute(
            "DELETE FROM reference_cloud_remote_tombstone_markers "
            "WHERE cloud_user_id=? AND entity_type=? AND entity_id=?",
            (cloud_user_id, row["entity_type"], row["entity_id"]),
        )


def reconcile_reference_library_feed(
    cloud_user_id: str,
    feed: StagedReferenceLibraryFeed,
) -> ReferencePullApplyResult:
    """Atomically reconcile one validated complete three-table owner feed."""
    connection = get_reference_connection()
    connection.row_factory = sqlite3.Row
    init_reference_library_schema(connection)
    connection.execute(
        "ATTACH DATABASE ? AS observation_db",
        (str(database_schema.get_database_path()),),
    )
    applied = 0
    conflicts: list[str] = []
    blocked_items: list[str] = []
    try:
        connection.execute("BEGIN IMMEDIATE")
        _claim_remote_restore_markers(connection, cloud_user_id)
        live_sets = {row["id"]: row for row in feed.measurement_sets if not row["deleted_at"]}
        for kind in ("work", "treatment"):
            for row in sorted(
                (item for item in _rows(feed, kind) if not item["deleted_at"]),
                key=lambda item: item["id"],
            ):
                blocked = _live_dependency_blocked(connection, kind, row)
                if blocked:
                    blocked_items.append(f"{kind}:{row['id']}:{blocked}")
                    continue
                count, conflict = _reconcile_live(connection, cloud_user_id, kind, row)
                applied += count
                if conflict:
                    conflicts.append(conflict)
        for row in sorted(
            live_sets.values(), key=lambda item: (_set_depth(item, live_sets), item["id"])
        ):
            blocked = _live_dependency_blocked(connection, "measurement_set", row)
            if blocked:
                blocked_items.append(
                    f"measurement_set:{row['id']}:{blocked}"
                )
                continue
            count, conflict = _reconcile_live(
                connection, cloud_user_id, "measurement_set", row
            )
            applied += count
            if conflict:
                conflicts.append(conflict)

        deleted_sets = {row["id"]: row for row in feed.measurement_sets if row["deleted_at"]}
        for row in sorted(
            deleted_sets.values(),
            key=lambda item: (-_set_depth(item, {**live_sets, **deleted_sets}), item["id"]),
        ):
            count, conflict = _reconcile_tombstone(
                connection, cloud_user_id, "measurement_set", row
            )
            applied += count
            if conflict:
                conflicts.append(conflict)
        for kind in ("treatment", "work"):
            for row in sorted(
                (item for item in _rows(feed, kind) if item["deleted_at"]),
                key=lambda item: item["id"],
            ):
                count, conflict = _reconcile_tombstone(connection, cloud_user_id, kind, row)
                applied += count
                if conflict:
                    conflicts.append(conflict)
        if not conflicts and not blocked_items:
            _advance_cursors(connection, cloud_user_id, feed)
        connection.commit()
    except sqlite3.Error as exc:
        connection.rollback()
        raise ReferencePullRetryableError(
            "reference graph application failed"
        ) from exc
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return ReferencePullApplyResult(
        applied,
        tuple(sorted(set(conflicts))),
        tuple(sorted(set(blocked_items))),
    )
