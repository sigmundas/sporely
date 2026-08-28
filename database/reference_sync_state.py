"""Dormant local transport state for normalized reference cloud sync.

This module stores device/account-specific sync metadata separately from the
portable reference domain rows. Stage 4c mutation owners use its
connection-scoped helpers, but this module performs no network operations.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from database.reference_library_schema import (
    init_observation_reference_uses_schema,
    init_reference_library_schema,
)
from database.schema import get_connection, get_reference_connection


LibraryEntityType = Literal["work", "treatment", "measurement_set"]
RemoteIdentityState = Literal[
    "never_attempted", "create_outcome_unknown", "acknowledged"
]
SyncStatus = Literal["dirty", "clean", "retry", "conflict"]

_LIBRARY_ENTITY_TYPES = frozenset({"work", "treatment", "measurement_set"})
_REMOTE_IDENTITY_STATES = frozenset(
    {"never_attempted", "create_outcome_unknown", "acknowledged"}
)
_SYNC_STATUSES = frozenset({"dirty", "clean", "retry", "conflict"})

_LIBRARY_PAYLOAD_COLUMNS = {
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
        "measurement_method", "notes", "raw_points_json", "supersedes_id",
        "revision",
    ),
}
_LIBRARY_TABLES = {
    "work": "reference_works",
    "treatment": "reference_taxon_treatments",
    "measurement_set": "reference_measurement_sets",
}
_JSON_PAYLOAD_COLUMNS = frozenset({"authors_json", "editors_json", "raw_points_json"})


class ReferenceCloudSyncStateError(ValueError):
    """Invalid or missing local reference transport state."""


class ReferenceCloudSyncAccountMismatchError(ReferenceCloudSyncStateError):
    """A transport row is already bound to another cloud account."""


@dataclass(frozen=True, slots=True)
class ReferenceCloudSyncState:
    entity_type: str
    entity_id: str
    cloud_user_id: str | None = None
    remote_identity_state: str = "never_attempted"
    cloud_row_version: int | None = None
    accepted_payload: dict[str, Any] | None = None
    sync_status: str = "dirty"
    conflict: dict[str, Any] | None = None
    retry_count: int = 0
    last_error: str | None = None
    last_attempted_at: str | None = None
    updated_at: str | None = None


@dataclass(frozen=True, slots=True)
class ReferenceCloudTombstone:
    entity_type: str
    entity_id: str
    cloud_user_id: str
    remote_identity_state: str
    expected_row_version: int | None
    accepted_payload: dict[str, Any] | None
    reference_work_id: str | None = None
    taxon_treatment_id: str | None = None
    deleted_at: str | None = None
    sync_status: str = "dirty"
    conflict: dict[str, Any] | None = None
    retry_count: int = 0
    last_error: str | None = None
    last_attempted_at: str | None = None
    updated_at: str | None = None


@dataclass(frozen=True, slots=True)
class ObservationReferenceUseCloudTombstone:
    use_id: str
    reference_measurement_set_id: str
    local_observation_id: int | None
    observation_cloud_id: str | None
    cloud_user_id: str
    remote_identity_state: str
    expected_row_version: int | None
    accepted_payload: dict[str, Any] | None
    deleted_at: str | None = None
    sync_status: str = "dirty"
    conflict: dict[str, Any] | None = None
    retry_count: int = 0
    last_error: str | None = None
    last_attempted_at: str | None = None
    updated_at: str | None = None


def _canonical_json(value: dict[str, Any] | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ReferenceCloudSyncStateError("sync payloads must be JSON objects")
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _load_json(value: str | None) -> dict[str, Any] | None:
    if value is None:
        return None
    loaded = json.loads(value)
    if not isinstance(loaded, dict):
        raise ReferenceCloudSyncStateError("stored sync payload is not a JSON object")
    return loaded


def _validate_state(state: ReferenceCloudSyncState) -> None:
    if state.remote_identity_state not in _REMOTE_IDENTITY_STATES:
        raise ReferenceCloudSyncStateError("invalid remote identity state")
    if state.sync_status not in _SYNC_STATUSES:
        raise ReferenceCloudSyncStateError("invalid sync status")
    if state.retry_count < 0:
        raise ReferenceCloudSyncStateError("retry count cannot be negative")
    if state.remote_identity_state == "never_attempted":
        if state.cloud_row_version is not None or state.accepted_payload is not None:
            raise ReferenceCloudSyncStateError(
                "never-attempted state cannot have a cloud version or baseline"
            )
    elif not str(state.cloud_user_id or "").strip():
        raise ReferenceCloudSyncStateError("remote identity state requires an account")
    if state.remote_identity_state == "create_outcome_unknown":
        if state.cloud_row_version is not None:
            raise ReferenceCloudSyncStateError(
                "unknown create outcome cannot have a cloud row version"
            )
        if state.accepted_payload is not None:
            raise ReferenceCloudSyncStateError(
                "unknown create outcome cannot have an accepted baseline"
            )
    elif state.remote_identity_state == "acknowledged":
        if not state.cloud_row_version or state.cloud_row_version < 1:
            raise ReferenceCloudSyncStateError(
                "acknowledged state requires a positive cloud row version"
            )
        if state.accepted_payload is None:
            raise ReferenceCloudSyncStateError(
                "acknowledged state requires an accepted baseline"
            )
    _canonical_json(state.accepted_payload)
    _canonical_json(state.conflict)


def _reference_connection() -> sqlite3.Connection:
    connection = get_reference_connection()
    connection.row_factory = sqlite3.Row
    init_reference_library_schema(connection)
    return connection


def _observation_connection() -> sqlite3.Connection:
    connection = get_connection()
    connection.row_factory = sqlite3.Row
    init_observation_reference_uses_schema(connection)
    return connection


def _state_from_row(
    row: sqlite3.Row, *, entity_type: str, id_column: str
) -> ReferenceCloudSyncState:
    return ReferenceCloudSyncState(
        entity_type=entity_type,
        entity_id=str(row[id_column]),
        cloud_user_id=row["cloud_user_id"],
        remote_identity_state=str(row["remote_identity_state"]),
        cloud_row_version=row["cloud_row_version"],
        accepted_payload=_load_json(row["accepted_payload_json"]),
        sync_status=str(row["sync_status"]),
        conflict=_load_json(row["conflict_json"]),
        retry_count=int(row["retry_count"]),
        last_error=row["last_error"],
        last_attempted_at=row["last_attempted_at"],
        updated_at=row["updated_at"],
    )


def _reject_account_change(existing: str | None, requested: str | None) -> None:
    if existing is not None and requested != existing:
        raise ReferenceCloudSyncAccountMismatchError(
            f"reference sync state is bound to cloud account {existing}"
        )


def _require_cloud_user_id(cloud_user_id: str) -> str:
    value = str(cloud_user_id or "").strip()
    if not value:
        raise ReferenceCloudSyncStateError("cloud account is required")
    return value


def _attempted_at() -> str:
    return datetime.now(timezone.utc).isoformat()


def _payload_from_mapping(entity_type: LibraryEntityType, row) -> dict[str, Any]:
    try:
        columns = _LIBRARY_PAYLOAD_COLUMNS[entity_type]
    except KeyError as exc:
        raise ReferenceCloudSyncStateError("invalid library entity type") from exc
    payload: dict[str, Any] = {}
    for column in columns:
        value = row[column]
        if column in _JSON_PAYLOAD_COLUMNS and isinstance(value, str):
            try:
                value = json.loads(value)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ReferenceCloudSyncStateError(
                    f"invalid JSON in library payload field {column}"
                ) from exc
        payload[column] = value
    payload["deleted"] = bool(row.get("deleted_at")) if isinstance(row, dict) else False
    return payload


def canonical_library_payload(
    entity_type: LibraryEntityType, row: dict[str, Any]
) -> dict[str, Any]:
    """Project a local or remote row to the strict Stage 3 mutation payload."""
    payload = _payload_from_mapping(entity_type, row)
    if not isinstance(row, dict):
        payload["deleted"] = False
    return payload


def _load_library_payload(
    connection: sqlite3.Connection,
    entity_type: LibraryEntityType,
    entity_id: str,
) -> dict[str, Any] | None:
    if entity_type not in _LIBRARY_ENTITY_TYPES:
        raise ReferenceCloudSyncStateError("invalid library entity type")
    row = connection.execute(
        f"SELECT * FROM {_LIBRARY_TABLES[entity_type]} WHERE id=?", (entity_id,)
    ).fetchone()
    return _payload_from_mapping(entity_type, row) if row is not None else None


def load_library_payload(
    entity_type: LibraryEntityType, entity_id: str
) -> dict[str, Any] | None:
    connection = _reference_connection()
    try:
        return _load_library_payload(connection, entity_type, entity_id)
    finally:
        connection.close()


def record_library_mutation_intent(
    connection: sqlite3.Connection,
    entity_type: LibraryEntityType,
    entity_id: str,
    *,
    schema_name: str = "main",
) -> None:
    """Mark a local library edit inside its caller-owned transaction."""
    if entity_type not in _LIBRARY_ENTITY_TYPES:
        raise ReferenceCloudSyncStateError("invalid library entity type")
    if not schema_name.replace("_", "").isalnum():
        raise ReferenceCloudSyncStateError("invalid SQLite schema name")
    cursor = connection.execute(
        f"""
        UPDATE {schema_name}.reference_cloud_sync_state
        SET sync_status=CASE
                WHEN sync_status='conflict' THEN 'conflict' ELSE 'dirty'
            END,
            retry_count=0, last_error=NULL, last_attempted_at=NULL,
            updated_at=CURRENT_TIMESTAMP
        WHERE entity_type=? AND entity_id=?
        """,
        (entity_type, entity_id),
    )
    if cursor.rowcount != 1:
        raise ReferenceCloudSyncStateError("library sync state does not exist")


def record_use_mutation_intent(
    connection: sqlite3.Connection,
    use_id: str,
) -> None:
    """Mark a local observation-use edit inside its caller-owned transaction."""
    cursor = connection.execute(
        """
        UPDATE observation_reference_use_cloud_sync_state
        SET sync_status=CASE
                WHEN sync_status='conflict' THEN 'conflict' ELSE 'dirty'
            END,
            retry_count=0, last_error=NULL, last_attempted_at=NULL,
            updated_at=CURRENT_TIMESTAMP
        WHERE use_id=?
        """,
        (use_id,),
    )
    if cursor.rowcount != 1:
        raise ReferenceCloudSyncStateError("observation-use sync state does not exist")


class ReferenceCloudSyncStateRepository:
    """Read/write the dormant Stage 4b transport state and tombstones."""

    @staticmethod
    def get_library(
        entity_type: LibraryEntityType, entity_id: str
    ) -> ReferenceCloudSyncState | None:
        if entity_type not in _LIBRARY_ENTITY_TYPES:
            raise ReferenceCloudSyncStateError("invalid library entity type")
        connection = _reference_connection()
        try:
            row = connection.execute(
                "SELECT * FROM reference_cloud_sync_state "
                "WHERE entity_type=? AND entity_id=?",
                (entity_type, entity_id),
            ).fetchone()
        finally:
            connection.close()
        return (
            _state_from_row(row, entity_type=entity_type, id_column="entity_id")
            if row is not None
            else None
        )

    @staticmethod
    def get_library_pull_cursor(
        cloud_user_id: str, entity_type: LibraryEntityType
    ) -> tuple[str, str] | None:
        if entity_type not in _LIBRARY_ENTITY_TYPES:
            raise ReferenceCloudSyncStateError("invalid library entity type")
        account_id = _require_cloud_user_id(cloud_user_id)
        connection = _reference_connection()
        try:
            row = connection.execute(
                "SELECT updated_at, entity_id FROM reference_cloud_pull_cursors "
                "WHERE cloud_user_id=? AND entity_type=?",
                (account_id, entity_type),
            ).fetchone()
        finally:
            connection.close()
        return (str(row["updated_at"]), str(row["entity_id"])) if row else None

    @staticmethod
    def get_library_remote_tombstone(
        cloud_user_id: str, entity_type: LibraryEntityType, entity_id: str
    ) -> dict[str, Any] | None:
        if entity_type not in _LIBRARY_ENTITY_TYPES:
            raise ReferenceCloudSyncStateError("invalid library entity type")
        account_id = _require_cloud_user_id(cloud_user_id)
        connection = _reference_connection()
        try:
            row = connection.execute(
                "SELECT cloud_row_version, accepted_payload_json, deleted_at "
                "FROM reference_cloud_remote_tombstone_markers "
                "WHERE cloud_user_id=? AND entity_type=? AND entity_id=?",
                (account_id, entity_type, entity_id),
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            return None
        return {
            "row_version": int(row["cloud_row_version"]),
            "accepted_payload": _load_json(row["accepted_payload_json"]),
            "deleted_at": str(row["deleted_at"]),
        }

    @classmethod
    def save_library(
        cls, state: ReferenceCloudSyncState
    ) -> ReferenceCloudSyncState:
        if state.entity_type not in _LIBRARY_ENTITY_TYPES:
            raise ReferenceCloudSyncStateError("invalid library entity type")
        _validate_state(state)
        connection = _reference_connection()
        try:
            existing = connection.execute(
                "SELECT cloud_user_id FROM reference_cloud_sync_state "
                "WHERE entity_type=? AND entity_id=?",
                (state.entity_type, state.entity_id),
            ).fetchone()
            if existing is None:
                raise ReferenceCloudSyncStateError("library sync state does not exist")
            _reject_account_change(existing["cloud_user_id"], state.cloud_user_id)
            connection.execute(
                """
                UPDATE reference_cloud_sync_state
                SET cloud_user_id=?, remote_identity_state=?, cloud_row_version=?,
                    accepted_payload_json=?, sync_status=?, conflict_json=?,
                    retry_count=?, last_error=?, last_attempted_at=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE entity_type=? AND entity_id=?
                """,
                (
                    state.cloud_user_id,
                    state.remote_identity_state,
                    state.cloud_row_version,
                    _canonical_json(state.accepted_payload),
                    state.sync_status,
                    _canonical_json(state.conflict),
                    state.retry_count,
                    state.last_error,
                    state.last_attempted_at,
                    state.entity_type,
                    state.entity_id,
                ),
            )
            connection.commit()
        finally:
            connection.close()
        saved = cls.get_library(state.entity_type, state.entity_id)
        if saved is None:
            raise ReferenceCloudSyncStateError("library sync state disappeared")
        return saved

    @classmethod
    def prepare_library_create(
        cls, entity_type: LibraryEntityType, entity_id: str, cloud_user_id: str
    ) -> ReferenceCloudSyncState:
        """Durably record that a create may reach cloud before sending it."""
        state = cls.get_library(entity_type, entity_id)
        if state is None:
            raise ReferenceCloudSyncStateError("library sync state does not exist")
        if state.remote_identity_state not in {"never_attempted", "create_outcome_unknown"}:
            raise ReferenceCloudSyncStateError("library row is not awaiting creation")
        return cls.save_library(
            ReferenceCloudSyncState(
                entity_type=entity_type,
                entity_id=entity_id,
                cloud_user_id=_require_cloud_user_id(cloud_user_id),
                remote_identity_state="create_outcome_unknown",
                sync_status=state.sync_status,
                conflict=state.conflict,
                retry_count=state.retry_count,
                last_error=state.last_error,
                last_attempted_at=_attempted_at(),
            )
        )

    @classmethod
    def acknowledge_library(
        cls,
        entity_type: LibraryEntityType,
        entity_id: str,
        cloud_user_id: str,
        *,
        sent_payload: dict[str, Any],
        accepted_payload: dict[str, Any],
        cloud_row_version: int,
    ) -> ReferenceCloudSyncState | None:
        """Persist an acknowledgement to the live row or its new tombstone."""
        account_id = _require_cloud_user_id(cloud_user_id)
        if not isinstance(cloud_row_version, int) or cloud_row_version < 1:
            raise ReferenceCloudSyncStateError("acknowledgement requires a row version")
        connection = _reference_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            state_row = connection.execute(
                "SELECT cloud_user_id FROM reference_cloud_sync_state "
                "WHERE entity_type=? AND entity_id=?",
                (entity_type, entity_id),
            ).fetchone()
            if state_row is None:
                tombstone_row = connection.execute(
                    "SELECT cloud_user_id FROM reference_cloud_tombstones "
                    "WHERE entity_type=? AND entity_id=?",
                    (entity_type, entity_id),
                ).fetchone()
                if tombstone_row is None:
                    raise ReferenceCloudSyncStateError(
                        "library sync state and tombstone do not exist"
                    )
                _reject_account_change(tombstone_row["cloud_user_id"], account_id)
                connection.execute(
                    """
                    UPDATE reference_cloud_tombstones
                    SET remote_identity_state='acknowledged',
                        expected_row_version=?, accepted_payload_json=?,
                        sync_status='dirty', conflict_json=NULL, retry_count=0,
                        last_error=NULL, last_attempted_at=?,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE entity_type=? AND entity_id=? AND cloud_user_id=?
                    """,
                    (
                        cloud_row_version,
                        _canonical_json(accepted_payload),
                        _attempted_at(),
                        entity_type,
                        entity_id,
                        account_id,
                    ),
                )
                connection.commit()
                return None
            _reject_account_change(state_row["cloud_user_id"], account_id)
            current_payload = _load_library_payload(connection, entity_type, entity_id)
            if current_payload is None:
                raise ReferenceCloudSyncStateError("library row disappeared during push")
            sync_status = "clean" if current_payload == sent_payload else "dirty"
            connection.execute(
                """
                UPDATE reference_cloud_sync_state
                SET cloud_user_id=?, remote_identity_state='acknowledged',
                    cloud_row_version=?, accepted_payload_json=?, sync_status=?,
                    conflict_json=NULL, retry_count=0, last_error=NULL,
                    last_attempted_at=?, updated_at=CURRENT_TIMESTAMP
                WHERE entity_type=? AND entity_id=?
                """,
                (
                    account_id,
                    cloud_row_version,
                    _canonical_json(accepted_payload),
                    sync_status,
                    _attempted_at(),
                    entity_type,
                    entity_id,
                ),
            )
            connection.execute(
                "DELETE FROM reference_cloud_tombstones "
                "WHERE entity_type=? AND entity_id=? AND cloud_user_id=?",
                (entity_type, entity_id, account_id),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        saved = cls.get_library(entity_type, entity_id)
        if saved is None:
            raise ReferenceCloudSyncStateError("library sync state disappeared")
        return saved

    @classmethod
    def acknowledge_library_tombstone(
        cls,
        entity_type: LibraryEntityType,
        entity_id: str,
        cloud_user_id: str,
        *,
        accepted_payload: dict[str, Any],
        cloud_row_version: int,
    ) -> None:
        """Resolve a tombstone or atomically transfer it to a recreated row."""
        account_id = _require_cloud_user_id(cloud_user_id)
        if not isinstance(cloud_row_version, int) or cloud_row_version < 1:
            raise ReferenceCloudSyncStateError("acknowledgement requires a row version")
        connection = _reference_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            tombstone = connection.execute(
                "SELECT cloud_user_id FROM reference_cloud_tombstones "
                "WHERE entity_type=? AND entity_id=?",
                (entity_type, entity_id),
            ).fetchone()
            if tombstone is None:
                raise ReferenceCloudSyncStateError("library tombstone does not exist")
            _reject_account_change(tombstone["cloud_user_id"], account_id)
            live_state = connection.execute(
                "SELECT cloud_user_id FROM reference_cloud_sync_state "
                "WHERE entity_type=? AND entity_id=?",
                (entity_type, entity_id),
            ).fetchone()
            if live_state is not None:
                _reject_account_change(live_state["cloud_user_id"], account_id)
                connection.execute(
                    """
                    UPDATE reference_cloud_sync_state
                    SET cloud_user_id=?, remote_identity_state='acknowledged',
                        cloud_row_version=?, accepted_payload_json=?,
                        sync_status='dirty', conflict_json=NULL, retry_count=0,
                        last_error=NULL, last_attempted_at=?,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE entity_type=? AND entity_id=?
                    """,
                    (
                        account_id,
                        cloud_row_version,
                        _canonical_json(accepted_payload),
                        _attempted_at(),
                        entity_type,
                        entity_id,
                    ),
                )
            connection.execute(
                "DELETE FROM reference_cloud_tombstones "
                "WHERE entity_type=? AND entity_id=? AND cloud_user_id=?",
                (entity_type, entity_id, account_id),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @classmethod
    def clean_library_if_unchanged(
        cls,
        entity_type: LibraryEntityType,
        entity_id: str,
        expected_payload: dict[str, Any],
    ) -> bool:
        """Clear redundant dirty intent only while its domain payload is unchanged."""
        connection = _reference_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            if _load_library_payload(connection, entity_type, entity_id) != expected_payload:
                connection.rollback()
                return False
            cursor = connection.execute(
                """
                UPDATE reference_cloud_sync_state
                SET sync_status='clean', conflict_json=NULL, retry_count=0,
                    last_error=NULL, last_attempted_at=?, updated_at=CURRENT_TIMESTAMP
                WHERE entity_type=? AND entity_id=?
                  AND remote_identity_state='acknowledged'
                  AND sync_status!='conflict'
                """,
                (_attempted_at(), entity_type, entity_id),
            )
            connection.commit()
            return cursor.rowcount == 1
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @classmethod
    def claim_library_restores(cls, cloud_user_id: str) -> int:
        """Atomically transfer same-ID tombstone transport state to live rows."""
        account_id = _require_cloud_user_id(cloud_user_id)
        connection = _reference_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT tombstone.*
                FROM reference_cloud_tombstones AS tombstone
                JOIN reference_cloud_sync_state AS state
                  ON state.entity_type=tombstone.entity_type
                 AND state.entity_id=tombstone.entity_id
                WHERE tombstone.cloud_user_id=?
                ORDER BY tombstone.entity_type, tombstone.entity_id
                """,
                (account_id,),
            ).fetchall()
            for row in rows:
                connection.execute(
                    """
                    UPDATE reference_cloud_sync_state
                    SET cloud_user_id=?, remote_identity_state=?, cloud_row_version=?,
                        accepted_payload_json=?, sync_status='dirty', conflict_json=NULL,
                        retry_count=0, last_error=NULL, last_attempted_at=NULL,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE entity_type=? AND entity_id=?
                    """,
                    (
                        account_id,
                        row["remote_identity_state"],
                        row["expected_row_version"],
                        row["accepted_payload_json"],
                        row["entity_type"],
                        row["entity_id"],
                    ),
                )
                connection.execute(
                    "DELETE FROM reference_cloud_tombstones "
                    "WHERE entity_type=? AND entity_id=? AND cloud_user_id=?",
                    (row["entity_type"], row["entity_id"], account_id),
                )
                connection.execute(
                    "DELETE FROM reference_cloud_remote_tombstone_markers "
                    "WHERE entity_type=? AND entity_id=? AND cloud_user_id=?",
                    (row["entity_type"], row["entity_id"], account_id),
                )
            marker_rows = connection.execute(
                """
                SELECT marker.*
                FROM reference_cloud_remote_tombstone_markers AS marker
                JOIN reference_cloud_sync_state AS state
                  ON state.entity_type=marker.entity_type
                 AND state.entity_id=marker.entity_id
                WHERE marker.cloud_user_id=?
                ORDER BY marker.entity_type, marker.entity_id
                """,
                (account_id,),
            ).fetchall()
            for row in marker_rows:
                connection.execute(
                    """
                    UPDATE reference_cloud_sync_state
                    SET cloud_user_id=?, remote_identity_state='acknowledged',
                        cloud_row_version=?, accepted_payload_json=?,
                        sync_status='dirty', conflict_json=NULL, retry_count=0,
                        last_error=NULL, last_attempted_at=NULL,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE entity_type=? AND entity_id=?
                    """,
                    (
                        account_id,
                        row["cloud_row_version"],
                        row["accepted_payload_json"],
                        row["entity_type"],
                        row["entity_id"],
                    ),
                )
                connection.execute(
                    "DELETE FROM reference_cloud_remote_tombstone_markers "
                    "WHERE entity_type=? AND entity_id=? AND cloud_user_id=?",
                    (row["entity_type"], row["entity_id"], account_id),
                )
            connection.commit()
            return len(rows) + len(marker_rows)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def get_use(use_id: str) -> ReferenceCloudSyncState | None:
        connection = _observation_connection()
        try:
            row = connection.execute(
                "SELECT * FROM observation_reference_use_cloud_sync_state "
                "WHERE use_id=?",
                (use_id,),
            ).fetchone()
        finally:
            connection.close()
        return (
            _state_from_row(
                row,
                entity_type="observation_use",
                id_column="use_id",
            )
            if row is not None
            else None
        )

    @classmethod
    def save_use(cls, state: ReferenceCloudSyncState) -> ReferenceCloudSyncState:
        if state.entity_type != "observation_use":
            raise ReferenceCloudSyncStateError("expected observation-use state")
        _validate_state(state)
        connection = _observation_connection()
        try:
            existing = connection.execute(
                "SELECT cloud_user_id FROM observation_reference_use_cloud_sync_state "
                "WHERE use_id=?",
                (state.entity_id,),
            ).fetchone()
            if existing is None:
                raise ReferenceCloudSyncStateError("observation-use sync state does not exist")
            _reject_account_change(existing["cloud_user_id"], state.cloud_user_id)
            connection.execute(
                """
                UPDATE observation_reference_use_cloud_sync_state
                SET cloud_user_id=?, remote_identity_state=?, cloud_row_version=?,
                    accepted_payload_json=?, sync_status=?, conflict_json=?,
                    retry_count=?, last_error=?, last_attempted_at=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE use_id=?
                """,
                (
                    state.cloud_user_id,
                    state.remote_identity_state,
                    state.cloud_row_version,
                    _canonical_json(state.accepted_payload),
                    state.sync_status,
                    _canonical_json(state.conflict),
                    state.retry_count,
                    state.last_error,
                    state.last_attempted_at,
                    state.entity_id,
                ),
            )
            connection.commit()
        finally:
            connection.close()
        saved = cls.get_use(state.entity_id)
        if saved is None:
            raise ReferenceCloudSyncStateError("observation-use sync state disappeared")
        return saved

    @staticmethod
    def list_library_tombstones(
        cloud_user_id: str,
    ) -> list[ReferenceCloudTombstone]:
        account_id = _require_cloud_user_id(cloud_user_id)
        connection = _reference_connection()
        try:
            rows = connection.execute(
                "SELECT * FROM reference_cloud_tombstones "
                "WHERE cloud_user_id=? "
                "ORDER BY deleted_at, entity_type, entity_id",
                (account_id,),
            ).fetchall()
        finally:
            connection.close()
        return [
            ReferenceCloudTombstone(
                entity_type=str(row["entity_type"]),
                entity_id=str(row["entity_id"]),
                cloud_user_id=str(row["cloud_user_id"]),
                remote_identity_state=str(row["remote_identity_state"]),
                expected_row_version=row["expected_row_version"],
                accepted_payload=_load_json(row["accepted_payload_json"]),
                reference_work_id=row["reference_work_id"],
                taxon_treatment_id=row["taxon_treatment_id"],
                deleted_at=row["deleted_at"],
                sync_status=str(row["sync_status"]),
                conflict=_load_json(row["conflict_json"]),
                retry_count=int(row["retry_count"]),
                last_error=row["last_error"],
                last_attempted_at=row["last_attempted_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    @classmethod
    def save_library_tombstone(
        cls, tombstone: ReferenceCloudTombstone
    ) -> ReferenceCloudTombstone:
        if tombstone.entity_type not in _LIBRARY_ENTITY_TYPES:
            raise ReferenceCloudSyncStateError("invalid library entity type")
        account_id = _require_cloud_user_id(tombstone.cloud_user_id)
        if tombstone.remote_identity_state == "acknowledged":
            if not tombstone.expected_row_version or tombstone.accepted_payload is None:
                raise ReferenceCloudSyncStateError(
                    "acknowledged tombstone requires token and baseline"
                )
        elif tombstone.remote_identity_state == "create_outcome_unknown":
            if (
                tombstone.expected_row_version is not None
                or tombstone.accepted_payload is not None
            ):
                raise ReferenceCloudSyncStateError(
                    "unknown tombstone cannot have token or baseline"
                )
        else:
            raise ReferenceCloudSyncStateError("invalid tombstone identity state")
        if tombstone.sync_status not in {"dirty", "retry", "conflict"}:
            raise ReferenceCloudSyncStateError("invalid tombstone sync status")
        connection = _reference_connection()
        try:
            cursor = connection.execute(
                """
                UPDATE reference_cloud_tombstones
                SET remote_identity_state=?, expected_row_version=?,
                    accepted_payload_json=?, sync_status=?, conflict_json=?,
                    retry_count=?, last_error=?, last_attempted_at=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE entity_type=? AND entity_id=? AND cloud_user_id=?
                """,
                (
                    tombstone.remote_identity_state,
                    tombstone.expected_row_version,
                    _canonical_json(tombstone.accepted_payload),
                    tombstone.sync_status,
                    _canonical_json(tombstone.conflict),
                    tombstone.retry_count,
                    tombstone.last_error,
                    tombstone.last_attempted_at,
                    tombstone.entity_type,
                    tombstone.entity_id,
                    account_id,
                ),
            )
            connection.commit()
            if cursor.rowcount != 1:
                raise ReferenceCloudSyncStateError("library tombstone does not exist")
        finally:
            connection.close()
        for saved in cls.list_library_tombstones(account_id):
            if (saved.entity_type, saved.entity_id) == (
                tombstone.entity_type,
                tombstone.entity_id,
            ):
                return saved
        raise ReferenceCloudSyncStateError("library tombstone disappeared")

    @staticmethod
    def list_use_tombstones(
        cloud_user_id: str,
    ) -> list[ObservationReferenceUseCloudTombstone]:
        account_id = _require_cloud_user_id(cloud_user_id)
        connection = _observation_connection()
        try:
            rows = connection.execute(
                "SELECT * FROM observation_reference_use_cloud_tombstones "
                "WHERE cloud_user_id=? ORDER BY deleted_at, use_id",
                (account_id,),
            ).fetchall()
        finally:
            connection.close()
        return [
            ObservationReferenceUseCloudTombstone(
                use_id=str(row["use_id"]),
                reference_measurement_set_id=str(
                    row["reference_measurement_set_id"]
                ),
                local_observation_id=row["local_observation_id"],
                observation_cloud_id=row["observation_cloud_id"],
                cloud_user_id=str(row["cloud_user_id"]),
                remote_identity_state=str(row["remote_identity_state"]),
                expected_row_version=row["expected_row_version"],
                accepted_payload=_load_json(row["accepted_payload_json"]),
                deleted_at=row["deleted_at"],
                sync_status=str(row["sync_status"]),
                conflict=_load_json(row["conflict_json"]),
                retry_count=int(row["retry_count"]),
                last_error=row["last_error"],
                last_attempted_at=row["last_attempted_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    @staticmethod
    def resolve_library_tombstone(
        entity_type: LibraryEntityType, entity_id: str, cloud_user_id: str
    ) -> bool:
        if entity_type not in _LIBRARY_ENTITY_TYPES:
            raise ReferenceCloudSyncStateError("invalid library entity type")
        account_id = _require_cloud_user_id(cloud_user_id)
        connection = _reference_connection()
        try:
            cursor = connection.execute(
                "DELETE FROM reference_cloud_tombstones "
                "WHERE entity_type=? AND entity_id=? AND cloud_user_id=?",
                (entity_type, entity_id, account_id),
            )
            connection.commit()
            return cursor.rowcount == 1
        finally:
            connection.close()

    @staticmethod
    def resolve_use_tombstone(use_id: str, cloud_user_id: str) -> bool:
        account_id = _require_cloud_user_id(cloud_user_id)
        connection = _observation_connection()
        try:
            cursor = connection.execute(
                "DELETE FROM observation_reference_use_cloud_tombstones "
                "WHERE use_id=? AND cloud_user_id=?",
                (use_id, account_id),
            )
            connection.commit()
            return cursor.rowcount == 1
        finally:
            connection.close()
