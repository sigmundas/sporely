"""Dormant local transport state for normalized reference cloud sync.

This module stores device/account-specific sync metadata separately from the
portable reference domain rows. Stage 4b does not perform network operations or
wire these repositories into ordinary reference mutations.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
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


def _state_from_row(row: sqlite3.Row, *, entity_type: str, id_column: str) -> ReferenceCloudSyncState:
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
