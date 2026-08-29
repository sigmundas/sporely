"""Complete-feed reconciliation for observation reference uses."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from database import schema as database_schema
from database.reference_library_schema import (
    OBSERVATION_REFERENCE_ROLES,
    init_observation_reference_uses_schema,
)
from database.reference_sync_reconciliation import (
    ReferencePullApplyResult,
    ReferencePullReconciliationError,
)
from database.reference_sync_state import (
    ReferenceCloudSyncStateError,
    canonical_observation_use_payload,
)


_MUTABLE_FIELDS = frozenset(
    {
        "observation_id",
        "reference_measurement_set_id",
        "role",
        "note",
        "selected_at",
        "reference_revision",
        "snapshot_json",
    }
)
_MERGEABLE_FIELDS = frozenset({"role", "note"})


@dataclass(frozen=True, slots=True)
class StagedObservationReferenceUseFeed:
    rows: tuple[dict[str, Any], ...]


def _canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _parse_json(value: str | None) -> dict[str, Any] | None:
    if value is None:
        return None
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ReferencePullReconciliationError("stored use baseline is not an object")
    return parsed


def stage_observation_reference_use_feed(
    cloud_user_id: str, rows: tuple[dict[str, Any], ...]
) -> StagedObservationReferenceUseFeed:
    """Validate the complete owner use feed without changing local state."""
    account_id = str(cloud_user_id or "").strip()
    seen: set[str] = set()
    staged: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        use_id = str(row.get("id") or "").strip()
        if not use_id or use_id in seen:
            raise ReferencePullReconciliationError(
                "duplicate or missing remote observation-use identity"
            )
        seen.add(use_id)
        if str(row.get("user_id") or "").strip() != account_id:
            raise ReferencePullReconciliationError(
                "remote observation use belongs to another account"
            )
        version = row.get("row_version")
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise ReferencePullReconciliationError(
                "remote observation use has invalid row_version"
            )
        if not str(row.get("created_at") or "").strip() or not str(
            row.get("updated_at") or ""
        ).strip():
            raise ReferencePullReconciliationError(
                "remote observation use has incomplete timestamps"
            )
        if row.get("deleted_at") is not None and not str(row["deleted_at"]).strip():
            raise ReferencePullReconciliationError(
                "remote observation use has invalid deleted_at"
            )
        try:
            payload = canonical_observation_use_payload(row)
        except (KeyError, TypeError, ValueError, ReferenceCloudSyncStateError) as exc:
            raise ReferencePullReconciliationError(
                "remote observation use is missing canonical fields"
            ) from exc
        if payload["role"] not in OBSERVATION_REFERENCE_ROLES:
            raise ReferencePullReconciliationError(
                "remote observation use has invalid role"
            )
        snapshot = payload["snapshot_json"]
        if snapshot.get("schema_version") != 1:
            raise ReferencePullReconciliationError(
                "remote observation use has unsupported snapshot schema"
            )
        if (
            str(snapshot.get("reference_measurement_set_id") or "")
            != payload["reference_measurement_set_id"]
        ):
            raise ReferencePullReconciliationError(
                "remote observation use snapshot identity disagrees"
            )
        staged.append({**row, "_payload": payload})
    return StagedObservationReferenceUseFeed(tuple(staged))


def _local_payload(connection: sqlite3.Connection, use_id: str) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT use_row.*, NULLIF(TRIM(observation.cloud_id), '') AS observation_cloud_id
        FROM observation_reference_uses AS use_row
        JOIN observations AS observation ON observation.id=use_row.observation_id
        WHERE use_row.id=?
        """,
        (use_id,),
    ).fetchone()
    return canonical_observation_use_payload(row) if row is not None else None


def _observation_id_for_cloud(
    connection: sqlite3.Connection, observation_cloud_id: int
) -> int | None:
    rows = connection.execute(
        "SELECT id FROM observations WHERE TRIM(cloud_id)=? ORDER BY id",
        (str(observation_cloud_id),),
    ).fetchall()
    return int(rows[0]["id"]) if len(rows) == 1 else None


def _source_ready(
    cloud_user_id: str,
    set_id: str,
    *,
    connection: sqlite3.Connection | None = None,
) -> str | None:
    owned_connection = connection is None
    if connection is None:
        connection = database_schema.get_reference_connection()
        connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            """
            SELECT library.id, state.cloud_user_id, state.remote_identity_state,
                   state.sync_status
            FROM reference_measurement_sets AS library
            LEFT JOIN reference_cloud_sync_state AS state
              ON state.entity_type='measurement_set' AND state.entity_id=library.id
            WHERE library.id=?
            """,
            (set_id,),
        ).fetchone()
    finally:
        if owned_connection:
            connection.close()
    if row is None:
        return "missing_reference_measurement_set"
    if row["cloud_user_id"] != cloud_user_id:
        return "reference_measurement_set_account_mismatch"
    if row["remote_identity_state"] != "acknowledged":
        return "reference_measurement_set_not_acknowledged"
    if row["sync_status"] == "conflict":
        return "reference_measurement_set_conflict"
    if row["sync_status"] != "clean":
        return "reference_measurement_set_not_converged"
    return None


def _write_state(
    connection: sqlite3.Connection,
    use_id: str,
    cloud_user_id: str,
    payload: dict[str, Any],
    row_version: int,
    *,
    sync_status: str,
    conflict: dict[str, Any] | None = None,
) -> None:
    connection.execute(
        """
        UPDATE observation_reference_use_cloud_sync_state
        SET cloud_user_id=?, remote_identity_state='acknowledged',
            cloud_row_version=?, accepted_payload_json=?, sync_status=?,
            conflict_json=?, retry_count=0, last_error=?,
            last_attempted_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP
        WHERE use_id=?
        """,
        (
            cloud_user_id,
            row_version,
            _canonical_json(payload),
            sync_status,
            _canonical_json(conflict) if conflict is not None else None,
            "remote/local observation-use conflict" if conflict else None,
            use_id,
        ),
    )


def _claim_restored_uses(
    connection: sqlite3.Connection, cloud_user_id: str
) -> None:
    """Move same-ID tombstone tokens onto recreated live rows in this transaction."""
    tombstones = connection.execute(
        """
        SELECT tombstone.*
        FROM observation_reference_use_cloud_tombstones AS tombstone
        JOIN observation_reference_use_cloud_sync_state AS state
          ON state.use_id=tombstone.use_id
        WHERE tombstone.cloud_user_id=?
        ORDER BY tombstone.use_id
        """,
        (cloud_user_id,),
    ).fetchall()
    for row in tombstones:
        cursor = connection.execute(
            """
            UPDATE observation_reference_use_cloud_sync_state
            SET cloud_user_id=?, remote_identity_state=?, cloud_row_version=?,
                accepted_payload_json=?, sync_status='dirty', conflict_json=NULL,
                retry_count=0, last_error=NULL, last_attempted_at=NULL,
                updated_at=CURRENT_TIMESTAMP
            WHERE use_id=? AND (cloud_user_id IS NULL OR cloud_user_id=?)
            """,
            (
                cloud_user_id,
                row["remote_identity_state"],
                row["expected_row_version"],
                row["accepted_payload_json"],
                row["use_id"],
                cloud_user_id,
            ),
        )
        if cursor.rowcount != 1:
            raise ReferencePullReconciliationError(
                "restored observation use belongs to another account"
            )
        connection.execute(
            "DELETE FROM observation_reference_use_cloud_tombstones "
            "WHERE use_id=? AND cloud_user_id=?",
            (row["use_id"], cloud_user_id),
        )
        connection.execute(
            "DELETE FROM observation_reference_use_cloud_remote_tombstone_markers "
            "WHERE use_id=? AND cloud_user_id=?",
            (row["use_id"], cloud_user_id),
        )
    markers = connection.execute(
        """
        SELECT marker.*
        FROM observation_reference_use_cloud_remote_tombstone_markers AS marker
        JOIN observation_reference_use_cloud_sync_state AS state
          ON state.use_id=marker.use_id
        WHERE marker.cloud_user_id=?
        ORDER BY marker.use_id
        """,
        (cloud_user_id,),
    ).fetchall()
    for row in markers:
        cursor = connection.execute(
            """
            UPDATE observation_reference_use_cloud_sync_state
            SET cloud_user_id=?, remote_identity_state='acknowledged',
                cloud_row_version=?, accepted_payload_json=?, sync_status='dirty',
                conflict_json=NULL, retry_count=0, last_error=NULL,
                last_attempted_at=NULL, updated_at=CURRENT_TIMESTAMP
            WHERE use_id=? AND (cloud_user_id IS NULL OR cloud_user_id=?)
            """,
            (
                cloud_user_id,
                row["cloud_row_version"],
                row["accepted_payload_json"],
                row["use_id"],
                cloud_user_id,
            ),
        )
        if cursor.rowcount != 1:
            raise ReferencePullReconciliationError(
                "restored observation use belongs to another account"
            )
        connection.execute(
            "DELETE FROM observation_reference_use_cloud_remote_tombstone_markers "
            "WHERE use_id=? AND cloud_user_id=?",
            (row["use_id"], cloud_user_id),
        )


def _conflict(
    connection: sqlite3.Connection,
    use_id: str,
    cloud_user_id: str,
    remote: dict[str, Any],
    *,
    local: dict[str, Any] | None,
    baseline: dict[str, Any] | None,
    reason: str,
) -> None:
    diagnostic = {
        "operation": "pull",
        "reason": reason,
        "baseline": baseline,
        "local": local,
        "remote": remote,
    }
    connection.execute(
        """
        UPDATE observation_reference_use_cloud_sync_state
        SET cloud_user_id=COALESCE(cloud_user_id, ?), sync_status='conflict',
            conflict_json=?, last_error=?, last_attempted_at=CURRENT_TIMESTAMP,
            updated_at=CURRENT_TIMESTAMP
        WHERE use_id=?
        """,
        (cloud_user_id, _canonical_json(diagnostic), reason, use_id),
    )


def _apply_payload(
    connection: sqlite3.Connection,
    use_id: str,
    observation_id: int,
    payload: dict[str, Any],
    remote_row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO observation_reference_uses(
            id, observation_id, reference_measurement_set_id, role, note,
            selected_at, reference_revision, snapshot_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            observation_id=excluded.observation_id,
            reference_measurement_set_id=excluded.reference_measurement_set_id,
            role=excluded.role, note=excluded.note,
            selected_at=excluded.selected_at,
            reference_revision=excluded.reference_revision,
            snapshot_json=excluded.snapshot_json,
            updated_at=excluded.updated_at
        """,
        (
            use_id,
            observation_id,
            payload["reference_measurement_set_id"],
            payload["role"],
            payload["note"],
            payload["selected_at"],
            payload["reference_revision"],
            _canonical_json(payload["snapshot_json"]),
            remote_row["created_at"],
            remote_row["updated_at"],
        ),
    )


def _changed(payload: dict[str, Any], baseline: dict[str, Any]) -> set[str]:
    return {field for field in _MUTABLE_FIELDS if payload[field] != baseline[field]}


def _reconcile_live(
    connection: sqlite3.Connection,
    cloud_user_id: str,
    row: dict[str, Any],
    observation_id: int,
) -> tuple[int, str | None]:
    use_id = str(row["id"])
    remote = row["_payload"]
    tombstone = connection.execute(
        "SELECT * FROM observation_reference_use_cloud_tombstones WHERE use_id=?",
        (use_id,),
    ).fetchone()
    if tombstone is not None:
        if tombstone["cloud_user_id"] != cloud_user_id:
            connection.execute(
                """
                UPDATE observation_reference_use_cloud_tombstones
                SET sync_status='conflict', conflict_json=?, last_error=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE use_id=?
                """,
                (
                    _canonical_json(
                        {
                            "operation": "pull_delete_race",
                            "reason": "observation-use account mismatch",
                            "remote": remote,
                        }
                    ),
                    "observation-use account mismatch",
                    use_id,
                ),
            )
            return 0, f"observation_use:{use_id}"
        baseline = _parse_json(tombstone["accepted_payload_json"])
        if tombstone["sync_status"] == "conflict" or baseline != remote:
            diagnostic = {
                "operation": "pull_delete_race",
                "reason": "remote_changed_while_local_delete_pending",
                "baseline": baseline,
                "remote": remote,
            }
            connection.execute(
                """
                UPDATE observation_reference_use_cloud_tombstones
                SET sync_status='conflict', conflict_json=?, last_error=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE use_id=?
                """,
                (
                    _canonical_json(diagnostic),
                    "remote changed while local delete was pending",
                    use_id,
                ),
            )
            return 0, f"observation_use:{use_id}"
        connection.execute(
            """
            UPDATE observation_reference_use_cloud_tombstones
            SET remote_identity_state='acknowledged', expected_row_version=?,
                accepted_payload_json=?, sync_status='dirty', conflict_json=NULL,
                last_error=NULL, updated_at=CURRENT_TIMESTAMP
            WHERE use_id=?
            """,
            (row["row_version"], _canonical_json(remote), use_id),
        )
        return 0, None

    local = _local_payload(connection, use_id)
    state = connection.execute(
        "SELECT * FROM observation_reference_use_cloud_sync_state WHERE use_id=?",
        (use_id,),
    ).fetchone()
    if local is None:
        duplicate = connection.execute(
            """
            SELECT id FROM observation_reference_uses
            WHERE observation_id=? AND reference_measurement_set_id=? AND id!=?
            """,
            (observation_id, remote["reference_measurement_set_id"], use_id),
        ).fetchone()
        if duplicate is not None:
            duplicate_id = str(duplicate["id"])
            duplicate_payload = _local_payload(connection, duplicate_id)
            duplicate_state = connection.execute(
                "SELECT accepted_payload_json FROM observation_reference_use_cloud_sync_state WHERE use_id=?",
                (duplicate_id,),
            ).fetchone()
            _conflict(
                connection,
                duplicate_id,
                cloud_user_id,
                remote,
                local=duplicate_payload,
                baseline=(
                    _parse_json(duplicate_state["accepted_payload_json"])
                    if duplicate_state is not None
                    else None
                ),
                reason="remote uniqueness collision",
            )
            return 0, f"observation_use:{duplicate_id}"
        _apply_payload(connection, use_id, observation_id, remote, row)
        _write_state(
            connection,
            use_id,
            cloud_user_id,
            remote,
            row["row_version"],
            sync_status="clean",
        )
        return 1, None

    if state is None:
        raise ReferencePullReconciliationError("local observation use has no sync state")
    if state["cloud_user_id"] not in {None, cloud_user_id}:
        _conflict(
            connection, use_id, cloud_user_id, remote,
            local=local, baseline=None, reason="observation-use account mismatch"
        )
        return 0, f"observation_use:{use_id}"
    baseline = _parse_json(state["accepted_payload_json"])
    if state["sync_status"] == "conflict":
        return 0, f"observation_use:{use_id}"
    if baseline is None:
        if local == remote:
            _write_state(
                connection, use_id, cloud_user_id, remote, row["row_version"],
                sync_status="clean"
            )
            return 0, None
        _conflict(
            connection, use_id, cloud_user_id, remote,
            local=local, baseline=None, reason="remote use collides with unacknowledged local use"
        )
        return 0, f"observation_use:{use_id}"
    if local == remote:
        _write_state(
            connection, use_id, cloud_user_id, remote, row["row_version"],
            sync_status="clean"
        )
        return 0, None
    if local == baseline:
        _apply_payload(connection, use_id, observation_id, remote, row)
        _write_state(
            connection, use_id, cloud_user_id, remote, row["row_version"],
            sync_status="clean"
        )
        return 1, None
    if remote == baseline:
        _write_state(
            connection, use_id, cloud_user_id, remote, row["row_version"],
            sync_status="dirty"
        )
        return 0, None

    local_changed = _changed(local, baseline)
    remote_changed = _changed(remote, baseline)
    if (
        local_changed.isdisjoint(remote_changed)
        and local_changed | remote_changed <= _MERGEABLE_FIELDS
    ):
        merged = dict(remote)
        for field in local_changed:
            merged[field] = local[field]
        _apply_payload(connection, use_id, observation_id, merged, row)
        _write_state(
            connection, use_id, cloud_user_id, remote, row["row_version"],
            sync_status="dirty"
        )
        return 1, None
    _conflict(
        connection, use_id, cloud_user_id, remote,
        local=local, baseline=baseline, reason="overlapping observation-use edits"
    )
    return 0, f"observation_use:{use_id}"


def _reconcile_tombstone(
    connection: sqlite3.Connection, cloud_user_id: str, row: dict[str, Any]
) -> tuple[int, str | None]:
    use_id = str(row["id"])
    remote = row["_payload"]
    pending = connection.execute(
        "SELECT * FROM observation_reference_use_cloud_tombstones WHERE use_id=?",
        (use_id,),
    ).fetchone()
    if pending is not None:
        if pending["cloud_user_id"] != cloud_user_id:
            connection.execute(
                """
                UPDATE observation_reference_use_cloud_tombstones
                SET sync_status='conflict', conflict_json=?, last_error=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE use_id=?
                """,
                (
                    _canonical_json(
                        {
                            "operation": "pull_tombstone",
                            "reason": "observation-use account mismatch",
                            "remote": remote,
                        }
                    ),
                    "observation-use account mismatch",
                    use_id,
                ),
            )
            return 0, f"observation_use:{use_id}"
        if pending["sync_status"] == "conflict":
            return 0, f"observation_use:{use_id}"
        connection.execute(
            "DELETE FROM observation_reference_use_cloud_tombstones WHERE use_id=?",
            (use_id,),
        )
        connection.execute(
            """
            INSERT INTO observation_reference_use_cloud_remote_tombstone_markers(
                cloud_user_id, use_id, cloud_row_version,
                accepted_payload_json, deleted_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(cloud_user_id, use_id) DO UPDATE SET
                cloud_row_version=excluded.cloud_row_version,
                accepted_payload_json=excluded.accepted_payload_json,
                deleted_at=excluded.deleted_at
            """,
            (cloud_user_id, use_id, row["row_version"], _canonical_json(remote), row["deleted_at"]),
        )
        return 1, None

    local = _local_payload(connection, use_id)
    state = connection.execute(
        "SELECT * FROM observation_reference_use_cloud_sync_state WHERE use_id=?",
        (use_id,),
    ).fetchone()
    if local is not None and state is not None:
        if state["cloud_user_id"] not in {None, cloud_user_id}:
            _conflict(
                connection,
                use_id,
                cloud_user_id,
                remote,
                local=local,
                baseline=_parse_json(state["accepted_payload_json"]),
                reason="observation-use account mismatch",
            )
            return 0, f"observation_use:{use_id}"
        baseline = _parse_json(state["accepted_payload_json"])
        if baseline == remote:
            # A same-ID local reattach is an explicit restore of this remote
            # tombstone. Preserve it as dirty and retain the authoritative
            # deleted-row token for the current-mode restore RPC.
            _write_state(
                connection,
                use_id,
                cloud_user_id,
                remote,
                row["row_version"],
                sync_status="dirty",
            )
            return 0, None
        if state["sync_status"] == "conflict" or baseline is None or local != baseline:
            _conflict(
                connection, use_id, cloud_user_id, remote,
                local=local, baseline=baseline,
                reason="remote deleted use with unacknowledged local intent"
            )
            return 0, f"observation_use:{use_id}"
        connection.execute("DELETE FROM observation_reference_uses WHERE id=?", (use_id,))
        connection.execute(
            "DELETE FROM observation_reference_use_cloud_tombstones WHERE use_id=?",
            (use_id,),
        )
    connection.execute(
        """
        INSERT INTO observation_reference_use_cloud_remote_tombstone_markers(
            cloud_user_id, use_id, cloud_row_version,
            accepted_payload_json, deleted_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(cloud_user_id, use_id) DO UPDATE SET
            cloud_row_version=excluded.cloud_row_version,
            accepted_payload_json=excluded.accepted_payload_json,
            deleted_at=excluded.deleted_at
        """,
        (cloud_user_id, use_id, row["row_version"], _canonical_json(remote), row["deleted_at"]),
    )
    return (1 if local is not None else 0), None


def reconcile_observation_reference_use_feed(
    cloud_user_id: str,
    feed: StagedObservationReferenceUseFeed,
    *,
    advance_cursor: bool = True,
    connection: sqlite3.Connection | None = None,
    manage_transaction: bool = True,
    apply_live: bool = True,
    apply_tombstones: bool = True,
    remote_live_set_ids: set[str] | None = None,
) -> ReferencePullApplyResult:
    """Reconcile uses after their library dependencies have converged."""
    owned_connection = connection is None
    if connection is None:
        connection = database_schema.get_connection()
        connection.row_factory = sqlite3.Row
        init_observation_reference_uses_schema(connection)
    applied = 0
    conflicts: list[str] = []
    blocked: list[str] = []
    try:
        if manage_transaction:
            connection.execute("BEGIN IMMEDIATE")
        if apply_live:
            _claim_restored_uses(connection, cloud_user_id)
        if apply_live:
            for row in sorted(
                (item for item in feed.rows if not item["deleted_at"]),
                key=lambda item: item["id"],
            ):
                payload = row["_payload"]
                observation_id = _observation_id_for_cloud(
                    connection, payload["observation_id"]
                )
                if observation_id is None:
                    blocked.append(f"observation_use:{row['id']}:missing_observation")
                    continue
                if (
                    remote_live_set_ids is not None
                    and payload["reference_measurement_set_id"]
                    not in remote_live_set_ids
                ):
                    blocked.append(
                        f"observation_use:{row['id']}:"
                        "missing_reference_measurement_set"
                    )
                    continue
                dependency = _source_ready(
                    cloud_user_id,
                    payload["reference_measurement_set_id"],
                    connection=connection if not owned_connection else None,
                )
                if dependency is not None:
                    blocked.append(f"observation_use:{row['id']}:{dependency}")
                    continue
                count, conflict = _reconcile_live(
                    connection, cloud_user_id, row, observation_id
                )
                applied += count
                if conflict:
                    conflicts.append(conflict)
        if apply_tombstones:
            for row in sorted(
                (item for item in feed.rows if item["deleted_at"]),
                key=lambda item: item["id"],
            ):
                count, conflict = _reconcile_tombstone(connection, cloud_user_id, row)
                applied += count
                if conflict:
                    conflicts.append(conflict)
        if advance_cursor and not conflicts and not blocked:
            _advance_cursor(connection, cloud_user_id, feed)
        if manage_transaction:
            connection.commit()
    except sqlite3.Error as exc:
        if manage_transaction:
            connection.rollback()
        raise ReferencePullReconciliationError(
            "observation-use reconciliation failed"
        ) from exc
    except Exception:
        if manage_transaction:
            connection.rollback()
        raise
    finally:
        if owned_connection:
            connection.close()
    return ReferencePullApplyResult(
        applied=applied,
        conflicts=tuple(sorted(set(conflicts))),
        blocked=tuple(sorted(set(blocked))),
    )


def _advance_cursor(
    connection: sqlite3.Connection,
    cloud_user_id: str,
    feed: StagedObservationReferenceUseFeed,
) -> None:
    if not feed.rows:
        return
    cursor_row = max(feed.rows, key=lambda item: (item["updated_at"], item["id"]))
    connection.execute(
        """
        INSERT INTO observation_reference_use_cloud_pull_cursors(
            cloud_user_id, updated_at, use_id
        ) VALUES (?, ?, ?)
        ON CONFLICT(cloud_user_id) DO UPDATE SET
            updated_at=excluded.updated_at, use_id=excluded.use_id
        WHERE (excluded.updated_at, excluded.use_id) > (updated_at, use_id)
        """,
        (cloud_user_id, cursor_row["updated_at"], cursor_row["id"]),
    )
