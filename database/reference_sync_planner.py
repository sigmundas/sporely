"""Pure deterministic planning for normalized reference synchronization.

Stage 4c plans durable local work only. It performs no network operations and
does not mutate either database.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from database.schema import get_connection, get_reference_connection


_LIVE_RANK = {
    "work": 0,
    "treatment": 1,
    "measurement_set": 2,
    "observation_use": 3,
}
_TOMBSTONE_RANK = {
    "observation_use": 0,
    "measurement_set": 1,
    "treatment": 2,
    "work": 3,
}


@dataclass(frozen=True, slots=True)
class ReferenceSyncLiveNode:
    entity_type: str
    entity_id: str
    remote_identity_state: str
    sync_status: str
    cloud_user_id: str | None = None
    parent_id: str | None = None
    observation_cloud_id: str | None = None
    cloud_row_version: int | None = None


@dataclass(frozen=True, slots=True)
class ReferenceSyncTombstoneNode:
    entity_type: str
    entity_id: str
    cloud_user_id: str
    remote_identity_state: str
    sync_status: str = "dirty"
    parent_id: str | None = None
    observation_cloud_id: str | None = None
    expected_row_version: int | None = None


@dataclass(frozen=True, slots=True)
class ReferenceSyncGraphSnapshot:
    live_nodes: tuple[ReferenceSyncLiveNode, ...] = ()
    tombstones: tuple[ReferenceSyncTombstoneNode, ...] = ()


@dataclass(frozen=True, slots=True)
class ReferenceSyncPlanItem:
    operation: str
    entity_type: str
    entity_id: str
    remote_identity_state: str
    parent_id: str | None
    observation_cloud_id: str | None = None
    expected_row_version: int | None = None
    blocked_reason: str | None = None


@dataclass(frozen=True, slots=True)
class ReferenceSyncPlan:
    live: tuple[ReferenceSyncPlanItem, ...]
    tombstones: tuple[ReferenceSyncPlanItem, ...]


def load_reference_sync_graph() -> ReferenceSyncGraphSnapshot:
    """Read one durable graph snapshot from the two owning databases."""
    reference = get_reference_connection()
    reference.row_factory = sqlite3.Row
    try:
        live_rows = reference.execute(
            """
            SELECT state.*,
                   CASE state.entity_type
                     WHEN 'treatment' THEN treatment.reference_work_id
                     WHEN 'measurement_set' THEN measurement_set.taxon_treatment_id
                   END AS parent_id
            FROM reference_cloud_sync_state AS state
            LEFT JOIN reference_taxon_treatments AS treatment
              ON state.entity_type='treatment' AND treatment.id=state.entity_id
            LEFT JOIN reference_measurement_sets AS measurement_set
              ON state.entity_type='measurement_set'
             AND measurement_set.id=state.entity_id
            """
        ).fetchall()
        tombstone_rows = reference.execute(
            "SELECT * FROM reference_cloud_tombstones"
        ).fetchall()
    finally:
        reference.close()

    observation = get_connection()
    observation.row_factory = sqlite3.Row
    try:
        use_rows = observation.execute(
            """
            SELECT state.*, use_row.reference_measurement_set_id AS parent_id,
                   NULLIF(TRIM(observation.cloud_id), '') AS observation_cloud_id
            FROM observation_reference_use_cloud_sync_state AS state
            JOIN observation_reference_uses AS use_row ON use_row.id=state.use_id
            LEFT JOIN observations AS observation ON observation.id=use_row.observation_id
            """
        ).fetchall()
        use_tombstone_rows = observation.execute(
            "SELECT * FROM observation_reference_use_cloud_tombstones"
        ).fetchall()
    finally:
        observation.close()

    live = [
        ReferenceSyncLiveNode(
            entity_type=str(row["entity_type"]),
            entity_id=str(row["entity_id"]),
            cloud_user_id=row["cloud_user_id"],
            remote_identity_state=str(row["remote_identity_state"]),
            sync_status=str(row["sync_status"]),
            parent_id=row["parent_id"],
            cloud_row_version=row["cloud_row_version"],
        )
        for row in live_rows
    ]
    live.extend(
        ReferenceSyncLiveNode(
            entity_type="observation_use",
            entity_id=str(row["use_id"]),
            cloud_user_id=row["cloud_user_id"],
            remote_identity_state=str(row["remote_identity_state"]),
            sync_status=str(row["sync_status"]),
            parent_id=str(row["parent_id"]),
            observation_cloud_id=row["observation_cloud_id"],
            cloud_row_version=row["cloud_row_version"],
        )
        for row in use_rows
    )
    tombstones = [
        ReferenceSyncTombstoneNode(
            entity_type=str(row["entity_type"]),
            entity_id=str(row["entity_id"]),
            cloud_user_id=str(row["cloud_user_id"]),
            remote_identity_state=str(row["remote_identity_state"]),
            sync_status=str(row["sync_status"]),
            parent_id=(
                row["reference_work_id"]
                if row["entity_type"] == "treatment"
                else row["taxon_treatment_id"]
            ),
            expected_row_version=row["expected_row_version"],
        )
        for row in tombstone_rows
    ]
    tombstones.extend(
        ReferenceSyncTombstoneNode(
            entity_type="observation_use",
            entity_id=str(row["use_id"]),
            cloud_user_id=str(row["cloud_user_id"]),
            remote_identity_state=str(row["remote_identity_state"]),
            sync_status=str(row["sync_status"]),
            parent_id=str(row["reference_measurement_set_id"]),
            observation_cloud_id=row["observation_cloud_id"],
            expected_row_version=row["expected_row_version"],
        )
        for row in use_tombstone_rows
    )
    return ReferenceSyncGraphSnapshot(tuple(live), tuple(tombstones))


def build_reference_sync_plan(cloud_user_id: str) -> ReferenceSyncPlan:
    """Re-read durable state and produce a fresh deterministic plan."""
    return plan_reference_sync(load_reference_sync_graph(), cloud_user_id)


def _account_blocked(bound_user_id: str | None, cloud_user_id: str) -> bool:
    return bool(bound_user_id and bound_user_id != cloud_user_id)


def plan_reference_sync(
    snapshot: ReferenceSyncGraphSnapshot,
    cloud_user_id: str,
) -> ReferenceSyncPlan:
    """Return a stable plan without reading, writing, or contacting cloud."""
    account_id = str(cloud_user_id or "").strip()
    if not account_id:
        raise ValueError("cloud account is required")

    live_by_key = {
        (node.entity_type, node.entity_id): node for node in snapshot.live_nodes
    }
    pending_live: list[ReferenceSyncPlanItem] = []
    for node in snapshot.live_nodes:
        account_mismatch = _account_blocked(node.cloud_user_id, account_id)
        if (
            node.sync_status == "clean"
            and node.remote_identity_state == "acknowledged"
            and not account_mismatch
        ):
            continue
        blocked: str | None = None
        if account_mismatch:
            blocked = "account_mismatch"
        elif node.sync_status == "conflict":
            blocked = "conflict"
        elif node.parent_id:
            parent_kind = {
                "treatment": "work",
                "measurement_set": "treatment",
                "observation_use": "measurement_set",
            }.get(node.entity_type)
            parent = live_by_key.get((str(parent_kind), node.parent_id))
            if parent is None:
                blocked = "missing_dependency"
            elif _account_blocked(parent.cloud_user_id, account_id):
                blocked = "dependency_account_mismatch"
            elif parent.sync_status == "conflict":
                blocked = "parent_conflict"
            elif parent.remote_identity_state != "acknowledged":
                blocked = "parent_not_acknowledged"
        if (
            blocked is None
            and node.entity_type == "observation_use"
            and not str(node.observation_cloud_id or "").strip()
        ):
            blocked = "missing_observation_cloud_id"
        pending_live.append(
            ReferenceSyncPlanItem(
                operation="reconcile_create"
                if node.remote_identity_state == "create_outcome_unknown"
                else "upsert",
                entity_type=node.entity_type,
                entity_id=node.entity_id,
                remote_identity_state=node.remote_identity_state,
                parent_id=node.parent_id,
                observation_cloud_id=node.observation_cloud_id,
                expected_row_version=node.cloud_row_version,
                blocked_reason=blocked,
            )
        )

    tombstone_children: set[tuple[str, str]] = set()
    for node in snapshot.tombstones:
        if node.parent_id:
            parent_kind = {
                "observation_use": "measurement_set",
                "measurement_set": "treatment",
                "treatment": "work",
            }.get(node.entity_type)
            if parent_kind:
                tombstone_children.add((parent_kind, node.parent_id))

    pending_tombstones: list[ReferenceSyncPlanItem] = []
    for node in snapshot.tombstones:
        blocked = None
        if _account_blocked(node.cloud_user_id, account_id):
            blocked = "account_mismatch"
        elif node.sync_status == "conflict":
            blocked = "conflict"
        elif (
            node.entity_type == "observation_use"
            and not str(node.observation_cloud_id or "").strip()
        ):
            blocked = "missing_observation_cloud_id"
        elif (node.entity_type, node.entity_id) in tombstone_children:
            blocked = "pending_descendant_tombstone"
        pending_tombstones.append(
            ReferenceSyncPlanItem(
                operation="reconcile_delete"
                if node.remote_identity_state == "create_outcome_unknown"
                else "tombstone",
                entity_type=node.entity_type,
                entity_id=node.entity_id,
                remote_identity_state=node.remote_identity_state,
                parent_id=node.parent_id,
                observation_cloud_id=node.observation_cloud_id,
                expected_row_version=node.expected_row_version,
                blocked_reason=blocked,
            )
        )

    return ReferenceSyncPlan(
        live=tuple(
            sorted(
                pending_live,
                key=lambda item: (_LIVE_RANK[item.entity_type], item.entity_id),
            )
        ),
        tombstones=tuple(
            sorted(
                pending_tombstones,
                key=lambda item: (_TOMBSTONE_RANK[item.entity_type], item.entity_id),
            )
        ),
    )
