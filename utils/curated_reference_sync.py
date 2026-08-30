"""Private cloud synchronization for immutable Stage 6k fork provenance."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from database.curated_reference_forks import validate_frozen_curated_provenance
from database.reference_library_schema import init_reference_library_schema
from database.schema import get_reference_connection


@dataclass(frozen=True, slots=True)
class CuratedForkSyncResult:
    pushed: int = 0
    pulled: int = 0
    errors: tuple[str, ...] = ()
    conflicts: tuple[str, ...] = ()


_PAYLOAD_KEYS = (
    "curated_measurement_set_id", "bundle_revision", "sporely_taxon_id",
    "reference_work_id", "taxon_treatment_id", "reference_measurement_set_id",
    "source_sha256", "source_envelope_json",
)


def _payload(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    return {key: row[key] for key in _PAYLOAD_KEYS}


def _valid_remote_row(row: object, cloud_user_id: str) -> dict[str, Any]:
    if not isinstance(row, dict) or str(row.get("user_id") or "") != cloud_user_id:
        raise ValueError("curated fork row belongs to another account")
    if set(_PAYLOAD_KEYS) - set(row):
        raise ValueError("curated fork row is incomplete")
    version = row.get("row_version")
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise ValueError("curated fork row has invalid row_version")
    return row


def _dependencies_match(
    connection: sqlite3.Connection,
    payload: dict[str, Any],
    *,
    converged_cloud_user_id: str | None = None,
) -> bool:
    graph_exists = connection.execute(
        "SELECT 1 FROM reference_measurement_sets m "
        "JOIN reference_taxon_treatments t ON t.id=m.taxon_treatment_id "
        "JOIN reference_works w ON w.id=t.reference_work_id "
        "WHERE w.id=? AND t.id=? AND m.id=?",
        (payload["reference_work_id"], payload["taxon_treatment_id"], payload["reference_measurement_set_id"]),
    ).fetchone() is not None
    if not graph_exists or converged_cloud_user_id is None:
        return graph_exists
    required = (
        ("work", payload["reference_work_id"]),
        ("treatment", payload["taxon_treatment_id"]),
        ("measurement_set", payload["reference_measurement_set_id"]),
    )
    return all(
        connection.execute(
            "SELECT 1 FROM reference_cloud_sync_state WHERE entity_type=? AND entity_id=? "
            "AND cloud_user_id=? AND remote_identity_state='acknowledged' AND sync_status='clean'",
            (entity_type, entity_id, converged_cloud_user_id),
        ).fetchone() is not None
        for entity_type, entity_id in required
    )


def push_curated_reference_forks(client: object) -> CuratedForkSyncResult:
    if not hasattr(client, "sync_reference_curated_fork"):
        return CuratedForkSyncResult()
    cloud_user_id = str(getattr(client, "user_id", "") or "").strip()
    if not cloud_user_id:
        return CuratedForkSyncResult(errors=("curated fork sync: missing cloud account",))
    connection = get_reference_connection()
    connection.row_factory = sqlite3.Row
    init_reference_library_schema(connection)
    pushed = 0
    errors: list[str] = []
    conflicts: list[str] = []
    try:
        rows = connection.execute(
            "SELECT f.*,s.cloud_user_id,s.cloud_row_version,s.sync_status "
            "FROM curated_reference_forks f JOIN curated_reference_fork_cloud_sync_state s "
            "USING(curated_measurement_set_id,bundle_revision) "
            "WHERE s.sync_status!='clean' ORDER BY f.created_at,f.curated_measurement_set_id,f.bundle_revision"
        ).fetchall()
        for row in rows:
            identity = f"{row['curated_measurement_set_id']}@{row['bundle_revision']}"
            if row["cloud_user_id"] not in (None, cloud_user_id):
                errors.append(f"curated fork {identity}: account mismatch")
                continue
            payload = _payload(row)
            if not _dependencies_match(
                connection, payload, converged_cloud_user_id=cloud_user_id
            ):
                errors.append(f"curated fork {identity}: private graph not converged")
                continue
            try:
                response = client.sync_reference_curated_fork(
                    payload, int(row["cloud_row_version"] or 0)
                )
            except Exception as exc:
                errors.append(f"curated fork {identity}: {exc}")
                continue
            if not isinstance(response, dict) or set(response) != {"status", "row"}:
                errors.append(f"curated fork {identity}: malformed response")
                continue
            status, remote = response["status"], response["row"]
            if status == "conflict":
                conflicts.append(identity)
                continue
            if status not in {"created", "no_change"}:
                errors.append(f"curated fork {identity}: remote status {status}")
                continue
            try:
                remote = _valid_remote_row(remote, cloud_user_id)
            except ValueError as exc:
                errors.append(f"curated fork {identity}: {exc}")
                continue
            if _payload(remote) != payload:
                conflicts.append(identity)
                continue
            connection.execute(
                "UPDATE curated_reference_fork_cloud_sync_state SET cloud_user_id=?,cloud_row_version=?,sync_status='clean',accepted_payload_json=?,last_error=NULL,updated_at=CURRENT_TIMESTAMP WHERE curated_measurement_set_id=? AND bundle_revision=?",
                (cloud_user_id, remote["row_version"], json.dumps(payload, sort_keys=True, separators=(",", ":")), row["curated_measurement_set_id"], row["bundle_revision"]),
            )
            connection.commit()
            pushed += 1
    finally:
        connection.close()
    return CuratedForkSyncResult(pushed=pushed, errors=tuple(errors), conflicts=tuple(conflicts))


def pull_curated_reference_forks(client: object) -> CuratedForkSyncResult:
    if not hasattr(client, "list_reference_curated_forks"):
        return CuratedForkSyncResult()
    cloud_user_id = str(getattr(client, "user_id", "") or "").strip()
    if not cloud_user_id:
        return CuratedForkSyncResult(errors=("curated fork pull: missing cloud account",))
    try:
        remote_rows = client.list_reference_curated_forks()
    except Exception as exc:
        return CuratedForkSyncResult(errors=(f"curated fork pull: {exc}",))
    if not isinstance(remote_rows, list) or len(remote_rows) > 10_000:
        return CuratedForkSyncResult(errors=("curated fork pull: invalid or oversized feed",))
    validated: list[tuple[dict[str, Any], dict[str, Any]]] = []
    try:
        for raw in remote_rows:
            remote = _valid_remote_row(raw, cloud_user_id)
            payload = _payload(remote)
            validate_frozen_curated_provenance(
                payload["source_envelope_json"], payload["source_sha256"],
                curated_measurement_set_id=payload["curated_measurement_set_id"],
                bundle_revision=payload["bundle_revision"],
                sporely_taxon_id=payload["sporely_taxon_id"],
            )
            validated.append((remote, payload))
    except Exception as exc:
        return CuratedForkSyncResult(errors=(f"curated fork pull: {exc}",))
    connection = get_reference_connection()
    connection.row_factory = sqlite3.Row
    init_reference_library_schema(connection)
    pulled = 0
    errors: list[str] = []
    conflicts: list[str] = []
    try:
        for remote, payload in validated:
            try:
                identity = f"{payload['curated_measurement_set_id']}@{payload['bundle_revision']}"
                if not _dependencies_match(connection, payload):
                    errors.append(f"curated fork {identity}: private graph not reconciled")
                    continue
                source_json = payload["source_envelope_json"]
                existing = connection.execute(
                    "SELECT * FROM curated_reference_forks WHERE curated_measurement_set_id=? AND bundle_revision=?",
                    (payload["curated_measurement_set_id"], payload["bundle_revision"]),
                ).fetchone()
                if existing is not None and _payload(existing) != payload:
                    conflicts.append(identity)
                    continue
                connection.execute("BEGIN IMMEDIATE")
                if existing is None:
                    connection.execute(
                        "INSERT INTO curated_reference_forks (curated_measurement_set_id,bundle_revision,sporely_taxon_id,reference_work_id,taxon_treatment_id,reference_measurement_set_id,source_envelope_json,source_sha256) VALUES (?,?,?,?,?,?,?,?)",
                        (
                            payload["curated_measurement_set_id"], payload["bundle_revision"],
                            payload["sporely_taxon_id"], payload["reference_work_id"],
                            payload["taxon_treatment_id"], payload["reference_measurement_set_id"],
                            source_json, payload["source_sha256"],
                        ),
                    )
                    pulled += 1
                connection.execute(
                    "UPDATE curated_reference_fork_cloud_sync_state SET cloud_user_id=?,cloud_row_version=?,sync_status='clean',accepted_payload_json=?,last_error=NULL,updated_at=CURRENT_TIMESTAMP WHERE curated_measurement_set_id=? AND bundle_revision=?",
                    (cloud_user_id, remote["row_version"], json.dumps(payload, sort_keys=True, separators=(",", ":")), payload["curated_measurement_set_id"], payload["bundle_revision"]),
                )
                connection.commit()
            except Exception as exc:
                connection.rollback()
                errors.append(f"curated fork pull: {exc}")
    finally:
        connection.close()
    return CuratedForkSyncResult(pulled=pulled, errors=tuple(errors), conflicts=tuple(conflicts))
