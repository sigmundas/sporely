"""Dormant normalized reference-library cloud-sync facade."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone

from database.reference_sync_planner import build_reference_sync_plan
from database.reference_sync_reconciliation import (
    ReferencePullReconciliationError,
    ReferencePullRetryableError,
    reconcile_reference_library_feed,
    stage_reference_library_feed,
)
from database.reference_use_sync_reconciliation import (
    stage_observation_reference_use_feed,
)
from database.reference_sync_state import (
    ObservationReferenceUseCloudTombstone,
    ReferenceCloudSyncStateError,
    ReferenceCloudSyncStateRepository,
    ReferenceCloudTombstone,
    canonical_library_payload,
    canonical_observation_use_payload,
    load_library_payload,
    load_use_payload,
)
from utils.reference_client_capabilities import (
    capability_fingerprint,
    hold_marker,
    parse_hold_marker,
    payload_digest,
    reference_client_capabilities,
    report_reference_client_capabilities,
)
from utils.reference_cloud_adapter import (
    ReferenceCloudAccountMismatchError,
    ReferenceCloudAdapter,
    ReferenceCloudProtocolError,
    ReferenceCloudTransportError,
    ReferenceRemoteMutationResult,
)


_LIBRARY_TYPES = frozenset({"work", "treatment", "measurement_set"})
_REFERENCE_TYPES = frozenset({*_LIBRARY_TYPES, "observation_use"})


def _attempted_at() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True, slots=True)
class ReferenceSyncResult:
    """Typed outcome returned by the normalized reference-sync subsystem."""

    pushed: int = 0
    pulled: int = 0
    errors: tuple[str, ...] = ()
    retryable_errors: tuple[str, ...] = ()
    terminal_errors: tuple[str, ...] = ()
    conflicts: tuple[str, ...] = ()
    blocked: tuple[str, ...] = ()
    #: ``entity:id:status`` of local changes the server refused for client
    #: capability (Stage M). Pending, not retried until the hold fingerprint
    #: changes; a notice, not an error.
    capability_holds: tuple[str, ...] = ()


#: Block reasons that only mean "the parent/predecessor has not converged
#: yet". A row blocked for one of these while its set is capability-held is
#: part of that hold; every other reason (account mismatch, missing
#: dependency, invalid cloud identity, ...) surfaces as itself.
_HOLD_INHERITING_BLOCK_REASONS = frozenset({
    "parent_not_acknowledged",
    "parent_not_converged",
    "superseded_set_not_acknowledged",
})


class _CapabilityHolds:
    """Per-push view of Stage M capability holds.

    A local row refused with ``requires_newer_client``/``older_client_active``
    keeps its pending change and records a hold marker (status, capability
    fingerprint, payload digest) in ``last_error``. It is skipped while both
    the fingerprint and its payload are unchanged; it is retried after an app
    upgrade, a change in the owner's blocking devices (an older device
    upgraded or aged out of the 30-day window) or a local edit of the row.
    The device list is read only when a hold exists.
    """

    def __init__(self, client: object) -> None:
        self._client = client
        self._fingerprint: str | None = None

    def fingerprint(self) -> str:
        if self._fingerprint is None:
            devices = None
            reader = getattr(self._client, "list_reference_client_devices", None)
            if callable(reader):
                try:
                    rows = reader()
                    devices = rows if isinstance(rows, list) else None
                except Exception:
                    devices = None
            self._fingerprint = capability_fingerprint(
                reference_client_capabilities(), devices
            )
        return self._fingerprint

    def marker(self, status: str, payload: object) -> str:
        return hold_marker(status, self.fingerprint(), payload)

    def _state_and_payload(self, entity_type: str, entity_id: str):
        try:
            if entity_type == "observation_use":
                return (
                    ReferenceCloudSyncStateRepository.get_use(entity_id),
                    load_use_payload(entity_id),
                )
            return (
                ReferenceCloudSyncStateRepository.get_library(entity_type, entity_id),
                load_library_payload(entity_type, entity_id),
            )
        except ReferenceCloudSyncStateError:
            return None, None

    def held_status(self, entity_type: str, entity_id: str) -> str | None:
        if entity_type not in {"measurement_set", "observation_use"}:
            return None
        state, payload = self._state_and_payload(entity_type, entity_id)
        if state is None or payload is None or state.sync_status == "conflict":
            return None
        parsed = parse_hold_marker(state.last_error)
        if parsed is None:
            return None
        status, fingerprint, digest = parsed
        if digest != payload_digest(payload) or fingerprint != self.fingerprint():
            return None
        return status

    def dependency_held_status(self, entity_type: str, entity_id: str) -> str | None:
        """Hold status of the set a pending row waits on, if that set is held."""
        try:
            if entity_type == "observation_use":
                payload = load_use_payload(entity_id)
                parent = str((payload or {}).get("reference_measurement_set_id") or "")
            elif entity_type == "measurement_set":
                payload = load_library_payload("measurement_set", entity_id)
                parent = str((payload or {}).get("supersedes_id") or "")
            else:
                return None
        except ReferenceCloudSyncStateError:
            return None
        return self.held_status("measurement_set", parent) if parent else None


def _record_capability_hold(
    entity_type: str, entity_id: str, status: str, payload: dict, holds: _CapabilityHolds
) -> None:
    marker = holds.marker(status, payload)
    if entity_type == "observation_use":
        state = ReferenceCloudSyncStateRepository.get_use(entity_id)
        save = ReferenceCloudSyncStateRepository.save_use
    else:
        state = ReferenceCloudSyncStateRepository.get_library(entity_type, entity_id)
        save = ReferenceCloudSyncStateRepository.save_library
    if state is None:
        return
    save(
        replace(
            state,
            sync_status="conflict" if state.sync_status == "conflict" else "retry",
            last_error=marker,
            last_attempted_at=_attempted_at(),
        )
    )


def _adapter_sync(adapter: ReferenceCloudAdapter, entity_type: str):
    return {
        "work": adapter.sync_work,
        "treatment": adapter.sync_treatment,
        "measurement_set": adapter.sync_measurement_set,
    }[entity_type]


def _adapter_list(adapter: ReferenceCloudAdapter, entity_type: str):
    return {
        "work": adapter.list_works,
        "treatment": adapter.list_treatments,
        "measurement_set": adapter.list_measurement_sets,
    }[entity_type]


def _diagnostic(
    *, operation: str, expected_row_version: int, payload: dict, row: dict | None
) -> dict:
    return {
        "operation": operation,
        "expected_row_version": expected_row_version,
        "payload": payload,
        "remote_row": row,
    }


def _record_live_failure(entity_type: str, entity_id: str, message: str) -> None:
    state = ReferenceCloudSyncStateRepository.get_library(entity_type, entity_id)
    if state is None:
        return
    ReferenceCloudSyncStateRepository.save_library(
        replace(
            state,
            sync_status="conflict" if state.sync_status == "conflict" else "retry",
            retry_count=state.retry_count + 1,
            last_error=message,
            last_attempted_at=_attempted_at(),
        )
    )


def _record_tombstone_failure(
    tombstone: ReferenceCloudTombstone, message: str
) -> None:
    ReferenceCloudSyncStateRepository.save_library_tombstone(
        replace(
            tombstone,
            sync_status=(
                "conflict" if tombstone.sync_status == "conflict" else "retry"
            ),
            retry_count=tombstone.retry_count + 1,
            last_error=message,
            last_attempted_at=_attempted_at(),
        )
    )


def _remote_row(
    adapter: ReferenceCloudAdapter, entity_type: str, entity_id: str
) -> dict | None:
    return next(
        (row for row in _adapter_list(adapter, entity_type)() if row["id"] == entity_id),
        None,
    )


def _canonical_remote_payload(entity_type: str, row: dict) -> dict:
    try:
        return canonical_library_payload(entity_type, row)
    except (KeyError, TypeError, ValueError, ReferenceCloudSyncStateError) as exc:
        raise ReferenceCloudProtocolError(
            "reference RPC row is missing its canonical baseline"
        ) from exc


def _canonical_remote_use_payload(row: dict) -> dict:
    try:
        return canonical_observation_use_payload(row)
    except (KeyError, TypeError, ValueError, ReferenceCloudSyncStateError) as exc:
        raise ReferenceCloudProtocolError(
            "observation-use row is missing its canonical baseline"
        ) from exc


def _record_use_failure(use_id: str, message: str) -> None:
    state = ReferenceCloudSyncStateRepository.get_use(use_id)
    if state is None:
        return
    ReferenceCloudSyncStateRepository.save_use(
        replace(
            state,
            sync_status="conflict" if state.sync_status == "conflict" else "retry",
            retry_count=state.retry_count + 1,
            last_error=message,
            last_attempted_at=_attempted_at(),
        )
    )


def _record_use_tombstone_failure(
    tombstone: ObservationReferenceUseCloudTombstone, message: str
) -> None:
    ReferenceCloudSyncStateRepository.save_use_tombstone(
        replace(
            tombstone,
            sync_status=(
                "conflict" if tombstone.sync_status == "conflict" else "retry"
            ),
            retry_count=tombstone.retry_count + 1,
            last_error=message,
            last_attempted_at=_attempted_at(),
        )
    )


def _executor_live_blocked(item, cloud_user_id: str) -> str | None:
    if item.blocked_reason is not None:
        return item.blocked_reason
    if item.entity_type != "measurement_set":
        return None
    payload = load_library_payload("measurement_set", item.entity_id)
    predecessor_id = str((payload or {}).get("supersedes_id") or "").strip()
    if not predecessor_id:
        return None
    predecessor = ReferenceCloudSyncStateRepository.get_library(
        "measurement_set", predecessor_id
    )
    if predecessor is None:
        return "missing_superseded_set"
    if predecessor.cloud_user_id not in {None, cloud_user_id}:
        return "superseded_set_account_mismatch"
    if predecessor.sync_status == "conflict":
        return "superseded_set_conflict"
    if predecessor.remote_identity_state != "acknowledged":
        return "superseded_set_not_acknowledged"
    return None


def _handle_live_result(
    result: ReferenceRemoteMutationResult,
    *,
    entity_type: str,
    entity_id: str,
    cloud_user_id: str,
    payload: dict,
    expected_row_version: int,
    holds: _CapabilityHolds | None = None,
) -> str:
    if result.disposition == "capability_hold" and holds is not None:
        _record_capability_hold(entity_type, entity_id, result.status, payload, holds)
        return f"held:{result.status}"
    if result.disposition == "acknowledged":
        accepted = _canonical_remote_payload(entity_type, result.row)
        ReferenceCloudSyncStateRepository.acknowledge_library(
            entity_type,
            entity_id,
            cloud_user_id,
            sent_payload=payload,
            accepted_payload=accepted,
            cloud_row_version=result.row["row_version"],
        )
        return "pushed"
    if result.disposition == "conflict":
        state = ReferenceCloudSyncStateRepository.get_library(entity_type, entity_id)
        if state is None:
            raise ReferenceCloudProtocolError(
                "library row disappeared while recording conflict"
            )
        ReferenceCloudSyncStateRepository.save_library(
            replace(
                state,
                sync_status="conflict",
                conflict=_diagnostic(
                    operation="upsert",
                    expected_row_version=expected_row_version,
                    payload=payload,
                    row=result.row,
                ),
                last_error="remote compare-and-set conflict",
                last_attempted_at=_attempted_at(),
            )
        )
        return "conflict"
    _record_live_failure(entity_type, entity_id, f"remote status: {result.status}")
    return (
        f"blocked:{result.status}"
        if result.disposition == "blocked"
        else f"error:{result.status}"
    )


def _execute_live(
    adapter: ReferenceCloudAdapter,
    cloud_user_id: str,
    entity_type: str,
    entity_id: str,
    holds: _CapabilityHolds | None = None,
) -> str:
    payload = load_library_payload(entity_type, entity_id)
    state = ReferenceCloudSyncStateRepository.get_library(entity_type, entity_id)
    if payload is None or state is None:
        raise ReferenceCloudProtocolError("planned library row is missing")
    if (
        state.remote_identity_state == "acknowledged"
        and state.accepted_payload == payload
        and ReferenceCloudSyncStateRepository.clean_library_if_unchanged(
            entity_type, entity_id, payload
        )
    ):
        return "noop"

    expected = state.cloud_row_version or 0
    if state.remote_identity_state == "never_attempted":
        ReferenceCloudSyncStateRepository.prepare_library_create(
            entity_type, entity_id, cloud_user_id
        )
    elif state.remote_identity_state == "create_outcome_unknown":
        remote = _remote_row(adapter, entity_type, entity_id)
        if remote is not None:
            expected = remote["row_version"]
            remote_payload = _canonical_remote_payload(entity_type, remote)
            if remote_payload == payload:
                ReferenceCloudSyncStateRepository.acknowledge_library(
                    entity_type,
                    entity_id,
                    cloud_user_id,
                    sent_payload=payload,
                    accepted_payload=remote_payload,
                    cloud_row_version=expected,
                )
                return "noop"

    result = _adapter_sync(adapter, entity_type)(payload, expected)
    return _handle_live_result(
        result,
        entity_type=entity_type,
        entity_id=entity_id,
        cloud_user_id=cloud_user_id,
        payload=payload,
        expected_row_version=expected,
        holds=holds,
    )


#: The server's sync RPCs check the parent before the tombstone branch
#: (``sync_reference_taxon_treatment_unthrottled`` /
#: ``sync_reference_measurement_set_unthrottled``: no live parent named in the
#: payload -> ``invalid_parent``), so a tombstone carries its unchanged
#: parent identity. Found by the local Supabase harness.
_TOMBSTONE_PARENT_KEY = {
    "treatment": "reference_work_id",
    "measurement_set": "taxon_treatment_id",
}


def _library_tombstone_payload(tombstone: ReferenceCloudTombstone) -> dict:
    payload = {"id": tombstone.entity_id, "deleted": True}
    key = _TOMBSTONE_PARENT_KEY.get(tombstone.entity_type)
    if key is not None:
        parent = getattr(tombstone, key, None) or (tombstone.accepted_payload or {}).get(key)
        if not str(parent or "").strip():
            raise ReferenceCloudProtocolError("tombstone has no acknowledged parent identity")
        payload[key] = str(parent)
    return payload


def _execute_tombstone(
    adapter: ReferenceCloudAdapter,
    cloud_user_id: str,
    tombstone: ReferenceCloudTombstone,
) -> str:
    current = tombstone
    if current.remote_identity_state == "create_outcome_unknown":
        remote = _remote_row(adapter, current.entity_type, current.entity_id)
        if remote is None or remote.get("deleted_at"):
            ReferenceCloudSyncStateRepository.resolve_library_tombstone(
                current.entity_type, current.entity_id, cloud_user_id
            )
            return "noop"
        current = ReferenceCloudSyncStateRepository.save_library_tombstone(
            replace(
                current,
                remote_identity_state="acknowledged",
                expected_row_version=remote["row_version"],
                accepted_payload=_canonical_remote_payload(current.entity_type, remote),
                sync_status="dirty",
                retry_count=0,
                last_error=None,
            )
        )

    expected = current.expected_row_version
    if not expected:
        raise ReferenceCloudProtocolError("tombstone has no acknowledged row version")
    payload = _library_tombstone_payload(current)
    result = _adapter_sync(adapter, current.entity_type)(payload, expected)
    if result.disposition == "acknowledged":
        accepted_payload = _canonical_remote_payload(current.entity_type, result.row)
        ReferenceCloudSyncStateRepository.acknowledge_library_tombstone(
            current.entity_type,
            current.entity_id,
            cloud_user_id,
            accepted_payload=accepted_payload,
            cloud_row_version=result.row["row_version"],
        )
        return "pushed"
    if result.disposition == "conflict":
        ReferenceCloudSyncStateRepository.save_library_tombstone(
            replace(
                current,
                sync_status="conflict",
                conflict=_diagnostic(
                    operation="tombstone",
                    expected_row_version=expected,
                    payload=payload,
                    row=result.row,
                ),
                last_error="remote compare-and-set conflict",
                last_attempted_at=_attempted_at(),
            )
        )
        return "conflict"
    _record_tombstone_failure(current, f"remote status: {result.status}")
    return (
        f"blocked:{result.status}"
        if result.disposition == "blocked"
        else f"error:{result.status}"
    )


def _executor_use_blocked(item, cloud_user_id: str) -> str | None:
    if item.blocked_reason is not None:
        return item.blocked_reason
    try:
        payload = load_use_payload(item.entity_id)
    except ReferenceCloudSyncStateError:
        return "invalid_observation_cloud_id"
    if payload is None:
        return "missing_use"
    parent = ReferenceCloudSyncStateRepository.get_library(
        "measurement_set", str(payload["reference_measurement_set_id"])
    )
    if parent is None:
        return "missing_dependency"
    if parent.cloud_user_id not in {None, cloud_user_id}:
        return "dependency_account_mismatch"
    if parent.sync_status == "conflict":
        return "parent_conflict"
    if parent.remote_identity_state != "acknowledged":
        return "parent_not_acknowledged"
    if parent.sync_status != "clean":
        return "parent_not_converged"
    return None


def _handle_use_live_result(
    result: ReferenceRemoteMutationResult,
    *,
    use_id: str,
    cloud_user_id: str,
    payload: dict,
    expected_row_version: int,
    holds: _CapabilityHolds | None = None,
) -> str:
    if result.disposition == "capability_hold" and holds is not None:
        _record_capability_hold("observation_use", use_id, result.status, payload, holds)
        return f"held:{result.status}"
    if result.disposition == "acknowledged":
        accepted = _canonical_remote_use_payload(result.row)
        ReferenceCloudSyncStateRepository.acknowledge_use(
            use_id,
            cloud_user_id,
            sent_payload=payload,
            accepted_payload=accepted,
            cloud_row_version=result.row["row_version"],
        )
        return "pushed"
    if result.disposition == "conflict":
        state = ReferenceCloudSyncStateRepository.get_use(use_id)
        if state is None:
            raise ReferenceCloudProtocolError(
                "observation use disappeared while recording conflict"
            )
        ReferenceCloudSyncStateRepository.save_use(
            replace(
                state,
                sync_status="conflict",
                conflict=_diagnostic(
                    operation="upsert",
                    expected_row_version=expected_row_version,
                    payload=payload,
                    row=result.row,
                ),
                last_error="remote compare-and-set conflict",
                last_attempted_at=_attempted_at(),
            )
        )
        return "conflict"
    _record_use_failure(use_id, f"remote status: {result.status}")
    return (
        f"blocked:{result.status}"
        if result.disposition == "blocked"
        else f"error:{result.status}"
    )


def _execute_use_live(
    adapter: ReferenceCloudAdapter,
    cloud_user_id: str,
    use_id: str,
    holds: _CapabilityHolds | None = None,
) -> str:
    payload = load_use_payload(use_id)
    state = ReferenceCloudSyncStateRepository.get_use(use_id)
    if payload is None or state is None:
        raise ReferenceCloudProtocolError("planned observation use is missing")
    if (
        state.remote_identity_state == "acknowledged"
        and state.accepted_payload == payload
        and ReferenceCloudSyncStateRepository.clean_use_if_unchanged(
            use_id, payload
        )
    ):
        return "noop"

    expected = state.cloud_row_version or 0
    snapshot_mode = "current" if expected else "historical_import"
    if state.remote_identity_state == "never_attempted":
        ReferenceCloudSyncStateRepository.prepare_use_create(use_id, cloud_user_id)
    elif state.remote_identity_state == "create_outcome_unknown":
        remote = next(
            (
                row
                for row in adapter.list_observation_uses()
                if str(row.get("id")) == use_id
            ),
            None,
        )
        if remote is not None:
            remote_payload = _canonical_remote_use_payload(remote)
            if remote_payload == payload:
                ReferenceCloudSyncStateRepository.acknowledge_use(
                    use_id,
                    cloud_user_id,
                    sent_payload=payload,
                    accepted_payload=remote_payload,
                    cloud_row_version=remote["row_version"],
                )
                return "noop"
            state = ReferenceCloudSyncStateRepository.get_use(use_id)
            ReferenceCloudSyncStateRepository.save_use(
                replace(
                    state,
                    sync_status="conflict",
                    conflict=_diagnostic(
                        operation="reconcile_create",
                        expected_row_version=0,
                        payload=payload,
                        row=remote,
                    ),
                    last_error="remote create outcome diverged",
                    last_attempted_at=_attempted_at(),
                )
            )
            return "conflict"

    result = adapter.sync_observation_use(
        payload, expected, snapshot_mode=snapshot_mode
    )
    return _handle_use_live_result(
        result,
        use_id=use_id,
        cloud_user_id=cloud_user_id,
        payload=payload,
        expected_row_version=expected,
        holds=holds,
    )


def _execute_use_tombstone(
    adapter: ReferenceCloudAdapter,
    cloud_user_id: str,
    tombstone: ObservationReferenceUseCloudTombstone,
) -> str:
    current = tombstone
    remote = next(
        (
            row
            for row in adapter.list_observation_uses()
            if str(row.get("id")) == current.use_id
        ),
        None,
    )
    if remote is None:
        # A complete owner read proves the delete already happened (including
        # a parent-observation cascade whose response was acknowledged before
        # the desktop could clear its child intents).
        ReferenceCloudSyncStateRepository.resolve_use_tombstone(
            current.use_id, cloud_user_id
        )
        return "noop"
    remote_payload = _canonical_remote_use_payload(remote)
    if remote.get("deleted_at"):
        ReferenceCloudSyncStateRepository.resolve_use_tombstone(
            current.use_id, cloud_user_id
        )
        ReferenceCloudSyncStateRepository.save_use_remote_tombstone(
            cloud_user_id,
            current.use_id,
            cloud_row_version=remote["row_version"],
            accepted_payload=remote_payload,
            deleted_at=remote["deleted_at"],
        )
        return "noop"
    if current.remote_identity_state == "create_outcome_unknown":
        if (
            str(remote_payload["observation_id"])
            != str(current.observation_cloud_id or "").strip()
            or remote_payload["reference_measurement_set_id"]
            != current.reference_measurement_set_id
        ):
            ReferenceCloudSyncStateRepository.save_use_tombstone(
                replace(
                    current,
                    sync_status="conflict",
                    conflict=_diagnostic(
                        operation="reconcile_delete",
                        expected_row_version=0,
                        payload={"id": current.use_id, "deleted": True},
                        row=remote,
                    ),
                    last_error="remote create identity diverged",
                    last_attempted_at=_attempted_at(),
                )
            )
            return "conflict"
        current = ReferenceCloudSyncStateRepository.save_use_tombstone(
            replace(
                current,
                remote_identity_state="acknowledged",
                expected_row_version=remote["row_version"],
                accepted_payload=remote_payload,
                sync_status="dirty",
                conflict=None,
                retry_count=0,
                last_error=None,
            )
        )
    elif (
        str(remote_payload["observation_id"])
        != str(current.observation_cloud_id or "").strip()
        or remote_payload["reference_measurement_set_id"]
        != current.reference_measurement_set_id
    ):
        ReferenceCloudSyncStateRepository.save_use_tombstone(
            replace(
                current,
                sync_status="conflict",
                conflict=_diagnostic(
                    operation="delete_identity",
                    expected_row_version=current.expected_row_version or 0,
                    payload={"id": current.use_id, "deleted": True},
                    row=remote,
                ),
                last_error="remote observation-use identity diverged",
                last_attempted_at=_attempted_at(),
            )
        )
        return "conflict"
    expected = current.expected_row_version
    if not expected:
        raise ReferenceCloudProtocolError(
            "observation-use tombstone has no acknowledged row version"
        )
    payload = {"id": current.use_id, "deleted": True}
    result = adapter.sync_observation_use(
        payload, expected, snapshot_mode="current"
    )
    if result.disposition == "acknowledged":
        ReferenceCloudSyncStateRepository.acknowledge_use_tombstone(
            current.use_id,
            cloud_user_id,
            accepted_payload=_canonical_remote_use_payload(result.row),
            cloud_row_version=result.row["row_version"],
            deleted_at=result.row["deleted_at"],
        )
        return "pushed"
    if result.disposition == "conflict":
        ReferenceCloudSyncStateRepository.save_use_tombstone(
            replace(
                current,
                sync_status="conflict",
                conflict=_diagnostic(
                    operation="tombstone",
                    expected_row_version=expected,
                    payload=payload,
                    row=result.row,
                ),
                last_error="remote compare-and-set conflict",
                last_attempted_at=_attempted_at(),
            )
        )
        return "conflict"
    _record_use_tombstone_failure(current, f"remote status: {result.status}")
    return (
        f"blocked:{result.status}"
        if result.disposition == "blocked"
        else f"error:{result.status}"
    )


def pull_reference_library(client: object) -> ReferenceSyncResult:
    """Stage all four owner feeds, then reconcile dependencies before uses."""
    cloud_user_id = str(getattr(client, "user_id", "") or "").strip()
    adapter = ReferenceCloudAdapter(client, cloud_user_id)
    try:
        works = adapter.list_works()
        treatments = adapter.list_treatments()
        measurement_sets = adapter.list_measurement_sets()
        observation_uses = adapter.list_observation_uses()
        library_feed = stage_reference_library_feed(
            cloud_user_id, works, treatments, measurement_sets
        )
        use_feed = stage_observation_reference_use_feed(
            cloud_user_id, tuple(observation_uses)
        )
        applied = reconcile_reference_library_feed(
            cloud_user_id,
            library_feed,
            observation_use_feed=use_feed,
        )
    except ReferenceCloudTransportError as exc:
        message = f"reference pull: {exc}"
        return ReferenceSyncResult(
            errors=(message,),
            retryable_errors=(message,) if exc.retryable else (),
            terminal_errors=() if exc.retryable else (message,),
        )
    except ReferencePullRetryableError as exc:
        message = f"reference pull: {exc}"
        return ReferenceSyncResult(
            errors=(message,), retryable_errors=(message,), blocked=("reference_graph",)
        )
    except (
        ReferenceCloudProtocolError,
        ReferenceCloudAccountMismatchError,
        ReferencePullReconciliationError,
        ReferenceCloudSyncStateError,
    ) as exc:
        message = f"reference pull: {exc}"
        return ReferenceSyncResult(errors=(message,), terminal_errors=(message,))
    from utils.curated_reference_sync import pull_curated_reference_forks
    forks = pull_curated_reference_forks(client)
    return ReferenceSyncResult(
        pulled=applied.applied + forks.pulled,
        errors=forks.errors,
        terminal_errors=forks.errors,
        conflicts=applied.conflicts + forks.conflicts,
        blocked=applied.blocked,
    )


def _skip_held(
    holds: _CapabilityHolds,
    candidate,
    attempted: set[tuple[str, str, str]],
    capability_holds: list[str],
) -> bool:
    """Skip (and report once) a candidate whose capability hold still applies."""
    status = holds.held_status(candidate.entity_type, candidate.entity_id)
    if status is None:
        return False
    attempted.add(("live", candidate.entity_type, candidate.entity_id))
    capability_holds.append(f"{candidate.entity_type}:{candidate.entity_id}:{status}")
    return True


def _push_reference_library(client: object) -> ReferenceSyncResult:
    """Execute deterministic library and observation-use graph pushes."""
    cloud_user_id = str(getattr(client, "user_id", "") or "").strip()
    adapter = ReferenceCloudAdapter(client, cloud_user_id)
    ReferenceCloudSyncStateRepository.claim_library_restores(cloud_user_id)
    ReferenceCloudSyncStateRepository.claim_use_restores(cloud_user_id)

    pushed = 0
    errors: list[str] = []
    retryable_errors: list[str] = []
    terminal_errors: list[str] = []
    conflicts: list[str] = []
    blocked: list[str] = []
    capability_holds: list[str] = []
    attempted: set[tuple[str, str, str]] = set()
    holds = _CapabilityHolds(client)

    while True:
        plan = build_reference_sync_plan(cloud_user_id)
        item = next(
            (
                candidate for candidate in plan.live
                if candidate.entity_type in _REFERENCE_TYPES
                and (
                    _executor_use_blocked(candidate, cloud_user_id)
                    if candidate.entity_type == "observation_use"
                    else _executor_live_blocked(candidate, cloud_user_id)
                ) is None
                and ("live", candidate.entity_type, candidate.entity_id) not in attempted
                and not _skip_held(holds, candidate, attempted, capability_holds)
            ),
            None,
        )
        if item is None:
            break
        attempted.add(("live", item.entity_type, item.entity_id))
        try:
            outcome = (
                _execute_use_live(adapter, cloud_user_id, item.entity_id, holds)
                if item.entity_type == "observation_use"
                else _execute_live(
                    adapter, cloud_user_id, item.entity_type, item.entity_id, holds
                )
            )
        except ReferenceCloudTransportError as exc:
            if item.entity_type == "observation_use":
                _record_use_failure(item.entity_id, str(exc))
            else:
                _record_live_failure(item.entity_type, item.entity_id, str(exc))
            message = f"{item.entity_type}:{item.entity_id}: {exc}"
            errors.append(message)
            (retryable_errors if exc.retryable else terminal_errors).append(message)
            continue
        except (ReferenceCloudProtocolError, ReferenceCloudAccountMismatchError) as exc:
            if item.entity_type == "observation_use":
                _record_use_failure(item.entity_id, str(exc))
            else:
                _record_live_failure(item.entity_type, item.entity_id, str(exc))
            message = f"{item.entity_type}:{item.entity_id}: {exc}"
            errors.append(message)
            terminal_errors.append(message)
            continue
        if outcome == "pushed":
            pushed += 1
        elif outcome == "conflict":
            conflicts.append(f"{item.entity_type}:{item.entity_id}")
        elif outcome.startswith("held:"):
            status = outcome.removeprefix("held:")
            capability_holds.append(f"{item.entity_type}:{item.entity_id}:{status}")
        elif outcome.startswith("blocked:"):
            status = outcome.removeprefix("blocked:")
            blocked.append(f"{item.entity_type}:{item.entity_id}:{status}")
        elif outcome.startswith("error:"):
            status = outcome.removeprefix("error:")
            message = f"{item.entity_type}:{item.entity_id}: remote status: {status}"
            errors.append(message)
            terminal_errors.append(message)

    while True:
        plan = build_reference_sync_plan(cloud_user_id)
        item = next(
            (
                candidate for candidate in plan.tombstones
                if candidate.entity_type in _REFERENCE_TYPES
                and candidate.blocked_reason is None
                and ("tombstone", candidate.entity_type, candidate.entity_id)
                not in attempted
            ),
            None,
        )
        if item is None:
            break
        attempted.add(("tombstone", item.entity_type, item.entity_id))
        if item.entity_type == "observation_use":
            tombstone = next(
                tombstone
                for tombstone in ReferenceCloudSyncStateRepository.list_use_tombstones(
                    cloud_user_id
                )
                if tombstone.use_id == item.entity_id
            )
        else:
            tombstone = next(
                tombstone
                for tombstone in ReferenceCloudSyncStateRepository.list_library_tombstones(
                    cloud_user_id
                )
                if (tombstone.entity_type, tombstone.entity_id)
                == (item.entity_type, item.entity_id)
            )
        try:
            outcome = (
                _execute_use_tombstone(adapter, cloud_user_id, tombstone)
                if item.entity_type == "observation_use"
                else _execute_tombstone(adapter, cloud_user_id, tombstone)
            )
        except ReferenceCloudTransportError as exc:
            if item.entity_type == "observation_use":
                _record_use_tombstone_failure(tombstone, str(exc))
            else:
                _record_tombstone_failure(tombstone, str(exc))
            message = f"{item.entity_type}:{item.entity_id}: {exc}"
            errors.append(message)
            (retryable_errors if exc.retryable else terminal_errors).append(message)
            continue
        except (ReferenceCloudProtocolError, ReferenceCloudAccountMismatchError) as exc:
            if item.entity_type == "observation_use":
                _record_use_tombstone_failure(tombstone, str(exc))
            else:
                _record_tombstone_failure(tombstone, str(exc))
            message = f"{item.entity_type}:{item.entity_id}: {exc}"
            errors.append(message)
            terminal_errors.append(message)
            continue
        if outcome == "pushed":
            pushed += 1
        elif outcome == "conflict":
            conflicts.append(f"{item.entity_type}:{item.entity_id}")
        elif outcome.startswith("blocked:"):
            status = outcome.removeprefix("blocked:")
            blocked.append(f"{item.entity_type}:{item.entity_id}:{status}")
        elif outcome.startswith("error:"):
            status = outcome.removeprefix("error:")
            message = f"{item.entity_type}:{item.entity_id}: remote status: {status}"
            errors.append(message)
            terminal_errors.append(message)

    final_plan = build_reference_sync_plan(cloud_user_id)
    blocked.extend(
        f"{item.entity_type}:{item.entity_id}:{item.blocked_reason}"
        for item in final_plan.tombstones
        if item.entity_type in _REFERENCE_TYPES
        and item.blocked_reason is not None
        and item.blocked_reason != "conflict"
    )
    for item in final_plan.live:
        if item.entity_type not in _REFERENCE_TYPES or item.blocked_reason == "conflict":
            continue
        reason = item.blocked_reason
        if reason is None or reason in _HOLD_INHERITING_BLOCK_REASONS:
            # The executor check is more specific (it sees, e.g., an invalid
            # observation cloud id before the unconverged parent).
            replace_item = replace(item, blocked_reason=None)
            reason = (
                _executor_use_blocked(replace_item, cloud_user_id)
                if item.entity_type == "observation_use"
                else _executor_live_blocked(replace_item, cloud_user_id)
            ) or reason
        if reason is None:
            continue
        # A row waiting only on a capability-held set is part of that hold,
        # not a sync error repeated every sync.
        held = (
            holds.dependency_held_status(item.entity_type, item.entity_id)
            if reason in _HOLD_INHERITING_BLOCK_REASONS
            else None
        )
        if held is not None:
            capability_holds.append(f"{item.entity_type}:{item.entity_id}:{held}")
        else:
            blocked.append(f"{item.entity_type}:{item.entity_id}:{reason}")
    from utils.curated_reference_sync import push_curated_reference_forks
    forks = push_curated_reference_forks(client)
    return ReferenceSyncResult(
        pushed=pushed + forks.pushed,
        errors=tuple(errors) + forks.errors,
        retryable_errors=tuple(retryable_errors),
        terminal_errors=tuple(terminal_errors) + forks.errors,
        conflicts=tuple(dict.fromkeys((*conflicts, *forks.conflicts))),
        blocked=tuple(dict.fromkeys(blocked)),
        capability_holds=tuple(dict.fromkeys(capability_holds)),
    )


def _combine_reference_results(*results: ReferenceSyncResult) -> ReferenceSyncResult:
    return ReferenceSyncResult(
        pushed=sum(result.pushed for result in results),
        pulled=sum(result.pulled for result in results),
        errors=tuple(item for result in results for item in result.errors),
        retryable_errors=tuple(
            item for result in results for item in result.retryable_errors
        ),
        terminal_errors=tuple(
            item for result in results for item in result.terminal_errors
        ),
        conflicts=tuple(
            dict.fromkeys(item for result in results for item in result.conflicts)
        ),
        blocked=tuple(
            dict.fromkeys(item for result in results for item in result.blocked)
        ),
        capability_holds=tuple(
            dict.fromkeys(
                item for result in results for item in result.capability_holds
            )
        ),
    )


def sync_reference_library(
    client: object, *, pull_only: bool = False
) -> ReferenceSyncResult:
    """Reconcile the normalized graph, pushing only in bidirectional mode."""
    pulled = pull_reference_library(client)
    if pull_only or pulled.errors:
        return pulled
    # Stage M device report: once per app session and account, before the
    # first write of the session; failure never blocks the sync.
    report_reference_client_capabilities(client)
    return _combine_reference_results(pulled, _push_reference_library(client))


def acknowledge_observation_parent_delete(
    cloud_user_id: str, observation_cloud_id: str
) -> int:
    """Finish durable child-delete intent after a confirmed parent delete."""
    return ReferenceCloudSyncStateRepository.acknowledge_parent_delete_use_tombstones(
        cloud_user_id, observation_cloud_id
    )


def merge_reference_sync_result(
    legacy_result: dict[str, object],
    reference_result: ReferenceSyncResult,
) -> dict[str, object]:
    """Attach typed reference outcomes without changing legacy sync counters."""

    reference_payload = {
        "pushed": reference_result.pushed,
        "pulled": reference_result.pulled,
        "errors": list(reference_result.errors),
        "retryable_errors": list(reference_result.retryable_errors),
        "terminal_errors": list(reference_result.terminal_errors),
        "conflicts": list(reference_result.conflicts),
        "blocked": list(reference_result.blocked),
        "capability_holds": list(reference_result.capability_holds),
    }
    legacy_result["reference_sync"] = reference_payload
    errors = list(legacy_result.get("errors") or [])
    errors.extend(reference_result.errors)
    errors.extend(
        f"reference sync conflict: {item}" for item in reference_result.conflicts
    )
    errors.extend(
        f"reference sync blocked: {item}" for item in reference_result.blocked
    )
    legacy_result["errors"] = errors
    return legacy_result
