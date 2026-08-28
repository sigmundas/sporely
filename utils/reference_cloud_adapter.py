"""Typed transport boundary for the normalized reference Supabase contract."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Protocol

from utils.cloud_sync import (
    AccountMismatchError,
    CloudReauthRequiredError,
    CloudSyncError,
    CloudTemporarilyUnavailableError,
)


ReferenceMutationStatus = Literal[
    "created", "updated", "no_change", "conflict", "blocked",
    "invalid_parent", "invalid_payload", "invalid_revision",
    "invalid_snapshot", "invalid_snapshot_mode", "invalid_successor",
    "account_deleting",
]

_STATUS_DISPOSITION = {
    "created": "acknowledged",
    "updated": "acknowledged",
    "no_change": "acknowledged",
    "conflict": "conflict",
    "blocked": "blocked",
    "invalid_parent": "blocked",
    "invalid_payload": "rejected",
    "invalid_revision": "rejected",
    "invalid_snapshot": "rejected",
    "invalid_snapshot_mode": "rejected",
    "invalid_successor": "rejected",
    "account_deleting": "account_terminal",
}

_WORK_KEYS = frozenset({
    "id", "type", "citation_key", "authors_json", "editors_json", "title",
    "container_title", "year", "edition", "publisher", "place", "volume",
    "issue", "pages", "doi", "isbn", "url", "language", "short_label",
    "citation_override", "revision", "deleted",
})
_TREATMENT_KEYS = frozenset({
    "id", "reference_work_id", "taxon_id", "name_as_published", "page_from",
    "page_to", "locator_text", "treatment_notes", "revision", "deleted",
})
_MEASUREMENT_SET_KEYS = frozenset({
    "id", "taxon_treatment_id", "character", "raw_text", "data_kind",
    "length_min", "length_core_min", "length_core_max", "length_max",
    "width_min", "width_core_min", "width_core_max", "width_max", "q_min",
    "q_max", "q_mean", "length_mean", "width_mean", "sample_size",
    "specimen_count", "mount_medium", "stain", "preparation",
    "measurement_method", "notes", "raw_points_json", "supersedes_id",
    "revision", "deleted",
})
_USE_KEYS = frozenset({
    "id", "observation_id", "reference_measurement_set_id", "role", "note",
    "selected_at", "reference_revision", "snapshot_json", "deleted",
})


class ReferenceCloudProtocolError(CloudSyncError):
    """The request or server response violates the Stage 3 contract."""


class ReferenceCloudAccountMismatchError(AccountMismatchError):
    """The adapter, client, or returned row belongs to another account."""


class ReferenceCloudTransportError(CloudSyncError):
    """A request failed before a structured Stage 3 result was received."""

    def __init__(self, message: str, *, retryable: bool, auth_required: bool = False):
        super().__init__(message)
        self.retryable = retryable
        self.auth_required = auth_required


@dataclass(frozen=True, slots=True)
class ReferenceRemoteMutationResult:
    status: ReferenceMutationStatus
    disposition: str
    row: dict[str, Any] | None


class ReferenceCloudClient(Protocol):
    user_id: str

    def sync_reference_work(self, payload: dict, expected: int): ...
    def sync_reference_taxon_treatment(self, payload: dict, expected: int): ...
    def sync_reference_measurement_set(self, payload: dict, expected: int): ...
    def sync_observation_reference_use(
        self, payload: dict, expected: int, snapshot_mode: str
    ): ...
    def list_reference_works(self) -> list[dict]: ...
    def list_reference_taxon_treatments(self) -> list[dict]: ...
    def list_reference_measurement_sets(self) -> list[dict]: ...
    def list_observation_reference_uses(self) -> list[dict]: ...


class ReferenceCloudAdapter:
    """Validate and type Stage 3 calls without scheduling or retry policy."""

    def __init__(self, client: ReferenceCloudClient, cloud_user_id: str):
        self._client = client
        self._cloud_user_id = str(cloud_user_id or "").strip()

    def _check_account(self) -> None:
        client_user_id = str(getattr(self._client, "user_id", "") or "").strip()
        if not self._cloud_user_id or client_user_id != self._cloud_user_id:
            raise ReferenceCloudAccountMismatchError(
                "reference adapter and cloud client account do not match"
            )

    @staticmethod
    def _request(payload: dict, allowed: frozenset[str], expected: int) -> dict:
        if not isinstance(payload, dict) or not str(payload.get("id") or "").strip():
            raise ReferenceCloudProtocolError("reference payload requires an id")
        unknown = set(payload) - allowed
        if unknown:
            raise ReferenceCloudProtocolError(
                f"reference payload has unknown keys: {', '.join(sorted(unknown))}"
            )
        if isinstance(expected, bool) or not isinstance(expected, int) or expected < 0:
            raise ReferenceCloudProtocolError("expected row version must be nonnegative")
        if payload.get("deleted") is True and expected == 0:
            raise ReferenceCloudProtocolError(
                "a tombstone requires an acknowledged positive row version"
            )
        return dict(payload)

    def _call(self, call, payload: dict, expected: int, *args) -> ReferenceRemoteMutationResult:
        self._check_account()
        try:
            response = call(payload, expected, *args)
        except ReferenceCloudAccountMismatchError:
            raise
        except AccountMismatchError as exc:
            raise ReferenceCloudAccountMismatchError(str(exc)) from exc
        except CloudTemporarilyUnavailableError as exc:
            raise ReferenceCloudTransportError(str(exc), retryable=True) from exc
        except CloudReauthRequiredError as exc:
            raise ReferenceCloudTransportError(
                str(exc), retryable=False, auth_required=True
            ) from exc
        except CloudSyncError as exc:
            raise ReferenceCloudTransportError(str(exc), retryable=False) from exc
        return self._parse_result(response, str(payload["id"]), payload.get("deleted") is True)

    def _parse_result(
        self, response: object, entity_id: str, tombstone: bool
    ) -> ReferenceRemoteMutationResult:
        if not isinstance(response, dict) or set(response) != {"status", "row"}:
            raise ReferenceCloudProtocolError("malformed reference RPC envelope")
        status = response["status"]
        if status not in _STATUS_DISPOSITION:
            raise ReferenceCloudProtocolError("unknown reference RPC status")
        row = response["row"]
        if row is not None:
            if not isinstance(row, dict):
                raise ReferenceCloudProtocolError("reference RPC row must be an object")
            self._validate_row(row, entity_id)
        if status in {"created", "updated", "no_change"} and row is None:
            raise ReferenceCloudProtocolError("acknowledged result requires a row")
        if tombstone and status in {"created", "updated", "no_change"}:
            if not str(row.get("deleted_at") or "").strip():
                raise ReferenceCloudProtocolError("tombstone acknowledgement is not deleted")
        return ReferenceRemoteMutationResult(status, _STATUS_DISPOSITION[status], row)

    def _validate_row(self, row: dict, entity_id: str | None = None) -> None:
        if str(row.get("user_id") or "").strip() != self._cloud_user_id:
            raise ReferenceCloudAccountMismatchError("reference row belongs to another account")
        if not str(row.get("id") or "").strip():
            raise ReferenceCloudProtocolError("reference row has no id")
        if entity_id is not None and str(row["id"]) != entity_id:
            raise ReferenceCloudProtocolError("reference RPC returned another identity")
        version = row.get("row_version")
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise ReferenceCloudProtocolError("reference row has invalid row_version")

    def sync_work(self, payload: dict, expected_row_version: int):
        clean = self._request(payload, _WORK_KEYS, expected_row_version)
        return self._call(self._client.sync_reference_work, clean, expected_row_version)

    def sync_treatment(self, payload: dict, expected_row_version: int):
        clean = self._request(payload, _TREATMENT_KEYS, expected_row_version)
        return self._call(
            self._client.sync_reference_taxon_treatment, clean, expected_row_version
        )

    def sync_measurement_set(self, payload: dict, expected_row_version: int):
        clean = self._request(payload, _MEASUREMENT_SET_KEYS, expected_row_version)
        return self._call(
            self._client.sync_reference_measurement_set, clean, expected_row_version
        )

    def sync_observation_use(
        self, payload: dict, expected_row_version: int, *, snapshot_mode: str = "current"
    ):
        if snapshot_mode not in {"current", "historical_import"}:
            raise ReferenceCloudProtocolError("invalid observation-use snapshot mode")
        clean = self._request(payload, _USE_KEYS, expected_row_version)
        return self._call(
            self._client.sync_observation_reference_use,
            clean,
            expected_row_version,
            snapshot_mode,
        )

    def _list(self, call) -> tuple[dict[str, Any], ...]:
        self._check_account()
        try:
            rows = call()
        except AccountMismatchError as exc:
            raise ReferenceCloudAccountMismatchError(str(exc)) from exc
        except CloudTemporarilyUnavailableError as exc:
            raise ReferenceCloudTransportError(str(exc), retryable=True) from exc
        except CloudReauthRequiredError as exc:
            raise ReferenceCloudTransportError(
                str(exc), retryable=False, auth_required=True
            ) from exc
        except CloudSyncError as exc:
            raise ReferenceCloudTransportError(str(exc), retryable=False) from exc
        if not isinstance(rows, list):
            raise ReferenceCloudProtocolError("reference owner read must return a list")
        result = []
        for row in rows:
            if not isinstance(row, dict):
                raise ReferenceCloudProtocolError("reference owner row must be an object")
            self._validate_row(row)
            result.append(dict(row))
        return tuple(result)

    def list_works(self):
        return self._list(self._client.list_reference_works)

    def list_treatments(self):
        return self._list(self._client.list_reference_taxon_treatments)

    def list_measurement_sets(self):
        return self._list(self._client.list_reference_measurement_sets)

    def list_observation_uses(self):
        return self._list(self._client.list_observation_reference_uses)
