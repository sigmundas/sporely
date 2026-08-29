from __future__ import annotations

import copy

import pytest

from database import schema
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ObservationReferenceUseRepository,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
)
from database.reference_sync_state import ReferenceCloudSyncStateRepository
from utils.cloud_sync import CloudTemporarilyUnavailableError
from utils.reference_cloud_sync import (
    ReferenceSyncResult,
    acknowledge_observation_parent_delete,
    sync_reference_library,
)


@pytest.fixture()
def databases(tmp_path, monkeypatch):
    database_path = tmp_path / "mushrooms.db"
    reference_path = tmp_path / "reference_values.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: database_path)
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: reference_path)
    monkeypatch.setattr(
        schema,
        "get_bundled_reference_database_path",
        lambda: tmp_path / "missing-reference.db",
    )
    schema.init_database()
    return database_path, reference_path


def _create_graph_and_use(*, cloud_id: str | None = "101"):
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-1", "book", "Work", "Work")
    )
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment("treatment-1", work.id, "Russula paludosa")
    )
    measurement_set = MeasurementSetRepository.create(
        MeasurementSet(
            "set-1",
            treatment.id,
            "spore_size",
            "range",
            raw_text="7-9 x 5-6 um",
            length_min=7.0,
            length_max=9.0,
            width_min=5.0,
            width_max=6.0,
        )
    )
    connection = schema.get_connection()
    try:
        connection.execute(
            "INSERT INTO observations (id, date, cloud_id) VALUES (1, '2026-08-29', ?)",
            (cloud_id,),
        )
        connection.commit()
    finally:
        connection.close()
    use = ObservationReferenceUseRepository.attach(
        1,
        measurement_set.id,
        role="supports_identification",
        note="frozen note",
    )
    return work, treatment, measurement_set, use


class ReferenceGraphClient:
    def __init__(self):
        self.user_id = "user-1"
        self.calls: list[tuple] = []
        self.read_calls: list[str] = []
        self.rows = {
            "work": [],
            "treatment": [],
            "measurement_set": [],
            "observation_use": [],
        }
        self.remote_rows: dict[tuple[str, str], dict] = {}
        self.failures: dict[tuple[str, str], Exception] = {}
        self.conflicts: set[tuple[str, str]] = set()

    def _sync(self, kind, payload, expected, snapshot_mode=None):
        self.calls.append((kind, copy.deepcopy(payload), expected, snapshot_mode))
        key = (kind, payload["id"])
        if key in self.failures:
            raise self.failures[key]
        if key in self.conflicts:
            return {
                "status": "conflict",
                "row": self.remote_rows.get(key),
            }
        row = {
            **self.remote_rows.get(key, {}),
            **copy.deepcopy(payload),
            "user_id": self.user_id,
            "row_version": expected + 1 if expected else 1,
            "created_at": "2026-08-29T00:00:00Z",
            "updated_at": "2026-08-29T00:00:01Z",
            "deleted_at": "2026-08-29T00:00:02Z" if payload.get("deleted") else None,
        }
        self.remote_rows[key] = row
        return {"status": "updated" if expected else "created", "row": row}

    def sync_reference_work(self, payload, expected):
        return self._sync("work", payload, expected)

    def sync_reference_taxon_treatment(self, payload, expected):
        return self._sync("treatment", payload, expected)

    def sync_reference_measurement_set(self, payload, expected):
        return self._sync("measurement_set", payload, expected)

    def sync_observation_reference_use(self, payload, expected, snapshot_mode):
        return self._sync("observation_use", payload, expected, snapshot_mode)

    def _list(self, kind):
        self.read_calls.append(kind)
        if self.rows[kind] is None:
            return None
        rows = list(self.rows[kind])
        seen = {str(row.get("id")) for row in rows}
        rows.extend(
            row
            for (remote_kind, remote_id), row in self.remote_rows.items()
            if remote_kind == kind and remote_id not in seen
        )
        return copy.deepcopy(rows)

    def list_reference_works(self):
        return self._list("work")

    def list_reference_taxon_treatments(self):
        return self._list("treatment")

    def list_reference_measurement_sets(self):
        return self._list("measurement_set")

    def list_observation_reference_uses(self):
        return self._list("observation_use")


def _use_calls(client):
    return [call for call in client.calls if call[0] == "observation_use"]


def test_first_use_push_waits_for_library_and_uses_historical_import(databases):
    _, _, _, use = _create_graph_and_use()
    original_snapshot = use.snapshot_json
    client = ReferenceGraphClient()

    result = sync_reference_library(client)

    assert [call[0] for call in client.calls] == [
        "work",
        "treatment",
        "measurement_set",
        "observation_use",
    ]
    kind, payload, expected, mode = client.calls[-1]
    assert kind == "observation_use"
    assert expected == 0
    assert mode == "historical_import"
    assert payload["id"] == use.id
    assert payload["observation_id"] == 101
    assert payload["reference_measurement_set_id"] == "set-1"
    assert payload["role"] == "supports_identification"
    assert payload["note"] == "frozen note"
    assert payload["snapshot_json"] == __import__("json").loads(original_snapshot)
    assert "updated_at" not in payload
    assert result.pushed == 4
    state = ReferenceCloudSyncStateRepository.get_use(use.id)
    assert state.remote_identity_state == "acknowledged"
    assert state.cloud_row_version == 1
    assert state.sync_status == "clean"


def test_role_note_update_uses_current_and_preserves_frozen_snapshot(databases):
    _, _, _, use = _create_graph_and_use()
    client = ReferenceGraphClient()
    sync_reference_library(client)
    frozen = ObservationReferenceUseRepository.get(use.id).snapshot_json
    ObservationReferenceUseRepository.update(use.id, role="contradicts", note="new")
    client.calls.clear()

    result = sync_reference_library(client)

    call = _use_calls(client)[0]
    assert call[2:] == (1, "current")
    assert call[1]["role"] == "contradicts"
    assert call[1]["note"] == "new"
    assert call[1]["snapshot_json"] == __import__("json").loads(frozen)
    assert ObservationReferenceUseRepository.get(use.id).snapshot_json == frozen
    assert result.pushed == 1


def test_missing_observation_cloud_identity_blocks_use_without_writer(databases):
    _, _, _, use = _create_graph_and_use(cloud_id=None)
    client = ReferenceGraphClient()

    result = sync_reference_library(client)

    assert _use_calls(client) == []
    assert any(
        item.startswith(f"observation_use:{use.id}:missing_observation_cloud_id")
        for item in result.blocked
    )
    assert ReferenceCloudSyncStateRepository.get_use(use.id).sync_status == "dirty"


def test_attach_then_detach_before_first_sync_cancels_without_remote_delete(databases):
    _, _, _, use = _create_graph_and_use()
    ObservationReferenceUseRepository.detach(use.id)
    client = ReferenceGraphClient()

    sync_reference_library(client)

    assert _use_calls(client) == []
    assert ReferenceCloudSyncStateRepository.list_use_tombstones("user-1") == []


def test_observation_delete_before_first_reference_push_cancels_use(databases):
    _, _, _, use = _create_graph_and_use()
    connection = schema.get_connection()
    try:
        connection.execute("DELETE FROM observations WHERE id=1")
        connection.commit()
    finally:
        connection.close()
    client = ReferenceGraphClient()

    sync_reference_library(client)

    assert _use_calls(client) == []
    assert ReferenceCloudSyncStateRepository.get_use(use.id) is None
    assert ReferenceCloudSyncStateRepository.list_use_tombstones("user-1") == []


def test_acknowledged_detach_pushes_tombstone_with_saved_token(databases):
    _, _, _, use = _create_graph_and_use()
    client = ReferenceGraphClient()
    sync_reference_library(client)
    ObservationReferenceUseRepository.detach(use.id)
    client.calls.clear()

    result = sync_reference_library(client)

    call = _use_calls(client)[0]
    assert call[1] == {"id": use.id, "deleted": True}
    assert call[2] == 1
    assert call[3] == "current"
    assert ReferenceCloudSyncStateRepository.list_use_tombstones("user-1") == []


def test_detach_and_reattach_reuses_stable_uuid_and_remote_token(databases):
    _, _, measurement_set, first_use = _create_graph_and_use()
    client = ReferenceGraphClient()
    sync_reference_library(client)

    ObservationReferenceUseRepository.detach(first_use.id)
    restored = ObservationReferenceUseRepository.attach(1, measurement_set.id)
    client.calls.clear()

    result = sync_reference_library(client)

    assert restored.id == first_use.id
    call = _use_calls(client)[0]
    assert call[1]["id"] == first_use.id
    assert call[2:] == (1, "current")
    assert result.pushed == 1


def test_reattach_after_acknowledged_detach_restores_same_remote_uuid(databases):
    _, _, measurement_set, first_use = _create_graph_and_use()
    client = ReferenceGraphClient()
    sync_reference_library(client)
    ObservationReferenceUseRepository.detach(first_use.id)
    sync_reference_library(client)

    restored = ObservationReferenceUseRepository.attach(1, measurement_set.id)
    client.rows["observation_use"] = []
    client.calls.clear()
    result = sync_reference_library(client)

    assert restored.id == first_use.id
    call = _use_calls(client)[0]
    assert call[2:] == (2, "current")
    assert result.pushed == 1


def test_reattach_after_remote_tombstone_reuses_stable_uuid(databases):
    _, _, measurement_set, first_use = _create_graph_and_use()
    client = ReferenceGraphClient()
    sync_reference_library(client)
    remote = {
        **client.remote_rows[("observation_use", first_use.id)],
        "row_version": 2,
        "updated_at": "2026-08-29T00:00:02Z",
        "deleted_at": "2026-08-29T00:00:03Z",
    }
    client.rows["observation_use"] = [remote]
    sync_reference_library(client)
    assert ObservationReferenceUseRepository.get(first_use.id) is None

    restored = ObservationReferenceUseRepository.attach(1, measurement_set.id)
    client.calls.clear()
    result = sync_reference_library(client)

    assert restored.id == first_use.id
    call = _use_calls(client)[0]
    assert call[2:] == (2, "current")
    assert result.pushed == 1


def test_acknowledged_parent_delete_resolves_durable_use_tombstone(databases):
    _, _, _, use = _create_graph_and_use()
    client = ReferenceGraphClient()
    sync_reference_library(client)
    connection = schema.get_connection()
    try:
        connection.execute("DELETE FROM observations WHERE id=1")
        connection.commit()
    finally:
        connection.close()

    assert [row.use_id for row in ReferenceCloudSyncStateRepository.list_use_tombstones("user-1")] == [use.id]

    assert acknowledge_observation_parent_delete("user-1", "101") == 1
    assert ReferenceCloudSyncStateRepository.list_use_tombstones("user-1") == []


def test_restart_after_parent_delete_response_resolves_absent_child(databases):
    _, _, _, use = _create_graph_and_use()
    client = ReferenceGraphClient()
    sync_reference_library(client)
    connection = schema.get_connection()
    try:
        connection.execute("DELETE FROM observations WHERE id=1")
        connection.commit()
    finally:
        connection.close()
    # Model a crash after the server acknowledged and cascaded the parent
    # deletion but before local child intent was cleared.
    client.remote_rows.pop(("observation_use", use.id))
    client.calls.clear()

    result = sync_reference_library(client)

    assert _use_calls(client) == []
    assert result.errors == ()
    assert ReferenceCloudSyncStateRepository.list_use_tombstones("user-1") == []


def test_use_retry_reuses_identical_payload_uuid_and_expected_token(databases):
    _, _, _, use = _create_graph_and_use()
    first = ReferenceGraphClient()
    first.failures[("observation_use", use.id)] = CloudTemporarilyUnavailableError(
        "lost response"
    )

    first_result = sync_reference_library(first)
    first_call = _use_calls(first)[0]
    second = ReferenceGraphClient()
    second_result = sync_reference_library(second)
    second_call = _use_calls(second)[0]

    assert first_result.retryable_errors
    assert first_call[1:] == second_call[1:]
    assert second_call[2] == 0
    assert second_call[3] == "historical_import"
    assert second_result.pushed == 1


def test_lost_create_response_is_reconciled_without_duplicate_write(databases):
    _, _, _, use = _create_graph_and_use()
    first = ReferenceGraphClient()
    first.failures[("observation_use", use.id)] = CloudTemporarilyUnavailableError(
        "lost response"
    )
    sync_reference_library(first)
    sent = _use_calls(first)[0][1]
    restarted = ReferenceGraphClient()
    for kind in ("work", "treatment", "measurement_set"):
        restarted.rows[kind] = [
            copy.deepcopy(row)
            for (remote_kind, _), row in first.remote_rows.items()
            if remote_kind == kind
        ]
    restarted.rows["observation_use"] = [
        {
            **sent,
            "user_id": "user-1",
            "row_version": 1,
            "created_at": "2026-08-29T00:00:00Z",
            "updated_at": "2026-08-29T00:00:01Z",
            "deleted_at": None,
        }
    ]

    result = sync_reference_library(restarted)

    assert _use_calls(restarted) == []
    assert result.errors == ()
    state = ReferenceCloudSyncStateRepository.get_use(use.id)
    assert state.remote_identity_state == "acknowledged"
    assert state.cloud_row_version == 1
    assert state.sync_status == "clean"


def test_use_cas_conflict_is_durable_and_does_not_overwrite_snapshot(databases):
    _, _, _, use = _create_graph_and_use()
    client = ReferenceGraphClient()
    sync_reference_library(client)
    frozen = ObservationReferenceUseRepository.get(use.id).snapshot_json
    ObservationReferenceUseRepository.update(use.id, note="local")
    client.calls.clear()
    client.conflicts.add(("observation_use", use.id))
    client.remote_rows[("observation_use", use.id)] = {
        **client.remote_rows[("observation_use", use.id)],
        "note": "remote",
        "row_version": 2,
    }

    result = sync_reference_library(client)

    state = ReferenceCloudSyncStateRepository.get_use(use.id)
    assert state.sync_status == "conflict"
    assert state.cloud_row_version == 1
    assert ObservationReferenceUseRepository.get(use.id).snapshot_json == frozen
    assert result.conflicts == (f"observation_use:{use.id}",)


def test_complete_use_read_failure_does_not_push_any_graph_mutations(databases):
    _create_graph_and_use()
    client = ReferenceGraphClient()
    client.rows["observation_use"] = None

    result = sync_reference_library(client)

    assert client.calls == []
    assert result.terminal_errors
