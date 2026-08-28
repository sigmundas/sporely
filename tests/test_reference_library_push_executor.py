from __future__ import annotations

import sqlite3

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
from database.reference_sync_state import (
    ReferenceCloudSyncStateRepository,
    load_library_payload,
)
from utils.cloud_sync import (
    CloudReauthRequiredError,
    CloudSyncError,
    CloudTemporarilyUnavailableError,
)
from utils.reference_cloud_sync import ReferenceSyncResult, sync_reference_library


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


def _create_graph(prefix: str = "a"):
    work = ReferenceWorkRepository.create(
        ReferenceWork(
            id=f"work-{prefix}",
            type="book",
            title=f"Work {prefix}",
            short_label=f"Work {prefix}",
            authors_json='[{"family":"Tester"}]',
        )
    )
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id=f"treatment-{prefix}",
            reference_work_id=work.id,
            name_as_published="Russula paludosa",
        )
    )
    measurement_set = MeasurementSetRepository.create(
        MeasurementSet(
            id=f"set-{prefix}",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="range",
            length_min=7.0,
            length_max=9.0,
        )
    )
    return work, treatment, measurement_set


class RecordingReferenceClient:
    def __init__(self, *, user_id: str = "user-1") -> None:
        self.user_id = user_id
        self.calls: list[tuple[str, dict, int]] = []
        self.read_calls: list[str] = []
        self.rows: dict[str, list[dict]] = {
            "work": [],
            "treatment": [],
            "measurement_set": [],
        }
        self.remote_rows: dict[tuple[str, str], dict] = {}
        self.failures: dict[tuple[str, str], Exception] = {}
        self.conflicts: set[tuple[str, str]] = set()
        self.statuses: dict[tuple[str, str], str] = {}
        self.responses: dict[tuple[str, str], dict] = {}

    def _sync(self, kind: str, payload: dict, expected: int):
        self.calls.append((kind, dict(payload), expected))
        key = (kind, payload["id"])
        failure = self.failures.get(key)
        if failure is not None:
            raise failure
        if key in self.responses:
            return self.responses[key]
        if key in self.conflicts:
            current = self.remote_rows.get(key, {})
            return {
                "status": "conflict",
                "row": {
                    **current,
                    "id": payload["id"],
                    "user_id": self.user_id,
                    "row_version": max(expected, 1),
                    "deleted_at": None,
                    **{k: v for k, v in payload.items() if k != "deleted"},
                },
            }
        status = self.statuses.get(key, "updated" if expected else "created")
        row = {
            **self.remote_rows.get(key, {}),
            **payload,
            "user_id": self.user_id,
            "row_version": expected + 1 if expected else 1,
            "deleted_at": "2026-08-29T10:00:00Z" if payload.get("deleted") else None,
        }
        self.remote_rows[key] = row
        return {"status": status, "row": row}

    def sync_reference_work(self, payload: dict, expected: int):
        return self._sync("work", payload, expected)

    def sync_reference_taxon_treatment(self, payload: dict, expected: int):
        return self._sync("treatment", payload, expected)

    def sync_reference_measurement_set(self, payload: dict, expected: int):
        return self._sync("measurement_set", payload, expected)

    def sync_observation_reference_use(self, *_args, **_kwargs):
        raise AssertionError("Stage 4e must not push observation reference uses")

    def _list(self, kind: str) -> list[dict]:
        self.read_calls.append(kind)
        return list(self.rows[kind])

    def list_reference_works(self):
        return self._list("work")

    def list_reference_taxon_treatments(self):
        return self._list("treatment")

    def list_reference_measurement_sets(self):
        return self._list("measurement_set")

    def list_observation_reference_uses(self):
        raise AssertionError("Stage 4e must not read observation reference uses")


def test_pushes_library_parent_first_and_persists_each_acknowledgement(databases):
    _create_graph()
    client = RecordingReferenceClient()

    result = sync_reference_library(client)

    assert [call[0] for call in client.calls] == [
        "work",
        "treatment",
        "measurement_set",
    ]
    assert [call[2] for call in client.calls] == [0, 0, 0]
    assert client.calls[0][1]["authors_json"] == [{"family": "Tester"}]
    assert client.calls[0][1]["deleted"] is False
    assert "owner_id" not in client.calls[0][1]
    assert "created_at" not in client.calls[0][1]
    assert "legacy_reference_value_id" not in client.calls[2][1]
    assert result == ReferenceSyncResult(pushed=3)
    for kind, entity_id in (
        ("work", "work-a"),
        ("treatment", "treatment-a"),
        ("measurement_set", "set-a"),
    ):
        state = ReferenceCloudSyncStateRepository.get_library(kind, entity_id)
        assert state.remote_identity_state == "acknowledged"
        assert state.cloud_row_version == 1
        assert state.sync_status == "clean"
        assert state.accepted_payload["id"] == entity_id
        assert "row_version" not in state.accepted_payload
        assert "user_id" not in state.accepted_payload


def test_measurement_set_successor_waits_for_acknowledged_predecessor(databases):
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-a", "book", "Work", "Work")
    )
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment("treatment-a", work.id, "Russula paludosa")
    )
    MeasurementSetRepository.create(
        MeasurementSet(
            "set-z-parent", treatment.id, "spore_size", "range", revision=1
        )
    )
    MeasurementSetRepository.create(
        MeasurementSet(
            "set-a-successor",
            treatment.id,
            "spore_size",
            "range",
            revision=2,
            supersedes_id="set-z-parent",
        )
    )
    client = RecordingReferenceClient()

    sync_reference_library(client)

    assert [
        payload["id"] for kind, payload, _ in client.calls
        if kind == "measurement_set"
    ] == ["set-z-parent", "set-a-successor"]


def test_partial_progress_restarts_at_failed_level_and_keeps_descendant_pending(
    databases,
):
    _create_graph()
    first = RecordingReferenceClient()
    first.failures[("treatment", "treatment-a")] = CloudTemporarilyUnavailableError(
        "offline"
    )

    first_result = sync_reference_library(first)

    assert [call[0] for call in first.calls] == ["work", "treatment"]
    assert first_result.pushed == 1
    failed = ReferenceCloudSyncStateRepository.get_library("treatment", "treatment-a")
    assert failed.remote_identity_state == "create_outcome_unknown"
    assert failed.sync_status == "retry"
    assert failed.retry_count == 1
    assert ReferenceCloudSyncStateRepository.get_library(
        "measurement_set", "set-a"
    ).remote_identity_state == "never_attempted"

    restarted = RecordingReferenceClient()
    second_result = sync_reference_library(restarted)

    assert restarted.read_calls == [
        "work", "treatment", "measurement_set", "treatment"
    ]
    assert [call[0] for call in restarted.calls] == [
        "treatment",
        "measurement_set",
    ]
    assert restarted.calls[0][1] == first.calls[1][1]
    assert restarted.calls[0][2] == first.calls[1][2] == 0
    assert second_result.pushed == 2


def test_conflict_blocks_its_graph_but_unrelated_graph_continues(databases):
    _create_graph("a")
    ReferenceWorkRepository.create(
        ReferenceWork("work-b", "book", "Work b", "Work b")
    )
    client = RecordingReferenceClient()
    client.conflicts.add(("work", "work-a"))

    result = sync_reference_library(client)

    assert [(kind, payload["id"]) for kind, payload, _ in client.calls] == [
        ("work", "work-a"),
        ("work", "work-b"),
    ]
    conflict = ReferenceCloudSyncStateRepository.get_library("work", "work-a")
    assert conflict.sync_status == "conflict"
    assert conflict.cloud_row_version is None
    assert conflict.accepted_payload is None
    assert result.pushed == 1
    assert result.conflicts == ("work:work-a",)


def test_exact_accepted_payload_is_cleaned_without_remote_call(databases):
    work, _, _ = _create_graph()
    client = RecordingReferenceClient()
    sync_reference_library(client)
    client.calls.clear()

    ReferenceWorkRepository.update(work.id, {"title": work.title}, bump_revision=False)
    result = sync_reference_library(client)

    assert client.calls == []
    assert result == ReferenceSyncResult()
    assert ReferenceCloudSyncStateRepository.get_library(
        "work", work.id
    ).sync_status == "clean"


def test_lost_update_retry_accepts_no_change_row_with_authoritative_version(databases):
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-a", "book", "Before", "Before")
    )
    client = RecordingReferenceClient()
    sync_reference_library(client)
    ReferenceWorkRepository.update(work.id, {"title": "After"})
    client.calls.clear()
    client.statuses[("work", work.id)] = "no_change"
    client.remote_rows[("work", work.id)] = {
        **load_library_payload("work", work.id),
        "user_id": "user-1",
        "row_version": 2,
        "deleted_at": None,
    }

    result = sync_reference_library(client)

    assert result.pushed == 1
    assert client.calls[0][2] == 1
    state = ReferenceCloudSyncStateRepository.get_library("work", work.id)
    assert state.cloud_row_version == 2
    assert state.accepted_payload["title"] == "After"
    assert state.sync_status == "clean"


def test_local_edit_during_rpc_remains_dirty_after_remote_ack(databases):
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-a", "book", "Before", "Before")
    )
    client = RecordingReferenceClient()
    original = client.sync_reference_work

    def mutate_while_in_flight(payload: dict, expected: int):
        response = original(payload, expected)
        ReferenceWorkRepository.update(work.id, {"title": "After"})
        return response

    client.sync_reference_work = mutate_while_in_flight

    sync_reference_library(client)

    state = ReferenceCloudSyncStateRepository.get_library("work", work.id)
    assert state.cloud_row_version == 1
    assert state.accepted_payload["title"] == "Before"
    assert state.sync_status == "dirty"


def test_local_delete_during_live_rpc_transfers_ack_to_tombstone(databases):
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-a", "book", "Work", "Work")
    )
    client = RecordingReferenceClient()
    original = client.sync_reference_work

    def delete_while_in_flight(payload: dict, expected: int):
        response = original(payload, expected)
        if not payload.get("deleted"):
            ReferenceWorkRepository.delete(work.id)
        return response

    client.sync_reference_work = delete_while_in_flight

    result = sync_reference_library(client)

    assert [(payload["deleted"], expected) for _, payload, expected in client.calls] == [
        (False, 0),
        (True, 1),
    ]
    assert result.pushed == 2
    assert ReferenceCloudSyncStateRepository.get_library("work", work.id) is None
    assert ReferenceCloudSyncStateRepository.list_library_tombstones("user-1") == []


def test_delete_and_recreate_during_live_rpc_consumes_stale_tombstone(databases):
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-a", "book", "Original", "Original")
    )
    client = RecordingReferenceClient()
    sync_reference_library(client)
    ReferenceWorkRepository.update(work.id, {"title": "Sent"})
    client.calls.clear()
    original = client.sync_reference_work

    def replace_while_in_flight(payload: dict, expected: int):
        response = original(payload, expected)
        ReferenceWorkRepository.delete(work.id)
        ReferenceWorkRepository.create(
            ReferenceWork(work.id, "book", "Restored", "Restored")
        )
        client.conflicts.add(("work", work.id))
        return response

    client.sync_reference_work = replace_while_in_flight

    first = sync_reference_library(client)

    assert first.pushed == 1
    assert len(client.calls) == 1
    assert ReferenceCloudSyncStateRepository.list_library_tombstones("user-1") == []
    state = ReferenceCloudSyncStateRepository.get_library("work", work.id)
    assert state.remote_identity_state == "acknowledged"
    assert state.cloud_row_version == 2
    assert state.sync_status == "dirty"

    client.sync_reference_work = original
    client.conflicts.clear()
    client.calls.clear()
    second = sync_reference_library(client)

    assert second.pushed == 1
    assert client.calls[0][1]["title"] == "Restored"
    assert client.calls[0][2] == 2


def test_tombstones_execute_child_first_and_resolve_only_after_success(databases):
    _create_graph()
    client = RecordingReferenceClient()
    sync_reference_library(client)
    ReferenceWorkRepository.delete("work-a")
    client.calls.clear()

    result = sync_reference_library(client)

    assert [(kind, payload, expected) for kind, payload, expected in client.calls] == [
        ("measurement_set", {"id": "set-a", "deleted": True}, 1),
        ("treatment", {"id": "treatment-a", "deleted": True}, 1),
        ("work", {"id": "work-a", "deleted": True}, 1),
    ]
    assert result.pushed == 3
    assert ReferenceCloudSyncStateRepository.list_library_tombstones("user-1") == []


def test_recreation_during_tombstone_rpc_inherits_deleted_remote_token(databases):
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-a", "book", "Original", "Original")
    )
    client = RecordingReferenceClient()
    sync_reference_library(client)
    ReferenceWorkRepository.delete(work.id)
    original = client.sync_reference_work

    def recreate_while_in_flight(payload: dict, expected: int):
        response = original(payload, expected)
        if payload.get("deleted"):
            ReferenceWorkRepository.create(
                ReferenceWork(work.id, "book", "Restored", "Restored")
            )
        return response

    client.sync_reference_work = recreate_while_in_flight
    client.calls.clear()

    first = sync_reference_library(client)

    assert first.pushed == 1
    assert client.calls[0][2] == 1
    assert ReferenceCloudSyncStateRepository.list_library_tombstones("user-1") == []
    restored = ReferenceCloudSyncStateRepository.get_library("work", work.id)
    assert restored.remote_identity_state == "acknowledged"
    assert restored.cloud_row_version == 2
    assert restored.sync_status == "dirty"
    assert restored.accepted_payload["deleted"] is True

    client.sync_reference_work = original
    client.calls.clear()
    second = sync_reference_library(client)

    assert second.pushed == 1
    assert client.calls[0][1]["deleted"] is False
    assert client.calls[0][2] == 2


def test_recreated_uuid_restores_with_prior_token_and_clears_tombstone_atomically(
    databases,
):
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-a", "book", "Original", "Original")
    )
    client = RecordingReferenceClient()
    sync_reference_library(client)
    ReferenceWorkRepository.delete(work.id)
    ReferenceWorkRepository.create(
        ReferenceWork(work.id, "book", "Restored", "Restored")
    )
    client.calls.clear()

    result = sync_reference_library(client)

    assert len(client.calls) == 1
    kind, payload, expected = client.calls[0]
    assert kind == "work"
    assert expected == 1
    assert payload["id"] == "work-a"
    assert payload["title"] == "Restored"
    assert payload["deleted"] is False
    assert result.pushed == 1
    assert ReferenceCloudSyncStateRepository.list_library_tombstones("user-1") == []
    state = ReferenceCloudSyncStateRepository.get_library("work", work.id)
    assert state.cloud_row_version == 2
    assert state.sync_status == "clean"


def test_unknown_create_tombstone_uses_complete_owner_read_before_resolution(databases):
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-a", "book", "Work", "Work")
    )
    first = RecordingReferenceClient()
    first.failures[("work", work.id)] = CloudTemporarilyUnavailableError("lost")
    sync_reference_library(first)
    ReferenceWorkRepository.delete(work.id)

    second = RecordingReferenceClient()
    result = sync_reference_library(second)

    assert second.read_calls == ["work", "treatment", "measurement_set", "work"]
    assert second.calls == []
    assert result == ReferenceSyncResult()
    assert ReferenceCloudSyncStateRepository.list_library_tombstones("user-1") == []


def test_unknown_create_reconciles_matching_remote_row_without_duplicate_write(databases):
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-a", "book", "Work", "Work")
    )
    first = RecordingReferenceClient()
    first.failures[("work", work.id)] = CloudTemporarilyUnavailableError("lost")
    sync_reference_library(first)
    sent_payload = first.calls[0][1]

    second = RecordingReferenceClient()
    second.rows["work"] = [{
        **sent_payload,
        "user_id": "user-1",
        "row_version": 1,
        "created_at": "2026-08-29T00:00:00Z",
        "updated_at": "2026-08-29T00:00:00Z",
        "deleted_at": None,
    }]
    result = sync_reference_library(second)

    assert second.read_calls == ["work", "treatment", "measurement_set"]
    assert second.calls == []
    assert result == ReferenceSyncResult(pulled=1)
    state = ReferenceCloudSyncStateRepository.get_library("work", work.id)
    assert state.remote_identity_state == "acknowledged"
    assert state.cloud_row_version == 1
    assert state.sync_status == "clean"


def test_tombstone_conflict_is_durable_and_not_resolved(databases):
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-a", "book", "Work", "Work")
    )
    client = RecordingReferenceClient()
    sync_reference_library(client)
    ReferenceWorkRepository.delete(work.id)
    client.calls.clear()
    client.conflicts.add(("work", work.id))

    result = sync_reference_library(client)

    tombstone = ReferenceCloudSyncStateRepository.list_library_tombstones(
        "user-1"
    )[0]
    assert tombstone.sync_status == "conflict"
    assert tombstone.expected_row_version == 1
    assert tombstone.conflict["operation"] == "tombstone"
    assert result.conflicts == ("work:work-a",)


def test_tombstone_transport_failure_keeps_durable_retry_intent(databases):
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-a", "book", "Work", "Work")
    )
    client = RecordingReferenceClient()
    sync_reference_library(client)
    ReferenceWorkRepository.delete(work.id)
    client.failures[("work", work.id)] = CloudTemporarilyUnavailableError("offline")

    result = sync_reference_library(client)

    tombstone = ReferenceCloudSyncStateRepository.list_library_tombstones(
        "user-1"
    )[0]
    assert tombstone.sync_status == "retry"
    assert tombstone.retry_count == 1
    assert tombstone.expected_row_version == 1
    assert result.retryable_errors == result.errors


def test_remote_dependency_block_is_recorded_without_advancing_state(databases):
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-a", "book", "Work", "Work")
    )
    client = RecordingReferenceClient()
    client.statuses[("work", work.id)] = "blocked"

    result = sync_reference_library(client)

    state = ReferenceCloudSyncStateRepository.get_library("work", work.id)
    assert state.remote_identity_state == "create_outcome_unknown"
    assert state.cloud_row_version is None
    assert state.accepted_payload is None
    assert state.sync_status == "retry"
    assert result.blocked == ("work:work-a",)


def test_incomplete_acknowledgement_row_is_terminal_and_does_not_advance(databases):
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-a", "book", "Work", "Work")
    )
    client = RecordingReferenceClient()
    client.responses[("work", work.id)] = {
        "status": "created",
        "row": {
            "id": work.id,
            "user_id": "user-1",
            "row_version": 1,
            "deleted_at": None,
        },
    }

    result = sync_reference_library(client)

    state = ReferenceCloudSyncStateRepository.get_library("work", work.id)
    assert state.remote_identity_state == "create_outcome_unknown"
    assert state.cloud_row_version is None
    assert result.terminal_errors == result.errors


def test_observation_reference_uses_remain_disabled_in_stage4e(databases):
    database_path, _ = databases
    _, _, measurement_set = _create_graph()
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "INSERT INTO observations (id, date, cloud_id) "
            "VALUES (1, '2026-08-29', 'cloud-observation-1')"
        )
        connection.commit()
    use = ObservationReferenceUseRepository.attach(1, measurement_set.id)
    client = RecordingReferenceClient()

    sync_reference_library(client)

    use_state = ReferenceCloudSyncStateRepository.get_use(use.id)
    assert use_state.remote_identity_state == "never_attempted"
    assert use_state.sync_status == "dirty"


@pytest.mark.parametrize(
    "failure",
    [
        CloudSyncError("terminal"),
        CloudReauthRequiredError("sign in"),
        CloudTemporarilyUnavailableError("temporary"),
    ],
)
def test_remote_failures_are_durable_and_never_advance_baseline(databases, failure):
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-a", "book", "Work", "Work")
    )
    client = RecordingReferenceClient()
    client.failures[("work", work.id)] = failure

    result = sync_reference_library(client)

    state = ReferenceCloudSyncStateRepository.get_library("work", work.id)
    assert state.remote_identity_state == "create_outcome_unknown"
    assert state.cloud_row_version is None
    assert state.accepted_payload is None
    assert state.sync_status == "retry"
    assert state.retry_count == 1
    assert len(result.errors) == 1
    if isinstance(failure, CloudTemporarilyUnavailableError):
        assert result.retryable_errors == result.errors
        assert result.terminal_errors == ()
    else:
        assert result.terminal_errors == result.errors
        assert result.retryable_errors == ()
