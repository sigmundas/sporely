from __future__ import annotations

import json
import sqlite3
from dataclasses import replace

import pytest

from database import schema
from database.reference_library import (
    ObservationReferenceUseRepository,
    ReferenceWork,
    ReferenceWorkRepository,
)


@pytest.fixture()
def sync_databases(tmp_path, monkeypatch):
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


def _create_work() -> ReferenceWork:
    return ReferenceWorkRepository.create(
        ReferenceWork(
            id="work-1",
            type="book",
            title="Reference work",
            short_label="Reference",
        )
    )


def test_library_sync_state_round_trips_transport_and_conflict_state(
    sync_databases,
) -> None:
    from database.reference_sync_state import (
        ReferenceCloudSyncStateRepository,
    )

    _create_work()
    initial = ReferenceCloudSyncStateRepository.get_library("work", "work-1")
    assert initial is not None
    assert initial.remote_identity_state == "never_attempted"
    assert initial.cloud_row_version is None
    assert initial.accepted_payload is None
    assert initial.sync_status == "dirty"

    unknown = ReferenceCloudSyncStateRepository.save_library(
        replace(
            initial,
            cloud_user_id="user-1",
            remote_identity_state="create_outcome_unknown",
            last_attempted_at="2026-08-28T12:00:00+00:00",
        )
    )
    assert unknown.remote_identity_state == "create_outcome_unknown"

    acknowledged = ReferenceCloudSyncStateRepository.save_library(
        replace(
            unknown,
            remote_identity_state="acknowledged",
            cloud_row_version=7,
            accepted_payload={"id": "work-1", "revision": 2},
            sync_status="clean",
        )
    )
    assert acknowledged.cloud_row_version == 7
    assert acknowledged.accepted_payload == {"id": "work-1", "revision": 2}

    conflicted = ReferenceCloudSyncStateRepository.save_library(
        replace(
            acknowledged,
            sync_status="conflict",
            conflict={"remote_revision": 3},
            last_error="overlapping edit",
        )
    )
    assert conflicted.sync_status == "conflict"
    assert conflicted.conflict == {"remote_revision": 3}


def test_sync_state_account_binding_fails_closed(sync_databases) -> None:
    from database.reference_sync_state import (
        ReferenceCloudSyncAccountMismatchError,
        ReferenceCloudSyncStateError,
        ReferenceCloudSyncStateRepository,
    )

    _create_work()
    initial = ReferenceCloudSyncStateRepository.get_library("work", "work-1")
    bound = ReferenceCloudSyncStateRepository.save_library(
        replace(initial, cloud_user_id="user-1")
    )

    with pytest.raises(ReferenceCloudSyncAccountMismatchError):
        ReferenceCloudSyncStateRepository.save_library(
            replace(bound, cloud_user_id="user-2")
        )

    with pytest.raises(
        ReferenceCloudSyncStateError,
        match="unknown create outcome cannot have an accepted baseline",
    ):
        ReferenceCloudSyncStateRepository.save_library(
            replace(
                initial,
                cloud_user_id="user-1",
                remote_identity_state="create_outcome_unknown",
                accepted_payload={"id": "work-1"},
            )
        )


def test_use_sync_state_and_tombstone_round_trip(sync_databases) -> None:
    from database.reference_sync_state import ReferenceCloudSyncStateRepository

    database_path, _ = sync_databases
    _create_work()
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "INSERT INTO observations (id, date, cloud_id) "
            "VALUES (1, '2026-08-28', 'cloud-observation-1')"
        )
        connection.commit()
    use = ObservationReferenceUseRepository.attach(
        1,
        "missing-set",
        allow_dangling=True,
    )
    initial = ReferenceCloudSyncStateRepository.get_use(use.id)
    acknowledged = ReferenceCloudSyncStateRepository.save_use(
        replace(
            initial,
            cloud_user_id="user-1",
            remote_identity_state="acknowledged",
            cloud_row_version=5,
            accepted_payload={"id": use.id},
            sync_status="clean",
        )
    )
    assert acknowledged.cloud_row_version == 5

    ObservationReferenceUseRepository.detach(use.id)

    assert ReferenceCloudSyncStateRepository.get_use(use.id) is None
    assert ReferenceCloudSyncStateRepository.list_use_tombstones("other-user") == []
    tombstones = ReferenceCloudSyncStateRepository.list_use_tombstones("user-1")
    assert len(tombstones) == 1
    assert tombstones[0].use_id == use.id
    assert tombstones[0].reference_measurement_set_id == "missing-set"
    assert tombstones[0].local_observation_id == 1
    assert tombstones[0].observation_cloud_id == "cloud-observation-1"
    assert tombstones[0].cloud_user_id == "user-1"
    assert tombstones[0].expected_row_version == 5
    assert tombstones[0].accepted_payload == {"id": use.id}

    assert ReferenceCloudSyncStateRepository.resolve_use_tombstone(
        use.id, "other-user"
    ) is False
    assert ReferenceCloudSyncStateRepository.resolve_use_tombstone(
        use.id, "user-1"
    ) is True
    assert ReferenceCloudSyncStateRepository.list_use_tombstones("user-1") == []


def test_library_tombstone_retains_dependency_and_transport_state(
    sync_databases,
) -> None:
    from database.reference_library import (
        MeasurementSet,
        MeasurementSetRepository,
        TaxonTreatment,
        TaxonTreatmentRepository,
    )
    from database.reference_sync_state import ReferenceCloudSyncStateRepository

    _create_work()
    TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="treatment-1",
            reference_work_id="work-1",
            name_as_published="Russula paludosa",
        )
    )
    MeasurementSetRepository.create(
        MeasurementSet(
            id="set-1",
            taxon_treatment_id="treatment-1",
            character="spore_size",
            data_kind="range",
        )
    )
    state = ReferenceCloudSyncStateRepository.get_library(
        "measurement_set", "set-1"
    )
    ReferenceCloudSyncStateRepository.save_library(
        replace(
            state,
            cloud_user_id="user-1",
            remote_identity_state="acknowledged",
            cloud_row_version=3,
            accepted_payload={"id": "set-1"},
            sync_status="clean",
        )
    )

    MeasurementSetRepository.delete("set-1")

    assert ReferenceCloudSyncStateRepository.list_library_tombstones("other-user") == []
    tombstones = ReferenceCloudSyncStateRepository.list_library_tombstones("user-1")
    assert len(tombstones) == 1
    assert tombstones[0].entity_type == "measurement_set"
    assert tombstones[0].entity_id == "set-1"
    assert tombstones[0].taxon_treatment_id == "treatment-1"
    assert tombstones[0].expected_row_version == 3
    assert tombstones[0].accepted_payload == {"id": "set-1"}

    assert ReferenceCloudSyncStateRepository.resolve_library_tombstone(
        "measurement_set", "set-1", "other-user"
    ) is False
    assert ReferenceCloudSyncStateRepository.resolve_library_tombstone(
        "measurement_set", "set-1", "user-1"
    ) is True
    assert ReferenceCloudSyncStateRepository.list_library_tombstones("user-1") == []


def test_sync_state_payloads_are_stored_as_canonical_json(sync_databases) -> None:
    from database.reference_sync_state import ReferenceCloudSyncStateRepository

    _, reference_path = sync_databases
    _create_work()
    state = ReferenceCloudSyncStateRepository.get_library("work", "work-1")
    ReferenceCloudSyncStateRepository.save_library(
        replace(
            state,
            cloud_user_id="user-1",
            remote_identity_state="acknowledged",
            cloud_row_version=1,
            accepted_payload={"z": 1, "a": [2, 1]},
            sync_status="clean",
        )
    )

    with sqlite3.connect(reference_path) as connection:
        stored = connection.execute(
            "SELECT accepted_payload_json FROM reference_cloud_sync_state "
            "WHERE entity_type='work' AND entity_id='work-1'"
        ).fetchone()[0]
    assert stored == json.dumps(
        {"a": [2, 1], "z": 1},
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
