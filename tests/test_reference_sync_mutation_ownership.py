from __future__ import annotations

import sqlite3
from dataclasses import replace

import pytest

from database import schema
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ObservationReferenceUseRepository,
    ReferenceIntegrityError,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
)
from database.reference_sync_state import ReferenceCloudSyncStateRepository
from utils.archive.portable_import import _merge_reference_entity


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


def _graph(database_path):
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-1", "book", "Work", "Work")
    )
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment("treatment-1", work.id, "Russula paludosa")
    )
    measurement_set = MeasurementSetRepository.create(
        MeasurementSet(
            "set-1", treatment.id, "spore_size", "range",
            raw_text="8–10 × 5–6 µm",
            length_core_min=8.0, length_core_max=10.0,
            width_core_min=5.0, width_core_max=6.0,
        )
    )
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "INSERT INTO observations (id, date, cloud_id) "
            "VALUES (1, '2026-08-28', 'cloud-observation-1')"
        )
        connection.commit()
    use = ObservationReferenceUseRepository.attach(1, measurement_set.id)
    return work, treatment, measurement_set, use


def _acknowledge_library(kind: str, entity_id: str) -> None:
    state = ReferenceCloudSyncStateRepository.get_library(kind, entity_id)
    ReferenceCloudSyncStateRepository.save_library(
        replace(
            state,
            cloud_user_id="user-1",
            remote_identity_state="acknowledged",
            cloud_row_version=7,
            accepted_payload={"id": entity_id},
            sync_status="clean",
            retry_count=2,
            last_error="old transport error",
        )
    )


def _acknowledge_use(use_id: str) -> None:
    state = ReferenceCloudSyncStateRepository.get_use(use_id)
    ReferenceCloudSyncStateRepository.save_use(
        replace(
            state,
            cloud_user_id="user-1",
            remote_identity_state="acknowledged",
            cloud_row_version=5,
            accepted_payload={"id": use_id},
            sync_status="clean",
            retry_count=2,
            last_error="old transport error",
        )
    )


def test_library_repository_updates_atomically_mark_transport_state_dirty(
    databases,
) -> None:
    database_path, _ = databases
    work, treatment, measurement_set, _ = _graph(database_path)
    for kind, entity_id in (
        ("work", work.id),
        ("treatment", treatment.id),
        ("measurement_set", measurement_set.id),
    ):
        _acknowledge_library(kind, entity_id)

    ReferenceWorkRepository.update(work.id, {"title": "Changed work"})
    TaxonTreatmentRepository.update(treatment.id, {"locator_text": "p. 9"})
    MeasurementSetRepository.update(measurement_set.id, {"notes": "Changed set"})

    for kind, entity_id in (
        ("work", work.id),
        ("treatment", treatment.id),
        ("measurement_set", measurement_set.id),
    ):
        state = ReferenceCloudSyncStateRepository.get_library(kind, entity_id)
        assert state.sync_status == "dirty"
        assert state.cloud_user_id == "user-1"
        assert state.cloud_row_version == 7
        assert state.accepted_payload == {"id": entity_id}
        assert state.retry_count == 0
        assert state.last_error is None


def test_library_mutation_rolls_back_when_intent_cannot_be_recorded(databases) -> None:
    _, reference_path = databases
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-1", "book", "Original", "Work")
    )
    with sqlite3.connect(reference_path) as connection:
        connection.execute(
            "CREATE TRIGGER reject_reference_intent BEFORE UPDATE "
            "ON reference_cloud_sync_state BEGIN "
            "SELECT RAISE(ABORT, 'intent rejected'); END"
        )
        connection.commit()

    with pytest.raises(ReferenceIntegrityError, match="intent rejected"):
        ReferenceWorkRepository.update(work.id, {"title": "Must roll back"})

    assert ReferenceWorkRepository.get(work.id).title == "Original"


def test_local_mutation_does_not_silently_clear_conflict(databases) -> None:
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-1", "book", "Original", "Work")
    )
    _acknowledge_library("work", work.id)
    state = ReferenceCloudSyncStateRepository.get_library("work", work.id)
    ReferenceCloudSyncStateRepository.save_library(
        replace(state, sync_status="conflict", conflict={"field": "title"})
    )

    ReferenceWorkRepository.update(work.id, {"title": "Another local edit"})

    conflicted = ReferenceCloudSyncStateRepository.get_library("work", work.id)
    assert conflicted.sync_status == "conflict"
    assert conflicted.conflict == {"field": "title"}
    assert conflicted.retry_count == 0
    assert conflicted.last_error is None


def test_portable_reference_revision_upgrade_records_mutation_intent(
    databases,
) -> None:
    _, reference_path = databases
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-1", "book", "Original", "Work")
    )
    _acknowledge_library("work", work.id)
    with sqlite3.connect(reference_path) as connection:
        connection.row_factory = sqlite3.Row
        _merge_reference_entity(
            {"id": work.id, "title": "Imported", "revision": 2},
            connection,
            table="main.reference_works",
            immutable_fields=set(),
        )
        connection.commit()

    assert ReferenceWorkRepository.get(work.id).title == "Imported"
    state = ReferenceCloudSyncStateRepository.get_library("work", work.id)
    assert state.sync_status == "dirty"
    assert state.cloud_user_id == "user-1"
    assert state.cloud_row_version == 7
    assert state.accepted_payload == {"id": work.id}


def test_use_mutations_mark_dirty_but_idempotent_operations_do_not(databases) -> None:
    database_path, _ = databases
    _, _, measurement_set, use = _graph(database_path)
    _acknowledge_use(use.id)

    same = ObservationReferenceUseRepository.attach(1, measurement_set.id)
    assert same.id == use.id
    assert ReferenceCloudSyncStateRepository.get_use(use.id).sync_status == "clean"

    unchanged, changed = ObservationReferenceUseRepository.refresh_snapshot(use.id)
    assert changed is False
    assert unchanged.id == use.id
    assert ReferenceCloudSyncStateRepository.get_use(use.id).sync_status == "clean"

    ObservationReferenceUseRepository.update(use.id, note="local edit")
    state = ReferenceCloudSyncStateRepository.get_use(use.id)
    assert state.sync_status == "dirty"
    assert state.cloud_row_version == 5
    assert state.accepted_payload == {"id": use.id}

    _acknowledge_use(use.id)
    ReferenceWorkRepository.update("work-1", {"title": "Refresh source"})
    _, changed = ObservationReferenceUseRepository.refresh_snapshot(use.id)
    assert changed is True
    assert ReferenceCloudSyncStateRepository.get_use(use.id).sync_status == "dirty"

    _acknowledge_use(use.id)
    successor = MeasurementSetRepository.create_revision(
        measurement_set.id,
        {"length_core_min": 9.0, "length_core_max": 11.0},
    )
    ObservationReferenceUseRepository.adopt_successor(use.id)
    adopted_state = ReferenceCloudSyncStateRepository.get_use(use.id)
    assert adopted_state.sync_status == "dirty"
    assert adopted_state.cloud_row_version == 5
    assert adopted_state.accepted_payload == {"id": use.id}
    assert ObservationReferenceUseRepository.get(use.id).reference_measurement_set_id == (
        successor.id
    )
