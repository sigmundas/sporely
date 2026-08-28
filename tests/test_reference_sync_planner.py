from __future__ import annotations

import sqlite3
from dataclasses import replace

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
from database.reference_sync_planner import (
    ReferenceSyncGraphSnapshot,
    ReferenceSyncLiveNode,
    ReferenceSyncTombstoneNode,
    build_reference_sync_plan,
    plan_reference_sync,
)
from database.reference_sync_state import ReferenceCloudSyncStateRepository


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
    return database_path


def _live(kind: str, entity_id: str, **values) -> ReferenceSyncLiveNode:
    return ReferenceSyncLiveNode(
        entity_type=kind,
        entity_id=entity_id,
        remote_identity_state=values.pop("remote_identity_state", "never_attempted"),
        sync_status=values.pop("sync_status", "dirty"),
        **values,
    )


def _tombstone(kind: str, entity_id: str, **values) -> ReferenceSyncTombstoneNode:
    return ReferenceSyncTombstoneNode(
        entity_type=kind,
        entity_id=entity_id,
        cloud_user_id=values.pop("cloud_user_id", "user-1"),
        remote_identity_state=values.pop("remote_identity_state", "acknowledged"),
        **values,
    )


def test_live_plan_is_parent_first_and_blocks_until_parent_acknowledged() -> None:
    snapshot = ReferenceSyncGraphSnapshot(
        live_nodes=(
            _live(
                "observation_use", "use-z", parent_id="set-z",
                observation_cloud_id="cloud-observation-1",
            ),
            _live("measurement_set", "set-z", parent_id="treatment-z"),
            _live("treatment", "treatment-z", parent_id="work-z"),
            _live("work", "work-z"),
        ),
    )

    plan = plan_reference_sync(snapshot, "user-1")

    assert [(item.entity_type, item.entity_id) for item in plan.live] == [
        ("work", "work-z"),
        ("treatment", "treatment-z"),
        ("measurement_set", "set-z"),
        ("observation_use", "use-z"),
    ]
    assert plan.live[0].blocked_reason is None
    assert [item.blocked_reason for item in plan.live[1:]] == [
        "parent_not_acknowledged",
        "parent_not_acknowledged",
        "parent_not_acknowledged",
    ]


def test_tombstones_are_child_first_and_wait_for_descendants() -> None:
    snapshot = ReferenceSyncGraphSnapshot(
        tombstones=(
            _tombstone("work", "work-a"),
            _tombstone("treatment", "treatment-a", parent_id="work-a"),
            _tombstone(
                "measurement_set", "set-a", parent_id="treatment-a"
            ),
            _tombstone(
                "observation_use", "use-a", parent_id="set-a",
                observation_cloud_id="cloud-observation-a",
                expected_row_version=9,
            ),
        ),
    )

    plan = plan_reference_sync(snapshot, "user-1")

    assert [(item.entity_type, item.entity_id) for item in plan.tombstones] == [
        ("observation_use", "use-a"),
        ("measurement_set", "set-a"),
        ("treatment", "treatment-a"),
        ("work", "work-a"),
    ]
    assert plan.tombstones[0].blocked_reason is None
    assert plan.tombstones[0].expected_row_version == 9
    assert [item.blocked_reason for item in plan.tombstones[1:]] == [
        "pending_descendant_tombstone",
        "pending_descendant_tombstone",
        "pending_descendant_tombstone",
    ]


def test_planner_is_permutation_stable_and_isolates_conflicts_and_accounts() -> None:
    nodes = (
        _live("work", "work-b"),
        _live("work", "work-a", sync_status="conflict"),
        _live(
            "work", "work-c", cloud_user_id="other-user",
            remote_identity_state="acknowledged", sync_status="clean",
        ),
    )

    forward = plan_reference_sync(ReferenceSyncGraphSnapshot(live_nodes=nodes), "user-1")
    reverse = plan_reference_sync(
        ReferenceSyncGraphSnapshot(live_nodes=tuple(reversed(nodes))), "user-1"
    )

    assert forward == reverse
    assert [(item.entity_id, item.blocked_reason) for item in forward.live] == [
        ("work-a", "conflict"),
        ("work-b", None),
        ("work-c", "account_mismatch"),
    ]


def test_use_without_verified_observation_cloud_id_is_blocked() -> None:
    snapshot = ReferenceSyncGraphSnapshot(
        live_nodes=(
            _live(
                "measurement_set", "set-a", cloud_user_id="user-1",
                remote_identity_state="acknowledged", sync_status="clean",
            ),
            _live("observation_use", "use-a", parent_id="set-a"),
        )
    )

    plan = plan_reference_sync(snapshot, "user-1")

    assert [(item.entity_id, item.blocked_reason) for item in plan.live] == [
        ("use-a", "missing_observation_cloud_id")
    ]


def test_use_plan_retains_verified_observation_dependency() -> None:
    snapshot = ReferenceSyncGraphSnapshot(
        live_nodes=(
            _live(
                "measurement_set", "set-a", cloud_user_id="user-1",
                remote_identity_state="acknowledged", sync_status="clean",
            ),
            _live(
                "observation_use", "use-a", parent_id="set-a",
                observation_cloud_id="cloud-observation-7",
            ),
        )
    )

    item = plan_reference_sync(snapshot, "user-1").live[0]
    assert item.blocked_reason is None
    assert item.parent_id == "set-a"
    assert item.observation_cloud_id == "cloud-observation-7"


def test_conflicted_parent_and_tombstone_block_only_their_graph() -> None:
    snapshot = ReferenceSyncGraphSnapshot(
        live_nodes=(
            _live(
                "work", "work-conflict", cloud_user_id="user-1",
                remote_identity_state="acknowledged", sync_status="conflict",
            ),
            _live("treatment", "treatment-blocked", parent_id="work-conflict"),
            _live("work", "work-ready"),
        ),
        tombstones=(
            _tombstone(
                "observation_use", "use-conflict", parent_id="set-conflict",
                sync_status="conflict",
            ),
            _tombstone("measurement_set", "set-conflict", parent_id="treatment-x"),
            _tombstone("measurement_set", "set-ready", parent_id="treatment-y"),
        ),
    )

    plan = plan_reference_sync(snapshot, "user-1")

    assert [(item.entity_id, item.blocked_reason) for item in plan.live] == [
        ("work-conflict", "conflict"),
        ("work-ready", None),
        ("treatment-blocked", "parent_conflict"),
    ]
    assert [(item.entity_id, item.blocked_reason) for item in plan.tombstones] == [
        ("use-conflict", "conflict"),
        ("set-conflict", "pending_descendant_tombstone"),
        ("set-ready", None),
    ]


def test_use_tombstone_without_verified_observation_cloud_id_is_blocked() -> None:
    plan = plan_reference_sync(
        ReferenceSyncGraphSnapshot(
            tombstones=(
                _tombstone("observation_use", "use-a", parent_id="set-a"),
            )
        ),
        "user-1",
    )

    assert plan.tombstones[0].blocked_reason == "missing_observation_cloud_id"


def test_replanning_after_durable_acknowledgement_unblocks_next_level() -> None:
    before = ReferenceSyncGraphSnapshot(
        live_nodes=(
            _live("treatment", "treatment-a", parent_id="work-a"),
            _live("work", "work-a"),
        )
    )
    after = ReferenceSyncGraphSnapshot(
        live_nodes=(
            _live("treatment", "treatment-a", parent_id="work-a"),
            _live(
                "work", "work-a", cloud_user_id="user-1",
                remote_identity_state="acknowledged", sync_status="clean",
            ),
        )
    )

    assert plan_reference_sync(before, "user-1").live[1].blocked_reason == (
        "parent_not_acknowledged"
    )
    replanned = plan_reference_sync(after, "user-1")
    assert [(item.entity_id, item.blocked_reason) for item in replanned.live] == [
        ("treatment-a", None)
    ]


def test_database_planner_reloads_durable_progress_after_restart(databases) -> None:
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-a", "book", "Work", "Work")
    )
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment("treatment-a", work.id, "Russula paludosa")
    )
    measurement_set = MeasurementSetRepository.create(
        MeasurementSet("set-a", treatment.id, "spore_size", "range")
    )
    with sqlite3.connect(databases) as connection:
        connection.execute(
            "INSERT INTO observations (id, date, cloud_id) "
            "VALUES (1, '2026-08-28', 'cloud-observation-1')"
        )
        connection.commit()
    ObservationReferenceUseRepository.attach(1, measurement_set.id)

    first = build_reference_sync_plan("user-1")
    assert [(item.entity_type, item.blocked_reason) for item in first.live] == [
        ("work", None),
        ("treatment", "parent_not_acknowledged"),
        ("measurement_set", "parent_not_acknowledged"),
        ("observation_use", "parent_not_acknowledged"),
    ]

    state = ReferenceCloudSyncStateRepository.get_library("work", work.id)
    ReferenceCloudSyncStateRepository.save_library(
        replace(
            state,
            cloud_user_id="user-1",
            remote_identity_state="acknowledged",
            cloud_row_version=1,
            accepted_payload={"id": work.id},
            sync_status="clean",
        )
    )

    restarted = build_reference_sync_plan("user-1")
    assert restarted == build_reference_sync_plan("user-1")
    assert [(item.entity_type, item.blocked_reason) for item in restarted.live] == [
        ("treatment", None),
        ("measurement_set", "parent_not_acknowledged"),
        ("observation_use", "parent_not_acknowledged"),
    ]


def test_database_planner_reloads_tombstone_progress_after_restart(databases) -> None:
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-a", "book", "Work", "Work")
    )
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment("treatment-a", work.id, "Russula paludosa")
    )
    measurement_set = MeasurementSetRepository.create(
        MeasurementSet("set-a", treatment.id, "spore_size", "range")
    )
    with sqlite3.connect(databases) as connection:
        connection.execute(
            "INSERT INTO observations (id, date, cloud_id) "
            "VALUES (1, '2026-08-28', 'cloud-observation-1')"
        )
        connection.commit()
    use = ObservationReferenceUseRepository.attach(1, measurement_set.id)
    for kind, entity_id in (
        ("work", work.id),
        ("treatment", treatment.id),
        ("measurement_set", measurement_set.id),
    ):
        state = ReferenceCloudSyncStateRepository.get_library(kind, entity_id)
        ReferenceCloudSyncStateRepository.save_library(
            replace(
                state, cloud_user_id="user-1",
                remote_identity_state="acknowledged", cloud_row_version=1,
                accepted_payload={"id": entity_id}, sync_status="clean",
            )
        )
    use_state = ReferenceCloudSyncStateRepository.get_use(use.id)
    ReferenceCloudSyncStateRepository.save_use(
        replace(
            use_state, cloud_user_id="user-1",
            remote_identity_state="acknowledged", cloud_row_version=1,
            accepted_payload={"id": use.id}, sync_status="clean",
        )
    )
    ObservationReferenceUseRepository.detach(use.id)
    ReferenceWorkRepository.delete(work.id)

    first = build_reference_sync_plan("user-1")
    assert [(item.entity_type, item.blocked_reason) for item in first.tombstones] == [
        ("observation_use", None),
        ("measurement_set", "pending_descendant_tombstone"),
        ("treatment", "pending_descendant_tombstone"),
        ("work", "pending_descendant_tombstone"),
    ]

    ReferenceCloudSyncStateRepository.resolve_use_tombstone(use.id, "user-1")
    restarted = build_reference_sync_plan("user-1")
    assert restarted == build_reference_sync_plan("user-1")
    assert [(item.entity_type, item.blocked_reason) for item in restarted.tombstones] == [
        ("measurement_set", None),
        ("treatment", "pending_descendant_tombstone"),
        ("work", "pending_descendant_tombstone"),
    ]
