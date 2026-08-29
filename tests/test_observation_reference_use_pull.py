from __future__ import annotations

import copy
import json

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
    ReferenceCloudSyncState,
    ReferenceCloudSyncStateRepository,
    load_library_payload,
    load_use_payload,
)
from database.reference_use_sync_reconciliation import (
    reconcile_observation_reference_use_feed,
    stage_observation_reference_use_feed,
)
from utils.cloud_sync import CloudTemporarilyUnavailableError
from utils.reference_cloud_sync import ReferenceSyncResult, pull_reference_library


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


def _seed_graph_and_observation(*, cloud_id: str = "101"):
    work = ReferenceWorkRepository.create(
        ReferenceWork("work-1", "book", "Work", "Work", authors_json="[]")
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
    for kind, entity_id in (
        ("work", work.id),
        ("treatment", treatment.id),
        ("measurement_set", measurement_set.id),
    ):
        payload = load_library_payload(kind, entity_id)
        ReferenceCloudSyncStateRepository.save_library(
            ReferenceCloudSyncState(
                kind, entity_id, "user-1", "acknowledged", 1, payload, "clean"
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
    return work, treatment, measurement_set


def _library_rows():
    rows = {}
    for kind, entity_id in (
        ("work", "work-1"),
        ("treatment", "treatment-1"),
        ("measurement_set", "set-1"),
    ):
        payload = load_library_payload(kind, entity_id)
        rows[kind] = {
            **payload,
            "user_id": "user-1",
            "row_version": 1,
            "created_at": "2026-08-01T00:00:00Z",
            "updated_at": "2026-08-01T00:00:01Z",
            "deleted_at": None,
        }
    return rows


def _snapshot():
    return {
        "schema_version": 1,
        "reference_work_id": "work-1",
        "reference_treatment_id": "treatment-1",
        "reference_measurement_set_id": "set-1",
        "reference_revision": 1,
        "frozen": "remote evidence",
    }


def _use_row(**updates):
    row = {
        "user_id": "user-1",
        "id": "use-1",
        "observation_id": 101,
        "reference_measurement_set_id": "set-1",
        "role": "compared",
        "note": "remote note",
        "selected_at": "2026-08-01T00:00:00Z",
        "reference_revision": 1,
        "snapshot_json": _snapshot(),
        "row_version": 1,
        "created_at": "2026-08-01T00:00:00Z",
        "updated_at": "2026-08-01T00:00:01Z",
        "deleted_at": None,
    }
    row.update(updates)
    return row


class PullClient:
    def __init__(self, *, uses=(), works=None, treatments=None, sets=None):
        library = _library_rows()
        self.user_id = "user-1"
        self.works = list(works if works is not None else [library["work"]])
        self.treatments = list(
            treatments if treatments is not None else [library["treatment"]]
        )
        self.sets = list(sets if sets is not None else [library["measurement_set"]])
        self.uses = list(uses)
        self.calls: list[str] = []
        self.failures: set[str] = set()

    def _read(self, kind, rows):
        self.calls.append(kind)
        if kind in self.failures:
            raise CloudTemporarilyUnavailableError(f"{kind} page failed")
        return copy.deepcopy(rows)

    def list_reference_works(self):
        return self._read("work", self.works)

    def list_reference_taxon_treatments(self):
        return self._read("treatment", self.treatments)

    def list_reference_measurement_sets(self):
        return self._read("measurement_set", self.sets)

    def list_observation_reference_uses(self):
        return self._read("observation_use", self.uses)

    def sync_reference_work(self, *_args):
        raise AssertionError("pull must not write")

    sync_reference_taxon_treatment = sync_reference_work
    sync_reference_measurement_set = sync_reference_work
    sync_observation_reference_use = sync_reference_work


def _acknowledge_local_use(use_id: str) -> None:
    payload = load_use_payload(use_id)
    ReferenceCloudSyncStateRepository.save_use(
        ReferenceCloudSyncState(
            "observation_use", use_id, "user-1", "acknowledged", 1, payload, "clean"
        )
    )


def test_fresh_use_pull_preserves_exact_uuid_and_frozen_snapshot(databases):
    _seed_graph_and_observation()
    remote = _use_row()

    result = pull_reference_library(PullClient(uses=[remote]))

    use = ObservationReferenceUseRepository.get("use-1")
    state = ReferenceCloudSyncStateRepository.get_use("use-1")
    assert result == ReferenceSyncResult(pulled=1)
    assert use is not None
    assert use.id == "use-1"
    assert use.observation_id == 1
    assert json.loads(use.snapshot_json) == remote["snapshot_json"]
    assert state is not None
    assert state.cloud_row_version == 1
    assert state.sync_status == "clean"
    assert ReferenceCloudSyncStateRepository.get_use_pull_cursor("user-1") == (
        "2026-08-01T00:00:01Z",
        "use-1",
    )


def test_remote_use_update_applies_exact_remote_snapshot_and_is_idempotent(databases):
    _, _, measurement_set = _seed_graph_and_observation()
    local = ObservationReferenceUseRepository.attach(1, measurement_set.id, role="compared")
    _acknowledge_local_use(local.id)
    remote = _use_row(
        id=local.id,
        role="contradicts",
        note="updated remotely",
        snapshot_json={**_snapshot(), "frozen": "explicit remote refresh"},
        row_version=2,
        updated_at="2026-08-01T00:00:02Z",
    )
    client = PullClient(uses=[remote])

    first = pull_reference_library(client)
    second = pull_reference_library(client)

    use = ObservationReferenceUseRepository.get(local.id)
    assert first == ReferenceSyncResult(pulled=1)
    assert second == ReferenceSyncResult()
    assert use.role == "contradicts"
    assert use.note == "updated remotely"
    assert json.loads(use.snapshot_json) == remote["snapshot_json"]


def test_remote_use_tombstone_removes_local_use_without_outbound_intent(databases):
    _, _, measurement_set = _seed_graph_and_observation()
    local = ObservationReferenceUseRepository.attach(1, measurement_set.id)
    _acknowledge_local_use(local.id)
    remote = _use_row(
        id=local.id,
        row_version=2,
        updated_at="2026-08-01T00:00:02Z",
        deleted_at="2026-08-01T00:00:03Z",
    )

    result = pull_reference_library(PullClient(uses=[remote]))

    assert result == ReferenceSyncResult(pulled=1)
    assert ObservationReferenceUseRepository.get(local.id) is None
    assert ReferenceCloudSyncStateRepository.list_use_tombstones("user-1") == []
    marker = ReferenceCloudSyncStateRepository.get_use_remote_tombstone(
        "user-1", local.id
    )
    assert marker is not None
    assert marker["row_version"] == 2


def test_remote_use_and_source_tombstones_apply_child_first(databases):
    _, _, measurement_set = _seed_graph_and_observation()
    local = ObservationReferenceUseRepository.attach(1, measurement_set.id)
    _acknowledge_local_use(local.id)
    rows = _library_rows()
    rows["measurement_set"] = {
        **rows["measurement_set"],
        "row_version": 2,
        "updated_at": "2026-08-01T00:00:02Z",
        "deleted_at": "2026-08-01T00:00:03Z",
    }
    remote_use = _use_row(
        id=local.id,
        row_version=2,
        updated_at="2026-08-01T00:00:02Z",
        deleted_at="2026-08-01T00:00:03Z",
    )

    result = pull_reference_library(
        PullClient(
            works=[rows["work"]],
            treatments=[rows["treatment"]],
            sets=[rows["measurement_set"]],
            uses=[remote_use],
        )
    )

    assert result.conflicts == ()
    assert result.blocked == ()
    assert result.pulled == 2
    assert ObservationReferenceUseRepository.get(local.id) is None
    assert MeasurementSetRepository.get(measurement_set.id) is None
    assert ReferenceCloudSyncStateRepository.get_use_pull_cursor("user-1") == (
        "2026-08-01T00:00:02Z",
        local.id,
    )
    assert ReferenceCloudSyncStateRepository.get_library_pull_cursor(
        "user-1", "measurement_set"
    ) == ("2026-08-01T00:00:02Z", "set-1")


def test_overlapping_remote_use_change_conflicts_without_rewriting_snapshot(databases):
    _, _, measurement_set = _seed_graph_and_observation()
    local = ObservationReferenceUseRepository.attach(1, measurement_set.id, note="base")
    _acknowledge_local_use(local.id)
    original_snapshot = ObservationReferenceUseRepository.get(local.id).snapshot_json
    ObservationReferenceUseRepository.update(local.id, note="local")
    remote = _use_row(
        id=local.id,
        note="remote",
        row_version=2,
        updated_at="2026-08-01T00:00:02Z",
    )

    result = pull_reference_library(PullClient(uses=[remote]))

    state = ReferenceCloudSyncStateRepository.get_use(local.id)
    assert result.conflicts == (f"observation_use:{local.id}",)
    assert ObservationReferenceUseRepository.get(local.id).note == "local"
    assert ObservationReferenceUseRepository.get(local.id).snapshot_json == original_snapshot
    assert state.sync_status == "conflict"
    assert state.cloud_row_version == 1
    assert ReferenceCloudSyncStateRepository.get_use_pull_cursor("user-1") is None
    assert ReferenceCloudSyncStateRepository.get_library_pull_cursor(
        "user-1", "work"
    ) is None


def test_pending_local_use_mutation_keeps_content_dirty_and_advances_baseline_token(databases):
    _, _, measurement_set = _seed_graph_and_observation()
    local = ObservationReferenceUseRepository.attach(1, measurement_set.id, note="base")
    _acknowledge_local_use(local.id)
    ObservationReferenceUseRepository.update(local.id, note="local")
    baseline = ReferenceCloudSyncStateRepository.get_use(local.id).accepted_payload
    remote = _use_row(
        **baseline,
        user_id="user-1",
        row_version=2,
        created_at="2026-08-01T00:00:00Z",
        updated_at="2026-08-01T00:00:02Z",
        deleted_at=None,
    )

    result = pull_reference_library(PullClient(uses=[remote]))

    state = ReferenceCloudSyncStateRepository.get_use(local.id)
    assert result.errors == ()
    assert ObservationReferenceUseRepository.get(local.id).note == "local"
    assert state.cloud_row_version == 2
    assert state.sync_status == "dirty"


def test_library_conflict_holds_observation_use_cursor(databases):
    _, _, measurement_set = _seed_graph_and_observation()
    local = ObservationReferenceUseRepository.attach(1, measurement_set.id)
    _acknowledge_local_use(local.id)
    ReferenceWorkRepository.update("work-1", {"title": "local title"})
    rows = _library_rows()
    rows["work"] = {
        **rows["work"],
        "title": "remote title",
        "revision": 2,
        "row_version": 2,
        "updated_at": "2026-08-01T00:00:02Z",
    }
    use_payload = load_use_payload(local.id)
    remote_use = {
        **use_payload,
        "user_id": "user-1",
        "row_version": 1,
        "created_at": "2026-08-01T00:00:00Z",
        "updated_at": "2026-08-01T00:00:01Z",
        "deleted_at": None,
    }

    result = pull_reference_library(
        PullClient(
            works=[rows["work"]],
            treatments=[rows["treatment"]],
            sets=[rows["measurement_set"]],
            uses=[remote_use],
        )
    )

    assert result.conflicts == ("work:work-1",)
    assert ReferenceCloudSyncStateRepository.get_use_pull_cursor("user-1") is None
    assert ReferenceCloudSyncStateRepository.get_library_pull_cursor(
        "user-1", "work"
    ) is None


@pytest.mark.parametrize(
    ("remote", "expected_fragment"),
    [
        (_use_row(observation_id=999), "observation"),
        (
            _use_row(
                reference_measurement_set_id="missing-set",
                snapshot_json={
                    **_snapshot(),
                    "reference_measurement_set_id": "missing-set",
                },
            ),
            "reference_measurement_set",
        ),
    ],
)
def test_missing_observation_or_set_blocks_use_pull_without_cursor(
    databases, remote, expected_fragment
):
    _seed_graph_and_observation()

    result = pull_reference_library(PullClient(uses=[remote]))

    assert result.pulled == 0
    assert result.blocked
    assert expected_fragment in " ".join(result.blocked)
    assert ObservationReferenceUseRepository.get(remote["id"]) is None
    assert ReferenceCloudSyncStateRepository.get_use_pull_cursor("user-1") is None


def test_remote_use_uniqueness_collision_preserves_existing_local_attachment(databases):
    _, _, measurement_set = _seed_graph_and_observation()
    local = ObservationReferenceUseRepository.attach(1, measurement_set.id, note="local")
    _acknowledge_local_use(local.id)
    remote = _use_row(id="other-use", note="remote")

    result = pull_reference_library(PullClient(uses=[remote]))

    uses = ObservationReferenceUseRepository.list_for_observation(1)
    assert [(use.id, use.note) for use in uses] == [(local.id, "local")]
    assert result.conflicts == (f"observation_use:{local.id}",)
    assert ReferenceCloudSyncStateRepository.get_use_pull_cursor("user-1") is None


def test_use_reader_failure_rolls_back_and_restart_is_idempotent(databases):
    _seed_graph_and_observation()
    remote = _use_row()
    failed = PullClient(uses=[remote])
    failed.failures.add("observation_use")

    first = pull_reference_library(failed)

    assert first.retryable_errors
    assert ObservationReferenceUseRepository.get("use-1") is None
    assert ReferenceCloudSyncStateRepository.get_use_pull_cursor("user-1") is None

    restarted = PullClient(uses=[remote])
    second = pull_reference_library(restarted)
    third = pull_reference_library(restarted)

    assert second == ReferenceSyncResult(pulled=1)
    assert third == ReferenceSyncResult()
    assert restarted.calls == [
        "work", "treatment", "measurement_set", "observation_use",
        "work", "treatment", "measurement_set", "observation_use",
    ]


def test_use_apply_failure_rolls_back_library_rows_baselines_and_cursors(
    databases, monkeypatch
):
    _seed_graph_and_observation()
    rows = _library_rows()
    rows["work"] = {
        **rows["work"],
        "title": "must roll back",
        "revision": 2,
        "row_version": 2,
        "updated_at": "2026-08-01T00:00:02Z",
    }

    def fail_use_apply(*_args, **_kwargs):
        from database.reference_sync_reconciliation import (
            ReferencePullReconciliationError,
        )

        raise ReferencePullReconciliationError("injected use apply failure")

    monkeypatch.setattr(
        "database.reference_use_sync_reconciliation.reconcile_observation_reference_use_feed",
        fail_use_apply,
    )

    result = pull_reference_library(
        PullClient(
            works=[rows["work"]],
            treatments=[rows["treatment"]],
            sets=[rows["measurement_set"]],
        )
    )

    assert result.terminal_errors
    assert ReferenceWorkRepository.get("work-1").title == "Work"
    state = ReferenceCloudSyncStateRepository.get_library("work", "work-1")
    assert state.cloud_row_version == 1
    assert ReferenceCloudSyncStateRepository.get_library_pull_cursor(
        "user-1", "work"
    ) is None


@pytest.mark.parametrize("detach_first", [False, True])
def test_remote_tombstone_from_other_account_fails_closed(databases, detach_first):
    _, _, measurement_set = _seed_graph_and_observation()
    local = ObservationReferenceUseRepository.attach(1, measurement_set.id)
    _acknowledge_local_use(local.id)
    if detach_first:
        ObservationReferenceUseRepository.detach(local.id)
    remote = _use_row(
        id=local.id,
        user_id="user-2",
        row_version=2,
        deleted_at="2026-08-29T00:00:03Z",
    )
    feed = stage_observation_reference_use_feed("user-2", (remote,))

    result = reconcile_observation_reference_use_feed("user-2", feed)

    assert result.conflicts == (f"observation_use:{local.id}",)
    if detach_first:
        tombstone = ReferenceCloudSyncStateRepository.list_use_tombstones("user-1")
        assert [item.use_id for item in tombstone] == [local.id]
        assert tombstone[0].sync_status == "conflict"
    else:
        assert ObservationReferenceUseRepository.get(local.id) is not None
        assert ReferenceCloudSyncStateRepository.get_use(local.id).cloud_user_id == "user-1"
