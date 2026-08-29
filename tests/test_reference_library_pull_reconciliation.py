from __future__ import annotations

from dataclasses import replace

import pytest

from database import schema
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
)
from database.reference_sync_state import (
    ReferenceCloudSyncState,
    ReferenceCloudSyncStateRepository,
    load_library_payload,
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
    return reference_path


def _work_row(**updates):
    row = {
        "user_id": "user-1",
        "id": "work-1",
        "type": "book",
        "citation_key": None,
        "authors_json": [{"family": "Remote"}],
        "editors_json": [],
        "title": "Remote work",
        "container_title": None,
        "year": 2024,
        "edition": None,
        "publisher": None,
        "place": None,
        "volume": None,
        "issue": None,
        "pages": None,
        "doi": None,
        "isbn": None,
        "url": None,
        "language": None,
        "short_label": "Remote 2024",
        "citation_override": None,
        "revision": 1,
        "row_version": 1,
        "created_at": "2026-08-01T00:00:00Z",
        "updated_at": "2026-08-01T00:00:01Z",
        "deleted_at": None,
    }
    row.update(updates)
    return row


def _treatment_row(**updates):
    row = {
        "user_id": "user-1",
        "id": "treatment-1",
        "reference_work_id": "work-1",
        "taxon_id": "taxon-1",
        "name_as_published": "Russula paludosa",
        "page_from": 12,
        "page_to": 13,
        "locator_text": "pp. 12-13",
        "treatment_notes": None,
        "revision": 1,
        "row_version": 1,
        "created_at": "2026-08-01T00:00:02Z",
        "updated_at": "2026-08-01T00:00:03Z",
        "deleted_at": None,
    }
    row.update(updates)
    return row


def _set_row(**updates):
    row = {
        "user_id": "user-1",
        "id": "set-1",
        "taxon_treatment_id": "treatment-1",
        "character": "spore_size",
        "raw_text": "7-9 x 5-6 um",
        "data_kind": "range",
        "length_min": 7.0,
        "length_core_min": None,
        "length_core_max": None,
        "length_max": 9.0,
        "width_min": 5.0,
        "width_core_min": None,
        "width_core_max": None,
        "width_max": 6.0,
        "q_min": None,
        "q_max": None,
        "q_mean": None,
        "length_mean": None,
        "width_mean": None,
        "sample_size": None,
        "specimen_count": None,
        "mount_medium": None,
        "stain": None,
        "preparation": None,
        "measurement_method": None,
        "notes": None,
        "raw_points_json": None,
        "supersedes_id": None,
        "revision": 1,
        "row_version": 1,
        "created_at": "2026-08-01T00:00:04Z",
        "updated_at": "2026-08-01T00:00:05Z",
        "deleted_at": None,
    }
    row.update(updates)
    return row


class PullClient:
    def __init__(self, *, works=None, treatments=None, sets=None, uses=None):
        self.user_id = "user-1"
        self.works = list(works or [])
        self.treatments = list(treatments or [])
        self.sets = list(sets or [])
        self.uses = list(uses or [])
        self.calls: list[str] = []
        self.failure: str | None = None

    def _read(self, kind, rows):
        self.calls.append(f"list:{kind}")
        if self.failure == kind:
            raise CloudTemporarilyUnavailableError("page two failed")
        return list(rows)

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


def _seed_acknowledged_graph():
    work = ReferenceWorkRepository.create(
        ReferenceWork(
            "work-1",
            "book",
            "Remote work",
            "Remote 2024",
            authors_json='[{"family":"Remote"}]',
            year=2024,
        )
    )
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            "treatment-1",
            work.id,
            "Russula paludosa",
            taxon_id="taxon-1",
            page_from=12,
            page_to=13,
            locator_text="pp. 12-13",
        )
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
                kind,
                entity_id,
                "user-1",
                "acknowledged",
                1,
                payload,
                "clean",
            )
        )


def test_fresh_complete_graph_is_imported_parent_first_and_clean(databases):
    client = PullClient(
        works=[_work_row()], treatments=[_treatment_row()], sets=[_set_row()]
    )

    result = pull_reference_library(client)

    assert result == ReferenceSyncResult(pulled=3)
    assert ReferenceWorkRepository.get("work-1").title == "Remote work"
    assert TaxonTreatmentRepository.get("treatment-1").reference_work_id == "work-1"
    assert MeasurementSetRepository.get("set-1").taxon_treatment_id == "treatment-1"
    for kind, entity_id in (
        ("work", "work-1"),
        ("treatment", "treatment-1"),
        ("measurement_set", "set-1"),
    ):
        state = ReferenceCloudSyncStateRepository.get_library(kind, entity_id)
        assert state.cloud_row_version == 1
        assert state.sync_status == "clean"
    assert client.calls == [
        "list:work",
        "list:treatment",
        "list:measurement_set",
        "list:observation_use",
    ]
    assert ReferenceCloudSyncStateRepository.get_library_pull_cursor(
        "user-1", "measurement_set"
    ) == ("2026-08-01T00:00:05Z", "set-1")


def test_remote_update_applies_and_repeated_pull_is_idempotent(databases):
    _seed_acknowledged_graph()
    client = PullClient(
        works=[_work_row(title="Cloud title", revision=2, row_version=2)],
        treatments=[_treatment_row(name_as_published="Russula paludosa", row_version=1)],
        sets=[_set_row(row_version=1)],
    )

    first = pull_reference_library(client)
    updated_at = ReferenceWorkRepository.get("work-1").updated_at
    second = pull_reference_library(client)

    assert first.pulled == 1
    assert second == ReferenceSyncResult()
    assert ReferenceWorkRepository.get("work-1").title == "Cloud title"
    assert ReferenceWorkRepository.get("work-1").updated_at == updated_at


def test_nonoverlapping_offline_edits_merge_and_remain_dirty(databases):
    _seed_acknowledged_graph()
    ReferenceWorkRepository.update("work-1", {"title": "Local title"})
    client = PullClient(
        works=[
            _work_row(
                title="Remote work",
                short_label="Remote label",
                year=2024,
                revision=2,
                row_version=2,
            )
        ],
        treatments=[_treatment_row()],
        sets=[_set_row()],
    )

    result = pull_reference_library(client)

    work = ReferenceWorkRepository.get("work-1")
    state = ReferenceCloudSyncStateRepository.get_library("work", "work-1")
    assert work.title == "Local title"
    assert work.short_label == "Remote label"
    assert work.revision == 3
    assert state.cloud_row_version == 2
    assert state.sync_status == "dirty"
    assert result.pulled == 1


def test_overlapping_edit_records_conflict_without_overwriting_local(databases):
    _seed_acknowledged_graph()
    ReferenceWorkRepository.update("work-1", {"title": "Local title"})
    client = PullClient(
        works=[_work_row(title="Remote title", year=2020, revision=2, row_version=2)],
        treatments=[_treatment_row()],
        sets=[_set_row()],
    )

    result = pull_reference_library(client)

    state = ReferenceCloudSyncStateRepository.get_library("work", "work-1")
    assert ReferenceWorkRepository.get("work-1").title == "Local title"
    assert state.cloud_row_version == 1
    assert state.sync_status == "conflict"
    assert state.conflict["reason"] == "overlapping_remote_change"
    assert state.conflict["overlapping_fields"] == ["title"]
    assert result.conflicts == ("work:work-1",)
    assert ReferenceCloudSyncStateRepository.get_library_pull_cursor(
        "user-1", "work"
    ) is None


def test_pending_local_change_survives_remote_baseline_and_stale_token_advances(databases):
    _seed_acknowledged_graph()
    ReferenceWorkRepository.update("work-1", {"title": "Local title"})
    baseline = ReferenceCloudSyncStateRepository.get_library(
        "work", "work-1"
    ).accepted_payload
    client = PullClient(
        works=[{**_work_row(), **baseline, "user_id": "user-1", "row_version": 4,
                "created_at": "2026-08-01T00:00:00Z",
                "updated_at": "2026-08-01T00:00:06Z", "deleted_at": None}],
        treatments=[_treatment_row()],
        sets=[_set_row()],
    )

    pull_reference_library(client)

    state = ReferenceCloudSyncStateRepository.get_library("work", "work-1")
    assert ReferenceWorkRepository.get("work-1").title == "Local title"
    assert state.cloud_row_version == 4
    assert state.sync_status == "dirty"


def test_explicit_remote_tombstones_apply_child_first_without_echo_intent(databases):
    _seed_acknowledged_graph()
    client = PullClient(
        works=[_work_row(row_version=2, deleted_at="2026-08-02T00:00:03Z")],
        treatments=[_treatment_row(row_version=2, deleted_at="2026-08-02T00:00:02Z")],
        sets=[_set_row(row_version=2, deleted_at="2026-08-02T00:00:01Z")],
    )

    result = pull_reference_library(client)

    assert result.pulled == 3
    assert ReferenceWorkRepository.get("work-1") is None
    assert TaxonTreatmentRepository.get("treatment-1") is None
    assert MeasurementSetRepository.get("set-1") is None
    assert ReferenceCloudSyncStateRepository.list_library_tombstones("user-1") == []
    assert ReferenceCloudSyncStateRepository.get_library_remote_tombstone(
        "user-1", "work", "work-1"
    )["row_version"] == 2


def test_remote_delete_conflicts_with_newer_local_intent(databases):
    _seed_acknowledged_graph()
    ReferenceWorkRepository.update("work-1", {"title": "Unsynced"})
    client = PullClient(
        works=[_work_row(row_version=2, deleted_at="2026-08-02T00:00:00Z")],
        treatments=[_treatment_row(row_version=2, deleted_at="2026-08-02T00:00:00Z")],
        sets=[_set_row(row_version=2, deleted_at="2026-08-02T00:00:00Z")],
    )

    result = pull_reference_library(client)

    assert ReferenceWorkRepository.get("work-1").title == "Unsynced"
    state = ReferenceCloudSyncStateRepository.get_library("work", "work-1")
    assert state.sync_status == "conflict"
    assert state.conflict["reason"] == "remote_delete_local_change"
    assert result.conflicts == ("work:work-1",)


def test_missing_remote_dependency_rejects_whole_feed_without_cursor_or_rows(databases):
    client = PullClient(treatments=[_treatment_row()])

    result = pull_reference_library(client)

    assert result.pulled == 0
    assert result.retryable_errors
    assert result.blocked == ("reference_graph",)
    assert TaxonTreatmentRepository.get("treatment-1") is None
    assert ReferenceCloudSyncStateRepository.get_library_pull_cursor(
        "user-1", "treatment"
    ) is None


def test_failed_final_complete_feed_leaves_earlier_rows_and_cursors_uncommitted(databases):
    client = PullClient(works=[_work_row()], treatments=[_treatment_row()])
    client.failure = "measurement_set"

    result = pull_reference_library(client)

    assert result.retryable_errors
    assert ReferenceWorkRepository.get("work-1") is None
    assert ReferenceCloudSyncStateRepository.get_library_pull_cursor(
        "user-1", "work"
    ) is None


def test_remote_tombstone_for_absent_row_is_retained_only_as_restore_token(databases):
    client = PullClient(
        works=[_work_row(row_version=3, deleted_at="2026-08-02T00:00:00Z")]
    )

    first = pull_reference_library(client)
    second = pull_reference_library(client)

    assert first.pulled == 1
    assert second == ReferenceSyncResult()
    assert ReferenceCloudSyncStateRepository.list_library_tombstones("user-1") == []
    marker = ReferenceCloudSyncStateRepository.get_library_remote_tombstone(
        "user-1", "work", "work-1"
    )
    assert marker["row_version"] == 3


def test_recorded_conflict_is_not_overwritten_by_later_pull(databases):
    _seed_acknowledged_graph()
    state = ReferenceCloudSyncStateRepository.get_library("work", "work-1")
    ReferenceCloudSyncStateRepository.save_library(
        replace(state, sync_status="conflict", conflict={"reason": "review_me"})
    )
    client = PullClient(
        works=[_work_row(title="Cloud title", revision=2, row_version=2)],
        treatments=[_treatment_row()],
        sets=[_set_row()],
    )

    result = pull_reference_library(client)

    saved = ReferenceCloudSyncStateRepository.get_library("work", "work-1")
    assert ReferenceWorkRepository.get("work-1").title == "Remote work"
    assert saved.conflict == {"reason": "review_me"}
    assert result.conflicts == ("work:work-1",)


def test_local_delete_intent_uses_remote_live_token_without_resurrecting(databases):
    work = ReferenceWorkRepository.create(
        ReferenceWork(
            "work-1", "book", "Remote work", "Remote 2024",
            authors_json='[{"family":"Remote"}]', year=2024,
        )
    )
    baseline = load_library_payload("work", work.id)
    ReferenceCloudSyncStateRepository.save_library(
        ReferenceCloudSyncState(
            "work", work.id, "user-1", "acknowledged", 1, baseline, "clean"
        )
    )
    ReferenceWorkRepository.delete(work.id)
    client = PullClient(works=[_work_row(row_version=2)])

    result = pull_reference_library(client)

    tombstone = ReferenceCloudSyncStateRepository.list_library_tombstones("user-1")[0]
    assert ReferenceWorkRepository.get(work.id) is None
    assert tombstone.expected_row_version == 2
    assert tombstone.sync_status == "dirty"
    assert result.conflicts == ()


def test_recreated_remote_tombstone_retains_restore_intent_across_restart(databases):
    remote = _work_row(row_version=3, deleted_at="2026-08-02T00:00:00Z")
    client = PullClient(works=[remote])
    pull_reference_library(client)
    ReferenceWorkRepository.create(
        ReferenceWork("work-1", "book", "Restored", "Restored")
    )

    result = pull_reference_library(client)

    state = ReferenceCloudSyncStateRepository.get_library("work", "work-1")
    assert ReferenceWorkRepository.get("work-1").title == "Restored"
    assert state.remote_identity_state == "acknowledged"
    assert state.cloud_row_version == 3
    assert state.sync_status == "dirty"
    assert state.accepted_payload["deleted"] is True
    assert ReferenceCloudSyncStateRepository.get_library_remote_tombstone(
        "user-1", "work", "work-1"
    ) is None
    assert result.conflicts == ()


def test_parent_conflict_blocks_remote_descendant_update(databases):
    _seed_acknowledged_graph()
    ReferenceWorkRepository.update("work-1", {"title": "Local work"})
    client = PullClient(
        works=[_work_row(title="Remote work changed", revision=2, row_version=2)],
        treatments=[
            _treatment_row(
                locator_text="remote locator", revision=2, row_version=2
            )
        ],
        sets=[_set_row()],
    )

    result = pull_reference_library(client)

    assert TaxonTreatmentRepository.get("treatment-1").locator_text == "pp. 12-13"
    assert "work:work-1" in result.conflicts
    assert "treatment:treatment-1:dependency_conflict" in result.blocked
    assert ReferenceCloudSyncStateRepository.get_library_pull_cursor(
        "user-1", "work"
    ) is None


def test_stale_remote_token_rolls_back_entire_graph(databases):
    _seed_acknowledged_graph()
    state = ReferenceCloudSyncStateRepository.get_library("work", "work-1")
    ReferenceCloudSyncStateRepository.save_library(
        replace(state, cloud_row_version=5)
    )
    client = PullClient(
        works=[_work_row(title="Stale", revision=2, row_version=4)],
        treatments=[_treatment_row()],
        sets=[_set_row()],
    )

    result = pull_reference_library(client)

    assert result.retryable_errors
    assert ReferenceWorkRepository.get("work-1").title == "Remote work"
    assert ReferenceCloudSyncStateRepository.get_library(
        "work", "work-1"
    ).cloud_row_version == 5
    assert ReferenceCloudSyncStateRepository.get_library_pull_cursor(
        "user-1", "work"
    ) is None


def test_remote_set_tombstone_preserves_active_observation_use(databases):
    _seed_acknowledged_graph()
    connection = schema.get_connection()
    try:
        connection.execute(
            "INSERT INTO observations (id, date) VALUES (1, '2026-08-29')"
        )
        connection.execute(
            """
            INSERT INTO observation_reference_uses(
                id, observation_id, reference_measurement_set_id, role,
                reference_revision, snapshot_json
            ) VALUES ('use-1', 1, 'set-1', 'compared', 1, '{}')
            """
        )
        connection.commit()
    finally:
        connection.close()
    client = PullClient(
        works=[_work_row(row_version=2, deleted_at="2026-08-02T00:00:00Z")],
        treatments=[_treatment_row(row_version=2, deleted_at="2026-08-02T00:00:00Z")],
        sets=[_set_row(row_version=2, deleted_at="2026-08-02T00:00:00Z")],
    )

    result = pull_reference_library(client)

    assert MeasurementSetRepository.get("set-1") is not None
    state = ReferenceCloudSyncStateRepository.get_library(
        "measurement_set", "set-1"
    )
    assert state.sync_status == "conflict"
    assert state.conflict["reason"] == "remote_delete_local_use"
    connection = schema.get_connection()
    try:
        count = connection.execute(
            "SELECT COUNT(*) FROM observation_reference_uses WHERE id='use-1'"
        ).fetchone()[0]
    finally:
        connection.close()
    assert count == 1
    assert "measurement_set:set-1" in result.conflicts


def test_stale_remote_tombstone_is_retryable_and_rolls_back(databases):
    _seed_acknowledged_graph()
    state = ReferenceCloudSyncStateRepository.get_library("work", "work-1")
    ReferenceCloudSyncStateRepository.save_library(
        replace(state, cloud_row_version=5)
    )
    client = PullClient(
        works=[_work_row(row_version=4, deleted_at="2026-08-02T00:00:00Z")],
        treatments=[_treatment_row(row_version=2, deleted_at="2026-08-02T00:00:00Z")],
        sets=[_set_row(row_version=2, deleted_at="2026-08-02T00:00:00Z")],
    )

    result = pull_reference_library(client)

    assert result.retryable_errors
    assert ReferenceWorkRepository.get("work-1") is not None
    assert TaxonTreatmentRepository.get("treatment-1") is not None
    assert MeasurementSetRepository.get("set-1") is not None
    assert ReferenceCloudSyncStateRepository.get_library_pull_cursor(
        "user-1", "work"
    ) is None
