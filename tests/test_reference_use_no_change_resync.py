"""Issue #11: a no-change sync must not re-select clean observation uses.

Mechanism: local rows store ``selected_at`` as SQLite ``YYYY-MM-DD HH:MM:SS``
(UTC) while the server returns ``timestamptz`` as ISO-8601 with an offset.
Comparing the two raw strings made every pulled clean use look locally
changed (``local != remote``, ``remote == baseline``), so the pull marked it
dirty and the next push re-sent it; the server answered ``no_change``.
"""
from __future__ import annotations

import json

import pytest

from database import schema
from database.reference_library import ObservationReferenceUseRepository
from database.reference_sync_planner import build_reference_sync_plan
from database.reference_sync_state import (
    ReferenceCloudSyncState,
    ReferenceCloudSyncStateRepository,
    load_use_payload,
)
from utils.reference_cloud_sync import pull_reference_library
from tests.test_observation_reference_use_pull import (  # noqa: F401
    PullClient,
    _seed_graph_and_observation,
    _use_row,
    databases,
)


def _server_form(local_selected_at: str) -> str:
    return local_selected_at.replace(" ", "T") + "+00:00"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-10-02 18:20:43", "2026-10-02T18:20:43+00:00"),
        ("2026-10-02T18:20:43+00:00", "2026-10-02T18:20:43+00:00"),
        ("2026-10-02T18:20:43Z", "2026-10-02T18:20:43+00:00"),
        ("2026-10-02T20:20:43+02:00", "2026-10-02T18:20:43+00:00"),
        ("2026-10-02T18:20:43.123456+00:00", "2026-10-02T18:20:43.123456+00:00"),
        ("not a timestamp", "not a timestamp"),
    ],
)
def test_use_timestamp_has_one_canonical_form(value, expected):
    from database.reference_sync_state import canonical_use_timestamp

    assert canonical_use_timestamp(value) == expected


def _raw_selected_at(use_id: str) -> str:
    connection = schema.get_connection()
    try:
        return connection.execute(
            "SELECT selected_at FROM observation_reference_uses WHERE id=?", (use_id,)
        ).fetchone()[0]
    finally:
        connection.close()


def _acknowledged_clean_use():
    _, _, measurement_set = _seed_graph_and_observation()
    use = ObservationReferenceUseRepository.attach(1, measurement_set.id, role="compared")
    local_selected_at = _raw_selected_at(use.id)
    assert "T" not in local_selected_at  # SQLite-style local storage
    accepted = load_use_payload(use.id)
    accepted["selected_at"] = _server_form(local_selected_at)
    ReferenceCloudSyncStateRepository.save_use(
        ReferenceCloudSyncState(
            "observation_use", use.id, "user-1", "acknowledged", 1, accepted, "clean"
        )
    )
    connection = schema.get_connection()
    try:
        connection.execute(
            "UPDATE observation_reference_use_cloud_sync_state SET accepted_payload_json=? "
            "WHERE use_id=?",
            (json.dumps(accepted, sort_keys=True), use.id),
        )
        connection.commit()
    finally:
        connection.close()
    remote = _use_row(
        id=use.id,
        role="compared",
        note=None,
        selected_at=_server_form(local_selected_at),
        snapshot_json=json.loads(use.snapshot_json),
        reference_revision=use.reference_revision,
    )
    return use, remote


def _planned_use_ids() -> list[str]:
    plan = build_reference_sync_plan("user-1")
    return [item.entity_id for item in plan.live if item.entity_type == "observation_use"]


@pytest.mark.parametrize("legacy_baseline", [False, True])
def test_pulled_clean_use_stays_clean_and_is_not_planned(databases, legacy_baseline):
    use, remote = _acknowledged_clean_use()
    if legacy_baseline:
        # A baseline persisted in the local representation by an older build.
        connection = schema.get_connection()
        try:
            accepted = dict(load_use_payload(use.id))
            accepted["selected_at"] = _raw_selected_at(use.id)
            connection.execute(
                "UPDATE observation_reference_use_cloud_sync_state "
                "SET accepted_payload_json=? WHERE use_id=?",
                (json.dumps(accepted, sort_keys=True), use.id),
            )
            connection.commit()
        finally:
            connection.close()

    pull_reference_library(PullClient(uses=[remote]))

    state = ReferenceCloudSyncStateRepository.get_use(use.id)
    assert state.sync_status == "clean"
    assert state.accepted_payload == load_use_payload(use.id)
    assert _planned_use_ids() == []


def test_changed_use_is_planned_alone(databases):
    use, remote = _acknowledged_clean_use()
    pull_reference_library(PullClient(uses=[remote]))
    assert _planned_use_ids() == []

    ObservationReferenceUseRepository.update(use.id, note="local edit")

    assert _planned_use_ids() == [use.id]


@pytest.mark.parametrize("status", ["dirty", "retry"])
def test_pending_use_is_still_planned(databases, status):
    use, _ = _acknowledged_clean_use()
    state = ReferenceCloudSyncStateRepository.get_use(use.id)
    ReferenceCloudSyncStateRepository.save_use(
        ReferenceCloudSyncState(
            "observation_use", use.id, "user-1", "acknowledged", 1,
            state.accepted_payload, status, retry_count=1 if status == "retry" else 0,
        )
    )
    assert _planned_use_ids() == [use.id]


# --- Follow-up: a no-change sync must not rewrite local status rows ---------

from tests.test_observation_reference_use_sync import (  # noqa: E402
    ReferenceGraphClient,
    _create_graph_and_use,
)
from utils.reference_cloud_sync import sync_reference_library  # noqa: E402


def _server_timestamps(client):
    # Echo selected_at the way PostgREST renders timestamptz.
    for (kind, _), row in client.remote_rows.items():
        if kind == "observation_use" and " " in str(row.get("selected_at")):
            row["selected_at"] = _server_form(row["selected_at"])


def _status_tables():
    import sqlite3

    observation = schema.get_connection()
    reference = schema.get_reference_connection()
    observation.row_factory = reference.row_factory = sqlite3.Row
    try:
        return {
            "uses": [dict(row) for row in observation.execute(
                "SELECT * FROM observation_reference_use_cloud_sync_state ORDER BY use_id"
            )],
            "library": [dict(row) for row in reference.execute(
                "SELECT * FROM reference_cloud_sync_state ORDER BY entity_type, entity_id"
            )],
        }
    finally:
        observation.close()
        reference.close()


def _age_status_rows() -> None:
    """Preset old timestamps so any rewrite is visible (CURRENT_TIMESTAMP has
    one-second resolution and every sync here lands in the same second)."""
    for table, reference in (
        ("observation_reference_use_cloud_sync_state", False),
        ("reference_cloud_sync_state", True),
    ):
        connection = schema.get_reference_connection() if reference else schema.get_connection()
        try:
            connection.execute(
                f"UPDATE {table} SET updated_at='2000-01-01 00:00:00', "
                "last_attempted_at='2000-01-01T00:00:00+00:00'"
            )
            connection.commit()
        finally:
            connection.close()


def test_no_change_sync_leaves_every_status_row_byte_identical(databases):
    _create_graph_and_use()
    client = ReferenceGraphClient()
    sync_reference_library(client)
    _server_timestamps(client)
    sync_reference_library(client)  # pull sees the server form once
    _age_status_rows()
    before = _status_tables()
    calls_before = len(client.calls)

    sync_reference_library(client)

    assert len(client.calls) == calls_before
    assert _status_tables() == before


def test_real_change_updates_only_that_use_state_row(databases):
    _, _, measurement_set, use = _create_graph_and_use()
    connection = schema.get_connection()
    try:
        connection.execute(
            "INSERT INTO observations (id, date, cloud_id) VALUES (2, '2026-08-30', '102')"
        )
        connection.commit()
    finally:
        connection.close()
    second = ObservationReferenceUseRepository.attach(
        2, measurement_set.id, role="compared"
    )
    assert second.id != use.id
    client = ReferenceGraphClient()
    sync_reference_library(client)
    _server_timestamps(client)
    sync_reference_library(client)
    before = _status_tables()
    by_id = {row["use_id"]: row for row in before["uses"]}

    ObservationReferenceUseRepository.update(second.id, note="changed")
    sync_reference_library(client)

    after = _status_tables()
    after_by_id = {row["use_id"]: row for row in after["uses"]}
    assert [c[1]["id"] for c in client.calls[-1:]] == [second.id]
    assert after_by_id[use.id] == by_id[use.id]
    changed = after_by_id[second.id]
    assert changed["sync_status"] == "clean"
    assert changed["last_attempted_at"] != by_id[second.id]["last_attempted_at"]
    assert after["library"] == before["library"]


# --- Pull-detected conflicts are not upload attempts -------------------------

_SENTINEL = "2000-01-01T00:00:00+00:00"


def _stamp(table: str, where: str, args: tuple, *, reference: bool = False) -> None:
    connection = schema.get_reference_connection() if reference else schema.get_connection()
    try:
        connection.execute(f"UPDATE {table} SET last_attempted_at=? WHERE {where}", (_SENTINEL, *args))
        connection.commit()
    finally:
        connection.close()


def test_pull_detected_use_conflict_keeps_last_attempted_at(databases):
    _, _, measurement_set = _seed_graph_and_observation()
    use = ObservationReferenceUseRepository.attach(1, measurement_set.id, note="base")
    accepted = load_use_payload(use.id)
    ReferenceCloudSyncStateRepository.save_use(
        ReferenceCloudSyncState(
            "observation_use", use.id, "user-1", "acknowledged", 1, accepted, "clean"
        )
    )
    ObservationReferenceUseRepository.update(use.id, note="local")
    _stamp("observation_reference_use_cloud_sync_state", "use_id=?", (use.id,))
    remote = _use_row(id=use.id, note="remote", row_version=2,
                      updated_at="2026-08-01T00:00:02Z")

    result = pull_reference_library(PullClient(uses=[remote]))

    state = ReferenceCloudSyncStateRepository.get_use(use.id)
    assert result.conflicts == (f"observation_use:{use.id}",)
    assert state.sync_status == "conflict"
    assert state.last_attempted_at == _SENTINEL


def test_pull_detected_library_conflict_keeps_last_attempted_at(databases):
    from tests.test_observation_reference_use_pull import _library_rows

    _seed_graph_and_observation()
    rows = _library_rows()
    connection = schema.get_reference_connection()
    try:
        connection.execute(
            "UPDATE reference_measurement_sets SET raw_text='local edit' WHERE id='set-1'"
        )
        connection.commit()
    finally:
        connection.close()
    _stamp(
        "reference_cloud_sync_state", "entity_type='measurement_set' AND entity_id=?",
        ("set-1",), reference=True,
    )
    remote_set = {**rows["measurement_set"], "raw_text": "remote edit",
                  "row_version": 2, "updated_at": "2026-08-01T00:00:02Z"}

    result = pull_reference_library(PullClient(sets=[remote_set]))

    state = ReferenceCloudSyncStateRepository.get_library("measurement_set", "set-1")
    assert "measurement_set:set-1" in result.conflicts
    assert state.sync_status == "conflict"
    assert state.last_attempted_at == _SENTINEL


def test_pull_detected_library_tombstone_conflict_keeps_last_attempted_at(databases):
    from database.reference_library import MeasurementSetRepository
    from tests.test_observation_reference_use_pull import _library_rows  # noqa: F811

    _seed_graph_and_observation()
    rows = _library_rows()
    remote_set = {**rows["measurement_set"], "raw_text": "remote edit",
                  "row_version": 2, "updated_at": "2026-08-01T00:00:02Z"}
    client = PullClient(sets=[remote_set])  # reads the live graph; build first
    MeasurementSetRepository.delete("set-1")
    _stamp(
        "reference_cloud_tombstones", "entity_type='measurement_set' AND entity_id=?",
        ("set-1",), reference=True,
    )

    result = pull_reference_library(client)

    connection = schema.get_reference_connection()
    try:
        status, attempted = connection.execute(
            "SELECT sync_status, last_attempted_at FROM reference_cloud_tombstones "
            "WHERE entity_type='measurement_set' AND entity_id='set-1'"
        ).fetchone()
    finally:
        connection.close()
    assert "measurement_set:set-1" in result.conflicts
    assert status == "conflict"
    assert attempted == _SENTINEL
