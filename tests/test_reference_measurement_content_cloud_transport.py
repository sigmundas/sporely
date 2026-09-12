"""Stage 3C: the measurement-content extension across cloud transport.

Contract: ``docs/reference-data/measurement-content-contract.md`` sections 3–5
and 9; plan sections *Small cloud compatibility mechanism* and *Imports,
baselines and retry behavior*. The cloud side is exercised in ``sporely-web``
(``supabase/tests/reference_measurement_content_extension_test.sql``); here the
desktop registries, adapter, push executor, baselines and pull reconciliation
are driven through the production owners with recording fakes for the RPCs.
"""

from __future__ import annotations

import inspect
import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from database import reference_sync_reconciliation as reconciliation
from database import reference_sync_state as sync_state
from database import schema as _schema
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
)
from database.reference_sync_reconciliation import (
    _extension_write_blocked,
    _measurement_content_error,
)
from database.reference_sync_state import (
    ReferenceCloudSyncStateRepository,
    canonical_library_payload,
    load_library_payload,
    recognize_library_baseline,
)
from references.measurement_content import EXTENSION_FIELDS, SCIENTIFIC_CONTENT_FIELDS
from utils import reference_cloud_adapter as adapter_module
from utils.cloud_sync import CloudTemporarilyUnavailableError, SporelyCloudClient
from utils.reference_cloud_adapter import ReferenceCloudAdapter
from utils.reference_cloud_sync import (
    ReferenceSyncResult,
    pull_reference_library,
    sync_reference_library,
)

from tests.test_reference_cloud_adapter import FakeClient, _row as _adapter_row
from tests.test_reference_library_pull_reconciliation import (
    PullClient,
    _seed_acknowledged_graph,
    _set_row,
    _treatment_row,
    _work_row,
)
from tests.test_reference_library_push_executor import RecordingReferenceClient

FIXTURES = Path(__file__).parent / "fixtures" / "reference_statistics"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


ENHANCED = _load("row_enhanced.json")
LEGACY = _load("row_legacy_only.json")
FUTURE = _load("details_unsupported_future_version.json")
WORK_ID = "0f3e7c2a-5b1d-4e9f-a8c7-6d5e4f3a2b1c"
TREATMENT_ID = ENHANCED["taxon_treatment_id"]
_ROW_FIELDS = {f.name for f in MeasurementSet.__dataclass_fields__.values()}
_TIMESTAMPS = {"created_at": "2026-09-13T00:00:00Z", "updated_at": "2026-09-13T00:00:01Z"}


# --- Fixtures and helpers -----------------------------------------------------


def _point_at(monkeypatch, root: Path) -> tuple[Path, Path]:
    db_path = root / "mushrooms.db"
    ref_path = root / "reference_values.db"
    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(_schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(_schema, "get_reference_database_path", lambda: ref_path)
    monkeypatch.setattr(
        _schema, "get_bundled_reference_database_path", lambda: root / "missing.db"
    )
    _schema.init_database()
    return db_path, ref_path


def _seed_fixture_parents() -> None:
    ReferenceWorkRepository.create(
        ReferenceWork(id=WORK_ID, type="book", title="Fixture", short_label="Fixture 2020")
    )
    TaxonTreatmentRepository.create(
        TaxonTreatment(
            id=TREATMENT_ID, reference_work_id=WORK_ID, name_as_published="Hebeloma fixtura"
        )
    )


@pytest.fixture()
def libs(tmp_path, monkeypatch):
    paths = _point_at(monkeypatch, tmp_path / "a")
    _seed_fixture_parents()
    return paths


def _set(row: dict, **overrides) -> MeasurementSet:
    data = {k: v for k, v in row.items() if k in _ROW_FIELDS}
    data.update(overrides)
    return MeasurementSet(**data)


def _raw_row(ref_path: Path, set_id: str) -> dict | None:
    conn = sqlite3.connect(ref_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM reference_measurement_sets WHERE id=?", (set_id,)
        ).fetchone()
        return dict(row) if row is not None else None
    finally:
        conn.close()


def _details(metrics: dict) -> dict:
    return {"schema_version": 1, "metrics": metrics}


LENGTH_TAG = _details({"length": {"outer_range": {"kind": "reported_extremes"}}})
WIDTH_TAG = _details({"width": {"outer_range": {"kind": "reported_extremes"}}})


def _remote_rows_of(client: RecordingReferenceClient, kind: str) -> list[dict]:
    """The rows a later owner read would return for everything pushed."""
    return [
        {**row, **_TIMESTAMPS}
        for (row_kind, _), row in client.remote_rows.items()
        if row_kind == kind
    ]


def _state(entity_id: str):
    return ReferenceCloudSyncStateRepository.get_library("measurement_set", entity_id)


# --- Registries (contract section 4) -----------------------------------------


def test_every_payload_and_key_registry_carries_the_extension():
    for name in EXTENSION_FIELDS:
        assert name in sync_state._LIBRARY_PAYLOAD_COLUMNS["measurement_set"], name
        assert name in reconciliation._PAYLOAD_COLUMNS["measurement_set"], name
        assert name in adapter_module._MEASUREMENT_SET_KEYS, name
    assert "measurement_details_json" in sync_state._JSON_PAYLOAD_COLUMNS
    assert "measurement_details_json" in reconciliation._JSON_COLUMNS
    read_allowlist = inspect.getsource(SporelyCloudClient.list_reference_measurement_sets)
    for name in EXTENSION_FIELDS:
        assert name in read_allowlist, f"owner read omits {name}"


def test_canonical_payload_uses_decoded_object_equality_for_details(libs):
    created = MeasurementSetRepository.create(_set(ENHANCED))
    local = load_library_payload("measurement_set", created.id)
    assert all(name in local for name in EXTENSION_FIELDS)
    assert local["measurement_details_json"] == json.loads(ENHANCED["measurement_details_json"])
    assert (local["q_core_min"], local["q_core_max"]) == (1.36, 2.19)
    # A remote row carries the JSONB column as an object whose key order is
    # arbitrary; equality is decoded-object equality, never byte equality.
    details = local["measurement_details_json"]
    reordered = {k: details[k] for k in sorted(details, reverse=True)}
    assert list(reordered) != list(details)
    remote = {**local, "measurement_details_json": reordered, "user_id": "u", "row_version": 1}
    assert canonical_library_payload("measurement_set", remote) == local


# --- Adapter (section 9 item 1) ------------------------------------------------


@pytest.mark.parametrize("expected_row_version", [0, 1])
def test_adapter_never_strips_a_null_extension_key(expected_row_version):
    payload = {
        **{k: v for k, v in LEGACY.items() if k in adapter_module._MEASUREMENT_SET_KEYS},
        "raw_points_json": None,
        "deleted": False,
    }
    assert all(payload[name] is None for name in EXTENSION_FIELDS)
    client = FakeClient(
        mutation_response={
            "status": "created" if expected_row_version == 0 else "updated",
            "row": _adapter_row(payload["id"], row_version=expected_row_version + 1),
        }
    )
    ReferenceCloudAdapter(client, "user-1").sync_measurement_set(payload, expected_row_version)
    sent = client.calls[0][1]
    for name in EXTENSION_FIELDS:
        assert name in sent and sent[name] is None, name
    # The historical raw-points create workaround stays limited to its field.
    assert ("raw_points_json" in sent) == (expected_row_version != 0)


def test_adapter_forwards_enhanced_content_unchanged():
    payload = {
        **{k: v for k, v in ENHANCED.items() if k in adapter_module._MEASUREMENT_SET_KEYS},
        "measurement_details_json": json.loads(ENHANCED["measurement_details_json"]),
        "deleted": False,
    }
    client = FakeClient(mutation_response={"status": "created", "row": _adapter_row(payload["id"])})
    ReferenceCloudAdapter(client, "user-1").sync_measurement_set(payload, 0)
    sent = client.calls[0][1]
    assert sent["measurement_details_json"] == payload["measurement_details_json"]
    assert (sent["q_core_min"], sent["q_core_max"]) == (1.36, 2.19)


# --- Push executor, baselines and retries -------------------------------------


def test_push_of_enhanced_row_sends_all_keys_and_persists_the_acknowledged_baseline(libs):
    created = MeasurementSetRepository.create(_set(ENHANCED))
    client = RecordingReferenceClient()

    result = sync_reference_library(client)

    assert result == ReferenceSyncResult(pushed=3)
    sent = next(payload for kind, payload, _ in client.calls if kind == "measurement_set")
    assert sent["measurement_details_json"] == json.loads(ENHANCED["measurement_details_json"])
    assert (sent["q_core_min"], sent["q_core_max"]) == (1.36, 2.19)
    state = _state(created.id)
    assert state.sync_status == "clean"
    assert state.accepted_payload == load_library_payload("measurement_set", created.id)

    # Unchanged record: no remote call, state stays clean (no-op churn check).
    client.calls.clear()
    assert sync_reference_library(client) == ReferenceSyncResult()
    assert client.calls == []


def test_legacy_row_with_historical_baseline_is_not_pushed_again(libs):
    """A baseline persisted before Stage 3C omits the extension keys; it is
    recognized as NULL, so an unchanged legacy row stays a no-op."""
    created = MeasurementSetRepository.create(_set(LEGACY))
    client = RecordingReferenceClient()
    sync_reference_library(client)
    state = _state(created.id)
    historical = {k: v for k, v in state.accepted_payload.items() if k not in EXTENSION_FIELDS}
    ReferenceCloudSyncStateRepository.save_library(replace(state, accepted_payload=historical))
    # Mark the row dirty without changing content, as a same-value edit does.
    MeasurementSetRepository.update(created.id, {"notes": None}, bump_revision=False)
    client.calls.clear()

    assert _state(created.id).accepted_payload == {**historical, **{k: None for k in EXTENSION_FIELDS}}
    assert sync_reference_library(client) == ReferenceSyncResult()
    assert client.calls == []
    assert _state(created.id).sync_status == "clean"


def test_edit_after_upgrade_pushes_with_all_keys_against_historical_baseline(libs):
    """Upgrade/retry transition: the next content push after the upgrade
    carries the extension keys and replaces the historical baseline."""
    created = MeasurementSetRepository.create(_set(LEGACY))
    client = RecordingReferenceClient()
    sync_reference_library(client)
    state = _state(created.id)
    ReferenceCloudSyncStateRepository.save_library(
        replace(
            state,
            accepted_payload={
                k: v for k, v in state.accepted_payload.items() if k not in EXTENSION_FIELDS
            },
        )
    )
    MeasurementSetRepository.update(created.id, {"notes": "after upgrade"})
    client.calls.clear()

    result = sync_reference_library(client)

    assert result == ReferenceSyncResult(pushed=1)
    kind, sent, expected = client.calls[0]
    assert (kind, expected) == ("measurement_set", 1)
    assert all(name in sent and sent[name] is None for name in EXTENSION_FIELDS)
    state = _state(created.id)
    assert state.cloud_row_version == 2 and state.sync_status == "clean"
    assert all(name in state.accepted_payload for name in EXTENSION_FIELDS)


def test_remote_tombstone_over_historical_baseline_applies_without_conflict(libs):
    created = MeasurementSetRepository.create(_set(LEGACY))
    client = RecordingReferenceClient()
    sync_reference_library(client)
    state = _state(created.id)
    ReferenceCloudSyncStateRepository.save_library(
        replace(
            state,
            accepted_payload={
                k: v for k, v in state.accepted_payload.items() if k not in EXTENSION_FIELDS
            },
        )
    )
    remote_set = {
        **_remote_rows_of(client, "measurement_set")[0],
        "row_version": 2,
        "deleted_at": "2026-09-13T01:00:00Z",
        "updated_at": "2026-09-13T01:00:00Z",
    }
    puller = PullClient(
        works=_remote_rows_of(client, "work"),
        treatments=_remote_rows_of(client, "treatment"),
        sets=[remote_set],
    )

    result = pull_reference_library(puller)

    assert result.conflicts == () and result.errors == ()
    assert _raw_row(libs[1], created.id) is None


def test_unknown_create_recovery_matches_remote_row_carrying_the_extension(libs):
    created = MeasurementSetRepository.create(_set(ENHANCED))
    first = RecordingReferenceClient()
    first.failures[("measurement_set", created.id)] = CloudTemporarilyUnavailableError("lost")
    sync_reference_library(first)
    assert _state(created.id).remote_identity_state == "create_outcome_unknown"
    sent = next(payload for kind, payload, _ in first.calls if kind == "measurement_set")

    second = RecordingReferenceClient()
    second.rows["work"] = _remote_rows_of(first, "work")
    second.rows["treatment"] = _remote_rows_of(first, "treatment")
    # The create request omitted the absent point series (adapter workaround);
    # the authoritative row returns it as NULL alongside the extension keys.
    second.rows["measurement_set"] = [
        {**sent, "raw_points_json": None, "user_id": "user-1", "row_version": 1,
         "deleted_at": None, **_TIMESTAMPS}
    ]
    result = sync_reference_library(second)

    assert second.calls == []
    assert result.errors == () and result.conflicts == ()
    state = _state(created.id)
    assert state.remote_identity_state == "acknowledged"
    assert state.cloud_row_version == 1 and state.sync_status == "clean"
    assert state.accepted_payload["measurement_details_json"] == json.loads(
        ENHANCED["measurement_details_json"]
    )


def test_recognize_library_baseline_only_completes_full_omissions():
    assert recognize_library_baseline("measurement_set", None) is None
    assert recognize_library_baseline("work", {"id": "w"}) == {"id": "w"}
    assert recognize_library_baseline("measurement_set", {"id": "s"}) == {
        "id": "s", "measurement_details_json": None, "q_core_min": None, "q_core_max": None
    }
    partial = {"id": "s", "q_core_min": 1.0}
    assert recognize_library_baseline("measurement_set", partial) == partial


# --- Round trip: push from one library, pull into another ---------------------


@pytest.mark.parametrize(
    "row",
    [
        pytest.param(ENHANCED, id="enhanced"),
        pytest.param(LEGACY, id="legacy"),
        pytest.param(
            {**ENHANCED, "measurement_details_json": json.dumps(FUTURE, indent=2)},
            id="future-version",
        ),
    ],
)
def test_round_trip_through_cloud_transport_is_canonical_and_lossless(tmp_path, monkeypatch, row):
    _, ref_a = _point_at(monkeypatch, tmp_path / "a")
    _seed_fixture_parents()
    created = MeasurementSetRepository.create(_set(ENHANCED if row is not LEGACY else LEGACY))
    if row["measurement_details_json"] not in (None, ENHANCED["measurement_details_json"]):
        # The repository refuses to author an unsupported details version
        # (Stage 3B, inspect-only); such a row reaches a library only through
        # an authoritative path, modelled here by an aware raw write.
        from database import reference_library_schema as lib_schema

        conn = sqlite3.connect(ref_a)
        lib_schema.register_measurement_contract(conn)
        conn.execute(
            "UPDATE reference_measurement_sets SET measurement_details_json=? WHERE id=?",
            (row["measurement_details_json"], created.id),
        )
        conn.commit()
        conn.close()
    pushed_payload = load_library_payload("measurement_set", created.id)
    assert pushed_payload["measurement_details_json"] == (
        None if row["measurement_details_json"] is None else json.loads(row["measurement_details_json"])
    )
    client = RecordingReferenceClient()
    assert sync_reference_library(client) == ReferenceSyncResult(pushed=3)

    _, ref_b = _point_at(monkeypatch, tmp_path / "b")
    puller = PullClient(
        works=_remote_rows_of(client, "work"),
        treatments=_remote_rows_of(client, "treatment"),
        sets=_remote_rows_of(client, "measurement_set"),
    )
    result = pull_reference_library(puller)

    assert result.errors == () and result.conflicts == ()
    pulled_payload = load_library_payload("measurement_set", created.id)
    assert pulled_payload == pushed_payload
    stored = _raw_row(ref_b, created.id)
    if row["measurement_details_json"] is None:
        assert all(stored[name] is None for name in EXTENSION_FIELDS)
        assert not MeasurementSetRepository.get(created.id).is_enhanced
    else:
        # Stored text is the codec's canonical form and decodes to the same object.
        assert stored["measurement_details_json"] == json.dumps(
            json.loads(row["measurement_details_json"]),
            ensure_ascii=True, sort_keys=True, separators=(",", ":"),
        )
        assert json.loads(stored["measurement_details_json"]) == json.loads(
            row["measurement_details_json"]
        )
        assert MeasurementSetRepository.get(created.id).is_enhanced
    # Pulling the same feed again writes nothing.
    assert pull_reference_library(puller) == ReferenceSyncResult()


def test_pull_only_mode_with_enhanced_rows_issues_no_writes(libs):
    MeasurementSetRepository.create(_set(ENHANCED))
    client = RecordingReferenceClient()
    sync_reference_library(client)
    puller = PullClient(
        works=_remote_rows_of(client, "work"),
        treatments=_remote_rows_of(client, "treatment"),
        sets=_remote_rows_of(client, "measurement_set"),
    )
    MeasurementSetRepository.update(ENHANCED["id"], {"notes": "local only"})

    # PullClient raises on any writer call; pull-only must never reach one.
    result = sync_reference_library(puller, pull_only=True)

    assert result.errors == () and result.pushed == 0
    assert _state(ENHANCED["id"]).sync_status == "dirty"


# --- Pull reconciliation: the section 5 group rule ----------------------------


def test_local_bound_edit_versus_remote_tag_edit_conflicts_as_one_group(libs):
    _seed_acknowledged_graph()
    MeasurementSetRepository.update("set-1", {"length_max": 9.5})
    before = _raw_row(libs[1], "set-1")
    puller = PullClient(
        works=[_work_row()], treatments=[_treatment_row()],
        sets=[_set_row(measurement_details_json=LENGTH_TAG, revision=2, row_version=2)],
    )

    result = pull_reference_library(puller)

    state = _state("set-1")
    assert result.conflicts == ("measurement_set:set-1",)
    assert state.conflict["reason"] == "overlapping_remote_change"
    assert set(state.conflict["overlapping_fields"]) == {"length_max", "measurement_details_json"}
    assert _raw_row(libs[1], "set-1") == before


def test_local_notes_edit_versus_remote_tag_edit_merges(libs):
    _seed_acknowledged_graph()
    MeasurementSetRepository.update("set-1", {"notes": "local note"})
    puller = PullClient(
        works=[_work_row()], treatments=[_treatment_row()],
        sets=[_set_row(measurement_details_json=LENGTH_TAG, revision=2, row_version=2)],
    )

    result = pull_reference_library(puller)

    assert result.conflicts == () and result.pulled == 1
    row = _raw_row(libs[1], "set-1")
    assert row["notes"] == "local note"
    assert json.loads(row["measurement_details_json"]) == LENGTH_TAG
    assert row["revision"] == 3
    assert _state("set-1").sync_status == "dirty"


def test_identical_group_content_on_both_sides_is_not_a_conflict(libs):
    _seed_acknowledged_graph()
    MeasurementSetRepository.update("set-1", {"q_core_min": 1.4, "q_core_max": 1.8})
    puller = PullClient(
        works=[_work_row()], treatments=[_treatment_row()],
        sets=[_set_row(q_core_min=1.4, q_core_max=1.8, revision=2, row_version=2)],
    )

    result = pull_reference_library(puller)

    assert result.conflicts == ()
    row = _raw_row(libs[1], "set-1")
    assert (row["q_core_min"], row["q_core_max"]) == (1.4, 1.8)
    assert _state("set-1").cloud_row_version == 2


def test_unrelated_numeric_edits_inside_the_group_now_conflict(libs):
    """Behaviour change to flag: before Stage 3C a local length edit and a
    remote width edit merged field by field; the group moves as one unit."""
    _seed_acknowledged_graph()
    MeasurementSetRepository.update("set-1", {"length_max": 9.5})
    puller = PullClient(
        works=[_work_row()], treatments=[_treatment_row()],
        sets=[_set_row(width_max=6.5, revision=2, row_version=2)],
    )

    result = pull_reference_library(puller)

    state = _state("set-1")
    assert result.conflicts == ("measurement_set:set-1",)
    assert set(state.conflict["overlapping_fields"]) == {"length_max", "width_max"}
    assert set(state.conflict["overlapping_fields"]) <= SCIENTIFIC_CONTENT_FIELDS


def test_remote_explicit_null_clears_a_locally_enhanced_acknowledged_row(libs):
    _seed_acknowledged_graph()
    # Row enhanced and acknowledged as such (as after a successful push).
    MeasurementSetRepository.update(
        "set-1", {"measurement_details_json": json.dumps(LENGTH_TAG)}, bump_revision=False
    )
    state = _state("set-1")
    ReferenceCloudSyncStateRepository.save_library(
        replace(
            state,
            accepted_payload=load_library_payload("measurement_set", "set-1"),
            sync_status="clean",
        )
    )
    puller = PullClient(
        works=[_work_row()], treatments=[_treatment_row()],
        sets=[_set_row(revision=2, row_version=2)],  # extension keys present, NULL
    )

    result = pull_reference_library(puller)

    assert result.conflicts == () and result.pulled == 1
    row = _raw_row(libs[1], "set-1")
    assert all(row[name] is None for name in EXTENSION_FIELDS)
    assert _state("set-1").sync_status == "clean"


def test_write_domain_stores_details_in_canonical_codec_form(libs):
    _seed_acknowledged_graph()
    unsorted = {"metrics": {"length": {"outer_range": {"kind": "reported_extremes"}}}, "schema_version": 1}
    puller = PullClient(
        works=[_work_row()], treatments=[_treatment_row()],
        sets=[_set_row(measurement_details_json=unsorted, revision=2, row_version=2)],
    )

    assert pull_reference_library(puller).conflicts == ()

    stored = _raw_row(libs[1], "set-1")["measurement_details_json"]
    assert stored == json.dumps(LENGTH_TAG, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def test_invalid_enhanced_remote_row_rejects_the_feed_without_writes(libs):
    _seed_acknowledged_graph()
    before = _raw_row(libs[1], "set-1")
    invalid = _details({"length": {"mean_interval": {"lower": 7.5, "upper": 8.5, "kind": "reported_range"}}})
    puller = PullClient(
        works=[_work_row(title="Changed", revision=2, row_version=2)],
        treatments=[_treatment_row()],
        sets=[_set_row(measurement_details_json=invalid, length_mean=8.0, revision=2, row_version=2)],
    )

    result = pull_reference_library(puller)

    assert result.errors and "invalid measurement content" in result.errors[0]
    assert result.pulled == 0
    assert _raw_row(libs[1], "set-1") == before
    assert ReferenceWorkRepository.get("work-1").title == "Remote work"


def test_legacy_remote_row_acquires_no_new_rule(libs):
    """A legacy row with historically odd numbers still pulls as today."""
    _seed_acknowledged_graph()
    puller = PullClient(
        works=[_work_row()], treatments=[_treatment_row()],
        sets=[_set_row(length_min=9.5, length_max=7.0, revision=2, row_version=2)],
    )

    result = pull_reference_library(puller)

    assert result.errors == () and result.conflicts == ()
    row = _raw_row(libs[1], "set-1")
    assert (row["length_min"], row["length_max"]) == (9.5, 7.0)
    assert all(row[name] is None for name in EXTENSION_FIELDS)


def test_feed_from_a_server_without_the_extension_is_rejected_whole(libs):
    """Contract section 11: pulling from a pre-Stage-3C server rejects the
    feed at staging (missing canonical fields) and writes nothing."""
    _seed_acknowledged_graph()
    before = _raw_row(libs[1], "set-1")
    old_server_row = {k: v for k, v in _set_row(length_max=9.5, revision=2, row_version=2).items()
                      if k not in EXTENSION_FIELDS}
    puller = PullClient(works=[_work_row()], treatments=[_treatment_row()], sets=[old_server_row])

    result = pull_reference_library(puller)

    assert result.errors == ("reference pull: remote measurement_set is missing canonical fields",)
    assert _raw_row(libs[1], "set-1") == before
    assert ReferenceCloudSyncStateRepository.get_library_pull_cursor("user-1", "measurement_set") is None


def test_fail_closed_guard_still_applies_to_non_acknowledging_remotes(libs):
    _seed_acknowledged_graph()
    MeasurementSetRepository.update("set-1", {"q_core_min": 1.4, "q_core_max": 1.8}, bump_revision=False)
    connection = sqlite3.connect(libs[1])
    connection.row_factory = sqlite3.Row
    try:
        unaware = {k: v for k, v in _set_row().items() if k not in EXTENSION_FIELDS}
        assert _extension_write_blocked(connection, "measurement_set", "set-1", unaware) is True
        assert _extension_write_blocked(connection, "measurement_set", "set-1", _set_row()) is False
        assert _extension_write_blocked(connection, "work", "work-1", {"id": "work-1"}) is False
    finally:
        connection.close()


def test_measurement_content_error_validates_enhanced_rows_only():
    payload = canonical_library_payload(
        "measurement_set",
        {**ENHANCED, "measurement_details_json": json.loads(ENHANCED["measurement_details_json"])},
    )
    assert _measurement_content_error(payload) is None
    assert _measurement_content_error({**payload, "length_mean": 11.0}) is not None
    assert _measurement_content_error({**payload, "measurement_details_json": FUTURE}) is None
    assert _measurement_content_error({**payload, "measurement_details_json": {"schema_version": 1}}) is not None
    legacy = canonical_library_payload("measurement_set", {**LEGACY, "length_min": 9.0, "length_max": 7.0})
    assert _measurement_content_error(legacy) is None
