"""Stage M desktop: reference client capability (sporely-web 20261002120000).

Covers the declaration on writes, the device id, the capability-aware owner
feed, the requires_newer_client / older_client_active hold policy and its
notice, the device report, and the public-read marker tolerance.
"""
from __future__ import annotations

import copy
import uuid
from datetime import datetime, timedelta, timezone

import pytest

import utils.cloud_sync as cloud_sync
import utils.reference_client_capabilities as capabilities_module
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
from references import measurement_content_gates
from tests.conftest import TEST_REFERENCE_DEVICE_ID
from utils.cloud_sync import (
    CloudSyncError,
    CloudTemporarilyUnavailableError,
    PullOnlyCloudClient,
    PullOnlyModeError,
    SporelyCloudClient,
)
from utils.reference_client_capabilities import (
    NIL_UUID,
    capability_fingerprint,
    get_reference_device_id as real_get_reference_device_id,
    reference_client_capabilities,
    summarize_capability_holds,
)
from utils.reference_cloud_sync import (
    ReferenceSyncResult,
    merge_reference_sync_result,
    sync_reference_library,
)


@pytest.fixture(autouse=True)
def _app_version_and_report_state():
    previous = cloud_sync._current_source_app_version()
    cloud_sync.set_cloud_sync_source_app_version("0.9.25")
    capabilities_module._reset_reported_for_tests()
    yield
    cloud_sync.set_cloud_sync_source_app_version(previous)
    capabilities_module._reset_reported_for_tests()


def test_gates_stay_closed():
    assert measurement_content_gates.MINIMUM_SUPPORTED_READER_VERSION_GATE_OPEN is False
    assert measurement_content_gates.MINIMUM_SUPPORTED_DESKTOP_VERSION_GATE_OPEN is False


# Declaration ------------------------------------------------------------------------

def test_capability_payload_shape():
    assert reference_client_capabilities() == {
        "reference_snapshot_versions": [1, 2],
        "device_id": TEST_REFERENCE_DEVICE_ID,
        "client": "desktop",
        "app_version": "0.9.25",
    }


def test_version_two_is_declared_only_when_every_v2_reader_ships(monkeypatch):
    import database.reference_citation as citation

    assert capabilities_module.declared_snapshot_versions() == (1, 2)
    monkeypatch.setattr(citation, "SUPPORTED_SNAPSHOT_VERSIONS", frozenset({1}))
    assert capabilities_module.declared_snapshot_versions() == (1,)


@pytest.mark.parametrize(
    ("method", "args", "rpc"),
    [
        ("sync_reference_measurement_set", ({"id": "s"}, 3), "sync_reference_measurement_set"),
        ("sync_observation_reference_use", ({"id": "u"}, 0, "historical_import"),
         "sync_observation_reference_use"),
    ],
)
def test_both_reference_writes_declare_capabilities(monkeypatch, method, args, rpc):
    client = SporelyCloudClient.__new__(SporelyCloudClient)
    calls = []
    monkeypatch.setattr(client, "_rpc", lambda name, body: calls.append((name, body)) or {})
    getattr(client, method)(*args)
    assert calls[0][0] == rpc
    assert calls[0][1]["p_client_capabilities"] == reference_client_capabilities()
    assert list(calls[0][1])[-1] == "p_client_capabilities"


def test_unrelated_reference_writes_do_not_declare(monkeypatch):
    client = SporelyCloudClient.__new__(SporelyCloudClient)
    calls = []
    monkeypatch.setattr(client, "_rpc", lambda name, body: calls.append((name, body)) or {})
    client.sync_reference_work({"id": "w"}, 0)
    client.sync_reference_curated_fork({"id": "f"}, 0)
    assert all("p_client_capabilities" not in body for _name, body in calls)


# Device id ---------------------------------------------------------------------------

@pytest.fixture()
def settings(monkeypatch):
    store: dict = {}
    monkeypatch.setattr(schema, "get_app_settings", lambda: dict(store))
    monkeypatch.setattr(schema, "update_app_settings", lambda updates: store.update(updates) or dict(store))
    return store


def test_device_id_is_generated_once_and_persisted(settings):
    first = real_get_reference_device_id()
    assert uuid.UUID(first).version == 4
    assert first != NIL_UUID
    assert settings == {"reference_client_device_id": first}
    assert real_get_reference_device_id() == first


@pytest.mark.parametrize("stored", ["", "not-a-uuid", NIL_UUID, None, 7])
def test_invalid_or_nil_stored_device_id_is_replaced(settings, stored):
    settings["reference_client_device_id"] = stored
    device_id = real_get_reference_device_id()
    assert device_id not in {NIL_UUID, stored}
    assert uuid.UUID(device_id)
    assert settings["reference_client_device_id"] == device_id


def test_valid_stored_device_id_is_kept(settings):
    stored = str(uuid.uuid4())
    settings["reference_client_device_id"] = stored
    assert real_get_reference_device_id() == stored


# Feed ------------------------------------------------------------------------------

SET_FIELDS = (
    "user_id,id,taxon_treatment_id,character,raw_text,data_kind,length_min,"
    "length_core_min,length_core_max,length_max,width_min,width_core_min,"
    "width_core_max,width_max,q_min,q_max,q_mean,length_mean,width_mean,"
    "sample_size,specimen_count,mount_medium,stain,preparation,"
    "measurement_method,notes,raw_points_json,supersedes_id,revision,"
    "measurement_details_json,q_core_min,q_core_max,"
    "row_version,created_at,updated_at,deleted_at"
).split(",")


def _feed_row(index: int, *, deleted: bool = False) -> dict:
    row = {field: None for field in SET_FIELDS}
    row.update(
        user_id="user-1",
        id=f"00000000-0000-4000-8000-{index:012d}",
        updated_at=f"2026-10-02T00:00:{index:02d}+00:00",
        row_version=1,
        deleted_at="2026-10-02T01:00:00+00:00" if deleted else None,
        # Full table row: columns the select never asked for are dropped.
        owner_only_extra="x",
    )
    return row


def _paged_client(monkeypatch, pages: list[dict]):
    client = SporelyCloudClient.__new__(SporelyCloudClient)
    client.user_id = "user-1"
    calls: list[tuple[str, dict]] = []
    responses = iter(pages)
    monkeypatch.setattr(
        client, "_rpc", lambda name, body: calls.append((name, copy.deepcopy(body))) or next(responses)
    )
    monkeypatch.setattr(
        client, "_get_paginated",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("table GET used")),
    )
    return client, calls


def _page(rows, *, next_cursor=None, entity="measurement_set", withheld=0):
    return {
        "status": "ok", "entity": entity, "rows": rows,
        "withheld_count": withheld, "next_cursor": next_cursor,
    }


def test_feed_full_pull_pages_with_cursor_and_keeps_deleted_rows(monkeypatch):
    rows = [_feed_row(1), _feed_row(2, deleted=True), _feed_row(3)]
    pages = [
        _page(rows[:2], next_cursor={"updated_at": rows[1]["updated_at"], "id": rows[1]["id"]}),
        _page(rows[2:], next_cursor={"updated_at": rows[2]["updated_at"], "id": rows[2]["id"]}),
        _page([]),
    ]
    client, calls = _paged_client(monkeypatch, pages)

    result = client.list_reference_measurement_sets()

    assert [row["id"] for row in result] == [row["id"] for row in rows]
    assert result[1]["deleted_at"] == "2026-10-02T01:00:00+00:00"
    assert all(list(row) == SET_FIELDS for row in result)
    assert [name for name, _body in calls] == ["list_reference_library_feed"] * 3
    first, second, third = (body for _name, body in calls)
    assert first["p_entity"] == "measurement_set"
    assert first["p_after_updated_at"] is None and first["p_after_id"] is None
    assert first["p_limit"] == 500
    assert first["p_client_capabilities"] == reference_client_capabilities()
    assert (second["p_after_updated_at"], second["p_after_id"]) == (rows[1]["updated_at"], rows[1]["id"])
    assert (third["p_after_updated_at"], third["p_after_id"]) == (rows[2]["updated_at"], rows[2]["id"])


def test_every_feed_pull_starts_without_a_cursor(monkeypatch):
    pages = [_page([_feed_row(1)]), _page([_feed_row(1)])]
    client, calls = _paged_client(monkeypatch, pages)
    client.list_reference_measurement_sets()
    client.list_reference_measurement_sets()
    assert all(body["p_after_id"] is None for _name, body in calls)


@pytest.mark.parametrize(
    ("method", "entity"),
    [
        ("list_observation_reference_uses", "observation_use"),
        ("list_reference_curated_forks", "curated_fork"),
    ],
)
def test_use_and_fork_readers_use_the_feed(monkeypatch, method, entity):
    client = SporelyCloudClient.__new__(SporelyCloudClient)
    client.user_id = "user-1"
    calls = []
    monkeypatch.setattr(
        client, "_rpc",
        lambda name, body: calls.append((name, body)) or _page([], entity=entity),
    )
    assert getattr(client, method)() == []
    assert calls[0][0] == "list_reference_library_feed"
    assert calls[0][1]["p_entity"] == entity


@pytest.mark.parametrize(
    ("response", "error"),
    [
        ({"status": "rate_limited", "retry_after_seconds": 3}, CloudTemporarilyUnavailableError),
        ({"status": "ok", "entity": "observation_use", "rows": [], "next_cursor": None}, CloudSyncError),
        ({"status": "ok", "entity": "measurement_set", "rows": None, "next_cursor": None}, CloudSyncError),
        ([], CloudSyncError),
    ],
)
def test_feed_error_statuses_fail_the_whole_pull(monkeypatch, response, error):
    client, _calls = _paged_client(monkeypatch, [response])
    with pytest.raises(error):
        client.list_reference_measurement_sets()


def test_feed_never_returns_a_partial_pull(monkeypatch):
    row = _feed_row(1)
    cursor = {"updated_at": row["updated_at"], "id": row["id"]}
    client, _calls = _paged_client(
        monkeypatch, [_page([row], next_cursor=cursor), _page([_feed_row(2)], next_cursor=cursor)]
    )
    with pytest.raises(CloudSyncError, match="cursor did not advance"):
        client.list_reference_measurement_sets()
    incomplete = _feed_row(3)
    del incomplete["measurement_details_json"]
    client, _calls = _paged_client(monkeypatch, [_page([incomplete])])
    with pytest.raises(CloudSyncError, match="lacks"):
        client.list_reference_measurement_sets()


def test_pull_only_allows_the_feed_and_blocks_the_device_report():
    assert "list_reference_library_feed" in cloud_sync._PULL_ONLY_ALLOWED_RPC_NAMES
    assert "record_reference_client_capabilities" in cloud_sync._PULL_ONLY_BLOCKED_CLIENT_METHODS
    assert "record_reference_client_capabilities" not in cloud_sync._PULL_ONLY_ALLOWED_RPC_NAMES

    class Wrapped:
        user_id = "user-1"

        def record_reference_client_capabilities(self, capabilities):
            raise AssertionError("reached the network")

    proxy = PullOnlyCloudClient(Wrapped())
    with pytest.raises(PullOnlyModeError):
        proxy.record_reference_client_capabilities({})


# Hold policy -------------------------------------------------------------------------

@pytest.fixture()
def databases(tmp_path, monkeypatch):
    database_path = tmp_path / "mushrooms.db"
    reference_path = tmp_path / "reference_values.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: database_path)
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: reference_path)
    monkeypatch.setattr(
        schema, "get_bundled_reference_database_path", lambda: tmp_path / "missing-reference.db",
    )
    schema.init_database()
    return database_path, reference_path


def _recent(days: int = 1) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


class HoldClient:
    """Owner client whose set writes are refused with one capability status."""

    def __init__(self, status: str | None = "older_client_active"):
        self.user_id = "user-1"
        self.status = status
        self.calls: list[tuple[str, str]] = []
        self.reports: list[dict] = []
        self.report_response: object = {"status": "recorded"}
        self.devices: list[dict] | Exception = [
            {"device_id": str(uuid.uuid4()), "reference_snapshot_versions": [1],
             "last_seen_at": _recent()},
        ]
        self.remote_rows: dict[tuple[str, str], dict] = {}

    def _ack(self, kind, payload, expected):
        row = {
            **payload, "user_id": self.user_id,
            "row_version": expected + 1 if expected else 1,
            "created_at": "2026-10-02T00:00:00Z", "updated_at": "2026-10-02T00:00:01Z",
            "deleted_at": None,
        }
        if kind == "measurement_set":
            row.setdefault("raw_points_json", None)
        self.remote_rows[(kind, payload["id"])] = row
        return {"status": "updated" if expected else "created", "row": row}

    def sync_reference_work(self, payload, expected):
        self.calls.append(("work", payload["id"]))
        return self._ack("work", payload, expected)

    def sync_reference_taxon_treatment(self, payload, expected):
        self.calls.append(("treatment", payload["id"]))
        return self._ack("treatment", payload, expected)

    def sync_reference_measurement_set(self, payload, expected):
        self.calls.append(("measurement_set", payload["id"]))
        if self.status is not None:
            return {"status": self.status, "row": None}
        return self._ack("measurement_set", payload, expected)

    def sync_observation_reference_use(self, payload, expected, snapshot_mode):
        self.calls.append(("observation_use", payload["id"]))
        return self._ack("observation_use", payload, expected)

    def _list(self, kind):
        return [copy.deepcopy(row) for (k, _id), row in self.remote_rows.items() if k == kind]

    def list_reference_works(self):
        return self._list("work")

    def list_reference_taxon_treatments(self):
        return self._list("treatment")

    def list_reference_measurement_sets(self):
        return self._list("measurement_set")

    def list_observation_reference_uses(self):
        return self._list("observation_use")

    def list_reference_curated_forks(self):
        return []

    def list_reference_client_devices(self):
        if isinstance(self.devices, Exception):
            raise self.devices
        return copy.deepcopy(self.devices)

    def record_reference_client_capabilities(self, capabilities):
        self.reports.append(capabilities)
        if isinstance(self.report_response, Exception):
            raise self.report_response
        return self.report_response


def _graph(with_use: bool = False):
    work = ReferenceWorkRepository.create(ReferenceWork("work-a", "book", "Work", "Work"))
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment("treatment-a", work.id, "Russula paludosa")
    )
    MeasurementSetRepository.create(
        MeasurementSet("set-a", treatment.id, "spore_size", "range", length_min=7.0, length_max=9.0)
    )
    if with_use:
        connection = schema.get_connection()
        try:
            connection.execute(
                "INSERT INTO observations (id, date, cloud_id) VALUES (1, '2026-10-02', '101')"
            )
            connection.commit()
        finally:
            connection.close()
        ObservationReferenceUseRepository.attach(1, "set-a", role="supports_identification")


def _set_calls(client):
    return [call for call in client.calls if call[0] == "measurement_set"]


@pytest.mark.parametrize("status", ["older_client_active", "requires_newer_client"])
def test_capability_refusal_keeps_change_pending_without_error(databases, status):
    _graph()
    client = HoldClient(status)

    result = sync_reference_library(client)

    assert result.capability_holds == (f"measurement_set:set-a:{status}",)
    assert result.errors == () and result.blocked == () and result.terminal_errors == ()
    state = ReferenceCloudSyncStateRepository.get_library("measurement_set", "set-a")
    assert state.sync_status == "retry"
    assert state.remote_identity_state != "acknowledged"
    assert state.accepted_payload is None
    assert state.last_error.startswith(f"capability_hold:{status}:")
    # Never downgraded: the local row keeps its content.
    assert MeasurementSetRepository.get("set-a").length_max == 9.0


def test_held_change_is_not_retried_every_sync(databases):
    _graph()
    client = HoldClient()
    sync_reference_library(client)
    sync_reference_library(client)
    third = sync_reference_library(client)

    assert len(_set_calls(client)) == 1
    assert third.capability_holds == ("measurement_set:set-a:older_client_active",)
    assert third.errors == ()


def test_hold_is_retried_after_the_older_device_upgrades(databases):
    _graph()
    client = HoldClient()
    sync_reference_library(client)
    client.devices[0]["reference_snapshot_versions"] = [1, 2]
    client.status = None

    result = sync_reference_library(client)

    assert len(_set_calls(client)) == 2
    assert result.capability_holds == ()
    state = ReferenceCloudSyncStateRepository.get_library("measurement_set", "set-a")
    assert state.sync_status == "clean"


def test_hold_is_retried_after_an_app_upgrade(databases):
    _graph()
    client = HoldClient()
    sync_reference_library(client)
    cloud_sync.set_cloud_sync_source_app_version("0.9.26")
    sync_reference_library(client)
    assert len(_set_calls(client)) == 2


def test_hold_is_retried_after_a_local_edit(databases):
    _graph()
    client = HoldClient()
    sync_reference_library(client)
    MeasurementSetRepository.update("set-a", {"length_max": 10.0})
    sync_reference_library(client)
    assert len(_set_calls(client)) == 2


def test_device_read_failure_keeps_the_hold(databases):
    _graph()
    client = HoldClient()
    sync_reference_library(client)
    client.devices = CloudSyncError("devices unavailable")
    sync_reference_library(client)
    sync_reference_library(client)
    # One retry when the fingerprint first becomes "unknown", then held again.
    assert len(_set_calls(client)) == 2


def test_use_waiting_on_a_held_set_is_part_of_the_hold(databases):
    _graph(with_use=True)
    client = HoldClient()

    result = sync_reference_library(client)

    assert not [call for call in client.calls if call[0] == "observation_use"]
    assert result.blocked == ()
    assert result.errors == ()
    assert "measurement_set:set-a:older_client_active" in result.capability_holds
    assert any(
        item.startswith("observation_use:") and item.endswith(":older_client_active")
        for item in result.capability_holds
    )


def test_fingerprint_tracks_only_blocking_other_devices():
    own = reference_client_capabilities()
    other = str(uuid.uuid4())
    base = capability_fingerprint(own, [])
    assert capability_fingerprint(own, [
        {"device_id": other, "reference_snapshot_versions": [1, 2], "last_seen_at": _recent()},
        {"device_id": own["device_id"], "reference_snapshot_versions": [1], "last_seen_at": _recent()},
        {"device_id": str(uuid.uuid4()), "reference_snapshot_versions": [1], "last_seen_at": _recent(31)},
    ]) == base
    blocking = capability_fingerprint(own, [
        {"device_id": other, "reference_snapshot_versions": [1], "last_seen_at": _recent()},
    ])
    assert blocking != base
    assert capability_fingerprint(own, None) not in {base, blocking}


# Notice and result -------------------------------------------------------------------

def test_holds_are_a_notice_not_sync_errors():
    merged = merge_reference_sync_result(
        {"errors": []},
        ReferenceSyncResult(capability_holds=(
            "measurement_set:a:older_client_active",
            "observation_use:b:older_client_active",
            "measurement_set:c:requires_newer_client",
        )),
    )
    assert merged["errors"] == []
    assert summarize_capability_holds(merged) == {
        "older_client_active": 2, "requires_newer_client": 1,
    }
    assert not cloud_sync.sync_result_requires_observation_refresh(
        {**merged, "sync_summary": {}, "pushed": 0, "pulled": 0}
    )


def test_notice_text_names_both_situations():
    from ui.observations_tab import ObservationsTab

    class Host:
        def tr(self, text):
            return text

    result = {"reference_sync": {"capability_holds": [
        "measurement_set:a:older_client_active", "measurement_set:b:requires_newer_client",
    ]}}
    text = ObservationsTab._reference_capability_hold_notice(Host(), result)
    assert "1 reference change(s) are waiting" in text
    assert "older Sporely version" in text
    assert "need a newer Sporely version" in text
    assert ObservationsTab._reference_capability_hold_notice(Host(), {}) == ""


# Device report -----------------------------------------------------------------------

def test_device_report_once_per_session_before_pushes(databases):
    _graph()
    client = HoldClient(status=None)
    sync_reference_library(client)
    sync_reference_library(client)
    assert client.reports == [reference_client_capabilities()]


@pytest.mark.parametrize(
    "response", [CloudSyncError("offline"), {"status": "rate_limited", "retry_after_seconds": 5}],
)
def test_device_report_failure_is_non_fatal_and_retried_next_sync(databases, response):
    _graph()
    client = HoldClient(status=None)
    client.report_response = response
    result = sync_reference_library(client)
    assert result.errors == ()
    assert result.pushed == 3
    sync_reference_library(client)
    assert len(client.reports) == 2


def test_pull_only_sync_never_reports(databases):
    client = HoldClient(status=None)
    sync_reference_library(client, pull_only=True)
    assert client.reports == []


# Public reads ------------------------------------------------------------------------

from database.curated_reference_forks import (  # noqa: E402
    CuratedReferenceError,
    copy_curated_bundle_to_personal_library,
    normalize_curated_bundle,
    search_shared_reference_contributions,
)
from tests.test_curated_reference_forks import bundle_row, shared_row  # noqa: E402

TAXON = 2_100_000_081


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("search_public_reference_contributions_v2", (TAXON, 25, None, None)),
        ("get_public_reference_contribution_v2", ("68000000-0000-4000-8000-000000006701", None)),
    ],
)
def test_public_reads_opt_in_to_snapshot_version_two(monkeypatch, method, args):
    client = SporelyCloudClient.__new__(SporelyCloudClient)
    calls = []
    monkeypatch.setattr(client, "_rpc", lambda name, body: calls.append((name, body)) or [])
    getattr(client, method)(*args)
    assert calls[0][0] == method
    assert calls[0][1]["p_accept_snapshot_versions"] == [1, 2]


@pytest.mark.parametrize("row_factory", [shared_row, bundle_row])
def test_marked_envelope_is_tolerated_and_flagged(row_factory):
    plain = normalize_curated_bundle(row_factory(), expected_taxon_id=TAXON)
    marked = normalize_curated_bundle(
        {**row_factory(), "measurement_details_omitted": True}, expected_taxon_id=TAXON,
    )
    assert plain.measurement_details_omitted is False
    assert marked.measurement_details_omitted is True
    assert marked.snapshot == plain.snapshot
    assert marked.source_envelope["measurement_details_omitted"] is True
    with pytest.raises(CuratedReferenceError, match="marker"):
        normalize_curated_bundle(
            {**row_factory(), "measurement_details_omitted": False}, expected_taxon_id=TAXON,
        )


def test_marked_bundle_is_never_copied(databases):
    marked = normalize_curated_bundle(
        {**shared_row(), "measurement_details_omitted": True}, expected_taxon_id=TAXON,
    )
    with pytest.raises(CuratedReferenceError, match="without its measurement details"):
        copy_curated_bundle_to_personal_library(marked)


def test_one_bad_item_does_not_fail_the_page(caplog):
    good = shared_row()
    marked = {**shared_row(), "measurement_details_omitted": True,
              "contribution_id": "68000000-0000-4000-8000-000000006702"}
    marked["snapshot"] = {**marked["snapshot"],
                          "reference_measurement_set_id": "68000000-0000-4000-8000-000000006702"}
    unknown_version = copy.deepcopy(shared_row())
    unknown_version["snapshot"]["schema_version"] = 3

    class Client:
        def search_public_reference_contributions_v2(self, *args):
            return [unknown_version, good, marked, {"junk": True}, good]

    bundles = search_shared_reference_contributions(Client(), TAXON)
    assert [bundle.measurement_details_omitted for bundle in bundles] == [False, True]
    assert sum("skipping" in record.message for record in caplog.records) == 3


def test_other_block_reasons_of_a_held_sets_child_surface_as_themselves(databases):
    _graph(with_use=True)
    connection = schema.get_connection()
    try:
        connection.execute("UPDATE observations SET cloud_id='not-a-cloud-id' WHERE id=1")
        connection.commit()
    finally:
        connection.close()
    client = HoldClient()

    result = sync_reference_library(client)

    assert result.capability_holds == ("measurement_set:set-a:older_client_active",)
    assert any(
        item.startswith("observation_use:") and item.endswith(":invalid_observation_cloud_id")
        for item in result.blocked
    )


def test_frozen_provenance_rejects_the_marker():
    for row in (shared_row(), bundle_row()):
        envelope = {**row, "measurement_details_omitted": True}
        envelope.pop("relationship_roles", None)
        with pytest.raises(CuratedReferenceError, match="omit measurement details"):
            normalize_curated_bundle(envelope, expected_taxon_id=TAXON, frozen=True)
