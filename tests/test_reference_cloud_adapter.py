from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from utils.cloud_sync import (
    CloudReauthRequiredError,
    CloudSyncError,
    CloudTemporarilyUnavailableError,
    PullOnlyCloudClient,
    PullOnlyModeError,
    SporelyCloudClient,
)
from utils.reference_cloud_adapter import (
    ReferenceCloudAccountMismatchError,
    ReferenceCloudAdapter,
    ReferenceCloudProtocolError,
    ReferenceCloudTransportError,
)


@dataclass
class FakeClient:
    user_id: str = "user-1"
    mutation_response: object = None
    read_rows: list[dict] = field(default_factory=list)
    error: Exception | None = None
    calls: list[tuple] = field(default_factory=list)

    def _mutation(self, name, payload, expected, snapshot_mode=None):
        self.calls.append((name, payload, expected, snapshot_mode))
        if self.error:
            raise self.error
        return self.mutation_response

    def sync_reference_work(self, payload, expected):
        return self._mutation("work", payload, expected)

    def sync_reference_taxon_treatment(self, payload, expected):
        return self._mutation("treatment", payload, expected)

    def sync_reference_measurement_set(self, payload, expected):
        return self._mutation("measurement_set", payload, expected)

    def sync_observation_reference_use(self, payload, expected, snapshot_mode):
        return self._mutation("observation_use", payload, expected, snapshot_mode)

    def _read(self, name):
        self.calls.append((name,))
        if self.error:
            raise self.error
        return self.read_rows

    def list_reference_works(self):
        return self._read("work")

    def list_reference_taxon_treatments(self):
        return self._read("treatment")

    def list_reference_measurement_sets(self):
        return self._read("measurement_set")

    def list_observation_reference_uses(self):
        return self._read("observation_use")


def _row(entity_id="work-1", **values):
    return {
        "id": entity_id,
        "user_id": "user-1",
        "row_version": 1,
        "deleted_at": None,
        **values,
    }


@pytest.mark.parametrize(
    ("method", "payload", "expected_call"),
    [
        (
            "sync_work",
            {"id": "work-1", "type": "book", "revision": 1},
            ("work", {"id": "work-1", "type": "book", "revision": 1}, 0, None),
        ),
        (
            "sync_treatment",
            {"id": "treatment-1", "reference_work_id": "work-1", "revision": 1},
            (
                "treatment",
                {"id": "treatment-1", "reference_work_id": "work-1", "revision": 1},
                0,
                None,
            ),
        ),
        (
            "sync_measurement_set",
            {"id": "set-1", "taxon_treatment_id": "treatment-1", "revision": 1},
            (
                "measurement_set",
                {"id": "set-1", "taxon_treatment_id": "treatment-1", "revision": 1},
                0,
                None,
            ),
        ),
        (
            "sync_observation_use",
            {
                "id": "use-1",
                "observation_id": 7,
                "reference_measurement_set_id": "set-1",
                "reference_revision": 1,
                "snapshot_json": {},
            },
            (
                "observation_use",
                {
                    "id": "use-1",
                    "observation_id": 7,
                    "reference_measurement_set_id": "set-1",
                    "reference_revision": 1,
                    "snapshot_json": {},
                },
                0,
                "historical_import",
            ),
        ),
    ],
)
def test_mutations_forward_exact_typed_request(method, payload, expected_call):
    client = FakeClient(mutation_response={"status": "created", "row": _row(payload["id"])})
    adapter = ReferenceCloudAdapter(client, "user-1")

    kwargs = {"snapshot_mode": "historical_import"} if method == "sync_observation_use" else {}
    result = getattr(adapter, method)(payload, 0, **kwargs)

    assert result.status == "created"
    assert result.disposition == "acknowledged"
    assert client.calls == [expected_call]


def test_tombstone_request_and_response_are_preserved():
    client = FakeClient(
        mutation_response={
            "status": "updated",
            "row": _row("work-1", row_version=4, deleted_at="2026-08-28T12:00:00Z"),
        }
    )
    result = ReferenceCloudAdapter(client, "user-1").sync_work(
        {"id": "work-1", "deleted": True}, 3
    )
    assert result.row["deleted_at"] == "2026-08-28T12:00:00Z"
    assert client.calls[0][1] == {"id": "work-1", "deleted": True}


@pytest.mark.parametrize(
    ("status", "disposition"),
    [
        ("no_change", "acknowledged"),
        ("conflict", "conflict"),
        ("blocked", "blocked"),
        ("invalid_parent", "blocked"),
        ("invalid_payload", "rejected"),
        ("invalid_revision", "rejected"),
        ("invalid_snapshot", "rejected"),
        ("invalid_snapshot_mode", "rejected"),
        ("invalid_successor", "rejected"),
        ("account_deleting", "account_terminal"),
    ],
)
def test_structured_statuses_are_mapped_without_retry(status, disposition):
    row = None if status == "conflict" else _row()
    client = FakeClient(mutation_response={"status": status, "row": row})
    result = ReferenceCloudAdapter(client, "user-1").sync_work(
        {"id": "work-1", "type": "book", "revision": 1}, 1
    )
    assert result.status == status
    assert result.disposition == disposition
    assert len(client.calls) == 1


@pytest.mark.parametrize(
    "response",
    [None, [], {}, {"status": "created"}, {"status": "mystery", "row": None},
     {"status": "created", "row": None},
     {"status": "created", "row": _row(row_version=0)},
     {"status": "created", "row": _row(user_id="other-user")},
     {"status": "created", "row": _row("other-id")},
     {"status": "created", "row": _row(), "extra": True}],
)
def test_malformed_mutation_responses_fail_closed(response):
    client = FakeClient(mutation_response=response)
    with pytest.raises((ReferenceCloudProtocolError, ReferenceCloudAccountMismatchError)):
        ReferenceCloudAdapter(client, "user-1").sync_work(
            {"id": "work-1", "type": "book", "revision": 1}, 0
        )


def test_unknown_or_forbidden_payload_keys_are_rejected_before_call():
    client = FakeClient(mutation_response={"status": "created", "row": _row()})
    adapter = ReferenceCloudAdapter(client, "user-1")
    with pytest.raises(ReferenceCloudProtocolError):
        adapter.sync_work({"id": "work-1", "user_id": "forged"}, 0)
    with pytest.raises(ReferenceCloudProtocolError):
        adapter.sync_work({"id": "work-1", "deleted_at": "now"}, 1)
    assert client.calls == []


@pytest.mark.parametrize("expected", [-1, True, 1.5])
def test_invalid_cas_token_is_rejected_before_call(expected):
    client = FakeClient()
    with pytest.raises(ReferenceCloudProtocolError):
        ReferenceCloudAdapter(client, "user-1").sync_work(
            {"id": "work-1", "type": "book", "revision": 1}, expected
        )
    assert client.calls == []


def test_unsaved_tombstone_is_rejected_before_call():
    client = FakeClient()
    with pytest.raises(ReferenceCloudProtocolError):
        ReferenceCloudAdapter(client, "user-1").sync_work(
            {"id": "work-1", "deleted": True}, 0
        )
    assert client.calls == []


def test_account_mismatch_is_rejected_before_call():
    client = FakeClient(user_id="user-2")
    with pytest.raises(ReferenceCloudAccountMismatchError):
        ReferenceCloudAdapter(client, "user-1").list_works()
    assert client.calls == []


@pytest.mark.parametrize(
    ("error", "retryable"),
    [
        (CloudTemporarilyUnavailableError("offline"), True),
        (CloudReauthRequiredError("expired"), False),
        (CloudSyncError("bad request"), False),
    ],
)
def test_transport_failures_are_explicitly_classified(error, retryable):
    client = FakeClient(error=error)
    with pytest.raises(ReferenceCloudTransportError) as caught:
        ReferenceCloudAdapter(client, "user-1").list_works()
    assert caught.value.retryable is retryable
    assert caught.value.auth_required is isinstance(error, CloudReauthRequiredError)
    assert caught.value.__cause__ is error


@pytest.mark.parametrize(
    ("method", "call_name"),
    [
        ("list_works", "work"),
        ("list_treatments", "treatment"),
        ("list_measurement_sets", "measurement_set"),
        ("list_observation_uses", "observation_use"),
    ],
)
def test_owner_readers_validate_rows(method, call_name):
    client = FakeClient(read_rows=[_row("a"), _row("b", row_version=3)])
    rows = getattr(ReferenceCloudAdapter(client, "user-1"), method)()
    assert [row["id"] for row in rows] == ["a", "b"]
    assert client.calls == [(call_name,)]


def test_owner_reader_rejects_cross_account_row():
    client = FakeClient(read_rows=[_row(user_id="other-user")])
    with pytest.raises(ReferenceCloudAccountMismatchError):
        ReferenceCloudAdapter(client, "user-1").list_works()


def test_pull_only_blocks_reference_writers_and_allows_readers():
    wrapped = FakeClient(read_rows=[])
    client = PullOnlyCloudClient(wrapped)
    assert client.list_reference_works() == []
    assert client.write_attempts == []
    with pytest.raises(PullOnlyModeError):
        client.sync_reference_work({"id": "work-1"}, 0)
    assert client.write_attempts == ["sync_reference_work"]
    assert wrapped.calls == [("work",)]


@pytest.mark.parametrize(
    ("method", "rpc_name", "extra"),
    [
        ("sync_reference_work", "sync_reference_work", {}),
        ("sync_reference_taxon_treatment", "sync_reference_taxon_treatment", {}),
        ("sync_reference_measurement_set", "sync_reference_measurement_set", {}),
        (
            "sync_observation_reference_use",
            "sync_observation_reference_use",
            {"snapshot_mode": "historical_import"},
        ),
    ],
)
def test_cloud_client_rpc_wrappers_use_exact_stage3_parameters(
    monkeypatch, method, rpc_name, extra
):
    client = SporelyCloudClient.__new__(SporelyCloudClient)
    calls = []
    monkeypatch.setattr(
        client, "_rpc", lambda name, body: calls.append((name, body)) or {"ok": True}
    )

    result = getattr(client, method)({"id": "entity-1"}, 7, **extra)

    expected = {
        "p_payload": {"id": "entity-1"},
        "p_expected_row_version": 7,
    }
    if extra:
        expected["p_snapshot_mode"] = "historical_import"
    assert result == {"ok": True}
    assert calls == [(rpc_name, expected)]


@pytest.mark.parametrize(
    ("method", "table"),
    [
        ("list_reference_works", "reference_works"),
        ("list_reference_taxon_treatments", "reference_taxon_treatments"),
        ("list_reference_measurement_sets", "reference_measurement_sets"),
        ("list_observation_reference_uses", "observation_reference_uses"),
    ],
)
def test_cloud_client_owner_readers_use_complete_deterministic_pagination(
    monkeypatch, method, table
):
    client = SporelyCloudClient.__new__(SporelyCloudClient)
    client.user_id = "user-1"
    paths = []
    monkeypatch.setattr(
        client, "_get_paginated", lambda path: paths.append(path) or [{"id": "a"}]
    )

    assert getattr(client, method)() == [{"id": "a"}]
    assert paths[0].startswith(f"{table}?user_id=eq.user-1&select=user_id,id,")
    assert paths[0].endswith("&order=updated_at.asc,id.asc")


def test_reference_owner_reader_returns_all_pages_or_no_partial_result(monkeypatch):
    client = SporelyCloudClient.__new__(SporelyCloudClient)
    client.user_id = "user-1"
    calls = []

    def get_page(path):
        calls.append(path)
        if "offset=0" in path:
            return [{"id": str(index)} for index in range(1000)]
        if "offset=1000" in path:
            return [{"id": "last"}]
        raise AssertionError(path)

    monkeypatch.setattr(client, "_get", get_page)
    rows = client.list_reference_works()
    assert len(rows) == 1001
    assert calls[0].endswith("&order=updated_at.asc,id.asc&limit=1000&offset=0")
    assert calls[1].endswith("&order=updated_at.asc,id.asc&limit=1000&offset=1000")

    monkeypatch.setattr(
        client,
        "_get",
        lambda path: (_ for _ in ()).throw(CloudSyncError("page failed")),
    )
    with pytest.raises(CloudSyncError, match="page failed"):
        client.list_reference_works()


def test_pull_only_classifies_all_reference_methods_before_delegation():
    wrapped = FakeClient(read_rows=[])
    client = PullOnlyCloudClient(wrapped)
    for reader in (
        "list_reference_works",
        "list_reference_taxon_treatments",
        "list_reference_measurement_sets",
        "list_observation_reference_uses",
    ):
        assert getattr(client, reader)() == []
    for writer in (
        "sync_reference_work",
        "sync_reference_taxon_treatment",
        "sync_reference_measurement_set",
        "sync_observation_reference_use",
    ):
        with pytest.raises(PullOnlyModeError):
            getattr(client, writer)({}, 0)
    assert client.write_attempts == [
        "sync_reference_work",
        "sync_reference_taxon_treatment",
        "sync_reference_measurement_set",
        "sync_observation_reference_use",
    ]
    assert wrapped.calls == [
        ("work",), ("treatment",), ("measurement_set",), ("observation_use",)
    ]


def test_pull_only_blocks_reference_writes_through_generic_rpc():
    class RpcClient:
        def __init__(self):
            self.calls = []

        def _rpc(self, name, payload=None):
            self.calls.append((name, payload))
            return {"status": "created", "row": {}}

    wrapped = RpcClient()
    client = PullOnlyCloudClient(wrapped)

    with pytest.raises(PullOnlyModeError):
        client._rpc(
            "sync_reference_work",
            {"p_payload": {"id": "work-1"}, "p_expected_row_version": 0},
        )

    assert client.write_attempts == ["_rpc:sync_reference_work"]
    assert wrapped.calls == []


def test_pull_only_generic_rpc_allows_only_named_read_contracts():
    class RpcClient:
        def _rpc(self, name, payload=None):
            return {"name": name, "payload": payload}

    client = PullOnlyCloudClient(RpcClient())
    assert client._rpc("get_public_observation", {"p_observation_id": 7}) == {
        "name": "get_public_observation",
        "payload": {"p_observation_id": 7},
    }
    assert client.write_attempts == []
