"""Stage 3: observation identity on per-record sync keys (U tier).

Two SQLite databases and one shared fake PostgREST. The fake sits below the
real ``SporelyCloudClient`` at the HTTP-session layer, so the client's own
request, error-parsing, retry and refusal code runs unchanged. It enforces
the server rules the desktop depends on, with their real names:

- per-owner ``observations_user_sync_key_uidx`` and the legacy
  ``observations_desktop_id_user_unique``, both answering ``23505``;
- the identity guard (``P0001`` / ``sporely_sync_identity_refused``): a key
  never changes, and a row never carries both ``sync_key`` and ``desktop_id``;
- the write stop (``P0001`` / ``sporely_upgrade_required``), keyed only on the
  two desktop request headers;
- a pre-key server, which has no ``sync_key`` column.

The only test seams are the crash itself (the fault hook's process exit
becomes an exception) and the transient-retry backoff sleep.
"""
from __future__ import annotations

import copy
import json
import shutil
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlsplit

import pytest
import requests

from database import schema
from database.models import ObservationDB
from utils import cloud_sync
from utils import sync_client_identity
from utils import taxon_identity
from utils.cloud_sync_impl import fault_injection
from utils.r2_storage import CloudflareMediaWorkerClient, MediaUpgradeRequiredError

USER_ID = "8b0e1f8a-3c2d-4e5f-8a9b-0c1d2e3f4a5b"
OTHER_USER_ID = "1f2e3d4c-5b6a-4978-8695-a4b3c2d1e0f9"
CANDIDATE_VERSION = "9.9.9-candidate"

_WRITE_BLOCK_MESSAGE = (
    "This version of Sporely Desktop is blocked from uploading. Install the latest "
    "version from sporely.no. Nothing was changed in the cloud."
)


class _Killed(BaseException):
    """The process exit of ``SPORELY_SYNC_FAULT``, raised so the test survives it."""


class _FakeResponse:
    def __init__(self, status: int, body=None, headers: dict | None = None):
        self.status_code = status
        self.ok = status < 400
        self.headers = dict(headers or {})
        self.content = b"" if body is None else json.dumps(body).encode()
        self.text = self.content.decode()

    def json(self):
        return json.loads(self.text) if self.text else None

    def iter_content(self, chunk_size=1):
        yield self.content

    def close(self):
        pass


def _error(status: int, code: str, message: str, hint: str | None = None) -> _FakeResponse:
    return _FakeResponse(status, {"code": code, "message": message, "details": None, "hint": hint})


class FakePostgrest:
    """One cloud ``observations`` table shared by every client in a test."""

    def __init__(self, *, sync_key_schema: bool = True):
        self.sync_key_schema = sync_key_schema
        self.rows: list[dict] = []
        self.write_block: set[str] = set()
        self.requests: list[dict] = []
        self.inject_post_error: _FakeResponse | None = None
        self.timeout_after_next_insert = False
        self.after_get = None  # callable(filters) run after answering a GET
        self._next_id = 1000

    # ── wiring ──────────────────────────────────────────────────────────
    def client(self, user_id: str = USER_ID) -> cloud_sync.SporelyCloudClient:
        client = cloud_sync.SporelyCloudClient("token", user_id)
        session = client._s

        def request(method, url, **kwargs):
            headers = {**dict(session.headers), **dict(kwargs.get("headers") or {})}
            return self.handle(method, url, headers=headers, payload=kwargs.get("json"))

        session.request = request
        return client

    # ── views for assertions ───────────────────────────────────────────
    def observation_writes(self) -> list[dict]:
        return [
            r for r in self.requests
            if r["table"] == "observations" and r["method"] in {"POST", "PATCH", "DELETE"}
        ]

    def rows_for(self, user_id: str = USER_ID) -> list[dict]:
        return [copy.deepcopy(r) for r in self.rows if r["user_id"] == user_id]

    def insert_legacy_row(self, **values) -> dict:
        """A row written by a released desktop: ``desktop_id``, no key."""
        row = self._new_row({"user_id": USER_ID, **values})
        row["sync_key"] = None
        self.rows.append(row)
        return copy.deepcopy(row)

    def edit_elsewhere(self, row_id: int, **values) -> None:
        for row in self.rows:
            if row["id"] == row_id:
                row.update(values)
                row["updated_at"] = _now()

    # ── PostgREST emulation ────────────────────────────────────────────
    def handle(self, method: str, url: str, *, headers: dict, payload) -> _FakeResponse:
        parts = urlsplit(url)
        path = unquote(parts.path.split("/rest/v1/", 1)[-1])
        params = parse_qsl(parts.query, keep_blank_values=True)
        table = path.split("/", 1)[0] if not path.startswith("rpc/") else "rpc"
        self.requests.append({
            "method": method.upper(), "table": table, "path": path,
            "params": params, "headers": dict(headers), "payload": copy.deepcopy(payload),
        })
        if table == "rpc":
            return self._rpc(path.split("/", 1)[1], dict(payload or {}), headers)
        if table != "observations":
            return self._other_table(method.upper(), path)
        filters = [(k, v) for k, v in params if k not in {"select", "order", "limit", "offset"}]
        mentions_key = any(k == "sync_key" for k, _ in filters) or any(
            k == "select" and "sync_key" in v.split(",") for k, v in params
        )
        if not self.sync_key_schema and mentions_key:
            return _error(400, "42703", "column observations.sync_key does not exist")
        method = method.upper()
        if method == "GET":
            response = self._get(params, filters)
            if self.after_get is not None:
                self.after_get(filters)
            return response
        refusal = self._write_block_refusal(headers)
        if refusal is not None:
            return refusal
        if method == "POST":
            return self._post(dict(payload or {}))
        if method == "PATCH":
            return self._patch(filters, dict(payload or {}), headers)
        if method == "DELETE":
            matched = self._match(filters)
            self.rows = [r for r in self.rows if r not in matched]
            return _FakeResponse(204)
        return _error(405, "PGRST000", "unsupported")

    _WRITE_RPCS = frozenset({
        "set_observation_selected_taxon_v2", "set_observation_identification_v2",
        "set_observation_location",
    })

    def _rpc(self, name: str, payload: dict, headers: dict) -> _FakeResponse:
        if name in self._WRITE_RPCS:
            refusal = self._write_block_refusal(headers)
            if refusal is not None:
                return refusal
        if name == "set_observation_selected_taxon_v2":
            self.edit_elsewhere(
                int(payload["p_observation_id"]),
                selected_sporely_taxon_id=payload["p_sporely_taxon_id"],
            )
        return _FakeResponse(200, None)

    def _other_table(self, method: str, path: str) -> _FakeResponse:
        if method == "GET":
            return _FakeResponse(200, [])
        if path.startswith("rpc/"):
            return _FakeResponse(200, None)
        return _FakeResponse(201, [])

    def _write_block_refusal(self, headers: dict) -> _FakeResponse | None:
        lowered = {str(k).lower(): str(v) for k, v in headers.items()}
        if (
            "x-sporely-sync-identity" in lowered
            and lowered.get("x-sporely-client-version") in self.write_block
        ):
            return _error(400, "P0001", _WRITE_BLOCK_MESSAGE, "sporely_upgrade_required")
        return None

    def _match(self, filters) -> list[dict]:
        matched = list(self.rows)
        for column, expression in filters:
            op, _, value = expression.partition(".")
            if op == "eq":
                matched = [r for r in matched if r.get(column) is not None and str(r.get(column)) == value]
            elif op == "is" and value == "null":
                matched = [r for r in matched if r.get(column) is None]
            else:
                raise AssertionError(f"fake PostgREST: unsupported filter {column}={expression}")
        return matched

    def _get(self, params, filters) -> _FakeResponse:
        rows = sorted(self._match(filters), key=lambda r: r["id"])
        query = dict(params)
        offset = int(query.get("offset") or 0)
        rows = rows[offset:]
        if "limit" in query:
            rows = rows[: int(query["limit"])]
        return _FakeResponse(200, copy.deepcopy(rows))

    def _new_row(self, values: dict) -> dict:
        self._next_id += 1
        now = _now()
        row = {"id": self._next_id, "desktop_id": None, "sync_key": None,
               "created_at": now, "updated_at": now}
        row.update(values)
        return row

    def _guard(self, row: dict, before: dict | None) -> _FakeResponse | None:
        if before is not None and before.get("sync_key") != row.get("sync_key"):
            return _error(400, "P0001", "sync_key is immutable", "sporely_sync_identity_refused")
        if row.get("sync_key") is not None and row.get("desktop_id") is not None:
            return _error(
                400, "P0001", "a row with a sync_key cannot carry desktop_id",
                "sporely_sync_identity_refused",
            )
        return None

    def _unique_violation(self, row: dict, existing: dict | None = None) -> _FakeResponse | None:
        for other in self.rows:
            if other is existing or other["user_id"] != row["user_id"]:
                continue
            if row.get("sync_key") is not None and other.get("sync_key") == row.get("sync_key"):
                return _error(
                    409, "23505",
                    'duplicate key value violates unique constraint "observations_user_sync_key_uidx"',
                )
            if row.get("desktop_id") is not None and other.get("desktop_id") == row.get("desktop_id"):
                return _error(
                    409, "23505",
                    'duplicate key value violates unique constraint "observations_desktop_id_user_unique"',
                )
        return None

    def _post(self, payload: dict) -> _FakeResponse:
        if self.inject_post_error is not None:
            injected, self.inject_post_error = self.inject_post_error, None
            return injected
        if not self.sync_key_schema and "sync_key" in payload:
            return _error(400, "PGRST204", "Could not find the 'sync_key' column of 'observations'")
        row = self._new_row(payload)
        refusal = self._guard(row, None) or self._unique_violation(row)
        if refusal is not None:
            self._next_id -= 1
            return refusal
        self.rows.append(row)
        if self.timeout_after_next_insert:
            self.timeout_after_next_insert = False
            raise requests.Timeout("read timed out after the INSERT landed")
        return _FakeResponse(201, [copy.deepcopy(row)])

    def _patch(self, filters, payload: dict, headers: dict) -> _FakeResponse:
        updated = []
        for row in self._match(filters):
            candidate = {**row, **payload}
            refusal = self._guard(candidate, row) or self._unique_violation(candidate, row)
            if refusal is not None:
                return refusal
            row.update(payload)
            row["updated_at"] = _now()
            updated.append(copy.deepcopy(row))
        prefer = str(headers.get("Prefer") or "")
        return _FakeResponse(200, updated) if "representation" in prefer else _FakeResponse(204)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Two local databases ──────────────────────────────────────────────────


class _Installation:
    def __init__(self, root: Path):
        self.root = root

    def activate(self, monkeypatch) -> None:
        monkeypatch.setattr(schema, "_app_dir", self.root)
        monkeypatch.setattr(schema, "DATABASE_PATH", self.root / "mushrooms.db")
        monkeypatch.setattr(schema, "REFERENCE_DATABASE_PATH", self.root / "reference_values.db")
        monkeypatch.setattr(schema, "SETTINGS_PATH", self.root / "app_settings.json")


@pytest.fixture
def two_desktops(monkeypatch, tmp_path):
    installations = {}
    for name in ("A", "B"):
        root = tmp_path / name
        root.mkdir()
        installation = _Installation(root)
        installation.activate(monkeypatch)
        schema.init_database()
        installations[name] = installation
    installations["A"].activate(monkeypatch)
    monkeypatch.setattr(cloud_sync, "_sleep_supabase_backoff", lambda attempt: None)
    monkeypatch.setattr(fault_injection, "_terminate", _raise_killed)
    monkeypatch.delenv(fault_injection.SYNC_FAULT_ENV_VAR, raising=False)
    sync_client_identity.set_client_version(CANDIDATE_VERSION)
    yield installations
    sync_client_identity.set_client_version(None)


def _raise_killed() -> None:
    raise _Killed()


def _local(local_id: int) -> dict:
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.row_factory = sqlite3.Row
        return dict(connection.execute("SELECT * FROM observations WHERE id=?", (local_id,)).fetchone())


def _create(**values) -> int:
    return ObservationDB.create_observation(
        date=values.pop("date", "2026-10-01"),
        genus=values.pop("genus", "Cortinarius"),
        species=values.pop("species", "violaceus"),
        **values,
    )


def _push(client) -> dict:
    return cloud_sync.push_all(
        client,
        sync_images=False,
        remote_obs=client.list_remote_observations(),
        sync_calibrations=False,
        full_pull=False,
        verify_stamped_measurements=False,
    )


def _no_identity_columns_sent(cloud: FakePostgrest) -> None:
    for write in cloud.observation_writes():
        assert "desktop_id" not in (write["payload"] or {}), write
        if write["method"] == "PATCH":
            assert "sync_key" not in (write["payload"] or {}), write


# ── Matrix row 1: overlapping local ids on two desktops ──────────────────


def test_overlapping_local_ids_create_two_rows_and_leave_the_first_untouched(two_desktops, monkeypatch):
    cloud = FakePostgrest()
    two_desktops["A"].activate(monkeypatch)
    a_id = _create(genus="Amanita", species="muscaria")
    assert _push(cloud.client())["errors"] == []
    a_link = _local(a_id)["cloud_id"]
    a_row_before = cloud.rows_for()[0]

    two_desktops["B"].activate(monkeypatch)
    b_id = _create(genus="Boletus", species="edulis")
    assert b_id == a_id, "the fixture needs overlapping local id sequences"
    assert _push(cloud.client())["errors"] == []
    b_link = _local(b_id)["cloud_id"]

    rows = {row["id"]: row for row in cloud.rows_for()}
    assert len(rows) == 2 and str(a_link) != str(b_link)
    assert rows[int(a_link)] == a_row_before, "B's push changed A's cloud row"
    assert rows[int(b_link)]["genus"] == "Boletus"
    assert all(row["desktop_id"] is None for row in rows.values())
    assert {row["sync_key"] for row in rows.values()} == {
        _local(b_id)["sync_key"], a_row_before["sync_key"],
    }
    _no_identity_columns_sent(cloud)


def test_every_data_api_request_carries_both_desktop_headers(two_desktops):
    cloud = FakePostgrest()
    _create()
    _push(cloud.client())
    assert cloud.requests
    for request in cloud.requests:
        headers = {k.lower(): v for k, v in request["headers"].items()}
        assert headers["x-sporely-sync-identity"] == "2"
        assert headers["x-sporely-client-version"] == CANDIDATE_VERSION


def test_an_l2_edit_patches_by_verified_cloud_id_without_identity_columns(two_desktops):
    cloud = FakePostgrest()
    local_id = _create()
    _push(cloud.client())
    cloud_id = _local(local_id)["cloud_id"]
    ObservationDB.update_observation(local_id, notes="edited on A")
    cloud.requests.clear()
    assert _push(cloud.client())["errors"] == []
    patches = [w for w in cloud.observation_writes() if w["method"] == "PATCH"]
    assert [p["params"] for p in patches] == [[("id", f"eq.{cloud_id}")]]
    assert not [w for w in cloud.observation_writes() if w["method"] == "POST"]
    _no_identity_columns_sent(cloud)


# ── Matrix rows 6–8: crash and timeout around the create ──────────────────


def test_kill_after_the_insert_adopts_the_row_on_retry(two_desktops, monkeypatch):
    cloud = FakePostgrest()
    local_id = _create()
    monkeypatch.setenv(fault_injection.SYNC_FAULT_ENV_VAR, "after_insert")
    with pytest.raises(_Killed):
        _push(cloud.client())
    assert _local(local_id)["cloud_id"] is None
    assert len(cloud.rows_for()) == 1

    monkeypatch.delenv(fault_injection.SYNC_FAULT_ENV_VAR)
    cloud.requests.clear()
    summary = cloud_sync._new_sync_summary()
    with cloud_sync._cloud_sync_summary_scope(summary):
        assert _push(cloud.client())["errors"] == []
    rows = cloud.rows_for()
    assert len(rows) == 1
    assert str(_local(local_id)["cloud_id"]) == str(rows[0]["id"])
    assert rows[0]["sync_key"] == _local(local_id)["sync_key"]
    writes = cloud.observation_writes()
    assert [w["method"] for w in writes] == ["POST"], "adoption must never PATCH"
    assert summary["observations_adopted"] == 1


def test_timeout_after_a_successful_insert_adopts_without_a_duplicate(two_desktops):
    cloud = FakePostgrest()
    local_id = _create()
    cloud.timeout_after_next_insert = True
    assert _push(cloud.client())["errors"] == []
    rows = cloud.rows_for()
    assert len(rows) == 1
    assert str(_local(local_id)["cloud_id"]) == str(rows[0]["id"])
    assert [w["method"] for w in cloud.observation_writes()] == ["POST", "POST"]


def test_kill_before_the_request_lands_inserts_once_on_retry(two_desktops, monkeypatch):
    cloud = FakePostgrest()
    local_id = _create()
    monkeypatch.setenv(fault_injection.SYNC_FAULT_ENV_VAR, "before_request")
    with pytest.raises(_Killed):
        _push(cloud.client())
    assert cloud.rows_for() == [] and cloud.observation_writes() == []

    monkeypatch.delenv(fault_injection.SYNC_FAULT_ENV_VAR)
    assert _push(cloud.client())["errors"] == []
    assert len(cloud.rows_for()) == 1
    assert [w["method"] for w in cloud.observation_writes()] == ["POST"]
    assert _local(local_id)["cloud_id"] is not None


def test_the_fault_hook_is_inert_unless_set_and_scoped_by_table(monkeypatch):
    monkeypatch.setattr(fault_injection, "_terminate", _raise_killed)
    monkeypatch.delenv(fault_injection.SYNC_FAULT_ENV_VAR, raising=False)
    fault_injection.sync_fault_point("after_insert", table="observations")
    monkeypatch.setenv(fault_injection.SYNC_FAULT_ENV_VAR, "after_insert:observation_images")
    fault_injection.sync_fault_point("after_insert", table="observations")
    with pytest.raises(_Killed):
        fault_injection.sync_fault_point("after_insert", table="observation_images")


# ── Matrix rows 13–13d: copied databases and other refusals ───────────────


def _copy_database(source: _Installation, target: _Installation) -> None:
    # Fold the WAL into the main file first, as copying a closed library does.
    with sqlite3.connect(source.root / "mushrooms.db") as connection:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    for suffix in ("-wal", "-shm"):
        (target.root / f"mushrooms.db{suffix}").unlink(missing_ok=True)
    shutil.copyfile(source.root / "mushrooms.db", target.root / "mushrooms.db")


def test_copied_database_with_unchanged_content_adopts_silently(two_desktops, monkeypatch):
    cloud = FakePostgrest()
    local_id = _create(notes="same everywhere")
    _copy_database(two_desktops["A"], two_desktops["B"])
    _push(cloud.client())

    two_desktops["B"].activate(monkeypatch)
    cloud.requests.clear()
    result = _push(cloud.client())
    assert result["errors"] == []
    rows = cloud.rows_for()
    assert len(rows) == 1
    assert str(_local(local_id)["cloud_id"]) == str(rows[0]["id"])
    assert [w["method"] for w in cloud.observation_writes()] == ["POST"]


def test_copied_database_edited_elsewhere_raises_a_conflict_and_never_patches(two_desktops, monkeypatch):
    cloud = FakePostgrest()
    local_id = _create(notes="original")
    _copy_database(two_desktops["A"], two_desktops["B"])
    _push(cloud.client())
    cloud_row = cloud.rows_for()[0]
    cloud.edit_elsewhere(cloud_row["id"], notes="edited on the web")
    expected = cloud.rows_for()

    two_desktops["B"].activate(monkeypatch)
    for _attempt in range(2):
        cloud.requests.clear()
        result = _push(cloud.client())
        assert any("needs review" in str(error) for error in result["errors"]), result["errors"]
        assert [w["method"] for w in cloud.observation_writes()] == ["POST"]
        assert cloud.rows_for() == expected, "the adopted row must not change"
        local = _local(local_id)
        assert local["cloud_id"] is None, "a differing copy is never linked automatically"
        assert local["sync_blocked_reason"] == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER

    # The owner keeps the cloud copy: the row links to its own keyed cloud row.
    cloud_sync.resolve_conflict_keep_cloud(cloud.client(), local_id, cloud_id=str(cloud_row["id"]))
    local = _local(local_id)
    assert str(local["cloud_id"]) == str(cloud_row["id"])
    assert local["notes"] == "edited on the web"
    assert cloud.rows_for() == expected


def test_keep_local_on_an_adoption_conflict_patches_the_keyed_row_by_id(two_desktops, monkeypatch):
    cloud = FakePostgrest()
    local_id = _create(notes="original")
    _copy_database(two_desktops["A"], two_desktops["B"])
    _push(cloud.client())
    cloud_row = cloud.rows_for()[0]
    cloud.edit_elsewhere(cloud_row["id"], notes="edited on the web")

    two_desktops["B"].activate(monkeypatch)
    _push(cloud.client())
    cloud.requests.clear()
    cloud_sync.resolve_conflict_keep_local(cloud.client(), local_id)
    assert str(_local(local_id)["cloud_id"]) == str(cloud_row["id"])
    assert cloud.rows_for()[0]["notes"] == "original"
    patches = [w for w in cloud.observation_writes() if w["method"] == "PATCH"]
    assert patches and all(p["params"] == [("id", f"eq.{cloud_row['id']}")] for p in patches)
    _no_identity_columns_sent(cloud)


def test_purged_cloud_copy_of_a_linked_row_is_never_recreated(two_desktops, monkeypatch):
    cloud = FakePostgrest()
    local_id = _create()
    _push(cloud.client())
    _copy_database(two_desktops["A"], two_desktops["B"])
    cloud.rows.clear()  # purged in the cloud

    two_desktops["B"].activate(monkeypatch)
    ObservationDB.update_observation(local_id, notes="edited on the copy")
    cloud.requests.clear()
    _push(cloud.client())
    assert cloud.rows_for() == []
    assert cloud.observation_writes() == []

    with pytest.raises(cloud_sync.ObservationIdentityConflictError):
        cloud.client().push_observation(ObservationDB.get_observation(local_id))
    assert cloud.observation_writes() == []


def test_foreign_key_refusal_is_a_plain_error_not_adoption(two_desktops):
    cloud = FakePostgrest()
    local_id = _create()
    cloud.inject_post_error = _error(
        409, "23503",
        'insert or update on table "observations" violates foreign key constraint '
        '"observations_region_id_fkey"',
    )
    result = _push(cloud.client())
    assert len(result["errors"]) == 1 and "23503" in result["errors"][0]
    assert _local(local_id)["cloud_id"] is None
    assert [r["method"] for r in cloud.requests if r["table"] == "observations"][-1] == "POST"
    assert not any(
        ("sync_key", f"eq.{_local(local_id)['sync_key']}") in r["params"] for r in cloud.requests
    ), "a 23503 must never trigger the adoption lookup"


def test_unique_violation_on_the_legacy_constraint_is_a_plain_error(two_desktops):
    cloud = FakePostgrest()
    local_id = _create()
    cloud.inject_post_error = _error(
        409, "23505",
        'duplicate key value violates unique constraint "observations_desktop_id_user_unique"',
    )
    result = _push(cloud.client())
    assert len(result["errors"]) == 1
    assert _local(local_id)["cloud_id"] is None
    assert cloud.rows_for() == []
    assert not any(("sync_key", f"eq.{_local(local_id)['sync_key']}") in r["params"] for r in cloud.requests)


def test_a_row_without_a_committed_key_is_never_posted(two_desktops):
    cloud = FakePostgrest()
    local_id = _create()
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("UPDATE observations SET sync_key = NULL WHERE id = ?", (local_id,))
    result = _push(cloud.client())
    assert "no committed sync key" in " ".join(result["errors"])
    assert cloud.observation_writes() == []


def test_another_tenant_with_the_same_key_is_not_a_conflict(two_desktops):
    cloud = FakePostgrest()
    local_id = _create()
    shared_key = _local(local_id)["sync_key"]
    cloud.rows.append(cloud._new_row({"user_id": OTHER_USER_ID, "sync_key": shared_key, "genus": "X"}))
    assert _push(cloud.client())["errors"] == []
    assert len(cloud.rows_for()) == 1


# ── Matrix row 15: server without the key schema ──────────────────────────


def _sync(client, **kwargs) -> dict:
    return cloud_sync.sync_all(
        client, sync_images=False, materialize_remote_images=False, full_pull=False, **kwargs,
    )


def _observation_errors(result: dict) -> list[str]:
    # The fake answers reference-library RPCs with nothing; their errors are
    # outside this stage.
    return [e for e in result.get("errors") or [] if not str(e).startswith("reference ")]


def test_cloud_not_ready_blocks_uploads_and_pull_keeps_working(two_desktops):
    cloud = FakePostgrest(sync_key_schema=False)
    local_id = _create()
    web_row = cloud._new_row({"user_id": USER_ID, "genus": "Russula", "species": "emetica",
                              "date": "2026-09-30", "visibility": "private"})
    web_row.pop("sync_key")
    cloud.rows.append(web_row)

    result = _sync(cloud.client())
    assert result["upload_blocked"] == sync_client_identity.UPLOAD_BLOCK_CLOUD_NOT_READY
    assert result["upload_blocked_message"] == sync_client_identity.CLOUD_NOT_READY_MESSAGE
    assert cloud.observation_writes() == []
    assert _local(local_id)["cloud_id"] is None
    pulled = [row for row in ObservationDB.get_all_observations() if row.get("cloud_id")]
    assert [str(row["cloud_id"]) for row in pulled] == [str(web_row["id"])]
    assert schema.get_app_settings()[sync_client_identity.UPLOAD_BLOCK_SETTING] == "cloud_not_ready"

    cloud.sync_key_schema = True
    result = _sync(cloud.client())
    assert "upload_blocked" not in result
    assert _local(local_id)["cloud_id"] is not None
    assert schema.get_app_settings()[sync_client_identity.UPLOAD_BLOCK_SETTING] == ""


# ── Write stop and identity-guard refusals ────────────────────────────────


def test_write_stop_blocks_this_version_keeps_rows_dirty_and_unblock_uploads(two_desktops):
    cloud = FakePostgrest()
    first = _create(genus="Lactarius")
    assert _observation_errors(_sync(cloud.client())) == []
    ObservationDB.update_observation(first, notes="edited while blocked")
    second = _create(genus="Hygrocybe")
    web_row = cloud._new_row({"user_id": USER_ID, "genus": "Russula", "date": "2026-09-30"})
    cloud.rows.append(web_row)

    cloud.write_block.add(CANDIDATE_VERSION)
    rows_before = cloud.rows_for()
    cloud.requests.clear()
    blocked_client = cloud.client()
    result = _sync(blocked_client)
    assert result["upload_blocked"] == sync_client_identity.UPLOAD_BLOCK_UPGRADE_REQUIRED
    assert result["upload_blocked_message"] == sync_client_identity.UPGRADE_REQUIRED_MESSAGE
    assert cloud.rows_for() == rows_before, "nothing changed in the cloud"
    refused = [w for w in cloud.observation_writes()]
    assert len(refused) == 1, "the first refusal stops the upload phase"
    assert _local(first)["sync_status"] == "dirty"
    assert _local(second)["cloud_id"] is None
    assert any(str(row.get("cloud_id")) == str(web_row["id"]) for row in ObservationDB.get_all_observations()), (
        "pull keeps working while uploads are refused"
    )
    assert schema.get_app_settings()[sync_client_identity.UPLOAD_BLOCK_SETTING] == "upgrade_required"
    assert not cloud_sync.is_cloud_auth_error(result["upload_blocked_message"])

    # A released desktop sends no identity header and is never blocked.
    released = cloud.client()
    for header in ("x-sporely-sync-identity", "x-sporely-client-version"):
        released._s.headers.pop(header, None)
    released._patch(f"observations?id=eq.{web_row['id']}", {"notes": "released desktop"})
    assert cloud.rows_for()[-1]["notes"] == "released desktop"

    # The same long-lived client uploads again once the version is unblocked.
    cloud.write_block.clear()
    result = _sync(blocked_client)
    assert "upload_blocked" not in result
    assert _local(first)["sync_status"] == "synced"
    assert _local(second)["cloud_id"] is not None
    assert schema.get_app_settings()[sync_client_identity.UPLOAD_BLOCK_SETTING] == ""


def test_identity_refusal_stops_every_later_write_without_a_request(two_desktops):
    cloud = FakePostgrest()
    local_id = _create()
    _push(cloud.client())
    cloud_id = _local(local_id)["cloud_id"]
    client = cloud.client()
    with pytest.raises(cloud_sync.CloudUploadRefusedError) as refused:
        client._patch(f"observations?id=eq.{cloud_id}", {"desktop_id": local_id})
    assert refused.value.hint == "sporely_sync_identity_refused"
    assert client.upload_refusal is not None
    sent = len(cloud.requests)
    with pytest.raises(cloud_sync.CloudUploadRefusedError):
        client._post("observations", {"user_id": USER_ID, "sync_key": str(uuid.uuid4())})
    assert len(cloud.requests) == sent
    assert client._get(f"observations?id=eq.{cloud_id}"), "reads keep working"


def test_media_worker_426_raises_and_latches_the_upload_stop(monkeypatch):
    sync_client_identity.set_client_version(CANDIDATE_VERSION)
    try:
        client = cloud_sync.SporelyCloudClient("token", USER_ID)
        worker = client._get_media_worker()
    finally:
        sync_client_identity.set_client_version(None)
    seen = {}

    def request(method, url, **kwargs):
        seen["headers"] = {**dict(worker._session.headers), **dict(kwargs.get("headers") or {})}
        return _FakeResponse(426, {"error": "upgrade"}, {"X-Sporely-Error-Code": "sporely_upgrade_required"})

    worker._session.request = request
    with pytest.raises(MediaUpgradeRequiredError):
        worker.delete_object("u/x/obs/1/a.webp")
    assert client.upload_refusal is not None
    assert seen["headers"]["x-sporely-sync-identity"] == "2"
    assert seen["headers"]["x-sporely-client-version"] == CANDIDATE_VERSION
    assert isinstance(worker, CloudflareMediaWorkerClient)


# ── Matrix row 22: one-time legacy recovery ───────────────────────────────


def _mark_legacy(local_id: int) -> None:
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute(
            "UPDATE observations SET legacy_recovery_pending = 1 WHERE id = ?", (local_id,)
        )


def _legacy_copy(local_id: int, **overrides) -> dict:
    local = ObservationDB.get_observation(local_id)
    payload = cloud_sync._observation_push_payload(local, local=True)
    payload.pop("region_id", None)
    return {**payload, "desktop_id": local_id, **overrides}


def test_legacy_recovery_links_an_equal_keyless_row_without_any_cloud_write(two_desktops):
    cloud = FakePostgrest()
    local_id = _create(notes="uploaded by v0.9.25 before it crashed")
    _mark_legacy(local_id)
    legacy = cloud.insert_legacy_row(**_legacy_copy(local_id))
    rows_before = cloud.rows_for()

    summary = cloud_sync._new_sync_summary()
    with cloud_sync._cloud_sync_summary_scope(summary):
        assert _push(cloud.client())["errors"] == []
    assert cloud.observation_writes() == []
    assert cloud.rows_for() == rows_before, "no key is added and desktop_id is never written"
    local = _local(local_id)
    assert str(local["cloud_id"]) == str(legacy["id"])
    assert local["legacy_recovery_pending"] == 0
    assert summary.get("observations_possible_duplicate", 0) == 0


def test_legacy_recovery_with_different_content_posts_a_new_row_and_reports_a_duplicate(two_desktops):
    cloud = FakePostgrest()
    local_id = _create(notes="this desktop's observation")
    _mark_legacy(local_id)
    other_device = cloud.insert_legacy_row(**_legacy_copy(local_id, notes="another desktop's row"))

    summary = cloud_sync._new_sync_summary()
    with cloud_sync._cloud_sync_summary_scope(summary):
        assert _push(cloud.client())["errors"] == []
    rows = {row["id"]: row for row in cloud.rows_for()}
    assert len(rows) == 2
    assert rows[other_device["id"]] == other_device, "the matched row is never PATCHed"
    local = _local(local_id)
    assert int(local["cloud_id"]) != other_device["id"]
    assert rows[int(local["cloud_id"])]["sync_key"] == local["sync_key"]
    assert local["legacy_recovery_pending"] == 0
    assert summary["observations_possible_duplicate"] == 1
    assert "possible duplicate of an earlier upload" in cloud_sync.format_sync_summary(summary)
    assert [w["method"] for w in cloud.observation_writes()] == ["POST"]


def test_legacy_recovery_with_several_matches_posts_and_never_touches_them(two_desktops):
    cloud = FakePostgrest()
    local_id = _create()
    _mark_legacy(local_id)
    first = cloud.insert_legacy_row(**_legacy_copy(local_id))
    second = cloud._new_row({**_legacy_copy(local_id), "user_id": USER_ID})
    second["sync_key"] = None
    cloud.rows.append(second)  # duplicates the legacy constraint would refuse today

    summary = cloud_sync._new_sync_summary()
    with cloud_sync._cloud_sync_summary_scope(summary):
        assert _push(cloud.client())["errors"] == []
    local = _local(local_id)
    assert int(local["cloud_id"]) not in {first["id"], second["id"]}
    assert summary["observations_possible_duplicate"] == 1


def test_legacy_recovery_with_no_match_posts_and_reports_a_possible_duplicate(two_desktops):
    cloud = FakePostgrest()
    local_id = _create()
    _mark_legacy(local_id)

    summary = cloud_sync._new_sync_summary()
    with cloud_sync._cloud_sync_summary_scope(summary):
        assert _push(cloud.client())["errors"] == []
    assert len(cloud.rows_for()) == 1
    assert _local(local_id)["legacy_recovery_pending"] == 0
    assert summary["observations_possible_duplicate"] == 1


def test_unmarked_rows_never_make_the_legacy_lookup(two_desktops):
    cloud = FakePostgrest()
    local_id = _create()
    cloud.insert_legacy_row(**_legacy_copy(local_id))
    _push(cloud.client())
    assert not any(("desktop_id", f"eq.{local_id}") in r["params"] for r in cloud.requests)
    assert len(cloud.rows_for()) == 2


def test_marker_migration_marks_only_rows_unlinked_at_upgrade(tmp_path):
    connection = sqlite3.connect(tmp_path / "legacy.db")
    connection.executescript(
        """
        CREATE TABLE observations (id INTEGER PRIMARY KEY, cloud_id TEXT);
        CREATE TABLE images (id INTEGER PRIMARY KEY, observation_id INTEGER, cloud_id TEXT);
        CREATE TABLE spore_measurements (id INTEGER PRIMARY KEY, image_id INTEGER, cloud_id TEXT);
        INSERT INTO observations VALUES (1, NULL), (2, '55'), (3, '  ');
        INSERT INTO images VALUES (10, 1, NULL), (11, 2, 'img');
        INSERT INTO spore_measurements VALUES (20, 10, NULL), (21, 11, 'meas');
        """
    )
    cursor = connection.cursor()
    schema.ensure_sync_key_columns(cursor)
    schema.ensure_legacy_recovery_marker_columns(cursor)
    marker = schema.LEGACY_RECOVERY_MARKER_COLUMN
    assert dict(cursor.execute(f"SELECT id, {marker} FROM observations")) == {1: 1, 2: 0, 3: 1}
    assert dict(cursor.execute(f"SELECT id, {marker} FROM images")) == {10: 1, 11: 0}
    assert dict(cursor.execute(f"SELECT id, {marker} FROM spore_measurements")) == {20: 1, 21: 0}

    cursor.execute("INSERT INTO observations (id, cloud_id) VALUES (4, NULL)")
    cursor.execute("UPDATE observations SET legacy_recovery_pending = 0 WHERE id = 1")
    schema.ensure_legacy_recovery_marker_columns(cursor)
    assert dict(cursor.execute(f"SELECT id, {marker} FROM observations")) == {1: 0, 2: 0, 3: 1, 4: 0}
    connection.close()


def test_pull_never_heals_the_observation_desktop_id(two_desktops):
    cloud = FakePostgrest()
    web_row = cloud._new_row({"user_id": USER_ID, "genus": "Russula", "date": "2026-09-30",
                              "sync_key": str(uuid.uuid4())})
    cloud.rows.append(web_row)
    _sync(cloud.client())
    _sync(cloud.client())
    assert cloud.observation_writes() == []
    assert cloud.rows_for()[0]["desktop_id"] is None


def test_imported_forks_are_never_legacy_recovery_candidates():
    from utils import db_share
    from utils.archive import portable_import

    bundle_row = {"sync_key": str(uuid.uuid4()), "legacy_recovery_pending": 1, "cloud_id": "55"}
    original_key = bundle_row["sync_key"]
    db_share._fork_bundle_row(
        bundle_row, {"sync_key", "legacy_recovery_pending", "cloud_id"}, ("cloud_id",),
    )
    assert bundle_row["legacy_recovery_pending"] == 0
    assert bundle_row["cloud_id"] is None and bundle_row["sync_key"] != original_key

    portable_row = {"legacy_recovery_pending": 1}
    portable_import._clear_legacy_recovery_marker(portable_row)
    assert portable_row["legacy_recovery_pending"] == 0


# ── Adoption compares the uploaded content, identity and provenance included ─


def _make_proven(local_id: int, taxon_id: int = 4242) -> None:
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute(
            "UPDATE observations SET sporely_taxon_id = ?, taxon_identity_state = ?, "
            "taxon_identity_proof = ? WHERE id = ?",
            (taxon_id, taxon_identity.STATE_SPORELY, taxon_identity.PROOF_TAXONOMY_V2_ARTIFACT, local_id),
        )


def test_adoption_of_a_copy_whose_selection_was_cleared_is_a_conflict(two_desktops, monkeypatch):
    cloud = FakePostgrest()
    local_id = _create()
    _make_proven(local_id)
    _copy_database(two_desktops["A"], two_desktops["B"])
    _push(cloud.client())
    row = cloud.rows_for()[0]
    assert row["selected_sporely_taxon_id"] == 4242, "the create's identity step ran"
    cloud.edit_elsewhere(row["id"], selected_sporely_taxon_id=None)  # cleared on the web
    expected = cloud.rows_for()

    two_desktops["B"].activate(monkeypatch)
    cloud.requests.clear()
    result = _push(cloud.client())
    assert any("needs review" in str(error) for error in result["errors"])
    assert cloud.rows_for() == expected, "a cleared selection is never re-asserted"
    assert not [r for r in cloud.requests if r["table"] == "rpc"]
    assert [w["method"] for w in cloud.observation_writes()] == ["POST"]
    assert _local(local_id)["cloud_id"] is None


def test_adoption_with_a_different_uploaded_location_pair_is_a_conflict(two_desktops, monkeypatch):
    cloud = FakePostgrest()
    local_id = _create(
        gps_latitude=61.0, gps_longitude=9.0,
        location_source="image_exif", location_status="acquired",
    )
    _copy_database(two_desktops["A"], two_desktops["B"])
    _push(cloud.client())
    row = cloud.rows_for()[0]
    assert (row["location_source"], row["location_status"]) == ("image_exif", "acquired")
    cloud.edit_elsewhere(row["id"], location_source="manual")
    expected = cloud.rows_for()

    two_desktops["B"].activate(monkeypatch)
    result = _push(cloud.client())
    assert any("needs review" in str(error) for error in result["errors"])
    assert cloud.rows_for() == expected
    assert _local(local_id)["cloud_id"] is None


def test_adoption_with_the_same_location_pair_links(two_desktops, monkeypatch):
    cloud = FakePostgrest()
    local_id = _create(
        gps_latitude=61.0, gps_longitude=9.0,
        location_source="image_exif", location_status="acquired",
    )
    _copy_database(two_desktops["A"], two_desktops["B"])
    _push(cloud.client())

    two_desktops["B"].activate(monkeypatch)
    assert _push(cloud.client())["errors"] == []
    assert str(_local(local_id)["cloud_id"]) == str(cloud.rows_for()[0]["id"])


# ── The refusal stops every later upload, not only row POST/PATCH ─────────


def test_refusal_stops_writing_rpcs_but_not_read_rpcs(two_desktops):
    cloud = FakePostgrest()
    client = cloud.client()
    client._latch_upload_refusal("sporely_upgrade_required", "blocked")
    sent = len(cloud.requests)
    for call in (
        lambda: client.set_observation_selected_taxon("1001", 7),
        lambda: client._rpc("set_observation_location", {}),
        lambda: client._rpc("sync_reference_work", {}),
        lambda: client._rpc("an_rpc_added_later", {}),
    ):
        with pytest.raises(cloud_sync.CloudUploadRefusedError):
            call()
    assert len(cloud.requests) == sent, "no refused write reached the server"
    client._rpc("list_reference_library_feed", {})
    assert len(cloud.requests) == sent + 1, "read RPCs keep downloads working"


def test_refusal_stops_media_uploads_and_deletes_but_not_downloads(tmp_path):
    client = cloud_sync.SporelyCloudClient("token", USER_ID)
    worker = client._get_media_worker()
    sent = []

    def request(method, url, **kwargs):
        sent.append(method)
        return _FakeResponse(200, {"key": "u/x/a.webp"})

    worker._session.request = request
    client._latch_upload_refusal("sporely_upgrade_required", "blocked")
    source = tmp_path / "a.webp"
    source.write_bytes(b"bytes")
    with pytest.raises(cloud_sync.CloudUploadRefusedError):
        worker.put_file(source, "u/x/a.webp", content_type="image/webp")
    with pytest.raises(cloud_sync.CloudUploadRefusedError):
        worker.delete_object("u/x/a.webp")
    assert sent == []
    worker.download_to_file("u/x/a.webp", tmp_path / "out.webp")
    assert sent == ["GET"]


def test_calibration_push_stops_on_a_refusal_swallowed_by_an_earlier_step(two_desktops, monkeypatch):
    from utils.cloud_sync_impl import calibrations

    cloud = FakePostgrest()
    client = cloud.client()
    rows = [
        {"calibration_uuid": str(uuid.uuid4()), "objective_name": f"obj {i}"} for i in range(3)
    ]
    monkeypatch.setattr(calibrations, "_load_local_calibration_rows", lambda: rows)
    visited = []

    def find_remote(calibration_uuid):
        visited.append(calibration_uuid)
        # An earlier step refused and reported instead of raising.
        client._latch_upload_refusal("sporely_upgrade_required", "blocked")
        return None

    monkeypatch.setattr(client, "find_remote_calibration", find_remote)
    with pytest.raises(cloud_sync.CloudUploadRefusedError):
        calibrations.push_calibrations(client, remote_calibrations=[])
    assert len(visited) <= 1


# ── Interrupted creates with a proven selection; the adoption baseline ────


def _rpcs(cloud: FakePostgrest) -> list[str]:
    return [r["path"] for r in cloud.requests if r["table"] == "rpc"]


def test_kill_after_insert_with_a_proven_selection_adopts_then_sends_it_as_a_local_change(two_desktops, monkeypatch):
    cloud = FakePostgrest()
    local_id = _create()
    _make_proven(local_id)
    monkeypatch.setenv(fault_injection.SYNC_FAULT_ENV_VAR, "after_insert")
    with pytest.raises(_Killed):
        _push(cloud.client())
    assert cloud.rows_for()[0].get("selected_sporely_taxon_id") is None, "killed before the identity step"

    # Retry: adopted with no write at all; the selection stays pending.
    monkeypatch.delenv(fault_injection.SYNC_FAULT_ENV_VAR)
    cloud.requests.clear()
    assert _push(cloud.client())["errors"] == []
    rows = cloud.rows_for()
    assert len(rows) == 1 and str(_local(local_id)["cloud_id"]) == str(rows[0]["id"])
    assert [w["method"] for w in cloud.observation_writes()] == ["POST"], "never a PATCH"
    assert _rpcs(cloud) == [], "adoption never writes the selection"
    assert _local(local_id)["sync_status"] == "dirty"

    # Next push: the ordinary local change, checked against the adopted base.
    cloud.requests.clear()
    assert _push(cloud.client())["errors"] == []
    assert cloud.rows_for()[0]["selected_sporely_taxon_id"] == 4242
    assert _rpcs(cloud) == ["rpc/set_observation_selected_taxon_v2"]

    cloud.requests.clear()
    assert _push(cloud.client())["errors"] == []
    assert cloud.observation_writes() == [] and _rpcs(cloud) == []


def test_timeout_after_insert_with_a_proven_selection_adopts_then_sends_it(two_desktops):
    cloud = FakePostgrest()
    local_id = _create()
    _make_proven(local_id)
    cloud.timeout_after_next_insert = True
    assert _push(cloud.client())["errors"] == []
    assert len(cloud.rows_for()) == 1
    assert str(_local(local_id)["cloud_id"]) == str(cloud.rows_for()[0]["id"])
    assert _push(cloud.client())["errors"] == []
    assert cloud.rows_for()[0]["selected_sporely_taxon_id"] == 4242


def test_a_selection_made_after_the_adoption_read_is_never_overwritten(two_desktops, monkeypatch):
    cloud = FakePostgrest()
    local_id = _create()
    _make_proven(local_id)
    monkeypatch.setenv(fault_injection.SYNC_FAULT_ENV_VAR, "after_insert")
    with pytest.raises(_Killed):
        _push(cloud.client())
    monkeypatch.delenv(fault_injection.SYNC_FAULT_ENV_VAR)
    cloud_id = cloud.rows_for()[0]["id"]

    def select_elsewhere_after_adoption_read(filters):
        if any(column == "sync_key" for column, _ in filters):
            cloud.after_get = None
            cloud.edit_elsewhere(cloud_id, selected_sporely_taxon_id=77)  # web picks 77

    cloud.after_get = select_elsewhere_after_adoption_read
    cloud.requests.clear()
    _push(cloud.client())
    assert cloud.rows_for()[0]["selected_sporely_taxon_id"] == 77
    assert _rpcs(cloud) == []

    for _attempt in range(2):
        cloud.requests.clear()
        result = _push(cloud.client())
        assert any("needs review" in str(error) for error in result["errors"]), result["errors"]
        assert cloud.rows_for()[0]["selected_sporely_taxon_id"] == 77, "the competing selection survives"
        assert _rpcs(cloud) == []
        assert not [w for w in cloud.observation_writes() if w["method"] == "PATCH"]


def test_an_untouched_row_with_a_different_selection_is_still_a_conflict(two_desktops, monkeypatch):
    cloud = FakePostgrest()
    local_id = _create()
    _make_proven(local_id)
    monkeypatch.setenv(fault_injection.SYNC_FAULT_ENV_VAR, "after_insert")
    with pytest.raises(_Killed):
        _push(cloud.client())
    monkeypatch.delenv(fault_injection.SYNC_FAULT_ENV_VAR)
    row = cloud.rows_for()[0]
    for stored in cloud.rows:  # a different selection with the timestamps untouched
        stored["selected_sporely_taxon_id"] = 77
    result = _push(cloud.client())
    assert any("needs review" in str(error) for error in result["errors"])
    assert cloud.rows_for()[0]["selected_sporely_taxon_id"] == 77
    assert _local(local_id)["cloud_id"] is None and row["id"]


def test_adoption_keeps_the_fetched_row_as_baseline_and_pulls_a_later_edit(two_desktops, monkeypatch):
    cloud = FakePostgrest()
    local_id = _create(notes="original")
    _copy_database(two_desktops["A"], two_desktops["B"])
    _push(cloud.client())
    cloud_id = cloud.rows_for()[0]["id"]

    armed = {"edited": False}

    def edit_after_adoption_read(filters):
        if any(column == "sync_key" for column, _ in filters) and not armed["edited"]:
            armed["edited"] = True  # a competing edit lands right after the adoption read
            cloud.edit_elsewhere(cloud_id, notes="competing edit")

    cloud.after_get = edit_after_adoption_read
    two_desktops["B"].activate(monkeypatch)
    assert _push(cloud.client())["errors"] == []
    assert armed["edited"]
    cloud.after_get = None
    snapshot = cloud_sync._parse_cloud_observation_snapshot(
        cloud_sync._load_cloud_observation_snapshot(str(cloud_id))
    )
    assert snapshot["observation"]["notes"] == "original", "baseline is the row adoption judged"

    # A later local edit of another field must not overwrite the competing
    # cloud change: against the judged baseline it is a remote-only change.
    ObservationDB.update_observation(local_id, location="edited on B")
    result = _push(cloud.client())
    assert _observation_errors(result) == []
    row = cloud.rows_for()[0]
    assert row["notes"] == "competing edit", "the competing cloud edit survives"
    assert row["location"] == "edited on B"
    assert _local(local_id)["notes"] == "competing edit", "and is adopted locally"

    # A deep pull agrees and writes nothing.
    cloud.requests.clear()
    result = cloud_sync.sync_all(
        cloud.client(), sync_images=False, materialize_remote_images=False, full_pull=True,
    )
    assert _observation_errors(result) == []
    assert cloud.observation_writes() == []


# ── The no-write link is committed crash-safely (restart regressions) ─────


def _crash_around_link_commit(monkeypatch, *, after: bool) -> None:
    """Kill the process just before or just after the local link commit."""
    commit = cloud_sync._commit_linked_observation

    def crashing(*args, **kwargs):
        if not after:
            raise _Killed()
        commit(*args, **kwargs)
        raise _Killed()

    monkeypatch.setattr(cloud_sync, "_commit_linked_observation", crashing)


def _crashed_proven_create(two_desktops, monkeypatch) -> tuple[FakePostgrest, int]:
    cloud = FakePostgrest()
    local_id = _create()
    _make_proven(local_id)
    monkeypatch.setenv(fault_injection.SYNC_FAULT_ENV_VAR, "after_insert")
    with pytest.raises(_Killed):
        _push(cloud.client())
    monkeypatch.delenv(fault_injection.SYNC_FAULT_ENV_VAR)
    return cloud, local_id


def _stored_baseline(cloud_id) -> dict:
    return cloud_sync._parse_cloud_observation_snapshot(
        cloud_sync._load_cloud_observation_snapshot(str(cloud_id))
    ).get("observation") or {}


def test_restart_after_the_link_commit_keeps_the_pending_selection_and_baseline(two_desktops, monkeypatch):
    cloud, local_id = _crashed_proven_create(two_desktops, monkeypatch)
    _crash_around_link_commit(monkeypatch, after=True)
    with pytest.raises(_Killed):
        _push(cloud.client())
    monkeypatch.undo()  # the restarted process: real code, fresh client
    two_desktops["A"].activate(monkeypatch)
    monkeypatch.setattr(cloud_sync, "_sleep_supabase_backoff", lambda attempt: None)

    local = _local(local_id)
    cloud_id = cloud.rows_for()[0]["id"]
    assert str(local["cloud_id"]) == str(cloud_id)
    assert local["sync_status"] == "dirty", "the unfinished selection stays queued"
    assert _stored_baseline(cloud_id).get("id") is not None, "the judged row survived the restart"

    cloud.requests.clear()
    assert _push(cloud.client())["errors"] == []
    rows = cloud.rows_for()
    assert len(rows) == 1 and rows[0]["selected_sporely_taxon_id"] == 4242
    assert _rpcs(cloud) == ["rpc/set_observation_selected_taxon_v2"]
    assert not [w for w in cloud.observation_writes() if w["method"] in {"POST", "PATCH"}]
    assert _local(local_id)["sync_status"] == "synced"


def test_restart_after_the_link_commit_never_overwrites_a_competing_selection(two_desktops, monkeypatch):
    cloud, local_id = _crashed_proven_create(two_desktops, monkeypatch)
    _crash_around_link_commit(monkeypatch, after=True)
    with pytest.raises(_Killed):
        _push(cloud.client())
    monkeypatch.undo()
    two_desktops["A"].activate(monkeypatch)
    monkeypatch.setattr(cloud_sync, "_sleep_supabase_backoff", lambda attempt: None)
    cloud_id = cloud.rows_for()[0]["id"]
    cloud.edit_elsewhere(cloud_id, selected_sporely_taxon_id=77)  # while the desktop was down

    for _attempt in range(2):
        cloud.requests.clear()
        result = _push(cloud.client())
        assert any("needs review" in str(error) for error in result["errors"]), result["errors"]
        assert cloud.rows_for()[0]["selected_sporely_taxon_id"] == 77
        assert _rpcs(cloud) == []
        assert len(cloud.rows_for()) == 1


def test_restart_before_the_link_commit_leaves_the_row_unlinked_and_retry_adopts(two_desktops, monkeypatch):
    cloud, local_id = _crashed_proven_create(two_desktops, monkeypatch)
    _crash_around_link_commit(monkeypatch, after=False)
    with pytest.raises(_Killed):
        _push(cloud.client())
    monkeypatch.undo()
    two_desktops["A"].activate(monkeypatch)
    monkeypatch.setattr(cloud_sync, "_sleep_supabase_backoff", lambda attempt: None)
    cloud_id = cloud.rows_for()[0]["id"]
    assert _local(local_id)["cloud_id"] is None
    assert _stored_baseline(cloud_id) == {}

    assert _push(cloud.client())["errors"] == []
    assert str(_local(local_id)["cloud_id"]) == str(cloud_id)
    assert _local(local_id)["sync_status"] == "dirty"
    assert _push(cloud.client())["errors"] == []
    assert cloud.rows_for()[0]["selected_sporely_taxon_id"] == 4242
    assert len(cloud.rows_for()) == 1


def test_restart_after_a_legacy_link_commit_keeps_link_baseline_and_marker_state(two_desktops, monkeypatch):
    cloud = FakePostgrest()
    local_id = _create(notes="uploaded by v0.9.25 before it crashed")
    _mark_legacy(local_id)
    legacy = cloud.insert_legacy_row(**_legacy_copy(local_id))
    _crash_around_link_commit(monkeypatch, after=True)
    with pytest.raises(_Killed):
        _push(cloud.client())
    monkeypatch.undo()
    two_desktops["A"].activate(monkeypatch)
    monkeypatch.setattr(cloud_sync, "_sleep_supabase_backoff", lambda attempt: None)

    local = _local(local_id)
    assert str(local["cloud_id"]) == str(legacy["id"])
    assert local["legacy_recovery_pending"] == 0 and local["sync_status"] == "synced"
    assert _stored_baseline(legacy["id"]).get("notes") == "uploaded by v0.9.25 before it crashed"
    cloud.requests.clear()
    assert _push(cloud.client())["errors"] == []
    assert cloud.observation_writes() == [] and _rpcs(cloud) == []
    assert cloud.rows_for() == [legacy]


def test_keep_local_adoption_keeps_the_judged_baseline_against_a_competing_selection(two_desktops, monkeypatch):
    cloud, local_id = _crashed_proven_create(two_desktops, monkeypatch)
    cloud_id = cloud.rows_for()[0]["id"]

    def select_elsewhere_after_adoption_read(filters):
        if any(column == "sync_key" for column, _ in filters):
            cloud.after_get = None
            cloud.edit_elsewhere(cloud_id, selected_sporely_taxon_id=77)

    cloud.after_get = select_elsewhere_after_adoption_read
    cloud.requests.clear()
    cloud_sync.resolve_conflict_keep_local(cloud.client(), local_id)
    assert _rpcs(cloud) == [], "keep-local adoption never writes the selection"
    assert cloud.rows_for()[0]["selected_sporely_taxon_id"] == 77
    local = _local(local_id)
    assert str(local["cloud_id"]) == str(cloud_id) and local["sync_status"] == "dirty"
    assert _stored_baseline(cloud_id).get("selected_sporely_taxon_id") is None, (
        "the judged row stays the baseline; the later read never replaces it"
    )

    for _attempt in range(2):
        cloud.requests.clear()
        result = _push(cloud.client())
        assert any("needs review" in str(error) for error in result["errors"]), result["errors"]
        assert cloud.rows_for()[0]["selected_sporely_taxon_id"] == 77
        assert _rpcs(cloud) == []
        assert len(cloud.rows_for()) == 1
