"""Tests for utils.spore_summary_sync (Stage D — cloud sync of structured
observation spore summaries)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from utils.spore_summary import compute_context_hash, build_context
from utils.spore_summary_sync import (
    STATUS_SKIP_NO_CLOUD_ID,
    STATUS_SKIP_TABLE_MISSING,
    STATUS_SYNCED,
    _SUMMARY_UPSERT_FIELDS,
    sync_observation_spore_summaries,
)

FIXED_COMPUTED_AT = datetime(2026, 7, 12, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Fake HTTP client — captures every REST call for assertions.
# ---------------------------------------------------------------------------


class FakeSummaryClient:
    """A minimal duck-typed stand-in for SporelyCloudClient.

    Records every call and lets tests script GET responses and per-call
    exceptions.
    """

    def __init__(self, user_id: str = "user-abc"):
        self.user_id = user_id
        self.calls: list[tuple[str, str, Any]] = []  # (method, path, payload)
        self.get_responses: list[list[dict]] = []    # popped in order
        self.raise_on: list[tuple[str, str, Exception]] = []  # (method, contains, exc)
        self._post_counter = 0

    def _maybe_raise(self, method: str, path: str) -> None:
        for i, (m, needle, exc) in enumerate(self.raise_on):
            if m == method and needle in path:
                self.raise_on.pop(i)
                raise exc

    def _get(self, path: str) -> list:
        self.calls.append(("GET", path, None))
        self._maybe_raise("GET", path)
        if self.get_responses:
            return self.get_responses.pop(0)
        return []

    def _post(self, path: str, payload: Any) -> list:
        self.calls.append(("POST", path, payload))
        self._maybe_raise("POST", path)
        self._post_counter += 1
        # POST returns a list of row(s) with generated ids; the caller in
        # this stage does not use the return value, but keep the shape
        # consistent with SporelyCloudClient._post().
        return [{"id": 1000 + self._post_counter}]

    def _patch(self, path: str, payload: dict) -> None:
        self.calls.append(("PATCH", path, payload))
        self._maybe_raise("PATCH", path)

    def _delete(self, path: str) -> None:
        self.calls.append(("DELETE", path, None))
        self._maybe_raise("DELETE", path)


def _pair(length, width, **image_ctx):
    row = {"length_um": length, "width_um": width, "measurement_type": "manual"}
    row.update(image_ctx)
    return row


def _table_missing_error() -> Exception:
    # Matches the string shape PostgREST returns when the target relation
    # is missing (see _is_missing_table_error).
    return Exception(
        "GET observation_spore_summaries?...: {\"code\":\"PGRST205\","
        "\"message\":\"Could not find the table 'public.observation_spore_summaries'\"}"
    )


# ---------------------------------------------------------------------------
# Remote observation id + user_id
# ---------------------------------------------------------------------------


def test_upsert_payload_uses_remote_observation_id_not_local():
    """Even when compute is misused with the local id, the sync layer
    must stamp the remote observation id into every payload."""
    client = FakeSummaryClient(user_id="user-abc")
    result = sync_observation_spore_summaries(
        client,
        local_observation_id=999,
        remote_observation_id=42,
        user_id="user-abc",
        load_measurements=lambda _: [_pair(10.0, 5.0), _pair(12.0, 6.0)],
        computed_at=FIXED_COMPUTED_AT,
    )
    assert result["status"] == STATUS_SYNCED
    posts = [(p, payload) for method, p, payload in client.calls if method == "POST"]
    assert len(posts) == 1
    _, payload = posts[0]
    assert payload["observation_id"] == 42
    assert payload["observation_id"] != 999
    assert payload["user_id"] == "user-abc"


def test_source_app_version_passes_through():
    client = FakeSummaryClient()
    sync_observation_spore_summaries(
        client,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        source_app_version="0.9.6",
        load_measurements=lambda _: [_pair(10.0, 5.0)],
        computed_at=FIXED_COMPUTED_AT,
    )
    post_payload = next(p for m, _, p in client.calls if m == "POST")
    assert post_payload["source_app"] == "sporely-py"
    assert post_payload["source_app_version"] == "0.9.6"
    assert post_payload["stats_version"] == 1


def test_upsert_payload_contains_every_stage_b_column():
    client = FakeSummaryClient()
    sync_observation_spore_summaries(
        client,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        load_measurements=lambda _: [_pair(10.0, 5.0)],
        computed_at=FIXED_COMPUTED_AT,
    )
    post_payload = next(p for m, _, p in client.calls if m == "POST")
    assert set(post_payload.keys()) == set(_SUMMARY_UPSERT_FIELDS)


# ---------------------------------------------------------------------------
# Insert / update / delete against existing remote state
# ---------------------------------------------------------------------------


def test_no_existing_remote_rows_produces_insert_only():
    client = FakeSummaryClient()
    result = sync_observation_spore_summaries(
        client,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        load_measurements=lambda _: [
            _pair(10.0, 5.0, mount_medium="KOH"),
            _pair(11.0, 5.5, mount_medium="Water"),
        ],
        computed_at=FIXED_COMPUTED_AT,
    )
    posts = [c for c in client.calls if c[0] == "POST"]
    patches = [c for c in client.calls if c[0] == "PATCH"]
    deletes = [c for c in client.calls if c[0] == "DELETE"]
    assert len(posts) == 2
    assert len(patches) == 0
    assert len(deletes) == 0
    assert result == {
        "status": STATUS_SYNCED,
        "inserted": 2,
        "updated": 0,
        "deleted": 0,
        "total_local": 2,
    }


def test_existing_remote_context_is_patched_not_reinserted():
    koh_hash = compute_context_hash(build_context(mount_reagent="koh"))
    client = FakeSummaryClient()
    client.get_responses = [
        [{"id": 555, "context_hash": koh_hash}]
    ]
    result = sync_observation_spore_summaries(
        client,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        load_measurements=lambda _: [_pair(10.0, 5.0, mount_medium="KOH")],
        computed_at=FIXED_COMPUTED_AT,
    )
    assert result["inserted"] == 0
    assert result["updated"] == 1
    assert result["deleted"] == 0
    patches = [c for c in client.calls if c[0] == "PATCH"]
    assert len(patches) == 1
    patch_path, patch_payload = patches[0][1], patches[0][2]
    assert "id=eq.555" in patch_path
    assert patch_payload["context_hash"] == koh_hash


def test_stale_remote_context_is_deleted_after_upsert():
    """A context that used to exist in the cloud but is no longer in the
    local computed set must be DELETEd."""
    koh_hash = compute_context_hash(build_context(mount_reagent="koh"))
    water_hash = compute_context_hash(build_context(mount_reagent="water"))
    client = FakeSummaryClient()
    client.get_responses = [[
        {"id": 111, "context_hash": koh_hash},
        {"id": 222, "context_hash": water_hash},
    ]]
    result = sync_observation_spore_summaries(
        client,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        # Only KOH remains locally; Water context is stale.
        load_measurements=lambda _: [_pair(10.0, 5.0, mount_medium="KOH")],
        computed_at=FIXED_COMPUTED_AT,
    )
    assert result["updated"] == 1
    assert result["inserted"] == 0
    assert result["deleted"] == 1
    delete_paths = [path for m, path, _ in client.calls if m == "DELETE"]
    assert delete_paths == ["observation_spore_summaries?id=eq.222"]


def test_empty_local_summaries_deletes_all_remote_rows():
    """No local measurements -> every remote summary row for this
    observation must be removed."""
    hash_a = "a" * 64
    hash_b = "b" * 64
    client = FakeSummaryClient()
    client.get_responses = [[
        {"id": 10, "context_hash": hash_a},
        {"id": 20, "context_hash": hash_b},
    ]]
    result = sync_observation_spore_summaries(
        client,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        load_measurements=lambda _: [],
        computed_at=FIXED_COMPUTED_AT,
    )
    assert result["total_local"] == 0
    assert result["deleted"] == 2
    assert result["inserted"] == 0
    assert result["updated"] == 0
    delete_paths = sorted(path for m, path, _ in client.calls if m == "DELETE")
    assert delete_paths == [
        "observation_spore_summaries?id=eq.10",
        "observation_spore_summaries?id=eq.20",
    ]


# ---------------------------------------------------------------------------
# Multiple contexts / null-context row
# ---------------------------------------------------------------------------


def test_multiple_contexts_produce_multiple_upsert_rows():
    client = FakeSummaryClient()
    result = sync_observation_spore_summaries(
        client,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        load_measurements=lambda _: [
            _pair(10.0, 5.0, mount_medium="KOH", contrast="DIC"),
            _pair(11.0, 5.5, mount_medium="Water", contrast="BF"),
            _pair(12.0, 6.0, mount_medium="Water", contrast="BF"),
        ],
        computed_at=FIXED_COMPUTED_AT,
    )
    assert result["inserted"] == 2
    posts = [payload for m, _, payload in client.calls if m == "POST"]
    contexts = {(p["mount_reagent"], p["contrast_method"]) for p in posts}
    assert contexts == {("koh", "dic"), ("water", "bf")}


def test_null_context_row_syncs_correctly():
    client = FakeSummaryClient()
    result = sync_observation_spore_summaries(
        client,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        load_measurements=lambda _: [_pair(10.0, 5.0), _pair(12.0, 6.0)],
        computed_at=FIXED_COMPUTED_AT,
    )
    assert result["inserted"] == 1
    payload = next(p for m, _, p in client.calls if m == "POST")
    assert payload["mount_reagent"] is None
    assert payload["stain_reagent"] is None
    assert payload["contrast_method"] is None
    assert payload["sample_type"] is None
    assert payload["measurement_type"] == "spore"
    assert payload["context_hash"] == compute_context_hash(build_context())


# ---------------------------------------------------------------------------
# Missing table compatibility skip
# ---------------------------------------------------------------------------


def test_missing_table_error_on_get_returns_skip_status():
    client = FakeSummaryClient()
    client.raise_on = [("GET", "observation_spore_summaries", _table_missing_error())]
    result = sync_observation_spore_summaries(
        client,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        load_measurements=lambda _: [_pair(10.0, 5.0)],
        computed_at=FIXED_COMPUTED_AT,
    )
    assert result["status"] == STATUS_SKIP_TABLE_MISSING
    # No POST/PATCH/DELETE should have run.
    assert not any(c[0] in ("POST", "PATCH", "DELETE") for c in client.calls)


def test_missing_table_error_on_post_returns_skip_status():
    client = FakeSummaryClient()
    client.raise_on = [("POST", "observation_spore_summaries", _table_missing_error())]
    result = sync_observation_spore_summaries(
        client,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        load_measurements=lambda _: [_pair(10.0, 5.0)],
        computed_at=FIXED_COMPUTED_AT,
    )
    assert result["status"] == STATUS_SKIP_TABLE_MISSING


def test_unrelated_supabase_error_propagates():
    class FakeAuthError(Exception):
        pass

    client = FakeSummaryClient()
    client.raise_on = [("GET", "observation_spore_summaries", FakeAuthError("401 unauthorized"))]
    with pytest.raises(FakeAuthError):
        sync_observation_spore_summaries(
            client,
            local_observation_id=1,
            remote_observation_id=7,
            user_id="user-abc",
            load_measurements=lambda _: [_pair(10.0, 5.0)],
            computed_at=FIXED_COMPUTED_AT,
        )


# ---------------------------------------------------------------------------
# Idempotence
# ---------------------------------------------------------------------------


def test_sync_is_idempotent_across_two_runs():
    """After the first run inserts, a second run with the same input and
    the corresponding remote state must produce only PATCHes, never new
    INSERTs, and never spurious DELETEs."""
    load = lambda _: [_pair(10.0, 5.0), _pair(11.0, 5.5)]

    # First run: no existing remote rows.
    client_a = FakeSummaryClient()
    result_a = sync_observation_spore_summaries(
        client_a,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        load_measurements=load,
        computed_at=FIXED_COMPUTED_AT,
    )
    assert result_a["inserted"] == 1
    assert result_a["updated"] == 0
    assert result_a["deleted"] == 0

    inserted_payload = next(p for m, _, p in client_a.calls if m == "POST")
    inserted_hash = inserted_payload["context_hash"]

    # Second run: remote already has that context (with an id assigned).
    client_b = FakeSummaryClient()
    client_b.get_responses = [[{"id": 999, "context_hash": inserted_hash}]]
    result_b = sync_observation_spore_summaries(
        client_b,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        load_measurements=load,
        computed_at=FIXED_COMPUTED_AT,
    )
    assert result_b["inserted"] == 0
    assert result_b["updated"] == 1
    assert result_b["deleted"] == 0
    assert not any(m == "POST" for m, _, _ in client_b.calls)


# ---------------------------------------------------------------------------
# Edge cases: bad ids
# ---------------------------------------------------------------------------


def test_missing_remote_observation_id_returns_skip():
    client = FakeSummaryClient()
    result = sync_observation_spore_summaries(
        client,
        local_observation_id=1,
        remote_observation_id="",
        user_id="user-abc",
        load_measurements=lambda _: [_pair(10.0, 5.0)],
        computed_at=FIXED_COMPUTED_AT,
    )
    assert result["status"] == STATUS_SKIP_NO_CLOUD_ID
    assert client.calls == []


def test_missing_user_id_returns_skip():
    client = FakeSummaryClient(user_id="")
    result = sync_observation_spore_summaries(
        client,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="",
        load_measurements=lambda _: [_pair(10.0, 5.0)],
        computed_at=FIXED_COMPUTED_AT,
    )
    assert result["status"] == STATUS_SKIP_NO_CLOUD_ID
    assert client.calls == []


# ---------------------------------------------------------------------------
# Call-site behavior — the cloud_sync.py wrapper.
#
# These tests exercise `_push_summary_for_current_observation`, the tiny
# helper that lives in cloud_sync.py and wraps sync_observation_spore_
# summaries with error surfacing. They intentionally patch the imported
# sync function so no HTTP work happens, and use monkeypatch instead of
# hitting the real load_measurements / SporelyCloudClient.
# ---------------------------------------------------------------------------


class _FakeAuthError(Exception):
    """Used to test that auth errors re-raise through the helper."""


class _FakeTemporaryError(Exception):
    """Used to test that temporary-unavailable errors re-raise."""


def _install_cloud_sync_error_predicates(monkeypatch):
    """Wire predicates so _FakeAuthError/_FakeTemporaryError are treated
    as auth / temporary errors by _push_summary_for_current_observation."""
    from utils import cloud_sync as cs

    monkeypatch.setattr(
        cs, "is_cloud_auth_error",
        lambda exc: isinstance(exc, _FakeAuthError),
    )
    monkeypatch.setattr(
        cs, "is_cloud_temporary_unavailable_error",
        lambda exc: isinstance(exc, _FakeTemporaryError),
    )


class _CallSiteFakeClient:
    """Minimal stand-in for SporelyCloudClient at the call site."""

    def __init__(self, user_id="user-xyz"):
        self.user_id = user_id


def test_call_site_missing_table_is_soft_no_error_recorded(monkeypatch):
    """STATUS_SKIP_TABLE_MISSING must NOT add to `errors`."""
    from utils import cloud_sync as cs

    _install_cloud_sync_error_predicates(monkeypatch)
    monkeypatch.setattr(
        cs, "sync_observation_spore_summaries",
        lambda *a, **kw: {
            "status": STATUS_SKIP_TABLE_MISSING,
            "inserted": 0, "updated": 0, "deleted": 0, "total_local": 0,
        },
    )

    errors: list[str] = []
    result = cs._push_summary_for_current_observation(
        _CallSiteFakeClient(),
        obs={"id": 17},
        local_obs_id=17,
        cloud_id=42,
        errors=errors,
    )
    assert result["status"] == STATUS_SKIP_TABLE_MISSING
    assert errors == []


def test_call_site_unexpected_error_recorded_in_errors_list(monkeypatch):
    """A non-auth / non-temporary / non-missing-table error must be
    appended to `errors` so `sync_all` surfaces it in its result dict,
    and the observation must be marked dirty for retry."""
    from utils import cloud_sync as cs

    _install_cloud_sync_error_predicates(monkeypatch)

    def _raise(*a, **kw):
        raise RuntimeError("boom: RLS denied")
    monkeypatch.setattr(cs, "sync_observation_spore_summaries", _raise)

    dirty_calls: list[int] = []
    monkeypatch.setattr(
        cs, "mark_observation_sync_dirty",
        lambda obs_id: dirty_calls.append(obs_id),
    )

    errors: list[str] = []
    result = cs._push_summary_for_current_observation(
        _CallSiteFakeClient(),
        obs={"id": 17},
        local_obs_id=17,
        cloud_id=42,
        errors=errors,
    )
    assert result is None
    assert len(errors) == 1
    assert "obs 17" in errors[0]
    assert "spore summary sync failed" in errors[0]
    assert "boom: RLS denied" in errors[0]
    assert dirty_calls == [17]


def test_call_site_auth_error_re_raises(monkeypatch):
    """Auth errors must propagate so the outer sync loop can abort."""
    from utils import cloud_sync as cs

    _install_cloud_sync_error_predicates(monkeypatch)

    def _raise(*a, **kw):
        raise _FakeAuthError("401")
    monkeypatch.setattr(cs, "sync_observation_spore_summaries", _raise)

    errors: list[str] = []
    with pytest.raises(_FakeAuthError):
        cs._push_summary_for_current_observation(
            _CallSiteFakeClient(),
            obs={"id": 17},
            local_obs_id=17,
            cloud_id=42,
            errors=errors,
        )
    # Auth errors must NOT be recorded in `errors` (they abort the whole
    # sync via re-raise). Same convention as CloudSyncError handling in
    # the surrounding loop.
    assert errors == []


def test_call_site_temporary_error_re_raises(monkeypatch):
    """Temporary-unavailable errors must propagate."""
    from utils import cloud_sync as cs

    _install_cloud_sync_error_predicates(monkeypatch)

    def _raise(*a, **kw):
        raise _FakeTemporaryError("503")
    monkeypatch.setattr(cs, "sync_observation_spore_summaries", _raise)

    with pytest.raises(_FakeTemporaryError):
        cs._push_summary_for_current_observation(
            _CallSiteFakeClient(),
            obs={"id": 17},
            local_obs_id=17,
            cloud_id=42,
            errors=[],
        )


def test_call_site_source_app_version_from_module_slot(monkeypatch):
    """The helper must pass whatever `_current_source_app_version()`
    returns; setting the slot via `set_cloud_sync_source_app_version`
    must propagate to the sync call."""
    from utils import cloud_sync as cs

    _install_cloud_sync_error_predicates(monkeypatch)

    seen_kwargs: dict = {}

    def _capture(client_arg, **kw):
        seen_kwargs.update(kw)
        return {
            "status": STATUS_SYNCED,
            "inserted": 0, "updated": 0, "deleted": 0, "total_local": 0,
        }
    monkeypatch.setattr(cs, "sync_observation_spore_summaries", _capture)

    cs.set_cloud_sync_source_app_version("9.9.9")
    try:
        cs._push_summary_for_current_observation(
            _CallSiteFakeClient(user_id="uid-1"),
            obs={"id": 1},
            local_obs_id=1,
            cloud_id=100,
            errors=[],
        )
    finally:
        cs.set_cloud_sync_source_app_version(None)

    assert seen_kwargs["local_observation_id"] == 1
    assert seen_kwargs["remote_observation_id"] == 100
    assert seen_kwargs["user_id"] == "uid-1"
    assert seen_kwargs["source_app_version"] == "9.9.9"


def test_call_site_summary_sync_runs_once_per_call(monkeypatch):
    """One invocation of the helper produces exactly one call to the
    underlying sync function — the helper does not loop or retry
    internally. `sync_all`'s per-observation single-call guarantee is
    upheld by not calling this helper twice; that's a separate concern
    verified by the placement in cloud_sync.py (outside `if sync_images`,
    before `_store_remote_snapshot`)."""
    from utils import cloud_sync as cs

    _install_cloud_sync_error_predicates(monkeypatch)

    call_count = {"n": 0}

    def _one_call(*a, **kw):
        call_count["n"] += 1
        return {
            "status": STATUS_SYNCED,
            "inserted": 1, "updated": 0, "deleted": 0, "total_local": 1,
        }
    monkeypatch.setattr(cs, "sync_observation_spore_summaries", _one_call)

    cs._push_summary_for_current_observation(
        _CallSiteFakeClient(),
        obs={"id": 1},
        local_obs_id=1,
        cloud_id=100,
        errors=[],
    )
    assert call_count["n"] == 1


# ---------------------------------------------------------------------------
# Reconciliation pass — the fix for the "0 measured summaries" regression.
#
# push_all only iterates dirty observations, so observations that were
# synced pre-Stage-D and are clean/no-op never see the summary writer.
# `_reconcile_missing_spore_summaries` runs once at the end of push_all
# and backfills any observation that has local measurements but no
# remote summary row yet.
# ---------------------------------------------------------------------------


class _ReconcileFakeClient:
    """A fake `SporelyCloudClient` for reconciliation tests. Tracks the
    bulk GET call and any per-observation writes."""

    def __init__(self, user_id: str = "user-recon"):
        self.user_id = user_id
        self.calls: list[tuple[str, str, Any]] = []
        # Rows returned by the bulk `?user_id=eq.<uid>&select=observation_id`
        # GET. Tests overwrite this to simulate different remote coverage.
        self.remote_summary_observation_ids: list[Any] = []

    def _get(self, path: str) -> list:
        self.calls.append(("GET", path, None))
        if path.startswith("observation_spore_summaries?user_id=eq."):
            return [
                {"observation_id": oid}
                for oid in self.remote_summary_observation_ids
            ]
        return []


def _install_reconcile_cloud_sync_stubs(monkeypatch, local_measurements_by_obs):
    """Patch cloud_sync so `_reconcile_missing_spore_summaries` can run
    against an in-memory local dataset and a no-op push helper.

    `local_measurements_by_obs` maps local observation id -> list of
    already-camelCase-mapped measurement dicts (as returned by
    `load_measurements_with_context`).
    """
    from utils import cloud_sync as cs

    monkeypatch.setattr(
        cs, "is_cloud_auth_error", lambda exc: False,
    )
    monkeypatch.setattr(
        cs, "is_cloud_temporary_unavailable_error", lambda exc: False,
    )

    # Stub the local SQLite query. `get_connection()` is used inside the
    # reconcile helper to fetch (local_id, cloud_id) pairs; we replace it
    # with a tiny sqlite in-memory DB seeded with the test fixture.
    import sqlite3
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE observations (id INTEGER, cloud_id TEXT);
        CREATE TABLE images (id INTEGER, observation_id INTEGER);
        CREATE TABLE spore_measurements (id INTEGER, image_id INTEGER);
        """
    )
    image_seq = 1
    meas_seq = 1
    for obs_id, (cloud_id, has_measurements) in local_measurements_by_obs.items():
        cur.execute("INSERT INTO observations VALUES (?, ?)", (obs_id, cloud_id))
        if has_measurements:
            cur.execute("INSERT INTO images VALUES (?, ?)", (image_seq, obs_id))
            cur.execute(
                "INSERT INTO spore_measurements VALUES (?, ?)",
                (meas_seq, image_seq),
            )
            image_seq += 1
            meas_seq += 1
    conn.commit()
    monkeypatch.setattr(cs, "get_connection", lambda: conn)
    return conn


def test_reconcile_backfills_synced_observation_missing_summary(monkeypatch):
    """The scenario from the field: an observation was synced pre-Stage-D
    (cloud_id set, sync_status='synced', not dirty) and has local paired
    spore measurements. The main push loop skips it. Reconciliation must
    still populate its remote summary row."""
    from utils import cloud_sync as cs

    _install_reconcile_cloud_sync_stubs(
        monkeypatch,
        local_measurements_by_obs={
            42: ("cloud-obs-631", True),   # missing remotely → should backfill
        },
    )
    writer_calls: list[dict] = []
    def _capture(client, *, obs, local_obs_id, cloud_id, errors):
        writer_calls.append({
            "obs": obs, "local_obs_id": local_obs_id, "cloud_id": cloud_id,
        })
        return None
    monkeypatch.setattr(cs, "_push_summary_for_current_observation", _capture)

    client = _ReconcileFakeClient()
    client.remote_summary_observation_ids = []  # nothing covered remotely yet.
    errors: list[str] = []
    counters = cs._reconcile_missing_spore_summaries(client, errors)

    assert errors == []
    assert counters == {"candidates": 1, "attempted": 1}
    assert len(writer_calls) == 1
    assert writer_calls[0]["local_obs_id"] == 42
    assert writer_calls[0]["cloud_id"] == "cloud-obs-631"


def test_reconcile_skips_observations_already_covered_remotely(monkeypatch):
    """Idempotence: once summary rows exist for an observation, the
    reconciliation pass must not re-invoke the writer for it."""
    from utils import cloud_sync as cs

    _install_reconcile_cloud_sync_stubs(
        monkeypatch,
        local_measurements_by_obs={
            42: ("cloud-obs-631", True),   # already covered
            43: ("cloud-obs-732", True),   # missing → should backfill
        },
    )
    writer_calls: list[dict] = []
    monkeypatch.setattr(
        cs, "_push_summary_for_current_observation",
        lambda client, *, obs, local_obs_id, cloud_id, errors: writer_calls.append(
            {"local_obs_id": local_obs_id, "cloud_id": cloud_id},
        ),
    )

    client = _ReconcileFakeClient()
    client.remote_summary_observation_ids = ["cloud-obs-631"]  # 631 covered.

    counters = cs._reconcile_missing_spore_summaries(client, [])
    assert counters == {"candidates": 2, "attempted": 1}
    assert len(writer_calls) == 1
    assert writer_calls[0]["cloud_id"] == "cloud-obs-732"


def test_reconcile_ignores_observations_without_local_measurements(monkeypatch):
    """An observation with a cloud_id but no local spore_measurements
    must not appear as a reconciliation candidate — nothing to compute."""
    from utils import cloud_sync as cs

    _install_reconcile_cloud_sync_stubs(
        monkeypatch,
        local_measurements_by_obs={
            42: ("cloud-obs-631", False),   # no measurements → skip.
        },
    )
    writer_calls: list[dict] = []
    monkeypatch.setattr(
        cs, "_push_summary_for_current_observation",
        lambda client, *, obs, local_obs_id, cloud_id, errors: writer_calls.append(
            {"local_obs_id": local_obs_id, "cloud_id": cloud_id},
        ),
    )
    counters = cs._reconcile_missing_spore_summaries(_ReconcileFakeClient(), [])
    assert counters == {"candidates": 0, "attempted": 0}
    assert writer_calls == []


def test_reconcile_ignores_observations_without_cloud_id(monkeypatch):
    """A never-synced observation (cloud_id NULL / empty) is not this
    pass's responsibility — the main push loop will visit it because
    `cloud_id IS NULL` matches the dirty-loop WHERE clause."""
    from utils import cloud_sync as cs

    _install_reconcile_cloud_sync_stubs(
        monkeypatch,
        local_measurements_by_obs={
            42: ("", True),         # empty cloud_id — never synced.
            43: (None, True),       # NULL cloud_id — never synced.
        },
    )
    writer_calls: list[dict] = []
    monkeypatch.setattr(
        cs, "_push_summary_for_current_observation",
        lambda client, *, obs, local_obs_id, cloud_id, errors: writer_calls.append(
            {"local_obs_id": local_obs_id, "cloud_id": cloud_id},
        ),
    )
    counters = cs._reconcile_missing_spore_summaries(_ReconcileFakeClient(), [])
    assert counters["attempted"] == 0
    assert writer_calls == []


def test_reconcile_soft_skips_when_summary_table_missing(monkeypatch):
    """Older cloud deployments predating Stage B lack the table. Bulk
    GET fails with the missing-table shape; reconciliation must log
    and return without touching `errors`."""
    from utils import cloud_sync as cs

    _install_reconcile_cloud_sync_stubs(
        monkeypatch,
        local_measurements_by_obs={
            42: ("cloud-obs-631", True),
        },
    )
    class _MissingTableClient(_ReconcileFakeClient):
        def _get(self, path):
            raise Exception(
                "GET observation_spore_summaries?...: "
                "PGRST205 Could not find the table 'public.observation_spore_summaries'"
            )
    writer_calls: list[dict] = []
    monkeypatch.setattr(
        cs, "_push_summary_for_current_observation",
        lambda client, *, obs, local_obs_id, cloud_id, errors: writer_calls.append(
            {"local_obs_id": local_obs_id, "cloud_id": cloud_id},
        ),
    )

    errors: list[str] = []
    counters = cs._reconcile_missing_spore_summaries(_MissingTableClient(), errors)
    assert counters == {"candidates": 0, "attempted": 0}
    assert errors == []
    assert writer_calls == []


def test_reconcile_records_unrelated_bulk_error_and_does_not_abort(monkeypatch):
    """A non-auth, non-temporary, non-missing-table error from the bulk
    GET must be recorded in `errors` for the outer sync to surface, but
    must not raise and must not attempt any per-observation writes."""
    from utils import cloud_sync as cs

    _install_reconcile_cloud_sync_stubs(
        monkeypatch,
        local_measurements_by_obs={
            42: ("cloud-obs-631", True),
        },
    )
    class _BadRequestClient(_ReconcileFakeClient):
        def _get(self, path):
            raise Exception("400 Bad Request: something else")
    writer_calls: list[dict] = []
    monkeypatch.setattr(
        cs, "_push_summary_for_current_observation",
        lambda client, *, obs, local_obs_id, cloud_id, errors: writer_calls.append(
            {"local_obs_id": local_obs_id, "cloud_id": cloud_id},
        ),
    )

    errors: list[str] = []
    counters = cs._reconcile_missing_spore_summaries(_BadRequestClient(), errors)
    assert counters == {"candidates": 0, "attempted": 0}
    assert len(errors) == 1
    assert "spore summary reconciliation" in errors[0]
    assert writer_calls == []

