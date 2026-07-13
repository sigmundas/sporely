"""Tests for utils.spore_summary_sync (Stage D — cloud sync of structured
observation spore summaries)."""

from __future__ import annotations

import math
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
        "unchanged": 0,
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


def test_sync_is_fully_idempotent_when_remote_matches_computed_payload():
    """True idempotence: after the first run inserts, a second run with
    the same input AND a remote row already carrying every material
    column from the first run's payload MUST produce zero POSTs, zero
    PATCHes, zero DELETEs. Only the initial GET is spent."""
    load = lambda _: [_pair(10.0, 5.0), _pair(11.0, 5.5)]

    # First run: no existing remote rows -> one INSERT.
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
    assert result_a["unchanged"] == 0
    assert result_a["deleted"] == 0
    inserted_payload = next(p for m, _, p in client_a.calls if m == "POST")

    # Second run: remote already has the row with every material column
    # matching what the writer will compute. Assign an id and keep the
    # first run's computed_at — a stable timestamp we can also assert
    # does NOT move.
    stable_computed_at = "2026-07-01T00:00:00+00:00"
    remote_row = {"id": 999, **inserted_payload, "computed_at": stable_computed_at}

    client_b = FakeSummaryClient()
    client_b.get_responses = [[remote_row]]
    result_b = sync_observation_spore_summaries(
        client_b,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        load_measurements=load,
        computed_at=FIXED_COMPUTED_AT,
    )
    assert result_b["inserted"] == 0
    assert result_b["updated"] == 0
    assert result_b["unchanged"] == 1
    assert result_b["deleted"] == 0
    assert not any(m in ("POST", "PATCH", "DELETE") for m, _, _ in client_b.calls)


def test_sync_patches_when_material_column_differs():
    """When the remote row's material data drifts from the local
    computed payload (e.g. after a writer semantic change bumped
    `stats_version`), the writer MUST PATCH rather than leave stale
    data in place. Tests the material-comparison decision."""
    load = lambda _: [_pair(10.0, 5.0), _pair(11.0, 5.5)]

    # First run to build the reference payload.
    client_seed = FakeSummaryClient()
    sync_observation_spore_summaries(
        client_seed,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        load_measurements=load,
        computed_at=FIXED_COMPUTED_AT,
    )
    seed_payload = next(p for m, _, p in client_seed.calls if m == "POST")

    # Remote row differs by ONE material field (stats_version bumped).
    remote_row = {"id": 999, **seed_payload, "stats_version": 42}
    client_b = FakeSummaryClient()
    client_b.get_responses = [[remote_row]]
    result_b = sync_observation_spore_summaries(
        client_b,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        load_measurements=load,
        computed_at=FIXED_COMPUTED_AT,
    )
    assert result_b["updated"] == 1
    assert result_b["unchanged"] == 0
    assert result_b["inserted"] == 0
    patch_paths = [path for m, path, _ in client_b.calls if m == "PATCH"]
    assert patch_paths == ["observation_spore_summaries?id=eq.999"]


def test_float_material_field_equal_absorbs_postgrest_roundtrip_noise():
    """Real values captured from `SPORE_SUMMARY_DIFF_DEBUG=1` in the
    field. PG double-precision <-> JSON round-trip drops or gains a
    trailing bit at ~1e-15 magnitude. Material comparison MUST return
    True for these — they are not measurement changes, just IEEE-754
    formatting noise."""
    from utils.spore_summary_sync import _material_field_equal

    # length_min_um regression case.
    assert _material_field_equal(
        "length_min_um",
        4.63386615453041,       # remote (PostgREST-returned)
        4.633866154530411,      # payload (locally-computed)
    ) is True

    # length_p05_um regression case.
    assert _material_field_equal(
        "length_p05_um",
        6.0911206428518,        # remote
        6.091120642851802,      # payload
    ) is True

    # q_min regression case.
    assert _material_field_equal(
        "q_min",
        1.62274339104208,       # remote
        1.622743391042079,      # payload
    ) is True

    # Symmetric — the reverse order still matches.
    assert _material_field_equal(
        "length_p05_um", 6.091120642851802, 6.0911206428518,
    ) is True


def test_float_material_field_equal_still_patches_meaningful_drift():
    """The tolerance is 1e-12 — well below biological/display
    precision. Any real change (µm to the 4th decimal, Q to the 3rd)
    is still a mismatch that MUST trigger a PATCH."""
    from utils.spore_summary_sync import _material_field_equal

    # 1e-5 drift on a length mean — well above tolerance.
    assert _material_field_equal(
        "length_mean_um", 10.0, 10.00001,
    ) is False

    # 1e-3 drift on Q — well above tolerance.
    assert _material_field_equal(
        "q_mean", 2.0, 2.001,
    ) is False

    # Zero versus tolerance boundary: 1e-11 IS above the 1e-12
    # absolute tolerance, so it must patch.
    assert _material_field_equal(
        "width_mean_um", 0.0, 1e-11,
    ) is False


def test_float_material_field_equal_edge_cases():
    """None handling and NaN handling. NaN in both sides is treated as
    equal for material-diff purposes — a stored NaN column should not
    trigger a PATCH on every sync just because IEEE-754 says NaN != NaN."""
    from utils.spore_summary_sync import _material_field_equal
    import math

    assert _material_field_equal("length_mean_um", None, None) is True
    assert _material_field_equal("length_mean_um", None, 1.0) is False
    assert _material_field_equal("length_mean_um", 1.0, None) is False
    assert _material_field_equal("length_mean_um", float("nan"), float("nan")) is True
    assert _material_field_equal("length_mean_um", float("nan"), 1.0) is False
    assert _material_field_equal("length_mean_um", 1.0, float("nan")) is False


def test_sync_skips_patch_when_remote_differs_only_by_float_roundtrip_noise():
    """End-to-end: build the exact scenario that produced the field
    regression. Local writer computes `length_min_um = 4.633866154530411`
    but the remote row (from a prior sync) carries `4.63386615453041`
    (missing trailing digit — a PG double-precision serialization
    artifact). Second sync must NOT PATCH — every material float
    comparison is now tolerance-aware.
    """
    load = lambda _: [_pair(10.0, 5.0), _pair(11.0, 5.5), _pair(12.0, 6.0)]

    # Seed a payload once so we know its exact material shape.
    client_seed = FakeSummaryClient()
    sync_observation_spore_summaries(
        client_seed,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        load_measurements=load,
        computed_at=FIXED_COMPUTED_AT,
    )
    seed_payload = next(p for m, _, p in client_seed.calls if m == "POST")

    # Simulate PG round-trip noise: mutate a handful of float columns
    # on the remote row by ~1e-15. These are within the tolerance
    # threshold and must be treated as unchanged.
    def _bit_noisy(value: float) -> float:
        # Add or subtract a bit at the LSB of the mantissa. abs(delta)
        # ends up around 1e-15 for typical spore µm magnitudes — the
        # exact class of drift the field log captured.
        return math.nextafter(value, math.inf)

    remote_row = {
        "id": 999,
        **seed_payload,
        "length_min_um": _bit_noisy(seed_payload["length_min_um"]),
        "length_p05_um": _bit_noisy(seed_payload["length_p05_um"]),
        "length_mean_um": _bit_noisy(seed_payload["length_mean_um"]),
        "q_min": _bit_noisy(seed_payload["q_min"]),
        "q_mean": _bit_noisy(seed_payload["q_mean"]),
        "computed_at": "2026-07-01T00:00:00+00:00",
    }
    # Sanity: the noisy floats really differ at the bit level.
    assert remote_row["length_min_um"] != seed_payload["length_min_um"]
    assert remote_row["q_mean"] != seed_payload["q_mean"]

    client = FakeSummaryClient()
    client.get_responses = [[remote_row]]
    result = sync_observation_spore_summaries(
        client,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        load_measurements=load,
        computed_at=FIXED_COMPUTED_AT,
    )
    assert result["unchanged"] == 1
    assert result["updated"] == 0
    assert result["inserted"] == 0
    assert result["deleted"] == 0
    assert not any(m in ("POST", "PATCH", "DELETE") for m, _, _ in client.calls)


def test_context_json_comparison_is_structural_not_string():
    """`context_json` is jsonb on the server side. PostgREST usually
    returns it as an already-parsed Python dict, but a serializer /
    proxy could return it as a raw JSON string with keys in a
    different order than the local writer emits. Comparison MUST be
    structural so that:
      * dict-vs-dict with reordered keys → equal (Python dict `==`
        is already order-insensitive; this locks it in);
      * str-vs-dict with different serialization key order → equal
        (helper json.loads the string side);
      * str-vs-str with different key orders → equal;
      * a genuinely different value (extra key, wrong value) → not
        equal.
    """
    from utils.spore_summary_sync import _material_field_equal

    remote_dict = {
        "measurement_type": "spore", "sample_type": "fresh",
        "mount_reagent": "koh", "stain_reagent": None, "contrast_method": "dic",
    }
    payload_dict = {
        "contrast_method": "dic", "sample_type": "fresh",
        "measurement_type": "spore", "mount_reagent": "koh",
        "stain_reagent": None,
    }
    assert _material_field_equal("context_json", remote_dict, payload_dict) is True

    # Remote returned as a JSON string, in an alphabetical key order
    # that differs from CONTEXT_KEYS. This is the exact case that
    # produced false PATCHes before the structural-comparison fix.
    remote_str_reordered = (
        '{"contrast_method":"dic","measurement_type":"spore",'
        '"mount_reagent":"koh","sample_type":"fresh","stain_reagent":null}'
    )
    assert _material_field_equal(
        "context_json", remote_str_reordered, payload_dict,
    ) is True

    # Both sides serialized, both with different key orderings.
    payload_str = (
        '{"measurement_type":"spore","sample_type":"fresh",'
        '"mount_reagent":"koh","stain_reagent":null,"contrast_method":"dic"}'
    )
    assert _material_field_equal(
        "context_json", remote_str_reordered, payload_str,
    ) is True

    # Extra key on remote — genuinely different, should NOT match.
    remote_extra = dict(remote_dict, note="extra")
    assert _material_field_equal(
        "context_json", remote_extra, payload_dict,
    ) is False

    # Value drift — genuinely different, should NOT match.
    remote_wrong_value = dict(remote_dict, mount_reagent="water")
    assert _material_field_equal(
        "context_json", remote_wrong_value, payload_dict,
    ) is False

    # None-vs-None still equal; None-vs-dict still not equal.
    assert _material_field_equal("context_json", None, None) is True
    assert _material_field_equal("context_json", None, payload_dict) is False

    # A malformed JSON string falls back to raw equality — better to
    # PATCH than silently accept a corrupted remote row.
    assert _material_field_equal(
        "context_json", "{not json", payload_dict,
    ) is False


def test_sync_skips_patch_when_remote_context_json_serialization_order_differs():
    """End-to-end: writer sends the same computed summary as before,
    but the remote row's `context_json` was persisted with a different
    key order (server-side JSON canonicalization / storage engine
    choice). The writer must recognize the structural match and skip
    the PATCH."""
    load = lambda _: [_pair(10.0, 5.0), _pair(11.0, 5.5)]

    # Seed a payload to know the exact material shape.
    client_seed = FakeSummaryClient()
    sync_observation_spore_summaries(
        client_seed,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        load_measurements=load,
        computed_at=FIXED_COMPUTED_AT,
    )
    seed_payload = next(p for m, _, p in client_seed.calls if m == "POST")
    canonical_context = seed_payload["context_json"]

    # Reorder the context_json keys — same content, different insertion
    # order — and stash the whole row as remote state.
    reordered_context = {
        "contrast_method": canonical_context.get("contrast_method"),
        "sample_type": canonical_context.get("sample_type"),
        "measurement_type": canonical_context.get("measurement_type"),
        "stain_reagent": canonical_context.get("stain_reagent"),
        "mount_reagent": canonical_context.get("mount_reagent"),
    }
    stable_computed_at = "2026-07-01T00:00:00+00:00"
    remote_row = {
        "id": 88,
        **seed_payload,
        "context_json": reordered_context,
        "computed_at": stable_computed_at,
    }

    client = FakeSummaryClient()
    client.get_responses = [[remote_row]]
    result = sync_observation_spore_summaries(
        client,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        load_measurements=load,
        computed_at=FIXED_COMPUTED_AT,
    )
    assert result["unchanged"] == 1
    assert result["updated"] == 0
    assert result["inserted"] == 0
    assert result["deleted"] == 0
    assert not any(m in ("POST", "PATCH", "DELETE") for m, _, _ in client.calls)


def test_sync_skips_patch_when_only_source_app_version_would_change():
    """A desktop-client version bump alone must NOT rewrite existing
    rows. `source_app` / `source_app_version` are provenance stamps,
    not data-content signals — a 0.9.6 → 0.9.7 upgrade with no
    measurement change must produce zero PATCHes and preserve the
    row's original `computed_at`/`updated_at` from whichever earlier
    version wrote it."""
    load = lambda _: [_pair(10.0, 5.0), _pair(11.0, 5.5)]

    # Seed payload with the "old" desktop version.
    client_seed = FakeSummaryClient()
    sync_observation_spore_summaries(
        client_seed,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        source_app_version="0.9.6",
        load_measurements=load,
        computed_at=FIXED_COMPUTED_AT,
    )
    seed_payload = next(p for m, _, p in client_seed.calls if m == "POST")
    assert seed_payload["source_app_version"] == "0.9.6"

    # Remote reflects the seeded state exactly (stamped by 0.9.6).
    stable_computed_at = "2026-07-01T00:00:00+00:00"
    remote_row = {"id": 77, **seed_payload, "computed_at": stable_computed_at}

    # Second run: same measurements, same context — but the desktop
    # client has been upgraded to 0.9.7. The writer sends a payload
    # stamped 0.9.7. Material comparison must ignore the version bump
    # and skip the PATCH entirely.
    client_after_upgrade = FakeSummaryClient()
    client_after_upgrade.get_responses = [[remote_row]]
    result = sync_observation_spore_summaries(
        client_after_upgrade,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        source_app_version="0.9.7",
        load_measurements=load,
        computed_at=FIXED_COMPUTED_AT,
    )
    assert result["updated"] == 0
    assert result["unchanged"] == 1
    assert result["inserted"] == 0
    assert result["deleted"] == 0
    assert not any(m in ("POST", "PATCH", "DELETE") for m, _, _ in client_after_upgrade.calls)


def test_sync_skips_patch_when_only_computed_at_would_change():
    """A locally-fresh `computed_at` on every run must not by itself
    trigger a PATCH — `computed_at` is intentionally excluded from
    material-column comparison so back-to-back syncs don't flap the
    Stage B updated_at trigger for zero-value-change reasons."""
    load = lambda _: [_pair(10.0, 5.0)]

    # Seed payload once.
    client_seed = FakeSummaryClient()
    sync_observation_spore_summaries(
        client_seed,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        load_measurements=load,
        computed_at=FIXED_COMPUTED_AT,
    )
    seed_payload = next(p for m, _, p in client_seed.calls if m == "POST")

    # Remote has a stale computed_at; everything else matches.
    remote_row = {"id": 42, **seed_payload, "computed_at": "1999-01-01T00:00:00+00:00"}
    client = FakeSummaryClient()
    client.get_responses = [[remote_row]]

    # Second run passes a NEW computed_at — production writer uses
    # datetime.now() every call. The writer must still recognize the
    # material data is identical and skip the PATCH.
    later = datetime(2030, 1, 1, tzinfo=timezone.utc)
    result = sync_observation_spore_summaries(
        client,
        local_observation_id=1,
        remote_observation_id=7,
        user_id="user-abc",
        load_measurements=load,
        computed_at=later,
    )
    assert result["updated"] == 0
    assert result["unchanged"] == 1
    assert not any(m in ("POST", "PATCH", "DELETE") for m, _, _ in client.calls)


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

    Also seeds `images.cloud_id` non-null and `spore_measurements
    .cloud_id` non-null so the summary reconciliation query (which does
    not care about measurement cloud_ids) still returns the row. The
    measurement reconciliation tests below build their own richer
    fixture that varies `spore_measurements.cloud_id`.
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
        CREATE TABLE images (id INTEGER, observation_id INTEGER, cloud_id TEXT);
        CREATE TABLE spore_measurements (id INTEGER, image_id INTEGER, cloud_id TEXT);
        """
    )
    image_seq = 1
    meas_seq = 1
    for obs_id, (cloud_id, has_measurements) in local_measurements_by_obs.items():
        cur.execute("INSERT INTO observations VALUES (?, ?)", (obs_id, cloud_id))
        if has_measurements:
            cur.execute(
                "INSERT INTO images VALUES (?, ?, ?)",
                (image_seq, obs_id, f"cloud-image-{image_seq}"),
            )
            cur.execute(
                "INSERT INTO spore_measurements VALUES (?, ?, ?)",
                (meas_seq, image_seq, f"cloud-meas-{meas_seq}"),
            )
            image_seq += 1
            meas_seq += 1
    conn.commit()

    class _KeepAliveConnection:
        """Proxy around the seeded in-memory sqlite connection that
        forwards everything except `close()`. `sqlite3.Connection.close`
        is read-only so we cannot monkey-patch it on the instance, but
        we can wrap the connection so the reconciliation function's
        cleanup does not permanently close it — tests that invoke
        reconciliation more than once (partial-coverage / idempotence)
        need the fixture to survive across calls."""

        def __init__(self, real):
            self._real = real
            self.row_factory = real.row_factory

        def cursor(self, *args, **kwargs):
            return self._real.cursor(*args, **kwargs)

        def close(self):
            return None

        def __getattr__(self, name):
            return getattr(self._real, name)

    proxy = _KeepAliveConnection(conn)
    monkeypatch.setattr(cs, "get_connection", lambda: proxy)
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


def test_reconcile_attempts_every_candidate_and_trusts_writer_idempotence(monkeypatch):
    """Every observation with cloud_id + local measurements is routed
    through the writer, whose Stage D contract (see
    ``test_sync_is_idempotent_across_two_runs``) handles the no-op /
    PATCH-vs-INSERT decision internally. A fully-covered observation
    still gets one write pass — costing one GET + N PATCHes — but no
    partial-coverage row can slip through.
    """
    from utils import cloud_sync as cs

    _install_reconcile_cloud_sync_stubs(
        monkeypatch,
        local_measurements_by_obs={
            42: ("cloud-obs-631", True),   # fully covered remotely.
            43: ("cloud-obs-732", True),   # partially covered — writer must be called.
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
    counters = cs._reconcile_missing_spore_summaries(client, [])
    # Both candidates attempted. The writer's own tests prove that a
    # fully-covered call is a no-op (INSERTs / DELETEs both zero).
    assert counters == {"candidates": 2, "attempted": 2}
    attempted_cloud_ids = sorted(c["cloud_id"] for c in writer_calls)
    assert attempted_cloud_ids == ["cloud-obs-631", "cloud-obs-732"]


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


# ---------------------------------------------------------------------------
# Partial-coverage reconciliation — the specific case that motivated the
# tightening. Exercises the REAL writer chain (`_push_summary_for_current
# _observation` -> `sync_observation_spore_summaries`) against a fake HTTP
# client, so a genuine remote-vs-local delta produces a POST for the
# missing context and a PATCH for the already-covered one.
# ---------------------------------------------------------------------------


class _PartialCoverageClient:
    """Fake client that records every REST call and lets the test
    script GET responses per invocation. Just enough surface for
    `sync_observation_spore_summaries` and the reconciliation probe."""

    def __init__(self, user_id: str = "user-partial"):
        self.user_id = user_id
        self.calls: list[tuple[str, str, Any]] = []
        # A FIFO of responses for successive GETs to the summary table
        # (the reconciliation probe pops the first entry; the writer
        # pops the next).
        self.summary_get_responses: list[list[dict]] = []
        self._post_counter = 0

    def _get(self, path: str) -> list:
        self.calls.append(("GET", path, None))
        if "observation_spore_summaries" in path:
            if self.summary_get_responses:
                return self.summary_get_responses.pop(0)
        return []

    def _post(self, path: str, payload: Any) -> list:
        self.calls.append(("POST", path, payload))
        self._post_counter += 1
        return [{"id": 5000 + self._post_counter}]

    def _patch(self, path: str, payload: dict) -> None:
        self.calls.append(("PATCH", path, payload))

    def _delete(self, path: str) -> None:
        self.calls.append(("DELETE", path, None))


def test_reconcile_inserts_missing_context_on_partial_coverage(monkeypatch):
    """Local measurements produce TWO contexts (KOH and water). Remote
    already contains the KOH row; the water row is missing. The
    reconciliation must route the observation through the writer, which
    then patches KOH and posts water. Re-running the same reconciliation
    must be a no-op (both contexts now covered → both patched, no
    inserts, no deletes)."""
    from utils import cloud_sync as cs
    from utils import spore_summary_sync as sss
    from utils.spore_summary import build_context, compute_context_hash

    # Local fixture: one synced observation, one image row is unused
    # here — we monkeypatch `load_measurements_with_context` below so
    # the writer sees synthetic paired measurements with distinct
    # image-context columns.
    _install_reconcile_cloud_sync_stubs(
        monkeypatch,
        local_measurements_by_obs={
            42: ("cloud-obs-42", True),
        },
    )

    # Synthetic measurements yield two context groups. Keys mirror the
    # local `images` column names so `_row_context` picks them up
    # verbatim without any camelCase override.
    def _fake_load_measurements_with_context(local_id):
        assert local_id == 42
        return [
            # KOH / DIC group — n_paired=2, mean 10.5.
            {"length_um": 10.0, "width_um": 5.0, "measurement_type": "manual",
             "mount_medium": "KOH", "contrast": "DIC"},
            {"length_um": 11.0, "width_um": 5.5, "measurement_type": "manual",
             "mount_medium": "KOH", "contrast": "DIC"},
            # Water / brightfield group — n_paired=2, mean 12.5.
            {"length_um": 12.0, "width_um": 6.0, "measurement_type": "manual",
             "mount_medium": "Water", "contrast": "brightfield"},
            {"length_um": 13.0, "width_um": 6.5, "measurement_type": "manual",
             "mount_medium": "Water", "contrast": "brightfield"},
        ]
    monkeypatch.setattr(sss, "load_measurements_with_context", _fake_load_measurements_with_context)

    hash_koh = compute_context_hash(
        build_context(mount_reagent="koh", contrast_method="dic"),
    )
    hash_water = compute_context_hash(
        build_context(mount_reagent="water", contrast_method="brightfield"),
    )
    assert hash_koh != hash_water

    client = _PartialCoverageClient()
    # 1) Reconciliation's probe GET (limit=1) — return an empty list;
    #    the table exists but the observation may not be covered yet.
    # 2) Writer's per-observation GET — return the pre-existing KOH row.
    client.summary_get_responses = [
        [],                                            # probe
        [{"id": 111, "context_hash": hash_koh}],       # writer's GET
    ]

    errors: list[str] = []
    counters = cs._reconcile_missing_spore_summaries(client, errors)

    assert errors == []
    assert counters == {"candidates": 1, "attempted": 1}

    # Exactly one PATCH (KOH context already existed remotely) and
    # exactly one POST (water context is new). No DELETEs.
    method_counts = {"GET": 0, "POST": 0, "PATCH": 0, "DELETE": 0}
    for method, _path, _payload in client.calls:
        method_counts[method] = method_counts.get(method, 0) + 1
    assert method_counts["PATCH"] == 1
    assert method_counts["POST"] == 1
    assert method_counts["DELETE"] == 0

    # The POSTed payload is the water context; the PATCHed row is the
    # existing KOH id.
    posted = next(p for m, _path, p in client.calls if m == "POST")
    assert posted["context_hash"] == hash_water
    assert posted["mount_reagent"] == "water"
    assert posted["contrast_method"] == "brightfield"
    assert posted["observation_id"] == "cloud-obs-42"
    assert posted["user_id"] == "user-partial"

    patched_path, patched_payload = next(
        (path, payload) for m, path, payload in client.calls if m == "PATCH"
    )
    assert "id=eq.111" in patched_path
    assert patched_payload["context_hash"] == hash_koh
    assert patched_payload["mount_reagent"] == "koh"

    # ── Round two: repeat run is a total no-op. ───────────────────────
    #
    # After round one the KOH row's material fields match what the
    # writer just PATCHed onto id=111, and the water row's material
    # fields match what was POSTed. In round two we pre-seed remote
    # with FULL rows carrying those material columns (plus a stable
    # computed_at that must NOT move) — the material comparison
    # should return match on both rows, yielding zero POSTs, zero
    # PATCHes, zero DELETEs.
    stable_computed_at = "2026-07-01T00:00:00+00:00"
    remote_koh_full = {"id": 111, **patched_payload, "computed_at": stable_computed_at}
    remote_water_full = {"id": 5001, **posted, "computed_at": stable_computed_at}

    client_round_two = _PartialCoverageClient()
    client_round_two.summary_get_responses = [
        [],                                          # probe
        [remote_koh_full, remote_water_full],        # writer's per-obs GET
    ]
    counters_2 = cs._reconcile_missing_spore_summaries(client_round_two, [])
    assert counters_2 == {"candidates": 1, "attempted": 1}
    method_counts_2 = {"GET": 0, "POST": 0, "PATCH": 0, "DELETE": 0}
    for method, _path, _payload in client_round_two.calls:
        method_counts_2[method] = method_counts_2.get(method, 0) + 1
    assert method_counts_2["POST"] == 0
    assert method_counts_2["PATCH"] == 0
    assert method_counts_2["DELETE"] == 0
    # Remote rows are untouched, so computed_at stays at the seeded
    # value — no in-memory mutation happens here, but this pin
    # documents the semantic (Stage B updated_at trigger fires only on
    # a real UPDATE; if we PATCH nothing, computed_at + updated_at do
    # not move).
    assert remote_koh_full["computed_at"] == stable_computed_at
    assert remote_water_full["computed_at"] == stable_computed_at


# ---------------------------------------------------------------------------
# Measurement reconciliation — Problem B from the field report.
#
# Some observations were synced before Stage D landed, then had local
# spore measurements added / re-imported without going dirty. Their
# public.spore_measurements rows lag behind. The summary writer counts
# LOCAL measurements, so a summary row can advertise n_paired = 29
# while the cloud raw table only exposes 20. Fix: reconciliation pass
# that catches up raw measurements for synced observations.
# ---------------------------------------------------------------------------


def _install_measurement_reconcile_stubs(monkeypatch, fixture_rows):
    """Build a minimal in-memory SQLite fixture keyed for the
    measurement reconciliation query.

    `fixture_rows` is a list of tuples:
        (obs_local_id, obs_cloud_id, image_cloud_id, measurement_cloud_id,
         image_type, tombstone_cloud_id)
    where `image_type` defaults to `'microscope'` and
    `tombstone_cloud_id` (when non-None) inserts a row into
    `image_tombstones` matching that cloud image id — mirroring the
    push-side tombstone filter. Any string value can be replaced with
    None or ''. A single tuple corresponds to one image row + one
    spore_measurement row (+ optional tombstone).
    """
    from utils import cloud_sync as cs

    monkeypatch.setattr(cs, "is_cloud_auth_error", lambda exc: False)
    monkeypatch.setattr(cs, "is_cloud_temporary_unavailable_error", lambda exc: False)

    import sqlite3
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE observations (id INTEGER, cloud_id TEXT);
        CREATE TABLE images (id INTEGER, observation_id INTEGER, cloud_id TEXT, image_type TEXT);
        CREATE TABLE spore_measurements (id INTEGER, image_id INTEGER, cloud_id TEXT);
        CREATE TABLE image_tombstones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deleted_cloud_id TEXT NOT NULL,
            deleted_at TEXT DEFAULT ''
        );
        """
    )
    obs_seen: set[int] = set()
    image_seq = 1
    meas_seq = 1
    for row in fixture_rows:
        # Backwards-compat: accept 4-tuple (defaults to microscope, no tombstone).
        if len(row) == 4:
            obs_local_id, obs_cloud_id, image_cloud_id, measurement_cloud_id = row
            image_type = "microscope"
            tombstone_cloud_id = None
        elif len(row) == 5:
            (obs_local_id, obs_cloud_id, image_cloud_id, measurement_cloud_id, image_type) = row
            tombstone_cloud_id = None
        else:
            (obs_local_id, obs_cloud_id, image_cloud_id, measurement_cloud_id, image_type,
             tombstone_cloud_id) = row
        if obs_local_id not in obs_seen:
            cur.execute("INSERT INTO observations VALUES (?, ?)", (obs_local_id, obs_cloud_id))
            obs_seen.add(obs_local_id)
        cur.execute(
            "INSERT INTO images VALUES (?, ?, ?, ?)",
            (image_seq, obs_local_id, image_cloud_id, image_type),
        )
        cur.execute(
            "INSERT INTO spore_measurements VALUES (?, ?, ?)",
            (meas_seq, image_seq, measurement_cloud_id),
        )
        if tombstone_cloud_id:
            cur.execute(
                "INSERT INTO image_tombstones (deleted_cloud_id) VALUES (?)",
                (tombstone_cloud_id,),
            )
        image_seq += 1
        meas_seq += 1
    conn.commit()

    class _KeepAliveConnection:
        def __init__(self, real):
            self._real = real
            self.row_factory = real.row_factory

        def cursor(self, *a, **k):
            return self._real.cursor(*a, **k)

        def close(self):
            return None

        def __getattr__(self, name):
            return getattr(self._real, name)

    monkeypatch.setattr(cs, "get_connection", lambda: _KeepAliveConnection(conn))
    return conn


def test_measurement_reconcile_backfills_synced_observation_with_missing_cloud_ids(monkeypatch):
    """The obs 733 scenario: observation is synced (cloud_id set), its
    image is synced (cloud_id set), but some local spore_measurements
    have `cloud_id IS NULL` because the measurement push loop was
    skipped when the observation went clean. Reconciliation must call
    `_push_measurements_for_observation` for that observation exactly
    once."""
    from utils import cloud_sync as cs

    _install_measurement_reconcile_stubs(
        monkeypatch,
        fixture_rows=[
            # obs 42: one measurement has cloud_id, one doesn't -> needs backfill.
            (42, "cloud-obs-733", "cloud-image-a", "cloud-meas-1"),
            (42, "cloud-obs-733", "cloud-image-a", None),
            # obs 43: every measurement is already synced -> should be skipped.
            (43, "cloud-obs-746", "cloud-image-b", "cloud-meas-3"),
        ],
    )

    push_calls: list[int] = []
    monkeypatch.setattr(
        cs, "_push_measurements_for_observation",
        lambda client, local_id: push_calls.append(int(local_id)),
    )

    class _FakeClient:
        user_id = "user-x"

    counters = cs._reconcile_missing_spore_measurements(_FakeClient(), [])
    assert counters == {"candidates": 1, "attempted": 1}
    assert push_calls == [42]


def test_measurement_reconcile_is_noop_when_all_measurements_have_cloud_ids(monkeypatch):
    """Steady state: every local measurement carries a cloud_id. The
    reconciliation query returns zero candidates and the writer never
    runs."""
    from utils import cloud_sync as cs

    _install_measurement_reconcile_stubs(
        monkeypatch,
        fixture_rows=[
            (42, "cloud-obs-1", "cloud-image-a", "cloud-meas-1"),
            (42, "cloud-obs-1", "cloud-image-a", "cloud-meas-2"),
        ],
    )
    push_calls: list[int] = []
    monkeypatch.setattr(
        cs, "_push_measurements_for_observation",
        lambda client, local_id: push_calls.append(int(local_id)),
    )
    class _FakeClient:
        user_id = "user-x"

    counters = cs._reconcile_missing_spore_measurements(_FakeClient(), [])
    assert counters == {"candidates": 0, "attempted": 0}
    assert push_calls == []


def test_measurement_reconcile_skips_when_image_has_no_cloud_id(monkeypatch):
    """A local image whose parent hasn't itself been synced (no
    `images.cloud_id`) cannot have its measurements pushed yet — the
    measurement writer requires the parent image cloud_id. Skip
    reconciliation for such rows; the image sync step will handle them
    when that observation goes dirty."""
    from utils import cloud_sync as cs

    _install_measurement_reconcile_stubs(
        monkeypatch,
        fixture_rows=[
            (42, "cloud-obs-1", None, None),  # neither image nor measurement synced.
        ],
    )
    push_calls: list[int] = []
    monkeypatch.setattr(
        cs, "_push_measurements_for_observation",
        lambda client, local_id: push_calls.append(int(local_id)),
    )
    class _FakeClient:
        user_id = "user-x"

    counters = cs._reconcile_missing_spore_measurements(_FakeClient(), [])
    assert counters == {"candidates": 0, "attempted": 0}
    assert push_calls == []


def test_measurement_reconcile_records_per_observation_errors(monkeypatch):
    """When `_push_measurements_for_observation` raises a non-auth /
    non-temporary error for a specific observation, the reconciliation
    pass records it in `errors` and marks the observation dirty for
    retry — same convention as the summary reconciliation."""
    from utils import cloud_sync as cs

    _install_measurement_reconcile_stubs(
        monkeypatch,
        fixture_rows=[
            (42, "cloud-obs-1", "cloud-image-a", None),
        ],
    )

    def _boom(client, local_id):
        raise RuntimeError("boom")

    dirty_calls: list[int] = []
    monkeypatch.setattr(cs, "_push_measurements_for_observation", _boom)
    monkeypatch.setattr(cs, "mark_observation_sync_dirty", lambda obs_id: dirty_calls.append(int(obs_id)))

    class _FakeClient:
        user_id = "user-x"

    errors: list[str] = []
    counters = cs._reconcile_missing_spore_measurements(_FakeClient(), errors)
    assert counters == {"candidates": 1, "attempted": 1}
    assert len(errors) == 1
    assert "obs 42" in errors[0]
    assert "measurement reconciliation failed" in errors[0]
    assert dirty_calls == [42]


def test_measurement_reconcile_excludes_non_microscope_images(monkeypatch):
    """A local `spore_measurement` attached to a NON-microscope image
    is invisible to `_push_measurements_for_observation` (which filters
    `i.image_type = 'microscope'`). Reconciliation MUST also filter
    those out — otherwise the observation gets flagged as a candidate
    every sync but the push helper refuses to push its measurement,
    creating the infinite "measurements=22 pushed=22 next sync same
    22" loop reported in the field."""
    from utils import cloud_sync as cs

    _install_measurement_reconcile_stubs(
        monkeypatch,
        fixture_rows=[
            # Non-microscope image with an unsynced measurement — MUST
            # be skipped by reconciliation.
            (42, "cloud-obs-1", "cloud-image-a", None, "field"),
            # Microscope image with a synced measurement — nothing to
            # reconcile.
            (42, "cloud-obs-1", "cloud-image-b", "cloud-meas-fine", "microscope"),
        ],
    )
    push_calls: list[int] = []
    monkeypatch.setattr(
        cs, "_push_measurements_for_observation",
        lambda client, local_id: push_calls.append(int(local_id)),
    )
    class _FakeClient:
        user_id = "user-x"

    counters = cs._reconcile_missing_spore_measurements(_FakeClient(), [])
    assert counters == {"candidates": 0, "attempted": 0}
    assert push_calls == []


def test_measurement_reconcile_excludes_tombstoned_images(monkeypatch):
    """`_push_measurements_for_observation` explicitly skips
    measurements whose parent image is in `image_tombstones`. The
    reconciliation query MUST mirror that — otherwise it flags the
    observation forever."""
    from utils import cloud_sync as cs

    _install_measurement_reconcile_stubs(
        monkeypatch,
        fixture_rows=[
            # Tombstoned microscope image with an unsynced measurement.
            (42, "cloud-obs-1", "cloud-image-tomb", None, "microscope", "cloud-image-tomb"),
        ],
    )
    push_calls: list[int] = []
    monkeypatch.setattr(
        cs, "_push_measurements_for_observation",
        lambda client, local_id: push_calls.append(int(local_id)),
    )
    class _FakeClient:
        user_id = "user-x"

    counters = cs._reconcile_missing_spore_measurements(_FakeClient(), [])
    assert counters == {"candidates": 0, "attempted": 0}
    assert push_calls == []


def test_measurement_reconcile_is_idempotent_after_successful_push(monkeypatch):
    """The specific field regression: after a successful push,
    subsequent reconciliations must find zero candidates. Simulates
    push by having the fake helper stamp cloud_id on the seeded rows,
    then re-runs reconciliation and asserts zero candidates."""
    from utils import cloud_sync as cs

    conn = _install_measurement_reconcile_stubs(
        monkeypatch,
        fixture_rows=[
            (42, "cloud-obs-1", "cloud-image-a", None, "microscope"),
            (42, "cloud-obs-1", "cloud-image-a", None, "microscope"),
        ],
    )

    def _stamp_all_local_cloud_ids(client, local_id):
        # Emulate `_push_measurements_for_observation`'s
        # local-cloud-id stamping side effect.
        c = conn.cursor()
        c.execute(
            "UPDATE spore_measurements SET cloud_id = 'cloud-meas-' || id "
            "WHERE image_id IN (SELECT id FROM images WHERE observation_id = ?)",
            (local_id,),
        )
        conn.commit()

    monkeypatch.setattr(cs, "_push_measurements_for_observation", _stamp_all_local_cloud_ids)
    class _FakeClient:
        user_id = "user-x"

    counters_1 = cs._reconcile_missing_spore_measurements(_FakeClient(), [])
    assert counters_1 == {"candidates": 1, "attempted": 1}
    counters_2 = cs._reconcile_missing_spore_measurements(_FakeClient(), [])
    assert counters_2 == {"candidates": 0, "attempted": 0}


