"""Stage C review round 3 — three follow-ups on the round-2 mechanisms.

Sync-integrity follow-ups (docs/plans/active/2026-09-25-sync-integrity-follow-ups.md).

1. **Rejected narrowing-while-blocked must fail closed.** The Case F
   privacy-while-blocked exception (`_push_narrower_visibility_while_blocked`,
   round 2 item 2) issued its scoped visibility-only PATCH and, on any
   failure, only logged a warning — nothing recorded the failure anywhere a
   caller or a later sync could see, and nothing distinguished "the PATCH
   never even ran" from "the cloud actively rejected it" (e.g. a
   privacy-slot-limit quota check). The observation already stays
   dirty/under-review for the Case F reason regardless, but the specific
   narrowing failure must be visible via the same sync-error machinery
   every other push failure uses, not just a log line, and must never
   leave a baseline recording the narrowing as if it had landed.

2. **Narrowing must use fresh state.** The decision (and, before this fix,
   the blind PATCH) is based on `remote`, a snapshot fetched once, early
   in the sync cycle (`push_all`'s bulk `remote_lookup`), not immediately
   before the write. A concurrent write to the SAME `visibility` column
   between that fetch and the PATCH could be silently overwritten. Fixed
   with a PostgREST equality precondition (`visibility=eq.<expected>`,
   `Prefer: return=representation`): a lost race reports success with an
   empty body, never an error, so the caller must inspect the returned
   rows.

3. **Failed identity-clear read-back must fail closed for every failure
   shape.** `_verify_identity_clear_landed` (round 2 item 3) already failed
   closed when the read-back explicitly showed the identity still
   attached. It did NOT fail closed when the read-back call itself raised
   (transport failure), nor when it returned a malformed/incomplete row
   (missing `selected_sporely_taxon_id` entirely) or `None` for a row that
   MUST exist (the RPC just ran against this exact `cloud_id` without
   raising) — those were treated as "nothing to enforce" and silently let
   the clear count as confirmed.

Uses the same harness as tests/test_cloud_sync_identity_clear.py and
tests/test_cloud_sync_no_baseline_identity_contradiction.py: a
`SporelyCloudClient` subclass whose PATCH/POST/RPC mutate one in-memory
cloud row, driving the real `sync_all`.
"""
from __future__ import annotations

import pytest

from database import models, schema
from database.models import SettingsDB
from utils import cloud_sync

from tests.test_cloud_sync_identity_clear import (  # noqa: F401  reuse the harness
    _FakeCloud,
    _FakeConcept,
    _sync,
    _local,
    _edit_locally,
    _establish_proven_identity,
)
from tests.test_cloud_sync_no_baseline_identity_contradiction import (  # noqa: F401
    _seed_contradictory_observation,
)


@pytest.fixture
def env(tmp_path, monkeypatch):
    db_path = tmp_path / "sporely.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: tmp_path / "ref.db")
    monkeypatch.setattr(schema, "get_bundled_reference_database_path", lambda: tmp_path / "bundled.db")
    monkeypatch.setattr(schema, "_migrate_reference_values", lambda *a, **k: None)
    monkeypatch.setattr(schema, "_migrate_reference_mounts_and_stains", lambda *a, **k: None)
    schema.init_database()

    def _fake_installed_concept(sporely_taxon_id):
        if int(sporely_taxon_id) == 83668:
            return _FakeConcept(release_id=1, scientific_name="Conocybe rugosa", rank="species")
        return None

    monkeypatch.setattr(cloud_sync, "_installed_taxon_concept", _fake_installed_concept)
    app_settings: dict = {}
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(app_settings))
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda values: app_settings.update(values))
    from utils import reference_cloud_sync
    monkeypatch.setattr(cloud_sync, "_push_summary_for_current_observation", lambda *a, **k: None)
    monkeypatch.setattr(cloud_sync, "_reconcile_missing_spore_summaries", lambda *a, **k: 0)
    monkeypatch.setattr(cloud_sync, "_reconcile_missing_spore_measurements", lambda *a, **k: 0)
    monkeypatch.setattr(
        reference_cloud_sync, "sync_reference_library",
        lambda _client, *, pull_only=False: reference_cloud_sync.ReferenceSyncResult(),
    )
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *a, **k: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "pull_calibrations", lambda *a, **k: {"pulled": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_backfill_missing_exif_on_cloud_images", lambda: None)
    return _FakeCloud()


@pytest.fixture
def make_identity_env(tmp_path, monkeypatch):
    """Factory version of tests/test_cloud_sync_identity_clear.py's `env`
    fixture: takes the desired `_FakeCloud` subclass INSTANCE (so each test
    can inject its own read-back failure behaviour) and returns
    `(cloud, local_id, cloud_id)` for one already-synced observation, ready
    for `_establish_proven_identity`.
    """
    db_path = tmp_path / "sporely.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: tmp_path / "ref.db")
    monkeypatch.setattr(schema, "get_bundled_reference_database_path", lambda: tmp_path / "bundled.db")
    monkeypatch.setattr(schema, "_migrate_reference_values", lambda *a, **k: None)
    monkeypatch.setattr(schema, "_migrate_reference_mounts_and_stains", lambda *a, **k: None)
    schema.init_database()

    def _fake_installed_concept(sporely_taxon_id):
        if int(sporely_taxon_id) == 83668:
            return _FakeConcept(release_id=1, scientific_name="Conocybe rugosa", rank="species")
        return None

    monkeypatch.setattr(cloud_sync, "_installed_taxon_concept", _fake_installed_concept)
    app_settings: dict = {}
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(app_settings))
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda values: app_settings.update(values))
    from utils import reference_cloud_sync
    monkeypatch.setattr(cloud_sync, "_push_summary_for_current_observation", lambda *a, **k: None)
    monkeypatch.setattr(cloud_sync, "_reconcile_missing_spore_summaries", lambda *a, **k: 0)
    monkeypatch.setattr(cloud_sync, "_reconcile_missing_spore_measurements", lambda *a, **k: 0)
    monkeypatch.setattr(
        reference_cloud_sync, "sync_reference_library",
        lambda _client, *, pull_only=False: reference_cloud_sync.ReferenceSyncResult(),
    )
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *a, **k: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "pull_calibrations", lambda *a, **k: {"pulled": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_backfill_missing_exif_on_cloud_images", lambda: None)

    def _make(cloud):
        local_id = models.ObservationDB.create_observation(
            date="2026-09-20", genus="Conocybe", species="rugosa",
            notes="notes A", location="Location A", habitat="Habitat A",
        )
        _sync(cloud)
        obs = models.ObservationDB.get_observation(local_id)
        assert obs["cloud_id"] and obs["sync_status"] == "synced", obs
        return cloud, local_id, obs["cloud_id"]

    return _make


# ── Finding 1: rejected narrowing-while-blocked fails closed ────────────────


class _RejectingNarrowCloud(_FakeCloud):
    """Simulates the cloud rejecting the scoped visibility-only PATCH (e.g.
    a privacy-slot-limit quota check) while the observation is otherwise
    blocked for Case F review."""

    def _patch_with_precondition(self, path, payload):
        if "visibility=eq." in path:
            raise cloud_sync.CloudSyncError(
                'PATCH observations: {"code":"23514","message":"Free Sporely '
                'accounts can keep up to 20 privacy slot observations. Publish '
                'or use exact public location to continue."}'
            )
        return super()._patch_with_precondition(path, payload)


def test_rejected_privacy_narrowing_while_blocked_fails_closed(env):
    cloud = _RejectingNarrowCloud()
    local_id, cloud_id = _seed_contradictory_observation(cloud)
    cloud.rows[cloud_id]["visibility"] = "public"
    _sync(cloud)  # establishes the Case F blocked state
    assert _local(local_id)["sync_blocked_reason"] == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER

    _edit_locally(local_id, sharing_scope="private")
    result = _sync(cloud)

    # The cloud never actually changed.
    assert cloud.rows[cloud_id]["visibility"] == "public"

    local = _local(local_id)
    assert local["sync_status"] == "dirty", "must stay dirty, never look synced"
    assert local["sync_blocked_reason"] == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER, (
        "the Case F identity block must stay authoritative, not be overwritten"
    )
    # The rejection must be visible via the existing sync-error machinery,
    # not only a log line.
    assert local.get("sync_error_code") == "privacy_narrowing_failed"
    assert any("did not reach the cloud" in str(e).lower() for e in result.get("errors") or []), result["errors"]

    # No baseline may record the narrowing as if it had landed.
    stored = cloud_sync._load_cloud_observation_snapshot(cloud_id)
    if stored:
        import json
        baseline_obs = json.loads(stored)["observation"]
        assert baseline_obs.get("visibility") != "private"


# ── Finding 2: narrowing must use fresh state (deterministic race) ──────────


class _RaceLosingNarrowCloud(_FakeCloud):
    """Deterministically simulates a concurrent write landing on the SAME
    `visibility` column between this sync cycle's earlier remote fetch
    (`push_all`'s `remote_lookup`, captured as `public`) and the desktop's
    own scoped narrowing PATCH: the "other client's" write is applied
    synchronously, right before the precondition is evaluated, so the
    precondition (`visibility=eq.public`) no longer matches."""

    def __init__(self, concurrent_visibility="friends"):
        super().__init__()
        self._concurrent_visibility = concurrent_visibility
        self._race_armed = False

    def arm_race(self):
        self._race_armed = True

    def _patch_with_precondition(self, path, payload):
        if self._race_armed and "visibility=eq." in path:
            self._race_armed = False
            cloud_id = path.split("id=eq.", 1)[1].split("&", 1)[0]
            self.rows[cloud_id]["visibility"] = self._concurrent_visibility
        return super()._patch_with_precondition(path, payload)


def test_privacy_narrowing_lost_race_never_overwrites_concurrent_state(env):
    cloud = _RaceLosingNarrowCloud(concurrent_visibility="friends")
    local_id, cloud_id = _seed_contradictory_observation(cloud)
    cloud.rows[cloud_id]["visibility"] = "public"
    _sync(cloud)  # establishes the Case F blocked state
    assert _local(local_id)["sync_blocked_reason"] == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER

    _edit_locally(local_id, sharing_scope="private")
    cloud.arm_race()
    result = _sync(cloud)

    # The "other client's" concurrent write must survive untouched — the
    # desktop's stale-state PATCH must never have overwritten it, and must
    # never have widened it back to public either.
    assert cloud.rows[cloud_id]["visibility"] == "friends"

    local = _local(local_id)
    assert local["sync_status"] == "dirty"
    assert local["sync_blocked_reason"] == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER
    assert local.get("sync_error_code") == "privacy_narrowing_lost_race"
    assert any("did not reach the cloud" in str(e).lower() for e in result.get("errors") or []), result["errors"]

    stored = cloud_sync._load_cloud_observation_snapshot(cloud_id)
    if stored:
        import json
        baseline_obs = json.loads(stored)["observation"]
        assert baseline_obs.get("visibility") != "private"


def test_privacy_narrowing_succeeds_when_state_is_fresh(env):
    """Control: the precondition PATCH must still succeed on the ordinary,
    non-racing path — the round-2 regression test's own contract."""
    cloud = _RaceLosingNarrowCloud(concurrent_visibility="friends")
    local_id, cloud_id = _seed_contradictory_observation(cloud)
    cloud.rows[cloud_id]["visibility"] = "public"
    _sync(cloud)

    _edit_locally(local_id, sharing_scope="private")
    result = _sync(cloud)  # race not armed

    assert cloud.rows[cloud_id]["visibility"] == "private"
    local = _local(local_id)
    assert local["sync_blocked_reason"] == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER
    assert local.get("sync_error_code") != "privacy_narrowing_failed"
    assert local.get("sync_error_code") != "privacy_narrowing_lost_race"


# ── Finding 3: failed identity-clear read-back fails closed ─────────────────


def _arm_rename_clear(local_id):
    _edit_locally(
        local_id,
        genus="Funny", species="brown mushroom", common_name=None,
        sporely_taxon_id=None, taxon_identity_state="no_identity_evidence",
        taxon_identity_proof=None, taxon_identity_source_system=None,
        taxon_identity_namespace=None, taxon_identity_external_id=None,
        taxon_identity_raw_external_id=None,
        scientific_name_snapshot=None, taxon_rank_snapshot=None,
        allow_nulls=True,
    )


class _TransportFailureOnVerifyCloud(_FakeCloud):
    """The identity-clear RPC succeeds; the desktop's OWN read-back call
    then hits a transport failure (network error) instead of a response."""

    def __init__(self):
        super().__init__()
        self.raise_on_next_get_observation = False

    def get_observation(self, cloud_id):
        if self.raise_on_next_get_observation:
            self.raise_on_next_get_observation = False
            raise ConnectionError("simulated transport failure")
        return super().get_observation(cloud_id)


def test_identity_clear_readback_transport_failure_fails_closed(make_identity_env):
    cloud, local_id, cloud_id = make_identity_env(_TransportFailureOnVerifyCloud())
    _establish_proven_identity(cloud, local_id, cloud_id)
    _arm_rename_clear(local_id)

    cloud.raise_on_next_get_observation = True
    result = _sync(cloud)

    assert any(
        "identity clear did not take effect" in str(e).lower() for e in result.get("errors") or []
    ), result["errors"]
    local = _local(local_id)
    assert local["sync_status"] == "dirty", "a failed read-back must stay dirty, never advance as verified"


class _MalformedVerifyCloud(_FakeCloud):
    """The identity-clear RPC succeeds; the read-back returns a row that is
    missing `selected_sporely_taxon_id` entirely — a malformed/incomplete
    response, never to be conflated with "confirmed cleared"."""

    def __init__(self):
        super().__init__()
        self.malform_next_get_observation = False

    def get_observation(self, cloud_id):
        if self.malform_next_get_observation:
            self.malform_next_get_observation = False
            row = super().get_observation(cloud_id) or {}
            row.pop("selected_sporely_taxon_id", None)
            return row
        return super().get_observation(cloud_id)


def test_identity_clear_readback_malformed_row_fails_closed(make_identity_env):
    cloud, local_id, cloud_id = make_identity_env(_MalformedVerifyCloud())
    _establish_proven_identity(cloud, local_id, cloud_id)
    _arm_rename_clear(local_id)

    cloud.malform_next_get_observation = True
    result = _sync(cloud)

    assert any(
        "identity clear did not take effect" in str(e).lower() for e in result.get("errors") or []
    ), result["errors"]
    local = _local(local_id)
    assert local["sync_status"] == "dirty"


class _EmptyVerifyCloud(_FakeCloud):
    """The identity-clear RPC succeeds; the read-back returns `None` for a
    row that MUST exist (this exact cloud_id was just mutated by the RPC
    without raising) — a concurrency-loss/inaccessible shape that must not
    be treated as "confirmed cleared" either."""

    def __init__(self):
        super().__init__()
        self.empty_next_get_observation = False

    def get_observation(self, cloud_id):
        if self.empty_next_get_observation:
            self.empty_next_get_observation = False
            return None
        return super().get_observation(cloud_id)


def test_identity_clear_readback_unexpected_empty_fails_closed(make_identity_env):
    cloud, local_id, cloud_id = make_identity_env(_EmptyVerifyCloud())
    _establish_proven_identity(cloud, local_id, cloud_id)
    _arm_rename_clear(local_id)

    cloud.empty_next_get_observation = True
    result = _sync(cloud)

    assert any(
        "identity clear did not take effect" in str(e).lower() for e in result.get("errors") or []
    ), result["errors"]
    local = _local(local_id)
    assert local["sync_status"] == "dirty"
