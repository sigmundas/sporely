"""Case F — no usable identity baseline, cloud holds identity A, local names
contradict it.

Sync-integrity follow-up 4 (docs/plans/active/2026-09-25-sync-integrity-follow-ups.md),
Stage C review round 2. Distinct from the manual-rename explicit-clear case
(that requires a KNOWN baseline proving a previously-synced identity): here
there is no usable baseline at all (no stored snapshot, or a stored snapshot
that predates identity tracking), the cloud row carries a bound
`selected_sporely_taxon_id` = A, and the local row's own committed
genus/species (B) already contradict A, with no local identity claim of its
own (state `no_identity_evidence`).

Verified defect (reproduced at runtime before the fix): with no baseline,
`_classify_identity_sync_change`'s "no local claim" branch treats this as
ordinary silent adoption territory, so push_all's early identity-review check
(which only fires when the LOCAL row itself already claims a conflicting
identity) never triggers. The observation's own genus/species then PATCH onto
the cloud row unconditionally — while `selected_sporely_taxon_id` stays A —
producing exactly the stale/contradictory cloud row Stage C's explicit clear
fixes on the other side (new names, old identity), except introduced from a
push instead of a rename. Separately, pull's own "no stored snapshot" full
apply (`_apply_remote_observation_fields(..., identity_fail_closed=True)`)
only fails closed on the IDENTITY columns when local already holds a claim;
a non-claiming local's plain genus/species are overwritten unconditionally,
so even blocking the push alone left the very next pull silently adopting the
cloud's contradictory identity onto the preserved names.

Fix: two symmetric guards (push and pull), both keyed on "no usable
baseline + remote holds a non-empty identity + local holds no identity claim
+ the committed identification differs from remote's" — block the WHOLE
observation's push or pull-apply through the SAME existing conflict-review
mechanism used elsewhere (`_format_review_needed_error`,
`_set_observation_conflict_review_pending`), never a new one.

These tests drive the real `sync_all` (push, then pull) against real SQLite
persistence, following the harness in tests/test_cloud_sync_identity_clear.py.
"""
from __future__ import annotations

import pytest

from database import models, schema
from database.models import SettingsDB
from utils import cloud_sync
from utils.taxon_identity import TaxonIdentity

from tests.test_cloud_sync_identity_clear import (  # noqa: F401  reuse the harness
    _FakeCloud,
    _sync,
    _local,
    _edit_locally,
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
    monkeypatch.setattr(cloud_sync, "_installed_taxon_concept", lambda sid: None)
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
    cloud = _FakeCloud()
    return cloud


def _seed_contradictory_observation(cloud, *, notes="notes A"):
    """A local row synced once, then the cloud independently binds A while
    this device's own committed identification stays B, with the stored
    baseline wiped so no usable snapshot exists."""
    local_id = models.ObservationDB.create_observation(
        date="2026-09-20", genus="Conocybe", species="rugosa", notes=notes,
    )
    _sync(cloud)
    obs = models.ObservationDB.get_observation(local_id)
    cloud_id = obs["cloud_id"]
    # Cloud independently binds identity A = 83668 (e.g. the web).
    cloud.rows[cloud_id]["selected_sporely_taxon_id"] = 83668
    cloud.rows[cloud_id]["taxon_identity_state"] = "sporely_v2"
    # No usable baseline: wipe the stored snapshot.
    SettingsDB.set_setting(cloud_sync._cloud_observation_snapshot_key(cloud_id), '')
    # Local's own committed identification is B, contradicting A, with no
    # local identity claim at all.
    _edit_locally(local_id, genus="Something", species="different")
    return local_id, cloud_id


def test_contradiction_blocks_the_push_before_any_cloud_mutation(env):
    cloud = env
    local_id, cloud_id = _seed_contradictory_observation(cloud)

    cloud_before = dict(cloud.rows[cloud_id])
    result = _sync(cloud)
    cloud_after = dict(cloud.rows[cloud_id])

    # (a) the cloud row is unchanged, column for column.
    assert cloud_after == cloud_before, "no cloud mutation may occur while the contradiction is unresolved"
    assert cloud.patches == [], "the push must be blocked before any PATCH"
    assert cloud.posts == [] or all(p.get("id") != cloud_id for p in cloud.posts)
    assert cloud.rpcs == [], "no identity RPC either"

    # Local keeps its own names; the cloud identity is not adopted.
    local = _local(local_id)
    assert (local["genus"], local["species"]) == ("Something", "different")
    identity = TaxonIdentity.from_row(local)
    assert identity.state == "no_identity_evidence"
    assert identity.sporely_taxon_id != 83668

    # Kept dirty, surfaced through the existing conflict-review mechanism.
    assert local["sync_status"] == "dirty"
    assert local["sync_blocked_reason"] == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER
    assert any("needs review" in str(e).lower() for e in result.get("errors") or [])


def test_second_sync_does_not_escalate_or_resurrect(env):
    """No repeated writes and no silent resolution on a later idle sync."""
    cloud = env
    local_id, cloud_id = _seed_contradictory_observation(cloud)
    _sync(cloud)
    writes_after_first = cloud.writes
    cloud_state_after_first = dict(cloud.rows[cloud_id])

    result = _sync(cloud)

    assert cloud.writes == writes_after_first, "an idle re-sync must not touch the cloud again"
    assert cloud.rows[cloud_id] == cloud_state_after_first
    local = _local(local_id)
    assert (local["genus"], local["species"]) == ("Something", "different")
    assert local["sync_status"] == "dirty"
    assert any("needs review" in str(e).lower() for e in result.get("errors") or [])


# ── (b): unrelated fields on the same observation are not partially pushed ──


def test_unrelated_field_edit_on_the_same_observation_is_also_blocked(env):
    """Contrast with the ALREADY-covered, differently-shaped case where a
    local row already CLAIMS a conflicting identity: there, an independent
    field (notes) is explicitly allowed to go out while only the identity
    itself stays under review — see
    tests/test_cloud_identity_fail_closed.py::
    test_no_baseline_disagreement_stays_under_review_and_blocks_the_next_push,
    and the code comment at the Case F guard in `push_all` explaining why
    that policy does not extend here. For a NON-claiming local whose plain
    names already contradict the cloud's bound identity, the contradiction
    IS the identification itself, so nothing on the observation is safe to
    push — including an otherwise-independent field like notes.
    """
    cloud = env
    local_id, cloud_id = _seed_contradictory_observation(cloud, notes="notes A")
    _edit_locally(local_id, notes="notes A2")

    cloud_before = dict(cloud.rows[cloud_id])
    _sync(cloud)
    cloud_after = dict(cloud.rows[cloud_id])

    assert cloud_after == cloud_before, "notes must not be partially pushed past the block"
    assert cloud.patches == []
    local = _local(local_id)
    assert local["notes"] == "notes A2", "the local edit is preserved, just not yet synced"
    assert local["sync_status"] == "dirty"


# ── (c): an explicit picker resolution afterwards clears the conflict ───────


def test_picker_resolution_to_the_cloud_concept_clears_the_conflict(env):
    cloud = env
    local_id, cloud_id = _seed_contradictory_observation(cloud)
    _sync(cloud)  # establish the blocked state
    assert _local(local_id)["sync_blocked_reason"] == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER

    # Explicit picker resolution: the owner accepts the cloud's own concept
    # (83668 / Conocybe rugosa) as the correct identification.
    proven = TaxonIdentity.from_taxonomy_v2_artifact(
        83668, scientific_name="Conocybe rugosa", rank="species",
    ).to_row()
    _edit_locally(local_id, genus="Conocybe", species="rugosa", allow_nulls=True, **proven)

    result = _sync(cloud)

    assert not any("needs review" in str(e).lower() for e in result.get("errors") or []), result["errors"]
    local = _local(local_id)
    assert local["sync_status"] == "synced"
    assert local["sync_blocked_reason"] is None
    identity = TaxonIdentity.from_row(local)
    assert identity.is_proven_sporely and identity.sporely_taxon_id == 83668
    row = cloud.rows[cloud_id]
    assert (row["genus"], row["species"]) == ("Conocybe", "rugosa")
    assert row["selected_sporely_taxon_id"] == 83668


def test_picker_resolution_to_a_different_concept_stays_under_review_not_a_clear(env):
    """Resolving to a DIFFERENT proven concept (not the cloud's A) turns the
    non-claiming contradiction into an ordinary proven-vs-proven disagreement
    with no baseline — the ALREADY-covered case
    (tests/test_cloud_identity_fail_closed.py::
    test_no_baseline_disagreement_stays_under_review_and_blocks_the_next_push):
    the identity RPC is withheld and the observation stays under review, but
    ordinary fields (here, genus/species) are still pushed per that existing
    accepted partial-push policy. It must NOT go through the explicit-clear
    path either (there is still no baseline to justify a clear), and it must
    NOT silently bind the cloud to 99001 without review.
    """
    cloud = env
    local_id, cloud_id = _seed_contradictory_observation(cloud)
    _sync(cloud)

    other = TaxonIdentity.from_taxonomy_v2_artifact(
        99001, scientific_name="Amanita muscaria", rank="species",
    ).to_row()
    _edit_locally(local_id, genus="Amanita", species="muscaria", allow_nulls=True, **other)

    result = _sync(cloud)

    local = _local(local_id)
    # Falls into the pre-existing "claim vs claim, no baseline" review policy:
    # stays dirty/under review rather than resolving in one sync.
    assert local["sync_status"] == "dirty"
    assert any("needs review" in str(e).lower() for e in result.get("errors") or [])
    row = cloud.rows[cloud_id]
    # Ordinary fields go out per the existing accepted partial-push policy...
    assert (row["genus"], row["species"]) == ("Amanita", "muscaria")
    # ...but the identity itself is withheld, never silently bound to 99001.
    assert row["selected_sporely_taxon_id"] == 83668
    select_rpcs = [p for name, p in cloud.rpcs if name == "set_observation_selected_taxon_v2"]
    assert select_rpcs == [], "the identity RPC is withheld while under review, not silently issued"
    clear_rpcs = [p for name, p in cloud.rpcs if name == "set_observation_identification_v2"]
    assert clear_rpcs == [], "resolving to a new concept is a selection, not a clear"


# ── (d): does a manual free-text rename (the supported non-picker action) ───
# ── clear this conflict on its own?                                        ──


def test_manual_free_text_rename_does_not_clear_a_no_baseline_contradiction(env):
    """The desktop's other supported manual-identification action — a
    free-text scientific-name edit (`ui/observations_tab.py``
    `_on_taxon_identity_field_edited`, the same path Stage C's explicit-clear
    fix targets for a KNOWN baseline) — does NOT clear this conflict. Stage
    C's explicit-clear condition requires a stored baseline proving a
    previously-synced identity (`_maybe_clear_stale_cloud_identity` returns
    False immediately when `baseline_obs is None`), which Case F lacks by
    definition. Renaming to yet another committed name (C) still contradicts
    the cloud's A and is still not a local identity claim, so the SAME guard
    keeps blocking it. There is no local UI action other than a picker
    resolution (to A or to a genuinely different proven concept) that clears
    this specific no-baseline contradiction.
    """
    cloud = env
    local_id, cloud_id = _seed_contradictory_observation(cloud)
    _sync(cloud)  # establish the blocked state

    # Manual free-text rename to yet a THIRD name, still no identity claim.
    _edit_locally(local_id, genus="Yet", species="another name")

    cloud_before = dict(cloud.rows[cloud_id])
    result = _sync(cloud)
    cloud_after = dict(cloud.rows[cloud_id])

    assert cloud_after == cloud_before, "a further manual rename must not clear the conflict either"
    local = _local(local_id)
    assert (local["genus"], local["species"]) == ("Yet", "another name")
    assert local["sync_status"] == "dirty"
    assert local["sync_blocked_reason"] == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER
    assert any("needs review" in str(e).lower() for e in result.get("errors") or [])
