"""A deliberate desktop identification change must explicitly clear a stale
cloud taxonomy identity, not leave it beside a contradictory new name.

Sync-integrity follow-up 4 (docs/plans/active/2026-09-25-sync-integrity-follow-ups.md).

Verified defect: an observation can carry a proven cloud identity (picker ->
Sporely concept A, agreed by local, cloud and the sync baseline). When the
owner then changes the identification through a non-picker path (manual
free-text rename, or a genus/species dropdown change), the local identity
correctly becomes ``no_identity_evidence`` — but the push gate
(``_sync_observation_selected_taxon``) only ever asserts a *proven* identity;
an unproven/absent one was unconditionally a skip, never a clear. The desktop
still pushes the new genus/species/name as ordinary fields, so the cloud ends
up with the new name next to the stale old ``selected_sporely_taxon_id``. A
fresh device can then download and preserve that stale identity as
``cloud_selected_unverified`` beside the contradictory new name.

These tests drive the real `sync_all` (push, then pull) against the real
SQLite schema and real snapshot bookkeeping, exactly like
``tests/test_cloud_sync_remote_only_adoption.py`` (Stage B). Only the network
is replaced: a `SporelyCloudClient` subclass whose PATCH/POST/RPC mutate one
in-memory cloud row, including applying `set_observation_identification_v2`
so the fake cloud behaves like the real atomic RPC.
"""
from __future__ import annotations

import copy
import itertools
import json
from datetime import datetime, timedelta, timezone

import pytest

from database import models, schema
from utils import cloud_sync
from utils.taxon_identity import TaxonIdentity

USER_ID = "user-123"
_TICK = itertools.count(1)


def _now_text() -> str:
    return (datetime.now(timezone.utc) + timedelta(microseconds=next(_TICK))).isoformat()


class _FakeCloud(cloud_sync.SporelyCloudClient):
    """One user's `observations` table, held in memory.

    Unlike the Stage B fake (which only records RPC calls), this one applies
    `set_observation_selected_taxon_v2` and `set_observation_identification_v2`
    to the in-memory row, the way the real atomic RPCs mutate the server row —
    otherwise a test could not tell an issued clear from a no-op one.
    """

    def __init__(self):  # noqa: D401 - no network session
        self.user_id = USER_ID
        self.rows: dict[str, dict] = {}
        self.patches: list[dict] = []
        self.posts: list[dict] = []
        self.rpcs: list[tuple[str, dict]] = []
        self._next_id = 900

    # ── reads ────────────────────────────────────────────────────────────
    def fetch_current_user_id(self):
        return USER_ID

    def list_remote_observations(self):
        return [copy.deepcopy(row) for row in self.rows.values()]

    def get_observation(self, cloud_id):
        row = self.rows.get(str(cloud_id or "").strip())
        return copy.deepcopy(row) if row else None

    def _find_cloud_observation(self, desktop_id):
        for row in self.rows.values():
            if row.get("desktop_id") == desktop_id:
                return row["id"]
        return None

    def list_remote_calibrations(self):
        return []

    def pull_image_metadata(self, obs_cloud_id, include_deleted_for_sync=False):
        return []

    def pull_bulk_image_metadata(self, obs_cloud_ids):
        return []

    def pull_measurements_for_images(self, image_cloud_ids):
        return []

    # ── writes ───────────────────────────────────────────────────────────
    def _patch(self, path, payload):
        cloud_id = path.split("id=eq.", 1)[1].split("&", 1)[0]
        self.patches.append(copy.deepcopy(payload))
        row = self.rows[cloud_id]
        row.update({k: v for k, v in payload.items() if k != "user_id"})
        row["updated_at"] = _now_text()

    def _post(self, path, payload):
        assert path == "observations", path
        self.posts.append(copy.deepcopy(payload))
        cloud_id = str(self._next_id)
        self._next_id += 1
        row = {k: v for k, v in payload.items() if k != "user_id"}
        row.update({"id": cloud_id, "created_at": _now_text(), "updated_at": _now_text()})
        self.rows[cloud_id] = row
        return [copy.deepcopy(row)]

    def _rpc(self, function_name, payload=None):
        payload = dict(payload or {})
        self.rpcs.append((function_name, dict(payload)))
        cloud_id = str(payload.get("p_observation_id") or "").strip()
        row = self.rows.get(cloud_id)
        if row is None:
            return None
        if function_name == "set_observation_selected_taxon_v2":
            row["selected_sporely_taxon_id"] = payload.get("p_sporely_taxon_id")
            row["taxon_identity_state"] = "sporely_v2"
            row["updated_at"] = _now_text()
        elif function_name == "set_observation_identification_v2":
            row["selected_sporely_taxon_id"] = payload.get("p_sporely_taxon_id")
            row["taxon_identity_state"] = payload.get("p_identity_state")
            row["taxon_identity_source_system"] = payload.get("p_source_system")
            row["taxon_identity_namespace"] = payload.get("p_namespace")
            row["taxon_identity_external_id"] = payload.get("p_external_id")
            row["taxon_identity_raw_external_id"] = payload.get("p_raw_external_id")
            if payload.get("p_write_name"):
                row["genus"] = payload.get("p_genus")
                row["species"] = payload.get("p_species")
                row["common_name"] = payload.get("p_common_name")
            row["updated_at"] = _now_text()
        return None

    def set_desktop_id(self, cloud_id, desktop_id):
        self.rows[str(cloud_id)]["desktop_id"] = desktop_id

    # ── another device ───────────────────────────────────────────────────
    def edit_from_other_device(self, cloud_id, **fields):
        self.rows[str(cloud_id)].update(fields)
        self.rows[str(cloud_id)]["updated_at"] = _now_text()

    @property
    def writes(self) -> int:
        return len(self.patches) + len(self.posts) + len(self.rpcs)

    def clear_rpcs_since(self, index: int = 0) -> list[dict]:
        return [payload for name, payload in self.rpcs[index:] if name == "set_observation_identification_v2"]

    def select_rpcs_since(self, index: int = 0) -> list[dict]:
        return [payload for name, payload in self.rpcs[index:] if name == "set_observation_selected_taxon_v2"]


@pytest.fixture
def env(tmp_path, monkeypatch):
    db_path = tmp_path / "sporely.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: tmp_path / "ref.db")
    monkeypatch.setattr(schema, "get_bundled_reference_database_path", lambda: tmp_path / "bundled.db")
    monkeypatch.setattr(schema, "_migrate_reference_values", lambda *a, **k: None)
    monkeypatch.setattr(schema, "_migrate_reference_mounts_and_stains", lambda *a, **k: None)
    schema.init_database()

    # Route the cloud->local artifact lookup for `83668` to a fixed
    # "Conocybe rugosa" concept so `cloud_selected_unverified` adoption
    # (fresh-device tests) can be exercised deterministically.
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

    cloud = _FakeCloud()
    local_id = models.ObservationDB.create_observation(
        date="2026-09-20", genus="Conocybe", species="rugosa",
        notes="notes A", location="Location A", habitat="Habitat A",
    )
    _sync(cloud)  # ordinary first sync: POST, snapshot, media signature
    obs = models.ObservationDB.get_observation(local_id)
    assert obs["cloud_id"] and obs["sync_status"] == "synced", obs
    return cloud, local_id, obs["cloud_id"]


class _FakeConcept:
    def __init__(self, release_id, scientific_name, rank):
        self.release_id = release_id
        self.scientific_name = scientific_name
        self.rank = rank


def _sync(cloud):
    """The Observations-tab refresh mode (see .claude/rules/cloud-sync.md)."""
    return cloud_sync.sync_all(cloud, sync_images=False, materialize_remote_images=True, full_pull=False)


def _local(local_id) -> dict:
    return models.ObservationDB.get_observation(local_id)


def _is_dirty(local_id) -> bool:
    return str(_local(local_id).get("sync_status") or "").lower() == "dirty"


def _baseline(cloud_id) -> dict:
    raw = cloud_sync._load_cloud_observation_snapshot(cloud_id)
    return json.loads(raw)["observation"]


def _edit_locally(local_id, **fields):
    models.ObservationDB.update_observation(local_id, **fields)
    assert _is_dirty(local_id)


def _proven_83668() -> dict:
    return TaxonIdentity.from_taxonomy_v2_artifact(
        83668, scientific_name="Conocybe rugosa", rank="species",
    ).to_row()


def _establish_proven_identity(cloud, local_id, cloud_id):
    """Local, cloud and baseline all agree on 83668 (a completed picker pick)."""
    models.ObservationDB.update_observation(local_id, allow_nulls=False, **_proven_83668())
    assert _sync(cloud)["errors"] == []
    assert cloud.rows[cloud_id]["selected_sporely_taxon_id"] == 83668
    assert cloud.rows[cloud_id]["taxon_identity_state"] == "sporely_v2"
    baseline = _baseline(cloud_id)
    assert baseline.get(cloud_sync.TAXON_IDENTITY_SYNC_FIELD) == "sporely:83668"
    return baseline


# ── A: manual free-text rename from a proven identity ───────────────────────


def test_manual_rename_from_proven_identity_explicitly_clears_cloud(env):
    cloud, local_id, cloud_id = env
    _establish_proven_identity(cloud, local_id, cloud_id)
    rpcs_before = len(cloud.rpcs)

    # Manual/free-text rename: the desktop's own identity resolution (not
    # under test here) invalidates the former identity, leaving
    # no_identity_evidence and a new committed name.
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
    identity = TaxonIdentity.from_row(_local(local_id))
    assert identity.state == "no_identity_evidence"

    result = _sync(cloud)
    assert result["errors"] == []

    row = cloud.rows[cloud_id]
    assert row["genus"] == "Funny" and row["species"] == "brown mushroom"
    assert row["selected_sporely_taxon_id"] is None, "stale identity must be explicitly cleared"
    assert cloud.clear_rpcs_since(rpcs_before), "the atomic identification RPC must be used, not a bare PATCH"
    assert cloud.select_rpcs_since(rpcs_before) == [], "never route a clear through the picker-selection RPC"

    baseline = _baseline(cloud_id)
    assert baseline.get(cloud_sync.TAXON_IDENTITY_SYNC_FIELD) in (None, ""), baseline

    local = _local(local_id)
    assert TaxonIdentity.from_row(local).state == "no_identity_evidence"
    assert not _is_dirty(local_id)


# ── B: genus/species dropdown change from a proven identity ─────────────────


def test_genus_species_dropdown_change_explicitly_clears_cloud(env):
    cloud, local_id, cloud_id = env
    _establish_proven_identity(cloud, local_id, cloud_id)
    rpcs_before = len(cloud.rpcs)

    _edit_locally(
        local_id,
        genus="Psilocybe", species="semilanceata", common_name=None,
        sporely_taxon_id=None, taxon_identity_state="no_identity_evidence",
        taxon_identity_proof=None, taxon_identity_source_system=None,
        taxon_identity_namespace=None, taxon_identity_external_id=None,
        taxon_identity_raw_external_id=None,
        scientific_name_snapshot=None, taxon_rank_snapshot=None,
        allow_nulls=True,
    )

    result = _sync(cloud)
    assert result["errors"] == []

    row = cloud.rows[cloud_id]
    assert (row["genus"], row["species"]) == ("Psilocybe", "semilanceata")
    assert row["selected_sporely_taxon_id"] is None
    assert cloud.clear_rpcs_since(rpcs_before)


# ── C: an unrelated edit must never clear a proven identity ─────────────────


def test_unrelated_edit_keeps_identity_and_issues_no_clear(env):
    cloud, local_id, cloud_id = env
    _establish_proven_identity(cloud, local_id, cloud_id)
    rpcs_before = len(cloud.rpcs)

    _edit_locally(local_id, notes="notes A2", location="Location A2")
    result = _sync(cloud)
    assert result["errors"] == []

    row = cloud.rows[cloud_id]
    assert row["selected_sporely_taxon_id"] == 83668, "an unrelated edit must not clear identity"
    assert len(cloud.rpcs) == rpcs_before, "no RPC at all for a matching, unchanged identity"
    local = _local(local_id)
    assert TaxonIdentity.from_row(local).sporely_taxon_id == 83668
    assert local["notes"] == "notes A2"


# ── D: picker A -> B stays the ordinary identity RPC, never a clear ─────────


def test_picker_reselection_uses_the_selection_rpc_not_a_clear(env):
    cloud, local_id, cloud_id = env
    _establish_proven_identity(cloud, local_id, cloud_id)

    other = TaxonIdentity.from_taxonomy_v2_artifact(
        99001, scientific_name="Amanita muscaria", rank="species",
    ).to_row()
    _edit_locally(local_id, genus="Amanita", species="muscaria", allow_nulls=True, **other)

    result = _sync(cloud)
    assert result["errors"] == []

    row = cloud.rows[cloud_id]
    assert row["selected_sporely_taxon_id"] == 99001
    assert cloud.clear_rpcs_since() == [], "a picker re-selection is never routed through the clear RPC"
    select_rpcs = cloud.select_rpcs_since()
    assert select_rpcs and select_rpcs[-1]["p_sporely_taxon_id"] == 99001


# ── E: legacy/no-baseline identity never speculatively clears ───────────────


def test_legacy_row_with_no_baseline_identity_issues_no_clear(env):
    """A legacy row that never had a proven baseline identity must not have
    its absence of local proof read as an instruction to clear the cloud."""
    cloud, local_id, cloud_id = env
    # No prior proven identity was ever established: baseline never held a
    # `sporely:*` key. A genuine identification edit still happens (so
    # condition 3 could hold), but condition 2 (baseline proof) never does.
    _edit_locally(local_id, genus="Coprinus", species="comatus")

    result = _sync(cloud)
    assert result["errors"] == []
    assert cloud.rpcs == [], "no baseline proof of a prior selection: no speculative clear"
    row = cloud.rows[cloud_id]
    assert row.get("selected_sporely_taxon_id") is None


# ── F: contradictory cloud concept vs. committed names is not adopted ───────


def test_contradictory_cloud_identity_is_not_silently_adopted(env):
    """Cloud holds identity A (83668, Conocybe rugosa) but the observation's
    own committed names read B; a fresh pull with no local claim and no known
    baseline must not overwrite the desktop's own names to match A."""
    cloud, local_id, cloud_id = env
    _establish_proven_identity(cloud, local_id, cloud_id)

    # Simulate a fresh desktop: no stored snapshot, but the local names
    # already disagree with the cloud's bound concept (as if another client
    # wrote a contradictory name directly, bypassing the atomic RPC).
    from database.models import SettingsDB
    SettingsDB.set_setting(cloud_sync._cloud_observation_snapshot_key(cloud_id), '')

    _edit_locally(local_id, genus="Contradictory", species="name")

    result = _sync(cloud)
    # Either a review is surfaced, or the pull/push fail closed silently
    # (no adoption, no overwrite) — but the names must never be forced back
    # to Conocybe rugosa, and 83668 must never be bound onto "Contradictory
    # name" as cloud_selected_unverified.
    local = _local(local_id)
    identity = TaxonIdentity.from_row(local)
    assert not (identity.is_cloud_selected_unverified and identity.sporely_taxon_id == 83668
                and local.get("genus") == "Contradictory"), (
        "must never bind a contradictory cloud concept onto the preserved names"
    )


# ── G: external-unresolved identity is unaffected ───────────────────────────


def test_external_unresolved_identity_is_never_treated_as_a_clear_target(env):
    cloud, local_id, cloud_id = env
    _establish_proven_identity(cloud, local_id, cloud_id)
    rpcs_before = len(cloud.rpcs)

    external = TaxonIdentity.unresolved_external(
        source_system="nortaxa", namespace="nortaxa_taxon_id", external_id="53482",
        raw_external_id="NBIC:53482", scientific_name="Something else", rank="species",
    ).to_row()
    _edit_locally(local_id, genus="Something", species="else", allow_nulls=True, **external)

    result = _sync(cloud)
    assert result["errors"] == []
    assert cloud.clear_rpcs_since(rpcs_before) == [], "an external-unresolved identity must never trigger a clear"
    assert cloud.select_rpcs_since(rpcs_before) == [], "an external-unresolved identity is not a proven Sporely selection"
    row = cloud.rows[cloud_id]
    assert row["selected_sporely_taxon_id"] == 83668, (
        "unproven external evidence is not proof the prior cloud concept is wrong"
    )


# ── H: fresh-device download after the clear ─────────────────────────────────


def test_fresh_device_download_after_clear_does_not_resurrect_old_identity(env):
    cloud, local_id, cloud_id = env
    _establish_proven_identity(cloud, local_id, cloud_id)
    _edit_locally(
        local_id,
        genus="Funny", species="brown mushroom",
        sporely_taxon_id=None, taxon_identity_state="no_identity_evidence",
        taxon_identity_proof=None, taxon_identity_source_system=None,
        taxon_identity_namespace=None, taxon_identity_external_id=None,
        taxon_identity_raw_external_id=None,
        scientific_name_snapshot=None, taxon_rank_snapshot=None,
        allow_nulls=True,
    )
    assert _sync(cloud)["errors"] == []
    assert cloud.rows[cloud_id]["selected_sporely_taxon_id"] is None

    # Device B: a brand-new local database downloading this observation
    # for the first time (full pull, no push).
    import tempfile

    with tempfile.TemporaryDirectory() as tmp_dir_b:
        import pathlib
        db_path_b = pathlib.Path(tmp_dir_b) / "device_b.db"
        from database import schema as schema_module
        original_get_path = schema_module.get_database_path
        try:
            schema_module.get_database_path = lambda: db_path_b
            schema_module.init_database()
            cloud_sync.sync_all(cloud, pull_only=True, full_pull=True)
            rows = models.ObservationDB.get_all_observations()
            assert len(rows) == 1, rows
            device_b_obs = rows[0]
            assert device_b_obs["genus"] == "Funny" and device_b_obs["species"] == "brown mushroom"
            identity = TaxonIdentity.from_row(device_b_obs)
            assert identity.sporely_taxon_id != 83668, "old identity must not return"
            assert not identity.is_cloud_selected_unverified or identity.sporely_taxon_id is None
        finally:
            schema_module.get_database_path = original_get_path


# ── I: second sync converges with no repeated clear ─────────────────────────


def test_second_sync_after_clear_issues_no_repeated_clear(env):
    cloud, local_id, cloud_id = env
    _establish_proven_identity(cloud, local_id, cloud_id)
    _edit_locally(
        local_id,
        genus="Funny", species="brown mushroom",
        sporely_taxon_id=None, taxon_identity_state="no_identity_evidence",
        taxon_identity_proof=None, taxon_identity_source_system=None,
        taxon_identity_namespace=None, taxon_identity_external_id=None,
        taxon_identity_raw_external_id=None,
        scientific_name_snapshot=None, taxon_rank_snapshot=None,
        allow_nulls=True,
    )
    assert _sync(cloud)["errors"] == []
    rpcs_after_clear = len(cloud.rpcs)

    _edit_locally(local_id, notes="notes A2")
    result = _sync(cloud)
    assert result["errors"] == []
    assert len(cloud.rpcs) == rpcs_after_clear, "no repeated clear on a later unrelated sync"
    assert cloud.rows[cloud_id]["selected_sporely_taxon_id"] is None
