"""Taxonomy identity takes part in pull/push change and conflict detection.

Adopting the cloud identity on pull is only safe if an identity-only cloud
change is visible to the three-way comparison and a disagreement fails closed.
The identity travels as the virtual observation field ``taxon_identity``:

* the stored snapshot records the cloud identity key;
* ``_classify_identity_sync_change`` decides remote-only / local-only /
  conflict / shared against that baseline, with its own rules for snapshots
  stored before identity joined change detection ("unknown baseline");
* an identity follows the identification it names, so a cloud identity change
  against a locally edited genus/species is a conflict;
* only a proven local identity counts as a local change to push (the RPC gate
  accepts nothing else), so unpushable local states never loop.
"""
from __future__ import annotations

import json

import pytest

from database import models, schema
from database.taxon_lookup import InstalledTaxonConcept
from utils import cloud_sync
from utils.taxon_identity import PROOF_CLOUD_SELECTED_UNVERIFIED, TaxonIdentity

F = cloud_sync.TAXON_IDENTITY_SYNC_FIELD
RELEASE = "tax-2026.09.23-01"


# ── Row builders ─────────────────────────────────────────────────────────────


def _local(identity: TaxonIdentity | None = None, *, genus="Conocybe", species="rugosa", **extra):
    row = {"id": 917, "cloud_id": "14", "genus": genus, "species": species}
    if identity is not None:
        row.update(identity.to_row())
    row.update(extra)
    return row


def _proven(taxon_id):
    return TaxonIdentity.from_taxonomy_v2_artifact(taxon_id, scientific_name="X y", rank="species")


def _cloud_selected(taxon_id):
    return TaxonIdentity.from_cloud_selection(taxon_id, local_release_id=RELEASE)


_NBIC = TaxonIdentity.from_prefixed_external_id("NBIC:53482", scientific_name="Entoloma conferendum")


def _legacy(taxon_id):
    return {"sporely_taxon_id": taxon_id}


def _remote(selected=None, *, genus="Conocybe", species="rugosa", state="unset", **extra):
    row = {"id": "14", "genus": genus, "species": species, "selected_sporely_taxon_id": selected}
    row["taxon_identity_state"] = ("sporely_v2" if selected else None) if state == "unset" else state
    row.update(extra)
    return row


UNKNOWN = object()


def _baseline(key=UNKNOWN):
    return {} if key is UNKNOWN else {F: key}


def _classify(local, remote, baseline, *, names_owned=False):
    return cloud_sync._classify_identity_sync_change(
        local, remote, baseline, identification_locally_owned=names_owned,
    )


# ── Classifier ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize("label, local, remote, baseline, names_owned, expected", [
    # Unknown baseline: a snapshot stored before identity joined detection.
    ("desktop pick awaiting the RPC", _local(_proven(83668)), _remote(None), _baseline(), False, "local_only"),
    ("two different claims disagree", _local(_proven(83668)), _remote(99), _baseline(), False, "conflict"),
    ("existing device, legacy 52369 → cloud 83668", _local(**_legacy(52369)), _remote(83668), _baseline(), False, "remote_only"),
    ("no local identity → cloud 83668", _local(), _remote(83668), _baseline(), False, "remote_only"),
    ("locally edited identification is not overwritten", _local(), _remote(83668), _baseline(), True, None),
    ("nothing anywhere", _local(), _remote(None), _baseline(), False, None),
    ("unpushable local claim, nothing in cloud", _local(_NBIC), _remote(None), _baseline(), False, None),
    ("already equal", _local(_proven(83668)), _remote(83668), _baseline(), False, None),
    ("legacy with nothing in cloud stays", _local(**_legacy(52369)), _remote(None), _baseline(), False, None),
    # Known baseline.
    ("web re-identified, desktop untouched", _local(_proven(83668)), _remote(99), _baseline("sporely:83668"), False, "remote_only"),
    ("cloud token follows a cloud change", _local(_cloud_selected(83668)), _remote(99), _baseline("sporely:83668"), False, "remote_only"),
    ("desktop re-picked, cloud untouched", _local(_proven(99)), _remote(83668), _baseline("sporely:83668"), False, "local_only"),
    ("both changed differently", _local(_proven(99)), _remote(77), _baseline("sporely:83668"), False, "conflict"),
    ("both changed to the same", _local(_proven(99)), _remote(99), _baseline("sporely:83668"), False, "shared"),
    ("cloud identity changed against local name edits", _local(), _remote(99), _baseline("sporely:83668"), True, "conflict"),
    ("unpushable local claim never loops", _local(_NBIC), _remote(None), _baseline(""), False, None),
    ("cloud token, cloud unchanged", _local(_cloud_selected(83668)), _remote(83668), _baseline("sporely:83668"), False, None),
    ("unconfirmable cloud id preserved: no perpetual diff",
     _local(TaxonIdentity.unresolved_external(source_system="sporely", namespace="sporely_taxon_id", external_id="999001")),
     _remote(999001), _baseline("sporely:999001"), False, None),
])
def test_identity_classification(label, local, remote, baseline, names_owned, expected):
    assert _classify(local, remote, baseline, names_owned=names_owned) == expected, label


def test_a_row_without_identity_columns_is_never_classified():
    remote = {"id": "14", "genus": "Conocybe", "species": "rugosa"}
    assert _classify(_local(_proven(83668)), remote, _baseline("sporely:99")) is None


# ── Real change-detection functions ──────────────────────────────────────────


def _snapshot_of(remote) -> dict:
    return json.loads(cloud_sync._cloud_observation_snapshot(remote, [], []))


def test_snapshot_records_the_cloud_identity_key():
    assert _snapshot_of(_remote(83668))["observation"][F] == "sporely:83668"
    assert _snapshot_of(_remote(None))["observation"][F] == ""
    legacy_server_row = {"id": "14", "genus": "Conocybe", "species": "rugosa"}
    assert F not in _snapshot_of(legacy_server_row)["observation"]


def test_identity_only_cloud_change_is_a_meaningful_remote_change():
    stored = cloud_sync._cloud_observation_snapshot(_remote(83668), [], [])
    assert cloud_sync._remote_snapshot_has_meaningful_changes(_remote(83668), [], [], stored) is False
    assert cloud_sync._remote_snapshot_has_meaningful_changes(_remote(99), [], [], stored) is True


def test_pre_identity_snapshot_reconciles_once_then_is_quiet():
    old = json.dumps({"schema_version": 2, "observation": {
        k: v for k, v in _snapshot_of(_remote(83668))["observation"].items() if k != F
    }})
    assert cloud_sync._remote_snapshot_has_meaningful_changes(_remote(83668), [], [], old) is True
    new = cloud_sync._cloud_observation_snapshot(_remote(83668), [], [])
    assert cloud_sync._remote_snapshot_has_meaningful_changes(_remote(83668), [], [], new) is False


def test_three_way_analysis_reports_identity_with_the_identification():
    """The existing-device 917 pull: names and identity move together."""
    baseline = cloud_sync._baseline_observation_compare_payload(
        {"id": "14", "genus": "Pholiotina", "species": "rugosa"}
    )
    changes = cloud_sync._analyze_observation_field_changes(
        _local(genus="Pholiotina", **_legacy(52369)), _remote(83668), baseline,
    )
    assert {"genus", F} <= set(changes["remote_only_fields"])
    assert F not in changes["conflict_fields"]


def test_three_way_analysis_fails_closed_on_disagreement():
    baseline = cloud_sync._baseline_observation_compare_payload(
        _snapshot_of(_remote(83668))["observation"]
    )
    changes = cloud_sync._analyze_observation_field_changes(
        _local(_proven(99)), _remote(77), baseline,
    )
    assert F in changes["conflict_fields"]
    assert cloud_sync._remaining_local_changes_after_remote_merge(
        changes, local_media_changed=False,
    ), "a conflict keeps the observation pending review"


def test_baseline_payload_keeps_unknown_distinct_from_empty():
    assert F not in cloud_sync._baseline_observation_compare_payload({"id": "14"})
    assert cloud_sync._baseline_observation_compare_payload({"id": "14", F: ""})[F] == ""
    # Idempotent: callers pass an already-normalised baseline back in.
    once = cloud_sync._baseline_observation_compare_payload({"id": "14", F: "sporely:1"})
    assert cloud_sync._baseline_observation_compare_payload(once)[F] == "sporely:1"


@pytest.mark.parametrize("identity, baseline_key, expected", [
    (_proven(83668), "", True),          # picked on desktop, not yet asserted
    (_proven(83668), UNKNOWN, True),     # pre-identity snapshot: still has to go out
    (_proven(83668), "sporely:83668", False),
    (_cloud_selected(83668), "", False), # cloud-derived: nothing to push
    (_NBIC, "", False),                  # unpushable: never keeps a row dirty
])
def test_local_real_changes_count_only_a_pushable_identity(monkeypatch, identity, baseline_key, expected):
    snapshot_obs = {**_snapshot_of(_remote(None))["observation"]}
    if baseline_key is UNKNOWN:
        snapshot_obs.pop(F)
    else:
        snapshot_obs[F] = baseline_key
    stored = json.dumps({"schema_version": 2, "observation": snapshot_obs})
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda _cid: stored)
    # Media and measurements report "no change", so the identity is the only
    # thing that can make the answer True.
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda _id: "sig")
    monkeypatch.setattr(cloud_sync, "_local_cloud_media_signature", lambda _id: "")
    local = _local(identity)
    local.update({k: v for k, v in snapshot_obs.items() if k not in {"id", "desktop_id", F}})
    local["id"], local["cloud_id"] = 917, "14"

    assert cloud_sync._local_has_real_changes_since_snapshot(local, "14") is expected

    # Control: the same row with no identity has no real change.
    bare = {k: v for k, v in local.items() if not k.startswith("taxon_identity") and k != "sporely_taxon_id"}
    assert cloud_sync._local_has_real_changes_since_snapshot(bare, "14") is False


# ── Applying a remote-only identity change ───────────────────────────────────


@pytest.fixture
def real_db(tmp_path, monkeypatch):
    db_path = tmp_path / "sporely.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: tmp_path / "ref.db")
    monkeypatch.setattr(schema, "get_bundled_reference_database_path", lambda: tmp_path / "bundled.db")
    monkeypatch.setattr(schema, "_migrate_reference_values", lambda *a, **k: None)
    monkeypatch.setattr(schema, "_migrate_reference_mounts_and_stains", lambda *a, **k: None)
    schema.init_database()
    concepts = {
        83668: InstalledTaxonConcept(83668, RELEASE, "Conocybe rugosa", "species"),
        99: InstalledTaxonConcept(99, RELEASE, "Conocybe rugosa", "species"),
    }
    monkeypatch.setattr(cloud_sync, "_installed_taxon_concept", lambda sid: concepts.get(int(sid)))
    return db_path


def test_identity_only_remote_change_is_applied_through_the_virtual_field(real_db):
    local_id = models.ObservationDB.create_observation(
        date="2026-09-15", genus="Conocybe", species="rugosa",
        scientific_name_snapshot="Conocybe rugosa", taxon_rank_snapshot="species",
        **_cloud_selected(83668).to_row(),
    )
    baseline = cloud_sync._baseline_observation_compare_payload(
        _snapshot_of(_remote(83668))["observation"]
    )
    local = models.ObservationDB.get_observation(local_id)
    changes = cloud_sync._analyze_observation_field_changes(local, _remote(99), baseline)
    assert changes["remote_only_fields"] == [F]

    cloud_sync._apply_remote_observation_fields(
        local_id, _remote(99), fields=set(changes["remote_only_fields"]),
    )
    identity = TaxonIdentity.from_row(models.ObservationDB.get_observation(local_id))
    assert identity.sporely_taxon_id == 99
    assert identity.identity_proof == PROOF_CLOUD_SELECTED_UNVERIFIED


# ── Conflict resolution ──────────────────────────────────────────────────────


def test_keep_local_identity_resolution_uses_the_rpc_gate_never_a_column_patch(monkeypatch):
    from tests.test_cloud_conflict_plan_execution import (
        _RecordingClient, _baseline_from_state, _patch_common,
    )

    local_obs = {**_local(_proven(99)), "id": 1, "cloud_id": "14"}
    remote_obs = _remote(77)
    baseline = _baseline_from_state(local_obs, remote_obs, [], [], [], [])
    _patch_common(monkeypatch, local_obs=local_obs, remote_obs=remote_obs)

    class _Client(_RecordingClient):
        def __init__(self, **kw):
            super().__init__(**kw)
            self.rpc_calls = []

        def set_observation_selected_taxon(self, cloud_id, sporely_taxon_id):
            self.rpc_calls.append((cloud_id, sporely_taxon_id))

        _sync_observation_selected_taxon = cloud_sync.SporelyCloudClient._sync_observation_selected_taxon

    client = _Client(remote_obs=remote_obs)
    cloud_sync.resolve_conflict_plan(client, 1, plan={
        "baseline": baseline,
        "items": [{"kind": "field", "field": F, "choice": "local"}],
    })
    assert client.rpc_calls == [("14", 99)]
    assert all(F not in payload for _path, payload in client.patches)


@pytest.mark.parametrize("local_identity, remote_selected, expected", [
    (_proven(99), 77, "conflict"),              # both changed differently
    (_cloud_selected(83668), 99, "pull_cloud"), # cloud-only change
    (_proven(99), 83668, "push_local"),         # desktop-only change
])
def test_conflict_review_classifies_identity_with_its_own_rule(monkeypatch, local_identity,
                                                                remote_selected, expected):
    from tests.test_cloud_conflict_plan_execution import (
        _make_get_conflict_detail_env, _snapshot_json,
    )

    baseline_obs = {"id": "14", "genus": "Conocybe", "species": "rugosa", F: "sporely:83668"}
    local_obs = {**_local(local_identity), "id": 1}
    client = _make_get_conflict_detail_env(
        monkeypatch, local_obs=local_obs, remote_obs=_remote(remote_selected),
        snapshot=_snapshot_json(observation=baseline_obs),
    )
    detail = cloud_sync.get_conflict_detail(client, 1, "14")
    rows = [row for row in detail["field_rows"] if row["field"] == F]
    auto = [d for d in detail["automatic_decisions"]["fields"] if d["field"] == F]
    if expected == "conflict":
        assert [row["label"] for row in rows] == ["Taxon identity"]
        assert rows[0]["local"] == "sporely:99" and rows[0]["remote"] == "sporely:77"
        assert detail["has_manual_conflicts"]
    else:
        assert rows == []
        assert [d["action"] for d in auto] == [expected]
