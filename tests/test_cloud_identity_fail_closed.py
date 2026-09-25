"""Identity disagreements with no evidence of who changed fail closed.

Review findings on the S1–S5 candidate (87ba20a):

* pull — with no stored snapshot the "cloud wins" full apply replaced a
  proven local identity with a DIFFERENT cloud one;
* push — push_all runs before pull_all; a cloud-only identity change was
  classified remote-only, yet the RPC re-asserted the stale local identity
  over it, leaving the cloud row with the new genus and the old ID.

Both directions now leave the identity alone: the pull reports a review and
stores the snapshot with an unknown identity baseline (so later pulls and the
push preflight keep classifying the disagreement as a conflict), and the push
withholds the identity so the RPC gate skips (never clears).
"""
from __future__ import annotations

import json
import sqlite3

import pytest

from database import models, schema
from utils import cloud_sync
from utils.taxon_identity import TaxonIdentity
from tests.test_cloud_sync_conflict_preflight import (  # noqa: F401  (autouse fixture)
    _StubClient,
    _baseline_remote_obs,
    _init_db,
    _isolate_side_effects,
    _mark_observation_dirty,
    _patch_connections,
    _remote_image_row,
    _seed_observation,
    _snapshot,
    _stub_snapshot_and_signature,
    _track_push_calls,
)

F = cloud_sync.TAXON_IDENTITY_SYNC_FIELD


def _proven(taxon_id):
    return TaxonIdentity.from_taxonomy_v2_artifact(taxon_id, scientific_name="Amanita muscaria", rank="species")


# ── Pull: no-snapshot full apply ─────────────────────────────────────────────


@pytest.fixture
def real_db(tmp_path, monkeypatch):
    db_path = tmp_path / "sporely.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: tmp_path / "ref.db")
    monkeypatch.setattr(schema, "get_bundled_reference_database_path", lambda: tmp_path / "bundled.db")
    monkeypatch.setattr(schema, "_migrate_reference_values", lambda *a, **k: None)
    monkeypatch.setattr(schema, "_migrate_reference_mounts_and_stains", lambda *a, **k: None)
    schema.init_database()
    monkeypatch.setattr(cloud_sync, "_installed_taxon_concept", lambda sid: None)
    return db_path


def _remote(selected, **extra):
    return {"id": "14", "date": "2026-09-15", "genus": "Conocybe", "species": "rugosa",
            "selected_sporely_taxon_id": selected, "taxon_identity_state": "sporely_v2", **extra}


def test_no_snapshot_apply_never_replaces_a_different_proven_identity(real_db):
    local_id = models.ObservationDB.create_observation(
        date="2026-09-15", genus="Conocybe", species="rugosa",
        scientific_name_snapshot="Conocybe rugosa", taxon_rank_snapshot="species",
        **_proven(83668).to_row(),
    )
    outcome = cloud_sync._apply_remote_observation_fields(
        local_id, _remote(99001), identity_fail_closed=True,
    )
    assert outcome == cloud_sync.IDENTITY_APPLY_CONFLICT
    identity = TaxonIdentity.from_row(models.ObservationDB.get_observation(local_id))
    assert identity.is_proven_sporely and identity.sporely_taxon_id == 83668


def test_no_snapshot_apply_still_adopts_when_local_has_no_claim(real_db):
    local_id = models.ObservationDB.create_observation(date="2026-09-15", genus="Pholiotina", species="rugosa")
    outcome = cloud_sync._apply_remote_observation_fields(
        local_id, _remote(99001), identity_fail_closed=True,
    )
    assert outcome == cloud_sync.IDENTITY_APPLY_APPLIED


def test_explicit_keep_cloud_resolution_is_still_cloud_wins(real_db):
    """Not fail-closed: the owner chose the cloud side."""
    local_id = models.ObservationDB.create_observation(
        date="2026-09-15", genus="Conocybe", species="rugosa", **_proven(83668).to_row(),
    )
    assert cloud_sync._apply_remote_observation_fields(local_id, _remote(99001)) == \
        cloud_sync.IDENTITY_APPLY_APPLIED


def test_snapshot_after_a_no_baseline_conflict_keeps_the_disagreement_detectable():
    """Stored without identity, the next pull and the push preflight still
    see a conflict instead of a local-only change the RPC would push."""
    stored = cloud_sync._cloud_observation_snapshot(
        cloud_sync._remote_row_without_identity(_remote(99001)), [], [],
    )
    baseline = cloud_sync._baseline_observation_compare_payload(json.loads(stored)["observation"])
    assert F not in baseline
    local = {"id": 1, "cloud_id": "14", "genus": "Conocybe", "species": "rugosa", **_proven(83668).to_row()}
    changes = cloud_sync._analyze_observation_field_changes(local, _remote(99001), baseline)
    assert F in changes["conflict_fields"]


# ── Push ─────────────────────────────────────────────────────────────────────


def _add_identity_columns(db_path, identity: TaxonIdentity):
    conn = sqlite3.connect(db_path)
    try:
        for column in ("sporely_taxon_id INTEGER", *(f"{c} TEXT" for c in cloud_sync._TAXON_IDENTITY_COLUMNS)):
            conn.execute(f"ALTER TABLE observations ADD COLUMN {column}")
        row = identity.to_row()
        conn.execute(
            f"UPDATE observations SET {', '.join(f'{k} = ?' for k in row)} WHERE id = 1",
            tuple(row.values()),
        )
        conn.commit()
    finally:
        conn.close()


def _push_setup(monkeypatch, tmp_path, *, stored_identity_selected, remote_selected, with_snapshot=True):
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"bytes")
    _seed_observation(db_path, image_path)
    _add_identity_columns(db_path, _proven(83668))

    baseline_remote = {**_baseline_remote_obs(), "selected_sporely_taxon_id": stored_identity_selected,
                       "taxon_identity_state": "sporely_v2"}
    image = _remote_image_row()
    stored_snapshot = _snapshot(baseline_remote, [image]) if with_snapshot else ""
    signature = cloud_sync._local_cloud_media_signature(1)

    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE observations SET notes = 'local edit' WHERE id = 1")
    conn.commit()
    conn.close()
    _mark_observation_dirty(db_path)

    live_remote = {**_baseline_remote_obs(), "genus": "Pholiotina",
                   "selected_sporely_taxon_id": remote_selected, "taxon_identity_state": "sporely_v2"}
    _stub_snapshot_and_signature(monkeypatch, stored_snapshot=stored_snapshot, baseline_signature=signature)
    _track_push_calls(monkeypatch)
    client = _StubClient(live_remote, [image])
    result = cloud_sync.push_all(
        client, remote_obs=[dict(live_remote)], sync_images=True, sync_calibrations=False,
        prepare_images_cb=lambda obs, progress_cb: ([], None, []),
    )
    return client, result


def test_push_does_not_reassert_a_stale_identity_over_a_cloud_only_change(monkeypatch, tmp_path):
    """Web re-identified 83668 → 99 (and Amanita → Pholiotina); the desktop
    only edited notes. The push must not carry 83668 to the RPC."""
    client, result = _push_setup(monkeypatch, tmp_path, stored_identity_selected=83668, remote_selected=99)
    assert client.push_observation_calls, "the unrelated local edit is still pushed"
    pushed = client.push_observation_calls[0]
    assert pushed["notes"] == "local edit"
    assert pushed["genus"] == "Pholiotina"
    assert TaxonIdentity.from_row(pushed).is_proven_sporely is False, \
        "no identity reaches set_observation_selected_taxon_v2"
    assert not [e for e in result.get("errors") or [] if "needs review" in str(e)]


def test_push_without_a_baseline_withholds_a_disagreeing_identity(monkeypatch, tmp_path):
    client, result = _push_setup(
        monkeypatch, tmp_path, stored_identity_selected=None, remote_selected=99, with_snapshot=False,
    )
    pushed = client.push_observation_calls[0]
    assert TaxonIdentity.from_row(pushed).is_proven_sporely is False
    assert any("taxon identity" in str(e) for e in result.get("errors") or [])


def test_push_still_asserts_a_desktop_pick_the_cloud_lacks(monkeypatch, tmp_path):
    """The 917 case: proven local pick, cloud has no identity yet."""
    client, result = _push_setup(
        monkeypatch, tmp_path, stored_identity_selected=None, remote_selected=None,
    )
    pushed = client.push_observation_calls[0]
    assert TaxonIdentity.from_row(pushed).sporely_taxon_id == 83668
    assert TaxonIdentity.from_row(pushed).is_proven_sporely


def test_missing_column_fallback_ignores_the_echoed_select_list():
    class _Client(cloud_sync.SporelyCloudClient):
        def __init__(self):
            self.user_id = "u"

        def _get(self, path):
            # The error echoes the path (which lists taxon_identity_* columns)
            # but concerns something else entirely.
            raise cloud_sync.CloudSyncError(f"GET {path}: relation \"x\" does not exist")

    client = _Client()
    with pytest.raises(cloud_sync.CloudSyncError):
        client.get_observation("14")
    assert not getattr(client, "_observation_identity_columns_unsupported", False)
