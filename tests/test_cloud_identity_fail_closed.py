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
        # The real schema's identity + snapshot columns; the preflight
        # harness schema predates them.
        for column in ("sporely_taxon_id INTEGER", "scientific_name_snapshot TEXT",
                       "taxon_rank_snapshot TEXT", "updated_at TEXT",
                       *(f"{c} TEXT" for c in cloud_sync._TAXON_IDENTITY_COLUMNS)):
            conn.execute(f"ALTER TABLE observations ADD COLUMN {column}")
        row = identity.to_row()
        conn.execute(
            f"UPDATE observations SET {', '.join(f'{k} = ?' for k in row)} WHERE id = 1",
            tuple(row.values()),
        )
        conn.commit()
    finally:
        conn.close()


class _SnapshotStore:
    """The real snapshot bookkeeping, held in memory.

    The preflight harness stubs `_store_remote_snapshot` to a no-op, which is
    exactly why the first fix looked sufficient: what the post-push snapshot
    records as baseline decides what the NEXT sync does.
    """

    def __init__(self, initial: str):
        self.value = initial

    def install(self, monkeypatch):
        monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda _cid: self.value)

        def _store(client, cloud_id, remote=None, remote_images=None, remote_measurements=None, **_kw):
            remote_row = remote or client.get_observation(cloud_id)
            images = remote_images if remote_images is not None else client.pull_image_metadata(cloud_id)
            self.value = cloud_sync._cloud_observation_snapshot(remote_row, images, remote_measurements or [])

        monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", _store)


def _push_env(monkeypatch, tmp_path, *, baseline_selected, remote_selected, with_snapshot=True):
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    # Not beside the database: adopting a cloud genus renames the
    # observation folder inferred from this path, as a real pull would.
    image_path = tmp_path / "media" / "image.jpg"
    image_path.parent.mkdir()
    image_path.write_bytes(b"bytes")
    _seed_observation(db_path, image_path)
    _add_identity_columns(db_path, _proven(83668))

    image = _remote_image_row()
    baseline_remote = {**_baseline_remote_obs(), "selected_sporely_taxon_id": baseline_selected,
                       "taxon_identity_state": "sporely_v2" if baseline_selected else None}
    signature = cloud_sync._local_cloud_media_signature(1)
    _stub_snapshot_and_signature(monkeypatch, stored_snapshot="", baseline_signature=signature)
    store = _SnapshotStore(_snapshot(baseline_remote, [image]) if with_snapshot else "")
    store.install(monkeypatch)
    _track_push_calls(monkeypatch)
    live_remote = {**_baseline_remote_obs(), "genus": "Pholiotina" if remote_selected != baseline_selected else "Amanita",
                   "selected_sporely_taxon_id": remote_selected,
                   "taxon_identity_state": "sporely_v2" if remote_selected else None}
    client = _StubClient(live_remote, [image])
    return db_path, client, live_remote, store


def _edit_and_push(db_path, client, live_remote, notes):
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE observations SET notes = ? WHERE id = 1", (notes,))
    conn.commit()
    conn.close()
    _mark_observation_dirty(db_path)
    return cloud_sync.push_all(
        client, remote_obs=[dict(live_remote)], sync_images=True, sync_calibrations=False,
        prepare_images_cb=lambda obs, progress_cb: ([], None, []),
    )


def _local_identity(db_path) -> TaxonIdentity:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return TaxonIdentity.from_row(dict(conn.execute("SELECT * FROM observations WHERE id = 1").fetchone()))
    finally:
        conn.close()


def test_cloud_only_identity_change_is_adopted_and_never_reverted_by_a_later_push(monkeypatch, tmp_path):
    """Web re-identified 83668 → 99; the desktop only edits notes, twice."""
    db_path, client, live, store = _push_env(monkeypatch, tmp_path, baseline_selected=83668, remote_selected=99)

    _edit_and_push(db_path, client, live, "first local edit")
    first = client.push_observation_calls[-1]
    assert first["notes"] == "first local edit"
    assert TaxonIdentity.from_row(first).is_proven_sporely is False, "no RPC for the stale 83668"
    local = _local_identity(db_path)
    assert (local.source_system, local.external_id) == ("sporely", "99"), "adopted from the cloud"
    assert local.sporely_taxon_id is None, "99 is not in the (absent) local artifact: preserved, not bound"
    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT genus FROM observations WHERE id = 1").fetchone() == ("Pholiotina",), \
        "the cloud-only genus is adopted with the identity"
    conn.close()

    _edit_and_push(db_path, client, live, "second local edit")
    second = client.push_observation_calls[-1]
    assert second["notes"] == "second local edit"
    assert TaxonIdentity.from_row(second).is_proven_sporely is False, \
        "the next push must not re-assert 83668 either"


def test_no_baseline_disagreement_stays_under_review_and_blocks_the_next_push(monkeypatch, tmp_path):
    db_path, client, live, store = _push_env(
        monkeypatch, tmp_path, baseline_selected=None, remote_selected=99, with_snapshot=False,
    )
    result = _edit_and_push(db_path, client, live, "first local edit")
    first = client.push_observation_calls[-1]
    assert TaxonIdentity.from_row(first).is_proven_sporely is False
    assert any("taxon identity" in str(e) for e in result.get("errors") or [])
    assert _local_identity(db_path).sporely_taxon_id == 83668, "local claim untouched"
    conn = sqlite3.connect(db_path)
    status, reason = conn.execute("SELECT sync_status, sync_blocked_reason FROM observations WHERE id = 1").fetchone()
    conn.close()
    assert status == "dirty" and reason == cloud_sync.CONFLICT_REVIEW_PENDING_MARKER
    assert F not in json.loads(store.value)["observation"], "baseline identity stays unknown"

    calls_before = len(client.push_observation_calls)
    result = _edit_and_push(db_path, client, live, "second local edit")
    assert len(client.push_observation_calls) == calls_before, "the preflight blocks the push"
    assert any("needs review" in str(e) for e in result.get("errors") or [])


def test_push_still_asserts_a_desktop_pick_the_cloud_lacks(monkeypatch, tmp_path):
    """The 917 case: proven local pick, cloud has no identity yet."""
    db_path, client, live, store = _push_env(monkeypatch, tmp_path, baseline_selected=None, remote_selected=None)
    _edit_and_push(db_path, client, live, "local edit")
    pushed = client.push_observation_calls[-1]
    assert TaxonIdentity.from_row(pushed).is_proven_sporely
    assert TaxonIdentity.from_row(pushed).sporely_taxon_id == 83668


def test_three_way_cloud_clear_is_adopted_not_reasserted(real_db):
    """Baseline 83668, cloud now none, same names: the clear is evidence."""
    local_id = models.ObservationDB.create_observation(
        date="2026-09-15", genus="Conocybe", species="rugosa",
        **TaxonIdentity.from_cloud_selection(83668, local_release_id="r").to_row(),
    )
    cloud_sync._apply_remote_observation_fields(
        local_id, _remote(None, taxon_identity_state=None), fields={F},
    )
    assert TaxonIdentity.from_row(models.ObservationDB.get_observation(local_id)).sporely_taxon_id is None


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
