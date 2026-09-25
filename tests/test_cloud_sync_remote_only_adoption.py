"""A cloud-only observation edit accepted by push is persisted locally.

Sync-integrity follow-up 3 (docs/plans/active/2026-09-25-sync-integrity-follow-ups.md).

`sync_all` pushes before it pulls. When the push preflight classifies an
ordinary field as remote-only it keeps the cloud value in the outgoing
payload, and the post-push snapshot records that value as the baseline. The
local row used to keep the pre-sync value, so a later unrelated local edit made
the stale value read as a local-only change and pushed it back over the cloud
edit. The intermediate pull cannot repair it: the stored baseline already
equals the cloud value, so the pull sees nothing to apply.

These tests drive the real `sync_all` (push, then pull) against the real
SQLite schema and real snapshot bookkeeping. Only the network is replaced: a
`SporelyCloudClient` subclass whose PATCH/POST mutate one in-memory cloud row
and bump `updated_at` like the server trigger. The linked state is created by
an ordinary first sync, not hand-seeded.
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
    base = datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc)
    return (base + timedelta(seconds=next(_TICK))).isoformat()


class _FakeCloud(cloud_sync.SporelyCloudClient):
    """One user's `observations` table, held in memory.

    Reads return copies; writes go through the real `push_observation` and
    land in `_patch`/`_post`, which apply the payload and bump `updated_at`.
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
        self.rpcs.append((function_name, dict(payload or {})))
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
    # App settings (linked account, child-change cursor) live in the user's
    # profile, not the database: keep them per-test and in memory.
    app_settings: dict = {}
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(app_settings))
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda values: app_settings.update(values))

    # Subsystems outside observation metadata (their own suites cover them).
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
        date="2026-09-20", genus="Agaricus", species="campestris",
        notes="notes A", location="Location A", habitat="Habitat A",
    )
    _sync(cloud)  # ordinary first sync: POST, snapshot, media signature
    obs = models.ObservationDB.get_observation(local_id)
    assert obs["cloud_id"] and obs["sync_status"] == "synced", obs
    assert not _is_dirty(local_id)
    return cloud, local_id, obs["cloud_id"]


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


# ── Cases 1 and 2: the reproduced defect ────────────────────────────────────


@pytest.mark.parametrize(
    ("field", "old", "new"),
    [("notes", "notes A", "notes B"), ("location", "Location A", "Location B")],
)
def test_cloud_only_edit_is_persisted_locally_and_never_reverted(env, field, old, new):
    cloud, local_id, cloud_id = env
    # Device B edits the field; device A has an unrelated real local edit
    # (a clean local row would not be pushed at all — the pull applies it).
    cloud.edit_from_other_device(cloud_id, **{field: new})
    _edit_locally(local_id, habitat="Habitat A2")

    result = _sync(cloud)
    assert result["errors"] == []
    assert cloud.rows[cloud_id][field] == new, "the push kept the cloud edit"
    assert cloud.rows[cloud_id]["habitat"] == "Habitat A2", "the local edit went out"
    assert _baseline(cloud_id)[field] == new
    assert _local(local_id)[field] == new, (
        f"local {field} must equal the accepted cloud value and the new baseline"
    )
    assert not _is_dirty(local_id)

    # A later unrelated local edit must not resurrect the pre-sync value.
    _edit_locally(local_id, habitat="Habitat A3")
    result = _sync(cloud)
    assert result["errors"] == []
    assert cloud.rows[cloud_id]["habitat"] == "Habitat A3"
    assert cloud.rows[cloud_id][field] == new, f"stale local {old!r} was pushed back"
    assert _local(local_id)[field] == new
    assert _baseline(cloud_id)[field] == new


def test_extra_value_fields_are_adopted_and_not_overwritten_by_the_same_push(env):
    """Remote-only fields outside `_remote_observation_update_kwargs`
    (`_remote_observation_extra_values`) were not even kept in the payload."""
    cloud, local_id, cloud_id = env
    cloud.edit_from_other_device(cloud_id, inaturalist_id=4242, author="Other device")
    _edit_locally(local_id, habitat="Habitat A2")

    assert _sync(cloud)["errors"] == []
    assert cloud.rows[cloud_id]["inaturalist_id"] == 4242
    assert cloud.rows[cloud_id]["author"] == "Other device"
    local = _local(local_id)
    assert (local["inaturalist_id"], local["author"]) == (4242, "Other device")


# ── Case 3: a genuine local change still wins ───────────────────────────────


def test_genuine_local_only_change_is_pushed_not_overwritten(env):
    cloud, local_id, cloud_id = env
    cloud.edit_from_other_device(cloud_id, location="Location B")
    _edit_locally(local_id, notes="notes A2")

    assert _sync(cloud)["errors"] == []
    assert cloud.rows[cloud_id]["notes"] == "notes A2"
    assert _local(local_id)["notes"] == "notes A2"
    assert _baseline(cloud_id)["notes"] == "notes A2"
    assert _local(local_id)["location"] == "Location B"


# ── Case 4: both sides changed the same field ───────────────────────────────


def test_both_sides_changed_is_still_a_blocked_review_not_a_cloud_win(env, monkeypatch):
    cloud, local_id, cloud_id = env
    cloud.edit_from_other_device(cloud_id, notes="notes B", location="Location B")
    _edit_locally(local_id, notes="notes A2")
    patches_before = len(cloud.patches)
    push_adoptions = []
    real_apply = cloud_sync._apply_remote_observation_fields
    real_push_all = cloud_sync.push_all

    def _tracking_push_all(*a, **k):
        monkeypatch.setattr(
            cloud_sync, "_apply_remote_observation_fields",
            lambda *aa, **kk: push_adoptions.append(kk.get("fields")) or real_apply(*aa, **kk),
        )
        try:
            return real_push_all(*a, **k)
        finally:
            monkeypatch.setattr(cloud_sync, "_apply_remote_observation_fields", real_apply)

    monkeypatch.setattr(cloud_sync, "push_all", _tracking_push_all)

    result = _sync(cloud)
    assert any("review" in str(e).lower() for e in result["errors"]), result["errors"]
    assert len(cloud.patches) == patches_before, "the preflight blocks the push"
    assert push_adoptions == [], "a blocked push adopts nothing locally"
    assert cloud.rows[cloud_id]["notes"] == "notes B"
    local = _local(local_id)
    assert local["notes"] == "notes A2", "the local side of a conflict is kept"
    assert _is_dirty(local_id)
    assert _baseline(cloud_id)["notes"] == "notes A", "the conflict stays detectable"
    # The pull still merges the non-overlapping cloud edit (existing contract).
    assert local["location"] == "Location B"


# ── Failure between local adoption and the cloud write ──────────────────────


def test_failed_push_after_adoption_converges_on_the_next_sync(env, monkeypatch):
    """Adoption commits before the PATCH. If the PATCH fails, local and cloud
    already agree on the adopted value, which three-way reads as shared."""
    cloud, local_id, cloud_id = env
    cloud.edit_from_other_device(cloud_id, notes="notes B")
    _edit_locally(local_id, habitat="Habitat A2")
    real_patch = cloud._patch

    def _failing_patch(path, payload):
        raise cloud_sync.CloudSyncError("PATCH observations failed: 500")

    monkeypatch.setattr(cloud, "_patch", _failing_patch)
    result = _sync(cloud)
    assert result["errors"], "the failure is reported"
    assert cloud.rows[cloud_id]["habitat"] == "Habitat A"
    # Whatever baseline the pull recorded, local and cloud agree on notes.
    assert _local(local_id)["notes"] == cloud.rows[cloud_id]["notes"] == "notes B"
    assert _is_dirty(local_id), "the local habitat edit is still pending"
    assert _local(local_id)["habitat"] == "Habitat A2"

    monkeypatch.setattr(cloud, "_patch", real_patch)
    result = _sync(cloud)
    assert result["errors"] == []
    assert cloud.rows[cloud_id]["notes"] == "notes B"
    assert cloud.rows[cloud_id]["habitat"] == "Habitat A2"
    assert _local(local_id)["notes"] == "notes B"
    assert _baseline(cloud_id)["notes"] == "notes B"
    assert not _is_dirty(local_id)


# ── Case 5: convergence ─────────────────────────────────────────────────────


def test_second_sync_with_nothing_changed_writes_nothing(env):
    cloud, local_id, cloud_id = env
    cloud.edit_from_other_device(cloud_id, notes="notes B")
    _edit_locally(local_id, habitat="Habitat A2")
    assert _sync(cloud)["errors"] == []
    local_after, cloud_after, baseline_after = _local(local_id), dict(cloud.rows[cloud_id]), _baseline(cloud_id)
    writes_after = cloud.writes

    result = _sync(cloud)
    assert result["errors"] == []
    assert cloud.writes == writes_after, "no semantic cloud writes"
    assert cloud.rows[cloud_id] == cloud_after
    assert _baseline(cloud_id) == baseline_after
    assert {k: _local(local_id)[k] for k in ("notes", "habitat", "location", "sync_status")} == \
        {k: local_after[k] for k in ("notes", "habitat", "location", "sync_status")}


def test_no_local_write_when_the_remote_only_field_already_matches_local(env, monkeypatch):
    """Nothing remote-only, nothing adopted: the apply helper is not called."""
    cloud, local_id, cloud_id = env
    _edit_locally(local_id, habitat="Habitat A2")
    calls = []
    real_apply = cloud_sync._apply_remote_observation_fields
    monkeypatch.setattr(
        cloud_sync, "_apply_remote_observation_fields",
        lambda *a, **k: calls.append(k.get("fields")) or real_apply(*a, **k),
    )
    assert _sync(cloud)["errors"] == []
    assert calls == []


# ── Identity is Stage C's; the adoption must not touch it ───────────────────


def test_ordinary_adoption_leaves_a_proven_local_identity_alone(env):
    cloud, local_id, cloud_id = env
    proven = TaxonIdentity.from_taxonomy_v2_artifact(83668, scientific_name="Agaricus campestris", rank="species")
    models.ObservationDB.update_observation(local_id, allow_nulls=False, **proven.to_row())
    assert _sync(cloud)["errors"] == []  # asserts the pick; baseline records it
    cloud.rows[cloud_id]["selected_sporely_taxon_id"] = 83668
    cloud.rows[cloud_id]["taxon_identity_state"] = "sporely_v2"
    assert _sync(cloud)["errors"] == []
    rpcs_before = len(cloud.rpcs)

    cloud.edit_from_other_device(cloud_id, notes="notes B")
    _edit_locally(local_id, habitat="Habitat A2")
    assert _sync(cloud)["errors"] == []

    local = _local(local_id)
    assert local["notes"] == "notes B"
    identity = TaxonIdentity.from_row(local)
    assert identity.is_proven_sporely and identity.sporely_taxon_id == 83668
    assert {k: local[k] for k in proven.to_row()} == proven.to_row()
    assert len(cloud.rpcs) == rpcs_before, "matching cloud selection: no RPC"
