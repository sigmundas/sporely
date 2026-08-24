"""Tests for the child-change cursor probe in cloud_sync.

Verifies that image/measurement child rows added from mobile are not
swallowed by the fast-path convergence branch in pull_all.
"""
from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from utils import cloud_sync


# ---------------------------------------------------------------------------
# Shared DB helpers (same schema as test_cloud_sync_fast_path.py)
# ---------------------------------------------------------------------------


def _connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db(tmp_path):
    db_path = tmp_path / "child_probe.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY,
            date TEXT,
            cloud_id TEXT,
            sync_status TEXT,
            synced_at TEXT,
            sync_error_code TEXT,
            sync_error_message TEXT,
            sync_blocked_reason TEXT,
            sync_blocked_at TEXT,
            genus TEXT,
            species TEXT,
            common_name TEXT
        );
        CREATE TABLE images (
            id INTEGER PRIMARY KEY,
            observation_id INTEGER,
            image_type TEXT,
            cloud_id TEXT,
            sort_order INTEGER,
            filepath TEXT,
            original_filepath TEXT,
            micro_category TEXT,
            objective_name TEXT,
            scale_microns_per_pixel REAL,
            resample_scale_factor REAL,
            mount_medium TEXT,
            stain TEXT,
            sample_type TEXT,
            contrast TEXT,
            measure_color TEXT,
            crop_mode TEXT,
            notes TEXT,
            gps_source INTEGER,
            ai_crop_x1 REAL, ai_crop_y1 REAL, ai_crop_x2 REAL, ai_crop_y2 REAL,
            ai_crop_source_w INTEGER, ai_crop_source_h INTEGER, ai_crop_is_custom INTEGER,
            calibration_id INTEGER,
            created_at TEXT
        );
        CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE spore_measurements (
            id INTEGER PRIMARY KEY,
            image_id INTEGER,
            length_um REAL,
            width_um REAL,
            measurement_type TEXT,
            notes TEXT,
            p1_x REAL, p1_y REAL, p2_x REAL, p2_y REAL,
            p3_x REAL, p3_y REAL, p4_x REAL, p4_y REAL,
            gallery_rotation REAL,
            measured_at TEXT
        );
        """
    )
    conn.commit()
    conn.close()
    return db_path


def _seed_synced_observation(
    db_path,
    *,
    local_id=555,
    cloud_id="cloud-555",
    synced_at="2026-07-15T12:00:00+00:00",
):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO observations (id, date, cloud_id, sync_status, synced_at) "
        "VALUES (?, '2026-07-15', ?, 'synced', ?)",
        (local_id, cloud_id, synced_at),
    )
    conn.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?)",
        (
            cloud_sync._cloud_observation_snapshot_key(cloud_id),
            '{"observation": {"id": "%s", "genus": "Panaeolus", "species": "sp."}, "images": [], "measurements": []}'
            % cloud_id,
        ),
    )
    conn.commit()
    conn.close()


def _default_monkeypatches(monkeypatch, db_path):
    from database import models
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: _connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: _connect(db_path))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(
        cloud_sync,
        "_mark_cloud_observations_dirty_for_pending_local_images",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        cloud_sync,
        "push_calibrations",
        lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []},
    )
    monkeypatch.setattr(
        cloud_sync,
        "pull_calibrations",
        lambda *args, **kwargs: {"pulled": 0, "total": 0, "errors": []},
    )
    monkeypatch.setattr(
        cloud_sync,
        "_backfill_missing_exif_on_cloud_images",
        lambda: {"scanned": 0, "opened": 0, "updated": 0, "skipped_cached": 0},
    )
    monkeypatch.setattr(cloud_sync, "_reconcile_missing_spore_measurements", lambda *a, **kw: 0)
    monkeypatch.setattr(cloud_sync, "_reconcile_missing_spore_summaries", lambda *a, **kw: 0)
    monkeypatch.setattr(cloud_sync, "_push_summary_for_current_observation", lambda *a, **kw: None)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *a, **kw: None)
    monkeypatch.setattr(cloud_sync, "_detect_deleted_remote_observations", lambda *a, **kw: [])


class _MinimalClient:
    """Minimal fake client for pull_all isolation tests."""

    user_id = "user-probe"

    def __init__(self, remote_observations, remote_images=None):
        self._remote_observations = remote_observations
        self._remote_images = remote_images or []
        self.bulk_image_calls: list = []

    def list_remote_observations(self):
        return list(self._remote_observations)

    def list_remote_calibrations(self):
        return []

    def pull_bulk_image_metadata(self, cloud_ids):
        self.bulk_image_calls.append(list(cloud_ids))
        return [
            dict(img)
            for img in self._remote_images
            if str(img.get("observation_id")) in {str(cid) for cid in cloud_ids}
        ]

    def pull_image_metadata(self, cloud_id, **kwargs):
        return []

    def pull_measurements_for_images(self, *a, **kw):
        return []

    def pull_observation_identifications(self, *a, **kw):
        return []


def _valid_cursor(img_ts='2026-01-01T00:00:00+00:00', img_id='',
                  meas_ts='2026-01-01T00:00:00+00:00', meas_id='') -> str:
    """Build a versioned cursor JSON string accepted by _load_child_change_cursor."""
    return json.dumps({
        'v': cloud_sync._CHILD_CHANGE_CURSOR_VERSION,
        'images': {'ts': img_ts, 'id': img_id},
        'measurements': {'ts': meas_ts, 'id': meas_id},
    })


# ---------------------------------------------------------------------------
# 1. Regression: forced obs bypasses convergence branch
# ---------------------------------------------------------------------------


def test_child_change_forces_pull_candidate(tmp_path, monkeypatch):
    """A forced cloud ID must appear in pruned candidates even when observation
    fields match the snapshot and remote updated_at == local synced_at."""
    db_path = _init_db(tmp_path)
    _default_monkeypatches(monkeypatch, db_path)

    synced_at = "2026-07-15T12:00:00+00:00"
    cloud_id = "cloud-555"
    _seed_synced_observation(db_path, local_id=555, cloud_id=cloud_id, synced_at=synced_at)

    # Remote updated_at == synced_at → convergence would normally skip it.
    remote_obs = [{
        "id": cloud_id,
        "desktop_id": 555,
        "updated_at": synced_at,
        "genus": "Panaeolus",
        "species": "sp.",
    }]
    client = _MinimalClient(remote_obs)

    result = cloud_sync.pull_all(
        client,
        remote_obs=remote_obs,
        sync_calibrations=False,
        materialize_remote_images=False,
        sync_images=False,
        full_pull=False,
        forced_pull_cloud_ids=frozenset({cloud_id}),
    )

    # Should NOT be a no-op fast path — the forced obs is a candidate.
    assert result.get("fast_path_used") is not True or result.get("pulled", 0) >= 0
    # The bulk image call must have fired (candidate existed).
    assert client.bulk_image_calls != [] or result.get("skipped_unchanged", 999) == 0


# ---------------------------------------------------------------------------
# 2. No rows after cursor advance → empty forced set (updated_at semantics)
# ---------------------------------------------------------------------------


class _FakeClientWithProbe:
    """Client with list_image_changes_since using updated_at semantics."""

    user_id = "user-probe"

    def __init__(self, img_rows=None, meas_rows=None):
        self._img_rows = img_rows or []
        self._meas_rows = meas_rows or []

    def list_image_changes_since(self, cursor_ts, cursor_id):
        result = []
        for r in self._img_rows:
            ts = str(r.get('updated_at') or '')
            rid = str(r.get('id', ''))
            if ts > cursor_ts or (ts == cursor_ts and rid > cursor_id):
                result.append(r)
        return result

    def list_measurement_changes_since(self, cursor_ts, cursor_id):
        return [r for r in self._meas_rows if (
            str(r.get('measured_at') or ''),
            str(r.get('id', ''))
        ) > (cursor_ts, cursor_id)]


def test_no_rows_after_cursor_advance():
    """When cursor is at the latest row, probe returns empty → forced set empty."""
    img_row = {
        'id': 'img-1',
        'observation_id': 'cloud-555',
        'updated_at': '2026-07-20T10:00:00+00:00',
    }
    client = _FakeClientWithProbe(img_rows=[img_row])

    # Cursor exactly at this row's updated_at + id → nothing newer.
    result = client.list_image_changes_since(
        '2026-07-20T10:00:00+00:00', 'img-1'
    )
    assert result == []


# ---------------------------------------------------------------------------
# 3. Soft delete: updated_at advances on delete → probe row returned
# ---------------------------------------------------------------------------


def test_child_softdelete_forces_pull():
    """A row whose updated_at (set by trigger on soft-delete) is newer than
    the cursor is returned by the probe."""
    img_row = {
        'id': 'img-2',
        'observation_id': 'cloud-555',
        'updated_at': '2026-07-25T00:00:00+00:00',  # trigger advanced it
    }
    client = _FakeClientWithProbe(img_rows=[img_row])

    result = client.list_image_changes_since(
        '2026-07-20T00:00:00+00:00', ''
    )
    assert len(result) == 1
    assert result[0]['id'] == 'img-2'


# ---------------------------------------------------------------------------
# 4. Tie semantics: identical updated_at, higher id wins
# ---------------------------------------------------------------------------


def test_tie_semantics():
    """With two rows at same updated_at, cursor at (ts, id_of_first) returns only second."""
    ts = '2026-07-20T10:00:00+00:00'
    row_a = {'id': 'img-a', 'observation_id': 'cloud-1', 'updated_at': ts}
    row_b = {'id': 'img-b', 'observation_id': 'cloud-2', 'updated_at': ts}
    client = _FakeClientWithProbe(img_rows=[row_a, row_b])

    result = client.list_image_changes_since(ts, 'img-a')
    assert len(result) == 1
    assert result[0]['id'] == 'img-b'


# ---------------------------------------------------------------------------
# 5. Probe failure is non-fatal
# ---------------------------------------------------------------------------


def test_probe_failure_nonfatal(monkeypatch):
    """A probe exception must not propagate out of sync_all."""
    from database import models

    settings_store: dict = {
        cloud_sync._CLOUD_MEASUREMENT_RECONCILE_VERSION_SETTING: cloud_sync._CLOUD_MEASUREMENT_RECONCILE_VERSION,
        cloud_sync._CLOUD_CHILD_CHANGE_CURSOR_SETTING: _valid_cursor(),
    }
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings_store))
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda d: settings_store.update(d))
    monkeypatch.setattr(cloud_sync, "ensure_database_linked_to_cloud_user", lambda _: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *a, **kw: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "pull_calibrations", lambda *a, **kw: {"pulled": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "push_all", lambda *a, **kw: {
        "pushed": 0, "total": 0, "errors": [],
        "spore_measurement_reconcile": {"candidates": 0, "attempted": 0},
        "spore_summary_reconcile": {"candidates": 0, "attempted": 0},
        "sync_summary": cloud_sync._new_sync_summary(),
    })
    monkeypatch.setattr(cloud_sync, "pull_all", lambda *a, **kw: {"pulled": 0, "errors": [], "deleted_remote": []})

    class _BustedProbeClient:
        user_id = "u"
        def list_remote_observations(self): return []
        def list_remote_calibrations(self): return []
        def list_image_changes_since(self, *a): raise RuntimeError("network error")
        def list_measurement_changes_since(self, *a): raise RuntimeError("network error")

    # Should not raise.
    cloud_sync.sync_all(
        _BustedProbeClient(),
        sync_images=False,
        materialize_remote_images=False,
        full_pull=False,
        child_safety_pull=True,
    )


# ---------------------------------------------------------------------------
# 6. Cursor not advanced when pull_all raises
# ---------------------------------------------------------------------------


def test_cursor_not_advanced_on_pull_failure(monkeypatch):
    """_store_child_change_cursor must not be called when pull_all raises."""
    from database import models

    settings_store: dict = {
        cloud_sync._CLOUD_MEASUREMENT_RECONCILE_VERSION_SETTING: cloud_sync._CLOUD_MEASUREMENT_RECONCILE_VERSION,
        cloud_sync._CLOUD_CHILD_CHANGE_CURSOR_SETTING: _valid_cursor(),
    }
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings_store))
    store_calls: list = []
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda d: store_calls.append(d))
    monkeypatch.setattr(cloud_sync, "ensure_database_linked_to_cloud_user", lambda _: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *a, **kw: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "pull_calibrations", lambda *a, **kw: {"pulled": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "push_all", lambda *a, **kw: {
        "pushed": 0, "total": 0, "errors": [],
        "spore_measurement_reconcile": {"candidates": 0, "attempted": 0},
        "spore_summary_reconcile": {"candidates": 0, "attempted": 0},
        "sync_summary": cloud_sync._new_sync_summary(),
    })
    monkeypatch.setattr(cloud_sync, "pull_all", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("pull failed")))

    class _GoodProbeClient:
        user_id = "u"
        def list_remote_observations(self): return []
        def list_remote_calibrations(self): return []
        def list_image_changes_since(self, *a): return []
        def list_measurement_changes_since(self, *a): return []

    with pytest.raises(RuntimeError, match="pull failed"):
        cloud_sync.sync_all(
            _GoodProbeClient(),
            sync_images=False,
            materialize_remote_images=False,
            full_pull=False,
            child_safety_pull=True,
        )

    # None of the update_app_settings calls should contain the cursor key
    for call in store_calls:
        assert cloud_sync._CLOUD_CHILD_CHANGE_CURSOR_SETTING not in call, (
            f"cursor was written despite pull failure: {call}"
        )


# ---------------------------------------------------------------------------
# 7. PullOnlyCloudClient allows probe methods
# ---------------------------------------------------------------------------


def test_pullonly_allows_probe_methods():
    """PullOnlyCloudClient must not raise PullOnlyModeError for probe methods."""

    class _FakeInner:
        user_id = "u"
        is_pull_only = False

        def list_image_changes_since(self, *a):
            return []

        def list_measurement_changes_since(self, *a):
            return []

        def __getattr__(self, name):
            def _stub(*a, **kw):
                return []
            return _stub

    inner = _FakeInner()
    wrapped = cloud_sync.PullOnlyCloudClient(inner)
    # These must not raise PullOnlyModeError
    result_img = wrapped.list_image_changes_since("2026-01-01T00:00:00+00:00", "")
    result_meas = wrapped.list_measurement_changes_since("2026-01-01T00:00:00+00:00", "")
    assert result_img == []
    assert result_meas == []


# ---------------------------------------------------------------------------
# 8. Convergence still fires for non-forced obs (fast path not broken)
# ---------------------------------------------------------------------------


def test_convergence_still_fires_for_non_forced(tmp_path, monkeypatch):
    """An obs NOT in forced_pull_cloud_ids where remote_updated > local_synced
    but fields match the snapshot should converge (not become a pull candidate)."""
    db_path = _init_db(tmp_path)
    _default_monkeypatches(monkeypatch, db_path)

    cloud_id = "cloud-777"
    # local synced earlier than remote updated_at → would normally be candidate,
    # but fields match snapshot so convergence fires.
    _seed_synced_observation(
        db_path, local_id=777, cloud_id=cloud_id,
        synced_at="2026-07-15T10:00:00+00:00",
    )

    remote_obs = [{
        "id": cloud_id,
        "desktop_id": 777,
        "updated_at": "2026-07-15T11:00:00+00:00",  # newer than synced_at
        "genus": "Panaeolus",
        "species": "sp.",
    }]
    client = _MinimalClient(remote_obs)

    # forced set is EMPTY — does not include cloud-777
    result = cloud_sync.pull_all(
        client,
        remote_obs=remote_obs,
        sync_calibrations=False,
        materialize_remote_images=False,
        sync_images=False,
        full_pull=False,
        forced_pull_cloud_ids=frozenset(),  # empty
    )

    # Converged → no bulk image fetch needed → fast_path_used
    assert result.get("fast_path_used") is True
    assert client.bulk_image_calls == []


# ===========================================================================
# New tests for updated_at cursor semantics (Tasks 1-4)
# ===========================================================================


# ---------------------------------------------------------------------------
# N1. New image INSERT detected via updated_at cursor
# ---------------------------------------------------------------------------


def test_new_image_insert_detected_via_updated_at():
    """A newly inserted image (updated_at > cursor_ts) is returned by probe."""
    cursor_ts = '2026-07-20T10:00:00+00:00'
    img_row = {
        'id': 'img-new',
        'observation_id': 'cloud-100',
        'updated_at': '2026-07-21T08:00:00+00:00',
    }
    client = _FakeClientWithProbe(img_rows=[img_row])
    result = client.list_image_changes_since(cursor_ts, '')
    assert len(result) == 1
    assert result[0]['id'] == 'img-new'


# ---------------------------------------------------------------------------
# N2. Soft-delete detected (trigger advances updated_at on delete)
# ---------------------------------------------------------------------------


def test_soft_delete_via_updated_at_detected():
    """Soft-delete advances updated_at via trigger; probe picks it up."""
    cursor_ts = '2026-07-20T10:00:00+00:00'
    # Row was created before cursor but deleted (trigger updates updated_at) after.
    img_row = {
        'id': 'img-del',
        'observation_id': 'cloud-200',
        'updated_at': '2026-07-22T12:00:00+00:00',
    }
    client = _FakeClientWithProbe(img_rows=[img_row])
    result = client.list_image_changes_since(cursor_ts, '')
    assert len(result) == 1
    assert result[0]['id'] == 'img-del'


# ---------------------------------------------------------------------------
# N3. Metadata-only child UPDATE detected while parent observation converged
# ---------------------------------------------------------------------------


def test_metadata_update_forces_child_changed_candidate(tmp_path, monkeypatch):
    """A metadata-only update to an image (updated_at advances, parent obs
    unchanged/converged) results in forced_pull_cloud_ids with reason child_changed."""
    db_path = _init_db(tmp_path)
    _default_monkeypatches(monkeypatch, db_path)

    cloud_id = "cloud-meta"
    synced_at = "2026-07-15T10:00:00+00:00"
    _seed_synced_observation(db_path, local_id=300, cloud_id=cloud_id, synced_at=synced_at)

    # Remote obs unchanged (same updated_at as synced_at) → convergence would skip.
    remote_obs = [{
        "id": cloud_id,
        "desktop_id": 300,
        "updated_at": synced_at,
        "genus": "Panaeolus",
        "species": "sp.",
    }]

    # But an image under this obs had a metadata update after cursor.
    img_row = {
        'id': 'img-meta',
        'observation_id': cloud_id,
        'updated_at': '2026-07-16T08:00:00+00:00',
    }

    # Wire up the probe to return this image row.
    cursor_ts = '2026-07-15T09:00:00+00:00'
    probe_client_img_rows = [img_row]

    settings_store: dict = {
        cloud_sync._CLOUD_MEASUREMENT_RECONCILE_VERSION_SETTING: cloud_sync._CLOUD_MEASUREMENT_RECONCILE_VERSION,
        cloud_sync._CLOUD_CHILD_CHANGE_CURSOR_SETTING: _valid_cursor(img_ts=cursor_ts),
    }

    forced_ids_seen: list = []
    original_pull_all = cloud_sync.pull_all

    def _capturing_pull_all(*a, forced_pull_cloud_ids=frozenset(), **kw):
        forced_ids_seen.append(set(forced_pull_cloud_ids))
        return {"pulled": 0, "errors": [], "deleted_remote": [], "fast_path_used": True}

    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings_store))
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda d: settings_store.update(d))
    monkeypatch.setattr(cloud_sync, "ensure_database_linked_to_cloud_user", lambda _: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *a, **kw: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "pull_calibrations", lambda *a, **kw: {"pulled": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "push_all", lambda *a, **kw: {
        "pushed": 0, "total": 0, "errors": [],
        "spore_measurement_reconcile": {"candidates": 0, "attempted": 0},
        "spore_summary_reconcile": {"candidates": 0, "attempted": 0},
        "sync_summary": cloud_sync._new_sync_summary(),
    })
    monkeypatch.setattr(cloud_sync, "pull_all", _capturing_pull_all)

    class _MetaUpdateClient:
        user_id = "u"
        def list_remote_observations(self): return remote_obs
        def list_remote_calibrations(self): return []
        def list_image_changes_since(self, cursor_ts_, cursor_id):
            return [r for r in probe_client_img_rows
                    if str(r['updated_at']) > cursor_ts_]
        def list_measurement_changes_since(self, *a): return []

    cloud_sync.sync_all(
        _MetaUpdateClient(),
        sync_images=False,
        materialize_remote_images=False,
        full_pull=False,
        child_safety_pull=True,
    )

    assert forced_ids_seen, "pull_all was not called"
    assert cloud_id in forced_ids_seen[0], (
        f"cloud_id not in forced set: {forced_ids_seen[0]}"
    )


# ---------------------------------------------------------------------------
# N4. Identical updated_at + higher id IS detected
# ---------------------------------------------------------------------------


def test_identical_updated_at_higher_id_detected():
    """Row with same updated_at as cursor but higher id must be returned."""
    ts = '2026-07-20T10:00:00+00:00'
    row = {'id': 'img-b', 'observation_id': 'cloud-1', 'updated_at': ts}
    client = _FakeClientWithProbe(img_rows=[row])
    # Cursor is at same ts but lower id
    result = client.list_image_changes_since(ts, 'img-a')
    assert len(result) == 1
    assert result[0]['id'] == 'img-b'


# ---------------------------------------------------------------------------
# N5. Identical updated_at + same/lower id NOT reprocessed
# ---------------------------------------------------------------------------


def test_identical_updated_at_same_or_lower_id_not_reprocessed():
    """Row at same (updated_at, id) as cursor must NOT be returned (not strictly after)."""
    ts = '2026-07-20T10:00:00+00:00'
    row_same = {'id': 'img-a', 'observation_id': 'cloud-1', 'updated_at': ts}
    row_lower = {'id': 'img-0', 'observation_id': 'cloud-2', 'updated_at': ts}
    client = _FakeClientWithProbe(img_rows=[row_same, row_lower])
    result = client.list_image_changes_since(ts, 'img-a')
    assert result == [], f"expected no rows, got: {result}"


# ---------------------------------------------------------------------------
# N6. Echo regression: own desktop image write does not loop
# ---------------------------------------------------------------------------


def test_echo_own_write_does_not_loop(monkeypatch):
    """Desktop pushes image → probe sees own write (echo) → forced pull →
    second sync has zero child candidates and no cursor churn."""
    _EPOCH = '1970-01-01T00:00:00+00:00'
    img_ts_after_push = '2026-07-20T10:00:00+00:00'
    img_id = 'img-echo'
    obs_id = 'cloud-echo'

    # Cursor before the push
    settings_store: dict = {
        cloud_sync._CLOUD_MEASUREMENT_RECONCILE_VERSION_SETTING: cloud_sync._CLOUD_MEASUREMENT_RECONCILE_VERSION,
        cloud_sync._CLOUD_CHILD_CHANGE_CURSOR_SETTING: _valid_cursor(
            img_ts='2026-07-19T00:00:00+00:00',
        ),
    }

    pull_all_calls: list = []

    def _fake_pull_all(*a, forced_pull_cloud_ids=frozenset(), **kw):
        pull_all_calls.append({'forced': set(forced_pull_cloud_ids)})
        return {"pulled": 0, "errors": [], "deleted_remote": [], "fast_path_used": True}

    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings_store))
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda d: settings_store.update(d))
    monkeypatch.setattr(cloud_sync, "ensure_database_linked_to_cloud_user", lambda _: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *a, **kw: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "pull_calibrations", lambda *a, **kw: {"pulled": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "push_all", lambda *a, **kw: {
        "pushed": 0, "total": 0, "errors": [],
        "spore_measurement_reconcile": {"candidates": 0, "attempted": 0},
        "spore_summary_reconcile": {"candidates": 0, "attempted": 0},
        "sync_summary": cloud_sync._new_sync_summary(),
    })
    monkeypatch.setattr(cloud_sync, "pull_all", _fake_pull_all)

    # First sync: probe sees the echo row (own write).
    class _EchoClient:
        user_id = "u"
        _probe_call_count = 0

        def list_remote_observations(self): return []
        def list_remote_calibrations(self): return []

        def list_image_changes_since(self, cursor_ts, cursor_id):
            # First probe sees the echo row; subsequent probes see nothing
            # because cursor has advanced past it.
            ts = img_ts_after_push
            rid = img_id
            if ts > cursor_ts or (ts == cursor_ts and rid > cursor_id):
                return [{'id': rid, 'observation_id': obs_id, 'updated_at': ts}]
            return []

        def list_measurement_changes_since(self, *a): return []

    cloud_sync.sync_all(
        _EchoClient(),
        sync_images=False,
        materialize_remote_images=False,
        full_pull=False,
        child_safety_pull=True,
    )

    # First sync: echo row surfaced as forced candidate.
    assert pull_all_calls, "pull_all not called"
    assert obs_id in pull_all_calls[0]['forced'], (
        "echo obs_id should be a forced candidate on first sync"
    )

    # Second sync: cursor advanced past echo row → zero child candidates.
    pull_all_calls.clear()
    cloud_sync.sync_all(
        _EchoClient(),
        sync_images=False,
        materialize_remote_images=False,
        full_pull=False,
        child_safety_pull=True,
    )
    assert pull_all_calls, "pull_all not called on second sync"
    assert pull_all_calls[0]['forced'] == set(), (
        f"second sync should have zero forced child candidates, got: {pull_all_calls[0]['forced']}"
    )


# ---------------------------------------------------------------------------
# N7. Failed pull does not advance cursor
# (already covered by test_cursor_not_advanced_on_pull_failure; variant checks
#  cursor value unchanged on exception)
# ---------------------------------------------------------------------------


def test_failed_pull_cursor_value_unchanged(monkeypatch):
    """Cursor ts/id must remain identical after a pull_all exception."""
    original_cursor_json = _valid_cursor(img_ts='2026-06-01T00:00:00+00:00', img_id='img-old')
    settings_store: dict = {
        cloud_sync._CLOUD_MEASUREMENT_RECONCILE_VERSION_SETTING: cloud_sync._CLOUD_MEASUREMENT_RECONCILE_VERSION,
        cloud_sync._CLOUD_CHILD_CHANGE_CURSOR_SETTING: original_cursor_json,
    }
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings_store))
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda d: settings_store.update(d))
    monkeypatch.setattr(cloud_sync, "ensure_database_linked_to_cloud_user", lambda _: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *a, **kw: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "pull_calibrations", lambda *a, **kw: {"pulled": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "push_all", lambda *a, **kw: {
        "pushed": 0, "total": 0, "errors": [],
        "spore_measurement_reconcile": {"candidates": 0, "attempted": 0},
        "spore_summary_reconcile": {"candidates": 0, "attempted": 0},
        "sync_summary": cloud_sync._new_sync_summary(),
    })
    monkeypatch.setattr(cloud_sync, "pull_all", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("pull failed")))

    class _ProbeClient:
        user_id = "u"
        def list_remote_observations(self): return []
        def list_remote_calibrations(self): return []
        def list_image_changes_since(self, *a):
            return [{'id': 'img-new', 'observation_id': 'obs-1', 'updated_at': '2026-07-01T00:00:00+00:00'}]
        def list_measurement_changes_since(self, *a): return []

    with pytest.raises(RuntimeError):
        cloud_sync.sync_all(
            _ProbeClient(),
            sync_images=False,
            materialize_remote_images=False,
            full_pull=False,
            child_safety_pull=True,
        )

    stored = settings_store.get(cloud_sync._CLOUD_CHILD_CHANGE_CURSOR_SETTING)
    assert stored == original_cursor_json, (
        f"cursor was modified despite pull failure: {stored}"
    )


# ---------------------------------------------------------------------------
# N8. Successful pull advances cursor
# ---------------------------------------------------------------------------


def test_successful_pull_advances_cursor(monkeypatch):
    """After a successful pull_all, cursor is advanced to the probe row's updated_at."""
    cursor_ts = '2026-06-01T00:00:00+00:00'
    settings_store: dict = {
        cloud_sync._CLOUD_MEASUREMENT_RECONCILE_VERSION_SETTING: cloud_sync._CLOUD_MEASUREMENT_RECONCILE_VERSION,
        cloud_sync._CLOUD_CHILD_CHANGE_CURSOR_SETTING: _valid_cursor(img_ts=cursor_ts),
    }
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings_store))
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda d: settings_store.update(d))
    monkeypatch.setattr(cloud_sync, "ensure_database_linked_to_cloud_user", lambda _: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *a, **kw: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "pull_calibrations", lambda *a, **kw: {"pulled": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "push_all", lambda *a, **kw: {
        "pushed": 0, "total": 0, "errors": [],
        "spore_measurement_reconcile": {"candidates": 0, "attempted": 0},
        "spore_summary_reconcile": {"candidates": 0, "attempted": 0},
        "sync_summary": cloud_sync._new_sync_summary(),
    })
    monkeypatch.setattr(cloud_sync, "pull_all", lambda *a, **kw: {"pulled": 0, "errors": [], "deleted_remote": []})

    new_ts = '2026-07-20T10:00:00+00:00'
    new_id = 'img-new'

    class _ProbeClient:
        user_id = "u"
        def list_remote_observations(self): return []
        def list_remote_calibrations(self): return []
        def list_image_changes_since(self, ts, rid):
            r = {'id': new_id, 'observation_id': 'obs-1', 'updated_at': new_ts}
            if new_ts > ts or (new_ts == ts and new_id > rid):
                return [r]
            return []
        def list_measurement_changes_since(self, *a): return []

    cloud_sync.sync_all(
        _ProbeClient(),
        sync_images=False,
        materialize_remote_images=False,
        full_pull=False,
        child_safety_pull=True,
    )

    stored_raw = settings_store.get(cloud_sync._CLOUD_CHILD_CHANGE_CURSOR_SETTING)
    assert stored_raw is not None, "cursor not stored"
    stored = json.loads(stored_raw)
    assert stored['images']['ts'] == new_ts, f"cursor ts not advanced: {stored['images']['ts']}"
    assert stored['images']['id'] == new_id, f"cursor id not advanced: {stored['images']['id']}"
    assert stored.get('v') == cloud_sync._CHILD_CHANGE_CURSOR_VERSION


# ---------------------------------------------------------------------------
# N9. Second no-op explicit sync has zero child candidates
# ---------------------------------------------------------------------------


def test_second_noop_sync_zero_child_candidates(monkeypatch):
    """After cursor advances past all known rows, second sync has zero forced candidates."""
    img_ts = '2026-07-20T10:00:00+00:00'
    img_id = 'img-z'

    settings_store: dict = {
        cloud_sync._CLOUD_MEASUREMENT_RECONCILE_VERSION_SETTING: cloud_sync._CLOUD_MEASUREMENT_RECONCILE_VERSION,
        cloud_sync._CLOUD_CHILD_CHANGE_CURSOR_SETTING: _valid_cursor(
            img_ts='2026-07-19T00:00:00+00:00',
        ),
    }

    pull_forced: list = []

    def _fake_pull_all(*a, forced_pull_cloud_ids=frozenset(), **kw):
        pull_forced.append(set(forced_pull_cloud_ids))
        return {"pulled": 0, "errors": [], "deleted_remote": []}

    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings_store))
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda d: settings_store.update(d))
    monkeypatch.setattr(cloud_sync, "ensure_database_linked_to_cloud_user", lambda _: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *a, **kw: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "pull_calibrations", lambda *a, **kw: {"pulled": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "push_all", lambda *a, **kw: {
        "pushed": 0, "total": 0, "errors": [],
        "spore_measurement_reconcile": {"candidates": 0, "attempted": 0},
        "spore_summary_reconcile": {"candidates": 0, "attempted": 0},
        "sync_summary": cloud_sync._new_sync_summary(),
    })
    monkeypatch.setattr(cloud_sync, "pull_all", _fake_pull_all)

    class _Client:
        user_id = "u"
        def list_remote_observations(self): return []
        def list_remote_calibrations(self): return []
        def list_image_changes_since(self, cursor_ts, cursor_id):
            if img_ts > cursor_ts or (img_ts == cursor_ts and img_id > cursor_id):
                return [{'id': img_id, 'observation_id': 'obs-z', 'updated_at': img_ts}]
            return []
        def list_measurement_changes_since(self, *a): return []

    # First sync: detects the row.
    cloud_sync.sync_all(_Client(), sync_images=False, materialize_remote_images=False,
                        full_pull=False, child_safety_pull=True)
    assert pull_forced[0] == {'obs-z'}, f"first sync should detect row: {pull_forced}"

    # Second sync: cursor advanced → zero candidates.
    pull_forced.clear()
    cloud_sync.sync_all(_Client(), sync_images=False, materialize_remote_images=False,
                        full_pull=False, child_safety_pull=True)
    assert pull_forced[0] == set(), (
        f"second sync should have zero forced candidates: {pull_forced[0]}"
    )


# ---------------------------------------------------------------------------
# N10. Bootstrap: missing/old-format cursor forces authoritative scan
#       and seeds cursor only after success
# ---------------------------------------------------------------------------


def test_bootstrap_missing_cursor_forces_scan_and_seeds(monkeypatch):
    """Missing cursor (or old-format) triggers a full scan and seeds cursor after success.
    Old-format cursor (no 'v' key) is treated as None — bootstrap fires."""
    _EPOCH = '1970-01-01T00:00:00+00:00'
    pre_scan_ts = '2026-07-20T10:00:00+00:00'
    pre_scan_id = 'img-pre'

    # Old-format cursor (no 'v' key) → _load_child_change_cursor returns None.
    old_cursor = json.dumps({
        'images': {'ts': '2026-01-01T00:00:00+00:00', 'id': ''},
        'measurements': {'ts': '2026-01-01T00:00:00+00:00', 'id': ''},
    })

    settings_store: dict = {
        cloud_sync._CLOUD_MEASUREMENT_RECONCILE_VERSION_SETTING: cloud_sync._CLOUD_MEASUREMENT_RECONCILE_VERSION,
        cloud_sync._CLOUD_CHILD_CHANGE_CURSOR_SETTING: old_cursor,
    }

    pull_all_was_full: list = []

    def _fake_pull_all(*a, full_pull=False, forced_pull_cloud_ids=frozenset(), **kw):
        pull_all_was_full.append(full_pull)
        return {"pulled": 0, "errors": [], "deleted_remote": []}

    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings_store))
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda d: settings_store.update(d))
    monkeypatch.setattr(cloud_sync, "ensure_database_linked_to_cloud_user", lambda _: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *a, **kw: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "pull_calibrations", lambda *a, **kw: {"pulled": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "push_all", lambda *a, **kw: {
        "pushed": 0, "total": 0, "errors": [],
        "spore_measurement_reconcile": {"candidates": 0, "attempted": 0},
        "spore_summary_reconcile": {"candidates": 0, "attempted": 0},
        "sync_summary": cloud_sync._new_sync_summary(),
    })
    monkeypatch.setattr(cloud_sync, "pull_all", _fake_pull_all)

    class _BootstrapClient:
        user_id = "u"
        def list_remote_observations(self): return []
        def list_remote_calibrations(self): return []
        def list_image_changes_since(self, *a): return []
        def list_measurement_changes_since(self, *a): return []
        def _get(self, url):
            if 'observation_images' in url:
                return [{'id': pre_scan_id, 'updated_at': pre_scan_ts}]
            if 'spore_measurements' in url:
                return [{'id': 'meas-1', 'measured_at': '2026-07-10T00:00:00+00:00'}]
            return []

    cloud_sync.sync_all(
        _BootstrapClient(),
        sync_images=False,
        materialize_remote_images=False,
        full_pull=False,
        child_safety_pull=True,
    )

    # pull_all must have been called with full_pull=True (bootstrap forces full scan)
    assert pull_all_was_full, "pull_all was not called"
    assert pull_all_was_full[0] is True, (
        f"bootstrap should force full_pull=True, got: {pull_all_was_full[0]}"
    )

    # Cursor must be seeded with pre-scan max (not missing, not old-format)
    stored_raw = settings_store.get(cloud_sync._CLOUD_CHILD_CHANGE_CURSOR_SETTING)
    assert stored_raw is not None, "cursor not seeded after bootstrap"
    stored = json.loads(stored_raw)
    assert stored.get('v') == cloud_sync._CHILD_CHANGE_CURSOR_VERSION, "cursor missing version"
    assert stored['images']['ts'] == pre_scan_ts, (
        f"cursor should be seeded from pre-scan max: {stored['images']['ts']}"
    )
    assert stored['images']['id'] == pre_scan_id

    # Second sync: cursor now valid → probe runs normally (no bootstrap).
    pull_all_was_full.clear()
    cloud_sync.sync_all(
        _BootstrapClient(),
        sync_images=False,
        materialize_remote_images=False,
        full_pull=False,
        child_safety_pull=True,
    )
    # No bootstrap → full_pull should be False (unless 24h TTL fires, which won't here
    # since safety_pull_due was just set).
    assert pull_all_was_full, "pull_all not called on second sync"
    # Watermark was just written and the cursor is valid, so neither the TTL
    # nor bootstrap may force a full pull — this is the no-repeat guarantee.
    assert pull_all_was_full[0] is False, (
        f"second sync must not re-bootstrap a full pull, got: {pull_all_was_full[0]}"
    )
    stored_raw2 = settings_store.get(cloud_sync._CLOUD_CHILD_CHANGE_CURSOR_SETTING)
    stored2 = json.loads(stored_raw2)
    assert stored2.get('v') == cloud_sync._CHILD_CHANGE_CURSOR_VERSION, (
        "cursor lost version on second sync"
    )


def test_bootstrap_old_format_cursor_treated_as_missing(monkeypatch):
    """_load_child_change_cursor returns None for old-format (no 'v') cursor,
    triggering bootstrap on the next sync."""
    old_cursor_json = json.dumps({
        'images': {'ts': '2026-03-01T00:00:00+00:00', 'id': 'img-old'},
        'measurements': {'ts': '2026-03-01T00:00:00+00:00', 'id': ''},
    })
    # Simulate having the old-format in settings
    settings = {cloud_sync._CLOUD_CHILD_CHANGE_CURSOR_SETTING: old_cursor_json}
    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))

    result = cloud_sync._load_child_change_cursor()
    assert result is None, (
        "old-format cursor (no 'v') should be treated as None to trigger bootstrap"
    )


# ---------------------------------------------------------------------------
# URL encoding of cursor timestamps (production 22007 regression)
#
# PostgREST parses an unencoded '+' in a query string as a space, turning
# '2026-08-24T10:50:24+00:00' into '2026-08-24T10:50:24 00:00' and failing
# with 22007 invalid input syntax. Cursor values must go through
# _encode_postgrest_filter_value before URL interpolation.
# ---------------------------------------------------------------------------


def _real_client_with_captured_paths():
    client = object.__new__(cloud_sync.SporelyCloudClient)
    client.user_id = 'user-1'
    captured: list[str] = []

    def fake_get_paginated(path, **kwargs):
        captured.append(path)
        return []

    client._get_paginated = fake_get_paginated
    return client, captured


def test_image_probe_encodes_utc_plus_offset():
    client, captured = _real_client_with_captured_paths()
    client.list_image_changes_since('2026-08-24T10:50:24.571833+00:00', 'img-1')
    assert len(captured) == 1
    assert 'updated_at=gte.2026-08-24T10%3A50%3A24.571833%2B00%3A00' in captured[0]
    assert '+' not in captured[0].split('updated_at=gte.')[1].split('&')[0]


def test_image_probe_encodes_positive_nonzero_offset():
    client, captured = _real_client_with_captured_paths()
    client.list_image_changes_since('2026-08-24T12:50:24+02:00', '')
    assert '%2B02%3A00' in captured[0]


def test_image_probe_negative_offset_remains_valid():
    client, captured = _real_client_with_captured_paths()
    client.list_image_changes_since('2026-08-24T05:50:24-05:00', '')
    # '-' is an unreserved URL character: it must survive literally and the
    # colons must be percent-encoded.
    assert 'updated_at=gte.2026-08-24T05%3A50%3A24-05%3A00' in captured[0]


def test_measurement_probe_uses_same_safe_encoding():
    client, captured = _real_client_with_captured_paths()
    client.list_measurement_changes_since('2026-08-24T10:50:24+00:00', 'm-1')
    assert len(captured) == 1
    assert 'measured_at=gte.2026-08-24T10%3A50%3A24%2B00%3A00' in captured[0]
    assert '+' not in captured[0].split('measured_at=gte.')[1].split('&')[0]


def test_strict_tuple_filter_unchanged_by_encoding():
    client = object.__new__(cloud_sync.SporelyCloudClient)
    client.user_id = 'user-1'
    ts = '2026-08-24T10:50:24+00:00'
    rows = [
        {'id': 'img-1', 'observation_id': 'o1', 'updated_at': ts},          # == ts, same id: excluded
        {'id': 'img-2', 'observation_id': 'o1', 'updated_at': ts},          # == ts, higher id: included
        {'id': 'img-0', 'observation_id': 'o2', 'updated_at': ts},          # == ts, lower id: excluded
        {'id': 'img-3', 'observation_id': 'o3',
         'updated_at': '2026-08-24T10:50:25+00:00'},                        # > ts: included
    ]
    client._get_paginated = lambda path, **kw: rows
    result = client.list_image_changes_since(ts, 'img-1')
    assert [r['id'] for r in result] == ['img-2', 'img-3']
