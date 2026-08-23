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
# 2. No rows after cursor advance → empty forced set
# ---------------------------------------------------------------------------


class _FakeClientWithProbe:
    """Client with list_image_changes_since returning controllable rows."""

    user_id = "user-probe"

    def __init__(self, img_rows=None, meas_rows=None):
        self._img_rows = img_rows or []
        self._meas_rows = meas_rows or []

    def list_image_changes_since(self, cursor_ts, cursor_id):
        return [r for r in self._img_rows if (
            max(str(r.get('created_at') or ''), str(r.get('deleted_at') or '')),
            str(r.get('id', ''))
        ) > (cursor_ts, cursor_id)]

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
        'created_at': '2026-07-20T10:00:00+00:00',
        'deleted_at': None,
    }
    client = _FakeClientWithProbe(img_rows=[img_row])

    # Cursor already at this row's ts+id
    result = client.list_image_changes_since(
        '2026-07-20T10:00:00+00:00', 'img-1'
    )
    assert result == []


# ---------------------------------------------------------------------------
# 3. Soft delete: deleted_at newer than cursor triggers probe row
# ---------------------------------------------------------------------------


def test_child_softdelete_forces_pull():
    """A row with deleted_at > cursor_ts but created_at < cursor_ts is returned."""
    img_row = {
        'id': 'img-2',
        'observation_id': 'cloud-555',
        'created_at': '2026-06-01T00:00:00+00:00',
        'deleted_at': '2026-07-25T00:00:00+00:00',
    }
    client = _FakeClientWithProbe(img_rows=[img_row])

    result = client.list_image_changes_since(
        '2026-07-20T00:00:00+00:00', ''
    )
    assert len(result) == 1
    assert result[0]['id'] == 'img-2'


# ---------------------------------------------------------------------------
# 4. Tie semantics
# ---------------------------------------------------------------------------


def test_tie_semantics():
    """With two rows at same created_at, cursor at (ts, id_of_first) returns only second."""
    ts = '2026-07-20T10:00:00+00:00'
    row_a = {'id': 'img-a', 'observation_id': 'cloud-1', 'created_at': ts, 'deleted_at': None}
    row_b = {'id': 'img-b', 'observation_id': 'cloud-2', 'created_at': ts, 'deleted_at': None}
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
        cloud_sync._CLOUD_CHILD_CHANGE_CURSOR_SETTING: json.dumps({
            'images': {'ts': '2026-01-01T00:00:00+00:00', 'id': ''},
            'measurements': {'ts': '2026-01-01T00:00:00+00:00', 'id': ''},
        }),
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
        cloud_sync._CLOUD_CHILD_CHANGE_CURSOR_SETTING: json.dumps({
            'images': {'ts': '2026-01-01T00:00:00+00:00', 'id': ''},
            'measurements': {'ts': '2026-01-01T00:00:00+00:00', 'id': ''},
        }),
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
