"""No-op fast-path tests for Refresh / background sync.

Locks in the invariants that keep a no-change Refresh cheap:

  * ``push_all(full_pull=False)`` runs the lightweight reconciliation passes
    so historical cloud gaps are repaired during an ordinary Refresh.
  * ``pull_all(full_pull=False)`` prunes candidates by ``updated_at`` and, if
    nothing changed, returns early WITHOUT running the bulk image-metadata or
    bulk-measurement fetches (the two calls that dominate no-op sync time).
  * A changed remote observation still gets pulled through the fast path.
  * A locally-dirty observation still pushes through push_all.
  * A full-refresh path (``full_pull=True``) still fetches everything.

Design goal: with 220 observations, a no-op Refresh does 0 bulk fetches.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from database import models
from utils import cloud_sync


def _connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db(tmp_path):
    db_path = tmp_path / "fast_path.sqlite"
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


class _RecordingClient:
    """Instrumented fake client so tests can assert what network calls fire."""

    user_id = "user-fast-path"

    def __init__(self, remote_observations: list[dict], remote_images: list[dict] | None = None):
        self._remote_observations = remote_observations
        self._remote_images = remote_images or []
        self.bulk_image_calls: list[list[str]] = []
        self.per_obs_image_calls: list[str] = []
        self.list_calls = 0
        self.pushed: list[dict] = []
        self.measurements_calls: list[list[str]] = []

    def list_remote_observations(self):
        self.list_calls += 1
        return list(self._remote_observations)

    def list_remote_calibrations(self):
        return []

    def push_observation(self, obs, remote_obs=None):
        self.pushed.append(dict(obs))
        return str(obs.get("cloud_id") or "cloud-generated")

    def get_observation(self, cloud_id):
        for row in self._remote_observations:
            if str(row.get("id")) == str(cloud_id):
                return dict(row)
        return {"id": str(cloud_id)}

    def pull_bulk_image_metadata(self, cloud_ids):
        self.bulk_image_calls.append(list(cloud_ids))
        return [
            dict(img)
            for img in self._remote_images
            if str(img.get("observation_id")) in {str(cid) for cid in cloud_ids}
        ]

    def pull_image_metadata(self, cloud_id, **kwargs):
        self.per_obs_image_calls.append(str(cloud_id))
        return [dict(img) for img in self._remote_images if str(img.get("observation_id")) == str(cloud_id)]


def _default_monkeypatches(monkeypatch, db_path):
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
    monkeypatch.setattr(
        cloud_sync, "_reconcile_missing_spore_measurements", lambda *a, **kw: 0
    )
    monkeypatch.setattr(
        cloud_sync, "_reconcile_missing_spore_summaries", lambda *a, **kw: 0
    )
    monkeypatch.setattr(
        cloud_sync,
        "_push_summary_for_current_observation",
        lambda *a, **kw: None,
    )
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *a, **kw: None)
    monkeypatch.setattr(
        cloud_sync,
        "_detect_deleted_remote_observations",
        lambda *a, **kw: [],
    )


# ---------------------------------------------------------------------------
# push_all fast-path: no reconciliation scans
# ---------------------------------------------------------------------------


def test_push_all_fast_path_runs_lightweight_spore_reconciliation(tmp_path, monkeypatch):
    db_path = _init_db(tmp_path)
    _default_monkeypatches(monkeypatch, db_path)

    reconcile_calls: list[str] = []
    monkeypatch.setattr(
        cloud_sync,
        "_reconcile_missing_spore_measurements",
        lambda *a, **kw: reconcile_calls.append("measurements") or 0,
    )
    monkeypatch.setattr(
        cloud_sync,
        "_reconcile_missing_spore_summaries",
        lambda *a, **kw: reconcile_calls.append("summaries") or 0,
    )

    client = _RecordingClient([])
    cloud_sync.push_all(
        client, sync_images=False, sync_calibrations=False, full_pull=False
    )
    assert reconcile_calls == ["measurements", "summaries"]


def test_push_all_fast_path_flushes_image_tombstones_without_dirty_observations(
    tmp_path,
    monkeypatch,
):
    db_path = _init_db(tmp_path)
    _default_monkeypatches(monkeypatch, db_path)

    tombstone_calls: list[object] = []
    monkeypatch.setattr(
        cloud_sync,
        "_push_pending_image_tombstones",
        lambda client: tombstone_calls.append(client) or [],
    )

    client = _RecordingClient([])
    result = cloud_sync.push_all(
        client,
        sync_images=False,
        sync_calibrations=False,
        full_pull=False,
    )

    assert tombstone_calls == [client]
    assert result["total"] == 0
    assert result["errors"] == []


def test_push_all_surfaces_image_tombstone_failures(tmp_path, monkeypatch):
    db_path = _init_db(tmp_path)
    _default_monkeypatches(monkeypatch, db_path)
    monkeypatch.setattr(
        cloud_sync,
        "_push_pending_image_tombstones",
        lambda client: ["obs 604: could not sync cloud image tombstone 3694"],
    )

    result = cloud_sync.push_all(
        _RecordingClient([]),
        sync_images=False,
        sync_calibrations=False,
        full_pull=False,
    )

    assert result["errors"] == [
        "obs 604: could not sync cloud image tombstone 3694"
    ]


def test_push_all_full_pull_still_runs_reconciliation(tmp_path, monkeypatch):
    db_path = _init_db(tmp_path)
    _default_monkeypatches(monkeypatch, db_path)

    reconcile_calls: list[str] = []
    monkeypatch.setattr(
        cloud_sync,
        "_reconcile_missing_spore_measurements",
        lambda *a, **kw: reconcile_calls.append("measurements") or 0,
    )
    monkeypatch.setattr(
        cloud_sync,
        "_reconcile_missing_spore_summaries",
        lambda *a, **kw: reconcile_calls.append("summaries") or 0,
    )

    client = _RecordingClient([])
    cloud_sync.push_all(
        client, sync_images=False, sync_calibrations=False, full_pull=True
    )
    assert reconcile_calls == ["measurements", "summaries"], (
        f"Full-pull push must still run both reconciliation scans; got {reconcile_calls}"
    )


# ---------------------------------------------------------------------------
# pull_all fast-path: prune candidates + skip bulk fetches
# ---------------------------------------------------------------------------


def _seed_synced_observation(db_path, *, local_id=555, cloud_id="cloud-555", synced_at="2026-07-15T12:00:00+00:00"):
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


def test_pull_all_fast_path_returns_early_when_nothing_changed(tmp_path, monkeypatch, capsys):
    """The core promise: 220 synced observations with no remote changes must
    NOT trigger the bulk image + measurement fetches."""
    db_path = _init_db(tmp_path)
    _default_monkeypatches(monkeypatch, db_path)
    _seed_synced_observation(db_path, local_id=555, cloud_id="cloud-555", synced_at="2026-07-15T12:00:00+00:00")

    remote_obs = [{
        "id": "cloud-555",
        "desktop_id": 555,
        # Remote is OLDER than local synced_at → no work.
        "updated_at": "2026-07-15T11:00:00+00:00",
    }]
    client = _RecordingClient(remote_obs)

    result = cloud_sync.pull_all(
        client,
        remote_obs=remote_obs,
        sync_calibrations=False,
        materialize_remote_images=False,
        sync_images=False,
        full_pull=False,
    )
    assert result.get("fast_path_used") is True
    assert result.get("skipped_unchanged") == 1
    assert client.bulk_image_calls == [], (
        "Fast path with no changed remote observations must NOT invoke the "
        "bulk image metadata fetch"
    )
    assert client.per_obs_image_calls == [], (
        "Fast path with no changed remote observations must not fetch any "
        "per-observation image metadata either"
    )
    out = capsys.readouterr().out
    assert "no-op fast path" in out


def test_pull_all_fast_path_still_pulls_when_remote_updated(tmp_path, monkeypatch):
    """A newer remote updated_at must survive fast-path pruning."""
    db_path = _init_db(tmp_path)
    _default_monkeypatches(monkeypatch, db_path)
    _seed_synced_observation(db_path, local_id=555, cloud_id="cloud-555", synced_at="2026-07-15T12:00:00+00:00")

    remote_obs = [{
        "id": "cloud-555",
        "desktop_id": 555,
        # Remote NEWER than local synced_at → must pull.
        "updated_at": "2026-07-15T13:00:00+00:00",
        "genus": "Panaeolus",
        "species": "reticulatus",
    }]
    client = _RecordingClient(remote_obs)

    cloud_sync.pull_all(
        client,
        remote_obs=remote_obs,
        sync_calibrations=False,
        materialize_remote_images=False,
        sync_images=False,
        full_pull=False,
    )
    assert len(client.bulk_image_calls) == 1
    assert client.bulk_image_calls[0] == ["cloud-555"], (
        "Fast path must fetch image metadata only for the changed observation, "
        f"not all cloud ids; got {client.bulk_image_calls}"
    )


def test_pull_all_fast_path_pulls_new_remote_without_local_row(tmp_path, monkeypatch):
    """First-time pull for a cloud-side new observation must always fire."""
    db_path = _init_db(tmp_path)
    _default_monkeypatches(monkeypatch, db_path)
    # No local row, no snapshot — this is a brand-new remote observation.

    remote_obs = [{
        "id": "cloud-new",
        "desktop_id": None,
        "updated_at": "2026-07-15T13:00:00+00:00",
        "genus": "New",
        "species": "species",
    }]
    client = _RecordingClient(remote_obs)

    # _create_local_from_remote runs a lot of image-import machinery; stub it
    # so we can exercise only the candidate-pruning contract.
    monkeypatch.setattr(cloud_sync, "_create_local_from_remote", lambda *a, **kw: 999)

    cloud_sync.pull_all(
        client,
        remote_obs=remote_obs,
        sync_calibrations=False,
        materialize_remote_images=False,
        sync_images=False,
        full_pull=False,
    )
    # New observation → still a candidate → bulk fetch runs for it.
    assert len(client.bulk_image_calls) == 1
    assert client.bulk_image_calls[0] == ["cloud-new"]


def test_pull_all_full_pull_fetches_everything(tmp_path, monkeypatch):
    """`full_pull=True` (explicit 'Full refresh') must still fetch bulk data
    for ALL candidate observations even if nothing changed since last sync."""
    db_path = _init_db(tmp_path)
    _default_monkeypatches(monkeypatch, db_path)
    _seed_synced_observation(db_path, local_id=555, cloud_id="cloud-555", synced_at="2026-07-15T12:00:00+00:00")

    remote_obs = [{
        "id": "cloud-555",
        "desktop_id": 555,
        "updated_at": "2026-07-15T11:00:00+00:00",  # older, would prune under fast path
    }]
    client = _RecordingClient(remote_obs)

    cloud_sync.pull_all(
        client,
        remote_obs=remote_obs,
        sync_calibrations=False,
        materialize_remote_images=False,
        sync_images=False,
        full_pull=True,
    )
    assert client.bulk_image_calls == [["cloud-555"]], (
        "Full refresh must still bulk-fetch even for unchanged observations; "
        f"got {client.bulk_image_calls}"
    )


# ---------------------------------------------------------------------------
# UI wiring — Refresh defaults to full_pull=False
# ---------------------------------------------------------------------------


def test_start_cloud_sync_defaults_to_fast_path():
    pytest.importorskip("PySide6.QtCore")
    from ui.observations_tab import ObservationsTab
    import inspect

    sig = inspect.signature(ObservationsTab._start_cloud_sync)
    assert sig.parameters["sync_images"].default is False
    assert sig.parameters["materialize_remote_images"].default is False
    assert sig.parameters["full_pull"].default is False, (
        "Refresh must default to the no-op fast path — the 220-observation "
        "bulk fetch on every button press is the exact regression the fast "
        "path exists to prevent."
    )


def test_auto_sync_worker_accepts_full_pull_flag():
    pytest.importorskip("PySide6.QtCore")
    from ui.observations_tab import _CloudAutoSyncWorker
    import inspect

    sig = inspect.signature(_CloudAutoSyncWorker.__init__)
    assert "full_pull" in sig.parameters
    assert sig.parameters["full_pull"].default is True


# ---------------------------------------------------------------------------
# Periodic child-change safety pull
# ---------------------------------------------------------------------------


def _run_safety_sync(
    monkeypatch,
    *,
    watermark,
    pull_result=None,
    pull_error=None,
    settings_updates=None,
    settings_state=None,
):
    pull_kwargs: dict[str, Any] = {}
    settings_updates = settings_updates if settings_updates is not None else []
    settings_state = settings_state if settings_state is not None else (
        {} if watermark is None else {
            cloud_sync._CLOUD_LAST_CHILD_SAFETY_PULL_AT_SETTING: watermark,
        }
    )

    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings_state))
    monkeypatch.setattr(
        cloud_sync,
        "update_app_settings",
        lambda updates: (
            settings_updates.append(dict(updates)),
            settings_state.update(updates),
            dict(settings_state),
        )[-1],
    )
    monkeypatch.setattr(cloud_sync, "ensure_database_linked_to_cloud_user", lambda _client: None)
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
        "push_all",
        lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []},
    )

    def _pull(*args, **kwargs):
        pull_kwargs.update(kwargs)
        if pull_error is not None:
            raise pull_error
        return pull_result or {"pulled": 0, "total": 0, "errors": [], "deleted_remote": []}

    monkeypatch.setattr(cloud_sync, "pull_all", _pull)
    client = SimpleNamespace(
        list_remote_observations=lambda: [{"id": "cloud-555"}],
        list_remote_calibrations=lambda: [],
    )
    result = cloud_sync.sync_all(
        client,
        sync_images=False,
        materialize_remote_images=False,
        full_pull=False,
        child_safety_pull=True,
    )
    return result, pull_kwargs, settings_updates


def test_child_safety_pull_runs_when_watermark_is_missing(monkeypatch, capsys):
    _result, pull_kwargs, updates = _run_safety_sync(monkeypatch, watermark=None)

    assert pull_kwargs["full_pull"] is True
    assert pull_kwargs["sync_images"] is False
    assert pull_kwargs["materialize_remote_images"] is False
    assert updates and cloud_sync._CLOUD_LAST_CHILD_SAFETY_PULL_AT_SETTING in updates[-1]
    assert "reason=stale_child_watermark last=missing interval_hours=24" in capsys.readouterr().out


def test_child_safety_pull_runs_when_watermark_is_stale(monkeypatch):
    stale = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
    _result, pull_kwargs, updates = _run_safety_sync(monkeypatch, watermark=stale)

    assert pull_kwargs["full_pull"] is True
    assert len(updates) == 1


def test_child_safety_pull_skips_when_watermark_is_fresh(monkeypatch, capsys):
    fresh = datetime.now(timezone.utc).isoformat()
    _result, pull_kwargs, updates = _run_safety_sync(monkeypatch, watermark=fresh)

    assert pull_kwargs["full_pull"] is False
    assert updates == []
    assert f"child-change safety pull skipped: fresh watermark last={fresh}" in capsys.readouterr().out


def test_child_safety_pull_advances_watermark_with_row_level_review_issues(monkeypatch):
    result = {"pulled": 0, "total": 1, "errors": ["cloud child reconciliation failed"]}
    _result, pull_kwargs, updates = _run_safety_sync(
        monkeypatch,
        watermark=None,
        pull_result=result,
    )

    assert pull_kwargs["full_pull"] is True
    assert len(updates) == 1


def test_measurement_review_issue_does_not_block_child_safety_watermark(monkeypatch):
    result = {
        "pulled": 0,
        "total": 1,
        "errors": ["obs 11: skipped cloud measurement 1344 because the local copy changed"],
    }
    _result, pull_kwargs, updates = _run_safety_sync(
        monkeypatch,
        watermark=None,
        pull_result=result,
    )

    assert pull_kwargs["full_pull"] is True
    assert len(updates) == 1


@pytest.mark.parametrize(
    "failure",
    [
        RuntimeError("temporary cloud failure"),
        cloud_sync.CloudSyncError("Authentication required"),
        cloud_sync.CloudSyncError("bulk observation_images fetch failed"),
    ],
    ids=["transient", "auth", "bulk-child-fetch"],
)
def test_child_safety_pull_does_not_advance_watermark_on_pass_failure(monkeypatch, failure):
    updates: list[dict] = []
    with pytest.raises(type(failure), match=str(failure)):
        _run_safety_sync(
            monkeypatch,
            watermark=None,
            pull_error=failure,
            settings_updates=updates,
        )
    assert updates == []


def test_fresh_child_safety_watermark_keeps_normal_second_refresh_fast(monkeypatch):
    fresh = datetime.now(timezone.utc).isoformat()
    _first, first_kwargs, _updates = _run_safety_sync(monkeypatch, watermark=fresh)
    _second, second_kwargs, _updates = _run_safety_sync(monkeypatch, watermark=fresh)

    assert first_kwargs["full_pull"] is False
    assert second_kwargs["full_pull"] is False


def test_successful_child_safety_pull_makes_next_refresh_fast(monkeypatch):
    settings_state: dict[str, str] = {}
    _first, first_kwargs, first_updates = _run_safety_sync(
        monkeypatch,
        watermark=None,
        settings_state=settings_state,
    )
    _second, second_kwargs, second_updates = _run_safety_sync(
        monkeypatch,
        watermark=None,
        settings_state=settings_state,
    )

    assert first_kwargs["full_pull"] is True
    assert len(first_updates) == 1
    assert second_kwargs["full_pull"] is False
    assert second_updates == []


def test_child_safety_pull_selects_deep_metadata_reconciliation_without_media(monkeypatch):
    """Deep mode covers image metadata and measurement snapshot reconciliation."""
    _result, pull_kwargs, _updates = _run_safety_sync(monkeypatch, watermark=None)

    assert pull_kwargs["full_pull"] is True
    assert pull_kwargs["sync_images"] is False
    assert pull_kwargs["materialize_remote_images"] is False


# ---------------------------------------------------------------------------
# Steady-state convergence — the core promise of this fix
# ---------------------------------------------------------------------------


def _seed_snapshot_matching_remote(db_path, cloud_id, remote):
    """Seed a stored snapshot whose observation part matches the given remote."""
    from utils.cloud_sync import _SNAPSHOT_OBS_FIELDS, _normalize_snapshot_value
    obs_part = {
        field: _normalize_snapshot_value(remote.get(field))
        for field in _SNAPSHOT_OBS_FIELDS
    }
    snapshot = '{"observation": %s, "images": [], "measurements": []}' % (
        __import__("json").dumps(obs_part, sort_keys=True)
    )
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?)",
        (cloud_sync._cloud_observation_snapshot_key(cloud_id), snapshot),
    )
    conn.commit()
    conn.close()


def test_fast_pull_converges_when_remote_updated_at_bumped_but_fields_unchanged(tmp_path, monkeypatch):
    """The exact live-log scenario: 119 observations survive prune because
    their remote updated_at is newer than local synced_at, but the fields
    haven't actually changed. First fast pull must converge them so the
    second fast pull sees zero candidates and zero bulk fetches."""
    db_path = _init_db(tmp_path)
    _default_monkeypatches(monkeypatch, db_path)

    # Seed a synced observation whose remote updated_at is newer than local
    # synced_at (the "119 survivors" pattern).
    cloud_id = "cloud-conv"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO observations (id, date, cloud_id, sync_status, synced_at, genus, species) "
        "VALUES (?, '2026-07-15', ?, 'synced', ?, 'Panaeolus', 'reticulatus')",
        (777, cloud_id, "2026-07-15T10:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    remote = {
        "id": cloud_id,
        "desktop_id": 777,
        "updated_at": "2026-07-15T12:00:00+00:00",  # newer than local synced_at
        "genus": "Panaeolus",
        "species": "reticulatus",
    }
    _seed_snapshot_matching_remote(db_path, cloud_id, remote)

    client = _RecordingClient([remote])

    # First fast pull: candidate survives updated_at prune, cheap-convergence
    # branch fires (observation fields match) → no bulk fetch.
    cloud_sync.pull_all(
        client,
        remote_obs=[remote],
        sync_calibrations=False,
        materialize_remote_images=False,
        sync_images=False,
        full_pull=False,
    )
    assert client.bulk_image_calls == [], (
        f"Cheap convergence must skip bulk fetch when observation fields match "
        f"the snapshot; got {client.bulk_image_calls}"
    )

    # After that pass, synced_at should be bumped to >= remote.updated_at.
    row = _connect(db_path).execute(
        "SELECT synced_at, sync_status FROM observations WHERE id = 777"
    ).fetchone()
    from utils.cloud_sync import _parse_sync_timestamp
    remote_ts = _parse_sync_timestamp(remote["updated_at"])
    local_ts = _parse_sync_timestamp(row["synced_at"])
    assert row["sync_status"] == "synced"
    assert local_ts is not None and remote_ts is not None
    assert local_ts >= remote_ts, (
        f"synced_at must be at least as recent as remote updated_at after "
        f"convergence; got synced_at={row['synced_at']} vs remote={remote['updated_at']}"
    )

    # Second fast pull: nothing should be a candidate. Zero bulk fetches, and
    # `fast_path_used=True` in the early-return.
    client2 = _RecordingClient([remote])
    result2 = cloud_sync.pull_all(
        client2,
        remote_obs=[remote],
        sync_calibrations=False,
        materialize_remote_images=False,
        sync_images=False,
        full_pull=False,
    )
    assert client2.bulk_image_calls == []
    assert result2.get("fast_path_used") is True, (
        "Second fast pull must reach the no-op fast path early-return; the "
        "steady-state convergence goal is that repeated Refresh does no "
        "network work when nothing changed"
    )
    assert result2.get("skipped_unchanged") == 1


def test_fast_pull_deep_convergence_when_reconcile_finds_no_change(tmp_path, monkeypatch):
    """If the fast-path cheap convergence branch can't decide (e.g. missing
    timestamp on either side) the candidate falls through to the reconcile
    loop. When that loop finds remote_changed=False, the new else branch
    must stamp synced so the next pull converges."""
    db_path = _init_db(tmp_path)
    _default_monkeypatches(monkeypatch, db_path)

    cloud_id = "cloud-deep"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO observations (id, date, cloud_id, sync_status, synced_at, genus, species) "
        "VALUES (?, '2026-07-15', ?, 'synced', NULL, 'Panaeolus', 'reticulatus')",
        (778, cloud_id),
    )
    conn.commit()
    conn.close()

    remote = {
        "id": cloud_id,
        "desktop_id": 778,
        "updated_at": "2026-07-15T12:00:00+00:00",
        "genus": "Panaeolus",
        "species": "reticulatus",
    }
    _seed_snapshot_matching_remote(db_path, cloud_id, remote)

    client = _RecordingClient([remote])
    cloud_sync.pull_all(
        client,
        remote_obs=[remote],
        sync_calibrations=False,
        materialize_remote_images=False,
        sync_images=False,
        full_pull=False,
    )
    row = _connect(db_path).execute(
        "SELECT synced_at, sync_status FROM observations WHERE id = 778"
    ).fetchone()
    assert row["sync_status"] == "synced"
    assert row["synced_at"] is not None, (
        "Deep reconcile path must stamp synced_at even when remote_changed=False"
    )


# ---------------------------------------------------------------------------
# Timestamp parsing edge cases
# ---------------------------------------------------------------------------


class TestParseSyncTimestamp:
    def test_iso_with_z_suffix(self):
        ts = cloud_sync._parse_sync_timestamp("2026-07-15T10:00:00Z")
        assert ts is not None
        assert ts.tzinfo is not None

    def test_iso_with_utc_offset(self):
        ts = cloud_sync._parse_sync_timestamp("2026-07-15T10:00:00+00:00")
        assert ts is not None

    def test_iso_with_non_utc_offset_normalizes_to_utc(self):
        ts1 = cloud_sync._parse_sync_timestamp("2026-07-15T12:00:00+02:00")
        ts2 = cloud_sync._parse_sync_timestamp("2026-07-15T10:00:00+00:00")
        assert ts1 == ts2

    def test_iso_naive_treated_as_utc(self):
        naive = cloud_sync._parse_sync_timestamp("2026-07-15T10:00:00")
        utc = cloud_sync._parse_sync_timestamp("2026-07-15T10:00:00+00:00")
        assert naive == utc

    def test_microsecond_precision(self):
        ts_a = cloud_sync._parse_sync_timestamp("2026-07-15T10:00:00.123456+00:00")
        ts_b = cloud_sync._parse_sync_timestamp("2026-07-15T10:00:00.123457+00:00")
        assert ts_b > ts_a

    def test_empty_or_none_returns_none(self):
        assert cloud_sync._parse_sync_timestamp(None) is None
        assert cloud_sync._parse_sync_timestamp("") is None
        assert cloud_sync._parse_sync_timestamp("  ") is None

    def test_invalid_returns_none(self):
        assert cloud_sync._parse_sync_timestamp("not-a-timestamp") is None

    def test_equal_timestamps_compare_equal(self):
        ts_a = cloud_sync._parse_sync_timestamp("2026-07-15T10:00:00+00:00")
        ts_b = cloud_sync._parse_sync_timestamp("2026-07-15T10:00:00+00:00")
        assert ts_a == ts_b
        # Prune inclusive: equality means "unchanged", NOT newer.
        assert not (ts_b > ts_a)
