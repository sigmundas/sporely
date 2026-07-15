"""Regression: metadata-only sync must reach steady state.

Reproduces the live-DB scenario the user hit with observation 368:

  * Observation is dirty from an earlier UI edit.
  * All images already have cloud_id — no pending byte upload.
  * The stored local media signature drifted from the current one for reasons
    the metadata-only sync cannot resolve (e.g. sample_type normalization,
    float precision re-rendering).
  * Push runs, cloud row gets patched, local `sync_status` stamped 'synced'.
  * Pull step sees the (unresolvable) signature drift and flips it back to
    'dirty' — so every subsequent sync selects the same row again.

The fixes verified here:

  1. Metadata-only push refreshes the stored media signature after a successful
     observation patch so the pull step's signature comparison agrees with
     current DB state.
  2. The pull-side `remaining_local_changes_after_remote_merge` decision
     ignores byte-level media drift when `sync_images=False`, since that mode
     cannot upload the bytes that would resolve it.
  3. `red_list_category` / `red_list_categories_json` are pushable so a
     desktop-derived value round-trips to the cloud and doesn't stay as a
     perpetual local-only diff.
"""

from __future__ import annotations

import json
import sqlite3

import pytest

from utils import cloud_sync
from utils.cloud_sync import (
    _local_media_signatures_match,
    _local_cloud_media_signature,
    _load_local_cloud_media_signature,
    _observation_push_payload,
    _refresh_local_cloud_media_signature,
    _remaining_local_changes_after_remote_merge,
    _OBS_PUSH_COLS,
)


# ---------------------------------------------------------------------------
# Push payload — red_list is now pushable so LC on desktop reaches cloud
# ---------------------------------------------------------------------------


def test_red_list_category_is_pushable_from_desktop():
    """The dirty loop for obs 368 was caused by red_list_category='LC' being
    dropped from the push payload — cloud stayed None, so pull re-flagged the
    field as a local-only diff every sync."""
    assert "red_list_category" in _OBS_PUSH_COLS
    assert "red_list_categories_json" in _OBS_PUSH_COLS


def test_push_payload_serializes_red_list_categories_json_as_object():
    """Local column is TEXT; cloud column is JSONB. The push payload must send
    a decoded object, not a quoted string, or PostgREST rejects it."""
    obs = {
        "date": "2026-07-15",
        "red_list_category": "LC",
        "red_list_categories_json": '{"NO": "LC"}',
    }
    payload = _observation_push_payload(obs, local=True)
    assert payload["red_list_category"] == "LC"
    assert payload["red_list_categories_json"] == {"NO": "LC"}


def test_push_payload_accepts_dict_red_list_categories_json():
    obs = {
        "date": "2026-07-15",
        "red_list_category": "NT",
        "red_list_categories_json": {"NO": "NT"},
    }
    payload = _observation_push_payload(obs, local=True)
    assert payload["red_list_categories_json"] == {"NO": "NT"}


def test_push_payload_red_list_null_stays_null():
    obs = {"date": "2026-07-15", "red_list_category": None, "red_list_categories_json": None}
    payload = _observation_push_payload(obs, local=True)
    assert payload["red_list_category"] is None
    assert payload["red_list_categories_json"] is None


# ---------------------------------------------------------------------------
# Pull-side dirty flip: metadata-only mode ignores byte-level drift
# ---------------------------------------------------------------------------


def test_metadata_only_mode_ignores_media_drift_in_remaining_changes():
    """The core knob: `_remaining_local_changes_after_remote_merge` is called
    by pull_all with `local_media_changed=effective_media_changed`, where
    `effective_media_changed = local_media_changed if sync_images else False`.
    Verify the underlying helper agrees with that shape."""
    # sync_images=True path (byte-level drift matters).
    assert _remaining_local_changes_after_remote_merge(
        {"local_only_fields": [], "conflict_fields": []},
        local_media_changed=True,
    )
    # sync_images=False path (byte-level drift explicitly zeroed out).
    assert not _remaining_local_changes_after_remote_merge(
        {"local_only_fields": [], "conflict_fields": []},
        local_media_changed=False,
    )


def test_metadata_only_mode_still_dirties_on_real_field_conflict():
    """Real metadata conflicts still block clearance — silent divergence would
    be worse than a lingering dirty state."""
    assert _remaining_local_changes_after_remote_merge(
        {"local_only_fields": [], "conflict_fields": ["notes"]},
        local_media_changed=False,
    )
    assert _remaining_local_changes_after_remote_merge(
        {"local_only_fields": ["location"], "conflict_fields": []},
        local_media_changed=False,
    )


# ---------------------------------------------------------------------------
# Signature refresh — the actual fix for the dirty loop
# ---------------------------------------------------------------------------


def _connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def test_signature_refresh_makes_current_and_stored_match(tmp_path, monkeypatch):
    """After metadata-only push refreshes the stored signature, the next call
    to `_local_media_signatures_match` returns True — that's what stops the
    pull-side dirty flip."""
    db_path = tmp_path / "sig_refresh.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (id INTEGER PRIMARY KEY, cloud_id TEXT, sync_status TEXT);
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
            gallery_rotation REAL
        );
        INSERT INTO observations (id, cloud_id, sync_status) VALUES (368, 'cloud-606', 'synced');
        INSERT INTO images (
            id, observation_id, image_type, cloud_id, sort_order, filepath, sample_type,
            scale_microns_per_pixel, created_at
        ) VALUES
            (708, 368, 'microscope', 'cloud-708', 3, '/tmp/img.jpg', NULL, 0.0534937320902084, '2026-05-22');
        -- Simulate a stale stored signature that references sample_type='Spore_print'
        -- and a slightly higher-precision float — the exact drift we saw live.
        INSERT INTO settings (key, value) VALUES
            (
                'sporely_cloud_local_media_sig_obs_368',
                '{"render_version":0,"cloud_media_signature":"","cloud_image_size_mode":"full","excluded_image_ids_raw":"[]","gallery_settings_raw":"","images":[{"id":708,"sample_type":"Spore_print","scale_microns_per_pixel":0.05349373209020841,"image_type":"microscope","sort_order":3}],"measurements":[]}'
            );
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: _connect(db_path))
    from database import models
    monkeypatch.setattr(models, "get_connection", lambda: _connect(db_path))

    # Before the fix: signatures do NOT match (stale sample_type + float
    # precision drift).
    stored_before = _load_local_cloud_media_signature(368)
    current_before = _local_cloud_media_signature(368)
    assert not _local_media_signatures_match(stored_before, current_before)

    # The fix applied inside push_all: refresh the stored signature after a
    # successful metadata-only push.
    _refresh_local_cloud_media_signature(368)

    stored_after = _load_local_cloud_media_signature(368)
    current_after = _local_cloud_media_signature(368)
    assert _local_media_signatures_match(stored_after, current_after), (
        "After metadata-only push refresh, stored + current signatures must "
        "match so the pull step doesn't re-flip the observation dirty"
    )


# ---------------------------------------------------------------------------
# End-to-end: push_all signature refresh path is wired for sync_images=False
# ---------------------------------------------------------------------------


def test_push_all_metadata_only_refreshes_signature_after_stamp(tmp_path, monkeypatch):
    """The full end-to-end guarantee: a successful metadata-only push must
    refresh the stored media signature so a repeat sync finds no drift."""
    db_path = tmp_path / "push_all_meta.sqlite"
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
            sync_blocked_at TEXT
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
            gallery_rotation REAL
        );
        INSERT INTO observations (id, date, cloud_id, sync_status) VALUES (777, '2026-07-15', 'cloud-777', 'dirty');
        INSERT INTO images (id, observation_id, image_type, cloud_id, sort_order, filepath, sample_type, scale_microns_per_pixel, created_at)
            VALUES (900, 777, 'field', 'cloud-900', 0, '/tmp/x.jpg', NULL, 0.1234567890123456, '2026-07-15');
        -- Seed a stale stored signature.
        INSERT INTO settings (key, value) VALUES
            ('sporely_cloud_local_media_sig_obs_777', '{"render_version":0,"images":[{"id":900,"sample_type":"Spore_print"}],"measurements":[]}');
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: _connect(db_path))
    from database import models
    monkeypatch.setattr(models, "get_connection", lambda: _connect(db_path))
    monkeypatch.setattr(
        cloud_sync,
        "_mark_cloud_observations_dirty_for_media_changes",
        lambda: None,
    )
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
        cloud_sync, "_reconcile_missing_spore_measurements", lambda *a, **kw: 0
    )
    monkeypatch.setattr(
        cloud_sync, "_reconcile_missing_spore_summaries", lambda *a, **kw: 0
    )
    # The spore-summary push runs downstream of the metadata push and would
    # crash on the minimal test schema (missing columns); stub it so we only
    # exercise the sync_status / signature refresh contract.
    monkeypatch.setattr(
        cloud_sync,
        "_push_summary_for_current_observation",
        lambda *a, **kw: None,
    )

    class _FakeClient:
        user_id = "user-123"

        def push_observation(self, obs, remote_obs=None):
            return str(obs.get("cloud_id") or "cloud-777")

        def get_observation(self, cloud_id):
            return {"id": str(cloud_id), "desktop_id": 777}

        def pull_image_metadata(self, cloud_id, **kwargs):
            return []

    # Before push: signatures drift.
    stored_before = _load_local_cloud_media_signature(777)
    current_before = _local_cloud_media_signature(777)
    assert not _local_media_signatures_match(stored_before, current_before)

    cloud_sync.push_all(
        _FakeClient(),
        sync_images=False,
        sync_calibrations=False,
        remote_obs=[{"id": "cloud-777", "desktop_id": 777}],
    )

    # After push: sync_status flipped to synced.
    row = _connect(db_path).execute(
        "SELECT sync_status FROM observations WHERE id = 777"
    ).fetchone()
    assert row["sync_status"] == "synced"

    # And the stored signature now matches current DB state so the next pull
    # won't see any drift to re-dirty the observation.
    stored_after = _load_local_cloud_media_signature(777)
    current_after = _local_cloud_media_signature(777)
    assert _local_media_signatures_match(stored_after, current_after), (
        "Metadata-only push must refresh the stored media signature to break "
        "the dirty loop for observations whose local signature has drifted"
    )


def test_push_all_metadata_only_logs_dirty_to_synced_transition(tmp_path, monkeypatch, capsys):
    """The diagnostic that lets us prove obs 368 actually flipped to synced."""
    db_path = tmp_path / "push_all_meta_log.sqlite"
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
            sync_blocked_at TEXT
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
            gallery_rotation REAL
        );
        INSERT INTO observations (id, date, cloud_id, sync_status) VALUES (777, '2026-07-15', 'cloud-777', 'dirty');
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: _connect(db_path))
    from database import models
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
        cloud_sync, "_reconcile_missing_spore_measurements", lambda *a, **kw: 0
    )
    monkeypatch.setattr(
        cloud_sync, "_reconcile_missing_spore_summaries", lambda *a, **kw: 0
    )
    # The spore-summary push runs downstream of the metadata push and would
    # crash on the minimal test schema (missing columns); stub it so we only
    # exercise the sync_status / signature refresh contract.
    monkeypatch.setattr(
        cloud_sync,
        "_push_summary_for_current_observation",
        lambda *a, **kw: None,
    )

    class _FakeClient:
        user_id = "user-123"

        def push_observation(self, obs, remote_obs=None):
            return "cloud-777"

        def get_observation(self, cloud_id):
            return {"id": str(cloud_id), "desktop_id": 777}

        def pull_image_metadata(self, cloud_id, **kwargs):
            return []

    cloud_sync.push_all(
        _FakeClient(),
        sync_images=False,
        sync_calibrations=False,
        remote_obs=[{"id": "cloud-777", "desktop_id": 777}],
    )
    out = capsys.readouterr().out
    assert "sync_status transition obs 777: dirty→synced caller=push_all" in out, (
        f"Expected transition log, got: {out}"
    )
