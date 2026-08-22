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


def test_ai_selection_local_only_fields_do_not_redirty_after_push():
    assert not _remaining_local_changes_after_remote_merge(
        {
            "local_only_fields": [
                "ai_selected_at",
                "ai_selected_probability",
                "ai_selected_scientific_name",
                "ai_selected_service",
                "ai_selected_taxon_id",
            ],
            "conflict_fields": [],
        },
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

        def push_image_metadata(self, img, obs_cloud_id, storage_path):
            # This test only exercises the observation-level signature-refresh
            # contract; accept image metadata PATCH calls as a no-op so the
            # new metadata-only branch doesn't error out here.
            return str(img.get("cloud_id") or "cloud-image-noop")

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


def test_metadata_only_refresh_patches_image_metadata_on_existing_cloud_rows(tmp_path, monkeypatch, capsys):
    """The obs 631 regression: on a normal Refresh (sync_images=False), an
    observation whose LOCAL images gained new tag values (sample_source,
    sample_type, mount_medium, ...) must trigger a metadata PATCH on the
    already-linked cloud image rows. Previously the whole image-sync
    branch lived inside `if sync_images:`, so a fast Refresh happily
    stamped the observation 'synced' without ever calling
    `push_image_metadata` — cloud rows kept stale NULL / Not_set values
    forever. The new branch below runs the metadata PATCH without any
    byte upload, without `prepare_images_cb`, and only for images that
    already carry `cloud_id`."""
    db_path = tmp_path / "meta_only_refresh.sqlite"
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
            sample_source TEXT,
            contrast TEXT,
            measure_color TEXT,
            crop_mode TEXT,
            notes TEXT,
            gps_source INTEGER,
            ai_crop_x1 REAL, ai_crop_y1 REAL, ai_crop_x2 REAL, ai_crop_y2 REAL,
            ai_crop_source_w INTEGER, ai_crop_source_h INTEGER, ai_crop_is_custom INTEGER,
            calibration_id INTEGER,
            created_at TEXT,
            synced_at TEXT
        );
        CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE spore_measurements (
            id INTEGER PRIMARY KEY,
            image_id INTEGER,
            length_um REAL, width_um REAL,
            measurement_type TEXT,
            notes TEXT,
            p1_x REAL, p1_y REAL, p2_x REAL, p2_y REAL,
            p3_x REAL, p3_y REAL, p4_x REAL, p4_y REAL,
            gallery_rotation REAL,
            measured_at TEXT
        );
        -- Observation 389 shape: dirty (from the user's tag edit).
        INSERT INTO observations (id, date, cloud_id, sync_status) VALUES (389, '2026-06-02', 'cloud-631', 'dirty');
        -- Seed a microscope metadata-only anchor with PRE-edit tag values.
        -- After we capture the signature below, we update the row to the
        -- POST-edit values so the diagnostic sees only-metadata-fields-changed.
        INSERT INTO images (id, observation_id, image_type, cloud_id, sort_order, filepath,
                            sample_type, sample_source, mount_medium, stain, contrast, created_at)
            VALUES (871, 389, 'microscope', '3124', 3, '/tmp/micro.jpg',
                    'Not_set', NULL, 'Not_set', 'Not_set', 'DIC', '2026-06-03');
        INSERT INTO spore_measurements (id, image_id, length_um, width_um, measured_at)
            VALUES (1, 871, 10.0, 5.0, '2026-06-03');
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: _connect(db_path))
    from database import models
    monkeypatch.setattr(models, "get_connection", lambda: _connect(db_path))

    # Capture the local signature in the PRE-edit state (matches current DB
    # in shape but with the "old" tag values), then mutate the DB to the
    # POST-edit state and store the captured signature as the "last synced"
    # baseline. This is how the diagnostic sees a legitimate metadata-only
    # diff — only sample_type / sample_source / mount_medium changed.
    pre_edit_signature = cloud_sync._local_cloud_media_signature(389)
    conn = _connect(db_path)
    conn.execute(
        """
        UPDATE images
           SET sample_type = 'Fresh',
               sample_source = 'Hymenium',
               mount_medium = 'KOH'
         WHERE id = 871
        """
    )
    conn.execute(
        "INSERT INTO settings (key, value) VALUES ('sporely_cloud_local_media_sig_obs_389', ?)",
        (pre_edit_signature,),
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_pending_local_images",
                        lambda **_kwargs: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations",
                        lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_reconcile_missing_spore_measurements", lambda *a, **kw: 0)
    monkeypatch.setattr(cloud_sync, "_reconcile_missing_spore_summaries", lambda *a, **kw: 0)
    monkeypatch.setattr(cloud_sync, "_push_summary_for_current_observation", lambda *a, **kw: None)

    patch_calls: list[dict] = []
    post_calls: list[dict] = []

    class _RecordingClient:
        user_id = "user-obs-631"

        def push_observation(self, obs, remote_obs=None):
            return str(obs.get("cloud_id") or "cloud-631")

        def get_observation(self, cloud_id):
            return {"id": str(cloud_id), "desktop_id": 389}

        def pull_image_metadata(self, cloud_id, **kwargs):
            return [{
                "id": "3124", "desktop_id": 871, "observation_id": "cloud-631",
                "image_type": "microscope", "sort_order": 3,
                "storage_path": None,           # metadata-only anchor
                "sample_type": "Not_set", "sample_source": None,
                "mount_medium": "Not_set", "stain": "Not_set", "contrast": "DIC",
            }]

        # Capability probes — everything except sample_source is off in the
        # minimal test schema; sample_source is on because Stage 2A is live.
        def _observation_images_support_ai_crop(self): return False
        def _observation_images_support_ai_crop_custom(self): return False
        def _observation_images_support_upload_metadata(self): return False
        def _observation_images_support_storage_exif_safe(self): return False
        def _observation_images_support_sample_source(self): return True
        def _set_observation_media_keys(self, *a, **kw): return None

        _img_store = [
            {"id": "3124", "desktop_id": 871, "user_id": "user-obs-631",
             "observation_id": "cloud-631", "deleted_at": None},
        ]

        def _get(self, path):
            import re
            rows = [dict(r) for r in self._img_store]
            for param in ("id", "desktop_id", "user_id", "observation_id"):
                m = re.search(rf"[?&]{param}=eq\.([^&]+)", path)
                if m:
                    rows = [r for r in rows if str(r.get(param) or "") == m.group(1)]
            if "&limit=1" in path:
                rows = rows[:1]
            return rows

        def _find_cloud_image(self, desktop_id, obs_cloud_id, **kw):
            return {"id": "3124", "deleted_at": None} if int(desktop_id or 0) == 871 else None

        def _resolve_existing_image_for_push(self, img, obs_cloud_id, **kw):
            return cloud_sync.SporelyCloudClient._resolve_existing_image_for_push(
                self, img, obs_cloud_id, **kw
            )

        def _patch(self, path, payload):
            patch_calls.append({"path": path, "payload": dict(payload)})
            return []

        def _post(self, path, payload):
            post_calls.append({"path": path, "payload": dict(payload)})
            return [{"id": "unexpected-new"}]

        # Reuse the real client's push_image_metadata so the metadata-only
        # branch exercises the same code path production runs — PATCH via
        # `_patch`, no `upload_image_file` call.
        def push_image_metadata(self, img, obs_cloud_id, storage_path, **kw):
            return cloud_sync.SporelyCloudClient.push_image_metadata(
                self, img, obs_cloud_id, storage_path, **kw
            )

        # `_set_observation_media_keys` is invoked inside push_image_metadata;
        # keep the test fake compatible.
        _observation_supports_media_keys = lambda self: False
        _cloud_image_storage_key_cache: dict = {}

    client = _RecordingClient()
    cloud_sync.push_all(
        client,
        sync_images=False,
        sync_calibrations=False,
        remote_obs=[{"id": "cloud-631", "desktop_id": 389}],
    )

    # Observation was flipped to synced.
    row = _connect(db_path).execute(
        "SELECT sync_status FROM observations WHERE id = 389"
    ).fetchone()
    assert row["sync_status"] == "synced"

    # The critical assertion: image row was PATCHed with the new tag values.
    assert len(patch_calls) >= 1, (
        f"Fast Refresh must PATCH image metadata on already-linked cloud "
        f"rows when local tags changed; got patch_calls={patch_calls}"
    )
    assert len(post_calls) == 0, (
        f"Metadata-only PATCH must not POST a new cloud row; got {post_calls}"
    )

    # Find the image-metadata PATCH (path targets observation_images by cloud id).
    image_patches = [c for c in patch_calls if "observation_images?id=eq.3124" in c["path"]]
    assert len(image_patches) == 1, (
        f"Exactly one PATCH to the metadata-only anchor cloud row; "
        f"got {image_patches}"
    )
    body = image_patches[0]["payload"]
    assert body["sample_type"] == "Fresh"
    assert body["sample_source"] == "hymenium", (
        f"Push must send the lowercase cloud canonical; got "
        f"{body.get('sample_source')!r}"
    )
    assert body["mount_medium"] == "KOH"
    assert body["contrast"] == "DIC"
    # storage_path field is present but preserves the anchor state
    # (empty / normalized-empty) — no bytes were uploaded.
    assert not body.get("storage_path")

    out = capsys.readouterr().out
    assert "metadata-only image PATCH under Refresh" in out, (
        f"Expected the new diagnostic log line; got: {out}"
    )


def test_metadata_only_refresh_excludes_field_images_from_patch(
    tmp_path, monkeypatch, capsys,
):
    """Obs 389 regression: on a Refresh (`sync_images=False`), the
    metadata-only PATCH branch must not touch field-image cloud rows.

    Live symptom: RLS 42501 rejections for cloud ids 2317/2318 (local
    field images 838/839). PATCHing a field image with `storage_path`
    forced to NULL violates the RLS WITH CHECK
    (`storage_path IS NULL AND image_type = 'microscope'`), so PostgREST
    rejects every attempt and the observation keeps flipping back to
    dirty forever.

    Contract enforced here:
      * Only microscope metadata-only anchors are PATCHed.
      * Field images (even with cloud_id + valid `storage_path`) are
        not sent through `push_image_metadata`.
      * No POST, no `upload_image_file` invocation.
    """
    db_path = tmp_path / "meta_only_field_exclusion.sqlite"
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
            sample_source TEXT,
            contrast TEXT,
            measure_color TEXT,
            crop_mode TEXT,
            notes TEXT,
            gps_source INTEGER,
            ai_crop_x1 REAL, ai_crop_y1 REAL, ai_crop_x2 REAL, ai_crop_y2 REAL,
            ai_crop_source_w INTEGER, ai_crop_source_h INTEGER, ai_crop_is_custom INTEGER,
            calibration_id INTEGER,
            created_at TEXT,
            synced_at TEXT
        );
        CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE spore_measurements (
            id INTEGER PRIMARY KEY,
            image_id INTEGER,
            length_um REAL, width_um REAL,
            measurement_type TEXT,
            notes TEXT,
            p1_x REAL, p1_y REAL, p2_x REAL, p2_y REAL,
            p3_x REAL, p3_y REAL, p4_x REAL, p4_y REAL,
            gallery_rotation REAL,
            measured_at TEXT
        );
        -- Obs 389 shape: dirty from a tag edit on the microscope anchors.
        INSERT INTO observations (id, date, cloud_id, sync_status)
            VALUES (389, '2026-07-01', 'cloud-obs-389', 'dirty');
        -- Two field images already uploaded (valid cloud storage_path).
        -- These must NOT be PATCHed on a metadata-only Refresh.
        INSERT INTO images (id, observation_id, image_type, cloud_id, sort_order,
                            filepath, sample_type, sample_source, mount_medium,
                            stain, contrast, notes, created_at)
            VALUES
                (838, 389, 'field', '2317', 0, '/tmp/field1.jpg',
                 NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-01'),
                (839, 389, 'field', '2318', 1, '/tmp/field2.jpg',
                 NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-01');
        -- Microscope metadata-only anchors — these are eligible for PATCH.
        INSERT INTO images (id, observation_id, image_type, cloud_id, sort_order,
                            filepath, sample_type, sample_source, mount_medium,
                            stain, contrast, notes, created_at)
            VALUES
                (871, 389, 'microscope', '3124', 2, '/tmp/m1.jpg',
                 'Not_set', NULL, 'Not_set', 'Not_set', 'DIC', NULL, '2026-07-01'),
                (872, 389, 'microscope', '3125', 3, '/tmp/m2.jpg',
                 'Not_set', NULL, 'Not_set', 'Not_set', 'DIC', NULL, '2026-07-01');
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: _connect(db_path))
    from database import models
    monkeypatch.setattr(models, "get_connection", lambda: _connect(db_path))

    # Prime dirty prep metadata only on the microscope anchors: capture the
    # pre-edit signature, then apply the edit and store the pre-edit sig as
    # the "last synced" baseline. The field-image rows are unchanged.
    pre_edit_signature = cloud_sync._local_cloud_media_signature(389)
    conn = _connect(db_path)
    conn.execute(
        """
        UPDATE images
           SET sample_type = 'Fresh',
               sample_source = 'Hymenium',
               mount_medium = 'KOH'
         WHERE image_type = 'microscope'
        """
    )
    conn.execute(
        "INSERT INTO settings (key, value) "
        "VALUES ('sporely_cloud_local_media_sig_obs_389', ?)",
        (pre_edit_signature,),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_pending_local_images",
                        lambda **_kwargs: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations",
                        lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_reconcile_missing_spore_measurements", lambda *a, **kw: 0)
    monkeypatch.setattr(cloud_sync, "_reconcile_missing_spore_summaries", lambda *a, **kw: 0)
    monkeypatch.setattr(cloud_sync, "_push_summary_for_current_observation", lambda *a, **kw: None)

    push_metadata_calls: list[dict] = []
    upload_calls: list[str] = []
    patch_calls: list[dict] = []
    post_calls: list[dict] = []

    class _RecordingClient:
        user_id = "user-obs-389"

        def push_observation(self, obs, remote_obs=None):
            return str(obs.get("cloud_id") or "cloud-obs-389")

        def get_observation(self, cloud_id):
            return {"id": str(cloud_id), "desktop_id": 389}

        def pull_image_metadata(self, cloud_id, **kwargs):
            return [
                {
                    "id": "2317", "desktop_id": 838, "observation_id": "cloud-obs-389",
                    "image_type": "field", "sort_order": 0,
                    "storage_path": "user-obs-389/cloud-obs-389/838.webp",
                },
                {
                    "id": "2318", "desktop_id": 839, "observation_id": "cloud-obs-389",
                    "image_type": "field", "sort_order": 1,
                    "storage_path": "user-obs-389/cloud-obs-389/839.webp",
                },
                {
                    "id": "3124", "desktop_id": 871, "observation_id": "cloud-obs-389",
                    "image_type": "microscope", "sort_order": 2,
                    "storage_path": None,
                    "sample_type": "Not_set", "sample_source": None,
                    "mount_medium": "Not_set", "stain": "Not_set", "contrast": "DIC",
                },
                {
                    "id": "3125", "desktop_id": 872, "observation_id": "cloud-obs-389",
                    "image_type": "microscope", "sort_order": 3,
                    "storage_path": None,
                    "sample_type": "Not_set", "sample_source": None,
                    "mount_medium": "Not_set", "stain": "Not_set", "contrast": "DIC",
                },
            ]

        # Capability probes.
        def _observation_images_support_ai_crop(self): return False
        def _observation_images_support_ai_crop_custom(self): return False
        def _observation_images_support_upload_metadata(self): return False
        def _observation_images_support_storage_exif_safe(self): return False
        def _observation_images_support_sample_source(self): return True
        def _set_observation_media_keys(self, *a, **kw): return None

        _img_store = [
            {"id": "2317", "desktop_id": 838, "user_id": "user-obs-389", "observation_id": "cloud-obs-389", "deleted_at": None},
            {"id": "2318", "desktop_id": 839, "user_id": "user-obs-389", "observation_id": "cloud-obs-389", "deleted_at": None},
            {"id": "3124", "desktop_id": 871, "user_id": "user-obs-389", "observation_id": "cloud-obs-389", "deleted_at": None},
            {"id": "3125", "desktop_id": 872, "user_id": "user-obs-389", "observation_id": "cloud-obs-389", "deleted_at": None},
        ]

        def _get(self, path):
            import re
            rows = [dict(r) for r in self._img_store]
            for param in ("id", "desktop_id", "user_id", "observation_id"):
                m = re.search(rf"[?&]{param}=eq\.([^&]+)", path)
                if m:
                    rows = [r for r in rows if str(r.get(param) or "") == m.group(1)]
            if "&limit=1" in path:
                rows = rows[:1]
            return rows

        def _find_cloud_image(self, desktop_id, obs_cloud_id, **kw):
            _id = {838: "2317", 839: "2318", 871: "3124", 872: "3125"}.get(
                int(desktop_id or 0), None
            )
            return {"id": _id, "deleted_at": None} if _id else None

        def _resolve_existing_image_for_push(self, img, obs_cloud_id, **kw):
            return cloud_sync.SporelyCloudClient._resolve_existing_image_for_push(
                self, img, obs_cloud_id, **kw
            )

        def _patch(self, path, payload):
            patch_calls.append({"path": path, "payload": dict(payload)})
            return []

        def _post(self, path, payload):
            post_calls.append({"path": path, "payload": dict(payload)})
            return [{"id": "unexpected-new"}]

        def push_image_metadata(self, img, obs_cloud_id, storage_path, **kw):
            push_metadata_calls.append({
                "cloud_id": str(img.get("cloud_id") or ""),
                "image_type": str(img.get("image_type") or ""),
                "storage_path_arg": storage_path,
            })
            return cloud_sync.SporelyCloudClient.push_image_metadata(
                self, img, obs_cloud_id, storage_path, **kw
            )

        def upload_image_file(self, local_path, *args, **kwargs):
            upload_calls.append(str(local_path))
            return kwargs.get("storage_path") or "user/upload.webp"

        _observation_supports_media_keys = lambda self: False
        _cloud_image_storage_key_cache: dict = {}

    client = _RecordingClient()
    cloud_sync.push_all(
        client,
        sync_images=False,
        sync_calibrations=False,
        remote_obs=[{"id": "cloud-obs-389", "desktop_id": 389}],
    )

    # Field-image cloud ids must never see a PATCH / metadata push.
    patched_cloud_ids = {call["cloud_id"] for call in push_metadata_calls}
    assert "2317" not in patched_cloud_ids, (
        f"Field image cloud id 2317 must NOT be PATCHed under Refresh; "
        f"push_metadata_calls={push_metadata_calls}"
    )
    assert "2318" not in patched_cloud_ids, (
        f"Field image cloud id 2318 must NOT be PATCHed under Refresh; "
        f"push_metadata_calls={push_metadata_calls}"
    )
    field_patches = [c for c in patch_calls if "observation_images?id=eq.2317" in c["path"]
                     or "observation_images?id=eq.2318" in c["path"]]
    assert field_patches == [], (
        f"No cloud-image PATCH must target the field-image rows; "
        f"got {field_patches}"
    )

    # Microscope anchors still PATCH.
    microscope_patches = [
        c for c in patch_calls
        if "observation_images?id=eq.3124" in c["path"]
        or "observation_images?id=eq.3125" in c["path"]
    ]
    assert len(microscope_patches) == 2, (
        f"Both microscope metadata-only anchors must PATCH; "
        f"got {microscope_patches}"
    )
    for call in microscope_patches:
        body = call["payload"]
        assert body.get("sample_type") == "Fresh"
        assert body.get("sample_source") == "hymenium"
        assert body.get("mount_medium") == "KOH"
        # Metadata-only PATCH must not force storage_path — the cloud
        # anchor keeps its existing NULL value.
        assert "storage_path" not in body, (
            f"Metadata-only PATCH payload must omit storage_path; "
            f"got {body}"
        )

    # No POST (would create a duplicate cloud row) and no byte upload.
    assert post_calls == [], f"Metadata-only PATCH must not POST; got {post_calls}"
    assert upload_calls == [], (
        f"Metadata-only PATCH must not upload bytes; got {upload_calls}"
    )
