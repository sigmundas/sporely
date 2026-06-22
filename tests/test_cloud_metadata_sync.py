from __future__ import annotations

import json
import sqlite3

from database import models
from utils import cloud_sync


def _init_metadata_sync_db(tmp_path):
    db_path = tmp_path / "metadata_sync.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cloud_id TEXT,
            sync_status TEXT,
            synced_at TEXT,
            date TEXT,
            genus TEXT,
            species TEXT,
            common_name TEXT,
            species_guess TEXT,
            notes TEXT,
            location TEXT,
            habitat TEXT,
            open_comment TEXT,
            private_comment TEXT,
            interesting_comment INTEGER,
            uncertain INTEGER,
            unspontaneous INTEGER,
            determination_method TEXT,
            sharing_scope TEXT,
            location_public INTEGER,
            location_precision TEXT,
            spore_data_visibility TEXT,
            is_draft INTEGER,
            publish_target TEXT,
            artsdata_id INTEGER,
            artportalen_id INTEGER,
            inaturalist_id INTEGER,
            mushroomobserver_id INTEGER,
            ai_selected_service TEXT,
            ai_selected_taxon_id TEXT,
            ai_selected_scientific_name TEXT,
            ai_selected_probability REAL,
            ai_selected_at TEXT,
            habitat_nin2_path TEXT,
            habitat_substrate_path TEXT,
            habitat_host_genus TEXT,
            habitat_host_species TEXT,
            habitat_host_common_name TEXT,
            habitat_nin2_note TEXT,
            habitat_substrate_note TEXT,
            habitat_grows_on_note TEXT,
            gps_latitude REAL,
            gps_longitude REAL,
            folder_path TEXT,
            user_id TEXT,
            sync_error_code TEXT,
            sync_error_message TEXT,
            sync_blocked_reason TEXT,
            sync_blocked_at TEXT
        );
        CREATE TABLE images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observation_id INTEGER,
            cloud_id TEXT,
            filepath TEXT,
            original_filepath TEXT,
            image_type TEXT,
            sort_order INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
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
            ai_crop_x1 REAL,
            ai_crop_y1 REAL,
            ai_crop_x2 REAL,
            ai_crop_y2 REAL,
            ai_crop_source_w INTEGER,
            ai_crop_source_h INTEGER,
            ai_crop_is_custom INTEGER,
            synced_at TEXT,
            source_role TEXT,
            file_purpose TEXT
        );
        CREATE TABLE spore_measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            image_id INTEGER NOT NULL,
            length_um REAL,
            width_um REAL,
            measurement_type TEXT,
            notes TEXT,
            p1_x REAL,
            p1_y REAL,
            p2_x REAL,
            p2_y REAL,
            p3_x REAL,
            p3_y REAL,
            p4_x REAL,
            p4_y REAL,
            gallery_rotation INTEGER,
            measured_at TEXT,
            cloud_id TEXT,
            desktop_id INTEGER
        );
        CREATE TABLE settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """
    )
    conn.commit()
    conn.close()
    return db_path


def _insert_observation(db_path, **overrides):
    defaults = {
        "cloud_id": None,
        "sync_status": "synced",
        "synced_at": "2026-05-01T00:00:00Z",
        "date": "2026-05-01",
        "genus": "Agaricus",
        "species": "campestris",
        "common_name": None,
        "species_guess": "Agaricus campestris",
        "notes": "baseline note",
        "location": None,
        "habitat": None,
        "open_comment": None,
        "private_comment": None,
        "interesting_comment": 0,
        "uncertain": 0,
        "unspontaneous": 0,
        "determination_method": None,
        "sharing_scope": "public",
        "location_public": 1,
        "location_precision": "exact",
        "spore_data_visibility": "public",
        "is_draft": 1,
        "publish_target": None,
        "artsdata_id": None,
        "artportalen_id": None,
        "inaturalist_id": None,
        "mushroomobserver_id": None,
        "ai_selected_service": None,
        "ai_selected_taxon_id": None,
        "ai_selected_scientific_name": None,
        "ai_selected_probability": None,
        "ai_selected_at": None,
        "habitat_nin2_path": None,
        "habitat_substrate_path": None,
        "habitat_host_genus": None,
        "habitat_host_species": None,
        "habitat_host_common_name": None,
        "habitat_nin2_note": None,
        "habitat_substrate_note": None,
        "habitat_grows_on_note": None,
        "gps_latitude": None,
        "gps_longitude": None,
        "folder_path": None,
        "user_id": "user-123",
        "sync_error_code": None,
        "sync_error_message": None,
        "sync_blocked_reason": None,
        "sync_blocked_at": None,
    }
    defaults.update(overrides)
    conn = sqlite3.connect(db_path)
    try:
        columns = ", ".join(defaults.keys())
        placeholders = ", ".join(["?"] * len(defaults))
        conn.execute(
            f"INSERT INTO observations ({columns}) VALUES ({placeholders})",
            tuple(defaults.values()),
        )
        conn.commit()
    finally:
        conn.close()


def _snapshot_observation(remote: dict) -> str:
    return cloud_sync._cloud_observation_snapshot(remote, [], [])


def test_update_observation_marks_cloud_linked_taxon_dirty(tmp_path, monkeypatch):
    db_path = _init_metadata_sync_db(tmp_path)
    _insert_observation(db_path, cloud_id="cloud-obs-1")
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    models.ObservationDB.update_observation(
        1,
        genus="Agaricus",
        species="arvensis",
        species_guess="Agaricus arvensis",
        allow_nulls=True,
    )

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT genus, species, sync_status FROM observations WHERE id = 1"
        ).fetchone()
    finally:
        conn.close()

    assert row == ("Agaricus", "arvensis", "dirty")


def test_sync_all_refreshes_remote_after_push_and_preserves_remote_only_metadata(monkeypatch, tmp_path):
    db_path = _init_metadata_sync_db(tmp_path)
    _insert_observation(db_path, cloud_id="cloud-obs-1")
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "pull_calibrations", lambda *args, **kwargs: {"pulled": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_load_linked_cloud_user_id", lambda: "user-123")
    monkeypatch.setattr(cloud_sync, "_save_linked_cloud_user_id", lambda user_id: None)
    monkeypatch.setattr(cloud_sync, "_backfill_missing_exif_on_cloud_images", lambda: None)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_local_media_signature_if_equivalent", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda *args, **kwargs: "")
    monkeypatch.setattr(cloud_sync, "_local_cloud_media_signature", lambda *args, **kwargs: "")
    monkeypatch.setattr(cloud_sync, "_detect_deleted_remote_observations", lambda remote_obs: [])
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *args, **kwargs: [])
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *args, **kwargs: None)

    baseline_remote = {
        "id": "cloud-obs-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "genus": "Agaricus",
        "species": "campestris",
        "common_name": None,
        "species_guess": "Agaricus campestris",
        "notes": "baseline note",
        "location": None,
        "habitat": None,
        "open_comment": None,
        "private_comment": None,
        "interesting_comment": False,
        "uncertain": False,
        "unspontaneous": False,
        "determination_method": None,
        "sharing_scope": "public",
        "location_public": True,
        "location_precision": "exact",
        "spore_data_visibility": "public",
        "is_draft": True,
        "publish_target": None,
        "artsdata_id": None,
        "artportalen_id": None,
        "inaturalist_id": None,
        "mushroomobserver_id": None,
        "ai_selected_service": None,
        "ai_selected_taxon_id": None,
        "ai_selected_scientific_name": None,
        "ai_selected_probability": None,
        "ai_selected_at": None,
        "habitat_nin2_path": None,
        "habitat_substrate_path": None,
        "habitat_host_genus": None,
        "habitat_host_species": None,
        "habitat_host_common_name": None,
        "habitat_nin2_note": None,
        "habitat_substrate_note": None,
        "habitat_grows_on_note": None,
    }
    remote_state = dict(baseline_remote, notes="cloud note")
    list_calls: list[int] = []
    push_calls: list[dict] = []

    class DummyClient:
        def fetch_current_user_id(self):
            return "user-123"

        def list_remote_observations(self):
            list_calls.append(len(list_calls) + 1)
            return [dict(remote_state)]

        def list_remote_calibrations(self):
            return []

        def pull_bulk_image_metadata(self, obs_cloud_ids):
            return []

        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            return []

        def pull_measurements_for_images(self, image_cloud_ids):
            return []

        def set_desktop_id(self, *args, **kwargs):
            return None

        def push_observation(self, obs, remote_obs=None, **kwargs):
            payload = dict(obs)
            push_calls.append(payload)
            for key, value in payload.items():
                if key in {"id", "user_id", "desktop_id"}:
                    continue
                remote_state[key] = value
            remote_state["id"] = "cloud-obs-1"
            remote_state["desktop_id"] = 1
            return "cloud-obs-1"

    models.ObservationDB.update_observation(
        1,
        genus="Agaricus",
        species="arvensis",
        species_guess="Agaricus arvensis",
        allow_nulls=True,
    )

    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: _snapshot_observation(baseline_remote))

    result = cloud_sync.sync_all(
        DummyClient(),
        sync_images=False,
        materialize_remote_images=False,
    )

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT genus, species, notes, sync_status FROM observations WHERE id = 1"
        ).fetchone()
    finally:
        conn.close()

    assert len(list_calls) == 2
    assert push_calls and push_calls[0]["genus"] == "Agaricus"
    assert push_calls[0]["species"] == "arvensis"
    assert push_calls[0]["species_guess"] == "Agaricus arvensis"
    assert push_calls[0]["notes"] == "cloud note"
    assert row == ("Agaricus", "arvensis", "cloud note", "synced")
    assert result["pushed"] == 1
    assert result["pulled"] == 1
    assert result["errors"] == []


def test_sync_all_ends_progress_on_neutral_finalizing_message(monkeypatch, tmp_path):
    """The last progress message must not be a stale per-calibration label.

    The UI shows the most recent message until the table refresh completes, so
    sync_all emits a neutral "Finalizing cloud sync…" after the last phase.
    """
    db_path = _init_metadata_sync_db(tmp_path)
    _insert_observation(db_path, cloud_id="cloud-obs-1")
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_pending_local_images", lambda: None)
    monkeypatch.setattr(
        cloud_sync,
        "push_calibrations",
        lambda *args, **kwargs: cloud_sync._emit_progress(
            kwargs.get("progress_cb"), "Checking calibration 1/1: 100X…", kwargs.get("progress_state")
        )
        or {"pushed": 0, "total": 1, "errors": []},
    )
    monkeypatch.setattr(
        cloud_sync,
        "pull_calibrations",
        lambda *args, **kwargs: cloud_sync._emit_progress(
            kwargs.get("progress_cb"), "Checking calibration 1/1: 100X…", kwargs.get("progress_state")
        )
        or {"pulled": 0, "total": 1, "errors": []},
    )
    monkeypatch.setattr(cloud_sync, "_load_linked_cloud_user_id", lambda: "user-123")
    monkeypatch.setattr(cloud_sync, "_save_linked_cloud_user_id", lambda user_id: None)
    monkeypatch.setattr(cloud_sync, "_backfill_missing_exif_on_cloud_images", lambda: None)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_detect_deleted_remote_observations", lambda remote_obs: [])
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *args, **kwargs: [])
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *args, **kwargs: None)

    class DummyClient:
        def fetch_current_user_id(self):
            return "user-123"

        def list_remote_observations(self):
            return []

        def list_remote_calibrations(self):
            return []

        def pull_bulk_image_metadata(self, obs_cloud_ids):
            return []

        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            return []

        def set_desktop_id(self, *args, **kwargs):
            return None

    messages: list[str] = []

    cloud_sync.sync_all(
        DummyClient(),
        progress_cb=lambda msg, cur, tot: messages.append(msg),
        sync_images=False,
        materialize_remote_images=False,
    )

    assert messages, "expected at least one progress message"
    assert messages[-1] == "Finalizing cloud sync…"
    assert "Finalizing cloud sync…" not in messages[:-1]


def test_pull_all_emits_phase_progress_messages(monkeypatch, tmp_path):
    """Each slow pull-preflight sub-step announces itself with truthful text."""
    db_path = tmp_path / "sporely.db"
    sqlite3.connect(db_path).close()
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_backfill_missing_exif_on_cloud_images", lambda: {"scanned": 0})
    monkeypatch.setattr(cloud_sync, "_load_local_observation_lookup", lambda: ({}, {}))
    monkeypatch.setattr(cloud_sync, "_find_local_observation_for_remote_cached", lambda *a, **k: None)
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: "")
    monkeypatch.setattr(
        cloud_sync,
        "_pull_remote_measurements_for_images",
        lambda client, ids: [{"id": "m1", "image_id": "img-1"}],
    )
    monkeypatch.setattr(cloud_sync, "_create_local_from_remote", lambda *a, **k: 1)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *a, **k: None)
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *a, **k: None)
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *a, **k: None)
    monkeypatch.setattr(cloud_sync, "_detect_deleted_remote_observations", lambda remote_obs: [])

    class DummyClient:
        def pull_bulk_image_metadata(self, ids):
            return [{"id": "img-1", "observation_id": "cloud-obs-1"}]

        def set_desktop_id(self, *args, **kwargs):
            return None

    messages: list[str] = []

    cloud_sync.pull_all(
        DummyClient(),
        progress_cb=lambda msg, cur, tot: messages.append(msg),
        remote_obs=[{"id": "cloud-obs-1", "desktop_id": 1, "date": "2026-05-01"}],
        sync_calibrations=False,
        materialize_remote_images=False,
    )

    assert "Checking image EXIF metadata…" in messages
    assert "Loading cloud image metadata…" in messages
    assert "Loading cloud measurements…" in messages
    # EXIF check announced before the image-metadata fetch.
    assert messages.index("Checking image EXIF metadata…") < messages.index("Loading cloud image metadata…")
    assert messages.index("Loading cloud image metadata…") < messages.index("Loading cloud measurements…")


def test_sync_all_emits_preflight_message_between_calibration_push_and_observation(monkeypatch, tmp_path, capsys):
    """A no-change sync must move the UI off the calibration label immediately.

    The dirty scans + remote refresh + pull preflight run before the first
    observation progress event. Without a neutral message the UI stayed stuck on
    "Syncing calibration N/M" for that whole gap. Assert the message right after
    the last calibration push event is the pre-observation preflight message.
    """
    db_path = _init_metadata_sync_db(tmp_path)
    _insert_observation(db_path, cloud_id="cloud-obs-1")
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_pending_local_images", lambda: None)
    monkeypatch.setattr(
        cloud_sync,
        "push_calibrations",
        lambda *args, **kwargs: cloud_sync._emit_progress(
            kwargs.get("progress_cb"), "Syncing calibration 8/8: 63X…", kwargs.get("progress_state")
        )
        or {"pushed": 0, "total": 8, "errors": []},
    )
    monkeypatch.setattr(
        cloud_sync,
        "pull_calibrations",
        lambda *args, **kwargs: cloud_sync._emit_progress(
            kwargs.get("progress_cb"), "Checking calibration 8/8: 63X…", kwargs.get("progress_state")
        )
        or {"pulled": 0, "total": 8, "errors": []},
    )
    monkeypatch.setattr(cloud_sync, "_load_linked_cloud_user_id", lambda: "user-123")
    monkeypatch.setattr(cloud_sync, "_save_linked_cloud_user_id", lambda user_id: None)
    monkeypatch.setattr(cloud_sync, "_backfill_missing_exif_on_cloud_images", lambda: None)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_detect_deleted_remote_observations", lambda remote_obs: [])
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *args, **kwargs: [])
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *args, **kwargs: None)

    class DummyClient:
        def fetch_current_user_id(self):
            return "user-123"

        def list_remote_observations(self):
            return []

        def list_remote_calibrations(self):
            return []

        def pull_bulk_image_metadata(self, obs_cloud_ids):
            return []

        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            return []

        def set_desktop_id(self, *args, **kwargs):
            return None

    messages: list[str] = []

    cloud_sync.sync_all(
        DummyClient(),
        progress_cb=lambda msg, cur, tot: messages.append(msg),
        sync_images=False,
        materialize_remote_images=False,
    )

    push_calibration_index = messages.index("Syncing calibration 8/8: 63X…")
    assert "Checking local observation changes…" in messages
    preflight_index = messages.index("Checking local observation changes…")
    # The UI moves off the calibration label straight into preflight.
    assert messages[push_calibration_index + 1] == "Checking local observation changes…"
    assert preflight_index > push_calibration_index
    # The pull-side preamble also announces itself before observation checking.
    assert "Preparing cloud observations…" in messages
    assert messages.index("Preparing cloud observations…") > preflight_index

    # Named, timed sub-step logs identify the actual slow step in the gap.
    output = capsys.readouterr().out
    assert "[cloud_sync] observation preflight: start" in output
    assert "observation preflight: media dirty scan complete" in output
    assert "observation preflight: local dirty scan complete count=" in output
    assert "observation preflight: complete" in output
    assert "observation preflight: remote observations refreshed count=" in output
    assert "pull preflight: candidate build complete count=" in output
    assert "pull preflight: cloud image metadata fetched" in output
    assert "pull preflight: complete" in output


def test_push_all_skips_noop_patch_and_clears_dirty_state_after_normalized_match(monkeypatch, tmp_path):
    db_path = _init_metadata_sync_db(tmp_path)
    _insert_observation(
        db_path,
        cloud_id="cloud-obs-1",
        sync_status="dirty",
        synced_at="2026-05-01T00:00:00Z",
        date="2026-05-01T12:34:56Z",
        genus="Agaricus",
        species="campestris",
        species_guess="Agaricus campestris",
        notes="baseline note",
        sharing_scope="public",
        location_public="1",
        location_precision="Exact",
        spore_data_visibility="Public",
        is_draft="0",
        interesting_comment="0",
        uncertain="0",
        unspontaneous="1",
        gps_latitude="63.0000000001",
        gps_longitude="10.0",
        ai_selected_probability="0.9700000001",
    )
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_pending_local_images", lambda: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_load_linked_cloud_user_id", lambda: "user-123")
    monkeypatch.setattr(cloud_sync, "_save_linked_cloud_user_id", lambda user_id: None)
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *args, **kwargs: [])
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)

    remote_state = {
        "id": "cloud-obs-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "genus": "Agaricus",
        "species": "campestris",
        "species_guess": "Agaricus campestris",
        "notes": "baseline note",
        "sharing_scope": "public",
        "visibility": "public",
        "location_public": True,
        "location_precision": "exact",
        "spore_data_visibility": "public",
        "is_draft": False,
        "interesting_comment": False,
        "uncertain": False,
        "unspontaneous": True,
        "gps_latitude": 63.0,
        "gps_longitude": 10.0,
        "ai_selected_probability": 0.97,
    }
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: _snapshot_observation(remote_state))

    client = cloud_sync.SporelyCloudClient("token", "user-123")
    patch_calls: list[tuple[str, dict]] = []
    monkeypatch.setattr(client, "fetch_current_user_id", lambda: "user-123")
    monkeypatch.setattr(client, "list_remote_observations", lambda: [dict(remote_state)])
    monkeypatch.setattr(client, "list_remote_calibrations", lambda: [])
    monkeypatch.setattr(client, "pull_bulk_image_metadata", lambda obs_cloud_ids: [])
    monkeypatch.setattr(client, "pull_image_metadata", lambda cloud_id, include_deleted_for_sync=False: [])
    monkeypatch.setattr(client, "pull_measurements_for_images", lambda image_cloud_ids: [])
    monkeypatch.setattr(client, "set_desktop_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(client, "_find_cloud_observation", lambda desktop_id: "cloud-obs-1")
    monkeypatch.setattr(client, "_patch", lambda path, payload: patch_calls.append((path, dict(payload))))
    monkeypatch.setattr(client, "_post", lambda *args, **kwargs: pytest.fail("no-op push should not POST"))

    first_result = cloud_sync.push_all(
        client,
        sync_images=False,
        sync_calibrations=False,
        remote_obs=[dict(remote_state)],
    )
    summary = cloud_sync._new_sync_summary()
    with cloud_sync._cloud_sync_summary_scope(summary):
        second_result = cloud_sync.push_all(
            client,
            sync_images=False,
            sync_calibrations=False,
            remote_obs=[dict(remote_state)],
        )

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT cloud_id, sync_status FROM observations WHERE id = 1"
        ).fetchone()
    finally:
        conn.close()

    assert patch_calls == []
    assert row == ("cloud-obs-1", "synced")
    assert first_result["pushed"] == 1
    assert second_result["pushed"] == 0
    assert second_result["sync_summary"]["observations_checked"] == 0
    assert second_result["sync_summary"]["observations_skipped_noop"] == 0


def test_pull_all_keeps_conflict_fields_local_and_reports_review_needed(monkeypatch, tmp_path):
    db_path = _init_metadata_sync_db(tmp_path)
    _insert_observation(
        db_path,
        cloud_id="cloud-obs-1",
        sync_status="dirty",
        synced_at="2026-05-01T00:00:00Z",
        genus="Local genus",
        species="campestris",
        species_guess="Local genus campestris",
    )
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_backfill_missing_exif_on_cloud_images", lambda: None)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_local_media_signature_if_equivalent", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda *args, **kwargs: "")
    monkeypatch.setattr(cloud_sync, "_local_cloud_media_signature", lambda *args, **kwargs: "")
    monkeypatch.setattr(cloud_sync, "_detect_deleted_remote_observations", lambda remote_obs: [])
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *args, **kwargs: [])
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *args, **kwargs: None)

    baseline_remote = {
        "id": "cloud-obs-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "genus": "Baseline genus",
        "species": "campestris",
        "species_guess": "Baseline genus campestris",
        "notes": "baseline note",
        "sharing_scope": "public",
        "location_public": True,
        "location_precision": "exact",
        "spore_data_visibility": "public",
        "is_draft": True,
    }
    remote_current = dict(baseline_remote, genus="Remote genus")

    class DummyClient:
        def fetch_current_user_id(self):
            return "user-123"

        def list_remote_observations(self):
            return [dict(remote_current)]

        def pull_bulk_image_metadata(self, obs_cloud_ids):
            return []

        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            return []

        def pull_measurements_for_images(self, image_cloud_ids):
            return []

        def set_desktop_id(self, *args, **kwargs):
            return None

    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: _snapshot_observation(baseline_remote))

    result = cloud_sync.pull_all(
        DummyClient(),
        remote_obs=[dict(remote_current)],
        sync_calibrations=False,
    )

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT genus, species, sync_status FROM observations WHERE id = 1"
        ).fetchone()
    finally:
        conn.close()

    assert row == ("Local genus", "campestris", "dirty")
    assert any("needs review" in str(error) for error in result["errors"])


def test_pull_all_reports_deleted_remote_observations_in_sync_summary(monkeypatch, tmp_path):
    db_path = _init_metadata_sync_db(tmp_path)
    _insert_observation(db_path, cloud_id="cloud-obs-1")
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_backfill_missing_exif_on_cloud_images", lambda: None)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_local_media_signature_if_equivalent", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda *args, **kwargs: "")
    monkeypatch.setattr(cloud_sync, "_local_cloud_media_signature", lambda *args, **kwargs: "")
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *args, **kwargs: [])
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        cloud_sync,
        "_detect_deleted_remote_observations",
        lambda remote_obs: [
            {
                "local_id": 1,
                "cloud_id": "cloud-obs-deleted",
                "title": "Agaricus campestris",
                "date": "2026-05-01",
                "location": None,
                "sync_status": "synced",
                "observation": {"id": "cloud-obs-deleted"},
            }
        ],
    )

    baseline_remote = {
        "id": "cloud-obs-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "genus": "Agaricus",
        "species": "campestris",
        "species_guess": "Agaricus campestris",
        "notes": "baseline note",
        "sharing_scope": "public",
        "location_public": True,
        "location_precision": "exact",
        "spore_data_visibility": "public",
        "is_draft": True,
    }
    remote_current = dict(baseline_remote, notes="remote note")

    class DummyClient:
        def fetch_current_user_id(self):
            return "user-123"

        def list_remote_observations(self):
            return [dict(remote_current)]

        def pull_bulk_image_metadata(self, obs_cloud_ids):
            return []

        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            return []

        def pull_measurements_for_images(self, image_cloud_ids):
            return []

        def set_desktop_id(self, *args, **kwargs):
            return None

    monkeypatch.setattr(
        cloud_sync,
        "_load_cloud_observation_snapshot",
        lambda cloud_id: _snapshot_observation(baseline_remote),
    )

    summary = cloud_sync._new_sync_summary()
    with cloud_sync._cloud_sync_summary_scope(summary):
        result = cloud_sync.pull_all(
            DummyClient(),
            remote_obs=[dict(remote_current)],
            sync_calibrations=False,
        )

    assert result["deleted_remote"] == [
        {
            "local_id": 1,
            "cloud_id": "cloud-obs-deleted",
            "title": "Agaricus campestris",
            "date": "2026-05-01",
            "location": None,
            "sync_status": "synced",
            "observation": {"id": "cloud-obs-deleted"},
        }
    ]
    assert result["sync_summary"]["observations_deleted_remote"] == 1
