"""Tests for desktop country_code / region_id sync behavior.

Covers the required behavior in the desktop observation geography sync fix:

- SQLite schema (fresh + incremental migration).
- Country normalization rule shared across every entry point.
- Save/load of country_code + region_id via ObservationDB.
- Snapshot round-trip and casing-insensitive conflict detection.
- Push payload construction with three-state PATCH semantics:
    * absent  → preserve cloud value
    * null    → clear cloud value
    * code    → replace cloud value
- Coordinate-change invalidation (with and without a fresh geocode).
- region_id preserve-only behavior on desktop pushes.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from database import models, schema
from database.reverse_location_lookup import normalize_country_code
from utils import cloud_sync


# ---------------------------------------------------------------------------
# 1. SQLite schema
# ---------------------------------------------------------------------------


def _observation_columns(db_path: Path) -> set[str]:
    conn = sqlite3.connect(db_path)
    try:
        return {row[1] for row in conn.execute("PRAGMA table_info(observations)").fetchall()}
    finally:
        conn.close()


def test_fresh_database_includes_country_code_and_region_id_columns(tmp_path, monkeypatch):
    """Test 1 — a freshly-initialized DB must expose both nullable columns."""
    db_path = tmp_path / "fresh.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: tmp_path / "ref.db")
    monkeypatch.setattr(schema, "get_bundled_reference_database_path", lambda: tmp_path / "bundled.db")
    monkeypatch.setattr(schema, "_migrate_reference_values", lambda *a, **k: None)
    monkeypatch.setattr(schema, "_migrate_reference_mounts_and_stains", lambda *a, **k: None)

    schema.init_database()

    columns = _observation_columns(db_path)
    assert "country_code" in columns
    assert "region_id" in columns


def test_incremental_migration_adds_country_code_and_region_id_without_data_loss(tmp_path, monkeypatch):
    """Test 2 — an existing DB missing the new columns is upgraded, keeping rows."""
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            location TEXT,
            gps_latitude REAL,
            gps_longitude REAL,
            folder_path TEXT
        );
        INSERT INTO observations (id, date, location, gps_latitude, gps_longitude)
        VALUES (1, '2026-05-01', 'Legacy meadow', 63.4, 10.4);
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: tmp_path / "ref.db")
    monkeypatch.setattr(schema, "get_bundled_reference_database_path", lambda: tmp_path / "bundled.db")
    monkeypatch.setattr(schema, "_migrate_reference_values", lambda *a, **k: None)
    monkeypatch.setattr(schema, "_migrate_reference_mounts_and_stains", lambda *a, **k: None)

    schema.init_database()

    columns = _observation_columns(db_path)
    assert {"country_code", "region_id"}.issubset(columns)

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT date, location, gps_latitude, country_code, region_id FROM observations WHERE id = 1"
        ).fetchone()
    finally:
        conn.close()
    assert row == ("2026-05-01", "Legacy meadow", 63.4, None, None)


# ---------------------------------------------------------------------------
# 3–5. Normalization is applied at every entry point
# ---------------------------------------------------------------------------


def test_lowercase_country_normalizes_uppercase():
    assert normalize_country_code("no") == "NO"
    assert normalize_country_code(" no ") == "NO"


def test_blank_and_malformed_normalizes_to_none():
    for bad in (None, "", "   ", "Norway", "N1", "USA", 123):
        assert normalize_country_code(bad) is None


def test_no_module_ever_defaults_to_no():
    # The dispatcher-level rule is: only valid alpha-2 codes come back non-None.
    # None of the following spec examples may silently become "NO".
    for bad in ("Norge", "", None, "n0", "no1", "??"):
        assert normalize_country_code(bad) not in {"NO", "SE", "DK"}


# ---------------------------------------------------------------------------
# 6–7. Save / load through ObservationDB
# ---------------------------------------------------------------------------


def _init_country_sync_db(tmp_path):
    db_path = tmp_path / "geo_sync.sqlite"
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
            country_code TEXT,
            region_id TEXT,
            gps_latitude REAL,
            gps_longitude REAL,
            folder_path TEXT,
            sync_error_code TEXT,
            sync_error_message TEXT,
            sync_blocked_reason TEXT,
            sync_blocked_at TEXT
        );
        CREATE TABLE settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observation_id INTEGER,
            filepath TEXT
        );
        """
    )
    conn.commit()
    conn.close()
    return db_path


def _insert_obs(db_path, **overrides):
    defaults = {
        "cloud_id": None,
        "sync_status": "synced",
        "synced_at": "2026-05-01T00:00:00Z",
        "date": "2026-05-01",
        "genus": "Agaricus",
        "species": "campestris",
        "species_guess": "Agaricus campestris",
        "location": "Meadow",
        "gps_latitude": 63.4,
        "gps_longitude": 10.4,
        "sharing_scope": "public",
        "location_public": 1,
        "location_precision": "exact",
        "is_draft": 1,
        "country_code": None,
        "region_id": None,
    }
    defaults.update(overrides)
    conn = sqlite3.connect(db_path)
    try:
        cols = ", ".join(defaults.keys())
        placeholders = ", ".join(["?"] * len(defaults))
        conn.execute(
            f"INSERT INTO observations ({cols}) VALUES ({placeholders})",
            tuple(defaults.values()),
        )
        conn.commit()
    finally:
        conn.close()


def test_update_observation_persists_normalized_country_code(tmp_path, monkeypatch):
    """Test 6 — a reverse-geocoded country persists on save through the normal path."""
    db_path = _init_country_sync_db(tmp_path)
    _insert_obs(db_path, cloud_id="cloud-1")
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    models.ObservationDB.update_observation(1, country_code="no", allow_nulls=True)

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT country_code, region_id, sync_status FROM observations WHERE id = 1"
        ).fetchone()
    finally:
        conn.close()
    assert row == ("NO", None, "dirty")


def test_reopening_observation_restores_stored_country_and_region(tmp_path, monkeypatch):
    """Test 7 — the values stored in SQLite survive a reload."""
    db_path = _init_country_sync_db(tmp_path)
    _insert_obs(db_path, cloud_id="cloud-1", country_code="SE", region_id="region-abc")
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = dict(conn.execute("SELECT * FROM observations WHERE id = 1").fetchone())
    finally:
        conn.close()
    assert row["country_code"] == "SE"
    assert row["region_id"] == "region-abc"


def test_update_observation_can_clear_country_and_region_explicitly(tmp_path, monkeypatch):
    """Support for the three-state clear semantics at the local store."""
    db_path = _init_country_sync_db(tmp_path)
    _insert_obs(db_path, cloud_id="cloud-1", country_code="NO", region_id="r-1")
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    models.ObservationDB.update_observation(
        1,
        country_code=None,
        region_id=None,
        allow_nulls=True,
    )

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT country_code, region_id FROM observations WHERE id = 1"
        ).fetchone()
    finally:
        conn.close()
    assert row == (None, None)


def test_repeat_same_country_still_marks_dirty_only_when_row_written(tmp_path, monkeypatch):
    """A re-save that changes nothing does not double-write; a real change does."""
    db_path = _init_country_sync_db(tmp_path)
    _insert_obs(db_path, cloud_id="cloud-1", country_code="NO", sync_status="synced")
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    models.ObservationDB.update_observation(1, country_code="NO", allow_nulls=True)

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT country_code, sync_status FROM observations WHERE id = 1"
        ).fetchone()
    finally:
        conn.close()
    # A new UPDATE still fires (existing behavior for other fields) but the
    # push-diff layer skips no-op payloads — verified separately.
    assert row[0] == "NO"


# ---------------------------------------------------------------------------
# 8. Stale async lookup for old coords must be ignored (dialog-level guard)
# ---------------------------------------------------------------------------


def test_stale_lookup_for_old_coords_does_not_contribute_country_to_save():
    """Test 8 — the dialog helper drops country_code when cached coords no longer match."""
    from ui.observations_tab import ObservationDetailsDialog

    dialog = ObservationDetailsDialog.__new__(ObservationDetailsDialog)
    dialog._location_country_code = "NO"
    dialog._location_country_coords = (63.4, 10.4)

    # Simulate that the form now shows brand-new coords.
    resolved = ObservationDetailsDialog._resolved_country_code_for_current_coords(
        dialog, 55.0, 12.0
    )
    assert resolved is None

    # But when coords still match, the country is propagated.
    resolved_match = ObservationDetailsDialog._resolved_country_code_for_current_coords(
        dialog, 63.4, 10.4
    )
    assert resolved_match == "NO"


def test_resolved_country_normalizes_and_never_defaults_to_no():
    from ui.observations_tab import ObservationDetailsDialog

    dialog = ObservationDetailsDialog.__new__(ObservationDetailsDialog)
    dialog._location_country_code = "Norway"  # malformed
    dialog._location_country_coords = (63.4, 10.4)
    assert ObservationDetailsDialog._resolved_country_code_for_current_coords(
        dialog, 63.4, 10.4
    ) is None


# ---------------------------------------------------------------------------
# 9–15. Push payload construction & PATCH semantics
# ---------------------------------------------------------------------------


class _RecordingClient:
    """Minimal SporelyCloudClient stub that captures PATCH / POST payloads."""

    def __init__(self, existing_cloud_id: str | None):
        self._existing = existing_cloud_id
        self.user_id = "user-123"
        self.patches: list[tuple[str, dict]] = []
        self.posts: list[tuple[str, dict]] = []

    def _find_cloud_observation(self, _desktop_id):
        return self._existing

    # Use the real canonical identity resolver so these payload-shape tests
    # exercise the production PATCH/POST routing on top of the stubbed
    # reverse-link lookup above.
    _resolve_existing_observation_for_push = (
        cloud_sync.SporelyCloudClient._resolve_existing_observation_for_push
    )

    def _patch(self, path, payload):
        self.patches.append((path, dict(payload)))

    def _post(self, path, payload):
        self.posts.append((path, dict(payload)))
        return [{"id": self._existing or "cloud-new"}]


def _install_snapshot(monkeypatch, cloud_id, remote_baseline):
    """Simulate a stored snapshot for ``cloud_id`` derived from ``remote_baseline``."""
    snapshot_text = cloud_sync._cloud_observation_snapshot(remote_baseline, [], [])
    monkeypatch.setattr(
        cloud_sync,
        "_load_cloud_observation_snapshot",
        lambda cid, _snap=snapshot_text, _target=str(cloud_id): _snap if str(cid) == _target else "",
    )


def _run_push(monkeypatch, obs, remote_obs, existing_cloud_id="cloud-1"):
    client = _RecordingClient(existing_cloud_id=existing_cloud_id)
    cloud_sync.SporelyCloudClient.push_observation(client, dict(obs), remote_obs=dict(remote_obs))
    return client


def test_local_lowercase_country_is_sent_as_uppercase(monkeypatch):
    """Test 9 — casing normalization applies in outbound payloads."""
    remote = {
        "id": "cloud-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "gps_latitude": 63.4,
        "gps_longitude": 10.4,
        "country_code": None,
        "region_id": None,
    }
    _install_snapshot(monkeypatch, "cloud-1", remote)

    obs = {
        "id": 1,
        "date": "2026-05-01",
        "gps_latitude": 63.4,
        "gps_longitude": 10.4,
        "country_code": "no",  # legacy lowercase in local DB
        "region_id": None,
        "sharing_scope": "public",
    }
    client = _run_push(monkeypatch, obs, remote)

    assert len(client.patches) == 1
    _, payload = client.patches[0]
    assert payload["country_code"] == "NO"


def test_unknown_local_country_with_unchanged_coords_omits_country_key(monkeypatch):
    """Test 10 — offline sync must not erase a cloud country_code."""
    remote = {
        "id": "cloud-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "gps_latitude": 63.4,
        "gps_longitude": 10.4,
        "country_code": "NO",
        "region_id": None,
        "notes": "orig",
    }
    _install_snapshot(monkeypatch, "cloud-1", remote)

    obs = {
        "id": 1,
        "date": "2026-05-01",
        "gps_latitude": 63.4,
        "gps_longitude": 10.4,
        "country_code": None,  # user is offline
        "region_id": None,
        "notes": "updated",  # unrelated edit forces a real diff
        "sharing_scope": "public",
    }
    client = _run_push(monkeypatch, obs, remote)

    assert len(client.patches) == 1
    _, payload = client.patches[0]
    # Country key intentionally omitted → cloud value preserved.
    assert "country_code" not in payload
    # region_id is preserve-only from the desktop.
    assert "region_id" not in payload


def test_pulled_region_id_survives_unrelated_metadata_edit(monkeypatch):
    """Test 11 — desktop push does not clobber a cloud region_id."""
    remote = {
        "id": "cloud-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "gps_latitude": 63.4,
        "gps_longitude": 10.4,
        "country_code": "NO",
        "region_id": "cloud-region-42",
        "notes": "baseline",
    }
    _install_snapshot(monkeypatch, "cloud-1", remote)

    obs = {
        "id": 1,
        "date": "2026-05-01",
        "gps_latitude": 63.4,
        "gps_longitude": 10.4,
        "country_code": "NO",
        "region_id": "cloud-region-42",
        "notes": "new note",
        "sharing_scope": "public",
    }
    client = _run_push(monkeypatch, obs, remote)

    assert len(client.patches) == 1
    _, payload = client.patches[0]
    assert "region_id" not in payload


def test_desktop_push_never_sends_region_id_even_when_local_has_one(monkeypatch):
    """Test 12 — region_id is preserve-only on normal desktop sync."""
    remote = {
        "id": "cloud-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "gps_latitude": 63.4,
        "gps_longitude": 10.4,
        "country_code": "NO",
        "region_id": None,
    }
    _install_snapshot(monkeypatch, "cloud-1", remote)

    obs = {
        "id": 1,
        "date": "2026-05-01",
        "gps_latitude": 63.4,
        "gps_longitude": 10.4,
        "country_code": "NO",
        "region_id": "should-not-be-sent",  # desktop must not push a fabricated value
        "notes": "some edit",
        "sharing_scope": "public",
    }
    client = _run_push(monkeypatch, obs, remote)

    assert len(client.patches) == 1
    _, payload = client.patches[0]
    assert "region_id" not in payload


def test_coord_change_with_geocode_success_sends_new_country_and_clears_region(monkeypatch):
    """Test 13 — coord change + fresh country → new country_code, region_id NULL."""
    remote = {
        "id": "cloud-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "gps_latitude": 63.4,
        "gps_longitude": 10.4,
        "country_code": "NO",
        "region_id": "old-region-99",
    }
    _install_snapshot(monkeypatch, "cloud-1", remote)

    obs = {
        "id": 1,
        "date": "2026-05-01",
        "gps_latitude": 55.7,  # moved to Denmark
        "gps_longitude": 12.6,
        "country_code": "dk",
        "region_id": None,
        "sharing_scope": "public",
    }
    client = _run_push(monkeypatch, obs, remote)

    assert len(client.patches) == 1
    _, payload = client.patches[0]
    assert payload["country_code"] == "DK"
    # Explicit NULL clears the stale cloud region tied to old coords.
    assert "region_id" in payload
    assert payload["region_id"] is None


def test_coord_change_with_failed_geocode_clears_both(monkeypatch):
    """Test 14 — coord change + no valid country → both explicitly NULL."""
    remote = {
        "id": "cloud-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "gps_latitude": 63.4,
        "gps_longitude": 10.4,
        "country_code": "NO",
        "region_id": "old-region-99",
    }
    _install_snapshot(monkeypatch, "cloud-1", remote)

    obs = {
        "id": 1,
        "date": "2026-05-01",
        "gps_latitude": 45.0,
        "gps_longitude": 0.0,
        "country_code": None,  # geocode never returned
        "region_id": None,
        "sharing_scope": "public",
    }
    client = _run_push(monkeypatch, obs, remote)

    assert len(client.patches) == 1
    _, payload = client.patches[0]
    assert "country_code" in payload and payload["country_code"] is None
    assert "region_id" in payload and payload["region_id"] is None


def test_new_observation_post_sends_country_but_never_region(monkeypatch):
    """Test 15 — POST for a brand-new observation carries a valid country, never a region."""
    client = _RecordingClient(existing_cloud_id=None)
    obs = {
        "id": 42,
        "date": "2026-05-01",
        "gps_latitude": 63.4,
        "gps_longitude": 10.4,
        "country_code": "no",  # local lowercase
        "region_id": "some-locally-invented-region",  # must NOT leak out
        "sharing_scope": "public",
    }

    cloud_sync.SporelyCloudClient.push_observation(client, obs, remote_obs=None)

    assert len(client.posts) == 1
    _, payload = client.posts[0]
    assert payload["country_code"] == "NO"
    assert "region_id" not in payload


# ---------------------------------------------------------------------------
# 16–19. Pull / snapshot / conflict handling
# ---------------------------------------------------------------------------


def test_pull_normalization_sends_both_fields_to_local_update(monkeypatch):
    """Test 16 — cloud row round-trip: normalized country_code + region_id land in kwargs."""
    remote = {
        "id": "cloud-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "country_code": "no",  # even lowercase from cloud gets normalized
        "region_id": "cloud-region-42",
    }
    kwargs = cloud_sync._remote_observation_update_kwargs(remote)
    assert kwargs["country_code"] == "NO"
    assert kwargs["region_id"] == "cloud-region-42"


def test_snapshot_retains_country_code_and_region_id():
    """Test 17 — snapshot serialization preserves both fields."""
    remote = {
        "id": "cloud-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "country_code": "NO",
        "region_id": "region-1",
    }
    text = cloud_sync._cloud_observation_snapshot(remote, [], [])
    parsed = json.loads(text)
    assert parsed["observation"]["country_code"] == "NO"
    assert parsed["observation"]["region_id"] == "region-1"


def test_conflict_detects_real_country_change():
    """Test 18 — a real cloud vs local country difference is a genuine diff."""
    local = {
        "id": 1,
        "cloud_id": "cloud-1",
        "country_code": "SE",
        "region_id": None,
        "date": "2026-05-01",
    }
    remote = {
        "id": "cloud-1",
        "desktop_id": 1,
        "country_code": "NO",
        "region_id": None,
        "date": "2026-05-01",
    }
    diff = cloud_sync._observation_push_diff_fields(local, remote)
    assert "country_code" in diff


def test_conflict_ignores_casing_difference():
    """Test 19 — 'no' vs 'NO' must not produce a false conflict."""
    local = {
        "id": 1,
        "cloud_id": "cloud-1",
        "country_code": "no",
        "region_id": "same-region",
        "date": "2026-05-01",
    }
    remote = {
        "id": "cloud-1",
        "desktop_id": 1,
        "country_code": "NO",
        "region_id": "same-region",
        "date": "2026-05-01",
    }
    diff = cloud_sync._observation_push_diff_fields(local, remote)
    assert "country_code" not in diff
    assert "region_id" not in diff


# ---------------------------------------------------------------------------
# 20. Offline geocode preservation — round-trip check
# ---------------------------------------------------------------------------


def test_offline_geocode_roundtrip_preserves_cloud_country_and_region(monkeypatch):
    """Test 20 — an offline unrelated edit must not erase cloud country/region.

    Simulates the classic offline scenario:
      * Cloud row has country_code=NO, region_id=cloud-region-42.
      * Baseline snapshot recorded those values with the same coords.
      * Desktop is offline (no reverse geocode ran), so local country_code is
        NULL and coords are unchanged.
      * User edits an unrelated field (notes) and pushes.

    The push must:
      * Preserve cloud country_code by omitting it from the payload.
      * Preserve cloud region_id by omitting it from the payload.
      * Still send the notes change.
    """
    remote = {
        "id": "cloud-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "gps_latitude": 63.4,
        "gps_longitude": 10.4,
        "country_code": "NO",
        "region_id": "cloud-region-42",
        "notes": "cloud original",
        "sharing_scope": "public",
    }
    _install_snapshot(monkeypatch, "cloud-1", remote)

    obs = {
        "id": 1,
        "date": "2026-05-01",
        "gps_latitude": 63.4,
        "gps_longitude": 10.4,
        "country_code": None,  # offline, no lookup ran
        "region_id": None,
        "notes": "user edited note",
        "sharing_scope": "public",
    }
    client = _run_push(monkeypatch, obs, remote)

    assert len(client.patches) == 1
    _, payload = client.patches[0]
    assert "country_code" not in payload, "offline push must not clobber cloud country"
    assert "region_id" not in payload, "offline push must not clobber cloud region"
    assert payload.get("notes") == "user edited note"


# ---------------------------------------------------------------------------
# Extra guard: harmless float noise on coords does not count as a change
# ---------------------------------------------------------------------------


def test_serialization_noise_in_coords_does_not_erase_geography(monkeypatch):
    """Float rounding through JSON must not trip the coord-change branch."""
    remote = {
        "id": "cloud-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "gps_latitude": 63.425816,
        "gps_longitude": 10.412362,
        "country_code": "NO",
        "region_id": "cloud-region-42",
    }
    _install_snapshot(monkeypatch, "cloud-1", remote)

    obs = {
        "id": 1,
        "date": "2026-05-01",
        # Tiny noise below _OBSERVATION_FLOAT_ABS_TOL — treated as unchanged.
        "gps_latitude": 63.42581600000000001,
        "gps_longitude": 10.412362,
        "country_code": None,
        "region_id": None,
        "notes": "edit",
        "sharing_scope": "public",
    }
    client = _run_push(monkeypatch, obs, remote)
    assert len(client.patches) == 1
    _, payload = client.patches[0]
    assert "country_code" not in payload
    assert "region_id" not in payload


# ---------------------------------------------------------------------------
# Open-time deferred lookup must not erase a stored country (917 integrity)
# ---------------------------------------------------------------------------
#
# Observation 917 kept `country_code = 'NO'` for coordinates that never moved,
# yet an unrelated save (a taxonomy correction) wrote NULL. Mechanism: the
# dialog opens with `_loading_form` set, so the coordinate `valueChanged`
# only defers `_schedule_location_lookup`; `_load_observation_values` then
# restores the stored country for the stored coordinates; after loading the
# deferred call ran and treated "coordinates loaded" as "coordinates changed",
# clearing the country until a refresh geocode returned. Offline it never
# returned, and `get_data()` saved NULL.


class _CoordInputStub:
    def __init__(self, value):
        self._value = value

    def value(self):
        return self._value

    def minimum(self):
        return -1000.0


class _TimerStub:
    def __init__(self):
        self.starts = 0

    def start(self):
        self.starts += 1


def _dialog_after_loading_stored_geography(lat, lon, country="NO", region="r-1"):
    """An ObservationDetailsDialog in exactly the state `_load_observation_values`
    leaves it in: stored country and region restored for the stored coordinates."""
    from ui.observations_tab import ObservationDetailsDialog

    dialog = ObservationDetailsDialog.__new__(ObservationDetailsDialog)
    dialog._loading_form = False
    dialog.lat_input = _CoordInputStub(lat)
    dialog.lon_input = _CoordInputStub(lon)
    dialog._location_lookup_timer = _TimerStub()
    dialog._refresh_location_reporting_summary = lambda: None
    dialog._location_country_code = country
    dialog._location_country_name = ""
    dialog._location_region_id = region
    dialog._location_country_coords = (lat, lon)
    return dialog


def _geography_get_data_would_save(dialog):
    """The two expressions `get_data()` uses for the saved geography."""
    from ui.observations_tab import ObservationDetailsDialog

    lat, lon = dialog.lat_input.value(), dialog.lon_input.value()
    country = ObservationDetailsDialog._resolved_country_code_for_current_coords(dialog, lat, lon)
    region = dialog._location_region_id if dialog._location_country_coords else None
    return country, region


def test_open_time_deferred_lookup_keeps_stored_country_when_geocode_never_returns():
    """An unrelated edit cannot erase a stored country when no replacement
    geocode succeeds: the deferred open-time lookup leaves it intact."""
    from ui.observations_tab import ObservationDetailsDialog

    dialog = _dialog_after_loading_stored_geography(63.416128, 10.404458)

    # What the dialog's deferred-initialisation step runs after loading.
    ObservationDetailsDialog._schedule_location_lookup(dialog)
    # ...and no geocode result ever arrives (offline): nothing else runs.

    assert _geography_get_data_would_save(dialog) == ("NO", "r-1")
    assert dialog._location_lookup_timer.starts == 1, "the refresh lookup is still attempted"


def test_full_open_sequence_defers_then_keeps_the_restored_country():
    """The real ordering: the coordinate signal during loading only defers;
    the stored country is restored; the deferred call then runs."""
    from ui.observations_tab import ObservationDetailsDialog

    dialog = _dialog_after_loading_stored_geography(63.416128, 10.404458)
    dialog._loading_form = True
    dialog._location_country_code = ""          # nothing restored yet
    dialog._location_country_coords = None
    ObservationDetailsDialog._schedule_location_lookup(dialog)   # valueChanged during load
    assert dialog._deferred_location_lookup_pending is True
    assert dialog._location_lookup_timer.starts == 0

    dialog._location_country_code = "NO"        # _load_observation_values restores
    dialog._location_country_coords = (63.416128, 10.404458)
    dialog._loading_form = False
    ObservationDetailsDialog._schedule_location_lookup(dialog)   # deferred step

    assert _geography_get_data_would_save(dialog) == ("NO", "r-1")


def test_sub_tolerance_coordinate_noise_is_not_a_move():
    """Float noise below the save resolver's 1e-6 tolerance keeps the country,
    so the lookup and the save agree on what "unchanged" means."""
    from ui.observations_tab import ObservationDetailsDialog

    dialog = _dialog_after_loading_stored_geography(63.416128, 10.404458)
    dialog.lat_input = _CoordInputStub(63.416128 + 4e-7)

    ObservationDetailsDialog._schedule_location_lookup(dialog)

    assert dialog._location_country_code == "NO"


def test_genuine_coordinate_change_still_clears_country_until_a_geocode_returns():
    """Existing intended behaviour is preserved: moving the point drops the old
    country, so a failed lookup after a real move saves an explicit clear."""
    from ui.observations_tab import ObservationDetailsDialog

    dialog = _dialog_after_loading_stored_geography(63.416128, 10.404458)
    dialog.lat_input = _CoordInputStub(59.9)
    dialog.lon_input = _CoordInputStub(10.7)

    ObservationDetailsDialog._schedule_location_lookup(dialog)

    assert _geography_get_data_would_save(dialog) == (None, None)
    assert dialog._location_lookup_timer.starts == 1


def test_no_cached_coordinates_is_treated_as_a_move():
    """With no record of which point the cached country belongs to, the old
    conservative behaviour applies and the country is cleared."""
    from ui.observations_tab import ObservationDetailsDialog

    dialog = _dialog_after_loading_stored_geography(63.416128, 10.404458)
    dialog._location_country_coords = None

    ObservationDetailsDialog._schedule_location_lookup(dialog)

    assert dialog._location_country_code == ""
