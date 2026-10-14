"""Observation push identity resolution.

Regression coverage for the Download-from-Cloud duplicate-observation bug:
``push_observation`` used to resolve identity only via the remote reverse
link (``observations.desktop_id``). Observations imported by pull-only sync
legitimately carry a local ``cloud_id`` while the remote ``desktop_id`` is
still NULL (pull-only performs zero cloud writes), so the reverse lookup
found nothing and a later normal push POSTed a duplicate cloud observation.

The canonical resolver under test is
``SporelyCloudClient._resolve_existing_observation_for_push``:

* verified local ``cloud_id`` is the only push target (PATCH, never POST);
* ``desktop_id`` is retired as an identity: never looked up, never sent
  (per-record sync keys, plan 2026-10-14; the create path and its
  ``sync_key`` adoption are covered by tests/test_sync_key_observation_identity.py);
* a ``cloud_id`` that does not verify raises
  ``ObservationIdentityConflictError`` and must leave the observation
  dirty/retryable — no PATCH, no POST, no snapshot.
"""

from __future__ import annotations

import re
import sqlite3

import pytest

from database import models
from utils import cloud_sync


# ---------------------------------------------------------------------------
# Fake user-scoped PostgREST observation store
# ---------------------------------------------------------------------------


class _FakeObservationCloud:
    """Serves the two identity lookups from an in-memory observation list.

    Handles the exact query shapes used by ``get_observation`` (direct link
    verification, user-scoped) and ``_find_cloud_observation`` (reverse-link
    recovery, user-scoped, ``order=id.asc``).
    """

    def __init__(self, rows: list[dict], current_user: str = "user-123"):
        self.rows = [dict(r) for r in rows]
        self.current_user = current_user
        self.get_paths: list[str] = []

    def get(self, path: str) -> list[dict]:
        self.get_paths.append(path)
        if not path.startswith("observations?"):
            raise AssertionError(f"unexpected GET path: {path}")

        def _eq(name: str):
            match = re.search(rf"[?&]{name}=eq\.([^&]+)", path)
            return match.group(1) if match else None

        user_id = _eq("user_id")
        assert user_id == self.current_user, "identity lookups must stay user-scoped"

        matches = [dict(r) for r in self.rows if str(r.get("user_id")) == user_id]
        direct_id = _eq("id")
        if direct_id is not None:
            matches = [r for r in matches if str(r.get("id")) == direct_id]
        desktop_id = _eq("desktop_id")
        if desktop_id is not None:
            matches = [r for r in matches if str(r.get("desktop_id") or "") == desktop_id]
        matches.sort(key=lambda r: str(r.get("id")))
        if "select=id" in path:
            return [{"id": r["id"]} for r in matches]
        return matches


def _make_client(monkeypatch, remote_rows: list[dict]) -> tuple[cloud_sync.SporelyCloudClient, dict]:
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    cloud = _FakeObservationCloud(remote_rows)
    calls: dict[str, list] = {"patch": [], "post": [], "cloud": cloud}
    monkeypatch.setattr(client, "_get", cloud.get)
    monkeypatch.setattr(
        client, "_patch", lambda path, payload: calls["patch"].append((path, dict(payload)))
    )

    def _fake_post(path, payload):
        calls["post"].append((path, dict(payload)))
        return [{"id": "new-cloud-id"}]

    monkeypatch.setattr(client, "_post", _fake_post)
    # Geography shaping reads the stored snapshot; identity tests do not
    # exercise geography, so serve an empty baseline.
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: "")
    return client, calls


_KEY = "2b3c4d5e-6f70-4182-93a4-b5c6d7e8f901"


def _local_obs(local_id: int, cloud_id: str | None = None, **overrides) -> dict:
    obs = {
        "id": local_id,
        "cloud_id": cloud_id,
        "date": "2026-08-01",
        "genus": "Agaricus",
        "species": "campestris",
        "species_guess": "Agaricus campestris",
        "notes": "field note",
        "sharing_scope": "public",
        "location_public": 1,
        "location_precision": "exact",
        "spore_data_visibility": "public",
        "is_draft": 0,
        "uncertain": 0,
        "unspontaneous": 0,
        "interesting_comment": 0,
    }
    obs.update(overrides)
    return obs


# ---------------------------------------------------------------------------
# Resolver unit tests (identity rules A–E, G)
# ---------------------------------------------------------------------------


def test_verified_local_cloud_id_is_primary_identity(monkeypatch):
    """Rule A: a verified direct link wins even with remote desktop_id NULL."""
    client, calls = _make_client(
        monkeypatch,
        [{"id": "1100", "desktop_id": None, "user_id": "user-123"}],
    )

    cloud_id = client.push_observation(_local_obs(762, cloud_id="1100"))

    assert cloud_id == "1100"
    assert [path for path, _ in calls["patch"]] == ["observations?id=eq.1100"]
    assert calls["post"] == [], "verified direct identity must never POST a replacement"


def test_patch_never_sends_an_identity_column(monkeypatch):
    """Rule 21: no client writes an identity onto an existing cloud row."""
    client, calls = _make_client(
        monkeypatch,
        [{"id": "1100", "desktop_id": None, "user_id": "user-123"}],
    )

    client.push_observation(_local_obs(762, cloud_id="1100", sync_key=_KEY))

    assert len(calls["patch"]) == 1
    path, payload = calls["patch"][0]
    assert path == "observations?id=eq.1100"
    assert "desktop_id" not in payload
    assert "sync_key" not in payload
    assert calls["post"] == []


def test_missing_local_cloud_id_creates_with_its_key_and_ignores_desktop_id(monkeypatch):
    """No direct link: POST with the committed key. A row carrying this local
    id as desktop_id (another device's, or duplicates) is never looked up."""
    client, calls = _make_client(
        monkeypatch,
        [
            {"id": "900", "desktop_id": 5, "user_id": "user-123"},
            {"id": "901", "desktop_id": 5, "user_id": "user-123"},
        ],
    )

    cloud_id = client.push_observation(_local_obs(5, cloud_id=None, sync_key=_KEY))

    assert cloud_id == "new-cloud-id"
    assert calls["patch"] == []
    assert [path for path, _ in calls["post"]] == ["observations"]
    assert calls["post"][0][1]["sync_key"] == _KEY
    assert "desktop_id" not in calls["post"][0][1]
    assert not any("desktop_id=" in path for path in calls["cloud"].get_paths)


@pytest.mark.parametrize(
    "remote_rows",
    [
        [],
        [{"id": "901", "desktop_id": 5, "user_id": "user-123"}],
    ],
    ids=["no-reverse-match", "unique-reverse-match"],
)
def test_dead_local_cloud_id_is_never_recreated_or_rerouted(monkeypatch, remote_rows):
    """A linked row whose cloud copy is gone fails closed: no POST of a
    replacement, and no PATCH of a row found by desktop_id."""
    client, calls = _make_client(monkeypatch, remote_rows)

    with pytest.raises(cloud_sync.ObservationIdentityConflictError):
        client.push_observation(_local_obs(5, cloud_id="9999", sync_key=_KEY))

    assert calls["patch"] == []
    assert calls["post"] == []


def test_local_cloud_id_owned_by_another_user_is_never_patched(monkeypatch):
    """Rule E: a foreign-owner row is invisible to the user-scoped client and
    must never be patched, nor replaced by a POST."""
    client, calls = _make_client(
        monkeypatch,
        [{"id": "1100", "desktop_id": None, "user_id": "someone-else"}],
    )

    with pytest.raises(cloud_sync.ObservationIdentityConflictError):
        client.push_observation(_local_obs(5, cloud_id="1100", sync_key=_KEY))

    assert calls["patch"] == [], "foreign-owner rows must never be hijacked via PATCH"
    assert calls["post"] == []


def test_verified_direct_link_ignores_a_row_carrying_this_desktop_id(monkeypatch):
    """cloud_id → 1100 verifies; another row with desktop_id=762 is not an
    identity and is neither consulted nor touched."""
    client, calls = _make_client(
        monkeypatch,
        [
            {"id": "1100", "desktop_id": None, "user_id": "user-123"},
            {"id": "1143", "desktop_id": 762, "user_id": "user-123"},
        ],
    )

    assert client.push_observation(_local_obs(762, cloud_id="1100")) == "1100"

    assert [path for path, _ in calls["patch"]] == ["observations?id=eq.1100"]
    assert calls["post"] == []
    assert not any("desktop_id=" in path for path in calls["cloud"].get_paths)


def test_identity_conflict_error_is_retryable_cloud_sync_error():
    """push_all's per-observation handler catches CloudSyncError and leaves the
    observation dirty; the identity conflict must ride that path."""
    assert issubclass(cloud_sync.ObservationIdentityConflictError, cloud_sync.CloudSyncError)


# ---------------------------------------------------------------------------
# push_all-level regressions (pull-only import → later normal push)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolate_reconciliation(monkeypatch):
    monkeypatch.setattr(cloud_sync, "_push_summary_for_current_observation", lambda *a, **k: None)
    monkeypatch.setattr(cloud_sync, "_reconcile_missing_spore_summaries", lambda *a, **k: 0)
    monkeypatch.setattr(cloud_sync, "_reconcile_missing_spore_measurements", lambda *a, **k: 0)


def _init_db(tmp_path):
    db_path = tmp_path / "push_identity.sqlite"
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
            synced_at TEXT,
            source_role TEXT,
            file_purpose TEXT
        );
        CREATE TABLE spore_measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            image_id INTEGER NOT NULL,
            length_um REAL,
            width_um REAL,
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


_REMOTE_FIELDS = {
    "date": "2026-08-01",
    "genus": "Agaricus",
    "species": "campestris",
    "common_name": None,
    "species_guess": "Agaricus campestris",
    "notes": "field note",
    "sharing_scope": "public",
    "visibility": "public",
    "location_public": True,
    "location_precision": "exact",
    "spore_data_visibility": "public",
    "is_draft": False,
    "interesting_comment": False,
    "uncertain": False,
    "unspontaneous": False,
}


def _insert_pull_only_import(db_path, local_id: int, cloud_id: str, **overrides):
    """A local row as Download from Cloud creates it: cloud_id set, synced."""
    values = {
        "id": local_id,
        "cloud_id": cloud_id,
        "sync_status": "synced",
        "synced_at": "2026-08-10T00:00:00Z",
        "date": _REMOTE_FIELDS["date"],
        "genus": _REMOTE_FIELDS["genus"],
        "species": _REMOTE_FIELDS["species"],
        "species_guess": _REMOTE_FIELDS["species_guess"],
        "notes": _REMOTE_FIELDS["notes"],
        "sharing_scope": "public",
        "location_public": 1,
        "location_precision": "exact",
        "spore_data_visibility": "public",
        "is_draft": 0,
        "interesting_comment": 0,
        "uncertain": 0,
        "unspontaneous": 0,
        "user_id": "user-123",
    }
    values.update(overrides)
    conn = sqlite3.connect(db_path)
    try:
        columns = ", ".join(values.keys())
        placeholders = ", ".join(["?"] * len(values))
        conn.execute(
            f"INSERT INTO observations ({columns}) VALUES ({placeholders})",
            tuple(values.values()),
        )
        conn.commit()
    finally:
        conn.close()


def _remote_observation(cloud_id: str, desktop_id=None, **overrides) -> dict:
    remote = {"id": cloud_id, "desktop_id": desktop_id, "user_id": "user-123"}
    remote.update(_REMOTE_FIELDS)
    remote.update(overrides)
    return remote


def _push_all_harness(monkeypatch, db_path, remote_rows: list[dict]):
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "_push_pending_image_tombstones", lambda client: [])
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *a, **k: [])
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *a, **k: None)
    monkeypatch.setattr(cloud_sync, "_store_local_media_signature_if_equivalent", lambda *a, **k: None)
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda *a, **k: "")
    monkeypatch.setattr(cloud_sync, "_local_cloud_media_signature", lambda *a, **k: "")

    snapshot_calls: list[str] = []
    monkeypatch.setattr(
        cloud_sync, "_store_remote_snapshot", lambda client, cloud_id: snapshot_calls.append(cloud_id)
    )
    # The stored snapshot equals current remote state: the pull-only import
    # captured it, and the cloud has not drifted since.
    snapshots = {
        str(row["id"]): cloud_sync._cloud_observation_snapshot(row, [], [])
        for row in remote_rows
    }
    monkeypatch.setattr(
        cloud_sync,
        "_load_cloud_observation_snapshot",
        lambda cloud_id: snapshots.get(str(cloud_id), ""),
    )

    client = cloud_sync.SporelyCloudClient("token", "user-123")
    cloud = _FakeObservationCloud(remote_rows)
    calls: dict[str, list] = {"patch": [], "post": [], "snapshots": snapshot_calls}
    monkeypatch.setattr(client, "_get", cloud.get)
    monkeypatch.setattr(
        client, "_patch", lambda path, payload: calls["patch"].append((path, dict(payload)))
    )

    def _fake_post(path, payload):
        calls["post"].append((path, dict(payload)))
        return [{"id": "duplicate-cloud-id"}]

    monkeypatch.setattr(client, "_post", _fake_post)
    monkeypatch.setattr(
        client, "pull_image_metadata", lambda cloud_id, include_deleted_for_sync=False: []
    )
    return client, calls


def _observation_row(db_path, local_id: int):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(
            "SELECT cloud_id, sync_status FROM observations WHERE id = ?",
            (local_id,),
        ).fetchone()
    finally:
        conn.close()


def test_pull_only_import_then_metadata_edit_patches_original_row(tmp_path, monkeypatch):
    """Regression 1: pull-only import (remote desktop_id NULL) later edited
    locally must PATCH cloud row 1100 — never POST a duplicate."""
    db_path = _init_db(tmp_path)
    _insert_pull_only_import(db_path, 762, "1100")
    remote = _remote_observation("1100", desktop_id=None)
    client, calls = _push_all_harness(monkeypatch, db_path, [remote])

    # Local metadata edit marks the row dirty.
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE observations SET notes = 'edited note', sync_status = 'dirty' WHERE id = 762")
    conn.commit()
    conn.close()

    result = cloud_sync.push_all(
        client,
        sync_images=False,
        sync_calibrations=False,
        remote_obs=[dict(remote)],
    )

    assert result["errors"] == []
    assert calls["post"] == [], "pull-only imported observation must never be re-POSTed"
    assert [path for path, _ in calls["patch"]] == ["observations?id=eq.1100"]
    assert "desktop_id" not in calls["patch"][0][1]
    assert _observation_row(db_path, 762) == ("1100", "synced")


def test_pull_only_import_then_media_dirty_reuses_original_row(tmp_path, monkeypatch):
    """Regression 2: dirty state from the existing media-dirty mechanism
    (mark_observation_media_dirty) must also reuse cloud row 1100."""
    db_path = _init_db(tmp_path)
    _insert_pull_only_import(db_path, 762, "1100")
    remote = _remote_observation("1100", desktop_id=None)
    client, calls = _push_all_harness(monkeypatch, db_path, [remote])

    cloud_sync.mark_observation_media_dirty(762)
    assert _observation_row(db_path, 762)[1] == "dirty"

    result = cloud_sync.push_all(
        client,
        sync_images=False,
        sync_calibrations=False,
        remote_obs=[dict(remote)],
    )

    assert result["errors"] == []
    assert calls["post"] == [], "media-dirty push must reuse the existing cloud row"
    assert _observation_row(db_path, 762) == ("1100", "synced")


def test_identity_conflict_in_push_all_stays_dirty_without_snapshot(tmp_path, monkeypatch):
    """Regression 9 (orchestration level): the direct link → 1100 is gone from
    the cloud while another row (1143) carries desktop_id 762. That row is
    never used as a substitute: no PATCH, no POST, no snapshot; the
    observation stays dirty for the cloud-deletion flow."""
    db_path = _init_db(tmp_path)
    _insert_pull_only_import(db_path, 762, "1100")
    remote_duplicate = _remote_observation("1143", desktop_id=762)
    client, calls = _push_all_harness(monkeypatch, db_path, [remote_duplicate])

    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE observations SET notes = 'edited note', sync_status = 'dirty' WHERE id = 762")
    conn.commit()
    conn.close()

    cloud_sync.push_all(
        client,
        sync_images=False,
        sync_calibrations=False,
        remote_obs=[dict(remote_duplicate)],
    )

    assert calls["patch"] == []
    assert calls["post"] == []
    assert calls["snapshots"] == [], "identity failure must not persist a snapshot"
    assert _observation_row(db_path, 762) == ("1100", "dirty")
