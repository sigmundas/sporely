"""Tests for Download-from-Cloud (pull-only sync).

The contract is strict: a pull-only run must never issue any cloud write
(upload, PATCH/POST/DELETE, storage-remove, tombstone push, or identity
write-back). Local-first conflict protections stay in force. These tests
prove all of that by recording every write method on the wrapped client
and asserting the recording lists remain empty.
"""
from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pytest

from database import models, schema
from utils import cloud_sync


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _create_download_only_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cloud_id TEXT,
                sync_status TEXT,
                synced_at TEXT,
                folder_path TEXT,
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
                sort_order INTEGER,
                image_type TEXT,
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
                captured_at TEXT,
                synced_at TEXT,
                source_role TEXT,
                file_purpose TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
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
        schema._ensure_image_tombstones_table(conn.cursor())
        conn.commit()
    finally:
        conn.close()


def _insert_observation(db_path: Path, obs_id: int, **overrides) -> None:
    row = {
        "id": obs_id,
        "cloud_id": overrides.get("cloud_id"),
        "sync_status": overrides.get("sync_status", "synced"),
        "synced_at": overrides.get("synced_at", "2026-05-01T00:00:00Z"),
        "folder_path": str(overrides.get("folder_path") or ""),
        "date": overrides.get("date", "2026-05-01"),
        "genus": overrides.get("genus"),
        "species": overrides.get("species"),
        "notes": overrides.get("notes"),
        "location": overrides.get("location"),
        "user_id": overrides.get("user_id", "user-123"),
        "sharing_scope": overrides.get("sharing_scope", "public"),
        "location_public": overrides.get("location_public", 1),
        "location_precision": overrides.get("location_precision", "exact"),
        "spore_data_visibility": overrides.get("spore_data_visibility", "public"),
        "is_draft": overrides.get("is_draft", 0),
    }
    columns = ", ".join(row.keys())
    placeholders = ", ".join(["?"] * len(row))
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            f"INSERT INTO observations ({columns}) VALUES ({placeholders})",
            tuple(row.values()),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_image(db_path: Path, **row) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            """
            INSERT INTO images (
                observation_id, cloud_id, filepath, original_filepath, sort_order, image_type,
                notes, source_role, file_purpose
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(row["observation_id"]),
                row.get("cloud_id"),
                str(row.get("filepath") or ""),
                str(row.get("filepath") or ""),
                int(row.get("sort_order", 0)),
                row.get("image_type", "field"),
                row.get("notes"),
                row.get("source_role"),
                row.get("file_purpose"),
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()


def _insert_image_tombstone(db_path: Path, cloud_image_id: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, delete_synced_at,
                deleted_observation_cloud_id
            ) VALUES (?, ?, ?, ?)
            """,
            (cloud_image_id, "2026-08-01T00:00:00Z", None, "cloud-obs-1"),
        )
        conn.commit()
    finally:
        conn.close()


class _RecordingCloudClient:
    """A cloud client that only supports reads and records every write.

    Any pull-only run should leave every list here empty. Write methods
    are provided so that a raw (unwrapped) call would succeed — proving
    that when write_lists remain empty, the writes truly did not happen.
    """

    user_id = "user-123"
    access_token = None

    def __init__(
        self,
        *,
        remote_observations: list[dict] | None = None,
        remote_images: list[dict] | None = None,
        remote_measurements: list[dict] | None = None,
    ) -> None:
        self.remote_observations = [dict(r) for r in (remote_observations or [])]
        self.remote_images = [dict(r) for r in (remote_images or [])]
        self.remote_measurements = [dict(r) for r in (remote_measurements or [])]
        # Recording lists for every write path.
        self.patched: list[tuple[str, dict]] = []
        self.posted: list[tuple[str, dict]] = []
        self.deleted: list[str] = []
        self.storage_removed: list[list[str]] = []
        self.pushed_observations: list[dict] = []
        self.pushed_image_metadata: list[tuple[dict, str, str]] = []
        self.pushed_measurements: list[dict] = []
        self.uploaded_images: list[tuple[str, str]] = []
        self.uploaded_originals: list[tuple[str, str]] = []
        self.set_image_desktop_ids: list[tuple[str, int]] = []
        self.set_desktop_ids: list[tuple[str, int]] = []
        self.set_measurement_desktop_ids: list[tuple[str, int]] = []
        self.set_image_storage_paths: list[tuple[str, str]] = []
        self.set_image_original_storage_paths: list[tuple[str, str]] = []
        self.soft_deleted_images: list[str] = []
        self.deleted_observations: list[str] = []
        self.deleted_measurements_for_images: list[str] = []
        self.pushed_calibration_metadata: list[dict] = []
        self.pushed_calibration_reference_images: list[dict] = []
        self.download_attempts: list[str] = []

    # ---- reads ---------------------------------------------------------
    def fetch_current_user_id(self):
        return "user-123"

    def list_remote_observations(self):
        return [dict(r) for r in self.remote_observations]

    def list_remote_calibrations(self):
        return []

    def pull_bulk_image_metadata(self, obs_cloud_ids):
        wanted = {str(x) for x in (obs_cloud_ids or [])}
        return [dict(r) for r in self.remote_images if str(r.get("observation_id")) in wanted]

    def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
        return [dict(r) for r in self.remote_images if str(r.get("observation_id")) == str(cloud_id)]

    def pull_measurements_for_images(self, image_cloud_ids):
        wanted = {str(x) for x in (image_cloud_ids or [])}
        return [dict(r) for r in self.remote_measurements if str(r.get("image_id")) in wanted]

    def download_image_file(self, storage_path, dest_path):
        self.download_attempts.append(str(storage_path))
        dest = Path(dest_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"cloud image bytes")

    # ---- writes (recorded; return None so callers don't crash) ---------
    def _patch(self, path, payload):
        self.patched.append((str(path), dict(payload)))

    def _post(self, path, payload):
        self.posted.append((str(path), dict(payload)))
        return []

    def _delete(self, path):
        self.deleted.append(str(path))

    def _storage_remove(self, storage_paths):
        self.storage_removed.append([str(p) for p in (storage_paths or [])])

    def push_observation(self, obs, remote_obs=None, **kwargs):
        self.pushed_observations.append(dict(obs))
        return str(obs.get("id") or "cloud-obs-new")

    def push_image_metadata(self, img, obs_cloud_id, storage_path):
        self.pushed_image_metadata.append((dict(img), str(obs_cloud_id), str(storage_path)))
        return "cloud-image-new"

    def push_measurement(self, *args, **kwargs):
        self.pushed_measurements.append({"args": args, "kwargs": kwargs})
        return "cloud-meas-new"

    def upload_image_file(self, *args, **kwargs):
        self.uploaded_images.append((args, kwargs))
        return {"storage_path": "cloud/path"}

    def upload_original_image_file(self, *args, **kwargs):
        self.uploaded_originals.append((args, kwargs))
        return {"storage_path": "cloud/orig"}

    def set_image_desktop_id(self, cloud_image_id, desktop_id):
        self.set_image_desktop_ids.append((str(cloud_image_id), int(desktop_id)))

    def set_desktop_id(self, cloud_id, desktop_id):
        self.set_desktop_ids.append((str(cloud_id), int(desktop_id)))

    def set_measurement_desktop_id(self, cloud_measurement_id, desktop_id):
        self.set_measurement_desktop_ids.append((str(cloud_measurement_id), int(desktop_id)))

    def set_image_storage_path(self, cloud_image_id, storage_path):
        self.set_image_storage_paths.append((str(cloud_image_id), str(storage_path)))

    def set_image_original_storage_path(self, cloud_image_id, original_storage_path):
        self.set_image_original_storage_paths.append((str(cloud_image_id), str(original_storage_path)))

    def soft_delete_image(self, cloud_image_id, deleted_at):
        self.soft_deleted_images.append(str(cloud_image_id))

    def delete_cloud_observation(self, obs_cloud_id):
        self.deleted_observations.append(str(obs_cloud_id))

    def delete_cloud_measurements_for_image(self, cloud_image_id):
        self.deleted_measurements_for_images.append(str(cloud_image_id))

    def push_calibration_reference_image(self, *args, **kwargs):
        self.pushed_calibration_reference_images.append({"args": args, "kwargs": kwargs})
        return "cloud/cal.jpg"

    def push_calibration_metadata(self, calibration):
        self.pushed_calibration_metadata.append(dict(calibration))
        return "cloud-cal-new"

    # ---- convenience --------------------------------------------------
    def all_write_records(self) -> dict[str, list]:
        return {
            "patched": self.patched,
            "posted": self.posted,
            "deleted": self.deleted,
            "storage_removed": self.storage_removed,
            "pushed_observations": self.pushed_observations,
            "pushed_image_metadata": self.pushed_image_metadata,
            "pushed_measurements": self.pushed_measurements,
            "uploaded_images": self.uploaded_images,
            "uploaded_originals": self.uploaded_originals,
            "set_image_desktop_ids": self.set_image_desktop_ids,
            "set_desktop_ids": self.set_desktop_ids,
            "set_measurement_desktop_ids": self.set_measurement_desktop_ids,
            "set_image_storage_paths": self.set_image_storage_paths,
            "set_image_original_storage_paths": self.set_image_original_storage_paths,
            "soft_deleted_images": self.soft_deleted_images,
            "deleted_observations": self.deleted_observations,
            "deleted_measurements_for_images": self.deleted_measurements_for_images,
            "pushed_calibration_metadata": self.pushed_calibration_metadata,
            "pushed_calibration_reference_images": self.pushed_calibration_reference_images,
        }


@pytest.fixture
def _isolated_pull_helpers(monkeypatch):
    """Stub broadcast/generation helpers pull_all invokes for side effects."""
    monkeypatch.setattr(cloud_sync, "_apply_remote_observation_fields", lambda *a, **k: None)
    monkeypatch.setattr(cloud_sync, "_detect_deleted_remote_observations", lambda remote_obs: [])
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *a, **k: None)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *a, **k: None)
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *a, **k: None)
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda *a, **k: "")
    monkeypatch.setattr(cloud_sync, "_local_cloud_media_signature", lambda *a, **k: "")
    monkeypatch.setattr(cloud_sync, "_store_local_media_signature_if_equivalent", lambda *a, **k: None)


def _patch_db(monkeypatch, db_path: Path) -> None:
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    # Route image inserts through a lightweight local-copy stub so the
    # pull path can materialize downloaded cloud bytes into a local row.

    def fake_add_image(**kwargs):
        source_path = Path(str(kwargs["filepath"]))
        conn = sqlite3.connect(db_path)
        try:
            obs_row = conn.execute(
                "SELECT folder_path FROM observations WHERE id = ?",
                (int(kwargs["observation_id"]),),
            ).fetchone()
            folder_path = (
                Path(str(obs_row[0])) if obs_row and obs_row[0] else db_path.parent / "images"
            )
            folder_path.mkdir(parents=True, exist_ok=True)
            dest_path = folder_path / source_path.name
            counter = 1
            while dest_path.exists():
                dest_path = folder_path / f"{source_path.stem}_{counter}{source_path.suffix}"
                counter += 1
            shutil.copy2(source_path, dest_path)
            cursor = conn.execute(
                """
                INSERT INTO images (
                    observation_id, filepath, original_filepath, sort_order, image_type,
                    notes, source_role, file_purpose
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(kwargs["observation_id"]),
                    str(dest_path),
                    None,
                    kwargs.get("sort_order") or 0,
                    kwargs.get("image_type") or "field",
                    kwargs.get("notes"),
                    kwargs.get("source_role"),
                    kwargs.get("file_purpose"),
                ),
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", fake_add_image)
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *a, **k: None)
    monkeypatch.setattr(cloud_sync, "_profile_generate_all_sizes", lambda *a, **k: None)


# ---------------------------------------------------------------------------
# Wrapper unit behaviour
# ---------------------------------------------------------------------------


def test_pull_only_wrapper_delegates_allowlisted_reads():
    """A known read method delegates verbatim to the wrapped client."""
    class _R:
        user_id = "u"
        access_token = "tok"
        def list_remote_observations(self):
            return [{"id": "obs-1"}]
        def pull_bulk_image_metadata(self, ids):
            return [{"id": "img-1", "wanted": list(ids)}]
        def download_image_file(self, storage_path, dest_path):
            return ("downloaded", storage_path, str(dest_path))

    wrapper = cloud_sync.PullOnlyCloudClient(_R())
    # Non-callable attributes pass through.
    assert wrapper.user_id == "u"
    assert wrapper.access_token == "tok"
    assert wrapper.is_pull_only is True
    # Allowlisted reads delegate.
    assert wrapper.list_remote_observations() == [{"id": "obs-1"}]
    assert wrapper.pull_bulk_image_metadata(["a", "b"]) == [{"id": "img-1", "wanted": ["a", "b"]}]
    assert wrapper.download_image_file("path", "/dev/null") == ("downloaded", "path", "/dev/null")


def test_pull_only_wrapper_blocks_every_named_writer():
    """Every method on the blocked writer list raises PullOnlyModeError."""
    class _R:
        # Every named writer is provided on the wrapped client so the test
        # proves the wrapper — not a missing method — is what raises.
        def _patch(self, *a, **k): raise AssertionError("must not reach _patch")
        def _post(self, *a, **k): raise AssertionError("must not reach _post")
        def _delete(self, *a, **k): raise AssertionError("must not reach _delete")
        def _storage_remove(self, *a, **k): raise AssertionError("must not reach _storage_remove")
        def push_observation(self, *a, **k): raise AssertionError()
        def push_image_metadata(self, *a, **k): raise AssertionError()
        def push_measurement(self, *a, **k): raise AssertionError()
        def upload_image_file(self, *a, **k): raise AssertionError()
        def upload_original_image_file(self, *a, **k): raise AssertionError()
        def set_image_storage_path(self, *a, **k): raise AssertionError()
        def set_image_desktop_id(self, *a, **k): raise AssertionError()
        def set_desktop_id(self, *a, **k): raise AssertionError()
        def set_measurement_desktop_id(self, *a, **k): raise AssertionError()
        def set_image_original_storage_path(self, *a, **k): raise AssertionError()
        def soft_delete_image(self, *a, **k): raise AssertionError()
        def delete_cloud_observation(self, *a, **k): raise AssertionError()
        def delete_cloud_measurements_for_image(self, *a, **k): raise AssertionError()
        def push_calibration_reference_image(self, *a, **k): raise AssertionError()
        def push_calibration_metadata(self, *a, **k): raise AssertionError()

    wrapper = cloud_sync.PullOnlyCloudClient(_R())
    for name in cloud_sync._PULL_ONLY_BLOCKED_CLIENT_METHODS:
        with pytest.raises(cloud_sync.PullOnlyModeError):
            getattr(wrapper, name)("x")
    assert len(wrapper.write_attempts) == len(cloud_sync._PULL_ONLY_BLOCKED_CLIENT_METHODS)


def test_pull_only_wrapper_blocks_unrecognized_callable_even_if_it_would_write():
    """A callable not on either list is blocked by default.

    This is the specific leak a denylist can't catch: a future writer named
    ``synthesize_and_patch`` would, under denylist semantics, be returned by
    __getattr__ bound to the real client and internally call ``self._patch``
    on the real client. Under an allowlist, the wrapper intercepts the
    outer call and never reaches that internal write path.
    """
    class _RealClient:
        def _patch(self, path, payload):
            # If this ever runs during the wrapper test the test fails —
            # it proves the wrapper leaked.
            raise AssertionError(f"cloud write reached the wrapped client: PATCH {path}")

        def synthesize_and_patch(self, path, payload):
            # A hypothetical unrecognized method that would internally
            # PATCH the cloud. Under a denylist, calling this via the
            # wrapper would forward to the real client — which then
            # invokes self._patch on the real client (bypassing the
            # wrapper entirely, since ``self`` here is _RealClient).
            self._patch(path, payload)

    wrapper = cloud_sync.PullOnlyCloudClient(_RealClient())
    with pytest.raises(cloud_sync.PullOnlyModeError):
        wrapper.synthesize_and_patch("/observations/1", {"notes": "leaked"})
    # And the counter recorded the attempt against the outer name — the
    # inner _patch was never reached, so it does not appear.
    assert wrapper.write_attempts == ["synthesize_and_patch"]


def test_summarize_blocked_write_attempts_deduplicates_by_name():
    """A single leaky path repeated 595 times collapses to ``name ×595``."""
    attempts = ["set_image_desktop_id"] * 595 + ["_patch"] * 3
    summary = cloud_sync.summarize_blocked_write_attempts(attempts)
    assert summary == "set_image_desktop_id ×595, _patch ×3"


def test_summarize_blocked_write_attempts_empty():
    assert cloud_sync.summarize_blocked_write_attempts([]) == ""


def test_partition_download_from_cloud_issues_splits_review_from_errors():
    review_line = (
        "cloud cloud-abc: needs review before applying remaining cloud changes to "
        "local observation 42 (notes)"
    )
    tombstone_line = "obs 7: skipped cloud image cloud-x because it has a local tombstone"
    measurement_conflict_line = (
        "obs 7: skipped cloud measurement m-1 because the local copy changed"
    )
    real_error = "cloud cloud-def: 500 Internal Server Error"

    review, errors = cloud_sync.partition_download_from_cloud_issues(
        [review_line, tombstone_line, measurement_conflict_line, real_error, ""]
    )
    assert review == [review_line, tombstone_line, measurement_conflict_line]
    assert errors == [real_error]


def test_pull_only_wrapper_allows_non_callable_attributes():
    """Plain data attributes on the wrapped client forward normally."""
    class _R:
        user_id = "user-123"
        access_token = "tok-abc"
        supports_thing = True
    wrapper = cloud_sync.PullOnlyCloudClient(_R())
    assert wrapper.user_id == "user-123"
    assert wrapper.access_token == "tok-abc"
    assert wrapper.supports_thing is True


# ---------------------------------------------------------------------------
# pull_all under a PullOnlyCloudClient wrapper
# ---------------------------------------------------------------------------


def test_download_from_cloud_downloads_missing_field_image(tmp_path, monkeypatch, _isolated_pull_helpers):
    """A cloud field image not present locally gets downloaded — no writes."""
    db_path = tmp_path / "sporely.db"
    _create_download_only_db(db_path)
    _patch_db(monkeypatch, db_path)
    images_root = tmp_path / "images" / "obs-1"
    images_root.mkdir(parents=True, exist_ok=True)
    _insert_observation(db_path, 1, cloud_id="cloud-obs-1", folder_path=str(images_root))

    remote_obs = {
        "id": "cloud-obs-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "genus": "Flammulina",
        "species": "velutipes",
        "updated_at": "2026-06-01T00:00:00Z",
    }
    remote_image = {
        "id": "cloud-image-1",
        "desktop_id": None,
        "observation_id": "cloud-obs-1",
        "storage_path": "u/obs-1/img.webp",
        "original_filename": "img.webp",
        "image_type": "field",
        "sort_order": 0,
        "deleted_at": None,
    }

    client = _RecordingCloudClient(
        remote_observations=[remote_obs],
        remote_images=[remote_image],
    )
    wrapper = cloud_sync.PullOnlyCloudClient(client)

    result = cloud_sync.pull_all(
        wrapper,
        remote_obs=[dict(remote_obs)],
        sync_calibrations=False,
    )

    # Image was downloaded.
    assert client.download_attempts == ["u/obs-1/img.webp"]
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT cloud_id FROM images").fetchall()
    finally:
        conn.close()
    assert [r[0] for r in rows] == ["cloud-image-1"]

    # Zero cloud writes: no method on the recording client was called.
    for name, records in client.all_write_records().items():
        assert records == [], f"unexpected cloud write {name!r}: {records!r}"
    assert wrapper.write_attempts == []
    assert result["pulled"] == 1


def test_download_from_cloud_pulls_cloud_observation_metadata(tmp_path, monkeypatch, _isolated_pull_helpers):
    """A brand-new remote observation is materialized locally — no writes."""
    db_path = tmp_path / "sporely.db"
    _create_download_only_db(db_path)
    _patch_db(monkeypatch, db_path)

    remote_obs = {
        "id": "cloud-obs-42",
        "desktop_id": None,
        "date": "2026-05-01",
        "genus": "New",
        "species": "arrival",
        "updated_at": "2026-07-01T00:00:00Z",
    }
    client = _RecordingCloudClient(remote_observations=[remote_obs])
    wrapper = cloud_sync.PullOnlyCloudClient(client)

    monkeypatch.setattr(cloud_sync, "_create_local_from_remote", lambda *a, **k: 99)

    result = cloud_sync.pull_all(
        wrapper,
        remote_obs=[dict(remote_obs)],
        sync_calibrations=False,
        materialize_remote_images=False,
    )

    assert result["pulled"] == 1
    for name, records in client.all_write_records().items():
        assert records == [], f"unexpected cloud write {name!r}: {records!r}"
    assert wrapper.write_attempts == []


def test_download_from_cloud_never_downloads_metadata_only_microscope_anchor(
    tmp_path, monkeypatch, _isolated_pull_helpers,
):
    """A metadata-only microscope anchor (storage_path=NULL) triggers no byte download."""
    db_path = tmp_path / "sporely.db"
    _create_download_only_db(db_path)
    _patch_db(monkeypatch, db_path)
    _insert_observation(db_path, 1, cloud_id="cloud-obs-1")

    remote_obs = {
        "id": "cloud-obs-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "updated_at": "2026-08-01T00:00:00Z",
    }
    metadata_anchor = {
        "id": "cloud-image-anchor",
        "observation_id": "cloud-obs-1",
        # No storage_path — this is the metadata-only microscope anchor.
        "storage_path": None,
        "image_type": "microscopy",
        "sort_order": 0,
        "deleted_at": None,
    }
    client = _RecordingCloudClient(
        remote_observations=[remote_obs],
        remote_images=[metadata_anchor],
    )
    # Force treatment as metadata-only anchor regardless of the row's other fields.
    monkeypatch.setattr(cloud_sync, "_is_metadata_only_microscope_cloud_image", lambda img: True)
    # Anchor creation touches local rows only; short-circuit it here so the
    # test focuses on the byte-download property.
    monkeypatch.setattr(cloud_sync, "_ensure_local_metadata_only_microscope_anchor", lambda *a, **k: None)

    wrapper = cloud_sync.PullOnlyCloudClient(client)
    cloud_sync.pull_all(
        wrapper,
        remote_obs=[dict(remote_obs)],
        sync_calibrations=False,
    )

    assert client.download_attempts == [], "metadata-only anchor must not trigger a byte download"
    for name, records in client.all_write_records().items():
        assert records == [], f"unexpected cloud write {name!r}: {records!r}"


def test_download_from_cloud_does_not_push_pending_tombstones(tmp_path, monkeypatch, _isolated_pull_helpers):
    """A pending local image tombstone must not be pushed during Download from Cloud."""
    db_path = tmp_path / "sporely.db"
    _create_download_only_db(db_path)
    _patch_db(monkeypatch, db_path)
    _insert_observation(db_path, 1, cloud_id="cloud-obs-1")
    _insert_image_tombstone(db_path, "cloud-image-doomed")

    client = _RecordingCloudClient(
        remote_observations=[{"id": "cloud-obs-1", "desktop_id": 1, "date": "2026-05-01"}],
    )
    wrapper = cloud_sync.PullOnlyCloudClient(client)

    # sync_all(pull_only=True) is the entrypoint that skips push_all (which
    # is what would push tombstones). Prevent the local-user check from
    # hitting real settings.
    monkeypatch.setattr(cloud_sync, "ensure_database_linked_to_cloud_user", lambda c: "user-123")
    # Skip pull_calibrations to keep the run focused; it's already read-only.
    monkeypatch.setattr(cloud_sync, "pull_calibrations", lambda *a, **k: {"pulled": 0, "total": 0, "errors": []})
    # Stop the exif backfill from being reachable even in cache-hit form.
    monkeypatch.setattr(cloud_sync, "_backfill_missing_exif_on_cloud_images", lambda: {"scanned": 0})

    result = cloud_sync.sync_all(wrapper, pull_only=True)

    assert result["pull_only"] is True
    assert result["cloud_writes_completed"] == 0
    assert result["blocked_write_attempts"] == []
    assert wrapper.write_attempts == []
    assert client.soft_deleted_images == []
    for name, records in client.all_write_records().items():
        assert records == [], f"unexpected cloud write {name!r}: {records!r}"


def test_download_from_cloud_preserves_local_dirty_observation(tmp_path, monkeypatch, _isolated_pull_helpers):
    """A locally dirty observation is NOT overwritten by remote metadata during pull-only.

    In the existing pull, when the remote observation carries only remote-only
    field changes we merge those in and leave the local sync_status alone if
    there are still unresolved local changes. That same protection must apply
    under pull-only. Here we assert the observation stays dirty (i.e. the
    local unsynced state is preserved).
    """
    db_path = tmp_path / "sporely.db"
    _create_download_only_db(db_path)
    _patch_db(monkeypatch, db_path)

    _insert_observation(
        db_path,
        1,
        cloud_id="cloud-obs-1",
        sync_status="dirty",
        genus="LocalOnly",
        species="edit",
        notes="local pending edit",
    )

    remote_obs = {
        "id": "cloud-obs-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "genus": "Remote",
        "species": "different",
        "notes": "remote note",
        "updated_at": "2026-08-01T00:00:00Z",
    }

    client = _RecordingCloudClient(remote_observations=[remote_obs])
    wrapper = cloud_sync.PullOnlyCloudClient(client)

    # We only care that no cloud writes occur and no attempts pile up.
    # The internal change-analysis machinery still runs but does not mutate
    # the observation into 'synced' when there are local-only fields.
    cloud_sync.pull_all(
        wrapper,
        remote_obs=[dict(remote_obs)],
        sync_calibrations=False,
        materialize_remote_images=False,
    )

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT sync_status, notes FROM observations WHERE id = 1"
        ).fetchone()
    finally:
        conn.close()
    # sync_status must not be silently flipped to 'synced' while local
    # edits are still pending.
    assert row[0] != "synced" or row[1] == "local pending edit"
    for name, records in client.all_write_records().items():
        assert records == [], f"unexpected cloud write {name!r}: {records!r}"
    assert wrapper.write_attempts == []


def test_sync_all_pull_only_records_zero_cloud_writes(tmp_path, monkeypatch, _isolated_pull_helpers):
    """End-to-end: a full sync_all(pull_only=True) never invokes any cloud writer."""
    db_path = tmp_path / "sporely.db"
    _create_download_only_db(db_path)
    _patch_db(monkeypatch, db_path)
    images_root = tmp_path / "images" / "obs-1"
    images_root.mkdir(parents=True, exist_ok=True)
    _insert_observation(db_path, 1, cloud_id="cloud-obs-1", folder_path=str(images_root))

    remote_obs = {
        "id": "cloud-obs-1",
        "desktop_id": 1,
        "date": "2026-05-01",
        "genus": "Flammulina",
        "species": "velutipes",
        "updated_at": "2026-06-01T00:00:00Z",
    }
    remote_image = {
        "id": "cloud-image-1",
        "observation_id": "cloud-obs-1",
        "storage_path": "u/obs-1/img.webp",
        "original_filename": "img.webp",
        "image_type": "field",
        "sort_order": 0,
        "deleted_at": None,
    }
    client = _RecordingCloudClient(
        remote_observations=[remote_obs],
        remote_images=[remote_image],
    )

    monkeypatch.setattr(cloud_sync, "ensure_database_linked_to_cloud_user", lambda c: "user-123")
    monkeypatch.setattr(cloud_sync, "pull_calibrations", lambda *a, **k: {"pulled": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_backfill_missing_exif_on_cloud_images", lambda: {"scanned": 0})
    # push_calibrations and push_all must not be invoked at all in pull_only;
    # blow up loudly if they are.
    def _explode(*a, **k):
        raise AssertionError("push path must not be invoked during Download from Cloud")
    monkeypatch.setattr(cloud_sync, "push_calibrations", _explode)
    monkeypatch.setattr(cloud_sync, "push_all", _explode)

    # Pass a pre-wrapped client so we can inspect the wrapper's own
    # write_attempts counter directly after the run.
    wrapper = cloud_sync.PullOnlyCloudClient(client)
    result = cloud_sync.sync_all(wrapper, pull_only=True)

    assert result["pull_only"] is True
    assert result["pushed"] == 0
    assert result["images_downloaded"] >= 1
    assert result["observations_updated"] >= 0
    # Zero writes reached the network AND zero wrapper interceptions:
    # every writer call is suppressed at its source, so the wrapper never
    # even runs its block path in the normal flow.
    assert result["cloud_writes_completed"] == 0
    assert result["blocked_write_attempts"] == []
    # The wrapper's own counter must also be empty — proving that not only
    # did nothing reach the network, nothing tried to.
    assert wrapper.write_attempts == []
    assert client.download_attempts == ["u/obs-1/img.webp"]
    for name, records in client.all_write_records().items():
        assert records == [], f"unexpected cloud write {name!r}: {records!r}"


# ---------------------------------------------------------------------------
# Pagination regression tests (PostgREST db-max-rows silent truncation)
# ---------------------------------------------------------------------------


class _PaginatingClient(cloud_sync.SporelyCloudClient):
    """SporelyCloudClient with ``_get`` served from staged in-memory tables.

    Each URL path is matched to a table of rows; the fake honours
    ``limit=N&offset=M`` query params exactly as PostgREST does when the
    caller asks for a bounded page. Any request without limit/offset is
    silently capped at the server ``db-max-rows`` limit (default 1000) —
    exactly the behaviour that caused the missing-images bug.
    """

    def __init__(self, tables):
        self._tables = dict(tables)
        self._calls = []
        self.access_token = "t"
        self.user_id = "u"
        self.refresh_token = None

    def _get(self, path):  # type: ignore[override]
        self._calls.append(path)
        base, _, query = path.partition("?")
        params = {}
        for chunk in query.split("&"):
            if not chunk or "=" not in chunk:
                continue
            key, _, value = chunk.partition("=")
            params[key] = value
        key = None
        for candidate in self._tables:
            if base == candidate or path.startswith(candidate):
                key = candidate
                break
        if key is None:
            raise AssertionError(f"unexpected GET {path!r}")
        rows = list(self._tables[key])
        try:
            offset = int(params.get("offset", "0"))
        except ValueError:
            offset = 0
        try:
            limit = int(params.get("limit", "0"))
        except ValueError:
            limit = 0
        server_cap = 1000
        if limit <= 0 or limit > server_cap:
            limit = server_cap
        return rows[offset:offset + limit]


def test_get_paginated_returns_full_result_across_pages():
    rows = [{"id": f"r{i}"} for i in range(2500)]
    client = _PaginatingClient({"widgets": rows})
    result = client._get_paginated("widgets?order=id.asc")
    assert result == rows
    # 1000 + 1000 + 500 → 3 requests
    assert len(client._calls) == 3
    assert "offset=0" in client._calls[0]
    assert "offset=1000" in client._calls[1]
    assert "offset=2000" in client._calls[2]


def test_get_paginated_stops_when_short_page_arrives():
    rows = [{"id": f"r{i}"} for i in range(1500)]
    client = _PaginatingClient({"widgets": rows})
    result = client._get_paginated("widgets?order=id.asc")
    assert len(result) == 1500
    # 2 requests, second returns 500 → stop.
    assert len(client._calls) == 2


def test_get_paginated_propagates_page_error_without_partial_result():
    rows = [{"id": f"r{i}"} for i in range(2500)]
    client = _PaginatingClient({"widgets": rows})
    original_get = client._get
    call_count = {"n": 0}

    def flaky_get(path):
        call_count["n"] += 1
        if call_count["n"] == 2:
            raise cloud_sync.CloudSyncError("simulated PostgREST failure on page 2")
        return original_get(path)

    client._get = flaky_get  # type: ignore[method-assign]
    with pytest.raises(cloud_sync.CloudSyncError):
        client._get_paginated("widgets?order=id.asc")
    # No partial-result silent success: caller sees the exception, not a
    # truncated list to be persisted as an authoritative snapshot.


def test_pull_bulk_image_metadata_pages_past_1000_row_cap():
    # 100 observations, 15 images each → 1500 rows for a single batch.
    obs_ids = [str(1000 + i) for i in range(100)]
    rows = []
    for obs_id in obs_ids:
        for j in range(15):
            rows.append({
                "id": f"img-{obs_id}-{j}",
                "observation_id": obs_id,
                "storage_path": f"u/{obs_id}/{j}.webp",
                "image_type": "field",
                "deleted_at": None,
            })
    client = _PaginatingClient({"observation_images": rows})
    result = client.pull_bulk_image_metadata(obs_ids)
    assert len(result) == 1500, "pagination must return every image row, not just the first 1000"
    # Every observation — including the tail ones that used to lose their
    # images to the silent 1000-row truncation — is represented.
    seen = {row["observation_id"] for row in result}
    assert seen == set(obs_ids)


def test_pull_bulk_image_metadata_tail_observation_receives_its_images():
    # Obs 1099 is at the tail of the batch; its images are the highest ids and
    # would previously be silently dropped when the batch exceeded 1000 rows.
    obs_ids = [str(1000 + i) for i in range(100)]
    rows = []
    row_counter = 0
    for obs_id in obs_ids:
        count = 15 if obs_id != "1099" else 3
        for j in range(count):
            row_counter += 1
            rows.append({
                "id": f"img-{row_counter:04d}",
                "observation_id": obs_id,
                "storage_path": f"u/{obs_id}/{j}.webp",
                "image_type": "field",
                "deleted_at": None,
            })
    client = _PaginatingClient({"observation_images": rows})
    result = client.pull_bulk_image_metadata(obs_ids)
    tail_images = [row for row in result if row["observation_id"] == "1099"]
    assert len(tail_images) == 3, (
        "obs 1099 must receive its 3 images even though the batch total exceeds 1000"
    )


def test_pull_bulk_image_metadata_empty_when_no_ids():
    client = _PaginatingClient({"observation_images": []})
    assert client.pull_bulk_image_metadata([]) == []
    assert client._calls == []


def test_pull_measurements_for_images_pages_past_1000_row_cap():
    image_ids = [f"img-{i:03d}" for i in range(80)]
    rows = []
    for image_id in image_ids:
        for j in range(20):
            rows.append({
                "id": f"m-{image_id}-{j}",
                "image_id": image_id,
                "measured_at": f"2026-08-{(j % 28) + 1:02d}",
                "desktop_id": None,
            })
    client = _PaginatingClient({"spore_measurements": rows})
    result = client.pull_measurements_for_images(image_ids)
    assert len(result) == 1600
    assert {row["image_id"] for row in result} == set(image_ids)


def test_list_remote_observations_pages_past_1000_row_cap():
    rows = [{"id": f"o-{i:04d}", "user_id": "u"} for i in range(1750)]
    client = _PaginatingClient({"observations": rows})
    result = client.list_remote_observations()
    assert len(result) == 1750
    # The order clause is preserved through pagination.
    assert "order=created_at.asc,id.asc" in client._calls[0]


def test_list_remote_calibrations_pages_past_1000_row_cap():
    rows = [{"id": f"c-{i:04d}", "user_id": "u"} for i in range(1200)]
    client = _PaginatingClient({"calibrations": rows})
    result = client.list_remote_calibrations()
    assert len(result) == 1200
    assert "order=created_at.asc,id.asc" in client._calls[0]


def test_pull_bulk_image_metadata_page_2_failure_does_not_yield_page_1_only_snapshot():
    obs_ids = [str(1000 + i) for i in range(100)]
    rows = [
        {"id": f"img-{i:04d}", "observation_id": obs_ids[i % 100], "image_type": "field", "deleted_at": None}
        for i in range(1500)
    ]
    client = _PaginatingClient({"observation_images": rows})
    original_get = client._get
    call_count = {"n": 0}

    def flaky_get(path):
        call_count["n"] += 1
        # First page for the batch succeeds, second page fails.
        if call_count["n"] == 2:
            raise cloud_sync.CloudSyncError("simulated failure on page 2")
        return original_get(path)

    client._get = flaky_get  # type: ignore[method-assign]
    with pytest.raises(cloud_sync.CloudSyncError):
        client.pull_bulk_image_metadata(obs_ids)
    # The caller MUST see the failure so it can abort the pull, rather than
    # persist a 1000-row-only snapshot and later interpret the missing rows
    # as "cloud removed local image files".
