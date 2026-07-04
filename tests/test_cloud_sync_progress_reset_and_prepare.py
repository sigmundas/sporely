"""Regression tests for cloud sync polish fixes.

Issue 1 — progress-bar reset:
  The status bar used to remember the previous run's finish value and clamp
  the new run's first event up to it, so a second sync in the same session
  visibly hung at ~99%.

Issue 2 — per-image prepare gating:
  When only one image in an observation needs a byte upload (e.g. the sibling
  images are already linked and unchanged), WebP encoding must run only for
  the image that actually needs it. Already-linked sibling images should be
  patched via metadata-only requests without ever being encoded.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from database import models
from utils import cloud_sync


# --- helpers ------------------------------------------------------------------


def _init_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "sporely.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cloud_id TEXT,
            sync_status TEXT,
            synced_at TEXT,
            date TEXT,
            user_id TEXT,
            sync_error_code TEXT,
            sync_error_message TEXT,
            sync_blocked_reason TEXT,
            sync_blocked_at TEXT,
            folder_path TEXT,
            artsdata_id INTEGER,
            publish_target TEXT
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
            calibration_id INTEGER,
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
            gallery_rotation INTEGER
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


def _patch_connections(monkeypatch, db_path: Path) -> None:
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))


# --- Issue 1: progress bar resets on the second sync -------------------------


def test_progress_bar_resets_between_sync_runs():
    """Simulate the UI progress handler across two sync runs.

    The first run reaches 100%. When the second run's initial "auth" event
    (~0-5%) arrives, the bar must drop back down instead of clamping to the
    previous run's finish value.
    """

    class _FakeProgressBar:
        """Minimal stand-in for QProgressBar covering the calls we make."""

        def __init__(self) -> None:
            self._value = 0
            self._min = 0
            self._max = 100

        def setRange(self, low: int, high: int) -> None:
            self._min = int(low)
            self._max = int(high)

        def setValue(self, value: int) -> None:
            self._value = int(value)

        def value(self) -> int:
            return int(self._value)

    class _FakeLabel:
        def __init__(self) -> None:
            self._text = ""

        def setText(self, text: str) -> None:
            self._text = str(text)

        def text(self) -> str:
            return self._text

    # Mirror the real _set_status_progress method after the fix — this is the
    # code path that used to clamp new pct to the previous value.
    from ui import observations_tab as observations_tab_mod

    tab = observations_tab_mod.ObservationsTab.__new__(observations_tab_mod.ObservationsTab)
    tab.status_progress_bar = _FakeProgressBar()
    tab.status_progress_text = _FakeLabel()
    tab.status_progress_pct = _FakeLabel()

    # First run: bar climbs to 100%.
    observations_tab_mod.ObservationsTab._set_status_progress(tab, "Finalizing cloud sync…", 100, 100)
    assert tab.status_progress_bar.value() == 100

    # Second run starts. The pre-worker reset drops it to 0…
    observations_tab_mod.ObservationsTab._reset_status_progress(tab)
    assert tab.status_progress_bar.value() == 0

    # …and the auth phase's initial "Connecting…" event maps to a small
    # percentage. It must NOT be clamped back up to 100.
    observations_tab_mod.ObservationsTab._set_status_progress(
        tab,
        "Connecting to Sporely Cloud...",
        0,
        100,
    )
    assert tab.status_progress_bar.value() == 0

    # A slightly-later event within the auth phase (say 3%) also stays low.
    observations_tab_mod.ObservationsTab._set_status_progress(
        tab,
        "Loading cloud observations…",
        3,
        100,
    )
    assert tab.status_progress_bar.value() == 3


def test_progress_bar_never_clamps_to_previous_value_within_run():
    """The clamp is fully gone: a smaller-percent event just moves the bar.

    Real sync work never emits a smaller percent within a single run because
    cloud_sync maps everything through the monotonic phase model — but the
    UI should not defensively pin the bar to the previous value either, so
    the second-sync-at-99% bug can't come back.
    """
    from ui import observations_tab as observations_tab_mod

    class _FakeProgressBar:
        def __init__(self) -> None:
            self._value = 0
            self._min, self._max = 0, 100

        def setRange(self, low: int, high: int) -> None:
            self._min, self._max = int(low), int(high)

        def setValue(self, value: int) -> None:
            self._value = int(value)

        def value(self) -> int:
            return int(self._value)

    class _FakeLabel:
        def setText(self, _text: str) -> None:
            pass

    tab = observations_tab_mod.ObservationsTab.__new__(observations_tab_mod.ObservationsTab)
    tab.status_progress_bar = _FakeProgressBar()
    tab.status_progress_text = _FakeLabel()
    tab.status_progress_pct = _FakeLabel()

    observations_tab_mod.ObservationsTab._set_status_progress(tab, "high", 90, 100)
    assert tab.status_progress_bar.value() == 90
    observations_tab_mod.ObservationsTab._set_status_progress(tab, "low", 10, 100)
    assert tab.status_progress_bar.value() == 10


# --- Issue 2: only the image that needs bytes is prepared ---------------------


def test_reconcile_metadata_only_linked_images_skips_unchanged_siblings(monkeypatch, tmp_path):
    """3 images: 2 already linked & unchanged, 1 cloud_id NULL.

    The metadata-only prepass should:
    - patch metadata for the 2 unchanged linked images without preparing WebPs
    - leave the cloud_id-NULL image in the prepare/upload path
    - return the linked images' cloud_ids as kept so they are not deleted
    """
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)

    linked_1_path = tmp_path / "img_1825.jpg"
    linked_1_path.write_bytes(b"linked-1-bytes")
    linked_2_path = tmp_path / "img_1826.jpg"
    linked_2_path.write_bytes(b"linked-2-bytes")
    new_path = tmp_path / "img_1898.jpg"
    new_path.write_bytes(b"new-image-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at, date, user_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (481, "cloud-obs-481", "dirty", "2026-05-01T00:00:00Z", "2026-05-01", "user-1"),
        )
        for image_id, cloud_id, filepath, notes in (
            (1825, "cloud-image-1825", str(linked_1_path), "old note"),
            (1826, "cloud-image-1826", str(linked_2_path), "matching note"),
            (1898, None, str(new_path), None),
        ):
            conn.execute(
                "INSERT INTO images (id, observation_id, cloud_id, filepath, image_type, "
                "sort_order, notes, synced_at, source_role, file_purpose) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    image_id,
                    481,
                    cloud_id,
                    filepath,
                    "field",
                    image_id,
                    notes,
                    "2026-05-01T00:00:00Z" if cloud_id else None,
                    "local_canonical",
                    "field",
                ),
            )
        conn.commit()
    finally:
        conn.close()

    # Stamp the local media signature so the prepass can tell the linked
    # images' source bytes are unchanged.
    cloud_sync._refresh_local_cloud_media_signature(481)

    class _StubClient:
        user_id = "user-1"

        def __init__(self) -> None:
            self.push_image_metadata_calls: list[dict] = []

        def _observation_images_support_ai_crop(self) -> bool:
            return True

        def _observation_images_support_ai_crop_custom(self) -> bool:
            return True

        def _observation_images_support_upload_metadata(self) -> bool:
            return False

        def _observation_images_support_original_storage_path(self) -> bool:
            return False

        def push_image_metadata(self, img, obs_cloud_id, storage_path):
            record = dict(img)
            record["_obs_cloud_id"] = obs_cloud_id
            record["_storage_path"] = storage_path
            self.push_image_metadata_calls.append(record)
            return str(img.get("cloud_id") or "cloud-image-new")

    existing_rows = [
        {
            "id": "cloud-image-1825",
            "desktop_id": 1825,
            "sort_order": 1825,
            "image_type": "field",
            "storage_path": "user-1/cloud-obs-481/cloud-image-1825.webp",
            "original_filename": "img_1825.jpg",
            "notes": "cloud-updated note",  # metadata drifted
        },
        {
            "id": "cloud-image-1826",
            "desktop_id": 1826,
            "sort_order": 1826,
            "image_type": "field",
            "storage_path": "user-1/cloud-obs-481/cloud-image-1826.webp",
            "original_filename": "img_1826.jpg",
            "notes": "matching note",  # already in sync
        },
    ]

    client = _StubClient()
    obs = {"id": 481}
    skip_ids, kept_ids = cloud_sync._reconcile_metadata_only_linked_images(
        client, obs, "cloud-obs-481", existing_rows
    )

    # Both already-linked images are skipped for WebP prep.
    assert skip_ids == {1825, 1826}
    assert kept_ids == {"cloud-image-1825", "cloud-image-1826"}
    # Only the drifted-metadata image triggered a push_image_metadata call —
    # the already-matching one is a true no-op.
    assert len(client.push_image_metadata_calls) == 1
    patched = client.push_image_metadata_calls[0]
    assert int(patched["id"]) == 1825
    assert patched.get("notes") == "old note"  # local wins for the local push
    assert patched["_storage_path"] == "user-1/cloud-obs-481/cloud-image-1825.webp"


def test_reconcile_metadata_only_linked_images_leaves_changed_bytes_alone(monkeypatch, tmp_path):
    """If the local source bytes changed, keep the image in the prepare path.

    Guards against a stale WebP being kept when the user replaces a file:
    the mtime moves and the pass must decline to add the image to skip_ids.
    """
    db_path = _init_db(tmp_path)
    _patch_connections(monkeypatch, db_path)

    image_path = tmp_path / "img_stale.jpg"
    image_path.write_bytes(b"original-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at, date, user_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (91, "cloud-obs-91", "dirty", "2026-05-01T00:00:00Z", "2026-05-01", "user-1"),
        )
        conn.execute(
            "INSERT INTO images (id, observation_id, cloud_id, filepath, image_type, "
            "sort_order, notes, synced_at, source_role, file_purpose) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                701,
                91,
                "cloud-image-701",
                str(image_path),
                "field",
                0,
                "note",
                "2026-05-01T00:00:00Z",
                "local_canonical",
                "field",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    cloud_sync._refresh_local_cloud_media_signature(91)

    # Simulate the user editing the file: rewrite bytes AND bump mtime.
    image_path.write_bytes(b"edited-larger-payload")
    import os
    stat = image_path.stat()
    os.utime(image_path, (stat.st_atime + 5.0, stat.st_mtime + 5.0))

    class _StubClient:
        user_id = "user-1"

        def __init__(self) -> None:
            self.push_image_metadata_calls: list[dict] = []

        def _observation_images_support_ai_crop(self) -> bool:
            return True

        def _observation_images_support_ai_crop_custom(self) -> bool:
            return True

        def _observation_images_support_upload_metadata(self) -> bool:
            return False

        def _observation_images_support_original_storage_path(self) -> bool:
            return False

        def push_image_metadata(self, img, obs_cloud_id, storage_path):
            self.push_image_metadata_calls.append(dict(img))
            return "cloud-image-701"

    existing_rows = [
        {
            "id": "cloud-image-701",
            "desktop_id": 701,
            "sort_order": 0,
            "image_type": "field",
            "storage_path": "user-1/cloud-obs-91/cloud-image-701.webp",
            "original_filename": "img_stale.jpg",
            "notes": "note",
        }
    ]

    client = _StubClient()
    skip_ids, kept_ids = cloud_sync._reconcile_metadata_only_linked_images(
        client, {"id": 91}, "cloud-obs-91", existing_rows
    )

    # Bytes changed → do NOT metadata-skip. The image must flow through the
    # normal prepare-and-upload path so the new bytes actually reach the
    # cloud.
    assert skip_ids == set()
    assert kept_ids == set()
    assert client.push_image_metadata_calls == []
