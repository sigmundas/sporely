"""Stage 1: cloud-storage-desired excluded-set initializer.

Pins the invariants of
``_initialize_cloud_image_storage_desired_state_for_observation``:

  * UPLOADED images stay desired even if the user historically excluded them
    from Artsobs publication — legacy publication exclusions never migrate
    into cloud storage state.
  * DELETE_PENDING/DELETED images land in the excluded set but are not
    re-tombstoned by the initializer (idempotent).
  * Local-only microscope images with multiple magnification groups get a
    sparse default: one desired per group, the rest excluded.
  * A microscope group with an already-uploaded image keeps that image as the
    desired keeper and defaults the cloud-null siblings to excluded (the
    2026-08-19 fix — the old group-freeze rule left siblings looking checked).
  * The per-image intent ledger prevents a second initialization from
    clobbering user edits.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from database import models, schema
from utils import cloud_sync


def _init_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "initializer.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cloud_id TEXT,
                user_id TEXT,
                sync_status TEXT,
                updated_at TEXT
            );
            CREATE TABLE images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observation_id INTEGER,
                cloud_id TEXT,
                filepath TEXT NOT NULL,
                original_filepath TEXT,
                image_type TEXT,
                sort_order INTEGER,
                objective_name TEXT,
                synced_at TEXT,
                notes TEXT,
                source_role TEXT,
                file_purpose TEXT,
                storage_path TEXT
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
    return db_path


def _patch(monkeypatch, db_path: Path) -> None:
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))


def _seed_obs(db_path: Path, obs_id: int = 700) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, user_id, sync_status, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (obs_id, f"cloud-{obs_id}", "user-x", "synced", "2026-08-01 10:00:00"),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_image(
    db_path: Path,
    *,
    image_id: int,
    obs_id: int,
    filepath: str,
    image_type: str = "field",
    cloud_id: str | None = None,
    synced: bool = False,
    objective_name: str | None = None,
    sort_order: int | None = None,
) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO images (id, observation_id, cloud_id, filepath, image_type, "
            "sort_order, objective_name, synced_at, source_role, file_purpose) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                image_id,
                obs_id,
                cloud_id,
                filepath,
                image_type,
                sort_order if sort_order is not None else image_id,
                objective_name,
                "2026-08-01 10:05:00" if synced else None,
                "converted_local",
                image_type,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_setting(db_path: Path, key: str, value: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key, value),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_tombstone(db_path: Path, deleted_cloud_id: str, delete_synced_at: str | None) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO image_tombstones (deleted_cloud_id, deleted_at, delete_synced_at, "
            "local_observation_id) VALUES (?, ?, ?, ?)",
            (deleted_cloud_id, "2026-08-01 10:10:00", delete_synced_at, None),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 1. UPLOADED image preserved as desired even if the user historically
#    excluded it via the old Artsobs publish key.
# ---------------------------------------------------------------------------


def test_initializer_does_not_migrate_legacy_artsobs_exclusion_for_uploaded(
    monkeypatch, tmp_path
):
    db_path = _init_db(tmp_path)
    _patch(monkeypatch, db_path)
    _seed_obs(db_path, 700)
    (tmp_path / "img.jpg").write_bytes(b"x")
    _seed_image(
        db_path,
        image_id=11,
        obs_id=700,
        filepath=str(tmp_path / "img.jpg"),
        image_type="field",
        cloud_id="cloud-img-11",
        synced=True,
    )
    # Legacy artsobs exclusion — must NOT migrate.
    _seed_setting(db_path, "artsobs_publish_excluded_image_ids_700", "[11]")

    cloud_sync._initialize_cloud_image_storage_desired_state_for_observation(700)

    excluded = cloud_sync._cloud_image_storage_excluded_image_ids(700)
    assert 11 not in excluded
    assert cloud_sync.cloud_image_bytes_desired(700, 11) is True


# ---------------------------------------------------------------------------
# 2. DELETE_PENDING image ends up in excluded set; no additional tombstone
#    is written by the initializer.
# ---------------------------------------------------------------------------


def test_initializer_marks_delete_pending_excluded_without_extra_tombstone(
    monkeypatch, tmp_path
):
    db_path = _init_db(tmp_path)
    _patch(monkeypatch, db_path)
    _seed_obs(db_path, 701)
    (tmp_path / "pending.jpg").write_bytes(b"x")
    _seed_image(
        db_path,
        image_id=12,
        obs_id=701,
        filepath=str(tmp_path / "pending.jpg"),
        image_type="field",
        cloud_id="cloud-img-12",
        synced=True,
    )
    # Existing unsynced tombstone → image is DELETE_PENDING.
    _seed_tombstone(db_path, "cloud-img-12", delete_synced_at=None)

    tombstones_before = models.list_pending_image_tombstones()
    assert len(tombstones_before) == 1

    cloud_sync._initialize_cloud_image_storage_desired_state_for_observation(701)

    excluded = cloud_sync._cloud_image_storage_excluded_image_ids(701)
    assert 12 in excluded
    tombstones_after = models.list_pending_image_tombstones()
    # No new tombstone rows created.
    assert len(tombstones_after) == 1
    assert tombstones_after[0]["deleted_cloud_id"] == "cloud-img-12"


# ---------------------------------------------------------------------------
# 3. Local-only microscope with 3 magnification groups: one desired per group,
#    the rest excluded.
# ---------------------------------------------------------------------------


def test_initializer_sparse_default_for_local_microscope_groups(
    monkeypatch, tmp_path
):
    db_path = _init_db(tmp_path)
    _patch(monkeypatch, db_path)
    _seed_obs(db_path, 702)
    # Group "10x": images 21, 22
    # Group "40x": images 23, 24, 25
    # Group "100x": image 26
    groups = [
        (21, "10x objective"),
        (22, "10x objective"),
        (23, "40x objective"),
        (24, "40x objective"),
        (25, "40x objective"),
        (26, "100x objective"),
    ]
    for img_id, obj in groups:
        p = tmp_path / f"micro-{img_id}.jpg"
        p.write_bytes(b"x")
        _seed_image(
            db_path,
            image_id=img_id,
            obs_id=702,
            filepath=str(p),
            image_type="microscope",
            objective_name=obj,
        )

    cloud_sync._initialize_cloud_image_storage_desired_state_for_observation(702)

    excluded = cloud_sync._cloud_image_storage_excluded_image_ids(702)
    # Expect one desired per group: 21 (10x), 23 (40x), 26 (100x).
    # The rest are excluded: 22, 24, 25.
    assert excluded == {22, 24, 25}
    desired = {img_id for img_id, _ in groups} - excluded
    assert desired == {21, 23, 26}


# ---------------------------------------------------------------------------
# 4. Microscope group that already contains a cloud-identified image keeps
#    that image as the desired keeper and defaults the cloud-null siblings to
#    excluded. (Replaces the old group-freeze rule that left siblings looking
#    explicitly checked — mechanism A of the 2026-08-19 mass upload.)
# ---------------------------------------------------------------------------


def test_initializer_keeps_uploaded_keeper_and_excludes_cloud_null_siblings(
    monkeypatch, tmp_path
):
    db_path = _init_db(tmp_path)
    _patch(monkeypatch, db_path)
    _seed_obs(db_path, 703)
    # Group "10x": one uploaded (31), two local (32, 33).
    # Group "40x": all local (34, 35).
    for image_id, obj, cloud_id in [
        (31, "10x objective", "cloud-img-31"),
        (32, "10x objective", None),
        (33, "10x objective", None),
        (34, "40x objective", None),
        (35, "40x objective", None),
    ]:
        p = tmp_path / f"m-{image_id}.jpg"
        p.write_bytes(b"x")
        _seed_image(
            db_path,
            image_id=image_id,
            obs_id=703,
            filepath=str(p),
            image_type="microscope",
            objective_name=obj,
            cloud_id=cloud_id,
            synced=bool(cloud_id),
        )

    cloud_sync._initialize_cloud_image_storage_desired_state_for_observation(703)

    excluded = cloud_sync._cloud_image_storage_excluded_image_ids(703)
    # Group 10x has an already-uploaded image (31): it is the byte-backed
    # keeper and stays desired; cloud-null siblings 32 and 33 default to
    # excluded instead of silently looking checked.
    assert 31 not in excluded
    assert 32 in excluded
    assert 33 in excluded
    # Group 40x has no cloud-identified image — sparse default applies:
    # 34 stays desired (first), 35 is excluded.
    assert excluded == {32, 33, 35}
    # Every image now has a recorded storage-intent decision.
    assert cloud_sync._cloud_image_storage_intent_initialized_ids(703) == {
        31, 32, 33, 34, 35,
    }


# ---------------------------------------------------------------------------
# 5. The per-image intent ledger prevents re-initialization from clobbering
#    subsequent user edits.
# ---------------------------------------------------------------------------


def test_initializer_ledger_makes_second_call_a_noop(monkeypatch, tmp_path):
    db_path = _init_db(tmp_path)
    _patch(monkeypatch, db_path)
    _seed_obs(db_path, 704)
    # Group with two local microscope images so the sparse default is visible.
    for image_id in (41, 42):
        p = tmp_path / f"m-{image_id}.jpg"
        p.write_bytes(b"x")
        _seed_image(
            db_path,
            image_id=image_id,
            obs_id=704,
            filepath=str(p),
            image_type="microscope",
            objective_name="40x objective",
        )

    cloud_sync._initialize_cloud_image_storage_desired_state_for_observation(704)
    first_pass = cloud_sync._cloud_image_storage_excluded_image_ids(704)
    # First pass: image 41 desired, 42 excluded.
    assert first_pass == {42}

    # Now the user re-selects 42 and de-selects 41.
    cloud_sync._set_cloud_image_storage_excluded_image_ids(704, {41})

    # A second initializer call must be a no-op — do not clobber the user
    # choice. Both images are already in the per-image intent ledger, so the
    # call is a cheap early return.
    cloud_sync._initialize_cloud_image_storage_desired_state_for_observation(704)
    after_second = cloud_sync._cloud_image_storage_excluded_image_ids(704)
    assert after_second == {41}
