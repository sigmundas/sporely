"""Per-image cloud-storage intent ledger (2026-08-19 mass-upload fix).

Pins the incremental initialization model in
``_ensure_cloud_image_storage_intent_initialized``:

  * Storage intent is recorded per image in
    ``sporely_cloud_image_storage_intent_ids_<obs>``. Absence from the
    excluded set alone never proves a decision.
  * Images imported after a previous initialization (stale observation
    sentinel — mechanism B) still receive a default.
  * A magnification group with any initialized member defaults new members
    to excluded and never silently selects a replacement keeper
    (mechanism A / group freeze replacement).
  * Explicit checkbox interaction marks the image initialized so seeding
    can never override it.
  * The pending-media dirty scan seeds intent first and only evaluates
    initialized rows.
  * Initialization performs zero cloud I/O.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from database import models, schema
from utils import cloud_sync


def _init_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "intent-ledger.sqlite"
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
                file_purpose TEXT
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


def _seed_obs(db_path: Path, obs_id: int, *, cloud_id: str | None = None,
              sync_status: str = "synced") -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, user_id, sync_status, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (obs_id, cloud_id, "user-x", sync_status, "2026-08-01 10:00:00"),
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
    image_type: str = "microscope",
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


def _seed_tombstone(db_path: Path, deleted_cloud_id: str,
                    delete_synced_at: str | None) -> None:
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


def _write_file(tmp_path: Path, name: str) -> str:
    p = tmp_path / name
    p.write_bytes(b"x")
    return str(p)


def _excluded(obs_id: int) -> set[int]:
    return cloud_sync._cloud_image_storage_excluded_image_ids(obs_id)


def _ledger(obs_id: int) -> set[int]:
    return cloud_sync._cloud_image_storage_intent_initialized_ids(obs_id)


def _seed_ledger(db_path: Path, obs_id: int, image_ids) -> None:
    _seed_setting(
        db_path,
        cloud_sync._cloud_image_storage_intent_ledger_key(obs_id),
        json.dumps(sorted(image_ids)),
    )


# ---------------------------------------------------------------------------
# 1. STALE SENTINEL: observation initialized while it had field images only;
#    20 microscope images imported later in one new group still get the
#    sparse default, and the pending scan does not dirty for all 20.
# ---------------------------------------------------------------------------


def test_stale_sentinel_late_microscope_import_gets_sparse_default(
    monkeypatch, tmp_path
):
    db_path = _init_db(tmp_path)
    _patch(monkeypatch, db_path)
    _seed_obs(db_path, 800, cloud_id="cloud-800")
    _seed_image(
        db_path, image_id=1, obs_id=800, image_type="field",
        filepath=_write_file(tmp_path, "field-1.jpg"),
        cloud_id="cloud-img-1", synced=True,
    )
    # Historic initialization: only the field image has intent. The legacy
    # observation-level sentinel may also linger — it must be ignored.
    _seed_ledger(db_path, 800, [1])
    _seed_setting(db_path, "sporely_cloud_image_storage_initialized_800", "1")

    micro_ids = list(range(10, 30))  # 20 images, one new 63x group
    for image_id in micro_ids:
        _seed_image(
            db_path, image_id=image_id, obs_id=800,
            filepath=_write_file(tmp_path, f"m-{image_id}.jpg"),
            image_type="microscope", objective_name="63X_1.32_PL_APO",
        )

    cloud_sync._ensure_cloud_image_storage_intent_initialized(800)

    excluded = _excluded(800)
    desired_micro = set(micro_ids) - excluded
    # Exactly one keeper; the other 19 default excluded.
    assert desired_micro == {10}
    assert excluded == set(micro_ids) - {10}
    assert _ledger(800) == {1, *micro_ids}

    # Pending dirty scan must not re-dirty over the 19 excluded frames: only
    # the keeper is pending.
    pending = cloud_sync._pending_cloud_pushable_image_ids(800)
    assert pending == [10]


# ---------------------------------------------------------------------------
# 2. MULTIPLE NEW GROUPS: one keeper per genuinely new group.
# ---------------------------------------------------------------------------


def test_multiple_new_groups_get_one_keeper_each(monkeypatch, tmp_path):
    db_path = _init_db(tmp_path)
    _patch(monkeypatch, db_path)
    _seed_obs(db_path, 801, cloud_id="cloud-801")
    _seed_image(
        db_path, image_id=1, obs_id=801, image_type="field",
        filepath=_write_file(tmp_path, "f.jpg"), cloud_id="c-1", synced=True,
    )
    _seed_ledger(db_path, 801, [1])
    for image_id, objective in [
        (11, "63X_1.32_PL_APO"), (12, "63X_1.32_PL_APO"),
        (21, "100X"), (22, "100X"), (23, "100X"),
    ]:
        _seed_image(
            db_path, image_id=image_id, obs_id=801,
            filepath=_write_file(tmp_path, f"m-{image_id}.jpg"),
            image_type="microscope", objective_name=objective,
        )

    cloud_sync._ensure_cloud_image_storage_intent_initialized(801)

    assert _excluded(801) == {12, 22, 23}
    assert _ledger(801) == {1, 11, 12, 21, 22, 23}


# ---------------------------------------------------------------------------
# 3. EXISTING GROUP: initialized 63x group with a byte-backed image; ten
#    later-imported 63x images all default excluded.
# ---------------------------------------------------------------------------


def test_existing_initialized_group_defaults_new_members_excluded(
    monkeypatch, tmp_path
):
    db_path = _init_db(tmp_path)
    _patch(monkeypatch, db_path)
    _seed_obs(db_path, 802, cloud_id="cloud-802")
    _seed_image(
        db_path, image_id=5, obs_id=802,
        filepath=_write_file(tmp_path, "keeper.jpg"),
        image_type="microscope", objective_name="63X_1.32_PL_APO",
        cloud_id="cloud-img-5", synced=True,
    )
    _seed_ledger(db_path, 802, [5])

    new_ids = list(range(50, 60))
    for image_id in new_ids:
        _seed_image(
            db_path, image_id=image_id, obs_id=802,
            filepath=_write_file(tmp_path, f"m-{image_id}.jpg"),
            image_type="microscope", objective_name="63X_1.32_PL_APO",
        )

    cloud_sync._ensure_cloud_image_storage_intent_initialized(802)

    assert _excluded(802) == set(new_ids)
    assert 5 not in _excluded(802)
    assert _ledger(802) == {5, *new_ids}


# ---------------------------------------------------------------------------
# 4. EXISTING GROUP, USER UNCHECKED EVERYTHING: a new member also defaults
#    excluded; the initializer does not silently re-enable cloud storage.
# ---------------------------------------------------------------------------


def test_group_with_all_members_unchecked_does_not_reenable(monkeypatch, tmp_path):
    db_path = _init_db(tmp_path)
    _patch(monkeypatch, db_path)
    _seed_obs(db_path, 803, cloud_id="cloud-803")
    for image_id in (61, 62):
        _seed_image(
            db_path, image_id=image_id, obs_id=803,
            filepath=_write_file(tmp_path, f"m-{image_id}.jpg"),
            image_type="microscope", objective_name="40x objective",
        )
    # Both initialized and both explicitly unchecked by the user.
    _seed_ledger(db_path, 803, [61, 62])
    _seed_setting(
        db_path,
        cloud_sync._cloud_image_storage_excluded_ids_key(803),
        json.dumps([61, 62]),
    )

    _seed_image(
        db_path, image_id=63, obs_id=803,
        filepath=_write_file(tmp_path, "m-63.jpg"),
        image_type="microscope", objective_name="40x objective",
    )

    cloud_sync._ensure_cloud_image_storage_intent_initialized(803)

    assert _excluded(803) == {61, 62, 63}
    assert _ledger(803) == {61, 62, 63}


# ---------------------------------------------------------------------------
# 5. EXPLICIT USER SELECTION: checking a newly-added microscope image marks
#    it initialized; future initialization never overrides it.
# ---------------------------------------------------------------------------


def test_explicit_selection_marks_initialized_and_is_never_overridden(
    monkeypatch, tmp_path
):
    db_path = _init_db(tmp_path)
    _patch(monkeypatch, db_path)
    _seed_obs(db_path, 804, cloud_id="cloud-804")
    _seed_image(
        db_path, image_id=71, obs_id=804,
        filepath=_write_file(tmp_path, "m-71.jpg"),
        image_type="microscope", objective_name="63X_1.32_PL_APO",
        cloud_id="cloud-img-71", synced=True,
    )
    _seed_ledger(db_path, 804, [71])
    _seed_image(
        db_path, image_id=72, obs_id=804,
        filepath=_write_file(tmp_path, "m-72.jpg"),
        image_type="microscope", objective_name="63X_1.32_PL_APO",
    )

    # User explicitly checks the new image before any seeding ran.
    result = cloud_sync.set_image_cloud_selected(72, True)
    assert result is not None
    assert cloud_sync.cloud_image_storage_intent_initialized(804, 72) is True
    assert 72 not in _excluded(804)

    # Default seeding for its (initialized) group would exclude it — the
    # ledger entry must protect the explicit choice.
    cloud_sync._ensure_cloud_image_storage_intent_initialized(804)
    assert 72 not in _excluded(804)
    assert cloud_sync.cloud_image_bytes_desired(804, 72) is True


# ---------------------------------------------------------------------------
# 6. FIELD IMAGES: a new field image keeps the desired=true default.
# ---------------------------------------------------------------------------


def test_new_field_image_defaults_desired(monkeypatch, tmp_path):
    db_path = _init_db(tmp_path)
    _patch(monkeypatch, db_path)
    _seed_obs(db_path, 805, cloud_id="cloud-805")
    _seed_image(
        db_path, image_id=81, obs_id=805, image_type="field",
        filepath=_write_file(tmp_path, "f-81.jpg"),
        cloud_id="c-81", synced=True,
    )
    _seed_ledger(db_path, 805, [81])
    _seed_image(
        db_path, image_id=82, obs_id=805, image_type="field",
        filepath=_write_file(tmp_path, "f-82.jpg"),
    )

    cloud_sync._ensure_cloud_image_storage_intent_initialized(805)

    assert 82 not in _excluded(805)
    assert cloud_sync.cloud_image_bytes_desired(805, 82) is True
    assert _ledger(805) == {81, 82}


# ---------------------------------------------------------------------------
# 7. LEGACY GROUP WITH BYTE-BACKED MEMBER: byte-backed member unchanged;
#    the 15 cloud-null siblings become excluded; nothing is deleted.
# ---------------------------------------------------------------------------


def test_legacy_group_with_byte_backed_member_excludes_pending_siblings(
    monkeypatch, tmp_path
):
    db_path = _init_db(tmp_path)
    _patch(monkeypatch, db_path)
    _seed_obs(db_path, 806, cloud_id="cloud-806")
    _seed_image(
        db_path, image_id=90, obs_id=806,
        filepath=_write_file(tmp_path, "m-90.jpg"),
        image_type="microscope", objective_name="63X_1.32_PL_APO",
        cloud_id="cloud-img-90", synced=True,
    )
    sibling_ids = list(range(91, 106))  # 15 cloud-null siblings
    for image_id in sibling_ids:
        _seed_image(
            db_path, image_id=image_id, obs_id=806,
            filepath=_write_file(tmp_path, f"m-{image_id}.jpg"),
            image_type="microscope", objective_name="63X_1.32_PL_APO",
        )
    # No ledger, no sentinel — pure legacy rows.

    cloud_sync._ensure_cloud_image_storage_intent_initialized(806)

    assert 90 not in _excluded(806)
    assert _excluded(806) == set(sibling_ids)
    assert _ledger(806) == {90, *sibling_ids}
    # Storage-intent repair only: no tombstones were generated.
    assert models.list_pending_image_tombstones() == []
    # And the byte-backed member's identity is untouched.
    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute(
            "SELECT cloud_id FROM images WHERE id = 90"
        ).fetchone()[0] == "cloud-img-90"
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 8. LEGACY GROUP WITHOUT BYTE-BACKED MEMBER: exactly one desired, 14 excluded.
# ---------------------------------------------------------------------------


def test_legacy_group_without_byte_backed_member_gets_single_keeper(
    monkeypatch, tmp_path
):
    db_path = _init_db(tmp_path)
    _patch(monkeypatch, db_path)
    _seed_obs(db_path, 807, cloud_id="cloud-807")
    image_ids = list(range(110, 125))  # 15 pending microscope images
    for image_id in image_ids:
        _seed_image(
            db_path, image_id=image_id, obs_id=807,
            filepath=_write_file(tmp_path, f"m-{image_id}.jpg"),
            image_type="microscope", objective_name="100X",
        )

    cloud_sync._ensure_cloud_image_storage_intent_initialized(807)

    excluded = _excluded(807)
    desired = set(image_ids) - excluded
    assert desired == {110}
    assert len(excluded) == 14
    assert _ledger(807) == set(image_ids)


# ---------------------------------------------------------------------------
# 9. METADATA-ONLY ANCHORS: anchor identity and registry stay intact while
#    byte intent defaults local-only; anchors are never inferred byte-backed.
# ---------------------------------------------------------------------------


def test_metadata_only_anchor_defaults_local_only_and_keeps_identity(
    monkeypatch, tmp_path
):
    db_path = _init_db(tmp_path)
    _patch(monkeypatch, db_path)
    _seed_obs(db_path, 808, cloud_id="cloud-808")
    # Anchor: cloud-identified but registered metadata-only.
    _seed_image(
        db_path, image_id=130, obs_id=808,
        filepath=_write_file(tmp_path, "m-130.jpg"),
        image_type="microscope", objective_name="63X_1.32_PL_APO",
        cloud_id="cloud-anchor-130", synced=True,
    )
    # Byte-backed member of the same group.
    _seed_image(
        db_path, image_id=131, obs_id=808,
        filepath=_write_file(tmp_path, "m-131.jpg"),
        image_type="microscope", objective_name="63X_1.32_PL_APO",
        cloud_id="cloud-img-131", synced=True,
    )
    _seed_setting(
        db_path,
        cloud_sync._cloud_metadata_only_image_ids_key(808),
        json.dumps([130]),
    )

    cloud_sync._ensure_cloud_image_storage_intent_initialized(808)

    # The anchor is not treated as byte-backed: it defaults local-only.
    assert 130 in _excluded(808)
    # The genuinely byte-backed member stays desired.
    assert 131 not in _excluded(808)
    # Anchor identity and registry untouched.
    assert cloud_sync._cloud_metadata_only_image_ids(808) == {130}
    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute(
            "SELECT cloud_id FROM images WHERE id = 130"
        ).fetchone()[0] == "cloud-anchor-130"
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 10. TOMBSTONES: DELETE_PENDING/DELETED remain excluded; initialization
#     generates no new tombstones.
# ---------------------------------------------------------------------------


def test_tombstoned_images_stay_excluded_without_new_tombstones(
    monkeypatch, tmp_path
):
    db_path = _init_db(tmp_path)
    _patch(monkeypatch, db_path)
    _seed_obs(db_path, 809, cloud_id="cloud-809")
    _seed_image(
        db_path, image_id=140, obs_id=809,
        filepath=_write_file(tmp_path, "m-140.jpg"),
        image_type="microscope", objective_name="100X",
        cloud_id="cloud-img-140", synced=True,
    )
    _seed_image(
        db_path, image_id=141, obs_id=809, image_type="field",
        filepath=_write_file(tmp_path, "f-141.jpg"),
        cloud_id="cloud-img-141", synced=True,
    )
    _seed_tombstone(db_path, "cloud-img-140", delete_synced_at=None)   # DELETE_PENDING
    _seed_tombstone(db_path, "cloud-img-141", delete_synced_at="2026-08-02")  # DELETED

    before = models.list_pending_image_tombstones()

    cloud_sync._ensure_cloud_image_storage_intent_initialized(809)

    assert {140, 141} <= _excluded(809)
    assert _ledger(809) == {140, 141}
    after = models.list_pending_image_tombstones()
    assert len(after) == len(before)


# ---------------------------------------------------------------------------
# 11. IDEMPOTENCY: the second run makes zero settings writes.
# ---------------------------------------------------------------------------


def test_second_run_makes_zero_changes(monkeypatch, tmp_path):
    db_path = _init_db(tmp_path)
    _patch(monkeypatch, db_path)
    _seed_obs(db_path, 810, cloud_id="cloud-810")
    _seed_image(
        db_path, image_id=150, obs_id=810,
        filepath=_write_file(tmp_path, "m-150.jpg"),
        image_type="microscope", objective_name="63X_1.32_PL_APO",
        cloud_id="cloud-img-150", synced=True,
    )
    for image_id in (151, 152):
        _seed_image(
            db_path, image_id=image_id, obs_id=810,
            filepath=_write_file(tmp_path, f"m-{image_id}.jpg"),
            image_type="microscope", objective_name="63X_1.32_PL_APO",
        )

    first = cloud_sync._ensure_cloud_image_storage_intent_initialized(810)
    assert first == {"seeded_desired": 1, "seeded_excluded": 2}
    excluded_after_first = _excluded(810)
    ledger_after_first = _ledger(810)

    writes: list[tuple[str, str]] = []
    original_set_setting = models.SettingsDB.set_setting

    def _spy(key, value):
        writes.append((key, value))
        return original_set_setting(key, value)

    monkeypatch.setattr(models.SettingsDB, "set_setting", staticmethod(_spy))
    second = cloud_sync._ensure_cloud_image_storage_intent_initialized(810)
    assert second == {"seeded_desired": 0, "seeded_excluded": 0}
    assert writes == []
    assert _excluded(810) == excluded_after_first
    assert _ledger(810) == ledger_after_first


# ---------------------------------------------------------------------------
# 12. PENDING DIRTY SCAN ORDER: uninitialized rows are seeded before the
#     pending-media scan evaluates them; the scan dirties only genuinely
#     desired pending rows.
# ---------------------------------------------------------------------------


def test_pending_dirty_scan_seeds_intent_before_evaluating(monkeypatch, tmp_path):
    db_path = _init_db(tmp_path)
    _patch(monkeypatch, db_path)
    _seed_obs(db_path, 811, cloud_id="cloud-811", sync_status="synced")
    _seed_image(
        db_path, image_id=160, obs_id=811,
        filepath=_write_file(tmp_path, "m-160.jpg"),
        image_type="microscope", objective_name="63X_1.32_PL_APO",
        cloud_id="cloud-img-160", synced=True,
    )
    late_ids = list(range(161, 171))  # 10 later-imported, uninitialized
    for image_id in late_ids:
        _seed_image(
            db_path, image_id=image_id, obs_id=811,
            filepath=_write_file(tmp_path, f"m-{image_id}.jpg"),
            image_type="microscope", objective_name="63X_1.32_PL_APO",
        )
    _seed_ledger(db_path, 811, [160])

    # Direct evaluation without seeding must refuse the uninitialized rows.
    pending_before = cloud_sync._pending_cloud_pushable_image_ids(811)
    assert pending_before == []

    completed = cloud_sync._mark_cloud_observations_dirty_for_pending_local_images(
        include_pending_local_media_uploads=True,
    )
    assert completed is True

    # The scan seeded intent: all late imports default excluded (their group
    # already has an initialized byte-backed member), so nothing is pending
    # and the observation is NOT re-dirtied.
    assert _ledger(811) == {160, *late_ids}
    assert _excluded(811) == set(late_ids)
    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute(
            "SELECT sync_status FROM observations WHERE id = 811"
        ).fetchone()[0] == "synced"
    finally:
        conn.close()


def test_pending_dirty_scan_dirties_only_for_desired_pending_rows(
    monkeypatch, tmp_path
):
    db_path = _init_db(tmp_path)
    _patch(monkeypatch, db_path)
    _seed_obs(db_path, 812, cloud_id="cloud-812", sync_status="synced")
    # Genuinely new group: keeper is desired and pending → obs must dirty.
    for image_id in (170, 171, 172):
        _seed_image(
            db_path, image_id=image_id, obs_id=812,
            filepath=_write_file(tmp_path, f"m-{image_id}.jpg"),
            image_type="microscope", objective_name="100X",
        )

    completed = cloud_sync._mark_cloud_observations_dirty_for_pending_local_images(
        include_pending_local_media_uploads=True,
    )
    assert completed is True

    assert _excluded(812) == {171, 172}
    pending = cloud_sync._pending_cloud_pushable_image_ids(812)
    assert pending == [170]
    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute(
            "SELECT sync_status FROM observations WHERE id = 812"
        ).fetchone()[0] == "dirty"
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 13. PULL-ONLY / ZERO CLOUD I/O: intent initialization and the dirty scan
#     perform no HTTP traffic at all.
# ---------------------------------------------------------------------------


def test_intent_initialization_performs_zero_cloud_io(monkeypatch, tmp_path):
    db_path = _init_db(tmp_path)
    _patch(monkeypatch, db_path)
    _seed_obs(db_path, 813, cloud_id="cloud-813", sync_status="synced")
    for image_id in (180, 181):
        _seed_image(
            db_path, image_id=image_id, obs_id=813,
            filepath=_write_file(tmp_path, f"m-{image_id}.jpg"),
            image_type="microscope", objective_name="63X_1.32_PL_APO",
        )

    def _no_network(*args, **kwargs):
        raise AssertionError("cloud I/O attempted during intent initialization")

    monkeypatch.setattr(cloud_sync.requests.Session, "request", _no_network)
    monkeypatch.setattr(cloud_sync.requests, "request", _no_network)
    monkeypatch.setattr(cloud_sync.requests, "get", _no_network)
    monkeypatch.setattr(cloud_sync.requests, "post", _no_network)
    monkeypatch.setattr(cloud_sync.requests, "patch", _no_network)

    cloud_sync._ensure_cloud_image_storage_intent_initialized(813)
    cloud_sync._mark_cloud_observations_dirty_for_pending_local_images(
        include_pending_local_media_uploads=True,
    )

    assert _excluded(813) == {181}
    assert _ledger(813) == {180, 181}
