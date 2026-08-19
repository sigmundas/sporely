"""Stage 2 cloud reconciliation classifier tests.

Pins the classification decisions of the Stage 2 dry-run reconciliation tool
against every category (A-H). One integration-style test exercises the CLI
runner against an in-memory-style SQLite file and a fake, strictly read-only
cloud client. The fake client raises on any mutating method to prove the
tool never issues writes.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from database import schema
from tools import cloud_reconciliation_report as recon


# ---------------------------------------------------------------------------
# Common fixtures / helpers
# ---------------------------------------------------------------------------


def _seed_schema(db_path: Path) -> None:
    """Create the tables and indexes the tool reads. Kept minimal on purpose."""
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cloud_id TEXT,
                folder_path TEXT,
                sync_status TEXT
            );
            CREATE TABLE images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observation_id INTEGER,
                cloud_id TEXT,
                filepath TEXT,
                original_filepath TEXT,
                image_type TEXT,
                sort_order INTEGER,
                synced_at TEXT,
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


def _insert_observation(db_path: Path, *, local_id: int, cloud_id: str | None) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, folder_path, sync_status) "
            "VALUES (?, ?, ?, ?)",
            (local_id, cloud_id, f"/tmp/obs/{local_id}", "synced"),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_image(
    db_path: Path,
    *,
    image_id: int,
    observation_id: int,
    cloud_id: str | None,
    image_type: str = "field",
    sort_order: int | None = None,
    synced: bool = False,
) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO images (id, observation_id, cloud_id, filepath, "
            "image_type, sort_order, synced_at, source_role, file_purpose) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                image_id,
                observation_id,
                cloud_id,
                f"/tmp/img/{image_id}.jpg",
                image_type,
                sort_order,
                "2026-08-01T00:00:00Z" if synced else None,
                "local_canonical",
                image_type,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _set_setting(db_path: Path, key: str, value: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_tombstone(
    db_path: Path,
    *,
    deleted_cloud_id: str,
    local_image_id: int | None = None,
    delete_synced_at: str | None = None,
    deleted_at: str = "2026-08-05T00:00:00Z",
) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO image_tombstones (deleted_cloud_id, deleted_at, "
            "delete_synced_at, local_image_id) VALUES (?, ?, ?, ?)",
            (deleted_cloud_id, deleted_at, delete_synced_at, local_image_id),
        )
        conn.commit()
    finally:
        conn.close()


def _cloud_row(**overrides):
    row = {
        "id": None,
        "desktop_id": None,
        "user_id": "user-1",
        "observation_id": None,
        "storage_path": None,
        "original_filename": None,
        "image_type": "field",
        "micro_category": None,
        "sort_order": 0,
        "deleted_at": None,
        "upload_mode": "full",
        "source_width": 100,
        "source_height": 100,
        "stored_width": 100,
        "stored_height": 100,
        "stored_bytes": 1234,
        "created_at": "2026-08-01T00:00:00Z",
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# Category A — healthy_uploaded
# ---------------------------------------------------------------------------


def test_category_a_healthy_uploaded(tmp_path: Path) -> None:
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    _insert_observation(db_path, local_id=1, cloud_id="obs-1")
    _insert_image(db_path, image_id=11, observation_id=1, cloud_id="img-11", synced=True)

    with recon._open_readonly(db_path) as conn:
        local = recon.load_local_state(conn)

    cloud_row = _cloud_row(
        id="img-11",
        desktop_id=11,
        observation_id="obs-1",
        storage_path="user-1/obs-1/originals/img-11/photo.webp",
    )
    match = recon.match_cloud_row_to_local(cloud_row, local)
    row = recon.classify_cloud_row(cloud_row, match, local)

    assert row.category == recon.CATEGORY_HEALTHY_UPLOADED
    assert row.match_method == recon.MATCH_METHOD_CLOUD_ID
    assert row.local_image_id == 11
    assert row.local_cloud_bytes_desired is True


# ---------------------------------------------------------------------------
# Category B — metadata_only_anchor_ok
# ---------------------------------------------------------------------------


def test_category_b_metadata_only_anchor_ok(tmp_path: Path) -> None:
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    _insert_observation(db_path, local_id=2, cloud_id="obs-2")
    _insert_image(
        db_path,
        image_id=21,
        observation_id=2,
        cloud_id="img-21",
        image_type="microscope",
        synced=True,
    )
    # Explicit anchor state per Stage 1 persistence.
    _set_setting(
        db_path,
        f"{recon.CLOUD_METADATA_ONLY_PREFIX}2",
        json.dumps([21]),
    )

    with recon._open_readonly(db_path) as conn:
        local = recon.load_local_state(conn)

    cloud_row = _cloud_row(
        id="img-21",
        desktop_id=21,
        observation_id="obs-2",
        image_type="microscope",
        storage_path=None,  # NULL: anchor shape
        stored_bytes=None,
    )
    match = recon.match_cloud_row_to_local(cloud_row, local)
    row = recon.classify_cloud_row(cloud_row, match, local)

    assert row.category == recon.CATEGORY_METADATA_ONLY_ANCHOR_OK
    assert row.local_is_metadata_only_anchor is True
    assert row.cloud_has_bytes is False


# ---------------------------------------------------------------------------
# Category C — unwanted_cloud_bytes
# ---------------------------------------------------------------------------


def test_category_c_unwanted_cloud_bytes(tmp_path: Path) -> None:
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    _insert_observation(db_path, local_id=3, cloud_id="obs-3")
    _insert_image(db_path, image_id=31, observation_id=3, cloud_id="img-31", synced=True)
    # Persist "unchecked" state — Stage 1 canonical desired-false.
    _set_setting(
        db_path,
        f"{recon.CLOUD_IMAGE_STORAGE_EXCLUDED_PREFIX}3",
        json.dumps([31]),
    )

    with recon._open_readonly(db_path) as conn:
        local = recon.load_local_state(conn)

    cloud_row = _cloud_row(
        id="img-31",
        desktop_id=31,
        observation_id="obs-3",
        storage_path="user-1/obs-3/originals/img-31/photo.webp",
    )
    match = recon.match_cloud_row_to_local(cloud_row, local)
    row = recon.classify_cloud_row(cloud_row, match, local)

    assert row.category == recon.CATEGORY_UNWANTED_CLOUD_BYTES
    assert row.local_cloud_bytes_desired is False
    assert row.cloud_has_bytes is True


# ---------------------------------------------------------------------------
# Category D1 / D2 — lost link repairable
# ---------------------------------------------------------------------------


def test_category_d1_lost_link_desired(tmp_path: Path) -> None:
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    _insert_observation(db_path, local_id=4, cloud_id="obs-4")
    # local cloud_id is missing / stale
    _insert_image(db_path, image_id=41, observation_id=4, cloud_id=None)

    with recon._open_readonly(db_path) as conn:
        local = recon.load_local_state(conn)

    cloud_row = _cloud_row(
        id="img-41",
        desktop_id=41,
        observation_id="obs-4",
        storage_path="user-1/obs-4/originals/img-41/photo.webp",
    )
    match = recon.match_cloud_row_to_local(cloud_row, local)
    row = recon.classify_cloud_row(cloud_row, match, local)

    assert row.category == recon.CATEGORY_LOST_LINK_REPAIRABLE_KEEP
    assert row.match_method == recon.MATCH_METHOD_DESKTOP_ID
    assert row.local_cloud_bytes_desired is True


def test_category_d2_lost_link_undesired(tmp_path: Path) -> None:
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    _insert_observation(db_path, local_id=5, cloud_id="obs-5")
    _insert_image(db_path, image_id=51, observation_id=5, cloud_id=None)
    _set_setting(
        db_path,
        f"{recon.CLOUD_IMAGE_STORAGE_EXCLUDED_PREFIX}5",
        json.dumps([51]),
    )

    with recon._open_readonly(db_path) as conn:
        local = recon.load_local_state(conn)

    cloud_row = _cloud_row(
        id="img-51",
        desktop_id=51,
        observation_id="obs-5",
        storage_path="user-1/obs-5/originals/img-51/photo.webp",
    )
    match = recon.match_cloud_row_to_local(cloud_row, local)
    row = recon.classify_cloud_row(cloud_row, match, local)

    assert row.category == recon.CATEGORY_LOST_LINK_REPAIRABLE_REMOVE
    assert row.match_method == recon.MATCH_METHOD_DESKTOP_ID
    assert row.local_cloud_bytes_desired is False


# ---------------------------------------------------------------------------
# Category E — duplicate_or_ambiguous (two forms)
# ---------------------------------------------------------------------------


def test_category_e_multiple_local_share_cloud_id(tmp_path: Path) -> None:
    """Two local rows carry the same cloud_id — ambiguous match."""
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    _insert_observation(db_path, local_id=6, cloud_id="obs-6")
    _insert_image(db_path, image_id=61, observation_id=6, cloud_id="img-61")
    _insert_image(db_path, image_id=62, observation_id=6, cloud_id="img-61")

    with recon._open_readonly(db_path) as conn:
        local = recon.load_local_state(conn)

    cloud_row = _cloud_row(
        id="img-61",
        desktop_id=61,
        observation_id="obs-6",
        storage_path="user-1/obs-6/originals/img-61/photo.webp",
    )
    match = recon.match_cloud_row_to_local(cloud_row, local)
    row = recon.classify_cloud_row(cloud_row, match, local)

    assert row.category == recon.CATEGORY_DUPLICATE_OR_AMBIGUOUS
    assert row.ambiguity is not None
    assert sorted(row.candidate_local_image_ids) == [61, 62]


def test_category_e_multiple_cloud_rows_match_single_local(tmp_path: Path) -> None:
    """Two active cloud rows collapse to the same local image — flagged E."""
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    _insert_observation(db_path, local_id=7, cloud_id="obs-7")
    _insert_image(db_path, image_id=71, observation_id=7, cloud_id="img-71")

    with recon._open_readonly(db_path) as conn:
        local = recon.load_local_state(conn)

    row_a = _cloud_row(
        id="img-71",
        desktop_id=71,
        observation_id="obs-7",
        storage_path="user-1/obs-7/originals/img-71/photo.webp",
    )
    row_b = _cloud_row(
        id="img-71-dup",
        desktop_id=71,
        observation_id="obs-7",
        storage_path="user-1/obs-7/originals/img-71-dup/photo.webp",
    )
    report = recon.reconcile(local, [row_a, row_b], include_broken_active=False)
    categories = [row.category for row in report.rows]
    assert recon.CATEGORY_DUPLICATE_OR_AMBIGUOUS in categories
    assert report.ambiguity_warnings


# ---------------------------------------------------------------------------
# Category F — cloud_only_orphan (with and without bytes)
# ---------------------------------------------------------------------------


def test_category_f_cloud_only_orphan_with_bytes(tmp_path: Path) -> None:
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    # No matching observation locally — no cloud_id -> obs-8 mapping.
    with recon._open_readonly(db_path) as conn:
        local = recon.load_local_state(conn)

    cloud_row = _cloud_row(
        id="img-81",
        desktop_id=99,
        observation_id="obs-8",
        storage_path="user-1/obs-8/originals/img-81/photo.webp",
    )
    match = recon.match_cloud_row_to_local(cloud_row, local)
    row = recon.classify_cloud_row(cloud_row, match, local)

    assert row.category == recon.CATEGORY_CLOUD_ONLY_ORPHAN
    assert row.cloud_has_bytes is True
    assert "with_bytes" in row.note


def test_category_f_cloud_only_orphan_metadata_only(tmp_path: Path) -> None:
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    with recon._open_readonly(db_path) as conn:
        local = recon.load_local_state(conn)

    cloud_row = _cloud_row(
        id="img-82",
        desktop_id=99,
        observation_id="obs-9",
        image_type="microscope",
        storage_path=None,
    )
    match = recon.match_cloud_row_to_local(cloud_row, local)
    row = recon.classify_cloud_row(cloud_row, match, local)

    assert row.category == recon.CATEGORY_CLOUD_ONLY_ORPHAN
    assert row.cloud_has_bytes is False
    assert "metadata_only" in row.note


# ---------------------------------------------------------------------------
# Category G — broken_active (local UPLOADED but no cloud row seen)
# ---------------------------------------------------------------------------


def test_category_g_broken_active_missing_cloud_row(tmp_path: Path) -> None:
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    _insert_observation(db_path, local_id=10, cloud_id="obs-10")
    _insert_image(db_path, image_id=101, observation_id=10, cloud_id="img-101", synced=True)

    with recon._open_readonly(db_path) as conn:
        local = recon.load_local_state(conn)

    report = recon.reconcile(local, [], include_broken_active=True)
    assert any(row.category == recon.CATEGORY_BROKEN_ACTIVE for row in report.rows)
    broken = [row for row in report.rows if row.category == recon.CATEGORY_BROKEN_ACTIVE]
    assert broken[0].local_image_id == 101
    assert broken[0].local_cloud_id == "img-101"


def test_category_g_ignores_deleted_state(tmp_path: Path) -> None:
    """A local row with a synced tombstone is DELETED, not broken_active."""
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    _insert_observation(db_path, local_id=11, cloud_id="obs-11")
    _insert_image(db_path, image_id=111, observation_id=11, cloud_id="img-111", synced=True)
    _insert_tombstone(
        db_path,
        deleted_cloud_id="img-111",
        local_image_id=111,
        delete_synced_at="2026-08-05T01:00:00Z",
    )

    with recon._open_readonly(db_path) as conn:
        local = recon.load_local_state(conn)

    report = recon.reconcile(local, [], include_broken_active=True)
    assert all(row.category != recon.CATEGORY_BROKEN_ACTIVE for row in report.rows)


# ---------------------------------------------------------------------------
# Category H — incomplete_upload_metadata (cloud row missing storage_path
# where it should have bytes)
# ---------------------------------------------------------------------------


def test_category_h_non_microscope_row_missing_storage_path(tmp_path: Path) -> None:
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    _insert_observation(db_path, local_id=12, cloud_id="obs-12")
    _insert_image(db_path, image_id=121, observation_id=12, cloud_id="img-121", synced=True)

    with recon._open_readonly(db_path) as conn:
        local = recon.load_local_state(conn)

    cloud_row = _cloud_row(
        id="img-121",
        desktop_id=121,
        observation_id="obs-12",
        image_type="field",  # not microscope: NULL storage_path is a defect
        storage_path=None,
    )
    match = recon.match_cloud_row_to_local(cloud_row, local)
    row = recon.classify_cloud_row(cloud_row, match, local)

    assert row.category == recon.CATEGORY_INCOMPLETE_UPLOAD_METADATA
    assert row.cloud_has_bytes is False


def test_category_h_microscope_shaped_but_local_is_field(tmp_path: Path) -> None:
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    _insert_observation(db_path, local_id=13, cloud_id="obs-13")
    _insert_image(
        db_path,
        image_id=131,
        observation_id=13,
        cloud_id="img-131",
        image_type="field",
        synced=True,
    )

    with recon._open_readonly(db_path) as conn:
        local = recon.load_local_state(conn)

    cloud_row = _cloud_row(
        id="img-131",
        desktop_id=131,
        observation_id="obs-13",
        image_type="microscope",
        storage_path=None,
    )
    match = recon.match_cloud_row_to_local(cloud_row, local)
    row = recon.classify_cloud_row(cloud_row, match, local)

    assert row.category == recon.CATEGORY_INCOMPLETE_UPLOAD_METADATA


# ---------------------------------------------------------------------------
# Match semantics
# ---------------------------------------------------------------------------


def test_match_prefers_cloud_id_over_desktop_id(tmp_path: Path) -> None:
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    _insert_observation(db_path, local_id=14, cloud_id="obs-14")
    # local row keeps its cloud_id; desktop_id also matches — cloud_id wins.
    _insert_image(db_path, image_id=141, observation_id=14, cloud_id="img-141")

    with recon._open_readonly(db_path) as conn:
        local = recon.load_local_state(conn)

    cloud_row = _cloud_row(
        id="img-141", desktop_id=141, observation_id="obs-14",
        storage_path="user-1/obs-14/originals/img-141/photo.webp",
    )
    match = recon.match_cloud_row_to_local(cloud_row, local)
    assert match.method == recon.MATCH_METHOD_CLOUD_ID
    assert match.cloud_id_link_broken is False


def test_desktop_id_match_skips_wrong_image_type(tmp_path: Path) -> None:
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    _insert_observation(db_path, local_id=15, cloud_id="obs-15")
    # local row is microscope but cloud row is field with same desktop_id.
    _insert_image(
        db_path,
        image_id=151,
        observation_id=15,
        cloud_id=None,
        image_type="microscope",
    )

    with recon._open_readonly(db_path) as conn:
        local = recon.load_local_state(conn)

    cloud_row = _cloud_row(id="img-151", desktop_id=151, observation_id="obs-15",
                          image_type="field", storage_path="k")
    match = recon.match_cloud_row_to_local(cloud_row, local)
    assert match.method is None


# ---------------------------------------------------------------------------
# Integration: end-to-end CLI runner with a fake read-only cloud client
# ---------------------------------------------------------------------------


class _RaisingClient:
    """Fake client that only allows read paths. Any mutating method raises."""

    def __init__(self, cloud_rows):
        self.user_id = "user-1"
        self._rows = list(cloud_rows)
        self.reads: list[str] = []

    def get_read_only(self, path: str):
        self.reads.append(path)
        return list(self._rows)

    # Every possible mutating method must raise if invoked.
    def _post(self, *_a, **_k):  # pragma: no cover - defensive
        raise AssertionError("mutating _post invoked on read-only client")

    def _patch(self, *_a, **_k):  # pragma: no cover - defensive
        raise AssertionError("mutating _patch invoked on read-only client")

    def _delete(self, *_a, **_k):  # pragma: no cover - defensive
        raise AssertionError("mutating _delete invoked on read-only client")

    def _rpc(self, *_a, **_k):  # pragma: no cover - defensive
        raise AssertionError("mutating _rpc invoked on read-only client")

    def upload_image_file(self, *_a, **_k):  # pragma: no cover
        raise AssertionError("upload_image_file invoked on read-only client")

    def upload_original_image_file(self, *_a, **_k):  # pragma: no cover
        raise AssertionError("upload_original_image_file invoked on read-only client")


def _db_mtime(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns


def test_integration_run_produces_report_and_never_writes(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    _insert_observation(db_path, local_id=100, cloud_id="obs-100")
    _insert_image(
        db_path, image_id=1001, observation_id=100, cloud_id="img-1001", synced=True
    )
    # local desires bytes for 1001, does not desire bytes for 1002.
    _insert_image(
        db_path, image_id=1002, observation_id=100, cloud_id="img-1002", synced=True
    )
    _set_setting(
        db_path,
        f"{recon.CLOUD_IMAGE_STORAGE_EXCLUDED_PREFIX}100",
        json.dumps([1002]),
    )

    cloud_rows = [
        _cloud_row(
            id="img-1001", desktop_id=1001, observation_id="obs-100",
            storage_path="user-1/obs-100/originals/img-1001/photo.webp",
        ),
        _cloud_row(
            id="img-1002", desktop_id=1002, observation_id="obs-100",
            storage_path="user-1/obs-100/originals/img-1002/photo.webp",
        ),
        # Also throw in an orphan for good measure.
        _cloud_row(
            id="img-lost", desktop_id=9999, observation_id="obs-lost",
            storage_path="user-1/obs-lost/originals/img-lost/photo.webp",
        ),
    ]

    client = _RaisingClient(cloud_rows)

    # Force the CLI to use our fake client and our tmp DB.
    monkeypatch.setattr(recon, "_load_client_or_die", lambda: client)
    baseline = _db_mtime(db_path)

    json_out = tmp_path / "report.json"
    rc = recon.run(["--db", str(db_path), "--json", str(json_out)])
    assert rc == 0

    captured = capsys.readouterr()
    # Text report includes each category header we produced.
    assert "healthy_uploaded" in captured.out
    assert "unwanted_cloud_bytes" in captured.out
    assert "cloud_only_orphan" in captured.out

    payload = json.loads(json_out.read_text(encoding="utf-8"))
    counts = payload["counts"]
    assert counts[recon.CATEGORY_HEALTHY_UPLOADED] == 1
    assert counts[recon.CATEGORY_UNWANTED_CLOUD_BYTES] == 1
    assert counts[recon.CATEGORY_CLOUD_ONLY_ORPHAN] == 1

    # The reads-only client saw exactly one read.
    assert len(client.reads) >= 1

    # SQLite DB file must be byte-for-byte untouched.
    assert _db_mtime(db_path) == baseline


def test_apply_flag_raises_not_implemented(tmp_path: Path) -> None:
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    # No DB access happens because parse_args + apply gate fires first.
    with pytest.raises(NotImplementedError):
        recon.run(["--db", str(db_path), "--apply"])


def test_soft_deleted_cloud_row_with_local_tombstone_is_healthy_lifecycle(
    tmp_path: Path,
) -> None:
    """A cloud row with deleted_at set + a synced local tombstone is
    normal DELETED lifecycle. It should be flagged with the healthy
    delete-lifecycle subcategory, not counted as a truly broken row."""
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    _insert_observation(db_path, local_id=301, cloud_id="obs-301")
    _insert_image(db_path, image_id=3011, observation_id=301, cloud_id="img-3011", synced=True)
    _insert_tombstone(
        db_path,
        deleted_cloud_id="img-3011",
        local_image_id=3011,
        delete_synced_at="2026-08-05T01:00:00Z",
    )

    with recon._open_readonly(db_path) as conn:
        local = recon.load_local_state(conn)

    cloud_row = _cloud_row(
        id="img-3011",
        desktop_id=3011,
        observation_id="obs-301",
        storage_path="user-1/obs-301/originals/img-3011/photo.webp",
        deleted_at="2026-08-05T00:00:00Z",
    )
    match = recon.match_cloud_row_to_local(cloud_row, local)
    row = recon.classify_cloud_row(cloud_row, match, local)

    assert row.category == recon.CATEGORY_BROKEN_ACTIVE
    assert row.subcategory == recon.SUBCATEGORY_G_HEALTHY_DELETE_LIFECYCLE
    assert row.local_tombstone_present is True


def test_soft_deleted_cloud_row_without_local_tombstone_is_remote_only_deletion(
    tmp_path: Path,
) -> None:
    """A cloud row with deleted_at set + NO matching local tombstone is a
    remote-only deletion — the desktop has not yet acknowledged it."""
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    _insert_observation(db_path, local_id=302, cloud_id="obs-302")
    _insert_image(db_path, image_id=3021, observation_id=302, cloud_id="img-3021", synced=True)

    with recon._open_readonly(db_path) as conn:
        local = recon.load_local_state(conn)

    cloud_row = _cloud_row(
        id="img-3021",
        desktop_id=3021,
        observation_id="obs-302",
        storage_path="user-1/obs-302/originals/img-3021/photo.webp",
        deleted_at="2026-08-05T00:00:00Z",
    )
    match = recon.match_cloud_row_to_local(cloud_row, local)
    row = recon.classify_cloud_row(cloud_row, match, local)

    assert row.category == recon.CATEGORY_BROKEN_ACTIVE
    assert row.subcategory == recon.SUBCATEGORY_G_REMOTE_ONLY_DELETION
    assert row.local_tombstone_present is False


def test_orphan_subcategory_reflects_bytes_state(tmp_path: Path) -> None:
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    with recon._open_readonly(db_path) as conn:
        local = recon.load_local_state(conn)

    with_bytes = _cloud_row(
        id="img-with", desktop_id=99, observation_id="obs-orphan",
        storage_path="user-1/obs-orphan/originals/img-with/photo.webp",
    )
    metadata_only = _cloud_row(
        id="img-meta", desktop_id=99, observation_id="obs-orphan",
        image_type="microscope", storage_path=None,
    )
    row_with = recon.classify_cloud_row(
        with_bytes, recon.match_cloud_row_to_local(with_bytes, local), local
    )
    row_meta = recon.classify_cloud_row(
        metadata_only, recon.match_cloud_row_to_local(metadata_only, local), local
    )
    assert row_with.subcategory == recon.SUBCATEGORY_F_WITH_BYTES
    assert row_meta.subcategory == recon.SUBCATEGORY_F_METADATA_ONLY


def test_desktop_id_match_scoped_to_local_observation(tmp_path: Path) -> None:
    """desktop_id fallback must not match a same-numbered id across a
    different observation (owner+observation scope, per contract)."""
    db_path = tmp_path / "sporely.db"
    _seed_schema(db_path)
    _insert_observation(db_path, local_id=201, cloud_id="obs-201")
    _insert_observation(db_path, local_id=202, cloud_id="obs-202")
    # Local id 33 belongs to observation 201.
    _insert_image(db_path, image_id=33, observation_id=201, cloud_id=None)

    with recon._open_readonly(db_path) as conn:
        local = recon.load_local_state(conn)

    # Cloud row claims desktop_id=33 but belongs to obs-202 — must not match.
    cloud_row = _cloud_row(
        id="img-33", desktop_id=33, observation_id="obs-202",
        storage_path="user-1/obs-202/originals/img-33/photo.webp",
    )
    match = recon.match_cloud_row_to_local(cloud_row, local)
    assert match.local_image is None
    assert match.method is None
