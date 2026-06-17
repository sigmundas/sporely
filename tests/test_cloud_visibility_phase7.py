import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from database import models, schema
from utils import cloud_sync


def _init_tombstone_sync_db(tmp_path):
    db_path = tmp_path / "sporely.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cloud_id TEXT,
            sync_status TEXT,
            synced_at TEXT,
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
            synced_at TEXT
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
    schema._ensure_image_tombstones_table(conn.cursor())
    conn.commit()
    conn.close()
    return db_path


def _init_push_all_sync_db(tmp_path):
    db_path = _init_tombstone_sync_db(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("ALTER TABLE observations ADD COLUMN date TEXT")
        conn.execute("ALTER TABLE observations ADD COLUMN user_id TEXT")
        conn.execute("ALTER TABLE observations ADD COLUMN sync_error_code TEXT")
        conn.execute("ALTER TABLE observations ADD COLUMN sync_error_message TEXT")
        conn.execute("ALTER TABLE observations ADD COLUMN sync_blocked_reason TEXT")
        conn.execute("ALTER TABLE observations ADD COLUMN sync_blocked_at TEXT")
        conn.execute("ALTER TABLE images ADD COLUMN source_role TEXT")
        conn.execute("ALTER TABLE images ADD COLUMN file_purpose TEXT")
        conn.commit()
    finally:
        conn.close()
    return db_path


class _PushAllImageClient(cloud_sync.SporelyCloudClient):
    def __init__(self, *, fail_derivative_upload: bool = False, fail_original_upload: bool = False):
        super().__init__("token", "user-123")
        self.fail_derivative_upload = bool(fail_derivative_upload)
        self.fail_original_upload = bool(fail_original_upload)
        self.upload_image_calls: list[dict] = []
        self.upload_original_calls: list[dict] = []
        self.push_metadata_calls: list[dict] = []
        self.original_patch_calls: list[dict] = []

    def push_observation(self, obs):
        return "cloud-obs-1"

    def pull_image_metadata(self, obs_cloud_id: str, include_deleted_for_sync: bool = False) -> list[dict]:
        return []

    def pull_measurements_for_images(self, image_cloud_ids: list[str]) -> list[dict]:
        return []

    def _observation_images_support_ai_crop(self) -> bool:
        return False

    def _observation_images_support_ai_crop_custom(self) -> bool:
        return False

    def _observation_images_support_upload_metadata(self) -> bool:
        return False

    def _observation_images_support_original_storage_path(self) -> bool:
        return True

    def upload_image_file(
        self,
        local_path: str,
        obs_cloud_id: str,
        img_cloud_id: str,
        storage_path: str | None = None,
        upload_meta: dict | None = None,
    ) -> str | None:
        self.upload_image_calls.append(
            {
                "local_path": str(local_path),
                "obs_cloud_id": str(obs_cloud_id),
                "img_cloud_id": str(img_cloud_id),
                "storage_path": str(storage_path or ""),
                "upload_meta": dict(upload_meta or {}),
            }
        )
        if self.fail_derivative_upload:
            raise cloud_sync.CloudSyncError(cloud_sync.IMAGE_TOO_LARGE_FOR_PLAN_MESSAGE)
        return storage_path or self._build_storage_path(obs_cloud_id, img_cloud_id, local_path)

    def push_image_metadata(self, img: dict, obs_cloud_id: str, storage_path: str) -> str:
        cloud_id = str(img.get("cloud_id") or "").strip() or f"cloud-image-{len(self.push_metadata_calls) + 1}"
        self.push_metadata_calls.append(
            {
                "cloud_id": cloud_id,
                "storage_path": cloud_sync.normalize_media_key(storage_path),
                "desktop_id": img.get("id"),
            }
        )
        return cloud_id

    def upload_original_image_file(
        self,
        local_path: str,
        obs_cloud_id: str,
        img_cloud_id: str,
        storage_path: str | None = None,
        upload_meta: dict | None = None,
    ) -> str | None:
        self.upload_original_calls.append(
            {
                "local_path": str(local_path),
                "obs_cloud_id": str(obs_cloud_id),
                "img_cloud_id": str(img_cloud_id),
                "storage_path": str(storage_path or ""),
                "upload_meta": dict(upload_meta or {}),
            }
        )
        if self.fail_original_upload:
            raise cloud_sync.CloudSyncError("Original media upload failed: worker refused")
        return storage_path or self._build_original_storage_path(obs_cloud_id, img_cloud_id, local_path)

    def set_image_original_storage_path(self, cloud_image_id: str, original_storage_path: str) -> None:
        self.original_patch_calls.append(
            {
                "cloud_image_id": str(cloud_image_id),
                "original_storage_path": cloud_sync.normalize_media_key(original_storage_path),
            }
        )


def test_phase7_visibility_normalization_maps_cloud_draft_to_local_private():
    assert cloud_sync._normalize_sharing_scope("draft") == "private"
    assert cloud_sync._cloud_visibility_to_sharing_scope("draft") == "private"
    assert cloud_sync._cloud_visibility_to_sharing_scope("friends") == "friends"
    assert cloud_sync._cloud_visibility_to_sharing_scope("public") == "public"


def test_phase7_visibility_normalization_sends_private_visibility_to_cloud():
    assert cloud_sync._sharing_scope_to_cloud_visibility("private") == "private"
    assert cloud_sync._sharing_scope_to_cloud_visibility("draft") == "private"
    assert cloud_sync._sharing_scope_to_cloud_visibility("friends") == "friends"
    assert cloud_sync._sharing_scope_to_cloud_visibility("public") == "public"


def test_push_observation_sends_private_visibility_for_local_private(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    posted_payloads = []

    monkeypatch.setattr(client, "_find_cloud_observation", lambda desktop_id: None)

    def fake_post(path, payload):
        posted_payloads.append((path, dict(payload)))
        return [{"id": "cloud-obs-1"}]

    monkeypatch.setattr(client, "_post", fake_post)

    cloud_id = client.push_observation(
        {
            "id": 7,
            "sharing_scope": "private",
            "location_public": 0,
            "is_draft": True,
            "location_precision": "fuzzed",
            "uncertain": 0,
            "unspontaneous": 0,
            "interesting_comment": 0,
        }
    )

    assert cloud_id == "cloud-obs-1"
    assert posted_payloads[0][0] == "observations"
    assert posted_payloads[0][1]["visibility"] == "private"
    assert posted_payloads[0][1]["is_draft"] is True
    assert posted_payloads[0][1]["location_precision"] == "fuzzed"
    assert posted_payloads[0][1]["user_id"] == "user-123"
    assert posted_payloads[0][1]["desktop_id"] == 7


def test_push_observation_preserves_public_visibility(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    patched_payloads = []

    monkeypatch.setattr(client, "_find_cloud_observation", lambda desktop_id: "cloud-obs-2")

    def fake_patch(path, payload):
        patched_payloads.append((path, dict(payload)))

    monkeypatch.setattr(client, "_patch", fake_patch)

    cloud_id = client.push_observation(
        {
            "id": 8,
            "sharing_scope": "public",
            "location_public": 1,
            "is_draft": False,
            "location_precision": "exact",
            "uncertain": 1,
            "unspontaneous": 0,
            "interesting_comment": 0,
        }
    )

    assert cloud_id == "cloud-obs-2"
    assert patched_payloads[0][0] == "observations?id=eq.cloud-obs-2"
    assert patched_payloads[0][1]["visibility"] == "public"
    assert patched_payloads[0][1]["location_public"] is True
    assert patched_payloads[0][1]["is_draft"] is False
    assert patched_payloads[0][1]["location_precision"] == "exact"


def test_set_image_desktop_id_scopes_patch_to_user(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    patched_payloads = []

    monkeypatch.setattr(client, "_patch", lambda path, payload: patched_payloads.append((path, dict(payload))))

    client.set_image_desktop_id("cloud-img-1", 77)

    assert patched_payloads == [
        ("observation_images?id=eq.cloud-img-1&user_id=eq.user-123", {"desktop_id": 77})
    ]


def test_soft_delete_image_scopes_patch_to_user(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    fetched_paths = []
    patched_payloads = []

    monkeypatch.setattr(
        client,
        "_get",
        lambda path: fetched_paths.append(path) or [{"id": "cloud-img-1", "deleted_at": None}],
    )
    monkeypatch.setattr(client, "_patch", lambda path, payload: patched_payloads.append((path, dict(payload))))

    client.soft_delete_image("cloud-img-1", "2026-05-01 10:00:00")

    assert fetched_paths == [
        "observation_images?id=eq.cloud-img-1&user_id=eq.user-123&select=id,deleted_at&limit=1"
    ]
    assert patched_payloads == [
        (
            "observation_images?id=eq.cloud-img-1&user_id=eq.user-123",
            {"deleted_at": "2026-05-01 10:00:00"},
        )
    ]


def test_pull_image_metadata_filters_deleted_rows_by_default(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    fetched_paths = []

    monkeypatch.setattr(
        client,
        "_get",
        lambda path: fetched_paths.append(path) or [
            {"id": "cloud-img-1", "deleted_at": None, "storage_path": "user/cloud-obs-1/cloud-img-1.jpg"},
            {
                "id": "cloud-img-2",
                "deleted_at": "2026-05-01 10:00:00",
                "storage_path": "user/cloud-obs-1/cloud-img-2.jpg",
            },
        ],
    )

    rows = client.pull_image_metadata("cloud-obs-1")

    assert fetched_paths == [
        "observation_images?observation_id=eq.cloud-obs-1&user_id=eq.user-123&deleted_at=is.null&select=*"
    ]
    assert [row["id"] for row in rows] == ["cloud-img-1"]
    assert rows[0]["deleted_at"] is None


def test_pull_image_metadata_include_deleted_for_sync_returns_deleted_rows(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    fetched_paths = []

    monkeypatch.setattr(
        client,
        "_get",
        lambda path: fetched_paths.append(path) or [
            {"id": "cloud-img-1", "deleted_at": None, "storage_path": "user/cloud-obs-1/cloud-img-1.jpg"},
            {
                "id": "cloud-img-2",
                "deleted_at": "2026-05-01 10:00:00",
                "storage_path": "user/cloud-obs-1/cloud-img-2.jpg",
            },
        ],
    )

    rows = client.pull_image_metadata("cloud-obs-1", include_deleted_for_sync=True)

    assert fetched_paths == [
        "observation_images?observation_id=eq.cloud-obs-1&user_id=eq.user-123&select=*"
    ]
    assert [row["id"] for row in rows] == ["cloud-img-1", "cloud-img-2"]
    assert rows[1]["deleted_at"] == "2026-05-01 10:00:00"


def test_pull_image_metadata_does_not_change_local_db_rows(monkeypatch, tmp_path):
    db_path = tmp_path / "local.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE images (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
        conn.execute("INSERT INTO images (name) VALUES (?)", ("kept",))
        conn.commit()
    finally:
        conn.close()

    client = cloud_sync.SporelyCloudClient("token", "user-123")
    monkeypatch.setattr(
        client,
        "_get",
        lambda path: [
            {"id": "cloud-img-1", "deleted_at": None, "storage_path": "user/cloud-obs-1/cloud-img-1.jpg"}
        ],
    )
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: pytest.fail("unexpected local DB access"))

    rows = client.pull_image_metadata("cloud-obs-1", include_deleted_for_sync=True)

    conn = sqlite3.connect(db_path)
    try:
        local_rows = conn.execute("SELECT id, name FROM images ORDER BY id").fetchall()
    finally:
        conn.close()

    assert rows == [
        {"id": "cloud-img-1", "deleted_at": None, "storage_path": "user/cloud-obs-1/cloud-img-1.jpg"}
    ]
    assert local_rows == [(1, "kept")]


def test_push_pending_image_tombstones_marks_delete_synced_at(monkeypatch, tmp_path):
    db_path = _init_tombstone_sync_db(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?)
            """,
            ("cloud-image-1", "2026-05-01 10:00:00", 1, 11),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    calls = []

    class DummyClient:
        def soft_delete_image(self, cloud_image_id, deleted_at):
            calls.append((cloud_image_id, deleted_at))

    warnings = cloud_sync._push_pending_image_tombstones(DummyClient())

    conn = sqlite3.connect(db_path)
    try:
        delete_synced_at = conn.execute(
            "SELECT delete_synced_at FROM image_tombstones WHERE deleted_cloud_id = ?",
            ("cloud-image-1",),
        ).fetchone()[0]
    finally:
        conn.close()

    assert warnings == []
    assert calls == [("cloud-image-1", "2026-05-01 10:00:00")]
    assert delete_synced_at is not None


def test_push_pending_image_tombstones_leaves_delete_synced_at_null_on_failure(monkeypatch, tmp_path):
    db_path = _init_tombstone_sync_db(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?)
            """,
            ("cloud-image-1", "2026-05-01 10:00:00", 1, 11),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    class DummyClient:
        def soft_delete_image(self, cloud_image_id, deleted_at):
            raise cloud_sync.CloudSyncError("boom")

    warnings = cloud_sync._push_pending_image_tombstones(DummyClient())

    conn = sqlite3.connect(db_path)
    try:
        delete_synced_at = conn.execute(
            "SELECT delete_synced_at FROM image_tombstones WHERE deleted_cloud_id = ?",
            ("cloud-image-1",),
        ).fetchone()[0]
    finally:
        conn.close()

    assert warnings and "could not sync cloud image tombstone" in warnings[0]
    assert delete_synced_at is None


def test_push_pending_image_tombstones_runs_before_active_image_push(monkeypatch, tmp_path):
    db_path = _init_tombstone_sync_db(tmp_path)
    images_root = tmp_path / "images"
    images_root.mkdir()
    image_path = images_root / "image.jpg"
    image_path.write_bytes(b"image-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, image_type, sort_order, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (11, 1, None, str(image_path), "field", 0, "2026-05-01T10:00:00Z"),
        )
        conn.commit()
    finally:
        conn.close()

    order = []
    client = cloud_sync.SporelyCloudClient("token", "user-123")

    monkeypatch.setattr(
        cloud_sync,
        "_push_pending_image_tombstones",
        lambda _client: order.append("tombstones") or [],
    )
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(client, "pull_image_metadata", lambda obs_cloud_id: order.append("existing_rows") or [])
    monkeypatch.setattr(client, "push_image_metadata", lambda img, obs_cloud_id, storage_path: order.append("push_metadata") or "cloud-image-1")
    monkeypatch.setattr(client, "upload_image_file", lambda local_path, obs_cloud_id, img_cloud_id, storage_path=None, upload_meta=None: order.append("upload_file") or storage_path)
    monkeypatch.setattr(client, "set_image_desktop_id", lambda cloud_image_id, desktop_id: order.append("set_desktop_id"))
    monkeypatch.setattr(client, "_patch", lambda *args, **kwargs: order.append("patch_storage"))
    monkeypatch.setattr(client, "_observation_images_support_ai_crop", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_upload_metadata", lambda: False)

    result = cloud_sync._push_images_for_observation(client, {"id": 1}, "cloud-obs-1")

    assert result is True
    assert order.index("tombstones") < order.index("existing_rows") < order.index("upload_file") < order.index("push_metadata")


def test_push_images_for_observation_leaves_image_pending_when_worker_upload_fails(monkeypatch, tmp_path):
    db_path = _init_tombstone_sync_db(tmp_path)
    images_root = tmp_path / "images"
    images_root.mkdir()
    image_path = images_root / "image.jpg"
    image_path.write_bytes(b"image-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, image_type, sort_order, created_at, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (11, 1, None, str(image_path), "field", 0, "2026-05-01T10:00:00Z", None),
        )
        conn.commit()
    finally:
        conn.close()

    order = []
    client = cloud_sync.SporelyCloudClient("token", "user-123")

    monkeypatch.setattr(
        cloud_sync,
        "_push_pending_image_tombstones",
        lambda _client: order.append("tombstones") or [],
    )
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(client, "pull_image_metadata", lambda obs_cloud_id: order.append("existing_rows") or [])
    monkeypatch.setattr(
        client,
        "upload_image_file",
        lambda *args, **kwargs: order.append("upload_file") or (_ for _ in ()).throw(cloud_sync.CloudSyncError("Worker upload failed")),
    )
    monkeypatch.setattr(client, "push_image_metadata", lambda *args, **kwargs: order.append("push_metadata") or "cloud-image-1")
    monkeypatch.setattr(client, "set_image_desktop_id", lambda *args, **kwargs: order.append("set_desktop_id"))
    monkeypatch.setattr(client, "_observation_images_support_ai_crop", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_upload_metadata", lambda: False)

    result = cloud_sync._push_images_for_observation(client, {"id": 1}, "cloud-obs-1")

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT cloud_id, synced_at FROM images WHERE id = ?",
            (11,),
        ).fetchone()
    finally:
        conn.close()

    assert result is False
    assert "push_metadata" not in order
    assert row == (None, None)


def test_apply_remote_observation_fields_pulls_interesting_comment(monkeypatch):
    captured = {}

    def fake_update_observation(observation_id, **kwargs):
        captured["observation_id"] = observation_id
        captured["kwargs"] = dict(kwargs)

    monkeypatch.setattr(cloud_sync.ObservationDB, "update_observation", fake_update_observation)

    cloud_sync._apply_remote_observation_fields(
        42,
        {"interesting_comment": True},
        fields={"interesting_comment"},
    )

    assert captured == {
        "observation_id": 42,
        "kwargs": {
            "interesting_comment": True,
            "allow_nulls": True,
        },
    }


@pytest.mark.parametrize(
    ("remote_value", "expected"),
    [
        (True, True),
        (None, False),
    ],
)
def test_create_local_from_remote_preserves_interesting_comment(
    monkeypatch,
    tmp_path,
    remote_value,
    expected,
):
    db_path = tmp_path / "sporely.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY,
            cloud_id TEXT,
            sync_status TEXT,
            synced_at TEXT
        )
        """
    )
    conn.commit()
    conn.close()

    captured = {}

    def fake_create_observation(**kwargs):
        captured.update(kwargs)
        return 1

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync.ObservationDB, "create_observation", fake_create_observation)
    monkeypatch.setattr(cloud_sync, "_import_remote_images", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "from_stored_credentials", staticmethod(lambda: None))
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *args, **kwargs: None)

    remote = {
        "id": "cloud-obs-1",
        "date": "2026-05-01",
    }
    if remote_value is not None:
        remote["interesting_comment"] = remote_value

    local_id = cloud_sync._create_local_from_remote(remote, remote_images=[])

    assert local_id == 1
    assert captured["interesting_comment"] is expected


def test_mark_observation_dirty_clears_blocked_sync_state(tmp_path, monkeypatch):
    db_path = tmp_path / "cloud_dirty.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY,
            cloud_id TEXT,
            sync_status TEXT,
            synced_at TEXT,
            sync_error_code TEXT,
            sync_error_message TEXT,
            sync_blocked_reason TEXT,
            sync_blocked_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO observations (
            id, cloud_id, sync_status, synced_at,
            sync_error_code, sync_error_message, sync_blocked_reason, sync_blocked_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1,
            "cloud-obs-1",
            "blocked",
            "2026-01-01T00:00:00Z",
            "23514",
            'POST observations: {"code":"23514","message":"Free Sporely accounts can keep up to 20 privacy slot observations. Publish or use exact public location to continue."}',
            cloud_sync.privacy_slot_limit_user_message(),
            "2026-01-01T00:00:01Z",
        ),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))

    cloud_sync.mark_observation_dirty(1)

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT
                cloud_id, sync_status, synced_at,
                sync_error_code, sync_error_message,
                sync_blocked_reason, sync_blocked_at
            FROM observations
            WHERE id = ?
            """,
            (1,),
        ).fetchone()
    finally:
        conn.close()

    assert row == (
        "cloud-obs-1",
        "dirty",
        "2026-01-01T00:00:00Z",
        None,
        None,
        None,
        None,
    )


def test_privacy_slot_limit_error_classifier_matches_supabase_payload():
    assert cloud_sync.is_privacy_slot_limit_error(
        {
            "code": "23514",
            "message": "Free Sporely accounts can keep up to 20 privacy slot observations. Publish or use exact public location to continue.",
        }
    )
    assert cloud_sync.is_privacy_slot_limit_error(
        'POST observations: {"code":"23514","message":"Free Sporely accounts can keep up to 20 privacy slot observations. Publish or use exact public location to continue."}'
    )
    assert not cloud_sync.is_privacy_slot_limit_error(
        {
            "code": "23514",
            "message": "Some other check constraint failed.",
        }
    )


def test_fetch_cloud_usage_summary_counts_private_and_fuzzed_slots():
    client = SimpleNamespace(
        fetch_cloud_plan_profile=lambda: {"cloud_plan": "free", "is_pro": False},
        list_remote_observations=lambda: [
            {"visibility": "private", "location_precision": "exact"},
            {"visibility": "public", "location_precision": "fuzzed"},
            {"visibility": "public", "location_precision": "exact"},
        ],
    )

    summary = cloud_sync.fetch_cloud_usage_summary(client)

    assert summary["cloud_usage_loaded"] is True
    assert summary["privacy_slots_used"] == 2
    assert summary["privacy_slots_available"] == 18
    assert summary["cloud_plan"] == "free"


def test_push_all_blocks_privacy_slot_limit_and_continues_to_next_row(tmp_path, monkeypatch):
    db_path = tmp_path / "push_all_blocked.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY,
            date TEXT,
            cloud_id TEXT,
            sync_status TEXT,
            synced_at TEXT,
            user_id TEXT,
            visibility TEXT,
            location_precision TEXT,
            sync_error_code TEXT,
            sync_error_message TEXT,
            sync_blocked_reason TEXT,
            sync_blocked_at TEXT
        );
        INSERT INTO observations (
            id, date, cloud_id, sync_status, synced_at,
            user_id, visibility, location_precision
        ) VALUES
            (1, '2026-05-02', NULL, 'dirty', NULL, 'user-1', 'private', 'fuzzed'),
            (2, '2026-05-01', NULL, 'dirty', NULL, 'user-1', 'public', 'exact');
        """
    )
    conn.commit()
    conn.close()

    class DummyClient:
        def push_observation(self, obs):
            if int(obs.get("id") or 0) == 1:
                raise cloud_sync.CloudSyncError(
                    'POST observations: {"code":"23514","message":"Free Sporely accounts can keep up to 20 privacy slot observations. Publish or use exact public location to continue."}'
                )
            return "cloud-obs-2"

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)

    result = cloud_sync.push_all(
        DummyClient(),
        sync_images=False,
        sync_calibrations=False,
    )

    summary = cloud_sync.summarize_sync_issues(result["errors"])
    conn = sqlite3.connect(db_path)
    try:
        blocked_row = conn.execute(
            """
            SELECT
                cloud_id, sync_status, sync_error_code, sync_error_message,
                sync_blocked_reason, sync_blocked_at
            FROM observations
            WHERE id = 1
            """
        ).fetchone()
        synced_row = conn.execute(
            """
            SELECT cloud_id, sync_status
            FROM observations
            WHERE id = 2
            """
        ).fetchone()
    finally:
        conn.close()

    assert result["pushed"] == 1
    assert summary["blocked_count"] == 1
    assert summary["other_count"] == 0
    assert blocked_row[0] is None
    assert blocked_row[1] == "blocked"
    assert blocked_row[2] == "privacy_slot_limit"
    assert "privacy slot observations" in blocked_row[3]
    assert blocked_row[4] == cloud_sync.privacy_slot_limit_user_message()
    assert blocked_row[5] is not None
    assert synced_row == ("cloud-obs-2", "synced")


def test_push_all_marks_image_too_large_as_retryable_and_keeps_next_row_moving(tmp_path, monkeypatch):
    db_path = tmp_path / "push_all_retryable.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY,
            date TEXT,
            cloud_id TEXT,
            sync_status TEXT,
            synced_at TEXT,
            user_id TEXT,
            visibility TEXT,
            location_precision TEXT,
            sync_error_code TEXT,
            sync_error_message TEXT,
            sync_blocked_reason TEXT,
            sync_blocked_at TEXT
        );
        INSERT INTO observations (
            id, date, cloud_id, sync_status, synced_at,
            user_id, visibility, location_precision
        ) VALUES
            (1, '2026-05-02', NULL, 'dirty', NULL, 'user-1', 'public', 'exact'),
            (2, '2026-05-01', NULL, 'dirty', NULL, 'user-1', 'public', 'exact');
        """
    )
    conn.commit()
    conn.close()

    class DummyClient:
        def push_observation(self, obs):
            if int(obs.get("id") or 0) == 1:
                raise cloud_sync.CloudSyncError(
                    'Image is too large for your plan. Make it smaller or upgrade to Pro.'
                )
            return "cloud-obs-2"

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)

    result = cloud_sync.push_all(
        DummyClient(),
        sync_images=False,
        sync_calibrations=False,
    )

    summary = cloud_sync.summarize_sync_issues(result["errors"])
    conn = sqlite3.connect(db_path)
    try:
        retryable_row = conn.execute(
            """
            SELECT
                cloud_id, sync_status, sync_error_code, sync_error_message,
                sync_blocked_reason, sync_blocked_at
            FROM observations
            WHERE id = 1
            """
        ).fetchone()
        synced_row = conn.execute(
            """
            SELECT cloud_id, sync_status
            FROM observations
            WHERE id = 2
            """
        ).fetchone()
    finally:
        conn.close()

    assert result["pushed"] == 1
    assert summary["retryable_count"] == 1
    assert summary["blocked_count"] == 0
    assert summary["other_count"] == 0
    assert retryable_row[0] is None
    assert retryable_row[1] == "dirty"
    assert retryable_row[2] == "image_too_large_for_plan"
    assert "Image is too large for your plan" in retryable_row[3]
    assert retryable_row[4] is None
    assert retryable_row[5] is None
    assert synced_row == ("cloud-obs-2", "synced")


def test_push_all_marks_image_upload_plan_limit_retryable_and_keeps_observation_dirty(
    tmp_path,
    monkeypatch,
):
    db_path = _init_push_all_sync_db(tmp_path)
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"image-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO observations (
                id, cloud_id, sync_status, synced_at, sync_error_code,
                sync_error_message, sync_blocked_reason, sync_blocked_at, user_id, date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, None, "dirty", None, None, None, None, None, "user-1", "2026-05-02"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath,
                image_type, source_role, file_purpose, sort_order, created_at, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                11,
                1,
                None,
                str(image_path),
                None,
                "field",
                "local_canonical",
                "field",
                0,
                "2026-05-01T10:00:00Z",
                None,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    client = _PushAllImageClient(fail_derivative_upload=True)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_load_cloud_image_file_signature", lambda *args, **kwargs: "")
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *args, **kwargs: "")
    monkeypatch.setattr(cloud_sync, "_store_local_cloud_media_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_push_measurements_for_observation", lambda *args, **kwargs: None)

    result = cloud_sync.push_all(
        client,
        sync_images=True,
        sync_calibrations=False,
    )

    summary = cloud_sync.summarize_sync_issues(result["errors"])
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT cloud_id, sync_status, sync_error_code, sync_error_message,
                   sync_blocked_reason, sync_blocked_at
            FROM observations
            WHERE id = 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert result["pushed"] == 0
    assert summary["retryable_count"] == 1
    assert summary["blocked_count"] == 0
    assert summary["other_count"] == 0
    assert row[0] == "cloud-obs-1"
    assert row[1] == "dirty"
    assert row[2] == "image_too_large_for_plan"
    assert cloud_sync.IMAGE_TOO_LARGE_FOR_PLAN_MESSAGE in row[3]
    assert row[4] is None
    assert row[5] is None
    assert len(client.upload_image_calls) == 1
    assert client.upload_original_calls == []


def test_push_all_keeps_optional_original_upload_failure_out_of_top_level_errors_and_dirty_state(
    tmp_path,
    monkeypatch,
):
    db_path = _init_push_all_sync_db(tmp_path)
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"image-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO observations (
                id, cloud_id, sync_status, synced_at, sync_error_code,
                sync_error_message, sync_blocked_reason, sync_blocked_at, user_id, date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, None, "dirty", None, None, None, None, None, "user-1", "2026-05-02"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath,
                image_type, source_role, file_purpose, sort_order, created_at, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                11,
                1,
                None,
                str(image_path),
                None,
                "field",
                "local_canonical",
                "field",
                0,
                "2026-05-01T10:00:00Z",
                None,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    client = _PushAllImageClient(fail_original_upload=True)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "push_calibrations", lambda *args, **kwargs: {"pushed": 0, "total": 0, "errors": []})
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_load_cloud_image_file_signature", lambda *args, **kwargs: "")
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *args, **kwargs: "")
    monkeypatch.setattr(cloud_sync, "_store_local_cloud_media_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "is_full_resolution_original_sync_enabled", lambda: True)
    monkeypatch.setattr(cloud_sync, "_push_measurements_for_observation", lambda *args, **kwargs: None)

    result = cloud_sync.push_all(
        client,
        sync_images=True,
        sync_calibrations=False,
    )

    summary = cloud_sync.summarize_sync_issues(result["errors"])
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT cloud_id, sync_status, sync_error_code, sync_error_message,
                   sync_blocked_reason, sync_blocked_at
            FROM observations
            WHERE id = 1
            """
        ).fetchone()
        image_row = conn.execute(
            """
            SELECT cloud_id, synced_at
            FROM images
            WHERE id = 11
            """
        ).fetchone()
    finally:
        conn.close()

    assert result["pushed"] == 1
    assert result["errors"] == []
    assert summary["retryable_count"] == 0
    assert summary["blocked_count"] == 0
    assert summary["other_count"] == 0
    assert result["original_sync"]["failed_uploads"] == 1
    assert row[0] == "cloud-obs-1"
    assert row[1] == "synced"
    assert row[2] is None
    assert row[3] is None
    assert row[4] is None
    assert row[5] is None
    assert image_row[0] is not None
    assert image_row[1] is not None
    assert len(client.upload_image_calls) == 1
    assert len(client.upload_original_calls) == 1


def test_push_all_announces_cloud_media_check_before_skip_message(tmp_path, monkeypatch):
    db_path = _init_push_all_sync_db(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO observations (
                id, cloud_id, sync_status, synced_at, sync_error_code,
                sync_error_message, sync_blocked_reason, sync_blocked_at, user_id, date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "cloud-obs-1",
                "dirty",
                "2026-05-01T00:00:00Z",
                None,
                None,
                None,
                None,
                "user-1",
                "2026-05-02",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    remote_current = {
        "id": "cloud-obs-1",
        "desktop_id": 1,
        "date": "2026-05-02",
        "genus": "Agaricus",
        "species": "campestris",
        "species_guess": "Agaricus campestris",
        "notes": "baseline note",
        "sharing_scope": "public",
        "location_public": True,
        "location_precision": "exact",
        "spore_data_visibility": "public",
        "is_draft": False,
    }
    stored_snapshot = cloud_sync._cloud_observation_snapshot(remote_current, [], [])
    progress_messages: list[str] = []

    class DummyClient:
        def push_observation(self, obs):
            return "cloud-obs-1"

        def pull_image_metadata(self, obs_cloud_id, include_deleted_for_sync=False):
            return []

        def pull_measurements_for_images(self, image_cloud_ids):
            return []

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_media_changes", lambda: None)
    monkeypatch.setattr(cloud_sync, "_mark_cloud_observations_dirty_for_pending_local_images", lambda: None)
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: stored_snapshot)
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *args, **kwargs: [])
    monkeypatch.setattr(cloud_sync, "_push_measurements_for_observation", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda local_id: "sig")
    monkeypatch.setattr(cloud_sync, "_local_cloud_media_signature", lambda local_id: "sig")
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *args, **kwargs: "sig")
    monkeypatch.setattr(cloud_sync, "_store_local_cloud_media_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)

    result = cloud_sync.push_all(
        DummyClient(),
        progress_cb=lambda text, current, total: progress_messages.append(text),
        remote_obs=[dict(remote_current)],
        sync_images=True,
        sync_calibrations=False,
    )

    check_index = next(
        i for i, message in enumerate(progress_messages)
        if message.startswith("Checking cloud media for observation 1/1:")
    )
    skip_index = next(
        i for i, message in enumerate(progress_messages)
        if message.startswith("Skipping unchanged cloud media for observation 1/1:")
    )

    assert result["pushed"] == 1
    assert check_index < skip_index


def test_get_conflict_detail_ignores_file_size_differences(monkeypatch):
    local_obs = {
        "id": 1,
        "cloud_id": "cloud-obs-1",
        "date": "2026-01-01",
        "genus": "Agaricus",
        "species": "campestris",
        "location": "Field edge",
        "sharing_scope": "private",
        "location_public": 0,
        "is_draft": True,
        "location_precision": "exact",
    }
    remote_obs = dict(local_obs)
    baseline_snapshot = {
        "observation": dict(remote_obs),
        "images": [
            {
                "desktop_id": 10,
                "sort_order": 0,
                "image_type": "field",
                "original_filename": "same.jpg",
                "stored_bytes": 1024,
            }
        ],
    }
    local_images = [
        {
            "id": 10,
            "cloud_id": "cloud-img-1",
            "sort_order": 0,
            "image_type": "field",
            "original_filename": "same.jpg",
            "stored_bytes": 2048,
            "filepath": "/tmp/same.jpg",
        }
    ]
    remote_images = [
        {
            "id": "cloud-img-1",
            "desktop_id": 10,
            "sort_order": 0,
            "image_type": "field",
            "original_filename": "same.jpg",
            "stored_bytes": 1024,
        }
    ]

    monkeypatch.setattr(cloud_sync.ObservationDB, "get_observation", lambda local_id: dict(local_obs))
    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation", lambda local_id: [dict(row) for row in local_images])
    monkeypatch.setattr(cloud_sync.MeasurementDB, "get_measurements_for_observation", lambda local_id: [])
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: json.dumps(baseline_snapshot))

    class DummyClient:
        def get_observation(self, cloud_id):
            return dict(remote_obs)

        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            return [dict(row) for row in remote_images]

    detail = cloud_sync.get_conflict_detail(DummyClient(), 1, "cloud-obs-1")

    assert detail["image_mismatches"] == []


def test_summarize_image_changes_ignores_upload_size_metadata_and_matches_cloud_id():
    baseline = [
        {
            "id": "cloud-img-1",
            "desktop_id": 10,
            "sort_order": 0,
            "image_type": "field",
            "original_filename": "same.jpg",
            "measure_color": "blue",
            "upload_mode": "full",
            "source_width": 4032,
            "source_height": 3024,
            "stored_width": 4032,
            "stored_height": 3024,
            "stored_bytes": 123456,
        }
    ]
    current = [
        {
            "id": "cloud-img-1",
            "desktop_id": 11,
            "sort_order": 0,
            "image_type": "field",
            "original_filename": "same.jpg",
            "measure_color": "blue",
            "upload_mode": "reduced",
            "source_width": 2000,
            "source_height": 1500,
            "stored_width": 2000,
            "stored_height": 1500,
            "stored_bytes": 45678,
        }
    ]

    assert cloud_sync._summarize_image_changes(current, baseline) == []


def test_import_remote_images_preserves_original_filename(monkeypatch, tmp_path):
    temp_root = tmp_path / "sync"
    temp_root.mkdir()
    captured = {}

    class DummyClient:
        def download_image_file(self, storage_path, dest_path):
            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"fake image")

    class DummyConn:
        def execute(self, *args, **kwargs):
            return self

        def commit(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "from_stored_credentials", lambda: DummyClient())
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_rename_to_detected_image_extension", lambda path: Path(path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: DummyConn())
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(":memory:"))
    monkeypatch.setattr(cloud_sync.tempfile, "mkdtemp", lambda prefix=None: str(temp_root))
    monkeypatch.setattr(
        cloud_sync.ImageDB,
        "add_image",
        lambda **kwargs: (captured.setdefault("filepath", kwargs["filepath"]) or 1) and 1,
    )

    cloud_sync._import_remote_images(
        {"id": "cloud-obs-1", "genus": "Flammulina", "species": "velutipes"},
        1,
        "cloud-obs-1",
        remote_images=[
            {
                "id": "cloud-img-1",
                "storage_path": "user/cloud-obs-1/cloud-img-1_cloud_1.jpg",
                "original_filename": "cloud_1.jpg",
                "image_type": "field",
            }
        ],
    )

    assert Path(captured["filepath"]).name == "cloud_1.jpg"


def test_import_remote_images_preserves_metadata_and_sets_desktop_id(monkeypatch, tmp_path):
    temp_root = tmp_path / "sync-meta"
    temp_root.mkdir()
    captured = {}
    desktop_id_calls = []
    conn_statements = []

    class DummyClient:
        def download_image_file(self, storage_path, dest_path):
            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"cloud image bytes")

        def set_image_desktop_id(self, cloud_image_id, desktop_id):
            desktop_id_calls.append((cloud_image_id, desktop_id))

    class DummyConn:
        def execute(self, sql, params=()):
            conn_statements.append((sql, tuple(params)))
            return self

        def commit(self):
            pass

        def close(self):
            pass

    def fake_add_image(**kwargs):
        captured.update(kwargs)
        return 23

    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "from_stored_credentials", lambda: DummyClient())
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_rename_to_detected_image_extension", lambda path: Path(path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: DummyConn())
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(":memory:"))
    monkeypatch.setattr(cloud_sync.tempfile, "mkdtemp", lambda prefix=None: str(temp_root))
    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", fake_add_image)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)

    cloud_sync._import_remote_images(
        {"id": "cloud-obs-1", "genus": "Flammulina", "species": "velutipes"},
        1,
        "cloud-obs-1",
        remote_images=[
            {
                "id": "cloud-img-1",
                "storage_path": "user/cloud-obs-1/cloud-img-1_cloud_1.jpg",
                "original_filename": "cloud_1.jpg",
                "image_type": "field",
                "sort_order": 4,
                "micro_category": "Spores",
                "objective_name": "100X Plan achro",
                "scale_microns_per_pixel": 0.0315,
                "resample_scale_factor": 2.0,
                "mount_medium": "Water",
                "stain": "Congo Red",
                "sample_type": "spores",
                "contrast": "BF",
                "measure_color": "blue",
                "crop_mode": "manual",
                "notes": "cloud note",
                "gps_source": True,
                "ai_crop_x1": 1.0,
                "ai_crop_y1": 2.0,
                "ai_crop_x2": 3.0,
                "ai_crop_y2": 4.0,
                "ai_crop_source_w": 640,
                "ai_crop_source_h": 480,
                "ai_crop_is_custom": True,
                "captured_at": "2026-05-01T12:34:56Z",
            }
        ],
    )

    assert captured["scale"] == 0.0315
    assert captured["notes"] == "cloud note"
    assert captured["micro_category"] == "Spores"
    assert captured["objective_name"] == "100X Plan achro"
    assert captured["measure_color"] == "blue"
    assert captured["mount_medium"] == "Water"
    assert captured["stain"] == "Congo Red"
    assert captured["sample_type"] == "spores"
    assert captured["contrast"] == "BF"
    assert captured["sort_order"] == 4
    assert captured["crop_mode"] == "manual"
    assert captured["gps_source"] is True
    assert captured["resample_scale_factor"] == 2.0
    assert captured["ai_crop_box"] == (1.0, 2.0, 3.0, 4.0)
    assert captured["ai_crop_source_size"] == (640, 480)
    assert captured["ai_crop_is_custom"] is True
    assert captured["captured_at"] == "2026-05-01T12:34:56Z"
    assert captured["copy_to_folder"] is True
    assert captured["mark_observation_dirty"] is False
    assert captured["source_role"] == "cloud_recovery_cache"
    assert captured["file_purpose"] == "cache"
    assert captured["original_mime_type"] is None
    assert captured["working_mime_type"] == "image/jpeg"
    assert desktop_id_calls == [("cloud-img-1", 23)]
    assert any(
        sql == "UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?"
        and params[0] == "cloud-img-1"
        and params[2] == 23
        for sql, params in conn_statements
    )


def test_import_remote_images_leaves_working_mime_type_null_for_unknown_extension(monkeypatch, tmp_path):
    temp_root = tmp_path / "sync-unknown-mime"
    temp_root.mkdir()
    captured = {}
    desktop_id_calls = []

    class DummyClient:
        def download_image_file(self, storage_path, dest_path):
            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"cloud image bytes")

        def set_image_desktop_id(self, cloud_image_id, desktop_id):
            desktop_id_calls.append((cloud_image_id, desktop_id))

    class DummyConn:
        def execute(self, *args, **kwargs):
            return self

        def commit(self):
            pass

        def close(self):
            pass

    def fake_add_image(**kwargs):
        captured.update(kwargs)
        return 24

    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "from_stored_credentials", lambda: DummyClient())
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_rename_to_detected_image_extension", lambda path: Path(path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: DummyConn())
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(":memory:"))
    monkeypatch.setattr(cloud_sync.tempfile, "mkdtemp", lambda prefix=None: str(temp_root))
    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", fake_add_image)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)

    cloud_sync._import_remote_images(
        {"id": "cloud-obs-1", "genus": "Flammulina", "species": "velutipes"},
        1,
        "cloud-obs-1",
        remote_images=[
            {
                "id": "cloud-img-2",
                "storage_path": "user/cloud-obs-1/cloud-img-2_cloud_1.unknown",
                "original_filename": "cloud_1.unknown",
                "image_type": "field",
            }
        ],
    )

    assert captured["source_role"] == "cloud_recovery_cache"
    assert captured["file_purpose"] == "cache"
    assert captured["original_mime_type"] is None
    assert captured["working_mime_type"] is None
    assert desktop_id_calls == [("cloud-img-2", 24)]


def test_pull_all_existing_observation_records_remote_deleted_rows(monkeypatch, tmp_path):
    db_path = _init_tombstone_sync_db(tmp_path)
    temp_root = tmp_path / "pull-all"
    temp_root.mkdir()

    existing_image_path = tmp_path / "images" / "observation-1" / "existing.jpg"
    existing_image_path.parent.mkdir(parents=True, exist_ok=True)
    existing_image_path.write_text("existing", encoding="utf-8")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath, image_type
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (11, 1, None, str(existing_image_path), None, "field"),
        )
        conn.commit()
    finally:
        conn.close()

    fetched_calls: list[tuple[str, bool]] = []
    download_calls: list[str] = []
    add_calls: list[dict] = []

    class DummyClient:
        def pull_bulk_image_metadata(self, obs_cloud_ids):
            return []

        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            fetched_calls.append((cloud_id, include_deleted_for_sync))
            return [
                {
                    "id": "cloud-image-deleted",
                    "observation_id": "cloud-obs-1",
                    "deleted_at": "2026-05-01 10:00:00",
                    "storage_path": "user/cloud-obs-1/cloud-image-deleted.jpg",
                    "original_filename": "deleted.jpg",
                    "image_type": "field",
                    "sort_order": 0,
                },
                {
                    "id": "cloud-image-active",
                    "observation_id": "cloud-obs-1",
                    "deleted_at": None,
                    "storage_path": "user/cloud-obs-1/cloud-image-active.jpg",
                    "original_filename": "active.jpg",
                    "image_type": "field",
                    "sort_order": 1,
                },
            ]

        def set_desktop_id(self, *args, **kwargs):
            pass

        def download_image_file(self, storage_path, dest_path):
            download_calls.append(storage_path)
            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"cloud image bytes")

    def fake_add_image(**kwargs):
        add_calls.append(dict(kwargs))
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.execute(
                """
                INSERT INTO images (observation_id, filepath, image_type, sort_order)
                VALUES (?, ?, ?, ?)
                """,
                (
                    kwargs["observation_id"],
                    kwargs["filepath"],
                    kwargs["image_type"],
                    kwargs.get("sort_order"),
                ),
            )
            local_id = cursor.lastrowid
            conn.commit()
            return local_id
        finally:
            conn.close()

    monkeypatch.setattr(cloud_sync, "_backfill_missing_exif_on_cloud_images", lambda: None)
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: "")
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda observation_id: "")
    monkeypatch.setattr(cloud_sync, "_local_cloud_media_signature", lambda observation_id: "")
    monkeypatch.setattr(cloud_sync, "_store_local_media_signature_if_equivalent", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *args, **kwargs: [])
    monkeypatch.setattr(cloud_sync, "_detect_deleted_remote_observations", lambda remote_obs: [])
    monkeypatch.setattr(cloud_sync, "_apply_remote_observation_fields", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_rename_to_detected_image_extension", lambda path: Path(path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync.tempfile, "mkdtemp", lambda prefix=None: str(temp_root))
    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", fake_add_image)

    result = cloud_sync.pull_all(
        DummyClient(),
        remote_obs=[
            {
                "id": "cloud-obs-1",
                "desktop_id": 1,
                "date": "2026-05-01",
                "genus": "Flammulina",
                "species": "velutipes",
            }
        ],
        sync_calibrations=False,
    )

    conn = sqlite3.connect(db_path)
    try:
        tombstone = conn.execute(
            """
            SELECT deleted_cloud_id, deleted_at, deleted_storage_path,
                   deleted_observation_cloud_id, local_observation_id
            FROM image_tombstones
            WHERE deleted_cloud_id = ?
            """,
            ("cloud-image-deleted",),
        ).fetchone()
        images = conn.execute(
            "SELECT id, cloud_id, filepath FROM images ORDER BY id"
        ).fetchall()
        observation = conn.execute(
            "SELECT cloud_id, sync_status FROM observations WHERE id = ?",
            (1,),
        ).fetchone()
    finally:
        conn.close()

    assert result["pulled"] == 1
    assert fetched_calls == [("cloud-obs-1", True)]
    assert download_calls == ["user/cloud-obs-1/cloud-image-active.jpg"]
    assert len(add_calls) == 1
    assert add_calls[0]["source_role"] == "cloud_recovery_cache"
    assert add_calls[0]["file_purpose"] == "cache"
    assert add_calls[0]["original_mime_type"] is None
    assert add_calls[0]["working_mime_type"] == "image/jpeg"
    assert len(images) == 2
    assert tombstone == (
        "cloud-image-deleted",
        "2026-05-01 10:00:00",
        "user/cloud-obs-1/cloud-image-deleted.jpg",
        "cloud-obs-1",
        1,
    )
    assert images[0][0] == 11
    assert images[0][1] is None
    assert images[1][1] == "cloud-image-active"
    assert existing_image_path.exists()
    assert observation[0] == "cloud-obs-1"
    assert observation[1] == "synced"


@pytest.mark.parametrize("linked_by_cloud_id", [True, False])
def test_get_conflict_detail_ignores_tombstoned_image_changes_for_cloud_id_and_desktop_id(
    monkeypatch,
    linked_by_cloud_id,
):
    local_obs = {
        "id": 1,
        "cloud_id": "cloud-obs-1",
        "date": "2026-01-01",
        "genus": "Flammulina",
        "species": "velutipes",
        "location": "Field edge",
        "sharing_scope": "private",
        "location_public": 0,
        "is_draft": True,
        "location_precision": "exact",
    }
    remote_obs = dict(local_obs)
    baseline_snapshot = {
        "observation": dict(remote_obs),
        "images": [
            {
                "id": "cloud-image-1",
                "desktop_id": 11,
                "sort_order": 0,
                "image_type": "field",
                "original_filename": "image.jpg",
                "ai_crop_x1": 0,
                "ai_crop_y1": 0,
                "ai_crop_x2": 100,
                "ai_crop_y2": 100,
                "ai_crop_source_w": 100,
                "ai_crop_source_h": 100,
                "ai_crop_is_custom": False,
            }
        ],
    }
    local_images = [
        {
            "id": 11,
            "cloud_id": "cloud-image-1" if linked_by_cloud_id else None,
            "filepath": "/tmp/image.jpg",
            "image_type": "field",
            "sort_order": 0,
            "original_filename": "image.jpg",
            "ai_crop_x1": 5,
            "ai_crop_y1": 6,
            "ai_crop_x2": 95,
            "ai_crop_y2": 94,
            "ai_crop_source_w": 100,
            "ai_crop_source_h": 100,
            "ai_crop_is_custom": True,
        }
    ]
    remote_images = [
        {
            "id": "cloud-image-1",
            "desktop_id": 11,
            "sort_order": 0,
            "image_type": "field",
            "original_filename": "image.jpg",
            "deleted_at": "2026-05-01 10:00:00",
            "storage_path": "user/cloud-obs-1/0_1780049894442.webp",
        }
    ]

    monkeypatch.setattr(cloud_sync.ObservationDB, "get_observation", lambda local_id: dict(local_obs))
    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation", lambda local_id: [dict(row) for row in local_images])
    monkeypatch.setattr(cloud_sync.MeasurementDB, "get_measurements_for_observation", lambda local_id: [])
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: json.dumps(baseline_snapshot))

    class DummyClient:
        def get_observation(self, cloud_id):
            return dict(remote_obs)

        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            return [dict(row) for row in remote_images]

    detail = cloud_sync.get_conflict_detail(DummyClient(), 1, "cloud-obs-1")

    assert detail["field_rows"] == []
    assert detail["image_mismatches"] == []
    assert detail["local_image_changes"] == []
    assert detail["remote_image_changes"] == []


def test_pull_all_ignores_cloud_tombstone_even_when_local_media_changed(monkeypatch, tmp_path):
    db_path = _init_tombstone_sync_db(tmp_path)
    images_root = tmp_path / "images"
    images_root.mkdir()
    image_path = images_root / "image.jpg"
    image_path.write_bytes(b"image-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath, image_type, sort_order,
                ai_crop_x1, ai_crop_y1, ai_crop_x2, ai_crop_y2, ai_crop_source_w, ai_crop_source_h,
                ai_crop_is_custom, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                11,
                1,
                "cloud-image-1",
                str(image_path),
                None,
                "field",
                0,
                0,
                0,
                100,
                100,
                100,
                100,
                0,
                "2026-05-01T00:00:00Z",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    baseline_snapshot = {
        "observation": {
            "id": "cloud-obs-1",
            "desktop_id": 1,
            "date": "2026-01-01",
            "genus": "Flammulina",
            "species": "velutipes",
            "location": "Field edge",
            "sharing_scope": "private",
            "location_public": 0,
            "is_draft": True,
            "location_precision": "exact",
        },
        "images": [
            {
                "id": "cloud-image-1",
                "desktop_id": 11,
                "sort_order": 0,
                "image_type": "field",
                "original_filename": "image.jpg",
                "ai_crop_x1": 0,
                "ai_crop_y1": 0,
                "ai_crop_x2": 100,
                "ai_crop_y2": 100,
                "ai_crop_source_w": 100,
                "ai_crop_source_h": 100,
                "ai_crop_is_custom": False,
            }
        ],
        "measurements": [],
    }

    class DummyClient:
        def pull_bulk_image_metadata(self, obs_cloud_ids):
            return []

        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            return [
                {
                    "id": "cloud-image-1",
                    "desktop_id": 1,
                    "observation_id": cloud_id,
                    "deleted_at": "2026-05-29 10:22:16.824+00",
                    "storage_path": "8c471394-b274-4933-b830-59805820d93c/614/0_1780049894442.webp",
                    "original_filename": "image.jpg",
                    "image_type": "field",
                    "sort_order": 0,
                }
            ]

        def set_desktop_id(self, *args, **kwargs):
            pass

    monkeypatch.setattr(cloud_sync, "_backfill_missing_exif_on_cloud_images", lambda: None)
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: json.dumps(baseline_snapshot))
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda observation_id: "old-signature")
    monkeypatch.setattr(cloud_sync, "_local_cloud_media_signature", lambda observation_id: "new-signature")
    monkeypatch.setattr(cloud_sync, "_store_local_media_signature_if_equivalent", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *args, **kwargs: [])
    monkeypatch.setattr(cloud_sync, "_detect_deleted_remote_observations", lambda remote_obs: [])
    monkeypatch.setattr(cloud_sync, "_apply_remote_observation_fields", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_rename_to_detected_image_extension", lambda path: Path(path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    result = cloud_sync.pull_all(
        DummyClient(),
        remote_obs=[
            {
                "id": "cloud-obs-1",
                "desktop_id": 1,
                "date": "2026-01-01",
                "genus": "Flammulina",
                "species": "velutipes",
            }
        ],
        sync_calibrations=False,
    )

    conn = sqlite3.connect(db_path)
    try:
        tombstone = conn.execute(
            """
            SELECT deleted_cloud_id, deleted_at, local_observation_id, local_image_id
            FROM image_tombstones
            WHERE deleted_cloud_id = ?
            """,
            ("cloud-image-1",),
        ).fetchone()
        image_row = conn.execute(
            "SELECT COUNT(*) FROM images WHERE id = ?",
            (11,),
        ).fetchone()[0]
        observation = conn.execute(
            "SELECT cloud_id, sync_status FROM observations WHERE id = ?",
            (1,),
        ).fetchone()
    finally:
        conn.close()

    assert result["errors"] == []
    assert result["pulled"] == 1
    assert tombstone == ("cloud-image-1", "2026-05-29 10:22:16", 1, 11)
    assert image_row == 1
    assert observation == ("cloud-obs-1", "dirty")


def test_pull_all_records_unmatched_cloud_tombstone_without_conflict(monkeypatch, tmp_path):
    db_path = _init_tombstone_sync_db(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.commit()
    finally:
        conn.close()

    baseline_snapshot = {
        "observation": {
            "id": "cloud-obs-1",
            "desktop_id": 1,
            "date": "2026-01-01",
            "genus": "Flammulina",
            "species": "velutipes",
            "location": "Field edge",
            "sharing_scope": "private",
            "location_public": 0,
            "is_draft": True,
            "location_precision": "exact",
        },
        "images": [
            {
                "id": "cloud-image-1",
                "desktop_id": 11,
                "sort_order": 0,
                "image_type": "field",
                "original_filename": "image.jpg",
            }
        ],
        "measurements": [],
    }

    class DummyClient:
        def pull_bulk_image_metadata(self, obs_cloud_ids):
            return []

        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            return [
                {
                    "id": "cloud-image-1",
                    "desktop_id": 11,
                    "observation_id": cloud_id,
                    "deleted_at": "2026-05-29 10:22:16.824+00",
                    "storage_path": "8c471394-b274-4933-b830-59805820d93c/614/0_1780049894442.webp",
                    "original_filename": "image.jpg",
                    "image_type": "field",
                    "sort_order": 0,
                }
            ]

        def set_desktop_id(self, *args, **kwargs):
            pass

    monkeypatch.setattr(cloud_sync, "_backfill_missing_exif_on_cloud_images", lambda: None)
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda cloud_id: json.dumps(baseline_snapshot))
    monkeypatch.setattr(cloud_sync, "_load_local_cloud_media_signature", lambda observation_id: "")
    monkeypatch.setattr(cloud_sync, "_local_cloud_media_signature", lambda observation_id: "")
    monkeypatch.setattr(cloud_sync, "_store_local_media_signature_if_equivalent", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *args, **kwargs: [])
    monkeypatch.setattr(cloud_sync, "_detect_deleted_remote_observations", lambda remote_obs: [])
    monkeypatch.setattr(cloud_sync, "_apply_remote_observation_fields", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "update_app_settings", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_rename_to_detected_image_extension", lambda path: Path(path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    result = cloud_sync.pull_all(
        DummyClient(),
        remote_obs=[
            {
                "id": "cloud-obs-1",
                "desktop_id": 1,
                "date": "2026-01-01",
                "genus": "Flammulina",
                "species": "velutipes",
            }
        ],
        sync_calibrations=False,
    )

    conn = sqlite3.connect(db_path)
    try:
        tombstone = conn.execute(
            """
            SELECT deleted_cloud_id, deleted_at, local_observation_id, local_image_id
            FROM image_tombstones
            WHERE deleted_cloud_id = ?
            """,
            ("cloud-image-1",),
        ).fetchone()
    finally:
        conn.close()

    assert result["errors"] == []
    assert result["pulled"] == 1
    assert tombstone == ("cloud-image-1", "2026-05-29 10:22:16", 1, None)


def test_push_images_for_observation_skips_tombstoned_local_image_id(monkeypatch, tmp_path, capsys):
    db_path = _init_tombstone_sync_db(tmp_path)
    images_root = tmp_path / "images"
    images_root.mkdir()
    image_path = images_root / "image.jpg"
    image_path.write_bytes(b"image-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "synced", "2026-05-01T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath, image_type, sort_order
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (11, 1, None, str(image_path), None, "field", 0),
        )
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?)
            """,
            ("cloud-image-1", "2026-05-01 10:00:00", 1, 11),
        )
        conn.commit()
    finally:
        conn.close()

    pushed_ids: list[int] = []
    uploaded_paths: list[str] = []

    client = cloud_sync.SporelyCloudClient("token", "user-123")
    monkeypatch.setattr(client, "pull_image_metadata", lambda obs_cloud_id: [])
    monkeypatch.setattr(client, "_observation_images_support_ai_crop", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_ai_crop_custom", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_upload_metadata", lambda: False)
    monkeypatch.setattr(client, "_patch", lambda *args, **kwargs: None)

    def fake_push_image_metadata(img, obs_cloud_id, storage_path):
        pushed_ids.append(int(img["id"]))
        return f"cloud-image-{int(img['id'])}"

    def fake_upload_image_file(local_path, obs_cloud_id, img_cloud_id, storage_path=None, upload_meta=None):
        uploaded_paths.append(str(local_path))
        return storage_path

    def prepare_images_cb(obs, progress_cb):
        return (
            [
                {
                    "image_row": {
                        "id": 11,
                        "cloud_id": None,
                        "filepath": str(image_path),
                        "image_type": "field",
                        "sort_order": 0,
                    },
                    "upload_path": str(image_path),
                }
            ],
            None,
            [],
        )

    monkeypatch.setattr(client, "push_image_metadata", fake_push_image_metadata)
    monkeypatch.setattr(client, "upload_image_file", fake_upload_image_file)
    monkeypatch.setattr(cloud_sync, "_push_pending_image_tombstones", lambda client: [])
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    result = cloud_sync._push_images_for_observation(
        client,
        {"id": 1},
        "cloud-obs-1",
        prepare_images_cb=prepare_images_cb,
    )

    output = capsys.readouterr().out

    assert result is True
    assert pushed_ids == []
    assert uploaded_paths == []
    assert "skipped local image 11 because it has a local tombstone" in output


def test_summarize_sync_issues_still_flags_real_conflicts():
    summary = cloud_sync.summarize_sync_issues(
        [
            "cloud cloud-obs-1: skipped remote update because local observation 1 has unsynced desktop edits"
        ]
    )

    assert summary["conflict_count"] == 1
    assert summary["other_count"] == 0
    assert summary["conflicts"][0]["local_id"] == 1
    assert summary["conflicts"][0]["cloud_id"] == "cloud-obs-1"


def test_import_remote_images_records_deleted_rows_and_imports_only_active_rows(
    monkeypatch,
    tmp_path,
):
    db_path = _init_tombstone_sync_db(tmp_path)
    temp_root = tmp_path / "sync-tombstones"
    temp_root.mkdir()
    download_calls: list[str] = []
    fetched_calls: list[tuple[str, bool]] = []
    add_calls: list[dict] = []

    images_root = tmp_path / "images"
    image_path = images_root / "observation-1" / "image.jpg"
    original_path = images_root / "observation-1" / "originals" / "image-original.jpg"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    original_path.parent.mkdir(parents=True, exist_ok=True)
    image_path.write_text("image", encoding="utf-8")
    original_path.write_text("original", encoding="utf-8")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status) VALUES (?, ?, ?)",
            (1, "cloud-obs-1", "synced"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, original_filepath, image_type
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (11, 1, "cloud-image-1", str(image_path), str(original_path), "field"),
        )
        conn.commit()
    finally:
        conn.close()

    class DummyClient:
        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            fetched_calls.append((cloud_id, include_deleted_for_sync))
            return [
                {
                    "id": "cloud-image-1",
                    "observation_id": "cloud-obs-1",
                    "deleted_at": "2026-05-01 10:00:00",
                    "storage_path": "user/cloud-obs-1/cloud-image-1_deleted.jpg",
                    "original_filename": "deleted.jpg",
                    "image_type": "field",
                },
                {
                    "id": "cloud-image-2",
                    "observation_id": "cloud-obs-1",
                    "deleted_at": None,
                    "storage_path": "user/cloud-obs-1/cloud-image-2_kept.jpg",
                    "original_filename": "kept.jpg",
                    "image_type": "field",
                },
            ]

        def download_image_file(self, storage_path, dest_path):
            download_calls.append(storage_path)
            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"cloud image bytes")

        def set_image_desktop_id(self, *args, **kwargs):
            pass

    def fake_add_image(**kwargs):
        add_calls.append(dict(kwargs))
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.execute(
                """
                INSERT INTO images (observation_id, filepath, image_type, sort_order)
                VALUES (?, ?, ?, ?)
                """,
                (
                    kwargs["observation_id"],
                    kwargs["filepath"],
                    kwargs["image_type"],
                    kwargs.get("sort_order"),
                ),
            )
            local_id = cursor.lastrowid
            conn.commit()
            return local_id
        finally:
            conn.close()

    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "from_stored_credentials", lambda: DummyClient())
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_rename_to_detected_image_extension", lambda path: Path(path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync.tempfile, "mkdtemp", lambda prefix=None: str(temp_root))
    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", fake_add_image)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)

    cloud_sync._import_remote_images(
        {"id": "cloud-obs-1", "genus": "Flammulina", "species": "velutipes"},
        1,
        "cloud-obs-1",
    )

    conn = sqlite3.connect(db_path)
    try:
        tombstone = conn.execute(
            """
            SELECT deleted_cloud_id, deleted_at, deleted_storage_path,
                   deleted_observation_cloud_id, local_observation_id, local_image_id
            FROM image_tombstones
            WHERE deleted_cloud_id = ?
            """,
            ("cloud-image-1",),
        ).fetchone()
        image_rows = conn.execute(
            "SELECT id, cloud_id FROM images ORDER BY id"
        ).fetchall()
    finally:
        conn.close()

    assert fetched_calls == [("cloud-obs-1", True)]
    assert len(add_calls) == 1
    assert download_calls == ["user/cloud-obs-1/cloud-image-2_kept.jpg"]
    assert tombstone == (
        "cloud-image-1",
        "2026-05-01 10:00:00",
        "user/cloud-obs-1/cloud-image-1_deleted.jpg",
        "cloud-obs-1",
        1,
        11,
    )
    assert image_rows == [
        (11, "cloud-image-1"),
        (12, "cloud-image-2"),
    ]
    assert image_path.exists()
    assert original_path.exists()


def test_import_remote_images_skips_tombstoned_cloud_image_and_keeps_unrelated_imports(
    monkeypatch,
    tmp_path,
    capsys,
):
    db_path = _init_tombstone_sync_db(tmp_path)
    temp_root = tmp_path / "sync-tombstones"
    temp_root.mkdir()
    download_calls: list[str] = []

    conn = sqlite3.connect(db_path)
    try:
        schema._ensure_image_tombstones_table(conn.cursor())
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?)
            """,
            ("cloud-image-1", "2026-05-01 10:00:00", 1, 11),
        )
        conn.commit()
    finally:
        conn.close()

    class DummyClient:
        def download_image_file(self, storage_path, dest_path):
            download_calls.append(storage_path)
            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"cloud image bytes")

        def set_image_desktop_id(self, *args, **kwargs):
            pass

    def fake_add_image(**kwargs):
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.execute(
                """
                INSERT INTO images (observation_id, filepath, image_type, sort_order)
                VALUES (?, ?, ?, ?)
                """,
                (
                    kwargs["observation_id"],
                    kwargs["filepath"],
                    kwargs["image_type"],
                    kwargs.get("sort_order"),
                ),
            )
            local_id = cursor.lastrowid
            conn.commit()
            return local_id
        finally:
            conn.close()

    monkeypatch.setattr(cloud_sync.SporelyCloudClient, "from_stored_credentials", lambda: DummyClient())
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_rename_to_detected_image_extension", lambda path: Path(path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync.tempfile, "mkdtemp", lambda prefix=None: str(temp_root))
    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", fake_add_image)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)

    cloud_sync._import_remote_images(
        {"id": "cloud-obs-1", "genus": "Flammulina", "species": "velutipes"},
        1,
        "cloud-obs-1",
        remote_images=[
            {
                "id": "cloud-image-1",
                "storage_path": "user/cloud-obs-1/cloud-image-1_deleted.jpg",
                "original_filename": "deleted.jpg",
                "image_type": "field",
            },
            {
                "id": "cloud-image-2",
                "storage_path": "user/cloud-obs-1/cloud-image-2_kept.jpg",
                "original_filename": "kept.jpg",
                "image_type": "field",
            },
        ],
    )

    output = capsys.readouterr().out
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT cloud_id, image_type, filepath FROM images ORDER BY id"
        ).fetchall()
    finally:
        conn.close()

    assert download_calls == ["user/cloud-obs-1/cloud-image-2_kept.jpg"]
    assert len(rows) == 1
    assert rows[0][0] == "cloud-image-2"
    assert rows[0][1] == "field"
    assert "skipped cloud image cloud-image-1 because it has a local tombstone" in output


def test_apply_remote_images_to_local_skips_tombstoned_cloud_image_and_keeps_unrelated_imports(
    monkeypatch,
    tmp_path,
):
    db_path = _init_tombstone_sync_db(tmp_path)
    temp_root = tmp_path / "apply-tombstones"
    temp_root.mkdir()
    download_calls: list[str] = []

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status) VALUES (?, ?, ?)",
            (1, "cloud-obs-1", "synced"),
        )
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?)
            """,
            ("cloud-image-1", "2026-05-01 10:00:00", 1, 11),
        )
        conn.commit()
    finally:
        conn.close()

    class DummyClient:
        def download_image_file(self, storage_path, dest_path):
            download_calls.append(storage_path)
            dest = Path(dest_path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b"cloud image bytes")

        def set_image_desktop_id(self, *args, **kwargs):
            pass

    def fake_add_image(**kwargs):
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.execute(
                """
                INSERT INTO images (observation_id, filepath, image_type, sort_order)
                VALUES (?, ?, ?, ?)
                """,
                (
                    kwargs["observation_id"],
                    kwargs["filepath"],
                    kwargs["image_type"],
                    kwargs.get("sort_order"),
                ),
            )
            local_id = cursor.lastrowid
            conn.commit()
            return local_id
        finally:
            conn.close()

    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_rename_to_detected_image_extension", lambda path: Path(path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync.tempfile, "mkdtemp", lambda prefix=None: str(temp_root))
    monkeypatch.setattr(cloud_sync.ImageDB, "add_image", fake_add_image)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)

    warnings = cloud_sync._apply_remote_images_to_local(
        DummyClient(),
        1,
        [
            {
                "id": "cloud-image-1",
                "observation_id": "cloud-obs-1",
                "storage_path": "user/cloud-obs-1/cloud-image-1_deleted.jpg",
                "original_filename": "deleted.jpg",
                "image_type": "field",
                "sort_order": 0,
            },
            {
                "id": "cloud-image-2",
                "observation_id": "cloud-obs-1",
                "storage_path": "user/cloud-obs-1/cloud-image-2_kept.jpg",
                "original_filename": "kept.jpg",
                "image_type": "field",
                "sort_order": 1,
            },
        ],
        allow_delete=True,
    )

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT cloud_id, image_type FROM images ORDER BY id"
        ).fetchall()
    finally:
        conn.close()

    assert download_calls == ["user/cloud-obs-1/cloud-image-2_kept.jpg"]
    assert rows == [("cloud-image-2", "field")]
    assert any("local tombstone" in warning for warning in warnings)


def test_push_images_for_observation_skips_tombstoned_cloud_image_and_keeps_unrelated_uploads(
    monkeypatch,
    tmp_path,
    capsys,
):
    db_path = _init_tombstone_sync_db(tmp_path)
    images_root = tmp_path / "images"
    images_root.mkdir()

    tombstoned_path = images_root / "tombstoned.jpg"
    kept_path = images_root / "kept.jpg"
    tombstoned_path.write_bytes(b"tombstoned")
    kept_path.write_bytes(b"kept")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status) VALUES (?, ?, ?)",
            (1, "cloud-obs-1", "synced"),
        )
        cursor = conn.execute(
            """
            INSERT INTO images (
                observation_id, cloud_id, filepath, image_type, sort_order, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (1, "cloud-image-1", str(tombstoned_path), "field", 0, "2026-05-01 10:00:00"),
        )
        tombstoned_image_id = cursor.lastrowid
        cursor = conn.execute(
            """
            INSERT INTO images (
                observation_id, cloud_id, filepath, image_type, sort_order, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (1, None, str(kept_path), "field", 1, "2026-05-01 10:00:00"),
        )
        kept_image_id = cursor.lastrowid
        conn.execute(
            """
            INSERT INTO image_tombstones (
                deleted_cloud_id, deleted_at, local_observation_id, local_image_id
            ) VALUES (?, ?, ?, ?)
            """,
            ("cloud-image-1", "2026-05-01 10:00:00", 1, tombstoned_image_id),
        )
        conn.commit()
    finally:
        conn.close()

    pushed_ids: list[int] = []
    uploaded_paths: list[str] = []

    client = cloud_sync.SporelyCloudClient("token", "user-123")
    monkeypatch.setattr(client, "pull_image_metadata", lambda obs_cloud_id: [])
    monkeypatch.setattr(client, "_observation_images_support_ai_crop", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_ai_crop_custom", lambda: False)
    monkeypatch.setattr(client, "_observation_images_support_upload_metadata", lambda: False)
    monkeypatch.setattr(client, "_patch", lambda *args, **kwargs: None)

    def fake_push_image_metadata(img, obs_cloud_id, storage_path):
        pushed_ids.append(int(img["id"]))
        return f"cloud-image-{int(img['id'])}"

    def fake_upload_image_file(local_path, obs_cloud_id, img_cloud_id, storage_path=None, upload_meta=None):
        uploaded_paths.append(str(local_path))
        return storage_path

    monkeypatch.setattr(client, "push_image_metadata", fake_push_image_metadata)
    monkeypatch.setattr(client, "upload_image_file", fake_upload_image_file)
    monkeypatch.setattr(cloud_sync, "_push_pending_image_tombstones", lambda client: [])
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)

    result = cloud_sync._push_images_for_observation(
        client,
        {"id": 1},
        "cloud-obs-1",
    )

    output = capsys.readouterr().out
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT id, cloud_id FROM images ORDER BY id"
        ).fetchall()
    finally:
        conn.close()

    assert result is True
    assert pushed_ids == [kept_image_id]
    assert uploaded_paths == [str(kept_path)]
    assert rows == [
        (tombstoned_image_id, "cloud-image-1"),
        (kept_image_id, f"cloud-image-{kept_image_id}"),
    ]
    assert "cloud-image-1 because it has a local tombstone" not in output


def test_resolve_conflict_keep_local_records_deleted_cloud_images_before_push(
    monkeypatch,
    tmp_path,
):
    db_path = _init_tombstone_sync_db(tmp_path)
    images_root = tmp_path / "images"
    images_root.mkdir()
    image_path = images_root / "image.jpg"
    image_path.write_bytes(b"image-bytes")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
            (1, "cloud-obs-1", "dirty", "2026-05-01T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO images (
                id, observation_id, cloud_id, filepath, image_type, sort_order, created_at, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (11, 1, "cloud-image-1", str(image_path), "field", 0, "2026-05-01T10:00:00Z", "2026-05-01T10:00:00Z"),
        )
        conn.commit()
    finally:
        conn.close()

    pull_calls: list[bool] = []
    push_calls: list[tuple[str, str, str]] = []
    uploaded_paths: list[str] = []
    set_desktop_calls: list[tuple[str, int]] = []

    class DummyClient:
        def push_observation(self, local_obs):
            return "cloud-obs-1"

        def get_observation(self, cloud_id):
            return {"id": cloud_id}

        def pull_image_metadata(self, cloud_id, include_deleted_for_sync=False):
            pull_calls.append(include_deleted_for_sync)
            if include_deleted_for_sync:
                return [
                    {
                        "id": "cloud-image-1",
                        "deleted_at": "2026-05-02 10:00:00",
                        "storage_path": "user/cloud-obs-1/cloud-image-1.jpg",
                    }
                ]
            return []

        def soft_delete_image(self, cloud_image_id, deleted_at):
            push_calls.append(("soft_delete", cloud_image_id, deleted_at))

        def push_image_metadata(self, *args, **kwargs):
            push_calls.append(("push_metadata", str(args[0].get("id")), str(args[1])))
            return "cloud-image-1"

        def upload_image_file(self, local_path, obs_cloud_id, img_cloud_id, storage_path=None, upload_meta=None):
            uploaded_paths.append(str(local_path))
            return storage_path

        def set_image_desktop_id(self, cloud_image_id, desktop_id):
            set_desktop_calls.append((cloud_image_id, desktop_id))

        def _observation_images_support_ai_crop(self):
            return False

        def _observation_images_support_upload_metadata(self):
            return False

    def prepare_images_cb(obs, progress_cb):
        return (
            [
                {
                    "image_row": {
                        "id": 11,
                        "cloud_id": "cloud-image-1",
                        "filepath": str(image_path),
                        "image_type": "field",
                        "sort_order": 0,
                    },
                    "upload_path": str(image_path),
                }
            ],
            None,
            [],
        )

    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    result = cloud_sync.resolve_conflict_keep_local(
        DummyClient(),
        1,
        prepare_images_cb=prepare_images_cb,
    )

    conn = sqlite3.connect(db_path)
    try:
        tombstone = conn.execute(
            """
            SELECT deleted_cloud_id, delete_synced_at
            FROM image_tombstones
            WHERE deleted_cloud_id = ?
            """,
            ("cloud-image-1",),
        ).fetchone()
        image_row = conn.execute(
            "SELECT COUNT(*) FROM images WHERE id = ?",
            (11,),
        ).fetchone()[0]
    finally:
        conn.close()

    assert result["cloud_id"] == "cloud-obs-1"
    assert pull_calls and pull_calls[0] is True
    assert tombstone is not None
    assert tombstone[0] == "cloud-image-1"
    assert tombstone[1] is not None
    assert image_row == 1
    assert push_calls == [("soft_delete", "cloud-image-1", "2026-05-02 10:00:00")]
    assert uploaded_paths == []
    assert set_desktop_calls == []
    assert image_path.exists()


def test_sync_existing_remote_field_image_preserves_local_file(monkeypatch, tmp_path):
    local_file = tmp_path / "local_high_res.jpg"
    local_bytes = b"LOCAL-HIGH-RES-BYTES"
    remote_bytes = b"CLOUD-2MP-BYTES"
    local_file.write_bytes(local_bytes)

    class DummyClient:
        def download_image_file(self, storage_path, dest_path):
            Path(dest_path).write_bytes(remote_bytes)

        def set_image_desktop_id(self, *args, **kwargs):
            pass

    class DummyConn:
        def execute(self, *args, **kwargs):
            return self

        def commit(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: DummyConn())
    monkeypatch.setattr(cloud_sync.ImageDB, "update_image", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_store_cloud_image_file_signature", lambda *args, **kwargs: None)

    cloud_sync._sync_existing_remote_image_to_local(
        DummyClient(),
        {
            "id": 19,
            "observation_id": 19,
            "filepath": str(local_file),
        },
        {
            "id": "cloud-img-19",
            "storage_path": "user/cloud-obs-1/cloud-img-19.jpg",
            "original_filename": "cloud-img-19.jpg",
            "image_type": "field",
        },
    )

    assert local_file.read_bytes() == local_bytes
