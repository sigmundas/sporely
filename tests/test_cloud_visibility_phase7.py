import json
import sqlite3
from pathlib import Path

import pytest

from utils import cloud_sync


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


def test_mark_observation_dirty_only_marks_sync_status(tmp_path, monkeypatch):
    db_path = tmp_path / "cloud_dirty.sqlite"
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
    conn.execute(
        "INSERT INTO observations (id, cloud_id, sync_status, synced_at) VALUES (?, ?, ?, ?)",
        (1, "cloud-obs-1", "synced", "2026-01-01T00:00:00Z"),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))

    cloud_sync.mark_observation_dirty(1)

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT cloud_id, sync_status, synced_at FROM observations WHERE id = ?",
            (1,),
        ).fetchone()
    finally:
        conn.close()

    assert row == ("cloud-obs-1", "dirty", "2026-01-01T00:00:00Z")


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

        def pull_image_metadata(self, cloud_id):
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
    assert desktop_id_calls == [("cloud-img-1", 23)]
    assert any(
        sql == "UPDATE images SET cloud_id = ?, synced_at = ? WHERE id = ?"
        and params[0] == "cloud-img-1"
        and params[2] == 23
        for sql, params in conn_statements
    )


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
