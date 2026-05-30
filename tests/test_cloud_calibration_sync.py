import copy
import io
import json
import sqlite3
import uuid
from pathlib import Path

import pytest
from PIL import Image

from database import models
from utils import cloud_sync


def _create_calibrations_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE calibrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            calibration_uuid TEXT NOT NULL UNIQUE,
            objective_key TEXT NOT NULL,
            calibration_date TEXT NOT NULL,
            calibration_image_date TEXT,
            microns_per_pixel REAL NOT NULL,
            microns_per_pixel_std REAL,
            confidence_interval_low REAL,
            confidence_interval_high REAL,
            num_measurements INTEGER,
            measurements_json TEXT,
            image_filepath TEXT,
            camera TEXT,
            megapixels REAL,
            target_sampling_pct REAL,
            resample_scale_factor REAL,
            calibration_image_width INTEGER,
            calibration_image_height INTEGER,
            notes TEXT,
            is_active INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


def _prepare_db(tmp_path):
    db_path = tmp_path / "calibrations.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        _create_calibrations_table(conn)
        conn.commit()
    finally:
        conn.close()
    return db_path


def _patch_connections(monkeypatch, db_path):
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))


def _patch_app_data_dir(monkeypatch, tmp_path):
    app_root = tmp_path / "appdata"
    monkeypatch.setattr(cloud_sync, "app_data_dir", lambda: app_root)
    return app_root


def _fetch_rows(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in conn.execute("SELECT * FROM calibrations ORDER BY id").fetchall()]
    finally:
        conn.close()


def _write_test_image(path: Path, size=(3200, 2400), color=(128, 64, 32)) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    image = Image.new("RGB", size, color)
    image.save(path)
    return path


def test_push_calibration_metadata_sends_calibration_uuid_and_omits_local_file_path(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    posted_payloads = []

    monkeypatch.setattr(client, "find_remote_calibration", lambda calibration_uuid: None)

    def fake_post(path, payload):
        posted_payloads.append((path, dict(payload)))
        return [{"id": "cloud-cal-1"}]

    monkeypatch.setattr(client, "_post", fake_post)

    cloud_id = client.push_calibration_metadata(
        {
            "calibration_uuid": str(uuid.uuid4()),
            "objective_key": "100X",
            "calibration_date": "2026-05-08 08:30:00",
            "calibration_image_date": "2026-05-07 12:00:00",
            "microns_per_pixel": 0.0315,
            "microns_per_pixel_std": 0.0001,
            "confidence_interval_low": 0.0310,
            "confidence_interval_high": 0.0320,
            "num_measurements": 12,
            "measurements_json": {"images": [{"id": 1}]},
            "image_filepath": "/local/only.jpg",
            "camera": "Camera A",
            "megapixels": 12.3,
            "target_sampling_pct": 0.8,
            "resample_scale_factor": 2.0,
            "calibration_image_width": 4032,
            "calibration_image_height": 3024,
            "notes": "local note",
            "is_active": True,
        }
    )

    assert cloud_id == "cloud-cal-1"
    assert posted_payloads[0][0] == "calibrations"
    payload = posted_payloads[0][1]
    assert payload["calibration_uuid"]
    assert payload["objective_key"] == "100X"
    assert payload["calibration_date"] == "2026-05-08"
    assert payload["calibration_image_date"] == "2026-05-07"
    assert payload["microns_per_pixel"] == 0.0315
    assert payload["measurements_json"] == {"images": [{"id": 1}]}
    assert payload["is_active"] is True
    assert "image_filepath" not in payload
    assert payload["user_id"] == "user-123"


def test_push_calibration_metadata_treats_normalized_same_uuid_payload_as_safe_no_op(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    remote_row = {
        "id": "cloud-cal-1",
        "calibration_uuid": str(uuid.uuid4()).upper(),
        "objective_key": "100X",
        "calibration_date": "2026-05-08",
        "calibration_image_date": "2026-05-07",
        "microns_per_pixel": 0.0315,
        "microns_per_pixel_std": 0.0001,
        "confidence_interval_low": 0.0310,
        "confidence_interval_high": 0.0320,
        "num_measurements": 12,
        "measurements_json": {"images": [{"id": 1}]},
        "camera": "Camera A",
        "megapixels": 12.3,
        "target_sampling_pct": 0.8,
        "resample_scale_factor": 2.0,
        "calibration_image_width": 4032,
        "calibration_image_height": 3024,
        "notes": "local note",
        "is_active": True,
        "image_storage_path": "cloud/path/ignored.jpg",
    }
    posted_payloads = []

    monkeypatch.setattr(client, "find_remote_calibration", lambda calibration_uuid: dict(remote_row))
    monkeypatch.setattr(client, "_post", lambda path, payload: posted_payloads.append((path, dict(payload))) or [{"id": "new"}])

    cloud_id = client.push_calibration_metadata(
        {
            "calibration_uuid": remote_row["calibration_uuid"].lower(),
            "objective_key": "100X",
            "calibration_date": "2026-05-08 08:30:00",
            "calibration_image_date": "2026-05-07 12:00:00",
            "microns_per_pixel": 0.0315,
            "microns_per_pixel_std": 0.0001,
            "confidence_interval_low": 0.0310,
            "confidence_interval_high": 0.0320,
            "num_measurements": 12,
            "measurements_json": '{"images":[{"id":1}]}',
            "image_filepath": "/local/only.jpg",
            "camera": "Camera A",
            "megapixels": 12.3,
            "target_sampling_pct": 0.8,
            "resample_scale_factor": 2.0,
            "calibration_image_width": 4032,
            "calibration_image_height": 3024,
            "notes": "local note",
            "is_active": True,
        }
    )

    assert cloud_id == "cloud-cal-1"
    assert posted_payloads == []


def test_push_calibration_metadata_rejects_invalid_uuid(monkeypatch):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    posted_payloads = []

    monkeypatch.setattr(client, "find_remote_calibration", lambda calibration_uuid: None)
    monkeypatch.setattr(
        client,
        "_post",
        lambda path, payload: posted_payloads.append((path, dict(payload))) or [{"id": "new"}],
    )

    with pytest.raises(cloud_sync.CloudSyncError, match="Missing calibration UUID"):
        client.push_calibration_metadata(
            {
                "calibration_uuid": "not-a-uuid",
                "objective_key": "100X",
                "calibration_date": "2026-05-08 08:30:00",
                "microns_per_pixel": 0.0315,
            }
        )

    assert posted_payloads == []


@pytest.mark.parametrize("raw_value", ["false", "0"])
def test_push_calibration_metadata_normalizes_string_false_to_inactive(monkeypatch, raw_value):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    posted_payloads = []

    monkeypatch.setattr(client, "find_remote_calibration", lambda calibration_uuid: None)
    monkeypatch.setattr(
        client,
        "_post",
        lambda path, payload: posted_payloads.append((path, dict(payload))) or [{"id": "cloud-cal-1"}],
    )

    client.push_calibration_metadata(
        {
            "calibration_uuid": str(uuid.uuid4()),
            "objective_key": "100X",
            "calibration_date": "2026-05-08 08:30:00",
            "microns_per_pixel": 0.0315,
            "is_active": raw_value,
        }
    )

    assert posted_payloads[0][1]["is_active"] is False


def test_select_representative_calibration_image_prefers_readable_image_filepath(tmp_path):
    preferred = _write_test_image(tmp_path / "preferred.jpg", size=(1024, 768))
    fallback = _write_test_image(tmp_path / "fallback.jpg", size=(800, 600))

    calibration = {
        "image_filepath": str(preferred),
        "measurements_json": json.dumps({"images": [{"path": str(fallback)}]}),
    }

    selected = cloud_sync._select_representative_calibration_image_path(calibration)

    assert selected == preferred


def test_select_representative_calibration_image_falls_back_to_measurements_json(tmp_path):
    fallback = _write_test_image(tmp_path / "fallback.jpg", size=(640, 480))
    calibration = {
        "image_filepath": str(tmp_path / "missing.jpg"),
        "measurements_json": json.dumps(
            {
                "images": [
                    {"path": str(tmp_path / "also_missing.jpg")},
                    {"path": str(fallback)},
                ]
            }
        ),
    }

    selected = cloud_sync._select_representative_calibration_image_path(calibration)

    assert selected == fallback


def test_push_calibration_reference_image_uploads_derivative_and_patches_relative_key(monkeypatch, tmp_path):
    source = _write_test_image(tmp_path / "source.jpg", size=(3200, 2400))
    calibration_uuid = str(uuid.uuid4())
    calibration = {
        "calibration_uuid": calibration_uuid,
        "objective_key": "100X",
        "calibration_date": "2026-05-08 08:30:00",
        "microns_per_pixel": 0.0315,
        "image_filepath": str(source),
        "measurements_json": {"images": [{"path": str(tmp_path / "other.jpg")}]},
    }
    original = copy.deepcopy(calibration)

    client = cloud_sync.SporelyCloudClient("token", "user-123")
    uploads = []
    patches = []

    class DummyR2:
        def put_bytes(self, data, key, *, content_type=None, cache_control=None, custom_metadata=None, timeout=None):
            uploads.append(
                {
                    "data": bytes(data),
                    "key": key,
                    "content_type": content_type,
                    "cache_control": cache_control,
                    "custom_metadata": dict(custom_metadata or {}),
                    "timeout": timeout,
                }
            )

        def put_file(self, *args, **kwargs):
            raise AssertionError("put_file should not be used for calibration reference uploads")

    monkeypatch.setattr(client, "_get_r2", lambda: DummyR2())
    monkeypatch.setattr(client, "_patch", lambda path, payload: patches.append((path, dict(payload))))

    warning = client.push_calibration_reference_image(
        calibration,
        cloud_row_id="cloud-cal-1",
        remote_row={"id": "cloud-cal-1", "image_storage_path": None},
    )

    assert warning is None
    assert calibration == original
    assert len(uploads) == 1
    assert len(patches) == 1
    assert patches[0][0] == "calibrations?user_id=eq.user-123&id=eq.cloud-cal-1"
    assert patches[0][1]["image_storage_path"] == uploads[0]["key"]
    assert uploads[0]["key"] == f"user-123/{calibration_uuid}/reference{Path(uploads[0]['key']).suffix}"
    assert not uploads[0]["key"].startswith("http")
    assert uploads[0]["content_type"] in {"image/webp", "image/jpeg"}

    uploaded_image = Image.open(io.BytesIO(uploads[0]["data"]))
    assert max(uploaded_image.size) <= cloud_sync._CALIBRATION_REFERENCE_MAX_EDGE
    assert uploaded_image.size != (3200, 2400)


def test_push_calibration_reference_image_skips_with_warning_when_no_readable_image_exists(monkeypatch):
    calibration = {
        "calibration_uuid": str(uuid.uuid4()),
        "objective_key": "100X",
        "calibration_date": "2026-05-08 08:30:00",
        "microns_per_pixel": 0.0315,
        "image_filepath": "/tmp/missing-reference.jpg",
        "measurements_json": {"images": [{"path": "/tmp/also-missing.jpg"}]},
    }
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    uploads = []
    patches = []

    class DummyR2:
        def put_bytes(self, *args, **kwargs):
            uploads.append((args, kwargs))

    monkeypatch.setattr(client, "_get_r2", lambda: DummyR2())
    monkeypatch.setattr(client, "_patch", lambda path, payload: patches.append((path, dict(payload))))

    warning = client.push_calibration_reference_image(
        calibration,
        cloud_row_id="cloud-cal-1",
        remote_row={"id": "cloud-cal-1", "image_storage_path": None},
    )

    assert warning is not None
    assert "no readable local calibration image" in warning
    assert uploads == []
    assert patches == []


def test_push_calibration_reference_image_respects_existing_cloud_image_storage_path(monkeypatch, tmp_path):
    source = _write_test_image(tmp_path / "source.jpg", size=(1600, 1200))
    calibration = {
        "calibration_uuid": str(uuid.uuid4()),
        "objective_key": "100X",
        "calibration_date": "2026-05-08 08:30:00",
        "microns_per_pixel": 0.0315,
        "image_filepath": str(source),
        "measurements_json": {"images": []},
    }
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    uploads = []
    patches = []

    class DummyR2:
        def put_bytes(self, *args, **kwargs):
            uploads.append((args, kwargs))

        def put_file(self, *args, **kwargs):
            uploads.append(("put_file", args, kwargs))

    monkeypatch.setattr(client, "_get_r2", lambda: DummyR2())
    monkeypatch.setattr(client, "_patch", lambda path, payload: patches.append((path, dict(payload))))

    warning = client.push_calibration_reference_image(
        calibration,
        cloud_row_id="cloud-cal-1",
        remote_row={
            "id": "cloud-cal-1",
            "calibration_uuid": calibration["calibration_uuid"],
            "image_storage_path": "user-123/existing/reference.webp",
        },
    )

    assert warning is None
    assert uploads == []
    assert patches == []


def test_push_calibrations_still_pushes_metadata_when_reference_image_is_missing(monkeypatch, tmp_path):
    db_path = _prepare_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    calibration_uuid = str(uuid.uuid4())
    missing_path = tmp_path / "missing-reference.jpg"

    models.CalibrationDB.add_calibration(
        objective_key="100X",
        microns_per_pixel=0.0315,
        calibration_date="2026-05-08 08:30:00",
        calibration_image_date="2026-05-07 12:00:00",
        measurements_json=json.dumps({"images": [{"path": str(tmp_path / "also-missing.jpg")}]}),
        image_filepath=str(missing_path),
        notes="local note",
        set_active=False,
        calibration_uuid=calibration_uuid,
    )

    client = cloud_sync.SporelyCloudClient("token", "user-123")
    posted_payloads = []

    monkeypatch.setattr(client, "find_remote_calibration", lambda calibration_uuid: None)
    monkeypatch.setattr(client, "list_remote_calibrations", lambda: [])
    monkeypatch.setattr(
        client,
        "_post",
        lambda path, payload: posted_payloads.append((path, dict(payload))) or [{"id": "cloud-cal-1"}],
    )

    result = cloud_sync.push_calibrations(client, remote_calibrations=[])

    rows = _fetch_rows(db_path)
    assert result["pushed"] == 1
    assert len(posted_payloads) == 1
    assert len(rows) == 1
    assert any("no readable local calibration image" in error for error in result["errors"])


def test_pull_calibrations_inserts_remote_metadata_into_empty_local_db(monkeypatch, tmp_path):
    db_path = _prepare_db(tmp_path)
    _patch_connections(monkeypatch, db_path)

    remote_rows = [
        {
            "id": "cloud-cal-1",
            "calibration_uuid": str(uuid.uuid4()),
            "objective_key": "100X",
            "calibration_date": "2026-05-08",
            "calibration_image_date": "2026-05-07",
            "microns_per_pixel": 0.0315,
            "microns_per_pixel_std": 0.0001,
            "confidence_interval_low": 0.0310,
            "confidence_interval_high": 0.0320,
            "num_measurements": 12,
            "measurements_json": {"images": [{"id": 1}]},
            "image_storage_path": "cloud/path/ignored.jpg",
            "camera": "Camera A",
            "megapixels": 12.3,
            "target_sampling_pct": 0.8,
            "resample_scale_factor": 2.0,
            "calibration_image_width": 4032,
            "calibration_image_height": 3024,
            "notes": "remote note",
            "is_active": True,
        }
    ]

    class DummyClient:
        def list_remote_calibrations(self):
            return [dict(row) for row in remote_rows]

    result = cloud_sync.pull_calibrations(DummyClient())

    rows = _fetch_rows(db_path)
    assert result["pulled"] == 1
    assert result["errors"] == []
    assert len(rows) == 1
    row = rows[0]
    assert row["calibration_uuid"] == remote_rows[0]["calibration_uuid"]
    assert row["objective_key"] == "100X"
    assert row["calibration_date"] == "2026-05-08"
    assert row["calibration_image_date"] == "2026-05-07"
    assert row["microns_per_pixel"] == 0.0315
    assert json.loads(row["measurements_json"]) == {"images": [{"id": 1}]}
    assert row["notes"] == "remote note"
    assert row["is_active"] == 1
    assert row["image_filepath"] is None


def test_pull_calibrations_skips_conflicting_same_uuid(monkeypatch, tmp_path):
    db_path = _prepare_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    calibration_uuid = str(uuid.uuid4())

    models.CalibrationDB.add_calibration(
        objective_key="100X",
        microns_per_pixel=0.0315,
        calibration_date="2026-05-08 08:30:00",
        notes="local note",
        set_active=False,
        calibration_uuid=calibration_uuid,
    )

    remote_rows = [
        {
            "id": "cloud-cal-1",
            "calibration_uuid": calibration_uuid,
            "objective_key": "100X",
            "calibration_date": "2026-05-08",
            "microns_per_pixel": 0.0400,
            "notes": "remote note",
            "is_active": False,
        }
    ]

    class DummyClient:
        def list_remote_calibrations(self):
            return [dict(row) for row in remote_rows]

    result = cloud_sync.pull_calibrations(DummyClient())
    rows = _fetch_rows(db_path)

    assert result["pulled"] == 0
    assert result["errors"]
    assert "skipped pull" in result["errors"][0]
    assert len(rows) == 1
    assert rows[0]["notes"] == "local note"
    assert rows[0]["microns_per_pixel"] == 0.0315


def test_push_calibrations_skips_conflicting_same_uuid(monkeypatch, tmp_path):
    db_path = _prepare_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    calibration_uuid = str(uuid.uuid4())

    models.CalibrationDB.add_calibration(
        objective_key="100X",
        microns_per_pixel=0.0315,
        calibration_date="2026-05-08 08:30:00",
        calibration_image_date="2026-05-07 12:00:00",
        notes="local note",
        set_active=False,
        calibration_uuid=calibration_uuid,
    )

    remote_rows = [
        {
            "id": "cloud-cal-1",
            "calibration_uuid": calibration_uuid,
            "objective_key": "100X",
            "calibration_date": "2026-05-08",
            "calibration_image_date": "2026-05-07",
            "microns_per_pixel": 0.0400,
            "notes": "remote note",
            "is_active": False,
        }
    ]

    class DummyClient:
        def __init__(self):
            self.pushed_payloads = []

        def list_remote_calibrations(self):
            return [dict(row) for row in remote_rows]

        def find_remote_calibration(self, calibration_uuid):
            return dict(remote_rows[0]) if calibration_uuid == remote_rows[0]["calibration_uuid"] else None

        def push_calibration_metadata(self, calibration):
            self.pushed_payloads.append(dict(calibration))
            return "cloud-cal-2"

    client = DummyClient()
    result = cloud_sync.push_calibrations(client)

    assert result["pushed"] == 0
    assert client.pushed_payloads == []
    assert result["errors"]
    assert "skipped push" in result["errors"][0]


def test_push_calibrations_keeps_same_objective_date_separate_by_uuid(monkeypatch, tmp_path):
    db_path = _prepare_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    calibration_date = "2026-05-08 08:30:00"

    first_uuid = str(uuid.uuid4())
    second_uuid = str(uuid.uuid4())
    models.CalibrationDB.add_calibration(
        objective_key="100X",
        microns_per_pixel=0.0315,
        calibration_date=calibration_date,
        notes="first",
        set_active=False,
        calibration_uuid=first_uuid,
    )
    models.CalibrationDB.add_calibration(
        objective_key="100X",
        microns_per_pixel=0.0317,
        calibration_date=calibration_date,
        notes="second",
        set_active=False,
        calibration_uuid=second_uuid,
    )

    class DummyClient:
        def __init__(self):
            self.pushed_payloads = []

        def list_remote_calibrations(self):
            return []

        def find_remote_calibration(self, calibration_uuid):
            return None

        def push_calibration_metadata(self, calibration):
            self.pushed_payloads.append(dict(calibration))
            return f'cloud-{len(self.pushed_payloads)}'

    client = DummyClient()
    result = cloud_sync.push_calibrations(client)

    assert result["pushed"] == 2
    assert len(client.pushed_payloads) == 2
    assert {row["calibration_uuid"] for row in client.pushed_payloads} == {first_uuid, second_uuid}
    assert {row["objective_key"] for row in client.pushed_payloads} == {"100X"}
    assert {row["calibration_date"] for row in client.pushed_payloads} == {calibration_date}


def test_pull_calibrations_keeps_same_objective_date_separate_and_only_one_active_row(monkeypatch, tmp_path):
    db_path = _prepare_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    calibration_date = "2026-05-08"

    first_uuid = str(uuid.uuid4())
    second_uuid = str(uuid.uuid4())

    remote_rows = [
        {
            "id": "cloud-cal-1",
            "calibration_uuid": first_uuid,
            "objective_key": "100X",
            "calibration_date": calibration_date,
            "calibration_image_date": None,
            "microns_per_pixel": 0.0315,
            "notes": "first",
            "is_active": False,
        },
        {
            "id": "cloud-cal-2",
            "calibration_uuid": second_uuid,
            "objective_key": "100X",
            "calibration_date": calibration_date,
            "calibration_image_date": None,
            "microns_per_pixel": 0.0317,
            "notes": "second",
            "is_active": True,
        },
    ]

    class DummyClient:
        def list_remote_calibrations(self):
            return [dict(row) for row in remote_rows]

    result = cloud_sync.pull_calibrations(DummyClient())

    rows = _fetch_rows(db_path)
    assert result["pulled"] == 2
    assert result["errors"] == []
    assert len(rows) == 2
    assert {row["calibration_uuid"] for row in rows} == {first_uuid, second_uuid}
    assert {row["calibration_date"] for row in rows} == {calibration_date, calibration_date}
    assert sum(1 for row in rows if row["is_active"] == 1) == 1
    active_row = next(row for row in rows if row["is_active"] == 1)
    assert active_row["calibration_uuid"] == second_uuid


def test_calibration_reference_cache_path_uses_calibration_uuid_and_cloud_cache_root(monkeypatch, tmp_path):
    app_root = _patch_app_data_dir(monkeypatch, tmp_path)
    calibration_uuid = str(uuid.uuid4())

    path = cloud_sync._calibration_recovery_cache_path(
        calibration_uuid,
        "user-123/100X/reference.webp",
    )

    assert path == app_root / "cloud_cache" / "calibrations" / calibration_uuid / "reference.webp"
    assert path.parts[-4] == "cloud_cache"
    assert path.parts[-3] == "calibrations"
    assert path.parts[-2] == calibration_uuid
    assert "100X" not in str(path)


def test_calibration_reference_recovery_state_distinguishes_existing_and_missing_local_photo(tmp_path):
    existing = _write_test_image(tmp_path / "existing.jpg", size=(800, 600))
    calibration_uuid = str(uuid.uuid4())
    base = {
        "calibration_uuid": calibration_uuid,
        "image_storage_path": f"user-123/{calibration_uuid}/reference.jpg",
    }

    existing_state = cloud_sync._calibration_reference_recovery_state(
        {
            **base,
            "image_filepath": str(existing),
            "measurements_json": {"images": [{"path": str(tmp_path / "unused.jpg")}]},
        }
    )
    missing_state = cloud_sync._calibration_reference_recovery_state(
        {
            **base,
            "image_filepath": str(tmp_path / "missing.jpg"),
            "measurements_json": {"images": [{"path": str(existing)}]},
        }
    )
    unavailable_state = cloud_sync._calibration_reference_recovery_state(
        {
            "calibration_uuid": calibration_uuid,
            "image_filepath": str(tmp_path / "missing-again.jpg"),
            "image_storage_path": "",
        }
    )

    assert existing_state["local_original_exists"] is True
    assert existing_state["recovery_available"] is False
    assert existing_state["local_original_missing"] is False

    assert missing_state["local_original_exists"] is False
    assert missing_state["recovery_available"] is True
    assert missing_state["local_original_missing"] is True

    assert unavailable_state["recovery_available"] is False
    assert unavailable_state["image_storage_path"] is None


def test_calibration_reference_recovery_state_treats_existing_empty_local_file_as_authoritative(tmp_path):
    empty_file = tmp_path / "empty.jpg"
    empty_file.write_bytes(b"")
    calibration_uuid = str(uuid.uuid4())

    state = cloud_sync._calibration_reference_recovery_state(
        {
            "calibration_uuid": calibration_uuid,
            "image_filepath": str(empty_file),
            "image_storage_path": f"user-123/{calibration_uuid}/reference.jpg",
        }
    )

    assert state["local_original_exists"] is True
    assert state["recovery_available"] is False
    assert state["local_original_path"] == empty_file


def test_download_calibration_reference_to_cache_skips_existing_local_original(monkeypatch, tmp_path):
    _patch_app_data_dir(monkeypatch, tmp_path)
    local_original = _write_test_image(tmp_path / "original.jpg", size=(1024, 768))
    calibration_uuid = str(uuid.uuid4())
    calibration = {
        "calibration_uuid": calibration_uuid,
        "objective_key": "100X",
        "image_filepath": str(local_original),
        "image_storage_path": f"user-123/{calibration_uuid}/reference.webp",
        "measurements_json": {"images": [{"id": 1, "path": str(tmp_path / "other.jpg")}]},
    }
    original = copy.deepcopy(calibration)

    class DummyClient:
        def __init__(self):
            self.calls = []

        def download_image_file(self, storage_path, dest_path):
            self.calls.append((storage_path, dest_path))
            raise AssertionError("download_image_file should not be called when the local original exists")

    client = DummyClient()
    result = cloud_sync.download_calibration_reference_to_cache(client, calibration)

    assert result["status"] == "skipped_local_original_exists"
    assert result["downloaded"] is False
    assert result["cache_path"] is None
    assert result["recovery_available"] is False
    assert client.calls == []
    assert calibration == original


def test_download_calibration_reference_to_cache_downloads_missing_local_photo_without_mutating_input(monkeypatch, tmp_path):
    app_root = _patch_app_data_dir(monkeypatch, tmp_path)
    source = _write_test_image(tmp_path / "download_source.png", size=(640, 480))
    calibration_uuid = str(uuid.uuid4())
    calibration = {
        "calibration_uuid": calibration_uuid,
        "objective_key": "100X",
        "image_filepath": str(tmp_path / "missing-original.jpg"),
        "image_storage_path": f"user-123/{calibration_uuid}/reference",
        "measurements_json": {"images": [{"id": 1, "path": str(tmp_path / "other.jpg")}], "meta": {"note": "keep me"}},
    }
    original = copy.deepcopy(calibration)

    class DummyClient:
        def __init__(self):
            self.calls = []

        def download_image_file(self, storage_path, dest_path):
            self.calls.append((storage_path, dest_path))
            dest_path.write_bytes(source.read_bytes())
            return dest_path

    client = DummyClient()
    result = cloud_sync.download_calibration_reference_to_cache(client, calibration)

    cache_path = result["cache_path"]
    assert result["recovery_available"] is True
    assert result["status"] == "downloaded_to_cache"
    assert result["downloaded"] is True
    assert result["warning"] is None
    assert len(client.calls) == 1
    assert client.calls[0][0] == f"user-123/{calibration_uuid}/reference"
    assert calibration == original
    assert cache_path is not None
    assert isinstance(cache_path, Path)
    assert cache_path.exists()
    assert cache_path.suffix == ".png"
    assert cache_path.parent.parent.name == "calibrations"
    assert cache_path.parent.name == calibration_uuid
    assert "100X" not in str(cache_path)
    assert calibration["image_filepath"] == original["image_filepath"]
    assert calibration["measurements_json"] == original["measurements_json"]
    assert cache_path.parent.parent.parent == app_root / "cloud_cache"


@pytest.mark.parametrize("storage_path", [None, ""])
def test_download_calibration_reference_to_cache_returns_unavailable_without_storage_path(monkeypatch, tmp_path, storage_path):
    _patch_app_data_dir(monkeypatch, tmp_path)
    calibration_uuid = str(uuid.uuid4())
    calibration = {
        "calibration_uuid": calibration_uuid,
        "objective_key": "100X",
        "image_filepath": str(tmp_path / "missing-original.jpg"),
        "image_storage_path": storage_path,
        "measurements_json": {"images": []},
    }

    class DummyClient:
        def download_image_file(self, storage_path, dest_path):
            raise AssertionError("download_image_file should not be called without a storage path")

    result = cloud_sync.download_calibration_reference_to_cache(DummyClient(), calibration)

    assert result["status"] == "unavailable_missing_storage_path"
    assert result["downloaded"] is False
    assert result["cache_path"] is None
    assert result["warning"]


def test_download_calibration_reference_to_cache_reports_failed_download_without_db_mutation(monkeypatch, tmp_path):
    db_path = _prepare_db(tmp_path)
    _patch_connections(monkeypatch, db_path)
    _patch_app_data_dir(monkeypatch, tmp_path)

    calibration_uuid = str(uuid.uuid4())
    models.CalibrationDB.add_calibration(
        objective_key="100X",
        microns_per_pixel=0.0315,
        calibration_date="2026-05-08 08:30:00",
        measurements_json=json.dumps({"images": [{"id": 1}], "meta": {"note": "keep me"}}),
        image_filepath=str(tmp_path / "missing-original.jpg"),
        notes="local note",
        set_active=False,
        calibration_uuid=calibration_uuid,
    )
    row_before = _fetch_rows(db_path)[0]
    calibration = dict(row_before)
    original = copy.deepcopy(calibration)

    class DummyClient:
        def download_image_file(self, storage_path, dest_path):
            raise RuntimeError("network exploded")

    result = cloud_sync.download_calibration_reference_to_cache(
        DummyClient(),
        {
            **calibration,
            "image_storage_path": f"user-123/{calibration_uuid}/reference.jpg",
        },
    )

    row_after = _fetch_rows(db_path)[0]

    assert result["status"] == "download_failed"
    assert result["downloaded"] is False
    assert "network exploded" in result["warning"]
    assert calibration == original
    assert row_after == row_before
