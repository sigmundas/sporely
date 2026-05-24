import json
import sqlite3
import uuid

import pytest

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


def _fetch_rows(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in conn.execute("SELECT * FROM calibrations ORDER BY id").fetchall()]
    finally:
        conn.close()


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
