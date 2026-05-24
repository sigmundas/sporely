import sqlite3
import uuid

import pytest

from database import models, schema
from utils import db_share


def _create_legacy_calibrations_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE calibrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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


def _create_calibrations_table_with_uuid(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE calibrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            calibration_uuid TEXT,
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


def _prepare_calibration_db(db_path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        _create_legacy_calibrations_table(conn)
        schema.ensure_calibration_uuid_column(conn.cursor())
        conn.commit()
    finally:
        conn.close()


def _fetch_calibrations(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in conn.execute("SELECT * FROM calibrations ORDER BY id").fetchall()]
    finally:
        conn.close()


def test_legacy_calibration_migration_backfills_uuid_and_index(tmp_path):
    db_path = tmp_path / "legacy.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        _create_legacy_calibrations_table(conn)
        conn.executemany(
            """
            INSERT INTO calibrations (
                objective_key, calibration_date, microns_per_pixel, is_active, notes
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                ("100X", "2026-05-01 10:00:00", 0.0315, 1, "first"),
                ("40X", "2026-05-02 11:00:00", 0.0725, 0, "second"),
            ],
        )
        conn.commit()

        schema.ensure_calibration_uuid_column(conn.cursor())
        conn.commit()

        rows = conn.execute("SELECT calibration_uuid FROM calibrations ORDER BY id").fetchall()
        uuids = [row[0] for row in rows]
        assert len(uuids) == 2
        assert all(uuid.UUID(value) for value in uuids)
        assert len(set(uuids)) == 2

        index_names = {row[1] for row in conn.execute("PRAGMA index_list(calibrations)").fetchall()}
        assert "idx_calibrations_uuid" in index_names
    finally:
        conn.close()


def test_invalid_calibration_uuid_is_replaced_with_valid_uuid(tmp_path):
    db_path = tmp_path / "invalid.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        _create_calibrations_table_with_uuid(conn)
        conn.execute(
            """
            INSERT INTO calibrations (
                calibration_uuid, objective_key, calibration_date, microns_per_pixel
            ) VALUES (?, ?, ?, ?)
            """,
            ("not-a-uuid", "100X", "2026-05-01 10:00:00", 0.0315),
        )
        conn.commit()

        schema.ensure_calibration_uuid_column(conn.cursor())
        conn.commit()

        value = conn.execute("SELECT calibration_uuid FROM calibrations WHERE id = 1").fetchone()[0]
        assert value != "not-a-uuid"
        assert uuid.UUID(value)
    finally:
        conn.close()


def test_uppercase_calibration_uuid_is_normalized_to_canonical_lowercase(tmp_path):
    db_path = tmp_path / "uppercase.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        _create_calibrations_table_with_uuid(conn)
        raw_uuid = str(uuid.uuid4()).upper()
        conn.execute(
            """
            INSERT INTO calibrations (
                calibration_uuid, objective_key, calibration_date, microns_per_pixel
            ) VALUES (?, ?, ?, ?)
            """,
            (raw_uuid, "100X", "2026-05-02 10:00:00", 0.0315),
        )
        conn.commit()

        schema.ensure_calibration_uuid_column(conn.cursor())
        conn.commit()

        value = conn.execute("SELECT calibration_uuid FROM calibrations WHERE id = 1").fetchone()[0]
        assert value == str(uuid.UUID(raw_uuid))
    finally:
        conn.close()


def test_new_calibration_gets_uuid_when_not_provided(tmp_path, monkeypatch):
    db_path = tmp_path / "calibration.sqlite"
    _prepare_calibration_db(db_path)
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    calibration_id = models.CalibrationDB.add_calibration(
        objective_key="100X",
        microns_per_pixel=0.0315,
        calibration_date="2026-05-03 12:00:00",
        notes="generated uuid",
        set_active=False,
    )

    row = _fetch_calibrations(db_path)[0]
    assert calibration_id == row["id"]
    assert row["calibration_uuid"]
    assert uuid.UUID(row["calibration_uuid"])


def test_duplicate_calibration_uuid_is_rejected_by_unique_index(tmp_path, monkeypatch):
    db_path = tmp_path / "duplicate.sqlite"
    _prepare_calibration_db(db_path)
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    duplicate_uuid = str(uuid.uuid4())

    models.CalibrationDB.add_calibration(
        objective_key="100X",
        microns_per_pixel=0.0315,
        calibration_date="2026-05-04 09:00:00",
        notes="first",
        set_active=False,
        calibration_uuid=duplicate_uuid,
    )

    with pytest.raises(sqlite3.IntegrityError):
        models.CalibrationDB.add_calibration(
            objective_key="40X",
            microns_per_pixel=0.0725,
            calibration_date="2026-05-04 09:05:00",
            notes="duplicate",
            set_active=False,
            calibration_uuid=duplicate_uuid,
        )

    rows = _fetch_calibrations(db_path)
    assert len(rows) == 1
    assert rows[0]["calibration_uuid"] == duplicate_uuid


def test_duplicate_insert_rolls_back_previous_active_state(tmp_path, monkeypatch):
    db_path = tmp_path / "rollback.sqlite"
    _prepare_calibration_db(db_path)
    active_uuid = str(uuid.uuid4())

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO calibrations (
                calibration_uuid, objective_key, calibration_date, microns_per_pixel, is_active, notes
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (active_uuid, "100X", "2026-05-06 10:00:00", 0.0315, 1, "active"),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    with pytest.raises(sqlite3.IntegrityError):
        models.CalibrationDB.add_calibration(
            objective_key="100X",
            microns_per_pixel=0.0320,
            calibration_date="2026-05-06 10:05:00",
            notes="duplicate",
            set_active=True,
            calibration_uuid=active_uuid,
        )

    row = _fetch_calibrations(db_path)[0]
    assert row["calibration_uuid"] == active_uuid
    assert row["is_active"] == 1


def test_export_import_preserves_calibration_uuid_and_skips_duplicate_import(tmp_path, monkeypatch):
    source_db = tmp_path / "source.sqlite"
    dest_db = tmp_path / "dest.sqlite"
    bundle_path = tmp_path / "calibrations.zip"
    images_dir = tmp_path / "images"
    objectives_path = tmp_path / "objectives.json"
    reference_db_path = tmp_path / "reference.sqlite"
    images_dir.mkdir()

    _prepare_calibration_db(source_db)
    _prepare_calibration_db(dest_db)

    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(source_db))
    calibration_uuid = str(uuid.uuid4())
    models.CalibrationDB.add_calibration(
        objective_key="100X",
        microns_per_pixel=0.0315,
        calibration_date="2026-05-05 08:00:00",
        notes="bundle source",
        set_active=False,
        calibration_uuid=calibration_uuid,
    )

    monkeypatch.setattr(db_share, "get_database_path", lambda: source_db)
    monkeypatch.setattr(db_share, "get_images_dir", lambda: images_dir)
    monkeypatch.setattr(db_share, "get_objectives_path", lambda: objectives_path)
    monkeypatch.setattr(db_share, "get_reference_database_path", lambda: reference_db_path)
    monkeypatch.setattr(db_share, "load_objectives", lambda: {})
    monkeypatch.setattr(db_share, "save_objectives", lambda _settings: None)
    db_share.export_database_bundle(
        str(bundle_path),
        include_observations=False,
        include_images=False,
        include_measurements=False,
        include_calibrations=True,
        include_reference_values=False,
    )

    monkeypatch.setattr(db_share, "get_connection", lambda: sqlite3.connect(dest_db))
    result = db_share.import_database_bundle(
        str(bundle_path),
        include_observations=False,
        include_images=False,
        include_measurements=False,
        include_calibrations=True,
        include_reference_values=False,
    )

    rows = _fetch_calibrations(dest_db)
    assert result["calibrations"] == 1
    assert rows[0]["calibration_uuid"] == calibration_uuid
    assert rows[0]["notes"] == "bundle source"

    result = db_share.import_database_bundle(
        str(bundle_path),
        include_observations=False,
        include_images=False,
        include_measurements=False,
        include_calibrations=True,
        include_reference_values=False,
    )

    rows = _fetch_calibrations(dest_db)
    assert result["calibrations"] == 0
    assert rows[0]["calibration_uuid"] == calibration_uuid
    assert rows[0]["notes"] == "bundle source"
    assert any("duplicate calibration_uuid" in warning for warning in result["warnings"])


def test_same_objective_and_date_can_stay_separate_with_distinct_uuids(tmp_path, monkeypatch):
    db_path = tmp_path / "separate.sqlite"
    _prepare_calibration_db(db_path)
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    calibration_date = "2026-05-06 07:30:00"

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

    rows = _fetch_calibrations(db_path)
    assert len(rows) == 2
    assert {row["calibration_uuid"] for row in rows} == {first_uuid, second_uuid}
    assert {row["calibration_date"] for row in rows} == {calibration_date}
    assert {row["objective_key"] for row in rows} == {"100X"}


def test_calibration_uuid_does_not_change_when_objective_or_date_changes(tmp_path, monkeypatch):
    db_path = tmp_path / "immutability.sqlite"
    _prepare_calibration_db(db_path)
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    calibration_uuid = str(uuid.uuid4())

    calibration_id = models.CalibrationDB.add_calibration(
        objective_key="100X",
        microns_per_pixel=0.0315,
        calibration_date="2026-05-07 07:00:00",
        notes="immutable",
        set_active=False,
        calibration_uuid=calibration_uuid,
    )

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            UPDATE calibrations
            SET objective_key = ?, calibration_date = ?
            WHERE id = ?
            """,
            ("40X", "2026-05-08 08:30:00", calibration_id),
        )
        conn.commit()
    finally:
        conn.close()

    row = _fetch_calibrations(db_path)[0]
    assert row["calibration_uuid"] == calibration_uuid
    assert row["objective_key"] == "40X"
    assert row["calibration_date"] == "2026-05-08 08:30:00"
