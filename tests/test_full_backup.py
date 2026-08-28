import json
import shutil
import sqlite3
from pathlib import Path
from zipfile import ZipFile

import pytest

from database import schema
from utils.archive.full_backup import FullBackupError, _snapshot_database, create_full_backup
from utils.archive.full_restore import restore_full_backup
from utils.archive.inventory import MAIN_DATABASE_TABLES, REFERENCE_DATABASE_TABLES
from utils.archive.manifest import ArchiveManifest
from utils.archive.validation import ArchiveValidationError, validate_full_backup
from utils.raw_detection import is_raw_image_path


@pytest.fixture
def installation(monkeypatch, tmp_path):
    app_root = tmp_path / "app"
    app_root.mkdir()
    monkeypatch.setattr(schema, "_app_dir", app_root)
    monkeypatch.setattr(schema, "DATABASE_PATH", app_root / "mushrooms.db")
    monkeypatch.setattr(schema, "REFERENCE_DATABASE_PATH", app_root / "reference_values.db")
    monkeypatch.setattr(schema, "SETTINGS_PATH", app_root / "app_settings.json")
    schema.init_database()
    return app_root


def test_sqlite_snapshot_includes_committed_wal_and_excludes_uncommitted(tmp_path):
    source = tmp_path / "source.db"
    snapshot = tmp_path / "snapshot.db"
    writer = sqlite3.connect(source)
    writer.execute("PRAGMA journal_mode=WAL")
    writer.execute("PRAGMA wal_autocheckpoint=0")
    writer.execute("CREATE TABLE records (value TEXT)")
    writer.execute("INSERT INTO records VALUES ('committed')")
    writer.commit()
    assert source.with_name(source.name + "-wal").exists()
    writer.execute("INSERT INTO records VALUES ('uncommitted')")

    _snapshot_database(source, snapshot)

    with sqlite3.connect(snapshot) as connection:
        assert connection.execute("SELECT value FROM records").fetchall() == [("committed",)]
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    writer.rollback()
    writer.close()


def test_full_backup_sanitizes_staged_state_and_collects_authoritative_assets(
    installation, tmp_path
):
    images_dir = tmp_path / "external-images"
    images_dir.mkdir()
    working = images_dir / "same.jpg"
    working.write_bytes(b"working")
    original = tmp_path / "external-originals" / "same.jpg"
    original.parent.mkdir()
    original.write_bytes(b"original")
    cache = installation / "cloud_cache" / "observations" / "cache.webp"
    cache.parent.mkdir(parents=True)
    cache.write_bytes(b"cache-secret")
    unrelated = images_dir / "unrelated.jpg"
    unrelated.write_bytes(b"not referenced")
    calibration = tmp_path / "calibration.tif"
    calibration.write_bytes(b"calibration")
    missing = tmp_path / "missing-original.raw"
    layouts = installation / "plate_layouts"
    layouts.mkdir()
    (layouts / "authored.mplate").write_text('{"layout": 1}', encoding="utf-8")
    (layouts / "authored.png").write_bytes(b"preview")

    schema.save_app_settings({
        "images_dir": str(images_dir),
        "cloud_access_token": "do-not-archive-token",
        "linked_cloud_user_id": "account-1",
        "ui_theme": "dark",
        "cloud_last_sync_status": "ok",
    })
    with sqlite3.connect(schema.get_reference_database_path()) as connection:
        reference_value_id = connection.execute(
            "INSERT INTO reference_values (genus, species, source) VALUES (?, ?, ?)",
            ("Amanita", "muscaria", "phase-2-test"),
        ).lastrowid
        connection.execute(
            "INSERT INTO reference_works (id, type, title, short_label) "
            "VALUES ('work-1', 'book', 'Reference work', 'Work')"
        )
        connection.execute(
            "INSERT INTO reference_taxon_treatments "
            "(id, reference_work_id, name_as_published) "
            "VALUES ('treatment-1', 'work-1', 'Amanita muscaria')"
        )
        connection.execute(
            "INSERT INTO reference_measurement_sets "
            "(id, taxon_treatment_id, character, data_kind, legacy_reference_value_id) "
            "VALUES ('set-1', 'treatment-1', 'spore_size', 'range', ?)",
            (reference_value_id,),
        )
        connection.commit()
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("INSERT INTO observations (date) VALUES ('2026-08-27')")
        observation_id = connection.execute("SELECT last_insert_rowid()").fetchone()[0]
        connection.execute(
            "INSERT INTO images (observation_id, filepath, original_filepath) VALUES (?, ?, ?)",
            (observation_id, str(working), str(original)),
        )
        connection.execute(
            "INSERT INTO images (observation_id, filepath, source_role, file_purpose) VALUES (?, ?, ?, ?)",
            (observation_id, str(cache), "cloud_recovery_cache", "cache"),
        )
        connection.execute(
            "INSERT INTO images (observation_id, filepath, original_filepath) VALUES (?, ?, ?)",
            (observation_id, str(working), str(missing)),
        )
        calibration_id = connection.execute(
            "INSERT INTO calibrations (calibration_uuid, objective_key, calibration_date, microns_per_pixel, image_filepath, measurements_json) VALUES (?, ?, ?, ?, ?, ?)",
            ("calibration-uuid", "100X", "2026-08-27", 0.1, str(calibration), json.dumps({"images": [{"path": str(calibration)}]})),
        ).lastrowid
        connection.execute(
            "INSERT INTO calibration_assets "
            "(asset_uuid, calibration_id, calibration_uuid, role, source_role, "
            "file_purpose, local_path) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("asset-uuid", calibration_id, "calibration-uuid", "source",
             "local_original", "authoritative", str(calibration)),
        )
        measurement_id = connection.execute(
            "INSERT INTO spore_measurements (image_id, length_um, width_um) "
            "VALUES (1, 10.5, 5.2)"
        ).lastrowid
        connection.execute(
            "INSERT INTO spore_annotations "
            "(image_id, measurement_id, spore_number, annotation_source) "
            "VALUES (1, ?, 1, 'manual')",
            (measurement_id,),
        )
        connection.execute(
            "INSERT INTO session_logs "
            "(observation_id, session_id, event_type, metadata_json) "
            "VALUES (?, 'session-1', 'capture', '{}')",
            (observation_id,),
        )
        connection.execute(
            "INSERT INTO image_tombstones "
            "(deleted_cloud_id, deleted_at, local_observation_id) "
            "VALUES ('deleted-image', '2026-08-27', ?)",
            (observation_id,),
        )
        connection.execute(
            "INSERT INTO observation_reference_uses "
            "(id, observation_id, reference_measurement_set_id, role, "
            "reference_revision, snapshot_json) "
            "VALUES ('use-1', ?, 'set-1', 'compared', 1, '{}')",
            (observation_id,),
        )
        connection.executemany(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            [
                ("inat_client_secret", "db-secret"),
                ("profile_name", "Mushroom User"),
                ("originals_dir", str(original.parent)),
            ],
        )
        connection.commit()

    destination = tmp_path / "backup.sporely"
    result = create_full_backup(
        destination,
        app_version="test-version",
        archive_id="fixed-archive-id",
        created_at="2026-08-27T12:00:00+00:00",
        source_platform="test-platform",
        qsettings_values={
            ("Sporely", "Sporely"): {"geometry/MainWindow": b"geometry"},
            ("Sporely", "SpeciesPlate"): {
                "ins_r": 88,
                "obs_1/label_top": "Author label",
                "unknown": "excluded",
            },
        },
    )

    assert result.path == destination
    validated = validate_full_backup(destination)
    assert validated == result.manifest
    assert "assets/originals/3/original.raw" not in result.warnings
    assert next(
        entry for entry in result.manifest.files
        if entry.path == "assets/originals/3/original.raw"
    ).status == "excluded_by_policy"
    with ZipFile(destination) as archive:
        names = archive.namelist()
        assert names[0] == "manifest.json"
        assert "assets/images/1/working.jpg" in names
        assert "assets/originals/1/original.jpg" in names
        assert "assets/images/2/working.webp" not in names
        assert "data/plate_layouts/authored.mplate" in names
        assert all("authored.png" not in name and "unrelated" not in name for name in names)
        app_settings = json.loads(archive.read("data/app_settings.json"))
        assert app_settings["settings"] == {
            "linked_cloud_user_id": "account-1", "ui_theme": "dark"
        }
        qsettings = json.loads(archive.read("data/qsettings.json"))
        plate = next(item for item in qsettings["namespaces"] if item["application"] == "SpeciesPlate")
        assert plate["values"] == {"ins_r": 88, "obs_1/label_top": "Author label"}
        staged_db = tmp_path / "staged.db"
        staged_db.write_bytes(archive.read("databases/mushrooms.db"))
        staged_reference = tmp_path / "staged-reference.db"
        staged_reference.write_bytes(archive.read("databases/reference_values.db"))
    with sqlite3.connect(staged_db) as connection:
        staged_settings = dict(connection.execute("SELECT key, value FROM settings"))
        assert connection.execute("SELECT COUNT(*) FROM spore_measurements").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM spore_annotations").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM session_logs").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM image_tombstones").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM calibration_assets").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM observation_reference_uses").fetchone()[0] == 1
    assert "inat_client_secret" not in staged_settings
    assert staged_settings["profile_name"] == "Mushroom User"
    assert staged_settings["originals_dir"] == str(original.parent)
    with sqlite3.connect(schema.get_database_path()) as connection:
        assert connection.execute(
            "SELECT value FROM settings WHERE key='inat_client_secret'"
        ).fetchone()[0] == "db-secret"
    with sqlite3.connect(staged_reference) as connection:
        assert connection.execute(
            "SELECT genus, species FROM reference_values WHERE source='phase-2-test'"
        ).fetchone() == ("Amanita", "muscaria")
        for table in (
            "reference_works", "reference_taxon_treatments",
            "reference_measurement_sets",
        ):
            assert connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 1
    assert b"do-not-archive-token" not in destination.read_bytes()
    assert b"db-secret" not in destination.read_bytes()
    assert b"cache-secret" not in destination.read_bytes()

    restore_full_backup(
        destination,
        app_version="test-version",
        safety_backup_path=tmp_path / "pre-round-trip.sporely",
        close_live=lambda: None,
        reopen_live=lambda: None,
    )
    with sqlite3.connect(staged_db) as expected, sqlite3.connect(
        schema.get_database_path()
    ) as restored:
        for table in MAIN_DATABASE_TABLES:
            if table == "settings":
                continue
            assert restored.execute(f"SELECT COUNT(*) FROM {table}").fetchone() == (
                expected.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            )
        assert restored.execute(
            "SELECT genus, species FROM observations ORDER BY id"
        ).fetchall() == expected.execute(
            "SELECT genus, species FROM observations ORDER BY id"
        ).fetchall()
        assert restored.execute(
            "SELECT length_um, width_um FROM spore_measurements ORDER BY id"
        ).fetchall() == expected.execute(
            "SELECT length_um, width_um FROM spore_measurements ORDER BY id"
        ).fetchall()
    with sqlite3.connect(staged_reference) as expected, sqlite3.connect(
        schema.get_reference_database_path()
    ) as restored:
        for table in REFERENCE_DATABASE_TABLES:
            assert restored.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall() == (
                expected.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
            )


def test_full_backup_excludes_orf_assets_by_policy(installation, tmp_path):
    working = tmp_path / "working.jpg"
    working.write_bytes(b"working")
    original = tmp_path / "camera-original.ORF"
    original.write_bytes(b"large-raw-original")
    calibration_original = tmp_path / "calibration-source.orf"
    calibration_original.write_bytes(b"calibration-raw-original")
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("INSERT INTO observations (date) VALUES ('2026-08-28')")
        connection.execute(
            "INSERT INTO images (observation_id, filepath, original_filepath) "
            "VALUES (1, ?, ?)",
            (str(working), str(original)),
        )
        connection.execute(
            "INSERT INTO calibrations "
            "(calibration_uuid, objective_key, calibration_date, microns_per_pixel, "
            "image_filepath) VALUES ('cal-orf', '100X', '2026-08-28', 0.1, ?)",
            (str(calibration_original),),
        )
        connection.commit()

    destination = tmp_path / "without-orfs.sporely"
    result = create_full_backup(destination, app_version="test", qsettings_values={})

    statuses = {entry.path: entry.status for entry in result.manifest.files}
    assert not any(is_raw_image_path(path) for path in result.warnings)
    assert statuses["assets/originals/1/original.orf"] == "excluded_by_policy"
    assert (
        statuses["assets/calibrations/records/1/working.orf"]
        == "excluded_by_policy"
    )
    with ZipFile(destination) as archive:
        assert "assets/images/1/working.jpg" in archive.namelist()
        assert not any(name.lower().endswith(".orf") for name in archive.namelist())


def test_full_backup_refuses_destination_without_required_free_space(
    installation, tmp_path, monkeypatch
):
    destination = tmp_path / "too-large.sporely"
    monkeypatch.setattr(
        shutil,
        "disk_usage",
        lambda path: shutil._ntuple_diskusage(total=1000, used=999, free=1),
    )

    with pytest.raises(FullBackupError, match="not enough free space"):
        create_full_backup(destination, app_version="test", qsettings_values={})

    assert not destination.exists()
    assert not list(tmp_path.glob(".too-large.sporely.*.tmp"))


def test_full_backup_reports_monotonic_phase_progress(installation, tmp_path):
    image = tmp_path / "image.jpg"
    image.write_bytes(b"image-bytes")
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("INSERT INTO observations (date) VALUES ('2026-08-28')")
        connection.execute(
            "INSERT INTO images (observation_id, filepath) VALUES (1, ?)",
            (str(image),),
        )
        connection.commit()
    progress = []

    create_full_backup(
        tmp_path / "progress.sporely",
        app_version="test",
        qsettings_values={},
        progress_callback=lambda phase, percent: progress.append((phase, percent)),
    )

    assert progress[0] == ("preparing", 0)
    assert {phase for phase, _percent in progress} >= {
        "checking_space", "hashing", "writing", "validating", "complete",
    }
    assert [percent for _phase, percent in progress] == sorted(
        percent for _phase, percent in progress
    )
    assert progress[-1] == ("complete", 100)


def test_unknown_setting_aborts_without_final_archive(installation, tmp_path):
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("INSERT INTO settings (key, value) VALUES ('unknown_new_key', 'value')")
        connection.commit()
    destination = tmp_path / "must-not-exist.sporely"
    with pytest.raises(FullBackupError, match="unclassified database setting"):
        create_full_backup(destination, app_version="test", qsettings_values={})
    assert not destination.exists()
    assert not list(tmp_path.glob(".must-not-exist.sporely.*.tmp"))


def test_full_backup_accepts_representative_production_settings(installation, tmp_path):
    production_settings = {
        "active_reporting_target": "artportalen_se",
        "artportalen_username": "example-user",
        "ingestion_hub_offset_seconds": "12.5",
        "measure_observation_scale_bar_value_micro_um_19": "10.0",
        "measure_observation_scale_bar_value_micro_um_5": "25.0",
        "resize_jpeg_quality": "90",
        "sporely_debug_cloud_plan_override": "server",
        "sync_full_resolution_originals": "1",
    }
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.executemany(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            production_settings.items(),
        )
        connection.commit()

    destination = tmp_path / "production-settings.sporely"
    create_full_backup(destination, app_version="test", qsettings_values={})

    with ZipFile(destination) as archive:
        staged = tmp_path / "production-settings.db"
        staged.write_bytes(archive.read("databases/mushrooms.db"))
    with sqlite3.connect(staged) as connection:
        archived = dict(connection.execute("SELECT key, value FROM settings"))
    assert {key: archived[key] for key in production_settings} == production_settings


def test_staged_database_does_not_retain_deleted_secret_bytes(installation, tmp_path):
    secret = "phase11-unique-secret-" + ("0123456789abcdef" * 128)
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute(
            "INSERT INTO settings (key, value) VALUES ('inat_client_secret', ?)",
            (secret,),
        )
        connection.commit()

    archive_path = tmp_path / "scrubbed.sporely"
    create_full_backup(
        archive_path, app_version="test", qsettings_values={}
    )
    extracted = tmp_path / "scrubbed.db"
    with ZipFile(archive_path) as archive:
        extracted.write_bytes(archive.read("databases/mushrooms.db"))

    assert secret.encode("utf-8") not in extracted.read_bytes()


def test_unknown_app_setting_aborts_and_preserves_existing_destination(installation, tmp_path):
    schema.save_app_settings({"future_unknown": "value"})
    destination = tmp_path / "existing.sporely"
    destination.write_bytes(b"previous-backup")
    with pytest.raises(FullBackupError, match="unclassified app setting"):
        create_full_backup(destination, app_version="test", qsettings_values={})
    assert destination.read_bytes() == b"previous-backup"


def test_validation_failure_is_atomic_and_cleans_temporary_zip(installation, tmp_path):
    destination = tmp_path / "existing.sporely"
    destination.write_bytes(b"previous-backup")

    def reject(_path):
        raise ArchiveValidationError("injected validation failure")

    with pytest.raises(FullBackupError, match="injected validation failure"):
        create_full_backup(
            destination, app_version="test", qsettings_values={}, validate=reject
        )
    assert destination.read_bytes() == b"previous-backup"
    assert not list(tmp_path.glob(".existing.sporely.*.tmp"))


def test_validator_detects_corrupt_member(installation, tmp_path):
    destination = tmp_path / "backup.sporely"
    create_full_backup(destination, app_version="test", qsettings_values={})
    corrupt = tmp_path / "corrupt.sporely"
    with ZipFile(destination) as source, ZipFile(corrupt, "w") as target:
        for info in source.infolist():
            payload = source.read(info.filename)
            if info.filename == "data/app_settings.json":
                payload += b"corrupt"
            target.writestr(info, payload)
    with pytest.raises(ArchiveValidationError, match="size or checksum mismatch"):
        validate_full_backup(corrupt)
