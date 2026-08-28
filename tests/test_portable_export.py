import json
import shutil
import sqlite3
from pathlib import Path
from zipfile import ZipFile

import pytest

from database import schema
from utils.archive.portable_export import PortableExportError, export_observations
from utils.archive.portable_import import preview_portable_archive
from utils.archive.validation import validate_portable_observations
from utils.raw_detection import is_raw_image_path


@pytest.fixture
def portable_installation(monkeypatch, tmp_path):
    app_root = tmp_path / "app"
    app_root.mkdir()
    monkeypatch.setattr(schema, "_app_dir", app_root)
    monkeypatch.setattr(schema, "DATABASE_PATH", app_root / "mushrooms.db")
    monkeypatch.setattr(schema, "REFERENCE_DATABASE_PATH", app_root / "reference_values.db")
    monkeypatch.setattr(schema, "SETTINGS_PATH", app_root / "app_settings.json")
    schema.init_database()
    return app_root


def _ids(connection: sqlite3.Connection, table: str) -> set[object]:
    return {row[0] for row in connection.execute(f"SELECT id FROM {table}")}


def test_portable_export_contains_only_selected_dependency_closure(
    portable_installation, tmp_path
):
    images_dir = tmp_path / "images"
    images_dir.mkdir()
    selected_image = images_dir / "selected.jpg"
    selected_image.write_bytes(b"selected-working")
    selected_original = tmp_path / "originals" / "selected.ORF"
    selected_original.parent.mkdir()
    selected_original.write_bytes(b"selected-original")
    cache_image = portable_installation / "cloud_cache" / "observations" / "cached.webp"
    cache_image.parent.mkdir(parents=True)
    cache_image.write_bytes(b"cloud-recovery-bytes")
    unrelated_file = images_dir / "unrelated-neighbor.jpg"
    unrelated_file.write_bytes(b"unrelated-neighbor")
    other_image = images_dir / "other.jpg"
    other_image.write_bytes(b"other-observation")
    calibration_image = tmp_path / "calibration-x.tif"
    calibration_image.write_bytes(b"calibration-x")
    calibration_asset = tmp_path / "calibration-x-original.raw"
    calibration_asset.write_bytes(b"calibration-x-asset")
    calibration_cache = tmp_path / "calibration-cache.tif"
    calibration_cache.write_bytes(b"calibration-cache-bytes")
    unrelated_calibration = tmp_path / "calibration-y.tif"
    unrelated_calibration.write_bytes(b"calibration-y")

    schema.save_app_settings({"images_dir": str(images_dir)})
    schema.get_objectives_path().write_text(json.dumps({
        "100X": {"name": "100X", "magnification": 100},
        "40X": {"name": "40X", "magnification": 40},
    }), encoding="utf-8")

    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.executemany(
            "INSERT INTO observations (id, date, notes) VALUES (?, ?, ?)",
            [
                (1, "2026-08-01", "A"),
                (2, "2026-08-02", "UNRELATED_OBSERVATION_SENTINEL_B"),
                (3, "2026-08-03", "UNRELATED_OBSERVATION_SENTINEL_C"),
            ],
        )
        connection.executemany(
            "INSERT INTO calibrations "
            "(id, calibration_uuid, objective_key, calibration_date, microns_per_pixel, image_filepath) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                (10, "cal-x", "100X", "2026-08-01", 0.1, str(calibration_image)),
                (20, "cal-y", "40X", "2026-08-02", 0.2, str(unrelated_calibration)),
            ],
        )
        connection.executemany(
            "INSERT INTO images "
            "(id, observation_id, filepath, original_filepath, objective_name, calibration_id, source_role, file_purpose) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (101, 1, str(selected_image), str(selected_original), "100X", 10, "local_original", "authoritative"),
                (102, 1, str(cache_image), None, "100X", 10, "cloud_recovery_cache", "cache"),
                (201, 2, str(other_image), None, "40X", 20, "local_original", "authoritative"),
                (301, 3, str(other_image), None, None, None, "local_original", "authoritative"),
            ],
        )
        connection.executemany(
            "INSERT INTO spore_measurements (id, image_id, length_um, width_um) VALUES (?, ?, ?, ?)",
            [(1001, 101, 10.0, 5.0), (2001, 201, 11.0, 6.0)],
        )
        connection.executemany(
            "INSERT INTO spore_annotations (id, image_id, measurement_id, spore_number) VALUES (?, ?, ?, ?)",
            [(1101, 101, 1001, 1), (2101, 201, 2001, 1)],
        )
        connection.executemany(
            "INSERT INTO session_logs (id, observation_id, session_id, event_type) VALUES (?, ?, ?, ?)",
            [(1201, 1, "shared-session", "selected"), (2201, 2, "shared-session", "other")],
        )
        connection.executemany(
            "INSERT INTO calibration_assets "
            "(id, asset_uuid, calibration_id, calibration_uuid, role, source_role, file_purpose, local_path) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (1301, "asset-x", 10, "cal-x", "source", "local_original", "authoritative", str(calibration_asset)),
                (1302, "asset-cache", 10, "cal-x", "source", "cloud_recovery_cache", "cache", str(calibration_cache)),
                (2301, "asset-y", 20, "cal-y", "source", "local_original", "authoritative", str(unrelated_calibration)),
            ],
        )
        connection.executemany(
            "INSERT INTO observation_reference_uses "
            "(id, observation_id, reference_measurement_set_id, role, reference_revision, snapshot_json) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                ("use-a", 1, "set-a", "compared", 1, "{}"),
                ("use-b", 2, "set-b", "compared", 1, "{}"),
            ],
        )
        connection.execute(
            "INSERT INTO settings (key, value) VALUES "
            "('profile_name', 'EXCLUDED_INSTALLATION_SETTING_SENTINEL')"
        )
        connection.execute(
            "INSERT INTO image_tombstones (deleted_cloud_id, deleted_at) VALUES ('gone', '2026-08-01')"
        )
        connection.execute(
            "INSERT INTO thumbnails (image_id, size_preset, filepath) VALUES (101, 'small', 'thumb.jpg')"
        )
        connection.execute(
            "INSERT INTO portable_import_provenance "
            "(archive_id, source_item_type, source_item_id, destination_item_id, source_content_sha256) "
            "VALUES ('prior-archive', 'observation', '77', '1', ?)",
            ("a" * 64,),
        )
        connection.commit()

    with sqlite3.connect(schema.get_reference_database_path()) as connection:
        connection.executemany(
            "INSERT INTO reference_values (id, genus, species, source) VALUES (?, ?, ?, ?)",
            [(501, "Selected", "reference", "test"), (502, "Other", "reference", "test")],
        )
        connection.executemany(
            "INSERT INTO reference_works (id, type, title, short_label) VALUES (?, ?, ?, ?)",
            [
                ("work-a", "book", "Selected work", "A"),
                ("work-b", "book", "UNRELATED_REFERENCE_SENTINEL", "B"),
            ],
        )
        connection.executemany(
            "INSERT INTO reference_taxon_treatments "
            "(id, reference_work_id, name_as_published) VALUES (?, ?, ?)",
            [("treatment-a", "work-a", "Selected taxon"), ("treatment-b", "work-b", "Other taxon")],
        )
        connection.executemany(
            "INSERT INTO reference_measurement_sets "
            "(id, taxon_treatment_id, character, data_kind, legacy_reference_value_id) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                ("set-a", "treatment-a", "spore_size", "range", 501),
                ("set-b", "treatment-b", "spore_size", "range", 502),
            ],
        )
        connection.commit()

    destination = tmp_path / "selected.sporely"
    result = export_observations(
        {1}, destination, app_version="test", archive_id="portable-id",
        created_at="2026-08-27T12:00:00+00:00", source_platform="test-platform",
    )
    manifest = validate_portable_observations(destination)
    assert manifest == result.manifest
    assert manifest.mode == "portable_observations"
    assert manifest.identity_policy == "portable"
    assert manifest.contents == {
        "annotations": 1,
        "calibration_assets": 2,
        "calibrations": 1,
        "images": 2,
        "measurements": 1,
        "observation_reference_uses": 1,
        "observations": 1,
        "reference_measurement_sets": 1,
        "reference_taxon_treatments": 1,
        "reference_values": 1,
        "reference_works": 1,
        "session_logs": 1,
    }

    with ZipFile(destination) as archive:
        names = archive.namelist()
        included = {entry.path for entry in manifest.files if entry.status == "included"}
        assert names[0] == "manifest.json"
        assert set(names) - {"manifest.json"} == included
        assert "portable/assets/images/101/working.jpg" in names
        assert "portable/assets/originals/101/original.orf" not in names
        assert "portable/assets/calibrations/records/10/working.tif" in names
        assert "portable/assets/calibrations/assets/1301/local.raw" not in names
        assert "portable/assets/images/102/working.webp" not in names
        assert "portable/assets/calibrations/assets/1302/local.tif" not in names
        assert all("201" not in name and "2301" not in name for name in names)
        assert b"unrelated-neighbor" not in destination.read_bytes()
        assert b"cloud-recovery-bytes" not in destination.read_bytes()
        assert b"calibration-cache-bytes" not in destination.read_bytes()
        assert json.loads(archive.read("portable/objectives.json")) == {
            "100X": {"magnification": 100, "name": "100X"}
        }
        main_db = tmp_path / "portable-main.db"
        main_db.write_bytes(archive.read("portable/mushrooms.db"))
        reference_db = tmp_path / "portable-reference.db"
        reference_db.write_bytes(archive.read("portable/reference_values.db"))

    statuses = {entry.path: entry.status for entry in manifest.files}
    assert statuses["portable/assets/originals/101/original.orf"] == "excluded_by_policy"
    assert (
        statuses["portable/assets/calibrations/assets/1301/local.raw"]
        == "excluded_by_policy"
    )
    assert not result.warnings

    with sqlite3.connect(main_db) as connection:
        assert _ids(connection, "observations") == {1}
        assert _ids(connection, "images") == {101, 102}
        assert _ids(connection, "spore_measurements") == {1001}
        assert _ids(connection, "spore_annotations") == {1101}
        assert _ids(connection, "session_logs") == {1201}
        assert _ids(connection, "calibrations") == {10}
        assert _ids(connection, "calibration_assets") == {1301, 1302}
        assert _ids(connection, "observation_reference_uses") == {"use-a"}
        assert connection.execute("SELECT COUNT(*) FROM settings").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM thumbnails").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM image_tombstones").fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM portable_import_provenance"
        ).fetchone()[0] == 0
    assert b"UNRELATED_OBSERVATION_SENTINEL" not in main_db.read_bytes()
    assert b"EXCLUDED_INSTALLATION_SETTING_SENTINEL" not in main_db.read_bytes()
    with sqlite3.connect(reference_db) as connection:
        assert _ids(connection, "reference_values") == {501}
        assert _ids(connection, "reference_works") == {"work-a"}
        assert _ids(connection, "reference_taxon_treatments") == {"treatment-a"}
        assert _ids(connection, "reference_measurement_sets") == {"set-a"}
    assert b"UNRELATED_REFERENCE_SENTINEL" not in reference_db.read_bytes()


def test_portable_export_requires_explicit_existing_roots(portable_installation, tmp_path):
    with pytest.raises(PortableExportError, match="at least one"):
        export_observations(set(), tmp_path / "empty.sporely", app_version="test")
    with pytest.raises(PortableExportError, match="do not exist: 999"):
        export_observations({999}, tmp_path / "missing.sporely", app_version="test")
    assert not (tmp_path / "empty.sporely").exists()
    assert not (tmp_path / "missing.sporely").exists()


def test_cache_provenance_blocks_calibration_record_path(portable_installation, tmp_path):
    images_dir = tmp_path / "images"
    images_dir.mkdir()
    selected = images_dir / "selected.jpg"
    selected.write_bytes(b"selected")
    recovery = tmp_path / "cloud-recovery-calibration.tif"
    recovery.write_bytes(b"CLOUD_CALIBRATION_SENTINEL")
    schema.save_app_settings({"images_dir": str(images_dir)})
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("INSERT INTO observations (id, date) VALUES (1, '2026-08-27')")
        connection.execute(
            "INSERT INTO calibrations "
            "(id, calibration_uuid, objective_key, calibration_date, microns_per_pixel, image_filepath) "
            "VALUES (10, 'cal-x', '100X', '2026-08-27', 0.1, ?)",
            (str(recovery),),
        )
        connection.execute(
            "INSERT INTO images (id, observation_id, filepath, calibration_id) VALUES (101, 1, ?, 10)",
            (str(selected),),
        )
        connection.execute(
            "INSERT INTO calibration_assets "
            "(asset_uuid, calibration_id, calibration_uuid, role, source_role, file_purpose, local_path) "
            "VALUES ('cache', 10, 'cal-x', 'source', 'cloud_recovery_cache', 'cache', ?)",
            (str(recovery),),
        )
        connection.commit()

    destination = tmp_path / "cache-safe.sporely"
    result = export_observations({1}, destination, app_version="test")
    record = next(
        entry for entry in result.manifest.files
        if entry.path == "portable/assets/calibrations/records/10/working.tif"
    )
    assert record.status == "excluded_by_policy"
    assert b"CLOUD_CALIBRATION_SENTINEL" not in destination.read_bytes()


def test_repeated_missing_source_path_is_reported_once(portable_installation, tmp_path):
    images_dir = tmp_path / "images"
    images_dir.mkdir()
    selected = images_dir / "selected.jpg"
    selected.write_bytes(b"selected")
    missing = tmp_path / "expired-calibration-source.jpg"
    schema.save_app_settings({"images_dir": str(images_dir)})
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("INSERT INTO observations (id, date) VALUES (1, '2026-08-28')")
        connection.execute(
            "INSERT INTO calibrations "
            "(id, calibration_uuid, objective_key, calibration_date, microns_per_pixel, "
            "image_filepath, measurements_json) VALUES (10, 'cal-x', '100X', "
            "'2026-08-28', 0.1, ?, ?)",
            (
                str(missing),
                json.dumps({
                    "images": [{
                        "path": str(missing),
                        "source_path": str(missing),
                    }]
                }),
            ),
        )
        connection.execute(
            "INSERT INTO images (id, observation_id, filepath, calibration_id) "
            "VALUES (101, 1, ?, 10)",
            (str(selected),),
        )
        connection.commit()

    result = export_observations(
        {1}, tmp_path / "deduplicated-warning.sporely", app_version="test"
    )

    missing_entries = [
        entry for entry in result.manifest.files
        if entry.status == "missing_at_source"
    ]
    assert len(missing_entries) == 3
    assert len(result.warnings) == 1


def test_portable_export_excludes_raw_paths_from_every_asset_slot(
    portable_installation, tmp_path
):
    paths = {
        "image_working": tmp_path / "image.NEF",
        "image_original": tmp_path / "image-original.ARW",
        "calibration": tmp_path / "calibration.CR3",
        "measurement_path": tmp_path / "measurement.DNG",
        "measurement_companion": tmp_path / "measurement-companion.RAF",
        "asset_local": tmp_path / "asset-local.RAW",
        "asset_original": tmp_path / "asset-original.RWL",
        "asset_source": tmp_path / "asset-source.ORF",
        "asset_companion": tmp_path / "asset-companion.NRW",
        "asset_working": tmp_path / "asset-working.TIF",
        "ordinary_image": tmp_path / "ordinary.JPG",
    }
    for name, path in paths.items():
        path.write_bytes(name.encode("ascii"))
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("INSERT INTO observations (id, date) VALUES (1, '2026-08-28')")
        connection.execute(
            "INSERT INTO calibrations "
            "(id, calibration_uuid, objective_key, calibration_date, microns_per_pixel, "
            "image_filepath, measurements_json) VALUES (10, 'cal-raw', '100X', "
            "'2026-08-28', 0.1, ?, ?)",
            (
                str(paths["calibration"]),
                json.dumps({"images": [{
                    "path": str(paths["measurement_path"]),
                    "companion_paths": [str(paths["measurement_companion"])],
                }]}),
            ),
        )
        connection.executemany(
            "INSERT INTO images (id, observation_id, filepath, original_filepath, calibration_id) "
            "VALUES (?, 1, ?, ?, 10)",
            [
                (101, str(paths["image_working"]), str(paths["image_original"])),
                (102, str(paths["ordinary_image"]), None),
            ],
        )
        connection.execute(
            "INSERT INTO calibration_assets "
            "(id, asset_uuid, calibration_id, calibration_uuid, role, local_path, "
            "original_path, metadata_json) VALUES (1301, 'asset-raw', 10, 'cal-raw', "
            "'source', ?, ?, ?)",
            (
                str(paths["asset_local"]),
                str(paths["asset_original"]),
                json.dumps({
                    "source_path": str(paths["asset_source"]),
                    "working_path": str(paths["asset_working"]),
                    "companion_paths": [str(paths["asset_companion"])],
                }),
            ),
        )
        connection.commit()

    destination = tmp_path / "raw-policy.sporely"
    result = export_observations({1}, destination, app_version="test")
    statuses = {entry.path: entry.status for entry in result.manifest.files}
    raw_entries = {
        path: status for path, status in statuses.items()
        if is_raw_image_path(path)
    }

    assert raw_entries
    assert set(raw_entries.values()) == {"excluded_by_policy"}
    assert statuses["portable/assets/images/102/working.jpg"] == "included"
    assert (
        statuses["portable/assets/calibrations/assets/1301/metadata-working_path.tif"]
        == "included"
    )
    assert not result.warnings
    with ZipFile(destination) as archive:
        assert not any(is_raw_image_path(name) for name in archive.namelist())
        assert "portable/assets/images/102/working.jpg" in archive.namelist()
        assert (
            "portable/assets/calibrations/assets/1301/metadata-working_path.tif"
            in archive.namelist()
        )
    assert preview_portable_archive(destination).observations[0].observation_id == 1


def test_portable_export_refuses_destination_without_required_free_space(
    portable_installation, tmp_path, monkeypatch
):
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("INSERT INTO observations (id, date) VALUES (1, '2026-08-28')")
        connection.commit()
    monkeypatch.setattr(
        shutil,
        "disk_usage",
        lambda path: shutil._ntuple_diskusage(total=1000, used=999, free=1),
    )
    destination = tmp_path / "too-large.sporely"

    with pytest.raises(PortableExportError, match="not enough free space"):
        export_observations({1}, destination, app_version="test")

    assert not destination.exists()
    assert not list(tmp_path.glob(".too-large.sporely.*.tmp"))


def test_portable_export_reports_monotonic_phase_progress(
    portable_installation, tmp_path
):
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("INSERT INTO observations (id, date) VALUES (1, '2026-08-28')")
        connection.commit()
    progress = []

    export_observations(
        {1},
        tmp_path / "progress.sporely",
        app_version="test",
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


def test_conflicting_calibration_asset_identity_fails_closed(portable_installation, tmp_path):
    selected = tmp_path / "selected.jpg"
    selected.write_bytes(b"selected")
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("INSERT INTO observations (id, date) VALUES (1, '2026-08-27')")
        connection.executemany(
            "INSERT INTO calibrations "
            "(id, calibration_uuid, objective_key, calibration_date, microns_per_pixel) "
            "VALUES (?, ?, ?, '2026-08-27', 0.1)",
            [(10, "cal-x", "100X"), (20, "cal-y", "40X")],
        )
        connection.execute(
            "INSERT INTO images (id, observation_id, filepath, calibration_id) VALUES (101, 1, ?, 10)",
            (str(selected),),
        )
        connection.execute(
            "INSERT INTO calibration_assets "
            "(asset_uuid, calibration_id, calibration_uuid, role) "
            "VALUES ('conflict', 20, 'cal-x', 'source')"
        )
        connection.commit()

    with pytest.raises(PortableExportError, match="conflicting calibration identities"):
        export_observations({1}, tmp_path / "conflict.sporely", app_version="test")
