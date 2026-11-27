"""F04b (2026-11-26 security audit): backups and portable exports must not
follow file paths from calibration metadata JSON out of the library."""

import json
import sqlite3
from zipfile import ZipFile

import pytest

from database import schema
from utils.archive.full_backup import create_full_backup
from utils.archive.full_restore import restore_full_backup
from utils.archive.portable_export import export_observations


OUTSIDE_SENTINEL = b"OUTSIDE_LIBRARY_SENTINEL"


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


def _library(tmp_path):
    images_dir = tmp_path / "library-images"
    (images_dir / "calibrations" / "100X").mkdir(parents=True)
    originals_dir = tmp_path / "library-originals"
    originals_dir.mkdir()
    camera_dir = tmp_path / "camera-card"
    camera_dir.mkdir()
    outside_dir = tmp_path / "private-documents"
    outside_dir.mkdir()

    files = {
        "selected": images_dir / "selected.jpg",
        "calibration_working": images_dir / "calibrations" / "100X" / "cal.jpg",
        "metadata_in_images": images_dir / "calibrations" / "100X" / "cal-meta.jpg",
        "metadata_in_originals": originals_dir / "cal-original.jpg",
        "camera_source": camera_dir / "P0001.JPG",
        "camera_companion": camera_dir / "P0001.xmp",
        "outside_record": outside_dir / "record-secret.txt",
        "outside_asset": outside_dir / "asset-secret.txt",
        "outside_companion": outside_dir / "id_ed25519",
    }
    for name, path in files.items():
        payload = OUTSIDE_SENTINEL + name.encode() if name.startswith("outside") else name.encode()
        path.write_bytes(payload)
    schema.save_app_settings({"images_dir": str(images_dir)})

    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute(
            "INSERT INTO settings (key, value) VALUES ('originals_dir', ?)", (str(originals_dir),)
        )
        connection.execute("INSERT INTO observations (id, date) VALUES (1, '2026-08-01')")
        connection.execute(
            "INSERT INTO calibrations (id, calibration_uuid, objective_key, calibration_date, "
            "microns_per_pixel, image_filepath, measurements_json) VALUES (10, 'cal-x', '100X', "
            "'2026-08-01', 0.1, ?, ?)",
            (
                str(files["calibration_working"]),
                json.dumps({"images": [{
                    "path": str(files["metadata_in_images"]),
                    "original_path": str(files["metadata_in_originals"]),
                    # Equal to a calibration asset path column: legitimate.
                    "source_path": str(files["camera_source"]),
                    "selected_path": str(files["outside_record"]),
                    "companion_paths": [str(files["outside_companion"])],
                }]}),
            ),
        )
        connection.execute(
            "INSERT INTO images (id, observation_id, filepath, calibration_id) VALUES (101, 1, ?, 10)",
            (str(files["selected"]),),
        )
        connection.execute(
            "INSERT INTO calibration_assets (id, asset_uuid, calibration_id, calibration_uuid, role, "
            "source_role, file_purpose, local_path, metadata_json) VALUES "
            "(1301, 'asset-x', 10, 'cal-x', 'source_photo', 'import_source', 'calibration', ?, ?)",
            (
                str(files["camera_source"]),
                json.dumps({
                    "source_path": str(files["outside_asset"]),
                    "working_path": str(files["camera_source"]),
                    # Same-stem companion beside the asset's own file.
                    "companion_paths": [str(files["camera_companion"])],
                }),
            ),
        )
    return files


def _archive_bytes(path):
    with ZipFile(path) as archive:
        return {name: archive.read(name) for name in archive.namelist()}


def _assert_policy(members, prefix):
    for name, data in members.items():
        assert OUTSIDE_SENTINEL not in data, name
    expected = {
        f"{prefix}records/10/metadata-0-path.jpg": b"metadata_in_images",
        f"{prefix}records/10/metadata-0-original_path.jpg": b"metadata_in_originals",
        f"{prefix}records/10/metadata-0-source_path.jpg": b"camera_source",
        f"{prefix}assets/1301/metadata-working_path.jpg": b"camera_source",
        f"{prefix}assets/1301/metadata-companion-0.xmp": b"camera_companion",
    }
    for name, data in expected.items():
        assert members.get(name) == data, name


def test_full_backup_skips_metadata_paths_outside_library(installation, tmp_path):
    _library(tmp_path)
    destination = tmp_path / "backup.sporely"

    result = create_full_backup(destination, app_version="test", qsettings_values={})

    _assert_policy(_archive_bytes(destination), "assets/calibrations/")
    statuses = {entry.path: entry.status for entry in result.manifest.files}
    assert statuses["assets/calibrations/records/10/metadata-0-selected_path.txt"] == "missing_at_source"
    assert statuses["assets/calibrations/assets/1301/metadata-source_path.txt"] == "missing_at_source"

    # The backup still restores; the rejected metadata paths are cleared.
    restore_full_backup(
        destination, app_version="test", safety_backup_path=tmp_path / "safety.sporely",
        close_live=lambda: None, reopen_live=lambda: None,
    )
    with sqlite3.connect(schema.get_database_path()) as connection:
        record = json.loads(connection.execute(
            "SELECT measurements_json FROM calibrations WHERE id=10"
        ).fetchone()[0])
    assert record["images"][0]["selected_path"] is None
    assert record["images"][0]["companion_paths"] == [None]
    assert record["images"][0]["path"]


def test_portable_export_skips_metadata_paths_outside_library(installation, tmp_path):
    _library(tmp_path)
    destination = tmp_path / "portable.sporely"

    result = export_observations({1}, destination, app_version="test")

    _assert_policy(_archive_bytes(destination), "portable/assets/calibrations/")
    statuses = {entry.path: entry.status for entry in result.manifest.files}
    assert (
        statuses["portable/assets/calibrations/records/10/metadata-0-selected_path.txt"]
        == "missing_at_source"
    )


def test_metadata_path_spelled_with_other_case_stays_inside_library(installation, tmp_path):
    images_dir = tmp_path / "Library-Images"
    images_dir.mkdir()
    sidecar = images_dir / "cal.xmp"
    sidecar.write_bytes(b"in-library-sidecar")
    other_spelling = tmp_path / "library-images" / "cal.xmp"
    if not other_spelling.exists():
        pytest.skip("case-sensitive file system")
    schema.save_app_settings({"images_dir": str(images_dir)})
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute(
            "INSERT INTO calibrations (id, calibration_uuid, objective_key, calibration_date, "
            "microns_per_pixel, measurements_json) VALUES (10, 'cal-x', '100X', '2026-08-01', 0.1, ?)",
            (json.dumps({"images": [{"companion_paths": [str(other_spelling)]}]}),),
        )
    destination = tmp_path / "backup.sporely"

    create_full_backup(destination, app_version="test", qsettings_values={})

    members = _archive_bytes(destination)
    assert members["assets/calibrations/records/10/metadata-0-companion_0.xmp"] == b"in-library-sidecar"
