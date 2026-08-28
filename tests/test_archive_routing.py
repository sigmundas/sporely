from __future__ import annotations

from pathlib import Path
import sqlite3
from zipfile import ZipFile

import pytest

from utils.archive.manifest import build_manifest
from utils.archive.routing import ArchiveRoute, ArchiveRoutingError, classify_archive


def _current_archive(path: Path, mode: str) -> Path:
    manifest = build_manifest(
        mode=mode,
        archive_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        created_at="2026-08-27T12:00:00+00:00",
        app_version="test",
        source_platform="test",
        contents={},
        files=[],
    )
    with ZipFile(path, "w") as archive:
        archive.writestr("manifest.json", manifest.to_json_bytes())
    return path


def test_current_manifest_routes_to_new_importers_regardless_of_suffix(tmp_path):
    assert classify_archive(_current_archive(tmp_path / "portable.zip", "portable_observations")) is ArchiveRoute.PORTABLE_OBSERVATIONS
    assert classify_archive(_current_archive(tmp_path / "backup.sporely", "full_backup")) is ArchiveRoute.FULL_BACKUP


@pytest.mark.parametrize("suffix", [".zip", ".sporely"])
def test_legacy_signature_routes_to_compatibility_importer_regardless_of_suffix(tmp_path, suffix):
    path = tmp_path / f"legacy{suffix}"
    database = tmp_path / f"legacy-{suffix[1:]}.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE observations (id INTEGER PRIMARY KEY)")
    with ZipFile(path, "w") as archive:
        archive.write(database, "mushrooms.db")
    assert classify_archive(path) is ArchiveRoute.LEGACY_DATA_PACKAGE


def test_mixed_current_and_legacy_signatures_are_rejected(tmp_path):
    path = _current_archive(tmp_path / "ambiguous.sporely", "portable_observations")
    with ZipFile(path, "a") as archive:
        archive.writestr("mushrooms.db", b"legacy")
    with pytest.raises(ArchiveRoutingError, match="mixes current and legacy"):
        classify_archive(path)


def test_malformed_legacy_database_is_rejected_before_routing(tmp_path):
    path = tmp_path / "malformed-legacy.zip"
    with ZipFile(path, "w") as archive:
        archive.writestr("mushrooms.db", b"not sqlite")
    with pytest.raises(ArchiveRoutingError, match="invalid or unsupported"):
        classify_archive(path)


@pytest.mark.parametrize("payload", [b"not a zip", None])
def test_malformed_or_unknown_archives_are_rejected(tmp_path, payload):
    path = tmp_path / "invalid.zip"
    if payload is None:
        with ZipFile(path, "w") as archive:
            archive.writestr("unrelated.txt", b"data")
    else:
        path.write_bytes(payload)
    with pytest.raises(ArchiveRoutingError):
        classify_archive(path)


def test_truncated_zip_is_rejected(tmp_path):
    path = _current_archive(tmp_path / "truncated.sporely", "full_backup")
    payload = path.read_bytes()
    path.write_bytes(payload[:-12])

    with pytest.raises(ArchiveRoutingError):
        classify_archive(path)
