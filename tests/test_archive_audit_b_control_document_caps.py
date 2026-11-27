"""F08 (2026-11-26 security audit): archive control documents are capped
before their bodies are read."""

from __future__ import annotations

import io
import json
import zipfile
from types import SimpleNamespace
from zipfile import ZipFile

import pytest

from database import schema
from utils.archive import paths
from utils.archive.full_backup import create_full_backup
from utils.archive.routing import ArchiveRoutingError, classify_archive
from utils.archive.validation import (
    ArchiveValidationError,
    validate_full_backup,
    validate_portable_observations,
)


@pytest.fixture
def opened_members(monkeypatch):
    opened: list[str] = []
    original_open = zipfile.ZipFile.open

    def recording_open(self, name, mode="r", *args, **kwargs):
        if mode == "r":
            opened.append(name if isinstance(name, str) else name.filename)
        return original_open(self, name, mode, *args, **kwargs)

    def recording_read(self, name, *args, **kwargs):
        opened.append(name if isinstance(name, str) else name.filename)
        with original_open(self, name, "r") as source:
            return source.read()

    monkeypatch.setattr(zipfile.ZipFile, "open", recording_open)
    monkeypatch.setattr(zipfile.ZipFile, "read", recording_read)
    return opened


def _oversized_manifest_archive(tmp_path, mode):
    archive = tmp_path / f"{mode}.sporely"
    manifest = {"format_version": 1, "mode": mode, "padding": "x" * 8192}
    with ZipFile(archive, "w") as target:
        target.writestr("manifest.json", json.dumps(manifest))
    return archive


def test_routing_rejects_oversized_manifest_unread(tmp_path, monkeypatch, opened_members):
    monkeypatch.setattr(paths, "MAX_MANIFEST_BYTES", 1024, raising=False)
    archive = _oversized_manifest_archive(tmp_path, "full_backup")

    with pytest.raises(ArchiveRoutingError, match="too large"):
        classify_archive(archive)
    assert "manifest.json" not in opened_members


@pytest.mark.parametrize(
    ("validator", "mode"),
    [(validate_full_backup, "full_backup"), (validate_portable_observations, "portable_observations")],
)
def test_validation_rejects_oversized_manifest_unread(
    tmp_path, monkeypatch, opened_members, validator, mode
):
    monkeypatch.setattr(paths, "MAX_MANIFEST_BYTES", 1024, raising=False)
    archive = _oversized_manifest_archive(tmp_path, mode)

    with pytest.raises(ArchiveValidationError, match="too large"):
        validator(archive)
    assert "manifest.json" not in opened_members


def test_validation_rejects_oversized_settings_document_unread(
    monkeypatch, tmp_path, opened_members
):
    root = tmp_path / "installation"
    root.mkdir()
    monkeypatch.setattr(schema, "_app_dir", root)
    monkeypatch.setattr(schema, "DATABASE_PATH", root / "mushrooms.db")
    monkeypatch.setattr(schema, "REFERENCE_DATABASE_PATH", root / "reference_values.db")
    monkeypatch.setattr(schema, "SETTINGS_PATH", root / "app_settings.json")
    schema.init_database()
    schema.save_app_settings({"ui_theme": "x" * 4096})
    archive = tmp_path / "backup.sporely"
    create_full_backup(archive, app_version="test", qsettings_values={})
    opened_members.clear()
    monkeypatch.setattr(paths, "MAX_CONTROL_DOCUMENT_BYTES", 1024, raising=False)

    with pytest.raises(ArchiveValidationError, match="too large"):
        validate_full_backup(archive)
    # The checksum pass streams every member once; the size cap must stop the
    # second, whole-document read of the settings JSON.
    assert opened_members.count("data/app_settings.json") == 1


def test_bounded_read_ignores_a_wrong_declared_size():
    class _UnderDeclaringArchive:
        """Declares one byte but streams far more."""

        def __init__(self):
            self.served = 0

        def getinfo(self, name):
            return SimpleNamespace(filename=name, file_size=1)

        def open(self, info, mode="r"):
            archive = self

            class _Stream(io.BytesIO):
                def read(self, size=-1):
                    chunk = super().read(size)
                    archive.served += len(chunk)
                    return chunk

            return _Stream(b"y" * (4 * 1024 * 1024))

    archive = _UnderDeclaringArchive()
    with pytest.raises(paths.UnsafeArchivePathError, match="too large"):
        paths.read_control_member(archive, "manifest.json", limit=100)
    assert archive.served <= 101
