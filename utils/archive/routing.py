"""Strict routing for current and legacy Sporely archive containers."""

from __future__ import annotations

from enum import Enum
from pathlib import Path
import shutil
import tempfile
import sqlite3
from zipfile import BadZipFile, ZipFile

from utils.archive.manifest import ArchiveManifest, ManifestError
from utils.archive.paths import UnsafeArchivePathError, validate_zip_entries
from utils.archive.validation import ArchiveValidationError, verify_sqlite_integrity


class ArchiveRoute(str, Enum):
    PORTABLE_OBSERVATIONS = "portable_observations"
    FULL_BACKUP = "full_backup"
    LEGACY_DATA_PACKAGE = "legacy_data_package"


class ArchiveRoutingError(ValueError):
    """Raised when an archive cannot be routed without ambiguity."""


_LEGACY_DATABASE_MEMBERS = {"mushrooms.db", "reference_values.db"}


def classify_archive(path: str | Path) -> ArchiveRoute:
    """Classify a ZIP by its validated internal signature, never its suffix."""
    try:
        with ZipFile(path, "r") as archive:
            names = validate_zip_entries(archive.infolist())
            members = set(names)
            has_manifest = "manifest.json" in members
            has_legacy_database = bool(members & _LEGACY_DATABASE_MEMBERS)
            if has_manifest and has_legacy_database:
                raise ArchiveRoutingError(
                    "archive mixes current and legacy Sporely signatures"
                )
            if has_manifest:
                if not names or names[0] != "manifest.json":
                    raise ArchiveRoutingError(
                        "current Sporely archives must begin with manifest.json"
                    )
                manifest = ArchiveManifest.from_json(archive.read("manifest.json"))
                return (
                    ArchiveRoute.FULL_BACKUP
                    if manifest.mode == "full_backup"
                    else ArchiveRoute.PORTABLE_OBSERVATIONS
                )
            if has_legacy_database:
                with tempfile.TemporaryDirectory(prefix="sporely-legacy-route-") as temporary:
                    root = Path(temporary)
                    for name in sorted(members & _LEGACY_DATABASE_MEMBERS):
                        destination = root / name
                        with archive.open(name) as source, destination.open("xb") as target:
                            shutil.copyfileobj(source, target, length=1024 * 1024)
                        verify_sqlite_integrity(destination)
                return ArchiveRoute.LEGACY_DATA_PACKAGE
            raise ArchiveRoutingError("archive has no recognized Sporely signature")
    except ArchiveRoutingError:
        raise
    except (
        BadZipFile, KeyError, OSError, sqlite3.Error, ManifestError,
        UnsafeArchivePathError, ArchiveValidationError,
    ) as exc:
        raise ArchiveRoutingError(f"invalid or unsupported Sporely archive: {exc}") from exc
