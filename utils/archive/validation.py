"""Validation for completed Sporely full-backup archives."""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import tempfile
from pathlib import Path
from zipfile import BadZipFile, ZipFile

from utils.archive.manifest import ArchiveManifest, ManifestError
from utils.archive.paths import UnsafeArchivePathError, safe_staging_destination, validate_zip_entries
from utils.archive.inventory import SettingPolicy, app_setting_policy, qsettings_policy


class ArchiveValidationError(ValueError):
    """Raised when a completed archive fails structural or content validation."""


def verify_sqlite_integrity(path: str | Path) -> None:
    """Require SQLite's full integrity check to return exactly ``ok``."""
    try:
        with sqlite3.connect(f"{Path(path).resolve().as_uri()}?mode=ro", uri=True) as connection:
            rows = [str(row[0]) for row in connection.execute("PRAGMA integrity_check")]
    except sqlite3.Error as exc:
        raise ArchiveValidationError(f"SQLite integrity check failed for {Path(path).name}") from exc
    if rows != ["ok"]:
        raise ArchiveValidationError(
            f"SQLite integrity check failed for {Path(path).name}: {'; '.join(rows)}"
        )


def validate_full_backup(path: str | Path) -> ArchiveManifest:
    """Validate a completed full backup without extracting it wholesale."""
    archive_path = Path(path)
    try:
        with ZipFile(archive_path, "r") as archive:
            infos = archive.infolist()
            names = validate_zip_entries(infos)
            if not names or names[0] != "manifest.json":
                raise ArchiveValidationError("manifest.json must be the first ZIP member")
            manifest = ArchiveManifest.from_json(archive.read("manifest.json"))
            if manifest.mode != "full_backup":
                raise ArchiveValidationError("archive is not a full backup")
            included = {entry.path: entry for entry in manifest.files if entry.status == "included"}
            members = set(names) - {"manifest.json"}
            if set(included) != members:
                raise ArchiveValidationError("manifest and ZIP members are not a bijection")
            info_by_name = {info.filename: info for info in infos}
            required = {
                "databases/mushrooms.db", "databases/reference_values.db",
                "data/app_settings.json", "data/qsettings.json",
            }
            missing_required = required - set(included)
            if missing_required:
                raise ArchiveValidationError(
                    f"required archive members missing: {', '.join(sorted(missing_required))}"
                )
            for name, entry in included.items():
                digest = hashlib.sha256()
                size = 0
                with archive.open(info_by_name[name], "r") as source:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        size += len(chunk)
                        digest.update(chunk)
                if size != entry.size or digest.hexdigest() != entry.sha256:
                    raise ArchiveValidationError(f"size or checksum mismatch: {name}")

            try:
                app_payload = json.loads(archive.read("data/app_settings.json"))
                qsettings_payload = json.loads(archive.read("data/qsettings.json"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ArchiveValidationError("invalid settings JSON") from exc
            if not isinstance(app_payload, dict) or app_payload.get("format_version") != 1 or not isinstance(app_payload.get("settings"), dict):
                raise ArchiveValidationError("invalid application settings payload")
            for key in app_payload["settings"]:
                if not isinstance(key, str) or app_setting_policy(key) is not SettingPolicy.EXACT:
                    raise ArchiveValidationError(f"non-restorable application setting: {key}")
            if not isinstance(qsettings_payload, dict) or qsettings_payload.get("format_version") != 1 or not isinstance(qsettings_payload.get("namespaces"), list):
                raise ArchiveValidationError("invalid QSettings payload")
            seen_namespaces: set[tuple[str, str]] = set()
            for namespace in qsettings_payload["namespaces"]:
                if not isinstance(namespace, dict) or not isinstance(namespace.get("organization"), str) or not isinstance(namespace.get("application"), str) or not isinstance(namespace.get("values"), dict):
                    raise ArchiveValidationError("invalid QSettings namespace")
                identity = (namespace["organization"], namespace["application"])
                if identity in seen_namespaces:
                    raise ArchiveValidationError("duplicate QSettings namespace")
                seen_namespaces.add(identity)
                for key in namespace["values"]:
                    if not isinstance(key, str) or qsettings_policy(identity, key) is not SettingPolicy.EXACT:
                        raise ArchiveValidationError(f"non-restorable QSettings value: {key}")
            for name in ("data/objectives.json", "data/last_objective.json"):
                if name in included:
                    try:
                        json.loads(archive.read(name))
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        raise ArchiveValidationError(f"invalid JSON payload: {name}") from exc

            with tempfile.TemporaryDirectory(prefix="sporely-verify-") as temporary:
                root = Path(temporary)
                for name in ("databases/mushrooms.db", "databases/reference_values.db"):
                    if name not in included:
                        raise ArchiveValidationError(f"required database is missing: {name}")
                    destination = safe_staging_destination(root, name)
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    with archive.open(name, "r") as source, destination.open("wb") as target:
                        shutil.copyfileobj(source, target, length=1024 * 1024)
                    verify_sqlite_integrity(destination)
    except (BadZipFile, KeyError, OSError, ManifestError, UnsafeArchivePathError) as exc:
        if isinstance(exc, ArchiveValidationError):
            raise
        raise ArchiveValidationError(f"invalid Sporely archive: {exc}") from exc
    return manifest


def validate_portable_observations(path: str | Path) -> ArchiveManifest:
    """Validate a portable selected-observation archive and its filtered DBs."""
    archive_path = Path(path)
    try:
        with ZipFile(archive_path, "r") as archive:
            infos = archive.infolist()
            names = validate_zip_entries(infos)
            if not names or names[0] != "manifest.json":
                raise ArchiveValidationError("manifest.json must be the first ZIP member")
            manifest = ArchiveManifest.from_json(archive.read("manifest.json"))
            if manifest.mode != "portable_observations" or manifest.identity_policy != "portable":
                raise ArchiveValidationError("archive is not a portable observation export")
            included = {entry.path: entry for entry in manifest.files if entry.status == "included"}
            members = set(names) - {"manifest.json"}
            if set(included) != members:
                raise ArchiveValidationError("manifest and ZIP members are not a bijection")
            required = {
                "portable/mushrooms.db", "portable/reference_values.db",
                "portable/objectives.json",
            }
            if missing := required - set(included):
                raise ArchiveValidationError(
                    f"required archive members missing: {', '.join(sorted(missing))}"
                )
            info_by_name = {info.filename: info for info in infos}
            for name, entry in included.items():
                digest = hashlib.sha256()
                size = 0
                with archive.open(info_by_name[name], "r") as source:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        size += len(chunk)
                        digest.update(chunk)
                if size != entry.size or digest.hexdigest() != entry.sha256:
                    raise ArchiveValidationError(f"size or checksum mismatch: {name}")
            try:
                objectives = json.loads(archive.read("portable/objectives.json"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ArchiveValidationError("invalid portable objectives JSON") from exc
            if not isinstance(objectives, dict):
                raise ArchiveValidationError("portable objectives must be a JSON object")
            with tempfile.TemporaryDirectory(prefix="sporely-portable-verify-") as temporary:
                root = Path(temporary)
                extracted: dict[str, Path] = {}
                for name in ("portable/mushrooms.db", "portable/reference_values.db"):
                    destination = safe_staging_destination(root, name)
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    with archive.open(name, "r") as source, destination.open("wb") as target:
                        shutil.copyfileobj(source, target, length=1024 * 1024)
                    verify_sqlite_integrity(destination)
                    extracted[name] = destination
                expected: dict[str, int] = {}
                main_tables = (
                    "observations", "images", "session_logs", "calibrations",
                    "calibration_assets", "observation_reference_uses",
                )
                with sqlite3.connect(extracted["portable/mushrooms.db"]) as connection:
                    for table in main_tables:
                        expected[table] = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                    expected["measurements"] = int(connection.execute("SELECT COUNT(*) FROM spore_measurements").fetchone()[0])
                    expected["annotations"] = int(connection.execute("SELECT COUNT(*) FROM spore_annotations").fetchone()[0])
                reference_tables = (
                    "reference_values", "reference_works", "reference_taxon_treatments",
                    "reference_measurement_sets",
                )
                with sqlite3.connect(extracted["portable/reference_values.db"]) as connection:
                    for table in reference_tables:
                        expected[table] = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                if manifest.contents != expected:
                    raise ArchiveValidationError("manifest contents counts do not match portable databases")
    except (BadZipFile, KeyError, OSError, sqlite3.Error, ManifestError, UnsafeArchivePathError) as exc:
        if isinstance(exc, ArchiveValidationError):
            raise
        raise ArchiveValidationError(f"invalid Sporely archive: {exc}") from exc
    return manifest
