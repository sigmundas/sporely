"""Creation of complete, identity-preserving Sporely backups."""

from __future__ import annotations

import base64
import json
import os
import platform
import re
import shutil
import sqlite3
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable
from zipfile import ZIP_DEFLATED, ZipFile

from utils.archive.checksums import sha256_file
from utils.archive.inventory import (
    RESOURCE_INVENTORY,
    BackupPolicy,
    SettingPolicy,
    app_setting_policy,
    database_setting_policy,
    qsettings_policy,
)
from utils.archive.manifest import ArchiveManifest, ManifestFile, build_manifest
from utils.archive.paths import canonical_archive_path
from utils.archive.validation import validate_full_backup, verify_sqlite_integrity


_MINIMUM_FREE_SPACE_RESERVE = 512 * 1024 * 1024


class FullBackupError(RuntimeError):
    """Raised when a full backup cannot be safely completed."""


@dataclass(frozen=True)
class BackupResult:
    path: Path
    manifest: ArchiveManifest
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class _StagedFile:
    archive_path: str
    source_path: Path | None
    status: str
    warning_identity: str | None = None


def _report_progress(
    callback: Callable[[str, int], None] | None,
    phase: str,
    percent: int,
) -> None:
    if callback is not None:
        callback(phase, max(0, min(100, int(percent))))


def _format_binary_size(size: int) -> str:
    value = float(max(0, size))
    for unit in ("bytes", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024.0 or unit == "TiB":
            return f"{value:.1f} {unit}" if unit != "bytes" else f"{int(value)} bytes"
        value /= 1024.0
    raise AssertionError("unreachable")


def _require_destination_space(destination: Path, estimated_bytes: int) -> None:
    reserve = max(_MINIMUM_FREE_SPACE_RESERVE, estimated_bytes // 20)
    required = estimated_bytes + reserve
    available = shutil.disk_usage(destination.parent).free
    if available < required:
        raise FullBackupError(
            "not enough free space for backup: "
            f"at least {_format_binary_size(required)} required, "
            f"{_format_binary_size(available)} available"
        )


def _stable_json_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _snapshot_database(source_path: Path, destination_path: Path) -> None:
    if not source_path.is_file():
        raise FullBackupError(f"required database does not exist: {source_path}")
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    source = sqlite3.connect(f"{source_path.resolve().as_uri()}?mode=ro", uri=True, timeout=10)
    destination = sqlite3.connect(destination_path)
    try:
        source.backup(destination)
        # A source in WAL mode can transfer that persistent journal setting.
        # Force the standalone snapshot back to a single-file journal before
        # any staged sanitization, since sidecar files are never archived.
        destination.execute("PRAGMA journal_mode=DELETE")
    except sqlite3.Error as exc:
        raise FullBackupError(f"could not snapshot database: {source_path}") from exc
    finally:
        destination.close()
        source.close()


def _sanitize_staged_settings(database_path: Path) -> None:
    try:
        with sqlite3.connect(database_path) as connection:
            connection.execute("PRAGMA secure_delete=ON")
            keys = [str(row[0]) for row in connection.execute("SELECT key FROM settings")]
            secret_keys = [key for key in keys if database_setting_policy(key) is SettingPolicy.SECRET]
            connection.executemany("DELETE FROM settings WHERE key = ?", ((key,) for key in secret_keys))
            connection.commit()
            connection.execute("VACUUM")
    except (sqlite3.Error, KeyError) as exc:
        raise FullBackupError(f"could not sanitize staged database settings: {exc}") from exc


def _safe_app_settings(settings: dict[str, object]) -> bytes:
    safe: dict[str, object] = {}
    for key in sorted(settings):
        try:
            policy = app_setting_policy(key)
        except KeyError as exc:
            raise FullBackupError(str(exc)) from exc
        if policy is SettingPolicy.EXACT:
            safe[key] = settings[key]
    try:
        return _stable_json_bytes({"format_version": 1, "settings": safe})
    except (TypeError, ValueError) as exc:
        raise FullBackupError("safe application settings are not JSON serializable") from exc


def _json_safe_qsetting(value: object) -> object:
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, (bytes, bytearray)):
        return {"type": "bytes", "base64": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, (list, tuple)):
        return [_json_safe_qsetting(item) for item in value]
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        return {key: _json_safe_qsetting(value[key]) for key in sorted(value)}
    try:
        from PySide6.QtCore import QByteArray

        if isinstance(value, QByteArray):
            return {"type": "bytes", "base64": base64.b64encode(bytes(value)).decode("ascii")}
    except ImportError:
        pass
    raise FullBackupError(f"unsupported QSettings value type: {type(value).__name__}")


def _safe_qsettings_snapshot(
    values: dict[tuple[str, str], dict[str, object]] | None = None,
) -> bytes:
    if values is None:
        from PySide6.QtCore import QSettings
        from app_identity import SETTINGS_APP, SETTINGS_ORG

        values = {}
        for namespace in ((SETTINGS_ORG, SETTINGS_APP), (SETTINGS_ORG, "SpeciesPlate")):
            settings = QSettings(*namespace)
            settings.sync()
            if settings.status() != QSettings.NoError:
                raise FullBackupError(f"could not read QSettings namespace: {namespace[1]}")
            values[namespace] = {key: settings.value(key) for key in settings.allKeys()}

    namespaces: list[dict[str, object]] = []
    for namespace in sorted(values):
        included: dict[str, object] = {}
        for key in sorted(values[namespace]):
            if qsettings_policy(namespace, key) is SettingPolicy.EXACT:
                included[key] = _json_safe_qsetting(values[namespace][key])
        namespaces.append({
            "organization": namespace[0],
            "application": namespace[1],
            "values": included,
        })
    return _stable_json_bytes({"format_version": 1, "namespaces": namespaces})


def _suffix(path: Path) -> str:
    suffix = path.suffix.lower()
    return suffix if re.fullmatch(r"\.[a-z0-9]{1,12}", suffix) else ".bin"


def _resolve_row_path(value: object, images_dir: Path) -> Path | None:
    text = str(value or "").strip()
    if not text:
        return None
    path = Path(text).expanduser()
    return path if path.is_absolute() else images_dir / path


def _asset_excluded_by_policy(path: Path) -> bool:
    return path.suffix.casefold() == ".orf"


def _candidate(path: Path, archive_path: str, *, excluded: bool = False) -> _StagedFile:
    canonical_archive_path(archive_path)
    if excluded:
        return _StagedFile(archive_path, None, "excluded_by_policy")
    if not path.is_file():
        return _StagedFile(
            archive_path,
            None,
            "missing_at_source",
            os.path.normcase(str(path.resolve(strict=False))),
        )
    return _StagedFile(archive_path, path, "included")


def _json_image_entries(value: object) -> Iterable[tuple[str, str, object, bool]]:
    if not value:
        return ()
    try:
        loaded = json.loads(value) if isinstance(value, str) else value
    except (json.JSONDecodeError, TypeError):
        return ()
    if not isinstance(loaded, dict):
        return ()
    found: list[tuple[str, str, object, bool]] = []
    for collection in ("images", "auto_images"):
        for index, entry in enumerate(loaded.get(collection) or []):
            if not isinstance(entry, dict):
                continue
            token = str(index) if collection == "images" else f"auto-{index}"
            excluded = (
                str(entry.get("source_role") or "").lower() == "cloud_recovery_cache"
                or str(entry.get("file_purpose") or "").lower() == "cache"
            )
            for key in ("source_path", "source_filepath", "original_path", "selected_path", "path", "working_path"):
                if entry.get(key):
                    found.append((token, key, entry[key], excluded))
            for companion_index, companion in enumerate(entry.get("companion_paths") or []):
                if companion:
                    found.append((token, f"companion_{companion_index}", companion, excluded))
    return found


def _collect_database_assets(database_path: Path, images_dir: Path) -> list[_StagedFile]:
    candidates: list[_StagedFile] = []
    with sqlite3.connect(database_path) as connection:
        connection.row_factory = sqlite3.Row
        for row in connection.execute(
            "SELECT id, filepath, original_filepath, source_role, file_purpose FROM images ORDER BY id"
        ):
            excluded = (
                str(row["source_role"] or "").lower() == "cloud_recovery_cache"
                or str(row["file_purpose"] or "").lower() == "cache"
            )
            for field, label, root in (
                ("filepath", "working", "images"),
                ("original_filepath", "original", "originals"),
            ):
                source = _resolve_row_path(row[field], images_dir)
                if source is not None:
                    destination = f"assets/{root}/{row['id']}/{label}{_suffix(source)}"
                    candidates.append(_candidate(
                        source,
                        destination,
                        excluded=excluded or _asset_excluded_by_policy(source),
                    ))

        for row in connection.execute(
            "SELECT id, image_filepath, measurements_json FROM calibrations ORDER BY id"
        ):
            source = _resolve_row_path(row["image_filepath"], images_dir)
            if source is not None:
                destination = f"assets/calibrations/records/{row['id']}/working{_suffix(source)}"
                candidates.append(_candidate(
                    source,
                    destination,
                    excluded=_asset_excluded_by_policy(source),
                ))
            for index, label, value, excluded in _json_image_entries(row["measurements_json"]):
                source = _resolve_row_path(value, images_dir)
                if source is not None:
                    destination = f"assets/calibrations/records/{row['id']}/metadata-{index}-{label}{_suffix(source)}"
                    candidates.append(_candidate(
                        source,
                        destination,
                        excluded=excluded or _asset_excluded_by_policy(source),
                    ))

        for row in connection.execute(
            "SELECT id, asset_uuid, local_path, original_path, source_role, file_purpose, metadata_json FROM calibration_assets ORDER BY id"
        ):
            identity = str(row["id"])
            excluded = (
                str(row["source_role"] or "").lower() == "cloud_recovery_cache"
                or str(row["file_purpose"] or "").lower() == "cache"
            )
            for field, label in (("local_path", "local"), ("original_path", "original")):
                source = _resolve_row_path(row[field], images_dir)
                if source is not None:
                    destination = f"assets/calibrations/assets/{identity}/{label}{_suffix(source)}"
                    candidates.append(_candidate(
                        source,
                        destination,
                        excluded=excluded or _asset_excluded_by_policy(source),
                    ))
            metadata = row["metadata_json"]
            try:
                loaded = json.loads(metadata) if isinstance(metadata, str) else metadata
            except (json.JSONDecodeError, TypeError):
                loaded = None
            if isinstance(loaded, dict):
                for label in ("source_path", "working_path", "original_path", "local_path"):
                    source = _resolve_row_path(loaded.get(label), images_dir)
                    if source is not None:
                        destination = f"assets/calibrations/assets/{identity}/metadata-{label}{_suffix(source)}"
                        candidates.append(_candidate(
                            source,
                            destination,
                            excluded=excluded or _asset_excluded_by_policy(source),
                        ))
                for index, value in enumerate(loaded.get("companion_paths") or []):
                    source = _resolve_row_path(value, images_dir)
                    if source is not None:
                        destination = f"assets/calibrations/assets/{identity}/metadata-companion-{index}{_suffix(source)}"
                        candidates.append(_candidate(
                            source,
                            destination,
                            excluded=excluded or _asset_excluded_by_policy(source),
                        ))
    return candidates


def _collect_plate_layouts(database_path: Path) -> list[_StagedFile]:
    root = database_path.parent / "plate_layouts"
    if not root.is_dir():
        return [_StagedFile("data/plate_layouts", None, "missing_at_source")]
    layouts = [
        _candidate(path, f"data/plate_layouts/{path.name}")
        for path in sorted(root.glob("*.mplate"), key=lambda item: item.name.casefold())
        if path.is_file()
    ]
    return layouts or [_StagedFile("data/plate_layouts", None, "missing_at_source")]


def _excluded_policy_entries() -> list[_StagedFile]:
    entries: list[_StagedFile] = []
    for item in RESOURCE_INVENTORY:
        if item.policy.backup in {
            BackupPolicy.REGENERABLE, BackupPolicy.CACHE, BackupPolicy.SECRET, BackupPolicy.DOWNLOADABLE,
        }:
            entries.append(_StagedFile(item.archive_path, None, "excluded_by_policy"))
    entries.append(_StagedFile("data/app_settings.raw.json", None, "excluded_by_policy"))
    return entries


def _write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _count_contents(database_path: Path) -> dict[str, int]:
    with sqlite3.connect(database_path) as connection:
        return {
            "observations": int(connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0]),
            "images": int(connection.execute("SELECT COUNT(*) FROM images").fetchone()[0]),
            "measurements": int(connection.execute("SELECT COUNT(*) FROM spore_measurements").fetchone()[0]),
        }


def create_full_backup(
    destination: str | Path,
    *,
    app_version: str,
    archive_id: str | None = None,
    created_at: str | None = None,
    source_platform: str | None = None,
    qsettings_values: dict[tuple[str, str], dict[str, object]] | None = None,
    validate: Callable[[str | Path], ArchiveManifest] = validate_full_backup,
    progress_callback: Callable[[str, int], None] | None = None,
) -> BackupResult:
    """Create, verify, and atomically publish a complete ``.sporely`` backup."""
    from database.schema import (
        get_app_settings,
        get_database_path,
        get_images_dir,
        get_last_objective_path,
        get_objectives_path,
        get_reference_database_path,
    )

    final_path = Path(destination).expanduser()
    if final_path.suffix.lower() != ".sporely":
        final_path = final_path.with_name(final_path.name + ".sporely")
    final_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_zip: Path | None = None
    try:
        _report_progress(progress_callback, "preparing", 0)
        with tempfile.TemporaryDirectory(prefix="sporely-backup-") as temporary:
            staging = Path(temporary)
            staged_main = staging / "databases/mushrooms.db"
            staged_reference = staging / "databases/reference_values.db"
            _snapshot_database(Path(get_database_path()), staged_main)
            _sanitize_staged_settings(staged_main)
            verify_sqlite_integrity(staged_main)
            _snapshot_database(Path(get_reference_database_path()), staged_reference)
            verify_sqlite_integrity(staged_reference)

            _write_bytes(staging / "data/app_settings.json", _safe_app_settings(get_app_settings()))
            _write_bytes(staging / "data/qsettings.json", _safe_qsettings_snapshot(qsettings_values))

            files = [
                _StagedFile("databases/mushrooms.db", staged_main, "included"),
                _StagedFile("databases/reference_values.db", staged_reference, "included"),
                _StagedFile("data/app_settings.json", staging / "data/app_settings.json", "included"),
                _StagedFile("data/qsettings.json", staging / "data/qsettings.json", "included"),
            ]
            for source, archive_path in (
                (Path(get_objectives_path()), "data/objectives.json"),
                (Path(get_last_objective_path()), "data/last_objective.json"),
            ):
                files.append(_candidate(source, archive_path))
            files.extend(_collect_database_assets(staged_main, Path(get_images_dir())))
            files.extend(_collect_plate_layouts(Path(get_database_path())))
            files.extend(_excluded_policy_entries())

            seen: set[str] = set()
            for item in files:
                folded = item.archive_path.casefold()
                if folded in seen:
                    raise FullBackupError(f"duplicate archive destination: {item.archive_path}")
                seen.add(folded)

            source_sizes = {
                item.archive_path: item.source_path.stat().st_size
                for item in files
                if item.status == "included" and item.source_path is not None
            }
            estimated_bytes = sum(source_sizes.values())
            _report_progress(progress_callback, "checking_space", 3)
            _require_destination_space(final_path, estimated_bytes)

            manifest_files: list[ManifestFile] = []
            included_files: list[_StagedFile] = []
            warnings: list[str] = []
            hashed_bytes = 0
            for item in sorted(files, key=lambda value: value.archive_path):
                if item.status == "included":
                    assert item.source_path is not None
                    size = source_sizes[item.archive_path]
                    manifest_files.append(ManifestFile(
                        item.archive_path, "included", size,
                        sha256_file(item.source_path),
                    ))
                    included_files.append(item)
                    hashed_bytes += size
                    _report_progress(
                        progress_callback,
                        "hashing",
                        5 + int(25 * hashed_bytes / max(1, estimated_bytes)),
                    )
                else:
                    manifest_files.append(ManifestFile(item.archive_path, item.status))
                    if item.status == "missing_at_source":
                        warnings.append(item.archive_path)

            manifest = build_manifest(
                mode="full_backup",
                archive_id=archive_id or str(uuid.uuid4()),
                created_at=created_at or datetime.now(timezone.utc).isoformat(),
                app_version=app_version,
                source_platform=source_platform or platform.platform(),
                contents=_count_contents(staged_main),
                files=manifest_files,
            )
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{final_path.name}.", suffix=".tmp", dir=final_path.parent,
            )
            os.close(descriptor)
            temporary_zip = Path(temporary_name)
            with ZipFile(temporary_zip, "w", compression=ZIP_DEFLATED, allowZip64=True) as archive:
                archive.writestr("manifest.json", manifest.to_json_bytes())
                written_bytes = 0
                for item in sorted(included_files, key=lambda value: value.archive_path):
                    assert item.source_path is not None
                    archive.write(item.source_path, item.archive_path)
                    written_bytes += source_sizes[item.archive_path]
                    _report_progress(
                        progress_callback,
                        "writing",
                        30 + int(60 * written_bytes / max(1, estimated_bytes)),
                    )
            _report_progress(progress_callback, "validating", 90)
            validate(temporary_zip)
            os.replace(temporary_zip, final_path)
            temporary_zip = None
            _report_progress(progress_callback, "complete", 100)
            return BackupResult(final_path, manifest, tuple(warnings))
    except FullBackupError:
        raise
    except Exception as exc:
        raise FullBackupError(f"backup failed: {exc}") from exc
    finally:
        if temporary_zip is not None:
            temporary_zip.unlink(missing_ok=True)
