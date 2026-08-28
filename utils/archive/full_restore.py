"""Staged, identity-preserving restoration of full Sporely backups."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from zipfile import BadZipFile, ZipFile

from utils.archive.full_backup import (
    BackupResult,
    _asset_excluded_by_policy,
    create_full_backup,
)
from utils.archive.inventory import MAIN_DATABASE_TABLES, REFERENCE_DATABASE_TABLES, SettingPolicy, app_setting_policy, database_setting_policy, qsettings_policy
from utils.archive.manifest import ArchiveManifest
from utils.archive.paths import safe_staging_destination, validate_zip_entries
from utils.archive.validation import ArchiveValidationError, validate_full_backup, verify_sqlite_integrity


class FullRestoreError(RuntimeError):
    """Raised when a backup cannot be restored without risking live state."""


@dataclass(frozen=True)
class RestoreResult:
    manifest: ArchiveManifest
    safety_backup: Path


def _read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArchiveValidationError(f"invalid JSON payload: {path.name}") from exc
    if not isinstance(value, dict) or value.get("format_version") != 1:
        raise ArchiveValidationError(f"unsupported JSON payload: {path.name}")
    return value


def _extract_verified(archive_path: Path, staging: Path, manifest: ArchiveManifest) -> None:
    included = {item.path: item for item in manifest.files if item.status == "included"}
    try:
        with ZipFile(archive_path) as archive:
            names = validate_zip_entries(archive.infolist())
            if set(names) - {"manifest.json"} != set(included):
                raise ArchiveValidationError("manifest and ZIP members are not a bijection")
            for name in names:
                if name == "manifest.json":
                    continue
                destination = safe_staging_destination(staging, name)
                destination.parent.mkdir(parents=True, exist_ok=True)
                digest, size = hashlib.sha256(), 0
                with archive.open(name) as source, destination.open("xb") as target:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        target.write(chunk)
                        digest.update(chunk)
                        size += len(chunk)
                entry = included[name]
                if size != entry.size or digest.hexdigest() != entry.sha256:
                    raise ArchiveValidationError(f"size or checksum mismatch during extraction: {name}")
    except (BadZipFile, KeyError, OSError) as exc:
        raise ArchiveValidationError(f"archive extraction failed: {exc}") from exc


def _require_tables(path: Path, required: set[str]) -> None:
    verify_sqlite_integrity(path)
    try:
        with sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True) as connection:
            actual = {str(row[0]) for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )}
            foreign_keys = list(connection.execute("PRAGMA foreign_key_check"))
    except sqlite3.Error as exc:
        raise ArchiveValidationError(f"database sanity check failed: {path.name}") from exc
    missing = required - actual
    if missing:
        raise ArchiveValidationError(f"required tables missing from {path.name}: {', '.join(sorted(missing))}")
    if foreign_keys:
        raise ArchiveValidationError(f"foreign-key violations in {path.name}")


def _require_current_columns(
    candidate: Path, current: Path, required_tables: set[str]
) -> None:
    """Fail closed when a v1 snapshot cannot satisfy the installed schema."""
    try:
        with sqlite3.connect(candidate) as staged, sqlite3.connect(current) as live:
            for table in sorted(required_tables):
                staged_columns = {
                    str(row[1]) for row in staged.execute(f'PRAGMA table_info("{table}")')
                }
                live_columns = {
                    str(row[1]) for row in live.execute(f'PRAGMA table_info("{table}")')
                }
                missing = live_columns - staged_columns
                if missing:
                    raise ArchiveValidationError(
                        f"required columns missing from {table}: "
                        + ", ".join(sorted(missing))
                    )
    except sqlite3.Error as exc:
        raise ArchiveValidationError("database schema compatibility check failed") from exc


def _migrate_staged_databases(main_database: Path, reference_database: Path) -> None:
    """Apply supported production additive migrations only to staged copies."""
    from database import schema

    try:
        needs_migration = False
        for candidate, current, tables in (
            (main_database, Path(schema.get_database_path()), set(MAIN_DATABASE_TABLES)),
            (
                reference_database,
                Path(schema.get_reference_database_path()),
                set(REFERENCE_DATABASE_TABLES),
            ),
        ):
            with sqlite3.connect(candidate) as staged, sqlite3.connect(current) as live:
                staged_tables = {
                    str(row[0]) for row in staged.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' "
                        "AND name NOT LIKE 'sqlite_%'"
                    )
                }
                if tables - staged_tables:
                    needs_migration = True
                    break
                for table in tables:
                    staged_columns = {
                        str(row[1]) for row in staged.execute(f'PRAGMA table_info("{table}")')
                    }
                    live_columns = {
                        str(row[1]) for row in live.execute(f'PRAGMA table_info("{table}")')
                    }
                    if live_columns - staged_columns:
                        needs_migration = True
                        break
                if needs_migration:
                    break
        if needs_migration:
            schema.init_database(
                db_path=main_database,
                reference_path=reference_database,
                run_model_backfills=False,
            )
    except Exception as exc:
        raise ArchiveValidationError("staged database migration failed") from exc


def _asset_path(staging: Path, statuses: dict[str, str], prefix: str, row_id: int, label: str, old: object, *, excluded: bool = False) -> Path | None:
    if not str(old or "").strip():
        return None
    candidates = sorted((staging / prefix / str(row_id)).glob(f"{label}.*"))
    if len(candidates) > 1:
        raise ArchiveValidationError(f"ambiguous archived asset for {prefix}/{row_id}/{label}")
    declared = [(name, status) for name, status in statuses.items() if name.startswith(f"{prefix}/{row_id}/{label}.")]
    if len(declared) != 1:
        raise ArchiveValidationError(f"missing or ambiguous manifest asset for {prefix}/{row_id}/{label}")
    _name, status = declared[0]
    if excluded:
        if status != "excluded_by_policy":
            raise ArchiveValidationError(f"cache asset was not excluded by policy: {_name}")
        return None
    if _asset_excluded_by_policy(Path(str(old))) and status == "excluded_by_policy":
        return None
    if status == "missing_at_source":
        return None
    if status != "included" or len(candidates) != 1:
        raise ArchiveValidationError(f"required archived asset is missing: {_name}")
    return candidates[0]


def _rebase_and_validate_assets(database: Path, staging: Path, images_root: Path, manifest: ArchiveManifest) -> None:
    """Rebase path-bearing rows and require every archived authoritative byte."""
    statuses = {item.path: item.status for item in manifest.files}
    with sqlite3.connect(database) as connection:
        connection.row_factory = sqlite3.Row
        for observation_id, folder_path in connection.execute(
            "SELECT id, folder_path FROM observations"
        ):
            if str(folder_path or "").strip():
                connection.execute(
                    "UPDATE observations SET folder_path=? WHERE id=?",
                    (
                        str(images_root / "restored-observations" / str(observation_id)),
                        observation_id,
                    ),
                )
        for row in connection.execute("SELECT id, filepath, original_filepath, source_role, file_purpose FROM images"):
            excluded = str(row["source_role"] or "").lower() == "cloud_recovery_cache" or str(row["file_purpose"] or "").lower() == "cache"
            updates: dict[str, str | None] = {}
            for field, prefix, label, target_dir in (
                ("filepath", "assets/images", "working", images_root / "images"),
                ("original_filepath", "assets/originals", "original", images_root / "originals"),
            ):
                source = _asset_path(staging, statuses, prefix, int(row["id"]), label, row[field], excluded=excluded)
                if source is not None:
                    target = target_dir / str(row["id"]) / source.name
                    updates[field] = str(target)
                elif str(row[field] or "").strip():
                    # Phase 2 explicitly records missing source files. Preserve the
                    # row, but do not retain a stale path from another machine.
                    updates[field] = "" if field == "filepath" else None
            for field, value in updates.items():
                connection.execute(f"UPDATE images SET {field}=? WHERE id=?", (value, row["id"]))

        for row in connection.execute("SELECT id, image_filepath, measurements_json FROM calibrations"):
            source = _asset_path(staging, statuses, "assets/calibrations/records", int(row["id"]), "working", row["image_filepath"])
            connection.execute("UPDATE calibrations SET image_filepath=? WHERE id=?", (
                str(images_root / "calibrations/records" / str(row["id"]) / source.name) if source else None,
                row["id"],
            ))
            metadata = _rebase_metadata_paths(
                row["measurements_json"], staging, statuses,
                f"assets/calibrations/records/{row['id']}",
                images_root / "calibrations/records" / str(row["id"]),
                nested_images=True,
            )
            connection.execute("UPDATE calibrations SET measurements_json=? WHERE id=?", (metadata, row["id"]))
        for row in connection.execute(
            "SELECT id, local_path, original_path, metadata_json, source_role, "
            "file_purpose FROM calibration_assets"
        ):
            excluded = (
                str(row["source_role"] or "").lower() == "cloud_recovery_cache"
                or str(row["file_purpose"] or "").lower() == "cache"
            )
            for field, label in (("local_path", "local"), ("original_path", "original")):
                source = _asset_path(
                    staging, statuses, "assets/calibrations/assets", int(row["id"]),
                    label, row[field], excluded=excluded,
                )
                value = str(images_root / "calibrations/assets" / str(row["id"]) / source.name) if source else None
                connection.execute(f"UPDATE calibration_assets SET {field}=? WHERE id=?", (value, row["id"]))
            metadata = _rebase_metadata_paths(
                row["metadata_json"], staging, statuses,
                f"assets/calibrations/assets/{row['id']}",
                images_root / "calibrations/assets" / str(row["id"]),
                excluded=excluded,
            )
            connection.execute("UPDATE calibration_assets SET metadata_json=? WHERE id=?", (metadata, row["id"]))

        keys = [str(row[0]) for row in connection.execute("SELECT key FROM settings")]
        for key in keys:
            policy = database_setting_policy(key)
            if policy is SettingPolicy.SECRET:
                raise ArchiveValidationError(f"secret database setting present in backup: {key}")
            if policy in {SettingPolicy.MACHINE_SPECIFIC, SettingPolicy.REGENERABLE, SettingPolicy.EXCLUDE}:
                connection.execute("DELETE FROM settings WHERE key=?", (key,))
        connection.commit()


def _rebase_metadata_paths(
    value: object, staging: Path, statuses: dict[str, str], archive_root: str,
    target_root: Path, *, nested_images: bool = False, excluded: bool = False,
) -> object:
    try:
        loaded = json.loads(value) if isinstance(value, str) else value
    except (json.JSONDecodeError, TypeError):
        return value
    if not isinstance(loaded, dict):
        return value
    if nested_images:
        image_records = loaded.get("images")
        auto_records = loaded.get("auto_images") or []
        if not isinstance(image_records, list) or not isinstance(auto_records, list):
            return value
        records = [
            (str(index), record) for index, record in enumerate(image_records)
        ] + [
            (f"auto-{index}", record) for index, record in enumerate(auto_records)
        ]
    else:
        records = [("0", loaded)]
    if not isinstance(records, list):
        return value
    keys = ("source_path", "source_filepath", "original_path", "selected_path", "path", "working_path") if nested_images else ("source_path", "working_path", "original_path", "local_path")
    for index, record in records:
        if not isinstance(record, dict):
            continue
        record_excluded = excluded or (
            str(record.get("source_role") or "").lower() == "cloud_recovery_cache"
            or str(record.get("file_purpose") or "").lower() == "cache"
        )
        for key in keys:
            if not record.get(key):
                continue
            label = f"metadata-{index}-{key}" if nested_images else f"metadata-{key}"
            source = _asset_path(
                staging, statuses, archive_root.rsplit("/", 1)[0],
                int(archive_root.rsplit("/", 1)[1]), label, record[key],
                excluded=record_excluded,
            )
            record[key] = str(target_root / source.name) if source else None
        companions = record.get("companion_paths")
        if isinstance(companions, list):
            for companion_index, old in enumerate(companions):
                if not old:
                    continue
                label = f"metadata-{index}-companion_{companion_index}" if nested_images else f"metadata-companion-{companion_index}"
                source = _asset_path(
                    staging, statuses, archive_root.rsplit("/", 1)[0],
                    int(archive_root.rsplit("/", 1)[1]), label, old,
                    excluded=record_excluded,
                )
                companions[companion_index] = str(target_root / source.name) if source else None
    return json.dumps(loaded, ensure_ascii=False, sort_keys=True)


def _copy_asset_tree(staging: Path, payload: Path) -> None:
    destination = payload / "images"
    destination.mkdir(parents=True, exist_ok=True)
    for archive_root, relative in (
        (staging / "assets/images", "images"),
        (staging / "assets/originals", "originals"),
        (staging / "assets/calibrations", "calibrations"),
    ):
        if archive_root.is_dir():
            shutil.copytree(archive_root, destination / relative, dirs_exist_ok=True)


def _merge_app_settings(staged: Path, current: dict) -> dict:
    archived = _read_json(staged / "data/app_settings.json").get("settings")
    if not isinstance(archived, dict):
        raise ArchiveValidationError("invalid application settings payload")
    # The archive only contains EXACT values. Current machine paths and secrets
    # remain authoritative on this machine.
    for key in archived:
        if app_setting_policy(key) is not SettingPolicy.EXACT:
            raise ArchiveValidationError(f"non-restorable application setting in backup: {key}")
    merged = {}
    for key, value in current.items():
        if app_setting_policy(key) in {SettingPolicy.MACHINE_SPECIFIC, SettingPolicy.SECRET}:
            merged[key] = value
    merged.update(archived)
    return merged


def _decode_qsetting(value: object) -> object:
    if isinstance(value, dict) and set(value) == {"type", "base64"} and value["type"] == "bytes":
        try:
            return base64.b64decode(str(value["base64"]), validate=True)
        except ValueError as exc:
            raise ArchiveValidationError("invalid QSettings byte value") from exc
    if isinstance(value, list):
        return [_decode_qsetting(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _decode_qsetting(item) for key, item in value.items()}
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise ArchiveValidationError("invalid QSettings value")


def _load_qsettings(staged: Path) -> list[tuple[str, str, dict[str, object]]]:
    payload = _read_json(staged / "data/qsettings.json")
    namespaces = payload.get("namespaces")
    if not isinstance(namespaces, list):
        raise ArchiveValidationError("invalid QSettings payload")
    result = []
    seen: set[tuple[str, str]] = set()
    for item in namespaces:
        if not isinstance(item, dict) or not isinstance(item.get("organization"), str) or not isinstance(item.get("application"), str) or not isinstance(item.get("values"), dict):
            raise ArchiveValidationError("invalid QSettings namespace")
        namespace = (item["organization"], item["application"])
        if namespace in seen:
            raise ArchiveValidationError("duplicate QSettings namespace")
        seen.add(namespace)
        if any(not isinstance(key, str) or qsettings_policy(namespace, key) is not SettingPolicy.EXACT for key in item["values"]):
            raise ArchiveValidationError("non-restorable QSettings value in backup")
        result.append((item["organization"], item["application"], {key: _decode_qsetting(value) for key, value in item["values"].items()}))
    return result


def _local_swap_copy(source: Path | None, target: Path) -> tuple[Path, Path, Path]:
    """Copy staged content beside its target so final renames stay on one filesystem."""
    swap_root = target.parent / f".sporely-restore-{uuid.uuid4().hex}"
    swap_root.mkdir(parents=True)
    incoming = swap_root / "incoming"
    rollback = swap_root / "previous"
    try:
        if source is None:
            pass
        elif source.is_dir():
            shutil.copytree(source, incoming)
        else:
            shutil.copy2(source, incoming)
    except Exception:
        shutil.rmtree(swap_root, ignore_errors=True)
        raise
    return incoming, rollback, swap_root


@dataclass
class PreparedRestore:
    manifest: ArchiveManifest
    safety_backup: Path
    app_version: str
    backup_creator: Callable[..., BackupResult]
    temporary: tempfile.TemporaryDirectory
    targets: list[tuple[Path | None, Path]]
    swap_targets: list[tuple[tuple[Path, Path, Path], Path]]
    qsettings: list[tuple[str, str, dict[str, object]]]

    def cleanup(self) -> None:
        for (_incoming, _old, swap_root), _target in self.swap_targets:
            shutil.rmtree(swap_root, ignore_errors=True)
        self.temporary.cleanup()


@dataclass
class RestoreSwap:
    """A filesystem swap awaiting UI-thread settings and sanity checks."""

    prepared: PreparedRestore
    safety_backup: Path
    replaced: list[tuple[Path, Path, bool]]
    finished: bool = False

    def rollback(self) -> None:
        if self.finished:
            return
        rollback_errors: list[Exception] = []
        for target, old, existed in reversed(self.replaced):
            if target.exists():
                shutil.rmtree(target) if target.is_dir() else target.unlink()
            if existed and old.exists():
                try:
                    os.replace(old, target)
                except Exception as rollback_exc:
                    try:
                        shutil.copytree(old, target) if old.is_dir() else shutil.copy2(old, target)
                    except Exception:
                        rollback_errors.append(rollback_exc)
        if rollback_errors:
            raise FullRestoreError(
                "restore rollback could not recover all live paths; recovery copies were preserved"
            ) from rollback_errors[0]
        self.finished = True
        self.prepared.cleanup()

    def commit(self) -> RestoreResult:
        if self.finished:
            raise FullRestoreError("restore swap is already finished")
        self.finished = True
        result = RestoreResult(self.prepared.manifest, self.safety_backup)
        self.prepared.cleanup()
        return result


@dataclass
class RestoredQSettings:
    """UI-thread-owned QSettings changes that can be rolled back."""

    previous: list[tuple[object, dict[str, object]]]

    def rollback(self) -> None:
        for settings, values in self.previous:
            for key, value in values.items():
                settings.remove(key) if value is _MISSING else settings.setValue(key, value)
            settings.sync()


def apply_prepared_restore_qsettings(prepared: PreparedRestore) -> RestoredQSettings:
    """Apply restored QSettings on the caller's Qt-owning thread."""
    from PySide6.QtCore import QSettings
    from app_identity import SETTINGS_ORG

    previous: list[tuple[object, dict[str, object]]] = []
    try:
        for organization, application, values in prepared.qsettings:
            target_namespace = (
                (SETTINGS_ORG, "SpeciesPlate")
                if application == "SpeciesPlate"
                else (organization, application)
            )
            settings = QSettings(*target_namespace)
            exact_keys = [
                key
                for key in settings.allKeys()
                if qsettings_policy(target_namespace, key) is SettingPolicy.EXACT
            ]
            previous.append(
                (
                    settings,
                    {
                        key: settings.value(key) if settings.contains(key) else _MISSING
                        for key in set(exact_keys) | set(values)
                    },
                )
            )
            for key in exact_keys:
                settings.remove(key)
            for key, value in values.items():
                settings.setValue(key, value)
            settings.sync()
            if settings.status() != QSettings.NoError:
                raise FullRestoreError(
                    f"could not restore QSettings namespace: {application}"
                )
        return RestoredQSettings(previous)
    except Exception:
        RestoredQSettings(previous).rollback()
        raise


def prepare_full_restore(
    archive_path: str | Path,
    *,
    app_version: str,
    safety_backup_path: str | Path | None = None,
    backup_creator: Callable[..., BackupResult] = create_full_backup,
) -> PreparedRestore:
    """Perform expensive validation and staging without mutating live state."""
    from database import schema

    source = Path(archive_path)
    temporary: tempfile.TemporaryDirectory | None = None
    swap_targets: list[tuple[tuple[Path, Path, Path], Path]] = []
    try:
        manifest = validate_full_backup(source)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        safety = Path(safety_backup_path) if safety_backup_path else schema.get_database_path().parent / "backups" / f"Sporely Pre-Restore {stamp}.sporely"
        temporary = tempfile.TemporaryDirectory(prefix="sporely-restore-")
        root = Path(temporary.name)
        extracted, payload = root / "extracted", root / "payload"
        _extract_verified(source, extracted, manifest)
        main_db = extracted / "databases/mushrooms.db"
        ref_db = extracted / "databases/reference_values.db"
        _migrate_staged_databases(main_db, ref_db)
        _require_tables(main_db, set(MAIN_DATABASE_TABLES))
        _require_tables(ref_db, set(REFERENCE_DATABASE_TABLES))
        _require_current_columns(main_db, Path(schema.get_database_path()), set(MAIN_DATABASE_TABLES))
        _require_current_columns(ref_db, Path(schema.get_reference_database_path()), set(REFERENCE_DATABASE_TABLES))
        merged_settings = _merge_app_settings(extracted, schema.get_app_settings())
        qsettings = _load_qsettings(extracted)
        images_root = Path(schema.get_images_dir())
        _rebase_and_validate_assets(main_db, extracted, images_root, manifest)
        with sqlite3.connect(main_db) as connection:
            counts = {
                "observations": connection.execute("SELECT COUNT(*) FROM observations").fetchone()[0],
                "images": connection.execute("SELECT COUNT(*) FROM images").fetchone()[0],
                "measurements": connection.execute("SELECT COUNT(*) FROM spore_measurements").fetchone()[0],
            }
        if counts != manifest.contents:
            raise ArchiveValidationError("manifest content counts do not match the database")
        verify_sqlite_integrity(main_db)
        verify_sqlite_integrity(ref_db)
        payload.mkdir()
        (payload / "app_settings.json").write_text(
            json.dumps(merged_settings, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        shutil.copy2(main_db, payload / "mushrooms.db")
        shutil.copy2(ref_db, payload / "reference_values.db")
        _copy_asset_tree(extracted, payload)
        manifest_statuses = {item.path: item.status for item in manifest.files}
        plate_layouts = extracted / "data/plate_layouts"
        if plate_layouts.is_dir():
            shutil.copytree(plate_layouts, payload / "plate_layouts")
        for name in ("objectives.json", "last_objective.json"):
            candidate = extracted / "data" / name
            if candidate.exists():
                shutil.copy2(candidate, payload / name)
        targets = [
            (payload / "mushrooms.db", Path(schema.get_database_path())),
            (payload / "reference_values.db", Path(schema.get_reference_database_path())),
            (payload / "app_settings.json", Path(schema.SETTINGS_PATH)),
        ]
        targets.extend(
            (asset, images_root / asset.relative_to(payload / "images"))
            for asset in sorted((payload / "images").rglob("*")) if asset.is_file()
        )
        plate_marker = manifest_statuses.get("data/plate_layouts")
        plate_members = [
            item
            for item in manifest.files
            if item.path.startswith("data/plate_layouts/")
        ]
        if plate_marker is not None and plate_members:
            raise ArchiveValidationError("plate-layout collection has conflicting manifest state")
        plate_target = Path(schema.get_database_path()).parent / "plate_layouts"
        if plate_members:
            if any(item.status != "included" for item in plate_members):
                raise ArchiveValidationError("plate-layout collection has invalid manifest state")
            targets.append((payload / "plate_layouts", plate_target))
        elif plate_marker == "missing_at_source":
            targets.append((None, plate_target))
        elif plate_marker not in {None, "excluded_by_policy"}:
            raise ArchiveValidationError("plate-layout collection has invalid manifest state")

        for name, target in (
            ("objectives.json", Path(schema.get_objectives_path())),
            ("last_objective.json", Path(schema.get_last_objective_path())),
        ):
            status = manifest_statuses.get(f"data/{name}")
            if status == "included":
                targets.append((payload / name, target))
            elif status == "missing_at_source":
                targets.append((None, target))
            elif status != "excluded_by_policy":
                raise ArchiveValidationError(f"exact resource lacks manifest state: data/{name}")
        for new, target in targets:
            swap_targets.append((_local_swap_copy(new, target), target))
        return PreparedRestore(
            manifest,
            safety,
            app_version,
            backup_creator,
            temporary,
            targets,
            swap_targets,
            qsettings,
        )
    except Exception as exc:
        for (_incoming, _old, swap_root), _target in swap_targets:
            shutil.rmtree(swap_root, ignore_errors=True)
        if temporary is not None:
            temporary.cleanup()
        if isinstance(exc, FullRestoreError):
            raise
        raise FullRestoreError(f"restore preparation failed: {exc}") from exc


def execute_prepared_restore_swap(
    prepared: PreparedRestore,
    *,
    live_quiesced: bool,
    app_version: str | None = None,
    backup_creator: Callable[..., BackupResult] | None = None,
) -> RestoreSwap:
    """Create a fresh safety backup and replace live paths without using Qt.

    The caller must first prevent new database work and close all live database
    users. This function is intentionally suitable for a non-UI worker thread.
    """
    if not live_quiesced:
        raise FullRestoreError("live database users are not confirmed quiescent")

    replaced: list[tuple[Path, Path, bool]] = []
    try:
        creator = backup_creator or prepared.backup_creator
        creator(
            prepared.safety_backup,
            app_version=app_version or prepared.app_version,
        )
        for index, database_target in enumerate(
            (prepared.targets[0][1], prepared.targets[1][1])
        ):
            for suffix in ("-wal", "-shm", "-journal"):
                sidecar = database_target.with_name(database_target.name + suffix)
                if sidecar.exists():
                    old = prepared.swap_targets[index][0][2] / f"sidecar-{index}{suffix}"
                    old.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        os.replace(sidecar, old)
                    except FileNotFoundError:
                        continue
                    replaced.append((sidecar, old, True))
        for (incoming, old, _swap_root), target in prepared.swap_targets:
            existed = target.exists()
            target.parent.mkdir(parents=True, exist_ok=True)
            if existed:
                old.parent.mkdir(parents=True, exist_ok=True)
                os.replace(target, old)
            replaced.append((target, old, existed))
            if incoming.exists():
                os.replace(incoming, target)
        return RestoreSwap(prepared, prepared.safety_backup, replaced)
    except Exception as exc:
        swap = RestoreSwap(prepared, prepared.safety_backup, replaced)
        try:
            swap.rollback()
        except FullRestoreError:
            raise
        if isinstance(exc, FullRestoreError):
            raise
        raise FullRestoreError(f"restore swap failed: {exc}") from exc


def apply_prepared_restore(
    prepared: PreparedRestore,
    *,
    close_live: Callable[[], None],
    reopen_live: Callable[[], None],
    sanity_check: Callable[[], None],
) -> RestoreResult:
    """Perform the short live close/swap/reopen phase for a prepared restore."""
    from database import schema

    restored_qsettings: RestoredQSettings | None = None
    live_closed = False
    reopen_attempted = False
    swap: RestoreSwap | None = None
    try:
        close_live()
        live_closed = True
        swap = execute_prepared_restore_swap(
            prepared,
            live_quiesced=True,
        )
        restored_qsettings = apply_prepared_restore_qsettings(prepared)
        reopen_attempted = True
        reopen_live()
        sanity_check()
        _require_tables(Path(schema.get_database_path()), set(MAIN_DATABASE_TABLES))
        _require_tables(Path(schema.get_reference_database_path()), set(REFERENCE_DATABASE_TABLES))
        return swap.commit()
    except Exception as exc:
        if live_closed and reopen_attempted:
            try:
                close_live()
            except Exception as close_exc:
                raise FullRestoreError(
                    "restore failed and live state could not re-quiesce; "
                    "restored files and recovery copies were preserved"
                ) from close_exc
        if restored_qsettings is not None:
            restored_qsettings.rollback()
        rollback_error: FullRestoreError | None = None
        if swap is not None:
            try:
                swap.rollback()
            except FullRestoreError as rollback_exc:
                rollback_error = rollback_exc
        try:
            reopen_live()
        except Exception:
            pass
        if swap is None:
            prepared.cleanup()
        if rollback_error is not None:
            raise rollback_error
        if isinstance(exc, FullRestoreError):
            raise
        raise FullRestoreError(f"restore failed: {exc}") from exc


def restore_full_backup(
    archive_path: str | Path,
    *,
    app_version: str,
    close_live: Callable[[], None],
    reopen_live: Callable[[], None],
    sanity_check: Callable[[], None] = lambda: None,
    safety_backup_path: str | Path | None = None,
    backup_creator: Callable[..., BackupResult] = create_full_backup,
) -> RestoreResult:
    """Compatibility wrapper that prepares and applies a full restore."""
    prepared = prepare_full_restore(
        archive_path,
        app_version=app_version,
        safety_backup_path=safety_backup_path,
        backup_creator=backup_creator,
    )
    return apply_prepared_restore(
        prepared,
        close_live=close_live,
        reopen_live=reopen_live,
        sanity_check=sanity_check,
    )


_MISSING = object()
