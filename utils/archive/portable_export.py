"""Creation of portable archives rooted at explicitly selected observations."""

from __future__ import annotations

import json
import os
import platform
import sqlite3
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from zipfile import ZIP_DEFLATED, ZipFile

from utils.archive.checksums import sha256_file
from utils.archive.full_backup import (
    BackupResult,
    _StagedFile,
    _candidate,
    _json_image_entries,
    _report_progress,
    _require_destination_space,
    _resolve_row_path,
    _snapshot_database,
    _stable_json_bytes,
    _suffix,
)
from utils.archive.manifest import ArchiveManifest, ManifestFile, build_manifest
from utils.archive.paths import canonical_archive_path
from utils.archive.validation import validate_portable_observations, verify_sqlite_integrity
from utils.raw_detection import is_raw_image_path


class PortableExportError(RuntimeError):
    """Raised when a selected-observation archive cannot be created safely."""


def _placeholders(values: set[object]) -> str:
    return ",".join("?" for _value in values)


def _prune_main_database(database_path: Path, observation_ids: set[int]) -> None:
    ordered_ids = tuple(sorted(observation_ids))
    placeholders = _placeholders(observation_ids)
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("PRAGMA secure_delete=ON")
        found = {
            int(row[0])
            for row in connection.execute(
                f"SELECT id FROM observations WHERE id IN ({placeholders})", ordered_ids
            )
        }
        missing = observation_ids - found
        if missing:
            raise PortableExportError(
                "selected observations do not exist: " + ", ".join(str(value) for value in sorted(missing))
            )

        connection.execute(
            f"DELETE FROM observations WHERE id NOT IN ({placeholders})", ordered_ids
        )
        connection.execute("DELETE FROM images WHERE observation_id NOT IN (SELECT id FROM observations)")
        connection.execute("DELETE FROM spore_measurements WHERE image_id NOT IN (SELECT id FROM images)")
        connection.execute("DELETE FROM spore_annotations WHERE image_id NOT IN (SELECT id FROM images)")
        dangling_annotation = connection.execute(
            """
            SELECT id FROM spore_annotations
            WHERE measurement_id IS NOT NULL
              AND measurement_id NOT IN (SELECT id FROM spore_measurements)
            LIMIT 1
            """
        ).fetchone()
        if dangling_annotation is not None:
            raise PortableExportError(
                f"annotation {dangling_annotation[0]} references a measurement outside its image closure"
            )
        connection.execute("DELETE FROM session_logs WHERE observation_id NOT IN (SELECT id FROM observations)")
        connection.execute(
            "DELETE FROM observation_reference_uses WHERE observation_id NOT IN (SELECT id FROM observations)"
        )
        dangling_calibration = connection.execute(
            """
            SELECT DISTINCT calibration_id FROM images
            WHERE calibration_id IS NOT NULL
              AND calibration_id NOT IN (SELECT id FROM calibrations)
            LIMIT 1
            """
        ).fetchone()
        if dangling_calibration is not None:
            raise PortableExportError(
                f"selected image references missing calibration {dangling_calibration[0]}"
            )
        conflicting_asset = connection.execute(
            """
            SELECT asset.id
            FROM calibration_assets AS asset
            JOIN calibrations AS linked ON linked.id = asset.calibration_id
            WHERE asset.calibration_id IS NOT NULL
              AND asset.calibration_uuid IS NOT NULL
              AND TRIM(asset.calibration_uuid) != ''
              AND asset.calibration_uuid != linked.calibration_uuid
              AND (asset.calibration_id IN (
                    SELECT DISTINCT calibration_id FROM images WHERE calibration_id IS NOT NULL
                  ) OR asset.calibration_uuid IN (
                    SELECT calibration_uuid FROM calibrations WHERE id IN (
                      SELECT DISTINCT calibration_id FROM images WHERE calibration_id IS NOT NULL
                    )
                  ))
            LIMIT 1
            """
        ).fetchone()
        if conflicting_asset is not None:
            raise PortableExportError(
                f"calibration asset {conflicting_asset[0]} has conflicting calibration identities"
            )
        connection.execute(
            "DELETE FROM calibration_assets WHERE "
            "COALESCE(calibration_id, -1) NOT IN (SELECT DISTINCT calibration_id FROM images WHERE calibration_id IS NOT NULL) "
            "AND NOT (calibration_id IS NULL AND COALESCE(calibration_uuid, '') IN ("
            "SELECT calibration_uuid FROM calibrations WHERE id IN ("
            "SELECT DISTINCT calibration_id FROM images WHERE calibration_id IS NOT NULL)))"
        )
        connection.execute(
            "DELETE FROM calibrations WHERE id NOT IN ("
            "SELECT DISTINCT calibration_id FROM images WHERE calibration_id IS NOT NULL)"
        )
        for table in (
            "settings", "image_tombstones", "thumbnails",
            "portable_import_provenance",
            "observation_reference_use_cloud_sync_state",
            "observation_reference_use_cloud_tombstones",
            "observation_reference_use_cloud_pull_cursors",
            "observation_reference_use_cloud_remote_tombstone_markers",
        ):
            connection.execute(f"DELETE FROM {table}")
        connection.commit()
        connection.execute("VACUUM")


def _prune_reference_database(main_database: Path, reference_database: Path) -> None:
    with sqlite3.connect(main_database) as main:
        set_ids = {
            str(row[0])
            for row in main.execute(
                "SELECT DISTINCT reference_measurement_set_id FROM observation_reference_uses"
            )
        }
    with sqlite3.connect(reference_database) as connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("PRAGMA secure_delete=ON")
        if set_ids:
            ordered = tuple(sorted(set_ids))
            placeholders = _placeholders(set_ids)
            found = {
                str(row[0])
                for row in connection.execute(
                    f"SELECT id FROM reference_measurement_sets WHERE id IN ({placeholders})", ordered
                )
            }
            missing = set_ids - found
            if missing:
                raise PortableExportError(
                    "observation reference links are dangling: " + ", ".join(sorted(missing))
                )
            missing_treatment = connection.execute(
                f"""
                SELECT ms.id FROM reference_measurement_sets AS ms
                LEFT JOIN reference_taxon_treatments AS treatment
                  ON treatment.id = ms.taxon_treatment_id
                WHERE ms.id IN ({placeholders}) AND treatment.id IS NULL
                LIMIT 1
                """,
                ordered,
            ).fetchone()
            if missing_treatment is not None:
                raise PortableExportError(
                    f"reference measurement set {missing_treatment[0]} has no taxon treatment"
                )
            missing_work = connection.execute(
                f"""
                SELECT ms.id FROM reference_measurement_sets AS ms
                JOIN reference_taxon_treatments AS treatment
                  ON treatment.id = ms.taxon_treatment_id
                LEFT JOIN reference_works AS work ON work.id = treatment.reference_work_id
                WHERE ms.id IN ({placeholders}) AND work.id IS NULL
                LIMIT 1
                """,
                ordered,
            ).fetchone()
            if missing_work is not None:
                raise PortableExportError(
                    f"reference measurement set {missing_work[0]} has no reference work"
                )
            missing_legacy = connection.execute(
                f"""
                SELECT ms.id FROM reference_measurement_sets AS ms
                LEFT JOIN reference_values AS legacy ON legacy.id = ms.legacy_reference_value_id
                WHERE ms.id IN ({placeholders})
                  AND ms.legacy_reference_value_id IS NOT NULL
                  AND legacy.id IS NULL
                LIMIT 1
                """,
                ordered,
            ).fetchone()
            if missing_legacy is not None:
                raise PortableExportError(
                    f"reference measurement set {missing_legacy[0]} has no legacy reference value"
                )
            connection.execute(
                f"DELETE FROM reference_measurement_sets WHERE id NOT IN ({placeholders})", ordered
            )
        else:
            connection.execute("DELETE FROM reference_measurement_sets")
        connection.execute(
            "DELETE FROM reference_taxon_treatments WHERE id NOT IN ("
            "SELECT DISTINCT taxon_treatment_id FROM reference_measurement_sets)"
        )
        connection.execute(
            "DELETE FROM reference_works WHERE id NOT IN ("
            "SELECT DISTINCT reference_work_id FROM reference_taxon_treatments)"
        )
        connection.execute(
            "DELETE FROM reference_values WHERE id NOT IN ("
            "SELECT DISTINCT legacy_reference_value_id FROM reference_measurement_sets "
            "WHERE legacy_reference_value_id IS NOT NULL)"
        )
        connection.execute("DELETE FROM reference_cloud_sync_state")
        connection.execute("DELETE FROM reference_cloud_tombstones")
        connection.execute("DELETE FROM reference_cloud_pull_cursors")
        connection.execute("DELETE FROM reference_cloud_remote_tombstone_markers")
        connection.execute("DELETE FROM reference_measurement_set_preferences")
        connection.commit()
        connection.execute("VACUUM")


def _filtered_objectives(database_path: Path, objectives_path: Path) -> bytes:
    if not objectives_path.is_file():
        return _stable_json_bytes({})
    try:
        loaded = json.loads(objectives_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PortableExportError("objective profiles are not valid JSON") from exc
    if not isinstance(loaded, dict):
        raise PortableExportError("objective profiles must be a JSON object")
    with sqlite3.connect(database_path) as connection:
        names = {
            str(row[0]).strip()
            for row in connection.execute(
                "SELECT objective_name FROM images WHERE objective_name IS NOT NULL "
                "UNION SELECT objective_key FROM calibrations WHERE objective_key IS NOT NULL"
            )
            if str(row[0]).strip()
        }
    from database.schema import resolve_objective_key

    keys = {key for name in names if (key := resolve_objective_key(name, loaded)) is not None}
    return _stable_json_bytes({key: loaded[key] for key in sorted(keys)})


def _collect_assets(database_path: Path, images_dir: Path) -> list[_StagedFile]:
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
                    candidates.append(_candidate(
                        source,
                        f"portable/assets/{root}/{row['id']}/{label}{_suffix(source)}",
                        excluded=excluded or is_raw_image_path(source),
                    ))
        excluded_calibration_paths: set[Path] = set()
        for row in connection.execute(
            "SELECT local_path, original_path, source_role, file_purpose, metadata_json "
            "FROM calibration_assets ORDER BY id"
        ):
            excluded = (
                str(row["source_role"] or "").lower() == "cloud_recovery_cache"
                or str(row["file_purpose"] or "").lower() == "cache"
            )
            if not excluded:
                continue
            for field in ("local_path", "original_path"):
                source = _resolve_row_path(row[field], images_dir)
                if source is not None:
                    excluded_calibration_paths.add(source.resolve())
            try:
                metadata = json.loads(row["metadata_json"]) if row["metadata_json"] else None
            except (json.JSONDecodeError, TypeError):
                metadata = None
            if isinstance(metadata, dict):
                values = [metadata.get(key) for key in (
                    "source_path", "working_path", "original_path", "local_path"
                )]
                values.extend(metadata.get("companion_paths") or [])
                for value in values:
                    source = _resolve_row_path(value, images_dir)
                    if source is not None:
                        excluded_calibration_paths.add(source.resolve())

        for row in connection.execute(
            "SELECT id, image_filepath, measurements_json FROM calibrations ORDER BY id"
        ):
            source = _resolve_row_path(row["image_filepath"], images_dir)
            if source is not None:
                candidates.append(_candidate(
                    source,
                    f"portable/assets/calibrations/records/{row['id']}/working{_suffix(source)}",
                    excluded=(
                        source.resolve() in excluded_calibration_paths
                        or is_raw_image_path(source)
                    ),
                ))
            for index, label, value, excluded in _json_image_entries(row["measurements_json"]):
                source = _resolve_row_path(value, images_dir)
                if source is not None:
                    candidates.append(_candidate(
                        source,
                        f"portable/assets/calibrations/records/{row['id']}/metadata-{index}-{label}{_suffix(source)}",
                        excluded=excluded or is_raw_image_path(source),
                    ))
        for row in connection.execute(
            "SELECT id, local_path, original_path, source_role, file_purpose, metadata_json "
            "FROM calibration_assets ORDER BY id"
        ):
            excluded = (
                str(row["source_role"] or "").lower() == "cloud_recovery_cache"
                or str(row["file_purpose"] or "").lower() == "cache"
            )
            for field, label in (("local_path", "local"), ("original_path", "original")):
                source = _resolve_row_path(row[field], images_dir)
                if source is not None:
                    candidates.append(_candidate(
                        source,
                        f"portable/assets/calibrations/assets/{row['id']}/{label}{_suffix(source)}",
                        excluded=excluded or is_raw_image_path(source),
                    ))
            try:
                metadata = json.loads(row["metadata_json"]) if row["metadata_json"] else None
            except (json.JSONDecodeError, TypeError):
                metadata = None
            if isinstance(metadata, dict):
                for label in ("source_path", "working_path", "original_path", "local_path"):
                    source = _resolve_row_path(metadata.get(label), images_dir)
                    if source is not None:
                        candidates.append(_candidate(
                            source,
                            f"portable/assets/calibrations/assets/{row['id']}/metadata-{label}{_suffix(source)}",
                            excluded=excluded or is_raw_image_path(source),
                        ))
                for index, value in enumerate(metadata.get("companion_paths") or []):
                    source = _resolve_row_path(value, images_dir)
                    if source is not None:
                        candidates.append(_candidate(
                            source,
                            f"portable/assets/calibrations/assets/{row['id']}/metadata-companion-{index}{_suffix(source)}",
                            excluded=excluded or is_raw_image_path(source),
                        ))
    return candidates


def _contents(main_database: Path, reference_database: Path) -> dict[str, int]:
    main_tables = (
        "observations", "images", "spore_measurements", "spore_annotations",
        "session_logs", "calibrations", "calibration_assets", "observation_reference_uses",
    )
    reference_tables = (
        "reference_values", "reference_works", "reference_taxon_treatments",
        "reference_measurement_sets",
    )
    counts: dict[str, int] = {}
    with sqlite3.connect(main_database) as connection:
        for table in main_tables:
            counts[table] = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    with sqlite3.connect(reference_database) as connection:
        for table in reference_tables:
            counts[table] = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    counts["measurements"] = counts.pop("spore_measurements")
    counts["annotations"] = counts.pop("spore_annotations")
    return counts


def export_observations(
    observation_ids: set[int],
    destination: str | Path,
    *,
    app_version: str,
    archive_id: str | None = None,
    created_at: str | None = None,
    source_platform: str | None = None,
    validate: Callable[[str | Path], ArchiveManifest] = validate_portable_observations,
    progress_callback: Callable[[str, int], None] | None = None,
) -> BackupResult:
    """Export the exact dependency closure of explicit observation roots."""
    try:
        normalized_ids = {int(value) for value in observation_ids}
    except (TypeError, ValueError) as exc:
        raise PortableExportError("observation IDs must be integers") from exc
    if not normalized_ids or any(value <= 0 for value in normalized_ids):
        raise PortableExportError("at least one positive observation ID is required")

    from database.schema import (
        get_database_path,
        get_images_dir,
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
        with tempfile.TemporaryDirectory(prefix="sporely-portable-") as temporary:
            staging = Path(temporary)
            staged_main = staging / "portable/mushrooms.db"
            staged_reference = staging / "portable/reference_values.db"
            _snapshot_database(Path(get_database_path()), staged_main)
            _prune_main_database(staged_main, normalized_ids)
            verify_sqlite_integrity(staged_main)
            _snapshot_database(Path(get_reference_database_path()), staged_reference)
            _prune_reference_database(staged_main, staged_reference)
            verify_sqlite_integrity(staged_reference)
            objectives = staging / "portable/objectives.json"
            objectives.parent.mkdir(parents=True, exist_ok=True)
            objectives.write_bytes(_filtered_objectives(staged_main, Path(get_objectives_path())))

            files = [
                _StagedFile("portable/mushrooms.db", staged_main, "included"),
                _StagedFile("portable/reference_values.db", staged_reference, "included"),
                _StagedFile("portable/objectives.json", objectives, "included"),
                *_collect_assets(staged_main, Path(get_images_dir())),
            ]
            seen: set[str] = set()
            for item in files:
                canonical_archive_path(item.archive_path)
                folded = item.archive_path.casefold()
                if folded in seen:
                    raise PortableExportError(f"duplicate archive destination: {item.archive_path}")
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
            warning_identities: set[str] = set()
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
                        identity = item.warning_identity or item.archive_path
                        if identity not in warning_identities:
                            warning_identities.add(identity)
                            warnings.append(item.archive_path)
            manifest = build_manifest(
                mode="portable_observations",
                archive_id=archive_id or str(uuid.uuid4()),
                created_at=created_at or datetime.now(timezone.utc).isoformat(),
                app_version=app_version,
                source_platform=source_platform or platform.platform(),
                contents=_contents(staged_main, staged_reference),
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
    except PortableExportError:
        raise
    except Exception as exc:
        raise PortableExportError(f"portable export failed: {exc}") from exc
    finally:
        if temporary_zip is not None:
            temporary_zip.unlink(missing_ok=True)
