"""Helpers for exporting/importing shared database bundles."""
import json
import sqlite3
import shutil
import tempfile
import zipfile
from collections.abc import Callable
from pathlib import Path

from app_identity import app_data_dir, legacy_app_data_dir
from database.schema import (
    get_connection,
    get_database_path,
    get_images_dir,
    get_objectives_path,
    get_reference_database_path,
    get_reference_connection,
    load_objectives,
    save_objectives,
)


def _safe_copy(src: Path, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if src.exists():
        shutil.copy2(src, dest)
    return dest


def _relativize_path(path_value: str | None, base_dir: Path) -> str | None:
    if not path_value:
        return path_value
    try:
        path = Path(path_value)
    except Exception:
        return path_value
    if path.is_absolute():
        path = _remap_known_local_path(path)
        try:
            return str(path.relative_to(base_dir))
        except ValueError:
            return str(path)
    return str(path)


def _resolve_archive_path(path_value: str | None, base_dir: Path) -> Path | None:
    if not path_value:
        return None
    path = Path(path_value)
    if path.is_absolute():
        return path
    return base_dir / path


def _remap_known_local_path(path: Path) -> Path:
    candidate = path
    if candidate.exists():
        return candidate
    try:
        legacy_root = legacy_app_data_dir().resolve()
        current_root = app_data_dir().resolve()
        rel = candidate.resolve(strict=False).relative_to(legacy_root)
    except Exception:
        return candidate
    remapped = current_root / rel
    return remapped if remapped.exists() else remapped


def _resolve_local_asset_path(path_value: str | None, base_dir: Path) -> Path | None:
    if not path_value:
        return None
    try:
        path = Path(path_value)
    except Exception:
        return None
    if path.is_absolute():
        return _remap_known_local_path(path)
    return base_dir / path


def _archive_image_arcname(path: Path, images_dir: Path) -> str:
    try:
        rel = path.relative_to(images_dir)
    except ValueError:
        rel = Path(path.name)
    return str(Path("images") / rel)


def _collect_calibration_asset_paths(
    src_conn: sqlite3.Connection | None,
    images_dir: Path,
) -> list[Path]:
    if src_conn is None:
        return []
    src_conn.row_factory = sqlite3.Row
    assets: dict[str, Path] = {}
    try:
        rows = src_conn.execute(
            "SELECT image_filepath, measurements_json FROM calibrations ORDER BY id"
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    for row in rows:
        image_path = _resolve_local_asset_path(row["image_filepath"], images_dir)
        if image_path and image_path.exists() and image_path.is_file():
            assets[str(image_path.resolve())] = image_path
        measurements_json = row["measurements_json"]
        if not measurements_json:
            continue
        try:
            loaded = json.loads(measurements_json)
        except Exception:
            loaded = None
        if not isinstance(loaded, dict):
            continue
        for entry in loaded.get("images", []):
            if not isinstance(entry, dict):
                continue
            entry_path = _resolve_local_asset_path(entry.get("path"), images_dir)
            if entry_path and entry_path.exists() and entry_path.is_file():
                assets[str(entry_path.resolve())] = entry_path
    return sorted(assets.values(), key=lambda path: str(path))


def _restore_archive_asset(
    path_value: str | None,
    src_base_dir: Path,
    dest_base_dir: Path,
    *,
    copy_file: bool,
    warnings: list[str],
    warning_label: str,
) -> str | None:
    if not path_value:
        return path_value
    src_path = _resolve_archive_path(path_value, src_base_dir)
    if src_path is None:
        return path_value
    if src_base_dir in src_path.parents:
        rel = src_path.relative_to(src_base_dir)
        dest_path = dest_base_dir / rel
    else:
        dest_path = dest_base_dir / src_path.name
    if not src_path.exists():
        warnings.append(f"Missing bundled {warning_label}: {path_value}")
        return None
    if copy_file:
        _safe_copy(src_path, dest_path)
    return str(dest_path)


def _copy_table_schema(src_conn: sqlite3.Connection, dest_conn: sqlite3.Connection, table: str) -> None:
    row = src_conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    if row and row[0]:
        dest_conn.execute(row[0])


def _copy_table_rows(
    src_conn: sqlite3.Connection,
    dest_conn: sqlite3.Connection,
    table: str,
    transform_row=None,
) -> int:
    src_conn.row_factory = sqlite3.Row
    cursor = src_conn.execute(f"SELECT * FROM {table}")
    rows = cursor.fetchall()
    if not rows:
        return 0
    columns = list(rows[0].keys())
    placeholders = ", ".join(["?"] * len(columns))
    insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    values = []
    for row in rows:
        data = dict(row)
        if transform_row:
            data = transform_row(data)
        values.append([data.get(col) for col in columns])
    dest_conn.executemany(insert_sql, values)
    return len(values)


def _normalize_reference_row_key(row: dict | None) -> str:
    payload = {
        str(key): row.get(key)
        for key in sorted((row or {}).keys())
        if str(key) not in {"id", "updated_at"}
    }
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _open_source_db_connection() -> sqlite3.Connection:
    """Open the main DB for bundle export with a lightweight preflight.

    On some systems the first SQLite open in a short-lived process can report
    an intermittent "unable to open database file" during immediate follow-up
    metadata queries. A quick warm-up connection avoids that flake.
    """
    db_path = get_database_path()
    warm_conn = sqlite3.connect(db_path, timeout=10)
    try:
        warm_conn.execute("SELECT name FROM sqlite_master LIMIT 1").fetchone()
    finally:
        warm_conn.close()
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def export_database_bundle(
    zip_path: str,
    *,
    include_observations: bool = True,
    include_images: bool = True,
    include_measurements: bool = True,
    include_calibrations: bool = True,
    include_reference_values: bool = True,
    progress_cb: Callable[[str, int, int], None] | None = None,
) -> None:
    """Export selected data to a zip file."""
    if include_measurements:
        include_images = True
    if include_images:
        include_observations = True

    images_dir = get_images_dir()
    ref_path = get_reference_database_path()
    include_main_db = any(
        [include_observations, include_images, include_measurements, include_calibrations]
    )
    temp_dir = Path(tempfile.mkdtemp())
    objectives_path = get_objectives_path()

    selected_tables: list[str] = []
    if include_observations:
        selected_tables.append("observations")
    if include_images or include_measurements:
        selected_tables.append("images")
    if include_measurements:
        selected_tables.extend(["spore_measurements", "spore_annotations"])
    if include_calibrations:
        selected_tables.append("calibrations")

    image_files: list[Path] = []
    if include_images and images_dir.exists():
        image_files = [path for path in images_dir.rglob("*") if path.is_file()]

    calibration_asset_files: list[Path] = []
    source_db_path = get_database_path()
    if include_calibrations and not include_images and source_db_path.exists():
        preview_conn = _open_source_db_connection()
        try:
            calibration_asset_files = _collect_calibration_asset_paths(preview_conn, images_dir)
        finally:
            preview_conn.close()

    total_steps = max(
        1,
        len(selected_tables)
        + (1 if include_main_db else 0)
        + len(image_files)
        + len(calibration_asset_files)
        + (1 if include_calibrations and objectives_path.exists() else 0)
        + (1 if include_reference_values and ref_path.exists() else 0),
    )
    completed_steps = 0

    def _emit_progress(text: str) -> None:
        if progress_cb is not None:
            progress_cb(text, completed_steps, total_steps)

    try:
        db_path = temp_dir / "mushrooms.db"
        src_conn = None
        if include_main_db:
            src_conn = _open_source_db_connection()
            dest_conn = sqlite3.connect(db_path)

            for table in selected_tables:
                _emit_progress(f"Exporting table: {table}...")
                _copy_table_schema(src_conn, dest_conn, table)

                def _transform(data, table_name=table):
                    if table_name == "images":
                        data["filepath"] = _relativize_path(data.get("filepath"), images_dir)
                        data["original_filepath"] = _relativize_path(
                            data.get("original_filepath"), images_dir
                        )
                    if table_name == "calibrations":
                        data["image_filepath"] = _relativize_path(
                            data.get("image_filepath"), images_dir
                        )
                        measurements_json = data.get("measurements_json")
                        if measurements_json:
                            try:
                                loaded = json.loads(measurements_json)
                            except Exception:
                                loaded = None
                            if isinstance(loaded, dict):
                                for entry in loaded.get("images", []):
                                    if isinstance(entry, dict) and entry.get("path"):
                                        entry["path"] = _relativize_path(
                                            entry.get("path"), images_dir
                                        )
                                data["measurements_json"] = json.dumps(loaded, ensure_ascii=False)
                    return data

                _copy_table_rows(src_conn, dest_conn, table, transform_row=_transform)
                completed_steps += 1
                _emit_progress(f"Exported table: {table}.")

            dest_conn.commit()
            dest_conn.close()
        elif include_calibrations and source_db_path.exists():
            src_conn = _open_source_db_connection()

        if include_calibrations and not include_images:
            calibration_asset_files = _collect_calibration_asset_paths(src_conn, images_dir)

        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            if include_main_db and db_path.exists():
                _emit_progress("Adding database file...")
                zf.write(db_path, arcname="mushrooms.db")
                completed_steps += 1
                _emit_progress("Database file added.")
            if include_images and image_files:
                for idx, path in enumerate(image_files, start=1):
                    zf.write(path, arcname=_archive_image_arcname(path, images_dir))
                    completed_steps += 1
                    if idx == 1 or idx == len(image_files) or idx % 10 == 0:
                        _emit_progress(f"Adding images... ({idx}/{len(image_files)})")
            elif calibration_asset_files:
                for idx, path in enumerate(calibration_asset_files, start=1):
                    zf.write(path, arcname=_archive_image_arcname(path, images_dir))
                    completed_steps += 1
                    if idx == 1 or idx == len(calibration_asset_files) or idx % 10 == 0:
                        _emit_progress(
                            f"Adding calibration images... ({idx}/{len(calibration_asset_files)})"
                        )
            if include_calibrations and objectives_path.exists():
                _emit_progress("Adding objective profiles...")
                zf.write(objectives_path, arcname="objectives.json")
                completed_steps += 1
                _emit_progress("Objective profiles added.")
            if include_reference_values and ref_path.exists():
                _emit_progress("Adding reference values...")
                zf.write(ref_path, arcname="reference_values.db")
                completed_steps += 1
                _emit_progress("Reference values added.")
        completed_steps = total_steps
        _emit_progress("Export complete.")
    finally:
        if src_conn is not None:
            src_conn.close()
        shutil.rmtree(temp_dir, ignore_errors=True)


def import_database_bundle(
    zip_path: str,
    *,
    include_observations: bool = True,
    include_images: bool = True,
    include_measurements: bool = True,
    include_calibrations: bool = True,
    include_reference_values: bool = True,
) -> dict:
    """Import selected data from a bundle into the current DB."""
    if include_measurements:
        include_images = True
    if include_images:
        include_observations = True

    temp_dir = Path(tempfile.mkdtemp())
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(temp_dir)

        src_db_path = temp_dir / "mushrooms.db"
        if any([include_observations, include_images, include_measurements, include_calibrations]):
            if not src_db_path.exists():
                raise FileNotFoundError("No mushrooms.db found in bundle.")

        src_images_dir = temp_dir / "images"
        dest_images_dir = get_images_dir()
        if include_images or include_calibrations:
            dest_images_dir.mkdir(parents=True, exist_ok=True)
        warnings: list[str] = []

        src_conn = None
        src_cur = None
        if src_db_path.exists():
            src_conn = sqlite3.connect(src_db_path)
            src_conn.row_factory = sqlite3.Row
            src_cur = src_conn.cursor()

        dest_conn = get_connection()
        dest_conn.row_factory = sqlite3.Row
        dest_cur = dest_conn.cursor()

        obs_map: dict[int, int] = {}
        img_map: dict[int, int] = {}
        meas_map: dict[int, int] = {}
        imported_calibrations = 0
        imported_refs = 0
        imported_objectives = 0
        fallback_objectives: dict[str, dict] = {}

        if include_observations and src_cur is not None:
            src_cur.execute("SELECT * FROM observations ORDER BY id")
            for row in src_cur.fetchall():
                data = dict(row)
                data.pop("id", None)
                columns = [k for k in data.keys()]
                values = [data[k] for k in columns]
                placeholders = ", ".join(["?"] * len(columns))
                dest_cur.execute(
                    f"INSERT INTO observations ({', '.join(columns)}) VALUES ({placeholders})",
                    values
                )
                obs_map[row["id"]] = dest_cur.lastrowid

        if (include_images or include_measurements) and src_cur is not None:
            src_cur.execute("SELECT * FROM images ORDER BY id")
            for row in src_cur.fetchall():
                data = dict(row)
                old_image_id = data.pop("id", None)
                old_obs_id = data.get("observation_id")
                if include_observations and old_obs_id in obs_map:
                    data["observation_id"] = obs_map[old_obs_id]
                elif not include_observations:
                    data["observation_id"] = None
                if not include_calibrations:
                    data["calibration_id"] = None

                restored_path = _restore_archive_asset(
                    data.get("filepath"),
                    src_images_dir,
                    dest_images_dir,
                    copy_file=include_images,
                    warnings=warnings,
                    warning_label="image file",
                )
                if restored_path is not None:
                    data["filepath"] = restored_path

                restored_original = _restore_archive_asset(
                    data.get("original_filepath"),
                    src_images_dir,
                    dest_images_dir,
                    copy_file=include_images,
                    warnings=warnings,
                    warning_label="original image file",
                )
                if restored_original is not None:
                    data["original_filepath"] = restored_original

                columns = [k for k in data.keys()]
                values = [data[k] for k in columns]
                placeholders = ", ".join(["?"] * len(columns))
                dest_cur.execute(
                    f"INSERT INTO images ({', '.join(columns)}) VALUES ({placeholders})",
                    values
                )
                img_map[old_image_id] = dest_cur.lastrowid

        if include_measurements and src_cur is not None:
            if not img_map:
                raise ValueError("Cannot import measurements without images.")
            src_cur.execute("SELECT * FROM spore_measurements ORDER BY id")
            for row in src_cur.fetchall():
                data = dict(row)
                old_id = data.pop("id", None)
                old_image_id = data.get("image_id")
                if old_image_id in img_map:
                    data["image_id"] = img_map[old_image_id]
                else:
                    continue
                columns = [k for k in data.keys()]
                values = [data[k] for k in columns]
                placeholders = ", ".join(["?"] * len(columns))
                dest_cur.execute(
                    f"INSERT INTO spore_measurements ({', '.join(columns)}) VALUES ({placeholders})",
                    values
                )
                meas_map[old_id] = dest_cur.lastrowid

            src_cur.execute("SELECT * FROM spore_annotations ORDER BY id")
            for row in src_cur.fetchall():
                data = dict(row)
                data.pop("id", None)
                old_image_id = data.get("image_id")
                old_meas_id = data.get("measurement_id")
                if old_image_id in img_map:
                    data["image_id"] = img_map[old_image_id]
                else:
                    continue
                if old_meas_id in meas_map:
                    data["measurement_id"] = meas_map[old_meas_id]
                else:
                    data["measurement_id"] = None
                columns = [k for k in data.keys()]
                values = [data[k] for k in columns]
                placeholders = ", ".join(["?"] * len(columns))
                dest_cur.execute(
                    f"INSERT INTO spore_annotations ({', '.join(columns)}) VALUES ({placeholders})",
                    values
                )

        if include_calibrations and src_cur is not None:
            src_cur.execute("SELECT * FROM calibrations ORDER BY id")
            for row in src_cur.fetchall():
                data = dict(row)
                data.pop("id", None)
                objective_key = str(data.get("objective_key") or "").strip()

                restored_image = _restore_archive_asset(
                    data.get("image_filepath"),
                    src_images_dir,
                    dest_images_dir,
                    copy_file=True,
                    warnings=warnings,
                    warning_label="calibration image",
                )
                if restored_image is not None:
                    data["image_filepath"] = restored_image

                measurements_json = data.get("measurements_json")
                if measurements_json:
                    try:
                        loaded = json.loads(measurements_json)
                    except Exception:
                        loaded = None
                    if isinstance(loaded, dict):
                        for entry in loaded.get("images", []):
                            if isinstance(entry, dict) and entry.get("path"):
                                restored_entry = _restore_archive_asset(
                                    entry.get("path"),
                                    src_images_dir,
                                    dest_images_dir,
                                    copy_file=True,
                                    warnings=warnings,
                                    warning_label="calibration auxiliary image",
                                )
                                if restored_entry is not None:
                                    entry["path"] = restored_entry
                                else:
                                    entry.pop("path", None)
                        data["measurements_json"] = json.dumps(loaded, ensure_ascii=False)

                if objective_key and objective_key not in fallback_objectives:
                    fallback_objectives[objective_key] = {
                        "microns_per_pixel": data.get("microns_per_pixel"),
                        "target_sampling_pct": data.get("target_sampling_pct"),
                        "resample_scale_factor": data.get("resample_scale_factor"),
                    }

                columns = [k for k in data.keys()]
                values = [data[k] for k in columns]
                placeholders = ", ".join(["?"] * len(columns))
                dest_cur.execute(
                    f"INSERT INTO calibrations ({', '.join(columns)}) VALUES ({placeholders})",
                    values
                )
                imported_calibrations += 1

            objectives_bundle_path = temp_dir / "objectives.json"
            existing_objectives = load_objectives()
            merged_objectives = dict(existing_objectives)
            if objectives_bundle_path.exists():
                try:
                    bundled_objectives = json.loads(objectives_bundle_path.read_text(encoding="utf-8"))
                except Exception:
                    bundled_objectives = None
                if isinstance(bundled_objectives, dict):
                    for key, value in bundled_objectives.items():
                        if not isinstance(value, dict):
                            continue
                        merged_objectives[str(key)] = dict(value)
                    imported_objectives = len(
                        {
                            str(key)
                            for key, value in bundled_objectives.items()
                            if isinstance(value, dict)
                        }
                    )
            for key, value in fallback_objectives.items():
                if key not in merged_objectives:
                    merged_objectives[key] = dict(value)
                    imported_objectives += 1
            if merged_objectives != existing_objectives:
                save_objectives(merged_objectives)

        if include_reference_values:
            ref_path = temp_dir / "reference_values.db"
            if ref_path.exists():
                ref_src = sqlite3.connect(ref_path)
                ref_src.row_factory = sqlite3.Row
                ref_cur = ref_src.cursor()
                ref_dest = get_reference_connection()
                ref_dest.row_factory = sqlite3.Row
                ref_dest_cur = ref_dest.cursor()
                existing_ref_keys = {
                    _normalize_reference_row_key(dict(row))
                    for row in ref_dest_cur.execute("SELECT * FROM reference_values").fetchall()
                }
                ref_cur.execute("SELECT * FROM reference_values ORDER BY id")
                for row in ref_cur.fetchall():
                    data = dict(row)
                    row_key = _normalize_reference_row_key(data)
                    if row_key in existing_ref_keys:
                        continue
                    data.pop("id", None)
                    columns = [k for k in data.keys()]
                    values = [data[k] for k in columns]
                    placeholders = ", ".join(["?"] * len(columns))
                    ref_dest_cur.execute(
                        f"INSERT INTO reference_values ({', '.join(columns)}) VALUES ({placeholders})",
                        values,
                    )
                    existing_ref_keys.add(row_key)
                    imported_refs += 1
                ref_dest.commit()
                ref_dest.close()
                ref_src.close()

        dest_conn.commit()
        return {
            "observations": len(obs_map),
            "images": len(img_map),
            "measurements": len(meas_map),
            "calibrations": imported_calibrations,
            "objectives": imported_objectives,
            "reference_values": imported_refs,
            "warnings": warnings,
        }
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
