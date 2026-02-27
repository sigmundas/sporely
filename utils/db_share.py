"""Helpers for exporting/importing shared database bundles."""
import json
import sqlite3
import zipfile
import tempfile
import shutil
from collections.abc import Callable
from pathlib import Path

from database.schema import (
    DATABASE_PATH,
    get_connection,
    get_images_dir,
    get_reference_database_path,
    get_reference_connection,
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
    images_dir = get_images_dir()
    ref_path = get_reference_database_path()
    include_main_db = any(
        [include_observations, include_images, include_measurements, include_calibrations]
    )
    temp_dir = Path(tempfile.mkdtemp())

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

    total_steps = max(
        1,
        len(selected_tables)
        + (1 if include_main_db else 0)
        + len(image_files)
        + (1 if include_reference_values and ref_path.exists() else 0),
    )
    completed_steps = 0

    def _emit_progress(text: str) -> None:
        if progress_cb is not None:
            progress_cb(text, completed_steps, total_steps)

    try:
        db_path = temp_dir / "mushrooms.db"
        if include_main_db:
            src_conn = sqlite3.connect(DATABASE_PATH)
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
            src_conn.close()

        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            if include_main_db and db_path.exists():
                _emit_progress("Adding database file...")
                zf.write(db_path, arcname="mushrooms.db")
                completed_steps += 1
                _emit_progress("Database file added.")
            if include_images and image_files:
                for idx, path in enumerate(image_files, start=1):
                    zf.write(path, arcname=str(Path("images") / path.relative_to(images_dir)))
                    completed_steps += 1
                    if idx == 1 or idx == len(image_files) or idx % 10 == 0:
                        _emit_progress(f"Adding images... ({idx}/{len(image_files)})")
            if include_reference_values and ref_path.exists():
                _emit_progress("Adding reference values...")
                zf.write(ref_path, arcname="reference_values.db")
                completed_steps += 1
                _emit_progress("Reference values added.")
        completed_steps = total_steps
        _emit_progress("Export complete.")
    finally:
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
        if include_images:
            dest_images_dir.mkdir(parents=True, exist_ok=True)

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

                src_path = _resolve_archive_path(data.get("filepath"), src_images_dir)
                dest_path = None
                if src_path and src_images_dir in src_path.parents:
                    rel = src_path.relative_to(src_images_dir)
                    dest_path = dest_images_dir / rel
                elif src_path:
                    dest_path = dest_images_dir / src_path.name

                if dest_path:
                    if include_images:
                        _safe_copy(src_path, dest_path)
                    data["filepath"] = str(dest_path)

                original_path = _resolve_archive_path(data.get("original_filepath"), src_images_dir)
                if original_path:
                    if src_images_dir in original_path.parents:
                        rel = original_path.relative_to(src_images_dir)
                        dest_orig = dest_images_dir / rel
                    else:
                        dest_orig = dest_images_dir / original_path.name
                    if include_images:
                        _safe_copy(original_path, dest_orig)
                    data["original_filepath"] = str(dest_orig)

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

                image_path = _resolve_archive_path(data.get("image_filepath"), src_images_dir)
                if image_path:
                    if src_images_dir in image_path.parents:
                        rel = image_path.relative_to(src_images_dir)
                        dest_path = dest_images_dir / rel
                    else:
                        dest_path = dest_images_dir / image_path.name
                    if include_images:
                        _safe_copy(image_path, dest_path)
                    data["image_filepath"] = str(dest_path)

                measurements_json = data.get("measurements_json")
                if measurements_json:
                    try:
                        loaded = json.loads(measurements_json)
                    except Exception:
                        loaded = None
                    if isinstance(loaded, dict):
                        for entry in loaded.get("images", []):
                            if isinstance(entry, dict) and entry.get("path"):
                                entry_path = _resolve_archive_path(entry.get("path"), src_images_dir)
                                if entry_path:
                                    if src_images_dir in entry_path.parents:
                                        rel = entry_path.relative_to(src_images_dir)
                                        dest_entry = dest_images_dir / rel
                                    else:
                                        dest_entry = dest_images_dir / entry_path.name
                                    if include_images:
                                        _safe_copy(entry_path, dest_entry)
                                    entry["path"] = str(dest_entry)
                        data["measurements_json"] = json.dumps(loaded, ensure_ascii=False)

                columns = [k for k in data.keys()]
                values = [data[k] for k in columns]
                placeholders = ", ".join(["?"] * len(columns))
                dest_cur.execute(
                    f"INSERT INTO calibrations ({', '.join(columns)}) VALUES ({placeholders})",
                    values
                )
                imported_calibrations += 1

        if include_reference_values:
            ref_path = temp_dir / "reference_values.db"
            if ref_path.exists():
                ref_src = sqlite3.connect(ref_path)
                ref_src.row_factory = sqlite3.Row
                ref_cur = ref_src.cursor()
                ref_dest = get_reference_connection()
                ref_dest.row_factory = sqlite3.Row
                ref_dest_cur = ref_dest.cursor()
                ref_cur.execute("SELECT * FROM reference_values ORDER BY id")
                for row in ref_cur.fetchall():
                    data = dict(row)
                    data.pop("id", None)
                    columns = [k for k in data.keys()]
                    values = [data[k] for k in columns]
                    placeholders = ", ".join(["?"] * len(columns))
                    ref_dest_cur.execute(
                        f"INSERT INTO reference_values ({', '.join(columns)}) VALUES ({placeholders})",
                        values,
                    )
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
            "reference_values": imported_refs,
        }
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
