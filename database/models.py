"""Data access layer for database operations"""
import sqlite3
import shutil
import re
import json
from pathlib import Path
from typing import List, Optional, Tuple
from datetime import datetime
from .schema import (
    get_app_settings,
    get_connection,
    get_database_path,
    get_reference_connection,
    get_images_dir,
    get_calibrations_dir,
    save_app_settings,
)
from utils.exif_reader import get_image_datetime
from utils.publish_targets import (
    PUBLISH_TARGET_ARTSOBS_NO,
    normalize_publish_target,
    infer_publish_target_from_coords,
)

_UNSET = object()
_CLOUD_APP_SETTING_KEYS = {
    "cloud_last_pull_at",
    "cloud_recent_import_local_ids",
    "linked_cloud_user_id",
}
_CLOUD_SQLITE_SETTING_PREFIXES = (
    "sporely_cloud_snapshot_obs_",
    "sporely_cloud_image_file_sig_",
    "sporely_cloud_local_media_sig_obs_",
)
_CLOUD_SQLITE_SETTING_KEYS = {
    "sporely_cloud_media_signature_v1",
}

# Images directory
def _images_dir() -> Path:
    return get_images_dir()


def _thumbnails_dir() -> Path:
    return get_database_path().parent / "thumbnails"


def _path_under_root(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except Exception:
        return False


def _remove_file_under_root(path_value: str | None, root: Path) -> str | None:
    if not path_value:
        return None
    try:
        path = Path(path_value).resolve()
        if not _path_under_root(path, root):
            return None
        if path.exists() and path.is_file():
            path.unlink()
            _prune_empty_dirs(path.parent, root)
    except Exception as exc:
        print(f"Warning: Could not delete file {path_value}: {exc}")
        return str(path_value)
    return None


def _prune_empty_dirs(start_dir: str | Path | None, root: Path) -> list[str]:
    if not start_dir:
        return []
    failed: list[str] = []
    try:
        current = Path(start_dir).resolve()
        resolved_root = root.resolve()
    except Exception:
        return failed
    while current != resolved_root and _path_under_root(current, resolved_root):
        try:
            current.rmdir()
        except FileNotFoundError:
            pass
        except OSError:
            break
        except Exception as exc:
            print(f"Warning: Could not remove empty folder {current}: {exc}")
            failed.append(str(current))
            break
        current = current.parent
    return failed


def _sqlite_now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _observations_has_updated_at(cursor) -> bool:
    try:
        cursor.execute("PRAGMA table_info(observations)")
        return any(str(row[1] or '') == 'updated_at' for row in cursor.fetchall())
    except Exception:
        return False


def _table_columns(cursor, table_name: str) -> set[str]:
    try:
        cursor.execute(f"PRAGMA table_info({table_name})")
        return {str(row[1] or "") for row in cursor.fetchall()}
    except sqlite3.OperationalError:
        return set()


def reset_cloud_sync_state() -> dict[str, int]:
    """Remove all local cloud links so the DB can bind to a new account."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        observations_cols = _table_columns(cursor, "observations")
        images_cols = _table_columns(cursor, "images")
        measurements_cols = _table_columns(cursor, "spore_measurements")
        settings_cols = _table_columns(cursor, "settings")

        observation_count = 0
        image_count = 0
        measurement_count = 0
        settings_count = 0

        if {"cloud_id", "sync_status"}.issubset(observations_cols):
            set_parts = ["cloud_id = NULL", "sync_status = 'dirty'"]
            if "synced_at" in observations_cols:
                set_parts.append("synced_at = NULL")
            cursor.execute(f"UPDATE observations SET {', '.join(set_parts)}")
            observation_count = cursor.rowcount if cursor.rowcount is not None else 0

        if "cloud_id" in images_cols:
            set_parts = ["cloud_id = NULL"]
            if "synced_at" in images_cols:
                set_parts.append("synced_at = NULL")
            cursor.execute(f"UPDATE images SET {', '.join(set_parts)}")
            image_count = cursor.rowcount if cursor.rowcount is not None else 0

        if "cloud_id" in measurements_cols:
            cursor.execute("UPDATE spore_measurements SET cloud_id = NULL")
            measurement_count = cursor.rowcount if cursor.rowcount is not None else 0

        if {"key", "value"}.issubset(settings_cols):
            for key in sorted(_CLOUD_SQLITE_SETTING_KEYS):
                cursor.execute("DELETE FROM settings WHERE key = ?", (key,))
                settings_count += max(0, cursor.rowcount or 0)
            for prefix in _CLOUD_SQLITE_SETTING_PREFIXES:
                cursor.execute("DELETE FROM settings WHERE key LIKE ?", (f"{prefix}%",))
                settings_count += max(0, cursor.rowcount or 0)

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    app_settings = get_app_settings()
    removed_app_settings = 0
    for key in sorted(_CLOUD_APP_SETTING_KEYS):
        if key in app_settings:
            removed_app_settings += 1
            app_settings.pop(key, None)
    if removed_app_settings:
        save_app_settings(app_settings)

    return {
        "observations": observation_count,
        "images": image_count,
        "measurements": measurement_count,
        "settings": settings_count,
        "app_settings": removed_app_settings,
    }


def _touch_observation(cursor, observation_id: int | None, *, mark_dirty: bool = True) -> None:
    try:
        obs_id = int(observation_id or 0)
    except (TypeError, ValueError):
        return
    if obs_id <= 0:
        return
    has_updated_at = _observations_has_updated_at(cursor)
    if mark_dirty:
        if has_updated_at:
            cursor.execute(
                """
                UPDATE observations
                SET updated_at = ?,
                    sync_status = CASE
                        WHEN cloud_id IS NOT NULL THEN 'dirty'
                        ELSE sync_status
                    END
                WHERE id = ?
                """,
                (_sqlite_now_text(), obs_id),
            )
        else:
            cursor.execute(
                """
                UPDATE observations
                SET sync_status = CASE
                    WHEN cloud_id IS NOT NULL THEN 'dirty'
                    ELSE sync_status
                END
                WHERE id = ?
                """,
                (obs_id,),
            )
    else:
        if has_updated_at:
            cursor.execute(
                "UPDATE observations SET updated_at = ? WHERE id = ?",
                (_sqlite_now_text(), obs_id),
            )


def _normalize_taxon_key(genus: str | None, species: str | None) -> tuple[str, str] | None:
    if not genus or not species:
        return None
    genus = genus.strip()
    species = species.strip()
    if not genus or not species:
        return None
    genus = genus[0].upper() + genus[1:]
    species = species.lower()
    return genus, species


def _normalize_reference_metadata(value) -> dict:
    if isinstance(value, dict):
        return dict(value)
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        data = json.loads(text)
    except Exception:
        return {}
    return dict(data) if isinstance(data, dict) else {}


def _normalize_json_object(value) -> dict:
    if isinstance(value, dict):
        return dict(value)
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        data = json.loads(text)
    except Exception:
        return {}
    return dict(data) if isinstance(data, dict) else {}


def _serialize_json_object(value) -> str | None:
    data = _normalize_json_object(value)
    return json.dumps(data, ensure_ascii=False, sort_keys=True) if data else None


def _normalize_timestamp_text(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    to_python = getattr(value, "toPython", None)
    if callable(to_python):
        try:
            converted = to_python()
        except Exception:
            converted = None
        if isinstance(converted, datetime):
            return converted.strftime("%Y-%m-%d %H:%M:%S")
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _normalize_lab_metadata(value) -> dict:
    return _normalize_json_object(value)


def _hydrate_image_row(row) -> dict | None:
    if not row:
        return None
    data = dict(row)
    if "lab_metadata" in data:
        data["lab_metadata"] = _normalize_lab_metadata(data.get("lab_metadata"))
    return data


def _hydrate_session_log_row(row) -> dict | None:
    if not row:
        return None
    data = dict(row)
    if "metadata_json" in data:
        data["metadata_json"] = _normalize_json_object(data.get("metadata_json"))
    return data


def _lookup_adb_taxon_id_from_db(genus: str, species: str) -> int | None:
    try:
        from utils.vernacular_utils import resolve_vernacular_db_path
    except Exception:
        return None
    db_path = resolve_vernacular_db_path()
    if not db_path or not db_path.exists():
        return None
    try:
        conn = sqlite3.connect(db_path)
    except Exception:
        return None
    try:
        cur = conn.execute("PRAGMA table_info(taxon_min)")
        columns = {(row[1] or "").lower() for row in cur.fetchall()}
        if "adbtaxonid" in columns:
            cur = conn.execute(
                """
                SELECT AdbTaxonId
                FROM taxon_min
                WHERE genus = ? COLLATE NOCASE
                  AND specific_epithet = ? COLLATE NOCASE
                  AND AdbTaxonId IS NOT NULL
                LIMIT 1
                """,
                (genus, species),
            )
        elif "taxon_id" in columns:
            cur = conn.execute(
                """
                SELECT taxon_id
                FROM taxon_min
                WHERE genus = ? COLLATE NOCASE
                  AND specific_epithet = ? COLLATE NOCASE
                LIMIT 1
                """,
                (genus, species),
            )
        else:
            return None
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        try:
            return int(row[0])
        except (TypeError, ValueError):
            return None
    finally:
        conn.close()


def _lookup_external_taxon_id_from_db(genus: str, species: str, source_system: str) -> int | None:
    try:
        from utils.vernacular_utils import resolve_vernacular_db_path
    except Exception:
        return None
    db_path = resolve_vernacular_db_path()
    if not db_path or not db_path.exists():
        return None
    scientific_name = f"{genus} {species}".strip()
    try:
        conn = sqlite3.connect(db_path)
    except Exception:
        return None
    try:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {str(row[0] or "") for row in cur.fetchall()}
        cur = conn.execute("PRAGMA table_info(taxon_min)")
        columns = {(row[1] or "").lower() for row in cur.fetchall()}

        if source_system == "artportalen":
            if "taxon_external_id_min" in tables:
                row = conn.execute(
                    """
                    SELECT e.external_id
                    FROM taxon_min t
                    JOIN taxon_external_id_min e
                      ON e.taxon_id = t.taxon_id
                     AND e.source_system = ?
                    LEFT JOIN scientific_name_min s
                      ON s.taxon_id = t.taxon_id
                    WHERE (
                            t.genus = ? COLLATE NOCASE
                        AND t.specific_epithet = ? COLLATE NOCASE
                    )
                       OR (
                            t.canonical_scientific_name = ? COLLATE NOCASE
                    )
                       OR (
                            s.scientific_name = ? COLLATE NOCASE
                    )
                    ORDER BY e.is_preferred DESC, s.is_preferred_name DESC, e.external_id
                    LIMIT 1
                    """,
                    (source_system, genus, species, scientific_name, scientific_name),
                ).fetchone()
                if row and row[0] is not None:
                    try:
                        return int(row[0])
                    except (TypeError, ValueError):
                        return None
            if "swedish_taxon_id" in columns:
                row = conn.execute(
                    """
                    SELECT swedish_taxon_id
                    FROM taxon_min
                    WHERE genus = ? COLLATE NOCASE
                      AND specific_epithet = ? COLLATE NOCASE
                      AND swedish_taxon_id IS NOT NULL
                    LIMIT 1
                    """,
                    (genus, species),
                ).fetchone()
                if row and row[0] is not None:
                    try:
                        return int(row[0])
                    except (TypeError, ValueError):
                        return None

        if source_system == "inaturalist" and "inaturalist_taxon_id" in columns:
            row = conn.execute(
                """
                SELECT inaturalist_taxon_id
                FROM taxon_min
                WHERE genus = ? COLLATE NOCASE
                  AND specific_epithet = ? COLLATE NOCASE
                  AND inaturalist_taxon_id IS NOT NULL
                LIMIT 1
                """,
                (genus, species),
            ).fetchone()
            if row and row[0] is not None:
                try:
                    return int(row[0])
                except (TypeError, ValueError):
                    return None
        return None
    finally:
        conn.close()


def _resolve_adb_taxon_id(genus: str | None, species: str | None) -> int | None:
    key = _normalize_taxon_key(genus, species)
    if not key:
        return None
    return _lookup_adb_taxon_id_from_db(*key)


def _resolve_external_taxon_id(genus: str | None, species: str | None, source_system: str) -> int | None:
    key = _normalize_taxon_key(genus, species)
    if not key:
        return None
    return _lookup_external_taxon_id_from_db(*key, source_system=source_system)


def sanitize_folder_name(name: str) -> str:
    """Sanitize a string for use as a folder name."""
    if not name:
        return "unknown"
    # Remove or replace invalid characters
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    name = name.strip('. ')
    return name if name else "unknown"


class ObservationDB:
    """Handle observation database operations"""

    @staticmethod
    def _normalize_sharing_scope(value: str | None, fallback: str = "public") -> str:
        normalized = str(value or fallback or "public").strip().lower()
        return normalized if normalized in {"private", "friends", "public"} else "public"

    @staticmethod
    def _normalize_location_precision(value: str | None, fallback: str = "exact") -> str:
        normalized = str(value or fallback or "exact").strip().lower()
        return normalized if normalized in {"exact", "fuzzed"} else "exact"

    @staticmethod
    def _build_observation_folder_path(
        genus: str | None,
        species: str | None,
        date: str | None,
    ) -> Path:
        genus_folder = sanitize_folder_name(genus) if genus else "unknown"
        species_name = sanitize_folder_name(species) if species else "sp"
        date_part = date.replace(':', '-') if date else datetime.now().strftime('%Y-%m-%d %H-%M')
        folder_name = f"{species_name} - {date_part}"
        return _images_dir() / genus_folder / folder_name

    @staticmethod
    def _ensure_unique_observation_folder_path(
        cursor,
        desired_path: Path,
        exclude_observation_id: int | None = None,
    ) -> Path:
        desired_path = Path(desired_path)
        parent = desired_path.parent
        stem = desired_path.name
        suffix = 2
        candidate = desired_path
        while True:
            query = 'SELECT id FROM observations WHERE folder_path = ?'
            params: list[object] = [str(candidate)]
            if exclude_observation_id is not None:
                query += ' AND id != ?'
                params.append(int(exclude_observation_id))
            row = cursor.execute(query, tuple(params)).fetchone()
            if not row and not candidate.exists():
                return candidate
            candidate = parent / f"{stem} ({suffix})"
            suffix += 1

    @staticmethod
    def resolve_adb_taxon_id(genus: str | None, species: str | None) -> int | None:
        return _resolve_adb_taxon_id(genus, species)

    @staticmethod
    def resolve_external_taxon_id(genus: str | None, species: str | None, source_system: str) -> int | None:
        return _resolve_external_taxon_id(genus, species, source_system)

    @staticmethod
    def _infer_image_folder(cursor, observation_id: int) -> Optional[str]:
        """Infer the observation folder from stored image paths."""
        cursor.execute('SELECT filepath FROM images WHERE observation_id = ?', (observation_id,))
        rows = cursor.fetchall()
        if not rows:
            return None
        parents = set()
        for row in rows:
            path = row[0]
            if not path:
                continue
            parents.add(str(Path(path).resolve().parent))
        if len(parents) == 1:
            return parents.pop()
        return None

    @staticmethod
    def _move_observation_folder(cursor, observation_id: int, old_folder: str, new_folder: str):
        """Move an observation folder and update image paths."""
        old_path = Path(old_folder)
        new_path = Path(new_folder)
        new_path.parent.mkdir(parents=True, exist_ok=True)

        if not new_path.exists():
            shutil.move(str(old_path), str(new_path))
            ObservationDB._update_image_paths(cursor, observation_id, str(old_path), str(new_path))
            return

        # Merge files into existing folder, updating filepaths individually.
        for item in old_path.iterdir():
            dest = new_path / item.name
            if dest.exists():
                counter = 1
                while dest.exists():
                    dest = new_path / f"{item.stem}_{counter}{item.suffix}"
                    counter += 1
            shutil.move(str(item), str(dest))
            if item.is_file():
                cursor.execute(
                    'UPDATE images SET filepath = ? WHERE filepath = ?',
                    (str(dest), str(item))
                )

        try:
            old_path.rmdir()
        except OSError:
            pass

    @staticmethod
    def create_observation(date: str, genus: str = None, species: str = None,
                          common_name: str = None, location: str = None, habitat: str = None,
                          species_guess: str = None, notes: str = None,
                          open_comment: str = None, private_comment: str = None, interesting_comment: bool = False,
                          is_draft: bool | None = None,
                          sharing_scope: str | None = None, location_public: bool | None = None,
                          location_precision: str | None = None,
                          spore_data_visibility: str | None = None,
                          uncertain: bool = False, inaturalist_id: int = None,
                          artportalen_id: int | None = None,
                          gps_latitude: float = None, gps_longitude: float = None,
                          author: str = None, source_type: str = "personal",
                          citation: str = None, data_provider: str = None,
                          artsdata_id: int | None = None,
                          publish_target: str | None = None,
                          unspontaneous: bool = False,
                          determination_method: int | None = None,
                          habitat_nin2_path: str | None = None,
                          habitat_substrate_path: str | None = None,
                          habitat_host_genus: str | None = None,
                          habitat_host_species: str | None = None,
                          habitat_host_common_name: str | None = None,
                          habitat_nin2_note: str | None = None,
                          habitat_substrate_note: str | None = None,
                          habitat_grows_on_note: str | None = None,
                          ai_state_json: str | None = None) -> int:
        """Create a new observation and return its ID"""
        conn = get_connection()
        cursor = conn.cursor()

        # Build species_guess from genus/species if not provided
        if not species_guess and (genus or species):
            parts = []
            if genus:
                parts.append(genus)
            if species:
                parts.append(species)
            species_guess = ' '.join(parts)

        # Create folder path: genus/species date-time
        folder_path = str(
            ObservationDB._ensure_unique_observation_folder_path(
                cursor,
                ObservationDB._build_observation_folder_path(genus, species, date),
            )
        )
        resolved_publish_target = normalize_publish_target(
            publish_target,
            fallback=(
                infer_publish_target_from_coords(gps_latitude, gps_longitude)
                or PUBLISH_TARGET_ARTSOBS_NO
            ),
        )
        resolved_is_draft = True if is_draft is None else bool(is_draft)
        resolved_sharing_scope = ObservationDB._normalize_sharing_scope(sharing_scope)
        resolved_location_public = (
            bool(location_public)
            if location_public is not None
            else resolved_sharing_scope != "private"
        )
        resolved_location_precision = ObservationDB._normalize_location_precision(location_precision)
        resolved_spore_visibility = str(spore_data_visibility or "public").strip().lower()
        if resolved_spore_visibility not in {"private", "friends", "public"}:
            resolved_spore_visibility = "public"

        cursor.execute('''
            INSERT INTO observations (date, genus, species, common_name, location, habitat,
                                     artsdata_id, artportalen_id, publish_target, species_guess, notes, uncertain, unspontaneous,
                                     is_draft, sharing_scope, location_public, location_precision, spore_data_visibility,
                                     determination_method,
                                     folder_path, inaturalist_id, gps_latitude, gps_longitude,
                                     author, source_type, citation, data_provider,
                                     habitat_nin2_path, habitat_substrate_path,
                                     habitat_host_genus, habitat_host_species, habitat_host_common_name,
                                     habitat_nin2_note, habitat_substrate_note, habitat_grows_on_note,
                                     open_comment, private_comment, interesting_comment, ai_state_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (date, genus, species, common_name, location, habitat, artsdata_id,
              artportalen_id, resolved_publish_target, species_guess, notes, 1 if uncertain else 0, 1 if unspontaneous else 0,
              1 if resolved_is_draft else 0, resolved_sharing_scope, 1 if resolved_location_public else 0,
              resolved_location_precision, resolved_spore_visibility,
              determination_method,
              folder_path,
              inaturalist_id, gps_latitude, gps_longitude, author, source_type,
              citation, data_provider,
              habitat_nin2_path, habitat_substrate_path,
              habitat_host_genus, habitat_host_species, habitat_host_common_name,
              habitat_nin2_note, habitat_substrate_note, habitat_grows_on_note,
              open_comment, private_comment, 1 if interesting_comment else 0, ai_state_json))

        obs_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return obs_id

    @staticmethod
    def update_observation(observation_id: int, genus: str | object = _UNSET, species: str | object = _UNSET,
                           common_name: str | object = _UNSET, location: str | object = _UNSET, habitat: str | object = _UNSET,
                           notes: str | object = _UNSET, uncertain: bool | object = _UNSET,
                           open_comment: str | object = _UNSET, private_comment: str | object = _UNSET, interesting_comment: bool | object = _UNSET,
                           is_draft: bool | object = _UNSET,
                           sharing_scope: str | object = _UNSET, location_public: bool | object = _UNSET,
                           location_precision: str | object = _UNSET,
                           spore_data_visibility: str | object = _UNSET,
                           species_guess: str | object = _UNSET, date: str | object = _UNSET,
                           gps_latitude: float | object = _UNSET, gps_longitude: float | object = _UNSET,
                           allow_nulls: bool = False,
                           artsdata_id: int | None | object = _UNSET,
                           artportalen_id: int | None | object = _UNSET,
                           publish_target: str | object = _UNSET,
                           unspontaneous: bool | object = _UNSET,
                           determination_method: int | None | object = _UNSET,
                           habitat_nin2_path: str | object = _UNSET,
                           habitat_substrate_path: str | object = _UNSET,
                           habitat_host_genus: str | object = _UNSET,
                           habitat_host_species: str | object = _UNSET,
                           habitat_host_common_name: str | object = _UNSET,
                           habitat_nin2_note: str | object = _UNSET,
                           habitat_substrate_note: str | object = _UNSET,
                           habitat_grows_on_note: str | object = _UNSET,
                           ai_state_json: str | object = _UNSET) -> Optional[str]:
        """Update an observation. Returns new folder path if genus/species changed."""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            # Get current observation
            cursor.execute('SELECT * FROM observations WHERE id = ?', (observation_id,))
            row = cursor.fetchone()
            if not row:
                return None

            current = dict(row)
            old_folder_path = current.get('folder_path')

            # Check if genus/species changed
            new_folder_path = None
            genus_changed = genus is not _UNSET and genus != current.get('genus')
            species_changed = species is not _UNSET and species != current.get('species')

            if genus_changed or species_changed:
                # Build new folder path
                new_genus = current.get('genus') if genus is _UNSET else genus
                new_species = current.get('species') if species is _UNSET else species

                new_folder_path = str(
                    ObservationDB._ensure_unique_observation_folder_path(
                        cursor,
                        ObservationDB._build_observation_folder_path(
                            new_genus,
                            new_species,
                            current['date'],
                        ),
                        exclude_observation_id=observation_id,
                    )
                )

            # Rename folder if it exists (or infer it from image paths)
            inferred_folder = None
            if not old_folder_path or not Path(old_folder_path).exists():
                inferred_folder = ObservationDB._infer_image_folder(cursor, observation_id)
            folder_to_move = old_folder_path if old_folder_path and Path(old_folder_path).exists() else inferred_folder

            if new_folder_path and folder_to_move and folder_to_move != new_folder_path:
                try:
                    ObservationDB._move_observation_folder(cursor, observation_id, folder_to_move, new_folder_path)
                except Exception as e:
                    print(f"Warning: Could not rename folder: {e}")
                    new_folder_path = old_folder_path or folder_to_move  # Keep old path on error

            # Build update query
            updates = []
            values = []

            if genus is not _UNSET and (allow_nulls or genus is not None):
                updates.append('genus = ?')
                values.append(genus)
            if species is not _UNSET and (allow_nulls or species is not None):
                updates.append('species = ?')
                values.append(species)
            if common_name is not _UNSET and (allow_nulls or common_name is not None):
                updates.append('common_name = ?')
                values.append(common_name)
            if location is not _UNSET and (allow_nulls or location is not None):
                updates.append('location = ?')
                values.append(location)
            if habitat is not _UNSET and (allow_nulls or habitat is not None):
                updates.append('habitat = ?')
                values.append(habitat)
            if date is not _UNSET and (allow_nulls or date is not None):
                date_value = date
                updates.append('date = ?')
                values.append(date_value)
            if notes is not _UNSET and (allow_nulls or notes is not None):
                updates.append('notes = ?')
                values.append(notes)
            if open_comment is not _UNSET and (allow_nulls or open_comment is not None):
                updates.append('open_comment = ?')
                values.append(open_comment)
            if private_comment is not _UNSET and (allow_nulls or private_comment is not None):
                updates.append('private_comment = ?')
                values.append(private_comment)
            if interesting_comment is not _UNSET and (allow_nulls or interesting_comment is not None):
                updates.append('interesting_comment = ?')
                values.append(1 if interesting_comment else 0)
            if artsdata_id is not _UNSET and (allow_nulls or artsdata_id is not None):
                updates.append('artsdata_id = ?')
                values.append(artsdata_id)
            if artportalen_id is not _UNSET and (allow_nulls or artportalen_id is not None):
                updates.append('artportalen_id = ?')
                values.append(artportalen_id)
            if publish_target is not _UNSET and (allow_nulls or publish_target is not None):
                updates.append('publish_target = ?')
                values.append(normalize_publish_target(str(publish_target) if publish_target is not None else None))
            if is_draft is not _UNSET and (allow_nulls or is_draft is not None):
                updates.append('is_draft = ?')
                values.append(1 if is_draft else 0)
            if sharing_scope is not _UNSET and (allow_nulls or sharing_scope is not None):
                normalized_scope = ObservationDB._normalize_sharing_scope(str(sharing_scope) if sharing_scope is not None else None)
                updates.append('sharing_scope = ?')
                values.append(normalized_scope)
            if location_public is not _UNSET and (allow_nulls or location_public is not None):
                updates.append('location_public = ?')
                values.append(1 if location_public else 0)
            if location_precision is not _UNSET and (allow_nulls or location_precision is not None):
                updates.append('location_precision = ?')
                values.append(ObservationDB._normalize_location_precision(str(location_precision) if location_precision is not None else None))
            if spore_data_visibility is not _UNSET and (allow_nulls or spore_data_visibility is not None):
                normalized_vis = str(spore_data_visibility or "public").strip().lower()
                if normalized_vis not in {"private", "friends", "public"}:
                    normalized_vis = "public"
                updates.append('spore_data_visibility = ?')
                values.append(normalized_vis)
            if uncertain is not _UNSET and (allow_nulls or uncertain is not None):
                updates.append('uncertain = ?')
                values.append(1 if uncertain else 0)
            if unspontaneous is not _UNSET and (allow_nulls or unspontaneous is not None):
                updates.append('unspontaneous = ?')
                values.append(1 if unspontaneous else 0)
            if determination_method is not _UNSET and (allow_nulls or determination_method is not None):
                updates.append('determination_method = ?')
                values.append(determination_method)
            if gps_latitude is not _UNSET and (allow_nulls or gps_latitude is not None):
                updates.append('gps_latitude = ?')
                values.append(gps_latitude)
            if gps_longitude is not _UNSET and (allow_nulls or gps_longitude is not None):
                updates.append('gps_longitude = ?')
                values.append(gps_longitude)
            if species_guess is not _UNSET and (allow_nulls or species_guess is not None):
                updates.append('species_guess = ?')
                values.append(species_guess)
            if habitat_nin2_path is not _UNSET and (allow_nulls or habitat_nin2_path is not None):
                updates.append('habitat_nin2_path = ?')
                values.append(habitat_nin2_path)
            if habitat_substrate_path is not _UNSET and (allow_nulls or habitat_substrate_path is not None):
                updates.append('habitat_substrate_path = ?')
                values.append(habitat_substrate_path)
            if habitat_host_genus is not _UNSET and (allow_nulls or habitat_host_genus is not None):
                updates.append('habitat_host_genus = ?')
                values.append(habitat_host_genus)
            if habitat_host_species is not _UNSET and (allow_nulls or habitat_host_species is not None):
                updates.append('habitat_host_species = ?')
                values.append(habitat_host_species)
            if habitat_host_common_name is not _UNSET and (allow_nulls or habitat_host_common_name is not None):
                updates.append('habitat_host_common_name = ?')
                values.append(habitat_host_common_name)
            if habitat_nin2_note is not _UNSET and (allow_nulls or habitat_nin2_note is not None):
                updates.append('habitat_nin2_note = ?')
                values.append(habitat_nin2_note)
            if habitat_substrate_note is not _UNSET and (allow_nulls or habitat_substrate_note is not None):
                updates.append('habitat_substrate_note = ?')
                values.append(habitat_substrate_note)
            if habitat_grows_on_note is not _UNSET and (allow_nulls or habitat_grows_on_note is not None):
                updates.append('habitat_grows_on_note = ?')
                values.append(habitat_grows_on_note)
            if ai_state_json is not _UNSET and (allow_nulls or ai_state_json is not None):
                updates.append('ai_state_json = ?')
                values.append(ai_state_json)
            if new_folder_path:
                updates.append('folder_path = ?')
                values.append(new_folder_path)

            # Update species_guess based on new genus/species if not explicitly provided
            if not allow_nulls and species_guess is _UNSET and (genus is not _UNSET or species is not _UNSET):
                new_genus = current.get('genus') if genus is _UNSET else genus
                new_species = current.get('species') if species is _UNSET else species
                parts = []
                if new_genus:
                    parts.append(new_genus)
                if new_species:
                    parts.append(new_species)
                updates.append('species_guess = ?')
                values.append(' '.join(parts) if parts else 'Unknown')

            if updates:
                if _observations_has_updated_at(cursor):
                    updates.append('updated_at = ?')
                    values.append(_sqlite_now_text())
                if current.get('cloud_id'):
                    updates.append("sync_status = 'dirty'")
                values.append(observation_id)
                cursor.execute(f'''
                    UPDATE observations SET {', '.join(updates)} WHERE id = ?
                ''', values)

            conn.commit()
            return new_folder_path
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _update_image_paths(cursor, observation_id: int, old_folder: str, new_folder: str):
        """Update image filepaths when folder is renamed."""
        cursor.execute('SELECT id, filepath FROM images WHERE observation_id = ?', (observation_id,))
        rows = cursor.fetchall()

        for row in rows:
            old_path = row[1]
            if old_path and old_folder in old_path:
                new_path = old_path.replace(old_folder, new_folder)
                cursor.execute('UPDATE images SET filepath = ? WHERE id = ?', (new_path, row[0]))
    
    @staticmethod
    def get_all_observations() -> List[dict]:
        """Get all observations"""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM observations ORDER BY date DESC')
        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    @staticmethod
    def get_personal_observations_for_species(
        genus: str,
        species: str,
        exclude_observation_id: int | None = None,
    ) -> List[dict]:
        """Return personal observations for a species that have spore measurements."""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        where = [
            "o.genus = ?",
            "o.species = ?",
            "o.source_type = 'personal'",
            "EXISTS (SELECT 1 FROM spore_measurements m JOIN images i ON m.image_id = i.id"
            " WHERE i.observation_id = o.id AND m.length_um IS NOT NULL AND m.width_um IS NOT NULL)",
        ]
        params: list = [genus, species]
        if exclude_observation_id:
            where.append("o.id != ?")
            params.append(exclude_observation_id)
        cursor.execute(
            f"SELECT id, date, author FROM observations o WHERE {' AND '.join(where)} ORDER BY date DESC",
            tuple(params),
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    @staticmethod
    def get_observation(observation_id: int) -> Optional[dict]:
        """Get a single observation by ID"""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM observations WHERE id = ?', (observation_id,))
        row = cursor.fetchone()
        conn.close()

        return dict(row) if row else None

    @staticmethod
    def update_spore_statistics(observation_id: int, spore_statistics: str = None):
        """Update stored spore statistics string for an observation."""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('SELECT spore_statistics FROM observations WHERE id = ?', (observation_id,))
        row = cursor.fetchone()
        current_value = None if not row else row['spore_statistics']
        if (current_value or None) == (spore_statistics or None):
            conn.close()
            return

        if _observations_has_updated_at(cursor):
            cursor.execute('''
                UPDATE observations
                SET spore_statistics = ?,
                    updated_at = ?
                WHERE id = ?
            ''', (spore_statistics, _sqlite_now_text(), observation_id))
        else:
            cursor.execute('''
                UPDATE observations
                SET spore_statistics = ?
                WHERE id = ?
            ''', (spore_statistics, observation_id))
        _touch_observation(cursor, observation_id, mark_dirty=True)

        conn.commit()
        conn.close()

    @staticmethod
    def clear_artsdata_id(observation_id: int) -> None:
        """Clear Artsobservasjoner ID without touching other observation fields."""
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE observations
            SET artsdata_id = NULL
            WHERE id = ?
            ''',
            (observation_id,),
        )
        _touch_observation(cursor, observation_id, mark_dirty=True)
        conn.commit()
        conn.close()

    @staticmethod
    def clear_artportalen_id(observation_id: int) -> None:
        """Clear Artportalen ID without touching other observation fields."""
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE observations
            SET artportalen_id = NULL
            WHERE id = ?
            ''',
            (observation_id,),
        )
        _touch_observation(cursor, observation_id, mark_dirty=True)
        conn.commit()
        conn.close()

    @staticmethod
    def set_artportalen_id(observation_id: int, artportalen_id: int | None) -> None:
        """Set Artportalen ID for an observation."""
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE observations
            SET artportalen_id = ?
            WHERE id = ?
            ''',
            (artportalen_id, observation_id),
        )
        _touch_observation(cursor, observation_id, mark_dirty=True)
        conn.commit()
        conn.close()

    @staticmethod
    def set_publish_target(observation_id: int, publish_target: str | None) -> None:
        """Set the preferred Norway/Sweden reporting target for an observation."""
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE observations
            SET publish_target = ?
            WHERE id = ?
            ''',
            (normalize_publish_target(publish_target), observation_id),
        )
        _touch_observation(cursor, observation_id, mark_dirty=True)
        conn.commit()
        conn.close()

    @staticmethod
    def set_inaturalist_id(observation_id: int, inaturalist_id: int | None) -> None:
        """Set iNaturalist ID for an observation."""
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE observations
            SET inaturalist_id = ?
            WHERE id = ?
            ''',
            (inaturalist_id, observation_id),
        )
        _touch_observation(cursor, observation_id, mark_dirty=True)
        conn.commit()
        conn.close()

    @staticmethod
    def set_mushroomobserver_id(observation_id: int, mushroomobserver_id: int | None) -> None:
        """Set Mushroom Observer ID for an observation."""
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE observations
            SET mushroomobserver_id = ?
            WHERE id = ?
            ''',
            (mushroomobserver_id, observation_id),
        )
        _touch_observation(cursor, observation_id, mark_dirty=True)
        conn.commit()
        conn.close()

    @staticmethod
    def set_auto_threshold(observation_id: int, auto_threshold: float = None):
        """Store the auto-measure threshold for an observation."""
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE observations
            SET auto_threshold = ?
            WHERE id = ?
        ''', (auto_threshold, observation_id))

        _touch_observation(cursor, observation_id, mark_dirty=True)
        conn.commit()
        conn.close()

    @staticmethod
    def delete_observation(observation_id: int, progress_cb=None) -> list[str]:
        """Delete an observation and all associated images/measurements.

        Returns a list of file or folder paths that could not be deleted.
        """
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        folder_path = None
        image_rows = []
        thumbnail_paths: list[str] = []
        failed_paths: list[str] = []
        shared_file_refs: dict[str, bool] = {}
        shared_folder_ref = False
        try:
            # Collect image filepaths and observation folder before deleting rows
            cursor.execute('SELECT folder_path FROM observations WHERE id = ?', (observation_id,))
            obs_row = cursor.fetchone()
            if obs_row and obs_row[0]:
                folder_path = obs_row[0]

            cursor.execute(
                'SELECT id, filepath, original_filepath FROM images WHERE observation_id = ?',
                (observation_id,),
            )
            image_rows = cursor.fetchall()

            cursor.execute(
                '''
                SELECT t.filepath
                FROM thumbnails t
                JOIN images i ON i.id = t.image_id
                WHERE i.observation_id = ?
                ''',
                (observation_id,),
            )
            thumbnail_paths = [
                str(row[0] or '').strip()
                for row in cursor.fetchall()
                if str(row[0] or '').strip()
            ]

            for row in image_rows:
                for candidate in (row["filepath"], row["original_filepath"]):
                    if not candidate:
                        continue
                    ref_row = cursor.execute(
                        '''
                        SELECT 1
                        FROM images
                        WHERE observation_id != ?
                          AND (filepath = ? OR original_filepath = ?)
                        LIMIT 1
                        ''',
                        (observation_id, candidate, candidate),
                    ).fetchone()
                    shared_file_refs[str(candidate)] = bool(ref_row)

            if folder_path:
                shared_folder_row = cursor.execute(
                    '''
                    SELECT 1
                    FROM observations
                    WHERE id != ?
                      AND folder_path = ?
                    LIMIT 1
                    ''',
                    (observation_id, folder_path),
                ).fetchone()
                if shared_folder_row:
                    shared_folder_ref = True
                else:
                    folder_prefix = f"{folder_path.rstrip('/')}%"
                    shared_image_row = cursor.execute(
                        '''
                        SELECT 1
                        FROM images
                        WHERE observation_id != ?
                          AND (
                                filepath LIKE ?
                             OR original_filepath LIKE ?
                          )
                        LIMIT 1
                        ''',
                        (observation_id, folder_prefix, folder_prefix),
                    ).fetchone()
                    shared_folder_ref = bool(shared_image_row)

            # Delete dependent rows first (annotations -> measurements -> thumbnails -> images -> session logs)
            cursor.execute('''
                DELETE FROM spore_annotations
                WHERE image_id IN (SELECT id FROM images WHERE observation_id = ?)
            ''', (observation_id,))
            cursor.execute('''
                DELETE FROM spore_annotations
                WHERE measurement_id IN (
                    SELECT id FROM spore_measurements
                    WHERE image_id IN (SELECT id FROM images WHERE observation_id = ?)
                )
            ''', (observation_id,))
            cursor.execute('''
                DELETE FROM spore_measurements
                WHERE image_id IN (SELECT id FROM images WHERE observation_id = ?)
            ''', (observation_id,))
            cursor.execute('''
                DELETE FROM thumbnails
                WHERE image_id IN (SELECT id FROM images WHERE observation_id = ?)
            ''', (observation_id,))

            # Delete all images for this observation
            cursor.execute('DELETE FROM images WHERE observation_id = ?', (observation_id,))

            # Remove retrospective/live session history tied to this observation.
            cursor.execute('DELETE FROM session_logs WHERE observation_id = ?', (observation_id,))

            # Delete the observation itself
            cursor.execute('DELETE FROM observations WHERE id = ?', (observation_id,))

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        # Remove thumbnails and image files from disk
        images_root = _images_dir()
        thumbnails_root = _thumbnails_dir()
        for thumb_path in sorted(set(thumbnail_paths)):
            failed = _remove_file_under_root(thumb_path, thumbnails_root)
            if failed:
                failed_paths.append(failed)

        total_images = len(image_rows)
        for index, (image_id, filepath, original_filepath) in enumerate(image_rows, start=1):
            for candidate in (filepath, original_filepath):
                if not candidate:
                    continue
                if shared_file_refs.get(str(candidate)):
                    continue
                failed = _remove_file_under_root(candidate, images_root)
                if failed:
                    failed_paths.append(failed)
            if callable(progress_cb):
                try:
                    progress_cb(index, total_images, int(image_id or 0))
                except Exception:
                    pass

        # Remove observation folder if it lives under images root
        if folder_path:
            try:
                obs_folder = Path(folder_path).resolve()
                if (
                    not shared_folder_ref
                    and obs_folder.exists()
                    and _path_under_root(obs_folder, images_root)
                ):
                    failed_paths.extend(_prune_empty_dirs(obs_folder, images_root))
            except Exception as e:
                print(f"Warning: Could not delete observation folder {folder_path}: {e}")
                failed_paths.append(str(folder_path))
        for row in image_rows:
            for candidate in (row["filepath"], row["original_filepath"]):
                if candidate:
                    failed_paths.extend(_prune_empty_dirs(Path(candidate).parent, images_root))
        return failed_paths

class ImageDB:
    """Handle image database operations"""

    # Microscope image categories
    MICRO_CATEGORIES = [
        'spores',
        'basidia',
        'pleurocystidia',
        'cheilocystidia',
        'caulocystidia',
        'pileipellis',
        'stipitipellis',
        'clamp_connections',
        'other'
    ]

    @staticmethod
    def add_image(observation_id: int, filepath: str, image_type: str,
                  scale: float = None, notes: str = None,
                  micro_category: str = None, objective_name: str = None,
                  measure_color: str = None, mount_medium: str = None,
                  stain: str = None,
                  sample_type: str = None, contrast: str = None,
                  sort_order: int | None = None,
                  captured_at: object = None,
                  calibration_id: int = None,
                  ai_crop_box: tuple[float, float, float, float] | None = None,
                  ai_crop_source_size: tuple[int, int] | None = None,
                  crop_mode: str | None = None,
                  gps_source: bool | None = None,
                  resample_scale_factor: float | None = None,
                  original_filepath: str | None = None,
                  lab_metadata: dict | str | None = None,
                  copy_to_folder: bool = True,
                  mark_observation_dirty: bool = True) -> int:
        """Add an image and return its ID.

        Args:
            observation_id: ID of the observation
            filepath: Source filepath of the image
            image_type: 'field' or 'microscope'
            scale: Scale in microns per pixel
            notes: Optional notes
            micro_category: Category for microscope images
            objective_name: Name of the objective used
            copy_to_folder: If True, copy image to observation folder
        """
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        final_filepath = filepath
        final_original_filepath = original_filepath
        artsobs_web_unpublished = 0

        if observation_id:
            cursor.execute('SELECT artsdata_id, publish_target FROM observations WHERE id = ?', (observation_id,))
            obs_row = cursor.fetchone()
            try:
                target = normalize_publish_target(obs_row["publish_target"] if obs_row else None)
                if obs_row and target == PUBLISH_TARGET_ARTSOBS_NO and int(obs_row["artsdata_id"] or 0) > 0:
                    artsobs_web_unpublished = 1
            except (TypeError, ValueError):
                artsobs_web_unpublished = 0
            if sort_order is None:
                cursor.execute(
                    'SELECT COALESCE(MAX(sort_order), -1) + 1 FROM images WHERE observation_id = ?',
                    (observation_id,),
                )
                row = cursor.fetchone()
                try:
                    sort_order = int(row[0] if row else 0)
                except (TypeError, ValueError):
                    sort_order = 0
        elif sort_order is None:
            sort_order = 0

        # Copy image to observation folder if requested
        if copy_to_folder and observation_id:
            cursor.execute('SELECT folder_path FROM observations WHERE id = ?', (observation_id,))
            row = cursor.fetchone()
            if row and row['folder_path']:
                folder_path = Path(row['folder_path'])
                folder_path.mkdir(parents=True, exist_ok=True)

                source_path = Path(filepath)
                if source_path.exists():
                    # Generate unique filename if needed
                    dest_path = folder_path / source_path.name
                    counter = 1
                    while dest_path.exists():
                        dest_path = folder_path / f"{source_path.stem}_{counter}{source_path.suffix}"
                        counter += 1

                    try:
                        shutil.copy2(filepath, dest_path)
                        final_filepath = str(dest_path)
                    except Exception as e:
                        print(f"Warning: Could not copy image: {e}")

        storage_mode = SettingsDB.get_setting("original_storage_mode", "observation")
        if not storage_mode:
            storage_mode = "observation"
        if storage_mode == "none":
            original_filepath = None

        if original_filepath:
            original_path = Path(original_filepath)
            if original_path.exists():
                target_dir = None
                if storage_mode == "global":
                    global_dir = SettingsDB.get_setting("originals_dir") or str(get_database_path().parent / "originals")
                    target_dir = Path(global_dir)
                    if observation_id:
                        cursor.execute('SELECT folder_path FROM observations WHERE id = ?', (observation_id,))
                        row = cursor.fetchone()
                        if row and row['folder_path']:
                            obs_folder = Path(row['folder_path'])
                            try:
                                rel = obs_folder.resolve().relative_to(_images_dir().resolve())
                                target_dir = target_dir / rel
                            except Exception:
                                target_dir = target_dir / obs_folder.name
                        else:
                            target_dir = target_dir / f"observation_{observation_id}"
                else:
                    if copy_to_folder and observation_id:
                        cursor.execute('SELECT folder_path FROM observations WHERE id = ?', (observation_id,))
                        row = cursor.fetchone()
                        if row and row['folder_path']:
                            target_dir = Path(row['folder_path']) / "originals"
                if target_dir:
                    target_dir.mkdir(parents=True, exist_ok=True)
                    dest_original = target_dir / original_path.name
                    counter = 1
                    while dest_original.exists():
                        dest_original = target_dir / f"{original_path.stem}_{counter}{original_path.suffix}"
                        counter += 1
                    try:
                        shutil.copy2(original_filepath, dest_original)
                        final_original_filepath = str(dest_original)
                    except Exception as e:
                        print(f"Warning: Could not copy original image: {e}")

        captured_at_text = _normalize_timestamp_text(captured_at)
        if not captured_at_text:
            for candidate_path in (original_filepath, filepath, final_original_filepath, final_filepath):
                candidate = str(candidate_path or "").strip()
                if not candidate:
                    continue
                try:
                    detected = get_image_datetime(candidate)
                except Exception:
                    detected = None
                captured_at_text = _normalize_timestamp_text(detected)
                if captured_at_text:
                    break

        crop_x1 = crop_y1 = crop_x2 = crop_y2 = None
        if ai_crop_box and len(ai_crop_box) == 4:
            crop_x1, crop_y1, crop_x2, crop_y2 = ai_crop_box
        crop_w = crop_h = None
        if ai_crop_source_size and len(ai_crop_source_size) == 2:
            crop_w, crop_h = ai_crop_source_size
        gps_source_value = None if gps_source is None else (1 if gps_source else 0)
        lab_metadata_json = _serialize_json_object(lab_metadata)

        cursor.execute('''
            INSERT INTO images (observation_id, filepath, image_type, micro_category,
                              objective_name, scale_microns_per_pixel, resample_scale_factor,
                              mount_medium, stain, sample_type, contrast, measure_color, notes, lab_metadata, calibration_id,
                              captured_at,
                              ai_crop_x1, ai_crop_y1, ai_crop_x2, ai_crop_y2,
                              ai_crop_source_w, ai_crop_source_h, crop_mode, gps_source, original_filepath, sort_order,
                              artsobs_web_unpublished)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (observation_id, final_filepath, image_type, micro_category,
              objective_name, scale, resample_scale_factor, mount_medium, stain, sample_type, contrast, measure_color, notes,
              lab_metadata_json, calibration_id, captured_at_text, crop_x1, crop_y1, crop_x2, crop_y2, crop_w, crop_h, crop_mode, gps_source_value,
              final_original_filepath, sort_order, artsobs_web_unpublished))

        img_id = cursor.lastrowid
        if mark_observation_dirty and observation_id:
            _touch_observation(cursor, observation_id, mark_dirty=True)
        conn.commit()
        conn.close()
        return img_id

    @staticmethod
    def get_image(image_id: int) -> Optional[dict]:
        """Get a single image by ID"""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM images WHERE id = ?', (image_id,))
        row = cursor.fetchone()
        conn.close()

        return _hydrate_image_row(row)

    @staticmethod
    def get_images_for_observation(observation_id: int) -> List[dict]:
        """Get all images for an observation"""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM images
            WHERE observation_id = ?
            ORDER BY
                CASE WHEN sort_order IS NULL THEN 1 ELSE 0 END,
                sort_order,
                image_type,
                micro_category,
                created_at,
                id
        ''', (observation_id,))

        rows = cursor.fetchall()
        conn.close()
        return [_hydrate_image_row(row) for row in rows]

    @staticmethod
    def get_images_by_type(observation_id: int, image_type: str) -> List[dict]:
        """Get images of a specific type for an observation"""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM images
            WHERE observation_id = ? AND image_type = ?
            ORDER BY
                CASE WHEN sort_order IS NULL THEN 1 ELSE 0 END,
                sort_order,
                micro_category,
                created_at,
                id
        ''', (observation_id, image_type))

        rows = cursor.fetchall()
        conn.close()
        return [_hydrate_image_row(row) for row in rows]

    @staticmethod
    def get_pending_artsobs_web_uploads() -> List[dict]:
        """Get images marked as pending upload for Artsobservasjoner web."""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT
                i.id AS image_id,
                i.observation_id,
                i.filepath,
                i.original_filepath,
                i.created_at,
                o.artsdata_id
            FROM images i
            JOIN observations o ON o.id = i.observation_id
            WHERE COALESCE(i.artsobs_web_unpublished, 0) = 1
              AND COALESCE(o.artsdata_id, 0) > 0
            ORDER BY
                CASE WHEN i.sort_order IS NULL THEN 1 ELSE 0 END,
                i.sort_order,
                i.created_at,
                i.id
        ''')
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    @staticmethod
    def get_pending_artsobs_web_upload_count_for_observation(observation_id: int) -> int:
        """Return number of pending Artsobservasjoner web image uploads for one observation."""
        try:
            obs_id = int(observation_id)
        except (TypeError, ValueError):
            return 0
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT COUNT(*)
            FROM images i
            JOIN observations o ON o.id = i.observation_id
            WHERE i.observation_id = ?
              AND COALESCE(i.artsobs_web_unpublished, 0) = 1
              AND COALESCE(o.artsdata_id, 0) > 0
            ''',
            (obs_id,),
        )
        row = cursor.fetchone()
        conn.close()
        try:
            return int(row[0] if row else 0)
        except Exception:
            return 0

    @staticmethod
    def mark_images_artsobs_web_uploaded(image_ids: List[int]) -> None:
        if not image_ids:
            return
        clean_ids: list[int] = []
        for value in image_ids:
            try:
                clean_ids.append(int(value))
            except (TypeError, ValueError):
                continue
        if not clean_ids:
            return
        placeholders = ",".join("?" for _ in clean_ids)
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE images SET artsobs_web_unpublished = 0 WHERE id IN ({placeholders})",
            clean_ids,
        )
        conn.commit()
        conn.close()

    @staticmethod
    def mark_observation_images_artsobs_web_uploaded(observation_id: int) -> None:
        try:
            obs_id = int(observation_id)
        except (TypeError, ValueError):
            return
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE images SET artsobs_web_unpublished = 0 WHERE observation_id = ?",
            (obs_id,),
        )
        conn.commit()
        conn.close()

    @staticmethod
    def update_image(image_id: int, micro_category: str = None,
                     scale: float = None, notes: str | None | object = _UNSET,
                     objective_name: str = None, filepath: str = None,
                     measure_color: str = None, image_type: str = None,
                     sort_order: int | None = None,
                     mount_medium: str = None, stain: str = None, sample_type: str = None,
                     contrast: str = None, calibration_id: int | None | object = _UNSET,
                     ai_crop_box: tuple[float, float, float, float] | None | object = _UNSET,
                     ai_crop_source_size: tuple[int, int] | None | object = _UNSET,
                     crop_mode: str | None | object = _UNSET,
                     gps_source: bool | None | object = _UNSET,
                     resample_scale_factor: float | None | object = _UNSET,
                     original_filepath: str | None | object = _UNSET,
                     lab_metadata: dict | str | None | object = _UNSET,
                     scale_bar_selection: object = _UNSET):
        """Update image metadata"""
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT observation_id FROM images WHERE id = ?', (image_id,))
        image_row = cursor.fetchone()
        try:
            observation_id = int(image_row[0]) if image_row and image_row[0] is not None else None
        except Exception:
            observation_id = None
        cursor.execute("PRAGMA table_info(images)")
        image_columns = {row[1] for row in cursor.fetchall()}

        updates = []
        values = []

        if micro_category is not None:
            updates.append('micro_category = ?')
            values.append(micro_category)
        if scale is not None:
            updates.append('scale_microns_per_pixel = ?')
            values.append(scale)
        if objective_name is not None:
            updates.append('objective_name = ?')
            values.append(objective_name)
        if image_type is not None:
            updates.append('image_type = ?')
            values.append(image_type)
        if sort_order is not None and 'sort_order' in image_columns:
            updates.append('sort_order = ?')
            values.append(int(sort_order))
        if mount_medium is not None:
            updates.append('mount_medium = ?')
            values.append(mount_medium)
        if stain is not None:
            updates.append('stain = ?')
            values.append(stain)
        if sample_type is not None:
            updates.append('sample_type = ?')
            values.append(sample_type)
        if contrast is not None:
            updates.append('contrast = ?')
            values.append(contrast)
        if filepath is not None:
            updates.append('filepath = ?')
            values.append(filepath)
        if resample_scale_factor is not _UNSET:
            updates.append('resample_scale_factor = ?')
            values.append(resample_scale_factor)
        if original_filepath is not _UNSET:
            updates.append('original_filepath = ?')
            values.append(original_filepath)
        if notes is not _UNSET:
            updates.append('notes = ?')
            values.append(notes)
        if lab_metadata is not _UNSET and 'lab_metadata' in image_columns:
            updates.append('lab_metadata = ?')
            values.append(_serialize_json_object(lab_metadata))
        if measure_color is not None:
            updates.append('measure_color = ?')
            values.append(measure_color)
        if calibration_id is not _UNSET:
            updates.append('calibration_id = ?')
            values.append(calibration_id)
        if ai_crop_box is not _UNSET:
            crop_x1 = crop_y1 = crop_x2 = crop_y2 = None
            if ai_crop_box and len(ai_crop_box) == 4:
                crop_x1, crop_y1, crop_x2, crop_y2 = ai_crop_box
            updates.extend([
                'ai_crop_x1 = ?',
                'ai_crop_y1 = ?',
                'ai_crop_x2 = ?',
                'ai_crop_y2 = ?',
            ])
            values.extend([crop_x1, crop_y1, crop_x2, crop_y2])
        if ai_crop_source_size is not _UNSET:
            crop_w = crop_h = None
            if ai_crop_source_size and len(ai_crop_source_size) == 2:
                crop_w, crop_h = ai_crop_source_size
            updates.extend(['ai_crop_source_w = ?', 'ai_crop_source_h = ?'])
            values.extend([crop_w, crop_h])
        if crop_mode is not _UNSET and 'crop_mode' in image_columns:
            updates.append('crop_mode = ?')
            values.append(crop_mode)
        if gps_source is not _UNSET:
            gps_value = None if gps_source is None else (1 if gps_source else 0)
            updates.append('gps_source = ?')
            values.append(gps_value)
        if scale_bar_selection is not _UNSET:
            x1 = y1 = x2 = y2 = None
            if scale_bar_selection and len(scale_bar_selection) == 2:
                (x1, y1), (x2, y2) = scale_bar_selection
            if {"scale_bar_x1", "scale_bar_y1", "scale_bar_x2", "scale_bar_y2"}.issubset(image_columns):
                updates.extend(['scale_bar_x1 = ?', 'scale_bar_y1 = ?', 'scale_bar_x2 = ?', 'scale_bar_y2 = ?'])
                values.extend([x1, y1, x2, y2])

        if updates:
            values.append(image_id)
            cursor.execute(f'''
                UPDATE images SET {', '.join(updates)} WHERE id = ?
            ''', values)
            if observation_id:
                _touch_observation(cursor, observation_id, mark_dirty=True)

        conn.commit()
        conn.close()

    @staticmethod
    def set_image_captured_at(image_id: int, captured_at: object) -> None:
        captured_at_text = _normalize_timestamp_text(captured_at)
        if not captured_at_text:
            return
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(images)")
        image_columns = {row[1] for row in cursor.fetchall()}
        if "captured_at" not in image_columns:
            conn.close()
            return
        cursor.execute(
            '''
            UPDATE images
            SET captured_at = COALESCE(captured_at, ?)
            WHERE id = ?
            ''',
            (captured_at_text, int(image_id)),
        )
        conn.commit()
        conn.close()

    @staticmethod
    def reorder_images(observation_id: int, ordered_image_ids: List[int]) -> None:
        """Persist a new image order for one observation."""
        try:
            obs_id = int(observation_id)
        except (TypeError, ValueError):
            return

        normalized_ids: list[int] = []
        seen: set[int] = set()
        for value in ordered_image_ids or []:
            try:
                image_id = int(value)
            except (TypeError, ValueError):
                continue
            if image_id in seen:
                continue
            seen.add(image_id)
            normalized_ids.append(image_id)

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT id FROM images
            WHERE observation_id = ?
            ORDER BY
                CASE WHEN sort_order IS NULL THEN 1 ELSE 0 END,
                sort_order,
                image_type,
                micro_category,
                created_at,
                id
            ''',
            (obs_id,),
        )
        existing_ids = [int(row[0]) for row in cursor.fetchall()]
        if not existing_ids:
            conn.close()
            return

        existing_set = set(existing_ids)
        ordered = [image_id for image_id in normalized_ids if image_id in existing_set]
        ordered.extend(image_id for image_id in existing_ids if image_id not in ordered)
        for index, image_id in enumerate(ordered):
            cursor.execute(
                'UPDATE images SET sort_order = ? WHERE id = ? AND observation_id = ?',
                (index, image_id, obs_id),
            )
        _touch_observation(cursor, obs_id, mark_dirty=True)
        conn.commit()
        conn.close()

    @staticmethod
    def delete_image(image_id: int):
        """Delete an image, its measurements, thumbnails, and unshared local files."""
        failed_paths: list[str] = []
        filepath = None
        original_filepath = None
        thumbnail_paths: list[str] = []
        shared_file_refs: dict[str, bool] = {}
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT observation_id, filepath, original_filepath FROM images WHERE id = ?',
                (image_id,),
            )
            image_row = cursor.fetchone()
            try:
                observation_id = int(image_row[0]) if image_row and image_row[0] is not None else None
            except Exception:
                observation_id = None
            if image_row:
                filepath = image_row[1]
                original_filepath = image_row[2]

            cursor.execute('SELECT filepath FROM thumbnails WHERE image_id = ?', (image_id,))
            thumbnail_paths = [
                str(row[0] or '').strip()
                for row in cursor.fetchall()
                if str(row[0] or '').strip()
            ]

            for candidate in (filepath, original_filepath):
                if not candidate:
                    continue
                ref_row = cursor.execute(
                    '''
                    SELECT 1
                    FROM images
                    WHERE id != ?
                      AND (filepath = ? OR original_filepath = ?)
                    LIMIT 1
                    ''',
                    (image_id, candidate, candidate),
                ).fetchone()
                shared_file_refs[str(candidate)] = bool(ref_row)

            # Delete dependent rows first to satisfy foreign keys.
            cursor.execute(
                '''
                DELETE FROM spore_annotations
                WHERE image_id = ?
                ''',
                (image_id,),
            )
            cursor.execute(
                '''
                DELETE FROM spore_annotations
                WHERE measurement_id IN (
                    SELECT id FROM spore_measurements WHERE image_id = ?
                )
                ''',
                (image_id,),
            )
            cursor.execute(
                '''
                DELETE FROM spore_measurements
                WHERE image_id = ?
                ''',
                (image_id,),
            )
            cursor.execute(
                '''
                DELETE FROM thumbnails
                WHERE image_id = ?
                ''',
                (image_id,),
            )
            cursor.execute(
                '''
                DELETE FROM images
                WHERE id = ?
                ''',
                (image_id,),
            )
            if observation_id:
                _touch_observation(cursor, observation_id, mark_dirty=True)

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        thumbnails_root = _thumbnails_dir()
        for thumb_path in sorted(set(thumbnail_paths)):
            failed = _remove_file_under_root(thumb_path, thumbnails_root)
            if failed:
                failed_paths.append(failed)

        images_root = _images_dir()
        for candidate in (filepath, original_filepath):
            if not candidate or shared_file_refs.get(str(candidate)):
                continue
            failed = _remove_file_under_root(candidate, images_root)
            if failed:
                failed_paths.append(failed)
        for candidate in (filepath, original_filepath):
            if candidate:
                failed_paths.extend(_prune_empty_dirs(Path(candidate).parent, images_root))
        return failed_paths


class SessionLogDB:
    """Persist timestamped Live Lab / retrospective-session events."""

    @staticmethod
    def add_event(
        observation_id: int,
        session_id: str,
        event_type: str,
        *,
        session_kind: str = "live",
        attribute_name: str | None = None,
        value: str | None = None,
        metadata_json: dict | str | None = None,
        recorded_at: str | None = None,
    ) -> int:
        try:
            obs_id = int(observation_id)
        except (TypeError, ValueError):
            return 0
        session_key = str(session_id or "").strip()
        event_name = str(event_type or "").strip()
        if obs_id <= 0 or not session_key or not event_name:
            return 0

        kind = str(session_kind or "live").strip().lower()
        if kind not in {"live", "offline"}:
            kind = "live"

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO session_logs (
                observation_id,
                session_id,
                session_kind,
                event_type,
                attribute_name,
                value,
                metadata_json,
                recorded_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                obs_id,
                session_key,
                kind,
                event_name,
                str(attribute_name or "").strip() or None,
                str(value or "").strip() or None,
                _serialize_json_object(metadata_json),
                str(recorded_at or "").strip() or _sqlite_now_text(),
            ),
        )
        row_id = int(cursor.lastrowid or 0)
        conn.commit()
        conn.close()
        return row_id

    @staticmethod
    def get_events(
        *,
        observation_id: int | None = None,
        session_id: str | None = None,
        session_kind: str | None = None,
        limit: int | None = None,
    ) -> List[dict]:
        clauses: list[str] = []
        values: list[object] = []
        if observation_id is not None:
            try:
                clauses.append("observation_id = ?")
                values.append(int(observation_id))
            except (TypeError, ValueError):
                return []
        session_key = str(session_id or "").strip()
        if session_key:
            clauses.append("session_id = ?")
            values.append(session_key)
        kind = str(session_kind or "").strip().lower()
        if kind:
            clauses.append("session_kind = ?")
            values.append(kind)

        sql = "SELECT * FROM session_logs"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY recorded_at ASC, id ASC"
        if limit is not None:
            try:
                sql += f" LIMIT {max(1, int(limit))}"
            except (TypeError, ValueError):
                pass

        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(sql, values)
        rows = cursor.fetchall()
        conn.close()
        return [_hydrate_session_log_row(row) for row in rows]

    @staticmethod
    def get_latest_state(session_id: str) -> dict:
        state: dict[str, str] = {}
        for row in SessionLogDB.get_events(session_id=session_id):
            if str(row.get("event_type") or "").strip().lower() != "dropdown_change":
                continue
            attribute_name = str(row.get("attribute_name") or "").strip()
            if not attribute_name:
                continue
            state[attribute_name] = str(row.get("value") or "").strip()
        return state

    @staticmethod
    def delete_sessions_for_observation(
        observation_id: int,
        *,
        session_kind: str | None = None,
    ) -> int:
        try:
            obs_id = int(observation_id or 0)
        except (TypeError, ValueError):
            return 0
        if obs_id <= 0:
            return 0

        clauses = ["observation_id = ?"]
        values: list[object] = [obs_id]
        kind = str(session_kind or "").strip().lower()
        if kind:
            clauses.append("session_kind = ?")
            values.append(kind)

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"DELETE FROM session_logs WHERE {' AND '.join(clauses)}",
            values,
        )
        deleted = int(cursor.rowcount or 0)
        if deleted:
            _touch_observation(cursor, obs_id, mark_dirty=True)
        conn.commit()
        conn.close()
        return deleted


class MeasurementDB:
    """Handle spore measurement database operations"""
    
    @staticmethod
    def add_measurement(image_id: int, length: float, width: float = None,
                       measurement_type: str = 'manual', notes: str = None,
                       points: list = None) -> int:
        """Add a measurement and return its ID

        Args:
            image_id: ID of the image
            length: Length in microns
            width: Width in microns
            measurement_type: Type of measurement
            notes: Optional notes
            points: List of 4 QPointF objects [p1, p2, p3, p4]
        """
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT observation_id FROM images WHERE id = ?', (image_id,))
        row = cursor.fetchone()
        try:
            observation_id = int(row[0]) if row and row[0] is not None else None
        except Exception:
            observation_id = None

        if points and len(points) == 4:
            cursor.execute('''
                INSERT INTO spore_measurements
                (image_id, length_um, width_um, measurement_type, notes,
                 p1_x, p1_y, p2_x, p2_y, p3_x, p3_y, p4_x, p4_y)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (image_id, length, width, measurement_type, notes,
                  points[0].x(), points[0].y(),
                  points[1].x(), points[1].y(),
                  points[2].x(), points[2].y(),
                  points[3].x(), points[3].y()))
        elif points and len(points) == 2:
            cursor.execute('''
                INSERT INTO spore_measurements
                (image_id, length_um, width_um, measurement_type, notes,
                 p1_x, p1_y, p2_x, p2_y)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (image_id, length, width, measurement_type, notes,
                  points[0].x(), points[0].y(),
                  points[1].x(), points[1].y()))
        else:
            cursor.execute('''
                INSERT INTO spore_measurements (image_id, length_um, width_um, measurement_type, notes)
                VALUES (?, ?, ?, ?, ?)
            ''', (image_id, length, width, measurement_type, notes))

        meas_id = cursor.lastrowid
        if observation_id:
            _touch_observation(cursor, observation_id, mark_dirty=True)
        conn.commit()
        conn.close()
        return meas_id
    
    @staticmethod
    def get_measurements_for_image(image_id: int) -> List[dict]:
        """Get all measurements for an image"""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM spore_measurements 
            WHERE image_id = ?
            ORDER BY measured_at
        ''', (image_id,))
        
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    @staticmethod
    def get_measurements_for_observation(observation_id: int) -> List[dict]:
        """Get all measurements for all images in an observation"""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
            SELECT m.*, i.filepath AS image_filepath
            FROM spore_measurements m
            JOIN images i ON m.image_id = i.id
            WHERE i.observation_id = ?
            ORDER BY m.measured_at
        ''', (observation_id,))

        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    @staticmethod
    def get_measurements_for_species(
        genus: str,
        species: str,
        source_type: str | None = None,
        measurement_category: str | None = None,
        exclude_observation_id: int | None = None,
    ) -> List[dict]:
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        where = [
            "o.genus = ?",
            "o.species = ?",
            "m.length_um IS NOT NULL",
            "m.width_um IS NOT NULL",
        ]
        params = [genus, species]
        if source_type:
            where.append("o.source_type = ?")
            params.append(source_type)
        if measurement_category:
            category = str(measurement_category).lower()
            if category in ("spore", "spores"):
                where.append(
                    "(m.measurement_type IS NULL OR m.measurement_type = '' "
                    "OR LOWER(m.measurement_type) IN ('manual', 'spore', 'spores'))"
                )
            else:
                where.append("LOWER(m.measurement_type) = ?")
                params.append(category)
        if exclude_observation_id:
            where.append("o.id != ?")
            params.append(exclude_observation_id)

        cursor.execute(
            f'''
            SELECT m.length_um, m.width_um, o.id as observation_id
            FROM spore_measurements m
            JOIN images i ON m.image_id = i.id
            JOIN observations o ON i.observation_id = o.id
            WHERE {' AND '.join(where)}
            ''',
            tuple(params),
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    @staticmethod
    def get_statistics_for_observation(observation_id: int, measurement_category: str = 'spores') -> dict:
        """Calculate statistics for measurements of an observation."""
        measurements = MeasurementDB.get_measurements_for_observation(observation_id)

        if measurement_category:
            category = measurement_category.lower()
            if category in ('spore', 'spores'):
                measurements = [
                    m for m in measurements
                    if (m.get('measurement_type') in (None, '', 'manual', 'spore', 'spores'))
                ]
            else:
                measurements = [
                    m for m in measurements
                    if (m.get('measurement_type') or '').lower() == category
                ]

        if not measurements:
            return {}

        lengths = [m['length_um'] for m in measurements]
        widths = [m['width_um'] for m in measurements if m['width_um']]

        import numpy as np

        stats = {
            'count': len(lengths),
            'length_mean': np.mean(lengths),
            'length_std': np.std(lengths),
            'length_min': np.min(lengths),
            'length_max': np.max(lengths),
            'length_p5': np.percentile(lengths, 5),
            'length_p95': np.percentile(lengths, 95),
        }

        if widths:
            ratios = [l/w for l, w in zip(lengths, widths) if w > 0]
            stats.update({
                'width_mean': np.mean(widths),
                'width_std': np.std(widths),
                'width_min': np.min(widths),
                'width_max': np.max(widths),
                'width_p5': np.percentile(widths, 5),
                'width_p95': np.percentile(widths, 95),
                'ratio_mean': np.mean(ratios),
                'ratio_min': np.min(ratios),
                'ratio_max': np.max(ratios),
                'ratio_p5': np.percentile(ratios, 5),
                'ratio_p95': np.percentile(ratios, 95),
            })

        return stats

    @staticmethod
    def get_measurement_types_for_observation(observation_id: int) -> List[str]:
        """Get distinct measurement types for an observation"""
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT DISTINCT m.measurement_type
            FROM spore_measurements m
            JOIN images i ON m.image_id = i.id
            WHERE i.observation_id = ?
            ORDER BY m.measurement_type
        ''', (observation_id,))

        rows = cursor.fetchall()
        conn.close()
        return [row[0] for row in rows]
    
    @staticmethod
    def get_statistics_for_image(image_id: int, measurement_category: str = 'spores') -> dict:
        """Calculate statistics for measurements of an image"""
        measurements = MeasurementDB.get_measurements_for_image(image_id)

        if measurement_category:
            category = measurement_category.lower()
            if category in ('spore', 'spores'):
                measurements = [
                    m for m in measurements
                    if (m.get('measurement_type') in (None, '', 'manual', 'spore', 'spores'))
                ]
            else:
                measurements = [
                    m for m in measurements
                    if (m.get('measurement_type') or '').lower() == category
                ]

        if not measurements:
            return {}

        lengths = [m['length_um'] for m in measurements]
        widths = [m['width_um'] for m in measurements if m['width_um']]

        import numpy as np

        stats = {
            'count': len(lengths),
            'length_mean': np.mean(lengths),
            'length_std': np.std(lengths),
            'length_min': np.min(lengths),
            'length_max': np.max(lengths),
            'length_p5': np.percentile(lengths, 5),
            'length_p95': np.percentile(lengths, 95),
        }

        if widths:
            ratios = [l/w for l, w in zip(lengths, widths) if w > 0]
            stats.update({
                'width_mean': np.mean(widths),
                'width_std': np.std(widths),
                'width_min': np.min(widths),
                'width_max': np.max(widths),
                'width_p5': np.percentile(widths, 5),
                'width_p95': np.percentile(widths, 95),
                'ratio_mean': np.mean(ratios),
                'ratio_min': np.min(ratios),
                'ratio_max': np.max(ratios),
                'ratio_p5': np.percentile(ratios, 5),
                'ratio_p95': np.percentile(ratios, 95),
            })

        return stats

    @staticmethod
    def delete_measurement(measurement_id: int):
        """Delete a measurement by ID"""
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("BEGIN")
            cursor.execute(
                '''
                SELECT i.observation_id
                FROM spore_measurements m
                JOIN images i ON i.id = m.image_id
                WHERE m.id = ?
                LIMIT 1
                ''',
                (measurement_id,),
            )
            row = cursor.fetchone()
            try:
                observation_id = int(row[0]) if row and row[0] is not None else None
            except Exception:
                observation_id = None
            cursor.execute(
                'DELETE FROM spore_annotations WHERE measurement_id = ?',
                (measurement_id,)
            )
            cursor.execute(
                'DELETE FROM spore_measurements WHERE id = ?',
                (measurement_id,)
            )
            if observation_id:
                _touch_observation(cursor, observation_id, mark_dirty=True)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


class ReferenceDB:
    """Handle reference spore size values."""

    @staticmethod
    def get_reference(
        genus: str,
        species: str,
        source: str = None,
        mount_medium: str = None,
        stain: str = None,
    ) -> Optional[dict]:
        conn = get_reference_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        if source and mount_medium is not None and stain is not None:
            cursor.execute('''
                SELECT * FROM reference_values
                WHERE genus = ? AND species = ? AND source = ? AND mount_medium = ? AND stain = ?
                ORDER BY updated_at DESC
                LIMIT 1
            ''', (genus, species, source, mount_medium, stain))
        elif source and mount_medium is not None:
            cursor.execute('''
                SELECT * FROM reference_values
                WHERE genus = ? AND species = ? AND source = ? AND mount_medium = ?
                ORDER BY updated_at DESC
                LIMIT 1
            ''', (genus, species, source, mount_medium))
        elif source and stain is not None:
            cursor.execute('''
                SELECT * FROM reference_values
                WHERE genus = ? AND species = ? AND source = ? AND stain = ?
                ORDER BY updated_at DESC
                LIMIT 1
            ''', (genus, species, source, stain))
        elif source:
            cursor.execute('''
                SELECT * FROM reference_values
                WHERE genus = ? AND species = ? AND source = ?
                ORDER BY updated_at DESC
                LIMIT 1
            ''', (genus, species, source))
        else:
            cursor.execute('''
                SELECT * FROM reference_values
                WHERE genus = ? AND species = ?
                ORDER BY updated_at DESC
                LIMIT 1
            ''', (genus, species))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        data = dict(row)
        if "metadata_json" in data:
            data["metadata_json"] = _normalize_reference_metadata(data.get("metadata_json"))
        return data

    @staticmethod
    def set_reference(values: dict):
        conn = get_reference_connection()
        cursor = conn.cursor()
        metadata = _normalize_reference_metadata(values.get("metadata_json"))
        if values.get("source_type") and "source_type" not in metadata:
            metadata["source_type"] = values.get("source_type")
        if metadata and "imported_at" not in metadata:
            metadata["imported_at"] = datetime.now().isoformat(timespec="seconds")
        metadata_json = json.dumps(metadata, ensure_ascii=False, sort_keys=True) if metadata else None

        cursor.execute('''
            DELETE FROM reference_values
            WHERE genus = ? AND species = ?
              AND (source = ? OR (? IS NULL AND source IS NULL))
              AND (mount_medium = ? OR (? IS NULL AND mount_medium IS NULL))
              AND (stain = ? OR (? IS NULL AND stain IS NULL))
        ''', (
            values.get("genus"),
            values.get("species"),
            values.get("source"),
            values.get("source"),
            values.get("mount_medium"),
            values.get("mount_medium"),
            values.get("stain"),
            values.get("stain"),
        ))

        cursor.execute('''
            INSERT INTO reference_values (
                genus, species, source, mount_medium, stain,
                plot_color,
                parmasto_length_mean, parmasto_width_mean, parmasto_q_mean,
                parmasto_v_sp_length, parmasto_v_sp_width, parmasto_v_sp_q,
                parmasto_v_ind_length, parmasto_v_ind_width, parmasto_v_ind_q,
                length_min, length_p05, length_p50, length_p95, length_max, length_avg,
                width_min, width_p05, width_p50, width_p95, width_max, width_avg,
                q_min, q_p50, q_max, q_avg,
                metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            values.get("genus"),
            values.get("species"),
            values.get("source"),
            values.get("mount_medium"),
            values.get("stain"),
            values.get("plot_color"),
            values.get("parmasto_length_mean"),
            values.get("parmasto_width_mean"),
            values.get("parmasto_q_mean"),
            values.get("parmasto_v_sp_length"),
            values.get("parmasto_v_sp_width"),
            values.get("parmasto_v_sp_q"),
            values.get("parmasto_v_ind_length"),
            values.get("parmasto_v_ind_width"),
            values.get("parmasto_v_ind_q"),
            values.get("length_min"),
            values.get("length_p05"),
            values.get("length_p50"),
            values.get("length_p95"),
            values.get("length_max"),
            values.get("length_avg"),
            values.get("width_min"),
            values.get("width_p05"),
            values.get("width_p50"),
            values.get("width_p95"),
            values.get("width_max"),
            values.get("width_avg"),
            values.get("q_min"),
            values.get("q_p50"),
            values.get("q_max"),
            values.get("q_avg"),
            metadata_json,
        ))

        conn.commit()
        conn.close()

    @staticmethod
    def delete_reference(
        genus: str,
        species: str,
        source: str = None,
        mount_medium: str = None,
        stain: str = None,
    ) -> None:
        conn = get_reference_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''
            DELETE FROM reference_values
            WHERE genus = ? AND species = ?
              AND (source = ? OR (? IS NULL AND source IS NULL))
              AND (mount_medium = ? OR (? IS NULL AND mount_medium IS NULL))
              AND (stain = ? OR (? IS NULL AND stain IS NULL))
            ''',
            (
                genus,
                species,
                source,
                source,
                mount_medium,
                mount_medium,
                stain,
                stain,
            ),
        )
        conn.commit()
        conn.close()

    @staticmethod
    def list_genera(prefix: str = "") -> List[str]:
        conn = get_reference_connection()
        cursor = conn.cursor()
        if prefix:
            cursor.execute('''
                SELECT DISTINCT genus FROM reference_values
                WHERE genus LIKE ?
                ORDER BY genus
            ''', (f"{prefix}%",))
        else:
            cursor.execute('''
                SELECT DISTINCT genus FROM reference_values
                ORDER BY genus
            ''')
        rows = cursor.fetchall()
        conn.close()
        return [row[0] for row in rows if row and row[0]]

    @staticmethod
    def list_species(genus: str, prefix: str = "") -> List[str]:
        conn = get_reference_connection()
        cursor = conn.cursor()
        if prefix:
            cursor.execute('''
                SELECT DISTINCT species FROM reference_values
                WHERE genus = ? AND species LIKE ?
                ORDER BY species
            ''', (genus, f"{prefix}%"))
        else:
            cursor.execute('''
                SELECT DISTINCT species FROM reference_values
                WHERE genus = ?
                ORDER BY species
            ''', (genus,))
        rows = cursor.fetchall()
        conn.close()
        return [row[0] for row in rows if row and row[0]]

    @staticmethod
    def list_sources(genus: str, species: str, prefix: str = "") -> List[str]:
        conn = get_reference_connection()
        cursor = conn.cursor()
        if prefix:
            cursor.execute('''
                SELECT DISTINCT source FROM reference_values
                WHERE genus = ? AND species = ? AND source LIKE ?
                ORDER BY source
            ''', (genus, species, f"{prefix}%"))
        else:
            cursor.execute('''
                SELECT DISTINCT source FROM reference_values
                WHERE genus = ? AND species = ?
                ORDER BY source
            ''', (genus, species))
        rows = cursor.fetchall()
        conn.close()
        return [row[0] for row in rows if row and row[0]]

    @staticmethod
    def list_mount_mediums(genus: str, species: str, source: str, prefix: str = "") -> List[str]:
        conn = get_reference_connection()
        cursor = conn.cursor()
        if prefix:
            cursor.execute('''
                SELECT DISTINCT mount_medium FROM reference_values
                WHERE genus = ? AND species = ? AND source = ? AND mount_medium LIKE ?
                ORDER BY mount_medium
            ''', (genus, species, source, f"{prefix}%"))
        else:
            cursor.execute('''
                SELECT DISTINCT mount_medium FROM reference_values
                WHERE genus = ? AND species = ? AND source = ?
                ORDER BY mount_medium
            ''', (genus, species, source))
        rows = cursor.fetchall()
        conn.close()
        return [row[0] for row in rows if row and row[0]]

    @staticmethod
    def list_stains(genus: str, species: str, source: str, prefix: str = "") -> List[str]:
        conn = get_reference_connection()
        cursor = conn.cursor()
        if prefix:
            cursor.execute('''
                SELECT DISTINCT stain FROM reference_values
                WHERE genus = ? AND species = ? AND source = ? AND stain LIKE ?
                ORDER BY stain
            ''', (genus, species, source, f"{prefix}%"))
        else:
            cursor.execute('''
                SELECT DISTINCT stain FROM reference_values
                WHERE genus = ? AND species = ? AND source = ?
                ORDER BY stain
            ''', (genus, species, source))
        rows = cursor.fetchall()
        conn.close()
        return [row[0] for row in rows if row and row[0]]


class SpeciesDataAvailability:
    """Cache and query data availability for species."""

    DATA_POINT_EMOJI = "🔹"
    MINMAX_EMOJI = "📏"

    def __init__(self):
        self._cache = None
        self._last_update = None

    def _build_cache(self) -> dict:
        cache: dict[tuple[str, str], dict] = {}

        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 
                o.id as obs_id,
                o.genus, 
                o.species,
                o.source_type,
                COUNT(DISTINCT sm.id) as measurement_count
            FROM observations o
            JOIN images i ON i.observation_id = o.id
            JOIN spore_measurements sm ON sm.image_id = i.id
            WHERE o.genus IS NOT NULL 
              AND o.species IS NOT NULL
              AND sm.length_um IS NOT NULL
              AND (
                    sm.measurement_type IS NULL
                    OR sm.measurement_type = ''
                    OR LOWER(sm.measurement_type) IN ('manual', 'spore', 'spores')
                  )
            GROUP BY o.id, o.genus, o.species, o.source_type
        ''')

        for row in cursor.fetchall():
            key = (row["genus"], row["species"])
            if key not in cache:
                cache[key] = {
                    "has_personal_points": False,
                    "has_shared_points": False,
                    "has_published_points": False,
                    "has_reference_minmax": False,
                    "personal_count": 0,
                    "shared_count": 0,
                    "published_count": 0,
                    "reference_count": 0,
                    "measurement_count": 0,
                    "obs_ids_by_source": {"personal": set(), "shared": set(), "published": set()},
                }

            source_type = row["source_type"] or "personal"
            if source_type not in ("personal", "shared", "published"):
                source_type = "personal"
            obs_id = row["obs_id"]
            cache[key]["obs_ids_by_source"][source_type].add(obs_id)
            cache[key]["measurement_count"] += row["measurement_count"] or 0

        for info in cache.values():
            info["personal_count"] = len(info["obs_ids_by_source"]["personal"])
            info["shared_count"] = len(info["obs_ids_by_source"]["shared"])
            info["published_count"] = len(info["obs_ids_by_source"]["published"])
            info["has_personal_points"] = info["personal_count"] > 0
            info["has_shared_points"] = info["shared_count"] > 0
            info["has_published_points"] = info["published_count"] > 0

        conn.close()

        ref_conn = get_reference_connection()
        ref_conn.row_factory = sqlite3.Row
        ref_cursor = ref_conn.cursor()
        ref_cursor.execute('''
            SELECT 
                genus, 
                species,
                COUNT(*) as ref_count
            FROM reference_values
            WHERE length_min IS NOT NULL 
               OR length_max IS NOT NULL
               OR length_p05 IS NOT NULL
               OR length_p50 IS NOT NULL
               OR length_p95 IS NOT NULL
               OR length_avg IS NOT NULL
               OR width_min IS NOT NULL
               OR width_max IS NOT NULL
               OR width_p05 IS NOT NULL
               OR width_p50 IS NOT NULL
               OR width_p95 IS NOT NULL
               OR width_avg IS NOT NULL
               OR q_min IS NOT NULL
               OR q_p50 IS NOT NULL
               OR q_max IS NOT NULL
               OR q_avg IS NOT NULL
               OR parmasto_length_mean IS NOT NULL
               OR parmasto_width_mean IS NOT NULL
               OR parmasto_q_mean IS NOT NULL
               OR parmasto_v_sp_length IS NOT NULL
               OR parmasto_v_sp_width IS NOT NULL
               OR parmasto_v_sp_q IS NOT NULL
               OR parmasto_v_ind_length IS NOT NULL
               OR parmasto_v_ind_width IS NOT NULL
               OR parmasto_v_ind_q IS NOT NULL
            GROUP BY genus, species
        ''')

        for row in ref_cursor.fetchall():
            key = (row["genus"], row["species"])
            if key not in cache:
                cache[key] = {
                    "has_personal_points": False,
                    "has_shared_points": False,
                    "has_published_points": False,
                    "has_reference_minmax": False,
                    "personal_count": 0,
                    "shared_count": 0,
                    "published_count": 0,
                    "reference_count": 0,
                    "measurement_count": 0,
                    "obs_ids_by_source": {"personal": set(), "shared": set(), "published": set()},
                }
            cache[key]["has_reference_minmax"] = True
            cache[key]["reference_count"] = row["ref_count"]

        ref_conn.close()
        return cache

    def get_cache(self, force_refresh: bool = False) -> dict:
        if force_refresh or self._cache is None:
            self._cache = self._build_cache()
            self._last_update = datetime.now()
        return self._cache

    def _apply_observation_exclusion(self, info: dict, exclude_observation_id: int | None) -> dict:
        if not exclude_observation_id:
            return info
        adjusted = dict(info)
        by_source = info.get("obs_ids_by_source") or {}
        personal_ids = by_source.get("personal", set())
        shared_ids = by_source.get("shared", set())
        published_ids = by_source.get("published", set())
        personal_count = len([obs_id for obs_id in personal_ids if obs_id != exclude_observation_id])
        shared_count = len([obs_id for obs_id in shared_ids if obs_id != exclude_observation_id])
        published_count = len([obs_id for obs_id in published_ids if obs_id != exclude_observation_id])
        adjusted["personal_count"] = personal_count
        adjusted["shared_count"] = shared_count
        adjusted["published_count"] = published_count
        adjusted["has_personal_points"] = personal_count > 0
        adjusted["has_shared_points"] = shared_count > 0
        adjusted["has_published_points"] = published_count > 0
        return adjusted

    def get_species_display_name(
        self,
        genus: str,
        species: str,
        exclude_observation_id: int | None = None,
    ) -> tuple[str, bool]:
        cache = self.get_cache()
        key = (genus, species)
        info = cache.get(
            key,
            {
                "has_personal_points": False,
                "has_shared_points": False,
                "has_published_points": False,
                "has_reference_minmax": False,
                "obs_ids_by_source": {"personal": set(), "shared": set(), "published": set()},
                "personal_count": 0,
                "shared_count": 0,
                "published_count": 0,
            },
        )
        info = self._apply_observation_exclusion(info, exclude_observation_id)

        emojis = []
        has_any_data = False

        if (
            info.get("has_personal_points")
            or info.get("has_shared_points")
            or info.get("has_published_points")
        ):
            emojis.append(self.DATA_POINT_EMOJI)
            has_any_data = True

        if info.get("has_reference_minmax"):
            emojis.append(self.MINMAX_EMOJI)
            has_any_data = True

        emoji_str = " ".join(emojis)
        display = f"{genus} {species} {emoji_str}".strip()
        return display, has_any_data

    def get_detailed_info(
        self,
        genus: str,
        species: str,
        exclude_observation_id: int | None = None,
    ) -> dict:
        cache = self.get_cache()
        info = cache.get((genus, species), {})
        return self._apply_observation_exclusion(info, exclude_observation_id)


class SettingsDB:
    """Store simple key/value settings."""

    @staticmethod
    def get_setting(key: str, default: str = None) -> str:
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.cursor()
            try:
                cursor.execute('SELECT value FROM settings WHERE key = ?', (key,))
            except sqlite3.OperationalError as exc:
                # Fresh isolated profiles may read settings before init_database()
                # has created the settings table.
                if "no such table" in str(exc).lower():
                    return default
                raise
            row = cursor.fetchone()
            return row['value'] if row else default
        finally:
            conn.close()

    @staticmethod
    def set_setting(key: str, value: str) -> None:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO settings (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        ''', (key, value))
        conn.commit()
        conn.close()

    @staticmethod
    def get_list_setting(key: str, default: list) -> list:
        raw = SettingsDB.get_setting(key)
        if not raw:
            return default
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return default
        return data if isinstance(data, list) else default

    @staticmethod
    def set_list_setting(key: str, values: list) -> None:
        SettingsDB.set_setting(key, json.dumps(values))

    @staticmethod
    def get_profile() -> dict:
        return {
            "name": SettingsDB.get_setting("profile_name", ""),
            "email": SettingsDB.get_setting("profile_email", ""),
            "bio": SettingsDB.get_setting("profile_bio", ""),
            "username": SettingsDB.get_setting("profile_username", ""),
            "avatar_url": SettingsDB.get_setting("profile_avatar_url", ""),
        }

    @staticmethod
    def set_profile(
        name: str,
        email: str,
        bio: str = "",
        username: str = "",
        avatar_url: str = "",
    ) -> None:
        SettingsDB.set_setting("profile_name", name or "")
        SettingsDB.set_setting("profile_email", email or "")
        SettingsDB.set_setting("profile_bio", bio or "")
        SettingsDB.set_setting("profile_username", username or "")
        SettingsDB.set_setting("profile_avatar_url", avatar_url or "")


class CalibrationDB:
    """Handle calibration database operations for microscope objectives."""

    @staticmethod
    def _estimate_calibration_megapixels(cal: dict) -> float | None:
        if not cal:
            return None
        values: list[float] = []

        def _add_from_path(path: str | None) -> None:
            if not path:
                return
            try:
                from PIL import Image
            except Exception:
                return
            try:
                with Image.open(path) as img:
                    if img.width > 0 and img.height > 0:
                        values.append((img.width * img.height) / 1_000_000.0)
            except Exception:
                return

        measurements_json = cal.get("measurements_json")
        if measurements_json:
            try:
                loaded = json.loads(measurements_json)
            except Exception:
                loaded = None
            if isinstance(loaded, dict):
                for info in loaded.get("images") or []:
                    crop_source = info.get("crop_source_size")
                    if crop_source and len(crop_source) == 2:
                        try:
                            source_w = float(crop_source[0])
                            source_h = float(crop_source[1])
                        except (TypeError, ValueError):
                            source_w = source_h = 0
                        if source_w > 0 and source_h > 0:
                            values.append((source_w * source_h) / 1_000_000.0)
                            continue
                    _add_from_path(info.get("path"))
        if not values:
            _add_from_path(cal.get("image_filepath"))
        if values:
            return float(sum(values) / len(values))
        return None

    @staticmethod
    def backfill_megapixels(diff_threshold: float = 0.01, force: bool = False) -> int:
        """Backfill calibration megapixels using full image sizes."""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT id, megapixels, measurements_json, image_filepath FROM calibrations")
        rows = cursor.fetchall()
        updated = 0
        for row in rows:
            cal = dict(row)
            estimate = CalibrationDB._estimate_calibration_megapixels(cal)
            if not estimate:
                continue
            mp_value = cal.get("megapixels")
            if isinstance(mp_value, (int, float)) and mp_value > 0:
                diff_ratio = abs(float(mp_value) - float(estimate)) / max(1e-6, float(estimate))
                if not force and diff_ratio <= diff_threshold:
                    continue
            cursor.execute(
                "UPDATE calibrations SET megapixels = ? WHERE id = ?",
                (float(estimate), cal.get("id")),
            )
            updated += 1
        conn.commit()
        conn.close()
        return updated

    @staticmethod
    def add_calibration(
        objective_key: str,
        microns_per_pixel: float,
        calibration_date: str = None,
        calibration_image_date: str | None = None,
        microns_per_pixel_std: float = None,
        confidence_interval_low: float = None,
        confidence_interval_high: float = None,
        num_measurements: int = None,
        measurements_json: str = None,
        image_filepath: str = None,
        camera: str = None,
        megapixels: float = None,
        target_sampling_pct: float | None = None,
        resample_scale_factor: float | None = None,
        calibration_image_width: int | None = None,
        calibration_image_height: int | None = None,
        notes: str = None,
        set_active: bool = True,
    ) -> int:
        """Add a new calibration record and return its ID."""
        conn = get_connection()
        cursor = conn.cursor()

        if calibration_date is None:
            calibration_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # If setting as active, deactivate other calibrations for this objective
        if set_active:
            cursor.execute(
                "UPDATE calibrations SET is_active = 0 WHERE objective_key = ?",
                (objective_key,)
            )

        cursor.execute("PRAGMA table_info(calibrations)")
        columns = {row[1] for row in cursor.fetchall()}

        insert_cols = [
            "objective_key",
            "calibration_date",
            "calibration_image_date",
            "microns_per_pixel",
            "microns_per_pixel_std",
            "confidence_interval_low",
            "confidence_interval_high",
            "num_measurements",
            "measurements_json",
            "image_filepath",
        ]
        values = [
            objective_key,
            calibration_date,
            calibration_image_date,
            microns_per_pixel,
            microns_per_pixel_std,
            confidence_interval_low,
            confidence_interval_high,
            num_measurements,
            measurements_json,
            image_filepath,
        ]

        if "camera" in columns:
            insert_cols.append("camera")
            values.append(camera)
        if "megapixels" in columns:
            insert_cols.append("megapixels")
            values.append(megapixels)
        if "target_sampling_pct" in columns:
            insert_cols.append("target_sampling_pct")
            values.append(target_sampling_pct)
        if "resample_scale_factor" in columns:
            insert_cols.append("resample_scale_factor")
            values.append(resample_scale_factor)
        if "calibration_image_width" in columns:
            insert_cols.append("calibration_image_width")
            values.append(calibration_image_width)
        if "calibration_image_height" in columns:
            insert_cols.append("calibration_image_height")
            values.append(calibration_image_height)

        insert_cols.extend(["notes", "is_active"])
        values.extend([notes, 1 if set_active else 0])

        placeholders = ", ".join(["?"] * len(insert_cols))
        cols_sql = ", ".join(insert_cols)
        cursor.execute(
            f"INSERT INTO calibrations ({cols_sql}) VALUES ({placeholders})",
            values,
        )

        calibration_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return calibration_id

    @staticmethod
    def get_calibration(calibration_id: int) -> Optional[dict]:
        """Get a single calibration by ID."""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM calibrations WHERE id = ?", (calibration_id,))
        row = cursor.fetchone()
        conn.close()

        return dict(row) if row else None

    @staticmethod
    def get_calibrations_for_objective(objective_key: str) -> List[dict]:
        """Get all calibrations for an objective, ordered by date descending."""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM calibrations
            WHERE objective_key = ?
            ORDER BY calibration_date DESC
        ''', (objective_key,))

        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    @staticmethod
    def get_active_calibration(objective_key: str) -> Optional[dict]:
        """Get the active calibration for an objective."""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM calibrations
            WHERE objective_key = ? AND is_active = 1
            ORDER BY calibration_date DESC
            LIMIT 1
        ''', (objective_key,))

        row = cursor.fetchone()
        conn.close()

        return dict(row) if row else None

    @staticmethod
    def get_active_calibration_id(objective_key: str) -> Optional[int]:
        """Get the active calibration ID for an objective, or None if not set."""
        cal = CalibrationDB.get_active_calibration(objective_key)
        return cal.get("id") if cal else None

    @staticmethod
    def set_active_calibration(calibration_id: int) -> None:
        """Set a calibration as active, deactivating others for the same objective."""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get the objective key for this calibration
        cursor.execute("SELECT objective_key FROM calibrations WHERE id = ?", (calibration_id,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return

        objective_key = row["objective_key"]

        # Deactivate all calibrations for this objective
        cursor.execute(
            "UPDATE calibrations SET is_active = 0 WHERE objective_key = ?",
            (objective_key,)
        )

        # Activate the specified calibration
        cursor.execute(
            "UPDATE calibrations SET is_active = 1 WHERE id = ?",
            (calibration_id,)
        )

        conn.commit()
        conn.close()

    @staticmethod
    def get_calibration_history(objective_key: str) -> List[dict]:
        """Get calibration history with % difference from the first calibration."""
        calibrations = CalibrationDB.get_calibrations_for_objective(objective_key)
        if not calibrations:
            return []

        # Sort by date ascending to find the first calibration
        sorted_by_date = sorted(calibrations, key=lambda c: c.get("calibration_date", ""))
        first_calibration = sorted_by_date[0] if sorted_by_date else None
        first_value = first_calibration.get("microns_per_pixel") if first_calibration else None

        history = []
        for cal in calibrations:
            cal_copy = dict(cal)
            if first_value and first_value > 0:
                current_value = cal.get("microns_per_pixel", 0)
                if current_value and cal["id"] != first_calibration["id"]:
                    diff_percent = ((current_value - first_value) / first_value) * 100
                    cal_copy["diff_from_first_percent"] = diff_percent
                else:
                    cal_copy["diff_from_first_percent"] = None  # First calibration has no diff
            else:
                cal_copy["diff_from_first_percent"] = None
            history.append(cal_copy)

        return history

    @staticmethod
    def delete_calibration(calibration_id: int) -> list[str]:
        """Delete a calibration by ID.

        Returns a list of file or folder paths that could not be deleted.
        """
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT objective_key, image_filepath, measurements_json FROM calibrations WHERE id = ?",
            (calibration_id,),
        )
        row = cursor.fetchone()
        cursor.execute("DELETE FROM calibrations WHERE id = ?", (calibration_id,))
        conn.commit()
        conn.close()

        image_paths: set[Path] = set()
        failed_paths: list[str] = []
        if row:
            image_filepath = row["image_filepath"]
            if image_filepath:
                image_paths.add(Path(image_filepath))
            measurements_json = row["measurements_json"]
            if measurements_json:
                try:
                    loaded = json.loads(measurements_json)
                except Exception:
                    loaded = None
                if isinstance(loaded, dict):
                    for entry in loaded.get("images", []):
                        if isinstance(entry, dict):
                            path = entry.get("path")
                            if path:
                                image_paths.add(Path(path))

        if image_paths:
            images_root = get_images_dir().resolve()
            for path in image_paths:
                try:
                    target = path
                    if not target.is_absolute():
                        target = images_root / target
                    target = target.resolve()
                    if images_root in target.parents and target.exists():
                        target.unlink()
                except Exception:
                    failed_paths.append(str(path))
        objective_key = None
        if row:
            try:
                objective_key = row["objective_key"]
            except Exception:
                objective_key = None
        if objective_key:
            try:
                cal_dir = get_calibrations_dir() / objective_key
                if cal_dir.exists() and not any(cal_dir.iterdir()):
                    cal_dir.rmdir()
            except Exception:
                failed_paths.append(str(cal_dir))
        return failed_paths

    @staticmethod
    def delete_calibrations_for_objective(objective_key: str) -> list[str]:
        """Delete all calibrations (and calibration images) for an objective.

        Returns a list of file or folder paths that could not be deleted.
        """
        if not objective_key:
            return []
        calibrations = CalibrationDB.get_calibrations_for_objective(objective_key)
        failed_paths: list[str] = []
        for cal in calibrations:
            cal_id = None
            if isinstance(cal, dict):
                cal_id = cal.get("id")
            else:
                try:
                    cal_id = cal["id"]
                except Exception:
                    cal_id = None
            if cal_id:
                failed_paths.extend(CalibrationDB.delete_calibration(cal_id))
        try:
            cal_dir = get_calibrations_dir() / objective_key
            if cal_dir.exists():
                shutil.rmtree(cal_dir)
        except Exception:
            failed_paths.append(str(cal_dir))
        return failed_paths

    @staticmethod
    def clear_objective_usage(objective_key: str) -> None:
        """Clear objective usage from images tied to an objective."""
        if not objective_key:
            return
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM calibrations WHERE objective_key = ?", (objective_key,))
        cal_ids = [row[0] for row in cursor.fetchall()]
        if cal_ids:
            placeholders = ",".join("?" for _ in cal_ids)
            params = [objective_key, *cal_ids]
            cursor.execute(
                f'''
                UPDATE images
                SET calibration_id = NULL,
                    scale_microns_per_pixel = NULL,
                    objective_name = NULL
                WHERE objective_name = ? OR calibration_id IN ({placeholders})
                ''',
                params,
            )
        else:
            cursor.execute(
                '''
                UPDATE images
                SET calibration_id = NULL,
                    scale_microns_per_pixel = NULL,
                    objective_name = NULL
                WHERE objective_name = ?
                ''',
                (objective_key,),
            )
        conn.commit()
        conn.close()

    @staticmethod
    def clear_calibration_usage(calibration_id: int, clear_objective: bool = True) -> None:
        """Clear calibration usage from images tied to a calibration."""
        conn = get_connection()
        cursor = conn.cursor()
        updates = ["calibration_id = NULL", "scale_microns_per_pixel = NULL"]
        if clear_objective:
            updates.append("objective_name = NULL")
        cursor.execute(
            f"UPDATE images SET {', '.join(updates)} WHERE calibration_id = ?",
            (calibration_id,),
        )
        conn.commit()
        conn.close()

    @staticmethod
    def get_images_using_objective(objective_key: str) -> List[dict]:
        """Get all images that use a specific objective."""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
            SELECT i.*, o.id AS observation_id, o.genus, o.species, o.common_name, o.date
            FROM images i
            LEFT JOIN observations o ON i.observation_id = o.id
            WHERE i.objective_name = ?
        ''', (objective_key,))

        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    @staticmethod
    def get_images_by_calibration(calibration_id: int) -> List[dict]:
        """Get all images that used a specific calibration."""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
            SELECT i.*, o.id AS observation_id, o.genus, o.species, o.common_name, o.date,
                   (SELECT COUNT(*) FROM spore_measurements WHERE image_id = i.id) AS measurement_count
            FROM images i
            LEFT JOIN observations o ON i.observation_id = o.id
            WHERE i.calibration_id = ?
            ORDER BY i.created_at DESC
        ''', (calibration_id,))

        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    @staticmethod
    def get_calibration_usage_summary(objective_key: str) -> List[dict]:
        """Get summary of how many observations/images/measurements use each calibration for an objective."""
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
            SELECT
                c.id AS calibration_id,
                c.calibration_date,
                c.microns_per_pixel,
                c.is_active,
                COUNT(DISTINCT i.observation_id) AS observation_count,
                COUNT(DISTINCT i.id) AS image_count,
                COALESCE(SUM(
                    (SELECT COUNT(*) FROM spore_measurements WHERE image_id = i.id)
                ), 0) AS measurement_count
            FROM calibrations c
            LEFT JOIN images i ON i.calibration_id = c.id
            WHERE c.objective_key = ?
            GROUP BY c.id
            ORDER BY c.calibration_date DESC
        ''', (objective_key,))

        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    @staticmethod
    def recalculate_measurements_for_objective(
        objective_key: str,
        old_scale: float,
        new_scale: float
    ) -> int:
        """Recalculate all measurements for images using an objective.

        Returns the number of measurements updated.
        """
        if old_scale <= 0 or new_scale <= 0:
            return 0

        scale_ratio = new_scale / old_scale

        conn = get_connection()
        cursor = conn.cursor()

        # Get all image IDs using this objective
        cursor.execute(
            "SELECT id FROM images WHERE objective_name = ?",
            (objective_key,)
        )
        image_ids = [row[0] for row in cursor.fetchall()]

        if not image_ids:
            conn.close()
            return 0

        # Update scale on images
        cursor.execute(
            "UPDATE images SET scale_microns_per_pixel = ? WHERE objective_name = ?",
            (new_scale, objective_key)
        )

        # Update measurements
        placeholders = ",".join("?" * len(image_ids))
        cursor.execute(f'''
            UPDATE spore_measurements
            SET length_um = length_um * ?,
                width_um = CASE WHEN width_um IS NOT NULL THEN width_um * ? ELSE NULL END
            WHERE image_id IN ({placeholders})
        ''', [scale_ratio, scale_ratio] + image_ids)

        updated_count = cursor.rowcount
        conn.commit()
        conn.close()

        return updated_count

    @staticmethod
    def recalculate_measurements_for_calibration(
        calibration_id: int,
        new_calibration_id: int,
        new_scale: float
    ) -> int:
        """Recalculate measurements for images that used a specific calibration.

        Updates the images to use the new calibration and recalculates their measurements.
        Returns the number of measurements updated.
        """
        conn = get_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get images using the old calibration
        cursor.execute(
            "SELECT id, scale_microns_per_pixel FROM images WHERE calibration_id = ?",
            (calibration_id,)
        )
        images = cursor.fetchall()

        if not images:
            conn.close()
            return 0

        total_updated = 0

        for img in images:
            image_id = img["id"]
            old_scale = img["scale_microns_per_pixel"] or 0

            if old_scale <= 0 or new_scale <= 0:
                continue

            scale_ratio = new_scale / old_scale

            # Update the image's calibration and scale
            cursor.execute(
                "UPDATE images SET calibration_id = ?, scale_microns_per_pixel = ? WHERE id = ?",
                (new_calibration_id, new_scale, image_id)
            )

            # Update measurements for this image
            cursor.execute('''
                UPDATE spore_measurements
                SET length_um = length_um * ?,
                    width_um = CASE WHEN width_um IS NOT NULL THEN width_um * ? ELSE NULL END
                WHERE image_id = ?
            ''', (scale_ratio, scale_ratio, image_id))

            total_updated += cursor.rowcount

        conn.commit()
        conn.close()

        return total_updated
