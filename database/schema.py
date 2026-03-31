"""Database schema and initialization"""
import json
import re
import sqlite3
import shutil
from pathlib import Path

from app_identity import app_data_dir
from database.database_tags import DatabaseTerms

_app_dir = app_data_dir()
DATABASE_PATH = _app_dir / "mushrooms.db"
REFERENCE_DATABASE_PATH = _app_dir / "reference_values.db"
BUNDLED_REFERENCE_DATABASE_PATH = Path(__file__).resolve().with_name("reference_values.db")
SETTINGS_PATH = _app_dir / "app_settings.json"

DEFAULT_OBJECTIVES = {
    "100X": {
        "optics_type": "microscope",
        "magnification": 100.0,
        "na": 1.25,
        "objective_name": "Plan achro",
        "name": "100X/1.25 Plan achro",
        "microns_per_pixel": 0.0315,
        "notes": "Leica DM2000, Olympus MFT 1:1",
    },
}

_DEFAULT_MEASURE_CATEGORIES = [
    "Spores",
    "Field",
    "Basidia",
    "Pileipellis",
    "Pleurocystidia",
    "Cheilocystidia",
    "Caulocystidia",
    "Other",
]

_DEFAULT_CONTRAST_METHODS = [
    "Not_set",
    "BF",
    "DF",
    "DIC",
    "Oblique",
    "Phase",
    "HMC",
]

_DEFAULT_MOUNT_MEDIA = [
    "Not_set",
    "Water",
    "KOH",
    "NH3",
    "Glycerine",
    "L4",
]

_DEFAULT_STAIN_TYPES = [
    "Not_set",
    "Melzer",
    "Congo_Red",
    "Cotton_Blue",
    "Lactofuchsin",
    "Cresyl_Blue",
    "Trypan_Blue",
    "Chlorazol_Black_E",
]


def _format_objective_number(value, decimals: int = 2) -> str:
    if value is None:
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if abs(number - round(number)) < 1e-6:
        return str(int(round(number)))
    text = f"{number:.{max(0, decimals)}f}".rstrip("0").rstrip(".")
    return text


def format_objective_display(
    magnification,
    numerical_aperture,
    objective_name: str | None,
    optics_type: str | None = "microscope",
) -> str:
    optics = str(optics_type or "microscope").strip().lower()
    mag_text = _format_objective_number(magnification, decimals=2)
    na_text = _format_objective_number(numerical_aperture, decimals=2)
    base = ""
    if optics == "macro":
        if mag_text:
            base = f"1:{mag_text}"
        elif na_text:
            base = f"NA {na_text}"
    else:
        if mag_text and na_text:
            base = f"{mag_text}X/{na_text}"
        elif mag_text:
            base = f"{mag_text}X"
        elif na_text:
            base = f"NA {na_text}"
    name = (objective_name or "").strip()
    if name:
        return f"{base} {name}".strip()
    return base.strip()


def _parse_magnification(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    match = re.search(r"(\d+(?:\.\d+)?)", str(value))
    return float(match.group(1)) if match else None


def _parse_na(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    match = re.search(r"/\s*([0-9.]+)", str(value))
    return float(match.group(1)) if match else None


def _parse_objective_name(text: str | None) -> str | None:
    if not text:
        return None
    match = re.match(r"\s*(\d+(?:\.\d+)?)\s*[xX]\s*/\s*([0-9.]+)\s*(.*)$", str(text))
    if match:
        name = match.group(3).strip()
        return name or None
    return None


def _normalize_optics_type(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if text in {"macro", "macro_lens", "macro lens", "camera"}:
        return "macro"
    return "microscope"


def _upgrade_objective_entry(key: str, obj: dict) -> tuple[dict, bool]:
    updated = False
    entry = dict(obj) if isinstance(obj, dict) else {}
    magnification = entry.get("magnification")
    na_value = entry.get("na") or entry.get("numerical_aperture")
    objective_name = entry.get("objective_name")
    legacy_name = entry.get("name")
    optics_type = _normalize_optics_type(entry.get("optics_type"))
    if entry.get("optics_type") != optics_type:
        entry["optics_type"] = optics_type
        updated = True

    parsed_mag = _parse_magnification(magnification)
    if parsed_mag is None:
        parsed_mag = _parse_magnification(key) or _parse_magnification(legacy_name)
    if parsed_mag is not None and parsed_mag != magnification:
        entry["magnification"] = parsed_mag
        updated = True

    parsed_na = _parse_na(na_value)
    if parsed_na is None:
        parsed_na = _parse_na(legacy_name)
    if parsed_na is not None and parsed_na != na_value:
        entry["na"] = parsed_na
        updated = True

    if not objective_name:
        parsed_name = _parse_objective_name(legacy_name)
        if parsed_name:
            entry["objective_name"] = parsed_name
            updated = True

    normalized_objective_name = str(entry.get("objective_name") or "")
    corrected_objective_name = re.sub(r"\bacrho\b", "achro", normalized_objective_name, flags=re.IGNORECASE)
    if corrected_objective_name != normalized_objective_name:
        entry["objective_name"] = corrected_objective_name
        updated = True

    display_name = format_objective_display(
        entry.get("magnification"),
        entry.get("na"),
        entry.get("objective_name"),
        entry.get("optics_type"),
    )
    if entry.get("objective_name"):
        if display_name and entry.get("name") != display_name:
            entry["name"] = display_name
            updated = True
    elif not entry.get("name") and display_name:
        entry["name"] = display_name
        updated = True

    return entry, updated


def _upgrade_objectives(objectives: dict) -> tuple[dict, bool]:
    if not isinstance(objectives, dict):
        return {}, False
    updated = False
    normalized = {}
    for key, obj in objectives.items():
        entry, changed = _upgrade_objective_entry(key, obj)
        if changed:
            updated = True
        normalized[key] = entry
    return normalized, updated


def objective_display_name(obj: dict, fallback_key: str | None = None) -> str:
    if not isinstance(obj, dict):
        return fallback_key or ""
    optics = _normalize_optics_type(obj.get("optics_type"))
    prefix = "Macro"
    objective_name = obj.get("objective_name")
    if objective_name:
        display = format_objective_display(
            obj.get("magnification"),
            obj.get("na"),
            objective_name,
            obj.get("optics_type"),
        )
        if display:
            return f"{prefix} • {display}" if optics == "macro" else display
    name = obj.get("name")
    if name:
        return f"{prefix} • {str(name)}" if optics == "macro" else str(name)
    fallback = str(fallback_key) if fallback_key is not None else ""
    if optics == "macro":
        return f"{prefix} • {fallback}" if fallback else prefix
    return fallback


def objective_sort_value(obj: dict, fallback_key: str | None = None) -> float:
    optics_group = 10000.0 if _normalize_optics_type(obj.get("optics_type") if isinstance(obj, dict) else None) == "macro" else 0.0
    if isinstance(obj, dict):
        mag = obj.get("magnification")
        parsed = _parse_magnification(mag)
        if parsed is not None:
            return optics_group + parsed
    parsed = _parse_magnification(fallback_key)
    return optics_group + (parsed if parsed is not None else 9999.0)


def resolve_objective_key(name: str | None, objectives: dict) -> str | None:
    if not name or not isinstance(objectives, dict):
        return None
    if name in objectives:
        return name
    normalized = str(name).strip()
    if not normalized:
        return None
    for key, obj in objectives.items():
        display = objective_display_name(obj, key)
        if display == normalized:
            return key
    # Case-insensitive match on display text
    lower_name = normalized.lower()
    for key, obj in objectives.items():
        display = objective_display_name(obj, key)
        if display and display.lower() == lower_name:
            return key
    mag = _parse_magnification(normalized)
    if mag is not None:
        matches = []
        for key, obj in objectives.items():
            obj_mag = _parse_magnification(obj.get("magnification"))
            if obj_mag is not None and abs(obj_mag - mag) < 1e-6:
                matches.append(key)
        if len(matches) == 1:
            return matches[0]
    return None


def get_app_dir() -> Path:
    return _app_dir


def get_objectives_path() -> Path:
    return _app_dir / "objectives.json"


def get_last_objective_path() -> Path:
    return _app_dir / "last_objective.json"


def load_objectives() -> dict:
    path = get_objectives_path()
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            if isinstance(data, dict):
                upgraded, changed = _upgrade_objectives(data)
                if changed:
                    save_objectives(upgraded)
                return upgraded
        except (OSError, json.JSONDecodeError):
            pass
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized, _changed = _upgrade_objectives(DEFAULT_OBJECTIVES)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(normalized, handle, indent=2)
    return dict(normalized)


def save_objectives(objectives: dict) -> None:
    path = get_objectives_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized, _changed = _upgrade_objectives(objectives)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(normalized, handle, indent=2)

def _load_app_settings() -> dict:
    if not SETTINGS_PATH.exists():
        return {}
    try:
        with open(SETTINGS_PATH, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}

def get_app_settings() -> dict:
    return _load_app_settings()

def save_app_settings(settings: dict) -> None:
    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SETTINGS_PATH, "w", encoding="utf-8") as handle:
        json.dump(settings, handle, indent=2)

def update_app_settings(updates: dict) -> dict:
    settings = _load_app_settings()
    settings.update(updates)
    save_app_settings(settings)
    return settings

def get_database_path() -> Path:
    settings = _load_app_settings()
    folder = settings.get("database_folder")
    if folder:
        return Path(folder) / "mushrooms.db"
    path = settings.get("database_path")
    return Path(path) if path else DATABASE_PATH

def get_reference_database_path() -> Path:
    settings = _load_app_settings()
    folder = settings.get("database_folder")
    if folder:
        return Path(folder) / "reference_values.db"
    path = settings.get("reference_database_path")
    if path:
        return Path(path)
    return get_database_path().parent / "reference_values.db"


def get_bundled_reference_database_path() -> Path:
    return BUNDLED_REFERENCE_DATABASE_PATH

def get_images_dir() -> Path:
    settings = _load_app_settings()
    path = settings.get("images_dir")
    if path:
        return Path(path)
    return get_database_path().parent / "images"


def get_calibrations_dir() -> Path:
    """Get the directory for storing calibration images."""
    return get_images_dir() / "calibrations"

def get_connection():
    """Get a connection to the main observation database."""
    db_path = get_database_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=10)
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn

def get_reference_connection():
    """Get a connection to the reference values database."""
    ref_path = get_reference_database_path()
    ref_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(ref_path, timeout=10)
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn

def _copy_reference_seed_rows(source_path: Path, target_path: Path) -> bool:
    if not source_path.exists():
        return False
    try:
        if source_path.resolve() == target_path.resolve():
            return False
    except FileNotFoundError:
        return False

    source_conn = sqlite3.connect(source_path)
    target_conn = sqlite3.connect(target_path)
    try:
        target_cursor = target_conn.cursor()
        target_cursor.execute("SELECT COUNT(*) FROM reference_values")
        if int(target_cursor.fetchone()[0] or 0) > 0:
            return False

        source_cursor = source_conn.cursor()
        source_cursor.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name = 'reference_values'
            """
        )
        if not source_cursor.fetchone():
            return False

        source_columns = [row[1] for row in source_cursor.execute("PRAGMA table_info(reference_values)").fetchall()]
        target_columns = [row[1] for row in target_cursor.execute("PRAGMA table_info(reference_values)").fetchall()]
        columns = [column for column in source_columns if column in target_columns and column != "id"]
        if not columns:
            return False

        rows = source_cursor.execute(
            f"SELECT {', '.join(columns)} FROM reference_values ORDER BY id"
        ).fetchall()
        if not rows:
            return False

        placeholders = ", ".join("?" for _ in columns)
        target_cursor.executemany(
            f"INSERT INTO reference_values ({', '.join(columns)}) VALUES ({placeholders})",
            rows,
        )
        target_conn.commit()
        return True
    finally:
        source_conn.close()
        target_conn.close()


def init_reference_database(
    ref_path: Path | None = None,
    *,
    seed_from_bundle: bool = True,
    migrate_legacy: bool = True,
):
    """Initialize the reference values database."""
    ref_path = Path(ref_path) if ref_path is not None else get_reference_database_path()
    ref_path.parent.mkdir(parents=True, exist_ok=True)
    bundled_path = get_bundled_reference_database_path()
    if seed_from_bundle and bundled_path.exists():
        try:
            same_path = bundled_path.resolve() == ref_path.resolve()
        except FileNotFoundError:
            same_path = False
        if not same_path and not ref_path.exists():
            shutil.copy2(bundled_path, ref_path)

    conn = sqlite3.connect(ref_path)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reference_values (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            genus TEXT NOT NULL,
            species TEXT NOT NULL,
            source TEXT,
            mount_medium TEXT,
            stain TEXT,
            plot_color TEXT,
            parmasto_length_mean REAL,
            parmasto_width_mean REAL,
            parmasto_q_mean REAL,
            parmasto_v_sp_length REAL,
            parmasto_v_sp_width REAL,
            parmasto_v_sp_q REAL,
            parmasto_v_ind_length REAL,
            parmasto_v_ind_width REAL,
            parmasto_v_ind_q REAL,
            length_min REAL,
            length_p05 REAL,
            length_p50 REAL,
            length_p95 REAL,
            length_max REAL,
            length_avg REAL,
            width_min REAL,
            width_p05 REAL,
            width_p50 REAL,
            width_p95 REAL,
            width_max REAL,
            width_avg REAL,
            q_min REAL,
            q_p50 REAL,
            q_max REAL,
            q_avg REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    conn.commit()
    conn.close()

    _ensure_reference_columns(ref_path)
    if seed_from_bundle:
        _copy_reference_seed_rows(bundled_path, ref_path)
    if migrate_legacy:
        _migrate_reference_values(ref_path)
    _migrate_reference_mounts_and_stains(ref_path)

def _ensure_reference_columns(ref_path: Path | None = None):
    """Ensure new percentile columns exist in the reference values table."""
    path = Path(ref_path) if ref_path is not None else get_reference_database_path()
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(reference_values)")
    existing = {row[1] for row in cursor.fetchall()}
    to_add = {
        "stain": "TEXT",
        "plot_color": "TEXT",
        "parmasto_length_mean": "REAL",
        "parmasto_width_mean": "REAL",
        "parmasto_q_mean": "REAL",
        "parmasto_v_sp_length": "REAL",
        "parmasto_v_sp_width": "REAL",
        "parmasto_v_sp_q": "REAL",
        "parmasto_v_ind_length": "REAL",
        "parmasto_v_ind_width": "REAL",
        "parmasto_v_ind_q": "REAL",
        "length_p05": "REAL",
        "length_p50": "REAL",
        "length_p95": "REAL",
        "width_p05": "REAL",
        "width_p50": "REAL",
        "width_p95": "REAL",
        "q_p50": "REAL",
    }
    for col, col_type in to_add.items():
        if col not in existing:
            cursor.execute(f"ALTER TABLE reference_values ADD COLUMN {col} {col_type}")
    conn.commit()
    conn.close()

def _migrate_reference_values(ref_path: Path | None = None):
    """Copy legacy reference values from the main database if needed."""
    path = Path(ref_path) if ref_path is not None else get_reference_database_path()
    ref_conn = sqlite3.connect(path)
    ref_cursor = ref_conn.cursor()
    ref_cursor.execute('SELECT COUNT(*) FROM reference_values')
    ref_count = ref_cursor.fetchone()[0]
    ref_conn.close()

    if ref_count:
        return

    main_conn = sqlite3.connect(get_database_path())
    main_cursor = main_conn.cursor()
    main_cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type = 'table' AND name = 'reference_values'
    """)
    if not main_cursor.fetchone():
        main_conn.close()
        return

    main_cursor.execute("PRAGMA table_info(reference_values)")
    main_cols = {row[1] for row in main_cursor.fetchall()}
    has_p05 = "length_p05" in main_cols
    has_p50 = "length_p50" in main_cols
    has_p95 = "length_p95" in main_cols
    has_wp05 = "width_p05" in main_cols
    has_wp50 = "width_p50" in main_cols
    has_wp95 = "width_p95" in main_cols
    has_qp50 = "q_p50" in main_cols
    has_stain = "stain" in main_cols
    has_plot_color = "plot_color" in main_cols
    has_parmasto_length_mean = "parmasto_length_mean" in main_cols
    has_parmasto_width_mean = "parmasto_width_mean" in main_cols
    has_parmasto_q_mean = "parmasto_q_mean" in main_cols
    has_parmasto_v_sp_length = "parmasto_v_sp_length" in main_cols
    has_parmasto_v_sp_width = "parmasto_v_sp_width" in main_cols
    has_parmasto_v_sp_q = "parmasto_v_sp_q" in main_cols
    has_parmasto_v_ind_length = "parmasto_v_ind_length" in main_cols
    has_parmasto_v_ind_width = "parmasto_v_ind_width" in main_cols
    has_parmasto_v_ind_q = "parmasto_v_ind_q" in main_cols

    if (
        has_p05 or has_p50 or has_p95 or has_wp05 or has_wp50 or has_wp95 or has_qp50
        or has_stain or has_plot_color
        or has_parmasto_length_mean or has_parmasto_width_mean or has_parmasto_q_mean
        or has_parmasto_v_sp_length or has_parmasto_v_sp_width or has_parmasto_v_sp_q
        or has_parmasto_v_ind_length or has_parmasto_v_ind_width or has_parmasto_v_ind_q
    ):
        main_cursor.execute('''
            SELECT genus, species, source, mount_medium,
                   {stain_expr},
                   {plot_color_expr},
                   {parmasto_length_mean_expr},
                   {parmasto_width_mean_expr},
                   {parmasto_q_mean_expr},
                   {parmasto_v_sp_length_expr},
                   {parmasto_v_sp_width_expr},
                   {parmasto_v_sp_q_expr},
                   {parmasto_v_ind_length_expr},
                   {parmasto_v_ind_width_expr},
                   {parmasto_v_ind_q_expr},
                   length_min, length_p05, length_p50, length_p95, length_max, length_avg,
                   width_min, width_p05, width_p50, width_p95, width_max, width_avg,
                   q_min, q_p50, q_max, q_avg, updated_at
            FROM reference_values
        '''.format(
            stain_expr="stain" if has_stain else "NULL",
            plot_color_expr="plot_color" if has_plot_color else "NULL",
            parmasto_length_mean_expr="parmasto_length_mean" if has_parmasto_length_mean else "NULL",
            parmasto_width_mean_expr="parmasto_width_mean" if has_parmasto_width_mean else "NULL",
            parmasto_q_mean_expr="parmasto_q_mean" if has_parmasto_q_mean else "NULL",
            parmasto_v_sp_length_expr="parmasto_v_sp_length" if has_parmasto_v_sp_length else "NULL",
            parmasto_v_sp_width_expr="parmasto_v_sp_width" if has_parmasto_v_sp_width else "NULL",
            parmasto_v_sp_q_expr="parmasto_v_sp_q" if has_parmasto_v_sp_q else "NULL",
            parmasto_v_ind_length_expr="parmasto_v_ind_length" if has_parmasto_v_ind_length else "NULL",
            parmasto_v_ind_width_expr="parmasto_v_ind_width" if has_parmasto_v_ind_width else "NULL",
            parmasto_v_ind_q_expr="parmasto_v_ind_q" if has_parmasto_v_ind_q else "NULL",
        ))
    else:
        main_cursor.execute('''
            SELECT genus, species, source, mount_medium, NULL, NULL,
                   NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                   length_min, NULL, NULL, NULL, length_max, length_avg,
                   width_min, NULL, NULL, NULL, width_max, width_avg,
                   q_min, NULL, q_max, q_avg, updated_at
            FROM reference_values
        ''')
    rows = main_cursor.fetchall()
    main_conn.close()

    if not rows:
        return

    ref_conn = sqlite3.connect(path)
    ref_cursor = ref_conn.cursor()
    ref_cursor.executemany('''
        INSERT INTO reference_values (
            genus, species, source, mount_medium, stain,
            plot_color,
            parmasto_length_mean, parmasto_width_mean, parmasto_q_mean,
            parmasto_v_sp_length, parmasto_v_sp_width, parmasto_v_sp_q,
            parmasto_v_ind_length, parmasto_v_ind_width, parmasto_v_ind_q,
            length_min, length_p05, length_p50, length_p95, length_max, length_avg,
            width_min, width_p05, width_p50, width_p95, width_max, width_avg,
            q_min, q_p50, q_max, q_avg, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', rows)
    ref_conn.commit()
    ref_conn.close()


def _migrate_reference_mounts_and_stains(ref_path: Path | None = None) -> None:
    """Move legacy stain-only mount values into the dedicated stain column."""
    path = Path(ref_path) if ref_path is not None else get_reference_database_path()
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    cursor.execute("SELECT id, mount_medium, stain FROM reference_values")
    for row_id, mount_medium, stain in cursor.fetchall():
        if str(stain or "").strip():
            canonical_stain = DatabaseTerms.canonicalize("stain", stain)
            if canonical_stain and canonical_stain != stain:
                cursor.execute(
                    "UPDATE reference_values SET stain = ? WHERE id = ?",
                    (canonical_stain, row_id),
                )
            continue
        canonical_stain = DatabaseTerms.canonicalize("stain", mount_medium)
        if canonical_stain in _DEFAULT_STAIN_TYPES[1:]:
            cursor.execute(
                "UPDATE reference_values SET stain = ?, mount_medium = NULL WHERE id = ?",
                (canonical_stain, row_id),
            )
    conn.commit()
    conn.close()


def _migrate_measure_categories_setting(cursor: sqlite3.Cursor) -> None:
    """Backfill built-in measure categories in existing settings rows."""
    def _canonicalize(value) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        normalized = re.sub(r"[\s_-]+", "", text.lower())
        mapping = {
            "manual": "Spores",
            "spore": "Spores",
            "spores": "Spores",
            "field": "Field",
            "basidia": "Basidia",
            "pileipellis": "Pileipellis",
            "pleurocystidia": "Pleurocystidia",
            "cheilocystidia": "Cheilocystidia",
            "caulocystidia": "Caulocystidia",
            "other": "Other",
        }
        return mapping.get(normalized, re.sub(r"\s+", "_", text))

    def _canonicalize_list(values: list) -> list[str]:
        cleaned: list[str] = []
        seen: set[str] = set()
        for value in values or []:
            canonical = _canonicalize(value)
            if not canonical or canonical in seen:
                continue
            seen.add(canonical)
            cleaned.append(canonical)
        return cleaned or list(_DEFAULT_MEASURE_CATEGORIES)

    key = "measure_categories"
    defaults = list(_DEFAULT_MEASURE_CATEGORIES)
    cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cursor.fetchone()

    if row is None or row[0] is None:
        normalized = _canonicalize_list(defaults)
        cursor.execute(
            """
            INSERT INTO settings (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, json.dumps(normalized)),
        )
        return

    raw_value = row[0]
    try:
        parsed = json.loads(raw_value)
    except (TypeError, json.JSONDecodeError):
        parsed = []
    if not isinstance(parsed, list):
        parsed = []

    normalized = _canonicalize_list(parsed)
    merged = list(normalized)
    changed = merged != parsed
    for default_category in defaults:
        if default_category not in merged:
            merged.append(default_category)
            changed = True

    if changed:
        cursor.execute(
            """
            INSERT INTO settings (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, json.dumps(merged)),
        )


def _migrate_contrast_options_setting(cursor: sqlite3.Cursor) -> None:
    """Backfill built-in contrast methods in existing settings rows."""

    def _canonicalize(value) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        normalized = re.sub(r"[\s_-]+", "", text.lower())
        mapping = {
            "notset": "Not_set",
            "bf": "BF",
            "brightfield": "BF",
            "df": "DF",
            "darkfield": "DF",
            "dic": "DIC",
            "differentialinterferencecontrast": "DIC",
            "oblique": "Oblique",
            "obliquelighting": "Oblique",
            "obliqueillumination": "Oblique",
            "phase": "Phase",
            "phasecontrast": "Phase",
            "hmc": "HMC",
            "hoffman": "HMC",
            "hoffmanmodulationcontrast": "HMC",
        }
        return mapping.get(normalized, re.sub(r"\s+", "_", text))

    def _canonicalize_list(values: list) -> list[str]:
        cleaned: list[str] = []
        seen: set[str] = set()
        for value in values or []:
            canonical = _canonicalize(value)
            if not canonical or canonical in seen:
                continue
            seen.add(canonical)
            cleaned.append(canonical)
        return cleaned or list(_DEFAULT_CONTRAST_METHODS)

    key = "contrast_options"
    defaults = list(_DEFAULT_CONTRAST_METHODS)
    cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cursor.fetchone()

    if row is None or row[0] is None:
        normalized = _canonicalize_list(defaults)
        cursor.execute(
            """
            INSERT INTO settings (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, json.dumps(normalized)),
        )
        return

    raw_value = row[0]
    try:
        parsed = json.loads(raw_value)
    except (TypeError, json.JSONDecodeError):
        parsed = []
    if not isinstance(parsed, list):
        parsed = []

    normalized = _canonicalize_list(parsed)
    merged = list(normalized)
    changed = merged != parsed
    for default_method in defaults:
        if default_method not in merged:
            merged.append(default_method)
            changed = True

    if changed:
        cursor.execute(
            """
            INSERT INTO settings (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, json.dumps(merged)),
        )


def _migrate_mount_and_stain_settings(cursor: sqlite3.Cursor) -> None:
    """Split stain values out of legacy mount settings and seed stain defaults."""

    def _load_list(key: str) -> list[str]:
        cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = cursor.fetchone()
        if row is None or row[0] is None:
            return []
        try:
            parsed = json.loads(row[0])
        except (TypeError, json.JSONDecodeError):
            parsed = []
        return parsed if isinstance(parsed, list) else []

    def _save_value(key: str, value) -> None:
        cursor.execute(
            """
            INSERT INTO settings (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, json.dumps(value) if isinstance(value, list) else value),
        )

    mount_values = _load_list("mount_options")
    stain_values = _load_list("stain_options")

    normalized_mounts = DatabaseTerms.canonicalize_list("mount", mount_values or _DEFAULT_MOUNT_MEDIA)
    normalized_stains = DatabaseTerms.canonicalize_list("stain", stain_values or _DEFAULT_STAIN_TYPES)

    moved_stains = []
    kept_mounts = []
    for value in normalized_mounts:
        canonical_stain = DatabaseTerms.canonicalize("stain", value)
        if canonical_stain in _DEFAULT_STAIN_TYPES[1:]:
            if canonical_stain not in moved_stains:
                moved_stains.append(canonical_stain)
        else:
            kept_mounts.append(value)

    merged_mounts = list(kept_mounts)
    for default_value in _DEFAULT_MOUNT_MEDIA:
        if default_value not in merged_mounts:
            merged_mounts.append(default_value)

    merged_stains = list(normalized_stains)
    for value in moved_stains:
        if value not in merged_stains:
            merged_stains.append(value)
    for default_value in _DEFAULT_STAIN_TYPES:
        if default_value not in merged_stains:
            merged_stains.append(default_value)

    _save_value("mount_options", merged_mounts)
    _save_value("stain_options", merged_stains)

    cursor.execute("SELECT value FROM settings WHERE key = ?", ("last_used_mount",))
    last_mount_row = cursor.fetchone()
    last_mount = last_mount_row[0] if last_mount_row else None
    canonical_stain = DatabaseTerms.canonicalize("stain", last_mount)
    if canonical_stain in _DEFAULT_STAIN_TYPES[1:]:
        _save_value("last_used_stain", canonical_stain)
        _save_value("last_used_mount", "Not_set")


def _migrate_images_mounts_and_stains(cursor: sqlite3.Cursor) -> None:
    """Move legacy stain-only image mount values into the dedicated stain column."""
    cursor.execute("SELECT id, mount_medium, stain FROM images")
    for row_id, mount_medium, stain in cursor.fetchall():
        if str(stain or "").strip():
            canonical_stain = DatabaseTerms.canonicalize("stain", stain)
            if canonical_stain and canonical_stain != stain:
                cursor.execute(
                    "UPDATE images SET stain = ? WHERE id = ?",
                    (canonical_stain, row_id),
                )
            continue
        canonical_stain = DatabaseTerms.canonicalize("stain", mount_medium)
        if canonical_stain in _DEFAULT_STAIN_TYPES[1:]:
            cursor.execute(
                "UPDATE images SET stain = ?, mount_medium = NULL WHERE id = ?",
                (canonical_stain, row_id),
            )


def _migrate_image_sort_order(cursor: sqlite3.Cursor) -> None:
    """Backfill and normalize per-observation image ordering."""
    cursor.execute("SELECT DISTINCT observation_id FROM images WHERE observation_id IS NOT NULL ORDER BY observation_id")
    observation_ids = [row[0] for row in cursor.fetchall() if row and row[0] is not None]
    for observation_id in observation_ids:
        cursor.execute(
            """
            SELECT id, sort_order, image_type, micro_category, created_at
            FROM images
            WHERE observation_id = ?
            ORDER BY
                CASE WHEN sort_order IS NULL THEN 1 ELSE 0 END,
                sort_order,
                image_type,
                micro_category,
                created_at,
                id
            """,
            (observation_id,),
        )
        rows = cursor.fetchall()
        for index, row in enumerate(rows):
            image_id = row[0]
            current_sort_order = row[1]
            if current_sort_order == index:
                continue
            cursor.execute(
                "UPDATE images SET sort_order = ? WHERE id = ?",
                (index, image_id),
            )

def init_database():
    """Initialize the database with required tables"""
    db_path = get_database_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Observations table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            location TEXT,
            habitat TEXT,
            genus TEXT,
            species TEXT,
            artsdata_id INTEGER,
            artportalen_id INTEGER,
            publish_target TEXT DEFAULT 'artsobs_no',
            common_name TEXT,
            species_guess TEXT,
            uncertain INTEGER DEFAULT 0,
            unspontaneous INTEGER DEFAULT 0,
            determination_method INTEGER,
            notes TEXT,
            inaturalist_id INTEGER,
            mushroomobserver_id INTEGER,
            folder_path TEXT,
            spore_statistics TEXT,
            auto_threshold REAL,
            author TEXT,
            source_type TEXT DEFAULT 'personal',
            citation TEXT,
            data_provider TEXT,
            habitat_nin2_path TEXT,
            habitat_substrate_path TEXT,
            habitat_host_genus TEXT,
            habitat_host_species TEXT,
            habitat_host_common_name TEXT,
            habitat_nin2_note TEXT,
            habitat_substrate_note TEXT,
            habitat_grows_on_note TEXT,
            open_comment TEXT,
            private_comment TEXT,
            interesting_comment INTEGER DEFAULT 0,
            ai_state_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Add spore_statistics column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN spore_statistics TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add auto_threshold column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN auto_threshold REAL')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add author column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN author TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add artsdata_id column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN artsdata_id INTEGER')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add Mushroom Observer ID column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN mushroomobserver_id INTEGER')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add Artportalen ID column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN artportalen_id INTEGER')
    except sqlite3.OperationalError:
        pass

    # Add publish target column if it doesn't exist
    try:
        cursor.execute("ALTER TABLE observations ADD COLUMN publish_target TEXT DEFAULT 'artsobs_no'")
    except sqlite3.OperationalError:
        pass

    # Remove legacy adb_taxon_id column if present.
    try:
        cursor.execute('ALTER TABLE observations DROP COLUMN adb_taxon_id')
    except sqlite3.OperationalError:
        pass

    # Add source tracking columns if they don't exist
    try:
        cursor.execute("ALTER TABLE observations ADD COLUMN source_type TEXT DEFAULT 'personal'")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN citation TEXT')
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN data_provider TEXT')
    except sqlite3.OperationalError:
        pass

    # Settings table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    _migrate_contrast_options_setting(cursor)
    _migrate_mount_and_stain_settings(cursor)
    _migrate_measure_categories_setting(cursor)

    # Add genus column if it doesn't exist (migration for existing DBs)
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN genus TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add species column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN species TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add common_name column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN common_name TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add uncertain column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN uncertain INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add unspontaneous column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN unspontaneous INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add determination_method column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN determination_method INTEGER')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add folder_path column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN folder_path TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add GPS latitude column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN gps_latitude REAL')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add GPS longitude column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN gps_longitude REAL')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add structured habitat columns if they don't exist
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN habitat_nin2_path TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN habitat_substrate_path TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN habitat_host_genus TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN habitat_host_species TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN habitat_host_common_name TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN habitat_nin2_note TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN habitat_substrate_note TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN habitat_grows_on_note TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN open_comment TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN private_comment TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN interesting_comment INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        pass  # Column already exists
    try:
        cursor.execute('ALTER TABLE observations ADD COLUMN ai_state_json TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Images table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observation_id INTEGER,
            filepath TEXT NOT NULL,
            original_filepath TEXT,
            sort_order INTEGER,
            image_type TEXT CHECK(image_type IN ('field', 'microscope')),
            micro_category TEXT,
            objective_name TEXT,
            scale_microns_per_pixel REAL,
            resample_scale_factor REAL,
            mount_medium TEXT,
            stain TEXT,
            sample_type TEXT,
            contrast TEXT,
            measure_color TEXT,
            notes TEXT,
            ai_crop_x1 REAL,
            ai_crop_y1 REAL,
            ai_crop_x2 REAL,
            ai_crop_y2 REAL,
            ai_crop_source_w INTEGER,
            ai_crop_source_h INTEGER,
            crop_mode TEXT,
            gps_source INTEGER DEFAULT 0,
            artsobs_web_unpublished INTEGER DEFAULT 0,
            scale_bar_x1 REAL,
            scale_bar_y1 REAL,
            scale_bar_x2 REAL,
            scale_bar_y2 REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (observation_id) REFERENCES observations(id)
        )
    ''')

    # Add micro_category column if it doesn't exist (migration for existing DBs)
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN micro_category TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add objective_name column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN objective_name TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add original_filepath column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN original_filepath TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists

    try:
        cursor.execute('ALTER TABLE images ADD COLUMN sort_order INTEGER')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add mount_medium column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN mount_medium TEXT')
    except sqlite3.OperationalError:
        pass

    # Add stain column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN stain TEXT')
    except sqlite3.OperationalError:
        pass

    # Add sample_type column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN sample_type TEXT')
    except sqlite3.OperationalError:
        pass

    # Add contrast column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN contrast TEXT')
    except sqlite3.OperationalError:
        pass

    # Add measure_color column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN measure_color TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add resample_scale_factor column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN resample_scale_factor REAL')
    except sqlite3.OperationalError:
        pass  # Column already exists

    _migrate_images_mounts_and_stains(cursor)
    _migrate_image_sort_order(cursor)

    # Add calibration_id column to link images to calibrations
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN calibration_id INTEGER')
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add AI crop columns for Artsorakelet
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN ai_crop_x1 REAL')
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN ai_crop_y1 REAL')
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN ai_crop_x2 REAL')
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN ai_crop_y2 REAL')
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN ai_crop_source_w INTEGER')
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN ai_crop_source_h INTEGER')
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN crop_mode TEXT')
    except sqlite3.OperationalError:
        pass

    # Add GPS source flag for observation metadata
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN gps_source INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        pass

    # Add pending Artsobs web upload flag for images
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN artsobs_web_unpublished INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        pass

    # Add stored scale bar endpoints for Prepare Images overlay restore
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN scale_bar_x1 REAL')
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN scale_bar_y1 REAL')
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN scale_bar_x2 REAL')
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN scale_bar_y2 REAL')
    except sqlite3.OperationalError:
        pass

    # Spore measurements table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS spore_measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            image_id INTEGER NOT NULL,
            length_um REAL NOT NULL,
            width_um REAL,
            measurement_type TEXT DEFAULT 'manual',
            gallery_rotation INTEGER DEFAULT 0,
            p1_x REAL,
            p1_y REAL,
            p2_x REAL,
            p2_y REAL,
            p3_x REAL,
            p3_y REAL,
            p4_x REAL,
            p4_y REAL,
            notes TEXT,
            measured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (image_id) REFERENCES images(id)
        )
    ''')

    # Add gallery_rotation column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE spore_measurements ADD COLUMN gallery_rotation INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        pass

    # Thumbnails for efficient loading and ML training
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS thumbnails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            image_id INTEGER NOT NULL,
            size_preset TEXT NOT NULL,
            filepath TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (image_id) REFERENCES images(id),
            UNIQUE(image_id, size_preset)
        )
    ''')

    # Spore annotations for ML training (bounding boxes + measurements)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS spore_annotations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            image_id INTEGER NOT NULL,
            measurement_id INTEGER,
            spore_number INTEGER,
            bbox_x INTEGER,
            bbox_y INTEGER,
            bbox_width INTEGER,
            bbox_height INTEGER,
            center_x REAL,
            center_y REAL,
            length_um REAL,
            width_um REAL,
            rotation_angle REAL,
            annotation_source TEXT DEFAULT 'manual',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (image_id) REFERENCES images(id),
            FOREIGN KEY (measurement_id) REFERENCES spore_measurements(id)
        )
    ''')

    # Calibrations table for storing objective calibration history
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS calibrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            objective_key TEXT NOT NULL,
            calibration_date TEXT NOT NULL,
            calibration_image_date TEXT,
            microns_per_pixel REAL NOT NULL,
            microns_per_pixel_std REAL,
            confidence_interval_low REAL,
            confidence_interval_high REAL,
            num_measurements INTEGER,
            measurements_json TEXT,
            image_filepath TEXT,
            camera TEXT,
            megapixels REAL,
            target_sampling_pct REAL,
            resample_scale_factor REAL,
            calibration_image_width INTEGER,
            calibration_image_height INTEGER,
            notes TEXT,
            is_active INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Add camera column if it doesn't exist (migration for existing DBs)
    try:
        cursor.execute('ALTER TABLE calibrations ADD COLUMN camera TEXT')
    except sqlite3.OperationalError:
        pass
    # Add megapixels column if it doesn't exist (migration for existing DBs)
    try:
        cursor.execute('ALTER TABLE calibrations ADD COLUMN megapixels REAL')
    except sqlite3.OperationalError:
        pass
    # Add target sampling percent column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE calibrations ADD COLUMN target_sampling_pct REAL')
    except sqlite3.OperationalError:
        pass
    # Add resample scale factor column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE calibrations ADD COLUMN resample_scale_factor REAL')
    except sqlite3.OperationalError:
        pass
    # Add calibration image dimensions if they don't exist
    try:
        cursor.execute('ALTER TABLE calibrations ADD COLUMN calibration_image_width INTEGER')
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute('ALTER TABLE calibrations ADD COLUMN calibration_image_height INTEGER')
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute('ALTER TABLE calibrations ADD COLUMN calibration_image_date TEXT')
    except sqlite3.OperationalError:
        pass

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_observations_species ON observations(genus, species)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_observations_source ON observations(source_type)')

    conn.commit()
    conn.close()

    init_reference_database()
    print(f"Database initialized at {db_path}")

if __name__ == "__main__":
    init_database()
