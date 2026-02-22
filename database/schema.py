"""Database schema and initialization"""
import json
import re
import sqlite3
from pathlib import Path
from platformdirs import user_data_dir

_app_dir = Path(user_data_dir("MycoLog", appauthor=False, roaming=True))
DATABASE_PATH = _app_dir / "mushrooms.db"
REFERENCE_DATABASE_PATH = _app_dir / "reference_values.db"
SETTINGS_PATH = _app_dir / "app_settings.json"

DEFAULT_OBJECTIVES = {
    "100X": {
        "magnification": 100.0,
        "na": 1.25,
        "objective_name": "Plan acrho",
        "name": "100X/1.25 Plan acrho",
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
) -> str:
    mag_text = _format_objective_number(magnification, decimals=2)
    na_text = _format_objective_number(numerical_aperture, decimals=2)
    base = ""
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


def _upgrade_objective_entry(key: str, obj: dict) -> tuple[dict, bool]:
    updated = False
    entry = dict(obj) if isinstance(obj, dict) else {}
    magnification = entry.get("magnification")
    na_value = entry.get("na") or entry.get("numerical_aperture")
    objective_name = entry.get("objective_name")
    legacy_name = entry.get("name")

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

    display_name = format_objective_display(
        entry.get("magnification"),
        entry.get("na"),
        entry.get("objective_name"),
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
    objective_name = obj.get("objective_name")
    if objective_name:
        display = format_objective_display(obj.get("magnification"), obj.get("na"), objective_name)
        if display:
            return display
    name = obj.get("name")
    if name:
        return str(name)
    return str(fallback_key) if fallback_key is not None else ""


def objective_sort_value(obj: dict, fallback_key: str | None = None) -> float:
    if isinstance(obj, dict):
        mag = obj.get("magnification")
        parsed = _parse_magnification(mag)
        if parsed is not None:
            return parsed
    parsed = _parse_magnification(fallback_key)
    return parsed if parsed is not None else 9999.0


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

def init_reference_database():
    """Initialize the reference values database."""
    ref_path = get_reference_database_path()
    ref_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(ref_path)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reference_values (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            genus TEXT NOT NULL,
            species TEXT NOT NULL,
            source TEXT,
            mount_medium TEXT,
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

    _ensure_reference_columns()
    _migrate_reference_values()

def _ensure_reference_columns():
    """Ensure new percentile columns exist in the reference values table."""
    conn = sqlite3.connect(get_reference_database_path())
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(reference_values)")
    existing = {row[1] for row in cursor.fetchall()}
    to_add = {
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

def _migrate_reference_values():
    """Copy legacy reference values from the main database if needed."""
    ref_conn = sqlite3.connect(get_reference_database_path())
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

    if has_p05 or has_p50 or has_p95 or has_wp05 or has_wp50 or has_wp95 or has_qp50:
        main_cursor.execute('''
            SELECT genus, species, source, mount_medium,
                   length_min, length_p05, length_p50, length_p95, length_max, length_avg,
                   width_min, width_p05, width_p50, width_p95, width_max, width_avg,
                   q_min, q_p50, q_max, q_avg, updated_at
            FROM reference_values
        ''')
    else:
        main_cursor.execute('''
            SELECT genus, species, source, mount_medium,
                   length_min, NULL, NULL, NULL, length_max, length_avg,
                   width_min, NULL, NULL, NULL, width_max, width_avg,
                   q_min, NULL, q_max, q_avg, updated_at
            FROM reference_values
        ''')
    rows = main_cursor.fetchall()
    main_conn.close()

    if not rows:
        return

    ref_conn = sqlite3.connect(get_reference_database_path())
    ref_cursor = ref_conn.cursor()
    ref_cursor.executemany('''
        INSERT INTO reference_values (
            genus, species, source, mount_medium,
            length_min, length_p05, length_p50, length_p95, length_max, length_avg,
            width_min, width_p05, width_p50, width_p95, width_max, width_avg,
            q_min, q_p50, q_max, q_avg, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', rows)
    ref_conn.commit()
    ref_conn.close()


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

    # Images table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observation_id INTEGER,
            filepath TEXT NOT NULL,
            original_filepath TEXT,
            image_type TEXT CHECK(image_type IN ('field', 'microscope')),
            micro_category TEXT,
            objective_name TEXT,
            scale_microns_per_pixel REAL,
            resample_scale_factor REAL,
            mount_medium TEXT,
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
            gps_source INTEGER DEFAULT 0,
            artsobs_web_unpublished INTEGER DEFAULT 0,
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

    # Add mount_medium column if it doesn't exist
    try:
        cursor.execute('ALTER TABLE images ADD COLUMN mount_medium TEXT')
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

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_observations_species ON observations(genus, species)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_observations_source ON observations(source_type)')

    conn.commit()
    conn.close()

    init_reference_database()
    print(f"Database initialized at {db_path}")

if __name__ == "__main__":
    init_database()
