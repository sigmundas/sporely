"""Database migration script to update schema."""
import json
import sqlite3
import shutil
from database.schema import get_database_path, load_objectives, resolve_objective_key

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
    "BF",
    "DF",
    "DIC",
    "Phase",
    "HMC",
]


def backup_database():
    """Create a backup of the database."""
    database_path = get_database_path()
    if database_path.exists():
        backup_path = database_path.with_suffix('.db.backup')
        shutil.copy2(database_path, backup_path)
        print(f"Backup created at: {backup_path}")
        return True
    return False


def _migrate_measure_categories_setting(cursor: sqlite3.Cursor) -> None:
    def _canonicalize(value) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        normalized = "".join(ch for ch in text.lower() if ch.isalnum())
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
        return mapping.get(normalized, "_".join(text.split()))

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
    def _canonicalize(value) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        normalized = "".join(ch for ch in text.lower() if ch.isalnum())
        mapping = {
            "bf": "BF",
            "brightfield": "BF",
            "df": "DF",
            "darkfield": "DF",
            "dic": "DIC",
            "differentialinterferencecontrast": "DIC",
            "phase": "Phase",
            "phasecontrast": "Phase",
            "hmc": "HMC",
            "hoffman": "HMC",
            "hoffmanmodulationcontrast": "HMC",
        }
        return mapping.get(normalized, "_".join(text.split()))

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


def migrate_database():
    """Migrate old database schema to new schema."""
    database_path = get_database_path()
    if not database_path.exists():
        print("No database found - will create new one")
        return

    # Backup first
    backup_database()

    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # Check if old schema exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='spore_measurements'")
    if cursor.fetchone():
        # Check if it has the old schema
        cursor.execute("PRAGMA table_info(spore_measurements)")
        columns = [col[1] for col in cursor.fetchall()]

        if 'image_path' in columns and 'image_id' not in columns:
            print("Found old schema with 'image_path' - migrating to new schema...")

            # Create new tables
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS observations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    location TEXT,
                    habitat TEXT,
                    species_guess TEXT,
                    notes TEXT,
                    inaturalist_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS images_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    observation_id INTEGER,
                    filepath TEXT NOT NULL,
                    image_type TEXT CHECK(image_type IN ('field', 'microscope')),
                    objective_name TEXT,
                    scale_microns_per_pixel REAL,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (observation_id) REFERENCES observations(id)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS spore_measurements_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    image_id INTEGER NOT NULL,
                    length_um REAL NOT NULL,
                    width_um REAL,
                    measurement_type TEXT DEFAULT 'manual',
                    notes TEXT,
                    measured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (image_id) REFERENCES images_new(id)
                )
            ''')

            # Migrate data
            # Get all old measurements with their image paths
            cursor.execute('SELECT image_path, length_um, width_um, scale, timestamp FROM spore_measurements')
            old_measurements = cursor.fetchall()

            # Group by image_path
            images_map = {}
            for image_path, length_um, width_um, scale, timestamp in old_measurements:
                if image_path not in images_map:
                    # Create image record
                    cursor.execute('''
                        INSERT INTO images_new (filepath, image_type, scale_microns_per_pixel, created_at)
                        VALUES (?, 'microscope', ?, ?)
                    ''', (image_path, scale, timestamp))
                    images_map[image_path] = cursor.lastrowid

                # Insert measurement
                image_id = images_map[image_path]
                cursor.execute('''
                    INSERT INTO spore_measurements_new (image_id, length_um, width_um, measurement_type, measured_at)
                    VALUES (?, ?, ?, 'manual', ?)
                ''', (image_id, length_um, width_um, timestamp))

            # Drop old tables and rename new ones
            cursor.execute('DROP TABLE IF EXISTS spore_measurements')
            cursor.execute('DROP TABLE IF EXISTS images')
            cursor.execute('ALTER TABLE spore_measurements_new RENAME TO spore_measurements')
            cursor.execute('ALTER TABLE images_new RENAME TO images')

            print(f"Migration complete! Migrated {len(old_measurements)} measurements from {len(images_map)} images")
        else:
            print("Database already has new schema - no migration needed")
    else:
        print("No spore_measurements table found - will create new schema")

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='observations'")
    if cursor.fetchone():
        cursor.execute("PRAGMA table_info(observations)")
        columns = {col[1] for col in cursor.fetchall()}
        if "source_type" not in columns:
            cursor.execute("ALTER TABLE observations ADD COLUMN source_type TEXT DEFAULT 'personal'")
        if "citation" not in columns:
            cursor.execute("ALTER TABLE observations ADD COLUMN citation TEXT")
        if "data_provider" not in columns:
            cursor.execute("ALTER TABLE observations ADD COLUMN data_provider TEXT")
        if "unspontaneous" not in columns:
            cursor.execute("ALTER TABLE observations ADD COLUMN unspontaneous INTEGER DEFAULT 0")
        if "determination_method" not in columns:
            cursor.execute("ALTER TABLE observations ADD COLUMN determination_method INTEGER")
        if "mushroomobserver_id" not in columns:
            cursor.execute("ALTER TABLE observations ADD COLUMN mushroomobserver_id INTEGER")
        if "open_comment" not in columns:
            cursor.execute("ALTER TABLE observations ADD COLUMN open_comment TEXT")
        if "private_comment" not in columns:
            cursor.execute("ALTER TABLE observations ADD COLUMN private_comment TEXT")
        if "interesting_comment" not in columns:
            cursor.execute("ALTER TABLE observations ADD COLUMN interesting_comment INTEGER DEFAULT 0")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_observations_species ON observations(genus, species)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_observations_source ON observations(source_type)")

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='images'")
    if cursor.fetchone():
        cursor.execute("PRAGMA table_info(images)")
        columns = {col[1] for col in cursor.fetchall()}
        if "artsobs_web_unpublished" not in columns:
            cursor.execute("ALTER TABLE images ADD COLUMN artsobs_web_unpublished INTEGER DEFAULT 0")
        if "scale_bar_x1" not in columns:
            cursor.execute("ALTER TABLE images ADD COLUMN scale_bar_x1 REAL")
            cursor.execute("ALTER TABLE images ADD COLUMN scale_bar_y1 REAL")
            cursor.execute("ALTER TABLE images ADD COLUMN scale_bar_x2 REAL")
            cursor.execute("ALTER TABLE images ADD COLUMN scale_bar_y2 REAL")

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='calibrations'")
    if cursor.fetchone():
        cursor.execute("PRAGMA table_info(calibrations)")
        columns = {col[1] for col in cursor.fetchall()}
        if "camera" not in columns:
            cursor.execute("ALTER TABLE calibrations ADD COLUMN camera TEXT")
        if "megapixels" not in columns:
            cursor.execute("ALTER TABLE calibrations ADD COLUMN megapixels REAL")
        # Normalize objective_name values to current objective keys when possible
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='images'")
        if cursor.fetchone():
            objectives = load_objectives()
            cursor.execute("SELECT id, objective_name FROM images WHERE objective_name IS NOT NULL")
            for image_id, objective_name in cursor.fetchall():
                resolved = resolve_objective_key(objective_name, objectives)
                if resolved and resolved != objective_name:
                    cursor.execute(
                        "UPDATE images SET objective_name = ? WHERE id = ?",
                        (resolved, image_id),
                    )
        # Backfill calibration_id for images missing or pointing to non-existent calibrations
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='images'")
        if cursor.fetchone():
            cursor.execute("SELECT COUNT(*) FROM images")
            total_images = cursor.fetchone()[0]
            if total_images:
                cursor.execute("""
                    UPDATE images
                    SET calibration_id = (
                        SELECT c.id
                        FROM calibrations c
                        WHERE c.objective_key = images.objective_name
                          AND c.is_active = 1
                        ORDER BY c.calibration_date DESC
                        LIMIT 1
                    )
                    WHERE objective_name IS NOT NULL
                      AND TRIM(objective_name) != ''
                      AND LOWER(objective_name) != 'custom'
                      AND (
                        calibration_id IS NULL
                        OR calibration_id NOT IN (SELECT id FROM calibrations)
                      )
                """)

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    _migrate_contrast_options_setting(cursor)
    _migrate_measure_categories_setting(cursor)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    migrate_database()
