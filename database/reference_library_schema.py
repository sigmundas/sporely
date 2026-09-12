"""Normalized reference library schema (Stage 1).

This module owns the idempotent schema for the reusable bibliographic
reference library (``reference_works``, ``reference_taxon_treatments``,
``reference_measurement_sets``) which lives in ``reference_values.db``
alongside the legacy ``reference_values`` table, and for the
``observation_reference_uses`` link table which lives in the main
``mushrooms.db`` observation database.

SQLite cannot enforce a foreign key across two separate database files,
so the cross-database link (``observation_reference_uses`` →
``reference_measurement_sets``) is enforced by the repository/service
layer (see ``database.reference_library``).
"""
from __future__ import annotations

import sqlite3


# --- Allowed enum values -----------------------------------------------------

REFERENCE_WORK_TYPES: frozenset[str] = frozenset(
    {"book", "article", "chapter", "website", "dataset", "other"}
)

# Legacy compatibility constants. The application no longer manually
# assigns a verification status or a per-work visibility scope — public
# exposure of an attached reference is governed by the observation's
# visibility and its frozen attachment snapshot, and bibliographic
# completeness is derived from the record's fields at display time
# (see ``ui.reference_library_manager_dialog.reference_work_completeness_hints``).
# These frozensets are kept only so that legacy schema DDL and any
# still-existing raw-SQL callers do not break at import time.
REFERENCE_WORK_VERIFICATION_STATUSES: frozenset[str] = frozenset(
    {"incomplete", "unverified", "verified"}
)

REFERENCE_WORK_VISIBILITIES: frozenset[str] = frozenset(
    {"private", "shared", "curated_public"}
)

REFERENCE_MEASUREMENT_CHARACTERS: frozenset[str] = frozenset({"spore_size"})

REFERENCE_MEASUREMENT_DATA_KINDS: frozenset[str] = frozenset(
    {"range", "summary", "raw_points", "parmasto"}
)

OBSERVATION_REFERENCE_ROLES: frozenset[str] = frozenset(
    {"compared", "supports_identification", "contradicts"}
)


# --- DDL ---------------------------------------------------------------------

_REFERENCE_WORKS_DDL = """
CREATE TABLE IF NOT EXISTS reference_works (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    citation_key TEXT,
    authors_json TEXT NOT NULL DEFAULT '[]',
    editors_json TEXT NOT NULL DEFAULT '[]',
    title TEXT NOT NULL,
    container_title TEXT,
    year INTEGER,
    edition TEXT,
    publisher TEXT,
    place TEXT,
    volume TEXT,
    issue TEXT,
    pages TEXT,
    doi TEXT,
    isbn TEXT,
    url TEXT,
    language TEXT,
    short_label TEXT NOT NULL,
    citation_override TEXT,
    -- verification_status and visibility are retained on the DDL for
    -- backwards compatibility with sqlite files created before these
    -- concepts were dropped from the product. Application code no
    -- longer reads or writes either column; DB defaults preserve
    -- forward-compatibility for older callers doing raw SQL.
    verification_status TEXT NOT NULL DEFAULT 'incomplete',
    visibility TEXT NOT NULL DEFAULT 'private',
    owner_id TEXT,
    revision INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

_REFERENCE_TAXON_TREATMENTS_DDL = """
CREATE TABLE IF NOT EXISTS reference_taxon_treatments (
    id TEXT PRIMARY KEY,
    reference_work_id TEXT NOT NULL,
    taxon_id TEXT,
    name_as_published TEXT NOT NULL,
    page_from INTEGER,
    page_to INTEGER,
    locator_text TEXT,
    treatment_notes TEXT,
    revision INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (reference_work_id) REFERENCES reference_works(id)
        ON DELETE RESTRICT
)
"""

_REFERENCE_MEASUREMENT_SETS_DDL = """
CREATE TABLE IF NOT EXISTS reference_measurement_sets (
    id TEXT PRIMARY KEY,
    taxon_treatment_id TEXT NOT NULL,
    character TEXT NOT NULL,
    raw_text TEXT,
    data_kind TEXT NOT NULL,
    length_min REAL,
    length_core_min REAL,
    length_core_max REAL,
    length_max REAL,
    width_min REAL,
    width_core_min REAL,
    width_core_max REAL,
    width_max REAL,
    q_min REAL,
    q_max REAL,
    q_mean REAL,
    length_mean REAL,
    width_mean REAL,
    sample_size INTEGER,
    specimen_count INTEGER,
    mount_medium TEXT,
    stain TEXT,
    preparation TEXT,
    measurement_method TEXT,
    notes TEXT,
    raw_points_json TEXT,
    revision INTEGER NOT NULL DEFAULT 1,
    supersedes_id TEXT,
    legacy_reference_value_id INTEGER,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    measurement_details_json TEXT,
    q_core_min REAL,
    q_core_max REAL,
    FOREIGN KEY (taxon_treatment_id) REFERENCES reference_taxon_treatments(id)
        ON DELETE RESTRICT
)
"""

# --- Measurement content contract: extension columns and write barrier -------
#
# Frozen by ``docs/reference-data/measurement-content-contract.md`` (sections 1,
# 6 and 14). The three extension columns are nullable with no default: existing
# rows keep NULL, which the contract defines as "never examined". Nothing here
# reads, validates or rewrites their content; the typed rules live in
# ``references/measurement_content.py`` and are applied by writers.
#
# The barrier is a pair of BEFORE triggers whose body calls the
# application-defined SQL function ``sporely_measurement_contract()``. Only a
# contract-aware connection registers that function (``register_measurement_contract``).
# SQLite resolves functions when it prepares the DML statement, so a connection
# without it cannot compile any INSERT or UPDATE on ``reference_measurement_sets``
# (``OperationalError: no such function``) and nothing is written. This is the
# coarse unsupported-open policy accepted on 2026-09-11: after a contract-aware
# binary has opened a library, unaware older binaries can read it and DELETE
# measurement sets but cannot INSERT or UPDATE the table, legacy rows included.
# DELETE is deliberately outside the barrier. The WHEN clauses cost aware
# connections nothing on legacy rows and confine a registered-but-lower version
# to enhanced rows.

MEASUREMENT_CONTRACT_FUNCTION = "sporely_measurement_contract"
LOCAL_MEASUREMENT_CONTRACT_VERSION = 1
MEASUREMENT_CONTRACT_BARRIER_MESSAGE = "measurement content contract required"
MEASUREMENT_CONTENT_EXTENSION_COLUMNS: tuple[tuple[str, str], ...] = (
    ("measurement_details_json", "TEXT"),
    ("q_core_min", "REAL"),
    ("q_core_max", "REAL"),
)
MEASUREMENT_CONTENT_GUARD_TRIGGERS: tuple[str, str] = (
    "reference_measurement_content_guard_update",
    "reference_measurement_content_guard_insert",
)

_ENHANCED_OLD = (
    "OLD.measurement_details_json IS NOT NULL "
    "OR OLD.q_core_min IS NOT NULL OR OLD.q_core_max IS NOT NULL"
)
_ENHANCED_NEW = (
    "NEW.measurement_details_json IS NOT NULL "
    "OR NEW.q_core_min IS NOT NULL OR NEW.q_core_max IS NOT NULL"
)
_MEASUREMENT_CONTRACT_GUARD_BODY = f"""
BEGIN
    SELECT CASE WHEN coalesce({MEASUREMENT_CONTRACT_FUNCTION}(), 0)
                     < {LOCAL_MEASUREMENT_CONTRACT_VERSION}
        THEN RAISE(ABORT, '{MEASUREMENT_CONTRACT_BARRIER_MESSAGE}') END;
END
"""

_MEASUREMENT_CONTENT_GUARD_TRIGGERS_DDL = (
    f"""
    CREATE TRIGGER IF NOT EXISTS {MEASUREMENT_CONTENT_GUARD_TRIGGERS[0]}
    BEFORE UPDATE ON reference_measurement_sets
    FOR EACH ROW
    WHEN {_ENHANCED_OLD} OR {_ENHANCED_NEW}
    {_MEASUREMENT_CONTRACT_GUARD_BODY}
    """,
    f"""
    CREATE TRIGGER IF NOT EXISTS {MEASUREMENT_CONTENT_GUARD_TRIGGERS[1]}
    BEFORE INSERT ON reference_measurement_sets
    FOR EACH ROW
    WHEN {_ENHANCED_NEW} OR (
        NEW.supersedes_id IS NOT NULL AND EXISTS (
            SELECT 1 FROM reference_measurement_sets p
            WHERE p.id = NEW.supersedes_id
              AND (p.measurement_details_json IS NOT NULL
                   OR p.q_core_min IS NOT NULL OR p.q_core_max IS NOT NULL)
        )
    )
    {_MEASUREMENT_CONTRACT_GUARD_BODY}
    """,
)


def register_measurement_contract(
    conn: sqlite3.Connection,
    version: int = LOCAL_MEASUREMENT_CONTRACT_VERSION,
) -> None:
    """Mark ``conn`` as aware of the measurement content contract.

    This is the only mechanism by which a connection may INSERT or UPDATE
    ``reference_measurement_sets`` once the barrier exists. It is invoked by
    ``init_reference_library_schema`` and by every production connection
    factory listed in the contract's writer map; test or tool code that writes
    rows through a raw connection must call it explicitly.
    """
    conn.create_function(
        MEASUREMENT_CONTRACT_FUNCTION, 0, lambda: version, deterministic=True
    )


def _ensure_measurement_content_extension(conn: sqlite3.Connection) -> None:
    """Add the extension columns and the barrier triggers where absent.

    Additive and idempotent: ``ALTER TABLE … ADD COLUMN`` with no default
    leaves every existing row NULL in the new columns and rewrites nothing
    else; the triggers use ``IF NOT EXISTS``. Missing columns and triggers are
    applied in one transaction (unless the caller already holds one), so the
    columns never become visible without the barrier and a partial upgrade
    cannot be committed. ``init_reference_library_schema`` calls it inside the
    transaction that creates the table, and the CASCADE-era rebuild recreates
    the triggers inside its own transaction.
    """
    existing = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(reference_measurement_sets)").fetchall()
    }
    missing = [
        (name, sql_type)
        for name, sql_type in MEASUREMENT_CONTENT_EXTENSION_COLUMNS
        if name not in existing
    ]
    present_triggers = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=?",
            ("reference_measurement_sets",),
        ).fetchall()
    }
    if not missing and set(MEASUREMENT_CONTENT_GUARD_TRIGGERS) <= present_triggers:
        return
    owns_transaction = not conn.in_transaction
    if owns_transaction:
        conn.execute("BEGIN")
    try:
        for name, sql_type in missing:
            conn.execute(
                f"ALTER TABLE reference_measurement_sets ADD COLUMN {name} {sql_type}"
            )
        for statement in _MEASUREMENT_CONTENT_GUARD_TRIGGERS_DDL:
            conn.execute(statement)
        if owns_transaction:
            conn.commit()
    except Exception:
        if owns_transaction:
            conn.rollback()
        raise


_REFERENCE_MEASUREMENT_SET_PREFERENCES_DDL = """
CREATE TABLE IF NOT EXISTS reference_measurement_set_preferences (
    measurement_set_id TEXT PRIMARY KEY,
    is_favorite INTEGER NOT NULL DEFAULT 0 CHECK (is_favorite IN (0, 1)),
    recent_use_sequence INTEGER,
    FOREIGN KEY (measurement_set_id) REFERENCES reference_measurement_sets(id)
        ON DELETE CASCADE
)
"""

_CURATED_REFERENCE_FORKS_DDL = """
CREATE TABLE IF NOT EXISTS curated_reference_forks (
    curated_measurement_set_id TEXT NOT NULL,
    bundle_revision INTEGER NOT NULL CHECK (bundle_revision > 0),
    sporely_taxon_id INTEGER NOT NULL CHECK (
        sporely_taxon_id > 0 AND sporely_taxon_id <= 2147483647
    ),
    reference_work_id TEXT NOT NULL,
    taxon_treatment_id TEXT NOT NULL,
    reference_measurement_set_id TEXT NOT NULL,
    source_envelope_json TEXT NOT NULL,
    source_sha256 TEXT NOT NULL CHECK (
        length(source_sha256) = 64
        AND source_sha256 = lower(source_sha256)
        AND source_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (curated_measurement_set_id, bundle_revision),
    UNIQUE (reference_work_id),
    UNIQUE (taxon_treatment_id),
    UNIQUE (reference_measurement_set_id),
    FOREIGN KEY (reference_work_id) REFERENCES reference_works(id)
        ON DELETE CASCADE,
    FOREIGN KEY (taxon_treatment_id) REFERENCES reference_taxon_treatments(id)
        ON DELETE CASCADE,
    FOREIGN KEY (reference_measurement_set_id) REFERENCES reference_measurement_sets(id)
        ON DELETE CASCADE
)
"""

_CURATED_REFERENCE_FORK_CLOUD_SYNC_STATE_DDL = """
CREATE TABLE IF NOT EXISTS curated_reference_fork_cloud_sync_state (
    curated_measurement_set_id TEXT NOT NULL,
    bundle_revision INTEGER NOT NULL,
    cloud_user_id TEXT,
    cloud_row_version INTEGER CHECK (cloud_row_version IS NULL OR cloud_row_version >= 1),
    sync_status TEXT NOT NULL DEFAULT 'dirty' CHECK (sync_status IN ('dirty','clean','retry','conflict')),
    accepted_payload_json TEXT,
    last_error TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (curated_measurement_set_id, bundle_revision),
    FOREIGN KEY (curated_measurement_set_id, bundle_revision)
        REFERENCES curated_reference_forks(curated_measurement_set_id, bundle_revision)
        ON DELETE CASCADE
)
"""

_OBSERVATION_REFERENCE_USES_DDL = """
CREATE TABLE IF NOT EXISTS observation_reference_uses (
    id TEXT PRIMARY KEY,
    observation_id INTEGER NOT NULL,
    reference_measurement_set_id TEXT NOT NULL,
    role TEXT NOT NULL,
    note TEXT,
    selected_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reference_revision INTEGER NOT NULL,
    snapshot_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (observation_id) REFERENCES observations(id) ON DELETE CASCADE
)
"""

_REFERENCE_CLOUD_SYNC_STATE_DDL = """
CREATE TABLE IF NOT EXISTS reference_cloud_sync_state (
    entity_type TEXT NOT NULL CHECK (
        entity_type IN ('work', 'treatment', 'measurement_set')
    ),
    entity_id TEXT NOT NULL,
    cloud_user_id TEXT,
    remote_identity_state TEXT NOT NULL DEFAULT 'never_attempted' CHECK (
        remote_identity_state IN (
            'never_attempted', 'create_outcome_unknown', 'acknowledged'
        )
    ),
    cloud_row_version INTEGER CHECK (
        cloud_row_version IS NULL OR cloud_row_version >= 1
    ),
    accepted_payload_json TEXT,
    sync_status TEXT NOT NULL DEFAULT 'dirty' CHECK (
        sync_status IN ('dirty', 'clean', 'retry', 'conflict')
    ),
    conflict_json TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    last_error TEXT,
    last_attempted_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (entity_type, entity_id),
    CHECK (
        (remote_identity_state = 'never_attempted'
            AND cloud_row_version IS NULL
            AND accepted_payload_json IS NULL)
        OR (remote_identity_state = 'create_outcome_unknown'
            AND cloud_user_id IS NOT NULL
            AND TRIM(cloud_user_id) != ''
            AND cloud_row_version IS NULL
            AND accepted_payload_json IS NULL)
        OR (remote_identity_state = 'acknowledged'
            AND cloud_user_id IS NOT NULL
            AND TRIM(cloud_user_id) != ''
            AND cloud_row_version >= 1
            AND accepted_payload_json IS NOT NULL)
    )
)
"""

_REFERENCE_CLOUD_TOMBSTONES_DDL = """
CREATE TABLE IF NOT EXISTS reference_cloud_tombstones (
    entity_type TEXT NOT NULL CHECK (
        entity_type IN ('work', 'treatment', 'measurement_set')
    ),
    entity_id TEXT NOT NULL,
    cloud_user_id TEXT NOT NULL CHECK (TRIM(cloud_user_id) != ''),
    remote_identity_state TEXT NOT NULL CHECK (
        remote_identity_state IN ('create_outcome_unknown', 'acknowledged')
    ),
    expected_row_version INTEGER CHECK (
        expected_row_version IS NULL OR expected_row_version >= 1
    ),
    accepted_payload_json TEXT,
    reference_work_id TEXT,
    taxon_treatment_id TEXT,
    deleted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sync_status TEXT NOT NULL DEFAULT 'dirty' CHECK (
        sync_status IN ('dirty', 'retry', 'conflict')
    ),
    conflict_json TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    last_error TEXT,
    last_attempted_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (entity_type, entity_id),
    CHECK (
        (remote_identity_state = 'create_outcome_unknown'
            AND expected_row_version IS NULL
            AND accepted_payload_json IS NULL)
        OR (remote_identity_state = 'acknowledged'
            AND expected_row_version >= 1
            AND accepted_payload_json IS NOT NULL)
    )
)
"""

_REFERENCE_CLOUD_PULL_CURSORS_DDL = """
CREATE TABLE IF NOT EXISTS reference_cloud_pull_cursors (
    cloud_user_id TEXT NOT NULL CHECK (TRIM(cloud_user_id) != ''),
    entity_type TEXT NOT NULL CHECK (
        entity_type IN ('work', 'treatment', 'measurement_set')
    ),
    updated_at TEXT NOT NULL CHECK (TRIM(updated_at) != ''),
    entity_id TEXT NOT NULL CHECK (TRIM(entity_id) != ''),
    PRIMARY KEY (cloud_user_id, entity_type)
)
"""

_REFERENCE_CLOUD_REMOTE_TOMBSTONE_MARKERS_DDL = """
CREATE TABLE IF NOT EXISTS reference_cloud_remote_tombstone_markers (
    cloud_user_id TEXT NOT NULL CHECK (TRIM(cloud_user_id) != ''),
    entity_type TEXT NOT NULL CHECK (
        entity_type IN ('work', 'treatment', 'measurement_set')
    ),
    entity_id TEXT NOT NULL CHECK (TRIM(entity_id) != ''),
    cloud_row_version INTEGER NOT NULL CHECK (cloud_row_version >= 1),
    accepted_payload_json TEXT NOT NULL,
    deleted_at TEXT NOT NULL CHECK (TRIM(deleted_at) != ''),
    PRIMARY KEY (cloud_user_id, entity_type, entity_id)
)
"""

_OBSERVATION_REFERENCE_USE_CLOUD_SYNC_STATE_DDL = """
CREATE TABLE IF NOT EXISTS observation_reference_use_cloud_sync_state (
    use_id TEXT PRIMARY KEY,
    cloud_user_id TEXT,
    remote_identity_state TEXT NOT NULL DEFAULT 'never_attempted' CHECK (
        remote_identity_state IN (
            'never_attempted', 'create_outcome_unknown', 'acknowledged'
        )
    ),
    cloud_row_version INTEGER CHECK (
        cloud_row_version IS NULL OR cloud_row_version >= 1
    ),
    accepted_payload_json TEXT,
    sync_status TEXT NOT NULL DEFAULT 'dirty' CHECK (
        sync_status IN ('dirty', 'clean', 'retry', 'conflict')
    ),
    conflict_json TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    last_error TEXT,
    last_attempted_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (use_id) REFERENCES observation_reference_uses(id)
        ON DELETE CASCADE,
    CHECK (
        (remote_identity_state = 'never_attempted'
            AND cloud_row_version IS NULL
            AND accepted_payload_json IS NULL)
        OR (remote_identity_state = 'create_outcome_unknown'
            AND cloud_user_id IS NOT NULL
            AND TRIM(cloud_user_id) != ''
            AND cloud_row_version IS NULL
            AND accepted_payload_json IS NULL)
        OR (remote_identity_state = 'acknowledged'
            AND cloud_user_id IS NOT NULL
            AND TRIM(cloud_user_id) != ''
            AND cloud_row_version >= 1
            AND accepted_payload_json IS NOT NULL)
    )
)
"""

_OBSERVATION_REFERENCE_USE_CLOUD_TOMBSTONES_DDL = """
CREATE TABLE IF NOT EXISTS observation_reference_use_cloud_tombstones (
    use_id TEXT PRIMARY KEY,
    reference_measurement_set_id TEXT NOT NULL,
    local_observation_id INTEGER,
    observation_cloud_id TEXT NOT NULL CHECK (TRIM(observation_cloud_id) != ''),
    cloud_user_id TEXT NOT NULL CHECK (TRIM(cloud_user_id) != ''),
    remote_identity_state TEXT NOT NULL CHECK (
        remote_identity_state IN ('create_outcome_unknown', 'acknowledged')
    ),
    expected_row_version INTEGER CHECK (
        expected_row_version IS NULL OR expected_row_version >= 1
    ),
    accepted_payload_json TEXT,
    deleted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sync_status TEXT NOT NULL DEFAULT 'dirty' CHECK (
        sync_status IN ('dirty', 'retry', 'conflict')
    ),
    conflict_json TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    last_error TEXT,
    last_attempted_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (
        (remote_identity_state = 'create_outcome_unknown'
            AND expected_row_version IS NULL
            AND accepted_payload_json IS NULL)
        OR (remote_identity_state = 'acknowledged'
            AND expected_row_version >= 1
            AND accepted_payload_json IS NOT NULL)
    )
)
"""

_OBSERVATION_REFERENCE_USE_CLOUD_PULL_CURSORS_DDL = """
CREATE TABLE IF NOT EXISTS observation_reference_use_cloud_pull_cursors (
    cloud_user_id TEXT NOT NULL CHECK (TRIM(cloud_user_id) != ''),
    updated_at TEXT NOT NULL CHECK (TRIM(updated_at) != ''),
    use_id TEXT NOT NULL CHECK (TRIM(use_id) != ''),
    PRIMARY KEY (cloud_user_id)
)
"""

_OBSERVATION_REFERENCE_USE_CLOUD_REMOTE_TOMBSTONE_MARKERS_DDL = """
CREATE TABLE IF NOT EXISTS observation_reference_use_cloud_remote_tombstone_markers (
    cloud_user_id TEXT NOT NULL CHECK (TRIM(cloud_user_id) != ''),
    use_id TEXT NOT NULL CHECK (TRIM(use_id) != ''),
    cloud_row_version INTEGER NOT NULL CHECK (cloud_row_version >= 1),
    accepted_payload_json TEXT NOT NULL,
    deleted_at TEXT NOT NULL CHECK (TRIM(deleted_at) != ''),
    PRIMARY KEY (cloud_user_id, use_id)
)
"""


_REFERENCE_LIBRARY_INDEXES: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_reference_works_title ON reference_works(title)",
    "CREATE INDEX IF NOT EXISTS idx_reference_works_year ON reference_works(year)",
    (
        "CREATE INDEX IF NOT EXISTS idx_reference_works_doi_normalized "
        "ON reference_works(doi) "
        "WHERE doi IS NOT NULL AND TRIM(doi) != ''"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_reference_works_isbn_normalized "
        "ON reference_works(isbn) "
        "WHERE isbn IS NOT NULL AND TRIM(isbn) != ''"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_reference_taxon_treatments_work "
        "ON reference_taxon_treatments(reference_work_id)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_reference_taxon_treatments_taxon "
        "ON reference_taxon_treatments(taxon_id)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_reference_measurement_sets_treatment "
        "ON reference_measurement_sets(taxon_treatment_id)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_reference_measurement_sets_legacy "
        "ON reference_measurement_sets(legacy_reference_value_id) "
        "WHERE legacy_reference_value_id IS NOT NULL"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_curated_reference_forks_taxon "
        "ON curated_reference_forks(sporely_taxon_id, curated_measurement_set_id, bundle_revision)"
    ),
)


_OBSERVATION_REFERENCE_USES_INDEXES: tuple[str, ...] = (
    (
        "CREATE INDEX IF NOT EXISTS idx_observation_reference_uses_observation "
        "ON observation_reference_uses(observation_id)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_observation_reference_uses_set "
        "ON observation_reference_uses(reference_measurement_set_id)"
    ),
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS "
        "idx_observation_reference_uses_observation_set_unique "
        "ON observation_reference_uses(observation_id, reference_measurement_set_id)"
    ),
)

_REFERENCE_CLOUD_SYNC_INDEXES: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_reference_cloud_sync_pending "
    "ON reference_cloud_sync_state(cloud_user_id, sync_status, entity_type)",
    "CREATE INDEX IF NOT EXISTS idx_reference_cloud_tombstones_pending "
    "ON reference_cloud_tombstones(cloud_user_id, sync_status, entity_type)",
    "CREATE INDEX IF NOT EXISTS idx_reference_cloud_remote_tombstones_entity "
    "ON reference_cloud_remote_tombstone_markers(entity_type, entity_id)",
)

_OBSERVATION_REFERENCE_USE_CLOUD_SYNC_INDEXES: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_reference_use_cloud_sync_pending "
    "ON observation_reference_use_cloud_sync_state(cloud_user_id, sync_status)",
    "CREATE INDEX IF NOT EXISTS idx_reference_use_cloud_tombstones_pending "
    "ON observation_reference_use_cloud_tombstones(cloud_user_id, sync_status)",
    "CREATE INDEX IF NOT EXISTS idx_reference_use_remote_tombstones_use "
    "ON observation_reference_use_cloud_remote_tombstone_markers(use_id)",
)

_REFERENCE_CLOUD_SYNC_TRIGGERS: tuple[str, ...] = (
    """
    CREATE TRIGGER IF NOT EXISTS reference_work_cloud_sync_insert
    AFTER INSERT ON reference_works
    BEGIN
        INSERT OR IGNORE INTO reference_cloud_sync_state(entity_type, entity_id)
        VALUES ('work', NEW.id);
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS reference_treatment_cloud_sync_insert
    AFTER INSERT ON reference_taxon_treatments
    BEGIN
        INSERT OR IGNORE INTO reference_cloud_sync_state(entity_type, entity_id)
        VALUES ('treatment', NEW.id);
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS reference_measurement_set_cloud_sync_insert
    AFTER INSERT ON reference_measurement_sets
    BEGIN
        INSERT OR IGNORE INTO reference_cloud_sync_state(entity_type, entity_id)
        VALUES ('measurement_set', NEW.id);
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS reference_work_cloud_sync_delete
    BEFORE DELETE ON reference_works
    BEGIN
        INSERT OR IGNORE INTO reference_cloud_tombstones(
            entity_type, entity_id, cloud_user_id, remote_identity_state,
            expected_row_version, accepted_payload_json
        )
        SELECT entity_type, entity_id, cloud_user_id, remote_identity_state,
               cloud_row_version, accepted_payload_json
        FROM reference_cloud_sync_state
        WHERE entity_type = 'work' AND entity_id = OLD.id
          AND remote_identity_state != 'never_attempted';
        DELETE FROM reference_cloud_sync_state
        WHERE entity_type = 'work' AND entity_id = OLD.id;
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS reference_treatment_cloud_sync_delete
    BEFORE DELETE ON reference_taxon_treatments
    BEGIN
        INSERT OR IGNORE INTO reference_cloud_tombstones(
            entity_type, entity_id, cloud_user_id, remote_identity_state,
            expected_row_version, accepted_payload_json, reference_work_id
        )
        SELECT entity_type, entity_id, cloud_user_id, remote_identity_state,
               cloud_row_version, accepted_payload_json, OLD.reference_work_id
        FROM reference_cloud_sync_state
        WHERE entity_type = 'treatment' AND entity_id = OLD.id
          AND remote_identity_state != 'never_attempted';
        DELETE FROM reference_cloud_sync_state
        WHERE entity_type = 'treatment' AND entity_id = OLD.id;
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS reference_measurement_set_cloud_sync_delete
    BEFORE DELETE ON reference_measurement_sets
    BEGIN
        INSERT OR IGNORE INTO reference_cloud_tombstones(
            entity_type, entity_id, cloud_user_id, remote_identity_state,
            expected_row_version, accepted_payload_json, taxon_treatment_id
        )
        SELECT entity_type, entity_id, cloud_user_id, remote_identity_state,
               cloud_row_version, accepted_payload_json, OLD.taxon_treatment_id
        FROM reference_cloud_sync_state
        WHERE entity_type = 'measurement_set' AND entity_id = OLD.id
          AND remote_identity_state != 'never_attempted';
        DELETE FROM reference_cloud_sync_state
        WHERE entity_type = 'measurement_set' AND entity_id = OLD.id;
    END
    """,
)

_OBSERVATION_REFERENCE_USE_CLOUD_SYNC_TRIGGERS: tuple[str, ...] = (
    """
    CREATE TRIGGER IF NOT EXISTS reference_use_cloud_sync_insert
    AFTER INSERT ON observation_reference_uses
    BEGIN
        INSERT OR IGNORE INTO observation_reference_use_cloud_sync_state(use_id)
        VALUES (NEW.id);
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS reference_use_cloud_sync_delete
    BEFORE DELETE ON observation_reference_uses
    BEGIN
        INSERT INTO observation_reference_use_cloud_tombstones(
            use_id, reference_measurement_set_id, local_observation_id,
            observation_cloud_id, cloud_user_id, remote_identity_state,
            expected_row_version, accepted_payload_json
        )
        SELECT OLD.id, OLD.reference_measurement_set_id, OLD.observation_id,
               (SELECT NULLIF(TRIM(cloud_id), '') FROM observations
                WHERE id = OLD.observation_id),
               cloud_user_id, remote_identity_state, cloud_row_version,
               accepted_payload_json
        FROM observation_reference_use_cloud_sync_state
        WHERE use_id = OLD.id
          AND remote_identity_state != 'never_attempted'
          AND NOT EXISTS (
              SELECT 1 FROM observation_reference_use_cloud_tombstones
              WHERE use_id = OLD.id
          );
        DELETE FROM observation_reference_use_cloud_sync_state
        WHERE use_id = OLD.id;
    END
    """,
)

_OBSERVATION_REFERENCE_USE_PARENT_DELETE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS observation_reference_uses_cloud_sync_parent_delete
BEFORE DELETE ON observations
BEGIN
    INSERT INTO observation_reference_use_cloud_tombstones(
        use_id, reference_measurement_set_id, local_observation_id,
        observation_cloud_id, cloud_user_id, remote_identity_state,
        expected_row_version, accepted_payload_json
    )
    SELECT use_row.id, use_row.reference_measurement_set_id, OLD.id,
           NULLIF(TRIM(OLD.cloud_id), ''), state.cloud_user_id,
           state.remote_identity_state, state.cloud_row_version,
           state.accepted_payload_json
    FROM observation_reference_uses AS use_row
    JOIN observation_reference_use_cloud_sync_state AS state
      ON state.use_id = use_row.id
    WHERE use_row.observation_id = OLD.id
      AND state.remote_identity_state != 'never_attempted'
      AND NOT EXISTS (
          SELECT 1 FROM observation_reference_use_cloud_tombstones
          WHERE use_id = use_row.id
      );
END
"""


def _fk_on_delete_actions(
    conn: sqlite3.Connection, table: str
) -> dict[str, str]:
    """Return a mapping of ``referenced_table`` → uppercase ``on_delete``
    action for the given local table. Empty dict if the table does not
    exist yet.
    """
    result: dict[str, str] = {}
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone():
        return result
    for row in conn.execute(f"PRAGMA foreign_key_list({table})").fetchall():
        # PRAGMA columns: id, seq, table, from, to, on_update, on_delete, match
        referenced_table = row[2]
        on_delete = str(row[6] or "NO ACTION").upper()
        result[str(referenced_table)] = on_delete
    return result


def _rebuild_table_with_restrict_fks(
    conn: sqlite3.Connection,
    *,
    table: str,
    ddl: str,
    post_ddl: tuple[str, ...] = (),
) -> None:
    """Recreate ``table`` in place from ``ddl`` (which must use RESTRICT).

    Copies all existing rows over. Runs inside its own transaction with
    ``foreign_keys`` temporarily disabled, then verifies FK integrity
    before committing. Column set is derived from the existing table so
    additive column changes remain safe. ``post_ddl`` statements (for
    example triggers, which ``DROP TABLE`` removes) run on the rebuilt table
    inside the same transaction, so they are never committed separately.
    """
    cursor = conn.cursor()
    prev_fk = cursor.execute("PRAGMA foreign_keys").fetchone()[0]
    cursor.execute("PRAGMA foreign_keys = OFF")
    try:
        cursor.execute("BEGIN")
        columns = [
            row[1]
            for row in cursor.execute(f"PRAGMA table_info({table})").fetchall()
        ]
        tmp_table = f"__{table}_restrict_migration"
        cursor.execute(f"DROP TABLE IF EXISTS {tmp_table}")
        # Rewrite ddl to target the temporary table name.
        create_stmt = ddl.strip().replace(
            f"CREATE TABLE IF NOT EXISTS {table}",
            f"CREATE TABLE {tmp_table}",
            1,
        )
        cursor.execute(create_stmt)
        if columns:
            column_list = ", ".join(columns)
            cursor.execute(
                f"INSERT INTO {tmp_table} ({column_list}) "
                f"SELECT {column_list} FROM {table}"
            )
        cursor.execute(f"DROP TABLE {table}")
        cursor.execute(f"ALTER TABLE {tmp_table} RENAME TO {table}")
        for statement in post_ddl:
            cursor.execute(statement)
        fk_violations = cursor.execute("PRAGMA foreign_key_check").fetchall()
        if fk_violations:
            raise sqlite3.IntegrityError(
                f"foreign key violations after rebuilding {table}: {fk_violations}"
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.execute(f"PRAGMA foreign_keys = {'ON' if prev_fk else 'OFF'}")


def _ensure_restrict_foreign_keys(conn: sqlite3.Connection) -> None:
    """Upgrade legacy CASCADE FKs on the new library tables to RESTRICT.

    Existing installations that were created before the Stage 1 correction
    have ``ON DELETE CASCADE`` between ``reference_taxon_treatments`` →
    ``reference_works`` and ``reference_measurement_sets`` →
    ``reference_taxon_treatments``. Silent cascade is unsafe for a
    library shared across observations, so rebuild those tables to
    ``ON DELETE RESTRICT`` on next open. Idempotent: no-op once the
    tables are already correct.
    """
    treatments_actions = _fk_on_delete_actions(conn, "reference_taxon_treatments")
    if treatments_actions.get("reference_works") == "CASCADE":
        _rebuild_table_with_restrict_fks(
            conn,
            table="reference_taxon_treatments",
            ddl=_REFERENCE_TAXON_TREATMENTS_DDL,
        )
    sets_actions = _fk_on_delete_actions(conn, "reference_measurement_sets")
    if sets_actions.get("reference_taxon_treatments") == "CASCADE":
        _rebuild_table_with_restrict_fks(
            conn,
            table="reference_measurement_sets",
            ddl=_REFERENCE_MEASUREMENT_SETS_DDL,
            post_ddl=_MEASUREMENT_CONTENT_GUARD_TRIGGERS_DDL,
        )


def _normalized_index_ddl(sql: str) -> str:
    # SQLite stores CREATE INDEX statements without the "IF NOT EXISTS"
    # clause, so strip it before comparing a target DDL string to what is
    # actually persisted in ``sqlite_master``.
    return sql.strip().replace("IF NOT EXISTS ", "", 1)


def _existing_index_sql(conn: sqlite3.Connection, index_name: str) -> str | None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
        (index_name,),
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return str(row[0]).strip()


def init_reference_library_schema(conn: sqlite3.Connection) -> None:
    """Create the normalized reference library tables and indexes.

    Idempotent: safe to call on every application startup and after
    connecting to an existing (possibly legacy-only) reference database.
    The legacy ``reference_values`` table is not touched here.

    Registers the measurement content contract on ``conn`` first, so every
    connection initialized here may write ``reference_measurement_sets``
    (see ``register_measurement_contract``).
    """
    register_measurement_contract(conn)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()
    # The core tables, the measurement content extension and its write
    # barrier are created in one explicit transaction: DDL statements do not
    # open an implicit transaction in Python's sqlite3, and an unaware older
    # binary must never find committed extension columns without the guard
    # triggers (fresh library, legacy upgrade or interrupted start alike).
    owns_transaction = not conn.in_transaction
    if owns_transaction:
        cursor.execute("BEGIN")
    try:
        cursor.execute(_REFERENCE_WORKS_DDL)
        cursor.execute(_REFERENCE_TAXON_TREATMENTS_DDL)
        cursor.execute(_REFERENCE_MEASUREMENT_SETS_DDL)
        _ensure_measurement_content_extension(conn)
        cursor.execute(_REFERENCE_MEASUREMENT_SET_PREFERENCES_DDL)
        cursor.execute(_CURATED_REFERENCE_FORKS_DDL)
        cursor.execute(_CURATED_REFERENCE_FORK_CLOUD_SYNC_STATE_DDL)
        # Older Stage 1 databases used uniqueness here. Stage 6k must create a
        # fresh private graph for every explicitly copied curated revision, even
        # when immutable bibliographic identifiers repeat. Duplicate discovery
        # remains available through repository lookups; identity is never merged.
        # Only drop when the stored definition actually differs from the target
        # (e.g. the old unique variant) — an already-migrated index must not be
        # rewritten on every ordinary initialization call.
        for legacy_index_name, target_sql in (
            ("idx_reference_works_doi_normalized", _REFERENCE_LIBRARY_INDEXES[2]),
            ("idx_reference_works_isbn_normalized", _REFERENCE_LIBRARY_INDEXES[3]),
        ):
            existing_sql = _existing_index_sql(conn, legacy_index_name)
            if existing_sql is not None and existing_sql != _normalized_index_ddl(target_sql):
                cursor.execute(f"DROP INDEX IF EXISTS {legacy_index_name}")
        for statement in _REFERENCE_LIBRARY_INDEXES:
            cursor.execute(statement)
        if owns_transaction:
            conn.commit()
    except Exception:
        if owns_transaction:
            conn.rollback()
        raise
    # A CASCADE-era rebuild drops the table's triggers; it recreates the
    # guard triggers inside its own transaction (see _ensure_restrict_foreign_keys).
    _ensure_restrict_foreign_keys(conn)
    cursor.execute(_REFERENCE_CLOUD_SYNC_STATE_DDL)
    cursor.execute(_REFERENCE_CLOUD_TOMBSTONES_DDL)
    cursor.execute(_REFERENCE_CLOUD_PULL_CURSORS_DDL)
    cursor.execute(_REFERENCE_CLOUD_REMOTE_TOMBSTONE_MARKERS_DDL)
    for statement in _REFERENCE_CLOUD_SYNC_INDEXES:
        cursor.execute(statement)
    for statement in _REFERENCE_CLOUD_SYNC_TRIGGERS:
        cursor.execute(statement)
    cursor.execute(
        """
        CREATE TRIGGER IF NOT EXISTS curated_reference_fork_cloud_sync_insert
        AFTER INSERT ON curated_reference_forks
        BEGIN
            INSERT OR IGNORE INTO curated_reference_fork_cloud_sync_state(
                curated_measurement_set_id, bundle_revision
            ) VALUES (NEW.curated_measurement_set_id, NEW.bundle_revision);
        END
        """
    )
    cursor.execute(
        """
        CREATE TRIGGER IF NOT EXISTS curated_reference_fork_graph_guard
        BEFORE INSERT ON curated_reference_forks
        BEGIN
            SELECT CASE WHEN NOT EXISTS (
                SELECT 1 FROM reference_measurement_sets m
                JOIN reference_taxon_treatments t ON t.id=m.taxon_treatment_id
                WHERE m.id=NEW.reference_measurement_set_id
                  AND t.id=NEW.taxon_treatment_id
                  AND t.reference_work_id=NEW.reference_work_id
            ) THEN RAISE(ABORT, 'curated fork graph mismatch') END;
        END
        """
    )
    cursor.execute(
        """
        CREATE TRIGGER IF NOT EXISTS curated_reference_fork_immutable
        BEFORE UPDATE ON curated_reference_forks
        BEGIN
            SELECT RAISE(ABORT, 'curated fork provenance is immutable');
        END
        """
    )
    cursor.execute(
        """
        INSERT OR IGNORE INTO reference_cloud_sync_state(entity_type, entity_id)
        SELECT 'work', id FROM reference_works
        UNION ALL
        SELECT 'treatment', id FROM reference_taxon_treatments
        UNION ALL
        SELECT 'measurement_set', id FROM reference_measurement_sets
        """
    )
    cursor.execute(
        """
        INSERT OR IGNORE INTO curated_reference_fork_cloud_sync_state(
            curated_measurement_set_id, bundle_revision
        )
        SELECT curated_measurement_set_id, bundle_revision FROM curated_reference_forks
        """
    )
    conn.commit()


def init_observation_reference_uses_schema(conn: sqlite3.Connection) -> None:
    """Create the ``observation_reference_uses`` link table and indexes.

    Idempotent. The link's foreign key targets the local ``observations``
    table only; the cross-database reference to
    ``reference_measurement_sets`` (stored as a UUID text) is enforced by
    the repository layer.
    """
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()
    cursor.execute(_OBSERVATION_REFERENCE_USES_DDL)
    cursor.execute(_OBSERVATION_REFERENCE_USE_CLOUD_SYNC_STATE_DDL)
    cursor.execute(_OBSERVATION_REFERENCE_USE_CLOUD_TOMBSTONES_DDL)
    cursor.execute(_OBSERVATION_REFERENCE_USE_CLOUD_PULL_CURSORS_DDL)
    cursor.execute(_OBSERVATION_REFERENCE_USE_CLOUD_REMOTE_TOMBSTONE_MARKERS_DDL)
    for statement in _OBSERVATION_REFERENCE_USES_INDEXES:
        cursor.execute(statement)
    for statement in _OBSERVATION_REFERENCE_USE_CLOUD_SYNC_INDEXES:
        cursor.execute(statement)
    for statement in _OBSERVATION_REFERENCE_USE_CLOUD_SYNC_TRIGGERS:
        cursor.execute(statement)
    cursor.execute(
        """
        INSERT OR IGNORE INTO observation_reference_use_cloud_sync_state(use_id)
        SELECT id FROM observation_reference_uses
        """
    )
    observation_columns = {
        str(row[1])
        for row in cursor.execute("PRAGMA table_info(observations)").fetchall()
    }
    if "cloud_id" in observation_columns:
        cursor.execute(_OBSERVATION_REFERENCE_USE_PARENT_DELETE_TRIGGER)
    conn.commit()
