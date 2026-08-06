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
    FOREIGN KEY (taxon_treatment_id) REFERENCES reference_taxon_treatments(id)
        ON DELETE RESTRICT
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


_REFERENCE_LIBRARY_INDEXES: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_reference_works_title ON reference_works(title)",
    "CREATE INDEX IF NOT EXISTS idx_reference_works_year ON reference_works(year)",
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_reference_works_doi_normalized "
        "ON reference_works(doi) "
        "WHERE doi IS NOT NULL AND TRIM(doi) != ''"
    ),
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_reference_works_isbn_normalized "
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
) -> None:
    """Recreate ``table`` in place from ``ddl`` (which must use RESTRICT).

    Copies all existing rows over. Runs inside its own transaction with
    ``foreign_keys`` temporarily disabled, then verifies FK integrity
    before committing. Column set is derived from the existing table so
    additive column changes remain safe.
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
        )


def init_reference_library_schema(conn: sqlite3.Connection) -> None:
    """Create the normalized reference library tables and indexes.

    Idempotent: safe to call on every application startup and after
    connecting to an existing (possibly legacy-only) reference database.
    The legacy ``reference_values`` table is not touched here.
    """
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()
    cursor.execute(_REFERENCE_WORKS_DDL)
    cursor.execute(_REFERENCE_TAXON_TREATMENTS_DDL)
    cursor.execute(_REFERENCE_MEASUREMENT_SETS_DDL)
    for statement in _REFERENCE_LIBRARY_INDEXES:
        cursor.execute(statement)
    conn.commit()
    _ensure_restrict_foreign_keys(conn)


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
    for statement in _OBSERVATION_REFERENCE_USES_INDEXES:
        cursor.execute(statement)
    conn.commit()
