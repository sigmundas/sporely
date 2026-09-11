"""Stage 1 spike: enforceable local write barrier for enhanced measurement rows.

Blocker 2 of ``docs/plans/active/2026-09-10-reported-statistics-and-range-semantics.md``.
The barrier is a pair of SQLite triggers on ``reference_measurement_sets`` whose
body calls an application-defined function, ``sporely_measurement_contract()``.
A contract-aware connection registers that function; an unaware (older binary)
connection does not. SQLite resolves trigger functions when it prepares the
DML statement, so an unaware connection cannot even compile an INSERT or
UPDATE against the table: the statement fails with ``no such function`` and
nothing is written. See the contract document, section "Local write barrier".

This module is prototype code. It adds the proposed columns and triggers to a
temporary database *after* the real ``init_reference_library_schema`` and
does not change any production schema. Stage 3 moves ``install_spike_barrier``
into the schema owner and ``register_measurement_contract`` into the
connection factories listed in the contract's writer map.
"""
from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path

import pytest

from database import reference_library_schema as lib_schema
from utils.archive.portable_import import _merge_reference_entity
from utils.db_share import _upsert_library_row_by_revision

FIXTURES = Path(__file__).parent / "fixtures" / "reference_statistics"

CONTRACT_FUNCTION = "sporely_measurement_contract"
LOCAL_CONTRACT_VERSION = 1
BARRIER_MESSAGE = "measurement content contract required"

EXTENSION_COLUMNS_DDL = (
    "ALTER TABLE reference_measurement_sets ADD COLUMN measurement_details_json TEXT",
    "ALTER TABLE reference_measurement_sets ADD COLUMN q_core_min REAL",
    "ALTER TABLE reference_measurement_sets ADD COLUMN q_core_max REAL",
)

_ENHANCED_OLD = (
    "OLD.measurement_details_json IS NOT NULL OR OLD.q_core_min IS NOT NULL OR OLD.q_core_max IS NOT NULL"
)
_ENHANCED_NEW = (
    "NEW.measurement_details_json IS NOT NULL OR NEW.q_core_min IS NOT NULL OR NEW.q_core_max IS NOT NULL"
)
_GUARD_BODY = f"""
BEGIN
    SELECT CASE WHEN coalesce({CONTRACT_FUNCTION}(), 0) < {LOCAL_CONTRACT_VERSION}
        THEN RAISE(ABORT, '{BARRIER_MESSAGE}') END;
END
"""

BARRIER_TRIGGERS_DDL = (
    f"""
    CREATE TRIGGER IF NOT EXISTS reference_measurement_content_guard_update
    BEFORE UPDATE ON reference_measurement_sets
    FOR EACH ROW
    WHEN {_ENHANCED_OLD} OR {_ENHANCED_NEW}
    {_GUARD_BODY}
    """,
    f"""
    CREATE TRIGGER IF NOT EXISTS reference_measurement_content_guard_insert
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
    {_GUARD_BODY}
    """,
)

WORK_ID = "0f3e7c2a-5b1d-4e9f-a8c7-6d5e4f3a2b1c"
TREATMENT_ID = "2a9e6b1c-7d4f-4a3e-8c5b-1f0e9d8c7b6a"


def register_measurement_contract(conn: sqlite3.Connection, version: int = LOCAL_CONTRACT_VERSION) -> None:
    """Mark ``conn`` as contract-aware (the only awareness mechanism)."""
    conn.create_function(CONTRACT_FUNCTION, 0, lambda: version, deterministic=True)


def install_spike_barrier(conn: sqlite3.Connection) -> None:
    columns = {row[1] for row in conn.execute("PRAGMA table_info(reference_measurement_sets)")}
    for ddl in EXTENSION_COLUMNS_DDL:
        if ddl.rsplit(" ", 2)[-2] not in columns:
            conn.execute(ddl)
    for ddl in BARRIER_TRIGGERS_DDL:
        conn.execute(ddl)
    conn.commit()


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _insert_row(conn: sqlite3.Connection, row: dict) -> None:
    columns = [key for key in row if key not in {"created_at", "updated_at"}]
    conn.execute(
        f"INSERT INTO reference_measurement_sets ({', '.join(columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)})",
        [row[key] for key in columns],
    )


def _seed(conn: sqlite3.Connection) -> None:
    conn.execute(
        "INSERT INTO reference_works (id, type, title, short_label) VALUES (?, 'book', 'Fixture', 'Fixture 2020')",
        (WORK_ID,),
    )
    conn.execute(
        "INSERT INTO reference_taxon_treatments (id, reference_work_id, name_as_published) VALUES (?, ?, 'Hebeloma fixtura')",
        (TREATMENT_ID, WORK_ID),
    )
    _insert_row(conn, _load("row_legacy_only.json"))
    _insert_row(conn, _load("row_enhanced.json"))
    conn.commit()


@pytest.fixture
def library(tmp_path):
    """A real normalized library with the spike barrier installed and rows seeded."""
    path = tmp_path / "reference_values.db"
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    lib_schema.init_reference_library_schema(conn)
    register_measurement_contract(conn)
    install_spike_barrier(conn)
    _seed(conn)
    conn.close()
    return path


def unaware(path: Path) -> sqlite3.Connection:
    """A connection exactly as an older desktop binary would open it."""
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def aware(path: Path) -> sqlite3.Connection:
    conn = unaware(path)
    register_measurement_contract(conn)
    return conn


def _row(conn: sqlite3.Connection, set_id: str) -> dict:
    return dict(conn.execute("SELECT * FROM reference_measurement_sets WHERE id=?", (set_id,)).fetchone())


def _triggers(conn: sqlite3.Connection) -> set[str]:
    return {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='reference_measurement_sets'"
        )
    }


ENHANCED = _load("row_enhanced.json")
LEGACY = _load("row_legacy_only.json")
ENHANCED_ID = ENHANCED["id"]
LEGACY_ID = LEGACY["id"]


# --- (a) unaware content writes on an enhanced row fail closed ----------------


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE reference_measurement_sets SET length_core_max = 15.0, revision = revision + 1 WHERE id = :id",
        "UPDATE reference_measurement_sets SET raw_text = 'edited' WHERE id = :id",
        "UPDATE reference_measurement_sets SET legacy_reference_value_id = 42 WHERE id = :id",
    ],
    ids=["bound-edit", "raw-text-edit", "legacy-id-enrichment"],
)
def test_unaware_update_of_enhanced_row_fails_closed(library, sql):
    before = _row(aware(library), ENHANCED_ID)
    conn = unaware(library)
    with pytest.raises(sqlite3.OperationalError, match=f"no such function: {CONTRACT_FUNCTION}"):
        conn.execute(sql, {"id": ENHANCED_ID})
    conn.rollback()
    assert _row(conn, ENHANCED_ID) == before
    conn.close()


def test_unaware_update_of_legacy_row_also_fails_this_is_the_unsupported_open_policy(library):
    """Function resolution happens at prepare time, before the WHEN clause is
    evaluated, so the barrier is coarse: an unaware binary cannot UPDATE any
    row of the table once the barrier is installed. The contract records this
    as the unsupported-open policy rather than hiding it; human review accepted
    it on 2026-09-11 as final (no finer row-dependent compatibility)."""
    conn = unaware(library)
    with pytest.raises(sqlite3.OperationalError, match="no such function"):
        conn.execute("UPDATE reference_measurement_sets SET raw_text='x' WHERE id=?", (LEGACY_ID,))
    conn.rollback()
    assert _row(conn, LEGACY_ID)["raw_text"] == LEGACY["raw_text"]
    conn.close()


def test_unaware_reads_and_startup_initialization_still_work(library):
    conn = unaware(library)
    rows = conn.execute("SELECT id, length_core_max FROM reference_measurement_sets ORDER BY id").fetchall()
    assert {row["id"] for row in rows} == {ENHANCED_ID, LEGACY_ID}
    # The older binary's idempotent startup path must not break on an enhanced library.
    lib_schema.init_reference_library_schema(conn)
    assert _triggers(conn) >= {
        "reference_measurement_content_guard_update",
        "reference_measurement_content_guard_insert",
    }
    assert _row(conn, ENHANCED_ID)["measurement_details_json"] == ENHANCED["measurement_details_json"]
    conn.close()


def test_unaware_delete_is_permitted_as_a_lifecycle_operation(library):
    """Deletion removes the whole record visibly; it never leaves legacy content
    standing without its extension. The contract keeps DELETE outside the barrier."""
    conn = unaware(library)
    conn.execute("DELETE FROM reference_measurement_sets WHERE id=?", (ENHANCED_ID,))
    conn.commit()
    assert conn.execute("SELECT COUNT(*) FROM reference_measurement_sets").fetchone()[0] == 1
    conn.close()


# --- (b) contract-aware connections succeed ---------------------------------


def test_aware_connection_can_edit_clear_and_supersede(library):
    conn = aware(library)
    conn.execute(
        "UPDATE reference_measurement_sets SET length_core_max=15.0, revision=revision+1 WHERE id=?",
        (ENHANCED_ID,),
    )
    assert _row(conn, ENHANCED_ID)["length_core_max"] == 15.0
    assert _row(conn, ENHANCED_ID)["measurement_details_json"] == ENHANCED["measurement_details_json"]

    successor = {**ENHANCED, "id": "c0ffee00-0000-4000-8000-000000000001", "supersedes_id": ENHANCED_ID, "revision": 3}
    _insert_row(conn, successor)
    assert _row(conn, successor["id"])["q_core_min"] == 1.36

    conn.execute(
        "UPDATE reference_measurement_sets SET measurement_details_json=NULL, q_core_min=NULL, q_core_max=NULL WHERE id=?",
        (ENHANCED_ID,),
    )
    cleared = _row(conn, ENHANCED_ID)
    assert cleared["measurement_details_json"] is None and cleared["q_core_min"] is None
    conn.commit()
    conn.close()


def test_aware_connection_declaring_a_lower_contract_version_is_rejected_by_the_trigger_body(library):
    conn = unaware(library)
    register_measurement_contract(conn, version=0)
    with pytest.raises(sqlite3.IntegrityError, match=BARRIER_MESSAGE):
        conn.execute("UPDATE reference_measurement_sets SET raw_text='x' WHERE id=?", (ENHANCED_ID,))
    conn.rollback()
    # Legacy rows are outside the WHEN clause, so a version-0 connection can still edit them.
    conn.execute("UPDATE reference_measurement_sets SET raw_text='legacy edit' WHERE id=?", (LEGACY_ID,))
    assert _row(conn, LEGACY_ID)["raw_text"] == "legacy edit"
    conn.close()


def test_aware_upsert_mirrors_pull_write_domain(library):
    conn = aware(library)
    conn.execute(
        "INSERT INTO reference_measurement_sets (id, taxon_treatment_id, character, data_kind, raw_text) "
        "VALUES (?, ?, 'spore_size', 'range', 'remote') "
        "ON CONFLICT(id) DO UPDATE SET raw_text=excluded.raw_text",
        (ENHANCED_ID, TREATMENT_ID),
    )
    assert _row(conn, ENHANCED_ID)["raw_text"] == "remote"
    assert _row(conn, ENHANCED_ID)["q_core_max"] == 2.19
    conn.close()


# --- (c) the barrier is not bypassed ----------------------------------------


def test_unaware_cannot_bypass_by_clearing_extension_fields(library):
    conn = unaware(library)
    with pytest.raises(sqlite3.OperationalError, match="no such function"):
        conn.execute(
            "UPDATE reference_measurement_sets SET measurement_details_json=NULL, q_core_min=NULL, q_core_max=NULL, "
            "length_core_max=15.0 WHERE id=?",
            (ENHANCED_ID,),
        )
    conn.rollback()
    assert _row(conn, ENHANCED_ID)["measurement_details_json"] == ENHANCED["measurement_details_json"]
    conn.close()


def test_unaware_upsert_as_used_by_pull_reconciliation_fails(library):
    conn = unaware(library)
    with pytest.raises(sqlite3.OperationalError, match="no such function"):
        conn.execute(
            "INSERT INTO reference_measurement_sets (id, taxon_treatment_id, character, data_kind, raw_text) "
            "VALUES (?, ?, 'spore_size', 'range', 'remote') "
            "ON CONFLICT(id) DO UPDATE SET raw_text=excluded.raw_text",
            (ENHANCED_ID, TREATMENT_ID),
        )
    conn.rollback()
    assert _row(conn, ENHANCED_ID)["raw_text"] == ENHANCED["raw_text"]
    conn.close()


def test_unaware_successor_of_enhanced_row_cannot_be_created(library):
    """An older ``create_revision`` would copy only the columns it knows and
    silently produce a successor without the extension."""
    conn = unaware(library)
    degraded = {k: v for k, v in ENHANCED.items() if k not in {"measurement_details_json", "q_core_min", "q_core_max"}}
    degraded.update(id="c0ffee00-0000-4000-8000-000000000002", supersedes_id=ENHANCED_ID, revision=3)
    with pytest.raises(sqlite3.OperationalError, match="no such function"):
        _insert_row(conn, degraded)
    conn.rollback()
    assert conn.execute(
        "SELECT COUNT(*) FROM reference_measurement_sets WHERE supersedes_id=?", (ENHANCED_ID,)
    ).fetchone()[0] == 0
    conn.close()


def test_unaware_attached_database_import_fails_and_aware_succeeds(library, tmp_path):
    """Mirrors ``import_portable_payload``: the destination reference library is
    ATTACHed to the main-database connection and written through
    ``_merge_reference_entity`` with a higher-revision source row."""
    incoming = _load("import_row_omitting_extension.json")
    immutable = {"taxon_treatment_id", "supersedes_id"}

    main = sqlite3.connect(tmp_path / "mushrooms.db")
    main.row_factory = sqlite3.Row
    main.execute("ATTACH DATABASE ? AS portable_reference", (str(library),))
    with pytest.raises(sqlite3.OperationalError, match="no such function"):
        _merge_reference_entity(
            incoming, main, table="portable_reference.reference_measurement_sets",
            immutable_fields=immutable, json_fields={"raw_points_json"},
        )
    main.rollback()
    untouched = dict(main.execute(
        "SELECT * FROM portable_reference.reference_measurement_sets WHERE id=?", (ENHANCED_ID,)
    ).fetchone())
    assert untouched["revision"] == 2 and untouched["length_core_max"] == 15.2

    register_measurement_contract(main)
    _merge_reference_entity(
        incoming, main, table="portable_reference.reference_measurement_sets",
        immutable_fields=immutable, json_fields={"raw_points_json"},
    )
    main.commit()
    merged = dict(main.execute(
        "SELECT * FROM portable_reference.reference_measurement_sets WHERE id=?", (ENHANCED_ID,)
    ).fetchone())
    assert merged["revision"] == 3 and merged["length_core_max"] == 15.0
    # Current production behavior, recorded as evidence for blocker 4: the
    # omitting source revision-upgrades the numeric fields while the newer
    # descriptors survive untouched. Stage 3 must reject this combination.
    assert merged["measurement_details_json"] == ENHANCED["measurement_details_json"]
    main.close()


def test_unaware_bundle_import_merge_fails_and_aware_reproduces_current_behavior(library):
    """Mirrors ``import_database_bundle`` → ``_upsert_library_row_by_revision``."""
    incoming = _load("import_row_omitting_extension.json")
    conn = unaware(library)
    with pytest.raises(sqlite3.OperationalError, match="no such function"):
        _upsert_library_row_by_revision(incoming, conn, table="reference_measurement_sets")
    conn.rollback()
    assert _row(conn, ENHANCED_ID)["revision"] == 2
    conn.close()

    conn = aware(library)
    assert _upsert_library_row_by_revision(incoming, conn, table="reference_measurement_sets") == "updated"
    conn.commit()
    merged = _row(conn, ENHANCED_ID)
    assert merged["revision"] == 3
    assert merged["measurement_details_json"] == ENHANCED["measurement_details_json"], (
        "column intersection keeps the extension but never rejects; Stage 3 policy"
    )
    conn.close()


def test_old_binary_table_rebuild_fails_closed_and_keeps_the_barrier(library):
    """``_rebuild_table_with_restrict_fks`` in an older binary would recreate the
    table from a DDL without the extension columns. The copy step names every
    existing column, so it fails before the old table is dropped."""
    conn = unaware(library)
    with pytest.raises(sqlite3.OperationalError):
        lib_schema._rebuild_table_with_restrict_fks(
            conn,
            table="reference_measurement_sets",
            ddl=lib_schema._REFERENCE_MEASUREMENT_SETS_DDL,
        )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(reference_measurement_sets)")}
    assert {"measurement_details_json", "q_core_min", "q_core_max"} <= columns
    assert _triggers(conn) >= {
        "reference_measurement_content_guard_update",
        "reference_measurement_content_guard_insert",
    }
    assert conn.execute("SELECT COUNT(*) FROM reference_measurement_sets").fetchone()[0] == 2
    assert _row(conn, ENHANCED_ID)["measurement_details_json"] == ENHANCED["measurement_details_json"]
    conn.close()


def test_barrier_constants_match_the_contract_document():
    doc = Path(__file__).parents[1] / "docs" / "reference-data" / "measurement-content-contract.md"
    blocks = re.findall(r"```json contract-spec\n(.*?)\n```", doc.read_text(encoding="utf-8"), re.DOTALL)
    assert len(blocks) == 1
    spec = json.loads(blocks[0])
    assert spec["contract_function"] == CONTRACT_FUNCTION
    assert spec["local_contract_version"] == LOCAL_CONTRACT_VERSION
    assert spec["barrier_message"] == BARRIER_MESSAGE
    trigger_names = {
        line.split()[-1]
        for ddl in BARRIER_TRIGGERS_DDL
        for line in ddl.splitlines()
        if "CREATE TRIGGER" in line
    }
    assert trigger_names == set(spec["barrier_triggers"])
    for column in spec["extension_fields"]:
        assert any(column in ddl for ddl in EXTENSION_COLUMNS_DDL)


def test_barrier_installation_is_idempotent(library):
    conn = aware(library)
    install_spike_barrier(conn)
    install_spike_barrier(conn)
    assert len(_triggers(conn) & {
        "reference_measurement_content_guard_update",
        "reference_measurement_content_guard_insert",
    }) == 2
    conn.close()
