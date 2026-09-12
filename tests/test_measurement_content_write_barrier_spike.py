"""Stage 1 barrier cases, now run against the production barrier.

Blocker 2 of ``docs/plans/active/2026-09-10-reported-statistics-and-range-semantics.md``.
The barrier is a pair of SQLite triggers on ``reference_measurement_sets`` whose
body calls an application-defined function, ``sporely_measurement_contract()``.
A contract-aware connection registers that function; an unaware (older binary)
connection does not. SQLite resolves trigger functions when it prepares the
DML statement, so an unaware connection cannot even compile an INSERT or
UPDATE against the table: the statement fails with ``no such function`` and
nothing is written. See the contract document, section "Local write barrier".

Written as a Stage 1 spike with its own DDL; since Stage 3A the columns,
triggers and ``register_measurement_contract`` are owned by
``database/reference_library_schema.py`` and installed by
``init_reference_library_schema``. This module keeps the Stage 1 bypass
scenarios (importer merge paths, upsert, successor, table rebuild) and runs
them against that production implementation; the schema/migration cases are in
``tests/test_reference_measurement_content_schema.py``.
"""
from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path

import pytest

from database import reference_library_schema as lib_schema
from database.reference_library_schema import register_measurement_contract
from utils.archive.portable_import import _merge_reference_entity
from utils.db_share import _upsert_library_row_by_revision

FIXTURES = Path(__file__).parent / "fixtures" / "reference_statistics"

CONTRACT_FUNCTION = lib_schema.MEASUREMENT_CONTRACT_FUNCTION
LOCAL_CONTRACT_VERSION = lib_schema.LOCAL_MEASUREMENT_CONTRACT_VERSION
BARRIER_MESSAGE = lib_schema.MEASUREMENT_CONTRACT_BARRIER_MESSAGE
EXTENSION_COLUMNS = tuple(name for name, _ in lib_schema.MEASUREMENT_CONTENT_EXTENSION_COLUMNS)


def _pre_feature_sets_ddl() -> str:
    """The table DDL as an older binary carries it (no extension columns)."""
    ddl = lib_schema._REFERENCE_MEASUREMENT_SETS_DDL
    for name, sql_type in lib_schema.MEASUREMENT_CONTENT_EXTENSION_COLUMNS:
        ddl = ddl.replace(f"    {name} {sql_type},\n", "", 1)
    assert not any(name in ddl for name in EXTENSION_COLUMNS)
    return ddl


WORK_ID = "0f3e7c2a-5b1d-4e9f-a8c7-6d5e4f3a2b1c"
TREATMENT_ID = "2a9e6b1c-7d4f-4a3e-8c5b-1f0e9d8c7b6a"


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
    """A real normalized library initialized by production code and seeded."""
    path = tmp_path / "reference_values.db"
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    lib_schema.init_reference_library_schema(conn)  # registers the contract on conn
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
    # The older binary's idempotent startup path must not break on an enhanced
    # library. Its ``init_reference_library_schema`` runs the same statements as
    # the current one minus the extension/barrier/registration steps: the table
    # DDL it carries (IF NOT EXISTS, a no-op here), the indexes, the sync
    # triggers and the sync-state backfill, all on an unregistered connection.
    conn.execute(_pre_feature_sets_ddl())
    for statement in lib_schema._REFERENCE_LIBRARY_INDEXES:
        conn.execute(statement)
    for statement in lib_schema._REFERENCE_CLOUD_SYNC_TRIGGERS:
        conn.execute(statement)
    conn.execute(
        "INSERT OR IGNORE INTO reference_cloud_sync_state(entity_type, entity_id) "
        "SELECT 'measurement_set', id FROM reference_measurement_sets"
    )
    conn.commit()
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
            ddl=_pre_feature_sets_ddl(),
        )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(reference_measurement_sets)")}
    assert set(EXTENSION_COLUMNS) <= columns
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
        for ddl in lib_schema._MEASUREMENT_CONTENT_GUARD_TRIGGERS_DDL
        for line in ddl.splitlines()
        if "CREATE TRIGGER" in line
    }
    assert trigger_names == set(spec["barrier_triggers"]) == set(lib_schema.MEASUREMENT_CONTENT_GUARD_TRIGGERS)
    assert list(spec["extension_fields"]) == list(EXTENSION_COLUMNS)


def test_barrier_installation_is_idempotent(library):
    conn = aware(library)
    lib_schema.init_reference_library_schema(conn)
    lib_schema.init_reference_library_schema(conn)
    assert len(_triggers(conn) & {
        "reference_measurement_content_guard_update",
        "reference_measurement_content_guard_insert",
    }) == 2
    conn.close()
