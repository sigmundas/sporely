"""Stage 3A: local schema extension and old-client write barrier.

Exercises the production migration and barrier owned by
``database/reference_library_schema.py`` (``init_reference_library_schema``,
``register_measurement_contract``) against temporary SQLite databases:

* upgrading a populated pre-feature library and initializing a fresh one;
* exact presence, type and nullability of the three extension columns;
* legacy rows receive NULL, never invented semantics;
* idempotence of repeated initialization, including on enhanced and malformed
  content;
* the coarse barrier accepted on 2026-09-11: an unaware connection (opened
  exactly as the pre-feature desktop client opens the library) cannot INSERT
  or UPDATE ``reference_measurement_sets``, can read and DELETE, and rejected
  writes leave rows unchanged; contract-aware connections write normally.

The unaware client is modelled with raw SQL shaped like the pre-feature
``MeasurementSetRepository`` (its column list and statement forms), not with a
special helper.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from database import reference_library_schema as lib_schema
from database import schema as _schema
from database.reference_library import MeasurementSet, MeasurementSetRepository
from references.measurement_content import (
    EXTENSION_FIELDS,
    is_enhanced_row,
)

FIXTURES = Path(__file__).parent / "fixtures" / "reference_statistics"

WORK_ID = "0f3e7c2a-5b1d-4e9f-a8c7-6d5e4f3a2b1c"
TREATMENT_ID = "2a9e6b1c-7d4f-4a3e-8c5b-1f0e9d8c7b6a"
LEGACY_ID = "6d1f0a8e-3c2b-4f8a-9e1d-0b7c5a2f4e11"
SECOND_LEGACY_ID = "7e2a1b9f-4d3c-4a5b-8c6d-1e2f3a4b5c6d"

# The pre-feature repository's column list (``MeasurementSetRepository._COLUMNS``
# at Stage 2 acceptance 829be09), i.e. what a v0.9.22-era client names in its
# INSERT and knows in its UPDATE.
OLD_CLIENT_COLUMNS = (
    "id", "taxon_treatment_id", "character", "raw_text", "data_kind",
    "length_min", "length_core_min", "length_core_max", "length_max",
    "width_min", "width_core_min", "width_core_max", "width_max",
    "q_min", "q_max", "q_mean", "length_mean", "width_mean",
    "sample_size", "specimen_count", "mount_medium", "stain", "preparation",
    "measurement_method", "notes", "raw_points_json", "revision",
    "supersedes_id", "legacy_reference_value_id", "created_at", "updated_at",
)

EXPECTED_EXTENSION_COLUMN_INFO = {
    # name: (declared type, notnull, default, pk)
    "measurement_details_json": ("TEXT", 0, None, 0),
    "q_core_min": ("REAL", 0, None, 0),
    "q_core_max": ("REAL", 0, None, 0),
}


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


ENHANCED = _load("row_enhanced.json")
LEGACY = _load("row_legacy_only.json")


# --- Library builders ---------------------------------------------------------


def _pre_feature_sets_ddl(*, cascade: bool = False) -> str:
    """The ``reference_measurement_sets`` DDL as shipped before this stage."""
    ddl = lib_schema._REFERENCE_MEASUREMENT_SETS_DDL
    for name, sql_type in lib_schema.MEASUREMENT_CONTENT_EXTENSION_COLUMNS:
        line = f"    {name} {sql_type},\n"
        assert line in ddl, line
        ddl = ddl.replace(line, "", 1)
    if cascade:
        ddl = ddl.replace("ON DELETE RESTRICT", "ON DELETE CASCADE")
    return ddl


def old_client_connect(path: Path) -> sqlite3.Connection:
    """Open the library exactly as the pre-feature desktop client does
    (``get_reference_connection`` + ``_connect_reference`` at 829be09, minus
    the registration this stage adds): no application-defined function."""
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def aware_connect(path: Path) -> sqlite3.Connection:
    conn = old_client_connect(path)
    lib_schema.register_measurement_contract(conn)
    return conn


def _old_client_insert(conn: sqlite3.Connection, row: dict) -> None:
    """The pre-feature ``MeasurementSetRepository.create`` statement."""
    values = tuple(row.get(name) for name in OLD_CLIENT_COLUMNS)
    conn.execute(
        f"INSERT INTO reference_measurement_sets ({', '.join(OLD_CLIENT_COLUMNS)}) "
        f"VALUES ({', '.join('?' for _ in OLD_CLIENT_COLUMNS)})",
        values,
    )


def _legacy_row(set_id: str, **overrides) -> dict:
    row = {name: LEGACY.get(name) for name in OLD_CLIENT_COLUMNS}
    row.update(id=set_id, created_at="2025-01-01T00:00:00", updated_at="2025-01-01T00:00:00")
    row.update(overrides)
    return row


def build_legacy_library(path: Path, *, cascade: bool = False) -> None:
    """A populated library as a pre-feature client would have left it:
    pre-feature table DDL, no extension columns, no barrier triggers."""
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(lib_schema._REFERENCE_WORKS_DDL)
        conn.execute(lib_schema._REFERENCE_TAXON_TREATMENTS_DDL)
        conn.execute(_pre_feature_sets_ddl(cascade=cascade))
        for statement in lib_schema._REFERENCE_LIBRARY_INDEXES:
            if any(f"ON {table}(" in statement for table in (
                "reference_works", "reference_taxon_treatments", "reference_measurement_sets",
            )):
                conn.execute(statement)
        conn.execute(
            "INSERT INTO reference_works (id, type, title, short_label) "
            "VALUES (?, 'book', 'Fixture', 'Fixture 2020')",
            (WORK_ID,),
        )
        conn.execute(
            "INSERT INTO reference_taxon_treatments (id, reference_work_id, name_as_published) "
            "VALUES (?, ?, 'Hebeloma fixtura')",
            (TREATMENT_ID, WORK_ID),
        )
        _old_client_insert(conn, _legacy_row(LEGACY_ID))
        _old_client_insert(
            conn,
            _legacy_row(
                SECOND_LEGACY_ID,
                raw_text="(6.9) 8.0-15.2 (16.1) x 4.1-8.9, Q = 1.17-2.79, Qm = 1.7",
                length_min=6.9, length_core_min=8.0, length_core_max=15.2, length_max=16.1,
                width_min=4.1, width_core_min=5.1, width_core_max=8.2, width_max=8.9,
                q_min=1.17, q_max=2.79, q_mean=1.7, sample_size=30, specimen_count=3,
                notes="legacy notes", raw_points_json="[[8.0, 5.1]]", revision=2,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def upgrade(path: Path) -> None:
    """Run the production initialization the way the application does."""
    conn = sqlite3.connect(path)
    try:
        lib_schema.init_reference_library_schema(conn)
    finally:
        conn.close()


def _insert_enhanced(conn: sqlite3.Connection, row: dict = ENHANCED) -> None:
    columns = [key for key in row if key not in {"created_at", "updated_at"}]
    conn.execute(
        f"INSERT INTO reference_measurement_sets ({', '.join(columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)})",
        [row[key] for key in columns],
    )


# --- Inspection helpers ------------------------------------------------------


def table_info(conn: sqlite3.Connection) -> dict[str, tuple]:
    return {
        row[1]: (row[2], row[3], row[4], row[5])
        for row in conn.execute("PRAGMA table_info(reference_measurement_sets)").fetchall()
    }


def rows(conn: sqlite3.Connection) -> dict[str, dict]:
    return {
        row["id"]: dict(row)
        for row in conn.execute("SELECT * FROM reference_measurement_sets ORDER BY id").fetchall()
    }


def schema_objects(conn: sqlite3.Connection) -> set[tuple]:
    return set(
        conn.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master WHERE sql IS NOT NULL"
        ).fetchall()
    )


def guard_triggers(conn: sqlite3.Connection) -> dict[str, str]:
    return {
        row[0]: row[1]
        for row in conn.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND name IN (?, ?)",
            lib_schema.MEASUREMENT_CONTENT_GUARD_TRIGGERS,
        )
    }


def _snapshot(path: Path) -> tuple[dict, set, dict]:
    conn = old_client_connect(path)
    try:
        return rows(conn), schema_objects(conn), table_info(conn)
    finally:
        conn.close()


@pytest.fixture
def legacy_library(tmp_path) -> Path:
    path = tmp_path / "reference_values.db"
    build_legacy_library(path)
    return path


@pytest.fixture
def upgraded_library(legacy_library) -> Path:
    upgrade(legacy_library)
    return legacy_library


@pytest.fixture
def enhanced_library(upgraded_library) -> Path:
    conn = aware_connect(upgraded_library)
    _insert_enhanced(conn)
    conn.commit()
    conn.close()
    return upgraded_library


# --- 1–5: upgrade and fresh initialization ------------------------------------


def test_upgrading_populated_legacy_library_adds_exactly_the_extension(legacy_library):
    before_rows, _, before_info = _snapshot(legacy_library)
    assert not set(EXPECTED_EXTENSION_COLUMN_INFO) & set(before_info)
    assert len(before_rows) == 2

    upgrade(legacy_library)

    after_rows, _, after_info = _snapshot(legacy_library)
    # Exactly the three columns were added, at the end, with the frozen types.
    assert list(after_info) == list(before_info) + list(EXPECTED_EXTENSION_COLUMN_INFO)
    for name, expected in EXPECTED_EXTENSION_COLUMN_INFO.items():
        assert after_info[name] == expected, name
    for name in before_info:
        assert after_info[name] == before_info[name], name
    # Every pre-existing value survives byte for byte; the new fields are NULL.
    for set_id, before in before_rows.items():
        after = after_rows[set_id]
        assert {k: after[k] for k in before} == before
        assert all(after[name] is None for name in EXTENSION_FIELDS)
        assert not is_enhanced_row(after)


def test_upgrade_of_cascade_era_library_rebuilds_then_extends_then_guards(tmp_path):
    """Libraries older than the RESTRICT correction are rebuilt on open; the
    rebuild drops triggers, so the barrier must be created after it."""
    path = tmp_path / "reference_values.db"
    build_legacy_library(path, cascade=True)
    before_rows, _, _ = _snapshot(path)

    upgrade(path)

    conn = old_client_connect(path)
    try:
        assert lib_schema._fk_on_delete_actions(conn, "reference_measurement_sets") == {
            "reference_taxon_treatments": "RESTRICT"
        }
        info = table_info(conn)
        for name, expected in EXPECTED_EXTENSION_COLUMN_INFO.items():
            assert info[name] == expected
        assert set(guard_triggers(conn)) == set(lib_schema.MEASUREMENT_CONTENT_GUARD_TRIGGERS)
        after = rows(conn)
        for set_id, before in before_rows.items():
            assert {k: after[set_id][k] for k in before} == before
            assert all(after[set_id][name] is None for name in EXTENSION_FIELDS)
    finally:
        conn.close()


def test_fresh_library_has_the_extension_and_the_barrier(tmp_path, monkeypatch):
    ref_path = tmp_path / "reference_values.db"
    monkeypatch.setattr(_schema, "get_reference_database_path", lambda: ref_path)
    monkeypatch.setattr(_schema, "get_bundled_reference_database_path", lambda: tmp_path / "none.db")
    _schema.init_reference_database(ref_path, seed_from_bundle=False, migrate_legacy=False)

    conn = old_client_connect(ref_path)
    try:
        info = table_info(conn)
        for name, expected in EXPECTED_EXTENSION_COLUMN_INFO.items():
            assert info[name] == expected, name
        assert set(guard_triggers(conn)) == set(lib_schema.MEASUREMENT_CONTENT_GUARD_TRIGGERS)
        assert conn.execute("SELECT COUNT(*) FROM reference_measurement_sets").fetchone()[0] == 0
    finally:
        conn.close()


def test_fresh_and_upgraded_libraries_have_identical_table_shape(tmp_path, upgraded_library):
    fresh = tmp_path / "fresh.db"
    upgrade(fresh)
    _, _, fresh_info = _snapshot(fresh)
    _, _, upgraded_info = _snapshot(upgraded_library)
    assert list(fresh_info.items()) == list(upgraded_info.items())


def test_declared_ddl_and_constants_match_the_contract_spec():
    doc = Path(__file__).parents[1] / "docs" / "reference-data" / "measurement-content-contract.md"
    text = doc.read_text(encoding="utf-8")
    start = text.index("```json contract-spec\n") + len("```json contract-spec\n")
    spec = json.loads(text[start : text.index("\n```", start)])
    assert spec["contract_function"] == lib_schema.MEASUREMENT_CONTRACT_FUNCTION
    assert spec["local_contract_version"] == lib_schema.LOCAL_MEASUREMENT_CONTRACT_VERSION
    assert spec["barrier_message"] == lib_schema.MEASUREMENT_CONTRACT_BARRIER_MESSAGE
    assert list(spec["barrier_triggers"]) == list(lib_schema.MEASUREMENT_CONTENT_GUARD_TRIGGERS)
    assert [name for name, _ in lib_schema.MEASUREMENT_CONTENT_EXTENSION_COLUMNS] == list(
        spec["extension_fields"]
    ) == list(EXTENSION_FIELDS)


# --- 6–7, 15–16: idempotence -------------------------------------------------


def test_repeated_initialization_changes_nothing(upgraded_library):
    first = _snapshot(upgraded_library)
    conn = aware_connect(upgraded_library)
    try:
        data_version = conn.execute("PRAGMA data_version").fetchone()[0]
        lib_schema.init_reference_library_schema(conn)
        lib_schema.init_reference_library_schema(conn)
        assert conn.execute("PRAGMA data_version").fetchone()[0] == data_version
    finally:
        conn.close()
    upgrade(upgraded_library)  # a fresh connection, as on every app start
    assert _snapshot(upgraded_library) == first
    conn = old_client_connect(upgraded_library)
    try:
        names = [row[1] for row in conn.execute("PRAGMA table_info(reference_measurement_sets)")]
        assert len(names) == len(set(names))
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        conn.close()


def test_repeated_initialization_preserves_enhanced_values_untouched(enhanced_library):
    before = _snapshot(enhanced_library)
    enhanced_before = before[0][ENHANCED["id"]]
    assert enhanced_before["measurement_details_json"] == ENHANCED["measurement_details_json"]
    assert (enhanced_before["q_core_min"], enhanced_before["q_core_max"]) == (1.36, 2.19)

    upgrade(enhanced_library)
    upgrade(enhanced_library)

    after = _snapshot(enhanced_library)
    assert after == before
    assert after[0][ENHANCED["id"]]["measurement_details_json"] == ENHANCED["measurement_details_json"]


@pytest.mark.parametrize(
    "details_json",
    [
        "{not json",
        json.dumps(_load("details_unsupported_future_version.json"), sort_keys=True),
        '{"schema_version": 1, "metrics": {}, "unknown": true}',
        "   ",
    ],
    ids=["malformed", "future-version", "wrong-shape", "blank"],
)
def test_initialization_never_rewrites_unsupported_or_malformed_details(upgraded_library, details_json):
    """The schema owner adds columns and triggers; it never reads, validates or
    normalizes ``measurement_details_json``. Content rules are the writers'
    business (``references/measurement_content.py``)."""
    conn = aware_connect(upgraded_library)
    _insert_enhanced(conn, {**ENHANCED, "measurement_details_json": details_json, "q_core_min": 9.0, "q_core_max": 1.0})
    conn.commit()
    conn.close()
    before = _snapshot(upgraded_library)

    upgrade(upgraded_library)

    after = _snapshot(upgraded_library)
    assert after == before
    stored = after[0][ENHANCED["id"]]
    assert stored["measurement_details_json"] == details_json
    assert (stored["q_core_min"], stored["q_core_max"]) == (9.0, 1.0)


def test_barrier_survives_reinitialization_and_is_not_recreated(enhanced_library):
    conn = aware_connect(enhanced_library)
    triggers_before = guard_triggers(conn)
    rootpages_before = dict(
        conn.execute(
            "SELECT name, rootpage FROM sqlite_master WHERE type='trigger' AND name IN (?, ?)",
            lib_schema.MEASUREMENT_CONTENT_GUARD_TRIGGERS,
        ).fetchall()
    )
    lib_schema.init_reference_library_schema(conn)
    assert guard_triggers(conn) == triggers_before
    assert dict(
        conn.execute(
            "SELECT name, rootpage FROM sqlite_master WHERE type='trigger' AND name IN (?, ?)",
            lib_schema.MEASUREMENT_CONTENT_GUARD_TRIGGERS,
        ).fetchall()
    ) == rootpages_before
    conn.close()

    old = old_client_connect(enhanced_library)
    with pytest.raises(sqlite3.OperationalError, match="no such function: sporely_measurement_contract"):
        old.execute("UPDATE reference_measurement_sets SET raw_text = ? WHERE id = ?", ("x", LEGACY_ID))
    old.close()


def test_columns_and_barrier_become_visible_together_and_missing_triggers_are_restored(legacy_library):
    """The ALTERs and the trigger DDL run in one transaction, so no committed
    state has the columns without the barrier; if the triggers are ever
    missing on an extended table, initialization restores them."""
    conn = aware_connect(legacy_library)
    lib_schema.init_reference_library_schema(conn)
    for name in lib_schema.MEASUREMENT_CONTENT_GUARD_TRIGGERS:
        conn.execute(f"DROP TRIGGER {name}")
    conn.commit()
    assert guard_triggers(conn) == {}
    lib_schema.init_reference_library_schema(conn)
    assert set(guard_triggers(conn)) == set(lib_schema.MEASUREMENT_CONTENT_GUARD_TRIGGERS)
    conn.close()

    old = old_client_connect(legacy_library)
    with pytest.raises(sqlite3.OperationalError, match="no such function"):
        old.execute("UPDATE reference_measurement_sets SET notes = 'x' WHERE id = ?", (LEGACY_ID,))
    old.close()


# --- Interruption: columns are never committed without the barrier ----------


class _InjectedFailure(RuntimeError):
    pass


class _StatementCounter:
    def __init__(self, fail_at: int) -> None:
        self.fail_at = fail_at
        self.seen = 0

    def tick(self) -> None:
        self.seen += 1
        if self.seen == self.fail_at:
            raise _InjectedFailure(f"injected failure at statement {self.fail_at}")


def _failing_connection_factory(counter: _StatementCounter):
    class FailingCursor(sqlite3.Cursor):
        def execute(self, sql, *args, **kwargs):
            counter.tick()
            return super().execute(sql, *args, **kwargs)

    class FailingConnection(sqlite3.Connection):
        def cursor(self, factory=FailingCursor):
            return super().cursor(factory)

        def execute(self, sql, *args, **kwargs):
            counter.tick()
            return super().execute(sql, *args, **kwargs)

    return FailingConnection


def _count_init_statements(path: Path) -> int:
    counter = _StatementCounter(fail_at=0)  # never fires
    conn = sqlite3.connect(path, factory=_failing_connection_factory(counter))
    try:
        lib_schema.init_reference_library_schema(conn)
    finally:
        conn.close()
    return counter.seen


def assert_extension_never_visible_without_barrier(path: Path) -> None:
    """The invariant an unaware binary relies on, checked on committed state
    through a fresh unregistered connection."""
    conn = old_client_connect(path)
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='reference_measurement_sets'"
        ).fetchone()
        if exists is None:
            return
        info = table_info(conn)
        has_extension = any(name in info for name in EXTENSION_FIELDS)
        if not has_extension:
            return
        assert all(name in info for name in EXTENSION_FIELDS), info
        assert set(guard_triggers(conn)) == set(lib_schema.MEASUREMENT_CONTENT_GUARD_TRIGGERS), (
            "extension columns committed without the barrier"
        )
        with pytest.raises(sqlite3.OperationalError, match=NO_SUCH_FUNCTION):
            conn.execute("UPDATE reference_measurement_sets SET notes = 'x' WHERE 0")
    finally:
        conn.close()


def _start_state(kind: str, path: Path) -> None:
    if kind == "legacy":
        build_legacy_library(path)
    elif kind == "cascade-legacy":
        build_legacy_library(path, cascade=True)
    else:
        assert kind == "empty"


@pytest.mark.parametrize("kind", ["empty", "legacy", "cascade-legacy"])
def test_initialization_interrupted_at_every_statement_never_exposes_columns_without_barrier(tmp_path, kind):
    """Inject a failure at each statement position of initialization, from a
    fresh copy of the start state each time. After every failure the committed
    file must satisfy the invariant, and a subsequent uninterrupted
    initialization must converge to the same schema and rows as an
    uninterrupted run."""
    reference = tmp_path / "reference.db"
    _start_state(kind, reference)
    total = _count_init_statements(reference)
    assert total > 10
    # Steady state after two starts. (A CASCADE-era rebuild drops the two
    # measurement-set indexes together with the old table and the same run
    # does not recreate them; the next start does. Pre-existing behaviour of
    # _ensure_restrict_foreign_keys, unrelated to the barrier and left as is.)
    upgrade(reference)
    expected_rows, expected_schema, expected_info = _snapshot(reference)

    interrupted_positions = 0
    for position in range(1, total + 1):
        path = tmp_path / f"{kind}-{position}.db"
        _start_state(kind, path)
        counter = _StatementCounter(fail_at=position)
        conn = sqlite3.connect(path, factory=_failing_connection_factory(counter))
        try:
            with pytest.raises(_InjectedFailure):
                lib_schema.init_reference_library_schema(conn)
        finally:
            conn.close()
        interrupted_positions += 1
        assert_extension_never_visible_without_barrier(path)

        upgrade(path)  # resume on the next application start
        assert_extension_never_visible_without_barrier(path)
        upgrade(path)  # and the start after that: steady state
        rows_after, schema_after, info_after = _snapshot(path)
        assert info_after == expected_info, position
        assert schema_after == expected_schema, position
        assert rows_after == expected_rows, position
    assert interrupted_positions == total


def test_cascade_rebuild_recreates_the_guards_inside_its_own_transaction(tmp_path):
    """Direct check of the rebuild path: dropping the old table removes the
    triggers, and the rebuilt table must carry them before the commit."""
    path = tmp_path / "reference.db"
    build_legacy_library(path, cascade=True)
    conn = aware_connect(path)
    try:
        lib_schema.init_reference_library_schema(conn)
        assert set(guard_triggers(conn)) == set(lib_schema.MEASUREMENT_CONTENT_GUARD_TRIGGERS)
        # Rebuild again explicitly with the production arguments and a failing
        # post-DDL step: the whole rebuild rolls back, columns and guards stay.
        with pytest.raises(sqlite3.OperationalError):
            lib_schema._rebuild_table_with_restrict_fks(
                conn,
                table="reference_measurement_sets",
                ddl=lib_schema._REFERENCE_MEASUREMENT_SETS_DDL,
                post_ddl=(*lib_schema._MEASUREMENT_CONTENT_GUARD_TRIGGERS_DDL, "CREATE TRIGGER broken"),
            )
        assert set(guard_triggers(conn)) == set(lib_schema.MEASUREMENT_CONTENT_GUARD_TRIGGERS)
        assert set(EXTENSION_FIELDS) <= set(table_info(conn))
        assert conn.execute("SELECT COUNT(*) FROM reference_measurement_sets").fetchone()[0] == 2
    finally:
        conn.close()
    assert_extension_never_visible_without_barrier(path)


# --- 8: supported writers pass the barrier deliberately -----------------------


def test_production_connection_factory_registers_the_contract(upgraded_library, monkeypatch):
    monkeypatch.setattr(_schema, "get_reference_database_path", lambda: upgraded_library)
    conn = _schema.get_reference_connection()
    try:
        conn.execute(
            "UPDATE reference_measurement_sets SET raw_text = 'via factory' WHERE id = ?", (LEGACY_ID,)
        )
        _insert_enhanced(conn)
        conn.commit()
    finally:
        conn.close()
    stored = _snapshot(upgraded_library)[0]
    assert stored[LEGACY_ID]["raw_text"] == "via factory"
    assert stored[ENHANCED["id"]]["q_core_max"] == 2.19


def test_repository_writes_pass_the_barrier(upgraded_library, monkeypatch):
    """``MeasurementSetRepository`` still knows only the pre-feature columns
    (Stage 3B wires the extension); its INSERT/UPDATE compile because
    ``_connect_reference`` → ``init_reference_library_schema`` registers the
    function. This is the "new application does not block itself" check."""
    monkeypatch.setattr(_schema, "get_reference_database_path", lambda: upgraded_library)
    created = MeasurementSetRepository.create(
        MeasurementSet(
            id="", taxon_treatment_id=TREATMENT_ID, character="spore_size", data_kind="range",
            raw_text="9-11 x 5-6", length_core_min=9.0, length_core_max=11.0,
            width_core_min=5.0, width_core_max=6.0,
        )
    )
    updated = MeasurementSetRepository.update(created.id, {"notes": "edited by new client"})
    assert updated is not None and updated.notes == "edited by new client"
    revision = MeasurementSetRepository.create_revision(created.id, {"raw_text": "9-12 x 5-6"})
    assert revision.supersedes_id == created.id
    stored = _snapshot(upgraded_library)[0]
    assert stored[created.id]["notes"] == "edited by new client"
    assert all(stored[revision.id][name] is None for name in EXTENSION_FIELDS)


def test_initialized_raw_connection_and_explicit_registration_both_write(upgraded_library):
    conn = sqlite3.connect(upgraded_library)
    lib_schema.init_reference_library_schema(conn)
    conn.execute("UPDATE reference_measurement_sets SET notes = 'init-registered' WHERE id = ?", (LEGACY_ID,))
    conn.commit()
    conn.close()

    conn = aware_connect(upgraded_library)
    conn.execute(
        "UPDATE reference_measurement_sets SET q_core_min = 1.8, q_core_max = 2.4 WHERE id = ?",
        (SECOND_LEGACY_ID,),
    )
    conn.execute(
        "UPDATE reference_measurement_sets SET q_core_min = NULL, q_core_max = NULL WHERE id = ?",
        (SECOND_LEGACY_ID,),
    )
    conn.commit()
    conn.close()
    stored = _snapshot(upgraded_library)[0]
    assert stored[LEGACY_ID]["notes"] == "init-registered"
    assert stored[SECOND_LEGACY_ID]["q_core_min"] is None


def test_registered_lower_version_is_stopped_by_the_trigger_body(enhanced_library):
    conn = old_client_connect(enhanced_library)
    lib_schema.register_measurement_contract(conn, version=0)
    with pytest.raises(sqlite3.IntegrityError, match=lib_schema.MEASUREMENT_CONTRACT_BARRIER_MESSAGE):
        conn.execute("UPDATE reference_measurement_sets SET raw_text = 'x' WHERE id = ?", (ENHANCED["id"],))
    conn.rollback()
    assert rows(conn)[ENHANCED["id"]]["raw_text"] == ENHANCED["raw_text"]
    conn.close()


# --- 9–12: the unaware old client ---------------------------------------------


NO_SUCH_FUNCTION = "no such function: sporely_measurement_contract"


def test_old_client_insert_is_rejected_whole(upgraded_library):
    before = _snapshot(upgraded_library)
    conn = old_client_connect(upgraded_library)
    with pytest.raises(sqlite3.OperationalError, match=NO_SUCH_FUNCTION):
        _old_client_insert(conn, _legacy_row("11111111-2222-4333-8444-555555555555"))
    assert conn.in_transaction is False
    assert conn.total_changes == 0
    conn.close()
    assert _snapshot(upgraded_library) == before


@pytest.mark.parametrize(
    "assignments, params",
    [
        # MeasurementSetRepository.update shape: partial field set + updated_at.
        ("raw_text = ?, length_core_max = ?, updated_at = ?", ("edited", 13.0, "2026-01-01T00:00:00")),
        ("notes = ?", ("only notes",)),
        ("revision = revision + 1, updated_at = CURRENT_TIMESTAMP", ()),
        ("legacy_reference_value_id = ?", (42,)),
    ],
    ids=["content-edit", "notes-only", "revision-bump", "legacy-id-enrichment"],
)
@pytest.mark.parametrize("target", [LEGACY_ID, ENHANCED["id"]], ids=["legacy-row", "enhanced-row"])
def test_old_client_update_is_rejected_and_row_unchanged(enhanced_library, assignments, params, target):
    before = _snapshot(enhanced_library)
    conn = old_client_connect(enhanced_library)
    with pytest.raises(sqlite3.OperationalError, match=NO_SUCH_FUNCTION):
        conn.execute(
            f"UPDATE reference_measurement_sets SET {assignments} WHERE id = ?", (*params, target)
        )
    assert conn.total_changes == 0
    conn.close()
    assert _snapshot(enhanced_library) == before


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT OR REPLACE INTO reference_measurement_sets (id, taxon_treatment_id, character, data_kind, raw_text) "
        "VALUES (?, ?, 'spore_size', 'range', 'replaced')",
        "REPLACE INTO reference_measurement_sets (id, taxon_treatment_id, character, data_kind, raw_text) "
        "VALUES (?, ?, 'spore_size', 'range', 'replaced')",
        "INSERT INTO reference_measurement_sets (id, taxon_treatment_id, character, data_kind, raw_text) "
        "VALUES (?, ?, 'spore_size', 'range', 'remote') ON CONFLICT(id) DO UPDATE SET raw_text = excluded.raw_text",
        "INSERT OR IGNORE INTO reference_measurement_sets (id, taxon_treatment_id, character, data_kind, raw_text) "
        "VALUES (?, ?, 'spore_size', 'range', 'ignored')",
        "UPDATE OR IGNORE reference_measurement_sets SET raw_text = 'x' WHERE id = ? AND taxon_treatment_id = ?",
    ],
    ids=["insert-or-replace", "replace", "upsert", "insert-or-ignore", "update-or-ignore"],
)
def test_old_client_conflict_clauses_cannot_bypass(enhanced_library, sql):
    before = _snapshot(enhanced_library)
    conn = old_client_connect(enhanced_library)
    with pytest.raises(sqlite3.OperationalError, match=NO_SUCH_FUNCTION):
        conn.execute(sql, (ENHANCED["id"], TREATMENT_ID))
    conn.close()
    assert _snapshot(enhanced_library) == before


def test_old_client_transaction_with_a_guarded_statement_rolls_back_whole(enhanced_library):
    """A pre-feature writer that already changed other tables in the same
    transaction cannot commit a half-applied change: the failed prepare leaves
    the transaction open, and rollback restores everything."""
    before = _snapshot(enhanced_library)
    conn = old_client_connect(enhanced_library)
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("UPDATE reference_works SET title = 'renamed in the same transaction' WHERE id = ?", (WORK_ID,))
    conn.execute("DELETE FROM reference_measurement_sets WHERE id = ?", (LEGACY_ID,))
    with pytest.raises(sqlite3.OperationalError, match=NO_SUCH_FUNCTION):
        _old_client_insert(conn, _legacy_row(LEGACY_ID, raw_text="re-created"))
    assert conn.in_transaction
    conn.rollback()
    conn.close()
    assert _snapshot(enhanced_library) == before


def test_old_client_executescript_stops_at_the_guarded_statement(enhanced_library):
    before = _snapshot(enhanced_library)
    conn = old_client_connect(enhanced_library)
    script = f"""
        BEGIN;
        UPDATE reference_measurement_sets SET raw_text = 'scripted' WHERE id = '{LEGACY_ID}';
        UPDATE reference_works SET title = 'after' WHERE id = '{WORK_ID}';
        COMMIT;
    """
    with pytest.raises(sqlite3.OperationalError, match=NO_SUCH_FUNCTION):
        conn.executescript(script)
    conn.rollback()
    conn.close()
    assert _snapshot(enhanced_library) == before


def test_old_client_successor_insert_of_enhanced_row_is_rejected(enhanced_library):
    before = _snapshot(enhanced_library)
    conn = old_client_connect(enhanced_library)
    degraded = {name: ENHANCED.get(name) for name in OLD_CLIENT_COLUMNS}
    degraded.update(id="c0ffee00-0000-4000-8000-000000000002", supersedes_id=ENHANCED["id"], revision=3)
    with pytest.raises(sqlite3.OperationalError, match=NO_SUCH_FUNCTION):
        _old_client_insert(conn, degraded)
    conn.close()
    assert _snapshot(enhanced_library) == before


def test_old_client_cannot_add_a_row_through_an_attached_database(enhanced_library, tmp_path):
    """Mirrors ``import_portable_payload`` in a pre-feature binary, which never
    registers the function on the main connection it ATTACHes the library to."""
    before = _snapshot(enhanced_library)
    main = sqlite3.connect(tmp_path / "mushrooms.db")
    main.execute("ATTACH DATABASE ? AS portable_reference", (str(enhanced_library),))
    with pytest.raises(sqlite3.OperationalError, match=NO_SUCH_FUNCTION):
        main.execute(
            "UPDATE portable_reference.reference_measurement_sets SET raw_text = 'x' WHERE id = ?",
            (LEGACY_ID,),
        )
    main.close()
    assert _snapshot(enhanced_library) == before


# --- 13: DELETE and reads follow the accepted policy --------------------------


def test_old_client_can_read_and_delete(enhanced_library):
    conn = old_client_connect(enhanced_library)
    listed = rows(conn)
    assert set(listed) == {LEGACY_ID, SECOND_LEGACY_ID, ENHANCED["id"]}
    assert listed[ENHANCED["id"]]["measurement_details_json"] == ENHANCED["measurement_details_json"]

    conn.execute("DELETE FROM reference_measurement_sets WHERE id = ?", (ENHANCED["id"],))
    conn.execute("DELETE FROM reference_measurement_sets WHERE id = ?", (LEGACY_ID,))
    conn.commit()
    conn.close()

    remaining = _snapshot(enhanced_library)[0]
    assert set(remaining) == {SECOND_LEGACY_ID}
    assert remaining[SECOND_LEGACY_ID]["q_mean"] == 1.7


def test_delete_keeps_the_existing_tombstone_trigger_behavior(enhanced_library):
    conn = aware_connect(enhanced_library)
    conn.execute(
        "UPDATE reference_cloud_sync_state SET remote_identity_state = 'acknowledged', cloud_user_id = 'u1', "
        "cloud_row_version = 3, accepted_payload_json = '{}' "
        "WHERE entity_type = 'measurement_set' AND entity_id = ?",
        (ENHANCED["id"],),
    )
    conn.commit()
    conn.close()

    old = old_client_connect(enhanced_library)
    old.execute("DELETE FROM reference_measurement_sets WHERE id = ?", (ENHANCED["id"],))
    old.commit()
    tombstones = old.execute(
        "SELECT entity_id FROM reference_cloud_tombstones WHERE entity_type = 'measurement_set'"
    ).fetchall()
    assert [row[0] for row in tombstones] == [ENHANCED["id"]]
    old.close()


# --- 14: the barrier is durable across connections ----------------------------


def test_barrier_holds_across_close_and_reopen(enhanced_library):
    for _ in range(3):
        old = old_client_connect(enhanced_library)
        with pytest.raises(sqlite3.OperationalError, match=NO_SUCH_FUNCTION):
            old.execute("UPDATE reference_measurement_sets SET notes = 'x' WHERE id = ?", (LEGACY_ID,))
        old.close()
        aware = aware_connect(enhanced_library)
        aware.execute("UPDATE reference_measurement_sets SET notes = 'aware' WHERE id = ?", (LEGACY_ID,))
        aware.commit()
        aware.close()
    assert _snapshot(enhanced_library)[0][LEGACY_ID]["notes"] == "aware"


def test_awareness_is_connection_scoped_and_does_not_leak(enhanced_library):
    aware = aware_connect(enhanced_library)
    aware.execute("UPDATE reference_measurement_sets SET notes = 'aware' WHERE id = ?", (LEGACY_ID,))
    aware.commit()  # an aware writer that committed does not open the door for others
    old = old_client_connect(enhanced_library)
    with pytest.raises(sqlite3.OperationalError, match=NO_SUCH_FUNCTION):
        old.execute("UPDATE reference_measurement_sets SET notes = 'old' WHERE id = ?", (LEGACY_ID,))
    old.close()
    aware.close()
