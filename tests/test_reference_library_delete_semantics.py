"""Direct-SQL deletion semantics for the normalized library.

Stage 1 uses ``ON DELETE RESTRICT`` for the new library FK
relationships. Silent SQL cascade would be unsafe for a library that
is shared across many observations; the repository layer performs its
own explicit cleanup after use-checks pass. These tests pin the
underlying SQL behavior so future refactors can't accidentally
regress the safety guarantee.
"""
from __future__ import annotations

import json
import sqlite3

import pytest

from database import schema as _schema
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ObservationReferenceUseRepository,
    ReferenceInUseError,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
)
from database import reference_library_schema as _lib_schema


@pytest.fixture()
def libs(tmp_path, monkeypatch):
    db_path = tmp_path / "mushrooms.db"
    ref_path = tmp_path / "reference_values.db"
    monkeypatch.setattr(_schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(_schema, "get_reference_database_path", lambda: ref_path)
    monkeypatch.setattr(
        _schema,
        "get_bundled_reference_database_path",
        lambda: tmp_path / "does_not_exist.db",
    )
    _schema.init_database()
    return db_path, ref_path


def _seed_work_treatment_set(libs):
    work = ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="book",
            title="Direct SQL deletion fixture",
            short_label="Test",
            authors_json=json.dumps([{"family": "Author"}]),
        )
    )
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="",
            reference_work_id=work.id,
            name_as_published="Fictus fictus",
        )
    )
    ms = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="range",
            length_min=1.0,
            length_max=2.0,
        )
    )
    return work, treatment, ms


def test_fk_on_delete_actions_are_restrict(libs):
    _, ref_path = libs
    conn = sqlite3.connect(ref_path)
    try:
        treatments = _lib_schema._fk_on_delete_actions(
            conn, "reference_taxon_treatments"
        )
        sets = _lib_schema._fk_on_delete_actions(
            conn, "reference_measurement_sets"
        )
    finally:
        conn.close()
    assert treatments["reference_works"] == "RESTRICT"
    assert sets["reference_taxon_treatments"] == "RESTRICT"


def test_direct_sql_delete_of_work_with_treatments_is_blocked(libs):
    _, ref_path = libs
    work, _, _ = _seed_work_treatment_set(libs)
    conn = sqlite3.connect(ref_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "DELETE FROM reference_works WHERE id = ?", (work.id,)
            )
        # Row still present.
        row = conn.execute(
            "SELECT id FROM reference_works WHERE id = ?", (work.id,)
        ).fetchone()
        assert row is not None
    finally:
        conn.close()


def test_direct_sql_delete_of_treatment_with_measurement_sets_is_blocked(libs):
    _, ref_path = libs
    _, treatment, _ = _seed_work_treatment_set(libs)
    conn = sqlite3.connect(ref_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "DELETE FROM reference_taxon_treatments WHERE id = ?",
                (treatment.id,),
            )
    finally:
        conn.close()


def test_repository_delete_of_work_explicitly_cascades_when_no_uses(libs):
    """When nothing is attached to observations, the repository is
    allowed to delete a work; it MUST explicitly clean up descendants
    since the SQL layer refuses cascade."""
    _, ref_path = libs
    work, treatment, ms = _seed_work_treatment_set(libs)
    ReferenceWorkRepository.delete(work.id)
    conn = sqlite3.connect(ref_path)
    try:
        counts = {
            "reference_works": conn.execute(
                "SELECT COUNT(*) FROM reference_works"
            ).fetchone()[0],
            "reference_taxon_treatments": conn.execute(
                "SELECT COUNT(*) FROM reference_taxon_treatments"
            ).fetchone()[0],
            "reference_measurement_sets": conn.execute(
                "SELECT COUNT(*) FROM reference_measurement_sets"
            ).fetchone()[0],
        }
    finally:
        conn.close()
    assert counts == {
        "reference_works": 0,
        "reference_taxon_treatments": 0,
        "reference_measurement_sets": 0,
    }


def test_repository_delete_of_work_blocked_when_use_exists(libs):
    db_path, _ = libs
    work, _, ms = _seed_work_treatment_set(libs)
    obs_conn = sqlite3.connect(db_path)
    try:
        cur = obs_conn.execute(
            "INSERT INTO observations (date, location) VALUES (?, ?)",
            ("2026-01-01", "Test"),
        )
        obs_id = cur.lastrowid
        obs_conn.commit()
    finally:
        obs_conn.close()
    ObservationReferenceUseRepository.attach(obs_id, ms.id)
    with pytest.raises(ReferenceInUseError):
        ReferenceWorkRepository.delete(work.id)


def test_cascade_upgrade_migrates_existing_databases(tmp_path, monkeypatch):
    """Databases created before the RESTRICT correction had CASCADE FKs;
    reopening them must silently rebuild the tables with RESTRICT and
    preserve existing rows."""
    ref_path = tmp_path / "reference_values.db"
    conn = sqlite3.connect(ref_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(_lib_schema._REFERENCE_WORKS_DDL)
        # Legacy CASCADE variants.
        conn.execute(
            """
            CREATE TABLE reference_taxon_treatments (
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
                    ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE reference_measurement_sets (
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
                    ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            "INSERT INTO reference_works (id, type, title, short_label, verification_status, visibility) "
            "VALUES ('work-1', 'book', 'Legacy', 'Legacy', 'incomplete', 'private')"
        )
        conn.execute(
            "INSERT INTO reference_taxon_treatments (id, reference_work_id, name_as_published) "
            "VALUES ('treat-1', 'work-1', 'Legacy taxon')"
        )
        conn.execute(
            "INSERT INTO reference_measurement_sets (id, taxon_treatment_id, character, data_kind) "
            "VALUES ('set-1', 'treat-1', 'spore_size', 'range')"
        )
        conn.commit()
    finally:
        conn.close()

    # Reopen and let init upgrade the FKs.
    conn = sqlite3.connect(ref_path)
    try:
        _lib_schema.init_reference_library_schema(conn)
        treatments = _lib_schema._fk_on_delete_actions(
            conn, "reference_taxon_treatments"
        )
        sets = _lib_schema._fk_on_delete_actions(
            conn, "reference_measurement_sets"
        )
        counts = {
            "reference_works": conn.execute(
                "SELECT COUNT(*) FROM reference_works"
            ).fetchone()[0],
            "reference_taxon_treatments": conn.execute(
                "SELECT COUNT(*) FROM reference_taxon_treatments"
            ).fetchone()[0],
            "reference_measurement_sets": conn.execute(
                "SELECT COUNT(*) FROM reference_measurement_sets"
            ).fetchone()[0],
        }
    finally:
        conn.close()
    assert treatments["reference_works"] == "RESTRICT"
    assert sets["reference_taxon_treatments"] == "RESTRICT"
    assert counts == {
        "reference_works": 1,
        "reference_taxon_treatments": 1,
        "reference_measurement_sets": 1,
    }
