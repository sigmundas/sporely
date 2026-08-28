"""Focused contracts for explicit attached-reference snapshot updates."""
from __future__ import annotations

import json
import sqlite3

import pytest

from database import schema as _schema
from database.reference_citation import observation_snapshots_semantically_equal
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ObservationReferenceUseRepository,
    ReferenceIntegrityError,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
)


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


def _seed_attached(libs):
    db_path, _ = libs
    with sqlite3.connect(db_path) as conn:
        observation_id = conn.execute(
            "INSERT INTO observations (date, location) VALUES (?, ?)",
            ("2026-08-28", "Test"),
        ).lastrowid
    work = ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="book",
            title="Original title",
            short_label="Author 2000",
            authors_json=json.dumps([{"family": "Author"}]),
            year=2000,
        )
    )
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="",
            reference_work_id=work.id,
            taxon_id="taxon-1",
            name_as_published="Russula originalis",
            locator_text="p. 10",
        )
    )
    measurement_set = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="range",
            raw_text="8–10 × 5–6 µm",
            length_core_min=8.0,
            length_core_max=10.0,
            width_core_min=5.0,
            width_core_max=6.0,
        )
    )
    use = ObservationReferenceUseRepository.attach(
        int(observation_id),
        measurement_set.id,
        role="supports_identification",
        note="comparison note",
    )
    return work, treatment, measurement_set, use


def test_snapshot_comparison_ignores_key_order_and_reference_revision_only():
    stored = json.dumps(
        {"schema_version": 1, "reference_revision": 1, "raw_text": "8–10"}
    )
    current = {
        "raw_text": "8–10",
        "reference_revision": 99,
        "schema_version": 1,
    }

    assert observation_snapshots_semantically_equal(stored, current) is True
    current["raw_text"] = "8–11"
    assert observation_snapshots_semantically_equal(stored, current) is False


def test_work_only_edit_marks_snapshot_update_available_without_mutating_use(libs):
    work, _, _, use = _seed_attached(libs)
    frozen = use.snapshot_json
    ReferenceWorkRepository.update(work.id, {"title": "Corrected title"})

    status = ObservationReferenceUseRepository.snapshot_status(use.id)
    unchanged = ObservationReferenceUseRepository.get(use.id)

    assert status.state == "update_available"
    assert unchanged is not None
    assert unchanged.snapshot_json == frozen


def test_treatment_only_edit_marks_snapshot_update_available(libs):
    _, treatment, _, use = _seed_attached(libs)
    TaxonTreatmentRepository.update(treatment.id, {"locator_text": "p. 12"})

    status = ObservationReferenceUseRepository.snapshot_status(use.id)

    assert status.state == "update_available"


def test_measurement_set_edit_marks_snapshot_update_available(libs):
    _, _, measurement_set, use = _seed_attached(libs)
    MeasurementSetRepository.update(
        measurement_set.id,
        {"raw_text": "8–11 × 5–6 µm", "length_core_max": 11.0},
    )

    status = ObservationReferenceUseRepository.snapshot_status(use.id)

    assert status.state == "update_available"


def test_explicit_update_preserves_attachment_identity_role_note_and_association(libs):
    work, _, _, use = _seed_attached(libs)
    ReferenceWorkRepository.update(work.id, {"title": "Corrected title"})

    refreshed, changed = ObservationReferenceUseRepository.refresh_snapshot(use.id)
    snapshot = json.loads(refreshed.snapshot_json)

    assert changed is True
    assert refreshed.id == use.id
    assert refreshed.observation_id == use.observation_id
    assert refreshed.reference_measurement_set_id == use.reference_measurement_set_id
    assert refreshed.role == "supports_identification"
    assert refreshed.note == "comparison note"
    assert refreshed.selected_at == use.selected_at
    assert snapshot["full_citation"].find("Corrected title") >= 0
    assert ObservationReferenceUseRepository.snapshot_status(use.id).state == "current"


def test_semantically_identical_saves_are_current_and_refresh_is_noop(libs):
    work, treatment, measurement_set, use = _seed_attached(libs)
    ReferenceWorkRepository.update(work.id, {"title": work.title})
    TaxonTreatmentRepository.update(
        treatment.id, {"locator_text": treatment.locator_text}
    )
    MeasurementSetRepository.update(
        measurement_set.id, {"raw_text": measurement_set.raw_text}
    )
    before = ObservationReferenceUseRepository.get(use.id)

    status = ObservationReferenceUseRepository.snapshot_status(use.id)
    after, changed = ObservationReferenceUseRepository.refresh_snapshot(use.id)

    assert status.state == "current"
    assert changed is False
    assert before is not None
    assert after == before


def test_missing_library_source_preserves_historical_snapshot(libs):
    _, _, measurement_set, use = _seed_attached(libs)
    frozen = use.snapshot_json
    _, ref_path = libs
    with sqlite3.connect(ref_path) as conn:
        conn.execute(
            "DELETE FROM reference_measurement_sets WHERE id = ?",
            (measurement_set.id,),
        )

    status = ObservationReferenceUseRepository.snapshot_status(use.id)

    assert status.state == "source_missing"
    with pytest.raises(ReferenceIntegrityError, match="source is unavailable"):
        ObservationReferenceUseRepository.refresh_snapshot(use.id)
    unchanged = ObservationReferenceUseRepository.get(use.id)
    assert unchanged is not None
    assert unchanged.snapshot_json == frozen


def test_successor_uuid_does_not_make_original_attachment_stale(libs):
    _, _, measurement_set, use = _seed_attached(libs)
    successor = MeasurementSetRepository.create_revision(
        measurement_set.id, {"raw_text": "9–11 × 5–6 µm"}
    )

    status = ObservationReferenceUseRepository.snapshot_status(use.id)

    assert successor.id != measurement_set.id
    assert status.state == "current"
