from __future__ import annotations

import sqlite3

import pytest

from database import schema as _schema
from database.reference_library import (
    MeasurementSet,
    MeasurementSetPreferenceRepository,
    MeasurementSetRepository,
    ReferenceIntegrityError,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
)
from database.reference_library_schema import init_reference_library_schema


@pytest.fixture()
def libs(tmp_path, monkeypatch):
    db_path = tmp_path / "mushrooms.db"
    ref_path = tmp_path / "reference_values.db"
    monkeypatch.setattr(_schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(_schema, "get_reference_database_path", lambda: ref_path)
    monkeypatch.setattr(
        _schema,
        "get_bundled_reference_database_path",
        lambda: tmp_path / "missing-reference.db",
    )
    _schema.init_database()
    return db_path, ref_path


def _seed_set(label: str) -> MeasurementSet:
    work = ReferenceWorkRepository.create(
        ReferenceWork(id="", type="book", title=label, short_label=label)
    )
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="",
            reference_work_id=work.id,
            name_as_published="Russula paludosa",
        )
    )
    return MeasurementSetRepository.create(
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


def test_preferences_schema_is_idempotent_and_cascades_on_set_delete(libs):
    measurement_set = _seed_set("Alpha")
    MeasurementSetPreferenceRepository.set_favorite(measurement_set.id, True)
    MeasurementSetPreferenceRepository.mark_used(measurement_set.id)

    with sqlite3.connect(libs[1]) as conn:
        init_reference_library_schema(conn)
        init_reference_library_schema(conn)
        assert conn.execute(
            "SELECT is_favorite, recent_use_sequence "
            "FROM reference_measurement_set_preferences"
        ).fetchone() == (1, 1)

    MeasurementSetRepository.delete(measurement_set.id)
    assert MeasurementSetPreferenceRepository.get(measurement_set.id) is None


def test_explicit_favorite_and_actual_use_are_independent(libs):
    measurement_set = _seed_set("Alpha")

    assert MeasurementSetPreferenceRepository.get(measurement_set.id) is None
    favorite = MeasurementSetPreferenceRepository.set_favorite(
        measurement_set.id, True
    )
    assert favorite.is_favorite is True
    assert favorite.recent_use_sequence is None

    used = MeasurementSetPreferenceRepository.mark_used(measurement_set.id)
    assert used.is_favorite is True
    assert used.recent_use_sequence == 1

    unfavorite = MeasurementSetPreferenceRepository.toggle_favorite(
        measurement_set.id
    )
    assert unfavorite.is_favorite is False
    assert unfavorite.recent_use_sequence == 1


def test_mark_used_sequence_and_candidate_order_are_deterministic(libs):
    alpha = _seed_set("Alpha")
    beta = _seed_set("Beta")
    gamma = _seed_set("Gamma")
    delta = _seed_set("Delta")

    MeasurementSetPreferenceRepository.mark_used(alpha.id)
    MeasurementSetPreferenceRepository.mark_used(beta.id)
    MeasurementSetPreferenceRepository.mark_used(alpha.id)
    MeasurementSetPreferenceRepository.set_favorite(delta.id, True)
    MeasurementSetPreferenceRepository.set_favorite(gamma.id, True)
    MeasurementSetPreferenceRepository.mark_used(gamma.id)

    candidates = MeasurementSetRepository.list_attachment_candidates()
    assert [candidate.measurement_set_id for candidate in candidates] == [
        gamma.id,
        delta.id,
        alpha.id,
        beta.id,
    ]
    by_id = {candidate.measurement_set_id: candidate for candidate in candidates}
    assert by_id[gamma.id].is_favorite is True
    assert by_id[gamma.id].recent_use_sequence == 4
    assert by_id[delta.id].is_favorite is True
    assert by_id[delta.id].recent_use_sequence is None
    assert by_id[alpha.id].is_favorite is False
    assert by_id[alpha.id].recent_use_sequence == 3


def test_recent_work_picker_order_is_derived_from_actual_set_use(libs):
    alpha = _seed_set("Alpha")
    beta = _seed_set("Beta")
    MeasurementSetPreferenceRepository.mark_used(alpha.id)
    MeasurementSetPreferenceRepository.mark_used(beta.id)

    works = ReferenceWorkRepository.list_recent()

    assert [work.title for work in works[:2]] == ["Beta", "Alpha"]


def test_missing_measurement_set_operations_fail_cleanly(libs):
    for operation in (
        lambda: MeasurementSetPreferenceRepository.set_favorite("missing", True),
        lambda: MeasurementSetPreferenceRepository.toggle_favorite("missing"),
        lambda: MeasurementSetPreferenceRepository.mark_used("missing"),
    ):
        with pytest.raises(ReferenceIntegrityError, match="measurement_set missing not found"):
            operation()
    assert MeasurementSetPreferenceRepository.get("missing") is None
