from __future__ import annotations

import json

import pytest

from database import schema as _schema
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ObservationReferenceUseRepository,
    QuickAddReferenceRequest,
    QuickAddReferenceService,
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
        lambda: tmp_path / "missing.db",
    )
    _schema.init_database()
    conn = _schema.get_connection()
    try:
        observation_id = conn.execute(
            "INSERT INTO observations (date, location) VALUES (?, ?)",
            ("2026-08-28", "Quick add"),
        ).lastrowid
        conn.commit()
    finally:
        conn.close()
    return int(observation_id)


def _work(**updates) -> ReferenceWork:
    values = dict(
        id="",
        type="article",
        title="A source",
        short_label="Author 2026",
    )
    values.update(updates)
    return ReferenceWork(**values)


def _request(observation_id: int, **updates) -> QuickAddReferenceRequest:
    values = dict(
        observation_id=observation_id,
        work=_work(),
        treatment=TaxonTreatment(
            id="",
            reference_work_id="",
            taxon_id="taxon-1",
            name_as_published="Russula example",
            locator_text=" p. 12 ",
        ),
        measurement_set=MeasurementSet(
            id="",
            taxon_treatment_id="",
            character="spore_size",
            data_kind="range",
            raw_text="(7–)8–10 × 5–6 µm",
            length_min=7,
            length_core_min=8,
            length_core_max=10,
            width_core_min=5,
            width_core_max=6,
        ),
        role="compared",
        note="quick note",
    )
    values.update(updates)
    return QuickAddReferenceRequest(**values)


def test_creates_new_hierarchy_and_canonical_attachment(libs):
    request = _request(libs)

    result = QuickAddReferenceService.create_and_attach(request)

    assert result.created_work is True
    assert result.created_treatment is True
    assert result.created_measurement_set is True
    assert result.created_attachment is True
    assert result.measurement_set.raw_text == request.measurement_set.raw_text
    assert result.treatment.locator_text == "p. 12"
    assert result.use.role == "compared"
    assert result.use.note == "quick note"
    snapshot = json.loads(result.use.snapshot_json)
    assert snapshot["reference_work_id"] == result.work.id
    assert snapshot["reference_treatment_id"] == result.treatment.id
    assert snapshot["reference_measurement_set_id"] == result.measurement_set.id
    assert snapshot["raw_text"] == request.measurement_set.raw_text


def test_reuses_explicit_work_and_exact_matching_treatment(libs):
    work = ReferenceWorkRepository.create(_work(id="work-existing"))
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="treatment-existing",
            reference_work_id=work.id,
            taxon_id="taxon-1",
            name_as_published="  Russula Example  ",
            locator_text="p. 12",
        )
    )
    request = _request(libs, existing_work_id=work.id)

    result = QuickAddReferenceService.create_and_attach(request)

    assert result.work.id == work.id
    assert result.treatment.id == treatment.id
    assert result.created_work is False
    assert result.created_treatment is False
    assert len(ReferenceWorkRepository.search()) == 1
    assert len(TaxonTreatmentRepository.list_for_work(work.id)) == 1
    assert len(MeasurementSetRepository.list_for_treatment(treatment.id)) == 1


def test_exact_doi_reuses_work_but_title_alone_never_fuzzy_merges(libs):
    existing = ReferenceWorkRepository.create(
        _work(id="doi-work", title="Original title", doi="10.1000/EXAMPLE")
    )
    doi_result = QuickAddReferenceService.create_and_attach(
        _request(libs, work=_work(title="Different title", doi="https://doi.org/10.1000/example"))
    )
    assert doi_result.work.id == existing.id
    assert doi_result.created_work is False

    other_observation = libs
    title_result = QuickAddReferenceService.create_and_attach(
        _request(other_observation, work=_work(title="Original title"))
    )
    assert title_result.work.id != existing.id
    assert title_result.created_work is True


def test_conflicting_doi_and_isbn_matches_fail_without_creating_records(libs):
    ReferenceWorkRepository.create(_work(id="doi-work", doi="10.1/one"))
    ReferenceWorkRepository.create(_work(id="isbn-work", isbn="978-1-23-456789-0"))
    request = _request(
        libs,
        work=_work(doi="10.1/one", isbn="9781234567890"),
    )

    with pytest.raises(ReferenceIntegrityError, match="different works"):
        QuickAddReferenceService.create_and_attach(request)

    assert len(ReferenceWorkRepository.search()) == 2
    assert ObservationReferenceUseRepository.list_for_observation(libs) == []


def test_incomplete_work_is_allowed(libs):
    result = QuickAddReferenceService.create_and_attach(
        _request(libs, work=_work(title="Minimal source", short_label=""))
    )
    assert result.work.title == "Minimal source"
    assert result.work.authors_json == "[]"
    assert result.work.year is None


def test_measurement_validation_failure_writes_nothing(libs):
    bad_set = _request(libs).measurement_set
    bad_set.data_kind = "not-a-kind"

    with pytest.raises(ValueError, match="data_kind"):
        QuickAddReferenceService.create_and_attach(
            _request(libs, measurement_set=bad_set)
        )

    assert ReferenceWorkRepository.search() == []
    assert MeasurementSetRepository.list_attachment_candidates() == []
    assert ObservationReferenceUseRepository.list_for_observation(libs) == []


def test_attachment_failure_compensates_only_new_records(libs, monkeypatch):
    existing = ReferenceWorkRepository.create(_work(id="keep-work"))
    request = _request(libs, existing_work_id=existing.id)

    def fail_attach(*args, **kwargs):
        raise RuntimeError("injected attach failure")

    monkeypatch.setattr(ObservationReferenceUseRepository, "attach_with_status", fail_attach)
    with pytest.raises(RuntimeError, match="injected"):
        QuickAddReferenceService.create_and_attach(request)

    assert ReferenceWorkRepository.get(existing.id) is not None
    assert TaxonTreatmentRepository.list_for_work(existing.id) == []
    assert MeasurementSetRepository.list_attachment_candidates() == []
    assert ObservationReferenceUseRepository.list_for_observation(libs) == []
