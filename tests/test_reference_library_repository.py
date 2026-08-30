"""Repository/service layer tests for the normalized reference library."""
from __future__ import annotations

import json

import pytest

from database import schema as _schema
from database.reference_library import (
    MeasurementSet,
    MeasurementSetRepository,
    ObservationReferenceUseRepository,
    ReferenceInUseError,
    ReferenceIntegrityError,
    ReferenceValidationError,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
    normalize_doi,
    normalize_isbn,
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


def _make_work(**overrides) -> ReferenceWork:
    defaults = dict(
        id="",
        type="book",
        title="Danmarks Basidiesvampe",
        short_label="Petersen et al. 1990",
        authors_json=json.dumps(
            [{"family": "Petersen", "given": "J. H."}, {"family": "Læssøe"}]
        ),
        year=1990,
        publisher="Foreningen til Svampekundskabens Fremme",
        place="Copenhagen",
    )
    defaults.update(overrides)
    return ReferenceWork(**defaults)


def test_normalize_doi_and_isbn():
    assert normalize_doi("https://doi.org/10.1234/ABC") == "10.1234/abc"
    assert normalize_doi(" 10.1234/ABC ") == "10.1234/abc"
    assert normalize_doi("") is None
    assert normalize_doi(None) is None
    assert normalize_isbn("978-0-306-40615-7") == "9780306406157"
    assert normalize_isbn("0-306-40615-2") == "0306406152"
    assert normalize_isbn(None) is None


def test_reference_work_create_get_update_bumps_revision(libs):
    work = ReferenceWorkRepository.create(_make_work())
    assert work.id
    assert work.revision == 1
    assert work.doi is None
    fetched = ReferenceWorkRepository.get(work.id)
    assert fetched is not None
    assert fetched.title == "Danmarks Basidiesvampe"
    # Nullable fields stay null (not zero).
    assert fetched.container_title is None

    updated = ReferenceWorkRepository.update(work.id, {"year": 1991})
    assert updated.revision == 2
    assert updated.year == 1991


def test_reference_work_stable_uuid(libs):
    work = ReferenceWorkRepository.create(_make_work())
    assert work.id
    # Round-trip preserves UUID.
    fetched = ReferenceWorkRepository.get(work.id)
    assert fetched.id == work.id


def test_reference_work_normalizes_and_detects_duplicate_doi_without_merging(libs):
    first = ReferenceWorkRepository.create(_make_work(doi="10.9999/xyz"))
    duplicate = _make_work(doi="https://doi.org/10.9999/XYZ", title="Other")
    second = ReferenceWorkRepository.create(duplicate)
    assert second.id != first.id
    found = ReferenceWorkRepository.find_by_doi("HTTPS://DOI.ORG/10.9999/xyz")
    assert found is not None


def test_reference_work_normalizes_and_detects_duplicate_isbn_without_merging(libs):
    first = ReferenceWorkRepository.create(_make_work(isbn="978-0-306-40615-7"))
    second = ReferenceWorkRepository.create(
        _make_work(isbn="9780306406157", title="Reprint")
    )
    assert second.id != first.id
    found = ReferenceWorkRepository.find_by_isbn("978 0 306 40615 7")
    assert found is not None


def test_reference_work_title_similarity_does_not_dedupe(libs):
    a = ReferenceWorkRepository.create(_make_work(title="Nordic Macromycetes Vol I"))
    b = ReferenceWorkRepository.create(_make_work(title="Nordic Macromycetes Vol I."))
    assert a.id != b.id


def test_reference_work_invalid_enum(libs):
    with pytest.raises(ReferenceValidationError):
        ReferenceWorkRepository.create(_make_work(type="not_a_type"))
    work = ReferenceWorkRepository.create(_make_work())
    with pytest.raises(ReferenceValidationError):
        ReferenceWorkRepository.update(work.id, {"visibility": "banned"})


def test_two_treatments_reuse_one_work(libs):
    work = ReferenceWorkRepository.create(_make_work())
    t1 = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="",
            reference_work_id=work.id,
            name_as_published="Russula paludosa",
            locator_text="p. 214",
        )
    )
    t2 = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="",
            reference_work_id=work.id,
            name_as_published="Russula claroflava",
        )
    )
    assert t1.id != t2.id
    for_work = TaxonTreatmentRepository.list_for_work(work.id)
    assert {t.id for t in for_work} == {t1.id, t2.id}


def test_measurement_sets_preserve_bounds_and_raw_text(libs):
    work = ReferenceWorkRepository.create(_make_work())
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="",
            reference_work_id=work.id,
            name_as_published="Russula paludosa",
        )
    )
    raw = "(7.5–)8–10(–10.5) × 5–6(–6.5) µm"
    ms = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="range",
            raw_text=raw,
            length_min=7.5,
            length_core_min=8.0,
            length_core_max=10.0,
            length_max=10.5,
            width_core_min=5.0,
            width_core_max=6.0,
            width_max=6.5,
        )
    )
    fetched = MeasurementSetRepository.get(ms.id)
    assert fetched.raw_text == raw
    assert fetched.length_min == 7.5
    assert fetched.length_max == 10.5
    assert fetched.width_min is None  # not supplied — stays null, not zero


def test_measurement_set_invalid_data_kind(libs):
    work = ReferenceWorkRepository.create(_make_work())
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="",
            reference_work_id=work.id,
            name_as_published="Russula paludosa",
        )
    )
    with pytest.raises(ReferenceValidationError):
        MeasurementSetRepository.create(
            MeasurementSet(
                id="",
                taxon_treatment_id=treatment.id,
                character="spore_size",
                data_kind="mystery",
            )
        )


def test_measurement_set_raw_points_roundtrip_and_no_synthesis(libs):
    work = ReferenceWorkRepository.create(_make_work())
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="",
            reference_work_id=work.id,
            name_as_published="Russula paludosa",
        )
    )
    points = [
        {"length": 9.0, "width": 5.5},
        {"length": 9.5, "width": 5.7},
        {"length": 10.0, "width": 5.9},
    ]
    ms = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="raw_points",
            raw_points_json=json.dumps(points),
        )
    )
    fetched = MeasurementSetRepository.get(ms.id)
    assert json.loads(fetched.raw_points_json) == points

    with pytest.raises(ReferenceValidationError):
        MeasurementSetRepository.create(
            MeasurementSet(
                id="",
                taxon_treatment_id=treatment.id,
                character="spore_size",
                data_kind="raw_points",
                raw_points_json="[]",
            )
        )
    # A summary set never invents raw points.
    summary = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="summary",
            length_min=6.0,
            length_max=8.0,
        )
    )
    assert summary.raw_points_json is None


def test_work_search_includes_year_doi_and_isbn(libs):
    work = ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="book",
            title="Distinct source",
            short_label="Author",
            year=1987,
            doi="10.1234/example",
            isbn="978-1-4028-9462-6",
        )
    )

    assert [item.id for item in ReferenceWorkRepository.search("1987")] == [work.id]
    assert [item.id for item in ReferenceWorkRepository.search("10.1234/example")] == [work.id]
    assert [item.id for item in ReferenceWorkRepository.search("9781402894626")] == [work.id]


def test_name_as_published_is_independent_of_taxon_id(libs):
    work = ReferenceWorkRepository.create(_make_work())
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="",
            reference_work_id=work.id,
            taxon_id="taxon-42",
            name_as_published="Russula paludosa",
        )
    )
    updated = TaxonTreatmentRepository.update(treatment.id, {"taxon_id": "taxon-99"})
    assert updated.name_as_published == "Russula paludosa"
    assert updated.taxon_id == "taxon-99"


def _make_observation(db_path):
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "INSERT INTO observations (date, location) VALUES (?, ?)",
            ("2026-01-01", "Test"),
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def _build_ms(libs):
    work = ReferenceWorkRepository.create(_make_work())
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="",
            reference_work_id=work.id,
            name_as_published="Russula paludosa",
            locator_text="p. 214",
        )
    )
    ms = MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=treatment.id,
            character="spore_size",
            data_kind="range",
            raw_text="(7.5–)8–10(–10.5) × 5–6(–6.5) µm",
            length_min=7.5,
            length_core_min=8.0,
            length_core_max=10.0,
            length_max=10.5,
        )
    )
    return work, treatment, ms


def test_observation_use_attach_list_update_detach(libs):
    db_path, _ = libs
    obs_id = _make_observation(db_path)
    _, _, ms = _build_ms(libs)

    use = ObservationReferenceUseRepository.attach(obs_id, ms.id, role="compared")
    assert use.reference_revision == ms.revision
    assert use.snapshot_json

    listed = ObservationReferenceUseRepository.list_for_observation(obs_id)
    assert len(listed) == 1
    assert listed[0].id == use.id

    updated = ObservationReferenceUseRepository.update(
        use.id, role="supports_identification", note="Matches well"
    )
    assert updated.role == "supports_identification"
    assert updated.note == "Matches well"

    ObservationReferenceUseRepository.detach(use.id)
    assert ObservationReferenceUseRepository.list_for_observation(obs_id) == []

    # Detach must not delete the library record.
    assert MeasurementSetRepository.get(ms.id) is not None


def test_observation_use_duplicate_attach_returns_existing(libs):
    db_path, _ = libs
    obs_id = _make_observation(db_path)
    _, _, ms = _build_ms(libs)
    first = ObservationReferenceUseRepository.attach(obs_id, ms.id)
    second = ObservationReferenceUseRepository.attach(obs_id, ms.id)
    assert first.id == second.id
    assert len(ObservationReferenceUseRepository.list_for_observation(obs_id)) == 1


def test_observation_use_role_validation(libs):
    db_path, _ = libs
    obs_id = _make_observation(db_path)
    _, _, ms = _build_ms(libs)
    with pytest.raises(ReferenceValidationError):
        ObservationReferenceUseRepository.attach(obs_id, ms.id, role="nope")


def test_snapshot_survives_when_library_record_removed_after_dangling_marker(libs):
    """Even though normal delete is blocked, the stored snapshot itself
    remains readable after we forcibly clear the reference library."""
    db_path, ref_path = libs
    obs_id = _make_observation(db_path)
    _, _, ms = _build_ms(libs)
    use = ObservationReferenceUseRepository.attach(obs_id, ms.id)

    # Simulate a lost/re-created library DB by wiping the source rows directly.
    import sqlite3

    conn = sqlite3.connect(ref_path)
    try:
        conn.execute("DELETE FROM reference_measurement_sets")
        conn.execute("DELETE FROM reference_taxon_treatments")
        conn.execute("DELETE FROM reference_works")
        conn.commit()
    finally:
        conn.close()

    reloaded = ObservationReferenceUseRepository.list_for_observation(obs_id)
    assert len(reloaded) == 1
    snapshot = json.loads(reloaded[0].snapshot_json)
    assert snapshot["short_label"]
    assert snapshot["reference_measurement_set_id"] == ms.id
    dangling = ObservationReferenceUseRepository.find_dangling_measurement_set_ids()
    assert dangling == [use.reference_measurement_set_id]


def test_missing_measurement_set_detection_on_attach(libs):
    db_path, _ = libs
    obs_id = _make_observation(db_path)
    with pytest.raises(ReferenceIntegrityError):
        ObservationReferenceUseRepository.attach(obs_id, "non-existent-uuid")


def test_dangling_attach_only_via_helper_mode(libs):
    db_path, _ = libs
    obs_id = _make_observation(db_path)
    use = ObservationReferenceUseRepository.attach(
        obs_id,
        "no-such-set",
        allow_dangling=True,
    )
    assert use.reference_measurement_set_id == "no-such-set"
    assert ObservationReferenceUseRepository.find_dangling_measurement_set_ids() == [
        "no-such-set"
    ]


def test_delete_measurement_set_blocked_when_in_use(libs):
    db_path, _ = libs
    obs_id = _make_observation(db_path)
    work, treatment, ms = _build_ms(libs)
    ObservationReferenceUseRepository.attach(obs_id, ms.id)
    with pytest.raises(ReferenceInUseError):
        MeasurementSetRepository.delete(ms.id)
    # Delete of parent work is likewise blocked.
    with pytest.raises(ReferenceInUseError):
        ReferenceWorkRepository.delete(work.id)


def test_delete_measurement_set_when_not_in_use(libs):
    _, _, ms = _build_ms(libs)
    MeasurementSetRepository.delete(ms.id)
    assert MeasurementSetRepository.get(ms.id) is None


def test_missing_observation_rejected(libs):
    _, _, ms = _build_ms(libs)
    with pytest.raises(ReferenceIntegrityError):
        ObservationReferenceUseRepository.attach(9999, ms.id)


def test_measurement_set_revision_supersedes(libs):
    _, _, ms = _build_ms(libs)
    successor = MeasurementSetRepository.create_revision(
        ms.id, {"length_max": 11.0}
    )
    assert successor.supersedes_id == ms.id
    assert successor.revision == ms.revision + 1
    assert successor.length_max == 11.0
