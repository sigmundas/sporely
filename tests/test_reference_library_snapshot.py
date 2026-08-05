"""Citation and snapshot service tests."""
from __future__ import annotations

import json

import pytest

from database.reference_citation import (
    SNAPSHOT_SCHEMA_VERSION,
    build_full_citation,
    build_observation_reference_snapshot,
    build_short_label,
    serialize_snapshot,
)
from database.reference_library import (
    MeasurementSet,
    ReferenceWork,
    TaxonTreatment,
)


def _make_work(**overrides) -> ReferenceWork:
    defaults = dict(
        id="work-1",
        type="book",
        title="Danmarks Basidiesvampe",
        short_label="",
        authors_json=json.dumps(
            [
                {"family": "Petersen", "given": "J. H."},
                {"family": "Læssøe"},
                {"family": "Vesterholt"},
            ]
        ),
        year=1990,
        publisher="Foreningen til Svampekundskabens Fremme",
        place="Copenhagen",
    )
    defaults.update(overrides)
    return ReferenceWork(**defaults)


def _make_bundle(**work_overrides):
    work = _make_work(**work_overrides)
    treatment = TaxonTreatment(
        id="treat-1",
        reference_work_id=work.id,
        taxon_id="taxon-42",
        name_as_published="Russula paludosa",
        locator_text="p. 214",
        page_from=214,
    )
    ms = MeasurementSet(
        id="set-1",
        taxon_treatment_id=treatment.id,
        character="spore_size",
        data_kind="range",
        raw_text="(7.5–)8–10(–10.5) × 5–6(–6.5) µm",
        length_min=7.5,
        length_core_min=8.0,
        length_core_max=10.0,
        length_max=10.5,
        width_core_min=5.0,
        width_core_max=6.0,
        width_max=6.5,
        revision=3,
    )
    return work, treatment, ms


def test_short_label_uses_stored_value_when_present():
    work = _make_work(short_label="Petersen 1990")
    assert build_short_label(work) == "Petersen 1990"


def test_short_label_derives_from_authors_and_year():
    work = _make_work()
    assert build_short_label(work) == "Petersen et al. 1990"


def test_full_citation_uses_override_when_present():
    work = _make_work(citation_override="Custom Citation (2020).")
    assert build_full_citation(work) == "Custom Citation (2020)."


def test_full_citation_is_generated_from_structured_fields():
    work = _make_work()
    citation = build_full_citation(work)
    assert "Petersen" in citation
    assert "(1990)" in citation
    assert "Danmarks Basidiesvampe" in citation
    assert "Copenhagen" in citation


def test_full_citation_omits_missing_pieces_without_fabricating():
    work = _make_work(year=None, publisher=None, place=None)
    citation = build_full_citation(work)
    assert "1990" not in citation
    assert "Foreningen" not in citation
    assert "Copenhagen" not in citation


def test_snapshot_schema_shape_and_keys():
    work, treatment, ms = _make_bundle()
    snapshot = build_observation_reference_snapshot(work, treatment, ms)
    assert snapshot["schema_version"] == SNAPSHOT_SCHEMA_VERSION == 1
    assert snapshot["reference_work_id"] == work.id
    assert snapshot["reference_measurement_set_id"] == ms.id
    assert snapshot["reference_revision"] == ms.revision == 3
    assert snapshot["short_label"] == "Petersen et al. 1990"
    assert snapshot["name_as_published"] == "Russula paludosa"
    assert snapshot["locator_text"] == "p. 214"
    assert snapshot["character"] == "spore_size"
    assert snapshot["data_kind"] == "range"
    assert snapshot["raw_text"] == ms.raw_text
    m = snapshot["measurements"]
    assert m["length_min"] == 7.5
    assert m["length_core_max"] == 10.0
    assert m["width_min"] is None
    assert m["sample_size"] is None


def test_snapshot_is_deterministic_and_json_serializable():
    work, treatment, ms = _make_bundle()
    a = serialize_snapshot(build_observation_reference_snapshot(work, treatment, ms))
    b = serialize_snapshot(build_observation_reference_snapshot(work, treatment, ms))
    assert a == b
    # Round trip.
    parsed = json.loads(a)
    assert parsed["schema_version"] == 1


def test_snapshot_excludes_private_owner_information():
    work, treatment, ms = _make_bundle(owner_id="user-42")
    snapshot = build_observation_reference_snapshot(work, treatment, ms)
    assert "owner_id" not in snapshot


def test_snapshot_rejects_mismatched_parents():
    work, treatment, ms = _make_bundle()
    bad_treatment = TaxonTreatment(
        id="other",
        reference_work_id=work.id,
        name_as_published="Other",
    )
    with pytest.raises(ValueError):
        build_observation_reference_snapshot(work, bad_treatment, ms)
