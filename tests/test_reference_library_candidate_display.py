"""Stage 1: the chooser list carries its own display semantics.

``MeasurementSetRepository.list_attachment_candidates`` joins the scientific
content columns into the query it already runs, so a Library row arrives with
its :class:`~references.reference_display.SourceDisplay` attached. The
regression these tests guard is an N+1: a chooser that loads the list and then
fetches each visible row again to find out whether to print ``Raw data`` or
``5–95% range``.
"""
from __future__ import annotations

import json

import pytest

from database import reference_library
from database import schema as _schema
from database.reference_library import (
    MeasurementSet,
    MeasurementSetCandidate,
    MeasurementSetRepository,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
)
from references.reference_display import DataLabel


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


def _seed(label: str, **fields) -> MeasurementSet:
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
    payload = {
        "character": "spore_size",
        "data_kind": "range",
        "length_core_min": 8.0,
        "length_core_max": 10.0,
        "width_core_min": 5.0,
        "width_core_max": 6.0,
    }
    payload.update(fields)
    return MeasurementSetRepository.create(
        MeasurementSet(id="", taxon_treatment_id=treatment.id, **payload)
    )


def _candidate(label: str) -> MeasurementSetCandidate:
    candidates = MeasurementSetRepository.list_attachment_candidates()
    match = [c for c in candidates if c.short_label == label]
    assert len(match) == 1, f"expected exactly one {label} candidate"
    return match[0]


def test_candidate_carries_the_projection_for_a_plain_published_range(libs):
    _seed("Alpha")

    display = _candidate("Alpha").source_display()

    assert display.source_kind == "library"
    assert display.data_label == DataLabel(kind="published_range")
    assert display.metric("length").core_range == (8.0, 10.0)
    assert display.metric("width").core_range == (5.0, 6.0)
    # A library row holds what an author published, never a cloud aggregate.
    assert display.statistics_origin == "reported"


def test_candidate_projection_marks_a_published_mean_as_reported(libs):
    _seed("Epsilon", length_mean=9.4, sample_size=30)

    display = _candidate("Epsilon").source_display()

    assert display.has_reported_statistic
    assert not display.has_computed_statistic
    assert display.sample_size == 30
    assert display.sample_size_is_reported


def test_candidate_projection_reads_stored_percentile_bounds(libs):
    _seed(
        "Beta",
        length_min=7.0,
        length_max=12.0,
        measurement_details_json=json.dumps(
            {
                "schema_version": 1,
                "metrics": {
                    "length": {
                        "outer_range": {"kind": "reported_extremes"},
                        "core_range": {
                            "kind": "percentile_interval",
                            "percentile_bounds": [5, 95],
                        },
                    }
                },
            }
        ),
    )

    display = _candidate("Beta").source_display()

    assert display.data_label == DataLabel(kind="percentile_range", percentile_bounds=(5, 95))
    assert display.metric("length").outer_range == (7.0, 12.0)
    assert display.has_reported_descriptor


def test_candidate_projection_counts_stored_raw_points(libs):
    points = [{"length": 9.0, "width": 5.1}, {"length": 9.6, "width": 5.4}]
    _seed(
        "Gamma",
        data_kind="raw_points",
        raw_points_json=json.dumps(points),
        sample_size=2,
    )

    display = _candidate("Gamma").source_display()

    assert display.data_label == DataLabel(kind="raw_data")
    assert display.raw_point_count == 2


def test_candidate_projection_does_not_trust_data_kind_alone(libs):
    _seed("Delta", data_kind="raw_points", raw_points_json=None)

    display = _candidate("Delta").source_display()

    assert display.stored_data_kind == "raw_points"
    assert not display.has_raw_points
    assert display.data_label == DataLabel(kind="published_range")


def test_listing_candidates_needs_no_query_per_row(libs, monkeypatch):
    for label in ("Alpha", "Beta", "Gamma"):
        _seed(label)

    executed: list[str] = []
    real_connect = reference_library._connect_reference

    def _tracing_connect():
        conn = real_connect()
        conn.set_trace_callback(executed.append)
        return conn

    monkeypatch.setattr(reference_library, "_connect_reference", _tracing_connect)
    candidates = MeasurementSetRepository.list_attachment_candidates()

    assert len(candidates) == 3
    assert all(c.display is not None for c in candidates)
    selects = [sql for sql in executed if "reference_measurement_sets" in sql]
    assert len(selects) == 1, "one joined query must serve every row's semantics"


def test_hand_built_candidate_falls_back_to_an_empty_projection():
    candidate = MeasurementSetCandidate(
        measurement_set_id="x",
        short_label="Scenario",
        name_as_published="Russula paludosa",
        locator_text=None,
        data_kind="range",
        raw_text="8-10 x 5-6 um",
        revision=1,
        reference_work_id="w",
        reference_treatment_id="t",
    )

    display = candidate.source_display()

    assert display.data_label == DataLabel(kind="none")
    assert display.stored_data_kind == "range"
    assert not display.has_raw_points
