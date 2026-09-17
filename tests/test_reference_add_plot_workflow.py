"""End-to-end regression for the manual-reference / Add plot workflow.

This is the path behind the "Add plot" button in the reference panel: the
editor's validated output reaches ``QuickAddReferenceService.create_and_attach``
via ``ui/main_window.py``, which creates or reuses the work, treatment and
measurement set and attaches the result to an observation.

The workflow is covered here because the measurement content contract put a
write barrier on ``reference_measurement_sets`` (see
``database/reference_library_schema.py``). A connection that has not registered
``sporely_measurement_contract`` cannot compile an INSERT or UPDATE on that
table, which surfaces to the user as "Could not add the library reference: no
such function: sporely_measurement_contract". These tests pin the contract-aware
path working normally, and pin the closed-gate behavior: the legacy-only
projection is persisted and the frozen snapshot stays version 1.
"""
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
    ReferenceWork,
    TaxonTreatment,
)
from references import measurement_content_gates


@pytest.fixture()
def observation_id(tmp_path, monkeypatch):
    monkeypatch.setattr(_schema, "get_database_path", lambda: tmp_path / "mushrooms.db")
    monkeypatch.setattr(
        _schema, "get_reference_database_path", lambda: tmp_path / "reference_values.db"
    )
    monkeypatch.setattr(
        _schema, "get_bundled_reference_database_path", lambda: tmp_path / "missing.db"
    )
    _schema.init_database()
    conn = _schema.get_connection()
    try:
        created = conn.execute(
            "INSERT INTO observations (date, location) VALUES (?, ?)",
            ("2026-09-17", "Add plot workflow"),
        ).lastrowid
        conn.commit()
    finally:
        conn.close()
    return int(created)


def _request(observation_id: int) -> QuickAddReferenceRequest:
    """A manual spore-size reference, as the quick-add editor would submit it."""
    return QuickAddReferenceRequest(
        observation_id=observation_id,
        work=ReferenceWork(
            id="",
            type="book",
            title="Danmarks basidiesvampe",
            short_label="Danmarks basidiesvampe",
        ),
        treatment=TaxonTreatment(
            id="",
            reference_work_id="",
            name_as_published="Hebeloma crustuliniforme",
        ),
        measurement_set=MeasurementSet(
            id="",
            taxon_treatment_id="",
            character="spore_size",
            data_kind="range",
            raw_text="9.5-11.5 x 5.5-6.5 um",
            length_min=9.5,
            length_max=11.5,
            width_min=5.5,
            width_max=6.5,
        ),
        role="compared",
    )


def test_manual_reference_add_plot_creates_and_attaches(observation_id):
    """The whole hierarchy is created and attached against a contract-aware library."""
    result = QuickAddReferenceService.create_and_attach(_request(observation_id))

    assert result.created_work
    assert result.created_treatment
    assert result.created_measurement_set
    assert result.created_attachment
    assert result.work.title == "Danmarks basidiesvampe"

    stored = MeasurementSetRepository.get(result.measurement_set.id)
    assert stored is not None
    assert (stored.length_min, stored.length_max) == (9.5, 11.5)
    assert (stored.width_min, stored.width_max) == (5.5, 6.5)

    uses = ObservationReferenceUseRepository.list_for_observation(observation_id)
    assert [use.reference_measurement_set_id for use in uses] == [
        result.measurement_set.id
    ]


def test_add_plot_persists_legacy_projection_while_gate_closed(observation_id):
    """Under a closed reader gate the extension columns stay untouched."""
    assert measurement_content_gates.enhanced_editing_enabled() is False

    result = QuickAddReferenceService.create_and_attach(_request(observation_id))

    stored = MeasurementSetRepository.get(result.measurement_set.id)
    assert stored is not None
    assert stored.measurement_details_json is None
    assert stored.q_core_min is None
    assert stored.q_core_max is None


def test_add_plot_freezes_version_1_snapshot_while_gate_closed(observation_id):
    """No enhanced content may become frozen evidence while the reader gate is closed."""
    result = QuickAddReferenceService.create_and_attach(_request(observation_id))

    use = ObservationReferenceUseRepository.list_for_observation(observation_id)[0]
    snapshot = json.loads(use.snapshot_json)

    # A version 1 snapshot omits the key entirely; the contract defines a
    # missing version as version 1.
    assert snapshot.get("version") in (None, 1)
    assert "measurement_details" not in snapshot
    assert snapshot.get("q_core_min") is None
    assert snapshot.get("q_core_max") is None
    assert result.measurement_set.id == use.reference_measurement_set_id
