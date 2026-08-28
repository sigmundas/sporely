"""Contracts for explicit adoption of successor measurement sets."""
from __future__ import annotations

import json
import sqlite3

import pytest

from database import schema as _schema
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
from references.reference_plotting import translate_observation_reference_use


@pytest.fixture()
def libs(tmp_path, monkeypatch):
    db_path = tmp_path / "mushrooms.db"
    ref_path = tmp_path / "reference_values.db"
    monkeypatch.setattr(_schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(_schema, "get_reference_database_path", lambda: ref_path)
    monkeypatch.setattr(
        _schema,
        "get_bundled_reference_database_path",
        lambda: tmp_path / "does-not-exist.db",
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
            title="Source",
            short_label="Author 2000",
            authors_json=json.dumps([{"family": "Author"}]),
            year=2000,
        )
    )
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="",
            reference_work_id=work.id,
            name_as_published="Russula originalis",
            locator_text="p. 10",
        )
    )
    original = MeasurementSetRepository.create(
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
        int(observation_id), original.id, role="contradicts", note="keep me"
    )
    return int(observation_id), original, use


def test_direct_successor_is_reported_without_changing_attachment(libs):
    _, original, use = _seed_attached(libs)
    successor = MeasurementSetRepository.create_revision(
        original.id, {"raw_text": "8–11 × 5–6 µm", "length_core_max": 11.0}
    )

    status = ObservationReferenceUseRepository.successor_status(use.id)
    unchanged = ObservationReferenceUseRepository.get(use.id)

    assert status.state == "successor_available"
    assert status.path_ids == (original.id, successor.id)
    assert status.successor_id == successor.id
    assert unchanged == use


def test_successor_chain_resolves_deterministically_to_terminal_set(libs):
    _, original, use = _seed_attached(libs)
    middle = MeasurementSetRepository.create_revision(
        original.id, {"raw_text": "8–11 × 5–6 µm"}
    )
    terminal = MeasurementSetRepository.create_revision(
        middle.id, {"raw_text": "9–11 × 5–6 µm"}
    )

    status = ObservationReferenceUseRepository.successor_status(use.id)

    assert status.state == "successor_available"
    assert status.path_ids == (original.id, middle.id, terminal.id)
    assert status.successor_id == terminal.id


def test_no_successor_is_current_lineage(libs):
    _, original, use = _seed_attached(libs)

    status = ObservationReferenceUseRepository.successor_status(use.id)

    assert status.state == "no_successor"
    assert status.path_ids == (original.id,)
    assert status.successor_id is None


def test_missing_attached_source_is_not_adoptable(libs):
    _, original, use = _seed_attached(libs)
    _, ref_path = libs
    with sqlite3.connect(ref_path) as conn:
        conn.execute(
            "DELETE FROM reference_measurement_sets WHERE id = ?", (original.id,)
        )

    status = ObservationReferenceUseRepository.successor_status(use.id)

    assert status.state == "source_missing"
    with pytest.raises(ReferenceIntegrityError, match="not adoptable"):
        ObservationReferenceUseRepository.adopt_successor(use.id)


def test_successor_cycle_fails_closed(libs):
    _, original, use = _seed_attached(libs)
    successor = MeasurementSetRepository.create_revision(original.id, {})
    MeasurementSetRepository.update(
        original.id, {"supersedes_id": successor.id}, bump_revision=False
    )

    status = ObservationReferenceUseRepository.successor_status(use.id)

    assert status.state == "cycle"
    assert status.successor_id is None
    with pytest.raises(ReferenceIntegrityError, match="not adoptable"):
        ObservationReferenceUseRepository.adopt_successor(use.id)


def test_successor_with_missing_parent_link_fails_closed(libs):
    _, original, use = _seed_attached(libs)
    successor = MeasurementSetRepository.create_revision(original.id, {})
    _, ref_path = libs
    with sqlite3.connect(ref_path) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            "UPDATE reference_measurement_sets SET taxon_treatment_id = ? WHERE id = ?",
            ("missing-treatment", successor.id),
        )

    status = ObservationReferenceUseRepository.successor_status(use.id)

    assert status.state == "broken"
    assert status.successor_id is None
    with pytest.raises(ReferenceIntegrityError, match="not adoptable"):
        ObservationReferenceUseRepository.adopt_successor(use.id)


def test_successor_chain_with_broken_intermediate_fails_closed(libs):
    _, original, use = _seed_attached(libs)
    middle = MeasurementSetRepository.create_revision(original.id, {})
    MeasurementSetRepository.create_revision(middle.id, {})
    _, ref_path = libs
    with sqlite3.connect(ref_path) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            "UPDATE reference_measurement_sets SET taxon_treatment_id = ? WHERE id = ?",
            ("missing-treatment", middle.id),
        )

    status = ObservationReferenceUseRepository.successor_status(use.id)

    assert status.state == "broken"
    assert status.successor_id is None


def test_successor_chain_with_broken_attached_source_fails_closed(libs):
    _, original, use = _seed_attached(libs)
    MeasurementSetRepository.create_revision(original.id, {})
    _, ref_path = libs
    with sqlite3.connect(ref_path) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            "UPDATE reference_measurement_sets SET taxon_treatment_id = ? WHERE id = ?",
            ("missing-treatment", original.id),
        )

    status = ObservationReferenceUseRepository.successor_status(use.id)

    assert status.state == "broken"
    assert status.successor_id is None


def test_unplottable_successor_is_not_adoptable(libs):
    _, original, use = _seed_attached(libs)
    MeasurementSetRepository.create_revision(
        original.id, {"data_kind": "parmasto", "raw_text": "Parmasto summary"}
    )

    status = ObservationReferenceUseRepository.successor_status(use.id)

    assert status.state == "unsupported"
    with pytest.raises(ReferenceIntegrityError, match="not adoptable"):
        ObservationReferenceUseRepository.adopt_successor(use.id)


def test_successor_fork_fails_closed_with_sorted_candidates(libs):
    _, original, use = _seed_attached(libs)
    first = MeasurementSetRepository.create_revision(original.id, {})
    second = MeasurementSetRepository.create_revision(original.id, {})

    status = ObservationReferenceUseRepository.successor_status(use.id)

    assert status.state == "fork"
    assert status.fork_successor_ids == tuple(sorted((first.id, second.id)))
    assert status.successor_id is None


def test_explicit_adoption_retargets_same_use_and_reopens_with_successor_plot(libs):
    observation_id, original, use = _seed_attached(libs)
    successor = MeasurementSetRepository.create_revision(
        original.id,
        {"raw_text": "9–11 × 5–6 µm", "length_core_min": 9.0, "length_core_max": 11.0},
    )

    adopted = ObservationReferenceUseRepository.adopt_successor(
        use.id, expected_successor_id=successor.id
    )
    reopened = ObservationReferenceUseRepository.list_for_observation(observation_id)
    plotted = translate_observation_reference_use(reopened[0])

    assert adopted.id == use.id
    assert adopted.observation_id == observation_id
    assert adopted.reference_measurement_set_id == successor.id
    assert adopted.role == "contradicts"
    assert adopted.note == "keep me"
    assert adopted.selected_at == use.selected_at
    assert len(reopened) == 1
    assert plotted is not None
    assert plotted["data"]["length_p05"] == 9.0
    assert plotted["data"]["length_p95"] == 11.0
    assert plotted["data"]["raw_text"] == "9–11 × 5–6 µm"


def test_adoption_refuses_target_already_attached_to_observation(libs):
    observation_id, original, use = _seed_attached(libs)
    successor = MeasurementSetRepository.create_revision(original.id, {})
    second_use = ObservationReferenceUseRepository.attach(observation_id, successor.id)

    with pytest.raises(ReferenceIntegrityError, match="already attached"):
        ObservationReferenceUseRepository.adopt_successor(use.id)

    assert ObservationReferenceUseRepository.get(use.id) == use
    assert ObservationReferenceUseRepository.get(second_use.id) == second_use


def test_adoption_refuses_same_successor_content_changed_after_review(libs):
    _, original, use = _seed_attached(libs)
    successor = MeasurementSetRepository.create_revision(original.id, {})
    reviewed = ObservationReferenceUseRepository.successor_status(use.id)
    MeasurementSetRepository.update(
        successor.id,
        {"raw_text": "9–12 × 5–6 µm", "length_core_max": 12.0},
    )

    with pytest.raises(ReferenceIntegrityError, match="changed since review"):
        ObservationReferenceUseRepository.adopt_successor(
            use.id,
            expected_successor_id=successor.id,
            expected_successor_snapshot_json=reviewed.successor_snapshot_json,
        )

    assert ObservationReferenceUseRepository.get(use.id) == use


def test_adoption_allows_revision_only_successor_save_after_review(libs):
    _, original, use = _seed_attached(libs)
    successor = MeasurementSetRepository.create_revision(original.id, {})
    reviewed = ObservationReferenceUseRepository.successor_status(use.id)
    MeasurementSetRepository.update(successor.id, {})

    adopted = ObservationReferenceUseRepository.adopt_successor(
        use.id,
        expected_successor_id=successor.id,
        expected_successor_snapshot_json=reviewed.successor_snapshot_json,
    )

    assert adopted.id == use.id
    assert adopted.reference_measurement_set_id == successor.id
