"""End-to-end regression: ReferenceAddDialog → normalized library →
observation attachment survives close/reopen.

Simulates the persistence chain that MainWindow's Add handler drives so
we can verify range and raw-points paths without spinning up the full
MainWindow. The test constructs a real ReferenceAddDialog, calls
:meth:`normalized_measurement_set_payload`, then persists via the same
repository calls the MainWindow helper uses. Reopen is simulated by
re-listing attachments via ObservationReferenceUseRepository.
"""
from __future__ import annotations

import json
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import sqlite3

import pytest
from PySide6.QtWidgets import QApplication, QTableWidgetItem, QWidget

from database import schema as _schema
from database.reference_library import (
    MeasurementSetRepository,
    ObservationReferenceUseRepository,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
)


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


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


def _make_observation(db_path, *, sporely_taxon_id: int | None = 7) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "INSERT INTO observations (date, location, sporely_taxon_id) VALUES (?, ?, ?)",
            ("2026-01-01", "Test", sporely_taxon_id),
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def _seed_work(short_label: str = "Petersen 1990") -> ReferenceWork:
    return ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="book",
            title=short_label,
            short_label=short_label,
            year=1990,
        )
    )


def _persist_like_mainwindow(dialog, *, work_id: str, taxon_id: int, obs_id: int):
    """Mirror MainWindow._persist_normalized_reference_from_dialog for
    the "enter new data" branch: resolve-or-create treatment, create the
    set, attach to the observation."""
    payload_ms = dialog.normalized_measurement_set_payload()
    assert payload_ms is not None, "dialog must produce a normalized payload"
    treatments = TaxonTreatmentRepository.list_for_work(work_id)
    matching = [
        t for t in treatments if str(getattr(t, "taxon_id", "") or "") == str(taxon_id)
    ]
    if matching:
        treatment = matching[0]
    else:
        treatment = TaxonTreatmentRepository.create(
            TaxonTreatment(
                id="",
                reference_work_id=work_id,
                taxon_id=str(taxon_id),
                name_as_published="Agaricus bisporus",
            )
        )
    payload_ms.taxon_treatment_id = treatment.id
    created = MeasurementSetRepository.create(payload_ms)
    ObservationReferenceUseRepository.attach(obs_id, created.id, role="compared")
    return created


def test_range_reference_persists_and_survives_reopen(qapp, libs):
    from ui.main_window import ReferenceAddDialog

    db_path, _ = libs
    obs_id = _make_observation(db_path, sporely_taxon_id=7)
    work = _seed_work()

    parent = QWidget()
    dialog = ReferenceAddDialog(
        parent,
        "Agaricus",
        "bisporus",
        observation_id=obs_id,
        sporely_taxon_id=7,
    )
    try:
        # Fill length row min/max + typical bounds + mean.
        for col, value in enumerate([5.5, 6.2, 7.0, 7.8, 8.5]):
            dialog.minmax_table.setItem(0, col, QTableWidgetItem(f"{value:.2f}"))
        # Fill width row min/max.
        dialog.minmax_table.setItem(1, 0, QTableWidgetItem("3.00"))
        dialog.minmax_table.setItem(1, 4, QTableWidgetItem("5.00"))
        created = _persist_like_mainwindow(
            dialog, work_id=work.id, taxon_id=7, obs_id=obs_id
        )
    finally:
        dialog.deleteLater()
        parent.deleteLater()

    # Simulate close/reopen: re-fetch attachments.
    listed = ObservationReferenceUseRepository.list_for_observation(obs_id)
    assert len(listed) == 1
    stored = listed[0]
    assert stored.reference_measurement_set_id == created.id
    assert stored.role == "compared"
    snapshot = json.loads(stored.snapshot_json)
    measurements = snapshot["measurements"]
    assert measurements["length_min"] == pytest.approx(5.5)
    assert measurements["length_max"] == pytest.approx(8.5)
    assert measurements["length_core_min"] == pytest.approx(6.2)
    assert measurements["length_core_max"] == pytest.approx(7.8)


def test_raw_points_reference_persists_and_survives_reopen(qapp, libs):
    from ui.main_window import ReferenceAddDialog

    db_path, _ = libs
    obs_id = _make_observation(db_path, sporely_taxon_id=11)
    work = _seed_work("Kalamees 2005")

    parent = QWidget()
    dialog = ReferenceAddDialog(
        parent,
        "Agaricus",
        "bisporus",
        observation_id=obs_id,
        sporely_taxon_id=11,
    )
    try:
        dialog.spore_table._ensure_rows(3)
        for row, (length, width) in enumerate([(6.0, 3.0), (7.0, 3.5), (7.5, 3.6)]):
            dialog.spore_table.setItem(row, 0, QTableWidgetItem(f"{length:.2f}"))
            dialog.spore_table.setItem(row, 1, QTableWidgetItem(f"{width:.2f}"))
        created = _persist_like_mainwindow(
            dialog, work_id=work.id, taxon_id=11, obs_id=obs_id
        )
        assert created.data_kind == "raw_points"
    finally:
        dialog.deleteLater()
        parent.deleteLater()

    listed = ObservationReferenceUseRepository.list_for_observation(obs_id)
    assert len(listed) == 1
    stored = listed[0]
    snapshot = json.loads(stored.snapshot_json)
    assert snapshot["data_kind"] == "raw_points"
    points = snapshot.get("raw_points") or snapshot.get("points") or []
    if not points:
        # Older snapshot shape stores JSON string under raw_points_json.
        points = json.loads(snapshot.get("raw_points_json") or "[]")
    assert len(points) == 3


def test_treatment_reuse_across_two_sets_on_same_work_and_taxon(qapp, libs):
    from ui.main_window import ReferenceAddDialog

    db_path, _ = libs
    obs_id = _make_observation(db_path, sporely_taxon_id=7)
    work = _seed_work("Reuse 2001")

    def _add(value_min: float, value_max: float) -> str:
        parent = QWidget()
        dialog = ReferenceAddDialog(
            parent,
            "Agaricus",
            "bisporus",
            observation_id=obs_id,
            sporely_taxon_id=7,
        )
        try:
            dialog.minmax_table.setItem(0, 0, QTableWidgetItem(f"{value_min:.2f}"))
            dialog.minmax_table.setItem(0, 4, QTableWidgetItem(f"{value_max:.2f}"))
            # Both dimensions must have at least one bound for the
            # payload to qualify as a plottable range.
            dialog.minmax_table.setItem(1, 0, QTableWidgetItem("3.00"))
            dialog.minmax_table.setItem(1, 4, QTableWidgetItem("5.00"))
            created = _persist_like_mainwindow(
                dialog, work_id=work.id, taxon_id=7, obs_id=obs_id
            )
            return created.taxon_treatment_id
        finally:
            dialog.deleteLater()
            parent.deleteLater()

    treatment_id_1 = _add(6.0, 8.0)
    treatment_id_2 = _add(7.0, 9.0)
    # Same (work, taxon) key → the second add must reuse the treatment
    # rather than creating a duplicate.
    assert treatment_id_1 == treatment_id_2
    treatments = TaxonTreatmentRepository.list_for_work(work.id)
    matching = [t for t in treatments if str(t.taxon_id or "") == "7"]
    assert len(matching) == 1
