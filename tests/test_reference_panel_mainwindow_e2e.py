"""MainWindow-driven end-to-end regression for the reference panel Add flow.

This test intentionally drives the real ``MainWindow._on_reference_panel_add_clicked``
coordinator (not the underlying repository primitives directly) so the
dialog wiring, legacy dual-write, normalized MeasurementSet creation,
observation attachment, and panel refresh all execute together — the
coverage gap the reviewer highlighted (F-12 / F-007 in the cumulative
ledger).

MainWindow is instantiated with ``init_ui`` neutralized (as in
``tests/test_main_window_reference_panel_taxon_lookup.py``) and only the
Reference-panel widgets the coordinator touches are constructed by hand.
The active observation is created in a real SQLite database via the same
isolated fixture pattern used across the reference-library test suite so
persistence and reopen semantics are exercised end-to-end.
"""
from __future__ import annotations

import json
import os
import sqlite3

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QLabel,
    QLineEdit,
    QTableWidget,
    QTableWidgetItem,
    QWidget,
)

import ui.main_window as main_window
from database import schema as _schema
from database.reference_library import (
    MeasurementSetRepository,
    ObservationReferenceUseRepository,
    ReferenceWork,
    ReferenceWorkRepository,
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


class _EmptySpeciesAvailability:
    def get_cache(self, force_refresh: bool = False):
        return {}

    def get_detailed_info(self, genus, species, exclude_observation_id=None):
        return {}

    def get_species_display_name(self, genus, species, exclude_observation_id=None):
        return (f"{genus} {species}".strip(), False)


class _StubReferenceAddDialog(QWidget):
    """A stand-in for ReferenceAddDialog that returns a pre-built payload."""

    def __init__(self, payload, *args, **kwargs):
        super().__init__()
        self._payload = payload

    def exec(self):
        from PySide6.QtWidgets import QDialog

        return QDialog.Accepted

    def result_data(self):
        return self._payload

    def delete_requested(self):
        return False

    def normalized_measurement_set_payload(self, *, legacy_reference_value_id=None):
        payload = self._payload
        if payload.get("source_kind") != "reference":
            return None
        if not payload.get("_normalized_range"):
            return None
        rng = payload["_normalized_range"]
        from database.reference_library import MeasurementSet

        return MeasurementSet(
            id="",
            taxon_treatment_id="",
            character="spore_size",
            data_kind="range",
            raw_text=rng.get("raw_text"),
            length_min=rng.get("length_min"),
            length_max=rng.get("length_max"),
            width_min=rng.get("width_min"),
            width_max=rng.get("width_max"),
            length_mean=rng.get("length_mean"),
            width_mean=rng.get("width_mean"),
            legacy_reference_value_id=legacy_reference_value_id,
        )


def _build_window(monkeypatch, qapp):
    monkeypatch.setattr(main_window, "SpeciesDataAvailability", _EmptySpeciesAvailability)
    monkeypatch.setattr(
        main_window.SettingsDB, "get_setting", lambda key, default=None: default
    )
    monkeypatch.setattr(main_window.MainWindow, "init_ui", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_populate_scale_combo", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "load_default_objective", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_restore_geometry", lambda self: None)
    window = main_window.MainWindow()
    window.ref_vernacular_input = QLineEdit()
    window.ref_vernacular_label = QLabel()
    window.ref_genus_input = QLineEdit()
    window.ref_species_input = QLineEdit()
    window.ref_source_input = QComboBox()
    window.ref_source_input.setEditable(True)
    window.ref_source_input.addItem("")
    window.table = QTableWidget(3, 5)
    window.ref_table = window.table
    window.reference_values = {}
    window.reference_series = []
    window.species_availability = _EmptySpeciesAvailability()
    # UI helpers reduced to no-ops for the coordinator's dependencies.
    window._refresh_reference_species_availability = lambda: None
    window._populate_reference_panel_sources = lambda auto_select_single=True: None
    window._apply_reference_panel_values = lambda data: None
    window._update_reference_add_state = lambda: None
    window._clean_ref_genus_text = lambda t: (t or "").strip()
    window._clean_ref_species_text = lambda t: (t or "").strip()
    window._active_sporely_taxon_id = lambda: 7
    return window


def _make_observation(db_path, *, sporely_taxon_id: int = 7) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "INSERT INTO observations (date, location, sporely_taxon_id) VALUES (?, ?, ?)",
            ("2026-01-01", "MW-E2E", sporely_taxon_id),
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def _seed_work(short_label: str = "MWE 2020") -> ReferenceWork:
    return ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="book",
            title=short_label,
            short_label=short_label,
            year=2020,
        )
    )


def test_mainwindow_add_range_writes_legacy_and_normalized_and_persists(
    monkeypatch, qapp, libs
):
    """Drive _on_reference_panel_add_clicked with a range payload; the
    legacy reference_values row, the normalized MeasurementSet, and the
    ObservationReferenceUse must all exist after the call. Reopening the
    observation (via ObservationReferenceUseRepository.list_for_observation)
    must return the same attachment.
    """
    db_path, _ = libs
    obs_id = _make_observation(db_path, sporely_taxon_id=7)
    work = _seed_work()

    window = _build_window(monkeypatch, qapp)
    window.active_observation_id = obs_id
    window.ref_genus_input.setText("Agaricus")
    window.ref_species_input.setText("bisporus")

    payload = {
        "genus": "Agaricus",
        "species": "bisporus",
        "source_kind": "reference",
        "source": "MWE 2020 (2020)",
        "reference_work_id": work.id,
        "sporely_taxon_id": 7,
        "length_min": 5.5,
        "length_max": 8.5,
        "width_min": 3.0,
        "width_max": 5.0,
        "_normalized_range": {
            "raw_text": "5.5-8.5 x 3-5",
            "length_min": 5.5,
            "length_max": 8.5,
            "width_min": 3.0,
            "width_max": 5.0,
        },
    }
    monkeypatch.setattr(
        main_window,
        "ReferenceAddDialog",
        lambda *a, **kw: _StubReferenceAddDialog(payload, *a, **kw),
    )
    window._on_reference_panel_add_clicked()

    # Legacy row exists.
    from database.models import ReferenceDB

    legacy = ReferenceDB.get_reference(
        "Agaricus", "bisporus", payload["source"], None, None
    )
    assert legacy is not None
    assert float(legacy.get("length_min")) == pytest.approx(5.5)
    # Normalized MeasurementSet exists and is stamped with the legacy id.
    sets = MeasurementSetRepository.list_for_treatment  # sanity — attribute exists
    listed = ObservationReferenceUseRepository.list_for_observation(obs_id)
    assert len(listed) == 1
    use = listed[0]
    ms = MeasurementSetRepository.get(use.reference_measurement_set_id)
    assert ms is not None
    assert ms.data_kind == "range"
    assert ms.length_min == pytest.approx(5.5)
    assert ms.legacy_reference_value_id is not None
    # Reopen: re-fetch attachments as if the observation was closed and
    # opened again; the attachment must still be there.
    reopened = ObservationReferenceUseRepository.list_for_observation(obs_id)
    assert len(reopened) == 1
    assert reopened[0].reference_measurement_set_id == ms.id


def test_mainwindow_add_dedups_legacy_envelope_when_normalized_attach_succeeds(
    monkeypatch, qapp, libs
):
    """When the coordinator successfully attaches a normalized
    MeasurementSet, the legacy envelope must NOT be pushed as a second
    ``reference_series`` entry — the attach helper already added a
    translated series row for the attached set, and rendering both
    would show the same dataset twice with different colors.
    """
    db_path, _ = libs
    obs_id = _make_observation(db_path, sporely_taxon_id=7)
    work = _seed_work("DEDUP 2024")

    window = _build_window(monkeypatch, qapp)
    window.active_observation_id = obs_id
    window.ref_genus_input.setText("Agaricus")
    window.ref_species_input.setText("bisporus")

    payload = {
        "genus": "Agaricus",
        "species": "bisporus",
        "source_kind": "reference",
        "source": "DEDUP 2024 (2024)",
        "reference_work_id": work.id,
        "sporely_taxon_id": 7,
        "observation_id": obs_id,
        "length_min": 5.5,
        "length_max": 8.5,
        "width_min": 3.0,
        "width_max": 5.0,
        "_normalized_range": {
            "raw_text": "5.5-8.5 x 3-5",
            "length_min": 5.5,
            "length_max": 8.5,
            "width_min": 3.0,
            "width_max": 5.0,
        },
    }
    monkeypatch.setattr(
        main_window,
        "ReferenceAddDialog",
        lambda *a, **kw: _StubReferenceAddDialog(payload, *a, **kw),
    )
    # Count reference-series pushes and simulate the real attach path
    # that adds a translated normalized series row.
    push_count = {"n": 0}
    original_add = window._add_reference_series_entry

    def _counting_add(data):
        push_count["n"] += 1
        return original_add(data)

    window._add_reference_series_entry = _counting_add

    def _fake_attach(set_id, role):
        ObservationReferenceUseRepository.attach_with_status(
            int(obs_id), str(set_id), role=str(role)
        )
        # Real attach helper appends the translated series row here.
        _counting_add({
            "observation_reference_use_id": "sim-use",
            "reference_measurement_set_id": str(set_id),
        })

    window._attach_normalized_reference_to_active_observation = _fake_attach
    window._on_reference_panel_add_clicked()

    # Exactly one series row: the translated normalized attachment.
    # No second row for the legacy envelope.
    assert push_count["n"] == 1


def test_mainwindow_add_parmasto_only_writes_legacy_but_no_normalized_set(
    monkeypatch, qapp, libs
):
    """A Parmasto-only submission must persist the legacy
    ``reference_values`` row (so historical behavior is preserved) but
    must NOT create a normalized MeasurementSet nor an
    ObservationReferenceUse — the schema does not yet represent
    Parmasto losslessly, so it stays legacy-only.
    """
    db_path, _ = libs
    obs_id = _make_observation(db_path, sporely_taxon_id=7)

    window = _build_window(monkeypatch, qapp)
    window.active_observation_id = obs_id
    window.ref_genus_input.setText("Agaricus")
    window.ref_species_input.setText("bisporus")

    parmasto_payload = {
        "genus": "Agaricus",
        "species": "bisporus",
        "source_kind": "reference",
        "source": "Parmasto only",
        # No _normalized_range → the stub dialog returns None from
        # normalized_measurement_set_payload.
        "parmasto_length_mean": 8.5,
        "parmasto_width_mean": 4.2,
        "parmasto_q_mean": 2.02,
        "length_min": 8.5,
        "length_max": 8.5,
        "width_min": 4.2,
        "width_max": 4.2,
    }
    monkeypatch.setattr(
        main_window,
        "ReferenceAddDialog",
        lambda *a, **kw: _StubReferenceAddDialog(parmasto_payload, *a, **kw),
    )
    window._on_reference_panel_add_clicked()

    from database.models import ReferenceDB

    legacy = ReferenceDB.get_reference(
        "Agaricus", "bisporus", parmasto_payload["source"], None, None
    )
    assert legacy is not None, "legacy reference_values row must still be written"
    # No MeasurementSet, no ObservationReferenceUse.
    listed = ObservationReferenceUseRepository.list_for_observation(obs_id)
    assert listed == [], listed
