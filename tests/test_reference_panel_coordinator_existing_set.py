"""Regression coordinator tests for the Add/Edit-reference handlers.

Cover the two coordinator-level defects surfaced during the last review
round:

* Adding an existing measurement set must not append an extra blank
  ``reference_series`` row on top of the translated attachment row.
* Editing an existing legacy reference and picking "Use existing
  measurement set" must actually attach that set (not silently drop
  the selection and add an empty envelope).

The tests exercise the real coordinator methods on a minimal MainWindow
built from the same pattern used by
``tests/test_main_window_reference_panel_taxon_lookup.py``, patching
``init_ui`` and the auxiliary UI wiring so the Reference panel state
alone is populated.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QLabel,
    QLineEdit,
    QTableWidget,
)

import ui.main_window as main_window


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class _EmptySpeciesAvailability:
    def get_cache(self, force_refresh: bool = False):
        return {}

    def get_detailed_info(self, genus, species, exclude_observation_id=None):
        return {}

    def get_species_display_name(
        self, genus, species, exclude_observation_id=None
    ):
        return (f"{genus} {species}".strip(), False)


class _StubDialog:
    """Stand-in for ReferenceAddDialog result payload holder.

    The coordinator only interacts with :meth:`result_data`,
    :meth:`delete_requested`, and
    :meth:`normalized_measurement_set_payload`; supply just those.
    """

    def __init__(self, data, *, accepted=True, delete=False):
        self._data = data
        self._accepted = accepted
        self._delete = delete

    def exec(self):
        from PySide6.QtWidgets import QDialog

        return QDialog.Accepted if self._accepted else QDialog.Rejected

    def result_data(self):
        return self._data

    def delete_requested(self):
        return self._delete

    def normalized_measurement_set_payload(self, *, legacy_reference_value_id=None):
        return None


def _build_minimal_window(monkeypatch, qapp):
    monkeypatch.setattr(main_window, "SpeciesDataAvailability", _EmptySpeciesAvailability)
    monkeypatch.setattr(
        main_window.SettingsDB,
        "get_setting",
        lambda key, default=None: default,
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
    window.active_observation_id = None
    # Neutralize UI-only branches that would otherwise require full init.
    window._refresh_reference_species_availability = lambda: None
    window._populate_reference_panel_sources = lambda auto_select_single=True: None
    window._apply_reference_panel_values = lambda data: None
    window._update_reference_add_state = lambda: None
    window._maybe_load_reference_panel_reference = lambda: None
    window._active_sporely_taxon_id = lambda: 7
    window._clean_ref_genus_text = lambda text: (text or "").strip()
    window._clean_ref_species_text = lambda text: (text or "").strip()
    return window


def test_add_existing_set_does_not_append_empty_envelope(monkeypatch, qapp):
    """When the dialog returns ``source_kind=existing_measurement_set``
    the Add coordinator must NOT set ``reference_values`` to the empty
    envelope or push an extra ``reference_series`` row on top of the
    translated attachment row appended by the attach helper.
    """
    window = _build_minimal_window(monkeypatch, qapp)
    window.ref_genus_input.setText("Agaricus")
    window.ref_species_input.setText("bisporus")

    dialog_payload = {
        "genus": "Agaricus",
        "species": "bisporus",
        "source_kind": "existing_measurement_set",
        "reference_measurement_set_id": "set-123",
    }
    attach_calls: list[tuple[str, str]] = []
    envelope_calls: list[dict] = []

    def _fake_attach(set_id, role):
        attach_calls.append((str(set_id), str(role)))
        # The real helper appends a translated series entry; we count
        # it here so the coordinator's own extra call would be visible.
        envelope_calls.append({"kind": "attached", "set_id": set_id})

    def _fake_persist(self, dialog, payload, *, legacy_id):
        # Emulate the persistence helper's existing-set branch.
        if payload.get("source_kind") == "existing_measurement_set":
            _fake_attach(payload["reference_measurement_set_id"], "compared")

    def _fake_add_series(data):
        envelope_calls.append({"kind": "envelope", "data": dict(data)})
        return True

    monkeypatch.setattr(
        main_window.MainWindow,
        "_persist_normalized_reference_from_dialog",
        _fake_persist,
    )
    window._add_reference_series_entry = _fake_add_series

    monkeypatch.setattr(
        main_window,
        "ReferenceAddDialog",
        lambda *a, **kw: _StubDialog(dialog_payload),
    )
    window._on_reference_panel_add_clicked()

    # The attach helper produced its translated entry, but the
    # coordinator must not have re-added the raw envelope.
    kinds = [call["kind"] for call in envelope_calls]
    assert kinds == ["attached"], envelope_calls
    assert attach_calls == [("set-123", "compared")]
    # ``reference_values`` must not be clobbered with the empty envelope.
    assert window.reference_values == {}


def test_edit_existing_set_routes_through_attach_and_skips_envelope(monkeypatch, qapp):
    """Selecting ``existing_measurement_set`` in edit mode must attach
    the set through the shared helper and skip the blank envelope
    push so no phantom row appears in the panel.
    """
    window = _build_minimal_window(monkeypatch, qapp)
    window.ref_genus_input.setText("Agaricus")
    window.ref_species_input.setText("bisporus")
    window.ref_source_input.setCurrentText("Petersen 1990 (1990)")

    dialog_payload = {
        "genus": "Agaricus",
        "species": "bisporus",
        "source_kind": "existing_measurement_set",
        "reference_measurement_set_id": "set-999",
    }
    attach_calls: list[tuple[str, str]] = []
    envelope_calls: list[dict] = []

    def _fake_persist(self, dialog, payload, *, legacy_id):
        if payload.get("source_kind") == "existing_measurement_set":
            attach_calls.append((payload["reference_measurement_set_id"], "compared"))

    def _fake_add_series(data):
        envelope_calls.append({"data": dict(data)})
        return True

    monkeypatch.setattr(
        main_window.MainWindow,
        "_persist_normalized_reference_from_dialog",
        _fake_persist,
    )
    window._add_reference_series_entry = _fake_add_series

    monkeypatch.setattr(
        main_window,
        "ReferenceAddDialog",
        lambda *a, **kw: _StubDialog(dialog_payload),
    )
    # Edit mode caller path.
    window._on_reference_panel_edit_clicked()

    assert attach_calls == [("set-999", "compared")]
    # No blank envelope pushed into the panel.
    assert envelope_calls == []
    # ``reference_values`` was not clobbered with the empty envelope
    # (the reviewer's F-016 concern was that the selection was silently
    # dropped; here we verify the attach happened AND the envelope was
    # not pushed).
    assert window.reference_values == {}
