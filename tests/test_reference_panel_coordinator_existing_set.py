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
    window.table = QTableWidget(3, 5)
    window.reference_values = {}
    window.reference_series = []
    window.active_observation_id = None
    # Neutralize UI-only branches that would otherwise require full init.
    window._active_sporely_taxon_id = lambda: 7
    window._clean_ref_genus_text = lambda text: (text or "").strip()
    window._clean_ref_species_text = lambda text: (text or "").strip()
    return window



def _add_through_live_path(window, dialog):
    """What “Add to plot” in Analyse's create dialog does (stage 10a2).

    ``_on_reference_picker_new_reference``'s ``add_callback`` hands the
    accepted editor to ``_submit_reference_editor_result`` without syncing
    the retired reference panel. These tests drove the removed panel's Add
    button, which ran the same submission.
    """
    from PySide6.QtWidgets import QDialog

    if dialog.exec() != QDialog.Accepted:
        return False
    return window._submit_reference_editor_result(dialog)

def test_add_existing_set_does_not_append_empty_envelope(monkeypatch, qapp):
    """When the dialog returns ``source_kind=existing_measurement_set``
    the Add coordinator must NOT set ``reference_values`` to the empty
    envelope or push an extra ``reference_series`` row on top of the
    translated attachment row appended by the attach helper.
    """
    window = _build_minimal_window(monkeypatch, qapp)

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

    _dialog_factory = lambda *a, **kw: _StubDialog(dialog_payload)
    _add_through_live_path(window, _dialog_factory())

    # The attach helper produced its translated entry, but the
    # coordinator must not have re-added the raw envelope.
    kinds = [call["kind"] for call in envelope_calls]
    assert kinds == ["attached"], envelope_calls
    assert attach_calls == [("set-123", "compared")]
    # ``reference_values`` must not be clobbered with the empty envelope.
    assert window.reference_values == {}


def test_edit_existing_set_routes_through_attach_and_skips_envelope(monkeypatch, qapp):
    """Selecting ``existing_measurement_set`` when editing a comparison row
    must attach the set through the shared helper and skip the blank
    envelope push so no phantom row appears.

    The comparison list's “Edit measurement set…” (``_edit_reference_series_row``)
    is the live edit path; the retired panel's Edit button ran the same branch.
    """
    window = _build_minimal_window(monkeypatch, qapp)
    row = {
        "genus": "Agaricus",
        "species": "bisporus",
        "source": "Petersen 1990 (1990)",
        "source_kind": "reference",
    }
    key = window._reference_series_key(row)
    window.reference_series = [{"key": key, "data": dict(row), "enabled": True}]

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
    monkeypatch.setattr(main_window, "ReferenceAddDialog", lambda *a, **kw: _StubDialog(dialog_payload))

    window._edit_reference_series_row(key)

    assert attach_calls == [("set-999", "compared")]
    # No blank envelope pushed into the comparison list.
    assert envelope_calls == []


def test_comparison_visibility_toggle_flips_one_row_and_saves_once(monkeypatch, qapp):
    """The comparison list's eye (``visibility_toggled``) shows or hides one row.

    Only a real change redraws the plots and saves; an unknown key or an
    unchanged state does nothing. (Its only tests went with the retired
    reference dialogs, stage 10a2.)
    """
    window = _build_minimal_window(monkeypatch, qapp)
    window.reference_series = [
        {"key": "a", "data": {"genus": "Agaricus"}, "enabled": True},
        {"key": "b", "data": {"genus": "Boletus"}, "enabled": True},
    ]
    calls: list[str] = []
    window.update_graph_plots_only = lambda: calls.append("plot")
    window._save_gallery_settings = lambda: calls.append("save")

    window._set_reference_series_enabled("b", False)
    assert [e["enabled"] for e in window.reference_series] == [True, False]
    assert calls == ["plot", "save"]

    calls.clear()
    window._set_reference_series_enabled("b", False)  # already hidden
    window._set_reference_series_enabled("missing", True)
    window._set_reference_series_enabled("", True)
    assert calls == []
    assert [e["enabled"] for e in window.reference_series] == [True, False]
