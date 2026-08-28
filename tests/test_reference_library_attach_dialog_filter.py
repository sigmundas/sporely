"""Tests for the taxon scope + text search on ReferenceLibraryAttachDialog."""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QWidget

from database.reference_library import MeasurementSetCandidate
from ui.reference_library_attach_dialog import ReferenceLibraryAttachDialog


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _candidate(
    id_: str,
    *,
    taxon_id: str | None,
    short_label: str = "Petersen 1990",
    name: str = "Agaricus bisporus",
    locator: str | None = "p. 42",
    kind: str = "range",
    raw: str = "7-10 x 4-6 µm",
    favorite: bool = False,
    recent_sequence: int | None = None,
) -> MeasurementSetCandidate:
    return MeasurementSetCandidate(
        measurement_set_id=id_,
        short_label=short_label,
        name_as_published=name,
        locator_text=locator,
        data_kind=kind,
        raw_text=raw,
        revision=1,
        reference_work_id="w1",
        reference_treatment_id=f"t-{id_}",
        taxon_id=taxon_id,
        is_favorite=favorite,
        recent_use_sequence=recent_sequence,
    )


def test_taxon_scope_filters_by_default_when_taxon_provided(qapp):
    parent = QWidget()
    candidates = [
        _candidate("m1", taxon_id="7"),
        _candidate("m2", taxon_id="99", name="Other species"),
    ]
    dialog = ReferenceLibraryAttachDialog(
        parent, candidates=list(candidates), taxon_id=7
    )
    try:
        assert dialog.only_this_taxon_checkbox.isEnabled()
        assert dialog.only_this_taxon_checkbox.isChecked()
        assert dialog.table.rowCount() == 1
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_taxon_scope_toggle_widens_to_full_list(qapp):
    parent = QWidget()
    candidates = [
        _candidate("m1", taxon_id="7"),
        _candidate("m2", taxon_id="99", name="Other species"),
    ]
    dialog = ReferenceLibraryAttachDialog(
        parent, candidates=list(candidates), taxon_id=7
    )
    try:
        dialog.only_this_taxon_checkbox.setChecked(False)
        assert dialog.table.rowCount() == 2
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_taxon_scope_disabled_when_no_taxon(qapp):
    parent = QWidget()
    candidates = [
        _candidate("m1", taxon_id="7"),
        _candidate("m2", taxon_id="99"),
    ]
    dialog = ReferenceLibraryAttachDialog(parent, candidates=list(candidates))
    try:
        assert not dialog.only_this_taxon_checkbox.isEnabled()
        assert dialog.table.rowCount() == 2
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_text_search_filters_visible_rows(qapp):
    parent = QWidget()
    candidates = [
        _candidate("m1", taxon_id=None, short_label="Petersen 1990"),
        _candidate("m2", taxon_id=None, short_label="Kalamees 2005", raw="12-15 x 5-7"),
    ]
    dialog = ReferenceLibraryAttachDialog(parent, candidates=list(candidates))
    try:
        dialog.search_input.setText("Kalamees")
        assert dialog.table.rowCount() == 1
        dialog.search_input.setText("12-15")
        assert dialog.table.rowCount() == 1
        dialog.search_input.setText("")
        assert dialog.table.rowCount() == 2
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_text_search_matches_across_columns(qapp):
    parent = QWidget()
    candidates = [
        _candidate("m1", taxon_id=None, name="Agaricus bisporus"),
        _candidate("m2", taxon_id=None, name="Amanita muscaria", locator="p. 128"),
    ]
    dialog = ReferenceLibraryAttachDialog(parent, candidates=list(candidates))
    try:
        dialog.search_input.setText("128")
        assert dialog.table.rowCount() == 1
        dialog.search_input.setText("bisporus")
        assert dialog.table.rowCount() == 1
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_exclusion_still_applies_with_filters(qapp):
    parent = QWidget()
    candidates = [
        _candidate("m1", taxon_id="7"),
        _candidate("m2", taxon_id="7", name="Second"),
    ]
    dialog = ReferenceLibraryAttachDialog(
        parent,
        candidates=list(candidates),
        exclude_measurement_set_ids=["m1"],
        taxon_id=7,
    )
    try:
        # Exclusion filters the underlying candidate list — with taxon
        # scope on we should still see m2 only.
        assert dialog.table.rowCount() == 1
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_usage_filter_shows_favourites_and_recent_items(qapp):
    parent = QWidget()
    dialog = ReferenceLibraryAttachDialog(
        parent,
        candidates=[
            _candidate("favorite", taxon_id=None, favorite=True),
            _candidate("recent", taxon_id=None, recent_sequence=4),
            _candidate("plain", taxon_id=None),
        ],
    )
    try:
        dialog.usage_filter_combo.setCurrentIndex(
            dialog.usage_filter_combo.findData("favorites")
        )
        assert dialog.table.rowCount() == 1
        assert dialog.table.item(0, 1).data(Qt.UserRole) == "favorite"

        dialog.usage_filter_combo.setCurrentIndex(
            dialog.usage_filter_combo.findData("recent")
        )
        assert dialog.table.rowCount() == 1
        assert dialog.table.item(0, 1).data(Qt.UserRole) == "recent"
    finally:
        dialog.deleteLater()
        parent.deleteLater()


def test_star_button_explicitly_toggles_favourite(qapp, monkeypatch):
    calls = []
    monkeypatch.setattr(
        "ui.reference_library_attach_dialog.MeasurementSetPreferenceRepository.set_favorite",
        lambda set_id, favorite: calls.append((set_id, favorite)),
    )
    parent = QWidget()
    dialog = ReferenceLibraryAttachDialog(
        parent, candidates=[_candidate("m1", taxon_id=None)]
    )
    try:
        star = dialog.table.item(0, 0)
        assert star.text() == "☆"
        dialog._on_table_cell_clicked(0, 0)
        assert calls == [("m1", True)]
        assert dialog._candidates[0].is_favorite is True
    finally:
        dialog.deleteLater()
        parent.deleteLater()
