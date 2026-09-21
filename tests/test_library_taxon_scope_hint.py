"""The Library tab must say when "Only this taxon" is what emptied the list.

The checkbox is on by default and ANDs with the search box, so a user
searching their library for another publication gets an empty list and no
indication that the checkbox -- not their search text -- excluded the rows.

These tests also pin down a duplicate-hint defect found while fixing that:
``_update_footer_state`` carried its own copy of the "no matching
measurement sets" wording and runs *after* ``_populate_results_list`` on
every filter change, so the more specific message that method sets was
always overwritten. That made the "library has no measurement sets yet"
branch unreachable.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication

from database.reference_library import MeasurementSetCandidate
from ui.add_reference_dialog import AddReferenceDialog


@pytest.fixture(autouse=True)
def isolated_picker_settings(tmp_path, monkeypatch):
    from PySide6.QtCore import QSettings
    import ui.add_reference_dialog as picker
    import ui.window_state as geometry

    def settings(*_args):
        return QSettings(str(tmp_path / "picker.ini"), QSettings.IniFormat)

    monkeypatch.setattr(picker, "QSettings", settings, raising=False)
    monkeypatch.setattr(geometry, "QSettings", settings, raising=False)


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _library_candidates() -> list[MeasurementSetCandidate]:
    return [
        MeasurementSetCandidate(
            measurement_set_id="ms-1",
            short_label="Niskanen et al. 2018",
            name_as_published="Cortinarius limonius (Fr.) Fr.",
            locator_text="pp. 146–148",
            data_kind="range",
            raw_text="(8.1–)8.5–10.8(–11.4) µm",
            revision=1,
            reference_work_id="w-1",
            reference_treatment_id="t-1",
            taxon_id="7",
        ),
        MeasurementSetCandidate(
            measurement_set_id="ms-2",
            short_label="Brandrud et al. 2020",
            name_as_published="Cortinarius rubellus Cooke",
            locator_text="Vol. 2, p. 311",
            data_kind="range",
            raw_text="8.0–9.5 µm",
            revision=1,
            reference_work_id="w-2",
            reference_treatment_id="t-2",
            taxon_id="99",
        ),
    ]


def _candidate_rows(dialog: AddReferenceDialog) -> int:
    """Candidate rows only: the list also holds group headings and the
    "+ New publication…" action row, neither of which is a source."""
    from ui.library_source_row import LibrarySourceRow

    return sum(
        1
        for row in range(dialog.results_list.count())
        if isinstance(
            dialog.results_list.itemWidget(dialog.results_list.item(row)),
            LibrarySourceRow,
        )
    )


def _library_dialog() -> AddReferenceDialog:
    _app()
    return AddReferenceDialog(
        None,
        taxon_label="Cortinarius limonius",
        taxon_id=7,
        candidates=_library_candidates(),
        community_results=[],
        my_observations=[],
    )


def test_taxon_scope_dead_end_says_the_checkbox_is_what_hid_the_rows():
    """"Brandrud" is in the library but on another taxon, so the scope hides it.

    The old hint read "No matching measurement sets in the library", which
    is false -- the set is there.
    """
    dialog = _library_dialog()
    try:
        assert dialog.only_this_taxon_checkbox.isChecked()
        dialog.search_input.setText("Brandrud")
        hint = dialog.status_hint_label.text()
        assert "Only this taxon" in hint
        assert "1 more match" in hint
    finally:
        dialog.close()


def test_genuinely_absent_text_keeps_the_plain_no_match_hint():
    """The new hint must not appear when unchecking would change nothing."""
    dialog = _library_dialog()
    try:
        dialog.search_input.setText("Zzzznotapublication")
        hint = dialog.status_hint_label.text()
        assert "Only this taxon" not in hint
        assert hint == "No matching measurement sets in the library."
    finally:
        dialog.close()


def test_unchecking_only_this_taxon_actually_reveals_the_promised_rows():
    """The hint's promise is checked against what unchecking really does."""
    dialog = _library_dialog()
    try:
        dialog.search_input.setText("Brandrud")
        assert _candidate_rows(dialog) == 0
        dialog.only_this_taxon_checkbox.setChecked(False)
        assert _candidate_rows(dialog) == 1
        assert dialog.status_hint_label.text() == ""
    finally:
        dialog.close()


def test_empty_library_still_reports_an_empty_library():
    """Guards the branch the duplicated footer hint used to make unreachable."""
    _app()
    dialog = AddReferenceDialog(
        None,
        taxon_label="Cortinarius limonius",
        taxon_id=7,
        candidates=[],
        community_results=[],
        my_observations=[],
    )
    try:
        assert "no measurement sets yet" in dialog.status_hint_label.text()
    finally:
        dialog.close()
