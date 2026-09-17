"""The shared preview pane's placeholders must not name one particular source.

``ReferencePreviewPane`` is used by all three source tabs of the Add
reference picker -- Library, Community and My observations -- but its
cleared state described every one of them as a "community result". Opening
the picker on the Library tab and selecting nothing therefore showed
"Select a community result to review raw spore points" over a list of the
user's own reference library.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication

from ui.add_reference_dialog import AddReferenceDialog
from ui.reference_preview_pane import ReferencePreviewPane


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


def test_preview_placeholders_do_not_claim_the_result_is_a_community_one():
    _app()
    pane = ReferencePreviewPane()
    pane.clear()

    placeholders = [
        pane.summary_meta_label.text(),
        pane.summary_note_label.text(),
        pane.raw_spores_text.toPlainText(),
        pane.calibration_text.toPlainText(),
        pane.provenance_text.toPlainText(),
    ]
    for text in placeholders:
        assert "community" not in text.casefold(), text
    # Still says something useful rather than having been blanked.
    assert "raw spore points" in pane.raw_spores_text.toPlainText()
    assert "calibration" in pane.calibration_text.toPlainText().casefold()


def test_library_tab_placeholder_is_source_neutral_in_a_built_dialog():
    """End-to-end: the Library tab's own cleared preview names no source."""
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
        assert "community" not in dialog.preview_pane.raw_spores_text.toPlainText().casefold()
    finally:
        dialog.close()
