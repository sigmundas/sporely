"""The reference flows say things where the user is looking (stage 10e2).

What the main window's reference flows said in message boxes is a notice in
Analysis's bottom bar (the comparison list, the reference drawer and the
library button are there), or, while the New reference dialog saves or adds,
in that dialog's bar; errors keep the error under Details. What keeps an
entry from being saved is said in red by the part of the form it is about,
and goes when the entry changes.

Real ``MainWindow``, ``ReferenceCreateDialog``, ``ReferenceAddDialog`` and
``ReferenceEntryEditor`` on isolated profiles; no network, no live data.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QPoint, Qt
from PySide6.QtWidgets import QApplication, QMessageBox, QWidget

import ui.main_window as main_window
from database.reference_library import ReferenceLibraryError
from references import measurement_content_gates as gates
from tests.test_reference_create_dialog import (  # noqa: F401
    _new_work,
    _observation,
    _open_drawer,
    _parse,
    _pick_publication,
    _run_create_dialog,
    _window,
    library,
    no_message_boxes,
    profile,
)
from tests.test_reference_editor_reported_statistics import HEBELOMA_TABLE
from ui.reference_create_dialog import SOURCE_STEP


def _analysis_notice(window):
    return window._gallery_hint_controller._notice


# --- Analysis's bar -----------------------------------------------------------


def test_new_reference_without_an_observation_is_said_on_analysis(qtbot, profile, no_message_boxes):
    window = _window(qtbot, _observation(profile))
    window.reference_picker_context = lambda: None
    window._on_reference_picker_new_reference()
    notice = _analysis_notice(window)
    assert notice.tone == "info"
    assert notice.text == "Select an observation first before adding a reference."
    assert no_message_boxes == []


def test_a_failed_library_update_is_said_on_analysis_with_details(qtbot, monkeypatch, profile, no_message_boxes):
    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)

    def broken(_use_id):
        raise ReferenceLibraryError("database is locked")

    monkeypatch.setattr(main_window.ObservationReferenceUseRepository, "refresh_snapshot", broken)
    window._update_reference_use_from_library("use-1", observation_id)
    notice = _analysis_notice(window)
    assert notice.tone == "error" and notice.text == "Could not update from the library."
    assert notice.details == "database is locked"
    assert window.gallery_hint_bar.isVisible() and window.gallery_hint_bar._details_btn.isVisible()
    assert no_message_boxes == []


def test_an_attach_from_the_library_after_the_observation_changed_is_a_warning(qtbot, profile, no_message_boxes):
    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)
    attached: list = []
    window._attach_normalized_reference_to_active_observation = lambda *a: attached.append(a)
    window._attach_normalized_reference_from_manager("set-1", "compared", observation_id + 1000)
    assert attached == []
    notice = _analysis_notice(window)
    assert notice.tone == "warning" and "active observation changed" in notice.text
    assert no_message_boxes == []


# --- The New reference dialog -------------------------------------------------


def test_a_failed_add_or_save_is_said_in_the_dialog_with_details(qtbot, monkeypatch, profile, library, no_message_boxes):
    window = _window(qtbot, _observation(profile))
    _open_drawer(qtbot, window)
    work = _new_work()

    def broken(_request):
        raise RuntimeError("disk I/O error")

    monkeypatch.setattr(main_window.QuickAddReferenceService, "create_and_attach", broken)
    monkeypatch.setattr(main_window.QuickAddReferenceService, "create_only", broken)
    said: list = []

    def drive(dialog):
        _pick_publication(dialog, work.id)
        qtbot.mouseClick(dialog.next_btn, Qt.LeftButton)
        _parse(qtbot, dialog)
        for button in (dialog.add_btn, dialog.save_btn):
            qtbot.mouseClick(button, Qt.LeftButton)
            assert dialog.isVisible() and dialog.result() == 0  # stays open to try again
            notice = dialog.hint_controller._notice
            said.append((notice.tone, dialog.hint_controller.current_text(), notice.details))
        dialog.reject()

    _run_create_dialog(monkeypatch, qtbot, window, drive)
    assert said == [("error", "Could not add the library reference.", "disk I/O error")] * 2
    assert not window._gallery_hint_controller.has_notice()  # said once, in the dialog
    assert window._reference_notice_sink is None  # Analysis's bar again once it closed
    assert no_message_boxes == []


def test_a_step_that_succeeds_and_closes_the_dialog_hands_its_notice_to_analysis(
    qtbot, monkeypatch, profile, library, no_message_boxes
):
    window = _window(qtbot, _observation(profile))
    _open_drawer(qtbot, window)
    work = _new_work()

    def stored_but_not_in_the_library(self, _editor):
        self._show_reference_notice(
            self.tr("The reference was saved, but adding it to the reference library failed."),
            details="no such table: measurement_sets",
        )
        return True

    monkeypatch.setattr(main_window.MainWindow, "_submit_reference_editor_result", stored_but_not_in_the_library)

    def drive(dialog):
        _pick_publication(dialog, work.id)
        qtbot.mouseClick(dialog.next_btn, Qt.LeftButton)
        _parse(qtbot, dialog)
        qtbot.mouseClick(dialog.add_btn, Qt.LeftButton)

    seen = _run_create_dialog(monkeypatch, qtbot, window, drive)
    assert seen["result"] == 1  # accepted and closed
    notice = _analysis_notice(window)
    assert notice.tone == "error"
    assert notice.text == "The reference was saved, but adding it to the reference library failed."
    assert notice.details == "no such table: measurement_sets"
    assert no_message_boxes == []


def test_an_earlier_failure_is_not_handed_on_when_a_later_add_succeeds(
    qtbot, monkeypatch, profile, library, no_message_boxes
):
    window = _window(qtbot, _observation(profile))
    _open_drawer(qtbot, window)
    work = _new_work()
    attempts: list[int] = []

    def fails_once(self, _editor):
        attempts.append(1)
        if len(attempts) == 1:
            self._show_reference_notice(self.tr("Could not add the library reference."), details="busy")
            return False
        return True

    monkeypatch.setattr(main_window.MainWindow, "_submit_reference_editor_result", fails_once)

    def drive(dialog):
        _pick_publication(dialog, work.id)
        qtbot.mouseClick(dialog.next_btn, Qt.LeftButton)
        _parse(qtbot, dialog)
        qtbot.mouseClick(dialog.add_btn, Qt.LeftButton)
        assert dialog.isVisible() and dialog.hint_controller.has_notice()
        qtbot.mouseClick(dialog.add_btn, Qt.LeftButton)

    seen = _run_create_dialog(monkeypatch, qtbot, window, drive)
    assert seen["result"] == 1 and len(attempts) == 2
    assert not window._gallery_hint_controller.has_notice()


def test_save_says_not_saved_when_the_host_said_nothing_this_time(qtbot, isolated_libs):
    """An earlier notice in the dialog's bar does not stand in for this Save's answer."""
    from ui.reference_create_dialog import ReferenceCreateDialog

    dialog = ReferenceCreateDialog(
        None, genus="Cortinarius", species="limonius", observation_id=1, sporely_taxon_id=7,
        save_callback=lambda _editor: None, add_callback=lambda _editor: False,
    )
    qtbot.addWidget(dialog)
    dialog.editor.validate_and_build_result = lambda: True
    dialog.hint_controller.show_notice("Could not add the library reference.", "error", details="busy")
    dialog._on_save_clicked()
    assert dialog.hint_controller.current_text() == "Not saved — nothing was stored in the reference library."


def test_a_publication_name_matching_several_is_said_by_the_field(qtbot, monkeypatch, profile, library, no_message_boxes):
    window = _window(qtbot, _observation(profile))
    _open_drawer(qtbot, window)
    work = _new_work("Funga Nordica")
    _new_work("Funga Nordica")  # the same label

    def drive(dialog):
        editor = dialog.editor
        combo = editor.publication_combo
        label = combo.itemText(combo.findData(work.id))
        combo.setCurrentIndex(0)
        combo.lineEdit().clear()
        qtbot.keyClicks(combo.lineEdit(), label)
        qtbot.mouseClick(dialog.next_btn, Qt.LeftButton)
        assert dialog.current_step() == SOURCE_STEP
        assert editor.publication_validation_label.isVisible()
        assert editor.validation_message() == "Several publications match this name — choose one from the list."
        assert dialog.hint_controller.current_text() != editor.validation_message()  # not in the bar
        qtbot.keyClicks(combo.lineEdit(), "x")
        assert not editor.publication_validation_label.isVisible()  # editing clears it
        dialog.reject()

    _run_create_dialog(monkeypatch, qtbot, window, drive)
    assert no_message_boxes == []


# --- The rollback keeps the reason ------------------------------------------


def test_a_rollback_that_fails_keeps_the_attach_failure_under_details(qtbot, tmp_path, monkeypatch):
    """The attach's own reason is replaced by the rollback notice; it moves to Details."""
    from database import schema
    from tests import test_reference_panel_taxon_drift_and_retry as drift

    qapp = QApplication.instance()
    monkeypatch.setattr(schema, "get_database_path", lambda: tmp_path / "mushrooms.db")
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: tmp_path / "reference_values.db")
    monkeypatch.setattr(schema, "get_bundled_reference_database_path", lambda: tmp_path / "none.db")
    schema.init_database()
    obs_id = drift._make_observation(tmp_path / "mushrooms.db", genus="Agaricus", species="bisporus")
    work = drift._seed_work("ROLLBACK 2024")
    window = drift._build_window(monkeypatch, qapp)
    window.active_observation_id = obs_id
    window._active_sporely_taxon_id = lambda: 7
    window._confirm_public_reference_attach = lambda *_a: True
    window._attach_normalized_reference_outcome = lambda *_a: (
        "failed", "Could not attach the reference.", "warning", "disk full"
    )

    def stuck(_set_id):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(main_window.MeasurementSetRepository, "delete", stuck)
    said: list = []
    window._show_reference_notice = lambda text, tone="error", details=None: said.append((tone, text, details))
    monkeypatch.setattr(QMessageBox, "warning", lambda *a, **k: pytest.fail("no message box"))
    monkeypatch.setattr(QMessageBox, "critical", lambda *a, **k: pytest.fail("no message box"))

    payload = drift._range_payload(work.id, 7, "Agaricus", "bisporus")
    drift._add_through_live_path(window, drift._StubDialog(payload))

    assert said[0] == ("warning", "Could not attach the reference.", "disk full")
    tone, text, details = said[-1]
    assert tone == "error" and "could not all be removed" in text
    assert details.startswith("Could not attach the reference.\ndisk full\n\n")
    assert "database is locked" in details


# --- The entry editor's own form --------------------------------------------


@pytest.fixture
def isolated_libs(tmp_path, monkeypatch):
    from database import schema

    monkeypatch.setattr(schema, "get_database_path", lambda: tmp_path / "mushrooms.db")
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: tmp_path / "reference_values.db")
    monkeypatch.setattr(schema, "get_bundled_reference_database_path", lambda: tmp_path / "none.db")
    schema.init_database()
    monkeypatch.setattr(QMessageBox, "warning", lambda *a, **k: pytest.fail("no message box"))
    monkeypatch.setattr(QMessageBox, "question", lambda *a, **k: QMessageBox.Yes)


def test_missing_values_are_said_under_the_measurements_in_view(qtbot, isolated_libs):
    """ReferenceAddDialog (Edit selected reference data) scrolls the editor; the message is brought into view."""
    parent = QWidget()
    qtbot.addWidget(parent)
    dialog = main_window.ReferenceAddDialog(parent, "Cortinarius", "limonius", observation_id=1, sporely_taxon_id=7)
    dialog.resize(700, 480)
    dialog.show()
    qtbot.waitExposed(dialog)
    editor = dialog.editor
    qtbot.mouseClick(dialog.save_btn, Qt.LeftButton)
    assert dialog.isVisible() and dialog.result() == 0
    label = editor.measurement_validation_label
    assert label.isVisible() and editor.validation_message() == "Enter at least one reference or Parmasto value."
    viewport = dialog.editor_scroll.viewport()
    top = label.mapTo(viewport, QPoint(0, 0)).y()
    assert 0 <= top and top + label.height() <= viewport.height()
    cell = editor._measurement_input(0, 1)
    assert dialog.focusWidget() is cell  # the cursor goes to the field
    cell.editingFinished.emit()  # leaving the field (reports data_changed) changes nothing
    assert label.isVisible()
    qtbot.keyClicks(cell, "9")
    assert not label.isVisible()


def test_existing_set_not_chosen_is_said_under_the_list(qtbot, isolated_libs):
    from ui.reference_entry_editor import ReferenceEntryEditor

    editor = ReferenceEntryEditor(None, "Cortinarius", "limonius", observation_id=1, sporely_taxon_id=7)
    qtbot.addWidget(editor)
    editor.show()
    editor.use_existing_radio.setEnabled(True)
    editor.use_existing_radio.setChecked(True)
    assert editor.validate_and_build_result() is False
    assert editor.data_validation_label.isVisible()
    assert "Select an existing measurement set" in editor.validation_message()
    editor.enter_new_radio.setChecked(True)
    assert not editor.data_validation_label.isVisible()


def test_reported_statistics_that_cannot_be_saved_are_said_under_the_grid(qtbot, isolated_libs, monkeypatch):
    from ui.reference_entry_editor import ReferenceEntryEditor

    monkeypatch.setattr(gates, "enhanced_attachments_enabled", lambda: False)
    monkeypatch.setattr(gates, "enhanced_bundle_export_enabled", lambda: False)
    editor = ReferenceEntryEditor(None, "Hebeloma", "crustuliniforme", observation_id=42, sporely_taxon_id=7)
    qtbot.addWidget(editor)
    editor.show()
    editor.measurement_paste_input.setText(HEBELOMA_TABLE)
    editor._on_parse_measurement_clicked()
    assert editor.validate_and_build_result() is False
    message = editor.validation_message()
    assert editor.measurement_validation_label.isVisible()
    assert message.startswith("This version cannot store everything this source reports")
    assert "\n• " in message and "Correct interpretation" in message
    assert editor.result_data() is None


def test_a_publication_editor_that_will_not_open_is_a_notice(qtbot, isolated_libs, monkeypatch):
    import ui.reference_library_manager_dialog as manager_module
    from ui.reference_entry_editor import ReferenceEntryEditor

    monkeypatch.delattr(manager_module, "ReferenceWorkEditor")
    editor = ReferenceEntryEditor(None, "Cortinarius", "limonius", observation_id=1, sporely_taxon_id=7)
    qtbot.addWidget(editor)
    editor.show()
    qtbot.mouseClick(editor.new_publication_btn, Qt.LeftButton)
    notice = editor._hint_controller._notice
    assert notice.tone == "error" and notice.text == "Could not open the publication editor."
    assert "ReferenceWorkEditor" in notice.details


def test_a_message_survives_the_focus_move_that_finishes_an_edit(qtbot, isolated_libs):
    """Codex: a cell left holding 9 becomes 9.00 as the focus moves to the
    field the message is about; that is not the user changing the entry."""
    from database.reference_library import ReferenceWork, ReferenceWorkRepository
    from ui.reference_entry_editor import ReferenceEntryEditor

    for _ in range(2):  # two publications with one label
        ReferenceWorkRepository.create(ReferenceWork(
            id="", type="book", title="Funga Nordica", short_label="Funga Nordica", year=2008))
    editor = ReferenceEntryEditor(None, "Cortinarius", "limonius", observation_id=1, sporely_taxon_id=7)
    qtbot.addWidget(editor)
    editor.show()
    editor.activateWindow()
    qtbot.waitActive(editor)
    editor.publication_combo.setEditText("Funga Nordica (2008)")
    cell = editor._measurement_input(0, 1)
    cell.setFocus()
    qtbot.waitUntil(cell.hasFocus)
    qtbot.keyClicks(cell, "9")
    assert editor.validate_and_build_result() is False
    assert cell.text() == "9.00" and not cell.hasFocus()  # finished by the focus move to the publication
    assert editor.publication_validation_label.isVisible()


def test_typing_in_the_paste_field_clears_the_message(qtbot, isolated_libs):
    from ui.reference_entry_editor import ReferenceEntryEditor

    editor = ReferenceEntryEditor(None, "Cortinarius", "limonius", observation_id=1, sporely_taxon_id=7)
    qtbot.addWidget(editor)
    editor.show()
    assert editor.validate_and_build_result() is False
    assert editor.measurement_validation_label.isVisible()
    qtbot.keyClicks(editor.measurement_paste_input, "9-11 x 5-6")
    assert not editor.measurement_validation_label.isVisible()


def test_only_save_and_add_speak_in_the_dialog(qtbot, monkeypatch, profile, library, no_message_boxes):
    """Codex: a reference flow that runs while the dialog is open, but is not
    its Save or Add, stays on Analysis's bar."""
    window = _window(qtbot, _observation(profile))
    _open_drawer(qtbot, window)

    def drive(dialog):
        window._show_reference_notice("Reference snapshot updated from the library.", "success")
        assert not dialog.hint_controller.has_notice()
        dialog.reject()

    _run_create_dialog(monkeypatch, qtbot, window, drive)
    assert _analysis_notice(window).text == "Reference snapshot updated from the library."
