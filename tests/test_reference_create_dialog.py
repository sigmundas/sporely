"""Creating a reference in two steps (redesign plan stage 7d), on a real ``MainWindow``.

"New reference…" in the reference drawer opens ``ReferenceCreateDialog``:
step 1 is the publication, step 2 the measurement string and Parse; the value
grid appears only after parsing or on "Fill in manually". The unlinked-taxon
notice shows only when the observation's taxon is not linked. Isolated
profile under ``tmp_path``; no network, no live data.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QPointF, Qt
from PySide6.QtWidgets import QApplication, QMessageBox

from database.models import ImageDB, MeasurementDB, ObservationDB
from database.reference_library import (
    MeasurementSetRepository,
    ObservationReferenceUseRepository,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatmentRepository,
)
from tools.review_ui.live_lab_harness import FakeRawpy, create_observation, isolation_patches, prepare_profile
from ui.reference_create_dialog import MEASUREMENTS_STEP, SOURCE_STEP, ReferenceCreateDialog

PASTED = "(9.0–)9.5–11.0(–12.0) × (5.0–)5.3–6.0(–6.4) µm, Q = 1.6–2.0, n = 30"


@pytest.fixture
def profile(tmp_path, monkeypatch, qtbot):
    rawpy = FakeRawpy()
    for owner, attribute, value in isolation_patches(tmp_path, rawpy):
        monkeypatch.setattr(owner, attribute, value)
    yield prepare_profile(tmp_path, rawpy)


@pytest.fixture
def library(profile):
    from tools.review_ui.scenarios.references import _seed_library

    work, sets = _seed_library()
    return work, sets


@pytest.fixture
def no_message_boxes(monkeypatch):
    """Any message box is a failure here, unless a test expects one."""
    shown: list[tuple[str, str]] = []

    def record(kind):
        def _box(_parent, title, text, *args, **kwargs):
            shown.append((kind, str(text)))
            return QMessageBox.No if kind == "question" else QMessageBox.Ok
        return _box

    for kind in ("information", "warning", "critical", "question"):
        monkeypatch.setattr(QMessageBox, kind, staticmethod(record(kind)))
    return shown


def _link_taxon(observation_id, taxon_id):
    """A proven Sporely taxon (``proven_sporely_taxon_id``), as the taxon picker stores it."""
    from database.schema import get_connection
    from utils.taxon_identity import PROOF_TAXONOMY_V2_ARTIFACT, STATE_SPORELY

    conn = get_connection()
    try:
        conn.execute(
            "UPDATE observations SET sporely_taxon_id = ?, taxon_identity_state = ?, taxon_identity_proof = ? WHERE id = ?",
            (int(taxon_id), STATE_SPORELY, PROOF_TAXONOMY_V2_ARTIFACT, int(observation_id)),
        )
        conn.commit()
    finally:
        conn.close()


def _observation(profile, key="default", *, taxon_id=7):
    observation_id = create_observation(profile, key, genus="Cortinarius", species="limonius")
    if taxon_id is not None:
        _link_taxon(observation_id, taxon_id)
    image_id = ImageDB.add_image(
        observation_id, str(profile.jpeg(f"{key}.JPG", seed=5)), "microscope", scale=0.1, objective_name="100X",
    )
    for index in range(6):
        x = 40 + index * 40
        MeasurementDB.add_measurement(
            image_id, 9.0 + index * 0.3, 5.0 + index * 0.1, measurement_type="spores",
            points=[QPointF(x, 50), QPointF(x + 28, 50), QPointF(x + 14, 42), QPointF(x + 14, 58)],
        )
    return observation_id


def _window(qtbot, observation_id):
    from ui.main_window import MainWindow

    window = MainWindow()
    qtbot.addWidget(window)
    window.resize(1500, 950)
    window.show()
    qtbot.waitExposed(window)
    window.on_observation_selected(observation_id, "Cortinarius limonius", switch_tab=False)
    window.tab_widget.setCurrentWidget(window.analysis_tab)
    qtbot.waitUntil(lambda: window.active_observation_id == observation_id, timeout=5000)
    qtbot.wait(200)
    return window


def _open_drawer(qtbot, window):
    qtbot.mouseClick(window.ref_add_reference_btn, Qt.LeftButton)
    qtbot.waitUntil(lambda: window.reference_drawer.is_open() and window.reference_drawer.isVisible(), timeout=2000)
    return window.reference_picker_page


def _run_create_dialog(monkeypatch, qtbot, window, drive):
    """Click "New reference…" with ``drive(dialog)`` standing in for the modal loop."""
    seen: dict = {}

    def fake_exec(dialog):
        dialog.show()
        qtbot.waitExposed(dialog)
        seen["dialog"] = dialog
        drive(dialog)
        seen["result"] = dialog.result()
        dialog.hide()
        return dialog.result()

    monkeypatch.setattr(ReferenceCreateDialog, "exec", fake_exec)
    page = window.reference_picker_page
    qtbot.mouseClick(page.new_reference_btn, Qt.LeftButton)
    assert "dialog" in seen, "New reference… did not open the two-step dialog"
    return seen


def _pick_publication(dialog, work_id):
    combo = dialog.editor.publication_combo
    combo.setCurrentIndex(combo.findData(work_id))
    assert dialog.editor.has_publication()


def _parse(qtbot, dialog, text=PASTED):
    editor = dialog.editor
    editor.measurement_paste_input.setText(text)
    qtbot.mouseClick(editor._parse_measurement_btn, Qt.LeftButton)


def _new_work(title="Another flora of Cortinarius"):
    return ReferenceWorkRepository.create(
        ReferenceWork(id="", type="book", title=title, short_label="Another 2020", year=2020)
    )


def test_new_reference_opens_the_two_step_dialog_on_the_source_step(qtbot, monkeypatch, profile, library, no_message_boxes):
    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)
    _open_drawer(qtbot, window)

    def drive(dialog):
        editor = dialog.editor
        assert dialog.current_step() == SOURCE_STEP
        assert "Cortinarius limonius" in dialog.taxon_label.text()
        assert editor.source_page.isVisible() and not editor.measurements_page.isVisible()
        assert editor.publication_combo.isVisible() and editor.locator_input.isVisible()
        assert dialog.next_btn.isVisible() and not dialog.back_btn.isVisible()
        assert not dialog.add_btn.isVisible() and not dialog.save_btn.isVisible()
        # Linked taxon: no unlinked-taxon notice; nothing about existing sets yet.
        assert not editor._no_taxon_notice_label.isVisible()
        assert not editor.existing_sets_note.isVisible()
        qtbot.mouseClick(dialog.next_btn, Qt.LeftButton)
        assert dialog.current_step() == MEASUREMENTS_STEP
        assert editor.measurements_page.isVisible() and not editor.source_page.isVisible()
        assert dialog.back_btn.isVisible() and dialog.add_btn.isVisible() and dialog.save_btn.isVisible()
        assert not dialog.next_btn.isVisible()
        qtbot.mouseClick(dialog.back_btn, Qt.LeftButton)
        assert dialog.current_step() == SOURCE_STEP
        dialog.reject()

    seen = _run_create_dialog(monkeypatch, qtbot, window, drive)
    assert seen["result"] == 0
    assert no_message_boxes == []


def test_the_grid_appears_only_after_parsing_or_fill_in_manually(qtbot, monkeypatch, profile, library, no_message_boxes):
    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)
    _open_drawer(qtbot, window)

    def drive(dialog):
        editor = dialog.editor
        dialog.show_step(MEASUREMENTS_STEP)
        assert editor.measurement_paste_input.isVisible()
        assert not editor.measurement_grid_panel.isVisible()
        assert not editor._swap_lw_btn.isVisible()
        assert editor.fill_manually_btn.isVisible()
        assert not dialog.add_btn.isEnabled() and not dialog.save_btn.isEnabled()

        # A string the parser cannot read keeps the grid hidden; manual entry stays offered.
        _parse(qtbot, dialog, "no numbers here")
        assert not editor.measurement_grid_panel.isVisible()
        assert editor.fill_manually_btn.isVisible()

        _parse(qtbot, dialog)
        assert editor.measurement_grid_panel.isVisible() and editor._swap_lw_btn.isVisible()
        assert not editor.fill_manually_btn.isVisible()
        assert editor.measurement_cell_text(0, 0) and editor.measurement_cell_text(1, 4)
        assert dialog.add_btn.isEnabled()
        dialog.reject()

    _run_create_dialog(monkeypatch, qtbot, window, drive)

    def drive_manual(dialog):
        editor = dialog.editor
        dialog.show_step(MEASUREMENTS_STEP)
        qtbot.mouseClick(editor.fill_manually_btn, Qt.LeftButton)
        assert editor.measurement_grid_panel.isVisible()
        assert not editor.fill_manually_btn.isVisible()
        assert QApplication.focusWidget() is editor._measurement_input(0, 0) or not dialog.isActiveWindow()
        dialog.reject()

    _run_create_dialog(monkeypatch, qtbot, window, drive_manual)


def test_enter_in_the_paste_field_parses_and_adds_nothing(qtbot, monkeypatch, profile, library, no_message_boxes):
    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)
    _open_drawer(qtbot, window)
    work = _new_work()

    def drive(dialog):
        _pick_publication(dialog, work.id)
        dialog.editor.locator_input.setFocus()
        qtbot.keyClick(dialog.editor.locator_input, Qt.Key_Return)  # Enter in step 1: Next
        assert dialog.current_step() == MEASUREMENTS_STEP
        editor = dialog.editor
        editor.measurement_paste_input.setText(PASTED)
        editor.measurement_paste_input.setFocus()
        qtbot.keyClick(editor.measurement_paste_input, Qt.Key_Return)
        assert editor.is_grid_revealed() and dialog.add_btn.isEnabled()
        assert dialog.isVisible() and dialog.result() == 0  # parsed, not added
        dialog.reject()

    _run_create_dialog(monkeypatch, qtbot, window, drive)
    assert ObservationReferenceUseRepository.list_for_observation(observation_id) == []


def test_add_to_plot_creates_attaches_and_the_drawer_shows_it_ticked(qtbot, monkeypatch, profile, library, no_message_boxes):
    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)
    page = _open_drawer(qtbot, window)
    work = _new_work()

    def drive(dialog):
        _pick_publication(dialog, work.id)
        dialog.editor.locator_input.setText("p. 12")
        qtbot.mouseClick(dialog.next_btn, Qt.LeftButton)
        _parse(qtbot, dialog)
        assert dialog.add_btn.isEnabled()
        qtbot.mouseClick(dialog.add_btn, Qt.LeftButton)

    seen = _run_create_dialog(monkeypatch, qtbot, window, drive)
    assert seen["result"] == 1 and seen["dialog"].added_to_plot
    uses = ObservationReferenceUseRepository.list_for_observation(observation_id)
    assert len(uses) == 1
    new_set_id = str(uses[0].reference_measurement_set_id)
    treatment = TaxonTreatmentRepository.get(MeasurementSetRepository.get(new_set_id).taxon_treatment_id)
    assert treatment.reference_work_id == work.id and str(treatment.taxon_id) == "7"
    assert treatment.locator_text == "p. 12"
    assert new_set_id in window._current_attached_measurement_set_ids()  # plotted
    row = page.row_for(new_set_id)  # the drawer reloaded: listed and ticked
    assert row is not None and row.is_checked()
    assert no_message_boxes == []


def test_save_to_library_stores_without_plotting_and_needs_a_publication(qtbot, monkeypatch, profile, library, no_message_boxes):
    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)
    page = _open_drawer(qtbot, window)
    work = _new_work()
    before = {c.measurement_set_id for c in MeasurementSetRepository.list_attachment_candidates(exclude_ids=set())}

    def drive(dialog):
        qtbot.mouseClick(dialog.next_btn, Qt.LeftButton)
        _parse(qtbot, dialog)
        assert dialog.add_btn.isEnabled()
        assert not dialog.save_btn.isEnabled()  # no publication: nothing for the library to hold it
        qtbot.mouseClick(dialog.back_btn, Qt.LeftButton)
        _pick_publication(dialog, work.id)
        qtbot.mouseClick(dialog.next_btn, Qt.LeftButton)
        assert dialog.editor.is_grid_revealed()  # the parsed values are kept across Back
        assert dialog.save_btn.isEnabled()
        qtbot.mouseClick(dialog.save_btn, Qt.LeftButton)

    seen = _run_create_dialog(monkeypatch, qtbot, window, drive)
    assert seen["result"] == 1 and seen["dialog"].saved_measurement_set_id
    new_set_id = seen["dialog"].saved_measurement_set_id
    after = {c.measurement_set_id for c in MeasurementSetRepository.list_attachment_candidates(exclude_ids=set())}
    assert after - before == {new_set_id}
    assert ObservationReferenceUseRepository.list_for_observation(observation_id) == []
    row = page.row_for(new_set_id)
    assert row is not None and not row.is_checked()
    assert no_message_boxes == []


def test_a_publication_with_sets_for_this_taxon_says_so(qtbot, monkeypatch, profile, library, no_message_boxes):
    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)
    _open_drawer(qtbot, window)
    seeded_work, _sets = library
    other_work = _new_work()

    def drive(dialog):
        editor = dialog.editor
        _pick_publication(dialog, seeded_work.id)
        assert editor.existing_sets_note.isVisible()
        assert "3" in editor.existing_sets_note.text()
        # Creating stays the only path: no "use an existing set" choice here.
        assert not editor.use_existing_radio.isVisible() and not editor.use_existing_radio.isEnabled()
        assert editor.enter_new_radio.isChecked()
        _pick_publication(dialog, other_work.id)
        assert not editor.existing_sets_note.isVisible()
        dialog.reject()

    _run_create_dialog(monkeypatch, qtbot, window, drive)


def test_the_unlinked_taxon_notice_shows_only_without_a_linked_taxon(qtbot, monkeypatch, profile, library, no_message_boxes):
    observation_id = _observation(profile, "unlinked", taxon_id=None)
    window = _window(qtbot, observation_id)
    assert window.reference_picker_context()["taxon_id"] is None
    _open_drawer(qtbot, window)

    def drive(dialog):
        notice = dialog.editor._no_taxon_notice_label
        assert notice.isVisible()
        assert "Edit" in notice.text() and "above" not in notice.text()
        dialog.reject()

    _run_create_dialog(monkeypatch, qtbot, window, drive)


def test_nothing_is_written_when_the_observation_changed_while_open(qtbot, monkeypatch, profile, library, no_message_boxes):
    observation_id = _observation(profile)
    other_id = _observation(profile, "other")
    window = _window(qtbot, observation_id)
    _open_drawer(qtbot, window)
    work = _new_work()
    before = {c.measurement_set_id for c in MeasurementSetRepository.list_attachment_candidates(exclude_ids=set())}

    def drive(dialog):
        _pick_publication(dialog, work.id)
        qtbot.mouseClick(dialog.next_btn, Qt.LeftButton)
        _parse(qtbot, dialog)
        window.active_observation_id = other_id  # another observation became active
        qtbot.mouseClick(dialog.add_btn, Qt.LeftButton)
        assert dialog.isVisible() and dialog.result() == 0  # stays open
        qtbot.mouseClick(dialog.save_btn, Qt.LeftButton)
        assert dialog.isVisible() and dialog.result() == 0
        dialog.reject()

    _run_create_dialog(monkeypatch, qtbot, window, drive)
    assert [kind for kind, _text in no_message_boxes] == ["warning", "warning"]
    assert ObservationReferenceUseRepository.list_for_observation(observation_id) == []
    assert ObservationReferenceUseRepository.list_for_observation(other_id) == []
    after = {c.measurement_set_id for c in MeasurementSetRepository.list_attachment_candidates(exclude_ids=set())}
    assert after == before


def test_the_one_column_editor_is_unchanged(qtbot, profile):
    from ui.reference_entry_editor import ReferenceEntryEditor

    editor = ReferenceEntryEditor(None, "Cortinarius", "limonius", observation_id=1, sporely_taxon_id=7)
    qtbot.addWidget(editor)
    editor.show()
    assert editor.steps is None and not editor.is_staged()
    assert editor.measurement_grid_panel.isVisible() and editor._swap_lw_btn.isVisible()
    assert not editor.fill_manually_btn.isVisible()
    assert not editor.existing_sets_note.isVisible()
    assert "above" in editor._no_taxon_notice_label.text()


def test_typing_an_exact_publication_name_counts_as_choosing_it(qtbot, monkeypatch, profile, library, no_message_boxes):
    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)
    _open_drawer(qtbot, window)
    work = _new_work()

    def drive(dialog):
        combo = dialog.editor.publication_combo
        label = combo.itemText(combo.findData(work.id))
        combo.setCurrentIndex(0)
        combo.lineEdit().clear()
        qtbot.keyClicks(combo.lineEdit(), label)
        assert not dialog.editor.has_publication()  # typed, not picked
        qtbot.mouseClick(dialog.next_btn, Qt.LeftButton)
        assert dialog.editor.has_publication() and dialog.editor.selected_reference_work_id() == work.id
        _parse(qtbot, dialog)
        assert dialog.save_btn.isEnabled()
        dialog.reject()

    _run_create_dialog(monkeypatch, qtbot, window, drive)


def test_save_stays_disabled_for_values_the_library_cannot_store(qtbot, monkeypatch, profile, library, no_message_boxes):
    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)
    _open_drawer(qtbot, window)
    work = _new_work()

    def drive(dialog):
        _pick_publication(dialog, work.id)
        qtbot.mouseClick(dialog.next_btn, Qt.LeftButton)
        qtbot.mouseClick(dialog.editor.fill_manually_btn, Qt.LeftButton)
        dialog.editor.set_measurement_cell_text(0, 0, "8")  # one extreme on its own: nothing to plot
        assert dialog.editor.normalized_measurement_set_payload() is None
        assert not dialog.save_btn.isEnabled()
        dialog.reject()

    _run_create_dialog(monkeypatch, qtbot, window, drive)


@pytest.mark.parametrize("new_taxon", [8, None], ids=["changed", "cleared"])
def test_nothing_is_written_when_the_taxon_changed_while_open(qtbot, monkeypatch, profile, library, no_message_boxes, new_taxon):
    from database.schema import get_connection

    observation_id = _observation(profile)
    window = _window(qtbot, observation_id)
    _open_drawer(qtbot, window)
    work = _new_work()
    before = {c.measurement_set_id for c in MeasurementSetRepository.list_attachment_candidates(exclude_ids=set())}

    def drive(dialog):
        _pick_publication(dialog, work.id)
        qtbot.mouseClick(dialog.next_btn, Qt.LeftButton)
        _parse(qtbot, dialog)
        if new_taxon is None:
            conn = get_connection()
            conn.execute("UPDATE observations SET sporely_taxon_id = NULL, taxon_identity_state = NULL, taxon_identity_proof = NULL WHERE id = ?", (observation_id,))
            conn.commit()
            conn.close()
        else:
            _link_taxon(observation_id, new_taxon)
        qtbot.mouseClick(dialog.add_btn, Qt.LeftButton)
        assert dialog.isVisible() and dialog.result() == 0
        qtbot.mouseClick(dialog.save_btn, Qt.LeftButton)
        assert dialog.isVisible() and dialog.result() == 0
        dialog.reject()

    _run_create_dialog(monkeypatch, qtbot, window, drive)
    assert [kind for kind, _text in no_message_boxes] == ["warning", "warning"]
    assert all("taxon changed" in text for _kind, text in no_message_boxes)
    assert ObservationReferenceUseRepository.list_for_observation(observation_id) == []
    after = {c.measurement_set_id for c in MeasurementSetRepository.list_attachment_candidates(exclude_ids=set())}
    assert after == before
