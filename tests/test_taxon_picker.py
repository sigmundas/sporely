"""The unified taxon picker widget (`ui/taxon_picker.py`), Stage 2.

Searches run against a real sample of the bundled taxonomy release
(`tools/review_ui/taxon_sample.py`). The tests cover what the widget emits
and what it shows; it writes nothing, so they also check that the database
file is untouched.
"""
from __future__ import annotations

import hashlib

import pytest
from PySide6.QtCore import QPoint, Qt
from PySide6.QtWidgets import QLineEdit, QStyleOptionViewItem, QVBoxLayout, QWidget

from tools.review_ui.taxon_sample import sample_lookup
from ui.taxon_picker import SOURCE_NOTE_SETTING, AiSuggestion, TaxonPicker


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "taxonomy.sqlite3"


@pytest.fixture
def lookup(db_path):
    return sample_lookup(db_path)


@pytest.fixture
def settings():
    return {}


@pytest.fixture
def setup(qtbot, lookup, settings):
    host = QWidget()
    layout = QVBoxLayout(host)
    picker = TaxonPicker(
        lookup,
        debounce_ms=0,
        setting_get=lambda key, default: settings.get(key, default),
        setting_set=settings.__setitem__,
    )
    other = QLineEdit()
    layout.addWidget(picker)
    layout.addWidget(other)
    layout.addStretch(1)
    qtbot.addWidget(host)
    host.resize(640, 600)
    host.show()
    host.activateWindow()
    qtbot.waitActive(host)
    picked, free, cleared = [], [], []
    picker.concept_picked.connect(lambda row, concept: picked.append((row, concept)))
    picker.free_text_committed.connect(free.append)
    picker.cleared.connect(lambda: cleared.append(True))
    picker.search_input.setFocus()
    yield picker, other, picked, free, cleared
    del host  # held for the test's duration


def type_query(qtbot, picker, text):
    qtbot.keyClicks(picker.search_input, text)
    qtbot.waitUntil(picker.is_popup_visible, timeout=2000)


def ids(row):
    return [c.sporely_taxon_id for c in row.concepts]


def row_index(picker, name, kind="taxon"):
    return next(i for i, r in enumerate(picker.results()) if r.name == name and r.kind == kind)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


# --- search, default source and picking -------------------------------------


def test_typing_shows_rows_with_the_default_source_selected(qtbot, setup):
    picker, _other, picked, _free, _cleared = setup
    type_query(qtbot, picker, "Flammulina")
    first = picker.current_entry()
    assert first.row.name == "Flammulina" and ids(first.row) == [625619, 50109]
    assert first.choice == 0  # NorTaxa, the entry with an Artsdatabanken id
    assert picked == []  # nothing is committed by typing
    assert picker.search_input.hasFocus()


def test_enter_picks_the_visibly_selected_default(qtbot, setup, db_path):
    picker, _other, picked, _free, _cleared = setup
    before = digest(db_path)
    type_query(qtbot, picker, "Flammulina")
    qtbot.keyClick(picker.search_input, Qt.Key_Return)
    row, concept = picked[-1]
    assert (row.name, concept.sporely_taxon_id) == ("Flammulina", 625619)
    assert concept.taxon_rank_snapshot == "genus"
    assert picker.card.isVisible() and not picker.search_input.isVisible()
    assert "sp." in picker.card.title.text()
    assert not picker.is_popup_visible()
    assert digest(db_path) == before


def test_alt_arrow_switches_source_before_picking(qtbot, setup):
    picker, _other, picked, _free, _cleared = setup
    type_query(qtbot, picker, "Flammulina")
    qtbot.keyClick(picker.search_input, Qt.Key_Right, Qt.AltModifier)
    assert picker.current_entry().choice == 1
    assert picked == []
    qtbot.keyClick(picker.search_input, Qt.Key_Return)
    assert picked[-1][1].sporely_taxon_id == 50109
    assert picked[-1][1].source_name == "COL"


def test_bare_arrows_move_the_text_cursor_until_the_list_is_navigated(qtbot, setup):
    picker, _other, _picked, _free, _cleared = setup
    type_query(qtbot, picker, "Flammulina")
    qtbot.keyClick(picker.search_input, Qt.Key_Left)
    assert picker.current_entry().choice == 0
    assert picker.search_input.cursorPosition() == len("Flammulina") - 1
    # Down then Up returns to the genus row in list mode; Right switches.
    qtbot.keyClick(picker.search_input, Qt.Key_Down)
    qtbot.keyClick(picker.search_input, Qt.Key_Up)
    qtbot.keyClick(picker.search_input, Qt.Key_Right)
    assert picker.current_entry().choice == 1
    assert picker.search_input.hasFocus()


def test_clicking_a_source_chip_picks_that_concept(qtbot, setup):
    picker, _other, picked, _free, _cleared = setup
    type_query(qtbot, picker, "velutipes")
    index = picker.model.index(row_index(picker, "Flammulina velutipes"), 0)
    view = picker.popup.view
    view.scrollTo(index)
    option = QStyleOptionViewItem()
    option.rect = view.visualRect(index)
    option.font = view.font()
    _pill, chips = picker._delegate.chip_rects(option, index.data(picker_role()))
    col = chips[1].center().toPoint()
    qtbot.mouseClick(view.viewport(), Qt.LeftButton, pos=col)
    row, concept = picked[-1]
    assert row.name == "Flammulina velutipes"
    assert concept.sporely_taxon_id == 98288 and concept.source_name == "COL"


def picker_role():
    from ui.taxon_picker import ROLE_ENTRY

    return ROLE_ENTRY


def test_clicking_a_row_picks_its_default(qtbot, setup):
    picker, _other, picked, _free, _cleared = setup
    type_query(qtbot, picker, "velutipes")
    index = picker.model.index(row_index(picker, "Flammulina velutipes"), 0)
    view = picker.popup.view
    view.scrollTo(index)
    rect = view.visualRect(index)
    qtbot.mouseClick(view.viewport(), Qt.LeftButton, pos=QPoint(rect.left() + 30, rect.center().y()))
    assert picked[-1][1].sporely_taxon_id == 625621


def test_tab_picks_the_highlighted_row(qtbot, setup):
    picker, _other, picked, _free, _cleared = setup
    type_query(qtbot, picker, "rød fluesopp")
    qtbot.keyClick(picker.search_input, Qt.Key_Tab)
    assert picked[-1][1].sporely_taxon_id == 78915


def test_programmatic_text_never_searches(qtbot, setup):
    picker, _other, picked, _free, _cleared = setup
    picker.search_input.setText("Flammulina")
    qtbot.wait(50)
    assert not picker.is_popup_visible() and picker.results() == []
    assert picked == []


def test_short_text_closes_the_results(qtbot, setup):
    picker, _other, _picked, _free, _cleared = setup
    type_query(qtbot, picker, "Fl")
    qtbot.keyClick(picker.search_input, Qt.Key_Backspace)
    assert not picker.is_popup_visible()


# --- synonyms, free names, genus --------------------------------------------


def test_synonym_pick_reports_the_name_and_the_concept(qtbot, setup):
    picker, _other, picked, _free, _cleared = setup
    type_query(qtbot, picker, "Collybia velutipes")
    assert picker.current_entry().row.kind == "synonym"
    qtbot.keyClick(picker.search_input, Qt.Key_Return)
    row, concept = picked[-1]
    assert concept.sporely_taxon_id == 625621
    assert concept.scientific_name == "Collybia velutipes"
    assert concept.canonical_scientific_name == "Flammulina velutipes"
    assert "Collybia" in picker.card.title.text()
    assert "Flammulina" in picker.card.note.text()


def test_free_name_row_is_last_and_emits_the_text(qtbot, setup):
    picker, _other, picked, free, _cleared = setup
    type_query(qtbot, picker, "Cortinarius sp nov")
    last = picker.model.index(picker.model.rowCount() - 1, 0)
    assert last.data(picker_role()).kind == "free"
    picker.popup.view.setCurrentIndex(last)
    qtbot.keyClick(picker.search_input, Qt.Key_Return)
    assert free == ["Cortinarius sp nov"] and picked == []
    assert picker.card.isVisible() and picker.card.property("tone") == "free"


# --- card: change, clear, switch source, escape -----------------------------


def test_card_source_switch_emits_the_other_concept(qtbot, setup):
    picker, _other, picked, _free, _cleared = setup
    type_query(qtbot, picker, "Cantharellus")
    qtbot.keyClick(picker.search_input, Qt.Key_Return)
    assert picked[-1][1].sporely_taxon_id == 626242
    buttons = picker.card.source_buttons
    # Each source shows its own family: the sources disagree.
    assert [b.text() for b in buttons] == ["NorTaxa · Cantharellaceae", "COL · Hydnaceae"]
    assert buttons[0].isChecked() and not buttons[1].isChecked()
    qtbot.mouseClick(buttons[1], Qt.LeftButton)
    assert picked[-1][1].sporely_taxon_id == 129742
    assert picker.card.source_buttons[1].isChecked()


def test_change_then_escape_restores_the_card_without_emitting(qtbot, setup):
    picker, _other, picked, _free, cleared = setup
    type_query(qtbot, picker, "rød fluesopp")
    qtbot.keyClick(picker.search_input, Qt.Key_Return)
    count = len(picked)
    qtbot.mouseClick(picker.card.change_button, Qt.LeftButton)
    assert picker.search_input.isVisible() and picker.search_input.text() == "Amanita muscaria"
    assert picker.search_input.hasFocus()
    qtbot.keyClick(picker.search_input, Qt.Key_Escape)  # nothing open: restore
    assert picker.card.isVisible()
    assert len(picked) == count and cleared == []
    assert picker.selection()[1].sporely_taxon_id == 78915


def test_clear_emits_cleared_and_returns_to_search(qtbot, setup):
    picker, _other, _picked, _free, cleared = setup
    type_query(qtbot, picker, "rød fluesopp")
    qtbot.keyClick(picker.search_input, Qt.Key_Return)
    qtbot.mouseClick(picker.card.clear_button, Qt.LeftButton)
    assert cleared == [True]
    assert picker.selection() is None
    assert picker.search_input.isVisible() and picker.search_input.text() == ""


def test_current_concept_stays_the_default_when_changing(qtbot, setup):
    picker, _other, picked, _free, _cleared = setup
    picker.set_current_taxon_id(98288)  # the observation holds the COL entry
    picker.set_evidence_taxon_ids([625621])  # Artsorakel points at NorTaxa
    picker.search_for("Flammulina velutipes")
    entry = picker.current_entry()
    assert ids(entry.row) == [98288, 625621]
    qtbot.keyClick(picker.search_input, Qt.Key_Return)
    assert picked[-1][1].sporely_taxon_id == 98288


def test_escape_closes_results_first(qtbot, setup):
    picker, _other, _picked, _free, _cleared = setup
    type_query(qtbot, picker, "Flammulina")
    qtbot.keyClick(picker.search_input, Qt.Key_Escape)
    assert not picker.is_popup_visible()
    assert picker.search_input.text() == "Flammulina"


def test_focus_leaving_the_field_closes_results(qtbot, setup):
    picker, other, _picked, _free, _cleared = setup
    type_query(qtbot, picker, "Flammulina")
    other.setFocus()
    qtbot.waitUntil(lambda: not picker.is_popup_visible(), timeout=1000)


# --- show more --------------------------------------------------------------


def test_show_more_appends_the_next_page_and_keeps_the_place(qtbot, lookup, settings):
    picker = TaxonPicker(lookup, page_size=10, debounce_ms=0,
                         setting_get=lambda k, d: settings.get(k, d), setting_set=settings.__setitem__)
    qtbot.addWidget(picker)
    picker.resize(600, 60)
    picker.show()
    picker.search_for("Russula")
    assert len(picker.results()) == 10
    more = picker.model.index(10, 0)
    assert more.data(picker_role()).kind == "more"
    picker.popup.view.setCurrentIndex(more)
    picker._activate_index(more)
    rows = picker.results()
    assert len(rows) == 20
    keys = [(r.kind, r.name, tuple(ids(r))) for r in rows]
    assert len(keys) == len(set(keys))
    assert picker.popup.view.currentIndex().row() == 10  # first new row
    assert picker.is_popup_visible()


# --- first-time note --------------------------------------------------------


def test_source_note_shows_until_dismissed(qtbot, setup, settings):
    picker, _other, _picked, _free, _cleared = setup
    type_query(qtbot, picker, "Flammulina")
    assert picker.popup.note.isVisible()
    qtbot.mouseClick(picker.popup.note_dismiss, Qt.LeftButton)
    assert settings[SOURCE_NOTE_SETTING] == "1"
    assert not picker.popup.note.isVisible()
    assert picker.search_input.hasFocus()  # the button never takes focus
    picker.search_for("Cantharellus")
    assert not picker.popup.note.isVisible()


def test_source_note_needs_a_row_with_a_choice(qtbot, setup):
    picker, _other, _picked, _free, _cleared = setup
    type_query(qtbot, picker, "rød fluesopp")
    assert not picker.popup.note.isVisible()


# --- Artsorakel suggestions -------------------------------------------------


def test_resolved_ai_chip_picks_its_concept_on_click_only(qtbot, setup):
    picker, _other, picked, _free, _cleared = setup
    picker.set_ai_suggestions([
        AiSuggestion("vintersopp", "Flammulina velutipes", 0.82, 98288),
        AiSuggestion("Gymnopus", "Gymnopus", 0.03, None),
    ])
    assert picker.ai_strip.isVisible() and picked == []
    qtbot.mouseClick(picker.ai_buttons[0], Qt.LeftButton)
    row, concept = picked[-1]
    # The identifier chose the COL entry; the NorTaxa one is still offered.
    assert concept.sporely_taxon_id == 98288
    assert ids(row) == [98288, 625621]
    assert [b.isChecked() for b in picker.card.source_buttons] == [True, False]


def test_unresolved_ai_chip_opens_the_search_without_picking(qtbot, setup):
    picker, _other, picked, _free, _cleared = setup
    picker.set_ai_suggestions([AiSuggestion("Gymnopus", "Flammulina elastica", 0.03, None)])
    qtbot.mouseClick(picker.ai_buttons[0], Qt.LeftButton)
    assert picked == []
    assert picker.search_input.text() == "Flammulina elastica"
    assert picker.is_popup_visible()


def test_evidence_marks_only_its_own_source(qtbot, setup):
    picker, _other, _picked, _free, _cleared = setup
    picker.set_evidence_taxon_ids([625621])
    picker.search_for("vintersopp")
    row = picker.current_entry().row
    assert ids(row) == [625621, 98288]
    assert [c.is_evidence for c in row.concepts] == [True, False]
    detail = picker.row_detail_html(row, 0, picker.palette())
    assert "NorTaxa" in detail and "COL" not in detail


# --- display-only states ----------------------------------------------------


def test_unlinked_and_legacy_names_never_emit(qtbot, setup):
    picker, _other, picked, free, cleared = setup
    picker.show_unlinked_name("Russula aeruginea", legacy=True)
    assert picker.card.isVisible() and picker.card.property("tone") == "warn"
    picker.show_unlinked_name("Cortinarius sp.")
    assert picker.card.property("tone") == "free"
    assert picked == [] and free == [] and cleared == []


def test_uncertain_is_shown_on_the_card(qtbot, setup):
    picker, _other, _picked, _free, _cleared = setup
    type_query(qtbot, picker, "kremler")
    qtbot.keyClick(picker.search_input, Qt.Key_Return)
    picker.set_uncertain(True)
    assert "uncertain" in picker.card.meta.text()
