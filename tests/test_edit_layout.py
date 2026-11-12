"""Rediger, the mockup's layout (redesign stage 9g3a).

Real ``MainWindow`` in an isolated profile (the fixtures of
``tests/test_files_source.py``); no network. Owner 2026-11-12: Taksonomi
beside AI-forslag with Bruk on each suggestion; date, place and the position
on one row beside both comments; the map as one line whose tools open below
it; a status badge in the heading.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication

from database.models import ObservationDB
from tests.test_edit_mode import rediger  # noqa: F401
from tests.test_files_source import profile, window  # noqa: F401

_FOMES = {"probability": 0.8, "scientificName": "Fomes fomentarius", "name": "Fomes fomentarius",
          "vernacularName": "knuskkjuke", "taxonId": "", "id": ""}
_PIPTOPORUS = {"probability": 0.1, "scientificName": "Fomitopsis betulina", "name": "Fomitopsis betulina",
               "vernacularName": "bjørkekjuke", "taxonId": "", "id": ""}


def _click_cell(qtbot, table, row: int, column: int) -> None:
    rect = table.visualRect(table.model().index(row, column))
    qtbot.mouseClick(table.viewport(), Qt.LeftButton, pos=rect.center())


def test_latitude_and_longitude_share_a_row_and_comments_sit_side_by_side(qtbot, window, rediger):
    editor = rediger.editor()
    assert editor.lat_input.isVisible() and editor.lon_input.isVisible()
    assert abs(editor.lat_input.mapTo(editor, editor.lat_input.rect().center()).y()
               - editor.lon_input.mapTo(editor, editor.lon_input.rect().center()).y()) <= 1
    assert editor.lon_input.mapTo(editor, editor.lon_input.rect().topLeft()).x() > editor.lat_input.mapTo(
        editor, editor.lat_input.rect().topRight()).x()
    open_top = editor.open_comment_input.mapTo(editor, editor.open_comment_input.rect().topLeft())
    private_top = editor.private_comment_input.mapTo(editor, editor.private_comment_input.rect().topLeft())
    assert open_top.y() == private_top.y() and private_top.x() > open_top.x()


def test_the_map_line_opens_its_tools(qtbot, window, rediger):
    editor = rediger.editor()
    assert not editor.map_tools.isVisible() and not editor.map_btn.isVisible()
    qtbot.mouseClick(editor.map_tools_toggle, Qt.LeftButton)
    assert editor.map_tools.isVisible() and editor.maplink_input.isVisible() and editor.map_btn.isVisible()
    qtbot.mouseClick(editor.map_tools_toggle, Qt.LeftButton)
    assert not editor.map_tools.isVisible()


def test_paste_map_link_sets_the_position_and_saves(qtbot, window, rediger):
    first = window.ids_for_test["first"]
    editor = rediger.editor()
    QApplication.clipboard().setText("https://www.openstreetmap.org/#map=17/63.43123/10.39456")
    qtbot.mouseClick(editor.paste_map_link_btn, Qt.LeftButton)
    assert abs(editor.lat_input.value() - 63.43123) < 1e-6 and abs(editor.lon_input.value() - 10.39456) < 1e-6
    qtbot.waitUntil(
        lambda: abs((ObservationDB.get_observation(first)["gps_latitude"] or 0) - 63.43123) < 1e-6, timeout=4000
    )


def test_pasting_something_else_shows_the_link_field(qtbot, window, rediger):
    editor = rediger.editor()
    QApplication.clipboard().setText("not a map link")
    qtbot.mouseClick(editor.paste_map_link_btn, Qt.LeftButton)
    assert editor.map_tools.isVisible() and editor.maplink_input.text() == "not a map link"


def test_the_badge_follows_the_draft_flag(qtbot, window, rediger, monkeypatch):
    import ui.publish_notice as publish_notice

    # Finishing a public observation asks the publish notice when it saves: accept it.
    monkeypatch.setattr(publish_notice, "show_publish_notice", lambda *args, **kwargs: True)
    editor = rediger.editor()
    editor.is_draft_checkbox.setChecked(True)
    assert editor.status_badge.text().endswith("Draft")
    editor.is_draft_checkbox.setChecked(False)
    assert editor.status_badge.text().endswith("Finished")


def test_use_on_a_suggestion_sets_the_name_and_marks_it_chosen(qtbot, window, rediger):
    first = window.ids_for_test["first"]
    editor = rediger.editor()
    editor._ai_predictions_by_index[0] = [_FOMES, _PIPTOPORUS]
    editor._update_ai_table()
    table = editor.ai_tables["arts"]
    use_column = table.columnCount() - 1
    assert [table.item(row, use_column).text() for row in range(2)] == ["Use", "Use"]
    _click_cell(qtbot, table, 1, use_column)
    assert (editor.genus_input.text(), editor.species_input.text()) == ("Fomitopsis", "betulina")
    assert [table.item(row, use_column).text() for row in range(2)] == ["Use", "Chosen"]
    qtbot.waitUntil(lambda: ObservationDB.get_observation(first)["genus"] == "Fomitopsis", timeout=4000)
    _click_cell(qtbot, table, 1, use_column)  # Chosen does nothing
    assert editor.genus_input.text() == "Fomitopsis"


# -- Codex review of stage 9g3a -------------------------------------------------


def test_use_names_the_observation_even_from_grows_on(qtbot, window, rediger):
    editor = rediger.editor()
    editor.taxonomy_tabs.setCurrentWidget(editor.grows_tab)
    editor._ai_predictions_by_index[0] = [_FOMES, _PIPTOPORUS]
    editor._update_ai_table()
    table = editor.ai_tables["arts"]
    _click_cell(qtbot, table, 1, table.columnCount() - 1)
    assert (editor.genus_input.text(), editor.species_input.text()) == ("Fomitopsis", "betulina")
    assert editor.host_genus_input.text() == ""


def test_chosen_follows_a_name_cleared_in_the_picker(qtbot, window, rediger):
    editor = rediger.editor()
    editor._ai_predictions_by_index[0] = [_FOMES, _PIPTOPORUS]
    editor._update_ai_table()
    table = editor.ai_tables["arts"]
    use_column = table.columnCount() - 1
    _click_cell(qtbot, table, 0, use_column)
    assert table.item(0, use_column).text() == "Chosen"
    qtbot.mouseClick(editor.taxon_picker.card.clear_button, Qt.LeftButton)  # the picker's ✕
    qtbot.wait(20)
    assert [table.item(row, use_column).text() for row in range(2)] == ["Use", "Use"]


def test_two_varieties_are_two_names(qtbot, window, rediger):
    editor = rediger.editor()
    varieties = [
        {**_FOMES, "scientificName": "Fomes fomentarius var. fomentarius", "name": "Fomes fomentarius var. fomentarius"},
        {**_FOMES, "scientificName": "Fomes fomentarius var. inzengae", "name": "Fomes fomentarius var. inzengae"},
    ]
    editor._ai_predictions_by_index[0] = varieties
    editor._update_ai_table()
    table = editor.ai_tables["arts"]
    use_column = table.columnCount() - 1
    assert [table.item(row, use_column).text() for row in range(2)].count("Chosen") <= 1


def test_pasting_the_same_link_again_sets_its_position_again(qtbot, window, rediger):
    editor = rediger.editor()
    QApplication.clipboard().setText("https://www.openstreetmap.org/#map=17/63.43123/10.39456")
    qtbot.mouseClick(editor.paste_map_link_btn, Qt.LeftButton)
    editor.lat_input.setValue(60.0)
    qtbot.mouseClick(editor.paste_map_link_btn, Qt.LeftButton)
    assert abs(editor.lat_input.value() - 63.43123) < 1e-6
