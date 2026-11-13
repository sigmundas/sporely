"""Rediger in place, saving as you go (redesign stage 9g2).

Real ``MainWindow`` in an isolated profile under ``tmp_path`` (the fixtures of
``tests/test_files_source.py``); no network. Edit in the mode bar shows the
observation editor as the Rediger mode instead of a dialog. Changes save on
their own (owner 2026-11-11): about a second after typing stops, at once when
the observation changes or Rediger is left; nothing is written by just
opening it; typed species search text is never saved; draft, sharing and
precision ask the publish notice at the change, and Cancel puts them back.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt

from database.models import ImageDB, ObservationDB
from tests.test_files_source import profile, window  # noqa: F401


@pytest.fixture
def rediger(qtbot, window):
    window.tab_widget.setCurrentWidget(window.observations_tab)
    window.activateWindow()
    qtbot.waitUntil(window.isActiveWindow, timeout=2000)
    qtbot.mouseClick(window.mode_bar.edit_button, Qt.LeftButton)
    page = window.edit_page
    qtbot.waitUntil(lambda: page.editor() is not None, timeout=5000)
    qtbot.wait(50)  # the form finishes opening on the next turn of the event loop
    return page


def _type_into(qtbot, widget, text: str) -> None:
    widget.setFocus(Qt.OtherFocusReason)
    qtbot.mouseClick(widget, Qt.LeftButton)
    widget.selectAll()
    qtbot.keyClicks(widget, text)


def test_edit_opens_rediger_in_place(qtbot, window, rediger):
    editor = rediger.editor()
    assert window.tab_widget.currentWidget() is rediger
    assert window.mode_bar.edit_button.isChecked()
    assert not editor.isWindow() and editor.isVisible()
    assert rediger.observation_id() == window.ids_for_test["first"]
    assert not editor.submit_observation_btn.isVisible() and not editor.cancel_btn.isVisible()
    assert window.observation_rail.isVisible()  # the list is a rail here, as in Bilder


def test_opening_and_leaving_writes_nothing(qtbot, window, monkeypatch):
    first = window.ids_for_test["first"]
    before = ObservationDB.get_observation(first)
    writes = []
    original = ObservationDB.update_observation
    monkeypatch.setattr(
        ObservationDB, "update_observation",
        staticmethod(lambda *a, **k: writes.append(sorted(k)) or original(*a, **k)),
    )
    page = _open_rediger(qtbot, window)  # spied from before opening
    qtbot.wait(1300)  # a watch tick
    qtbot.mouseClick(window.mode_bar.view_button, Qt.LeftButton)
    assert writes == [] and page.editor() is not None
    assert ObservationDB.get_observation(first) == before

def test_a_change_saves_on_its_own(qtbot, window, rediger):
    first = window.ids_for_test["first"]
    editor = rediger.editor()
    _type_into(qtbot, editor.location_input, "Lade, Trondheim")
    assert ObservationDB.get_observation(first)["location"] == "Bymarka, Trondheim"  # not on every key
    assert rediger.status_label.text() == "Unsaved changes…"
    qtbot.waitUntil(lambda: ObservationDB.get_observation(first)["location"] == "Lade, Trondheim", timeout=3000)
    assert rediger.status_label.text() == "Saved"
    assert window.tab_widget.currentWidget() is rediger  # no dialog, no prompt


def test_leaving_rediger_saves_at_once(qtbot, window, rediger):
    first = window.ids_for_test["first"]
    _type_into(qtbot, rediger.editor().location_input, "Byneset")
    qtbot.mouseClick(window.mode_bar.view_button, Qt.LeftButton)
    assert ObservationDB.get_observation(first)["location"] == "Byneset"
    # The table shows it once Rediger is left.
    qtbot.waitUntil(
        lambda: any(
            "Byneset" in (window.observations_tab.table.item(r, c).text() if window.observations_tab.table.item(r, c) else "")
            for r in range(window.observations_tab.table.rowCount())
            for c in range(window.observations_tab.table.columnCount())
        ),
        timeout=3000,
    )


def test_changing_observation_saves_and_shows_the_other(qtbot, window, rediger):
    first, other = window.ids_for_test["first"], window.ids_for_test["other"]
    _type_into(qtbot, rediger.editor().location_input, "Tautra")
    window.on_observation_selected(other, "Other", switch_tab=False)
    assert ObservationDB.get_observation(first)["location"] == "Tautra"
    qtbot.waitUntil(lambda: rediger.observation_id() == other, timeout=3000)
    assert window.tab_widget.currentWidget() is rediger


def test_typed_species_text_is_not_saved(qtbot, window, rediger):
    first = window.ids_for_test["first"]
    editor = rediger.editor()
    picker = editor.taxon_picker
    if not picker.search_input.isVisible():
        qtbot.mouseClick(picker.card.change_button, Qt.LeftButton)  # Endre: back to the search
    assert picker.search_input.isVisible()
    _type_into(qtbot, picker.search_input, "Amanita mus")
    qtbot.waitUntil(lambda: "is not saved" in rediger.status_label.text(), timeout=2000)
    rediger.save_now()
    saved = ObservationDB.get_observation(first)
    assert (saved["genus"], saved["species"]) == ("Inocybe", "geophylla")


def test_esc_keeps_the_page(qtbot, window, rediger):
    editor = rediger.editor()
    editor.location_input.setFocus(Qt.OtherFocusReason)
    qtbot.keyClick(editor.location_input, Qt.Key_Escape)
    assert rediger.editor() is editor and editor.isVisible()


@pytest.mark.parametrize("answer", [False, True])
def test_making_it_public_asks_at_the_change(qtbot, window, profile, monkeypatch, answer):
    import ui.publish_notice as publish_notice

    first = window.ids_for_test["first"]
    ObservationDB.update_observation(first, is_draft=0, sharing_scope="private")
    asked = []
    monkeypatch.setattr(publish_notice, "publish_notice_enabled", lambda: True)
    monkeypatch.setattr(publish_notice, "show_publish_notice", lambda *a, **k: asked.append(a) or answer)
    window.tab_widget.setCurrentWidget(window.observations_tab)
    window.activateWindow()
    qtbot.waitUntil(window.isActiveWindow, timeout=2000)
    qtbot.mouseClick(window.mode_bar.edit_button, Qt.LeftButton)
    page = window.edit_page
    qtbot.waitUntil(lambda: page.editor() is not None, timeout=5000)
    qtbot.wait(50)
    editor = page.editor()
    qtbot.mouseClick(editor.sharing_scope_public_radio, Qt.LeftButton)
    qtbot.waitUntil(lambda: bool(asked), timeout=2000)  # at the click, not a second later
    saved = ObservationDB.get_observation(first)
    if answer:
        assert saved["sharing_scope"] == "public"
    else:
        assert saved["sharing_scope"] == "private"
        assert editor._selected_sharing_scope() == "private"  # the control is back
    assert len(asked) == 1


def test_edit_images_opens_files_in_images(qtbot, window, rediger, profile):
    first = window.ids_for_test["first"]
    editor = rediger.editor()
    path = ImageDB.get_images_for_observation(first)[0]["filepath"]
    qtbot.mouseClick(editor.edit_images_btn, Qt.LeftButton)
    live = window.live_lab_tab
    assert window.tab_widget.currentWidget() is live
    assert live.current_source() == live.SOURCE_FILES
    assert window.files_source_page.observation_id() == first
    assert window.files_source_page.editor().import_results[0].filepath == path


def test_the_dialog_keeps_save_and_cancel(qtbot, profile):
    """The dialog itself (Ny observasjon and cloud-only rows until 9g4) keeps Save and Cancel."""
    from ui.observations_tab import ObservationDetailsDialog

    dialog = ObservationDetailsDialog(None, observation=None, image_results=[])
    qtbot.addWidget(dialog)
    dialog.show()
    assert dialog.submit_observation_btn.isVisible() and dialog.cancel_btn.isVisible()
    assert not dialog.is_in_place()
    dialog.reject()
    assert not dialog.isVisible()


def test_picking_a_name_saves_it(qtbot, window, monkeypatch, tmp_path_factory):
    from tests.test_observation_details_taxon_picker import _pick
    from ui import observation_details_dialog, observations_tab
    from utils import taxonomy_v2

    artifact = taxonomy_v2.ensure_installed(app_data_dir=tmp_path_factory.mktemp("taxonomy"))
    for module in (observations_tab, observation_details_dialog):
        monkeypatch.setattr(module, "resolve_vernacular_db_path", lambda _lang: artifact)
    first = window.ids_for_test["first"]
    window.tab_widget.setCurrentWidget(window.observations_tab)
    window.activateWindow()
    qtbot.waitUntil(window.isActiveWindow, timeout=2000)
    qtbot.mouseClick(window.mode_bar.edit_button, Qt.LeftButton)
    page = window.edit_page
    qtbot.waitUntil(lambda: page.editor() is not None, timeout=5000)
    qtbot.wait(50)
    _pick(page.editor(), "Russula", "Russula")
    qtbot.waitUntil(lambda: ObservationDB.get_observation(first)["genus"] == "Russula", timeout=3000)
    assert "is not saved" not in page.status_label.text()


# -- Codex review of stage 9g2 -------------------------------------------------


def _open_rediger(qtbot, window):
    window.tab_widget.setCurrentWidget(window.observations_tab)
    window.activateWindow()
    qtbot.waitUntil(window.isActiveWindow, timeout=2000)
    qtbot.mouseClick(window.mode_bar.edit_button, Qt.LeftButton)
    page = window.edit_page
    qtbot.waitUntil(lambda: page.editor() is not None, timeout=5000)
    qtbot.wait(50)
    return page


@pytest.mark.parametrize("selected", [True, False], ids=["a row chosen", "none chosen yet"])
def test_opening_with_saved_ai_predictions_writes_nothing(qtbot, window, monkeypatch, selected):
    import json

    from tools.review_ui.scenarios.observations import _flammulina_prediction

    first = window.ids_for_test["first"]
    image = ImageDB.get_images_for_observation(first)[0]
    prediction = _flammulina_prediction()
    ObservationDB.update_observation(first, ai_state_json=json.dumps({
        "predictions": {"0": [prediction]},
        "selected": {"0": prediction} if selected else {},
        "inat_predictions": {},
        "inat_selected": {},
        "selected_index": 0,
        "paths": [image["filepath"]],
        "image_ids": [int(image["id"])],
    }))
    writes = []
    original = ObservationDB.update_observation
    monkeypatch.setattr(
        ObservationDB, "update_observation",
        staticmethod(lambda *a, **k: writes.append(sorted(k)) or original(*a, **k)),
    )
    page = _open_rediger(qtbot, window)
    qtbot.wait(300)
    qtbot.mouseClick(window.mode_bar.view_button, Qt.LeftButton)
    assert writes == [] and page.editor() is not None


def test_a_change_made_elsewhere_meanwhile_is_kept(qtbot, window, rediger):
    first = window.ids_for_test["first"]
    # A sync (or another page) changes the open comment while Rediger is open.
    ObservationDB.update_observation(first, open_comment="From the web")
    _type_into(qtbot, rediger.editor().location_input, "Lade")
    qtbot.waitUntil(lambda: ObservationDB.get_observation(first)["location"] == "Lade", timeout=3000)
    assert ObservationDB.get_observation(first)["open_comment"] == "From the web"


def test_enter_in_a_field_keeps_the_page(qtbot, window, rediger):
    editor = rediger.editor()
    _type_into(qtbot, editor.location_input, "Lade")
    qtbot.keyClick(editor.location_input, Qt.Key_Return)
    assert editor.isVisible() and rediger.editor() is editor


def test_a_failed_save_says_so_and_keeps_the_change(qtbot, window, rediger, monkeypatch):
    first = window.ids_for_test["first"]
    tab = window.observations_tab
    working = tab.write_observation_edits

    def locked(*_args, **_kwargs):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(tab, "write_observation_edits", locked)
    _type_into(qtbot, rediger.editor().location_input, "Lade")
    assert rediger.save_now() is False
    assert rediger.status_label.text() == "Not saved: database is locked"
    monkeypatch.setattr(tab, "write_observation_edits", working)
    assert rediger.save_now() is True  # the change is still pending, and saves
    assert ObservationDB.get_observation(first)["location"] == "Lade"


def test_a_deleted_observation_leaves_rediger_empty(qtbot, window, rediger):
    first = window.ids_for_test["first"]
    ObservationDB.delete_observation(first)
    window.on_observation_deleted(first)
    assert rediger.editor() is None and rediger.observation_id() is None


def test_the_notice_asks_only_when_publishing_changes(qtbot, window, monkeypatch):
    import ui.publish_notice as publish_notice

    first = window.ids_for_test["first"]
    ObservationDB.update_observation(first, is_draft=0, sharing_scope="private")
    asked = []
    monkeypatch.setattr(publish_notice, "publish_notice_enabled", lambda: True)
    monkeypatch.setattr(publish_notice, "show_publish_notice", lambda *a, **k: asked.append(a) or True)
    page = _open_rediger(qtbot, window)
    qtbot.mouseClick(page.editor().sharing_scope_public_radio, Qt.LeftButton)
    qtbot.waitUntil(lambda: ObservationDB.get_observation(first)["sharing_scope"] == "public", timeout=3000)
    _type_into(qtbot, page.editor().location_input, "Lade")
    qtbot.waitUntil(lambda: ObservationDB.get_observation(first)["location"] == "Lade", timeout=3000)
    assert len(asked) == 1


def test_coming_back_shows_what_is_saved_now(qtbot, window, rediger):
    first = window.ids_for_test["first"]
    qtbot.mouseClick(window.mode_bar.view_button, Qt.LeftButton)
    ObservationDB.update_observation(first, location="Changed in Vis")
    qtbot.mouseClick(window.mode_bar.edit_button, Qt.LeftButton)
    qtbot.waitUntil(
        lambda: rediger.editor() is not None and rediger.editor().location_input.text() == "Changed in Vis",
        timeout=3000,
    )


# -- Codex follow-up review of stage 9g2 ----------------------------------------


def test_a_failed_save_asks_before_switching(qtbot, window, rediger, monkeypatch):
    from PySide6.QtWidgets import QMessageBox

    first, other = window.ids_for_test["first"], window.ids_for_test["other"]
    tab = window.observations_tab
    working = tab.write_observation_edits

    def locked(*_args, **_kwargs):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(tab, "write_observation_edits", locked)
    _type_into(qtbot, rediger.editor().location_input, "Lade")
    asked = []

    def answer(box):  # the database is back; the user presses Try again
        asked.append(box.text())
        monkeypatch.setattr(tab, "write_observation_edits", working)
        return 0

    monkeypatch.setattr(QMessageBox, "exec", answer)
    monkeypatch.setattr(
        QMessageBox, "clickedButton",
        lambda box: next(b for b in box.buttons() if b.text() == "Try again"),
    )
    window.on_observation_selected(other, "Other", switch_tab=False)
    assert asked and "database is locked" in asked[0]
    assert ObservationDB.get_observation(first)["location"] == "Lade"
    qtbot.waitUntil(lambda: rediger.observation_id() == other, timeout=3000)


def test_a_name_changed_elsewhere_is_kept(qtbot, window, rediger):
    first = window.ids_for_test["first"]
    ObservationDB.update_observation(first, genus="Russula", species="emetica")  # a sync meanwhile
    _type_into(qtbot, rediger.editor().location_input, "Lade")
    qtbot.waitUntil(lambda: ObservationDB.get_observation(first)["location"] == "Lade", timeout=3000)
    saved = ObservationDB.get_observation(first)
    assert (saved["genus"], saved["species"]) == ("Russula", "emetica")


def test_ai_suggestions_come_from_the_database(qtbot, window):
    first = window.ids_for_test["first"]
    window.observations_tab._ai_suggestions_cache[first] = {"predictions": {"0": [{"taxon_id": "stale"}]}}
    page = _open_rediger(qtbot, window)
    assert not page.editor().get_ai_state()["predictions"]


def test_a_suggestion_chosen_from_a_list_saves(qtbot, window, rediger):
    first = window.ids_for_test["first"]
    editor = rediger.editor()
    editor._set_location_lookup_suggestions(["Trondheim, Lade"])
    # The completer's popup sits outside the form; choosing its row sends this.
    editor.location_input.completer().activated.emit("Trondheim, Lade")
    assert editor.location_input.text() == "Trondheim, Lade"
    qtbot.waitUntil(lambda: ObservationDB.get_observation(first)["location"] == "Trondheim, Lade", timeout=3000)


# -- Codex second follow-up review of stage 9g2 ---------------------------------


def test_the_notice_judges_what_will_be_saved(qtbot, window, monkeypatch):
    """Made public elsewhere meanwhile: showing the exact position asks, though the form opened private."""
    import ui.publish_notice as publish_notice

    first = window.ids_for_test["first"]
    ObservationDB.update_observation(first, is_draft=0, sharing_scope="private", location_precision="fuzzed",
                                     gps_latitude=63.4, gps_longitude=10.4)
    asked = []
    monkeypatch.setattr(publish_notice, "show_publish_notice", lambda *a, **k: asked.append(k) or False)
    page = _open_rediger(qtbot, window)
    ObservationDB.update_observation(first, sharing_scope="public", location_public=1)  # another device
    qtbot.mouseClick(page.editor().location_precision_exact_radio, Qt.LeftButton)
    qtbot.waitUntil(lambda: bool(asked), timeout=3000)
    assert ObservationDB.get_observation(first)["location_precision"] == "fuzzed"  # Cancel: not shown


def test_a_click_that_only_opens_something_writes_nothing(qtbot, window, rediger, monkeypatch):
    from PySide6.QtCore import QPoint, QPointF
    from PySide6.QtGui import QDesktopServices, QWheelEvent
    from PySide6.QtWidgets import QApplication

    first = window.ids_for_test["first"]
    before = ObservationDB.get_observation(first)
    editor = rediger.editor()
    opened = []
    monkeypatch.setattr(QDesktopServices, "openUrl", staticmethod(lambda url: opened.append(url) or True))
    qtbot.mouseClick(editor.maplink_open_btn, Qt.LeftButton)
    assert opened  # the map opened (in a stand-in, not a browser)
    wheel = QWheelEvent(QPointF(20, 20), QPointF(20, 20), QPoint(0, 0), QPoint(0, -120),
                        Qt.NoButton, Qt.NoModifier, Qt.NoScrollPhase, False)
    QApplication.sendEvent(editor.open_comment_input, wheel)  # scrolling
    qtbot.wait(1300)
    qtbot.mouseClick(window.mode_bar.view_button, Qt.LeftButton)
    assert ObservationDB.get_observation(first) == before


def test_what_the_form_fills_in_saves_only_that_field(qtbot, window, rediger):
    """A place name looked up for the position saves, as Save did; a field changed elsewhere stays."""
    first = window.ids_for_test["first"]
    ObservationDB.update_observation(first, open_comment="From the web")  # a sync meanwhile
    rediger.editor().location_input.setText("Looked up")  # the form filling a field on its own
    qtbot.waitUntil(lambda: ObservationDB.get_observation(first)["location"] == "Looked up", timeout=3000)
    assert ObservationDB.get_observation(first)["open_comment"] == "From the web"



# -- Codex third follow-up review of stage 9g2 ----------------------------------


def test_cancel_writes_nothing_about_publishing(qtbot, window, monkeypatch):
    """Cancel never writes sharing: not even what the notice put back on the controls."""
    import ui.publish_notice as publish_notice

    first = window.ids_for_test["first"]
    ObservationDB.update_observation(first, is_draft=0, sharing_scope="private", location_precision="fuzzed",
                                     gps_latitude=63.4, gps_longitude=10.4)
    page = _open_rediger(qtbot, window)
    ObservationDB.update_observation(first, sharing_scope="public", location_public=1)  # another device

    def cancel_while_made_private_again(*_args, **_kwargs):
        ObservationDB.update_observation(first, sharing_scope="private", location_public=0)  # and again
        return False

    monkeypatch.setattr(publish_notice, "show_publish_notice", cancel_while_made_private_again)
    tab = window.observations_tab
    working = tab.write_observation_edits
    written = []
    monkeypatch.setattr(tab, "write_observation_edits",
                        lambda *a, **k: written.append(set(k.get("only_keys") or ())) or working(*a, **k))
    qtbot.mouseClick(page.editor().location_precision_exact_radio, Qt.LeftButton)
    qtbot.wait(1300)
    saved = ObservationDB.get_observation(first)
    assert saved["sharing_scope"] == "private" and saved["location_precision"] == "fuzzed"
    publishing = {"is_draft", "sharing_scope", "location_public", "location_precision"}
    assert not any(keys & publishing for keys in written)  # nothing about publishing was written


def test_a_wheel_turn_on_a_number_is_an_edit(qtbot, window, rediger):
    from PySide6.QtCore import QPoint, QPointF
    from PySide6.QtGui import QWheelEvent
    from PySide6.QtWidgets import QApplication

    first = window.ids_for_test["first"]
    ObservationDB.update_observation(first, gps_latitude=63.4, gps_longitude=10.4)
    rediger.reload()
    qtbot.wait(100)
    field = rediger.editor().lat_input
    before = field.value()
    centre = QPointF(field.width() / 2, field.height() / 2)
    wheel = QWheelEvent(centre, field.mapToGlobal(centre), QPoint(0, 0), QPoint(0, 120),
                        Qt.NoButton, Qt.NoModifier, Qt.NoScrollPhase, False)
    QApplication.sendEvent(field, wheel)
    assert field.value() != before
    qtbot.waitUntil(
        lambda: abs(float(ObservationDB.get_observation(first)["gps_latitude"]) - field.value()) < 1e-9,
        timeout=3000,
    )


# -- Codex fresh review of stage 9g2: the notice is modal --------------------------


def _modal_notice(monkeypatch, during, answer=True):
    """The publish notice as a modal: a nested event loop runs ``during`` while it is open."""
    import ui.publish_notice as publish_notice
    from PySide6.QtCore import QEventLoop, QTimer

    def show(*_args, **_kwargs):
        loop = QEventLoop()
        QTimer.singleShot(0, during)
        QTimer.singleShot(200, loop.quit)
        loop.exec()
        return answer

    monkeypatch.setattr(publish_notice, "publish_notice_enabled", lambda: True)
    monkeypatch.setattr(publish_notice, "show_publish_notice", show)


def _private(first):
    ObservationDB.update_observation(first, is_draft=0, sharing_scope="private")


def test_a_change_while_the_notice_is_open_is_kept(qtbot, window, monkeypatch):
    first = window.ids_for_test["first"]
    _private(first)
    page = _open_rediger(qtbot, window)
    _modal_notice(monkeypatch, lambda: page.editor().location_input.setText("Looked up meanwhile"))
    qtbot.mouseClick(page.editor().sharing_scope_public_radio, Qt.LeftButton)
    qtbot.waitUntil(lambda: ObservationDB.get_observation(first)["sharing_scope"] == "public", timeout=3000)
    qtbot.waitUntil(lambda: ObservationDB.get_observation(first)["location"] == "Looked up meanwhile", timeout=3000)


def test_switching_while_the_notice_is_open_happens_once_after(qtbot, window, monkeypatch):
    first, other = window.ids_for_test["first"], window.ids_for_test["other"]
    _private(first)
    page = _open_rediger(qtbot, window)
    calls = []
    original = page.show_observation
    monkeypatch.setattr(page, "show_observation", lambda obs, **kw: calls.append(obs) or original(obs, **kw))
    _modal_notice(monkeypatch, lambda: window.on_observation_selected(other, "Other", switch_tab=False))
    qtbot.mouseClick(page.editor().sharing_scope_public_radio, Qt.LeftButton)
    qtbot.waitUntil(lambda: page.observation_id() == other, timeout=3000)
    assert ObservationDB.get_observation(first)["sharing_scope"] == "public"
    assert len(calls) < 5  # once asked, once done; no loop while the notice was open


def test_deleted_while_the_notice_is_open(qtbot, window, monkeypatch):
    first = window.ids_for_test["first"]
    _private(first)
    page = _open_rediger(qtbot, window)

    def delete():
        ObservationDB.delete_observation(first)
        window.on_observation_deleted(first)

    _modal_notice(monkeypatch, delete)
    qtbot.mouseClick(page.editor().sharing_scope_public_radio, Qt.LeftButton)
    qtbot.waitUntil(lambda: page.editor() is None, timeout=3000)
    assert ObservationDB.get_observation(first) is None


def test_leaving_rediger_after_a_failed_save_asks(qtbot, window, rediger, monkeypatch):
    from PySide6.QtWidgets import QMessageBox

    tab = window.observations_tab

    def locked(*_args, **_kwargs):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(tab, "write_observation_edits", locked)
    _type_into(qtbot, rediger.editor().location_input, "Lade")
    asked = []
    monkeypatch.setattr(QMessageBox, "exec", lambda box: asked.append(box.text()) or 0)
    monkeypatch.setattr(
        QMessageBox, "clickedButton",
        lambda box: next(b for b in box.buttons() if b.text() == "Discard it"),
    )
    qtbot.mouseClick(window.mode_bar.view_button, Qt.LeftButton)
    assert asked and "database is locked" in asked[0]


# -- Codex focused check of stage 9g2 -------------------------------------------


def test_a_newer_switch_wins_over_a_held_one(qtbot, window, profile):
    """A switch held while the notice was open never undoes one made after it."""
    from tools.review_ui.live_lab_harness import create_observation

    other = window.ids_for_test["other"]
    third = create_observation(profile, "third", genus="Fomes", species="fomentarius")
    page = _open_rediger(qtbot, window)
    held_token = page._show_token  # a request held before the next switch
    page.show_observation(third)
    page.show_observation(other, _token=held_token)  # the held one runs late
    assert page.observation_id() == third

def test_ai_suggestions_saved_elsewhere_are_kept(qtbot, window, rediger):
    import json

    first = window.ids_for_test["first"]
    newer = json.dumps({"predictions": {"0": [{"taxon_id": "NBIC:1"}]}, "selected": {}})
    ObservationDB.update_observation(first, ai_state_json=newer)  # another page saved AI results
    _type_into(qtbot, rediger.editor().location_input, "Lade")
    qtbot.waitUntil(lambda: ObservationDB.get_observation(first)["location"] == "Lade", timeout=3000)
    assert ObservationDB.get_observation(first)["ai_state_json"] == newer


def test_quitting_waits_for_the_notice(qtbot, window, monkeypatch):
    from PySide6.QtGui import QCloseEvent

    first = window.ids_for_test["first"]
    _private(first)
    page = _open_rediger(qtbot, window)
    results = []

    def try_to_quit():
        event = QCloseEvent()
        window.closeEvent(event)
        results.append(event.isAccepted())

    _modal_notice(monkeypatch, try_to_quit)
    qtbot.mouseClick(page.editor().sharing_scope_public_radio, Qt.LeftButton)
    qtbot.waitUntil(lambda: bool(results), timeout=3000)
    assert results == [False] and page.editor() is not None


def test_ai_results_that_could_not_be_stored_are_tried_again(qtbot, window, rediger, monkeypatch):
    import json

    first = window.ids_for_test["first"]
    editor = rediger.editor()
    editor._persist_ai_paused = False
    original = ObservationDB.update_observation

    def locked(*args, **kwargs):
        if "ai_state_json" in kwargs:
            raise RuntimeError("database is locked")
        return original(*args, **kwargs)

    monkeypatch.setattr(ObservationDB, "update_observation", staticmethod(locked))
    editor._ai_predictions_by_index[0] = [{"taxon_id": "NBIC:1", "probability": 0.9}]
    editor._persist_ai_state_now()  # an AI result arrives; its own write fails quietly
    qtbot.waitUntil(lambda: rediger.status_label.text().startswith("Not saved"), timeout=3000)
    monkeypatch.setattr(ObservationDB, "update_observation", staticmethod(original))
    qtbot.waitUntil(lambda: "NBIC:1" in (ObservationDB.get_observation(first)["ai_state_json"] or ""), timeout=4000)
    qtbot.waitUntil(lambda: not rediger.status_label.text().startswith("Not saved"), timeout=3000)
    assert json.loads(ObservationDB.get_observation(first)["ai_state_json"])["predictions"]


# -- Owner's first try of stage 9g2 (2026-11-11) ------------------------------


def test_coming_back_keeps_the_form_when_nothing_changed(qtbot, window, rediger):
    """Building the form takes seconds on real data: coming back reuses it."""
    first = window.ids_for_test["first"]
    editor = rediger.editor()
    _type_into(qtbot, editor.location_input, "Lade")
    qtbot.mouseClick(window.mode_bar.view_button, Qt.LeftButton)
    assert ObservationDB.get_observation(first)["location"] == "Lade"  # saved on leaving
    qtbot.mouseClick(window.mode_bar.edit_button, Qt.LeftButton)
    qtbot.wait(100)
    assert rediger.editor() is editor and editor.isVisible()
    assert editor.location_input.text() == "Lade"


def test_coming_back_after_new_images_starts_again(qtbot, window, rediger, profile):
    first = window.ids_for_test["first"]
    editor = rediger.editor()
    qtbot.mouseClick(window.mode_bar.view_button, Qt.LeftButton)
    ImageDB.add_image(first, str(profile.jpeg("added.JPG", seed=9)), "field")  # Bilder added one
    qtbot.mouseClick(window.mode_bar.edit_button, Qt.LeftButton)
    qtbot.waitUntil(lambda: rediger.editor() is not None and rediger.editor() is not editor, timeout=3000)
    assert len(rediger.editor().image_results) == 2


def test_the_bottom_bar_stays_in_view_below_the_form(qtbot, window, rediger):
    editor = rediger.editor()
    bar = editor.hint_bar.parentWidget()
    assert bar.objectName() == "editBottomBar"
    assert not editor.isAncestorOf(bar)  # pinned below the scrolling form, not in it
    assert bar.isVisible() and editor.edit_images_btn.isVisible() and rediger.status_label.isVisible()
    window.resize(1280, 700)  # lower than the form
    qtbot.wait(50)
    frame = rediger._frame
    assert frame.verticalScrollBar().maximum() > 0  # the form scrolls
    bottom = bar.mapTo(window, bar.rect().bottomLeft()).y()
    assert bottom <= window.height()  # the bar does not


def test_a_tall_window_gives_its_height_to_the_form_not_the_bottom_bar(qtbot, window, rediger):
    """The bottom row took most spare height (an empty band to scroll into)."""
    window.resize(1500, 1300)
    qtbot.wait(50)
    editor = rediger.editor()
    bar = editor.hint_bar.parentWidget()
    assert bar.height() <= editor.edit_images_btn.height() + 12
    frame = rediger._frame
    assert frame.verticalScrollBar().maximum() == 0  # nothing to scroll into
    assert editor.height() == frame.viewport().height()


def test_a_change_made_elsewhere_before_a_save_still_reloads(qtbot, window, rediger):
    """A save writes only its own columns; it must not hide another change from the form."""
    first = window.ids_for_test["first"]
    editor = rediger.editor()
    ObservationDB.update_observation(first, gps_latitude=63.4, gps_longitude=10.4)  # a sync, meanwhile
    _type_into(qtbot, editor.location_input, "Lade")
    qtbot.waitUntil(lambda: ObservationDB.get_observation(first)["location"] == "Lade", timeout=3000)
    assert ObservationDB.get_observation(first)["gps_longitude"] == 10.4
    qtbot.mouseClick(window.mode_bar.view_button, Qt.LeftButton)
    qtbot.mouseClick(window.mode_bar.edit_button, Qt.LeftButton)
    qtbot.waitUntil(lambda: rediger.editor() is not None and rediger.editor() is not editor, timeout=3000)
    assert abs(rediger.editor().lon_input.value() - 10.4) < 1e-6


def test_a_new_name_language_reloads_the_form(qtbot, window, rediger):
    from database.models import SettingsDB

    editor = rediger.editor()
    qtbot.mouseClick(window.mode_bar.view_button, Qt.LeftButton)
    SettingsDB.set_setting("vernacular_language", "sv")
    qtbot.mouseClick(window.mode_bar.edit_button, Qt.LeftButton)
    qtbot.waitUntil(lambda: rediger.editor() is not None and rediger.editor() is not editor, timeout=3000)


def test_edit_images_shortcut_works_from_the_bottom_bar(qtbot, window, rediger):
    editor = rediger.editor()
    opened = []
    editor.editImagesRequested.disconnect()
    editor.editImagesRequested.connect(opened.append)
    editor.edit_images_btn.setFocus(Qt.OtherFocusReason)
    qtbot.waitUntil(editor.edit_images_btn.hasFocus, timeout=2000)
    qtbot.keyClick(editor.edit_images_btn, Qt.Key_E, Qt.ControlModifier if __import__("sys").platform == "darwin" else Qt.AltModifier)
    assert len(opened) == 1


def test_every_edit_images_entry_asks_the_page_with_its_photo(qtbot, window, rediger):
    """Edit images, a thumbnail's double-click and its Edit photo all ask the
    page to open Bilder (the form is never a window that closes for it)."""
    editor = rediger.editor()
    opened = []
    editor.editImagesRequested.disconnect()
    editor.editImagesRequested.connect(opened.append)
    # Unfinished species search text asks nothing first: in place it is never saved.
    editor.taxon_picker.begin_edit()
    qtbot.keyClicks(editor.taxon_picker.search_input, "Gymnopus dryo")
    assert editor.unfinished_taxon_text() == "Gymnopus dryo"

    qtbot.mouseClick(editor.edit_images_btn, Qt.LeftButton)
    editor.image_gallery.imageDoubleClicked.emit(1, "/photos/a.jpg")
    editor.image_gallery.editRequested.emit(2, " /photos/b.jpg ")

    assert opened == ["", "/photos/a.jpg", "/photos/b.jpg"]
    assert editor.isVisible()


def test_coming_back_after_ai_results_keeps_the_form(qtbot, window, rediger):
    """The form stores AI results itself when they arrive; that is no reason to rebuild it."""
    first = window.ids_for_test["first"]
    editor = rediger.editor()
    editor._persist_ai_paused = False
    editor._ai_predictions_by_index[0] = [{"taxon_id": "NBIC:1", "probability": 0.9}]
    editor._persist_ai_state_now()  # an AI result arrives
    assert "NBIC:1" in (ObservationDB.get_observation(first)["ai_state_json"] or "")
    qtbot.mouseClick(window.mode_bar.view_button, Qt.LeftButton)
    qtbot.mouseClick(window.mode_bar.edit_button, Qt.LeftButton)
    qtbot.wait(100)
    assert rediger.editor() is editor


def test_forms_share_the_cloud_usage_answer(qtbot, profile, monkeypatch):
    """Two cloud requests per form made Rediger slow on a signed-in profile."""
    from types import SimpleNamespace

    from ui import observation_details_dialog as details

    calls = []

    def fake_client():
        return SimpleNamespace(
            user_id="user-1",
            fetch_cloud_plan_profile=lambda: calls.append("profile") or {"cloud_plan": "free"},
            count_remote_privacy_slots=lambda: calls.append("count") or 2,
        )

    client = fake_client()
    monkeypatch.setattr(details.ObservationDetailsDialog, "_cloud_privacy_usage_client", lambda self: client)
    for _ in range(2):
        dialog = details.ObservationDetailsDialog(None, observation=None, image_results=[])
        qtbot.addWidget(dialog)
        dialog._refresh_cloud_privacy_slots_summary(force=True)
        assert "of" in dialog.cloud_privacy_slots_label.text()  # “Available private slots: … of …”
    assert calls == ["profile", "count"]
    client = fake_client()  # signed in again: a new client asks again
    dialog._refresh_cloud_privacy_slots_summary(force=True)
    assert calls == ["profile", "count", "profile", "count"]
