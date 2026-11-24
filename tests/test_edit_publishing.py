"""Rediger, Status and publishing (redesign stage 9g3b).

Real ``MainWindow`` in an isolated profile (the fixtures of
``tests/test_files_source.py``); no network: today's Publish is replaced by a
recorder, so the tests check what Rediger asks it to do. Owner 2026-11-12: the
steps Draft → Finished → Published to a service replace the draft checkbox;
“Finished and publish to …” clears the draft flag, then runs today's Publish for
the service chosen with ▾; sharing stays its own setting.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt

from database.models import ObservationDB
from tests.test_edit_mode import rediger  # noqa: F401
from tests.test_files_source import profile, window  # noqa: F401


@pytest.fixture
def published(monkeypatch, window):
    """Artsobservasjoner and iNaturalist enabled; Publish records its calls."""
    import ui.publish_notice as publish_notice

    tab = window.observations_tab
    calls = []
    during = []  # run inside Publish, as its own progress events would

    def record(key, **kwargs):
        for action in list(during):
            action()
        calls.append((key, kwargs.get("observation_ids"), tab._publish_observation_ids()))

    enabled = ["web", "inat"]
    monkeypatch.setattr(tab, "_enabled_publish_uploader_keys", lambda uploaders=None: list(enabled))
    monkeypatch.setattr(tab, "_publish_selected_observations", record)
    tab.ids_for_test_publish = (calls, during, enabled)
    answers = []
    monkeypatch.setattr(publish_notice, "show_publish_notice", lambda *a, **k: answers.pop(0) if answers else True)
    return calls, answers


def _open(qtbot, rediger):
    rediger._refresh_publish_state()
    return rediger.editor()


def _stored(window):
    return ObservationDB.get_observation(window.ids_for_test["first"])


def test_the_steps_replace_the_draft_checkbox(qtbot, window, rediger, published):
    editor = _open(qtbot, rediger)
    assert not editor.is_draft_checkbox.isVisible()
    steps = editor.status_step_buttons
    assert [button.isVisible() for button in steps] == [True, True, True]
    assert "Draft" in steps[0].text() and "now" in steps[0].text()


def test_finished_and_draft_save_the_draft_flag(qtbot, window, rediger, published):
    editor = _open(qtbot, rediger)
    qtbot.mouseClick(editor.status_step_buttons[1], Qt.LeftButton)
    qtbot.waitUntil(lambda: not _stored(window)["is_draft"], timeout=3000)
    assert "now" in editor.status_step_buttons[1].text() and editor.status_badge.text().endswith("Finished")
    qtbot.mouseClick(editor.status_step_buttons[0], Qt.LeftButton)
    qtbot.waitUntil(lambda: bool(_stored(window)["is_draft"]), timeout=3000)


def test_finished_and_publish_saves_then_publishes_this_observation(qtbot, window, rediger, published):
    calls, _answers = published
    first = window.ids_for_test["first"]
    editor = _open(qtbot, rediger)
    sharing_before = _stored(window)["sharing_scope"]
    assert editor.publish_service_button.text() == "Finished and publish to Artsobservasjoner"
    qtbot.mouseClick(editor.publish_service_button, Qt.LeftButton)
    assert not _stored(window)["is_draft"]  # saved before publishing
    assert calls == [("web", [first], [first])]
    assert _stored(window)["sharing_scope"] == sharing_before  # sharing never changes here


def test_cancelling_the_notice_publishes_nothing(qtbot, window, rediger, published):
    calls, answers = published
    editor = _open(qtbot, rediger)
    if editor._selected_sharing_scope() != "public":
        pytest.skip("the fixture observation is not public: no notice to cancel")
    answers.append(False)
    qtbot.mouseClick(editor.publish_service_button, Qt.LeftButton)
    assert _stored(window)["is_draft"] and editor.is_draft_checkbox.isChecked()
    assert calls == []


def test_both_publishes_this_observation_even_if_the_list_changes(qtbot, window, rediger, published):
    calls, _answers = published
    first = window.ids_for_test["first"]
    editor = _open(qtbot, rediger)
    tab = window.observations_tab
    tab.ids_for_test_publish[1].append(tab.table.clearSelection)  # a search empties the list meanwhile
    editor.publish_service_menu.actions()[2].trigger()  # Both
    qtbot.waitUntil(lambda: bool(calls), timeout=3000)
    assert calls == [("both", None, [first])]
    assert getattr(tab, "_publish_pinned_observation_ids", None) is None  # the list is the list again


def test_the_menu_chooses_another_service(qtbot, window, rediger, published):
    calls, _answers = published
    first = window.ids_for_test["first"]
    editor = _open(qtbot, rediger)
    labels = [action.text() for action in editor.publish_service_menu.actions()]
    assert labels == ["Artsobservasjoner", "iNaturalist", "Both"]
    editor.publish_service_menu.actions()[1].trigger()
    qtbot.waitUntil(lambda: bool(calls), timeout=3000)
    assert calls == [("inat", [first], [first])]
    assert editor.publish_service_button.text() == "Publish to iNaturalist"  # remembered, and now Finished


def test_published_to_a_service_is_the_last_step(qtbot, window, rediger, published):
    first = window.ids_for_test["first"]
    ObservationDB.update_observation(first, is_draft=0, artsdata_id=123456)
    rediger.reload()
    editor = _open(qtbot, rediger)
    assert "Published to Artsobservasjoner" in editor.status_step_buttons[2].text()
    assert "now" in editor.status_step_buttons[2].text()
    actions = {action.text(): action.isEnabled() for action in editor.publish_service_menu.actions()}
    assert actions == {"Artsobservasjoner": False, "iNaturalist": True, "Both": False}


def test_the_dialog_keeps_its_draft_checkbox(qtbot, profile):
    from ui.observations_tab import ObservationDetailsDialog

    dialog = ObservationDetailsDialog(None, observation=None, image_results=[])
    qtbot.addWidget(dialog)
    dialog.show()
    assert dialog.is_draft_checkbox.isVisible()
    assert not getattr(dialog, "status_step_buttons", None)


# -- Codex review of stage 9g3b -------------------------------------------------


def test_a_cancelled_notice_publishes_nothing_even_if_stored_finished(qtbot, window, rediger, monkeypatch, published):
    import ui.publish_notice as publish_notice

    calls, _answers = published
    first = window.ids_for_test["first"]
    editor = _open(qtbot, rediger)

    def cancel_while_another_writer_finishes(*args, **kwargs):
        ObservationDB.update_observation(first, is_draft=0)  # meanwhile, elsewhere
        return False

    monkeypatch.setattr(publish_notice, "show_publish_notice", cancel_while_another_writer_finishes)
    qtbot.mouseClick(editor.publish_service_button, Qt.LeftButton)
    assert editor.is_draft_checkbox.isChecked()
    assert calls == []


def test_a_second_click_while_publishing_is_ignored(qtbot, window, rediger, published):
    calls, _answers = published
    editor = _open(qtbot, rediger)
    tab = window.observations_tab
    clicks = []

    def click_again():
        clicks.append(editor.publish_service_button.isEnabled())
        editor.publish_service_button.click()
        rediger._on_publish_requested(window.ids_for_test["first"], "web")

    tab.ids_for_test_publish[1].append(click_again)
    qtbot.mouseClick(editor.publish_service_button, Qt.LeftButton)
    assert clicks == [False] and len(calls) == 1
    assert editor.publish_service_button.isEnabled()  # available again afterwards


def test_a_service_switched_off_meanwhile_is_not_published(qtbot, window, rediger, published):
    calls, _answers = published
    editor = _open(qtbot, rediger)
    window.observations_tab.ids_for_test_publish[2].clear()  # Settings › Online publishing: all off
    qtbot.mouseClick(editor.publish_service_button, Qt.LeftButton)
    assert calls == []
    assert not editor.publish_service_button.isEnabled()


def test_a_change_made_during_the_upload_still_reloads_the_form(qtbot, window, rediger, published):
    first = window.ids_for_test["first"]
    editor = _open(qtbot, rediger)
    tab = window.observations_tab
    tab.ids_for_test_publish[1].append(
        lambda: ObservationDB.update_observation(first, location="Synced meanwhile", artsdata_id=777)
    )
    qtbot.mouseClick(editor.publish_service_button, Qt.LeftButton)
    qtbot.mouseClick(window.mode_bar.view_button, Qt.LeftButton)
    qtbot.mouseClick(window.mode_bar.edit_button, Qt.LeftButton)
    qtbot.waitUntil(lambda: rediger.editor() is not None and rediger.editor() is not editor, timeout=3000)
    assert rediger.editor().location_input.text() == "Synced meanwhile"


# -- Codex second review of stage 9g3b ------------------------------------------


def test_vis_cannot_publish_while_rediger_publishes(qtbot, window, rediger, published):
    first = window.ids_for_test["first"]
    other = window.ids_for_test["other"]
    editor = _open(qtbot, rediger)
    tab = window.observations_tab
    seen = []

    def in_vis_meanwhile():
        tab.table.selectRow(tab._find_table_row_for_observation(other))
        seen.append((tab.publish_btn.isEnabled(), tab._selected_observation_ids(), tab._publish_observation_ids()))

    tab.ids_for_test_publish[1].append(in_vis_meanwhile)
    qtbot.mouseClick(editor.publish_service_button, Qt.LeftButton)
    # Vis' Publish is off; the list's own selection stays the list's (only Publish is pinned).
    assert seen == [(False, [other], [first])]
    assert tab._publish_observation_ids() == [other]  # released afterwards


def test_a_replaced_form_cannot_publish(qtbot, window, rediger, published):
    calls, _answers = published
    old = _open(qtbot, rediger)
    rediger.reload()  # the form is rebuilt for the same observation
    assert rediger.editor() is not old
    old.publishRequested.emit("web")  # a request still queued by the old form
    assert calls == []


def test_coming_back_during_an_upload_keeps_the_form_until_it_ends(qtbot, window, rediger, published):
    first = window.ids_for_test["first"]
    editor = _open(qtbot, rediger)
    tab = window.observations_tab
    inside = []

    def leave_and_return_meanwhile():
        ObservationDB.update_observation(first, location="Synced meanwhile")
        window.mode_bar.view_button.click()
        window.mode_bar.edit_button.click()
        qtbot.wait(50)
        inside.append(rediger.editor() is editor)

    tab.ids_for_test_publish[1].append(leave_and_return_meanwhile)
    qtbot.mouseClick(editor.publish_service_button, Qt.LeftButton)
    assert inside == [True]  # not rebuilt under the running Publish
    qtbot.waitUntil(lambda: rediger.editor() is not None and rediger.editor() is not editor, timeout=3000)
    assert rediger.editor().location_input.text() == "Synced meanwhile"


def test_a_reload_queued_after_publish_waits_until_rediger_shows(qtbot, window, rediger, published):
    first = window.ids_for_test["first"]
    editor = _open(qtbot, rediger)
    rediger._stale = True
    window.mode_bar.view_button.click()  # hidden before the queued check runs
    rediger._reload_if_changed()
    assert rediger._stale  # still checked when Rediger shows again
    ObservationDB.update_observation(first, location="Changed while hidden")
    window.mode_bar.edit_button.click()
    qtbot.waitUntil(lambda: rediger.editor() is not None and rediger.editor() is not editor, timeout=3000)
    assert rediger.editor().location_input.text() == "Changed while hidden"


def test_a_queued_switch_is_not_cancelled_by_a_reload(qtbot, window, rediger, published):
    other = window.ids_for_test["other"]
    _open(qtbot, rediger)
    ObservationDB.update_observation(window.ids_for_test["first"], location="Changed meanwhile")  # a reload is due
    rediger._stale = True
    rediger._pending_show, rediger._has_pending_show = other, True
    rediger._run_pending_show()
    rediger._reload_if_changed()  # queued earlier, runs first
    qtbot.waitUntil(lambda: rediger.observation_id() == other, timeout=3000)
