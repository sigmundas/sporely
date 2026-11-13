"""Stage 10b: the shared non-blocking notice in a screen's bottom (hint) bar."""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QEvent, Qt
from PySide6.QtWidgets import QApplication, QPushButton, QVBoxLayout, QWidget

from ui.hint_status import HintBar, HintProgressStack, HintStatusController

LONG_DETAILS = "\n".join(f"line {i}: the server said no" for i in range(1, 15))


@pytest.fixture
def bar_and_controller(qtbot):
    host = QWidget()
    layout = QVBoxLayout(host)
    button = QPushButton("Save", host)
    layout.addWidget(button)
    bar = HintBar(host)
    layout.addWidget(bar)
    controller = HintStatusController(bar, host)
    controller.register_widget(button, "Saves the observation")
    qtbot.addWidget(host)
    host.resize(700, 200)
    host.show()
    qtbot.waitExposed(host)
    return host, bar, controller, button


def _hover(button, entering: bool) -> None:
    QApplication.sendEvent(button, QEvent(QEvent.Enter if entering else QEvent.Leave))


def test_an_error_stays_until_dismissed_and_details_open_in_the_bar(qtbot, bar_and_controller):
    _host, bar, controller, _button = bar_and_controller
    controller.set_baseline("2 images")
    controller.show_notice("Could not upload the images.", "error", details=LONG_DETAILS)
    assert bar._label.text() == "Could not upload the images."
    assert not bar._details_btn.isHidden() and not bar._dismiss_btn.isHidden()
    assert bar._undo_btn.isHidden()
    assert not controller._notice_timer.isActive()  # no timeout for an error
    closed = bar.height()

    qtbot.mouseClick(bar._details_btn, Qt.LeftButton)
    assert bar.details_open() and "line 14" in bar._details_view.toPlainText()
    assert bar.height() > closed
    qtbot.mouseClick(bar._details_btn, Qt.LeftButton)
    assert not bar.details_open() and bar.height() == closed

    qtbot.mouseClick(bar._dismiss_btn, Qt.LeftButton)
    assert not controller.has_notice()
    assert bar._label.text() == "2 images"  # back to the resting text
    assert not bar.notice_controls_visible()


def test_info_and_success_clear_themselves(qtbot, bar_and_controller):
    _host, bar, controller, _button = bar_and_controller
    controller.show_notice("Saved.", "success", timeout_ms=60)
    assert bar._label.text() == "Saved." and not bar._dismiss_btn.isHidden()
    qtbot.waitUntil(lambda: not controller.has_notice(), timeout=2000)
    assert bar._label.text() == "" and not bar.notice_controls_visible()
    # The defaults: info goes after a while, longer with Undo; errors stay.
    controller.show_notice("Saved.", "info")
    assert controller._notice_timer.interval() == HintStatusController.NOTICE_TIMEOUT_MS
    controller.show_notice("Deleted.", "info", undo=lambda: None)
    assert controller._notice_timer.interval() == HintStatusController.UNDO_NOTICE_TIMEOUT_MS
    controller.show_notice("Failed.", "warning")
    assert not controller._notice_timer.isActive()


def test_opening_details_keeps_a_notice_that_would_clear_itself(qtbot, bar_and_controller):
    _host, bar, controller, _button = bar_and_controller
    controller.show_notice("Downloaded 3 observations.", "info", details="3 new, 0 changed", timeout_ms=200)
    qtbot.mouseClick(bar._details_btn, Qt.LeftButton)
    qtbot.wait(400)
    assert controller.has_notice() and bar.details_open()


def test_undo_runs_once_and_the_notice_goes(qtbot, bar_and_controller):
    _host, bar, controller, _button = bar_and_controller
    undone = []
    controller.show_notice("Image deleted.", "info", undo=lambda: undone.append(True), undo_text="Restore")
    assert bar._undo_btn.text() == "Restore" and not bar._undo_btn.isHidden()
    qtbot.mouseClick(bar._undo_btn, Qt.LeftButton)
    assert undone == [True]
    assert not controller.has_notice() and bar._undo_btn.isHidden()
    qtbot.mouseClick(bar._undo_btn, Qt.LeftButton)  # hidden now: nothing more
    assert undone == [True]


def test_a_failed_undo_says_so_with_the_error(qtbot, bar_and_controller):
    _host, bar, controller, _button = bar_and_controller

    def fail():
        raise RuntimeError("database is locked")

    controller.show_notice("Image deleted.", "info", undo=fail)
    qtbot.mouseClick(bar._undo_btn, Qt.LeftButton)
    assert controller.notice_text() == "Could not undo."
    assert bar._details_view.toPlainText() == "database is locked"
    assert not controller._notice_timer.isActive()  # an error: it stays


def test_hover_hints_cover_a_notice_and_it_comes_back(qtbot, bar_and_controller):
    _host, bar, controller, button = bar_and_controller
    controller.show_notice("Could not save.", "error", details="locked")
    _hover(button, True)
    assert bar._label.text() == "Saves the observation"
    assert not bar.notice_controls_visible()  # a hint shows no notice controls
    _hover(button, False)
    qtbot.waitUntil(lambda: bar._label.text() == "Could not save.", timeout=2000)
    assert not bar._dismiss_btn.isHidden() and not bar._details_btn.isHidden()


def test_a_notice_shows_over_the_hint_of_the_control_just_clicked(qtbot, bar_and_controller):
    _host, bar, controller, button = bar_and_controller
    _hover(button, True)
    assert bar._label.text() == "Saves the observation"
    controller.show_notice("Could not save.", "error")  # the click's result
    assert bar._label.text() == "Could not save."
    _hover(button, False)
    qtbot.wait(250)
    assert bar._label.text() == "Could not save."
    _hover(button, True)  # back onto a control: its hint covers the notice again
    assert bar._label.text() == "Saves the observation"


def test_a_status_shows_over_a_notice_then_the_notice_returns(qtbot, bar_and_controller):
    _host, bar, controller, _button = bar_and_controller
    controller.show_notice("Could not sync.", "error")
    controller.set_status("Saved", timeout_ms=60, tone="success")
    assert bar._label.text() == "Saved" and not bar.notice_controls_visible()
    qtbot.waitUntil(lambda: bar._label.text() == "Could not sync.", timeout=2000)
    assert bar.notice_controls_visible()


def test_a_new_notice_replaces_the_old_one(qtbot, bar_and_controller):
    _host, bar, controller, _button = bar_and_controller
    controller.show_notice("First failed.", "error", details="one")
    controller.show_notice("Exported.", "success")
    assert controller.notice_text() == "Exported."
    assert bar._details_btn.isHidden()  # the new notice has no details
    controller.show_notice("")  # empty: dismiss
    assert not controller.has_notice()


def test_a_bar_without_a_notice_keeps_its_size(qtbot):
    """Hosts size their stacks from the bar's size hint (Live Lab): it stays
    the label's, and an idle bar keeps the fixed slot height."""
    bar = HintBar()
    qtbot.addWidget(bar)
    bar.resize(600, 34)
    bar.set_status("A short hint", "tip")
    assert bar.sizeHint().height() == bar._label.sizeHint().height()
    assert bar.height() == HintBar._HEIGHT


def test_the_progress_stack_makes_room_for_a_notice_and_gives_it_back(qtbot):
    stack = HintProgressStack()
    qtbot.addWidget(stack)
    stack.resize(700, 60)
    stack.show()
    qtbot.waitExposed(stack)
    controller = HintStatusController(stack.hint_bar, stack)
    stable = stack._stack.height()

    controller.show_notice("Could not export the gallery.", "error", details=LONG_DETAILS)
    qtbot.mouseClick(stack.hint_bar._details_btn, Qt.LeftButton)
    assert stack._stack.height() > stable
    assert stack._stack.height() >= stack.hint_bar.height()
    # Progress takes the stable slot meanwhile; back to the notice, it grows again.
    stack.show_progress("Exporting…")
    assert stack._stack.height() == stable
    stack.hide_progress()
    assert stack._stack.height() >= stack.hint_bar.height() > stable

    controller.dismiss_notice()
    assert stack._stack.height() == stable
    # A hover hint, however long, never grows the stack.
    controller.set_hint("A long hint " * 30)
    assert stack._stack.height() == stable


def test_the_stack_follows_a_notice_that_keeps_the_height_of_the_hint_before(qtbot):
    """Review finding: a notice as tall as the hint before it changes no
    height, yet the stack must make room for it (and give it back after)."""
    stack = HintProgressStack()
    qtbot.addWidget(stack)
    stack.resize(240, 60)
    stack.show()
    qtbot.waitExposed(stack)
    controller = HintStatusController(stack.hint_bar, stack)
    stable = stack._stack.height()
    message = "A long message that wraps over several lines in a narrow bar " * 2
    controller.set_hint(message)
    tall = stack.hint_bar.height()
    assert tall > stable and stack._stack.height() == stable  # a hint never grows it
    controller.show_notice(message, "error")
    assert stack._stack.height() >= stack.hint_bar.height() > stable
    controller.dismiss_notice()  # the hint returns, as tall: the slot does not stay grown
    assert stack.hint_bar._label.text().startswith("A long message")
    assert stack._stack.height() == stable


def test_details_stay_open_when_the_same_notice_shows_again(qtbot, bar_and_controller):
    """Review finding: refreshing the bar (a new resting text, an empty
    status, a hint passing over) must not close Details the user opened."""
    _host, bar, controller, button = bar_and_controller
    controller.show_notice("Could not sync.", "error", details=LONG_DETAILS)
    qtbot.mouseClick(bar._details_btn, Qt.LeftButton)
    controller.set_baseline("2 images")
    assert bar.details_open()
    controller.set_status("")
    assert bar.details_open()
    _hover(button, True)
    assert not bar.details_open()  # covered by the hint
    _hover(button, False)
    qtbot.waitUntil(lambda: bar._label.text() == "Could not sync.", timeout=2000)
    assert bar.details_open() and bar._details_btn.isChecked()
    # A new notice starts closed.
    controller.show_notice("Could not sync again.", "error", details="more")
    assert not bar.details_open()


def test_a_notice_timing_out_after_its_bar_is_gone_does_nothing(qtbot):
    """Review finding: a page deleted with its bar leaves the controller (its
    parent lives on); the notice's timer must not write to the deleted bar."""
    host = QWidget()
    qtbot.addWidget(host)
    bar = HintBar(host)
    controller = HintStatusController(bar, host)
    controller.show_notice("Saved.", "info", timeout_ms=30)
    controller.set_status("Busy", timeout_ms=30)
    bar.deleteLater()
    qtbot.waitUntil(lambda: not controller._notice_timer.isActive(), timeout=2000)
    qtbot.wait(100)  # the timers would have fired by now
    assert not controller.has_notice()


def test_a_hidden_bar_keeps_its_notice_until_it_shows(qtbot, bar_and_controller):
    """Review finding (10c): a host may hide the bar for a progress panel;
    a self-clearing notice must not time out unseen meanwhile."""
    _host, bar, controller, _button = bar_and_controller
    bar.hide()
    controller.show_notice("Saved.", "success", timeout_ms=50)
    qtbot.wait(200)
    assert controller.has_notice()
    bar.show()
    qtbot.waitUntil(lambda: not controller.has_notice(), timeout=2000)
    # Hidden while counting down: it waits again.
    controller.show_notice("Saved again.", "success", timeout_ms=120)
    bar.hide()
    qtbot.wait(300)
    assert controller.has_notice()
