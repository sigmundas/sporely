"""Kameraimport's layout and names (redesign stage 9h).

Real ``MainWindow`` in an isolated profile under ``tmp_path`` (the fixtures of
``tests/test_files_source.py``); no network. The left column reads Import
folder, Camera time offset (was “Sync Shot”), Live lab sessions (was
“Microscope sessions”), Actions; each control has a hint in the hint bar on
hover; the old names are gone from the screen and the time-code dialog.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QEvent, QPoint
from PySide6.QtWidgets import QAbstractButton, QApplication, QFrame, QLabel

from tests.test_files_source import profile, window  # noqa: F401


def _show(qtbot, window):
    hub = window.ingestion_hub_tab
    window.tab_widget.setCurrentWidget(hub)
    qtbot.waitUntil(lambda: hub.isVisible(), timeout=3000)
    return hub


def _texts(widget) -> list[str]:
    labels = [label.text() for label in widget.findChildren(QLabel)]
    buttons = [button.text() for button in widget.findChildren(QAbstractButton)]
    return labels + buttons


def test_the_left_column_reads_folder_offset_sessions_actions(qtbot, window):
    hub = _show(qtbot, window)
    panel = hub.scan_dir_input.window()
    order = [hub.scan_dir_input, hub.offset_spin, hub.observation_queue, hub.refresh_matches_btn]
    tops = [widget.mapTo(panel, QPoint(0, 0)).y() for widget in order]
    assert tops == sorted(tops) and len(set(tops)) == len(tops)
    cards = [frame for frame in hub.findChildren(QFrame, "sectionCard") if frame.isVisible()]
    cards.sort(key=lambda frame: frame.mapTo(panel, QPoint(0, 0)).y())
    assert [card._box_header.title_label.text() for card in cards] == [
        "IMPORT FOLDER", "CAMERA TIME OFFSET", "LIVE LAB SESSIONS", "ACTIONS",
    ]
    assert hub.new_sync_shot_btn.text() == "Show time code"


def test_the_old_names_are_gone(qtbot, window):
    from ui.sync_shot_dialog import SyncShotDialog

    hub = _show(qtbot, window)
    dialog = SyncShotDialog(hub)
    qtbot.addWidget(dialog)
    for widget in (hub, dialog):
        texts = _texts(widget) + [widget.windowTitle(), hub.hint_bar._label.text()]
        assert not any("Sync Shot" in text or "SYNC-SHOT" in text for text in texts), texts
        assert not any("icroscope session" in text for text in texts), texts
    assert dialog.windowTitle() == "Camera time offset"


def test_each_control_has_a_hint_on_hover(qtbot, window):
    hub = _show(qtbot, window)
    label = hub.hint_bar._label
    seen = set()
    for widget in (
        hub.browse_scan_dir_btn, hub.scan_folder_btn, hub.new_sync_shot_btn, hub.choose_sync_shot_image_btn,
        hub.clear_sync_shot_btn, hub.offset_spin, hub.observation_queue, hub.refresh_matches_btn,
        hub.commit_matches_btn, hub.add_all_images_btn,
    ):
        QApplication.sendEvent(widget, QEvent(QEvent.Enter))
        text = label.text()
        if widget.isEnabled():
            assert text and text not in seen, (widget, text)
            seen.add(text)
        QApplication.sendEvent(widget, QEvent(QEvent.Leave))
    QApplication.sendEvent(hub.new_sync_shot_btn, QEvent(QEvent.Enter))
    assert "time code" in label.text()
