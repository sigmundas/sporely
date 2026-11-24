"""The cloud sync progress shows while Rediger is on screen.

Rediger became a tab of its own (stage 9g2); the sync progress lived only in
the Observations tab's bottom bar, so a sync started from Preferanser while
Rediger showed ran with nothing on screen (owner's report). Rediger now
borrows the panel while it shows.

Real ``MainWindow`` in an isolated profile (``tests/test_files_source.py``);
the sync worker is a stand-in that never touches the network.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QObject, Signal

import ui.observations_tab as observations_tab_module
from tests.test_files_source import profile, window  # noqa: F401


class _Worker(QObject):
    progress = Signal(str, int, int)
    sync_finished = Signal(dict)
    error = Signal(str)
    finished = Signal()
    prepare_requested = Signal(object)

    def __init__(self, *args, parent=None, **kwargs):
        super().__init__(parent)
        _Worker.last = self

    def start(self):
        pass

    def isRunning(self):  # noqa: N802 - Qt API
        return True

    def requestInterruption(self):  # noqa: N802 - Qt API
        pass

    def wait(self, *_args):
        return True


@pytest.fixture
def syncing(window, monkeypatch):  # noqa: F811
    monkeypatch.setattr(observations_tab_module, "_CloudAutoSyncWorker", _Worker)
    yield window
    window.observations_tab._set_status_progress_visible(False)
    window.observations_tab._cloud_sync_worker = None


def _on_screen_in(widget, page) -> bool:
    return widget.isVisible() and page.isAncestorOf(widget)


def test_a_download_started_while_rediger_shows_shows_its_progress_there(qtbot, syncing):
    window = syncing
    tab = window.observations_tab
    window.tab_widget.setCurrentIndex(window.EDIT_PAGE)
    qtbot.waitUntil(lambda: window.edit_page.isVisible(), timeout=3000)

    assert window.start_cloud_download()  # Preferanser › Download only
    panel = tab.status_progress_widget
    assert _on_screen_in(panel, window.edit_page)
    _Worker.last.progress.emit("Downloading observations…", 3, 10)
    assert tab.status_progress_text.text() == "Downloading observations…"
    assert tab.status_progress_pct.text() == "30%"
    assert tab.status_progress_cancel_btn.isVisible()


def test_the_progress_follows_the_page_shown_and_goes_home_below_the_bar(qtbot, syncing):
    window = syncing
    tab = window.observations_tab
    home_layout = tab.layout()
    window.tab_widget.setCurrentIndex(window.EDIT_PAGE)
    assert window.start_cloud_download()
    panel = tab.status_progress_widget

    window.tab_widget.setCurrentIndex(0)  # Observations
    qtbot.waitUntil(lambda: _on_screen_in(panel, tab), timeout=3000)
    assert home_layout.indexOf(panel) == home_layout.indexOf(tab.status_label) + 1

    window.tab_widget.setCurrentIndex(window.EDIT_PAGE)
    qtbot.waitUntil(lambda: _on_screen_in(panel, window.edit_page), timeout=3000)
    # Above the form's bottom bar, below the form and its strip.
    layout = window.edit_page.layout()
    assert layout.indexOf(panel) == layout.indexOf(window.edit_page.strip_splitter) + 1


def test_without_a_sync_rediger_shows_no_progress(qtbot, window):  # noqa: F811
    window.tab_widget.setCurrentIndex(window.EDIT_PAGE)
    qtbot.waitUntil(lambda: window.edit_page.isVisible(), timeout=3000)
    assert not window.observations_tab.status_progress_widget.isVisible()


def test_a_sync_that_ends_in_rediger_leaves_the_observations_bar_as_it_was(qtbot, syncing):
    window = syncing
    tab = window.observations_tab
    window.tab_widget.setCurrentIndex(window.EDIT_PAGE)
    assert window.start_cloud_download()
    tab._set_status_progress_visible(False)  # the run ended
    assert not tab.status_progress_widget.isVisible()
    window.tab_widget.setCurrentIndex(0)
    qtbot.waitUntil(lambda: tab.status_label.isVisible(), timeout=3000)
    assert not tab.status_progress_widget.isVisible()
    assert tab.layout().indexOf(tab.status_progress_widget) >= 0
