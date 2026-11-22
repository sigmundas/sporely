"""The real main window, left idle, hardly touches the database.

With no other connection open, SQLite creates and deletes mushrooms.db-wal and
-shm each time a connection closes. The activity badge's 400 ms timer used to
open two connections per tick: about 25 file events a second, enough to back
up macOS file events (fseventsd) so that the Live Lab folder watcher learned
of new captures about a minute late. Real ``MainWindow`` in an isolated
profile under ``tmp_path`` (see ``tools/review_ui/live_lab_harness.py``).
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import sqlite3

import pytest

from tools.review_ui.live_lab_harness import FakeRawpy, create_observation, isolation_patches, prepare_profile


@pytest.fixture
def profile(tmp_path, monkeypatch, qtbot):
    rawpy = FakeRawpy()
    for owner, attribute, value in isolation_patches(tmp_path, rawpy):
        monkeypatch.setattr(owner, attribute, value)
    return prepare_profile(tmp_path, rawpy)


def test_idle_main_window_opens_few_database_connections(qtbot, profile, monkeypatch):
    from ui.main_window import MainWindow

    create_observation(profile)
    window = MainWindow()
    qtbot.addWidget(window)
    window.resize(1400, 900)
    with qtbot.waitExposed(window):
        window.show()
    assert window._background_activity_timer.isActive()
    qtbot.wait(1500)  # startup work settles

    opened: list[int] = []
    real_connect = sqlite3.connect

    def counting_connect(*args, **kwargs):
        opened.append(1)
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", counting_connect)
    qtbot.wait(6000)  # fifteen badge ticks
    monkeypatch.setattr(sqlite3, "connect", real_connect)

    # One lookup (two connections) every 5 s: the timer still renews the
    # badge, but no longer at every tick (30 connections here before).
    assert 2 <= len(opened) <= 4, f"{len(opened)} connections opened in 6 s of idling"
