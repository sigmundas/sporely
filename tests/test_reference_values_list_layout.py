"""The Analysis tab's Reference values list uses the column's spare height
before its own scrollbar appears, and still scrolls when it runs out."""
from __future__ import annotations

import os
from unittest.mock import patch

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication

import ui.main_window as main_window
from database import schema as _schema


@pytest.fixture()
def window(tmp_path, monkeypatch):
    QApplication.instance() or QApplication([])
    monkeypatch.setattr(_schema, "get_database_path", lambda: tmp_path / "m.db")
    monkeypatch.setattr(_schema, "get_reference_database_path", lambda: tmp_path / "r.db")
    monkeypatch.setattr(_schema, "get_bundled_reference_database_path", lambda: tmp_path / "none.db")
    _schema.init_database()
    with patch.object(main_window.MainWindow, "init_ui", lambda self: None), \
         patch.object(main_window.MainWindow, "_populate_scale_combo", lambda self: None), \
         patch.object(main_window.MainWindow, "load_default_objective", lambda self: None), \
         patch.object(main_window.MainWindow, "_restore_geometry", lambda self: None):
        win = main_window.MainWindow()
    win.active_observation_id = None
    yield win
    win.deleteLater()


def _series(count: int) -> list[dict]:
    return [{
        "key": f"use-{i}",
        "data": {"source_kind": "reference", "observation_reference_use_id": f"use-{i}",
                 "short_label": "Funga Nordica (2008)", "name_as_published": f"Species {i}",
                 "reference_data_kind": "range", "raw_text": "6-9 × 4-5",
                 "length_p05": 6.0, "length_p95": 9.0, "width_p05": 4.0, "width_p95": 5.0},
        "enabled": True,
    } for i in range(count)]


def _show(win, count, height):
    win.reference_series = _series(count)
    panel = win.create_gallery_panel()
    panel.resize(440, height)
    panel.show()
    QApplication.processEvents()
    return panel


def _visible_rows(win) -> int:
    scroll = win.comparison_list._scroll
    viewport = scroll.viewport().height()
    rows = [w for w in scroll.widget().findChildren(main_window.QWidget)
            if type(w).__name__ == "_ComparisonRowWidget"]
    return sum(1 for w in rows if w.geometry().bottom() <= viewport)


def test_five_rows_fit_without_scrolling_when_space_exists(window):
    panel = _show(window, 5, 1000)
    assert _visible_rows(window) == 5
    assert window.comparison_list._scroll.verticalScrollBar().maximum() == 0
    assert window.comparison_list.height() > 300  # not the 160 px minimum
    panel.deleteLater()


def test_long_list_still_scrolls(window):
    panel = _show(window, 30, 1000)
    assert window.comparison_list._scroll.verticalScrollBar().maximum() > 0
    panel.deleteLater()


def test_collapsed_section_hands_the_stretch_to_the_spacer(window):
    panel = _show(window, 5, 1000)
    layout = window._reference_values_left_layout
    assert layout.stretch(window._reference_values_top_index) == 1
    window._reference_values_section._toggle_btn.click()
    assert layout.stretch(window._reference_values_top_index) == 0
    assert layout.stretch(window._reference_values_tail_index) == 1
    window._reference_values_section._toggle_btn.click()
    assert layout.stretch(window._reference_values_top_index) == 1
    panel.deleteLater()
