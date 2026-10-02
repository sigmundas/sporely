"""Add Reference (picker, manual tab) and Edit Reference: only the form
scrolls; the hint/status bar and the action buttons stay fixed below it."""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QPoint
from PySide6.QtWidgets import QApplication

from database import schema as _schema


@pytest.fixture()
def libs(tmp_path, monkeypatch):
    QApplication.instance() or QApplication([])
    monkeypatch.setattr(_schema, "get_database_path", lambda: tmp_path / "m.db")
    monkeypatch.setattr(_schema, "get_reference_database_path", lambda: tmp_path / "r.db")
    monkeypatch.setattr(_schema, "get_bundled_reference_database_path", lambda: tmp_path / "none.db")
    from PySide6.QtCore import QSettings
    import ui.add_reference_dialog as picker
    import ui.window_state as geometry
    settings = lambda *_a: QSettings(str(tmp_path / "s.ini"), QSettings.IniFormat)
    monkeypatch.setattr(picker, "QSettings", settings)
    monkeypatch.setattr(geometry, "QSettings", settings)
    _schema.init_database()


def _pos(widget, dialog) -> QPoint:
    return widget.mapTo(dialog, QPoint(0, 0))


def _assert_fixed(dialog, scroll, hint_row, buttons):
    dialog.resize(700, 420)  # small: the form must scroll
    dialog.show()
    QApplication.processEvents()
    bar = scroll.verticalScrollBar()
    assert bar.maximum() > 0
    assert not scroll.widget().isAncestorOf(hint_row)
    bar.setValue(0)
    QApplication.processEvents()
    top = (_pos(hint_row, dialog), [_pos(b, dialog) for b in buttons])
    bar.setValue(bar.maximum())
    QApplication.processEvents()
    bottom = (_pos(hint_row, dialog), [_pos(b, dialog) for b in buttons])
    assert top == bottom
    # Below the scroll viewport, not covering it; buttons below the hint.
    viewport_bottom = scroll.viewport().mapTo(dialog, QPoint(0, scroll.viewport().height())).y()
    assert top[0].y() >= viewport_bottom
    assert all(p.y() >= top[0].y() + hint_row.height() - 1 for p in top[1])
    assert hint_row.isVisible()


def test_edit_reference_dialog_hint_and_buttons_do_not_scroll(libs):
    from ui.main_window import ReferenceAddDialog

    dialog = ReferenceAddDialog(None, "Cortinarius", "limonius", "", observation_id=42,
                                sporely_taxon_id=7, title="Edit reference")
    try:
        _assert_fixed(dialog, dialog.editor_scroll, dialog.hint_row,
                      [dialog.save_btn, dialog.cancel_btn])
        dialog.editor._set_hint("Parser says hi", tone="warning")
        QApplication.processEvents()
        assert "Parser says hi" in dialog.editor.hint_bar._label.text()
    finally:
        dialog.close()


def test_add_reference_manual_tab_hint_and_buttons_do_not_scroll(libs):
    from ui.add_reference_dialog import AddReferenceDialog

    dialog = AddReferenceDialog(None, taxon_label="Cortinarius limonius", taxon_id=7,
                                genus="Cortinarius", species="limonius", candidates=[],
                                community_results=[], my_observations=[])
    try:
        dialog.tabs.setCurrentIndex(dialog._manual_tab_index)
        _assert_fixed(dialog, dialog._manual_scroll, dialog.manual_hint_row,
                      [dialog.add_to_plot_btn])
        dialog.manual_editor._set_hint("Validation hint", tone="warning")
        QApplication.processEvents()
        assert "Validation hint" in dialog.manual_editor.hint_bar._label.text()
    finally:
        dialog.close()
