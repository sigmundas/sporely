"""Add Reference and Edit Reference: the hint/status bar is dialog-level UI.

It sits in one fixed footer row with the action buttons (hint expanding on
the left, buttons on the right), spans the dialog, never moves when the
left or right content scrolls, and no hint widget is embedded in any tab
or pane.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QPoint
from PySide6.QtWidgets import QApplication, QScrollArea

from database import schema as _schema
from ui.hint_status import HintBar


@pytest.fixture()
def libs(tmp_path, monkeypatch):
    QApplication.instance() or QApplication([])
    monkeypatch.setattr(_schema, "get_database_path", lambda: tmp_path / "m.db")
    monkeypatch.setattr(_schema, "get_reference_database_path", lambda: tmp_path / "r.db")
    monkeypatch.setattr(_schema, "get_bundled_reference_database_path", lambda: tmp_path / "none.db")
    from PySide6.QtCore import QSettings
    import ui.window_state as geometry
    settings = lambda *_a: QSettings(str(tmp_path / "s.ini"), QSettings.IniFormat)
    monkeypatch.setattr(geometry, "QSettings", settings)
    _schema.init_database()


def _pos(widget, dialog) -> QPoint:
    return widget.mapTo(dialog, QPoint(0, 0))


def _layout_widgets(layout) -> list:
    return [layout.itemAt(i).widget() for i in range(layout.count()) if layout.itemAt(i).widget()]


def _footer_snapshot(dialog, hint, buttons):
    return (_pos(hint, dialog), [_pos(b, dialog) for b in buttons])


def _assert_footer_row(dialog, hint, buttons):
    widgets = _layout_widgets(dialog.footer_layout)
    assert hint in widgets and all(b in widgets for b in buttons)  # same parent layout
    hint_mid = _pos(hint, dialog).y() + hint.height() / 2
    for button in buttons:
        mid = _pos(button, dialog).y() + button.height() / 2
        assert abs(mid - hint_mid) <= 6  # same row
        assert _pos(button, dialog).x() > _pos(hint, dialog).x() + hint.width() - 1  # hint left
    margins = dialog.layout().contentsMargins()
    assert dialog.footer_layout.geometry().width() == dialog.width() - margins.left() - margins.right()


def _scroll_all(scrolls, dialog, hint, buttons):
    snapshots = []
    for value in ("min", "max"):
        for scroll in scrolls:
            bar = scroll.verticalScrollBar()
            bar.setValue(bar.minimum() if value == "min" else bar.maximum())
        QApplication.processEvents()
        snapshots.append(_footer_snapshot(dialog, hint, buttons))
    assert snapshots[0] == snapshots[1]


def _no_nested_scrollbars(scroll):
    inner = [s for s in scroll.widget().findChildren(QScrollArea)
             if s.verticalScrollBar().isVisible()]
    assert inner == []


def test_edit_reference_footer_row(libs):
    from ui.main_window import ReferenceAddDialog

    dialog = ReferenceAddDialog(None, "Cortinarius", "limonius", "", observation_id=42,
                                sporely_taxon_id=7, title="Edit reference")
    try:
        dialog.resize(700, 420)
        dialog.show()
        QApplication.processEvents()
        assert dialog.editor_scroll.verticalScrollBar().maximum() > 0
        assert dialog.editor.findChildren(HintBar) == []
        buttons = [dialog.save_btn, dialog.cancel_btn]
        _assert_footer_row(dialog, dialog.hint_row, buttons)
        _scroll_all([dialog.editor_scroll], dialog, dialog.hint_row, buttons)
        _no_nested_scrollbars(dialog.editor_scroll)
        dialog.editor._set_hint("Parser says hi", tone="warning")
        assert "Parser says hi" in dialog.hint_row.findChild(HintBar)._label.text()
    finally:
        dialog.close()


def test_edit_reference_shares_the_dialog_hint_api(libs):
    from ui.main_window import ReferenceAddDialog

    dialog = ReferenceAddDialog(None, "Cortinarius", "limonius", "", observation_id=42,
                                sporely_taxon_id=7, title="Edit reference")
    try:
        label = dialog.hint_row.findChild(HintBar)._label
        dialog.set_footer_status("Resting text")
        dialog.set_hint("")
        assert label.text() == "Resting text"
        dialog.set_hint("Context hint")
        assert label.text() == "Context hint"
        dialog.set_status("Saved!", timeout_ms=0, tone="success")
        assert label.text() == "Saved!"
    finally:
        dialog.close()
