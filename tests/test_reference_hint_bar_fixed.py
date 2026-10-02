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
    import ui.add_reference_dialog as picker
    import ui.window_state as geometry
    settings = lambda *_a: QSettings(str(tmp_path / "s.ini"), QSettings.IniFormat)
    monkeypatch.setattr(picker, "QSettings", settings)
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


def test_add_reference_footer_row_is_dialog_level(libs):
    from ui.add_reference_dialog import AddReferenceDialog

    dialog = AddReferenceDialog(None, taxon_label="Cortinarius limonius", taxon_id=7,
                                genus="Cortinarius", species="limonius", candidates=[],
                                community_results=[], my_observations=[])
    try:
        dialog.resize(1000, 460)
        dialog.tabs.setCurrentIndex(dialog._manual_tab_index)
        dialog.show()
        QApplication.processEvents()
        # No hint widget inside any tab or pane: only the footer's own bar.
        assert dialog.findChildren(HintBar) == [dialog.hint_bar]
        assert dialog.tabs.findChildren(HintBar) == []
        assert dialog._body_splitter.findChildren(HintBar) == []
        buttons = [dialog.cancel_btn, dialog.add_to_plot_btn]
        _assert_footer_row(dialog, dialog.hint_bar, buttons)
        assert dialog._manual_scroll.verticalScrollBar().maximum() > 0
        right = [s for s in dialog.preview_pane.findChildren(QScrollArea)]
        _scroll_all([dialog._manual_scroll, *right], dialog, dialog.hint_bar, buttons)
        _no_nested_scrollbars(dialog._manual_scroll)

        # The editor's contextual hints reach the dialog footer.
        dialog.manual_editor._set_hint("Validation hint", tone="warning")
        assert dialog.hint_bar._label.text() == "Validation hint"
        # Switching tabs: no stale hint from the manual tab.
        dialog.tabs.setCurrentWidget(dialog._library_tab)
        QApplication.processEvents()
        assert "Validation hint" not in dialog.hint_bar._label.text()
        # Any tab or pane can publish through the dialog-level API.
        dialog.set_hint("From the summary tab")
        assert dialog.hint_bar._label.text() == "From the summary tab"
        dialog.set_hint("")
        assert dialog.hint_bar._label.text() != "From the summary tab"
    finally:
        dialog.close()


def test_hint_text_is_vertically_centred_when_set_before_show(libs):
    from ui.add_reference_dialog import AddReferenceDialog

    dialog = AddReferenceDialog(None, taxon_label="C l", taxon_id=7, genus="C", species="l",
                                candidates=[], community_results=[], my_observations=[])
    try:
        dialog.tabs.setCurrentIndex(dialog._manual_tab_index)
        dialog.set_hint("Parsed — review and edit before saving.")
        dialog.resize(1460, 920)
        dialog.show()
        QApplication.processEvents()
        bar, label = dialog.hint_bar, dialog.hint_bar._label
        assert label.geometry().top() == 0 and label.height() == bar.height()
        image = bar.grab().toImage()
        rows = [y for y in range(image.height())
                if any(image.pixelColor(x, y).lightness() < 100 for x in range(10, 300))]
        assert rows
        assert abs((rows[0] + rows[-1]) / 2 - bar.height() / 2) <= 3
    finally:
        dialog.close()
