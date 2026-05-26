import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QAbstractItemView, QWidget

from ui.main_window import ReferenceAddDialog


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_reference_add_dialog_tables_accept_keyboard_focus(qapp):
    parent = QWidget()
    dialog = ReferenceAddDialog(parent, "Agaricus", "bisporus")

    assert dialog.minmax_table.focusPolicy() == Qt.StrongFocus
    assert dialog.minmax_table.editTriggers() == QAbstractItemView.AllEditTriggers
    assert dialog.spore_table.focusPolicy() == Qt.StrongFocus
    assert dialog.spore_table.editTriggers() == QAbstractItemView.AllEditTriggers

    dialog.deleteLater()
    parent.deleteLater()
