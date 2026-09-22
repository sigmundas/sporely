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


def test_reference_add_dialog_inputs_accept_keyboard_focus(qapp):
    parent = QWidget()
    dialog = ReferenceAddDialog(parent, "Agaricus", "bisporus")

    # The 3 x 5 measurement grid replaced the min/max table (contract N24),
    # so the keyboard guarantee is now about its fifteen plain inputs.
    for row in dialog.measurement_inputs:
        for field in row:
            assert field.focusPolicy() == Qt.StrongFocus
            assert field.isReadOnly() is False
    assert dialog.spore_table.focusPolicy() == Qt.StrongFocus
    assert dialog.spore_table.editTriggers() == QAbstractItemView.AllEditTriggers

    dialog.deleteLater()
    parent.deleteLater()


class _MemorySettings:
    values = {}

    def __init__(self, *_args):
        pass

    def value(self, key):
        return self.values.get(key)

    def setValue(self, key, value):
        self.values[key] = value


def test_reference_add_dialog_uses_taller_default(qapp, monkeypatch):
    monkeypatch.setattr("ui.window_state.QSettings", _MemorySettings)
    _MemorySettings.values = {}
    parent = QWidget()
    dialog = ReferenceAddDialog(parent, "Agaricus", "bisporus")

    assert dialog.size().width() == 860
    assert dialog.size().height() == 720

    dialog.deleteLater()
    parent.deleteLater()


def test_reference_add_dialog_restores_saved_position_and_size(qapp, monkeypatch):
    monkeypatch.setattr("ui.window_state.QSettings", _MemorySettings)
    _MemorySettings.values = {}
    parent = QWidget()
    first = ReferenceAddDialog(parent, "Agaricus", "bisporus")
    first.move(40, 50)
    first.resize(700, 650)
    first.reject()

    restored = ReferenceAddDialog(parent, "Agaricus", "bisporus")

    assert restored.pos() == first.pos()
    assert restored.size() == first.size()

    first.deleteLater()
    restored.deleteLater()
    parent.deleteLater()
