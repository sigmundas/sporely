from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QComboBox

from ui.combo_alerts import combo_is_unset, update_combo_alert, update_combo_alerts


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_combo_is_unset_recognizes_not_set_sentinels(qapp):
    combo = QComboBox()
    combo.addItem("Not set", None)
    combo.addItem("Phase", "phase")

    assert combo_is_unset(combo) is True

    combo.setCurrentIndex(1)
    assert combo_is_unset(combo) is False


def test_update_combo_alerts_marks_and_clears_alert_state(qapp):
    alert_combo = QComboBox()
    alert_combo.addItem("Not set", "Not_set")
    alert_combo.addItem("Water", "water")
    alert_combo.setCurrentIndex(0)

    normal_combo = QComboBox()
    normal_combo.addItem("Phase", "phase")
    normal_combo.setCurrentIndex(0)

    update_combo_alerts((alert_combo, normal_combo))

    assert alert_combo.property("labStateAlert") is True
    assert normal_combo.property("labStateAlert") is False
    assert "labStateAlert" in alert_combo.styleSheet()
    assert normal_combo.styleSheet() == ""

    alert_combo.setCurrentIndex(1)
    update_combo_alert(alert_combo)

    assert alert_combo.property("labStateAlert") is False
    assert alert_combo.styleSheet() == ""
