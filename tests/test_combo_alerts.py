from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QComboBox

from ui.adaptive_choice_selector import AdaptiveChoiceSelector
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
    combo.addItem("-", "-")
    combo.addItem("—", "Not_set")

    assert combo_is_unset(combo) is True

    combo.setCurrentIndex(1)
    assert combo_is_unset(combo) is False

    combo.setCurrentIndex(2)
    assert combo_is_unset(combo) is True

    combo.setCurrentIndex(3)
    assert combo_is_unset(combo) is True


def test_combo_is_unset_ignores_blank_display_text_when_value_is_set(qapp):
    combo = QComboBox()
    combo.addItem("", "phase")
    combo.setCurrentIndex(0)

    assert combo_is_unset(combo) is False


@pytest.mark.parametrize("label", ["4x", "BF"])
def test_combo_is_unset_does_not_treat_first_normal_value_as_missing(qapp, label):
    combo = QComboBox()
    combo.addItem(label, label)
    combo.setCurrentIndex(0)

    assert combo_is_unset(combo) is False


@pytest.mark.parametrize(("first_label", "second_label"), [("4x", "10x"), ("BF", "DIC")])
def test_combo_is_unset_handles_adaptive_choice_selector(qapp, first_label, second_label):
    selector = AdaptiveChoiceSelector(compact=True)
    selector.addItem(first_label, first_label, pillText=first_label)
    selector.addItem(second_label, second_label, pillText=second_label)

    selector.setCurrentIndex(0)
    assert combo_is_unset(selector) is False
    update_combo_alert(selector)
    assert selector.property("labStateAlert") is False
    assert selector.combo().property("labStateAlert") is False

    selector.setCurrentIndex(1)
    assert combo_is_unset(selector) is False
    update_combo_alert(selector)
    assert selector.property("labStateAlert") is False
    assert selector.combo().property("labStateAlert") is False


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
