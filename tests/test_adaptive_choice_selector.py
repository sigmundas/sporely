from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QEvent, Qt
from PySide6.QtGui import QColor, QPalette
from PySide6.QtTest import QTest
from PySide6.QtWidgets import QApplication, QComboBox, QHBoxLayout, QWidget

from database.database_tags import DatabaseTerms
from database.models import SettingsDB
from database import schema
from ui.adaptive_choice_selector import (
    AdaptiveChoiceSelector,
    objective_color,
    objective_short_label,
)
from ui.combo_alerts import combo_is_unset, update_combo_alert
from ui.image_import_dialog import ImageImportDialog
from ui.live_lab_tab import LiveLabTab


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


@pytest.mark.parametrize(
    ("objective", "expected_label"),
    [
        ({"magnification": 10.0}, "10x"),
        ({"magnification": 40.0}, "40x"),
        ({"magnification": 63.0}, "63x"),
        ({"optics_type": "macro", "magnification": 2.0}, "1:2"),
        ({"magnification": 1.0, "name": "1:1 Olympus 60mm"}, "1:1"),
        ({"name": "Plan achro"}, "Plan achro"),
    ],
)
def test_objective_short_labels_cover_common_objectives(objective, expected_label):
    assert objective_short_label(objective) == expected_label


@pytest.mark.parametrize(
    ("objective", "expected_color"),
    [
        ({"magnification": 1.25}, "#3498db"),
        ({"magnification": 2.5}, "#3498db"),
        ({"magnification": 4.0}, "#e74c3c"),
        ({"magnification": 5.0}, "#e74c3c"),
        ({"magnification": 6.0}, "#f39c12"),
        ({"magnification": 10.0}, "#f1c40f"),
        ({"magnification": 20.0}, "#2ecc71"),
        ({"magnification": 32.0}, "#2ecc71"),
        ({"magnification": 50.0}, "#3498db"),
        ({"magnification": 63.0}, "#1f4ea8"),
        ({"magnification": 100.0}, "#f7f1e5"),
        ({"magnification": 1.0, "name": "1:1 Olympus 60mm"}, "#6b7280"),
    ],
)
def test_objective_color_uses_standard_magnification_bands(objective, expected_color):
    assert objective_color(objective) == expected_color


def test_objective_color_prefers_stored_color():
    assert objective_color({"magnification": 40.0, "color": "#123456"}) == "#123456"


def test_objective_color_uses_real_100x_profile():
    objective = schema.load_objectives()["100X"]
    assert objective_color(objective, "100X") == "#f7f1e5"


def test_adaptive_choice_selector_uses_theme_tokens_for_neutral_pills(monkeypatch, qapp):
    from ui import adaptive_choice_selector as selector_module

    old_palette = QPalette(qapp.palette())
    palette = QPalette(qapp.palette())
    palette.setColor(QPalette.ButtonText, QColor("#e8e8e8"))
    qapp.setPalette(palette)
    try:
        monkeypatch.setattr(
            selector_module,
            "get_design_tokens",
            lambda: {
                "surface": "#131313",
                "surface_low": "#252423",
                "data_brd": "#334155",
                "text": "#e8e8e8",
                "text_dim": "#a1a1aa",
                "accent": "#4d7c7a",
            },
        )
        selector = AdaptiveChoiceSelector(compact=True)
        selector.addItem("Contrast", "contrast")
        button = selector.button_for_value("contrast")
        assert button is not None
        style = button.styleSheet()
        assert "background-color: #4d7c7a;" in style
        assert "border: 1px solid transparent;" in style
        assert "color: #e8e8e8;" in style
    finally:
        qapp.setPalette(old_palette)


def test_adaptive_choice_selector_reserves_red_for_explicit_unset(monkeypatch, qapp):
    from ui import adaptive_choice_selector as selector_module

    monkeypatch.setattr(
        selector_module,
        "get_design_tokens",
        lambda: {
            "surface": "#131313",
            "surface_low": "#252423",
            "data_brd": "#334155",
            "text": "#e8e8e8",
            "text_dim": "#a1a1aa",
            "accent": "#4d7c7a",
        },
    )
    selector = AdaptiveChoiceSelector(compact=True)
    selector.addItem("Not set", "Not_set", pillText="—")
    selector.addItem("Phase", "phase")
    selector.setCurrentIndex(0)
    update_combo_alert(selector)
    assert selector.property("labStateAlert") is True
    assert selector.combo().property("labStateAlert") is True
    button = selector.button_for_value("Not_set")
    assert button is not None
    style = button.styleSheet()
    assert "background-color: #d64545;" in style or "background-color: #d65a63;" in style
    assert "color: #ffffff;" in style


@pytest.mark.parametrize(("label", "next_label"), [("4x", "10x"), ("BF", "DIC")])
def test_adaptive_choice_selector_keeps_first_normal_choice_out_of_alert_state(qapp, label, next_label):
    selector = AdaptiveChoiceSelector(compact=True)
    selector.addItem(label, label, pillText=label)
    selector.addItem(next_label, next_label, pillText=next_label)
    selector.setCurrentIndex(0)

    assert combo_is_unset(selector) is False
    update_combo_alert(selector)

    assert selector.property("labStateAlert") is False
    assert selector.combo().property("labStateAlert") is False


def test_adaptive_choice_selector_warns_only_for_explicit_unset_contrast_choice(qapp):
    selector = AdaptiveChoiceSelector(compact=True)
    selector.addItem("Not set", "Not_set", pillText="—")
    selector.addItem("BF", "BF", pillText="BF")
    selector.addItem("DIC", "DIC", pillText="DIC")

    selector.setCurrentIndex(0)
    assert combo_is_unset(selector) is True
    update_combo_alert(selector)
    assert selector.property("labStateAlert") is True
    assert selector.combo().property("labStateAlert") is True

    selector.setCurrentIndex(1)
    assert combo_is_unset(selector) is False
    update_combo_alert(selector)
    assert selector.property("labStateAlert") is False
    assert selector.combo().property("labStateAlert") is False


@pytest.mark.parametrize(("first_label", "second_label"), [("4x", "10x"), ("BF", "DIC")])
def test_adaptive_choice_selector_keeps_first_normal_choice_clear_after_real_clicks(qapp, first_label, second_label):
    selector = AdaptiveChoiceSelector(compact=True)
    selector.addItem(first_label, first_label, pillText=first_label)
    selector.addItem(second_label, second_label, pillText=second_label)
    container = _show_selector_in_pill_mode(qapp, selector)
    try:
        first_button = selector.button_for_value(first_label)
        second_button = selector.button_for_value(second_label)
        assert first_button is not None
        assert second_button is not None

        update_combo_alert(selector)
        assert selector.property("labStateAlert") is False
        assert selector.combo().property("labStateAlert") is False

        _click_and_settle(qapp, second_button)
        assert selector.currentIndex() == 1
        assert selector.currentData() == second_label
        assert combo_is_unset(selector) is False
        update_combo_alert(selector)
        assert selector.property("labStateAlert") is False
        assert selector.combo().property("labStateAlert") is False

        _click_and_settle(qapp, first_button)
        assert selector.currentIndex() == 0
        assert selector.currentData() == first_label
        assert combo_is_unset(selector) is False
        update_combo_alert(selector)
        assert selector.property("labStateAlert") is False
        assert selector.combo().property("labStateAlert") is False
    finally:
        container.close()


def test_adaptive_choice_selector_warns_only_for_explicit_unset_choice_after_real_clicks(qapp):
    selector = AdaptiveChoiceSelector(compact=True)
    selector.addItem("Not set", "Not_set", pillText="—")
    selector.addItem("BF", "BF", pillText="BF")
    selector.addItem("DIC", "DIC", pillText="DIC")
    container = _show_selector_in_pill_mode(qapp, selector)
    try:
        unset_button = selector.button_for_value("Not_set")
        bf_button = selector.button_for_value("BF")
        assert unset_button is not None
        assert bf_button is not None

        _click_and_settle(qapp, unset_button)
        assert selector.currentIndex() == 0
        assert selector.currentData() == "Not_set"
        assert combo_is_unset(selector) is True
        update_combo_alert(selector)
        assert selector.property("labStateAlert") is True
        assert selector.combo().property("labStateAlert") is True

        _click_and_settle(qapp, bf_button)
        assert selector.currentIndex() == 1
        assert selector.currentData() == "BF"
        assert combo_is_unset(selector) is False
        update_combo_alert(selector)
        assert selector.property("labStateAlert") is False
        assert selector.combo().property("labStateAlert") is False
    finally:
        container.close()


def test_100x_objective_selector_uses_black_text_on_eggshell_background(qapp):
    objective = schema.load_objectives()["100X"]
    color = objective_color(objective, "100X")

    selector = AdaptiveChoiceSelector(compact=True)
    selector.addItem("100x", "100X", pillText="100x", color=color)
    selector.setCurrentIndex(0)

    button = selector.button_for_value("100X")
    assert button is not None
    style = button.styleSheet()
    assert f"background-color: {color};" in style
    assert "color: #000000;" in style


def test_adaptive_choice_selector_rebuilds_theme_styles_on_palette_change(qapp):
    old_palette = QPalette(qapp.palette())
    selector = AdaptiveChoiceSelector(compact=True)
    selector.addItem("Contrast", "contrast")
    button = selector.button_for_value("contrast")
    assert button is not None
    assert "color: #1e293b;" in button.styleSheet()

    dark_palette = QPalette(qapp.palette())
    dark_palette.setColor(QPalette.Window, QColor("#131313"))
    dark_palette.setColor(QPalette.WindowText, QColor("#e8e8e8"))
    dark_palette.setColor(QPalette.Base, QColor("#1c1b1b"))
    dark_palette.setColor(QPalette.AlternateBase, QColor("#252423"))
    dark_palette.setColor(QPalette.Button, QColor("#1c1b1b"))
    dark_palette.setColor(QPalette.ButtonText, QColor("#e8e8e8"))
    qapp.setPalette(dark_palette)
    try:
        selector.changeEvent(QEvent(QEvent.PaletteChange))
        assert "color: #e8e8e8;" in button.styleSheet()
        assert "background-color: #4d7c7a;" in button.styleSheet()
    finally:
        qapp.setPalette(old_palette)


def _build_selector_container() -> tuple[QWidget, AdaptiveChoiceSelector]:
    container = QWidget()
    layout = QHBoxLayout(container)
    layout.setContentsMargins(0, 0, 0, 0)
    selector = AdaptiveChoiceSelector(container, compact=True)
    layout.addWidget(selector)

    selector.addItem("Not set", None, pillText="No")
    selector.addItem("Phase", "phase")
    selector.addItem("BF", "bf")
    selector.addItem("DIC", "dic")
    selector.addItem("Oblique", "oblique")
    selector.addItem("10x", "10x")
    selector.addItem("40x", "40x")
    selector.addItem("63x", "63x")
    return container, selector


def _show_selector_in_pill_mode(qapp, selector: AdaptiveChoiceSelector, *, width: int = 960) -> QWidget:
    container = QWidget()
    layout = QHBoxLayout(container)
    layout.setContentsMargins(0, 0, 0, 0)
    layout.addWidget(selector)
    container.resize(width, 48)
    container.show()
    qapp.processEvents()
    QTest.qWait(150)
    assert selector.display_mode() == "pill"
    return container


def _click_and_settle(qapp, button) -> None:
    QTest.mouseClick(button, Qt.LeftButton)
    qapp.processEvents()
    QTest.qWait(150)


def test_adaptive_choice_selector_switches_between_pills_and_dropdown(qapp):
    container, selector = _build_selector_container()
    container.resize(1100, 48)
    container.show()
    qapp.processEvents()
    QTest.qWait(120)

    assert selector.display_mode() == "pill"
    assert selector.currentIndex() == 0
    assert selector.currentText() == "No"
    assert selector.findData("40x") == 6
    assert selector.findText("40x") == 6
    assert selector.itemData(1) == "phase"
    assert selector.itemText(1) == "Phase"
    pill_widths = [button.width() for button in selector.buttons()]
    assert pill_widths[0] < min(pill_widths[1:])
    assert max(pill_widths[1:]) - min(pill_widths[1:]) <= 1
    first_button, second_button = selector.buttons()[:2]
    assert second_button.geometry().x() - (first_button.geometry().x() + first_button.width()) == 2

    selector.setCurrentIndex(6)
    assert selector.currentData() == "40x"
    assert selector.currentText() == "40x"
    assert selector.button_for_value("40x").isChecked() is True

    container.resize(180, 48)
    qapp.processEvents()
    QTest.qWait(120)

    assert selector.display_mode() == "combo"
    assert selector.currentData() == "40x"
    assert selector.currentText() == "40x"

    container.resize(1100, 48)
    qapp.processEvents()
    QTest.qWait(120)

    assert selector.display_mode() == "pill"
    assert selector.currentData() == "40x"
    assert selector.button_for_value("40x").isChecked() is True
    pill_widths = [button.width() for button in selector.buttons()]
    assert pill_widths[0] < min(pill_widths[1:])
    assert max(pill_widths[1:]) - min(pill_widths[1:]) <= 1


def test_adaptive_choice_selector_blocked_signals_still_sync_visible_state(qapp):
    container, selector = _build_selector_container()
    container.resize(1100, 48)
    container.show()
    qapp.processEvents()
    QTest.qWait(120)

    seen: list[int] = []
    selector.currentIndexChanged.connect(seen.append)

    selector.blockSignals(True)
    selector.setCurrentIndex(2)
    selector.blockSignals(False)

    assert seen == []
    assert selector.currentIndex() == 2
    assert selector.currentData() == "bf"
    assert selector.button_for_value("bf").isChecked() is True

    selector.setCurrentIndex(3)
    assert seen == [3]


def test_microscope_lists_persist_and_are_read_by_prepare_images_and_live_lab(tmp_path, monkeypatch):
    app_dir = tmp_path / "appdata"
    app_dir.mkdir()

    monkeypatch.setattr(schema, "DATABASE_PATH", app_dir / "mushrooms.db")
    monkeypatch.setattr(schema, "REFERENCE_DATABASE_PATH", app_dir / "reference_values.db")
    monkeypatch.setattr(schema, "SETTINGS_PATH", app_dir / "app_settings.json")
    monkeypatch.setattr(schema, "init_reference_database", lambda *args, **kwargs: None)

    schema.init_database()

    reduced_lists = {
        "contrast": ["Not_set", "BF", "Phase"],
        "mount": ["Not_set", "Water"],
        "stain": ["Not_set", "Congo_Red", "Cotton_Blue"],
        "sample": ["Not_set", "Dried"],
    }

    for category, values in reduced_lists.items():
        SettingsDB.set_list_setting(DatabaseTerms.setting_key(category), values)

    schema.init_database()

    for category, values in reduced_lists.items():
        setting_key = DatabaseTerms.setting_key(category)
        assert SettingsDB.get_list_setting(setting_key, DatabaseTerms.default_values(category)) == values
        assert ImageImportDialog._load_tag_options(SimpleNamespace(), category) == values

        dummy = SimpleNamespace(
            _make_combo=lambda: QComboBox(),
            _add_choice_item=lambda combo, text, value, **kwargs: combo.addItem(text, value),
            _remember_last_used_term=lambda *args, **kwargs: None,
        )
        combo = LiveLabTab._build_term_combo(dummy, category)
        expected_values = values
        assert [combo.itemData(i) for i in range(combo.count())] == expected_values
