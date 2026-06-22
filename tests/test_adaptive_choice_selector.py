from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QEvent
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
from ui.combo_alerts import update_combo_alert
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
        ({"magnification": 1.25}, "#1f1f1f"),
        ({"magnification": 2.5}, "#8f9398"),
        ({"magnification": 4.0}, "#4d7c7a"),
        ({"magnification": 5.0}, "#d64545"),
        ({"magnification": 10.0}, "#f1c40f"),
        ({"magnification": 20.0}, "#2ecc71"),
        ({"magnification": 32.0}, "#16a085"),
        ({"magnification": 50.0}, "#5dade2"),
        ({"magnification": 63.0}, "#1f4ea8"),
        ({"magnification": 100.0}, "#f7f7f7"),
        ({"magnification": 1.0, "name": "1:1 Olympus 60mm"}, "#6b7280"),
    ],
)
def test_objective_color_uses_standard_magnification_bands(objective, expected_color):
    assert objective_color(objective) == expected_color


def test_objective_color_prefers_stored_color():
    assert objective_color({"magnification": 40.0, "color": "#123456"}) == "#123456"


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
    selector.addItem("Not set", None, pillText="—")
    selector.addItem("Phase", "phase")
    selector.setCurrentIndex(0)
    update_combo_alert(selector)
    button = selector.button_for_value(None)
    assert button is not None
    style = button.styleSheet()
    assert "background-color: #d64545;" in style or "background-color: #d65a63;" in style
    assert "color: #ffffff;" in style


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
        expected_values = values[1:] if category == "contrast" else values
        assert [combo.itemData(i) for i in range(combo.count())] == expected_values
