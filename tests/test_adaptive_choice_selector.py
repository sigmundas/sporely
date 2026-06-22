from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
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
    assert max(pill_widths) - min(pill_widths) <= 1
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
    assert max(pill_widths) - min(pill_widths) <= 1


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
