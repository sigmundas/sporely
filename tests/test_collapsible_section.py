"""CollapsibleSection: summary heading, mouse and keyboard toggling, animation, remembered state."""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QLabel, QVBoxLayout, QWidget

from ui import collapsible_section as module
from ui.collapsible_section import CollapsibleSection


class _MemorySettings:
    def __init__(self):
        self.values: dict[str, str] = {}

    def get_setting(self, key, default=None):
        return self.values.get(key, default)

    def set_setting(self, key, value):
        self.values[key] = value


@pytest.fixture
def settings(monkeypatch):
    store = _MemorySettings()
    import database.models as models

    monkeypatch.setattr(models.SettingsDB, "get_setting", staticmethod(store.get_setting))
    monkeypatch.setattr(models.SettingsDB, "set_setting", staticmethod(store.set_setting))
    return store


def _page(qtbot, **kwargs):
    page = QWidget()
    layout = QVBoxLayout(page)
    above = QLabel("above", page)
    section = CollapsibleSection("Preparation", parent=page, **kwargs)
    for index in range(4):
        section.body_layout.addWidget(QLabel(f"row {index}", section.body))
    below = QLabel("below", page)
    layout.addWidget(above)
    layout.addWidget(section)
    layout.addWidget(below)
    layout.addStretch(1)
    qtbot.addWidget(page)
    page.resize(420, 400)
    page.show()
    qtbot.waitExposed(page)
    return page, above, section, below


def _settled(qtbot, section):
    qtbot.waitUntil(lambda: section._animation.state() != section._animation.State.Running, timeout=2000)


def test_header_click_closes_and_opens_the_body(qtbot, settings):
    _page_, _above, section, _below = _page(qtbot)
    assert section.is_expanded() and section.body.isVisible()
    qtbot.mouseClick(section.header, Qt.LeftButton)
    _settled(qtbot, section)
    assert not section.is_expanded() and not section.body.isVisible()
    assert section.header.arrow_label.text() == "▸"
    qtbot.mouseClick(section.header, Qt.LeftButton)
    _settled(qtbot, section)
    assert section.is_expanded() and section.body.isVisible()
    assert section.body.maximumHeight() > 10_000  # back to unconstrained
    assert section.header.arrow_label.text() == "▾"


@pytest.mark.parametrize("key", [Qt.Key_Space, Qt.Key_Return, Qt.Key_Enter])
def test_header_toggles_from_the_keyboard(qtbot, settings, key):
    _page_, _above, section, _below = _page(qtbot)
    section.header.setFocus(Qt.TabFocusReason)
    qtbot.waitUntil(section.header.hasFocus, timeout=1000)
    qtbot.keyClick(section.header, key)
    _settled(qtbot, section)
    assert not section.is_expanded()
    qtbot.keyClick(section.header, key)
    _settled(qtbot, section)
    assert section.is_expanded()


def test_header_is_reachable_with_tab(qtbot, settings):
    from PySide6.QtWidgets import QLineEdit

    page, _above, section, _below = _page(qtbot)
    before = QLineEdit(page)
    page.layout().insertWidget(0, before)
    before.show()
    qtbot.waitUntil(before.isVisible, timeout=1000)
    page.activateWindow()
    QWidget.setTabOrder(before, section.header)
    before.setFocus(Qt.TabFocusReason)
    qtbot.waitUntil(before.hasFocus, timeout=1000)
    qtbot.keyClick(before, Qt.Key_Tab)
    qtbot.waitUntil(section.header.hasFocus, timeout=1000)
    qtbot.keyClick(section.header, Qt.Key_Space)
    _settled(qtbot, section)
    assert not section.is_expanded()


def test_repeating_the_same_request_does_nothing(qtbot, settings):
    _page_, _above, section, _below = _page(qtbot, settings_key="repeat_key")
    toggles: list[bool] = []
    writes: list[str] = []
    section.toggled.connect(toggles.append)
    original = settings.set_setting
    settings.set_setting = lambda key, value: (writes.append(value), original(key, value))
    import database.models as models

    models.SettingsDB.set_setting = staticmethod(settings.set_setting)
    for _ in range(5):
        section.set_expanded(False)
    _settled(qtbot, section)
    assert toggles == [False]
    assert writes == ["0"]
    assert not section.body.isVisible()


def test_a_second_click_during_the_animation_turns_it_around(qtbot, settings):
    _page_, _above, section, _below = _page(qtbot)
    full = section.body.height()
    qtbot.mouseClick(section.header, Qt.LeftButton)  # start closing
    qtbot.wait(module.ANIMATION_MS // 3)
    assert section._animation.state() == section._animation.State.Running
    qtbot.mouseClick(section.header, Qt.LeftButton)  # open again mid-way
    _settled(qtbot, section)
    assert section.is_expanded() and section.body.isVisible()
    qtbot.waitUntil(lambda: section.body.height() == full, timeout=1000)


def test_summary_changes_never_move_the_layout(qtbot, settings):
    _page_, above, section, below = _page(qtbot)
    header_height = section.header.height()
    below_y = below.y()
    for text in ("", "L4", "L4 · Dried · Melzer · Hymenium " * 6):
        section.set_summary(text)
        qtbot.wait(10)
        assert section.header.height() == header_height
        assert below.y() == below_y
    assert section.summary() == "L4 · Dried · Melzer · Hymenium " * 6
    assert section.header.summary_label.toolTip() == section.summary()
    assert section.header.summary_label.text().endswith("…")


def test_closing_moves_only_what_is_below(qtbot, settings):
    _page_, above, section, below = _page(qtbot)
    above_y, header_y, below_y = above.y(), section.header.mapTo(section.parentWidget(), section.header.rect().topLeft()).y(), below.y()
    header_height = section.header.height()
    section.set_expanded(False)
    _settled(qtbot, section)
    qtbot.wait(10)
    assert above.y() == above_y
    assert section.header.mapTo(section.parentWidget(), section.header.rect().topLeft()).y() == header_y
    assert section.header.height() == header_height
    assert below.y() < below_y


def test_a_warning_sits_after_the_summary_and_is_never_elided(qtbot, settings):
    _page_, _above, section, _below = _page(qtbot)
    header_height = section.header.height()
    section.set_summary("KOH · Dried · Hymenium " * 5, alert="Stain not set")
    qtbot.wait(10)
    assert section.summary_alert() == "Stain not set"
    assert section.header.alert_label.isVisible()
    assert section.header.alert_label.text() == "Stain not set"  # full text, not elided
    assert section.header.summary_label.text().endswith("…")
    assert section.header.height() == header_height
    assert "Stain not set" in section.header.accessibleDescription()
    section.set_summary("KOH · Dried · Melzer")
    assert section.summary_alert() == ""
    assert not section.header.alert_label.isVisible()


def test_open_state_is_remembered_by_settings_key(qtbot, settings):
    _page_, _above, section, _below = _page(qtbot, settings_key="test_section_expanded")
    section.set_expanded(False, animate=False)
    assert settings.values["test_section_expanded"] == "0"
    _page2, _above2, again, _below2 = _page(qtbot, settings_key="test_section_expanded")
    assert not again.is_expanded() and not again.body.isVisible()
    again.set_expanded(True, animate=False)
    assert settings.values["test_section_expanded"] == "1"


def test_without_a_stored_state_the_default_applies(qtbot, settings):
    _page_, _above, section, _below = _page(qtbot, settings_key="never_stored", expanded=False)
    assert not section.is_expanded()
    assert "never_stored" not in settings.values


def test_closing_in_a_resizable_scroll_area_gives_the_height_back(qtbot, settings):
    """Like Live Lab's side panel: a scroll area with a form section and a trailing stretch."""
    from PySide6.QtWidgets import QFormLayout, QScrollArea

    from ui.styles import get_style

    scroll = QScrollArea()
    scroll.setWidgetResizable(True)
    panel = QWidget()
    layout = QVBoxLayout(panel)
    section = CollapsibleSection("Preparation", QFormLayout, parent=panel)
    for index in range(4):
        section.body_layout.addRow(f"Row {index}:", QLabel("value", section.body))
    layout.addWidget(section)
    layout.addStretch(1)
    scroll.setWidget(panel)
    scroll.setStyleSheet(get_style("light"))
    qtbot.addWidget(scroll)
    scroll.resize(420, 700)
    scroll.show()
    qtbot.waitExposed(scroll)
    header_height = section.header.height()
    open_height = section.height()

    heights: list[int] = []
    section.set_expanded(False)
    while section._animation.state() == section._animation.State.Running:
        qtbot.wait(15)
        heights.append(section.header.height())
    qtbot.wait(20)
    assert section.header.height() == header_height
    assert all(height == header_height for height in heights)
    assert section.height() <= header_height + 4  # the card's border, nothing more

    section.set_expanded(True)
    previous = section.height()
    while section._animation.state() == section._animation.State.Running:
        qtbot.wait(15)
        assert section.height() >= previous  # opens steadily, no step back
        assert section.height() <= open_height
        previous = section.height()
    qtbot.wait(20)
    assert section.height() == open_height
