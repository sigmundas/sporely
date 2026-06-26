"""Shared alert styling for unset lab-state combos."""
from __future__ import annotations

from collections.abc import Iterable

from PySide6.QtWidgets import QApplication, QComboBox

_UNSET_TEXTS = {"not_set", "not set", "-", "—"}


def lab_state_alert_colors() -> dict[str, str]:
    app = QApplication.instance()
    palette = app.palette() if app is not None else QApplication.palette()
    dark = bool(palette.window().color().lightness() < 128)
    if dark:
        return {
            "background": "#4b1f24",
            "background_hover": "#5d262d",
            "border": "#d65a63",
            "text": "#ffecec",
        }
    return {
        "background": "#ffe1e1",
        "background_hover": "#ffd3d3",
        "border": "#d64545",
        "text": "#7f1d1d",
    }


def _normalized_text(value) -> str:
    return str(value or "").strip().lower()


def _is_unset_text(value) -> bool:
    return _normalized_text(value) in _UNSET_TEXTS


def _selected_item(combo):
    getter = getattr(combo, "selected_item", None)
    if not callable(getter):
        return None
    try:
        return getter()
    except Exception:
        return None


def _item_field(item, name: str):
    if item is None:
        return None
    if isinstance(item, dict):
        return item.get(name)
    return getattr(item, name, None)


def combo_is_unset(combo) -> bool:
    if combo is None:
        return False
    try:
        if combo.count() <= 0 or combo.currentIndex() < 0:
            return True
    except Exception:
        return True
    item = _selected_item(combo)
    if item is not None:
        value = _item_field(item, "value")
        if _is_unset_text(value):
            return True
        if value is not None:
            value_text = str(value).strip()
            if value_text:
                return False
        for field_name in ("pill_text", "display_text", "tooltip"):
            if _is_unset_text(_item_field(item, field_name)):
                return True
        return False
    try:
        data = combo.currentData()
    except Exception:
        data = None
    if _is_unset_text(data):
        return True
    if data is not None:
        data_text = str(data).strip()
        if data_text:
            return False
    try:
        current_text = str(combo.currentText() or "").strip()
    except Exception:
        current_text = ""
    return _is_unset_text(current_text)


def lab_state_combo_alert_stylesheet(alert: bool = True) -> str:
    if not alert:
        return ""
    app = QApplication.instance()
    palette = app.palette() if app is not None else QApplication.palette()
    colors = lab_state_alert_colors()
    background = colors["background"]
    background_hover = colors["background_hover"]
    border = colors["border"]
    text = colors["text"]
    view_base = palette.base().color().name()
    view_text = palette.text().color().name()
    view_highlight = palette.highlight().color().name()
    view_highlight_text = palette.highlightedText().color().name()
    return (
        'QComboBox[labStateAlert="true"] {'
        f" background-color: {background};"
        f" color: {text};"
        f" border: 1px solid {border};"
        " border-radius: 6px;"
        " }"
        "QComboBox[labStateAlert=\"true\"] QAbstractItemView {"
        f" background-color: {view_base};"
        f" color: {view_text};"
        f" selection-background-color: {view_highlight};"
        f" selection-color: {view_highlight_text};"
        " }"
        f'QComboBox[labStateAlert="true"]:hover {{ background-color: {background_hover}; }}'
        f'QComboBox[labStateAlert="true"]::drop-down {{ border-left: 1px solid {border}; }}'
    )


def update_combo_alert(combo: QComboBox | None, alert: bool | None = None) -> None:
    if combo is None:
        return
    state = combo_is_unset(combo) if alert is None else bool(alert)
    if hasattr(combo, "set_lab_state_alert"):
        combo.set_lab_state_alert(state)
        return
    combo.setProperty("labStateAlert", state)
    combo.setStyleSheet(lab_state_combo_alert_stylesheet(state))
    style = combo.style()
    try:
        style.unpolish(combo)
        style.polish(combo)
    except Exception:
        pass
    combo.update()


def update_combo_alerts(combos: Iterable[QComboBox | None]) -> None:
    for combo in combos:
        update_combo_alert(combo)
