"""Shared alert styling for unset lab-state combos."""
from __future__ import annotations

from collections.abc import Iterable

from PySide6.QtWidgets import QApplication, QComboBox


def combo_is_unset(combo) -> bool:
    if combo is None:
        return False
    try:
        if combo.count() <= 0 or combo.currentIndex() < 0:
            return True
    except Exception:
        return True
    try:
        data = combo.currentData()
    except Exception:
        data = None
    if data is None:
        return True
    data_text = str(data or "").strip().lower()
    if data_text in {"", "not_set", "not set"}:
        return True
    try:
        text = str(combo.currentText() or "").strip().lower()
    except Exception:
        text = ""
    return not text or text in {"not_set", "not set"}


def lab_state_combo_alert_stylesheet(alert: bool = True) -> str:
    if not alert:
        return ""
    app = QApplication.instance()
    palette = app.palette() if app is not None else QApplication.palette()
    dark = bool(palette.window().color().lightness() < 128)
    if dark:
        background = "#4b1f24"
        background_hover = "#5d262d"
        border = "#d65a63"
        text = "#ffecec"
    else:
        background = "#ffe1e1"
        background_hover = "#ffd3d3"
        border = "#d64545"
        text = "#7f1d1d"
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
