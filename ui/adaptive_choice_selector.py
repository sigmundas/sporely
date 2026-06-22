"""Adaptive choice selector that switches between pills and a dropdown."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import re

from PySide6.QtCore import QEvent, QTimer, Signal, QSize
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QButtonGroup,
    QComboBox,
    QHBoxLayout,
    QLayout,
    QPushButton,
    QSizePolicy,
    QStackedLayout,
    QWidget,
)

from .combo_alerts import lab_state_alert_colors
from .styles import get_design_tokens


@dataclass(slots=True)
class ChoiceItem:
    display_text: str
    value: Any = None
    pill_text: str | None = None
    tooltip: str | None = None
    color: str | QColor | None = None
    dropdown_only: bool = False


class AdaptiveChoiceSelector(QWidget):
    """Choice selector that uses pills when everything fits and a combo otherwise."""

    selectionChanged = Signal(object)
    currentIndexChanged = Signal(int)
    currentTextChanged = Signal(str)

    def __init__(self, parent=None, *, compact: bool = True) -> None:
        super().__init__(parent)
        self.setObjectName("adaptiveChoiceSelector")
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self._compact = bool(compact)
        self._show_unselected_border = True
        self._items: list[ChoiceItem] = []
        self._buttons: list[QPushButton | None] = []
        self._current_index = -1
        self._mode = "dropdown"
        self._lab_state_alert = False
        self._fit_timer = QTimer(self)
        self._fit_timer.setSingleShot(True)
        self._fit_timer.timeout.connect(self._refresh_mode_for_width)

        self._stack = QStackedLayout(self)
        self._stack.setContentsMargins(0, 0, 0, 0)
        self._stack.setSpacing(0)

        self._pill_container = QWidget(self)
        self._pill_container.setObjectName("adaptiveChoicePillContainer")
        self._pill_container.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self._pill_container_layout = QHBoxLayout(self._pill_container)
        self._pill_container_layout.setContentsMargins(0, 0, 0, 0)
        self._pill_container_layout.setSpacing(2)
        self._pill_container_layout.setSizeConstraint(QLayout.SetDefaultConstraint)

        self._button_group = QButtonGroup(self)
        self._button_group.setExclusive(True)
        self._button_group.buttonClicked.connect(self._on_button_clicked)

        self._combo = QComboBox(self)
        self._combo.setObjectName("adaptiveChoiceCombo")
        self._combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self._combo.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContentsOnFirstShow)
        self._combo.currentIndexChanged.connect(self._on_combo_index_changed)

        self._stack.addWidget(self._pill_container)
        self._stack.addWidget(self._combo)
        self._stack.setCurrentWidget(self._combo)

        self._apply_container_style()

    # ------------------------------------------------------------------
    # Combo-like API
    def clear(self) -> None:
        self._items.clear()
        self._current_index = -1
        self._combo.blockSignals(True)
        self._combo.clear()
        self._combo.blockSignals(False)
        while self._pill_container_layout.count():
            item = self._pill_container_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                self._button_group.removeButton(widget)  # type: ignore[arg-type]
                widget.setParent(None)
                widget.deleteLater()
        self._buttons.clear()
        self._schedule_fit_update(0)

    def addItem(
        self,
        text: str,
        userData: Any = None,
        *,
        pillText: str | None = None,
        tooltip: str | None = None,
        color: str | QColor | None = None,
        dropdown_only: bool = False,
    ) -> None:
        item = ChoiceItem(
            display_text=str(text),
            value=userData,
            pill_text=str(pillText) if pillText is not None else None,
            tooltip=str(tooltip) if tooltip is not None else None,
            color=color,
            dropdown_only=bool(dropdown_only),
        )
        index = len(self._items)
        self._items.append(item)
        self._combo.addItem(item.display_text, item.value)

        button: QPushButton | None = None
        if not item.dropdown_only:
            button = QPushButton(item.pill_text or item.display_text, self._pill_container)
            button.setObjectName("adaptiveChoicePill")
            button.setCheckable(True)
            button.setCursor(self.cursor())
            button.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            if item.tooltip:
                button.setToolTip(item.tooltip)
            elif item.display_text:
                button.setToolTip(item.display_text)
            button.setProperty("choiceIndex", index)
            self._button_group.addButton(button)
            self._pill_container_layout.addWidget(button, 1)
            self._buttons.append(button)
            self._apply_button_style(button, item)
        else:
            self._buttons.append(None)

        if self._current_index < 0 and index == 0:
            self._apply_index(0, emit=False)
        else:
            self._sync_button_states()
        self._schedule_fit_update(0)

    def add_option(self, *args, **kwargs) -> None:
        self.addItem(*args, **kwargs)

    def count(self) -> int:
        return len(self._items)

    def currentIndex(self) -> int:
        return self._current_index

    def setCurrentIndex(self, index: int) -> None:
        self._apply_index(int(index), emit=not self.signalsBlocked())

    def currentData(self) -> Any:
        item = self._item_at(self._current_index)
        return None if item is None else item.value

    def currentText(self) -> str:
        item = self._item_at(self._current_index)
        if item is None:
            return ""
        if self._mode == "pill":
            return item.pill_text or item.display_text
        return item.display_text

    def findData(self, value: Any) -> int:
        for index, item in enumerate(self._items):
            if item.value == value:
                return index
        return -1

    def findText(self, text: str) -> int:
        needle = str(text)
        for index, item in enumerate(self._items):
            if item.display_text == needle or (item.pill_text or "") == needle:
                return index
        return -1

    def itemData(self, index: int) -> Any:
        item = self._item_at(index)
        return None if item is None else item.value

    def itemText(self, index: int) -> str:
        item = self._item_at(index)
        return "" if item is None else item.display_text

    def setItemText(self, index: int, text: str) -> None:
        item = self._item_at(index)
        if item is None:
            return
        item.display_text = str(text)
        self._combo.blockSignals(True)
        self._combo.setItemText(index, item.display_text)
        self._combo.blockSignals(False)
        button = self._buttons[index] if 0 <= index < len(self._buttons) else None
        if button is not None:
            # Keep the pill label stable unless the caller explicitly provided one.
            if item.pill_text is None and item.value is not None:
                button.setText(item.display_text)
        self._schedule_fit_update(0)

    def buttons(self) -> list[QPushButton]:
        return [button for button in self._buttons if button is not None]

    def button_for_value(self, value: Any) -> QPushButton | None:
        index = self.findData(value)
        if index < 0:
            return None
        return self._buttons[index]

    def selected_value(self, fallback: Any | None = None) -> Any | None:
        data = self.currentData()
        return fallback if data is None else data

    def set_selected_value(self, value: Any, *, emit: bool = False) -> bool:
        index = self.findData(value)
        if index < 0:
            return False
        self._apply_index(index, emit=emit and not self.signalsBlocked())
        return True

    def display_mode(self) -> str:
        return self._mode

    def is_pill_mode(self) -> bool:
        return self._mode == "pill"

    def combo(self) -> QComboBox:
        return self._combo

    def set_lab_state_alert(self, alert: bool) -> None:
        self._lab_state_alert = bool(alert)
        self.setProperty("labStateAlert", self._lab_state_alert)
        self._combo.setProperty("labStateAlert", self._lab_state_alert)
        self._refresh_theme_styles()

    def set_unselected_border_visible(self, visible: bool) -> None:
        self._show_unselected_border = bool(visible)
        for index, button in enumerate(self._buttons):
            if button is None:
                continue
            self._apply_button_style(button, self._items[index])

    # ------------------------------------------------------------------
    # QWidget overrides
    def showEvent(self, event) -> None:  # type: ignore[override]
        super().showEvent(event)
        self._schedule_fit_update(0)
        self._update_pill_button_widths()

    def resizeEvent(self, event) -> None:  # type: ignore[override]
        super().resizeEvent(event)
        self._schedule_fit_update(75)
        self._update_pill_button_widths()

    def changeEvent(self, event) -> None:  # type: ignore[override]
        super().changeEvent(event)
        if event.type() in {QEvent.FontChange, QEvent.StyleChange, QEvent.PaletteChange}:
            self._refresh_theme_styles()
            self._schedule_fit_update(0)

    def sizeHint(self):  # type: ignore[override]
        combo_hint = self._combo.sizeHint()
        pill_hint = self._pill_container.sizeHint()
        return QSize(combo_hint.width(), max(combo_hint.height(), pill_hint.height()))

    def minimumSizeHint(self):  # type: ignore[override]
        combo_hint = self._combo.minimumSizeHint()
        pill_hint = self._pill_container.minimumSizeHint()
        return QSize(combo_hint.width(), max(combo_hint.height(), pill_hint.height()))

    def blockSignals(self, block: bool) -> bool:  # type: ignore[override]
        self._combo.blockSignals(block)
        for button in self.buttons():
            button.blockSignals(block)
        return super().blockSignals(block)

    # ------------------------------------------------------------------
    def _apply_container_style(self) -> None:
        tokens = get_design_tokens()
        self._pill_container.setStyleSheet(
            "QWidget#adaptiveChoicePillContainer {"
            f" background-color: {tokens['surface_low']};"
            f" border: 1px solid {tokens['data_brd']};"
            " border-radius: 10px;"
            " }"
        )

    def _refresh_theme_styles(self) -> None:
        for index, button in enumerate(self._buttons):
            if button is None:
                continue
            self._apply_button_style(button, self._items[index])
        if self._mode == "pill":
            if self._lab_state_alert:
                colors = lab_state_alert_colors()
                self._pill_container.setStyleSheet(
                    "QWidget#adaptiveChoicePillContainer {"
                    f" background-color: {colors['background']};"
                    f" border: 1px solid {colors['border']};"
                    " border-radius: 10px;"
                    " }"
                )
            else:
                self._apply_container_style()
            self._update_pill_button_widths()
        else:
            if self._lab_state_alert:
                from .combo_alerts import lab_state_combo_alert_stylesheet

                self._combo.setStyleSheet(lab_state_combo_alert_stylesheet(True))
            else:
                self._combo.setStyleSheet("")
        self.update()

    def _item_at(self, index: int) -> ChoiceItem | None:
        if index < 0 or index >= len(self._items):
            return None
        return self._items[index]

    def _style_color(self, color: str | QColor | None) -> QColor:
        if color is None:
            tokens = get_design_tokens()
            return QColor(tokens["accent"])
        qcolor = QColor(color)
        if qcolor.isValid():
            return qcolor
        tokens = get_design_tokens()
        return QColor(tokens["accent"])

    def _contrast_text_color(self, background: QColor) -> str:
        dark = QColor("#1f2937")
        white = QColor("#ffffff")
        return white.name() if self._contrast_ratio(background, white) >= self._contrast_ratio(background, dark) else dark.name()

    def _contrast_ratio(self, first: QColor, second: QColor) -> float:
        def _channel(value: float) -> float:
            if value <= 0.03928:
                return value / 12.92
            return ((value + 0.055) / 1.055) ** 2.4

        def _luminance(color: QColor) -> float:
            r = _channel(color.redF())
            g = _channel(color.greenF())
            b = _channel(color.blueF())
            return 0.2126 * r + 0.7152 * g + 0.0722 * b

        a = _luminance(first)
        b = _luminance(second)
        lighter = max(a, b)
        darker = min(a, b)
        return (lighter + 0.05) / (darker + 0.05)

    def _rgba(self, color: QColor, alpha: int) -> str:
        return f"rgba({color.red()}, {color.green()}, {color.blue()}, {alpha})"

    def _is_unset_choice_item(self, item: ChoiceItem) -> bool:
        for part in (item.pill_text, item.display_text, item.tooltip):
            text = str(part or "").strip().lower()
            if text in {"-", "—", "not set", "not_set"}:
                return True
        return False

    def _selected_item_is_unset(self) -> bool:
        item = self._item_at(self._current_index)
        if item is None:
            return False
        return self._is_unset_choice_item(item)

    def _apply_button_style(self, button: QPushButton, item: ChoiceItem) -> None:
        tokens = get_design_tokens()
        base_text = tokens["text"]
        accent = self._style_color(item.color)
        border = accent.darker(125)
        hover_fill = self._rgba(accent, 36 if accent.lightness() < 180 else 48)
        is_unset = self._is_unset_choice_item(item)
        alert_colors = lab_state_alert_colors()
        border_fill = "transparent"
        selected_bg = alert_colors["border"] if is_unset else accent.name()
        selected_text = "#ffffff" if is_unset else self._contrast_text_color(accent)
        selected_border = alert_colors["border"] if is_unset else border.name()
        selected_hover = self._rgba(QColor(alert_colors["border"]), 48) if is_unset else hover_fill
        button.setStyleSheet(
            "QPushButton#adaptiveChoicePill {"
            f" color: {base_text};"
            f" border: 1px solid {border_fill};"
            " border-radius: 10px;"
            f" padding: {'4px 8px' if self._compact else '6px 10px'};"
            " min-height: 0px;"
            " font-family: 'Manrope', sans-serif;"
            " font-size: 10pt;"
            " font-weight: 700;"
            " background-color: transparent;"
            " }"
            "QPushButton#adaptiveChoicePill:disabled {"
            f" color: {base_text};"
            " background-color: transparent;"
            " }"
            "QPushButton#adaptiveChoicePill:checked:disabled {"
            f" color: {selected_text};"
            f" background-color: {selected_bg};"
            f" border-color: {selected_border};"
            " }"
            "QPushButton#adaptiveChoicePill:hover:!checked {"
            f" background-color: {selected_hover};"
            " }"
            "QPushButton#adaptiveChoicePill:checked {"
            f" background-color: {selected_bg};"
            f" color: {selected_text};"
            f" border-color: {selected_border};"
            " }"
        )

    def _sync_button_states(self) -> None:
        for index, button in enumerate(self._buttons):
            if button is None:
                continue
            button.setChecked(index == self._current_index)

    def _refresh_mode_for_width(self) -> None:
        desired_mode = self._desired_mode()
        if desired_mode == self._mode:
            if self._mode == "pill":
                self._update_pill_button_widths()
            return
        self._mode = desired_mode
        self._stack.setCurrentWidget(self._pill_container if self._mode == "pill" else self._combo)
        if self._mode == "combo":
            self._combo.blockSignals(True)
            self._combo.setCurrentIndex(self._current_index)
            self._combo.blockSignals(False)
        else:
            self._update_pill_button_widths()
        self._apply_container_style()
        self.set_lab_state_alert(self._lab_state_alert)
        self.updateGeometry()

    def _desired_mode(self) -> str:
        if self._has_dropdown_only_items() or not self.buttons():
            return "combo"
        available = self.contentsRect().width()
        if available <= 0:
            available = self.width()
        if available <= 0:
            return "combo"
        required = self._pill_required_width()
        return "pill" if required <= available else "combo"

    def _has_dropdown_only_items(self) -> bool:
        return any(item.dropdown_only for item in self._items)

    def _schedule_fit_update(self, delay_ms: int) -> None:
        self._fit_timer.start(max(0, int(delay_ms)))

    def _pill_required_width(self) -> int:
        buttons = self.buttons()
        if not buttons:
            return 0
        spacing = max(0, self._pill_container_layout.spacing())
        total_spacing = spacing * max(0, len(buttons) - 1)
        required = total_spacing
        for index, button in enumerate(buttons):
            item = self._items[index]
            if self._is_unset_choice_item(item):
                required += max(36, button.sizeHint().width())
            else:
                required += button.sizeHint().width()
        return required

    def _update_pill_button_widths(self) -> None:
        if self._mode != "pill":
            return
        buttons = self.buttons()
        if not buttons:
            return
        available = self.contentsRect().width()
        if available <= 0:
            return
        spacing = max(0, self._pill_container_layout.spacing())
        total_spacing = spacing * max(0, len(buttons) - 1)
        usable = max(0, available - total_spacing)
        if usable <= 0:
            return
        fixed_widths: dict[int, int] = {}
        remaining_indices: list[int] = []
        for index, button in enumerate(buttons):
            if self._is_unset_choice_item(self._items[index]):
                fixed_widths[index] = max(36, button.sizeHint().width())
            else:
                remaining_indices.append(index)
        remaining_space = usable - sum(fixed_widths.values())
        if remaining_space < 0:
            remaining_space = 0
        if remaining_indices:
            base_width = remaining_space // len(remaining_indices) if remaining_space > 0 else 0
            remainder = remaining_space - (base_width * len(remaining_indices))
        else:
            base_width = 0
            remainder = 0
        for index, button in enumerate(buttons):
            if index in fixed_widths:
                width = fixed_widths[index]
            else:
                remaining_pos = remaining_indices.index(index) if index in remaining_indices else -1
                width = base_width + (1 if 0 <= remaining_pos < remainder else 0)
                width = max(button.sizeHint().width(), width)
            button.setFixedWidth(max(0, width))

    def _apply_index(self, index: int, *, emit: bool) -> None:
        if index < -1 or index >= len(self._items):
            return
        if index == self._current_index:
            self._sync_visible_selection()
            return
        self._current_index = index
        self._sync_visible_selection()
        if not emit:
            return
        self.currentIndexChanged.emit(self._current_index)
        self.selectionChanged.emit(self.currentData())
        self.currentTextChanged.emit(self.currentText())

    def _sync_visible_selection(self) -> None:
        blockers = [self._combo]
        for button in self._buttons:
            if button is not None:
                blockers.append(button)
        for widget in blockers:
            widget.blockSignals(True)
        try:
            self._combo.setCurrentIndex(self._current_index)
            for index, button in enumerate(self._buttons):
                if button is None:
                    continue
                button.setChecked(index == self._current_index)
        finally:
            for widget in blockers:
                widget.blockSignals(False)

    def _on_combo_index_changed(self, index: int) -> None:
        if self._syncing_from_user():
            return
        self._apply_index(int(index), emit=not self.signalsBlocked())

    def _on_button_clicked(self, button: QPushButton) -> None:
        if self.signalsBlocked():
            index = int(button.property("choiceIndex") or -1)
            self._apply_index(index, emit=False)
            return
        index = int(button.property("choiceIndex") or -1)
        self._apply_index(index, emit=True)

    def _syncing_from_user(self) -> bool:
        return False


def _format_choice_number(value: Any) -> str | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if abs(number - round(number)) < 1e-6:
        return str(int(round(number)))
    text = f"{number:.2f}".rstrip("0").rstrip(".")
    return text or None


def _parse_objective_magnification(obj: Any, fallback_key: str | None = None) -> float | None:
    for candidate in (
        obj.get("magnification") if isinstance(obj, dict) else None,
        fallback_key,
        obj.get("name") if isinstance(obj, dict) else None,
    ):
        if candidate is None:
            continue
        try:
            return float(candidate)
        except (TypeError, ValueError):
            match = re.search(r"(\d+(?:\.\d+)?)", str(candidate))
            if match:
                try:
                    return float(match.group(1))
                except (TypeError, ValueError):
                    continue
    return None


def objective_short_label(obj: Any, fallback_key: str | None = None) -> str:
    if not isinstance(obj, dict):
        text = str(fallback_key or obj or "").strip()
        if not text:
            return ""
        match = re.search(r"(\d+(?:\.\d+)?)\s*[xX]", text)
        if match:
            return f"{match.group(1)}x"
        match = re.search(r"1\s*[:/_-]\s*(\d+(?:\.\d+)?)", text)
        if match:
            return f"1:{match.group(1)}"
        match = re.search(r"(\d+(?:\.\d+)?)", text)
        return f"{match.group(1)}x" if match else text

    optics_type = str(obj.get("optics_type") or "microscope").strip().lower()
    magnification = _parse_objective_magnification(obj, fallback_key)
    ratio_texts = (
        str(obj.get("name") or "").strip(),
        str(obj.get("objective_name") or "").strip(),
        str(fallback_key or "").strip(),
    )
    ratio_match = next((re.search(r"1\s*[:/_-]\s*(\d+(?:\.\d+)?)", text) for text in ratio_texts if text), None)
    if optics_type == "macro" or ratio_match:
        if magnification is not None:
            mag_text = _format_choice_number(magnification)
            if mag_text:
                return f"1:{mag_text}"
        if ratio_match:
            return f"1:{ratio_match.group(1)}"
        name = str(obj.get("name") or obj.get("objective_name") or fallback_key or "").strip()
        return name
    if magnification is not None:
        mag_text = _format_choice_number(magnification)
        if mag_text:
            return f"{mag_text}x"
    name = str(obj.get("name") or obj.get("objective_name") or fallback_key or "").strip()
    match = re.search(r"(\d+(?:\.\d+)?)\s*[xX]", name)
    if match:
        return f"{match.group(1)}x"
    match = re.search(r"(\d+(?:\.\d+)?)", name)
    if match:
        return f"{match.group(1)}x"
    return name


def objective_color(obj: Any, fallback_key: str | None = None) -> str:
    if isinstance(obj, dict):
        for key in ("color", "colour", "plot_color", "tag_color", "objective_color", "band_color", "display_color"):
            value = obj.get(key)
            if value:
                color = QColor(str(value))
                if color.isValid():
                    return color.name()
    magnification = _parse_objective_magnification(obj, fallback_key)
    if objective_is_macro_profile(obj, fallback_key):
        return "#6b7280"
    if magnification is None:
        return "#4d7c7a"
    bands = (
        (1.5, "#1f1f1f"),
        (3.0, "#8f9398"),
        # Keep 4x in the neutral band; reserve red for the slightly higher low-power range.
        (4.5, "#4d7c7a"),
        (5.5, "#d64545"),
        (12.0, "#f1c40f"),
        (22.0, "#2ecc71"),
        (35.0, "#16a085"),
        (55.0, "#5dade2"),
        (70.0, "#1f4ea8"),
    )
    for threshold, color in bands:
        if magnification <= threshold:
            return color
    if magnification >= 90.0:
        return "#f7f7f7"
    return "#1f4ea8"


def objective_is_macro_profile(obj: Any, fallback_key: str | None = None) -> bool:
    if not isinstance(obj, dict):
        text = str(fallback_key or obj or "").strip()
        return bool(re.search(r"1\s*[:/]\s*\d+(?:\.\d+)?", text))
    if str(obj.get("optics_type") or "").strip().lower() == "macro":
        return True
    for candidate in (
        str(obj.get("name") or "").strip(),
        str(obj.get("objective_name") or "").strip(),
        str(fallback_key or "").strip(),
    ):
        if candidate and re.search(r"1\s*[:/_-]\s*\d+(?:\.\d+)?", candidate):
            return True
    return False


def stain_color(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    normalized = re.sub(r"[\s_-]+", "", text)
    mapping = {
        "melzer": "#9c7b3b",
        "congored": "#c0392b",
        "cottonblue": "#2980b9",
        "lactofuchsin": "#d46a9a",
        "cresylblue": "#6f63c7",
        "trypanblue": "#3b82f6",
        "chlorazolblacke": "#2d3436",
    }
    return mapping.get(normalized)
