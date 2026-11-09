"""Adaptive choice selector that switches between pills and a dropdown."""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any
import re

from PySide6.QtCore import QEvent, QSize, Qt, QTimer, Signal
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
from .styles import (
    OBJECTIVE_RING_MACRO,
    OBJECTIVE_RING_UNKNOWN,
    REAGENT_COLORS,
    get_design_tokens,
    objective_ring_color,
    objective_ring_outline,
)


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
        self._mixed_values: list[Any] = []
        self._has_measured_width = False
        self._display_listeners: list = []
        # Chip mode (set_chip_mode): wrapping chips, no dropdown fallback.
        self._chip_mode = False
        self._chip_clear_value: Any = None
        self._chips_expanded = False
        self._chip_container: QWidget | None = None
        self._chip_layout = None
        self._more_chip: QPushButton | None = None
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
        for button in self._buttons:
            if button is not None and self._chip_layout is not None:
                index = self._chip_layout.index_of_widget(button)
                if index >= 0:
                    self._chip_layout.takeAt(index)
                    self._button_group.removeButton(button)
                    button.setParent(None)
                    button.deleteLater()
        self._buttons.clear()
        self._chips_expanded = False
        self._schedule_fit_update(0)
        self._notify_display_listeners()

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
            self._buttons.append(button)
            if self._chip_mode:
                self._place_chip(button)
            else:
                self._pill_container_layout.addWidget(button, 1)
            self._apply_button_style(button, item)
        else:
            self._buttons.append(None)

        if self._current_index < 0 and index == 0:
            self._apply_index(0, emit=False)
        else:
            self._sync_button_states()
            self._notify_display_listeners()
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
        if index == self._current_index:
            self._notify_display_listeners()

    def buttons(self) -> list[QPushButton]:
        return [button for button in self._buttons if button is not None]

    def button_for_value(self, value: Any) -> QPushButton | None:
        index = self.findData(value)
        if index < 0:
            return None
        return self._buttons[index]

    def selected_item(self) -> ChoiceItem | None:
        return self._item_at(self._current_index)

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

    def set_chip_mode(self, *, clear_value: Any = "Not_set") -> None:
        """Show the choices as wrapping chips instead of pills or a dropdown.

        Chips never fall back to a dropdown. The item whose value is
        ``clear_value`` (the "not set" choice) gets no chip; clicking the
        selected chip selects it instead, so a second click clears the
        choice. Chips that do not fit on one row fold behind a "+ N more"
        chip that shows them all in place; the selected chip always stays
        visible.
        """
        if self._chip_mode:
            return
        from .flow_layout import FlowLayout

        self._chip_mode = True
        self._chip_clear_value = clear_value
        self._chip_container = QWidget(self)
        self._chip_container.setObjectName("adaptiveChoiceChipContainer")
        self._chip_layout = FlowLayout(self._chip_container, spacing=4)
        self._more_chip = QPushButton(self._chip_container)
        self._more_chip.setObjectName("adaptiveChoiceMoreChip")
        self._more_chip.setCursor(self.cursor())
        self._more_chip.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self._more_chip.clicked.connect(self._expand_chips)
        self._more_chip.installEventFilter(self)
        self._chip_layout.addWidget(self._more_chip)
        for button in self._buttons:
            if button is None:
                continue
            self._pill_container_layout.removeWidget(button)
            self._place_chip(button)
        policy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        policy.setHeightForWidth(True)
        self.setSizePolicy(policy)
        self._stack.addWidget(self._chip_container)
        self._mode = "chip"
        self._stack.setCurrentWidget(self._chip_container)
        self._refresh_theme_styles()
        self._layout_chips()

    def item_at(self, index: int) -> ChoiceItem | None:
        return self._item_at(index)

    def is_unset_item(self, index: int) -> bool:
        """True for the "not set" choice ("Not_set", "—")."""
        item = self._item_at(index)
        return item is not None and self._is_unset_choice_item(item)

    def is_mixed_present(self, index: int) -> bool:
        """True when a mixed selection contains the item at ``index``."""
        return self._index_is_mixed_present(index)

    def is_chip_mode(self) -> bool:
        return self._chip_mode

    def chips_expanded(self) -> bool:
        return self._chips_expanded

    def more_chip(self) -> QPushButton | None:
        return self._more_chip

    def visible_chip_values(self) -> list[Any]:
        """Values of the chips shown right now, in order."""
        if not self._chip_mode:
            return []
        return [
            self._items[index].value
            for index, button in enumerate(self._buttons)
            if button is not None and not button.isHidden()
        ]

    def add_display_listener(self, callback) -> None:
        """Call ``callback()`` whenever the shown selection changes.

        Unlike ``currentIndexChanged`` this also fires for changes made while
        signals are blocked (a gallery selection loading its values), so a
        summary that mirrors the selector never goes stale.
        """
        self._display_listeners.append(callback)

    def _notify_display_listeners(self) -> None:
        for callback in list(self._display_listeners):
            callback()

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
    # Mixed-state API (used when a multi-selection has different values
    # for the same field). In this mode no pill is "selected"; every
    # value that's present anywhere in the selection is rendered dimmed
    # so the user can see the spread. Any user click exits mixed mode.
    def set_mixed_values(self, values: Iterable[Any]) -> None:
        self._mixed_values = list(values or [])
        self._current_index = -1
        self._combo.blockSignals(True)
        self._combo.setCurrentIndex(-1)
        self._combo.blockSignals(False)
        self._sync_visible_selection()
        self._refresh_theme_styles()

    def clear_mixed(self) -> None:
        if not self._mixed_values:
            return
        self._mixed_values = []
        self._refresh_theme_styles()
        self._notify_display_listeners()

    def is_mixed(self) -> bool:
        return bool(self._mixed_values)

    def _index_is_mixed_present(self, index: int) -> bool:
        if not self._mixed_values or index < 0 or index >= len(self._items):
            return False
        item = self._items[index]
        for value in self._mixed_values:
            if item.value == value:
                return True
        return False

    # ------------------------------------------------------------------
    # QWidget overrides
    def showEvent(self, event) -> None:  # type: ignore[override]
        super().showEvent(event)
        self._schedule_fit_update(0)
        self._update_pill_button_widths()

    def resizeEvent(self, event) -> None:  # type: ignore[override]
        super().resizeEvent(event)
        if self._chip_mode:
            # Cheap, and a deferred refit would leave a stale row on screen.
            self._layout_chips()
            return
        self._schedule_fit_update(75)
        self._update_pill_button_widths()

    def changeEvent(self, event) -> None:  # type: ignore[override]
        super().changeEvent(event)
        if event.type() in {QEvent.FontChange, QEvent.StyleChange, QEvent.PaletteChange}:
            self._refresh_theme_styles()
            self._schedule_fit_update(0)

    def event(self, event) -> bool:  # type: ignore[override]
        if event.type() == QEvent.LayoutRequest and not self._chip_mode:
            # Fires when the parent layout has finished computing geometry.
            # This is our most reliable signal that self.width() is real —
            # showEvent / QTimer(0) can both fire before the splitter and
            # form layout have settled on final widths.
            self._schedule_fit_update(0)
        return super().event(event)

    def hasHeightForWidth(self) -> bool:  # type: ignore[override]
        return self._chip_mode or super().hasHeightForWidth()

    def heightForWidth(self, width: int) -> int:  # type: ignore[override]
        if self._chip_mode and self._chip_layout is not None:
            return self._chip_layout.heightForWidth(width)
        return super().heightForWidth(width)

    def sizeHint(self):  # type: ignore[override]
        if self._chip_mode and self._chip_layout is not None:
            # The natural width of the whole row on one line; the height is
            # what that width needs. (Not the current width: a layout that
            # keeps fields at their size hint would otherwise never grow it.)
            spacing = self._chip_layout.spacing()
            widths = [
                button.sizeHint().width()
                for index, button in enumerate(self._buttons)
                if button is not None and not self._chip_is_hidden_choice(index)
            ]
            width = max(40, sum(widths) + spacing * max(0, len(widths) - 1))
            current = self.width() if self.width() > 40 else width
            return QSize(width, self._chip_layout.heightForWidth(current))
        combo_hint = self._combo.sizeHint()
        pill_hint = self._pill_container.sizeHint()
        return QSize(combo_hint.width(), max(combo_hint.height(), pill_hint.height()))

    def minimumSizeHint(self):  # type: ignore[override]
        if self._chip_mode and self._chip_layout is not None:
            return self._chip_layout.minimumSize()
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
        if self._chip_mode:
            self._apply_chip_container_style()
            self.update()
            return
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

    def _apply_chip_container_style(self) -> None:
        if self._chip_container is None:
            return
        tokens = get_design_tokens()
        if self._lab_state_alert:
            colors = lab_state_alert_colors()
            border = f"1px solid {colors['border']}"
            background = colors["background"]
        else:
            border = "1px solid transparent"
            background = "transparent"
        self._chip_container.setStyleSheet(
            "QWidget#adaptiveChoiceChipContainer {"
            f" background-color: {background}; border: {border}; border-radius: 10px;"
            " }"
        )
        if self._more_chip is not None:
            self._more_chip.setStyleSheet(
                "QPushButton#adaptiveChoiceMoreChip {"
                f" color: {tokens['text_dim']};"
                f" border: 1px dashed {tokens['data_brd']};"
                " border-radius: 10px;"
                f" padding: {'4px 8px' if self._compact else '6px 10px'};"
                " min-height: 0px; font-size: 10pt; font-weight: 600;"
                " background-color: transparent;"
                " }"
                "QPushButton#adaptiveChoiceMoreChip:hover {"
                f" background-color: {tokens['surface_hover']};"
                " }"
            )

    _CHIP_ACTIVATE_KEYS = (Qt.Key_Space, Qt.Key_Return, Qt.Key_Enter)
    _CHIP_MOVE_KEYS = (Qt.Key_Left, Qt.Key_Right)

    def eventFilter(self, obj, event) -> bool:  # type: ignore[override]
        """Chip keyboard: Left/Right move between chips, Space/Enter choose.

        A focused chip claims these keys before window shortcuts (Live Lab
        binds them to RAW review), and arrows never choose a chip: Qt's own
        arrow handling in an exclusive group clicks the focused button, which
        on the chosen chip would clear it.
        """
        if self._chip_mode and isinstance(obj, QPushButton):
            event_type = event.type()
            if event_type in (QEvent.ShortcutOverride, QEvent.KeyPress):
                key = event.key()
                plain = not (event.modifiers() & ~Qt.KeypadModifier)
                if plain and key in self._CHIP_ACTIVATE_KEYS + self._CHIP_MOVE_KEYS:
                    event.accept()
                    if event_type == QEvent.KeyPress and not event.isAutoRepeat():
                        if key in self._CHIP_MOVE_KEYS:
                            self._move_chip_focus(obj, -1 if key == Qt.Key_Left else 1)
                        else:
                            obj.click()
                    return True
        return super().eventFilter(obj, event)

    def _focusable_chips(self) -> list[QPushButton]:
        if self._chip_layout is None:
            return []
        chips = []
        for position in range(self._chip_layout.count()):
            widget = self._chip_layout.itemAt(position).widget()
            if isinstance(widget, QPushButton) and not widget.isHidden():
                chips.append(widget)
        return chips

    def _move_chip_focus(self, current: QPushButton, step: int) -> None:
        chips = self._focusable_chips()
        if current not in chips:
            return
        target = chips[(chips.index(current) + step) % len(chips)]
        target.setFocus(Qt.TabFocusReason)

    def _place_chip(self, button: QPushButton) -> None:
        """Put a choice button into the chip row, before the "+ N more" chip."""
        button.installEventFilter(self)
        button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        button.setMinimumWidth(0)
        button.setMaximumWidth(16777215)
        index = self._chip_layout.index_of_widget(self._more_chip)
        self._chip_layout.insert_widget(index if index >= 0 else self._chip_layout.count(), button)
        self._schedule_fit_update(0)

    def _chip_clear_index(self) -> int:
        return self.findData(self._chip_clear_value) if self._chip_clear_value is not None else -1

    def _chip_is_hidden_choice(self, index: int) -> bool:
        item = self._item_at(index)
        if item is None:
            return True
        return index == self._chip_clear_index() or self._is_unset_choice_item(item)

    def _expand_chips(self) -> None:
        had_focus = self._more_chip is not None and self._more_chip.hasFocus()
        self._chips_expanded = True
        self._layout_chips()
        self.updateGeometry()
        if had_focus:
            # The "+ N more" chip is now hidden; keep keyboard focus in the row.
            chips = self._focusable_chips()
            if chips:
                chips[0].setFocus(Qt.TabFocusReason)

    def _layout_chips(self) -> None:
        """Show the chips that fit on one row (all of them when expanded)."""
        if not self._chip_mode or self._chip_layout is None or self._more_chip is None:
            return
        choices = [
            index
            for index, button in enumerate(self._buttons)
            if button is not None and not self._chip_is_hidden_choice(index)
        ]
        for index, button in enumerate(self._buttons):
            if button is not None and self._chip_is_hidden_choice(index):
                button.setVisible(False)
        visible = list(choices)
        # The selector's own width: the chip container only follows it once
        # the stacked layout has run, which can be after this.
        available = self.contentsRect().width()
        if not self._chips_expanded and available > 40:
            spacing = self._chip_layout.spacing()

            def fits(indices: list[int], extra: int) -> bool:
                widths = [self._buttons[i].sizeHint().width() for i in indices]
                total = sum(widths) + spacing * max(0, len(widths) - 1)
                if extra:
                    total += spacing + extra
                return total <= available

            if not fits(choices, 0):
                self._more_chip.setText(self.tr("+ {count} more").format(count=len(choices)))
                more_width = self._more_chip.sizeHint().width()
                keep: list[int] = []
                for index in choices:
                    if fits(keep + [index], more_width):
                        keep.append(index)
                    else:
                        break
                if not keep and choices:
                    keep.append(choices[0])  # always show a choice; "+ N more" may wrap
                # The selected chip (and, for a mixed selection, the chips
                # present in it) always stay visible: they replace the last
                # chips that fit.
                wanted = [i for i in choices if i == self._current_index or self._index_is_mixed_present(i)]
                for index in wanted:
                    if index in keep:
                        continue
                    while keep and not fits(keep + [index], more_width):
                        removable = [i for i in keep if i not in wanted]
                        if not removable:
                            break
                        keep.remove(removable[-1])
                    keep.append(index)
                visible = sorted(keep)
        hidden_count = len(choices) - len(visible)
        changed = False
        for index in choices:
            show = index in visible
            if self._buttons[index].isHidden() == show:
                self._buttons[index].setVisible(show)
                changed = True
        more_text = self.tr("+ {count} more").format(count=hidden_count)
        if self._more_chip.text() != more_text:
            self._more_chip.setText(more_text)
            changed = True
        if self._more_chip.isHidden() == (hidden_count > 0):
            self._more_chip.setVisible(hidden_count > 0)
            changed = True
        # Relayout only on a real change: invalidating posts a layout request,
        # which must not start another round of this.
        if changed:
            self._chip_layout.invalidate()
            self.updateGeometry()

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
        if background.isValid() and background.lightness() >= 180:
            return "#000000"
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
        for part in (item.value, item.pill_text, item.display_text, item.tooltip):
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
        selected_border = alert_colors["border"] if is_unset else (objective_ring_outline(accent) or border.name())
        selected_hover = self._rgba(QColor(alert_colors["border"]), 48) if is_unset else hover_fill
        # Look up whether this button belongs to a value present in the
        # current mixed selection — if so, render dimmed to signal "some of
        # the selected items have this value, but not all".
        button_index = button.property("choiceIndex")
        try:
            button_index = int(button_index) if button_index is not None else -1
        except Exception:
            button_index = -1
        is_mixed_present = self._index_is_mixed_present(button_index)
        base_background = "transparent"
        base_color = base_text
        base_border = tokens["data_brd"] if self._chip_mode else border_fill
        if is_mixed_present:
            base_accent = QColor(alert_colors["border"]) if is_unset else accent
            base_background = self._rgba(base_accent, 70)
            base_border = self._rgba(base_accent, 160)
        button.setStyleSheet(
            "QPushButton#adaptiveChoicePill {"
            f" color: {base_color};"
            f" border: 1px solid {base_border};"
            " border-radius: 10px;"
            f" padding: {'4px 8px' if self._compact else '6px 10px'};"
            " min-height: 0px;"
            " font-family: 'Manrope', sans-serif;"
            " font-size: 10pt;"
            " font-weight: 700;"
            f" background-color: {base_background};"
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
        if self._chip_mode:
            self._layout_chips()
            return
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
        # Refuse to commit to a mode based on a clearly unusable width — the
        # widget is likely inside a splitter / QScrollArea whose final
        # geometry hasn't been applied yet. Reschedule and keep the current
        # mode; LayoutRequest / resizeEvent will trigger a real check once
        # the width is known.
        if available < 40:
            self._schedule_fit_update(50)
            return self._mode if self._mode in {"pill", "combo"} else "combo"
        self._has_measured_width = True
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
        was_mixed = bool(self._mixed_values)
        if was_mixed:
            self._mixed_values = []
        if index == self._current_index:
            self._sync_visible_selection()
            if was_mixed:
                self._refresh_theme_styles()
            return
        self._current_index = index
        self._sync_visible_selection()
        if was_mixed:
            self._refresh_theme_styles()
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
        was_exclusive = self._button_group.exclusive()
        if self._current_index < 0 and was_exclusive:
            # An exclusive QButtonGroup otherwise refuses to uncheck its last
            # checked button, which made a mixed selector look like one value
            # was still selected instead of showing only the dimmed values.
            self._button_group.setExclusive(False)
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
            if was_exclusive:
                self._button_group.setExclusive(True)
        if self._chip_mode:
            self._layout_chips()
        self._notify_display_listeners()

    def _on_combo_index_changed(self, index: int) -> None:
        if self._syncing_from_user():
            return
        self._apply_index(int(index), emit=not self.signalsBlocked())

    def _on_button_clicked(self, button: QPushButton) -> None:
        choice_index = button.property("choiceIndex")
        try:
            index = int(choice_index) if choice_index is not None else -1
        except Exception:
            index = -1
        if self._chip_mode:
            if index == self._current_index and self._chip_clear_index() >= 0:
                index = self._chip_clear_index()
            if self._chips_expanded:
                self._chips_expanded = False
        if self.signalsBlocked():
            self._apply_index(index, emit=False)
        else:
            self._apply_index(index, emit=True)
        if self._chip_mode:
            self._layout_chips()

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
            # Prefer an explicit "40X" over the first number, so a name that
            # starts with its NA ("NA 0.65 40X Plan") still reads as 40x.
            text = str(candidate)
            match = re.search(r"(\d+(?:\.\d+)?)\s*[xX]", text) or re.search(r"(\d+(?:\.\d+)?)", text)
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
    """Return an objective's tag colour: its stored colour, else its ISO 8578 ring colour.

    Macro profiles are neutral grey; an objective with no known magnification
    gets ``OBJECTIVE_RING_UNKNOWN``. The ring colours live in ``ui.styles``.
    """
    if isinstance(obj, dict):
        for key in ("color", "colour", "plot_color", "tag_color", "objective_color", "band_color", "display_color"):
            value = obj.get(key)
            if value:
                color = QColor(str(value))
                if color.isValid():
                    return color.name()
    magnification = _parse_objective_magnification(obj, fallback_key)
    if objective_is_macro_profile(obj, fallback_key):
        return OBJECTIVE_RING_MACRO
    if magnification is None:
        return OBJECTIVE_RING_UNKNOWN
    return objective_ring_color(magnification)


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
    return REAGENT_COLORS.get(normalized)
