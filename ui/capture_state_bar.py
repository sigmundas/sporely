"""Capture-state bar for the Live Lab viewer (UI redesign stage 2).

A floating bar of large segmented buttons over the live view: one group for
the objective, one for the contrast method. Each group mirrors an
``AdaptiveChoiceSelector`` that stays the source of truth (session logging,
saved state, multi-selection and the unset alert all keep working through
it); the buttons only show its items and choose them.

Objective buttons carry their ring colour: the chosen one is filled with it,
the others show it as a stripe along the bottom. The chosen contrast method is
light on the dark bar. The bar looks the same in both themes, since it sits on
the image rather than on the app's surfaces.
"""
from __future__ import annotations

from typing import Any

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QColor
from PySide6.QtWidgets import QBoxLayout, QFrame, QHBoxLayout, QPushButton, QSizePolicy, QWidget

from .combo_alerts import combo_is_unset, lab_state_alert_colors
from .styles import HUD_COLORS, objective_ring_outline


def readable_text_color(background: QColor | str) -> str:
    """Black or white text, whichever reads better on ``background``."""
    color = QColor(background)
    return "#111111" if color.isValid() and color.lightness() >= 160 else "#ffffff"


class SelectorSegments(QFrame):
    """Large segmented buttons that mirror (and drive) an ``AdaptiveChoiceSelector``."""

    rebuilt = Signal()  # the buttons were recreated (new items, new size)
    chosen = Signal(object)  # a button was clicked (also when already selected)

    def __init__(
        self,
        selector,
        *,
        ring_colors: bool,
        accessible_name: str,
        shortcut_key=None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setObjectName("captureSegments")
        self.setAccessibleName(accessible_name)
        self._selector = selector
        self._ring_colors = bool(ring_colors)
        # shortcut_key(value, position) -> key text for the tooltip, or None.
        self._shortcut_key = shortcut_key
        self._buttons: dict[int, QPushButton] = {}
        self._signature: tuple = ()
        self._compact = False
        self._layout = QHBoxLayout(self)
        self._layout.setContentsMargins(3, 3, 3, 3)
        self._layout.setSpacing(2)
        self.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        selector.add_display_listener(self.refresh)
        self.refresh()

    # ------------------------------------------------------------------
    def set_compact(self, compact: bool) -> None:
        """Smaller buttons, for a viewer too narrow for the normal ones."""
        if bool(compact) != self._compact:
            self._compact = bool(compact)
            self._restyle()
            self.adjustSize()

    def is_compact(self) -> bool:
        return self._compact

    def buttons(self) -> dict[Any, QPushButton]:
        """Visible buttons by the selector value they choose."""
        return {self._selector.itemData(index): button for index, button in self._buttons.items()}

    def button_for_value(self, value: Any) -> QPushButton | None:
        return self.buttons().get(value)

    def refresh(self) -> None:
        """Rebuild the buttons when the selector's items change, then restyle them."""
        selector = self._selector
        items = []
        for index in range(selector.count()):
            item = selector.item_at(index)
            if item is None or selector.is_unset_item(index):
                continue
            key = self._shortcut_key(item.value, len(items)) if callable(self._shortcut_key) else None
            items.append(
                (index, item.value, item.pill_text or item.display_text, item.tooltip, str(item.color or ""), key)
            )
        signature = tuple(items)
        rebuilt = signature != self._signature
        if rebuilt:
            self._signature = signature
            self._rebuild(items)
        self._restyle()
        if rebuilt:
            # After styling: the new buttons' sizes come from their style.
            self.adjustSize()
            self.updateGeometry()
            self.rebuilt.emit()

    # ------------------------------------------------------------------
    def _rebuild(self, items) -> None:
        for button in self._buttons.values():
            self._layout.removeWidget(button)
            button.setParent(None)
            button.deleteLater()
        self._buttons = {}
        for index, value, text, tooltip, _color, key in items:
            button = QPushButton(str(text), self)
            button.setObjectName("captureSegmentButton")
            button.setCursor(Qt.PointingHandCursor)
            button.setFocusPolicy(Qt.TabFocus)
            button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
            hint = str(tooltip or text)
            button.setToolTip(f"{hint} ({key})" if key else hint)
            button.setAccessibleName(str(tooltip or text))
            button.clicked.connect(lambda _checked=False, v=value: self._choose(v))
            self._layout.addWidget(button)
            # A child created after its parent is shown stays hidden until a
            # later event; show it now so the new size is measured at once.
            button.show()
            self._buttons[index] = button

    def _choose(self, value: Any) -> None:
        if self._selector.selected_value() != value or self._selector.is_mixed():
            self._selector.set_selected_value(value, emit=True)
        self.chosen.emit(value)

    def _restyle(self) -> None:
        selector = self._selector
        current = selector.currentIndex()
        alert = combo_is_unset(selector) and not selector.is_mixed()
        if alert:
            colors = lab_state_alert_colors()
            border = f"1px solid {colors['border']}"
        else:
            border = f"1px solid {HUD_COLORS['border']}"
        self.setStyleSheet(
            "QFrame#captureSegments {"
            f" background-color: {HUD_COLORS['background']}; border: {border}; border-radius: 10px;"
            " }"
        )
        for index, button in self._buttons.items():
            item = selector.item_at(index)
            if item is None:
                continue
            selected = index == current and not selector.is_mixed()
            mixed = selector.is_mixed_present(index)
            ring = QColor(item.color) if self._ring_colors and item.color else QColor()
            if selected:
                background = ring.name() if ring.isValid() else HUD_COLORS["selected_background"]
                foreground = readable_text_color(background)
            else:
                background = "rgba(255, 255, 255, 40)" if mixed else "transparent"
                foreground = HUD_COLORS["text"]
            outline = objective_ring_outline(ring) if ring.isValid() else None
            side = f"1px solid {outline}" if outline else "1px solid transparent"
            bottom = f"4px solid {ring.name()}" if ring.isValid() and not selected else side
            if mixed and not selected:
                side = f"1px dashed {HUD_COLORS['text']}"
            button.setProperty("selected", selected)
            button.setStyleSheet(
                "QPushButton#captureSegmentButton {"
                f" background-color: {background}; color: {foreground};"
                f" border: {side}; border-bottom: {bottom};"
                " border-radius: 7px;"
                f" padding: {'5px 7px' if self._compact else '7px 12px'};"
                f" min-width: {'24px' if self._compact else '34px'};"
                f" font-size: {'12px' if self._compact else '14px'}; font-weight: 600;"
                " }"
                "QPushButton#captureSegmentButton:hover {"
                f" background-color: {background if selected else 'rgba(255, 255, 255, 30)'};"
                " }"
                "QPushButton#captureSegmentButton:focus {"
                f" border: 1px solid {HUD_COLORS['focus']};"
                f" border-bottom: {bottom if not selected else '1px solid ' + HUD_COLORS['focus']};"
                " }"
            )


class CaptureStateBar(QFrame):
    """The floating bar: objective segments and contrast segments side by side."""

    rebuilt = Signal()  # a group recreated its buttons: reposition, re-install filters

    def __init__(
        self,
        objective_selector,
        contrast_selector,
        *,
        objective_key=None,
        contrast_key=None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setObjectName("captureStateBar")
        self.setAttribute(Qt.WA_StyledBackground, False)
        self._layout = layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)
        self.objective_segments = SelectorSegments(
            objective_selector,
            ring_colors=True,
            accessible_name=self.tr("Objective"),
            shortcut_key=objective_key,
            parent=self,
        )
        self.contrast_segments = SelectorSegments(
            contrast_selector,
            ring_colors=False,
            accessible_name=self.tr("Contrast"),
            shortcut_key=contrast_key,
            parent=self,
        )
        layout.addWidget(self.objective_segments, 0, Qt.AlignHCenter)
        layout.addWidget(self.contrast_segments, 0, Qt.AlignHCenter)
        self.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.objective_segments.rebuilt.connect(self.rebuilt)
        self.contrast_segments.rebuilt.connect(self.rebuilt)

    def is_stacked(self) -> bool:
        return self._layout.direction() == QBoxLayout.TopToBottom

    def is_compact(self) -> bool:
        return self.objective_segments.is_compact()

    def fit_to_width(self, available: int) -> None:
        """Side by side when it fits; else contrast under objective; else smaller buttons too."""
        for stacked, compact in ((False, False), (True, False), (True, True)):
            self._layout.setDirection(QBoxLayout.TopToBottom if stacked else QBoxLayout.LeftToRight)
            self.objective_segments.set_compact(compact)
            self.contrast_segments.set_compact(compact)
            self._layout.activate()
            if self.sizeHint().width() <= available:
                return
