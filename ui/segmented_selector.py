"""Reusable segmented choice control used for compact pill-style options."""
from __future__ import annotations

from typing import Any

from PySide6.QtCore import Signal
from PySide6.QtWidgets import QButtonGroup, QFrame, QHBoxLayout, QLayout, QPushButton, QSizePolicy


class SegmentedSelector(QFrame):
    """A shared pill-style segmented selector made from checkable buttons."""

    selectionChanged = Signal(object)

    def __init__(
        self,
        parent=None,
        *,
        compact: bool = True,
        fill_width: bool = False,
        button_height: int | None = None,
        container_height: int | None = None,
        margins: tuple[int, int, int, int] | None = None,
        spacing: int | None = None,
    ) -> None:
        super().__init__(parent)
        self.setObjectName("segmentedControl")
        self.setProperty("compact", "true" if compact else "false")
        self._fill_width = bool(fill_width)
        self.setSizePolicy(
            QSizePolicy.Expanding if self._fill_width else QSizePolicy.Fixed,
            QSizePolicy.Fixed,
        )
        self._compact = bool(compact)
        self._buttons_by_value: dict[Any, QPushButton] = {}
        self._button_order: list[QPushButton] = []
        self.button_group = QButtonGroup(self)
        self.button_group.setExclusive(True)
        self.button_group.buttonClicked.connect(self._on_button_clicked)

        layout = QHBoxLayout(self)
        layout.setSizeConstraint(QLayout.SetDefaultConstraint if self._fill_width else QLayout.SetFixedSize)
        if margins is None:
            margins = (0, 0, 0, 0) if self._compact else (4, 4, 4, 4)
        if spacing is None:
            spacing = 0 if self._compact else 4
        layout.setContentsMargins(*margins)
        layout.setSpacing(spacing)

        self._layout = layout
        if container_height is None and not self._compact:
            container_height = 52
        if container_height is not None:
            self.setFixedHeight(int(container_height))

        if button_height is None:
            button_height = None if self._compact else 52
        self._button_height = int(button_height) if button_height is not None else None

    def add_option(
        self,
        text: str,
        value: Any = None,
        *,
        checked: bool = False,
        tooltip: str | None = None,
    ) -> QPushButton:
        button = QPushButton(str(text), self)
        button.setObjectName("segmentedButton")
        button.setProperty("compact", "true" if self._compact else "false")
        button.setCheckable(True)
        button.setSizePolicy(
            QSizePolicy.Expanding if self._fill_width else QSizePolicy.Fixed,
            QSizePolicy.Fixed,
        )
        if self._compact and self._button_height is not None:
            button.setMinimumHeight(self._button_height)
        if tooltip:
            button.setToolTip(str(tooltip))
        button.setProperty("segment_value", value)
        self.button_group.addButton(button)
        self._layout.addWidget(button, 1 if self._fill_width else 0)
        self._buttons_by_value[value] = button
        self._button_order.append(button)
        if checked:
            button.setChecked(True)
        return button

    def button_for_value(self, value: Any) -> QPushButton | None:
        return self._buttons_by_value.get(value)

    def buttons(self) -> list[QPushButton]:
        return list(self._button_order)

    def selected_value(self, fallback: Any | None = None) -> Any | None:
        checked = self.button_group.checkedButton()
        if checked is None:
            return fallback
        value = checked.property("segment_value")
        return fallback if value is None else value

    def currentData(self, fallback: Any | None = None) -> Any | None:  # noqa: N802 - Qt compatibility helper
        return self.selected_value(fallback)

    def currentIndex(self) -> int:  # noqa: N802 - Qt compatibility helper
        checked = self.button_group.checkedButton()
        if checked is None:
            return -1
        try:
            return self._button_order.index(checked)
        except ValueError:
            return -1

    def set_selected_value(self, value: Any, *, emit: bool = False) -> bool:
        button = self._buttons_by_value.get(value)
        if button is None:
            return False
        button.setChecked(True)
        if emit:
            self.selectionChanged.emit(button.property("segment_value"))
        return True

    def setCurrentIndex(self, index: int) -> bool:  # noqa: N802 - Qt compatibility helper
        try:
            button = self._button_order[int(index)]
        except Exception:
            return False
        return self.set_selected_value(button.property("segment_value"), emit=True)

    def _on_button_clicked(self, button: QPushButton) -> None:
        self.selectionChanged.emit(button.property("segment_value"))
