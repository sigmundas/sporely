"""Bars on the image for Bilder › Files and SD card (redesign stage 9f2).

Prepare Images, shown in place in Bilder, puts its per-image controls on the
image as in the mockup (Bilder-Filer): Field or Microscope with F/M at the top
left, a field photo's time and GPS on one line with “Use for the observation”
under it, the image tools at the bottom and Info at the top right. The bars
only show and drive the editor's own controls, which stay the source of truth.

Like the Live Lab capture-state bar they sit on the image, not on the app's
surfaces, so they look the same in both themes (``HUD_COLORS``).
"""
from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QPushButton, QSizePolicy, QWidget

from .styles import HUD_COLORS

FIELD_SELECTED = "#d6c9a8"  # the mockup's Felt colour (also its strip badge)
PRIMARY = "#3f7f7a"
PRIMARY_HOVER = "#4a908a"


def _button_css(name: str, *, selected_background: str = HUD_COLORS["selected_background"]) -> str:
    return (
        f"QPushButton#{name} {{"
        f" background-color: transparent; color: {HUD_COLORS['text']};"
        " border: 1px solid transparent; border-radius: 7px;"
        " padding: 6px 12px; font-size: 13px; font-weight: 600;"
        " }"
        f"QPushButton#{name}:hover {{ background-color: rgba(255, 255, 255, 30); }}"
        f"QPushButton#{name}:checked {{ background-color: {selected_background}; color: #111111; }}"
        f"QPushButton#{name}:focus {{ border: 1px solid {HUD_COLORS['focus']}; }}"
        f"QPushButton#{name}:disabled {{ color: rgba(224, 224, 224, 110); }}"
    )


class HudFrame(QFrame):
    """A dark rounded bar on the image."""

    def __init__(self, name: str, parent: QWidget | None = None, *, margins=(3, 3, 3, 3), spacing: int = 2) -> None:
        super().__init__(parent)
        self.setObjectName(name)
        self.setAttribute(Qt.WA_StyledBackground, True)
        self._base_css = (
            f"QFrame#{name} {{"
            f" background-color: {HUD_COLORS['background']};"
            f" border: 1px solid {HUD_COLORS['border']}; border-radius: 10px;"
            " }"
        )
        self.setStyleSheet(self._base_css)
        self.layout_ = QHBoxLayout(self)
        self.layout_.setContentsMargins(*margins)
        self.layout_.setSpacing(spacing)
        self.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

    def add_css(self, css: str) -> None:
        self.setStyleSheet(self._base_css + css)


class ImageTypeHud(HudFrame):
    """Field or Microscope for the selected images (keys F and M)."""

    chosen = Signal(str)  # "field" or "micro"

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__("imageTypeHud", parent)
        self.field_btn = self._add(self.tr("Field"), "F", "field", FIELD_SELECTED)
        self.micro_btn = self._add(self.tr("Microscope"), "M", "micro", HUD_COLORS["selected_background"])
        self.setAccessibleName(self.tr("Image type"))

    def _add(self, text: str, key: str, value: str, selected: str) -> QPushButton:
        button = QPushButton(f"{text}  {key}", self)
        name = f"imageType{value.title()}Button"
        button.setObjectName(name)
        button.setCheckable(True)
        button.setAutoExclusive(False)  # mixed selections check neither
        button.setFocusPolicy(Qt.NoFocus)  # F and M already work from the keyboard
        button.setToolTip(self.tr("{type} ({key})").format(type=text, key=key))
        button.setAccessibleName(text)
        button.setStyleSheet(_button_css(name, selected_background=selected) + f"QPushButton#{name} {{ font-size: 15px; padding: 7px 14px; }}")
        button.clicked.connect(lambda _checked=False, v=value: self._choose(v))
        self.layout_.addWidget(button)
        return button

    def _choose(self, value: str) -> None:
        self.set_current(value)  # until the editor reports back
        self.chosen.emit(value)

    def set_current(self, value: str | None) -> None:
        """``"field"``, ``"micro"`` or None for a mixed (or empty) selection."""
        self.field_btn.setChecked(value == "field")
        self.micro_btn.setChecked(value == "micro")

    def current(self) -> str | None:
        if self.field_btn.isChecked():
            return "field"
        if self.micro_btn.isChecked():
            return "micro"
        return None


class TimeGpsHud(HudFrame):
    """A field photo's time and GPS, with “Use for the observation”."""

    useRequested = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__("timeGpsHud", parent, margins=(12, 4, 4, 4), spacing=10)
        self.text_label = QLabel(self)
        self.text_label.setObjectName("timeGpsText")
        self.text_label.setStyleSheet(f"color: {HUD_COLORS['text']}; font-size: 12px; background: transparent;")
        self.text_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.layout_.addWidget(self.text_label)
        self.use_btn = QPushButton(self.tr("Use for the observation"), self)
        self.use_btn.setObjectName("timeGpsUseButton")
        self.use_btn.setStyleSheet(
            "QPushButton#timeGpsUseButton {"
            f" background-color: {PRIMARY}; color: #ffffff; border: none; border-radius: 7px;"
            " padding: 6px 12px; font-size: 13px; font-weight: 600;"
            " }"
            f"QPushButton#timeGpsUseButton:hover {{ background-color: {PRIMARY_HOVER}; }}"
            "QPushButton#timeGpsUseButton:disabled {"
            f" background-color: transparent; color: {HUD_COLORS['text']}; border: 1px solid {HUD_COLORS['border']};"
            " }"
        )
        self.use_btn.clicked.connect(self.useRequested)
        self.layout_.addWidget(self.use_btn)

    def set_values(self, text: str, *, used: bool) -> None:
        self.text_label.setText(text)
        self.use_btn.setEnabled(not used)
        self.use_btn.setText(self.tr("Used for the observation") if used else self.tr("Use for the observation"))
        self.adjustSize()


class ToolsHud(HudFrame):
    """The image tools (crop, AI crop, rotate): the editor's own buttons, on the image."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__("imageToolsHud", parent)
        self.setAccessibleName(self.tr("Image tools"))
        self.add_css(
            "QFrame#imageToolsHud QPushButton, QFrame#imageToolsHud QToolButton {"
            f" background-color: transparent; color: {HUD_COLORS['text']};"
            " border: 1px solid transparent; border-radius: 7px;"
            " padding: 6px 12px; font-size: 13px; font-weight: 600; min-height: 18px;"
            " }"
            "QFrame#imageToolsHud QPushButton:hover, QFrame#imageToolsHud QToolButton:hover {"
            " background-color: rgba(255, 255, 255, 30);"
            " }"
            "QFrame#imageToolsHud QPushButton:disabled, QFrame#imageToolsHud QToolButton:disabled {"
            " color: rgba(224, 224, 224, 90);"
            " }"
            'QFrame#imageToolsHud QPushButton[active="true"][cropRole="image"] { background-color: #b94a42; color: #ffffff; }'
            'QFrame#imageToolsHud QPushButton[active="true"][cropRole="ai"] { background-color: #b86b20; color: #ffffff; }'
            'QFrame#imageToolsHud QPushButton[hasUndo="true"] { color: #f0b3ad; }'
        )

    def add_tool(self, button: QWidget) -> None:
        button.setParent(self)
        self.layout_.addWidget(button)
        button.show()
