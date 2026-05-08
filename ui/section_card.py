"""Shared card sections with the Sporely box heading style."""
from __future__ import annotations

from typing import Any, Type

from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QLayout, QVBoxLayout, QWidget


class BoxHeader(QFrame):
    """Header strip used by section cards."""

    def __init__(self, title: str, parent: QWidget | None = None):
        super().__init__(parent)
        self.setObjectName("boxHeader")

        layout = QHBoxLayout(self)
        layout.setContentsMargins(12, 8, 12, 8)
        layout.setSpacing(8)

        self.title_label = QLabel(str(title or "").upper())
        self.title_label.setObjectName("metaLabel")
        layout.addWidget(self.title_label)
        layout.addStretch()

    def set_title(self, title: str) -> None:
        self.title_label.setText(str(title or "").upper())


def create_section_card(
    title: str,
    layout_type: Type[QLayout] = QVBoxLayout,
    *,
    parent: QWidget | None = None,
    body_margins: tuple[int, int, int, int] = (12, 12, 12, 12),
    body_spacing: int = 8,
) -> tuple[QFrame, Any]:
    """Create a framed section with a full-width heading strip."""
    card = QFrame(parent)
    card.setObjectName("sectionCard")
    card.setFrameShape(QFrame.NoFrame)

    outer_layout = QVBoxLayout(card)
    outer_layout.setContentsMargins(0, 0, 0, 0)
    outer_layout.setSpacing(0)

    header = BoxHeader(title, card)
    card._box_header = header
    outer_layout.addWidget(header)

    body = QWidget(card)
    body_layout = layout_type(body)
    body_layout.setContentsMargins(*body_margins)
    body_layout.setSpacing(body_spacing)
    outer_layout.addWidget(body, 1)
    return card, body_layout
