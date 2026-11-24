"""Collapsible section card with a summary line (UI redesign stage 1).

A ``CollapsibleSection`` looks like a ``create_section_card`` card, but its
heading strip is a button: an arrow, the title and a one-line summary of the
section's values. Clicking the heading, or pressing Space or Enter while it
has focus, opens or closes the body with a short animation. When the section
has a ``settings_key`` its open state is stored in ``SettingsDB`` and restored
the next time the section is built.

Layout rules: the heading keeps one fixed height whatever the summary says
(long summaries are elided, the full text is the tooltip), so a changing
summary never moves anything else; only opening or closing changes the
section's height.
"""
from __future__ import annotations

from typing import Any, Type

from PySide6.QtCore import QEasingCurve, QEvent, QPropertyAnimation, Qt, Signal
from PySide6.QtGui import QFontMetrics
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QLayout,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

ANIMATION_MS = 180
_MAX_HEIGHT = 16777215  # QWIDGETSIZE_MAX


class _ElidedLabel(QLabel):
    """Single-line label that elides its text to the width it gets.

    With ``text_setter`` its ``setText`` and ``text`` take and return the
    full text, so code written for a plain ``QLabel`` can use it as is.
    """

    def __init__(
        self, parent: QWidget | None = None, *, text_setter: bool = False, elide_mode=Qt.ElideRight,
    ) -> None:
        super().__init__(parent)
        self._full_text = ""
        self._elide_mode = elide_mode
        self._text_setter = bool(text_setter)
        self.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Fixed)
        self.setTextFormat(Qt.PlainText)

    def setText(self, text) -> None:  # noqa: N802 - Qt override
        if self._text_setter:
            self.set_full_text(text)
        else:
            super().setText(text)

    def text(self) -> str:
        return self._full_text if self._text_setter else super().text()

    def set_full_text(self, text: str) -> None:
        self._full_text = str(text or "")
        self.setToolTip(self._full_text)
        self._refresh()

    def full_text(self) -> str:
        return self._full_text

    def resizeEvent(self, event) -> None:  # type: ignore[override]
        super().resizeEvent(event)
        self._refresh()

    def _refresh(self) -> None:
        metrics = QFontMetrics(self.font())
        super().setText(metrics.elidedText(self._full_text, self._elide_mode, max(0, self.width())))


ElidedLabel = _ElidedLabel


class _SectionHeader(QFrame):
    """The clickable heading strip of a collapsible section."""

    activated = Signal()

    def __init__(self, title: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("boxHeader")
        self.setProperty("sectionToggle", True)
        self.setFocusPolicy(Qt.StrongFocus)
        self.setCursor(Qt.PointingHandCursor)
        # The heading keeps its own height; extra room never goes to it.
        self.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(12, 8, 12, 8)
        layout.setSpacing(8)

        self.arrow_label = QLabel(self)
        self.arrow_label.setObjectName("sectionArrowLabel")
        self.arrow_label.setFixedWidth(12)
        self.title_label = QLabel(str(title or "").upper(), self)
        self.title_label.setObjectName("metaLabel")
        self.summary_label = _ElidedLabel(self)
        self.summary_label.setObjectName("sectionSummaryLabel")
        # A warning (an unset value) sits after the summary and is never elided,
        # so it stays readable however long the summary is.
        self.alert_label = QLabel(self)
        self.alert_label.setObjectName("sectionSummaryAlertLabel")
        self.alert_label.setTextFormat(Qt.PlainText)
        self.alert_label.setVisible(False)
        for label in (self.arrow_label, self.title_label, self.summary_label, self.alert_label):
            label.setAttribute(Qt.WA_TransparentForMouseEvents, True)
        layout.addWidget(self.arrow_label, 0)
        layout.addWidget(self.title_label, 0)
        layout.addWidget(self.summary_label, 1)
        layout.addWidget(self.alert_label, 0)

    def mouseReleaseEvent(self, event) -> None:  # type: ignore[override]
        if event.button() == Qt.LeftButton and self.rect().contains(event.position().toPoint()):
            self.activated.emit()
            event.accept()
            return
        super().mouseReleaseEvent(event)

    _TOGGLE_KEYS = (Qt.Key_Space, Qt.Key_Return, Qt.Key_Enter)

    def event(self, event) -> bool:  # type: ignore[override]
        # Claim the toggle keys before window shortcuts see them: Live Lab binds
        # Return/Enter to "save current" while reviewing RAW captures.
        if event.type() == QEvent.ShortcutOverride and event.key() in self._TOGGLE_KEYS and not event.modifiers():
            event.accept()
            return True
        return super().event(event)

    def keyPressEvent(self, event) -> None:  # type: ignore[override]
        if event.key() in self._TOGGLE_KEYS and not event.modifiers():
            if not event.isAutoRepeat():
                self.activated.emit()
            event.accept()
            return
        super().keyPressEvent(event)


class CollapsibleSection(QFrame):
    """A section card whose body opens and closes under a summary heading."""

    toggled = Signal(bool)  # True when the section is now open

    def __init__(
        self,
        title: str,
        layout_type: Type[QLayout] = QVBoxLayout,
        *,
        settings_key: str | None = None,
        expanded: bool = True,
        parent: QWidget | None = None,
        body_margins: tuple[int, int, int, int] = (12, 12, 12, 12),
        body_spacing: int = 8,
    ) -> None:
        super().__init__(parent)
        self.setObjectName("sectionCard")
        self.setFrameShape(QFrame.NoFrame)
        self._title = str(title or "")
        self._settings_key = settings_key

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        self.header = _SectionHeader(self._title, self)
        self.header.setAccessibleName(self._title)
        self.header.activated.connect(self.toggle)
        outer.addWidget(self.header)

        self.body = QWidget(self)
        self.body_layout: Any = layout_type(self.body)
        self.body_layout.setContentsMargins(*body_margins)
        self.body_layout.setSpacing(body_spacing)
        outer.addWidget(self.body)

        self._animation = QPropertyAnimation(self.body, b"maximumHeight", self)
        self._animation.setDuration(ANIMATION_MS)
        self._animation.setEasingCurve(QEasingCurve.OutCubic)
        self._animation.finished.connect(self._on_animation_finished)

        stored = self._stored_expanded()
        self._expanded = bool(expanded) if stored is None else stored
        self._apply_expanded_immediately()

    # ------------------------------------------------------------------
    def title(self) -> str:
        return self._title

    def is_expanded(self) -> bool:
        return self._expanded

    def summary(self) -> str:
        return self.header.summary_label.full_text()

    def summary_alert(self) -> str:
        return self.header.alert_label.text() if self.header.alert_label.isVisibleTo(self.header) else ""

    def set_summary(self, text: str, *, alert: str = "") -> None:
        """Set the heading's summary line, and a warning shown after it in the alert colour."""
        text = str(text or "")
        alert = str(alert or "")
        self.header.summary_label.set_full_text(text)
        self.header.alert_label.setText(alert)
        self.header.alert_label.setVisible(bool(alert))
        description = " · ".join(part for part in (text, alert) if part)
        self.header.setAccessibleDescription(description)
        self.header.setToolTip(description)

    def toggle(self) -> None:
        self.set_expanded(not self._expanded)

    def set_expanded(self, expanded: bool, *, animate: bool = True) -> None:
        expanded = bool(expanded)
        if expanded == self._expanded:
            return
        self._expanded = expanded
        self._store_expanded()
        self._update_header_state()
        if not animate or not self.isVisible():
            self._animation.stop()
            self._apply_expanded_immediately()
        else:
            # Start from wherever the body is now, so a second click during the
            # animation simply turns it around.
            start = self.body.height() if self.body.isVisible() else 0
            self.body.setVisible(True)
            end = self.body.sizeHint().height() if expanded else 0
            self._animation.stop()
            self._animation.setStartValue(start)
            self._animation.setEndValue(max(0, end))
            self.body.setMaximumHeight(start)
            self._animation.start()
        self.toggled.emit(self._expanded)

    # ------------------------------------------------------------------
    def _apply_expanded_immediately(self) -> None:
        self.body.setVisible(self._expanded)
        self.body.setMaximumHeight(_MAX_HEIGHT if self._expanded else 0)
        self._update_header_state()

    def _on_animation_finished(self) -> None:
        if self._expanded:
            self.body.setMaximumHeight(_MAX_HEIGHT)
        else:
            self.body.setVisible(False)

    def _update_header_state(self) -> None:
        self.header.arrow_label.setText("▾" if self._expanded else "▸")
        self.header.setProperty("collapsed", not self._expanded)
        self.header.setAccessibleName(self._title)
        style = self.header.style()
        style.unpolish(self.header)
        style.polish(self.header)

    def _stored_expanded(self) -> bool | None:
        if not self._settings_key:
            return None
        try:
            from database.models import SettingsDB

            value = SettingsDB.get_setting(self._settings_key, None)
        except Exception:
            return None
        if value in (None, ""):
            return None
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def _store_expanded(self) -> None:
        if not self._settings_key:
            return
        try:
            from database.models import SettingsDB

            SettingsDB.set_setting(self._settings_key, "1" if self._expanded else "0")
        except Exception:
            pass
