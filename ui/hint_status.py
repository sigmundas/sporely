"""Reusable hint/status helpers for dialogs and windows."""
from __future__ import annotations

from PySide6.QtCore import QObject, QEvent, QTimer, Qt
from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import QLabel, QWidget


class HintStatusController(QObject):
    """Controls a persistent hint plus temporary status messages in a QLabel."""

    def __init__(self, hint_label: QLabel, parent: QObject | None = None) -> None:
        super().__init__(parent or hint_label)
        self._hint_label = hint_label
        self._hint_text = ""
        self._hint_tone = "info"
        self._status_tone = "info"
        self._status_timer = QTimer(self)
        self._status_timer.setSingleShot(True)
        self._status_timer.timeout.connect(self._restore_hint)
        self._apply_idle_style()

    def _set_label_text(self, text: str | None) -> None:
        self._hint_label.setText((text or "").strip())

    def _apply_idle_style(self) -> None:
        self._hint_label.setStyleSheet(
            "QLabel { background: transparent; border: none; padding-left: 0px; color: #222222; }"
        )

    def _apply_active_style(self, tone: str = "info") -> None:
        normalized = (tone or "info").strip().lower()
        if normalized == "success":
            bg = "#eefaf3"
            border = "#27ae60"
        elif normalized in {"warning", "error"}:
            bg = "#fdecec"
            border = "#e74c3c"
        else:
            bg = "#f0f7ff"
            border = "#0066cc"
        self._hint_label.setStyleSheet(
            "QLabel { "
            f"background: {bg}; "
            f"border-left: 3px solid {border}; "
            "padding-left: 6px; "
            "color: #222222; "
            "}"
        )

    def _restore_hint(self) -> None:
        if self._hint_text:
            self._set_label_text(self._hint_text)
            self._apply_active_style(self._hint_tone)
        else:
            self._set_label_text("")
            self._apply_idle_style()

    def set_hint(self, text: str | None, tone: str = "info") -> None:
        self._hint_text = (text or "").strip()
        self._hint_tone = (tone or "info").strip().lower()
        if not self._status_timer.isActive():
            if self._hint_text:
                self._set_label_text(self._hint_text)
                self._apply_active_style(self._hint_tone)
            else:
                self._set_label_text("")
                self._apply_idle_style()

    def set_status(self, text: str | None, timeout_ms: int = 4000, tone: str = "info") -> None:
        message = (text or "").strip()
        if not message:
            self._status_timer.stop()
            self._restore_hint()
            return
        self._status_tone = (tone or "info").strip().lower()
        self._set_label_text(message)
        self._apply_active_style(self._status_tone)
        if timeout_ms and timeout_ms > 0:
            self._status_timer.start(int(timeout_ms))
        else:
            self._status_timer.stop()

    def register_widget(
        self,
        widget: QWidget,
        hint_text: str | None,
        tone: str = "info",
        allow_when_disabled: bool = False,
    ) -> None:
        """Connect widget hover enter/leave to the hint bar and suppress native tooltips."""
        hint = (hint_text or "").strip()
        widget.setProperty("_hint_text", hint)
        widget.setProperty("_hint_tone", (tone or "info").strip().lower())
        widget.setProperty("_hint_allow_disabled", bool(allow_when_disabled))
        widget.setToolTip("")
        widget.installEventFilter(self)

    def eventFilter(self, watched: QObject, event: QEvent) -> bool:
        hint_text = watched.property("_hint_text") if hasattr(watched, "property") else None
        hint_tone = watched.property("_hint_tone") if hasattr(watched, "property") else None
        allow_when_disabled = (
            watched.property("_hint_allow_disabled") if hasattr(watched, "property") else False
        )
        tone = hint_tone if isinstance(hint_tone, str) and hint_tone else "info"
        allow_disabled = bool(allow_when_disabled)
        is_enabled = True
        if isinstance(watched, QWidget):
            is_enabled = watched.isEnabled()
        if isinstance(hint_text, str) and hint_text:
            if event.type() == QEvent.Enter:
                if is_enabled or allow_disabled:
                    self.set_hint(hint_text, tone=tone)
                else:
                    self.set_hint("")
            elif event.type() == QEvent.Leave:
                self.set_hint("")
            elif event.type() == QEvent.EnabledChange and not is_enabled and not allow_disabled:
                self.set_hint("")
        return super().eventFilter(watched, event)


class HintLabel(QLabel):
    """Hover-aware label with reliable interactive underline and hint forwarding."""

    def __init__(
        self,
        text: str = "",
        hint_text: str = "",
        set_hint_callback=None,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._plain_text = ""
        self._hint_text = (hint_text or "").strip()
        self._set_hint_callback = set_hint_callback
        self.setProperty("hint_interactive", "true")
        self.style().unpolish(self)
        self.style().polish(self)
        self.setTextInteractionFlags(Qt.NoTextInteraction)
        self.setTextFormat(Qt.PlainText)
        # Keep text readable; underline is drawn in paintEvent.
        self.setStyleSheet("QLabel { color: #2c3e50; }")
        self.setToolTip("")
        self._apply_hint_affordance()
        self.setText(text)

    def _apply_hint_affordance(self) -> None:
        if self._hint_text:
            self.setCursor(Qt.WhatsThisCursor)
        else:
            self.unsetCursor()

    def set_hint_text(self, hint_text: str | None) -> None:
        self._hint_text = (hint_text or "").strip()
        self.setToolTip("")
        self._apply_hint_affordance()

    def setText(self, text: str) -> None:  # noqa: N802 - Qt API
        self._plain_text = text or ""
        super().setText(self._plain_text)

    def enterEvent(self, event) -> None:
        if callable(self._set_hint_callback):
            self._set_hint_callback(self._hint_text)
        super().enterEvent(event)

    def leaveEvent(self, event) -> None:
        if callable(self._set_hint_callback):
            self._set_hint_callback("")
        super().leaveEvent(event)

    def paintEvent(self, event) -> None:
        super().paintEvent(event)
        if not self._hint_text:
            return
        text = self.text() or ""
        if not text:
            return
        fm = self.fontMetrics()
        line = text.splitlines()[0] if text else ""
        if not line:
            return
        width = fm.horizontalAdvance(line)
        if width <= 0:
            return
        rect = self.contentsRect()
        y = rect.top() + fm.ascent() + 2
        x = rect.left()
        painter = QPainter(self)
        pen = QPen(QColor("#0066cc"))
        pen.setStyle(Qt.DotLine)
        pen.setWidthF(1.0)
        painter.setPen(pen)
        painter.drawLine(x, y, x + width, y)
        painter.end()
