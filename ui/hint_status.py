"""Reusable hint/status helpers for dialogs and windows."""
from __future__ import annotations

from PySide6.QtCore import QEvent, QObject, QTimer, Qt
from PySide6.QtGui import QColor, QFont, QPainter, QPen
from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QProgressBar, QSizePolicy, QToolTip, QWidget

from .styles import pt

try:
    from shiboken6 import isValid as _qt_is_valid
except Exception:  # pragma: no cover - fallback for environments without shiboken helper
    def _qt_is_valid(obj) -> bool:
        return obj is not None


def _palette_is_dark() -> bool:
    from PySide6.QtWidgets import QApplication
    app = QApplication.instance()
    return app.palette().window().color().lightness() < 128 if app else False


def style_progress_widgets(
    progress_bar: QProgressBar | None,
    status_label: QLabel | None = None,
    percent_label: QLabel | None = None,
) -> None:
    """Apply a readable light/dark palette to progress UI elements."""
    dark = _palette_is_dark()
    if dark:
        bar_bg = "#25292f"
        bar_border = "#68707a"
        chunk = "#5aa2f2"
        text = "#eef5ff"
        status = "#7fc0ff"
    else:
        bar_bg = "#edf5f2"    # surface_container_low
        bar_border = "#a8b5b1"  # outline_variant
        chunk = "#47674a"     # primary
        text = "#293532"      # on_surface
        status = "#3b5a3e"    # primary_dim

    if progress_bar is not None:
        progress_bar.setStyleSheet(
            "QProgressBar {"
            f"background: {bar_bg};"
            f"border: 1px solid {bar_border};"
            "border-radius: 6px;"
            f"color: {text};"
            "text-align: center;"
            "padding: 0 4px;"
            f"font-size: {pt(9)}pt;"
            "}"
            "QProgressBar::chunk {"
            f"background-color: {chunk};"
            "border-radius: 5px;"
            "}"
        )
    if status_label is not None:
        status_label.setStyleSheet(f"color: {status}; font-size: {pt(9)}pt;")
    if percent_label is not None:
        percent_label.setStyleSheet(f"color: {text}; font-size: {pt(9)}pt;")


class HintBar(QFrame):
    """Always-visible hint/status strip with a colored left accent bar.

    The bar occupies a fixed-height slot at all times — in idle state it shows
    a neutral grey background with no text so the layout never shifts.

    States
    ------
    ``idle``    – light grey background, no text (default / reset)
    ``tip``     – soft blue background, contextual help text
    ``success`` – soft green background, e.g. "Observation saved"
    ``warning`` – soft red/orange background, e.g. "Could not connect"

    Usage::

        bar = HintBar(parent)
        bar.set_status("Observation saved", "success")
        bar.set_status()          # reset to idle
        bar.set_status("", "idle")  # same
    """

    IDLE = "idle"
    TIP = "tip"
    SUCCESS = "success"
    WARNING = "warning"

    # (background, left accent bar colour) — light mode defaults
    _STATE_COLORS: dict[str, tuple[str, str]] = {
        "idle":    ("#d8e5e1", "#a8b5b1"),   # surface_container_highest / outline_variant
        "tip":     ("#edf5f2", "#47674a"),   # surface_container_low / primary
        "info":    ("#edf5f2", "#47674a"),   # alias → tip
        "success": ("#c7ecc7", "#3b5a3e"),   # primary_container / primary_dim
        "warning": ("#fdecec", "#e74c3c"),
        "error":   ("#fdecec", "#e74c3c"),   # alias → warning
    }
    _STATE_COLORS_DARK: dict[str, tuple[str, str]] = {
        "idle":    ("#2a2a2c", "#555557"),
        "tip":     ("#162030", "#4a90d9"),
        "info":    ("#162030", "#4a90d9"),
        "success": ("#122418", "#2ecc71"),
        "warning": ("#2e1a1a", "#e74c3c"),
        "error":   ("#2e1a1a", "#e74c3c"),
    }

    _BAR_WIDTH = 4
    _HEIGHT = 34

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._wrap_mode = False
        self.setFixedHeight(self._HEIGHT)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.setFrameShape(QFrame.NoFrame)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(self._BAR_WIDTH + 6, 0, 6, 0)
        layout.setSpacing(0)

        self._label = QLabel("")
        self._label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self._label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self._label.setWordWrap(False)
        self._label.setStyleSheet(
            f"QLabel {{ background: transparent; border: none; color: #222222; font-size: {pt(9)}pt; }}"
            # colour is overwritten in _apply_style on each state change
        )
        font = self._label.font()
        font.setWeight(QFont.Weight.Medium)
        self._label.setFont(font)
        layout.addWidget(self._label)

        self._apply_style("idle")
        self.set_wrap_mode(True)

    # ------------------------------------------------------------------
    # Internal

    def _is_dark(self) -> bool:
        from PySide6.QtWidgets import QApplication
        app = QApplication.instance()
        return app.palette().window().color().lightness() < 128 if app else False

    def _apply_style(self, state: str) -> None:
        dark = self._is_dark()
        palette = self._STATE_COLORS_DARK if dark else self._STATE_COLORS
        bg, accent = palette.get(state, palette["tip"])
        text_color = "#e8e8e8" if dark else "#222222"
        self._label.setStyleSheet(
            f"QLabel {{ background: transparent; border: none; color: {text_color}; font-size: {pt(9)}pt; }}"
        )
        self.setStyleSheet(
            f"HintBar {{ background: {bg}; border-left: {self._BAR_WIDTH}px solid {accent}; }}"
        )

    def _update_height_for_wrap(self) -> None:
        if not self._wrap_mode:
            return
        layout = self.layout()
        if layout is None:
            return
        margins = layout.contentsMargins()
        available_width = max(
            80,
            self.width() - margins.left() - margins.right(),
        )
        label_height = self._label.heightForWidth(available_width)
        if label_height <= 0:
            label_height = self._label.sizeHint().height()
        target_height = max(self._HEIGHT, label_height + margins.top() + margins.bottom())
        self.setFixedHeight(target_height)

    # ------------------------------------------------------------------
    # Public API

    def set_status(self, message: str = "", state: str = "idle") -> None:
        """Set the displayed message and visual state.

        Calling with no arguments, or with an empty *message*, resets to
        idle state (grey strip, no text).
        """
        message = (message or "").strip()
        state = (state or "idle").strip().lower()
        if not message:
            state = "idle"
        self._label.setText(message)
        self._apply_style(state)
        self._update_height_for_wrap()

    def clear(self) -> None:
        """Reset to idle state with no message."""
        self.set_status()

    def set_wrap_mode(self, enabled: bool) -> None:
        """Enable multiline wrapping and auto-height based on available width."""
        self._wrap_mode = bool(enabled)
        self._label.setWordWrap(self._wrap_mode)
        if self._wrap_mode:
            self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
            self._update_height_for_wrap()
        else:
            self.setFixedHeight(self._HEIGHT)
            self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._update_height_for_wrap()


class HintStatusController(QObject):
    """Controls a persistent hint plus temporary status messages in a HintBar.

    Also accepts a plain ``QLabel`` for legacy call-sites, though a
    ``HintBar`` is strongly preferred.
    """

    def __init__(self, hint_bar: "HintBar | QLabel", parent: QObject | None = None) -> None:
        super().__init__(parent or hint_bar)
        if isinstance(hint_bar, HintBar):
            self._hint_bar: HintBar | None = hint_bar
            self._hint_label: QLabel = hint_bar._label
        else:
            # Legacy path: plain QLabel
            self._hint_bar = None
            self._hint_label = hint_bar

        self._hint_text = ""
        self._hint_tone = "info"
        self._status_tone = "info"
        self._status_timer = QTimer(self)
        self._status_timer.setSingleShot(True)
        self._status_timer.timeout.connect(self._restore_hint)
        # Small delay before resetting to idle on mouse-leave, so moving
        # between nearby buttons doesn't cause a grey flash.
        self._leave_timer = QTimer(self)
        self._leave_timer.setSingleShot(True)
        self._leave_timer.setInterval(120)
        self._leave_timer.timeout.connect(lambda: self.set_hint(""))
        self._apply_idle_style()

    # ------------------------------------------------------------------
    # Internal helpers

    @staticmethod
    def _tone_to_state(tone: str) -> str:
        t = (tone or "info").strip().lower()
        if t in ("error", "warning"):
            return "warning"
        if t in ("info", "tip"):
            return "tip"
        if t == "success":
            return "success"
        return "tip"

    def _set_label_text(self, text: str | None) -> None:
        self._hint_label.setText((text or "").strip())
        if self._hint_bar is not None:
            self._hint_bar._update_height_for_wrap()

    @staticmethod
    def _palette_is_dark() -> bool:
        return _palette_is_dark()

    def _apply_idle_style(self) -> None:
        self._set_label_text("")
        if self._hint_bar is not None:
            self._hint_bar._apply_style("idle")
        else:
            dark = self._palette_is_dark()
            bg, border = ("#2a2a2c", "#555557") if dark else ("#d8e5e1", "#a8b5b1")
            text = "#e8e8e8" if dark else "#293532"
            self._hint_label.setStyleSheet(
                f"QLabel {{ background: {bg}; border-left: 4px solid {border}; "
                f"padding-left: 6px; color: {text}; }}"
            )

    def _apply_active_style(self, tone: str = "info") -> None:
        state = self._tone_to_state(tone)
        if self._hint_bar is not None:
            self._hint_bar._apply_style(state)
        else:
            dark = self._palette_is_dark()
            if dark:
                colors: dict[str, tuple[str, str]] = {
                    "tip":     ("#162030", "#4a90d9"),
                    "success": ("#122418", "#2ecc71"),
                    "warning": ("#2e1a1a", "#e74c3c"),
                }
            else:
                colors = {
                    "tip":     ("#edf5f2", "#47674a"),   # surface_container_low / primary
                    "success": ("#c7ecc7", "#3b5a3e"),   # primary_container / primary_dim
                    "warning": ("#fdecec", "#e74c3c"),
                }
            bg, border = colors.get(state, colors["tip"])
            text = "#e8e8e8" if dark else "#222222"
            self._hint_label.setStyleSheet(
                f"QLabel {{ background: {bg}; border-left: 4px solid {border}; "
                f"padding-left: 6px; color: {text}; }}"
            )

    def _restore_hint(self) -> None:
        if self._hint_text:
            self._set_label_text(self._hint_text)
            self._apply_active_style(self._hint_tone)
        else:
            self._apply_idle_style()

    # ------------------------------------------------------------------
    # Public API

    def set_hint(self, text: str | None, tone: str = "info") -> None:
        """Set a persistent hint.  Pass empty / None to return to idle."""
        self._hint_text = (text or "").strip()
        self._hint_tone = (tone or "info").strip().lower()
        if not self._status_timer.isActive():
            if self._hint_text:
                self._set_label_text(self._hint_text)
                self._apply_active_style(self._hint_tone)
            else:
                self._apply_idle_style()

    def set_status(
        self,
        text: str | None,
        timeout_ms: int = 4000,
        tone: str = "info",
    ) -> None:
        """Show a temporary status message, auto-clearing after *timeout_ms*."""
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
        disabled_hint: str | None = None,
    ) -> None:
        """Connect widget hover enter/leave to the hint bar.

        *disabled_hint* is shown in the bar when the widget is hovered while
        disabled (instead of nothing).  Ignored when *allow_when_disabled* is
        True, since the normal hint is already shown in that case.
        """
        hint = (hint_text or "").strip()
        widget.setProperty("_hint_text", hint)
        widget.setProperty("_hint_tone", (tone or "info").strip().lower())
        widget.setProperty("_hint_allow_disabled", bool(allow_when_disabled))
        if disabled_hint is not None:
            widget.setProperty("_hint_disabled_text", (disabled_hint or "").strip())
        widget.setToolTip("")
        widget.installEventFilter(self)

    def eventFilter(self, watched: QObject, event: QEvent) -> bool:
        if watched is None or not _qt_is_valid(watched):
            return False
        hint_text = watched.property("_hint_text") if hasattr(watched, "property") else None
        hint_tone = watched.property("_hint_tone") if hasattr(watched, "property") else None
        allow_when_disabled = (
            watched.property("_hint_allow_disabled") if hasattr(watched, "property") else False
        )
        if (
            event.type() == QEvent.ToolTip
            and hasattr(watched, "property")
            and watched.property("_hint_text") is not None
        ):
            # Registered widgets use the bottom hint bar instead of native cursor tooltips.
            QToolTip.hideText()
            return True
        tone = hint_tone if isinstance(hint_tone, str) and hint_tone else "info"
        allow_disabled = bool(allow_when_disabled)
        is_enabled = True
        if isinstance(watched, QWidget):
            is_enabled = watched.isEnabled()
        if isinstance(hint_text, str) and hint_text:
            if event.type() == QEvent.Enter:
                self._leave_timer.stop()
                if is_enabled or allow_disabled:
                    self.set_hint(hint_text, tone=tone)
                else:
                    dis = watched.property("_hint_disabled_text") if hasattr(watched, "property") else None
                    self.set_hint(dis if isinstance(dis, str) and dis else "", tone="tip")
            elif event.type() == QEvent.Leave:
                self._leave_timer.start()
            elif event.type() == QEvent.EnabledChange and not is_enabled and not allow_disabled:
                self._leave_timer.stop()
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
        # No color override — inherits from global stylesheet / palette.
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
