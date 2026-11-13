"""Reusable hint/status helpers for dialogs and windows."""
from __future__ import annotations

import traceback
from dataclasses import dataclass
from typing import Callable

from PySide6.QtCore import QEvent, QObject, QTimer, Qt, Signal
from PySide6.QtGui import QColor, QFont, QPainter, QPen
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QPlainTextEdit,
    QProgressBar,
    QSizePolicy,
    QStackedWidget,
    QToolButton,
    QToolTip,
    QVBoxLayout,
    QWidget,
)

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

    A notice (stage 10b) adds controls at the right: Details (the text opens
    below the message, in the bar), Undo, and ✕ to dismiss. The bar shows
    them; :class:`HintStatusController` decides when.

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

    # The notice's controls.
    undoRequested = Signal()
    dismissRequested = Signal()
    detailsToggled = Signal(bool)
    # The bar's height changed (a wrapped message, Details opened).
    heightChanged = Signal(int)
    # The notice's controls came or went (the height may stay the same).
    noticeControlsChanged = Signal(bool)

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
    _DETAILS_MAX_HEIGHT = 140
    _DETAILS_GAP = 6

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._wrap_mode = False
        self._state = "idle"
        self.setFixedHeight(self._HEIGHT)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.setFrameShape(QFrame.NoFrame)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(self._BAR_WIDTH + 6, 0, 6, 0)
        layout.setSpacing(0)

        self._row = QWidget(self)
        self._row.setObjectName("hintBarRow")
        row = QHBoxLayout(self._row)
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(6)

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
        row.addWidget(self._label, 1)

        self._details_btn = QToolButton(self._row)
        self._details_btn.setObjectName("hintBarDetails")
        self._details_btn.setText(self.tr("Details"))
        self._details_btn.setCheckable(True)
        self._details_btn.toggled.connect(self._on_details_toggled)
        self._undo_btn = QToolButton(self._row)
        self._undo_btn.setObjectName("hintBarUndo")
        self._undo_btn.setText(self.tr("Undo"))
        self._undo_btn.clicked.connect(self.undoRequested.emit)
        self._dismiss_btn = QToolButton(self._row)
        self._dismiss_btn.setObjectName("hintBarDismiss")
        self._dismiss_btn.setText("✕")
        self._dismiss_btn.setToolTip(self.tr("Dismiss"))
        self._dismiss_btn.setAccessibleName(self.tr("Dismiss"))
        self._dismiss_btn.clicked.connect(self.dismissRequested.emit)
        for button in self._notice_buttons():
            button.setCursor(Qt.PointingHandCursor)
            button.setVisible(False)
            row.addWidget(button, 0, Qt.AlignVCenter)
        layout.addWidget(self._row)

        self._details_view = QPlainTextEdit(self)
        self._details_view.setObjectName("hintBarDetailsText")
        self._details_view.setReadOnly(True)
        self._details_view.setFrameShape(QFrame.NoFrame)
        self._details_view.setVisible(False)
        layout.addWidget(self._details_view)

        self._apply_style("idle")
        self.set_wrap_mode(True)

    # ------------------------------------------------------------------
    # Internal

    def _notice_buttons(self) -> tuple[QToolButton, ...]:
        return (self._details_btn, self._undo_btn, self._dismiss_btn)

    def _is_dark(self) -> bool:
        from PySide6.QtWidgets import QApplication
        app = QApplication.instance()
        return app.palette().window().color().lightness() < 128 if app else False

    def _apply_style(self, state: str) -> None:
        self._state = state
        dark = self._is_dark()
        palette = self._STATE_COLORS_DARK if dark else self._STATE_COLORS
        bg, accent = palette.get(state, palette["tip"])
        text_color = "#e8e8e8" if dark else "#222222"
        self._label.setStyleSheet(
            f"QLabel {{ background: transparent; border: none; color: {text_color}; font-size: {pt(9)}pt; }}"
        )
        details_bg = "#1e1e20" if dark else "#ffffff"
        self.setStyleSheet(
            f"HintBar {{ background: {bg}; border-left: {self._BAR_WIDTH}px solid {accent}; }}"
            f"QToolButton#hintBarDetails, QToolButton#hintBarUndo, QToolButton#hintBarDismiss {{"
            f" background: transparent; color: {text_color}; border: 1px solid {accent};"
            f" border-radius: 4px; padding: 1px 8px; font-size: {pt(9)}pt; }}"
            f"QToolButton#hintBarDismiss {{ border-color: transparent; padding: 1px 4px; }}"
            f"QToolButton#hintBarDetails:hover, QToolButton#hintBarUndo:hover,"
            f" QToolButton#hintBarDismiss:hover {{ background: {accent}; color: #ffffff; }}"
            f"QToolButton#hintBarDetails:checked {{ background: {accent}; color: #ffffff; }}"
            f"QPlainTextEdit#hintBarDetailsText {{ background: {details_bg}; color: {text_color};"
            f" border: 1px solid {accent}; border-radius: 4px; font-size: {pt(9)}pt; }}"
        )

    def _on_details_toggled(self, open_: bool) -> None:
        self._details_view.setVisible(bool(open_))
        self._update_height_for_wrap()
        self.detailsToggled.emit(bool(open_))

    def _visible_buttons_width(self) -> int:
        row = self._row.layout()
        width = 0
        for button in self._notice_buttons():
            if not button.isHidden():
                width += button.sizeHint().width() + row.spacing()
        return width

    def _details_height(self) -> int:
        if self._details_view.isHidden():
            return 0
        doc = self._details_view.document()
        lines = max(1, doc.blockCount())
        line_height = self._details_view.fontMetrics().lineSpacing()
        wanted = lines * line_height + 2 * int(doc.documentMargin()) + 4
        return min(self._DETAILS_MAX_HEIGHT, max(2 * line_height, wanted))

    def _update_height_for_wrap(self) -> None:
        layout = self.layout()
        if layout is None:
            return
        margins = layout.contentsMargins()
        row_height = self._HEIGHT
        if self._wrap_mode:
            available_width = max(
                80,
                self.width() - margins.left() - margins.right() - self._visible_buttons_width(),
            )
            label_height = self._label.heightForWidth(available_width)
            if label_height <= 0:
                label_height = self._label.sizeHint().height()
            row_height = max(self._HEIGHT, label_height + margins.top() + margins.bottom())
        details_height = self._details_height()
        if details_height:
            self._row.setFixedHeight(row_height)
            self._details_view.setFixedHeight(details_height)
        else:
            # Unconstrained: the bar's size hint stays the label's, as hosts
            # that size a stack from it expect.
            self._row.setMinimumHeight(0)
            self._row.setMaximumHeight(16777215)
        target_height = row_height + (details_height + self._DETAILS_GAP if details_height else 0)
        previous = self.height()
        self.setFixedHeight(target_height)
        # Re-run the bar's own layout now: otherwise the label keeps the
        # taller geometry it got while the bar was narrow (wrapped) and the
        # text sits at the bottom of the bar instead of centred.
        layout.invalidate()
        layout.activate()
        if target_height != previous:
            self.heightChanged.emit(target_height)

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

    def set_notice_controls(
        self,
        *,
        details: str | None = None,
        undo_text: str | None = None,
        dismissible: bool = False,
        details_open: bool = False,
    ) -> None:
        """Show the notice's controls; with no arguments, hide them all.

        ``details``: Details opens this text below the message;
        ``details_open`` shows it open. ``undo_text``: the Undo button's
        label (empty for none).
        """
        was_shown = self.notice_controls_visible()
        details = (details or "").strip()
        if self._details_view.toPlainText() != details:
            self._details_view.setPlainText(details)
        open_ = bool(details) and bool(details_open)
        self._details_btn.blockSignals(True)
        self._details_btn.setChecked(open_)
        self._details_btn.blockSignals(False)
        self._details_view.setVisible(open_)
        self._details_btn.setVisible(bool(details))
        undo_text = (undo_text or "").strip()
        self._undo_btn.setText(undo_text or self.tr("Undo"))
        self._undo_btn.setVisible(bool(undo_text))
        self._dismiss_btn.setVisible(bool(dismissible))
        self._update_height_for_wrap()
        shown = self.notice_controls_visible()
        if shown != was_shown:
            self.noticeControlsChanged.emit(shown)

    def notice_controls_visible(self) -> bool:
        return any(not button.isHidden() for button in self._notice_buttons())

    def details_open(self) -> bool:
        return not self._details_view.isHidden()

    def set_wrap_mode(self, enabled: bool) -> None:
        """Enable multiline wrapping and auto-height based on available width."""
        self._wrap_mode = bool(enabled)
        self._label.setWordWrap(self._wrap_mode)
        if self._wrap_mode:
            self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        else:
            self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self._update_height_for_wrap()

    def changeEvent(self, event) -> None:
        super().changeEvent(event)
        if event.type() == QEvent.LanguageChange:
            self._details_btn.setText(self.tr("Details"))
            self._dismiss_btn.setToolTip(self.tr("Dismiss"))
            self._dismiss_btn.setAccessibleName(self.tr("Dismiss"))

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._update_height_for_wrap()


@dataclass
class _Notice:
    text: str
    tone: str
    details: str
    undo: Callable[[], None] | None
    undo_text: str
    timeout_ms: int
    details_open: bool = False


class HintStatusController(QObject):
    """Controls a persistent hint plus temporary status messages in a HintBar.

    Also accepts a plain ``QLabel`` for legacy call-sites, though a
    ``HintBar`` is strongly preferred.

    Notices (stage 10b, :meth:`show_notice`) are the screen's non-blocking
    messages in place of a message box: shown at once, with optional Details
    and Undo, and ✕ to dismiss. Info and success clear themselves after a
    while; errors stay until dismissed or replaced. A hover hint covers a
    notice while the pointer is on a control, and the notice comes back when
    it leaves.
    """

    # How long info and success notices stay, in milliseconds.
    NOTICE_TIMEOUT_MS = 6000
    # How long a notice with Undo stays (time to change one's mind).
    UNDO_NOTICE_TIMEOUT_MS = 10000

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
        # Shown whenever there is no contextual hint (instead of a blank
        # idle bar): a dialog-level resting message, e.g. "2 sources
        # selected". Empty by default, so existing bars are unchanged.
        self._baseline_text = ""
        self._baseline_tone = "info"
        self._status_timer = QTimer(self)
        self._status_timer.setSingleShot(True)
        self._status_timer.timeout.connect(self._restore_hint)
        # Small delay before resetting to idle on mouse-leave, so moving
        # between nearby buttons doesn't cause a grey flash.
        self._leave_timer = QTimer(self)
        self._leave_timer.setSingleShot(True)
        self._leave_timer.setInterval(120)
        self._leave_timer.timeout.connect(lambda: self.set_hint(""))
        self._notice: _Notice | None = None
        # The notice shows over a hint the pointer was already on when it
        # came (the control just clicked); the next hint covers it again.
        self._notice_on_top = False
        self._notice_timer = QTimer(self)
        self._notice_timer.setSingleShot(True)
        self._notice_timer.timeout.connect(self.dismiss_notice)
        # The bar can go first (a page deleted with its form): nothing may
        # write to it afterwards.
        self._hint_label.destroyed.connect(self._on_bar_destroyed)
        if self._hint_bar is not None:
            self._hint_bar.undoRequested.connect(self._on_notice_undo)
            self._hint_bar.dismissRequested.connect(self.dismiss_notice)
            self._hint_bar.detailsToggled.connect(self._on_notice_details_toggled)
        self._apply_idle_style()

    # ------------------------------------------------------------------
    # Internal helpers

    def _on_bar_destroyed(self, *_args) -> None:
        self._bar_gone = True
        self._notice = None
        for timer in (self._status_timer, self._leave_timer, self._notice_timer):
            timer.stop()

    def _bar_alive(self) -> bool:
        return not getattr(self, "_bar_gone", False) and _qt_is_valid(self._hint_label)

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
            if self._hint_bar.notice_controls_visible():
                self._hint_bar.set_notice_controls()  # not the notice: its controls go
            else:
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

    def _show_current_notice(self) -> None:
        notice = self._notice
        self._hint_label.setText(notice.text)
        self._apply_active_style(notice.tone)
        if self._hint_bar is not None:
            self._hint_bar.set_notice_controls(
                details=notice.details,
                undo_text=notice.undo_text if notice.undo is not None else "",
                dismissible=True,
                details_open=notice.details_open,
            )

    def _restore_hint(self) -> None:
        if not self._bar_alive():
            return
        if self._notice is not None and (self._notice_on_top or not self._hint_text):
            self._show_current_notice()
        elif self._hint_text:
            self._set_label_text(self._hint_text)
            self._apply_active_style(self._hint_tone)
        elif self._baseline_text:
            self._apply_idle_style()
            self._set_label_text(self._baseline_text)
        else:
            self._apply_idle_style()

    # ------------------------------------------------------------------
    # Public API

    def set_hint(self, text: str | None, tone: str = "info") -> None:
        """Set a persistent hint.  Pass empty / None to return to idle."""
        self._hint_text = (text or "").strip()
        self._hint_tone = (tone or "info").strip().lower()
        if self._hint_text:
            self._notice_on_top = False
        if not self._status_timer.isActive():
            self._restore_hint()

    def show_notice(
        self,
        text: str | None,
        tone: str = "info",
        *,
        details: str | None = None,
        undo: Callable[[], None] | None = None,
        undo_text: str | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """Show a non-blocking notice in this bar, replacing any earlier one.

        ``tone``: ``info``, ``success``, or ``error``/``warning`` (these stay
        until dismissed). ``details``: longer text behind Details (an error's
        message, a summary). ``undo``: called by Undo, after which the notice
        goes. ``timeout_ms``: 0 keeps the notice until dismissed; by default
        info and success go after :attr:`NOTICE_TIMEOUT_MS` (with Undo,
        :attr:`UNDO_NOTICE_TIMEOUT_MS`). Opening Details keeps it.
        """
        message = (text or "").strip()
        if not message:
            self.dismiss_notice()
            return
        tone = (tone or "info").strip().lower()
        if timeout_ms is None:
            if tone in ("error", "warning"):
                timeout_ms = 0
            elif undo is not None:
                timeout_ms = self.UNDO_NOTICE_TIMEOUT_MS
            else:
                timeout_ms = self.NOTICE_TIMEOUT_MS
        self._notice = _Notice(
            text=message,
            tone=tone,
            details=(details or "").strip(),
            undo=undo,
            undo_text=(undo_text or "").strip() or self.tr("Undo"),
            timeout_ms=max(0, int(timeout_ms)),
        )
        self._notice_on_top = True
        self._status_timer.stop()  # the newest message shows
        if self._notice.timeout_ms:
            self._notice_timer.start(self._notice.timeout_ms)
        else:
            self._notice_timer.stop()
        self._restore_hint()

    def dismiss_notice(self) -> None:
        """Remove the notice (✕, its timeout, or Undo)."""
        self._notice_timer.stop()
        if self._notice is None:
            return
        self._notice = None
        self._notice_on_top = False
        if not self._status_timer.isActive():
            self._restore_hint()

    def has_notice(self) -> bool:
        return self._notice is not None

    def notice_text(self) -> str:
        return self._notice.text if self._notice is not None else ""

    def _on_notice_details_toggled(self, open_: bool) -> None:
        if self._notice is not None:
            self._notice.details_open = bool(open_)  # kept when the notice shows again
        if open_:
            self._notice_timer.stop()  # being read: it stays until dismissed

    def _on_notice_undo(self) -> None:
        notice = self._notice
        if notice is None or notice.undo is None:
            return
        self.dismiss_notice()
        try:
            notice.undo()
        except Exception as exc:  # the undo failed: say so, with the error
            self.show_notice(
                self.tr("Could not undo."),
                "error",
                details=str(exc) or traceback.format_exc(limit=1),
            )

    def set_baseline(self, text: str | None, tone: str = "info") -> None:
        """Resting text shown (in the idle style) when no contextual hint
        is active. Empty clears it back to a blank idle bar."""
        self._baseline_text = (text or "").strip()
        self._baseline_tone = (tone or "info").strip().lower()
        if not self._status_timer.isActive():
            self._restore_hint()

    def current_text(self) -> str:
        return self._hint_label.text()

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
        # WhatsThisCursor falls back to a Qt-embedded bitmap on macOS, which
        # crashes in Qt 6.11 / macOS 26 CoreGraphics via QImage::toCGImage.
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


class HintProgressStack(QWidget):
    """Bottom-of-tab hint area that combines a HintBar with a progress
    panel (bar + label + optional cancel button). Only one is visible at a
    time via an internal QStackedWidget, so showing progress never causes
    the surrounding layout to jump vertically.

    Use `hint_bar` to wire a HintStatusController. Call `show_progress()`
    to switch to the progress panel and `hide_progress()` to return to
    the hint. This mirrors the pattern Live Lab was already using inline;
    factored out here so Analysis / Prepare Images / Ingestion can share
    the same widget."""

    cancelRequested = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self.hint_bar = HintBar(self)
        self.hint_bar.set_wrap_mode(True)

        self._progress_widget = QWidget(self)
        prog_layout = QHBoxLayout(self._progress_widget)
        prog_layout.setContentsMargins(6, 4, 6, 4)
        prog_layout.setSpacing(6)

        prog_column = QVBoxLayout()
        prog_column.setContentsMargins(0, 0, 0, 0)
        prog_column.setSpacing(2)

        self._progress_label = QLabel("")
        self._progress_label.setWordWrap(False)
        self._progress_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self._progress_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        self._progress_bar = QProgressBar(self._progress_widget)
        self._progress_bar.setRange(0, 0)  # indeterminate by default
        self._progress_bar.setTextVisible(False)
        self._progress_bar.setFixedHeight(5)
        self._progress_bar.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        prog_column.addWidget(self._progress_label)
        prog_column.addWidget(self._progress_bar)
        prog_layout.addLayout(prog_column, 1)

        self._cancel_btn = QToolButton(self._progress_widget)
        self._cancel_btn.setText("✕")
        self._cancel_btn.setCursor(Qt.PointingHandCursor)
        self._cancel_btn.setFixedSize(22, 22)
        self._cancel_btn.setVisible(False)
        self._cancel_btn.setStyleSheet(
            "QToolButton {"
            " border: 1px solid #b91c1c;"
            " border-radius: 4px;"
            " color: #ffffff;"
            " background-color: #dc2626;"
            " font-weight: 700;"
            " padding: 0px;"
            "}"
            "QToolButton:hover { background-color: #b91c1c; }"
            "QToolButton:pressed { background-color: #991b1b; }"
        )
        self._cancel_btn.clicked.connect(self.cancelRequested.emit)
        prog_layout.addWidget(self._cancel_btn, 0, Qt.AlignRight | Qt.AlignVCenter)

        style_progress_widgets(self._progress_bar, self._progress_label)

        self._stack = QStackedWidget(self)
        self._stack.addWidget(self.hint_bar)
        self._stack.addWidget(self._progress_widget)
        stable_height = max(
            int(self.hint_bar.sizeHint().height() or 0),
            int(self._progress_widget.sizeHint().height() or 0),
            32,
        )
        self._stable_height = stable_height
        self._stack.setFixedHeight(stable_height)
        self._stack.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        layout.addWidget(self._stack)
        # A notice may need more room (a long message, Details open); hints
        # and progress keep the stable height, so hovering never shifts it.
        self.hint_bar.heightChanged.connect(self._fit_hint_height)
        self.hint_bar.noticeControlsChanged.connect(self._fit_hint_height)

        self._cancel_callback: Callable[[], None] | None = None
        self.cancelRequested.connect(self._on_cancel_clicked)

    def _fit_hint_height(self, *_args) -> None:
        height = self._stable_height
        if self._stack.currentWidget() is self.hint_bar and self.hint_bar.notice_controls_visible():
            height = max(height, self.hint_bar.height())
        self._stack.setFixedHeight(height)

    def _on_cancel_clicked(self) -> None:
        if self._cancel_callback is not None:
            try:
                self._cancel_callback()
            except Exception:
                pass

    def show_progress(
        self,
        text: str = "",
        *,
        value: int | None = None,
        maximum: int | None = None,
        cancel_callback: Callable[[], None] | None = None,
    ) -> None:
        """Switch to the progress panel. Pass value/maximum for a
        determinate bar or leave both None for indeterminate spinner. Set
        cancel_callback to enable the cancel button."""
        self._progress_label.setText(str(text or ""))
        if value is not None and maximum is not None and int(maximum) > 0:
            self._progress_bar.setRange(0, int(maximum))
            self._progress_bar.setValue(int(max(0, min(int(value), int(maximum)))))
        else:
            self._progress_bar.setRange(0, 0)
        self._cancel_callback = cancel_callback
        self._cancel_btn.setVisible(cancel_callback is not None)
        self._cancel_btn.setEnabled(cancel_callback is not None)
        self._stack.setCurrentWidget(self._progress_widget)
        self._fit_hint_height()

    def update_progress(self, text: str | None = None, *, value: int | None = None, maximum: int | None = None) -> None:
        if text is not None:
            self._progress_label.setText(str(text))
        if maximum is not None:
            self._progress_bar.setRange(0, int(maximum))
        if value is not None and int(self._progress_bar.maximum()) > 0:
            self._progress_bar.setValue(int(max(0, min(int(value), int(self._progress_bar.maximum())))))

    def hide_progress(self) -> None:
        """Switch back to the hint bar."""
        self._cancel_callback = None
        self._cancel_btn.setVisible(False)
        self._cancel_btn.setEnabled(False)
        self._stack.setCurrentWidget(self.hint_bar)
        self._fit_hint_height()
