"""Unified taxon picker: one search field and a compact identification card.

Typing searches names through :meth:`TaxonLookupService.search_taxa`
(:mod:`database.taxon_search`) and shows a two-line result popup. A row is a
name. When a COL and a NorTaxa concept share that name, the row carries one
chip per source, and the chip that a click or Enter will pick is drawn
selected before anything is committed. Picking shows the identification card,
where the source can still be switched.

The widget only *emits* selections (:attr:`TaxonPicker.concept_picked`,
:attr:`TaxonPicker.free_text_committed`, :attr:`TaxonPicker.cleared`). It
never writes an observation or an identity; the host decides what a pick
means. Artsorakel suggestions are offered as chips: a provider identifier the
host resolved to one concept only sets which source is the default, and a
suggestion is never applied without a click.
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field, replace
from html import escape
from typing import Callable

from PySide6.QtCore import QEvent, QModelIndex, QObject, QPoint, QRect, QRectF, QSize, Qt, QTimer, Signal
from PySide6.QtGui import (
    QColor,
    QFont,
    QFontMetrics,
    QGuiApplication,
    QPainter,
    QPalette,
    QStandardItem,
    QStandardItemModel,
    QTextDocument,
)
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListView,
    QPushButton,
    QSizePolicy,
    QStyle,
    QStyledItemDelegate,
    QToolButton,
    QToolTip,
    QVBoxLayout,
    QWidget,
)

from shiboken6 import isValid

from database.taxon_search import (
    KIND_SYNONYM,
    TAXON_SEARCH_MIN_CHARS,
    TaxonSearchConcept,
    TaxonSearchRow,
)

SOURCE_NOTE_SETTING = "taxon_picker_source_note_dismissed"

ROLE_ENTRY = Qt.UserRole + 41

_ACCENT = "#4d7c7a"
_ROW_HEIGHT = 46
_ACTION_HEIGHT = 30
_POPUP_MAX_ROWS = 9


@dataclass(frozen=True)
class AiSuggestion:
    """One Artsorakel (or other provider) candidate offered as a chip.

    ``taxon_id`` is set only when the host resolved the provider identifier
    to exactly one installed concept (``resolve_provider_concept``). It
    decides which source a pick defaults to; it is never applied on its own.
    """

    label: str
    scientific_name: str
    score: float | None = None
    taxon_id: int | None = None
    # The host's own record of the candidate (a provider prediction), handed
    # back to the host's suggestion handler.
    key: object = field(default=None, compare=False)


@dataclass(frozen=True)
class _Entry:
    kind: str  # "row" | "more" | "free"
    row: TaxonSearchRow | None = None
    choice: int = 0
    text: str = ""


def _accent(alpha: int = 255) -> QColor:
    color = QColor(_ACCENT)
    color.setAlpha(alpha)
    return color


def _dim(palette: QPalette) -> QColor:
    return palette.color(QPalette.PlaceholderText)


def _italic_name(name: str) -> str:
    """Italicise the Latin words of a name, not its rank markers."""
    words = []
    for word in name.split():
        if word in ("var.", "subsp.", "ssp.", "f.", "coll.", "agg.", "sp."):
            words.append(escape(word))
        else:
            words.append(f"<i>{escape(word)}</i>")
    return " ".join(words)


def _display_name(row_name: str, rank: str) -> str:
    """HTML for a row or card name; a genus identification reads ``Russula sp.``."""
    if rank == "genus":
        return _italic_name(row_name) + " sp."
    return _italic_name(row_name)


class _ResultDelegate(QStyledItemDelegate):
    """Two-line rows: the scientific name first, the vernacular after it;
    below, the synonym target, family per source and evidence notes. Source
    chips and a rank pill sit at the right edge."""

    def __init__(self, picker: "TaxonPicker") -> None:
        super().__init__(picker)
        self._picker = picker

    # -- geometry shared by paint, clicks and tooltips -----------------------

    def _fonts(self, option) -> tuple[QFont, QFont, QFont]:
        base = QFont(option.font)
        name = QFont(base)
        name.setBold(True)
        small = QFont(base)
        small.setPointSizeF(max(7.0, base.pointSizeF() - 1.5))
        return base, name, small

    def chip_rects(self, option, entry: _Entry) -> tuple[QRectF | None, list[QRectF]]:
        """``(rank pill, source chips)`` rectangles for a result row."""
        if entry.kind != "row" or entry.row is None:
            return None, []
        _base, _name, small = self._fonts(option)
        metrics = QFontMetrics(small)
        rect = option.rect
        top = rect.top() + 7
        pill_text = self._picker.rank_label(entry.row)
        pill_width = metrics.horizontalAdvance(pill_text) + 14
        pill = QRectF(rect.right() - pill_width - 10, top, pill_width, metrics.height() + 4)
        chips: list[QRectF] = []
        if entry.row.has_source_choice:
            right = pill.left() - 8
            for concept in reversed(entry.row.concepts):
                width = metrics.horizontalAdvance(concept.source_name) + 18
                chip = QRectF(right - width, top - 1, width, metrics.height() + 6)
                chips.insert(0, chip)
                right = chip.left() - 4
        return pill, chips

    def sizeHint(self, option, index):
        entry = index.data(ROLE_ENTRY)
        width = option.rect.width() or 400
        if entry is not None and entry.kind != "row":
            return QSize(width, _ACTION_HEIGHT)
        return QSize(width, _ROW_HEIGHT)

    # -- painting -------------------------------------------------------------

    def paint(self, painter: QPainter, option, index):
        entry = index.data(ROLE_ENTRY)
        if entry is None:
            return
        painter.save()
        palette = option.palette
        rect = option.rect
        painter.setRenderHint(QPainter.Antialiasing)
        selected = bool(option.state & QStyle.State_Selected)
        painter.fillRect(rect, _accent(46) if selected else palette.color(QPalette.Base))
        base, name_font, small = self._fonts(option)
        if entry.kind != "row":
            painter.setPen(palette.color(QPalette.Mid))
            painter.drawLine(rect.topLeft(), rect.topRight())
            painter.setFont(base)
            painter.setPen(_accent() if entry.kind == "more" else palette.color(QPalette.Text))
            painter.drawText(rect.adjusted(12, 0, -10, 0), Qt.AlignVCenter | Qt.AlignLeft, entry.text)
            painter.restore()
            return

        row = entry.row
        pill, chips = self.chip_rects(option, entry)
        # Rank pill.
        painter.setFont(small)
        painter.setPen(Qt.NoPen)
        tint = QColor(palette.color(QPalette.Text))
        tint.setAlpha(28)
        painter.setBrush(tint)
        painter.drawRoundedRect(pill, pill.height() / 2, pill.height() / 2)
        painter.setPen(palette.color(QPalette.Text))
        painter.drawText(pill, Qt.AlignCenter, self._picker.rank_label(row))
        # Source chips: the one Enter or a row click picks is filled.
        for i, (chip, concept) in enumerate(zip(chips, row.concepts)):
            radius = chip.height() / 2
            if i == entry.choice:
                painter.setPen(Qt.NoPen)
                painter.setBrush(_accent())
                painter.drawRoundedRect(chip, radius, radius)
                painter.setPen(QColor("white"))
            else:
                painter.setPen(_dim(palette))
                painter.setBrush(palette.color(QPalette.Base))
                painter.drawRoundedRect(chip.adjusted(0.5, 0.5, -0.5, -0.5), radius, radius)
                painter.setPen(palette.color(QPalette.Text))
            painter.drawText(chip, Qt.AlignCenter, concept.source_name)

        right_edge = (chips[0].left() if chips else pill.left()) - 8
        text_left = rect.left() + 12
        width = max(40, int(right_edge - text_left))
        detail_width = int(rect.right() - 10 - text_left)
        # The highlighted row with a source choice says how to switch it.
        hint = self._picker.source_hint() if selected and chips else ""
        if hint:
            painter.setFont(small)
            painter.setPen(_dim(palette))
            hint_width = QFontMetrics(small).horizontalAdvance(hint) + 2
            hint_rect = QRectF(pill.right() - hint_width, rect.top() + 25, hint_width, 18)
            painter.drawText(hint_rect, Qt.AlignRight | Qt.AlignVCenter, hint)
            detail_width = int(hint_rect.left() - 10 - text_left)
        line1 = self._picker.row_title_html(row, palette)
        line2 = self._picker.row_detail_html(row, entry.choice, palette)
        self._draw_html(painter, line1, base, QRectF(text_left - 4, rect.top() + 3, width, 22))
        self._draw_html(painter, line2, small, QRectF(text_left - 4, rect.top() + 23, max(40, detail_width), 20))
        painter.restore()

    @staticmethod
    def _draw_html(painter: QPainter, html: str, font: QFont, box: QRectF) -> None:
        doc = QTextDocument()
        doc.setDefaultFont(font)
        doc.setDocumentMargin(2)
        doc.setTextWidth(-1)
        doc.setHtml(f"<span style='white-space:pre'>{html}</span>")
        painter.save()
        painter.translate(box.topLeft())
        clip = QRectF(0, 0, box.width(), box.height())
        painter.setClipRect(clip)
        doc.drawContents(painter, clip)
        painter.restore()

    # -- interaction ----------------------------------------------------------

    def editorEvent(self, event, model, option, index):
        if event.type() == QEvent.MouseButtonPress:
            self._picker._chip_click_pending = False
        if event.type() == QEvent.MouseButtonRelease and event.button() == Qt.LeftButton:
            entry = index.data(ROLE_ENTRY)
            _pill, chips = self.chip_rects(option, entry) if entry is not None else (None, [])
            for i, chip in enumerate(chips):
                if chip.contains(event.position()):
                    # QListView still emits clicked() for this release; the
                    # picker must not then commit the row's default as well.
                    self._picker._chip_click_pending = True
                    self._picker._activate_index(index, choice=i)
                    return True
        return super().editorEvent(event, model, option, index)

    def helpEvent(self, event, view, option, index):
        entry = index.data(ROLE_ENTRY)
        if entry is not None and entry.kind == "row":
            _pill, chips = self.chip_rects(option, entry)
            for chip, concept in zip(chips, entry.row.concepts):
                if chip.contains(event.pos()):
                    QToolTip.showText(event.globalPos(), self._picker.concept_tooltip(concept), view)
                    return True
        return super().helpEvent(event, view, option, index)


def _floating_popup_supported() -> bool:
    """The result list floats as its own window where the platform lets an
    application place windows on the screen. Wayland does not, and the
    offscreen and minimal platforms have no real screens, so they keep the
    overlay. ``SPORELY_TAXON_POPUP=overlay`` forces the overlay anywhere."""
    if os.environ.get("SPORELY_TAXON_POPUP", "").strip().lower() == "overlay":
        return False
    if QGuiApplication.instance() is None:
        return False
    platform = QGuiApplication.platformName().lower()
    return bool(platform) and not platform.startswith(("wayland", "offscreen", "minimal"))


class _OutsideClickWatcher(QObject):
    """Application-wide filter, installed only while the list is open: a press
    outside the list and the search field closes it. Kept apart from the
    picker's own filter so the field's keys are never handled twice."""

    def __init__(self, picker: "TaxonPicker") -> None:
        super().__init__(picker)
        self._picker = picker

    def eventFilter(self, obj, event):
        if event.type() == QEvent.MouseButtonPress and isinstance(obj, QWidget):
            self._picker._on_press_anywhere(obj)
        return False


class _ResultPopup(QFrame):
    """The result list. It floats as a frameless tool window that never takes
    focus or activation, so it can reach past the dialog's edges while typing,
    arrows and Enter stay in the search field; clicks on rows and chips still
    land. Where windows cannot be placed it is an overlay inside the picker's
    window instead. Nothing in it accepts focus."""

    FLOATING_FLAGS = Qt.Tool | Qt.FramelessWindowHint | Qt.WindowDoesNotAcceptFocus

    def __init__(self, picker: "TaxonPicker") -> None:
        super().__init__(picker)
        self.setVisible(False)
        self.setAutoFillBackground(True)
        self.setObjectName("taxonPickerPopup")
        self.setFrameShape(QFrame.StyledPanel)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(1, 1, 1, 1)
        layout.setSpacing(0)

        self.note = QFrame()
        self.note.setObjectName("taxonPickerSourceNote")
        note_layout = QVBoxLayout(self.note)
        note_layout.setContentsMargins(10, 7, 10, 7)
        note_layout.setSpacing(4)
        self.note_title = QLabel()
        title_font = self.note_title.font()
        title_font.setBold(True)
        self.note_title.setFont(title_font)
        self.note_text = QLabel()
        self.note_text.setWordWrap(True)
        self.note_dismiss = QPushButton()
        self.note_dismiss.setFocusPolicy(Qt.NoFocus)
        self.note_dismiss.setCursor(Qt.PointingHandCursor)
        self.note_dismiss.setObjectName("taxonPickerNoteDismiss")
        note_row = QHBoxLayout()
        note_row.addWidget(self.note_text, 1)
        note_row.addWidget(self.note_dismiss, 0, Qt.AlignBottom)
        note_layout.addWidget(self.note_title)
        note_layout.addLayout(note_row)
        self.note.setVisible(False)
        layout.addWidget(self.note)

        self.view = QListView()
        self.view.setObjectName("taxonPickerResults")
        self.view.setFocusPolicy(Qt.NoFocus)
        self.view.setFrameShape(QFrame.NoFrame)
        self.view.setSelectionMode(QAbstractItemView.SingleSelection)
        self.view.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.view.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.view.setUniformItemSizes(False)
        self.view.setMouseTracking(True)
        # Two-line rows of mixed heights: scroll by pixel so trackpads glide.
        self.view.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.view.verticalScrollBar().setSingleStep(_ROW_HEIGHT // 2)
        layout.addWidget(self.view)
        palette = self.palette()
        border = palette.color(QPalette.Mid).name()
        self.setStyleSheet(
            f"#taxonPickerPopup {{ border: 1px solid {border}; background: palette(base); }}"
            f"#taxonPickerSourceNote {{ background: {_accent(30).name(QColor.HexArgb)};"
            f" border-bottom: 1px solid {_accent(90).name(QColor.HexArgb)}; }}"
        )

    def attach(self, host: QWidget, floating: bool) -> None:
        """Owned by ``host``: as a floating window above it, or as an overlay
        inside it. Reparenting only happens when the host or mode changes."""
        if self.parentWidget() is host and self.isWindow() == floating:
            return
        if floating:
            self.setParent(host, self.FLOATING_FLAGS)
            self.setAttribute(Qt.WA_ShowWithoutActivating, True)
        else:
            self.setParent(host)
            self.setAttribute(Qt.WA_ShowWithoutActivating, False)


class _IdentificationCard(QFrame):
    """The compact summary shown after a pick: name, rank, family, source."""

    def __init__(self, picker: "TaxonPicker") -> None:
        super().__init__(picker)
        self.setObjectName("taxonPickerCard")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 6, 6, 7)
        layout.setSpacing(2)
        top = QHBoxLayout()
        top.setSpacing(4)
        self.title = QLabel()
        self.title.setObjectName("taxonPickerCardTitle")
        self.title.setTextFormat(Qt.RichText)
        self.title.setTextInteractionFlags(Qt.TextSelectableByMouse)
        font = self.title.font()
        font.setPointSizeF(font.pointSizeF() + 1.5)
        self.title.setFont(font)
        self.title.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Preferred)
        top.addWidget(self.title, 1)
        self.change_button = QPushButton()
        self.change_button.setObjectName("taxonPickerChange")
        self.clear_button = QToolButton()
        self.clear_button.setObjectName("taxonPickerClear")
        self.clear_button.setText("✕")
        top.addWidget(self.change_button, 0, Qt.AlignTop)
        top.addWidget(self.clear_button, 0, Qt.AlignTop)
        layout.addLayout(top)
        self.meta = QLabel()
        self.meta.setObjectName("taxonPickerCardMeta")
        self.meta.setTextFormat(Qt.RichText)
        self.meta.setWordWrap(True)
        layout.addWidget(self.meta)
        self.sources = QWidget()
        sources_layout = QHBoxLayout(self.sources)
        sources_layout.setContentsMargins(0, 3, 0, 1)
        sources_layout.setSpacing(0)
        self.sources_label = QLabel()
        sources_layout.addWidget(self.sources_label)
        self.source_buttons: list[QPushButton] = []
        self._sources_layout = sources_layout
        sources_layout.addStretch(1)
        layout.addWidget(self.sources)
        self.note = QLabel()
        self.note.setObjectName("taxonPickerCardNote")
        self.note.setTextFormat(Qt.RichText)
        self.note.setWordWrap(True)
        layout.addWidget(self.note)
        self.set_tone("ok")

    def set_tone(self, tone: str) -> None:
        palette = self.palette()
        if tone == "warn":
            background, border = "rgba(214, 158, 46, 40)", "rgba(214, 158, 46, 140)"
        elif tone == "free":
            background, border = palette.color(QPalette.AlternateBase).name(), palette.color(QPalette.Mid).name()
        else:
            background, border = _accent(22).name(QColor.HexArgb), _accent(110).name(QColor.HexArgb)
        self.setProperty("tone", tone)
        self.setStyleSheet(
            f"#taxonPickerCard {{ background: {background}; border: 1px solid {border}; border-radius: 7px; }}"
        )

    def set_sources(self, labels: list[tuple[str, str]], checked: int) -> None:
        for button in self.source_buttons:
            self._sources_layout.removeWidget(button)
            button.hide()
            button.deleteLater()
        self.source_buttons = []
        for i, (text, tooltip) in enumerate(labels):
            button = QPushButton(text)
            button.setObjectName("taxonPickerSourceButton")
            button.setCheckable(True)
            button.setChecked(i == checked)
            button.setToolTip(tooltip)
            button.setStyleSheet(
                "QPushButton#taxonPickerSourceButton { padding: 1px 10px; border: 1px solid palette(mid);"
                " background: palette(base); color: palette(text); border-radius: 0px; }"
                f"QPushButton#taxonPickerSourceButton:checked {{ background: {_ACCENT}; color: white;"
                f" border-color: {_ACCENT}; }}"
            )
            self._sources_layout.insertWidget(1 + i, button)
            self.source_buttons.append(button)
        self.sources.setVisible(bool(labels))


class TaxonPicker(QWidget):
    """One search field for an observation's taxon, and its summary card."""

    concept_picked = Signal(object, object)  # TaxonSearchRow, TaxonSearchConcept
    free_text_committed = Signal(str)
    cleared = Signal()

    def __init__(
        self,
        lookup=None,
        parent: QWidget | None = None,
        *,
        page_size: int = 40,
        debounce_ms: int = 150,
        setting_get: Callable[[str, str], str] | None = None,
        setting_set: Callable[[str, str], None] | None = None,
    ) -> None:
        super().__init__(parent)
        self._lookup = lookup
        self._page_size = max(1, int(page_size))
        self._setting_get = setting_get or _settings_get
        self._setting_set = setting_set or _settings_set
        self._evidence_ids: tuple[int, ...] = ()
        self._current_taxon_id: int | None = None
        self._ai_suggestions: tuple[AiSuggestion, ...] = ()
        self._query = ""
        self._rows: list[TaxonSearchRow] = []
        self._has_more = False
        self._list_mode = False
        self._selection: tuple[TaxonSearchRow, TaxonSearchConcept] | None = None
        # (name, "free" | "legacy" | "missing") for a name with no concept shown.
        self._unlinked: tuple[str, str] | None = None
        self._uncertain = False
        self._editing = False
        self._chip_click_pending = False
        self._ai_handler: Callable[[AiSuggestion], None] | None = None
        # The name Endre put into the field; unchanged, it is not new input.
        self._edit_seed = ""

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(5)
        self.search_input = QLineEdit()
        self.search_input.setObjectName("taxonSearchInput")
        self.search_input.setClearButtonEnabled(True)
        layout.addWidget(self.search_input)
        self.card = _IdentificationCard(self)
        self.card.setVisible(False)
        layout.addWidget(self.card)
        self.ai_strip = QWidget()
        self.ai_strip.setObjectName("taxonPickerAiStrip")
        ai_layout = QHBoxLayout(self.ai_strip)
        ai_layout.setContentsMargins(0, 0, 0, 0)
        ai_layout.setSpacing(5)
        self.ai_label = QLabel()
        self.ai_label.setObjectName("taxonPickerAiLabel")
        ai_layout.addWidget(self.ai_label)
        self._ai_layout = ai_layout
        self.ai_buttons: list[QToolButton] = []
        ai_layout.addStretch(1)
        self.ai_strip.setVisible(False)
        layout.addWidget(self.ai_strip)
        # A crowded host must not squash the card or the chips: the picker
        # keeps the height its contents need.
        self.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)

        self.popup = _ResultPopup(self)
        # The overlay lives in the top-level window while shown; it must go
        # when the picker does.
        self.destroyed.connect(self.popup.deleteLater)
        self._overlay_host: QWidget | None = None
        self.floating_popup = _floating_popup_supported()
        self._watching_app = False
        self._watched_ancestors: list[QWidget] = []
        self._outside_clicks = _OutsideClickWatcher(self)
        self.model = QStandardItemModel(self)
        self.popup.view.setModel(self.model)
        self._delegate = _ResultDelegate(self)
        self.popup.view.setItemDelegate(self._delegate)
        self.popup.view.clicked.connect(self._on_view_clicked)
        self.popup.note_dismiss.clicked.connect(self._dismiss_source_note)

        self._timer = QTimer(self)
        self._timer.setSingleShot(True)
        self._timer.setInterval(max(0, int(debounce_ms)))
        self._timer.timeout.connect(self._run_search)

        self.search_input.textEdited.connect(self._on_text_edited)
        self.search_input.installEventFilter(self)
        self.card.change_button.clicked.connect(self.begin_edit)
        self.card.clear_button.clicked.connect(self._on_clear_clicked)
        self.retranslate()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def retranslate(self) -> None:
        self.search_input.setPlaceholderText(self.tr("Search for a fungus or scientific name…"))
        self.card.change_button.setText(self.tr("Change"))
        self.card.clear_button.setToolTip(self.tr("Remove the identification"))
        self.card.sources_label.setText(self.tr("Source:") + " ")
        self.ai_label.setText(self.tr("Artsorakel:"))
        self.popup.note_title.setText(self.tr("Several entries for one name"))
        self.popup.note_text.setText(self.tr(
            "Some names exist both in NorTaxa and in the Catalogue of Life (COL). They are separate "
            "entries and can differ in family and identifiers. The highlighted source is the one that "
            "is chosen; click the other one to switch."
        ))
        self.popup.note_dismiss.setText(self.tr("Don't show again"))

    def set_lookup(self, lookup) -> None:
        self._lookup = lookup

    def set_evidence_taxon_ids(self, taxon_ids) -> None:
        """Concepts proven by provider identifiers. They set a row's default
        source and rank it first; they never pick or remove anything."""
        self._evidence_ids = tuple(sorted({int(t) for t in taxon_ids or () if t is not None}))

    def set_current_taxon_id(self, taxon_id: int | None) -> None:
        """The observation's current concept: the default of its row, so
        Enter on an unchanged row keeps it."""
        self._current_taxon_id = int(taxon_id) if taxon_id is not None else None

    def set_ai_suggestions(self, suggestions) -> None:
        self._ai_suggestions = tuple(s for s in suggestions or () if isinstance(s, AiSuggestion))
        for button in self.ai_buttons:
            # Hidden at once: a deferred delete leaves it painted meanwhile.
            self._ai_layout.removeWidget(button)
            button.hide()
            button.deleteLater()
        self.ai_buttons = []
        for i, suggestion in enumerate(self._ai_suggestions):
            button = QToolButton()
            button.setObjectName("taxonPickerAiChip")
            button.setAutoRaise(False)
            score = f"  {round(suggestion.score * 100)} %" if suggestion.score is not None else ""
            button.setText(f"{suggestion.label}{score}")
            resolved = suggestion.taxon_id is not None
            button.setProperty("resolved", resolved)
            button.setToolTip(
                self.tr("Model probability, not a determination. Click to choose this name.")
                if resolved else
                self.tr("No database identifier: click to search for the name and choose the entry yourself.")
            )
            style = "solid" if resolved else "dashed"
            button.setStyleSheet(
                f"QToolButton#taxonPickerAiChip {{ border: 1px {style} palette(mid); border-radius: 10px;"
                " padding: 1px 9px; background: palette(base); color: palette(text); }"
            )
            button.clicked.connect(lambda _checked=False, s=suggestion: self._on_ai_chip(s))
            self._ai_layout.insertWidget(1 + i, button)
            self.ai_buttons.append(button)
        self.ai_strip.setVisible(bool(self.ai_buttons))

    def set_ai_suggestion_handler(self, handler: Callable[[AiSuggestion], None] | None) -> None:
        """Let the host apply a chip whose identifier resolved to a concept
        (its own provider-identity path). Without a handler the picker
        commits the resolved concept itself."""
        self._ai_handler = handler

    def set_uncertain(self, uncertain: bool) -> None:
        self._uncertain = bool(uncertain)
        self._refresh_card()

    def show_selection(self, row: TaxonSearchRow, concept: TaxonSearchConcept) -> None:
        """Display a selection without emitting anything (loading, restoring)."""
        self._selection = (row, concept)
        self._unlinked = None
        self._end_edit()

    def show_unlinked_name(self, name: str, *, legacy: bool = False) -> None:
        """Display a name with no bound concept: a free name, or an older
        observation whose name is not linked to a confirmed concept."""
        self._selection = None
        self._unlinked = (" ".join(str(name or "").split()), "legacy" if legacy else "free")
        self._end_edit()

    def show_missing_concept(self, name: str) -> None:
        """Display a stored concept that the installed taxonomy does not
        have (an older or newer release): kept as it is, not searchable."""
        self._selection = None
        self._unlinked = (" ".join(str(name or "").split()), "missing")
        self._end_edit()

    def unlinked(self) -> tuple[str, str] | None:
        """``(name, tone)`` of the shown name without a concept, or ``None``."""
        return self._unlinked

    def clear_display(self) -> None:
        self._selection = None
        self._unlinked = None
        self._end_edit()

    def selection(self) -> tuple[TaxonSearchRow, TaxonSearchConcept] | None:
        return self._selection

    def results(self) -> list[TaxonSearchRow]:
        return list(self._rows)

    def is_popup_visible(self) -> bool:
        return self.popup.isVisible()

    def current_entry(self) -> _Entry | None:
        index = self.popup.view.currentIndex()
        return index.data(ROLE_ENTRY) if index.isValid() else None

    def unfinished_text(self) -> str:
        """Search text typed but neither picked nor kept as a name."""
        if self.search_input.isHidden():
            return ""
        text = " ".join(self.search_input.text().split())
        if self._editing and text == self._edit_seed:
            return ""
        return text

    def discard_unfinished(self) -> None:
        """Drop the typed search text and keep what was there before."""
        self._hide_popup()
        if self._selection is not None or self._unlinked is not None:
            self._end_edit()
        else:
            self.search_input.clear()

    def keep_unfinished_as_name(self) -> bool:
        """Keep the typed search text as a name without a taxon."""
        text = self.unfinished_text()
        if not text:
            return False
        self._commit_free_text(text)
        return True

    def begin_edit(self) -> None:
        """Endre: back to the search field, with the current name selected."""
        self._editing = True
        if self._selection is not None:
            text = self._selection[1].scientific_name
        elif self._unlinked is not None:
            text = self._unlinked[0]
        else:
            text = ""
        self._edit_seed = " ".join(str(text or "").split())
        self.card.setVisible(False)
        self.search_input.setVisible(True)
        self.search_input.setText(text)
        self.search_input.selectAll()
        self.search_input.setFocus(Qt.OtherFocusReason)

    def search_for(self, text: str) -> None:
        """Open the search with ``text`` and show its results (used for an
        Artsorakel name without an identifier, and by hosts)."""
        self._editing = self._selection is not None or self._unlinked is not None
        self._edit_seed = ""
        self.card.setVisible(False)
        self.search_input.setVisible(True)
        self.search_input.setText(text)
        self.search_input.setFocus(Qt.OtherFocusReason)
        self._list_mode = False
        self._run_search()

    # ------------------------------------------------------------------
    # Labels (also used by the delegate)
    # ------------------------------------------------------------------

    def rank_label(self, row: TaxonSearchRow) -> str:
        if row.kind == KIND_SYNONYM:
            return self.tr("synonym")
        return {
            "genus": self.tr("genus"),
            "species": self.tr("species"),
            "aggregate": self.tr("group"),
            "subspecies": self.tr("subsp."),
            "variety": self.tr("var."),
            "form": self.tr("f."),
        }.get(row.rank, row.rank)

    def source_hint(self) -> str:
        """How to switch the highlighted row's source with the keyboard."""
        if self._list_mode:
            keys = "←/→"
        elif sys.platform == "darwin":
            keys = "⌥←/→"
        else:
            keys = "Alt+←/→"
        return self.tr("{0} switch source").format(keys)

    def _rank_title(self, rank: str) -> str:
        return {
            "genus": self.tr("Genus"),
            "species": self.tr("Species"),
            "aggregate": self.tr("Species group"),
            "subspecies": self.tr("Subspecies"),
            "variety": self.tr("Variety"),
            "form": self.tr("Form"),
        }.get(rank, rank)

    def row_title_html(self, row: TaxonSearchRow, palette: QPalette) -> str:
        text = palette.color(QPalette.Text).name()
        dim = _dim(palette).name()
        name = _italic_name(row.name)
        html = f"<span style='color:{text}'><b>{name}</b></span>"
        if row.vernacular_name and row.kind != KIND_SYNONYM:
            html += f"&nbsp;&nbsp;<span style='color:{text}'>{escape(row.vernacular_name)}</span>"
        elif row.kind == KIND_SYNONYM and row.synonym_of:
            html += f"&nbsp;&nbsp;<span style='color:{dim}'>→ {_italic_name(row.synonym_of)}</span>"
        return html

    def row_detail_html(self, row: TaxonSearchRow, choice: int, palette: QPalette) -> str:
        parts: list[str] = []
        if row.kind == KIND_SYNONYM and row.synonym_of:
            target = self.tr("synonym of {0}").format(_italic_name(row.synonym_of))
            if row.vernacular_name:
                target += f" ({escape(row.vernacular_name)})"
            parts.append(target)
        families = [c.family or "" for c in row.concepts]
        if row.has_source_choice and len(set(families)) > 1:
            no_family = self.tr("no family")
            parts.append(" · ".join(
                f"{escape(c.source_name)}: {escape(c.family or no_family)}" for c in row.concepts
            ))
        elif families and families[0]:
            parts.append(escape(families[0]))
        if not row.has_source_choice and (row.shares_name or row.concepts[0].is_evidence):
            parts.append(escape(row.concepts[0].source_name))
        for concept in row.concepts:
            if concept.is_current:
                parts.append(self.tr("current: {0}").format(escape(concept.source_name)))
        return f"<span style='color:{_dim(palette).name()}'>{'  ·  '.join(parts)}</span>"

    def concept_tooltip(self, concept: TaxonSearchConcept) -> str:
        no_family = self.tr("no family")
        lines = [f"{concept.source_name} · {concept.family or no_family}"]
        if concept.artsdatabanken_id is not None:
            lines.append(self.tr("Artsdatabanken {0}").format(concept.artsdatabanken_id))
        else:
            lines.append(self.tr("No Artsdatabanken identifier"))
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def _on_text_edited(self, text: str) -> None:
        self._list_mode = False
        if len(" ".join(str(text or "").split())) < TAXON_SEARCH_MIN_CHARS:
            self._timer.stop()
            self._hide_popup()
            self.model.clear()
            self._rows = []
            return
        self._timer.start()

    def _search_page(self, query: str, offset: int):
        lookup = self._lookup
        searcher = getattr(lookup, "search_taxa", None) if lookup is not None else None
        if not callable(searcher):
            return None
        try:
            return searcher(
                query,
                offset=offset,
                limit=self._page_size,
                evidence_taxon_ids=self._evidence_ids,
                current_taxon_id=self._current_taxon_id,
            )
        except Exception:
            return None

    def _run_search(self) -> None:
        self._timer.stop()
        query = " ".join(self.search_input.text().split())
        self._query = query
        page = self._search_page(query, 0)
        self._rows = list(page.rows) if page is not None else []
        self._has_more = bool(page.has_more) if page is not None else False
        self._fill_model(current_row=0)
        self._show_popup()

    def _fill_model(self, *, current_row: int) -> None:
        self.model.clear()
        for row in self._rows:
            self._append_item(_Entry("row", row=row, choice=0))
        if self._has_more:
            self._append_item(_Entry("more", text=self.tr("Show more")))
        if self._query:
            self._append_item(_Entry("free", text=self.tr("Use “{0}” as a name without a taxon").format(self._query)))
        if self.model.rowCount():
            self.popup.view.setCurrentIndex(self.model.index(min(current_row, self.model.rowCount() - 1), 0))

    def _append_item(self, entry: _Entry) -> None:
        item = QStandardItem()
        item.setEditable(False)
        item.setData(entry, ROLE_ENTRY)
        if entry.kind == "row":
            item.setData(entry.row.name, Qt.DisplayRole)
            item.setData(entry.row.name, Qt.AccessibleTextRole)
        else:
            item.setData(entry.text, Qt.DisplayRole)
        self.model.appendRow(item)

    def _show_more(self) -> None:
        first_new = len(self._rows)
        page = self._search_page(self._query, first_new)
        if page is None:
            return
        known = {(r.kind, r.name, r.rank, tuple(c.sporely_taxon_id for c in r.concepts)) for r in self._rows}
        for row in page.rows:
            key = (row.kind, row.name, row.rank, tuple(c.sporely_taxon_id for c in row.concepts))
            if key not in known:
                self._rows.append(row)
        self._has_more = bool(page.has_more)
        self._fill_model(current_row=first_new)
        self._show_popup()

    # ------------------------------------------------------------------
    # Popup
    # ------------------------------------------------------------------

    def _note_wanted(self) -> bool:
        if not any(r.has_source_choice for r in self._rows):
            return False
        try:
            return str(self._setting_get(SOURCE_NOTE_SETTING, "0") or "0") != "1"
        except Exception:
            return True

    def _show_popup(self) -> None:
        if not self.model.rowCount():
            self._hide_popup()
            return
        self.popup.note.setVisible(self._note_wanted())
        rows = sum(
            _ROW_HEIGHT if self.model.index(i, 0).data(ROLE_ENTRY).kind == "row" else _ACTION_HEIGHT
            for i in range(min(self.model.rowCount(), _POPUP_MAX_ROWS))
        )
        host = self.window()
        self.popup.attach(host, self.floating_popup)
        if self._overlay_host is not host:
            if self._overlay_host is not None:
                self._overlay_host.removeEventFilter(self)
            host.installEventFilter(self)
            self._overlay_host = host
        self._watch_app(True)
        self._watch_ancestors(True)
        self._place_popup(rows)
        self.popup.show()
        self.popup.raise_()
        self.popup.view.scrollTo(self.popup.view.currentIndex())

    def _place_popup(self, rows_height: int | None = None) -> None:
        """Below the field and at least 440 px wide, within the screen when
        floating (within the window as an overlay); above the field only when
        there is not room for the list below and more room above."""
        host = self.popup.parentWidget()
        if host is None:
            return
        if rows_height is None:
            rows_height = self.popup.view.height() - 2
        field = self.search_input
        if self.popup.isWindow():
            origin = field.mapToGlobal(QPoint(0, 0))
            bounds = self._screen_bounds(origin + QPoint(field.width() // 2, field.height() // 2))
        else:
            origin = field.mapTo(host, QPoint(0, 0))
            bounds = host.rect()
        width = min(max(field.width(), 440), bounds.width() - 8)
        self.popup.setFixedWidth(width)
        note_height = self.popup.note.sizeHint().height() if self.popup.note.isVisibleTo(self.popup) else 0
        field_bottom = origin.y() + field.height() + 2
        below = bounds.y() + bounds.height() - field_bottom - 4
        above = origin.y() - bounds.y() - 6
        wanted = rows_height + 2 + note_height + 2
        downward = below >= wanted or below >= above
        room = below if downward else above
        view_height = max(_ACTION_HEIGHT, min(rows_height + 2, room - note_height - 2))
        self.popup.view.setFixedHeight(view_height)
        self.popup.adjustSize()
        x = max(bounds.x() + 4, min(origin.x(), bounds.x() + bounds.width() - width - 4))
        y = field_bottom if downward else origin.y() - self.popup.height() - 2
        self.popup.move(x, y)

    def _screen_bounds(self, point: QPoint) -> QRect:
        """The available geometry of the screen under ``point``: the field's
        own monitor, without the menu bar and Dock."""
        screen = QGuiApplication.screenAt(point) or self.search_input.screen() or QGuiApplication.primaryScreen()
        return screen.availableGeometry()

    def _watch_app(self, on: bool) -> None:
        """While the list is open, a press anywhere outside it and the field
        closes it, as does the application losing activation."""
        app = QApplication.instance()
        if app is None or on == self._watching_app:
            return
        self._watching_app = on
        if on:
            app.installEventFilter(self._outside_clicks)
            app.applicationStateChanged.connect(self._on_application_state)
        else:
            app.removeEventFilter(self._outside_clicks)
            try:
                app.applicationStateChanged.disconnect(self._on_application_state)
            except (RuntimeError, TypeError):
                pass

    def _on_application_state(self, state) -> None:
        if self._alive() and state != Qt.ApplicationActive:
            self._hide_popup()

    def _watch_ancestors(self, on: bool) -> None:
        """While the list is open, follow the widgets between the picker and
        its window: a scroll area moves one of them, not the picker."""
        for widget in self._watched_ancestors:
            if isValid(widget):
                widget.removeEventFilter(self)
        self._watched_ancestors = []
        if not on:
            return
        widget = self.parentWidget()
        while widget is not None and not widget.isWindow():
            widget.installEventFilter(self)
            self._watched_ancestors.append(widget)
            widget = widget.parentWidget()

    def _follow_field(self) -> None:
        """Keep the list at the field after a parent moved; close it when the
        field has scrolled out of sight."""
        if self.search_input.visibleRegion().isEmpty():
            self._hide_popup()
        else:
            self._place_popup()

    def _on_press_anywhere(self, widget: QWidget) -> None:
        if not self._alive() or not self.popup.isVisible():
            return
        if widget is self.search_input or widget is self.popup or self.popup.isAncestorOf(widget):
            return
        self._hide_popup()

    def _hide_popup(self) -> None:
        self._timer.stop()
        self._watch_app(False)
        self._watch_ancestors(False)
        if self.popup.isVisible():
            self.popup.hide()

    def _dismiss_source_note(self) -> None:
        try:
            self._setting_set(SOURCE_NOTE_SETTING, "1")
        except Exception:
            pass
        self.popup.note.setVisible(False)
        if self.popup.isVisible():
            self._place_popup()

    # ------------------------------------------------------------------
    # Picking
    # ------------------------------------------------------------------

    def _on_view_clicked(self, index: QModelIndex) -> None:
        if self._chip_click_pending:
            self._chip_click_pending = False
            return
        entry = index.data(ROLE_ENTRY)
        self._activate_index(index, choice=entry.choice if entry is not None else 0)

    def _activate_index(self, index: QModelIndex, *, choice: int | None = None) -> None:
        if not index.isValid():
            return
        entry = index.data(ROLE_ENTRY)
        if entry is None:
            return
        if entry.kind == "more":
            self._show_more()
            return
        if entry.kind == "free":
            self._commit_free_text(self._query)
            return
        chosen = entry.choice if choice is None else max(0, min(choice, len(entry.row.concepts) - 1))
        self._commit(entry.row, entry.row.concepts[chosen])

    def _commit(self, row: TaxonSearchRow, concept: TaxonSearchConcept) -> None:
        self._hide_popup()
        self._selection = (row, concept)
        self._unlinked = None
        self._end_edit()
        self.concept_picked.emit(row, concept)

    def _commit_free_text(self, text: str) -> None:
        text = " ".join(str(text or "").split())
        if not text:
            return
        self._hide_popup()
        self._selection = None
        self._unlinked = (text, "free")
        self._end_edit()
        self.free_text_committed.emit(text)

    def _set_choice(self, step: int) -> bool:
        index = self.popup.view.currentIndex()
        entry = index.data(ROLE_ENTRY) if index.isValid() else None
        if entry is None or entry.kind != "row" or not entry.row.has_source_choice:
            return False
        choice = max(0, min(entry.choice + step, len(entry.row.concepts) - 1))
        if choice != entry.choice:
            self.model.setData(index, replace(entry, choice=choice), ROLE_ENTRY)
            self.popup.view.update(index)
        return True

    def _move(self, step: int) -> None:
        count = self.model.rowCount()
        if not count:
            return
        current = self.popup.view.currentIndex().row()
        target = max(0, min(count - 1, (current if current >= 0 else -1) + step))
        self.popup.view.setCurrentIndex(self.model.index(target, 0))
        self._list_mode = True

    def _on_clear_clicked(self) -> None:
        self._selection = None
        self._unlinked = None
        self._end_edit()
        self.search_input.setFocus(Qt.OtherFocusReason)
        self.cleared.emit()

    def _on_ai_chip(self, suggestion: AiSuggestion) -> None:
        if suggestion.taxon_id is not None and self._ai_handler is not None:
            self._ai_handler(suggestion)
            return
        if suggestion.taxon_id is not None:
            lookup = self._lookup
            searcher = getattr(lookup, "search_taxa", None) if lookup is not None else None
            page = None
            if callable(searcher):
                try:
                    page = searcher(
                        suggestion.scientific_name, offset=0, limit=self._page_size,
                        evidence_taxon_ids=(suggestion.taxon_id,),
                        current_taxon_id=self._current_taxon_id,
                    )
                except Exception:
                    page = None
            for row in (page.rows if page is not None else ()):
                for concept in row.concepts:
                    if concept.sporely_taxon_id == suggestion.taxon_id and row.name == suggestion.scientific_name:
                        self._commit(row, concept)
                        return
        # No resolvable identifier (or its entry is not found): the name goes
        # into the search, and the user chooses the entry.
        self.search_for(suggestion.scientific_name)

    # ------------------------------------------------------------------
    # Card
    # ------------------------------------------------------------------

    def _end_edit(self) -> None:
        self._editing = False
        self._hide_popup()
        has_card = self._selection is not None or self._unlinked is not None
        self.search_input.setVisible(not has_card)
        if not has_card:
            self.search_input.clear()
        self.card.setVisible(has_card)
        self._refresh_card()

    def _restore_after_escape(self) -> None:
        self._hide_popup()
        if self._selection is not None or self._unlinked is not None:
            self._end_edit()

    def _refresh_card(self) -> None:
        card = self.card
        palette = card.palette()
        if self._selection is not None:
            row, concept = self._selection
            title = f"<b>{_display_name(row.name if row.kind == KIND_SYNONYM else concept.canonical_scientific_name, row.rank)}</b>"
            vernacular = concept.vernacular_name or (row.vernacular_name if row.kind != KIND_SYNONYM else None)
            if vernacular:
                title += f"&nbsp;&nbsp;<span style='font-weight:normal'>{escape(vernacular)}</span>"
            card.title.setText(title)
            meta = [self._rank_title(row.rank)]
            if concept.family:
                meta.append(escape(concept.family))
            if not row.has_source_choice:
                meta.append(escape(concept.source_name))
            if concept.artsdatabanken_id is not None:
                meta.append(self.tr("Artsdatabanken {0}").format(concept.artsdatabanken_id))
            if self._uncertain:
                uncertain = self.tr("uncertain determination")
                meta.append(f"<b>{escape(uncertain)}</b>")
            card.meta.setText("  ·  ".join(meta))
            notes = []
            if row.kind == KIND_SYNONYM:
                notes.append(self.tr("You chose the synonym {0}; it is stored on the entry {1}.").format(
                    _italic_name(row.name), _italic_name(concept.display_scientific_name)))
            card.note.setText("<br>".join(notes))
            card.note.setVisible(bool(notes))
            if row.has_source_choice:
                families = {c.family for c in row.concepts}
                labels = [
                    (
                        f"{c.source_name} · {c.family}" if len(families) > 1 and c.family else c.source_name,
                        self.concept_tooltip(c),
                    )
                    for c in row.concepts
                ]
                card.set_sources(labels, row.concepts.index(concept))
                for i, button in enumerate(card.source_buttons):
                    button.clicked.connect(lambda _checked=False, i=i: self._switch_source(i))
            else:
                card.set_sources([], 0)
            card.set_tone("ok")
        elif self._unlinked is not None:
            name, tone = self._unlinked
            card.title.setText(f"<b>{escape(name)}</b>")
            if tone == "missing":
                card.meta.setText(escape(self.tr("Linked taxon not in the installed taxonomy")))
                card.note.setText(escape(self.tr("It is kept as it is until you choose another entry.")))
                card.note.setVisible(True)
                card.set_tone("warn")
            elif tone == "legacy":
                card.meta.setText(escape(self.tr("Not linked to a confirmed taxon")))
                card.note.setText(escape(self.tr("Nothing changes until you choose an entry.")))
                card.note.setVisible(True)
                card.set_tone("warn")
            else:
                card.meta.setText(escape(self.tr("Name without a taxon: not linked to the taxonomy")))
                card.note.setVisible(False)
                card.set_tone("free")
            card.set_sources([], 0)
        else:
            card.title.clear()
            card.meta.clear()
            card.note.clear()
            card.set_sources([], 0)

    def _switch_source(self, index: int) -> None:
        if self._selection is None:
            return
        row, concept = self._selection
        if not 0 <= index < len(row.concepts) or row.concepts[index] == concept:
            self._refresh_card()
            return
        self._commit(row, row.concepts[index])

    # ------------------------------------------------------------------
    # Keys and focus
    # ------------------------------------------------------------------

    def _alive(self) -> bool:
        """False while Qt tears the picker down: its window can still send
        events after the picker's children are gone."""
        return all(isValid(w) for w in (self, self.search_input, self.popup, self.card)) and isValid(self._timer)

    def eventFilter(self, obj, event):
        if not self._alive():
            return False
        event_type = event.type()
        if event_type == QEvent.Move and self.popup.isVisible() and obj in self._watched_ancestors:
            self._follow_field()
        if obj is self._overlay_host and self.popup.isVisible():
            if event_type in (QEvent.Resize, QEvent.Move):
                self._place_popup()
            elif event_type in (QEvent.Hide, QEvent.Close, QEvent.WindowDeactivate):
                self._hide_popup()
        if obj is self.search_input:
            if event_type == QEvent.ShortcutOverride and self._wants_key(event):
                # Hosts bind Return to Save and Escape to Close as shortcuts,
                # which Qt fires before the field sees the key. Claim the keys
                # the picker acts on.
                event.accept()
                return True
            if event_type == QEvent.KeyPress:
                return self._handle_key(event)
            if event_type == QEvent.FocusOut:
                # The popup never takes focus, so focus leaving the field means
                # the user went elsewhere.
                self._hide_popup()
        return super().eventFilter(obj, event)

    def _wants_key(self, event) -> bool:
        key = event.key()
        if key in (Qt.Key_Return, Qt.Key_Enter):
            return self.popup.isVisible() or bool(self.search_input.text().strip())
        if key == Qt.Key_Escape:
            return self.popup.isVisible() or self._editing
        return False

    def _handle_key(self, event) -> bool:
        key = event.key()
        popup_open = self.popup.isVisible()
        if key == Qt.Key_Escape:
            if popup_open:
                self._hide_popup()
            elif self._editing:
                self._restore_after_escape()
            else:
                return False
            return True
        if not popup_open:
            searchable = len(self.search_input.text().strip()) >= TAXON_SEARCH_MIN_CHARS
            if key == Qt.Key_Down and searchable:
                self._run_search()
                return True
            if key in (Qt.Key_Return, Qt.Key_Enter) and self.search_input.text().strip():
                # Typed text is never a dialog's default action: Enter shows
                # its results again instead of reaching the Save button.
                if searchable:
                    self._run_search()
                return True
            return False
        if key in (Qt.Key_Down, Qt.Key_Up):
            self._move(1 if key == Qt.Key_Down else -1)
            return True
        if key in (Qt.Key_PageDown, Qt.Key_PageUp):
            self._move(_POPUP_MAX_ROWS - 1 if key == Qt.Key_PageDown else 1 - _POPUP_MAX_ROWS)
            return True
        if key in (Qt.Key_Left, Qt.Key_Right):
            # Source switching: always with Alt/Option, and with the bare arrow
            # once the user is moving through the list rather than the text.
            if self._list_mode or event.modifiers() & Qt.AltModifier:
                if self._set_choice(1 if key == Qt.Key_Right else -1):
                    return True
            return False
        if key == Qt.Key_Tab:
            # Tab is focus navigation. It picks only a row the user moved to
            # with the arrow keys, never the first result highlighted for them.
            if self._list_mode and not self._timer.isActive():
                self._activate_index(self.popup.view.currentIndex())
                return True
            self._hide_popup()
            return False
        if key in (Qt.Key_Return, Qt.Key_Enter):
            if self._timer.isActive():
                # Typed text the results do not reflect yet: search first.
                self._run_search()
                return True
            self._activate_index(self.popup.view.currentIndex())
            return True
        return False

    def hideEvent(self, event) -> None:
        if self._alive():
            self._hide_popup()
        super().hideEvent(event)

    def moveEvent(self, event) -> None:
        if self._alive() and self.popup.isVisible():
            self._place_popup()
        super().moveEvent(event)


def _settings_get(key: str, default: str) -> str:
    from database.models import SettingsDB

    return SettingsDB.get_setting(key, default)


def _settings_set(key: str, value: str) -> None:
    from database.models import SettingsDB

    SettingsDB.set_setting(key, value)


__all__ = ["AiSuggestion", "SOURCE_NOTE_SETTING", "TaxonPicker"]
