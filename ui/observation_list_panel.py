"""The compact observation list and its rail (redesign stage 9b).

``ObservationListPanel`` is a second view on the Observations table's own
model: one line per observation, its number muted and its name, in the
table's current order and filter. It shares no rows of its own, so sorting,
searching and cloud rows in the table show here too. Choosing a row asks the
main window to select that row in the table and open its observation; the
table stays the one source of truth for what is selected.

``VerticalRailButton`` is the thin “OBSERVATIONS ▸” rail that opens the list
as a left drawer in Images, Measure and Analysis.
"""
from __future__ import annotations

from PySide6.QtCore import QModelIndex, QRect, QSize, Qt, Signal
from PySide6.QtGui import QColor, QPainter, QPalette
from PySide6.QtWidgets import (
    QAbstractButton,
    QAbstractItemView,
    QHBoxLayout,
    QLineEdit,
    QListView,
    QPushButton,
    QSizePolicy,
    QStyle,
    QStyledItemDelegate,
    QStyleOptionViewItem,
    QVBoxLayout,
    QWidget,
)

from .styles import STAGING_BORDER

# Columns of the Observations table this list reads (ui/observations_tab.py).
ID_COLUMN = 0
COMMON_NAME_COLUMN = 1
GENUS_COLUMN = 2
SPECIES_COLUMN = 3
DATE_COLUMN = 5


def _cell(index: QModelIndex, column: int) -> str:
    """A cell's first line: the name cell holds the common name, then the scientific one."""
    text = str(index.siblingAtColumn(column).data(Qt.DisplayRole) or "").strip()
    text = text.splitlines()[0].strip() if text else ""
    return "" if text == "-" else text


def observation_list_name(index: QModelIndex) -> str:
    """The common name, else the scientific name the table shows."""
    common = _cell(index, COMMON_NAME_COLUMN)
    if common.startswith("-"):
        # No common name: the table shows “- (Genus species)”.
        rest = common[1:].strip()
        common = rest[1:-1].strip() if rest.startswith("(") and rest.endswith(")") else rest
    if common:
        return common
    genus = _cell(index, GENUS_COLUMN)
    species = _cell(index, SPECIES_COLUMN)
    return " ".join(part for part in (genus, species if genus else "") if part)


def observation_list_number(index: QModelIndex) -> str:
    """The observation's number, also when the table shows thumbnails in its place."""
    id_index = index.siblingAtColumn(ID_COLUMN)
    row_data = id_index.data(Qt.UserRole + 1)
    if isinstance(row_data, dict):
        number = str(row_data.get("id_display") or "").strip()
        if number and number != "-":
            is_cloud = str(row_data.get("row_kind") or "") == "cloud"
            return f"{number} ⭐" if (bool(row_data.get("mark_star")) or is_cloud) else number
    return str(id_index.data(Qt.DisplayRole) or "").strip()


class ObservationListDelegate(QStyledItemDelegate):
    """Number (muted) and name on one line, like the mockup's list.

    An observation with unsaved Live captures shows their count at the right
    in the staging orange (stage 9d).
    """

    unsaved_counts: dict[int, int] = {}

    PADDING_X = 10
    PADDING_Y = 7
    GAP = 10

    def sizeHint(self, option, index) -> QSize:  # noqa: N802 - Qt override
        height = option.fontMetrics.height() + 2 * self.PADDING_Y
        return QSize(120, height)

    def paint(self, painter: QPainter, option, index) -> None:
        opt = QStyleOptionViewItem(option)
        self.initStyleOption(opt, index)
        painter.save()
        selected = bool(opt.state & QStyle.State_Selected)
        hovered = bool(opt.state & QStyle.State_MouseOver)
        palette = opt.palette
        rect = opt.rect.adjusted(2, 1, -2, -1)
        if selected or hovered:
            painter.setRenderHint(QPainter.Antialiasing, True)
            painter.setPen(Qt.NoPen)
            role = QPalette.Highlight if selected else QPalette.AlternateBase
            painter.setBrush(palette.brush(role))
            painter.drawRoundedRect(rect, 6, 6)
        number = observation_list_number(index)
        name = observation_list_name(index)
        text_rect = rect.adjusted(self.PADDING_X, 0, -self.PADDING_X, 0)
        metrics = opt.fontMetrics
        # A number column four digits wide, right-aligned, so the names line up.
        number_width = max(metrics.horizontalAdvance("0" * 4), metrics.horizontalAdvance(number))
        muted = palette.color(QPalette.HighlightedText if selected else QPalette.PlaceholderText)
        painter.setPen(muted)
        painter.drawText(
            QRect(text_rect.left(), text_rect.top(), number_width, text_rect.height()),
            Qt.AlignRight | Qt.AlignVCenter,
            number,
        )
        name_left = text_rect.left() + number_width + (self.GAP if number else 0)
        right = text_rect.right()
        try:
            observation_id = int(index.siblingAtColumn(ID_COLUMN).data(Qt.UserRole) or 0)
        except (TypeError, ValueError):
            observation_id = 0
        unsaved = int(self.unsaved_counts.get(observation_id, 0) or 0)
        if unsaved > 0:
            badge = str(unsaved)
            badge_width = metrics.horizontalAdvance(badge) + 12
            badge_rect = QRect(right - badge_width + 1, text_rect.center().y() - 9, badge_width, 18)
            painter.setRenderHint(QPainter.Antialiasing, True)
            painter.setPen(Qt.NoPen)
            painter.setBrush(QColor(STAGING_BORDER))
            painter.drawRoundedRect(badge_rect, 9, 9)
            painter.setPen(QColor("#ffffff"))
            painter.drawText(badge_rect, Qt.AlignCenter, badge)
            right = badge_rect.left() - self.GAP
        name_rect = QRect(name_left, text_rect.top(), max(0, right - name_left), text_rect.height())
        painter.setPen(palette.color(QPalette.HighlightedText if selected else QPalette.Text))
        painter.drawText(
            name_rect,
            Qt.AlignLeft | Qt.AlignVCenter,
            metrics.elidedText(name, Qt.ElideRight, name_rect.width()),
        )
        painter.restore()


class _ObservationListView(QListView):
    """Enter opens the current row; arrows only move."""

    rowChosen = Signal(int)

    def keyPressEvent(self, event) -> None:  # noqa: N802 - Qt override
        if event.key() in (Qt.Key_Return, Qt.Key_Enter):
            index = self.currentIndex()
            if index.isValid():
                self.rowChosen.emit(index.row())
                event.accept()
                return
        super().keyPressEvent(event)


class ObservationListPanel(QWidget):
    """“+ New”, “Table”, search and the list, over the table's model."""

    rowChosen = Signal(int)
    newRequested = Signal()
    tableRequested = Signal()
    searchTextChanged = Signal(str)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("observationListPanel")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        buttons = QHBoxLayout()
        buttons.setSpacing(6)
        self.new_button = QPushButton(self.tr("+ New"), self)
        self.new_button.setObjectName("primaryButton")
        self.new_button.setToolTip(self.tr("Create a new observation"))
        self.new_button.clicked.connect(self.newRequested)
        buttons.addWidget(self.new_button, 1)
        self.table_button = QPushButton(self.tr("Table"), self)
        self.table_button.setObjectName("outlineButton")
        self.table_button.setToolTip(self.tr("All observations as a sortable table"))
        self.table_button.clicked.connect(self.tableRequested)
        buttons.addWidget(self.table_button, 0)
        layout.addLayout(buttons)

        self.search_input = QLineEdit(self)
        self.search_input.setPlaceholderText(self.tr("Search observations..."))
        self.search_input.setClearButtonEnabled(True)
        self.search_input.textChanged.connect(self.searchTextChanged)
        layout.addWidget(self.search_input)

        self.list_view = _ObservationListView(self)
        self.list_view.setObjectName("observationList")
        self.list_view.setItemDelegate(ObservationListDelegate(self.list_view))
        self.list_view.setSelectionMode(QAbstractItemView.SingleSelection)
        self.list_view.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.list_view.setUniformItemSizes(True)
        self.list_view.setMouseTracking(True)
        self.list_view.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.list_view.setFrameShape(QListView.NoFrame)
        self.list_view.clicked.connect(lambda index: self.rowChosen.emit(index.row()))
        self.list_view.rowChosen.connect(self.rowChosen)
        layout.addWidget(self.list_view, 1)

    def set_unsaved_counts(self, counts: dict[int, int]) -> None:
        """Unsaved Live captures per observation, shown as a count on its row."""
        delegate = self.list_view.itemDelegate()
        delegate.unsaved_counts = {int(key): int(value) for key, value in dict(counts or {}).items() if value}
        self.list_view.viewport().update()

    def unsaved_counts(self) -> dict[int, int]:
        return dict(self.list_view.itemDelegate().unsaved_counts)

    def set_source_model(self, model) -> None:
        self.list_view.setModel(model)
        self.list_view.setModelColumn(ID_COLUMN)

    def set_search_text(self, text: str) -> None:
        """Follow the table's search field without echoing back."""
        if self.search_input.text() == text:
            return
        self.search_input.blockSignals(True)
        self.search_input.setText(text)
        self.search_input.blockSignals(False)

    def mark_row(self, row: int) -> None:
        """Show ``row`` as the current observation (``-1`` clears)."""
        model = self.list_view.model()
        selection = self.list_view.selectionModel()
        if model is None or selection is None:
            return
        if row < 0 or row >= model.rowCount():
            # No visible mark, no hidden Enter target either.
            selection.clear()
            return
        index = model.index(row, ID_COLUMN)
        selection.setCurrentIndex(index, selection.SelectionFlag.ClearAndSelect)
        self.list_view.scrollTo(index)

    def marked_row(self) -> int:
        selection = self.list_view.selectionModel()
        rows = selection.selectedIndexes() if selection is not None else []
        return rows[0].row() if rows else -1


class VerticalRailButton(QAbstractButton):
    """A thin button with its label written top to bottom."""

    WIDTH = 28

    def __init__(self, text: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("observationRail")
        self.setText(text)
        self.setCursor(Qt.PointingHandCursor)
        self.setFocusPolicy(Qt.TabFocus)
        self.setAttribute(Qt.WA_Hover, True)
        self.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Expanding)

    ARROW = "▸"  # drawn upright: the drawer opens to the right

    def sizeHint(self) -> QSize:  # noqa: N802 - Qt override
        return QSize(self.WIDTH, self.fontMetrics().horizontalAdvance(self.text()) + 48)

    def minimumSizeHint(self) -> QSize:  # noqa: N802 - Qt override
        return QSize(self.WIDTH, 40)

    def paintEvent(self, _event) -> None:  # noqa: N802 - Qt override
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing, True)
        palette = self.palette()
        rect = self.rect().adjusted(0, 0, -1, -1)
        painter.setPen(palette.color(QPalette.Mid))
        hover = self.underMouse() or self.isDown()
        painter.setBrush(palette.brush(QPalette.AlternateBase if hover else QPalette.Base))
        painter.drawRoundedRect(rect, 8, 8)
        painter.setPen(palette.color(QPalette.Text if hover else QPalette.PlaceholderText))
        font = painter.font()
        font.setPointSizeF(max(8.0, font.pointSizeF() * 0.9))
        font.setLetterSpacing(font.SpacingType.PercentageSpacing, 106)
        painter.setFont(font)
        metrics = painter.fontMetrics()
        label_length = metrics.horizontalAdvance(self.text())
        top = max(0, (self.height() - label_length - metrics.height()) // 2)
        painter.save()
        painter.translate(self.width() / 2, top)
        painter.rotate(90)
        painter.drawText(QRect(0, -self.width() // 2, label_length, self.width()), Qt.AlignCenter, self.text())
        painter.restore()
        painter.drawText(
            QRect(0, top + label_length + 4, self.width(), metrics.height()),
            Qt.AlignHCenter | Qt.AlignTop,
            self.ARROW,
        )
        painter.end()
