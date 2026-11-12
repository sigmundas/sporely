"""The mode bar above the main window's pages (redesign stage 9a).

Modes in workflow order: View, Edit, Images, Measure, Analysis; Camera import
sits apart at the far right because it works across all observations. The
bar only drives navigation: the pages themselves stay in the main window's
``QTabWidget`` (its own tab bar is hidden), so ``tab_widget`` remains the one
source of truth for the current page.

Edit is the Rediger mode since stage 9g2. Its click goes through
``editRequested`` (the window chooses the observation, and cloud-only rows
still open the dialog); it is checked while Rediger shows.
"""
from __future__ import annotations

from PySide6.QtCore import QEvent, QSize, Qt, Signal
from PySide6.QtGui import QIcon, QKeySequence
from PySide6.QtWidgets import (
    QButtonGroup,
    QHBoxLayout,
    QLabel,
    QSizePolicy,
    QToolButton,
    QWidget,
)


class ModeBar(QWidget):
    """Mode buttons on the left, Camera import and a corner widget on the right."""

    modeRequested = Signal(int)
    editRequested = Signal()

    def __init__(self, alt_label: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("modeBar")
        self.setAttribute(Qt.WA_StyledBackground, True)
        self._alt_label = alt_label
        self._page_by_button: dict[QToolButton, int] = {}
        self._button_by_page: dict[int, QToolButton] = {}
        self._waiting_count = 0

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        self._layout = layout

        self._group = QButtonGroup(self)
        self._group.setExclusive(True)
        self._group.buttonClicked.connect(self._on_mode_clicked)

        self.view_button = self._mode_button(self.tr("View"), "O")
        self.edit_button = self._mode_button(self.tr("Edit"), None)
        self.edit_button.setToolTip(
            self.tr("Edit the selected observation ({shortcut})").format(
                shortcut=QKeySequence("Ctrl+E").toString(QKeySequence.NativeText)
            )
        )
        self.edit_button.clicked.connect(self.editRequested)
        self.images_button = self._mode_button(self.tr("Images"), "L")
        self.measure_button = self._mode_button(self.tr("Measure"), "M")
        self.analysis_button = self._mode_button(self.tr("Analysis"), "A")
        layout.addStretch(1)

        self.camera_import_button = QToolButton(self)
        self.camera_import_button.setObjectName("cameraImportButton")
        self.camera_import_button.setCheckable(True)
        self.camera_import_button.setCursor(Qt.PointingHandCursor)
        self.camera_import_button.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        self.camera_import_button.setIconSize(QSize(16, 16))
        # QToolButton defaults to Fixed; Preferred lets the short label set the minimum.
        self.camera_import_button.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        # The long label when it fits; mode names never shrink instead.
        self._camera_full_text = self.tr("Camera import · all observations")
        self._camera_short_text = self.tr("Camera import")
        self.camera_import_button.setText(self._camera_full_text)
        self.camera_import_button.setToolTip(
            self.tr("Camera import works across all observations ({shortcut})").format(
                shortcut=f"{alt_label}C"
            )
        )
        self._group.addButton(self.camera_import_button)
        layout.addWidget(self.camera_import_button, 0, Qt.AlignVCenter)

        self.waiting_badge = QLabel(self)
        self.waiting_badge.setObjectName("modeBarBadge")
        self.waiting_badge.setAlignment(Qt.AlignCenter)
        self.waiting_badge.setVisible(False)
        layout.addSpacing(4)
        layout.addWidget(self.waiting_badge, 0, Qt.AlignVCenter)
        layout.addSpacing(8)

        self._corner: QWidget | None = None

    def _mode_button(self, name: str, key: str | None, *, checkable: bool = True) -> QToolButton:
        button = QToolButton(self)
        button.setObjectName("modeBarButton")
        button.setCursor(Qt.PointingHandCursor)
        button.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        button.setIconSize(QSize(12, 12))
        button.setText(f"{name}  {self._alt_label}{key}" if key else name)
        button.setProperty("baseText", button.text())
        button.setCheckable(checkable)
        button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Preferred)
        if checkable:
            self._group.addButton(button)
        self._layout.addWidget(button)
        return button

    def bind_page(self, button: QToolButton, page_index: int) -> None:
        """Clicking ``button`` asks for ``page_index``; that page checks it."""
        self._page_by_button[button] = int(page_index)
        self._button_by_page[int(page_index)] = button

    def bind_display_page(self, button: QToolButton, page_index: int) -> None:
        """``button`` is checked while ``page_index`` shows; its click is handled elsewhere."""
        self._button_by_page[int(page_index)] = button

    def set_corner_widget(self, widget: QWidget) -> None:
        widget.setParent(self)
        self._layout.addWidget(widget, 0, Qt.AlignVCenter)
        self._corner = widget

    def set_camera_icon(self, icon: QIcon) -> None:
        self.camera_import_button.setIcon(icon)

    def set_current_page(self, page_index: int) -> None:
        """Check the button for the page shown; the signal is not re-emitted."""
        button = self._button_by_page.get(int(page_index))
        if button is None:
            return
        button.setChecked(True)

    def set_page_icon(self, page_index: int, icon: QIcon) -> None:
        """An icon beside a mode's name (Live Lab's recording dot on Images)."""
        button = self._button_by_page.get(int(page_index))
        if button is not None:
            button.setIcon(icon)

    def set_page_badge(self, page_index: int, text: str) -> None:
        """A short state after a mode's name (“Live 00:14” on Images while a session runs)."""
        button = self._button_by_page.get(int(page_index))
        if button is None:
            return
        base = str(button.property("baseText") or button.text())
        button.setText(f"{base}  ·  {text}" if text else base)
        self._fit_camera_import_text()

    def page_badge(self, page_index: int) -> str:
        button = self._button_by_page.get(int(page_index))
        if button is None:
            return ""
        base = str(button.property("baseText") or "")
        return button.text()[len(base) + 5:] if button.text() != base else ""

    def set_edit_enabled(self, enabled: bool) -> None:
        self.edit_button.setEnabled(bool(enabled))

    def set_waiting_count(self, count: int) -> None:
        """Photos from the last Camera import scan that still wait for import."""
        count = max(0, int(count or 0))
        self._waiting_count = count
        self.waiting_badge.setText(str(count))
        self.waiting_badge.setVisible(count > 0)
        self._fit_camera_import_text()
        self.waiting_badge.setToolTip(
            self.tr("%n matched photo(s) from the last scan are waiting to be imported", "", count)
        )

    def camera_import_text_is_short(self) -> bool:
        return self.camera_import_button.text() == self._camera_short_text

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._fit_camera_import_text()

    def event(self, event) -> bool:
        handled = super().event(event)
        if event.type() == QEvent.LayoutRequest:
            # A corner change (activity badge shown or hidden) moves the space left.
            self._fit_camera_import_text()
        return handled

    def showEvent(self, event) -> None:
        super().showEvent(event)
        self._fit_camera_import_text()

    def _camera_hint_width(self, text: str) -> int:
        button = self.camera_import_button
        metrics = button.fontMetrics()
        return button.sizeHint().width() - metrics.horizontalAdvance(button.text()) + metrics.horizontalAdvance(text)

    def _fit_camera_import_text(self) -> None:
        """Long label when everything fits, else the short one.

        The pill's minimum width is pinned to the short label so switching
        text never changes the bar's minimum width (which would resize the
        window and call this again).
        """
        button = self.camera_import_button
        others = 0
        for index in range(self._layout.count()):
            item = self._layout.itemAt(index)
            if item.widget() is button:
                continue
            if item.spacerItem() is not None:
                others += item.spacerItem().sizeHint().width()  # fixed gaps; the stretch is 0
                continue
            if item.isEmpty():
                continue  # hidden widgets
            others += item.sizeHint().width()
        short_width = self._camera_hint_width(self._camera_short_text)
        if button.minimumWidth() != short_width:
            button.setMinimumWidth(short_width)
        fits = others + self._camera_hint_width(self._camera_full_text) <= self.width()
        text = self._camera_full_text if fits else self._camera_short_text
        if button.text() != text:
            button.setText(text)

    def _on_mode_clicked(self, button) -> None:
        page = self._page_by_button.get(button)
        if page is not None:
            self.modeRequested.emit(page)
