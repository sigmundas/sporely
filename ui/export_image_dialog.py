"""Shared export image dialog used by multiple tools."""
from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFormLayout,
    QLabel,
    QSpinBox,
)


class ExportImageDialog(QDialog):
    """Dialog to configure export size and quality."""

    def __init__(self, base_width, base_height, scale_percent, fmt, parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Export image"))
        self.setWindowFlags(
            Qt.Dialog
            | Qt.CustomizeWindowHint
            | Qt.WindowTitleHint
            | Qt.WindowCloseButtonHint
        )
        self.base_width = max(1, int(base_width))
        self.base_height = max(1, int(base_height))
        self.format = fmt or "png"
        self._updating = False
        self._init_ui(scale_percent, self.format)

    def _init_ui(self, scale_percent, fmt):
        layout = QFormLayout(self)
        layout.setSpacing(8)

        self.format_input = QComboBox()
        self.format_input.addItem("PNG", "png")
        self.format_input.addItem("JPEG", "jpg")
        self.format_input.addItem("SVG", "svg")
        current_format = self.format_input.findData(fmt)
        if current_format >= 0:
            self.format_input.setCurrentIndex(current_format)
        self.format_input.currentIndexChanged.connect(self.on_format_changed)
        layout.addRow(self.tr("Format:"), self.format_input)

        self.scale_label = QLabel(self.tr("Scale %:"))
        self.scale_input = QDoubleSpinBox()
        self.scale_input.setRange(1.0, 400.0)
        self.scale_input.setDecimals(1)
        self.scale_input.setValue(float(scale_percent))
        self.scale_input.valueChanged.connect(self.on_scale_changed)
        layout.addRow(self.scale_label, self.scale_input)

        self.width_label = QLabel(self.tr("Width:"))
        self.width_input = QSpinBox()
        self.width_input.setRange(1, 100000)
        self.width_input.setValue(int(self.base_width * float(scale_percent) / 100.0))
        self.width_input.valueChanged.connect(self.on_width_changed)
        layout.addRow(self.width_label, self.width_input)

        self.height_label = QLabel(self.tr("Height:"))
        self.height_input = QSpinBox()
        self.height_input.setRange(1, 100000)
        self.height_input.setValue(int(self.base_height * float(scale_percent) / 100.0))
        self.height_input.valueChanged.connect(self.on_height_changed)
        layout.addRow(self.height_label, self.height_input)

        self.quality_input = QSpinBox()
        self.quality_input.setRange(1, 10)
        self.quality_input.setValue(9)
        self.quality_label = QLabel(self.tr("JPEG quality (1-10):"))
        layout.addRow(self.quality_label, self.quality_input)

        self.on_format_changed()

        buttons = QDialogButtonBox(self)
        ok_btn = buttons.addButton(self.tr("OK"), QDialogButtonBox.AcceptRole)
        cancel_btn = buttons.addButton(self.tr("Cancel"), QDialogButtonBox.RejectRole)
        ok_btn.setDefault(True)
        ok_btn.clicked.connect(self.accept)
        cancel_btn.clicked.connect(self.reject)
        layout.addRow(buttons)

        self.setMinimumWidth(420)

    def on_scale_changed(self, value):
        if self._updating:
            return
        self._updating = True
        width = int(self.base_width * value / 100.0)
        height = int(self.base_height * value / 100.0)
        self.width_input.setValue(max(1, width))
        self.height_input.setValue(max(1, height))
        self._updating = False

    def on_width_changed(self, value):
        if self._updating or self.base_width <= 0:
            return
        self._updating = True
        scale = (value / self.base_width) * 100.0
        height = int(self.base_height * scale / 100.0)
        self.scale_input.setValue(max(1.0, scale))
        self.height_input.setValue(max(1, height))
        self._updating = False

    def on_height_changed(self, value):
        if self._updating or self.base_height <= 0:
            return
        self._updating = True
        scale = (value / self.base_height) * 100.0
        width = int(self.base_width * scale / 100.0)
        self.scale_input.setValue(max(1.0, scale))
        self.width_input.setValue(max(1, width))
        self._updating = False

    def on_format_changed(self):
        selected = self.format_input.currentData()
        if selected:
            self.format = selected
        is_jpeg = self.format == "jpg"
        is_svg = self.format == "svg"
        self.quality_input.setEnabled(is_jpeg)
        self.quality_label.setEnabled(is_jpeg)
        # SVG is vector — pixel dimensions don't apply
        for w in (
            self.scale_input, self.scale_label,
            self.width_input, self.width_label,
            self.height_input, self.height_label,
        ):
            w.setEnabled(not is_svg)

    def get_settings(self):
        return {
            "scale_percent": float(self.scale_input.value()),
            "width": int(self.width_input.value()),
            "height": int(self.height_input.value()),
            "quality": int(self.quality_input.value()) * 10,
            "format": self.format_input.currentData(),
        }


class ExportPlotDialog(QDialog):
    """Dialog to configure export format and theme for analysis plots."""

    def __init__(self, current_dark: bool = False, parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Export plot"))
        self.setWindowFlags(
            Qt.Dialog
            | Qt.CustomizeWindowHint
            | Qt.WindowTitleHint
            | Qt.WindowCloseButtonHint
        )
        self._current_dark = current_dark
        self._init_ui()

    def _init_ui(self):
        layout = QFormLayout(self)
        layout.setSpacing(8)

        self.format_input = QComboBox()
        self.format_input.addItem("SVG", "svg")
        self.format_input.addItem("PNG", "png")
        self.format_input.addItem("JPEG", "jpg")
        self.format_input.currentIndexChanged.connect(self._on_format_changed)
        layout.addRow(self.tr("Format:"), self.format_input)

        self.theme_input = QComboBox()
        current_label = self.tr("Dark (current)") if self._current_dark else self.tr("Light (current)")
        self.theme_input.addItem(current_label, "current")
        self.theme_input.addItem(self.tr("Light"), "light")
        self.theme_input.addItem(self.tr("Dark"), "dark")
        layout.addRow(self.tr("Theme:"), self.theme_input)

        self.quality_input = QSpinBox()
        self.quality_input.setRange(1, 10)
        self.quality_input.setValue(9)
        self.quality_label = QLabel(self.tr("JPEG quality (1-10):"))
        layout.addRow(self.quality_label, self.quality_input)

        self._on_format_changed()

        buttons = QDialogButtonBox(self)
        ok_btn = buttons.addButton(self.tr("OK"), QDialogButtonBox.AcceptRole)
        cancel_btn = buttons.addButton(self.tr("Cancel"), QDialogButtonBox.RejectRole)
        ok_btn.setDefault(True)
        ok_btn.clicked.connect(self.accept)
        cancel_btn.clicked.connect(self.reject)
        layout.addRow(buttons)

        self.setMinimumWidth(380)

    def _on_format_changed(self):
        is_jpeg = self.format_input.currentData() == "jpg"
        self.quality_input.setEnabled(is_jpeg)
        self.quality_label.setEnabled(is_jpeg)

    def get_settings(self):
        fmt = self.format_input.currentData()
        theme = self.theme_input.currentData()
        if theme == "current":
            theme = "dark" if self._current_dark else "light"
        return {
            "format": fmt,
            "theme": theme,
            "quality": int(self.quality_input.value()) * 10,
        }


class ExportGalleryDialog(QDialog):
    """Dialog to configure export format for gallery composite."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("Export gallery"))
        self.setWindowFlags(
            Qt.Dialog
            | Qt.CustomizeWindowHint
            | Qt.WindowTitleHint
            | Qt.WindowCloseButtonHint
        )
        self._init_ui()

    def _init_ui(self):
        layout = QFormLayout(self)
        layout.setSpacing(8)

        self.format_input = QComboBox()
        self.format_input.addItem("PNG", "png")
        self.format_input.addItem("JPEG", "jpg")
        self.format_input.addItem("SVG (lossless)", "svg")
        self.format_input.currentIndexChanged.connect(self._on_format_changed)
        layout.addRow(self.tr("Format:"), self.format_input)

        self.quality_input = QSpinBox()
        self.quality_input.setRange(1, 10)
        self.quality_input.setValue(9)
        self.quality_label = QLabel(self.tr("JPEG quality (1-10):"))
        layout.addRow(self.quality_label, self.quality_input)

        self._on_format_changed()

        buttons = QDialogButtonBox(self)
        ok_btn = buttons.addButton(self.tr("OK"), QDialogButtonBox.AcceptRole)
        cancel_btn = buttons.addButton(self.tr("Cancel"), QDialogButtonBox.RejectRole)
        ok_btn.setDefault(True)
        ok_btn.clicked.connect(self.accept)
        cancel_btn.clicked.connect(self.reject)
        layout.addRow(buttons)

        self.setMinimumWidth(360)

    def _on_format_changed(self):
        is_jpeg = self.format_input.currentData() == "jpg"
        self.quality_input.setEnabled(is_jpeg)
        self.quality_label.setEnabled(is_jpeg)

    def get_settings(self):
        return {
            "format": self.format_input.currentData(),
            "quality": int(self.quality_input.value()) * 10,
        }
