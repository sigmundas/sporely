
"""Modern stylesheet for the application."""
import sys

MODERN_STYLE = """
QMainWindow {
    background-color: #f5f5f5;
}

QWidget {
    font-family: 'Segoe UI', Arial, sans-serif;
    font-size: 10pt;
}

QGroupBox {
    background-color: white;
    border: 1px solid #e0e0e0;
    border-radius: 8px;
    margin-top: 12px;
    padding-top: 12px;
    font-weight: bold;
    color: #2c3e50;
}

QGroupBox::title {
    subcontrol-origin: margin;
    subcontrol-position: top left;
    padding: 4px 8px;
    background-color: white;
    border-radius: 4px;
}

QPushButton {
    background-color: #3498db;
    color: white;
    border: none;
    border-radius: 6px;
    padding: 10px 20px;
    font-weight: bold;
    font-size: 10pt;
}

QPushButton:hover {
    background-color: #2980b9;
}

QPushButton:pressed {
    background-color: #21618c;
}

QPushButton:disabled {
    background-color: #bdc3c7;
    color: #7f8c8d;
}

QPushButton#measureButton {
    background-color: #27ae60;
}

QPushButton#measureButton:hover {
    background-color: #229954;
}

QPushButton#loadButton {
    background-color: #9b59b6;
}

QPushButton#loadButton:hover {
    background-color: #8e44ad;
}

QLineEdit {
    background-color: white;
    border: 2px solid #e0e0e0;
    border-radius: 6px;
    padding: 8px;
    font-size: 10pt;
}

QLineEdit:focus {
    border: 2px solid #3498db;
}

/* Inline editors in item views need tighter padding to avoid clipped text. */
QAbstractItemView QLineEdit {
    margin: 0px;
    padding: 1px 4px;
    border: 1px solid #6aa9e9;
    border-radius: 3px;
    background-color: white;
    color: #2c3e50;
}

QTextEdit {
    background-color: white;
    border: 2px solid #e0e0e0;
    border-radius: 6px;
    padding: 8px;
    font-family: 'Consolas', 'Courier New', monospace;
    font-size: 9pt;
}

QLabel {
    color: #2c3e50;
}

QLabel[hint_interactive="true"] {
    color: #2c3e50;
}

QTableView,
QTableWidget,
QTreeView,
QListView,
QListWidget {
    selection-background-color: #d9e9f8;
    selection-color: #1f2d3d;
}

QTableView::item:selected,
QTableWidget::item:selected,
QTreeView::item:selected,
QListView::item:selected,
QListWidget::item:selected {
    background-color: #d9e9f8;
    color: #1f2d3d;
}

QTableView::item:selected:!active,
QTableWidget::item:selected:!active,
QTreeView::item:selected:!active,
QListView::item:selected:!active,
QListWidget::item:selected:!active {
    background-color: #eaf3ff;
    color: #1f2d3d;
}

QLabel#imageLabel {
    background-color: #ecf0f1;
    border: 2px solid #bdc3c7;
    border-radius: 8px;
}

QLabel#headerLabel {
    font-size: 12pt;
    font-weight: bold;
    color: #2c3e50;
}

QLabel#objectiveTag {
    background-color: rgba(52, 152, 219, 200);
    color: white;
    font-weight: bold;
    font-size: 11pt;
    border-radius: 6px;
    padding: 8px 12px;
}

QMenuBar {
    background-color: #34495e;
    color: white;
    padding: 4px;
}

QMenuBar::item {
    background-color: transparent;
    color: white;
    padding: 8px 12px;
}

QMenuBar::item:selected {
    background-color: #2c3e50;
    border-radius: 4px;
}

QMenu {
    background-color: white;
    border: 1px solid #e0e0e0;
    border-radius: 4px;
}

QMenu::item {
    padding: 8px 24px;
}

QMenu::item:selected {
    background-color: #3498db;
    color: white;
}

QDialog {
    background-color: #f5f5f5;
}

QComboBox {
    background-color: white;
    border: 2px solid #e0e0e0;
    border-radius: 6px;
    padding: 8px;
    font-size: 10pt;
}

QComboBox QAbstractItemView {
    background-color: white;
    color: #2c3e50;
    selection-background-color: #d9e9f8;
    selection-color: #1f2d3d;
}

QComboBox QAbstractItemView::item {
    color: #2c3e50;
    background-color: white;
}

QComboBox QAbstractItemView::item:selected,
QComboBox QAbstractItemView::item:selected:!active,
QComboBox QAbstractItemView::item:hover {
    background-color: #d9e9f8;
    color: #1f2d3d;
}

QComboBoxPrivateContainer {
    background-color: white;
    border: 1px solid #e0e0e0;
}

QComboBoxPrivateContainer QListView {
    background-color: white;
    color: #2c3e50;
    selection-background-color: #d9e9f8;
    selection-color: #1f2d3d;
}

QComboBoxPrivateContainer QListView::item {
    color: #2c3e50;
    background-color: white;
}

QComboBoxPrivateContainer QListView::item:selected,
QComboBoxPrivateContainer QListView::item:selected:!active,
QComboBoxPrivateContainer QListView::item:hover {
    background-color: #d9e9f8;
    color: #1f2d3d;
}

QComboBox:focus {
    border: 2px solid #3498db;
}

QComboBox::drop-down {
    border: none;
    width: 30px;
}

QComboBox::down-arrow {
    image: none;
    border-left: 5px solid transparent;
    border-right: 5px solid transparent;
    border-top: 6px solid #7f8c8d;
    margin-right: 8px;
}
"""

if sys.platform.startswith("linux"):
    MODERN_STYLE += """
QWidget {
    font-family: 'Noto Sans', 'DejaVu Sans', Arial, sans-serif;
    font-size: 9pt;
}

QPushButton {
    padding: 8px 14px;
    font-size: 9pt;
}

QMessageBox QPushButton,
QDialogButtonBox QPushButton {
    min-width: 120px;
    min-height: 35px;
    padding: 6px 12px;
}

QMessageBox QLabel#qt_msgbox_label,
QMessageBox QLabel#qt_msgbox_informativelabel {
    min-width: 360px;
}

QMessageBox QLabel#qt_msgboxex_icon_label {
    min-width: 0px;
}
"""
