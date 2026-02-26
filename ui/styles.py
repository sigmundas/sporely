
"""Dynamic stylesheet for the application.

All font sizes are derived from the system's actual base font size so the
layout looks correct on any platform without platform-specific checks.
"""
from __future__ import annotations


def get_style() -> str:
    """Return the application stylesheet scaled to the current system font."""
    from PySide6.QtWidgets import QApplication
    app = QApplication.instance()
    base_pt = 10
    if app is not None:
        sz = app.font().pointSize()
        if sz > 0:
            base_pt = sz

    small_pt = max(base_pt - 2, 7)
    tiny_pt = max(base_pt - 3, 6)
    large_pt = base_pt + 2
    header_pt = base_pt + 2
    obj_tag_pt = base_pt + 1

    return f"""
QMainWindow {{
    background-color: #f5f5f5;
}}

QWidget {{
    font-size: {base_pt}pt;
}}

QGroupBox {{
    background-color: white;
    border: 1px solid #e0e0e0;
    border-radius: 8px;
    margin-top: 12px;
    padding-top: 12px;
    font-weight: bold;
    color: #2c3e50;
}}

QGroupBox::title {{
    subcontrol-origin: margin;
    subcontrol-position: top left;
    left: 8px;
    padding: 4px 8px;
    background-color: white;
    border-radius: 4px;
}}

QPushButton {{
    background-color: #3498db;
    color: white;
    border: none;
    border-radius: 6px;
    padding: 10px 20px;
    font-weight: bold;
    font-size: {base_pt}pt;
}}

QPushButton:hover {{
    background-color: #2980b9;
}}

QPushButton:pressed {{
    background-color: #21618c;
}}

QPushButton:disabled {{
    background-color: #bdc3c7;
    color: #7f8c8d;
}}

QPushButton#measureButton {{
    background-color: #27ae60;
}}

QPushButton#measureButton:hover {{
    background-color: #229954;
}}

QPushButton#loadButton {{
    background-color: #9b59b6;
}}

QPushButton#loadButton:hover {{
    background-color: #8e44ad;
}}

QLineEdit {{
    background-color: white;
    border: 2px solid #e0e0e0;
    border-radius: 6px;
    padding: 8px;
    font-size: {base_pt}pt;
}}

QLineEdit:focus {{
    border: 2px solid #3498db;
}}

/* Inline editors in item views need tighter padding to avoid clipped text. */
QAbstractItemView QLineEdit {{
    margin: 0px;
    padding: 1px 4px;
    border: 1px solid #6aa9e9;
    border-radius: 3px;
    background-color: white;
    color: #2c3e50;
}}

QTextEdit {{
    background-color: white;
    border: 2px solid #e0e0e0;
    border-radius: 6px;
    padding: 8px;
    font-family: 'Menlo', 'Consolas', 'Courier New', monospace;
    font-size: {small_pt}pt;
}}

QLabel {{
    color: #2c3e50;
}}

QLabel[hint_interactive="true"] {{
    color: #2c3e50;
}}

QTableView,
QTableWidget,
QTreeView,
QListView,
QListWidget {{
    selection-background-color: #d9e9f8;
    selection-color: #1f2d3d;
}}

QTableView::item:selected,
QTableWidget::item:selected,
QTreeView::item:selected,
QListView::item:selected,
QListWidget::item:selected {{
    background-color: #d9e9f8;
    color: #1f2d3d;
}}

QTableView::item:selected:!active,
QTableWidget::item:selected:!active,
QTreeView::item:selected:!active,
QListView::item:selected:!active,
QListWidget::item:selected:!active {{
    background-color: #eaf3ff;
    color: #1f2d3d;
}}

QLabel#imageLabel {{
    background-color: #ecf0f1;
    border: 2px solid #bdc3c7;
    border-radius: 8px;
}}

QLabel#headerLabel {{
    font-size: {header_pt}pt;
    font-weight: bold;
    color: #2c3e50;
}}

QLabel#objectiveTag {{
    background-color: rgba(52, 152, 219, 200);
    color: white;
    font-weight: bold;
    font-size: {obj_tag_pt}pt;
    border-radius: 6px;
    padding: 8px 12px;
}}

QMenuBar {{
    background-color: #34495e;
    color: white;
    padding: 4px;
}}

QMenuBar::item {{
    background-color: transparent;
    color: white;
    padding: 8px 12px;
}}

QMenuBar::item:selected {{
    background-color: #2c3e50;
    border-radius: 4px;
}}

QMenu {{
    background-color: white;
    border: 1px solid #e0e0e0;
    border-radius: 4px;
}}

QMenu::item {{
    padding: 8px 24px;
}}

QMenu::item:selected {{
    background-color: #3498db;
    color: white;
}}

QDialog {{
    background-color: #f5f5f5;
}}

QComboBox {{
    background-color: white;
    border: 2px solid #e0e0e0;
    border-radius: 6px;
    padding: 8px;
    font-size: {base_pt}pt;
}}

QComboBox QAbstractItemView {{
    background-color: white;
    color: #2c3e50;
    selection-background-color: #d9e9f8;
    selection-color: #1f2d3d;
}}

QComboBox QAbstractItemView::item {{
    color: #2c3e50;
    background-color: white;
}}

QComboBox QAbstractItemView::item:selected,
QComboBox QAbstractItemView::item:selected:!active,
QComboBox QAbstractItemView::item:hover {{
    background-color: #d9e9f8;
    color: #1f2d3d;
}}

QComboBoxPrivateContainer {{
    background-color: white;
    border: 1px solid #e0e0e0;
}}

QComboBoxPrivateContainer QListView {{
    background-color: white;
    color: #2c3e50;
    selection-background-color: #d9e9f8;
    selection-color: #1f2d3d;
}}

QComboBoxPrivateContainer QListView::item {{
    color: #2c3e50;
    background-color: white;
}}

QComboBoxPrivateContainer QListView::item:selected,
QComboBoxPrivateContainer QListView::item:selected:!active,
QComboBoxPrivateContainer QListView::item:hover {{
    background-color: #d9e9f8;
    color: #1f2d3d;
}}

QComboBox:focus {{
    border: 2px solid #3498db;
}}

QComboBox::drop-down {{
    border: none;
    width: 30px;
}}

QComboBox::down-arrow {{
    image: none;
    border-left: 5px solid transparent;
    border-right: 5px solid transparent;
    border-top: 6px solid #7f8c8d;
    margin-right: 8px;
}}

/* Dialog and message-box buttons: same height as regular buttons,
   but capped so they don't expand to fill wide containers. */
QMessageBox QPushButton,
QDialogButtonBox QPushButton {{
    padding: 10px 20px;
    min-width: 90px;
    max-width: 200px;
}}

QMessageBox QLabel#qt_msgbox_label,
QMessageBox QLabel#qt_msgbox_informativelabel {{
    min-width: 360px;
}}

QMessageBox QLabel#qt_msgboxex_icon_label {{
    min-width: 0px;
}}
"""


def pt(n: int) -> int:
    """Return *n* scaled from a Windows-tuned (base=10pt) value to the current
    system font size.  Use this wherever you would otherwise write a hard-coded
    point size in a ``setStyleSheet()`` call::

        label.setStyleSheet(f"color: #7f8c8d; font-size: {pt(9)}pt;")
    """
    from PySide6.QtWidgets import QApplication
    app = QApplication.instance()
    if app is None:
        return n
    base = app.font().pointSize()
    if base <= 0:
        return n
    return max(round(n * base / 10), 6)
