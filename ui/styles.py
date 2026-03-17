
"""Dynamic stylesheet for the application.

All font sizes are derived from the system's actual base font size so the
layout looks correct on any platform without platform-specific checks.

Theme support: pass theme="auto" (default), "light", or "dark" to get_style().
Auto mode detects dark/light from the system palette before any palette
override has been applied (i.e. call get_style before apply_palette, or
just rely on the stored setting).
"""
from __future__ import annotations
from pathlib import Path as _Path

# Cached native dark-mode state, captured before any palette override runs.
# Call cache_system_dark() once at startup (before _apply_light_palette).
_system_dark_cached: bool | None = None

# White checkmark SVG file — drawn on top of the blue indicator background.
_ASSETS_DIR = _Path(__file__).parent.parent / "assets" / "icons"
_CHK_URL = f"url('{(_ASSETS_DIR / 'checkmark_white.svg').as_posix()}')"


def cache_system_dark() -> None:
    """Snapshot the system dark/light state from the current palette.

    Must be called before any forced palette is applied (e.g. in main.py
    before _apply_light_palette), so "auto" theme detection is correct
    across all Qt versions.
    """
    global _system_dark_cached
    from PySide6.QtWidgets import QApplication
    app = QApplication.instance()
    if app is not None:
        _system_dark_cached = app.palette().window().color().lightness() < 128


def _is_dark(theme: str) -> bool:
    """Resolve 'auto'/'light'/'dark' to a bool. Auto reads the system palette."""
    if theme == "dark":
        return True
    if theme == "light":
        return False
    from PySide6.QtWidgets import QApplication
    from PySide6.QtCore import Qt
    app = QApplication.instance()
    # Prefer native colorScheme hint (Qt 6.5+) — unaffected by palette overrides
    # as long as setColorScheme() hasn't been called.
    if app is not None:
        try:
            scheme = app.styleHints().colorScheme()
            ColorScheme = getattr(Qt, "ColorScheme", None)
            if ColorScheme is not None:
                if scheme == ColorScheme.Dark:
                    return True
                if scheme == ColorScheme.Light:
                    return False
        except Exception:
            pass
    # Fall back to cached snapshot (taken before any forced palette override)
    if _system_dark_cached is not None:
        return _system_dark_cached
    # Last resort: current palette (may be overridden, but better than nothing)
    if app is not None:
        return app.palette().window().color().lightness() < 128
    return False


def apply_palette(theme: str = "auto") -> None:
    """Set the QApplication colour palette for the given theme.

    Call this before (or together with) setStyleSheet so that widgets that
    are not covered by QSS (e.g. QScrollArea, QTabBar, QSpinBox) also paint
    with appropriate dark/light colours via the Fusion style engine.
    """
    from PySide6.QtWidgets import QApplication
    from PySide6.QtGui import QPalette, QColor
    from PySide6.QtCore import Qt
    app = QApplication.instance()
    if app is None:
        return

    dark = _is_dark(theme)
    palette = QPalette()

    if dark:
        palette.setColor(QPalette.Window,          QColor("#1c1c1e"))
        palette.setColor(QPalette.WindowText,      QColor("#e8e8e8"))
        palette.setColor(QPalette.Base,            QColor("#2b2b2d"))
        palette.setColor(QPalette.AlternateBase,   QColor("#333335"))
        palette.setColor(QPalette.ToolTipBase,     QColor("#2b2b2d"))
        palette.setColor(QPalette.ToolTipText,     QColor("#e8e8e8"))
        palette.setColor(QPalette.Text,            QColor("#e8e8e8"))
        palette.setColor(QPalette.Button,          QColor("#3a3a3c"))
        palette.setColor(QPalette.ButtonText,      QColor("#e8e8e8"))
        palette.setColor(QPalette.BrightText,      QColor("#ffffff"))
        palette.setColor(QPalette.Mid,             QColor("#4a4a4c"))
        palette.setColor(QPalette.Dark,            QColor("#555557"))
        palette.setColor(QPalette.Light,           QColor("#3a3a3c"))
        palette.setColor(QPalette.Highlight,       QColor("#1c3a5e"))
        palette.setColor(QPalette.HighlightedText, QColor("#c0deff"))
        palette.setColor(QPalette.PlaceholderText, QColor("#8e8e93"))
        palette.setColor(QPalette.Disabled, QPalette.WindowText, QColor("#636366"))
        palette.setColor(QPalette.Disabled, QPalette.Text,       QColor("#636366"))
        palette.setColor(QPalette.Disabled, QPalette.ButtonText, QColor("#636366"))
        palette.setColor(QPalette.Disabled, QPalette.Base,       QColor("#333335"))
        palette.setColor(QPalette.Disabled, QPalette.Button,     QColor("#333335"))
    else:
        palette.setColor(QPalette.Window,          QColor("#f5f5f5"))
        palette.setColor(QPalette.WindowText,      QColor("#2c3e50"))
        palette.setColor(QPalette.Base,            QColor("#ffffff"))
        palette.setColor(QPalette.AlternateBase,   QColor("#f5f5f5"))
        palette.setColor(QPalette.ToolTipBase,     QColor("#ffffff"))
        palette.setColor(QPalette.ToolTipText,     QColor("#2c3e50"))
        palette.setColor(QPalette.Text,            QColor("#2c3e50"))
        palette.setColor(QPalette.Button,          QColor("#f5f5f5"))
        palette.setColor(QPalette.ButtonText,      QColor("#2c3e50"))
        palette.setColor(QPalette.BrightText,      QColor("white"))
        palette.setColor(QPalette.Mid,             QColor("#d0d7de"))
        palette.setColor(QPalette.Dark,            QColor("#b0bec5"))
        palette.setColor(QPalette.Light,           QColor("#ffffff"))
        palette.setColor(QPalette.Highlight,       QColor("#3498db"))
        palette.setColor(QPalette.HighlightedText, QColor("white"))
        palette.setColor(QPalette.PlaceholderText, QColor("#7f8c8d"))
        palette.setColor(QPalette.Disabled, QPalette.WindowText, QColor("#95a5a6"))
        palette.setColor(QPalette.Disabled, QPalette.Text,       QColor("#95a5a6"))
        palette.setColor(QPalette.Disabled, QPalette.ButtonText, QColor("#95a5a6"))
        palette.setColor(QPalette.Disabled, QPalette.Base,       QColor("#eceff1"))
        palette.setColor(QPalette.Disabled, QPalette.Button,     QColor("#eceff1"))

    app.setPalette(palette)

    # Qt 6 color-scheme hint: tells Qt which native decorations to use.
    try:
        color_scheme_enum = getattr(Qt, "ColorScheme", None)
        set_color_scheme = getattr(app.styleHints(), "setColorScheme", None)
        if color_scheme_enum is not None and callable(set_color_scheme):
            scheme = color_scheme_enum.Dark if dark else color_scheme_enum.Light
            set_color_scheme(scheme)
    except Exception:
        pass


def get_style(theme: str = "auto") -> str:
    """Return the application stylesheet scaled to the current system font.

    Args:
        theme: "auto" (detect from system palette), "light", or "dark".
    """
    from PySide6.QtWidgets import QApplication
    app = QApplication.instance()
    base_pt = 10
    if app is not None:
        sz = app.font().pointSize()
        if sz > 0:
            base_pt = sz

    small_pt   = max(base_pt - 2, 7)
    header_pt  = base_pt + 2
    obj_tag_pt = base_pt + 1

    dark = _is_dark(theme)

    if dark:
        bg         = "#1c1c1e"
        surface    = "#2b2b2d"
        border     = "#3a3a3c"
        brd_focus  = "#4a90d9"
        text       = "#e8e8e8"
        text_dim   = "#8e8e93"
        accent     = "#4a90d9"
        accent_h   = "#3a7bc8"
        accent_p   = "#2a5fa0"
        dis_bg     = "#3a3a3c"
        dis_fg     = "#636366"
        menubar_bg = "#1c1c1e"
        menubar_h  = "#2c2c2e"
        img_bg     = "#2b2b2d"
        img_brd    = "#3a3a3c"
        sel_bg     = "#1c3a5e"
        sel_fg     = "#c0deff"
        sel_inact  = "#172a42"
        dlg_border = "#3a3a3c"
        inline_brd = "#4a90d9"
        indicator_border = "#8e8e93"
        indicator_bg = "#232325"
        indicator_disabled = "#555557"
        outline_btn_bg = "rgba(74,144,217,0.13)"  # subtle blue tint, matches light mode approach
        data_brd   = "#585860"   # more visible than border in dark
        data_fg    = "#a8a8b0"   # slightly brighter than text_dim
    else:
        bg         = "#f5f5f5"
        surface    = "white"
        border     = "#e0e0e0"
        brd_focus  = "#3498db"
        text       = "#2c3e50"
        text_dim   = "#7f8c8d"
        accent     = "#3498db"
        accent_h   = "#2980b9"
        accent_p   = "#21618c"
        dis_bg     = "#bdc3c7"
        dis_fg     = "#7f8c8d"
        menubar_bg = "#34495e"
        menubar_h  = "#2c3e50"
        img_bg     = "#ecf0f1"
        img_brd    = "#bdc3c7"
        sel_bg     = "#d9e9f8"
        sel_fg     = "#1f2d3d"
        sel_inact  = "#eaf3ff"
        dlg_border = "#c7d0da"
        inline_brd = "#6aa9e9"
        indicator_border = "#7f8c8d"
        indicator_bg = "#ffffff"
        indicator_disabled = "#bdc3c7"
        outline_btn_bg = "rgba(52,152,219,0.10)"  # subtle tint so white icons stay visible
        data_brd   = border
        data_fg    = text_dim

    chk_url = _CHK_URL

    return f"""
QMainWindow {{
    background-color: {bg};
}}

QWidget {{
    font-size: {base_pt}pt;
}}

QGroupBox {{
    background-color: {surface};
    border: 1px solid {border};
    border-radius: 8px;
    margin-top: 12px;
    padding-top: 12px;
    font-weight: bold;
    color: {text};
}}

QGroupBox::title {{
    subcontrol-origin: margin;
    subcontrol-position: top left;
    left: 8px;
    padding: 4px 8px;
    background-color: {surface};
    border-radius: 4px;
}}

QPushButton {{
    background-color: {accent};
    color: white;
    border: none;
    border-radius: 6px;
    padding: 6px 10px;
    font-weight: bold;
    font-size: {base_pt}pt;
}}

QPushButton:hover {{
    background-color: {accent_h};
}}

QPushButton:pressed {{
    background-color: {accent_p};
}}

QPushButton:disabled {{
    background-color: {dis_bg};
    color: {dis_fg};
}}

QPushButton#outlineButton {{
    background-color: {outline_btn_bg};
    color: {accent};
    border: 2px solid {accent};
    border-radius: 6px;
    padding: 5px 10px;
    font-weight: bold;
}}

QPushButton#outlineButton:hover {{
    background-color: {accent};
    color: white;
}}

QPushButton#outlineButton:pressed {{
    background-color: {accent_p};
    border-color: {accent_p};
    color: white;
}}

QPushButton#outlineButton:disabled {{
    background-color: transparent;
    color: {dis_fg};
    border: 2px solid {dis_fg};
}}

QPushButton#dataButton {{
    background-color: transparent;
    color: {data_fg};
    border: 1px solid {data_brd};
    border-radius: 6px;
    padding: 4px 8px;
    font-weight: normal;
}}

QPushButton#dataButton:hover {{
    color: {text};
    border-color: {text_dim};
}}

QPushButton#dataButton:pressed {{
    background-color: {dis_bg};
}}

QPushButton#dataButton:disabled {{
    color: {dis_fg};
    border-color: {dis_bg};
}}

QPushButton#destructiveButton {{
    background-color: transparent;
    color: #c0392b;
    border: 1px solid #c0392b;
    border-radius: 6px;
    padding: 4px 8px;
    font-weight: normal;
}}

QPushButton#destructiveButton:hover {{
    background-color: #c0392b;
    color: white;
}}

QPushButton#destructiveButton:pressed {{
    background-color: #a93226;
    border-color: #a93226;
    color: white;
}}

QPushButton#destructiveButton:disabled {{
    color: {dis_fg};
    border-color: {dis_bg};
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
    background-color: {surface};
    border: 2px solid {border};
    border-radius: 6px;
    padding: 5px 6px;
    font-size: {base_pt}pt;
    color: {text};
}}

QLineEdit:focus {{
    border: 2px solid {brd_focus};
}}

/* Inline editors in item views need tighter padding to avoid clipped text. */
QAbstractItemView QLineEdit {{
    margin: 0px;
    padding: 1px 4px;
    border: 1px solid {inline_brd};
    border-radius: 3px;
    background-color: {surface};
    color: {text};
}}

QTextEdit {{
    background-color: {surface};
    border: 2px solid {border};
    border-radius: 6px;
    padding: 5px 6px;
    font-family: 'Menlo', 'Consolas', 'Courier New', monospace;
    font-size: {small_pt}pt;
    color: {text};
}}

QLabel {{
    color: {text};
}}

QLabel[hint_interactive="true"] {{
    color: {text};
}}

QCheckBox {{
    color: {text};
    spacing: 8px;
}}

QCheckBox::indicator {{
    width: 16px;
    height: 16px;
    border: 2px solid {indicator_border};
    border-radius: 4px;
    background-color: {indicator_bg};
}}

QCheckBox::indicator:hover {{
    border: 2px solid {accent};
    border-radius: 4px;
}}

QCheckBox::indicator:checked {{
    background-color: {accent};
    border: 2px solid {accent};
    border-radius: 4px;
    image: {chk_url};
}}

QCheckBox::indicator:checked:disabled {{
    background-color: {dis_bg};
    border: 2px solid {indicator_disabled};
    border-radius: 4px;
    image: {chk_url};
}}

QCheckBox::indicator:unchecked {{
    background-color: {indicator_bg};
    border: 2px solid {indicator_border};
    border-radius: 4px;
}}

QCheckBox::indicator:unchecked:disabled {{
    background-color: {dis_bg};
    border: 2px solid {indicator_disabled};
    border-radius: 4px;
}}

QRadioButton {{
    color: {text};
    spacing: 8px;
}}

QRadioButton::indicator {{
    width: 16px;
    height: 16px;
    border: 2px solid {indicator_border};
    border-radius: 8px;
    background-color: {indicator_bg};
}}

QRadioButton::indicator:hover {{
    border: 2px solid {accent};
    border-radius: 8px;
}}

QRadioButton::indicator:checked {{
    background-color: {accent};
    border: 4px solid {indicator_bg};
    border-radius: 8px;
}}

QRadioButton::indicator:unchecked {{
    background-color: {indicator_bg};
    border: 2px solid {indicator_border};
    border-radius: 8px;
}}

QRadioButton::indicator:disabled {{
    background-color: {dis_bg};
    border: 2px solid {indicator_disabled};
    border-radius: 8px;
}}

QTableView,
QTableWidget,
QTreeView,
QListView,
QListWidget {{
    selection-background-color: {sel_bg};
    selection-color: {sel_fg};
}}

QTableView::item:selected,
QTableWidget::item:selected,
QTreeView::item:selected,
QListView::item:selected,
QListWidget::item:selected {{
    background-color: {sel_bg};
    color: {sel_fg};
}}

QTableView::item:selected:!active,
QTableWidget::item:selected:!active,
QTreeView::item:selected:!active,
QListView::item:selected:!active,
QListWidget::item:selected:!active {{
    background-color: {sel_inact};
    color: {sel_fg};
}}

QHeaderView::section {{
    background-color: {surface};
    color: {text};
    border: none;
    border-bottom: 1px solid {border};
    border-right: 1px solid {border};
    padding: 4px 8px;
    font-weight: bold;
}}

QHeaderView::section:last {{
    border-right: none;
}}

QTableView::item,
QTableWidget::item {{
    color: {text};
    padding: 2px 4px;
}}

QTableView::item:alternate,
QTableWidget::item:alternate {{
    background-color: {surface};
}}

QLabel#imageLabel {{
    background-color: {img_bg};
    border: 2px solid {img_brd};
    border-radius: 8px;
}}

QLabel#headerLabel {{
    font-size: {header_pt}pt;
    font-weight: bold;
    color: {text};
}}

QLabel#observationHeaderLabel {{
    font-size: {header_pt + 1}pt;
    font-weight: bold;
    color: {text};
    padding: 2px 0px 4px 0px;
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
    background-color: {menubar_bg};
    color: white;
    padding: 4px;
}}

QMenuBar::item {{
    background-color: transparent;
    color: white;
    padding: 8px 12px;
}}

QMenuBar::item:selected {{
    background-color: {menubar_h};
    border-radius: 4px;
}}

QMenu {{
    background-color: {surface};
    border: 1px solid {border};
    border-radius: 4px;
    color: {text};
}}

QMenu::item {{
    padding: 8px 24px;
    color: {text};
}}

QMenu::item:selected {{
    background-color: {accent};
    color: white;
}}

QDialog {{
    background-color: {bg};
    border: 2px solid {dlg_border};
    border-radius: 10px;
}}

QMessageBox {{
    background-color: {bg};
    border: 2px solid {dlg_border};
    border-radius: 10px;
}}

QComboBox {{
    background-color: {surface};
    border: 2px solid {border};
    border-radius: 6px;
    padding: 5px 6px;
    font-size: {base_pt}pt;
    color: {text};
}}

QComboBox QAbstractItemView {{
    background-color: {surface};
    color: {text};
    selection-background-color: {sel_bg};
    selection-color: {sel_fg};
}}

QComboBox QAbstractItemView::item {{
    color: {text};
    background-color: {surface};
}}

QComboBox QAbstractItemView::item:selected,
QComboBox QAbstractItemView::item:selected:!active,
QComboBox QAbstractItemView::item:hover {{
    background-color: {sel_bg};
    color: {sel_fg};
}}

QComboBoxPrivateContainer {{
    background-color: {surface};
    border: 1px solid {border};
}}

QComboBoxPrivateContainer QListView {{
    background-color: {surface};
    color: {text};
    selection-background-color: {sel_bg};
    selection-color: {sel_fg};
}}

QComboBoxPrivateContainer QListView::item {{
    color: {text};
    background-color: {surface};
}}

QComboBoxPrivateContainer QListView::item:selected,
QComboBoxPrivateContainer QListView::item:selected:!active,
QComboBoxPrivateContainer QListView::item:hover {{
    background-color: {sel_bg};
    color: {sel_fg};
}}

QComboBox:focus {{
    border: 2px solid {brd_focus};
}}

QComboBox::drop-down {{
    border: none;
    width: 30px;
}}

QComboBox::down-arrow {{
    image: none;
    border-left: 5px solid transparent;
    border-right: 5px solid transparent;
    border-top: 6px solid {text_dim};
    margin-right: 8px;
}}

/* Dialog and message-box buttons: same height as regular buttons,
   but capped so they don't expand to fill wide containers. */
QMessageBox QPushButton,
QDialogButtonBox QPushButton {{
    padding: 6px 10px;
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

/* CollapsibleSection toggle button — theme-aware */
QToolButton#collapsibleToggle {{
    font-weight: bold;
    padding: 6px 8px;
    background-color: transparent;
    color: {text};
    border: none;
    text-align: left;
}}

QToolButton#collapsibleToggle:hover {{
    background-color: {border};
    border-radius: 4px;
}}

QToolButton#collapsibleToggle:checked {{
    background-color: {surface};
    color: {text};
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
