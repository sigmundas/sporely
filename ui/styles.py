
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
        # Clinical Nocturne
        palette.setColor(QPalette.Window,          QColor("#131313"))
        palette.setColor(QPalette.WindowText,      QColor("#e8e8e8"))
        palette.setColor(QPalette.Base,            QColor("#1c1b1b"))
        palette.setColor(QPalette.AlternateBase,   QColor("#252423"))
        palette.setColor(QPalette.ToolTipBase,     QColor("#1c1b1b"))
        palette.setColor(QPalette.ToolTipText,     QColor("#e8e8e8"))
        palette.setColor(QPalette.Text,            QColor("#e8e8e8"))
        palette.setColor(QPalette.Button,          QColor("#1c1b1b"))
        palette.setColor(QPalette.ButtonText,      QColor("#e8e8e8"))
        palette.setColor(QPalette.BrightText,      QColor("#ffffff"))
        palette.setColor(QPalette.Mid,             QColor("#353534"))
        palette.setColor(QPalette.Dark,            QColor("#252423"))
        palette.setColor(QPalette.Light,           QColor("#252423"))
        palette.setColor(QPalette.Highlight,       QColor("#3d5a52"))
        palette.setColor(QPalette.HighlightedText, QColor("#e8e8e8"))
        palette.setColor(QPalette.PlaceholderText, QColor("#c1c8c4"))
        palette.setColor(QPalette.Disabled, QPalette.WindowText, QColor("#6b7270"))
        palette.setColor(QPalette.Disabled, QPalette.Text,       QColor("#6b7270"))
        palette.setColor(QPalette.Disabled, QPalette.ButtonText, QColor("#6b7270"))
        palette.setColor(QPalette.Disabled, QPalette.Base,       QColor("#353534"))
        palette.setColor(QPalette.Disabled, QPalette.Button,     QColor("#353534"))
    else:
        # Slate Lab
        palette.setColor(QPalette.Window,          QColor("#f8f9fa"))
        palette.setColor(QPalette.WindowText,      QColor("#2b3437"))
        palette.setColor(QPalette.Base,            QColor("#ffffff"))
        palette.setColor(QPalette.AlternateBase,   QColor("#f0f1f2"))
        palette.setColor(QPalette.ToolTipBase,     QColor("#ffffff"))
        palette.setColor(QPalette.ToolTipText,     QColor("#2b3437"))
        palette.setColor(QPalette.Text,            QColor("#2b3437"))
        palette.setColor(QPalette.Button,          QColor("#f0f1f2"))
        palette.setColor(QPalette.ButtonText,      QColor("#2b3437"))
        palette.setColor(QPalette.BrightText,      QColor("white"))
        palette.setColor(QPalette.Mid,             QColor("#e0e2e3"))
        palette.setColor(QPalette.Dark,            QColor("#e0e0e0"))
        palette.setColor(QPalette.Light,           QColor("#ffffff"))
        palette.setColor(QPalette.Highlight,       QColor("#defff4"))
        palette.setColor(QPalette.HighlightedText, QColor("#2b3437"))
        palette.setColor(QPalette.PlaceholderText, QColor("#8a9490"))
        palette.setColor(QPalette.Disabled, QPalette.WindowText, QColor("#8a9490"))
        palette.setColor(QPalette.Disabled, QPalette.Text,       QColor("#8a9490"))
        palette.setColor(QPalette.Disabled, QPalette.ButtonText, QColor("#8a9490"))
        palette.setColor(QPalette.Disabled, QPalette.Base,       QColor("#e8edeb"))
        palette.setColor(QPalette.Disabled, QPalette.Button,     QColor("#e8edeb"))

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
        # Clinical Nocturne — atmospheric, high-contrast, lab-optimised
        bg           = "#131313"         # deep charcoal background
        surface      = "#1c1b1b"         # slightly lighter charcoal for cards
        surface_low   = "#1c1b1b"         # tab bar, header row, side panel
        surface_hover = "#252423"         # hover on surface_low elements
        input_bg      = "#252423"         # input fields (just visible lift)
        brd_focus    = "#aecdc3"         # soft mint focus ring
        text         = "#e8e8e8"         # off-white primary text
        text_dim     = "#c1c8c4"         # muted sage-grey secondary text
        accent       = "#2e5c35"         # mid forest green — button background
        accent_h     = "#3a7042"         # slightly lighter hover
        accent_p     = "#3d5a52"         # deep teal pressed
        dis_bg       = "#353534"         # dark grey disabled bg
        dis_fg       = "#6b7270"         # muted disabled text
        menubar_bg   = "#131313"
        menubar_h    = "#1c1b1b"
        img_bg       = "#1c1b1b"
        sel_bg       = "#3d5a52"         # deep forest green selection
        sel_fg       = "#e8e8e8"
        sel_inact    = "#2a3c38"
        inline_brd   = "#aecdc3"
        indicator_border   = "#aecdc3"   # mint indicator/checkbox border
        indicator_checked  = "#aecdc3"   # mint fill for checked state
        indicator_bg       = "#1c1b1b"
        indicator_disabled = "#353534"
        data_brd     = "#353534"
        data_fg      = "#c1c8c4"
    else:
        # Slate Lab — technical, clinical, cool off-white
        bg           = "#f8f9fa"         # cool off-white canvas
        surface      = "#ffffff"         # pure white for cards/inputs
        surface_low   = "#f0f1f2"         # light grey tonal shift (tab bar, headers, side panel)
        surface_hover = "#e0e2e3"         # hover on surface_low elements
        input_bg      = "#e8edeb"         # soft input background
        brd_focus    = "#47645c"         # desaturated slate green
        text         = "#2b3437"         # dark slate grey primary text
        text_dim     = "#586064"         # medium grey secondary text
        accent       = "#47645c"         # desaturated slate green
        accent_h     = "#3a5450"         # hover: slightly darker
        accent_p     = "#2e4440"         # pressed: deepest
        dis_bg       = "#cdd5d2"         # muted disabled background
        dis_fg       = "#8a9490"         # muted disabled text
        menubar_bg   = "#47645c"
        menubar_h    = "#3a5450"
        img_bg       = "#f0f1f2"         # matches surface_low
        sel_bg       = "#defff4"         # very light mint selection
        sel_fg       = "#2b3437"
        sel_inact    = "#e8f8f4"         # lighter inactive selection
        inline_brd   = "#47645c"
        indicator_border   = "#586064"
        indicator_checked  = "#47645c"   # slate green fill for checked state
        indicator_bg       = "#ffffff"
        indicator_disabled = "#cdd5d2"
        data_brd     = "#e0e0e0"         # neutral grey divider
        data_fg      = "#586064"

    chk_url = _CHK_URL

    return f"""
QMainWindow {{
    background-color: {bg};
}}

QWidget {{
    font-family: 'Inter 18pt', '-apple-system', 'Segoe UI', sans-serif;
    font-size: {base_pt}pt;
}}

/* ── Seamless tab navigation ──────────────────────────────────────── */
/* pane background must match the selected tab background so the border
   between them disappears. margin-top: -1px hides the default separator. */
QTabWidget::pane {{
    border: none;
    background-color: {bg};
    margin-top: -1px;
}}

QTabWidget::tab-bar {{
    alignment: left;
}}

QTabBar {{
    background-color: {surface_low};
}}

QTabBar::tab {{
    background-color: {surface_low};
    color: {text_dim};
    border: none;
    border-bottom: 3px solid transparent;
    padding: 10px 20px;
    font-family: 'Manrope', 'SF Pro Display', 'Segoe UI', sans-serif;
    font-weight: 700;
    font-size: {base_pt}pt;
    min-width: 80px;
}}

QTabBar::tab:selected {{
    background-color: {bg};
    color: {text};
    border-bottom: 3px solid {accent};
}}

QTabBar::tab:hover:!selected {{
    background-color: {surface_hover};
    color: {text};
}}

QGroupBox {{
    background-color: {surface};
    border: none;
    border-radius: 8px;
    margin-top: 16px;
    padding-top: 16px;
    padding-left: 16px;
    padding-right: 16px;
    padding-bottom: 16px;
    font-family: 'Manrope', 'SF Pro Display', 'Segoe UI', sans-serif;
    font-weight: 700;
    color: {text};
}}

QGroupBox::title {{
    subcontrol-origin: margin;
    subcontrol-position: top left;
    left: 12px;
    padding: 4px 8px;
    background-color: {surface};
    border-radius: 4px;
}}

QPushButton {{
    background-color: qlineargradient(x1:0, y1:0, x2:1, y2:1,
        stop:0 {accent}, stop:1 {accent_h});
    color: white;
    border: none;
    border-radius: 8px;
    padding: 6px 16px;
    font-weight: bold;
    font-size: {base_pt}pt;
}}

QPushButton:hover {{
    background-color: {accent_h};
}}

QPushButton:pressed {{
    background-color: {accent_p};
    padding: 7px 15px 5px 17px;
}}

QPushButton:disabled {{
    background-color: {dis_bg};
    color: {dis_fg};
}}

QPushButton#outlineButton {{
    background-color: transparent;
    color: {accent};
    border: 1.5px solid {accent};
    border-radius: 8px;
    padding: 5px 16px;
    font-weight: bold;
}}

QPushButton#outlineButton:hover {{
    background-color: {sel_bg};
    color: {accent};
}}

QPushButton#outlineButton:pressed {{
    background-color: {accent_p};
    border-color: {accent_p};
    color: white;
}}

QPushButton#outlineButton:disabled {{
    background-color: transparent;
    color: {dis_fg};
    border: 1.5px solid {dis_fg};
}}

QPushButton#dataButton {{
    background-color: transparent;
    color: {data_fg};
    border: 1px solid {data_brd};
    border-radius: 8px;
    padding: 4px 12px;
    font-weight: normal;
}}

QPushButton#dataButton:hover {{
    background-color: {input_bg};
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
    color: #a73b21;
    border: 1px solid #a73b21;
    border-radius: 8px;
    padding: 4px 12px;
    font-weight: normal;
}}

QPushButton#destructiveButton:hover {{
    background-color: #a73b21;
    color: white;
}}

QPushButton#destructiveButton:pressed {{
    background-color: #8a2f18;
    border-color: #8a2f18;
    color: white;
}}

QPushButton#destructiveButton:disabled {{
    color: {dis_fg};
    border-color: {dis_bg};
}}

QPushButton#measureButton {{
    background-color: qlineargradient(x1:0, y1:0, x2:1, y2:1,
        stop:0 {accent}, stop:1 {accent_h});
}}

QPushButton#measureButton:hover {{
    background-color: {accent_h};
}}

QPushButton#loadButton {{
    background-color: #4f7080;
}}

QPushButton#loadButton:hover {{
    background-color: #3d5a68;
}}

QPushButton[sourceActive="true"] {{
    background-color: {sel_bg};
    color: {text};
    font-weight: bold;
}}

QLabel#sectionHeader {{
    font-family: 'Manrope', 'SF Pro Display', 'Segoe UI', sans-serif;
    font-weight: 700;
    font-size: {base_pt}pt;
    color: {text_dim};
    padding: 4px 0px 2px 0px;
    letter-spacing: 0.04em;
}}

QLineEdit {{
    background-color: {input_bg};
    border: none;
    border-radius: 8px;
    border-bottom: 2px solid transparent;
    padding: 5px 8px;
    font-size: {base_pt}pt;
    color: {text};
}}

QLineEdit:focus {{
    background-color: {sel_inact};
    border-bottom: 2px solid {brd_focus};
}}

/* Inline editors in item views need tighter padding to avoid clipped text. */
QAbstractItemView QLineEdit {{
    margin: 0px;
    padding: 1px 4px;
    border: none;
    border-bottom: 1px solid {inline_brd};
    border-radius: 3px;
    background-color: {surface};
    color: {text};
}}

QTextEdit {{
    background-color: {input_bg};
    border: none;
    border-radius: 8px;
    padding: 5px 8px;
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
    background-color: {indicator_checked};
    border: 2px solid {indicator_checked};
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
    background-color: {indicator_checked};
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
    border: none;
    gridline-color: transparent;
    background-color: {surface};
    alternate-background-color: {surface};
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
    background-color: {surface_low};
    color: {text_dim};
    border: none;
    padding: 8px 12px;
    font-family: 'Inter 18pt', '-apple-system', 'Segoe UI', sans-serif;
    font-size: {small_pt}pt;
    font-weight: 600;
    letter-spacing: 0.05em;
    text-transform: uppercase;
}}

QHeaderView::section:last {{
    border-right: none;
}}

QTableView::item,
QTableWidget::item {{
    color: {text};
    padding: 16px 12px;
}}

/* Remove the focus rectangle Qt draws around the active cell. */
QTableView::item:focus,
QTableWidget::item:focus {{
    outline: none;
    border: none;
}}

/* Reset calendar cell padding — the calendar uses QTableView internally
   and must not inherit the 16px item padding from the rule above. */
QCalendarWidget QTableView::item {{
    padding: 2px 4px;
}}

QCalendarWidget QHeaderView,
QCalendarWidget QHeaderView::section {{
    background-color: transparent;
    color: {text};
    border: none;
    padding: 1px 2px;
    font-family: 'Inter 18pt', '-apple-system', 'Segoe UI', sans-serif;
    font-size: {base_pt}pt;
    font-weight: normal;
}}

QTableView::item:alternate,
QTableWidget::item:alternate {{
    background-color: {surface};
}}

QLabel#imageLabel {{
    background-color: {img_bg};
    border: none;
    border-radius: 6px;
}}

QLabel#headerLabel {{
    font-family: 'Manrope', 'SF Pro Display', 'Segoe UI', sans-serif;
    font-size: {header_pt}pt;
    font-weight: 700;
    color: {text};
}}

QLabel#observationHeaderLabel {{
    font-family: 'Manrope', 'SF Pro Display', 'Segoe UI', sans-serif;
    font-size: {header_pt + 1}pt;
    font-weight: 700;
    color: {text};
    padding: 2px 0px 4px 0px;
}}

/* Metadata label: all-caps small Inter for technical fields (genus, latitude, etc.) */
QLabel#metaLabel {{
    font-family: 'Inter 18pt', '-apple-system', 'Segoe UI', sans-serif;
    font-size: {small_pt}pt;
    font-weight: 600;
    color: {text_dim};
    letter-spacing: 0.05em;
    text-transform: uppercase;
}}

QLabel#objectiveTag {{
    background-color: rgba(71, 103, 74, 200);
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
    border: none;
    border-radius: 8px;
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
    border: none;
    border-radius: 12px;
}}

QMessageBox {{
    background-color: {bg};
    border: none;
    border-radius: 12px;
}}

QComboBox {{
    background-color: {input_bg};
    border: none;
    border-radius: 8px;
    border-bottom: 2px solid transparent;
    padding: 5px 8px;
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
    border: none;
    border-radius: 8px;
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
    background-color: {sel_inact};
    border-bottom: 2px solid {brd_focus};
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

/* ── SpinBox — hide up/down arrows, render as plain text inputs ─── */
QSpinBox,
QDoubleSpinBox {{
    background-color: {input_bg};
    border: none;
    border-radius: 8px;
    border-bottom: 2px solid transparent;
    padding: 5px 8px;
    padding-right: 2px;
    font-size: {base_pt}pt;
    color: {text};
}}

QSpinBox:focus,
QDoubleSpinBox:focus {{
    background-color: {sel_inact};
    border-bottom: 2px solid {brd_focus};
}}

QSpinBox::up-button,   QDoubleSpinBox::up-button,
QSpinBox::down-button, QDoubleSpinBox::down-button {{
    width: 0px;
    height: 0px;
    border: none;
}}

/* ── DateEdit / DateTimeEdit — Soft Box style ───────────────────── */
QDateEdit,
QDateTimeEdit {{
    background-color: {input_bg};
    border: none;
    border-radius: 8px;
    border-bottom: 2px solid transparent;
    padding: 5px 8px;
    font-size: {base_pt}pt;
    color: {text};
}}

QDateEdit:focus,
QDateTimeEdit:focus {{
    background-color: {sel_inact};
    border-bottom: 2px solid {brd_focus};
}}

QDateEdit::drop-down,
QDateTimeEdit::drop-down {{
    subcontrol-origin: padding;
    subcontrol-position: center right;
    width: 28px;
    border: none;
    background: transparent;
}}

QDateEdit::down-arrow,
QDateTimeEdit::down-arrow {{
    image: none;
    border-left: 5px solid transparent;
    border-right: 5px solid transparent;
    border-top: 6px solid {text_dim};
    margin-right: 8px;
}}

QDateEdit::down-arrow:hover,
QDateTimeEdit::down-arrow:hover {{
    border-top-color: {accent};
}}


/* ── Dialog shell — consistent dialog backgrounds ───────────────── */
QGroupBox#dialogSection {{
    background-color: {surface};
    border: 1px solid {data_brd};
    border-radius: 6px;
    margin-top: 20px;
    padding-top: 16px;
    padding-left: 16px;
    padding-right: 16px;
    padding-bottom: 16px;
    font-family: 'Manrope', 'SF Pro Display', 'Segoe UI', sans-serif;
    font-weight: 700;
    color: {text};
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
    background-color: {surface_hover};
    border-radius: 4px;
}}

QToolButton#collapsibleToggle:checked {{
    background-color: {surface};
    color: {text};
}}

/* ------------------------------------------------------------------ */
/* Segmented category toggles (e.g. Spores / Cystidia / Basidia)      */
/* Set objectName="categoryButton" and property "position" left/middle/right */
/* ------------------------------------------------------------------ */
QPushButton#categoryButton {{
    background-color: {surface_low};
    color: {accent};
    border: 1px solid {data_brd};
    padding: 6px 16px;
    font-weight: bold;
    border-radius: 0px;
}}

QPushButton#categoryButton:checked {{
    background-color: {accent};
    color: white;
    border-color: {accent};
}}

QPushButton#categoryButton:hover:!checked {{
    background-color: {sel_bg};
}}

QPushButton#categoryButton[position="left"] {{
    border-top-left-radius: 8px;
    border-bottom-left-radius: 8px;
}}

QPushButton#categoryButton[position="right"] {{
    border-top-right-radius: 8px;
    border-bottom-right-radius: 8px;
    border-left: none;
}}

QPushButton#categoryButton[position="middle"] {{
    border-left: none;
}}

/* Side panel container */
#sidePanel {{
    background-color: {surface_low};
    border-radius: 12px;
}}

/* Primary action button — full gradient, same as default but explicit */
QPushButton#primaryButton {{
    background-color: qlineargradient(x1:0, y1:0, x2:1, y2:1,
        stop:0 {accent}, stop:1 {accent_h});
    color: white;
    border: none;
    border-radius: 8px;
    padding: 7px 18px;
    font-weight: bold;
    font-size: {base_pt}pt;
}}

QPushButton#primaryButton:hover {{
    background-color: {accent_h};
}}

QPushButton#primaryButton:pressed {{
    background-color: {accent_p};
}}

/* Inline map link — looks like a text hyperlink */
QPushButton#mapLink {{
    background-color: transparent;
    color: {accent};
    border: none;
    border-radius: 0px;
    padding: 2px 0px;
    font-weight: normal;
    text-decoration: underline;
}}

QPushButton#mapLink:hover {{
    color: {accent_h};
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
