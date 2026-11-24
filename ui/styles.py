
"""Dynamic stylesheet for the application.

All font sizes are derived from the system's actual base font size so the
layout looks correct on any platform without platform-specific checks.

Theme support: pass theme="auto" (default), "light", or "dark" to get_style().
Auto mode detects dark/light from the system palette before any palette
override has been applied (i.e. call get_style before apply_palette, or
just rely on the stored setting).
"""
from __future__ import annotations
import math as _math
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


def get_button_icon_color(object_name: str | None = None, theme: str = "auto") -> str:
    """Return the correct icon color for a button object name in the current theme."""
    colors = theme_colors(theme)
    if object_name == "primaryButton":
        return colors["primary_button_text"]
    if object_name == "destructiveButton":
        return colors["destructive_fg"]
    return colors["text"]


def get_design_tokens(theme: str = "auto") -> dict[str, str]:
    """Return shared color tokens used by code-built widgets."""
    colors = theme_colors(theme)
    return {
        name: colors[name]
        for name in (
            "surface",
            "surface_low",
            "tab_surface",
            "surface_hover",
            "data_brd",
            "text",
            "text_dim",
            "accent",
        )
    }


# The Live Lab capture-state bar sits on the image, not on the app's surfaces,
# so it uses the same dark bar in both themes.
HUD_COLORS: dict[str, str] = {
    "background": "rgba(18, 18, 18, 214)",
    "border": "rgba(255, 255, 255, 31)",
    "text": "#e0e0e0",
    "selected_background": "#e8e8e8",
    "focus": "#7cc4bd",
    # "Not calibrated" notice on the viewer (mockup Bilder-Live)
    "alert_background": "#3a1416",
    "alert_border": "#ff7a7a",
    "alert_text": "#ffd0d0",
}

# Images not yet saved to an observation (Live Lab pending captures,
# Kameraimport) carry this border colour.
STAGING_BORDER = "#e67e22"

# Reagent (stain) dot colours, keyed by the stain name in lower case without
# spaces, hyphens or underscores (see ui.adaptive_choice_selector.stain_color).
REAGENT_COLORS: dict[str, str] = {
    "melzer": "#9c7b3b",
    "congored": "#c0392b",
    "cottonblue": "#2980b9",
    "lactofuchsin": "#d46a9a",
    "cresylblue": "#6f63c7",
    "trypanblue": "#3b82f6",
    "chlorazolblacke": "#2d3436",
}

# Objective ring colours, ISO 8578:2012 Table 2 ("Colour of ring"): each band
# and the standard magnifications it covers. An objective's own stored colour
# overrides its band (ui.adaptive_choice_selector.objective_color).
OBJECTIVE_RING_BANDS: tuple[tuple[str, str, tuple[float, ...]], ...] = (
    ("black", "#202124", (1.0, 1.25)),
    ("grey", "#9aa0a6", (1.6, 2.0)),
    ("brown", "#8b5a2b", (2.5, 3.2)),
    ("red", "#e74c3c", (4.0, 5.0)),
    ("orange", "#f39c12", (6.3, 8.0)),
    ("yellow", "#f1c40f", (10.0, 12.5)),
    ("light green", "#2ecc71", (16.0, 20.0)),
    ("dark green", "#1e8449", (25.0, 32.0)),
    ("light blue", "#3498db", (40.0, 50.0)),
    ("dark blue", "#1f4ea8", (63.0, 80.0)),
    ("white", "#f7f1e5", (100.0, 125.0, 160.0)),
)
OBJECTIVE_RING_MACRO = "#6b7280"    # macro profiles: neutral grey
OBJECTIVE_RING_UNKNOWN = "#3498db"  # no magnification known


def objective_ring_color(magnification: float | str | None) -> str:
    """Return the ISO 8578 ring colour for an objective magnification.

    A magnification between two standard values takes the nearer one on a
    logarithmic scale, since the standard series is geometric (60x is nearer
    63x than 50x). An exact tie goes to the higher magnification. Values below
    1x or above 160x take the first or last band.
    """
    try:
        mag = float(magnification)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return OBJECTIVE_RING_UNKNOWN
    if not _math.isfinite(mag) or mag <= 0:
        return OBJECTIVE_RING_UNKNOWN
    target = _math.log(mag)
    best_color = OBJECTIVE_RING_UNKNOWN
    best_distance = _math.inf
    best_standard = 0.0
    for _name, color, standards in OBJECTIVE_RING_BANDS:
        for standard in standards:
            distance = abs(_math.log(standard) - target)
            closer = distance < best_distance - 1e-9
            tie_higher = abs(distance - best_distance) <= 1e-9 and standard > best_standard
            if closer or tie_higher:
                best_color, best_distance, best_standard = color, distance, standard
    return best_color


def objective_ring_outline(color) -> str | None:
    """Return an outline colour for a ring colour near black or white.

    Black and white rings (and other very dark or very light tag colours)
    disappear against one of the two themes or against bright or dark images,
    so they get a mid-grey outline that shows on both. Returns None when the
    colour stands out on its own.
    """
    from PySide6.QtGui import QColor

    qcolor = QColor(color)
    if not qcolor.isValid():
        return None
    lightness = qcolor.lightness()
    if lightness >= 200:
        return "#64748b"
    if lightness <= 60:
        return "#94a3b8"
    return None


RED_LIST_BADGE_COLORS = {
    "RE": "#2D3133",
    "CR": "#D40015",
    "EN": "#F04438",
    "VU": "#E67E22",
    "NT": "#DCB632",
    "DD": "#63666A",
    "LC": "#569D62",
    "NA": "#cccccc",
    "NE": "#aaaaaa",
}



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
        # Neither theme used to set Link, so every widget asking the palette
        # for an accent colour got Qt's default #0000ff. On #1c1b1b that is
        # effectively invisible: the reference picker's "+ New publication…"
        # row and the conflict dialog's status line both read as dark blue on
        # near-black. Stated per theme so the accent is legible on the base
        # colour it is actually painted on.
        palette.setColor(QPalette.Link,            QColor("#7fb3a3"))
        palette.setColor(QPalette.LinkVisited,     QColor("#a08bbd"))
        palette.setColor(QPalette.Disabled, QPalette.WindowText, QColor("#6b7270"))
        palette.setColor(QPalette.Disabled, QPalette.Text,       QColor("#6b7270"))
        palette.setColor(QPalette.Disabled, QPalette.ButtonText, QColor("#6b7270"))
        palette.setColor(QPalette.Disabled, QPalette.Base,       QColor("#353534"))
        palette.setColor(QPalette.Disabled, QPalette.Button,     QColor("#353534"))
    else:
        # Slate Lab
        palette.setColor(QPalette.Window,          QColor("#f8f9fa"))
        palette.setColor(QPalette.WindowText,      QColor("#1e293b"))
        palette.setColor(QPalette.Base,            QColor("#ffffff"))
        palette.setColor(QPalette.AlternateBase,   QColor("#f1f5f9"))
        palette.setColor(QPalette.ToolTipBase,     QColor("#ffffff"))
        palette.setColor(QPalette.ToolTipText,     QColor("#1e293b"))
        palette.setColor(QPalette.Text,            QColor("#1e293b"))
        palette.setColor(QPalette.Button,          QColor("#f1f5f9"))
        palette.setColor(QPalette.ButtonText,      QColor("#1e293b"))
        palette.setColor(QPalette.BrightText,      QColor("white"))
        palette.setColor(QPalette.Mid,             QColor("#cbd5e1"))
        palette.setColor(QPalette.Dark,            QColor("#cbd5e1"))
        palette.setColor(QPalette.Light,           QColor("#ffffff"))
        palette.setColor(QPalette.Highlight,       QColor("#dbeafe"))
        palette.setColor(QPalette.HighlightedText, QColor("#1e293b"))
        palette.setColor(QPalette.PlaceholderText, QColor("#64748b"))
        palette.setColor(QPalette.Link,            QColor("#1d4ed8"))
        palette.setColor(QPalette.LinkVisited,     QColor("#6d28d9"))
        palette.setColor(QPalette.Disabled, QPalette.WindowText, QColor("#64748b"))
        palette.setColor(QPalette.Disabled, QPalette.Text,       QColor("#64748b"))
        palette.setColor(QPalette.Disabled, QPalette.ButtonText, QColor("#64748b"))
        palette.setColor(QPalette.Disabled, QPalette.Base,       QColor("#f1f5f9"))
        palette.setColor(QPalette.Disabled, QPalette.Button,     QColor("#f1f5f9"))

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


# Colour tokens per theme. get_style() renders the global stylesheet from
# these, and get_design_tokens() / get_button_icon_color() hand the same
# values to code-built widgets. Inline colours elsewhere move here screen by
# screen as the UI redesign touches each screen.
_COLOR_TOKENS: dict[str, dict[str, str]] = {
    "light": {
        # Slate Lab — refined for the web-style card layout.
        "bg": "#f8f9fa",                  # bg-slate-50
        "surface": "#ffffff",             # pure white cards/inputs
        "surface_low": "#f1f5f9",         # box headers and soft panels
        "tab_surface": "#f1f5f9",         # full-width taxonomy tab strip
        "surface_hover": "#e2e8f0",       # hover on surface_low elements
        "input_bg": "#ffffff",            # white inputs
        "brd_focus": "#4d7c7a",           # teal focus ring
        "text": "#1e293b",                # text-slate-800
        "text_dim": "#64748b",            # text-slate-500
        "accent": "#4d7c7a",              # guess/action teal
        "accent_h": "#466f6d",            # hover: slightly darker
        "accent_p": "#3d6260",            # pressed: deepest
        "dis_bg": "#e2e8f0",              # muted disabled background
        "dis_fg": "#64748b",              # muted disabled text
        "menubar_bg": "#52796f",
        "menubar_h": "#486b62",
        "img_bg": "#f1f5f9",              # matches surface_low
        "sel_bg": "#dbeafe",              # softer blue selection for light mode
        "sel_fg": "#1e293b",
        "sel_inact": "#eff6ff",           # focus tint
        "inline_brd": "#4d7c7a",
        "indicator_border": "#64748b",
        "indicator_checked": "#52796f",   # save green fill for checked state
        "indicator_bg": "#ffffff",
        "indicator_disabled": "#e2e8f0",
        "data_brd": "#cbd5e1",            # border-slate-300
        "data_fg": "#64748b",
        "destructive_fg": "#9b3d35",
        "destructive_border": "#d2938d",
        "destructive_hover_bg": "#f6e4e2",
        "destructive_pressed_bg": "#e7c9c6",
        "primary_button_bg": "#52796f",
        "primary_button_bg_h": "#486b62",
        "primary_button_text": "#ffffff",
        "crop_image_active": "#b94a42",
        "crop_ai_active": "#b86b20",
        "dialog_brd": "#e2e8f0",
        "lab_alert": "#d64545",           # unset lab state (ui.combo_alerts)
        "load_button_bg": "#4f7080",      # QPushButton#loadButton
        "load_button_bg_h": "#3d5a68",
        "map_button_bg": "#334155",
        "objective_tag_bg": "rgba(71, 103, 74, 200)",
    },
    "dark": {
        # Clinical Nocturne — atmospheric, high-contrast, lab-optimised
        "bg": "#131313",                  # deep charcoal background
        "surface": "#1c1b1b",             # slightly lighter charcoal for cards
        "surface_low": "#252423",         # tab bar, header row, side panel
        "tab_surface": "#2b2a29",         # slightly lighter than box headers
        "surface_hover": "#252423",       # hover on surface_low elements
        "input_bg": "#1c1b1b",            # input fields
        "brd_focus": "#4d7c7a",           # teal focus ring
        "text": "#e8e8e8",                # off-white primary text
        "text_dim": "#a1a1aa",            # muted slate secondary text
        "accent": "#4d7c7a",              # guess/action teal
        "accent_h": "#5a908d",            # lighter hover
        "accent_p": "#3f6765",            # pressed teal
        "dis_bg": "#353534",              # dark grey disabled bg
        "dis_fg": "#6b7270",              # muted disabled text
        "menubar_bg": "#131313",
        "menubar_h": "#1c1b1b",
        "img_bg": "#1c1b1b",
        "sel_bg": "#3d5a52",              # deep forest green selection
        "sel_fg": "#e8e8e8",
        "sel_inact": "#2a3c38",
        "inline_brd": "#4d7c7a",
        "indicator_border": "#4d7c7a",    # teal indicator border
        "indicator_checked": "#52796f",   # save green fill for checked state
        "indicator_bg": "#1c1b1b",
        "indicator_disabled": "#353534",
        "data_brd": "#334155",
        "data_fg": "#a1a1aa",
        "destructive_fg": "#e4a7a1",
        "destructive_border": "#8b3530",
        "destructive_hover_bg": "rgba(228, 167, 161, 0.16)",
        "destructive_pressed_bg": "rgba(228, 167, 161, 0.24)",
        "primary_button_bg": "#52796f",
        "primary_button_bg_h": "#486b62",
        "primary_button_text": "#ffffff",
        "crop_image_active": "#b85a52",
        "crop_ai_active": "#b9702b",
        "dialog_brd": "transparent",
        "lab_alert": "#d65a63",           # unset lab state (ui.combo_alerts)
        "load_button_bg": "#4f7080",      # QPushButton#loadButton
        "load_button_bg_h": "#3d5a68",
        "map_button_bg": "#334155",
        "objective_tag_bg": "rgba(71, 103, 74, 200)",
    },
}

# Corner radii used by the global stylesheet (``{radius_md}`` and so on).
RADIUS: dict[str, str] = {
    "sm": "4px",
    "md": "6px",
    "lg": "8px",
    "xl": "10px",
    "xxl": "12px",
}

# Spacing scale in pixels for layouts built in code (margins, spacing,
# padding). The global stylesheet's paddings move to it screen by screen.
SPACING: dict[str, int] = {
    "xxs": 2,
    "xs": 4,
    "sm": 6,
    "md": 8,
    "lg": 12,
    "xl": 16,
    "xxl": 20,
}


def theme_colors(theme: str = "auto") -> dict[str, str]:
    """Return a copy of the colour tokens for *theme* ("auto", "light" or "dark")."""
    return dict(_COLOR_TOKENS["dark" if _is_dark(theme) else "light"])


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

    small_pt   = max(base_pt - 2, 8)
    header_pt  = base_pt + 2
    obj_tag_pt = base_pt + 1

    tokens: dict[str, object] = {
        **theme_colors(theme),
        **{f"radius_{name}": value for name, value in RADIUS.items()},
        "base_pt": base_pt,
        "small_pt": small_pt,
        "header_pt": header_pt,
        "title_pt": header_pt + 1,
        "obj_tag_pt": obj_tag_pt,
        "chk_url": _CHK_URL,
    }

    return """
QMainWindow {{
    background-color: {bg};
}}

QSplitter#gallerySplitter::handle {{
    background-color: {data_brd};
}}

QSplitter#gallerySplitter::handle:horizontal {{
    width: 3px;
    margin: 4px 0px;
    border-radius: 1px;
}}

QSplitter#gallerySplitter::handle:vertical {{
    height: 3px;
    margin: 0px 4px;
    border-radius: 1px;
}}

QSplitter#gallerySplitter::handle:hover {{
    background-color: {accent};
}}

QWidget {{
    font-family: 'Inter 18pt', sans-serif;
    font-size: {base_pt}pt;
    color: {text};
}}

QFrame#sectionCard,
QFrame#dialogBoxCard {{
    background-color: {surface};
    border: 1px solid {data_brd};
    border-radius: {radius_xxl};
}}

QFrame#boxHeader {{
    background-color: {surface_low};
    border-bottom: 1px solid {data_brd};
    border-top-left-radius: 11px;
    border-top-right-radius: 11px;
}}

QFrame#boxHeader[sectionToggle="true"] {{
    background-color: {surface_low};
}}

QFrame#boxHeader[sectionToggle="true"][collapsed="true"] {{
    border-bottom: none;
    border-bottom-left-radius: 11px;
    border-bottom-right-radius: 11px;
}}

QFrame#boxHeader[sectionToggle="true"]:hover,
QFrame#boxHeader[sectionToggle="true"]:focus {{
    background-color: {surface_hover};
}}

QLabel#sectionArrowLabel {{
    color: {text_dim};
    font-size: {base_pt}pt;
}}

QLabel#sectionSummaryLabel {{
    color: {text_dim};
    font-size: {small_pt}pt;
}}

QLabel#sectionSummaryAlertLabel {{
    color: {lab_alert};
    font-size: {small_pt}pt;
    font-weight: 600;
}}

QFrame#segmentedControl {{
    background-color: {surface_low};
    border: 1px solid {data_brd};
    border-radius: 22px;
}}

QFrame#segmentedControl[compact="true"] {{
    border-radius: {radius_xl};
}}

QPushButton#segmentedButton {{
    background-color: transparent;
    color: {text};
    border: none;
    border-radius: 17px;
    padding: 7px 18px;
    min-height: 34px;
    font-family: 'Manrope', sans-serif;
    font-size: {base_pt}pt;
    font-weight: 700;
}}

QPushButton#segmentedButton[compact="true"] {{
    border-radius: {radius_xl};
    padding: 5px 14px 6px 14px;
    min-height: 0px;
}}

QPushButton#segmentedButton:hover:!checked {{
    background-color: {surface_hover};
}}

QPushButton#segmentedButton:checked {{
    background-color: {accent};
    color: white;
}}

/* ── Seamless tab navigation ──────────────────────────────────────── */
/* The tab widget paints the whole strip, including empty space after the last tab.
   The pane below returns to the card surface used by dialog content. */
QTabWidget {{
    background-color: {tab_surface};
}}

QTabWidget::pane {{
    border: none;
    background-color: {surface};
    border-bottom-left-radius: 10px;
    border-bottom-right-radius: 10px;
    margin-top: 0px;
}}

QWidget#taxonomyPage {{
    background-color: {surface};
    border-bottom-left-radius: 10px;
    border-bottom-right-radius: 10px;
}}

QTabWidget::tab-bar {{
    alignment: left;
}}

QTabBar {{
    background-color: {tab_surface};
    border: none;
    qproperty-drawBase: 0;
}}

QTabBar::tab {{
    background-color: {tab_surface};
    color: {text_dim};
    border: none;
    border-bottom: 2px solid transparent;
    padding: 10px 20px;
    font-family: 'Manrope', sans-serif;
    font-weight: 700;
    font-size: {base_pt}pt;
    min-width: 80px;
}}

QTabBar::tab:selected {{
    background-color: {tab_surface};
    color: {accent};
    border-bottom: 2px solid {accent};
}}

QTabBar::tab:hover:!selected {{
    background-color: {surface_hover};
    color: {text};
}}

/* Stage 9a: the mode bar above the main window's pages, styled like the tab bar it replaces. */
QWidget#modeBar {{
    background-color: {tab_surface};
}}

QToolButton#modeBarButton {{
    background-color: {tab_surface};
    color: {text_dim};
    border: none;
    border-bottom: 2px solid transparent;
    padding: 10px 16px;
    font-family: 'Manrope', sans-serif;
    font-weight: 700;
    font-size: {base_pt}pt;
}}

QToolButton#modeBarButton:checked {{
    color: {accent};
    border-bottom: 2px solid {accent};
}}

QToolButton#modeBarButton:hover:!checked {{
    background-color: {surface_hover};
    color: {text};
}}

QToolButton#modeBarButton:disabled {{
    color: {dis_fg};
}}

QToolButton#cameraImportButton {{
    background-color: {surface};
    color: {text};
    border: 1px solid {data_brd};
    border-radius: 14px;
    padding: 4px 12px;
    font-weight: 600;
}}

QToolButton#cameraImportButton:hover {{
    background-color: {surface_hover};
}}

QToolButton#cameraImportButton:checked {{
    color: {accent};
    border: 1px solid {accent};
}}

QLabel#modeBarBadge {{
    background-color: {accent};
    color: #ffffff;
    border-radius: 9px;
    padding: 0px 7px;
    font-weight: 700;
    min-height: 18px;
}}

QGroupBox {{
    background-color: {surface};
    border: none;
    border-radius: {radius_lg};
    margin-top: 16px;
    padding-top: 16px;
    padding-left: 16px;
    padding-right: 16px;
    padding-bottom: 16px;
    font-family: 'Manrope', sans-serif;
    font-weight: 700;
    color: {text};
}}

QGroupBox::title {{
    subcontrol-origin: margin;
    subcontrol-position: top left;
    left: 12px;
    padding: 4px 8px;
    background-color: {surface};
    border-radius: {radius_sm};
}}

QPushButton {{
    background-color: qlineargradient(x1:0, y1:0, x2:1, y2:1,
        stop:0 {accent}, stop:1 {accent_h});
    color: white;
    border: none;
    border-radius: {radius_lg};
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

QPushButton::icon {{
    margin-right: 14px;
}}

QPushButton#primaryButton {{
    background-color: qlineargradient(x1:0, y1:0, x2:1, y2:1,
        stop:0 {primary_button_bg}, stop:1 {primary_button_bg_h});
    color: {primary_button_text};
    border: none;
    border-radius: {radius_xxl};
    padding: 10px 16px;
    min-height: 35px;
    font-weight: bold;
    font-size: {base_pt}pt;
}}

QPushButton#primaryButton:hover {{
    background-color: qlineargradient(x1:0, y1:0, x2:1, y2:1,
        stop:0 {primary_button_bg_h}, stop:1 {primary_button_bg});
}}

QPushButton#primaryButton:pressed {{
    background-color: {accent_p};
}}

QPushButton#outlineButton {{
    background-color: {surface};
    color: {text};
    border: 1.5px solid {accent};
    border-radius: {radius_xl};
    padding: 8px 14px;
    font-weight: bold;
}}

QPushButton#outlineButton:hover {{
    background-color: {surface_low};
    border-color: {accent_h};
}}

QPushButton#outlineButton:pressed {{
    background-color: {surface_hover};
    border-color: {accent_p};
    color: {text};
}}

QPushButton#outlineButton:disabled {{
    background-color: transparent;
    color: {dis_fg};
    border: 1.5px solid {dis_fg};
}}

/* A command that reads as a link (the reference drawer, stage 7). */
QPushButton#linkButton {{
    background-color: transparent;
    border: none;
    color: {accent};
    padding: 0px 2px;
    font-weight: normal;
    text-decoration: underline;
}}

QPushButton#linkButton:hover {{
    color: {accent_h};
}}

QPushButton#linkButton[active="true"] {{
    font-weight: bold;
}}

/* Opens a drawer page: outlined while closed, filled while its page shows (stage 6). */
QPushButton#drawerToggle {{
    background-color: {surface};
    color: {text};
    border: 1.5px solid {accent};
    border-radius: {radius_xl};
    padding: 5px 14px;
    font-weight: bold;
}}

QPushButton#drawerToggle:hover:!checked {{
    background-color: {surface_low};
    border-color: {accent_h};
}}

QPushButton#drawerToggle:checked {{
    background-color: {accent};
    color: white;
}}

QPushButton#dataButton {{
    background-color: {surface_low};
    color: {text};
    border: 1.5px solid {data_brd};
    border-radius: {radius_xl};
    padding: 8px 12px;
    font-weight: normal;
}}

QPushButton#dataButton:hover {{
    background-color: {surface};
    color: {text};
    border-color: {accent};
}}

QPushButton#dataButton:pressed {{
    background-color: {input_bg};
}}

QPushButton#dataButton:disabled {{
    color: {dis_fg};
    border-color: {dis_bg};
}}

QPushButton#destructiveButton {{
    background-color: transparent;
    color: {destructive_fg};
    border: 1.5px solid {destructive_border};
    border-radius: {radius_xl};
    padding: 8px 12px;
    font-weight: normal;
}}

QPushButton#destructiveButton:hover {{
    background-color: {destructive_hover_bg};
    color: {destructive_fg};
}}

QPushButton#destructiveButton:pressed {{
    background-color: {destructive_pressed_bg};
    border-color: {destructive_border};
    color: {destructive_fg};
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
    background-color: {load_button_bg};
}}

QPushButton#loadButton:hover {{
    background-color: {load_button_bg_h};
}}

QPushButton[sourceActive="true"] {{
    background-color: {sel_bg};
    color: {text};
    font-weight: bold;
}}

QLabel#sectionHeader {{
    font-family: 'Manrope', sans-serif;
    font-weight: 700;
    font-size: {base_pt}pt;
    color: {text_dim};
    padding: 4px 0px 2px 0px;
    letter-spacing: 0.04em;
}}

QLineEdit {{
    background-color: {input_bg};
    border: 1px solid {data_brd};
    border-radius: {radius_md};
    padding: 6px 10px;
    font-size: {base_pt}pt;
    color: {text};
    selection-background-color: {sel_bg};
    selection-color: {sel_fg};
}}

QLineEdit:focus {{
    background-color: {sel_inact};
    border: 1px solid {brd_focus};
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
    border: 1px solid {data_brd};
    border-radius: {radius_md};
    padding: 6px 10px;
    font-family: 'Menlo', 'Consolas', 'Courier New', monospace;
    font-size: {small_pt}pt;
    color: {text};
    selection-background-color: {sel_bg};
    selection-color: {sel_fg};
}}

QPlainTextEdit {{
    selection-background-color: {sel_bg};
    selection-color: {sel_fg};
}}

QTextEdit:focus {{
    background-color: {sel_inact};
    border: 1px solid {brd_focus};
}}

QPlainTextEdit#imageNoteInput {{
    background-color: transparent;
    border: none;
    border-radius: 0px;
    padding: 2px 0px;
    color: {text};
}}

QPlainTextEdit#imageNoteInput:focus {{
    background-color: transparent;
    border: none;
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
    border-radius: {radius_sm};
    background-color: {indicator_bg};
}}

QCheckBox::indicator:hover {{
    border: 2px solid {accent};
    border-radius: {radius_sm};
}}

QCheckBox::indicator:checked {{
    background-color: {indicator_checked};
    border: 2px solid {indicator_checked};
    border-radius: {radius_sm};
    image: {chk_url};
}}

QCheckBox::indicator:checked:disabled {{
    background-color: {dis_bg};
    border: 2px solid {indicator_disabled};
    border-radius: {radius_sm};
    image: {chk_url};
}}

QCheckBox::indicator:unchecked {{
    background-color: {indicator_bg};
    border: 2px solid {indicator_border};
    border-radius: {radius_sm};
}}

QCheckBox::indicator:unchecked:disabled {{
    background-color: {dis_bg};
    border: 2px solid {indicator_disabled};
    border-radius: {radius_sm};
}}

QRadioButton {{
    color: {text};
    spacing: 8px;
}}

QRadioButton::indicator {{
    width: 16px;
    height: 16px;
    border: 2px solid {indicator_border};
    border-radius: {radius_lg};
    background-color: {indicator_bg};
}}

QRadioButton::indicator:hover {{
    border: 2px solid {accent};
    border-radius: {radius_lg};
}}

QRadioButton::indicator:checked {{
    background-color: {indicator_checked};
    border: 4px solid {indicator_bg};
    border-radius: {radius_lg};
}}

QRadioButton::indicator:unchecked {{
    background-color: {indicator_bg};
    border: 2px solid {indicator_border};
    border-radius: {radius_lg};
}}

QRadioButton::indicator:disabled {{
    background-color: {dis_bg};
    border: 2px solid {indicator_disabled};
    border-radius: {radius_lg};
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

QListWidget#settingsNav {{
    background-color: {surface_low};
    border-right: 1px solid {data_brd};
    padding: 8px 0;
    font-size: {base_pt}pt;
}}

QListWidget#settingsNav::item {{
    padding: 8px 16px;
    border-radius: 0;
    color: {text_dim};
}}

QListWidget#settingsNav::item:selected {{
    background-color: {sel_bg};
    color: {text};
    border-left: 3px solid {accent};
    padding-left: 13px;
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
    font-family: 'Inter 18pt', sans-serif;
    font-size: {small_pt}pt;
    font-weight: 600;
    letter-spacing: 0.05em;
    text-transform: uppercase;
}}

QHeaderView::section:last {{
    border-right: none;
}}

/* Preserve the scientific µm unit: uppercasing turns it into the
   visually incorrect MM (millimetres) in the raw-points editor. */
QTableWidget#referenceSporeTable QHeaderView::section {{
    text-transform: none;
}}

QTableView::item,
QTableWidget::item {{
    color: {text};
    padding: 16px 12px;
}}

QTableView#referenceSeriesTable::item,
QTableWidget#referenceSeriesTable::item {{
    padding: 4px 8px;
}}

QTableView#referenceSeriesTable QHeaderView::section,
QTableWidget#referenceSeriesTable QHeaderView::section {{
    padding: 4px 8px;
}}

/* The global 16px item padding is larger than a typical 30 px row, which
   pushes inline editors (QLineEdit) to zero height and pushes link cell
   widgets (QLabel) to the bottom of the cell. Use compact padding on the
   tables that use short rows so inline editors stay visible and link
   widgets stay vertically centered. */
QTableView#observationsTable::item,
QTableWidget#observationsTable::item,
QTableView#referenceMinmaxTable::item,
QTableWidget#referenceMinmaxTable::item,
QTableView#referenceSporeTable::item,
QTableWidget#referenceSporeTable::item {{
    padding: 2px 6px;
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
    font-family: 'Inter 18pt', sans-serif;
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
    border-radius: {radius_md};
}}

QLabel#headerLabel {{
    font-family: 'Manrope', sans-serif;
    font-size: {header_pt}pt;
    font-weight: 700;
    color: {text};
}}

QLabel#observationHeaderLabel {{
    font-family: 'Manrope', sans-serif;
    font-size: {title_pt}pt;
    font-weight: 700;
    color: {text};
    padding: 2px 0px 4px 0px;
}}

/* Metadata label: all-caps small Inter for technical fields (genus, latitude, etc.) */
QLabel#metaLabel {{
    font-family: 'Inter 18pt', sans-serif;
    font-size: {small_pt}pt;
    font-weight: 800;
    color: {text_dim};
    letter-spacing: 0.05em;
    text-transform: uppercase;
}}

QLabel#objectiveTag {{
    background-color: {objective_tag_bg};
    color: white;
    font-weight: bold;
    font-size: {obj_tag_pt}pt;
    border-radius: {radius_md};
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
    border-radius: {radius_sm};
}}

QMenu {{
    background-color: {surface};
    border: none;
    border-radius: {radius_lg};
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
    border: 1px solid {dialog_brd};
    border-radius: {radius_xxl};
}}

QMessageBox {{
    background-color: {bg};
    border: 1px solid {dialog_brd};
    border-radius: {radius_xxl};
}}

QComboBox {{
    background-color: {input_bg};
    border: 1px solid {data_brd};
    border-radius: {radius_md};
    padding: 6px 10px;
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
    border-radius: {radius_lg};
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
    border: 1px solid {brd_focus};
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
    border: 1px solid {data_brd};
    border-radius: {radius_md};
    padding: 6px 10px;
    padding-right: 2px;
    font-size: {base_pt}pt;
    color: {text};
}}

QSpinBox:focus,
QDoubleSpinBox:focus {{
    background-color: {sel_inact};
    border: 1px solid {brd_focus};
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
    border: 1px solid {data_brd};
    border-radius: {radius_md};
    padding: 6px 10px;
    font-size: {base_pt}pt;
    color: {text};
}}

QDateEdit:focus,
QDateTimeEdit:focus {{
    background-color: {sel_inact};
    border: 1px solid {brd_focus};
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
    border-radius: {radius_md};
    margin-top: 20px;
    padding-top: 16px;
    padding-left: 16px;
    padding-right: 16px;
    padding-bottom: 16px;
    font-family: 'Manrope', sans-serif;
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
QToolButton#cornerIconBtn {{
    background-color: transparent;
    border: none;
    border-radius: {radius_md};
    padding: 4px;
}}

QToolButton#cornerIconBtn:hover {{
    background-color: {surface_hover};
}}

QToolButton#cornerIconBtn:pressed {{
    background-color: {input_bg};
}}

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
    border-radius: {radius_sm};
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
    border-radius: {radius_xxl};
}}

/* Primary action button — full gradient, same as default but explicit */
QPushButton#primaryButton {{
    background-color: {primary_button_bg};
    color: {primary_button_text};
    border: none;
    border-radius: {radius_md};
    padding: 8px 20px;
    min-height: 35px;
    font-weight: bold;
    font-size: {base_pt}pt;
}}

QPushButton#primaryButton:hover {{
    background-color: {primary_button_bg_h};
}}

QPushButton#primaryButton:pressed {{
    background-color: {accent_p};
}}

QPushButton#saveButton {{
    background-color: {primary_button_bg};
    color: {primary_button_text};
    font-weight: bold;
    border-radius: {radius_md};
    padding: 8px 20px;
}}

QPushButton#saveButton:hover {{
    background-color: {primary_button_bg_h};
}}

QPushButton#dialogPrimaryButton {{
    background-color: {primary_button_bg};
    color: {primary_button_text};
    border: none;
    border-radius: {radius_md};
    padding: 6px 16px;
    min-height: 0px;
    font-weight: bold;
    font-size: {base_pt}pt;
}}

QPushButton#dialogPrimaryButton:hover {{
    background-color: {primary_button_bg_h};
}}

QPushButton#dialogPrimaryButton:pressed {{
    background-color: {accent_p};
}}

QPushButton#cropActionButton {{
    background-color: {surface_low};
    color: {text};
    border: 1px solid {data_brd};
    border-radius: {radius_md};
    padding: 6px 10px;
    font-weight: bold;
}}

QPushButton#cropActionButton:hover {{
    background-color: {surface_hover};
}}

QPushButton#cropActionButton[active="true"][cropRole="image"] {{
    background-color: {crop_image_active};
    color: white;
    border-color: {crop_image_active};
}}

QPushButton#cropActionButton[active="true"][cropRole="ai"] {{
    background-color: {crop_ai_active};
    color: white;
    border-color: {crop_ai_active};
}}

QPushButton#cropActionButton[hasUndo="true"] {{
    background-color: {surface};
    color: {destructive_fg};
    border-color: {destructive_border};
}}

QPushButton#cropActionButton[hasUndo="true"]:hover {{
    background-color: {destructive_hover_bg};
}}

QPushButton#cropActionButton:disabled {{
    background-color: {dis_bg};
    color: {dis_fg};
    border-color: {dis_bg};
}}

QPushButton#guessButton {{
    background-color: {accent};
    color: white;
    font-weight: 600;
    border-radius: {radius_sm};
}}

QPushButton#guessButton:hover {{
    background-color: {accent_h};
}}

QPushButton#mapButton {{
    background-color: {map_button_bg};
    color: white;
    font-size: {small_pt}pt;
    border-radius: {radius_sm};
    padding: 4px 12px;
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
""".format_map(tokens)


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
