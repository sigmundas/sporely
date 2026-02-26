"""Main entry point for Mushroom Spore Analyzer"""
import os
import signal
import sys
import time
from pathlib import Path

os.environ.setdefault("QTWEBENGINE_DISABLE_GPU", "1")
os.environ.setdefault("QT_QUICK_BACKEND", "software")
os.environ.setdefault("LIBGL_ALWAYS_SOFTWARE", "1")
os.environ.setdefault("QTWEBENGINE_CHROMIUM_FLAGS", "--disable-gpu --disable-software-rasterizer")
if sys.platform.startswith("linux"):
    # Avoid loading libproxy-based GIO module in mixed snap/system setups.
    os.environ["GIO_USE_PROXY_RESOLVER"] = "0"
    # VS Code Snap may inject cached GIO modules built against a newer libstdc++.
    # Drop these extra modules to avoid non-fatal GLIBCXX warnings at startup.
    os.environ.pop("GIO_EXTRA_MODULES", None)

from PySide6.QtWidgets import QApplication, QSplashScreen
from PySide6.QtGui import QFont, QPixmap, QPainter, QColor, QPalette
from PySide6.QtCore import QTranslator, QLocale, Qt, QTimer
from database.schema import init_database, get_app_settings, update_app_settings
from database.models import SettingsDB
from ui.main_window import MainWindow

APP_VERSION = "0.5.9"


def _create_splash(app: QApplication, version: str) -> QSplashScreen | None:
    logo_path = Path(__file__).parent / "docs" / "images" / "mycolog-logo.png"
    if not logo_path.exists():
        return None
    logo = QPixmap(str(logo_path))
    if logo.isNull():
        return None

    extra_height = 36
    splash_pixmap = QPixmap(logo.width(), logo.height() + extra_height)
    splash_pixmap.fill(Qt.white)

    painter = QPainter(splash_pixmap)
    painter.drawPixmap(0, 0, logo)
    painter.setPen(QColor(60, 60, 60))
    font = QFont(app.font())
    font.setPointSize(max(9, font.pointSize() - 1))
    painter.setFont(font)
    painter.drawText(
        0,
        logo.height(),
        splash_pixmap.width(),
        extra_height,
        Qt.AlignCenter,
        f"v{version}" if version else ""
    )
    painter.end()

    splash = QSplashScreen(splash_pixmap)
    splash.setWindowFlag(Qt.WindowStaysOnTopHint, True)
    return splash


def _apply_light_palette(app: QApplication) -> None:
    """Force a light palette so app colors stay consistent across OS themes."""
    palette = QPalette()

    window = QColor("#f5f5f5")
    panel = QColor("#ffffff")
    text = QColor("#2c3e50")
    muted = QColor("#7f8c8d")
    border = QColor("#d0d7de")
    disabled_bg = QColor("#eceff1")
    disabled_text = QColor("#95a5a6")
    highlight = QColor("#3498db")

    palette.setColor(QPalette.Window, window)
    palette.setColor(QPalette.WindowText, text)
    palette.setColor(QPalette.Base, panel)
    palette.setColor(QPalette.AlternateBase, window)
    palette.setColor(QPalette.ToolTipBase, panel)
    palette.setColor(QPalette.ToolTipText, text)
    palette.setColor(QPalette.Text, text)
    palette.setColor(QPalette.Button, window)
    palette.setColor(QPalette.ButtonText, text)
    palette.setColor(QPalette.BrightText, QColor("white"))
    palette.setColor(QPalette.Mid, border)
    palette.setColor(QPalette.Dark, QColor("#b0bec5"))
    palette.setColor(QPalette.Light, QColor("#ffffff"))
    palette.setColor(QPalette.Highlight, highlight)
    palette.setColor(QPalette.HighlightedText, QColor("white"))
    palette.setColor(QPalette.PlaceholderText, muted)

    palette.setColor(QPalette.Disabled, QPalette.WindowText, disabled_text)
    palette.setColor(QPalette.Disabled, QPalette.Text, disabled_text)
    palette.setColor(QPalette.Disabled, QPalette.ButtonText, disabled_text)
    palette.setColor(QPalette.Disabled, QPalette.Base, disabled_bg)
    palette.setColor(QPalette.Disabled, QPalette.Button, disabled_bg)

    app.setPalette(palette)

    # Qt 6 may expose a runtime color-scheme hint; use it when available,
    # but keep this optional for compatibility across PySide6 versions.
    try:
        color_scheme_enum = getattr(Qt, "ColorScheme", None)
        set_color_scheme = getattr(app.styleHints(), "setColorScheme", None)
        if color_scheme_enum is not None and callable(set_color_scheme):
            set_color_scheme(color_scheme_enum.Light)
    except Exception:
        pass


def main():
    """Initialize and run the application."""
    # Create and run application
    app = QApplication(sys.argv)
    app.setApplicationName("MycoLog - Mushroom Log and Spore Analyzer")
    app.setApplicationVersion(APP_VERSION)
    # Fusion style gives fully consistent QSS rendering on every platform —
    # no native-style quirks that partially ignore stylesheet rules.
    app.setStyle("Fusion")
    _apply_light_palette(app)
    # Use the system locale so QDoubleSpinBox and other locale-aware widgets
    # accept the decimal separator the user's OS is configured for.
    QLocale.setDefault(QLocale.system())
    app_font = app.font()
    if app_font.pointSize() <= 0:
        app_font.setPointSize(10)
        app.setFont(app_font)

    splash = _create_splash(app, APP_VERSION)
    splash_shown_at: float | None = None
    if splash:
        splash.show()
        splash.raise_()
        app.processEvents()
        splash_shown_at = time.monotonic()

    # Initialize database (after splash is visible)
    print("Initializing database...")
    init_database()

    translator = QTranslator()
    app_settings = get_app_settings()
    lang_code = app_settings.get("ui_language")
    if not lang_code:
        lang_code = SettingsDB.get_setting("ui_language")
    if not lang_code:
        system_locale = QLocale.system().name().lower()
        system_prefix = system_locale.split("_")[0]
        if system_prefix in ("de",):
            lang_code = "de_DE"
        elif system_prefix in ("nb", "no"):
            lang_code = "nb_NO"
        elif system_prefix in ("en",):
            lang_code = "en"
        else:
            lang_code = "en"
        update_app_settings({"ui_language": lang_code})
        SettingsDB.set_setting("ui_language", lang_code)
    if lang_code != "en":
        qm_path = Path(__file__).parent / "i18n" / f"MycoLog_{lang_code}.qm"
        if translator.load(str(qm_path)):
            app.installTranslator(translator)
            app._translator = translator

    window = MainWindow(app_version=APP_VERSION)
    if splash:
        if splash_shown_at is not None:
            elapsed = time.monotonic() - splash_shown_at
            min_splash_seconds = 0.6
            if elapsed < min_splash_seconds:
                time.sleep(min_splash_seconds - elapsed)
                app.processEvents()
    window.show()
    if splash:
        splash.finish(window)
    window.start_update_check()

    # Keep Python signal handling responsive while Qt runs its event loop.
    signal_pump = QTimer()
    signal_pump.setInterval(200)
    signal_pump.timeout.connect(lambda: None)
    signal_pump.start()

    def _request_quit(*_args):
        print("\nShutdown requested, exiting...")
        app.quit()

    signal.signal(signal.SIGINT, _request_quit)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _request_quit)

    exit_code = app.exec()
    signal_pump.stop()
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
