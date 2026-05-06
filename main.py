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
_PROFILE_ENV = "SPORELY_PROFILE"
_APP_DATA_DIR_ENV = "SPORELY_APP_DATA_DIR"


def _extract_runtime_profile_args(argv: list[str]) -> list[str]:
    cleaned = [argv[0]] if argv else []
    i = 1
    while i < len(argv):
        arg = argv[i]
        if arg == "--profile":
            if i + 1 >= len(argv):
                raise SystemExit("--profile requires a value")
            os.environ[_PROFILE_ENV] = argv[i + 1]
            i += 2
            continue
        if arg.startswith("--profile="):
            os.environ[_PROFILE_ENV] = arg.split("=", 1)[1]
            i += 1
            continue
        if arg == "--data-dir":
            if i + 1 >= len(argv):
                raise SystemExit("--data-dir requires a value")
            os.environ[_APP_DATA_DIR_ENV] = argv[i + 1]
            i += 2
            continue
        if arg.startswith("--data-dir="):
            os.environ[_APP_DATA_DIR_ENV] = arg.split("=", 1)[1]
            i += 1
            continue
        cleaned.append(arg)
        i += 1
    return cleaned


sys.argv = _extract_runtime_profile_args(sys.argv)

try:
    import pillow_heif
    pillow_heif.register_heif_opener()
except ImportError:
    pass

if sys.platform.startswith("linux"):
    # Avoid loading libproxy-based GIO module in mixed snap/system setups.
    os.environ["GIO_USE_PROXY_RESOLVER"] = "0"
    # VS Code Snap may inject cached GIO modules built against a newer libstdc++.
    # Drop these extra modules to avoid non-fatal GLIBCXX warnings at startup.
    os.environ.pop("GIO_EXTRA_MODULES", None)

from PySide6.QtWidgets import QApplication, QSplashScreen
from PySide6.QtGui import QFont, QPixmap, QPainter, QColor, QPalette
from PySide6.QtCore import QTranslator, QLocale, Qt, QTimer
from app_identity import (
    APP_DISPLAY_NAME,
    APP_FULL_NAME,
    LEGACY_APP_NAME,
    app_data_dir,
    current_profile_name,
    migrate_legacy_storage,
)
from database.schema import init_database, get_app_settings, update_app_settings
from database.models import SettingsDB
from ui.main_window import MainWindow
from ui.styles import cache_system_dark, _is_dark

APP_VERSION = "0.8.2"


def _canonical_ui_language(code: str | None) -> str | None:
    """Map stored/system language codes to the app's supported UI locales."""
    text = str(code or "").strip().replace("-", "_")
    if not text:
        return None
    prefix = text.split("_", 1)[0].lower()
    if prefix == "de":
        return "de_DE"
    if prefix in {"nb", "nn", "no"}:
        return "nb_NO"
    if prefix == "sv":
        return "sv_SE"
    if prefix == "en":
        return "en"
    return None


def _create_splash(app: QApplication, version: str, theme: str = "auto") -> QSplashScreen | None:
    dark = _is_dark(theme)
    logo_name = "sporely-logo-light.png" if dark else "sporely-logo-dark.png"
    logo_path = Path(__file__).parent / "docs" / "images" / logo_name
    if not logo_path.exists():
        return None
    logo = QPixmap(str(logo_path))
    if logo.isNull():
        return None

    extra_height = 36
    splash_pixmap = QPixmap(logo.width(), logo.height() + extra_height)
    splash_pixmap.fill(QColor("#1c1c1e") if dark else QColor("#ffffff"))

    painter = QPainter(splash_pixmap)
    painter.drawPixmap(0, 0, logo)
    painter.setPen(QColor("#e8e8e8") if dark else QColor(60, 60, 60))
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


def main():
    """Initialize and run the application."""
    # Create and run application
    app = QApplication(sys.argv)
    app.setApplicationName(APP_FULL_NAME)
    app.setApplicationDisplayName(APP_DISPLAY_NAME)
    app.setApplicationVersion(APP_VERSION)

    _exec_started = False

    def _request_quit(*_args):
        print("\nShutdown requested, exiting...")
        if _exec_started:
            app.quit()
        else:
            sys.exit(0)

    signal.signal(signal.SIGINT, _request_quit)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _request_quit)

    # Keep Python signal handling responsive while Qt runs its event loop.
    signal_pump = QTimer()
    signal_pump.setInterval(200)
    signal_pump.timeout.connect(lambda: None)
    signal_pump.start()

    # Fusion style gives fully consistent QSS rendering on every platform —
    # no native-style quirks that partially ignore stylesheet rules.
    app.setStyle("Fusion")
    cache_system_dark()   # snapshot native dark state before palette override
    migrate_legacy_storage()
    print(
        "Starting Sporely with "
        f"data dir: {app_data_dir()}"
        + (
            f" (profile: {current_profile_name()})"
            if current_profile_name()
            else ""
        )
    )
    app_settings = get_app_settings()
    # _apply_light_palette(app) # Removed: MainWindow's _apply_theme handles palette
    # Use the system locale so QDoubleSpinBox and other locale-aware widgets
    # accept the decimal separator the user's OS is configured for.
    QLocale.setDefault(QLocale.system())

    # Preserve the OS-chosen UI size. On macOS this is typically larger than
    # 10pt, and hard-coding 10pt shrinks the entire app noticeably.
    system_font = QFont(app.font())
    if system_font.pointSize() <= 0:
        system_font.setPointSize(10)

    # Load bundled fonts (Inter for body/data, Manrope for headlines).
    # Falls back gracefully to the system UI font if files are absent or Qt
    # cannot register them on the current platform.
    from PySide6.QtGui import QFontDatabase
    _fonts_dir = Path(__file__).parent / "assets" / "fonts"
    preferred_body_family: str | None = None
    if _fonts_dir.is_dir():
        for _font_file in _fonts_dir.glob("*.ttf"):
            _fid = QFontDatabase.addApplicationFont(str(_font_file))
            if _fid >= 0:
                _families = QFontDatabase.applicationFontFamilies(_fid)
                print(f"Loaded font: {_font_file.name} → families: {_families}")
                if preferred_body_family is None and "Regular" in _font_file.stem and _families:
                    preferred_body_family = _families[0]

    # Keep the system point size and only switch family when Inter loaded.
    global_font = QFont(system_font)
    if preferred_body_family:
        global_font.setFamily(preferred_body_family)
    app.setFont(global_font)

    splash_theme = str(app_settings.get("ui_theme", "") or "").strip().lower()
    if splash_theme not in {"auto", "light", "dark"}:
        splash_theme = str(SettingsDB.get_setting("ui_theme", "auto") or "auto").strip().lower()
    if splash_theme not in {"auto", "light", "dark"}:
        splash_theme = "auto"
    if app_settings.get("ui_theme") != splash_theme:
        update_app_settings({"ui_theme": splash_theme})
    splash = _create_splash(app, APP_VERSION, theme=splash_theme)
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
    lang_code = _canonical_ui_language(app_settings.get("ui_language"))
    if not lang_code:
        lang_code = _canonical_ui_language(SettingsDB.get_setting("ui_language"))
    if not lang_code:
        lang_code = _canonical_ui_language(QLocale.system().name()) or "en"
        update_app_settings({"ui_language": lang_code})
        SettingsDB.set_setting("ui_language", lang_code)
    if lang_code != "en":
        qm_dir = Path(__file__).parent / "i18n"
        qm_path = qm_dir / f"Sporely_{lang_code}.qm"
        legacy_qm_path = qm_dir / f"{LEGACY_APP_NAME}_{lang_code}.qm"
        if translator.load(str(qm_path if qm_path.exists() else legacy_qm_path)):
            app.installTranslator(translator)
            app._translator = translator

    window = MainWindow(app_version=APP_VERSION)
    if splash:
        if splash_shown_at is not None:
            elapsed = time.monotonic() - splash_shown_at
            min_splash_seconds = 0.5
            if elapsed < min_splash_seconds:
                time.sleep(min_splash_seconds - elapsed)
                app.processEvents()
    window.show()
    if splash:
        splash.finish(window)
    window.start_update_check()

    _exec_started = True
    exit_code = app.exec()
    signal_pump.stop()
    parked_threads = list(getattr(app, "_sporely_parked_threads", set()) or [])
    for thread in parked_threads:
        try:
            if thread is not None and thread.isRunning():
                thread.requestInterruption()
                thread.quit()
                thread.wait(5000)
        except Exception:
            pass
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
