"""The UI language the running application was started with.

The translator is installed once at startup (``main.py``), so a changed
``ui_language`` setting takes effect only after a restart. Anything that must
agree with the translated UI — such as which national scientific name is shown
— reads the running language from here, not the stored setting.
"""
from __future__ import annotations

APP_PROPERTY = "sporely_ui_language"


def set_running_ui_language(app, code: str | None) -> None:
    """Record the UI language ``app`` was started with (``main.py`` only)."""
    if app is not None:
        app.setProperty(APP_PROPERTY, str(code or ""))


def running_ui_language() -> str | None:
    """Return the running UI locale (``"nb_NO"``, ``"sv_SE"``, ...) or ``None``.

    ``None`` without a running application or when none was recorded; callers
    then show canonical names.
    """
    try:
        from PySide6.QtCore import QCoreApplication
    except Exception:
        return None
    app = QCoreApplication.instance()
    if app is None:
        return None
    value = app.property(APP_PROPERTY)
    text = str(value or "").strip()
    return text or None
