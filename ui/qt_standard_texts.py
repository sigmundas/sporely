"""Qt's own standard button texts, listed so lupdate extracts them.

QDialogButtonBox and QMessageBox standard buttons (OK, Cancel, Yes, No, ...)
take their labels from Qt, which translates them in the ``QPlatformTheme``
context (``QCocoaTheme`` for macOS's "Don't Save"). Qt ships no Norwegian
catalogue for them, so Sporely translates them in its own catalogues: the
Sporely translator answers any context, including Qt's.

Nothing imports this module at runtime; it only has to be scanned by
``tools/update_translations.sh`` (and the PowerShell copy of its list).
The source strings must match Qt's exactly, mnemonics included.
"""

from PySide6.QtCore import QT_TRANSLATE_NOOP

QT_STANDARD_BUTTON_TEXTS = (
    QT_TRANSLATE_NOOP("QPlatformTheme", "OK"),
    QT_TRANSLATE_NOOP("QPlatformTheme", "Save"),
    QT_TRANSLATE_NOOP("QPlatformTheme", "Save All"),
    QT_TRANSLATE_NOOP("QPlatformTheme", "Open"),
    QT_TRANSLATE_NOOP("QPlatformTheme", "&Yes"),
    QT_TRANSLATE_NOOP("QPlatformTheme", "Yes to &All"),
    QT_TRANSLATE_NOOP("QPlatformTheme", "&No"),
    QT_TRANSLATE_NOOP("QPlatformTheme", "N&o to All"),
    QT_TRANSLATE_NOOP("QPlatformTheme", "Abort"),
    QT_TRANSLATE_NOOP("QPlatformTheme", "Retry"),
    QT_TRANSLATE_NOOP("QPlatformTheme", "Ignore"),
    QT_TRANSLATE_NOOP("QPlatformTheme", "Close"),
    QT_TRANSLATE_NOOP("QPlatformTheme", "Cancel"),
    QT_TRANSLATE_NOOP("QPlatformTheme", "Discard"),
    QT_TRANSLATE_NOOP("QPlatformTheme", "Help"),
    QT_TRANSLATE_NOOP("QPlatformTheme", "Apply"),
    QT_TRANSLATE_NOOP("QPlatformTheme", "Reset"),
    QT_TRANSLATE_NOOP("QPlatformTheme", "Restore Defaults"),
    QT_TRANSLATE_NOOP("QCocoaTheme", "Don't Save"),
)
