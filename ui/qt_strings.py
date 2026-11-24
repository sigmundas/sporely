"""Qt's own visible strings, translated in Sporely's translation files.

Qt draws some text itself: standard dialog buttons (OK, Cancel, Yes, No,
Close), the message box's Show/Hide Details button, the right-click menu of
text fields and spin boxes, and the macOS application menu. Qt looks these up
under its own contexts (``QPlatformTheme``, ``QMessageBox`` ...) in every
installed translator, so listing them here puts them in Sporely's ``.ts``
files and the ``Sporely_<lang>.qm`` that ``main.py`` installs translates them.

Qt ships no Norwegian Bokmål translation (only Nynorsk), which is why Sporely
carries these itself rather than loading Qt's ``qtbase_<lang>.qm``. The German
and Swedish entries use Qt's own wording, except where Qt's German gives two
entries of one menu the same shortcut letter (``tests/test_qt_strings.py``).

Nothing imports this module at runtime: ``QT_TRANSLATE_NOOP`` only marks the
strings for ``pyside6-lupdate`` (``tools/update_translations.sh``). The source
texts must match Qt's exactly, or Qt never finds them.
"""
from __future__ import annotations

from PySide6.QtCore import QT_TRANSLATE_NOOP

QT_STRINGS: tuple[tuple[str, str], ...] = (
    # Standard buttons (QDialogButtonBox, QMessageBox).
    ("QPlatformTheme", QT_TRANSLATE_NOOP("QPlatformTheme", "OK")),
    ("QPlatformTheme", QT_TRANSLATE_NOOP("QPlatformTheme", "Save")),
    ("QPlatformTheme", QT_TRANSLATE_NOOP("QPlatformTheme", "Save All")),
    ("QPlatformTheme", QT_TRANSLATE_NOOP("QPlatformTheme", "Open")),
    ("QPlatformTheme", QT_TRANSLATE_NOOP("QPlatformTheme", "&Yes")),
    ("QPlatformTheme", QT_TRANSLATE_NOOP("QPlatformTheme", "Yes to &All")),
    ("QPlatformTheme", QT_TRANSLATE_NOOP("QPlatformTheme", "&No")),
    ("QPlatformTheme", QT_TRANSLATE_NOOP("QPlatformTheme", "N&o to All")),
    ("QPlatformTheme", QT_TRANSLATE_NOOP("QPlatformTheme", "Abort")),
    ("QPlatformTheme", QT_TRANSLATE_NOOP("QPlatformTheme", "Retry")),
    ("QPlatformTheme", QT_TRANSLATE_NOOP("QPlatformTheme", "Ignore")),
    ("QPlatformTheme", QT_TRANSLATE_NOOP("QPlatformTheme", "Close")),
    ("QPlatformTheme", QT_TRANSLATE_NOOP("QPlatformTheme", "Cancel")),
    ("QPlatformTheme", QT_TRANSLATE_NOOP("QPlatformTheme", "Discard")),
    ("QPlatformTheme", QT_TRANSLATE_NOOP("QPlatformTheme", "Help")),
    ("QPlatformTheme", QT_TRANSLATE_NOOP("QPlatformTheme", "Apply")),
    ("QPlatformTheme", QT_TRANSLATE_NOOP("QPlatformTheme", "Reset")),
    ("QPlatformTheme", QT_TRANSLATE_NOOP("QPlatformTheme", "Restore Defaults")),
    # macOS names the Discard button itself.
    ("QCocoaTheme", QT_TRANSLATE_NOOP("QCocoaTheme", "Don't Save")),
    # The message box's details button.
    ("QMessageBox", QT_TRANSLATE_NOOP("QMessageBox", "Show Details...")),
    ("QMessageBox", QT_TRANSLATE_NOOP("QMessageBox", "Hide Details...")),
    # Right-click menu of one-line text fields.
    ("QLineEdit", QT_TRANSLATE_NOOP("QLineEdit", "&Undo")),
    ("QLineEdit", QT_TRANSLATE_NOOP("QLineEdit", "&Redo")),
    ("QLineEdit", QT_TRANSLATE_NOOP("QLineEdit", "Cu&t")),
    ("QLineEdit", QT_TRANSLATE_NOOP("QLineEdit", "&Copy")),
    ("QLineEdit", QT_TRANSLATE_NOOP("QLineEdit", "&Paste")),
    ("QLineEdit", QT_TRANSLATE_NOOP("QLineEdit", "Delete")),
    ("QLineEdit", QT_TRANSLATE_NOOP("QLineEdit", "Select All")),
    # Right-click menu of multi-line text fields.
    ("QWidgetTextControl", QT_TRANSLATE_NOOP("QWidgetTextControl", "&Undo")),
    ("QWidgetTextControl", QT_TRANSLATE_NOOP("QWidgetTextControl", "&Redo")),
    ("QWidgetTextControl", QT_TRANSLATE_NOOP("QWidgetTextControl", "Cu&t")),
    ("QWidgetTextControl", QT_TRANSLATE_NOOP("QWidgetTextControl", "&Copy")),
    ("QWidgetTextControl", QT_TRANSLATE_NOOP("QWidgetTextControl", "Copy &Link Location")),
    ("QWidgetTextControl", QT_TRANSLATE_NOOP("QWidgetTextControl", "&Paste")),
    ("QWidgetTextControl", QT_TRANSLATE_NOOP("QWidgetTextControl", "Delete")),
    ("QWidgetTextControl", QT_TRANSLATE_NOOP("QWidgetTextControl", "Select All")),
    # Right-click menu of spin boxes.
    ("QAbstractSpinBox", QT_TRANSLATE_NOOP("QAbstractSpinBox", "&Select All")),
    ("QAbstractSpinBox", QT_TRANSLATE_NOOP("QAbstractSpinBox", "&Step up")),
    ("QAbstractSpinBox", QT_TRANSLATE_NOOP("QAbstractSpinBox", "Step &down")),
    # The macOS application menu (%1 is the application name).
    ("MAC_APPLICATION_MENU", QT_TRANSLATE_NOOP("MAC_APPLICATION_MENU", "Services")),
    ("MAC_APPLICATION_MENU", QT_TRANSLATE_NOOP("MAC_APPLICATION_MENU", "Hide %1")),
    ("MAC_APPLICATION_MENU", QT_TRANSLATE_NOOP("MAC_APPLICATION_MENU", "Hide Others")),
    ("MAC_APPLICATION_MENU", QT_TRANSLATE_NOOP("MAC_APPLICATION_MENU", "Show All")),
    ("MAC_APPLICATION_MENU", QT_TRANSLATE_NOOP("MAC_APPLICATION_MENU", "Preferences...")),
    ("MAC_APPLICATION_MENU", QT_TRANSLATE_NOOP("MAC_APPLICATION_MENU", "Quit %1")),
    ("MAC_APPLICATION_MENU", QT_TRANSLATE_NOOP("MAC_APPLICATION_MENU", "About %1")),
)
