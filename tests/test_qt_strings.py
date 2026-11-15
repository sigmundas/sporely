"""Qt's own visible strings (standard buttons, the message box's details
button, text-field menus) translate through Sporely's .qm files
(``ui/qt_strings.py``)."""

from __future__ import annotations

from pathlib import Path

import pytest
from PySide6.QtCore import QCoreApplication, QLibraryInfo, QTranslator
from PySide6.QtWidgets import (
    QDialogButtonBox,
    QLineEdit,
    QPlainTextEdit,
    QPushButton,
)

from ui.dialog_helpers import build_message_box
from ui.qt_strings import QT_STRINGS

I18N = Path(__file__).resolve().parent.parent / "i18n"
LANGS = ("nb_NO", "sv_SE", "de_DE")


@pytest.fixture
def sporely_language(qapp):
    installed: list[QTranslator] = []

    def install(lang: str) -> None:
        translator = QTranslator()
        assert translator.load(str(I18N / f"Sporely_{lang}.qm"))
        QCoreApplication.installTranslator(translator)
        installed.append(translator)

    yield install
    for translator in installed:
        QCoreApplication.removeTranslator(translator)


@pytest.mark.parametrize("lang", LANGS)
def test_every_listed_qt_string_is_translated(lang):
    translator = QTranslator()
    assert translator.load(str(I18N / f"Sporely_{lang}.qm"))
    untranslated = [
        (context, source) for context, source in QT_STRINGS
        if not translator.translate(context, source)
    ]
    assert untranslated == []


@pytest.mark.parametrize("lang, qt_lang", [("sv_SE", "sv"), ("de_DE", "de")])
def test_sources_match_qt_and_wording_follows_qt(lang, qt_lang):
    """A source text that differs from Qt's by a character is never looked
    up; Qt's own file finding every one proves the sources are Qt's."""
    qt_translator = QTranslator()
    qt_dir = QLibraryInfo.path(QLibraryInfo.LibraryPath.TranslationsPath)
    if not qt_translator.load(f"qtbase_{qt_lang}", qt_dir):
        pytest.skip("this PySide6 install has no Qt translations")
    ours = QTranslator()
    assert ours.load(str(I18N / f"Sporely_{lang}.qm"))
    for context, source in QT_STRINGS:
        expected = qt_translator.translate(context, source)
        assert expected, (context, source)
        expected = OUR_WORDING.get((lang, context, source), expected)
        assert ours.translate(context, source) == expected, (context, source)


# Where Sporely departs from Qt's wording on purpose: Qt's German spin-box
# menu gives "Alles auswählen" the A that "Ausschneiden" already has.
OUR_WORDING = {
    ("de_DE", "QAbstractSpinBox", "&Select All"): "Alles a&uswählen",
}

# The menus Qt builds from these entries (QAbstractSpinBox reuses the line
# edit's menu, replaces its Select All and adds the two steps).
_EDIT = ("&Undo", "&Redo", "Cu&t", "&Copy", "&Paste", "Delete")
MENUS = {
    "line edit": [("QLineEdit", s) for s in (*_EDIT, "Select All")],
    "text edit": [("QWidgetTextControl", s) for s in (*_EDIT, "Copy &Link Location", "Select All")],
    "spin box": [("QLineEdit", s) for s in _EDIT]
    + [("QAbstractSpinBox", s) for s in ("&Select All", "&Step up", "Step &down")],
}


def _mnemonic(label: str) -> str | None:
    index = label.replace("&&", "").find("&")
    return label.replace("&&", "")[index + 1].casefold() if index >= 0 else None


@pytest.mark.parametrize("lang", LANGS)
@pytest.mark.parametrize("menu", sorted(MENUS))
def test_no_menu_gives_two_entries_one_shortcut_letter(lang, menu):
    translator = QTranslator()
    assert translator.load(str(I18N / f"Sporely_{lang}.qm"))
    letters = [_mnemonic(translator.translate(c, s)) for c, s in MENUS[menu]]
    letters = [letter for letter in letters if letter]
    assert len(letters) == len(set(letters)), letters


@pytest.mark.parametrize("lang, cancel, close, yes", [
    ("nb_NO", "Avbryt", "Lukk", "&Ja"),
    ("sv_SE", "Avbryt", "Stäng", "&Ja"),
    ("de_DE", "Abbrechen", "Schließen", "&Ja"),
])
def test_standard_buttons_are_translated(sporely_language, lang, cancel, close, yes):
    sporely_language(lang)
    box = QDialogButtonBox(
        QDialogButtonBox.StandardButton.Cancel
        | QDialogButtonBox.StandardButton.Close
        | QDialogButtonBox.StandardButton.Yes
    )
    try:
        texts = {
            box.button(QDialogButtonBox.StandardButton.Cancel).text(),
            box.button(QDialogButtonBox.StandardButton.Close).text(),
            box.button(QDialogButtonBox.StandardButton.Yes).text(),
        }
        assert texts == {cancel, close, yes}
    finally:
        box.deleteLater()


def test_standard_buttons_stay_english_without_a_translation(qapp):
    box = QDialogButtonBox(QDialogButtonBox.StandardButton.Cancel)
    try:
        assert box.button(QDialogButtonBox.StandardButton.Cancel).text() == "Cancel"
    finally:
        box.deleteLater()


@pytest.mark.parametrize("lang, show, hide", [
    ("nb_NO", "Vis detaljer...", "Skjul detaljer..."),
    ("sv_SE", "Visa detaljer...", "Dölj detaljer..."),
    ("de_DE", "Details einblenden...", "Details ausblenden..."),
])
def test_message_box_details_button_is_translated(sporely_language, qtbot, lang, show, hide):
    sporely_language(lang)
    box = build_message_box(None, "warning", "Import", "The import did not finish.",
                            details="disk full")
    qtbot.addWidget(box)
    box.show()
    texts = {button.text() for button in box.findChildren(QPushButton)}
    assert show in texts
    details_button = next(b for b in box.findChildren(QPushButton) if b.text() == show)
    details_button.click()
    assert details_button.text() == hide


@pytest.mark.parametrize("widget_class", [QLineEdit, QPlainTextEdit])
def test_text_field_menu_is_translated(sporely_language, qtbot, widget_class):
    sporely_language("nb_NO")
    field = widget_class()
    qtbot.addWidget(field)
    menu = field.createStandardContextMenu()
    try:
        labels = [action.text().split("\t")[0] for action in menu.actions() if action.text()]
        assert {"&Angre", "&Gjør om", "Klipp &ut", "&Kopier", "&Lim inn", "Slett", "Merk alt"} <= set(labels)
    finally:
        menu.deleteLater()
