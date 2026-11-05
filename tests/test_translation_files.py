"""The shipped translations: every supported UI language has its own
compiled Sporely_<lang>.qm. main.py has no fallback file, so a missing one
would silently show the app in English."""

from pathlib import Path

import pytest
from PySide6.QtCore import QTranslator

I18N = Path(__file__).resolve().parent.parent / "i18n"


@pytest.mark.parametrize("lang, expected", [
    ("nb_NO", "En nyere versjon av Sporely er tilgjengelig."),
    ("sv_SE", "En nyare version av Sporely finns tillgänglig."),
    ("de_DE", "Eine neuere Version von Sporely ist verfügbar."),
])
def test_each_language_loads_its_own_translation(lang, expected):
    translator = QTranslator()
    assert translator.load(str(I18N / f"Sporely_{lang}.qm"))
    assert translator.translate("MainWindow", "A newer version of Sporely is available.") == expected


def test_the_supported_languages_are_the_shipped_ones():
    from main import _canonical_ui_language

    supported = {_canonical_ui_language(code) for code in ("nb", "sv", "de")}
    assert {p.stem.removeprefix("Sporely_") for p in I18N.glob("*.qm")} == supported
    assert {p.stem.removeprefix("Sporely_") for p in I18N.glob("*.ts")} == supported
