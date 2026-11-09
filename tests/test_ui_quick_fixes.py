"""Desktop UI redesign stage 0: quick fixes.

Behaviour tests on the real production widgets for the fixes in stage 0 of
docs/plans/active/2026-11-08-desktop-ui-redesign.md, plus two guards against
the root cause of most of the English text in the translated UI: modules
whose tr() strings lupdate never scanned.
"""
from __future__ import annotations

import os
import re
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QTranslator, Qt
from PySide6.QtWidgets import QApplication, QDialogButtonBox, QScrollArea

from database import schema

ROOT = Path(__file__).resolve().parents[1]
LANGUAGES = ("nb_NO", "sv_SE", "de_DE")


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


@pytest.fixture(autouse=True)
def isolated_settings(monkeypatch, tmp_path):
    """Keep window geometry, splitter sizes and objectives out of the user's files."""
    import ui.image_import_dialog as image_import_dialog
    import ui.window_state as window_state
    from PySide6.QtCore import QSettings

    path = tmp_path / "settings.ini"
    factory = lambda *_args: QSettings(str(path), QSettings.IniFormat)
    monkeypatch.setattr(window_state, "QSettings", factory)
    monkeypatch.setattr(image_import_dialog, "QSettings", factory)
    monkeypatch.setattr(schema, "get_objectives_path", lambda: tmp_path / "objectives.json")


@pytest.fixture
def scratch_db(monkeypatch, tmp_path):
    monkeypatch.setattr(schema, "get_app_settings", lambda: {})
    monkeypatch.setattr(schema, "get_database_path", lambda: tmp_path / "mushrooms.db")
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: tmp_path / "reference_values.db")
    monkeypatch.setattr(
        schema, "get_bundled_reference_database_path", lambda: tmp_path / "no-bundled.db"
    )
    monkeypatch.setattr(schema, "get_images_dir", lambda: tmp_path / "images")
    schema.init_database()
    yield tmp_path


@contextmanager
def _translator(app, locale: str = "nb_NO"):
    """Install the compiled catalogue before widgets are built, as main.py does."""
    translator = QTranslator(app)
    assert translator.load(str(ROOT / "i18n" / f"Sporely_{locale}.qm"))
    app.installTranslator(translator)
    try:
        yield
    finally:
        app.removeTranslator(translator)


# ---------------------------------------------------------------------------
# Translation coverage guards
# ---------------------------------------------------------------------------

_TR_CALL = re.compile(r"(self\.tr|QCoreApplication\.translate|QT_TRANSLATE_NOOP|QT_TR_NOOP)\(")


def _scanned_modules() -> set[str]:
    script = (ROOT / "tools" / "update_translations.sh").read_text(encoding="utf-8")
    return set(re.findall(r'"([\w/]+\.py)"', script))


def test_every_ui_module_with_tr_calls_is_scanned_by_lupdate():
    scanned = _scanned_modules()
    missing = sorted(
        str(path.relative_to(ROOT))
        for path in (ROOT / "ui").glob("*.py")
        if _TR_CALL.search(path.read_text(encoding="utf-8"))
        and str(path.relative_to(ROOT)) not in scanned
    )
    assert missing == [], (
        "These modules call tr() but tools/update_translations.sh does not scan "
        f"them, so their strings are never translated: {missing}"
    )


@pytest.mark.parametrize("locale", LANGUAGES)
def test_translation_catalogue_has_no_unfinished_entries(locale):
    root = ET.parse(ROOT / "i18n" / f"Sporely_{locale}.ts").getroot()
    unfinished = []
    for message in root.iter("message"):
        if not (message.findtext("source") or "").strip():
            continue  # tr("") needs no translation
        translation = message.find("translation")
        has_text = bool(translation.text) or translation.find("numerusform") is not None
        if translation.get("type") == "unfinished" or not has_text:
            unfinished.append(message.findtext("source"))
    assert unfinished == []


def test_old_ingestion_hub_name_is_gone_from_ui_strings():
    offenders = [
        str(path.relative_to(ROOT))
        for path in (ROOT / "ui").glob("*.py")
        if "Ingestion Hub" in path.read_text(encoding="utf-8")
    ]
    assert offenders == []


# ---------------------------------------------------------------------------
# Prepare Images: one Close button when there is nothing to save
# ---------------------------------------------------------------------------


def test_prepare_images_edit_mode_shows_single_close_until_dirty(qapp, scratch_db):
    from ui.image_import_dialog import ImageImportDialog

    dialog = ImageImportDialog(continue_to_observation_details=False)
    try:
        assert dialog.next_btn.text() == "Close"
        assert dialog.cancel_btn.isHidden(), "a second Close/Cancel button is shown"

        dialog._mark_dirty()

        assert not dialog.cancel_btn.isHidden()
        assert dialog.cancel_btn.text() == "Discard"
        assert dialog.next_btn.text() == "Save changes"
    finally:
        dialog.close()


def test_prepare_images_continue_mode_keeps_cancel(qapp, scratch_db):
    from ui.image_import_dialog import ImageImportDialog

    dialog = ImageImportDialog(continue_to_observation_details=True)
    try:
        assert not dialog.cancel_btn.isHidden()
        assert dialog.cancel_btn.text() == "Cancel"
        assert dialog.next_btn.text() == "Continue"
    finally:
        dialog.close()


# ---------------------------------------------------------------------------
# Analysis: comparison source badges are translated
# ---------------------------------------------------------------------------


def test_comparison_source_badges_are_translated(qapp):
    from ui.comparison_panel import SourceKind, _source_kind_label

    assert _source_kind_label(SourceKind.OBSERVATION) == "Observation"
    with _translator(qapp):
        assert _source_kind_label(SourceKind.OBSERVATION) == "Observasjon"
        assert _source_kind_label(SourceKind.LIBRARY) == "Bibliotek"
        assert _source_kind_label(SourceKind.MY_OBS) == "Mine observasjoner"


# ---------------------------------------------------------------------------
# Reference library: aligned rows, translated Close and missing-metadata hint
# ---------------------------------------------------------------------------


def _seed_work(label: str, year: int | None):
    from database.reference_library import ReferenceWork, ReferenceWorkRepository

    return ReferenceWorkRepository.create(
        ReferenceWork(id="", type="book", title=label, short_label=label, year=year)
    )


def test_reference_library_works_table_elides_on_one_line(qapp, scratch_db):
    from ui.reference_library_manager_dialog import ReferenceLibraryManagerDialog

    label = "Funga Nordica: Agaricoid, boletoid, clavarioid, cyphelloid and gastroid genera"
    _seed_work(label, 2008)
    dialog = ReferenceLibraryManagerDialog(None, active_observation_id=None)
    try:
        table = dialog.works_table
        assert table.wordWrap() is False
        assert table.textElideMode() == Qt.ElideRight
        assert table.item(0, 0).text() == label
        assert table.item(0, 0).toolTip() == label
        assert table.item(0, 1).text() == "2008"
    finally:
        dialog.close()


def test_reference_library_close_and_hint_are_translated(qapp, scratch_db):
    from ui.reference_library_manager_dialog import (
        ReferenceLibraryManagerDialog,
        _CompletenessHintLabel,
    )

    with _translator(qapp):
        dialog = ReferenceLibraryManagerDialog(None, active_observation_id=None)
        try:
            buttons = dialog.findChildren(QDialogButtonBox)
            texts = [button.text() for box in buttons for button in box.buttons()]
            assert "Lukk" in texts
            assert "Close" not in texts
        finally:
            dialog.close()
        hint = _CompletenessHintLabel()
        hint.set_hints(["missing year", "missing authors"])
        assert hint.text() == "Mangler år, Mangler forfattere"


# ---------------------------------------------------------------------------
# Preferences: the publishing page scrolls instead of overlapping
# ---------------------------------------------------------------------------


def test_publishing_page_does_not_overlap_at_default_size(qapp, scratch_db, monkeypatch, tmp_path):
    import ui.main_window as main_window
    from utils.cloud_sync import SporelyCloudClient

    monkeypatch.setattr(main_window, "app_data_dir", lambda: tmp_path / "app-data")
    monkeypatch.setattr(
        SporelyCloudClient, "from_stored_credentials", classmethod(lambda cls: None)
    )
    monkeypatch.setattr(
        main_window.ArtsobservasjonerSettingsDialog,
        "_refresh_target_status",
        lambda dialog: setattr(dialog, "_target_status", {}),
    )
    hub = main_window.SettingsHubDialog(None, start_page=main_window.SettingsHubDialog.PAGE_PUBLISHING)
    try:
        hub.resize(960, 680)
        hub.show()
        qapp.processEvents()

        page = hub._stack.widget(main_window.SettingsHubDialog.PAGE_PUBLISHING)
        assert isinstance(page, QScrollArea)
        content = page.widget()
        notice = hub._show_publish_notice_check
        watermark = hub._artsobs_dialog.include_copyright_checkbox

        def rect_in_content(widget):
            top_left = widget.mapTo(content, widget.rect().topLeft())
            return widget.rect().translated(top_left)

        assert not rect_in_content(notice).intersects(rect_in_content(watermark.parentWidget()))
        assert rect_in_content(notice).top() >= rect_in_content(hub._artsobs_dialog).bottom()
    finally:
        hub.close()
