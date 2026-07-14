"""Live-widget presence tests for the Sample / Sample source split.

The tag-module + DB tests in test_sample_source_split.py pin the data model.
These tests pin the visible UI — they instantiate the real widgets in an
offscreen Qt app and inspect the resulting layout so any future removal or
rename of the "Sample source" row / tab breaks CI, not the user's session.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QFormLayout, QWidget

from database import schema
from ui.database_settings_dialog import DatabaseSettingsDialog
from ui.image_import_dialog import ImageImportDialog
from ui.live_lab_tab import LiveLabTab


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


@pytest.fixture
def scratch_db(monkeypatch, tmp_path):
    """Point the app's DB helpers at a temp dir so widget init doesn't touch real data."""
    monkeypatch.setattr(schema, "get_app_settings", lambda: {})
    monkeypatch.setattr(schema, "get_database_path", lambda: tmp_path / "mushrooms.db")
    monkeypatch.setattr(schema, "get_images_dir", lambda: tmp_path / "images")
    monkeypatch.setattr(schema, "init_reference_database", lambda *a, **kw: None)
    schema.init_database()
    yield tmp_path


# ---------------------------------------------------------------------------
# Live Lab — Sample source row is visible in the microscope panel
# ---------------------------------------------------------------------------


def _find_form_row(widget: QWidget, target_field: QWidget) -> tuple[QFormLayout, int, str] | None:
    """Return (form, row_index, label_text) for the QFormLayout row containing target_field."""
    for form in widget.findChildren(QFormLayout):
        for row in range(form.rowCount()):
            field_item = form.itemAt(row, QFormLayout.FieldRole)
            if field_item is None or field_item.widget() is not target_field:
                continue
            label_item = form.itemAt(row, QFormLayout.LabelRole)
            label_widget = label_item.widget() if label_item else None
            label_text = label_widget.text() if label_widget is not None else ""
            return form, row, label_text
    return None


def test_live_lab_microscope_form_shows_sample_and_sample_source_rows(qapp, scratch_db):
    fake_main = QWidget()
    fake_main.raw_capture_service = None
    tab = LiveLabTab(main_window=fake_main)

    # Both combos must exist.
    assert getattr(tab, "sample_combo", None) is not None
    assert getattr(tab, "sample_source_combo", None) is not None

    sample_row = _find_form_row(tab, tab.sample_combo)
    source_row = _find_form_row(tab, tab.sample_source_combo)
    assert sample_row is not None, "sample_combo not found in any QFormLayout — Sample row missing from UI"
    assert source_row is not None, "sample_source_combo not found in any QFormLayout — Sample source row missing from UI"

    _, sample_idx, sample_label = sample_row
    _, source_idx, source_label = source_row

    assert sample_label.rstrip(":").strip() == "Sample"
    assert source_label.rstrip(":").strip() == "Sample source"
    # Source must sit directly under Sample so operators associate the two.
    assert source_idx == sample_idx + 1


def test_live_lab_sample_combo_excludes_spore_print(qapp, scratch_db):
    fake_main = QWidget()
    fake_main.raw_capture_service = None
    tab = LiveLabTab(main_window=fake_main)

    values = [tab.sample_combo.itemData(i) for i in range(tab.sample_combo.count())]
    assert "Spore_print" not in values, (
        "Sample dropdown must not offer Spore_print — it is a source, not a condition"
    )
    assert "Fresh" in values
    assert "Dried" in values


def test_live_lab_sample_source_combo_offers_expected_sources(qapp, scratch_db):
    fake_main = QWidget()
    fake_main.raw_capture_service = None
    tab = LiveLabTab(main_window=fake_main)

    values = [tab.sample_source_combo.itemData(i) for i in range(tab.sample_source_combo.count())]
    # Order and content are user-facing — pin both.
    assert values == ["Not_set", "Spore_print", "Hymenium", "Stipe", "Pileus", "Context", "Other"]


# ---------------------------------------------------------------------------
# Preferences → Database → Microscope tags — Sample sources tab is visible
# ---------------------------------------------------------------------------


def _tab_labels(dlg: DatabaseSettingsDialog) -> list[str]:
    tabs = dlg.tag_tabs
    return [tabs.tabText(i) for i in range(tabs.count())]


def test_database_settings_dialog_has_sample_sources_tab(qapp, scratch_db):
    dlg = DatabaseSettingsDialog()
    labels = _tab_labels(dlg)

    assert "Sample sources" in labels, f"Sample sources tab missing — found {labels!r}"
    # Sample types remains its own tab; the two are not merged.
    assert "Sample types" in labels
    # Sample sources sits next to Sample types.
    assert labels.index("Sample sources") == labels.index("Sample types") + 1


def test_database_settings_sample_types_tab_no_longer_lists_spore_print(qapp, scratch_db):
    dlg = DatabaseSettingsDialog()
    sample_list = dlg._tag_lists["sample"]
    entries = [sample_list.item(row).text() for row in range(sample_list.count())]

    assert "Spore print" not in entries, (
        "Sample types must not list Spore print — it is a source, not a condition"
    )
    assert "Fresh" in entries
    assert "Dried" in entries


def test_database_settings_sample_sources_tab_lists_expected_entries(qapp, scratch_db):
    dlg = DatabaseSettingsDialog()
    source_list = dlg._tag_lists["sample_source"]
    entries = [source_list.item(row).text() for row in range(source_list.count())]

    assert entries == ["Not set", "Spore print", "Hymenium", "Stipe", "Pileus", "Context", "Other"]


# ---------------------------------------------------------------------------
# Prepare Images (ImageImportDialog) — microscope panel exposes Sample source
# ---------------------------------------------------------------------------


def test_prepare_images_microscope_form_shows_sample_and_sample_source_rows(qapp, scratch_db):
    dlg = ImageImportDialog()

    assert getattr(dlg, "sample_combo", None) is not None
    assert getattr(dlg, "sample_source_combo", None) is not None

    sample_row = _find_form_row(dlg, dlg.sample_combo)
    source_row = _find_form_row(dlg, dlg.sample_source_combo)
    assert sample_row is not None, "sample_combo missing from any Prepare Images form"
    assert source_row is not None, "sample_source_combo missing — Sample source row not in Prepare Images"

    _, sample_idx, sample_label = sample_row
    _, source_idx, source_label = source_row

    assert sample_label.rstrip(":").strip() == "Sample"
    assert source_label.rstrip(":").strip() == "Sample source"
    # Source must sit directly under Sample so operators associate the two.
    assert source_idx == sample_idx + 1


def test_prepare_images_sample_combo_excludes_spore_print(qapp, scratch_db):
    dlg = ImageImportDialog()

    values = [dlg.sample_combo.itemData(i) for i in range(dlg.sample_combo.count())]
    assert "Spore_print" not in values, (
        "Prepare Images Sample dropdown must not offer Spore_print"
    )
    assert "Fresh" in values
    assert "Dried" in values


def test_prepare_images_sample_source_combo_offers_expected_sources(qapp, scratch_db):
    dlg = ImageImportDialog()

    values = [dlg.sample_source_combo.itemData(i) for i in range(dlg.sample_source_combo.count())]
    assert values == ["Not_set", "Spore_print", "Hymenium", "Stipe", "Pileus", "Context", "Other"]


def test_prepare_images_sample_source_apply_writes_result_field(qapp, scratch_db):
    """When the microscope radio is active, apply must persist source into the result row."""
    from ui.image_import_dialog import ImageImportResult, IMAGE_IMPORT_STATUS_COMMITTED

    dlg = ImageImportDialog()
    result = ImageImportResult(
        filepath="/tmp/fake.jpg",
        preview_path="/tmp/fake.jpg",
        image_type="microscope",
        status=IMAGE_IMPORT_STATUS_COMMITTED,
    )
    dlg.import_results = [result]
    dlg.image_paths = [result.filepath]
    dlg.selected_index = 0
    dlg.selected_indices = [0]
    dlg.micro_radio.setChecked(True)

    idx = dlg.sample_source_combo.findData("Stipe")
    assert idx >= 0
    dlg.sample_source_combo.setCurrentIndex(idx)
    dlg._apply_settings_to_index(0, action="sample_source")

    assert result.sample_source == "Stipe", (
        f"apply did not persist sample_source; got {result.sample_source!r}"
    )


def test_prepare_images_import_result_dataclass_has_sample_source_field():
    """ImageImportResult must expose sample_source so downstream commit paths persist it."""
    from ui.image_import_dialog import ImageImportResult

    result = ImageImportResult(filepath="/tmp/x", preview_path="/tmp/x")
    assert hasattr(result, "sample_source")
    result.sample_source = "Hymenium"
    assert result.sample_source == "Hymenium"
