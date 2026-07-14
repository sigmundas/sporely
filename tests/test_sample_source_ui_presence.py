"""Live-widget presence tests for the microscope / slide-prep panel refactor.

The tag-module + DB tests in test_sample_source_split.py pin the data model.
These tests pin the visible UI — they instantiate the real widgets in an
offscreen Qt app and inspect the resulting layout so any future removal or
rename of the "Sample source" row / SLIDE / PREP group / short labels breaks
CI, not the user's session.
"""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QFormLayout, QPushButton, QWidget

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
# helpers
# ---------------------------------------------------------------------------


def _find_form_row(root: QWidget, target_field: QWidget) -> tuple[QFormLayout, int, str] | None:
    """Return (form, row_index, label_text) for the QFormLayout row containing target_field."""
    for form in root.findChildren(QFormLayout):
        for row in range(form.rowCount()):
            field_item = form.itemAt(row, QFormLayout.FieldRole)
            if field_item is None or field_item.widget() is not target_field:
                continue
            label_item = form.itemAt(row, QFormLayout.LabelRole)
            label_widget = label_item.widget() if label_item else None
            label_text = label_widget.text() if label_widget is not None else ""
            return form, row, label_text
    return None


def _is_descendant(child: QWidget, ancestor: QWidget) -> bool:
    node = child
    while node is not None:
        if node is ancestor:
            return True
        node = node.parentWidget()
    return False


def _sample_source_pill_texts(root: QWidget, combo: QWidget) -> list[str]:
    return [b.text() for b in combo.findChildren(QPushButton)]


def _sample_source_pill_tooltips(combo: QWidget) -> list[str]:
    return [b.toolTip() for b in combo.findChildren(QPushButton)]


def _live_lab(scratch_db) -> LiveLabTab:
    fake_main = QWidget()
    fake_main.raw_capture_service = None
    return LiveLabTab(main_window=fake_main)


# ---------------------------------------------------------------------------
# Live Lab — MICROSCOPE vs SLIDE / PREP grouping
# ---------------------------------------------------------------------------


def test_live_lab_side_panel_splits_microscope_and_slide_prep_into_separate_groups(qapp, scratch_db):
    tab = _live_lab(scratch_db)

    assert getattr(tab, "microscope_group", None) is not None, "microscope_group card missing"
    assert getattr(tab, "slide_prep_group", None) is not None, "slide_prep_group card missing"
    assert tab.microscope_group is not tab.slide_prep_group
    # The old combined 'sample' card must not linger.
    assert not hasattr(tab, "sample_group"), (
        "sample_group must be removed — its rows now live in slide_prep_group"
    )

    # Optical microscope settings belong in MICROSCOPE only.
    assert _is_descendant(tab.objective_combo, tab.microscope_group)
    assert _is_descendant(tab.contrast_combo, tab.microscope_group)
    assert not _is_descendant(tab.mount_combo, tab.microscope_group), (
        "mount_combo must not live in MICROSCOPE — it is slide/prep metadata"
    )
    assert not _is_descendant(tab.stain_combo, tab.microscope_group), (
        "stain_combo must not live in MICROSCOPE — it is slide/prep metadata"
    )

    # Slide/prep metadata belongs in SLIDE / PREP.
    assert _is_descendant(tab.mount_combo, tab.slide_prep_group)
    assert _is_descendant(tab.stain_combo, tab.slide_prep_group)
    assert _is_descendant(tab.sample_combo, tab.slide_prep_group)
    assert _is_descendant(tab.sample_source_combo, tab.slide_prep_group)


def test_live_lab_slide_prep_group_appears_below_microscope_group(qapp, scratch_db):
    tab = _live_lab(scratch_db)

    tab.resize(720, 900)
    tab.show()
    qapp.processEvents()

    micro_top = tab.microscope_group.mapTo(tab, tab.microscope_group.rect().topLeft()).y()
    prep_top = tab.slide_prep_group.mapTo(tab, tab.slide_prep_group.rect().topLeft()).y()
    assert prep_top > micro_top, (
        f"SLIDE / PREP must sit below MICROSCOPE; got micro_top={micro_top}, prep_top={prep_top}"
    )


def test_live_lab_slide_prep_rows_use_short_labels(qapp, scratch_db):
    """Row labels in the compact panel: Mount / Stain / Condition / Source — no 'Sample:'/'Sample source:'."""
    tab = _live_lab(scratch_db)

    for combo, expected_label in (
        (tab.mount_combo, "Mount"),
        (tab.stain_combo, "Stain"),
        (tab.sample_combo, "Condition"),
        (tab.sample_source_combo, "Source"),
    ):
        row = _find_form_row(tab.slide_prep_group, combo)
        assert row is not None, f"{expected_label} row missing from SLIDE / PREP"
        _, _, label_text = row
        assert label_text.rstrip(":").strip() == expected_label, (
            f"Expected row label {expected_label!r}, got {label_text!r}"
        )


def test_live_lab_sample_combo_excludes_spore_print(qapp, scratch_db):
    tab = _live_lab(scratch_db)

    values = [tab.sample_combo.itemData(i) for i in range(tab.sample_combo.count())]
    assert "Spore_print" not in values, (
        "Condition dropdown must not offer Spore_print — it is a source, not a condition"
    )
    assert "Fresh" in values
    assert "Dried" in values


def test_live_lab_sample_source_combo_offers_expected_sources(qapp, scratch_db):
    tab = _live_lab(scratch_db)

    values = [tab.sample_source_combo.itemData(i) for i in range(tab.sample_source_combo.count())]
    # Stored canonical values remain — this is a label refactor only.
    assert values == ["Not_set", "Spore_print", "Hymenium", "Stipe", "Pileus", "Context", "Other"]


def test_live_lab_sample_source_uses_short_pill_labels(qapp, scratch_db):
    """Compact panel must render Spore_print as 'Print' — canonical value unchanged."""
    tab = _live_lab(scratch_db)

    pill_texts = _sample_source_pill_texts(tab, tab.sample_source_combo)
    assert "Print" in pill_texts, f"'Print' pill missing — got {pill_texts!r}"
    assert "Spore print" not in pill_texts, (
        "Compact pill row must use the short label 'Print', not 'Spore print'"
    )
    assert "Context" in pill_texts
    assert "Hymenium" in pill_texts
    assert tab.sample_source_combo.findData("Spore_print") >= 0, (
        "canonical Spore_print value missing from combo"
    )


def test_live_lab_sample_source_pill_tooltips_describe_each_source(qapp, scratch_db):
    tab = _live_lab(scratch_db)

    tooltips = _sample_source_pill_tooltips(tab.sample_source_combo)
    joined = " | ".join(tooltips).lower()
    assert "spore print" in joined
    assert "gills" in joined or "hymenium" in joined
    assert "stem" in joined
    assert "cap" in joined
    assert "trama" in joined or "flesh" in joined


# ---------------------------------------------------------------------------
# Prepare Images — same MICROSCOPE / SLIDE / PREP grouping
# ---------------------------------------------------------------------------


def test_prepare_images_splits_microscope_and_slide_prep_into_separate_groups(qapp, scratch_db):
    dlg = ImageImportDialog()

    assert getattr(dlg, "micro_settings_group", None) is not None, "micro_settings_group missing"
    assert getattr(dlg, "slide_prep_group", None) is not None, "slide_prep_group missing"
    assert dlg.micro_settings_group is not dlg.slide_prep_group

    # Optical microscope settings belong in MICROSCOPE only.
    assert _is_descendant(dlg.objective_combo, dlg.micro_settings_group)
    assert _is_descendant(dlg.contrast_combo, dlg.micro_settings_group)
    assert not _is_descendant(dlg.mount_combo, dlg.micro_settings_group), (
        "mount_combo must not live under MICROSCOPE in Prepare Images"
    )
    assert not _is_descendant(dlg.stain_combo, dlg.micro_settings_group), (
        "stain_combo must not live under MICROSCOPE in Prepare Images"
    )

    # Slide/prep metadata belongs in SLIDE / PREP.
    assert _is_descendant(dlg.mount_combo, dlg.slide_prep_group)
    assert _is_descendant(dlg.stain_combo, dlg.slide_prep_group)
    assert _is_descendant(dlg.sample_combo, dlg.slide_prep_group)
    assert _is_descendant(dlg.sample_source_combo, dlg.slide_prep_group)


def test_prepare_images_slide_prep_group_appears_below_microscope_group(qapp, scratch_db):
    dlg = ImageImportDialog()
    dlg.resize(1400, 850)
    dlg.show()
    qapp.processEvents()

    micro_top = dlg.micro_settings_group.mapTo(dlg, dlg.micro_settings_group.rect().topLeft()).y()
    prep_top = dlg.slide_prep_group.mapTo(dlg, dlg.slide_prep_group.rect().topLeft()).y()
    assert prep_top > micro_top, (
        f"Prepare Images: SLIDE / PREP must sit below MICROSCOPE; "
        f"got micro_top={micro_top}, prep_top={prep_top}"
    )


def test_prepare_images_slide_prep_rows_use_short_labels(qapp, scratch_db):
    dlg = ImageImportDialog()

    for combo, expected_label in (
        (dlg.mount_combo, "Mount"),
        (dlg.stain_combo, "Stain"),
        (dlg.sample_combo, "Condition"),
        (dlg.sample_source_combo, "Source"),
    ):
        row = _find_form_row(dlg.slide_prep_group, combo)
        assert row is not None, f"{expected_label} row missing from Prepare Images SLIDE / PREP"
        _, _, label_text = row
        assert label_text.rstrip(":").strip() == expected_label, (
            f"Expected row label {expected_label!r}, got {label_text!r}"
        )


def test_prepare_images_sample_combo_excludes_spore_print(qapp, scratch_db):
    dlg = ImageImportDialog()

    values = [dlg.sample_combo.itemData(i) for i in range(dlg.sample_combo.count())]
    assert "Spore_print" not in values, (
        "Prepare Images Condition dropdown must not offer Spore_print"
    )
    assert "Fresh" in values
    assert "Dried" in values


def test_prepare_images_sample_source_combo_offers_expected_sources(qapp, scratch_db):
    dlg = ImageImportDialog()

    values = [dlg.sample_source_combo.itemData(i) for i in range(dlg.sample_source_combo.count())]
    assert values == ["Not_set", "Spore_print", "Hymenium", "Stipe", "Pileus", "Context", "Other"]


def test_prepare_images_sample_source_uses_short_pill_labels(qapp, scratch_db):
    dlg = ImageImportDialog()

    pill_texts = _sample_source_pill_texts(dlg, dlg.sample_source_combo)
    assert "Print" in pill_texts, (
        f"Prepare Images sample_source_combo must show 'Print' pill; got {pill_texts!r}"
    )
    assert "Spore print" not in pill_texts, (
        "Prepare Images must use the short 'Print' label, not 'Spore print'"
    )
    assert dlg.sample_source_combo.findData("Spore_print") >= 0


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
    from ui.image_import_dialog import ImageImportResult

    result = ImageImportResult(filepath="/tmp/x", preview_path="/tmp/x")
    assert hasattr(result, "sample_source")
    result.sample_source = "Hymenium"
    assert result.sample_source == "Hymenium"


# ---------------------------------------------------------------------------
# Database Settings — still uses the fuller labels
# ---------------------------------------------------------------------------


def _tab_labels(dlg: DatabaseSettingsDialog) -> list[str]:
    tabs = dlg.tag_tabs
    return [tabs.tabText(i) for i in range(tabs.count())]


def test_database_settings_dialog_has_sample_sources_tab(qapp, scratch_db):
    dlg = DatabaseSettingsDialog()
    labels = _tab_labels(dlg)

    assert "Sample sources" in labels, f"Sample sources tab missing — found {labels!r}"
    assert "Sample types" in labels
    assert labels.index("Sample sources") == labels.index("Sample types") + 1


def test_database_settings_sample_types_tab_no_longer_lists_spore_print(qapp, scratch_db):
    dlg = DatabaseSettingsDialog()
    sample_list = dlg._tag_lists["sample"]
    entries = [sample_list.item(row).text() for row in range(sample_list.count())]

    assert "Spore print" not in entries
    assert "Fresh" in entries
    assert "Dried" in entries


def test_database_settings_sample_sources_tab_lists_expected_entries(qapp, scratch_db):
    dlg = DatabaseSettingsDialog()
    source_list = dlg._tag_lists["sample_source"]
    entries = [source_list.item(row).text() for row in range(source_list.count())]

    assert entries == ["Not set", "Spore print", "Hymenium", "Stipe", "Pileus", "Context", "Other"]


def test_database_settings_sample_sources_still_uses_full_label(qapp, scratch_db):
    """Database settings must keep 'Spore print', never the compact 'Print'."""
    dlg = DatabaseSettingsDialog()
    source_list = dlg._tag_lists["sample_source"]
    entries = [source_list.item(row).text() for row in range(source_list.count())]

    assert "Spore print" in entries
    assert "Print" not in entries
