from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QCheckBox, QComboBox, QPushButton, QWidget

from database.models import SettingsDB
from ui.adaptive_choice_selector import AdaptiveChoiceSelector
from ui.database_settings_dialog import DatabaseSettingsDialog
from ui.image_import_dialog import ImageImportDialog
from ui.live_lab_tab import LiveLabTab
from ui.main_window import MainWindow


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _combo_values(combo: QComboBox) -> list[object]:
    return [combo.itemData(i) for i in range(combo.count())]


def _build_tag_combo_dialog_dummy(reduced_lists: dict[str, list[str]]) -> SimpleNamespace:
    dummy = SimpleNamespace()
    dummy.tr = lambda text: text
    dummy._add_choice_item = lambda combo, text, value, **kwargs: combo.addItem(text, value)
    dummy._populate_tag_combo = lambda combo, category, options: ImageImportDialog._populate_tag_combo(
        dummy,
        combo,
        category,
        options,
    )
    dummy._get_combo_tag_value = lambda combo, category: combo.currentData()
    dummy._update_lab_state_combo_alerts = lambda *_args: None
    dummy._load_tag_options = lambda category: list(reduced_lists[category])
    dummy.contrast_combo = QComboBox()
    dummy.mount_combo = QComboBox()
    dummy.stain_combo = QComboBox()
    dummy.sample_combo = QComboBox()
    return dummy


def test_database_settings_refresh_emits_and_rebuilds_open_controls(monkeypatch, qapp):
    reduced_lists = {
        "contrast": ["Not_set", "BF"],
        "mount": ["Not_set", "Water"],
        "stain": ["Not_set", "Cotton_Blue"],
        "sample": ["Not_set", "Dried"],
    }
    image_dummy = _build_tag_combo_dialog_dummy(reduced_lists)
    for combo, value in (
        (image_dummy.contrast_combo, "Phase"),
        (image_dummy.mount_combo, "Water"),
        (image_dummy.stain_combo, "Congo_Red"),
        (image_dummy.sample_combo, "Spore_print"),
    ):
        combo.addItem("Not set", "Not_set")
        combo.addItem(str(value), value)
        combo.setCurrentIndex(1)

    image_dummy.refresh_microscope_tag_preferences = lambda: ImageImportDialog.refresh_microscope_tag_preferences(image_dummy)

    live_dummy = SimpleNamespace()
    live_dummy._add_choice_item = lambda combo, text, value, **kwargs: combo.addItem(text, value)
    live_dummy._update_lab_state_combo_alerts = lambda *_args: None
    live_dummy.contrast_combo = QComboBox()
    live_dummy.mount_combo = QComboBox()
    live_dummy.stain_combo = QComboBox()
    live_dummy.sample_combo = QComboBox()
    for combo, value in (
        (live_dummy.contrast_combo, "Phase"),
        (live_dummy.mount_combo, "Water"),
        (live_dummy.stain_combo, "Congo_Red"),
        (live_dummy.sample_combo, "Spore_print"),
    ):
        combo.addItem("Not set", "Not_set")
        combo.addItem(str(value), value)
        combo.setCurrentIndex(1)

    def _rebuild_term_combo(combo, category: str) -> None:
        values = reduced_lists[category]
        combo.blockSignals(True)
        combo.clear()
        for value in values:
            if category == "contrast" and value == "Not_set":
                continue
            combo.addItem(value.replace("_", " "), value)
        combo.setCurrentIndex(0 if combo.count() else -1)
        combo.blockSignals(False)

    live_dummy._refresh_term_combo = _rebuild_term_combo
    live_dummy.refresh_microscope_tag_preferences = lambda: LiveLabTab.refresh_microscope_tag_preferences(live_dummy)

    refresh_calls: list[str] = []
    main_window_dummy = SimpleNamespace(
        observations_tab=SimpleNamespace(
            refresh_open_image_import_dialogs=lambda: refresh_calls.append("prepare"),
        ),
        live_lab_tab=SimpleNamespace(
            refresh_microscope_tag_preferences=lambda: refresh_calls.append("live"),
        ),
    )

    dialog = DatabaseSettingsDialog()
    monkeypatch.setattr(SettingsDB, "set_list_setting", lambda *args, **kwargs: None)
    monkeypatch.setattr(SettingsDB, "set_setting", lambda *args, **kwargs: None)
    dialog.microscopeTagsChanged.connect(
        lambda: MainWindow._refresh_microscope_tag_preferences(main_window_dummy)
    )
    dialog._loading_settings = False
    dialog._save_tag_settings()

    assert refresh_calls == ["prepare", "live"]

    image_dummy.refresh_microscope_tag_preferences()
    assert _combo_values(image_dummy.contrast_combo) == ["BF"]
    assert image_dummy.contrast_combo.currentData() == "BF"
    assert _combo_values(image_dummy.mount_combo) == ["Not_set", "Water"]
    assert image_dummy.mount_combo.currentData() == "Water"
    assert _combo_values(image_dummy.stain_combo) == ["Not_set", "Cotton_Blue"]
    assert image_dummy.stain_combo.currentData() == "Not_set"
    assert _combo_values(image_dummy.sample_combo) == ["Not_set", "Dried"]
    assert image_dummy.sample_combo.currentData() == "Not_set"

    live_dummy.refresh_microscope_tag_preferences()
    assert _combo_values(live_dummy.contrast_combo) == ["BF"]
    assert _combo_values(live_dummy.mount_combo) == ["Not_set", "Water"]
    assert _combo_values(live_dummy.stain_combo) == ["Not_set", "Cotton_Blue"]
    assert _combo_values(live_dummy.sample_combo) == ["Not_set", "Dried"]


def test_prepare_images_scale_controls_split_field_and_microscope_modes(qapp):
    dummy = SimpleNamespace()
    dummy.tr = lambda text: text
    dummy._current_selection_indices = lambda: [0]
    dummy._update_micro_settings_state = lambda enable=None: None
    dummy._update_resize_group_state = lambda: None
    dummy._update_scale_mismatch_warning = lambda: None
    dummy._update_scalebar_controls_visibility = lambda: ImageImportDialog._update_scalebar_controls_visibility(dummy)
    dummy._update_objective_selector_state = lambda: ImageImportDialog._update_objective_selector_state(dummy)
    dummy._update_scale_context_controls = lambda **kwargs: ImageImportDialog._update_scale_context_controls(dummy, **kwargs)
    dummy.import_results = [SimpleNamespace(image_type="field")]
    dummy.scale_group = QWidget()
    dummy.micro_settings_group = QWidget()
    dummy.field_scale_row = QWidget()
    dummy.scale_bar_mode_checkbox = QCheckBox()
    dummy.calibrate_btn = QPushButton("Set from scalebar")
    dummy._scale_bar_inline = QWidget()
    dummy.objective_combo = QComboBox()
    dummy.objective_combo.addItem("Not set", None)
    dummy.objective_combo.addItem("40x", "40x")
    dummy.field_radio = SimpleNamespace(isChecked=lambda: True)
    dummy.micro_radio = SimpleNamespace(isChecked=lambda: False)

    ImageImportDialog._update_scale_group_state(dummy)
    assert dummy.field_scale_row.isVisible() is True
    assert dummy.scale_bar_mode_checkbox.isVisible() is False
    assert dummy.objective_combo.isEnabled() is False

    dummy.import_results = [SimpleNamespace(image_type="microscope")]
    dummy.field_radio = SimpleNamespace(isChecked=lambda: False)
    dummy.micro_radio = SimpleNamespace(isChecked=lambda: True)
    ImageImportDialog._update_scale_group_state(dummy)
    assert dummy.field_scale_row.isVisible() is False
    assert dummy.scale_bar_mode_checkbox.isVisible() is True
    assert dummy.objective_combo.isEnabled() is True

    dummy.scale_bar_mode_checkbox.setChecked(True)
    ImageImportDialog._update_objective_selector_state(dummy)
    assert dummy.objective_combo.isEnabled() is False

    dummy.scale_bar_mode_checkbox.setChecked(False)
    ImageImportDialog._update_objective_selector_state(dummy)
    assert dummy.objective_combo.isEnabled() is True


def test_prepare_images_objective_and_field_scale_labels_do_not_use_dash_or_one_to_one(qapp):
    dummy = SimpleNamespace()
    dummy.tr = lambda text: text
    dummy._add_choice_item = lambda combo, text, value, **kwargs: combo.addItem(text, value, pillText=kwargs.get("pill_text"))
    dummy.objectives = {
        "40x": {"magnification": 40.0},
        "macro_1_1": {
            "magnification": 1.0,
            "objective_name": "Olympus 60mm",
            "name": "1:1 Olympus 60mm",
        },
    }
    dummy.default_objective = None
    dummy._update_lab_state_combo_alerts = lambda *_args: None
    dummy._populate_objectives = lambda selected_key=None: ImageImportDialog._populate_objectives(dummy, selected_key)
    dummy._populate_field_scale_combo = lambda selected_value=None: ImageImportDialog._populate_field_scale_combo(dummy, selected_value)
    dummy.objective_combo = AdaptiveChoiceSelector(compact=True)
    dummy.field_scale_combo = AdaptiveChoiceSelector(compact=True)

    ImageImportDialog._populate_objectives(dummy)
    ImageImportDialog._populate_field_scale_combo(dummy)

    assert dummy.objective_combo.count() == 1
    assert dummy.objective_combo.currentData() == "40x"
    assert dummy.objective_combo.findData("macro_1_1") == -1
    assert dummy.field_scale_combo.count() == 2
    assert dummy.field_scale_combo.button_for_value(None).text() == "—"
    assert dummy.field_scale_combo.findData("macro_1_1") == 1
    assert dummy.field_scale_combo.button_for_value("macro_1_1").text() == "1:1"


def test_prepare_images_scale_bar_workflow_sets_custom_scale(qapp):
    dummy = SimpleNamespace()
    dummy.tr = lambda text: text
    dummy.selected_index = 0
    dummy.selected_indices = [0]
    dummy._loading_form = False
    dummy._custom_scale = None
    dummy._scale_bar_pixel_distance = 25.0
    dummy._refresh_gallery = lambda: None
    dummy._update_summary = lambda: None
    dummy._update_current_image_sampling = lambda *_args: None
    dummy._refresh_resize_preview = lambda *args, **kwargs: None
    dummy._update_settings_hint_for_indices = lambda *args, **kwargs: None
    dummy._current_selection_indices = lambda: [0]
    dummy._update_scale_group_state = lambda: None
    dummy._update_ai_controls_state = lambda: None
    dummy._update_set_from_image_button_state = lambda: None
    dummy._update_lab_state_combo_alerts = lambda: None
    dummy._update_scalebar_controls_visibility = lambda: None
    dummy._update_objective_selector_state = lambda: ImageImportDialog._update_objective_selector_state(dummy)
    dummy._store_originals_enabled = lambda: False
    dummy._maybe_rescale_measurements_for_image = lambda *args, **kwargs: True
    dummy._collect_rescale_targets = lambda indices: []
    dummy._confirm_rescale_for_targets = lambda count: "single"
    dummy._rescale_measurements_for_image = lambda *args, **kwargs: None
    dummy._compute_resample_scale_factor = lambda result: 1.0
    dummy._current_image_note_text = lambda: None
    dummy._get_combo_tag_value = lambda combo, category: combo.currentData()
    dummy._field_tag_value = lambda category: "Not_set"
    dummy.contrast_combo = QComboBox()
    dummy.contrast_combo.addItem("Not set", "Not_set")
    dummy.mount_combo = QComboBox()
    dummy.mount_combo.addItem("Not set", "Not_set")
    dummy.stain_combo = QComboBox()
    dummy.stain_combo.addItem("Not set", "Not_set")
    dummy.sample_combo = QComboBox()
    dummy.sample_combo.addItem("Not set", "Not_set")
    dummy.objective_combo = QComboBox()
    dummy.objective_combo.addItem("Not set", None)
    dummy.objective_combo.addItem("40x", "40x")
    dummy.objective_combo.setCurrentIndex(1)
    dummy.scale_bar_mode_checkbox = QCheckBox()
    dummy.scale_bar_mode_checkbox.setChecked(False)
    dummy.micro_radio = SimpleNamespace(isChecked=lambda: True)
    dummy.field_radio = SimpleNamespace(isChecked=lambda: False)
    result = SimpleNamespace(
        image_type="microscope",
        image_id=None,
        objective=None,
        custom_scale=None,
        contrast=None,
        mount_medium=None,
        stain=None,
        sample_type=None,
        notes=None,
        resize_to_optimal=False,
        store_original=False,
        resample_scale_factor=None,
    )
    dummy.import_results = [result]
    dummy._apply_settings_to_index = lambda index, action=None, rescale_targets=None: ImageImportDialog._apply_settings_to_index(
        dummy,
        index,
        action,
        rescale_targets,
    )
    dummy._apply_settings_to_indices = lambda indices, action=None, previous_key=None: ImageImportDialog._apply_settings_to_indices(
        dummy,
        indices,
        action,
        previous_key,
    )

    ImageImportDialog.apply_scale_bar(dummy, 0.5)

    assert dummy._custom_scale == pytest.approx(0.5)
    assert dummy.scale_bar_mode_checkbox.isChecked() is True
    assert dummy.objective_combo.isEnabled() is False
    assert result.custom_scale == pytest.approx(0.5)
    assert result.objective == "40x"
