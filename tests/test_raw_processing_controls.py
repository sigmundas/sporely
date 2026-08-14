from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication

from PySide6.QtCore import QSignalBlocker

from ui.raw_processing_controls import AutoLevelsToggle, RawProcessingControls
from utils.raw_render import RawRenderSettings


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_raw_processing_controls_round_trip_preserves_raw_settings(qapp):
    controls = RawProcessingControls()
    assert not hasattr(controls, "preserve_tails_checkbox")
    assert controls.light_label.text() == "Light:"
    assert controls.dark_label.text() == "Dark:"
    assert controls.curve_strength_label.text() == "Strength:"
    assert controls.strength_label is controls.curve_strength_label
    assert controls.shadow_lift_label is controls.shadows_label
    assert controls.shadow_lift_slider is controls.shadows_slider
    assert controls.shadow_lift_value_label is controls.shadows_value_label
    assert controls.auto_levels_checkbox.isHidden() is False
    assert controls.auto_levels_checkbox.text() == "Auto levels"
    assert controls.auto_levels_btn is controls.auto_levels_checkbox
    assert controls.auto_levels_toggle is controls.auto_levels_checkbox
    assert controls.auto_levels_method_selector.selected_value() == "a"
    assert controls.auto_levels_clipping_selector.selected_value() is True
    assert controls.contrast_label.text() == "Contrast:"
    assert controls.contrast_slider.minimum() == -100
    assert controls.contrast_slider.maximum() == 100
    assert controls.light_slider.minimum() == 0
    assert controls.light_slider.maximum() == 2000
    assert controls.dark_slider.minimum() == 0
    assert controls.dark_slider.maximum() == 2000
    settings = RawRenderSettings(
        white_balance_mode="custom",
        wb_multipliers=(1.15, 1.0, 1.42),
        wb_selection=(12.0, 14.0, 24.0, 26.0),
        wb_multiplier_space="post_decode_rgb",
        wb_sample_point=(18.0, 19.0),
        wb_sample_size=9,
        wb_sample_base_mode="camera",
        wb_selection_space="preview_pixels",
        light_ev=0.5,
        dark_ev=-0.25,
        auto_levels=False,
        auto_levels_method="b",
        auto_levels_clipping=False,
        black_percentile=0.01,
        white_percentile=0.99,
        auto_levels_strength=0.7,
        auto_levels_soft_tails=True,
        auto_levels_tail_size=0.05,
        shadow_lift=0.03,
        tone_curve_enabled=True,
        tone_curve_strength=0.65,
        tone_curve_midpoint=0.45,
        tone_shadows=0.30,
        tone_highlights=-0.20,
        output_bps=8,
    )

    controls.set_settings(settings)

    round_tripped = controls.settings()

    assert round_tripped.white_balance_mode == settings.white_balance_mode
    assert round_tripped.wb_multipliers == settings.wb_multipliers
    assert round_tripped.wb_selection == settings.wb_selection
    assert round_tripped.wb_multiplier_space == settings.wb_multiplier_space
    assert round_tripped.wb_sample_point == settings.wb_sample_point
    assert round_tripped.wb_sample_size == settings.wb_sample_size
    assert round_tripped.wb_sample_base_mode == settings.wb_sample_base_mode
    assert round_tripped.wb_selection_space == settings.wb_selection_space
    assert round_tripped.light_ev == settings.light_ev
    assert round_tripped.dark_ev == settings.dark_ev
    assert round_tripped.exposure_ev == pytest.approx(settings.light_ev + settings.dark_ev)
    assert round_tripped.auto_levels == settings.auto_levels
    assert round_tripped.auto_levels_method == "b"
    assert round_tripped.auto_levels_clipping is False
    assert round_tripped.auto_levels_soft_tails is False
    assert round_tripped.shadow_lift == settings.shadow_lift
    assert round_tripped.tone_curve_enabled == settings.tone_curve_enabled
    assert round_tripped.tone_curve_strength == settings.tone_curve_strength
    assert round_tripped.tone_curve_midpoint == settings.tone_curve_midpoint
    assert round_tripped.tone_shadows == settings.tone_shadows
    assert round_tripped.tone_highlights == settings.tone_highlights
    assert round_tripped.output_bps == settings.output_bps
    assert controls.light_value_label.text() == "0.500"
    assert controls.dark_value_label.text() == "0.250"


def test_raw_processing_controls_exposure_and_shadows_update_settings_without_losing_custom_wb(qapp):
    controls = RawProcessingControls()
    settings = RawRenderSettings(
        white_balance_mode="custom",
        wb_multipliers=(1.2, 1.0, 1.4),
        wb_selection=(10.0, 12.0, 20.0, 22.0),
        wb_multiplier_space="post_decode_rgb",
        light_ev=0.25,
        dark_ev=-0.10,
        auto_levels=True,
        tone_curve_enabled=True,
        tone_curve_strength=0.55,
        tone_curve_midpoint=0.40,
        shadow_lift=0.02,
        tone_shadows=0.35,
        tone_highlights=-0.15,
    )

    controls.set_settings(settings)
    controls.light_slider.setValue(500)
    controls.dark_slider.setValue(400)

    updated = controls.settings()

    assert updated.white_balance_mode == "custom"
    assert updated.wb_multipliers == settings.wb_multipliers
    assert updated.light_ev == pytest.approx(0.50)
    assert updated.dark_ev == pytest.approx(-0.40)
    assert updated.exposure_ev == pytest.approx(0.10)
    assert updated.shadow_lift == pytest.approx(settings.shadow_lift)
    assert updated.tone_curve_enabled is True
    assert updated.tone_curve_strength == settings.tone_curve_strength
    assert updated.tone_curve_midpoint == settings.tone_curve_midpoint
    assert updated.tone_shadows == pytest.approx(0.35)
    assert updated.tone_highlights == pytest.approx(-0.15)


def test_raw_processing_controls_dragging_light_slider_emits_live_and_clears_auto_levels(qapp):
    controls = RawProcessingControls()
    emissions: list[RawRenderSettings] = []
    controls.settingsChanged.connect(lambda settings: emissions.append(settings))

    controls.set_settings(
        RawRenderSettings(
            white_balance_mode="camera",
            auto_levels=True,
            light_ev=0.0,
            dark_ev=0.0,
        )
    )

    controls.light_slider.setSliderDown(True)
    controls.light_slider.setValue(600)
    assert len(emissions) == 1
    assert emissions[0].light_ev == pytest.approx(0.60)
    assert controls.auto_levels_checkbox.isChecked() is False

    controls.light_slider.setSliderDown(False)
    controls._on_slider_released()

    assert len(emissions) == 1
    assert emissions[0].auto_levels is False


def test_raw_processing_controls_reenabling_auto_levels_restores_cached_slider_positions(qapp):
    controls = RawProcessingControls()
    controls.set_auto_level_settings(
        RawRenderSettings(
            white_balance_mode="camera",
            auto_levels=True,
            light_ev=0.357,
            dark_ev=-0.143,
        )
    )
    controls.set_settings(
        RawRenderSettings(
            white_balance_mode="camera",
            auto_levels=True,
            light_ev=0.125,
            dark_ev=-0.031,
        )
    )

    assert controls.auto_levels_checkbox.isChecked() is True
    assert controls.light_slider.value() == 357
    assert controls.dark_slider.value() == 143

    controls.light_slider.setValue(1234)
    assert controls.auto_levels_checkbox.isChecked() is False

    controls.auto_levels_checkbox.setChecked(True)

    assert controls.auto_levels_checkbox.isChecked() is True
    assert controls.light_slider.value() == 357
    assert controls.dark_slider.value() == 143


def test_raw_processing_controls_switching_method_clears_saved_bounds(qapp):
    controls = RawProcessingControls()
    controls.set_settings(
        RawRenderSettings(
            auto_levels=True,
            auto_levels_method="a",
            auto_black_level=0.1,
            auto_white_level=0.9,
            light_ev=0.152,
            dark_ev=-0.152,
        )
    )
    emissions: list[RawRenderSettings] = []
    controls.settingsChanged.connect(emissions.append)

    controls.auto_levels_method_selector.set_selected_value("b", emit=True)

    assert emissions[-1].auto_levels_method == "b"
    assert emissions[-1].auto_black_level is None
    assert emissions[-1].auto_white_level is None


def test_raw_processing_controls_clipping_uses_preferences_or_zero_cutoffs(qapp):
    controls = RawProcessingControls()
    controls.set_auto_level_cutoffs(0.004, 0.006)
    controls.set_settings(
        RawRenderSettings(
            auto_levels=True,
            auto_levels_clipping=True,
            auto_black_level=0.1,
            auto_white_level=0.9,
        )
    )
    emissions: list[RawRenderSettings] = []
    controls.settingsChanged.connect(emissions.append)

    controls.auto_levels_clipping_selector.set_selected_value(False, emit=True)

    assert emissions[-1].auto_levels_clipping is False
    assert emissions[-1].black_percentile == pytest.approx(0.0)
    assert emissions[-1].white_percentile == pytest.approx(1.0)
    assert emissions[-1].auto_black_level is None
    assert emissions[-1].auto_white_level is None

    controls.auto_levels_clipping_selector.set_selected_value(True, emit=True)

    assert emissions[-1].auto_levels_clipping is True
    assert emissions[-1].black_percentile == pytest.approx(0.004)
    assert emissions[-1].white_percentile == pytest.approx(0.994)


def test_raw_processing_controls_set_settings_does_not_emit_settings_changed(qapp):
    controls = RawProcessingControls()
    emissions: list[object] = []
    controls.settingsChanged.connect(lambda settings: emissions.append(settings))

    controls.set_settings(
        RawRenderSettings(
            white_balance_mode="custom",
            wb_multipliers=(1.2, 1.0, 1.4),
            tone_curve_enabled=True,
            tone_curve_strength=0.6,
            tone_curve_midpoint=0.4,
        )
    )

    assert emissions == []


def test_raw_processing_controls_multi_selection_shows_ranges_and_limits_wb_actions(qapp):
    controls = RawProcessingControls()
    controls.set_mixed_settings(
        [
            RawRenderSettings(
                white_balance_mode="camera",
                light_ev=0.2,
                dark_ev=-0.1,
                tone_contrast=-0.25,
            ),
            RawRenderSettings(
                white_balance_mode="auto",
                light_ev=0.8,
                dark_ev=-0.6,
                tone_contrast=0.4,
            ),
        ]
    )
    controls.set_multi_selection_mode(True)

    assert controls.light_slider.is_mixed() is True
    assert (controls.light_slider._mixed_min, controls.light_slider._mixed_max) == (200, 800)
    assert controls.dark_slider.is_mixed() is True
    assert (controls.dark_slider._mixed_min, controls.dark_slider._mixed_max) == (100, 600)
    assert controls.contrast_slider.is_mixed() is True
    assert (controls.contrast_slider._mixed_min, controls.contrast_slider._mixed_max) == (-25, 40)
    assert controls.white_balance_selector.button_group.checkedButton() is None
    assert controls.mixed_fields() >= {
        "light_ev",
        "dark_ev",
        "tone_contrast",
        "white_balance_mode",
    }

    assert controls.white_balance_selector.button_for_value("camera").isEnabled() is True
    assert controls.white_balance_selector.button_for_value("auto").isEnabled() is False
    assert controls.white_balance_selector.button_for_value("custom").isEnabled() is False
    assert controls.pick_button.isEnabled() is False

    controls.white_balance_selector.button_for_value("camera").click()

    assert controls.white_balance_selector.selected_value() == "camera"
    assert "white_balance_mode" not in controls.mixed_fields()


# --- AutoLevelsToggle adapter tests ------------------------------------------------
#
# The toggle is the compatibility layer between the old checkable QPushButton
# and the new segmented pill; every observable the widget code and existing
# tests rely on needs to be exercised. In particular the mixed-state path
# (temporary non-exclusive button group) is the most fragile bit.


def test_auto_levels_toggle_set_checked_updates_selected_segment(qapp):
    toggle = AutoLevelsToggle()
    assert toggle.isChecked() is False
    assert toggle._off_button.isChecked() is True
    assert toggle._on_button.isChecked() is False

    toggle.setChecked(True)
    assert toggle.isChecked() is True
    assert toggle._on_button.isChecked() is True
    assert toggle._off_button.isChecked() is False

    toggle.setChecked(False)
    assert toggle.isChecked() is False
    assert toggle._off_button.isChecked() is True
    assert toggle._on_button.isChecked() is False


def test_auto_levels_toggle_user_click_emits_toggled_exactly_once(qapp):
    toggle = AutoLevelsToggle()
    emissions: list[bool] = []
    toggle.toggled.connect(lambda checked: emissions.append(bool(checked)))

    # `.click()` mirrors a real mouse click on the pill: QButtonGroup fires
    # buttonClicked → SegmentedSelector emits selectionChanged →
    # AutoLevelsToggle._on_selection_changed emits `toggled` exactly once.
    toggle._on_button.click()
    assert toggle.isChecked() is True
    assert emissions == [True]

    toggle._off_button.click()
    assert toggle.isChecked() is False
    assert emissions == [True, False]


def test_auto_levels_toggle_signal_blocker_suppresses_toggled(qapp):
    toggle = AutoLevelsToggle()
    emissions: list[bool] = []
    toggle.toggled.connect(lambda checked: emissions.append(bool(checked)))

    with QSignalBlocker(toggle):
        toggle.setChecked(True)
    assert toggle.isChecked() is True
    assert emissions == []

    # After the blocker unwinds, later changes emit normally.
    toggle.setChecked(False)
    assert emissions == [False]


def test_auto_levels_toggle_mixed_state_deselects_both_and_survives_readback(qapp):
    toggle = AutoLevelsToggle()
    toggle.setChecked(True)

    toggle.setProperty("mixed", True)
    assert toggle.property("mixed") is True
    assert toggle._on_button.isChecked() is False
    assert toggle._off_button.isChecked() is False
    # `isChecked()` should keep reporting the last committed selection —
    # mixed is a visual state, not a value change.
    assert toggle.isChecked() is True


def test_auto_levels_toggle_leaving_mixed_restores_prior_selection(qapp):
    toggle = AutoLevelsToggle()
    toggle.setChecked(True)
    toggle.setProperty("mixed", True)

    toggle.setProperty("mixed", False)
    assert toggle.property("mixed") is False
    # Button group is exclusive again and the previously-committed choice
    # is what shows selected.
    assert toggle._selector.button_group.exclusive() is True
    assert toggle._on_button.isChecked() is True
    assert toggle._off_button.isChecked() is False
    assert toggle.isChecked() is True

    # And a subsequent user click still works — no stuck-button symptoms.
    toggle._off_button.click()
    assert toggle.isChecked() is False
    assert toggle._off_button.isChecked() is True


def test_auto_levels_toggle_mixed_state_does_not_leak_into_is_checked(qapp):
    toggle = AutoLevelsToggle()
    # Off → mixed → back to off.
    toggle.setChecked(False)
    toggle.setProperty("mixed", True)
    assert toggle.isChecked() is False
    toggle.setProperty("mixed", False)
    assert toggle.isChecked() is False

    # On → mixed → set to off explicitly — the explicit set wins.
    toggle.setChecked(True)
    toggle.setProperty("mixed", True)
    toggle.setChecked(False)
    assert toggle.isChecked() is False
    assert toggle.property("mixed") is False
    assert toggle._off_button.isChecked() is True
    assert toggle._on_button.isChecked() is False
