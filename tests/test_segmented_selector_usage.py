from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QHBoxLayout, QSizePolicy, QWidget

import ui.live_lab_tab as live_lab_tab
import ui.main_window as main_window
from ui.observations_tab import ObservationDetailsDialog
from ui.segmented_selector import SegmentedSelector
from ui.styles import get_style


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_segmented_selector_compact_and_tall_modes(qapp) -> None:
    qapp.setStyleSheet(get_style())
    compact = SegmentedSelector(None, compact=True)
    compact_private = compact.add_option("Private", "private", checked=True)
    compact_public = compact.add_option("Public", "public")

    tall = SegmentedSelector(None, compact=False)
    tall_field = tall.add_option("Field Image", "field", checked=True)
    tall_micro = tall.add_option("Microscope Image", "microscope")

    assert compact.objectName() == "segmentedControl"
    assert compact.property("compact") == "true"
    assert tall.property("compact") == "false"
    assert compact_private.objectName() == "segmentedButton"
    assert compact_public.objectName() == "segmentedButton"
    assert compact.selected_value() == "private"
    assert compact.button_for_value("public") is compact_public
    assert [button.text() for button in compact.buttons()] == ["Private", "Public"]
    assert compact.sizePolicy().horizontalPolicy() == QSizePolicy.Fixed
    compact.show()
    qapp.processEvents()
    assert compact.height() == 28
    assert tall_field.objectName() == "segmentedButton"
    assert tall_micro.objectName() == "segmentedButton"
    assert tall.selected_value() == "field"
    assert [button.text() for button in tall.buttons()] == ["Field Image", "Microscope Image"]
    assert tall.sizePolicy().horizontalPolicy() == QSizePolicy.Fixed

    compact.set_selected_value("public")
    tall.set_selected_value("microscope")

    assert compact.selected_value() == "public"
    assert tall.selected_value() == "microscope"


def test_observation_details_selectors_use_segmented_selector(qapp) -> None:
    dialog = SimpleNamespace()
    dialog._default_sharing_scope = lambda: "friends"

    dialog.sharing_scope_selector = SegmentedSelector(None, compact=True)
    dialog.sharing_scope_selector.add_option("Private", "private")
    dialog.sharing_scope_selector.add_option("Friends", "friends", checked=True)
    dialog.sharing_scope_selector.add_option("Public", "public")

    dialog.location_precision_selector = SegmentedSelector(None, compact=True)
    dialog.location_precision_selector.add_option("Exact", "exact", checked=True)
    dialog.location_precision_selector.add_option("Fuzzed", "fuzzed")

    assert ObservationDetailsDialog._selected_sharing_scope(dialog) == "friends"
    assert ObservationDetailsDialog._selected_location_precision(dialog) == "exact"

    ObservationDetailsDialog._set_sharing_scope(dialog, "public")
    ObservationDetailsDialog._set_location_precision(dialog, "fuzzed")

    assert dialog._sharing_scope_value == "public"
    assert dialog.sharing_scope_selector.selected_value() == "public"
    assert ObservationDetailsDialog._selected_sharing_scope(dialog) == "public"
    assert dialog.location_precision_selector.selected_value() == "fuzzed"
    assert ObservationDetailsDialog._selected_location_precision(dialog) == "fuzzed"

    ObservationDetailsDialog._set_sharing_scope(dialog, "not-a-scope", location_public=True)

    assert dialog._sharing_scope_value == "friends"
    assert dialog.sharing_scope_selector.selected_value() == "friends"


def test_live_lab_capture_mode_uses_segmented_selector(monkeypatch, qapp) -> None:
    state = SimpleNamespace()
    state.SESSION_MODE_LIVE = "live"
    state.SESSION_MODE_OFFLINE = "offline"
    state.SETTING_SESSION_MODE = "live_lab_session_mode"
    state._normalize_session_mode = lambda value: value if value in {"live", "offline"} else "live"
    state.session_mode_selector = SegmentedSelector(None, compact=True)
    state.session_mode_selector.add_option("Live capture (watch folder)", "live", checked=True)
    state.session_mode_selector.add_option("Offline (log only)", "offline")

    assert live_lab_tab.LiveLabTab._selected_session_mode(state) == "live"

    state.session_mode_selector.set_selected_value("offline")
    assert live_lab_tab.LiveLabTab._selected_session_mode(state) == "offline"

    monkeypatch.setattr(live_lab_tab.SettingsDB, "get_setting", lambda key, default=None: "offline")
    live_lab_tab.LiveLabTab._restore_session_mode(state)

    assert state.session_mode_selector.selected_value() == "offline"


def test_analysis_plot_style_helper_still_reads_segmented_selector(qapp) -> None:
    analysis = SimpleNamespace()
    selector = SegmentedSelector(None, compact=True)
    analysis.gallery_plot_style_ellipse_radio = selector.add_option("Ellipse", "ellipse")
    analysis.gallery_plot_style_kde_radio = selector.add_option("Kernel density", "kde", checked=True)
    analysis.gallery_plot_style_mean_radio = selector.add_option("Mean range", "mean")

    assert main_window.MainWindow._gallery_plot_style_from_controls(analysis) == "kde"

    analysis.gallery_plot_style_kde_radio.setChecked(False)
    analysis.gallery_plot_style_mean_radio.setChecked(True)

    assert main_window.MainWindow._gallery_plot_style_from_controls(analysis) == "mean"


def test_measure_rectangle_thickness_uses_segmented_selector(qapp) -> None:
    measure = SimpleNamespace()
    selector = SegmentedSelector(None, compact=True)
    measure.rectangle_thickness_selector = selector
    thin_radio = selector.add_option("Thin", "thin", checked=True)
    thick_radio = selector.add_option("Thick", "thick")

    assert [button.text() for button in selector.buttons()] == ["Thin", "Thick"]
    assert thin_radio.objectName() == "segmentedButton"
    assert thick_radio.objectName() == "segmentedButton"
    assert main_window.MainWindow._current_measure_rectangle_thickness(measure) == 1.0

    selector.set_selected_value("thick")
    assert main_window.MainWindow._current_measure_rectangle_thickness(measure) == 2.0


def test_segmented_selector_fill_width_mode_expands_buttons(qapp) -> None:
    qapp.setStyleSheet(get_style())
    container = QWidget()
    layout = QHBoxLayout(container)
    layout.setContentsMargins(0, 0, 0, 0)
    selector = SegmentedSelector(container, compact=True, fill_width=True)
    selector.add_option("Line", "lines", checked=True)
    selector.add_option("Rectangle", "rectangle")
    selector.add_option("Multi-line", "multiline")
    layout.addWidget(selector)

    container.resize(540, 48)
    container.show()
    qapp.processEvents()

    assert selector.sizePolicy().horizontalPolicy() == QSizePolicy.Expanding
    assert selector.width() >= 500
    widths = [button.width() for button in selector.buttons()]
    assert max(widths) - min(widths) <= 1
    assert all(button.width() > button.sizeHint().width() for button in selector.buttons())
