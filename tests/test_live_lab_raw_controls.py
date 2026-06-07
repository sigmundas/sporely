from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QCheckBox, QComboBox, QLabel, QSlider, QToolButton, QWidget

import ui.live_lab_tab as live_lab_tab
from utils.raw_render import RawRenderSettings


def _qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _build_raw_controls_state() -> SimpleNamespace:
    state = SimpleNamespace()
    state._raw_render_settings = RawRenderSettings.default()
    state.raw_processing_toggle_btn = QToolButton()
    state.raw_processing_toggle_btn.setCheckable(True)
    state.raw_processing_toggle_btn.setChecked(False)
    state.raw_white_balance_combo = QComboBox()
    state.raw_white_balance_combo.addItem("Camera WB", "camera")
    state.raw_white_balance_combo.addItem("Auto WB", "auto")
    state.raw_auto_levels_checkbox = QCheckBox("Auto levels")
    state.raw_auto_levels_checkbox.setChecked(True)
    state.raw_tone_curve_checkbox = QCheckBox("Tone curve")
    state.raw_tone_curve_checkbox.setChecked(False)
    state.raw_curve_strength_row = QWidget()
    state.raw_curve_midpoint_row = QWidget()
    state.raw_curve_strength_slider = QSlider(Qt.Horizontal)
    state.raw_curve_strength_slider.setRange(0, 100)
    state.raw_curve_strength_slider.setValue(45)
    state.raw_curve_strength_value_label = QLabel()
    state.raw_curve_midpoint_slider = QSlider(Qt.Horizontal)
    state.raw_curve_midpoint_slider.setRange(0, 100)
    state.raw_curve_midpoint_slider.setValue(48)
    state.raw_curve_midpoint_value_label = QLabel()
    state.objective_combo = QComboBox()
    state.objective_combo.addItem("Not set", None)
    state.contrast_combo = QComboBox()
    state.contrast_combo.addItem("Phase", "phase")
    state.mount_combo = QComboBox()
    state.mount_combo.addItem("Water", "water")
    state.stain_combo = QComboBox()
    state.stain_combo.addItem("None", "none")
    state.sample_combo = QComboBox()
    state.sample_combo.addItem("Spore", "spore")
    state._raw_processing_preset_context = lambda: {}
    state._raw_settings_from_controls = lambda: live_lab_tab.LiveLabTab._raw_settings_from_controls(state)
    state._set_raw_tone_controls_enabled = lambda enabled: live_lab_tab.LiveLabTab._set_raw_tone_controls_enabled(state, enabled)
    state._raw_processing_summary_text = lambda: live_lab_tab.LiveLabTab._raw_processing_summary_text(state)
    state._update_raw_processing_section_label = lambda expanded: live_lab_tab.LiveLabTab._update_raw_processing_section_label(state, expanded)
    state._current_raw_render_settings = lambda: live_lab_tab.LiveLabTab._current_raw_render_settings(state)
    state.tr = lambda text: text
    return state


def test_live_lab_raw_summary_and_slider_state():
    _qapp()
    state = _build_raw_controls_state()

    live_lab_tab.LiveLabTab._sync_raw_processing_controls_from_settings(
        state,
        RawRenderSettings.default(),
    )

    assert live_lab_tab.LiveLabTab._raw_processing_summary_text(state) == "Camera WB · Auto levels · Curve off"
    assert state.raw_curve_strength_row.isEnabled() is False
    assert state.raw_curve_midpoint_row.isEnabled() is False
    assert state.raw_curve_strength_slider.isEnabled() is False
    assert state.raw_curve_midpoint_slider.isEnabled() is False

    state.raw_curve_strength_slider.setValue(45)
    state.raw_curve_midpoint_slider.setValue(48)
    state.raw_tone_curve_checkbox.setChecked(True)
    live_lab_tab.LiveLabTab._on_raw_processing_controls_changed(state)

    assert live_lab_tab.LiveLabTab._raw_processing_summary_text(state) == "Camera WB · Auto levels · Curve 45 / mid 48"
    assert state.raw_curve_strength_row.isEnabled() is True
    assert state.raw_curve_midpoint_row.isEnabled() is True
    assert state.raw_curve_strength_slider.isEnabled() is True
    assert state.raw_curve_midpoint_slider.isEnabled() is True

    state.raw_white_balance_combo.setCurrentIndex(1)
    state.raw_auto_levels_checkbox.setChecked(False)
    live_lab_tab.LiveLabTab._on_raw_processing_controls_changed(state)

    assert live_lab_tab.LiveLabTab._raw_processing_summary_text(state) == "Auto WB · levels off · Curve 45 / mid 48"


def test_live_lab_ingest_uses_current_raw_settings_and_keeps_prior_snapshot(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")
    working_dir = tmp_path / "imports"
    working_dir.mkdir()
    working_path_one = str(working_dir / "sample_1.jpg")
    working_path_two = str(working_dir / "sample_2.jpg")

    state = _build_raw_controls_state()
    state._session_observation_id = 1
    state._session_image_ids = []
    state._selected_session_image_id = None
    state._session_import_count = 0
    state._current_lab_metadata = lambda: {
        "image_type": "microscope",
        "contrast": "phase",
        "mount_medium": "water",
        "stain": "none",
        "sample_type": "spore",
    }
    state._raw_processing_preset_context = lambda: {}
    state._show_status = lambda *args, **kwargs: None
    state._update_observation_thumbnail = lambda: None
    state._refresh_session_gallery = lambda: None
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._log_session_event = lambda *args, **kwargs: None
    state.tr = lambda text: text

    captured_calls: list[dict[str, object]] = []
    add_image_calls: list[dict[str, object]] = []

    def fake_prepare_local_ingest_image(source, *, raw_settings=None, lab_metadata=None, output_dir=None):
        snapshot = raw_settings.to_dict() if isinstance(raw_settings, RawRenderSettings) else None
        captured_calls.append(
            {
                "source": str(source),
                "raw_settings": snapshot,
                "lab_metadata": dict(lab_metadata or {}),
                "output_dir": str(output_dir),
            }
        )
        working_path = working_path_one if len(captured_calls) == 1 else working_path_two
        return SimpleNamespace(
            source_path=str(source),
            working_path=working_path,
            original_path=str(source),
            raw_render_snapshot={"settings": snapshot} if snapshot is not None else None,
            lab_metadata={
                **dict(lab_metadata or {}),
                "raw_processing": {
                    "engine": "rawpy",
                    "source": {"kind": "camera_raw", "path": str(source), "mime_type": "image/x-raw"},
                    "local_derivative": {
                        "kind": "rendered_from_raw",
                        "path": working_path,
                        "mime_type": "image/jpeg",
                        "quality": 95,
                        "subsampling": 0,
                    },
                    "settings": snapshot,
                },
            },
            provenance_kwargs=lambda: {
                "source_role": "converted_local",
                "file_purpose": "microscope",
                "original_mime_type": "image/x-raw",
                "working_mime_type": "image/jpeg",
            },
        )

    def fake_add_image(**kwargs):
        add_image_calls.append(dict(kwargs))
        return len(add_image_calls)

    monkeypatch.setattr(live_lab_tab, "prepare_local_ingest_image", fake_prepare_local_ingest_image)
    monkeypatch.setattr(live_lab_tab.ImageDB, "add_image", fake_add_image)
    monkeypatch.setattr(live_lab_tab.ImageDB, "get_image", lambda image_id: {"filepath": working_path_one if image_id == 1 else working_path_two})
    monkeypatch.setattr(live_lab_tab, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(live_lab_tab, "cleanup_import_temp_file", lambda *args, **kwargs: None)

    state.raw_white_balance_combo.setCurrentIndex(0)
    state.raw_auto_levels_checkbox.setChecked(True)
    state.raw_tone_curve_checkbox.setChecked(False)
    state.raw_curve_strength_slider.setValue(45)
    state.raw_curve_midpoint_slider.setValue(48)

    assert live_lab_tab.LiveLabTab._ingest_detected_image(state, str(source_path))
    first_snapshot = add_image_calls[0]["lab_metadata"]["raw_processing"]["settings"]
    assert captured_calls[0]["raw_settings"]["white_balance_mode"] == "camera"
    assert captured_calls[0]["raw_settings"]["auto_levels"] is True
    assert first_snapshot["white_balance_mode"] == "camera"
    assert first_snapshot["auto_levels"] is True

    state.raw_white_balance_combo.setCurrentIndex(1)
    state.raw_auto_levels_checkbox.setChecked(False)
    state.raw_tone_curve_checkbox.setChecked(True)
    state.raw_curve_strength_slider.setValue(70)
    state.raw_curve_midpoint_slider.setValue(35)

    assert live_lab_tab.LiveLabTab._ingest_detected_image(state, str(source_path))
    assert captured_calls[1]["raw_settings"]["white_balance_mode"] == "auto"
    assert captured_calls[1]["raw_settings"]["auto_levels"] is False
    assert captured_calls[1]["raw_settings"]["tone_curve_enabled"] is True
    assert captured_calls[1]["raw_settings"]["tone_curve_strength"] == 0.7
    assert captured_calls[1]["raw_settings"]["tone_curve_midpoint"] == 0.35

    # The first image's stored metadata must remain unchanged after later control edits.
    assert first_snapshot["white_balance_mode"] == "camera"
    assert first_snapshot["auto_levels"] is True
    assert first_snapshot["tone_curve_enabled"] is False


def test_live_lab_non_raw_capture_keeps_raw_processing_out_of_metadata(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "sample.jpg"
    source_path.write_bytes(b"jpeg-bytes")
    working_path = str(tmp_path / "imports" / "sample.jpg")

    state = _build_raw_controls_state()
    state._session_observation_id = 1
    state._session_image_ids = []
    state._selected_session_image_id = None
    state._session_import_count = 0
    state._current_lab_metadata = lambda: {"image_type": "microscope", "contrast": "phase"}
    state._raw_processing_preset_context = lambda: {}
    state._show_status = lambda *args, **kwargs: None
    state._update_observation_thumbnail = lambda: None
    state._refresh_session_gallery = lambda: None
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._log_session_event = lambda *args, **kwargs: None
    state.tr = lambda text: text

    captured_calls: list[dict[str, object]] = []
    add_image_calls: list[dict[str, object]] = []

    def fake_prepare_local_ingest_image(source, *, raw_settings=None, lab_metadata=None, output_dir=None):
        captured_calls.append(
            {
                "source": str(source),
                "raw_settings": raw_settings.to_dict() if isinstance(raw_settings, RawRenderSettings) else None,
                "lab_metadata": dict(lab_metadata or {}),
            }
        )
        return SimpleNamespace(
            source_path=str(source),
            working_path=working_path,
            original_path=str(source),
            raw_render_snapshot=None,
            lab_metadata=None,
            provenance_kwargs=lambda: {
                "source_role": "local_canonical",
                "file_purpose": "microscope",
                "original_mime_type": "image/jpeg",
                "working_mime_type": "image/jpeg",
            },
        )

    def fake_add_image(**kwargs):
        add_image_calls.append(dict(kwargs))
        return len(add_image_calls)

    monkeypatch.setattr(live_lab_tab, "prepare_local_ingest_image", fake_prepare_local_ingest_image)
    monkeypatch.setattr(live_lab_tab.ImageDB, "add_image", fake_add_image)
    monkeypatch.setattr(live_lab_tab.ImageDB, "get_image", lambda image_id: {"filepath": working_path})
    monkeypatch.setattr(live_lab_tab, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(live_lab_tab, "cleanup_import_temp_file", lambda *args, **kwargs: None)

    assert live_lab_tab.LiveLabTab._ingest_detected_image(state, str(source_path))
    assert captured_calls[0]["raw_settings"]["white_balance_mode"] == "camera"
    assert captured_calls[0]["raw_settings"]["auto_levels"] is True
    assert "raw_processing" not in add_image_calls[0]["lab_metadata"]
    assert add_image_calls[0]["lab_metadata"]["contrast"] == "phase"
