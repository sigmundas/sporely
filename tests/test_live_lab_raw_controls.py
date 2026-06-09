from __future__ import annotations

import copy
import json
import os
import tempfile
from types import SimpleNamespace
from pathlib import Path

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt, QPointF
from PySide6.QtGui import QImage, QPixmap
from PySide6.QtWidgets import QApplication, QCheckBox, QComboBox, QFrame, QLabel, QLineEdit, QPushButton, QSlider, QToolButton, QWidget

import ui.live_lab_tab as live_lab_tab
from ui.segmented_selector import SegmentedSelector
from utils.raw_render import RawRenderSettings, RawRenderingUnavailableError


def _qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _build_raw_controls_state() -> SimpleNamespace:
    state = SimpleNamespace()
    state.RAW_CAPTURE_MODE_AUTO_SAVE = live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_AUTO_SAVE
    state.RAW_CAPTURE_MODE_REVIEW = live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW
    state.RAW_BACKGROUND_WB_SAMPLE_SIZE = live_lab_tab.LiveLabTab.RAW_BACKGROUND_WB_SAMPLE_SIZE
    state.SETTING_RAW_CAPTURE_MODE = live_lab_tab.LiveLabTab.SETTING_RAW_CAPTURE_MODE
    state.SETTING_RAW_PROCESSING_PRESET_PREFIX = live_lab_tab.LiveLabTab.SETTING_RAW_PROCESSING_PRESET_PREFIX
    state._raw_render_settings = RawRenderSettings.default()
    state._raw_capture_mode = live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_AUTO_SAVE
    state._raw_companion_source_preference = live_lab_tab.RAW_COMPANION_SOURCE_PREFERENCE_PREFER_RAW
    state._pending_raw_background_wb_armed = False
    state.raw_processing_toggle_btn = QToolButton()
    state.raw_processing_toggle_btn.setCheckable(True)
    state.raw_processing_toggle_btn.setChecked(False)
    state.raw_capture_mode_selector = SegmentedSelector(compact=True)
    state.raw_capture_mode_selector.add_option("Auto-save RAW captures", live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_AUTO_SAVE, checked=True)
    state.raw_capture_mode_selector.add_option("Review RAW before saving", live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    state.raw_companion_source_selector = SegmentedSelector(compact=True)
    state.raw_companion_source_selector.add_option(
        "Prefer RAW",
        live_lab_tab.RAW_COMPANION_SOURCE_PREFERENCE_PREFER_RAW,
        checked=True,
    )
    state.raw_companion_source_selector.add_option(
        "Use camera JPEG",
        live_lab_tab.RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG,
    )
    state.raw_white_balance_combo = QComboBox()
    state.raw_white_balance_combo.addItem("Camera WB", "camera")
    state.raw_white_balance_combo.addItem("Auto WB", "auto")
    state.raw_white_balance_combo.addItem("Custom WB", "custom")
    state.raw_auto_levels_checkbox = QCheckBox("Auto levels")
    state.raw_auto_levels_checkbox.setChecked(True)
    state.raw_tone_curve_checkbox = QCheckBox("Tone curve")
    state.raw_tone_curve_checkbox.setChecked(False)
    state.raw_curve_strength_row = QWidget()
    state.raw_curve_midpoint_row = QWidget()
    state.raw_curve_strength_slider = QSlider(Qt.Horizontal)
    state.raw_curve_strength_slider.setRange(0, 100)
    state.raw_curve_strength_slider.setValue(50)
    state.raw_curve_strength_value_label = QLabel()
    state.raw_curve_midpoint_slider = QSlider(Qt.Horizontal)
    state.raw_curve_midpoint_slider.setRange(0, 100)
    state.raw_curve_midpoint_slider.setValue(50)
    state.raw_curve_midpoint_value_label = QLabel()
    state.objective_combo = QComboBox()
    state.objective_combo.addItem("Not set", None)
    state.contrast_combo = QComboBox()
    state.contrast_combo.addItem("Not set", "Not_set")
    state.contrast_combo.addItem("Phase", "phase")
    state.mount_combo = QComboBox()
    state.mount_combo.addItem("Not set", "Not_set")
    state.mount_combo.addItem("Water", "water")
    state.stain_combo = QComboBox()
    state.stain_combo.addItem("Not set", "Not_set")
    state.stain_combo.addItem("None", "none")
    state.sample_combo = QComboBox()
    state.sample_combo.addItem("Not set", "Not_set")
    state.sample_combo.addItem("Spore", "spore")
    state.pending_raw_frame = QFrame()
    state.pending_raw_count_label = QLabel()
    state.pending_raw_save_btn = QPushButton("Save current")
    state.pending_raw_apply_all_btn = QPushButton("Apply settings to all pending")
    state.pending_raw_pick_wb_btn = QPushButton("Pick background WB")
    state.pending_raw_pick_wb_btn.setCheckable(True)
    state.pending_raw_shortcuts_label = QLabel()
    state.raw_edit_frame = QFrame()
    state.raw_edit_frame.setVisible(False)
    state.raw_edit_summary_label = QLabel()
    state.raw_edit_note_label = QLabel()
    state.raw_edit_open_btn = QPushButton("Edit RAW settings")
    state.raw_edit_use_copied_btn = QPushButton("Use copied RAW settings")
    state.raw_edit_apply_btn = QPushButton("Apply re-render")
    state.raw_edit_copy_btn = QPushButton("Copy settings")
    state.raw_edit_pick_wb_btn = QPushButton("Pick background WB")
    state.raw_edit_pick_wb_btn.setCheckable(True)
    state.raw_edit_cancel_btn = QPushButton("Cancel")
    state.viewer_title_label = QLabel()
    state.viewer_meta_label = QLabel()
    state.reset_view_btn = QPushButton("Reset")
    state.live_image_label = SimpleNamespace(
        original_pixmap=QPixmap(10, 10),
        crop_mode=False,
        crop_box=None,
        crop_preview=None,
        cursor=Qt.ArrowCursor,
        set_image_sources=lambda pixmap, *args, **kwargs: setattr(state.live_image_label, "original_pixmap", pixmap),
        set_microns_per_pixel=lambda *args, **kwargs: None,
        set_scale_bar=lambda *args, **kwargs: None,
        reset_view=lambda: None,
        set_crop_mode=lambda enabled: setattr(state.live_image_label, "crop_mode", bool(enabled)),
        set_crop_overlay_style=lambda *args, **kwargs: None,
        clear_crop_box=lambda: (setattr(state.live_image_label, "crop_box", None), setattr(state.live_image_label, "crop_preview", None)),
        set_crop_box=lambda box: setattr(state.live_image_label, "crop_box", box),
        _normalized_crop_box_from_points=lambda start, end: (
            min(float(start.x()), float(end.x())),
            min(float(start.y()), float(end.y())),
            max(float(start.x()), float(end.x())),
            max(float(start.y()), float(end.y())),
        ),
        size=lambda: SimpleNamespace(
            width=lambda: int(state.live_image_label.original_pixmap.width()),
            height=lambda: int(state.live_image_label.original_pixmap.height()),
        ),
        get_current_view_crop_rect=lambda: None,
        setCursor=lambda cursor: setattr(state.live_image_label, "cursor", cursor),
    )
    state.session_gallery = SimpleNamespace(
        items=[],
        selected=None,
        visible=False,
        clear=lambda: (
            state.session_gallery.items.clear(),
            setattr(state.session_gallery, "visible", False),
            setattr(state.session_gallery, "selected", None),
        ),
        set_items=lambda items: setattr(state.session_gallery, "items", list(items)) or setattr(
            state.session_gallery,
            "visible",
            bool(items),
        ),
        select_image=lambda image_id: setattr(state.session_gallery, "selected", image_id),
    )
    state.pending_raw_gallery = state.session_gallery
    state._pending_raw_captures = []
    state._selected_pending_raw_index = -1
    state._pending_companion_groups = {}
    state._consumed_companion_groups = set()
    state._seen_source_paths = set()
    state._raw_companion_hold_ms = 2000
    state._pending_raw_preview_timer = SimpleNamespace(stop=lambda: None, start=lambda _ms: None)
    state._raw_edit_preview_timer = SimpleNamespace(stop=lambda: None, start=lambda _ms: None)
    state._pending_raw_preview_dir = lambda: Path(tempfile.gettempdir()) / "sporely_test_raw_previews"
    state._raw_edit_preview_dir = lambda: Path(tempfile.gettempdir()) / "sporely_test_raw_edit_previews"
    state._raw_edit_session = None
    state._raw_edit_background_wb_armed = False
    state._raw_copied_settings = None
    state._same_stem_companion_paths = lambda source_path: [str(source_path)]
    state._fallback_companion_path = lambda source_path, exclude_path=None: None
    state._companion_state_for_path = lambda source_path: live_lab_tab.LiveLabTab._companion_state_for_path(state, source_path)
    state._clear_companion_group = lambda group_key: live_lab_tab.LiveLabTab._clear_companion_group(state, group_key)
    state._handle_raw_companion_source = lambda source, group_key, state_dict: live_lab_tab.LiveLabTab._handle_raw_companion_source(state, source, group_key=group_key, state=state_dict)
    state._queue_companion_source = lambda source_path: live_lab_tab.LiveLabTab._queue_companion_source(state, source_path)
    state._flush_companion_group = lambda group_key: live_lab_tab.LiveLabTab._flush_companion_group(state, group_key)
    state._raw_processing_preset_context = lambda: {}
    state._raw_settings_from_controls = lambda update_session_settings=True: live_lab_tab.LiveLabTab._raw_settings_from_controls(
        state,
        update_session_settings=update_session_settings,
    )
    state._sync_raw_processing_controls_from_settings = lambda settings=None, update_session_settings=True: live_lab_tab.LiveLabTab._sync_raw_processing_controls_from_settings(
        state,
        settings,
        update_session_settings=update_session_settings,
    )
    state._set_raw_tone_controls_enabled = lambda enabled: live_lab_tab.LiveLabTab._set_raw_tone_controls_enabled(state, enabled)
    state._raw_settings_summary_text = lambda settings: live_lab_tab.LiveLabTab._raw_settings_summary_text(state, settings)
    state._raw_settings_info_text = lambda settings: live_lab_tab.LiveLabTab._raw_settings_info_text(state, settings)
    state._raw_processing_summary_text = lambda: live_lab_tab.LiveLabTab._raw_processing_summary_text(state)
    state._refresh_raw_processing_context_ui = lambda: live_lab_tab.LiveLabTab._refresh_raw_processing_context_ui(state)
    state._update_raw_processing_section_label = lambda expanded: live_lab_tab.LiveLabTab._update_raw_processing_section_label(state, expanded)
    state._lab_state_combo_alert_stylesheet = lambda: live_lab_tab.LiveLabTab._lab_state_combo_alert_stylesheet(state)
    state._combo_is_unset = lambda combo: live_lab_tab.LiveLabTab._combo_is_unset(combo)
    state._set_lab_state_combo_alert = lambda combo, alert: live_lab_tab.LiveLabTab._set_lab_state_combo_alert(state, combo, alert)
    state._update_lab_state_combo_alerts = lambda *_args: live_lab_tab.LiveLabTab._update_lab_state_combo_alerts(state, *_args)
    state._current_raw_render_settings = lambda: live_lab_tab.LiveLabTab._current_raw_render_settings(state)
    state._raw_settings_has_sampled_background_wb = lambda settings=None: live_lab_tab.LiveLabTab._raw_settings_has_sampled_background_wb(settings)
    state._raw_settings_for_copy = lambda settings: live_lab_tab.LiveLabTab._raw_settings_for_copy(state, settings)
    state._raw_white_balance_label = lambda settings=None: live_lab_tab.LiveLabTab._raw_white_balance_label(state, settings)
    state._raw_white_balance_readout_text = lambda settings=None: live_lab_tab.LiveLabTab._raw_white_balance_readout_text(state, settings)
    state._pending_raw_action_hint_text = lambda capture=None: live_lab_tab.LiveLabTab._pending_raw_action_hint_text(state, capture)
    state._set_pending_raw_background_wb_armed = lambda armed: live_lab_tab.LiveLabTab._set_pending_raw_background_wb_armed(state, armed)
    state._cancel_pending_raw_background_wb_selection = lambda: live_lab_tab.LiveLabTab._cancel_pending_raw_background_wb_selection(state)
    state._cancel_active_raw_background_wb_selection = lambda: live_lab_tab.LiveLabTab._cancel_active_raw_background_wb_selection(state)
    state._pending_raw_settings_for_copy = lambda settings: live_lab_tab.LiveLabTab._pending_raw_settings_for_copy(state, settings)
    state._raw_background_wb_selection_state = lambda target: live_lab_tab.LiveLabTab._raw_background_wb_selection_state(state, target)
    state._set_raw_background_wb_armed = lambda armed, target="pending": live_lab_tab.LiveLabTab._set_raw_background_wb_armed(state, armed, target=target)
    state._cancel_raw_background_wb_selection = lambda target="pending": live_lab_tab.LiveLabTab._cancel_raw_background_wb_selection(state, target=target)
    state._toggle_raw_background_wb_pick = lambda checked, target="pending": live_lab_tab.LiveLabTab._toggle_raw_background_wb_pick(state, checked, target=target)
    state._active_raw_background_target = lambda: live_lab_tab.LiveLabTab._active_raw_background_target(state)
    state._raw_background_wb_sample_base_mode = lambda settings=None: live_lab_tab.LiveLabTab._raw_background_wb_sample_base_mode(state, settings)
    state._raw_background_wb_sampling_settings = lambda settings=None: live_lab_tab.LiveLabTab._raw_background_wb_sampling_settings(state, settings)
    state._raw_background_wb_sampling_pixmap = lambda target="pending": live_lab_tab.LiveLabTab._raw_background_wb_sampling_pixmap(state, target)
    state._raw_background_wb_sampling_view = lambda target="pending": live_lab_tab.LiveLabTab._raw_background_wb_sampling_view(state, target)
    state._raw_background_wb_sample_rect_from_point = lambda point, sample_size=None, pixmap=None: live_lab_tab.LiveLabTab._raw_background_wb_sample_rect_from_point(
        state,
        point,
        sample_size=sample_size,
        pixmap=pixmap,
    )
    state._apply_raw_background_wb_result = lambda base_settings, multipliers, selection, target="pending", sample_point=None, sample_size=None: live_lab_tab.LiveLabTab._apply_raw_background_wb_result(
        state,
        base_settings,
        multipliers,
        selection,
        target=target,
        sample_point=sample_point,
        sample_size=sample_size,
    )
    state._apply_raw_background_wb_selection = lambda crop_box, target="pending": live_lab_tab.LiveLabTab._apply_raw_background_wb_selection(state, crop_box, target=target)
    state._apply_raw_background_wb_selection_from_point = lambda point, target="pending": live_lab_tab.LiveLabTab._apply_raw_background_wb_selection_from_point(state, point, target=target)
    state._on_raw_background_wb_crop_changed = lambda crop_box, target="pending": live_lab_tab.LiveLabTab._on_raw_background_wb_crop_changed(state, crop_box, target=target)
    state._on_live_image_clicked_for_background_wb = lambda point: live_lab_tab.LiveLabTab._on_live_image_clicked_for_background_wb(state, point)
    state._finalize_raw_background_wb_from_preview = lambda target="pending": live_lab_tab.LiveLabTab._finalize_raw_background_wb_from_preview(state, target=target)
    state._selected_committed_image_id = lambda: live_lab_tab.LiveLabTab._selected_committed_image_id(state)
    state._selected_committed_image = lambda: live_lab_tab.LiveLabTab._selected_committed_image(state)
    state._raw_editable_image_session = lambda image=None, settings=None: live_lab_tab.LiveLabTab._raw_editable_image_session(state, image, settings=settings)
    state._update_raw_edit_controls = lambda: live_lab_tab.LiveLabTab._update_raw_edit_controls(state)
    state._begin_raw_edit_for_selected_image = lambda settings=None, source_image=None: live_lab_tab.LiveLabTab._begin_raw_edit_for_selected_image(state, settings, source_image=source_image)
    state._begin_raw_edit_with_copied_settings = lambda: live_lab_tab.LiveLabTab._begin_raw_edit_with_copied_settings(state)
    state._cancel_raw_edit_session = lambda restore_selection=True: live_lab_tab.LiveLabTab._cancel_raw_edit_session(state, restore_selection=restore_selection)
    state._copy_raw_edit_settings = lambda: live_lab_tab.LiveLabTab._copy_raw_edit_settings(state)
    state._schedule_raw_edit_preview_refresh = lambda: live_lab_tab.LiveLabTab._schedule_raw_edit_preview_refresh(state)
    state._refresh_raw_edit_preview = lambda: live_lab_tab.LiveLabTab._refresh_raw_edit_preview(state)
    state._apply_raw_edit_session = lambda: live_lab_tab.LiveLabTab._apply_raw_edit_session(state)
    state._pixmap_rgb_array = lambda pixmap: live_lab_tab.LiveLabTab._pixmap_rgb_array(state, pixmap)
    state._apply_pending_raw_background_wb_selection = lambda crop_box: live_lab_tab.LiveLabTab._apply_pending_raw_background_wb_selection(state, crop_box)
    state._on_pending_raw_background_wb_crop_changed = lambda crop_box: live_lab_tab.LiveLabTab._on_pending_raw_background_wb_crop_changed(state, crop_box)
    state._toggle_pending_raw_background_wb_pick = lambda checked: live_lab_tab.LiveLabTab._toggle_pending_raw_background_wb_pick(state, checked)
    state._is_text_input_widget = lambda widget: live_lab_tab.LiveLabTab._is_text_input_widget(widget)
    state._raw_review_shortcut_allowed = lambda: live_lab_tab.LiveLabTab._raw_review_shortcut_allowed(state)
    state._handle_raw_review_shortcut = lambda action: live_lab_tab.LiveLabTab._handle_raw_review_shortcut(state, action)
    state._install_raw_review_shortcuts = lambda: live_lab_tab.LiveLabTab._install_raw_review_shortcuts(state)
    state._finalize_pending_raw_background_wb_from_preview = lambda: live_lab_tab.LiveLabTab._finalize_pending_raw_background_wb_from_preview(state)
    state._raw_processing_settings_key = lambda: live_lab_tab.LiveLabTab._raw_processing_settings_key(state)
    state._ingest_detected_image = lambda source_path, raw_settings=None, lab_metadata=None: live_lab_tab.LiveLabTab._ingest_detected_image(state, source_path, raw_settings=raw_settings, lab_metadata=lab_metadata)
    state._normalize_raw_capture_mode = lambda value: live_lab_tab.LiveLabTab._normalize_raw_capture_mode(state, value)
    state._selected_raw_capture_mode = lambda: getattr(state, "_raw_capture_mode", live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_AUTO_SAVE)
    state._normalize_raw_companion_source_preference = lambda value: live_lab_tab.LiveLabTab._normalize_raw_companion_source_preference(state, value)
    state._selected_raw_companion_source_preference = lambda: live_lab_tab.LiveLabTab._selected_raw_companion_source_preference(state)
    state._restore_raw_companion_source_preference = lambda: live_lab_tab.LiveLabTab._restore_raw_companion_source_preference(state)
    state._save_raw_companion_source_preference = lambda preference=None: live_lab_tab.LiveLabTab._save_raw_companion_source_preference(state, preference)
    state._on_raw_companion_source_preference_changed = lambda value: live_lab_tab.LiveLabTab._on_raw_companion_source_preference_changed(state, value)
    state._raw_capture_mode_label = lambda mode=None, short=False: live_lab_tab.LiveLabTab._raw_capture_mode_label(state, mode, short=short)
    state._restore_raw_capture_mode = lambda: live_lab_tab.LiveLabTab._restore_raw_capture_mode(state)
    state._save_raw_capture_mode = lambda mode=None: live_lab_tab.LiveLabTab._save_raw_capture_mode(state, mode)
    state._on_raw_capture_mode_changed = lambda value: live_lab_tab.LiveLabTab._on_raw_capture_mode_changed(state, value)
    state._pending_raw_capture_count = lambda: live_lab_tab.LiveLabTab._pending_raw_capture_count(state)
    state._selected_pending_raw_index_value = lambda: live_lab_tab.LiveLabTab._selected_pending_raw_index_value(state)
    state._current_pending_raw_capture = lambda: live_lab_tab.LiveLabTab._current_pending_raw_capture(state)
    state._pending_raw_gallery_key = lambda capture: live_lab_tab.LiveLabTab._pending_raw_gallery_key(state, capture)
    state._pending_raw_capture_index_for_key = lambda key: live_lab_tab.LiveLabTab._pending_raw_capture_index_for_key(state, key)
    state._pending_raw_gallery_items = lambda: live_lab_tab.LiveLabTab._pending_raw_gallery_items(state)
    state._session_gallery_items = lambda: live_lab_tab.LiveLabTab._session_gallery_items(state)
    state._refresh_pending_raw_gallery = lambda: live_lab_tab.LiveLabTab._refresh_pending_raw_gallery(state)
    state._on_pending_raw_gallery_clicked = lambda image_id, path: live_lab_tab.LiveLabTab._on_pending_raw_gallery_clicked(state, image_id, path)
    state._on_session_gallery_clicked = lambda image_id, path: live_lab_tab.LiveLabTab._on_session_gallery_clicked(state, image_id, path)
    state._update_pending_raw_controls = lambda: live_lab_tab.LiveLabTab._update_pending_raw_controls(state)
    state._show_pending_raw_capture = lambda index=None: live_lab_tab.LiveLabTab._show_pending_raw_capture(state, index)
    state._schedule_pending_raw_preview_refresh = lambda: live_lab_tab.LiveLabTab._schedule_pending_raw_preview_refresh(state)
    state._refresh_selected_pending_raw_preview = lambda: live_lab_tab.LiveLabTab._refresh_selected_pending_raw_preview(state)
    state._create_pending_raw_capture = lambda *args, **kwargs: live_lab_tab.LiveLabTab._create_pending_raw_capture(state, *args, **kwargs)
    state._add_pending_raw_capture = lambda pending: live_lab_tab.LiveLabTab._add_pending_raw_capture(state, pending)
    state._show_previous_pending_raw_capture = lambda: live_lab_tab.LiveLabTab._show_previous_pending_raw_capture(state)
    state._show_next_pending_raw_capture = lambda: live_lab_tab.LiveLabTab._show_next_pending_raw_capture(state)
    state._commit_pending_raw_capture = lambda pending: live_lab_tab.LiveLabTab._commit_pending_raw_capture(state, pending)
    state._commit_selected_pending_raw_capture = lambda: live_lab_tab.LiveLabTab._commit_selected_pending_raw_capture(state)
    state._discard_selected_pending_raw_capture = lambda: live_lab_tab.LiveLabTab._discard_selected_pending_raw_capture(state)
    state._apply_current_raw_settings_to_all_pending = lambda: live_lab_tab.LiveLabTab._apply_current_raw_settings_to_all_pending(state)
    state._finalize_local_ingest = lambda *args, **kwargs: live_lab_tab.LiveLabTab._finalize_local_ingest(state, *args, **kwargs)
    state._current_lab_metadata = lambda: {"image_type": "microscope"}
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._load_viewer_pixmap = lambda path: (QPixmap(10, 10), False)
    state._refresh_main_window_after_import = lambda *args, **kwargs: None
    state._update_session_controls = lambda: None
    state._show_session_image = lambda image_id: None
    state._update_observation_thumbnail = lambda: None
    state._refresh_session_gallery = lambda: live_lab_tab.LiveLabTab._refresh_session_gallery(state)
    state._log_session_event = lambda *args, **kwargs: None
    state.is_session_running = lambda: True
    state._selected_session_image_id = None
    state._session_image_ids = []
    state._session_import_count = 0
    state._main_window = SimpleNamespace()
    state.VIEWER_SCALE_BAR_UM = live_lab_tab.LiveLabTab.VIEWER_SCALE_BAR_UM
    state.tr = lambda text: text
    state._image_microns_per_pixel = lambda image: live_lab_tab.LiveLabTab._image_microns_per_pixel(state, image)
    state._viewer_scale_bar_config = lambda image: live_lab_tab.LiveLabTab._viewer_scale_bar_config(state, image)
    state._viewer_meta_text = lambda image: live_lab_tab.LiveLabTab._viewer_meta_text(state, image)
    return state


def _fake_render_raw_preview(source, *, settings=None, output_path=None, output_dir=None, **_kwargs):
    source_path = Path(source)
    preview_dir = Path(output_dir or tempfile.gettempdir())
    preview_dir.mkdir(parents=True, exist_ok=True)
    preview_path = Path(output_path) if output_path is not None else preview_dir / f"{source_path.stem}_preview.jpg"
    preview_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "source": str(source_path),
        "settings": settings.to_dict() if isinstance(settings, RawRenderSettings) else settings,
    }
    preview_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return str(preview_path)


def _fake_render_raw_jpeg(source, *, settings=None, output_path=None, output_dir=None, **_kwargs):
    source_path = Path(source)
    preview_dir = Path(output_dir or tempfile.gettempdir())
    preview_dir.mkdir(parents=True, exist_ok=True)
    preview_path = Path(output_path) if output_path is not None else preview_dir / f"{source_path.stem}_rendered.jpg"
    preview_path.parent.mkdir(parents=True, exist_ok=True)
    image = QImage(6, 4, QImage.Format.Format_RGB32)
    color = live_lab_tab.QColor(80, 120, 160)
    if isinstance(settings, RawRenderSettings):
        if settings.white_balance_mode == "auto":
            color = live_lab_tab.QColor(140, 110, 90)
        if settings.tone_curve_enabled:
            color = live_lab_tab.QColor(60, 180, 90)
    for y in range(image.height()):
        for x in range(image.width()):
            image.setPixelColor(x, y, color)
    qpixmap = QPixmap.fromImage(image)
    qpixmap.save(str(preview_path), "JPG")
    return str(preview_path)


def _make_raw_image_row(
    image_id: int,
    *,
    source_path: Path,
    derivative_path: Path,
    settings: RawRenderSettings | None = None,
) -> dict[str, object]:
    resolved_settings = RawRenderSettings.from_dict(settings or RawRenderSettings.default())
    return {
        "id": int(image_id),
        "filepath": str(derivative_path),
        "image_type": "microscope",
        "objective_name": "40x",
        "contrast": "phase",
        "mount_medium": "water",
        "stain": "none",
        "sample_type": "spore",
        "scale_microns_per_pixel": 1.2,
        "lab_metadata": {
            "image_type": "microscope",
            "contrast": "phase",
            "mount_medium": "water",
            "stain": "none",
            "sample_type": "spore",
            "raw_processing": {
                "engine": "rawpy",
                "source": {
                    "kind": "camera_raw",
                    "path": str(source_path),
                    "mime_type": "image/x-raw",
                    "captured_at": "2026:05:16 19:44:11",
                },
                "local_derivative": {
                    "kind": "rendered_from_raw",
                    "format": "jpeg",
                    "mime_type": "image/jpeg",
                    "path": str(derivative_path),
                    "quality": 95,
                    "subsampling": 0,
                    "width": 6,
                    "height": 4,
                    "rendered_at": "2026:05:16 19:45:11",
                },
                "settings": resolved_settings.to_dict(),
            },
        },
    }


def _install_fake_local_ingest_pipeline(
    monkeypatch,
    *,
    working_path_factory,
    captured_calls: list[dict[str, object]],
    add_image_calls: list[dict[str, object]],
    set_setting_calls: list[tuple[str, str]] | None = None,
):
    def fake_prepare_local_ingest_image(source, *, raw_settings=None, lab_metadata=None, output_dir=None):
        source_path = Path(source)
        source_text = str(source_path)
        is_raw = source_path.suffix.lower() in {".nef", ".orf", ".cr3", ".arw", ".dng", ".rw2", ".raf", ".pef", ".srw"}
        snapshot = raw_settings.to_dict() if isinstance(raw_settings, RawRenderSettings) else None
        working_path = str(working_path_factory(source_path))
        captured_calls.append(
            {
                "source": source_text,
                "raw_settings": snapshot,
                "lab_metadata": dict(lab_metadata or {}),
                "output_dir": str(output_dir) if output_dir is not None else None,
            }
        )
        metadata = dict(lab_metadata or {})
        if is_raw:
            metadata["raw_processing"] = {
                "engine": "rawpy",
                "source": {
                    "kind": "camera_raw",
                    "path": source_text,
                    "mime_type": "image/x-raw",
                },
                "local_derivative": {
                    "kind": "rendered_from_raw",
                    "path": working_path,
                    "mime_type": "image/jpeg",
                    "format": "jpeg",
                    "quality": 95,
                    "subsampling": 0,
                },
                "settings": snapshot,
            }
        return SimpleNamespace(
            source_path=source_text,
            working_path=working_path,
            original_path=source_text,
            raw_render_snapshot={"settings": snapshot} if is_raw and snapshot is not None else None,
            lab_metadata=metadata if is_raw else metadata,
            provenance_kwargs=lambda: {
                "source_role": "converted_local" if is_raw else "local_canonical",
                "file_purpose": "microscope",
                "original_mime_type": "image/x-raw" if is_raw else "image/jpeg",
                "working_mime_type": "image/jpeg",
            },
        )

    def fake_add_image(**kwargs):
        add_image_calls.append(dict(kwargs))
        return len(add_image_calls)

    monkeypatch.setattr(live_lab_tab, "prepare_local_ingest_image", fake_prepare_local_ingest_image)
    monkeypatch.setattr(live_lab_tab.ImageDB, "add_image", fake_add_image)
    monkeypatch.setattr(live_lab_tab, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(live_lab_tab, "cleanup_import_temp_file", lambda *args, **kwargs: None)
    if set_setting_calls is not None:
        monkeypatch.setattr(live_lab_tab.SettingsDB, "set_setting", lambda key, value: set_setting_calls.append((key, value)))


def _make_test_pixmap(width: int, height: int, *, rgb=(128, 128, 128)) -> QPixmap:
    image = QImage(width, height, QImage.Format.Format_RGB32)
    color = tuple(int(v) for v in rgb)
    for y in range(height):
        for x in range(width):
            image.setPixelColor(x, y, live_lab_tab.QColor(*color))
    return QPixmap.fromImage(image)


def test_live_lab_raw_summary_and_slider_state(monkeypatch):
    _qapp()
    state = _build_raw_controls_state()
    saved_settings: list[tuple[str, str]] = []
    monkeypatch.setattr(live_lab_tab.SettingsDB, "set_setting", lambda key, value: saved_settings.append((key, value)))

    live_lab_tab.LiveLabTab._sync_raw_processing_controls_from_settings(
        state,
        RawRenderSettings.default(),
    )

    assert live_lab_tab.LiveLabTab._raw_processing_summary_text(state) == "Camera WB · Auto levels · Curve off"
    assert (
        state._raw_settings_info_text(RawRenderSettings.default())
        == "Camera WB · Dark cutoff 0.05% · Bright cutoff 0.05% · Shadow lift 0.0% · Soft tails off · Curve strength 50% · Curve midpoint 50%"
    )
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
    assert saved_settings

    state.raw_white_balance_combo.setCurrentIndex(1)
    state.raw_auto_levels_checkbox.setChecked(False)
    live_lab_tab.LiveLabTab._on_raw_processing_controls_changed(state)

    assert live_lab_tab.LiveLabTab._raw_processing_summary_text(state) == "Auto WB · levels off · Curve 45 / mid 48"
    assert saved_settings[-1][0] == state._raw_processing_settings_key()
    assert json.loads(saved_settings[-1][1])["white_balance_mode"] == "auto"


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
    state._refresh_session_gallery = lambda: live_lab_tab.LiveLabTab._refresh_session_gallery(state)
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._log_session_event = lambda *args, **kwargs: None
    state.tr = lambda text: text

    captured_calls: list[dict[str, object]] = []
    add_image_calls: list[dict[str, object]] = []
    set_setting_calls: list[tuple[str, str]] = []

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

    def fake_set_setting(key, value):
        set_setting_calls.append((key, value))

    monkeypatch.setattr(live_lab_tab, "prepare_local_ingest_image", fake_prepare_local_ingest_image)
    monkeypatch.setattr(live_lab_tab.ImageDB, "add_image", fake_add_image)
    monkeypatch.setattr(live_lab_tab.ImageDB, "get_image", lambda image_id: {"filepath": working_path_one if image_id == 1 else working_path_two})
    monkeypatch.setattr(live_lab_tab, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(live_lab_tab, "cleanup_import_temp_file", lambda *args, **kwargs: None)
    monkeypatch.setattr(live_lab_tab.SettingsDB, "set_setting", fake_set_setting)

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
    assert set_setting_calls
    assert set_setting_calls[-1][0] == state._raw_processing_settings_key()
    assert json.loads(set_setting_calls[-1][1])["white_balance_mode"] == "auto"


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
    state._refresh_session_gallery = lambda: live_lab_tab.LiveLabTab._refresh_session_gallery(state)
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._log_session_event = lambda *args, **kwargs: None
    state.tr = lambda text: text

    captured_calls: list[dict[str, object]] = []
    add_image_calls: list[dict[str, object]] = []
    set_setting_calls: list[tuple[str, str]] = []

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

    def fake_set_setting(key, value):
        set_setting_calls.append((key, value))

    monkeypatch.setattr(live_lab_tab, "prepare_local_ingest_image", fake_prepare_local_ingest_image)
    monkeypatch.setattr(live_lab_tab.ImageDB, "add_image", fake_add_image)
    monkeypatch.setattr(live_lab_tab.ImageDB, "get_image", lambda image_id: {"filepath": working_path})
    monkeypatch.setattr(live_lab_tab, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(live_lab_tab, "cleanup_import_temp_file", lambda *args, **kwargs: None)
    monkeypatch.setattr(live_lab_tab.SettingsDB, "set_setting", fake_set_setting)

    assert live_lab_tab.LiveLabTab._ingest_detected_image(state, str(source_path))
    assert captured_calls[0]["raw_settings"]["white_balance_mode"] == "camera"
    assert captured_calls[0]["raw_settings"]["auto_levels"] is True
    assert "raw_processing" not in add_image_calls[0]["lab_metadata"]
    assert add_image_calls[0]["lab_metadata"]["contrast"] == "phase"
    assert not set_setting_calls


def test_live_lab_raw_settings_load_on_startup_context(monkeypatch):
    _qapp()
    state = _build_raw_controls_state()
    state._raw_processing_preset_context = lambda: {
        "capture_source": "live_lab",
        "instrument": "microscope",
        "objective_name": "40x",
        "contrast": "phase",
        "mount_medium": "water",
        "stain": "none",
        "sample_type": "spore",
    }
    expected_settings = RawRenderSettings(
        white_balance_mode="auto",
        auto_levels=False,
        tone_curve_enabled=True,
        tone_curve_strength=0.62,
        tone_curve_midpoint=0.31,
    )
    expected_key = state._raw_processing_settings_key()

    def fake_get_setting(key, default=None):
        if key == expected_key:
            return json.dumps(expected_settings.to_dict())
        return default

    monkeypatch.setattr(live_lab_tab.SettingsDB, "get_setting", fake_get_setting)
    monkeypatch.setattr(live_lab_tab.SettingsDB, "set_setting", lambda *args, **kwargs: None)

    loaded_settings = live_lab_tab.LiveLabTab._load_raw_processing_settings_for_current_context(state)

    assert loaded_settings == expected_settings
    assert state._raw_render_settings == expected_settings
    assert state.raw_white_balance_combo.currentData() == "auto"
    assert state.raw_auto_levels_checkbox.isChecked() is False
    assert state.raw_tone_curve_checkbox.isChecked() is True
    assert state.raw_curve_strength_slider.value() == 62
    assert state.raw_curve_midpoint_slider.value() == 31


def test_live_lab_raw_control_change_saves_current_context_settings(monkeypatch):
    _qapp()
    state = _build_raw_controls_state()
    state._raw_processing_preset_context = lambda: {
        "capture_source": "live_lab",
        "instrument": "microscope",
        "objective_name": "40x",
        "contrast": "phase",
        "mount_medium": "water",
        "stain": "none",
        "sample_type": "spore",
    }

    saved_settings: list[tuple[str, str]] = []
    monkeypatch.setattr(live_lab_tab.SettingsDB, "set_setting", lambda key, value: saved_settings.append((key, value)))

    state.raw_white_balance_combo.setCurrentIndex(1)
    state.raw_auto_levels_checkbox.setChecked(False)
    state.raw_tone_curve_checkbox.setChecked(True)
    state.raw_curve_strength_slider.setValue(70)
    state.raw_curve_midpoint_slider.setValue(35)

    live_lab_tab.LiveLabTab._on_raw_processing_controls_changed(state)

    assert saved_settings
    assert saved_settings[-1][0] == state._raw_processing_settings_key()
    saved_payload = json.loads(saved_settings[-1][1])
    assert saved_payload["white_balance_mode"] == "auto"
    assert saved_payload["auto_levels"] is False
    assert saved_payload["tone_curve_enabled"] is True
    assert saved_payload["tone_curve_strength"] == 0.7
    assert saved_payload["tone_curve_midpoint"] == 0.35


def test_live_lab_committed_raw_edit_preview_apply_updates_existing_row(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")
    derivative_path = tmp_path / "imports" / "sample.jpg"
    derivative_path.parent.mkdir(parents=True, exist_ok=True)
    _fake_render_raw_jpeg(source_path, output_path=derivative_path)

    state = _build_raw_controls_state()
    state._session_image_ids = [101]
    state._selected_session_image_id = 101
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: live_lab_tab.LiveLabTab._refresh_session_gallery(state)
    state._show_session_image = lambda image_id: live_lab_tab.LiveLabTab._show_session_image(state, image_id)
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda *args, **kwargs: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    state._current_lab_metadata = lambda: {"image_type": "microscope", "contrast": "phase"}
    state._load_viewer_pixmap = lambda path: (QPixmap(12, 8), False)

    images = {101: _make_raw_image_row(101, source_path=source_path, derivative_path=derivative_path)}
    update_calls: list[dict[str, object]] = []
    generate_calls: list[tuple[str, int]] = []
    saved_settings: list[tuple[str, str]] = []

    def fake_get_image(image_id):
        row = images.get(int(image_id))
        return copy.deepcopy(row) if row is not None else None

    def fake_update_image(image_id, **kwargs):
        update_calls.append({"image_id": int(image_id), **kwargs})
        row = images[int(image_id)]
        if "filepath" in kwargs and kwargs["filepath"] is not None:
            row["filepath"] = kwargs["filepath"]
        if "lab_metadata" in kwargs and kwargs["lab_metadata"] is not None:
            row["lab_metadata"] = copy.deepcopy(kwargs["lab_metadata"])

    monkeypatch.setattr(live_lab_tab.ImageDB, "get_image", fake_get_image)
    monkeypatch.setattr(live_lab_tab.ImageDB, "update_image", fake_update_image)
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)
    monkeypatch.setattr(live_lab_tab, "render_raw_image", _fake_render_raw_jpeg)
    monkeypatch.setattr(live_lab_tab, "generate_all_sizes", lambda filepath, image_id: generate_calls.append((str(filepath), int(image_id))) or {})
    monkeypatch.setattr(live_lab_tab.SettingsDB, "set_setting", lambda key, value: saved_settings.append((key, value)))

    assert live_lab_tab.LiveLabTab._begin_raw_edit_for_selected_image(state) is True
    assert state._raw_edit_session is not None
    preview_path = Path(state._raw_edit_session.preview_path)
    assert preview_path.exists()
    assert state.raw_edit_open_btn.isEnabled() is False
    assert state.raw_edit_apply_btn.isEnabled() is True
    assert state.viewer_title_label.text() == "Editing RAW: sample.nef"
    assert "Camera WB" in state.raw_edit_summary_label.text()

    state.raw_white_balance_combo.setCurrentIndex(1)
    state.raw_auto_levels_checkbox.setChecked(False)
    state.raw_tone_curve_checkbox.setChecked(True)
    state.raw_curve_strength_slider.setValue(70)
    state.raw_curve_midpoint_slider.setValue(35)
    live_lab_tab.LiveLabTab._on_raw_processing_controls_changed(state)

    assert state._raw_edit_session is not None
    assert state._raw_edit_session.working_settings.white_balance_mode == "auto"
    assert state._raw_edit_session.working_settings.auto_levels is False
    assert state._raw_edit_session.working_settings.tone_curve_enabled is True
    assert state._raw_edit_session.dirty is True
    assert saved_settings == []

    state._raw_edit_session.working_settings = RawRenderSettings(
        white_balance_mode="camera",
        wb_multipliers=(2.0, 1.0, 4.0),
        wb_selection=(1.0, 2.0, 3.0, 4.0),
        wb_selection_space="preview_pixels",
        auto_levels=True,
    )
    state._copy_raw_edit_settings()
    assert state._raw_copied_settings is not None
    assert state._raw_copied_settings.wb_multipliers == (2.0, 1.0, 4.0)
    assert state._raw_copied_settings.wb_selection is None
    assert state._raw_copied_settings.wb_selection_space == "inherited_multipliers"

    state.raw_white_balance_combo.setCurrentIndex(1)
    state.raw_auto_levels_checkbox.setChecked(False)
    state.raw_tone_curve_checkbox.setChecked(True)
    state.raw_curve_strength_slider.setValue(70)
    state.raw_curve_midpoint_slider.setValue(35)
    live_lab_tab.LiveLabTab._on_raw_processing_controls_changed(state)

    assert live_lab_tab.LiveLabTab._apply_raw_edit_session(state) is True
    assert state._raw_edit_session is None
    assert not preview_path.exists()
    assert len(update_calls) == 1
    assert len(generate_calls) == 1
    updated = update_calls[0]
    assert updated["image_id"] == 101
    assert updated["filepath"] == str(derivative_path)
    assert updated["lab_metadata"]["image_type"] == "microscope"
    assert updated["lab_metadata"]["contrast"] == "phase"
    assert updated["lab_metadata"]["raw_processing"]["source"]["kind"] == "camera_raw"
    assert updated["lab_metadata"]["raw_processing"]["source"]["path"] == str(source_path)
    assert updated["lab_metadata"]["raw_processing"]["local_derivative"]["kind"] == "rendered_from_raw"
    assert updated["lab_metadata"]["raw_processing"]["local_derivative"]["path"] == str(derivative_path)
    assert updated["lab_metadata"]["raw_processing"]["local_derivative"]["rendered_at"]
    assert updated["lab_metadata"]["raw_processing"]["settings"]["white_balance_mode"] == "auto"
    assert updated["lab_metadata"]["raw_processing"]["settings"]["tone_curve_enabled"] is True
    assert state._selected_session_image_id == 101
    assert state.raw_edit_open_btn.isEnabled() is True
    assert state.raw_edit_apply_btn.isEnabled() is False


def test_live_lab_committed_raw_edit_cancel_restores_future_controls_and_deletes_preview(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")
    derivative_path = tmp_path / "imports" / "sample.jpg"
    derivative_path.parent.mkdir(parents=True, exist_ok=True)
    _fake_render_raw_jpeg(source_path, output_path=derivative_path)

    state = _build_raw_controls_state()
    state._session_image_ids = [101]
    state._selected_session_image_id = 101
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: live_lab_tab.LiveLabTab._refresh_session_gallery(state)
    state._show_session_image = lambda image_id: live_lab_tab.LiveLabTab._show_session_image(state, image_id)
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda *args, **kwargs: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    state._current_lab_metadata = lambda: {"image_type": "microscope", "contrast": "phase"}
    state._load_viewer_pixmap = lambda path: (QPixmap(12, 8), False)

    images = {101: _make_raw_image_row(101, source_path=source_path, derivative_path=derivative_path)}
    monkeypatch.setattr(live_lab_tab.ImageDB, "get_image", lambda image_id: copy.deepcopy(images.get(int(image_id))) if images.get(int(image_id)) is not None else None)
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)
    monkeypatch.setattr(live_lab_tab.SettingsDB, "set_setting", lambda *args, **kwargs: None)

    assert live_lab_tab.LiveLabTab._begin_raw_edit_for_selected_image(state) is True
    assert state._raw_edit_session is not None
    preview_path = Path(state._raw_edit_session.preview_path)
    assert preview_path.exists()
    state.raw_white_balance_combo.setCurrentIndex(1)
    state.raw_auto_levels_checkbox.setChecked(False)
    live_lab_tab.LiveLabTab._on_raw_processing_controls_changed(state)
    assert state._raw_edit_session.dirty is True

    live_lab_tab.LiveLabTab._cancel_raw_edit_session(state)

    assert state._raw_edit_session is None
    assert not preview_path.exists()
    assert state._raw_render_settings == RawRenderSettings.default()
    assert state.raw_white_balance_combo.currentData() == "camera"
    assert state.raw_auto_levels_checkbox.isChecked() is True
    assert state.raw_tone_curve_checkbox.isChecked() is False
    assert state.raw_edit_open_btn.isEnabled() is True
    assert state.raw_edit_apply_btn.isEnabled() is False


def test_live_lab_raw_copy_settings_can_open_other_raw_image_without_selection_rect(tmp_path, monkeypatch):
    _qapp()
    source_one = tmp_path / "sample_1.nef"
    source_two = tmp_path / "sample_2.nef"
    source_one.write_bytes(b"raw-1")
    source_two.write_bytes(b"raw-2")
    derivative_one = tmp_path / "imports" / "sample_1.jpg"
    derivative_two = tmp_path / "imports" / "sample_2.jpg"
    derivative_one.parent.mkdir(parents=True, exist_ok=True)
    _fake_render_raw_jpeg(source_one, output_path=derivative_one)
    _fake_render_raw_jpeg(source_two, output_path=derivative_two)

    state = _build_raw_controls_state()
    state._session_image_ids = [101, 102]
    state._selected_session_image_id = 101
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: live_lab_tab.LiveLabTab._refresh_session_gallery(state)
    state._show_session_image = lambda image_id: live_lab_tab.LiveLabTab._show_session_image(state, image_id)
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda *args, **kwargs: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    state._current_lab_metadata = lambda: {"image_type": "microscope", "contrast": "phase"}
    state._load_viewer_pixmap = lambda path: (QPixmap(12, 8), False)

    images = {
        101: _make_raw_image_row(101, source_path=source_one, derivative_path=derivative_one),
        102: _make_raw_image_row(102, source_path=source_two, derivative_path=derivative_two),
    }
    monkeypatch.setattr(
        live_lab_tab.ImageDB,
        "get_image",
        lambda image_id: copy.deepcopy(images.get(int(image_id))) if images.get(int(image_id)) is not None else None,
    )
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)
    monkeypatch.setattr(live_lab_tab, "render_raw_image", _fake_render_raw_jpeg)
    monkeypatch.setattr(live_lab_tab.SettingsDB, "set_setting", lambda *args, **kwargs: None)

    assert live_lab_tab.LiveLabTab._begin_raw_edit_for_selected_image(state) is True
    state._raw_edit_session.working_settings = RawRenderSettings(
        white_balance_mode="camera",
        wb_multipliers=(1.9, 1.0, 2.8),
        wb_selection=(3.0, 4.0, 8.0, 6.0),
        wb_selection_space="preview_pixels",
        auto_levels=True,
        tone_curve_enabled=False,
    )
    state._copy_raw_edit_settings()
    assert state._raw_copied_settings is not None
    assert state._raw_copied_settings.wb_selection is None
    assert state._raw_copied_settings.wb_selection_space == "inherited_multipliers"

    state._selected_session_image_id = 102
    state.session_gallery.select_image(102)
    assert live_lab_tab.LiveLabTab._begin_raw_edit_with_copied_settings(state) is True
    assert state._raw_edit_session is not None
    assert state._raw_edit_session.image_id == 102
    assert state._raw_edit_session.working_settings.wb_multipliers == (1.9, 1.0, 2.8)
    assert state._raw_edit_session.working_settings.wb_selection is None
    assert state.raw_edit_open_btn.isEnabled() is False
    assert state.raw_edit_apply_btn.isEnabled() is True


def test_live_lab_non_raw_session_image_is_not_editable(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "sample.jpg"
    source_path.write_bytes(b"jpeg-bytes")

    state = _build_raw_controls_state()
    state._session_image_ids = [101]
    state._selected_session_image_id = 101
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: live_lab_tab.LiveLabTab._refresh_session_gallery(state)
    state._show_session_image = lambda image_id: live_lab_tab.LiveLabTab._show_session_image(state, image_id)
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda *args, **kwargs: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    state._current_lab_metadata = lambda: {"image_type": "microscope", "contrast": "phase"}
    state._load_viewer_pixmap = lambda path: (QPixmap(12, 8), False)

    images = {
        101: {
            "id": 101,
            "filepath": str(source_path),
            "image_type": "microscope",
            "contrast": "phase",
            "lab_metadata": {"image_type": "microscope", "contrast": "phase"},
        }
    }
    monkeypatch.setattr(
        live_lab_tab.ImageDB,
        "get_image",
        lambda image_id: copy.deepcopy(images.get(int(image_id))) if images.get(int(image_id)) is not None else None,
    )
    monkeypatch.setattr(live_lab_tab.SettingsDB, "set_setting", lambda *args, **kwargs: None)

    assert live_lab_tab.LiveLabTab._begin_raw_edit_for_selected_image(state) is False
    assert state._raw_edit_session is None
    assert state.raw_edit_frame.isVisible() is False
    assert state.raw_edit_open_btn.isEnabled() is False


def test_live_lab_raw_companion_source_preference_persists(monkeypatch):
    _qapp()
    state = _build_raw_controls_state()

    saved_settings: list[tuple[str, str]] = []
    monkeypatch.setattr(live_lab_tab.SettingsDB, "set_setting", lambda key, value: saved_settings.append((key, value)))

    state.raw_companion_source_selector.set_selected_value(live_lab_tab.RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG)
    live_lab_tab.LiveLabTab._on_raw_companion_source_preference_changed(
        state,
        live_lab_tab.RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG,
    )

    assert saved_settings[-1][0] == live_lab_tab.SETTING_RAW_COMPANION_SOURCE_PREFERENCE
    assert saved_settings[-1][1] == live_lab_tab.RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG
    assert state._raw_companion_source_preference == live_lab_tab.RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG

    monkeypatch.setattr(
        live_lab_tab.SettingsDB,
        "get_setting",
        lambda key, default=None: live_lab_tab.RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG if key == live_lab_tab.SETTING_RAW_COMPANION_SOURCE_PREFERENCE else default,
    )
    live_lab_tab.LiveLabTab._restore_raw_companion_source_preference(state)
    assert state.raw_companion_source_selector.selected_value() == live_lab_tab.RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG


def test_live_lab_review_queue_is_placed_in_the_main_viewer_area(monkeypatch, qapp):
    monkeypatch.setattr(live_lab_tab.LiveLabTab, "_populate_objective_combo", lambda self: None)
    monkeypatch.setattr(live_lab_tab.LiveLabTab, "_restore_watch_dir", lambda self: None)
    monkeypatch.setattr(live_lab_tab.LiveLabTab, "_restore_session_mode", lambda self: None)
    monkeypatch.setattr(live_lab_tab.LiveLabTab, "_restore_raw_capture_mode", lambda self: None)
    monkeypatch.setattr(live_lab_tab.LiveLabTab, "_load_raw_processing_settings_for_current_context", lambda self: self._raw_render_settings)
    monkeypatch.setattr(live_lab_tab.LiveLabTab, "_connect_session_logging_signals", lambda self: None)
    monkeypatch.setattr(live_lab_tab.LiveLabTab, "_clear_session_viewer", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(live_lab_tab.LiveLabTab, "_update_target_display", lambda self: None)
    monkeypatch.setattr(live_lab_tab.LiveLabTab, "_update_session_controls", lambda self: None)
    monkeypatch.setattr(live_lab_tab.LiveLabTab, "_register_hint_widgets", lambda self: None)
    monkeypatch.setattr(live_lab_tab.LiveLabTab, "_set_hint", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(live_lab_tab, "load_objectives", lambda: {})
    monkeypatch.setattr(
        live_lab_tab.SettingsDB,
        "get_setting",
        lambda key, default=None: default,
    )
    monkeypatch.setattr(live_lab_tab.SettingsDB, "set_setting", lambda *args, **kwargs: None)

    tab = live_lab_tab.LiveLabTab(SimpleNamespace())
    tab.show()
    qapp.processEvents()

    preview_path = Path(tempfile.gettempdir()) / "sporely_test_pending_preview.jpg"
    QPixmap(10, 10).save(str(preview_path), "JPG")
    pending = live_lab_tab.PendingRawCapture(
        source_path=Path(tempfile.gettempdir()) / "P070020_1.ORF",
        companion_jpeg_path=None,
        lab_metadata={"image_type": "microscope"},
        raw_settings=RawRenderSettings.default(),
        preview_path=preview_path,
    )
    tab._pending_raw_captures = [pending]
    tab._selected_pending_raw_index = 0
    tab._update_pending_raw_controls()
    qapp.processEvents()

    assert tab.viewer_panel.layout().indexOf(tab.pending_raw_frame) >= 0
    assert tab.pending_raw_frame.parentWidget() is tab.viewer_panel
    assert tab.pending_raw_frame.isVisible() is True
    assert tab.pending_raw_frame.sizeHint().height() <= 96
    assert tab.pending_raw_count_label.isVisible() is False
    assert tab.pending_raw_save_btn.text() == "Save current"
    assert tab.pending_raw_apply_all_btn.text() == "Apply settings to all pending"
    assert not hasattr(tab, "pending_raw_pick_wb_btn")
    assert tab.pending_raw_shortcuts_label.text() == "←/→ select · Delete/Backspace remove current image · Enter save"
    assert tab.session_gallery._items[0]["id"].startswith("pending:")
    assert tab.session_gallery._items[0]["badges"][0] == "UNSAVED RAW"
    assert tab.session_gallery._items[0]["frame_border_color"] == "#e67e22"
    assert tab.session_gallery._selected_id == tab.session_gallery._items[0]["id"]


def test_live_lab_review_keyboard_shortcuts_navigate_save_and_skip_text_focus(tmp_path, monkeypatch, qapp):
    _qapp()
    source_one = tmp_path / "P070020_1.ORF"
    source_two = tmp_path / "P070021_1.ORF"
    source_one.write_bytes(b"raw-1")
    source_two.write_bytes(b"raw-2")

    state = _build_raw_controls_state()
    state._session_observation_id = 1
    state._session_image_ids = []
    state._selected_session_image_id = None
    state._session_import_count = 0
    state._current_lab_metadata = lambda: {"image_type": "microscope", "contrast": "phase"}
    state._raw_processing_preset_context = lambda: {}
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: live_lab_tab.LiveLabTab._refresh_session_gallery(state)
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    state.raw_capture_mode_selector.set_selected_value(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    state._on_raw_capture_mode_changed(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)

    preview_pixmap = _make_test_pixmap(8, 8, rgb=(120, 120, 120))
    state._load_viewer_pixmap = lambda path: (preview_pixmap, False)

    captured_calls: list[dict[str, object]] = []
    add_image_calls: list[dict[str, object]] = []
    set_setting_calls: list[tuple[str, str]] = []
    _install_fake_local_ingest_pipeline(
        monkeypatch,
        working_path_factory=lambda source: tmp_path / "imports" / f"{source.stem}.jpg",
        captured_calls=captured_calls,
        add_image_calls=add_image_calls,
        set_setting_calls=set_setting_calls,
    )
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)

    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(state, str(source_one), group_key="group-1", state={})
    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(state, str(source_two), group_key="group-2", state={})

    assert state._selected_pending_raw_index == 1
    live_lab_tab.LiveLabTab._handle_raw_review_shortcut(state, "previous")
    assert state._selected_pending_raw_index == 0
    live_lab_tab.LiveLabTab._handle_raw_review_shortcut(state, "next")
    assert state._selected_pending_raw_index == 1

    focus_guard = QLineEdit()
    focus_guard.show()
    focus_guard.setFocus()
    qapp.processEvents()
    live_lab_tab.LiveLabTab._handle_raw_review_shortcut(state, "previous")
    assert state._selected_pending_raw_index == 1

    focus_guard.clearFocus()
    qapp.processEvents()
    live_lab_tab.LiveLabTab._handle_raw_review_shortcut(state, "save")

    assert len(add_image_calls) == 1
    assert state._pending_raw_captures
    assert state._selected_pending_raw_index == 0


def test_live_lab_review_keyboard_shortcut_discard_removes_selected_pending(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "P070020_1.ORF"
    source_path.write_bytes(b"raw-bytes")

    state = _build_raw_controls_state()
    state._session_observation_id = 1
    state._session_image_ids = []
    state._selected_session_image_id = None
    state._session_import_count = 0
    state._current_lab_metadata = lambda: {"image_type": "microscope", "contrast": "phase"}
    state._raw_processing_preset_context = lambda: {}
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: live_lab_tab.LiveLabTab._refresh_session_gallery(state)
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    state.raw_capture_mode_selector.set_selected_value(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    state._on_raw_capture_mode_changed(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)

    preview_pixmap = _make_test_pixmap(8, 8, rgb=(120, 120, 120))
    state._load_viewer_pixmap = lambda path: (preview_pixmap, False)

    add_image_calls: list[dict[str, object]] = []
    _install_fake_local_ingest_pipeline(
        monkeypatch,
        working_path_factory=lambda source: tmp_path / "imports" / f"{source.stem}.jpg",
        captured_calls=[],
        add_image_calls=add_image_calls,
    )
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)

    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(state, str(source_path), group_key="group-1", state={})
    assert state._pending_raw_captures
    assert live_lab_tab.LiveLabTab._handle_raw_review_shortcut(state, "discard") is None
    assert state._pending_raw_captures == []
    assert add_image_calls == []


def test_live_lab_current_lab_state_unset_combo_is_alert_styled():
    _qapp()
    state = _build_raw_controls_state()

    live_lab_tab.LiveLabTab._update_lab_state_combo_alerts(state)

    assert state.objective_combo.property("labStateAlert") is True
    assert state.objective_combo.styleSheet().startswith('QComboBox[labStateAlert="true"]')
    assert "background-color" in state.objective_combo.styleSheet()
    assert "QAbstractItemView" in state.objective_combo.styleSheet()
    assert state.contrast_combo.property("labStateAlert") is True
    assert state.mount_combo.property("labStateAlert") is True
    assert state.stain_combo.property("labStateAlert") is True
    assert state.sample_combo.property("labStateAlert") is True


def test_live_lab_background_wb_sampling_updates_pending_preview_and_metadata(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "P070020_1.ORF"
    source_path.write_bytes(b"raw-bytes")

    state = _build_raw_controls_state()
    state._session_observation_id = 1
    state._session_image_ids = []
    state._selected_session_image_id = None
    state._session_import_count = 0
    state._current_lab_metadata = lambda: {"image_type": "microscope", "contrast": "phase"}
    state._raw_processing_preset_context = lambda: {}
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: live_lab_tab.LiveLabTab._refresh_session_gallery(state)
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    state.raw_capture_mode_selector.set_selected_value(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    state._on_raw_capture_mode_changed(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)

    preview_image = QImage(8, 8, QImage.Format.Format_RGB32)
    for y in range(8):
        for x in range(8):
            preview_image.setPixelColor(x, y, live_lab_tab.QColor(20, 40, 10))
    preview_pixmap = QPixmap.fromImage(preview_image)
    state._load_viewer_pixmap = lambda path: (preview_pixmap, False)

    captured_calls: list[dict[str, object]] = []
    add_image_calls: list[dict[str, object]] = []
    _install_fake_local_ingest_pipeline(
        monkeypatch,
        working_path_factory=lambda source: tmp_path / "imports" / f"{source.stem}.jpg",
        captured_calls=captured_calls,
        add_image_calls=add_image_calls,
    )
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)

    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(state, str(source_path), group_key="group-1", state={})
    pending = state._current_pending_raw_capture()
    assert pending is not None

    live_lab_tab.LiveLabTab._toggle_pending_raw_background_wb_pick(state, True)
    assert state.pending_raw_pick_wb_btn.isChecked() is True

    assert live_lab_tab.LiveLabTab._on_live_image_clicked_for_background_wb(state, QPointF(0.5, 0.5)) is None

    pending = state._current_pending_raw_capture()
    assert pending is not None
    assert pending.raw_settings.white_balance_mode == "custom"
    assert pending.raw_settings.wb_multiplier_space == "post_decode_rgb"
    assert pending.raw_settings.wb_sample_base_mode == "camera"
    assert pending.raw_settings.wb_selection_space == "preview_pixels"
    assert pending.raw_settings.wb_sample_point == pytest.approx((0.5, 0.5), rel=1e-3)
    assert pending.raw_settings.wb_sample_size == 10
    assert pending.raw_settings.wb_selection == pytest.approx((0.0, 0.0, 5.5, 5.5), rel=1e-3)
    assert pending.raw_settings.wb_multipliers == pytest.approx((2.0, 1.0, 4.0), rel=1e-3)
    assert state.pending_raw_pick_wb_btn.isChecked() is False
    assert state.raw_white_balance_combo.currentData() == "custom"
    assert state.viewer_meta_label.text().startswith("Custom WB")
    assert "Dark cutoff" in state.viewer_meta_label.text()
    assert "Bright cutoff" in state.viewer_meta_label.text()
    assert "Shadow lift" in state.viewer_meta_label.text()
    assert "Soft tails" in state.viewer_meta_label.text()
    assert "Curve strength" in state.viewer_meta_label.text()
    assert "Curve midpoint" in state.viewer_meta_label.text()
    assert state.pending_raw_shortcuts_label.text().startswith("Custom WB")
    assert "Enter save" in state.pending_raw_shortcuts_label.text()
    preview_payload = json.loads(Path(pending.preview_path).read_text(encoding="utf-8"))
    assert preview_payload["settings"]["white_balance_mode"] == "custom"
    assert preview_payload["settings"]["wb_multiplier_space"] == "post_decode_rgb"
    assert preview_payload["settings"]["wb_sample_base_mode"] == "camera"
    assert preview_payload["settings"]["wb_selection_space"] == "preview_pixels"
    assert preview_payload["settings"]["wb_sample_point"] == [0.5, 0.5]
    assert preview_payload["settings"]["wb_sample_size"] == 10
    assert preview_payload["settings"]["wb_selection"] == [0.0, 0.0, 5.5, 5.5]
    assert preview_payload["settings"]["wb_multipliers"] == [2.0, 1.0, 4.0]

    state.live_image_label.original_pixmap = _make_test_pixmap(8, 8, rgb=(250, 10, 250))
    live_lab_tab.LiveLabTab._toggle_pending_raw_background_wb_pick(state, True)
    assert live_lab_tab.LiveLabTab._on_live_image_clicked_for_background_wb(state, QPointF(0.5, 0.5)) is None
    pending = state._current_pending_raw_capture()
    assert pending is not None
    assert pending.raw_settings.wb_multipliers == pytest.approx((2.0, 1.0, 4.0), rel=1e-3)
    assert pending.raw_settings.wb_sample_base_mode == "camera"
    assert captured_calls == []
    assert add_image_calls == []


def test_live_lab_background_wb_sampling_tracks_zoomed_view_crop(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "P070020_1.ORF"
    source_path.write_bytes(b"raw-bytes")

    state = _build_raw_controls_state()
    state._session_observation_id = 1
    state._session_image_ids = []
    state._selected_session_image_id = None
    state._session_import_count = 0
    state._current_lab_metadata = lambda: {"image_type": "microscope", "contrast": "phase"}
    state._raw_processing_preset_context = lambda: {}
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: live_lab_tab.LiveLabTab._refresh_session_gallery(state)
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    state.raw_capture_mode_selector.set_selected_value(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    state._on_raw_capture_mode_changed(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)

    preview_image = QImage(8, 8, QImage.Format.Format_RGB32)
    for y in range(8):
        for x in range(8):
            preview_image.setPixelColor(x, y, live_lab_tab.QColor(20, 40, 10))
    preview_pixmap = QPixmap.fromImage(preview_image)
    state._load_viewer_pixmap = lambda path: (preview_pixmap, False)

    captured_calls: list[dict[str, object]] = []
    add_image_calls: list[dict[str, object]] = []
    _install_fake_local_ingest_pipeline(
        monkeypatch,
        working_path_factory=lambda source: tmp_path / "imports" / f"{source.stem}.jpg",
        captured_calls=captured_calls,
        add_image_calls=add_image_calls,
    )
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)

    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(state, str(source_path), group_key="group-1", state={})
    pending = state._current_pending_raw_capture()
    assert pending is not None

    state.live_image_label.get_current_view_crop_rect = lambda: (2.0, 1.0, 4.0, 4.0)
    state.live_image_label.size = lambda: SimpleNamespace(width=lambda: 4, height=lambda: 4)

    live_lab_tab.LiveLabTab._toggle_pending_raw_background_wb_pick(state, True)
    assert live_lab_tab.LiveLabTab._on_live_image_clicked_for_background_wb(state, QPointF(4.0, 3.0)) is None

    pending = state._current_pending_raw_capture()
    assert pending is not None
    assert pending.raw_settings.wb_sample_point == pytest.approx((4.0, 3.0), rel=1e-3)
    assert pending.raw_settings.wb_selection == pytest.approx((2.0, 1.0, 4.0, 4.0), rel=1e-3)
    assert pending.raw_settings.wb_multipliers == pytest.approx((2.0, 1.0, 4.0), rel=1e-3)
    assert state.pending_raw_pick_wb_btn.isChecked() is False
    assert captured_calls == []
    assert add_image_calls == []


def test_live_lab_pending_raw_invalid_background_wb_sample_leaves_settings_unchanged(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "P070020_1.ORF"
    source_path.write_bytes(b"raw-bytes")

    state = _build_raw_controls_state()
    state._session_observation_id = 1
    state._session_image_ids = []
    state._selected_session_image_id = None
    state._session_import_count = 0
    state._current_lab_metadata = lambda: {"image_type": "microscope", "contrast": "phase"}
    state._raw_processing_preset_context = lambda: {}
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: live_lab_tab.LiveLabTab._refresh_session_gallery(state)
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    state.raw_capture_mode_selector.set_selected_value(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    state._on_raw_capture_mode_changed(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)

    captured_calls: list[dict[str, object]] = []
    add_image_calls: list[dict[str, object]] = []
    _install_fake_local_ingest_pipeline(
        monkeypatch,
        working_path_factory=lambda source: tmp_path / "imports" / f"{source.stem}.jpg",
        captured_calls=captured_calls,
        add_image_calls=add_image_calls,
    )
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)

    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(state, str(source_path), group_key="group-1", state={})
    pending = state._current_pending_raw_capture()
    assert pending is not None
    original_settings = RawRenderSettings.from_dict(pending.raw_settings)

    live_lab_tab.LiveLabTab._toggle_pending_raw_background_wb_pick(state, True)
    monkeypatch.setattr(state, "_raw_background_wb_sampling_pixmap", lambda target="pending": None)

    assert live_lab_tab.LiveLabTab._apply_raw_background_wb_selection_from_point(state, QPointF(1.0, 1.0)) is False
    assert pending.raw_settings == original_settings
    assert state.pending_raw_pick_wb_btn.isChecked() is True
    assert state.pending_raw_shortcuts_label.text() == "Click neutral background to set WB"

    live_lab_tab.LiveLabTab._cancel_active_raw_background_wb_selection(state)
    assert state.pending_raw_pick_wb_btn.isChecked() is False
    assert state.pending_raw_shortcuts_label.text() == "←/→ select · Delete/Backspace remove current image · Enter save"
    assert captured_calls == []
    assert add_image_calls == []


def test_live_lab_committed_raw_edit_background_wb_click_sets_custom_mode_and_applies_snapshot(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")
    derivative_path = tmp_path / "imports" / "sample.jpg"
    derivative_path.parent.mkdir(parents=True, exist_ok=True)
    _fake_render_raw_jpeg(source_path, output_path=derivative_path)

    state = _build_raw_controls_state()
    state._session_image_ids = [101]
    state._selected_session_image_id = 101
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: live_lab_tab.LiveLabTab._refresh_session_gallery(state)
    state._show_session_image = lambda image_id: live_lab_tab.LiveLabTab._show_session_image(state, image_id)
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda *args, **kwargs: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    state._current_lab_metadata = lambda: {"image_type": "microscope", "contrast": "phase"}
    preview_image = QImage(8, 8, QImage.Format.Format_RGB32)
    for y in range(8):
        for x in range(8):
            preview_image.setPixelColor(x, y, live_lab_tab.QColor(20, 40, 10))
    preview_pixmap = QPixmap.fromImage(preview_image)
    state._load_viewer_pixmap = lambda path: (preview_pixmap, False)

    images = {101: _make_raw_image_row(101, source_path=source_path, derivative_path=derivative_path)}
    update_calls: list[dict[str, object]] = []
    generate_calls: list[tuple[str, int]] = []
    saved_settings: list[tuple[str, str]] = []

    def fake_get_image(image_id):
        row = images.get(int(image_id))
        return copy.deepcopy(row) if row is not None else None

    def fake_update_image(image_id, **kwargs):
        update_calls.append({"image_id": int(image_id), **kwargs})
        row = images[int(image_id)]
        if "filepath" in kwargs and kwargs["filepath"] is not None:
            row["filepath"] = kwargs["filepath"]
        if "lab_metadata" in kwargs and kwargs["lab_metadata"] is not None:
            row["lab_metadata"] = copy.deepcopy(kwargs["lab_metadata"])

    monkeypatch.setattr(live_lab_tab.ImageDB, "get_image", fake_get_image)
    monkeypatch.setattr(live_lab_tab.ImageDB, "update_image", fake_update_image)
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)
    monkeypatch.setattr(live_lab_tab, "render_raw_image", _fake_render_raw_jpeg)
    monkeypatch.setattr(live_lab_tab, "generate_all_sizes", lambda filepath, image_id: generate_calls.append((str(filepath), int(image_id))) or {})
    monkeypatch.setattr(live_lab_tab.SettingsDB, "set_setting", lambda key, value: saved_settings.append((key, value)))

    assert live_lab_tab.LiveLabTab._begin_raw_edit_for_selected_image(state) is True
    assert state._raw_edit_session is not None
    assert state.raw_edit_pick_wb_btn.isChecked() is False

    live_lab_tab.LiveLabTab._toggle_raw_background_wb_pick(state, True, target="edit")
    assert state.raw_edit_pick_wb_btn.isChecked() is True
    assert live_lab_tab.LiveLabTab._on_live_image_clicked_for_background_wb(state, QPointF(0.5, 0.5)) is None

    assert state._raw_edit_session is not None
    assert state._raw_edit_session.working_settings.white_balance_mode == "custom"
    assert state._raw_edit_session.working_settings.wb_multiplier_space == "post_decode_rgb"
    assert state._raw_edit_session.working_settings.wb_sample_base_mode == "camera"
    assert state._raw_edit_session.working_settings.wb_sample_point == pytest.approx((0.5, 0.5), rel=1e-3)
    assert state._raw_edit_session.working_settings.wb_sample_size == 10
    assert state._raw_edit_session.working_settings.wb_selection == pytest.approx((0.0, 0.0, 5.5, 5.5), rel=1e-3)
    assert state._raw_edit_session.working_settings.wb_multipliers == pytest.approx((2.0, 1.0, 4.0), rel=1e-3)
    assert "Custom WB" in state.raw_edit_summary_label.text()
    assert "Dark cutoff" in state.raw_edit_summary_label.text()
    assert "Bright cutoff" in state.raw_edit_summary_label.text()
    assert "Shadow lift" in state.raw_edit_summary_label.text()
    assert "Soft tails" in state.raw_edit_summary_label.text()
    assert "Curve strength" in state.raw_edit_summary_label.text()
    assert "Curve midpoint" in state.raw_edit_summary_label.text()
    assert state.raw_edit_pick_wb_btn.isChecked() is False

    state.raw_tone_curve_checkbox.setChecked(True)
    state.raw_curve_strength_slider.setValue(70)
    state.raw_curve_midpoint_slider.setValue(35)
    live_lab_tab.LiveLabTab._on_raw_processing_controls_changed(state)

    assert live_lab_tab.LiveLabTab._apply_raw_edit_session(state) is True
    assert state._raw_edit_session is None
    assert len(update_calls) == 1
    assert len(generate_calls) == 1
    updated = update_calls[0]
    assert updated["lab_metadata"]["raw_processing"]["settings"]["white_balance_mode"] == "custom"
    assert updated["lab_metadata"]["raw_processing"]["settings"]["wb_multiplier_space"] == "post_decode_rgb"
    assert updated["lab_metadata"]["raw_processing"]["settings"]["wb_sample_base_mode"] == "camera"
    assert updated["lab_metadata"]["raw_processing"]["settings"]["wb_sample_size"] == 10
    assert updated["lab_metadata"]["raw_processing"]["settings"]["wb_sample_point"] == [0.5, 0.5]
    assert updated["lab_metadata"]["raw_processing"]["settings"]["wb_multipliers"] == [2.0, 1.0, 4.0]
    assert saved_settings == []


def test_live_lab_auto_save_mode_still_ingests_immediately(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "P070020_1.ORF"
    source_path.write_bytes(b"raw-bytes")

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
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: live_lab_tab.LiveLabTab._refresh_session_gallery(state)
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None

    captured_calls: list[dict[str, object]] = []
    add_image_calls: list[dict[str, object]] = []
    set_setting_calls: list[tuple[str, str]] = []
    _install_fake_local_ingest_pipeline(
        monkeypatch,
        working_path_factory=lambda source: tmp_path / "imports" / f"{source.stem}.jpg",
        captured_calls=captured_calls,
        add_image_calls=add_image_calls,
        set_setting_calls=set_setting_calls,
    )

    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(
        state,
        str(source_path),
        group_key="group-1",
        state={},
    )
    assert len(captured_calls) == 1
    assert captured_calls[0]["source"] == str(source_path)
    assert captured_calls[0]["raw_settings"]["white_balance_mode"] == "camera"
    assert len(add_image_calls) == 1
    assert add_image_calls[0]["filepath"].endswith("P070020_1.jpg")
    assert add_image_calls[0]["lab_metadata"]["raw_processing"]["source"]["kind"] == "camera_raw"
    assert state._pending_raw_captures == []
    assert state._session_import_count == 1
    assert set_setting_calls


def test_live_lab_review_mode_creates_pending_preview_and_defers_db_row(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "P070020_1.ORF"
    source_path.write_bytes(b"raw-bytes")

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
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: live_lab_tab.LiveLabTab._refresh_session_gallery(state)
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    state.raw_capture_mode_selector.set_selected_value(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    state._on_raw_capture_mode_changed(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)

    captured_calls: list[dict[str, object]] = []
    add_image_calls: list[dict[str, object]] = []
    set_setting_calls: list[tuple[str, str]] = []
    _install_fake_local_ingest_pipeline(
        monkeypatch,
        working_path_factory=lambda source: tmp_path / "imports" / f"{source.stem}.jpg",
        captured_calls=captured_calls,
        add_image_calls=add_image_calls,
        set_setting_calls=set_setting_calls,
    )
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)

    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(
        state,
        str(source_path),
        group_key="group-1",
        state={},
    )
    assert captured_calls == []
    assert add_image_calls == []
    assert len(state._pending_raw_captures) == 1
    pending = state._pending_raw_captures[0]
    assert pending.source_path == source_path
    assert pending.preview_path is not None
    assert pending.preview_path.exists()
    assert pending.status == "pending"
    assert pending.raw_settings == RawRenderSettings.default()
    assert pending.lab_metadata["contrast"] == "phase"
    assert state.raw_processing_toggle_btn.isChecked() is True
    assert state.pending_raw_frame.isVisible() is True
    assert state.session_gallery.visible is True
    assert len(state.session_gallery.items) == 1
    assert state.session_gallery.items[0]["filepath"] == str(source_path)
    assert state.session_gallery.items[0]["preview_path"] == str(pending.preview_path)
    assert state.session_gallery.items[0]["id"] == f"pending:{source_path}"
    assert state.session_gallery.items[0]["badges"][0] == "UNSAVED RAW"
    assert state.session_gallery.items[0]["frame_border_color"] == "#e67e22"
    assert state.session_gallery.selected == f"pending:{source_path}"


def test_live_lab_review_mode_pending_gallery_selection_tracks_current_item(tmp_path, monkeypatch):
    _qapp()
    source_one = tmp_path / "P070020_1.ORF"
    source_two = tmp_path / "P070021_1.ORF"
    source_one.write_bytes(b"raw-1")
    source_two.write_bytes(b"raw-2")

    state = _build_raw_controls_state()
    state._session_observation_id = 1
    state._session_image_ids = []
    state._selected_session_image_id = None
    state._session_import_count = 0
    state._current_lab_metadata = lambda: {"image_type": "microscope", "contrast": "phase"}
    state._raw_processing_preset_context = lambda: {}
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: live_lab_tab.LiveLabTab._refresh_session_gallery(state)
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    state.raw_capture_mode_selector.set_selected_value(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    state._on_raw_capture_mode_changed(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)

    captured_calls: list[dict[str, object]] = []
    add_image_calls: list[dict[str, object]] = []
    set_setting_calls: list[tuple[str, str]] = []
    _install_fake_local_ingest_pipeline(
        monkeypatch,
        working_path_factory=lambda source: tmp_path / "imports" / f"{source.stem}.jpg",
        captured_calls=captured_calls,
        add_image_calls=add_image_calls,
        set_setting_calls=set_setting_calls,
    )
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)

    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(state, str(source_one), group_key="group-1", state={})
    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(state, str(source_two), group_key="group-2", state={})

    assert len(state.session_gallery.items) == 2
    assert state.session_gallery.selected == f"pending:{source_two}"
    assert state.viewer_title_label.text().startswith("Pending RAW 2 of 2")

    first_item = state.session_gallery.items[0]
    live_lab_tab.LiveLabTab._on_pending_raw_gallery_clicked(state, first_item["id"], first_item["filepath"])

    assert state._selected_pending_raw_index == 0
    assert state.session_gallery.selected == f"pending:{source_one}"
    assert state.viewer_title_label.text().startswith("Pending RAW 1 of 2")


def test_live_lab_review_mode_commit_uses_selected_settings_and_keeps_snapshot(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "P070020_1.ORF"
    source_path.write_bytes(b"raw-bytes")

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
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: None
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    state.raw_capture_mode_selector.set_selected_value(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    state._on_raw_capture_mode_changed(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)

    captured_calls: list[dict[str, object]] = []
    add_image_calls: list[dict[str, object]] = []
    set_setting_calls: list[tuple[str, str]] = []
    _install_fake_local_ingest_pipeline(
        monkeypatch,
        working_path_factory=lambda source: tmp_path / "imports" / f"{source.stem}.jpg",
        captured_calls=captured_calls,
        add_image_calls=add_image_calls,
        set_setting_calls=set_setting_calls,
    )
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)

    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(
        state,
        str(source_path),
        group_key="group-1",
        state={},
    )
    assert len(state._pending_raw_captures) == 1
    pending = state._pending_raw_captures[0]
    state._selected_pending_raw_index = 0
    live_lab_tab.LiveLabTab._show_pending_raw_capture(state, 0)

    state.raw_white_balance_combo.setCurrentIndex(1)
    state.raw_auto_levels_checkbox.setChecked(False)
    state.raw_tone_curve_checkbox.setChecked(True)
    state.raw_curve_strength_slider.setValue(70)
    state.raw_curve_midpoint_slider.setValue(35)
    live_lab_tab.LiveLabTab._on_raw_processing_controls_changed(state)

    assert pending.raw_settings.white_balance_mode == "auto"
    assert pending.raw_settings.auto_levels is False
    assert pending.raw_settings.tone_curve_enabled is True
    assert pending.raw_settings.tone_curve_strength == 0.7
    assert pending.raw_settings.tone_curve_midpoint == 0.35

    live_lab_tab.LiveLabTab._commit_selected_pending_raw_capture(state)

    assert len(captured_calls) == 1
    assert captured_calls[0]["raw_settings"]["white_balance_mode"] == "auto"
    assert captured_calls[0]["raw_settings"]["auto_levels"] is False
    assert len(add_image_calls) == 1
    stored_snapshot = add_image_calls[0]["lab_metadata"]["raw_processing"]["settings"]
    assert stored_snapshot["white_balance_mode"] == "auto"
    assert stored_snapshot["tone_curve_enabled"] is True
    assert state._pending_raw_captures == []
    assert not pending.preview_path.exists()


def test_live_lab_ingest_detected_image_merges_partial_and_current_metadata(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "P070020_1.ORF"
    source_path.write_bytes(b"raw-bytes")

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
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: None
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    state._raw_processing_preset_context = lambda: {}

    captured_calls: list[dict[str, object]] = []
    add_image_calls: list[dict[str, object]] = []
    set_setting_calls: list[tuple[str, str]] = []
    _install_fake_local_ingest_pipeline(
        monkeypatch,
        working_path_factory=lambda source: tmp_path / "imports" / f"{source.stem}.jpg",
        captured_calls=captured_calls,
        add_image_calls=add_image_calls,
        set_setting_calls=set_setting_calls,
    )
    monkeypatch.setattr(live_lab_tab, "load_objectives", lambda: {"40x": {"microns_per_pixel": 0.5}})
    monkeypatch.setattr(
        live_lab_tab.CalibrationDB,
        "get_active_calibration_id",
        lambda objective_key: 77 if objective_key == "40x" else None,
    )
    monkeypatch.setattr(
        live_lab_tab.ImageDB,
        "get_image",
        lambda image_id: {"filepath": str(tmp_path / "imports" / "P070020_1.jpg")},
    )

    assert live_lab_tab.LiveLabTab._ingest_detected_image(
        state,
        str(source_path),
        lab_metadata={"objective_name": "40x"},
    )

    assert len(captured_calls) == 1
    assert captured_calls[0]["lab_metadata"]["objective_name"] == "40x"
    assert captured_calls[0]["lab_metadata"]["contrast"] == "phase"
    assert captured_calls[0]["lab_metadata"]["mount_medium"] == "water"
    assert captured_calls[0]["lab_metadata"]["stain"] == "none"
    assert captured_calls[0]["lab_metadata"]["sample_type"] == "spore"
    assert len(add_image_calls) == 1
    merged = add_image_calls[0]["lab_metadata"]
    assert merged["image_type"] == "microscope"
    assert merged["objective_name"] == "40x"
    assert merged["contrast"] == "phase"
    assert merged["mount_medium"] == "water"
    assert merged["stain"] == "none"
    assert merged["sample_type"] == "spore"
    assert merged["raw_processing"]["source"]["kind"] == "camera_raw"
    assert merged["raw_processing"]["source"]["path"] == str(source_path)
    assert set_setting_calls


def test_live_lab_review_mode_discard_drops_pending_without_db_row(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "P070020_1.ORF"
    source_path.write_bytes(b"raw-bytes")

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
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: None
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    state.raw_capture_mode_selector.set_selected_value(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    state._on_raw_capture_mode_changed(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)

    captured_calls: list[dict[str, object]] = []
    add_image_calls: list[dict[str, object]] = []
    set_setting_calls: list[tuple[str, str]] = []
    _install_fake_local_ingest_pipeline(
        monkeypatch,
        working_path_factory=lambda source: tmp_path / "imports" / f"{source.stem}.jpg",
        captured_calls=captured_calls,
        add_image_calls=add_image_calls,
        set_setting_calls=set_setting_calls,
    )
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)

    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(
        state,
        str(source_path),
        group_key="group-1",
        state={},
    )
    assert len(state._pending_raw_captures) == 1
    pending = state._pending_raw_captures[0]
    state._selected_pending_raw_index = 0
    live_lab_tab.LiveLabTab._show_pending_raw_capture(state, 0)
    preview_path = Path(pending.preview_path)
    assert preview_path.exists()

    live_lab_tab.LiveLabTab._discard_selected_pending_raw_capture(state)

    assert captured_calls == []
    assert add_image_calls == []
    assert state._pending_raw_captures == []
    assert not preview_path.exists()


def test_live_lab_review_mode_multiple_raw_arrivals_queue_separately(tmp_path, monkeypatch):
    _qapp()
    source_one = tmp_path / "P070020_1.ORF"
    source_two = tmp_path / "P070021_1.ORF"
    source_one.write_bytes(b"raw-1")
    source_two.write_bytes(b"raw-2")

    state = _build_raw_controls_state()
    state._session_observation_id = 1
    state._session_image_ids = []
    state._selected_session_image_id = None
    state._session_import_count = 0
    state._current_lab_metadata = lambda: {"image_type": "microscope", "contrast": "phase"}
    state._raw_processing_preset_context = lambda: {}
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: None
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    state.raw_capture_mode_selector.set_selected_value(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    state._on_raw_capture_mode_changed(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)

    captured_calls: list[dict[str, object]] = []
    add_image_calls: list[dict[str, object]] = []
    set_setting_calls: list[tuple[str, str]] = []
    _install_fake_local_ingest_pipeline(
        monkeypatch,
        working_path_factory=lambda source: tmp_path / "imports" / f"{source.stem}.jpg",
        captured_calls=captured_calls,
        add_image_calls=add_image_calls,
        set_setting_calls=set_setting_calls,
    )
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)

    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(state, str(source_one), group_key="group-1", state={})
    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(state, str(source_two), group_key="group-2", state={})

    assert len(state._pending_raw_captures) == 2
    assert [capture.source_path.name for capture in state._pending_raw_captures] == ["P070020_1.ORF", "P070021_1.ORF"]
    assert add_image_calls == []


def test_live_lab_review_mode_raw_jpeg_companion_creates_one_pending_entry(tmp_path, monkeypatch):
    _qapp()
    raw_path = tmp_path / "P070020_1.ORF"
    jpeg_path = tmp_path / "P070020_1.JPG"
    raw_path.write_bytes(b"raw-bytes")
    jpeg_path.write_bytes(b"jpeg-bytes")

    state = _build_raw_controls_state()
    state._session_observation_id = 1
    state._session_image_ids = []
    state._selected_session_image_id = None
    state._session_import_count = 0
    state._current_lab_metadata = lambda: {"image_type": "microscope", "contrast": "phase"}
    state._raw_processing_preset_context = lambda: {}
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: None
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    state.raw_capture_mode_selector.set_selected_value(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    state._on_raw_capture_mode_changed(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    state._same_stem_companion_paths = lambda source_path: [str(raw_path), str(jpeg_path)]
    state._fallback_companion_path = lambda source_path, exclude_path=None: str(jpeg_path) if str(source_path) == str(raw_path) else None

    captured_calls: list[dict[str, object]] = []
    add_image_calls: list[dict[str, object]] = []
    set_setting_calls: list[tuple[str, str]] = []
    _install_fake_local_ingest_pipeline(
        monkeypatch,
        working_path_factory=lambda source: tmp_path / "imports" / f"{source.stem}.jpg",
        captured_calls=captured_calls,
        add_image_calls=add_image_calls,
        set_setting_calls=set_setting_calls,
    )
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)

    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(
        state,
        str(raw_path),
        group_key="group-1",
        state={},
    )
    assert len(state._pending_raw_captures) == 1
    pending = state._pending_raw_captures[0]
    assert pending.source_path == raw_path
    assert pending.companion_jpeg_path == jpeg_path
    assert add_image_calls == []


def test_live_lab_review_mode_falls_back_to_companion_jpeg_when_raw_preview_fails(tmp_path, monkeypatch):
    _qapp()
    raw_path = tmp_path / "P070020_1.ORF"
    jpeg_path = tmp_path / "P070020_1.JPG"
    raw_path.write_bytes(b"raw-bytes")
    jpeg_path.write_bytes(b"jpeg-bytes")

    state = _build_raw_controls_state()
    state._session_observation_id = 1
    state._session_image_ids = []
    state._selected_session_image_id = None
    state._session_import_count = 0
    state._current_lab_metadata = lambda: {"image_type": "microscope", "contrast": "phase"}
    state._raw_processing_preset_context = lambda: {}
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: None
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    state.raw_capture_mode_selector.set_selected_value(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    state._on_raw_capture_mode_changed(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    state._same_stem_companion_paths = lambda source_path: [str(raw_path), str(jpeg_path)]
    state._fallback_companion_path = lambda source_path, exclude_path=None: str(jpeg_path) if str(source_path) == str(raw_path) else None

    captured_calls: list[dict[str, object]] = []
    add_image_calls: list[dict[str, object]] = []
    set_setting_calls: list[tuple[str, str]] = []
    _install_fake_local_ingest_pipeline(
        monkeypatch,
        working_path_factory=lambda source: tmp_path / "imports" / f"{source.stem}.jpg",
        captured_calls=captured_calls,
        add_image_calls=add_image_calls,
        set_setting_calls=set_setting_calls,
    )

    def failing_preview(*args, **kwargs):
        raise RawRenderingUnavailableError("rawpy unavailable")

    monkeypatch.setattr(live_lab_tab, "render_raw_preview", failing_preview)

    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(
        state,
        str(raw_path),
        group_key="group-1",
        state={},
    )
    assert len(captured_calls) == 1
    assert captured_calls[0]["source"] == str(jpeg_path)
    assert len(add_image_calls) == 1
    assert "raw_processing" not in add_image_calls[0]["lab_metadata"]
    assert state._pending_raw_captures == []


def test_live_lab_review_mode_apply_current_settings_to_all_pending(tmp_path, monkeypatch):
    _qapp()
    source_one = tmp_path / "P070020_1.ORF"
    source_two = tmp_path / "P070021_1.ORF"
    source_one.write_bytes(b"raw-1")
    source_two.write_bytes(b"raw-2")

    state = _build_raw_controls_state()
    state._session_observation_id = 1
    state._session_image_ids = []
    state._selected_session_image_id = None
    state._session_import_count = 0
    state._current_lab_metadata = lambda: {"image_type": "microscope", "contrast": "phase"}
    state._raw_processing_preset_context = lambda: {}
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: None
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    state.raw_capture_mode_selector.set_selected_value(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    state._on_raw_capture_mode_changed(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)

    captured_calls: list[dict[str, object]] = []
    add_image_calls: list[dict[str, object]] = []
    set_setting_calls: list[tuple[str, str]] = []
    _install_fake_local_ingest_pipeline(
        monkeypatch,
        working_path_factory=lambda source: tmp_path / "imports" / f"{source.stem}.jpg",
        captured_calls=captured_calls,
        add_image_calls=add_image_calls,
        set_setting_calls=set_setting_calls,
    )
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)

    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(state, str(source_one), group_key="group-1", state={})
    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(state, str(source_two), group_key="group-2", state={})

    state.raw_white_balance_combo.setCurrentIndex(1)
    state.raw_auto_levels_checkbox.setChecked(False)
    state.raw_tone_curve_checkbox.setChecked(True)
    state.raw_curve_strength_slider.setValue(80)
    state.raw_curve_midpoint_slider.setValue(20)
    live_lab_tab.LiveLabTab._on_raw_processing_controls_changed(state)

    live_lab_tab.LiveLabTab._apply_current_raw_settings_to_all_pending(state)

    assert len(state._pending_raw_captures) == 2
    assert all(capture.raw_settings.white_balance_mode == "auto" for capture in state._pending_raw_captures)
    assert all(capture.raw_settings.auto_levels is False for capture in state._pending_raw_captures)
    assert all(capture.raw_settings.tone_curve_enabled is True for capture in state._pending_raw_captures)
    assert all(capture.preview_path is not None and Path(capture.preview_path).exists() for capture in state._pending_raw_captures)
    assert all(
        json.loads(Path(capture.preview_path).read_text(encoding="utf-8"))["settings"]["white_balance_mode"] == "auto"
        for capture in state._pending_raw_captures
        if capture.preview_path is not None
    )
    assert add_image_calls == []
