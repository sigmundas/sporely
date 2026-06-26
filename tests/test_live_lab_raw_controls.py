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
from PySide6.QtWidgets import QApplication, QCheckBox, QComboBox, QFrame, QHBoxLayout, QLabel, QLineEdit, QPushButton, QSlider, QToolButton, QWidget
from PySide6.QtTest import QTest

import ui.live_lab_tab as live_lab_tab
from ui.adaptive_choice_selector import AdaptiveChoiceSelector
from ui.raw_processing_controls import RawProcessingControls
from ui.segmented_selector import SegmentedSelector
from utils.raw_render import RawRenderSettings, RawRenderingUnavailableError


def _qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _show_selector_in_pill_mode(qapp, selector: AdaptiveChoiceSelector, *, width: int = 1100) -> QWidget:
    container = QWidget()
    layout = QHBoxLayout(container)
    layout.setContentsMargins(0, 0, 0, 0)
    layout.addWidget(selector)
    container.resize(width, 48)
    container.show()
    qapp.processEvents()
    QTest.qWait(150)
    assert selector.display_mode() == "pill"
    return container


def _click_and_settle(qapp, button) -> None:
    QTest.mouseClick(button, Qt.LeftButton)
    qapp.processEvents()
    QTest.qWait(150)


def _build_raw_controls_state() -> SimpleNamespace:
    state = SimpleNamespace()
    state.RAW_CAPTURE_MODE_AUTO_SAVE = live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_AUTO_SAVE
    state.RAW_CAPTURE_MODE_REVIEW = live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW
    state.SESSION_MODE_LIVE = live_lab_tab.LiveLabTab.SESSION_MODE_LIVE
    state.SESSION_MODE_OFFLINE = live_lab_tab.LiveLabTab.SESSION_MODE_OFFLINE
    state.RAW_BACKGROUND_WB_SAMPLE_SIZE = live_lab_tab.LiveLabTab.RAW_BACKGROUND_WB_SAMPLE_SIZE
    state.SETTING_RAW_CAPTURE_MODE = live_lab_tab.LiveLabTab.SETTING_RAW_CAPTURE_MODE
    state.SETTING_LAST_OBJECTIVE = live_lab_tab.LiveLabTab.SETTING_LAST_OBJECTIVE
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
    state.raw_controls = RawProcessingControls()
    state._raw_auto_level_settings_for_source = lambda source, settings: live_lab_tab.LiveLabTab._raw_auto_level_settings_for_source(
        state,
        source,
        settings,
    )
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
    def _combo_value(combo):
        try:
            value = combo.currentData()
        except Exception:
            value = None
        return value

    state.pending_raw_frame = QFrame()
    state.pending_raw_count_label = QLabel()
    state.pending_raw_save_btn = QPushButton("Save current")
    state.pending_raw_save_all_btn = QPushButton("Save all")
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
        _full_image_path=None,
        crop_mode=False,
        crop_box=None,
        crop_preview=None,
        cursor=Qt.ArrowCursor,
        objective_text=None,
        objective_color=None,
        set_image_sources=lambda pixmap, full_path=None, *args, **kwargs: (
            setattr(state.live_image_label, "original_pixmap", pixmap),
            setattr(state.live_image_label, "_full_image_path", str(full_path) if full_path else None),
        ),
        set_microns_per_pixel=lambda *args, **kwargs: None,
        set_scale_bar=lambda *args, **kwargs: None,
        reset_view=lambda: None,
        set_crop_mode=lambda enabled: setattr(state.live_image_label, "crop_mode", bool(enabled)),
        set_crop_overlay_style=lambda *args, **kwargs: None,
        clear_crop_box=lambda: (setattr(state.live_image_label, "crop_box", None), setattr(state.live_image_label, "crop_preview", None)),
        set_crop_box=lambda box: setattr(state.live_image_label, "crop_box", box),
        set_objective_text=lambda text: setattr(state.live_image_label, "objective_text", text),
        set_objective_color=lambda color: setattr(state.live_image_label, "objective_color", color),
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
    def _gallery_item_key(item):
        return item.get("id") if item.get("id") is not None else item.get("filepath")

    def _gallery_select_paths(paths):
        normalized_paths = {str(path) for path in (paths or []) if path}
        selected_keys = set()
        first_selected = None
        for item in state.session_gallery.items:
            filepath = item.get("filepath")
            if filepath in normalized_paths:
                key = _gallery_item_key(item)
                selected_keys.add(key)
                if first_selected is None:
                    first_selected = key
        state.session_gallery._selected_keys = selected_keys
        state.session_gallery.selected = first_selected
        state.session_gallery._centered_on = first_selected

    def _gallery_selected_paths():
        selected = []
        for item in state.session_gallery.items:
            key = _gallery_item_key(item)
            if key in state.session_gallery._selected_keys and item.get("filepath"):
                selected.append(str(item.get("filepath")))
        return selected

    state.session_gallery = SimpleNamespace(
        items=[],
        selected=None,
        visible=False,
        invalidated_paths=[],
        _multi_select=False,
        _selected_keys=set(),
        _centered_on=None,
        clear=lambda: (
            state.session_gallery.items.clear(),
            setattr(state.session_gallery, "visible", False),
            setattr(state.session_gallery, "selected", None),
            setattr(state.session_gallery, "_selected_keys", set()),
        ),
        set_items=lambda items: setattr(state.session_gallery, "items", list(items)) or setattr(
            state.session_gallery,
            "visible",
            bool(items),
        ),
        select_image=lambda image_id: (
            setattr(state.session_gallery, "selected", image_id),
            setattr(state.session_gallery, "_selected_keys", {image_id} if image_id is not None else set()),
            setattr(state.session_gallery, "_centered_on", image_id),
        ),
        selected_keys=lambda: set(state.session_gallery._selected_keys),
        selected_paths=_gallery_selected_paths,
        select_paths=_gallery_select_paths,
        set_multi_select=lambda enabled: setattr(state.session_gallery, "_multi_select", bool(enabled)),
        is_multi_select=lambda: bool(state.session_gallery._multi_select),
        center_on_key=lambda key: setattr(state.session_gallery, "_centered_on", key),
        invalidate_pixmap_cache=lambda path=None: state.session_gallery.invalidated_paths.append(str(path) if path is not None else None),
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
    state._reset_companion_dedupe_state = lambda: live_lab_tab.LiveLabTab._reset_companion_dedupe_state(state)
    state._handle_raw_companion_source = lambda source, group_key, state_dict: live_lab_tab.LiveLabTab._handle_raw_companion_source(state, source, group_key=group_key, state=state_dict)
    state._queue_companion_source = lambda source_path: live_lab_tab.LiveLabTab._queue_companion_source(state, source_path)
    state._flush_companion_group = lambda group_key: live_lab_tab.LiveLabTab._flush_companion_group(state, group_key)
    state._raw_processing_preset_context = lambda: {}
    state._raw_settings_from_controls = lambda update_session_settings=True: live_lab_tab.LiveLabTab._raw_settings_from_controls(
        state,
        update_session_settings=update_session_settings,
    )
    state._sync_raw_processing_controls_from_settings = lambda settings=None, update_session_settings=True, auto_level_settings=None: live_lab_tab.LiveLabTab._sync_raw_processing_controls_from_settings(
        state,
        settings,
        update_session_settings=update_session_settings,
        auto_level_settings=auto_level_settings,
    )
    state._set_raw_tone_controls_enabled = lambda enabled: live_lab_tab.LiveLabTab._set_raw_tone_controls_enabled(state, enabled)
    state._raw_settings_summary_text = lambda settings: live_lab_tab.LiveLabTab._raw_settings_summary_text(state, settings)
    state._raw_settings_info_text = lambda settings: live_lab_tab.LiveLabTab._raw_settings_info_text(state, settings)
    state._raw_processing_summary_text = lambda: live_lab_tab.LiveLabTab._raw_processing_summary_text(state)
    state._refresh_raw_processing_context_ui = lambda: live_lab_tab.LiveLabTab._refresh_raw_processing_context_ui(state)
    state._session_gallery_selected_keys = lambda: live_lab_tab.LiveLabTab._session_gallery_selected_keys(state)
    state._objective_key_from_metadata = lambda metadata: live_lab_tab.LiveLabTab._objective_key_from_metadata(state, metadata)
    state._microscope_state_from_metadata = lambda metadata: live_lab_tab.LiveLabTab._microscope_state_from_metadata(state, metadata)
    state._apply_microscope_state_to_controls = lambda metadata: live_lab_tab.LiveLabTab._apply_microscope_state_to_controls(
        state,
        metadata,
    )
    state._selected_raw_processing_targets = lambda: live_lab_tab.LiveLabTab._selected_raw_processing_targets(state)
    state._visible_raw_processing_target = lambda: live_lab_tab.LiveLabTab._visible_raw_processing_target(state)
    state._preserve_raw_wb_fields = lambda base_settings, resolved_settings: live_lab_tab.LiveLabTab._preserve_raw_wb_fields(
        base_settings,
        resolved_settings,
    )
    state._apply_raw_settings_to_pending_capture = lambda capture, settings: live_lab_tab.LiveLabTab._apply_raw_settings_to_pending_capture(
        state,
        capture,
        settings,
    )
    state._sync_selected_pending_raw_metadata_from_controls = lambda: live_lab_tab.LiveLabTab._sync_selected_pending_raw_metadata_from_controls(state)
    state._selected_pending_raw_captures = lambda: live_lab_tab.LiveLabTab._selected_pending_raw_captures(state)
    state._update_viewer_objective_tag = lambda metadata=None, use_current_state=False: live_lab_tab.LiveLabTab._update_viewer_objective_tag(
        state,
        metadata,
        use_current_state=use_current_state,
    )
    state._refresh_viewer_objective_tag_from_current_state = lambda *_args: live_lab_tab.LiveLabTab._refresh_viewer_objective_tag_from_current_state(
        state,
        *_args,
    )
    state._selected_combo_value = lambda combo: live_lab_tab.LiveLabTab._selected_combo_value(combo)
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
    state._normalize_session_mode = lambda value: live_lab_tab.LiveLabTab._normalize_session_mode(state, value)
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
    state._invalidate_thumbnail_caches_for_raw_image = lambda image_id, final_path: live_lab_tab.LiveLabTab._invalidate_thumbnail_caches_for_raw_image(
        state,
        image_id,
        final_path,
    )
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
    state._ingest_detected_image = (
        lambda source_path, raw_settings=None, lab_metadata=None, observation_id=None: live_lab_tab.LiveLabTab._ingest_detected_image(
            state,
            source_path,
            raw_settings=raw_settings,
            lab_metadata=lab_metadata,
            observation_id=observation_id,
        )
    )
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
    state._remove_pending_raw_capture = lambda pending, status, refresh_ui=True: live_lab_tab.LiveLabTab._remove_pending_raw_capture(
        state,
        pending,
        status=status,
        refresh_ui=refresh_ui,
    )
    state._commit_selected_pending_raw_capture = lambda: live_lab_tab.LiveLabTab._commit_selected_pending_raw_capture(state)
    state._commit_all_pending_raw_captures = lambda: live_lab_tab.LiveLabTab._commit_all_pending_raw_captures(state)
    state._discard_selected_pending_raw_capture = lambda: live_lab_tab.LiveLabTab._discard_selected_pending_raw_capture(state)
    state._delete_current_raw_review_item = lambda: live_lab_tab.LiveLabTab._delete_current_raw_review_item(state)
    state._apply_current_raw_settings_to_all_pending = lambda: live_lab_tab.LiveLabTab._apply_current_raw_settings_to_all_pending(state)
    state._finalize_local_ingest = lambda *args, **kwargs: live_lab_tab.LiveLabTab._finalize_local_ingest(state, *args, **kwargs)
    state._current_lab_metadata = lambda: {
        "image_type": "microscope",
        "objective_name": _combo_value(state.objective_combo),
        "objective_label": str(state.objective_combo.currentText() or "").strip() or None,
        "contrast": _combo_value(state.contrast_combo),
        "contrast_label": str(state.contrast_combo.currentText() or "").strip() or None,
        "mount_medium": _combo_value(state.mount_combo),
        "mount_label": str(state.mount_combo.currentText() or "").strip() or None,
        "stain": _combo_value(state.stain_combo),
        "stain_label": str(state.stain_combo.currentText() or "").strip() or None,
        "sample_type": _combo_value(state.sample_combo),
        "sample_label": str(state.sample_combo.currentText() or "").strip() or None,
    }
    state._scientific_name_text = lambda observation=None: live_lab_tab.LiveLabTab._scientific_name_text(state, observation)
    state._vernacular_name_text = lambda observation=None: live_lab_tab.LiveLabTab._vernacular_name_text(state, observation)
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
        == "Camera WB · Dark cutoff 0.00% · Bright cutoff 0.00% · Soft tails off · Curve strength 50% · Curve midpoint 50%"
    )
    assert state.raw_controls.curve_strength_row.isEnabled() is True
    assert state.raw_controls.curve_midpoint_row.isEnabled() is True
    assert state.raw_controls.curve_strength_slider.isEnabled() is False
    assert state.raw_controls.curve_midpoint_slider.isEnabled() is False

    state.raw_controls.curve_strength_slider.setValue(45)
    state.raw_controls.curve_midpoint_slider.setValue(48)
    state.raw_controls.tone_curve_checkbox.setChecked(True)
    live_lab_tab.LiveLabTab._on_raw_processing_controls_changed(state)

    assert live_lab_tab.LiveLabTab._raw_processing_summary_text(state) == "Camera WB · Auto levels · Curve 45 / mid 48"
    assert state.raw_controls.curve_strength_row.isEnabled() is True
    assert state.raw_controls.curve_midpoint_row.isEnabled() is True
    assert state.raw_controls.curve_strength_slider.isEnabled() is True
    assert state.raw_controls.curve_midpoint_slider.isEnabled() is True
    assert saved_settings

    state.raw_controls.white_balance_selector.set_selected_value("auto")
    state.raw_controls.auto_levels_checkbox.setChecked(False)
    live_lab_tab.LiveLabTab._on_raw_processing_controls_changed(state)

    assert live_lab_tab.LiveLabTab._raw_processing_summary_text(state) == "Auto WB · levels off · Curve 45 / mid 48"
    assert saved_settings[-1][0] == state._raw_processing_settings_key()
    assert json.loads(saved_settings[-1][1])["white_balance_mode"] == "auto"


def test_live_lab_raw_sync_applies_auto_level_slider_values(monkeypatch):
    _qapp()
    state = _build_raw_controls_state()
    monkeypatch.setattr(live_lab_tab.SettingsDB, "set_setting", lambda *args, **kwargs: None)

    visible_settings = RawRenderSettings(
        white_balance_mode="camera",
        auto_levels=True,
        light_ev=0.125,
        dark_ev=-0.031,
    )
    auto_settings = RawRenderSettings(
        white_balance_mode="camera",
        auto_levels=True,
        light_ev=0.357,
        dark_ev=-0.143,
    )

    live_lab_tab.LiveLabTab._sync_raw_processing_controls_from_settings(
        state,
        visible_settings,
        auto_level_settings=auto_settings,
    )

    assert state.raw_controls.auto_levels_checkbox.isChecked() is True
    assert state.raw_controls.light_slider.value() == 357
    assert state.raw_controls.dark_slider.value() == 143
    assert state.raw_controls.light_value_label.text() == "0.357"
    assert state.raw_controls.dark_value_label.text() == "0.143"
    round_tripped = state.raw_controls.settings()
    assert round_tripped.light_ev == pytest.approx(0.0)
    assert round_tripped.dark_ev == pytest.approx(0.0)
    assert round_tripped.exposure_ev == pytest.approx(0.0)
    assert state.raw_controls._auto_level_settings is not None
    assert state.raw_controls._auto_level_settings.light_ev == pytest.approx(0.357)
    assert state.raw_controls._auto_level_settings.dark_ev == pytest.approx(-0.143)


def test_live_lab_raw_processing_prefers_visible_image_over_stale_thumbnail_selection(tmp_path, monkeypatch):
    _qapp()
    old_path = tmp_path / "imports" / "old.jpg"
    new_path = tmp_path / "imports" / "new.jpg"
    old_path.parent.mkdir(parents=True, exist_ok=True)
    old_path.write_text("old", encoding="utf-8")
    new_path.write_text("new", encoding="utf-8")

    state = _build_raw_controls_state()
    state._session_image_ids = [101, 102]
    state._selected_session_image_id = 101
    state.session_gallery.select_image(101)
    state.live_image_label._full_image_path = str(new_path)
    state._refresh_session_gallery = lambda: None
    state._update_pending_raw_controls = lambda: None
    state._update_raw_edit_controls = lambda: None
    state._update_raw_processing_section_label = lambda *args, **kwargs: None
    state._refresh_raw_processing_context_ui = lambda: None
    state._set_raw_tone_controls_enabled = lambda enabled: None

    images = {
        101: {
            "id": 101,
            "filepath": str(old_path),
            "image_type": "microscope",
            "lab_metadata": {"image_type": "microscope"},
        },
        102: {
            "id": 102,
            "filepath": str(new_path),
            "image_type": "microscope",
            "lab_metadata": {"image_type": "microscope"},
        },
    }
    monkeypatch.setattr(
        live_lab_tab.ImageDB,
        "get_image",
        lambda image_id: copy.deepcopy(images.get(int(image_id))) if int(image_id) in images else None,
    )

    def fake_raw_editable_image_session(image, settings=None):
        image_id = int(image.get("id") or 0)
        return live_lab_tab.RawEditSession(
            image_id=image_id,
            source_raw_path=Path(image["filepath"]),
            current_derivative_path=Path(image["filepath"]),
            original_settings=RawRenderSettings.default(),
            working_settings=RawRenderSettings.default(),
            image_lab_metadata=copy.deepcopy(image.get("lab_metadata") or {}),
        )

    committed_ids: list[int] = []
    state._raw_editable_image_session = fake_raw_editable_image_session
    state._commit_raw_edit_session = lambda session: committed_ids.append(int(session.image_id)) or True
    monkeypatch.setattr(live_lab_tab.SettingsDB, "set_setting", lambda *args, **kwargs: None)

    state.raw_controls.set_settings(
        RawRenderSettings(
            tone_curve_enabled=True,
            auto_levels=False,
            tone_curve_strength=0.8,
            tone_curve_midpoint=0.4,
        )
    )

    live_lab_tab.LiveLabTab._on_raw_processing_controls_changed(state)

    assert committed_ids == [102]


def test_live_lab_objective_state_prefers_nested_lab_metadata(monkeypatch):
    _qapp()
    state = _build_raw_controls_state()

    monkeypatch.setattr(
        live_lab_tab,
        "load_objectives",
        lambda: {
            "40x": {
                "magnification": 40.0,
                "objective_name": "Plan Apo",
                "name": "40X Plan Apo",
            }
        },
    )
    monkeypatch.setattr(
        live_lab_tab,
        "resolve_objective_key",
        lambda name, objectives: "40x" if "40" in str(name) else None,
    )

    metadata = {
        "lab_metadata": {
            "objective_label": "40x / 0.95 Plan Apo",
            "contrast": "phase",
        }
    }

    state_data = live_lab_tab.LiveLabTab._microscope_state_from_metadata(state, metadata)
    tag_text, tag_color = live_lab_tab.LiveLabTab._microscope_tag_for_metadata(state, metadata)

    assert state_data["objective_name"] == "40x"
    assert tag_text == "40x"
    assert tag_color == "#3498db"


def test_live_lab_viewer_objective_tag_tracks_current_controls(monkeypatch):
    _qapp()
    state = _build_raw_controls_state()
    state.SETTING_LAST_OBJECTIVE = live_lab_tab.LiveLabTab.SETTING_LAST_OBJECTIVE
    state.objective_combo.addItem("40x", "40x")
    state.objective_combo.setCurrentIndex(1)
    state._current_lab_metadata = lambda: {
        "image_type": "microscope",
        "objective_name": state.objective_combo.currentData(),
        "objective_label": state.objective_combo.currentText(),
    }
    monkeypatch.setattr(live_lab_tab.SettingsDB, "set_setting", lambda *args, **kwargs: None)

    captured: dict[str, object] = {}
    state.live_image_label = SimpleNamespace(
        set_objective_text=lambda text: captured.__setitem__("text", text),
        set_objective_color=lambda color: captured.__setitem__("color", QColor(color).name()),
    )

    live_lab_tab.LiveLabTab._save_objective_selection(state)

    assert captured["text"] == "40x"


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

    state.raw_controls.white_balance_selector.set_selected_value("camera")
    state.raw_controls.auto_levels_checkbox.setChecked(True)
    state.raw_controls.tone_curve_checkbox.setChecked(False)
    state.raw_controls.curve_strength_slider.setValue(45)
    state.raw_controls.curve_midpoint_slider.setValue(48)

    assert live_lab_tab.LiveLabTab._ingest_detected_image(state, str(source_path))
    first_snapshot = add_image_calls[0]["lab_metadata"]["raw_processing"]["settings"]
    assert captured_calls[0]["raw_settings"]["white_balance_mode"] == "camera"
    assert captured_calls[0]["raw_settings"]["auto_levels"] is True
    assert first_snapshot["white_balance_mode"] == "camera"
    assert first_snapshot["auto_levels"] is True

    state.raw_controls.white_balance_selector.set_selected_value("auto")
    state.raw_controls.auto_levels_checkbox.setChecked(False)
    state.raw_controls.tone_curve_checkbox.setChecked(True)
    state.raw_controls.curve_strength_slider.setValue(70)
    state.raw_controls.curve_midpoint_slider.setValue(35)

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


def test_live_lab_raw_controls_follow_visible_image_when_gallery_selection_is_stale(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")
    old_derivative_path = tmp_path / "imports" / "sample_101.jpg"
    old_derivative_path.parent.mkdir(parents=True, exist_ok=True)
    old_derivative_path.write_text("old", encoding="utf-8")
    new_derivative_path = tmp_path / "imports" / "sample_102.jpg"
    new_derivative_path.write_text("new", encoding="utf-8")

    state = _build_raw_controls_state()
    state.session_gallery.set_multi_select(True)
    state._session_observation_id = 1
    state._session_image_ids = [101]
    state._selected_session_image_id = 101
    state._session_import_count = 1
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
    state._save_raw_processing_settings_for_current_context = lambda settings=None: None

    images = {
        101: _make_raw_image_row(101, source_path=source_path, derivative_path=old_derivative_path),
    }

    def fake_get_image(image_id):
        image = images.get(int(image_id))
        return copy.deepcopy(image) if image is not None else None

    def fake_prepare_local_ingest_image(source, *, raw_settings=None, lab_metadata=None, output_dir=None):
        snapshot = raw_settings.to_dict() if isinstance(raw_settings, RawRenderSettings) else None
        return SimpleNamespace(
            source_path=str(source),
            working_path=str(new_derivative_path),
            original_path=str(source),
            raw_render_snapshot={"settings": snapshot} if snapshot is not None else None,
            lab_metadata={
                **dict(lab_metadata or {}),
                "raw_processing": {
                    "engine": "rawpy",
                    "source": {
                        "kind": "camera_raw",
                        "path": str(source),
                        "mime_type": "image/x-raw",
                    },
                    "local_derivative": {
                        "kind": "rendered_from_raw",
                        "path": str(new_derivative_path),
                        "mime_type": "image/jpeg",
                        "format": "jpeg",
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
        new_image_id = 102
        images[new_image_id] = _make_raw_image_row(
            new_image_id,
            source_path=source_path,
            derivative_path=new_derivative_path,
        )
        return new_image_id

    monkeypatch.setattr(live_lab_tab.ImageDB, "get_image", fake_get_image)
    monkeypatch.setattr(live_lab_tab, "prepare_local_ingest_image", fake_prepare_local_ingest_image)
    monkeypatch.setattr(live_lab_tab.ImageDB, "add_image", fake_add_image)
    monkeypatch.setattr(live_lab_tab, "generate_all_sizes", lambda *args, **kwargs: None)
    monkeypatch.setattr(live_lab_tab, "cleanup_import_temp_file", lambda *args, **kwargs: None)

    state._refresh_session_gallery()
    assert state.session_gallery.selected == 101
    assert [target[1] for target in state._selected_raw_processing_targets()] == [101]

    assert live_lab_tab.LiveLabTab._ingest_detected_image(state, str(source_path))
    assert state._session_image_ids == [101, 102]
    assert state._selected_session_image_id == 102
    assert state.session_gallery.selected == 101

    selected_targets = state._selected_raw_processing_targets()
    assert len(selected_targets) == 1
    assert selected_targets[0][0] == "image"
    assert int(selected_targets[0][1]) == 102


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
    assert state.raw_controls.white_balance_selector.selected_value("camera") == "auto"
    assert state.raw_controls.auto_levels_checkbox.isChecked() is False
    assert state.raw_controls.tone_curve_checkbox.isChecked() is True
    assert state.raw_controls.curve_strength_slider.value() == 62
    assert state.raw_controls.curve_midpoint_slider.value() == 31


def test_live_lab_raw_controls_round_trip_through_shared_widget(monkeypatch):
    _qapp()
    state = _build_raw_controls_state()
    state._raw_processing_preset_context = lambda: {}
    settings = RawRenderSettings(
        white_balance_mode="custom",
        wb_multipliers=(1.2, 1.0, 1.4),
        wb_selection=(5.0, 6.0, 7.0, 8.0),
        wb_multiplier_space="post_decode_rgb",
        wb_sample_point=(6.0, 7.0),
        wb_sample_size=8,
        wb_sample_base_mode="camera",
        wb_selection_space="preview_pixels",
        auto_levels=False,
        tone_curve_enabled=True,
        tone_curve_strength=0.63,
        tone_curve_midpoint=0.29,
    )

    live_lab_tab.LiveLabTab._sync_raw_processing_controls_from_settings(state, settings)
    round_tripped = live_lab_tab.LiveLabTab._raw_settings_from_controls(state, update_session_settings=False)

    assert round_tripped == settings


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

    state.raw_controls.white_balance_selector.set_selected_value("auto")
    state.raw_controls.auto_levels_checkbox.setChecked(False)
    state.raw_controls.tone_curve_checkbox.setChecked(True)
    state.raw_controls.curve_strength_slider.setValue(70)
    state.raw_controls.curve_midpoint_slider.setValue(35)

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
    state._invalidate_thumbnail_caches_for_raw_image = lambda image_id, final_path: live_lab_tab.LiveLabTab._invalidate_thumbnail_caches_for_raw_image(
        state,
        image_id,
        final_path,
    )
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

    state.raw_controls.white_balance_selector.set_selected_value("auto")
    state.raw_controls.auto_levels_checkbox.setChecked(False)
    state.raw_controls.tone_curve_checkbox.setChecked(True)
    state.raw_controls.curve_strength_slider.setValue(70)
    state.raw_controls.curve_midpoint_slider.setValue(35)
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

    state.raw_controls.white_balance_selector.set_selected_value("auto")
    state.raw_controls.auto_levels_checkbox.setChecked(False)
    state.raw_controls.tone_curve_checkbox.setChecked(True)
    state.raw_controls.curve_strength_slider.setValue(70)
    state.raw_controls.curve_midpoint_slider.setValue(35)
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


def test_live_lab_committed_raw_edit_refreshes_thumbnail_caches_after_apply(tmp_path, monkeypatch):
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
    state._invalidate_thumbnail_caches_for_raw_image = lambda image_id, final_path: live_lab_tab.LiveLabTab._invalidate_thumbnail_caches_for_raw_image(
        state,
        image_id,
        final_path,
    )
    thumb_updates: list[bool] = []
    state._update_observation_thumbnail = lambda: thumb_updates.append(True)
    state._log_session_event = lambda *args, **kwargs: None
    state._current_lab_metadata = lambda: {"image_type": "microscope", "contrast": "phase"}
    state._load_viewer_pixmap = lambda path: (QPixmap(12, 8), False)

    main_invalidations: list[str | None] = []
    state._main_window = SimpleNamespace(
        invalidate_pixmap_cache=lambda path=None: main_invalidations.append(str(path) if path is not None else None)
    )

    images = {101: _make_raw_image_row(101, source_path=source_path, derivative_path=derivative_path)}
    generate_calls: list[tuple[str, int]] = []

    monkeypatch.setattr(live_lab_tab.ImageDB, "get_image", lambda image_id: copy.deepcopy(images.get(int(image_id))) if images.get(int(image_id)) is not None else None)
    monkeypatch.setattr(live_lab_tab.ImageDB, "update_image", lambda *args, **kwargs: None)
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)
    monkeypatch.setattr(live_lab_tab, "render_raw_image", _fake_render_raw_jpeg)
    monkeypatch.setattr(live_lab_tab, "generate_all_sizes", lambda filepath, image_id: generate_calls.append((str(filepath), int(image_id))) or {})
    monkeypatch.setattr(live_lab_tab.SettingsDB, "set_setting", lambda *args, **kwargs: None)

    assert live_lab_tab.LiveLabTab._begin_raw_edit_for_selected_image(state) is True
    assert live_lab_tab.LiveLabTab._apply_raw_edit_session(state) is True

    thumb_path = live_lab_tab.get_thumbnail_path(101, "small")
    assert generate_calls == [(str(derivative_path), 101)]
    assert state.session_gallery.invalidated_paths == [str(thumb_path)]
    assert main_invalidations == [str(derivative_path)]
    assert thumb_updates == [True]


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
    state._invalidate_thumbnail_caches_for_raw_image = lambda image_id, final_path: live_lab_tab.LiveLabTab._invalidate_thumbnail_caches_for_raw_image(
        state,
        image_id,
        final_path,
    )
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
    state.raw_controls.white_balance_selector.set_selected_value("auto")
    state.raw_controls.auto_levels_checkbox.setChecked(False)
    live_lab_tab.LiveLabTab._on_raw_processing_controls_changed(state)
    assert state._raw_edit_session.dirty is True

    live_lab_tab.LiveLabTab._cancel_raw_edit_session(state)

    assert state._raw_edit_session is None
    assert not preview_path.exists()
    assert state._raw_render_settings == RawRenderSettings.default()
    assert state.raw_controls.white_balance_selector.selected_value("camera") == "camera"
    assert state.raw_controls.auto_levels_checkbox.isChecked() is True
    assert state.raw_controls.tone_curve_checkbox.isChecked() is False
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
    assert tab.pending_raw_save_all_btn.text() == "Save all"
    assert tab.pending_raw_apply_all_btn.text() == "Apply settings to all pending"
    assert not hasattr(tab, "pending_raw_pick_wb_btn")
    assert tab.pending_raw_shortcuts_label.text() == "←/→ select · Delete/Backspace/Cmd/Ctrl+D remove current image · Enter save current"
    assert tab.session_gallery._items[0]["id"].startswith("pending:")
    assert tab.session_gallery._items[0]["badges"][0] == "UNSAVED RAW"
    assert tab.session_gallery._items[0]["frame_border_color"] == "#e67e22"
    assert tab.session_gallery._selected_id == tab.session_gallery._items[0]["id"]


def _build_offscreen_live_lab_tab(monkeypatch):
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
    monkeypatch.setattr(live_lab_tab.SettingsDB, "get_setting", lambda key, default=None: default)
    monkeypatch.setattr(live_lab_tab.SettingsDB, "set_setting", lambda *args, **kwargs: None)
    return live_lab_tab.LiveLabTab(SimpleNamespace())


def test_live_lab_instantiates_without_committed_raw_edit_panel(monkeypatch, qapp):
    tab = _build_offscreen_live_lab_tab(monkeypatch)
    tab.show()
    qapp.processEvents()

    # The committed RAW re-render / edit panel and its buttons are gone.
    assert not hasattr(tab, "raw_edit_frame")
    assert not hasattr(tab, "raw_edit_open_btn")
    assert not hasattr(tab, "raw_edit_apply_btn")
    assert not hasattr(tab, "raw_edit_summary_label")
    assert not hasattr(tab, "raw_edit_note_label")

    # Pending RAW review controls remain available.
    assert tab.pending_raw_save_btn.text() == "Save current"
    assert tab.pending_raw_apply_all_btn.text() == "Apply settings to all pending"


def test_live_lab_selecting_committed_raw_image_does_not_show_edit_panel(tmp_path, monkeypatch, qapp):
    tab = _build_offscreen_live_lab_tab(monkeypatch)
    tab.show()
    qapp.processEvents()

    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")
    derivative_path = tmp_path / "imports" / "sample.jpg"
    derivative_path.parent.mkdir(parents=True, exist_ok=True)
    _fake_render_raw_jpeg(source_path, output_path=derivative_path)

    images = {101: _make_raw_image_row(101, source_path=source_path, derivative_path=derivative_path)}
    monkeypatch.setattr(
        live_lab_tab.ImageDB,
        "get_image",
        lambda image_id: copy.deepcopy(images.get(int(image_id))) if images.get(int(image_id)) is not None else None,
    )

    tab._session_image_ids = [101]
    tab._selected_session_image_id = 101

    # Selecting a committed RAW-backed image must not crash or surface an edit panel.
    tab._update_raw_edit_controls()
    tab._show_session_image(101)
    qapp.processEvents()

    assert not hasattr(tab, "raw_edit_frame")
    assert not hasattr(tab, "raw_edit_open_btn")
    assert not hasattr(tab, "raw_edit_apply_btn")
    # The committed RAW image still displays normally.
    assert tab.live_image_label is not None


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


def test_live_lab_microscope_pill_rows_clear_alert_for_real_first_values():
    qapp = _qapp()
    state = SimpleNamespace()
    state.objective_combo = AdaptiveChoiceSelector(compact=True)
    state.objective_combo.addItem("4x", "4x", pillText="4x")
    state.objective_combo.addItem("10x", "10x", pillText="10x")
    state.contrast_combo = AdaptiveChoiceSelector(compact=True)
    state.contrast_combo.addItem("BF", "BF", pillText="BF")
    state.contrast_combo.addItem("DIC", "DIC", pillText="DIC")
    objective_container = _show_selector_in_pill_mode(qapp, state.objective_combo)
    contrast_container = _show_selector_in_pill_mode(qapp, state.contrast_combo)
    state._update_lab_state_combo_alerts = lambda *_args: live_lab_tab.LiveLabTab._update_lab_state_combo_alerts(state, *_args)

    try:
        objective_10x = state.objective_combo.button_for_value("10x")
        objective_4x = state.objective_combo.button_for_value("4x")
        contrast_dic = state.contrast_combo.button_for_value("DIC")
        contrast_bf = state.contrast_combo.button_for_value("BF")
        assert objective_10x is not None
        assert objective_4x is not None
        assert contrast_dic is not None
        assert contrast_bf is not None

        state._update_lab_state_combo_alerts()
        assert state.objective_combo.property("labStateAlert") is False
        assert state.objective_combo.combo().property("labStateAlert") is False
        assert state.contrast_combo.property("labStateAlert") is False
        assert state.contrast_combo.combo().property("labStateAlert") is False

        _click_and_settle(qapp, objective_10x)
        _click_and_settle(qapp, objective_4x)
        state._update_lab_state_combo_alerts()
        assert state.objective_combo.currentIndex() == 0
        assert state.objective_combo.currentData() == "4x"
        assert live_lab_tab.LiveLabTab._combo_is_unset(state.objective_combo) is False
        assert state.objective_combo.property("labStateAlert") is False
        assert state.objective_combo.combo().property("labStateAlert") is False

        _click_and_settle(qapp, contrast_dic)
        _click_and_settle(qapp, contrast_bf)
        state._update_lab_state_combo_alerts()
        assert state.contrast_combo.currentIndex() == 0
        assert state.contrast_combo.currentData() == "BF"
        assert live_lab_tab.LiveLabTab._combo_is_unset(state.contrast_combo) is False
        assert state.contrast_combo.property("labStateAlert") is False
        assert state.contrast_combo.combo().property("labStateAlert") is False
    finally:
        objective_container.close()
        contrast_container.close()


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
    assert state.raw_controls.white_balance_selector.selected_value("camera") == "custom"
    assert state.viewer_meta_label.text().startswith("Custom WB")
    assert "Dark cutoff" in state.viewer_meta_label.text()
    assert "Bright cutoff" in state.viewer_meta_label.text()
    assert "Dark boost" not in state.viewer_meta_label.text()
    assert "Soft tails" in state.viewer_meta_label.text()
    assert "Curve strength" in state.viewer_meta_label.text()
    assert "Curve midpoint" in state.viewer_meta_label.text()
    assert state.pending_raw_shortcuts_label.text().startswith("Custom WB")
    assert "Enter save current" in state.pending_raw_shortcuts_label.text()
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
    assert state.pending_raw_shortcuts_label.text() == "←/→ select · Delete/Backspace/Cmd/Ctrl+D remove current image · Enter save current"
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
    assert "Dark boost" not in state.raw_edit_summary_label.text()
    assert "Soft tails" in state.raw_edit_summary_label.text()
    assert "Curve strength" in state.raw_edit_summary_label.text()
    assert "Curve midpoint" in state.raw_edit_summary_label.text()
    assert state.raw_edit_pick_wb_btn.isChecked() is False

    state.raw_controls.tone_curve_checkbox.setChecked(True)
    state.raw_controls.curve_strength_slider.setValue(70)
    state.raw_controls.curve_midpoint_slider.setValue(35)
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
    def fake_get_image(image_id):
        if not add_image_calls:
            return None
        try:
            resolved_image_id = int(image_id)
        except Exception:
            return None
        if resolved_image_id != 1:
            return None
        filepath = str(add_image_calls[0].get("filepath") or tmp_path / "imports" / "P070020_1.jpg")
        return {
            "id": resolved_image_id,
            "filepath": filepath,
            "image_type": "microscope",
            "objective_name": None,
            "contrast": "phase",
            "mount_medium": "water",
            "stain": "none",
            "sample_type": "spore",
            "scale_microns_per_pixel": 1.2,
            "lab_metadata": {
                "image_type": "microscope",
                "raw_processing": {
                    "source": {
                        "kind": "camera_raw",
                        "path": str(source_path),
                        "mime_type": "image/x-raw",
                    },
                },
            },
        }

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
    monkeypatch.setattr(live_lab_tab.ImageDB, "get_image", fake_get_image)

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
    state._raw_processing_preset_context = lambda: {}
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._refresh_session_gallery = lambda: live_lab_tab.LiveLabTab._refresh_session_gallery(state)
    state._show_session_image = lambda image_id: None
    state._update_session_controls = lambda: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._update_observation_thumbnail = lambda: None
    state._log_session_event = lambda *args, **kwargs: None
    monkeypatch.setattr(
        live_lab_tab,
        "load_objectives",
        lambda: {
            "10x": {"magnification": 10.0, "name": "10x"},
            "63x": {"magnification": 63.0, "name": "63x"},
        },
    )
    monkeypatch.setattr(
        live_lab_tab,
        "resolve_objective_key",
        lambda name, objectives: str(name) if str(name) in objectives else None,
    )

    for combo, values in (
        (state.objective_combo, [("10x", "10x"), ("63x", "63x")]),
        (state.contrast_combo, [("BF", "BF"), ("DIC", "DIC")]),
        (state.mount_combo, [("Water", "Water"), ("KOH", "KOH")]),
        (state.stain_combo, [("Not set", "Not_set"), ("Congo Red", "Congo_Red")]),
        (state.sample_combo, [("Not set", "Not_set"), ("Dried", "Dried")]),
    ):
        combo.clear()
        for label, value in values:
            combo.addItem(label, value)

    first_metadata = {
        "image_type": "microscope",
        "objective_name": "10x",
        "objective_label": "10x",
        "contrast": "BF",
        "contrast_label": "BF",
        "mount_medium": "Water",
        "mount_label": "Water",
        "stain": "Not_set",
        "stain_label": "Not set",
        "sample_type": "Not_set",
        "sample_label": "Not set",
    }
    second_metadata = {
        "image_type": "microscope",
        "objective_name": "63x",
        "objective_label": "63x",
        "contrast": "DIC",
        "contrast_label": "DIC",
        "mount_medium": "KOH",
        "mount_label": "KOH",
        "stain": "Congo_Red",
        "stain_label": "Congo Red",
        "sample_type": "Dried",
        "sample_label": "Dried",
    }

    state._current_lab_metadata = lambda: first_metadata
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
    state._current_lab_metadata = lambda: second_metadata
    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(state, str(source_two), group_key="group-2", state={})

    assert len(state.session_gallery.items) == 2
    assert state.session_gallery.selected == f"pending:{source_two}"
    assert state.viewer_title_label.text().startswith("Pending RAW 2 of 2")
    assert state.session_gallery.items[0]["microscope_tag_text"] == "10x BF"
    assert state.session_gallery.items[1]["microscope_tag_text"] == "63x DIC"
    assert state.objective_combo.currentData() == "63x"
    assert state.contrast_combo.currentData() == "DIC"
    assert state.mount_combo.currentData() == "KOH"
    assert state.stain_combo.currentData() == "Congo_Red"
    assert state.sample_combo.currentData() == "Dried"

    first_item = state.session_gallery.items[0]
    live_lab_tab.LiveLabTab._on_pending_raw_gallery_clicked(state, first_item["id"], first_item["filepath"])

    assert state._selected_pending_raw_index == 0
    assert state.session_gallery.selected == f"pending:{source_one}"
    assert state.viewer_title_label.text().startswith("Pending RAW 1 of 2")
    assert state.objective_combo.currentData() == "10x"
    assert state.contrast_combo.currentData() == "BF"
    assert state.mount_combo.currentData() == "Water"
    assert state.stain_combo.currentData() == "Not_set"
    assert state.sample_combo.currentData() == "Not_set"


def test_live_lab_review_mode_multi_select_preserves_selection_and_applies_microscope_changes_to_selected_pending_captures(tmp_path, monkeypatch):
    _qapp()
    source_one = tmp_path / "P070020_1.ORF"
    source_two = tmp_path / "P070021_1.ORF"
    source_three = tmp_path / "P070022_1.ORF"
    source_one.write_bytes(b"raw-1")
    source_two.write_bytes(b"raw-2")
    source_three.write_bytes(b"raw-3")

    preview_one = tmp_path / "previews" / "P070020_1_preview.jpg"
    preview_two = tmp_path / "previews" / "P070021_1_preview.jpg"
    preview_three = tmp_path / "previews" / "P070022_1_preview.jpg"
    preview_one.parent.mkdir(parents=True, exist_ok=True)
    preview_one.write_text("preview-1", encoding="utf-8")
    preview_two.write_text("preview-2", encoding="utf-8")
    preview_three.write_text("preview-3", encoding="utf-8")

    state = _build_raw_controls_state()
    state._session_observation_id = 1
    state._session_image_ids = []
    state._selected_session_image_id = None
    state._session_import_count = 0
    state._session_id = "session-1"
    state._active_session_mode = live_lab_tab.LiveLabTab.SESSION_MODE_LIVE
    state._pending_stop_status = None
    state._watcher = None
    state._pending_raw_captures = [
        live_lab_tab.PendingRawCapture(
            source_path=source_one,
            companion_jpeg_path=None,
            lab_metadata={
                "image_type": "microscope",
                "objective_name": "4x",
                "objective_label": "4x",
                "contrast": "BF",
                "contrast_label": "BF",
                "mount_medium": "Water",
                "mount_label": "Water",
                "stain": "Not_set",
                "stain_label": "Not set",
                "sample_type": "Not_set",
                "sample_label": "Not set",
            },
            raw_settings=RawRenderSettings.default(),
            preview_path=preview_one,
            group_key="group-1",
        ),
        live_lab_tab.PendingRawCapture(
            source_path=source_two,
            companion_jpeg_path=None,
            lab_metadata={
                "image_type": "microscope",
                "objective_name": "40x",
                "objective_label": "40x",
                "contrast": "DIC",
                "contrast_label": "DIC",
                "mount_medium": "KOH",
                "mount_label": "KOH",
                "stain": "Congo_Red",
                "stain_label": "Congo Red",
                "sample_type": "Dried",
                "sample_label": "Dried",
            },
            raw_settings=RawRenderSettings.default(),
            preview_path=preview_two,
            group_key="group-2",
        ),
        live_lab_tab.PendingRawCapture(
            source_path=source_three,
            companion_jpeg_path=None,
            lab_metadata={
                "image_type": "microscope",
                "objective_name": "63x",
                "objective_label": "63x",
                "contrast": "Phase",
                "contrast_label": "Phase",
                "mount_medium": "Water",
                "mount_label": "Water",
                "stain": "Not_set",
                "stain_label": "Not set",
                "sample_type": "Not_set",
                "sample_label": "Not set",
            },
            raw_settings=RawRenderSettings.default(),
            preview_path=preview_three,
            group_key="group-3",
        ),
    ]
    state._selected_pending_raw_index = 0
    state._pending_companion_groups = {
        "group-1": {"paths": {"ignored-1"}, "timer": None},
        "group-2": {"paths": {"ignored-2"}, "timer": None},
        "group-3": {"paths": {"ignored-3"}, "timer": None},
    }
    state._consumed_companion_groups = {"group-1", "group-2", "group-3"}
    state._pending_raw_preview_timer = SimpleNamespace(stop=lambda: None, start=lambda _ms: None)
    state._cancel_raw_edit_session = lambda restore_selection=False: None
    state._log_session_event = lambda *args, **kwargs: None
    state._update_session_controls = lambda: None
    state._save_raw_processing_settings_for_current_context = lambda settings=None: None
    state._refresh_session_gallery = lambda: live_lab_tab.LiveLabTab._refresh_session_gallery(state)
    state._show_session_image = lambda image_id: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._update_observation_thumbnail = lambda: None
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._current_lab_metadata = lambda: {
        "image_type": "microscope",
        "objective_name": state.objective_combo.currentData(),
        "objective_label": str(state.objective_combo.currentText() or "").strip() or None,
        "contrast": state.contrast_combo.currentData(),
        "contrast_label": str(state.contrast_combo.currentText() or "").strip() or None,
        "mount_medium": state.mount_combo.currentData(),
        "mount_label": str(state.mount_combo.currentText() or "").strip() or None,
        "stain": state.stain_combo.currentData(),
        "stain_label": str(state.stain_combo.currentText() or "").strip() or None,
        "sample_type": state.sample_combo.currentData(),
        "sample_label": str(state.sample_combo.currentText() or "").strip() or None,
    }

    for combo, values in (
        (state.objective_combo, [("4x", "4x"), ("100x", "100x"), ("40x", "40x"), ("63x", "63x")]),
        (state.contrast_combo, [("BF", "BF"), ("DIC", "DIC"), ("Phase", "Phase")]),
        (state.mount_combo, [("Water", "Water"), ("KOH", "KOH")]),
        (state.stain_combo, [("Not set", "Not_set"), ("Congo Red", "Congo_Red")]),
        (state.sample_combo, [("Not set", "Not_set"), ("Dried", "Dried")]),
    ):
        combo.clear()
        for label, value in values:
            combo.addItem(label, value)

    state.session_gallery.set_multi_select(True)
    state._refresh_session_gallery()
    state.session_gallery.select_paths([str(source_one), str(source_two)])

    expected_selected_keys = {
        f"pending:{source_one}",
        f"pending:{source_two}",
    }
    assert state.session_gallery.selected_keys() == expected_selected_keys

    live_lab_tab.LiveLabTab._show_pending_raw_capture(state, 0)
    assert state.session_gallery.selected_keys() == expected_selected_keys
    assert {capture.source_path for capture in state._selected_pending_raw_captures()} == {source_one, source_two}
    assert state.live_image_label.objective_text == "4x BF"

    state.objective_combo.setCurrentIndex(1)
    state.contrast_combo.setCurrentIndex(1)
    assert live_lab_tab.LiveLabTab._sync_selected_pending_raw_metadata_from_controls(state) is True

    assert state.session_gallery.selected_keys() == expected_selected_keys
    assert {capture.source_path for capture in state._selected_pending_raw_captures()} == {source_one, source_two}

    first_pending, second_pending, third_pending = state._pending_raw_captures
    assert first_pending.lab_metadata["objective_name"] == "100x"
    assert first_pending.lab_metadata["contrast"] == "DIC"
    assert first_pending.lab_metadata["mount_medium"] == "Water"
    assert first_pending.lab_metadata["stain"] == "Not_set"
    assert first_pending.lab_metadata["sample_type"] == "Not_set"
    assert second_pending.lab_metadata["objective_name"] == "100x"
    assert second_pending.lab_metadata["contrast"] == "DIC"
    assert second_pending.lab_metadata["mount_medium"] == "Water"
    assert second_pending.lab_metadata["stain"] == "Not_set"
    assert second_pending.lab_metadata["sample_type"] == "Not_set"
    assert third_pending.lab_metadata["objective_name"] == "63x"
    assert third_pending.lab_metadata["contrast"] == "Phase"
    assert third_pending.lab_metadata["mount_medium"] == "Water"
    assert third_pending.lab_metadata["stain"] == "Not_set"
    assert third_pending.lab_metadata["sample_type"] == "Not_set"
    assert state.session_gallery.items[0]["microscope_tag_text"] == "100x DIC"
    assert state.session_gallery.items[1]["microscope_tag_text"] == "100x DIC"
    assert state.session_gallery.items[2]["microscope_tag_text"] == "63x Phase"
    assert state.session_gallery.items[0]["microscope_tag_color"] == "#f7f1e5"
    assert state.live_image_label.objective_text == "100x DIC"
    assert state.live_image_label.objective_color == "#f7f1e5"


def test_live_lab_finalize_session_stop_preserves_pending_raw_gallery_and_preview(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "P070020_1.ORF"
    source_path.write_bytes(b"raw-bytes")

    preview_path = tmp_path / "previews" / "P070020_1_preview.jpg"
    preview_path.parent.mkdir(parents=True, exist_ok=True)
    preview_path.write_text("preview", encoding="utf-8")

    state = _build_raw_controls_state()
    state._session_active = True
    state._session_observation_id = 1
    state._session_id = "session-1"
    state._session_import_count = 1
    state._active_session_mode = live_lab_tab.LiveLabTab.SESSION_MODE_LIVE
    state._pending_stop_status = None
    state._watcher = None
    state._session_image_ids = []
    state._selected_session_image_id = None
    state._pending_raw_captures = [
        live_lab_tab.PendingRawCapture(
            source_path=source_path,
            companion_jpeg_path=None,
            lab_metadata={"image_type": "microscope"},
            raw_settings=RawRenderSettings.default(),
            preview_path=preview_path,
            group_key="group-1",
        )
    ]
    state._selected_pending_raw_index = 0
    state._pending_companion_groups = {"group-1": {"paths": {"ignored"}, "timer": None}}
    state._consumed_companion_groups = {"group-1"}
    state._pending_raw_preview_timer = SimpleNamespace(stop=lambda: setattr(state, "_preview_timer_stopped", True))
    state._cancel_raw_edit_session = lambda restore_selection=False: None
    state._log_session_event = lambda *args, **kwargs: None
    state._update_session_controls = lambda: None
    state._show_status = lambda *args, **kwargs: None

    live_lab_tab.LiveLabTab._finalize_session_stop(state)

    assert state._session_active is False
    assert len(state._pending_raw_captures) == 1
    assert state._pending_raw_captures[0].source_path == source_path
    assert state._pending_raw_captures[0].preview_path == preview_path
    assert preview_path.exists()
    assert state.session_gallery.visible is True
    assert state.session_gallery.items[0]["id"] == f"pending:{source_path}"
    assert state.session_gallery.items[0]["preview_path"] == str(preview_path)
    assert state.viewer_title_label.text().startswith("Pending RAW")


def test_live_lab_start_session_keeps_pending_raw_gallery_visible(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "P070020_1.ORF"
    source_path.write_bytes(b"raw-bytes")

    preview_path = tmp_path / "previews" / "P070020_1_preview.jpg"
    preview_path.parent.mkdir(parents=True, exist_ok=True)
    preview_path.write_text("preview", encoding="utf-8")

    state = _build_raw_controls_state()
    state._session_active = False
    state._target_observation_id = 1
    state._target_observation = {"id": 1, "name": "Observation 1"}
    state._session_image_ids = [101]
    state._selected_session_image_id = 101
    state._pending_raw_captures = [
        live_lab_tab.PendingRawCapture(
            source_path=source_path,
            companion_jpeg_path=None,
            lab_metadata={"image_type": "microscope"},
            raw_settings=RawRenderSettings.default(),
            preview_path=preview_path,
            group_key="group-1",
        )
    ]
    state._selected_pending_raw_index = 0
    state._seen_source_paths = set()
    state._pending_companion_groups = {}
    state._consumed_companion_groups = set()
    state._pending_raw_preview_timer = SimpleNamespace(stop=lambda: None, start=lambda _ms: None)
    state._cancel_raw_edit_session = lambda restore_selection=False: None
    state._selected_session_mode = lambda: live_lab_tab.LiveLabTab.SESSION_MODE_OFFLINE
    state._session_mode_label = lambda mode=None: live_lab_tab.LiveLabTab._session_mode_label(state, mode)
    state._observation_summary_text = lambda observation=None: live_lab_tab.LiveLabTab._observation_summary_text(state, observation)
    state.watch_dir_input = SimpleNamespace(text=lambda: "")
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None
    state._log_session_event = lambda *args, **kwargs: None
    state._log_initial_lab_state = lambda: None
    state._set_session_button_style = lambda *args, **kwargs: None
    state._update_tab_recording_indicator = lambda *args, **kwargs: None
    state._update_session_controls = lambda: None
    state.is_session_running = lambda: False
    state._pending_raw_capture_exists = lambda source_path, group_key=None: live_lab_tab.LiveLabTab._pending_raw_capture_exists(
        state,
        source_path,
        group_key=group_key,
    )

    live_lab_tab.LiveLabTab.start_session(state)

    assert state._session_active is True
    assert len(state._pending_raw_captures) == 1
    assert state.session_gallery.visible is True
    assert state.session_gallery.items[0]["id"] == f"pending:{source_path}"
    assert state.session_gallery.selected == f"pending:{source_path}"
    assert state.viewer_title_label.text().startswith("Pending RAW")
    assert state._queue_companion_source(str(source_path)) is False
    assert len(state._pending_raw_captures) == 1


def test_live_lab_pending_raw_controls_still_rerender_after_session_stop(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "P070020_1.ORF"
    source_path.write_bytes(b"raw-bytes")

    preview_path = tmp_path / "previews" / "P070020_1_preview.jpg"
    preview_path.parent.mkdir(parents=True, exist_ok=True)
    preview_path.write_text("preview", encoding="utf-8")

    state = _build_raw_controls_state()
    state._session_active = True
    state._session_observation_id = 1
    state._session_id = "session-1"
    state._session_import_count = 1
    state._active_session_mode = live_lab_tab.LiveLabTab.SESSION_MODE_LIVE
    state._pending_stop_status = None
    state._watcher = None
    state._session_image_ids = []
    state._selected_session_image_id = None
    state._pending_raw_captures = [
        live_lab_tab.PendingRawCapture(
            source_path=source_path,
            companion_jpeg_path=None,
            lab_metadata={"image_type": "microscope"},
            raw_settings=RawRenderSettings.default(),
            preview_path=preview_path,
            group_key="group-1",
        )
    ]
    state._selected_pending_raw_index = 0
    state._pending_companion_groups = {"group-1": {"paths": {"ignored"}, "timer": None}}
    state._consumed_companion_groups = {"group-1"}
    state._pending_raw_preview_timer = SimpleNamespace(stop=lambda: None, start=lambda _ms: None)
    state._cancel_raw_edit_session = lambda restore_selection=False: None
    state._log_session_event = lambda *args, **kwargs: None
    state._update_session_controls = lambda: None
    state._show_status = lambda *args, **kwargs: None
    state._save_raw_processing_settings_for_current_context = lambda settings=None: None
    state._schedule_pending_raw_preview_refresh = lambda: live_lab_tab.LiveLabTab._refresh_selected_pending_raw_preview(state)
    state._raw_capture_mode = live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)

    live_lab_tab.LiveLabTab._finalize_session_stop(state)
    state.raw_controls.tone_curve_checkbox.setChecked(True)
    state.raw_controls.curve_strength_slider.setValue(80)
    live_lab_tab.LiveLabTab._on_raw_processing_controls_changed(state)

    preview_payload = json.loads(preview_path.read_text(encoding="utf-8"))
    assert preview_payload["settings"]["tone_curve_enabled"] is True
    assert preview_payload["settings"]["tone_curve_strength"] == 0.8
    assert state.session_gallery.invalidated_paths == [str(preview_path)]
    assert state.viewer_title_label.text().startswith("Pending RAW")


def test_live_lab_review_mode_save_current_after_session_stop_commits_pending_raw(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "P070020_1.ORF"
    source_path.write_bytes(b"raw-bytes")

    preview_path = tmp_path / "previews" / "P070020_1_preview.jpg"
    preview_path.parent.mkdir(parents=True, exist_ok=True)
    preview_path.write_text("preview", encoding="utf-8")

    committed_path = tmp_path / "imports" / "P070020_1.jpg"
    committed_path.parent.mkdir(parents=True, exist_ok=True)

    state = _build_raw_controls_state()
    state._session_active = True
    state._session_observation_id = 1
    state._session_observation_snapshot = {"id": 1, "name": "Observation 1"}
    state._session_id = "session-1"
    state._session_import_count = 1
    state._active_session_mode = live_lab_tab.LiveLabTab.SESSION_MODE_LIVE
    state._pending_stop_status = None
    state._watcher = None
    state._session_image_ids = []
    state._selected_session_image_id = None
    state._pending_raw_captures = [
        live_lab_tab.PendingRawCapture(
            source_path=source_path,
            companion_jpeg_path=None,
            lab_metadata={"image_type": "microscope"},
            raw_settings=RawRenderSettings.default(),
            preview_path=preview_path,
            group_key="group-1",
            observation_id=1,
        )
    ]
    state._selected_pending_raw_index = 0
    state._pending_companion_groups = {"group-1": {"paths": {"ignored"}, "timer": None}}
    state._consumed_companion_groups = {"group-1"}
    state._pending_raw_preview_timer = SimpleNamespace(stop=lambda: None, start=lambda _ms: None)
    state._cancel_raw_edit_session = lambda restore_selection=False: None
    state._log_session_event = lambda *args, **kwargs: None
    state._update_session_controls = lambda: None
    state._save_raw_processing_settings_for_current_context = lambda settings=None: None
    state._show_session_image = lambda image_id: live_lab_tab.LiveLabTab._show_session_image(state, image_id)

    add_image_calls: list[dict[str, object]] = []
    captured_calls: list[dict[str, object]] = []
    _install_fake_local_ingest_pipeline(
        monkeypatch,
        working_path_factory=lambda source: committed_path,
        captured_calls=captured_calls,
        add_image_calls=add_image_calls,
    )
    images = {1: _make_raw_image_row(1, source_path=source_path, derivative_path=committed_path)}
    monkeypatch.setattr(
        live_lab_tab.ImageDB,
        "get_image",
        lambda image_id: copy.deepcopy(images.get(int(image_id))) if int(image_id) in images else None,
    )
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)

    live_lab_tab.LiveLabTab._finalize_session_stop(state)
    assert state._session_observation_id == 1

    statuses: list[tuple[str, str, int]] = []
    state._show_status = lambda text, tone="info", timeout_ms=4000: statuses.append((text, tone, timeout_ms))
    monkeypatch.setattr(live_lab_tab.QApplication, "focusWidget", lambda: None)

    state._handle_raw_review_shortcut("enter")

    assert len(captured_calls) == 1
    assert len(add_image_calls) == 1
    assert state._pending_raw_captures == []
    assert state._session_image_ids == [1]
    assert state.session_gallery.items[0]["id"] == 1
    assert state.session_gallery.items[0]["badges"][0] != "UNSAVED RAW"
    assert state.viewer_title_label.text().startswith("Last import")
    assert any("Saved pending RAW capture" in text and tone == "success" for text, tone, _ in statuses)


def test_live_lab_review_mode_save_all_after_session_stop_commits_every_pending_raw(tmp_path, monkeypatch):
    _qapp()
    source_one = tmp_path / "P070020_1.ORF"
    source_two = tmp_path / "P070021_1.ORF"
    source_one.write_bytes(b"raw-1")
    source_two.write_bytes(b"raw-2")

    preview_one = tmp_path / "previews" / "P070020_1_preview.jpg"
    preview_two = tmp_path / "previews" / "P070021_1_preview.jpg"
    preview_one.parent.mkdir(parents=True, exist_ok=True)
    preview_one.write_text("preview-1", encoding="utf-8")
    preview_two.write_text("preview-2", encoding="utf-8")

    committed_one = tmp_path / "imports" / "P070020_1.jpg"
    committed_two = tmp_path / "imports" / "P070021_1.jpg"
    committed_one.parent.mkdir(parents=True, exist_ok=True)

    state = _build_raw_controls_state()
    state._session_active = True
    state._session_observation_id = 1
    state._session_observation_snapshot = {"id": 1, "name": "Observation 1"}
    state._session_id = "session-1"
    state._session_import_count = 2
    state._active_session_mode = live_lab_tab.LiveLabTab.SESSION_MODE_LIVE
    state._pending_stop_status = None
    state._watcher = None
    state._session_image_ids = []
    state._selected_session_image_id = None
    state._pending_raw_captures = [
        live_lab_tab.PendingRawCapture(
            source_path=source_one,
            companion_jpeg_path=None,
            lab_metadata={"image_type": "microscope"},
            raw_settings=RawRenderSettings.default(),
            preview_path=preview_one,
            group_key="group-1",
            observation_id=1,
        ),
        live_lab_tab.PendingRawCapture(
            source_path=source_two,
            companion_jpeg_path=None,
            lab_metadata={"image_type": "microscope"},
            raw_settings=RawRenderSettings.default(),
            preview_path=preview_two,
            group_key="group-2",
            observation_id=1,
        ),
    ]
    state._selected_pending_raw_index = 0
    state._pending_companion_groups = {"group-1": {"paths": {"ignored-1"}, "timer": None}, "group-2": {"paths": {"ignored-2"}, "timer": None}}
    state._consumed_companion_groups = {"group-1", "group-2"}
    state._pending_raw_preview_timer = SimpleNamespace(stop=lambda: None, start=lambda _ms: None)
    state._cancel_raw_edit_session = lambda restore_selection=False: None
    state._log_session_event = lambda *args, **kwargs: None
    state._update_session_controls = lambda: None
    state._save_raw_processing_settings_for_current_context = lambda settings=None: None

    add_image_calls: list[dict[str, object]] = []
    captured_calls: list[dict[str, object]] = []
    _install_fake_local_ingest_pipeline(
        monkeypatch,
        working_path_factory=lambda source: committed_one if source == source_one else committed_two,
        captured_calls=captured_calls,
        add_image_calls=add_image_calls,
    )
    images = {
        1: _make_raw_image_row(1, source_path=source_one, derivative_path=committed_one),
        2: _make_raw_image_row(2, source_path=source_two, derivative_path=committed_two),
    }
    monkeypatch.setattr(
        live_lab_tab.ImageDB,
        "get_image",
        lambda image_id: copy.deepcopy(images.get(int(image_id))) if int(image_id) in images else None,
    )

    statuses: list[tuple[str, str, int]] = []
    state._show_status = lambda text, tone="info", timeout_ms=4000: statuses.append((text, tone, timeout_ms))

    live_lab_tab.LiveLabTab._finalize_session_stop(state)
    state._commit_all_pending_raw_captures()

    assert len(captured_calls) == 2
    assert len(add_image_calls) == 2
    assert state._pending_raw_captures == []
    assert state._session_image_ids == [1, 2]
    assert state.session_gallery.items and all(not str(item["id"]).startswith("pending:") for item in state.session_gallery.items)
    assert any("Saved 2 RAW capture(s)." in text and tone == "success" for text, tone, _ in statuses)


def test_live_lab_review_mode_pending_metadata_changes_stick_to_each_capture_on_save_all(tmp_path, monkeypatch):
    _qapp()
    source_one = tmp_path / "P070020_1.ORF"
    source_two = tmp_path / "P070021_1.ORF"
    source_one.write_bytes(b"raw-1")
    source_two.write_bytes(b"raw-2")

    preview_one = tmp_path / "previews" / "P070020_1_preview.jpg"
    preview_two = tmp_path / "previews" / "P070021_1_preview.jpg"
    preview_one.parent.mkdir(parents=True, exist_ok=True)
    preview_one.write_text("preview-1", encoding="utf-8")
    preview_two.write_text("preview-2", encoding="utf-8")

    committed_one = tmp_path / "imports" / "P070020_1.jpg"
    committed_two = tmp_path / "imports" / "P070021_1.jpg"
    committed_one.parent.mkdir(parents=True, exist_ok=True)

    state = _build_raw_controls_state()
    state._session_active = True
    state._session_observation_id = 1
    state._session_observation_snapshot = {"id": 1, "name": "Observation 1"}
    state._session_id = "session-1"
    state._session_import_count = 2
    state._active_session_mode = live_lab_tab.LiveLabTab.SESSION_MODE_LIVE
    state._pending_stop_status = None
    state._watcher = None
    state._session_image_ids = []
    state._selected_session_image_id = None
    state._pending_companion_groups = {
        "group-1": {"paths": {"ignored-1"}, "timer": None},
        "group-2": {"paths": {"ignored-2"}, "timer": None},
    }
    state._consumed_companion_groups = {"group-1", "group-2"}
    state._pending_raw_preview_timer = SimpleNamespace(stop=lambda: None, start=lambda _ms: None)
    state._cancel_raw_edit_session = lambda restore_selection=False: None
    state._log_session_event = lambda *args, **kwargs: None
    state._update_session_controls = lambda: None
    state._save_raw_processing_settings_for_current_context = lambda settings=None: None
    state._refresh_session_gallery = lambda: live_lab_tab.LiveLabTab._refresh_session_gallery(state)
    state._show_session_image = lambda image_id: None
    state._refresh_main_window_after_import = lambda image_id: None
    state._update_observation_thumbnail = lambda: None
    state._show_status = lambda *args, **kwargs: None
    state._clear_session_viewer = lambda *args, **kwargs: None

    for combo, values in (
        (state.objective_combo, [("10x", "10x"), ("40x", "40x"), ("63x", "63x")]),
        (state.contrast_combo, [("BF", "BF"), ("DIC", "DIC")]),
        (state.mount_combo, [("Water", "Water"), ("KOH", "KOH")]),
        (state.stain_combo, [("Not set", "Not_set"), ("Congo Red", "Congo_Red")]),
        (state.sample_combo, [("Not set", "Not_set"), ("Dried", "Dried")]),
    ):
        combo.clear()
        for label, value in values:
            combo.addItem(label, value)

    state.raw_capture_mode_selector.set_selected_value(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    state._on_raw_capture_mode_changed(live_lab_tab.LiveLabTab.RAW_CAPTURE_MODE_REVIEW)
    state._current_lab_metadata = lambda: {
        "image_type": "microscope",
        "objective_name": state.objective_combo.currentData(),
        "objective_label": str(state.objective_combo.currentText() or "").strip() or None,
        "contrast": state.contrast_combo.currentData(),
        "contrast_label": str(state.contrast_combo.currentText() or "").strip() or None,
        "mount_medium": state.mount_combo.currentData(),
        "mount_label": str(state.mount_combo.currentText() or "").strip() or None,
        "stain": state.stain_combo.currentData(),
        "stain_label": str(state.stain_combo.currentText() or "").strip() or None,
        "sample_type": state.sample_combo.currentData(),
        "sample_label": str(state.sample_combo.currentText() or "").strip() or None,
    }

    add_image_calls: list[dict[str, object]] = []
    captured_calls: list[dict[str, object]] = []
    _install_fake_local_ingest_pipeline(
        monkeypatch,
        working_path_factory=lambda source: committed_one if source == source_one else committed_two,
        captured_calls=captured_calls,
        add_image_calls=add_image_calls,
    )
    images = {
        1: _make_raw_image_row(1, source_path=source_one, derivative_path=committed_one),
        2: _make_raw_image_row(2, source_path=source_two, derivative_path=committed_two),
    }
    monkeypatch.setattr(
        live_lab_tab.ImageDB,
        "get_image",
        lambda image_id: copy.deepcopy(images.get(int(image_id))) if int(image_id) in images else None,
    )
    monkeypatch.setattr(live_lab_tab, "render_raw_preview", _fake_render_raw_preview)

    state.objective_combo.setCurrentIndex(0)
    state.contrast_combo.setCurrentIndex(0)
    state.mount_combo.setCurrentIndex(0)
    state.stain_combo.setCurrentIndex(0)
    state.sample_combo.setCurrentIndex(0)
    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(state, str(source_one), group_key="group-1", state={})

    state.objective_combo.setCurrentIndex(2)
    state.contrast_combo.setCurrentIndex(1)
    state.mount_combo.setCurrentIndex(0)
    state.stain_combo.setCurrentIndex(0)
    state.sample_combo.setCurrentIndex(0)
    assert live_lab_tab.LiveLabTab._handle_raw_companion_source(state, str(source_two), group_key="group-2", state={})

    live_lab_tab.LiveLabTab._show_pending_raw_capture(state, 0)
    state.objective_combo.setCurrentIndex(1)
    state.contrast_combo.setCurrentIndex(1)
    state.mount_combo.setCurrentIndex(1)
    state.stain_combo.setCurrentIndex(1)
    state.sample_combo.setCurrentIndex(1)
    assert live_lab_tab.LiveLabTab._sync_selected_pending_raw_metadata_from_controls(state) is True

    first_pending, second_pending = state._pending_raw_captures
    assert first_pending.lab_metadata["objective_name"] == "40x"
    assert first_pending.lab_metadata["contrast"] == "DIC"
    assert first_pending.lab_metadata["mount_medium"] == "KOH"
    assert first_pending.lab_metadata["stain"] == "Congo_Red"
    assert first_pending.lab_metadata["sample_type"] == "Dried"
    assert second_pending.lab_metadata["objective_name"] == "63x"
    assert second_pending.lab_metadata["contrast"] == "DIC"
    assert second_pending.lab_metadata["mount_medium"] == "Water"
    assert second_pending.lab_metadata["stain"] == "Not_set"
    assert second_pending.lab_metadata["sample_type"] == "Not_set"
    assert state.session_gallery.items[0]["microscope_tag_text"] == "40x DIC"
    assert state.session_gallery.items[1]["microscope_tag_text"] == "63x DIC"

    live_lab_tab.LiveLabTab._finalize_session_stop(state)
    state._commit_all_pending_raw_captures()

    assert len(captured_calls) == 2
    assert len(add_image_calls) == 2
    assert add_image_calls[0]["lab_metadata"]["objective_name"] == "40x"
    assert add_image_calls[0]["lab_metadata"]["contrast"] == "DIC"
    assert add_image_calls[0]["lab_metadata"]["mount_medium"] == "KOH"
    assert add_image_calls[0]["lab_metadata"]["stain"] == "Congo_Red"
    assert add_image_calls[0]["lab_metadata"]["sample_type"] == "Dried"
    assert add_image_calls[1]["lab_metadata"]["objective_name"] == "63x"
    assert add_image_calls[1]["lab_metadata"]["contrast"] == "DIC"
    assert add_image_calls[1]["lab_metadata"]["mount_medium"] == "Water"
    assert add_image_calls[1]["lab_metadata"]["stain"] == "Not_set"
    assert add_image_calls[1]["lab_metadata"]["sample_type"] == "Not_set"


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

    state.raw_controls.white_balance_selector.set_selected_value("auto")
    state.raw_controls.auto_levels_checkbox.setChecked(False)
    state.raw_controls.tone_curve_checkbox.setChecked(True)
    state.raw_controls.curve_strength_slider.setValue(70)
    state.raw_controls.curve_midpoint_slider.setValue(35)
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

    monkeypatch.setattr(live_lab_tab.QApplication, "focusWidget", lambda: None)
    state._handle_raw_review_shortcut("delete")

    assert captured_calls == []
    assert add_image_calls == []
    assert state._pending_raw_captures == []
    assert not preview_path.exists()


def test_live_lab_review_mode_delete_current_committed_image_removes_local_processed_file(tmp_path, monkeypatch):
    _qapp()
    source_path = tmp_path / "sample.nef"
    source_path.write_bytes(b"raw-bytes")
    processed_path = tmp_path / "imports" / "sample_102.jpg"
    processed_path.parent.mkdir(parents=True, exist_ok=True)
    processed_path.write_text("processed", encoding="utf-8")
    other_path = tmp_path / "imports" / "sample_101.jpg"
    other_path.write_text("processed-2", encoding="utf-8")

    state = _build_raw_controls_state()
    state._session_active = False
    state._session_observation_id = 1
    state._session_observation_snapshot = {"id": 1, "name": "Observation 1"}
    state._session_image_ids = [101, 102]
    state._selected_session_image_id = 102
    state._selected_pending_raw_index = -1
    state._pending_raw_captures = []
    state._pending_companion_groups = {}
    state._consumed_companion_groups = set()

    images = {
        101: _make_raw_image_row(101, source_path=source_path, derivative_path=other_path),
        102: _make_raw_image_row(102, source_path=source_path, derivative_path=processed_path),
    }
    deleted_ids: list[int] = []
    shown_ids: list[int] = []
    statuses: list[tuple[str, str, int]] = []

    monkeypatch.setattr(
        live_lab_tab.ImageDB,
        "get_image",
        lambda image_id: copy.deepcopy(images.get(int(image_id))) if int(image_id) in images else None,
    )

    def fake_delete_image(image_id):
        deleted_ids.append(int(image_id))
        processed_path.unlink(missing_ok=True)
        images.pop(int(image_id), None)

    monkeypatch.setattr(live_lab_tab.ImageDB, "delete_image", fake_delete_image)
    state._show_session_image = lambda image_id: shown_ids.append(image_id)
    state._show_status = lambda text, tone="info", timeout_ms=4000: statuses.append((text, tone, timeout_ms))
    monkeypatch.setattr(live_lab_tab.QApplication, "focusWidget", lambda: None)

    state._handle_raw_review_shortcut("delete")

    assert deleted_ids == [102]
    assert state._session_image_ids == [101]
    assert state._selected_session_image_id == 101
    assert shown_ids == [101]
    assert state.session_gallery.items and state.session_gallery.items[0]["id"] == 101
    assert not processed_path.exists()
    assert any("Deleted local processed image" in text and tone == "success" for text, tone, _ in statuses)


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

    state.raw_controls.white_balance_selector.set_selected_value("auto")
    state.raw_controls.auto_levels_checkbox.setChecked(False)
    state.raw_controls.tone_curve_checkbox.setChecked(True)
    state.raw_controls.curve_strength_slider.setValue(80)
    state.raw_controls.curve_midpoint_slider.setValue(20)
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
