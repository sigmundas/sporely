from __future__ import annotations

import os
from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtGui import QPixmap
from PySide6.QtWidgets import QApplication, QCheckBox, QComboBox

from ui import image_import_dialog
from ui.image_import_dialog import (
    ImageImportDialog,
    ImageImportResult,
    image_import_result_from_candidate,
)
from utils.image_import_candidates import (
    IMAGE_IMPORT_SOURCE_KIND_RAW,
    IMAGE_IMPORT_STATUS_FAILED,
    IMAGE_IMPORT_STATUS_READY,
    IMAGE_IMPORT_STATUS_SKIPPED,
    ImageImportCandidate,
)
from utils.image_metadata_merge import merge_image_lab_metadata
from utils.raw_render import RawRenderSettings


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class _FakeRadio:
    def __init__(self, checked: bool) -> None:
        self._checked = bool(checked)

    def isChecked(self) -> bool:
        return self._checked


class _FakeGallery:
    def __init__(self) -> None:
        self.items: list[dict] = []
        self._selected_paths: list[str] = []

    def selected_paths(self) -> list[str]:
        return list(self._selected_paths)

    def set_items(self, items: list[dict]) -> None:
        self.items = list(items)

    def select_paths(self, paths: list[str]) -> None:
        self._selected_paths = list(paths)


class _FakeLabel:
    def __init__(self) -> None:
        self.text = ""

    def setText(self, text: str) -> None:  # noqa: N802 - Qt-style name
        self.text = text


def _make_add_images_dummy(*, source_preference: str) -> SimpleNamespace:
    dummy = SimpleNamespace()
    dummy.import_results = []
    dummy.image_paths = []
    dummy._converted_import_paths = set()
    dummy._missing_exif_paths = set()
    dummy.resize_to_optimal_default = True
    dummy.store_original_default = False
    dummy.micro_radio = _FakeRadio(True)
    dummy.gallery = _FakeGallery()
    dummy.summary_label = _FakeLabel()
    dummy.objectives = {}
    dummy.tr = lambda text: text
    dummy._selected_raw_companion_source_preference = lambda: source_preference
    dummy._field_tag_value = lambda key: {
        "contrast": "phase",
        "mount": "water",
        "stain": "none",
        "sample": "spore",
    }.get(key)
    dummy._result_status = lambda result: ImageImportDialog._result_status(result)
    dummy._set_hint_progress_visible_calls = []
    dummy._set_hint_progress_calls = []
    dummy._set_hint_progress_visible = lambda visible: dummy._set_hint_progress_visible_calls.append(bool(visible))
    dummy._set_hint_progress = lambda text, value=None: dummy._set_hint_progress_calls.append((text, value))
    dummy._update_summary = lambda: ImageImportDialog._update_summary(dummy)
    dummy._seed_observation_metadata = lambda: None
    dummy._update_observation_source_index = lambda: None
    dummy._sync_observation_metadata_inputs = lambda: None
    dummy._refresh_gallery = lambda: ImageImportDialog._refresh_gallery(dummy)
    dummy._update_scale_group_state = lambda: None
    dummy._update_set_from_image_button_state = lambda: None
    dummy._update_ai_controls_state = lambda: None
    dummy._update_action_buttons_state = lambda: None
    dummy._compute_resample_scale_factor = lambda result, respect_toggle=True: 1.0
    dummy._select_image_calls = []
    dummy._select_image = lambda index, sync_gallery=True: dummy._select_image_calls.append((index, sync_gallery))
    dummy.set_status_messages = []
    dummy.set_status = lambda text, timeout_ms=4000, tone="info": dummy.set_status_messages.append((text, tone))
    dummy._set_settings_hint = lambda text, color: dummy.set_status_messages.append(
        (text, ImageImportDialog._status_tone_from_color(color))
    )
    dummy._observation_source_index = None
    dummy._accepted = False
    dummy._continue_to_observation_details = False
    dummy._observation_lat = None
    dummy._observation_lon = None
    dummy._current_exif_path = None
    dummy._current_exif_datetime = None
    dummy._current_exif_lat = None
    dummy._current_exif_lon = None
    dummy._clear_current_image_exif = lambda: None
    dummy._set_preview_for_result = lambda result, preserve_view=False: None
    dummy._load_result_into_form = lambda result: None
    dummy._update_current_image_exif = lambda result: None
    dummy._update_ai_table = lambda: None
    dummy._update_ai_overlay = lambda: None
    dummy._restore_scale_bar_overlay = lambda result: None
    dummy._stage_raw_candidates = lambda new_results: None
    return dummy


def test_image_import_result_from_candidate_preserves_prepared_metadata_and_failure_state(tmp_path):
    raw_path = tmp_path / "sample.nef"
    jpeg_path = tmp_path / "sample.jpg"
    raw_path.write_bytes(b"raw-bytes")
    jpeg_path.write_bytes(b"jpeg-bytes")
    captured_at = datetime(2026, 5, 16, 19, 44, 11)
    candidate = ImageImportCandidate(
        source_path=raw_path,
        selected_path=jpeg_path,
        working_path=jpeg_path,
        preview_path=jpeg_path,
        source_kind=IMAGE_IMPORT_SOURCE_KIND_RAW,
        status=IMAGE_IMPORT_STATUS_READY,
        companion_paths=(raw_path, jpeg_path),
        raw_path=raw_path,
        camera_jpeg_path=jpeg_path,
        has_raw_companion=True,
        selected_source_policy="camera_jpeg",
        captured_at=captured_at,
        gps_latitude=59.91,
        gps_longitude=10.75,
        lab_metadata={
            "objective_name": "40x",
            "raw_processing": {"source": {"kind": "camera_raw"}},
        },
        fallback_used=True,
        fallback_reason="raw rendering unavailable",
    )

    result = image_import_result_from_candidate(
        candidate,
        image_type="microscope",
        contrast="phase",
        mount_medium="water",
        stain="none",
        sample_type="spore",
        resize_to_optimal=True,
        store_original=False,
    )

    assert result.filepath == str(jpeg_path.resolve())
    assert result.preview_path == str(jpeg_path.resolve())
    assert result.original_filepath == str(raw_path.resolve())
    assert result.source_filepath == str(raw_path.resolve())
    assert result.image_type == "microscope"
    assert result.contrast == "phase"
    assert result.mount_medium == "water"
    assert result.stain == "none"
    assert result.sample_type == "spore"
    assert result.status == IMAGE_IMPORT_STATUS_READY
    assert result.captured_at is not None and result.captured_at.isValid()
    assert result.gps_latitude == pytest.approx(59.91)
    assert result.gps_longitude == pytest.approx(10.75)
    assert result.exif_has_gps is True
    assert result.resize_to_optimal is True
    assert result.store_original is False
    assert result.fallback_used is True
    assert result.fallback_reason == "raw rendering unavailable"
    assert result.selected_source_policy == "camera_jpeg"
    assert result.source_kind == IMAGE_IMPORT_SOURCE_KIND_RAW
    assert result.companion_paths == (str(raw_path.resolve()), str(jpeg_path.resolve()))
    assert result.lab_metadata["objective_name"] == "40x"
    assert result.lab_metadata["raw_processing"]["source"]["kind"] == "camera_raw"


def test_set_preview_for_result_uses_preview_path_as_full_source_when_preview_is_scaled(qapp):
    dummy = SimpleNamespace()
    dummy.preview_calls = []
    dummy.preview = SimpleNamespace(
        set_image_sources=lambda pixmap, full_path=None, preview_scaled=False, preserve_view=False: dummy.preview_calls.append(
            {
                "full_path": full_path,
                "preview_scaled": preview_scaled,
                "preserve_view": preserve_view,
            }
        ),
        set_image=lambda pixmap: None,
    )
    dummy.preview_stack = SimpleNamespace(setCurrentWidget=lambda widget: None)
    dummy._resolve_preview_pixmap = lambda result: (QPixmap(8, 8), True)
    dummy._apply_resize_preview = lambda pixmap, result: (pixmap, False)
    dummy._update_resize_preview_tag = lambda result, preview_resized, preview_pixmap=None: None
    dummy._compute_resample_scale_factor = lambda result, respect_toggle=True: 1.0
    dummy._format_significant = lambda value, digits: f"{value:.{digits}f}"
    dummy._get_image_size = lambda path: (8, 8)
    result = SimpleNamespace(
        filepath="/tmp/raw_source.nef",
        preview_path="/tmp/raw_preview.jpg",
        image_type="microscope",
    )

    ImageImportDialog._set_preview_for_result(dummy, result, preserve_view=False)

    assert dummy.preview_calls[0]["full_path"] == "/tmp/raw_preview.jpg"
    assert dummy.preview_calls[0]["preview_scaled"] is True


def test_collect_raw_settings_from_form_uses_raw_controls_widget_state():
    dummy = SimpleNamespace()
    expected = RawRenderSettings(
        white_balance_mode="custom",
        wb_multipliers=(1.2, 1.0, 1.4),
        tone_curve_enabled=True,
        tone_curve_strength=0.55,
        tone_curve_midpoint=0.35,
    )
    dummy.raw_controls = SimpleNamespace(settings=lambda: expected)

    collected = ImageImportDialog._collect_raw_settings_from_form(dummy, base={"white_balance_mode": "camera"})

    assert collected == expected.to_dict()


def test_prepare_images_combo_alerts_track_unset_and_custom_objective_state(qapp):
    dummy = SimpleNamespace()
    dummy.tr = lambda text: text
    dummy.objectives = {"40x": {"magnification": 40.0}}
    dummy.default_objective = None
    dummy._FIELD_TAG_DEFAULTS = ImageImportDialog._FIELD_TAG_DEFAULTS
    dummy._canonicalize_tag = lambda category, value: ImageImportDialog._canonicalize_tag(dummy, category, value)
    dummy._field_tag_value = lambda category: ImageImportDialog._field_tag_value(dummy, category)
    dummy._set_combo_tag_value = lambda combo, category, value: ImageImportDialog._set_combo_tag_value(dummy, combo, category, value)
    dummy._update_lab_state_combo_alerts = lambda *_args: ImageImportDialog._update_lab_state_combo_alerts(dummy, *_args)
    dummy._set_field_tag_defaults_in_form = lambda: ImageImportDialog._set_field_tag_defaults_in_form(dummy)
    dummy._populate_objectives = lambda selected_key=None: ImageImportDialog._populate_objectives(dummy, selected_key)
    dummy.scale_bar_mode_checkbox = QCheckBox()

    dummy.objective_combo = QComboBox()
    dummy.objective_combo.addItem("Not set", None)
    dummy.objective_combo.addItem("40x", "40x")

    dummy.contrast_combo = QComboBox()
    dummy.contrast_combo.addItem("Not set", "Not_set")
    dummy.contrast_combo.addItem("Phase", "phase")
    dummy.mount_combo = QComboBox()
    dummy.mount_combo.addItem("Not set", "Not_set")
    dummy.mount_combo.addItem("Water", "water")
    dummy.stain_combo = QComboBox()
    dummy.stain_combo.addItem("Not set", "Not_set")
    dummy.stain_combo.addItem("None", "none")
    dummy.sample_combo = QComboBox()
    dummy.sample_combo.addItem("Not set", "Not_set")
    dummy.sample_combo.addItem("Spore", "spore")

    dummy._populate_objectives(selected_key=None)
    assert dummy.objective_combo.count() == 1
    assert dummy.objective_combo.currentData() == "40x"
    assert dummy.objective_combo.property("labStateAlert") is False

    dummy.scale_bar_mode_checkbox.setChecked(True)
    dummy._update_lab_state_combo_alerts()
    assert dummy.objective_combo.property("labStateAlert") is False

    dummy._set_field_tag_defaults_in_form()
    assert dummy.contrast_combo.property("labStateAlert") is True
    assert dummy.mount_combo.property("labStateAlert") is True
    assert dummy.stain_combo.property("labStateAlert") is True
    assert dummy.sample_combo.property("labStateAlert") is True

    dummy.contrast_combo.setCurrentIndex(1)
    dummy.mount_combo.setCurrentIndex(1)
    dummy.stain_combo.setCurrentIndex(1)
    dummy.sample_combo.setCurrentIndex(1)
    dummy._update_lab_state_combo_alerts()

    assert dummy.contrast_combo.property("labStateAlert") is False
    assert dummy.mount_combo.property("labStateAlert") is False
    assert dummy.stain_combo.property("labStateAlert") is False
    assert dummy.sample_combo.property("labStateAlert") is False


def test_prepare_images_combo_alerts_are_suppressed_for_field_images(qapp):
    dummy = SimpleNamespace()
    dummy.tr = lambda text: text
    dummy.objectives = {}
    dummy.default_objective = None
    dummy._FIELD_TAG_DEFAULTS = ImageImportDialog._FIELD_TAG_DEFAULTS
    dummy._canonicalize_tag = lambda category, value: ImageImportDialog._canonicalize_tag(dummy, category, value)
    dummy._field_tag_value = lambda category: ImageImportDialog._field_tag_value(dummy, category)
    dummy._set_combo_tag_value = lambda combo, category, value: ImageImportDialog._set_combo_tag_value(dummy, combo, category, value)
    dummy._update_lab_state_combo_alerts = lambda *_args: ImageImportDialog._update_lab_state_combo_alerts(dummy, *_args)
    dummy._current_single_index = lambda: 0
    dummy.import_results = [SimpleNamespace(image_type="field")]
    dummy.scale_bar_mode_checkbox = QCheckBox()

    dummy.objective_combo = QComboBox()
    dummy.objective_combo.addItem("Not set", None)
    dummy.contrast_combo = QComboBox()
    dummy.contrast_combo.addItem("Not set", "Not_set")
    dummy.mount_combo = QComboBox()
    dummy.mount_combo.addItem("Not set", "Not_set")
    dummy.stain_combo = QComboBox()
    dummy.stain_combo.addItem("Not set", "Not_set")
    dummy.sample_combo = QComboBox()
    dummy.sample_combo.addItem("Not set", "Not_set")

    dummy._update_lab_state_combo_alerts()

    assert dummy.objective_combo.property("labStateAlert") is False
    assert dummy.contrast_combo.property("labStateAlert") is False
    assert dummy.mount_combo.property("labStateAlert") is False
    assert dummy.stain_combo.property("labStateAlert") is False
    assert dummy.sample_combo.property("labStateAlert") is False


def test_add_images_uses_candidates_and_shows_failed_rows_without_stopping_batch(monkeypatch, tmp_path, qapp):
    raw_path = tmp_path / "P070020_1.ORF"
    jpeg_path = tmp_path / "P070020_1.JPG"
    failed_path = tmp_path / "broken.heic"
    for path, payload in (
        (raw_path, b"raw-bytes"),
        (jpeg_path, b"jpeg-bytes"),
        (failed_path, b"heic-bytes"),
    ):
        path.write_bytes(payload)

    pair_candidate = ImageImportCandidate(
        source_path=raw_path,
        selected_path=jpeg_path,
        working_path=jpeg_path,
        preview_path=jpeg_path,
        source_kind=IMAGE_IMPORT_SOURCE_KIND_RAW,
        status="staged",
        companion_paths=(raw_path, jpeg_path),
        raw_path=raw_path,
        camera_jpeg_path=jpeg_path,
        has_raw_companion=True,
        selected_source_policy="camera_jpeg",
        captured_at=datetime(2026, 5, 16, 19, 44, 11),
        gps_latitude=59.91,
        gps_longitude=10.75,
        lab_metadata={
            "objective_name": "40x",
            "raw_processing": {"source": {"kind": "camera_raw"}},
        },
    )
    failed_candidate = ImageImportCandidate(
        source_path=failed_path,
        selected_path=failed_path,
        source_kind="heic",
        status="staged",
        companion_paths=(failed_path,),
        lab_metadata={"image_type": "microscope"},
    )

    build_calls: list[tuple[list[str], str]] = []
    prepare_calls: list[dict] = []

    def fake_build(paths, *, source_preference):
        build_calls.append(([str(path) for path in paths], source_preference))
        return [deepcopy(pair_candidate), deepcopy(failed_candidate)]

    def fake_prepare(candidates, *, raw_settings=None, lab_metadata=None, output_dir=None, allow_raw_render=True, allow_heic_convert=True):
        prepare_calls.append(
            {
                "candidates": [candidate.display_name for candidate in candidates],
                "lab_metadata": deepcopy(lab_metadata),
                "output_dir": output_dir,
                "allow_raw_render": allow_raw_render,
                "allow_heic_convert": allow_heic_convert,
            }
        )
        prepared = []
        for candidate in candidates:
            prepared_candidate = deepcopy(candidate)
            merged_lab_metadata = merge_image_lab_metadata(candidate.lab_metadata, lab_metadata)
            prepared_candidate.lab_metadata = merged_lab_metadata
            if candidate.source_path == raw_path.resolve():
                prepared_candidate.status = IMAGE_IMPORT_STATUS_READY
                prepared_candidate.working_path = jpeg_path.resolve()
                prepared_candidate.preview_path = jpeg_path.resolve()
                prepared_candidate.failure_reason = None
                prepared_candidate.error_detail = None
            else:
                prepared_candidate.status = IMAGE_IMPORT_STATUS_FAILED
                prepared_candidate.working_path = None
                prepared_candidate.preview_path = None
                prepared_candidate.failure_reason = "heic conversion failed"
                prepared_candidate.error_detail = "No converter available"
            prepared.append(prepared_candidate)
        return prepared

    monkeypatch.setattr(image_import_dialog, "build_image_import_candidates", fake_build)
    monkeypatch.setattr(image_import_dialog, "prepare_image_import_candidates", fake_prepare)
    monkeypatch.setattr(image_import_dialog, "get_images_dir", lambda: tmp_path)

    dialog = _make_add_images_dummy(source_preference="camera_jpeg")

    ImageImportDialog.add_images(dialog, [str(raw_path), str(jpeg_path), str(failed_path)])

    assert build_calls == [([str(raw_path), str(jpeg_path), str(failed_path)], "camera_jpeg")]
    assert len(prepare_calls) == 2
    assert prepare_calls[0]["lab_metadata"] == {"image_type": "microscope"}
    assert prepare_calls[0]["output_dir"] == tmp_path / "imports"
    assert len(dialog.import_results) == 2
    assert dialog.import_results[0].status == IMAGE_IMPORT_STATUS_READY
    assert dialog.import_results[0].filepath == str(jpeg_path.resolve())
    assert dialog.import_results[0].original_filepath == str(raw_path.resolve())
    assert dialog.import_results[0].source_filepath == str(raw_path.resolve())
    assert dialog.import_results[0].lab_metadata["image_type"] == "microscope"
    assert dialog.import_results[0].lab_metadata["objective_name"] == "40x"
    assert dialog.import_results[0].lab_metadata["raw_processing"]["source"]["kind"] == "camera_raw"
    assert dialog.import_results[1].status == IMAGE_IMPORT_STATUS_FAILED
    assert dialog.import_results[1].filepath == str(failed_path.resolve())
    assert dialog.image_paths == [str(jpeg_path.resolve()), str(failed_path.resolve())]
    assert dialog._select_image_calls == [(0, True)]
    assert dialog._set_hint_progress_visible_calls[:1] == [True]
    assert dialog._set_hint_progress_visible_calls[-1] is False
    assert dialog.summary_label.text.endswith("Failed: 1")
    assert dialog.set_status_messages
    assert "failed to prepare" in dialog.set_status_messages[-1][0].lower()
    assert dialog.gallery.items[0]["center_badge"] is None
    assert dialog.gallery.items[1]["center_badge"] == "Failed"
    assert "RAW-derived" in dialog.gallery.items[0]["badges"]


def test_accept_and_close_filters_failed_and_skipped_results_before_continue(monkeypatch):
    committed = ImageImportResult(filepath="/tmp/committed.jpg")
    ready = ImageImportResult(filepath="/tmp/ready.jpg", status=IMAGE_IMPORT_STATUS_READY)
    failed = ImageImportResult(filepath="/tmp/failed.jpg", status=IMAGE_IMPORT_STATUS_FAILED)
    skipped = ImageImportResult(filepath="/tmp/skipped.heic", status=IMAGE_IMPORT_STATUS_SKIPPED)

    emitted: list[list[ImageImportResult]] = []
    accepted_calls: list[bool] = []
    dialog = SimpleNamespace(
        import_results=[committed, ready, failed, skipped],
        image_paths=[committed.filepath, ready.filepath, failed.filepath, skipped.filepath],
        selected_indices=[0, 1, 2, 3],
        selected_index=3,
        _continue_to_observation_details=True,
        _observation_lat=None,
        _observation_lon=None,
        _accepted=False,
        _apply_to_selected=lambda: None,
        _save_last_used_tag_settings=lambda: None,
        accept=lambda: accepted_calls.append(True),
        continueRequested=SimpleNamespace(emit=lambda results: emitted.append(list(results))),
    )
    dialog._result_status = lambda result: ImageImportDialog._result_status(result)
    dialog._accepted_import_results = lambda: ImageImportDialog._accepted_import_results(dialog)

    ImageImportDialog._accept_and_close(dialog)

    assert accepted_calls == [True]
    assert len(emitted) == 1
    assert emitted[0] == [committed, ready]
    assert dialog.import_results == [committed, ready]
    assert dialog.image_paths == [committed.filepath, ready.filepath]
    assert dialog._accepted is True
