from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

import ui.main_window as main_window


def _qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_load_image_record_merges_existing_and_ingest_lab_metadata(monkeypatch, tmp_path):
    _qapp()
    original_path = tmp_path / "source.nef"
    converted_path = tmp_path / "converted.jpg"
    original_path.write_bytes(b"raw-bytes")
    converted_path.write_bytes(b"jpeg-bytes")
    captured_update: dict[str, object] = {}

    class DummyIngestResult:
        def provenance_kwargs(self):
            return {
                "source_role": "converted_local",
                "file_purpose": "microscope",
                "original_mime_type": "image/x-raw",
                "working_mime_type": "image/jpeg",
            }

    DummyIngestResult.working_path = str(converted_path)
    DummyIngestResult.original_path = str(original_path)
    DummyIngestResult.lab_metadata = {
        "raw_processing": {
            "engine": "rawpy",
            "source": {
                "kind": "camera_raw",
                "path": str(original_path),
                "mime_type": "image/x-raw",
            },
            "settings": {
                "white_balance_mode": "camera",
                "auto_levels": True,
            },
        }
    }

    fake_self = SimpleNamespace()
    fake_self._flush_measure_image_note = lambda: None
    fake_self.current_image_id = 0
    fake_self._save_current_image_measure_session_view = lambda: None
    fake_self._save_current_image_measure_view_settings = lambda: None
    fake_self._set_measure_image_note_text = lambda *args, **kwargs: None
    fake_self._reset_calibration_interaction_state = lambda: None
    fake_self._update_measure_copyright_overlay = lambda: None
    fake_self._load_pixmap_cached = lambda path: f"pixmap:{path}"
    fake_self.image_label = SimpleNamespace(
        set_image=lambda *args, **kwargs: None,
        reset_view=lambda: None,
        set_microns_per_pixel=lambda *args, **kwargs: None,
        set_objective_color=lambda *args, **kwargs: None,
    )
    fake_self.update_exif_panel = lambda *args, **kwargs: None
    fake_self._apply_measure_session_view_for_current_image = lambda: False
    fake_self.image_info_label = SimpleNamespace(setText=lambda *args, **kwargs: None)
    fake_self.apply_image_scale = lambda *args, **kwargs: None
    fake_self.microns_per_pixel = 1.0
    fake_self.update_controls_for_image_type = lambda *args, **kwargs: None
    fake_self._apply_measure_view_settings_for_current_image = lambda: None
    fake_self._update_scale_mismatch_warning = lambda: None
    fake_self.current_objective_name = None
    fake_self.set_measure_color = lambda *args, **kwargs: None
    fake_self.measure_color = None
    fake_self.default_measure_color = None
    fake_self.refresh_observation_images = lambda *args, **kwargs: None
    fake_self.measurement_lines = {}
    fake_self.temp_lines = []
    fake_self.points = []
    fake_self.load_measurement_lines = lambda: None
    fake_self.update_display_lines = lambda: None
    fake_self.update_statistics = lambda: None
    fake_self.update_measurements_table = lambda: None
    fake_self.measurements_table = SimpleNamespace(clearSelection=lambda: None)
    fake_self.spore_preview = SimpleNamespace(clear=lambda: None)
    fake_self._set_measure_category_for_current_image = lambda: None
    fake_self._suppress_gallery_update = True
    fake_self.schedule_gallery_refresh = lambda: None
    fake_self._prefetch_adjacent_images = lambda: None
    fake_self.current_image_path = None
    fake_self.current_image_type = None
    fake_self.active_observation_name = None

    monkeypatch.setattr(
        main_window,
        "prepare_local_ingest_image",
        lambda *args, **kwargs: DummyIngestResult(),
    )
    monkeypatch.setattr(main_window.ImageDB, "update_image", lambda image_id, **kwargs: captured_update.update(kwargs))
    monkeypatch.setattr(main_window.QTimer, "singleShot", lambda *args, **kwargs: None)

    main_window.MainWindow.load_image_record(
        fake_self,
        {
            "id": 7,
            "filepath": str(original_path),
            "image_type": "microscope",
            "notes": "note",
            "measure_color": "#123456",
            "lab_metadata": {
                "contrast": "phase",
                "objective_name": "40x",
            },
        },
        refresh_table=False,
    )

    assert captured_update["lab_metadata"]["contrast"] == "phase"
    assert captured_update["lab_metadata"]["objective_name"] == "40x"
    assert captured_update["lab_metadata"]["raw_processing"]["source"]["path"] == str(original_path)
    assert captured_update["lab_metadata"]["raw_processing"]["source"]["kind"] == "camera_raw"
    assert captured_update["lab_metadata"]["raw_processing"]["settings"]["white_balance_mode"] == "camera"
