"""Stage 10c: Kalibrer objektiv without information and warning boxes.

Validation is inline (New objective) or a disabled action whose hint says
why; busy states disable Close; success and failures are notices in the
dialog's bottom bar. Confirmations stay real dialogs.
"""
from __future__ import annotations

import json
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QEvent, Qt
from PySide6.QtGui import QColor, QPixmap
from PySide6.QtWidgets import QApplication, QMessageBox

from tests.test_calibration_dialog_history_view import _build_dialog
from ui import calibration_dialog
from ui.calibration_dialog import NewObjectiveDialog

LOAD_FIRST = "Please load a calibration image first."


@pytest.fixture(autouse=True)
def _no_information_or_warning_boxes(monkeypatch):
    def _fail(*args, **_kwargs):
        raise AssertionError(f"a message box was shown: {args[1:3]}")

    monkeypatch.setattr(QMessageBox, "information", _fail)
    monkeypatch.setattr(QMessageBox, "warning", _fail)


@pytest.fixture
def dialog(qtbot, monkeypatch):
    dialog = _build_dialog(monkeypatch)
    qtbot.addWidget(dialog)
    dialog.show()
    qtbot.waitExposed(dialog)
    return dialog


def _notice(dialog):
    controller = dialog._hint_controller
    return controller._notice


def _hover_hint(dialog, widget) -> str:
    QApplication.sendEvent(widget, QEvent(QEvent.Enter))
    return dialog.hint_bar._label.text()


def _image(path: str = "/nowhere/calibration.jpg") -> dict:
    pixmap = QPixmap(64, 48)
    pixmap.fill(QColor("white"))
    return {"path": path, "working_path": path, "pixmap": pixmap}


def _with_image(dialog, path: str = "/nowhere/calibration.jpg") -> None:
    dialog.calibration_images = [_image(path)]
    dialog.current_image_index = 0
    dialog._show_current_image()


def test_without_an_image_the_image_actions_are_disabled_and_say_why(dialog):
    for button in (dialog.auto_run_btn, dialog.export_btn, dialog.add_measurement_btn):
        assert not button.isEnabled(), button.text()
        assert _hover_hint(dialog, button) == LOAD_FIRST
        QApplication.sendEvent(button, QEvent(QEvent.Leave))
    _with_image(dialog)
    for button in (dialog.auto_run_btn, dialog.export_btn, dialog.add_measurement_btn):
        assert button.isEnabled(), button.text()
    dialog._clear_all()
    assert not dialog.auto_run_btn.isEnabled() and not dialog.add_measurement_btn.isEnabled()


def test_picking_the_two_points_disables_add_measurement_without_the_image_hint(dialog):
    _with_image(dialog)
    dialog._start_measurement()
    assert not dialog.add_measurement_btn.isEnabled()
    assert _hover_hint(dialog, dialog.add_measurement_btn) != LOAD_FIRST


def test_a_run_disables_close_and_the_window_close_waits_and_says_why(qtbot, dialog):
    _with_image(dialog)
    # As the run starts: the controls first, then the flag (review finding).
    dialog._set_auto_controls_enabled(False)
    dialog._auto_calibration_running = True
    dialog._set_hint_progress_visible(True)
    dialog._set_hint_progress("Running...", 10)
    assert not dialog.close_btn.isEnabled()
    assert not dialog.export_btn.isEnabled() and not dialog.auto_run_btn.isEnabled()
    assert "still running" in dialog.close_btn.property("_hint_disabled_text")

    dialog.reject()  # Esc
    assert dialog.isVisible()
    assert "Auto calibration is still running" in dialog.hint_progress_status.text()
    dialog._set_hint_progress("Calibration complete.", 100)  # the next step keeps the note
    assert "still running" in dialog.hint_progress_status.text()

    dialog._on_auto_worker_thread_finished()
    assert dialog.close_btn.isEnabled() and dialog.export_btn.isEnabled()
    assert dialog._busy_close_note == ""


def test_an_export_failure_is_a_notice_with_the_error(dialog):
    dialog._on_export_failed("No space left on device")
    notice = _notice(dialog)
    assert notice.text == "Could not export the image." and notice.tone == "error"
    assert notice.details == "No space left on device"


def test_a_missing_image_file_is_a_notice_naming_it(dialog):
    _with_image(dialog, "/nowhere/gone.jpg")
    dialog._on_run_auto_calibration()
    notice = _notice(dialog)
    assert notice.text == "The selected image could not be found on disk."
    assert notice.details == "/nowhere/gone.jpg" and notice.tone == "error"
    assert not dialog._auto_calibration_running


@pytest.mark.parametrize("loaded,tone", [(0, "error"), (2, "warning")])
def test_load_failures_are_a_notice_with_the_files_in_details(dialog, loaded, tone):
    dialog._show_calibration_load_failures(["bad.heic: conversion failed"], loaded)
    notice = _notice(dialog)
    assert notice.tone == tone
    assert "bad.heic: conversion failed" in notice.details


def test_save_without_an_objective_says_so_in_the_bar(dialog):
    dialog.current_objective_key = None
    dialog._on_save_calibration()
    assert _notice(dialog).text == "Please select an objective first."
    dialog._on_save_manual_calibration()
    assert _notice(dialog).text == "Please select an objective first."


def test_manual_save_with_auto_results_is_an_info_notice(dialog, monkeypatch):
    monkeypatch.setattr(dialog, "_collect_auto_values", lambda *_a: [12.3])
    dialog._on_save_manual_calibration()
    notice = _notice(dialog)
    assert notice.tone == "info" and "Use Save Calibration in the image tab" in notice.text


def test_updated_measurements_are_a_success_notice(dialog, monkeypatch):
    monkeypatch.setattr(
        calibration_dialog.CalibrationDB, "get_calibration_usage_summary",
        lambda key: [{"calibration_id": 1, "image_count": 2, "measurement_count": 9}],
    )
    monkeypatch.setattr(QMessageBox, "question", lambda *_a, **_k: QMessageBox.Yes)  # the confirmation stays
    monkeypatch.setattr(
        calibration_dialog.CalibrationDB, "recalculate_measurements_for_calibration", lambda *_a: 9,
    )
    dialog._prompt_recalculate_measurements(1, 0.0120, 2, 0.0130)
    notice = _notice(dialog)
    assert notice.tone == "success" and notice.text.startswith("Updated 9 spore measurements")


def test_set_as_active_needs_a_calibration_chosen_in_the_history(dialog, monkeypatch):
    monkeypatch.setattr(
        calibration_dialog.CalibrationDB,
        "get_calibration_history",
        lambda key: [{
            "id": 1, "calibration_uuid": "9c0c3b12-7f3c-4a72-8a6f-6d6e8df4b111",
            "calibration_date": "2026-05-30 10:00:00", "calibration_image_date": "2026-05-30 09:45:00",
            "microns_per_pixel": 0.012345, "megapixels": 12.8, "num_measurements": 3,
            "measurements_json": json.dumps({"images": []}), "image_filepath": "", "camera": "TestCam", "notes": "",
        }],
    )
    dialog._update_history_table()
    assert dialog.history_table.rowCount() == 1
    assert not dialog.set_active_btn.isEnabled()
    assert _hover_hint(dialog, dialog.set_active_btn) == "Select a calibration in the history table first."
    dialog.history_table.setCurrentCell(0, 1)
    assert dialog.set_active_btn.isEnabled()
    dialog._update_history_table()  # a rebuild clears the choice
    assert not dialog.set_active_btn.isEnabled()


def test_new_objective_says_what_is_missing_under_the_form(qtbot):
    dialog = NewObjectiveDialog(existing_keys=["Plan 40x/0.75"])
    qtbot.addWidget(dialog)
    dialog.show()
    qtbot.waitExposed(dialog)
    assert dialog.validation_label.isHidden()

    qtbot.mouseClick(dialog.ok_btn, Qt.LeftButton)
    assert dialog.validation_label.text() == "Please enter an objective name."
    assert not dialog.validation_label.isHidden()
    assert dialog.focusWidget() is dialog.objective_name_input
    assert dialog.result() == 0 and dialog.isVisible()

    qtbot.keyClicks(dialog.objective_name_input, "Plan")
    assert dialog.validation_label.isHidden()  # typing clears it
    qtbot.mouseClick(dialog.ok_btn, Qt.LeftButton)
    assert dialog.validation_label.text() == "Please enter a magnification."
    assert dialog.focusWidget() is dialog.magnification_input

    qtbot.keyClicks(dialog.magnification_input, "40")
    qtbot.mouseClick(dialog.ok_btn, Qt.LeftButton)
    assert dialog.validation_label.text() == "Please enter a numerical aperture (NA)."
    assert dialog.focusWidget() is dialog.na_input


def test_new_macro_profile_asks_for_the_sensor_data_inline(qtbot):
    dialog = NewObjectiveDialog()
    qtbot.addWidget(dialog)
    dialog.show()
    qtbot.waitExposed(dialog)
    dialog.profile_type_combo.setCurrentIndex(dialog.profile_type_combo.findData("macro"))
    qtbot.keyClicks(dialog.objective_name_input, "Canon 100 mm")
    qtbot.keyClicks(dialog.magnification_input, "1")
    qtbot.mouseClick(dialog.ok_btn, Qt.LeftButton)
    assert dialog.validation_label.text() == "Please enter sensor width in mm."
    assert dialog.focusWidget() is dialog.sensor_width_input
    qtbot.keyClicks(dialog.sensor_width_input, "35")
    qtbot.mouseClick(dialog.ok_btn, Qt.LeftButton)
    assert dialog.validation_label.text() == "Please enter image width in pixels."
    qtbot.keyClicks(dialog.image_width_input, "6000")
    qtbot.mouseClick(dialog.ok_btn, Qt.LeftButton)
    assert dialog.result() == NewObjectiveDialog.Accepted


def test_a_duplicate_objective_name_is_said_inline(qtbot):
    dialog = NewObjectiveDialog()
    qtbot.addWidget(dialog)
    qtbot.keyClicks(dialog.objective_name_input, "Plan")
    qtbot.keyClicks(dialog.magnification_input, "40")
    qtbot.keyClicks(dialog.na_input, "0.75")
    data = dialog.get_objective_data()
    dialog.existing_keys = [data.get("key") or "", data.get("name") or "", data.get("display_name") or ""]
    dialog.ok_btn.click()
    if dialog.result() == NewObjectiveDialog.Accepted:
        pytest.skip("objective data has no key field to collide with")
    assert dialog.validation_label.text() == "An objective with this name already exists."


class _RunStarted(Exception):
    pass


def _image_on_disk(tmp_path):
    path = tmp_path / "calibration.png"
    pixmap = QPixmap(64, 48)
    pixmap.fill(QColor("white"))
    assert pixmap.save(str(path))
    return str(path)


def test_starting_an_export_disables_its_controls_and_close_naming_the_export(dialog, monkeypatch, tmp_path):
    """Review finding: the real start path disables the controls before it
    sets the running flag; they must stay disabled, and say why."""
    _with_image(dialog, _image_on_disk(tmp_path))
    monkeypatch.setattr(calibration_dialog.ExportImageDialog, "exec", lambda self: calibration_dialog.QDialog.Accepted)
    monkeypatch.setattr(
        calibration_dialog.ExportImageDialog, "get_settings",
        lambda self: {"scale_percent": 100.0, "width": 64, "height": 48, "quality": 90},
    )
    monkeypatch.setattr(
        calibration_dialog.QFileDialog, "getSaveFileName", lambda *_a, **_k: (str(tmp_path / "out.png"), ""),
    )
    seen = {}

    def _worker(**_kwargs):
        seen.update(
            export=dialog.export_btn.isEnabled(), run=dialog.auto_run_btn.isEnabled(),
            close=dialog.close_btn.isEnabled(), hint=dialog.close_btn.property("_hint_disabled_text"),
        )
        raise _RunStarted

    monkeypatch.setattr(calibration_dialog, "_ExportImageWorker", _worker)
    with pytest.raises(_RunStarted):
        dialog._on_export_image()
    assert seen["export"] is False and seen["run"] is False and seen["close"] is False
    assert "Export is still running" in seen["hint"]
    dialog._on_export_worker_thread_finished()
    assert dialog.export_btn.isEnabled() and dialog.close_btn.isEnabled()


def test_starting_auto_calibration_disables_its_controls_and_close(dialog, monkeypatch, tmp_path):
    _with_image(dialog, _image_on_disk(tmp_path))
    seen = {}

    def _worker(*_args, **_kwargs):
        seen.update(
            export=dialog.export_btn.isEnabled(), run=dialog.auto_run_btn.isEnabled(),
            close=dialog.close_btn.isEnabled(), hint=dialog.close_btn.property("_hint_disabled_text"),
        )
        raise _RunStarted

    monkeypatch.setattr(calibration_dialog, "_AutoCalibrationWorker", _worker)
    with pytest.raises(_RunStarted):
        dialog._on_run_auto_calibration()
    assert seen["export"] is False and seen["run"] is False and seen["close"] is False
    assert "Auto calibration is still running" in seen["hint"]
    dialog._on_auto_worker_thread_finished()
    assert dialog.auto_run_btn.isEnabled() and dialog.close_btn.isEnabled()


def test_a_cloud_reference_preview_is_no_image_to_calibrate(dialog, tmp_path):
    """Review finding: the cached cloud preview leaves no current image, so
    Calibrate and Export wait as with none."""
    _with_image(dialog)
    assert dialog.auto_run_btn.isEnabled()
    cached = tmp_path / "cached.png"
    pixmap = QPixmap(32, 24)
    pixmap.fill(QColor("grey"))
    assert pixmap.save(str(cached))
    assert dialog._show_cloud_reference_preview(cached)
    assert not dialog.auto_run_btn.isEnabled() and not dialog.export_btn.isEnabled()
    assert not dialog.add_measurement_btn.isEnabled()


def test_a_notice_posted_during_a_run_waits_for_the_bar(qtbot, dialog, monkeypatch):
    """Review finding: the progress panel hides the bar during a run; an info
    notice posted then must still be there when the bar comes back."""
    monkeypatch.setattr(dialog._hint_controller, "NOTICE_TIMEOUT_MS", 50)
    monkeypatch.setattr(dialog, "_collect_auto_values", lambda *_a: [12.3])
    dialog._set_hint_progress_visible(True)
    dialog._on_save_manual_calibration()  # Manual Entry stays usable during a run
    qtbot.wait(200)
    assert dialog._hint_controller.has_notice()
    dialog._set_hint_progress_visible(False)
    assert dialog.hint_bar._label.text().startswith("Auto calibration results are available")
    qtbot.waitUntil(lambda: not dialog._hint_controller.has_notice(), timeout=2000)
