import json
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication

from ui import calibration_dialog
from ui.calibration_dialog import CalibrationDialog


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _build_dialog(monkeypatch) -> CalibrationDialog:
    monkeypatch.setattr(calibration_dialog, "load_objectives", lambda: {})
    monkeypatch.setattr(
        calibration_dialog.SettingsDB,
        "get_setting",
        lambda key, default=None: default,
    )
    monkeypatch.setattr(
        calibration_dialog.CalibrationDB,
        "get_calibration_history",
        lambda objective_key: [],
    )
    monkeypatch.setattr(
        calibration_dialog.CalibrationDB,
        "get_calibration_usage_summary",
        lambda objective_key: [],
    )

    dialog = CalibrationDialog()
    dialog.current_objective_key = "objective_1"
    dialog.objectives = {"objective_1": {"na": 0.75, "microns_per_pixel": 0.012345}}
    return dialog


def test_history_columns_show_full_timestamp_text(qapp, monkeypatch):
    dialog = _build_dialog(monkeypatch)

    assert dialog.history_table.columnWidth(0) >= 170
    assert dialog.history_table.columnWidth(1) >= 170

    dialog.deleteLater()


def test_history_row_click_uses_automatic_tab_for_stored_auto_results(qapp, monkeypatch):
    dialog = _build_dialog(monkeypatch)
    calibration_id = 42

    calibration_record = {
        "id": calibration_id,
        "objective_key": "objective_1",
        "calibration_date": "2026-05-29 12:34:56",
        "calibration_image_date": "2026-05-29 11:22:33",
        "microns_per_pixel": 0.012250,
        "microns_per_pixel_std": 0.000120,
        "confidence_interval_low": 0.0100,
        "confidence_interval_high": 0.0140,
        "num_measurements": 2,
        "measurements_json": json.dumps(
            {
                "images": [],
                "measurements": [],
                "auto_images": [
                    {
                        "index": 0,
                        "path": "",
                        "spacing_um": 0.1,
                        "result": {
                            "axis": "horizontal",
                            "angle_deg": 3.0,
                            "spacing_median_px": 12.0,
                            "spacing_median_edges_px": 12.0,
                            "nm_per_px": 12.0,
                            "nm_per_px_edges": 12.0,
                            "agreement_pct": 99.0,
                            "rel_scatter_mad_pct": 1.0,
                            "rel_scatter_iqr_pct": 1.5,
                            "drift_slope": 0.001,
                            "residual_slope_deg": 0.2,
                            "edges_px": [],
                        },
                    },
                    {
                        "index": 1,
                        "path": "",
                        "spacing_um": 0.1,
                        "result": {
                            "axis": "horizontal",
                            "angle_deg": 5.0,
                            "spacing_median_px": 12.5,
                            "spacing_median_edges_px": 12.5,
                            "nm_per_px": 12.5,
                            "nm_per_px_edges": 12.5,
                            "agreement_pct": 98.5,
                            "rel_scatter_mad_pct": 2.0,
                            "rel_scatter_iqr_pct": 2.5,
                            "drift_slope": 0.003,
                            "residual_slope_deg": 0.4,
                            "edges_px": [],
                        },
                    },
                ],
                "auto_summary": {
                    "method": "edges",
                    "average_nm_per_px": 12.25,
                    "max_deviation_nm_per_px": 0.25,
                    "n_images": 2,
                },
            }
        ),
        "image_filepath": "",
        "image_storage_path": "",
        "notes": "Historical calibration",
    }

    monkeypatch.setattr(
        calibration_dialog.CalibrationDB,
        "get_calibration",
        lambda selected_id: calibration_record if selected_id == calibration_id else None,
    )

    dialog._history_calibration_ids = [calibration_id]
    dialog.history_table.setRowCount(1)
    dialog.tab_widget.setCurrentIndex(1)
    dialog.image_mode_tabs.setCurrentIndex(1)

    dialog.history_table.cellClicked.emit(0, 0)

    assert dialog.tab_widget.currentIndex() == 0
    assert dialog.image_mode_tabs.currentIndex() == 0
    assert dialog.auto_scale_title.text() == "Scale (average):"
    assert dialog.auto_scale_label.text() == "12.25 nm/px"
    assert dialog.auto_scatter_mad_label.text() == "1.50%"
    assert dialog.auto_scatter_iqr_label.text() == "2.00%"
    assert dialog.auto_residual_label.text() == "0.300 deg"
    assert dialog.auto_drift_label.text() == "0.002 px/px"
    assert dialog.auto_angle_label.text() == "4.000 deg"
    assert dialog.auto_dev_label.text() == "+/-0.25 nm/px"
    assert dialog.auto_spread_label.text() == "2.04%"
    assert "stored automatic calibration" in dialog.hint_bar._label.text().lower()
    assert "missing" in dialog.cloud_reference_status_label.text().lower() or "photo" in dialog.cloud_reference_status_label.text().lower()

    dialog.deleteLater()


def test_history_row_click_keeps_manual_tab_for_manual_rows(qapp, monkeypatch):
    dialog = _build_dialog(monkeypatch)
    calibration_id = 84

    calibration_record = {
        "id": calibration_id,
        "objective_key": "objective_1",
        "calibration_date": "2026-05-29 12:34:56",
        "calibration_image_date": "2026-05-29 11:22:33",
        "microns_per_pixel": 0.012345,
        "microns_per_pixel_std": 0.000123,
        "confidence_interval_low": 0.0100,
        "confidence_interval_high": 0.0140,
        "num_measurements": 4,
        "measurements_json": json.dumps({"images": []}),
        "image_filepath": "",
        "image_storage_path": "",
        "notes": "Historical manual calibration",
    }

    monkeypatch.setattr(
        calibration_dialog.CalibrationDB,
        "get_calibration",
        lambda selected_id: calibration_record if selected_id == calibration_id else None,
    )

    dialog._history_calibration_ids = [calibration_id]
    dialog.history_table.setRowCount(1)
    dialog.tab_widget.setCurrentIndex(1)
    dialog.image_mode_tabs.setCurrentIndex(0)

    dialog.history_table.cellClicked.emit(0, 0)

    assert dialog.tab_widget.currentIndex() == 0
    assert dialog.image_mode_tabs.currentIndex() == 1
    assert dialog.result_average_label.text() == "12.35 nm/px"
    assert dialog.result_std_label.text() == "+/-0.12 nm/px"
    assert dialog.result_ci_label.text() == "[10.00, 14.00]"
    assert dialog.result_count_label.text() == "4"

    dialog.deleteLater()
