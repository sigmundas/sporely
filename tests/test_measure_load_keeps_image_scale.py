"""Opening an image in the Measure tab never rewrites its scale or calibration link.

An image's measurements were computed at its stored scale, and a re-calibration
may have deliberately left it on an earlier calibration ("New calibration only"
or "Update recent images"). Loading keeps the stored scale for display and
measuring, whatever its distance from the objective's current scale. A scale
change is an explicit action through ``apply_objective``, which offers to
rescale the measurements.

The real ``MainWindow.apply_image_scale`` runs on a real ``MainWindow`` (with
``init_ui`` neutralized and the widgets it touches built for real) against a
temporary SQLite database.
"""

from __future__ import annotations

import os
import sqlite3
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QComboBox, QLabel

from database import models, schema
import ui.main_window as main_window
from ui.zoomable_image_widget import ZoomableImageLabel


OBJECTIVE_KEY = "Leica_40x"
OLD_SCALE = 0.1000  # the previous calibration
NEW_SCALE = 0.1005  # the active calibration: 0.5% larger
WARNING = "Warning: Older calibration standard used."


@pytest.fixture
def real_db(tmp_path, monkeypatch):
    db_path = tmp_path / "sporely.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: tmp_path / "ref.db")
    monkeypatch.setattr(schema, "get_bundled_reference_database_path", lambda: tmp_path / "bundled.db")
    monkeypatch.setattr(schema, "_migrate_reference_values", lambda *a, **k: None)
    monkeypatch.setattr(schema, "_migrate_reference_mounts_and_stains", lambda *a, **k: None)
    schema.init_database()
    return db_path


def _sql(db_path, query, params=()):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(query, params).fetchall()
        conn.commit()
        return rows
    finally:
        conn.close()


class _NoSpeciesAvailability:
    def __init__(self, *args, **kwargs):
        pass


class _AcceptingMessageBox:
    """The rescale prompt, answered with OK."""

    Question = 0
    AcceptRole = 0
    RejectRole = 1
    shown = 0

    def __init__(self, *_a, **_k):
        self._buttons = []

    def setIcon(self, *_a):
        pass

    def setWindowTitle(self, *_a):
        pass

    def setText(self, *_a):
        pass

    def addButton(self, *_a):
        button = object()
        self._buttons.append(button)
        return button

    def exec(self):
        type(self).shown += 1
        return 0

    def clickedButton(self):
        return self._buttons[0]


@pytest.fixture
def window(real_db, qtbot, monkeypatch):
    monkeypatch.setattr(main_window, "SpeciesDataAvailability", _NoSpeciesAvailability)
    monkeypatch.setattr(main_window.MainWindow, "_apply_theme", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "init_ui", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "load_default_objective", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_restore_geometry", lambda self: None)
    objectives = {
        OBJECTIVE_KEY: {"name": "40x", "magnification": 40, "microns_per_pixel": NEW_SCALE},
    }
    monkeypatch.setattr(
        main_window.MainWindow, "load_objective_definitions", lambda self: {k: dict(v) for k, v in objectives.items()}
    )
    win = main_window.MainWindow()
    qtbot.addWidget(win)
    # The Measure-tab widgets apply_image_scale updates.
    win.scale_combo = QComboBox()
    win.calib_info_label = QLabel()
    win.measure_status_label = QLabel()
    win.image_label = ZoomableImageLabel()
    win.load_measurement_lines = lambda: None
    win.update_measurements_table = lambda: None
    win.update_statistics = lambda: None
    return win


@pytest.fixture
def scene(real_db):
    old_cal = models.CalibrationDB.add_calibration(
        objective_key=OBJECTIVE_KEY, microns_per_pixel=OLD_SCALE, calibration_date="2026-06-01"
    )
    new_cal = models.CalibrationDB.add_calibration(
        objective_key=OBJECTIVE_KEY, microns_per_pixel=NEW_SCALE, calibration_date="2026-11-21"
    )
    assert models.CalibrationDB.get_active_calibration_id(OBJECTIVE_KEY) == new_cal
    return SimpleNamespace(db=real_db, old_cal=old_cal, new_cal=new_cal)


def _add_image(db_path, *, scale, calibration_id, objective_name=OBJECTIVE_KEY, resample_factor=None):
    """An observation with one microscope image and a 10 x 4 µm spore."""
    obs_id = models.ObservationDB.create_observation(date="2026-10-01", genus="Russula", species="test")
    image_id = models.ImageDB.add_image(
        observation_id=obs_id,
        filepath="/nonexistent/spore.jpg",
        image_type="microscope",
        scale=scale,
        objective_name=objective_name,
        calibration_id=calibration_id,
    )
    if resample_factor is not None:
        models.ImageDB.update_image(image_id, resample_scale_factor=resample_factor)
    spore_id = models.MeasurementDB.add_measurement(image_id=image_id, length=10.0, width=4.0, notes="Q=2.5")
    # Cloud-linked and synced: any image write, even of the same values, marks it dirty.
    _sql(
        db_path,
        "UPDATE observations SET cloud_id = ?, sync_status = 'synced' WHERE id = ?",
        (f"cloud-obs-{obs_id}", obs_id),
    )
    return SimpleNamespace(obs_id=obs_id, image_id=image_id, spore_id=spore_id)


def _state(db_path, item):
    image = _sql(
        db_path,
        "SELECT scale_microns_per_pixel, calibration_id, objective_name FROM images WHERE id = ?",
        (item.image_id,),
    )[0]
    spore = _sql(db_path, "SELECT length_um, width_um FROM spore_measurements WHERE id = ?", (item.spore_id,))[0]
    sync = _sql(db_path, "SELECT sync_status FROM observations WHERE id = ?", (item.obs_id,))[0][0]
    return {
        "scale": image["scale_microns_per_pixel"],
        "calibration_id": image["calibration_id"],
        "objective_name": image["objective_name"],
        "length": spore["length_um"],
        "width": spore["width_um"],
        "sync": sync,
    }


def _open_in_measure(win, item):
    win.current_image_id = item.image_id
    win.current_image_type = "microscope"
    win.apply_image_scale(models.ImageDB.get_image(item.image_id))


def test_image_half_a_percent_off_keeps_its_scale_and_calibration(window, scene):
    item = _add_image(scene.db, scale=OLD_SCALE, calibration_id=scene.old_cal)
    before = _state(scene.db, item)

    _open_in_measure(window, item)

    assert _state(scene.db, item) == before
    assert window.microns_per_pixel == pytest.approx(OLD_SCALE)
    assert window.image_label.microns_per_pixel == pytest.approx(OLD_SCALE)
    assert window.current_objective_name == OBJECTIVE_KEY
    assert window.scale_combo.currentData() == OBJECTIVE_KEY
    # The calibration shown is the image's own, not the active one.
    assert window.current_calibration_id == scene.old_cal
    assert "2026-06-01" in window.calib_info_label.text()
    # Within 1%: no warning.
    assert window.measure_status_label.text() == ""


def test_image_more_than_one_percent_off_keeps_its_scale_and_warns(window, scene):
    # apply_image_scale sets the warning; opening from the Observations tab
    # clears the status label afterwards (pre-existing), so it is seen only
    # when moving between images inside the Measure tab.
    item = _add_image(scene.db, scale=0.0950, calibration_id=scene.old_cal)
    before = _state(scene.db, item)

    _open_in_measure(window, item)

    assert _state(scene.db, item) == before
    assert window.microns_per_pixel == pytest.approx(0.0950)
    assert window.current_calibration_id == scene.old_cal
    assert window.measure_status_label.text() == WARNING


def test_resampled_image_keeps_its_scale(window, scene):
    # Resampled to half size: the expected scale is twice the objective's.
    item = _add_image(scene.db, scale=2 * OLD_SCALE, calibration_id=scene.old_cal, resample_factor=0.5)
    before = _state(scene.db, item)

    _open_in_measure(window, item)

    assert _state(scene.db, item) == before
    assert window.microns_per_pixel == pytest.approx(2 * OLD_SCALE)
    assert window.current_calibration_id == scene.old_cal
    assert window.measure_status_label.text() == ""


def test_legacy_objective_name_is_normalized_without_relinking(window, scene):
    item = _add_image(scene.db, scale=OLD_SCALE, calibration_id=scene.old_cal, objective_name="40x")

    _open_in_measure(window, item)

    after = _state(scene.db, item)
    assert after["objective_name"] == OBJECTIVE_KEY
    assert after["scale"] == pytest.approx(OLD_SCALE)
    assert after["calibration_id"] == scene.old_cal
    assert (after["length"], after["width"]) == pytest.approx((10.0, 4.0))
    assert window.microns_per_pixel == pytest.approx(OLD_SCALE)


def test_warning_from_previous_image_is_cleared(window, scene):
    far = _add_image(scene.db, scale=0.0950, calibration_id=scene.old_cal)
    near = _add_image(scene.db, scale=OLD_SCALE, calibration_id=scene.old_cal)

    _open_in_measure(window, far)
    assert window.measure_status_label.text() == WARNING
    _open_in_measure(window, near)

    assert window.measure_status_label.text() == ""


def test_choosing_the_objective_afterwards_rescales_through_the_prompt(window, scene, monkeypatch):
    """The explicit action still moves the image to the active calibration, with a rescale."""
    monkeypatch.setattr(main_window, "QMessageBox", _AcceptingMessageBox)
    _AcceptingMessageBox.shown = 0
    item = _add_image(scene.db, scale=OLD_SCALE, calibration_id=scene.old_cal)
    _open_in_measure(window, item)

    objective = dict(window.load_objective_definitions()[OBJECTIVE_KEY], key=OBJECTIVE_KEY)
    assert window.apply_objective(objective) is True

    assert _AcceptingMessageBox.shown == 1
    after = _state(scene.db, item)
    assert after["scale"] == pytest.approx(NEW_SCALE)
    assert after["calibration_id"] == scene.new_cal
    ratio = NEW_SCALE / OLD_SCALE
    assert (after["length"], after["width"]) == pytest.approx((10.0 * ratio, 4.0 * ratio))
    assert window.microns_per_pixel == pytest.approx(NEW_SCALE)


def test_image_without_a_scale_takes_the_objective_scale(window, scene):
    """A first fill, not a rewrite: there is no stored scale to keep."""
    item = _add_image(scene.db, scale=None, calibration_id=None)

    _open_in_measure(window, item)

    after = _state(scene.db, item)
    assert after["scale"] == pytest.approx(NEW_SCALE)
    assert after["calibration_id"] == scene.new_cal
    assert window.microns_per_pixel == pytest.approx(NEW_SCALE)
