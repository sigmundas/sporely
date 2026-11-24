"""Re-choosing the shown objective offers to move the image to its active calibration.

Opening an image in Measure keeps its stored scale and calibration link, so an
image on an earlier calibration of its objective stays there. Choosing that same
objective again (in the scale combo or the image-tag Objective menu) asks
whether to move the image to the objective's active calibration, naming both
calibrations and the change to its measurements; Move rescales the measurements
by new ÷ old and links the active calibration. On an image already on the active
calibration, re-choosing does nothing.

The scale combo is a real ``QComboBox`` wired by the production
``_connect_scale_combo_signals`` and driven through its popup with key presses,
on a real ``MainWindow`` (``init_ui`` neutralized, the Measure widgets the
handlers touch built for real) against a temporary SQLite database. Only the
message box is replaced, to answer it and record what it asked.
"""

from __future__ import annotations

import os
import sqlite3
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtTest import QTest
from PySide6.QtWidgets import QComboBox, QLabel, QMenu

from database import models, schema
import ui.main_window as main_window
from ui.zoomable_image_widget import ZoomableImageLabel


OBJECTIVE_KEY = "Leica_40x"
OTHER_KEY = "Leica_100x"
OLD_SCALE = 0.1000  # the previous 40x calibration
NEW_SCALE = 0.1005  # the active 40x calibration: 0.5% larger
OTHER_SCALE = 0.0400
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


class _RecordingMessageBox:
    """A question box answered with its first button (Move/OK) or Cancel.

    Like a real modal box, ``exec`` runs the event loop while it is "open", so
    timers posted before it fire inside it.
    """

    Question = 0
    AcceptRole = 0
    RejectRole = 1
    answer_accept = True
    asked: list[tuple[str, str]] = []
    while_open = None

    def __init__(self, *_a, **_k):
        self._buttons = []
        self._title = ""
        self._text = ""

    def setIcon(self, *_a):
        pass

    def setWindowTitle(self, title):
        self._title = title

    def setText(self, text):
        self._text = text

    def addButton(self, *_a):
        button = object()
        self._buttons.append(button)
        return button

    def exec(self):
        type(self).asked.append((self._title, self._text))
        QTest.qWait(20)
        if type(self).while_open is not None:
            type(self).while_open()
        return 0

    def clickedButton(self):
        return self._buttons[0] if type(self).answer_accept else self._buttons[-1]


@pytest.fixture
def box(monkeypatch):
    monkeypatch.setattr(main_window, "QMessageBox", _RecordingMessageBox)
    _RecordingMessageBox.asked = []
    _RecordingMessageBox.answer_accept = True
    _RecordingMessageBox.while_open = None
    return _RecordingMessageBox


@pytest.fixture
def window(real_db, qtbot, monkeypatch):
    monkeypatch.setattr(main_window, "SpeciesDataAvailability", _NoSpeciesAvailability)
    monkeypatch.setattr(main_window.MainWindow, "_apply_theme", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "init_ui", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "load_default_objective", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_restore_geometry", lambda self: None)
    objectives = {
        OBJECTIVE_KEY: {"name": "40x", "magnification": 40, "microns_per_pixel": NEW_SCALE},
        OTHER_KEY: {"name": "100x", "magnification": 100, "microns_per_pixel": OTHER_SCALE},
    }
    monkeypatch.setattr(
        main_window.MainWindow, "load_objective_definitions", lambda self: {k: dict(v) for k, v in objectives.items()}
    )
    win = main_window.MainWindow()
    qtbot.addWidget(win)
    # The Measure-tab widgets the scale handlers update; the combo is wired by production code.
    win.scale_combo = QComboBox()
    qtbot.addWidget(win.scale_combo)
    win._connect_scale_combo_signals()
    win.scale_combo.show()
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
    other_cal = models.CalibrationDB.add_calibration(
        objective_key=OTHER_KEY, microns_per_pixel=OTHER_SCALE, calibration_date="2026-10-01"
    )
    assert models.CalibrationDB.get_active_calibration_id(OBJECTIVE_KEY) == new_cal
    return SimpleNamespace(db=real_db, old_cal=old_cal, new_cal=new_cal, other_cal=other_cal)


def _add_image(db_path, *, scale, calibration_id, with_measurement=True):
    """An observation with one microscope image and, optionally, a 10 x 4 µm spore."""
    obs_id = models.ObservationDB.create_observation(date="2026-10-01", genus="Russula", species="test")
    image_id = models.ImageDB.add_image(
        observation_id=obs_id,
        filepath="/nonexistent/spore.jpg",
        image_type="microscope",
        scale=scale,
        objective_name=OBJECTIVE_KEY,
        calibration_id=calibration_id,
    )
    spore_id = None
    if with_measurement:
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
    state = {
        "scale": image["scale_microns_per_pixel"],
        "calibration_id": image["calibration_id"],
        "objective_name": image["objective_name"],
        "sync": _sql(db_path, "SELECT sync_status FROM observations WHERE id = ?", (item.obs_id,))[0][0],
    }
    if item.spore_id is not None:
        spore = _sql(db_path, "SELECT length_um, width_um FROM spore_measurements WHERE id = ?", (item.spore_id,))[0]
        state["length"] = spore["length_um"]
        state["width"] = spore["width_um"]
    return state


def _open_in_measure(win, item):
    win.current_image_id = item.image_id
    win.current_image_type = "microscope"
    win.apply_image_scale(models.ImageDB.get_image(item.image_id))
    assert win.scale_combo.currentData() == OBJECTIVE_KEY


def _choose_in_combo(qtbot, combo, key):
    """Open the combo's popup, move to ``key`` with the arrow keys and press Return."""
    combo.showPopup()
    view = combo.view()
    qtbot.waitUntil(view.isVisible)
    target = combo.findData(key)
    assert target >= 0
    steps = target - combo.currentIndex()
    for _ in range(abs(steps)):
        qtbot.keyClick(view, Qt.Key_Down if steps > 0 else Qt.Key_Up)
    assert view.currentIndex().row() == target
    qtbot.keyClick(view, Qt.Key_Return)
    qtbot.waitUntil(lambda: not view.isVisible())


def _assert_moved_to_active(window, scene, item):
    after = _state(scene.db, item)
    assert after["scale"] == pytest.approx(NEW_SCALE)
    assert after["calibration_id"] == scene.new_cal
    assert after["objective_name"] == OBJECTIVE_KEY
    ratio = NEW_SCALE / OLD_SCALE
    assert (after["length"], after["width"]) == pytest.approx((10.0 * ratio, 4.0 * ratio))
    assert window.microns_per_pixel == pytest.approx(NEW_SCALE)
    assert window.current_calibration_id == scene.new_cal
    assert "2026-11-21" in window.calib_info_label.text()


def test_rechoosing_the_objective_moves_the_image_to_the_active_calibration(window, scene, box, qtbot):
    item = _add_image(scene.db, scale=OLD_SCALE, calibration_id=scene.old_cal)
    _open_in_measure(window, item)
    assert window.current_calibration_id == scene.old_cal

    _choose_in_combo(qtbot, window.scale_combo, OBJECTIVE_KEY)

    # One question, naming both calibrations and the change; no second rescale prompt.
    assert box.asked == [
        (
            "Use active calibration",
            "Move this image from the 2026-06-01 calibration to the active 2026-11-21 calibration? "
            "The image scale changes by +0.5%.",
        )
    ]
    _assert_moved_to_active(window, scene, item)
    assert window.scale_combo.currentData() == OBJECTIVE_KEY


def test_rechoosing_clears_the_older_calibration_warning(window, scene, box, qtbot):
    far_scale = 0.0950  # more than 1% off: opening it shows the warning
    item = _add_image(scene.db, scale=far_scale, calibration_id=scene.old_cal)
    _open_in_measure(window, item)
    assert window.measure_status_label.text() == WARNING

    _choose_in_combo(qtbot, window.scale_combo, OBJECTIVE_KEY)

    assert len(box.asked) == 1
    assert "The image scale changes by +5.8%." in box.asked[0][1]
    assert _state(scene.db, item)["scale"] == pytest.approx(NEW_SCALE)
    assert window.measure_status_label.text() == ""


def test_cancel_leaves_the_image_on_its_calibration(window, scene, box, qtbot):
    box.answer_accept = False
    item = _add_image(scene.db, scale=OLD_SCALE, calibration_id=scene.old_cal)
    _open_in_measure(window, item)
    before = _state(scene.db, item)

    _choose_in_combo(qtbot, window.scale_combo, OBJECTIVE_KEY)

    assert len(box.asked) == 1
    assert _state(scene.db, item) == before
    assert window.microns_per_pixel == pytest.approx(OLD_SCALE)
    assert window.current_calibration_id == scene.old_cal


def test_rechoosing_on_the_active_calibration_does_nothing(window, scene, box, qtbot):
    item = _add_image(scene.db, scale=NEW_SCALE, calibration_id=scene.new_cal)
    _open_in_measure(window, item)
    before = _state(scene.db, item)

    _choose_in_combo(qtbot, window.scale_combo, OBJECTIVE_KEY)

    assert box.asked == []
    assert _state(scene.db, item) == before  # no write: the observation stays synced
    assert before["sync"] == "synced"
    assert window.microns_per_pixel == pytest.approx(NEW_SCALE)


def test_image_without_measurements_is_asked_without_a_change_line(window, scene, box, qtbot):
    item = _add_image(scene.db, scale=OLD_SCALE, calibration_id=scene.old_cal, with_measurement=False)
    _open_in_measure(window, item)

    _choose_in_combo(qtbot, window.scale_combo, OBJECTIVE_KEY)

    assert box.asked == [
        ("Use active calibration", "Move this image from the 2026-06-01 calibration to the active 2026-11-21 calibration?")
    ]
    after = _state(scene.db, item)
    assert after["scale"] == pytest.approx(NEW_SCALE)
    assert after["calibration_id"] == scene.new_cal


def test_cancelled_switch_to_another_objective_is_not_read_as_a_rechoice(window, scene, box, qtbot):
    """Qt emits activated() after a change the rescale prompt reverts; that must not ask to move."""
    box.answer_accept = False
    item = _add_image(scene.db, scale=OLD_SCALE, calibration_id=scene.old_cal)
    _open_in_measure(window, item)
    before = _state(scene.db, item)

    _choose_in_combo(qtbot, window.scale_combo, OTHER_KEY)

    # Only the existing rescale prompt for the switch; Cancel reverts to 40x.
    assert [title for title, _text in box.asked] == ["Changing image scale"]
    assert window.scale_combo.currentData() == OBJECTIVE_KEY
    assert _state(scene.db, item) == before

    # Once that choice has been delivered, re-choosing 40x asks again as usual.
    qtbot.wait(10)
    box.asked.clear()
    box.answer_accept = True
    _choose_in_combo(qtbot, window.scale_combo, OBJECTIVE_KEY)
    assert [title for title, _text in box.asked] == ["Use active calibration"]
    _assert_moved_to_active(window, scene, item)


def test_move_is_dropped_when_the_active_calibration_changed_meanwhile(window, scene, box, qtbot):
    item = _add_image(scene.db, scale=OLD_SCALE, calibration_id=scene.old_cal)
    _open_in_measure(window, item)
    before = _state(scene.db, item)
    box.while_open = lambda: models.CalibrationDB.set_active_calibration(scene.old_cal)

    _choose_in_combo(qtbot, window.scale_combo, OBJECTIVE_KEY)

    assert len(box.asked) == 1
    assert _state(scene.db, item) == before


def test_move_is_dropped_when_another_image_opened_meanwhile(window, scene, box, qtbot):
    first = _add_image(scene.db, scale=OLD_SCALE, calibration_id=scene.old_cal)
    second = _add_image(scene.db, scale=OLD_SCALE, calibration_id=scene.old_cal)
    _open_in_measure(window, first)
    before = (_state(scene.db, first), _state(scene.db, second))
    box.while_open = lambda: _open_in_measure(window, second)

    _choose_in_combo(qtbot, window.scale_combo, OBJECTIVE_KEY)

    assert len(box.asked) == 1
    assert (_state(scene.db, first), _state(scene.db, second)) == before


def test_calibrations_from_the_same_day_show_their_time(window, scene, box, qtbot):
    morning = models.CalibrationDB.add_calibration(
        objective_key=OBJECTIVE_KEY, microns_per_pixel=OLD_SCALE, calibration_date="2026-11-21 08:15:00"
    )
    afternoon = models.CalibrationDB.add_calibration(
        objective_key=OBJECTIVE_KEY, microns_per_pixel=NEW_SCALE, calibration_date="2026-11-21 14:40:00"
    )
    assert models.CalibrationDB.get_active_calibration_id(OBJECTIVE_KEY) == afternoon
    item = _add_image(scene.db, scale=OLD_SCALE, calibration_id=morning)
    _open_in_measure(window, item)

    _choose_in_combo(qtbot, window.scale_combo, OBJECTIVE_KEY)

    assert box.asked[0][1].startswith(
        "Move this image from the 2026-11-21 08:15:00 calibration to the active 2026-11-21 14:40:00 calibration?"
    )


class _ChoosingMenu(QMenu):
    """The image-tag menu, answered with the Objective submenu's ``choice`` entry."""

    choice = OBJECTIVE_KEY

    def exec(self, *_a, **_k):
        for action in self.actions():
            submenu = action.menu()
            for sub_action in submenu.actions() if submenu else []:
                if sub_action.data() == ("objective", type(self).choice):
                    # Return the chosen entry's data, not the action, which the
                    # submenu's Python wrapper may already have released.
                    data = sub_action.data()
                    return SimpleNamespace(data=lambda: data)
        return None


def test_rechoosing_in_the_image_tag_menu_moves_the_image(window, scene, box, monkeypatch):
    monkeypatch.setattr(main_window, "QMenu", _ChoosingMenu)
    item = _add_image(scene.db, scale=OLD_SCALE, calibration_id=scene.old_cal)
    _open_in_measure(window, item)

    window._show_measure_tag_menu("microscope")

    assert [title for title, _text in box.asked] == ["Use active calibration"]
    _assert_moved_to_active(window, scene, item)


def test_rechoosing_in_the_image_tag_menu_on_the_active_calibration_does_nothing(window, scene, box, monkeypatch):
    monkeypatch.setattr(main_window, "QMenu", _ChoosingMenu)
    item = _add_image(scene.db, scale=NEW_SCALE, calibration_id=scene.new_cal)
    _open_in_measure(window, item)
    before = _state(scene.db, item)

    window._show_measure_tag_menu("microscope")

    assert box.asked == []
    assert _state(scene.db, item) == before
