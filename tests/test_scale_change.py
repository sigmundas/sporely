"""The one rescale path for a changed objective or scale (redesign plan stage 8a).

``MeasurementDB.apply_scale_changes`` recomputes every measurement from what
was clicked (two crossing lines, a single line, a multi-line polyline kept in
the notes), scales rows without points by the ratio, leaves calibration
measurements alone, writes the image's scale (and objective and calibration
when given) in one transaction and marks the observation for sync. Mål's
objective change uses it. Isolated profile under ``tmp_path``; no network.
"""
from __future__ import annotations

import json
import math
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QPointF
from PySide6.QtWidgets import QMessageBox

from database.models import ImageDB, MeasurementDB, ScaleChange, rescaled_measurement_size
from database.schema import get_connection
from tools.review_ui.live_lab_harness import FakeRawpy, create_observation, isolation_patches, prepare_profile


@pytest.fixture
def profile(tmp_path, monkeypatch, qtbot):
    rawpy = FakeRawpy()
    for owner, attribute, value in isolation_patches(tmp_path, rawpy):
        monkeypatch.setattr(owner, attribute, value)
    yield prepare_profile(tmp_path, rawpy)


CROSS = [QPointF(100, 90), QPointF(130, 90), QPointF(115, 80), QPointF(115, 100)]  # 30 px and 20 px
LINE = [QPointF(10, 10), QPointF(10, 50)]  # 40 px
POLYLINE = [(0, 0), (30, 0), (30, 40)]  # 30 + 40 = 70 px


def _image(profile, key="a", *, scale=0.1, objective="100X", synced=True):
    observation_id = create_observation(profile, key)
    image_id = ImageDB.add_image(
        observation_id, str(profile.jpeg(f"{key}.JPG", seed=3)), "microscope",
        scale=scale, objective_name=objective, contrast="DIC",
    )
    if synced:
        _set_observation(observation_id, cloud_id=f"cloud-{key}", sync_status="synced")
    return observation_id, image_id


def _set_observation(observation_id, **values):
    conn = get_connection()
    try:
        assignments = ", ".join(f"{column} = ?" for column in values)
        conn.execute(f"UPDATE observations SET {assignments} WHERE id = ?", (*values.values(), observation_id))
        conn.commit()
    finally:
        conn.close()


def _row(table, row_id):
    conn = get_connection()
    try:
        conn.row_factory = __import__("sqlite3").Row
        row = conn.execute(f"SELECT * FROM {table} WHERE id = ?", (row_id,)).fetchone()
        return dict(row)
    finally:
        conn.close()


def _measurements(image_id):
    """One of each kind, made at 0.1 µm/px."""
    ids = {}
    ids["cross"] = MeasurementDB.add_measurement(image_id, 3.0, 2.0, measurement_type="spores", notes="Q=1.5", points=CROSS)
    ids["line"] = MeasurementDB.add_measurement(image_id, 4.0, None, measurement_type="spores", points=LINE)
    notes = json.dumps({"multiline": [list(p) for p in POLYLINE]})
    ids["multiline"] = MeasurementDB.add_measurement(
        image_id, 7.0, None, measurement_type="hyphae", notes=notes, points=[QPointF(0, 0), QPointF(30, 0)],
    )
    ids["no_points"] = MeasurementDB.add_measurement(image_id, 5.0, 2.5, measurement_type="spores")
    ids["calibration"] = MeasurementDB.add_measurement(
        image_id, 10.0, None, measurement_type="calibration", points=[QPointF(0, 0), QPointF(100, 0)],
    )
    return ids


def test_every_kind_of_measurement_is_recomputed_at_the_new_scale(profile):
    observation_id, image_id = _image(profile)
    ids = _measurements(image_id)

    updated = MeasurementDB.apply_scale_changes([ScaleChange(image_id, 0.05, objective_name="40X", calibration_id=None)])

    assert updated == {image_id: 4}
    cross = _row("spore_measurements", ids["cross"])
    assert math.isclose(cross["length_um"], 1.5) and math.isclose(cross["width_um"], 1.0)
    assert cross["notes"] == "Q=1.5"  # Q does not change with the scale
    line = _row("spore_measurements", ids["line"])
    assert math.isclose(line["length_um"], 2.0) and line["width_um"] is None
    multiline = _row("spore_measurements", ids["multiline"])
    assert math.isclose(multiline["length_um"], 3.5)  # the whole polyline, not its first segment
    assert json.loads(multiline["notes"])["multiline"] == [list(p) for p in POLYLINE]
    no_points = _row("spore_measurements", ids["no_points"])
    assert math.isclose(no_points["length_um"], 2.5) and math.isclose(no_points["width_um"], 1.25)
    assert _row("spore_measurements", ids["calibration"])["length_um"] == 10.0
    image = _row("images", image_id)
    assert image["scale_microns_per_pixel"] == 0.05 and image["objective_name"] == "40X"
    assert image["calibration_id"] is None
    assert _row("observations", observation_id)["sync_status"] == "dirty"


def test_an_unchanged_image_is_not_marked_for_sync(profile):
    observation_id, image_id = _image(profile)
    MeasurementDB.add_measurement(image_id, 3.0, 2.0, measurement_type="spores", points=CROSS)
    _set_observation(observation_id, sync_status="synced")

    assert MeasurementDB.apply_scale_changes([ScaleChange(image_id, 0.1, objective_name="100X")]) == {image_id: 0}
    assert _row("observations", observation_id)["sync_status"] == "synced"


def test_a_failure_leaves_every_image_unchanged(profile):
    _obs_a, image_a = _image(profile, "a")
    _obs_b, image_b = _image(profile, "b")
    first = MeasurementDB.add_measurement(image_a, 3.0, 2.0, measurement_type="spores", points=CROSS)

    with pytest.raises(ValueError):
        MeasurementDB.apply_scale_changes([ScaleChange(image_a, 0.05), ScaleChange(image_b, 0)])

    assert _row("spore_measurements", first)["length_um"] == 3.0
    assert _row("images", image_a)["scale_microns_per_pixel"] == 0.1


def test_rows_that_cannot_be_rescaled_are_left_alone():
    assert rescaled_measurement_size({"length_um": 5.0, "width_um": 2.0}, None, 0.05) is None  # no points, no old scale
    assert rescaled_measurement_size({"length_um": 5.0}, 0.1, 0) is None
    assert rescaled_measurement_size({"notes": "{not json", "length_um": 5.0}, 0.1, 0.05) == (2.5, None)


def test_rescalable_count_leaves_out_calibration(profile):
    _observation_id, image_id = _image(profile)
    _measurements(image_id)
    assert MeasurementDB.rescalable_count(image_id) == 4


def test_changing_the_objective_in_measure_rescales_lines_and_multi_line(qtbot, profile, monkeypatch):
    """Before stage 8, Mål's rescale needed all four points: single lines and
    multi-line measurements kept the old scale."""
    from ui.main_window import MainWindow

    observation_id, image_id = _image(profile, synced=False)
    ids = _measurements(image_id)
    window = MainWindow()
    qtbot.addWidget(window)
    window.resize(1400, 900)
    window.show()
    qtbot.waitExposed(window)
    window.on_observation_selected(observation_id, "Inocybe geophylla", switch_tab=False)
    window.tab_widget.setCurrentWidget(window.measure_tab)
    qtbot.waitUntil(lambda: window.current_image_id == image_id, timeout=5000)
    assert math.isclose(window.microns_per_pixel, 0.1)

    asked = []
    monkeypatch.setattr(QMessageBox, "exec", lambda box: asked.append(box.text()) or 0)
    monkeypatch.setattr(
        QMessageBox, "clickedButton",
        lambda box: next(b for b in box.buttons() if box.buttonRole(b) == QMessageBox.AcceptRole),
    )
    assert window.apply_objective({"key": "40X", "name": "40X", "magnification": "40X", "microns_per_pixel": 0.05})

    assert len(asked) == 1
    assert math.isclose(_row("spore_measurements", ids["line"])["length_um"], 2.0)
    assert math.isclose(_row("spore_measurements", ids["multiline"])["length_um"], 3.5)
    assert math.isclose(_row("spore_measurements", ids["cross"])["length_um"], 1.5)
    image = _row("images", image_id)
    assert image["scale_microns_per_pixel"] == 0.05 and image["objective_name"] == "40X"


def test_cancelling_the_prompt_in_measure_changes_nothing(qtbot, profile, monkeypatch):
    from ui.main_window import MainWindow

    observation_id, image_id = _image(profile, synced=False)
    ids = _measurements(image_id)
    window = MainWindow()
    qtbot.addWidget(window)
    window.show()
    qtbot.waitExposed(window)
    window.on_observation_selected(observation_id, "Inocybe geophylla", switch_tab=False)
    window.tab_widget.setCurrentWidget(window.measure_tab)
    qtbot.waitUntil(lambda: window.current_image_id == image_id, timeout=5000)

    monkeypatch.setattr(QMessageBox, "exec", lambda box: 0)
    monkeypatch.setattr(
        QMessageBox, "clickedButton",
        lambda box: next(b for b in box.buttons() if box.buttonRole(b) == QMessageBox.RejectRole),
    )
    assert not window.apply_objective({"key": "40X", "name": "40X", "microns_per_pixel": 0.05})
    assert _row("spore_measurements", ids["line"])["length_um"] == 4.0
    assert _row("images", image_id)["scale_microns_per_pixel"] == 0.1


def test_a_resized_multi_line_measurement_is_scaled_by_the_ratio(profile):
    """Resizing scales the point columns and the image's scale, not the polyline in
    the notes: that polyline no longer gives the stored length and is not trusted."""
    _observation_id, image_id = _image(profile, scale=0.2)  # after a 0.5 resize
    notes = json.dumps({"multiline": [list(p) for p in POLYLINE]})  # still the 70 px before the resize
    measurement = MeasurementDB.add_measurement(
        image_id, 7.0, None, measurement_type="hyphae", notes=notes, points=[QPointF(0, 0), QPointF(15, 0)],
    )
    MeasurementDB.apply_scale_changes([ScaleChange(image_id, 0.1)])
    assert math.isclose(_row("spore_measurements", measurement)["length_um"], 3.5)


def test_a_multi_line_measurement_pulled_from_the_cloud_is_scaled_by_the_ratio(profile):
    """A pulled multi-line row has no polyline, only its first two points (30 px of 70)."""
    _observation_id, image_id = _image(profile)
    measurement = MeasurementDB.add_measurement(
        image_id, 7.0, None, measurement_type="hyphae", points=[QPointF(0, 0), QPointF(30, 0)],
    )
    MeasurementDB.apply_scale_changes([ScaleChange(image_id, 0.05)])
    assert math.isclose(_row("spore_measurements", measurement)["length_um"], 3.5)


def test_the_callers_old_scale_wins_over_the_image_row(profile):
    """Mål passes the scale its measurements were made at; the row can drift from it."""
    _observation_id, image_id = _image(profile, scale=0.1005)
    measurement = MeasurementDB.add_measurement(image_id, 10.0, 5.0, measurement_type="spores")
    MeasurementDB.apply_scale_changes([ScaleChange(image_id, 0.2, old_scale=0.1)])
    row = _row("spore_measurements", measurement)
    assert math.isclose(row["length_um"], 20.0) and math.isclose(row["width_um"], 10.0)


def test_the_same_scale_changes_nothing_even_for_rows_without_points(profile):
    observation_id, image_id = _image(profile)
    measurement = MeasurementDB.add_measurement(image_id, 3.0, 2.0, measurement_type="spores")
    _set_observation(observation_id, sync_status="synced")
    assert MeasurementDB.apply_scale_changes([ScaleChange(image_id, 0.1)]) == {image_id: 0}
    assert _row("spore_measurements", measurement)["length_um"] == 3.0
    assert _row("observations", observation_id)["sync_status"] == "synced"


def test_a_polyline_must_be_numbers():
    row = {"notes": json.dumps({"multiline": ["12", "34"]}), "length_um": 5.0}
    assert rescaled_measurement_size(row, 0.1, 0.05) == (2.5, None)  # ratio, not "points" (1,2),(3,4)
