"""Changing microscope settings on several images (redesign plan stage 8b).

``MicroscopeSettingsDB`` previews and applies a change of objective, contrast,
mount or stain in one transaction: only microscope images change, a new
objective recalculates measurements through the one rescale path, each
observation is marked for sync and gets one session-log event of kind "edit"
(never read by the offline time matcher). Mål's gallery opens the dialog
from its right-click menu. Isolated profile under ``tmp_path``; no network.
"""
from __future__ import annotations

import json
import math
import os
import sqlite3

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QPointF, Qt
from PySide6.QtWidgets import QMenu

from database.models import (
    EDIT_SESSION_KIND,
    MICROSCOPE_SETTINGS_EVENT,
    ImageDB,
    MeasurementDB,
    MicroscopeSettingsDB,
    ObjectiveChoice,
    SessionLogDB,
)
from database.schema import get_connection
from tools.review_ui.live_lab_harness import FakeRawpy, create_observation, isolation_patches, prepare_profile

CROSS = [QPointF(100, 90), QPointF(130, 90), QPointF(115, 80), QPointF(115, 100)]  # 30 px and 20 px
FORTY = ObjectiveChoice("40X", 0.25, None)


@pytest.fixture
def profile(tmp_path, monkeypatch, qtbot):
    rawpy = FakeRawpy()
    for owner, attribute, value in isolation_patches(tmp_path, rawpy):
        monkeypatch.setattr(owner, attribute, value)
    yield prepare_profile(tmp_path, rawpy)


def _db(sql, params=()):
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
        conn.commit()
        return rows
    finally:
        conn.close()


def _seed(profile, key="obs"):
    """One synced observation: two 100X microscope images with a spore each, and a field image."""
    observation_id = create_observation(profile, key)
    images = []
    for index in range(2):
        image_id = ImageDB.add_image(
            observation_id, str(profile.jpeg(f"{key}{index}.JPG", seed=index)), "microscope",
            scale=0.1, objective_name="100X", contrast="DIC", mount_medium="Water",
        )
        MeasurementDB.add_measurement(image_id, 3.0, 2.0, measurement_type="spores", points=CROSS)
        images.append(image_id)
    field = ImageDB.add_image(observation_id, str(profile.jpeg(f"{key}F.JPG", seed=9)), "field")
    _db("UPDATE observations SET cloud_id = ?, sync_status = 'synced' WHERE id = ?", (f"cloud-{key}", observation_id))
    return observation_id, images, field


def _image(image_id):
    return _db("SELECT * FROM images WHERE id = ?", (image_id,))[0]


def test_a_new_objective_rescales_logs_and_marks_for_sync(profile):
    observation_id, images, field = _seed(profile)
    selection = [*images, field]

    preview = MicroscopeSettingsDB.preview(selection, objective=FORTY, contrast="BF")
    assert preview.image_ids == tuple(images) and preview.measurements == 2 and preview.skipped_field_images == 1
    assert _image(images[0])["objective_name"] == "100X"  # a preview writes nothing

    outcome = MicroscopeSettingsDB.apply(selection, objective=FORTY, contrast="BF")
    assert outcome == preview
    for image_id in images:
        image = _image(image_id)
        assert (image["objective_name"], image["scale_microns_per_pixel"], image["contrast"]) == ("40X", 0.25, "BF")
        assert image["mount_medium"] == "Water"  # not asked to change
        (spore,) = _db("SELECT * FROM spore_measurements WHERE image_id = ?", (image_id,))
        assert math.isclose(spore["length_um"], 7.5) and math.isclose(spore["width_um"], 5.0)
    assert _image(field)["objective_name"] is None  # field images are left alone
    assert _db("SELECT sync_status FROM observations WHERE id = ?", (observation_id,))[0]["sync_status"] == "dirty"

    (event,) = SessionLogDB.get_events(observation_id=observation_id)
    assert event["session_kind"] == EDIT_SESSION_KIND and event["event_type"] == MICROSCOPE_SETTINGS_EVENT
    metadata = event["metadata_json"] if isinstance(event["metadata_json"], dict) else json.loads(event["metadata_json"])
    assert metadata["measurements_recalculated"] == 2
    logged = {entry["sync_key"]: entry for entry in metadata["images"]}
    assert set(logged) == {_image(image_id)["sync_key"] for image_id in images}
    entry = logged[_image(images[0])["sync_key"]]
    assert entry["before"] == {
        "objective_name": "100X", "scale_microns_per_pixel": 0.1, "calibration_id": None, "contrast": "DIC",
    }
    assert entry["after"] == {
        "objective_name": "40X", "scale_microns_per_pixel": 0.25, "calibration_id": None, "contrast": "BF",
    }
    assert entry["measurements_recalculated"] == 1


def test_the_log_survives_an_archive_import_after_an_image_is_deleted(profile):
    """Archive import reads image_id(s) in log metadata as references that must exist."""
    from utils.archive.portable_import import _rewrite_embedded_identity

    observation_id, images, _field = _seed(profile)
    MicroscopeSettingsDB.apply(images, objective=FORTY)
    (event,) = SessionLogDB.get_events(observation_id=observation_id)
    _rewrite_embedded_identity(event["metadata_json"], image_id_map={}, session_id_map={})


def test_a_resized_image_gets_the_objective_scale_for_its_size(profile):
    observation_id, images, _field = _seed(profile)
    _db("UPDATE images SET resample_scale_factor = 0.5, scale_microns_per_pixel = 0.2 WHERE id = ?", (images[0],))
    same = ObjectiveChoice("100X", 0.1, None)
    assert MicroscopeSettingsDB.preview([images[0]], objective=same).image_ids == ()
    MicroscopeSettingsDB.apply([images[0]], objective=FORTY)
    assert _image(images[0])["scale_microns_per_pixel"] == 0.5
    (spore,) = _db("SELECT * FROM spore_measurements WHERE image_id = ?", (images[0],))
    assert math.isclose(spore["length_um"], 7.5)  # the points are in the resized image: 30 px × 0.5 / 2


def test_the_preview_counts_only_what_apply_changes(profile):
    _observation_id, images, _field = _seed(profile)
    _db("UPDATE images SET scale_microns_per_pixel = NULL WHERE id = ?", (images[0],))
    _db("DELETE FROM spore_measurements WHERE image_id = ?", (images[0],))
    MeasurementDB.add_measurement(images[0], 3.0, 2.0, measurement_type="spores")  # no points, no old scale
    preview = MicroscopeSettingsDB.preview([images[0]], objective=FORTY)
    assert preview.measurements == 0
    assert MicroscopeSettingsDB.apply([images[0]], objective=FORTY, expected=preview) == preview


def test_apply_refuses_when_the_images_changed_since_the_preview(profile):
    from database.models import MicroscopeSettingsChanged

    _observation_id, images, _field = _seed(profile)
    preview = MicroscopeSettingsDB.preview(images, contrast="BF")
    _db("UPDATE images SET contrast = 'BF' WHERE id = ?", (images[0],))  # another writer
    with pytest.raises(MicroscopeSettingsChanged):
        MicroscopeSettingsDB.apply(images, contrast="BF", expected=preview)
    assert _image(images[1])["contrast"] == "DIC"


def test_a_value_that_differs_only_in_case_is_not_a_change(profile):
    observation_id, images, _field = _seed(profile)
    _db("UPDATE images SET contrast = 'dic'")
    assert MicroscopeSettingsDB.apply(images, contrast="DIC").image_ids == ()
    assert SessionLogDB.get_events(observation_id=observation_id) == []


def test_a_contrast_change_recalculates_nothing(profile):
    _observation_id, images, _field = _seed(profile)
    outcome = MicroscopeSettingsDB.apply(images, contrast="BF")
    assert outcome.image_ids == tuple(images) and outcome.measurements == 0
    assert _db("SELECT length_um FROM spore_measurements WHERE image_id = ?", (images[0],))[0]["length_um"] == 3.0
    assert _image(images[0])["scale_microns_per_pixel"] == 0.1


def test_values_already_set_change_nothing_and_log_nothing(profile):
    observation_id, images, _field = _seed(profile)
    same = ObjectiveChoice("100X", 0.1, None)
    outcome = MicroscopeSettingsDB.apply(images, objective=same, contrast="DIC")
    assert outcome.image_ids == () and outcome.measurements == 0
    assert SessionLogDB.get_events(observation_id=observation_id) == []
    assert _db("SELECT sync_status FROM observations WHERE id = ?", (observation_id,))[0]["sync_status"] == "synced"


def test_a_failure_changes_nothing(profile, monkeypatch):
    observation_id, images, _field = _seed(profile)

    def broken_log(*_args, **_kwargs):
        raise RuntimeError("log failed")

    monkeypatch.setattr(MicroscopeSettingsDB, "_log", staticmethod(broken_log))
    with pytest.raises(RuntimeError):
        MicroscopeSettingsDB.apply(images, objective=FORTY)
    assert _image(images[0])["objective_name"] == "100X"
    assert _db("SELECT length_um FROM spore_measurements WHERE image_id = ?", (images[0],))[0]["length_um"] == 3.0
    assert _db("SELECT sync_status FROM observations WHERE id = ?", (observation_id,))[0]["sync_status"] == "synced"


def test_the_offline_time_matcher_does_not_see_the_edit(profile):
    from utils.temporal_matcher import TemporalMatcher

    observation_id, images, _field = _seed(profile)
    MicroscopeSettingsDB.apply(images, objective=FORTY)
    assert SessionLogDB.get_events(observation_id=observation_id)
    assert TemporalMatcher(session_kind="offline").load_sessions(observation_id) == []


# -- in Mål ------------------------------------------------------------------


def _window(qtbot, observation_id, image_id):
    from ui.main_window import MainWindow

    window = MainWindow()
    qtbot.addWidget(window)
    window.resize(1400, 900)
    window.show()
    qtbot.waitExposed(window)
    window.on_observation_selected(observation_id, "Inocybe geophylla", switch_tab=False)
    window.tab_widget.setCurrentWidget(window.measure_tab)
    qtbot.waitUntil(lambda: window.current_image_id == image_id, timeout=5000)
    qtbot.wait(200)
    return window


def _objectives():
    return {
        "100X": {"name": "100X", "magnification": "100X", "microns_per_pixel": 0.1},
        "40X": {"name": "40X", "magnification": "40X", "microns_per_pixel": 0.25},
    }


def test_the_gallery_menu_offers_it_for_the_selection(qtbot, profile, monkeypatch):
    observation_id, images, field = _seed(profile)
    window = _window(qtbot, observation_id, images[0])
    gallery = window.measure_gallery
    qtbot.waitUntil(lambda: len(gallery._frames) == 3, timeout=5000)
    frames = {frame.image_id: frame for frame in gallery._frames}
    gallery._selected_keys = {frames[images[0]].image_key, frames[images[1]].image_key}

    offered = []

    class ChoosingMenu(QMenu):
        def exec(self, *_args):
            offered.extend(action.text() for action in self.actions())
            return next(action for action in self.actions() if action.text() == "Change microscope settings…")

    monkeypatch.setattr("ui.image_gallery_widget.QMenu", ChoosingMenu)
    opened = []
    monkeypatch.setattr(window, "_on_measure_gallery_change_settings_requested", lambda keys: opened.append(keys))
    gallery.changeSettingsRequested.disconnect()
    gallery.changeSettingsRequested.connect(window._on_measure_gallery_change_settings_requested)
    gallery._show_thumbnail_context_menu(frames[images[1]], frames[images[1]].mapToGlobal(frames[images[1]].rect().center()))

    assert "Change microscope settings…" in offered
    assert sorted(opened[0]) == sorted(images)


def test_the_dialog_previews_then_applies_and_mal_follows(qtbot, profile, monkeypatch):
    from ui.main_window import MainWindow
    from ui.microscope_settings_dialog import MicroscopeSettingsDialog

    observation_id, images, field = _seed(profile)
    window = _window(qtbot, observation_id, images[0])
    monkeypatch.setattr(MainWindow, "load_objective_definitions", lambda self: _objectives())
    assert math.isclose(window.microns_per_pixel, 0.1)

    seen = {}

    def drive(dialog):
        seen["intro"] = dialog.intro_label.text()
        assert not dialog.apply_btn.isEnabled()
        dialog.objective_combo.setCurrentIndex(dialog.objective_combo.findData("40X"))
        seen["effect"] = dialog.effect_label.text()
        seen["button"] = dialog.apply_btn.text()
        qtbot.mouseClick(dialog.apply_btn, Qt.LeftButton)
        return dialog.result()

    monkeypatch.setattr(MicroscopeSettingsDialog, "exec", drive)
    window._on_measure_gallery_change_settings_requested([*images, field])

    # English has no translation file: plurals read "(s)" as elsewhere in the app.
    assert seen["intro"] == "3 image(s) selected. 1 field image(s) will not be changed."
    assert seen["effect"] == "2 image(s) will change. 2 measurement(s) will be recalculated for the new scale."
    assert seen["button"] == "Apply to 2 image(s)"
    assert _image(images[0])["objective_name"] == "40X"
    qtbot.waitUntil(lambda: window.current_image_id == images[0], timeout=5000)
    qtbot.waitUntil(lambda: math.isclose(window.microns_per_pixel or 0, 0.25), timeout=5000)
    assert window.current_objective_name == "40X"


def test_mal_stays_on_the_open_image(qtbot, profile, monkeypatch):
    from ui.main_window import MainWindow
    from ui.microscope_settings_dialog import MicroscopeSettingsDialog

    observation_id, images, _field = _seed(profile)
    window = _window(qtbot, observation_id, images[0])
    monkeypatch.setattr(MainWindow, "load_objective_definitions", lambda self: _objectives())
    window._on_measure_gallery_clicked(images[1], "")
    qtbot.waitUntil(lambda: window.current_image_id == images[1], timeout=5000)

    def drive(dialog):
        dialog.field_combos["contrast"].setCurrentIndex(dialog.field_combos["contrast"].findData("BF"))
        qtbot.mouseClick(dialog.apply_btn, Qt.LeftButton)
        return dialog.result()

    monkeypatch.setattr(MicroscopeSettingsDialog, "exec", drive)
    window._on_measure_gallery_change_settings_requested([images[1]])
    assert _image(images[1])["contrast"] == "BF"
    qtbot.wait(300)
    assert window.current_image_id == images[1]
