"""How far back a re-calibrated objective applies.

After a new calibration of an objective is saved, the owner chooses:

1. new calibration only: existing images and measurements are unchanged;
2. update existing images: every image on the previous calibration;
3. update recent images: images taken today or in the previous N days.

An image's date is the day it was taken (``captured_at``), or the local day it
was added when it has no capture date. Images left out keep their calibration
link and scale. The database writes run for real against a temporary SQLite
database; the dialog is the real ``RecalibrationScopeDialog`` driven by qtbot.
"""

from __future__ import annotations

import os
import sqlite3
import time
from datetime import date
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QPoint, Qt
from PySide6.QtWidgets import QDialogButtonBox, QWidget

from database import models, schema
import ui.calibration_dialog as calibration_dialog
from ui.calibration_dialog import CalibrationDialog
from ui.recalibration_scope_dialog import RecalibrationScopeDialog


OLD_SCALE = 0.1
NEW_SCALE = 0.2
TODAY = date(2026, 11, 21)


@pytest.fixture(autouse=True)
def utc_plus_three(monkeypatch):
    """A fixed local time zone (UTC+3), so "the local day it was added" is deterministic."""
    monkeypatch.setenv("TZ", "Etc/GMT-3")
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


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


def _add_image(db_path, calibration_id, *, species, captured_at=None, created_at=None):
    """A cloud-linked, synced observation with one image: a 10 µm spore and a 20 µm scale bar."""
    obs_id = models.ObservationDB.create_observation(date="2026-10-01", genus="Russula", species=species)
    image_id = models.ImageDB.add_image(
        observation_id=obs_id,
        filepath=f"/nonexistent/{species}.jpg",
        image_type="microscope",
        scale=OLD_SCALE,
        calibration_id=calibration_id,
    )
    # add_image reads EXIF when no capture date is given; set it exactly.
    _sql(db_path, "UPDATE images SET captured_at = ? WHERE id = ?", (captured_at, image_id))
    if created_at is not None:
        _sql(db_path, "UPDATE images SET created_at = ? WHERE id = ?", (created_at, image_id))
    spore_id = models.MeasurementDB.add_measurement(image_id=image_id, length=10.0, width=4.0, notes="Q=2.5")
    bar_id = models.MeasurementDB.add_measurement(
        image_id=image_id, length=20.0, width=2.0, measurement_type="calibration", notes="Scale bar: 20.0 µm"
    )
    _sql(
        db_path,
        "UPDATE observations SET cloud_id = ?, sync_status = 'synced' WHERE id = ?",
        (f"cloud-{species}", obs_id),
    )
    return SimpleNamespace(obs_id=obs_id, image_id=image_id, spore_id=spore_id, bar_id=bar_id)


def _state(db_path, item):
    image = _sql(
        db_path, "SELECT calibration_id, scale_microns_per_pixel FROM images WHERE id = ?", (item.image_id,)
    )[0]
    spore = _sql(db_path, "SELECT length_um, width_um, notes FROM spore_measurements WHERE id = ?", (item.spore_id,))[0]
    bar = _sql(db_path, "SELECT length_um FROM spore_measurements WHERE id = ?", (item.bar_id,))[0]
    status = _sql(db_path, "SELECT sync_status FROM observations WHERE id = ?", (item.obs_id,))[0][0]
    return {
        "calibration_id": image["calibration_id"],
        "scale": image["scale_microns_per_pixel"],
        "length": spore["length_um"],
        "width": spore["width_um"],
        "notes": spore["notes"],
        "bar": bar["length_um"],
        "sync": status,
    }


def _assert_updated(db_path, item, new_cal):
    assert _state(db_path, item) == pytest.approx(
        {
            "calibration_id": new_cal,
            "scale": NEW_SCALE,
            "length": 20.0,
            "width": 8.0,
            "notes": "Q=2.5",
            "bar": 20.0,  # the scale-bar record keeps its labelled length
            "sync": "dirty",
        }
    )


def _assert_untouched(db_path, item, old_cal):
    assert _state(db_path, item) == pytest.approx(
        {
            "calibration_id": old_cal,
            "scale": OLD_SCALE,
            "length": 10.0,
            "width": 4.0,
            "notes": "Q=2.5",
            "bar": 20.0,
            "sync": "synced",
        }
    )


@pytest.fixture
def scene(real_db):
    """Images on the previous 40x calibration, taken on different days, plus one on another calibration."""
    older_cal = models.CalibrationDB.add_calibration(
        objective_key="40x", microns_per_pixel=OLD_SCALE, calibration_date="2026-01-01"
    )
    old_cal = models.CalibrationDB.add_calibration(
        objective_key="40x", microns_per_pixel=OLD_SCALE, calibration_date="2026-06-01"
    )
    new_cal = models.CalibrationDB.add_calibration(
        objective_key="40x", microns_per_pixel=NEW_SCALE, calibration_date="2026-11-21"
    )
    return SimpleNamespace(
        db=real_db,
        old_cal=old_cal,
        new_cal=new_cal,
        older_cal=older_cal,
        today=_add_image(real_db, old_cal, species="today", captured_at="2026-11-21 09:15:00"),
        three_days=_add_image(real_db, old_cal, species="three", captured_at="2026-11-18 23:59:00"),
        last_month=_add_image(real_db, old_cal, species="month", captured_at="2026-10-20 10:00:00"),
        # No capture date: dated by the local day it was added. created_at is
        # UTC; 22:30 UTC on 2026-11-18 is 01:30 on 2026-11-19 at UTC+3.
        added_only=_add_image(real_db, old_cal, species="added", created_at="2026-11-18 22:30:00"),
        other_calibration=_add_image(real_db, older_cal, species="other", captured_at="2026-11-21 08:00:00"),
    )


# --- Which images a choice covers --------------------------------------------


def test_scope_lists_every_image_on_the_previous_calibration(scene):
    rows = models.CalibrationDB.get_recalibration_images(scene.old_cal)

    assert [r["image_id"] for r in rows] == [
        scene.today.image_id,
        scene.added_only.image_id,
        scene.three_days.image_id,
        scene.last_month.image_id,
    ]
    by_id = {r["image_id"]: r for r in rows}
    assert by_id[scene.today.image_id]["image_date"] == "2026-11-21"
    assert by_id[scene.today.image_id]["image_date_source"] == "captured"
    assert by_id[scene.added_only.image_id]["image_date"] == "2026-11-19"
    assert by_id[scene.added_only.image_id]["image_date_source"] == "added"
    # The scale-bar record is not a measurement a rescale changes.
    assert {r["measurement_count"] for r in rows} == {1}
    assert by_id[scene.today.image_id]["species"] == "today"


def test_scope_since_a_date_includes_that_whole_day(scene):
    rows = models.CalibrationDB.get_recalibration_images(scene.old_cal, date(2026, 11, 18))

    assert {r["image_id"] for r in rows} == {
        scene.today.image_id,
        scene.added_only.image_id,
        scene.three_days.image_id,
    }
    assert {r["image_id"] for r in models.CalibrationDB.get_recalibration_images(scene.old_cal, "2026-11-21")} == {
        scene.today.image_id
    }


def test_scope_dates_a_capture_time_by_its_own_wall_clock_day(scene):
    """An offset after the time does not move the capture to another day, nor hide the date."""
    _sql(scene.db, "UPDATE images SET captured_at = ? WHERE id = ?",
         ("2026-11-21T00:10:00+01:00", scene.three_days.image_id))
    _sql(scene.db, "UPDATE images SET captured_at = ? WHERE id = ?",
         ("2026-11-01T08:30:00+0100", scene.today.image_id))

    rows = {r["image_id"]: r for r in models.CalibrationDB.get_recalibration_images(scene.old_cal)}

    assert rows[scene.three_days.image_id]["image_date"] == "2026-11-21"
    assert rows[scene.today.image_id]["image_date"] == "2026-11-01"
    assert rows[scene.today.image_id]["image_date_source"] == "captured"


def test_scope_counts_no_measurements_for_an_image_without_a_scale(scene):
    """With no stored scale the update only relinks the image, so nothing is rescaled."""
    _sql(scene.db, "UPDATE images SET scale_microns_per_pixel = NULL WHERE id = ?", (scene.today.image_id,))

    rows = {r["image_id"]: r for r in models.CalibrationDB.get_recalibration_images(scene.old_cal)}
    assert rows[scene.today.image_id]["measurement_count"] == 0

    updated = models.CalibrationDB.recalculate_measurements_for_calibration(
        scene.old_cal, scene.new_cal, NEW_SCALE, since_date=TODAY
    )
    assert updated == 0
    image = _sql(scene.db, "SELECT calibration_id FROM images WHERE id = ?", (scene.today.image_id,))[0]
    assert image["calibration_id"] == scene.new_cal


# --- Choice 2: update existing images ----------------------------------------


def test_update_existing_images_rescales_every_image_on_the_previous_calibration(scene):
    updated = models.CalibrationDB.recalculate_measurements_for_calibration(
        scene.old_cal, scene.new_cal, NEW_SCALE
    )

    assert updated == 4
    for item in (scene.today, scene.three_days, scene.last_month, scene.added_only):
        _assert_updated(scene.db, item, scene.new_cal)
    _assert_untouched(scene.db, scene.other_calibration, scene.older_cal)


# --- Images whose scale differs from the calibration -------------------------


@pytest.mark.parametrize(
    "image_scale, resample_factor",
    [
        (OLD_SCALE / 0.75, 0.75),  # resampled to 75% on import
        (0.098, 1.0),  # still carries an earlier calibration's scale
        (0.0536, 1.0),  # carries another objective's scale
    ],
    ids=["resampled", "earlier-calibration-scale", "other-objective-scale"],
)
def test_update_applies_the_calibration_change_to_the_image_scale(scene, image_scale, resample_factor):
    """The image keeps its relation to the calibration: scale and lengths change by new/old calibration."""
    _sql(
        scene.db,
        "UPDATE images SET scale_microns_per_pixel = ?, resample_scale_factor = ? WHERE id = ?",
        (image_scale, resample_factor, scene.today.image_id),
    )

    updated = models.CalibrationDB.recalculate_measurements_for_calibration(
        scene.old_cal, scene.new_cal, NEW_SCALE, since_date=TODAY
    )

    factor = NEW_SCALE / OLD_SCALE
    assert updated == 1
    assert _state(scene.db, scene.today) == pytest.approx(
        {
            "calibration_id": scene.new_cal,
            "scale": image_scale * factor,
            "length": 10.0 * factor,
            "width": 4.0 * factor,
            "notes": "Q=2.5",
            "bar": 20.0,
            "sync": "dirty",
        }
    )
    resample = _sql(scene.db, "SELECT resample_scale_factor FROM images WHERE id = ?", (scene.today.image_id,))
    assert resample[0][0] == pytest.approx(resample_factor)


@pytest.mark.parametrize("stored", [0.0, float("inf")], ids=["zero", "infinite"])
def test_update_without_an_old_calibration_scale_only_relinks(scene, stored):
    """With no usable old calibration scale there is no correction to apply."""
    _sql(scene.db, "UPDATE calibrations SET microns_per_pixel = ? WHERE id = ?", (stored, scene.old_cal))

    updated = models.CalibrationDB.recalculate_measurements_for_calibration(
        scene.old_cal, scene.new_cal, NEW_SCALE, since_date=TODAY
    )

    assert updated == 0
    state = _state(scene.db, scene.today)
    assert state["calibration_id"] == scene.new_cal
    assert state["scale"] == pytest.approx(OLD_SCALE)
    assert (state["length"], state["width"]) == pytest.approx((10.0, 4.0))


# --- Choice 3: update recent images ------------------------------------------


def test_update_recent_images_leaves_older_images_on_the_previous_calibration(scene):
    updated = models.CalibrationDB.recalculate_measurements_for_calibration(
        scene.old_cal, scene.new_cal, NEW_SCALE, since_date=date(2026, 11, 19)
    )

    assert updated == 2
    _assert_updated(scene.db, scene.today, scene.new_cal)
    _assert_updated(scene.db, scene.added_only, scene.new_cal)
    # Outside the window: same calibration link, scale and lengths; not dirtied.
    _assert_untouched(scene.db, scene.three_days, scene.old_cal)
    _assert_untouched(scene.db, scene.last_month, scene.old_cal)
    _assert_untouched(scene.db, scene.other_calibration, scene.older_cal)


def test_update_is_limited_to_the_images_the_preview_showed(scene):
    """An image that moved into the window after the preview is not pulled into the update."""
    shown = [r["image_id"] for r in models.CalibrationDB.get_recalibration_images(scene.old_cal, TODAY)]
    assert shown == [scene.today.image_id]
    # Its capture date changes (say, a sync) while the dialog is open.
    _sql(scene.db, "UPDATE images SET captured_at = ? WHERE id = ?",
         ("2026-11-21 07:00:00", scene.last_month.image_id))

    updated = models.CalibrationDB.recalculate_measurements_for_calibration(
        scene.old_cal, scene.new_cal, NEW_SCALE, since_date=TODAY, image_ids=shown
    )

    assert updated == 1
    _assert_updated(scene.db, scene.today, scene.new_cal)
    _assert_untouched(scene.db, scene.last_month, scene.old_cal)


def test_update_rolls_back_every_image_when_one_fails(scene, monkeypatch):
    real_rescale = models._rescale_measurements_on_cursor
    calls = []

    def _fail_on_second(cursor, image_id, ratio):
        calls.append(image_id)
        if len(calls) == 2:
            raise sqlite3.OperationalError("disk I/O error")
        return real_rescale(cursor, image_id, ratio)

    monkeypatch.setattr(models, "_rescale_measurements_on_cursor", _fail_on_second)

    with pytest.raises(sqlite3.OperationalError):
        models.CalibrationDB.recalculate_measurements_for_calibration(scene.old_cal, scene.new_cal, NEW_SCALE)

    assert len(calls) == 2
    for item in (scene.today, scene.three_days, scene.last_month, scene.added_only):
        _assert_untouched(scene.db, item, scene.old_cal)


def test_update_recent_images_with_nothing_in_the_window_changes_nothing(scene):
    updated = models.CalibrationDB.recalculate_measurements_for_calibration(
        scene.old_cal, scene.new_cal, NEW_SCALE, since_date=date(2026, 11, 22)
    )

    assert updated == 0
    for item in (scene.today, scene.three_days, scene.last_month, scene.added_only):
        _assert_untouched(scene.db, item, scene.old_cal)


# --- The dialog ----------------------------------------------------------------


def _open_scope_dialog(qtbot, scene) -> RecalibrationScopeDialog:
    dialog = RecalibrationScopeDialog(
        None, calibration_id=scene.old_cal, old_scale_um=OLD_SCALE, new_scale_um=NEW_SCALE, today=TODAY
    )
    qtbot.addWidget(dialog)
    dialog.show()
    qtbot.waitExposed(dialog)
    return dialog


def _click(qtbot, radio):
    """Click a radio button on its indicator (the button stretches past its label)."""
    qtbot.mouseClick(radio, Qt.LeftButton, pos=QPoint(8, radio.height() // 2))


def _tree_images(dialog) -> set[str]:
    tree = dialog.preview_tree
    names = set()
    for i in range(tree.topLevelItemCount()):
        parent = tree.topLevelItem(i)
        for j in range(parent.childCount()):
            names.add(parent.child(j).text(0))
    return names


def test_dialog_defaults_to_updating_existing_images_and_lists_them(qtbot, scene):
    dialog = _open_scope_dialog(qtbot, scene)

    assert dialog.all_radio.isChecked()
    assert dialog.choice() == RecalibrationScopeDialog.CHOICE_ALL
    assert dialog.since_date() is None
    assert _tree_images(dialog) == {"today.jpg", "three.jpg", "month.jpg", "added.jpg"}
    assert dialog.preview_tree.topLevelItemCount() == 4
    assert dialog.summary_label.text().startswith("4 images with 4 spore measurements in 4 observations")


def test_dialog_recent_images_follow_the_days_spinbox(qtbot, scene):
    dialog = _open_scope_dialog(qtbot, scene)

    _click(qtbot, dialog.recent_radio)
    assert dialog.choice() == RecalibrationScopeDialog.CHOICE_RECENT
    assert dialog.since_date() == TODAY
    assert _tree_images(dialog) == {"today.jpg"}
    assert "2026-11-21" in dialog.since_label.text()

    dialog.days_spin.setFocus()
    for _ in range(3):
        qtbot.keyClick(dialog.days_spin, Qt.Key_Up)
    assert dialog.days_back() == 3
    assert dialog.since_date() == date(2026, 11, 18)
    assert _tree_images(dialog) == {"today.jpg", "three.jpg", "added.jpg"}
    assert dialog.summary_label.text().startswith("3 images")
    assert "2026-11-18" in dialog.since_label.text()


def test_dialog_changing_the_days_selects_recent_images(qtbot, scene):
    dialog = _open_scope_dialog(qtbot, scene)
    assert dialog.all_radio.isChecked()

    dialog.days_spin.setFocus()
    qtbot.keyClick(dialog.days_spin, Qt.Key_Up)
    assert dialog.recent_radio.isChecked()
    qtbot.keyClick(dialog.days_spin, Qt.Key_Up)

    assert dialog.recent_radio.isChecked()
    assert dialog.since_date() == date(2026, 11, 19)
    assert _tree_images(dialog) == {"today.jpg", "added.jpg"}


def test_dialog_new_calibration_only_lists_nothing(qtbot, scene):
    dialog = _open_scope_dialog(qtbot, scene)

    _click(qtbot, dialog.new_only_radio)

    assert dialog.choice() == RecalibrationScopeDialog.CHOICE_NEW_ONLY
    assert dialog.since_date() is None
    assert dialog.preview_tree.topLevelItemCount() == 0
    assert not dialog.preview_tree.isEnabled()


def test_dialog_marks_dates_taken_from_when_the_image_was_added(qtbot, scene):
    dialog = _open_scope_dialog(qtbot, scene)

    tree = dialog.preview_tree
    children = [tree.topLevelItem(i).child(j) for i in range(tree.topLevelItemCount())
                for j in range(tree.topLevelItem(i).childCount())]
    added = next(c for c in children if c.text(0) == "added.jpg")
    taken = next(c for c in children if c.text(0) == "today.jpg")
    assert added.font(1).italic() and added.toolTip(1)
    assert not taken.font(1).italic() and not taken.toolTip(1)


# --- The calibration dialog runs the chosen update ---------------------------


def _drive_prompt(monkeypatch, scene, choose):
    """Run ``_prompt_recalculate_measurements`` with the real scope dialog.

    ``choose`` clicks a choice and returns the button to press (OK or Cancel);
    the button is clicked, so its accept/reject wiring is exercised.
    """
    info_messages = []

    def _exec(dialog):
        dialog._today = TODAY
        dialog._refresh_preview()
        button = choose(dialog)
        dialog.button_box.button(button).click()
        return dialog.result()

    monkeypatch.setattr(RecalibrationScopeDialog, "exec", _exec)
    # The method reads only the objective key from the calibration dialog.
    owner = QWidget()
    owner.current_objective_key = "40x"
    # The result is reported as a notice in the dialog's bottom bar.
    owner._show_notice = lambda text, tone="error", **kwargs: info_messages.append((text, tone))
    CalibrationDialog._prompt_recalculate_measurements(
        owner, scene.old_cal, OLD_SCALE, scene.new_cal, NEW_SCALE
    )
    return info_messages


def test_prompt_new_calibration_only_changes_nothing(qtbot, monkeypatch, scene):
    def choose(dialog):
        _click(qtbot, dialog.new_only_radio)
        return QDialogButtonBox.Ok

    messages = _drive_prompt(monkeypatch, scene, choose)

    assert messages == []
    for item in (scene.today, scene.three_days, scene.last_month, scene.added_only):
        _assert_untouched(scene.db, item, scene.old_cal)


def test_prompt_cancel_changes_nothing(qtbot, monkeypatch, scene):
    messages = _drive_prompt(monkeypatch, scene, lambda dialog: QDialogButtonBox.Cancel)

    assert messages == []
    for item in (scene.today, scene.three_days, scene.last_month, scene.added_only):
        _assert_untouched(scene.db, item, scene.old_cal)


def test_prompt_update_recent_images_rescales_only_the_window(qtbot, monkeypatch, scene):
    def choose(dialog):
        _click(qtbot, dialog.recent_radio)
        dialog.days_spin.setValue(3)
        return QDialogButtonBox.Ok

    messages = _drive_prompt(monkeypatch, scene, choose)

    assert len(messages) == 1
    for item in (scene.today, scene.three_days, scene.added_only):
        _assert_updated(scene.db, item, scene.new_cal)
    _assert_untouched(scene.db, scene.last_month, scene.old_cal)
    _assert_untouched(scene.db, scene.other_calibration, scene.older_cal)


def test_prompt_update_existing_images_rescales_all(qtbot, monkeypatch, scene):
    messages = _drive_prompt(monkeypatch, scene, lambda dialog: QDialogButtonBox.Ok)

    assert len(messages) == 1
    for item in (scene.today, scene.three_days, scene.last_month, scene.added_only):
        _assert_updated(scene.db, item, scene.new_cal)
    _assert_untouched(scene.db, scene.other_calibration, scene.older_cal)
