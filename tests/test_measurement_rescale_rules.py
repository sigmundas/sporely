"""Calibration replacement and measurement rescale rules.

Every path that rescales measurements or replaces a scale-bar calibration goes
through ``MeasurementDB``. The rules (decided 2026-11-20, see
``docs/plans/active/2026-11-18-measurement-rescale-consolidation.md``):

- a rescale multiplies length and width by ``new_scale / old_scale``;
- it never rewrites notes (Q is unchanged, and notes hold multiline paths);
- it never touches the scale-bar calibration record (its labelled length is real);
- it marks the observation dirty for sync when anything changed;
- replacing a calibration is one transaction and is case-insensitive.

UI methods are called unbound with a minimal stand-in for ``self``; the
database writes run for real against a temporary SQLite database.
"""

from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace

import pytest
from PySide6.QtCore import QPointF

from database import models, schema
import ui.image_import_dialog as image_import_dialog
import ui.main_window as main_window
import ui.observations_tab as observations_tab


OLD_SCALE = 0.1
NEW_SCALE = 0.2
MULTILINE_PATH = [[0, 0], [50, 30], [100, 0]]


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


def _seed(db_path, *, calibration_type="calibration"):
    """A cloud-linked, synced observation with a spore, a multiline and a calibration row.

    Spore: 100 px x 40 px at 0.1 um/px = 10 x 4 um, Q=2.5.
    Multiline: a two-segment path, length only.
    Calibration: a 200 px scale bar labelled 20 um, cross tick 20 px.
    """
    obs_id = models.ObservationDB.create_observation(date="2026-01-01", genus="Russula", species="test")
    image_id = models.ImageDB.add_image(
        observation_id=obs_id,
        filepath="/nonexistent/spore.jpg",
        image_type="microscope",
        scale=OLD_SCALE,
    )
    spore_id = models.MeasurementDB.add_measurement(
        image_id=image_id,
        length=10.0,
        width=4.0,
        notes="Q=2.5",
        points=[QPointF(0, 0), QPointF(100, 0), QPointF(50, -20), QPointF(50, 20)],
    )
    multiline_id = models.MeasurementDB.add_measurement(
        image_id=image_id,
        length=11.66,
        width=None,
        notes=json.dumps({"multiline": MULTILINE_PATH}),
        points=[QPointF(0, 0), QPointF(50, 30)],
    )
    calibration_id = models.MeasurementDB.add_measurement(
        image_id=image_id,
        length=20.0,
        width=2.0,
        measurement_type=calibration_type,
        notes="Scale bar: 20.0 µm",
        points=[QPointF(0, 0), QPointF(200, 0), QPointF(100, -10), QPointF(100, 10)],
    )
    _mark_synced(db_path, obs_id)
    return SimpleNamespace(
        obs_id=obs_id,
        image_id=image_id,
        spore_id=spore_id,
        multiline_id=multiline_id,
        calibration_id=calibration_id,
    )


def _mark_synced(db_path, obs_id):
    _sql(
        db_path,
        "UPDATE observations SET cloud_id = 'cloud-obs-1', sync_status = 'synced' WHERE id = ?",
        (obs_id,),
    )


def _measurement(db_path, measurement_id):
    row = _sql(
        db_path,
        "SELECT length_um, width_um, notes, measurement_type FROM spore_measurements WHERE id = ?",
        (measurement_id,),
    )[0]
    return dict(row)


def _image_scale(db_path, image_id):
    return _sql(db_path, "SELECT scale_microns_per_pixel FROM images WHERE id = ?", (image_id,))[0][0]


def _sync_status(db_path, obs_id):
    return _sql(db_path, "SELECT sync_status FROM observations WHERE id = ?", (obs_id,))[0][0]


def _calibration_rows(db_path, image_id):
    return [
        dict(r)
        for r in _sql(
            db_path,
            "SELECT measurement_type, length_um, width_um, notes, p1_x, p1_y, p2_x, p2_y, p3_x, p3_y, p4_x, p4_y "
            "FROM spore_measurements WHERE image_id = ? AND LOWER(measurement_type) = 'calibration' ORDER BY id",
            (image_id,),
        )
    ]


def _assert_rescaled_by_two(db_path, seed):
    assert _measurement(db_path, seed.spore_id) == pytest.approx(
        {"length_um": 20.0, "width_um": 8.0, "notes": "Q=2.5", "measurement_type": "manual"}
    )
    multiline = _measurement(db_path, seed.multiline_id)
    assert multiline["length_um"] == pytest.approx(23.32)
    assert multiline["width_um"] is None
    assert json.loads(multiline["notes"]) == {"multiline": MULTILINE_PATH}
    assert _measurement(db_path, seed.calibration_id) == pytest.approx(
        {"length_um": 20.0, "width_um": 2.0, "notes": "Scale bar: 20.0 µm", "measurement_type": "calibration"}
    )


# --- The shared rescale ------------------------------------------------------


def test_rescale_image_measurements_applies_the_rules(real_db):
    seed = _seed(real_db)

    updated = models.MeasurementDB.rescale_image_measurements(seed.image_id, OLD_SCALE, NEW_SCALE)

    assert updated == 2
    _assert_rescaled_by_two(real_db, seed)
    # The new scale is stored in the same transaction.
    assert _image_scale(real_db, seed.image_id) == pytest.approx(NEW_SCALE)
    assert _sync_status(real_db, seed.obs_id) == "dirty"


def test_rescale_divides_by_the_stored_image_scale_not_the_callers(real_db):
    """A caller's cached scale can be stale; the stored scale is what lengths were computed at."""
    seed = _seed(real_db)
    _sql(real_db, "UPDATE images SET scale_microns_per_pixel = ? WHERE id = ?", (0.2, seed.image_id))
    _sql(real_db, "UPDATE spore_measurements SET length_um = 20.0, width_um = 8.0 WHERE id = ?", (seed.spore_id,))
    _mark_synced(real_db, seed.obs_id)

    models.MeasurementDB.rescale_image_measurements(seed.image_id, OLD_SCALE, 0.3)

    assert _measurement(real_db, seed.spore_id)["length_um"] == pytest.approx(30.0)
    assert _image_scale(real_db, seed.image_id) == pytest.approx(0.3)


def test_rescale_falls_back_to_the_callers_scale_when_none_is_stored(real_db):
    seed = _seed(real_db)
    _sql(real_db, "UPDATE images SET scale_microns_per_pixel = NULL WHERE id = ?", (seed.image_id,))

    models.MeasurementDB.rescale_image_measurements(seed.image_id, OLD_SCALE, NEW_SCALE)

    assert _measurement(real_db, seed.spore_id)["length_um"] == pytest.approx(20.0)
    assert _image_scale(real_db, seed.image_id) == pytest.approx(NEW_SCALE)


@pytest.mark.parametrize(
    "old, new",
    [
        (OLD_SCALE, OLD_SCALE),
        # A stale caller scale: the image is already at the target scale.
        (0.05, OLD_SCALE),
        (OLD_SCALE, 0),
        (OLD_SCALE, None),
    ],
)
def test_rescale_without_a_real_change_does_nothing(real_db, old, new):
    seed = _seed(real_db)

    assert models.MeasurementDB.rescale_image_measurements(seed.image_id, old, new) == 0

    assert _measurement(real_db, seed.spore_id)["length_um"] == pytest.approx(10.0)
    assert _image_scale(real_db, seed.image_id) == pytest.approx(OLD_SCALE)
    assert _sync_status(real_db, seed.obs_id) == "synced"


def test_rescale_keeps_free_text_notes(real_db):
    seed = _seed(real_db)
    _sql(real_db, "UPDATE spore_measurements SET notes = 'odd apiculus' WHERE id = ?", (seed.spore_id,))

    models.MeasurementDB.rescale_image_measurements(seed.image_id, OLD_SCALE, NEW_SCALE)

    assert _measurement(real_db, seed.spore_id)["notes"] == "odd apiculus"


def test_rescale_skips_capitalised_calibration_rows(real_db):
    seed = _seed(real_db, calibration_type="Calibration")

    models.MeasurementDB.rescale_image_measurements(seed.image_id, OLD_SCALE, NEW_SCALE)

    assert _measurement(real_db, seed.calibration_id)["length_um"] == pytest.approx(20.0)


def test_has_rescalable_measurements():
    assert models.MeasurementDB.has_rescalable_measurements(
        [{"length_um": 5.0, "measurement_type": "manual"}]
    )
    assert models.MeasurementDB.has_rescalable_measurements(
        [{"length_um": 5.0, "measurement_type": None}]
    )
    assert not models.MeasurementDB.has_rescalable_measurements(
        [{"length_um": 20.0, "measurement_type": "Calibration"}, {"length_um": None}]
    )
    assert not models.MeasurementDB.has_rescalable_measurements([])
    assert not models.MeasurementDB.has_rescalable_measurements(None)


# --- Each UI rescale path uses it -------------------------------------------


def test_prepare_images_objective_change(real_db):
    """Legg til bilder: the rescale commits at once, so it marks the observation dirty."""
    seed = _seed(real_db)

    ok = image_import_dialog.ImageImportDialog._rescale_measurements_for_image(
        SimpleNamespace(), seed.image_id, OLD_SCALE, NEW_SCALE
    )

    assert ok is True
    _assert_rescaled_by_two(real_db, seed)
    # Stored now, so the later apply step does not rescale a second time.
    assert _image_scale(real_db, seed.image_id) == pytest.approx(NEW_SCALE)
    # Previously left "synced": closing the dialog without applying then meant
    # the cloud never received the rescaled values.
    assert _sync_status(real_db, seed.obs_id) == "dirty"


def test_prepare_images_apply_rescale(real_db):
    seed = _seed(real_db)

    observations_tab.ObservationsTab._rescale_measurement_lengths(
        SimpleNamespace(), seed.image_id, OLD_SCALE, NEW_SCALE
    )

    _assert_rescaled_by_two(real_db, seed)
    assert _image_scale(real_db, seed.image_id) == pytest.approx(NEW_SCALE)
    assert _sync_status(real_db, seed.obs_id) == "dirty"


class _AcceptingMessageBox:
    Question = None
    AcceptRole = None
    RejectRole = None
    prompts = 0

    def __init__(self, *_args, **_kwargs):
        type(self).prompts += 1
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
        return 0

    def clickedButton(self):
        return self._buttons[0]


def _main_window_stub(image_id):
    calls = []
    return SimpleNamespace(
        suppress_scale_prompt=False,
        current_image_id=image_id,
        tr=lambda text: text,
        load_measurement_lines=lambda: calls.append("lines"),
        update_measurements_table=lambda: calls.append("table"),
        update_statistics=lambda: calls.append("stats"),
        calls=calls,
    )


def test_measure_tab_objective_change(real_db, monkeypatch):
    seed = _seed(real_db)
    monkeypatch.setattr(main_window, "QMessageBox", _AcceptingMessageBox)
    stub = _main_window_stub(seed.image_id)

    ok = main_window.MainWindow._maybe_rescale_current_image(stub, OLD_SCALE, NEW_SCALE)

    assert ok is True
    # Multiline measurements were previously skipped and kept the old scale.
    _assert_rescaled_by_two(real_db, seed)
    assert _image_scale(real_db, seed.image_id) == pytest.approx(NEW_SCALE)
    assert stub.calls == ["lines", "table", "stats"]


def test_measure_tab_after_prepare_images_was_closed_without_applying(real_db, monkeypatch):
    """Legg til bilder rescales 0.1 -> 0.2 and is closed; the Measure tab still
    believes the image is at 0.1. Changing it to 0.3 must give 30 um, not 60."""
    seed = _seed(real_db)
    image_import_dialog.ImageImportDialog._rescale_measurements_for_image(
        SimpleNamespace(), seed.image_id, OLD_SCALE, NEW_SCALE
    )
    monkeypatch.setattr(main_window, "QMessageBox", _AcceptingMessageBox)

    main_window.MainWindow._maybe_rescale_current_image(_main_window_stub(seed.image_id), OLD_SCALE, 0.3)

    assert _measurement(real_db, seed.spore_id)["length_um"] == pytest.approx(30.0)
    assert _measurement(real_db, seed.calibration_id)["length_um"] == pytest.approx(20.0)


def test_measure_tab_asks_for_an_image_with_only_multiline_measurements(real_db, monkeypatch):
    seed = _seed(real_db)
    _sql(real_db, "DELETE FROM spore_measurements WHERE id IN (?, ?)", (seed.spore_id, seed.calibration_id))
    _AcceptingMessageBox.prompts = 0
    monkeypatch.setattr(main_window, "QMessageBox", _AcceptingMessageBox)

    main_window.MainWindow._maybe_rescale_current_image(_main_window_stub(seed.image_id), OLD_SCALE, NEW_SCALE)

    assert _AcceptingMessageBox.prompts == 1
    assert _measurement(real_db, seed.multiline_id)["length_um"] == pytest.approx(23.32)


def test_measure_tab_does_not_ask_for_a_calibration_only_image(real_db, monkeypatch):
    seed = _seed(real_db)
    _sql(real_db, "DELETE FROM spore_measurements WHERE id IN (?, ?)", (seed.spore_id, seed.multiline_id))
    _AcceptingMessageBox.prompts = 0
    monkeypatch.setattr(main_window, "QMessageBox", _AcceptingMessageBox)

    ok = main_window.MainWindow._maybe_rescale_current_image(_main_window_stub(seed.image_id), OLD_SCALE, NEW_SCALE)

    assert ok is True
    assert _AcceptingMessageBox.prompts == 0


def test_measure_tab_cancel_changes_nothing(real_db, monkeypatch):
    seed = _seed(real_db)

    class _Rejecting(_AcceptingMessageBox):
        def clickedButton(self):
            return self._buttons[1]

    monkeypatch.setattr(main_window, "QMessageBox", _Rejecting)

    ok = main_window.MainWindow._maybe_rescale_current_image(
        _main_window_stub(seed.image_id), OLD_SCALE, NEW_SCALE
    )

    assert ok is False
    assert _measurement(real_db, seed.spore_id)["length_um"] == pytest.approx(10.0)
    assert _sync_status(real_db, seed.obs_id) == "synced"


def test_recalibrating_an_objective(real_db):
    seed = _seed(real_db)
    old_cal = models.CalibrationDB.add_calibration(
        objective_key="40x", microns_per_pixel=OLD_SCALE, calibration_date="2026-01-01"
    )
    _sql(real_db, "UPDATE images SET calibration_id = ? WHERE id = ?", (old_cal, seed.image_id))

    updated = models.CalibrationDB.recalculate_measurements_for_calibration(old_cal, old_cal, NEW_SCALE)

    assert updated == 2
    _assert_rescaled_by_two(real_db, seed)
    assert _image_scale(real_db, seed.image_id) == pytest.approx(NEW_SCALE)
    assert _sync_status(real_db, seed.obs_id) == "dirty"


# --- Calibration replacement -------------------------------------------------

SELECTION = ((0.0, 0.0), (200.0, 0.0))


def test_replace_calibration_measurement(real_db):
    seed = _seed(real_db)

    new_id = models.MeasurementDB.replace_calibration_measurement(seed.image_id, SELECTION, 30.0)

    rows = _calibration_rows(real_db, seed.image_id)
    assert new_id is not None
    assert rows == pytest.approx(
        [
            {
                "measurement_type": "calibration",
                "length_um": 30.0,
                "width_um": 3.0,
                "notes": "Scale bar: 30.0 µm",
                # The bar, and a cross tick at its midpoint one tenth of its length.
                "p1_x": 0.0, "p1_y": 0.0, "p2_x": 200.0, "p2_y": 0.0,
                "p3_x": 100.0, "p3_y": -10.0, "p4_x": 100.0, "p4_y": 10.0,
            }
        ]
    )
    assert _sync_status(real_db, seed.obs_id) == "dirty"
    # Other measurements are untouched.
    assert _measurement(real_db, seed.spore_id)["length_um"] == pytest.approx(10.0)


def test_replace_calibration_replaces_a_capitalised_row(real_db):
    seed = _seed(real_db, calibration_type="Calibration")

    models.MeasurementDB.replace_calibration_measurement(seed.image_id, SELECTION, 30.0)

    assert [r["length_um"] for r in _calibration_rows(real_db, seed.image_id)] == pytest.approx([30.0])


@pytest.mark.parametrize(
    "selection, total_um",
    [(None, 30.0), (((0.0, 0.0),), 30.0), (((5.0, 5.0), (5.0, 5.0)), 30.0), (SELECTION, 0), (SELECTION, None)],
)
def test_replace_calibration_ignores_unusable_input(real_db, selection, total_um):
    seed = _seed(real_db)

    assert models.MeasurementDB.replace_calibration_measurement(seed.image_id, selection, total_um) is None

    assert [r["length_um"] for r in _calibration_rows(real_db, seed.image_id)] == pytest.approx([20.0])
    assert _sync_status(real_db, seed.obs_id) == "synced"


def test_replace_calibration_keeps_the_old_row_when_the_insert_fails(real_db, monkeypatch):
    seed = _seed(real_db)

    def _boom(*_args, **_kwargs):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(models.MeasurementDB, "_insert_measurement", staticmethod(_boom))

    with pytest.raises(sqlite3.OperationalError):
        models.MeasurementDB.replace_calibration_measurement(seed.image_id, SELECTION, 30.0)

    assert [r["length_um"] for r in _calibration_rows(real_db, seed.image_id)] == pytest.approx([20.0])


def test_add_measurement_still_stores_points(real_db):
    """add_measurement now delegates to the shared insert; QPointF input still works."""
    seed = _seed(real_db)

    row = _sql(
        real_db,
        "SELECT p1_x, p2_x, p3_y, p4_y, sync_key FROM spore_measurements WHERE id = ?",
        (seed.spore_id,),
    )[0]

    assert (row["p1_x"], row["p2_x"], row["p3_y"], row["p4_y"]) == (0.0, 100.0, -20.0, 20.0)
    assert row["sync_key"]
