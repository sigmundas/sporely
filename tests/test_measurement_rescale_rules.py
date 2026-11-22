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
# The multiline row stores 11.66 um (rounded); its path is 116.62 px. The Measure
# tab's rescale recomputes a measurement from what was drawn (redesign stage 8,
# owner 2026-11-10), so it gives the path's length at the new scale; Prepare
# Images still rescales by the ratio of the scales.
MULTILINE_PATH_PX = 2 * (50**2 + 30**2) ** 0.5
MULTILINE_BY_RATIO = 23.32
MULTILINE_FROM_PATH = MULTILINE_PATH_PX * NEW_SCALE


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


def _assert_rescaled_by_two(db_path, seed, multiline_um=MULTILINE_BY_RATIO):
    assert _measurement(db_path, seed.spore_id) == pytest.approx(
        {"length_um": 20.0, "width_um": 8.0, "notes": "Q=2.5", "measurement_type": "manual"}
    )
    multiline = _measurement(db_path, seed.multiline_id)
    assert multiline["length_um"] == pytest.approx(multiline_um)
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
        _notify_measurements_changed=lambda: None,
        calls=calls,
    )


def test_measure_tab_objective_change(real_db, monkeypatch):
    seed = _seed(real_db)
    monkeypatch.setattr(main_window, "QMessageBox", _AcceptingMessageBox)
    stub = _main_window_stub(seed.image_id)

    ok = main_window.MainWindow._maybe_rescale_current_image(stub, OLD_SCALE, NEW_SCALE)

    assert ok is True
    # Multiline measurements were previously skipped and kept the old scale.
    _assert_rescaled_by_two(real_db, seed, MULTILINE_FROM_PATH)
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
    assert _measurement(real_db, seed.multiline_id)["length_um"] == pytest.approx(MULTILINE_FROM_PATH)


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


# --- Geometry after resample or crop ----------------------------------------


def _points_and_path(db_path, measurement_id):
    row = _sql(
        db_path,
        "SELECT p1_x, p1_y, p2_x, p2_y, p3_x, p3_y, p4_x, p4_y, notes, length_um "
        "FROM spore_measurements WHERE id = ?",
        (measurement_id,),
    )[0]
    return dict(row)


def test_resample_scales_points_and_multiline_paths(real_db):
    seed = _seed(real_db)

    observations_tab.ObservationsTab._scale_measurement_points(SimpleNamespace(), seed.image_id, 0.5)

    spore = _points_and_path(real_db, seed.spore_id)
    assert (spore["p2_x"], spore["p3_y"], spore["p4_y"]) == pytest.approx((50.0, -10.0, 10.0))
    assert spore["notes"] == "Q=2.5"
    assert spore["length_um"] == pytest.approx(10.0)
    multiline = _points_and_path(real_db, seed.multiline_id)
    # Previously the stored points moved but the drawn path did not.
    assert json.loads(multiline["notes"]) == {"multiline": [[0.0, 0.0], [25.0, 15.0], [50.0, 0.0]]}
    assert (multiline["p2_x"], multiline["p2_y"]) == pytest.approx((25.0, 15.0))
    assert multiline["p3_x"] is None


def test_crop_shifts_points_and_multiline_paths(real_db):
    seed = _seed(real_db)

    observations_tab.ObservationsTab._translate_measurement_points(SimpleNamespace(), seed.image_id, 10.0, 5.0)

    spore = _points_and_path(real_db, seed.spore_id)
    assert (spore["p1_x"], spore["p1_y"], spore["p2_x"]) == pytest.approx((-10.0, -5.0, 90.0))
    multiline = _points_and_path(real_db, seed.multiline_id)
    assert json.loads(multiline["notes"]) == {"multiline": [[-10.0, -5.0], [40.0, 25.0], [90.0, -5.0]]}


def test_geometry_transform_leaves_unreadable_multiline_notes_alone(real_db):
    seed = _seed(real_db)
    _sql(real_db, "UPDATE spore_measurements SET notes = ? WHERE id = ?", ('{"multiline": broken', seed.multiline_id))

    models.MeasurementDB.scale_measurement_points(seed.image_id, 0.5)

    assert _points_and_path(real_db, seed.multiline_id)["notes"] == '{"multiline": broken'


@pytest.mark.parametrize("factor", [0, None, -1])
def test_scale_measurement_points_ignores_unusable_factors(real_db, factor):
    seed = _seed(real_db)

    models.MeasurementDB.scale_measurement_points(seed.image_id, factor)

    assert _points_and_path(real_db, seed.spore_id)["p2_x"] == pytest.approx(100.0)


# --- Editing the scale bar in the Measure tab ---------------------------------


def _calibration_edit_stub(seed):
    stub = SimpleNamespace(
        measurements_cache=[
            {"id": seed.calibration_id, "image_id": seed.image_id, "measurement_type": "calibration"},
            {"id": seed.spore_id, "image_id": seed.image_id, "measurement_type": "manual"},
        ],
        tr=lambda text: text,
        microns_per_pixel=OLD_SCALE,
        update_measurements_table=lambda: None,
        update_statistics=lambda: None,
        update_display_lines=lambda: None,
        load_measurement_lines=lambda: None,
        image_label=SimpleNamespace(scale=None),
        _get_measurement_by_id=lambda _mid: None,
    )
    stub.image_label.set_microns_per_pixel = lambda value: setattr(stub.image_label, "scale", value)
    return stub


def test_editing_the_scale_bar_rescales_by_the_rule(real_db, monkeypatch):
    """The scale bar (200 px) is edited to read 40 um: 0.1 -> 0.2 um/px."""
    seed = _seed(real_db)
    monkeypatch.setattr(main_window, "QMessageBox", _AcceptingMessageBox)
    stub = _calibration_edit_stub(seed)
    points = [QPointF(0, 0), QPointF(200, 0), QPointF(100, -10), QPointF(100, 10)]

    main_window.MainWindow._on_calibration_measurement_edited(stub, seed.calibration_id, 40.0, points)

    # Previously: notes rewritten to Q=..., multiline paths destroyed, not marked dirty.
    _assert_rescaled_by_two(real_db, seed)
    assert _image_scale(real_db, seed.image_id) == pytest.approx(NEW_SCALE)
    assert _sync_status(real_db, seed.obs_id) == "dirty"
    assert stub.microns_per_pixel == pytest.approx(NEW_SCALE)
    # The image overlay follows, not just the table.
    assert stub.image_label.scale == pytest.approx(NEW_SCALE)


def _dimensions_stub(seed, monkeypatch, *, real_edit_handler):
    stub = _calibration_edit_stub(seed)
    stub._cached_measurement_type = lambda mid: main_window.MainWindow._cached_measurement_type(stub, mid)
    stub.normalize_measurement_category = lambda c: main_window.MainWindow.normalize_measurement_category(stub, c)
    stub._invalidate_gallery_thumbnail_cache = lambda _mid: None
    stub.measurement_lines = {}
    stub.measurement_labels = []
    stub._build_measurement_label = lambda *a: {"id": a[0]}
    stub._build_line_measurement_label = lambda *a: {"id": a[0]}
    stub.measure_status_label = SimpleNamespace(setText=lambda *_a: None, setStyleSheet=lambda *_a: None)
    stub.edit_calls = []
    if real_edit_handler:
        monkeypatch.setattr(main_window, "QMessageBox", _AcceptingMessageBox)

        def _handler(*args):
            stub.edit_calls.append(args)
            return main_window.MainWindow._on_calibration_measurement_edited(stub, *args)

        stub._on_calibration_measurement_edited = _handler
    else:
        stub._on_calibration_measurement_edited = lambda *a: stub.edit_calls.append(a)
    return stub


def test_moving_scale_bar_endpoints_keeps_its_label_and_changes_the_scale(real_db, monkeypatch):
    """The 20 um bar is shortened from 200 to 160 px. The preview reports 16 um
    (pixels x current scale); the bar is still 20 um, so the scale becomes
    0.125 um/px and, after the prompt, the other measurements follow."""
    seed = _seed(real_db)
    stub = _dimensions_stub(seed, monkeypatch, real_edit_handler=True)
    points = [QPointF(0, 0), QPointF(160, 0), QPointF(80, -8), QPointF(80, 8)]

    main_window.MainWindow.on_dimensions_changed(stub, seed.calibration_id, 16.0, 1.6, points)

    assert _measurement(real_db, seed.calibration_id) == pytest.approx(
        {"length_um": 20.0, "width_um": 2.0, "notes": "Scale bar: 20.0 µm", "measurement_type": "calibration"}
    )
    assert stub.edit_calls[0][1] == pytest.approx(20.0)
    # Previously new scale = 16 / 160 = 0.1 = old scale, so nothing was offered.
    assert _image_scale(real_db, seed.image_id) == pytest.approx(0.125)
    assert _measurement(real_db, seed.spore_id)["length_um"] == pytest.approx(12.5)
    assert _sync_status(real_db, seed.obs_id) == "dirty"


def test_editing_an_ordinary_measurement_still_records_its_q(real_db, monkeypatch):
    seed = _seed(real_db)
    stub = _dimensions_stub(seed, monkeypatch, real_edit_handler=False)
    points = [QPointF(0, 0), QPointF(120, 0), QPointF(60, -20), QPointF(60, 20)]

    main_window.MainWindow.on_dimensions_changed(stub, seed.spore_id, 12.0, 4.0, points)

    assert _measurement(real_db, seed.spore_id) == pytest.approx(
        {"length_um": 12.0, "width_um": 4.0, "notes": "Q=3.0", "measurement_type": "manual"}
    )
    assert stub.edit_calls == []


# --- Prepare Images: which pixels a selected scale refers to ------------------


def _dialog_stub(*, scale_bar):
    stub = SimpleNamespace(
        scale_bar_mode_checkbox=SimpleNamespace(isChecked=lambda: scale_bar),
        _custom_scale=0.3 if scale_bar else None,
    )
    stub._selected_scale_is_scale_bar = lambda: image_import_dialog.ImageImportDialog._selected_scale_is_scale_bar(stub)
    return stub


@pytest.mark.parametrize(
    "scale_bar, factor, expected",
    [
        # An objective's scale is per sensor pixel: divide by the import resample factor.
        (False, 0.5, 0.6),
        # A scale bar is drawn on the stored (already resampled) image: use as is.
        (True, 0.5, 0.3),
        (False, None, 0.3),
        (False, 1.0, 0.3),
    ],
)
def test_stored_pixel_scale_for_result(scale_bar, factor, expected):
    stub = _dialog_stub(scale_bar=scale_bar)
    result = SimpleNamespace(resample_scale_factor=factor)

    value = image_import_dialog.ImageImportDialog._stored_pixel_scale_for_result(stub, result, 0.3)

    assert value == pytest.approx(expected)


def test_measure_tab_objective_uses_the_stored_pixel_scale(real_db):
    """An objective's 0.3 um/px is per sensor pixel; the image was halved at import."""
    seed = _seed(real_db)
    _sql(real_db, "UPDATE images SET resample_scale_factor = 0.5 WHERE id = ?", (seed.image_id,))
    stub = SimpleNamespace(current_image_id=seed.image_id)

    value = main_window.MainWindow._stored_pixel_scale_for_current_image(stub, 0.3)

    assert value == pytest.approx(0.6)


@pytest.mark.parametrize("factor", [None, 1.0, 0])
def test_measure_tab_objective_without_resample_is_unchanged(real_db, factor):
    seed = _seed(real_db)
    _sql(real_db, "UPDATE images SET resample_scale_factor = ? WHERE id = ?", (factor, seed.image_id))

    value = main_window.MainWindow._stored_pixel_scale_for_current_image(
        SimpleNamespace(current_image_id=seed.image_id), 0.3
    )

    assert value == pytest.approx(0.3)


@pytest.mark.parametrize(
    "notes",
    [
        '{"multiline": [{"x": 2, "y": 4}, {"x": 6, "y": 8}]}',
        '{"multiline": [[2, 4, 12], [6, 8, 1]]}',
        '{"multiline": [[2, "a"], [6, 8]]}',
        '{"multiline": "not a list"}',
    ],
)
def test_malformed_multiline_paths_are_kept_and_do_not_block_the_rest(real_db, notes):
    seed = _seed(real_db)
    _sql(real_db, "UPDATE spore_measurements SET notes = ? WHERE id = ?", (notes, seed.multiline_id))

    models.MeasurementDB.scale_measurement_points(seed.image_id, 0.5)

    assert _points_and_path(real_db, seed.multiline_id)["notes"] == notes
    assert _points_and_path(real_db, seed.spore_id)["p2_x"] == pytest.approx(50.0)


def test_multiline_json_keeps_other_keys(real_db):
    seed = _seed(real_db)
    _sql(
        real_db,
        "UPDATE spore_measurements SET notes = ? WHERE id = ?",
        (json.dumps({"multiline": [[10, 20]], "tag": 1}), seed.multiline_id),
    )

    models.MeasurementDB.scale_measurement_points(seed.image_id, 0.5)

    assert json.loads(_points_and_path(real_db, seed.multiline_id)["notes"]) == {"multiline": [[5.0, 10.0]], "tag": 1}


def test_partially_filled_point_columns_are_each_transformed(real_db):
    seed = _seed(real_db)
    _sql(real_db, "UPDATE spore_measurements SET p2_y = NULL WHERE id = ?", (seed.spore_id,))

    models.MeasurementDB.scale_measurement_points(seed.image_id, 0.5)

    row = _points_and_path(real_db, seed.spore_id)
    assert row["p2_x"] == pytest.approx(50.0)
    assert row["p2_y"] is None


def test_scale_bar_width_follows_its_tick_when_only_the_bar_is_shortened(real_db, monkeypatch):
    seed = _seed(real_db)
    stub = _dimensions_stub(seed, monkeypatch, real_edit_handler=False)
    # Bar 200 -> 160 px; the 20 px cross tick is untouched.
    points = [QPointF(0, 0), QPointF(160, 0), QPointF(100, -10), QPointF(100, 10)]

    main_window.MainWindow.on_dimensions_changed(stub, seed.calibration_id, 16.0, 2.0, points)

    row = _measurement(real_db, seed.calibration_id)
    assert row["length_um"] == pytest.approx(20.0)
    assert row["width_um"] == pytest.approx(2.5)


def test_measure_tab_objective_change_after_a_stale_cache(real_db, monkeypatch):
    """Prepare Images rescaled 0.1 -> 0.2 and was closed; the Measure tab still
    caches 0.1. Choosing a 0.1 objective must rescale back, not be skipped."""
    seed = _seed(real_db)
    models.MeasurementDB.rescale_image_measurements(seed.image_id, OLD_SCALE, NEW_SCALE)
    _AcceptingMessageBox.prompts = 0
    monkeypatch.setattr(main_window, "QMessageBox", _AcceptingMessageBox)

    ok = main_window.MainWindow._maybe_rescale_current_image(_main_window_stub(seed.image_id), OLD_SCALE, OLD_SCALE)

    assert ok is True
    assert _AcceptingMessageBox.prompts == 1
    assert _measurement(real_db, seed.spore_id)["length_um"] == pytest.approx(10.0)
    assert _image_scale(real_db, seed.image_id) == pytest.approx(OLD_SCALE)


def test_out_of_range_multiline_coordinate_is_kept_and_does_not_block_the_rest(real_db):
    seed = _seed(real_db)
    notes = '{"multiline": [[' + "9" * 400 + ', 1], [2, 3]]}'
    _sql(real_db, "UPDATE spore_measurements SET notes = ? WHERE id = ?", (notes, seed.multiline_id))

    models.MeasurementDB.scale_measurement_points(seed.image_id, 0.5)

    assert _points_and_path(real_db, seed.multiline_id)["notes"] == notes
    assert _points_and_path(real_db, seed.spore_id)["p2_x"] == pytest.approx(50.0)
