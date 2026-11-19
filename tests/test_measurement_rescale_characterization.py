"""Characterization of the calibration-upsert and measurement-rescale paths.

These tests pin what each implementation does *today* against a real SQLite
database, so the consolidation planned in
``docs/plans/active/2026-11-18-measurement-rescale-consolidation.md`` can prove
it preserves behaviour. Differences between the paths are deliberate
assertions, not endorsements: where a test pins a suspected defect, its name
and comment say so.

The UI methods are called unbound with a minimal stand-in for ``self``; the
claims are about the database writes, which run for real.
"""

from __future__ import annotations

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
    """A cloud-linked, synced observation with one spore and one calibration row.

    Spore: 100 px x 40 px at 0.1 um/px = 10 x 4 um, Q=2.5.
    Calibration: a 200 px scale bar labelled 20 um, width line 20 px.
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
    calibration_id = models.MeasurementDB.add_measurement(
        image_id=image_id,
        length=20.0,
        width=2.0,
        measurement_type=calibration_type,
        notes="Scale bar: 20.0 µm",
        points=[QPointF(0, 0), QPointF(200, 0), QPointF(100, -10), QPointF(100, 10)],
    )
    _sql(
        db_path,
        "UPDATE observations SET cloud_id = 'cloud-obs-1', sync_status = 'synced' WHERE id = ?",
        (obs_id,),
    )
    return SimpleNamespace(obs_id=obs_id, image_id=image_id, spore_id=spore_id, calibration_id=calibration_id)


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


# --- Rescale paths ---------------------------------------------------------


def test_image_import_dialog_rescale(real_db):
    seed = _seed(real_db)

    ok = image_import_dialog.ImageImportDialog._rescale_measurements_for_image(
        SimpleNamespace(), seed.image_id, OLD_SCALE, NEW_SCALE
    )

    assert ok is True
    # Multiplies stored lengths by the ratio and rewrites notes to Q=...
    assert _measurement(real_db, seed.spore_id) == pytest.approx(
        {"length_um": 20.0, "width_um": 8.0, "notes": "Q=2.5", "measurement_type": "manual"}
    )
    # Calibration rows are rescaled too, and their "Scale bar" note is replaced.
    assert _measurement(real_db, seed.calibration_id) == pytest.approx(
        {"length_um": 40.0, "width_um": 4.0, "notes": "Q=10.0", "measurement_type": "calibration"}
    )
    # Updates the image scale itself.
    assert _image_scale(real_db, seed.image_id) == pytest.approx(NEW_SCALE)
    # Suspected defect: the observation is not marked dirty. The dialog runs this
    # when the objective changes, before (and independently of) the apply step
    # whose ImageDB.update_image would mark it dirty.
    assert _sync_status(real_db, seed.obs_id) == "synced"


def test_image_import_dialog_rescale_overwrites_free_text_notes(real_db):
    seed = _seed(real_db)
    _sql(real_db, "UPDATE spore_measurements SET notes = 'odd apiculus' WHERE id = ?", (seed.spore_id,))

    image_import_dialog.ImageImportDialog._rescale_measurements_for_image(
        SimpleNamespace(), seed.image_id, OLD_SCALE, NEW_SCALE
    )

    # Suspected defect: any note text is replaced by the recomputed Q value.
    assert _measurement(real_db, seed.spore_id)["notes"] == "Q=2.5"


def test_observations_tab_rescale(real_db):
    seed = _seed(real_db)

    observations_tab.ObservationsTab._rescale_measurement_lengths(
        SimpleNamespace(), seed.image_id, OLD_SCALE, NEW_SCALE
    )

    # Multiplies lengths by the ratio; leaves notes alone.
    assert _measurement(real_db, seed.spore_id) == pytest.approx(
        {"length_um": 20.0, "width_um": 8.0, "notes": "Q=2.5", "measurement_type": "manual"}
    )
    # Skips calibration rows entirely.
    assert _measurement(real_db, seed.calibration_id) == pytest.approx(
        {"length_um": 20.0, "width_um": 2.0, "notes": "Scale bar: 20.0 µm", "measurement_type": "calibration"}
    )
    # Leaves the image scale and sync state to its caller, which passes the new
    # scale to ImageDB.update_image.
    assert _image_scale(real_db, seed.image_id) == pytest.approx(OLD_SCALE)
    assert _sync_status(real_db, seed.obs_id) == "synced"


class _AcceptingMessageBox:
    Question = None
    AcceptRole = None
    RejectRole = None

    def __init__(self, *_args, **_kwargs):
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


def test_main_window_rescale(real_db, monkeypatch):
    seed = _seed(real_db)
    monkeypatch.setattr(main_window, "QMessageBox", _AcceptingMessageBox)
    stub = _main_window_stub(seed.image_id)

    ok = main_window.MainWindow._maybe_rescale_current_image(stub, OLD_SCALE, NEW_SCALE)

    assert ok is True
    # Recomputes from the stored pixel points with the new scale (old scale is
    # only used for the "changed?" check) and rewrites notes to Q=...
    assert _measurement(real_db, seed.spore_id) == pytest.approx(
        {"length_um": 20.0, "width_um": 8.0, "notes": "Q=2.5", "measurement_type": "manual"}
    )
    # Calibration rows are recomputed too.
    assert _measurement(real_db, seed.calibration_id) == pytest.approx(
        {"length_um": 40.0, "width_um": 4.0, "notes": "Q=10.0", "measurement_type": "calibration"}
    )
    # The image scale is left to the caller (apply_objective / set_custom_scale
    # call ImageDB.update_image afterwards). Sync state is not asserted here:
    # the stubbed update_statistics really persists the observation's spore
    # summary via ObservationDB.update_spore_statistics, which marks it dirty
    # when the summary changes, and the caller's update_image marks it dirty.
    assert _image_scale(real_db, seed.image_id) == pytest.approx(OLD_SCALE)
    assert stub.calls == ["lines", "table", "stats"]


def test_main_window_rescale_cancel_changes_nothing(real_db, monkeypatch):
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


def test_calibration_db_recalculate_for_calibration(real_db):
    seed = _seed(real_db)
    old_cal = models.CalibrationDB.add_calibration(
        objective_key="40x", microns_per_pixel=OLD_SCALE, calibration_date="2026-01-01"
    )
    _sql(real_db, "UPDATE images SET calibration_id = ? WHERE id = ?", (old_cal, seed.image_id))

    updated = models.CalibrationDB.recalculate_measurements_for_calibration(old_cal, old_cal, NEW_SCALE)

    assert updated == 2
    # Ratio rescale of every row, calibration included; notes untouched.
    assert _measurement(real_db, seed.spore_id) == pytest.approx(
        {"length_um": 20.0, "width_um": 8.0, "notes": "Q=2.5", "measurement_type": "manual"}
    )
    assert _measurement(real_db, seed.calibration_id) == pytest.approx(
        {"length_um": 40.0, "width_um": 4.0, "notes": "Scale bar: 20.0 µm", "measurement_type": "calibration"}
    )
    assert _image_scale(real_db, seed.image_id) == pytest.approx(NEW_SCALE)
    # The only rescale path that marks the observation dirty itself.
    assert _sync_status(real_db, seed.obs_id) == "dirty"


# --- Calibration upsert paths ---------------------------------------------

SELECTION = ((0.0, 0.0), (200.0, 0.0))


def _upsert_main_window(image_id, selection, total_um, pixel_dist):
    main_window.MainWindow._upsert_calibration_measurement(
        SimpleNamespace(), image_id, selection, total_um, pixel_dist
    )


def _upsert_image_import(image_id, selection, total_um, pixel_dist):
    image_import_dialog.ImageImportDialog._upsert_calibration_measurement(
        SimpleNamespace(), image_id, selection, total_um, pixel_dist
    )


def _upsert_observations_tab(image_id, selection, total_um, pixel_dist):
    # This copy has no pixel_dist parameter.
    observations_tab.ObservationsTab._upsert_calibration_measurement_for_image(
        SimpleNamespace(), image_id, selection, total_um
    )


@pytest.mark.parametrize(
    "upsert", [_upsert_main_window, _upsert_image_import, _upsert_observations_tab], ids=["main_window", "image_import", "observations_tab"]
)
def test_upsert_replaces_calibration_and_marks_dirty(real_db, upsert):
    seed = _seed(real_db)

    upsert(seed.image_id, SELECTION, 30.0, 200.0)

    rows = _calibration_rows(real_db, seed.image_id)
    assert len(rows) == 1
    assert rows[0] == pytest.approx(
        {
            "measurement_type": "calibration",
            "length_um": 30.0,
            "width_um": 3.0,
            "notes": "Scale bar: 30.0 µm",
            "p1_x": 0.0, "p1_y": 0.0, "p2_x": 200.0, "p2_y": 0.0,
            "p3_x": 100.0, "p3_y": -10.0, "p4_x": 100.0, "p4_y": 10.0,
        }
    )
    # MeasurementDB.add_measurement marks the observation dirty.
    assert _sync_status(real_db, seed.obs_id) == "dirty"


@pytest.mark.parametrize(
    "upsert, expected_half_width",
    [(_upsert_main_window, 5.0), (_upsert_image_import, 5.0), (_upsert_observations_tab, 10.0)],
    ids=["main_window", "image_import", "observations_tab"],
)
def test_upsert_width_line_uses_pixel_dist_or_selection_length(real_db, upsert, expected_half_width):
    """Drift: two copies size the width line from pixel_dist, one from the selection."""
    seed = _seed(real_db)

    upsert(seed.image_id, SELECTION, 30.0, 100.0)  # pixel_dist differs from the 200 px selection

    row = _calibration_rows(real_db, seed.image_id)[0]
    assert row["p3_y"] == pytest.approx(-expected_half_width)
    assert row["p4_y"] == pytest.approx(expected_half_width)


@pytest.mark.parametrize(
    "upsert, expected_rows",
    [(_upsert_main_window, 2), (_upsert_image_import, 2), (_upsert_observations_tab, 1)],
    ids=["main_window", "image_import", "observations_tab"],
)
def test_upsert_with_capitalised_calibration_type(real_db, upsert, expected_rows):
    """Drift: only the case-insensitive copy replaces a 'Calibration' row."""
    seed = _seed(real_db, calibration_type="Calibration")

    upsert(seed.image_id, SELECTION, 30.0, 200.0)

    assert len(_calibration_rows(real_db, seed.image_id)) == expected_rows


def test_upsert_failure_after_delete_loses_the_calibration(real_db, monkeypatch):
    """Suspected defect: DELETE and INSERT are separate transactions."""
    seed = _seed(real_db)

    def _boom(**_kwargs):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(models.MeasurementDB, "add_measurement", staticmethod(_boom))
    monkeypatch.setattr(main_window.MeasurementDB, "add_measurement", staticmethod(_boom))

    with pytest.raises(sqlite3.OperationalError):
        _upsert_main_window(seed.image_id, SELECTION, 30.0, 200.0)

    assert _calibration_rows(real_db, seed.image_id) == []
