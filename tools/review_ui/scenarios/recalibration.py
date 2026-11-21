"""Re-calibration scope: how far back a new objective calibration applies.

The real ``RecalibrationScopeDialog`` against an isolated temporary database:
a previous 63x calibration with microscope images taken over the last weeks,
some in observations with several images, one image with no capture date.
"Today" is fixed so the date window is deterministic.
"""
from __future__ import annotations

from datetime import date

from PySide6.QtWidgets import QWidget

from ..context import ReviewContext
from ..live_lab_harness import review_profile, use_database
from ..registry import ReviewScenario, ScenarioRegistry


TODAY = date(2026, 11, 21)
OLD_SCALE = 0.0534937
NEW_SCALE = 0.0541120

# (genus, species, observation date, [(file name, captured_at or None, spores)])
_OBSERVATIONS = (
    ("Cortinarius", "caperatus", "2026-11-14", [
        ("P1210042.jpg", "2026-11-21 10:12:00", 32),
        ("P1210043.jpg", "2026-11-21 10:14:00", 18),
        ("P1210044.jpg", "2026-11-21 10:19:00", 7),
    ]),
    ("Inocybe", "geophylla", "2026-11-12", [
        ("P1200871.jpg", "2026-11-20 21:40:00", 25),
        ("P1200872.jpg", None, 11),
    ]),
    ("Russula", "decolorans", "2026-11-01", [
        ("P1180510.jpg", "2026-11-18 19:05:00", 40),
    ]),
    ("Lactarius", "deterrimus", "2026-10-25", [
        ("P1070233.jpg", "2026-11-07 21:30:00", 30),
        ("P1070234.jpg", "2026-11-07 21:33:00", 30),
    ]),
    ("Hygrocybe", "conica", "2026-10-02", [
        ("P1030102.jpg", "2026-10-03 14:00:00", 22),
    ]),
)


def _seed(context: ReviewContext) -> int:
    """Create the fixture once; return the previous calibration's id."""
    profile = review_profile(context)
    use_database(profile, "recalibration")
    existing = context.state.get("recalibration.old-calibration")
    if existing is not None:
        return existing

    from database import schema
    from database.models import CalibrationDB, ImageDB, MeasurementDB, ObservationDB

    old_cal = CalibrationDB.add_calibration(
        objective_key="63X_1.32_PL_APO", microns_per_pixel=OLD_SCALE, calibration_date="2026-07-04 00:14:28"
    )
    CalibrationDB.add_calibration(
        objective_key="63X_1.32_PL_APO", microns_per_pixel=NEW_SCALE, calibration_date="2026-11-21 11:02:00"
    )
    conn = schema.get_connection()
    try:
        for genus, species, obs_date, images in _OBSERVATIONS:
            obs_id = ObservationDB.create_observation(date=obs_date, genus=genus, species=species)
            for name, captured_at, spores in images:
                image_id = ImageDB.add_image(
                    observation_id=obs_id,
                    filepath=str(profile.root / "images-recalibration" / name),
                    image_type="microscope",
                    scale=OLD_SCALE,
                    calibration_id=old_cal,
                    copy_to_folder=False,
                )
                # Exact dates: no EXIF lookup, and the added date of the image
                # without a capture date is fixed too.
                conn.execute(
                    "UPDATE images SET captured_at = ?, created_at = ? WHERE id = ?",
                    (captured_at, "2026-11-20 12:00:00", image_id),
                )
                conn.commit()
                for index in range(spores):
                    MeasurementDB.add_measurement(
                        image_id=image_id, length=9.0 + index * 0.05, width=6.0, notes="Q=1.5"
                    )
    finally:
        conn.close()
    context.state["recalibration.old-calibration"] = old_cal
    return old_cal


def _dialog(context: ReviewContext, *, recent_days: int | None = None, new_only: bool = False) -> QWidget:
    old_cal = _seed(context)
    from ui.recalibration_scope_dialog import RecalibrationScopeDialog

    dialog = RecalibrationScopeDialog(
        context.host,
        calibration_id=old_cal,
        old_scale_um=OLD_SCALE,
        new_scale_um=NEW_SCALE,
        today=TODAY,
    )
    if new_only:
        dialog.new_only_radio.setChecked(True)
    elif recent_days is not None:
        dialog.recent_radio.setChecked(True)
        dialog.days_spin.setValue(recent_days)
    # Open the two newest observations so their images show, including the
    # one dated by when it was added (italic, with a tooltip).
    for index in range(min(2, dialog.preview_tree.topLevelItemCount())):
        dialog.preview_tree.topLevelItem(index).setExpanded(True)
    return dialog


def register_recalibration_scenarios(registry: ScenarioRegistry) -> None:
    scenarios = (
        ReviewScenario(
            id="recalibration.update-existing",
            group="recalibration",
            title="Re-calibration scope — update existing images (nb)",
            description="Default choice: every image on the previous calibration, grouped by observation, with counts.",
            build=lambda context: _dialog(context),
            viewport=(640, 600),
            locale="nb_NO",
        ),
        ReviewScenario(
            id="recalibration.update-recent",
            group="recalibration",
            title="Re-calibration scope — recent images (nb)",
            description="Today and the previous 3 days: the list narrows to images taken on or after 2026-11-18; one is dated by when it was added.",
            build=lambda context: _dialog(context, recent_days=3),
            viewport=(640, 600),
            locale="nb_NO",
        ),
        ReviewScenario(
            id="recalibration.update-recent-dark",
            group="recalibration",
            title="Re-calibration scope — recent images, dark (nb)",
            description="The recent-images choice in the dark theme.",
            build=lambda context: _dialog(context, recent_days=3),
            viewport=(640, 600),
            locale="nb_NO",
            theme="dark",
        ),
        ReviewScenario(
            id="recalibration.new-only-dark",
            group="recalibration",
            title="Re-calibration scope — new calibration only, dark (nb)",
            description="Nothing existing changes: the list is empty and disabled.",
            build=lambda context: _dialog(context, new_only=True),
            viewport=(640, 600),
            locale="nb_NO",
            theme="dark",
        ),
        ReviewScenario(
            id="recalibration.update-recent-small",
            group="recalibration",
            title="Re-calibration scope — small window (nb)",
            description="A small window: text wraps, the list shrinks and scrolls.",
            build=lambda context: _dialog(context, recent_days=3),
            viewport=(460, 480),
            locale="nb_NO",
        ),
        ReviewScenario(
            id="recalibration.update-recent-en",
            group="recalibration",
            title="Re-calibration scope — recent images (en)",
            description="The recent-images choice in English.",
            build=lambda context: _dialog(context, recent_days=3),
            viewport=(640, 600),
        ),
    )
    for scenario in scenarios:
        registry.register(scenario)
