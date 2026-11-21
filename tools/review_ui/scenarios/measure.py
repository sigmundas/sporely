"""Measure-view scenarios with deterministic microscope metadata."""
from __future__ import annotations

from PySide6.QtGui import QColor, QPixmap

from ui.zoomable_image_widget import ZoomableImageLabel

from ..context import ReviewContext
from ..registry import ReviewScenario, ScenarioRegistry


def _metadata_tags(_context: ReviewContext):
    viewer = ZoomableImageLabel()
    image = QPixmap(960, 600)
    image.fill(QColor("#d9d2c3"))
    viewer.set_image(image)
    viewer.set_top_left_tags(
        (
            ("40x DIC", "#3498db"),
            ("Water", "#59636e"),
            ("Congo Red", "#c0392b"),
            ("Fresh", "#59636e"),
            ("Hymenium", "#59636e"),
        )
    )
    return viewer


def _active_calibration_prompt(context: ReviewContext, *, change: str | None = "+0.5%"):
    """Re-choosing the shown objective for an image on an earlier calibration."""
    from ui.main_window import build_active_calibration_prompt

    box, _move = build_active_calibration_prompt(
        context.host, old_label="2026-06-01", new_label="2026-11-21", change=change
    )
    return box


def register_measure_scenarios(registry: ScenarioRegistry) -> None:
    for scenario in (
        ReviewScenario(
            id="measure.active-calibration-prompt",
            group="measure",
            title="Move image to active calibration (en)",
            description="Re-choosing the image's objective: both calibrations and the change to its measurements.",
            viewport=(520, 200),
            build=_active_calibration_prompt,
        ),
        ReviewScenario(
            id="measure.active-calibration-prompt-nb",
            group="measure",
            title="Move image to active calibration (nb)",
            description="The same question in Norwegian.",
            viewport=(520, 200),
            locale="nb_NO",
            build=_active_calibration_prompt,
        ),
        ReviewScenario(
            id="measure.active-calibration-prompt-de-dark",
            group="measure",
            title="Move image to active calibration, dark (de)",
            description="German, dark theme.",
            viewport=(520, 200),
            locale="de_DE",
            theme="dark",
            build=_active_calibration_prompt,
        ),
        ReviewScenario(
            id="measure.active-calibration-prompt-no-measurements",
            group="measure",
            title="Move image to active calibration, no measurements (en)",
            description="An image without measurements: no change line.",
            viewport=(520, 200),
            build=lambda context: _active_calibration_prompt(context, change=None),
        ),
    ):
        registry.register(scenario)
    registry.register(
        ReviewScenario(
            id="measure.metadata-tags",
            group="measure",
            title="Measure image metadata tags",
            description="Objective and contrast, mount, colored stain, condition, and source share the upper-left row.",
            viewport=(960, 600),
            build=_metadata_tags,
        )
    )
