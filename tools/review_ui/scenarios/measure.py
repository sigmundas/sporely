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


def register_measure_scenarios(registry: ScenarioRegistry) -> None:
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
