"""Observation image-view scenarios with deterministic image metadata."""
from __future__ import annotations

from unittest.mock import patch

from PySide6.QtGui import QColor, QPixmap

from ui import observations_tab

from ..context import ReviewContext
from ..registry import ReviewScenario, ScenarioRegistry


def _image_metadata_tags(context: ReviewContext):
    assert context.temporary_root is not None
    image_path = context.temporary_root / "observation-image.png"
    pixmap = QPixmap(960, 600)
    pixmap.fill(QColor("#d9d2c3"))
    pixmap.save(str(image_path))
    context.enter_fixture(
        patch.object(
            observations_tab.ImageDB,
            "get_image",
            lambda _image_id: {
                "id": 7,
                "objective_name": "40x",
                "contrast": "DIC",
                "mount_medium": "Water",
                "stain": "Not_set",
                "sample_type": "Fresh",
                "sample_source": "Hymenium",
            },
        )
    )
    context.enter_fixture(
        patch.object(
            observations_tab,
            "load_objectives",
            lambda: {"40x": {"magnification": 40, "name": "40x"}},
        )
    )
    browser = observations_tab._ObservationImageBrowser()
    browser.set_items([{"id": 7, "path": str(image_path)}])
    return browser


def register_observation_scenarios(registry: ScenarioRegistry) -> None:
    registry.register(
        ReviewScenario(
            id="observations.image-metadata-tags",
            group="observations",
            title="Observation image metadata tags",
            description="Image view exposes editable microscope tags and an explicit no-stain state.",
            viewport=(1040, 680),
            build=_image_metadata_tags,
        )
    )
