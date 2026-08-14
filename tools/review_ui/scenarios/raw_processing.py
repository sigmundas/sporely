"""RAW processing control scenarios."""
from __future__ import annotations

from PySide6.QtWidgets import QVBoxLayout, QWidget

from ui.raw_processing_controls import RawProcessingControls
from utils.raw_render import RawRenderSettings

from ..context import ReviewContext
from ..registry import ReviewScenario, ScenarioRegistry


def _method_selector(_context: ReviewContext):
    panel = QWidget()
    layout = QVBoxLayout(panel)
    controls = RawProcessingControls(panel)
    controls.set_settings(
        RawRenderSettings(
            auto_levels=True,
            auto_levels_method="b",
            auto_levels_clipping=False,
            light_ev=0.284,
            dark_ev=-0.091,
            auto_black_level=0.061,
            auto_white_level=0.821,
        )
    )
    layout.addWidget(controls)
    layout.addStretch(1)
    return panel


def register_raw_processing_scenarios(registry: ScenarioRegistry) -> None:
    registry.register(
        ReviewScenario(
            id="raw-processing.methods",
            group="raw-processing",
            title="Auto-level method selector",
            description="The shared RAW controls show Method A/B and Clipping On/Off choices beside Auto levels.",
            viewport=(520, 640),
            build=_method_selector,
        )
    )
