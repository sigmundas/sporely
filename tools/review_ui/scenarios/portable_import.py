"""Portable observation import preview scenarios."""

from __future__ import annotations

from PySide6.QtCore import Qt

from ui.portable_import_dialog import PortableImportDialog
from utils.archive.portable_import import PortableClosureCounts, PortableObservationPreview

from ..context import ReviewContext
from ..registry import ReviewScenario, ScenarioRegistry


class _PreviewFixture:
    created_at = "2026-08-07T12:00:00+02:00"
    app_version = "0.x.x"
    source_platform = "Linux"
    observations = (
        PortableObservationPreview(1, "Amanita muscaria", "2026-08-03", 5),
        PortableObservationPreview(2, "Russula emetica", "2026-08-02", 3),
        PortableObservationPreview(3, "Lactarius turpis", "2026-08-01", 8),
    )
    full_counts = PortableClosureCounts(3, 16, 212, 3, 6)

    def closure_counts(self, observation_ids: set[int]) -> PortableClosureCounts:
        images = {1: 5, 2: 3, 3: 8}
        return PortableClosureCounts(
            len(observation_ids), sum(images[item] for item in observation_ids),
            147 if observation_ids == {1, 2} else 212,
            len(observation_ids), len(observation_ids) * 2,
        )


def _build(context: ReviewContext, *, subset: bool) -> PortableImportDialog:
    dialog = PortableImportDialog(_PreviewFixture(), context.host)
    if subset:
        dialog.observation_table.item(2, 0).setCheckState(Qt.Unchecked)
    return dialog


def register_portable_import_scenarios(registry: ScenarioRegistry) -> None:
    registry.register(ReviewScenario(
        id="portable-import.all-selected", group="portable-import",
        title="Portable import — all selected",
        description="Validated archive metadata and complete observation selection.",
        build=lambda context: _build(context, subset=False), viewport=(900, 650),
    ))
    registry.register(ReviewScenario(
        id="portable-import.subset", group="portable-import",
        title="Portable import — subset",
        description="Changing checkboxes updates the selected dependency closure.",
        build=lambda context: _build(context, subset=True), viewport=(900, 650),
    ))
