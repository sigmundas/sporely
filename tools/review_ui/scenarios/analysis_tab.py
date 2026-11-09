"""The Analysis tab in a real ``MainWindow`` (redesign plan stage 7).

A *Cortinarius limonius* observation with 24 measured spores, a reference
library seeded like the reference-picker scenarios (``references._seed_library``)
and one of its measurement sets attached, in the shared isolated review
profile (``tools/review_ui/live_lab_harness.py``). No network or live data.
"""
from __future__ import annotations

import random

from PySide6.QtCore import QPointF
from PySide6.QtWidgets import QWidget

from ..context import ReviewContext
from ..live_lab_harness import create_observation, review_profile, use_database
from ..registry import ReviewScenario, ScenarioRegistry
from .measure_tab import _settle


def seed_analysis(context: ReviewContext, *, attach: bool = True) -> dict:
    """An observation with spores, a seeded library and (optionally) one attached set."""
    from database.models import ImageDB, MeasurementDB, ObservationDB
    from database.reference_library import ObservationReferenceUseRepository

    from .references import _seed_library

    profile = review_profile(context)
    count = int(context.state.get("analysis-tab.builds", 0)) + 1
    context.state["analysis-tab.builds"] = count
    use_database(profile, f"analysis-tab-{count}")
    observation_id = create_observation(
        profile, f"analysis-{count}", genus="Cortinarius", species="limonius", common_name="Sitronslørsopp",
    )
    ObservationDB.update_observation(observation_id, sporely_taxon_id=7)
    image_id = int(
        ImageDB.add_image(
            observation_id, str(profile.jpeg(f"A{count:04d}.JPG", seed=5)), "microscope",
            scale=0.1, objective_name="100X", contrast="DIC",
        )
    )
    rng = random.Random(11)
    for index in range(24):
        length = round(rng.gauss(9.7, 0.7), 2)
        width = round(rng.gauss(5.2, 0.35), 2)
        x, y = 40 + (index % 8) * 50, 40 + (index // 8) * 60
        points = [QPointF(x, y), QPointF(x + 28, y), QPointF(x + 14, y - 8), QPointF(x + 14, y + 8)]
        MeasurementDB.add_measurement(image_id, length, width, measurement_type="Spores", points=points)
    _work, sets = _seed_library()
    if attach:
        ObservationReferenceUseRepository.attach(observation_id, sets[0].id, role="compared")
    return {"observation_id": observation_id, "image_id": image_id, "sets": sets}


def _analysis_tab(context: ReviewContext, *, size: tuple[int, int] = (1500, 950), after=None) -> QWidget:
    from ui.main_window import MainWindow

    seeded = seed_analysis(context)
    window = MainWindow()
    window.resize(*size)
    window.show()
    _settle(0.2)
    window.on_observation_selected(seeded["observation_id"], "Cortinarius limonius", switch_tab=False)
    window.tab_widget.setCurrentWidget(window.analysis_tab)
    _settle(0.8)
    if after is not None:
        after(window)
        _settle(0.5)
    return window


def _open_drawer(window) -> None:
    window._toggle_reference_drawer()
    page = window.reference_picker_page
    rows = [page.results_list.itemWidget(page.results_list.item(i)) for i in range(page.results_list.count())]
    ticked = [row for row in rows if row is not None and row.is_checked()]
    if ticked:
        ticked[0].set_expanded(True)


def register_analysis_tab_scenarios(registry: ScenarioRegistry) -> None:
    for theme in ("light", "dark"):
        suffix = "" if theme == "light" else "-dark"
        registry.register(
            ReviewScenario(
                id=f"analysis.tab{suffix}",
                group="analysis",
                title=f"Analysis tab (nb{', dark' if theme == 'dark' else ''})",
                description="24 spores and one attached library reference in the plot and the comparison list.",
                build=lambda context: _analysis_tab(context),
                viewport=(1500, 950),
                locale="nb_NO",
                theme=theme,
            )
        )
    for theme in ("light", "dark"):
        suffix = "" if theme == "light" else "-dark"
        registry.register(
            ReviewScenario(
                id=f"analysis.reference-drawer{suffix}",
                group="analysis",
                title=f"Analysis tab, reference drawer open (nb{', dark' if theme == 'dark' else ''})",
                description="The reference drawer over the plot: one set attached (ticked), one row expanded (stage 7).",
                build=lambda context: _analysis_tab(context, after=_open_drawer),
                viewport=(1500, 950),
                locale="nb_NO",
                theme=theme,
            )
        )
    registry.register(
        ReviewScenario(
            id="analysis.reference-drawer-de",
            group="analysis",
            title="Analysis tab, reference drawer open (de)",
            description="The reference drawer with German strings.",
            build=lambda context: _analysis_tab(context, after=_open_drawer),
            viewport=(1500, 950),
            locale="de_DE",
        )
    )
    registry.register(
        ReviewScenario(
            id="analysis.tab-small",
            group="analysis",
            title="Analysis tab, small window (nb)",
            description="The Analysis tab in a 1100 × 720 window.",
            build=lambda context: _analysis_tab(context, size=(1100, 720)),
            viewport=(1100, 720),
            locale="nb_NO",
        )
    )
