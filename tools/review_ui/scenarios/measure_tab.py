"""The Measure tab in a real ``MainWindow`` (redesign plan stage 6).

A microscope image with three spore measurements in the shared isolated
review profile (see ``tools/review_ui/live_lab_harness.py``): no network,
credentials or live data. ``MainWindow`` is built in full, so the screenshots
show the tab as the user sees it.
"""
from __future__ import annotations

import time

from PySide6.QtCore import QPointF
from PySide6.QtWidgets import QApplication, QWidget

from ..context import ReviewContext
from ..live_lab_harness import create_observation, review_profile, use_database
from ..registry import ReviewScenario, ScenarioRegistry


def _settle(seconds: float = 0.6) -> None:
    app = QApplication.instance()
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        app.processEvents()
        time.sleep(0.02)


def _seed(context: ReviewContext, *, note: str | None) -> tuple[int, int]:
    from database.models import ImageDB, MeasurementDB

    profile = review_profile(context)
    count = int(context.state.get("measure-tab.builds", 0)) + 1
    context.state["measure-tab.builds"] = count
    use_database(profile, f"measure-tab-{count}")
    observation_id = create_observation(profile, f"measure-{count}")
    path = profile.jpeg(f"M{count:04d}.JPG", seed=3)
    image_id = int(
        ImageDB.add_image(
            observation_id,
            str(path),
            "microscope",
            scale=0.1,
            objective_name="100X",
            contrast="DIC",
            mount_medium="KOH",
            stain="Melzer",
            notes=note,
        )
    )
    for x, y in ((120, 90), (300, 160), (380, 240)):
        # A rectangle measurement: the length line, then the width line.
        points = [QPointF(x - 14, y), QPointF(x + 14, y), QPointF(x, y - 9), QPointF(x, y + 9)]
        MeasurementDB.add_measurement(image_id, 2.8, 1.8, measurement_type="Spores", points=points)
    return observation_id, image_id


def _measure_tab(context: ReviewContext, *, size: tuple[int, int] = (1500, 950), note: str | None = None,
                 after=None) -> QWidget:
    from ui.main_window import MainWindow

    observation_id, _image_id = _seed(context, note=note)
    window = MainWindow()
    window.resize(*size)
    window.show()
    _settle(0.2)
    window.on_observation_selected(observation_id, "Inocybe geophylla", switch_tab=False)
    window.tab_widget.setCurrentWidget(window.measure_tab)
    _settle()
    if after is not None:
        after(window)
        _settle(0.4)
    return window


def register_measure_tab_scenarios(registry: ScenarioRegistry) -> None:
    for theme in ("light", "dark"):
        suffix = "" if theme == "light" else "-dark"
        registry.register(
            ReviewScenario(
                id=f"measure.tab{suffix}",
                group="measure",
                title=f"Measure tab (nb{', dark' if theme == 'dark' else ''})",
                description="A microscope image with three spore measurements, the tools and the Measurements list.",
                build=lambda context: _measure_tab(context, note="Klemmeforbindelser ved septa"),
                viewport=(1500, 950),
                locale="nb_NO",
                theme=theme,
            )
        )
    for page, label in (("view", "Visning"), ("info", "Info")):
        for theme in ("light", "dark"):
            suffix = "" if theme == "light" else "-dark"
            registry.register(
                ReviewScenario(
                    id=f"measure.{page}-drawer{suffix}",
                    group="measure",
                    title=f"Measure tab, {label} drawer open (nb{', dark' if theme == 'dark' else ''})",
                    description=f"The right drawer shows {label} over the image's right edge (stage 6).",
                    build=lambda context, page=page: _measure_tab(
                        context,
                        note="Klemmeforbindelser ved septa",
                        after=lambda window: window.measure_drawer.open_page(page),
                    ),
                    viewport=(1500, 950),
                    locale="nb_NO",
                    theme=theme,
                )
            )
    registry.register(
        ReviewScenario(
            id="measure.view-drawer-de",
            group="measure",
            title="Measure tab, View drawer open (de)",
            description="The View drawer with German strings, the longest labels.",
            build=lambda context: _measure_tab(context, after=lambda window: window.measure_drawer.open_page("view")),
            viewport=(1500, 950),
            locale="de_DE",
        )
    )
    registry.register(
        ReviewScenario(
            id="measure.tab-de",
            group="measure",
            title="Measure tab (de)",
            description="The Measure tab with German strings, the longest labels.",
            build=lambda context: _measure_tab(context),
            viewport=(1500, 950),
            locale="de_DE",
        )
    )
    registry.register(
        ReviewScenario(
            id="measure.tab-small",
            group="measure",
            title="Measure tab, small window (nb)",
            description="The Measure tab in a 1100 × 720 window.",
            build=lambda context: _measure_tab(context, size=(1100, 720)),
            viewport=(1100, 720),
            locale="nb_NO",
        )
    )
