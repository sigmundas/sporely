"""The Measure tab in a real ``MainWindow`` (redesign plan stage 6).

A microscope image with three spore measurements in the shared isolated
review profile (see ``tools/review_ui/live_lab_harness.py``): no network,
credentials or live data. ``MainWindow`` is built in full, so the screenshots
show the tab as the user sees it.
"""
from __future__ import annotations

import time
from pathlib import Path

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


SETTINGS_OBJECTIVES = {
    "100X": {"name": "100X", "magnification": "100X", "microns_per_pixel": 0.1},
    "40X": {"name": "40X", "magnification": "40X", "microns_per_pixel": 0.25},
    "63X": {"name": "63X", "magnification": "63X", "microns_per_pixel": 0.16},
}


def _microscope_settings(context: ReviewContext) -> QWidget:
    """The batch dialog (stage 8) for two microscope images and a field image, 40X chosen."""
    from database.models import ImageDB
    from ui.microscope_settings_dialog import MicroscopeSettingsDialog

    observation_id, image_id = _seed(context, note=None)
    profile = review_profile(context)
    second = int(ImageDB.add_image(
        observation_id, str(profile.jpeg("M-second.JPG", seed=4)), "microscope",
        scale=0.16, objective_name="63X", contrast="DIC", mount_medium="KOH",
    ))
    field = int(ImageDB.add_image(observation_id, str(profile.jpeg("F-field.JPG", seed=5)), "field"))
    dialog = MicroscopeSettingsDialog(None, [image_id, second, field], objectives=SETTINGS_OBJECTIVES)
    dialog.objective_combo.setCurrentIndex(dialog.objective_combo.findData("40X"))
    dialog.show()
    _settle(0.3)
    return dialog


def _image_failed(context: ReviewContext, window) -> None:
    """Mål: an image that cannot be prepared; its notice, Details open (stage 10f1).
    The real handler; only the RAW library is missing."""
    from unittest.mock import patch

    import ui.main_window as main_window
    from database.models import ImageDB
    from utils.local_image_ingest import RawRenderingUnavailableError

    def unavailable(path, **_kwargs):
        raise RawRenderingUnavailableError("RAW support needs the rawpy package, which is not installed.")

    deadline = time.monotonic() + 5
    while window.current_pixmap is None and time.monotonic() < deadline:  # the first image is shown
        _settle(0.1)
    image = dict(ImageDB.get_images_for_observation(int(window.active_observation_id))[0])
    image["filepath"] = str(Path(image["filepath"]).with_name("IMG_4021.ORF"))
    with patch.object(main_window, "prepare_local_ingest_image", unavailable):  # this call only
        window.load_image_record(image)
    window.measure_hint_bar._details_btn.click()


def _microscope_settings_failed(context: ReviewContext) -> QWidget:
    """The batch dialog after Apply failed: its notice beside the buttons, Details open (stage 10f1)."""
    from unittest.mock import patch

    from database.models import MicroscopeSettingsDB

    def locked(*_args, **_kwargs):
        raise RuntimeError("database is locked")

    dialog = _microscope_settings(context)
    with patch.object(MicroscopeSettingsDB, "apply", locked):  # this click only
        dialog.apply_btn.click()
    dialog.hint_bar._details_btn.click()
    _settle(0.3)
    return dialog


def register_measure_tab_scenarios(registry: ScenarioRegistry) -> None:
    for suffix, theme, locale, size in (
        ("", "light", "nb_NO", (1500, 950)), ("-dark", "dark", "nb_NO", (1500, 950)),
        ("-small-de", "light", "de_DE", (1100, 720)),
    ):
        registry.register(
            ReviewScenario(
                id=f"measure.image-failed{suffix}",
                group="measure",
                title=f"Measure tab: an image that cannot be opened ({locale[:2]}{', dark' if theme == 'dark' else ''})",
                description="Mål's bottom bar shows only with a notice: the error, Details open (stage 10f1).",
                build=lambda context, size=size: _measure_tab(
                    context, size=size, after=lambda window: _image_failed(context, window)
                ),
                viewport=size,
                locale=locale,
                theme=theme,
            )
        )
    for suffix, locale in (("", "nb_NO"), ("-de", "de_DE")):
        registry.register(
            ReviewScenario(
                id=f"measure.microscope-settings-failed{suffix}",
                group="measure",
                title=f"Change microscope settings: Apply failed ({locale[:2]})",
                description="The failure beside the buttons, Details open (stage 10f1).",
                build=lambda context: _microscope_settings_failed(context),
                viewport=(560, 400),
                natural_size=True,
                locale=locale,
            )
        )
    for suffix, theme, locale in (("", "light", "nb_NO"), ("-dark", "dark", "nb_NO"), ("-de", "light", "de_DE")):
        registry.register(
            ReviewScenario(
                id=f"measure.microscope-settings{suffix}",
                group="measure",
                title=f"Change microscope settings ({locale}{', dark' if theme == 'dark' else ''})",
                description="The batch dialog from Mål's gallery: three images selected, 40X chosen (stage 8).",
                build=lambda context: _microscope_settings(context),
                viewport=(560, 360),
                natural_size=True,
                locale=locale,
                theme=theme,
            )
        )
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
