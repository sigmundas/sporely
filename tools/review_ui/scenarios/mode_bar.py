"""The mode bar above the main window's pages (redesign plan stage 9a).

A real ``MainWindow`` in the shared isolated review profile (see
``tools/review_ui/live_lab_harness.py``): no network, credentials or live
data. One observation with a microscope image, selected, so Edit is enabled.
"""
from __future__ import annotations

from PySide6.QtWidgets import QWidget

from ..context import ReviewContext
from ..registry import ReviewScenario, ScenarioRegistry
from ..live_lab_harness import review_profile
from .measure_tab import _seed, _settle


def _window(context: ReviewContext, *, page: str, size: tuple[int, int], waiting: int = 0) -> QWidget:
    from ui.main_window import MainWindow

    observation_id, _image_id = _seed(context, note=None)
    window = MainWindow()
    window.resize(*size)
    window.show()
    _settle(0.3)
    tab = window.observations_tab
    tab.refresh_observations()
    _settle(0.3)
    row = tab._find_table_row_for_observation(observation_id)
    if row >= 0:
        tab.table.selectRow(row)
    window.on_observation_selected(observation_id, "Inocybe geophylla", switch_tab=False)
    window.tab_widget.setCurrentWidget(getattr(window, page))
    if waiting:
        # Layout only: the badge as it shows after a Camera import scan.
        window.mode_bar.set_waiting_count(waiting)
    _settle()
    return window


NAMES = (
    ("Gulgrønn lærhatt", "Cortinarius", "olivaceofuscus"),
    ("Silketrevlesopp-gruppen", "Inocybe", "geophylla"),
    ("", "Flammulina", "velutipes"),
    ("Svartrandkjuke", "Bjerkandera", "adusta"),
    ("Knuskkjuke", "Fomes", "fomentarius"),
)


def _rail(context: ReviewContext, *, size: tuple[int, int], open_drawer: bool) -> QWidget:
    """Mål with a handful of observations; the list's drawer open or only the rail."""
    from database.models import ObservationDB

    def more(window):
        tab = window.observations_tab
        for common, genus, species in NAMES:
            ObservationDB.create_observation(
                date="2026-10-31 08:50", genus=genus, species=species, common_name=common or None,
            )
        tab.refresh_observations()
        _settle(0.4)
        window._select_active_observation_row()
        _settle(0.2)
        if open_drawer:
            window._toggle_observation_drawer()
            _settle(0.5)

    window = _window(context, page="measure_tab", size=size)
    more(window)
    return window


def _staged(context: ReviewContext, *, theme_dark: bool = False) -> QWidget:
    """Mål's shared strip with two unsaved images first (stage 9c's staged section)."""
    from database.models import ImageDB

    window = _window(context, page="measure_tab", size=(1500, 950))
    profile = review_profile(context)
    observation_id = int(window.active_observation_id)
    for n in range(4):
        ImageDB.add_image(observation_id, str(profile.jpeg(f"S{n}.JPG", seed=10 + n)), "microscope",
                          scale=0.1, objective_name="100X", contrast="DIC")
    window.refresh_observation_images(select_image_id=window.current_image_id)
    _settle(0.3)
    staged = [
        {"id": None, "filepath": str(profile.jpeg(f"U{n}.JPG", seed=30 + n)), "image_number": n + 1}
        for n in range(2)
    ]
    window.measure_gallery.set_staged_items(observation_id, staged)
    _settle(0.5)
    return window


def register_mode_bar_scenarios(registry: ScenarioRegistry) -> None:
    for theme in ("light", "dark"):
        suffix = "" if theme == "light" else "-dark"
        registry.register(
            ReviewScenario(
                id=f"image-strip.staged{suffix}",
                group="mode-bar",
                title=f"Shared strip with unsaved images (nb{', dark' if theme == 'dark' else ''})",
                description="Two unsaved images first with an orange border, then the observation's images (stage 9c).",
                build=lambda context: _staged(context),
                viewport=(1500, 950),
                locale="nb_NO",
                theme=theme,
            )
        )
    for open_drawer, state in ((False, "rail"), (True, "drawer")):
        for theme in ("light", "dark"):
            suffix = "" if theme == "light" else "-dark"
            registry.register(
                ReviewScenario(
                    id=f"observation-list.{state}{suffix}",
                    group="mode-bar",
                    title=f"Observation list, {state} in Mål (nb{', dark' if theme == 'dark' else ''})",
                    description="The rail at Mål's left edge; open, the list drawer over the page (stage 9b).",
                    build=lambda context, open_drawer=open_drawer: _rail(context, size=(1500, 950), open_drawer=open_drawer),
                    viewport=(1500, 950),
                    locale="nb_NO",
                    theme=theme,
                )
            )
    registry.register(
        ReviewScenario(
            id="observation-list.drawer-de",
            group="mode-bar",
            title="Observation list drawer (de)",
            description="The list drawer with German strings in a small window.",
            build=lambda context: _rail(context, size=(1100, 720), open_drawer=True),
            viewport=(1100, 720),
            locale="de_DE",
        )
    )
    for page, label in (("observations_tab", "view"), ("measure_tab", "measure")):
        for theme in ("light", "dark"):
            suffix = "" if theme == "light" else "-dark"
            registry.register(
                ReviewScenario(
                    id=f"mode-bar.{label}{suffix}",
                    group="mode-bar",
                    title=f"Mode bar, {label} (nb{', dark' if theme == 'dark' else ''})",
                    description="Vis, Rediger, Bilder, Mål, Analyse; Kameraimport with 12 photos waiting at the far right (stage 9a).",
                    build=lambda context, page=page: _window(context, page=page, size=(1500, 950), waiting=12),
                    viewport=(1500, 950),
                    locale="nb_NO",
                    theme=theme,
                )
            )
    registry.register(
        ReviewScenario(
            id="mode-bar.view-de",
            group="mode-bar",
            title="Mode bar, view (de)",
            description="The mode bar with German strings, the longest labels.",
            build=lambda context: _window(context, page="observations_tab", size=(1500, 950), waiting=12),
            viewport=(1500, 950),
            locale="de_DE",
        )
    )
    registry.register(
        ReviewScenario(
            id="mode-bar.small-de",
            group="mode-bar",
            title="Mode bar, small window (de)",
            description="German strings in a 1100 × 720 window, the tightest fit.",
            build=lambda context: _window(context, page="measure_tab", size=(1100, 720), waiting=12),
            viewport=(1100, 720),
            locale="de_DE",
        )
    )
