"""The mode bar above the main window's pages (redesign plan stage 9a).

A real ``MainWindow`` in the shared isolated review profile (see
``tools/review_ui/live_lab_harness.py``): no network, credentials or live
data. One observation with a microscope image, selected, so Edit is enabled.
"""
from __future__ import annotations

from PySide6.QtWidgets import QWidget

from ..context import ReviewContext
from ..registry import ReviewScenario, ScenarioRegistry
from ..live_lab_harness import create_observation, review_profile
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


class _DrainingWindow(QWidget):
    """Hosts the main window; closing stops Live Lab's session and waits for its RAW work."""

    def __init__(self, window, parent=None) -> None:
        from PySide6.QtWidgets import QVBoxLayout

        super().__init__(parent)
        self.window_under_review = window
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(window)

    def closeEvent(self, event) -> None:  # noqa: N802 - Qt override
        from ..live_lab_harness import drain_live_lab, pump_until

        drain_live_lab(self.window_under_review.live_lab_tab, pump_until)
        self.window_under_review.files_source_page.shutdown()  # discard: no quit question offscreen
        super().closeEvent(event)


def _images_live(context: ReviewContext) -> QWidget:
    """Bilder with a running session: two unsaved RAW captures first, then three saved images (stage 9d)."""
    from database.models import ImageDB
    from ui.main_window import MainWindow

    from ..live_lab_harness import FolderDropWatcher, pump_until
    from .live_lab import _fresh_profile, _reset_live_lab_settings

    profile = _fresh_profile(context, "images-live")
    observation_id = create_observation(profile, "images-live")
    for n in range(3):
        ImageDB.add_image(observation_id, str(profile.jpeg(f"saved{n}.JPG", seed=20 + n)), "microscope",
                          scale=0.1, objective_name="100X", contrast="DIC")
    _reset_live_lab_settings(session_mode="live", raw_mode="review", watch_dir=str(profile.watch_dir))
    window = MainWindow()
    host = _DrainingWindow(window)
    host.resize(1500, 950)
    host.show()
    _settle(0.3)
    window.on_observation_selected(observation_id, "Inocybe geophylla", switch_tab=False)
    window.tab_widget.setCurrentWidget(window.live_lab_tab)
    _settle(0.3)
    live = window.live_lab_tab
    live.start_session()
    pump_until(live.is_session_running)
    for name in ("P1010401.ORF", "P1010402.ORF"):
        before = len(live._pending_raw_captures)
        FolderDropWatcher.created[-1].drop(profile.raw(name))
        pump_until(lambda: len(live._pending_raw_captures) == before + 1)
    pump_until(lambda: not live.pending_raw_frame.isHidden())
    _settle(0.6)
    return host


def _images_files(context: ReviewContext, *, size: tuple[int, int] = (1500, 950)) -> QWidget:
    """Bilder › Files and SD card: Prepare Images in place, one saved and one new image (stage 9f)."""
    from database.models import ImageDB
    from ui.main_window import MainWindow

    from .live_lab import _fresh_profile

    profile = _fresh_profile(context, "images-files")
    observation_id = create_observation(profile, "images-files")
    ImageDB.add_image(observation_id, str(profile.jpeg("field1.JPG", seed=31)), "field")
    window = MainWindow()
    host = _DrainingWindow(window)
    host.resize(*size)
    host.show()
    _settle(0.3)
    window.on_observation_selected(observation_id, "Inocybe geophylla", switch_tab=False)
    window.tab_widget.setCurrentWidget(window.live_lab_tab)
    window.live_lab_tab.show_source(window.live_lab_tab.SOURCE_FILES)
    _settle(0.5)
    window.files_source_page.editor().add_images([str(profile.jpeg("new1.JPG", seed=32))])
    _settle(0.8)
    return host


def register_mode_bar_scenarios(registry: ScenarioRegistry) -> None:
    for theme in ("light", "dark"):
        suffix = "" if theme == "light" else "-dark"
        registry.register(
            ReviewScenario(
                id=f"images.files{suffix}",
                group="mode-bar",
                title=f"Bilder › Files and SD card (nb{', dark' if theme == 'dark' else ''})",
                description="Prepare Images in place in Bilder, one new unsaved image (stage 9f).",
                build=lambda context: _images_files(context),
                viewport=(1500, 950),
                locale="nb_NO",
                theme=theme,
            )
        )
    registry.register(
        ReviewScenario(
            id="images.files-small-de",
            group="mode-bar",
            title="Bilder › Files and SD card, small window (de)",
            description="German strings in a 1100 × 720 window.",
            build=lambda context: _images_files(context, size=(1100, 720)),
            viewport=(1100, 720),
            locale="de_DE",
        )
    )
    for theme in ("light", "dark"):
        suffix = "" if theme == "light" else "-dark"
        registry.register(
            ReviewScenario(
                id=f"images.live{suffix}",
                group="mode-bar",
                title=f"Bilder with unsaved captures (nb{', dark' if theme == 'dark' else ''})",
                description="The shared strip in Bilder: two unsaved RAW captures first, then the saved images; saving above the strip (stage 9d).",
                build=lambda context: _images_live(context),
                viewport=(1500, 950),
                locale="nb_NO",
                theme=theme,
            )
        )
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
