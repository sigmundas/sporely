"""Desktop UI redesign stage 0 (quick fixes) review scenarios.

Real production widgets in Norwegian Bokmål for the screens stage 0 touches:
the Preferences publishing page, Camera import, Live Lab, the Prepare Images
footer and the reference library's publication list. Everything runs against
an isolated temporary database, settings file, objectives file and app-data
directory; no stored credentials or login state are read, and nothing is
written outside the renderer's temporary directory.
"""
from __future__ import annotations

import json
from unittest.mock import patch

from PySide6.QtCore import QSettings
from PySide6.QtWidgets import QWidget

from database import schema as db_schema

from ..context import ReviewContext
from ..registry import ReviewScenario, ScenarioRegistry


def _isolate(context: ReviewContext) -> None:
    """Point the database, QSettings and app-data lookups at temp paths."""
    if context.state.get("quick-fixes.isolated"):
        return
    import ui.image_import_dialog as image_import_dialog
    import ui.main_window as main_window
    import ui.window_state as geometry
    from utils.cloud_sync import SporelyCloudClient

    assert context.temporary_root is not None
    root = context.temporary_root / "quick-fixes"
    (root / "app-data").mkdir(parents=True)
    settings_path = root / "settings.ini"
    factory = lambda *_args: QSettings(str(settings_path), QSettings.IniFormat)
    # Every module that writes QSettings directly gets the temp file, so
    # closing a dialog never saves geometry or splitter sizes into the
    # user's real application settings.
    context.enter_fixture(patch.object(geometry, "QSettings", factory))
    context.enter_fixture(patch.object(image_import_dialog, "QSettings", factory))
    # Objectives come from (and may be written to) a temp file, not the
    # user's objectives.json.
    context.enter_fixture(
        patch.object(db_schema, "get_objectives_path", lambda: root / "objectives.json")
    )
    context.enter_fixture(patch.object(db_schema, "get_database_path", lambda: root / "mushrooms.db"))
    context.enter_fixture(
        patch.object(db_schema, "get_reference_database_path", lambda: root / "reference_values.db")
    )
    context.enter_fixture(
        patch.object(
            db_schema, "get_bundled_reference_database_path", lambda: root / "no-bundled.db"
        )
    )
    context.enter_fixture(patch.object(db_schema, "get_images_dir", lambda: root / "images"))
    # Login state and cloud credentials are the user's: never read them here.
    context.enter_fixture(patch.object(main_window, "app_data_dir", lambda: root / "app-data"))
    context.enter_fixture(
        patch.object(SporelyCloudClient, "from_stored_credentials", classmethod(lambda cls: None))
    )
    # Every publishing target reports "not logged in" without reading any
    # cookie file, token file or keyring entry.
    def _no_login_state(dialog) -> None:
        dialog._target_status = {uploader.key: False for uploader in dialog._uploaders}

    context.enter_fixture(
        patch.object(
            main_window.ArtsobservasjonerSettingsDialog, "_refresh_target_status", _no_login_state
        )
    )
    db_schema.init_database()
    context.state["quick-fixes.isolated"] = True


def _preferences_publishing(context: ReviewContext) -> QWidget:
    _isolate(context)
    from ui.main_window import SettingsHubDialog

    return SettingsHubDialog(context.host, start_page=SettingsHubDialog.PAGE_PUBLISHING)


class _MainWindowStub:
    active_observation_id = 0
    raw_capture_service = None

    def refresh_observation_images(self, *args, **kwargs):
        pass

    def update_measurements_table(self):
        pass


def _camera_import(context: ReviewContext) -> QWidget:
    _isolate(context)
    from ui.ingestion_hub_tab import IngestionHubTab

    return IngestionHubTab(_MainWindowStub(), context.host)


def _live_lab(context: ReviewContext) -> QWidget:
    _isolate(context)
    from ui.live_lab_tab import LiveLabTab

    main = QWidget(context.host)
    main.raw_capture_service = None
    return LiveLabTab(main_window=main, parent=context.host)


def _image_import_footer(context: ReviewContext) -> QWidget:
    _isolate(context)
    from ui.image_import_dialog import ImageImportDialog

    # Edit mode (opened from an existing observation), the mode that showed
    # two "Close" buttons when nothing was dirty.
    return ImageImportDialog(context.host, continue_to_observation_details=False)


def _library_long_labels(context: ReviewContext) -> QWidget:
    _isolate(context)
    from database.reference_library import ReferenceWork, ReferenceWorkRepository
    from ui.reference_library_manager_dialog import ReferenceLibraryManagerDialog

    if not context.state.get("quick-fixes.works-seeded"):
        for label, year in (
            ("Danmarks Basidiesvampe, bind 2: Agaricales og Boletales", 1990),
            ("Funga Nordica: Agaricoid, boletoid, clavarioid, cyphelloid and gastroid genera", 2008),
            ("Wang et al., 2018", 2018),
            ("Parmasto, 1987", 1987),
            ("Mushroomexpert.com", None),
        ):
            ReferenceWorkRepository.create(
                ReferenceWork(
                    id="",
                    type="book",
                    title=label,
                    short_label=label,
                    authors_json=json.dumps([{"family": "Fixture"}]),
                    year=year,
                )
            )
        context.state["quick-fixes.works-seeded"] = True
    dialog = ReferenceLibraryManagerDialog(context.host, active_observation_id=None)
    # Select the work without a year so the (translated) missing-metadata
    # hint shows in the details pane.
    dialog.works_table.selectRow(dialog.works_table.rowCount() - 1)
    return dialog


def register_quick_fix_scenarios(registry: ScenarioRegistry) -> None:
    scenarios = (
        ReviewScenario(
            id="quick-fixes.preferences-publishing",
            group="quick-fixes",
            title="Preferences — Online publishing (nb)",
            description="The publishing page at the hub's default size: the content scrolls instead of the ‘Ask before making … public’ checkbox overlapping the watermark row.",
            build=_preferences_publishing,
            viewport=(960, 680),
            locale="nb_NO",
        ),
        ReviewScenario(
            id="quick-fixes.preferences-publishing-dark",
            group="quick-fixes",
            title="Preferences — Online publishing (nb, dark)",
            description="The scrolling publishing page in the dark theme.",
            build=_preferences_publishing,
            viewport=(960, 680),
            locale="nb_NO",
            theme="dark",
        ),
        ReviewScenario(
            id="quick-fixes.camera-import",
            group="quick-fixes",
            title="Camera import (nb)",
            description="Camera import with no folder scanned: section titles, buttons and the empty state in Norwegian.",
            build=_camera_import,
            viewport=(1280, 760),
            locale="nb_NO",
        ),
        ReviewScenario(
            id="quick-fixes.camera-import-de",
            group="quick-fixes",
            title="Camera import (de)",
            description="Camera import in German, the long-string locale.",
            build=_camera_import,
            viewport=(1280, 760),
            locale="de_DE",
        ),
        ReviewScenario(
            id="quick-fixes.live-lab",
            group="quick-fixes",
            title="Live Lab (nb)",
            description="Live Lab with no session: the side panel and session controls in Norwegian.",
            build=_live_lab,
            viewport=(1280, 800),
            locale="nb_NO",
        ),
        ReviewScenario(
            id="quick-fixes.image-import-footer",
            group="quick-fixes",
            title="Prepare Images footer (nb)",
            description="Prepare Images in edit mode with nothing to save: one Close button instead of two.",
            build=_image_import_footer,
            viewport=(1280, 800),
            locale="nb_NO",
        ),
        ReviewScenario(
            id="quick-fixes.reference-library-long-labels",
            group="quick-fixes",
            title="Reference library — long publication labels (nb)",
            description="Long short labels elide on one line, so the Year and Missing columns stay aligned with their rows; the missing-metadata hint is translated.",
            build=_library_long_labels,
            viewport=(1280, 760),
            locale="nb_NO",
        ),
    )
    for scenario in scenarios:
        registry.register(scenario)
