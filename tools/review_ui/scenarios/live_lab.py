"""Live Lab and Camera import review scenarios (redesign plan stage P1).

Today's screens, in Norwegian Bokmål and both themes, as the baseline for the
redesign pilot's before/after comparisons. The real ``LiveLabTab`` and
``IngestionHubTab`` run against the shared isolated review profile; the camera
folder watcher and rawpy are the harness stand-ins (see
``tools/review_ui/live_lab_harness.py``). No hardware, credentials or network.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from PIL import Image
from PySide6.QtWidgets import QVBoxLayout, QWidget

from ..context import ReviewContext
from ..live_lab_harness import (
    FolderDropWatcher,
    LiveLabProfile,
    create_observation,
    drain_live_lab,
    pump_until,
    review_profile,
    synthetic_frame,
    use_database,
)
from ..registry import ReviewScenario, ScenarioRegistry


LAB_STATE = {
    "objective_combo": "100X",
    "contrast_combo": "DIC",
    "mount_combo": "KOH",
    "stain_combo": "Melzer",
    "sample_combo": "Dried",
    "sample_source_combo": "Spore_print",
}


def _fresh_profile(context: ReviewContext, key: str) -> LiveLabProfile:
    """The review profile switched to an empty database for one scenario."""
    profile = review_profile(context)
    count = int(context.state.get("live-lab.builds", 0)) + 1
    context.state["live-lab.builds"] = count
    use_database(profile, f"live-lab-{count}-{key}")
    return profile


def _reset_live_lab_settings(*, session_mode: str, raw_mode: str, watch_dir: str) -> None:
    from database.models import SettingsDB
    from ui.live_lab_tab import LiveLabTab

    SettingsDB.set_setting(LiveLabTab.SETTING_SESSION_MODE, session_mode)
    SettingsDB.set_setting(LiveLabTab.SETTING_RAW_CAPTURE_MODE, raw_mode)
    SettingsDB.set_setting(LiveLabTab.SETTING_WATCH_DIR, watch_dir)
    for key in (LiveLabTab.SETTING_MAIN_SPLITTER, LiveLabTab.SETTING_CONTENT_SPLITTER):
        SettingsDB.set_setting(key, "")


def _live_lab_tab(
    context: ReviewContext,
    *,
    session_mode: str = "live",
    raw_mode: str = "auto_save",
    lab_state: dict[str, str] | None = None,
):
    from ui.live_lab_tab import LiveLabTab

    profile = _fresh_profile(context, "live")
    create_observation(profile, "live-lab")
    _reset_live_lab_settings(
        session_mode=session_mode,
        raw_mode=raw_mode,
        watch_dir=str(profile.watch_dir) if session_mode == "live" else "",
    )
    main_window = QWidget(context.host)
    main_window.raw_capture_service = None
    tab = LiveLabTab(main_window=main_window)
    tab._raw_companion_hold_ms = 20
    for attribute, value in (lab_state or LAB_STATE).items():
        selector = getattr(tab, attribute)
        selector.setCurrentIndex(selector.findData(value))
    tab.set_target_observation(profile.observation_ids["live-lab"])
    return tab, profile


class _DrainingHost(QWidget):
    """Hosts a Live Lab tab; when the renderer closes it, the session stops and
    any RAW save finishes before the tab can be deleted."""

    def __init__(self, tab, parent: QWidget) -> None:
        super().__init__(parent)
        self.tab = tab
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(tab)

    def closeEvent(self, event) -> None:  # noqa: N802 - Qt override
        drain_live_lab(self.tab, pump_until)
        super().closeEvent(event)


def _start(tab) -> None:
    tab.start_session()
    pump_until(tab.is_session_running)


def _drop_jpeg(tab, profile: LiveLabProfile, name: str, *, seed: int) -> None:
    before = len(tab._session_image_ids)
    FolderDropWatcher.created[-1].drop(profile.jpeg(name, seed=seed))
    pump_until(lambda: len(tab._session_image_ids) == before + 1)


def _idle(context: ReviewContext) -> QWidget:
    tab, _profile_ = _live_lab_tab(context)
    return _DrainingHost(tab, context.host)


def _session_live(context: ReviewContext) -> QWidget:
    tab, profile = _live_lab_tab(context)
    _start(tab)
    _drop_jpeg(tab, profile, "P1010101.JPG", seed=3)
    _drop_jpeg(tab, profile, "P1010102.JPG", seed=5)
    _drop_jpeg(tab, profile, "P1010103.JPG", seed=9)
    return _DrainingHost(tab, context.host)


def _session_offline(context: ReviewContext) -> QWidget:
    tab, _profile_ = _live_lab_tab(context, session_mode="offline")
    _start(tab)
    tab.stain_combo.setCurrentIndex(tab.stain_combo.findData("Congo_Red"))
    tab.session_note_input.setText("Amyloide sporer, tydelig plage")
    return _DrainingHost(tab, context.host)


def _raw_pending(context: ReviewContext) -> QWidget:
    tab, profile = _live_lab_tab(context, raw_mode="review", lab_state={**LAB_STATE, "contrast_combo": "Phase"})
    _start(tab)
    FolderDropWatcher.created[-1].drop(profile.raw("P1010201.ORF"))
    pump_until(lambda: len(tab._pending_raw_captures) == 1 and not tab.pending_raw_frame.isHidden())
    return _DrainingHost(tab, context.host)


def _uncalibrated(context: ReviewContext) -> QWidget:
    tab, profile = _live_lab_tab(context, lab_state={**LAB_STATE, "objective_combo": "63X"})
    _start(tab)
    _drop_jpeg(tab, profile, "P1010301.JPG", seed=13)
    return _DrainingHost(tab, context.host)


# ---------------------------------------------------------------------------
# Camera import (Kameraimport)
# ---------------------------------------------------------------------------


class _MainWindowStub:
    active_observation_id = 0
    raw_capture_service = None

    def refresh_observation_images(self, *args, **kwargs):
        pass

    def update_measurements_table(self):
        pass


_SESSION_START = datetime(2026, 10, 12, 14, 0, 0)


def _write_exif_jpeg(path, captured_at: datetime, *, seed: int) -> None:
    image = Image.fromarray(synthetic_frame(seed=seed))
    exif = Image.Exif()
    stamp = captured_at.strftime("%Y:%m:%d %H:%M:%S")
    exif[36867] = stamp
    exif[36868] = stamp
    exif[306] = stamp
    image.save(path, "JPEG", quality=88, exif=exif)


def _seed_offline_session(context: ReviewContext) -> tuple[LiveLabProfile, int]:
    """An offline (log-only) session and an SD-card folder with matching images."""
    from database.models import SessionLogDB

    profile = _fresh_profile(context, "camera-import")
    observation_id = create_observation(
        profile,
        "camera-import",
        genus="Cortinarius",
        species="semisanguineus",
        common_name="Rødskivet slørsopp",
        date="2026-10-12",
        location="Estenstadmarka, Trondheim",
    )

    def at(minutes: int) -> str:
        return (_SESSION_START + timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")

    def state(stain: str, contrast: str) -> dict:
        return {
            "objective_name": "100X",
            "objective_label": "100X/1.25 Plan achro",
            "contrast": contrast,
            "mount_medium": "KOH",
            "stain": stain,
            "sample_type": "Dried",
            "sample_source": "Spore_print",
        }

    session_id = "review-offline-session"
    events = [
        (0, "session_started", None, "offline", {"lab_metadata": state("Not_set", "BF")}),
        (0, "dropdown_change", "objective_name", "100X", {"lab_metadata": state("Not_set", "BF")}),
        (0, "dropdown_change", "contrast", "BF", {"lab_metadata": state("Not_set", "BF")}),
        (0, "dropdown_change", "mount_medium", "KOH", {"lab_metadata": state("Not_set", "BF")}),
        (18, "dropdown_change", "stain", "Melzer", {"lab_metadata": state("Melzer", "BF")}),
        (26, "dropdown_change", "contrast", "DIC", {"lab_metadata": state("Melzer", "DIC")}),
        (40, "session_stopped", None, "offline", {"import_count": 0}),
    ]
    for minutes, event_type, attribute, value, metadata in events:
        SessionLogDB.add_event(
            observation_id,
            session_id,
            event_type,
            session_kind="offline",
            attribute_name=attribute,
            value=value,
            metadata_json=metadata,
            recorded_at=at(minutes),
        )

    card = profile.root / "sd-card" / "DCIM" / "100OLYMP"
    card.mkdir(parents=True, exist_ok=True)
    for index, minutes in enumerate((5, 11, 21, 29, 34)):
        _write_exif_jpeg(card / f"P{1012001 + index}.JPG", _SESSION_START + timedelta(minutes=minutes), seed=20 + index)
    # Taken long after the session: stays unmatched.
    _write_exif_jpeg(card / "P1012099.JPG", _SESSION_START + timedelta(hours=3), seed=99)
    return profile, observation_id


def _camera_import_tab(context: ReviewContext):
    from database.models import SettingsDB
    from ui.ingestion_hub_tab import IngestionHubTab

    SettingsDB.set_setting(IngestionHubTab.SETTING_SCAN_DIR, "")
    SettingsDB.set_setting(IngestionHubTab.SETTING_OFFSET_SECONDS, 0.0)
    return IngestionHubTab(_MainWindowStub(), context.host)


def _camera_import_empty(context: ReviewContext) -> QWidget:
    _seed_offline_session(context)
    return _camera_import_tab(context)


def _camera_import_scanned(context: ReviewContext) -> QWidget:
    profile, _observation_id = _seed_offline_session(context)
    tab = _camera_import_tab(context)
    tab.scan_dir_input.setText(str(profile.root / "sd-card"))
    tab.scan_folder_btn.click()
    pump_until(lambda: bool(tab._batch_images))
    # Open the observation the images matched, as a user would next.
    tab._set_selected_observation(profile.observation_ids["camera-import"])
    return tab


_LIVE_LAB_STATES = (
    ("idle", "idle", "Live Lab with a current observation and lab state chosen, no session running.", _idle),
    (
        "session-live",
        "live session",
        "A live capture session with three imported captures; the last one is shown with its scale bar (100x, calibrated).",
        _session_live,
    ),
    (
        "session-offline",
        "offline session",
        "An offline (log-only) session: no watched folder, a stain change and a note being typed.",
        _session_offline,
    ),
    (
        "raw-pending",
        "RAW capture pending",
        "Review mode: a RAW capture waits in the queue with the RAW panel open and the Save/Copy bar on the viewer.",
        _raw_pending,
    ),
    (
        "uncalibrated",
        "uncalibrated objective",
        "A capture with an objective that has no calibration (63x): no scale bar on the viewer.",
        _uncalibrated,
    ),
)

_CAMERA_IMPORT_STATES = (
    (
        "empty",
        "empty",
        "Camera import before a folder is scanned; one offline session waits in the queue.",
        _camera_import_empty,
    ),
    (
        "scanned",
        "folder scanned",
        "An SD-card folder scanned: five images matched to the offline session's lab state, one unmatched.",
        _camera_import_scanned,
    ),
)


def register_live_lab_scenarios(registry: ScenarioRegistry) -> None:
    for slug, label, description, build in _LIVE_LAB_STATES:
        for theme in ("light", "dark"):
            suffix = "" if theme == "light" else "-dark"
            registry.register(
                ReviewScenario(
                    id=f"live-lab.{slug}{suffix}",
                    group="live-lab",
                    title=f"Live Lab — {label} (nb{', dark' if theme == 'dark' else ''})",
                    description=description,
                    build=build,
                    viewport=(1440, 900),
                    locale="nb_NO",
                    theme=theme,
                )
            )
    registry.register(
        ReviewScenario(
            id="live-lab.idle-small",
            group="live-lab",
            title="Live Lab — idle, small window (nb)",
            description="The idle state at a small window size, where the side panel is most crowded.",
            build=_idle,
            viewport=(1100, 720),
            locale="nb_NO",
        )
    )
    for slug, label, description, build in _CAMERA_IMPORT_STATES:
        for theme in ("light", "dark"):
            suffix = "" if theme == "light" else "-dark"
            registry.register(
                ReviewScenario(
                    id=f"live-lab.camera-import-{slug}{suffix}",
                    group="live-lab",
                    title=f"Camera import — {label} (nb{', dark' if theme == 'dark' else ''})",
                    description=description,
                    build=build,
                    viewport=(1440, 900),
                    locale="nb_NO",
                    theme=theme,
                )
            )
