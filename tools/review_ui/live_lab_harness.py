"""Deterministic Live Lab harness shared by behaviour tests and review scenarios.

It gives the real ``LiveLabTab`` an isolated profile and replaces only the two
hardware-facing boundaries:

* the camera folder watcher (``LabWatcherWorker``) becomes
  :class:`FolderDropWatcher`, which emits ``new_image_detected`` when a test
  drops a file into the watched folder instead of waiting for filesystem
  events;
* ``rawpy`` becomes :class:`FakeRawpy`, which "decodes" any ``.ORF`` file to a
  synthetic microscope frame and records the postprocess arguments, so RAW
  review and save run without a real RAW file.

Everything else (database, settings, objectives, thumbnails, preview and
import folders) lives under one temporary root. Nothing touches the user's
profile, credentials or network.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

import numpy as np
from PIL import Image
from PySide6.QtCore import QObject, QTimer, Signal

import database.models as _models
import database.schema as _schema


# The real lookups, captured before any review scenario or test patches them.
# Some renderer groups patch these for the whole run; ``use_database`` puts
# the real ones back for the scenarios that use this profile.
_REAL_BINDINGS = (
    (_schema, "get_database_path", _schema.get_database_path),
    (_schema, "get_reference_database_path", _schema.get_reference_database_path),
    (_schema, "get_images_dir", _schema.get_images_dir),
    (_models.SettingsDB, "get_setting", _models.SettingsDB.__dict__["get_setting"]),
    (_models.SettingsDB, "set_setting", _models.SettingsDB.__dict__["set_setting"]),
)


# Objectives used by the harness: one calibrated, one without a scale.
HARNESS_OBJECTIVES: dict[str, dict[str, Any]] = {
    "40X": {
        "optics_type": "microscope",
        "magnification": 40.0,
        "na": 0.65,
        "objective_name": "Plan achro",
        "name": "40X/0.65 Plan achro",
        "microns_per_pixel": 0.0788,
    },
    "100X": {
        "optics_type": "microscope",
        "magnification": 100.0,
        "na": 1.25,
        "objective_name": "Plan achro",
        "name": "100X/1.25 Plan achro",
        "microns_per_pixel": 0.0315,
    },
    "63X": {
        "optics_type": "microscope",
        "magnification": 63.0,
        "na": 0.85,
        "objective_name": "Plan achro",
        "name": "63X/0.85 Plan achro (not calibrated)",
    },
}


def synthetic_frame(width: int = 480, height: int = 320, *, bits: int = 8, seed: int = 7) -> np.ndarray:
    """A deterministic bright-field-like frame: light background, dark ellipses."""
    rng = np.random.default_rng(seed)
    yy, xx = np.mgrid[0:height, 0:width].astype(np.float32)
    background = 0.82 + 0.08 * (xx / max(1, width - 1)) - 0.05 * (yy / max(1, height - 1))
    image = np.repeat(background[..., None], 3, axis=2)
    image[..., 0] *= 0.97
    image[..., 2] *= 1.03
    for _ in range(14):
        cx, cy = rng.uniform(30, width - 30), rng.uniform(30, height - 30)
        rx, ry = rng.uniform(9, 16), rng.uniform(6, 10)
        mask = ((xx - cx) / rx) ** 2 + ((yy - cy) / ry) ** 2 <= 1.0
        rim = (((xx - cx) / (rx + 2)) ** 2 + ((yy - cy) / (ry + 2)) ** 2 <= 1.0) & ~mask
        image[mask] = image[mask] * 0.55 + np.array([0.18, 0.15, 0.10], dtype=np.float32)
        image[rim] *= 0.35
    image = np.clip(image, 0.0, 1.0)
    scale = 65535.0 if bits == 16 else 255.0
    dtype = np.uint16 if bits == 16 else np.uint8
    return (image * scale).astype(dtype)


class _FakeRawHandle:
    def __init__(self, module: "FakeRawpy", path: str) -> None:
        self._module = module
        self._path = path
        self.other = None

    def __enter__(self) -> "_FakeRawHandle":
        return self

    def __exit__(self, *_exc) -> None:
        return None

    def postprocess(self, **kwargs: Any) -> np.ndarray:
        self._module.postprocess_calls.append((self._path, dict(kwargs)))
        bits = int(kwargs.get("output_bps", 8) or 8)
        width, height = (240, 160) if kwargs.get("half_size") else (480, 320)
        return synthetic_frame(width, height, bits=16 if bits == 16 else 8)


class FakeRawpy:
    """Stands in for the ``rawpy`` module: every RAW decodes to the same frame."""

    def __init__(self) -> None:
        self.postprocess_calls: list[tuple[str, dict[str, Any]]] = []

    def imread(self, path: str) -> _FakeRawHandle:
        if not Path(path).exists():
            raise FileNotFoundError(path)
        return _FakeRawHandle(self, str(path))


class FolderDropWatcher(QObject):
    """Replaces ``LabWatcherWorker``: emits when a test drops a file in its folder."""

    new_image_detected = Signal(str)
    error_occurred = Signal(str)
    finished = Signal()

    created: list["FolderDropWatcher"] = []

    def __init__(self, directory_to_watch: str, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self.directory_to_watch = str(directory_to_watch)
        self._running = False
        FolderDropWatcher.created.append(self)

    def start(self) -> None:
        self._running = True

    def isRunning(self) -> bool:  # noqa: N802 - mirrors QThread
        return self._running

    def stop(self) -> None:
        if self._running:
            self._running = False
            QTimer.singleShot(0, self.finished.emit)

    def drop(self, source: Path, name: str | None = None) -> Path:
        """Copy ``source`` into the watched folder and report it like the camera would."""
        destination = Path(self.directory_to_watch) / (name or Path(source).name)
        shutil.copyfile(source, destination)
        if self._running:
            self.new_image_detected.emit(str(destination))
        return destination

    def deliver_late(self, source: Path, name: str | None = None) -> Path:
        """Copy and report a file even though the watcher has stopped.

        Models a filesystem notification that was already queued when the
        session stopped, so tests reach the tab's own guard.
        """
        destination = Path(self.directory_to_watch) / (name or Path(source).name)
        shutil.copyfile(source, destination)
        self.new_image_detected.emit(str(destination))
        return destination


@dataclass
class LiveLabProfile:
    root: Path
    watch_dir: Path
    fixtures_dir: Path
    rawpy: FakeRawpy
    observation_ids: dict[str, int] = field(default_factory=dict)

    def jpeg(self, name: str = "capture.jpg", *, seed: int = 7) -> Path:
        path = self.fixtures_dir / name
        if not path.exists():
            Image.fromarray(synthetic_frame(seed=seed)).save(path, "JPEG", quality=90)
        return path

    def raw(self, name: str = "P1010001.ORF") -> Path:
        path = self.fixtures_dir / name
        if not path.exists():
            path.write_bytes(b"FAKE-ORF" + bytes(4096))
        return path


def isolation_patches(root: Path, rawpy_module: FakeRawpy) -> Iterator[tuple[Any, str, Any]]:
    """(owner, attribute, value) patches that confine Live Lab to ``root``."""
    import database.schema as db_schema
    import ui.live_lab_tab as live_lab_tab
    import utils.raw_render as raw_render
    import utils.rawpy_import as rawpy_import
    import utils.thumbnail_generator as thumbnail_generator

    # app_settings.json decides the database folder and images folder for
    # every module, including ones that imported get_images_dir directly.
    yield db_schema, "SETTINGS_PATH", root / "app" / "app_settings.json"
    # objectives.json and the other app-dir files.
    yield db_schema, "_app_dir", root / "app"
    # Module-level constant computed from the real profile at import time.
    yield thumbnail_generator, "THUMBNAIL_DIR", root / "thumbnails"
    # Pending RAW previews go to tempfile.gettempdir()/sporely/raw_previews.
    yield tempfile, "tempdir", str(root / "tmp")
    yield raw_render, "import_rawpy", lambda: rawpy_module
    yield rawpy_import, "import_rawpy", lambda: rawpy_module
    yield live_lab_tab, "LabWatcherWorker", FolderDropWatcher


def prepare_profile(root: Path, rawpy_module: FakeRawpy) -> LiveLabProfile:
    """Create the folders, settings and objectives. Call after the patches are active."""
    from database import schema as db_schema

    for name in ("app", "tmp", "fixtures"):
        (root / name).mkdir(parents=True, exist_ok=True)
    db_schema.save_objectives(json.loads(json.dumps(HARNESS_OBJECTIVES)))
    FolderDropWatcher.created.clear()
    profile = LiveLabProfile(
        root=root,
        watch_dir=root / "watch",
        fixtures_dir=root / "fixtures",
        rawpy=rawpy_module,
    )
    use_database(profile, "default")
    return profile


def use_database(profile: LiveLabProfile, key: str) -> None:
    """Switch to the database, images, thumbnail and watch folders named ``key``.

    Renderer scenarios each use their own, so a scenario's screenshot does not
    depend on which scenarios ran before it.
    """
    from database import schema as db_schema
    import utils.thumbnail_generator as thumbnail_generator

    # Undo run-wide patches other renderer groups left on these lookups. Each
    # such patch still restores the real value when the review run ends.
    for owner, attribute, value in _REAL_BINDINGS:
        if owner.__dict__.get(attribute) is not value:
            setattr(owner, attribute, value)
    root = profile.root
    suffix = "" if key == "default" else f"-{key}"
    for name in ("db", "images", "thumbnails", "watch"):
        (root / f"{name}{suffix}").mkdir(parents=True, exist_ok=True)
    (root / "app" / "app_settings.json").write_text(
        json.dumps(
            {
                "database_folder": str(root / f"db{suffix}"),
                "images_dir": str(root / f"images{suffix}"),
            }
        ),
        encoding="utf-8",
    )
    # The attribute is already patched for the run; this only retargets it.
    thumbnail_generator.THUMBNAIL_DIR = root / f"thumbnails{suffix}"
    profile.watch_dir = root / f"watch{suffix}"
    profile.observation_ids.clear()
    db_schema.init_database()


def create_observation(
    profile: LiveLabProfile,
    key: str = "default",
    *,
    genus: str = "Inocybe",
    species: str = "geophylla",
    common_name: str = "Silkeslørsopp",
    date: str = "2026-10-12",
    location: str = "Bymarka, Trondheim",
) -> int:
    from database.models import ObservationDB

    observation_id = int(
        ObservationDB.create_observation(
            date=date,
            genus=genus,
            species=species,
            common_name=common_name,
            location=location,
        )
    )
    profile.observation_ids[key] = observation_id
    return observation_id


def review_profile(context) -> LiveLabProfile:
    """The renderer's shared isolated profile, created once per review run.

    ``context`` is a ``tools.review_ui.context.ReviewContext``; the patches
    stay active until the run ends.
    """
    from unittest.mock import patch

    existing = context.state.get("live-lab-harness.profile")
    if existing is not None:
        return existing
    assert context.temporary_root is not None
    root = context.temporary_root / "profile"
    rawpy_module = FakeRawpy()
    for owner, attribute, value in isolation_patches(root, rawpy_module):
        context.enter_fixture(patch.object(owner, attribute, value))
    profile = prepare_profile(root, rawpy_module)
    context.state["live-lab-harness.profile"] = profile
    return profile


def drain_live_lab(tab, wait) -> None:
    """Stop a Live Lab session and wait for its RAW workers before deletion.

    ``stop_session`` saves pending RAW captures on a worker thread parented to
    the tab; deleting the tab while it runs aborts with "QThread: Destroyed
    while thread is still running". ``wait(predicate)`` processes events until
    the predicate holds.
    """

    def _thread_running(thread) -> bool:
        try:
            return bool(thread is not None and thread.isRunning())
        except RuntimeError:  # already deleted
            return False

    def idle() -> bool:
        return (
            not tab.is_session_running()
            and not tab._pending_raw_commit_is_busy()
            and not _thread_running(getattr(tab, "_pending_raw_commit_worker_thread", None))
            and not _thread_running(getattr(tab, "_pending_raw_preview_worker_thread", None))
        )

    if tab.is_session_running():
        tab.stop_session()
    wait(idle)


def pump_until(predicate, *, timeout_s: float = 10.0) -> None:
    """Process Qt events until ``predicate()`` holds (renderer fixtures only)."""
    import time

    from PySide6.QtWidgets import QApplication

    deadline = time.monotonic() + timeout_s
    app = QApplication.instance()
    while time.monotonic() < deadline:
        app.processEvents()
        if predicate():
            return
        time.sleep(0.01)
    raise RuntimeError("timed out waiting for the Live Lab fixture state")
