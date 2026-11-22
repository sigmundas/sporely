import logging
import os
import time
from pathlib import Path

from PySide6.QtCore import QThread, Signal
from watchdog.events import FileSystemEventHandler

from utils.raw_detection import SUPPORTED_RAW_SUFFIXES, is_raw_image_path

logger = logging.getLogger(__name__)

WATCHED_IMAGE_SUFFIXES = frozenset(
    {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".heic", ".heif"} | set(SUPPORTED_RAW_SUFFIXES)
)

WATCHER_STABILITY_TIMEOUT_SECONDS = 10.0
WATCHER_RAW_STABILITY_TIMEOUT_SECONDS = 45.0
WATCHER_STABILITY_POLL_SECONDS = 0.25

# Minimum seconds between repeated timeout warnings for the same path.
# The path remains retryable; this only suppresses duplicate log noise.
WATCHER_TIMEOUT_LOG_COOLDOWN_SECONDS = 60.0


class NewImageHandler(FileSystemEventHandler):
    def __init__(
        self,
        callback,
        *,
        stability_timeout_seconds: float = WATCHER_STABILITY_TIMEOUT_SECONDS,
        raw_stability_timeout_seconds: float = WATCHER_RAW_STABILITY_TIMEOUT_SECONDS,
        poll_seconds: float = WATCHER_STABILITY_POLL_SECONDS,
        timeout_log_cooldown_seconds: float = WATCHER_TIMEOUT_LOG_COOLDOWN_SECONDS,
    ):
        super().__init__()
        self.callback = callback
        self.valid_extensions = set(WATCHED_IMAGE_SUFFIXES)
        self.stability_timeout_seconds = max(0.0, float(stability_timeout_seconds))
        self.raw_stability_timeout_seconds = max(0.0, float(raw_stability_timeout_seconds))
        self.poll_seconds = max(0.0, float(poll_seconds))
        self.timeout_log_cooldown_seconds = max(0.0, float(timeout_log_cooldown_seconds))
        # Paths that have been successfully emitted to the app.  Later watcher
        # events for the same path are skipped.
        self._handled_paths: set[str] = set()
        # Tracks the monotonic time of the last timeout warning per path so
        # repeated warnings are suppressed without blocking retry attempts.
        self._last_timeout_log_at: dict[str, float] = {}

    @staticmethod
    def _normalize_path(path: str) -> str:
        try:
            return str(Path(path).expanduser().resolve(strict=False))
        except Exception:
            try:
                return str(Path(path).expanduser())
            except Exception:
                return str(path or "").strip()

    def _timeout_seconds_for_path(self, path: Path) -> float:
        return self.raw_stability_timeout_seconds if is_raw_image_path(path) else self.stability_timeout_seconds

    def _emit_if_supported(self, path: str):
        ext = Path(path).suffix.lower()
        if ext not in self.valid_extensions:
            return

        normalized_path = self._normalize_path(path)
        if not normalized_path:
            return

        # Only skip if the path was *successfully* emitted before.
        if normalized_path in self._handled_paths:
            return

        candidate = Path(path)
        previous_size = -1
        stable_reads = 0
        timeout_seconds = self._timeout_seconds_for_path(candidate)
        deadline = time.monotonic() + timeout_seconds

        while time.monotonic() < deadline:
            try:
                size = candidate.stat().st_size
            except Exception:
                time.sleep(self.poll_seconds)
                continue
            if size > 0 and size == previous_size:
                stable_reads += 1
                if stable_reads >= 2:
                    # File is stable — mark as handled and emit.
                    self._handled_paths.add(normalized_path)
                    self._last_timeout_log_at.pop(normalized_path, None)
                    self.callback(path)
                    return
            else:
                stable_reads = 0
            previous_size = size
            time.sleep(self.poll_seconds)

        # Timeout: do NOT add to _handled_paths so later watcher events or
        # Rescan folder can retry this path.
        now = time.monotonic()
        last_logged = self._last_timeout_log_at.get(normalized_path)
        if last_logged is None or now - last_logged >= self.timeout_log_cooldown_seconds:
            self._last_timeout_log_at[normalized_path] = now
            logger.warning(
                "Timed out waiting for %s to stabilize after %.1f seconds",
                normalized_path,
                timeout_seconds,
            )

    def on_created(self, event):
        if not event.is_directory:
            self._emit_if_supported(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self._emit_if_supported(event.dest_path)


# How often the worker lists the watched folder for new captures.
WATCHER_FOLDER_POLL_SECONDS = 0.25
# A new file is reported once its size, above zero, is the same at this many
# listings in a row (about 0.5 s at the interval above).
WATCHER_STABLE_LISTINGS = 3


def _list_file_names(directory: str) -> set[str] | None:
    """The names of the files in ``directory``; None when it cannot be read."""
    try:
        with os.scandir(directory) as entries:
            return {entry.name for entry in entries if entry.is_file()}
    except OSError:
        return None


class LabWatcherWorker(QThread):
    """Reports image files that appear in a folder (not those there at start).

    It lists the folder every ``WATCHER_FOLDER_POLL_SECONDS`` instead of
    waiting for macOS file events (FSEvents): when ``fseventsd`` is busy those
    arrive a minute or more late, and the captures with them. Listing a
    folder of 5,000 files takes about 3 ms. A new file is reported once its
    size has stopped changing; until then each listing checks it again, so a
    file still being written never holds up the others or a stop.
    """

    new_image_detected = Signal(str)
    error_occurred = Signal(str)

    def __init__(self, directory_to_watch: str, parent=None):
        super().__init__(parent)
        self.setObjectName("Lab watcher")
        self.directory_to_watch = directory_to_watch
        self._is_running = False
        self._stop_requested = False
        self.listing_started = False  # the files there at the start are known

    def _should_run(self) -> bool:
        return self._is_running and not self._stop_requested and not self.isInterruptionRequested()

    def _pause(self) -> bool:
        time.sleep(WATCHER_FOLDER_POLL_SECONDS)
        return self._should_run()

    def run(self):
        if self._stop_requested or self.isInterruptionRequested():
            return
        self._is_running = True
        if not os.path.exists(self.directory_to_watch):
            self.error_occurred.emit(f"Directory does not exist: {self.directory_to_watch}")
            return
        try:
            known = _list_file_names(self.directory_to_watch)
            while known is None:  # unreadable for a moment: never take its files for new ones
                if not self._pause():
                    return
                known = _list_file_names(self.directory_to_watch)
            self.listing_started = True
            # Name -> (size at the last listing, listings in a row at that size).
            settling: dict[str, tuple[int, int]] = {}
            while self._pause():
                current = _list_file_names(self.directory_to_watch)
                if current is None:
                    continue  # unreadable for a moment (a network folder): try again
                for name in sorted(current - known):
                    if Path(name).suffix.lower() not in WATCHED_IMAGE_SUFFIXES:
                        known.add(name)
                        continue
                    try:
                        size = os.stat(os.path.join(self.directory_to_watch, name)).st_size
                    except OSError:
                        continue  # gone or locked: the next listing decides
                    last_size, count = settling.get(name, (-1, 0))
                    count = count + 1 if size > 0 and size == last_size else (1 if size > 0 else 0)
                    if count >= WATCHER_STABLE_LISTINGS:
                        settling.pop(name, None)
                        known.add(name)
                        self._handle_new_image(os.path.join(self.directory_to_watch, name))
                    else:
                        settling[name] = (size, count)
                # Forget deleted names, so a file written again under one is new.
                known &= current
                for name in list(settling):
                    if name not in current:
                        del settling[name]
        except Exception as e:
            self.error_occurred.emit(f"Watcher error: {str(e)}")
        finally:
            self._is_running = False

    def _handle_new_image(self, file_path):
        if self._is_running and not self._stop_requested and not self.isInterruptionRequested():
            self.new_image_detected.emit(file_path)

    def stop(self):
        self._stop_requested = True
        self._is_running = False
        self.requestInterruption()
        self.wait(5000)
