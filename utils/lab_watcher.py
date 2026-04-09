import os
import time
from pathlib import Path
from PySide6.QtCore import QThread, Signal
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class NewImageHandler(FileSystemEventHandler):
    def __init__(self, callback):
        super().__init__()
        self.callback = callback
        self.valid_extensions = {'.jpg', '.jpeg', '.png', '.tif', '.tiff', '.heic', '.heif'}

    def _emit_if_supported(self, path: str):
        ext = Path(path).suffix.lower()
        if ext not in self.valid_extensions:
            return
        candidate = Path(path)
        previous_size = -1
        stable_reads = 0
        for _ in range(20):
            try:
                size = candidate.stat().st_size
            except Exception:
                time.sleep(0.25)
                continue
            if size > 0 and size == previous_size:
                stable_reads += 1
                if stable_reads >= 2:
                    self.callback(path)
                    return
            else:
                stable_reads = 0
            previous_size = size
            time.sleep(0.25)

    def on_created(self, event):
        if not event.is_directory:
            self._emit_if_supported(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self._emit_if_supported(event.dest_path)

class LabWatcherWorker(QThread):
    new_image_detected = Signal(str)
    error_occurred = Signal(str)

    def __init__(self, directory_to_watch: str, parent=None):
        super().__init__(parent)
        self.directory_to_watch = directory_to_watch
        self.observer = None
        self._is_running = False
        self._stop_requested = False

    def run(self):
        if self._stop_requested or self.isInterruptionRequested():
            return
        self._is_running = True
        if not os.path.exists(self.directory_to_watch):
            self.error_occurred.emit(f"Directory does not exist: {self.directory_to_watch}")
            return

        event_handler = NewImageHandler(self._handle_new_image)
        self.observer = Observer()
        self.observer.schedule(event_handler, self.directory_to_watch, recursive=False)
        try:
            self.observer.start()
            while self._is_running and not self.isInterruptionRequested():
                time.sleep(0.25)
        except Exception as e:
            self.error_occurred.emit(f"Watcher error: {str(e)}")
        finally:
            if self.observer:
                self.observer.stop()
                self.observer.join()
            self._is_running = False

    def _handle_new_image(self, file_path):
        if self._is_running and not self._stop_requested and not self.isInterruptionRequested():
            self.new_image_detected.emit(file_path)

    def stop(self):
        self._stop_requested = True
        self._is_running = False
        self.requestInterruption()
        try:
            if self.observer:
                self.observer.stop()
                self.observer.join(timeout=3)
        except Exception:
            pass
        self.wait(5000)
