"""Shared deterministic lifetime and application facilities for scenarios."""
from __future__ import annotations

import socket
import tempfile
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import patch

from PySide6.QtCore import QTranslator
from PySide6.QtGui import QPalette
from PySide6.QtWidgets import QApplication, QWidget


ROOT = Path(__file__).resolve().parents[2]


def deny_network(*_args, **_kwargs):
    raise RuntimeError("network access is forbidden in the UI review renderer")


class ReviewContext:
    """Resources shared for one renderer invocation, not fixture business logic."""

    def __init__(self, app: QApplication) -> None:
        self.app = app
        self.host = QWidget()
        self.state: dict[str, Any] = {}
        self._stack = ExitStack()
        self._temporary_directory: tempfile.TemporaryDirectory[str] | None = None
        self.temporary_root: Path | None = None
        self._original_palette = QPalette(app.palette())
        self._original_stylesheet = app.styleSheet()
        self._original_style = app.style().objectName()
        try:
            self._original_color_scheme = app.styleHints().colorScheme()
        except Exception:
            self._original_color_scheme = None

    def __enter__(self) -> "ReviewContext":
        self._temporary_directory = self._stack.enter_context(
            tempfile.TemporaryDirectory(prefix="sporely-ui-review-")
        )
        self.temporary_root = Path(self._temporary_directory)
        self._stack.enter_context(patch.object(socket, "create_connection", deny_network))
        self._stack.enter_context(patch.object(socket.socket, "connect", deny_network))
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.host.close()
        self.host.deleteLater()
        self.app.setStyle(self._original_style)
        self.app.setPalette(self._original_palette)
        self.app.setStyleSheet(self._original_stylesheet)
        try:
            setter = getattr(self.app.styleHints(), "setColorScheme", None)
            if callable(setter) and self._original_color_scheme is not None:
                setter(self._original_color_scheme)
        except Exception:
            pass
        self.app.processEvents()
        self._stack.__exit__(exc_type, exc, traceback)

    def enter_fixture(self, manager):
        """Keep a feature-owned fixture/patch alive for the complete review run."""
        return self._stack.enter_context(manager)

    def set_theme(self, theme: str) -> None:
        from ui.styles import apply_palette, get_style

        apply_palette(theme)
        stylesheet = get_style(theme)
        self.app.setStyleSheet(stylesheet)
        self.host.setStyleSheet(stylesheet)
        self.app.processEvents()

    @contextmanager
    def locale(self, locale: str) -> Iterator[None]:
        if locale == "en":
            yield
            return

        translator = QTranslator(self.app)
        translation_path = ROOT / "i18n" / f"Sporely_{locale}.qm"
        if not translator.load(str(translation_path)):
            raise RuntimeError(f"could not load translation: {translation_path}")
        self.app.installTranslator(translator)
        try:
            yield
        finally:
            self.app.removeTranslator(translator)
