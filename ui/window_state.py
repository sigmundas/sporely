"""Save and restore window geometry across sessions using QSettings.

Usage — mix into any QWidget subclass:

    class MyDialog(GeometryMixin, QDialog):
        _geometry_key = "MyDialog"

        def __init__(self, parent=None):
            super().__init__(parent)
            ...build UI...
            self._restore_geometry()
            self.finished.connect(self._save_geometry)   # QDialog

For QMainWindow, call _save_geometry() inside closeEvent instead.
"""
from __future__ import annotations

from PySide6.QtCore import QSettings
from app_identity import (
    LEGACY_SETTINGS_APP,
    LEGACY_SETTINGS_ORG,
    SETTINGS_APP,
    SETTINGS_ORG,
)

_ORG = SETTINGS_ORG
_APP = SETTINGS_APP


class GeometryMixin:
    """Save and restore window geometry via QSettings.

    The subclass must define a class-level ``_geometry_key`` string that
    uniquely identifies this window type (used as the QSettings key).
    """

    _geometry_key: str = ""

    def _restore_geometry(self) -> None:
        if not self._geometry_key:
            return
        settings = QSettings(_ORG, _APP)
        geom = settings.value(f"geometry/{self._geometry_key}")
        if not geom:
            legacy = QSettings(LEGACY_SETTINGS_ORG, LEGACY_SETTINGS_APP)
            geom = legacy.value(f"geometry/{self._geometry_key}")
        if geom:
            self.restoreGeometry(geom)  # type: ignore[attr-defined]

    def _save_geometry(self) -> None:
        if not self._geometry_key:
            return
        settings = QSettings(_ORG, _APP)
        settings.setValue(
            f"geometry/{self._geometry_key}",
            self.saveGeometry(),  # type: ignore[attr-defined]
        )
