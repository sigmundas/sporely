"""Reusable item delegates for UI widgets."""
from PySide6.QtWidgets import QStyledItemDelegate, QStyle, QApplication
from PySide6.QtCore import Qt
from PySide6.QtGui import QColor


class SpeciesItemDelegate(QStyledItemDelegate):
    """Show a light background for species with available data."""

    def __init__(self, availability_cache, parent=None, exclude_observation_id=None, genus_provider=None):
        super().__init__(parent)
        self.availability_cache = availability_cache
        self.exclude_observation_id = exclude_observation_id
        self.genus_provider = genus_provider

    def paint(self, painter, option, index):
        if not (option.state & QStyle.State_Selected):
            has_data = index.data(Qt.UserRole + 3)
            genus = index.data(Qt.UserRole + 1)
            species = index.data(Qt.UserRole + 2)
            if not genus or not species:
                text = index.data(Qt.DisplayRole) or ""
                parts = text.split()
                if len(parts) >= 2:
                    if callable(self.genus_provider) and parts[1] in ("🔹", "📏"):
                        genus = self.genus_provider()
                        species = parts[0]
                    else:
                        genus = parts[0]
                        species = parts[1]
                elif len(parts) == 1:
                    species = parts[0]
                    if callable(self.genus_provider):
                        genus = self.genus_provider()
            if has_data is None:
                has_data = False
                exclude_id = self.exclude_observation_id() if callable(self.exclude_observation_id) else self.exclude_observation_id
                if genus and species and self.availability_cache:
                    info = self.availability_cache.get_detailed_info(genus, species, exclude_observation_id=exclude_id)
                    has_data = bool(
                        info.get("has_personal_points")
                        or info.get("has_shared_points")
                        or info.get("has_published_points")
                        or info.get("has_reference_minmax")
                    )
            if has_data:
                app = QApplication.instance()
                dark = app.palette().window().color().lightness() < 128 if app else False
                highlight = QColor(28, 53, 90) if dark else QColor(219, 234, 254)
                painter.save()
                painter.fillRect(option.rect, highlight)
                painter.restore()
        super().paint(painter, option, index)
