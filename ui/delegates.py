"""Reusable item delegates for UI widgets."""
from PySide6.QtWidgets import QStyledItemDelegate, QStyle, QApplication, QStyleOptionViewItem, QWidget
from PySide6.QtCore import Qt, QRect, QSize
from PySide6.QtGui import QColor, QPainter, QBrush, QPen


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
                highlight = QColor(28, 53, 90) if dark else QColor(199, 236, 199)  # primary_container
                painter.save()
                painter.fillRect(option.rect, highlight)
                painter.restore()
        super().paint(painter, option, index)


class RedListCircleDelegate(QStyledItemDelegate):
    """Paints a filled circle using the item's foreground color."""

    DIAMETER = 14  # px

    def paint(self, painter: QPainter, option: QStyleOptionViewItem, index):
        painter.save()

        # Draw default background (handles selection highlight etc.)
        super().paint(painter, option, index)

        brush_or_color = index.data(Qt.ForegroundRole)
        if isinstance(brush_or_color, QBrush):
            color = brush_or_color.color()
        else:
            color = brush_or_color
            
        if not isinstance(color, QColor) or not color.isValid():
            painter.restore()
            return

        d = self.DIAMETER
        cx = option.rect.left() + option.rect.width() // 2
        cy = option.rect.top() + option.rect.height() // 2
        circle = QRect(cx - d // 2, cy - d // 2, d, d)

        painter.setRenderHint(QPainter.Antialiasing)
        painter.setBrush(QBrush(color))
        painter.setPen(QPen(color.darker(130), 1))
        painter.drawEllipse(circle)

        painter.restore()

    def sizeHint(self, option, index):
        return super().sizeHint(option, index).expandedTo(QSize(28, 24))


class RedListCircleWidget(QWidget):
    """A simple widget that paints a filled circle for the red list badge."""
    DIAMETER = 14
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._color = QColor()
        self.setFixedSize(24, 24)

    def set_color(self, hex_color: str):
        self._color = QColor(hex_color)
        self.update()

    def paintEvent(self, event):
        if not self._color.isValid():
            return
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        
        d = self.DIAMETER
        cx = self.width() // 2
        cy = self.height() // 2
        circle = QRect(cx - d // 2, cy - d // 2, d, d)
        
        painter.setBrush(QBrush(self._color))
        painter.setPen(QPen(self._color.darker(130), 1))
        painter.drawEllipse(circle)
        painter.end()
