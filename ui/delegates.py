"""Reusable item delegates for UI widgets."""
from pathlib import Path

from PySide6.QtWidgets import QStyledItemDelegate, QStyle, QApplication, QStyleOptionViewItem, QWidget
from PySide6.QtCore import Qt, QRect, QSize
from PySide6.QtGui import QColor, QPainter, QBrush, QPen, QPixmap, QPalette


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
    """Paints a filled circle and the text next to it."""

    DIAMETER = 14  # px

    def displayText(self, value, locale):
        return ""

    def paint(self, painter: QPainter, option: QStyleOptionViewItem, index):
        painter.save()

        opt = QStyleOptionViewItem(option)
        self.initStyleOption(opt, index)
        # Draw default background (handles selection highlight etc.)
        super().paint(painter, opt, index)

        brush_or_color = index.data(Qt.ForegroundRole)
        if isinstance(brush_or_color, QBrush):
            color = brush_or_color.color()
        else:
            color = brush_or_color
            
        margin = 6
        d = self.DIAMETER
        text_rect = QRect(opt.rect)

        if isinstance(color, QColor) and color.isValid():
            cx = opt.rect.left() + margin + d // 2
            cy = opt.rect.top() + opt.rect.height() // 2
            circle = QRect(cx - d // 2, cy - d // 2, d, d)

            painter.setRenderHint(QPainter.Antialiasing)
            painter.setBrush(QBrush(color))
            painter.setPen(QPen(color.darker(130), 1))
            painter.drawEllipse(circle)
            
            text_rect.setLeft(circle.right() + margin)

        text = str(index.data(Qt.DisplayRole) or "")
        if text:
            from PySide6.QtGui import QPalette
            if isinstance(color, QColor) and color.isValid():
                painter.setPen(color)
            elif opt.state & QStyle.State_Selected:
                painter.setPen(opt.palette.color(QPalette.HighlightedText))
            else:
                painter.setPen(opt.palette.color(QPalette.Text))
            painter.drawText(text_rect, Qt.AlignVCenter | Qt.AlignLeft, text)

        painter.restore()

    def sizeHint(self, option, index):
        opt = QStyleOptionViewItem(option)
        self.initStyleOption(opt, index)
        hint = super().sizeHint(option, index)
        text = str(index.data(Qt.DisplayRole) or "")
        text_width = opt.fontMetrics.horizontalAdvance(text) if text else 0
        return QSize(text_width + self.DIAMETER + 20, max(hint.height(), 24))


class RedListCircleWidget(QWidget):
    """A simple widget that paints a filled circle and text for the red list badge."""
    DIAMETER = 14
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._color = QColor()
        self._text = ""
        self.setMinimumSize(44, 24)

    def set_color_and_text(self, hex_color: str, text: str):
        self._color = QColor(hex_color)
        self._text = text
        self.update()

    def paintEvent(self, event):
        if not self._color.isValid() and not self._text:
            return
            
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        
        margin = 0
        d = self.DIAMETER
        text_rect = QRect(self.rect())
        
        if self._color.isValid():
            cx = self.rect().left() + margin + d // 2
            cy = self.rect().top() + self.rect().height() // 2
            circle = QRect(cx - d // 2, cy - d // 2, d, d)
            
            painter.setBrush(QBrush(self._color))
            painter.setPen(QPen(self._color.darker(130), 1))
            painter.drawEllipse(circle)
            
            text_rect.setLeft(circle.right() + margin + 6)
            
        if self._text:
            painter.setPen(self.palette().color(self.foregroundRole()))
            font = painter.font()
            font.setBold(True)
            painter.setFont(font)
            painter.drawText(text_rect, Qt.AlignVCenter | Qt.AlignLeft, self._text)
            
        painter.end()


class StatusTagDelegate(QStyledItemDelegate):
    """Paints short observation status values as colored rounded tags."""

    _STYLE_BY_KIND = {
        "draft": ("#e67e22", "#ffffff"),
        "private": ("#e74c3c", "#ffffff"),
        "friends": ("#f1c40f", "#2c3e50"),
        "public": ("#27ae60", "#ffffff"),
    }

    def _style_for_kind(self, kind: str) -> tuple[QColor, QColor]:
        bg_hex, fg_hex = self._STYLE_BY_KIND.get(str(kind or "").strip().lower(), ("#95a5a6", "#ffffff"))
        return QColor(bg_hex), QColor(fg_hex)

    def paint(self, painter: QPainter, option: QStyleOptionViewItem, index):
        opt = QStyleOptionViewItem(option)
        self.initStyleOption(opt, index)

        text = str(index.data(Qt.DisplayRole) or "").strip()
        opt.text = ""
        super().paint(painter, opt, index)
        if not text:
            return

        kind = str(index.data(Qt.UserRole + 2) or "").strip().lower()
        bg_color, fg_color = self._style_for_kind(kind)
        rect = QRect(opt.rect).adjusted(7, 4, -7, -4)
        if rect.width() <= 4 or rect.height() <= 4:
            return

        metrics = opt.fontMetrics
        horizontal_padding = 24
        text_inset = 10
        vertical_padding = 4
        text_width = metrics.horizontalAdvance(text)
        text_height = metrics.height()
        chip_height = min(max(text_height + vertical_padding, 18), rect.height())
        chip_width = min(rect.width(), text_width + horizontal_padding)
        if chip_width <= 0 or chip_height <= 0:
            return
        if chip_width < text_width + horizontal_padding:
            elide_width = max(0, chip_width - (text_inset * 2))
            text = metrics.elidedText(text, Qt.ElideRight, elide_width) if elide_width > 0 else ""

        chip_rect = QRect(0, 0, chip_width, chip_height)
        chip_rect.moveCenter(rect.center())
        chip_rect = chip_rect.intersected(rect)
        if chip_rect.width() <= 0 or chip_rect.height() <= 0:
            return

        painter.save()
        painter.setRenderHint(QPainter.Antialiasing, True)
        painter.setPen(QPen(bg_color.darker(135), 1))
        painter.setBrush(QBrush(bg_color))
        painter.drawRoundedRect(chip_rect, chip_rect.height() / 2, chip_rect.height() / 2)

        font = opt.font
        font.setBold(True)
        painter.setFont(font)
        painter.setPen(fg_color)
        painter.drawText(chip_rect.adjusted(text_inset, 0, -text_inset, 0), Qt.AlignCenter, text)
        painter.restore()

    def sizeHint(self, option: QStyleOptionViewItem, index):
        opt = QStyleOptionViewItem(option)
        self.initStyleOption(opt, index)
        base = super().sizeHint(option, index)
        text = str(index.data(Qt.DisplayRole) or "").strip()
        if not text:
            return QSize(base.width(), max(base.height(), 24))
        width = opt.fontMetrics.horizontalAdvance(text) + 36
        return QSize(max(base.width(), width), max(base.height(), 24))


class AISuggestionItemDelegate(QStyledItemDelegate):
    """Paints an AI-suggestion combo row: text, then a source badge (Ao / iN).

    The badge is drawn from an SVG asset with a light/dark variant. The item's
    source key is stored in ``Qt.UserRole + 5`` as one of ``"arts"`` or
    ``"inat"``.
    """

    SOURCE_ROLE = Qt.UserRole + 5

    _ICON_DIR = Path(__file__).parent.parent / "assets" / "icons"
    _ICON_FILES = {
        ("arts", False): "ai_source_arts_light.svg",
        ("arts", True): "ai_source_arts_dark.svg",
        ("inat", False): "ai_source_inat_light.svg",
        ("inat", True): "ai_source_inat_dark.svg",
    }

    BADGE_W = 30
    BADGE_H = 15
    BADGE_MARGIN = 8

    def __init__(self, parent=None):
        super().__init__(parent)
        self._pixmap_cache: dict[tuple[str, bool, int], QPixmap] = {}

    def initStyleOption(self, option, index):
        super().initStyleOption(option, index)
        option.text = ""

    def _is_dark(self, option: QStyleOptionViewItem) -> bool:
        pal = option.palette if option is not None else None
        if pal is not None:
            return pal.color(QPalette.Window).lightness() < 128
        app = QApplication.instance()
        return app.palette().color(QPalette.Window).lightness() < 128 if app else False

    def _badge_pixmap(self, source: str, dark: bool, height_px: int) -> QPixmap | None:
        source = str(source or "").strip().lower()
        if source not in {"arts", "inat"}:
            return None
        key = (source, dark, int(height_px))
        cached = self._pixmap_cache.get(key)
        if cached is not None:
            return cached
        filename = self._ICON_FILES.get((source, dark))
        if not filename:
            return None
        svg_path = self._ICON_DIR / filename
        if not svg_path.exists():
            return None
        try:
            from PySide6.QtSvg import QSvgRenderer
        except Exception:
            return None
        try:
            renderer = QSvgRenderer(str(svg_path))
        except Exception:
            return None
        if not renderer.isValid():
            return None
        aspect = self.BADGE_W / self.BADGE_H
        h = max(12, int(height_px))
        w = max(12, int(round(h * aspect)))
        pixmap = QPixmap(w, h)
        pixmap.fill(Qt.transparent)
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.Antialiasing, True)
        renderer.render(painter)
        painter.end()
        self._pixmap_cache[key] = pixmap
        return pixmap

    def paint(self, painter: QPainter, option: QStyleOptionViewItem, index):
        opt = QStyleOptionViewItem(option)
        self.initStyleOption(opt, index)

        source = str(index.data(self.SOURCE_ROLE) or "").strip().lower()
        text = str(index.data(Qt.DisplayRole) or "")
        super().paint(painter, opt, index)
        if not text:
            return

        badge_h = int(opt.rect.height() * 0.62)
        pixmap = self._badge_pixmap(source, self._is_dark(opt), badge_h)
        badge_w = pixmap.width() if pixmap is not None else 0

        text_rect = QRect(opt.rect).adjusted(6, 0, -(self.BADGE_MARGIN + badge_w + 6), 0)
        if opt.state & QStyle.State_Selected:
            pen_color = opt.palette.color(QPalette.HighlightedText)
        else:
            pen_color = opt.palette.color(QPalette.Text)

        painter.save()
        painter.setPen(pen_color)
        elided = opt.fontMetrics.elidedText(text, Qt.ElideRight, max(0, text_rect.width()))
        painter.drawText(text_rect, Qt.AlignVCenter | Qt.AlignLeft, elided)
        painter.restore()

        if pixmap is not None:
            bx = opt.rect.right() - self.BADGE_MARGIN - pixmap.width()
            by = opt.rect.top() + (opt.rect.height() - pixmap.height()) // 2
            painter.drawPixmap(bx, by, pixmap)

    def sizeHint(self, option: QStyleOptionViewItem, index):
        opt = QStyleOptionViewItem(option)
        self.initStyleOption(opt, index)
        base = super().sizeHint(option, index)
        text = str(index.data(Qt.DisplayRole) or "").strip()
        text_w = opt.fontMetrics.horizontalAdvance(text) if text else 0
        extra = self.BADGE_W + self.BADGE_MARGIN + 18
        return QSize(max(base.width(), text_w + extra), max(base.height(), 22))
