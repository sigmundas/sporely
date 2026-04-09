"""Reusable image thumbnail gallery widget."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

from PySide6.QtCore import Qt, Signal, QEvent, QSize, QRectF, QTimer, QMimeData, QPoint
from PySide6.QtGui import QPixmap, QPainter, QPen, QColor, QImageReader, QDrag, QShortcut, QKeySequence
from PySide6.QtWidgets import QGraphicsDropShadowEffect
from PySide6.QtWidgets import (
    QApplication,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QScrollArea,
    QToolButton,
    QVBoxLayout,
    QWidget,
    QFrame,
    QGridLayout,
    QSizePolicy,
    QStyle,
)

from database.models import ImageDB, MeasurementDB
from database.schema import load_objectives, objective_display_name, resolve_objective_key
from database.database_tags import DatabaseTerms
from utils.thumbnail_generator import get_thumbnail_path
from .styles import pt

_GALLERY_REORDER_MIME = "application/x-sporely-gallery-item"


class _PublishToggle(QLabel):
    """A simple icon-based toggle that mimics QCheckBox for publish selection."""

    toggled = Signal(bool)

    _icon_dir = Path(__file__).parent.parent / "assets" / "icons"
    _pixmap_unchecked: QPixmap | None = None
    _pixmap_checked: QPixmap | None = None

    @classmethod
    def _ensure_pixmaps(cls) -> None:
        if cls._pixmap_unchecked is None:
            cls._pixmap_unchecked = QPixmap(str(cls._icon_dir / "check_unchecked.svg")).scaled(
                20, 20, Qt.KeepAspectRatio, Qt.SmoothTransformation
            )
            cls._pixmap_checked = QPixmap(str(cls._icon_dir / "check_checked.svg")).scaled(
                20, 20, Qt.KeepAspectRatio, Qt.SmoothTransformation
            )

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._checked = False
        self._ensure_pixmaps()
        self.setFixedSize(20, 20)
        self.setAlignment(Qt.AlignCenter)
        self.setStyleSheet("QLabel { background: transparent; border: none; }")
        self.setCursor(Qt.PointingHandCursor)
        self._refresh()

    def isChecked(self) -> bool:
        return self._checked

    def setChecked(self, state: bool) -> None:
        self._checked = bool(state)
        self._refresh()

    def mousePressEvent(self, event) -> None:
        self._checked = not self._checked
        self._refresh()
        if not self.signalsBlocked():
            self.toggled.emit(self._checked)

    def _refresh(self) -> None:
        self.setPixmap(self._pixmap_checked if self._checked else self._pixmap_unchecked)


class ImageGalleryWidget(QGroupBox):
    """Collapsible thumbnail gallery for observations or explicit image lists."""

    imageClicked = Signal(object, str)
    imageSelected = Signal(object, str)
    imageDoubleClicked = Signal(object, str)
    measureBadgeClicked = Signal(object, str)
    deleteRequested = Signal(object)  # Can be int (db ID) or str (custom ID like "cal_0")
    selectionChanged = Signal(list)
    publishSelectionChanged = Signal(object)
    itemsReordered = Signal(object)

    def __init__(
        self,
        title: str,
        parent: QWidget | None = None,
        show_delete: bool = True,
        show_badges: bool = True,
        thumbnail_size: int = 140,
        min_height: int = 60,
        default_height: int = 140,
        thumbnail_tooltip: str = "",
        show_publish_checkbox: bool = False,
        publish_checkbox_hint: str = "",
    ) -> None:
        super().__init__(title, parent)
        self.setCheckable(False)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.setFocusPolicy(Qt.StrongFocus)
        self._min_height = max(0, int(min_height))
        self._default_height = max(self._min_height, int(default_height))
        self.setMinimumHeight(self._min_height)

        self._show_delete = show_delete
        self._show_badges = show_badges
        self._multi_select = False
        self._thumbnail_tooltip = thumbnail_tooltip
        self._show_publish_checkbox = bool(show_publish_checkbox)
        self._publish_checkbox_hint = str(publish_checkbox_hint or "").strip()
        self._base_thumb_size = max(80, int(thumbnail_size))
        self._min_thumb_size = 80
        self._thumb_size = self._base_thumb_size
        self._fixed_thumbnail_size = False
        self._compact_overlay = False
        self._decode_max_dim = max(384, self._base_thumb_size * 4)
        self._items: list[dict] = []
        self._frames: list[QFrame] = []
        self._selected_id = None
        self._selected_keys: set[str | int] = set()
        self._last_clicked_index: int | None = None
        self._drag_start_pos: QPoint | None = None
        self._drag_start_key = None
        self._reorderable = False
        self._objectives_cache: dict | None = None
        self._publish_checked_by_key: dict[str | int, bool] = {}
        self._suppress_publish_signal = False
        self._pixmap_cache: dict[str, QPixmap] = {}
        self._pixmap_cache_max = 512
        self._content = QWidget(self)
        content_layout = QVBoxLayout(self._content)
        content_layout.setContentsMargins(0, 0, 0, 0)

        self._scroll = QScrollArea()
        self._scroll.setWidgetResizable(True)
        self._scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self._scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self._scroll.setFocusPolicy(Qt.NoFocus)
        self._scroll.viewport().installEventFilter(self)

        self._container = QWidget()
        self._container.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
        self._container.setFocusPolicy(Qt.NoFocus)
        self._grid = QHBoxLayout(self._container)
        self._grid.setContentsMargins(0, 0, 0, 0)
        self._grid.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        self._grid.setSpacing(10)
        self._container.installEventFilter(self)
        self._scroll.setWidget(self._container)
        content_layout.addWidget(self._scroll)

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(self._content)

        self._next_image_shortcut = QShortcut(QKeySequence(Qt.Key_Tab), self)
        self._next_image_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self._next_image_shortcut.activated.connect(lambda: self._select_adjacent_image(1))
        self._previous_image_shortcut = QShortcut(QKeySequence(Qt.Key_Backtab), self)
        self._previous_image_shortcut.setContext(Qt.WidgetWithChildrenShortcut)
        self._previous_image_shortcut.activated.connect(lambda: self._select_adjacent_image(-1))

    def clear(self) -> None:
        self._items = []
        self._selected_id = None
        self._selected_keys = set()
        self._publish_checked_by_key = {}
        self._clear_widgets()

    def _clear_widgets(self) -> None:
        while self._grid.count():
            item = self._grid.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        self._frames = []
        self._sync_container_height()

    def set_fixed_thumbnail_size(self, enabled: bool) -> None:
        self._fixed_thumbnail_size = bool(enabled)
        self._thumb_size = self._target_thumb_size()
        self._update_thumbnail_sizes()

    def set_compact_overlay(self, enabled: bool) -> None:
        self._compact_overlay = bool(enabled)
        if self._items:
            self._render()

    def preferred_single_row_height(self) -> int:
        title_height = max(24, self.fontMetrics().height() + 12)
        frame_height = int(self._scroll.frameWidth()) * 2 if self._scroll is not None else 2
        scrollbar_height = (
            int(self._scroll.horizontalScrollBar().sizeHint().height())
            if self._scroll is not None
            else 16
        )
        spacing = 10
        style = self.style()
        if style is not None:
            try:
                spacing = max(spacing, int(style.pixelMetric(QStyle.PM_LayoutVerticalSpacing, None, self)))
            except Exception:
                spacing = 10
        margins = self.contentsMargins()
        return (
            margins.top()
            + margins.bottom()
            + title_height
            + spacing
            + self._base_thumb_size
            + frame_height
            + scrollbar_height
            + 12
        )

    def maximum_useful_height(self) -> int:
        """Cap single-row galleries before extra blank space becomes excessive."""
        return self.preferred_single_row_height()

    def set_reorderable(self, enabled: bool) -> None:
        self._reorderable = bool(enabled)
        self.setAcceptDrops(self._reorderable)
        if hasattr(self, "_container") and self._container is not None:
            self._container.setAcceptDrops(self._reorderable)
        if hasattr(self, "_scroll") and self._scroll is not None:
            self._scroll.viewport().setAcceptDrops(self._reorderable)
        for frame in getattr(self, "_frames", []):
            frame.setAcceptDrops(self._reorderable)

    def set_images(self, image_paths: Iterable[str]) -> None:
        items = []
        for idx, path in enumerate(image_paths):
            if path:
                items.append(
                    {
                        "id": None,
                        "filepath": str(path),
                        "has_measurements": False,
                        "image_number": idx + 1,
                    }
                )
        self.set_items(items)

    def set_items(self, items: Iterable[dict]) -> None:
        self._items = []
        for idx, item in enumerate(items):
            if not item:
                continue
            filepath = item.get("filepath")
            if not filepath:
                continue
            item_id = item.get("id")
            item_path = str(filepath)
            item_key = item_id if item_id is not None else item_path
            explicit_publish_selected = item.get("publish_selected")
            if explicit_publish_selected is None:
                publish_selected = self._publish_checked_by_key.get(item_key, True)
            else:
                publish_selected = bool(explicit_publish_selected)
            self._publish_checked_by_key[item_key] = publish_selected
            self._items.append(
                {
                    "id": item_id,
                    "filepath": item_path,
                    "preview_path": item.get("preview_path"),
                    "has_measurements": item.get("has_measurements", False),
                    "image_number": item.get("image_number", idx + 1),
                    "badges": item.get("badges", []),
                    "gps_tag_text": item.get("gps_tag_text"),
                    "gps_tag_highlight": item.get("gps_tag_highlight", False),
                    "publish_selected": publish_selected,
                    "frame_border_color": item.get("frame_border_color"),
                }
            )
        self._render()

    def set_observation_id(self, observation_id: int | None) -> None:
        if not observation_id:
            self.clear()
            return
        images = ImageDB.get_images_for_observation(observation_id)
        objectives = self._get_objectives_cache()
        measurement_image_ids = self._spore_measurement_image_ids_for_observation(observation_id)
        objective_label_cache: dict[str, str | None] = {}
        items = []
        for idx, img in enumerate(images):
            img_id = img.get("id")
            image_type = (img.get("image_type") or "field").strip().lower()
            objective_name = img.get("objective_name")
            objective_display = objective_name
            if objective_name:
                objective_name_key = str(objective_name)
                if objective_name_key in objective_label_cache:
                    objective_short = objective_label_cache[objective_name_key]
                else:
                    resolved_key = resolve_objective_key(objective_name, objectives)
                    if resolved_key and resolved_key in objectives:
                        objective_display = objective_display_name(objectives[resolved_key], resolved_key)
                    elif objective_name in objectives:
                        objective_display = objective_display_name(objectives[objective_name], objective_name)
                    objective_short = (
                        ImageGalleryWidget._short_objective_label(objective_display, self.tr)
                        or objective_display
                    )
                    objective_label_cache[objective_name_key] = objective_short
            else:
                objective_short = None
            contrast = img.get("contrast")
            scale_value = img.get("scale_microns_per_pixel")
            custom_scale = bool(scale_value) and (not objective_name or str(objective_name).strip().lower() == "custom")
            needs_scale = (
                image_type == "microscope"
                and not objective_name
                and not scale_value
            )
            badges = self.build_image_type_badges(
                image_type=image_type,
                objective_name=objective_short,
                contrast=contrast,
                scale_microns_per_pixel=scale_value,
                custom_scale=custom_scale,
                needs_scale=needs_scale,
                resize_to_optimal=bool(
                    isinstance(img.get("resample_scale_factor"), (int, float))
                    and img.get("resample_scale_factor") is not None
                    and float(img.get("resample_scale_factor")) < 0.999
                ),
                translate=self.tr,
            )
            items.append(
                {
                    "id": img_id,
                    "filepath": img.get("filepath"),
                    "has_measurements": bool(img_id and int(img_id) in measurement_image_ids),
                    "image_number": idx + 1,
                    "badges": badges,
                }
            )
        self._items = items
        self._render()

    @staticmethod
    def _item_key(item: dict) -> str | int | None:
        if not item:
            return None
        item_id = item.get("id")
        if item_id is not None:
            return item_id
        filepath = item.get("filepath")
        return str(filepath) if filepath else None

    @staticmethod
    def _encode_item_key(key) -> bytes:
        if isinstance(key, int):
            return f"id:{key}".encode("utf-8")
        return f"path:{str(key)}".encode("utf-8")

    @staticmethod
    def _decode_item_key(payload: bytes | bytearray | memoryview | None):
        if payload is None:
            return None
        try:
            text = bytes(payload).decode("utf-8")
        except Exception:
            return None
        if text.startswith("id:"):
            try:
                return int(text[3:])
            except (TypeError, ValueError):
                return None
        if text.startswith("path:"):
            return text[5:]
        return None

    def _ordered_item_keys(self) -> list[str | int]:
        keys: list[str | int] = []
        for item in self._items:
            key = self._item_key(item)
            if key is not None:
                keys.append(key)
        return keys

    def _frame_at_global_pos(self, global_pos: QPoint | None) -> QFrame | None:
        if global_pos is None:
            return None
        for frame in self._frames:
            top_left = frame.mapToGlobal(QPoint(0, 0))
            rect = frame.rect().translated(top_left)
            if rect.contains(global_pos):
                return frame
        return None

    def _reorder_item(self, source_key, target_key, insert_after: bool = False) -> bool:
        ordered_keys = self._ordered_item_keys()
        if source_key not in ordered_keys or target_key not in ordered_keys:
            return False
        source_index = ordered_keys.index(source_key)
        target_index = ordered_keys.index(target_key)
        if source_index == target_index and not insert_after:
            return False

        moved_item = self._items.pop(source_index)
        if source_index < target_index:
            target_index -= 1
        if insert_after:
            target_index += 1
        target_index = max(0, min(target_index, len(self._items)))
        self._items.insert(target_index, moved_item)
        self._render()
        if self._selected_keys:
            self._apply_selection_styles()
        if source_key is not None:
            self._center_on_key(source_key)
        self.itemsReordered.emit(self._ordered_item_keys())
        return True

    def _get_objectives_cache(self) -> dict:
        if isinstance(self._objectives_cache, dict):
            return self._objectives_cache
        try:
            self._objectives_cache = load_objectives()
        except Exception:
            self._objectives_cache = {}
        return self._objectives_cache

    @staticmethod
    def _spore_measurement_image_ids_for_observation(observation_id: int) -> set[int]:
        image_ids: set[int] = set()
        try:
            measurements = MeasurementDB.get_measurements_for_observation(int(observation_id))
        except Exception:
            return image_ids
        for measurement in measurements or []:
            image_id = measurement.get("image_id")
            try:
                parsed = int(image_id)
            except (TypeError, ValueError):
                continue
            image_ids.add(parsed)
        return image_ids

    @staticmethod
    def _short_objective_label(name: str | None, translate=None) -> str | None:
        tr = translate if translate is not None else (lambda text: text)
        if not name:
            return None
        text = str(name).strip()
        if not text:
            return None
        if text.lower() == "custom":
            return tr("Scale bar")
        match = re.search(r"(\d+(?:\.\d+)?)\s*[xX]", text)
        if match:
            return f"{match.group(1)}X"
        match = re.search(r"(\d+(?:\.\d+)?)", text)
        if match:
            return f"{match.group(1)}X"
        return text

    @staticmethod
    def build_image_type_badges(
        image_type: str | None,
        objective_name: str | None = None,
        contrast: str | None = None,
        scale_microns_per_pixel: float | None = None,
        custom_scale: bool = False,
        needs_scale: bool = False,
        resize_to_optimal: bool = False,
        translate=None,
    ) -> list[str]:
        tr = translate if translate is not None else (lambda text: text)
        image_type = (image_type or "field").strip().lower()
        badges: list[str] = []

        if image_type == "microscope":
            detail = None
            if custom_scale:
                detail = tr("Scale bar")
            elif objective_name:
                if str(objective_name).strip().lower() == "custom":
                    detail = tr("Scale bar")
                else:
                    detail = ImageGalleryWidget._short_objective_label(objective_name, tr)
                    if not detail:
                        detail = tr("Micro")
            elif scale_microns_per_pixel:
                detail = tr("Scale bar")
            else:
                detail = tr("Micro")
            if contrast:
                detail = f"{detail} {DatabaseTerms.translate_contrast(contrast)}"
            badges.append(detail)
            if resize_to_optimal:
                badges.append("R")
            if needs_scale:
                badges.append(tr("(!) needs scale"))
        else:
            badges.append(tr("Field"))

        return badges

    def select_image(self, image_id: int | None) -> None:
        self._selected_id = image_id
        self._selected_keys = set()
        if image_id is not None:
            self._selected_keys.add(image_id)
        self._last_clicked_index = self._index_for_key(image_id)
        for frame in self._frames:
            is_selected = getattr(frame, "image_id", None) == image_id and image_id is not None
            frame.setProperty("selected", is_selected)
            frame.setStyleSheet(
                self._frame_style(
                    selected=is_selected,
                    border_color=getattr(frame, "frame_border_color", None),
                )
            )
            self._apply_frame_glow(frame, is_selected)
        if image_id is not None:
            self._center_on_key(image_id)

    def publish_selected_ids(self) -> set[int]:
        selected: set[int] = set()
        for item in self._items:
            item_id = item.get("id")
            if item_id is None:
                continue
            key = item_id
            if bool(self._publish_checked_by_key.get(key, item.get("publish_selected", True))):
                try:
                    selected.add(int(item_id))
                except Exception:
                    continue
        return selected

    def set_publish_selected_ids(self, selected_ids: set[int], emit_signal: bool = False) -> None:
        if not self._show_publish_checkbox:
            return
        normalized_ids = {int(i) for i in (selected_ids or set())}
        self._suppress_publish_signal = True
        try:
            for item in self._items:
                item_id = item.get("id")
                key = item_id if item_id is not None else item.get("filepath")
                is_checked = bool(item_id is not None and int(item_id) in normalized_ids)
                self._publish_checked_by_key[key] = is_checked
                item["publish_selected"] = is_checked
            for frame in self._frames:
                checkbox = getattr(frame, "publish_checkbox", None)
                key = getattr(frame, "image_key", None)
                if checkbox is None or key is None:
                    continue
                checked = bool(self._publish_checked_by_key.get(key, True))
                checkbox.blockSignals(True)
                checkbox.setChecked(checked)
                checkbox.blockSignals(False)
        finally:
            self._suppress_publish_signal = False
        if emit_signal:
            self.publishSelectionChanged.emit(self.publish_selected_ids())

    def publish_checkbox_widgets(self) -> list[_PublishToggle]:
        widgets: list[_PublishToggle] = []
        for frame in self._frames:
            checkbox = getattr(frame, "publish_checkbox", None)
            if isinstance(checkbox, _PublishToggle):
                widgets.append(checkbox)
        return widgets

    def _render(self) -> None:
        self._clear_widgets()
        self._thumb_size = self._target_thumb_size()
        self._sync_container_height()
        for item in self._items:
            frame = self._create_thumbnail_widget(item)
            self._frames.append(frame)
            self._grid.addWidget(frame)
        if self._selected_id is not None:
            self.select_image(self._selected_id)
        elif self._selected_keys:
            self._apply_selection_styles()

    def eventFilter(self, obj, event):
        if self._reorderable and event.type() in (QEvent.DragEnter, QEvent.DragMove, QEvent.Drop):
            source_key = self._decode_item_key(event.mimeData().data(_GALLERY_REORDER_MIME))
            if source_key is not None:
                if event.type() in (QEvent.DragEnter, QEvent.DragMove):
                    event.acceptProposedAction()
                    return True
                target_key = None
                insert_after = False
                target_frame = None
                try:
                    target_frame = self._frame_at_global_pos(event.globalPosition().toPoint())
                except Exception:
                    target_frame = None
                if target_frame is None and isinstance(obj, QFrame) and obj in self._frames:
                    target_frame = obj
                if target_frame is not None:
                    target_key = getattr(target_frame, "image_key", None)
                    try:
                        local_pos = target_frame.mapFromGlobal(event.globalPosition().toPoint())
                        insert_after = float(local_pos.x()) >= (target_frame.width() / 2.0)
                    except Exception:
                        insert_after = False
                elif obj in {self, self._container, self._scroll.viewport()} and self._frames:
                    target_key = getattr(self._frames[-1], "image_key", None)
                    insert_after = True
                if target_key is not None and self._reorder_item(source_key, target_key, insert_after=insert_after):
                    event.acceptProposedAction()
                    return True
        if obj == self._scroll.viewport() and event.type() == QEvent.Resize:
            self._update_thumbnail_sizes()
        if event.type() in (QEvent.Enter, QEvent.Leave) and isinstance(obj, QFrame) and obj in self._frames:
            is_selected = getattr(obj, "image_key", None) in self._selected_keys
            self._apply_frame_glow(obj, is_selected, hovered=event.type() == QEvent.Enter and not is_selected)
        return super().eventFilter(obj, event)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._update_thumbnail_sizes()

    def sizeHint(self) -> QSize:
        return QSize(320, self._default_height)

    def minimumSizeHint(self) -> QSize:
        return QSize(120, self._min_height)

    def _frame_style(self, selected: bool = False, border_color: str | None = None) -> str:
        border = border_color or ("#2980b9" if selected else "#bdc3c7")
        return (
            "QFrame { border: 2px solid %s; border-radius: 5px; background: white; }"
        ) % border

    @staticmethod
    def _apply_frame_glow(frame: QFrame, selected: bool, hovered: bool = False) -> None:
        if selected:
            effect = QGraphicsDropShadowEffect(frame)
            effect.setBlurRadius(30)
            effect.setOffset(0, 0)
            effect.setColor(QColor(52, 152, 219, 230))
            frame.setGraphicsEffect(effect)
        elif hovered:
            from PySide6.QtWidgets import QApplication
            is_dark = QApplication.instance().palette().window().color().lightness() < 128
            hover_color = QColor(255, 255, 255, 220) if is_dark else QColor(80, 80, 80, 160)
            effect = QGraphicsDropShadowEffect(frame)
            effect.setBlurRadius(26)
            effect.setOffset(0, 0)
            effect.setColor(hover_color)
            frame.setGraphicsEffect(effect)
        else:
            frame.setGraphicsEffect(None)

    def _create_thumbnail_widget(self, item: dict) -> QFrame:
        frame = QFrame()
        frame.setStyleSheet(self._frame_style(border_color=item.get("frame_border_color")))
        frame.setFixedSize(self._thumb_size, self._thumb_size)
        frame.setCursor(Qt.PointingHandCursor)
        frame.setAcceptDrops(self._reorderable)
        if self._compact_overlay:
            overlay_btn_size = max(12, min(14, int(round(self._thumb_size * 0.10))))
            overlay_btn_radius = max(6, overlay_btn_size // 2)
            overlay_font_px = max(8, overlay_btn_size - 5)
            overlay_label_font_px = max(9, min(12, int(round(self._thumb_size * 0.095))))
            overlay_label_pad_h = max(2, min(4, int(round(self._thumb_size * 0.018))))
            overlay_label_pad_v = max(1, min(3, int(round(self._thumb_size * 0.012))))
            overlay_margin = 1
            overlay_spacing = 2
        else:
            overlay_btn_size = 16
            overlay_btn_radius = 8
            overlay_font_px = pt(8)
            overlay_label_font_px = pt(8)
            overlay_label_pad_h = 4
            overlay_label_pad_v = 1
            overlay_margin = 2
            overlay_spacing = 4

        layout = QVBoxLayout(frame)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        thumb_label = QLabel()
        thumb_label.setAlignment(Qt.AlignCenter)
        thumb_label.setFixedSize(self._thumb_size, self._thumb_size)

        pixmap = self._load_pixmap(item)
        if pixmap and not pixmap.isNull():
            thumb_label._orig_pixmap = pixmap
            scaled_thumb = self._scaled_thumb(pixmap, self._thumb_size)
            crop_box = item.get("crop_box")
            if crop_box and isinstance(crop_box, (list, tuple)) and len(crop_box) == 4:
                crop_source_size = item.get("crop_source_size")
                scaled_thumb = self._apply_crop_overlay(scaled_thumb, crop_box, crop_source_size)
            thumb_label.setPixmap(scaled_thumb)
        else:
            thumb_label.setText(self.tr("No preview"))
            thumb_label.setStyleSheet("color: #7f8c8d;")

        image_container = QWidget()
        image_layout = QGridLayout(image_container)
        image_layout.setContentsMargins(0, 0, 0, 0)
        image_layout.setSpacing(0)
        image_layout.addWidget(thumb_label, 0, 0, alignment=Qt.AlignCenter)

        image_num = item.get("image_number")
        if image_num is not None:
            number_label = QLabel(str(image_num))
            number_label.setStyleSheet(
                "color: #000000; background-color: rgba(255, 255, 255, 77);"
                f"font-size: {overlay_label_font_px}{'px' if self._compact_overlay else 'pt'};"
                f" padding: {overlay_label_pad_v}px {overlay_label_pad_h}px;"
                " border-radius: 3px; border: none;"
            )
            number_label.setAttribute(Qt.WA_TransparentForMouseEvents, True)
            image_layout.addWidget(number_label, 0, 0, alignment=Qt.AlignTop | Qt.AlignLeft)

        gps_tag_text = item.get("gps_tag_text")
        if gps_tag_text:
            gps_label = QLabel(str(gps_tag_text))
            gps_highlight = bool(item.get("gps_tag_highlight"))
            color = "#ffffff" if gps_highlight else "#000000"
            background = "#c0392b" if gps_highlight else "rgba(255, 255, 255, 77)"
            weight = "bold" if gps_highlight else "normal"
            gps_label.setStyleSheet(
                f"color: {color}; background-color: {background};"
                f"font-size: {overlay_label_font_px}{'px' if self._compact_overlay else 'pt'}; font-weight: {weight};"
                f" padding: {overlay_label_pad_v}px {overlay_label_pad_h}px; border-radius: 3px; border: none;"
            )
            if self._compact_overlay:
                gps_label.setMaximumWidth(max(30, self._thumb_size - overlay_btn_size - 28))
            gps_label.setAttribute(Qt.WA_TransparentForMouseEvents, True)
            image_layout.addWidget(gps_label, 0, 0, alignment=Qt.AlignTop | Qt.AlignHCenter)

        badges = item.get("badges") or []
        if badges:
            badge_container = QWidget()
            badge_layout = QVBoxLayout(badge_container)
            badge_layout.setContentsMargins(2, 2, 2, 2)
            badge_layout.setSpacing(2)

            def _make_badge(text: str, is_resize: bool) -> QLabel:
                badge = QLabel(str(text))
                badge.setStyleSheet(
                    (
                        "color: #ffffff; background-color: rgba(30, 132, 73, 210);"
                        f"font-size: {pt(7)}pt; font-weight: bold; padding: 1px 4px; border-radius: 3px; border: none;"
                    )
                    if is_resize
                    else (
                        "color: #000000; background-color: rgba(255, 255, 255, 180);"
                        f"font-size: {pt(7)}pt; padding: 1px 4px; border-radius: 3px; border: none;"
                    )
                )
                badge.setAttribute(Qt.WA_TransparentForMouseEvents, True)
                return badge

            clean_badges = [str(b).strip() for b in badges if b]
            if clean_badges:
                first_row = QHBoxLayout()
                first_row.setContentsMargins(0, 0, 0, 0)
                first_row.setSpacing(2)
                first_row.addWidget(_make_badge(clean_badges[0], False))
                consumed = 1
                if len(clean_badges) > 1 and clean_badges[1] == "R":
                    first_row.addWidget(_make_badge("R", True))
                    consumed = 2
                first_row.addStretch(1)
                badge_layout.addLayout(first_row)
                for extra_text in clean_badges[consumed:]:
                    badge_layout.addWidget(_make_badge(extra_text, extra_text == "R"))
            image_layout.addWidget(badge_container, 0, 0, alignment=Qt.AlignBottom | Qt.AlignLeft)

        overlay = QWidget()
        overlay_layout = QHBoxLayout(overlay)
        overlay_layout.setContentsMargins(overlay_margin, overlay_margin, overlay_margin, overlay_margin)
        overlay_layout.setSpacing(overlay_spacing)
        overlay_layout.addStretch()

        if self._show_badges and item.get("has_measurements"):
            badge = QToolButton()
            badge.setText("M")
            badge.setFixedSize(overlay_btn_size, overlay_btn_size)
            badge.setStyleSheet(
                "QToolButton { background-color: #27ae60; color: white; border: none;"
                f" border-radius: {overlay_btn_radius}px; font-size: {overlay_font_px}{'px' if self._compact_overlay else 'pt'};"
                " font-weight: bold; padding: 0px; }"
                "QToolButton:hover { background-color: #229954; }"
            )
            badge.setToolTip(self.tr("Open in Measure tab"))
            badge.clicked.connect(
                lambda _checked=False, img_id=item.get("id"), path=item.get("filepath"): self.measureBadgeClicked.emit(img_id, path or "")
            )
            overlay_layout.addWidget(badge)

        delete_key = item.get("id") if item.get("id") is not None else item.get("filepath")
        if self._show_delete and delete_key:
            delete_btn = QToolButton()
            delete_btn.setText("X")
            delete_btn.setFixedSize(overlay_btn_size, overlay_btn_size)
            delete_btn.setStyleSheet(
                "QToolButton { background-color: #e74c3c; color: white; border: none;"
                f" border-radius: {overlay_btn_radius}px; font-size: {overlay_font_px}{'px' if self._compact_overlay else 'pt'}; padding: 0px; }}"
                "QToolButton:hover { background-color: #d6453a; }"
            )
            delete_btn.clicked.connect(lambda _, key=delete_key: self.deleteRequested.emit(key))
            overlay_layout.addWidget(delete_btn)

        image_layout.addWidget(overlay, 0, 0, alignment=Qt.AlignTop | Qt.AlignRight)

        publish_checkbox = None
        if self._show_publish_checkbox:
            publish_checkbox = _PublishToggle()
            key = item.get("id") if item.get("id") is not None else item.get("filepath")
            checked = bool(self._publish_checked_by_key.get(key, item.get("publish_selected", True)))
            self._publish_checked_by_key[key] = checked
            publish_checkbox.setChecked(checked)
            if self._publish_checkbox_hint:
                publish_checkbox.setProperty("_hint_text", self._publish_checkbox_hint)
                publish_checkbox.setToolTip(self._publish_checkbox_hint)
            publish_checkbox.toggled.connect(
                lambda checked, k=key: self._on_publish_checkbox_toggled(k, bool(checked))
            )
            image_layout.addWidget(publish_checkbox, 0, 0, alignment=Qt.AlignBottom | Qt.AlignRight)
        layout.addWidget(image_container)

        frame.image_id = item.get("id")
        frame.image_path = item.get("filepath")
        frame.image_key = item.get("id") if item.get("id") is not None else item.get("filepath")
        frame.frame_border_color = item.get("frame_border_color")
        frame.thumb_label = thumb_label
        frame.publish_checkbox = publish_checkbox
        if self._thumbnail_tooltip:
            frame.setToolTip(self._thumbnail_tooltip)
        frame.mousePressEvent = lambda e, f=frame: self._on_frame_mouse_press(e, f)
        frame.mouseMoveEvent = lambda e, f=frame: self._on_frame_mouse_move(e, f)
        frame.mouseReleaseEvent = lambda e: setattr(self, "_drag_start_pos", None)
        frame.mouseDoubleClickEvent = lambda e, img_id=frame.image_id, path=frame.image_path: self.imageDoubleClicked.emit(img_id, path or "")
        frame.installEventFilter(self)
        return frame

    def set_multi_select(self, enabled: bool) -> None:
        self._multi_select = bool(enabled)
        if not self._multi_select:
            self._selected_keys = set()
            if self._selected_id is not None:
                self._selected_keys.add(self._selected_id)
            self._apply_selection_styles()

    def selected_paths(self) -> list[str]:
        selected = []
        for item in self._items:
            key = item.get("id") if item.get("id") is not None else item.get("filepath")
            if key in self._selected_keys:
                selected.append(item.get("filepath"))
        return selected

    def select_paths(self, paths: list[str]) -> None:
        keys: set[str | int] = set()
        for item in self._items:
            filepath = item.get("filepath")
            if filepath in paths:
                key = item.get("id") if item.get("id") is not None else filepath
                keys.add(key)
        self._selected_keys = keys
        self._selected_id = None
        self._last_clicked_index = None
        first_selected_key = None
        if keys:
            for item in self._items:
                key = item.get("id") if item.get("id") is not None else item.get("filepath")
                if key in keys:
                    self._selected_id = item.get("id")
                    self._last_clicked_index = self._index_for_key(key)
                    first_selected_key = key
                    break
        self._apply_selection_styles()
        if first_selected_key is not None:
            self._center_on_key(first_selected_key)

    def _index_for_key(self, key) -> int | None:
        if key is None:
            return None
        for idx, item in enumerate(self._items):
            item_key = item.get("id") if item.get("id") is not None else item.get("filepath")
            if item_key == key:
                return idx
        return None

    def _apply_selection_styles(self) -> None:
        for frame in self._frames:
            key = getattr(frame, "image_key", None)
            is_selected = key in self._selected_keys if key is not None else False
            frame.setProperty("selected", is_selected)
            frame.setStyleSheet(
                self._frame_style(
                    selected=is_selected,
                    border_color=getattr(frame, "frame_border_color", None),
                )
            )
            self._apply_frame_glow(frame, is_selected)

    def _frame_for_key(self, key) -> QFrame | None:
        if key is None:
            return None
        for frame in self._frames:
            if getattr(frame, "image_key", None) == key:
                return frame
        return None

    def _center_on_key(self, key) -> None:
        frame = self._frame_for_key(key)
        if frame is None:
            return
        QTimer.singleShot(0, lambda f=frame: self._center_on_frame(f))

    def _center_on_frame(self, frame: QFrame | None) -> None:
        if frame is None or not self._scroll:
            return
        scrollbar = self._scroll.horizontalScrollBar()
        if scrollbar is None:
            return
        viewport = self._scroll.viewport()
        if viewport is None:
            return
        view_left = float(scrollbar.value())
        view_right = view_left + float(viewport.width())
        frame_left = float(frame.x())
        frame_right = frame_left + float(frame.width())
        # Keep current scroll when selected thumbnail is fully visible.
        if frame_left >= view_left and frame_right <= view_right:
            return
        frame_center_x = frame.x() + (frame.width() / 2.0)
        target = int(round(frame_center_x - (viewport.width() / 2.0)))
        target = max(scrollbar.minimum(), min(scrollbar.maximum(), target))
        scrollbar.setValue(target)

    def prepare_for_tab_switch(self) -> None:
        """Clear transient focus/effects before a parent tab is hidden."""
        try:
            self.clearFocus()
        except Exception:
            pass
        if self._scroll is not None:
            try:
                self._scroll.clearFocus()
            except Exception:
                pass
        for frame in list(self._frames):
            try:
                frame.setGraphicsEffect(None)
            except Exception:
                pass

    def _on_frame_mouse_press(self, event, frame: QFrame) -> None:
        if event.button() == Qt.LeftButton:
            self.setFocus(Qt.MouseFocusReason)
            try:
                self._drag_start_pos = event.position().toPoint()
            except Exception:
                self._drag_start_pos = QPoint()
            self._drag_start_key = getattr(frame, "image_key", None)
        self._on_click(event, getattr(frame, "image_id", None), getattr(frame, "image_path", ""))

    def _select_adjacent_image(self, step: int) -> None:
        if step == 0 or not self._items:
            return
        current_focus = QApplication.focusWidget()
        if current_focus not in (None, self) and not self.isAncestorOf(current_focus):
            return
        selected_indices = [
            idx for idx, item in enumerate(self._items)
            if self._item_key(item) in self._selected_keys
        ]
        if selected_indices:
            base_index = max(selected_indices) if step > 0 else min(selected_indices)
        elif self._last_clicked_index is not None:
            base_index = self._last_clicked_index
        else:
            base_index = -1 if step > 0 else len(self._items)

        target_index = max(0, min(len(self._items) - 1, base_index + step))
        if target_index == base_index and len(selected_indices) == 1:
            return

        target_item = self._items[target_index]
        target_key = self._item_key(target_item)
        target_id = target_item.get("id")
        target_path = target_item.get("filepath") or ""

        self._selected_id = target_id
        self._selected_keys = {target_key} if target_key is not None else set()
        self._last_clicked_index = target_index
        self._apply_selection_styles()
        if target_key is not None:
            self._center_on_key(target_key)
        if self._multi_select:
            self.selectionChanged.emit(self.selected_paths())
        else:
            self.imageSelected.emit(target_id, target_path)
        self.imageClicked.emit(target_id, target_path)

    def _on_frame_mouse_move(self, event, frame: QFrame) -> None:
        if not self._reorderable:
            return
        if not (event.buttons() & Qt.LeftButton):
            return
        if self._drag_start_pos is None:
            return
        if getattr(frame, "image_key", None) != self._drag_start_key:
            return
        try:
            current_pos = event.position().toPoint()
        except Exception:
            return
        if (current_pos - self._drag_start_pos).manhattanLength() < QApplication.startDragDistance():
            return
        self._drag_start_pos = None
        key = getattr(frame, "image_key", None)
        if key is None:
            return
        mime_data = QMimeData()
        mime_data.setData(_GALLERY_REORDER_MIME, self._encode_item_key(key))
        drag = QDrag(frame)
        drag.setMimeData(mime_data)
        pixmap = getattr(getattr(frame, "thumb_label", None), "pixmap", lambda: None)()
        if isinstance(pixmap, QPixmap) and not pixmap.isNull():
            drag.setPixmap(pixmap)
            drag.setHotSpot(current_pos)
        drag.exec(Qt.MoveAction)

    def _on_click(self, event, img_id, path):
        key = img_id if img_id is not None else path
        index = self._index_for_key(key)
        ctrl_like = bool(event.modifiers() & (Qt.ControlModifier | Qt.MetaModifier))
        if self._multi_select:
            if event.modifiers() & Qt.ShiftModifier and index is not None and self._last_clicked_index is not None:
                start = min(self._last_clicked_index, index)
                end = max(self._last_clicked_index, index)
                range_keys = set()
                for idx in range(start, end + 1):
                    item = self._items[idx]
                    item_key = item.get("id") if item.get("id") is not None else item.get("filepath")
                    range_keys.add(item_key)
                if ctrl_like:
                    self._selected_keys |= range_keys
                else:
                    self._selected_keys = range_keys
            elif ctrl_like:
                if key in self._selected_keys:
                    self._selected_keys.discard(key)
                else:
                    self._selected_keys.add(key)
            else:
                self._selected_keys = {key}
            self._selected_id = img_id
            if index is not None:
                self._last_clicked_index = index
            self._apply_selection_styles()
            self.selectionChanged.emit(self.selected_paths())
        else:
            self._selected_id = img_id
            self._selected_keys = {key} if key is not None else set()
            if index is not None:
                self._last_clicked_index = index
            self._apply_selection_styles()
            self.imageSelected.emit(img_id, path)
        self.imageClicked.emit(img_id, path)

    def _set_publish_state_for_key(self, key, checked: bool) -> None:
        if key is None:
            return
        self._publish_checked_by_key[key] = bool(checked)
        for item in self._items:
            item_key = item.get("id") if item.get("id") is not None else item.get("filepath")
            if item_key == key:
                item["publish_selected"] = bool(checked)
                break

    def _on_publish_checkbox_toggled(self, key, checked: bool) -> None:
        if self._suppress_publish_signal:
            return
        keys_to_update: list[str | int] = [key]
        if key in self._selected_keys and len(self._selected_keys) > 1:
            keys_to_update = [k for k in self._selected_keys]
        self._suppress_publish_signal = True
        try:
            for update_key in keys_to_update:
                self._set_publish_state_for_key(update_key, checked)
                frame = self._frame_for_key(update_key)
                checkbox = getattr(frame, "publish_checkbox", None) if frame is not None else None
                if checkbox is not None and checkbox.isChecked() != bool(checked):
                    checkbox.blockSignals(True)
                    checkbox.setChecked(bool(checked))
                    checkbox.blockSignals(False)
        finally:
            self._suppress_publish_signal = False
        self.publishSelectionChanged.emit(self.publish_selected_ids())

    def _target_thumb_size(self) -> int:
        if self._fixed_thumbnail_size:
            return self._base_thumb_size
        if not self._scroll:
            return self._base_thumb_size
        # Use the scroll area's allocated height rather than the live viewport height.
        # When the horizontal scrollbar is set to AsNeeded, basing the thumbnail size
        # on the viewport can oscillate: larger thumbs trigger the scrollbar, which
        # shrinks the viewport, which shrinks the thumbs enough for the scrollbar to
        # disappear, and so on.
        frame = max(0, int(self._scroll.frameWidth()) * 2)
        scrollbar_h = max(0, int(self._scroll.horizontalScrollBar().sizeHint().height()))
        available_h = max(0, int(self._scroll.height()) - frame - scrollbar_h - 8)
        target = max(self._min_thumb_size, min(self._base_thumb_size, available_h))
        return target

    def _sync_container_height(self) -> None:
        if not hasattr(self, "_container") or self._container is None:
            return
        row_height = max(0, int(self._thumb_size if self._frames or self._items else 0))
        self._container.setFixedHeight(row_height)

    def _update_thumbnail_sizes(self) -> None:
        if not self._frames:
            return
        new_size = self._target_thumb_size()
        if new_size == self._thumb_size:
            return
        self._thumb_size = new_size
        self._sync_container_height()
        for frame in self._frames:
            if not hasattr(frame, "thumb_label"):
                continue
            frame.setFixedSize(self._thumb_size, self._thumb_size)
            frame.thumb_label.setFixedSize(self._thumb_size, self._thumb_size)
            pixmap = getattr(frame.thumb_label, "_orig_pixmap", None)
            if isinstance(pixmap, QPixmap) and not pixmap.isNull():
                frame.thumb_label.setPixmap(self._scaled_thumb(pixmap, self._thumb_size))

    def _load_pixmap(self, item: dict) -> QPixmap | None:
        def _load_oriented(path: str, max_dim: int | None = None) -> QPixmap:
            reader = QImageReader(path)
            reader.setAutoTransform(True)
            if max_dim and max_dim > 0:
                size = reader.size()
                if size.isValid():
                    scaled_size = QSize(size)
                    scaled_size.scale(max_dim, max_dim, Qt.KeepAspectRatio)
                    if (
                        scaled_size.isValid()
                        and scaled_size.width() > 0
                        and scaled_size.height() > 0
                        and (
                            scaled_size.width() < size.width()
                            or scaled_size.height() < size.height()
                        )
                    ):
                        reader.setScaledSize(scaled_size)
            image = reader.read()
            if image.isNull():
                pixmap = QPixmap(path)
            else:
                pixmap = QPixmap.fromImage(image)
            if max_dim and not pixmap.isNull() and (
                pixmap.width() > max_dim or pixmap.height() > max_dim
            ):
                pixmap = pixmap.scaled(
                    max_dim,
                    max_dim,
                    Qt.KeepAspectRatio,
                    Qt.SmoothTransformation,
                )
            return pixmap

        def _cache_key(path: str, variant: str = "") -> str:
            try:
                mtime_ns = Path(path).stat().st_mtime_ns
            except Exception:
                mtime_ns = 0
            return f"{path}|{mtime_ns}|{variant}"

        def _cache_get(path: str, variant: str = "") -> QPixmap | None:
            key = _cache_key(path, variant)
            pix = self._pixmap_cache.get(key)
            if pix is None or pix.isNull():
                return None
            return pix

        def _cache_put(path: str, pix: QPixmap, variant: str = "") -> None:
            if pix.isNull():
                return
            key = _cache_key(path, variant)
            if key in self._pixmap_cache:
                self._pixmap_cache[key] = pix
                return
            if len(self._pixmap_cache) >= self._pixmap_cache_max:
                oldest_key = next(iter(self._pixmap_cache.keys()), None)
                if oldest_key is not None:
                    self._pixmap_cache.pop(oldest_key, None)
            self._pixmap_cache[key] = pix

        img_id = item.get("id")
        filepath = item.get("preview_path") or item.get("filepath")
        if img_id:
            thumb_path = get_thumbnail_path(img_id, "small")
            if thumb_path and Path(thumb_path).exists():
                thumb_path = str(thumb_path)
                cached = _cache_get(thumb_path, "thumb")
                if cached is not None:
                    return cached
                pixmap = _load_oriented(thumb_path, max_dim=max(256, self._decode_max_dim))
                _cache_put(thumb_path, pixmap, "thumb")
                return pixmap
        if filepath:
            filepath = str(filepath)
            variant = f"preview:{self._decode_max_dim}"
            cached = _cache_get(filepath, variant)
            if cached is not None:
                return cached
            pixmap = _load_oriented(filepath, max_dim=self._decode_max_dim)
            _cache_put(filepath, pixmap, variant)
            return pixmap
        return None

    @staticmethod
    def _scaled_thumb(pixmap: QPixmap, size: int) -> QPixmap:
        scaled = pixmap.scaled(size, size, Qt.KeepAspectRatioByExpanding, Qt.SmoothTransformation)
        if scaled.width() == size and scaled.height() == size:
            return scaled
        x = max(0, (scaled.width() - size) // 2)
        y = max(0, (scaled.height() - size) // 2)
        return scaled.copy(x, y, size, size)

    def _apply_crop_overlay(
        self,
        thumb: QPixmap,
        crop_box: tuple[float, float, float, float],
        crop_source_size: tuple[int, int] | None,
    ) -> QPixmap:
        size = thumb.width()
        orig_w = orig_h = None
        if crop_source_size and len(crop_source_size) == 2:
            orig_w, orig_h = crop_source_size
        if not orig_w or not orig_h:
            orig_w = thumb.width()
            orig_h = thumb.height()
        if orig_w <= 0 or orig_h <= 0 or size <= 0:
            return thumb

        scale = max(size / orig_w, size / orig_h)
        scaled_w = orig_w * scale
        scaled_h = orig_h * scale
        x_off = (scaled_w - size) / 2.0
        y_off = (scaled_h - size) / 2.0

        x1 = crop_box[0] * orig_w * scale - x_off
        y1 = crop_box[1] * orig_h * scale - y_off
        x2 = crop_box[2] * orig_w * scale - x_off
        y2 = crop_box[3] * orig_h * scale - y_off

        left = max(0.0, min(x1, x2))
        top = max(0.0, min(y1, y2))
        right = min(size, max(x1, x2))
        bottom = min(size, max(y1, y2))
        if right <= left or bottom <= top:
            return thumb

        annotated = QPixmap(thumb)
        painter = QPainter(annotated)
        pen = QPen(QColor(243, 156, 18), 2)
        painter.setPen(pen)
        painter.setBrush(Qt.NoBrush)
        painter.drawRect(QRectF(left, top, right - left, bottom - top))
        painter.end()
        return annotated

    def _has_spore_measurements(self, image_id: int) -> bool:
        measurements = MeasurementDB.get_measurements_for_image(image_id)
        for measurement in measurements:
            measurement_type = (measurement.get("measurement_type") or "").lower()
            if measurement_type in ("", "manual", "spore"):
                return True
        return False
