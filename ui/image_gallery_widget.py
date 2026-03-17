"""Reusable image thumbnail gallery widget."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

from PySide6.QtCore import Qt, Signal, QEvent, QSize, QRectF, QTimer
from PySide6.QtGui import QPixmap, QPainter, QPen, QColor, QImageReader
from PySide6.QtWidgets import QGraphicsDropShadowEffect
from PySide6.QtWidgets import (
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
)

from database.models import ImageDB, MeasurementDB
from database.schema import load_objectives, objective_display_name, resolve_objective_key
from database.database_tags import DatabaseTerms
from utils.thumbnail_generator import get_thumbnail_path
from .styles import pt


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
        self._items: list[dict] = []
        self._frames: list[QFrame] = []
        self._selected_id = None
        self._selected_keys: set[str | int] = set()
        self._last_clicked_index: int | None = None
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
        self._scroll.viewport().installEventFilter(self)

        self._container = QWidget()
        self._grid = QHBoxLayout(self._container)
        self._grid.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        self._grid.setSpacing(10)
        self._scroll.setWidget(self._container)
        content_layout.addWidget(self._scroll)

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(self._content)

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
        for item in self._items:
            frame = self._create_thumbnail_widget(item)
            self._frames.append(frame)
            self._grid.addWidget(frame)
        if self._selected_id is not None:
            self.select_image(self._selected_id)
        elif self._selected_keys:
            self._apply_selection_styles()

    def eventFilter(self, obj, event):
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
                f"font-size: {pt(8)}pt; padding: 1px 4px; border-radius: 3px; border: none;"
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
                f"font-size: {pt(8)}pt; font-weight: {weight}; padding: 1px 4px; border-radius: 3px; border: none;"
            )
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
        overlay_layout.setContentsMargins(2, 2, 2, 2)
        overlay_layout.setSpacing(4)
        overlay_layout.addStretch()

        if self._show_badges and item.get("has_measurements"):
            badge = QToolButton()
            badge.setText("M")
            badge.setFixedSize(16, 16)
            badge.setStyleSheet(
                "QToolButton { background-color: #27ae60; color: white; border-radius: 8px; border: none;"
                f" font-size: {pt(8)}pt; font-weight: bold; padding: 0px; }}"
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
            delete_btn.setFixedSize(16, 16)
            delete_btn.setStyleSheet(
                f"QToolButton {{ background-color: #e74c3c; color: white; border-radius: 8px; font-size: {pt(8)}pt; }}"
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
        frame.mousePressEvent = lambda e, img_id=frame.image_id, path=frame.image_path: self._on_click(e, img_id, path)
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
        viewport_h = self._scroll.viewport().height() if self._scroll else self._base_thumb_size
        target = max(self._min_thumb_size, min(self._base_thumb_size, viewport_h - 16))
        return target

    def _update_thumbnail_sizes(self) -> None:
        if not self._frames:
            return
        new_size = self._target_thumb_size()
        if new_size == self._thumb_size:
            return
        self._thumb_size = new_size
        for frame in self._frames:
            if not hasattr(frame, "thumb_label"):
                continue
            frame.setFixedSize(self._thumb_size, self._thumb_size)
            frame.thumb_label.setFixedSize(self._thumb_size, self._thumb_size)
            pixmap = getattr(frame.thumb_label, "_orig_pixmap", None)
            if isinstance(pixmap, QPixmap) and not pixmap.isNull():
                frame.thumb_label.setPixmap(self._scaled_thumb(pixmap, self._thumb_size))

    def _load_pixmap(self, item: dict) -> QPixmap | None:
        def _load_oriented(path: str) -> QPixmap:
            reader = QImageReader(path)
            reader.setAutoTransform(True)
            image = reader.read()
            if image.isNull():
                return QPixmap(path)
            return QPixmap.fromImage(image)

        def _cache_key(path: str) -> str:
            try:
                mtime_ns = Path(path).stat().st_mtime_ns
            except Exception:
                mtime_ns = 0
            return f"{path}|{mtime_ns}"

        def _cache_get(path: str) -> QPixmap | None:
            key = _cache_key(path)
            pix = self._pixmap_cache.get(key)
            if pix is None or pix.isNull():
                return None
            return pix

        def _cache_put(path: str, pix: QPixmap) -> None:
            if pix.isNull():
                return
            key = _cache_key(path)
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
            thumb_path = get_thumbnail_path(img_id, "224x224")
            if thumb_path and Path(thumb_path).exists():
                thumb_path = str(thumb_path)
                cached = _cache_get(thumb_path)
                if cached is not None:
                    return cached
                pixmap = _load_oriented(thumb_path)
                _cache_put(thumb_path, pixmap)
                return pixmap
        if filepath:
            filepath = str(filepath)
            cached = _cache_get(filepath)
            if cached is not None:
                return cached
            pixmap = _load_oriented(filepath)
            _cache_put(filepath, pixmap)
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
