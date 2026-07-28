"""Reusable image thumbnail gallery widget."""
from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterable

from PySide6.QtCore import Qt, Signal, QEvent, QSize, QRectF, QTimer, QMimeData, QPoint, QPointF, QThread
from PySide6.QtGui import QPixmap, QPainter, QPen, QColor, QDrag, QShortcut, QKeySequence, QIcon, QPalette
from PySide6.QtWidgets import QGraphicsDropShadowEffect
from PySide6.QtWidgets import (
    QApplication,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QMenu,
    QScrollArea,
    QToolButton,
    QVBoxLayout,
    QWidget,
    QFrame,
    QGridLayout,
    QSizePolicy,
    QStyle,
)

from database.models import ImageDB, MeasurementDB, get_image_tombstones_by_deleted_cloud_id
from database.schema import load_objectives, objective_display_name, resolve_objective_key
from database.database_tags import DatabaseTerms
from utils.thumbnail_generator import get_thumbnail_path
from utils.image_utils import load_oriented_pixmap
from .adaptive_choice_selector import objective_color, objective_short_label
from .styles import pt


# Padding around every thumbnail's outer selection-backdrop container.
# The colored backdrop rectangle that appears when a thumbnail is selected
# spans this padding on every side. Tuned to roughly half of the grid
# spacing (10 px) so a selected thumb's backdrop occupies about half of
# the visible gap between it and its neighbours without touching them.
_THUMB_SELECTION_BACKDROP_PADDING = 6


def _microscope_tag_from_image(image: dict, translate=None) -> tuple[str | None, str | None]:
    """Return (label, color) for the colored microscope tag rendered in
    the thumbnail's bottom-left. Mirrors
    LiveLabTab._microscope_tag_for_metadata but reads from a plain image
    row / import result dict, so every gallery instance can produce the
    same tag."""
    if not isinstance(image, dict):
        return None, None
    tr = translate if callable(translate) else (lambda text: text)
    objective_name = image.get("objective_name")
    if not objective_name:
        lab_metadata = image.get("lab_metadata")
        if isinstance(lab_metadata, dict):
            objective_name = lab_metadata.get("objective_name") or (
                lab_metadata.get("microscope") or {}
            ).get("objective_name")
    if not objective_name:
        return None, None
    objectives = load_objectives()
    objective = objectives.get(str(objective_name))
    tag_text = objective_short_label(objective, str(objective_name))
    if not tag_text:
        tag_text = (
            objective_display_name(objective, str(objective_name))
            if objective
            else str(objective_name)
        )
    contrast = image.get("contrast")
    if contrast is None:
        lab_metadata = image.get("lab_metadata")
        if isinstance(lab_metadata, dict):
            contrast = lab_metadata.get("contrast")
    canonical = DatabaseTerms.canonicalize("contrast", contrast) if contrast else None
    if canonical and str(canonical).strip().lower() not in {"not_set", "not set"}:
        tag_text = f"{tag_text} {DatabaseTerms.translate('contrast', canonical)}"
    return tag_text, objective_color(objective, str(objective_name))

_GALLERY_REORDER_MIME = "application/x-sporely-gallery-item"


@lru_cache(maxsize=1)
def _cloud_status_icon() -> QIcon:
    icon_path = Path(__file__).parent.parent / "assets" / "icons" / "cloud_badge.svg"
    if icon_path.exists():
        return QIcon(str(icon_path))
    return QIcon()


def _tag_text_color(background: QColor) -> str:
    if background.isValid() and background.lightness() >= 180:
        return "#000000"
    return "#ffffff"


@dataclass(frozen=True)
class ThumbnailSelectionColors:
    fill: QColor
    outer: QColor
    inner: QColor
    corner: QColor
    badge_fill: QColor
    badge_border: QColor
    badge_text: QColor


def _mix_qcolors(base: QColor, target: QColor, ratio: float) -> QColor:
    ratio = max(0.0, min(1.0, float(ratio)))
    inverse = 1.0 - ratio
    return QColor(
        int(round(base.red() * inverse + target.red() * ratio)),
        int(round(base.green() * inverse + target.green() * ratio)),
        int(round(base.blue() * inverse + target.blue() * ratio)),
        int(round(base.alpha() * inverse + target.alpha() * ratio)),
    )


def _palette_is_dark(palette: QPalette | None = None) -> bool:
    palette = palette or (QApplication.instance().palette() if QApplication.instance() is not None else None)
    if palette is None:
        return False
    try:
        return palette.window().color().lightness() < 128
    except Exception:
        return False


def thumbnail_selection_colors(dark_theme: bool, palette: QPalette | None = None) -> ThumbnailSelectionColors:
    palette = palette or QPalette()
    window_color = QColor(palette.window().color())
    text_color = QColor(palette.windowText().color())
    highlight_color = QColor(palette.highlight().color())
    accent_color = QColor("#3498db")
    corner_color = QColor("#2a7fff")

    if dark_theme:
        fill = _mix_qcolors(window_color, highlight_color, 0.55)
        fill.setAlpha(90)
        outer = QColor(text_color)
        outer.setAlpha(220)
        inner = QColor(accent_color)
        inner.setAlpha(240)
        corner = QColor(corner_color)
        corner.setAlpha(220)
        badge_fill = QColor(accent_color)
        badge_fill.setAlpha(235)
        badge_border = QColor(255, 255, 255, 80)
        badge_text = QColor(255, 255, 255, 255)
    else:
        fill = _mix_qcolors(window_color, highlight_color, 0.9)
        fill.setAlpha(72)
        outer = QColor(text_color)
        outer.setAlpha(170)
        inner = QColor(accent_color)
        inner.setAlpha(238)
        corner = QColor(corner_color)
        corner.setAlpha(230)
        badge_fill = QColor(accent_color)
        badge_fill.setAlpha(230)
        badge_border = QColor(255, 255, 255, 140)
        badge_text = QColor(255, 255, 255, 255)

    return ThumbnailSelectionColors(
        fill=fill,
        outer=outer,
        inner=inner,
        corner=corner,
        badge_fill=badge_fill,
        badge_border=badge_border,
        badge_text=badge_text,
    )


def _draw_thumbnail_corner_brackets(
    painter: QPainter,
    rect: QRectF,
    color: QColor,
    stroke_width: float,
    corner_len: float,
) -> None:
    if rect.isEmpty() or corner_len <= 0.0:
        return
    edge = rect.adjusted(stroke_width / 2.0, stroke_width / 2.0, -stroke_width / 2.0, -stroke_width / 2.0)
    if edge.width() <= 0.0 or edge.height() <= 0.0:
        return
    painter.save()
    pen = QPen(QColor(color), max(1.0, stroke_width))
    pen.setCapStyle(Qt.RoundCap)
    pen.setJoinStyle(Qt.RoundJoin)
    painter.setPen(pen)
    left = edge.left()
    top = edge.top()
    right = edge.right()
    bottom = edge.bottom()
    painter.drawLine(QPointF(left, top + corner_len), QPointF(left, top))
    painter.drawLine(QPointF(left, top), QPointF(left + corner_len, top))
    painter.drawLine(QPointF(right - corner_len, top), QPointF(right, top))
    painter.drawLine(QPointF(right, top), QPointF(right, top + corner_len))
    painter.drawLine(QPointF(left, bottom - corner_len), QPointF(left, bottom))
    painter.drawLine(QPointF(left, bottom), QPointF(left + corner_len, bottom))
    painter.drawLine(QPointF(right - corner_len, bottom), QPointF(right, bottom))
    painter.drawLine(QPointF(right, bottom - corner_len), QPointF(right, bottom))
    painter.restore()


def _draw_thumbnail_selection_badge(
    painter: QPainter,
    rect: QRectF,
    text: str,
    colors: ThumbnailSelectionColors,
    size_hint: float,
) -> None:
    badge_text = str(text or "").strip()
    if not badge_text:
        return
    badge_margin = max(3.0, min(5.0, size_hint * 0.04))
    badge_pad_x = max(4.0, min(7.0, size_hint * 0.06))
    badge_pad_y = max(2.0, min(4.0, size_hint * 0.03))

    badge_font = painter.font()
    badge_font.setBold(True)
    badge_font.setPixelSize(max(8, min(12, int(round(size_hint * 0.11)))))
    painter.setFont(badge_font)

    metrics = painter.fontMetrics()
    text_width = float(metrics.horizontalAdvance(badge_text))
    text_height = float(metrics.height())
    badge_width = min(max(0.0, rect.width() - (badge_margin * 2.0)), text_width + (badge_pad_x * 2.0))
    badge_height = min(max(0.0, rect.height() - (badge_margin * 2.0)), text_height + (badge_pad_y * 2.0))
    if badge_width <= 0.0 or badge_height <= 0.0:
        return

    badge_rect = QRectF(rect.left() + badge_margin, rect.top() + badge_margin, badge_width, badge_height)
    badge_radius = max(3.0, min(5.5, badge_height / 2.0))
    painter.setPen(QPen(colors.badge_border, max(1.0, min(2.0, size_hint * 0.012))))
    painter.setBrush(colors.badge_fill)
    painter.drawRoundedRect(badge_rect, badge_radius, badge_radius)
    painter.setPen(colors.badge_text)
    painter.drawText(badge_rect.adjusted(0.0, -1.0, 0.0, 1.0), Qt.AlignCenter, badge_text)


def paint_thumbnail_selection_overlay(
    painter: QPainter,
    rect: QRectF,
    *,
    selected: bool = False,
    hovered: bool = False,
    raw_halo_color: str | QColor | None = None,
    palette: QPalette | None = None,
    badge_text: str | None = None,
) -> None:
    if rect.isEmpty():
        return
    dark_theme = _palette_is_dark(palette)
    colors = thumbnail_selection_colors(dark_theme, palette)
    size_hint = min(float(rect.width()), float(rect.height()))
    if size_hint <= 0.0:
        return

    selection_inset = max(0.5, min(1.8, size_hint * 0.012))
    base_rect = QRectF(rect).adjusted(selection_inset, selection_inset, -selection_inset, -selection_inset)
    if base_rect.width() <= 0.0 or base_rect.height() <= 0.0:
        return

    radius = max(4.0, min(8.0, size_hint * 0.055))
    outer_width = max(1.4, min(3.0, size_hint * 0.022))
    inner_width = max(1.1, min(2.4, size_hint * 0.016))

    painter.save()
    painter.setRenderHint(QPainter.Antialiasing)

    # Selection itself is drawn as a square backdrop behind the frame by
    # ImageGalleryWidget._update_thumbnail_selection_backdrop. This overlay
    # only handles the hover glow + RAW halo now — leaving `selected` here
    # would paint a redundant rounded frame on top of the backdrop.
    if selected:
        pass
    elif hovered:
        hover_fill = QColor(colors.fill)
        hover_fill.setAlpha(max(18, hover_fill.alpha() // 2))
        hover_outer = QColor(colors.outer)
        hover_outer.setAlpha(max(100, hover_outer.alpha() - 60))
        painter.setBrush(hover_fill)
        painter.setPen(Qt.NoPen)
        painter.drawRoundedRect(base_rect, radius, radius)
        hover_rect = base_rect.adjusted(
            outer_width / 2.0,
            outer_width / 2.0,
            -outer_width / 2.0,
            -outer_width / 2.0,
        )
        if hover_rect.width() > 0.0 and hover_rect.height() > 0.0:
            painter.setBrush(Qt.NoBrush)
            painter.setPen(QPen(hover_outer, max(1.0, outer_width * 0.75), Qt.SolidLine, Qt.RoundCap, Qt.RoundJoin))
            painter.drawRoundedRect(hover_rect, max(1.0, radius - 0.5), max(1.0, radius - 0.5))
    elif raw_halo_color:
        halo_color = QColor(raw_halo_color)
        if not halo_color.isValid():
            halo_color = QColor(231, 76, 60, 190)
        else:
            halo_color.setAlpha(max(halo_color.alpha(), 190))
        halo_rect = base_rect.adjusted(
            outer_width / 2.0,
            outer_width / 2.0,
            -outer_width / 2.0,
            -outer_width / 2.0,
        )
        if halo_rect.width() > 0.0 and halo_rect.height() > 0.0:
            painter.setBrush(Qt.NoBrush)
            painter.setPen(QPen(halo_color, max(1.0, outer_width), Qt.SolidLine, Qt.RoundCap, Qt.RoundJoin))
            painter.drawRoundedRect(halo_rect, max(1.0, radius - 0.5), max(1.0, radius - 0.5))

    painter.restore()


def _visible_fraction(viewport_rect: QRectF, item_rect: QRectF | None) -> float:
    if item_rect is None:
        return 0.0
    viewport_width = float(viewport_rect.width())
    item_width = float(item_rect.width())
    if viewport_width <= 0.0 or item_width <= 0.0:
        return 0.0
    viewport_left = float(viewport_rect.x())
    viewport_right = viewport_left + viewport_width
    item_left = float(item_rect.x())
    item_right = item_left + item_width
    visible_width = max(0.0, min(viewport_right, item_right) - max(viewport_left, item_left))
    return visible_width / item_width


def center_horizontal_scroll_target(
    viewport_rect: QRectF,
    item_rect: QRectF | None,
    minimum: int,
    maximum: int,
    previous_rect: QRectF | None = None,
    next_rect: QRectF | None = None,
    *,
    visible_neighbor_threshold: float = 0.25,
    margin: float = 24.0,
) -> int | None:
    """Return the scroll offset needed to bring `item_rect` (plus its
    immediate neighbours) into view, or None when no scrolling is required.

    Rules:
    - Compute a "must be visible" range = item ∪ previous neighbour ∪ next
      neighbour. If the whole range already fits inside the viewport, return
      None. Extending the range means clicking a thumbnail near the visible
      edge nudges the strip enough to make the adjacent thumbnails easy to
      reach on the next click.
    - Otherwise, scroll the minimum distance needed to bring the range into
      view with a small margin. Range off the right edge nudges right; off
      the left edge nudges left.
    - `visible_neighbor_threshold` is accepted for backward compatibility
      and ignored.
    """
    del visible_neighbor_threshold  # unused
    if item_rect is None:
        return None
    item_width = float(item_rect.width())
    viewport_width = float(viewport_rect.width())
    if item_width <= 0.0 or viewport_width <= 0.0:
        return None

    item_left = float(item_rect.x())
    item_right = item_left + item_width
    must_left = item_left
    must_right = item_right
    if previous_rect is not None and previous_rect.width() > 0.0:
        must_left = min(must_left, float(previous_rect.x()))
    if next_rect is not None and next_rect.width() > 0.0:
        must_right = max(
            must_right, float(next_rect.x()) + float(next_rect.width())
        )

    view_left = float(viewport_rect.x())
    view_right = view_left + viewport_width
    margin_val = max(0.0, float(margin))

    # Whole must-visible range already fits inside the viewport → done.
    if must_left >= view_left and must_right <= view_right:
        return None

    must_width = must_right - must_left
    if must_width > viewport_width:
        # Can't fit the whole must-visible range. Prefer to reveal the
        # clicked item itself, nudged toward whichever edge it's clipped
        # against — never dead-center it just because a neighbour is off
        # the other side.
        if item_left < view_left:
            target = int(round(item_left - margin_val))
        elif item_right > view_right:
            target = int(round(item_right + margin_val - viewport_width))
        else:
            target = int(round(item_left + (item_width / 2.0) - (viewport_width / 2.0)))
    elif must_left < view_left:
        target = int(round(must_left - margin_val))
    else:  # must_right > view_right
        target = int(round(must_right + margin_val - viewport_width))
    return max(int(minimum), min(int(maximum), target))


def nudge_horizontal_scroll_target(
    viewport_rect: QRectF,
    item_rect: QRectF | None,
    minimum: int,
    maximum: int,
    previous_rect: QRectF | None = None,
    next_rect: QRectF | None = None,
    *,
    nudge_widths: float = 1.5,
    edge_threshold_widths: float = 0.75,
) -> int | None:
    """Return a small horizontal nudge when a thumbnail is near either edge.

    The target shifts the strip by roughly `nudge_widths` thumbnail widths
    toward the center instead of fully centering the clicked thumbnail.
    """
    if item_rect is None:
        return None
    item_width = float(item_rect.width())
    viewport_width = float(viewport_rect.width())
    if item_width <= 0.0 or viewport_width <= 0.0:
        return None

    view_left = float(viewport_rect.x())
    view_right = view_left + viewport_width
    item_left = float(item_rect.x())
    item_right = item_left + item_width
    nudge_px = max(0.0, item_width * float(nudge_widths))
    edge_threshold = max(nudge_px, item_width * float(edge_threshold_widths))

    near_left = item_left < (view_left + edge_threshold)
    near_right = item_right > (view_right - edge_threshold)
    if previous_rect is not None and previous_rect.width() > 0.0:
        near_left = near_left or float(previous_rect.x()) < view_left
    if next_rect is not None and next_rect.width() > 0.0:
        near_right = near_right or (float(next_rect.x()) + float(next_rect.width())) > view_right

    if near_left and near_right:
        return None
    if near_left:
        target = int(round(view_left - nudge_px))
    elif near_right:
        target = int(round(view_left + nudge_px))
    else:
        return None
    return max(int(minimum), min(int(maximum), target))


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


class _ObservationGalleryLoader(QThread):
    """Fetch observation image metadata away from the GUI thread."""

    loaded = Signal(int, object, object)  # observation_id, image rows, measurement image ids

    def __init__(self, observation_id: int) -> None:
        super().__init__()
        self.setObjectName("Observation gallery loader")
        self._observation_id = int(observation_id)

    def run(self) -> None:
        images = []
        measurement_image_ids: set[int] = set()
        try:
            images = ImageDB.get_images_for_observation(self._observation_id)
        except Exception:
            images = []
        try:
            measurements = MeasurementDB.get_measurements_for_observation(self._observation_id)
        except Exception:
            measurements = []
        for measurement in measurements or []:
            image_id = measurement.get("image_id")
            try:
                measurement_image_ids.add(int(image_id))
            except (TypeError, ValueError):
                continue
        self.loaded.emit(self._observation_id, images, measurement_image_ids)


class ImageGalleryWidget(QGroupBox):
    """Collapsible thumbnail gallery for observations or explicit image lists."""

    imageClicked = Signal(object, str)
    imageSelected = Signal(object, str)
    imageDoubleClicked = Signal(object, str)
    measureBadgeClicked = Signal(object, str)
    editRequested = Signal(object, str)
    # Unified delete signal — always fires with a list of keys (can be int
    # DB IDs or str custom IDs like "cal_0"). Single-item deletes (X icon
    # click) fire with a one-element list; multi-item deletes (right-click
    # "Delete selected photos") fire with the full selection.
    deleteImagesRequested = Signal(list)
    moveToObservationRequested = Signal(list)
    selectionChanged = Signal(list)
    publishSelectionChanged = Signal(object)
    itemsReordered = Signal(object)
    observationLoaded = Signal(object)

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
        show_move_to_observation: bool = False,
        show_edit: bool = False,
        publish_checkbox_hint: str = "",
        delete_menu_label_single: str = "",
        delete_menu_label_multi: str = "",
    ) -> None:
        super().__init__(title, parent)
        self._gallery_title = str(title or "")
        self.setCheckable(False)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        self.setFocusPolicy(Qt.StrongFocus)
        self._min_height = max(0, int(min_height))
        self._default_height = max(self._min_height, int(default_height))
        self.setMinimumHeight(self._min_height)

        self._show_delete = show_delete
        self._show_badges = show_badges
        self._multi_select = False
        self._toggle_selection_on_plain_click = False
        self._clear_selection_on_background_click = False
        self._thumbnail_tooltip = thumbnail_tooltip
        self._show_publish_checkbox = bool(show_publish_checkbox)
        self._show_move_to_observation = bool(show_move_to_observation)
        self._show_edit = bool(show_edit)
        self._publish_checkbox_hint = str(publish_checkbox_hint or "").strip()
        # Per-instance delete labels — lets callers say "Remove from batch"
        # / "Remove from staging" instead of the generic "Delete photo" when
        # the action is non-destructive to the underlying file.
        self._delete_menu_label_single = str(delete_menu_label_single or "").strip()
        self._delete_menu_label_multi = str(delete_menu_label_multi or "").strip()
        self._base_thumb_size = max(80, int(thumbnail_size))
        self._min_thumb_size = 80
        self._thumb_size = self._base_thumb_size
        self._fixed_thumbnail_size = False
        self._compact_overlay = False
        self._plain_container = False
        self._decode_max_dim = max(384, self._base_thumb_size * 4)
        self._items: list[dict] = []
        self._frames: list[QFrame] = []
        self._selected_id = None
        self._selected_keys: set[str | int] = set()
        # Filepaths to re-select on the next set_items / observation reload
        # (see set_selection_after_next_load). Survives the transient
        # clear() that fires when the observations table loses selection
        # mid-refresh.
        self._pending_selection_paths: list[str] = []
        self._last_clicked_index: int | None = None
        self._drag_start_pos: QPoint | None = None
        self._drag_start_key = None
        self._reorderable = False
        self._objectives_cache: dict | None = None
        self._publish_checked_by_key: dict[str | int, bool] = {}
        self._suppress_publish_signal = False
        self._pixmap_cache: dict[str, QPixmap] = {}
        self._pixmap_cache_max = 512
        self._render_batch_size = 8
        self._render_generation = 0
        self._render_index = 0
        self._center_request_generation = 0
        self._center_request_key = None
        self._center_reveal_mode = "precise"
        # Scroll behaviour to apply after the next batched render finishes.
        # "preserve" restores the previous horizontal scroll offset (used
        # for metadata refreshes so the view doesn't jitter). "new_at_end"
        # scrolls to the far right so a freshly-appended thumbnail becomes
        # visible. None means "no scroll manipulation from this refresh".
        self._pending_scroll_mode: str | None = None
        self._pending_scroll_restore: int | None = None
        self._pending_scroll_stick_to_end = False
        self._render_timer = QTimer(self)
        self._render_timer.setSingleShot(True)
        self._render_timer.timeout.connect(self._render_next_batch)
        self._observation_load_generation = 0
        self._observation_loaders: set[_ObservationGalleryLoader] = set()
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
        # No inter-container spacing — each thumbnail sits inside a
        # selection-backdrop container that already carries the visual gap
        # (via _THUMB_SELECTION_BACKDROP_PADDING). This lets two adjacent
        # selected thumbnails' backdrops meet in the middle instead of
        # leaving a dead strip between them.
        self._grid.setSpacing(0)
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
        self.set_plain_container(True)

    @staticmethod
    def _item_key_for_publish(item: dict):
        return item.get("id") if item.get("id") is not None else item.get("filepath")

    def _resolve_publish_checked_state(self, item: dict) -> bool:
        key = self._item_key_for_publish(item)
        if key in self._publish_checked_by_key:
            return bool(self._publish_checked_by_key.get(key))
        explicit_publish_selected = item.get("publish_selected")
        if explicit_publish_selected is not None:
            return bool(explicit_publish_selected)
        default_publish_selected = item.get("publish_selected_default")
        if default_publish_selected is not None:
            return bool(default_publish_selected)
        return True

    def clear(self) -> None:
        self._render_generation += 1
        self._observation_load_generation += 1
        self._invalidate_center_requests()
        self._render_timer.stop()
        self._render_index = 0
        self._items = []
        self._selected_id = None
        self._selected_keys = set()
        self._publish_checked_by_key = {}
        self._clear_widgets()

    def set_selection_after_next_load(self, paths) -> None:
        """Remember filepaths that should be re-selected as soon as the
        next observation-load / set_items call produces items containing
        them. Used to survive the "clear -> async reload" round-trip that
        fires when the observations table momentarily loses selection
        during a refresh."""
        cleaned: list[str] = []
        for path in paths or []:
            text = str(path or "").strip()
            if text:
                cleaned.append(text)
        self._pending_selection_paths = cleaned

    def invalidate_pixmap_cache(self, path: str | Path | None = None) -> None:
        if path is None:
            self._pixmap_cache.clear()
            return
        try:
            target = str(Path(path))
        except Exception:
            target = str(path or "").strip()
        if not target:
            return
        prefix = f"{target}|"
        stale_keys = [key for key in self._pixmap_cache.keys() if key.startswith(prefix)]
        for key in stale_keys:
            self._pixmap_cache.pop(key, None)

    def _clear_widgets(self) -> None:
        while self._grid.count():
            item = self._grid.takeAt(0)
            widget = item.widget()
            if widget is None:
                continue
            try:
                widget.hide()
            except Exception:
                pass
            try:
                widget.setParent(None)
            except Exception:
                pass
            widget.deleteLater()
        self._frames = []
        self._sync_container_height()

    def _invalidate_center_requests(self) -> None:
        self._center_request_generation += 1
        self._center_request_key = None

    def _thumbnail_selection_overlay_visible(self, frame: QFrame) -> bool:
        return bool(
            getattr(frame, "_thumbnail_selected", False)
            or getattr(frame, "_thumbnail_hovered", False)
            or getattr(frame, "raw_halo_color", None)
        )

    def _update_thumbnail_selection_overlay(self, frame: QFrame) -> None:
        overlay = getattr(frame, "_thumbnail_selection_overlay", None)
        if overlay is None:
            return
        visible = self._thumbnail_selection_overlay_visible(frame)
        try:
            overlay.setGeometry(frame.rect())
            overlay.setVisible(visible)
            if visible:
                overlay.raise_()
                overlay.update()
        except Exception:
            pass

    def _queue_center_on_key(self, key) -> None:
        if key is None:
            return
        self._center_request_generation += 1
        generation = self._center_request_generation
        self._center_request_key = key
        QTimer.singleShot(0, lambda gen=generation, requested_key=key: self._center_on_key_if_current(gen, requested_key))

    def _center_on_key_if_current(self, generation: int, key, retries: int = 3) -> None:
        if generation != self._center_request_generation or key != self._center_request_key:
            return
        if not self._scroll:
            return
        frame = self._frame_for_key(key)
        if frame is None:
            # Frame hasn't been rendered yet (batched render still in
            # progress). Try again shortly instead of silently giving up —
            # otherwise a caller that queues a center right after set_items
            # will land on scroll=0 whenever the target isn't in the first
            # batch.
            if retries > 0:
                QTimer.singleShot(
                    16,
                    lambda gen=generation, k=key, r=retries - 1: self._center_on_key_if_current(gen, k, r),
                )
            return
        scrollbar = self._scroll.horizontalScrollBar()
        viewport = self._scroll.viewport()
        if scrollbar is None or viewport is None:
            return
        # For scroll targeting, use the OUTER selection-backdrop container's
        # geometry — that's what's actually laid out in self._grid. The inner
        # frame sits offset inside its container by the backdrop padding.
        def _layout_geometry(widget):
            outer = getattr(widget, "_outer_container", widget)
            return outer.geometry() if outer is not None else widget.geometry()

        # Force the grid layout to run so freshly-added rows have real
        # geometry — otherwise .geometry() reports (0, 0, 0, 0) and we'd
        # snap the strip to scroll=0.
        grid_container = self._grid.parentWidget() if hasattr(self, "_grid") else None
        if grid_container is not None and grid_container.layout() is not None:
            try:
                grid_container.layout().activate()
            except Exception:
                pass
        frame_geometry = _layout_geometry(frame)
        if frame_geometry.width() <= 0 and retries > 0:
            # Layout hasn't settled yet — try again shortly.
            QTimer.singleShot(
                16,
                lambda gen=generation, k=key, r=retries - 1: self._center_on_key_if_current(gen, k, r),
            )
            return
        index = self._index_for_key(key)
        previous_frame = self._frames[index - 1] if index is not None and index > 0 and index - 1 < len(self._frames) else None
        next_frame = self._frames[index + 1] if index is not None and index + 1 < len(self._frames) else None
        viewport_rect = QRectF(
            float(scrollbar.value()),
            0.0,
            float(viewport.width()),
            float(viewport.height()),
        )
        previous_geometry = QRectF(_layout_geometry(previous_frame)) if previous_frame is not None else None
        next_geometry = QRectF(_layout_geometry(next_frame)) if next_frame is not None else None
        target = None
        if self._center_reveal_mode == "nudge":
            target = nudge_horizontal_scroll_target(
                viewport_rect,
                QRectF(frame_geometry),
                int(scrollbar.minimum()),
                int(scrollbar.maximum()),
                previous_geometry,
                next_geometry,
            )
        if target is None:
            target = center_horizontal_scroll_target(
                viewport_rect,
                QRectF(frame_geometry),
                int(scrollbar.minimum()),
                int(scrollbar.maximum()),
                previous_geometry,
                next_geometry,
            )
        if target is None:
            return
        scrollbar.setValue(target)
        # Re-apply on the next event-loop tick. scrollbar.maximum() may still
        # be growing as Qt lays out newly-added frames; a target beyond the
        # current maximum gets silently clamped on the first setValue, so we
        # need one more pass once the range has settled. Mirrors the
        # follow-up tick in _apply_pending_scroll.
        QTimer.singleShot(
            0,
            lambda sb=scrollbar, t=target: (
                sb.setValue(max(int(sb.minimum()), min(int(sb.maximum()), int(t))))
                if sb is not None else None
            ),
        )

    def _set_frame_selected_state(self, frame: QFrame, selected: bool) -> None:
        selected = bool(selected)
        if bool(getattr(frame, "_thumbnail_selected", False)) == selected:
            return
        frame._thumbnail_selected = selected
        self._update_thumbnail_selection_backdrop(frame)
        self._update_thumbnail_selection_overlay(frame)

    def _update_thumbnail_selection_backdrop(self, frame: QFrame) -> None:
        """Toggle the outer selection-backdrop container's background based
        on selection state. Replaces the old rounded overlay border with a
        square backdrop that surrounds the frame + a few px of padding."""
        container = getattr(frame, "_outer_container", None)
        if container is None:
            return
        selected = bool(getattr(frame, "_thumbnail_selected", False))
        colors = thumbnail_selection_colors(_palette_is_dark(container.palette()))
        if selected:
            background = QColor(colors.outer)
            background.setAlpha(min(255, background.alpha() + 30))
            rgba = (
                f"rgba({background.red()}, {background.green()}, "
                f"{background.blue()}, {background.alpha()})"
            )
            container.setStyleSheet(
                f"QWidget#thumbSelectionContainer {{ background-color: {rgba}; }}"
            )
        else:
            container.setStyleSheet(
                "QWidget#thumbSelectionContainer { background-color: transparent; }"
            )

    def _set_frame_hovered_state(self, frame: QFrame, hovered: bool) -> None:
        hovered = bool(hovered)
        if bool(getattr(frame, "_thumbnail_hovered", False)) == hovered:
            return
        frame._thumbnail_hovered = hovered
        self._update_thumbnail_selection_overlay(frame)

    def set_fixed_thumbnail_size(self, enabled: bool) -> None:
        self._fixed_thumbnail_size = bool(enabled)
        self._thumb_size = self._target_thumb_size()
        self._update_thumbnail_sizes()

    def set_center_reveal_mode(self, mode: str) -> None:
        text = str(mode or "").strip().lower()
        if text not in {"precise", "nudge"}:
            text = "precise"
        self._center_reveal_mode = text

    def set_compact_overlay(self, enabled: bool) -> None:
        self._compact_overlay = bool(enabled)
        if self._items:
            self._render()

    def set_plain_container(self, enabled: bool = True) -> None:
        self._plain_container = bool(enabled)
        self.setTitle("" if self._plain_container else self._gallery_title)
        if self._plain_container:
            self.setObjectName("plainImageGallery")
            self.setStyleSheet(
                "QGroupBox#plainImageGallery { background: transparent; border: none; "
                "border-radius: 0px; margin: 0px; padding: 0px; }"
                "QGroupBox#plainImageGallery::title { height: 0px; margin: 0px; padding: 0px; }"
            )
            self.setContentsMargins(0, 0, 0, 0)
            if self.layout() is not None:
                self.layout().setContentsMargins(0, 0, 0, 0)
            self._content.layout().setContentsMargins(0, 0, 0, 0)
            self._scroll.setFrameShape(QFrame.NoFrame)
            self._scroll.setStyleSheet("QScrollArea { background: transparent; border: none; }")
            self._scroll.viewport().setStyleSheet("background: transparent;")
        else:
            self.setObjectName("")
            self.setStyleSheet("")
            self.setContentsMargins(0, 0, 0, 0)
            if self.layout() is not None:
                self.layout().setContentsMargins(0, 0, 0, 0)

    def preferred_single_row_height(self) -> int:
        title_height = 0 if self._plain_container else max(24, self.fontMetrics().height() + 12)
        frame_height = int(self._scroll.frameWidth()) * 2 if self._scroll is not None else 2
        scrollbar_height = (
            int(self._scroll.horizontalScrollBar().sizeHint().height())
            if self._scroll is not None
            else 16
        )
        spacing = 0 if self._plain_container else 10
        style = self.style()
        if style is not None and not self._plain_container:
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

    def set_items(
        self,
        items: Iterable[dict],
        preserve_scroll: bool = False,
        *,
        reveal: str | None = None,
    ) -> None:
        # `reveal` (preferred): "preserve" | "new_at_end" | "off" (or None).
        # `preserve_scroll=True` remains for callers that haven't switched
        # to the reveal API; when both are unset the refresh doesn't touch
        # the scroll position.
        mode = reveal
        if mode is None:
            mode = "preserve" if preserve_scroll else "off"
        mode = str(mode or "off").strip().lower()
        if mode not in {"preserve", "new_at_end", "off"}:
            mode = "off"
        self._pending_scroll_mode = mode if mode != "off" else None
        if mode == "preserve" and self._scroll is not None:
            scrollbar = self._scroll.horizontalScrollBar()
            self._pending_scroll_restore = int(scrollbar.value()) if scrollbar is not None else None
            self._pending_scroll_stick_to_end = bool(
                scrollbar is not None
                and int(scrollbar.maximum()) - int(scrollbar.value()) <= 2
            )
        else:
            self._pending_scroll_restore = None
            self._pending_scroll_stick_to_end = False
        if self._pending_scroll_mode is not None:
            # Cancel any pending center-request so it doesn't fight our
            # scroll intent once layout settles.
            self._center_request_generation += 1
            self._center_request_key = None
        self._observation_load_generation += 1
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
            publish_selected = self._resolve_publish_checked_state(item)
            self._publish_checked_by_key[item_key] = publish_selected
            self._items.append(
                {
                    "id": item_id,
                    "filepath": item_path,
                    "preview_path": item.get("preview_path"),
                    "has_measurements": item.get("has_measurements", False),
                    "image_number": item.get("image_number", idx + 1),
                    "badges": item.get("badges", []),
                    "center_badge": item.get("center_badge"),
                    "microscope_tag_text": item.get("microscope_tag_text"),
                    "microscope_tag_color": item.get("microscope_tag_color"),
                    "gps_tag_text": item.get("gps_tag_text"),
                    "gps_tag_highlight": item.get("gps_tag_highlight", False),
                    "gps_tag_color": item.get("gps_tag_color"),
                    "publish_selected": publish_selected,
                    "publish_selected_default": item.get("publish_selected_default"),
                    "frame_border_color": item.get("frame_border_color"),
                    "raw_halo_color": item.get("raw_halo_color"),
                    "cloud_id": item.get("cloud_id"),
                    "cloud_uploaded": item.get("cloud_uploaded"),
                    "cloud_tombstone_synced": item.get("cloud_tombstone_synced"),
                }
            )
        self._consume_pending_selection_paths()
        self._render()

    def set_observation_id(self, observation_id: int | None, *, reveal: str | None = None) -> None:
        self._observation_load_generation += 1
        if not observation_id:
            self.clear()
            return
        images = ImageDB.get_images_for_observation(observation_id)
        measurement_image_ids = self._spore_measurement_image_ids_for_observation(observation_id)
        self._set_observation_rows(observation_id, images, measurement_image_ids, reveal=reveal)

    def set_observation_id_async(self, observation_id: int | None, *, reveal: str | None = None) -> None:
        self._observation_load_generation += 1
        generation = self._observation_load_generation
        if not observation_id:
            self.clear()
            return
        loader = _ObservationGalleryLoader(int(observation_id))
        self._observation_loaders.add(loader)
        loader.loaded.connect(
            lambda loaded_obs_id, images, measurement_ids, gen=generation, r=reveal:
                self._on_observation_rows_loaded(gen, loaded_obs_id, images, measurement_ids, reveal=r)
        )
        loader.finished.connect(lambda worker=loader: self._observation_loaders.discard(worker))
        loader.finished.connect(loader.deleteLater)
        loader.start(QThread.LowPriority)

    def _on_observation_rows_loaded(
        self,
        generation: int,
        observation_id: int,
        images: object,
        measurement_image_ids: object,
        *,
        reveal: str | None = None,
    ) -> None:
        if generation != self._observation_load_generation:
            return
        try:
            measurement_ids = {int(v) for v in (measurement_image_ids or set())}
        except Exception:
            measurement_ids = set()
        self._set_observation_rows(observation_id, list(images or []), measurement_ids, reveal=reveal)
        self.observationLoaded.emit(int(observation_id))

    def _set_observation_rows(
        self,
        observation_id: int | None,
        images: Iterable[dict],
        measurement_image_ids: set[int],
        *,
        reveal: str | None = None,
    ) -> None:
        if not observation_id:
            self.clear()
            return
        objectives = self._get_objectives_cache()
        objective_label_cache: dict[str, str | None] = {}
        image_rows = list(images or [])
        cloud_ids = [
            str(img.get("cloud_id") or "").strip()
            for img in image_rows
            if str(img.get("cloud_id") or "").strip()
        ]
        cloud_tombstones = get_image_tombstones_by_deleted_cloud_id(cloud_ids) if cloud_ids else {}
        items = []
        for idx, img in enumerate(image_rows):
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
            badges = self.build_gallery_badges(
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
                lab_metadata=img.get("lab_metadata"),
                translate=self.tr,
            )
            cloud_id = str(img.get("cloud_id") or "").strip()
            cloud_tombstone = cloud_tombstones.get(cloud_id) if cloud_id else None
            cloud_tombstone_synced = bool(str((cloud_tombstone or {}).get("delete_synced_at") or "").strip())
            microscope_tag_text = img.get("microscope_tag_text")
            microscope_tag_color = img.get("microscope_tag_color")
            if microscope_tag_text is None:
                computed_text, computed_color = _microscope_tag_from_image(img, self.tr)
                microscope_tag_text = computed_text
                if microscope_tag_color is None:
                    microscope_tag_color = computed_color
            # When the microscope tag is shown separately in the bottom-left,
            # the first badge (image-type + objective detail) becomes
            # redundant — strip it, matching Live Lab's behaviour.
            if (
                microscope_tag_text
                and badges
                and image_type == "microscope"
            ):
                badges = badges[1:]
            items.append(
                {
                    "id": img_id,
                    "filepath": img.get("filepath"),
                    "has_measurements": bool(img_id and int(img_id) in measurement_image_ids),
                    "image_number": idx + 1,
                    "badges": badges,
                    "microscope_tag_text": microscope_tag_text,
                    "microscope_tag_color": microscope_tag_color,
                    "gps_tag_color": img.get("gps_tag_color"),
                    "cloud_id": cloud_id or None,
                    "cloud_uploaded": bool(cloud_id and not cloud_tombstone_synced),
                    "cloud_tombstone_synced": cloud_tombstone_synced,
                    "publish_selected_default": image_type != "microscope",
                }
            )
        self.set_items(items, reveal=reveal)

    def _consume_pending_selection_paths(self) -> None:
        pending = getattr(self, "_pending_selection_paths", None)
        if not pending:
            return
        pending_set = {str(path or "").strip() for path in pending if path}
        self._pending_selection_paths = []
        if not pending_set:
            return
        matched_keys: set[str | int] = set()
        first_key: str | int | None = None
        first_id: object | None = None
        for item in self._items:
            filepath = str(item.get("filepath") or "").strip()
            if filepath and filepath in pending_set:
                key = item.get("id") if item.get("id") is not None else filepath
                if key is None:
                    continue
                matched_keys.add(key)
                if first_key is None:
                    first_key = key
                    first_id = item.get("id")
        if matched_keys:
            self._selected_keys = matched_keys
            self._selected_id = first_id
            self._last_clicked_index = self._index_for_key(first_key)

    @staticmethod
    def _cloud_badge_visible(item: dict) -> bool:
        if not item:
            return False
        explicit_state = item.get("cloud_uploaded")
        if explicit_state is not None:
            return bool(explicit_state)
        cloud_id = str(item.get("cloud_id") or "").strip()
        if not cloud_id:
            return False
        tombstone_synced = item.get("cloud_tombstone_synced")
        if tombstone_synced is not None:
            return not bool(tombstone_synced)
        return True

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

    def _set_context_menu_selection(self, frame: QFrame | None) -> None:
        if frame is None:
            return
        key = getattr(frame, "image_key", None)
        if key is None or key in self._selected_keys:
            return
        image_id = getattr(frame, "image_id", None)
        if self._multi_select:
            self._selected_id = image_id
            self._selected_keys = {key}
            self._last_clicked_index = self._index_for_key(key)
            self._apply_selection_styles()
            self.selectionChanged.emit(self.selected_paths())
        else:
            self.select_image(image_id)

    def _show_thumbnail_context_menu(self, frame: QFrame, global_pos: QPoint) -> None:
        self._set_context_menu_selection(frame)
        selected_keys = self.selected_image_keys()
        if not selected_keys:
            return

        menu = QMenu(self)
        edit_action = None
        if self._show_edit:
            edit_action = menu.addAction(self.tr("Edit photo"))
        delete_action = None
        if self._show_delete:
            if len(selected_keys) > 1:
                delete_text = self._delete_menu_label_multi or self.tr("Delete selected photos")
            else:
                delete_text = self._delete_menu_label_single or self.tr("Delete photo")
            delete_action = menu.addAction(delete_text)
        move_action = None
        if self._show_move_to_observation:
            move_action = menu.addAction(self.tr("Move to observation"))
        # An empty menu (no visible actions for this context) shouldn't pop up.
        if not menu.actions():
            return

        chosen = menu.exec(global_pos)
        if edit_action is not None and chosen == edit_action:
            image_id = getattr(frame, "image_id", None)
            image_path = getattr(frame, "image_path", "") or ""
            self.editRequested.emit(image_id, image_path)
            return
        if delete_action is not None and chosen == delete_action:
            self.deleteImagesRequested.emit(list(selected_keys))
        elif move_action is not None and chosen == move_action:
            self.moveToObservationRequested.emit(list(selected_keys))

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

    @staticmethod
    def build_gallery_badges(
        image_type: str | None,
        objective_name: str | None = None,
        contrast: str | None = None,
        scale_microns_per_pixel: float | None = None,
        custom_scale: bool = False,
        needs_scale: bool = False,
        resize_to_optimal: bool = False,
        lab_metadata=None,
        translate=None,
    ) -> list[str]:
        badges = ImageGalleryWidget.build_image_type_badges(
            image_type=image_type,
            objective_name=objective_name,
            contrast=contrast,
            scale_microns_per_pixel=scale_microns_per_pixel,
            custom_scale=custom_scale,
            needs_scale=needs_scale,
            resize_to_optimal=resize_to_optimal,
            translate=translate,
        )
        badges.extend(ImageGalleryWidget.build_raw_source_badges(lab_metadata, translate=translate))
        return badges

    @staticmethod
    def build_raw_source_badges(lab_metadata, translate=None) -> list[str]:
        tr = translate if translate is not None else (lambda text: text)
        if not isinstance(lab_metadata, dict):
            return []
        raw_processing = lab_metadata.get("raw_processing")
        if not isinstance(raw_processing, dict):
            return []
        source = raw_processing.get("source")
        if not isinstance(source, dict):
            return []
        if str(source.get("kind") or "").strip().lower() == "camera_raw":
            return [tr("From raw")]
        return []

    def select_image(self, image_id: int | None, center: bool = True) -> None:
        current_keys = {image_id} if image_id is not None else set()
        if image_id == self._selected_id and self._selected_keys == current_keys:
            self._last_clicked_index = self._index_for_key(image_id)
            if image_id is not None and center:
                self._queue_center_on_key(image_id)
            return
        previous_id = self._selected_id
        previous_frame = self._frame_for_key(previous_id) if previous_id is not None else None
        new_frame = self._frame_for_key(image_id) if image_id is not None else None
        self._selected_id = image_id
        self._selected_keys = set(current_keys)
        self._last_clicked_index = self._index_for_key(image_id)
        if previous_frame is not None and previous_id != image_id:
            self._set_frame_selected_state(previous_frame, False)
        if new_frame is not None:
            self._set_frame_selected_state(new_frame, True)
        if image_id is not None and center:
            self._queue_center_on_key(image_id)

    def publish_selected_ids(self) -> set[int]:
        selected: set[int] = set()
        for item in self._items:
            item_id = item.get("id")
            if item_id is None:
                continue
            if bool(self._resolve_publish_checked_state(item)):
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
        self._render_generation += 1
        self._render_timer.stop()
        self._render_index = 0
        self._clear_widgets()
        self._thumb_size = self._target_thumb_size()
        self._sync_container_height()
        if not self._items:
            return
        self._render_next_batch()

    def _render_next_batch(self) -> None:
        generation = self._render_generation
        end_index = min(len(self._items), self._render_index + self._render_batch_size)
        while self._render_index < end_index:
            item = self._items[self._render_index]
            self._render_index += 1
            # _create_thumbnail_widget returns the outer selection-backdrop
            # container. self._frames still tracks the inner QFrame so all
            # per-frame lookups (image_key, mouse handlers, etc.) stay
            # unchanged; the grid gets the container so the padded backdrop
            # has room to render behind the frame.
            container = self._create_thumbnail_widget(item)
            frame = getattr(container, "_thumbnail_frame", container)
            self._frames.append(frame)
            self._grid.addWidget(container)
            key = self._item_key(item)
            if key in self._selected_keys:
                self._set_frame_selected_state(frame, True)
        # If the caller told us to manage scroll for this refresh
        # ("preserve" or "new_at_end"), skip the "recenter on selected"
        # behaviour so we don't fight our own scroll intent.
        managed_scroll = self._pending_scroll_mode is not None
        if self._selected_id is not None:
            if self._multi_select and self._selected_keys:
                if self._selected_id not in self._selected_keys:
                    self._selected_id = None
                    for item in self._items:
                        key = self._item_key(item)
                        if key in self._selected_keys:
                            self._selected_id = item.get("id")
                            break
                self._last_clicked_index = self._index_for_key(self._selected_id)
                self._apply_selection_styles()
                if self._selected_id is not None and not managed_scroll:
                    self._queue_center_on_key(self._selected_id)
            else:
                if not managed_scroll:
                    self._queue_center_on_key(self._selected_id)
        elif self._selected_keys:
            self._apply_selection_styles()
        render_done = self._render_index >= len(self._items)
        if generation == self._render_generation and not render_done:
            self._render_timer.start(0)
            return
        if managed_scroll and self._scroll is not None:
            mode = self._pending_scroll_mode
            snapshot = self._pending_scroll_restore
            stick_to_end = self._pending_scroll_stick_to_end
            self._pending_scroll_mode = None
            self._pending_scroll_restore = None
            self._pending_scroll_stick_to_end = False
            self._apply_pending_scroll(mode, snapshot, stick_to_end=stick_to_end)

    def _apply_pending_scroll(
        self,
        mode: str | None,
        snapshot: int | None,
        *,
        stick_to_end: bool = False,
    ) -> None:
        if mode is None or self._scroll is None:
            return
        scrollbar = self._scroll.horizontalScrollBar()
        if scrollbar is None:
            return

        def _apply(retries: int = 8):
            try:
                current_max = int(scrollbar.maximum())
                follow_end = mode == "new_at_end" or (mode == "preserve" and stick_to_end)
                if follow_end:
                    target = current_max
                elif mode == "preserve":
                    target = int(snapshot if snapshot is not None else scrollbar.value())
                else:
                    return
                clamped = max(int(scrollbar.minimum()), min(int(scrollbar.maximum()), target))
                scrollbar.setValue(clamped)
                if follow_end and retries > 0:
                    # Thumbnail/layout updates can change the range after an
                    # apparently stable tick. Keep following the right edge
                    # briefly instead of stopping at the first stable value.
                    QTimer.singleShot(16, lambda remaining=retries - 1: _apply(remaining))
            except RuntimeError:
                pass

        # Immediate best-effort; then re-apply on the next event-loop tick
        # so the scrollbar range has had a chance to grow as Qt lays out
        # the freshly-added frames.
        _apply()
        QTimer.singleShot(0, _apply)

    def eventFilter(self, obj, event):
        if (
            self._clear_selection_on_background_click
            and obj in {self._container, self._scroll.viewport()}
            and event.type() == QEvent.MouseButtonPress
            and getattr(event, "button", lambda: None)() == Qt.LeftButton
        ):
            self.exit_edit_selection()
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
            self._set_frame_hovered_state(obj, event.type() == QEvent.Enter and not is_selected)
        return super().eventFilter(obj, event)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._update_thumbnail_sizes()

    def sizeHint(self) -> QSize:
        return QSize(320, self._default_height)

    def minimumSizeHint(self) -> QSize:
        return QSize(120, self._min_height)

    def _frame_style(self, selected: bool = False, border_color: str | None = None) -> str:
        border = "#2980b9" if selected else (border_color or "#bdc3c7")
        return (
            "QFrame { border: 2px solid %s; border-radius: 5px; background: white; }"
        ) % border

    @staticmethod
    def _apply_frame_glow(frame: QFrame, selected: bool, hovered: bool = False) -> None:
        raw_halo_color = getattr(frame, "raw_halo_color", None)
        # Selection is now indicated by the outer square backdrop — the old
        # drop-shadow was too soft and combined weirdly with the backdrop.
        # Keep the shadow only for RAW halo + hover states.
        if selected and raw_halo_color:
            # Retain the RAW halo when a RAW thumbnail is selected.
            pass  # falls through into the raw_halo_color branch below
        if raw_halo_color:
            effect = QGraphicsDropShadowEffect(frame)
            effect.setBlurRadius(22)
            effect.setOffset(0, 0)
            effect_color = QColor(raw_halo_color)
            if not effect_color.isValid():
                effect_color = QColor(231, 76, 60, 190)
            effect.setColor(effect_color)
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
        frame._thumbnail_selected = False
        frame._thumbnail_hovered = False
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
        thumb_label.setAttribute(Qt.WA_TransparentForMouseEvents, True)

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
        image_container.mousePressEvent = lambda e, f=frame: self._on_frame_mouse_press(e, f)
        image_container.mouseMoveEvent = lambda e, f=frame: self._on_frame_mouse_move(e, f)
        image_container.mouseReleaseEvent = lambda e: setattr(self, "_drag_start_pos", None)
        image_container.mouseDoubleClickEvent = lambda e, img_id=item.get("id"), path=item.get("filepath"): self.imageDoubleClicked.emit(img_id, path or "")

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

        cloud_badge = None
        cloud_badge_visible = self._cloud_badge_visible(item)
        microscope_tag_text = str(item.get("microscope_tag_text") or "").strip() or None
        microscope_tag_color = item.get("microscope_tag_color")
        gps_tag_text = item.get("gps_tag_text")
        if cloud_badge_visible:
            top_center = QWidget()
            top_center.setAttribute(Qt.WA_TransparentForMouseEvents, True)
            top_center_layout = QVBoxLayout(top_center)
            top_center_layout.setContentsMargins(0, 0, 0, 0)
            top_center_layout.setSpacing(2)

            if cloud_badge_visible:
                cloud_badge = QLabel()
                cloud_badge.setFixedSize(20, 20)
                cloud_badge.setAlignment(Qt.AlignCenter)
                cloud_badge.setStyleSheet(
                    "QLabel { background-color: transparent; border: none; }"
                )
                cloud_badge.setAttribute(Qt.WA_TransparentForMouseEvents, True)
                cloud_badge.setPixmap(_cloud_status_icon().pixmap(QSize(18, 18)))
                cloud_badge.setToolTip(self.tr("Uploaded to Sporely Cloud"))
                top_center_layout.addWidget(cloud_badge, 0, Qt.AlignHCenter)

            image_layout.addWidget(top_center, 0, 0, alignment=Qt.AlignTop | Qt.AlignHCenter)

        center_badge = str(item.get("center_badge") or "").strip()
        if center_badge:
            center_badge_label = QLabel(center_badge)
            center_badge_label.setStyleSheet(
                "color: #ffffff; background-color: rgba(231, 76, 60, 205);"
                f"font-size: {pt(8)}pt; font-weight: bold; padding: 3px 8px; border-radius: 4px; border: none;"
            )
            center_badge_label.setAttribute(Qt.WA_TransparentForMouseEvents, True)
            image_layout.addWidget(center_badge_label, 0, 0, alignment=Qt.AlignCenter)

        badges = item.get("badges") or []
        clean_badges = [str(b).strip() for b in badges if b]
        raw_badge_text = None
        for idx in range(len(clean_badges) - 1, -1, -1):
            if "raw" in clean_badges[idx].lower():
                raw_badge_text = clean_badges.pop(idx)
                break

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

        if microscope_tag_text:
            bottom_left = QWidget()
            bottom_left.setAttribute(Qt.WA_TransparentForMouseEvents, True)
            bottom_left_layout = QVBoxLayout(bottom_left)
            bottom_left_layout.setContentsMargins(2, 2, 2, 2)
            bottom_left_layout.setSpacing(2)

            # "From raw" first so it sits ABOVE the colored objective tag in
            # the stack, anchored to the bottom-left corner of the thumbnail.
            if raw_badge_text:
                raw_label = _make_badge(raw_badge_text, raw_badge_text == "R")
                bottom_left_layout.addWidget(raw_label, 0, Qt.AlignLeft)

            microscope_label = QLabel(str(microscope_tag_text))
            microscope_color = QColor(microscope_tag_color) if microscope_tag_color is not None else QColor("#3498db")
            text_color = _tag_text_color(microscope_color)
            if microscope_color.isValid():
                microscope_color.setAlpha(220)
                background = (
                    f"rgba({microscope_color.red()}, {microscope_color.green()}, {microscope_color.blue()}, {microscope_color.alpha()})"
                )
            else:
                background = "#3498db"
            microscope_label.setStyleSheet(
                f"color: {text_color}; background-color: {background};"
                f"font-size: {overlay_label_font_px}{'px' if self._compact_overlay else 'pt'}; font-weight: bold;"
                f" padding: {overlay_label_pad_v}px {overlay_label_pad_h}px; border-radius: 3px; border: none;"
            )
            if self._compact_overlay:
                microscope_label.setMaximumWidth(max(30, self._thumb_size - overlay_btn_size - 28))
            microscope_label.setAttribute(Qt.WA_TransparentForMouseEvents, True)
            bottom_left_layout.addWidget(microscope_label, 0, Qt.AlignLeft)

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
                bottom_left_layout.addLayout(first_row)
                if raw_badge_text:
                    raw_row = QHBoxLayout()
                    raw_row.setContentsMargins(0, 0, 0, 0)
                    raw_row.setSpacing(2)
                    raw_row.addWidget(_make_badge(raw_badge_text, raw_badge_text == "R"))
                    raw_row.addStretch(1)
                    bottom_left_layout.addLayout(raw_row)
                for extra_text in clean_badges[consumed:]:
                    bottom_left_layout.addWidget(_make_badge(extra_text, extra_text == "R"))

            image_layout.addWidget(bottom_left, 0, 0, alignment=Qt.AlignBottom | Qt.AlignLeft)
        else:
            if clean_badges:
                badge_container = QWidget()
                badge_layout = QVBoxLayout(badge_container)
                badge_layout.setContentsMargins(2, 2, 2, 2)
                badge_layout.setSpacing(2)

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
                if raw_badge_text:
                    raw_row = QHBoxLayout()
                    raw_row.setContentsMargins(0, 0, 0, 0)
                    raw_row.setSpacing(2)
                    raw_row.addWidget(_make_badge(raw_badge_text, raw_badge_text == "R"))
                    raw_row.addStretch(1)
                    badge_layout.addLayout(raw_row)
                for extra_text in clean_badges[consumed:]:
                    badge_layout.addWidget(_make_badge(extra_text, extra_text == "R"))
                image_layout.addWidget(badge_container, 0, 0, alignment=Qt.AlignBottom | Qt.AlignLeft)

            if gps_tag_text:
                bottom_right = QWidget()
                bottom_right.setAttribute(Qt.WA_TransparentForMouseEvents, True)
                bottom_right_layout = QVBoxLayout(bottom_right)
                bottom_right_layout.setContentsMargins(0, 0, 0, 0)
                bottom_right_layout.setSpacing(2)

                if gps_tag_text:
                    gps_label = QLabel(str(gps_tag_text))
                    gps_highlight = bool(item.get("gps_tag_highlight"))
                    gps_color = item.get("gps_tag_color")
                    if gps_color is not None:
                        color_value = QColor(gps_color)
                        if color_value.isValid():
                            color_value.setAlpha(220)
                            background = (
                                f"rgba({color_value.red()}, {color_value.green()}, {color_value.blue()}, {color_value.alpha()})"
                            )
                            color = "#ffffff"
                            weight = "bold"
                        else:
                            color = "#ffffff" if gps_highlight else "#000000"
                            background = "#c0392b" if gps_highlight else "rgba(255, 255, 255, 77)"
                            weight = "bold" if gps_highlight else "normal"
                    else:
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
                    bottom_right_layout.addWidget(gps_label, 0, Qt.AlignRight)

                image_layout.addWidget(bottom_right, 0, 0, alignment=Qt.AlignBottom | Qt.AlignRight)

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
            delete_btn.clicked.connect(lambda _, key=delete_key: self.deleteImagesRequested.emit([key]))
            overlay_layout.addWidget(delete_btn)

        image_layout.addWidget(overlay, 0, 0, alignment=Qt.AlignTop | Qt.AlignRight)

        publish_checkbox = None
        if self._show_publish_checkbox:
            publish_checkbox = _PublishToggle()
            key = item.get("id") if item.get("id") is not None else item.get("filepath")
            checked = bool(self._resolve_publish_checked_state(item))
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
        frame.raw_halo_color = item.get("raw_halo_color")
        frame.thumb_label = thumb_label
        frame.publish_checkbox = publish_checkbox
        frame.cloud_badge = cloud_badge
        frame._thumbnail_index_text = str(image_num) if image_num is not None else None
        selection_overlay = QWidget(frame)
        selection_overlay.setAttribute(Qt.WA_TransparentForMouseEvents, True)
        selection_overlay.setAttribute(Qt.WA_NoSystemBackground, True)
        selection_overlay.setGeometry(frame.rect())
        selection_overlay.hide()

        def _paint_selection_overlay(_event, *, _frame=frame, _overlay=selection_overlay):
            selected = bool(getattr(_frame, "_thumbnail_selected", False))
            hovered = bool(getattr(_frame, "_thumbnail_hovered", False))
            raw_halo_color = getattr(_frame, "raw_halo_color", None)
            if not selected and not hovered and not raw_halo_color:
                return
            painter = QPainter(_overlay)
            paint_thumbnail_selection_overlay(
                painter,
                QRectF(_overlay.rect()),
                selected=selected,
                hovered=hovered,
                raw_halo_color=raw_halo_color,
                palette=_overlay.palette(),
                badge_text=getattr(_frame, "_thumbnail_index_text", None) if selected else None,
            )
            painter.end()

        selection_overlay.paintEvent = _paint_selection_overlay
        frame._thumbnail_selection_overlay = selection_overlay
        self._update_thumbnail_selection_overlay(frame)
        if self._thumbnail_tooltip:
            frame.setToolTip(self._thumbnail_tooltip)
        frame.mousePressEvent = lambda e, f=frame: self._on_frame_mouse_press(e, f)
        frame.mouseMoveEvent = lambda e, f=frame: self._on_frame_mouse_move(e, f)
        frame.mouseReleaseEvent = lambda e: setattr(self, "_drag_start_pos", None)
        frame.mouseDoubleClickEvent = lambda e, img_id=frame.image_id, path=frame.image_path: self.imageDoubleClicked.emit(img_id, path or "")
        frame.installEventFilter(self)
        # Wrap the frame in an outer container that provides the square
        # selection backdrop. When a thumbnail is selected the container's
        # background paints as a solid colored rectangle behind the frame,
        # with a few pixels of padding on all sides so the color shows.
        container = QWidget()
        container.setObjectName("thumbSelectionContainer")
        container.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        container_layout = QVBoxLayout(container)
        pad = _THUMB_SELECTION_BACKDROP_PADDING
        container_layout.setContentsMargins(pad, pad, pad, pad)
        container_layout.setSpacing(0)
        container_layout.addWidget(frame)
        container.setAttribute(Qt.WA_StyledBackground, True)
        container.setStyleSheet(
            "QWidget#thumbSelectionContainer { background-color: transparent; }"
        )
        container._thumbnail_frame = frame
        frame._outer_container = container
        return container

    def set_multi_select(self, enabled: bool) -> None:
        self._multi_select = bool(enabled)
        if not self._multi_select:
            self._selected_keys = set()
            if self._selected_id is not None:
                self._selected_keys.add(self._selected_id)
            self._apply_selection_styles()

    def set_toggle_selection_on_plain_click(self, enabled: bool) -> None:
        self._toggle_selection_on_plain_click = bool(enabled)

    def set_clear_selection_on_background_click(self, enabled: bool) -> None:
        self._clear_selection_on_background_click = bool(enabled)

    def is_multi_select(self) -> bool:
        return bool(self._multi_select)

    def selected_paths(self) -> list[str]:
        selected = []
        for item in self._items:
            key = item.get("id") if item.get("id") is not None else item.get("filepath")
            if key in self._selected_keys:
                filepath = item.get("filepath")
                if filepath:
                    selected.append(str(filepath))
        return selected

    def selected_keys(self) -> set[str | int]:
        return set(self._selected_keys)

    def selected_image_keys(self) -> list[str | int]:
        keys: list[str | int] = []
        for item in self._items:
            key = self._item_key(item)
            if key is not None and key in self._selected_keys:
                keys.append(key)
        return keys

    def center_on_key(self, key) -> None:
        self._queue_center_on_key(key)

    def select_paths(self, paths: list[str], center: bool = True) -> None:
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
        if first_selected_key is not None and center:
            self._queue_center_on_key(first_selected_key)

    def exit_edit_selection(self) -> None:
        """Clear the gallery selection entirely — no highlighted thumbnail(s)
        remain. Used when a fresh capture arrives; any lingering selection
        from a prior multi-select-for-edit is no longer relevant. Emits
        selectionChanged so listeners can update their hints, and bumps the
        center-request generation so any queued auto-scroll is cancelled."""
        if not self._selected_keys and self._selected_id is None:
            return
        self._selected_keys = set()
        self._selected_id = None
        self._last_clicked_index = None
        # Cancel any pending recenter — we don't want a stale center-request
        # to snap the viewport once selection changes.
        self._center_request_generation += 1
        self._center_request_key = None
        self._apply_selection_styles()
        self.selectionChanged.emit(self.selected_paths())

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
            self._set_frame_selected_state(frame, is_selected)

    def _frame_for_key(self, key) -> QFrame | None:
        if key is None:
            return None
        for frame in self._frames:
            if getattr(frame, "image_key", None) == key:
                return frame
        return None

    def _center_on_key(self, key) -> None:
        self._queue_center_on_key(key)

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
            # Record the press point in GLOBAL coordinates. Using event.position()
            # (frame-local) breaks whenever the click triggers an auto-scroll —
            # the frame slides under the stationary cursor, its local coord
            # jumps by the scroll delta, and the drag threshold trips even
            # though the physical mouse never moved.
            try:
                self._drag_start_pos = event.globalPosition().toPoint()
            except Exception:
                self._drag_start_pos = QPoint()
            self._drag_start_key = getattr(frame, "image_key", None)
            self._on_click(event, getattr(frame, "image_id", None), getattr(frame, "image_path", ""))
            return
        if event.button() == Qt.RightButton:
            try:
                global_pos = event.globalPosition().toPoint()
            except Exception:
                global_pos = None
            if global_pos is not None:
                self._show_thumbnail_context_menu(frame, global_pos)
            try:
                event.accept()
            except Exception:
                pass

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

        if self._multi_select:
            self._selected_id = target_id
            self._selected_keys = {target_key} if target_key is not None else set()
            self._last_clicked_index = target_index
            self._apply_selection_styles()
            if target_key is not None and target_key in self._selected_keys:
                self._queue_center_on_key(target_key)
        else:
            self.select_image(target_id)
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
        # Compare in GLOBAL coordinates — see _on_frame_mouse_press for why
        # frame-local positions can't be trusted across auto-scroll.
        try:
            current_pos = event.globalPosition().toPoint()
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
            try:
                drag.setHotSpot(event.position().toPoint())
            except Exception:
                pass
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
                if self._toggle_selection_on_plain_click and self._selected_keys == {key}:
                    self._selected_keys = set()
                else:
                    self._selected_keys = {key}
            self._selected_id = img_id if key in self._selected_keys else None
            if index is not None:
                self._last_clicked_index = index
            self._apply_selection_styles()
            if key is not None and key in self._selected_keys:
                self._queue_center_on_key(key)
            self.selectionChanged.emit(self.selected_paths())
        else:
            self.select_image(img_id)
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
        # Reserve room for the selection-backdrop padding above + below the
        # thumbnail — otherwise the bottom edge of the frame gets clipped
        # by the strip's fixed height.
        backdrop_padding = _THUMB_SELECTION_BACKDROP_PADDING * 2
        available_h = max(
            0,
            int(self._scroll.height()) - frame - scrollbar_h - 8 - backdrop_padding,
        )
        target = max(self._min_thumb_size, min(self._base_thumb_size, available_h))
        return target

    def _sync_container_height(self) -> None:
        if not hasattr(self, "_container") or self._container is None:
            return
        if self._frames or self._items:
            # The row's actual height is the thumbnail plus the backdrop
            # padding on both sides — otherwise the padded containers get
            # clipped from below.
            row_height = int(self._thumb_size) + 2 * _THUMB_SELECTION_BACKDROP_PADDING
        else:
            row_height = 0
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
            self._update_thumbnail_selection_overlay(frame)

    def _load_pixmap(self, item: dict) -> QPixmap | None:
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
                pixmap = load_oriented_pixmap(thumb_path, max_dim=max(256, self._decode_max_dim))
                _cache_put(thumb_path, pixmap, "thumb")
                return pixmap
        if filepath:
            filepath = str(filepath)
            variant = f"preview:{self._decode_max_dim}"
            cached = _cache_get(filepath, variant)
            if cached is not None:
                return cached
            pixmap = load_oriented_pixmap(filepath, max_dim=self._decode_max_dim)
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
