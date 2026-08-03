"""Desktop Analysis-tab gallery export adapter.

Everything the Qt "Export gallery" button needs, split out of
`ui/main_window.py::export_gallery_composite` so the main window keeps
just a thin stub. The adapter:

* Filters + sorts gallery measurements exactly like the live preview.
* Routes tile geometry through the shared
  `utils.spore_mosaic_render.plan_mosaic` planner so every export tile
  shares uniform physical scale (mandatory now — the ``uniform_scale``
  checkbox was removed).
* Rasterises PNG / JPEG tiles with the existing Qt renderer
  (`main_window.create_spore_thumbnail(..., plan=...)`) so the exported
  look matches the live preview.
* Writes hybrid SVGs: each tile is a base64-encoded PNG (Pillow,
  no Qt) inside an `<image>` element, and the measurement rectangle,
  the corner segments (style B) and the dimension label are emitted as
  editable vector `<polygon>` / `<line>` / `<text>` — using tile-local
  polygon coordinates from the same shared plan the raster path uses.

The SVG rectangle rendering follows the Qt code paths in
`main_window._draw_measurement_rectangle`:

* Style A (dual polygon): two stroked ``<polygon>`` elements — a wider
  "glow" underneath and a thin outline on top. No composition modes;
  the closest single-pass approximation is used.
* Style B (corner outline): a thin outline ``<polygon>`` plus a
  ``<line>`` per corner segment produced by
  ``rectangle_corner_segments``.

Dimension label uses the shared semantic anchor
(``MosaicTilePlan.label``): a wide white-stroke halo ``<text>``
underneath a coloured fill ``<text>`` on top, both with
``text-anchor="middle"``. Each backend positions the glyphs itself so
the plan carries no font metrics.
"""

from __future__ import annotations

import base64
import html
import io
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

from PIL import Image
from PySide6.QtCore import QPointF, Qt
from PySide6.QtGui import QColor, QImageReader, QPainter, QPixmap
from PySide6.QtWidgets import QDialog, QFileDialog

from utils.spore_mosaic_render import (
    MosaicAnnotationSpec,
    MosaicCell,
    MosaicGridPolicy,
    MosaicLayoutPlan,
    MosaicTilePlan,
    SporeMosaicSource,
    plan_mosaic,
)
from utils.spore_thumbnail_render import (
    SporeThumbnailInputs,
    plan_spore_thumbnail,
    render_spore_thumbnail_common_crop,
    resolve_common_crop_placement,
)
from database.models import ImageDB, ObservationDB
from .export_image_dialog import ExportGalleryDialog
from .measurement_overlay_style import (
    DEFAULT_RECTANGLE_STYLE,
    RECTANGLE_STYLE_A,
    RECTANGLE_STYLE_B,
    clamp_rectangle_thickness,
    clamp_stroke_width,
    normalize_rectangle_style,
    rectangle_corner_segments,
    rectangle_thin_stroke_width,
)


# ── Public entry point ─────────────────────────────────────────────────────


def run_export(main_window) -> None:
    """Open the export dialog and write the composite to disk.

    ``main_window`` is a `MainWindow` instance. The adapter reuses the
    filter/sort helpers, per-image measurement colour, per-observation
    rectangle style + thickness and last-export-dir behaviour that live
    on the main window; only the tile geometry pipeline changes.
    """
    # Preserve the same P3/P4 filter the pre-v3 export used. Desktop
    # export bakes a rectangle onto the tile, so it drops measurements
    # without a rectangle rather than synthesising one — the cloud path
    # tolerates missing P3/P4 because landing draws the rectangle.
    measurements = main_window.get_gallery_measurements()
    if not measurements:
        return
    valid_measurements = [
        m for m in measurements
        if all(
            m.get(f"p{i}_{axis}") is not None
            for i in range(1, 5)
            for axis in ("x", "y")
        )
    ]
    if not valid_measurements:
        return

    dialog = ExportGalleryDialog(parent=main_window)
    if dialog.exec() != QDialog.Accepted:
        return
    fmt_settings = dialog.get_settings()
    export_format = str(fmt_settings.get("format") or "png").lower()
    export_quality = int(fmt_settings.get("quality") or 90)

    filename = _prompt_save_path(main_window, export_format)
    if not filename:
        return
    main_window._remember_export_dir(filename)

    filtered = main_window._filter_gallery_measurements(valid_measurements)
    filtered = main_window._sort_gallery_measurements(filtered)
    if not filtered:
        return

    orient = bool(
        hasattr(main_window, "orient_checkbox") and main_window.orient_checkbox.isChecked()
    )
    thumbnail_size = int(main_window._gallery_thumbnail_size())
    rectangle_style = normalize_rectangle_style(
        main_window._current_measure_rectangle_style()
    )
    rectangle_thickness = clamp_rectangle_thickness(
        main_window._current_measure_rectangle_thickness()
    )
    default_color = QColor(getattr(main_window, "default_measure_color", QColor("#0044aa")))

    # ── Build shared planner input ─────────────────────────────────────
    image_cache = _ImageMetadataCache(main_window)
    sources: list[SporeMosaicSource] = []
    measurement_lookup: dict[int, dict] = {}
    for measurement in filtered:
        try:
            mid = int(measurement.get("id"))
        except (TypeError, ValueError):
            continue
        image_path = measurement.get("image_filepath")
        if not image_path:
            continue
        dims = image_cache.resolve_dims(str(image_path))
        if dims is None:
            continue
        scale_um_per_px = image_cache.resolve_scale(measurement.get("image_id"))
        length_um = _maybe_float(measurement.get("length_um"))
        width_um = _maybe_float(measurement.get("width_um"))
        gallery_rot = float(
            measurement.get("gallery_rotation")
            or main_window.gallery_rotations.get(mid, 0)
            or 0.0
        )
        sources.append(SporeMosaicSource(
            item_id=mid,
            source_path=Path(str(image_path)),
            source_width=dims[0],
            source_height=dims[1],
            p1_x=float(measurement["p1_x"]), p1_y=float(measurement["p1_y"]),
            p2_x=float(measurement["p2_x"]), p2_y=float(measurement["p2_y"]),
            p3_x=float(measurement["p3_x"]), p3_y=float(measurement["p3_y"]),
            p4_x=float(measurement["p4_x"]), p4_y=float(measurement["p4_y"]),
            length_um=length_um, width_um=width_um,
            scale_um_per_px=scale_um_per_px,
            extra_rotation_deg=gallery_rot,
        ))
        measurement_lookup[mid] = measurement

    if not sources:
        return

    layout = plan_mosaic(
        sources,
        orient=orient,
        grid_policy=MosaicGridPolicy.ASPECT_4_3,
        output_tile_height_px=thumbnail_size,
        annotation=MosaicAnnotationSpec(
            draw_rectangle=True,
            draw_dimensions=True,
            rectangle_style=rectangle_style,
            rectangle_thickness=rectangle_thickness,
            default_colour_rgb=(
                default_color.red(), default_color.green(), default_color.blue(),
            ),
        ),
    )
    if layout is None or not layout.cells:
        return

    if export_format == "svg":
        _write_hybrid_svg(
            layout=layout,
            measurement_lookup=measurement_lookup,
            image_cache=image_cache,
            path=Path(filename),
            rectangle_style=rectangle_style,
            rectangle_thickness=rectangle_thickness,
            default_color=default_color,
        )
    else:
        _write_qt_composite(
            main_window=main_window,
            layout=layout,
            measurement_lookup=measurement_lookup,
            image_cache=image_cache,
            path=Path(filename),
            export_format=export_format,
            export_quality=export_quality,
            rectangle_style=rectangle_style,
            rectangle_thickness=rectangle_thickness,
            default_color=default_color,
        )

    _publish_success(main_window, filename)


# ── Filename dialog + naming ───────────────────────────────────────────────


def _prompt_save_path(main_window, export_format: str) -> str | None:
    default_name = "spore_gallery"
    if getattr(main_window, "active_observation_id", None):
        obs = ObservationDB.get_observation(main_window.active_observation_id)
        if obs:
            parts = [
                obs.get("genus") or "",
                obs.get("species") or obs.get("species_guess") or "",
                obs.get("date") or "",
            ]
            name = " ".join(p for p in parts if p).strip()
            name = name.replace(":", "-")
            name = re.sub(r'[<>:"/\\\\|?*]', "_", name)
            name = re.sub(r"\\s+", " ", name).strip()
            if name:
                default_name = f"{name} - gallery"

    ext_map = {"png": ".png", "jpg": ".jpg", "svg": ".svg"}
    filter_map = {
        "png": "PNG Images (*.png)",
        "jpg": "JPEG Images (*.jpg)",
        "svg": "SVG Files (*.svg)",
    }
    default_ext = ext_map.get(export_format, ".png")
    default_path = str(
        Path(main_window._get_default_export_dir()) / f"{default_name}{default_ext}"
    )
    filename, _ = QFileDialog.getSaveFileName(
        main_window,
        "Export Gallery Composite",
        default_path,
        f"{filter_map.get(export_format, 'PNG Images (*.png)')};;All Files (*)",
    )
    return filename or None


def _publish_success(main_window, filename: str) -> None:
    status = getattr(main_window, "measure_status_label", None)
    if status is None:
        return
    try:
        status.setText(f"✓ Gallery exported to {Path(filename).name}")
        status.setStyleSheet("color: #27ae60; font-weight: bold;")
    except Exception:
        pass


# ── Small helpers ──────────────────────────────────────────────────────────


def _maybe_float(value) -> float | None:
    if value is None:
        return None
    try:
        value_f = float(value)
    except (TypeError, ValueError):
        return None
    return value_f if value_f > 0 else None


@dataclass
class _ImageMetadataCache:
    """One-shot ImageDB reads for the export session."""

    main_window: object
    dims: dict[str, tuple[int, int]] | None = None
    scales: dict[int, float | None] | None = None
    colors: dict[int, str | None] | None = None

    def __post_init__(self) -> None:
        self.dims = {}
        self.scales = {}
        self.colors = {}

    def resolve_dims(self, path: str) -> tuple[int, int] | None:
        if not path:
            return None
        if path in self.dims:
            return self.dims[path]
        reader = QImageReader(path)
        size = reader.size()
        dims: tuple[int, int] | None = None
        if size.isValid() and size.width() > 0 and size.height() > 0:
            dims = (int(size.width()), int(size.height()))
        self.dims[path] = dims
        return dims

    def resolve_scale(self, image_id) -> float | None:
        if image_id is None:
            return None
        try:
            key = int(image_id)
        except (TypeError, ValueError):
            return None
        if key in self.scales:
            return self.scales[key]
        image_data = ImageDB.get_image(key)
        value: float | None = None
        if image_data:
            raw = image_data.get("scale_microns_per_pixel")
            try:
                if raw is not None:
                    value = float(raw)
                    if value <= 0:
                        value = None
            except (TypeError, ValueError):
                value = None
        self.scales[key] = value
        return value

    def resolve_color(self, image_id) -> str | None:
        if image_id is None:
            return None
        try:
            key = int(image_id)
        except (TypeError, ValueError):
            return None
        if key in self.colors:
            return self.colors[key]
        image_data = ImageDB.get_image(key)
        stored = None
        if image_data:
            stored = image_data.get("measure_color")
        self.colors[key] = stored if stored else None
        return self.colors[key]


# ── PNG / JPEG composite (Qt-backed tiles) ─────────────────────────────────


def _write_qt_composite(
    *,
    main_window,
    layout: MosaicLayoutPlan,
    measurement_lookup: dict[int, dict],
    image_cache: _ImageMetadataCache,
    path: Path,
    export_format: str,
    export_quality: int,
    rectangle_style: str,
    rectangle_thickness: float,
    default_color: QColor,
) -> None:
    """Composite Qt-rendered tiles onto a shared canvas.

    Cells are uniform now (planner enforces it), so tiles are drawn at
    ``(cell.x_px, cell.y_px)`` with no centring padding.
    """
    pixmap_cache: dict[str, QPixmap] = {}
    composite = QPixmap(layout.mosaic_width_px, layout.mosaic_height_px)
    composite.fill(QColor(255, 255, 255))
    painter = QPainter(composite)
    try:
        for cell in layout.cells:
            tile_pixmap = _render_qt_tile_for_cell(
                main_window=main_window,
                cell=cell,
                measurement=measurement_lookup.get(cell.tile.source.item_id),
                pixmap_cache=pixmap_cache,
                image_cache=image_cache,
                rectangle_style=rectangle_style,
                rectangle_thickness=rectangle_thickness,
                default_color=default_color,
            )
            if tile_pixmap is None:
                continue
            painter.drawPixmap(cell.x_px, cell.y_px, tile_pixmap)
    finally:
        painter.end()

    if export_format == "jpg":
        composite.save(str(path), "JPEG", int(export_quality))
    else:
        composite.save(str(path))


def _render_qt_tile_for_cell(
    *,
    main_window,
    cell: MosaicCell,
    measurement: dict | None,
    pixmap_cache: dict[str, QPixmap],
    image_cache: _ImageMetadataCache,
    rectangle_style: str,
    rectangle_thickness: float,
    default_color: QColor,
) -> QPixmap | None:
    if measurement is None:
        return None
    image_path = measurement.get("image_filepath")
    if not image_path:
        return None
    pixmap = pixmap_cache.get(str(image_path))
    if pixmap is None:
        pixmap = QPixmap(str(image_path))
        pixmap_cache[str(image_path)] = pixmap
    if pixmap.isNull():
        return None
    color_hex = image_cache.resolve_color(measurement.get("image_id"))
    color = QColor(color_hex) if color_hex else QColor(default_color)
    points = [
        QPointF(measurement["p1_x"], measurement["p1_y"]),
        QPointF(measurement["p2_x"], measurement["p2_y"]),
        QPointF(measurement["p3_x"], measurement["p3_y"]),
        QPointF(measurement["p4_x"], measurement["p4_y"]),
    ]
    extra_rotation = float(
        measurement.get("gallery_rotation")
        or main_window.gallery_rotations.get(int(measurement.get("id") or 0), 0)
        or 0
    )
    tile = main_window.create_spore_thumbnail(
        pixmap,
        points,
        measurement.get("length_um") or 0,
        measurement.get("width_um") or 0,
        cell.tile.output_h_px,
        0,
        orient=abs(cell.tile.rotation_deg) > 0.1 or True,  # planner already oriented
        extra_rotation=extra_rotation,
        color=color,
        rectangle_style=rectangle_style,
        rectangle_thickness=rectangle_thickness,
        selected=False,
        export_mode=True,
        plan=cell.tile,
    )
    return tile


# ── Hybrid SVG (Pillow raster tiles + vector annotations) ──────────────────


def _write_hybrid_svg(
    *,
    layout: MosaicLayoutPlan,
    measurement_lookup: dict[int, dict],
    image_cache: _ImageMetadataCache,
    path: Path,
    rectangle_style: str,
    rectangle_thickness: float,
    default_color: QColor,
) -> None:
    """Write a hybrid SVG: raster tiles + editable vector annotations.

    Raster tiles come from Pillow (`render_spore_thumbnail_common_crop`).
    The measurement rectangle, corner segments, dimension label and any
    "selected" outline are emitted as native SVG elements so they stay
    editable in downstream vector tools.
    """
    tile_pngs = _rasterise_tiles_with_pillow(layout, measurement_lookup)

    fragments: list[str] = []
    fragments.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'xmlns:xlink="http://www.w3.org/1999/xlink" '
        f'width="{int(layout.mosaic_width_px)}" '
        f'height="{int(layout.mosaic_height_px)}" '
        f'viewBox="0 0 {int(layout.mosaic_width_px)} {int(layout.mosaic_height_px)}">'
    )
    # White background matches the Qt PNG path.
    fragments.append(
        f'<rect x="0" y="0" '
        f'width="{int(layout.mosaic_width_px)}" '
        f'height="{int(layout.mosaic_height_px)}" fill="white"/>'
    )

    for cell in layout.cells:
        png_bytes = tile_pngs.get(cell.tile.source.item_id)
        measurement = measurement_lookup.get(cell.tile.source.item_id)
        tile_w = cell.tile.output_w_px
        tile_h = cell.tile.output_h_px
        fragments.append(f'<g transform="translate({cell.x_px} {cell.y_px})">')
        if png_bytes is not None:
            b64 = base64.b64encode(png_bytes).decode("ascii")
            fragments.append(
                f'<image x="0" y="0" width="{int(tile_w)}" height="{int(tile_h)}" '
                f'xlink:href="data:image/png;base64,{b64}"/>'
            )
        polygon = cell.tile.oriented_polygon_tile_local
        if polygon is not None:
            stroke_color = _color_for_measurement(
                measurement=measurement,
                image_cache=image_cache,
                default_color=default_color,
            )
            fragments.extend(_svg_rectangle_fragments(
                polygon=polygon,
                style_name=rectangle_style,
                wide_width=float(rectangle_thickness),
                stroke_color=stroke_color,
            ))
        label = cell.tile.label
        if label is not None and label.get("text"):
            fragments.extend(_svg_label_fragments(
                text=str(label.get("text")),
                anchor=label.get("anchor"),
                tile_height=tile_h,
                stroke_color=_color_for_measurement(
                    measurement=measurement,
                    image_cache=image_cache,
                    default_color=default_color,
                ),
            ))
        fragments.append('</g>')

    fragments.append('</svg>')
    path.write_text("".join(fragments), encoding="utf-8")


def _rasterise_tiles_with_pillow(
    layout: MosaicLayoutPlan,
    measurement_lookup: dict[int, dict],
) -> dict[int, bytes]:
    """Produce one PNG (bytes) per cell using the shared Pillow renderer."""
    out: dict[int, bytes] = {}
    open_cache: dict[str, Image.Image] = {}
    try:
        for cell in layout.cells:
            item_id = cell.tile.source.item_id
            measurement = measurement_lookup.get(item_id)
            if measurement is None:
                continue
            image_path = measurement.get("image_filepath")
            if not image_path:
                continue
            img = open_cache.get(str(image_path))
            if img is None:
                try:
                    img = Image.open(image_path)
                except (FileNotFoundError, OSError):
                    continue
                open_cache[str(image_path)] = img
            try:
                result = render_spore_thumbnail_common_crop(
                    img, cell.tile.thumbnail_plan,
                    common_crop_width=cell.tile.common_crop_width_px,
                    common_crop_height=cell.tile.common_crop_height_px,
                    output_width=cell.tile.output_w_px,
                    output_height=cell.tile.output_h_px,
                )
            except Exception:  # pragma: no cover — defensive
                continue
            buf = io.BytesIO()
            result.image.save(buf, format="PNG")
            out[item_id] = buf.getvalue()
    finally:
        for img in open_cache.values():
            try:
                img.close()
            except Exception:  # pragma: no cover
                pass
    return out


def _color_for_measurement(
    *,
    measurement: dict | None,
    image_cache: _ImageMetadataCache,
    default_color: QColor,
) -> QColor:
    if measurement is None:
        return QColor(default_color)
    stored = image_cache.resolve_color(measurement.get("image_id"))
    if stored:
        col = QColor(stored)
        if col.isValid():
            return col
    return QColor(default_color)


# ── SVG vector helpers ─────────────────────────────────────────────────────


def _svg_rectangle_fragments(
    *,
    polygon: list[tuple[float, float]],
    style_name: str,
    wide_width: float,
    stroke_color: QColor,
) -> list[str]:
    """Emit vector fragments matching `_draw_measurement_rectangle`.

    Style A: two stroked ``<polygon>`` elements (a wide translucent one
    below and a thin one on top), closest single-pass approximation of
    the Qt dual-stroke look.

    Style B: a thin outline ``<polygon>`` plus one ``<line>`` per corner
    segment (same segments the Qt path renders).
    """
    resolved_style = normalize_rectangle_style(style_name)
    wide = clamp_stroke_width(wide_width)
    thin = rectangle_thin_stroke_width(resolved_style, wide_width)
    color_hex = stroke_color.name()

    points_attr = " ".join(f"{x:.2f},{y:.2f}" for x, y in polygon)

    if resolved_style == RECTANGLE_STYLE_A:
        # Dual-polygon: wide (semi-transparent) beneath, thin on top.
        # Qt uses a screen/overlay composition mode; the closest static
        # SVG approximation is a translucent wide stroke.
        return [
            f'<polygon points="{points_attr}" fill="none" '
            f'stroke="{color_hex}" stroke-width="{wide:.2f}" '
            f'stroke-opacity="0.55"/>',
            f'<polygon points="{points_attr}" fill="none" '
            f'stroke="{color_hex}" stroke-width="{thin:.2f}"/>',
        ]

    # Style B: corner-outline. Emit the thin outline, then draw each
    # corner segment as its own <line>.
    segments = _rectangle_corner_segments_from_tuples(polygon)
    frags = [
        f'<polygon points="{points_attr}" fill="none" '
        f'stroke="{color_hex}" stroke-width="{thin:.2f}"/>',
    ]
    for (x1, y1), (x2, y2) in segments:
        frags.append(
            f'<line x1="{x1:.2f}" y1="{y1:.2f}" x2="{x2:.2f}" y2="{y2:.2f}" '
            f'stroke="{color_hex}" stroke-width="{wide:.2f}" '
            f'stroke-linecap="round"/>'
        )
    return frags


def _rectangle_corner_segments_from_tuples(
    polygon: list[tuple[float, float]],
) -> list[tuple[tuple[float, float], tuple[float, float]]]:
    """Pure-Python mirror of `rectangle_corner_segments` for SVG.

    Kept alongside the Qt path so the SVG segments are byte-for-byte
    the same as what `_draw_measurement_rectangle` would render. We
    delegate to the Qt helper via QPointF so the segment math stays in
    one place.
    """
    qt_points = [QPointF(x, y) for x, y in polygon]
    return [
        ((seg[0].x(), seg[0].y()), (seg[1].x(), seg[1].y()))
        for seg in rectangle_corner_segments(qt_points)
    ]


def _svg_label_fragments(
    *,
    text: str,
    anchor,
    tile_height: int,
    stroke_color: QColor,
) -> list[str]:
    """Emit dimension label as halo + fill ``<text>`` at the plan anchor.

    Backends position glyphs themselves; here we use SVG's own
    ``text-anchor="middle"`` for horizontal centring. The vertical
    baseline comes from `MosaicTilePlan.label.anchor`, which the plan
    sets to `output_height - margin`.
    """
    if not text or anchor is None:
        return []
    try:
        cx, baseline_y = float(anchor[0]), float(anchor[1])
    except (TypeError, ValueError):
        return []
    font_size = max(8.0, float(tile_height) * 0.055)
    halo_width = max(1.0, font_size * 0.4)
    text_escaped = html.escape(text)
    color_hex = stroke_color.name()
    return [
        # Halo (wide white stroke, no fill).
        f'<text x="{cx:.2f}" y="{baseline_y:.2f}" '
        f'font-family="sans-serif" font-size="{font_size:.2f}" '
        f'text-anchor="middle" fill="none" '
        f'stroke="#ffffff" stroke-opacity="0.4" '
        f'stroke-width="{halo_width:.2f}" '
        f'stroke-linejoin="round" stroke-linecap="round">'
        f'{text_escaped}</text>',
        # Fill on top.
        f'<text x="{cx:.2f}" y="{baseline_y:.2f}" '
        f'font-family="sans-serif" font-size="{font_size:.2f}" '
        f'text-anchor="middle" fill="{color_hex}">'
        f'{text_escaped}</text>',
    ]
