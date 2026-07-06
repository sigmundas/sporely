"""Per-observation public spore mosaic / atlas generator.

Given a set of local spore measurements (p1..p4 endpoints in the source
image's pixel space, plus a path to the source microscope image), this
module composes a single WebP sprite atlas containing one tile per
measurement plus a manifest describing where each tile lives in the atlas
and the tile-local polygon of the measurement rectangle.

The actual orient/crop/polygon math is delegated to
`utils.spore_thumbnail_render.render_spore_thumbnail`, which is a
line-for-line PIL port of `main_window.create_spore_thumbnail`. Both
paths therefore produce the same rectangle for the same measurement.

Design notes
------------
* Pure layout helpers (grid math, atlas placement, content-digest, storage
  key) stay in this file and can be tested without Pillow.
* Each atlas slot is `tile_size_px × tile_size_px` (default 320). The
  rendered tile may be narrower than the slot (desktop tiles are
  height-fixed with variable width); we paste it centred and adjust the
  polygon coords so they land on the pasted pixels.
* Overlay JSON stores `{"polygon": [{x, y}, …], "style": "b"}`. The old
  line overlay is not emitted. Landing ignores unknown / missing overlays.
* When p3/p4 are missing on a measurement we DO NOT synthesise a
  rectangle from the µm ratio — the tile still renders (oriented, cropped
  around p1/p2 with padding), but `overlay_json` is `None` and the
  landing site falls back to a bare tile.
"""

from __future__ import annotations

import hashlib
import io
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Sequence

from PIL import Image

from utils.spore_thumbnail_render import (
    SporeThumbnailInputs,
    render_spore_thumbnail,
)


# ── Defaults ─────────────────────────────────────────────────────────────────

DEFAULT_TILE_SIZE_PX = 320
DEFAULT_WEBP_QUALITY = 82
DEFAULT_BACKGROUND_RGB: tuple[int, int, int] = (18, 18, 22)
CONTENT_DIGEST_HEX_CHARS = 16

RECTANGLE_STYLE_A = "a"
RECTANGLE_STYLE_B = "b"
DEFAULT_RECTANGLE_STYLE = RECTANGLE_STYLE_B


# ── Public data model ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SporeCropSource:
    """One measurement's worth of input to the mosaic builder.

    All coordinates are in the source microscope image's native pixel
    space. p3/p4 are the width-axis endpoints; when either is None we
    still emit an oriented tile but no polygon overlay.
    """

    measurement_id: int
    image_id: int
    cloud_measurement_id: str
    cloud_image_id: str
    source_path: Path
    source_width: int
    source_height: int
    p1_x: float
    p1_y: float
    p2_x: float
    p2_y: float
    p3_x: float | None = None
    p3_y: float | None = None
    p4_x: float | None = None
    p4_y: float | None = None
    length_um: float | None = None
    width_um: float | None = None
    gallery_rotation_deg: int = 0


@dataclass(frozen=True)
class SporeMosaicTile:
    """Where a single measurement's tile sits in the composed mosaic."""

    measurement_id: int
    cloud_measurement_id: str
    cloud_image_id: str
    x_px: int
    y_px: int
    w_px: int
    h_px: int
    overlay_json: dict | None
    diagnostics: dict = field(default_factory=dict)


@dataclass
class SporeMosaicManifest:
    """Fully-built mosaic ready to upload."""

    image_bytes: bytes
    content_type: str
    width_px: int
    height_px: int
    tile_size_px: int
    tiles: list[SporeMosaicTile] = field(default_factory=list)
    skipped: list[tuple[int, str]] = field(default_factory=list)


# ── Pure layout helpers ──────────────────────────────────────────────────────


def compute_mosaic_grid(tile_count: int, tile_size_px: int) -> tuple[int, int, int, int]:
    """Return (cols, rows, width_px, height_px) for a near-square grid."""
    if tile_count < 1:
        raise ValueError("tile_count must be >= 1")
    if tile_size_px < 1:
        raise ValueError("tile_size_px must be >= 1")
    cols = max(1, math.ceil(math.sqrt(tile_count)))
    rows = max(1, math.ceil(tile_count / cols))
    return cols, rows, cols * tile_size_px, rows * tile_size_px


def place_tiles(tile_count: int, tile_size_px: int) -> list[tuple[int, int, int, int]]:
    """Return per-slot (x_px, y_px, w_px, h_px) rectangles in row-major order."""
    cols, _rows, _w, _h = compute_mosaic_grid(tile_count, tile_size_px)
    out: list[tuple[int, int, int, int]] = []
    for index in range(tile_count):
        row = index // cols
        col = index % cols
        out.append((col * tile_size_px, row * tile_size_px, tile_size_px, tile_size_px))
    return out


# ── Storage key ─────────────────────────────────────────────────────────────


def compute_content_digest(image_bytes: bytes, length: int = CONTENT_DIGEST_HEX_CHARS) -> str:
    """Short hex prefix of sha256 — used to content-address the storage key."""
    if length < 4 or length > 64:
        raise ValueError("digest length must be between 4 and 64 hex chars")
    return hashlib.sha256(image_bytes).hexdigest()[:length]


def build_storage_key(user_id: str, obs_cloud_id: str, version: int, digest: str) -> str:
    """`{user}/{obs}/spore_mosaic_v{version}_{digest}.webp`."""
    if not user_id or not obs_cloud_id:
        raise ValueError("user_id and obs_cloud_id required")
    clean_digest = str(digest or "").strip().lower()
    if not clean_digest:
        raise ValueError("digest required")
    if not all(c in "0123456789abcdef" for c in clean_digest):
        raise ValueError("digest must be lower-case hex")
    return (
        f"{str(user_id).strip()}/{str(obs_cloud_id).strip()}"
        f"/spore_mosaic_v{int(version)}_{clean_digest}.webp"
    )


# ── Overlay payload ─────────────────────────────────────────────────────────


def build_overlay_polygon(
    corners_slot_local: Sequence[tuple[float, float]] | None,
    *,
    style: str = DEFAULT_RECTANGLE_STYLE,
) -> dict | None:
    if not corners_slot_local or len(corners_slot_local) < 3:
        return None
    return {
        "polygon": [
            {"x": round(float(x), 2), "y": round(float(y), 2)}
            for x, y in corners_slot_local
        ],
        "style": (
            RECTANGLE_STYLE_B
            if str(style or "").strip().lower() == RECTANGLE_STYLE_B
            else RECTANGLE_STYLE_A
        ),
    }


# ── PIL builder ─────────────────────────────────────────────────────────────


def _open_source_image(path: Path) -> Image.Image:
    return Image.open(path)


def build_spore_mosaic(
    sources: Sequence[SporeCropSource],
    *,
    tile_size_px: int = DEFAULT_TILE_SIZE_PX,
    quality: int = DEFAULT_WEBP_QUALITY,
    background: tuple[int, int, int] = DEFAULT_BACKGROUND_RGB,
    overlay_style: str = DEFAULT_RECTANGLE_STYLE,
) -> SporeMosaicManifest | None:
    """Compose a WebP atlas + tile manifest for the given measurements.

    Each source is rendered through the desktop-parity renderer. The
    resulting non-square tile is pasted centred into a `tile_size_px`
    square slot; the polygon coordinates are shifted by the paste offset
    so they land on the pasted pixels.
    """
    if tile_size_px < 8:
        raise ValueError("tile_size_px too small")
    if not sources:
        return None

    ordered = list(sources)
    slot_rects = place_tiles(len(ordered), tile_size_px)
    _cols, _rows, mosaic_w, mosaic_h = compute_mosaic_grid(len(ordered), tile_size_px)
    canvas = Image.new("RGB", (mosaic_w, mosaic_h), background)

    tiles: list[SporeMosaicTile] = []
    skipped: list[tuple[int, str]] = []
    open_cache: dict[Path, Image.Image] = {}

    try:
        for src, (slot_x, slot_y, slot_w, slot_h) in zip(ordered, slot_rects):
            try:
                img = open_cache.get(src.source_path)
                if img is None:
                    img = _open_source_image(src.source_path)
                    open_cache[src.source_path] = img
            except FileNotFoundError:
                skipped.append((src.measurement_id, "source image missing"))
                continue
            except Exception as exc:  # pragma: no cover
                skipped.append((src.measurement_id, f"open failed: {exc}"))
                continue

            inputs = SporeThumbnailInputs(
                p1_x=src.p1_x, p1_y=src.p1_y,
                p2_x=src.p2_x, p2_y=src.p2_y,
                p3_x=src.p3_x, p3_y=src.p3_y,
                p4_x=src.p4_x, p4_y=src.p4_y,
                orient=True,
                extra_rotation_deg=float(src.gallery_rotation_deg or 0),
                background_rgb=background,
            )
            try:
                result = render_spore_thumbnail(img, inputs, height_px=tile_size_px)
            except Exception as exc:  # pragma: no cover
                skipped.append((src.measurement_id, f"render failed: {exc}"))
                continue

            # Scale down further if the rendered width exceeds a slot (e.g.
            # unusually wide-aspect crop). Rare in practice because oriented
            # spores are taller-than-wide, but a safety net.
            fitted_img = result.image
            fitted_w = result.tile_width_px
            fitted_h = result.tile_height_px
            if fitted_w > slot_w or fitted_h > slot_h:
                downscale = min(slot_w / fitted_w, slot_h / fitted_h)
                fitted_w = max(1, int(round(fitted_w * downscale)))
                fitted_h = max(1, int(round(fitted_h * downscale)))
                fitted_img = fitted_img.resize((fitted_w, fitted_h), Image.LANCZOS)
                polygon_local: list[tuple[float, float]] | None = (
                    [(x * downscale, y * downscale) for x, y in result.polygon_tile_local]
                    if result.polygon_tile_local is not None
                    else None
                )
            else:
                polygon_local = (
                    list(result.polygon_tile_local)
                    if result.polygon_tile_local is not None
                    else None
                )

            # Centre the (possibly non-square) tile in its square atlas slot.
            paste_off_x = (slot_w - fitted_w) // 2
            paste_off_y = (slot_h - fitted_h) // 2
            canvas.paste(fitted_img, (slot_x + paste_off_x, slot_y + paste_off_y))

            # The public tile row exposes the VISIBLE sub-rect of the atlas,
            # not the whole square slot — otherwise the landing frontend
            # would show black side bands wherever the rendered tile was
            # narrower than the slot. Overlay coordinates therefore stay
            # in the visible tile's local frame (0..fitted_w × 0..fitted_h)
            # and never need to be shifted by the paste offset.
            visible_x = slot_x + paste_off_x
            visible_y = slot_y + paste_off_y
            overlay = (
                build_overlay_polygon(polygon_local, style=overlay_style)
                if polygon_local is not None
                else None
            )

            diagnostics = {
                "measurement_id": src.measurement_id,
                "have_p1": src.p1_x is not None and src.p1_y is not None,
                "have_p2": src.p2_x is not None and src.p2_y is not None,
                "have_p3": src.p3_x is not None and src.p3_y is not None,
                "have_p4": src.p4_x is not None and src.p4_y is not None,
                "gallery_rotation_deg": src.gallery_rotation_deg,
                "rotation_deg": round(result.rotation_deg, 3),
                "crop_rect_source_pixels": result.crop_rect_source_pixels,
                "tile_size_after_render": (
                    result.tile_width_px, result.tile_height_px,
                ),
                "tile_size_after_fit": (fitted_w, fitted_h),
                "paste_offset": (paste_off_x, paste_off_y),
                "visible_rect_in_atlas": (visible_x, visible_y, fitted_w, fitted_h),
                "polygon_present": overlay is not None,
                "reason_no_polygon": result.reason_no_polygon,
                "polygon_bounds": (
                    (
                        round(min(p[0] for p in polygon_local), 2),
                        round(min(p[1] for p in polygon_local), 2),
                        round(max(p[0] for p in polygon_local), 2),
                        round(max(p[1] for p in polygon_local), 2),
                    )
                    if polygon_local
                    else None
                ),
            }

            tiles.append(SporeMosaicTile(
                measurement_id=src.measurement_id,
                cloud_measurement_id=src.cloud_measurement_id,
                cloud_image_id=src.cloud_image_id,
                x_px=visible_x, y_px=visible_y, w_px=fitted_w, h_px=fitted_h,
                overlay_json=overlay,
                diagnostics=diagnostics,
            ))
    finally:
        for img in open_cache.values():
            try:
                img.close()
            except Exception:
                pass

    if not tiles:
        return None

    buf = io.BytesIO()
    canvas.save(buf, format="WEBP", quality=quality, method=4)
    canvas.close()

    return SporeMosaicManifest(
        image_bytes=buf.getvalue(),
        content_type="image/webp",
        width_px=mosaic_w,
        height_px=mosaic_h,
        tile_size_px=tile_size_px,
        tiles=tiles,
        skipped=skipped,
    )


# ── Convenience: coerce local rows into SporeCropSource ─────────────────────


def sources_from_measurement_rows(
    rows: Iterable[dict],
    *,
    image_dir: Path,
    dims_resolver=None,
) -> tuple[list[SporeCropSource], list[tuple[int, str]]]:
    """Turn measurement-row dicts (as fetched by `cloud_sync`) into sources.

    Rows must carry: `id`, `image_id`, `cloud_id`, `image_cloud_id`,
    `image_filepath`, `p1_x`, `p1_y`, `p2_x`, `p2_y`, `gallery_rotation`.
    Optional: `p3_x`, `p3_y`, `p4_x`, `p4_y`, `length_um`, `width_um`.
    """
    def _default_resolver(path: Path) -> tuple[int, int]:
        with Image.open(path) as img:
            return int(img.width), int(img.height)

    resolver = dims_resolver or _default_resolver
    out: list[SporeCropSource] = []
    skipped: list[tuple[int, str]] = []
    dims_cache: dict[Path, tuple[int, int]] = {}

    def _maybe_float(v) -> float | None:
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    for row in rows:
        mid = int(row.get("id") or 0)
        cloud_meas_id = str(row.get("cloud_id") or "").strip()
        cloud_image_id = str(row.get("image_cloud_id") or "").strip()
        if not cloud_meas_id or not cloud_image_id:
            skipped.append((mid, "missing cloud id"))
            continue
        raw_path = str(row.get("image_filepath") or "").strip()
        if not raw_path:
            skipped.append((mid, "missing image_filepath"))
            continue
        path = Path(raw_path)
        if not path.is_absolute():
            path = image_dir / path
        try:
            p1x = float(row.get("p1_x"))
            p1y = float(row.get("p1_y"))
            p2x = float(row.get("p2_x"))
            p2y = float(row.get("p2_y"))
        except (TypeError, ValueError):
            skipped.append((mid, "invalid p1/p2"))
            continue
        dims = dims_cache.get(path)
        if dims is None:
            try:
                dims = resolver(path)
            except FileNotFoundError:
                skipped.append((mid, "source image missing"))
                continue
            except Exception as exc:  # pragma: no cover
                skipped.append((mid, f"open failed: {exc}"))
                continue
            dims_cache[path] = dims
        src_w, src_h = dims
        if src_w < 1 or src_h < 1:
            skipped.append((mid, "invalid image dims"))
            continue
        out.append(SporeCropSource(
            measurement_id=mid,
            image_id=int(row.get("image_id") or 0),
            cloud_measurement_id=cloud_meas_id,
            cloud_image_id=cloud_image_id,
            source_path=path,
            source_width=src_w,
            source_height=src_h,
            p1_x=p1x, p1_y=p1y, p2_x=p2x, p2_y=p2y,
            p3_x=_maybe_float(row.get("p3_x")),
            p3_y=_maybe_float(row.get("p3_y")),
            p4_x=_maybe_float(row.get("p4_x")),
            p4_y=_maybe_float(row.get("p4_y")),
            length_um=_maybe_float(row.get("length_um")),
            width_um=_maybe_float(row.get("width_um")),
            gallery_rotation_deg=int(row.get("gallery_rotation") or 0),
        ))
    return out, skipped
