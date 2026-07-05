"""Per-observation public spore mosaic / atlas generator.

Given a set of local spore measurements (with p1/p2 endpoints in the source
image's pixel space and a path to the source microscope image), this module
composes a single WebP "sprite sheet" containing one tile per measurement
plus a manifest describing each tile's rectangle in the mosaic and,
optionally, tile-local overlay geometry that lets the web frontend draw the
measurement line on top of the tile.

Design notes
------------
* Pure layout helpers (grid math, tile placement, crop rect selection,
  overlay coordinate transform) are separate from the PIL/WebP encoder so
  they can be tested without Pillow.
* `gallery_rotation` on `spore_measurements` is a DISPLAY hint (integer
  degrees, counter-clockwise around image center, applied by the Qt
  gallery when rendering the measurement thumbnail). The stored p1/p2
  values remain in source-image pixel space. See
  `ui/main_window.py::create_spore_thumbnail` for the on-screen path.
* For gallery_rotation != 0 the tile PIXELS are rotated to match the
  gallery view, but overlay coordinates are set to None. Mapping p1/p2
  through a matching rotation is straightforward, but the sign/pivot
  conventions differ subtly between Qt's Y-down QTransform and PIL's
  Image.rotate, and we would rather emit no overlay than a wrong one. The
  landing renderer already handles missing overlays gracefully.
"""

from __future__ import annotations

import hashlib
import io
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Sequence

from PIL import Image, ImageOps


# ── Defaults ─────────────────────────────────────────────────────────────────

DEFAULT_TILE_SIZE_PX = 160
DEFAULT_WEBP_QUALITY = 82
DEFAULT_BACKGROUND_RGB: tuple[int, int, int] = (18, 18, 22)
DEFAULT_PADDING_FRACTION = 0.6  # crop side = length * (1 + 2*padding) roughly
MIN_CROP_SIDE_PX = 80
CONTENT_DIGEST_HEX_CHARS = 16  # 64-bit prefix of sha256; ample per-observation


# ── Public data model ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SporeCropSource:
    """One measurement's worth of input to the mosaic builder.

    All coordinates are in the source microscope image's native pixel space.
    """

    measurement_id: int  # local desktop id (used for logging / tie-breaking)
    image_id: int         # local image id (used for logging)
    cloud_measurement_id: str  # Supabase spore_measurements.id (bigint as text)
    cloud_image_id: str        # Supabase observation_images.id
    source_path: Path
    source_width: int
    source_height: int
    p1_x: float
    p1_y: float
    p2_x: float
    p2_y: float
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
    """Return (cols, rows, width_px, height_px) for a near-square grid.

    Layout is a plain grid — the last row may have empty slots. Empty rows
    at the bottom are never allocated. `tile_count` must be >= 1 and
    `tile_size_px` must be positive.
    """
    if tile_count < 1:
        raise ValueError("tile_count must be >= 1")
    if tile_size_px < 1:
        raise ValueError("tile_size_px must be >= 1")
    cols = max(1, math.ceil(math.sqrt(tile_count)))
    rows = max(1, math.ceil(tile_count / cols))
    return cols, rows, cols * tile_size_px, rows * tile_size_px


def place_tiles(tile_count: int, tile_size_px: int) -> list[tuple[int, int, int, int]]:
    """Return per-tile (x_px, y_px, w_px, h_px) rectangles inside the mosaic.

    Row-major placement using `compute_mosaic_grid`.
    """
    cols, _rows, _w, _h = compute_mosaic_grid(tile_count, tile_size_px)
    out: list[tuple[int, int, int, int]] = []
    for index in range(tile_count):
        row = index // cols
        col = index % cols
        out.append((col * tile_size_px, row * tile_size_px, tile_size_px, tile_size_px))
    return out


def compute_crop_rect(
    p1_x: float,
    p1_y: float,
    p2_x: float,
    p2_y: float,
    source_width: int,
    source_height: int,
    tile_size_px: int,
    padding_fraction: float = DEFAULT_PADDING_FRACTION,
    min_side: int = MIN_CROP_SIDE_PX,
) -> tuple[int, int, int, int]:
    """Pick a square-ish crop around a measurement.

    Returns (x, y, w, h) in source-image pixel space. The crop is always
    square in intent but may be clipped near image edges (so w == h before
    clipping, and w/h may differ after clipping).

    The side length grows with the measurement length so both short and
    long spores get context. A hard lower bound (`min_side`) keeps very
    short measurements inspectable, and we never upscale beyond the source
    image bounds.
    """
    if source_width < 1 or source_height < 1:
        raise ValueError("source_width and source_height must be positive")
    length = math.hypot(p2_x - p1_x, p2_y - p1_y)
    # side ≈ length * (1 + 2*padding). For zero-length measurements we still
    # emit a minimum-size crop so the tile isn't empty.
    padded_side = length * (1.0 + 2.0 * max(0.0, padding_fraction))
    side = int(round(max(padded_side, float(min_side), float(tile_size_px) * 0.6)))
    side = max(min_side, min(side, source_width, source_height))
    cx = (p1_x + p2_x) * 0.5
    cy = (p1_y + p2_y) * 0.5
    half = side / 2.0
    x = int(round(cx - half))
    y = int(round(cy - half))
    # Shift so the crop stays inside the image if the midpoint sits near an edge.
    if x < 0:
        x = 0
    if y < 0:
        y = 0
    if x + side > source_width:
        x = max(0, source_width - side)
    if y + side > source_height:
        y = max(0, source_height - side)
    w = min(side, source_width - x)
    h = min(side, source_height - y)
    return x, y, w, h


def line_to_tile_local(
    p1_x: float,
    p1_y: float,
    p2_x: float,
    p2_y: float,
    crop_x: int,
    crop_y: int,
    crop_w: int,
    crop_h: int,
    tile_size_px: int,
) -> tuple[float, float, float, float] | None:
    """Map (p1, p2) from source-image pixels into tile-local pixels.

    The tile is `tile_size_px` × `tile_size_px`. Points that end up outside
    the tile after mapping are still returned (the SVG can clip them); this
    is not the place to filter them.

    Returns None when the crop rectangle is degenerate.
    """
    if crop_w < 1 or crop_h < 1 or tile_size_px < 1:
        return None
    sx = tile_size_px / float(crop_w)
    sy = tile_size_px / float(crop_h)
    x1 = (p1_x - crop_x) * sx
    y1 = (p1_y - crop_y) * sy
    x2 = (p2_x - crop_x) * sx
    y2 = (p2_y - crop_y) * sy
    return x1, y1, x2, y2


def build_overlay_line(
    p1_x: float,
    p1_y: float,
    p2_x: float,
    p2_y: float,
    crop_x: int,
    crop_y: int,
    crop_w: int,
    crop_h: int,
    tile_size_px: int,
    gallery_rotation_deg: int,
) -> dict | None:
    """Build the overlay_json shape stored in `spore_measurement_mosaic_tiles`.

    Returns None when the overlay cannot be emitted reliably. Currently that
    is any measurement whose gallery_rotation is non-zero — those cases are
    still cropped and pasted (with the pixels rotated to match the gallery
    view), but the line overlay is omitted to avoid rendering a wrong line
    on the web. Zero-rotation measurements get a `{'line': {...}}` dict in
    tile-local coordinates.
    """
    if gallery_rotation_deg % 360 != 0:
        return None
    mapped = line_to_tile_local(
        p1_x, p1_y, p2_x, p2_y,
        crop_x, crop_y, crop_w, crop_h, tile_size_px,
    )
    if mapped is None:
        return None
    x1, y1, x2, y2 = mapped
    return {
        "line": {
            "x1": round(x1, 2),
            "y1": round(y1, 2),
            "x2": round(x2, 2),
            "y2": round(y2, 2),
        }
    }


# ── PIL builder ──────────────────────────────────────────────────────────────


def _open_source_image(path: Path) -> Image.Image:
    """Open a source image and normalise orientation to source-pixel space.

    NOTE: we do NOT run ImageOps.exif_transpose() here because p1/p2 are
    stored against the raw pixel grid as the desktop app saw it — applying
    EXIF orientation would shift the coordinate system out from under the
    stored measurement. Callers who need to display sync'd images
    consistently do that transpose elsewhere.
    """
    return Image.open(path)


def _prepare_tile_pixels(
    src: Image.Image,
    crop: tuple[int, int, int, int],
    tile_size_px: int,
    gallery_rotation_deg: int,
    background: tuple[int, int, int],
) -> Image.Image:
    """Crop, rotate (if needed) and fit into a square tile."""
    crop_x, crop_y, crop_w, crop_h = crop
    tile = src.crop((crop_x, crop_y, crop_x + crop_w, crop_y + crop_h))
    if tile.mode not in ("RGB", "RGBA"):
        tile = tile.convert("RGB")
    elif tile.mode == "RGBA":
        flat = Image.new("RGB", tile.size, background)
        flat.paste(tile, mask=tile.split()[3])
        tile = flat

    rotation = gallery_rotation_deg % 360
    if rotation:
        # PIL Image.rotate is CCW; align direction with Qt's QTransform.rotate
        # in the gallery renderer (which operates in Y-down screen space).
        tile = tile.rotate(-rotation, resample=Image.BILINEAR, expand=True, fillcolor=background)

    # Fit into tile_size × tile_size preserving aspect. Do NOT upscale past
    # the source resolution — that would just blur a tiny crop.
    tw, th = tile.size
    scale = min(tile_size_px / tw, tile_size_px / th, 1.0)
    fitted_w = max(1, int(round(tw * scale)))
    fitted_h = max(1, int(round(th * scale)))
    if (fitted_w, fitted_h) != (tw, th):
        tile = tile.resize((fitted_w, fitted_h), Image.LANCZOS)

    canvas = Image.new("RGB", (tile_size_px, tile_size_px), background)
    off_x = (tile_size_px - fitted_w) // 2
    off_y = (tile_size_px - fitted_h) // 2
    canvas.paste(tile, (off_x, off_y))
    return canvas


def build_spore_mosaic(
    sources: Sequence[SporeCropSource],
    *,
    tile_size_px: int = DEFAULT_TILE_SIZE_PX,
    quality: int = DEFAULT_WEBP_QUALITY,
    background: tuple[int, int, int] = DEFAULT_BACKGROUND_RGB,
) -> SporeMosaicManifest | None:
    """Compose a WebP mosaic for the given measurement sources.

    Sources whose source image cannot be opened are skipped and recorded in
    `manifest.skipped`. If nothing renders the function returns None so the
    caller can no-op without uploading an empty mosaic.
    """
    if tile_size_px < 8:
        raise ValueError("tile_size_px too small")
    if not sources:
        return None

    ordered = list(sources)
    tile_rects = place_tiles(len(ordered), tile_size_px)
    cols, rows, mosaic_w, mosaic_h = compute_mosaic_grid(len(ordered), tile_size_px)
    canvas = Image.new("RGB", (mosaic_w, mosaic_h), background)

    tiles: list[SporeMosaicTile] = []
    skipped: list[tuple[int, str]] = []

    # Cache open source images so multiple measurements on the same image
    # only pay the decode cost once.
    open_cache: dict[Path, Image.Image] = {}
    try:
        for src, (tx, ty, tw, th) in zip(ordered, tile_rects):
            try:
                img = open_cache.get(src.source_path)
                if img is None:
                    img = _open_source_image(src.source_path)
                    open_cache[src.source_path] = img
            except FileNotFoundError:
                skipped.append((src.measurement_id, "source image missing"))
                continue
            except Exception as exc:  # pragma: no cover - PIL surface varies
                skipped.append((src.measurement_id, f"open failed: {exc}"))
                continue

            crop = compute_crop_rect(
                src.p1_x, src.p1_y, src.p2_x, src.p2_y,
                src.source_width, src.source_height, tile_size_px,
            )
            try:
                tile_img = _prepare_tile_pixels(
                    img, crop, tile_size_px, src.gallery_rotation_deg, background,
                )
            except Exception as exc:  # pragma: no cover
                skipped.append((src.measurement_id, f"tile prep failed: {exc}"))
                continue
            canvas.paste(tile_img, (tx, ty))

            overlay = build_overlay_line(
                src.p1_x, src.p1_y, src.p2_x, src.p2_y,
                *crop, tile_size_px, src.gallery_rotation_deg,
            )
            tiles.append(SporeMosaicTile(
                measurement_id=src.measurement_id,
                cloud_measurement_id=src.cloud_measurement_id,
                cloud_image_id=src.cloud_image_id,
                x_px=tx, y_px=ty, w_px=tw, h_px=th,
                overlay_json=overlay,
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

    # Grid stats are useful in a compact log line but not needed on the manifest.
    del cols, rows
    return SporeMosaicManifest(
        image_bytes=buf.getvalue(),
        content_type="image/webp",
        width_px=mosaic_w,
        height_px=mosaic_h,
        tile_size_px=tile_size_px,
        tiles=tiles,
        skipped=skipped,
    )


# ── Convenience: coerce local rows into SporeCropSource ──────────────────────


def sources_from_measurement_rows(
    rows: Iterable[dict],
    *,
    image_dir: Path,
    dims_resolver=None,
) -> tuple[list[SporeCropSource], list[tuple[int, str]]]:
    """Turn `_push_measurements_for_observation`-style rows into sources.

    Each row must carry: `id`, `image_id`, `cloud_id`, `image_cloud_id`,
    `image_filepath`, `p1_x`, `p1_y`, `p2_x`, `p2_y`, `gallery_rotation`.
    Image dimensions are looked up via `dims_resolver(path) -> (w, h)`, with
    a PIL-backed default when no resolver is passed. Rows whose image can't
    be resolved are dropped (and recorded in the returned skipped list) so
    the caller can log a single summary.

    Returns (sources, skipped) where `skipped` is a list of
    `(measurement_id, reason)` pairs. Never raises.
    """
    def _default_resolver(path: Path) -> tuple[int, int]:
        with Image.open(path) as img:
            return int(img.width), int(img.height)

    resolver = dims_resolver or _default_resolver
    out: list[SporeCropSource] = []
    skipped: list[tuple[int, str]] = []
    dims_cache: dict[Path, tuple[int, int]] = {}
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
            gallery_rotation_deg=int(row.get("gallery_rotation") or 0),
        ))
    return out, skipped


def compute_content_digest(image_bytes: bytes, length: int = CONTENT_DIGEST_HEX_CHARS) -> str:
    """Deterministic short hex digest of the mosaic WebP bytes.

    The prefix of sha256 is used as a content fingerprint so the storage key
    (and therefore the public URL) changes whenever the mosaic bytes change.
    That is what makes `Cache-Control: public, max-age=31536000, immutable`
    safe: browsers/CDNs never see two different mosaics under the same URL.
    """
    if length < 4 or length > 64:
        raise ValueError("digest length must be between 4 and 64 hex chars")
    return hashlib.sha256(image_bytes).hexdigest()[:length]


def build_storage_key(
    user_id: str,
    obs_cloud_id: str,
    version: int,
    digest: str,
) -> str:
    """Storage key for a mosaic upload. Matches the existing per-user prefix.

    The public web serves this via `https://media.sporely.no/{key}`. The
    `digest` argument must be a short content hash from
    `compute_content_digest(image_bytes)` so the URL changes on every byte
    change — this is what makes `Cache-Control: immutable` safe on the
    uploaded object. `version` remains 1 in the DB row; the digest is the
    knob that busts the cache.
    """
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
