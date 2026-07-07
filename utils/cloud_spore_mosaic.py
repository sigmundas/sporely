"""Per-observation public spore mosaic / atlas generator.

Given a set of local spore measurements (p1..p4 endpoints in the source
image's pixel space, plus a path to the source microscope image), this
module composes a single WebP sprite atlas containing one tile per
measurement plus a manifest describing where each tile lives in the atlas
and the tile-local polygon of the measurement rectangle.

Common-crop model
-----------------
All tiles for one observation share a single visible size. The pipeline:

1. Plans each measurement (oriented rotation, corners, centre, and the
   natural padded crop) via `plan_spore_thumbnail` — no PIL work.
2. Picks a common crop size = max natural crop width across the
   observation × max natural crop height across the observation. That
   size is centred on each measurement, edge-shifted to stay inside the
   oriented source, and only padded with background when the source is
   genuinely smaller than the requested crop.
3. Rescales every crop to the same output tile size (height fixed to
   `tile_size_px`; width follows the common aspect ratio).
4. Composes tiles into a grid with cells equal to the output tile size,
   so there is no filler and every mosaic tile row exposes the same
   `w_px`/`h_px`.

Overlay JSON stores `{"polygon": [{x, y}, …], "style": "b"}` in the
final visible tile's local coordinate system. The old line overlay is
not emitted. Landing ignores unknown / missing overlays.

When p3/p4 are missing on a measurement we DO NOT synthesise a
rectangle — the tile still renders (oriented, cropped around p1/p2 with
padding), but `overlay_json` is `None`.
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
    SporeThumbnailPlan,
    plan_spore_thumbnail,
    render_spore_thumbnail_common_crop,
)


# ── Defaults ─────────────────────────────────────────────────────────────────

DEFAULT_TILE_SIZE_PX = 320
DEFAULT_WEBP_QUALITY = 82
DEFAULT_BACKGROUND_RGB: tuple[int, int, int] = (18, 18, 22)
CONTENT_DIGEST_HEX_CHARS = 16

# Bumped when the rendered bytes or tile manifest can change semantically
# (crop math, orient logic, tile grid, overlay payload schema, WebP quality,
# background colour, etc). The sync-time mosaic signature includes this
# constant so a version bump forces every observation to rebuild once and
# store its new signature — even if the local rows didn't change.
MOSAIC_PIPELINE_VERSION = 1

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
    tile_width_px: int = 0
    tile_height_px: int = 0
    common_crop_width_px: int = 0
    common_crop_height_px: int = 0
    common_crop_width_um: float = 0.0
    common_crop_height_um: float = 0.0
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


def compute_mosaic_grid_cells(
    tile_count: int,
    cell_width_px: int,
    cell_height_px: int,
) -> tuple[int, int, int, int]:
    """Grid math when atlas cells are non-square (common-crop model)."""
    if tile_count < 1:
        raise ValueError("tile_count must be >= 1")
    if cell_width_px < 1 or cell_height_px < 1:
        raise ValueError("cell dimensions must be positive")
    cols = max(1, math.ceil(math.sqrt(tile_count)))
    rows = max(1, math.ceil(tile_count / cols))
    return cols, rows, cols * cell_width_px, rows * cell_height_px


def place_tiles(tile_count: int, tile_size_px: int) -> list[tuple[int, int, int, int]]:
    """Return per-slot (x_px, y_px, w_px, h_px) rectangles in row-major order."""
    cols, _rows, _w, _h = compute_mosaic_grid(tile_count, tile_size_px)
    out: list[tuple[int, int, int, int]] = []
    for index in range(tile_count):
        row = index // cols
        col = index % cols
        out.append((col * tile_size_px, row * tile_size_px, tile_size_px, tile_size_px))
    return out


def place_tiles_cells(
    tile_count: int,
    cell_width_px: int,
    cell_height_px: int,
) -> list[tuple[int, int, int, int]]:
    """Row-major placement using rectangular atlas cells."""
    cols, _rows, _w, _h = compute_mosaic_grid_cells(tile_count, cell_width_px, cell_height_px)
    out: list[tuple[int, int, int, int]] = []
    for index in range(tile_count):
        row = index // cols
        col = index % cols
        out.append((
            col * cell_width_px, row * cell_height_px,
            cell_width_px, cell_height_px,
        ))
    return out


# ── Crop plan ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class MosaicCropPlan:
    """Plan describing the common visible tile geometry for an observation.

    Fields are named after the *physical* crop. `common_crop_width` /
    `common_crop_height` are kept in **pixels** for backward-compatible
    diagnostics, but the source of truth is `common_crop_width_um` /
    `common_crop_height_um`. Per-tile pixel dims are derived at render
    time from each measurement's own `px_per_um` (see
    `build_spore_mosaic`).
    """

    common_crop_width_um: float
    common_crop_height_um: float
    output_tile_width: int
    output_tile_height: int
    common_crop_width: int = 0   # legacy: representative pixel width
    common_crop_height: int = 0  # legacy: representative pixel height


def plan_common_crop(
    plans: Sequence[SporeThumbnailPlan],
    output_height_px: int,
) -> MosaicCropPlan | None:
    """Pick a common crop size from per-measurement natural crops.

    The plan is expressed in **micrometres**: the widest / tallest
    natural crop across all measurements, in µm, becomes the common
    physical crop for the observation. The visible tile height is
    fixed to `output_height_px`; the visible tile width follows the
    common physical aspect ratio. That guarantees a consistent
    µm-per-output-pixel mapping across every tile in the observation,
    even when the underlying microscope frames have different
    px-per-µm.

    Plans without a valid physical scale (missing length_um or a zero
    length axis) are silently ignored — the caller filters them.
    """
    if not plans:
        return None
    scaled = [
        p for p in plans
        if p.natural_crop_width_um and p.natural_crop_height_um
        and p.natural_crop_width_um > 0 and p.natural_crop_height_um > 0
    ]
    if not scaled:
        return None
    if output_height_px < 8:
        raise ValueError("output_height_px too small")

    common_w_um = max(p.natural_crop_width_um for p in scaled)
    common_h_um = max(p.natural_crop_height_um for p in scaled)
    out_h = int(output_height_px)
    out_w = max(1, int(round(common_w_um * out_h / common_h_um)))

    # Representative pixel dims (rounded off the widest measurement).
    # Kept only for backwards-compatible diagnostics; the per-tile
    # pixel crop is recomputed from each plan's own scale.
    rep_plan = max(scaled, key=lambda p: p.natural_crop_height_um)
    rep_w_px = max(1, int(math.ceil(common_w_um * (rep_plan.width_axis_px_per_um or 0))))
    rep_h_px = max(1, int(math.ceil(common_h_um * (rep_plan.length_axis_px_per_um or 0))))

    return MosaicCropPlan(
        common_crop_width_um=float(common_w_um),
        common_crop_height_um=float(common_h_um),
        output_tile_width=out_w,
        output_tile_height=out_h,
        common_crop_width=rep_w_px,
        common_crop_height=rep_h_px,
    )


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
    """Compose a common-crop WebP atlas + tile manifest.

    Every tile in the returned manifest has the SAME `w_px` / `h_px`,
    chosen from the widest and tallest natural padded crops across the
    input measurements. Landing therefore renders a uniform strip
    without extra padding logic.
    """
    if tile_size_px < 8:
        raise ValueError("tile_size_px too small")
    if not sources:
        return None

    ordered = list(sources)

    # ── Plan phase ──────────────────────────────────────────────────────
    plans: list[tuple[SporeCropSource, SporeThumbnailPlan]] = []
    plan_skipped: list[tuple[int, str]] = []
    for src in ordered:
        if src.source_width < 1 or src.source_height < 1:
            plan_skipped.append((src.measurement_id, "invalid source dims"))
            continue
        inputs = SporeThumbnailInputs(
            p1_x=src.p1_x, p1_y=src.p1_y,
            p2_x=src.p2_x, p2_y=src.p2_y,
            p3_x=src.p3_x, p3_y=src.p3_y,
            p4_x=src.p4_x, p4_y=src.p4_y,
            orient=True,
            extra_rotation_deg=float(src.gallery_rotation_deg or 0),
            background_rgb=background,
            length_um=src.length_um,
            width_um=src.width_um,
        )
        try:
            plan = plan_spore_thumbnail(inputs, src.source_width, src.source_height)
        except Exception as exc:  # pragma: no cover
            plan_skipped.append((src.measurement_id, f"plan failed: {exc}"))
            continue
        if (
            plan.length_axis_px_per_um is None
            or plan.width_axis_px_per_um is None
            or plan.natural_crop_width_um is None
            or plan.natural_crop_height_um is None
        ):
            # Physical scale could not be derived. Rather than render at
            # the wrong scale, skip the measurement and record why.
            plan_skipped.append((
                src.measurement_id,
                "missing physical scale (need length_um / p1p2)",
            ))
            continue
        plans.append((src, plan))

    if not plans:
        return None

    crop_plan = plan_common_crop([p for _s, p in plans], output_height_px=tile_size_px)
    if crop_plan is None:
        return None

    common_w_um = crop_plan.common_crop_width_um
    common_h_um = crop_plan.common_crop_height_um
    out_w = crop_plan.output_tile_width
    out_h = crop_plan.output_tile_height

    # ── Layout ──────────────────────────────────────────────────────────
    tile_count = len(plans)
    slot_rects = place_tiles_cells(tile_count, out_w, out_h)
    _cols, _rows, mosaic_w, mosaic_h = compute_mosaic_grid_cells(tile_count, out_w, out_h)
    canvas = Image.new("RGB", (mosaic_w, mosaic_h), background)

    tiles: list[SporeMosaicTile] = list()
    skipped: list[tuple[int, str]] = list(plan_skipped)
    open_cache: dict[Path, Image.Image] = {}

    try:
        for (src, plan), (slot_x, slot_y, slot_w, slot_h) in zip(plans, slot_rects):
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

            # Per-tile physical → oriented pixel crop, using this
            # measurement's own scale. Two measurements with the same
            # length_um / width_um but different px_per_um therefore
            # end up cropping different oriented pixel windows — which
            # is exactly what makes them display at the same physical
            # scale after the resize below.
            length_scale = plan.length_axis_px_per_um or 0.0
            width_scale = plan.width_axis_px_per_um or 0.0
            crop_w_px = max(1, int(round(common_w_um * width_scale)))
            crop_h_px = max(1, int(round(common_h_um * length_scale)))

            try:
                result = render_spore_thumbnail_common_crop(
                    img, plan,
                    common_crop_width=crop_w_px,
                    common_crop_height=crop_h_px,
                    output_width=out_w,
                    output_height=out_h,
                )
            except Exception as exc:  # pragma: no cover
                skipped.append((src.measurement_id, f"render failed: {exc}"))
                continue

            canvas.paste(result.image, (slot_x, slot_y))

            overlay = (
                build_overlay_polygon(result.polygon_tile_local, style=overlay_style)
                if result.polygon_tile_local is not None
                else None
            )

            polygon_bounds = None
            if result.polygon_tile_local is not None:
                xs = [p[0] for p in result.polygon_tile_local]
                ys = [p[1] for p in result.polygon_tile_local]
                polygon_bounds = (
                    round(min(xs), 2), round(min(ys), 2),
                    round(max(xs), 2), round(max(ys), 2),
                )

            diagnostics = {
                "measurement_id": src.measurement_id,
                "have_p1": src.p1_x is not None and src.p1_y is not None,
                "have_p2": src.p2_x is not None and src.p2_y is not None,
                "have_p3": src.p3_x is not None and src.p3_y is not None,
                "have_p4": src.p4_x is not None and src.p4_y is not None,
                "gallery_rotation_deg": src.gallery_rotation_deg,
                "rotation_deg": round(plan.rotation_deg, 3),
                "length_um": src.length_um,
                "width_um": src.width_um,
                "length_axis_px": round(plan.length_axis_px, 3),
                "width_axis_px": round(plan.width_axis_px, 3),
                "length_axis_px_per_um": (
                    round(plan.length_axis_px_per_um, 4)
                    if plan.length_axis_px_per_um is not None else None
                ),
                "width_axis_px_per_um": (
                    round(plan.width_axis_px_per_um, 4)
                    if plan.width_axis_px_per_um is not None else None
                ),
                "scale_fallback_reason": plan.scale_fallback_reason,
                "natural_crop_um": (
                    round(plan.natural_crop_width_um or 0.0, 3),
                    round(plan.natural_crop_height_um or 0.0, 3),
                ),
                "common_crop_um": (round(common_w_um, 3), round(common_h_um, 3)),
                "crop_px": (crop_w_px, crop_h_px),
                "output_tile": (out_w, out_h),
                "crop_rect_before_shift": tuple(round(v, 2) for v in result.crop_rect_before_shift),
                "crop_rect_after_shift": result.crop_rect_after_shift,
                "padded_x": result.padded_x,
                "padded_y": result.padded_y,
                "visible_rect_in_atlas": (slot_x, slot_y, out_w, out_h),
                "polygon_present": overlay is not None,
                "reason_no_polygon": result.reason_no_polygon,
                "polygon_bounds": polygon_bounds,
            }

            tiles.append(SporeMosaicTile(
                measurement_id=src.measurement_id,
                cloud_measurement_id=src.cloud_measurement_id,
                cloud_image_id=src.cloud_image_id,
                x_px=slot_x, y_px=slot_y, w_px=out_w, h_px=out_h,
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
        tile_width_px=out_w,
        tile_height_px=out_h,
        common_crop_width_px=crop_plan.common_crop_width,
        common_crop_height_px=crop_plan.common_crop_height,
        common_crop_width_um=common_w_um,
        common_crop_height_um=common_h_um,
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
