"""Per-observation public spore mosaic / atlas generator.

Given a set of local spore measurements (p1..p4 endpoints in the source
image's pixel space, plus a path to the source microscope image), this
module composes a single WebP sprite atlas containing one tile per
measurement plus a manifest describing where each tile lives in the atlas
and the tile-local polygon of the measurement rectangle.

Cloud backend
-------------
The cloud path is a thin adapter on top of the shared planning core in
`utils.spore_mosaic_render`:

1. Rows are turned into `SporeMosaicSource` (neutral geometry, no cloud
   IDs). Cloud IDs are held in a side map keyed by `item_id` and bound
   back to each tile at manifest-build time.
2. `plan_mosaic(grid_policy=SQUARE_IMAGE, orient=True, annotation=None)`
   picks the common physical crop and grid layout. Aspect targets a
   near-square atlas because the landing site treats mosaic width and
   height independently.
3. Each cell is rasterised with `render_spore_thumbnail_common_crop`
   (Pillow) and pasted onto the shared canvas at `cell.x_px/y_px`.
4. A `SporeMosaicManifest` is built from `MosaicLayoutPlan` and the
   binding side map.

Overlay JSON stores `{"polygon": [{x, y}, …], "style": "b"}` in the
final visible tile's local coordinate system. The old line overlay is
not emitted. Landing ignores unknown / missing overlays.

When p3/p4 are missing on a measurement we DO NOT synthesise a
rectangle — the tile still renders (oriented, cropped around p1/p2 with
padding), but `overlay_json` is `None`. Desktop export filters such
measurements out upstream; cloud tolerates them and lets landing render
the tile without an overlay.
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
    plan_common_crop as _plan_common_crop_impl,
    plan_spore_thumbnail,
    render_spore_thumbnail_common_crop,
)
from utils.spore_mosaic_render import (
    MosaicGridPolicy,
    SporeMosaicSource,
    plan_mosaic,
    select_grid_shape,
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
#
# v2: manifest upsert payload gained tile_width_px / tile_height_px /
# common_crop_width_um / common_crop_height_um (see the sporely-web
# migration 20260721120000 and cloud_sync.py::_push_spore_mosaic_for_observation).
# Atlas bytes are unchanged, but pre-v2 mosaic rows on Supabase have NULL
# calibration columns and thus no scale bar on landing. The bump forces a
# one-off re-upload of every previously-synced observation so calibrated
# scale bars can render everywhere.
#
# v3: rebuilt on top of the shared planning core `utils.spore_mosaic_render`.
# Grid selection now targets a near-square atlas aspect (SQUARE_IMAGE
# policy) via `select_grid_shape`, which minimises `abs(log(actual/target))`
# aspect error — the previous ceil(sqrt(n)) heuristic produced tall, narrow
# atlases for slender spores (q ≈ 2.3). Landing already reads per-tile
# `mosaicX/Y/W/H` from the manifest, so the new positions are correct by
# construction; no landing change is required.
MOSAIC_PIPELINE_VERSION = 3

RECTANGLE_STYLE_A = "a"
RECTANGLE_STYLE_B = "b"
DEFAULT_RECTANGLE_STYLE = RECTANGLE_STYLE_B


# ── Public data model ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SporeCropSource:
    """One measurement's worth of input to the cloud mosaic builder.

    Cloud-specific: bundles cloud IDs alongside the geometry so the
    pusher can bind them onto tile manifest rows without threading a
    parallel structure through the planning core.
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


# ── Legacy grid helpers (kept for test back-compat) ─────────────────────────
#
# These preserve the pre-v3 `ceil(sqrt(n))` layout so existing tests that
# import the historical helper surface continue to work. New production
# code should not call them — use `plan_mosaic` (which routes through
# `select_grid_shape`) instead.


def compute_mosaic_grid(tile_count: int, tile_size_px: int) -> tuple[int, int, int, int]:
    """Legacy: near-square grid with square cells (`ceil(sqrt(n))`)."""
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
    """Legacy: grid math for non-square cells (`ceil(sqrt(n))`)."""
    if tile_count < 1:
        raise ValueError("tile_count must be >= 1")
    if cell_width_px < 1 or cell_height_px < 1:
        raise ValueError("cell dimensions must be positive")
    cols = max(1, math.ceil(math.sqrt(tile_count)))
    rows = max(1, math.ceil(tile_count / cols))
    return cols, rows, cols * cell_width_px, rows * cell_height_px


def place_tiles(tile_count: int, tile_size_px: int) -> list[tuple[int, int, int, int]]:
    """Row-major placement using square cells."""
    cols, _rows, _w, _h = compute_mosaic_grid(tile_count, tile_size_px)
    return [
        (index % cols * tile_size_px, index // cols * tile_size_px,
         tile_size_px, tile_size_px)
        for index in range(tile_count)
    ]


def place_tiles_cells(
    tile_count: int,
    cell_width_px: int,
    cell_height_px: int,
) -> list[tuple[int, int, int, int]]:
    """Row-major placement using rectangular cells."""
    cols, _rows, _w, _h = compute_mosaic_grid_cells(tile_count, cell_width_px, cell_height_px)
    return [
        (index % cols * cell_width_px, index // cols * cell_height_px,
         cell_width_px, cell_height_px)
        for index in range(tile_count)
    ]


# ── plan_common_crop re-export (kept for test back-compat) ──────────────────


@dataclass(frozen=True)
class MosaicCropPlan:
    """Legacy alias of `utils.spore_mosaic_render.MosaicCropPlan` used by
    existing test callers importing from this module."""

    common_crop_width_um: float
    common_crop_height_um: float
    output_tile_width: int
    output_tile_height: int
    common_crop_width: int = 0
    common_crop_height: int = 0


def plan_common_crop(
    plans: Sequence[SporeThumbnailPlan],
    output_height_px: int,
) -> MosaicCropPlan | None:
    """Backwards-compatible re-export of the planning-core helper.

    Returns the legacy `MosaicCropPlan` dataclass (structurally
    identical) so existing tests do not have to migrate their imports.
    """
    result = _plan_common_crop_impl(plans, output_height_px)
    if result is None:
        return None
    return MosaicCropPlan(
        common_crop_width_um=result.common_crop_width_um,
        common_crop_height_um=result.common_crop_height_um,
        output_tile_width=result.output_tile_width,
        output_tile_height=result.output_tile_height,
        common_crop_width=result.common_crop_width,
        common_crop_height=result.common_crop_height,
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


# ── PIL adapter ─────────────────────────────────────────────────────────────


def _open_source_image(path: Path) -> Image.Image:
    return Image.open(path)


def _bind_cloud_ids(sources: Sequence[SporeCropSource]) -> tuple[
    list[SporeMosaicSource], dict[int, SporeCropSource]
]:
    """Turn cloud-specific rows into the neutral `SporeMosaicSource`.

    The returned side map keeps the cloud IDs handy so the manifest
    builder can bind them onto each tile without threading a parallel
    structure through the planner.
    """
    neutral: list[SporeMosaicSource] = []
    cloud_index: dict[int, SporeCropSource] = {}
    for src in sources:
        cloud_index[int(src.measurement_id)] = src
        neutral.append(SporeMosaicSource(
            item_id=int(src.measurement_id),
            source_path=src.source_path,
            source_width=int(src.source_width),
            source_height=int(src.source_height),
            p1_x=float(src.p1_x), p1_y=float(src.p1_y),
            p2_x=float(src.p2_x), p2_y=float(src.p2_y),
            p3_x=src.p3_x, p3_y=src.p3_y,
            p4_x=src.p4_x, p4_y=src.p4_y,
            length_um=src.length_um,
            width_um=src.width_um,
            extra_rotation_deg=float(src.gallery_rotation_deg or 0),
        ))
    return neutral, cloud_index


def build_spore_mosaic(
    sources: Sequence[SporeCropSource],
    *,
    tile_size_px: int = DEFAULT_TILE_SIZE_PX,
    quality: int = DEFAULT_WEBP_QUALITY,
    background: tuple[int, int, int] = DEFAULT_BACKGROUND_RGB,
    overlay_style: str = DEFAULT_RECTANGLE_STYLE,
) -> SporeMosaicManifest | None:
    """Compose a common-crop WebP atlas + tile manifest.

    Thin adapter on top of the shared `plan_mosaic` planner. Every tile
    in the returned manifest has the SAME `w_px` / `h_px`, chosen from
    the widest and tallest natural padded crops across the input
    measurements. Grid layout targets a near-square atlas so slender
    spores do not produce a tall, narrow image.
    """
    if tile_size_px < 8:
        raise ValueError("tile_size_px too small")
    if not sources:
        return None

    ordered = list(sources)
    neutral_sources, cloud_index = _bind_cloud_ids(ordered)

    layout = plan_mosaic(
        neutral_sources,
        orient=True,
        grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=int(tile_size_px),
        annotation=None,
        background_rgb=background,
    )
    if layout is None:
        return None

    out_w = layout.tile_width_px
    out_h = layout.tile_height_px
    common_w_um = layout.common_crop_width_um
    common_h_um = layout.common_crop_height_um

    canvas = Image.new(
        "RGB", (layout.mosaic_width_px, layout.mosaic_height_px), background,
    )

    tiles: list[SporeMosaicTile] = []
    skipped: list[tuple[int, str]] = list(layout.skipped)
    open_cache: dict[Path, Image.Image] = {}

    try:
        for cell in layout.cells:
            tile_plan = cell.tile
            src_cloud = cloud_index.get(tile_plan.source.item_id)
            if src_cloud is None:
                skipped.append((tile_plan.source.item_id, "missing cloud binding"))
                continue

            try:
                img = open_cache.get(src_cloud.source_path)
                if img is None:
                    img = _open_source_image(src_cloud.source_path)
                    open_cache[src_cloud.source_path] = img
            except FileNotFoundError:
                skipped.append((tile_plan.source.item_id, "source image missing"))
                continue
            except Exception as exc:  # pragma: no cover
                skipped.append((tile_plan.source.item_id, f"open failed: {exc}"))
                continue

            try:
                result = render_spore_thumbnail_common_crop(
                    img, tile_plan.thumbnail_plan,
                    common_crop_width=tile_plan.common_crop_width_px,
                    common_crop_height=tile_plan.common_crop_height_px,
                    output_width=out_w,
                    output_height=out_h,
                )
            except Exception as exc:  # pragma: no cover
                skipped.append((tile_plan.source.item_id, f"render failed: {exc}"))
                continue

            canvas.paste(result.image, (cell.x_px, cell.y_px))

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

            plan_diag = tile_plan.diagnostics
            diagnostics = {
                "measurement_id": src_cloud.measurement_id,
                "have_p1": src_cloud.p1_x is not None and src_cloud.p1_y is not None,
                "have_p2": src_cloud.p2_x is not None and src_cloud.p2_y is not None,
                "have_p3": src_cloud.p3_x is not None and src_cloud.p3_y is not None,
                "have_p4": src_cloud.p4_x is not None and src_cloud.p4_y is not None,
                "gallery_rotation_deg": src_cloud.gallery_rotation_deg,
                "rotation_deg": plan_diag.get("rotation_deg"),
                "length_um": src_cloud.length_um,
                "width_um": src_cloud.width_um,
                "length_axis_px": round(tile_plan.thumbnail_plan.length_axis_px, 3),
                "width_axis_px": round(tile_plan.thumbnail_plan.width_axis_px, 3),
                "length_axis_px_per_um": plan_diag.get("length_axis_px_per_um"),
                "width_axis_px_per_um": plan_diag.get("width_axis_px_per_um"),
                "scale_fallback_reason": plan_diag.get("scale_fallback_reason"),
                "natural_crop_um": plan_diag.get("natural_crop_um"),
                "common_crop_um": (round(common_w_um, 3), round(common_h_um, 3)),
                "crop_px": (tile_plan.common_crop_width_px, tile_plan.common_crop_height_px),
                "output_tile": (out_w, out_h),
                "crop_rect_before_shift": tuple(
                    round(v, 2) for v in result.crop_rect_before_shift
                ),
                "crop_rect_after_shift": result.crop_rect_after_shift,
                "padded_x": result.padded_x,
                "padded_y": result.padded_y,
                "visible_rect_in_atlas": (cell.x_px, cell.y_px, out_w, out_h),
                "polygon_present": overlay is not None,
                "reason_no_polygon": result.reason_no_polygon,
                "polygon_bounds": polygon_bounds,
            }

            tiles.append(SporeMosaicTile(
                measurement_id=int(src_cloud.measurement_id),
                cloud_measurement_id=src_cloud.cloud_measurement_id,
                cloud_image_id=src_cloud.cloud_image_id,
                x_px=cell.x_px, y_px=cell.y_px, w_px=out_w, h_px=out_h,
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

    # Representative common-crop pixel dims (widest measurement) — kept
    # in the manifest only for legacy diagnostics; the per-tile crop
    # pixel dims live on the individual tile plans.
    rep_tile = max(
        layout.cells,
        key=lambda c: c.tile.common_crop_height_px,
    ).tile if layout.cells else None
    rep_w_px = int(rep_tile.common_crop_width_px) if rep_tile else 0
    rep_h_px = int(rep_tile.common_crop_height_px) if rep_tile else 0

    return SporeMosaicManifest(
        image_bytes=buf.getvalue(),
        content_type="image/webp",
        width_px=layout.mosaic_width_px,
        height_px=layout.mosaic_height_px,
        tile_size_px=int(tile_size_px),
        tile_width_px=out_w,
        tile_height_px=out_h,
        common_crop_width_px=rep_w_px,
        common_crop_height_px=rep_h_px,
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
