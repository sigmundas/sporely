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
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Sequence

from PIL import Image

from utils.spore_thumbnail_render import (
    SporeThumbnailInputs,
    SporeThumbnailPlan,
    _to_rgb,
    plan_common_crop as _plan_common_crop_impl,
    plan_spore_thumbnail,
    render_spore_thumbnail_common_crop,
)
from utils.spore_mosaic_render import (
    MosaicCell,
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
#
# v4: authoritative per-image µm-per-pixel now drives the render geometry
# (both axes) via `SporeMosaicSource.scale_um_per_px`. The pusher threads
# `images.scale_microns_per_pixel` into the source before planning, so
# tiles never use an endpoint-derived scale that disagrees with the image
# calibration. Stored `length_um` / `width_um` still power the label text.
# For observations where those two values agree (the common case) the
# atlas bytes are unchanged. When they disagree (recalibrated image,
# manual length_um edit) the tile now honours the calibrated scale, so a
# one-off rebuild lands the whole atlas at the corrected geometry.
# Padded off-centre placement also now centres the measurement on the
# tile rather than the source in the canvas — noticeable only for
# measurements near source edges when the common crop overflows the
# source on an axis.
MOSAIC_PIPELINE_VERSION = 4

RECTANGLE_STYLE_A = "a"
RECTANGLE_STYLE_B = "b"
DEFAULT_RECTANGLE_STYLE = RECTANGLE_STYLE_B


# ── Timing / progress instrumentation ───────────────────────────────────────
#
# `SPORELY_DEBUG_MOSAIC_TIMING=1` enables per-tile debug logs. The
# top-level aggregate is always attached to `SporeMosaicManifest.timings`
# so the sync layer (or benchmark harness) can inspect it without
# depending on a debug env flag.


def _mosaic_debug_timing_enabled() -> bool:
    raw = os.environ.get("SPORELY_DEBUG_MOSAIC_TIMING", "0").strip().lower()
    return raw not in ("", "0", "false", "no")


@dataclass
class MosaicBuildTimings:
    """Wall-clock breakdown for one `build_spore_mosaic` call.

    All values are in nanoseconds unless the field name indicates
    otherwise. Aggregates are populated at the end of the build so
    callers can turn `total_ns` into a rate (`tiles / total_s`) or
    detect regressions from a benchmark harness.

    Memory instrumentation (Phase 2.C bounded-source refactor)
    ----------------------------------------------------------
    * ``distinct_source_count`` — number of distinct source paths
      opened during the build.  Mirrors ``distinct_sources`` and is
      kept as a separate name so the "memory" story reads clearly in
      the benchmark harness table.
    * ``peak_open_sources`` — the maximum count of simultaneously
      decoded source images the builder held open at any moment.
      With the grouped-render loop this is bounded at 1.
    * ``peak_decoded_megapixels`` — the largest per-source decoded
      footprint the builder held at any moment, estimated as
      ``width * height * (channels_raw + channels_working) / 1e6``.
      When the raw source is already RGB the working image aliases
      it, so only the RGB channel count is charged.
    """

    total_ns: int = 0
    plan_ns: int = 0
    decode_ns: int = 0
    render_tile_ns: int = 0
    paste_ns: int = 0
    encode_ns: int = 0
    digest_ns: int = 0
    tile_count: int = 0
    distinct_sources: int = 0
    per_tile_ns: list[tuple[int, int]] = field(default_factory=list)
    total_source_megapixels: float = 0.0
    peak_open_sources: int = 0
    peak_decoded_megapixels: float = 0.0
    distinct_source_count: int = 0

    def top_slowest(self, k: int = 5) -> list[tuple[int, int]]:
        """Return the top-k slowest (measurement_id, ns) tiles."""
        return sorted(self.per_tile_ns, key=lambda pair: -pair[1])[: max(1, int(k))]

    def summary(self) -> dict:
        """Serialisable summary — no lists so it fits into a log line."""
        mean_tile_ns = (
            int(self.render_tile_ns // max(1, self.tile_count))
            if self.tile_count else 0
        )
        max_tile_ns = max((ns for _mid, ns in self.per_tile_ns), default=0)
        return {
            "total_ms": round(self.total_ns / 1e6, 2),
            "plan_ms": round(self.plan_ns / 1e6, 2),
            "decode_ms": round(self.decode_ns / 1e6, 2),
            "render_ms": round(self.render_tile_ns / 1e6, 2),
            "paste_ms": round(self.paste_ns / 1e6, 2),
            "encode_ms": round(self.encode_ns / 1e6, 2),
            "digest_ms": round(self.digest_ns / 1e6, 2),
            "tile_count": int(self.tile_count),
            "distinct_sources": int(self.distinct_sources),
            "distinct_source_count": int(self.distinct_source_count),
            "peak_open_sources": int(self.peak_open_sources),
            "peak_decoded_megapixels": round(self.peak_decoded_megapixels, 3),
            "mean_tile_ms": round(mean_tile_ns / 1e6, 2),
            "max_tile_ms": round(max_tile_ns / 1e6, 2),
            "total_source_megapixels": round(self.total_source_megapixels, 3),
        }


# Progress callback contract used by the sync layer and the live UI.
#
#     progress(stage: str, current: int, total: int) -> None
#
# `stage` is one of the constants below. `current` and `total` are
# advisory (0/0 for indeterminate stages such as "encode"). Callbacks
# must be short-lived and MUST NOT block on network or database work.
# Default is a no-op — determinism-critical code paths ignore progress.
MOSAIC_PROGRESS_PLANNING = "planning"
MOSAIC_PROGRESS_RENDERING = "rendering"
MOSAIC_PROGRESS_ENCODING = "encoding"
MOSAIC_PROGRESS_DIGEST = "digest"
MOSAIC_PROGRESS_COMPLETE = "complete"

MosaicProgressCallback = Callable[[str, int, int], None]


def _noop_progress(stage: str, current: int, total: int) -> None:
    _ = stage, current, total


class _ThrottledProgress:
    """Wraps a raw progress callback so intra-stage tile updates fire at
    most every ``min_interval_ns`` OR every ``every_k`` tiles, whichever
    comes first. Stage transitions and the terminal "complete" state
    ALWAYS fire.
    """

    __slots__ = ("_cb", "_min_interval_ns", "_every_k", "_last_ns", "_last_current")

    def __init__(
        self,
        cb: MosaicProgressCallback,
        *,
        min_interval_ns: int = 100_000_000,   # 100 ms
        every_k: int = 8,
    ):
        self._cb = cb
        self._min_interval_ns = int(min_interval_ns)
        self._every_k = max(1, int(every_k))
        self._last_ns = 0
        self._last_current = -1

    def emit(self, stage: str, current: int, total: int) -> None:
        if self._cb is None:
            return
        now_ns = time.monotonic_ns()
        elapsed = now_ns - self._last_ns
        current = int(current)
        if (
            elapsed >= self._min_interval_ns
            or (current - self._last_current) >= self._every_k
            or current in (0, total)
        ):
            try:
                self._cb(stage, current, int(total))
            except Exception:
                # Progress callbacks must never break the build.
                pass
            self._last_ns = now_ns
            self._last_current = current

    def force(self, stage: str, current: int = 0, total: int = 0) -> None:
        """Emit unconditionally — used for stage transitions."""
        try:
            self._cb(stage, int(current), int(total))
        except Exception:
            pass
        self._last_ns = time.monotonic_ns()
        self._last_current = int(current)


# ── Public data model ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SporeCropSource:
    """One measurement's worth of input to the cloud mosaic builder.

    Cloud-specific: bundles cloud IDs alongside the geometry so the
    pusher can bind them onto tile manifest rows without threading a
    parallel structure through the planning core.

    ``scale_um_per_px`` carries the AUTHORITATIVE per-image µm-per-pixel
    calibration when the source image has one. The neutral planner
    prefers this over the endpoint-derived length_um/p1p2 fallback, so
    tiles never render at a scale that disagrees with the image's own
    calibration. ``length_um`` / ``width_um`` remain the values shown in
    labels (unchanged by this override).
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
    scale_um_per_px: float | None = None
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
    # Timing breakdown for the build (Phase 2.A instrumentation). Always
    # populated; the sync layer prints the summary on completion.
    timings: MosaicBuildTimings | None = None


# ── Structured build result (Phase 2.D) ────────────────────────────────────
#
# `build_spore_mosaic` returns a `SporeMosaicBuildResult` so callers can
# tell WHY nothing was produced without losing the per-item skip list.
# The old contract ("None on any failure") collapsed distinct failure
# modes into one — the sync layer then had to guess whether that meant
# "no eligible measurements", "every source file was missing", "every
# item lacked calibration", or "the render loop emitted zero tiles".
# Each of those needs a different remediation, so callers now branch on
# ``result.reason`` while ``result.manifest`` remains the only field a
# happy-path caller has to touch.

MOSAIC_BUILD_REASON_NO_INPUT = "no_input"
MOSAIC_BUILD_REASON_ALL_SKIPPED = "all_skipped"
MOSAIC_BUILD_REASON_NO_TILES_RENDERED = "no_tiles_rendered"


@dataclass(frozen=True)
class SporeMosaicBuildResult:
    """Structured return value for `build_spore_mosaic`.

    * ``manifest`` — the built mosaic on success, else ``None``.
    * ``skipped`` — per-item ``(measurement_id, reason)`` pairs. Always
      populated: even a successful build may skip individual items and
      the sync layer surfaces every reason in its log.
    * ``reason`` — top-level failure category (``None`` on success):
        - ``"no_input"`` — the input source list was empty.
        - ``"all_skipped"`` — every planned tile was skipped upstream
          (missing calibration, invalid dims, missing source file …).
        - ``"no_tiles_rendered"`` — plan produced tiles but the render
          loop emitted zero (e.g. a render exception on every item).
    """

    manifest: SporeMosaicManifest | None
    skipped: list[tuple[int, str]]
    reason: str | None


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


def _normalize_source_for_build(
    img: Image.Image,
    background_rgb: tuple[int, int, int],
) -> Image.Image:
    """Convert a decoded source image to RGB exactly once per build.

    Named separately from `_to_rgb` so tests can spy on the per-build
    conversion path and assert it fires at most once per distinct
    source. For already-RGB sources the returned object aliases the
    input (no allocation) — matching `_to_rgb`'s early-return semantics.
    """
    return _to_rgb(img, background_rgb)


def _estimate_decoded_megapixels(
    raw: Image.Image, working: Image.Image,
) -> float:
    """Rough peak footprint estimate = width*height*channels/1e6.

    When ``working`` aliases ``raw`` (already-RGB source) only the raw
    channel count is charged — there is no second buffer. When they
    differ, both are held simultaneously for the group's lifetime, so
    both channel counts are summed."""
    channels_raw = len(raw.getbands())
    channels_working = 0 if working is raw else len(working.getbands())
    total_channels = channels_raw + channels_working
    return (
        float(raw.width) * float(raw.height) * float(total_channels)
        / 1_000_000.0
    )


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
            # Prefer the authoritative image µm-per-pixel over the
            # endpoint-derived scale — the planner resolves in the same
            # order and skips with "missing_calibration" only when
            # neither is available.
            scale_um_per_px=src.scale_um_per_px,
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
    progress_cb: MosaicProgressCallback | None = None,
) -> SporeMosaicBuildResult:
    """Compose a common-crop WebP atlas + tile manifest.

    Thin adapter on top of the shared `plan_mosaic` planner. Every tile
    in the returned manifest has the SAME `w_px` / `h_px`, chosen from
    the widest and tallest natural padded crops across the input
    measurements. Grid layout targets a near-square atlas so slender
    spores do not produce a tall, narrow image.

    Return contract
    ---------------
    Always returns a `SporeMosaicBuildResult`. Callers switch on:

    * ``result.manifest`` — the built mosaic on success, ``None`` when
      the build could not produce an atlas.
    * ``result.reason`` — one of ``None`` (success), ``"no_input"``,
      ``"all_skipped"``, ``"no_tiles_rendered"``.  The sync layer maps
      these to per-status codes so operators see a specific error
      instead of a generic "empty mosaic".
    * ``result.skipped`` — every per-item ``(measurement_id, reason)``
      the planner and render loop recorded, populated even on total
      failure so diagnostics never get lost.

    Source-image memory
    -------------------
    The render loop groups planned cells by ``source_path`` so at most
    one decoded source is held open at a time. RGB normalisation runs
    exactly once per source via `_normalize_source_for_build` — repeated
    per-tile `_to_rgb` invocations on the same grayscale/RGBA frame are
    gone. Both the raw PIL handle AND any distinct RGB-normalised
    intermediate are closed when the group finishes, on success and
    exception paths alike. Tile manifest order still matches the
    original ``layout.cells`` order — grouping is an implementation
    detail, never observable to the caller.

    Progress
    --------
    ``progress_cb(stage, current, total)`` is invoked as the pipeline
    moves through stages: ``planning`` → ``rendering`` → ``encoding``
    → ``digest`` → ``complete``. Callback failures are swallowed so a
    misbehaving UI hook can never break the build. Default is a no-op.
    """
    if tile_size_px < 8:
        raise ValueError("tile_size_px too small")
    if not sources:
        return SporeMosaicBuildResult(
            manifest=None, skipped=[], reason=MOSAIC_BUILD_REASON_NO_INPUT,
        )

    timings = MosaicBuildTimings()
    build_start_ns = time.monotonic_ns()
    progress = _ThrottledProgress(progress_cb or _noop_progress)

    ordered = list(sources)
    neutral_sources, cloud_index = _bind_cloud_ids(ordered)

    progress.force(MOSAIC_PROGRESS_PLANNING, 0, len(neutral_sources))
    plan_start_ns = time.monotonic_ns()
    plan_result = plan_mosaic(
        neutral_sources,
        orient=True,
        grid_policy=MosaicGridPolicy.SQUARE_IMAGE,
        output_tile_height_px=int(tile_size_px),
        annotation=None,
        background_rgb=background,
    )
    timings.plan_ns = time.monotonic_ns() - plan_start_ns
    layout = plan_result.layout
    if layout is None:
        # Preserve the per-item skip reasons from the planner so the
        # cloud sync log can explain "why nothing rendered" instead of
        # collapsing every failure into a bare `None`.
        for mid, reason in plan_result.skipped:
            print(
                f'[cloud_spore_mosaic] plan skip m={mid}: {reason}',
                flush=True,
            )
        # Preserve the planner's own reason (either "no_input" or
        # "all_skipped") so the sync layer can key off it.
        reason = plan_result.reason or MOSAIC_BUILD_REASON_ALL_SKIPPED
        return SporeMosaicBuildResult(
            manifest=None, skipped=list(plan_result.skipped), reason=reason,
        )

    out_w = layout.tile_width_px
    out_h = layout.tile_height_px
    common_w_um = layout.common_crop_width_um
    common_h_um = layout.common_crop_height_um

    canvas = Image.new(
        "RGB", (layout.mosaic_width_px, layout.mosaic_height_px), background,
    )

    skipped: list[tuple[int, str]] = list(layout.skipped)
    # Rendered tiles keyed by original layout position so grouping never
    # changes the manifest order — the atlas coordinates are already
    # baked into each cell, but tile emission order stays deterministic.
    rendered_tiles: dict[int, SporeMosaicTile] = {}
    total_source_megapixels = 0.0
    total_tiles = len(layout.cells)
    progress.force(MOSAIC_PROGRESS_RENDERING, 0, total_tiles)
    debug_timing = _mosaic_debug_timing_enabled()

    # ── Group planned cells by source_path ─────────────────────────────
    # Two cells sharing a source read the same file exactly once, and
    # the RGB normalisation runs exactly once per source. `groups` iters
    # in first-seen order so single-source observations keep their
    # historical ordering; multi-source ones read files in the order
    # they first appear in the layout.
    groups: dict[Path, list[tuple[int, MosaicCell]]] = {}
    cells_missing_binding: list[tuple[int, MosaicCell]] = []
    for cell_index, cell in enumerate(layout.cells):
        src_cloud = cloud_index.get(cell.tile.source.item_id)
        if src_cloud is None:
            cells_missing_binding.append((cell_index, cell))
            continue
        groups.setdefault(src_cloud.source_path, []).append((cell_index, cell))

    for cell_index, cell in cells_missing_binding:
        skipped.append((cell.tile.source.item_id, "missing cloud binding"))

    # ── Render loop, one group at a time ───────────────────────────────
    tile_index_progress = 0
    for source_path, cell_group in groups.items():
        raw_img: Image.Image | None = None
        working_rgb: Image.Image | None = None
        try:
            # Open + decode.
            try:
                decode_start_ns = time.monotonic_ns()
                raw_img = _open_source_image(source_path)
                # Force full decode so tile renders pay no lazy I/O
                # cost within the group.
                raw_img.load()
                # Normalise to RGB exactly once — subsequent
                # `_to_rgb` calls inside the renderers early-return
                # because the image is already RGB.
                working_rgb = _normalize_source_for_build(raw_img, background)
                timings.decode_ns += time.monotonic_ns() - decode_start_ns
            except FileNotFoundError:
                for _idx, group_cell in cell_group:
                    skipped.append(
                        (group_cell.tile.source.item_id, "source image missing"),
                    )
                continue
            except Exception as exc:  # pragma: no cover
                for _idx, group_cell in cell_group:
                    skipped.append(
                        (group_cell.tile.source.item_id, f"open failed: {exc}"),
                    )
                continue

            # Memory accounting: with grouping, at most one source is
            # open at a time, so peak_open_sources is bounded at 1.
            timings.peak_open_sources = max(timings.peak_open_sources, 1)
            source_footprint = _estimate_decoded_megapixels(raw_img, working_rgb)
            if source_footprint > timings.peak_decoded_megapixels:
                timings.peak_decoded_megapixels = source_footprint
            total_source_megapixels += (
                float(raw_img.width) * float(raw_img.height) / 1_000_000.0
            )
            timings.distinct_source_count += 1

            for cell_index, cell in cell_group:
                tile_plan = cell.tile
                src_cloud = cloud_index[tile_plan.source.item_id]

                tile_start_ns = time.monotonic_ns()
                try:
                    render_result = render_spore_thumbnail_common_crop(
                        working_rgb, tile_plan.thumbnail_plan,
                        common_crop_width=tile_plan.common_crop_width_px,
                        common_crop_height=tile_plan.common_crop_height_px,
                        output_width=out_w,
                        output_height=out_h,
                    )
                except Exception as exc:  # pragma: no cover
                    skipped.append(
                        (tile_plan.source.item_id, f"render failed: {exc}"),
                    )
                    continue
                tile_render_ns = time.monotonic_ns() - tile_start_ns
                timings.render_tile_ns += tile_render_ns

                paste_start_ns = time.monotonic_ns()
                canvas.paste(render_result.image, (cell.x_px, cell.y_px))
                timings.paste_ns += time.monotonic_ns() - paste_start_ns

                timings.per_tile_ns.append(
                    (int(src_cloud.measurement_id), int(tile_render_ns)),
                )
                if debug_timing:
                    print(
                        f'[cloud_spore_mosaic] tile m={src_cloud.measurement_id} '
                        f'render_us={tile_render_ns / 1e3:.1f}',
                        flush=True,
                    )
                tile_index_progress += 1
                progress.emit(
                    MOSAIC_PROGRESS_RENDERING, tile_index_progress, total_tiles,
                )

                overlay = (
                    build_overlay_polygon(
                        render_result.polygon_tile_local, style=overlay_style,
                    )
                    if render_result.polygon_tile_local is not None
                    else None
                )

                polygon_bounds = None
                if render_result.polygon_tile_local is not None:
                    xs = [p[0] for p in render_result.polygon_tile_local]
                    ys = [p[1] for p in render_result.polygon_tile_local]
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
                        round(v, 2) for v in render_result.crop_rect_before_shift
                    ),
                    "crop_rect_after_shift": render_result.crop_rect_after_shift,
                    "padded_x": render_result.padded_x,
                    "padded_y": render_result.padded_y,
                    "visible_rect_in_atlas": (cell.x_px, cell.y_px, out_w, out_h),
                    "polygon_present": overlay is not None,
                    "reason_no_polygon": render_result.reason_no_polygon,
                    "polygon_bounds": polygon_bounds,
                }

                rendered_tiles[cell_index] = SporeMosaicTile(
                    measurement_id=int(src_cloud.measurement_id),
                    cloud_measurement_id=src_cloud.cloud_measurement_id,
                    cloud_image_id=src_cloud.cloud_image_id,
                    x_px=cell.x_px, y_px=cell.y_px, w_px=out_w, h_px=out_h,
                    overlay_json=overlay,
                    diagnostics=diagnostics,
                )
        finally:
            # Close normalised RGB intermediate first (it aliases the raw
            # for already-RGB sources — the `is` guard avoids a
            # double-close), then the raw PIL handle. Both close paths
            # run on success and on any exception the group raised.
            if working_rgb is not None and working_rgb is not raw_img:
                try:
                    working_rgb.close()
                except Exception:
                    pass
            if raw_img is not None:
                try:
                    raw_img.close()
                except Exception:
                    pass

    # Emit tiles in the original plan order — grouping is invisible to
    # callers. `dict.keys()` iteration order isn't sorted, so key by
    # index explicitly.
    tiles: list[SporeMosaicTile] = [
        rendered_tiles[idx] for idx in sorted(rendered_tiles.keys())
    ]

    if not tiles:
        timings.total_ns = time.monotonic_ns() - build_start_ns
        timings.tile_count = 0
        timings.distinct_sources = timings.distinct_source_count
        timings.total_source_megapixels = round(total_source_megapixels, 3)
        return SporeMosaicBuildResult(
            manifest=None,
            skipped=skipped,
            reason=MOSAIC_BUILD_REASON_NO_TILES_RENDERED,
        )

    progress.force(MOSAIC_PROGRESS_ENCODING, 0, 0)
    encode_start_ns = time.monotonic_ns()
    buf = io.BytesIO()
    canvas.save(buf, format="WEBP", quality=quality, method=4)
    canvas.close()
    image_bytes = buf.getvalue()
    timings.encode_ns = time.monotonic_ns() - encode_start_ns

    progress.force(MOSAIC_PROGRESS_DIGEST, 0, 0)
    digest_start_ns = time.monotonic_ns()
    # Digest is cheap but distinct enough to track — it drives the
    # storage-key content-addressing and is the last CPU-bound step
    # before upload. Kept lazy: only summarised into `timings` here.
    _ = hashlib.sha256(image_bytes).digest()
    timings.digest_ns = time.monotonic_ns() - digest_start_ns

    # Representative common-crop pixel dims (widest measurement) — kept
    # in the manifest only for legacy diagnostics; the per-tile crop
    # pixel dims live on the individual tile plans.
    rep_tile = max(
        layout.cells,
        key=lambda c: c.tile.common_crop_height_px,
    ).tile if layout.cells else None
    rep_w_px = int(rep_tile.common_crop_width_px) if rep_tile else 0
    rep_h_px = int(rep_tile.common_crop_height_px) if rep_tile else 0

    timings.total_ns = time.monotonic_ns() - build_start_ns
    timings.tile_count = len(tiles)
    timings.distinct_sources = timings.distinct_source_count
    timings.total_source_megapixels = round(total_source_megapixels, 3)
    progress.force(MOSAIC_PROGRESS_COMPLETE, timings.tile_count, timings.tile_count)

    manifest = SporeMosaicManifest(
        image_bytes=image_bytes,
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
        timings=timings,
    )
    return SporeMosaicBuildResult(
        manifest=manifest, skipped=skipped, reason=None,
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
    Optional: `p3_x`, `p3_y`, `p4_x`, `p4_y`, `length_um`, `width_um`,
    `scale_microns_per_pixel`. The authoritative µm-per-pixel column
    from the `images` table (`scale_microns_per_pixel`) is threaded into
    `SporeCropSource.scale_um_per_px` so the planner can prefer it over
    the endpoint-derived scale.
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
            scale_um_per_px=_maybe_float(row.get("scale_microns_per_pixel")),
            gallery_rotation_deg=int(row.get("gallery_rotation") or 0),
        ))
    return out, skipped
