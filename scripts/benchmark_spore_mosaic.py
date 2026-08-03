#!/usr/bin/env python3
"""Synthetic benchmark harness for `utils.cloud_spore_mosaic.build_spore_mosaic`.

Usage:

    PYTHONPATH=. python scripts/benchmark_spore_mosaic.py \
        --sizes 9 25 49 200 \
        --source-size 1600 \
        --workers 1

Builds one temporary source image per requested spore count and threads
them through `build_spore_mosaic` twice — the first run records the
full render + encode, the second confirms the sync-time fast path
(unchanged mosaic signature) would kick in downstream (this harness
does NOT touch the network; it only prints the raw build cost).

Timing information comes from `MosaicBuildTimings.summary()`. The
harness prints a Markdown table plus the top-5 slowest tiles for the
49-tile case so we can spot which stage dominates the wall-clock
budget.
"""
from __future__ import annotations

import argparse
import statistics
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from PIL import Image  # noqa: E402

from utils.cloud_spore_mosaic import (  # noqa: E402
    DEFAULT_TILE_SIZE_PX,
    SporeCropSource,
    build_spore_mosaic,
)


@dataclass
class BenchResult:
    tile_count: int
    total_ms: float
    plan_ms: float
    decode_ms: float
    render_ms: float
    paste_ms: float
    encode_ms: float
    digest_ms: float
    mean_tile_ms: float
    max_tile_ms: float
    total_source_megapixels: float
    top_slowest: list[tuple[int, int]]


def _make_source(path: Path, size: int) -> None:
    """Write a synthetic RGB image with structure that isn't flat, so
    resampling costs mimic real microscope frames."""
    img = Image.new("RGB", (size, size), (30, 30, 40))
    # Simple diagonal gradient so the JPEG/WebP encoder has entropy to
    # work with — flat images encode absurdly fast and misrepresent
    # real-world timing.
    px = img.load()
    stride = max(1, size // 64)
    for y in range(0, size, stride):
        for x in range(0, size, stride):
            v = ((x + y) * 3) % 240 + 15
            px[x, y] = (v, (v * 3) % 255, (v * 5) % 255)
    img.save(path, format="PNG")


def _make_sources(
    n: int,
    image_path: Path,
    source_size: int,
) -> list[SporeCropSource]:
    """Build N synthetic measurements with varied length-axis directions
    so `orient=True` triggers a non-trivial rotation on every tile.
    Real spore galleries include measurements at every angle; a flat
    "all-vertical" benchmark hides the whole-source rotate cost by
    falling into the `abs(rotation) < 0.1` early-out.
    """
    sources = []
    cx = source_size // 2
    cy = source_size // 2
    import math as _math
    for i in range(n):
        # Distribute the "spore" measurement across the image so the
        # crop centre isn't always the source centre.
        jitter_x = ((i * 37) % (source_size // 4)) - (source_size // 8)
        jitter_y = ((i * 61) % (source_size // 4)) - (source_size // 8)
        # Rotate the length-axis direction so orient=True has real work
        # to do. Half-turn coverage is enough — the rotate cost is
        # symmetric under angle-vs-angle-+180°.
        angle = (i * 17) % 180
        theta = _math.radians(angle)
        dxl = _math.cos(theta) * 40.0
        dyl = _math.sin(theta) * 40.0
        dxw = _math.cos(theta + _math.pi / 2.0) * 12.0
        dyw = _math.sin(theta + _math.pi / 2.0) * 12.0
        px = cx + jitter_x
        py = cy + jitter_y
        p1 = (px + dxl, py + dyl)
        p2 = (px - dxl, py - dyl)
        p3 = (px + dxw, py + dyw)
        p4 = (px - dxw, py - dyw)
        sources.append(SporeCropSource(
            measurement_id=i + 1,
            image_id=1,
            cloud_measurement_id=str(i + 1),
            cloud_image_id="1",
            source_path=image_path,
            source_width=source_size,
            source_height=source_size,
            p1_x=float(p1[0]), p1_y=float(p1[1]),
            p2_x=float(p2[0]), p2_y=float(p2[1]),
            p3_x=float(p3[0]), p3_y=float(p3[1]),
            p4_x=float(p4[0]), p4_y=float(p4[1]),
            length_um=8.0, width_um=2.4,
        ))
    return sources


def _run_one(
    tile_count: int,
    source_size: int,
    quality: int,
) -> BenchResult:
    with tempfile.TemporaryDirectory(prefix="mosaic_bench_") as tmp:
        image_path = Path(tmp) / "src.png"
        _make_source(image_path, source_size)
        sources = _make_sources(tile_count, image_path, source_size)
        # Warm-up run to load Pillow codec + prime the OS page cache.
        first = build_spore_mosaic(
            sources, tile_size_px=DEFAULT_TILE_SIZE_PX, quality=quality,
        )
        assert first is not None and first.timings is not None
        # Actual measurement (post-warm).
        manifest = build_spore_mosaic(
            sources, tile_size_px=DEFAULT_TILE_SIZE_PX, quality=quality,
        )
        assert manifest is not None and manifest.timings is not None
        summary = manifest.timings.summary()
        return BenchResult(
            tile_count=tile_count,
            total_ms=summary["total_ms"],
            plan_ms=summary["plan_ms"],
            decode_ms=summary["decode_ms"],
            render_ms=summary["render_ms"],
            paste_ms=summary["paste_ms"],
            encode_ms=summary["encode_ms"],
            digest_ms=summary["digest_ms"],
            mean_tile_ms=summary["mean_tile_ms"],
            max_tile_ms=summary["max_tile_ms"],
            total_source_megapixels=summary["total_source_megapixels"],
            top_slowest=manifest.timings.top_slowest(5),
        )


def _print_table(results: list[BenchResult]) -> None:
    header = (
        "| tiles | total | plan | decode | render | paste | encode | "
        "digest | mean/tile | max/tile | src MPix |"
    )
    sep = (
        "|-------|-------|------|--------|--------|-------|--------|"
        "--------|-----------|----------|----------|"
    )
    print(header)
    print(sep)
    for r in results:
        print(
            f"| {r.tile_count:>5} "
            f"| {r.total_ms:>5.1f} "
            f"| {r.plan_ms:>4.1f} "
            f"| {r.decode_ms:>6.1f} "
            f"| {r.render_ms:>6.1f} "
            f"| {r.paste_ms:>5.1f} "
            f"| {r.encode_ms:>6.1f} "
            f"| {r.digest_ms:>6.1f} "
            f"| {r.mean_tile_ms:>9.2f} "
            f"| {r.max_tile_ms:>8.2f} "
            f"| {r.total_source_megapixels:>8.2f} |"
        )


def _mosaic_signature_probe(source_size: int, tile_count: int, quality: int) -> None:
    """Confirm that a second build of identical inputs produces bytes
    that hash identically — the same signal `cloud_sync` uses to skip
    an unchanged mosaic upload."""
    with tempfile.TemporaryDirectory(prefix="mosaic_bench_signature_") as tmp:
        image_path = Path(tmp) / "src.png"
        _make_source(image_path, source_size)
        sources = _make_sources(tile_count, image_path, source_size)
        first = build_spore_mosaic(sources, tile_size_px=DEFAULT_TILE_SIZE_PX, quality=quality)
        second = build_spore_mosaic(sources, tile_size_px=DEFAULT_TILE_SIZE_PX, quality=quality)
        assert first is not None and second is not None
        same = first.image_bytes == second.image_bytes
        print(
            f"[signature-probe] identical inputs → same atlas bytes? {same} "
            f"(first_bytes={len(first.image_bytes)} "
            f"second_bytes={len(second.image_bytes)})",
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", type=int, nargs="+", default=[9, 25, 49, 200])
    parser.add_argument("--source-size", type=int, default=1600)
    parser.add_argument("--quality", type=int, default=82)
    parser.add_argument("--iterations", type=int, default=1)
    args = parser.parse_args()

    print(
        f"# spore-mosaic benchmark  |  source={args.source_size}px  "
        f"|  quality={args.quality}  |  iterations={args.iterations}"
    )
    all_results: dict[int, list[BenchResult]] = {}
    for tile_count in args.sizes:
        runs = []
        for _ in range(max(1, args.iterations)):
            runs.append(_run_one(tile_count, args.source_size, args.quality))
        # Median over iterations to smooth over CI/thermal noise.
        median_total = statistics.median(r.total_ms for r in runs)
        # Pick the run closest to the median for the printed row.
        chosen = min(runs, key=lambda r: abs(r.total_ms - median_total))
        all_results[tile_count] = runs
        print(
            f"[{tile_count} tiles] runs={len(runs)} median_total_ms={median_total:.1f} "
            f"top5={chosen.top_slowest}"
        )

    print()
    _print_table([min(all_results[n], key=lambda r: r.total_ms) for n in args.sizes])
    print()
    _mosaic_signature_probe(args.source_size, args.sizes[0], args.quality)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
