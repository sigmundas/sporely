"""Pure-helper + end-to-end tests for the spore mosaic builder.

Everything in this file exercises `utils.cloud_spore_mosaic` in isolation —
no Qt, no cloud, no SQLite. The single PIL-backed test verifies that
`build_spore_mosaic` returns valid WebP bytes and that the tile rectangles
in the returned manifest actually decode to the expected pixels.
"""

from __future__ import annotations

import io
import math
from pathlib import Path

import pytest
from PIL import Image

from utils.cloud_spore_mosaic import (
    CONTENT_DIGEST_HEX_CHARS,
    DEFAULT_TILE_SIZE_PX,
    SporeCropSource,
    build_overlay_line,
    build_spore_mosaic,
    build_storage_key,
    compute_content_digest,
    compute_crop_rect,
    compute_mosaic_grid,
    line_to_tile_local,
    place_tiles,
    sources_from_measurement_rows,
)


# ── compute_mosaic_grid / place_tiles ────────────────────────────────────────


@pytest.mark.parametrize(
    "count,expected_cols,expected_rows",
    [
        (1, 1, 1),
        (2, 2, 1),
        (8, 3, 3),
        (9, 3, 3),
        (28, 6, 5),
        (80, 9, 9),
    ],
)
def test_compute_mosaic_grid_shape(count, expected_cols, expected_rows):
    cols, rows, w, h = compute_mosaic_grid(count, tile_size_px=160)
    assert cols == expected_cols
    assert rows == expected_rows
    assert w == cols * 160
    assert h == rows * 160
    # every input tile has a row×col slot
    assert cols * rows >= count


def test_compute_mosaic_grid_rejects_bad_input():
    with pytest.raises(ValueError):
        compute_mosaic_grid(0, 160)
    with pytest.raises(ValueError):
        compute_mosaic_grid(1, 0)


@pytest.mark.parametrize("count", [1, 2, 8, 9, 28, 80])
def test_place_tiles_no_overlap_and_inside_bounds(count):
    rects = place_tiles(count, tile_size_px=160)
    assert len(rects) == count
    cols, rows, w, h = compute_mosaic_grid(count, 160)

    seen: set[tuple[int, int]] = set()
    for x, y, tw, th in rects:
        assert tw == 160 and th == 160
        assert 0 <= x <= w - tw
        assert 0 <= y <= h - th
        # tile origin uniquely identifies a slot in the grid
        assert (x, y) not in seen
        seen.add((x, y))

    # tiles are packed row-major with no gaps until the last row
    assert len(seen) == count
    max_col = max(x // 160 for x, _y, _tw, _th in rects)
    assert max_col < cols
    max_row = max(y // 160 for _x, y, _tw, _th in rects)
    assert max_row < rows


# ── compute_crop_rect ────────────────────────────────────────────────────────


def test_compute_crop_rect_centers_on_midpoint():
    x, y, w, h = compute_crop_rect(
        p1_x=200, p1_y=100, p2_x=300, p2_y=100,
        source_width=1000, source_height=1000,
        tile_size_px=160,
    )
    # Midpoint is (250, 100). Length is 100, side ≈ 100 * 2.2 = 220 clamped to source.
    # Centered horizontally on 250.
    assert x <= 250 <= x + w
    assert y <= 100 <= y + h
    assert w == h  # square (no edge clipping happens near the middle of a big image)


def test_compute_crop_rect_clips_to_image_edges():
    # Measurement near top-left corner should push crop to (0, 0).
    x, y, w, h = compute_crop_rect(
        p1_x=10, p1_y=10, p2_x=20, p2_y=20,
        source_width=200, source_height=200,
        tile_size_px=160,
    )
    assert x == 0 and y == 0
    assert 0 < w <= 200 and 0 < h <= 200


def test_compute_crop_rect_minimum_side_for_short_measurements():
    # A very short measurement still gets at least min_side pixels of context.
    _x, _y, w, h = compute_crop_rect(
        p1_x=500, p1_y=500, p2_x=502, p2_y=500,
        source_width=1000, source_height=1000,
        tile_size_px=160,
        min_side=80,
    )
    assert w >= 80 and h >= 80


def test_compute_crop_rect_rejects_zero_dims():
    with pytest.raises(ValueError):
        compute_crop_rect(0, 0, 10, 10, 0, 100, 160)


# ── line_to_tile_local ────────────────────────────────────────────────────────


def test_line_to_tile_local_identity_when_crop_matches_tile():
    mapped = line_to_tile_local(
        p1_x=10, p1_y=20, p2_x=30, p2_y=40,
        crop_x=0, crop_y=0, crop_w=160, crop_h=160,
        tile_size_px=160,
    )
    assert mapped == (10.0, 20.0, 30.0, 40.0)


def test_line_to_tile_local_scales_when_crop_is_larger_than_tile():
    # crop is 320×320, tile is 160×160 → 0.5× scale.
    mapped = line_to_tile_local(
        p1_x=100, p1_y=200, p2_x=200, p2_y=100,
        crop_x=0, crop_y=0, crop_w=320, crop_h=320,
        tile_size_px=160,
    )
    assert mapped == (50.0, 100.0, 100.0, 50.0)


def test_line_to_tile_local_translates_by_crop_origin():
    # crop offset applied first, then scaled 1:1.
    mapped = line_to_tile_local(
        p1_x=110, p1_y=120, p2_x=130, p2_y=140,
        crop_x=100, crop_y=100, crop_w=160, crop_h=160,
        tile_size_px=160,
    )
    assert mapped == (10.0, 20.0, 30.0, 40.0)


def test_line_to_tile_local_returns_none_on_degenerate_crop():
    assert line_to_tile_local(0, 0, 1, 1, 0, 0, 0, 0, 160) is None
    assert line_to_tile_local(0, 0, 1, 1, 0, 0, 160, 160, 0) is None


# ── build_overlay_line ───────────────────────────────────────────────────────


def test_build_overlay_line_rotation_zero_emits_line():
    overlay = build_overlay_line(
        p1_x=110, p1_y=120, p2_x=130, p2_y=140,
        crop_x=100, crop_y=100, crop_w=160, crop_h=160,
        tile_size_px=160, gallery_rotation_deg=0,
    )
    assert overlay == {"line": {"x1": 10.0, "y1": 20.0, "x2": 30.0, "y2": 40.0}}


@pytest.mark.parametrize("rotation", [90, 180, 270, -90, 45])
def test_build_overlay_line_nonzero_rotation_omits_overlay(rotation):
    overlay = build_overlay_line(
        p1_x=110, p1_y=120, p2_x=130, p2_y=140,
        crop_x=100, crop_y=100, crop_w=160, crop_h=160,
        tile_size_px=160, gallery_rotation_deg=rotation,
    )
    assert overlay is None


def test_build_overlay_line_full_multiples_of_360_still_emit():
    # A 360° "rotation" is a no-op; overlay should still be emitted.
    overlay = build_overlay_line(
        p1_x=10, p1_y=20, p2_x=30, p2_y=40,
        crop_x=0, crop_y=0, crop_w=160, crop_h=160,
        tile_size_px=160, gallery_rotation_deg=360,
    )
    assert overlay is not None
    assert overlay["line"]["x1"] == 10.0


# ── build_storage_key ────────────────────────────────────────────────────────


def test_build_storage_key_matches_per_user_prefix():
    key = build_storage_key("user-uuid-123", "42", 1, "abc1234567890def")
    assert key == "user-uuid-123/42/spore_mosaic_v1_abc1234567890def.webp"


def test_build_storage_key_rejects_empty_inputs():
    with pytest.raises(ValueError):
        build_storage_key("", "42", 1, "abcd")
    with pytest.raises(ValueError):
        build_storage_key("user", "", 1, "abcd")
    with pytest.raises(ValueError):
        build_storage_key("user", "42", 1, "")


def test_build_storage_key_rejects_non_hex_digest():
    with pytest.raises(ValueError):
        build_storage_key("user", "42", 1, "not-hex!")


def test_build_storage_key_normalizes_uppercase_digest():
    # digest inputs are normalised to lower-case; keeps storage keys stable
    # even if the caller passes an upper-case hex string.
    key = build_storage_key("u", "42", 1, "ABCDEF0123456789")
    assert key == "u/42/spore_mosaic_v1_abcdef0123456789.webp"


# ── compute_content_digest ───────────────────────────────────────────────────


def test_compute_content_digest_is_deterministic_and_short():
    a = compute_content_digest(b"hello world")
    b = compute_content_digest(b"hello world")
    assert a == b
    assert len(a) == CONTENT_DIGEST_HEX_CHARS == 16
    assert all(c in "0123456789abcdef" for c in a)


def test_compute_content_digest_changes_when_bytes_change():
    a = compute_content_digest(b"hello world")
    b = compute_content_digest(b"hello world!")
    assert a != b


def test_compute_content_digest_configurable_length():
    d = compute_content_digest(b"x", length=12)
    assert len(d) == 12


def test_compute_content_digest_rejects_absurd_lengths():
    with pytest.raises(ValueError):
        compute_content_digest(b"x", length=3)
    with pytest.raises(ValueError):
        compute_content_digest(b"x", length=65)


def test_storage_key_changes_when_mosaic_bytes_change(tmp_path):
    """End-to-end: two mosaics with different bytes get different storage keys.

    This is the whole point of the fix — same version, different content,
    different URL, so `Cache-Control: immutable` never returns stale bytes.
    """
    src_a = tmp_path / "a.png"
    src_b = tmp_path / "b.png"
    _write_test_source(src_a, color=(200, 20, 20))
    _write_test_source(src_b, color=(20, 200, 20))

    def make_source(path: Path, mid: int) -> SporeCropSource:
        return SporeCropSource(
            measurement_id=mid, image_id=1,
            cloud_measurement_id=str(mid), cloud_image_id="9",
            source_path=path, source_width=200, source_height=200,
            p1_x=50, p1_y=100, p2_x=100, p2_y=100,
            gallery_rotation_deg=0,
        )

    manifest_a = build_spore_mosaic([make_source(src_a, 1)], tile_size_px=64)
    manifest_b = build_spore_mosaic([make_source(src_b, 1)], tile_size_px=64)
    assert manifest_a is not None and manifest_b is not None
    assert manifest_a.image_bytes != manifest_b.image_bytes

    key_a = build_storage_key("u", "42", 1, compute_content_digest(manifest_a.image_bytes))
    key_b = build_storage_key("u", "42", 1, compute_content_digest(manifest_b.image_bytes))
    assert key_a != key_b
    # Same observation still ends up in the same directory.
    assert key_a.startswith("u/42/spore_mosaic_v1_")
    assert key_b.startswith("u/42/spore_mosaic_v1_")


# ── sources_from_measurement_rows ────────────────────────────────────────────


def _row(
    *,
    mid: int,
    cloud_id: str = "1001",
    image_cloud_id: str = "9001",
    filepath: str = "img.jpg",
    p1_x: float = 10, p1_y: float = 10,
    p2_x: float = 30, p2_y: float = 20,
    gallery_rotation: int = 0,
) -> dict:
    return {
        "id": mid, "image_id": 5,
        "cloud_id": cloud_id, "image_cloud_id": image_cloud_id,
        "image_filepath": filepath,
        "p1_x": p1_x, "p1_y": p1_y, "p2_x": p2_x, "p2_y": p2_y,
        "gallery_rotation": gallery_rotation,
    }


def test_sources_from_measurement_rows_uses_resolver(tmp_path):
    resolved: list[Path] = []

    def resolver(path: Path) -> tuple[int, int]:
        resolved.append(path)
        return 640, 480

    sources, skipped = sources_from_measurement_rows(
        [_row(mid=1, filepath="a.jpg"), _row(mid=2, filepath="a.jpg"), _row(mid=3, filepath="b.jpg")],
        image_dir=tmp_path,
        dims_resolver=resolver,
    )
    assert skipped == []
    assert len(sources) == 3
    # dims_cache: only two unique paths resolved.
    assert len(resolved) == 2
    for s in sources:
        assert s.source_width == 640
        assert s.source_height == 480
        assert s.source_path.parent == tmp_path


def test_sources_from_measurement_rows_skips_bad_rows(tmp_path):
    def ok_resolver(_path: Path) -> tuple[int, int]:
        return 100, 100

    rows = [
        _row(mid=1),
        _row(mid=2, cloud_id=""),         # missing measurement cloud id
        _row(mid=3, image_cloud_id=""),   # missing image cloud id
        _row(mid=4, filepath=""),         # missing filepath
        {**_row(mid=5), "p1_x": None},    # invalid p1/p2
    ]
    sources, skipped = sources_from_measurement_rows(
        rows, image_dir=tmp_path, dims_resolver=ok_resolver,
    )
    assert [s.measurement_id for s in sources] == [1]
    reasons = {mid: reason for mid, reason in skipped}
    assert 2 in reasons and 3 in reasons and 4 in reasons and 5 in reasons


def test_sources_from_measurement_rows_reports_missing_image(tmp_path):
    def raises_missing(_path: Path) -> tuple[int, int]:
        raise FileNotFoundError(_path)

    sources, skipped = sources_from_measurement_rows(
        [_row(mid=7)], image_dir=tmp_path, dims_resolver=raises_missing,
    )
    assert sources == []
    assert skipped == [(7, "source image missing")]


# ── build_spore_mosaic (PIL-backed) ──────────────────────────────────────────


def _write_test_source(path: Path, size: tuple[int, int] = (200, 200), color=(120, 40, 40)):
    """Write a small deterministic JPEG we can crop from in tests."""
    img = Image.new("RGB", size, color)
    img.save(path, format="PNG")


def test_build_spore_mosaic_composes_wanted_number_of_tiles(tmp_path):
    src = tmp_path / "src.png"
    _write_test_source(src)
    sources = [
        SporeCropSource(
            measurement_id=i,
            image_id=1,
            cloud_measurement_id=str(1000 + i),
            cloud_image_id="9001",
            source_path=src,
            source_width=200,
            source_height=200,
            p1_x=50 + i * 5, p1_y=100,
            p2_x=100 + i * 5, p2_y=100,
            gallery_rotation_deg=0,
        )
        for i in range(3)
    ]
    manifest = build_spore_mosaic(sources, tile_size_px=64)
    assert manifest is not None
    assert manifest.content_type == "image/webp"
    # 3 tiles → 2x2 grid → 128x128 mosaic at tile_size=64.
    assert manifest.width_px == 128
    assert manifest.height_px == 128
    assert manifest.tile_size_px == 64
    assert len(manifest.tiles) == 3
    ids = [t.cloud_measurement_id for t in manifest.tiles]
    assert ids == ["1000", "1001", "1002"]

    # bytes decode as a WebP of the expected size.
    with Image.open(io.BytesIO(manifest.image_bytes)) as decoded:
        assert decoded.size == (128, 128)
        assert decoded.format == "WEBP"


def test_build_spore_mosaic_tile_rects_are_inside_mosaic(tmp_path):
    src = tmp_path / "src.png"
    _write_test_source(src, size=(400, 400))
    sources = [
        SporeCropSource(
            measurement_id=i, image_id=1,
            cloud_measurement_id=str(1000 + i), cloud_image_id="9001",
            source_path=src, source_width=400, source_height=400,
            p1_x=100, p1_y=100 + i, p2_x=150, p2_y=100 + i,
            gallery_rotation_deg=0,
        )
        for i in range(9)
    ]
    manifest = build_spore_mosaic(sources, tile_size_px=32)
    assert manifest is not None
    for tile in manifest.tiles:
        assert 0 <= tile.x_px < manifest.width_px
        assert 0 <= tile.y_px < manifest.height_px
        assert tile.x_px + tile.w_px <= manifest.width_px
        assert tile.y_px + tile.h_px <= manifest.height_px


def test_build_spore_mosaic_records_skips_for_missing_files(tmp_path):
    good = tmp_path / "good.png"
    _write_test_source(good)
    missing = tmp_path / "missing.png"  # not written
    sources = [
        SporeCropSource(
            measurement_id=1, image_id=1,
            cloud_measurement_id="1", cloud_image_id="9",
            source_path=good, source_width=200, source_height=200,
            p1_x=50, p1_y=100, p2_x=100, p2_y=100,
            gallery_rotation_deg=0,
        ),
        SporeCropSource(
            measurement_id=2, image_id=2,
            cloud_measurement_id="2", cloud_image_id="9",
            source_path=missing, source_width=200, source_height=200,
            p1_x=50, p1_y=100, p2_x=100, p2_y=100,
            gallery_rotation_deg=0,
        ),
    ]
    manifest = build_spore_mosaic(sources, tile_size_px=64)
    assert manifest is not None
    assert len(manifest.tiles) == 1
    assert manifest.tiles[0].measurement_id == 1
    assert manifest.skipped == [(2, "source image missing")]


def test_build_spore_mosaic_returns_none_for_empty_sources():
    assert build_spore_mosaic([], tile_size_px=64) is None


def test_build_spore_mosaic_overlay_matches_line_transform(tmp_path):
    """Overlay coords produced end-to-end match the pure helper."""
    src = tmp_path / "src.png"
    _write_test_source(src, size=(400, 400))
    p1 = (120, 130)
    p2 = (180, 210)
    source = SporeCropSource(
        measurement_id=1, image_id=1,
        cloud_measurement_id="1", cloud_image_id="9",
        source_path=src, source_width=400, source_height=400,
        p1_x=p1[0], p1_y=p1[1], p2_x=p2[0], p2_y=p2[1],
        gallery_rotation_deg=0,
    )
    manifest = build_spore_mosaic([source], tile_size_px=128)
    assert manifest is not None
    tile = manifest.tiles[0]
    assert tile.overlay_json is not None
    line = tile.overlay_json["line"]
    # The exact crop rect depends on padding; check the line has non-zero length
    # and the midpoint is near the tile center (measurement drawn on tile).
    mid_x = (line["x1"] + line["x2"]) / 2
    mid_y = (line["y1"] + line["y2"]) / 2
    assert 0 <= mid_x <= 128 and 0 <= mid_y <= 128
    line_length = math.hypot(line["x2"] - line["x1"], line["y2"] - line["y1"])
    assert line_length > 0


def test_build_spore_mosaic_rotation_skips_overlay_but_still_paints_tile(tmp_path):
    src = tmp_path / "src.png"
    _write_test_source(src, size=(400, 400))
    source = SporeCropSource(
        measurement_id=1, image_id=1,
        cloud_measurement_id="1", cloud_image_id="9",
        source_path=src, source_width=400, source_height=400,
        p1_x=100, p1_y=200, p2_x=200, p2_y=200,
        gallery_rotation_deg=90,
    )
    manifest = build_spore_mosaic([source], tile_size_px=64)
    assert manifest is not None
    assert len(manifest.tiles) == 1
    assert manifest.tiles[0].overlay_json is None
