"""End-to-end tests for the mosaic builder and its pure-helper surface.

The orient/crop/polygon math lives in `utils.spore_thumbnail_render` and
is covered separately in `test_spore_thumbnail_render.py`. This file
verifies that the mosaic composer paces those results onto the atlas
correctly and produces well-formed manifest + overlay data.
"""

from __future__ import annotations

import io
from pathlib import Path

import pytest
from PIL import Image

from utils.cloud_spore_mosaic import (
    CONTENT_DIGEST_HEX_CHARS,
    DEFAULT_RECTANGLE_STYLE,
    DEFAULT_TILE_SIZE_PX,
    RECTANGLE_STYLE_A,
    RECTANGLE_STYLE_B,
    SporeCropSource,
    build_overlay_polygon,
    build_spore_mosaic,
    build_storage_key,
    compute_content_digest,
    compute_mosaic_grid,
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
        assert (x, y) not in seen
        seen.add((x, y))

    assert len(seen) == count


def test_default_tile_size_matches_atlas_slot():
    assert DEFAULT_TILE_SIZE_PX == 320


# ── build_overlay_polygon ───────────────────────────────────────────────────


def test_build_overlay_polygon_returns_expected_shape():
    corners = [(10.1234, 20.4567), (30.0, 40.0), (50.0, 60.0), (70.0, 80.0)]
    payload = build_overlay_polygon(corners, style=RECTANGLE_STYLE_B)
    assert payload == {
        "polygon": [
            {"x": 10.12, "y": 20.46},
            {"x": 30.0, "y": 40.0},
            {"x": 50.0, "y": 60.0},
            {"x": 70.0, "y": 80.0},
        ],
        "style": "b",
    }


def test_build_overlay_polygon_default_style_is_corner_outline():
    assert DEFAULT_RECTANGLE_STYLE == RECTANGLE_STYLE_B
    payload = build_overlay_polygon(
        [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)],
    )
    assert payload is not None and payload["style"] == RECTANGLE_STYLE_B


def test_build_overlay_polygon_normalises_style_input():
    payload = build_overlay_polygon(
        [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)],
        style="A",
    )
    assert payload is not None and payload["style"] == RECTANGLE_STYLE_A


def test_build_overlay_polygon_none_on_missing_or_short_input():
    assert build_overlay_polygon(None) is None
    assert build_overlay_polygon([]) is None
    assert build_overlay_polygon([(0.0, 0.0), (1.0, 1.0)]) is None


# ── Storage key + digest ────────────────────────────────────────────────────


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
    assert build_storage_key("u", "42", 1, "ABCDEF0123456789") == (
        "u/42/spore_mosaic_v1_abcdef0123456789.webp"
    )


def test_compute_content_digest_is_deterministic_and_short():
    a = compute_content_digest(b"hello world")
    b = compute_content_digest(b"hello world")
    assert a == b
    assert len(a) == CONTENT_DIGEST_HEX_CHARS == 16
    assert all(c in "0123456789abcdef" for c in a)


def test_compute_content_digest_changes_when_bytes_change():
    assert compute_content_digest(b"a") != compute_content_digest(b"b")


def test_compute_content_digest_configurable_length():
    assert len(compute_content_digest(b"x", length=12)) == 12


def test_compute_content_digest_rejects_absurd_lengths():
    with pytest.raises(ValueError):
        compute_content_digest(b"x", length=3)
    with pytest.raises(ValueError):
        compute_content_digest(b"x", length=65)


# ── sources_from_measurement_rows ────────────────────────────────────────────


def _row(
    *,
    mid: int,
    cloud_id: str = "1001",
    image_cloud_id: str = "9001",
    filepath: str = "img.jpg",
    p1_x: float = 10, p1_y: float = 10,
    p2_x: float = 30, p2_y: float = 20,
    p3_x=None, p3_y=None, p4_x=None, p4_y=None,
    length_um=None, width_um=None,
    gallery_rotation: int = 0,
) -> dict:
    return {
        "id": mid, "image_id": 5,
        "cloud_id": cloud_id, "image_cloud_id": image_cloud_id,
        "image_filepath": filepath,
        "p1_x": p1_x, "p1_y": p1_y, "p2_x": p2_x, "p2_y": p2_y,
        "p3_x": p3_x, "p3_y": p3_y, "p4_x": p4_x, "p4_y": p4_y,
        "length_um": length_um, "width_um": width_um,
        "gallery_rotation": gallery_rotation,
    }


def test_sources_from_measurement_rows_propagates_p3p4_and_um(tmp_path):
    def resolver(_p: Path) -> tuple[int, int]:
        return 640, 480

    sources, skipped = sources_from_measurement_rows(
        [_row(
            mid=1,
            p1_x=100, p1_y=100, p2_x=200, p2_y=100,
            p3_x=150, p3_y=80, p4_x=150, p4_y=120,
            length_um=10.0, width_um=4.0,
        )],
        image_dir=tmp_path,
        dims_resolver=resolver,
    )
    assert skipped == []
    assert len(sources) == 1
    src = sources[0]
    assert src.p3_x == 150 and src.p4_y == 120
    assert src.length_um == pytest.approx(10.0)


def test_sources_from_measurement_rows_missing_p3p4_ok(tmp_path):
    def resolver(_p: Path) -> tuple[int, int]:
        return 100, 100

    sources, skipped = sources_from_measurement_rows(
        [_row(mid=1, p3_x=None, p3_y=None, p4_x=None, p4_y=None)],
        image_dir=tmp_path, dims_resolver=resolver,
    )
    assert skipped == []
    assert sources[0].p3_x is None and sources[0].p4_x is None


def test_sources_from_measurement_rows_skips_bad_rows(tmp_path):
    def ok_resolver(_p: Path) -> tuple[int, int]:
        return 100, 100

    rows = [
        _row(mid=1),
        _row(mid=2, cloud_id=""),
        _row(mid=3, image_cloud_id=""),
        _row(mid=4, filepath=""),
        {**_row(mid=5), "p1_x": None},
    ]
    sources, skipped = sources_from_measurement_rows(
        rows, image_dir=tmp_path, dims_resolver=ok_resolver,
    )
    assert [s.measurement_id for s in sources] == [1]
    reasons = {mid: reason for mid, reason in skipped}
    for mid in (2, 3, 4, 5):
        assert mid in reasons


def test_sources_from_measurement_rows_reports_missing_image(tmp_path):
    def raises_missing(_path: Path) -> tuple[int, int]:
        raise FileNotFoundError(_path)

    sources, skipped = sources_from_measurement_rows(
        [_row(mid=7)], image_dir=tmp_path, dims_resolver=raises_missing,
    )
    assert sources == []
    assert skipped == [(7, "source image missing")]


# ── build_spore_mosaic (PIL-backed) ─────────────────────────────────────────


def _write_test_source(path: Path, size: tuple[int, int] = (400, 400), color=(120, 40, 40)):
    Image.new("RGB", size, color).save(path, format="PNG")


def _make_source(
    src_path: Path, mid: int,
    *,
    p1=(150, 300), p2=(250, 300),
    p3=(200, 285), p4=(200, 315),
    length_um: float | None = 10.0,
    width_um: float | None = 5.0,
    gallery_rotation_deg: int = 0,
) -> SporeCropSource:
    return SporeCropSource(
        measurement_id=mid, image_id=1,
        cloud_measurement_id=str(mid), cloud_image_id="9",
        source_path=src_path, source_width=400, source_height=400,
        p1_x=p1[0], p1_y=p1[1], p2_x=p2[0], p2_y=p2[1],
        p3_x=p3[0] if p3 else None, p3_y=p3[1] if p3 else None,
        p4_x=p4[0] if p4 else None, p4_y=p4[1] if p4 else None,
        length_um=length_um, width_um=width_um,
        gallery_rotation_deg=gallery_rotation_deg,
    )


def test_build_spore_mosaic_composes_expected_slots(tmp_path):
    src = tmp_path / "src.png"
    _write_test_source(src)
    sources = [_make_source(src, mid=i) for i in range(3)]
    manifest = build_spore_mosaic(sources, tile_size_px=128)
    assert manifest is not None
    assert manifest.content_type == "image/webp"
    # 3 tiles → 2×2 grid → 256×256 at slot=128.
    assert manifest.width_px == 256 and manifest.height_px == 256
    assert manifest.tile_size_px == 128
    assert len(manifest.tiles) == 3
    with Image.open(io.BytesIO(manifest.image_bytes)) as decoded:
        assert decoded.size == (256, 256) and decoded.format == "WEBP"


def test_build_spore_mosaic_emits_polygon_when_p3p4_present(tmp_path):
    src = tmp_path / "src.png"
    _write_test_source(src)
    manifest = build_spore_mosaic([_make_source(src, mid=1)], tile_size_px=256)
    assert manifest is not None
    tile = manifest.tiles[0]
    assert tile.overlay_json is not None
    assert "polygon" in tile.overlay_json
    assert "line" not in tile.overlay_json
    poly = tile.overlay_json["polygon"]
    assert len(poly) == 4
    # Polygon lives in the VISIBLE tile-local coord system, so its bounds
    # must fit inside (0..tile.w_px, 0..tile.h_px), NOT the whole slot.
    for point in poly:
        assert 0 <= point["x"] <= tile.w_px
        assert 0 <= point["y"] <= tile.h_px


def test_build_spore_mosaic_tile_metadata_is_visible_subrect(tmp_path):
    """The public tile row must expose the visible sub-rect (no black bars).

    Landing renders a variable-width tile whose bounds are the visible
    sub-rect within the atlas; the polygon is inside those bounds, not
    inside a padded square slot.
    """
    src = tmp_path / "src.png"
    _write_test_source(src)
    # Length axis horizontal, orient will swing it to vertical → rendered
    # tile height fills the slot, width is narrower.
    source = SporeCropSource(
        measurement_id=1, image_id=1,
        cloud_measurement_id="1", cloud_image_id="9",
        source_path=src, source_width=400, source_height=400,
        p1_x=150, p1_y=200, p2_x=250, p2_y=200,   # horizontal length axis
        p3_x=200, p3_y=190, p4_x=200, p4_y=210,   # narrow width axis
        length_um=10.0, width_um=2.0,
        gallery_rotation_deg=0,
    )
    manifest = build_spore_mosaic([source], tile_size_px=256)
    assert manifest is not None
    tile = manifest.tiles[0]
    # Visible tile height matches slot (renderer height_px = tile_size_px).
    assert tile.h_px == 256
    # After orient, the rectangle is taller-than-wide, so the rendered
    # tile is narrower than the slot → visible sub-rect is genuinely
    # smaller than 256×256.
    assert tile.w_px < 256
    # And the sub-rect must sit inside the atlas.
    assert tile.x_px + tile.w_px <= manifest.width_px
    assert tile.y_px + tile.h_px <= manifest.height_px
    # Diagnostics carry both the paste offset and the resolved sub-rect.
    diag = tile.diagnostics
    assert diag["visible_rect_in_atlas"] == (tile.x_px, tile.y_px, tile.w_px, tile.h_px)
    assert diag["paste_offset"] == ((256 - tile.w_px) // 2, (256 - tile.h_px) // 2)


def test_build_spore_mosaic_skips_polygon_when_p3p4_missing(tmp_path):
    src = tmp_path / "src.png"
    _write_test_source(src)
    # No p3/p4: renderer still produces a tile, but polygon is None per the
    # explicit user directive "do not synthesise a rectangle".
    manifest = build_spore_mosaic(
        [_make_source(src, mid=1, p3=None, p4=None)],
        tile_size_px=256,
    )
    assert manifest is not None
    tile = manifest.tiles[0]
    assert tile.overlay_json is None
    assert tile.diagnostics.get("polygon_present") is False
    assert tile.diagnostics.get("reason_no_polygon") == "missing_p3p4"


def test_build_spore_mosaic_orient_makes_length_axis_vertical(tmp_path):
    """After orient, the polygon's Y-span should be its longest dimension.

    Simulate a length-along-X measurement (200 wide, 40 tall). Orient
    must swing the length axis to vertical so the polygon in tile-local
    coords is taller than wide.
    """
    src = tmp_path / "src.png"
    _write_test_source(src, size=(800, 800))
    horizontal = SporeCropSource(
        measurement_id=1, image_id=1,
        cloud_measurement_id="1", cloud_image_id="9",
        source_path=src, source_width=800, source_height=800,
        p1_x=300, p1_y=400, p2_x=500, p2_y=400,   # length axis 200px, horizontal
        p3_x=400, p3_y=380, p4_x=400, p4_y=420,   # width axis 40px, vertical
        length_um=20.0, width_um=4.0,
        gallery_rotation_deg=0,
    )
    manifest = build_spore_mosaic([horizontal], tile_size_px=256)
    assert manifest is not None
    tile = manifest.tiles[0]
    assert tile.overlay_json is not None
    poly = tile.overlay_json["polygon"]
    xs = [p["x"] for p in poly]
    ys = [p["y"] for p in poly]
    span_x = max(xs) - min(xs)
    span_y = max(ys) - min(ys)
    assert span_y > span_x, f"length axis should be vertical after orient (got {span_x=} {span_y=})"

    diag = tile.diagnostics
    # Orient rotation should be -90° for a horizontal length axis.
    assert diag["rotation_deg"] == pytest.approx(-90.0, abs=1.0)
    assert diag["polygon_present"] is True


def test_build_spore_mosaic_records_diagnostics(tmp_path):
    src = tmp_path / "src.png"
    _write_test_source(src)
    manifest = build_spore_mosaic([_make_source(src, mid=42)], tile_size_px=256)
    assert manifest is not None
    diag = manifest.tiles[0].diagnostics
    for key in (
        "measurement_id",
        "have_p1", "have_p2", "have_p3", "have_p4",
        "gallery_rotation_deg",
        "rotation_deg",
        "crop_rect_source_pixels",
        "tile_size_after_render",
        "tile_size_after_fit",
        "paste_offset",
        "visible_rect_in_atlas",
        "polygon_present",
        "polygon_bounds",
    ):
        assert key in diag, f"missing diagnostic {key!r}"
    assert diag["measurement_id"] == 42
    assert diag["polygon_present"] is True
    assert diag["reason_no_polygon"] is None


def test_build_spore_mosaic_records_skips_for_missing_files(tmp_path):
    good = tmp_path / "good.png"
    _write_test_source(good)
    missing = tmp_path / "missing.png"
    manifest = build_spore_mosaic(
        [_make_source(good, mid=1), _make_source(missing, mid=2)],
        tile_size_px=128,
    )
    assert manifest is not None
    assert [tile.measurement_id for tile in manifest.tiles] == [1]
    assert manifest.skipped == [(2, "source image missing")]


def test_build_spore_mosaic_returns_none_for_empty_sources():
    assert build_spore_mosaic([], tile_size_px=128) is None


def test_build_spore_mosaic_gallery_rotation_still_paints_polygon(tmp_path):
    src = tmp_path / "src.png"
    _write_test_source(src)
    manifest = build_spore_mosaic(
        [_make_source(src, mid=1, gallery_rotation_deg=180)],
        tile_size_px=256,
    )
    assert manifest is not None
    tile = manifest.tiles[0]
    assert tile.overlay_json is not None
    # gallery_rotation composes with orient: length axis remains vertical.
    poly = tile.overlay_json["polygon"]
    xs = [p["x"] for p in poly]
    ys = [p["y"] for p in poly]
    assert max(ys) - min(ys) > max(xs) - min(xs)


def test_storage_key_changes_when_mosaic_bytes_change(tmp_path):
    src_a = tmp_path / "a.png"
    src_b = tmp_path / "b.png"
    _write_test_source(src_a, color=(200, 20, 20))
    _write_test_source(src_b, color=(20, 200, 20))
    manifest_a = build_spore_mosaic([_make_source(src_a, 1)], tile_size_px=128)
    manifest_b = build_spore_mosaic([_make_source(src_b, 1)], tile_size_px=128)
    assert manifest_a is not None and manifest_b is not None
    assert manifest_a.image_bytes != manifest_b.image_bytes
    key_a = build_storage_key("u", "42", 1, compute_content_digest(manifest_a.image_bytes))
    key_b = build_storage_key("u", "42", 1, compute_content_digest(manifest_b.image_bytes))
    assert key_a != key_b
    assert key_a.startswith("u/42/spore_mosaic_v1_")
