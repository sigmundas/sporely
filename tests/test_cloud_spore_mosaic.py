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
    compute_mosaic_grid_cells,
    place_tiles,
    place_tiles_cells,
    plan_common_crop,
    sources_from_measurement_rows,
)
from utils.spore_thumbnail_render import (
    SporeThumbnailInputs,
    plan_spore_thumbnail,
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
    assert manifest.tile_size_px == 128
    # Every tile shares the output tile size (common-crop model).
    tw = manifest.tile_width_px
    th = manifest.tile_height_px
    assert th == 128
    assert tw > 0
    # 3 tiles → 2×2 grid → 2 * tw wide, 2 * th tall.
    assert manifest.width_px == 2 * tw
    assert manifest.height_px == 2 * th
    assert len(manifest.tiles) == 3
    with Image.open(io.BytesIO(manifest.image_bytes)) as decoded:
        assert decoded.size == (2 * tw, 2 * th) and decoded.format == "WEBP"


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

    Under the common-crop model, every tile in one observation has the
    same visible size. When a single measurement has an aspect that is
    tall-and-narrow after orient, the shared visible width is < the
    output height and every tile carries the same w_px/h_px.
    """
    src = tmp_path / "src.png"
    _write_test_source(src)
    # Length axis horizontal, orient will swing it to vertical → oriented
    # measurement is taller than wide → common crop is taller than wide.
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
    # Visible tile height matches the requested output height.
    assert tile.h_px == 256
    # Tall-and-narrow oriented measurement → visible width is < height.
    assert tile.w_px < 256
    # The sub-rect must sit inside the atlas.
    assert tile.x_px + tile.w_px <= manifest.width_px
    assert tile.y_px + tile.h_px <= manifest.height_px
    # No black bar padding was needed — measurement was centred inside
    # a source image significantly larger than the natural crop.
    diag = tile.diagnostics
    assert diag["visible_rect_in_atlas"] == (tile.x_px, tile.y_px, tile.w_px, tile.h_px)
    assert diag["padded_x"] is False
    assert diag["padded_y"] is False
    # Manifest exposes the shared tile size explicitly.
    assert manifest.tile_width_px == tile.w_px
    assert manifest.tile_height_px == tile.h_px


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
        "length_um", "width_um",
        "length_axis_px", "width_axis_px",
        "length_axis_px_per_um", "width_axis_px_per_um",
        "scale_fallback_reason",
        "natural_crop_um",
        "common_crop_um",
        "crop_px",
        "output_tile",
        "crop_rect_before_shift",
        "crop_rect_after_shift",
        "padded_x", "padded_y",
        "visible_rect_in_atlas",
        "polygon_present",
        "polygon_bounds",
    ):
        assert key in diag, f"missing diagnostic {key!r}"
    assert diag["measurement_id"] == 42
    assert diag["polygon_present"] is True
    assert diag["reason_no_polygon"] is None
    assert diag["length_um"] == pytest.approx(10.0)
    assert diag["width_um"] == pytest.approx(5.0)
    assert diag["length_axis_px_per_um"] == pytest.approx(10.0, abs=1e-6)
    assert diag["width_axis_px_per_um"] == pytest.approx(6.0, abs=1e-6)


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


# ── Common crop model ──────────────────────────────────────────────────────


def _plan_from_source(source: SporeCropSource):
    inputs = SporeThumbnailInputs(
        p1_x=source.p1_x, p1_y=source.p1_y,
        p2_x=source.p2_x, p2_y=source.p2_y,
        p3_x=source.p3_x, p3_y=source.p3_y,
        p4_x=source.p4_x, p4_y=source.p4_y,
        orient=True,
        extra_rotation_deg=float(source.gallery_rotation_deg or 0),
        length_um=source.length_um,
        width_um=source.width_um,
    )
    return plan_spore_thumbnail(inputs, source.source_width, source.source_height)


def test_plan_common_crop_picks_max_natural_dimensions(tmp_path):
    """Common crop is chosen in micrometres from the widest / tallest
    natural crop, not raw pixels."""
    src = tmp_path / "src.png"
    _write_test_source(src, size=(600, 600))
    # Same source scale, differing physical size. Length axis 100 px →
    # 10 µm ⇒ 10 px/µm.  Width axis differs per measurement.
    short_spore = _make_source(
        src, mid=1,
        p1=(300, 350), p2=(300, 250),        # 100 px length axis
        p3=(290, 300), p4=(310, 300),        # 20 px width axis
        length_um=10.0, width_um=4.0,        # 20/4 = 5 px/µm width
    )
    tall_spore = _make_source(
        src, mid=2,
        p1=(300, 400), p2=(300, 200),        # 200 px length axis
        p3=(280, 300), p4=(320, 300),        # 40 px width axis
        length_um=20.0, width_um=8.0,        # 40/8 = 5 px/µm width
    )
    plans = [_plan_from_source(short_spore), _plan_from_source(tall_spore)]
    crop_plan = plan_common_crop(plans, output_height_px=320)
    assert crop_plan is not None
    # Common physical crop tracks the physically larger measurement.
    expected_w_um = max(p.natural_crop_width_um for p in plans)
    expected_h_um = max(p.natural_crop_height_um for p in plans)
    assert crop_plan.common_crop_width_um == pytest.approx(expected_w_um)
    assert crop_plan.common_crop_height_um == pytest.approx(expected_h_um)
    assert crop_plan.output_tile_height == 320


def test_build_spore_mosaic_uniform_tile_sizes_across_observation(tmp_path):
    """Every tile in one mosaic shares the same w_px/h_px, chosen so the
    widest measurement fits."""
    src = tmp_path / "src.png"
    _write_test_source(src, size=(800, 800))
    # Three measurements with varying oriented widths.
    measurements = [
        _make_source(src, mid=1, p3=(200, 288), p4=(200, 312)),   # narrow  (width 24)
        _make_source(src, mid=2, p3=(200, 270), p4=(200, 330)),   # medium (width 60)
        _make_source(
            src, mid=3, p1=(150, 300), p2=(250, 300),             # 100 px length
            p3=(200, 240), p4=(200, 360),                          # width 120
        ),
    ]
    manifest = build_spore_mosaic(measurements, tile_size_px=320)
    assert manifest is not None
    ws = {tile.w_px for tile in manifest.tiles}
    hs = {tile.h_px for tile in manifest.tiles}
    assert len(ws) == 1, f"non-uniform tile widths: {ws}"
    assert len(hs) == 1, f"non-uniform tile heights: {hs}"
    assert next(iter(hs)) == 320
    # Manifest exposes the shared dimensions.
    assert manifest.tile_width_px == next(iter(ws))
    assert manifest.tile_height_px == 320


def test_build_spore_mosaic_polygons_stay_within_tile_bounds(tmp_path):
    src = tmp_path / "src.png"
    _write_test_source(src, size=(800, 800))
    measurements = [
        _make_source(src, mid=1, p3=(200, 288), p4=(200, 312)),
        _make_source(src, mid=2, p3=(200, 270), p4=(200, 330)),
        _make_source(
            src, mid=3, p1=(150, 300), p2=(250, 300),
            p3=(200, 240), p4=(200, 360),
        ),
    ]
    manifest = build_spore_mosaic(measurements, tile_size_px=320)
    assert manifest is not None
    for tile in manifest.tiles:
        assert tile.overlay_json is not None
        poly = tile.overlay_json["polygon"]
        for point in poly:
            assert 0 <= point["x"] <= tile.w_px + 0.5
            assert 0 <= point["y"] <= tile.h_px + 0.5


def test_build_spore_mosaic_no_padding_when_source_is_large(tmp_path):
    """Padding must only kick in when the source is genuinely smaller
    than the requested common crop."""
    src = tmp_path / "src.png"
    _write_test_source(src, size=(1000, 1000))
    manifest = build_spore_mosaic([_make_source(src, mid=1)], tile_size_px=320)
    assert manifest is not None
    diag = manifest.tiles[0].diagnostics
    assert diag["padded_x"] is False
    assert diag["padded_y"] is False


def test_build_spore_mosaic_padding_used_when_source_smaller_than_crop(tmp_path):
    """A tiny source triggers background padding on the deficient axis.

    Padding is `DESKTOP_PADDING_X=20` / `DESKTOP_PADDING_Y=15` on each
    side, so with a small source the natural crop is guaranteed to
    exceed the oriented image dimensions on both axes.
    """
    src = tmp_path / "tiny.png"
    _write_test_source(src, size=(20, 20))
    tiny = SporeCropSource(
        measurement_id=1, image_id=1,
        cloud_measurement_id="1", cloud_image_id="9",
        source_path=src, source_width=20, source_height=20,
        # Length axis already pointing up, small measurement centred in
        # the source → no expensive rotation, natural crop = length + 30
        # (y padding) by width + 40 (x padding), both > 20.
        p1_x=10, p1_y=12, p2_x=10, p2_y=8,
        p3_x=9, p3_y=10, p4_x=11, p4_y=10,
        length_um=4.0, width_um=2.0,
    )
    manifest = build_spore_mosaic([tiny], tile_size_px=320)
    assert manifest is not None
    diag = manifest.tiles[0].diagnostics
    # Both axes must be padded because 4+30 > 20 and 2+40 > 20.
    assert diag["padded_x"] is True
    assert diag["padded_y"] is True


def test_build_spore_mosaic_variable_natural_widths_produce_uniform_output(tmp_path):
    """745-like case: many measurements with different natural widths →
    a single shared visible width in the output manifest."""
    src = tmp_path / "src.png"
    _write_test_source(src, size=(1200, 1200))
    measurements = []
    for i, half_w in enumerate([12, 20, 28, 36, 44], start=1):
        measurements.append(_make_source(
            src, mid=i,
            p1=(600, 640), p2=(600, 560),
            p3=(600 - half_w, 600), p4=(600 + half_w, 600),
        ))
    manifest = build_spore_mosaic(measurements, tile_size_px=320)
    assert manifest is not None
    widths = {t.w_px for t in manifest.tiles}
    heights = {t.h_px for t in manifest.tiles}
    assert widths == {manifest.tile_width_px}
    assert heights == {320}


def test_storage_digest_changes_when_common_crop_geometry_changes(tmp_path):
    """Changing the widest measurement's physical size changes the
    common physical crop → different tile width → different content
    digest."""
    src = tmp_path / "src.png"
    _write_test_source(src, size=(800, 800))
    small_only = [
        _make_source(
            src, mid=1,
            p1=(400, 450), p2=(400, 350), p3=(390, 400), p4=(410, 400),
            length_um=10.0, width_um=4.0,
        ),
        _make_source(
            src, mid=2,
            p1=(400, 450), p2=(400, 350), p3=(390, 400), p4=(410, 400),
            length_um=10.0, width_um=4.0,
        ),
    ]
    with_large = [
        _make_source(
            src, mid=1,
            p1=(400, 450), p2=(400, 350), p3=(390, 400), p4=(410, 400),
            length_um=10.0, width_um=4.0,
        ),
        _make_source(
            src, mid=2,
            p1=(400, 500), p2=(400, 300), p3=(380, 400), p4=(420, 400),
            length_um=20.0, width_um=8.0,
        ),
    ]
    m_small = build_spore_mosaic(small_only, tile_size_px=256)
    m_large = build_spore_mosaic(with_large, tile_size_px=256)
    assert m_small is not None and m_large is not None
    # With_large has a physically taller/wider spore → tile aspect changes.
    assert (m_small.tile_width_px, m_small.tile_height_px) != (m_large.tile_width_px, m_large.tile_height_px)
    assert compute_content_digest(m_small.image_bytes) != compute_content_digest(m_large.image_bytes)


def test_place_tiles_cells_uses_rectangular_slots():
    cells = place_tiles_cells(4, cell_width_px=100, cell_height_px=200)
    # 4 tiles → 2×2 grid.
    assert cells == [
        (0, 0, 100, 200),
        (100, 0, 100, 200),
        (0, 200, 100, 200),
        (100, 200, 100, 200),
    ]


def test_compute_mosaic_grid_cells_uses_rectangular_slots():
    cols, rows, w, h = compute_mosaic_grid_cells(3, cell_width_px=100, cell_height_px=200)
    assert (cols, rows) == (2, 2)
    assert (w, h) == (200, 400)


# ── Physical-scale consistency ─────────────────────────────────────────────


def test_axis_specific_px_per_um_from_p1p2_and_p3p4(tmp_path):
    src = tmp_path / "src.png"
    _write_test_source(src, size=(800, 800))
    source = _make_source(
        src, mid=1,
        p1=(400, 450), p2=(400, 350),         # length 100 px
        p3=(380, 400), p4=(420, 400),         # width 40 px
        length_um=10.0, width_um=5.0,
    )
    plan = _plan_from_source(source)
    assert plan.length_axis_px == pytest.approx(100.0)
    assert plan.width_axis_px == pytest.approx(40.0)
    assert plan.length_axis_px_per_um == pytest.approx(10.0)
    assert plan.width_axis_px_per_um == pytest.approx(8.0)
    assert plan.scale_fallback_reason is None


def test_width_scale_fallback_when_p3p4_missing(tmp_path):
    src = tmp_path / "src.png"
    _write_test_source(src, size=(400, 400))
    source = _make_source(
        src, mid=1,
        p1=(200, 250), p2=(200, 150),
        p3=None, p4=None,
        length_um=10.0, width_um=None,
    )
    plan = _plan_from_source(source)
    # Length axis derives cleanly; width axis falls back to the same
    # scale as length and records why.
    assert plan.length_axis_px_per_um == pytest.approx(10.0)
    assert plan.width_axis_px_per_um == pytest.approx(10.0)
    assert plan.scale_fallback_reason is not None


def test_same_physical_dimensions_different_px_scales_render_same_size(tmp_path):
    """Two spores of identical physical size, imaged at different
    px/µm, must display at the same size in the output tile."""
    low_res = tmp_path / "low.png"
    hi_res = tmp_path / "hi.png"
    _write_test_source(low_res, size=(400, 400))
    _write_test_source(hi_res, size=(1600, 1600))
    low = SporeCropSource(
        measurement_id=1, image_id=1,
        cloud_measurement_id="1", cloud_image_id="9",
        source_path=low_res, source_width=400, source_height=400,
        # length 50 px, width 20 px → 5 px/µm length, 4 px/µm width.
        p1_x=200, p1_y=225, p2_x=200, p2_y=175,
        p3_x=190, p3_y=200, p4_x=210, p4_y=200,
        length_um=10.0, width_um=5.0,
    )
    hi = SporeCropSource(
        measurement_id=2, image_id=2,
        cloud_measurement_id="2", cloud_image_id="9",
        source_path=hi_res, source_width=1600, source_height=1600,
        # length 200 px, width 80 px → 20 px/µm length, 16 px/µm width.
        p1_x=800, p1_y=900, p2_x=800, p2_y=700,
        p3_x=760, p3_y=800, p4_x=840, p4_y=800,
        length_um=10.0, width_um=5.0,
    )
    manifest = build_spore_mosaic([low, hi], tile_size_px=320)
    assert manifest is not None
    assert len(manifest.tiles) == 2

    def polygon_span(tile: SporeMosaicTile) -> tuple[float, float]:
        poly = tile.overlay_json["polygon"]
        xs = [p["x"] for p in poly]
        ys = [p["y"] for p in poly]
        return max(xs) - min(xs), max(ys) - min(ys)

    low_w, low_h = polygon_span(manifest.tiles[0])
    hi_w, hi_h = polygon_span(manifest.tiles[1])

    # Same physical size → same displayed spore size (within rounding).
    assert low_w == pytest.approx(hi_w, abs=1.5)
    assert low_h == pytest.approx(hi_h, abs=1.5)

    # And the per-tile pixel crops differ (that's how the physical
    # scale is enforced: bigger source → bigger raw crop → same
    # displayed size after LANCZOS resize).
    low_crop = manifest.tiles[0].diagnostics["crop_px"]
    hi_crop = manifest.tiles[1].diagnostics["crop_px"]
    assert low_crop != hi_crop
    assert hi_crop[0] > low_crop[0]
    assert hi_crop[1] > low_crop[1]


def test_different_physical_sizes_render_proportional(tmp_path):
    """A 20 µm spore must display roughly twice the length of a 10 µm
    spore, even when both share the same source image scale."""
    src = tmp_path / "src.png"
    _write_test_source(src, size=(800, 800))
    small = _make_source(
        src, mid=1,
        p1=(400, 450), p2=(400, 350), p3=(390, 400), p4=(410, 400),
        length_um=10.0, width_um=4.0,
    )
    big = _make_source(
        src, mid=2,
        p1=(400, 500), p2=(400, 300), p3=(380, 400), p4=(420, 400),
        length_um=20.0, width_um=8.0,
    )
    manifest = build_spore_mosaic([small, big], tile_size_px=320)
    assert manifest is not None

    def polygon_span(tile: SporeMosaicTile) -> tuple[float, float]:
        poly = tile.overlay_json["polygon"]
        xs = [p["x"] for p in poly]
        ys = [p["y"] for p in poly]
        return max(xs) - min(xs), max(ys) - min(ys)

    small_w, small_h = polygon_span(manifest.tiles[0])
    big_w, big_h = polygon_span(manifest.tiles[1])

    # Physical ratio is 2×; displayed ratio should be close.
    assert big_h / small_h == pytest.approx(2.0, rel=0.05)
    assert big_w / small_w == pytest.approx(2.0, rel=0.05)


def test_common_crop_is_in_micrometres_not_pixels(tmp_path):
    """The common crop should be chosen from physical natural crops so
    that source pixel scale differences do not shrink the tile."""
    low_res = tmp_path / "low.png"
    hi_res = tmp_path / "hi.png"
    _write_test_source(low_res, size=(400, 400))
    _write_test_source(hi_res, size=(1600, 1600))
    # Both spores are physically identical: 10 µm × 5 µm.
    low = SporeCropSource(
        measurement_id=1, image_id=1,
        cloud_measurement_id="1", cloud_image_id="9",
        source_path=low_res, source_width=400, source_height=400,
        p1_x=200, p1_y=225, p2_x=200, p2_y=175,
        p3_x=190, p3_y=200, p4_x=210, p4_y=200,
        length_um=10.0, width_um=5.0,
    )
    hi = SporeCropSource(
        measurement_id=2, image_id=2,
        cloud_measurement_id="2", cloud_image_id="9",
        source_path=hi_res, source_width=1600, source_height=1600,
        p1_x=800, p1_y=900, p2_x=800, p2_y=700,
        p3_x=760, p3_y=800, p4_x=840, p4_y=800,
        length_um=10.0, width_um=5.0,
    )
    manifest = build_spore_mosaic([low, hi], tile_size_px=320)
    assert manifest is not None
    # Common physical crop is the same for both.
    assert manifest.common_crop_width_um > 0
    assert manifest.common_crop_height_um > 0
    # The two tiles share one common physical crop.
    diag_low = manifest.tiles[0].diagnostics
    diag_hi = manifest.tiles[1].diagnostics
    assert diag_low["common_crop_um"] == diag_hi["common_crop_um"]


def test_overlay_polygon_stays_inside_output_after_physical_crop(tmp_path):
    """Polygons must land inside the visible tile bounds even after the
    per-tile physical→pixel conversion + resize."""
    src = tmp_path / "src.png"
    _write_test_source(src, size=(1200, 1200))
    measurements = []
    for i, (l_um, w_um) in enumerate(
        [(10, 4), (12, 5), (15, 6), (18, 7), (20, 8)], start=1,
    ):
        # Vary length axis pixel span to simulate different px/µm.
        length_px = 40 + i * 10
        measurements.append(SporeCropSource(
            measurement_id=i, image_id=1,
            cloud_measurement_id=str(i), cloud_image_id="9",
            source_path=src, source_width=1200, source_height=1200,
            p1_x=600, p1_y=600 + length_px // 2,
            p2_x=600, p2_y=600 - length_px // 2,
            p3_x=600 - length_px // 5, p3_y=600,
            p4_x=600 + length_px // 5, p4_y=600,
            length_um=float(l_um), width_um=float(w_um),
        ))
    manifest = build_spore_mosaic(measurements, tile_size_px=320)
    assert manifest is not None
    for tile in manifest.tiles:
        assert tile.overlay_json is not None
        for point in tile.overlay_json["polygon"]:
            assert 0 <= point["x"] <= tile.w_px + 0.5
            assert 0 <= point["y"] <= tile.h_px + 0.5


def test_all_tiles_one_distinct_mosaic_w_and_h(tmp_path):
    """Regression guard on uniform mosaicW/mosaicH per observation
    under the physical crop model."""
    src = tmp_path / "src.png"
    _write_test_source(src, size=(1200, 1200))
    measurements = []
    for i, (l_um, w_um) in enumerate(
        [(10, 4), (12, 5), (15, 6), (18, 7), (20, 8)], start=1,
    ):
        length_px = 40 + i * 15
        width_px = length_px // 4
        measurements.append(SporeCropSource(
            measurement_id=i, image_id=1,
            cloud_measurement_id=str(i), cloud_image_id="9",
            source_path=src, source_width=1200, source_height=1200,
            p1_x=600, p1_y=600 + length_px // 2,
            p2_x=600, p2_y=600 - length_px // 2,
            p3_x=600 - width_px, p3_y=600,
            p4_x=600 + width_px, p4_y=600,
            length_um=float(l_um), width_um=float(w_um),
        ))
    manifest = build_spore_mosaic(measurements, tile_size_px=320)
    assert manifest is not None
    ws = {t.w_px for t in manifest.tiles}
    hs = {t.h_px for t in manifest.tiles}
    assert len(ws) == 1
    assert len(hs) == 1
    assert next(iter(hs)) == 320


def test_build_spore_mosaic_slender_spores_produce_near_square_atlas(tmp_path):
    """v3 grid policy: many slender (q ≈ 2.3) spores should not produce a
    tall, narrow atlas. The SQUARE_IMAGE grid policy picks a
    near-square (cols, rows) so the atlas aspect stays close to 1.0.

    Regression guard for the pre-v3 ``ceil(sqrt(n))`` grid, which
    ignored tile aspect and produced tall, narrow atlases for slender
    spores.
    """
    src = tmp_path / "slender.png"
    _write_test_source(src, size=(1200, 1200))
    measurements = []
    for i in range(12):
        # q ≈ 2.3: 46 px length, 20 px width, 10 µm × 4.3 µm.
        measurements.append(SporeCropSource(
            measurement_id=i + 1, image_id=1,
            cloud_measurement_id=str(i + 1), cloud_image_id="9",
            source_path=src, source_width=1200, source_height=1200,
            p1_x=600, p1_y=623, p2_x=600, p2_y=577,
            p3_x=590, p3_y=600, p4_x=610, p4_y=600,
            length_um=10.0, width_um=4.3,
        ))
    manifest = build_spore_mosaic(measurements, tile_size_px=320)
    assert manifest is not None
    aspect = manifest.width_px / max(1, manifest.height_px)
    assert abs(aspect - 1.0) < 0.25, (
        f"SQUARE_IMAGE atlas is not near-square: "
        f"{manifest.width_px}x{manifest.height_px} aspect={aspect:.3f}"
    )
