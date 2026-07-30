from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from PIL import Image

import ui.observations_tab as observations_tab
from ui.main_window import MainWindow
from ui.observations_tab import ObservationsTab
from utils.publish_media import (
    ANNOTATED_IMAGE_RENDERER_VERSION,
    MOSAIC_RENDERER_VERSION,
    PublishMediaBundle,
    annotated_image_dependencies,
    mosaic_dependencies,
    prepare_ordered_mosaic_inputs,
)
from utils.publish_media_cache import PublishMediaCache, publish_media_signature


def _measurement(
    measurement_id: int,
    image_id: int,
    *,
    length: float = 10.0,
    width: float = 5.0,
    rotation: int = 0,
    path: str = "",
) -> dict:
    return {
        "id": measurement_id,
        "image_id": image_id,
        "measurement_type": "spores",
        "length_um": length,
        "width_um": width,
        "gallery_rotation": rotation,
        "image_filepath": path,
        "p1_x": 1.0,
        "p1_y": 2.0,
        "p2_x": 11.0,
        "p2_y": 2.0,
        "p3_x": 1.0,
        "p3_y": 7.0,
        "p4_x": 11.0,
        "p4_y": 7.0,
    }


def _write_png(path: Path, color=(10, 20, 30)) -> None:
    Image.new("RGB", (12, 8), color).save(path, "PNG")


def test_canonical_mosaic_order_has_stable_tie_breaking():
    rows = [
        _measurement(9, 2, length=8),
        _measurement(3, 1, length=8),
        _measurement(2, 1, length=8),
    ]

    first = prepare_ordered_mosaic_inputs(
        rows,
        category="spores",
        sort_key="length",
        image_order=[2, 1],
    )
    second = prepare_ordered_mosaic_inputs(
        reversed(rows),
        category="spores",
        sort_key="length",
        image_order=[2, 1],
    )

    assert [row["id"] for row in first] == [2, 3, 9]
    assert [row["id"] for row in second] == [2, 3, 9]


def test_analysis_and_publish_use_same_ordered_mosaic_inputs(monkeypatch, tmp_path):
    source = tmp_path / "source.png"
    _write_png(source)
    rows = [
        _measurement(4, 20, width=7, path=str(source)),
        _measurement(2, 10, width=4, path=str(source)),
        _measurement(3, 20, width=4, path=str(source)),
    ]
    images = [
        {"id": 20, "filepath": str(source)},
        {"id": 10, "filepath": str(source)},
    ]
    combo = SimpleNamespace(currentData=lambda: "width")
    analysis = SimpleNamespace(
        gallery_sort_combo=combo,
        active_observation_id=77,
    )
    monkeypatch.setattr(
        observations_tab.MeasurementDB,
        "get_measurements_for_observation",
        lambda _observation_id: list(rows),
    )
    monkeypatch.setattr(
        observations_tab.ImageDB,
        "get_images_for_observation",
        lambda _observation_id: list(images),
    )
    import ui.main_window as main_window

    monkeypatch.setattr(
        main_window.ImageDB,
        "get_images_for_observation",
        lambda _observation_id: list(images),
    )
    publish = SimpleNamespace(
        window=lambda: SimpleNamespace(
            _gallery_thumbnail_size=lambda: 200,
            _current_measure_rectangle_style=lambda: "a",
            _current_measure_rectangle_thickness=lambda: 1,
        ),
        _load_gallery_settings_for_observation=lambda _observation_id: {
            "measurement_type": "spores",
            "gallery_sort": "width",
            "orient": True,
            "uniform_scale": False,
        },
    )

    analysis_rows = MainWindow._sort_gallery_measurements(analysis, rows)
    _settings, publish_rows, _images, _render = (
        ObservationsTab._prepare_publish_mosaic_inputs(publish, 77)
    )

    assert [row["id"] for row in analysis_rows] == [2, 3, 4]
    assert [row["id"] for row in publish_rows] == [2, 3, 4]

    publish._publish_excluded_image_ids = lambda _observation_id: {20}
    _settings, excluded_rows, _images, _render = (
        ObservationsTab._prepare_publish_mosaic_inputs(publish, 77)
    )
    assert [row["id"] for row in excluded_rows] == [2]


def test_mosaic_signature_tracks_relevant_inputs_but_not_scale_bar(tmp_path):
    source = tmp_path / "source.png"
    _write_png(source)
    row = _measurement(3, 8, path=str(source))
    image_rows = {
        8: {
            "id": 8,
            "filepath": str(source),
            "scale_microns_per_pixel": 0.2,
            "measure_color": "#123456",
        }
    }
    settings = {
        "measurement_type": "spores",
        "gallery_sort": "length",
        "orient": True,
        "uniform_scale": False,
        "show_scale_bar": False,
    }
    render = {"thumbnail_size": 200, "format": "png"}

    base = mosaic_dependencies(
        observation_id=11,
        measurements=[row],
        image_rows=image_rows,
        settings=settings,
        render_options=render,
    )
    scale_changed = mosaic_dependencies(
        observation_id=11,
        measurements=[row],
        image_rows=image_rows,
        settings={**settings, "show_scale_bar": True, "scale_bar_um": 50},
        render_options=render,
    )
    rotated = mosaic_dependencies(
        observation_id=11,
        measurements=[{**row, "gallery_rotation": 90}],
        image_rows=image_rows,
        settings=settings,
        render_options=render,
    )

    base_signature = publish_media_signature(
        "mosaic", MOSAIC_RENDERER_VERSION, base
    )
    assert base_signature == publish_media_signature(
        "mosaic", MOSAIC_RENDERER_VERSION, scale_changed
    )
    assert base_signature != publish_media_signature(
        "mosaic", MOSAIC_RENDERER_VERSION, rotated
    )


def test_annotated_signature_tracks_scale_bar_and_measurements(tmp_path):
    source = tmp_path / "source.png"
    _write_png(source)
    image_row = {"id": 8, "scale_microns_per_pixel": 0.2}
    row = _measurement(3, 8, path=str(source))
    preferences = {
        "show_overlays": True,
        "show_labels": True,
        "show_scale_bar": True,
        "scale_bar_um": 10,
    }
    render = {"format": "jpeg", "quality": 92}
    base = annotated_image_dependencies(
        image_row=image_row,
        source_path=source,
        measurements=[row],
        preferences=preferences,
        render_options=render,
    )
    changed = annotated_image_dependencies(
        image_row=image_row,
        source_path=source,
        measurements=[{**row, "p2_x": 12.0}],
        preferences={**preferences, "scale_bar_um": 20},
        render_options=render,
    )

    assert publish_media_signature(
        "annotated", ANNOTATED_IMAGE_RENDERER_VERSION, base
    ) != publish_media_signature(
        "annotated", ANNOTATED_IMAGE_RENDERER_VERSION, changed
    )


def test_bundle_reuses_mosaic_within_operation_and_across_instances(tmp_path):
    cache_root = tmp_path / "cache"
    calls = []

    def render(destination: Path):
        calls.append(destination)
        _write_png(destination)
        return True

    dependencies = {"observation_id": 5, "measurements": [{"id": 1}]}
    with PublishMediaBundle(
        5,
        cache=PublishMediaCache(cache_root),
    ) as first_bundle:
        first = first_bundle.resolve_cached_image(
            asset_kind="mosaic",
            renderer_version=MOSAIC_RENDERER_VERSION,
            dependencies=dependencies,
            extension="png",
            render=render,
        )
        second = first_bundle.resolve_cached_image(
            asset_kind="mosaic",
            renderer_version=MOSAIC_RENDERER_VERSION,
            dependencies=dependencies,
            extension="png",
            render=render,
        )
        operation_dir = first_bundle.operation_dir

    assert first.path == second.path
    assert len(calls) == 1
    assert not operation_dir.exists()
    assert first.path.exists()

    with PublishMediaBundle(
        5,
        cache=PublishMediaCache(cache_root),
    ) as later_bundle:
        later = later_bundle.resolve_cached_image(
            asset_kind="mosaic",
            renderer_version=MOSAIC_RENDERER_VERSION,
            dependencies=dependencies,
            extension="png",
            render=render,
        )

    assert later.path == first.path
    assert later.from_cache is True
    assert len(calls) == 1


def test_cache_cleanup_runs_once_per_session_root_and_is_non_fatal(tmp_path):
    cleanup_calls = []

    class TrackingCache(PublishMediaCache):
        def cleanup(self, **kwargs):
            cleanup_calls.append(self.root)
            return {"assets": 0, "temporary": 0}

    root = tmp_path / "tracked-cache"
    PublishMediaBundle(1, cache=TrackingCache(root))
    PublishMediaBundle(2, cache=TrackingCache(root))
    assert cleanup_calls == [root]

    class BrokenCleanupCache(PublishMediaCache):
        def cleanup(self, **kwargs):
            raise PermissionError("cache is read-only")

    # Maintenance failure must not prevent constructing or using a bundle.
    broken = PublishMediaBundle(
        3,
        cache=BrokenCleanupCache(tmp_path / "broken-cache"),
    )
    assert broken.observation_id == 3


def test_two_target_preparation_survives_first_failure_and_renders_mosaic_once(
    tmp_path,
):
    source = tmp_path / "source.png"
    _write_png(source)
    row = _measurement(1, 2, path=str(source))
    images = {
        2: {
            "id": 2,
            "filepath": str(source),
            "scale_microns_per_pixel": 0.25,
        }
    }
    settings = {
        "measurement_type": "spores",
        "gallery_sort": "length",
        "orient": True,
        "uniform_scale": False,
    }
    render_options = {
        "thumbnail_size": 200,
        "rectangle_style": "a",
        "rectangle_thickness": 1,
        "format": "png",
    }
    render_calls = []

    def render_mosaic(
        _observation_id,
        _temp_dir,
        progress_cb=None,
        cancel_cb=None,
        *,
        output_path=None,
        prepared=None,
    ):
        render_calls.append(prepared)
        _write_png(Path(output_path))
        return str(output_path)

    fake = SimpleNamespace(
        tr=lambda text: text,
        _publish_render_preferences=lambda: {"show_scale_bar": False},
        _prepare_publish_mosaic_inputs=lambda _observation_id: (
            settings,
            [row],
            images,
            render_options,
        ),
        _generate_publish_gallery_mosaic_image=render_mosaic,
    )
    bundle = PublishMediaBundle(
        9,
        cache=PublishMediaCache(tmp_path / "cache"),
    )
    with bundle:
        first, first_temp, _warnings = (
            ObservationsTab._prepare_publish_media_assets(
                fake,
                observation_id=9,
                base_image_paths=[str(source)],
                include_annotations=False,
                include_measure_plots=False,
                include_thumbnail_gallery=True,
                include_plate=False,
                include_copyright=False,
                publish_bundle=bundle,
            )
        )
        try:
            raise RuntimeError("first target failed")
        except RuntimeError:
            pass
        second, second_temp, _warnings = (
            ObservationsTab._prepare_publish_media_assets(
                fake,
                observation_id=9,
                base_image_paths=[str(source)],
                include_annotations=False,
                include_measure_plots=False,
                include_thumbnail_gallery=True,
                include_plate=False,
                include_copyright=False,
                publish_bundle=bundle,
            )
        )

    assert len(render_calls) == 1
    assert first[-1] == second[-1]
    assert Path(first[-1]).exists()
    assert first_temp is None
    assert second_temp is None


def test_bundle_cleans_temporary_files_after_exception_but_keeps_cache(tmp_path):
    cache = PublishMediaCache(tmp_path / "cache")
    bundle = PublishMediaBundle(6, cache=cache)
    cached_path = None
    operation_dir = None
    try:
        with bundle:
            resolved = bundle.resolve_cached_image(
                asset_kind="mosaic",
                renderer_version=MOSAIC_RENDERER_VERSION,
                dependencies={"observation_id": 6},
                extension="png",
                render=lambda destination: _write_png(destination) or True,
            )
            cached_path = resolved.path
            operation_dir = bundle.operation_dir
            bundle.temporary_path("target.jpg").write_bytes(b"temporary")
            raise RuntimeError("target failed")
    except RuntimeError:
        pass

    assert operation_dir is not None and not operation_dir.exists()
    assert cached_path is not None and cached_path.exists()


def test_corrupt_or_missing_cached_mosaic_is_regenerated(tmp_path):
    cache = PublishMediaCache(tmp_path / "cache")
    dependencies = {"observation_id": 8}
    calls = []

    def render(destination: Path):
        calls.append(destination)
        _write_png(destination, color=(len(calls), 2, 3))
        return True

    with PublishMediaBundle(8, cache=cache) as bundle:
        first = bundle.resolve_cached_image(
            asset_kind="mosaic",
            renderer_version=MOSAIC_RENDERER_VERSION,
            dependencies=dependencies,
            extension="png",
            render=render,
        )
    first.path.write_bytes(b"corrupt")

    with PublishMediaBundle(8, cache=cache) as restarted:
        regenerated = restarted.resolve_cached_image(
            asset_kind="mosaic",
            renderer_version=MOSAIC_RENDERER_VERSION,
            dependencies=dependencies,
            extension="png",
            render=render,
        )

    assert regenerated.path == first.path
    assert len(calls) == 2

    regenerated.path.unlink()
    with PublishMediaBundle(8, cache=cache) as restarted_again:
        missing_regenerated = restarted_again.resolve_cached_image(
            asset_kind="mosaic",
            renderer_version=MOSAIC_RENDERER_VERSION,
            dependencies=dependencies,
            extension="png",
            render=render,
        )

    assert missing_regenerated.path.exists()
    assert len(calls) == 3


def test_disabled_derived_media_does_not_call_renderers():
    fake = SimpleNamespace(
        _publish_render_preferences=lambda: {"show_scale_bar": False},
    )

    paths, temp_dir, warnings = ObservationsTab._prepare_publish_media_assets(
        fake,
        observation_id=3,
        base_image_paths=["source.jpg"],
        include_annotations=False,
        include_measure_plots=False,
        include_thumbnail_gallery=False,
        include_plate=False,
        include_copyright=False,
    )

    assert paths == ["source.jpg"]
    assert temp_dir is None
    assert warnings == []
