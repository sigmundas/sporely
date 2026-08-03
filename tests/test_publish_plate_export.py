from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PIL import Image
from PySide6.QtCore import QSettings
from PySide6.QtWidgets import QApplication

import ui.observations_tab as observations_tab
import ui.species_plate_dialog as species_plate_dialog
from ui.observations_tab import ObservationsTab


def _qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class _FakePublishContext:
    def _yield_background_sync_ui(self) -> None:
        return None

    def tr(self, text: str) -> str:
        return text

    def _publish_excluded_image_ids(self, observation_id: int) -> set[int]:
        return set()

    def _quantize_png8(self, path) -> None:
        raise AssertionError("Plate exports should stay full-color PNGs.")


def test_publish_gallery_mosaic_uniform_tile_dims_despite_legacy_uniform_scale_false(
    monkeypatch, tmp_path,
):
    """Phase 1.1 migration behaviour: even when persisted settings still
    carry `uniform_scale=False`, the publish plate uses `plan_mosaic`
    → every tile in the composite must share identical output pixel
    dimensions.  The legacy `uniform_length_px` clamp is gone; the
    setting is silently dropped."""
    _qapp()
    import ui.main_window as main_window
    from utils.spore_mosaic_render import (
        MosaicAnnotationSpec,
        MosaicGridPolicy,
        SporeMosaicSource,
        plan_mosaic,
    )

    src = tmp_path / "src.png"
    Image.new("RGB", (800, 800), (120, 120, 120)).save(src, format="PNG")

    measurements = [
        {
            "id": 1, "image_id": 11, "image_filepath": str(src),
            "length_um": 10.0, "width_um": 4.0, "gallery_rotation": 0,
            "p1_x": 400.0, "p1_y": 450.0, "p2_x": 400.0, "p2_y": 350.0,
            "p3_x": 380.0, "p3_y": 400.0, "p4_x": 420.0, "p4_y": 400.0,
        },
        {
            "id": 2, "image_id": 12, "image_filepath": str(src),
            "length_um": 14.0, "width_um": 6.0, "gallery_rotation": 0,
            "p1_x": 400.0, "p1_y": 470.0, "p2_x": 400.0, "p2_y": 330.0,
            "p3_x": 370.0, "p3_y": 400.0, "p4_x": 430.0, "p4_y": 400.0,
        },
        {
            "id": 3, "image_id": 13, "image_filepath": str(src),
            "length_um": 8.0, "width_um": 3.0, "gallery_rotation": 0,
            "p1_x": 400.0, "p1_y": 440.0, "p2_x": 400.0, "p2_y": 360.0,
            "p3_x": 385.0, "p3_y": 400.0, "p4_x": 415.0, "p4_y": 400.0,
        },
    ]

    prepared = (
        {"orient": True, "uniform_scale": False, "measurement_type": "spores"},
        measurements,
        # image rows keyed by image_id — no per-image µm/px so the
        # planner uses the endpoint-derived scale fallback.
        {11: {}, 12: {}, 13: {}},
        {
            "thumbnail_size": 160,
            "rectangle_style": "b",
            "rectangle_thickness": 2.0,
        },
    )

    # Build minimal window that create_spore_thumbnail depends on.
    class _DummyAvail:
        def __init__(self, *args, **kwargs): pass

    monkeypatch.setattr(main_window, "SpeciesDataAvailability", _DummyAvail)
    monkeypatch.setattr(main_window.MainWindow, "_apply_theme", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_populate_scale_combo", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "load_default_objective", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_restore_geometry", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "init_ui", lambda self: None)
    window = main_window.MainWindow()
    window.default_measure_color = None
    from PySide6.QtGui import QColor
    window.default_measure_color = QColor("#0044aa")

    ctx = _FakePublishContext()
    ctx.window = lambda: window
    ctx._normalize_publish_measurement_category = lambda category: "spores"
    ctx._prepare_publish_mosaic_inputs = lambda observation_id: prepared

    # Sanity-check the planner directly first: three tiles all share
    # the same output_w × output_h.
    sources = [
        SporeMosaicSource(
            item_id=int(m["id"]),
            source_path=src,
            source_width=800, source_height=800,
            p1_x=m["p1_x"], p1_y=m["p1_y"],
            p2_x=m["p2_x"], p2_y=m["p2_y"],
            p3_x=m["p3_x"], p3_y=m["p3_y"],
            p4_x=m["p4_x"], p4_y=m["p4_y"],
            length_um=m["length_um"], width_um=m["width_um"],
        )
        for m in measurements
    ]
    layout = plan_mosaic(
        sources, orient=True,
        grid_policy=MosaicGridPolicy.ASPECT_4_3,
        output_tile_height_px=160,
        annotation=MosaicAnnotationSpec(
            draw_rectangle=True, draw_dimensions=True, rectangle_style="b",
        ),
    ).layout
    assert layout is not None
    widths = {cell.tile.output_w_px for cell in layout.cells}
    heights = {cell.tile.output_h_px for cell in layout.cells}
    assert widths == {layout.tile_width_px}
    assert heights == {layout.tile_height_px}

    # Now run the actual publish plate generator; the composite width
    # equals cols * tile_w and height equals rows * tile_h.
    out_path = ObservationsTab._generate_publish_gallery_mosaic_image(
        ctx, 99, tmp_path, prepared=prepared,
    )
    assert out_path is not None
    with Image.open(out_path) as decoded:
        assert decoded.width == layout.mosaic_width_px
        assert decoded.height == layout.mosaic_height_px


def test_publish_plate_export_keeps_full_color_png(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        observations_tab.ObservationDB,
        "get_observation",
        lambda observation_id: {
            "id": observation_id,
            "genus": "Leratiomyces",
            "species": "percevalii",
            "common_name": "flisskurvehatt",
        },
    )

    def fake_export_observation_plate_image(observation, path, excluded_image_ids=None):
        captured["excluded_image_ids"] = excluded_image_ids
        image = Image.new("RGB", (12, 12))
        for x in range(12):
            for y in range(12):
                image.putpixel((x, y), (x * 20 % 256, y * 20 % 256, (x + y) * 10 % 256))
        image.save(path, format="PNG")
        return True

    monkeypatch.setattr(
        "ui.species_plate_dialog.export_observation_plate_image",
        fake_export_observation_plate_image,
    )

    ctx = _FakePublishContext()
    out_path = ObservationsTab._generate_publish_plate_image(ctx, 377, tmp_path)

    assert out_path is not None
    assert captured["excluded_image_ids"] is None
    with Image.open(out_path) as exported:
        assert exported.mode in {"RGB", "RGBA"}
        assert exported.mode != "P"


def test_species_plate_publish_selection_does_not_mutate_cloud_state(monkeypatch):
    def _unexpected_cloud_mutation(*_args, **_kwargs):
        raise AssertionError("external selection must not mutate cloud state")

    for method_name in (
        "queue_image_tombstone_for_local_image",
        "clear_image_tombstone_by_deleted_cloud_id",
        "clear_image_cloud_sync_state",
    ):
        monkeypatch.setattr(
            species_plate_dialog.ImageDB,
            method_name,
            _unexpected_cloud_mutation,
        )

    excluded_calls = []
    dialog = SimpleNamespace(
        _all_images=[
            {"id": 1, "cloud_id": "cloud-1"},
            {"id": 2, "cloud_id": "cloud-2"},
        ],
        _set_publish_excluded_image_ids=lambda excluded: excluded_calls.append(
            set(excluded)
        ),
    )

    species_plate_dialog.SpeciesPlateDialog._on_gallery_publish_selection_changed(
        dialog,
        {2},
    )

    assert excluded_calls == [{1}]


def test_species_plate_saved_empty_slot_stays_empty_on_reopen(monkeypatch, tmp_path):
    _qapp()
    org = "SporelyTestPlateEmptySlot"
    monkeypatch.setattr(species_plate_dialog, "SETTINGS_ORG", org)
    settings = QSettings(org, "SpeciesPlate")
    settings.clear()

    image_paths = []
    for idx in range(1, 4):
        path = tmp_path / f"micro_{idx}.jpg"
        Image.new("RGB", (20, 20), (idx * 30, idx * 30, idx * 30)).save(path)
        image_paths.append(path)

    monkeypatch.setattr(
        species_plate_dialog.ImageDB,
        "get_images_for_observation",
        lambda observation_id: [
            {"id": 1, "filepath": str(image_paths[0]), "image_type": "microscope", "sample_type": "spores"},
            {"id": 2, "filepath": str(image_paths[1]), "image_type": "microscope", "sample_type": "basidia"},
            {"id": 3, "filepath": str(image_paths[2]), "image_type": "microscope", "sample_type": "cheilocystidia"},
        ],
    )
    monkeypatch.setattr(
        species_plate_dialog.MeasurementDB,
        "get_measurements_for_observation",
        lambda observation_id: [],
    )
    monkeypatch.setattr(species_plate_dialog.SettingsDB, "get_profile", lambda: {})
    monkeypatch.setattr(
        species_plate_dialog.SettingsDB,
        "get_setting",
        lambda key, default=None: "[2]" if key == "artsobs_publish_excluded_image_ids_91" else default,
    )

    obs = {"id": 91, "genus": "Agaricus", "species": "campestris"}
    dialog = species_plate_dialog.SpeciesPlateDialog(obs)
    try:
        gallery_by_id = {item["id"]: item for item in dialog._gallery._items}
        assert set(gallery_by_id) == {1, 2, 3}
        assert gallery_by_id[2]["publish_selected"] is False
        dialog._slot_images["BR"] = None
        dialog.reject()
    finally:
        dialog.deleteLater()

    reopened = species_plate_dialog.SpeciesPlateDialog(obs)
    try:
        assert reopened._slot_images["BR"] is None
    finally:
        reopened.reject()
        reopened.deleteLater()
        settings.clear()
