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
