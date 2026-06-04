from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PIL import Image

import ui.observations_tab as observations_tab
from ui.observations_tab import ObservationsTab


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
    with Image.open(out_path) as exported:
        assert exported.mode in {"RGB", "RGBA"}
        assert exported.mode != "P"
