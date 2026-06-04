from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PIL import Image
import pytest
from PySide6.QtWidgets import QApplication

from ui.export_image_dialog import ExportPlotDialog
from ui.observations_tab import ObservationsTab


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_export_plot_dialog_offers_png16_png8_jpeg_and_svg(qapp):
    dialog = ExportPlotDialog(current_dark=False)

    formats = [dialog.format_input.itemData(index) for index in range(dialog.format_input.count())]

    assert formats == ["png16", "png8", "jpg", "svg"]
    assert dialog.format_input.currentData() == "png16"
    assert dialog.quality_input.isEnabled() is False

    dialog.format_input.setCurrentIndex(formats.index("jpg"))

    assert dialog.quality_input.isEnabled() is True
    assert dialog.quality_label.isEnabled() is True


class _FakePublishPlotParent:
    def __init__(self) -> None:
        self.called_with: tuple[int, Path] | None = None

    def export_publish_measure_plot_jpg(self, observation_id: int, out_path: Path | str) -> bool:
        path = Path(out_path)
        self.called_with = (observation_id, path)
        image = Image.new("RGB", (12, 12), (48, 96, 160))
        image.save(path, format="JPEG", quality=90)
        return True


class _FakePublishPlotContext:
    def __init__(self, parent: _FakePublishPlotParent) -> None:
        self._parent = parent

    def _yield_background_sync_ui(self) -> None:
        return None

    def tr(self, text: str) -> str:
        return text

    def window(self):
        return self._parent


def test_publish_measure_plot_uses_jpeg_export(tmp_path, qapp):
    parent = _FakePublishPlotParent()
    ctx = _FakePublishPlotContext(parent)

    out_path = ObservationsTab._generate_publish_measure_plot_image(ctx, 377, tmp_path)

    assert out_path is not None
    assert parent.called_with is not None
    assert parent.called_with[0] == 377
    assert parent.called_with[1].suffix == ".jpg"

    with Image.open(out_path) as exported:
        assert exported.format == "JPEG"
