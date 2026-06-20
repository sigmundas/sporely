import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from ui.image_gallery_widget import ImageGalleryWidget


def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_build_raw_source_badges_marks_raw_backed_derivatives():
    metadata = {
        "raw_processing": {
            "source": {
                "kind": "camera_raw",
                "path": "/tmp/P070020_1.ORF",
                "mime_type": "image/x-raw",
            }
        }
    }

    assert ImageGalleryWidget.build_raw_source_badges(metadata) == ["RAW-derived"]
    assert ImageGalleryWidget.build_raw_source_badges({"raw_processing": {"source": {"kind": "local_derivative"}}}) == []
    assert ImageGalleryWidget.build_raw_source_badges({"image_type": "microscope"}) == []


def test_build_gallery_badges_combines_image_and_raw_badges():
    metadata = {
        "raw_processing": {
            "source": {
                "kind": "camera_raw",
            }
        }
    }

    badges = ImageGalleryWidget.build_gallery_badges(
        image_type="microscope",
        lab_metadata=metadata,
    )

    assert badges[0] == "Micro"
    assert badges[-1] == "RAW-derived"


def test_observation_gallery_rows_include_raw_badges(monkeypatch):
    qapp()

    monkeypatch.setattr(
        "ui.image_gallery_widget.ImageDB.get_images_for_observation",
        lambda observation_id: [
            {
                "id": 101,
                "filepath": "/tmp/source.jpg",
                "image_type": "microscope",
                "lab_metadata": {
                    "raw_processing": {
                        "source": {
                            "kind": "camera_raw",
                        }
                    }
                },
            }
        ],
    )
    monkeypatch.setattr(
        "ui.image_gallery_widget.MeasurementDB.get_measurements_for_observation",
        lambda observation_id: [{"image_id": 101}],
    )

    widget = ImageGalleryWidget("Images")
    widget.set_observation_id(7)

    assert widget._items[0]["badges"] == ["Micro", "RAW-derived"]
    assert widget._items[0]["has_measurements"] is True
