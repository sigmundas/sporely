import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QLabel
from PySide6.QtGui import QImage, QColor

from ui.image_gallery_widget import ImageGalleryWidget
from ui.adaptive_choice_selector import objective_color


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


def test_objective_color_matches_live_lab_palette():
    assert objective_color(None, "4x") == "#e74c3c"
    assert objective_color(None, "10x") == "#f1c40f"
    assert objective_color(None, "40x") == "#3498db"
    assert objective_color(None, "100x") == "#f7f1e5"


def test_thumbnail_widget_renders_microscope_tag_above_raw_badge(qapp, tmp_path):
    widget = ImageGalleryWidget("Images")
    widget.set_items(
        [
            {
                "id": 1,
                "filepath": str(tmp_path / "placeholder.jpg"),
                "badges": ["UNSAVED RAW", "Preview pending"],
                "microscope_tag_text": "63x DIC",
                "microscope_tag_color": "#1f4ea8",
            }
        ]
    )
    widget.resize(220, 180)
    widget.show()
    qapp.processEvents()

    frame = widget._frames[0]
    labels = {
        label.text(): label
        for label in frame.findChildren(QLabel)
        if label.text() in {"63x DIC", "UNSAVED RAW", "Preview pending"}
    }

    assert "63x DIC" in labels
    assert "UNSAVED RAW" in labels
    assert labels["63x DIC"].geometry().y() < labels["UNSAVED RAW"].geometry().y()
    assert labels["63x DIC"].geometry().x() <= labels["UNSAVED RAW"].geometry().x()
    assert "color: #ffffff" in labels["63x DIC"].styleSheet()


def test_thumbnail_widget_renders_light_100x_tag_with_black_text(qapp, tmp_path):
    widget = ImageGalleryWidget("Images")
    widget.set_items(
        [
            {
                "id": 1,
                "filepath": str(tmp_path / "placeholder.jpg"),
                "badges": ["UNSAVED RAW"],
                "microscope_tag_text": "100x DIC",
                "microscope_tag_color": objective_color(None, "100x"),
            }
        ]
    )
    widget.resize(220, 180)
    widget.show()
    qapp.processEvents()

    frame = widget._frames[0]
    labels = {
        label.text(): label
        for label in frame.findChildren(QLabel)
        if label.text() in {"100x DIC", "UNSAVED RAW"}
    }

    assert "100x DIC" in labels
    assert "UNSAVED RAW" in labels
    assert "color: #000000" in labels["100x DIC"].styleSheet()


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

    assert widget._items[0]["badges"] == ["Micro", "(!) needs scale", "RAW-derived"]
    assert widget._items[0]["has_measurements"] is True


def test_observation_gallery_rows_show_cloud_badge_for_uploaded_images(monkeypatch, tmp_path):
    qapp()

    image_path = tmp_path / "cloud-image.png"
    image = QImage(32, 32, QImage.Format_ARGB32)
    image.fill(QColor("#ffffff"))
    assert image.save(str(image_path))

    monkeypatch.setattr(
        "ui.image_gallery_widget.get_image_tombstones_by_deleted_cloud_id",
        lambda cloud_ids: {},
    )

    widget = ImageGalleryWidget("Images")
    widget._set_observation_rows(
        7,
        [
            {
                "id": 101,
                "filepath": str(image_path),
                "image_type": "field",
                "cloud_id": "cloud-image-101",
            }
        ],
        set(),
    )

    assert widget._items[0]["cloud_uploaded"] is True
    assert widget._items[0]["cloud_tombstone_synced"] is False
    cloud_badge = getattr(widget._frames[0], "cloud_badge", None)
    assert cloud_badge is not None
    assert "background-color: transparent" in cloud_badge.styleSheet()
    assert "border-radius" not in cloud_badge.styleSheet()


def test_observation_gallery_rows_hide_cloud_badge_for_synced_tombstones(monkeypatch, tmp_path):
    qapp()

    image_path = tmp_path / "cloud-image.png"
    image = QImage(32, 32, QImage.Format_ARGB32)
    image.fill(QColor("#ffffff"))
    assert image.save(str(image_path))

    monkeypatch.setattr(
        "ui.image_gallery_widget.get_image_tombstones_by_deleted_cloud_id",
        lambda cloud_ids: {
            "cloud-image-101": {
                "deleted_cloud_id": "cloud-image-101",
                "delete_synced_at": "2026-06-01T10:00:00+00:00",
            }
        },
    )

    widget = ImageGalleryWidget("Images")
    widget._set_observation_rows(
        7,
        [
            {
                "id": 101,
                "filepath": str(image_path),
                "image_type": "field",
                "cloud_id": "cloud-image-101",
            }
        ],
        set(),
    )

    assert widget._items[0]["cloud_uploaded"] is False
    assert widget._items[0]["cloud_tombstone_synced"] is True
    assert getattr(widget._frames[0], "cloud_badge", None) is None


def test_publish_selection_defaults_can_start_unchecked_for_microscope_items(tmp_path):
    qapp()

    field_path = tmp_path / "field.png"
    microscope_path = tmp_path / "microscope.png"
    field_image = QImage(24, 24, QImage.Format_ARGB32)
    field_image.fill(QColor("#ffffff"))
    microscope_image = QImage(24, 24, QImage.Format_ARGB32)
    microscope_image.fill(QColor("#ffffff"))
    assert field_image.save(str(field_path))
    assert microscope_image.save(str(microscope_path))

    widget = ImageGalleryWidget("Images", show_publish_checkbox=True)
    widget.set_items(
        [
            {
                "id": 1,
                "filepath": str(field_path),
                "publish_selected_default": True,
            },
            {
                "id": 2,
                "filepath": str(microscope_path),
                "publish_selected_default": False,
            },
        ]
    )

    assert widget.publish_selected_ids() == {1}
    assert widget._items[0]["publish_selected"] is True
    assert widget._items[1]["publish_selected"] is False
