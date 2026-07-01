import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QRectF, Qt
from PySide6.QtWidgets import QApplication, QFrame, QLabel, QScrollArea, QWidget
from PySide6.QtGui import QImage, QColor

import ui.image_gallery_widget as gallery_module
from ui.image_gallery_widget import ImageGalleryWidget, center_horizontal_scroll_target
from ui.adaptive_choice_selector import objective_color


def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _build_scroll_scene(frame_specs):
    scroll = QScrollArea()
    scroll.setWidgetResizable(True)
    scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
    scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

    container = QWidget()
    container.setMinimumWidth(max(x + width for _, x, width in frame_specs) + 400)
    container.setMinimumHeight(120)
    scroll.setWidget(container)
    scroll.resize(500, 140)
    scroll.show()

    frames = []
    for key, x, width in frame_specs:
        frame = QFrame(container)
        frame.setGeometry(x, 0, width, 120)
        frame.image_key = key
        frames.append(frame)

    return scroll, container, frames


def test_center_horizontal_scroll_target_centers_when_neighbor_is_mostly_hidden():
    viewport_rect = QRectF(300, 0, 500, 120)

    target_rect = QRectF(650, 0, 120, 120)
    previous_rect = QRectF(530, 0, 120, 120)
    next_rect = QRectF(780, 0, 120, 120)
    assert center_horizontal_scroll_target(
        viewport_rect,
        target_rect,
        0,
        2000,
        previous_rect,
        next_rect,
    ) == 460


def test_center_horizontal_scroll_target_centers_when_previous_neighbor_is_mostly_hidden():
    viewport_rect = QRectF(300, 0, 500, 120)

    target_rect = QRectF(330, 0, 120, 120)
    previous_rect = QRectF(200, 0, 120, 120)
    next_rect = QRectF(450, 0, 120, 120)
    assert center_horizontal_scroll_target(
        viewport_rect,
        target_rect,
        0,
        2000,
        previous_rect,
        next_rect,
    ) == 140


def test_center_on_key_ignores_stale_queued_requests(monkeypatch, qapp):
    widget = ImageGalleryWidget("Images")
    scroll, _, frames = _build_scroll_scene(
        [
            (1, 100, 120),
            (2, 650, 120),
            (3, 1200, 120),
        ]
    )

    widget._scroll = scroll
    widget._frames = frames
    widget._items = [{"id": 1}, {"id": 2}, {"id": 3}]
    widget._selected_id = None
    widget._selected_keys = set()

    queued_callbacks = []

    class _FakeTimer:
        @staticmethod
        def singleShot(_msec, callback):
            queued_callbacks.append(callback)

    monkeypatch.setattr(gallery_module, "QTimer", _FakeTimer)

    widget.center_on_key(1)
    widget.center_on_key(3)

    assert len(queued_callbacks) == 2

    expected = center_horizontal_scroll_target(
        QRectF(scroll.horizontalScrollBar().value(), 0, scroll.viewport().width(), scroll.viewport().height()),
        QRectF(frames[2].geometry()),
        scroll.horizontalScrollBar().minimum(),
        scroll.horizontalScrollBar().maximum(),
        QRectF(frames[1].geometry()),
        None,
    )

    queued_callbacks[1]()
    queued_callbacks[0]()

    assert scroll.horizontalScrollBar().value() == expected


def test_select_image_updates_only_previous_and_new_frame_state(monkeypatch, qapp):
    widget = ImageGalleryWidget("Images")
    frames = []
    for key in (1, 2, 3):
        frame = QFrame()
        frame.image_key = key
        frames.append(frame)

    widget._frames = frames
    widget._items = [{"id": 1}, {"id": 2}, {"id": 3}]
    widget._selected_id = 1
    widget._selected_keys = {1}
    widget._last_clicked_index = 0

    state_changes = []
    queued_centers = []
    rebuild_calls = []

    monkeypatch.setattr(
        widget,
        "_set_frame_selected_state",
        lambda frame, selected: state_changes.append((getattr(frame, "image_key", None), bool(selected))),
    )
    monkeypatch.setattr(widget, "_apply_selection_styles", lambda: rebuild_calls.append("_apply_selection_styles"))
    monkeypatch.setattr(widget, "_clear_widgets", lambda: rebuild_calls.append("_clear_widgets"))
    monkeypatch.setattr(widget, "_render_next_batch", lambda: rebuild_calls.append("_render_next_batch"))
    monkeypatch.setattr(widget, "_queue_center_on_key", lambda key: queued_centers.append(key))

    widget.select_image(2)

    assert state_changes == [(1, False), (2, True)]
    assert queued_centers == [2]
    assert rebuild_calls == []
    assert widget._selected_id == 2
    assert widget._selected_keys == {2}


def test_select_image_same_image_is_visual_no_op(monkeypatch, qapp):
    widget = ImageGalleryWidget("Images")
    frame = QFrame()
    frame.image_key = 7
    widget._frames = [frame]
    widget._items = [{"id": 7}]
    widget._selected_id = 7
    widget._selected_keys = {7}
    widget._last_clicked_index = 0

    state_changes = []
    queued_centers = []
    rebuild_calls = []

    monkeypatch.setattr(
        widget,
        "_set_frame_selected_state",
        lambda frame, selected: state_changes.append((getattr(frame, "image_key", None), bool(selected))),
    )
    monkeypatch.setattr(widget, "_apply_selection_styles", lambda: rebuild_calls.append("_apply_selection_styles"))
    monkeypatch.setattr(widget, "_clear_widgets", lambda: rebuild_calls.append("_clear_widgets"))
    monkeypatch.setattr(widget, "_render_next_batch", lambda: rebuild_calls.append("_render_next_batch"))
    monkeypatch.setattr(widget, "_queue_center_on_key", lambda key: queued_centers.append(key))

    widget.select_image(7)

    assert state_changes == []
    assert rebuild_calls == []
    assert queued_centers == [7]
    assert widget._selected_id == 7
    assert widget._selected_keys == {7}


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
