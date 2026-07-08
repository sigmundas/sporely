import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QRectF, Qt
from PySide6.QtWidgets import QApplication, QFrame, QLabel, QScrollArea, QToolButton, QWidget
from PySide6.QtGui import QImage, QColor, QPalette
from PySide6.QtTest import QTest

import ui.image_gallery_widget as gallery_module
from ui.image_gallery_widget import ImageGalleryWidget, center_horizontal_scroll_target, thumbnail_selection_colors
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


def _build_gallery_items(tmp_path, count: int = 3) -> list[dict]:
    items: list[dict] = []
    for index in range(count):
        image_path = tmp_path / f"gallery-{index + 1}.png"
        image = QImage(32, 32, QImage.Format_ARGB32)
        image.fill(QColor.fromHsv((index * 70) % 360, 180, 220))
        assert image.save(str(image_path))
        items.append(
            {
                "id": index + 1,
                "filepath": str(image_path),
                "image_number": index + 1,
            }
        )
    return items


def test_center_horizontal_scroll_target_returns_none_when_item_and_neighbours_visible():
    # Target and both neighbours fully inside the viewport → no scroll.
    viewport_rect = QRectF(300, 0, 700, 120)
    target_rect = QRectF(650, 0, 120, 120)
    previous_rect = QRectF(530, 0, 120, 120)
    next_rect = QRectF(780, 0, 120, 120)  # ends at 900 < viewport_right (1000)
    assert center_horizontal_scroll_target(
        viewport_rect,
        target_rect,
        0,
        2000,
        previous_rect,
        next_rect,
    ) is None


def test_center_horizontal_scroll_target_nudges_left_to_reveal_previous_neighbour():
    # Regression: clicking a thumbnail that's already fully visible but
    # whose previous neighbour is only partially visible should nudge the
    # strip left so the neighbour becomes fully visible + margin.
    viewport_rect = QRectF(300, 0, 500, 120)
    target_rect = QRectF(420, 0, 120, 120)  # fully in [300, 800]
    previous_rect = QRectF(260, 0, 120, 120)  # left edge clipped
    next_rect = QRectF(540, 0, 120, 120)  # fully in view
    # must_left = 260 → target = 260 - margin(24) = 236
    assert center_horizontal_scroll_target(
        viewport_rect,
        target_rect,
        0,
        2000,
        previous_rect,
        next_rect,
    ) == 236


def test_center_horizontal_scroll_target_nudges_right_to_reveal_next_neighbour():
    viewport_rect = QRectF(300, 0, 500, 120)
    target_rect = QRectF(560, 0, 120, 120)  # fully in [300, 800]
    previous_rect = QRectF(440, 0, 120, 120)  # fully in view
    next_rect = QRectF(720, 0, 120, 120)  # ends at 840, clipped
    # must_right = 840 → target = 840 + margin(24) - viewport_width(500) = 364
    assert center_horizontal_scroll_target(
        viewport_rect,
        target_rect,
        0,
        2000,
        previous_rect,
        next_rect,
    ) == 364


def test_center_horizontal_scroll_target_nudges_right_when_item_off_right_no_neighbours():
    viewport_rect = QRectF(300, 0, 500, 120)
    target_rect = QRectF(720, 0, 120, 120)  # extends to x=840, past view right 800
    assert center_horizontal_scroll_target(
        viewport_rect,
        target_rect,
        0,
        2000,
    ) == 720 + 120 + 24 - 500  # 364


def test_center_horizontal_scroll_target_nudges_left_when_item_off_left_no_neighbours():
    viewport_rect = QRectF(300, 0, 500, 120)
    target_rect = QRectF(220, 0, 120, 120)  # left edge is left of viewport
    assert center_horizontal_scroll_target(
        viewport_rect,
        target_rect,
        0,
        2000,
    ) == 220 - 24  # 196


def test_center_horizontal_scroll_target_clamps_to_scrollbar_range():
    viewport_rect = QRectF(0, 0, 500, 120)
    target_rect = QRectF(1900, 0, 120, 120)  # far off to the right
    # Would want 1900+120+24-500=1544, but max is 500 → clamped.
    assert center_horizontal_scroll_target(
        viewport_rect,
        target_rect,
        0,
        500,
    ) == 500


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


def test_select_image_does_not_restyle_frames(monkeypatch, qapp):
    class _SpyFrame(QFrame):
        def __init__(self):
            super().__init__()
            self.style_calls = 0

        def setStyleSheet(self, *args, **kwargs):
            self.style_calls += 1
            return super().setStyleSheet(*args, **kwargs)

    widget_style_calls = []
    original_set_style_sheet = gallery_module.ImageGalleryWidget.setStyleSheet

    def _spy_widget_set_style_sheet(self, *args, **kwargs):
        widget_style_calls.append(args[0] if args else "")
        return original_set_style_sheet(self, *args, **kwargs)

    monkeypatch.setattr(gallery_module.ImageGalleryWidget, "setStyleSheet", _spy_widget_set_style_sheet)

    widget = ImageGalleryWidget("Images")
    widget_style_calls.clear()

    frames = []
    for key in (1, 2, 3):
        frame = _SpyFrame()
        frame.image_key = key
        frames.append(frame)

    widget._frames = frames
    widget._items = [{"id": 1}, {"id": 2}, {"id": 3}]
    widget._selected_id = 1
    widget._selected_keys = {1}
    widget._last_clicked_index = 0

    monkeypatch.setattr(widget, "_queue_center_on_key", lambda key: None)

    widget.select_image(2)

    assert [frame.style_calls for frame in frames] == [0, 0, 0]
    assert widget_style_calls == []


def test_right_click_on_unselected_thumbnail_selects_only_that_item_before_menu_action(monkeypatch, qapp, tmp_path):
    widget = ImageGalleryWidget("Images")
    widget.set_multi_select(True)
    items = _build_gallery_items(tmp_path, 3)
    widget.set_items(items)
    widget.select_paths([items[0]["filepath"], items[1]["filepath"]])
    widget.resize(420, 180)
    widget.show()
    qapp.processEvents()

    seen = {}

    monkeypatch.setattr(
        widget,
        "_show_thumbnail_context_menu",
        lambda frame, global_pos: (
            widget._set_context_menu_selection(frame),
            seen.update(selected_keys=widget.selected_keys()),
        ),
    )

    frame = widget._frames[2]
    QTest.mouseClick(frame, Qt.RightButton, pos=frame.rect().center())
    qapp.processEvents()

    assert seen["selected_keys"] == {3}
    assert widget.selected_keys() == {3}


def test_right_click_on_selected_thumbnail_preserves_multi_selection(monkeypatch, qapp, tmp_path):
    widget = ImageGalleryWidget("Images")
    widget.set_multi_select(True)
    items = _build_gallery_items(tmp_path, 3)
    widget.set_items(items)
    widget.select_paths([items[0]["filepath"], items[1]["filepath"]])
    widget.resize(420, 180)
    widget.show()
    qapp.processEvents()

    seen = {}

    monkeypatch.setattr(
        widget,
        "_show_thumbnail_context_menu",
        lambda frame, global_pos: (
            widget._set_context_menu_selection(frame),
            seen.update(selected_keys=widget.selected_keys()),
        ),
    )

    frame = widget._frames[1]
    QTest.mouseClick(frame, Qt.RightButton, pos=frame.rect().center())
    qapp.processEvents()

    assert seen["selected_keys"] == {1, 2}
    assert widget.selected_keys() == {1, 2}


def test_delete_menu_emits_all_selected_keys_for_multiselect(monkeypatch, qapp, tmp_path):
    widget = ImageGalleryWidget("Images")
    widget.set_multi_select(True)
    items = _build_gallery_items(tmp_path, 3)
    widget.set_items(items)
    widget.select_paths([items[0]["filepath"], items[1]["filepath"]])
    widget.resize(420, 180)
    widget.show()
    qapp.processEvents()

    deleted_keys: list[list[object]] = []
    widget.deleteSelectionRequested.connect(lambda keys: deleted_keys.append(list(keys)))

    monkeypatch.setattr(
        widget,
        "_show_thumbnail_context_menu",
        lambda frame, global_pos: (
            widget._set_context_menu_selection(frame),
            widget.deleteSelectionRequested.emit(widget.selected_image_keys()),
        ),
    )

    frame = widget._frames[0]
    QTest.mouseClick(frame, Qt.RightButton, pos=frame.rect().center())
    qapp.processEvents()

    assert deleted_keys == [[1, 2]]
    assert widget.selected_keys() == {1, 2}


def test_edit_menu_emits_clicked_item_path(monkeypatch, qapp, tmp_path):
    widget = ImageGalleryWidget("Images", show_edit=True)
    widget.set_multi_select(True)
    items = _build_gallery_items(tmp_path, 3)
    widget.set_items(items)
    widget.select_paths([items[0]["filepath"], items[1]["filepath"]])

    edits: list[tuple[object, str]] = []
    widget.editRequested.connect(lambda image_id, path: edits.append((image_id, path)))

    class _FakeMenu:
        def __init__(self, parent=None):
            self._actions = []

        def addAction(self, text):
            action = SimpleNamespace(text=lambda: text)
            self._actions.append(action)
            return action

        def actions(self):
            return list(self._actions)

        def exec(self, global_pos):
            for action in self._actions:
                if action.text() == "Edit photo":
                    return action
            return None

    monkeypatch.setattr(gallery_module, "QMenu", _FakeMenu)

    frame = widget._frames[2]
    widget._show_thumbnail_context_menu(frame, frame.mapToGlobal(frame.rect().center()))

    assert edits == [(3, items[2]["filepath"])]
    assert widget.selected_keys() == {3}


def test_raw_source_badge_uses_raw_label():
    badges = ImageGalleryWidget.build_raw_source_badges(
        {"raw_processing": {"source": {"kind": "camera_raw"}}},
        translate=lambda text: text,
    )

    assert badges == ["From raw"]


def test_existing_thumbnail_delete_button_still_emits_single_key(qapp, tmp_path):
    widget = ImageGalleryWidget("Images")
    items = _build_gallery_items(tmp_path, 1)
    widget.set_items(items)
    widget.resize(220, 180)
    widget.show()
    qapp.processEvents()

    deleted_keys: list[object] = []
    widget.deleteRequested.connect(deleted_keys.append)

    delete_btn = None
    for candidate in widget._frames[0].findChildren(QToolButton):
        if candidate.text() == "X":
            delete_btn = candidate
            break
    assert delete_btn is not None

    QTest.mouseClick(delete_btn, Qt.LeftButton)
    qapp.processEvents()

    assert deleted_keys == [1]


def test_thumbnail_selection_overlay_tracks_resized_frame(monkeypatch, qapp):
    widget = ImageGalleryWidget("Images")
    frame = QFrame()
    frame.image_key = 1
    frame._thumbnail_selected = True
    frame._thumbnail_hovered = False
    frame.raw_halo_color = None
    frame.thumb_label = QLabel(frame)
    frame.thumb_label.setFixedSize(80, 80)
    overlay = QWidget(frame)
    overlay.setGeometry(0, 0, 80, 80)
    frame._thumbnail_selection_overlay = overlay
    widget._frames = [frame]
    widget._thumb_size = 80
    widget._base_thumb_size = 80
    widget._min_thumb_size = 80
    widget._scroll.resize(160, 120)

    monkeypatch.setattr(widget, "_target_thumb_size", lambda: 120)

    widget._update_thumbnail_sizes()

    assert frame.size().width() == 120
    assert frame.size().height() == 120
    assert overlay.geometry() == frame.rect()


def test_thumbnail_selection_colors_are_theme_aware():
    light_palette = QPalette()
    light_palette.setColor(QPalette.Window, QColor("#ffffff"))
    light_palette.setColor(QPalette.WindowText, QColor("#1e293b"))
    light_palette.setColor(QPalette.Highlight, QColor("#dbeafe"))

    dark_palette = QPalette()
    dark_palette.setColor(QPalette.Window, QColor("#131313"))
    dark_palette.setColor(QPalette.WindowText, QColor("#e8e8e8"))
    dark_palette.setColor(QPalette.Highlight, QColor("#3d5a52"))

    light = thumbnail_selection_colors(False, light_palette)
    dark = thumbnail_selection_colors(True, dark_palette)

    assert dark.outer.lightness() > light.outer.lightness()
    assert dark.fill.alpha() > light.fill.alpha()
    assert light.inner.name() == "#3498db"
    assert dark.inner.name() == "#3498db"
    assert light.badge_text.name() == "#ffffff"
    assert dark.badge_text.name() == "#ffffff"


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

    assert ImageGalleryWidget.build_raw_source_badges(metadata) == ["From raw"]
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
    assert badges[-1] == "From raw"


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
                "badges": ["From raw", "Preview pending"],
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
        if label.text() in {"63x DIC", "From raw", "Preview pending"}
    }

    assert "63x DIC" in labels
    assert "From raw" in labels
    assert labels["63x DIC"].geometry().y() < labels["From raw"].geometry().y()
    assert labels["63x DIC"].geometry().x() <= labels["From raw"].geometry().x()
    assert "color: #ffffff" in labels["63x DIC"].styleSheet()


def test_thumbnail_widget_renders_light_100x_tag_with_black_text(qapp, tmp_path):
    widget = ImageGalleryWidget("Images")
    widget.set_items(
        [
            {
                "id": 1,
                "filepath": str(tmp_path / "placeholder.jpg"),
                "badges": ["From raw"],
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
        if label.text() in {"100x DIC", "From raw"}
    }

    assert "100x DIC" in labels
    assert "From raw" in labels
    assert "color: #000000" in labels["100x DIC"].styleSheet()


def test_thumbnail_widget_renders_raw_badge_below_objective_badge(qapp, tmp_path):
    widget = ImageGalleryWidget("Images")
    widget.set_items(
        [
            {
                "id": 1,
                "filepath": str(tmp_path / "placeholder.jpg"),
                "badges": ["20X BR", "From raw"],
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
        if label.text() in {"20X BR", "From raw"}
    }

    assert "20X BR" in labels
    assert "From raw" in labels
    assert labels["20X BR"].geometry().y() < labels["From raw"].geometry().y()


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

    assert widget._items[0]["badges"] == ["Micro", "(!) needs scale", "From raw"]
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
