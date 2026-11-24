"""One image strip shared by Vis and Mål (redesign stage 9c).

Real ``MainWindow`` in an isolated profile under ``tmp_path``; no network.
The strip is one widget that moves to the page shown and keeps its
selection, scroll and height; its signals reach only that page; a page that
does not hold it never writes to it, and reloads it when it takes it back.
Unsaved (staged) images are kept per observation and shown first.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt

from database.models import ImageDB, ObservationDB
from tools.review_ui.live_lab_harness import FakeRawpy, create_observation, isolation_patches, prepare_profile
from ui.styles import STAGING_BORDER

IMAGES = 12


@pytest.fixture
def profile(tmp_path, monkeypatch, qtbot):
    rawpy = FakeRawpy()
    for owner, attribute, value in isolation_patches(tmp_path, rawpy):
        monkeypatch.setattr(owner, attribute, value)
    yield prepare_profile(tmp_path, rawpy)


@pytest.fixture
def window(qtbot, profile):
    from ui.main_window import MainWindow

    first = create_observation(profile)
    image_ids = [
        ImageDB.add_image(first, str(profile.jpeg(f"img{n}.JPG", seed=n)), "microscope", scale=0.05, objective_name="63X")
        for n in range(IMAGES)
    ]
    other = ObservationDB.create_observation(date="2026-10-20 10:00", genus="Fomes", species="fomentarius")
    other_image = ImageDB.add_image(other, str(profile.jpeg("other.JPG", seed=40)), "field")
    window = MainWindow()
    qtbot.addWidget(window)
    window.resize(1400, 900)
    window.show()
    qtbot.waitExposed(window)
    tab = window.observations_tab
    qtbot.waitUntil(lambda: tab.table.rowCount() == 2, timeout=5000)
    tab.table.selectRow(tab._find_table_row_for_observation(first))
    qtbot.waitUntil(lambda: tab.gallery_widget.shown_observation_id() == first, timeout=5000)
    window.ids_for_test = {"first": first, "images": image_ids, "other": other, "other_image": other_image}
    return window


def _strip(window):
    return window.observations_tab.gallery_widget


def _to(qtbot, window, page):
    window.tab_widget.setCurrentWidget(getattr(window, page))
    qtbot.wait(100)


def test_one_strip_moves_to_the_page_shown(qtbot, window):
    strip = _strip(window)
    assert window.measure_gallery is strip
    assert window.observations_tab.gallery_slot.isAncestorOf(strip)
    _to(qtbot, window, "measure_tab")
    assert window.measure_strip_slot.isAncestorOf(strip)
    assert strip.isVisible()
    _to(qtbot, window, "observations_tab")
    assert window.observations_tab.gallery_slot.isAncestorOf(strip)
    assert strip.isVisible()


def test_the_image_chosen_in_view_is_the_one_shown_in_measure(qtbot, window):
    strip = _strip(window)
    chosen = window.ids_for_test["images"][4]
    frame = strip._frames[[item.get("id") for item in strip._items].index(chosen)]
    qtbot.mouseClick(frame, Qt.LeftButton)
    assert strip.selected_keys() == {chosen}
    _to(qtbot, window, "measure_tab")
    qtbot.waitUntil(lambda: window.current_image_id == chosen, timeout=3000)
    assert strip.selected_keys() == {chosen}


def test_a_click_reaches_only_the_page_shown(qtbot, window):
    strip = _strip(window)
    heard = {"view": [], "measure": []}
    for key in heard:
        strip.channel(key).imageClicked.connect(lambda image_id, _path, key=key: heard[key].append(image_id))
    image_id = window.ids_for_test["images"][2]
    strip.imageClicked.emit(image_id, "")
    assert heard == {"view": [image_id], "measure": []}

    _to(qtbot, window, "measure_tab")
    strip.imageClicked.emit(image_id, "")
    assert heard == {"view": [image_id], "measure": [image_id]}
    assert window.current_image_id == image_id  # Mål opened it


def test_each_page_keeps_its_own_right_click_actions(qtbot, window):
    strip = _strip(window)
    _to(qtbot, window, "measure_tab")
    assert strip._show_change_settings and not strip._show_move_to_observation and not strip._show_delete_cloud_copy
    _to(qtbot, window, "observations_tab")
    assert not strip._show_change_settings and strip._show_move_to_observation and strip._show_delete_cloud_copy


def test_height_and_scroll_carry_over(qtbot, window):
    strip = _strip(window)
    window.resize(900, 900)
    qtbot.wait(100)
    splitter = window.observations_tab.gallery_slot.parentWidget()
    total = sum(splitter.sizes())
    splitter.setSizes([total - 210, 210])
    qtbot.wait(100)
    height = strip.height()
    bar = strip._scroll.horizontalScrollBar()
    qtbot.waitUntil(lambda: bar.maximum() > 0, timeout=3000)
    bar.setValue(bar.maximum())
    qtbot.wait(50)
    chosen = window.ids_for_test["images"][-2]  # far right, now in view
    frame = strip._frames[[item.get("id") for item in strip._items].index(chosen)]
    qtbot.mouseClick(frame, Qt.LeftButton)
    qtbot.wait(50)
    position = bar.value()
    assert position > 0

    _to(qtbot, window, "measure_tab")
    qtbot.waitUntil(lambda: window.current_image_id == chosen, timeout=3000)
    qtbot.wait(200)
    assert abs(strip.height() - height) <= 4
    assert bar.value() > 0  # still scrolled to the chosen image, not back to the start
    assert strip.selected_keys() == {chosen}


def test_view_does_not_write_while_measure_holds_the_strip(qtbot, window):
    first, other = window.ids_for_test["first"], window.ids_for_test["other"]
    tab = window.observations_tab
    _to(qtbot, window, "measure_tab")
    qtbot.waitUntil(lambda: window.active_observation_id == first, timeout=3000)
    tab.table.clearSelection()  # View would clear its strip
    tab.table.selectRow(tab._find_table_row_for_observation(other))  # and load another observation
    qtbot.wait(400)
    assert _strip(window).shown_observation_id() == first

    assert tab._strip_stale  # skipped, to be redone

    _to(qtbot, window, "observations_tab")
    qtbot.waitUntil(lambda: [_strip(window).shown_observation_id()] == tab.selected_observation_ids(), timeout=3000)
    assert not tab._strip_stale


def test_measure_reloads_the_strip_when_it_takes_it_back(qtbot, window):
    first, other = window.ids_for_test["first"], window.ids_for_test["other"]
    tab = window.observations_tab
    _to(qtbot, window, "measure_tab")
    qtbot.waitUntil(lambda: window.active_observation_id == first, timeout=3000)
    _to(qtbot, window, "analysis_tab")
    _to(qtbot, window, "observations_tab")
    tab.table.selectRow(tab._find_table_row_for_observation(other))
    qtbot.waitUntil(lambda: _strip(window).shown_observation_id() == other, timeout=3000)
    window.tab_widget.setCurrentWidget(window.measure_tab)
    qtbot.waitUntil(lambda: window.active_observation_id == other, timeout=3000)
    qtbot.waitUntil(lambda: _strip(window).shown_observation_id() == other, timeout=3000)
    assert {item.get("id") for item in _strip(window)._items} == {window.ids_for_test["other_image"]}


def test_staged_images_show_first_with_an_orange_border_for_their_observation(qtbot, window):
    strip = _strip(window)
    first, other = window.ids_for_test["first"], window.ids_for_test["other"]
    staged = {"id": None, "filepath": str(window.ids_for_test["images"][0]) + "-staged.jpg", "image_number": 0}
    strip.set_staged_items(first, [staged])
    assert strip._items[0]["staged"] is True
    assert strip._items[0]["frame_border_color"] == STAGING_BORDER
    assert len(strip._items) == IMAGES + 1
    qtbot.waitUntil(lambda: len(strip._frames) == IMAGES + 1, timeout=3000)
    assert getattr(strip._frames[0], "publish_checkbox", None) is None  # nothing to keep in the cloud yet
    assert getattr(strip._frames[1], "publish_checkbox", None) is not None

    tab = window.observations_tab
    tab.table.selectRow(tab._find_table_row_for_observation(other))
    qtbot.waitUntil(lambda: strip.shown_observation_id() == other, timeout=3000)
    assert not any(item.get("staged") for item in strip._items)

    tab.table.selectRow(tab._find_table_row_for_observation(first))
    qtbot.waitUntil(lambda: strip.shown_observation_id() == first, timeout=3000)
    assert strip._items[0]["staged"] is True

    deleted = []
    strip.channel("probe").deleteImagesRequested.connect(deleted.append)  # not View's real (confirming) delete
    strip.set_active_channel("probe")
    saved = window.ids_for_test["images"][1]
    strip.deleteImagesRequested.emit([staged["filepath"]])
    strip.deleteImagesRequested.emit([staged["filepath"], saved])
    strip.set_active_channel("view")
    assert deleted == [[saved]]  # an unsaved image never reaches a page's delete

    strip.set_staged_items(first, [])
    assert not any(item.get("staged") for item in strip._items)
    assert len(strip._items) == IMAGES


def test_a_multi_selection_made_in_view_stays_in_measure(qtbot, window):
    from database.models import ImageDB as _ImageDB

    strip = _strip(window)
    first = window.ids_for_test["first"]
    _to(qtbot, window, "measure_tab")
    qtbot.waitUntil(lambda: window.active_observation_id == first, timeout=3000)
    _to(qtbot, window, "observations_tab")
    picked = window.ids_for_test["images"][3:6]
    strip.select_paths([_ImageDB.get_image(i)["filepath"] for i in picked], center=False)
    assert strip.selected_keys() == set(picked)
    _to(qtbot, window, "measure_tab")
    qtbot.wait(200)
    assert strip.selected_keys() == set(picked)


def test_select_image_unmarks_every_frame_of_a_multi_selection(qtbot, window):
    from database.models import ImageDB as _ImageDB

    strip = _strip(window)
    picked = window.ids_for_test["images"][3:6]
    qtbot.waitUntil(lambda: len(strip._frames) == IMAGES, timeout=3000)
    strip.select_paths([_ImageDB.get_image(i)["filepath"] for i in picked], center=False)
    strip.select_image(window.ids_for_test["images"][0], center=False)
    marked = [
        item.get("id") for item, frame in zip(strip._items, strip._frames)
        if getattr(frame, "_thumbnail_selected", False)
    ]
    assert strip.selected_keys() == {window.ids_for_test["images"][0]}
    assert set(marked) <= {window.ids_for_test["images"][0]}


def test_view_does_not_reselect_while_measure_holds_the_strip(qtbot, window):
    strip = _strip(window)
    first = window.ids_for_test["first"]
    _to(qtbot, window, "measure_tab")
    qtbot.waitUntil(lambda: window.active_observation_id == first, timeout=3000)
    chosen = window.ids_for_test["images"][7]
    strip.select_image(chosen, center=False)
    window.observations_tab._sync_gallery_selection_to_current_browser_image()
    assert strip.selected_keys() == {chosen}


def test_staged_images_cannot_be_dragged_among_saved_ones(qtbot, window):
    strip = _strip(window)
    first = window.ids_for_test["first"]
    staged = {"id": None, "filepath": "/tmp/unsaved-1.jpg", "image_number": 0}
    strip.set_staged_items(first, [staged])
    saved = window.ids_for_test["images"][4]
    assert not strip._reorder_item(staged["filepath"], saved, insert_after=True)
    assert not strip._reorder_item(saved, staged["filepath"])
    assert strip._items[0]["staged"]


def test_no_new_observation_drop_onto_the_strip_in_measure(qtbot, window):
    from PySide6.QtCore import QMimeData, QPointF, QUrl
    from PySide6.QtGui import QDropEvent

    strip = _strip(window)
    _to(qtbot, window, "measure_tab")
    data = QMimeData()
    data.setUrls([QUrl.fromLocalFile("/tmp/photo.jpg")])
    event = QDropEvent(QPointF(10, 10), Qt.CopyAction, data, Qt.LeftButton, Qt.NoModifier)
    called = []
    window.observations_tab._handle_new_observation_drop = lambda _event: called.append(True) or True
    assert window.observations_tab.eventFilter(strip, event) is False
    assert called == []
