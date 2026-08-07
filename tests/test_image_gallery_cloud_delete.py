"""Tests for the gallery-widget side of the 'delete cloud copy' feature.

Covers three responsibilities the gallery owns:

  * derives ``CLOUD_IMAGE_STATE_*`` from ``cloud_id`` + tombstone row,
  * only lists items in ``UPLOADED`` state as eligible for cloud delete,
  * emits ``deleteCloudCopiesRequested`` with the pre-filtered id list from
    the right-click context menu.

The context-menu path is exercised via the ``QMenu`` monkey-patch pattern
used elsewhere in ``test_image_gallery_widget.py``.
"""
import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtGui import QColor, QImage
from PySide6.QtWidgets import QApplication

import ui.image_gallery_widget as gallery_module
from ui.image_gallery_widget import ImageGalleryWidget
from database.models import (
    CLOUD_IMAGE_STATE_DELETED,
    CLOUD_IMAGE_STATE_DELETE_PENDING,
    CLOUD_IMAGE_STATE_NONE,
    CLOUD_IMAGE_STATE_UPLOADED,
)


def _ensure_qapp() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _mixed_cloud_items(tmp_path) -> list[dict]:
    """Three items — uploaded, pending-delete, and no-cloud — for eligibility tests."""
    items = []
    for index, cloud_state, cloud_id in (
        (0, CLOUD_IMAGE_STATE_UPLOADED, "cloud-1"),
        (1, CLOUD_IMAGE_STATE_DELETE_PENDING, "cloud-2"),
        (2, CLOUD_IMAGE_STATE_NONE, None),
    ):
        image_path = tmp_path / f"gallery-{index}.png"
        image = QImage(32, 32, QImage.Format_ARGB32)
        image.fill(QColor.fromHsv((index * 90) % 360, 180, 220))
        assert image.save(str(image_path))
        items.append(
            {
                "id": index + 1,
                "filepath": str(image_path),
                "cloud_id": cloud_id,
                "cloud_state": cloud_state,
            }
        )
    return items


def test_derive_cloud_state_covers_all_four_states():
    derive = ImageGalleryWidget._derive_cloud_state

    assert derive("", None) == CLOUD_IMAGE_STATE_NONE
    assert derive(None, None) == CLOUD_IMAGE_STATE_NONE
    assert derive("cloud-1", None) == CLOUD_IMAGE_STATE_UPLOADED
    assert derive("cloud-1", {"delete_synced_at": None}) == CLOUD_IMAGE_STATE_DELETE_PENDING
    assert derive("cloud-1", {"delete_synced_at": ""}) == CLOUD_IMAGE_STATE_DELETE_PENDING
    assert derive("cloud-1", {"delete_synced_at": "2026-08-07 10:00:00"}) == CLOUD_IMAGE_STATE_DELETED


def test_cloud_state_for_item_falls_back_to_legacy_bool_fields():
    fallback = ImageGalleryWidget._cloud_state_for_item
    # Item without explicit cloud_state must still be classified correctly
    # from cloud_uploaded / cloud_tombstone_synced (legacy set_items input).
    assert fallback({}) == CLOUD_IMAGE_STATE_NONE
    assert fallback({"cloud_id": "cloud-1", "cloud_uploaded": True}) == CLOUD_IMAGE_STATE_UPLOADED
    assert fallback(
        {"cloud_id": "cloud-1", "cloud_uploaded": False, "cloud_tombstone_synced": False}
    ) == CLOUD_IMAGE_STATE_DELETE_PENDING
    assert fallback(
        {"cloud_id": "cloud-1", "cloud_uploaded": False, "cloud_tombstone_synced": True}
    ) == CLOUD_IMAGE_STATE_DELETED


def test_cloud_delete_eligible_image_ids_returns_only_uploaded_int_keys(tmp_path):
    _ensure_qapp()
    widget = ImageGalleryWidget("Images", show_delete_cloud_copy=True)
    widget.set_multi_select(True)
    items = _mixed_cloud_items(tmp_path)
    widget.set_items(items)

    # All three items selected. Only the UPLOADED one (id=1) is eligible;
    # DELETE_PENDING and NONE are filtered out.
    assert widget.cloud_delete_eligible_image_ids([1, 2, 3]) == [1]
    # Explicit keys=None uses current selection.
    widget.select_paths([items[0]["filepath"], items[1]["filepath"]])
    assert widget.cloud_delete_eligible_image_ids() == [1]
    # String keys (calibration placeholders) are never eligible.
    assert widget.cloud_delete_eligible_image_ids(["cal_0", 1]) == [1]
    # Empty input → empty output, not the full selection.
    assert widget.cloud_delete_eligible_image_ids([]) == []


def test_context_menu_hides_delete_cloud_when_selection_has_no_uploaded_items(
    monkeypatch, tmp_path
):
    _ensure_qapp()
    widget = ImageGalleryWidget("Images", show_delete_cloud_copy=True)
    widget.set_multi_select(True)
    items = _mixed_cloud_items(tmp_path)
    widget.set_items(items)
    widget.select_paths([items[1]["filepath"], items[2]["filepath"]])

    emitted: list[list[int]] = []
    widget.deleteCloudCopiesRequested.connect(lambda ids: emitted.append(list(ids)))

    actions_added: list[str] = []

    class _FakeMenu:
        def __init__(self, parent=None):
            self._actions = []

        def addAction(self, text):
            actions_added.append(str(text))
            action = SimpleNamespace(text=lambda t=text: t)
            self._actions.append(action)
            return action

        def actions(self):
            return list(self._actions)

        def exec(self, global_pos):
            return None

    monkeypatch.setattr(gallery_module, "QMenu", _FakeMenu)

    frame = widget._frames[1]
    widget._show_thumbnail_context_menu(frame, frame.mapToGlobal(frame.rect().center()))

    assert not any("cloud copy" in text.lower() or "cloud copies" in text.lower() for text in actions_added)
    assert emitted == []


def test_context_menu_emits_delete_cloud_copies_with_eligible_ids_only(
    monkeypatch, tmp_path
):
    _ensure_qapp()
    widget = ImageGalleryWidget("Images", show_delete_cloud_copy=True)
    widget.set_multi_select(True)
    items = _mixed_cloud_items(tmp_path)
    widget.set_items(items)
    # Select all three — only the UPLOADED item should be signaled.
    widget.select_paths([item["filepath"] for item in items])

    emitted: list[list[int]] = []
    widget.deleteCloudCopiesRequested.connect(lambda ids: emitted.append(list(ids)))

    class _FakeMenu:
        def __init__(self, parent=None):
            self._actions = []

        def addAction(self, text):
            action = SimpleNamespace(text=lambda t=text: t)
            self._actions.append(action)
            return action

        def actions(self):
            return list(self._actions)

        def exec(self, global_pos):
            for action in self._actions:
                if "cloud cop" in action.text().lower():
                    return action
            return None

    monkeypatch.setattr(gallery_module, "QMenu", _FakeMenu)

    frame = widget._frames[0]
    widget._show_thumbnail_context_menu(frame, frame.mapToGlobal(frame.rect().center()))

    assert emitted == [[1]]


def test_mark_cloud_delete_pending_flips_state_and_keeps_badge_visible(tmp_path):
    """Delete-pending is now a visible state (red cloud + strike), not a hide."""
    _ensure_qapp()
    widget = ImageGalleryWidget("Images", show_delete_cloud_copy=True)
    widget.set_multi_select(True)
    items = _mixed_cloud_items(tmp_path)
    widget.set_items(items)

    uploaded_item = widget._items[0]
    assert ImageGalleryWidget._cloud_badge_visible(uploaded_item) is True

    updated = widget.mark_cloud_delete_pending([1, 99])
    assert updated == 1

    flipped_item = widget._items[0]
    assert flipped_item["cloud_state"] == CLOUD_IMAGE_STATE_DELETE_PENDING
    assert flipped_item["cloud_uploaded"] is False
    # Badge stays visible for DELETE_PENDING; the render layer picks a red
    # strikethrough icon variant. Only NONE and DELETED hide it entirely.
    assert ImageGalleryWidget._cloud_badge_visible(flipped_item) is True


def test_mark_cloud_delete_pending_unticks_publish_checkbox_and_emits_change(tmp_path):
    _ensure_qapp()
    widget = ImageGalleryWidget(
        "Images",
        show_delete_cloud_copy=True,
        show_publish_checkbox=True,
    )
    widget.set_multi_select(True)
    items = _mixed_cloud_items(tmp_path)
    # Uploaded item is ticked; the other two are explicitly unticked so we
    # can assert exactly which id survives in the emitted selection set.
    items[0]["publish_selected"] = True
    items[1]["publish_selected"] = False
    items[2]["publish_selected"] = False
    widget.set_items(items)

    emitted: list[set[int]] = []
    widget.publishSelectionChanged.connect(lambda ids: emitted.append(set(ids)))

    updated = widget.mark_cloud_delete_pending([1])

    assert updated == 1
    assert widget._items[0]["publish_selected"] is False
    # After the auto-uncheck the surviving publish set is empty.
    assert emitted == [set()]

    # Calling again with an already-unticked item should not re-emit.
    updated = widget.mark_cloud_delete_pending([1])
    assert updated == 1
    assert emitted == [set()]


def test_mark_cloud_delete_pending_no_publish_change_no_signal(tmp_path):
    _ensure_qapp()
    widget = ImageGalleryWidget(
        "Images",
        show_delete_cloud_copy=True,
        show_publish_checkbox=True,
    )
    widget.set_multi_select(True)
    items = _mixed_cloud_items(tmp_path)
    # Nothing is ticked for publishing.
    for item in items:
        item["publish_selected"] = False
    widget.set_items(items)

    emitted: list[set[int]] = []
    widget.publishSelectionChanged.connect(lambda ids: emitted.append(set(ids)))

    widget.mark_cloud_delete_pending([1])

    assert emitted == []


def test_show_delete_cloud_copy_disabled_never_adds_menu_entry(monkeypatch, tmp_path):
    _ensure_qapp()
    widget = ImageGalleryWidget("Images")  # default: show_delete_cloud_copy=False
    widget.set_multi_select(True)
    items = _mixed_cloud_items(tmp_path)
    widget.set_items(items)
    widget.select_paths([items[0]["filepath"]])

    actions_added: list[str] = []

    class _FakeMenu:
        def __init__(self, parent=None):
            self._actions = []

        def addAction(self, text):
            actions_added.append(str(text))
            action = SimpleNamespace(text=lambda t=text: t)
            self._actions.append(action)
            return action

        def actions(self):
            return list(self._actions)

        def exec(self, global_pos):
            return None

    monkeypatch.setattr(gallery_module, "QMenu", _FakeMenu)

    frame = widget._frames[0]
    widget._show_thumbnail_context_menu(frame, frame.mapToGlobal(frame.rect().center()))

    assert not any("cloud cop" in text.lower() for text in actions_added)
