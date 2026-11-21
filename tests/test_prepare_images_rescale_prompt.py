"""Prepare Images: changing the scale of several selected images.

Every selected image gets the new scale, so every one has its measurements
rescaled. The prompt used to offer an "OK" that rescaled only the first image
while the others still took the new objective, leaving their measurements at
the old scale (or rescaled later, unasked, at apply).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import ui.image_import_dialog as image_import_dialog
from ui.image_import_dialog import ImageImportDialog


class _FakeMessageBox:
    Question = None
    AcceptRole = "accept"
    RejectRole = "reject"
    last = None
    click = "accept"

    def __init__(self, *_args, **_kwargs):
        self.buttons = []
        type(self).last = self

    def setIcon(self, *_a):
        pass

    def setWindowTitle(self, *_a):
        pass

    def setText(self, text):
        self.text = text

    def addButton(self, label, role):
        button = SimpleNamespace(label=label, role=role)
        self.buttons.append(button)
        return button

    def exec(self):
        return 0

    def clickedButton(self):
        if type(self).click is None:
            return None
        return next(b for b in self.buttons if b.role == type(self).click)


@pytest.fixture
def fake_box(monkeypatch):
    monkeypatch.setattr(image_import_dialog, "QMessageBox", _FakeMessageBox)
    return _FakeMessageBox


def _stub():
    return SimpleNamespace(tr=lambda text: text)


@pytest.mark.parametrize("count", [1, 2, 5])
def test_prompt_offers_ok_and_cancel_only(fake_box, count):
    fake_box.click = "accept"

    decision = ImageImportDialog._confirm_rescale_for_targets(_stub(), count)

    assert decision == "all"
    assert [b.label for b in fake_box.last.buttons] == ["OK", "Cancel"]
    assert fake_box.last.text == (
        "Changing image scale: This will update previous measurements to match the new scale."
    )


@pytest.mark.parametrize("click", ["reject", None])
def test_cancel_or_closing_the_prompt_cancels(fake_box, click):
    fake_box.click = click

    assert ImageImportDialog._confirm_rescale_for_targets(_stub(), 2) == "cancel"


def _dialog_for_two_images(decision):
    dialog = MagicMock()
    dialog._collect_rescale_targets.return_value = [
        (11, 0.1, 0.2, [{"id": 1}]),
        (12, 0.1, 0.2, [{"id": 2}]),
    ]
    dialog._confirm_rescale_for_targets.return_value = decision
    dialog.selected_index = None
    dialog.selected_indices = [0, 1]
    seen = []

    def _apply(idx, action, rescale_targets):
        seen.append((idx, dict(rescale_targets) if rescale_targets is not None else None))
        return True

    dialog._apply_settings_to_index.side_effect = _apply
    return dialog, seen


def test_accepting_rescales_every_selected_image(fake_box):
    dialog, seen = _dialog_for_two_images("all")

    ImageImportDialog._apply_settings_to_indices(dialog, [0, 1], action="scale", previous_key="10x")

    # The first call receives both targets; each pops its own as it applies.
    assert seen[0] == (0, {11: (0.1, 0.2, [{"id": 1}]), 12: (0.1, 0.2, [{"id": 2}])})
    assert [idx for idx, _ in seen] == [0, 1]


def test_cancelling_changes_no_image_and_restores_the_objective(fake_box):
    dialog, seen = _dialog_for_two_images("cancel")
    dialog.objective_combo.findData.return_value = 3

    ImageImportDialog._apply_settings_to_indices(dialog, [0, 1], action="scale", previous_key="10x")

    assert seen == []
    dialog.objective_combo.setCurrentIndex.assert_called_once_with(3)
