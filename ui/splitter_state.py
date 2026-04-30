"""Shared splitter sizing helpers for resizable tab layouts."""
from __future__ import annotations

import json
from collections.abc import Sequence

from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QScrollArea, QSizePolicy, QSplitter, QWidget

from database.models import SettingsDB


SIDEBAR_MIN_WIDTH = 300
SIDEBAR_DEFAULT_WIDTH = 420
SECONDARY_PANEL_MIN_WIDTH = 260
GALLERY_MIN_HEIGHT = 100
GALLERY_DEFAULT_HEIGHT = 220
SPLITTER_HANDLE_WIDTH = 6


def configure_splitter_pane(
    widget: QWidget,
    *,
    min_width: int | None = None,
    min_height: int | None = None,
    horizontal_policy: QSizePolicy.Policy = QSizePolicy.Ignored,
    vertical_policy: QSizePolicy.Policy = QSizePolicy.Expanding,
) -> None:
    """Make a widget cooperate with a parent QSplitter."""
    widget.setSizePolicy(horizontal_policy, vertical_policy)
    if min_width is not None:
        widget.setMinimumWidth(int(min_width))
    if min_height is not None:
        widget.setMinimumHeight(int(min_height))


def configure_sidebar_scroll(scroll: QScrollArea, panel: QWidget, min_width: int = SIDEBAR_MIN_WIDTH) -> None:
    """Configure a scroll-wrapped sidebar so the splitter, not fixed widths, controls it."""
    panel.setMinimumWidth(int(min_width))
    panel.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
    scroll.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Expanding)
    scroll.setMinimumWidth(int(min_width))


def install_persistent_splitter(
    splitter: QSplitter,
    *,
    key: str,
    default_sizes: Sequence[int],
    minimum_sizes: Sequence[int] | None = None,
) -> None:
    """Restore a splitter now and save later user changes under a SettingsDB key."""
    splitter.setHandleWidth(SPLITTER_HANDLE_WIDTH)
    sizes = _load_splitter_sizes(key, default_sizes, minimum_sizes)
    splitter.setSizes(sizes)
    QTimer.singleShot(0, lambda: splitter.setSizes(_load_splitter_sizes(key, sizes, minimum_sizes)))
    splitter.splitterMoved.connect(
        lambda _pos, _index: save_splitter_sizes(splitter, key=key, minimum_sizes=minimum_sizes)
    )


def save_splitter_sizes(
    splitter: QSplitter,
    *,
    key: str,
    minimum_sizes: Sequence[int] | None = None,
) -> None:
    sizes = _clamp_splitter_sizes(splitter.sizes(), minimum_sizes)
    SettingsDB.set_setting(key, json.dumps(sizes))


def restore_splitter_sizes(
    splitter: QSplitter,
    sizes: Sequence[int] | None,
    *,
    default_sizes: Sequence[int],
    minimum_sizes: Sequence[int] | None = None,
) -> bool:
    """Apply explicit saved sizes. Returns False when the data is unusable."""
    if not _is_valid_size_sequence(sizes, len(default_sizes)):
        return False
    splitter.setSizes(_clamp_splitter_sizes(sizes or default_sizes, minimum_sizes))
    return True


def _load_splitter_sizes(
    key: str,
    default_sizes: Sequence[int],
    minimum_sizes: Sequence[int] | None = None,
) -> list[int]:
    raw = SettingsDB.get_setting(key, "")
    parsed: object = None
    if raw:
        try:
            parsed = json.loads(str(raw))
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = None
    sizes = parsed if _is_valid_size_sequence(parsed, len(default_sizes)) else default_sizes
    return _clamp_splitter_sizes(sizes, minimum_sizes)


def _is_valid_size_sequence(value: object, expected_length: int) -> bool:
    return (
        isinstance(value, list | tuple)
        and len(value) >= expected_length
        and all(isinstance(item, int | float) for item in value[:expected_length])
    )


def _clamp_splitter_sizes(
    sizes: Sequence[int | float],
    minimum_sizes: Sequence[int] | None = None,
) -> list[int]:
    clamped = [max(0, int(round(value))) for value in sizes]
    if minimum_sizes is None:
        return clamped
    for index, minimum in enumerate(minimum_sizes):
        if index < len(clamped):
            clamped[index] = max(int(minimum), clamped[index])
    return clamped
