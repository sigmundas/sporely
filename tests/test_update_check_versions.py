"""The update check compares release versions numerically across the 1.0 boundary."""

from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

from ui.main_window import MainWindow


def _window():
    window = SimpleNamespace()
    window._parse_version = lambda value: MainWindow._parse_version(window, value)
    return window


@pytest.mark.parametrize(
    ("latest", "current", "newer"),
    [
        ("1.0.0", "0.9.30", True),
        ("v1.0.0", "0.9.30", True),
        ("0.9.30", "1.0.0", False),
        ("1.0.0", "1.0.0", False),
        ("1.0.1", "1.0.0", True),
        ("0.10.0", "0.9.30", True),
    ],
)
def test_release_versions_compare_numerically(latest, current, newer):
    window = _window()
    current_parsed = MainWindow._parse_version(window, current)
    assert MainWindow._is_newer_version(window, latest, current_parsed) is newer
