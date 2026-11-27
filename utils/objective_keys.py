"""Objective profile keys double as folder names under the calibrations folder.

A key from an imported bundle or archive is untrusted: deleting an objective
removes ``get_calibrations_dir() / key``, so a key that is an absolute path or
climbs out with ``..`` would name an unrelated folder.
"""

from __future__ import annotations

import uuid
from pathlib import PurePosixPath, PureWindowsPath


def is_safe_objective_key(key: object) -> bool:
    """True when ``key`` is one plain path component on every platform."""
    if not isinstance(key, str):
        return False
    # Windows drops trailing dots and spaces, so ".. " would name "..".
    if not key.strip() or not key.rstrip(" ."):
        return False
    if "/" in key or "\\" in key or "\x00" in key:
        return False
    windows = PureWindowsPath(key)
    if windows.drive or windows.root or PurePosixPath(key).is_absolute():
        return False
    return len(windows.parts) == 1 and len(PurePosixPath(key).parts) == 1


def fresh_objective_key() -> str:
    """A new local key for an imported objective whose key was refused."""
    return f"imported_{uuid.uuid4().hex[:12]}"
