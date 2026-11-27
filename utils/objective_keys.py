"""Objective profile keys double as folder names under the calibrations folder.

A key from an imported bundle or archive is untrusted: deleting an objective
removes ``get_calibrations_dir() / key``, so a key that is an absolute path or
climbs out with ``..`` would name an unrelated folder.
"""

from __future__ import annotations

import hashlib
from pathlib import PurePosixPath, PureWindowsPath

# Windows device names, reserved with or without an extension.
_WINDOWS_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{index}" for index in range(1, 10)),
    *(f"LPT{index}" for index in range(1, 10)),
}


def is_safe_objective_key(key: object) -> bool:
    """True when ``key`` is one plain folder name on every platform.

    Refuses separators, ``.``/``..`` (also with trailing dots or spaces,
    which Windows drops), drives and roots, NUL and Windows device names.
    A key the dialog made from a name ending in "." stays valid; that it
    aliases another key on Windows is checked at import with
    :func:`aliases_existing_key`.
    """
    if not isinstance(key, str) or not key.strip() or not key.rstrip(" ."):
        return False
    if "/" in key or "\\" in key or "\x00" in key:
        return False
    windows = PureWindowsPath(key)
    if windows.drive or windows.root or PurePosixPath(key).is_absolute():
        return False
    if key.split(".", 1)[0].strip().upper() in _WINDOWS_RESERVED:
        return False
    return len(windows.parts) == 1 and len(PurePosixPath(key).parts) == 1


def fresh_objective_key(refused_key: str) -> str:
    """A local key for an imported objective whose key was refused.

    Derived from the refused key, so importing the same bundle again maps it
    to the same profile instead of adding another.
    """
    digest = hashlib.sha256(str(refused_key).encode("utf-8", "surrogatepass")).hexdigest()
    return f"imported_{digest[:12]}"


def aliases_existing_key(key: str, existing_keys) -> bool:
    """True when ``key`` and a different existing key name the same folder.

    Case-insensitive file systems ignore letter case, and Windows drops
    trailing dots and spaces.
    """
    def _folder_identity(value: str) -> str:
        return value.rstrip(" .").casefold()

    folded = _folder_identity(key)
    return any(
        other != key and _folder_identity(str(other)) == folded for other in existing_keys
    )
