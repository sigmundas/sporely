"""Write files that hold secrets (tokens, session cookies) owner-only."""

from __future__ import annotations

import json
import logging
import os
import stat
import sys
from pathlib import Path

PRIVATE_FILE_MODE = 0o600
PRIVATE_DIR_MODE = 0o700

_log = logging.getLogger(__name__)

_DB_SUFFIXES = (".db", ".sqlite", ".sqlite3")
_DB_SIBLING_SUFFIXES = ("-wal", "-shm", "-journal")
_SECRET_NAME_SUFFIXES = ("_cookies.json", "_tokens.json")
_SETTINGS_NAME = "app_settings.json"


def write_private_json(path: Path | str, data: object, *, indent: int | None = 2) -> None:
    """Write JSON to ``path`` readable and writable by the owner only."""
    write_private_text(path, json.dumps(data, indent=indent))


def write_private_text(path: Path | str, text: str) -> None:
    """Write UTF-8 text to ``path`` readable and writable by the owner only.

    A new file is created with mode 0600; an existing file is tightened to
    0600 before its contents are replaced, so a profile written by an older
    version with the default umask stops being readable by other accounts.
    On Windows the mode only controls the read-only flag; the per-user
    profile folder's ACL is what keeps other accounts out there.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    # O_BINARY: on Windows a raw descriptor is text mode by default, which
    # would translate the newlines a second time under the text wrapper.
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0)
    fd = os.open(target, flags, PRIVATE_FILE_MODE)
    try:
        os.chmod(target, PRIVATE_FILE_MODE)
    except OSError:
        pass
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(text)


def _is_private_target(name: str) -> bool:
    """True for the settings file, cookie/token files and SQLite files."""
    lowered = name.lower()
    if lowered == _SETTINGS_NAME or lowered.endswith(_SECRET_NAME_SUFFIXES):
        return True
    for suffix in _DB_SIBLING_SUFFIXES:
        if lowered.endswith(suffix):
            lowered = lowered[: -len(suffix)]
            break
    return lowered.endswith(_DB_SUFFIXES)


def _chmod_no_follow(path: str, mode: int, *, directory: bool) -> None:
    """chmod ``path`` without following a symlink at the final component."""
    flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0)
    if directory:
        flags |= getattr(os, "O_DIRECTORY", 0)
    fd = os.open(path, flags)
    try:
        info = os.fstat(fd)
        if directory != stat.S_ISDIR(info.st_mode):
            return
        if not directory and not stat.S_ISREG(info.st_mode):
            return
        if stat.S_IMODE(info.st_mode) != mode:
            os.fchmod(fd, mode)
    finally:
        os.close(fd)


def tighten_app_data_permissions(data_dir: Path | str) -> None:
    """Make the app data folder 0700 and its secret and database files 0600.

    Runs at startup on POSIX so a profile written by an older version (default
    umask) stops being readable by other accounts. Only the top level of
    ``data_dir`` is touched: the settings file, ``*_cookies.json``,
    ``*_tokens.json`` and SQLite files with their ``-wal``/``-shm``/``-journal``
    siblings. Symlinks are never followed, a database folder the user chose
    elsewhere is not visited, and a failure is logged, never raised. No-op on
    Windows, where the per-user profile ACL applies.
    """
    if sys.platform == "win32":
        return
    root = str(data_dir)
    try:
        _chmod_no_follow(root, PRIVATE_DIR_MODE, directory=True)
    except OSError as exc:
        _log.warning("Could not restrict app data folder %s: %s", root, exc)
        return
    try:
        with os.scandir(root) as entries:
            names = [entry.name for entry in entries if _is_private_target(entry.name)]
    except OSError as exc:
        _log.warning("Could not list app data folder %s: %s", root, exc)
        return
    for name in names:
        path = os.path.join(root, name)
        try:
            _chmod_no_follow(path, PRIVATE_FILE_MODE, directory=False)
        except OSError as exc:
            _log.warning("Could not restrict %s: %s", path, exc)
