"""Write files that hold secrets (tokens, session cookies) owner-only."""

from __future__ import annotations

import json
import logging
import os
import re
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
    """True for the settings file, cookie/token files and SQLite files.

    Also matches a leftover temporary copy (``x.tmp``, ``.x.<pid>.tmp``) that a
    crashed write can strand with the default umask.
    """
    lowered = name.lower()
    if lowered.endswith(".tmp"):
        lowered = re.sub(r"(\.\d+)?\.tmp$", "", lowered).lstrip(".")
    if lowered == _SETTINGS_NAME or lowered.endswith(_SECRET_NAME_SUFFIXES):
        return True
    for suffix in _DB_SIBLING_SUFFIXES:
        if lowered.endswith(suffix):
            lowered = lowered[: -len(suffix)]
            break
    return lowered.endswith(_DB_SUFFIXES)


def _chmod_no_follow(
    name: str, mode: int, *, directory: bool, dir_fd: int | None = None
) -> int:
    """chmod ``name`` (relative to ``dir_fd``) without following a symlink."""
    flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0)
    if directory:
        flags |= getattr(os, "O_DIRECTORY", 0)
    fd = os.open(name, flags, dir_fd=dir_fd)
    try:
        info = os.fstat(fd)
        if directory != stat.S_ISDIR(info.st_mode):
            return 0
        # A file with other hard links shares its inode with something that
        # may live outside the folder; leave it alone.
        if not directory and (not stat.S_ISREG(info.st_mode) or info.st_nlink > 1):
            return 0
        if stat.S_IMODE(info.st_mode) != mode:
            os.fchmod(fd, mode)
    finally:
        os.close(fd)
    return 0


def tighten_app_data_permissions(data_dir: Path | str) -> None:
    """Make the app data folder 0700 and its secret and database files 0600.

    Runs at startup on POSIX so a profile written by an older version (default
    umask) stops being readable by other accounts. Only the top level of
    ``data_dir`` is touched: the settings file, ``*_cookies.json``,
    ``*_tokens.json`` and SQLite files with their ``-wal``/``-shm``/``-journal``
    siblings. Symlinks are never followed (the folder is opened once and its
    files are reached through that descriptor), a database folder the user
    chose elsewhere is not visited, and a failure is logged, never raised.
    No-op on Windows, where the per-user profile ACL applies.
    """
    if sys.platform == "win32":
        return
    root = str(data_dir)
    flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_DIRECTORY", 0)
    try:
        dir_fd = os.open(root, flags)
    except OSError as exc:
        _log.warning("Could not open app data folder %s: %s", root, exc)
        return
    try:
        try:
            if stat.S_IMODE(os.fstat(dir_fd).st_mode) != PRIVATE_DIR_MODE:
                os.fchmod(dir_fd, PRIVATE_DIR_MODE)
        except OSError as exc:
            # Still tighten the files this account owns.
            _log.warning("Could not restrict app data folder %s: %s", root, exc)
        try:
            with os.scandir(dir_fd) as entries:
                names = [e.name for e in entries if _is_private_target(e.name)]
        except OSError as exc:
            _log.warning("Could not list app data folder %s: %s", root, exc)
            return
        for name in names:
            try:
                _chmod_no_follow(name, PRIVATE_FILE_MODE, directory=False, dir_fd=dir_fd)
            except OSError as exc:
                _log.warning("Could not restrict %s: %s", os.path.join(root, name), exc)
    finally:
        os.close(dir_fd)
