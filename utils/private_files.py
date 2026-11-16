"""Write files that hold secrets (tokens, session cookies) owner-only."""

from __future__ import annotations

import json
import os
from pathlib import Path

PRIVATE_FILE_MODE = 0o600


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
