"""One owner-only temporary directory per process, removed on exit."""
from __future__ import annotations

import atexit
import os
import shutil
import tempfile
from pathlib import Path

_ROOT: str | None = None


def _remove_root() -> None:
    global _ROOT
    root, _ROOT = _ROOT, None
    if root:
        shutil.rmtree(root, ignore_errors=True)


def private_temp_dir(*parts: str) -> Path:
    """Return a directory under this process's private temp root (mode 0700)."""
    global _ROOT
    if _ROOT is None or not os.path.isdir(_ROOT):
        _ROOT = tempfile.mkdtemp(prefix="sporely-")  # created 0700 by mkdtemp
        atexit.register(_remove_root)
    path = Path(_ROOT).joinpath(*parts)
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    return path
