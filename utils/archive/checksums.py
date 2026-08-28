"""Streaming checksum helpers for archive files."""

from __future__ import annotations

import hashlib
from pathlib import Path


DEFAULT_CHUNK_SIZE = 1024 * 1024


def sha256_file(path: str | Path, *, chunk_size: int = DEFAULT_CHUNK_SIZE) -> str:
    """Return the lowercase SHA-256 digest of *path* without loading it whole."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_sha256(
    path: str | Path,
    expected: str,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> bool:
    """Return whether *path* has the expected lowercase SHA-256 digest."""
    if not isinstance(expected, str) or len(expected) != 64:
        return False
    if expected != expected.lower() or any(char not in "0123456789abcdef" for char in expected):
        return False
    return sha256_file(path, chunk_size=chunk_size) == expected
