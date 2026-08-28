"""Archive-name and ZIP-entry security primitives."""

from __future__ import annotations

import re
import stat
from pathlib import Path, PurePosixPath
from zipfile import ZipInfo


class UnsafeArchivePathError(ValueError):
    """Raised when an archive member name is unsafe or non-canonical."""


_WINDOWS_DRIVE = re.compile(r"^[A-Za-z]:")
_MAX_ZIP_MEMBERS = 100_000
_MAX_ZIP_MEMBER_BYTES = 8 * 1024 * 1024 * 1024
_MAX_ZIP_TOTAL_BYTES = 32 * 1024 * 1024 * 1024
_MAX_COMPRESSION_RATIO = 10_000
_RATIO_CHECK_MIN_BYTES = 16 * 1024 * 1024


def canonical_archive_path(value: str) -> str:
    """Validate and return a canonical relative POSIX archive path."""
    if not isinstance(value, str) or not value:
        raise UnsafeArchivePathError("archive path must be a non-empty string")
    if "\x00" in value:
        raise UnsafeArchivePathError("archive path contains NUL")
    if "\\" in value:
        raise UnsafeArchivePathError("archive path contains a backslash")
    if value.startswith("/") or value.startswith("//") or _WINDOWS_DRIVE.match(value):
        raise UnsafeArchivePathError("archive path must be relative")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise UnsafeArchivePathError("archive path contains an empty or traversal component")
    path = PurePosixPath(value)
    canonical = path.as_posix()
    if canonical != value:
        raise UnsafeArchivePathError("archive path is not canonical POSIX form")
    return canonical


def safe_staging_destination(staging_root: str | Path, archive_path: str) -> Path:
    """Resolve an archive path beneath *staging_root*, rejecting escapes."""
    canonical = canonical_archive_path(archive_path)
    root = Path(staging_root).resolve()
    destination = root.joinpath(*PurePosixPath(canonical).parts).resolve()
    if destination == root or root not in destination.parents:
        raise UnsafeArchivePathError("archive path escapes staging root")
    return destination


def zip_info_is_symlink(info: ZipInfo) -> bool:
    """Return whether a ZIP entry represents a Unix symbolic link."""
    mode = (info.external_attr >> 16) & 0xFFFF
    return stat.S_ISLNK(mode)


def validate_zip_entries(infos: list[ZipInfo] | tuple[ZipInfo, ...]) -> tuple[str, ...]:
    """Validate ZIP entries and return their names in input order."""
    if len(infos) > _MAX_ZIP_MEMBERS:
        raise UnsafeArchivePathError("ZIP contains too many members")
    names: list[str] = []
    exact: set[str] = set()
    folded: set[str] = set()
    total_size = 0
    for info in infos:
        name = canonical_archive_path(info.filename)
        if info.is_dir():
            raise UnsafeArchivePathError(f"directory ZIP entry is not allowed: {name}")
        if zip_info_is_symlink(info):
            raise UnsafeArchivePathError(f"symbolic-link ZIP entry is not allowed: {name}")
        unix_mode = (info.external_attr >> 16) & 0xFFFF
        if unix_mode and stat.S_IFMT(unix_mode) not in {0, stat.S_IFREG}:
            raise UnsafeArchivePathError(f"special-file ZIP entry is not allowed: {name}")
        if info.file_size < 0 or info.file_size > _MAX_ZIP_MEMBER_BYTES:
            raise UnsafeArchivePathError(f"ZIP member is too large: {name}")
        total_size += info.file_size
        if total_size > _MAX_ZIP_TOTAL_BYTES:
            raise UnsafeArchivePathError("ZIP expands beyond the allowed total size")
        if (
            info.file_size >= _RATIO_CHECK_MIN_BYTES
            and info.file_size > max(1, info.compress_size) * _MAX_COMPRESSION_RATIO
        ):
            raise UnsafeArchivePathError(f"ZIP member has an unsafe compression ratio: {name}")
        folded_name = name.casefold()
        if name in exact:
            raise UnsafeArchivePathError(f"duplicate ZIP entry: {name}")
        if folded_name in folded:
            raise UnsafeArchivePathError(f"case-folding ZIP entry collision: {name}")
        exact.add(name)
        folded.add(folded_name)
        names.append(name)
    return tuple(names)
