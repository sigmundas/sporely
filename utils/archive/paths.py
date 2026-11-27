"""Archive-name and ZIP-entry security primitives."""

from __future__ import annotations

import re
import stat
from pathlib import Path, PurePosixPath
from zipfile import ZIP_DEFLATED, ZIP_STORED, ZipFile, ZipInfo


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


# Control documents (manifest, settings and objectives JSON) are parsed whole,
# so they get their own caps far below the per-member asset limit. A manifest
# lists every archived file, and the QSettings snapshot carries every
# observation's Species Plate settings, so both grow with a large library.
MAX_MANIFEST_BYTES = 64 * 1024 * 1024
MAX_CONTROL_DOCUMENT_BYTES = 64 * 1024 * 1024
# Sporely writes its archives with deflate; other codecs decompress without an
# output bound inside zipfile, so control documents must not use them.
_CONTROL_COMPRESSION = frozenset({ZIP_STORED, ZIP_DEFLATED})


def require_control_member(info: ZipInfo, limit: int | None = None) -> None:
    """Refuse a control document by its declared size and codec, unread."""
    if limit is None:
        limit = MAX_MANIFEST_BYTES if info.filename == "manifest.json" else MAX_CONTROL_DOCUMENT_BYTES
    if info.file_size > limit:
        raise UnsafeArchivePathError(f"archive control document is too large: {info.filename}")
    if info.compress_type not in _CONTROL_COMPRESSION:
        raise UnsafeArchivePathError(
            f"archive control document uses an unsupported compression: {info.filename}"
        )


def read_control_member(archive: ZipFile, name: str, limit: int | None = None) -> bytes:
    """Read a small ZIP control document, refusing one above *limit* bytes.

    The declared size is checked before any byte is read, and the read is
    bounded so that a wrong declared size cannot bypass the cap.
    """
    if limit is None:
        limit = MAX_MANIFEST_BYTES if name == "manifest.json" else MAX_CONTROL_DOCUMENT_BYTES
    info = archive.getinfo(name)
    require_control_member(info, limit)
    chunks: list[bytes] = []
    remaining = limit + 1
    with archive.open(info, "r") as source:
        while remaining > 0:
            chunk = source.read(min(remaining, 1024 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
    data = b"".join(chunks)
    if len(data) > limit:
        raise UnsafeArchivePathError(f"archive control document is too large: {name}")
    return data
