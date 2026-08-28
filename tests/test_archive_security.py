import stat
from zipfile import ZipInfo

import pytest

from utils.archive.paths import (
    UnsafeArchivePathError,
    canonical_archive_path,
    safe_staging_destination,
    validate_zip_entries,
)


@pytest.mark.parametrize(
    "name",
    ["", "/absolute", "//server/share", "C:/windows", "C:\\windows", "a\\b", "a/../b", "a/./b", "a//b", "a/", "nul\x00name"],
)
def test_unsafe_archive_paths_are_rejected(name):
    with pytest.raises(UnsafeArchivePathError):
        canonical_archive_path(name)


def test_safe_staging_destination_stays_beneath_root(tmp_path):
    assert safe_staging_destination(tmp_path, "assets/images/a.webp") == (
        tmp_path / "assets/images/a.webp"
    ).resolve()
    outside = tmp_path.parent / "outside"
    outside.mkdir(exist_ok=True)
    (tmp_path / "link").symlink_to(outside, target_is_directory=True)
    with pytest.raises(UnsafeArchivePathError):
        safe_staging_destination(tmp_path, "link/escape")


def test_zip_entries_reject_duplicates_case_collisions_and_symlinks():
    with pytest.raises(UnsafeArchivePathError):
        validate_zip_entries([ZipInfo("a"), ZipInfo("a")])
    with pytest.raises(UnsafeArchivePathError):
        validate_zip_entries([ZipInfo("Folder/a"), ZipInfo("folder/A")])
    link = ZipInfo("link")
    link.create_system = 3
    link.external_attr = (stat.S_IFLNK | 0o777) << 16
    with pytest.raises(UnsafeArchivePathError):
        validate_zip_entries([link])


def test_zip_entry_validation_preserves_valid_order():
    assert validate_zip_entries([ZipInfo("manifest.json"), ZipInfo("data/file")]) == (
        "manifest.json", "data/file",
    )


def test_zip_entry_validation_rejects_pathological_expansion_limits():
    oversized = ZipInfo("huge.bin")
    oversized.file_size = 8 * 1024 * 1024 * 1024 + 1
    with pytest.raises(UnsafeArchivePathError, match="too large"):
        validate_zip_entries([oversized])

    compressed_bomb = ZipInfo("bomb.bin")
    compressed_bomb.file_size = 16 * 1024 * 1024
    compressed_bomb.compress_size = 1
    with pytest.raises(UnsafeArchivePathError, match="compression ratio"):
        validate_zip_entries([compressed_bomb])

    with pytest.raises(UnsafeArchivePathError, match="too many members"):
        validate_zip_entries([ZipInfo(f"files/{index}") for index in range(100_001)])
