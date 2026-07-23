#!/usr/bin/env python3
"""Bounded offline ZIP central-directory inspection and range-audit planning."""

from __future__ import annotations

import argparse
import json
import posixpath
import stat
import struct
import unicodedata
from collections import Counter
from pathlib import PurePosixPath
from typing import Any

from refresh_col_xr import AcquisitionError


EOCD = b"PK\x05\x06"
ZIP64_LOCATOR = b"PK\x06\x07"
ZIP64_EOCD = b"PK\x06\x06"
CENTRAL = b"PK\x01\x02"
MAX_EOCD_SUFFIX_BYTES = 65_557
MAX_RANGE_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_FILENAME_BYTES = 4_096
MEMBER_POLICY_VERSION = 2
MEMBER_WARNING_THRESHOLD = 20_000
MEMBER_EMERGENCY_CEILING = 250_000


def discover_central_directory(
    suffix: bytes,
    *,
    suffix_offset: int,
    archive_length: int,
    zip64_record: bytes | None = None,
) -> dict[str, Any]:
    position = suffix.rfind(EOCD)
    if position < 0 or len(suffix) - position < 22:
        raise AcquisitionError("ordinary ZIP EOCD is missing or truncated")
    fields = struct.unpack_from("<4sHHHHIIH", suffix, position)
    comment_length = fields[7]
    if position + 22 + comment_length != len(suffix):
        raise AcquisitionError("ZIP EOCD comment length is inconsistent")
    disk, central_disk, disk_count, total_count, central_size, central_offset = fields[1:7]
    if disk != 0 or central_disk != 0 or disk_count != total_count:
        raise AcquisitionError("multi-disk ZIP archives are unsupported")
    needs_zip64 = (
        total_count == 0xFFFF
        or central_size == 0xFFFFFFFF
        or central_offset == 0xFFFFFFFF
    )
    result = {
        "archive_length": archive_length,
        "eocd_offset": suffix_offset + position,
        "zip64": needs_zip64,
    }
    if not needs_zip64:
        if central_offset + central_size > archive_length:
            raise AcquisitionError("central directory lies outside the archive")
        return {
            **result,
            "member_count": total_count,
            "central_directory_offset": central_offset,
            "central_directory_size": central_size,
        }
    locator_position = position - 20
    if locator_position < 0 or suffix[locator_position:locator_position + 4] != ZIP64_LOCATOR:
        raise AcquisitionError("ZIP64 locator is missing")
    _, locator_disk, zip64_offset, disk_total = struct.unpack_from(
        "<4sIQI", suffix, locator_position
    )
    if locator_disk != 0 or disk_total != 1:
        raise AcquisitionError("multi-disk ZIP64 archives are unsupported")
    result["zip64_eocd_offset"] = zip64_offset
    result["zip64_metadata_range"] = {"start": zip64_offset, "end": zip64_offset + 55}
    if zip64_record is None:
        result["requires_zip64_metadata"] = True
        return result
    if len(zip64_record) < 56 or zip64_record[:4] != ZIP64_EOCD:
        raise AcquisitionError("ZIP64 EOCD is malformed or truncated")
    values = struct.unpack_from("<4sQHHIIQQQQ", zip64_record)
    record_size = values[1]
    if record_size < 44 or len(zip64_record) < record_size + 12:
        raise AcquisitionError("ZIP64 EOCD declared size is inconsistent")
    disk, central_disk, disk_count, total_count = values[4:8]
    central_size, central_offset = values[8:10]
    if disk != 0 or central_disk != 0 or disk_count != total_count:
        raise AcquisitionError("multi-disk ZIP64 archives are unsupported")
    if central_offset + central_size > archive_length:
        raise AcquisitionError("ZIP64 central directory lies outside the archive")
    return {
        **result,
        "requires_zip64_metadata": False,
        "member_count": total_count,
        "central_directory_offset": central_offset,
        "central_directory_size": central_size,
    }


def _zip64_values(extra: bytes, needs: list[str]) -> dict[str, int]:
    position = 0
    while position + 4 <= len(extra):
        kind, length = struct.unpack_from("<HH", extra, position)
        position += 4
        data = extra[position:position + length]
        position += length
        if len(data) != length:
            raise AcquisitionError("central-directory extra field is truncated")
        if kind == 0x0001:
            required = 8 * len(needs)
            if len(data) < required:
                raise AcquisitionError("ZIP64 central extra field is truncated")
            return {
                name: struct.unpack_from("<Q", data, index * 8)[0]
                for index, name in enumerate(needs)
            }
    if needs:
        raise AcquisitionError("ZIP64 sizes or offsets lack the required extra field")
    return {}


def inspect_central_directory(
    data: bytes,
    *,
    expected_count: int,
    expected_size: int,
) -> dict[str, Any]:
    if len(data) != expected_size:
        raise AcquisitionError("central-directory response size is inconsistent")
    if expected_count > MEMBER_EMERGENCY_CEILING:
        raise AcquisitionError("ZIP member count exceeds emergency ceiling")
    position = 0
    count = 0
    compressed_total = 0
    uncompressed_total = 0
    paths: set[str] = set()
    normalized: dict[str, str] = {}
    duplicates: list[str] = []
    normalization_collisions: list[list[str]] = []
    traversal: list[str] = []
    absolute: list[str] = []
    symlinks: list[str] = []
    special: list[str] = []
    directories: list[str] = []
    encrypted: list[str] = []
    unsupported: Counter[int] = Counter()
    top_levels: Counter[str] = Counter()
    extensions: Counter[str] = Counter()
    largest: list[tuple[int, str]] = []
    ratios: list[tuple[float, str]] = []
    max_filename = 0
    local_offsets: dict[int, str] = {}
    duplicate_local_offsets: list[list[Any]] = []
    while position < len(data):
        if len(data) - position < 46 or data[position:position + 4] != CENTRAL:
            raise AcquisitionError("central-directory record is malformed or truncated")
        values = struct.unpack_from("<4s6H3I5H2I", data, position)
        (
            _, version_made, _, flags, method, _, _, _, compressed, uncompressed,
            name_length, extra_length, comment_length, disk_start, _, external, local_offset,
        ) = values
        record_size = 46 + name_length + extra_length + comment_length
        if record_size > len(data) - position:
            raise AcquisitionError("central-directory variable fields are truncated")
        if name_length == 0 or name_length > MAX_FILENAME_BYTES:
            raise AcquisitionError("central-directory filename length is unsafe")
        name_bytes = data[position + 46:position + 46 + name_length]
        extra_start = position + 46 + name_length
        extra = data[extra_start:extra_start + extra_length]
        encoding = "utf-8" if flags & 0x800 else "cp437"
        try:
            name = name_bytes.decode(encoding)
        except UnicodeDecodeError as exc:
            raise AcquisitionError("central-directory filename cannot be decoded") from exc
        needs = []
        if uncompressed == 0xFFFFFFFF:
            needs.append("uncompressed")
        if compressed == 0xFFFFFFFF:
            needs.append("compressed")
        if local_offset == 0xFFFFFFFF:
            needs.append("local_offset")
        if disk_start == 0xFFFF:
            needs.append("disk_start")
        zip64 = _zip64_values(extra, needs)
        uncompressed = zip64.get("uncompressed", uncompressed)
        compressed = zip64.get("compressed", compressed)
        local_offset = zip64.get("local_offset", local_offset)
        disk_start = zip64.get("disk_start", disk_start)
        if disk_start != 0:
            raise AcquisitionError("multi-disk central record is unsupported")
        if flags & 0x1:
            encrypted.append(name)
        if method not in {0, 8}:
            unsupported[method] += 1
        unix_mode = external >> 16 if version_made >> 8 == 3 else 0
        if stat.S_ISLNK(unix_mode):
            symlinks.append(name)
        elif name.endswith("/") or stat.S_ISDIR(unix_mode):
            directories.append(name)
        elif unix_mode and stat.S_IFMT(unix_mode) not in {0, stat.S_IFREG, stat.S_IFDIR}:
            special.append(name)
        previous_offset = local_offsets.get(local_offset)
        if previous_offset is not None:
            duplicate_local_offsets.append([local_offset, previous_offset, name])
        else:
            local_offsets[local_offset] = name
        pure = PurePosixPath(name)
        if pure.is_absolute() or name.startswith(("/", "\\")) or (
            len(name) >= 2 and name[1] == ":"
        ):
            absolute.append(name)
        if ".." in pure.parts or "\\" in name:
            traversal.append(name)
        if name in paths:
            duplicates.append(name)
        paths.add(name)
        normalized_name = unicodedata.normalize(
            "NFC", posixpath.normpath(name.replace("\\", "/"))
        ).casefold()
        previous = normalized.get(normalized_name)
        if previous is not None and previous != name:
            normalization_collisions.append([previous, name])
        else:
            normalized[normalized_name] = name
        top_levels[pure.parts[0] if pure.parts else "<root>"] += 1
        suffix = pure.suffix.casefold() or "<none>"
        extensions[suffix] += 1
        compressed_total += compressed
        uncompressed_total += uncompressed
        largest.append((uncompressed, name))
        ratios.append((uncompressed / max(1, compressed), name))
        max_filename = max(max_filename, name_length)
        count += 1
        position += record_size
    if count != expected_count:
        raise AcquisitionError(
            f"central-directory member count mismatch: parsed {count}, expected {expected_count}"
        )
    return {
        "member_count": count,
        "member_count_policy_version": MEMBER_POLICY_VERSION,
        "member_count_warning_threshold": MEMBER_WARNING_THRESHOLD,
        "member_count_emergency_ceiling": MEMBER_EMERGENCY_CEILING,
        "member_count_warning_triggered": count > MEMBER_WARNING_THRESHOLD,
        "member_count_emergency_rejected": count > MEMBER_EMERGENCY_CEILING,
        "policy_classification": (
            "emergency_ceiling_exceeded"
            if count > MEMBER_EMERGENCY_CEILING
            else "warning"
            if count > MEMBER_WARNING_THRESHOLD
            else "within_policy"
        ),
        "central_directory_bytes": len(data),
        "aggregate_compressed_bytes": compressed_total,
        "aggregate_uncompressed_bytes": uncompressed_total,
        "aggregate_compression_ratio": uncompressed_total / max(1, compressed_total),
        "largest_members": [
            {"name": name, "uncompressed_bytes": size}
            for size, name in sorted(largest, reverse=True)[:20]
        ],
        "highest_compression_ratios": [
            {"name": name, "ratio": ratio}
            for ratio, name in sorted(ratios, reverse=True)[:20]
        ],
        "top_level_groups": dict(top_levels.most_common()),
        "extensions": dict(extensions.most_common()),
        "duplicate_paths": duplicates,
        "normalization_collisions": normalization_collisions,
        "traversal_paths": traversal,
        "absolute_paths": absolute,
        "symlinks": symlinks,
        "directories": directories,
        "special_files": special,
        "duplicate_local_header_offsets": duplicate_local_offsets,
        "encrypted_members": encrypted,
        "unsupported_compression_methods": dict(unsupported),
        "maximum_filename_bytes": max_filename,
    }


def remote_zip_audit_plan(
    *,
    endpoint: str,
    archive_length: int,
    etag: str,
    last_modified: str,
) -> dict[str, Any]:
    suffix_bytes = min(MAX_EOCD_SUFFIX_BYTES, archive_length)
    return {
        "plan_schema_version": 1,
        "execution_authorized": False,
        "separate_network_authorization_required": True,
        "canonical_endpoint": endpoint,
        "archive_length": archive_length,
        "validators": {
            "required_status": 206,
            "reject_status_200_before_body": True,
            "require_valid_content_range": True,
            "required_etag": etag,
            "required_last_modified": last_modified,
            "required_total_length": archive_length,
            "https_only": True,
            "approved_hosts": ["api.checklistbank.org", "download.checklistbank.org"],
            "maximum_redirects": 3,
            "no_retries": True,
            "no_resume": True,
            "no_data_member_ranges": True,
        },
        "requests": [
            {
                "purpose": "locate ordinary EOCD and ZIP64 locator if present",
                "range": f"bytes=-{suffix_bytes}",
                "maximum_response_bytes": suffix_bytes,
            },
            {
                "purpose": "ZIP64 EOCD metadata only, conditional on locator",
                "range": "computed exact 56-byte range",
                "maximum_response_bytes": 56,
                "conditional": True,
            },
            {
                "purpose": "central directory only",
                "range": "computed from EOCD/ZIP64 metadata",
                "maximum_response_bytes": MAX_RANGE_RESPONSE_BYTES - suffix_bytes - 56,
                "conditional": True,
            },
        ],
        "maximum_total_response_bytes": MAX_RANGE_RESPONSE_BYTES,
    }


def validate_future_range_response(status: int, content_range: str | None) -> None:
    if status == 200:
        raise AcquisitionError("range server returned 200; abort before reading response body")
    if status != 206:
        raise AcquisitionError(f"range server returned unexpected HTTP {status}")
    if not content_range or not content_range.startswith("bytes "):
        raise AcquisitionError("range response lacks a valid Content-Range")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["remote-zip-audit-plan"])
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--archive-length", required=True, type=int)
    parser.add_argument("--etag", required=True)
    parser.add_argument("--last-modified", required=True)
    args = parser.parse_args()
    print(json.dumps(remote_zip_audit_plan(
        endpoint=args.endpoint,
        archive_length=args.archive_length,
        etag=args.etag,
        last_modified=args.last_modified,
    ), sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
