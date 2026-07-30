import io
import json
import socket
import stat
import struct
import sys
import zipfile
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from refresh_col_xr import AcquisitionError
from remote_zip_audit import (
    MAX_RANGE_RESPONSE_BYTES,
    discover_central_directory,
    inspect_central_directory,
    remote_zip_audit_plan,
    validate_future_range_response,
)


def ordinary_zip():
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("metadata.yaml", "title: fixture\n")
        archive.writestr("NameUsage.tsv", "ID\tscientificName\nF\tFungi\n")
    raw = output.getvalue()
    discovery = discover_central_directory(
        raw, suffix_offset=0, archive_length=len(raw)
    )
    start = discovery["central_directory_offset"]
    end = start + discovery["central_directory_size"]
    return raw, discovery, raw[start:end]


def central_record(
    name,
    *,
    compressed=10,
    uncompressed=20,
    method=8,
    flags=0x800,
    external=0,
    version_made=20,
):
    encoded = name.encode("utf-8")
    fixed = struct.pack(
        "<4s6H3I5H2I",
        b"PK\x01\x02",
        version_made,
        20,
        flags,
        method,
        0,
        0,
        0,
        compressed,
        uncompressed,
        len(encoded),
        0,
        0,
        0,
        0,
        external,
        0,
    )
    return fixed + encoded


def test_ordinary_eocd_and_central_directory_parsing() -> None:
    _, discovery, central = ordinary_zip()
    report = inspect_central_directory(
        central,
        expected_count=discovery["member_count"],
        expected_size=discovery["central_directory_size"],
    )
    assert report["member_count"] == 2
    assert report["member_count_warning_triggered"] is False
    assert report["aggregate_uncompressed_bytes"] > 0


def test_zip64_locator_and_eocd_parsing() -> None:
    archive_length = 10_000
    zip64_offset = 8_000
    zip64 = struct.pack(
        "<4sQHHIIQQQQ",
        b"PK\x06\x06", 44, 45, 45, 0, 0, 70_000, 70_000, 456, 123,
    )
    locator = struct.pack("<4sIQI", b"PK\x06\x07", 0, zip64_offset, 1)
    eocd = struct.pack(
        "<4sHHHHIIH",
        b"PK\x05\x06", 0, 0, 0xFFFF, 0xFFFF, 0xFFFFFFFF, 0xFFFFFFFF, 0,
    )
    suffix = locator + eocd
    first = discover_central_directory(
        suffix,
        suffix_offset=archive_length - len(suffix),
        archive_length=archive_length,
    )
    assert first["requires_zip64_metadata"] is True
    result = discover_central_directory(
        suffix,
        suffix_offset=archive_length - len(suffix),
        archive_length=archive_length,
        zip64_record=zip64,
    )
    assert result["member_count"] == 70_000
    assert result["central_directory_offset"] == 123
    assert result["central_directory_size"] == 456


@pytest.mark.parametrize("payload", [b"", b"PK\x05\x06short", b"not-a-zip"])
def test_malformed_or_truncated_eocd_is_rejected(payload) -> None:
    with pytest.raises(AcquisitionError):
        discover_central_directory(payload, suffix_offset=0, archive_length=len(payload))


@pytest.mark.parametrize("count", [19_999, 20_000, 20_001, 250_000])
def test_exact_member_count_above_and_below_policy(count) -> None:
    record = central_record("a")
    data = record * count
    report = inspect_central_directory(
        data, expected_count=count, expected_size=len(data)
    )
    assert report["member_count"] == count
    assert report["member_count_warning_triggered"] is (count > 20_000)
    if count > 20_000:
        assert report["policy_classification"] == "warning"


def test_member_count_above_emergency_ceiling_is_rejected() -> None:
    with pytest.raises(AcquisitionError, match="emergency ceiling"):
        inspect_central_directory(b"", expected_count=250_001, expected_size=0)


def test_path_symlink_duplicate_and_normalization_detection() -> None:
    symlink_mode = (stat.S_IFLNK | 0o777) << 16
    data = b"".join([
        central_record("../escape.tsv"),
        central_record("/absolute.tsv"),
        central_record("same.tsv"),
        central_record("same.tsv"),
        central_record("Café.tsv"),
        central_record("Cafe\u0301.tsv"),
        central_record(
            "link",
            external=symlink_mode,
            version_made=(3 << 8) | 20,
        ),
    ])
    report = inspect_central_directory(data, expected_count=7, expected_size=len(data))
    assert report["traversal_paths"] == ["../escape.tsv"]
    assert report["absolute_paths"] == ["/absolute.tsv"]
    assert report["duplicate_paths"] == ["same.tsv"]
    assert report["normalization_collisions"] == [["Café.tsv", "Cafe\u0301.tsv"]]
    assert report["symlinks"] == ["link"]


def test_aggregate_sizes_ratios_methods_and_groups() -> None:
    data = b"".join([
        central_record("a/one.tsv", compressed=10, uncompressed=100),
        central_record("b/two.csv", compressed=20, uncompressed=40, method=99),
    ])
    report = inspect_central_directory(data, expected_count=2, expected_size=len(data))
    assert report["aggregate_compressed_bytes"] == 30
    assert report["aggregate_uncompressed_bytes"] == 140
    assert report["aggregate_compression_ratio"] == pytest.approx(140 / 30)
    assert report["unsupported_compression_methods"] == {99: 1}
    assert report["top_level_groups"] == {"a": 1, "b": 1}


def test_range_plan_is_bounded_and_requires_separate_authorization() -> None:
    plan = remote_zip_audit_plan(
        endpoint="https://api.checklistbank.org/dataset/315834/export.zip?extended=true&format=ColDP",
        archive_length=1383646570,
        etag='"fixture"',
        last_modified="fixture-date",
    )
    assert plan["execution_authorized"] is False
    assert plan["maximum_total_response_bytes"] <= MAX_RANGE_RESPONSE_BYTES
    assert plan["validators"]["required_status"] == 206
    assert plan["validators"]["no_data_member_ranges"] is True
    assert sum(item["maximum_response_bytes"] for item in plan["requests"]) <= MAX_RANGE_RESPONSE_BYTES


def test_future_range_executor_would_reject_200_before_body() -> None:
    with pytest.raises(AcquisitionError, match="200"):
        validate_future_range_response(200, None)
    with pytest.raises(AcquisitionError, match="Content-Range"):
        validate_future_range_response(206, None)
    validate_future_range_response(206, "bytes 0-9/100")


def test_offline_audit_components_cannot_open_socket(monkeypatch) -> None:
    monkeypatch.setattr(socket, "socket", lambda *args, **kwargs: pytest.fail("socket opened"))
    _, discovery, central = ordinary_zip()
    inspect_central_directory(
        central,
        expected_count=discovery["member_count"],
        expected_size=discovery["central_directory_size"],
    )
    remote_zip_audit_plan(
        endpoint="https://api.checklistbank.org/example.zip",
        archive_length=100,
        etag="etag",
        last_modified="date",
    )
