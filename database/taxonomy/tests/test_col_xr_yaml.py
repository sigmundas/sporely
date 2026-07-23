import io
import sys
import zipfile
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from acquire_col_xr import (
    COL_XR_METADATA_BYTES,
    MAX_METADATA_BYTES,
    PINNED_COL_XR_IDENTITY,
    PINNED_COL_XR_RELEASE,
    _metadata_limit,
    _sample_source_members,
)
from col_xr_yaml import YamlLimits, validate_yaml_events
from refresh_col_xr import AcquisitionError


def approved() -> dict:
    return {**PINNED_COL_XR_IDENTITY, "release_identity": PINNED_COL_XR_RELEASE}


def member(size: int) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo("metadata.yaml")
    info.file_size = size
    return info


def parse(text: bytes, **changes):
    values = {"max_bytes": max(1, len(text)), "max_seconds": 10}
    values.update(changes)
    return validate_yaml_events(
        io.BytesIO(text), limits=YamlLimits(**values), expected_bytes=len(text)
    )


def test_default_metadata_ceiling_applies_at_and_above_boundary() -> None:
    assert _metadata_limit({}, member(MAX_METADATA_BYTES)) == (MAX_METADATA_BYTES, False)
    with pytest.raises(AcquisitionError, match="exact pinned"):
        _metadata_limit({}, member(MAX_METADATA_BYTES + 1))


def test_pinned_col_xr_override_is_bounded_to_256_mib() -> None:
    assert _metadata_limit(approved(), member(MAX_METADATA_BYTES + 1)) == (
        COL_XR_METADATA_BYTES,
        True,
    )
    with pytest.raises(AcquisitionError, match="256 MiB"):
        _metadata_limit(approved(), member(COL_XR_METADATA_BYTES + 1))


def test_identity_mismatch_prevents_large_metadata_override() -> None:
    wrong = approved()
    wrong["request_sha256"] = "0" * 64
    with pytest.raises(AcquisitionError, match="exact pinned"):
        _metadata_limit(wrong, member(MAX_METADATA_BYTES + 1))


def test_large_yaml_is_event_parsed_with_bounded_reads() -> None:
    class Tracking(io.BytesIO):
        def read(self, size=-1):
            assert 0 <= size <= 64 * 1024
            return super().read(size)

    raw = b"title: fixture\npadding: " + b"x" * (5 * 1024 * 1024) + b"\n"
    result = validate_yaml_events(
        Tracking(raw),
        limits=YamlLimits(max_bytes=6 * 1024 * 1024, max_scalar_bytes=6 * 1024 * 1024),
        expected_bytes=len(raw),
    )
    assert result["complete_document"] is True
    assert result["read_calls"] > 1


@pytest.mark.parametrize(
    ("raw", "changes", "message"),
    [
        (b"a:\n  b:\n    c: 1\n", {"max_depth": 2}, "depth"),
        (b"a: abcdef\n", {"max_scalar_bytes": 5}, "scalar"),
        (b"a: 1\nb: 2\n", {"max_nodes": 2}, "node"),
        (b"a: [1, 2, 3]\n", {"max_sequence_entries": 2}, "sequence"),
        (b"a: &one 1\nb: &two 2\n", {"max_anchors": 1}, "anchor"),
        (b"a: &one 1\nb: *one\nc: *one\n", {"max_aliases": 1}, "alias"),
    ],
)
def test_yaml_resource_limits(raw, changes, message) -> None:
    with pytest.raises(AcquisitionError, match=message):
        parse(raw, **changes)


def test_unsafe_tag_and_duplicate_critical_key_are_rejected() -> None:
    with pytest.raises(AcquisitionError, match="custom YAML tag"):
        parse(b"value: !python/object fixture\n")
    with pytest.raises(AcquisitionError, match="duplicate critical"):
        parse(b"title: first\ntitle: second\n")


@pytest.mark.parametrize("raw", [b"title: [unterminated\n", b"---\na: 1\n---\nb: 2\n"])
def test_malformed_or_multiple_documents_are_rejected(raw) -> None:
    with pytest.raises(AcquisitionError, match="malformed|multiple"):
        parse(raw)


def test_complete_document_termination_is_observed() -> None:
    result = parse(b"---\ntitle: fixture\n...\n")
    assert result["complete_document"] is True
    assert result["root_type"] == "mapping"


def test_source_sampling_is_deterministic_and_fully_parses_selected_files(tmp_path) -> None:
    path = tmp_path / "sources.zip"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for value in range(30):
            archive.writestr(f"source/{value}.yaml", f"key: {value}\ntitle: source {value}\n")
    with zipfile.ZipFile(path) as archive:
        sources = sorted(archive.infolist(), key=lambda item: item.filename)
        first = _sample_source_members(archive, sources)
        second = _sample_source_members(archive, sources)
    assert [item["path"] for item in first["reports"]] == [
        item["path"] for item in second["reports"]
    ]
    assert all(item["yaml"]["complete_document"] for item in first["reports"])


def test_anomalous_source_path_is_never_silently_sampled(tmp_path) -> None:
    path = tmp_path / "sources.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("source/1.yaml", "key: 1\n")
        archive.writestr("source/not-an-id.yaml", "key: nope\n")
    with zipfile.ZipFile(path) as archive:
        sources = [archive.getinfo("source/1.yaml")]
        with pytest.raises(AcquisitionError, match="anomalous"):
            _sample_source_members(archive, sources)
