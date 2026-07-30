import io
import json
import sys
import zipfile
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from refresh_col_xr import (
    AcquisitionError,
    ColXrRequest,
    ImmutableReleaseError,
    TransportError,
    inspect_fixture_archive,
    load_request,
    plan,
    release_dir,
    safe_release_slug,
    stage_download,
    status,
    validate_and_promote,
    validate_manifest,
)


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "col_xr"


def valid_raw() -> dict:
    return json.loads((FIXTURE_DIR / "valid-request.json").read_text(encoding="utf-8"))


def valid_request() -> ColXrRequest:
    return load_request(FIXTURE_DIR / "valid-request.json")


def zip_bytes(
    request: ColXrRequest,
    *,
    metadata: bool = True,
    metadata_overrides: dict[str, str] | None = None,
    member_name: str = "NameUsage.tsv",
) -> bytes:
    metadata_values = {
        "release": request.release_label,
        "datasetKey": str(request.dataset_key),
        "issued": request.issued_date,
        "format": request.archive_format,
        "recordCount": "1",
    }
    metadata_values.update(metadata_overrides or {})
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        if metadata:
            archive.writestr(
                "metadata.yaml",
                "".join(f"{key}: {value}\n" for key, value in metadata_values.items()),
            )
        archive.writestr(member_name, "ID\tscientificName\tstatus\nfixture:1\tFungus exemplaris\taccepted\n")
    return output.getvalue()


class BytesTransport:
    def __init__(self, payload: bytes):
        self.payload = payload
        self.calls = 0

    def stream(self, request, policy):
        self.calls += 1
        yield self.payload[:11]
        yield self.payload[11:]


class FailingTransport:
    def __init__(self, exc: Exception):
        self.exc = exc
        self.calls = 0

    def stream(self, request, policy):
        self.calls += 1
        raise self.exc
        yield b"unreachable"


class InterruptedTransport:
    def __init__(self):
        self.calls = 0

    def stream(self, request, policy):
        self.calls += 1
        yield b"partial bytes"
        raise OSError("connection interrupted")


def test_valid_pinned_extended_release_selection() -> None:
    request = valid_request()
    assert request.release_type == "Extended Release"
    assert request.dataset_key == 123456


@pytest.mark.parametrize("value", ["latest", "2099 latest XR", "LATEST"])
def test_latest_is_rejected(value: str) -> None:
    raw = valid_raw()
    raw["release_label"] = value
    with pytest.raises(AcquisitionError, match="latest"):
        ColXrRequest.from_dict(raw)


def test_missing_dataset_key_is_rejected() -> None:
    raw = valid_raw()
    del raw["dataset_key"]
    with pytest.raises(AcquisitionError, match="dataset_key"):
        ColXrRequest.from_dict(raw)


@pytest.mark.parametrize("value", ["123456", 1.5, True, 0, -1])
def test_invalid_dataset_key_is_rejected(value) -> None:
    raw = valid_raw()
    raw["dataset_key"] = value
    with pytest.raises(AcquisitionError, match="dataset_key"):
        ColXrRequest.from_dict(raw)


def test_base_release_is_rejected() -> None:
    raw = valid_raw()
    raw["release_type"] = "Base Release"
    with pytest.raises(AcquisitionError, match="Extended Release"):
        ColXrRequest.from_dict(raw)


@pytest.mark.parametrize("value", ["2099-13-01", "15-01-2099", "2099-1-5"])
def test_malformed_issued_date_is_rejected(value: str) -> None:
    raw = valid_raw()
    raw["issued_date"] = value
    raw["release_label"] = f"{value} XR fixture"
    with pytest.raises(AcquisitionError, match="issued_date"):
        ColXrRequest.from_dict(raw)


def test_mismatched_release_metadata_is_rejected() -> None:
    raw = valid_raw()
    raw["issued_date"] = "2099-02-15"
    with pytest.raises(AcquisitionError, match="do not match"):
        ColXrRequest.from_dict(raw)


def test_mismatched_endpoint_dataset_key_is_rejected() -> None:
    raw = valid_raw()
    raw["endpoint"] = "https://api.checklistbank.org/dataset/999999/export"
    with pytest.raises(AcquisitionError, match="dataset key"):
        ColXrRequest.from_dict(raw)


def test_unsupported_archive_format_is_rejected() -> None:
    raw = valid_raw()
    raw["archive_format"] = "CSV"
    with pytest.raises(AcquisitionError, match="unsupported"):
        ColXrRequest.from_dict(raw)


def test_request_hash_is_deterministic_and_ignores_execution_metadata() -> None:
    raw = valid_raw()
    reordered = dict(reversed(list(raw.items())))
    reordered["included_fields"] = list(reversed(reordered["included_fields"]))
    first = ColXrRequest.from_dict(raw)
    second = ColXrRequest.from_dict(reordered)
    assert first.request_sha256 == second.request_sha256
    assert first.persisted(created_at="2000-01-01T00:00:00Z", tool_git_commit="a")[
        "canonical_request_sha256"
    ] == second.persisted(created_at="2100-01-01T00:00:00Z", tool_git_commit="b")[
        "canonical_request_sha256"
    ]


@pytest.mark.parametrize("field", ["token", "authorization", "signed_url", "client_secret"])
def test_secret_fields_are_rejected(field: str) -> None:
    raw = valid_raw()
    raw[field] = "do-not-persist"
    with pytest.raises(AcquisitionError, match="secret field"):
        ColXrRequest.from_dict(raw)


def test_signed_endpoint_is_rejected() -> None:
    raw = valid_raw()
    raw["endpoint"] += "?token=do-not-persist"
    with pytest.raises(AcquisitionError, match="signed or secret"):
        ColXrRequest.from_dict(raw)


def test_safe_release_path_derivation(tmp_path: Path) -> None:
    request = valid_request()
    assert safe_release_slug(request.release_label) == "2099-01-15-XR-fixture"
    assert release_dir(tmp_path, request).parent == tmp_path.resolve()


@pytest.mark.parametrize("label", ["../escape", "/absolute", r"..\\escape", "a/b"])
def test_release_path_traversal_is_rejected(label: str) -> None:
    with pytest.raises(AcquisitionError, match="unsafe"):
        safe_release_slug(label)


def test_dry_run_planning_makes_no_transport_calls(tmp_path: Path) -> None:
    transport = BytesTransport(b"must not be read")
    target, idempotent = plan(valid_request(), tmp_path)
    assert not idempotent
    assert transport.calls == 0
    assert status(target)["state"] == "planned"
    assert not (target / "archive.zip").exists()


def test_permanent_transport_failure_does_not_promote(tmp_path: Path) -> None:
    request = valid_request()
    target, _ = plan(request, tmp_path)
    transport = FailingTransport(TransportError("HTTP 404"))
    with pytest.raises(TransportError, match="404"):
        stage_download(request, target, transport)
    assert status(target)["state"] == "failed"
    assert not (target / "archive.zip").exists()


def test_interrupted_stream_does_not_promote(tmp_path: Path) -> None:
    request = valid_request()
    target, _ = plan(request, tmp_path)
    with pytest.raises(OSError, match="interrupted"):
        stage_download(request, target, InterruptedTransport())
    assert status(target)["state"] == "failed"
    assert not (target / "archive.zip").exists()


def test_archive_checksum_and_validated_promotion(tmp_path: Path) -> None:
    request = valid_request()
    payload = zip_bytes(request)
    target, _ = plan(request, tmp_path)
    staged = stage_download(request, target, BytesTransport(payload))
    downloaded = status(target)
    assert downloaded["download"]["bytes"] == len(payload)
    assert len(downloaded["download"]["sha256"]) == 64
    validated = validate_and_promote(request, target, staged)
    assert validated["state"] == "validated"
    assert (target / "archive.zip").read_bytes() == payload


def test_malformed_zip_is_rejected(tmp_path: Path) -> None:
    archive = tmp_path / "bad.zip"
    archive.write_bytes(b"not a zip")
    with pytest.raises(AcquisitionError, match="malformed"):
        inspect_fixture_archive(archive, valid_request())


def test_unsafe_zip_member_is_rejected(tmp_path: Path) -> None:
    archive = tmp_path / "unsafe.zip"
    archive.write_bytes(zip_bytes(valid_request(), member_name="../NameUsage.tsv"))
    with pytest.raises(AcquisitionError, match="unsafe ZIP"):
        inspect_fixture_archive(archive, valid_request())


def test_missing_required_metadata_is_rejected(tmp_path: Path) -> None:
    archive = tmp_path / "missing-metadata.zip"
    archive.write_bytes(zip_bytes(valid_request(), metadata=False))
    with pytest.raises(AcquisitionError, match="metadata.yaml"):
        inspect_fixture_archive(archive, valid_request())


def test_mismatched_archive_release_metadata_is_rejected(tmp_path: Path) -> None:
    archive = tmp_path / "mismatch.zip"
    archive.write_bytes(zip_bytes(valid_request(), metadata_overrides={"datasetKey": "999999"}))
    with pytest.raises(AcquisitionError, match="metadata mismatch"):
        inspect_fixture_archive(archive, valid_request())


def test_empty_archive_is_rejected(tmp_path: Path) -> None:
    archive = tmp_path / "empty.zip"
    with zipfile.ZipFile(archive, "w"):
        pass
    with pytest.raises(AcquisitionError, match="empty"):
        inspect_fixture_archive(archive, valid_request())


def test_unimplemented_fixture_archive_format_is_rejected(tmp_path: Path) -> None:
    raw = valid_raw()
    raw["archive_format"] = "DwCA"
    request = ColXrRequest.from_dict(raw)
    archive = tmp_path / "fixture.zip"
    archive.write_bytes(zip_bytes(valid_request()))
    with pytest.raises(AcquisitionError, match="not implemented"):
        inspect_fixture_archive(archive, request)


def test_identical_request_is_idempotent(tmp_path: Path) -> None:
    request = valid_request()
    first, first_idempotent = plan(request, tmp_path)
    second, second_idempotent = plan(request, tmp_path)
    assert first == second
    assert not first_idempotent
    assert second_idempotent


def test_different_request_cannot_overwrite_release(tmp_path: Path) -> None:
    request = valid_request()
    target, _ = plan(request, tmp_path)
    staged = stage_download(request, target, BytesTransport(zip_bytes(request)))
    validate_and_promote(request, target, staged)
    changed = valid_raw()
    changed["included_fields"].append("NameUsage.authorship")
    with pytest.raises(ImmutableReleaseError, match="different immutable request"):
        plan(ColXrRequest.from_dict(changed), tmp_path)


def test_identical_completed_request_is_idempotent(tmp_path: Path) -> None:
    request = valid_request()
    target, _ = plan(request, tmp_path)
    staged = stage_download(request, target, BytesTransport(zip_bytes(request)))
    validate_and_promote(request, target, staged)
    repeated, idempotent = plan(request, tmp_path)
    assert repeated == target
    assert idempotent
    assert status(target)["state"] == "validated"


def test_manifest_cannot_jump_from_planned_to_validated() -> None:
    manifest = {
        "state": "validated",
        "download": None,
        "validation": {"result": "passed"},
    }
    with pytest.raises(AcquisitionError, match="archive evidence"):
        validate_manifest(manifest)
