import copy
import io
import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from col_xr_delivery import (
    ACCEPTED_ARCHIVE_CONTENT_TYPES,
    PUBLIC_ARCHIVE_ENDPOINT,
    HeadResponse,
    normalize_head_metadata,
    preflight_download,
    proposal_request_identity,
    sha256_value,
    validate_approved_artifact,
    validate_attempt3_authorization,
    validate_proposal,
)
from refresh_col_xr import (
    AcquisitionError,
    DownloadPolicy,
    TransportError,
    load_request,
    plan,
    stage_download,
)
import acquire_col_xr
from acquire_col_xr import (
    StructuralPolicyError,
    TransferFailure,
    cleanup_created_empty_directories,
    create_and_revalidate_layout,
    failed_release_retry_status,
    nearest_existing_ancestor,
    plan_filesystem_layout,
    promote_quarantined_archive,
    quarantine_complete_archive,
    require_retry_authorization,
    stream_once,
)
from refresh_col_xr import sha256_file


TAXONOMY = Path(__file__).resolve().parents[1]
PROPOSAL_PATH = TAXONOMY / "col-xr-source-selection.proposal.json"
VALID_REQUEST = Path(__file__).resolve().parent / "fixtures" / "col_xr" / "valid-request.json"
REDIRECT = "https://download.checklistbank.org/job/fixture.zip"


def proposal():
    return json.loads(PROPOSAL_PATH.read_text())


def head(**overrides):
    values = {
        "status": 200,
        "requested_url": PUBLIC_ARCHIVE_ENDPOINT,
        "redirect_urls": (REDIRECT,),
        "final_url": REDIRECT,
        "headers": {
            "Content-Type": "application/zip",
            "Content-Length": "1383646570",
            "ETag": '"fixture"',
            "Last-Modified": "Tue, 21 Jul 2026 15:31:43 GMT",
            "Accept-Ranges": "bytes",
        },
    }
    values.update(overrides)
    return HeadResponse(**values)


def approved_for(raw):
    identity = proposal_request_identity(raw)
    return {
        "proposal_sha256": sha256_value(raw),
        "approval_status": "approved",
        "download_authorized": True,
        "approved_at": "2026-07-23T09:00:00Z",
        "approved_canonical_endpoint": PUBLIC_ARCHIVE_ENDPOINT,
        "declared_content_length": 1383646570,
        "approved_maximum_bytes": 1610612736,
        "minimum_free_disk_bytes": 4294967296,
        "approved_redirect_hosts": ["api.checklistbank.org", "download.checklistbank.org"],
        "release_identity": identity["release"],
        "request_sha256": sha256_value(identity),
    }


def test_proposal_uses_exact_public_prebuilt_get_contract() -> None:
    raw = proposal()
    validate_proposal(raw)
    delivery = raw["delivery"]
    assert delivery["http_method"] == "GET"
    assert delivery["delivery_mode"] == "public prebuilt full release"
    assert delivery["authentication_required"] is False
    assert delivery["export_job_submission_required"] is False
    assert delivery["dataset_key"] == 315834
    assert delivery["extended"] is True
    assert delivery["format"] == "ColDP"
    assert set(delivery["accepted_final_content_types"]) == ACCEPTED_ARCHIVE_CONTENT_TYPES


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://api.checklistbank.org/dataset/315834/export",
        "https://api.checklistbank.org/dataset/315834/export.zip?format=ColDP",
        "https://api.checklistbank.org/dataset/315834/export.zip?extended=false&format=ColDP",
        "https://api.checklistbank.org/dataset/315834/export.zip?extended=true&format=DwCA",
        "https://api.checklistbank.org/dataset/315834/export.zip?extended=true&format=ColDP&extra=1",
    ],
)
def test_wrong_or_custom_export_endpoint_is_rejected(endpoint) -> None:
    raw = proposal()
    raw["delivery"]["canonical_entry_endpoint"] = endpoint
    with pytest.raises(AcquisitionError, match="exact pinned public GET"):
        validate_proposal(raw)


def test_custom_post_is_rejected_for_full_release_mode() -> None:
    raw = proposal()
    raw["delivery"]["http_method"] = "POST"
    with pytest.raises(AcquisitionError, match="GET, not custom POST"):
        validate_proposal(raw)


def test_compiler_fields_are_not_export_filters() -> None:
    raw = proposal()
    assert "included_fields" not in raw
    assert "audit_only_fields" not in raw
    assert "excluded_bulk_entities" not in raw
    assert raw["compiler_consumption"]["field_selection_claim"] is False
    assert raw["compiler_consumption"]["ignored_entities_claimed_absent"] is False
    assert raw["compiler_consumption"]["archive_preservation"] == "preserve complete immutable bytes"


def test_official_head_redirect_and_metadata_are_normalized() -> None:
    normalized = normalize_head_metadata(head())
    assert normalized["final_official_host"] == "download.checklistbank.org"
    assert normalized["content_type"] == "application/zip"
    assert normalized["content_length"] == 1383646570
    assert normalized["etag"] == '"fixture"'
    assert normalized["accept_ranges"] == "bytes"


def test_absent_content_length_is_allowed_without_body_download() -> None:
    response = head(headers={"Content-Type": "application/zip"})
    assert normalize_head_metadata(response)["content_length"] is None


def test_unapproved_redirect_host_is_rejected() -> None:
    bad = "https://objects.example.org/archive.zip"
    with pytest.raises(AcquisitionError, match="unapproved host"):
        normalize_head_metadata(head(redirect_urls=(bad,), final_url=bad))


def test_redirect_loop_is_rejected() -> None:
    with pytest.raises(AcquisitionError, match="loop"):
        normalize_head_metadata(head(redirect_urls=(REDIRECT, REDIRECT)))


def test_excessive_redirects_are_rejected() -> None:
    redirects = tuple(f"https://download.checklistbank.org/job/{index}.zip" for index in range(4))
    with pytest.raises(AcquisitionError, match="redirect limit"):
        normalize_head_metadata(head(redirect_urls=redirects, final_url=redirects[-1]))


def test_declared_oversize_archive_is_rejected_before_streaming(tmp_path) -> None:
    with pytest.raises(AcquisitionError, match="declared archive size"):
        preflight_download(
            approved_maximum_bytes=100,
            content_length=101,
            destination=tmp_path / "archive.part",
            free_bytes=1000,
        )
    with pytest.raises(AcquisitionError, match="declared archive size"):
        DownloadPolicy(max_bytes=100, declared_content_length=101)


class Chunks:
    def __init__(self, chunks):
        self.chunks = chunks

    def stream(self, request, policy):
        yield from self.chunks


def test_received_bytes_exceeding_maximum_are_rejected(tmp_path) -> None:
    request = load_request(VALID_REQUEST)
    target, _ = plan(request, tmp_path)
    with pytest.raises(TransportError, match="approved maximum"):
        stage_download(
            request,
            target,
            Chunks([b"1234", b"5678"]),
            download_policy=DownloadPolicy(max_bytes=7),
        )
    assert not (target / "archive.zip").exists()


def test_received_size_mismatch_is_rejected(tmp_path) -> None:
    request = load_request(VALID_REQUEST)
    target, _ = plan(request, tmp_path)
    with pytest.raises(TransportError, match="Content-Length"):
        stage_download(
            request,
            target,
            Chunks([b"1234567"]),
            download_policy=DownloadPolicy(max_bytes=10, declared_content_length=8),
        )
    assert not (target / "archive.zip").exists()


def test_preflight_reports_expected_size_and_available_disk(tmp_path) -> None:
    result = preflight_download(
        approved_maximum_bytes=1000,
        content_length=None,
        destination=tmp_path / "archive.part",
        free_bytes=2000,
    )
    assert result == {
        "expected_bytes": None,
        "approved_maximum_bytes": 1000,
        "available_disk_bytes": 2000,
    }


def test_future_approval_requires_exact_proposal_hash() -> None:
    raw = proposal()
    approved = approved_for(raw)
    del approved["proposal_sha256"]
    with pytest.raises(AcquisitionError, match="proposal_sha256"):
        validate_approved_artifact(raw, approved)


def test_mismatched_proposal_hash_is_rejected() -> None:
    raw = proposal()
    approved = approved_for(raw)
    approved["proposal_sha256"] = "0" * 64
    with pytest.raises(AcquisitionError, match="proposal hash mismatch"):
        validate_approved_artifact(raw, approved)


def test_proposal_cannot_be_used_as_approved_artifact() -> None:
    raw = proposal()
    with pytest.raises(AcquisitionError, match="required fields"):
        validate_approved_artifact(raw, raw)
    with pytest.raises(AcquisitionError, match="not approved"):
        load_request(PROPOSAL_PATH)


def test_valid_future_approval_contract_can_be_verified_offline() -> None:
    raw = proposal()
    result = validate_approved_artifact(raw, approved_for(raw))
    assert result["canonical_endpoint"] == PUBLIC_ARCHIVE_ENDPOINT
    assert result["maximum_bytes"] == 1610612736


def test_attempt3_authorization_binds_policy_and_expected_archive_hash() -> None:
    raw = proposal()
    approval = approved_for(raw)
    attempt2 = retry_artifact(approval)
    identity = {
        "transfer_authorization_schema_version": 1,
        "authorization_status": "approved",
        "transfer_authorized": True,
        "proposal_sha256": approval["proposal_sha256"],
        "request_sha256": approval["request_sha256"],
        "original_approval_sha256": sha256_value(approval),
        "attempt_2_authorization_sha256": attempt2["canonical_authorization_sha256"],
        "previous_attempt_number": 2,
        "authorized_attempt_number": 3,
        "maximum_full_transfer_attempts": 3,
        "release_identity": approval["release_identity"],
        "canonical_endpoint": approval["approved_canonical_endpoint"],
        "expected_content_length": approval["declared_content_length"],
        "maximum_archive_bytes": approval["approved_maximum_bytes"],
        "expected_archive_sha256": "397d701c8eb269bf78d6ac7b03149915b0d9e2a2c18694be2c91445b807814f9",
        "member_warning_threshold": 20_000,
        "member_emergency_ceiling": 250_000,
        "approved_redirect_hosts": approval["approved_redirect_hosts"],
        "authorized_at": "2026-07-23T18:00:00Z",
        "authorization_reason": "fixture policy revision",
    }
    attempt3 = {**identity, "canonical_authorization_sha256": sha256_value(identity)}
    result = validate_attempt3_authorization(raw, approval, attempt2, attempt3)
    assert result["authorized_attempt_number"] == 3
    changed = dict(attempt3, member_emergency_ceiling=250_001)
    with pytest.raises(AcquisitionError, match="member_emergency_ceiling"):
        validate_attempt3_authorization(raw, approval, attempt2, changed)


def test_proposal_mutation_invalidates_existing_approval() -> None:
    raw = proposal()
    approved = approved_for(raw)
    changed = copy.deepcopy(raw)
    changed["archive_policy"]["proposed_maximum_bytes"] -= 1
    with pytest.raises(AcquisitionError, match="proposal hash mismatch"):
        validate_approved_artifact(changed, approved)


def test_col_source_byte_ignore_policy_keeps_evidence_trackable() -> None:
    root = Path(__file__).resolve().parents[3]
    release = "database/taxonomy/sources/col_xr/2026-07-17-XR"
    ignored = [
        f"{release}/.staging/archive.part",
        f"{release}/archive.zip",
        f"{release}/extracted/example.tsv",
        f"{release}/interrupted.partial",
    ]
    trackable = [
        f"{release}/request.json",
        f"{release}/manifest.json",
        f"{release}/validation.json",
        f"{release}/checksums.txt",
    ]
    for path in ignored:
        result = subprocess.run(
            ["git", "check-ignore", "--quiet", "--", path],
            cwd=root,
            check=False,
        )
        assert result.returncode == 0, path
    for path in trackable:
        result = subprocess.run(
            ["git", "check-ignore", "--quiet", "--", path],
            cwd=root,
            check=False,
        )
        assert result.returncode == 1, path


def filesystem_paths(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    source = repo / "database/taxonomy/sources/col_xr"
    release = source / "2026-07-17-XR"
    return repo, source, release / ".staging/archive.part", release / "archive.zip"


def test_absent_proposed_parents_use_nearest_existing_ancestor_and_create(tmp_path) -> None:
    repo, source, staged, final = filesystem_paths(tmp_path)
    plan = plan_filesystem_layout(
        repository_root=repo,
        source_root=source,
        staged_archive=staged,
        final_archive=final,
    )
    assert plan["staged_nearest_existing_ancestor"] == repo
    assert plan["final_nearest_existing_ancestor"] == repo
    layout = create_and_revalidate_layout(plan)
    assert staged.parent.is_dir()
    assert final.parent.is_dir()
    assert layout["device"] == staged.parent.stat().st_dev == final.parent.stat().st_dev
    cleanup_created_empty_directories(layout["created_directories"])
    assert repo.is_dir()


def test_different_existing_ancestor_devices_are_rejected(tmp_path, monkeypatch) -> None:
    repo, source, staged, final = filesystem_paths(tmp_path)
    real_stat = acquire_col_xr.os.stat
    calls = 0

    def different_devices(path):
        nonlocal calls
        result = real_stat(path)
        calls += 1
        if calls == 2:
            return type("Stat", (), {"st_dev": result.st_dev + 1})()
        return result

    monkeypatch.setattr(acquire_col_xr.os, "stat", different_devices)
    with pytest.raises(AcquisitionError, match="different filesystems"):
        plan_filesystem_layout(
            repository_root=repo,
            source_root=source,
            staged_archive=staged,
            final_archive=final,
        )


def test_existing_parent_symlink_is_rejected(tmp_path) -> None:
    repo, source, staged, final = filesystem_paths(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (repo / "database").symlink_to(outside, target_is_directory=True)
    with pytest.raises(AcquisitionError, match="symlink"):
        plan_filesystem_layout(
            repository_root=repo,
            source_root=source,
            staged_archive=staged,
            final_archive=final,
        )


def test_symlink_introduced_after_planning_is_rejected(tmp_path) -> None:
    repo, source, staged, final = filesystem_paths(tmp_path)
    plan = plan_filesystem_layout(
        repository_root=repo,
        source_root=source,
        staged_archive=staged,
        final_archive=final,
    )
    outside = tmp_path / "outside"
    outside.mkdir()
    (repo / "database").symlink_to(outside, target_is_directory=True)
    with pytest.raises(AcquisitionError, match="symlink"):
        create_and_revalidate_layout(plan)
    assert not final.parent.exists()


def test_source_root_escape_is_rejected(tmp_path) -> None:
    repo, source, staged, final = filesystem_paths(tmp_path)
    with pytest.raises(AcquisitionError, match="escapes"):
        plan_filesystem_layout(
            repository_root=repo,
            source_root=source,
            staged_archive=repo / "outside/archive.part",
            final_archive=final,
        )


def test_file_where_parent_directory_is_expected_is_rejected(tmp_path) -> None:
    repo, source, staged, final = filesystem_paths(tmp_path)
    (repo / "database").write_text("not a directory")
    with pytest.raises(AcquisitionError, match="file exists"):
        nearest_existing_ancestor(staged.parent, repo)


def test_failed_post_creation_check_removes_only_created_empty_dirs(tmp_path, monkeypatch) -> None:
    repo, source, staged, final = filesystem_paths(tmp_path)
    preexisting = repo / "database"
    preexisting.mkdir()
    plan = plan_filesystem_layout(
        repository_root=repo,
        source_root=source,
        staged_archive=staged,
        final_archive=final,
    )
    real_stat = acquire_col_xr.os.stat

    def changed_device(path):
        result = real_stat(path)
        if Path(path) == staged.parent:
            return type("Stat", (), {"st_dev": result.st_dev + 1})()
        return result

    monkeypatch.setattr(acquire_col_xr.os, "stat", changed_device)
    with pytest.raises(AcquisitionError, match="planned filesystem"):
        create_and_revalidate_layout(plan)
    assert preexisting.is_dir()
    assert not source.exists()


def test_filesystem_failure_prevents_network_preflight(monkeypatch) -> None:
    calls = []

    def fail_layout(**kwargs):
        raise AcquisitionError("filesystem blocked")

    monkeypatch.setattr(acquire_col_xr, "plan_filesystem_layout", fail_layout)
    monkeypatch.setattr(acquire_col_xr, "fresh_head", lambda *args: calls.append(args))
    root = Path(__file__).resolve().parents[3]
    with pytest.raises(AcquisitionError, match="filesystem blocked"):
        acquire_col_xr.acquire(
            TAXONOMY / "col-xr-source-selection.proposal.json",
            TAXONOMY / "col-xr-source-selection.approved.json",
            root / "database/taxonomy/sources/col_xr/test-no-network",
        )
    assert calls == []


class FakeResponse:
    def __init__(self, chunks=None, error=None):
        self.headers = {"Content-Type": "application/zip", "Content-Length": "4"}
        self._chunks = iter(chunks or [b"data", b""])
        self._error = error

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def geturl(self):
        return "https://download.checklistbank.org/job/fixture.zip"

    def read(self, size):
        value = next(self._chunks)
        if self._error is not None and value == b"raise":
            raise self._error
        return value


class FakeOpener:
    def __init__(self, response=None, error=None, on_open=None):
        self.response = response
        self.error = error
        self.on_open = on_open
        self.calls = 0

    def open(self, request, timeout):
        self.calls += 1
        if self.on_open:
            self.on_open()
        if self.error:
            raise self.error
        return self.response


def stream_args(tmp_path, opener):
    staging = tmp_path / "release/.staging"
    staging.mkdir(parents=True)
    destination = staging / "archive.part"
    return destination, {
        "endpoint": PUBLIC_ARCHIVE_ENDPOINT,
        "destination": destination,
        "max_redirects": 3,
        "maximum_bytes": 10,
        "declared_content_length": 4,
        "expected_staging_root": staging,
        "expected_device": staging.stat().st_dev,
        "opener_builder": lambda maximum: (opener, type("Redirects", (), {"urls": []})()),
    }


@pytest.mark.parametrize("condition", ["missing", "symlink", "file", "device", "exists"])
def test_local_stream_preconditions_prevent_transport(tmp_path, condition) -> None:
    opener = FakeOpener(response=FakeResponse())
    destination, kwargs = stream_args(tmp_path, opener)
    if condition == "missing":
        destination.parent.rmdir()
    elif condition == "symlink":
        real = tmp_path / "real"
        real.mkdir()
        destination.parent.rmdir()
        destination.parent.symlink_to(real, target_is_directory=True)
    elif condition == "file":
        destination.parent.rmdir()
        destination.parent.write_text("file")
    elif condition == "device":
        kwargs["expected_device"] += 1
    elif condition == "exists":
        destination.write_bytes(b"x")
    with pytest.raises(TransferFailure) as caught:
        stream_once(**kwargs)
    assert caught.value.evidence["transport_open_attempted"] is False
    assert opener.calls == 0


def test_exclusive_open_and_permission_failure_precede_transport(tmp_path, monkeypatch) -> None:
    opener = FakeOpener(response=FakeResponse())
    destination, kwargs = stream_args(tmp_path, opener)
    original_open = Path.open

    def denied(self, *args, **options):
        if self == destination:
            raise PermissionError("fixture denied")
        return original_open(self, *args, **options)

    monkeypatch.setattr(Path, "open", denied)
    with pytest.raises(TransferFailure, match="fixture denied"):
        stream_once(**kwargs)
    assert opener.calls == 0


def test_destination_is_exclusively_opened_before_transport(tmp_path) -> None:
    opener = FakeOpener(
        response=FakeResponse(),
        on_open=lambda: (
            (_ for _ in ()).throw(AssertionError("destination not created"))
            if not destination.exists() else None
        ),
    )
    destination, kwargs = stream_args(tmp_path, opener)
    result = stream_once(**kwargs)
    assert result["bytes"] == 4
    assert destination.read_bytes() == b"data"


def test_response_open_failure_removes_zero_length_partial(tmp_path) -> None:
    opener = FakeOpener(error=OSError("offline fixture failure"))
    destination, kwargs = stream_args(tmp_path, opener)
    with pytest.raises(TransferFailure) as caught:
        stream_once(**kwargs)
    assert caught.value.evidence["transport_open_attempted"] is True
    assert caught.value.evidence["response_opened"] is False
    assert caught.value.evidence["bytes_written"] == 0
    assert caught.value.evidence["partial_file_removed"] is True
    assert not destination.exists()


def test_interrupted_stream_removes_partial_bytes(tmp_path) -> None:
    response = FakeResponse(chunks=[b"da", b"raise"], error=OSError("interrupted"))
    opener = FakeOpener(response=response)
    destination, kwargs = stream_args(tmp_path, opener)
    with pytest.raises(TransferFailure) as caught:
        stream_once(**kwargs)
    assert caught.value.evidence["response_opened"] is True
    assert caught.value.evidence["bytes_written"] == 2
    assert caught.value.evidence["partial_file_removed"] is True
    assert not destination.exists()


def test_cleanup_failure_preserves_original_stream_error(tmp_path, monkeypatch) -> None:
    response = FakeResponse(chunks=[b"da", b"raise"], error=OSError("original interruption"))
    opener = FakeOpener(response=response)
    destination, kwargs = stream_args(tmp_path, opener)
    original_unlink = Path.unlink

    def fail_unlink(self, *args, **options):
        if self == destination:
            raise PermissionError("cleanup denied")
        return original_unlink(self, *args, **options)

    monkeypatch.setattr(Path, "unlink", fail_unlink)
    with pytest.raises(TransferFailure, match="original interruption") as caught:
        stream_once(**kwargs)
    assert caught.value.evidence["failure_message"] == "original interruption"
    assert "cleanup denied" in caught.value.evidence["cleanup_error"]


def failed_release_fixture(tmp_path):
    release = tmp_path / "release"
    (release / ".staging").mkdir(parents=True)
    proposal_raw = proposal()
    approval_raw = approved_for(proposal_raw)
    request = {
        **proposal_request_identity(proposal_raw),
        "proposal_sha256": approval_raw["proposal_sha256"],
        "request_sha256": approval_raw["request_sha256"],
    }
    attempt = {
        "attempt_number": 1,
        "transport_open_attempted": True,
        "response_opened": True,
        "bytes_written": 0,
        "expected_bytes": 1383646570,
        "partial_file_removed": False,
        "cleanup_error": None,
        "failure_phase": "local_destination_setup_after_response_open",
        "failure_type": "FileExistsError",
        "failure_message": "fixture",
        "started_at": "2026-07-23T08:00:00Z",
        "failed_at": "2026-07-23T08:00:01Z",
    }
    manifest = {
        "state": "failed",
        "proposal_sha256": approval_raw["proposal_sha256"],
        "request_definition_sha256": approval_raw["request_sha256"],
        "release": approval_raw["release_identity"],
        "attempts": [attempt],
    }
    proposal_path = tmp_path / "proposal.json"
    approval_path = tmp_path / "approved.json"
    proposal_path.write_text(json.dumps(proposal_raw))
    approval_path.write_text(json.dumps(approval_raw))
    (release / "request.json").write_text(json.dumps(request))
    (release / "approval.json").write_text(json.dumps(approval_raw))
    (release / "manifest.json").write_text(json.dumps(manifest))
    return release, proposal_path, approval_path, approval_raw


def retry_artifact(raw_approval):
    raw = {
        "retry_authorization_schema_version": 1,
        "authorization_status": "approved",
        "retry_authorized": True,
        "request_sha256": raw_approval["request_sha256"],
        "proposal_sha256": raw_approval["proposal_sha256"],
        "original_approval_sha256": sha256_value(raw_approval),
        "failed_attempt_number": 1,
        "authorized_attempt_number": 2,
        "authorization_reason": "test-only fixture",
        "maximum_get_attempts": 2,
        "canonical_endpoint": raw_approval["approved_canonical_endpoint"],
        "maximum_archive_bytes": raw_approval["approved_maximum_bytes"],
        "expected_content_length": raw_approval["declared_content_length"],
        "approved_redirect_hosts": raw_approval["approved_redirect_hosts"],
        "release_identity": raw_approval["release_identity"],
        "attempt_1_response_opened": True,
        "attempt_1_bytes_written": 0,
        "authorized_at": "2026-07-23T10:00:00Z",
    }
    return {**raw, "canonical_authorization_sha256": sha256_value(raw)}


def test_safe_failed_release_status_is_read_only_and_network_free(tmp_path, monkeypatch) -> None:
    release, proposal_path, approval_path, _ = failed_release_fixture(tmp_path)
    before = (release / "manifest.json").read_text()
    monkeypatch.setattr(acquire_col_xr, "fresh_head", lambda *args: pytest.fail("network called"))
    status = failed_release_retry_status(release, proposal_path, approval_path)
    assert status["eligible_for_retry_authorization"] is True
    assert status["network_calls"] == 0
    assert (release / "manifest.json").read_text() == before


@pytest.mark.parametrize("mutation", ["state", "attempt", "payload", "quarantine"])
def test_ambiguous_or_inconsistent_failed_release_is_refused(tmp_path, mutation) -> None:
    release, proposal_path, approval_path, _ = failed_release_fixture(tmp_path)
    manifest_path = release / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if mutation == "state":
        manifest["state"] = "validated"
        manifest_path.write_text(json.dumps(manifest))
    elif mutation == "attempt":
        manifest["attempts"][0]["bytes_written"] = 1
        manifest_path.write_text(json.dumps(manifest))
    elif mutation == "payload":
        (release / "archive.zip").write_bytes(b"not allowed")
    else:
        quarantine = release / ".quarantine"
        quarantine.mkdir()
        (quarantine / "unknown.bin").write_bytes(b"not allowed")
    assert not failed_release_retry_status(release, proposal_path, approval_path)[
        "eligible_for_retry_authorization"
    ]


def test_retry_authorization_is_required_and_attempt_one_is_unchanged(tmp_path) -> None:
    release, proposal_path, approval_path, approval_raw = failed_release_fixture(tmp_path)
    before = (release / "manifest.json").read_text()
    with pytest.raises(AcquisitionError, match="separate retry authorization"):
        require_retry_authorization(release, proposal_path, approval_path, None)
    malformed = tmp_path / "retry.json"
    malformed.write_text("{}")
    with pytest.raises(AcquisitionError, match="required fields"):
        require_retry_authorization(release, proposal_path, approval_path, malformed)
    retry = retry_artifact(approval_raw)
    retry["request_sha256"] = "0" * 64
    malformed.write_text(json.dumps(retry))
    with pytest.raises(AcquisitionError, match="request_sha256"):
        require_retry_authorization(release, proposal_path, approval_path, malformed)
    malformed.write_text(json.dumps(retry_artifact(approval_raw)))
    result = require_retry_authorization(release, proposal_path, approval_path, malformed)
    assert result["retry_authorization"]["authorized_attempt_number"] == 2
    assert (release / "manifest.json").read_text() == before


def test_chunked_hashing_does_not_use_path_read_bytes(tmp_path, monkeypatch) -> None:
    sparse = tmp_path / "large-sparse.bin"
    with sparse.open("wb") as handle:
        handle.seek(32 * 1024 * 1024)
        handle.write(b"x")
    monkeypatch.setattr(Path, "read_bytes", lambda self: pytest.fail("whole-file read"))
    assert len(sha256_file(sparse, chunk_bytes=1024 * 1024)) == 64


def test_acquisition_scripts_have_no_path_read_bytes() -> None:
    scripts = Path(__file__).resolve().parents[1] / "scripts"
    for path in [scripts / "acquire_col_xr.py", scripts / "refresh_col_xr.py"]:
        assert ".read_bytes(" not in path.read_text()


def test_byte_complete_archive_is_quarantined_and_never_active(tmp_path) -> None:
    release = tmp_path / "release"
    staging = release / ".staging"
    staging.mkdir(parents=True)
    staged = staging / "archive.part"
    staged.write_bytes(b"complete")
    digest = sha256_file(staged)
    evidence = quarantine_complete_archive(
        staged,
        release,
        byte_count=8,
        sha256=digest,
        attempt_number=2,
        reason="member count exceeds policy",
        validator="fixture",
        policy_rule="max_members",
        classification="unresolved_policy_limit",
    )
    assert evidence["state"] == "quarantined"
    assert evidence["classification"] == "unresolved_policy_limit"
    assert (release / ".quarantine/archive.zip").read_bytes() == b"complete"
    assert not (release / "archive.zip").exists()
    assert not staged.exists()


def test_quarantine_cannot_overwrite_or_accept_unknown_payload(tmp_path) -> None:
    release = tmp_path / "release"
    staging = release / ".staging"
    staging.mkdir(parents=True)
    staged = staging / "archive.part"
    staged.write_bytes(b"complete")
    quarantine = release / ".quarantine"
    quarantine.mkdir()
    (quarantine / "archive.zip").write_bytes(b"existing")
    with pytest.raises(AcquisitionError, match="cannot be overwritten"):
        quarantine_complete_archive(
            staged,
            release,
            byte_count=8,
            sha256=sha256_file(staged),
            attempt_number=2,
            reason="fixture",
            validator="fixture",
            policy_rule="fixture",
            classification="unresolved_policy_limit",
        )
    assert (quarantine / "archive.zip").read_bytes() == b"existing"


def test_exact_quarantine_can_be_validated_then_atomically_promoted(tmp_path) -> None:
    release = tmp_path / "release"
    staging = release / ".staging"
    staging.mkdir(parents=True)
    staged = staging / "archive.part"
    staged.write_bytes(b"complete")
    quarantine_complete_archive(
        staged,
        release,
        byte_count=8,
        sha256=sha256_file(staged),
        attempt_number=2,
        reason="fixture policy",
        validator="fixture",
        policy_rule="fixture",
        classification="unresolved_policy_limit",
    )
    result = promote_quarantined_archive(
        release,
        {},
        validator=lambda path, approved: {"result": "passed", "validated_at": "fixture"},
    )
    assert result["result"] == "passed"
    assert (release / "archive.zip").read_bytes() == b"complete"
    assert not (release / ".quarantine/archive.zip").exists()


def test_member_emergency_limit_is_policy_rejection() -> None:
    class Members:
        def __len__(self):
            return 250_001

        def __iter__(self):
            pytest.fail("emergency count must reject before member processing")

    class Archive:
        def infolist(self):
            return Members()

    with pytest.raises(StructuralPolicyError) as caught:
        acquire_col_xr._safe_members(Archive())
    assert caught.value.policy_rule == "max_members"
    assert caught.value.classification == "unresolved_policy_limit"


def test_member_warning_does_not_bypass_path_failure() -> None:
    unsafe = zipfile.ZipInfo("../escape")

    class Members:
        def __len__(self):
            return 20_001

        def __iter__(self):
            yield unsafe

    class Archive:
        def infolist(self):
            return Members()

    with pytest.raises(AcquisitionError, match="unsafe"):
        acquire_col_xr._safe_members(Archive())
