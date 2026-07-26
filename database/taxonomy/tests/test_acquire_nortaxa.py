from __future__ import annotations

import fcntl
import hashlib
import http.client
import io
import json
import os
import shutil
import socket
import stat
import sys
from email.message import Message
from datetime import datetime, timezone
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

import acquire_nortaxa as subject  # noqa: E402
from refresh_col_xr import AcquisitionError  # noqa: E402


TAXONOMY = Path(__file__).resolve().parents[1]
VALID_ARCHIVE = TAXONOMY / "tests" / "fixtures" / "nortaxa" / "valid-dwca.zip"
NOW = datetime(2026, 7, 26, 12, 0, tzinfo=timezone.utc)
GIT = subject.GitState(
    head="a" * 40,
    clean=True,
    committed_executor_sha256=subject._executor_sha(),
    committed_test_sha256=subject._executor_test_sha(),
)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("network/socket operation attempted in offline executor test")

    monkeypatch.setattr(socket, "socket", denied)
    monkeypatch.setattr(socket, "create_connection", denied)
    monkeypatch.setattr(socket, "getaddrinfo", denied)
    monkeypatch.setattr(socket, "gethostbyname", denied)
    monkeypatch.setattr(http.client.HTTPConnection, "connect", denied)
    monkeypatch.setattr(http.client.HTTPSConnection, "connect", denied)


class RecordingStream:
    def __init__(self, payload: bytes, *, fail_after: int | None = None):
        self.payload = payload
        self.offset = 0
        self.fail_after = fail_after
        self.requests: list[int] = []

    def read(self, size: int = -1) -> bytes:
        self.requests.append(size)
        if self.fail_after is not None and self.offset >= self.fail_after:
            raise OSError("synthetic interrupted read")
        if size < 0:
            size = len(self.payload) - self.offset
        value = self.payload[self.offset:self.offset + size]
        self.offset += len(value)
        return value


class FakeTransport:
    def __init__(
        self,
        payload: bytes,
        *,
        status: int = 200,
        final_url: str = subject.CANONICAL_ENDPOINT,
        headers: dict[str, str] | None = None,
        fail_after: int | None = None,
    ):
        self.stream = RecordingStream(payload, fail_after=fail_after)
        self.status = status
        self.final_url = final_url
        self.headers = headers or {"Content-Type": "application/zip"}
        self.calls = 0
        self.closed = 0

    def open(self, endpoint: str) -> subject.TransportResponse:
        self.calls += 1
        assert endpoint == subject.CANONICAL_ENDPOINT
        return subject.TransportResponse(
            status=self.status,
            final_url=self.final_url,
            headers=self.headers,
            stream=self.stream,
            close=lambda: setattr(self, "closed", self.closed + 1),
        )


def approval(maximum: int = subject.MAXIMUM_BYTES) -> dict:
    return {
        "approval_schema_version": 1,
        "approval_status": "approved",
        "download_authorized": True,
        "acquisition_proposal_sha256": subject.ACQUISITION_PROPOSAL_SHA256,
        "source_selection_proposal_sha256": subject.SOURCE_SELECTION_PROPOSAL_SHA256,
        "request_sha256": subject.REQUEST_SHA256,
        "policy_resolution_sha256": subject.POLICY_RESOLUTION_SHA256,
        "metadata_verification_sha256": subject.METADATA_VERIFICATION_SHA256,
        "attempt_4_sha256": subject.ATTEMPT_4_SHA256,
        "source_code": subject.SOURCE_CODE,
        "profile_code": subject.PROFILE_CODE,
        "version": subject.VERSION,
        "issued_date": subject.ISSUED_DATE,
        "dataset_uuid": subject.DATASET_UUID,
        "approved_canonical_endpoint": subject.CANONICAL_ENDPOINT,
        "allowed_hosts": [subject.ALLOWED_HOST],
        "approved_redirect_hosts": [],
        "approved_maximum_bytes": maximum,
        "permitted_get_attempts": 1,
        "redirects_authorized": False,
        "range_requests_authorized": False,
        "retries_authorized": False,
        "resume_authorized": False,
        "fallback_endpoint_authorized": False,
        "authentication_authorized": False,
        "cookies_authorized": False,
        "conditional_requests_authorized": False,
        "approved_at": "2026-07-26T00:00:00Z",
        "expires_at": "2026-08-02T00:00:00Z",
        "superseded_by": None,
        "executor_git_sha": GIT.head,
        "executor_script_sha256": subject._executor_sha(),
        "executor_test_evidence_sha256": subject._executor_test_sha(),
    }


@pytest.fixture
def isolated(tmp_path: Path) -> tuple[subject.AcquisitionPaths, Path]:
    taxonomy = tmp_path / "repo" / "database" / "taxonomy"
    release = taxonomy / "sources" / "nortaxa" / subject.VERSION
    release.mkdir(parents=True)
    copies = {
        TAXONOMY / "nortaxa-acquisition.proposal.json": taxonomy / "nortaxa-acquisition.proposal.json",
        TAXONOMY / "nortaxa-source-selection.proposal.json": taxonomy / "nortaxa-source-selection.proposal.json",
        TAXONOMY / "sources/nortaxa/1.284/request.json": release / "request.json",
        TAXONOMY / "sources/nortaxa/1.284/policy-resolution.json": release / "policy-resolution.json",
        TAXONOMY / "sources/nortaxa/1.284/metadata-verification.json": release / "metadata-verification.json",
        TAXONOMY / "sources/nortaxa/1.284/metadata-verification-attempt-4.json": release / "metadata-verification-attempt-4.json",
    }
    for source, target in copies.items():
        shutil.copyfile(source, target)
    paths = subject.AcquisitionPaths(taxonomy_root=taxonomy, repository_root=tmp_path / "repo")
    approval_path = tmp_path / "synthetic" / "nortaxa-acquisition.approved.json"
    approval_path.parent.mkdir()
    approval_path.write_text(json.dumps(approval()), encoding="utf-8")
    return paths, approval_path


def run(
    isolated: tuple[subject.AcquisitionPaths, Path],
    transport: FakeTransport,
    **kwargs,
):
    paths, approval_path = isolated
    return subject.acquire(
        paths=paths,
        approval_path=approval_path,
        transport=transport,
        git_state=kwargs.pop("git_state", GIT),
        clock=kwargs.pop("clock", lambda: NOW),
        free_space=kwargs.pop("free_space", lambda path: subject.MAXIMUM_BYTES),
        **kwargs,
    )


def rewrite_approval(isolated, mutate) -> None:
    _, path = isolated
    raw = json.loads(path.read_text())
    mutate(raw)
    path.write_text(json.dumps(raw), encoding="utf-8")


def valid_payload() -> bytes:
    return VALID_ARCHIVE.read_bytes()


def assert_no_partial(paths: subject.AcquisitionPaths) -> None:
    assert not list(paths.release.rglob("*.part"))


def test_missing_approval_fails_closed(isolated) -> None:
    paths, approval_path = isolated
    approval_path.unlink()
    with pytest.raises(AcquisitionError, match="cannot load"):
        run((paths, approval_path), FakeTransport(valid_payload()))


def test_unauthorized_proposal_cannot_be_used_as_approval(isolated) -> None:
    paths, _ = isolated
    transport = FakeTransport(valid_payload())
    with pytest.raises(AcquisitionError, match="approval fields differ"):
        subject.acquire(
            paths=paths,
            approval_path=paths.proposal,
            transport=transport,
            git_state=GIT,
            clock=lambda: NOW,
        )
    assert transport.calls == 0


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("approval_status", "proposed", "approval_status"),
        ("download_authorized", False, "download_authorized"),
        ("acquisition_proposal_sha256", "0" * 64, "acquisition_proposal"),
        ("metadata_verification_sha256", "0" * 64, "metadata_verification"),
        ("attempt_4_sha256", "0" * 64, "attempt_4"),
        ("source_code", "other", "source_code"),
        ("version", "latest", "version"),
        ("approved_canonical_endpoint", "https://example.org/a.zip", "endpoint"),
        ("allowed_hosts", ["example.org"], "allowed_hosts"),
        ("approved_redirect_hosts", ["ipt.artsdatabanken.no"], "redirect_hosts"),
        ("approved_maximum_bytes", subject.MAXIMUM_BYTES + 1, "ceiling"),
        ("permitted_get_attempts", 2, "permitted_get_attempts"),
        ("redirects_authorized", True, "redirects_authorized"),
        ("range_requests_authorized", True, "range_requests"),
        ("retries_authorized", True, "retries_authorized"),
        ("resume_authorized", True, "resume_authorized"),
        ("fallback_endpoint_authorized", True, "fallback_endpoint_authorized"),
        ("authentication_authorized", True, "authentication_authorized"),
        ("cookies_authorized", True, "cookies_authorized"),
        ("conditional_requests_authorized", True, "conditional_requests_authorized"),
        ("executor_git_sha", "c" * 40, "Git SHA"),
        ("executor_script_sha256", "d" * 64, "script hash"),
        ("executor_test_evidence_sha256", "z" * 64, "test evidence"),
        ("superseded_by", "new-approval", "superseded"),
    ],
)
def test_invalid_or_authority_expanding_approval_rejected(
    isolated, field, value, message,
) -> None:
    rewrite_approval(isolated, lambda raw: raw.__setitem__(field, value))
    with pytest.raises(AcquisitionError, match=message):
        run(isolated, FakeTransport(valid_payload()))


def test_unknown_approval_field_rejected(isolated) -> None:
    rewrite_approval(isolated, lambda raw: raw.__setitem__("retry_count", 1))
    with pytest.raises(AcquisitionError, match="unknown"):
        run(isolated, FakeTransport(valid_payload()))


def test_expired_approval_rejected(isolated) -> None:
    rewrite_approval(isolated, lambda raw: raw.__setitem__("expires_at", "2026-07-26T11:00:00Z"))
    with pytest.raises(AcquisitionError, match="expired"):
        run(isolated, FakeTransport(valid_payload()))


def test_not_yet_valid_approval_rejected(isolated) -> None:
    rewrite_approval(isolated, lambda raw: raw.__setitem__("approved_at", "2026-07-27T00:00:00Z"))
    with pytest.raises(AcquisitionError, match="not currently valid"):
        run(isolated, FakeTransport(valid_payload()))


def test_dirty_worktree_and_wrong_head_rejected(isolated) -> None:
    with pytest.raises(AcquisitionError, match="clean"):
        run(isolated, FakeTransport(valid_payload()), git_state=subject.GitState(GIT.head, False))
    with pytest.raises(AcquisitionError, match="Git SHA"):
        run(isolated, FakeTransport(valid_payload()), git_state=subject.GitState("f" * 40, True))


def test_clean_but_uncommitted_executor_or_test_evidence_is_rejected(isolated) -> None:
    state = subject.GitState(
        head=GIT.head,
        clean=True,
        committed_executor_sha256="0" * 64,
        committed_test_sha256=GIT.committed_test_sha256,
    )
    with pytest.raises(AcquisitionError, match="must be committed"):
        run(isolated, FakeTransport(valid_payload()), git_state=state)


@pytest.mark.parametrize(
    ("relative", "message"),
    [
        ("nortaxa-acquisition.proposal.json", "acquisition proposal"),
        ("sources/nortaxa/1.284/metadata-verification.json", "metadata verification"),
        ("sources/nortaxa/1.284/metadata-verification-attempt-4.json", "attempt 4"),
    ],
)
def test_tampered_bound_artifact_rejected(isolated, relative, message) -> None:
    paths, _ = isolated
    path = paths.taxonomy_root / relative
    raw = json.loads(path.read_text())
    raw["tampered"] = True
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(AcquisitionError, match=message):
        run(isolated, FakeTransport(valid_payload()))


@pytest.mark.parametrize("declared", [True, False])
def test_declared_and_undeclared_streams_promote_once(isolated, declared) -> None:
    payload = valid_payload()
    headers = {"Content-Type": "application/zip;charset=UTF-8"}
    if declared:
        headers["Content-Length"] = str(len(payload))
    transport = FakeTransport(payload, headers=headers)
    result = run(isolated, transport)
    paths, _ = isolated
    assert transport.calls == 1
    assert transport.closed == 1
    assert result["observed_bytes"] == len(payload)
    assert result["archive_sha256"] == hashlib.sha256(payload).hexdigest()
    assert paths.final_archive.read_bytes() == payload
    assert paths.result.exists()
    assert_no_partial(paths)


def test_redirect_status_and_changed_final_url_rejected(isolated) -> None:
    transport = FakeTransport(valid_payload(), status=302)
    with pytest.raises(AcquisitionError, match="HTTP 200"):
        run(isolated, transport)
    assert transport.calls == 1
    assert_no_partial(isolated[0])


def test_changed_final_url_is_rejected_without_body_read(isolated) -> None:
    transport = FakeTransport(
        valid_payload(),
        final_url="https://ipt.artsdatabanken.no/archive.do?r=artsnavnebase&v=1.285",
    )
    with pytest.raises(AcquisitionError, match="final URL"):
        run(isolated, transport)
    assert transport.stream.requests == []


def test_transparently_encoded_response_is_rejected_without_body_read(isolated) -> None:
    transport = FakeTransport(
        valid_payload(),
        headers={
            "Content-Type": "application/zip",
            "Content-Encoding": "gzip",
        },
    )
    with pytest.raises(AcquisitionError, match="Content-Encoding"):
        run(isolated, transport)
    assert transport.stream.requests == []


@pytest.mark.parametrize(
    ("value", "accepted"),
    [
        ("123, 123", True),
        ("123, 124", False),
    ],
)
def test_repeated_content_length_values_are_preserved_for_policy(value, accepted) -> None:
    response = subject.TransportResponse(
        status=200,
        final_url=subject.CANONICAL_ENDPOINT,
        headers={"Content-Type": "application/zip", "Content-Length": value},
        stream=io.BytesIO(),
    )
    if accepted:
        _, observed = subject._validate_response(response, subject.MAXIMUM_BYTES)
        assert observed == value
    else:
        with pytest.raises(AcquisitionError, match="conflicting"):
            subject._validate_response(response, subject.MAXIMUM_BYTES)


def test_case_variant_content_length_fields_cannot_hide_a_conflict() -> None:
    response = subject.TransportResponse(
        status=200,
        final_url=subject.CANONICAL_ENDPOINT,
        headers={
            "Content-Type": "application/zip",
            "Content-Length": "123",
            "content-length": "124",
        },
        stream=io.BytesIO(),
    )
    with pytest.raises(AcquisitionError, match="conflicting"):
        subject._validate_response(response, subject.MAXIMUM_BYTES)


def test_empty_body_rejected_and_deleted(isolated) -> None:
    with pytest.raises(AcquisitionError, match="empty"):
        run(isolated, FakeTransport(b""))
    assert_no_partial(isolated[0])


def test_exact_ceiling_boundary_succeeds(isolated) -> None:
    payload = valid_payload()
    rewrite_approval(isolated, lambda raw: raw.__setitem__("approved_maximum_bytes", len(payload)))
    transport = FakeTransport(
        payload,
        headers={"Content-Type": "application/zip", "Content-Length": str(len(payload))},
    )
    assert run(isolated, transport)["observed_bytes"] == len(payload)
    assert transport.stream.requests[-1] == 1


def test_one_byte_overflow_is_detected_before_write_and_partial_deleted(isolated) -> None:
    payload = valid_payload()
    ceiling = len(payload)
    rewrite_approval(isolated, lambda raw: raw.__setitem__("approved_maximum_bytes", ceiling))
    transport = FakeTransport(payload + b"x")
    with pytest.raises(AcquisitionError, match="exceeds"):
        run(isolated, transport)
    assert transport.stream.requests == [ceiling + 1]
    assert_no_partial(isolated[0])
    assert not isolated[0].final_archive.exists()


@pytest.mark.parametrize(
    ("delta", "message"),
    [(-1, "disagrees"), (1, "disagrees")],
)
def test_truncated_and_overlong_declared_bodies(isolated, delta, message) -> None:
    payload = valid_payload()
    transport = FakeTransport(
        payload,
        headers={"Content-Type": "application/zip", "Content-Length": str(len(payload) + delta)},
    )
    with pytest.raises(AcquisitionError, match=message):
        run(isolated, transport)
    assert_no_partial(isolated[0])


def test_malformed_and_over_ceiling_declared_length_fail_before_body_read(isolated) -> None:
    for value in ("bogus", str(subject.MAXIMUM_BYTES + 1)):
        paths, approval_path = isolated
        transport = FakeTransport(
            valid_payload(),
            headers={"Content-Type": "application/zip", "Content-Length": value},
        )
        with pytest.raises(AcquisitionError, match="Content-Length"):
            run((paths, approval_path), transport)
        assert transport.stream.requests == []
        # Each approval is consumed after transport open, so reset isolated
        # durable state only within this parameter's synthetic test directory.
        paths.attempt.unlink()


def test_interrupted_read_consumes_attempt_and_deletes_partial(isolated) -> None:
    transport = FakeTransport(valid_payload(), fail_after=1)
    with pytest.raises(OSError, match="interrupted"):
        run(isolated, transport)
    paths, _ = isolated
    assert paths.attempt.exists()
    assert_no_partial(paths)
    with pytest.raises(AcquisitionError, match="already consumed"):
        run(isolated, FakeTransport(valid_payload()))


def test_recovery_deletes_hard_crash_partial_without_transport(isolated) -> None:
    transport = FakeTransport(valid_payload(), fail_after=1)
    with pytest.raises(OSError, match="interrupted"):
        run(isolated, transport)
    paths, _ = isolated
    paths.staging.mkdir()
    orphan = paths.staging / f"nortaxa-{subject.VERSION}-hard-crash.part"
    orphan.write_bytes(b"partial")
    forbidden = FakeTransport(valid_payload())
    with pytest.raises(AcquisitionError, match="already consumed"):
        run(isolated, forbidden)
    assert forbidden.calls == 0
    assert not orphan.exists()


def test_inadequate_space_does_not_consume_or_invoke_transport(isolated) -> None:
    transport = FakeTransport(valid_payload())
    with pytest.raises(AcquisitionError, match="insufficient"):
        run(isolated, transport, free_space=lambda path: subject.MAXIMUM_BYTES - 1)
    assert transport.calls == 0
    assert not isolated[0].attempt.exists()


def test_free_space_is_checked_on_destination_filesystem(isolated) -> None:
    observed = []
    reports = []
    devices = []
    transport = FakeTransport(valid_payload())
    paths, _ = isolated

    def available(path):
        assert path == paths.staging
        assert path.exists()
        assert path.is_dir()
        assert not path.is_symlink()
        observed.append(path)
        devices.append(os.stat(path, follow_symlinks=False).st_dev)
        return subject.MAXIMUM_BYTES

    run(
        isolated,
        transport,
        free_space=available,
        preflight_report=lambda expected, free, path: reports.append((expected, free, path)),
    )
    assert observed == [paths.staging]
    assert reports == [(subject.MAXIMUM_BYTES, subject.MAXIMUM_BYTES, paths.staging)]
    assert devices == [os.stat(paths.final_archive.parent, follow_symlinks=False).st_dev]
    assert not paths.staging.exists()


def test_existing_final_archive_is_never_overwritten(isolated) -> None:
    paths, _ = isolated
    paths.final_archive.write_bytes(b"existing")
    with pytest.raises(AcquisitionError, match="already exists"):
        run(isolated, FakeTransport(valid_payload()))
    assert paths.final_archive.read_bytes() == b"existing"


def test_symlinked_staging_and_lock_are_rejected_before_transport(isolated, tmp_path) -> None:
    paths, _ = isolated
    outside = tmp_path / "outside"
    outside.mkdir()
    paths.staging.symlink_to(outside, target_is_directory=True)
    transport = FakeTransport(valid_payload())
    with pytest.raises(AcquisitionError, match="real directory"):
        run(isolated, transport)
    assert transport.calls == 0
    paths.staging.unlink()
    paths.lock.unlink()
    target = outside / "lock"
    target.touch()
    paths.lock.symlink_to(target)
    with pytest.raises(AcquisitionError, match="safely open"):
        run(isolated, transport)
    assert transport.calls == 0


@pytest.mark.parametrize("state_name", ["attempt", "receipt", "result", "final_archive"])
def test_symlinked_durable_state_paths_are_rejected_before_transport(
    isolated, tmp_path, state_name,
) -> None:
    paths, _ = isolated
    outside = tmp_path / f"outside-{state_name}"
    outside.write_bytes(b"unrelated")
    getattr(paths, state_name).symlink_to(outside)
    transport = FakeTransport(valid_payload())
    with pytest.raises(AcquisitionError, match="real regular file|already exists"):
        run(isolated, transport)
    assert transport.calls == 0
    assert outside.read_bytes() == b"unrelated"


def test_lock_contention_and_concurrent_invocation_are_deterministic(isolated) -> None:
    paths, _ = isolated
    paths.lock.touch()
    with paths.lock.open("a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        transport = FakeTransport(valid_payload())
        with pytest.raises(subject.LockContentionError):
            run(isolated, transport)
        assert transport.calls == 0


def test_preexisting_consumed_authorization_refuses_transport(isolated) -> None:
    paths, _ = isolated
    paths.attempt.write_text(json.dumps({"state": "network_attempt_consumed"}))
    transport = FakeTransport(valid_payload())
    with pytest.raises(AcquisitionError, match="already consumed"):
        run(isolated, transport)
    assert transport.calls == 0


def test_structural_zip_failure_is_deleted_and_never_promoted(isolated) -> None:
    with pytest.raises((AcquisitionError, OSError)):
        run(isolated, FakeTransport(b"not-a-zip"))
    paths, _ = isolated
    assert paths.attempt.exists()
    assert_no_partial(paths)
    assert not paths.final_archive.exists()


@pytest.mark.parametrize(
    "transition_name",
    [
        "attempt_consumed",
        "staging_created",
        "stream_complete",
        "structural_validation_complete",
        "promotion_receipt_persisted",
    ],
)
def test_crashes_before_promotion_never_restore_get_authority(isolated, transition_name) -> None:
    class SyntheticCrash(BaseException):
        pass

    def crash(state):
        if state == transition_name:
            raise SyntheticCrash(state)

    with pytest.raises(SyntheticCrash):
        run(isolated, FakeTransport(valid_payload()), transition=crash)
    assert isolated[0].attempt.exists()
    assert not isolated[0].final_archive.exists()
    assert_no_partial(isolated[0])
    retry = FakeTransport(valid_payload())
    with pytest.raises(AcquisitionError, match="already consumed"):
        run(isolated, retry)
    assert retry.calls == 0


def test_crash_before_attempt_consumption_leaves_authority_unused(isolated) -> None:
    class SyntheticCrash(BaseException):
        pass

    def crash(state):
        if state == "local_preflight_complete":
            raise SyntheticCrash

    with pytest.raises(SyntheticCrash):
        run(isolated, FakeTransport(valid_payload()), transition=crash)
    assert not isolated[0].attempt.exists()


@pytest.mark.parametrize("failed_fsync", range(1, 9))
def test_every_file_and_directory_fsync_failure_fails_closed(
    isolated, monkeypatch, failed_fsync,
) -> None:
    original = subject.os.fsync
    calls = 0

    def injected(descriptor):
        nonlocal calls
        calls += 1
        if calls == failed_fsync:
            kind = "directory" if stat.S_ISDIR(os.fstat(descriptor).st_mode) else "file"
            raise OSError(f"synthetic {kind} fsync failure {failed_fsync}")
        return original(descriptor)

    monkeypatch.setattr(subject.os, "fsync", injected)
    transport = FakeTransport(valid_payload())
    with pytest.raises(OSError, match="fsync failure"):
        run(isolated, transport)
    paths, _ = isolated
    assert transport.calls == (0 if failed_fsync <= 2 else 1)
    assert paths.attempt.exists() is (failed_fsync >= 2)
    assert paths.final_archive.exists() is (failed_fsync >= 6)
    if failed_fsync < 6:
        assert_no_partial(paths)


def test_recovery_after_promotion_validates_archive_without_another_get(isolated) -> None:
    class SyntheticCrash(BaseException):
        pass

    def crash(state):
        if state == "archive_promoted":
            raise SyntheticCrash

    first = FakeTransport(valid_payload())
    with pytest.raises(SyntheticCrash):
        run(isolated, first, transition=crash)
    paths, _ = isolated
    assert first.calls == 1
    assert paths.final_archive.exists()
    assert not paths.result.exists()
    forbidden = FakeTransport(valid_payload())
    result = run(isolated, forbidden)
    assert forbidden.calls == 0
    assert result["recovered_after_promotion"] is True
    assert paths.result.exists()


def test_recovery_after_archive_link_before_staging_unlink_uses_no_transport(isolated) -> None:
    class SyntheticCrash(BaseException):
        pass

    def crash(state):
        if state == "archive_linked":
            raise SyntheticCrash

    with pytest.raises(SyntheticCrash):
        run(isolated, FakeTransport(valid_payload()), transition=crash)
    paths, _ = isolated
    assert paths.final_archive.exists()
    assert list(paths.staging.glob("*.part"))
    forbidden = FakeTransport(valid_payload())
    result = run(isolated, forbidden)
    assert forbidden.calls == 0
    assert result["recovered_after_promotion"] is True
    assert_no_partial(paths)


def test_pre_promotion_receipt_never_reports_acquisition_success(isolated) -> None:
    class SyntheticCrash(BaseException):
        pass

    def crash(state):
        if state == "promotion_receipt_persisted":
            raise SyntheticCrash

    with pytest.raises(SyntheticCrash):
        run(isolated, FakeTransport(valid_payload()), transition=crash)
    receipt = json.loads(isolated[0].receipt.read_text())
    assert receipt["state"] == "validated_ready_for_atomic_promotion"
    assert receipt["validation"]["status"] == "structurally_validated"
    assert '"result": "passed"' not in isolated[0].receipt.read_text()


def test_pre_promotion_receipt_cannot_adopt_unrelated_identical_archive(isolated) -> None:
    class SyntheticCrash(BaseException):
        pass

    def crash(state):
        if state == "promotion_receipt_persisted":
            raise SyntheticCrash

    with pytest.raises(SyntheticCrash):
        run(isolated, FakeTransport(valid_payload()), transition=crash)
    paths, _ = isolated
    shutil.copyfile(VALID_ARCHIVE, paths.final_archive)
    forbidden = FakeTransport(valid_payload())
    with pytest.raises(AcquisitionError, match="receipt-bound staging file"):
        run(isolated, forbidden)
    assert forbidden.calls == 0


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("state", "passed", "receipt"),
        ("attempt_sha256", "0" * 64, "consumed authorization"),
        ("staged_inode", 1, "receipt-bound staging file"),
    ],
)
def test_malicious_recovery_receipt_combinations_fail_closed(
    isolated, field, value, message,
) -> None:
    class SyntheticCrash(BaseException):
        pass

    def crash(state):
        if state == "archive_promoted":
            raise SyntheticCrash

    with pytest.raises(SyntheticCrash):
        run(isolated, FakeTransport(valid_payload()), transition=crash)
    paths, _ = isolated
    receipt = json.loads(paths.receipt.read_text())
    receipt[field] = value
    paths.receipt.write_text(json.dumps(receipt), encoding="utf-8")
    forbidden = FakeTransport(valid_payload())
    with pytest.raises(AcquisitionError, match=message):
        run(isolated, forbidden)
    assert forbidden.calls == 0


def test_result_is_not_reported_before_durable_result_record(isolated, monkeypatch) -> None:
    original = subject._write_json_exclusive

    def fail_result(path, value):
        if path == isolated[0].result:
            raise OSError("synthetic result fsync failure")
        return original(path, value)

    monkeypatch.setattr(subject, "_write_json_exclusive", fail_result)
    with pytest.raises(OSError, match="result"):
        run(isolated, FakeTransport(valid_payload()))
    assert isolated[0].final_archive.exists()
    assert not isolated[0].result.exists()


def test_success_result_is_append_only_and_idempotently_read(isolated) -> None:
    first = run(isolated, FakeTransport(valid_payload()))
    second_transport = FakeTransport(valid_payload())
    second = run(isolated, second_transport)
    assert second_transport.calls == 0
    assert second == first


def test_completed_result_refuses_tampered_archive_without_transport(isolated) -> None:
    run(isolated, FakeTransport(valid_payload()))
    paths, _ = isolated
    paths.final_archive.write_bytes(paths.final_archive.read_bytes() + b"tampered")
    retry = FakeTransport(valid_payload())
    with pytest.raises(AcquisitionError, match="differs"):
        run(isolated, retry)
    assert retry.calls == 0


def test_stream_that_violates_bounded_read_contract_is_rejected(isolated) -> None:
    class OversizedRead:
        def read(self, requested):
            return b"x" * (requested + 1)

    class BadTransport:
        calls = 0

        def open(self, endpoint):
            self.calls += 1
            return subject.TransportResponse(
                200, subject.CANONICAL_ENDPOINT,
                {"Content-Type": "application/zip"}, OversizedRead(),
            )

    transport = BadTransport()
    with pytest.raises(AcquisitionError, match="more bytes than requested"):
        run(isolated, transport)
    assert transport.calls == 1
    assert_no_partial(isolated[0])


def test_crash_after_result_persistence_recovers_as_completed(isolated) -> None:
    class SyntheticCrash(BaseException):
        pass

    def crash(state):
        if state == "result_persisted":
            raise SyntheticCrash

    with pytest.raises(SyntheticCrash):
        run(isolated, FakeTransport(valid_payload()), transition=crash)
    retry = FakeTransport(valid_payload())
    result = run(isolated, retry)
    assert retry.calls == 0
    assert result["result"] == "passed"


def test_production_adapter_builds_one_plain_get_without_prohibited_headers(
    isolated, monkeypatch,
) -> None:
    payload = valid_payload()
    opened = []

    class Response(io.BytesIO):
        status = 200
        headers = {"Content-Type": "application/zip", "Content-Length": str(len(payload))}

        def geturl(self):
            return subject.CANONICAL_ENDPOINT

    class Opener:
        def open(self, request, timeout):
            opened.append((request, timeout))
            return Response(payload)

    monkeypatch.setattr(subject, "build_opener", lambda *handlers: Opener())
    adapter = subject.ProductionHTTPTransport()
    response = adapter.open(subject.CANONICAL_ENDPOINT)
    assert response.stream.tell() == 0
    assert response.stream.read() == payload
    assert len(opened) == 1
    request, timeout = opened[0]
    assert request.get_method() == "GET"
    assert timeout == subject.TIMEOUT_SECONDS
    prohibited = {
        "authorization", "cookie", "range", "if-match", "if-none-match",
        "if-modified-since", "if-unmodified-since",
    }
    assert not prohibited.intersection(key.casefold() for key in request.headers)
    assert not prohibited.intersection(key.casefold() for key in adapter.request_headers)
    assert request.get_header("Accept-encoding") == "identity"
    with pytest.raises(AcquisitionError, match="exactly one"):
        adapter.open(subject.CANONICAL_ENDPOINT)


def test_production_adapter_preserves_repeated_raw_content_lengths(monkeypatch) -> None:
    headers = Message()
    headers.add_header("Content-Type", "application/zip")
    headers.add_header("Content-Length", "10")
    headers.add_header("Content-Length", "11")

    class Response(io.BytesIO):
        status = 200

        def __init__(self):
            super().__init__(b"")
            self.headers = headers

        def geturl(self):
            return subject.CANONICAL_ENDPOINT

    class Opener:
        def open(self, request, timeout):
            return Response()

    monkeypatch.setattr(subject, "build_opener", lambda *handlers: Opener())
    response = subject.ProductionHTTPTransport().open(subject.CANONICAL_ENDPOINT)
    assert response.headers["content-length"] == "10, 11"
    with pytest.raises(AcquisitionError, match="conflicting"):
        subject._validate_response(response, subject.MAXIMUM_BYTES)
    assert response.stream.tell() == 0


def test_response_evidence_is_allowlisted_and_bounded(isolated) -> None:
    headers = {
        "Content-Type": "application/zip",
        "X-Secret-Debug": "must-not-persist",
        "Set-Cookie": "must-not-persist",
    }
    result = run(isolated, FakeTransport(valid_payload(), headers=headers))
    persisted = json.dumps(result["response_metadata"])
    assert "X-Secret" not in persisted
    assert "Set-Cookie" not in persisted
    assert set(result["response_metadata"]["headers"]) == subject.EVIDENCE_HEADERS
