"""Focused offline tests for the simplified NorTaxa acquisition executor.

Manual invocation of ``acquire_nortaxa.py --execute`` is the acquisition
authorization. There is no approval, readiness, or proposal artifact at runtime.
Each invocation opens at most one HTTP GET. No automatic retry, no automatic
recovery — a stale ``.part`` causes the next invocation to refuse.
"""
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
import subprocess
import sys
import zipfile
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

    def read(self, size: int = -1) -> bytes:
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
        self.headers = headers or {"content-type": "application/zip"}
        self.calls = 0

    def open(self, endpoint: str) -> subject.TransportResponse:
        self.calls += 1
        assert endpoint == subject.CANONICAL_ENDPOINT
        return subject.TransportResponse(
            status=self.status, final_url=self.final_url,
            headers=self.headers, stream=self.stream,
        )


@pytest.fixture
def release(tmp_path: Path) -> subject.ReleasePaths:
    taxonomy = tmp_path / "database" / "taxonomy"
    rel = taxonomy / "sources" / "nortaxa" / subject.VERSION
    rel.mkdir(parents=True)
    for src, dst in {
        TAXONOMY / "nortaxa-source-selection.proposal.json": taxonomy / "nortaxa-source-selection.proposal.json",
        TAXONOMY / "sources/nortaxa/1.284/request.json": rel / "request.json",
    }.items():
        shutil.copyfile(src, dst)
    manifest = {
        "manifest_schema_version": 2,
        "source_code": "nortaxa",
        "profile_code": "nortaxa_dwca",
        "release": {"issued_date": "2026-07-17", "version": "1.284"},
        "state": "planned",
        "download": None,
        "validation": None,
        "execution_attempts": [],
    }
    (rel / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return subject.ReleasePaths(taxonomy_root=taxonomy)


def run(release: subject.ReleasePaths, transport: FakeTransport, **kwargs) -> dict:
    return subject.acquire(
        release, transport=transport, clock=kwargs.pop("clock", lambda: NOW),
        free_space=kwargs.pop("free_space", lambda p: subject.MAXIMUM_BYTES),
        **kwargs,
    )


def valid_payload() -> bytes:
    return VALID_ARCHIVE.read_bytes()


# ----- --execute flag required -----


def test_cli_refuses_without_execute() -> None:
    """Invocation without --execute exits 2 and describes the manual auth model."""
    result = subprocess.run(
        [sys.executable, str(SCRIPTS / "acquire_nortaxa.py")],
        capture_output=True, text=True,
    )
    assert result.returncode == 2
    assert "--execute" in result.stderr
    assert "manual" in result.stderr.lower()


def test_cli_execute_flag_is_the_authorization() -> None:
    parser = subject.build_parser()
    assert parser.parse_args(["--execute"]).execute is True
    assert parser.parse_args([]).execute is False


# ----- No approval / readiness / proposal machinery remains -----


def test_no_approval_readiness_or_proposal_module_globals() -> None:
    for absent in (
        "ACQUISITION_PROPOSAL_SHA256", "APPROVAL_KEYS", "GitState",
        "validate_approval", "current_git_state", "MAXIMUM_APPROVAL_LIFETIME_SECONDS",
        "SUPPLEMENTAL_ATTEMPT_AUTHORIZED_OPERATIONS",
    ):
        assert not hasattr(subject, absent), absent


# ----- Successful acquisition -----


def test_successful_stream_promotes_archive_and_records_manifest(release) -> None:
    transport = FakeTransport(valid_payload())
    manifest = run(release, transport)
    assert transport.calls == 1
    assert release.archive.exists()
    assert release.archive.stat().st_size == len(valid_payload())
    assert hashlib.sha256(release.archive.read_bytes()).hexdigest() == manifest["download"]["archive_sha256"]
    assert manifest["state"] == "validated"
    attempts = manifest["execution_attempts"]
    assert len(attempts) == 1
    a = attempts[0]
    assert a["outcome"] == "succeeded"
    assert a["phase"] == "validated_and_promoted"
    assert a["endpoint"] == subject.CANONICAL_ENDPOINT
    assert a["observed_bytes"] == len(valid_payload())
    assert not release.staging.exists()


# ----- No existing archive overwrite -----


def test_refuses_when_archive_already_exists(release) -> None:
    release.archive.write_bytes(b"existing")
    with pytest.raises(AcquisitionError, match="already exists"):
        run(release, FakeTransport(valid_payload()))


def test_exclusive_promotion_no_overwrite(release) -> None:
    """Even if a race allowed archive.zip to appear during streaming, promotion
    refuses to overwrite via os.link(follow_symlinks=False)."""
    class LatePromoter(FakeTransport):
        def __init__(self, payload):
            super().__init__(payload)
            self._first_read = True

        def open(self, endpoint):
            resp = super().open(endpoint)
            original_read = resp.stream.read

            def racy(size=-1):
                nonlocal LatePromoter
                data = original_read(size)
                if self._first_read and data:
                    self._first_read = False
                    release.archive.write_bytes(b"raced-in")
                return data

            resp.stream.read = racy
            return resp

    with pytest.raises(AcquisitionError, match="already exists|appeared before"):
        run(release, LatePromoter(valid_payload()))


# ----- One transport call, no automatic retry -----


def test_exactly_one_transport_call_per_invocation(release) -> None:
    transport = FakeTransport(valid_payload())
    run(release, transport)
    assert transport.calls == 1


def test_transport_error_does_not_retry(release) -> None:
    class Once(FakeTransport):
        def open(self, endpoint):
            if self.calls >= 1:
                raise AcquisitionError("second invocation")
            return super().open(endpoint)

    transport = Once(valid_payload(), fail_after=100)
    with pytest.raises(OSError):
        run(release, transport)
    assert transport.calls == 1


# ----- Response gate: redirect / final URL / content type / encoding -----


def test_non_200_status_rejected(release) -> None:
    transport = FakeTransport(valid_payload(), status=302)
    with pytest.raises(AcquisitionError, match="HTTP 200"):
        run(release, transport)


def test_changed_final_url_rejected(release) -> None:
    transport = FakeTransport(valid_payload(),
                              final_url="https://example.org/archive.zip")
    with pytest.raises(AcquisitionError, match="final URL"):
        run(release, transport)


def test_unsupported_content_type_rejected(release) -> None:
    transport = FakeTransport(valid_payload(),
                              headers={"content-type": "text/html"})
    with pytest.raises(AcquisitionError, match="Content-Type"):
        run(release, transport)


def test_non_identity_content_encoding_rejected(release) -> None:
    transport = FakeTransport(valid_payload(), headers={
        "content-type": "application/zip", "content-encoding": "gzip",
    })
    with pytest.raises(AcquisitionError, match="Content-Encoding"):
        run(release, transport)


def test_identity_content_encoding_accepted(release) -> None:
    transport = FakeTransport(valid_payload(), headers={
        "content-type": "application/zip", "content-encoding": "identity",
    })
    run(release, transport)
    assert release.archive.exists()


# ----- Content-Length policy -----


def test_repeated_identical_content_lengths_accepted(release) -> None:
    n = len(valid_payload())
    transport = FakeTransport(valid_payload(), headers={
        "content-type": "application/zip", "content-length": f"{n}, {n}",
    })
    run(release, transport)


def test_conflicting_content_lengths_rejected(release) -> None:
    transport = FakeTransport(valid_payload(), headers={
        "content-type": "application/zip", "content-length": "10, 999999",
    })
    with pytest.raises(AcquisitionError, match="conflicting"):
        run(release, transport)


def test_over_ceiling_declared_length_rejected(release) -> None:
    transport = FakeTransport(valid_payload(), headers={
        "content-type": "application/zip",
        "content-length": str(subject.MAXIMUM_BYTES + 1),
    })
    with pytest.raises(AcquisitionError, match="exceeds"):
        run(release, transport)


def test_truncated_declared_body_rejected(release) -> None:
    payload = valid_payload()
    transport = FakeTransport(payload, headers={
        "content-type": "application/zip",
        "content-length": str(len(payload) + 1),
    })
    with pytest.raises(AcquisitionError):
        run(release, transport)


# ----- Streaming ceiling / overflow -----


def test_ceiling_boundary_stream_at_max_bytes_streams_but_fails_validation(release) -> None:
    """A payload exactly at the ceiling streams cleanly; then structural
    validation fires because arbitrary bytes are not a valid ZIP."""
    payload = b"x" * subject.MAXIMUM_BYTES
    transport = FakeTransport(payload)
    with pytest.raises(AcquisitionError):
        run(release, transport)
    # Failure recorded and staging cleaned.
    m = json.loads(release.manifest.read_text())
    assert m["execution_attempts"][-1]["outcome"] == "failed"
    assert not release.archive.exists()


def test_one_byte_overflow_rejected(release) -> None:
    payload = b"x" * (subject.MAXIMUM_BYTES + 1)
    transport = FakeTransport(payload)
    with pytest.raises(AcquisitionError, match="exceeds"):
        run(release, transport)


def test_interrupted_stream_deletes_partial_and_records_failure(release) -> None:
    payload = valid_payload()
    transport = FakeTransport(payload, fail_after=len(payload) // 2)
    with pytest.raises(OSError):
        run(release, transport)
    assert not release.archive.exists()
    # Temp files removed; staging directory removed.
    assert not release.staging.exists()
    manifest = json.loads(release.manifest.read_text())
    a = manifest["execution_attempts"][-1]
    assert a["outcome"] == "failed"
    assert a["phase"] == "streaming"


# ----- Structural validation failure cleanup -----


def test_structural_validation_failure_cleans_up_and_records(release, monkeypatch) -> None:
    def failing_validate(_path, _request):
        raise AcquisitionError("unsupported extension row type: http://example/x")

    monkeypatch.setattr(subject, "validate_fixture", failing_validate)
    with pytest.raises(AcquisitionError, match="unsupported extension"):
        run(release, FakeTransport(valid_payload()))
    assert not release.archive.exists()
    assert not release.staging.exists()
    m = json.loads(release.manifest.read_text())
    a = m["execution_attempts"][-1]
    assert a["outcome"] == "failed"
    assert a["phase"] == "structural_validation"
    assert a["archive_sha256"]
    assert a["observed_bytes"] == len(valid_payload())


def test_manifest_success_entry_has_full_evidence(release) -> None:
    transport = FakeTransport(valid_payload(), headers={
        "content-type": "application/zip",
        "content-length": str(len(valid_payload())),
        "etag": '"synthetic"', "last-modified": "Fri, 17 Jul 2026 00:00:00 GMT",
    })
    manifest = run(release, transport)
    d = manifest["download"]
    assert d["endpoint"] == subject.CANONICAL_ENDPOINT
    assert d["response_headers"]["etag"]["value"] == '"synthetic"'
    assert d["response_headers"]["last-modified"]["value"].startswith("Fri, 17 Jul 2026")
    assert d["observed_bytes"] == len(valid_payload())


# ----- Lock contention -----


def test_lock_contention_is_deterministic(release) -> None:
    """A second concurrent invocation raises LockContentionError immediately."""
    fd = os.open(release.lock, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(subject.LockContentionError):
            run(release, FakeTransport(valid_payload()))
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


# ----- Symlink and unsafe staging rejection -----


def test_staging_symlink_rejected(release, tmp_path) -> None:
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    release.staging.symlink_to(outside)
    with pytest.raises(AcquisitionError, match="real directory"):
        run(release, FakeTransport(valid_payload()))


def test_stale_part_causes_refusal(release) -> None:
    """A pre-existing staging payload causes the next invocation to refuse.
    Manual review is required; the executor does NOT auto-recover."""
    release.staging.mkdir()
    (release.staging / "nortaxa-1.284-orphan.part").write_bytes(b"garbage")
    with pytest.raises(AcquisitionError, match="manual review required"):
        run(release, FakeTransport(valid_payload()))


# ----- Free-space guard -----


def test_insufficient_free_space_does_not_open_transport(release) -> None:
    transport = FakeTransport(valid_payload())
    with pytest.raises(AcquisitionError, match="insufficient free space"):
        run(release, transport, free_space=lambda p: 1)
    assert transport.calls == 0


# ----- No cookies / auth / conditional headers in production adapter -----


def test_production_adapter_sends_only_permitted_headers(monkeypatch) -> None:
    """The production adapter builds a Request with only Accept, Accept-Encoding,
    User-Agent — no cookies, no authentication, no Range, no conditional."""
    captured = {}

    class Recorder:
        def open(self, request, timeout):
            captured["headers"] = dict(request.headers)
            raise RuntimeError("stop before wire")

    monkeypatch.setattr(subject, "build_opener", lambda *a, **kw: Recorder())
    with pytest.raises(RuntimeError, match="stop before wire"):
        subject.ProductionHTTPTransport().open(subject.CANONICAL_ENDPOINT)
    keys_lower = {k.lower() for k in captured["headers"]}
    assert "accept-encoding" in keys_lower
    assert captured["headers"].get("Accept-encoding") == "identity"
    for prohibited in ("cookie", "authorization", "range", "if-match",
                       "if-none-match", "if-modified-since"):
        assert prohibited not in keys_lower


def test_production_adapter_only_permits_one_call() -> None:
    t = subject.ProductionHTTPTransport()
    t._used = True
    with pytest.raises(AcquisitionError, match="exactly one GET"):
        t.open(subject.CANONICAL_ENDPOINT)


# ----- Historical evidence is preserved -----


HISTORICAL_APPROVAL = TAXONOMY / "sources" / "nortaxa" / "1.284" / "acquisition-attempt-1-approval.json"
HISTORICAL_JOURNAL  = TAXONOMY / "sources" / "nortaxa" / "1.284" / "acquisition-attempt-1-journal.json"


def _sha256_json(path: Path) -> str:
    d = json.loads(path.read_text())
    canon = json.dumps(d, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canon.encode()).hexdigest()


def test_historical_attempt_1_evidence_bytes_preserved() -> None:
    assert _sha256_json(HISTORICAL_APPROVAL) == \
        "6de49d43812cbc6fe87bb89d329e72f3d24379d30f08b71b1c12d353124eb9e1"
    assert _sha256_json(HISTORICAL_JOURNAL) == \
        "026f3f4060d672deb307e17763ba02f41c8c7d3c22cb958709a3f511913b3d7a"


def test_post_promotion_manifest_write_failure_preserves_archive(release, monkeypatch) -> None:
    """If the final success-manifest write fails AFTER archive.zip has been
    promoted, the archive must remain on disk (not be deleted or redownloaded)
    and the invocation must report the inconsistent state by raising the write
    error. The next invocation must refuse because archive.zip already exists.
    """
    original_update = subject._atomic_update_manifest
    calls = {"count": 0}

    def flaky_update(path, updater):
        calls["count"] += 1
        # Success recorder is invoked exactly once, at the very end. Every prior
        # call in the success flow is either absent (nothing failed) or a
        # failure recorder in the failure branches. Fail only the terminal call
        # AFTER the archive is already linked.
        if release.archive.exists() and not release.staging.exists():
            raise OSError("synthetic manifest-write failure after promotion")
        return original_update(path, updater)

    monkeypatch.setattr(subject, "_atomic_update_manifest", flaky_update)
    with pytest.raises(OSError, match="synthetic manifest-write failure after promotion"):
        run(release, FakeTransport(valid_payload()))
    # Archive stayed put; temp file cleared; no redownload attempted.
    assert release.archive.exists()
    assert release.archive.stat().st_size == len(valid_payload())
    # A second invocation refuses because archive.zip already exists.
    with pytest.raises(AcquisitionError, match="already exists"):
        run(release, FakeTransport(valid_payload()))


def test_lock_contention_does_not_mutate_manifest(release) -> None:
    """Lock contention raises before any manifest write and must leave
    execution_attempts unchanged."""
    before = release.manifest.read_bytes()
    fd = os.open(release.lock, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(subject.LockContentionError):
            run(release, FakeTransport(valid_payload()))
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
    assert release.manifest.read_bytes() == before


def test_pre_lock_failure_does_not_mutate_manifest(release) -> None:
    """A failure before the lock is acquired (e.g. archive.zip already exists)
    must not write to the manifest."""
    release.archive.write_bytes(b"pre-existing")
    before = release.manifest.read_bytes()
    with pytest.raises(AcquisitionError, match="already exists"):
        run(release, FakeTransport(valid_payload()))
    assert release.manifest.read_bytes() == before


def test_unrelated_staging_entry_is_not_removed_on_failure(release) -> None:
    """The cleanup helper only removes the executor's own temporary file and
    the .staging/ directory when it is empty. A pre-existing unrelated entry
    causes the executor to refuse BEFORE transport (see stale-.part test)."""
    release.staging.mkdir()
    unrelated = release.staging / "unrelated-payload.data"
    unrelated.write_bytes(b"do-not-delete")
    with pytest.raises(AcquisitionError, match="manual review required"):
        run(release, FakeTransport(valid_payload()))
    assert unrelated.exists()
    assert unrelated.read_bytes() == b"do-not-delete"


def test_historical_attempt_1_recorded_in_committed_manifest() -> None:
    m = json.loads((TAXONOMY / "sources/nortaxa/1.284/manifest.json").read_text())
    assert m["state"] == "validated"
    a1 = m["execution_attempts"][0]
    assert a1["attempt_number"] == 1
    assert a1["outcome"] == "failed"
    assert a1["approval_sha256"] == "6de49d43812cbc6fe87bb89d329e72f3d24379d30f08b71b1c12d353124eb9e1"
    assert a1["attempt_journal_sha256"] == "026f3f4060d672deb307e17763ba02f41c8c7d3c22cb958709a3f511913b3d7a"
    assert a1["approval_path"] == "acquisition-attempt-1-approval.json"
    assert a1["attempt_journal_path"] == "acquisition-attempt-1-journal.json"
