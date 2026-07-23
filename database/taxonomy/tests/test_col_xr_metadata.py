import json
import sys
from dataclasses import replace
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from col_xr_metadata import (
    MetadataPolicy,
    MetadataResponse,
    ReleaseCandidate,
    verify_fungi_root,
    verify_release_metadata,
)
from refresh_col_xr import AcquisitionError, load_request


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "col_xr"
METADATA_ENDPOINT = "https://api.checklistbank.org/dataset/315834"
FUNGI_ENDPOINT = (
    "https://api.checklistbank.org/dataset/315834/nameusage/search"
    "?q=Fungi&type=EXACT&minRank=KINGDOM&maxRank=KINGDOM&limit=100"
)
PROPOSAL = Path(__file__).resolve().parents[1] / "col-xr-source-selection.proposal.json"


def candidate() -> ReleaseCandidate:
    return ReleaseCandidate(
        release_label="2026-07-17 XR",
        release_type="Extended Release",
        dataset_key=315834,
        issued_date="2026-07-17",
        doi="10.48580/dgykv",
    )


def official_metadata_bytes() -> bytes:
    fixture = json.loads((FIXTURES / "official-release-metadata-315834.json").read_text())
    return json.dumps(fixture["source_fields"], sort_keys=True).encode()


def fungi_bytes() -> bytes:
    return (FIXTURES / "official-fungi-root-315834.json").read_bytes()


class RecordingTransport:
    def __init__(self, response: MetadataResponse):
        self.response = response
        self.calls = []

    def get(self, url, policy):
        self.calls.append((url, policy))
        return self.response


def response(body: bytes, **overrides) -> MetadataResponse:
    values = {
        "status": 200,
        "final_url": METADATA_ENDPOINT,
        "content_type": "application/json; charset=utf-8",
        "body": body,
        "redirect_urls": (),
        "content_length": len(body),
    }
    values.update(overrides)
    return MetadataResponse(**values)


def test_selected_release_metadata_matches_and_hashes_exact_bytes() -> None:
    body = official_metadata_bytes()
    transport = RecordingTransport(response(body))
    verified = verify_release_metadata(candidate(), METADATA_ENDPOINT, transport)
    assert verified.normalized == {
        "release_label": "2026-07-17 XR",
        "release_type": "Extended Release",
        "dataset_key": 315834,
        "issued_date": "2026-07-17",
        "doi": "10.48580/dgykv",
    }
    assert verified.response_sha256 == "0bebafaf33fba6f62cc5e0e4305160e19d1121ef6fc13a36cad8977ff3af7107"
    assert [call[0] for call in transport.calls] == [METADATA_ENDPOINT]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("release_label", "2026-06-12 XR"),
        ("release_type", "Base Release"),
        ("dataset_key", 1),
        ("issued_date", "2026-06-12"),
        ("doi", "10.0000/wrong"),
    ],
)
def test_candidate_metadata_mismatches_are_rejected(field, value) -> None:
    changed = replace(candidate(), **{field: value})
    with pytest.raises(AcquisitionError, match="metadata mismatch"):
        verify_release_metadata(changed, METADATA_ENDPOINT, RecordingTransport(response(official_metadata_bytes())))


@pytest.mark.parametrize("content_type", ["text/html", "text/plain", "application/zip"])
def test_non_json_content_type_is_rejected(content_type) -> None:
    with pytest.raises(AcquisitionError, match="not JSON"):
        verify_release_metadata(
            candidate(),
            METADATA_ENDPOINT,
            RecordingTransport(response(official_metadata_bytes(), content_type=content_type)),
        )


def test_html_disguised_as_json_is_rejected() -> None:
    body = b"<!doctype html><title>login</title>"
    with pytest.raises(AcquisitionError, match="HTML"):
        verify_release_metadata(candidate(), METADATA_ENDPOINT, RecordingTransport(response(body)))


@pytest.mark.parametrize("length_kind", ["header", "body"])
def test_oversize_metadata_is_rejected(length_kind) -> None:
    policy = MetadataPolicy(max_bytes=32)
    body = official_metadata_bytes()
    kwargs = {"content_length": 33} if length_kind == "header" else {"content_length": None}
    with pytest.raises(AcquisitionError, match="size limit"):
        verify_release_metadata(candidate(), METADATA_ENDPOINT, RecordingTransport(response(body, **kwargs)), policy)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"final_url": "https://example.org/dataset/315834"},
        {"redirect_urls": ("https://example.org/redirect",)},
        {"redirect_urls": (
            "https://api.checklistbank.org/1",
            "https://api.checklistbank.org/2",
            "https://api.checklistbank.org/3",
            "https://api.checklistbank.org/4",
        )},
    ],
)
def test_host_and_redirect_policy_is_enforced(kwargs) -> None:
    with pytest.raises(AcquisitionError, match="official|redirect"):
        verify_release_metadata(
            candidate(), METADATA_ENDPOINT, RecordingTransport(response(official_metadata_bytes(), **kwargs))
        )


def fungi_response(body: bytes | None = None) -> MetadataResponse:
    payload = fungi_bytes() if body is None else body
    return response(payload, final_url=FUNGI_ENDPOINT, content_length=len(payload))


def test_exact_fungi_root_evidence_is_unambiguous() -> None:
    root = verify_fungi_root(fungi_response(), dataset_key=315834)
    assert root["usage_id"] == "F"
    assert root["identifier_type"] == "opaque string"
    assert root["rank"] == "kingdom"
    assert root["status"] == "accepted"
    assert root["parent_usage_id"] == "CS5HF"


def test_ambiguous_fungi_root_is_rejected() -> None:
    raw = json.loads(fungi_bytes())
    raw["result"].append(raw["result"][0])
    body = json.dumps(raw).encode()
    with pytest.raises(AcquisitionError, match="ambiguous"):
        verify_fungi_root(fungi_response(body), dataset_key=315834)


def test_wrong_fungi_rank_is_rejected() -> None:
    raw = json.loads(fungi_bytes())
    raw["result"][0]["usage"]["name"]["rank"] = "phylum"
    body = json.dumps(raw).encode()
    with pytest.raises(AcquisitionError, match="kingdom"):
        verify_fungi_root(fungi_response(body), dataset_key=315834)


def test_unapproved_proposal_is_rejected_by_acquisition() -> None:
    with pytest.raises(AcquisitionError, match="not approved"):
        load_request(PROPOSAL)


def test_proposal_does_not_invoke_export() -> None:
    proposal = json.loads(PROPOSAL.read_text())
    assert proposal["download_authorized"] is False
    assert proposal["selection_evidence"]["export_job_invoked"] is False
    assert proposal["selection_evidence"]["archive_body_retrieved"] is False
    assert proposal["delivery"]["canonical_entry_endpoint"] == (
        "https://api.checklistbank.org/dataset/315834/export.zip"
        "?extended=true&format=ColDP"
    )


def test_official_fixtures_and_provenance_contain_no_secret_fields() -> None:
    forbidden = {"authorization", "password", "secret", "signed_url", "token", "access_token"}
    for path in FIXTURES.glob("official-*"):
        raw = json.loads(path.read_text())
        keys = set()

        def walk(value):
            if isinstance(value, dict):
                for key, child in value.items():
                    keys.add(str(key).casefold())
                    walk(child)
            elif isinstance(value, list):
                for child in value:
                    walk(child)

        walk(raw)
        assert not (keys & forbidden), path
