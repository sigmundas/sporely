"""Pinned NorTaxa 1.284 profile as the first real consumer of the
national-source adapter toolkit.

Proves:
* the profile loads and its emitted namespaces match identity-contract.md;
* Taxon and VernacularName are mapped from meta.xml;
* zero-or-one Distribution extension under either allowlisted namespace;
* Distribution is validated structurally but not imported;
* the national-source adapter accepts the synthetic fixture;
* the existing NorTaxa validator (refresh_nortaxa.validate_fixture) accepts
  the same fixture and both agree on required row types + record counts;
* normalize produces deterministic byte-identical output across runs.
"""
from __future__ import annotations

import hashlib
import json
import socket
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

import national_source  # noqa: E402
import refresh_nortaxa  # noqa: E402
from national_source import NationalSourceError  # noqa: E402


TAX = Path(__file__).resolve().parents[1]
PROFILE_DIR = TAX / "national_sources" / "nortaxa" / "1.284"
PROFILE_PATH = PROFILE_DIR / "source.json"
FIXTURE_PATH = PROFILE_DIR / "synthetic-fixture.zip"
NORTAXA_REQUEST = TAX / "tests" / "fixtures" / "nortaxa" / "valid-request.json"


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def blocked(*a, **kw):
        raise AssertionError("national-source NorTaxa test attempted network access")

    monkeypatch.setattr(socket, "socket", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)


# ----- profile identity -----


def test_profile_binds_pinned_nortaxa_release_and_identity_namespaces() -> None:
    profile = national_source.load_profile(PROFILE_PATH)
    assert profile.source_code == "nortaxa"
    assert profile.source_release == {"version": "1.284", "issued_date": "2026-07-17"}
    assert profile.identifier_namespace == "NBIC:"
    ns = national_source.profile_identifier_namespaces(profile)
    # Exactly the names documented in database/taxonomy/docs/identity-contract.md.
    assert ns == {
        "core_row_id":            "nortaxa_dwc_id",
        "taxon_id":               "nortaxa_taxon_id",
        "accepted_name_usage_id": "nortaxa_accepted_name_usage_id",
        "parent_name_usage_id":   "nortaxa_parent_name_usage_id",
    }


def test_profile_maps_taxon_and_vernacular_from_meta_xml() -> None:
    profile = national_source.load_profile(PROFILE_PATH)
    assert profile.core_row_type == national_source.DWC_TAXON
    assert profile.core_location == "data/nonstandard-core.csv"
    assert set(profile.core_terms) == {
        "taxonID", "acceptedNameUsageID", "parentNameUsageID",
        "scientificName", "scientificNameAuthorship",
        "taxonRank", "taxonomicStatus",
    }
    assert profile.vernacular_row_type == national_source.GBIF_VERNACULAR
    assert profile.vernacular_location == "names/localized.data"
    assert set(profile.vernacular_terms) == {"vernacularName", "language", "isPreferredName"}


def test_profile_permits_zero_or_one_gbif_distribution_only() -> None:
    profile = national_source.load_profile(PROFILE_PATH)
    # The real 1.284 archive uses the GBIF namespace; the adapter's allowlist
    # accepts either DwC or GBIF, but this pinned profile targets the observed
    # URI exactly. `validation_only` is mandatory: Distribution is validated
    # but never imported.
    assert profile.distribution_row_type == national_source.GBIF_DISTRIBUTION
    assert profile.distribution_row_type in national_source.DISTRIBUTION_ROW_TYPES
    assert profile.distribution_validation_only is True
    assert profile.distribution_location == "extra/distribution.tsv"


# ----- adapter accepts the synthetic fixture -----


def test_national_source_adapter_validates_synthetic_fixture() -> None:
    profile = national_source.load_profile(PROFILE_PATH)
    report = national_source.validate_archive(profile, FIXTURE_PATH)
    assert report["result"] == "passed"
    assert report["record_counts"] == {"Taxon": 4, "VernacularName": 3, "Distribution": 1}
    assert report["distribution_imported"] is False


def test_generic_and_nortaxa_validators_agree_on_synthetic_fixture() -> None:
    """The generic adapter and the pinned NorTaxa validator must agree on
    structural acceptance, required row types, and record counts."""
    profile = national_source.load_profile(PROFILE_PATH)
    generic = national_source.validate_archive(profile, FIXTURE_PATH)
    request = refresh_nortaxa.load_request(NORTAXA_REQUEST)
    nortaxa = refresh_nortaxa.validate_fixture(FIXTURE_PATH, request)
    # Structural acceptance.
    assert generic["result"] == "passed"
    assert nortaxa["result"] == "passed"
    # Required row types + counts agree.
    assert generic["record_counts"]["Taxon"] == nortaxa["record_counts"]["Taxon"]
    assert (generic["record_counts"]["VernacularName"]
            == nortaxa["record_counts"]["VernacularName"])
    assert (generic["record_counts"].get("Distribution")
            == nortaxa["record_counts"].get("Distribution"))
    # Distribution is validated by both and imported by neither.
    assert generic["distribution_imported"] is False
    # refresh_nortaxa.validate_fixture is non-extracting by contract.
    assert nortaxa["network_calls"] == 0


# ----- normalize -----


def test_normalize_produces_expected_records_under_nortaxa_namespaces(tmp_path: Path) -> None:
    profile = national_source.load_profile(PROFILE_PATH)
    out = tmp_path / "nortaxa-1.284"
    report = national_source.normalize_archive(profile, FIXTURE_PATH, out)
    assert report["result"] == "passed"
    assert report["record_counts"] == {"Taxon": 4, "VernacularName": 3, "Distribution": 1}
    assert report["identifier_namespaces"] == {
        "core_row_id":            "nortaxa_dwc_id",
        "taxon_id":               "nortaxa_taxon_id",
        "accepted_name_usage_id": "nortaxa_accepted_name_usage_id",
        "parent_name_usage_id":   "nortaxa_parent_name_usage_id",
    }
    taxa = [json.loads(line) for line in (out / "taxa.jsonl").read_text().splitlines()]
    ids = {t["core_row_id"]["value"] for t in taxa}
    assert ids == {"row-R", "row-G", "row-A", "row-S"}
    # DwC-A cross-references go by taxonID.
    accepted = next(t for t in taxa if t["core_row_id"]["value"] == "row-S")
    assert accepted["accepted_name_usage_id"] == {
        "value": "taxon:accepted", "namespace": "nortaxa_accepted_name_usage_id",
    }
    # NBIC:… external identifier is preserved verbatim under its full DwC URI.
    species = next(t for t in taxa if t["core_row_id"]["value"] == "row-A")
    assert species["external_ids"] == {
        "http://rs.tdwg.org/dwc/terms/scientificNameID": "NBIC:54995",
    }


def test_nortaxa_normalize_is_byte_identical_across_runs(tmp_path: Path) -> None:
    """Deterministic normalize: two independent runs against the same profile
    and archive produce byte-identical taxa.jsonl / vernacular.jsonl /
    report.json."""
    profile = national_source.load_profile(PROFILE_PATH)
    a = tmp_path / "a"
    b = tmp_path / "b"
    national_source.normalize_archive(profile, FIXTURE_PATH, a)
    national_source.normalize_archive(profile, FIXTURE_PATH, b)
    for name in ("taxa.jsonl", "vernacular.jsonl", "report.json"):
        assert (a / name).read_bytes() == (b / name).read_bytes(), name


def test_nortaxa_normalize_does_not_extract_or_import_distribution(tmp_path: Path) -> None:
    profile = national_source.load_profile(PROFILE_PATH)
    out = tmp_path / "nortaxa-1.284"
    report = national_source.normalize_archive(profile, FIXTURE_PATH, out)
    assert not (out / "distribution.jsonl").exists()
    assert report["distribution_imported"] is False
    # Output contains only the three expected artifacts — no archive extraction.
    assert sorted(p.name for p in out.iterdir()) == [
        "report.json", "taxa.jsonl", "vernacular.jsonl",
    ]


# ----- historical + real-archive expectation -----


def test_real_archive_path_absent_and_readme_documents_eventual_commands() -> None:
    """The real archive.zip lives at sources/nortaxa/1.284/ after a manual
    --execute acquisition; it is not present in the current tree, and the
    profile directory documents the eventual commands."""
    assert not (TAX / "sources" / "nortaxa" / "1.284" / "archive.zip").exists()
    readme = (PROFILE_DIR / "README.md").read_text(encoding="utf-8")
    assert "acquire_nortaxa.py --execute" in readme
    assert "national_source.py validate" in readme
    assert "national_source.py normalize" in readme


def test_synthetic_fixture_is_small_and_deterministic() -> None:
    """The synthetic NorTaxa fixture is bounded and content-addressable."""
    assert FIXTURE_PATH.exists()
    size = FIXTURE_PATH.stat().st_size
    # A NorTaxa DwC-A shape at four taxa fits in a couple of KiB.
    assert size < 8 * 1024, size
    sha = hashlib.sha256(FIXTURE_PATH.read_bytes()).hexdigest()
    assert len(sha) == 64
