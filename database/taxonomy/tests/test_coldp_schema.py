import sys
import io
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from coldp_schema import (
    CANONICAL_PROFILE,
    CHECKLISTBANK_COL_XR_PROFILE,
    NAME_USAGE_FIELDS,
    resolve_entity_header,
)
from refresh_col_xr import AcquisitionError
from acquire_col_xr import _scan_nameusage


REQUIRED = ("ID", "parentID", "status", "scientificName", "authorship", "rank")


def resolve(tokens, profile=CHECKLISTBANK_COL_XR_PROFILE):
    return resolve_entity_header("NameUsage", tokens, source_profile=profile)


def test_canonical_and_exact_col_headers_resolve_with_provenance() -> None:
    canonical = resolve(REQUIRED, CANONICAL_PROFILE)
    assert canonical.normalized_tokens == REQUIRED
    prefixed = resolve(tuple(f"col:{value}" for value in REQUIRED))
    assert prefixed.normalized_tokens == REQUIRED
    assert prefixed.original_to_normalized[0] == ("col:ID", "ID")


def test_complete_observed_nameusage_header_resolves() -> None:
    observed = [f"col:{value}" for value in NAME_USAGE_FIELDS] + ["clb:merged"]
    result = resolve(observed)
    assert result.normalized_tokens[:-1] == NAME_USAGE_FIELDS
    assert result.unknown_columns == ("clb:merged",)


def test_mixed_known_forms_are_allowed_without_collision() -> None:
    result = resolve(("col:ID", "parentID", "col:status", "scientificName", "authorship", "rank"))
    assert result.normalized_tokens == REQUIRED


def test_unknown_unprefixed_extra_remains_opaque() -> None:
    result = resolve((*REQUIRED, "vendorExtra"))
    assert result.normalized_tokens[-1] == "vendorExtra"
    assert result.unknown_columns == ("vendorExtra",)


@pytest.mark.parametrize(
    "bad",
    [
        "foo:ID", "dwc:ID", "COL:ID", "Col:ID", "col::ID", "col:col:ID",
        " col:ID", "col:ID ", "col：ID", "col%3AID", "col%3aID",
        "col:\u200bID", "col:\x00ID", "col:", "col:unknownRequired",
    ],
)
def test_deceptive_prefix_forms_are_rejected(bad) -> None:
    tokens = (bad, "parentID", "status", "scientificName", "authorship", "rank")
    with pytest.raises(AcquisitionError):
        resolve(tokens)


@pytest.mark.parametrize(
    "tokens",
    [
        ("ID", "col:ID", "parentID", "status", "scientificName", "authorship", "rank"),
        ("col:ID", "col:ID", "parentID", "status", "scientificName", "authorship", "rank"),
        ("ID", "id", "parentID", "status", "scientificName", "authorship", "rank"),
    ],
)
def test_normalization_duplicates_and_casefold_collisions_are_rejected(tokens) -> None:
    with pytest.raises(AcquisitionError, match="collision|duplicate"):
        resolve(tokens)


def test_prefixed_headers_require_pinned_source_profile() -> None:
    with pytest.raises(AcquisitionError, match="pinned ChecklistBank"):
        resolve(tuple(f"col:{value}" for value in REQUIRED), CANONICAL_PROFILE)


def test_fuzzy_required_field_names_do_not_satisfy_contract() -> None:
    with pytest.raises(AcquisitionError, match="required"):
        resolve(("id", "parentID", "status", "scientificName", "authorship", "rank"))


def test_header_resolution_never_changes_data_values() -> None:
    value = "col:F"
    resolve(tuple(f"col:{item}" for item in REQUIRED))
    assert value == "col:F"


def test_streaming_scan_preserves_colon_qualified_identifier_values() -> None:
    header = "\t".join(f"col:{item}" for item in REQUIRED)
    rows = [
        ("F", "CS5HF", "accepted", "Fungi", "", "kingdom"),
        ("C1", "GENUS", "accepted", "Candolleomyces candolleanus", "Smith", "species"),
        ("P1", "C1", "synonym", "Psathyrella candolleana", "Jones", "species"),
        ("col:F", "F", "accepted", "Fixture", "", "species"),
    ]
    payload = (header + "\n" + "\n".join("\t".join(row) for row in rows) + "\n").encode()
    report = _scan_nameusage(io.BytesIO(payload), "NameUsage.tsv")
    assert report["row_count"] == 4
    assert report["identifier_shape_counts"]["colon_qualified"] == 1
    assert report["uncompressed_bytes_read"] == len(payload)
