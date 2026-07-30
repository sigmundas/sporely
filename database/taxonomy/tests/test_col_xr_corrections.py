import hashlib
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from col_xr_corrections import BOM, apply_exact_source_correction
from refresh_col_xr import AcquisitionError


HEADER = (
    "ID", "parentID", "status", "scientificName", "authorship", "rank",
    "namePublishedInPage",
)
RAW_FIELD = b"58, f" + BOM + BOM + "igs 6–7".encode()
RAW_ROW = b"\t".join([
    b"5BK77", b"7PKV4", b"accepted", b"Virpazaria stojaspali",
    "A. Reischütz".encode(), b"species", RAW_FIELD,
]) + b"\n"


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def fingerprint() -> dict:
    body = RAW_ROW[:-1]
    first = body.find(BOM)
    cleaned = RAW_FIELD.replace(BOM + BOM, b"")
    return {
        "approval_status": "approved",
        "archive_sha256": "a" * 64,
        "member_sha256": "m" * 64,
        "source_profile": "checklistbank-col-xr-2026-07-17",
        "release_label": "2026-07-17 XR",
        "dataset_key": 315834,
        "member_name": "NameUsage.tsv",
        "line_number": 1853650,
        "raw_row_bytes": len(RAW_ROW),
        "raw_row_sha256": digest(RAW_ROW),
        "row_taxon_id": "5BK77",
        "row_scientific_name": "Virpazaria stojaspali",
        "field_index_zero_based": 6,
        "canonical_field": "namePublishedInPage",
        "raw_field_sha256": digest(RAW_FIELD),
        "expected_bom_count": 2,
        "expected_bom_byte_offsets_in_record_body": [first, first + 3],
        "expected_columns": 7,
        "normalized_field_value": "58, figs 6–7",
        "normalized_field_sha256": digest(cleaned),
    }


def apply(raw=RAW_ROW, *, changes=None, runtime=None):
    trusted = fingerprint()
    policy = {**trusted, "correction_id": "fixture-double-bom-v1"}
    if changes:
        policy.update(changes)
    values = {
        "line_number": 1853650,
        "normalized_header": HEADER,
        "archive_sha256": "a" * 64,
        "member_sha256": "m" * 64,
        "source_profile": "checklistbank-col-xr-2026-07-17",
        "release_label": "2026-07-17 XR",
        "dataset_key": 315834,
    }
    values.update(runtime or {})
    return apply_exact_source_correction(
        raw,
        **values,
        policy=policy,
        max_line_bytes=4096,
        max_field_bytes=1024,
        trusted_fingerprint=trusted,
    )


def test_exact_fingerprint_applies_once_and_preserves_dual_evidence() -> None:
    result = apply()
    assert result.record.raw_fields[6] == "58, f\ufeff\ufeffigs 6–7"
    assert result.record.semantic_fields[6] == "58, figs 6–7"
    assert result.evidence["removed_code_point_count"] == 2
    assert result.evidence["raw_field_sha256"] == digest(RAW_FIELD)
    assert result.evidence["semantic_field_sha256"] == digest(b"58, figs 6\xe2\x80\x937")
    assert result.record.raw_fields[:6] == result.record.semantic_fields[:6]


@pytest.mark.parametrize(
    "raw",
    [
        RAW_ROW.replace(BOM + BOM, BOM),
        RAW_ROW.replace(BOM + BOM, BOM + BOM + BOM),
        RAW_ROW.replace(BOM + BOM, b""),
        RAW_ROW.replace(b"5BK77", b"DIFFR"),
    ],
)
def test_one_remaining_multiple_zero_or_different_row_fails_closed(raw) -> None:
    with pytest.raises(AcquisitionError):
        apply(raw)


@pytest.mark.parametrize(
    ("changes", "runtime"),
    [
        ({"line_number": 1}, None),
        ({"row_taxon_id": "OTHER"}, None),
        ({"field_index_zero_based": 5}, None),
        ({"expected_bom_byte_offsets_in_record_body": [1, 4]}, None),
        ({"expected_bom_count": 1}, None),
        ({"raw_field_sha256": "0" * 64}, None),
        (None, {"archive_sha256": "x" * 64}),
        (None, {"member_sha256": "x" * 64}),
        (None, {"release_label": "future release"}),
    ],
)
def test_any_policy_or_runtime_fingerprint_difference_fails(changes, runtime) -> None:
    with pytest.raises(AcquisitionError):
        apply(changes=changes, runtime=runtime)


def test_correction_cannot_target_identity_field() -> None:
    trusted = fingerprint()
    trusted["field_index_zero_based"] = 0
    trusted["canonical_field"] = "ID"
    trusted["raw_field_sha256"] = digest(b"5BK77")
    policy = {**trusted, "correction_id": "bad"}
    with pytest.raises(AcquisitionError):
        apply_exact_source_correction(
            RAW_ROW,
            line_number=1853650,
            normalized_header=HEADER,
            archive_sha256="a" * 64,
            member_sha256="m" * 64,
            source_profile="checklistbank-col-xr-2026-07-17",
            release_label="2026-07-17 XR",
            dataset_key=315834,
            policy=policy,
            max_line_bytes=4096,
            max_field_bytes=1024,
            trusted_fingerprint=trusted,
        )


def test_no_generic_bom_stripping_api_exists() -> None:
    import col_xr_corrections

    assert not hasattr(col_xr_corrections, "strip_bom")
