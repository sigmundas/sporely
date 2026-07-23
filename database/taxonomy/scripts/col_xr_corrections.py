#!/usr/bin/env python3
"""Fingerprint-bound semantic corrections for immutable COL XR source bytes."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from coldp_schema import CHECKLISTBANK_COL_XR_PROFILE
from coldp_table import LiteralTsvRecord, parse_literal_tsv_record
from refresh_col_xr import AcquisitionError


ARCHIVE_SHA256 = "397d701c8eb269bf78d6ac7b03149915b0d9e2a2c18694be2c91445b807814f9"
MEMBER_SHA256 = "5b7d7ec383ad69b7dc9c959dadd866a2769ea2433cbcbe1ae30f4b7d9359bdd0"
IDENTITY_FIELDS = {"ID", "scientificName", "status", "rank", "parentID"}
BOM = b"\xef\xbb\xbf"
PINNED_CORRECTION = {
    "approval_status": "approved",
    "archive_sha256": ARCHIVE_SHA256,
    "member_sha256": MEMBER_SHA256,
    "source_profile": CHECKLISTBANK_COL_XR_PROFILE,
    "release_label": "2026-07-17 XR",
    "dataset_key": 315834,
    "member_name": "NameUsage.tsv",
    "line_number": 1853650,
    "raw_row_bytes": 511,
    "raw_row_sha256": "156d19f3c53506a4f145799ff6ab1a5664c92b62e19e59bf20bd9573a907f9b5",
    "row_taxon_id": "5BK77",
    "row_scientific_name": "Virpazaria stojaspali",
    "field_index_zero_based": 31,
    "canonical_field": "namePublishedInPage",
    "raw_field_sha256": "6ba3b50d10472e4b0f3c66573664b8176726ddefea0d836873659d93396c79c1",
    "expected_bom_count": 2,
    "expected_bom_byte_offsets_in_record_body": [224, 227],
    "expected_columns": 73,
    "normalized_field_value": "58, figs 6–7",
    "normalized_field_sha256": "b3fcc6029626887bb6c28a5fb074f1f42416a9541b0d67c2ddca1b82c776bcee",
}


@dataclass(frozen=True)
class AppliedCorrection:
    record: LiteralTsvRecord
    evidence: dict[str, Any]


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def apply_exact_source_correction(
    raw_record: bytes,
    *,
    line_number: int,
    normalized_header: tuple[str, ...],
    archive_sha256: str,
    member_sha256: str,
    source_profile: str,
    release_label: str,
    dataset_key: int,
    policy: dict[str, Any],
    max_line_bytes: int,
    max_field_bytes: int,
    trusted_fingerprint: dict[str, Any] = PINNED_CORRECTION,
) -> AppliedCorrection:
    expected = trusted_fingerprint
    for key, value in expected.items():
        if policy.get(key) != value:
            raise AcquisitionError(f"source correction policy fingerprint mismatch: {key}")
    runtime = {
        "archive_sha256": archive_sha256,
        "member_sha256": member_sha256,
        "source_profile": source_profile,
        "release_label": release_label,
        "dataset_key": dataset_key,
        "line_number": line_number,
    }
    for key, value in runtime.items():
        if value != expected[key]:
            raise AcquisitionError(f"source correction runtime fingerprint mismatch: {key}")
    if len(raw_record) != expected["raw_row_bytes"] or _sha256(raw_record) != expected["raw_row_sha256"]:
        raise AcquisitionError("source correction raw-row fingerprint mismatch")
    terminator_bytes = b""
    body = raw_record
    if body.endswith(b"\n"):
        terminator_bytes = b"\n"
        body = body[:-1]
        if body.endswith(b"\r"):
            terminator_bytes = b"\r\n"
            body = body[:-1]
    fields = body.split(b"\t")
    if len(fields) != expected["expected_columns"]:
        raise AcquisitionError("source correction column-count fingerprint mismatch")
    index = expected["field_index_zero_based"]
    if normalized_header[index] != expected["canonical_field"]:
        raise AcquisitionError("source correction canonical-field fingerprint mismatch")
    raw_field = fields[index]
    if _sha256(raw_field) != expected["raw_field_sha256"]:
        raise AcquisitionError("source correction raw-field fingerprint mismatch")
    offsets: list[int] = []
    start = 0
    while True:
        found = body.find(BOM, start)
        if found < 0:
            break
        offsets.append(found)
        start = found + len(BOM)
    if offsets != expected["expected_bom_byte_offsets_in_record_body"]:
        raise AcquisitionError("source correction BOM-offset fingerprint mismatch")
    if raw_field.count(BOM) != expected["expected_bom_count"] or BOM + BOM not in raw_field:
        raise AcquisitionError("source correction requires exactly two consecutive BOMs")
    cleaned_field = raw_field.replace(BOM + BOM, b"", 1)
    if BOM in cleaned_field:
        raise AcquisitionError("source correction would leave an unapproved BOM")
    if _sha256(cleaned_field) != expected["normalized_field_sha256"]:
        raise AcquisitionError("source correction normalized-field fingerprint mismatch")
    cleaned_fields = list(fields)
    cleaned_fields[index] = cleaned_field
    cleaned_record = b"\t".join(cleaned_fields) + terminator_bytes
    parsed = parse_literal_tsv_record(
        cleaned_record,
        line_number=line_number,
        max_line_bytes=max_line_bytes,
        max_field_bytes=max_field_bytes,
    )
    if len(parsed.raw_fields) != expected["expected_columns"]:
        raise AcquisitionError("source correction changed the column count")
    indexes = {name: position for position, name in enumerate(normalized_header)}
    raw_decoded = tuple(field.decode("utf-8") for field in fields)
    for name in IDENTITY_FIELDS:
        position = indexes[name]
        if raw_decoded[position] != parsed.raw_fields[position]:
            raise AcquisitionError(f"source correction changed identity field: {name}")
    if parsed.raw_fields[index] != expected["normalized_field_value"]:
        raise AcquisitionError("source correction normalized value mismatch")
    if (
        parsed.raw_fields[index] != "58, figs 6–7"
        or raw_decoded[index] != "58, f\ufeff\ufeffigs 6–7"
        or parsed.raw_fields[index].replace("figs", "") == raw_decoded[index]
    ):
        raise AcquisitionError("source correction bounded context mismatch")
    if parsed.raw_fields[indexes["ID"]] != expected["row_taxon_id"]:
        raise AcquisitionError("source correction row ID mismatch")
    if parsed.raw_fields[indexes["scientificName"]] != expected["row_scientific_name"]:
        raise AcquisitionError("source correction scientific-name mismatch")
    return AppliedCorrection(
        record=LiteralTsvRecord(
            raw_fields=raw_decoded,
            semantic_fields=parsed.semantic_fields,
            terminator=parsed.terminator,
        ),
        evidence={
            "correction_id": policy["correction_id"],
            "line": line_number,
            "field_index_zero_based": index,
            "canonical_field": expected["canonical_field"],
            "raw_row_sha256": expected["raw_row_sha256"],
            "raw_field_value": raw_decoded[index],
            "raw_field_sha256": expected["raw_field_sha256"],
            "semantic_field_value": parsed.semantic_fields[index],
            "semantic_field_sha256": expected["normalized_field_sha256"],
            "removed_code_point_count": 2,
            "raw_bytes_preserved": True,
        },
    )
