#!/usr/bin/env python3
"""
Reconcile Artportalen "Swedish-only" taxa against the Norway source taxonomy.

Input:
- a CSV produced by ``fetch_artportalen_taxon_ids_by_genus.py`` containing
  taxa that were present in Artportalen but not in the local Norway SQLite DB

Checks performed:
- exact normalized scientific-name lookup in ``database/taxon.txt``
- resolve synonym rows via ``acceptedNameUsageID``
- report whether the accepted Norwegian taxon exists in the local SQLite DB

This is useful for cases like:
- Swedish name exists in Artportalen
- Norway source contains it as an accepted species
- or Norway source contains it only as a synonym to another accepted species

Example:
    python3 database/reconcile_artportalen_swedish_only.py
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path
from typing import Any

from fetch_artportalen_taxon_ids import _normalize_text


DEFAULT_SWEDISH_ONLY_CSV = Path("database/artportalen_taxon_ids_swedish_only.csv")
DEFAULT_TAXON_TXT = Path("database/taxon.txt")
DEFAULT_LOCAL_DB = Path("database/vernacular_multilanguage.sqlite3")
DEFAULT_OUTPUT_CSV = Path("database/artportalen_taxon_ids_swedish_only_reconciled.csv")


def _set_csv_field_limit() -> None:
    try:
        csv.field_size_limit(1024 * 1024 * 10)
    except OverflowError:
        csv.field_size_limit(2_147_483_647)


def _stripped(value: Any) -> str:
    return str(value or "").strip()


def _int_or_none(value: Any) -> int | None:
    raw = _stripped(value)
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def load_taxon_txt(path: Path) -> tuple[dict[int, dict[str, str]], dict[str, list[dict[str, str]]]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing taxon source: {path}")

    by_id: dict[int, dict[str, str]] = {}
    by_normalized_name: dict[str, list[dict[str, str]]] = {}

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            row_id = _int_or_none(row.get("id") or row.get("taxonID"))
            if row_id is None:
                continue
            by_id[row_id] = row
            normalized_name = _normalize_text(row.get("scientificName") or "")
            if normalized_name:
                by_normalized_name.setdefault(normalized_name, []).append(row)

    return by_id, by_normalized_name


def load_local_taxon_ids(path: Path) -> set[int]:
    if not path.exists():
        raise FileNotFoundError(f"Missing local DB: {path}")
    conn = sqlite3.connect(path)
    try:
        rows = conn.execute("SELECT taxon_id FROM taxon_min").fetchall()
    finally:
        conn.close()
    return {int(row[0]) for row in rows if row and row[0] is not None}


def classify_match(
    rows: list[dict[str, str]],
    by_id: dict[int, dict[str, str]],
    local_taxon_ids: set[int],
) -> dict[str, Any]:
    if not rows:
        return {
            "norway_match_status": "missing_from_taxon_txt",
            "norway_match_count": 0,
            "norway_taxon_id": "",
            "norway_scientific_name": "",
            "norway_taxon_rank": "",
            "norway_taxonomic_status": "",
            "norway_accepted_taxon_id": "",
            "norway_accepted_scientific_name": "",
            "norway_accepted_rank": "",
            "accepted_exists_in_local_db": "",
            "note": "Scientific name not found in taxon.txt",
        }

    exact_species_valid: list[dict[str, str]] = []
    exact_valid_other: list[dict[str, str]] = []
    exact_synonyms: list[dict[str, str]] = []
    for row in rows:
        rank = _stripped(row.get("taxonRank")).lower()
        status = _stripped(row.get("taxonomicStatus")).lower()
        if status == "valid" and rank == "species":
            exact_species_valid.append(row)
        elif status == "valid":
            exact_valid_other.append(row)
        elif status == "synonym":
            exact_synonyms.append(row)

    def _accepted_payload(row: dict[str, str]) -> tuple[int | None, dict[str, str] | None]:
        accepted_id = _int_or_none(row.get("acceptedNameUsageID"))
        accepted_row = by_id.get(accepted_id) if accepted_id is not None else None
        return accepted_id, accepted_row

    def _result(status: str, row: dict[str, str], note: str = "") -> dict[str, Any]:
        accepted_id, accepted_row = _accepted_payload(row)
        accepted_exists = (
            "true" if accepted_id is not None and accepted_id in local_taxon_ids else "false"
            if accepted_id is not None
            else ""
        )
        return {
            "norway_match_status": status,
            "norway_match_count": len(rows),
            "norway_taxon_id": _stripped(row.get("id") or row.get("taxonID")),
            "norway_scientific_name": _stripped(row.get("scientificName")),
            "norway_taxon_rank": _stripped(row.get("taxonRank")),
            "norway_taxonomic_status": _stripped(row.get("taxonomicStatus")),
            "norway_accepted_taxon_id": accepted_id or "",
            "norway_accepted_scientific_name": _stripped(accepted_row.get("scientificName")) if accepted_row else "",
            "norway_accepted_rank": _stripped(accepted_row.get("taxonRank")) if accepted_row else "",
            "accepted_exists_in_local_db": accepted_exists,
            "note": note,
        }

    if len(exact_species_valid) == 1:
        row = exact_species_valid[0]
        return _result(
            "accepted_species_in_taxon_txt",
            row,
            "Exact scientific name is an accepted species in taxon.txt",
        )
    if len(exact_species_valid) > 1:
        row = exact_species_valid[0]
        return _result(
            "ambiguous_multiple_accepted_species",
            row,
            "Multiple accepted species rows with the same normalized scientific name",
        )
    if len(exact_valid_other) == 1:
        row = exact_valid_other[0]
        return _result(
            "accepted_non_species_in_taxon_txt",
            row,
            "Exact scientific name is valid in taxon.txt, but not rank=species",
        )
    if len(exact_valid_other) > 1:
        row = exact_valid_other[0]
        return _result(
            "ambiguous_multiple_valid_non_species",
            row,
            "Multiple valid non-species rows with the same normalized scientific name",
        )
    if len(exact_synonyms) == 1:
        row = exact_synonyms[0]
        return _result(
            "synonym_in_taxon_txt",
            row,
            "Exact scientific name is a synonym in taxon.txt; use the accepted Norwegian taxon id",
        )
    if len(exact_synonyms) > 1:
        row = exact_synonyms[0]
        return _result(
            "ambiguous_multiple_synonyms",
            row,
            "Multiple synonym rows with the same normalized scientific name",
        )

    row = rows[0]
    return _result(
        "present_in_taxon_txt_unclassified",
        row,
        "Scientific name exists in taxon.txt, but did not fit the expected accepted/synonym buckets",
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reconcile Artportalen Swedish-only taxa against taxon.txt and the local taxonomy DB."
    )
    parser.add_argument("--input-csv", type=Path, default=DEFAULT_SWEDISH_ONLY_CSV)
    parser.add_argument("--taxon-txt", type=Path, default=DEFAULT_TAXON_TXT)
    parser.add_argument("--local-db", type=Path, default=DEFAULT_LOCAL_DB)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--overwrite", action="store_true", help="Rewrite the output CSV from scratch.")
    return parser


def main() -> int:
    _set_csv_field_limit()
    args = build_arg_parser().parse_args()

    by_id, by_normalized_name = load_taxon_txt(args.taxon_txt)
    local_taxon_ids = load_local_taxon_ids(args.local_db)

    if not args.input_csv.exists():
        raise FileNotFoundError(f"Missing input CSV: {args.input_csv}")

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if args.overwrite else "w"
    with (
        args.input_csv.open("r", encoding="utf-8", newline="") as in_handle,
        args.output_csv.open(mode, encoding="utf-8", newline="") as out_handle,
    ):
        reader = csv.DictReader(in_handle)
        fieldnames = list(reader.fieldnames or []) + [
            "norway_match_status",
            "norway_match_count",
            "norway_taxon_id",
            "norway_scientific_name",
            "norway_taxon_rank",
            "norway_taxonomic_status",
            "norway_accepted_taxon_id",
            "norway_accepted_scientific_name",
            "norway_accepted_rank",
            "accepted_exists_in_local_db",
        ]
        writer = csv.DictWriter(out_handle, fieldnames=fieldnames)
        writer.writeheader()

        total = 0
        missing = 0
        synonym = 0
        accepted = 0
        for row in reader:
            total += 1
            normalized_name = _normalize_text(row.get("scientific_name") or "")
            matches = by_normalized_name.get(normalized_name, [])
            resolved = classify_match(matches, by_id, local_taxon_ids)
            status = str(resolved.get("norway_match_status") or "")
            if status == "missing_from_taxon_txt":
                missing += 1
            elif status == "synonym_in_taxon_txt":
                synonym += 1
            elif status == "accepted_species_in_taxon_txt":
                accepted += 1
            merged = dict(row)
            merged.update(resolved)
            writer.writerow(merged)

    print(
        f"Wrote {args.output_csv} "
        f"(rows={total}, accepted_species={accepted}, synonyms={synonym}, missing={missing})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
