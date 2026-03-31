#!/usr/bin/env python3
"""
Build a multilingual iNaturalist common-name CSV for Sporely.

The output CSV is used as:
- the vernacular-name input for the taxonomy builders
- an iNaturalist taxon-id mapping source when it includes
  ``inaturalist_taxon_id``

The script can resume from an existing CSV unless ``--overwrite`` is used.
"""

from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path

import requests
from requests.exceptions import RequestException


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_TAXON_FILE = SCRIPT_DIR / "taxon.txt"
DEFAULT_OUT_CSV = SCRIPT_DIR / "vernacular_inat_11lang.csv"

# Target filters
ASCO_ORDERS = {"pezizales", "morchellales", "helvellales", "tuberales"}
BASIDIO_CLASS_ALLOW = {"agaricomycetes"}

# Output columns (order matters)
OUTPUT_LANGS = ["en", "de", "fr", "es", "da", "sv", "no", "fi", "pl", "pt", "it"]

# Map iNaturalist lexicon/parameterized_lexicon to output columns
LEXICON_TO_CODE = {
    "english": "en",
    "german": "de",
    "french": "fr",
    "spanish": "es",
    "danish": "da",
    "swedish": "sv",
    "norwegian": "no",
    "finnish": "fi",
    "polish": "pl",
    "portuguese": "pt",
    "brazilian-portuguese": "pt",
    "portuguese-brazil": "pt",
    "portuguese-brazilian": "pt",
    "italian": "it",
}

API_BASE = "https://api.inaturalist.org/v1"
WEB_BASE = "https://www.inaturalist.org"
HEADERS = {
    "User-Agent": "Sporely/1.0 (taxonomy build script; contact: sigmund.aas@gmail.com)",
    "Accept-Encoding": "gzip",
}

CSV_FIELD_LIMIT = 1024 * 1024 * 10  # 10 MB


def _set_csv_field_limit() -> None:
    try:
        csv.field_size_limit(CSV_FIELD_LIMIT)
    except OverflowError:
        csv.field_size_limit(2_147_483_647)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a multilingual iNaturalist vernacular CSV with iNaturalist taxon IDs."
    )
    parser.add_argument("--taxon-file", type=Path, default=DEFAULT_TAXON_FILE)
    parser.add_argument("--out-csv", type=Path, default=DEFAULT_OUT_CSV)
    parser.add_argument("--request-delay", type=float, default=0.05)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--backoff-base", type=float, default=5.0)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--overwrite", action="store_true", help="Rewrite the output CSV from scratch.")
    parser.add_argument("--limit", type=int, help="Only process the first N taxa after filtering.")
    return parser


def normalize_lexicon(entry: dict) -> str:
    return (entry.get("parameterized_lexicon") or entry.get("lexicon") or "").strip().lower()


def iter_taxa(taxon_file: Path):
    with taxon_file.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            phylum = (row.get("phylum") or "").strip().lower()
            order = (row.get("order") or "").strip().lower()
            tax_class = (row.get("class") or "").strip().lower()
            if phylum == "ascomycota":
                if order not in ASCO_ORDERS:
                    continue
            elif phylum == "basidiomycota":
                if tax_class not in BASIDIO_CLASS_ALLOW:
                    continue
            else:
                continue
            rank = (row.get("taxonRank") or "").strip().lower()
            if rank != "species":
                continue
            status = (row.get("taxonomicStatus") or "").strip().lower()
            if status != "valid":
                continue
            name = (row.get("scientificName") or "").strip()
            if name:
                yield name


def fetch_taxon_id(
    session: requests.Session,
    scientific_name: str,
    *,
    max_retries: int,
    backoff_base: float,
    timeout_seconds: float,
) -> int | None:
    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(
                f"{API_BASE}/taxa",
                params={"q": scientific_name, "per_page": 5},
                headers=HEADERS,
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            results = response.json().get("results", [])
            if not results:
                return None
            for item in results:
                if (item.get("name") or "").strip() == scientific_name:
                    return item.get("id")
            return results[0].get("id")
        except RequestException as exc:
            print(f"  Taxa lookup failed (attempt {attempt}/{max_retries}): {exc}")
            if attempt < max_retries:
                time.sleep(backoff_base * attempt)
    return None


def fetch_taxon_names(
    session: requests.Session,
    taxon_id: int,
    *,
    max_retries: int,
    backoff_base: float,
    timeout_seconds: float,
) -> list[dict]:
    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(
                f"{WEB_BASE}/taxon_names.json",
                params={"taxon_id": taxon_id},
                headers=HEADERS,
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            data = response.json()
            return data.get("results", []) if isinstance(data, dict) else list(data)
        except RequestException as exc:
            print(f"  Taxon names lookup failed (attempt {attempt}/{max_retries}): {exc}")
            if attempt < max_retries:
                time.sleep(backoff_base * attempt)
    return []


def load_completed_taxa(out_csv: Path) -> set[str]:
    if not out_csv.exists():
        return set()
    completed: set[str] = set()
    try:
        with out_csv.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                name = (row.get("scientificName") or "").strip()
                if name:
                    completed.add(name)
    except Exception as exc:
        print(f"Failed to read existing CSV for resume: {exc}")
    return completed


def main() -> None:
    _set_csv_field_limit()
    args = build_arg_parser().parse_args()

    taxon_file = Path(args.taxon_file).resolve()
    out_csv = Path(args.out_csv).resolve()
    if not taxon_file.exists():
        raise SystemExit(f"Missing taxon source: {taxon_file}")

    names = sorted(set(iter_taxa(taxon_file)))
    if args.limit:
        names = names[: max(0, int(args.limit))]
    print(f"Found {len(names)} accepted fungal species to process")

    completed = set() if args.overwrite else load_completed_taxa(out_csv)
    remaining = [name for name in names if name not in completed]
    if completed:
        print(f"Resuming: {len(completed)} already written, {len(remaining)} remaining")

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if args.overwrite else "a"
    fieldnames = ["scientificName", "inaturalist_taxon_id"] + OUTPUT_LANGS

    with requests.Session() as session, out_csv.open(mode, encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if args.overwrite or not completed:
            writer.writeheader()
            handle.flush()

        for index, scientific_name in enumerate(remaining, start=1):
            print(f"[{index}/{len(remaining)}] {scientific_name}")
            taxon_id = fetch_taxon_id(
                session,
                scientific_name,
                max_retries=args.max_retries,
                backoff_base=args.backoff_base,
                timeout_seconds=args.timeout_seconds,
            )
            row = {field: "" for field in fieldnames}
            row["scientificName"] = scientific_name
            row["inaturalist_taxon_id"] = str(int(taxon_id)) if taxon_id else ""

            if taxon_id:
                entries = fetch_taxon_names(
                    session,
                    int(taxon_id),
                    max_retries=args.max_retries,
                    backoff_base=args.backoff_base,
                    timeout_seconds=args.timeout_seconds,
                )
                names_by_lang: dict[str, set[str]] = {code: set() for code in OUTPUT_LANGS}
                for entry in entries:
                    code = LEXICON_TO_CODE.get(normalize_lexicon(entry))
                    if not code:
                        continue
                    name = (entry.get("name") or "").strip()
                    if name:
                        names_by_lang[code].add(name)
                for code in OUTPUT_LANGS:
                    row[code] = "; ".join(sorted(names_by_lang[code]))
            else:
                print("  No taxon ID found")

            writer.writerow(row)
            handle.flush()
            time.sleep(max(0.0, float(args.request_delay)))

    print(f"Wrote {out_csv}")


if __name__ == "__main__":
    main()
