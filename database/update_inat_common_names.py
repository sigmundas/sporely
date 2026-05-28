#!/usr/bin/env python3
"""Refresh the cached iNaturalist multilingual common-name CSV for Sporely.

This is the only script in the local taxonomy pipeline that talks to
iNaturalist. The normal SQLite rebuild should consume this generated CSV and
must not call iNaturalist itself.

Default paths, relative to this file in ``database/``::

    reference_data/sources/taxon.txt
    reference_data/sources/vernacularname.txt
    reference_data/generated/vernacular_inat_11lang.csv

Output CSV::

    scientificName,inaturalist_taxon_id,en,de,fr,es,da,sv,no,fi,pl,pt,it

Default scope is ``sporely``:

- exclude Animalia
- keep Basidiomycota species
- keep Ascomycota species only if they have Norwegian/Sámi vernacular names
- keep other fungal species only if they have Norwegian/Sámi vernacular names
- keep land-plant genera/species only if they have Norwegian/Sámi vernacular names
- exclude algal plant phyla and non-target kingdoms

Use ``--scope all`` if you really want all accepted Nortaxa rows matching the
generic filters.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from requests.exceptions import RequestException


SCRIPT_DIR = Path(__file__).resolve().parent
REFERENCE_DATA_DIR = SCRIPT_DIR / "reference_data"
REFERENCE_DATA_SOURCES_DIR = REFERENCE_DATA_DIR / "sources"
REFERENCE_DATA_GENERATED_DIR = REFERENCE_DATA_DIR / "generated"

DEFAULT_TAXON_FILE = REFERENCE_DATA_SOURCES_DIR / "taxon.txt"
DEFAULT_VERNACULAR_FILE = REFERENCE_DATA_SOURCES_DIR / "vernacularname.txt"
DEFAULT_OUT_CSV = REFERENCE_DATA_GENERATED_DIR / "vernacular_inat_11lang.csv"

OUTPUT_LANGS = ["en", "de", "fr", "es", "da", "sv", "no", "fi", "pl", "pt", "it"]
OUTPUT_FIELDS = ["scientificName", "inaturalist_taxon_id", *OUTPUT_LANGS]

LEXICON_TO_CODE = {
    "english": "en",
    "german": "de",
    "french": "fr",
    "spanish": "es",
    "danish": "da",
    "swedish": "sv",
    "norwegian": "no",
    "norwegian bokmal": "no",
    "norwegian bokmål": "no",
    "norwegian nynorsk": "no",
    "finnish": "fi",
    "polish": "pl",
    "portuguese": "pt",
    "brazilian-portuguese": "pt",
    "portuguese-brazil": "pt",
    "portuguese-brazilian": "pt",
    "italian": "it",
}

FUNGI_PHYLA = {
    "ascomycota",
    "basidiomycota",
    "chytridiomycota",
    "glomeromycota",
    "mucoromycota",
    "zoopagomycota",
    "mortierellomycota",
    "blastocladiomycota",
    "neocallimastigomycota",
    "entorrhizomycota",
}

LAND_PLANT_PHYLA = {
    "magnoliophyta",
    "pinophyta",
    "bryophyta",
    "marchantiophyta",
    "anthocerotophyta",
    "lycopodiophyta",
    "pteridophyta",
    "equisetophyta",
}

EXCLUDED_PLANT_PHYLA = {
    "chlorophyta",
    "rhodophyta",
    "glaucophyta",
    "cyanidiophyta",
    "charophyta",
}

NO_OR_SAMI_LANGUAGE_PREFIXES = (
    "no",
    "nb",
    "nn",
    "se",
    "sma",
    "smj",
    "smn",
    "sms",
)

API_BASE = "https://api.inaturalist.org/v1"
WEB_BASE = "https://www.inaturalist.org"
HEADERS = {
    "User-Agent": "Sporely/1.0 taxonomy refresh (contact: sigmund.aas@gmail.com)",
    "Accept-Encoding": "gzip",
}
CSV_FIELD_LIMIT = 1024 * 1024 * 10


@dataclass(frozen=True)
class SourceTaxon:
    scientific_name: str
    source_id: str
    taxon_id: str
    rank: str
    status: str
    kingdom: str
    phylum: str
    class_name: str
    order: str
    family: str
    genus: str
    has_no_or_sami_name: bool


class RateLimitedClient:
    def __init__(
        self,
        session: requests.Session,
        *,
        request_delay: float,
        rate_limit_sleep: float,
        max_retries: int,
        backoff_base: float,
        timeout_seconds: float,
    ) -> None:
        self.session = session
        self.request_delay = max(0.0, float(request_delay))
        self.rate_limit_sleep = max(0.0, float(rate_limit_sleep))
        self.max_retries = max(1, int(max_retries))
        self.backoff_base = max(0.0, float(backoff_base))
        self.timeout_seconds = float(timeout_seconds)
        self._last_request_at = 0.0

    def _wait_before_request(self) -> None:
        if self.request_delay <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        remaining = self.request_delay - elapsed
        if remaining > 0:
            time.sleep(remaining)

    def get_json(self, url: str, *, params: dict[str, Any]) -> Any | None:
        for attempt in range(1, self.max_retries + 1):
            self._wait_before_request()
            try:
                response = self.session.get(
                    url,
                    params=params,
                    headers=HEADERS,
                    timeout=self.timeout_seconds,
                )
                self._last_request_at = time.monotonic()

                if response.status_code == 429:
                    retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                    sleep_for = max(
                        self.rate_limit_sleep,
                        retry_after or 0.0,
                        self.backoff_base * attempt,
                    )
                    print(
                        f"  HTTP 429 rate limited; sleeping {sleep_for:.1f}s "
                        f"(attempt {attempt}/{self.max_retries})"
                    )
                    time.sleep(sleep_for)
                    continue

                response.raise_for_status()
                return response.json()

            except RequestException as exc:
                print(f"  request failed (attempt {attempt}/{self.max_retries}): {exc}")
                if attempt < self.max_retries:
                    time.sleep(self.backoff_base * attempt)

        return None


def _set_csv_field_limit() -> None:
    try:
        csv.field_size_limit(CSV_FIELD_LIMIT)
    except OverflowError:
        csv.field_size_limit(2_147_483_647)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _norm(value: Any) -> str:
    return _clean(value).casefold()


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Refresh multilingual iNaturalist common names and taxon IDs."
    )
    parser.add_argument("--taxon-file", type=Path, default=DEFAULT_TAXON_FILE)
    parser.add_argument("--vernacular-file", type=Path, default=DEFAULT_VERNACULAR_FILE)
    parser.add_argument("--out-csv", type=Path, default=DEFAULT_OUT_CSV)

    parser.add_argument(
        "--scope",
        choices=["sporely", "all"],
        default="sporely",
        help="sporely = reduced useful local DB scope; all = all valid rows matching filters.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Select and print taxa, but do not call iNaturalist and do not write CSV.",
    )
    parser.add_argument(
        "--print-first",
        type=int,
        default=25,
        help="Number of selected taxa to print in --dry-run mode.",
    )

    parser.add_argument(
        "--request-delay",
        type=float,
        default=1.25,
        help="Minimum delay between HTTP requests, not merely between taxa.",
    )
    parser.add_argument(
        "--rate-limit-sleep",
        type=float,
        default=120.0,
        help="Sleep duration after HTTP 429 when Retry-After is absent or shorter.",
    )
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--backoff-base", type=float, default=5.0)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)

    parser.add_argument("--overwrite", action="store_true", help="Rewrite output CSV from scratch.")
    parser.add_argument("--limit", type=int, help="Only process the first N taxa after filtering.")
    parser.add_argument(
        "--allow-fuzzy-match",
        action="store_true",
        help=(
            "Use the first iNaturalist result when no exact scientific-name match is found. "
            "Default is safer: write the row with an empty iNaturalist ID."
        ),
    )

    parser.add_argument(
        "--rank",
        default="",
        help="Optional generic Nortaxa taxonRank filter. Empty string means no generic rank filter.",
    )
    parser.add_argument(
        "--taxonomic-status",
        default="valid",
        help="Filter by Nortaxa taxonomicStatus. Empty string disables status filtering.",
    )
    parser.add_argument("--kingdom", help="Optional kingdom filter, e.g. Fungi, Plantae.")
    parser.add_argument("--phylum", help="Optional phylum filter.")
    parser.add_argument("--class-name", help="Optional class filter. Named class-name because class is reserved.")
    parser.add_argument("--order", help="Optional order filter.")
    parser.add_argument("--family", help="Optional family filter.")
    parser.add_argument("--genus", help="Optional genus filter.")
    parser.add_argument(
        "--fungi-only",
        action="store_true",
        help="Compatibility convenience filter for fungal phyla/kingdom.",
    )
    return parser


def _row_taxon_ids(row: dict[str, str]) -> set[str]:
    ids: set[str] = set()
    for key in ("id", "taxonID", "taxonId", "taxon_id"):
        value = _clean(row.get(key))
        if value:
            ids.add(value)
    return ids


def _language_is_no_or_sami(raw_language: str) -> bool:
    language = _norm(raw_language).replace("_", "-")
    if not language:
        return True
    return any(language == prefix or language.startswith(prefix + "-") for prefix in NO_OR_SAMI_LANGUAGE_PREFIXES)


def load_no_or_sami_vernacular_taxon_ids(vernacular_file: Path) -> set[str]:
    if not vernacular_file.exists():
        raise SystemExit(f"Missing vernacular source: {vernacular_file}")

    ids: set[str] = set()
    with vernacular_file.open("r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(4096)
        handle.seek(0)
        delimiter = "\t" if sample.count("\t") >= sample.count(",") else ","
        reader = csv.DictReader(handle, delimiter=delimiter)

        if not reader.fieldnames:
            raise SystemExit(f"Could not read header from {vernacular_file}")

        for row in reader:
            name = _clean(
                row.get("vernacularName")
                or row.get("vernacular_name")
                or row.get("name")
            )
            if not name:
                continue

            language = _clean(
                row.get("language")
                or row.get("languageCode")
                or row.get("language_code")
                or row.get("lang")
            )
            if not _language_is_no_or_sami(language):
                continue

            for value in _row_taxon_ids(row):
                ids.add(value)

    return ids


def row_to_source_taxon(row: dict[str, str], no_or_sami_ids: set[str]) -> SourceTaxon | None:
    scientific_name = _clean(row.get("scientificName"))
    if not scientific_name:
        return None

    ids = _row_taxon_ids(row)
    return SourceTaxon(
        scientific_name=scientific_name,
        source_id=_clean(row.get("id")),
        taxon_id=_clean(row.get("taxonID")),
        rank=_clean(row.get("taxonRank")),
        status=_clean(row.get("taxonomicStatus")),
        kingdom=_clean(row.get("kingdom")),
        phylum=_clean(row.get("phylum")),
        class_name=_clean(row.get("class")),
        order=_clean(row.get("order")),
        family=_clean(row.get("family")),
        genus=_clean(row.get("genus")),
        has_no_or_sami_name=bool(ids & no_or_sami_ids),
    )


def _passes_generic_filters(taxon: SourceTaxon, args: argparse.Namespace) -> bool:
    status_filter = _norm(args.taxonomic_status)
    if status_filter and _norm(taxon.status) != status_filter:
        return False

    rank_filter = _norm(args.rank)
    if rank_filter and _norm(taxon.rank) != rank_filter:
        return False

    if args.fungi_only and _norm(taxon.kingdom) != "fungi" and _norm(taxon.phylum) not in FUNGI_PHYLA:
        return False

    filters = {
        "kingdom": args.kingdom,
        "phylum": args.phylum,
        "class_name": args.class_name,
        "order": args.order,
        "family": args.family,
        "genus": args.genus,
    }
    for attr, requested in filters.items():
        if requested and _norm(getattr(taxon, attr)) != _norm(requested):
            return False

    return True


def _passes_sporely_scope(taxon: SourceTaxon) -> bool:
    kingdom = _norm(taxon.kingdom)
    phylum = _norm(taxon.phylum)
    rank = _norm(taxon.rank)

    if kingdom == "animalia":
        return False

    if kingdom == "fungi":
        if phylum == "basidiomycota":
            return rank == "species"
        if phylum == "ascomycota":
            return rank == "species" and taxon.has_no_or_sami_name
        if phylum in FUNGI_PHYLA:
            return rank == "species" and taxon.has_no_or_sami_name
        return False

    if kingdom == "plantae":
        if phylum in EXCLUDED_PLANT_PHYLA:
            return False
        if phylum in LAND_PLANT_PHYLA:
            return rank in {"species", "genus"} and taxon.has_no_or_sami_name
        return False

    return False


def source_taxon_matches(taxon: SourceTaxon, args: argparse.Namespace) -> bool:
    if not _passes_generic_filters(taxon, args):
        return False
    if args.scope == "sporely":
        return _passes_sporely_scope(taxon)
    if args.scope == "all":
        return True
    raise ValueError(f"Unknown scope: {args.scope}")


def iter_source_taxa(taxon_file: Path, no_or_sami_ids: set[str], args: argparse.Namespace) -> Iterator[SourceTaxon]:
    with taxon_file.open("r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(4096)
        handle.seek(0)
        delimiter = "\t" if sample.count("\t") >= sample.count(",") else ","
        reader = csv.DictReader(handle, delimiter=delimiter)

        if not reader.fieldnames:
            raise SystemExit(f"Could not read header from {taxon_file}")

        for row in reader:
            taxon = row_to_source_taxon(row, no_or_sami_ids)
            if taxon and source_taxon_matches(taxon, args):
                yield taxon


def normalize_lexicon(entry: dict[str, Any]) -> str:
    return _norm(entry.get("parameterized_lexicon") or entry.get("lexicon"))


def choose_inat_taxon_id(scientific_name: str, results: list[dict[str, Any]], allow_fuzzy: bool) -> int | None:
    wanted = _norm(scientific_name)

    for item in results:
        if _norm(item.get("name")) == wanted:
            return _int_or_none(item.get("id"))

    for item in results:
        matched_term = item.get("matched_term") or item.get("matchedTerm")
        if _norm(matched_term) == wanted:
            return _int_or_none(item.get("id"))

    if allow_fuzzy and results:
        return _int_or_none(results[0].get("id"))

    return None


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def fetch_taxon_id(
    client: RateLimitedClient,
    scientific_name: str,
    *,
    allow_fuzzy: bool,
) -> int | None:
    data = client.get_json(
        f"{API_BASE}/taxa",
        params={"q": scientific_name, "per_page": 10, "is_active": "true"},
    )
    if not isinstance(data, dict):
        return None

    results = data.get("results")
    if not isinstance(results, list):
        return None

    return choose_inat_taxon_id(scientific_name, results, allow_fuzzy=allow_fuzzy)


def fetch_taxon_names(client: RateLimitedClient, taxon_id: int) -> list[dict[str, Any]]:
    data = client.get_json(
        f"{WEB_BASE}/taxon_names.json",
        params={"taxon_id": taxon_id},
    )
    if isinstance(data, dict):
        results = data.get("results", [])
        return list(results) if isinstance(results, list) else []
    if isinstance(data, list):
        return data
    return []


def load_completed_taxa(out_csv: Path) -> set[str]:
    if not out_csv.exists():
        return set()

    completed: set[str] = set()
    try:
        with out_csv.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                return completed
            if "scientificName" not in reader.fieldnames:
                raise SystemExit(f"Existing CSV has no scientificName column: {out_csv}")
            if "inaturalist_taxon_id" not in reader.fieldnames:
                raise SystemExit(
                    f"Existing CSV lacks inaturalist_taxon_id: {out_csv}\n"
                    "Run with --overwrite or restore a CSV created by this script."
                )

            for row in reader:
                name = _clean(row.get("scientificName"))
                if name:
                    completed.add(name)
    except SystemExit:
        raise
    except Exception as exc:
        print(f"Failed to read existing CSV for resume: {exc}", file=sys.stderr)

    return completed


def row_for_taxon(
    client: RateLimitedClient,
    taxon: SourceTaxon,
    args: argparse.Namespace,
) -> dict[str, str]:
    row = {field: "" for field in OUTPUT_FIELDS}
    row["scientificName"] = taxon.scientific_name

    taxon_id = fetch_taxon_id(
        client,
        taxon.scientific_name,
        allow_fuzzy=bool(args.allow_fuzzy_match),
    )
    if taxon_id is None:
        print("  No exact iNaturalist taxon ID found")
        return row

    row["inaturalist_taxon_id"] = str(int(taxon_id))

    entries = fetch_taxon_names(client, int(taxon_id))
    names_by_lang: dict[str, set[str]] = {code: set() for code in OUTPUT_LANGS}

    for entry in entries:
        code = LEXICON_TO_CODE.get(normalize_lexicon(entry))
        if not code:
            continue
        name = _clean(entry.get("name"))
        if name:
            names_by_lang[code].add(name)

    for code in OUTPUT_LANGS:
        row[code] = "; ".join(sorted(names_by_lang[code], key=str.casefold))

    return row


def print_scope_summary(taxa: list[SourceTaxon]) -> None:
    by_kingdom: dict[str, int] = {}
    by_phylum: dict[tuple[str, str], int] = {}

    for taxon in taxa:
        kingdom = taxon.kingdom or "(blank)"
        phylum = taxon.phylum or "(blank)"
        by_kingdom[kingdom] = by_kingdom.get(kingdom, 0) + 1
        key = (kingdom, phylum)
        by_phylum[key] = by_phylum.get(key, 0) + 1

    print("Selected by kingdom:")
    for kingdom, count in sorted(by_kingdom.items(), key=lambda item: item[0].casefold()):
        print(f"  {kingdom}: {count}")

    print("Selected by kingdom/phylum:")
    for (kingdom, phylum), count in sorted(by_phylum.items(), key=lambda item: (item[0][0].casefold(), item[0][1].casefold())):
        print(f"  {kingdom} / {phylum}: {count}")


def main() -> int:
    _set_csv_field_limit()
    args = build_arg_parser().parse_args()

    taxon_file = Path(args.taxon_file).expanduser().resolve()
    vernacular_file = Path(args.vernacular_file).expanduser().resolve()
    out_csv = Path(args.out_csv).expanduser().resolve()

    if not taxon_file.exists():
        raise SystemExit(f"Missing taxon source: {taxon_file}")
    if not vernacular_file.exists():
        raise SystemExit(f"Missing vernacular source: {vernacular_file}")

    no_or_sami_ids = load_no_or_sami_vernacular_taxon_ids(vernacular_file)
    print(f"Loaded {len(no_or_sami_ids)} taxon IDs with Norwegian/Sámi vernacular names")

    taxa_by_name: dict[str, SourceTaxon] = {}
    for taxon in iter_source_taxa(taxon_file, no_or_sami_ids, args):
        existing = taxa_by_name.get(taxon.scientific_name)
        if existing is None:
            taxa_by_name[taxon.scientific_name] = taxon
        elif taxon.has_no_or_sami_name and not existing.has_no_or_sami_name:
            taxa_by_name[taxon.scientific_name] = taxon

    taxa = sorted(taxa_by_name.values(), key=lambda item: item.scientific_name.casefold())

    if args.limit is not None:
        taxa = taxa[: max(0, int(args.limit))]

    print(f"Selected {len(taxa)} Nortaxa rows using scope={args.scope!r}")
    print_scope_summary(taxa)

    if args.dry_run:
        print()
        print(f"First {min(len(taxa), int(args.print_first))} selected taxa:")
        for taxon in taxa[: max(0, int(args.print_first))]:
            marker = "NO/Sámi name" if taxon.has_no_or_sami_name else "no local vernacular"
            print(
                f"  {taxon.scientific_name} "
                f"[{taxon.kingdom}; {taxon.phylum}; {taxon.rank}; {marker}]"
            )
        return 0

    if not args.allow_fuzzy_match:
        print("Fuzzy iNaturalist matches are disabled; only exact/matched-term IDs will be stored.")

    completed = set() if args.overwrite else load_completed_taxa(out_csv)
    remaining = [taxon for taxon in taxa if taxon.scientific_name not in completed]
    if completed:
        print(f"Resuming: {len(completed)} already written, {len(remaining)} remaining")

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if args.overwrite else "a"
    write_header = args.overwrite or not out_csv.exists() or out_csv.stat().st_size == 0

    with requests.Session() as session, out_csv.open(mode, encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        if write_header:
            writer.writeheader()
            handle.flush()

        client = RateLimitedClient(
            session,
            request_delay=float(args.request_delay),
            rate_limit_sleep=float(args.rate_limit_sleep),
            max_retries=int(args.max_retries),
            backoff_base=float(args.backoff_base),
            timeout_seconds=float(args.timeout_seconds),
        )

        for index, taxon in enumerate(remaining, start=1):
            print(f"[{index}/{len(remaining)}] {taxon.scientific_name}")
            writer.writerow(row_for_taxon(client, taxon, args))
            handle.flush()

    print(f"Wrote {out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())