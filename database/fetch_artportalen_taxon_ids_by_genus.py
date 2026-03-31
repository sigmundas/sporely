#!/usr/bin/env python3
"""
Fetch Artportalen taxon ids using a genus-first strategy.

Workflow
--------
1. Read local taxa from ``taxon_min`` in the existing Sporely taxonomy DB.
2. Group local taxa by genus.
3. Search Artportalen once per genus.
4. Resolve the matching genus node and fetch its descendants via
   ``/Taxon/RenderChildren``.
5. Match returned Swedish taxa back to the local Norway taxonomy by scientific
   name.
6. Write:
   - a local mapping CSV for taxa that exist in both systems
   - a second CSV for Swedish taxa that exist in Artportalen under that genus
     but do not exist in the local Norway taxonomy DB

By default, the script resumes from existing CSV output files.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fetch_artportalen_taxon_ids import (
    DEFAULT_SOURCE_DB,
    HOME_URL,
    PICKER_URL,
    REPORT_URL,
    ArtportalenTaxonFetcher,
    DEFAULT_OUTPUT_CSV,
    LocalTaxon,
    PickerResult,
    SessionRequestError,
    _load_cookie_json,
    _normalize_text,
    _parse_cookie_header,
    ensure_output_writer,
    load_local_taxa,
    load_processed_ids,
    parse_picker_results,
)


DEFAULT_LOCAL_OUTPUT_CSV = Path("database/artportalen_taxon_ids_by_genus.csv")
DEFAULT_SWEDISH_ONLY_CSV = Path("database/artportalen_taxon_ids_swedish_only.csv")
CHILDREN_URL = "https://www.artportalen.se/Taxon/RenderChildren"


@dataclass(frozen=True)
class GenusSelection:
    status: str
    node: PickerResult | None
    note: str = ""


class ArtportalenGenusFetcher(ArtportalenTaxonFetcher):
    def fetch_children(self, parent_id: int) -> list[PickerResult]:
        self._maybe_refresh_session()
        params = {
            "parentId": str(parent_id),
            "cache": str(int(time.time() * 1000)),
        }

        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            if attempt > 0:
                delay = self.retry_backoff * (2 ** (attempt - 1))
                if delay > 0:
                    time.sleep(delay)
                self.refresh_session(warmup=True)

            self._maybe_pause()
            try:
                response = self.session.get(
                    CHILDREN_URL,
                    params=params,
                    timeout=self.timeout_seconds,
                    allow_redirects=True,
                )
                self._requests_since_refresh += 1
                final_url = str(response.url or "").lower()
                if "useradmin-auth.slu.se" in final_url or "/account/login" in final_url:
                    raise RuntimeError(
                        "Child fetch was redirected to login. Provide fresh Artportalen cookies."
                    )
                if response.status_code == 429:
                    raise RuntimeError("Rate limited by Artportalen (HTTP 429).")
                if response.status_code >= 500:
                    raise RuntimeError(f"Server error from Artportalen (HTTP {response.status_code}).")
                response.raise_for_status()
                return parse_picker_results(response.text)
            except (SessionRequestError, RuntimeError) as exc:
                last_error = exc
                if self.verbose:
                    print(
                        f"[artportalen] child fetch failed for parent {parent_id} "
                        f"(attempt {attempt + 1}/{self.retries + 1}): {exc}",
                        file=sys.stderr,
                    )
                continue

        if last_error is None:
            raise RuntimeError(f"Fetching children failed for parent {parent_id}.")
        raise last_error

    def fetch_descendants(self, parent_id: int, max_depth: int = 4) -> list[PickerResult]:
        seen_ids: set[int] = set()
        results: list[PickerResult] = []

        def _walk(node_id: int, depth: int) -> None:
            children = self.fetch_children(node_id)
            for child in children:
                if child.taxon_id in seen_ids:
                    continue
                seen_ids.add(child.taxon_id)
                results.append(child)
                if not child.is_leaf and depth < max_depth:
                    _walk(child.taxon_id, depth + 1)

        _walk(parent_id, 1)
        return results


def group_taxa_by_genus(taxa: list[LocalTaxon]) -> dict[str, list[LocalTaxon]]:
    grouped: dict[str, list[LocalTaxon]] = defaultdict(list)
    for taxon in taxa:
        grouped[taxon.genus].append(taxon)
    return dict(sorted(grouped.items(), key=lambda item: item[0].lower()))


def choose_genus_node(genus: str, candidates: list[PickerResult]) -> GenusSelection:
    normalized_genus = _normalize_text(genus)
    exact = [item for item in candidates if _normalize_text(item.scientific_name) == normalized_genus]
    exact_nonleaf = [item for item in exact if not item.is_leaf]

    def _unique(items: list[PickerResult]) -> list[PickerResult]:
        seen: set[int] = set()
        out: list[PickerResult] = []
        for item in items:
            if item.taxon_id in seen:
                continue
            seen.add(item.taxon_id)
            out.append(item)
        return out

    exact = _unique(exact)
    exact_nonleaf = _unique(exact_nonleaf)

    if len(exact_nonleaf) == 1:
        return GenusSelection(status="exact_genus_match", node=exact_nonleaf[0])
    if len(exact_nonleaf) > 1:
        note = ",".join(str(item.taxon_id) for item in exact_nonleaf[:5])
        return GenusSelection(status="ambiguous_genus_match", node=None, note=note)
    if len(exact) == 1:
        return GenusSelection(status="exact_genus_leaf_match", node=exact[0])
    if len(exact) > 1:
        note = ",".join(str(item.taxon_id) for item in exact[:5])
        return GenusSelection(status="ambiguous_exact_genus_match", node=None, note=note)
    return GenusSelection(status="genus_not_found", node=None)


def load_processed_swedish_only(output_csv: Path) -> set[tuple[int, str]]:
    if not output_csv.exists():
        return set()
    processed: set[tuple[int, str]] = set()
    with output_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            raw_id = (row.get("artportalen_taxon_id") or "").strip()
            sci = _normalize_text(row.get("scientific_name") or "")
            if not raw_id or not sci:
                continue
            try:
                processed.add((int(raw_id), sci))
            except ValueError:
                continue
    return processed


def ensure_swedish_only_writer(output_csv: Path, overwrite: bool) -> tuple[Any, csv.DictWriter]:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    write_header = overwrite or not output_csv.exists() or output_csv.stat().st_size == 0
    mode = "w" if overwrite else "a"
    handle = output_csv.open(mode, encoding="utf-8", newline="")
    writer = csv.DictWriter(
        handle,
        fieldnames=[
            "genus",
            "genus_taxon_id",
            "artportalen_taxon_id",
            "swedish_name",
            "scientific_name",
            "species_group_id",
            "protection_level_id",
            "exists_in_norway",
            "note",
            "fetched_at_utc",
        ],
    )
    if write_header:
        writer.writeheader()
        handle.flush()
    return handle, writer


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch Artportalen taxon ids by searching genus first and mapping descendants."
    )
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_LOCAL_OUTPUT_CSV)
    parser.add_argument("--swedish-only-csv", type=Path, default=DEFAULT_SWEDISH_ONLY_CSV)
    parser.add_argument("--cookie-json", type=Path, help="Path to a JSON file containing Artportalen cookies.")
    parser.add_argument("--cookie-header", help="Raw Cookie header value copied from a browser.")
    parser.add_argument("--pause-seconds", type=float, default=0.4)
    parser.add_argument(
        "--pause-jitter",
        type=float,
        default=0.2,
        help="Random extra delay added on top of --pause-seconds.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--retry-backoff", type=float, default=2.0)
    parser.add_argument("--session-refresh-every", type=int, default=400)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--taxon-id-min", type=int)
    parser.add_argument("--taxon-id-max", type=int)
    parser.add_argument("--genus-prefix", help="Only process genera starting with this prefix.")
    parser.add_argument(
        "--max-depth",
        type=int,
        default=4,
        help="Maximum depth for recursive descendant fetching under a genus.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Rewrite both output CSVs from scratch.")
    parser.add_argument("--no-resume", action="store_true", help="Do not skip already-written rows.")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    cookies: dict[str, str] = {}
    if args.cookie_json:
        cookies.update(_load_cookie_json(args.cookie_json))
    if args.cookie_header:
        cookies.update(_parse_cookie_header(args.cookie_header))

    taxa = load_local_taxa(
        source_db=args.source_db,
        offset=args.offset,
        limit=args.limit,
        taxon_id_min=args.taxon_id_min,
        taxon_id_max=args.taxon_id_max,
        genus_prefix=args.genus_prefix,
    )
    if not taxa:
        print("No taxa selected.", file=sys.stderr)
        return 1

    processed_local_ids: set[int] = set()
    processed_sv_only: set[tuple[int, str]] = set()
    if not args.overwrite and not args.no_resume:
        processed_local_ids = load_processed_ids(args.output_csv)
        processed_sv_only = load_processed_swedish_only(args.swedish_only_csv)

    grouped = group_taxa_by_genus(taxa)
    pending_grouped: dict[str, list[LocalTaxon]] = {}
    for genus, genus_taxa in grouped.items():
        pending = [taxon for taxon in genus_taxa if taxon.adb_taxon_id not in processed_local_ids]
        if pending:
            pending_grouped[genus] = pending

    total_pending_taxa = sum(len(items) for items in pending_grouped.values())
    print(
        f"Selected {len(taxa)} taxa in {len(grouped)} genera; "
        f"{total_pending_taxa} taxa across {len(pending_grouped)} genera remaining after resume filtering.",
        file=sys.stderr,
    )
    print(
        f"Resume mode: {'off' if args.no_resume or args.overwrite else 'on'}",
        file=sys.stderr,
    )

    local_handle, local_writer = ensure_output_writer(args.output_csv, overwrite=bool(args.overwrite))
    sv_only_handle, sv_only_writer = ensure_swedish_only_writer(
        args.swedish_only_csv, overwrite=bool(args.overwrite)
    )
    fetcher = ArtportalenGenusFetcher(
        cookies=cookies,
        timeout_seconds=args.timeout_seconds,
        retries=args.retries,
        retry_backoff=args.retry_backoff,
        session_refresh_every=args.session_refresh_every,
        pause_seconds=args.pause_seconds,
        pause_jitter=args.pause_jitter,
        verbose=args.verbose,
    )

    matched = 0
    unresolved = 0
    errors = 0
    swedish_only_count = 0
    genus_index = 0
    started_at = time.time()

    try:
        fetcher.refresh_session(warmup=True)
        for genus, pending_taxa in pending_grouped.items():
            genus_index += 1
            all_local_taxa_for_genus = grouped[genus]
            print(
                f"[genus {genus_index}/{len(pending_grouped)}] {genus} "
                f"({len(pending_taxa)} pending local taxa)",
                flush=True,
            )
            try:
                search_results = fetcher.search(genus)
                selection = choose_genus_node(genus, search_results)
                if selection.node is None:
                    now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                    for taxon in pending_taxa:
                        local_writer.writerow(
                            {
                                "adb_taxon_id": taxon.adb_taxon_id,
                                "scientific_name": taxon.scientific_name,
                                "artportalen_taxon_id": "",
                                "swedish_name": "",
                                "matched_scientific_name": "",
                                "species_group_id": "",
                                "protection_level_id": "",
                                "match_status": selection.status,
                                "match_note": selection.note,
                                "fetched_at_utc": now_utc,
                            }
                        )
                        unresolved += 1
                    local_handle.flush()
                    continue

                descendants = fetcher.fetch_descendants(selection.node.taxon_id, max_depth=max(1, args.max_depth))
                leaf_by_scientific_name: dict[str, PickerResult] = {}
                for item in descendants:
                    normalized_name = _normalize_text(item.scientific_name)
                    if not normalized_name:
                        continue
                    existing = leaf_by_scientific_name.get(normalized_name)
                    if existing is None:
                        leaf_by_scientific_name[normalized_name] = item
                    elif not existing.is_leaf and item.is_leaf:
                        leaf_by_scientific_name[normalized_name] = item

                local_names_for_genus = {
                    _normalize_text(taxon.scientific_name): taxon for taxon in all_local_taxa_for_genus
                }

                now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                for taxon in pending_taxa:
                    scientific_name = taxon.scientific_name
                    normalized = _normalize_text(scientific_name)
                    chosen = leaf_by_scientific_name.get(normalized)
                    local_writer.writerow(
                        {
                            "adb_taxon_id": taxon.adb_taxon_id,
                            "scientific_name": scientific_name,
                            "artportalen_taxon_id": chosen.taxon_id if chosen else "",
                            "swedish_name": chosen.taxon_name_sv if chosen else "",
                            "matched_scientific_name": chosen.scientific_name if chosen else "",
                            "species_group_id": chosen.species_group_id if chosen else "",
                            "protection_level_id": chosen.protection_level_id if chosen else "",
                            "match_status": "matched_via_genus_descendants" if chosen else "not_found_under_genus",
                            "match_note": (
                                f"genus={genus}; genus_taxon_id={selection.node.taxon_id}; "
                                f"genus_status={selection.status}"
                            ),
                            "fetched_at_utc": now_utc,
                        }
                    )
                    if chosen:
                        matched += 1
                    else:
                        unresolved += 1
                local_handle.flush()

                for normalized_name, item in sorted(
                    leaf_by_scientific_name.items(),
                    key=lambda pair: pair[1].scientific_name.lower(),
                ):
                    if normalized_name in local_names_for_genus:
                        continue
                    key = (item.taxon_id, normalized_name)
                    if key in processed_sv_only:
                        continue
                    sv_only_writer.writerow(
                        {
                            "genus": genus,
                            "genus_taxon_id": selection.node.taxon_id,
                            "artportalen_taxon_id": item.taxon_id,
                            "swedish_name": item.taxon_name_sv,
                            "scientific_name": item.scientific_name,
                            "species_group_id": item.species_group_id,
                            "protection_level_id": item.protection_level_id,
                            "exists_in_norway": "false",
                            "note": "Exists in Artportalen under matched genus, but not in local Norway taxonomy DB.",
                            "fetched_at_utc": now_utc,
                        }
                    )
                    processed_sv_only.add(key)
                    swedish_only_count += 1
                sv_only_handle.flush()
            except Exception as exc:
                now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                for taxon in pending_taxa:
                    local_writer.writerow(
                        {
                            "adb_taxon_id": taxon.adb_taxon_id,
                            "scientific_name": taxon.scientific_name,
                            "artportalen_taxon_id": "",
                            "swedish_name": "",
                            "matched_scientific_name": "",
                            "species_group_id": "",
                            "protection_level_id": "",
                            "match_status": "request_error",
                            "match_note": str(exc),
                            "fetched_at_utc": now_utc,
                        }
                    )
                    errors += 1
                local_handle.flush()
                if args.verbose:
                    print(f"[error] genus {genus}: {exc}", file=sys.stderr)

            processed_taxa = matched + unresolved + errors
            if genus_index % 20 == 0 or genus_index == len(pending_grouped):
                elapsed = max(0.001, time.time() - started_at)
                rate = processed_taxa / elapsed
                print(
                    f"Processed {genus_index}/{len(pending_grouped)} genera "
                    f"(local matched={matched}, unresolved={unresolved}, errors={errors}, "
                    f"swedish_only={swedish_only_count}, {rate:.2f} local taxa/s)",
                    file=sys.stderr,
                )
    finally:
        local_handle.close()
        sv_only_handle.close()
        fetcher.session.close()

    print(
        f"Finished. local matched={matched}, unresolved={unresolved}, errors={errors}, "
        f"swedish_only={swedish_only_count}, output={args.output_csv}, "
        f"swedish_only_output={args.swedish_only_csv}",
        file=sys.stderr,
    )
    return 0 if errors == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
