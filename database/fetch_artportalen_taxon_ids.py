#!/usr/bin/env python3
"""
Fetch Artportalen taxon ids for taxa in the local Sporely taxonomy database.

The script reads accepted species from ``taxon_min`` in
``vernacular_multilanguage.sqlite3``, queries Artportalen's taxon picker, and
writes a CSV mapping file with:

- local Artsdatabanken taxon id
- scientific name from the local DB
- Artportalen taxon id
- Swedish taxon name returned by Artportalen

Designed for long-running, polite scraping:
- adjustable pause and jitter between requests
- configurable retries and exponential backoff
- persistent session reuse
- periodic session refresh
- resume support from an existing output CSV

Example:
    python3 database/fetch_artportalen_taxon_ids.py \
        --cookie-json artportalen_cookies.json \
        --pause-seconds 1.2 \
        --pause-jitter 0.4 \
        --retries 5

By default, the script resumes from an existing output CSV by skipping any
``adb_taxon_id`` values already present there. Use ``--overwrite`` to start
fresh, or ``--no-resume`` to append without skipping.
"""

from __future__ import annotations

import argparse
import csv
import html
import http.cookiejar
import json
import random
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_SOURCE_DB = Path("database/vernacular_multilanguage.sqlite3")
DEFAULT_OUTPUT_CSV = Path("database/artportalen_taxon_ids.csv")
REPORT_URL = "https://www.artportalen.se/SubmitSighting/Report"
PICKER_URL = "https://www.artportalen.se/Taxon/PickerSearch"
HOME_URL = "https://www.artportalen.se/"

ITEMJSON_RE = re.compile(
    r'<span[^>]*class=["\']itemjson["\'][^>]*>(.*?)</span>',
    flags=re.IGNORECASE | re.DOTALL,
)
LOGIN_REDIRECT_MARKERS = (
    "useradmin-auth.slu.se",
    "/account/login",
)


@dataclass(frozen=True)
class LocalTaxon:
    adb_taxon_id: int
    genus: str
    specific_epithet: str

    @property
    def scientific_name(self) -> str:
        return f"{self.genus} {self.specific_epithet}".strip()


@dataclass(frozen=True)
class PickerResult:
    taxon_id: int
    taxon_name_sv: str
    scientific_name: str
    species_group_id: str
    protection_level_id: str
    is_leaf: bool


class SessionRequestError(RuntimeError):
    """HTTP/session level error raised by the lightweight URL session."""


@dataclass
class HttpResponse:
    status_code: int
    text: str
    url: str

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise SessionRequestError(f"HTTP {self.status_code}")


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    text = html.unescape(str(value))
    text = text.replace("\xa0", " ")
    text = text.replace("×", " x ")
    return " ".join(text.strip().lower().split())


def _cookie_from_name_value(name: str, value: str) -> http.cookiejar.Cookie:
    return http.cookiejar.Cookie(
        version=0,
        name=name,
        value=value,
        port=None,
        port_specified=False,
        domain=".artportalen.se",
        domain_specified=True,
        domain_initial_dot=True,
        path="/",
        path_specified=True,
        secure=False,
        expires=None,
        discard=True,
        comment=None,
        comment_url=None,
        rest={},
        rfc2109=False,
    )


class UrlSession:
    def __init__(self, headers: dict[str, str], cookies: dict[str, str]) -> None:
        self.headers = dict(headers)
        self.cookie_jar = http.cookiejar.CookieJar()
        for name, value in cookies.items():
            self.cookie_jar.set_cookie(_cookie_from_name_value(name, value))
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cookie_jar)
        )

    def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        timeout: float = 20.0,
        allow_redirects: bool = True,
    ) -> HttpResponse:
        del allow_redirects  # urllib follows redirects by default.
        full_url = url
        if params:
            query = urllib.parse.urlencode(params)
            sep = "&" if "?" in url else "?"
            full_url = f"{url}{sep}{query}"
        request = urllib.request.Request(full_url, headers=self.headers, method="GET")
        try:
            with self._opener.open(request, timeout=timeout) as response:
                status_code = int(getattr(response, "status", response.getcode()))
                raw = response.read()
                charset = response.headers.get_content_charset() or "utf-8"
                text = raw.decode(charset, errors="replace")
                return HttpResponse(
                    status_code=status_code,
                    text=text,
                    url=str(response.geturl() or full_url),
                )
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            charset = exc.headers.get_content_charset() or "utf-8"
            text = raw.decode(charset, errors="replace")
            return HttpResponse(
                status_code=int(exc.code),
                text=text,
                url=str(exc.geturl() or full_url),
            )
        except urllib.error.URLError as exc:
            raise SessionRequestError(str(exc.reason or exc)) from exc

    def close(self) -> None:
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cookie_jar)
        )


def _load_cookie_json(path: Path) -> dict[str, str]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Cookie JSON must be an object: {path}")
    cookies: dict[str, str] = {}
    for key, value in data.items():
        if key and value is not None:
            cookies[str(key)] = str(value)
    return cookies


def _parse_cookie_header(header: str) -> dict[str, str]:
    cookies: dict[str, str] = {}
    for part in header.split(";"):
        chunk = part.strip()
        if not chunk or "=" not in chunk:
            continue
        name, value = chunk.split("=", 1)
        name = name.strip()
        value = value.strip()
        if name:
            cookies[name] = value
    return cookies


def load_local_taxa(
    source_db: Path,
    offset: int,
    limit: int | None,
    taxon_id_min: int | None,
    taxon_id_max: int | None,
    genus_prefix: str | None,
) -> list[LocalTaxon]:
    if not source_db.exists():
        raise FileNotFoundError(f"Source DB not found: {source_db}")

    conditions: list[str] = []
    values: list[Any] = []

    if taxon_id_min is not None:
        conditions.append("taxon_id >= ?")
        values.append(int(taxon_id_min))
    if taxon_id_max is not None:
        conditions.append("taxon_id <= ?")
        values.append(int(taxon_id_max))
    if genus_prefix:
        conditions.append("genus LIKE ?")
        values.append(f"{genus_prefix.strip()}%")

    where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = (
        "SELECT taxon_id, genus, specific_epithet "
        "FROM taxon_min "
        f"{where_sql} "
        "ORDER BY taxon_id "
        "LIMIT ? OFFSET ?"
    )
    effective_limit = int(limit) if limit is not None else -1
    values.extend([effective_limit, int(offset)])

    conn = sqlite3.connect(source_db)
    try:
        rows = conn.execute(sql, values).fetchall()
    finally:
        conn.close()

    taxa: list[LocalTaxon] = []
    for adb_taxon_id, genus, specific_epithet in rows:
        if not genus or not specific_epithet:
            continue
        taxa.append(
            LocalTaxon(
                adb_taxon_id=int(adb_taxon_id),
                genus=str(genus).strip(),
                specific_epithet=str(specific_epithet).strip(),
            )
        )
    return taxa


def parse_picker_results(fragment: str) -> list[PickerResult]:
    results: list[PickerResult] = []
    for raw_payload in ITEMJSON_RE.findall(fragment or ""):
        payload_text = html.unescape(raw_payload.strip())
        if not payload_text.startswith("{"):
            continue
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            continue
        try:
            taxon_id = int(payload.get("taxonid"))
        except (TypeError, ValueError):
            continue
        results.append(
            PickerResult(
                taxon_id=taxon_id,
                taxon_name_sv=str(payload.get("taxonname") or "").strip(),
                scientific_name=str(payload.get("scientificname") or "").strip(),
                species_group_id=str(payload.get("speciesgroupid") or "").strip(),
                protection_level_id=str(payload.get("protectionlevelid") or "").strip(),
                is_leaf=_normalize_text(str(payload.get("leaf") or "")) == "true",
            )
        )
    return results


class ArtportalenTaxonFetcher:
    def __init__(
        self,
        cookies: dict[str, str],
        timeout_seconds: float,
        retries: int,
        retry_backoff: float,
        session_refresh_every: int,
        pause_seconds: float,
        pause_jitter: float,
        verbose: bool,
    ) -> None:
        self._cookies = dict(cookies)
        self.timeout_seconds = float(timeout_seconds)
        self.retries = max(0, int(retries))
        self.retry_backoff = max(0.0, float(retry_backoff))
        self.session_refresh_every = max(0, int(session_refresh_every))
        self.pause_seconds = max(0.0, float(pause_seconds))
        self.pause_jitter = max(0.0, float(pause_jitter))
        self.verbose = bool(verbose)
        self._requests_since_refresh = 0
        self.session = self._new_session()

    def _new_session(self) -> UrlSession:
        return UrlSession(
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:148.0) "
                    "Gecko/20100101 Firefox/148.0"
                ),
                "Accept": "text/html, */*; q=0.01",
                "Accept-Language": "en-US,en;q=0.9",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": REPORT_URL,
            },
            cookies=self._cookies,
        )

    def refresh_session(self, warmup: bool = True) -> None:
        self.session.close()
        self.session = self._new_session()
        self._requests_since_refresh = 0
        if warmup:
            self._warmup_session()

    def _warmup_session(self) -> None:
        for url in (REPORT_URL, HOME_URL):
            try:
                response = self.session.get(url, timeout=self.timeout_seconds)
                if response.status_code < 400:
                    return
            except SessionRequestError:
                continue

    def _maybe_pause(self) -> None:
        total = self.pause_seconds
        if self.pause_jitter:
            total += random.uniform(0.0, self.pause_jitter)
        if total > 0:
            time.sleep(total)

    def _maybe_refresh_session(self) -> None:
        if self.session_refresh_every and self._requests_since_refresh >= self.session_refresh_every:
            if self.verbose:
                print("[artportalen] refreshing session", file=sys.stderr)
            self.refresh_session(warmup=True)

    def search(self, scientific_name: str) -> list[PickerResult]:
        self._maybe_refresh_session()
        params = {
            "search": scientific_name,
            "returnformat": "html",
            "searchAllSpecies": "true",
            "speciesGroup": "undefined",
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
                    PICKER_URL,
                    params=params,
                    timeout=self.timeout_seconds,
                    allow_redirects=True,
                )
                self._requests_since_refresh += 1
                final_url = str(response.url or "").lower()
                if any(marker in final_url for marker in LOGIN_REDIRECT_MARKERS):
                    raise RuntimeError(
                        "Request was redirected to login. Provide fresh Artportalen cookies."
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
                        f"[artportalen] search failed for {scientific_name!r} "
                        f"(attempt {attempt + 1}/{self.retries + 1}): {exc}",
                        file=sys.stderr,
                    )
                continue

        if last_error is None:
            raise RuntimeError("Artportalen search failed for an unknown reason.")
        raise last_error


def choose_best_match(scientific_name: str, candidates: list[PickerResult]) -> tuple[str, PickerResult | None, str]:
    normalized_target = _normalize_text(scientific_name)
    exact = [item for item in candidates if _normalize_text(item.scientific_name) == normalized_target]
    exact_leaf = [item for item in exact if item.is_leaf]

    def _distinct(items: list[PickerResult]) -> list[PickerResult]:
        seen: set[int] = set()
        distinct: list[PickerResult] = []
        for item in items:
            if item.taxon_id in seen:
                continue
            seen.add(item.taxon_id)
            distinct.append(item)
        return distinct

    exact_leaf = _distinct(exact_leaf)
    exact = _distinct(exact)

    if len(exact_leaf) == 1:
        return "exact_leaf_match", exact_leaf[0], ""
    if len(exact_leaf) > 1:
        ids = ",".join(str(item.taxon_id) for item in exact_leaf[:5])
        return "ambiguous_exact_leaf_match", None, ids
    if len(exact) == 1:
        return "exact_match_non_leaf", exact[0], ""
    if len(exact) > 1:
        ids = ",".join(str(item.taxon_id) for item in exact[:5])
        return "ambiguous_exact_match", None, ids
    if not candidates:
        return "no_results", None, ""

    preview = "; ".join(
        f"{item.taxon_id}:{item.taxon_name_sv}|{item.scientific_name}"
        for item in candidates[:3]
    )
    return "no_exact_match", None, preview


def load_processed_ids(output_csv: Path) -> set[int]:
    if not output_csv.exists():
        return set()
    processed: set[int] = set()
    with output_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            raw = (row.get("adb_taxon_id") or "").strip()
            if not raw:
                continue
            try:
                processed.add(int(raw))
            except ValueError:
                continue
    return processed


def ensure_output_writer(output_csv: Path, overwrite: bool) -> tuple[Any, csv.DictWriter]:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    write_header = overwrite or not output_csv.exists() or output_csv.stat().st_size == 0
    mode = "w" if overwrite else "a"
    handle = output_csv.open(mode, encoding="utf-8", newline="")
    writer = csv.DictWriter(
        handle,
        fieldnames=[
            "adb_taxon_id",
            "scientific_name",
            "artportalen_taxon_id",
            "swedish_name",
            "matched_scientific_name",
            "species_group_id",
            "protection_level_id",
            "match_status",
            "match_note",
            "fetched_at_utc",
        ],
    )
    if write_header:
        writer.writeheader()
        handle.flush()
    return handle, writer


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch Artportalen taxon ids for taxa in the local taxonomy DB."
    )
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--cookie-json", type=Path, help="Path to a JSON file containing Artportalen cookies.")
    parser.add_argument("--cookie-header", help="Raw Cookie header value copied from a browser.")
    parser.add_argument("--pause-seconds", type=float, default=0.8)
    parser.add_argument(
        "--pause-jitter",
        type=float,
        default=0.4,
        help="Random extra delay added on top of --pause-seconds to avoid a fixed request rhythm.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--retry-backoff", type=float, default=2.0)
    parser.add_argument("--session-refresh-every", type=int, default=250)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--taxon-id-min", type=int)
    parser.add_argument("--taxon-id-max", type=int)
    parser.add_argument("--genus-prefix", help="Only process genera starting with this prefix.")
    parser.add_argument("--overwrite", action="store_true", help="Rewrite the output CSV from scratch.")
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Do not skip taxon ids already present in the output CSV.",
    )
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

    processed_ids: set[int] = set()
    if not args.overwrite and not args.no_resume:
        processed_ids = load_processed_ids(args.output_csv)

    taxa_to_process = [taxon for taxon in taxa if taxon.adb_taxon_id not in processed_ids]
    print(
        f"Selected {len(taxa)} taxa, {len(taxa_to_process)} remaining after resume filtering.",
        file=sys.stderr,
    )
    print(
        f"Resume mode: {'off' if args.no_resume or args.overwrite else 'on'}",
        file=sys.stderr,
    )

    handle, writer = ensure_output_writer(args.output_csv, overwrite=bool(args.overwrite))
    fetcher = ArtportalenTaxonFetcher(
        cookies=cookies,
        timeout_seconds=args.timeout_seconds,
        retries=args.retries,
        retry_backoff=args.retry_backoff,
        session_refresh_every=args.session_refresh_every,
        pause_seconds=args.pause_seconds,
        pause_jitter=args.pause_jitter,
        verbose=args.verbose,
    )

    started_at = time.time()
    success = 0
    unresolved = 0
    failed = 0

    try:
        fetcher.refresh_session(warmup=True)
        for index, taxon in enumerate(taxa_to_process, start=1):
            scientific_name = taxon.scientific_name
            print(
                f"[{index}/{len(taxa_to_process)}] {taxon.adb_taxon_id} {scientific_name}",
                flush=True,
            )
            try:
                candidates = fetcher.search(scientific_name)
                match_status, chosen, match_note = choose_best_match(scientific_name, candidates)
                row = {
                    "adb_taxon_id": taxon.adb_taxon_id,
                    "scientific_name": scientific_name,
                    "artportalen_taxon_id": chosen.taxon_id if chosen else "",
                    "swedish_name": chosen.taxon_name_sv if chosen else "",
                    "matched_scientific_name": chosen.scientific_name if chosen else "",
                    "species_group_id": chosen.species_group_id if chosen else "",
                    "protection_level_id": chosen.protection_level_id if chosen else "",
                    "match_status": match_status,
                    "match_note": match_note,
                    "fetched_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
                writer.writerow(row)
                handle.flush()

                if chosen:
                    success += 1
                else:
                    unresolved += 1
                    if args.verbose:
                        print(
                            f"[unresolved] {taxon.adb_taxon_id} {scientific_name}: "
                            f"{match_status} {match_note}",
                            file=sys.stderr,
                        )
            except Exception as exc:
                failed += 1
                writer.writerow(
                    {
                        "adb_taxon_id": taxon.adb_taxon_id,
                        "scientific_name": scientific_name,
                        "artportalen_taxon_id": "",
                        "swedish_name": "",
                        "matched_scientific_name": "",
                        "species_group_id": "",
                        "protection_level_id": "",
                        "match_status": "request_error",
                        "match_note": str(exc),
                        "fetched_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    }
                )
                handle.flush()
                if args.verbose:
                    print(
                        f"[error] {taxon.adb_taxon_id} {scientific_name}: {exc}",
                        file=sys.stderr,
                    )

            if index % 50 == 0 or index == len(taxa_to_process):
                elapsed = max(0.001, time.time() - started_at)
                rate = index / elapsed
                print(
                    f"Processed {index}/{len(taxa_to_process)} "
                    f"(matched={success}, unresolved={unresolved}, errors={failed}, "
                    f"{rate:.2f} taxa/s)",
                    file=sys.stderr,
                )
    finally:
        handle.close()
        fetcher.session.close()

    print(
        f"Finished. matched={success}, unresolved={unresolved}, errors={failed}, "
        f"output={args.output_csv}",
        file=sys.stderr,
    )
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
