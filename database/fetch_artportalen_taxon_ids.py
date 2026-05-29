#!/usr/bin/env python3
"""Compatibility helpers for fetch_artportalen_taxon_ids_by_genus.py.

This file keeps the genus-first Artportalen fetcher working without restoring
the old single-species fetch script as an active workflow.
"""

from __future__ import annotations

import csv
import html
import json
import random
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from database.reference_data_paths import REFERENCE_DATA_GENERATED_DIR


DEFAULT_SOURCE_DB = REFERENCE_DATA_GENERATED_DIR / "vernacular_multilanguage.sqlite3"
DEFAULT_OUTPUT_CSV = REFERENCE_DATA_GENERATED_DIR / "artportalen_taxon_ids.csv"

HOME_URL = "https://www.artportalen.se/"
REPORT_URL = "https://www.artportalen.se/SubmitSighting/Report"
PICKER_URL = "https://www.artportalen.se/Taxon/PickerSearch"

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
    pass


class _RequestsSession:
    def __init__(self, headers: dict[str, str], cookies: dict[str, str]) -> None:
        self._session = requests.Session()
        self._session.headers.update(headers)
        for name, value in cookies.items():
            if name and value is not None:
                self._session.cookies.set(str(name), str(value), domain=".artportalen.se")

    def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        timeout: float = 20.0,
        allow_redirects: bool = True,
    ) -> requests.Response:
        try:
            return self._session.get(
                url,
                params=params,
                timeout=timeout,
                allow_redirects=allow_redirects,
            )
        except requests.RequestException as exc:
            raise SessionRequestError(str(exc)) from exc

    def close(self) -> None:
        self._session.close()


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    text = html.unescape(str(value))
    text = text.replace("\xa0", " ")
    text = text.replace("×", " x ")
    return " ".join(text.strip().lower().split())


def _load_cookie_json(path: Path) -> dict[str, str]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Cookie JSON must be an object: {path}")
    return {str(k): str(v) for k, v in data.items() if k and v is not None}


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


def _payload_value(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload:
            return payload.get(key)
    return None


def _payload_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _normalize_text(str(value or "")) in {"true", "1", "yes", "y"}


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

        raw_taxon_id = _payload_value(payload, "taxonid", "taxonId", "id")
        try:
            taxon_id = int(raw_taxon_id)
        except (TypeError, ValueError):
            continue

        results.append(
            PickerResult(
                taxon_id=taxon_id,
                taxon_name_sv=str(_payload_value(payload, "taxonname", "taxonName", "name") or "").strip(),
                scientific_name=str(_payload_value(payload, "scientificname", "scientificName") or "").strip(),
                species_group_id=str(_payload_value(payload, "speciesgroupid", "speciesGroupId") or "").strip(),
                protection_level_id=str(_payload_value(payload, "protectionlevelid", "protectionLevelId") or "").strip(),
                is_leaf=_payload_bool(_payload_value(payload, "leaf", "isLeaf")),
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

    def _new_session(self) -> _RequestsSession:
        return _RequestsSession(
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:148.0) "
                    "Gecko/20100101 Firefox/148.0"
                ),
                "Accept": "text/html, */*; q=0.01",
                "Accept-Language": "sv-SE,sv;q=0.9,en-US;q=0.8,en;q=0.7",
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
                print("[artportalen] refreshing session")
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
                    raise RuntimeError("Artportalen search redirected to login. Provide fresh cookies.")
                if response.status_code == 429:
                    raise RuntimeError("Rate limited by Artportalen (HTTP 429).")
                if response.status_code >= 500:
                    raise RuntimeError(f"Server error from Artportalen (HTTP {response.status_code}).")

                response.raise_for_status()
                return parse_picker_results(response.text)

            except (SessionRequestError, RuntimeError, requests.RequestException) as exc:
                last_error = exc
                if self.verbose:
                    print(
                        f"[artportalen] search failed for {scientific_name!r} "
                        f"(attempt {attempt + 1}/{self.retries + 1}): {exc}"
                    )
                continue

        if last_error is None:
            raise RuntimeError("Artportalen search failed for an unknown reason.")
        raise last_error


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
