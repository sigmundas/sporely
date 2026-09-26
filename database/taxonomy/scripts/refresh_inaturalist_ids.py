#!/usr/bin/env python3
"""Re-validate contradictory legacy iNaturalist ids against the iNaturalist API.

This is a network acquisition step, run by hand. It is deliberately not part
of ``build_release.py``: builds stay offline and read only the committed cache
this script writes.

Selection: the concepts of a built candidate that carry legacy iNaturalist ids
but no usable lookup id, because the legacy id is shared with another concept
or the concept has several (``build_sqlite_candidate.py`` only accepts
one-to-one pairings).

Per concept, the script searches ``/v1/taxa`` for the scientific name and
accepts an iNaturalist taxon only when all of these hold:

* exactly one active iNaturalist taxon has exactly the concept's scientific
  name (case-sensitive);
* its rank equals the concept's rank;
* its lineage is compatible: it descends from kingdom Fungi, and for a rank
  below genus its genus ancestor has the concept's genus name;
* no other concept in the candidate already uses the id as its lookup id,
  and the id is not resolved for more than one concept in this refresh (two
  concepts can share a scientific name, e.g. a COL concept and an unmapped
  NorTaxa concept).

Anything else leaves the concept unresolved, with the reason recorded. Every
request is recorded with its URL, time, HTTP status and response SHA-256, and
the evidence for each decision (candidates considered, matched taxon and
lineage) is kept so the decision can be audited or reproduced.

Output: ``<output-dir>/inaturalist-refresh.json`` (committed; its SHA-256 goes
into ``release-recipe.json``). ``--raw-dir`` keeps the raw responses.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Callable

API = "https://api.inaturalist.org/v1"
USER_AGENT = "Sporely taxonomy refresh (https://sporely.no)"
FORMAT = "sporely-inaturalist-refresh-v1"
FUNGI_KINGDOM_ID = 47170
DETAIL_BATCH = 30
ACCEPTANCE_RULES = (
    "exactly one active iNaturalist taxon has exactly the concept's scientific name",
    "the iNaturalist rank equals the concept's rank",
    "the iNaturalist taxon descends from kingdom Fungi (47170)",
    "below genus, the iNaturalist genus ancestor has the concept's genus name",
    "no other concept in the candidate already uses the id as its lookup id",
    "the id is not resolved for more than one concept in this refresh",
)

# (url) -> (http_status, body bytes)
Fetcher = Callable[[str], tuple[int, bytes]]


class RefreshError(Exception):
    pass


def http_fetcher(delay_seconds: float = 1.1, retries: int = 4) -> Fetcher:
    """A polite fetcher: one request per ``delay_seconds``, retrying 429/5xx."""
    last = [0.0]

    def fetch(url: str) -> tuple[int, bytes]:
        for attempt in range(retries + 1):
            wait = last[0] + delay_seconds - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            last[0] = time.monotonic()
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                           "Accept": "application/json"})
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    return response.status, response.read()
            except urllib.error.HTTPError as exc:
                if exc.code in (429, 500, 502, 503, 504) and attempt < retries:
                    time.sleep(5 * (attempt + 1))
                    continue
                return exc.code, exc.read()
            except urllib.error.URLError:
                if attempt < retries:
                    time.sleep(5 * (attempt + 1))
                    continue
                raise
        raise RefreshError(f"no response from {url}")
    return fetch


def affected_concepts(candidate: Path) -> list[dict]:
    """Concepts with legacy iNaturalist ids but no usable lookup id."""
    conn = sqlite3.connect(f"file:{candidate}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT t.taxon_id, t.canonical_scientific_name, t.taxon_rank, t.genus, t.family, "
            "       GROUP_CONCAT(e.external_id) "
            "FROM taxon_min t JOIN taxon_external_id_min e ON e.taxon_id = t.taxon_id "
            "WHERE e.source_system = 'inaturalist' AND t.inaturalist_taxon_id IS NULL "
            "GROUP BY t.taxon_id ORDER BY t.taxon_id").fetchall()
        lookup_ids = dict(conn.execute(
            "SELECT inaturalist_taxon_id, taxon_id FROM taxon_min WHERE inaturalist_taxon_id IS NOT NULL"))
    finally:
        conn.close()
    return [{"sporely_taxon_id": r[0], "scientific_name": r[1], "taxon_rank": r[2], "genus": r[3],
             "family": r[4], "legacy_inaturalist_ids": sorted({int(x) for x in r[5].split(",")}),
             "_lookup_ids": lookup_ids} for r in rows]


def _record(url: str, status: int, body: bytes) -> dict:
    return {"url": url, "retrieved_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "http_status": status, "response_sha256": hashlib.sha256(body).hexdigest()}


def _lineage(detail: dict) -> list[dict]:
    return [{"id": a.get("id"), "rank": a.get("rank"), "name": a.get("name")}
            for a in detail.get("ancestors") or []]


def decide(concept: dict, exact: list[dict], detail: dict | None, lookup_ids: dict[int, int]) -> tuple[str, str | None]:
    """Return (status, reason) for one concept under ACCEPTANCE_RULES."""
    if not exact:
        return "unresolved", "no_active_exact_name_match"
    if len(exact) > 1:
        return "unresolved", "multiple_active_exact_name_matches"
    taxon = exact[0]
    if taxon.get("rank") != concept["taxon_rank"]:
        return "unresolved", f"rank_mismatch:{taxon.get('rank')}"
    if detail is None:
        return "unresolved", "detail_unavailable"
    ancestor_ids = detail.get("ancestor_ids") or []
    if FUNGI_KINGDOM_ID not in ancestor_ids:
        return "unresolved", "not_in_kingdom_fungi"
    if concept["taxon_rank"] not in ("genus", "family", "order", "class", "phylum", "kingdom"):
        genus = next((a["name"] for a in _lineage(detail) if a["rank"] == "genus"), None)
        if genus != concept["genus"]:
            return "unresolved", f"genus_mismatch:{genus}"
    holder = lookup_ids.get(int(taxon["id"]))
    if holder is not None and holder != concept["sporely_taxon_id"]:
        return "unresolved", f"id_already_lookup_id_of:{holder}"
    return "resolved", None


def refresh(concepts: list[dict], fetch: Fetcher, *, raw_dir: Path | None = None,
            progress: Callable[[str], None] = lambda message: None) -> list[dict]:
    searches: dict[int, tuple[dict, list[dict], dict]] = {}
    for n, concept in enumerate(concepts, 1):
        query = urllib.parse.urlencode({"q": concept["scientific_name"], "per_page": 30})
        url = f"{API}/taxa?{query}"
        status, body = fetch(url)
        record = _record(url, status, body)
        if raw_dir:
            (raw_dir / f"search-{concept['sporely_taxon_id']}.json").write_bytes(body)
        results = json.loads(body).get("results", []) if status == 200 else []
        exact = [r for r in results if r.get("name") == concept["scientific_name"] and r.get("is_active")]
        searches[concept["sporely_taxon_id"]] = (record, results, exact)
        if n % 25 == 0:
            progress(f"searched {n}/{len(concepts)}")

    wanted = sorted({int(e[0]["id"]) for _, _, e in searches.values() if len(e) == 1})
    details: dict[int, tuple[dict, dict]] = {}
    for start in range(0, len(wanted), DETAIL_BATCH):
        ids = wanted[start:start + DETAIL_BATCH]
        url = f"{API}/taxa/{','.join(map(str, ids))}"
        status, body = fetch(url)
        record = _record(url, status, body)
        if raw_dir:
            (raw_dir / f"detail-{ids[0]}-{ids[-1]}.json").write_bytes(body)
        for detail in (json.loads(body).get("results", []) if status == 200 else []):
            details[int(detail["id"])] = (record, detail)
    progress(f"fetched details for {len(details)} taxa")

    entries = []
    for concept in concepts:
        record, results, exact = searches[concept["sporely_taxon_id"]]
        detail_record, detail = details.get(int(exact[0]["id"]), (None, None)) if len(exact) == 1 else (None, None)
        status, reason = decide(concept, exact, detail, concept["_lookup_ids"])
        entry = {
            "sporely_taxon_id": concept["sporely_taxon_id"],
            "scientific_name": concept["scientific_name"],
            "taxon_rank": concept["taxon_rank"],
            "genus": concept["genus"],
            "family": concept["family"],
            "legacy_inaturalist_ids": concept["legacy_inaturalist_ids"],
            "status": status,
            "candidates_considered": [
                {"taxon_id": r.get("id"), "name": r.get("name"), "rank": r.get("rank"),
                 "is_active": r.get("is_active"), "matched_term": r.get("matched_term")}
                for r in results if r.get("name") == concept["scientific_name"]],
            "requests": [record] + ([detail_record] if detail_record else []),
        }
        if reason:
            entry["reason"] = reason
        if detail is not None:
            lineage = _lineage(detail)
            entry["inaturalist"] = {
                "taxon_id": int(detail["id"]), "name": detail.get("name"), "rank": detail.get("rank"),
                "is_active": detail.get("is_active"),
                "lineage": [a for a in lineage if a["rank"] in ("kingdom", "phylum", "class", "order",
                                                                 "family", "genus")],
            }
            family = next((a["name"] for a in lineage if a["rank"] == "family"), None)
            entry["checks"] = {"kingdom_fungi": FUNGI_KINGDOM_ID in (detail.get("ancestor_ids") or []),
                               "genus_matches": next((a["name"] for a in lineage if a["rank"] == "genus"), None)
                               == concept["genus"],
                               "family_matches": family == concept["family"] if concept["family"] else None}
        entries.append(entry)
    return entries


def enforce_one_to_one(entries: list[dict]) -> int:
    """Unresolve every entry whose id would also be resolved for another concept."""
    by_id: dict[int, list[dict]] = {}
    for entry in entries:
        if entry["status"] == "resolved":
            by_id.setdefault(int(entry["inaturalist"]["taxon_id"]), []).append(entry)
    changed = 0
    for group in by_id.values():
        if len(group) > 1:
            others = sorted(e["sporely_taxon_id"] for e in group)
            for entry in group:
                entry["status"] = "unresolved"
                entry["reason"] = f"id_resolved_for_multiple_concepts:{','.join(map(str, others))}"
                changed += 1
    return changed


def _counts(entries: list[dict]) -> dict:
    resolved = sum(1 for e in entries if e["status"] == "resolved")
    reasons: dict[str, int] = {}
    for e in entries:
        if e["status"] != "resolved":
            key = e["reason"].split(":")[0]
            reasons[key] = reasons.get(key, 0) + 1
    return {"concepts": len(entries), "resolved": resolved, "unresolved": len(entries) - resolved,
            "unresolved_by_reason": dict(sorted(reasons.items()))}


def build_document(entries: list[dict], *, candidate: Path, acquired_on: str) -> dict:
    conn = sqlite3.connect(f"file:{candidate}?mode=ro", uri=True)
    try:
        release_id = dict(conn.execute("SELECT key, value FROM taxonomy_meta")).get("content_release_id")
    finally:
        conn.close()
    digest = hashlib.sha256()
    with candidate.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    enforce_one_to_one(entries)
    return {
        "format": FORMAT,
        "acquired_on": acquired_on,
        "source": {"api": API, "endpoints": ["GET /taxa?q=<scientific name>&per_page=30",
                                             "GET /taxa/<id,...>"],
                   "user_agent": USER_AGENT},
        "tool": "database/taxonomy/scripts/refresh_inaturalist_ids.py",
        "selection": {
            "rule": "concepts with legacy iNaturalist ids but no one-to-one lookup id",
            "candidate_content_release_id": release_id,
            "candidate_sqlite_sha256": digest.hexdigest(),
        },
        "acceptance_rules": list(ACCEPTANCE_RULES),
        "counts": _counts(entries),
        "entries": sorted(entries, key=lambda e: e["sporely_taxon_id"]),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--candidate", type=Path, help="candidate SQLite built by build_release.py")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--recheck", type=Path, metavar="REFRESH_JSON",
                        help="offline: re-apply the acceptance rules to an existing refresh file's "
                             "recorded evidence and rewrite it (no network)")
    parser.add_argument("--raw-dir", type=Path, help="keep raw API responses here (not committed)")
    parser.add_argument("--limit", type=int, help="only the first N concepts (for a trial run)")
    parser.add_argument("--list", action="store_true", help="print the selected concepts and exit")
    args = parser.parse_args(argv)

    if args.recheck:
        document = json.loads(args.recheck.read_text(encoding="utf-8"))
        if document.get("format") != FORMAT:
            print(f"error: {args.recheck} is not a {FORMAT} file", file=sys.stderr)
            return 2
        changed = enforce_one_to_one(document["entries"])
        if changed == 0 and document.get("acceptance_rules") == list(ACCEPTANCE_RULES):
            print("no change; file left as is (its pinned SHA-256 still holds)")
            return 0
        document["acceptance_rules"] = list(ACCEPTANCE_RULES)
        document["counts"] = _counts(document["entries"])
        document.setdefault("rechecks", []).append(
            {"on": dt.date.today().isoformat(), "rule": ACCEPTANCE_RULES[-1], "entries_changed": changed})
        args.recheck.write_text(json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                                encoding="utf-8")
        print(json.dumps(document["counts"], indent=2))
        return 0
    if not (args.candidate and args.output_dir):
        parser.error("--candidate and --output-dir are required unless --recheck is given")
    concepts = affected_concepts(args.candidate)
    if args.limit:
        concepts = concepts[:args.limit]
    if args.list:
        for c in concepts:
            print(c["sporely_taxon_id"], c["scientific_name"], c["legacy_inaturalist_ids"])
        print(f"{len(concepts)} concepts", file=sys.stderr)
        return 0
    output = args.output_dir / "inaturalist-refresh.json"
    if output.exists():
        print(f"error: {output} exists; acquisitions are not overwritten", file=sys.stderr)
        return 2
    if args.raw_dir:
        args.raw_dir.mkdir(parents=True, exist_ok=True)
    acquired_on = dt.date.today().isoformat()
    entries = refresh(concepts, http_fetcher(), raw_dir=args.raw_dir,
                      progress=lambda m: print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True))
    document = build_document(entries, candidate=args.candidate, acquired_on=acquired_on)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(document["counts"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
