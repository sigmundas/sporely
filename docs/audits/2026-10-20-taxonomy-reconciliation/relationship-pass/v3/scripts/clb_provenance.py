#!/usr/bin/env python3
"""Extract ChecklistBank source-record provenance from the cached GBIF-research
responses (no network). Read-only.

Input: the cache directory of docs/audits/2026-10-21-gbif-nortaxa-col (one JSON
file per URL: {url, status, fetched_at, body}). Every cached
`/dataset/{315834|315557}/nameusage/{id}/source` response becomes one row:

  release_dataset_key, col_usage_id, http_status, source_dataset_key,
  source_record_id, sector_key, verbatim_source_key, fetched_at, cache_file

and is cross-checked against the GBIF folder's population_provenance.csv.

  python -B scripts/clb_provenance.py --cache <scratch>/gbif/cache \
      --population ../../../2026-10-21-gbif-nortaxa-col/data/population_provenance.csv \
      --out data/checklistbank_source_records.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path

sys.dont_write_bytecode = True
URL = re.compile(r"^https://api\.checklistbank\.org/dataset/(315834|315557)/nameusage/([^/]+)/source$")
COLS = ["release_dataset_key", "col_usage_id", "http_status", "source_dataset_key", "source_record_id",
        "sector_key", "verbatim_source_key", "source_entity", "fetched_at", "cache_file"]


def load(cache: Path) -> list[dict]:
    rows = []
    for p in sorted(cache.glob("*.json")):
        d = json.loads(p.read_text(encoding="utf-8"))
        m = URL.match(d.get("url") or "")
        if not m:
            continue
        b = d.get("body") or {}
        ok = d.get("status") == 200 and isinstance(b, dict) and "sourceDatasetKey" in b
        rows.append({"release_dataset_key": m.group(1), "col_usage_id": m.group(2), "http_status": d.get("status"),
                     "source_dataset_key": str(b.get("sourceDatasetKey")) if ok else "",
                     "source_record_id": str(b.get("sourceId") or "") if ok else "",
                     "sector_key": str(b.get("sectorKey") or "") if ok else "",
                     "verbatim_source_key": str(b.get("id") or "") if ok else "",
                     "source_entity": b.get("sourceEntity", "") if ok else "",
                     "fetched_at": d.get("fetched_at", ""), "cache_file": p.name})
    return sorted(rows, key=lambda r: (r["release_dataset_key"], r["col_usage_id"]))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", required=True, type=Path)
    ap.add_argument("--population", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    a = ap.parse_args(argv)
    rows = load(a.cache)
    with a.out.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        w.writeheader()
        w.writerows(rows)
    pinned = {r["col_usage_id"]: r for r in rows if r["release_dataset_key"] == "315834"}
    mism = 0
    pop = list(csv.DictReader(a.population.open(encoding="utf-8-sig")))
    for p in pop:
        r = pinned.get(p["col_usage_id"])
        if not r or (r["source_dataset_key"], r["source_record_id"]) != (p["clb_source_dataset"], p["clb_source_id"]):
            mism += 1
    by = {}
    for r in rows:
        k = (r["release_dataset_key"], r["source_dataset_key"] == "2030" and "2030" or
             (r["source_dataset_key"] and "other") or f"http_{r['http_status']}")
        by[k] = by.get(k, 0) + 1
    print(json.dumps({"rows": len(rows), "by_release_and_source": {f"{k[0]}|{k[1]}": v for k, v in sorted(by.items())},
                      "population_rows": len(pop), "population_mismatches": mism}, indent=1))
    return 1 if mism else 0


if __name__ == "__main__":
    raise SystemExit(main())
