"""Polite, capped, cached GET for public ChecklistBank requests (owner-authorised,
read-only, at most 25 network requests for pass 3). Every response is stored as
data/clb_cache/<sha1(url)>.json.gz = {url, status, fetched_at, body}; a cached URL
is never re-requested. CLB_OFFLINE=1 forbids network access.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

CACHE = Path(__file__).resolve().parents[1] / "data" / "clb_cache"
CAP = 25
UA = "sporely-taxonomy-audit/2026-10-22 (read-only research; owner-authorised; contact via github.com/sigmundas)"


def _path(url: str) -> Path:
    return CACHE / (hashlib.sha1(url.encode()).hexdigest() + ".json.gz")


def count() -> int:
    return len(list(CACHE.glob("*.json.gz"))) if CACHE.exists() else 0


def get(url: str, delay: float = 2.0, max_bytes: int = 60_000_000):
    CACHE.mkdir(parents=True, exist_ok=True)
    p = _path(url)
    if p.exists():
        return json.loads(gzip.decompress(p.read_bytes()))
    if os.environ.get("CLB_OFFLINE") == "1":
        raise RuntimeError("offline and not cached: " + url)
    if count() >= CAP:
        raise RuntimeError(f"request cap {CAP} reached")
    time.sleep(delay)
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=180) as r:
            status, raw = r.status, r.read(max_bytes + 1)
    except urllib.error.HTTPError as e:
        status, raw = e.code, e.read()
    truncated = len(raw) > max_bytes
    try:
        body = json.loads(raw[:max_bytes].decode("utf-8")) if not truncated else None
    except ValueError:
        body = {"_non_json": raw[:2000].decode("utf-8", "replace")}
    rec = {"url": url, "status": status, "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
           "bytes": len(raw), "truncated": truncated, "body": body}
    p.write_bytes(gzip.compress(json.dumps(rec, ensure_ascii=False).encode("utf-8"), mtime=0))
    return rec
