"""Polite cached GET for public GBIF / ChecklistBank APIs (read-only).

Every response is stored as <cache>/<sha1(url)>.json with an index line in
<cache>/index.jsonl, so the experiment re-runs offline. Requests are
sequential with a fixed delay. Set GBIF_CACHE to the cache directory and
GBIF_OFFLINE=1 to forbid network access.
"""
import hashlib, json, os, sys, time, urllib.request, urllib.error

CACHE = os.environ.get("GBIF_CACHE", os.path.join(os.path.dirname(__file__), "..", "cache"))
DELAY = float(os.environ.get("GBIF_DELAY", "0.4"))
OFFLINE = os.environ.get("GBIF_OFFLINE") == "1"
UA = "sporely-taxonomy-audit/2026-10-21 (read-only research; contact via github.com/sigmundas)"
_last = [0.0]
stats = {"network": 0, "cache": 0}


def _path(url):
    return os.path.join(CACHE, hashlib.sha1(url.encode()).hexdigest() + ".json")


def get(url):
    os.makedirs(CACHE, exist_ok=True)
    p = _path(url)
    if os.path.exists(p):
        stats["cache"] += 1
        with open(p, encoding="utf-8") as f:
            return json.load(f)["body"]
    if OFFLINE:
        raise RuntimeError("offline and not cached: " + url)
    wait = DELAY - (time.time() - _last[0])
    if wait > 0:
        time.sleep(wait)
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    status, body = None, None
    for attempt in range(4):  # retry only transport timeouts, with backoff
        try:
            with urllib.request.urlopen(req, timeout=90) as r:
                status = r.status
                raw = r.read().decode("utf-8")
            break
        except urllib.error.HTTPError as e:
            status = e.code
            raw = e.read().decode("utf-8", "replace")
            break
        except (urllib.error.URLError, TimeoutError):
            if attempt == 3:
                raise
            time.sleep(10 * (attempt + 1))
    _last[0] = time.time()
    stats["network"] += 1
    try:
        body = json.loads(raw) if raw.strip() else None
    except ValueError:
        body = {"_non_json": raw[:2000]}
    rec = {"url": url, "status": status, "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "body": body}
    with open(p, "w", encoding="utf-8") as f:
        json.dump(rec, f, ensure_ascii=False)
    with open(os.path.join(CACHE, "index.jsonl"), "a", encoding="utf-8") as f:
        f.write(json.dumps({"url": url, "status": status, "fetched_at": rec["fetched_at"], "file": os.path.basename(p)}) + "\n")
    return body


if __name__ == "__main__":
    for u in sys.argv[1:]:
        print(json.dumps(get(u), ensure_ascii=False, indent=1)[:int(os.environ.get("MAXC", "3000"))])
