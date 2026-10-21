"""Query GBIF and ChecklistBank for each sampled NorTaxa concept (read-only, cached).

  sample      : per row of data/sample.csv
      G1  GBIF NorTaxa checklist usage by source taxonID   /v1/species?datasetKey=<NT>&sourceId=<id>
      G2  GBIF Backbone usage behind its nubKey            /v1/species/<nubKey>
      G3  GBIF matching-ws against COL XR (name-based)     /v2/species/match?checklistKey=<COL>&...&verbose=true
      C1  ChecklistBank NorTaxa (dataset 2030) usage       /dataset/2030/nameusage/<id>
      C2  ChecklistBank match against pinned 26.7 XR       /dataset/315834/match/nameusage?...&verbose=true
      C3  ChecklistBank provenance of each COL id involved /dataset/315834/nameusage/<colId>/source
  population  : C3 for every COL usage in the pinned release that (a) came from source 2030 and
                (b) shares the canonical name of a Sporely NorTaxa-only anchor (local workdb, read-only).

Responses are cached by fetch.py (GBIF_CACHE). Output: data/raw_<mode>.jsonl (one record per row).
"""
import argparse, csv, json, os, sqlite3, sys, urllib.parse

sys.path.insert(0, os.path.dirname(__file__))
import fetch  # noqa: E402

NT_GBIF = "a6c6cead-b5ce-4a4e-8cf5-1542ba708dec"
COL_GBIF = "7ddf754f-d193-4cc9-b351-99906754a03b"
XR = 315834
GBIF = "https://api.gbif.org"
CLB = "https://api.checklistbank.org"


def q(base, params):
    return base + "?" + urllib.parse.urlencode({k: v for k, v in params.items() if v not in (None, "")})


def source_of(col_id):
    if not col_id:
        return None
    d = fetch.get(f"{CLB}/dataset/{XR}/nameusage/{urllib.parse.quote(col_id)}/source")
    return d


def run_sample(sample, out):
    rows = list(csv.DictReader(open(sample, encoding="utf-8-sig")))
    with open(out, "w", encoding="utf-8") as fo:
        for n, r in enumerate(rows, 1):
            rec = {"row": r}
            g1 = fetch.get(q(f"{GBIF}/v1/species", {"datasetKey": NT_GBIF, "sourceId": r["nortaxa_taxon_id"]}))
            rec["G1"] = g1
            res = (g1 or {}).get("results") or []
            nub = res[0].get("nubKey") if res else None
            rec["G2"] = fetch.get(f"{GBIF}/v1/species/{nub}") if nub else None
            rank = (r["rank"] or "").upper()
            rec["G3"] = fetch.get(q(f"{GBIF}/v2/species/match", {
                "checklistKey": COL_GBIF, "scientificName": r["scientific_name"], "authorship": r["authorship"],
                "taxonRank": rank, "kingdom": r["kingdom"], "phylum": r["phylum"], "class": r["class"],
                "order": r["order"], "family": r["family"], "genus": r["genus"], "verbose": "true"}))
            rec["C1"] = fetch.get(f"{CLB}/dataset/2030/nameusage/{r['nortaxa_taxon_id']}")
            rec["C2"] = fetch.get(q(f"{CLB}/dataset/{XR}/match/nameusage", {
                "scientificName": r["scientific_name"], "authorship": r["authorship"], "rank": r["rank"],
                "code": "botanical", "kingdom": r["kingdom"], "family": r["family"], "genus": r["genus"],
                "verbose": "true"}))
            ids = set()
            for k in ("sporely_col_usage_id", "audit_v2_col_usage_id"):
                if r[k]:
                    ids.add(r[k])
            if r["compiler_candidates"]:
                ids.update(json.loads(r["compiler_candidates"]))
            u = (rec["G3"] or {}).get("usage") or {}
            au = (rec["G3"] or {}).get("acceptedUsage") or {}
            ids.update(x for x in (u.get("key"), au.get("key")) if x)
            cu = (rec["C2"] or {}).get("usage") or {}
            if cu.get("id") and cu.get("rank") == r["rank"]:
                ids.add(cu["id"])
            rec["C3"] = {i: source_of(i) for i in sorted(ids)}
            fo.write(json.dumps(rec, ensure_ascii=False) + "\n")
            if n % 20 == 0:
                print(n, fetch.stats, flush=True)


def run_population(workdb, out):
    c = sqlite3.connect(f"file:{workdb}?mode=ro", uri=True)
    pairs = c.execute("""
        select u.ident, u.name, u.authorship, u.rank, cr.id, cr.status, cr.name, cr.authorship, cr.rank
        from usages u join col_raw cr on cr.nkey = u.nkey and cr.source_id = '2030'
        where u.src = 'nortaxa' and u.binding = 'anchor' order by u.ident, cr.id""").fetchall()
    with open(out, "w", encoding="utf-8") as fo:
        for n, p in enumerate(pairs, 1):
            fo.write(json.dumps({"pair": p, "C3": source_of(p[4])}, ensure_ascii=False) + "\n")
            if n % 50 == 0:
                print(n, fetch.stats, flush=True)
    print("pairs", len(pairs))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["sample", "population"])
    ap.add_argument("--sample")
    ap.add_argument("--workdb")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    run_sample(a.sample, a.out) if a.mode == "sample" else run_population(a.workdb, a.out)
    print("done", fetch.stats)
