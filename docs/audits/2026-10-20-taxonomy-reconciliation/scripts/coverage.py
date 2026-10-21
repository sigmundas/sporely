#!/usr/bin/env python3
"""Section 8: offline publishing-identifier coverage, before and after the
category-A batch (and the optional B-placement batch).

Per live concept, from the release (read-only) and the COL archive's
col:alternativeID (via the work DB):

  Artsobservasjoner  direct  = taxon_min.norwegian_taxon_id, or an
                               'artsdatabanken' accepted id (the NorTaxa taxonID
                               the desktop already sends: database/models.py
                               _lookup_adb_taxon_id_from_db)
  Artportalen        direct  = 'artportalen' id, role accepted or publishing
                     source  = Dyntaxa Taxon number only (reviewed dyntaxa_taxon_id
                               bridge, or COL alternativeID dyntaxa:...:Taxon:N);
                               needs a validated Dyntaxa->Artportalen mapping
  iNaturalist        direct  = 'inaturalist' id or taxon_min.inaturalist_taxon_id
                     source  = COL alternativeID inat:N only; needs validation

  python -B scripts/coverage.py --workdb <workdb> --release <release copy> \
      --pairs <out>/cross_source_pairs.jsonl.gz --out <out>/coverage.json
"""
from __future__ import annotations

import argparse
import collections
import gzip
import json
import sqlite3
from pathlib import Path


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdb", required=True, type=Path)
    ap.add_argument("--release", required=True, type=Path)
    ap.add_argument("--pairs", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    a = ap.parse_args(argv)
    db = sqlite3.connect(f"file:{a.workdb}?mode=ro", uri=True)
    db.execute(f"attach database 'file:{a.release}?mode=ro' as r")

    concepts = {sid: (src, rank) for sid, src, rank in db.execute(
        "select taxon_id, source_system, taxon_rank from r.taxon_min")}
    cloud = {r[0] for r in db.execute("select sid from cloud_scope")}
    ids = collections.defaultdict(lambda: collections.defaultdict(set))
    for sid, no, inat in db.execute(
            "select taxon_id, norwegian_taxon_id, inaturalist_taxon_id from r.taxon_min"):
        if no:
            ids[sid]["artsobs"].add(no)
        if inat:
            ids[sid]["inat"].add(inat)
    for sid, src, eid, role in db.execute(
            "select taxon_id, source_system, external_id, id_role from r.taxon_external_id_min"):
        if src == "artsdatabanken" and role == "accepted":
            ids[sid]["artsobs"].add(eid)
        elif src == "artportalen" and role in ("accepted", "publishing"):
            ids[sid]["artportalen"].add(eid)
        elif src == "inaturalist" and role == "accepted":
            ids[sid]["inat"].add(eid)
    for sid, eid in db.execute(
            "select taxon_id, external_id from r.taxon_external_id_text_min where namespace='dyntaxa_taxon_id'"):
        if ":Taxon:" in eid:
            ids[sid]["dyntaxa_src"].add(int(eid.rsplit(":", 1)[1]))
    col_sid = {ident: sid for sid, ident in db.execute(
        "select sid, ident from taxa where src='col_xr'")}
    for ident, alt in db.execute(
            "select id, alt_id from col_raw where alt_id<>'' and status like '%accepted'"):
        sid = col_sid.get(ident)
        if sid is None:
            continue
        for tok in alt.split(","):
            tok = tok.strip()
            if tok.startswith("inat:") and tok[5:].isdigit():
                ids[sid]["inat_src"].add(int(tok[5:]))
            elif tok.startswith("dyntaxa:urn:lsid:dyntaxa.se:Taxon:"):
                v = tok.rsplit(":", 1)[1]
                if v.isdigit():
                    ids[sid]["dyntaxa_src"].add(int(v))

    pairs = [json.loads(l) for l in gzip.open(a.pairs, "rt")]

    def merged_view(batch):
        """retired -> survivor for the chosen batch; survivors take the union."""
        retire = {}
        for p in pairs:
            if batch(p):
                retire[p["nortaxa_sporely_taxon_id"]] = p["col_sporely_taxon_id"]
        view = {}
        for sid in concepts:
            if sid in retire:
                continue
            view[sid] = {k: set(v) for k, v in ids[sid].items()}
        for old, new in retire.items():
            for k, v in ids[old].items():
                view[new].setdefault(k, set()).update(v)
        view["__retire__"] = retire
        return view

    def measure(view, population):
        pop = [s for s in view if s != "__retire__" and population(s)]
        n = len(pop)
        def c(pred):
            k = sum(1 for s in pop if pred(view[s]))
            return {"n": k, "pct": round(100.0 * k / n, 1) if n else 0.0}
        return {
            "concepts": n,
            "artsobservasjoner_direct": c(lambda v: bool(v.get("artsobs"))),
            "artportalen_direct": c(lambda v: bool(v.get("artportalen"))),
            "artportalen_source_only_dyntaxa": c(lambda v: not v.get("artportalen") and bool(v.get("dyntaxa_src"))),
            "artportalen_none": c(lambda v: not v.get("artportalen") and not v.get("dyntaxa_src")),
            "inaturalist_direct": c(lambda v: bool(v.get("inat"))),
            "inaturalist_source_only_col": c(lambda v: not v.get("inat") and bool(v.get("inat_src"))),
            "inaturalist_none": c(lambda v: not v.get("inat") and not v.get("inat_src")),
            "all_three_direct": c(lambda v: bool(v.get("artsobs")) and bool(v.get("artportalen")) and bool(v.get("inat"))),
            "artportalen_direct_conflicts_dyntaxa_src": c(lambda v: bool(v.get("artportalen")) and bool(v.get("dyntaxa_src")) and not (v["artportalen"] & v["dyntaxa_src"])),
            "inat_direct_conflicts_col_src": c(lambda v: bool(v.get("inat")) and bool(v.get("inat_src")) and not (v["inat"] & v["inat_src"])),
        }

    observed = {r[0] for r in db.execute("select sid from obs_desktop")}
    if db.execute("select count(*) from sqlite_master where name='obs_cloud'").fetchone()[0]:
        observed |= {r[0] for r in db.execute("select sid from obs_cloud where sid is not null")}
    nortaxa_backed = {s for s in concepts if ids[s].get("artsobs")}
    pops = {
        "all_concepts": lambda s: True,
        "species_rank": lambda s: concepts[s][1] == "species",
        "species_rank_cloud_scope": lambda s: concepts[s][1] == "species" and s in cloud,
        "species_rank_nortaxa_concepts": lambda s: concepts[s] == ("nortaxa", "species"),
        # Norwegian-relevant: species concepts that carry (or, after a batch,
        # inherit) a NorTaxa taxonID; computed per view below.
        "species_rank_norwegian_relevant": None,
        "observed_concepts_desktop_or_cloud": "observed",
    }
    views = {
        "before": merged_view(lambda p: False),
        "after_A": merged_view(lambda p: p["category"] == "A"),
        "after_A_and_B_placement": merged_view(lambda p: p["category"] == "A" or p.get("b_subclass") == "placement_only"),
    }
    out = {}
    for v, view in views.items():
        out[v] = {}
        for p, f in pops.items():
            retire = view["__retire__"]
            if f is None:
                f = (lambda vw: (lambda s: concepts[s][1] == "species" and bool(vw[s].get("artsobs"))))(view)
            elif f == "observed":
                mapped = {retire.get(x, x) for x in observed}
                f = (lambda m: (lambda s: s in m))(mapped)
            out[v][p] = measure(view, f)
    # cloud scope after A: survivors are COL concepts, so the cloud population
    # is unchanged in size; its coverage improves through re-keyed ids.
    out["observed_ids_absent_from_release"] = {
        "count": len(observed - set(concepts)),
        "ids": sorted(observed - set(concepts)),
        "note": "excluded from every population above; W2EA2 supplement anchors 634896-634916",
    }
    out["note_artsobservasjoner"] = ("artsobservasjoner_direct here = the concept carries a NorTaxa id; "
                                     "the desktop's real name-keyed lookup is measured by artsobs_lookup.py")
    a.out.write_text(json.dumps(out, indent=1, sort_keys=True))
    for v in views:
        for p in ("species_rank", "species_rank_cloud_scope", "species_rank_norwegian_relevant", "observed_concepts_desktop_or_cloud"):
            m = out[v][p]
            print(v, p, m["concepts"], "artsobs", m["artsobservasjoner_direct"], "ap", m["artportalen_direct"],
                  "ap_src", m["artportalen_source_only_dyntaxa"], "inat", m["inaturalist_direct"],
                  "inat_src", m["inaturalist_source_only_col"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
