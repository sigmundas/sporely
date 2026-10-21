"""Classify GBIF / ChecklistBank results against Sporely's existing identities (offline).

Reads data/raw_sample.jsonl and raw_population.jsonl produced by run_queries.py (kept in the
scratch cache directory; regenerate with GBIF_OFFLINE=1 from the cache) plus the local pinned
COL id set (workdb col_raw, read-only) and writes:
  data/results.csv              one row per sampled NorTaxa concept (UTF-8 with BOM)
  data/population_provenance.csv one row per anchor x source-2030 COL usage (UTF-8 with BOM)
  data/summary.json             counts behind the README tables

A matching scientific name is never treated as concept equivalence. The categories are:
  identifier_provenance   ChecklistBank says the COL XR usage was copied from this exact NorTaxa
                          taxonID (VerbatimSource sourceDatasetKey=2030, sourceId=<taxonID>)
  name_exact              GBIF matching-ws EXACT on name, CLB names-index exact/variant authorship
  name_author_differs     GBIF EXACT/VARIANT on canonical name, CLB says authorship differs (canonical)
  synonym_match           GBIF matched a COL synonym and resolved it to its accepted usage
  ambiguous               GBIF matchType AMBIGUOUS, or >1 COL usage with the same canonical name
                          and rank among its candidates (homonym / duplicate) - GBIF still picks one
  fuzzy                   GBIF FUZZY
  missing                 GBIF HIGHERRANK / NONE (no usage at the NorTaxa rank)
Agreement is then compared with Sporely's reference COL usage (approved bridge, automatic name
bind, audit candidate, compiler single candidate, or none).
"""
import argparse, collections, csv, json, os, sqlite3

LICHEN = "lichen_proxy"


def w_csv(path, cols, rows):
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def prov(c3):
    if not isinstance(c3, dict) or "sourceDatasetKey" not in c3 and "datasetKey" not in c3:
        return "not_in_pinned_xr", ""
    if "sourceDatasetKey" not in c3:
        return "no_source_record", ""
    return str(c3.get("sourceDatasetKey")), str(c3.get("sourceId") or "")


def reference(r):
    s = r["stratum"]
    if s in ("A1", "A2"):
        return "approved_bridge", r["sporely_col_usage_id"]
    if s in ("B1", "B2"):
        return "automatic_name_bind", r["sporely_col_usage_id"]
    if s == "F":
        return ("synonym_of_bridged_concept" if r["sporely_col_usage_id"] else "synonym_of_nortaxa_only"), r["sporely_col_usage_id"]
    if r["audit_v2_outcome"]:
        return "audit_" + r["audit_v2_outcome"], r["audit_v2_col_usage_id"]
    cands = json.loads(r["compiler_candidates"] or "[]")
    if r["compiler_proposal"] == "ambiguous":
        return "compiler_ambiguous", ""
    if len(cands) == 1:
        return "compiler_single_candidate", cands[0]
    return "none", ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-sample", required=True)
    ap.add_argument("--raw-population", required=True)
    ap.add_argument("--workdb", required=True)
    ap.add_argument("--out-dir", required=True)
    a = ap.parse_args()
    c = sqlite3.connect(f"file:{a.workdb}?mode=ro", uri=True)
    pinned = {r[0]: r[1:] for r in c.execute("select id,status,source_id,name,authorship from col_raw")}

    out = []
    for line in open(a.raw_sample, encoding="utf-8"):
        rec = json.loads(line)
        r = rec["row"]
        o = {k: r[k] for k in ("stratum", "nortaxa_taxon_id", "scientific_name", "authorship", "rank",
                               "nortaxa_status", "class", LICHEN, "sporely_taxon_id", "sporely_bridge_class")}
        ref_kind, ref = reference(r)
        o["sporely_reference_kind"], o["sporely_reference_col_id"] = ref_kind, ref
        # G1/G2: GBIF NorTaxa checklist and its nub link
        res = (rec["G1"] or {}).get("results") or []
        g1 = res[0] if res else {}
        o["gbif_nt_key"] = g1.get("key", "")
        o["gbif_nt_status"] = g1.get("taxonomicStatus", "")
        o["gbif_nt_nubKey"] = g1.get("nubKey", "")
        g2 = rec["G2"] or {}
        o["gbif_backbone_name"] = g2.get("scientificName", "")
        o["gbif_backbone_status"] = g2.get("taxonomicStatus", "")
        o["nub_same_canonical"] = int(bool(g2) and (g2.get("canonicalName") or "").lower() == (g1.get("canonicalName") or "").lower())
        # G3: GBIF matching-ws -> COL XR (26.6)
        g3 = rec["G3"] or {}
        d = g3.get("diagnostics") or {}
        u = g3.get("usage") or {}
        au = g3.get("acceptedUsage") or {}
        mt = d.get("matchType", "")
        o["gbif_col_matchType"] = mt
        o["gbif_col_confidence"] = d.get("confidence", "")
        o["gbif_col_usage"] = u.get("key", "")
        o["gbif_col_usage_name"] = u.get("name", "")
        o["gbif_col_usage_status"] = u.get("status", "")
        o["gbif_col_accepted"] = au.get("key", "") or (u.get("key", "") if not g3.get("synonym") else "")
        o["gbif_col_rank"] = u.get("rank", "")
        same_name = set()
        for x in [g3] + (d.get("alternatives") or []):
            xu = x.get("usage") or {}
            if xu.get("canonicalName") and xu.get("canonicalName") == u.get("canonicalName") and xu.get("rank") == u.get("rank"):
                same_name.add(xu.get("key"))
        o["gbif_same_name_candidates"] = len(same_name)
        o["gbif_note"] = d.get("note", "")
        # C1/C2: ChecklistBank
        c1 = rec["C1"] or {}
        o["clb_nt_found"] = int(c1.get("id") == r["nortaxa_taxon_id"])
        o["clb_nt_namesIndexId"] = (c1.get("name") or {}).get("namesIndexId", "")
        c2 = rec["C2"] or {}
        cu = c2.get("usage") or {}
        o["clb_xr_type"] = c2.get("type", "")
        o["clb_xr_usage"] = cu.get("id", "") if cu.get("rank") == r["rank"] else ""
        o["clb_xr_status"] = cu.get("status", "") if o["clb_xr_usage"] else ""
        o["clb_xr_same_nidx"] = int(bool(o["clb_nt_namesIndexId"]) and cu.get("namesIndexId") == o["clb_nt_namesIndexId"])
        # C3: provenance
        provs = {}
        for cid, body in (rec["C3"] or {}).items():
            provs[cid] = prov(body)
        acc = o["gbif_col_accepted"]
        o["gbif_col_id_in_pinned_xr"] = (int(provs.get(acc, ("x",))[0] != "not_in_pinned_xr") if acc in provs else int(acc in pinned)) if acc else ""
        o["provenance"] = ";".join(f"{k}<-{v[0]}:{v[1]}" for k, v in sorted(provs.items()))
        idprov = [k for k, v in provs.items() if v[0] == "2030" and v[1] == r["nortaxa_taxon_id"]]
        o["identifier_provenance_col_ids"] = ";".join(sorted(idprov))
        other_nt = [f"{k}<-{v[1]}" for k, v in provs.items() if v[0] == "2030" and v[1] != r["nortaxa_taxon_id"]]
        o["provenance_other_nortaxa_ids"] = ";".join(sorted(other_nt))
        # provenance route (ChecklistBank, pinned 26.7 XR): independent of GBIF's name match
        prov_status = ""
        if idprov:
            st = pinned.get(idprov[0], ("",))[0]
            prov_status = st or "not_in_local_fungi_set"
        o["provenance_copy_status"] = prov_status
        if not idprov:
            pc = "no_copy_of_this_taxonID"
        elif ref and ref in idprov:
            pc = "copy_is_sporely_reference"
        elif ref:
            pc = "copy_differs_from_sporely_reference"
        else:
            pc = "copy_where_sporely_has_no_col_identity"
        o["provenance_category"] = pc
        o["gbif_target_is_copy"] = int(bool(idprov) and u.get("key") in idprov)
        # GBIF name-match category (matching-ws never uses NorTaxa identifiers)
        if mt in ("HIGHERRANK", "NONE", "") or not u:
            cat = "missing"
        elif mt == "AMBIGUOUS" or o["gbif_same_name_candidates"] > 1:
            cat = "ambiguous"
        elif g3.get("synonym"):
            cat = "synonym_match"
        elif mt == "FUZZY":
            cat = "fuzzy"
        elif o["clb_xr_type"] in ("exact", "variant") and o["clb_xr_usage"] == u.get("key"):
            cat = "name_exact"
        else:
            cat = "name_author_differs"
        o["gbif_category"] = cat
        tgt = o["gbif_col_accepted"] if mt not in ("HIGHERRANK", "NONE", "") else ""
        if not ref:
            agree = "sporely_has_no_col_identity" if tgt else "both_none"
        elif not tgt:
            agree = "gbif_none"
        elif tgt == ref:
            agree = "same"
        elif o["gbif_col_id_in_pinned_xr"] == 0:
            agree = "different_id_not_in_pinned_xr"
        else:
            agree = "different"
        o["agreement_with_sporely"] = agree
        out.append(o)
    cols = list(out[0].keys())
    w_csv(os.path.join(a.out_dir, "results.csv"), cols, out)

    pop = []
    for line in open(a.raw_population, encoding="utf-8"):
        rec = json.loads(line)
        p = rec["pair"]
        ds, sid = prov(rec["C3"])
        pop.append({"nortaxa_taxon_id": p[0], "nortaxa_name": p[1], "nortaxa_authorship": p[2], "nortaxa_rank": p[3],
                    "col_usage_id": p[4], "col_status": p[5], "col_name": p[6], "col_authorship": p[7], "col_rank": p[8],
                    "clb_source_dataset": ds, "clb_source_id": sid,
                    "same_nortaxa_id": int(ds == "2030" and sid == p[0]), "same_rank": int(p[3] == p[8])})
    w_csv(os.path.join(a.out_dir, "population_provenance.csv"), list(pop[0].keys()) if pop else ["empty"], pop)

    summ = {
        "sample_rows": len(out),
        "by_category": collections.Counter(o["gbif_category"] for o in out),
        "by_provenance_category": collections.Counter(o["provenance_category"] for o in out),
        "by_provenance_x_status": collections.Counter(f'{o["provenance_category"]}|{o["provenance_copy_status"]}' for o in out),
        "gbif_target_is_copy": sum(o["gbif_target_is_copy"] for o in out),
        "by_category_x_agreement": collections.Counter(f'{o["gbif_category"]}|{o["agreement_with_sporely"]}' for o in out),
        "by_reference_x_agreement": collections.Counter(f'{o["sporely_reference_kind"]}|{o["agreement_with_sporely"]}' for o in out),
        "by_stratum_x_category": collections.Counter(f'{o["stratum"]}|{o["gbif_category"]}' for o in out),
        "gbif_nt_found": sum(1 for o in out if o["gbif_nt_key"]),
        "gbif_nt_with_nubKey": sum(1 for o in out if o["gbif_nt_nubKey"]),
        "nub_same_canonical": sum(o["nub_same_canonical"] for o in out),
        "clb_nt_found": sum(o["clb_nt_found"] for o in out),
        "gbif_col_target_not_in_pinned_xr_fungi_set": sum(1 for o in out if o["gbif_col_id_in_pinned_xr"] == 0),
        "gbif_matchType": collections.Counter(o["gbif_col_matchType"] for o in out),
        "clb_xr_type": collections.Counter(o["clb_xr_type"] for o in out),
        "gbif_vs_clb_same_target": sum(1 for o in out if o["clb_xr_usage"] and o["clb_xr_usage"] == o["gbif_col_usage"]),
        "population_pairs": len(pop),
        "population_distinct_anchors": len({p["nortaxa_taxon_id"] for p in pop}),
        "population_same_nortaxa_id": sum(p["same_nortaxa_id"] for p in pop),
        "population_same_id_distinct_anchors": len({p["nortaxa_taxon_id"] for p in pop if p["same_nortaxa_id"]}),
        "population_same_id_by_col_status": collections.Counter(p["col_status"] for p in pop if p["same_nortaxa_id"]),
        "population_same_id_same_rank_accepted": len({p["nortaxa_taxon_id"] for p in pop if p["same_nortaxa_id"] and p["same_rank"] and p["col_status"] in ("accepted", "provisionally accepted")}),
        "population_source": collections.Counter(p["clb_source_dataset"] for p in pop),
    }
    with open(os.path.join(a.out_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summ, f, ensure_ascii=False, indent=1, sort_keys=True)
    print(json.dumps(summ, ensure_ascii=False, indent=1, sort_keys=True))


if __name__ == "__main__":
    main()
