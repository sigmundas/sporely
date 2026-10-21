#!/usr/bin/env python3
"""COL usage-id churn between COL26.6 XR (ChecklistBank 315557) and the pinned
COL26.7 XR (315834), and its exposure in Sporely's identity anchors. Read-only,
no network: it uses data/checklistbank_source_records.csv (cached /source
responses on both releases), the cached source-2030 metrics of both releases,
and the audit work DB (immutable).

Per COL id queried on both releases, the pinned (26.7) source record is
compared with 26.6: same_source_record, different_source_record, or absent.
Ids queried on 26.7 that 404 there (26.6 ids GBIF returned) are listed as
absent_in_26_7.

  python -B scripts/id_churn.py --clb data/checklistbank_source_records.csv \
      --http <scratch>/pass3/http --workdb <scratch>/rev5/workdb.sqlite3 \
      --out-csv data/id_churn.csv --out-json data/id_churn_summary.json
"""
from __future__ import annotations

import argparse
import collections
import csv
import json
import sqlite3
import sys
from pathlib import Path

sys.dont_write_bytecode = True


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    for x in ("clb", "http", "workdb", "out-csv", "out-json"):
        ap.add_argument("--" + x, required=True, type=Path)
    a = ap.parse_args(argv)
    rows = list(csv.DictReader(a.clb.open(encoding="utf-8-sig")))
    r26_7 = {r["col_usage_id"]: r for r in rows if r["release_dataset_key"] == "315834"}
    r26_6 = {r["col_usage_id"]: r for r in rows if r["release_dataset_key"] == "315557"}
    w = sqlite3.connect(f"file:{a.workdb}?mode=ro&immutable=1", uri=True)
    local_src = dict(w.execute("select id, source_id from col_raw"))
    local_status = dict(w.execute("select id, status from col_raw"))

    out, tally = [], collections.Counter()
    for cid in sorted(set(r26_6) | {k for k, v in r26_7.items() if not v["source_dataset_key"]}):
        p, o = r26_7.get(cid), r26_6.get(cid)
        if p and p["source_dataset_key"]:
            grp = "2030_nortaxa" if p["source_dataset_key"] == "2030" else "other_source"
            if o is None:
                st = "not_queried_on_26_6"
            elif not o["source_dataset_key"]:
                st = "absent_in_26_6"
            elif (o["source_dataset_key"], o["source_record_id"]) == (p["source_dataset_key"], p["source_record_id"]):
                st = "same_source_record"
            else:
                st = "different_source_record"
            src26_7 = f"{p['source_dataset_key']}:{p['source_record_id']}"
        else:   # a 26.6 id GBIF returned that 404s on the pinned release
            grp = ("2030_nortaxa" if o and o["source_dataset_key"] == "2030" else
                   "other_source" if o and o["source_dataset_key"] else "unknown")
            st, src26_7 = "absent_in_26_7", ""
        tally[(grp, st)] += 1
        out.append({"col_usage_id": cid, "group": grp, "status": st,
                    "source_record_26_7": src26_7,
                    "source_record_26_6": f"{o['source_dataset_key']}:{o['source_record_id']}" if o and o["source_dataset_key"] else "",
                    "local_26_7_source_id": local_src.get(cid, ""), "local_26_7_status": local_status.get(cid, "")})
    # re-identified records: same source record under a different id in the other release
    by_rec_7 = {f"{v['source_dataset_key']}:{v['source_record_id']}": k for k, v in r26_7.items() if v["source_dataset_key"]}
    for r in out:
        if r["status"] == "absent_in_26_7" and r["source_record_26_6"] in by_rec_7:
            r["same_record_in_26_7_as"] = by_rec_7[r["source_record_26_6"]]
    with a.out_csv.open("w", newline="", encoding="utf-8-sig") as f:
        cols = ["col_usage_id", "group", "status", "source_record_26_7", "source_record_26_6",
                "same_record_in_26_7_as", "local_26_7_source_id", "local_26_7_status"]
        cw = csv.DictWriter(f, fieldnames=cols)
        cw.writeheader()
        for r in out:
            cw.writerow({c: r.get(c, "") for c in cols})

    # release metadata (cached): names contributed by NorTaxa (source 2030)
    meta = {}
    for p in a.http.glob("*.json"):
        d = json.loads(p.read_text(encoding="utf-8"))
        for key in ("315557", "315834"):
            if d.get("url") == f"https://api.checklistbank.org/dataset/{key}/source/2030/metrics":
                b = d["body"]
                meta[key] = {k: b.get(k) for k in ("nameCount", "taxonCount", "synonymCount", "vernacularCount")}
    # Sporely exposure: anchors keyed on a COL id whose pinned source is NorTaxa (2030)
    exp = {}
    exp["col_anchors_total"] = w.execute("select count(*) from taxa where src='col_xr'").fetchone()[0]
    exp["col_anchors_on_source_2030"] = w.execute(
        "select count(*) from taxa t join col_raw c on c.id=t.ident where t.src='col_xr' and c.source_id='2030'").fetchone()[0]
    exp["col_aliases_on_source_2030"] = w.execute(
        "select count(*) from usages u join col_raw c on c.id=u.ident where u.src='col_xr' and u.binding!='anchor' "
        "and c.source_id='2030'").fetchone()[0]
    exp["nortaxa_bridged_onto_source_2030_anchor"] = w.execute(
        "select count(*) from usages u join taxa t on t.sid=u.sid join col_raw c on c.id=t.ident "
        "where u.src='nortaxa' and t.src='col_xr' and c.source_id='2030'").fetchone()[0]
    exp["desktop_observed_concepts_on_source_2030"] = w.execute(
        "select count(*), coalesce(sum(o.n),0) from obs_desktop o join taxa t on t.sid=o.sid join col_raw c on c.id=t.ident "
        "where t.src='col_xr' and c.source_id='2030'").fetchone()
    exp["cloud_observed_concepts_on_source_2030"] = w.execute(
        "select count(distinct o.sid), coalesce(sum(o.n),0) from obs_cloud o join taxa t on t.sid=o.sid join col_raw c on c.id=t.ident "
        "where t.src='col_xr' and c.source_id='2030' and o.kind in ('resolved','selected')").fetchone()
    # Observed counts only. No rates, intervals or projections: the ids are those the GBIF
    # research happened to touch (selected, mixed accepted/synonym, several event types).
    backward = collections.Counter()      # ids of the pinned 26.7, looked up in 26.6
    for r in out:
        if r["status"] in ("same_source_record", "different_source_record", "absent_in_26_6"):
            nt = r["source_record_26_7"].startswith("2030:") or r["source_record_26_6"].startswith("2030:")
            backward["nortaxa_in_either_release" if nt else "never_nortaxa", r["status"]] += 1
    forward = collections.Counter()       # ids of 26.6 (with a source record), looked up in the pinned 26.7
    for cid, o in r26_6.items():
        if not o["source_dataset_key"]:
            continue
        g = "2030_nortaxa_in_26_6" if o["source_dataset_key"] == "2030" else "other_source_in_26_6"
        p = r26_7.get(cid)
        st = (("absent_in_26_7(local_pinned_fungal_archive)" if cid not in local_src else "present_in_local_archive(not_queried)")
              if p is None else "absent_in_26_7" if not p["source_dataset_key"] else
              "same_source_record" if (p["source_dataset_key"], p["source_record_id"]) ==
              (o["source_dataset_key"], o["source_record_id"]) else "different_source_record")
        forward[g, st] += 1
    reid = [r for r in out if r.get("same_record_in_26_7_as")]
    summary = {"by_group_status": {f"{g}|{s}": n for (g, s), n in sorted(tally.items())},
               "backward_26_7_ids_in_26_6": {f"{g}|{s}": n for (g, s), n in sorted(backward.items())},
               "forward_26_6_ids_in_26_7": {f"{g}|{s}": n for (g, s), n in sorted(forward.items())},
               "reidentified_same_source_record": {r["col_usage_id"]: r["same_record_in_26_7_as"] for r in reid},
               "nortaxa_source_metrics": meta, "sporely_exposure": exp,
               "note": "observed counts on a selected sample; no rate, interval or forecast is claimed"}
    a.out_json.write_text(json.dumps(summary, indent=1) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
