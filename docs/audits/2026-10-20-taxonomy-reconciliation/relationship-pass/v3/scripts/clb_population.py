#!/usr/bin/env python3
"""Classify the GBIF research's population provenance (605 source-2030 COL usages
sharing a canonical name with a NorTaxa-only anchor) under the evidence class
'checklistbank_source_record'. Read-only, no network.

Sub-classes:
  exact_copy:col_accepted            the COL usage IS this NorTaxa taxonID, accepted
  exact_copy:col_provisionally_accepted   ... provisionally accepted
  exact_copy:col_synonym             the COL usage IS this NorTaxa taxonID, but COL made it a
                                     synonym of another accepted usage (named, with its source)
  name_match:copy_of_other_record    same name, but COL copied a different NorTaxa taxonID
Plus: rank agreement, and the pass-3 outcome when the anchor is one of the 2,805 pairs.

  python -B scripts/clb_population.py --population <gbif>/data/population_provenance.csv \
      --workdb <scratch>/rev5/workdb.sqlite3 --data data --out data/checklistbank_source_record_population.csv
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
    for x in ("population", "workdb", "data", "out"):
        ap.add_argument("--" + x, required=True, type=Path)
    a = ap.parse_args(argv)
    w = sqlite3.connect(f"file:{a.workdb}?mode=ro&immutable=1", uri=True)
    col = {r[0]: r for r in w.execute("select id, status, parent_id, source_id, name, authorship from col_raw")}
    p3 = {}
    for g in ("verified", "pragmatic_compatible", "pragmatic_missing_authorship", "unresolved"):
        for l in (a.data / f"{g}.jsonl").open(encoding="utf-8"):
            r = json.loads(l)
            p3[r["nortaxa_taxon_id"]] = (r["outcome"], r["subclass"], r["col_usage_id"])
    out, tally = [], collections.Counter()
    for p in csv.DictReader(a.population.open(encoding="utf-8-sig")):
        exact = p["same_nortaxa_id"] == "1"
        syn = p["col_status"] == "synonym"
        sub = ("exact_copy:col_synonym" if syn else "exact_copy:col_" + p["col_status"].replace(" ", "_")) \
            if exact else "name_match:copy_of_other_record"
        acc = col.get(p["col_usage_id"])
        tgt = col.get(acc[2]) if acc and syn else None
        o = p3.get(p["nortaxa_taxon_id"])
        row = {"nortaxa_taxon_id": p["nortaxa_taxon_id"], "nortaxa_name": p["nortaxa_name"],
               "nortaxa_authorship": p["nortaxa_authorship"], "col_usage_id": p["col_usage_id"],
               "col_status": p["col_status"], "col_authorship": p["col_authorship"],
               "source_record": f"{p['clb_source_dataset']}:{p['clb_source_id']}", "subclass": sub,
               "same_rank": p["same_rank"],
               "col_accepted_target": f"{tgt[0]} {tgt[4]} {tgt[5]} [source {tgt[3]}]" if tgt else "",
               "col_accepted_target_source": tgt[3] if tgt else "",
               "in_2805_pairs": bool(o), "pass3_outcome": o[0] if o else "", "pass3_subclass": o[1] if o else "",
               "pass3_col_usage_id": o[2] if o else "",
               "pair_col_is_this_copy": bool(o) and o[2] == p["col_usage_id"],
               "provenance_class": "copied_source_provenance", "verified_eligible": False}
        out.append(row)
        tally[(sub, "same_rank" if p["same_rank"] == "1" else "rank_differs",
               "in_pairs" if o else "not_in_pairs")] += 1
    cols = list(out[0])
    with a.out.open("w", newline="", encoding="utf-8-sig") as f:
        cw = csv.DictWriter(f, fieldnames=cols)
        cw.writeheader()
        cw.writerows(out)
    tgt_src = collections.Counter(r["col_accepted_target_source"] for r in out if r["subclass"] == "exact_copy:col_synonym")
    s = {"rows": len(out), "by_subclass": dict(collections.Counter(r["subclass"] for r in out)),
         "by_subclass_rank_pairs": {"|".join(k): v for k, v in sorted(tally.items())},
         "distinct_nortaxa_with_exact_copy": len({r["nortaxa_taxon_id"] for r in out if r["subclass"].startswith("exact")}),
         "synonym_copy_targets_by_source": dict(tgt_src),
         "exact_accepted_in_pairs_by_pass3": dict(collections.Counter(
             f"{r['pass3_outcome']}:{r['pass3_subclass']}" for r in out
             if r["subclass"].startswith("exact_copy:col_") and r["subclass"] != "exact_copy:col_synonym"
             and r["in_2805_pairs"]))}
    print(json.dumps(s, indent=1))
    (a.out.with_suffix(".summary.json")).write_text(json.dumps(s, indent=1) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
