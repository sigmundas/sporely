#!/usr/bin/env python3
"""List every pair whose outcome or subclass changed between the reviewed pass-3
outputs (--old, a copy of data/ taken before the pass-3 correction) and the
corrected outputs (--new). Writes --out (UTF-8 BOM CSV, header even if empty).

  python -B scripts/compare_pass3.py --old <copy of reviewed data> --new data --out data/changes_from_pass3.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

sys.dont_write_bytecode = True
GROUPS = ("verified", "pragmatic_compatible", "pragmatic_missing_authorship", "unresolved")


def load(d: Path) -> dict:
    out = {}
    for g in GROUPS:
        for l in (d / f"{g}.jsonl").open(encoding="utf-8"):
            r = json.loads(l)
            out[(r["nortaxa_taxon_id"], r["col_usage_id"])] = r
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    for x in ("old", "new", "out"):
        ap.add_argument("--" + x, required=True, type=Path)
    a = ap.parse_args(argv)
    old, new = load(a.old), load(a.new)
    assert set(old) == set(new), "pair sets differ"
    rows = []
    for k in sorted(old, key=lambda k: (new[k]["outcome"], new[k]["subclass"], new[k]["scientific_name"])):
        o, n = old[k], new[k]
        if (o["outcome"], o["subclass"]) == (n["outcome"], n["subclass"]):
            continue
        why = []
        if o["accepted_author_cmp"] != n["accepted_author_cmp"]:
            why.append(f"accepted_author_cmp {o['accepted_author_cmp']} -> {n['accepted_author_cmp']}")
        if o["basionym_author_cmp"] != n["basionym_author_cmp"]:
            why.append(f"basionym_author_cmp {o['basionym_author_cmp']} -> {n['basionym_author_cmp']}")
        for f in ("contradictions", "ambiguities"):
            gone = sorted(set(o[f]) - set(n[f]))
            added = sorted(set(n[f]) - set(o[f]))
            if gone:
                why.append(f"{f} removed: " + " | ".join(gone))
            if added:
                why.append(f"{f} added: " + " | ".join(added))
        rows.append([n["scientific_name"], n["nortaxa_taxon_id"], n["nortaxa_sporely_taxon_id"], n["col_usage_id"],
                     n["col_sporely_taxon_id"], n["nortaxa_authorship"], n["col_authorship"],
                     o["outcome"], o["subclass"], o["reason"], n["outcome"], n["subclass"], n["reason"], "; ".join(why)])
    with a.out.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["scientific_name", "nortaxa_taxon_id", "nortaxa_sporely_taxon_id", "col_usage_id",
                    "col_sporely_taxon_id", "nortaxa_authorship", "col_authorship", "old_outcome", "old_subclass",
                    "old_reason", "new_outcome", "new_subclass", "new_reason", "change_reason"])
        w.writerows(rows)
    print(len(rows), "changed pairs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
