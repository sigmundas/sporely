#!/usr/bin/env python3
"""Category movement between the first audit run (v1, before review) and the
corrected run (v2): old->new transition matrix by rank group, with the rule
responsible for each move.

  python -B scripts/transitions.py --old <pairs_v1.jsonl.gz> --new <out>/cross_source_pairs.jsonl.gz \
      --out <out>/transitions.json

v1 is the cross-source-pairs manifest as first delivered (members_sha256 of
A-species ace310a5...); keep a copy before regenerating.
"""
from __future__ import annotations

import argparse
import collections
import gzip
import json
from pathlib import Path


def rank_group(r):
    if r["nortaxa_rank"] != r["col_rank"]:
        return "different_rank"
    rk = r["nortaxa_rank"]
    return ("species" if rk == "species" else "infraspecific" if rk in ("subspecies", "variety", "form")
            else "genus" if rk == "genus" else "higher")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", required=True, type=Path)
    ap.add_argument("--new", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    a = ap.parse_args(argv)
    key = lambda r: (r["nortaxa_sporely_taxon_id"], r["col_sporely_taxon_id"])
    old = {key(r): r for r in map(json.loads, gzip.open(a.old, "rt"))}
    new = {key(r): r for r in map(json.loads, gzip.open(a.new, "rt"))}
    assert old.keys() == new.keys()
    matrix = collections.Counter()
    why = collections.Counter()
    examples = collections.defaultdict(list)
    for k, n in new.items():
        o = old[k]
        g = rank_group(n)
        matrix[(g, o["category"], n["category"])] += 1
        if o["category"] != n["category"]:
            if o["authorship_class"] != n["authorship_class"]:
                rule = f"authorship {o['authorship_class']} -> {n['authorship_class']}"
            else:
                rule = "; ".join(n["decisive_evidence"])[:120]
            why[(g, o["category"], n["category"], rule)] += 1
            if len(examples[(o["category"], n["category"], rule)]) < 3:
                examples[(o["category"], n["category"], rule)].append(
                    f"{n['scientific_name']} {n['nortaxa_sporely_taxon_id']}/{n['col_sporely_taxon_id']}: "
                    f"{n['nortaxa_authorship']} || {n['col_authorship']}")
    out = {
        "matrix": {"|".join(k): v for k, v in sorted(matrix.items())},
        "moves_by_rule": {"|".join(k): v for k, v in sorted(why.items(), key=lambda kv: -kv[1])},
        "examples": {"|".join(k): v for k, v in sorted(examples.items())},
    }
    a.out.write_text(json.dumps(out, indent=1, ensure_ascii=False))
    for k, v in out["matrix"].items():
        g, o, n = k.split("|")
        if o != n:
            print("MOVE", k, v)
    for k, v in list(out["moves_by_rule"].items()):
        print(v, k)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
