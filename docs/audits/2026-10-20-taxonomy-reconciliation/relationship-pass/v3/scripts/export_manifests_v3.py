#!/usr/bin/env python3
"""Write the pass-3 relationship groups as candidate manifests in the existing
validator format (as ../../../scripts/groups.py does for S1), with their own
index and pins. Approves nothing.

One manifest per group / pragmatic sub-group:
  rel3-verified
  rel3-pragmatic-compatible-identical
  rel3-pragmatic-compatible-icn
  rel3-pragmatic-compatible-author-standard
  rel3-pragmatic-compatible-held-new-entrants
  rel3-pragmatic-missing-authorship

A pair the unmodified validator would refuse is excluded and listed in
data/manifest_exclusions.csv, never passed by weakening validation:
  * one_to_one is not True (validator: "not one-to-one");
  * a NorTaxa or COL Sporely id that occurs in more than one candidate pair
    across these manifests (not one-to-one within the proposal).

  python -B scripts/export_manifests_v3.py --data data --out manifests \
      --s1-index ../../manifests/index.json
"""
from __future__ import annotations

import argparse
import collections
import csv
import hashlib
import json
import sys
from pathlib import Path

sys.dont_write_bytecode = True
V3 = Path(__file__).resolve().parents[1]
AUDIT = V3.parents[1]
REPO = AUDIT.parents[2]
REL = "docs/audits/2026-10-20-taxonomy-reconciliation/relationship-pass/v3"

COLUMNS = ["nortaxa_taxon_id", "nortaxa_sporely_taxon_id", "col_usage_id", "col_sporely_taxon_id",
           "one_to_one", "canonical_scientific_name", "rank", "nortaxa_authorship", "col_authorship",
           "accepted_author_cmp", "group", "subclass", "reason", "provenance_class",
           "checklistbank_source_record", "placement_differs", "s1_package_partition"]
GROUPS = [
    ("rel3-verified", "verified", None),
    ("rel3-pragmatic-compatible-identical", "pragmatic_compatible", "identical"),
    ("rel3-pragmatic-compatible-icn", "pragmatic_compatible", "icn_compatible"),
    ("rel3-pragmatic-compatible-author-standard", "pragmatic_compatible", "author_standard_candidate"),
    ("rel3-pragmatic-compatible-held-new-entrants", "pragmatic_compatible", "verified_rules_new_entrant_held"),
    ("rel3-pragmatic-missing-authorship", "pragmatic_missing_authorship", None),
]


def canonical(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--s1-index", required=True, type=Path)
    a = ap.parse_args(argv)
    a.out.mkdir(parents=True, exist_ok=True)
    s1 = json.loads(a.s1_index.read_text())
    s1_part = {}
    for k, v in s1["partitions"].items():
        if v["validator_format"]:
            d = json.loads((a.s1_index.parent / v["path"]).read_text())
            for m in d["members"]:
                s1_part[dict(zip(d["columns"], m))["nortaxa_sporely_taxon_id"]] = k
    pins = {k: s1["pins"][k] for k in ("bridge_emission_policy", "cloud_scope_policy", "release", "source_archives")}
    for name, rel in (("comparator", "scripts/authors_v3.py"), ("relationship_rules", "scripts/relate_v3.py"),
                      ("checklistbank_source_records", "data/checklistbank_source_records.csv")):
        pins[name] = {"path": f"{REL}/{rel}", "sha256": sha(V3 / rel)}
    pins["checklistbank_release"] = {"dataset_key": 315834, "alias": "COL26.7 XR", "doi": "10.48580/dgykv",
                                     "endpoint": "/dataset/315834/nameusage/{id}/source (cached 2026-10-21)"}
    rows = {}
    for g in ("verified", "pragmatic_compatible", "pragmatic_missing_authorship"):
        rows[g] = [json.loads(l) for l in (a.data / f"{g}.jsonl").open(encoding="utf-8")]
    cand = [r for g in rows.values() for r in g]
    n_by = collections.Counter(r["nortaxa_sporely_taxon_id"] for r in cand)
    c_by = collections.Counter(r["col_sporely_taxon_id"] for r in cand)
    excluded = []
    for r in cand:
        why = []
        if r["one_to_one"] is not True:
            why.append("not_one_to_one(validator_refuses)")
        if n_by[r["nortaxa_sporely_taxon_id"]] > 1:
            why.append("nortaxa_sporely_id_in_several_candidate_pairs")
        if c_by[r["col_sporely_taxon_id"]] > 1:
            why.append("col_sporely_id_in_several_candidate_pairs")
        if why:
            excluded.append((r, why))
    ex_keys = {(r["nortaxa_taxon_id"], r["col_usage_id"]) for r, _ in excluded}
    index = {"pins": pins, "review_status": "proposal_not_approved",
             "rule_revision": "relationship pass 3 (after relationship-pass/v2/review-codex.md)",
             "precedence": "disjoint by construction (one group per pair)",
             "note": "Alternative grouping of the 2,805 not-in-S1 species pairs; members also occur in the S1 "
                     "package's A1/A2/B candidate partitions (column s1_package_partition). Never approve both "
                     "groupings for the same member. S1 (A0) members do not occur here.",
             "partitions": {}}
    for part, outcome, subclass in GROUPS:
        xs = [r for r in rows[outcome] if (subclass is None or r["subclass"] == subclass)
              and (r["nortaxa_taxon_id"], r["col_usage_id"]) not in ex_keys]
        xs.sort(key=lambda r: (r["nortaxa_sporely_taxon_id"], r["col_sporely_taxon_id"]))
        members = [[r["nortaxa_taxon_id"], r["nortaxa_sporely_taxon_id"], r["col_usage_id"], r["col_sporely_taxon_id"],
                    True, r["scientific_name"], r["rank"], r["nortaxa_authorship"], r["col_authorship"],
                    r["accepted_author_cmp"], outcome, r["subclass"], r["reason"], r["provenance_class"],
                    r["checklistbank_source_record"],
                    [f for f in r["flags"] if f.startswith("placement_differs")],
                    s1_part.get(r["nortaxa_sporely_taxon_id"], "")] for r in xs]
        doc = {"format": "sporely-taxonomy-reconciliation-candidate-manifest-v1",
               "approval_mode": "batch_by_file_sha256", "review_status": "needs_review",
               "evidence_class": f"relationship_pass_v3:{outcome}" + (f":{subclass}" if subclass else ""),
               "population": f"relationship pass 3 candidates, partition {part}",
               "pins": pins, "columns": COLUMNS, "member_count": len(members),
               "members_sha256": hashlib.sha256(canonical({"columns": COLUMNS, "members": members})).hexdigest(),
               "members": members,
               "note": "Candidate only. Approves nothing. checklistbank_source_record is traceable origin, not "
                       f"independent corroboration. Generated by {REL}/scripts/export_manifests_v3.py."}
        path = a.out / f"{part}.manifest.json"
        path.write_text(json.dumps(doc, ensure_ascii=False, indent=1, sort_keys=True) + "\n", encoding="utf-8")
        index["partitions"][part] = {"path": path.name, "validator_format": True, "member_count": len(members),
                                     "members_sha256": doc["members_sha256"], "file_sha256": sha(path)}
    index["excluded"] = {"count": len(excluded), "path": "../data/manifest_exclusions.csv"}
    (a.out / "index.json").write_text(json.dumps(index, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    with (a.data / "manifest_exclusions.csv").open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["outcome", "subclass", "scientific_name", "nortaxa_taxon_id", "nortaxa_sporely_taxon_id",
                    "col_usage_id", "col_sporely_taxon_id", "one_to_one", "excluded_because"])
        for r, why in sorted(excluded, key=lambda x: (x[0]["col_sporely_taxon_id"], x[0]["nortaxa_sporely_taxon_id"])):
            w.writerow([r["outcome"], r["subclass"], r["scientific_name"], r["nortaxa_taxon_id"],
                        r["nortaxa_sporely_taxon_id"], r["col_usage_id"], r["col_sporely_taxon_id"],
                        r["one_to_one"], ";".join(why)])
    print(json.dumps({k: v["member_count"] for k, v in index["partitions"].items()} | {"excluded": len(excluded)}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
