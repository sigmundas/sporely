#!/usr/bin/env python3
"""Copy classification outputs and data files into the audit directory.
Since revision 3 the candidate partitions and manifests/index.json are written
by groups.py (A0/A1/A2, validator format); this script no longer writes them.

Original description: write the candidate reconciliation manifests (proposals only; nothing is
approved or applied) from classify.py output.

  python -B scripts/export_manifests.py --in-dir <out> --dest <audit dir>

Writes <dest>/manifests/*.jsonl(.gz) and <dest>/manifests/index.json with a
members_sha256 per partition (sha256 of the sorted member key lines), so an
owner decision can name an exact, immutable member set.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import shutil
from pathlib import Path

PIN = {
    "content_release_id": "tax-2026.10.07-01",
    "sqlite_sha256": "e349e8b1d7cdbafa688621a71f9be278ac6e381853ff7fe2763d8b856b7724f9",
    "evidence_archive_sha256": "e2d1dfcaa261457fe1ae1392783c503ae56b833c7652a259056103012c089424",
    "source_archives": {
        "col_xr": {"source_release_id": "col_xr:2026-07-17-XR:2026-07-17",
                   "sha256": "397d701c8eb269bf78d6ac7b03149915b0d9e2a2c18694be2c91445b807814f9"},
        "nortaxa": {"source_release_id": "nortaxa:1.284:2026-07-17",
                    "sha256": "29c11c54d955dc44e4e5a38944dd7932989a256d1b173777579b9f33abd2fe22"},
    },
    "review_status": "proposal_not_approved",
    "rule_revision": "deterministic_name_rank_authorship_v1 rev2 (after review 2026-10-20)",
}

COMPACT = ["nortaxa_sporely_taxon_id", "col_sporely_taxon_id", "nortaxa_taxon_id", "col_usage_id",
           "scientific_name", "nortaxa_rank", "nortaxa_authorship", "col_authorship",
           "authorship_class", "classification_agree", "classification_disagree", "col_status",
           "published_synonymy", "inaturalist", "artportalen_vs_dyntaxa", "col_in_cloud_scope",
           "shared_nb_vernacular", "obs_nortaxa", "obs_col", "category", "b_subclass",
           "decisive_evidence", "proposed_treatment"]


def rank_group(r):
    if r["nortaxa_rank"] != r["col_rank"]:
        return "different_rank"
    rk = r["nortaxa_rank"]
    return ("species" if rk == "species" else "infraspecific" if rk in ("subspecies", "variety", "form")
            else "genus" if rk == "genus" else "higher")


def write_partition(path: Path, rows: list[dict], key) -> dict:
    lines = sorted(key(r) for r in rows)
    sha = hashlib.sha256(("\n".join(lines) + "\n").encode()).hexdigest() if lines else None
    with path.open("w") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n")
    return {"path": path.name, "member_count": len(rows), "members_sha256": sha,
            "member_key": "sorted lines '<retired sporely id>,<survivor sporely id>'"}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-dir", required=True, type=Path)
    ap.add_argument("--dest", required=True, type=Path)
    a = ap.parse_args(argv)
    m = a.dest / "manifests"
    d = a.dest / "data"
    m.mkdir(parents=True, exist_ok=True)
    d.mkdir(parents=True, exist_ok=True)
    pairs = [json.loads(l) for l in gzip.open(a.in_dir / "cross_source_pairs.jsonl.gz", "rt")]
    shutil.copyfile(a.in_dir / "cross_source_pairs.jsonl.gz", m / "cross-source-pairs.all.jsonl.gz")
    for name in ("col_internal_groups.jsonl", "nortaxa_internal_groups.jsonl"):
        shutil.copyfile(a.in_dir / name, m / name.replace("_", "-"))
    if (a.in_dir / "existing_bindings_failing.jsonl").exists():
        shutil.copyfile(a.in_dir / "existing_bindings_failing.jsonl", m / "existing-bindings-failing.jsonl")
    for name in ("summary.json", "coverage.json", "section2_counts.json", "transitions.json",
                 "existing_bindings_summary.json", "references.json", "artsobs_lookup_coverage.json",
                 "groups_summary.json", "transitions_v2_v3.json", "transitions_v3_v4.json",
                 "desktop_migration.json", "id-mapping-S1.json"):
        if (a.in_dir / name).exists():
            shutil.copyfile(a.in_dir / name, d / name)

    for p in sorted(m.iterdir()):
        print(p.name, p.stat().st_size)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
