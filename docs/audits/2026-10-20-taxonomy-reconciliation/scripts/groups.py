#!/usr/bin/env python3
"""Revision 3: NorTaxa parent repair (isolated), structural dependencies,
the disjoint executable groups A0 / A1 / A2, and candidate manifests in the
existing validator's format.

Parent repair is an AUDIT REIMPLEMENTATION: it imports the real
build_sqlite_candidate._iter_source_usage_source_index and
_resolve_parent_sporely_id from the repository (read-only, no bytecode),
confirms the shipped behaviour (every NorTaxa parent -> None), and resolves
the parent reference again under the corrected key namespace
(nortaxa_parent_name_usage_id -> nortaxa_taxon_id). Nothing in the
repository is modified.

Group precedence, disjoint: A2 > A1 > A0.
  A2  structurally complicated: a surviving child concept would have to be
      re-parented (parent repair), or the retiree has desktop observations,
      desktop/cloud reference uses, cloud references in the 2026-10-20
      snapshot, it is one-to-one only after authorship filtering (fails the
      validator's one_to_one), or it is a within-source (COL-internal)
      supersession, which the validator has no manifest kind for.
  A1  abbreviation-dependent or authorship-missing match (no standard-form
      reference available: needs expert validation).
  A0  identical / typography / citation_stripped / sanctioning, none of the above.

  python -B scripts/groups.py --repo <worktree> --workdb <workdb> --release <release copy> \
     --evidence-dir <extracted evidence> --desktop <mushrooms.db copy> \
     --reference-db <reference_values.db copy> --pairs <out>/cross_source_pairs.jsonl.gz \
     --col-internal <out>/col_internal_groups.jsonl --pairs-v2 <pairs_v2.jsonl.gz> \
     --index-v2 <index_v2.json> --out-dir <out> --dest <audit dir>
"""
from __future__ import annotations

import argparse
import collections
import gzip
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

sys.dont_write_bytecode = True

PINS = {
    "bridge_emission_policy": {"path": "database/taxonomy/policies/mapping_policy.yml",
                               "sha256": "a5a329848c45fcd030165a51ad1193944dffe7573ddb2a9c69441f0d76ae4afb"},
    "cloud_scope_policy": {"path": "database/taxonomy/policies/global-macrofungi-scope.yml",
                           "scope_predicate_id": "global_macrofungi_policy_v1",
                           "sha256": "e4e796286df93b5372264c702c824eea746f680b3b4e8d6467621c99e5b64ea6"},
    "release": {"content_release_id": "tax-2026.10.07-01",
                "gz_artifact": "database/reference_data/generated/taxonomy_v2/tax-2026.10.07-01.sqlite3.gz",
                "gz_sha256": "bff40f0d5d70ec2be47fe415dfd4cfc623a7841553f8a7d928ed801117ca21c5",
                "sqlite_sha256": "e349e8b1d7cdbafa688621a71f9be278ac6e381853ff7fe2763d8b856b7724f9"},
    "source_archives": {
        "col_xr": {"sha256": "397d701c8eb269bf78d6ac7b03149915b0d9e2a2c18694be2c91445b807814f9",
                   "source_release_id": "col_xr:2026-07-17-XR:2026-07-17"},
        "nortaxa": {"sha256": "29c11c54d955dc44e4e5a38944dd7932989a256d1b173777579b9f33abd2fe22",
                    "source_release_id": "nortaxa:1.284:2026-07-17"}},
}
COLUMNS = ["nortaxa_taxon_id", "nortaxa_sporely_taxon_id", "col_usage_id", "col_sporely_taxon_id",
           "one_to_one", "canonical_scientific_name", "rank", "nortaxa_authorship", "col_authorship",
           "authorship_class", "classification_agree", "group", "group_reasons",
           "reparent_child_count", "col_in_cloud_scope"]
EXACT = ("identical", "typography", "citation_stripped", "sanctioning")
A1_CLASSES = ("abbreviation", "missing_one", "missing_both")


def canonical(value) -> bytes:  # == audit_stage0_coverage._canonical
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def rank_group(rank):
    return ("species" if rank == "species" else "infraspecific" if rank in ("subspecies", "variety", "form")
            else "genus" if rank == "genus" else "higher")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    for a in ("repo", "workdb", "release", "evidence-dir", "desktop", "reference-db", "pairs",
              "col-internal", "pairs-v2", "index-v2", "out-dir", "dest"):
        ap.add_argument("--" + a, required=True, type=Path)
    a = ap.parse_args(argv)
    sys.path.insert(0, str(a.repo / "database/taxonomy/scripts"))
    import build_sqlite_candidate as bsc  # noqa: E402

    work = sqlite3.connect(f"file:{a.workdb}?mode=ro", uri=True)
    rel = sqlite3.connect(f"file:{a.release}?mode=ro", uri=True)
    desk = sqlite3.connect(f"file:{a.desktop}?mode=ro", uri=True)
    ref = sqlite3.connect(f"file:{a.reference_db}?mode=ro", uri=True)

    # ---------------------------------------------------------- parent repair
    index = bsc._iter_source_usage_source_index(a.evidence_dir / "compiler" / "source_usages.jsonl")
    parent = {}
    for sid, p in rel.execute("select taxon_id, parent_taxon_id from taxon_min where parent_taxon_id is not null"):
        parent[sid] = p
    nt_parent_cat = collections.Counter()
    shipped_hits = 0
    for sid, ns, pid in work.execute("select sid, parent_ns, parent_id from taxa where src='nortaxa'"):
        row = {"canonical_source_code": "nortaxa", "parent_reference_resolution": "resolved",
               "parent_name_usage_id": {"namespace": ns, "value": pid} if pid else None}
        if bsc._resolve_parent_sporely_id(row, index) is not None:
            shipped_hits += 1
        if not pid:
            nt_parent_cat["no_parent_reference_root"] += 1
            continue
        fixed = index.get(("nortaxa", "nortaxa_taxon_id", pid))
        if fixed is None:
            nt_parent_cat["parent_reference_unbound"] += 1
            continue
        if fixed == sid:
            nt_parent_cat["self_reference_dropped"] += 1
            continue
        parent[sid] = fixed
        nt_parent_cat["parent_resolved"] += 1
    src_of = dict(rel.execute("select taxon_id, source_system from taxon_min"))
    nt_parent_split = collections.Counter(
        src_of.get(parent[s]) for s, src in src_of.items() if src == "nortaxa" and s in parent)
    children = collections.defaultdict(list)
    for c, p in parent.items():
        children[p].append(c)

    # ---------------------------------------------------------- candidates
    pairs = [json.loads(l) for l in gzip.open(a.pairs, "rt")]
    colint = [json.loads(l) for l in open(a.col_internal)]
    cand = []  # (kind, record)
    for r in pairs:
        if r["category"] == "A" or r.get("b_subclass") == "placement_only":
            cand.append(("cross", r))
    for g in colint:
        if g["category"] == "A":
            surv = g["proposed_survivor"]
            sm = next(m for m in g["members"] if m["sporely_taxon_id"] == surv)
            for m in g["members"]:
                if m["sporely_taxon_id"] != surv:
                    cand.append(("colint", {"retired": m["sporely_taxon_id"], "retired_col": m["col_usage_id"],
                                            "survivor": surv, "survivor_col": sm["col_usage_id"],
                                            "scientific_name": g["scientific_name"], "rank": g["rank"],
                                            "authorship_class": _auth(m["authorship"], sm["authorship"]),
                                            "category": "A"}))
    retire = {}
    for kind, r in cand:
        if kind == "cross":
            retire[r["nortaxa_sporely_taxon_id"]] = r["col_sporely_taxon_id"]
        else:
            retire[r["retired"]] = r["survivor"]

    # ---------------------------------------------------------- dependencies
    desk_obs = collections.Counter(dict(work.execute("select sid, n from obs_desktop")))
    desk_ident = collections.Counter()
    for ext, n in desk.execute("""select taxon_identity_external_id, count(*) from observations
            where taxon_identity_namespace='sporely_taxon_id' group by 1"""):
        if str(ext or "").isdigit():
            desk_ident[int(ext)] += n
    desk_uses = collections.Counter(dict(desk.execute("""select o.sporely_taxon_id, count(*)
            from observation_reference_uses u join observations o on o.id=u.observation_id group by 1""")))
    ref_treat = collections.Counter()
    for t, n in ref.execute("select taxon_id, count(*) from reference_taxon_treatments group by 1"):
        if str(t or "").strip().isdigit():
            ref_treat[int(t)] += n
    cloud = collections.Counter()
    for k, sid, n in work.execute("select kind, sid, n from obs_cloud where sid is not null"):
        cloud[sid] += n
    redlist = collections.Counter(dict(rel.execute(
        "select taxon_id, count(*) from taxon_redlist_min where taxon_id is not null group by 1")))
    nt_ident = dict(work.execute("select sid, ident from taxa where src='nortaxa'"))
    src_parents = collections.Counter(r[0] for r in work.execute(
        "select parent_id from nt_raw where status in ('valid','accepted') and parent_id<>'' and parent_id<>taxon_id"))

    ops = []
    def deps(old):
        d = {}
        for name, counter in (("desktop_observations", desk_obs), ("desktop_identity_sporely_namespace", desk_ident),
                              ("desktop_reference_uses", desk_uses), ("reference_treatments", ref_treat),
                              ("cloud_snapshot_references", cloud)):
            if counter.get(old):
                d[name] = counter[old]
        return d

    rows = []
    for kind, r in cand:
        if kind == "cross":
            old, new = r["nortaxa_sporely_taxon_id"], r["col_sporely_taxon_id"]
            d = deps(old)
            if not r["one_to_one"]:
                d["author_filtered_one_to_one"] = True
            auth, rank, cat = r["authorship_class"], r["nortaxa_rank"], r["category"]
        else:
            old, new = r["retired"], r["survivor"]
            d = deps(old)
            d["within_source_manifest_kind"] = True
            auth, rank, cat = r["authorship_class"], r["rank"], "A"
        info = {"redlist_rows": redlist.get(old, 0),
                "source_parent_of_valid_usages": src_parents.get(nt_ident.get(old, ""), 0)}
        if cat != "A":
            grp = "B-placement"
        elif d:
            grp = "A2"
        elif auth in A1_CLASSES:
            grp = "A1"
        else:
            assert auth in EXACT, auth
            grp = "A0"
        rows.append({"kind": kind, "group": grp, "rank_group": rank_group(rank), "retired": old,
                     "survivor": new, "authorship_class": auth, "dependencies": d, "info": info, "rec": r})

    # ---------------------------------------------------------- per-stage dependency closure (rev 4)
    # Re-parenting is computed against what is retired BY THE END OF EACH
    # RELEASE STAGE, never against the union of all proposed retirements
    # (Codex review-3 finding 2). Stages: S1 A0 species+infraspecific,
    # S2 A0 genus+higher, S3 A1, S4 A2, S5 B-placement. A member of S1-S3 that
    # would need a re-parenting operation in its stage moves to A2 (fixpoint).
    def stage_of(x):
        if x["group"] == "A0":
            return "S1" if x["rank_group"] in ("species", "infraspecific") else "S2"
        return {"A1": "S3", "A2": "S4", "B-placement": "S5"}[x["group"]]
    moved_to_a2 = collections.Counter()
    cum: set[int] = set()
    for stage in ("S1", "S2", "S3"):
        while True:
            members = [x for x in rows if stage_of(x) == stage]
            in_stage = cum | {x["retired"] for x in members}
            blocked = [x for x in members
                       if any(c not in in_stage for c in children.get(x["retired"], []))]
            if not blocked:
                break
            for x in blocked:
                moved_to_a2[(stage, x["rank_group"])] += 1
                x["group"] = "A2"
        cum = in_stage
    stage_ops = collections.Counter()
    for stage in ("S1", "S2", "S3", "S4", "S5"):
        members = [x for x in rows if stage_of(x) == stage]
        in_stage = cum | {x["retired"] for x in members} if stage in ("S4", "S5") else \
            ({y["retired"] for y in rows if stage_of(y) in ("S1", "S2", "S3")[: ("S1", "S2", "S3").index(stage) + 1]})
        for x in members:
            live = sorted(c for c in children.get(x["retired"], []) if c not in in_stage)
            if live:
                x["dependencies"]["reparent_children"] = len(live)
                for c in live:
                    ops.append({"stage": stage, "child_sporely_taxon_id": c, "child_source": src_of.get(c),
                                "old_parent_retiree": x["retired"], "new_parent_survivor": x["survivor"]})
                    stage_ops[stage] += 1
        if stage in ("S4", "S5"):
            cum = in_stage
    stage_closure = {"stage_ops": dict(stage_ops),
                     "moved_to_A2_by_stage_rank": {"|".join(k): v for k, v in sorted(moved_to_a2.items())},
                     "stage_sizes": dict(collections.Counter(stage_of(x) for x in rows))}

    # ---------------------------------------------------------- movement from rev-2 A
    key = lambda x: (x["nortaxa_sporely_taxon_id"], x["col_sporely_taxon_id"])
    v2 = {key(r): r for r in map(json.loads, gzip.open(a.pairs_v2, "rt"))}
    now = {(x["rec"]["nortaxa_sporely_taxon_id"], x["rec"]["col_sporely_taxon_id"]): x
           for x in rows if x["kind"] == "cross"}
    v3_all = {key(r): r for r in pairs}
    movement = collections.Counter()
    for k, r in v2.items():
        if r["category"] != "A":
            continue
        g = now[k]["group"] if k in now and now[k]["group"] != "B-placement" else "not_A:" + v3_all[k]["category"]
        movement[(rank_group(r["nortaxa_rank"]), "A", g)] += 1
    for k, x in now.items():
        if x["group"] != "B-placement" and v2[k]["category"] != "A":
            movement[(x["rank_group"], "v2:" + v2[k]["category"], x["group"])] += 1
    movement[("col-internal", "A", "A2")] = sum(1 for x in rows if x["kind"] == "colint")

    # ---------------------------------------------------------- source-level parent counts (review-2 #3)
    v2_index = json.loads(a.index_v2.read_text())
    review_check = {}
    for part in ("A-species", "A-genus", "A-higher"):
        info = v2_index["partitions"].get(part)
        path = a.dest / "manifests" / info["path"] if info else Path("/nonexistent")
        ids = []
        if path.exists():
            ids = [json.loads(l)["nortaxa_sporely_taxon_id"] for l in open(path)]
        review_check[part] = sum(1 for s in ids if src_parents.get(nt_ident.get(s, ""), 0))

    # ---------------------------------------------------------- outputs
    m = a.dest / "manifests"
    for old in m.glob("batch-*.jsonl"):
        old.unlink()  # rev-2 partitions are superseded by the rev-3 groups
    vdir = m / "validator-format"
    vdir.mkdir(exist_ok=True)
    for old in vdir.glob("*.json"):
        old.unlink()
    index = {"pins": {**PINS, "comparator": {"path": "docs/audits/2026-10-20-taxonomy-reconciliation/scripts/authorship.py",
                                              "sha256": hashlib.sha256((Path(__file__).parent / "authorship.py").read_bytes()).hexdigest()}},
             "review_status": "proposal_not_approved",
             "rule_revision": "deterministic_name_rank_authorship_v1 rev3 (after review-2)",
             "precedence": "A2 > A1 > A0 (disjoint)", "partitions": {}}
    by_part = collections.defaultdict(list)
    for x in rows:
        if x["kind"] == "colint":
            by_part["A2-col-internal"].append(x)
        elif x["group"] == "B-placement":
            by_part["B-placement"].append(x)
        elif "author_filtered_one_to_one" in x["dependencies"]:
            by_part["A2-author-filtered"].append(x)
        else:
            by_part[f"{x['group']}-{x['rank_group']}"].append(x)
    for part, xs in sorted(by_part.items()):
        xs.sort(key=lambda x: (x["retired"], x["survivor"]))
        validator = part not in ("A2-col-internal", "A2-author-filtered") and \
            all(x["kind"] == "cross" and x["rec"]["one_to_one"] for x in xs)
        if validator:
            members = []
            for x in xs:
                r = x["rec"]
                members.append([r["nortaxa_taxon_id"], r["nortaxa_sporely_taxon_id"], r["col_usage_id"],
                                r["col_sporely_taxon_id"], True, r["scientific_name"], r["nortaxa_rank"],
                                r["nortaxa_authorship"], r["col_authorship"], r["authorship_class"],
                                r["classification_agree"], x["group"], sorted(x["dependencies"]),
                                x["dependencies"].get("reparent_children", 0), r["col_in_cloud_scope"]])
            doc = {"format": "sporely-taxonomy-reconciliation-candidate-manifest-v1",
                   "approval_mode": "batch_by_file_sha256", "review_status": "needs_review",
                   "evidence_class": "deterministic_name_rank_authorship_v1_rev3",
                   "population": f"reconciliation candidates, partition {part}",
                   "pins": index["pins"], "columns": COLUMNS, "member_count": len(members),
                   "members_sha256": hashlib.sha256(canonical({"columns": COLUMNS, "members": members})).hexdigest(),
                   "members": members,
                   "note": "Candidate only. Approves nothing. Generated by docs/audits/2026-10-20-"
                           "taxonomy-reconciliation/scripts/groups.py; pinned to the release and archives above."}
            path = vdir / f"{part}.manifest.json"
            path.write_text(json.dumps(doc, ensure_ascii=False, indent=1, sort_keys=True) + "\n")
            index["partitions"][part] = {
                "path": f"validator-format/{path.name}", "validator_format": True,
                "member_count": len(members), "members_sha256": doc["members_sha256"],
                "file_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        else:
            path = m / f"{part}.jsonl"
            with path.open("w") as f:
                for x in xs:
                    f.write(json.dumps({"retired_sporely_taxon_id": x["retired"],
                                        "survivor_sporely_taxon_id": x["survivor"], "group": x["group"],
                                        "dependencies": x["dependencies"], "info": x["info"],
                                        "record": x["rec"]}, ensure_ascii=False, sort_keys=True) + "\n")
            lines = sorted(f"{x['retired']},{x['survivor']}" for x in xs)
            index["partitions"][part] = {
                "path": path.name, "validator_format": False, "member_count": len(xs),
                "members_sha256": hashlib.sha256(("\n".join(lines) + "\n").encode()).hexdigest(),
                "required_validator_change": (
                    "verify_supersession_manifest_approvals requires one_to_one is True and a "
                    "NorTaxa->COL member; these members are one-to-one only after authorship "
                    "filtering" if part == "A2-author-filtered" else
                    "a within-source (COL->COL) manifest kind and record shape do not exist")}
    (m / "index.json").write_text(json.dumps(index, indent=1, sort_keys=True) + "\n")
    with (m / "reparent-ops.jsonl").open("w") as f:
        for op in sorted(ops, key=lambda o: (o["old_parent_retiree"], o["child_sporely_taxon_id"])):
            f.write(json.dumps(op, sort_keys=True) + "\n")

    comp = collections.Counter((x["group"], x["rank_group"] if x["kind"] == "cross" else "col-internal") for x in rows)
    reasons = collections.Counter((x["rank_group"] if x["kind"] == "cross" else "col-internal", r)
                                  for x in rows if x["group"] == "A2" for r in x["dependencies"])
    a2_sole = collections.Counter((x["rank_group"] if x["kind"] == "cross" else "col-internal",
                                   next(iter(x["dependencies"])))
                                  for x in rows if x["group"] == "A2" and len(x["dependencies"]) == 1)
    ops_by_group = collections.Counter()
    gmap = {x["retired"]: x for x in rows}
    for op in ops:
        x = gmap[op["old_parent_retiree"]]
        ops_by_group[(x["group"], x["rank_group"], op["child_source"])] += 1
    out = {
        "parent_repair": {
            "shipped_builder_resolved_nortaxa_parents": shipped_hits,
            "nortaxa_parent_reference": dict(nt_parent_cat),
            "nortaxa_parent_resolved_by_parent_source": {str(k): v for k, v in nt_parent_split.items()},
            "retirees_that_are_parents_in_candidate": sum(1 for r in retire if any(c not in retire for c in children.get(r, []))),
            "reparent_operations": len(ops),
            "reparent_operations_by_group_rank_childsource": {"|".join(map(str, k)): v for k, v in sorted(ops_by_group.items())},
            "review2_check_rev2_partitions_retirees_parent_of_valid_source_usages": review_check,
        },
        "composition": {"|".join(k): v for k, v in sorted(comp.items())},
        "a2_reasons_any": {"|".join(map(str, k)): v for k, v in sorted(reasons.items())},
        "a2_reasons_sole": {"|".join(map(str, k)): v for k, v in sorted(a2_sole.items())},
        "movement_from_previous_revision_A": {"|".join(k): v for k, v in sorted(movement.items())},
        "stage_dependency_closure": stage_closure,
        "redlist_rows_by_group": {g: sum(x["info"]["redlist_rows"] for x in rows if x["group"] == g)
                                  for g in ("A0", "A1", "A2", "B-placement")},
    }
    (a.out_dir / "groups_summary.json").write_text(json.dumps(out, indent=1, sort_keys=True))
    print(json.dumps(out, indent=1))
    return 0


def _auth(x, y):
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import authorship as au
    return au.compare(x, y)


if __name__ == "__main__":
    raise SystemExit(main())
