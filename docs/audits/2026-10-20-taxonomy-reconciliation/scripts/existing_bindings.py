#!/usr/bin/env python3
"""Review finding 6: re-evaluate the 16,424 existing name-based NorTaxa->COL
identity bindings (compiler `automatic_exact`: 14,291 strict + 2,133
missing-authorship) against the corrected rule set
deterministic_name_rank_authorship_v1 (revision 4: the same R1-R9 evaluator as new
candidates, via rules.py; nomenclatural status, uniqueness, external-id conflicts).

  python -B scripts/existing_bindings.py --workdb <workdb> --release <release copy> \
      --out-dir <out>

Writes existing_bindings_summary.json and existing_bindings_failing.jsonl.
"""
from __future__ import annotations

import argparse
import collections
import json
import sqlite3
import sys
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
import authorship as au  # noqa: E402
import rules  # noqa: E402

FIELDS = (("phylum", "phylum"), ("class", "class"), ("ord", "order"), ("family", "family"))
OWN = {"family": "family", "order": "ord", "class": "class", "phylum": "phylum"}


def norm(v):
    v = (v or "").strip()
    return "" if v.lower().startswith("incertae sedis") else v


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdb", required=True, type=Path)
    ap.add_argument("--release", required=True, type=Path)
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--evidence-dir", required=True, type=Path)
    a = ap.parse_args(argv)
    db = sqlite3.connect(f"file:{a.workdb}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    db.execute(f"attach database 'file:{a.release}?mode=ro' as r")
    taxa = {r["sid"]: dict(r) for r in db.execute("select * from taxa where src='col_xr'")}
    nt = {r["taxon_id"]: dict(r) for r in db.execute("select * from nt_raw")}
    nt_live = collections.defaultdict(list)
    for r in db.execute("select sid, nkey, rank from taxa where src='nortaxa'"):
        nt_live[(r["nkey"], r["rank"])].append(r["sid"])
    col_syn = collections.defaultdict(list)
    for r in db.execute("select id, parent_id, nkey, authorship, rank, status from col_raw "
                        "where status not in ('accepted','provisionally accepted')"):
        col_syn[r["nkey"]].append(dict(r))
    nt_syn = collections.defaultdict(list)
    for r in db.execute("select taxon_id, accepted_id, nkey, authorship, rank, status from nt_raw "
                        "where accepted_id <> '' and accepted_id <> taxon_id"):
        nt_syn[r["nkey"]].append(dict(r))
    cloud = {r[0] for r in db.execute("select sid from cloud_scope")}
    # rev 4: same evaluator as new candidates (R1-R9)
    col_by = {r["id"]: dict(r) for r in db.execute(
        "select id, alt_id, name_status, nkey, rank, status from col_raw where status like '%accepted'")}
    col_same = collections.Counter((t["nkey"], t["rank"]) for t in taxa.values())
    nt_valid_same = collections.Counter((r["nkey"], r["rank"]) for r in nt.values()
                                        if r["status"] == "valid" and r["kingdom"] == "Fungi")
    nt_bound = collections.Counter(r[0] for r in db.execute(
        "select sid from usages where src='nortaxa' and binding='alias' and bec in ("
        "'cross_source_automatic_exact_strict','cross_source_automatic_exact_missing_authorship',"
        "'manual_approved_exact','reviewed_supersession') and alias_reason <> 'synonym_of_accepted'"))
    legacy = collections.defaultdict(lambda: collections.defaultdict(set))
    with open(a.evidence_dir / "compiler" / "legacy_external_ids.jsonl") as fh:
        for line in fh:
            x = json.loads(line)
            if x.get("id_role") == "accepted" and x.get("resolved_via_nortaxa_taxon_id"):
                legacy[str(x["resolved_via_nortaxa_taxon_id"])][x["namespace"]].add(str(x["external_id"]))

    def col_alt(cid):
        out = collections.defaultdict(set)
        for tok in ((col_by.get(cid) or {}).get("alt_id") or "").split(","):
            tok = tok.strip()
            if tok.startswith("inat:"):
                out["inaturalist_taxon_id"].add(tok[5:])
            elif tok.startswith("dyntaxa:urn:lsid:dyntaxa.se:Taxon:"):
                out["artportalen_taxon_id"].add(tok.rsplit(":", 1)[1])
        return out

    rows = db.execute("""select u.sid, u.ident, u.name, u.nkey, u.authorship, u.rank, u.status, u.bec
        from usages u where u.src='nortaxa'
        and u.bec in ('cross_source_automatic_exact_strict','cross_source_automatic_exact_missing_authorship')""").fetchall()
    summary = collections.Counter()
    reasons = collections.Counter()
    by_class_rank = collections.Counter()
    pass_group = collections.Counter()
    failing = []
    for r in rows:
        c = taxa[r["sid"]]
        n = nt.get(r["ident"], {})
        cls = "strict" if r["bec"].endswith("strict") else "missing_authorship"
        auth = au.compare(r["authorship"], c["authorship"])
        agree, dis = [], []
        own = OWN.get(r["rank"])
        for nf, label in FIELDS:
            if nf == own:
                continue
            x, y = norm(n.get(nf)), norm(c[nf])
            if x and y:
                (agree if x == y else dis).append(label)
        blockers = []
        if r["nkey"] != c["nkey"] or r["rank"] != c["rank"]:
            blockers.append("name_or_rank_differs")                                   # R1
        if col_same[(c["nkey"], c["rank"])] != 1 or nt_valid_same[(r["nkey"], r["rank"])] != 1:
            blockers.append("not_one_to_one")                                         # R2
        if c["status"] != "accepted":
            blockers.append("col_provisionally_accepted")                             # R3
        if r["status"] != "valid":
            blockers.append("nortaxa_status_" + str(r["status"]))
        blockers.extend(rules.authorship_blockers(auth, agree))                       # R4
        blockers.extend(rules.status_blockers(n.get("nom_status"),
                                              (col_by.get(c["ident"]) or {}).get("name_status")))
        alt = col_alt(c["ident"])                                                     # R7
        for ns in ("inaturalist_taxon_id", "artportalen_taxon_id"):
            mine = legacy[r["ident"]].get(ns, set())
            if mine and alt.get(ns) and not (mine & alt[ns]):
                blockers.append("external_id_conflict:" + ns)
        if nt_bound[r["sid"]] > 1:                                                    # R8
            blockers.append("survivor_has_several_nortaxa_accepted_ids")
        if dis:
            blockers.append("classification_disagrees:" + "+".join(dis))
        if r["rank"] == "genus" and "family" not in agree:
            blockers.append("genus_family_not_confirmed")
        others = [s for s in nt_live[(r["nkey"], r["rank"])]]
        if others:
            blockers.append("nortaxa_has_another_live_concept_of_same_name_rank")
        if any(s["parent_id"] != c["ident"] and s["status"] != "misapplied"
               and s["rank"].lower() == r["rank"] and au.compatible(s["authorship"], r["authorship"])
               for s in col_syn[r["nkey"]]):
            blockers.append("col_synonym_elsewhere_same_authorship")
        if any(s["accepted_id"] != r["ident"] and s["rank"] == r["rank"]
               and au.compatible(s["authorship"], c["authorship"]) for s in nt_syn[r["nkey"]]):
            blockers.append("nortaxa_synonym_elsewhere_same_authorship")
        rank_group = ("species" if r["rank"] == "species" else
                      "infraspecific" if r["rank"] in ("subspecies", "variety", "form") else
                      "genus" if r["rank"] == "genus" else "higher")
        ok = not blockers
        summary[(cls, "pass" if ok else "fail")] += 1
        if ok:
            pass_group[("A0-equivalent" if auth in au.EXACT else "A1-equivalent", rank_group)] += 1
        by_class_rank[(cls, rank_group, "pass" if ok else "fail")] += 1
        for b in blockers:
            reasons[(cls, b.split(":")[0])] += 1
        if not ok:
            failing.append({"binding_class": cls, "col_sporely_taxon_id": r["sid"],
                            "col_usage_id": c["ident"], "nortaxa_taxon_id": r["ident"],
                            "scientific_name": r["name"], "rank": r["rank"],
                            "nortaxa_authorship": r["authorship"], "col_authorship": c["authorship"],
                            "authorship_class": auth, "classification_disagree": dis,
                            "col_in_cloud_scope": r["sid"] in cloud, "blockers": blockers})
    a.out_dir.mkdir(parents=True, exist_ok=True)
    failing.sort(key=lambda x: (x["binding_class"], x["rank"], x["scientific_name"]))
    with open(a.out_dir / "existing_bindings_failing.jsonl", "w") as f:
        for x in failing:
            f.write(json.dumps(x, ensure_ascii=False, sort_keys=True) + "\n")
    single = collections.Counter(tuple(x["blockers"])[0].split(":")[0] for x in failing if len(x["blockers"]) == 1)

    # Mutually exclusive primary reason (precedence, strongest first) -> sums to the
    # failing total; and "any" counts -> a binding counted under every reason it has.
    def cats(x):
        out = set()
        for b in x["blockers"]:
            if b.startswith("authorship_qualified") or b.startswith("authorship_unpublished") \
                    or b.startswith("authorship_malformed") or b.startswith("nomenclatural_status"):
                out.add("1_qualified_or_unpublished_authorship")
            elif b == "col_provisionally_accepted" or b.startswith("nortaxa_status_"):
                out.add("2_col_provisionally_accepted")
            elif b.startswith("col_synonym_elsewhere") or b.startswith("nortaxa_synonym_elsewhere") \
                    or b.startswith("external_id_conflict"):
                out.add("3_competing_placement")
            elif b in ("nortaxa_has_another_live_concept_of_same_name_rank", "not_one_to_one",
                       "survivor_has_several_nortaxa_accepted_ids", "name_or_rank_differs"):
                out.add("4_not_unique")
            elif b.startswith("classification_disagrees"):
                dis = set(x["classification_disagree"])
                if dis & {"class", "phylum"}:
                    out.add("5_class_or_phylum_disagrees")
                else:
                    out.add("8_order_or_family_only")
            elif b == "authorship_missing_without_classification_support":
                out.add("6_missing_authorship_unsupported")
            elif b == "genus_family_not_confirmed":
                out.add("7_genus_family_not_confirmed")
            else:
                out.add("9_other:" + b)
        return out
    primary = collections.Counter(min(cats(x)) for x in failing)
    anyc = collections.Counter(c for x in failing for c in cats(x))
    failing_ids = {x["nortaxa_taxon_id"] for x in failing}
    legacy_ids = {r["ident"] for r in rows}
    syn = collections.Counter()
    for acc, in db.execute("""select u.accepted_ident from usages u join taxa t on t.sid = u.sid
            where u.src='nortaxa' and u.bec='intra_source_synonym' and t.src='col_xr'"""):
        syn["on_failing_legacy_host" if acc in failing_ids else
            "on_passing_legacy_host" if acc in legacy_ids else "on_other_host"] += 1
    out = {
        "synonym_of_accepted_usages_bound_to_col_via_host": dict(syn),
        "bindings": len(rows),
        "by_class": {f"{k[0]}|{k[1]}": v for k, v in sorted(summary.items())},
        "by_class_rank": {"|".join(k): v for k, v in sorted(by_class_rank.items())},
        "blocker_counts": {f"{k[0]}|{k[1]}": v for k, v in sorted(reasons.items())},
        "single_blocker": dict(single.most_common()),
        "passing_by_ratification_group_rank": {"|".join(k): v for k, v in sorted(pass_group.items())},
        "primary_reason_exclusive": dict(sorted(primary.items())),
        "primary_reason_exclusive_total": sum(primary.values()),
        "any_reason_counts": dict(sorted(anyc.items())),
        "failing_in_cloud_scope": sum(1 for x in failing if x["col_in_cloud_scope"]),
    }
    (a.out_dir / "existing_bindings_summary.json").write_text(json.dumps(out, indent=1, sort_keys=True))
    print(json.dumps(out, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
