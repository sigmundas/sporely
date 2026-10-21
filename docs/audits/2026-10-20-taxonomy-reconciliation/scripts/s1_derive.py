#!/usr/bin/env python3
"""Revision 5: independent re-derivation and freeze of the S1 candidate set
(A0 species + infraspecific).

Independent of classify.py / groups.py: it re-implements R1-R9 for species
and infraspecific ranks directly against the work DB, the release copy and
the desktop copies. Only the rule DEFINITIONS are shared: the comparator
(authorship.py) and the status lists (rules.py). It then compares its result
with manifests/validator-format/A0-{species,infraspecific}.manifest.json and
with data/id-mapping-S1.json, and runs the S1 independence checks.

Read-only over every input (mode=ro URIs). Writes only --out.

  python -B scripts/s1_derive.py --workdb <workdb> --release <release copy> \
      --desktop <mushrooms.db copy> --reference-db <reference_values.db copy> \
      --audit <audit dir> --out <audit>/data/s1-candidate.json
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import re
import sqlite3
import sys
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
import authorship as au  # noqa: E402
import rules  # noqa: E402

S1_RANKS = {"species", "subspecies", "variety", "form"}
EXACT = {"identical", "typography", "citation_stripped", "sanctioning"}
CLS = ("phylum", "class", "ord", "family")


def ro(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def blank(v):
    v = (v or "").strip()
    return "" if v.lower().startswith("incertae sedis") else v


def canonical(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    for x in ("workdb", "release", "desktop", "reference-db", "audit", "out"):
        ap.add_argument("--" + x, required=True, type=Path)
    a = ap.parse_args(argv)
    w, rel, desk, ref = ro(a.workdb), ro(a.release), ro(a.desktop), ro(a.reference_db)
    w.row_factory = sqlite3.Row

    taxa = [dict(r) for r in w.execute("select * from taxa")]
    by_key = collections.defaultdict(list)
    for t in taxa:
        by_key[(t["nkey"], t["rank"])].append(t)
    nt_raw = {r["taxon_id"]: dict(r) for r in w.execute("select * from nt_raw")}
    col_raw_acc, col_syn = {}, collections.defaultdict(list)
    for r in w.execute("select * from col_raw"):
        r = dict(r)
        if r["status"] in ("accepted", "provisionally accepted"):
            col_raw_acc[r["id"]] = r
        else:
            col_syn[r["nkey"]].append(r)
    nt_syn = collections.defaultdict(list)
    for r in nt_raw.values():
        if r["accepted_id"] and r["accepted_id"] != r["taxon_id"]:
            nt_syn[r["nkey"]].append(r)
    ext = collections.defaultdict(lambda: collections.defaultdict(set))
    for sid, src, eid, role in rel.execute(
            "select taxon_id, source_system, external_id, id_role from taxon_external_id_min "
            "where id_role in ('accepted','publishing')"):
        ext[sid][src].add(int(eid))

    def col_alt(cid):
        out = collections.defaultdict(set)
        for tok in ((col_raw_acc.get(cid) or {}).get("alt_id") or "").split(","):
            tok = tok.strip()
            if tok.startswith("inat:") and tok[5:].isdigit():
                out["inat"].add(int(tok[5:]))
            elif tok.startswith("dyntaxa:urn:lsid:dyntaxa.se:Taxon:") and tok.rsplit(":", 1)[1].isdigit():
                out["dyntaxa"].add(int(tok.rsplit(":", 1)[1]))
        return out

    # ---- R1-R8 for species / infraspecific
    cand, reject = [], collections.Counter()
    for (k, rank), mem in by_key.items():
        if rank not in S1_RANKS:
            continue
        nts = [t for t in mem if t["src"] == "nortaxa"]
        cols = [t for t in mem if t["src"] == "col_xr"]
        if not nts or not cols:
            continue
        if len(nts) != 1 or len(cols) != 1:          # R2 strict one-to-one (author-filtered never in S1)
            reject["R2_not_one_to_one"] += len(nts) * len(cols)
            continue
        n, c = nts[0], cols[0]
        why = []
        if n["status"] != "valid" or c["status"] != "accepted":
            why.append("R3_status")
        why += ["R3_" + s for s in rules.status_blockers(nt_raw.get(n["ident"], {}).get("nom_status"),
                                                         col_raw_acc.get(c["ident"], {}).get("name_status"))]
        if (n["kingdom"] or "Fungi") != "Fungi" or (c["kingdom"] or "Fungi") != "Fungi":
            why.append("kingdom")
        cls = au.compare(n["authorship"], c["authorship"])
        if cls not in EXACT:
            why.append("R4_" + cls)
        agree = [f for f in CLS if blank(n[f]) and blank(c[f]) and blank(n[f]) == blank(c[f])]
        if any(blank(n[f]) and blank(c[f]) and blank(n[f]) != blank(c[f]) for f in CLS):
            why.append("R5_classification")
        if re.search(r"\bsensu\b|\bss\.|\bauct\b", n["authorship"] or "", re.I):
            why.append("sensu")
        comp_col = [s for s in col_syn[k] if s["parent_id"] != c["ident"] and s["status"] != "misapplied"
                    and (s["rank"] or "").lower() == rank and au.compatible(s["authorship"], n["authorship"])]
        comp_nt = [s for s in nt_syn[k] if s["accepted_id"] != n["ident"] and s["rank"] == rank
                   and s["status"] != "misapplied" and au.compatible(s["authorship"], c["authorship"])]
        if comp_col or comp_nt:
            why.append("R6_competing_synonym")
        ce, ne, alt = ext[c["sid"]], ext[n["sid"]], col_alt(c["ident"])
        for x, y, tag in ((ne.get("inaturalist", set()), ce.get("inaturalist", set()) | alt["inat"], "inat"),
                          (ne.get("artportalen", set()), ce.get("artportalen", set()) | alt["dyntaxa"], "artportalen")):
            if x and y and not (x & y):
                why.append("R7_" + tag)
        if ce.get("artsdatabanken"):
            why.append("R8_survivor_has_nortaxa_id")
        if why:
            reject[why[0]] += 1
            continue
        cand.append({"retired": n["sid"], "survivor": c["sid"], "nortaxa_taxon_id": n["ident"],
                     "col_usage_id": c["ident"], "name": n["name"], "rank": rank,
                     "nortaxa_authorship": n["authorship"], "col_authorship": c["authorship"],
                     "authorship_class": cls, "classification_agree": agree})

    # ---- A2 dependencies (any live reference -> not S1)
    desk_obs = collections.Counter(dict(w.execute("select sid, n from obs_desktop")))
    for (e, n) in desk.execute("select taxon_identity_external_id, count(*) from observations "
                               "where taxon_identity_namespace='sporely_taxon_id' group by 1"):
        if str(e or "").isdigit():
            desk_obs[int(e)] += n
    for (t, n) in desk.execute("select o.sporely_taxon_id, count(*) from observation_reference_uses u "
                               "join observations o on o.id=u.observation_id group by 1"):
        if t is not None:
            desk_obs[t] += n
    for (t, n) in ref.execute("select taxon_id, count(*) from reference_taxon_treatments group by 1"):
        if str(t or "").strip().isdigit():
            desk_obs[int(t)] += n
    cloud = collections.Counter()
    for sid, n in w.execute("select sid, n from obs_cloud where sid is not null"):
        cloud[sid] += n
    dep = [x for x in cand if desk_obs.get(x["retired"]) or cloud.get(x["retired"])]
    cand = [x for x in cand if not (desk_obs.get(x["retired"]) or cloud.get(x["retired"]))]

    # ---- R9 per-stage closure: children under BOTH the shipped parent map
    # (release taxon_min) and the corrected NorTaxa parent map (nt_raw parent
    # -> NorTaxa concept). A retiree with a child outside S1 leaves S1.
    nt_sid = {t["ident"]: t["sid"] for t in taxa if t["src"] == "nortaxa"}
    for ident, sid in w.execute("select ident, sid from usages where src='nortaxa'"):
        nt_sid.setdefault(ident, sid)
    shipped_children, fixed_children = collections.defaultdict(set), collections.defaultdict(set)
    for c_, p in rel.execute("select taxon_id, parent_taxon_id from taxon_min where parent_taxon_id is not null"):
        shipped_children[p].add(c_)
    for t in taxa:
        if t["src"] == "nortaxa" and t["parent_id"]:
            p = nt_sid.get(t["parent_id"])
            if p is not None and p != t["sid"]:
                fixed_children[p].add(t["sid"])
    moved = []
    while True:
        s1 = {x["retired"] for x in cand}
        blocked = [x for x in cand if (shipped_children[x["retired"]] | fixed_children[x["retired"]]) - s1]
        if not blocked:
            break
        moved += blocked
        cand = [x for x in cand if x not in blocked]
    cand.sort(key=lambda x: (x["retired"], x["survivor"]))
    pairs = [(x["retired"], x["survivor"]) for x in cand]

    # ---- compare with the rev-4 manifests and mapping
    mdir = a.audit / "manifests"
    idx = json.loads((mdir / "index.json").read_text())["partitions"]
    man, man_rows = set(), {}
    pins = {}
    for part in ("A0-species", "A0-infraspecific"):
        p = mdir / idx[part]["path"]
        doc = json.loads(p.read_text())
        recomputed = sha(canonical({"columns": doc["columns"], "members": doc["members"]}))
        pins[part] = {"file_sha256": sha(p.read_bytes()), "members_sha256": doc["members_sha256"],
                      "members_sha256_recomputed": recomputed, "member_count": len(doc["members"]),
                      "index_agrees": idx[part]["file_sha256"] == sha(p.read_bytes())
                      and idx[part]["members_sha256"] == recomputed}
        for row in doc["members"]:
            r = dict(zip(doc["columns"], row))
            man.add((r["nortaxa_sporely_taxon_id"], r["col_sporely_taxon_id"]))
            man_rows[r["nortaxa_sporely_taxon_id"]] = r
    field_mismatch = [x["retired"] for x in cand if x["retired"] in man_rows and (
        man_rows[x["retired"]]["nortaxa_taxon_id"] != x["nortaxa_taxon_id"]
        or man_rows[x["retired"]]["col_usage_id"] != x["col_usage_id"]
        or man_rows[x["retired"]]["authorship_class"] != x["authorship_class"])]
    mp = a.audit / "data" / "id-mapping-S1.json"
    mapping = {int(k): v for k, v in json.loads(mp.read_text())["mapping"].items()}

    # ---- S1 independence checks (no dependency on A1/A2/genus/legacy decisions)
    all_ret = {}
    for part, info in idx.items():
        p = mdir / info["path"]
        if info["validator_format"]:
            doc = json.loads(p.read_text())
            for row in doc["members"]:
                r = dict(zip(doc["columns"], row))
                all_ret[r["nortaxa_sporely_taxon_id"]] = (part, r["col_sporely_taxon_id"])
        else:
            for line in p.open():
                r = json.loads(line)
                all_ret[r["retired_sporely_taxon_id"]] = (part, r["survivor_sporely_taxon_id"])
    ledger = {r[0]: (r[1], r[2], r[3]) for r in w.execute(
        "select superseded_sid, cur_src, cur_ns, cur_ident from supersessions")}
    ledger_current = set()
    for s_, src, ns, ident in w.execute("select superseded_sid, cur_src, cur_ns, cur_ident from supersessions"):
        hit = w.execute("select sid from usages where src=? and ns=? and ident=?", (src, ns, ident)).fetchone()
        if hit:
            ledger_current.add(hit[0])
    s1r, s1s = {p[0] for p in pairs}, {p[1] for p in pairs}
    bindings = collections.Counter({"survivor_hosts_legacy_automatic_exact_binding": 0})
    for sid, binding, bec in w.execute("select sid, binding, bec from usages where src='nortaxa'"):
        if sid in s1s and binding == "alias" and bec.startswith("cross_source_automatic_exact"):
            bindings["survivor_hosts_legacy_automatic_exact_binding"] += 1
    other_pairs = collections.Counter({"retiree_has_cross_rank_col_namesake(category C)": 0})
    taxa_by_sid = {t["sid"]: t for t in taxa}
    for sid in s1r:
        t = taxa_by_sid[sid]
        for (k2, r2), mem in ((kk, vv) for kk, vv in by_key.items() if kk[0] == t["nkey"] and kk[1] != t["rank"]):
            if any(m["src"] == "col_xr" for m in mem):
                other_pairs["retiree_has_cross_rank_col_namesake(category C)"] += 1
    in_scope = {r[0] for r in w.execute("select sid from cloud_scope")}
    indep = {
        "survivor_is_retired_in_any_other_partition": sorted(s for s in s1s if s in all_ret and all_ret[s][0] not in ("A0-species", "A0-infraspecific")),
        "retiree_in_another_partition": sorted(r for r in s1r if all_ret.get(r, ("A0-species",))[0] not in ("A0-species", "A0-infraspecific")),
        "retiree_already_superseded_in_shipped_ledger": sorted(s1r & set(ledger)),
        "survivor_already_superseded_in_shipped_ledger(chain)": sorted(s1s & set(ledger)),
        "retiree_is_current_concept_of_shipped_supersession": sorted(s1r & ledger_current),
        "survivor_is_current_concept_of_shipped_supersession": len(s1s & ledger_current),
        "survivor_targeted_by_more_than_one_s1_retiree": [s for s, n in collections.Counter(p[1] for p in pairs).items() if n > 1],
        "survivor_absent_from_release": sorted(s1s - {r[0] for r in rel.execute("select taxon_id from taxon_min")}),
        "retiree_children_shipped_parent_map": sum(len(shipped_children[r]) for r in s1r),
        "retiree_children_corrected_parent_map": sum(len(fixed_children[r]) for r in s1r),
        "retiree_children_outside_s1": sum(len((shipped_children[r] | fixed_children[r]) - s1r) for r in s1r),
        "reparent_ops_in_s1": 0,
        "retirees_in_cloud_scope": len(s1r & in_scope),
        "survivors_in_cloud_scope": len(s1s & in_scope),
        **dict(bindings), **dict(other_pairs),
    }
    # comparator generalisation probes (review-4 #5) on S1 authorship strings
    probe = collections.Counter()
    for x in cand:
        for s in (x["nortaxa_authorship"], x["col_authorship"]):
            s = s or ""
            if ";" in s:
                probe["semicolon"] += 1
            if re.search(r"\bsens\.|\bs\. ?str\.|\bs\. ?l(at)?\.|\bsensu\b", s):
                probe["sense_qualifier"] += 1
            if re.search(r"\bet al\.?", s):
                probe["et_al"] += 1
    status_seen = collections.Counter()
    for x in cand:
        status_seen["nt:" + (nt_raw.get(x["nortaxa_taxon_id"], {}).get("nom_status") or "")] += 1
        status_seen["col:" + (col_raw_acc.get(x["col_usage_id"], {}).get("name_status") or "")] += 1

    out = {
        "scope": "S1 = A0 species + infraspecific; provisional until independently verified and approved",
        "derivation": "independent (s1_derive.py); rule definitions from authorship.py + rules.py",
        "counts": {"derived": len(pairs), "by_rank": dict(collections.Counter(x["rank"] for x in cand)),
                   "manifest": len(man), "mapping": len(mapping),
                   "dropped_for_live_dependency": len(dep), "dropped_for_children": len(moved),
                   "first_rejection_reason": dict(reject)},
        "equal": {"derived_vs_manifest_pairs": set(pairs) == man,
                  "derived_vs_mapping": dict(pairs) == mapping,
                  "manifest_field_mismatches": field_mismatch,
                  "only_in_derived": sorted(set(pairs) - man), "only_in_manifest": sorted(man - set(pairs))},
        "pins": {**pins,
                 "s1_pairs_sha256": sha(("\n".join(f"{r},{s}" for r, s in pairs) + "\n").encode()),
                 "s1_members_sha256": sha(canonical(cand)),
                 "id_mapping_S1_file_sha256": sha(mp.read_bytes())},
        "independence": indep,
        "comparator_probes_on_s1_strings": dict(probe),
        "nomenclatural_status_values_on_s1_members": dict(status_seen),
        "members": cand,
    }
    a.out.write_text(json.dumps(out, ensure_ascii=False, indent=0, sort_keys=True) + "\n")
    print(json.dumps({k: v for k, v in out.items() if k != "members"}, indent=1, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
