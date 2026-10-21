#!/usr/bin/env python3
"""Classify every duplicate-concept candidate in tax-2026.10.07-01 into
A (deterministic batch) / B (review) / C (keep distinct) / D (insufficient data).

Inputs (read-only): the work DB from build_workdb.py (+ compute_cloud_scope.py,
optionally cloud_usage_readonly.py --load), the release copy (ATTACHed
read-only), the repository worktree (Stage 2 Group-B manifests and
cross_reference_evidence.grade).

Outputs (to --out-dir):
  cross_source_pairs.jsonl.gz   one row per NorTaxa/COL name-equal pair (any rank)
  col_internal_groups.jsonl     one row per COL same-name+rank group
  nortaxa_internal_groups.jsonl one row per NorTaxa same-name group (any rank)
  summary.json                  every count quoted in the report

  python -B scripts/classify.py --repo <worktree> --workdb <workdb> \
      --release <release copy> --out-dir <dir>
"""
from __future__ import annotations

import argparse
import collections
import glob
import gzip
import json
import re
import sqlite3
import sys
from pathlib import Path

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import authorship as au  # noqa: E402
import rules  # noqa: E402

SPECIES_RANKS = {"species", "subspecies", "variety", "form"}
ACCEPTED_COL = {"accepted", "provisionally accepted"}
CLASS_FIELDS = ("phylum", "class", "ord", "family")


def norm_cls(v: str) -> str:
    v = (v or "").strip()
    return "" if v.lower().startswith("incertae sedis") else v


RANK_FIELD = {"family": "family", "order": "ord", "class": "class", "phylum": "phylum"}


def cls_compare(a: dict, b: dict, fields=CLASS_FIELDS) -> tuple[list, list]:
    """Agreeing / disagreeing populated higher-classification fields. A
    concept's own rank field (a family's 'family') is its name, not
    evidence, and is skipped."""
    agree, disagree = [], []
    own = RANK_FIELD.get(a["rank"])
    for f in fields:
        if f == own:
            continue
        x, y = norm_cls(a[f]), norm_cls(b[f])
        if x and y:
            (agree if x == y else disagree).append(f)
    return agree, disagree


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True, type=Path)
    ap.add_argument("--workdb", required=True, type=Path)
    ap.add_argument("--release", required=True, type=Path)
    ap.add_argument("--out-dir", required=True, type=Path)
    a = ap.parse_args(argv)
    a.out_dir.mkdir(parents=True, exist_ok=True)
    sys.path.insert(0, str(a.repo / "database/taxonomy/scripts"))
    import cross_reference_evidence as xre  # noqa: E402

    db = sqlite3.connect(f"file:{a.workdb}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    db.execute(f"attach database 'file:{a.release}?mode=ro' as r")

    taxa = {r["sid"]: dict(r) for r in db.execute("select * from taxa")}
    by_nkey = collections.defaultdict(list)
    for t in taxa.values():
        by_nkey[t["nkey"]].append(t)
    cloud = {r[0] for r in db.execute("select sid from cloud_scope")}
    desk = dict(db.execute("select sid, n from obs_desktop").fetchall())
    have_cloud_obs = db.execute(
        "select count(*) from sqlite_master where name='obs_cloud'").fetchone()[0]
    csel, cres = collections.Counter(), collections.Counter()
    if have_cloud_obs:
        for k, sid, n in db.execute("select kind, sid, n from obs_cloud where sid is not null"):
            (csel if k == "selected" else cres)[sid] += n
    children = collections.Counter(r[0] for r in db.execute(
        "select parent_taxon_id from r.taxon_min where parent_taxon_id is not null"))

    # external ids per concept (release)
    ext = collections.defaultdict(lambda: collections.defaultdict(set))
    for sid, src, eid, role in db.execute(
            "select taxon_id, source_system, external_id, id_role from r.taxon_external_id_min"):
        if role in ("accepted", "publishing"):
            ext[sid][src].add(int(eid))
    nb = collections.defaultdict(set)
    nb_pref = {}
    for sid, name, pref in db.execute(
            "select taxon_id, vernacular_name, is_preferred_name from r.vernacular_min "
            "where language_code in ('nb','no')"):
        nb[sid].add(name.casefold())
        if pref:
            nb_pref[sid] = name
    for sid, name in db.execute(
            "select taxon_id, min(vernacular_name) from r.vernacular_min "
            "where language_code in ('nb','no') group by 1"):
        nb_pref.setdefault(sid, name)

    # COL raw: accepted rows, alt ids, synonyms by parent, synonyms by nkey
    col_acc, col_syn_by_parent, col_syn_by_nkey = {}, collections.defaultdict(list), collections.defaultdict(list)
    for r in db.execute("select id, alt_id, source_id, parent_id, status, name, nkey, authorship, rank, merged, name_status from col_raw"):
        r = dict(r)
        if r["status"] in ACCEPTED_COL:
            col_acc[r["id"]] = r
        else:
            col_syn_by_parent[r["parent_id"]].append(r)
            col_syn_by_nkey[r["nkey"]].append(r)

    def col_alt(cid: str) -> dict:
        out = collections.defaultdict(set)
        row = col_acc.get(cid)
        if not row or not row["alt_id"]:
            return out
        for tok in row["alt_id"].split(","):
            tok = tok.strip()
            if tok.startswith("inat:"):
                v = tok[5:]
                if v.isdigit():
                    out["inaturalist"].add(int(v))
            elif tok.startswith("dyntaxa:urn:lsid:dyntaxa.se:Taxon:"):
                v = tok.rsplit(":", 1)[1]
                if v.isdigit():
                    out["dyntaxa_taxon"].add(int(v))
        return out

    # NorTaxa raw
    nt_rows = [dict(r) for r in db.execute("select * from nt_raw")]
    nt_by_tid = {r["taxon_id"]: r for r in nt_rows}
    nt_syn_by_acc = collections.defaultdict(list)
    nt_syn_by_nkey = collections.defaultdict(list)
    for r in nt_rows:
        if r["accepted_id"] and r["accepted_id"] != r["taxon_id"]:
            nt_syn_by_acc[r["accepted_id"]].append(r)
            nt_syn_by_nkey[r["nkey"]].append(r)

    def nt_concept(tid):
        acc = nt_by_tid.get(tid)
        if not acc:
            return None
        return {"accepted": xre._name_key(acc["name"], acc["authorship"]),
                "synonyms": {xre._name_key(s["name"], s["authorship"]) for s in nt_syn_by_acc[tid]},
                "scientific_name": acc["name"], "authorship": acc["authorship"]}

    def col_concept(cid):
        acc = col_acc.get(cid)
        if not acc:
            return None
        return {"accepted": xre._name_key(acc["name"], acc["authorship"]),
                "synonyms": {xre._name_key(s["name"], s["authorship"]) for s in col_syn_by_parent[cid]},
                "scientific_name": acc["name"], "authorship": acc["authorship"]}

    # Stage 2 Group-B manifests (tax-2026.09.26-02): pair -> manifest leaf
    stage2 = {}
    for f in sorted(glob.glob(str(a.repo / "database/taxonomy/evidence/taxonomy-v3/stage2/group-b-*.manifest.json"))):
        m = json.loads(Path(f).read_text())
        cols = m["columns"]
        i_n, i_c = cols.index("nortaxa_taxon_id"), cols.index("col_usage_id")
        leaf = Path(f).name.replace("group-b-", "").replace(".manifest.json", "")
        for mem in m["members"]:
            key = (str(mem[i_n]), str(mem[i_c]))
            # keep the most specific (longest) leaf name
            if key not in stage2 or len(leaf) > len(stage2[key]):
                stage2[key] = leaf

    def obs(sid):
        return {"desktop": desk.get(sid, 0), "cloud_selected": csel.get(sid, 0),
                "cloud_resolved": cres.get(sid, 0)}

    # ------------------------------------------------ cross-source pairs
    pairs = []
    for key, members in by_nkey.items():
        nts = [t for t in members if t["src"] == "nortaxa"]
        cols_ = [t for t in members if t["src"] == "col_xr"]
        if not nts or not cols_:
            continue
        for n in nts:
            for c in cols_:
                pairs.append((n, c))

    def classify_pair(n, c):
        rec = {
            "nortaxa_sporely_taxon_id": n["sid"], "col_sporely_taxon_id": c["sid"],
            "nortaxa_taxon_id": n["ident"], "col_usage_id": c["ident"],
            "scientific_name": n["name"], "nortaxa_rank": n["rank"], "col_rank": c["rank"],
            "nortaxa_authorship": n["authorship"], "col_authorship": c["authorship"],
            "col_status": c["status"],
        }
        same_rank_cols = [t for t in by_nkey[n["nkey"]] if t["src"] == "col_xr" and t["rank"] == n["rank"]]
        same_rank_nts = [t for t in by_nkey[n["nkey"]] if t["src"] == "nortaxa" and t["rank"] == c["rank"]]
        rec["col_twins_same_rank"] = len(same_rank_cols)
        rec["nortaxa_twins_same_rank"] = len(same_rank_nts)
        rec["one_to_one"] = len(same_rank_cols) == 1 and len(same_rank_nts) == 1
        # Authorship-disambiguated uniqueness: when one side holds several
        # same-name concepts, only those whose authorship is compatible with the
        # other side's are candidates (Stereum durum Burt vs Lloyd).
        ok = lambda x, y: au.compare(x, y) in au.COMPATIBLE
        compat_cols = [t for t in same_rank_cols if ok(t["authorship"], n["authorship"])]
        compat_nts = [t for t in same_rank_nts if ok(t["authorship"], c["authorship"])]
        rec["one_to_one_by_authorship"] = (not rec["one_to_one"]) and ok(n["authorship"], c["authorship"]) \
            and len(compat_cols) == 1 and len(compat_nts) == 1
        substantive = lambda x: len(au._person(au._authors(au._split_parts(au._clean(x))[1])[0][-1][-1])[1]) >= 3 \
            if au._authors(au._split_parts(au._clean(x))[1])[0] else False
        rec["authorship_identifies_other_partner"] = (not ok(n["authorship"], c["authorship"])) and \
            substantive(n["authorship"]) and substantive(c["authorship"]) and \
            ((len(compat_nts) == 1 and compat_nts[0]["sid"] != n["sid"]) or
             (len(compat_cols) == 1 and compat_cols[0]["sid"] != c["sid"]))
        rec["col_concepts_same_name_any_rank"] = sum(1 for t in by_nkey[n["nkey"]] if t["src"] == "col_xr")
        auth = au.compare(n["authorship"], c["authorship"])
        rec["authorship_class"] = auth
        agree, disagree = cls_compare(n, c)
        rec["classification_agree"] = agree
        rec["classification_disagree"] = disagree
        # competing placements of the same name
        comp_col = [s for s in col_syn_by_nkey[n["nkey"]] if s["parent_id"] != c["ident"]
                    and s["status"] != "misapplied" and s["rank"].lower() == n["rank"]]
        comp_col_n = [s for s in comp_col if au.compatible(s["authorship"], n["authorship"])]
        comp_col_n_only = [s for s in comp_col_n if not au.compatible(s["authorship"], c["authorship"])]
        comp_nt = [s for s in nt_syn_by_nkey[n["nkey"]] if s["accepted_id"] != n["ident"]
                   and s["rank"] == n["rank"] and s["status"] not in ("misapplied",)]
        comp_nt_c = [s for s in comp_nt if au.compatible(s["authorship"], c["authorship"])]
        rec["col_synonym_elsewhere_compatible_with_nortaxa"] = [
            {"col_usage_id": s["id"], "authorship": s["authorship"], "status": s["status"],
             "accepted_col_usage_id": s["parent_id"]} for s in comp_col_n[:5]]
        rec["nortaxa_synonym_elsewhere_compatible_with_col"] = [
            {"nortaxa_taxon_id": s["taxon_id"], "authorship": s["authorship"],
             "accepted_nortaxa_taxon_id": s["accepted_id"]} for s in comp_nt_c[:5]]
        # external id corroboration
        n_ext, c_ext, c_alt = ext[n["sid"]], ext[c["sid"]], col_alt(c["ident"])
        inat_n = n_ext.get("inaturalist", set())
        inat_c = c_ext.get("inaturalist", set()) | c_alt.get("inaturalist", set())
        ap_n = n_ext.get("artportalen", set())
        ap_c = c_ext.get("artportalen", set()) | c_alt.get("dyntaxa_taxon", set())
        def rel(x, y):
            if not x or not y:
                return "absent"
            return "agree" if x & y else "conflict"
        rec["inaturalist"] = rel(inat_n, inat_c)
        rec["artportalen_vs_dyntaxa"] = rel(ap_n, ap_c)
        rec["col_record_from_nortaxa_source"] = (col_acc.get(c["ident"]) or {}).get("source_id") == "2030"
        g = xre.grade(nt_concept(n["ident"]), col_concept(c["ident"]))
        rec["published_synonymy"] = g["evidence_class"]
        rec["shared_synonym_count"] = g.get("shared_synonym_count", 0)
        rec["stage2_manifest_0926"] = stage2.get((n["ident"], c["ident"]))
        rec["col_in_cloud_scope"] = c["sid"] in cloud
        rec["nortaxa_nb"] = sorted(nb[n["sid"]])[:3]
        rec["col_nb"] = sorted(nb[c["sid"]])[:3]
        rec["shared_nb_vernacular"] = bool(nb[n["sid"]] & nb[c["sid"]])
        rec["obs_nortaxa"] = obs(n["sid"])
        rec["obs_col"] = obs(c["sid"])
        rec["col_children"] = children.get(c["sid"], 0)

        # ---------------- decision
        reasons = []
        if n["rank"] != c["rank"]:
            cat = "C"
            reasons.append(f"rank differs ({n['rank']} vs {c['rank']}): different concepts by construction")
        elif rec["authorship_identifies_other_partner"]:
            cat = "C"
            reasons.append("same-name homonyms: authorship conflicts here, and the other side has "
                           "exactly one same-name concept whose authorship matches; that pair is the "
                           "candidate, this pair names two different names")
        elif re.search(r"\bsensu\b|\bss\.|\bauct\b", n["authorship"], re.I) and comp_col:
            cat = "C"
            reasons.append("NorTaxa uses the name in a 'sensu' (misapplied) sense and COL places that "
                           "usage under another accepted concept: different taxon")
        else:
            blockers = []
            if comp_col_n_only:
                blockers.append("col_places_nortaxa_authorship_under_other_concept")
            if not (rec["one_to_one"] or rec["one_to_one_by_authorship"]):
                blockers.append("not_one_to_one")
            if c["status"] != "accepted":
                blockers.append("col_provisionally_accepted")
            if n["status"] != "valid":
                blockers.append("nortaxa_status_" + n["status"])
            if (n["kingdom"] or "Fungi") != "Fungi" or (c["kingdom"] or "Fungi") != "Fungi":
                blockers.append("kingdom_not_fungi")
            if disagree:
                blockers.append("classification_disagrees:" + "+".join(disagree))
            blockers.extend(rules.authorship_blockers(auth, agree))
            sb = rules.status_blockers((nt_by_tid.get(n["ident"]) or {}).get("nom_status"),
                                       (col_acc.get(c["ident"]) or {}).get("name_status"))
            blockers.extend(sb)
            rec["nomenclatural_status_blockers"] = sb
            if comp_col_n and not comp_col_n_only:
                blockers.append("col_synonym_elsewhere_same_authorship")
            if comp_nt_c:
                blockers.append("nortaxa_synonym_elsewhere_same_authorship")
            if c_ext.get("artsdatabanken"):
                # the COL concept already carries a NorTaxa accepted taxonID
                # (another binding); a second one would make publishing ambiguous
                blockers.append("survivor_already_has_nortaxa_accepted_id")
            if rec["inaturalist"] == "conflict":
                blockers.append("inaturalist_id_conflict")
            if rec["artportalen_vs_dyntaxa"] == "conflict":
                blockers.append("artportalen_dyntaxa_id_conflict")
            if n["rank"] in SPECIES_RANKS:
                pass
            elif n["rank"] == "genus":
                if "family" not in agree:
                    blockers.append("genus_family_not_confirmed")
            elif n["rank"] in ("family", "subfamily", "order", "suborder"):
                if len(agree) < 1:
                    blockers.append("higher_rank_classification_not_confirmed")
            # ranks class and above: one-to-one at rank is the only guard;
            # their classification above is (sub)phylum/kingdom only.
            insufficient = (auth == "missing_both" and len(agree) < 2) or \
                (not any(norm_cls(n[f]) for f in CLASS_FIELDS) and auth not in au.COMPATIBLE)
            if not blockers:
                cat = "A"
                reasons.append(f"same name+rank, one-to-one, authorship {auth}, classification "
                               f"agrees on {'+'.join(agree) or 'none populated'}, no competing "
                               f"placement or identifier conflict")
            elif insufficient and blockers == ["authorship_missing_without_classification_support"]:
                cat = "D"
                reasons.append("no authorship on either side and too little classification to compare")
            else:
                cat = "B"
                reasons.extend(blockers)
        rec["category"] = cat
        rec["decisive_evidence"] = reasons
        # B sub-class proposed as a second, separately approved batch: the only
        # blocker is a family/order placement difference (class and phylum agree).
        rec["b_subclass"] = None
        if cat == "B":
            if all(b.startswith("classification_disagrees:") for b in reasons) and \
                    set(disagree) <= {"ord", "family"} and n["rank"] in SPECIES_RANKS and \
                    auth in au.COMPATIBLE:
                rec["b_subclass"] = "placement_only"
            elif reasons == ["col_places_nortaxa_authorship_under_other_concept"] or \
                    reasons[:1] == ["col_places_nortaxa_authorship_under_other_concept"]:
                rec["b_subclass"] = "col_places_nortaxa_authorship_under_other_concept"
            elif all(b.startswith("nomenclatural_status_") for b in reasons):
                rec["b_subclass"] = "nomenclatural_status"
            elif reasons == ["col_provisionally_accepted"]:
                rec["b_subclass"] = "col_provisional_only"
            elif len(reasons) == 1 and reasons[0].startswith("authorship_"):
                rec["b_subclass"] = reasons[0]
            else:
                rec["b_subclass"] = "multiple_or_other"
        if cat == "A":
            rec["proposed_treatment"] = (f"supersede Sporely {n['sid']} (NorTaxa {n['ident']}) by "
                                         f"{c['sid']} (COL {c['ident']}); re-key names, vernaculars, "
                                         "external ids; emit nortaxa_taxon_id bridge")
        elif cat == "C":
            rec["proposed_treatment"] = "keep both concepts; record as reviewed-distinct (rejected mapping)"
        elif cat == "D":
            rec["proposed_treatment"] = "keep both; needs source data (authorship/classification) before review"
        else:
            rec["proposed_treatment"] = "owner/taxonomist review per blocker list"
        return rec

    recs = [classify_pair(n, c) for n, c in pairs]
    recs.sort(key=lambda r: (r["category"], r["nortaxa_rank"], r["scientific_name"], r["nortaxa_sporely_taxon_id"], r["col_sporely_taxon_id"]))
    with gzip.open(a.out_dir / "cross_source_pairs.jsonl.gz", "wt") as f:
        for r in recs:
            f.write(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n")

    # ------------------------------------------------ COL-internal groups
    col_groups = []
    groups = collections.defaultdict(list)
    for t in taxa.values():
        if t["src"] == "col_xr":
            groups[(t["nkey"], t["rank"])].append(t)
    for (k, rank), mem in sorted(groups.items()):
        if len(mem) < 2:
            continue
        mem.sort(key=lambda t: (t["status"] != "accepted", -children.get(t["sid"], 0), t["sid"]))
        auths = [au.compare(mem[0]["authorship"], m["authorship"]) for m in mem[1:]]
        cls_dis = [cls_compare(mem[0], m)[1] for m in mem[1:]]
        statuses = sorted(m["status"] for m in mem)
        any_obs = any(sum(obs(m["sid"]).values()) for m in mem[1:])
        retired_children = sum(children.get(m["sid"], 0) for m in mem[1:])
        nt_twins = [t["sid"] for t in by_nkey[k] if t["src"] == "nortaxa" and t["rank"] == rank]
        reasons = []
        if any(x == "conflicting" for x in auths):
            if any(cls_dis):
                cat = "C"; reasons.append("different authors and different higher classification: homonyms")
            else:
                cat = "B"; reasons.append("different authors, same classification: possible homonym or COL duplicate")
        elif any(cls_dis):
            cat = "B"; reasons.append("compatible authorship but contradictory classification")
        elif any(x not in au.COMPATIBLE for x in auths):
            cat = "B"; reasons.append("authorship not compatible on at least one record "
                                      "(incomplete, qualified, unpublished or ex-author)")
        elif any((col_acc.get(m["ident"]) or {}).get("name_status") in rules.COL_BLOCKED_NAME for m in mem):
            cat = "B"; reasons.append("nomenclatural status (COL col:nameStatus) blocks automatic approval")
        elif retired_children:
            cat = "B"; reasons.append("redundant, but the retiring record has child concepts that would need re-parenting")
        elif len(mem) > 2:
            cat = "B"; reasons.append("more than two records")
        else:
            cat = "A"; reasons.append("same name, rank, compatible authorship, agreeing classification; survivor by status/children/id rule")
        col_groups.append({
            "scientific_name": mem[0]["name"], "rank": rank,
            "members": [{"sporely_taxon_id": m["sid"], "col_usage_id": m["ident"], "status": m["status"],
                         "authorship": m["authorship"], "family": m["family"], "order": m["ord"],
                         "class": m["class"], "children": children.get(m["sid"], 0),
                         "col_source_id": (col_acc.get(m["ident"]) or {}).get("source_id"),
                         "in_cloud_scope": m["sid"] in cloud, "obs": obs(m["sid"]),
                         "nb": sorted(nb[m["sid"]])[:2]} for m in mem],
            "authorship_vs_first": auths, "classification_disagree_vs_first": cls_dis,
            "statuses": statuses, "nortaxa_same_rank_twins": nt_twins,
            "category": cat, "decisive_evidence": reasons,
            "proposed_survivor": mem[0]["sid"] if cat == "A" else None,
            "survivor_rule": "accepted before provisionally accepted; then most child concepts; then lowest sporely_taxon_id",
            "retired_members_with_observations": any_obs,
        })
    with open(a.out_dir / "col_internal_groups.jsonl", "w") as f:
        for g in col_groups:
            f.write(json.dumps(g, ensure_ascii=False, sort_keys=True) + "\n")

    # ------------------------------------------------ NorTaxa-internal groups
    nt_groups = []
    groups = collections.defaultdict(list)
    for t in taxa.values():
        if t["src"] == "nortaxa":
            groups[t["nkey"]].append(t)
    for k, mem in sorted(groups.items()):
        if len(mem) < 2:
            continue
        mem.sort(key=lambda t: (t["rank"], t["sid"]))
        ranks = sorted({m["rank"] for m in mem})
        placeholder = mem[0]["name"].lower().startswith("incertae sedis")
        reasons = []
        same_rank = collections.defaultdict(list)
        for m in mem:
            same_rank[m["rank"]].append(m)
        subgroups = []
        for rank, sm in sorted(same_rank.items()):
            if len(sm) < 2:
                continue
            auths = [au.compare(sm[0]["authorship"], m["authorship"]) for m in sm[1:]]
            dis = [cls_compare(sm[0], m)[1] for m in sm[1:]]
            if placeholder:
                c = "C"; why = "classification placeholder, not a taxon: never merge; exclude from search"
            elif any(x == "conflicting" for x in auths):
                c = "C"; why = "different authorship: distinct names (homonyms or different basionyms)"
            elif any(dis) or any(x not in au.COMPATIBLE for x in auths):
                c = "B"; why = "same name and rank, but classification or authorship differ"
            else:
                c = "A"; why = "identical name, rank, authorship and classification: redundant source records"
            subgroups.append({"rank": rank, "sporely_taxon_ids": [m["sid"] for m in sm],
                              "nortaxa_taxon_ids": [m["ident"] for m in sm],
                              "authorships": [m["authorship"] for m in sm],
                              "families": [m["family"] for m in sm],
                              "authorship_vs_first": auths, "classification_disagree_vs_first": dis,
                              "category": c, "decisive_evidence": why,
                              "obs": [obs(m["sid"]) for m in sm]})
        if placeholder:
            cat = "C"; reasons.append("Incertae sedis placeholder")
        elif subgroups:
            order = {"C": 0, "B": 1, "A": 2}
            cat = sorted((s["category"] for s in subgroups), key=lambda x: order[x])[0]
            reasons.append("same-rank subgroup(s) present")
        else:
            cat = "C"; reasons.append("same name at different ranks (genus/subgenus/section...): legitimate")
        nt_groups.append({"scientific_name": mem[0]["name"], "ranks": ranks, "size": len(mem),
                          "placeholder": placeholder, "same_rank_subgroups": subgroups,
                          "members": [{"sporely_taxon_id": m["sid"], "nortaxa_taxon_id": m["ident"],
                                       "rank": m["rank"], "authorship": m["authorship"],
                                       "family": m["family"]} for m in mem] if not placeholder else [],
                          "category": cat, "decisive_evidence": reasons})
    with open(a.out_dir / "nortaxa_internal_groups.jsonl", "w") as f:
        for g in nt_groups:
            f.write(json.dumps(g, ensure_ascii=False, sort_keys=True) + "\n")

    # ------------------------------------------------ summary
    S = {}
    def cnt(rows, *keys):
        c = collections.Counter(tuple(k(r) for k in keys) for r in rows)
        return {"|".join(map(str, k)): v for k, v in sorted(c.items())}
    rank_group = lambda r: ("species" if r["nortaxa_rank"] == "species" else
                            "infraspecific" if r["nortaxa_rank"] in SPECIES_RANKS else
                            "genus" if r["nortaxa_rank"] == "genus" else "higher") \
        if r["nortaxa_rank"] == r["col_rank"] else "different_rank"
    S["cross_source"] = {
        "pairs": len(recs),
        "distinct_nortaxa": len({r["nortaxa_sporely_taxon_id"] for r in recs}),
        "distinct_col": len({r["col_sporely_taxon_id"] for r in recs}),
        "by_rank_group_category": cnt(recs, rank_group, lambda r: r["category"]),
        "by_rank_group_category_distinct_nortaxa": {
            k: len(v) for k, v in sorted(_group_ids(recs, rank_group).items())},
        "species_authorship_class": cnt([r for r in recs if rank_group(r) == "species"],
                                        lambda r: r["authorship_class"], lambda r: r["category"]),
        "b_subclass": cnt([r for r in recs if r["category"] == "B"], rank_group, lambda r: r["b_subclass"]),
        "b_placement_only_cloud": cnt([r for r in recs if r.get("b_subclass") == "placement_only"], rank_group, lambda r: r["col_in_cloud_scope"]),
        "species_B_blockers": _blockers([r for r in recs if rank_group(r) == "species" and r["category"] == "B"]),
        "genus_B_blockers": _blockers([r for r in recs if rank_group(r) == "genus" and r["category"] == "B"]),
        "species_B_single_blocker": _single([r for r in recs if rank_group(r) == "species" and r["category"] == "B"]),
        "genus_B_single_blocker": _single([r for r in recs if rank_group(r) == "genus" and r["category"] == "B"]),
        "species_by_published_synonymy": cnt([r for r in recs if rank_group(r) == "species"],
                                             lambda r: r["published_synonymy"], lambda r: r["category"]),
        "species_by_stage2_manifest": cnt([r for r in recs if rank_group(r) == "species"],
                                          lambda r: (r["stage2_manifest_0926"] or "none").split("--")[0], lambda r: r["category"]),
        "by_cloud_scope": cnt(recs, rank_group, lambda r: r["category"], lambda r: r["col_in_cloud_scope"]),
        "inaturalist": cnt([r for r in recs if rank_group(r) == "species"], lambda r: r["inaturalist"], lambda r: r["category"]),
        "artportalen_vs_dyntaxa": cnt([r for r in recs if rank_group(r) == "species"], lambda r: r["artportalen_vs_dyntaxa"], lambda r: r["category"]),
        "col_record_from_nortaxa_source": cnt(recs, rank_group, lambda r: r["col_record_from_nortaxa_source"], lambda r: r["category"]),
        "shared_nb_vernacular": cnt(recs, rank_group, lambda r: r["shared_nb_vernacular"], lambda r: r["category"]),
        "observations": {
            cat: {
                "pairs_with_nortaxa_desktop_obs": sum(1 for r in recs if r["category"] == cat and r["obs_nortaxa"]["desktop"]),
                "nortaxa_desktop_obs": sum(r["obs_nortaxa"]["desktop"] for r in recs if r["category"] == cat),
                "pairs_with_col_desktop_obs": sum(1 for r in recs if r["category"] == cat and r["obs_col"]["desktop"]),
                "pairs_with_nortaxa_cloud_obs": sum(1 for r in recs if r["category"] == cat and (r["obs_nortaxa"]["cloud_selected"] + r["obs_nortaxa"]["cloud_resolved"])),
                "nortaxa_cloud_resolved_obs": sum(r["obs_nortaxa"]["cloud_resolved"] for r in recs if r["category"] == cat),
                "nortaxa_cloud_selected_obs": sum(r["obs_nortaxa"]["cloud_selected"] for r in recs if r["category"] == cat),
                "pairs_with_col_cloud_obs": sum(1 for r in recs if r["category"] == cat and (r["obs_col"]["cloud_selected"] + r["obs_col"]["cloud_resolved"])),
            } for cat in "ABCD"},
    }
    S["col_internal"] = {"groups": len(col_groups), "concepts": sum(len(g["members"]) for g in col_groups),
                         "by_category": cnt(col_groups, lambda g: g["category"]),
                         "by_rank_category": cnt(col_groups, lambda g: g["rank"], lambda g: g["category"]),
                         "by_statuses": cnt(col_groups, lambda g: "+".join(g["statuses"]), lambda g: g["category"]),
                         "by_reason": cnt(col_groups, lambda g: g["decisive_evidence"][0]),
                         "groups_with_any_obs": sum(1 for g in col_groups if any(sum(m["obs"].values()) for m in g["members"])),
                         "groups_with_nortaxa_twin": sum(1 for g in col_groups if g["nortaxa_same_rank_twins"])}
    S["nortaxa_internal"] = {"groups": len(nt_groups), "concepts": sum(g["size"] for g in nt_groups),
                             "placeholder_groups": sum(1 for g in nt_groups if g["placeholder"]),
                             "placeholder_concepts": sum(g["size"] for g in nt_groups if g["placeholder"]),
                             "by_category": cnt(nt_groups, lambda g: g["category"]),
                             "same_rank_subgroups": cnt([s for g in nt_groups for s in g["same_rank_subgroups"]], lambda s: s["rank"], lambda s: s["category"])}
    # ------------------------------------------------ user-visible duplicate labels (nb)
    retire_A = {r["nortaxa_sporely_taxon_id"] for r in recs if r["category"] == "A"}
    retire_A_species = {r["nortaxa_sporely_taxon_id"] for r in recs if r["category"] == "A" and rank_group(r) == "species"}
    S["duplicate_labels_nb"] = {
        "before": _labels(taxa, nb_pref, set()),
        "after_A_species_only": _labels(taxa, nb_pref, retire_A_species),
        "after_A_all": _labels(taxa, nb_pref, retire_A),
    }
    (a.out_dir / "summary.json").write_text(json.dumps(S, indent=1, ensure_ascii=False, sort_keys=True))
    print(json.dumps({k: (v if k != "cross_source" else {kk: vv for kk, vv in v.items() if kk in ("pairs", "by_rank_group_category")}) for k, v in S.items()}, indent=1, ensure_ascii=False))
    return 0


def _group_ids(recs, rank_group):
    out = collections.defaultdict(set)
    for r in recs:
        out[f"{rank_group(r)}|{r['category']}"].add(r["nortaxa_sporely_taxon_id"])
    return out


def _blockers(rows):
    c = collections.Counter()
    for r in rows:
        for b in r["decisive_evidence"]:
            c[b.split(":")[0]] += 1
    return dict(c.most_common())


def _single(rows):
    c = collections.Counter()
    for r in rows:
        if len(r["decisive_evidence"]) == 1:
            c[r["decisive_evidence"][0]] += 1
    return dict(c.most_common())


def _labels(taxa, nb_pref, retired):
    """Picker-visible duplicates: concepts of rank species or genus whose
    display label '<nb preferred vernacular> (<canonical name>)' (or the
    canonical name alone without an nb name) equals another live concept's."""
    labels = collections.Counter()
    sci_only = collections.Counter()
    for t in taxa.values():
        if t["sid"] in retired or t["rank"] not in ("species", "genus"):
            continue
        v = nb_pref.get(t["sid"])
        labels[(t["rank"], f"{v} ({t['name']})" if v else t["name"])] += 1
        sci_only[(t["rank"], t["name"])] += 1
    dup = {k: n for k, n in labels.items() if n > 1}
    dups = {k: n for k, n in sci_only.items() if n > 1}
    return {
        "identical_label_groups": len(dup),
        "redundant_rows": sum(n - 1 for n in dup.values()),
        "identical_label_groups_species": sum(1 for k in dup if k[0] == "species"),
        "redundant_rows_species": sum(n - 1 for k, n in dup.items() if k[0] == "species"),
        "same_scientific_name_groups": len(dups),
        "same_scientific_name_redundant_rows": sum(n - 1 for n in dups.values()),
    }


if __name__ == "__main__":
    raise SystemExit(main())
