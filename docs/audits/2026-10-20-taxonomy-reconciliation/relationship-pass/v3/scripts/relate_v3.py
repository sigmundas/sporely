#!/usr/bin/env python3
"""Relationship pass 3 (correction of pass 2 after ../../v2/review-codex.md, plus
ChecklistBank source-record provenance from the closed GBIF research).

Same 2,805 pairs and pinned inputs as passes 1 and 2. Built on
../../v2/scripts/relate_v2.py; changes:

 1. Author comparison by authors_v3.py: initials kept, particles kept, 'f.'
    and 'in' handled, roles moving between citation parts are a
    role_mismatch, unparsable authorship is 'unparsed' and empty authorship
    'missing' (both fail closed).
 2. Homotypic consistency (review finding 1): every documented homotypic
    witness must share the original-combination author with both accepted
    names, and COL's accepted name with its own basionymID record. A
    'different' or 'role_mismatch' there is a contradiction -> review.
 3. Verified's witness-author exception is limited to a surname abbreviation
    in the combining author (both sides cite one, basionym parts equal).
    Pre-'ex' omission ('Hepp' / 'Nägeli ex Hepp'), initials on one side and
    particles no longer pass a witness. Accepted authors may still differ by
    abbreviation-type candidates (listed in VERIFIED_ACCEPTED_CANDIDATE).
 4. Pragmatic is split: pragmatic_compatible (identical, ICN-compatible or an
    author-standard candidate), pragmatic_missing_authorship, and the
    contradictory rest goes to unresolved with subclass 'contradictory_authorship'
    (combining author differs, basionym cited on one side only).
 5. New evidence class 'checklistbank_source_record' from the cached
    /dataset/315834/nameusage/{id}/source responses (data/
    checklistbank_source_records.csv). It is traceable ORIGIN evidence, never
    independent corroboration: verified_eligible is always False. Exact copies
    (the COL usage's source record IS this NorTaxa taxonID) are distinguished
    from name matches, and COL accepted usages from COL synonyms; a copy of
    this NorTaxa record that COL places under a different concept is a
    contradiction.

Outputs (UTF-8-BOM CSV + JSONL) in --out-dir:
  verified.{csv,jsonl}, pragmatic_compatible.{csv,jsonl},
  pragmatic_missing_authorship.{csv,jsonl}, unresolved.{csv,jsonl},
  transition_from_pass2.csv, transition_pairs_from_pass2.csv, summary.json
"""
from __future__ import annotations

import argparse
import collections
import csv
import gzip
import json
import sqlite3
import sys
import unicodedata
from pathlib import Path

sys.dont_write_bytecode = True
V3 = Path(__file__).resolve().parent
sys.path.insert(0, str(V3))
sys.path.insert(0, str(V3.parents[2] / "scripts"))
import authors_v3 as av  # noqa: E402
import authorship as au  # noqa: E402  (pass-1 qualifier flags only)

NORTAXA_SOURCE = "2030"
PINNED_CLB = "315834"
COL_BAD_STATUS = {"unacceptable", "doubtful", "manuscript", "not established", "rejected"}
NT_BAD_STATUS = {"illegitimate", "dubious", "notvalidlypublished", "orthographic", "misapplied"}
OK = av.OK
COMPAT = av.COMPATIBLE_REASONS                       # ICN-permitted omissions, carried along with candidates
WITNESS_EXCEPTION = {"surname_abbreviation"} | COMPAT
VERIFIED_ACCEPTED_CANDIDATE = {"surname_abbreviation", "initials_one_side", "initials_prefix",
                               "particle_one_side", "et_al", "pre_ex_omitted"} | COMPAT
AUTHOR_STANDARD = VERIFIED_ACCEPTED_CANDIDATE        # pre_ex_differs, in_host_differs, *_part_one_side: contradictory
CONTRADICTORY = ("different", "role_mismatch")


def _key(a: str) -> str:
    return "".join(ch for ch in av.fold(a) if ch.isalpha())


def _lev(x: str, y: str) -> int:
    prev = list(range(len(y) + 1))
    for i, cx in enumerate(x, 1):
        cur = [i]
        for j, cy in enumerate(y, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (cx != cy)))
        prev = cur
    return prev[-1]


def prov(source_id: str) -> str:
    return "copied_from_nortaxa" if source_id == NORTAXA_SOURCE else (
        "original" if source_id else "unknown_source")


def ro(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    for x in ("csv", "pairs", "pass2-dir", "workdb", "relations", "release", "manual-mappings", "clb", "out-dir"):
        ap.add_argument("--" + x, required=True, type=Path)
    a = ap.parse_args(argv)
    a.out_dir.mkdir(parents=True, exist_ok=True)
    rows = list(csv.DictReader(a.csv.open(encoding="utf-8-sig")))
    recs = {}
    for line in gzip.open(a.pairs, "rt"):
        r = json.loads(line)
        recs[(str(r["nortaxa_sporely_taxon_id"]), str(r["col_sporely_taxon_id"]))] = r
    pass2 = {}
    for f in ("verified_candidates", "pragmatic_candidates", "unresolved_pairs"):
        for r in map(json.loads, (a.pass2_dir / f"{f}.jsonl").open(encoding="utf-8")):
            pass2[(r["nortaxa_taxon_id"], r["col_usage_id"])] = r
    w = ro(a.workdb)
    w.row_factory = sqlite3.Row
    rl = ro(a.relations)
    rel = ro(a.release)

    col = {r["id"]: dict(r) for r in w.execute(
        "select id, alt_id, source_id, parent_id, basionym_id, status, name, nkey, authorship, rank, "
        "name_status, class, phylum from col_raw")}
    col_children, col_by_nkey = collections.defaultdict(list), collections.defaultdict(list)
    for r in col.values():
        if r["status"] not in ("accepted", "provisionally accepted"):
            col_children[r["parent_id"]].append(r)
        col_by_nkey[r["nkey"]].append(r)
    col_rel = collections.defaultdict(list)
    for x, y, src, t in rl.execute("select nameID, relatedNameID, sourceID, type from col_rel"):
        col_rel[x].append((y, t, src))
        col_rel[y].append((x, t, src))
    nt = {r["taxon_id"]: dict(r) for r in w.execute(
        "select taxon_id, accepted_id, name, nkey, authorship, rank, status, nom_status, class, phylum from nt_raw")}
    nt_syn, nt_by_nkey = collections.defaultdict(list), collections.defaultdict(list)
    for r in nt.values():
        if r["accepted_id"] and r["accepted_id"] != r["taxon_id"]:
            nt_syn[r["accepted_id"]].append(r)
        nt_by_nkey[r["nkey"]].append(r)
    nt_id_of_row = dict(rl.execute('select "id", "taxonID" from nt_taxon'))
    nt_vern = collections.defaultdict(set)
    for rid, name, lang in rl.execute('select "id", "vernacularName", "language" from nt_vern'):
        if lang in ("nb", "nn", "no"):
            nt_vern[nt_id_of_row.get(rid, rid)].add(f"{lang}:{name.strip()}")
    col_vern = collections.defaultdict(set)
    for tid, lang, name, src in rl.execute("select taxonID, language, name, sourceID from col_vern"):
        col_vern[tid].add((name.strip(), lang, src or ""))
    dyn = {row[0]: row for row in rl.execute("select * from dyn_taxon")}
    dyn_by_acc = collections.defaultdict(list)
    for row in dyn.values():
        dyn_by_acc[row[1]].append(row)
    ext = collections.defaultdict(list)
    for sid, ss, e, note in rel.execute(
            "select taxon_id, source_system, external_id, note from taxon_external_id_min "
            "where source_system='inaturalist'"):
        ext[sid].append((e, note or ""))
    mm = collections.defaultdict(list)
    for m in json.loads(a.manual_mappings.read_text())["mappings"]:
        if m["source_usage"]["source"] == "nortaxa":
            mm[m["source_usage"]["identifier"]].append((m["target"]["source_usage"]["identifier"], m["mapping_id"]))
    # ChecklistBank source records on the pinned release (cached; no network)
    clb, copies = {}, collections.defaultdict(list)
    for r in csv.DictReader(a.clb.open(encoding="utf-8-sig")):
        if r["release_dataset_key"] != PINNED_CLB:
            continue
        clb[r["col_usage_id"]] = r
        if r["source_dataset_key"] == NORTAXA_SOURCE:
            copies[r["source_record_id"]].append(r["col_usage_id"])

    def acc_of(u):
        return u["id"] if u["status"] in ("accepted", "provisionally accepted") else u["parent_id"]

    def qualified(s):
        return bool(au.flags(s or ""))

    def clb_ref(cid):
        return [["clb:dataset/315834/nameusage/{id}/source", cid],
                ["v3/data/checklistbank_source_records.csv", f"{PINNED_CLB}:{cid}"]]

    results = []
    for row in rows:
        nid, cid = row["nortaxa_taxon_id"], row["col_usage_id"]
        nsid, csid = row["nortaxa_sporely_taxon_id"], row["col_sporely_taxon_id"]
        N, C = nt[nid], col[cid]
        rec = recs.get((nsid, csid), {})
        B = col.get(C["basionym_id"]) if C["basionym_id"] else None
        ev, contra, ambi, flags = [], [], [], []
        acmp = av.compare(N["authorship"], C["authorship"])
        alvl, areasons = av.level(acmp), av.reasons(acmp)
        bcmp = av.basionym_cmp(N["authorship"], C["authorship"])
        blvl = av.level(bcmp)
        bas_check = av.original_cmp(C["authorship"], B["authorship"]) if B else None

        # ---------------- witnesses: synonyms published by both sides
        cands = list(col_children.get(cid, []))
        if B and B not in cands:
            cands.append(B)
        dyn_links = [f"urn:lsid:dyntaxa.se:Taxon:{t.rsplit(':', 1)[1]}"
                     for t in (C["alt_id"] or "").split(",") if t.startswith("dyntaxa:")]
        homotypic_contradictions = []
        for s in nt_syn.get(nid, []):
            for u in cands:
                if u["nkey"] != s["nkey"]:
                    continue
                cmp = av.compare(s["authorship"], u["authorship"])
                docs = []
                if B and u["id"] == B["id"]:
                    docs.append({"link": "col:basionymID", "row": ["col:NameUsage.tsv", cid], "source_id": C["source_id"]})
                if u["basionym_id"] and u["basionym_id"] in {cid, C["basionym_id"]}:
                    docs.append({"link": "col:basionymID", "row": ["col:NameUsage.tsv", u["id"]], "source_id": u["source_id"]})
                for y, t, src in col_rel.get(u["id"], []):
                    if y in {cid, C["basionym_id"]} and t in ("basionym", "homotypic", "based on"):
                        docs.append({"link": f"col:NameRelation:{t}", "row": ["col:NameRelation.tsv", f"{u['id']}~{y}"],
                                     "source_id": src or u["source_id"]})
                for dl in dyn_links:
                    for dr in dyn_by_acc.get(dl, []):
                        dc = av.compare(dr[5], s["authorship"])
                        if dr[6] == "homotypicSynonym" and unicodedata.normalize("NFC", dr[3]).casefold() == s["nkey"] \
                                and (av.level(dc) in OK or (av.level(dc) == "candidate" and av.reasons(dc) <= WITNESS_EXCEPTION)):
                            docs.append({"link": "dyntaxa:homotypicSynonym", "row": ["dyntaxa:Taxon.csv", dr[0]],
                                         "source_id": "dyntaxa"})
                bad, exception = [], None
                if u["status"] != "synonym":
                    bad.append("col_status:" + u["status"])
                if (u["name_status"] or "") in COL_BAD_STATUS:
                    bad.append("col_name_status:" + u["name_status"])
                if qualified(s["authorship"]) or qualified(u["authorship"]):
                    bad.append("qualified_or_unpublished")
                if av.level(cmp) not in OK:
                    try:
                        both_comb = bool(av.split_paren(av.tidy(s["authorship"]))[1]) and \
                            bool(av.split_paren(av.tidy(u["authorship"]))[1])
                    except av.Unparsed:
                        both_comb = False
                    if av.level(cmp) == "candidate" and av.reasons(cmp) <= WITNESS_EXCEPTION and both_comb \
                            and av.level(av.basionym_cmp(s["authorship"], u["authorship"])) in OK:   # None (no basionym part) fails
                        exception = "combining_author_surname_abbreviation"
                    else:
                        bad.append("witness_authorship:" + cmp)
                # homotypic consistency: original-combination authors (review finding 1). Only a
                # witness that is otherwise acceptable (same name and authorship on both sides)
                # can contradict the pair; for excluded witnesses the checks are recorded only.
                orig, would_be_ok = {}, not bad
                if docs:
                    orig = {"nortaxa_accepted_vs_witness": av.original_cmp(N["authorship"], s["authorship"]),
                            "col_accepted_vs_witness": av.original_cmp(C["authorship"], u["authorship"])}
                    if bas_check is not None:
                        orig["col_accepted_vs_col_basionym"] = bas_check
                    hard = [k for k, v in orig.items() if av.level(v) in CONTRADICTORY]
                    if hard == ["nortaxa_accepted_vs_witness"] and nt_misspelling(N["authorship"], s["authorship"]):
                        hard = []                         # NorTaxa's own synonym spells the author as COL does
                        orig["note"] = "nortaxa_accepted_original_author_misspelt(edit_distance<=2)"
                    if hard:
                        if would_be_ok:
                            homotypic_contradictions.append(f"{s['taxon_id']}/{u['id']}:" + ",".join(hard))
                        bad.append("homotypic_original_author_contradiction:" + ",".join(hard))
                    elif any(av.level(v) == "unparsed" for v in orig.values()):
                        bad.append("original_author_unparsed")
                col_prov = prov(u["source_id"])
                indep_docs = [d for d in docs if d["source_id"] == "dyntaxa" or
                              (prov(d["source_id"]) == "original" and col_prov == "original")]
                kind = ("homotypic_documented" if docs else "shared_synonym_undocumented")
                ev.append({"kind": kind, "nortaxa_row": ["nortaxa:taxon.txt", s["taxon_id"]],
                           "col_row": ["col:NameUsage.tsv", u["id"]], "witness": f"{s['name']} {s['authorship']}",
                           "col_witness": f"{u['name']} {u['authorship']}", "witness_author_cmp": cmp,
                           "witness_author_exception": exception, "original_author_checks": orig,
                           "col_row_provenance": col_prov, "provenance_class": col_prov,
                           "nortaxa_nom_status": s["nom_status"] or "",
                           "documentation": docs, "excluded_because": bad,
                           "source_verified": True,
                           "verified_eligible": bool(indep_docs) and not bad,
                           "independent_documentation": [d["link"] for d in indep_docs]})
        # Dyntaxa homotypic rows for NorTaxa synonyms that COL does not list
        for s in nt_syn.get(nid, []):
            if any(e.get("nortaxa_row", [None, None])[1] == s["taxon_id"] for e in ev):
                continue
            for dl in dyn_links:
                for dr in dyn_by_acc.get(dl, []):
                    if dr[6] == "homotypicSynonym" and unicodedata.normalize("NFC", dr[3]).casefold() == s["nkey"] \
                            and av.level(av.compare(dr[5], s["authorship"])) in OK:
                        orig = {"nortaxa_accepted_vs_witness": av.original_cmp(N["authorship"], s["authorship"]),
                                "col_accepted_vs_witness": av.original_cmp(C["authorship"], dr[5])}
                        bad = ["qualified_or_unpublished"] if qualified(s["authorship"]) else []
                        hard = [k for k, v in orig.items() if av.level(v) in CONTRADICTORY]
                        if hard and not bad:
                            homotypic_contradictions.append(f"{s['taxon_id']}/dyntaxa:{dr[0]}:" + ",".join(hard))
                            bad.append("homotypic_original_author_contradiction:" + ",".join(hard))
                        elif any(av.level(v) == "unparsed" for v in orig.values()):
                            bad.append("original_author_unparsed")
                        ev.append({"kind": "homotypic_documented", "nortaxa_row": ["nortaxa:taxon.txt", s["taxon_id"]],
                                   "col_row": None, "witness": f"{s['name']} {s['authorship']}",
                                   "documentation": [{"link": "dyntaxa:homotypicSynonym", "row": ["dyntaxa:Taxon.csv", dr[0]],
                                                      "source_id": "dyntaxa"}],
                                   "original_author_checks": orig, "excluded_because": bad,
                                   "source_verified": True, "col_row_provenance": None, "provenance_class": "dyntaxa",
                                   "verified_eligible": not bad,
                                   "independent_documentation": ["dyntaxa:homotypicSynonym"]})
        # ---------------- synonym-listing match (a name lookup, not an identifier)
        for u in col_by_nkey[N["nkey"]]:
            if u["id"] != cid and acc_of(u) == cid and u["status"] == "synonym" \
                    and av.level(av.compare(u["authorship"], N["authorship"])) in OK and not qualified(u["authorship"]):
                ev.append({"kind": "synonym_listing_match", "col_row": ["col:NameUsage.tsv", u["id"]],
                           "witness": f"{u['name']} {u['authorship']}", "col_row_provenance": prov(u["source_id"]),
                           "provenance_class": prov(u["source_id"]),
                           "source_verified": True, "verified_eligible": False, "documentation": [],
                           "note": "COL lists NorTaxa's name and authorship as a synonym of the COL concept (name lookup, not an identifier)"})
        # ---------------- identifier cross-reference (shared identifier from both sources)
        id_xref = [mid for tgt, mid in mm.get(nid, []) if tgt == cid]
        for mid in id_xref:
            ev.append({"kind": "identifier_xref_manual_mapping", "row": ["repo:manual_mappings", mid],
                       "provenance_class": "repository_manual_mapping",
                       "source_verified": True, "verified_eligible": True})
        cinat = {int(t[5:]) for t in (C["alt_id"] or "").split(",") if t.startswith("inat:") and t[5:].isdigit()}
        for e, note in ext.get(int(nsid), []):
            if e in cinat:
                ev.append({"kind": "inaturalist_id_agrees", "source_verified": False, "verified_eligible": False,
                           "provenance_class": "inferred_release_row",
                           "rows": [["col:NameUsage.tsv", cid], ["release:taxon_external_id_min", f"{nsid}/inaturalist/{e}"]],
                           "note": "NorTaxa-side id is " + (note or "release row") + " (not published by NorTaxa)"})
        # ---------------- checklistbank_source_record (traceable origin, never corroboration)
        clb_classes = []
        cp = clb.get(cid)
        if cp is None and C["source_id"] == NORTAXA_SOURCE:
            sub, basis, pclass = "not_queried(col_source_2030)", "none", "not_queried"
        elif cp is None:
            # the archive's own sourceID already says this usage is not a NorTaxa copy
            sub, basis, pclass = "name_match:col_usage_from_other_source(local_sourceID_only)", "name", "col_source_record"
        elif not cp["source_dataset_key"]:
            sub, basis, pclass = f"no_source_record(http_{cp['http_status']})", "none", "not_available"
        elif cp["source_dataset_key"] == NORTAXA_SOURCE and cp["source_record_id"] == nid:
            st = {"accepted": "accepted", "provisionally accepted": "provisionally_accepted"}.get(C["status"], C["status"].replace(" ", "_"))
            sub, basis, pclass = f"exact_copy:col_{st}_usage_is_this_nortaxa_record", "source_record_id", "copied_source_provenance"
        elif cp["source_dataset_key"] == NORTAXA_SOURCE:
            sub, basis, pclass = "name_match:col_usage_is_copy_of_other_nortaxa_record", "name", "copied_source_provenance"
        else:
            sub, basis, pclass = "name_match:col_usage_from_other_source", "name", "col_source_record"
        clb_classes.append(sub)
        ev.append({"kind": "checklistbank_source_record", "subclass": sub, "match_basis": basis,
                   "provenance_class": pclass, "col_usage": cid,
                   "source_record": f"{cp['source_dataset_key']}:{cp['source_record_id']}" if cp and cp["source_dataset_key"] else "",
                   "rows": clb_ref(cid) if cp else [["col:NameUsage.tsv#sourceID", f"{cid}:{C['source_id']}"]],
                   "source_verified": cp is not None and bool(cp["source_dataset_key"]),
                   "verified_eligible": False, "independent_corroboration": False,
                   "note": "ChecklistBank VerbatimSource on COL 26.7 XR: traceable origin, not independent biological corroboration"})
        for uid in sorted(copies.get(nid, [])):
            if uid == cid:
                continue
            u = col.get(uid)
            if u is None:
                sub = "exact_copy:not_in_local_fungal_col"
            elif acc_of(u) == cid:
                sub = "exact_copy:col_synonym_of_this_concept"
            elif u["status"] in ("accepted", "provisionally accepted"):
                sub = "exact_copy:other_" + u["status"].replace(" ", "_") + "_col_concept"
                contra.append(f"checklistbank_copy_of_nortaxa_record_is_other_concept:{uid}")
            else:
                sub = "exact_copy:col_synonym_of_other_concept"
                contra.append(f"checklistbank_copy_of_nortaxa_record_placed_under_other_concept:{uid}->{acc_of(u)}")
            clb_classes.append(sub)
            ev.append({"kind": "checklistbank_source_record", "subclass": sub, "match_basis": "source_record_id",
                       "provenance_class": "copied_source_provenance", "col_usage": uid,
                       "col_usage_status": u["status"] if u else "", "col_usage_accepted": acc_of(u) if u else "",
                       "source_record": f"2030:{nid}", "rows": clb_ref(uid), "source_verified": True,
                       "verified_eligible": False, "independent_corroboration": False,
                       "note": "COL usage copied from this NorTaxa taxonID (traceable origin, not corroboration)"})
        # ---------------- vernaculars (support only)
        nv = nt_vern.get(nid, set())
        cv = col_vern.get(cid, set())
        nv_names = {x.split(":", 1)[1].casefold() for x in nv}
        shared = [(n_, l_, s_) for n_, l_, s_ in cv if n_.casefold() in nv_names]
        for n_, l_, s_ in sorted(shared):
            p = "copied_from_nortaxa" if s_ == NORTAXA_SOURCE else ("unverified_blank_source" if not s_ else f"independent:col_source_{s_}")
            ev.append({"kind": "shared_norwegian_vernacular", "name": n_, "provenance": p, "provenance_class": p,
                       "rows": [["nortaxa:vernacularname.txt", nid], ["col:VernacularName.tsv", cid]],
                       "source_verified": True, "verified_eligible": False,
                       "independent_corroboration": p.startswith("independent")})

        # ---------------- contradictions / ambiguities
        if N["rank"] != (C["rank"] or "").lower():
            contra.append("rank_differs")
        for u in col_by_nkey[N["nkey"]]:
            if u["id"] != cid and (u["rank"] or "").lower() == N["rank"] and acc_of(u) != cid \
                    and av.level(av.compare(u["authorship"], N["authorship"])) in OK:
                contra.append(f"col_places_nortaxa_name_elsewhere:{u['id']}->{acc_of(u)}")
        for s in nt_by_nkey[C["nkey"]]:
            if s["taxon_id"] != nid and s["rank"] == N["rank"] and s["accepted_id"] != nid \
                    and av.level(av.compare(s["authorship"], C["authorship"])) in OK:
                contra.append(f"nortaxa_places_col_name_elsewhere:{s['taxon_id']}->{s['accepted_id']}")
        if rec.get("inaturalist") == "conflict":
            contra.append("inaturalist_id_conflict")
        if rec.get("artportalen_vs_dyntaxa") == "conflict":
            contra.append("artportalen_dyntaxa_id_conflict")
        if blvl == "different":
            contra.append("different_basionym_author")
        for tgt, mid in mm.get(nid, []):
            if tgt != cid:
                contra.append(f"manual_mapping_to_other:{mid}")
        higher = [f for f in ("class", "phylum") if (N[f] or "").strip() and (C[f] or "").strip()
                  and not (N[f] or "").lower().startswith("incertae") and not (C[f] or "").lower().startswith("incertae")
                  and N[f].strip() != C[f].strip()]
        if not rec.get("one_to_one", False):
            ambi.append("not_one_to_one")
        if (N["nom_status"] or "") in NT_BAD_STATUS:
            ambi.append("nortaxa_status:" + N["nom_status"])
        if (C["name_status"] or "") in COL_BAD_STATUS:
            ambi.append("col_name_status:" + C["name_status"])
        if C["status"] != "accepted":
            ambi.append("col_" + C["status"].replace(" ", "_"))
        if qualified(N["authorship"]) or qualified(C["authorship"]):
            ambi.append("qualified_or_unpublished_accepted_authorship")
        if "unparsed" in (alvl, blvl):
            ambi.append("authorship_unparsed(fail_closed)")
        if "role_mismatch" in (alvl, blvl):
            ambi.append("author_role_mismatch(sanctioning_validating_ex_in_or_basionym)")
        if homotypic_contradictions:
            ambi.append("homotypic_original_author_contradiction:" + "|".join(homotypic_contradictions))

        if rec.get("classification_disagree"):
            flags.append("placement_differs:" + "+".join(rec["classification_disagree"]))

        # ---------------- outcome
        verified_w = [e for e in ev if e.get("verified_eligible")]
        attribution_only = alvl == "different" and blvl in OK
        if attribution_only:
            flags.append("combining_author_attribution_differs")
        reconciled = alvl in OK or (alvl == "candidate" and areasons <= VERIFIED_ACCEPTED_CANDIDATE)
        if alvl == "candidate" and reconciled:
            flags.append("accepted_author_candidate:" + "+".join(sorted(areasons)))
        # Misspelt accepted author (Entoloma sphagneti 'Navaeu'/'Naveau'), unchanged rule from pass 2.
        if alvl == "different":
            try:
                no_paren = av.split_paren(av.tidy(N["authorship"]))[0] is None and \
                    av.split_paren(av.tidy(C["authorship"]))[0] is None
            except av.Unparsed:
                no_paren = False
            if no_paren and _lev(_key(N["authorship"]), _key(C["authorship"])) <= 2:
                for e in verified_w:
                    if e.get("nortaxa_row") and av.level(av.original_cmp(nt[e["nortaxa_row"][1]]["authorship"],
                                                                        C["authorship"])) in OK:
                        reconciled = True
                        flags.append("accepted_author_misspelling_explained_by_nortaxa_own_homotypic_synonym")
                        break
        if alvl == "different" and blvl is None and not reconciled and no_paren_safe(N["authorship"], C["authorship"]):
            ambi.append("possible_homonym_different_authors")
        for e in verified_w:
            if e.get("witness_author_exception"):
                flags.append("verified_witness_author_exception:" + e["witness_author_exception"])
        if contra:
            outcome, sub, reason = "unresolved", "conflict", "conflict:" + ";".join(sorted({c.split(":")[0] for c in contra}))
        elif higher:
            outcome, sub, reason = "unresolved", "placement_review", "placement_review:" + "+".join(f"{f}_differs" for f in higher)
        elif ambi:
            outcome, sub, reason = "unresolved", "ambiguity", "ambiguity:" + ";".join(sorted({x.split(":")[0] for x in ambi}))
        elif verified_w and reconciled:
            outcome, sub = "verified", "verified"
            reason = "+".join(sorted({d for e in verified_w for d in e.get("independent_documentation", [])} |
                                     ({"identifier_xref"} if id_xref else set())))
            if pass2.get((nid, cid), {}).get("outcome") != "verified":
                # entered only through pass-3 comparator corrections: not yet reviewed, held out of verified
                outcome, sub = "pragmatic_compatible", "verified_rules_new_entrant_held"
                reason = "meets_pass3_verified_rules_but_not_verified_in_pass2:" + reason
        elif alvl == "missing":
            outcome, sub = "pragmatic_missing_authorship", (
                "missing_both_sides" if not (N["authorship"] or "").strip() and not (C["authorship"] or "").strip()
                else "missing_one_side")
            reason = sub + ("+documented_homotypy_present" if verified_w else "")
        elif verified_w and attribution_only:
            outcome, sub, reason = "unresolved", "contradictory_authorship", "contradictory_authorship:combining_author_differs(documented_homotypy)"
        elif verified_w:
            outcome, sub = "unresolved", "accepted_authors_differ"
            reason = "accepted_authors_differ_despite_documented_homotypy:" + acmp
        elif alvl == "identical":
            outcome, sub, reason = "pragmatic_compatible", "identical", "authorship_identical"
        elif alvl == "compatible":
            outcome, sub, reason = "pragmatic_compatible", "icn_compatible", "authorship_" + acmp
        elif alvl == "candidate" and areasons <= AUTHOR_STANDARD:
            outcome, sub, reason = "pragmatic_compatible", "author_standard_candidate", "authorship_" + acmp
        elif alvl == "candidate":
            outcome, sub, reason = "unresolved", "contradictory_authorship", "contradictory_authorship:" + acmp
        elif attribution_only:
            outcome, sub, reason = "unresolved", "contradictory_authorship", "contradictory_authorship:combining_author_differs(same_basionym_author)"
        else:
            kinds = {e["kind"] for e in ev}
            outcome, sub = "unresolved", "accepted_authors_differ"
            reason = ("accepted_authors_differ;shared_synonymy_undocumented_or_ineligible"
                      if kinds & {"homotypic_documented", "shared_synonym_undocumented"} else "accepted_authors_differ;name_only")
        if outcome.startswith("pragmatic"):
            sup = sorted({e["kind"] + ("[" + (e.get("subclass") or e.get("col_row_provenance") or "") + "]"
                                       if (e.get("subclass") or e.get("col_row_provenance")) else "")
                          for e in ev if e["kind"] not in ("shared_norwegian_vernacular",)})
            flags.append("support:" + ",".join(sup))
        p2 = pass2.get((nid, cid), {})
        results.append({"outcome": outcome, "subclass": sub, "reason": reason, "scientific_name": row["scientific_name"],
                        "nortaxa_taxon_id": nid, "nortaxa_sporely_taxon_id": int(nsid),
                        "col_usage_id": cid, "col_sporely_taxon_id": int(csid),
                        "nortaxa_authorship": N["authorship"], "col_authorship": C["authorship"],
                        "accepted_author_cmp": acmp, "basionym_author_cmp": bcmp,
                        "col_source_id": C["source_id"], "col_basionym": f"{B['id']} {B['name']} {B['authorship']}" if B else "",
                        "checklistbank_source_record": clb_classes,
                        "provenance_class": ("copied_source_provenance" if any(c.startswith("exact_copy") for c in clb_classes)
                                             else prov(C["source_id"])),
                        "current_category": row["category"] + (":" + row["b_subclass"] if row["b_subclass"] else ""),
                        "one_to_one": bool(rec.get("one_to_one", False)), "rank": N["rank"],
                        "pass2_outcome": p2.get("outcome"), "pass2_reason": p2.get("reason"),
                        "pass2_accepted_author_cmp": p2.get("accepted_author_cmp"),
                        "contradictions": contra, "ambiguities": ambi, "higher_classification_differs": higher,
                        "evidence": ev, "flags": flags,
                        "nortaxa_vernacular_no": sorted(f"{x}[nortaxa:vernacularname.txt]" for x in nv),
                        "col_vernacular_no": sorted(f"{l_}:{n_}[{'copied_from_nortaxa' if s_ == NORTAXA_SOURCE else ('unverified_blank_source' if not s_ else 'col_source_' + s_)}]"
                                                    for n_, l_, s_ in cv)})
    write_outputs(a.out_dir, results)
    return 0


def nt_misspelling(nt_accepted: str, nt_witness: str) -> bool:
    """NorTaxa's accepted original author differs from its own homotypic synonym's by
    at most two letters (Entoloma sphagneti 'Navaeu' vs 'Naveau')."""
    try:
        x, y = av.original_author(av.tidy(nt_accepted)), av.original_author(av.tidy(nt_witness))
    except av.Unparsed:
        return False
    return bool(x and y) and _lev(_key(x), _key(y)) <= 2


def no_paren_safe(x, y) -> bool:
    try:
        return av.split_paren(av.tidy(x))[0] is None and av.split_paren(av.tidy(y))[0] is None
    except av.Unparsed:
        return False


def _ev_text(e) -> str:
    refs = [x for x in (e.get("nortaxa_row"), e.get("col_row"), e.get("row")) if x] + \
        [d["row"] for d in e.get("documentation", [])] + (e.get("rows") or [])
    tag = e["kind"]
    if e.get("subclass"):
        tag += f"[{e['subclass']}]"
    tag += f"[provenance_class={e.get('provenance_class')}]"
    if e.get("source_record"):
        tag += f"[source_record={e['source_record']}]"
    if e.get("witness_author_exception"):
        tag += f"[exception={e['witness_author_exception']}]"
    if e.get("excluded_because"):
        tag += "{excluded:" + ",".join(e["excluded_because"]) + "}"
    tag += f"(source_verified={e.get('source_verified')},verified_eligible={e.get('verified_eligible')})"
    return tag + " @ " + " ".join(f"{x[0]}#{x[1]}" for x in refs)


CSV_COLS = ["outcome", "subclass", "reason", "scientific_name", "rank", "nortaxa_taxon_id", "nortaxa_sporely_taxon_id",
            "nortaxa_authorship", "col_usage_id", "col_sporely_taxon_id", "col_authorship", "accepted_author_cmp",
            "basionym_author_cmp", "col_source_id", "provenance_class", "checklistbank_source_record", "col_basionym",
            "current_category", "one_to_one", "pass2_outcome", "pass2_reason", "contradictions", "ambiguities",
            "evidence", "nortaxa_vernacular_no", "col_vernacular_no", "flags"]


def write_outputs(out: Path, results: list[dict]) -> None:
    def write(name, rs):
        rs = sorted(rs, key=lambda r: (r["subclass"], r["reason"], r["scientific_name"], r["nortaxa_taxon_id"]))
        with (out / f"{name}.jsonl").open("w", encoding="utf-8") as f:
            for r in rs:
                f.write(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n")
        with (out / f"{name}.csv").open("w", newline="", encoding="utf-8-sig") as f:
            cw = csv.writer(f)
            cw.writerow(CSV_COLS)
            for r in rs:
                v = dict(r)
                v["checklistbank_source_record"] = "; ".join(r["checklistbank_source_record"])
                v["contradictions"] = " | ".join(r["contradictions"] + r["higher_classification_differs"])
                v["ambiguities"] = " | ".join(r["ambiguities"])
                v["evidence"] = " || ".join(_ev_text(e) for e in r["evidence"])
                v["nortaxa_vernacular_no"] = "; ".join(r["nortaxa_vernacular_no"])
                v["col_vernacular_no"] = "; ".join(r["col_vernacular_no"])
                v["flags"] = "; ".join(r["flags"])
                cw.writerow([v[c] if v[c] is not None else "" for c in CSV_COLS])

    groups = ("verified", "pragmatic_compatible", "pragmatic_missing_authorship", "unresolved")
    for g in groups:
        write(g, [r for r in results if r["outcome"] == g])
    # transition from pass 2
    def p2key(r):
        o, rs = r["pass2_outcome"], r["pass2_reason"] or ""
        return o if o == "verified" else f"{o}:{rs.split(':')[0]}"
    def comp_change(r):
        a, b = (r["pass2_accepted_author_cmp"] or "").split(":")[0], av.level(r["accepted_author_cmp"])
        return "" if a == b else f"{a}->{b}"
    trans = collections.Counter((p2key(r), r["outcome"], r["subclass"], r["reason"].split(":")[0] if r["outcome"] == "unresolved"
                                 and r["subclass"] in ("conflict", "placement_review", "ambiguity") else r["reason"],
                                 comp_change(r)) for r in results)
    with (out / "transition_from_pass2.csv").open("w", newline="", encoding="utf-8-sig") as f:
        cw = csv.writer(f)
        cw.writerow(["pass2", "pass3_outcome", "pass3_subclass", "pass3_reason", "accepted_author_cmp_change", "pairs"])
        for k, n in sorted(trans.items(), key=lambda x: (x[0][0], x[0][1], -x[1])):
            cw.writerow(list(k) + [n])
    with (out / "transition_pairs_from_pass2.csv").open("w", newline="", encoding="utf-8-sig") as f:
        cw = csv.writer(f)
        cw.writerow(["pass2_outcome", "pass2_reason", "pass3_outcome", "pass3_subclass", "pass3_reason",
                     "accepted_author_cmp_change", "scientific_name", "nortaxa_taxon_id", "nortaxa_sporely_taxon_id",
                     "col_usage_id", "col_sporely_taxon_id", "nortaxa_authorship", "col_authorship",
                     "checklistbank_source_record", "ambiguities", "contradictions"])
        for r in sorted(results, key=lambda r: (r["pass2_outcome"] or "", r["outcome"], r["reason"], r["scientific_name"])):
            cw.writerow([r["pass2_outcome"], r["pass2_reason"], r["outcome"], r["subclass"], r["reason"], comp_change(r),
                         r["scientific_name"], r["nortaxa_taxon_id"], r["nortaxa_sporely_taxon_id"], r["col_usage_id"],
                         r["col_sporely_taxon_id"], r["nortaxa_authorship"], r["col_authorship"],
                         "; ".join(r["checklistbank_source_record"]), " | ".join(r["ambiguities"]),
                         " | ".join(r["contradictions"] + r["higher_classification_differs"])])
    ev_all = [e for r in results for e in r["evidence"]]
    clb_pairs = collections.Counter()
    for r in results:
        for c in set(r["checklistbank_source_record"]):
            clb_pairs[c] += 1
    summary = {
        "outcomes": dict(collections.Counter(r["outcome"] for r in results)),
        "by_outcome_subclass": {f"{k[0]}|{k[1]}": v for k, v in sorted(collections.Counter(
            (r["outcome"], r["subclass"]) for r in results).items())},
        "by_outcome_reason": {f"{k[0]}|{k[1]}": v for k, v in sorted(collections.Counter(
            (r["outcome"], r["reason"] if r["subclass"] not in ("conflict", "placement_review", "ambiguity")
             else r["reason"].split(":")[0] + ":" + r["reason"].split(":", 1)[1]) for r in results).items())},
        "pass2_to_pass3": {f"{k[0]}|{k[1]}": v for k, v in sorted(collections.Counter(
            (r["pass2_outcome"], r["outcome"]) for r in results).items())},
        "checklistbank_source_record_pairs_by_subclass": dict(sorted(clb_pairs.items())),
        "checklistbank_source_record_pairs_by_subclass_and_outcome": {f"{k[0]}|{k[1]}": v for k, v in sorted(collections.Counter(
            (c, r["outcome"]) for r in results for c in set(r["checklistbank_source_record"])).items())},
        "identifier_cross_references": sum(1 for e in ev_all if e["kind"].startswith("identifier_xref")),
        "evidence_items": dict(sorted(collections.Counter(
            e["kind"] + "|" + str(e.get("subclass") or e.get("provenance_class") or "") for e in ev_all).items())),
        "witness_exclusions": dict(sorted(collections.Counter(
            x.split(":")[0] for e in ev_all for x in e.get("excluded_because", [])).items())),
        "verified_witness_author_exception_pairs": sum(1 for r in results if r["outcome"] == "verified" and any(
            f.startswith("verified_witness_author_exception") for f in r["flags"])),
        "verified_accepted_author_candidate_pairs": dict(collections.Counter(
            f for r in results if r["outcome"] == "verified" for f in r["flags"] if f.startswith("accepted_author_candidate"))),
        "pairs_with_independent_vernacular": sum(1 for r in results if any(e.get("independent_corroboration") for e in r["evidence"])),
        "copied_vernacular_occurrences": sum(1 for e in ev_all if e.get("provenance") == "copied_from_nortaxa"),
        "copied_vernacular_pairs": sum(1 for r in results if any(e.get("provenance") == "copied_from_nortaxa" for e in r["evidence"])),
        "blank_source_vernacular_occurrences": sum(1 for e in ev_all if e.get("provenance") == "unverified_blank_source"),
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({k: summary[k] for k in ("outcomes", "by_outcome_subclass", "pass2_to_pass3",
                                              "checklistbank_source_record_pairs_by_subclass")}, indent=1))


if __name__ == "__main__":
    raise SystemExit(main())
