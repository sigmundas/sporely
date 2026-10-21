#!/usr/bin/env python3
"""Relationship pass 2 (focused correction after relationship-pass/review-codex.md).

Same 2,805 pairs and inputs as pass 1; pass 1's outputs are read only for the
transition table. Changes:
 1. Provenance: every COL row carries its sourceID. Source 2030 is NorTaxa
    (Artsnavnebasen) itself: such rows are 'copied_from_nortaxa' and never
    independent corroboration. Blank sourceID on a vernacular = unverified.
 2. Verified homotypy needs a DOCUMENTED link: COL basionymID, COL NameRelation
    basionym / homotypic / based-on, or Dyntaxa homotypicSynonym status (Dyntaxa
    reached through COL's own dyntaxa: identifier). Equal basionym authors are
    never enough.
 3. Witnesses that are ambiguous synonyms, misapplied, COL-unacceptable /
    doubtful / manuscript / not established / rejected, or carry a qualifier
    or unpublished marker never support 'verified'.
 4. Author identities via authors_v2.py (initials, sanctioning vs validating).
 5. 'Explicit cross-reference' is renamed 'synonym_listing_match'. Identifier
    cross-references (a shared identifier published by both sources) are
    counted separately.
 6. Vernaculars are support only, with provenance.

Outputs (UTF-8 with BOM CSVs + JSONL) in --out-dir:
  verified_candidates.{csv,jsonl}   independently supported, unqualified links
  pragmatic_candidates.{csv,jsonl}  name/rank match, compatible authorship or
                                    history, no known contradiction, but not
                                    independently verified
  unresolved_pairs.{csv,jsonl}      conflict / ambiguity / everything else
  transition_from_pass1_566.csv, summary.json
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
V2 = Path(__file__).resolve().parent
sys.path.insert(0, str(V2))
sys.path.insert(0, str(V2.parents[2] / "scripts"))
import authors_v2 as av  # noqa: E402
import authorship as au  # noqa: E402

NORTAXA_SOURCE = "2030"
COL_BAD_STATUS = {"unacceptable", "doubtful", "manuscript", "not established", "rejected"}
NT_BAD_STATUS = {"illegitimate", "dubious", "notvalidlypublished", "orthographic", "misapplied"}
OK = ("identical", "compatible")


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


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    for x in ("csv", "pairs", "pass1", "workdb", "relations", "release", "manual-mappings", "out-dir"):
        ap.add_argument("--" + x, required=True, type=Path)
    a = ap.parse_args(argv)
    a.out_dir.mkdir(parents=True, exist_ok=True)
    rows = list(csv.DictReader(a.csv.open(encoding="utf-8-sig")))
    recs = {}
    for line in gzip.open(a.pairs, "rt"):
        r = json.loads(line)
        recs[(str(r["nortaxa_sporely_taxon_id"]), str(r["col_sporely_taxon_id"]))] = r
    pass1 = {(r["nortaxa_taxon_id"], r["col_usage_id"]): r["bucket"]
             for r in map(json.loads, a.pass1.open())}
    w = sqlite3.connect(f"file:{a.workdb}?mode=ro", uri=True)
    w.row_factory = sqlite3.Row
    rl = sqlite3.connect(f"file:{a.relations}?mode=ro", uri=True)
    rel = sqlite3.connect(f"file:{a.release}?mode=ro", uri=True)

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

    def acc_of(u):
        return u["id"] if u["status"] in ("accepted", "provisionally accepted") else u["parent_id"]

    def qualified(s):
        return bool(au.flags(s or ""))

    results, counts, trans = [], collections.Counter(), collections.Counter()
    for row in rows:
        nid, cid = row["nortaxa_taxon_id"], row["col_usage_id"]
        nsid, csid = row["nortaxa_sporely_taxon_id"], row["col_sporely_taxon_id"]
        N, C = nt[nid], col[cid]
        rec = recs.get((nsid, csid), {})
        B = col.get(C["basionym_id"]) if C["basionym_id"] else None
        ev, contra, ambi, flags = [], [], [], []
        acmp = av.compare(N["authorship"], C["authorship"])
        bcmp = av.basionym_cmp(N["authorship"], C["authorship"])

        # ---------------- witnesses: synonyms published by both sides
        cands = list(col_children.get(cid, []))
        if B and B not in cands:
            cands.append(B)
        dyn_links = [f"urn:lsid:dyntaxa.se:Taxon:{t.rsplit(':', 1)[1]}"
                     for t in (C["alt_id"] or "").split(",") if t.startswith("dyntaxa:")]
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
                        if dr[6] == "homotypicSynonym" and unicodedata.normalize("NFC", dr[3]).casefold() == s["nkey"] \
                                and av.compare(dr[5], s["authorship"]) in OK + ("candidate",):
                            docs.append({"link": "dyntaxa:homotypicSynonym", "row": ["dyntaxa:Taxon.csv", dr[0]],
                                         "source_id": "dyntaxa"})
                bad = []
                if u["status"] != "synonym":
                    bad.append("col_status:" + u["status"])
                if (u["name_status"] or "") in COL_BAD_STATUS:
                    bad.append("col_name_status:" + u["name_status"])
                if qualified(s["authorship"]) or qualified(u["authorship"]):
                    bad.append("qualified_or_unpublished")
                both_comb = bool(av.split_paren(s["authorship"])[1]) and bool(av.split_paren(u["authorship"])[1])
                if not (cmp in OK or (cmp == "candidate" and both_comb
                                      and av.basionym_cmp(s["authorship"], u["authorship"]) in OK)):
                    bad.append("witness_authorship:" + cmp)
                col_prov = prov(u["source_id"])
                indep_docs = [d for d in docs if d["source_id"] == "dyntaxa" or
                              (prov(d["source_id"]) == "original" and col_prov == "original")]
                kind = ("homotypic_documented" if docs else "shared_synonym_undocumented")
                ev.append({"kind": kind, "nortaxa_row": ["nortaxa:taxon.txt", s["taxon_id"]],
                           "col_row": ["col:NameUsage.tsv", u["id"]], "witness": f"{s['name']} {s['authorship']}",
                           "col_witness": f"{u['name']} {u['authorship']}", "witness_author_cmp": cmp,
                           "col_row_provenance": col_prov, "nortaxa_nom_status": s["nom_status"] or "",
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
                            and av.compare(dr[5], s["authorship"]) in OK:
                        ev.append({"kind": "homotypic_documented", "nortaxa_row": ["nortaxa:taxon.txt", s["taxon_id"]],
                                   "col_row": None, "witness": f"{s['name']} {s['authorship']}",
                                   "documentation": [{"link": "dyntaxa:homotypicSynonym", "row": ["dyntaxa:Taxon.csv", dr[0]],
                                                      "source_id": "dyntaxa"}],
                                   "excluded_because": ["qualified_or_unpublished"] if qualified(s["authorship"]) else [],
                                   "source_verified": True, "col_row_provenance": None,
                                   "verified_eligible": not qualified(s["authorship"]),
                                   "independent_documentation": ["dyntaxa:homotypicSynonym"]})
        # ---------------- synonym-listing match (formerly 'explicit cross-reference')
        for u in col_by_nkey[N["nkey"]]:
            if u["id"] != cid and acc_of(u) == cid and u["status"] == "synonym" \
                    and av.compare(u["authorship"], N["authorship"]) in OK and not qualified(u["authorship"]):
                ev.append({"kind": "synonym_listing_match", "col_row": ["col:NameUsage.tsv", u["id"]],
                           "witness": f"{u['name']} {u['authorship']}", "col_row_provenance": prov(u["source_id"]),
                           "source_verified": True, "verified_eligible": False, "documentation": [],
                           "note": "COL lists NorTaxa's name and authorship as a synonym of the COL concept (name lookup, not an identifier)"})
        # ---------------- identifier cross-reference (shared identifier from both sources)
        id_xref = [mid for tgt, mid in mm.get(nid, []) if tgt == cid]
        for mid in id_xref:
            ev.append({"kind": "identifier_xref_manual_mapping", "row": ["repo:manual_mappings", mid],
                       "source_verified": True, "verified_eligible": True})
        cinat = {int(t[5:]) for t in (C["alt_id"] or "").split(",") if t.startswith("inat:") and t[5:].isdigit()}
        for e, note in ext.get(int(nsid), []):
            if e in cinat:
                ev.append({"kind": "inaturalist_id_agrees", "source_verified": False, "verified_eligible": False,
                           "rows": [["col:NameUsage.tsv", cid], ["release:taxon_external_id_min", f"{nsid}/inaturalist/{e}"]],
                           "note": "NorTaxa-side id is " + (note or "release row") + " (not published by NorTaxa)"})
        # ---------------- vernaculars (support only)
        nv = nt_vern.get(nid, set())
        cv = col_vern.get(cid, set())
        nv_names = {x.split(":", 1)[1].casefold() for x in nv}
        shared = [(n_, l_, s_) for n_, l_, s_ in cv if n_.casefold() in nv_names]
        for n_, l_, s_ in sorted(shared):
            p = "copied_from_nortaxa" if s_ == NORTAXA_SOURCE else ("unverified_blank_source" if not s_ else f"independent:col_source_{s_}")
            ev.append({"kind": "shared_norwegian_vernacular", "name": n_, "provenance": p,
                       "rows": [["nortaxa:vernacularname.txt", nid], ["col:VernacularName.tsv", cid]],
                       "source_verified": True, "verified_eligible": False,
                       "independent_corroboration": p.startswith("independent")})

        # ---------------- contradictions / ambiguities
        if N["rank"] != (C["rank"] or "").lower():
            contra.append("rank_differs")
        for u in col_by_nkey[N["nkey"]]:
            if u["id"] != cid and (u["rank"] or "").lower() == N["rank"] and acc_of(u) != cid \
                    and av.compare(u["authorship"], N["authorship"]) in OK:
                contra.append(f"col_places_nortaxa_name_elsewhere:{u['id']}->{acc_of(u)}")
        for s in nt_by_nkey[C["nkey"]]:
            if s["taxon_id"] != nid and s["rank"] == N["rank"] and s["accepted_id"] != nid \
                    and av.compare(s["authorship"], C["authorship"]) in OK:
                contra.append(f"nortaxa_places_col_name_elsewhere:{s['taxon_id']}->{s['accepted_id']}")
        if rec.get("inaturalist") == "conflict":
            contra.append("inaturalist_id_conflict")
        if rec.get("artportalen_vs_dyntaxa") == "conflict":
            contra.append("artportalen_dyntaxa_id_conflict")
        if bcmp == "different":
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
        if "role_mismatch" in (acmp, bcmp):
            ambi.append("author_role_mismatch(sanctioning_vs_validating_or_ex)")

        if rec.get("classification_disagree"):
            flags.append("placement_differs:" + "+".join(rec["classification_disagree"]))

        # ---------------- outcome
        verified_w = [e for e in ev if e.get("verified_eligible")]
        reconciled = acmp in OK + ("candidate",)
        attribution_only = acmp == "different" and bcmp in OK
        if attribution_only:
            flags.append("combining_author_attribution_differs")
        # Misspelt accepted author (Entoloma sphagneti 'Navaeu'/'Naveau'): only when
        # neither accepted name is a recombination, the two author strings are
        # within edit distance 2 after folding, and a verified-eligible NorTaxa
        # homotypic synonym cites COL's author as its basionym author.
        if not reconciled and acmp == "different" and not av.split_paren(N["authorship"])[0] \
                and not av.split_paren(C["authorship"])[0] and _lev(_key(N["authorship"]), _key(C["authorship"])) <= 2:
            for e in verified_w:
                sb = av.split_paren(nt[e["nortaxa_row"][1]]["authorship"])[0]
                if sb and av.part_cmp(av.parse_part(sb), av.parse_part(C["authorship"])) in OK:
                    reconciled = True
                    flags.append("accepted_author_misspelling_explained_by_nortaxa_own_homotypic_synonym")
                    break
        if acmp == "different" and bcmp is None and not reconciled and av.split_paren(N["authorship"])[0] is None \
                and av.split_paren(C["authorship"])[0] is None:
            ambi.append("possible_homonym_different_authors")
        if contra:
            outcome, reason = "unresolved", "conflict:" + ";".join(sorted({c.split(":")[0] for c in contra}))
        elif higher:
            outcome, reason = "unresolved", "placement_review:" + "+".join(f"{f}_differs" for f in higher)
        elif ambi:
            outcome, reason = "unresolved", "ambiguity:" + ";".join(sorted({x.split(":")[0] for x in ambi}))
        elif verified_w and reconciled:
            outcome = "verified"
            reason = "+".join(sorted({d for e in verified_w for d in e.get("independent_documentation", [])} |
                                     ({"identifier_xref"} if id_xref else set())))
        elif verified_w and attribution_only:
            outcome, reason = "pragmatic", "documented_homotypy_but_combining_author_attribution_differs"
        elif verified_w:
            outcome, reason = "unresolved", "accepted_authors_differ_despite_documented_homotypy"
        elif acmp in OK:
            outcome, reason = "pragmatic", "authorship_" + acmp
        elif acmp == "candidate":
            outcome, reason = "pragmatic", "authorship_candidate(needs_author_standard)"
        elif attribution_only:
            outcome, reason = "pragmatic", "same_basionym_author_combining_author_attribution_differs"
        else:
            kinds = {e["kind"] for e in ev}
            outcome = "unresolved"
            reason = ("accepted_authors_differ;shared_synonymy_undocumented_or_ineligible"
                      if kinds & {"homotypic_documented", "shared_synonym_undocumented"} else "accepted_authors_differ;name_only")
        if outcome == "pragmatic":
            sup = sorted({e["kind"] + ("[" + e.get("col_row_provenance", "") + "]" if e.get("col_row_provenance") else "")
                          for e in ev if e["kind"] not in ("shared_norwegian_vernacular",)})
            flags.append("support:" + ",".join(sup) if sup else "support:name_rank_and_authorship_only")
        counts[(outcome, reason.split(":")[0] if outcome == "unresolved" else reason)] += 1
        p1 = pass1.get((nid, cid))
        if p1 in ("B1_explicit_xref", "B2_shared_homotypic"):
            trans[(p1, outcome, reason)] += 1
        results.append({"outcome": outcome, "reason": reason, "scientific_name": row["scientific_name"],
                        "nortaxa_taxon_id": nid, "nortaxa_sporely_taxon_id": int(nsid),
                        "col_usage_id": cid, "col_sporely_taxon_id": int(csid),
                        "nortaxa_authorship": N["authorship"], "col_authorship": C["authorship"],
                        "accepted_author_cmp": acmp, "basionym_author_cmp": bcmp,
                        "col_source_id": C["source_id"], "col_basionym": f"{B['id']} {B['name']} {B['authorship']}" if B else "",
                        "current_category": row["category"] + (":" + row["b_subclass"] if row["b_subclass"] else ""),
                        "pass1_bucket": p1, "contradictions": contra, "ambiguities": ambi, "higher_classification_differs": higher,
                        "evidence": ev, "flags": flags,
                        "nortaxa_vernacular_no": sorted(nv),
                        "col_vernacular_no": sorted(f"{l_}:{n_}[{'copied_from_nortaxa' if s_ == NORTAXA_SOURCE else ('unverified_blank_source' if not s_ else 'col_source_' + s_)}]"
                                                    for n_, l_, s_ in cv)})

    def write(name, rs):
        rs = sorted(rs, key=lambda r: (r["reason"], r["scientific_name"]))
        with (a.out_dir / f"{name}.jsonl").open("w", encoding="utf-8") as f:
            for r in rs:
                f.write(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n")
        with (a.out_dir / f"{name}.csv").open("w", newline="", encoding="utf-8-sig") as f:
            cw = csv.writer(f)
            cw.writerow(["outcome", "reason", "scientific_name", "nortaxa_taxon_id", "nortaxa_sporely_taxon_id",
                         "nortaxa_authorship", "col_usage_id", "col_sporely_taxon_id", "col_authorship",
                         "accepted_author_cmp", "basionym_author_cmp", "col_source_id", "col_basionym",
                         "current_category", "pass1_bucket", "contradictions", "ambiguities", "evidence",
                         "nortaxa_vernacular_no", "col_vernacular_no", "flags"])
            for r in rs:
                evs = []
                for e in r["evidence"]:
                    refs = [x for x in (e.get("nortaxa_row"), e.get("col_row"), e.get("row")) if x] + \
                        [d["row"] for d in e.get("documentation", [])] + (e.get("rows") or [])
                    tag = e["kind"]
                    if e.get("col_row_provenance"):
                        tag += f"[{e['col_row_provenance']}]"
                    if e.get("provenance"):
                        tag += f"[{e['provenance']}]"
                    if e.get("excluded_because"):
                        tag += "{excluded:" + ",".join(e["excluded_because"]) + "}"
                    tag += f"(source_verified={e.get('source_verified')},verified_eligible={e.get('verified_eligible')})"
                    evs.append(tag + " @ " + " ".join(f"{x[0]}#{x[1]}" for x in refs))
                cw.writerow([r["outcome"], r["reason"], r["scientific_name"], r["nortaxa_taxon_id"],
                             r["nortaxa_sporely_taxon_id"], r["nortaxa_authorship"], r["col_usage_id"],
                             r["col_sporely_taxon_id"], r["col_authorship"], r["accepted_author_cmp"],
                             r["basionym_author_cmp"], r["col_source_id"], r["col_basionym"], r["current_category"],
                             r["pass1_bucket"] or "", " | ".join(r["contradictions"] + r["higher_classification_differs"]),
                             " | ".join(r["ambiguities"]), " || ".join(evs), "; ".join(r["nortaxa_vernacular_no"]),
                             "; ".join(r["col_vernacular_no"]), "; ".join(r["flags"])])

    write("verified_candidates", [r for r in results if r["outcome"] == "verified"])
    write("pragmatic_candidates", [r for r in results if r["outcome"] == "pragmatic"])
    write("unresolved_pairs", [r for r in results if r["outcome"] == "unresolved"])
    with (a.out_dir / "transition_from_pass1_566.csv").open("w", newline="", encoding="utf-8-sig") as f:
        cw = csv.writer(f)
        cw.writerow(["pass1_bucket", "pass2_class", "pass2_outcome", "pass2_reason", "pairs"])
        coarse = lambda o, rsn: o if o != "unresolved" else ("conflict" if rsn.startswith("conflict") else "review")
        for (p1, o, rsn), n in sorted(trans.items(), key=lambda x: (x[0][0], x[0][1], -x[1])):
            cw.writerow([p1, coarse(o, rsn), o, rsn, n])
    with (a.out_dir / "transition_pairs_from_pass1_566.csv").open("w", newline="", encoding="utf-8-sig") as f:
        cw = csv.writer(f)
        cw.writerow(["pass1_bucket", "pass2_class", "pass2_outcome", "pass2_reason", "scientific_name",
                     "nortaxa_taxon_id", "nortaxa_sporely_taxon_id", "col_usage_id", "col_sporely_taxon_id",
                     "nortaxa_authorship", "col_authorship", "flags"])
        for r in sorted((r for r in results if r["pass1_bucket"] in ("B1_explicit_xref", "B2_shared_homotypic")),
                        key=lambda r: (r["pass1_bucket"], r["outcome"], r["reason"], r["scientific_name"])):
            cls = r["outcome"] if r["outcome"] != "unresolved" else (
                "conflict" if r["reason"].startswith("conflict") else "review")
            cw.writerow([r["pass1_bucket"], cls, r["outcome"], r["reason"], r["scientific_name"], r["nortaxa_taxon_id"],
                         r["nortaxa_sporely_taxon_id"], r["col_usage_id"], r["col_sporely_taxon_id"],
                         r["nortaxa_authorship"], r["col_authorship"], "; ".join(r["flags"])])
    ev_all = [e for r in results for e in r["evidence"]]
    summary = {
        "outcomes": dict(collections.Counter(r["outcome"] for r in results)),
        "by_outcome_reason": {f"{k[0]}|{k[1]}": v for k, v in sorted(counts.items())},
        "pass1_566": {f"{k[0]}|{k[1]}": v for k, v in sorted(collections.Counter(
            (r["pass1_bucket"], r["outcome"]) for r in results if r["pass1_bucket"] in ("B1_explicit_xref", "B2_shared_homotypic")).items())},
        "pass1_566_coarse": dict(collections.Counter(
            f"{p1}|{(o if o != 'unresolved' else ('conflict' if rsn.startswith('conflict') else 'review'))}"
            for (p1, o, rsn), n in trans.items() for _ in range(n))),
        "identifier_cross_references": sum(1 for e in ev_all if e["kind"].startswith("identifier_xref")),
        "evidence_items": dict(collections.Counter(
            e["kind"] + ("|" + (e.get("col_row_provenance") or e.get("provenance") or "") if e.get("col_row_provenance") or e.get("provenance") else "")
            for e in ev_all)),
        "witness_exclusions": dict(collections.Counter(x.split(":")[0] for e in ev_all for x in e.get("excluded_because", []))),
        "pairs_with_independent_vernacular": sum(1 for r in results if any(e.get("independent_corroboration") for e in r["evidence"])),
    }
    (a.out_dir / "summary.json").write_text(json.dumps(summary, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=1, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
