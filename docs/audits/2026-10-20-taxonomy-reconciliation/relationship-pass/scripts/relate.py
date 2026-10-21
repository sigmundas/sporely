#!/usr/bin/env python3
"""Relationship pass, step 2: grade every species pair that did not make S1
(data/species-not-merged.csv) on SOURCE RELATIONSHIPS first:
  accepted-name resolution in each source, COL basionymID / NameRelation
  homotypy, synonymy published by both sources, nomenclatural status,
  external identifiers, Dyntaxa as a third witness, and only then vernacular
  names and authorship strings.

Read-only. Inputs: the not-merged CSV, the cross-source pair records
(manifests/cross-source-pairs.all.jsonl.gz), the audit work DB, the
relations DB from extract_relations.py, the release copy, the release
evidence inputs (manual_mappings.json). Outputs go to --out-dir only.

Disjoint primary buckets, precedence top to bottom:
  X1_conflict            contradictory evidence (competing placement in either
                         source, external-id conflict, different basionym,
                         class/phylum disagreement, different rank)
  X2_ambiguity           not one-to-one, blocked nomenclatural status on
                         either accepted name, COL provisional
  B1_explicit_xref       a source lists the OTHER source's exact accepted
                         name+authorship as a usage of its own concept
  B2_shared_homotypic    a synonym published by BOTH sources that is
                         homotypic with the COL concept (COL basionymID /
                         NameRelation, or its basionym author is the accepted
                         name's author)
  B3_basionym_inferred   same epithet and agreeing basionym (parenthetical)
                         author on both sides; COL basionym row where present;
                         NorTaxa side inferred from the authorship string
  B4_heterotypic_or_uncertain  only non-homotypic shared synonymy
  B5_accepted_plus_vernacular  same accepted name, compatible classification,
                         an independent shared Norwegian vernacular (NorTaxa
                         vernacularname.txt vs COL VernacularName.tsv), not a
                         reused / genus-level / aggregate name
  B6_normalised_authorship_candidate  authorship equal after documented
                         folding, or abbreviation-prefix (A1): candidate only,
                         no local author standard exists
  R_residual             name equality only (4-letter overlap is flagged as a
                         discovery signal, never evidence)
"""
from __future__ import annotations

import argparse
import collections
import csv
import gzip
import json
import re
import sqlite3
import sys
import unicodedata
from pathlib import Path

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(HERE))
import authorship as au  # noqa: E402

BUCKETS = ["X1_conflict", "X2_ambiguity", "B1_explicit_xref", "B2_shared_homotypic",
           "B3_basionym_inferred", "B4_heterotypic_or_uncertain", "B5_accepted_plus_vernacular",
           "B6_normalised_authorship_candidate", "R_residual"]
AGG = re.compile(r"\b(coll|agg|gruppe|kompleks|komplekset|slekt|arter)\b|-arter$|gruppa$", re.I)
PARTICLES = {"de", "des", "du", "van", "von", "der", "la", "le", "d", "di", "da"}


def fold(s: str) -> str:
    s = (s or "").replace("ð", "d").replace("Ð", "D").replace("ø", "o").replace("Ø", "O") \
        .replace("æ", "ae").replace("Æ", "Ae").replace("ß", "ss").replace("þ", "th")
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c)).lower()


def paren(a: str):
    """(basionym author, combination author) from an authorship string."""
    m = re.match(r"^\s*\((.+?)\)\s*(.*)$", a or "")
    return (m.group(1).strip(), m.group(2).strip()) if m else (None, (a or "").strip())


def _publishing(part: str) -> str:
    """'A ex B' -> 'B' (B validly published the name)."""
    part = part or ""
    return re.split(r"\bex\b", part)[-1] if re.search(r"\bex\b", part) else part


def author_tokens(a: str) -> list[str]:
    return [t for t in re.split(r"[^a-z]+", fold(_publishing(a))) if t and t not in PARTICLES and len(t) > 1]


def _sanction_parts(a: str) -> list[str]:
    """'Alb. & Schwein. : Fr.' -> ['Alb. & Schwein.', 'Fr.'] (ICN sanctioning notation)."""
    return [x.strip() for x in (a or "").split(":") if x.strip()] or [""]


def _lev(x: str, y: str) -> int:
    prev = list(range(len(y) + 1))
    for i, cx in enumerate(x, 1):
        cur = [i]
        for j, cy in enumerate(y, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (cx != cy)))
        prev = cur
    return prev[-1]


def same_author(a: str, b: str) -> bool:
    """Same author(s): comparator-compatible, equal after documented folding
    (diacritics, punctuation, particles), or equal on either side of the ICN
    sanctioning colon."""
    if not a or not b:
        return False
    if au.compatible(a, b):
        return True
    for x in _sanction_parts(a):
        for y in _sanction_parts(b):
            if author_tokens(x) and author_tokens(x) == author_tokens(y):
                return True
    return False


def _all_tokens(a: str) -> list[str]:
    return [t for t in re.split(r"[^a-z]+", fold(a)) if t and t not in PARTICLES and t != "ex" and len(t) > 1]


def _rel(x: str, y: str) -> bool:
    return x == y or ((x.startswith(y) or y.startswith(x)) and min(len(x), len(y)) >= 3)


def variant_author(a: str, b: str) -> str | None:
    """Candidate-only relation between two author strings: abbreviation
    prefix, small spelling distance, order, partial list or ex-author
    difference. None means the strings share no author at all."""
    ta, tb = author_tokens(a), author_tokens(b)
    if not ta or not tb:
        return None
    if len(ta) == len(tb) and all(_rel(x, y) for x, y in zip(ta, tb)):
        return "abbreviation_prefix"
    ja, jb = "".join(ta), "".join(tb)
    if _lev(ja, jb) <= 2 and min(len(ja), len(jb)) >= 5:
        return "spelling_distance_le2"
    sa, sb = _all_tokens(a), _all_tokens(b)
    ina = [x for x in sa if any(_rel(x, y) for y in sb)]
    inb = [y for y in sb if any(_rel(x, y) for x in sa)]
    if len(ina) == len(sa) and len(inb) == len(sb):
        return "same_authors_other_order"
    if len(ina) == len(sa) or len(inb) == len(sb):
        return "partial_list_or_ex_author"
    if ina:
        return "partial_overlap"
    return None


def norm_author_candidate(a: str, b: str) -> str | None:
    """'folded_equal' or a variant relation over the WHOLE authorship
    (basionym and combination parts). Candidate only."""
    pa, pb = paren(a), paren(b)
    if (pa[0] is None) != (pb[0] is None):
        return None
    if author_tokens(a) == author_tokens(b) and author_tokens(a):
        return "folded_equal"
    parts = [variant_author(x or "", y or "") if (x or y) else "equal" for x, y in zip(pa, pb)]
    if all(p for p in parts) and any(p != "equal" for p in parts):
        return next(p for p in parts if p != "equal")
    return None


def four_letter(a: str, b: str) -> bool:
    ta, tb = author_tokens(paren(a)[1]), author_tokens(paren(b)[1])
    return bool(ta and tb and ta[-1] != tb[-1] and len(ta[-1]) >= 4 and len(tb[-1]) >= 4
                and ta[-1][:4] == tb[-1][:4])


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    for x in ("csv", "pairs", "workdb", "relations", "release", "manual-mappings", "out-dir"):
        ap.add_argument("--" + x, required=True, type=Path)
    a = ap.parse_args(argv)
    a.out_dir.mkdir(parents=True, exist_ok=True)
    rows = list(csv.DictReader(a.csv.open()))
    recs = {}
    for line in gzip.open(a.pairs, "rt"):
        r = json.loads(line)
        recs[(str(r["nortaxa_sporely_taxon_id"]), str(r["col_sporely_taxon_id"]))] = r
    w = sqlite3.connect(f"file:{a.workdb}?mode=ro", uri=True)
    w.row_factory = sqlite3.Row
    rl = sqlite3.connect(f"file:{a.relations}?mode=ro", uri=True)
    rel = sqlite3.connect(f"file:{a.release}?mode=ro", uri=True)

    col = {r["id"]: dict(r) for r in w.execute(
        "select id, alt_id, source_id, parent_id, basionym_id, status, name, nkey, authorship, rank, "
        "comb_auth, bas_auth, name_status, class, phylum, link from col_raw")}
    col_children, col_by_bas, col_by_nkey = (collections.defaultdict(list) for _ in range(3))
    for r in col.values():
        if r["status"] not in ("accepted", "provisionally accepted"):
            col_children[r["parent_id"]].append(r)
        if r["basionym_id"]:
            col_by_bas[r["basionym_id"]].append(r)
        col_by_nkey[r["nkey"]].append(r)
    col_rel = collections.defaultdict(list)
    for x, y, t in rl.execute("select nameID, relatedNameID, type from col_rel"):
        col_rel[x].append((y, t))
        col_rel[y].append((x, t))
    nt = {r["taxon_id"]: dict(r) for r in w.execute(
        "select taxon_id, accepted_id, name, nkey, authorship, rank, status, nom_status, class, phylum from nt_raw")}
    nt_syn, nt_by_nkey = collections.defaultdict(list), collections.defaultdict(list)
    for r in nt.values():
        if r["accepted_id"] and r["accepted_id"] != r["taxon_id"]:
            nt_syn[r["accepted_id"]].append(r)
        nt_by_nkey[r["nkey"]].append(r)
    nt_id_of_row = dict(rl.execute('select "id", "taxonID" from nt_taxon'))
    nt_pub = dict(rl.execute('select "taxonID", "namePublishedIn" from nt_taxon'))
    nt_vern = collections.defaultdict(set)
    for rid, name, lang in rl.execute('select "id", "vernacularName", "language" from nt_vern'):
        if lang in ("nb", "nn", "no"):
            nt_vern[nt_id_of_row.get(rid, rid)].add(name.strip().casefold())
    # COL source 2030 is NorTaxa (Artsnavnebasen) itself: its vernaculars are
    # copies, not independent evidence.
    col_vern, col_vern_copy = collections.defaultdict(set), collections.defaultdict(set)
    for tid, lang, name, src in rl.execute("select taxonID, language, name, sourceID from col_vern"):
        (col_vern_copy if src == "2030" else col_vern)[tid].add(name.strip().casefold())
    # vernacular reuse: how many NorTaxa accepted concepts carry each name; genus-level names
    vern_concepts = collections.defaultdict(set)
    genus_vern = set()
    for tid, names in nt_vern.items():
        r = nt.get(tid)
        if not r or r["status"] != "valid":
            continue
        for n_ in names:
            vern_concepts[n_].add(tid)
            if r["rank"] in ("genus", "family", "order", "subgenus", "section"):
                genus_vern.add(n_)
    dyn = {}
    for row in rl.execute('select * from dyn_taxon'):
        dyn[row[0]] = row
    dyn_by_acc = collections.defaultdict(list)
    for row in dyn.values():
        dyn_by_acc[row[1]].append(row)
    ext = collections.defaultdict(list)
    for sid, ss, e, role, note in rel.execute(
            "select taxon_id, source_system, external_id, id_role, note from taxon_external_id_min "
            "where source_system in ('inaturalist','artportalen')"):
        ext[sid].append((ss, e, role, note or ""))
    mm = collections.defaultdict(list)
    for m in json.loads(a.manual_mappings.read_text())["mappings"]:
        su, tg = m["source_usage"], m["target"]["source_usage"]
        if su["source"] == "nortaxa":
            mm[su["identifier"]].append((tg["identifier"], m["review_status"], m["mapping_id"]))

    def acc_of_col(u):
        return u["id"] if u["status"] in ("accepted", "provisionally accepted") else u["parent_id"]

    out, buckets, anyev, trans = [], collections.Counter(), collections.Counter(), collections.Counter()
    for row in rows:
        nid, cid = row["nortaxa_taxon_id"], row["col_usage_id"]
        nsid, csid = row["nortaxa_sporely_taxon_id"], row["col_sporely_taxon_id"]
        N, C = nt[nid], col[cid]
        rec = recs.get((nsid, csid), {})
        ev, contra, ambi, flags = [], [], [], []

        def add(kind, verified, refs, note=""):
            ev.append({"kind": kind, "source_verified": verified, "rows": refs, "note": note})

        # ---- resolution of each name in the other source
        col_res = {}
        for u in col_by_nkey[N["nkey"]]:
            if u["rank"] and u["rank"].lower() != N["rank"]:
                continue
            col_res.setdefault(acc_of_col(u) if u["status"] != "misapplied" else "misapplied:" + u["parent_id"], []).append(u)
        nt_res = {}
        for s in nt_by_nkey[C["nkey"]]:
            if s["rank"] != N["rank"]:
                continue
            nt_res.setdefault(s["accepted_id"], []).append(s)
        resolution = {"nortaxa_name_in_col": sorted(col_res), "col_name_in_nortaxa": sorted(nt_res)}

        # ---- contradictions
        if N["rank"] != (C["rank"] or "").lower():
            contra.append("rank_differs")
        for acc, us in col_res.items():
            for u in us:
                if acc != cid and u["id"] != cid and au.compatible(u["authorship"], N["authorship"]):
                    contra.append(f"col_places_nortaxa_name_under_{acc}")
                    add("conflict_col_placement", True, [["col:NameUsage.tsv", u["id"]]], f"{u['status']} -> {acc}")
        for acc, ss in nt_res.items():
            for s in ss:
                if acc != nid and au.compatible(s["authorship"], C["authorship"]):
                    contra.append(f"nortaxa_places_col_name_under_{acc}")
                    add("conflict_nortaxa_placement", True, [["nortaxa:taxon.txt", s["taxon_id"]]], f"-> {acc}")
        if rec.get("inaturalist") == "conflict":
            contra.append("inaturalist_id_conflict")
        if rec.get("artportalen_vs_dyntaxa") == "conflict":
            contra.append("artportalen_dyntaxa_id_conflict")
        for f in ("class", "phylum"):
            x, y = (N[f] or "").strip(), (C[f] or "").strip()
            if x and y and not x.lower().startswith("incertae") and not y.lower().startswith("incertae") and x != y:
                contra.append(f"{f}_disagrees")
        pN, cN = paren(N["authorship"])
        pC = paren(C["authorship"])[0] or (C["bas_auth"] or None)
        if pN and pC and not same_author(pN, pC):
            if variant_author(pN, pC):
                flags.append("basionym_author_variant:" + variant_author(pN, pC))
            else:
                contra.append("different_basionym_author")
        if rec.get("classification_disagree"):
            flags.append("order_or_family_disagrees" if set(rec["classification_disagree"]) <= {"ord", "family"}
                         else "classification_disagrees")
        for tgt, st, mid in mm.get(nid, []):
            if tgt != cid:
                contra.append("manual_mapping_to_other_col_usage")
                add("conflict_manual_mapping", True, [["repo:manual_mappings", mid]], f"-> {tgt} ({st})")
        # ---- ambiguity
        if not rec.get("one_to_one", False):
            ambi.append("not_one_to_one")
        nsN, nsC = N["nom_status"] or "", C["name_status"] or ""
        if nsN in ("illegitimate", "dubious", "notvalidlypublished", "orthographic", "misapplied"):
            ambi.append("nortaxa_nomenclatural_status:" + nsN)
        if nsC in ("unacceptable", "doubtful", "manuscript", "not established", "rejected"):
            ambi.append("col_name_status:" + nsC)
        if C["status"] != "accepted":
            ambi.append("col_" + C["status"].replace(" ", "_"))
        if au.compare(N["authorship"], C["authorship"]) in au.NON_BATCHABLE_QUALIFIER:
            ambi.append("qualified_or_unpublished_authorship")

        # ---- B1 explicit cross-reference
        for u in col_res.get(cid, []):
            if u["id"] != cid and au.compatible(u["authorship"], N["authorship"]):
                add("col_lists_nortaxa_string_under_C", True, [["col:NameUsage.tsv", u["id"]]], u["status"])
        for s in nt_res.get(nid, []):
            if s["taxon_id"] != nid and au.compatible(s["authorship"], C["authorship"]):
                add("nortaxa_lists_col_string_under_N", True, [["nortaxa:taxon.txt", s["taxon_id"]]], s["status"])
        for tgt, st, mid in mm.get(nid, []):
            if tgt == cid:
                add("approved_manual_mapping", True, [["repo:manual_mappings", mid]], st)
        # ---- homotypic group of C
        B = col.get(C["basionym_id"]) if C["basionym_id"] else None
        hg = {cid}
        if B:
            hg.add(B["id"])
        for k in list(hg):
            hg.update(u["id"] for u in col_by_bas.get(k, []))
            hg.update(y for y, t in col_rel.get(k, []) if t in ("basionym", "homotypic", "based on", "spelling correction"))
        c_syn = col_children.get(cid, [])
        shared_h, shared_het = [], []
        for s in nt_syn.get(nid, []):
            for u in c_syn + ([B] if B and B not in c_syn else []):
                if u["nkey"] != s["nkey"]:
                    continue
                ps, pu = paren(s["authorship"]), paren(u["authorship"])
                if not (au.compatible(s["authorship"], u["authorship"]) or
                        (ps[0] and pu[0] and same_author(ps[0], pu[0]) and same_author(ps[1], pu[1]))):
                    continue
                refs = [["nortaxa:taxon.txt", s["taxon_id"]], ["col:NameUsage.tsv", u["id"]]]
                if u["id"] in hg:
                    shared_h.append(("homotypic_by_col_basionym_link", True, refs, f"{s['name']} {s['authorship']}"))
                elif pu[0] and ((not paren(C["authorship"])[0] and same_author(pu[0], paren(C["authorship"])[1]))
                                or (paren(C["authorship"])[0] and same_author(pu[0], paren(C["authorship"])[0]))):
                    shared_h.append(("homotypic_by_basionym_author_eq_accepted_author", False, refs,
                                     f"{s['name']} {s['authorship']}"))
                else:
                    shared_het.append(("shared_synonym_not_shown_homotypic", True, refs, f"{s['name']} {s['authorship']}"))
        for k, v, r_, n_ in shared_h:
            add("shared_synonym:" + k, v, r_, n_)
        for k, v, r_, n_ in shared_het:
            add(k, v, r_, n_)
        if any(u["status"] == "ambiguous synonym" for u in c_syn):
            flags.append("col_has_ambiguous_synonyms")
        # ---- B3 basionym inferred from authorship
        if pN and pC and same_author(pN, pC):
            refs = [["nortaxa:taxon.txt", nid], ["col:NameUsage.tsv", cid]]
            if B:
                refs.append(["col:NameUsage.tsv", B["id"]])
            add("same_epithet_same_basionym_author", False, refs,
                f"NorTaxa ({pN}) vs COL basionym {B['name'] + ' ' + B['authorship'] if B else '(' + pC + ')'}")
        # ---- corroboration: external ids, Dyntaxa third witness, xre grade
        cinat = {int(t[5:]) for t in (C["alt_id"] or "").split(",") if t.startswith("inat:") and t[5:].isdigit()}
        for ss, e, role, note in ext.get(int(nsid), []):
            if ss == "inaturalist" and e in cinat:
                add("inaturalist_id_agrees", False, [["col:NameUsage.tsv", cid], ["release:taxon_external_id_min", f"{nsid}/inaturalist/{e}"]],
                    "COL publishes the id; the NorTaxa-side id is " + (note or "release row"))
        dyn_ids = [t.rsplit(":", 1)[1] for t in (C["alt_id"] or "").split(",") if t.startswith("dyntaxa:")]
        for d in dyn_ids:
            drow = dyn.get(f"urn:lsid:dyntaxa.se:Taxon:{d}")
            if not drow:
                continue
            if au.compatible(drow[5], N["authorship"]) and not au.compatible(drow[5], C["authorship"]):
                add("dyntaxa_linked_by_col_uses_nortaxa_authorship", False, [["col:NameUsage.tsv", cid], ["dyntaxa:Taxon.csv", drow[0]]], drow[5])
            for s in nt_syn.get(nid, []):
                for dr in dyn_by_acc.get(drow[0], []):
                    if dr[6] == "homotypicSynonym" and unicodedata.normalize("NFC", dr[3]).casefold() == s["nkey"]:
                        add("dyntaxa_homotypic_synonym_matches_nortaxa_synonym", False,
                            [["dyntaxa:Taxon.csv", dr[0]], ["nortaxa:taxon.txt", s["taxon_id"]]], dr[3] + " " + dr[5])
        if rec.get("published_synonymy") and rec["published_synonymy"] not in ("none", "no_evidence", ""):
            flags.append("xre_grade:" + str(rec["published_synonymy"]))
        # ---- vernacular (support only)
        nv, cv = nt_vern.get(nid, set()), col_vern.get(cid, set())
        shared_v = sorted(nv & cv)
        vflags = []
        for v in shared_v:
            if len(vern_concepts.get(v, ())) > 1:
                vflags.append(f"{v}:reused_by_{len(vern_concepts[v])}_nortaxa_concepts")
            if v in genus_vern:
                vflags.append(f"{v}:genus_or_higher_level_name")
            if AGG.search(v):
                vflags.append(f"{v}:aggregate_or_group_name")
        clean_v = [v for v in shared_v if not any(f.startswith(v + ":") for f in vflags)]
        if shared_v:
            add("shared_norwegian_vernacular", True,
                [["nortaxa:vernacularname.txt", nid], ["col:VernacularName.tsv", cid]],
                ", ".join(shared_v) + (" | risks: " + "; ".join(vflags) if vflags else ""))
        if nv and cv and not shared_v:
            flags.append("norwegian_vernaculars_differ")
        if not shared_v and nv & col_vern_copy.get(cid, set()):
            flags.append("vernacular_shared_only_via_col_copy_of_nortaxa")
        # ---- authorship normalisation (candidate only)
        nac = norm_author_candidate(N["authorship"], C["authorship"])
        if four_letter(N["authorship"], C["authorship"]) and au.compare(N["authorship"], C["authorship"]) == "conflicting":
            flags.append("discovery_signal_4letter_overlap")
        if nac:
            flags.append("normalised_authorship:" + nac)

        # ---- bucket
        kinds = {e["kind"] for e in ev}
        positive = {k for k in kinds if not k.startswith("conflict")}
        if contra:
            b = "X1_conflict"
        elif ambi:
            b = "X2_ambiguity"
        elif kinds & {"col_lists_nortaxa_string_under_C", "nortaxa_lists_col_string_under_N", "approved_manual_mapping"}:
            b = "B1_explicit_xref"
        elif any(k.startswith("shared_synonym:") for k in kinds):
            b = "B2_shared_homotypic"
        elif "same_epithet_same_basionym_author" in kinds:
            b = "B3_basionym_inferred"
        elif "shared_synonym_not_shown_homotypic" in kinds:
            b = "B4_heterotypic_or_uncertain"
        elif clean_v and not any(f.endswith("_disagrees") for f in contra):
            b = "B5_accepted_plus_vernacular"
        elif nac or au.compare(N["authorship"], C["authorship"]) == "abbreviation":
            b = "B6_normalised_authorship_candidate"
        else:
            b = "R_residual"
        nA, cA = N["authorship"] or "", C["authorship"] or ""
        if b in ("B5_accepted_plus_vernacular", "B6_normalised_authorship_candidate", "R_residual") and nA and cA \
                and not same_author(nA, cA) and not variant_author(nA, cA) and not paren(nA)[0] and not paren(cA)[0]:
            ambi.append("possible_homonym_disjoint_authors")
            b = "X2_ambiguity"
        cur = row["category"] if row["category"] != "B" else "B:" + (row["b_subclass"] or "?")
        trans[(cur, b)] += 1
        buckets[b] += 1
        for k in positive:
            anyev[k.split(":")[0] if not k.startswith("shared_synonym:") else k] += 1
        if positive:
            anyev["__pairs_with_any_positive_relationship_evidence"] += 1
        out.append({"bucket": b, "current_category": cur, "scientific_name": row["scientific_name"],
                    "nortaxa_taxon_id": nid, "nortaxa_sporely_taxon_id": int(nsid),
                    "col_usage_id": cid, "col_sporely_taxon_id": int(csid),
                    "nortaxa_authorship": N["authorship"], "col_authorship": C["authorship"],
                    "authorship_class": au.compare(N["authorship"], C["authorship"]),
                    "nortaxa_nomenclatural_status": nsN, "col_name_status": nsC,
                    "nortaxa_name_published_in": nt_pub.get(nid, ""),
                    "col_basionym": {"id": B["id"], "name": B["name"], "authorship": B["authorship"]} if B else None,
                    "col_index_fungorum_record": (re.search(r"RecordID=(\d+)", C["link"] or "") or [None, None])[1],
                    "col_source_id": C["source_id"], "resolution": resolution,
                    "contradictions": contra, "ambiguities": ambi, "evidence": ev,
                    "flags": flags + vflags})
    out.sort(key=lambda r: (BUCKETS.index(r["bucket"]), r["scientific_name"]))
    with (a.out_dir / "pairs_relationship.jsonl").open("w") as f:
        for r in out:
            f.write(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n")
    with (a.out_dir / "evidence_gained.jsonl").open("w") as f:
        for r in out:
            if any(not e["kind"].startswith("conflict") for e in r["evidence"]):
                f.write(json.dumps({k: r[k] for k in ("bucket", "scientific_name", "nortaxa_taxon_id",
                                                      "nortaxa_sporely_taxon_id", "col_usage_id", "col_sporely_taxon_id",
                                                      "evidence")}, ensure_ascii=False, sort_keys=True) + "\n")
    with (a.out_dir / "promoted_pairs.csv").open("w", newline="") as f:
        cw = csv.writer(f)
        cw.writerow(["bucket", "scientific_name", "nortaxa_taxon_id", "nortaxa_sporely_taxon_id", "nortaxa_authorship",
                     "col_usage_id", "col_sporely_taxon_id", "col_authorship", "current_category",
                     "evidence_kinds", "all_evidence_source_verified", "evidence_rows", "flags"])
        for r in out:
            if r["bucket"] in ("B1_explicit_xref", "B2_shared_homotypic"):
                pos = [e for e in r["evidence"] if not e["kind"].startswith("conflict")]
                cw.writerow([r["bucket"], r["scientific_name"], r["nortaxa_taxon_id"], r["nortaxa_sporely_taxon_id"],
                             r["nortaxa_authorship"], r["col_usage_id"], r["col_sporely_taxon_id"], r["col_authorship"],
                             r["current_category"], ";".join(sorted({e["kind"] for e in pos})),
                             all(e["source_verified"] for e in pos),
                             " | ".join(f"{x[0]}#{x[1]}" for e in pos for x in e["rows"]), ";".join(r["flags"])])
    # coverage over the pairs' own rows
    cov = {"pairs": len(rows),
           "col_C_has_basionymID": sum(1 for r in out if r["col_basionym"]),
           "col_C_has_index_fungorum_record": sum(1 for r in out if r["col_index_fungorum_record"]),
           "col_C_has_synonyms": sum(1 for r in rows if col_children.get(r["col_usage_id"])),
           "nortaxa_N_has_synonyms": sum(1 for r in rows if nt_syn.get(r["nortaxa_taxon_id"])),
           "nortaxa_N_has_namePublishedIn": sum(1 for r in rows if nt_pub.get(r["nortaxa_taxon_id"])),
           "nortaxa_N_has_nomenclaturalStatus": sum(1 for r in rows if nt[r["nortaxa_taxon_id"]]["nom_status"]),
           "col_C_has_nameStatus": sum(1 for r in rows if col[r["col_usage_id"]]["name_status"]),
           "col_C_has_dyntaxa_alt_id": sum(1 for r in rows if "dyntaxa:" in (col[r["col_usage_id"]]["alt_id"] or "")),
           "col_C_has_inat_alt_id": sum(1 for r in rows if "inat:" in (col[r["col_usage_id"]]["alt_id"] or "")),
           "nortaxa_N_parenthetical_author": sum(1 for r in rows if paren(nt[r["nortaxa_taxon_id"]]["authorship"])[0]),
           "col_C_from_source_2030_nortaxa": sum(1 for r in rows if col[r["col_usage_id"]]["source_id"] == "2030"),
           "nortaxa_N_has_norwegian_vernacular": sum(1 for r in rows if nt_vern.get(r["nortaxa_taxon_id"])),
           "col_C_has_norwegian_vernacular": sum(1 for r in rows if col_vern.get(r["col_usage_id"]))}
    summary = {"buckets_disjoint": {b: buckets.get(b, 0) for b in BUCKETS}, "precedence": BUCKETS,
               "any_evidence_counts": dict(sorted(anyev.items())),
               "contradiction_reasons": dict(collections.Counter(c.split("_under_")[0] for r in out for c in r["contradictions"])),
               "ambiguity_reasons": dict(collections.Counter(c.split(":")[0] for r in out for c in r["ambiguities"])),
               "flags": dict(collections.Counter(f.split(":")[0] if ":" in f and not f.startswith("normalised") else f
                                                 for r in out for f in r["flags"] if not re.match(r"^[^:]+:(reused|genus|aggregate)", f))),
               "vernacular_risk_flags": dict(collections.Counter(f.split(":", 1)[1].split("_by_")[0].split("_level")[0]
                                                                for r in out for f in r["flags"] if re.match(r"^[^:]+:(reused|genus|aggregate)", f))),
               "transition_current_to_bucket": {f"{k[0]} -> {k[1]}": v for k, v in sorted(trans.items())},
               "pair_row_coverage": cov}
    (a.out_dir / "summary.json").write_text(json.dumps(summary, indent=1, ensure_ascii=False, sort_keys=False) + "\n")
    print(json.dumps(summary, indent=1, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
