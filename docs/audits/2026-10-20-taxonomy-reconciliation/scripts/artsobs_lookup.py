#!/usr/bin/env python3
"""Review finding 7 / owner task 5: Artsobservasjoner coverage measured with
the desktop's actual offline lookup, before and after batch A.

Emulates database/models.py::_lookup_adb_taxon_id_from_db (lines 1003-1064 at
main 19af384) on the release:
  1. distinct taxon_min.norwegian_taxon_id over rows with
     genus = ? AND specific_epithet = ? (COLLATE NOCASE); exactly one -> id
  2. else distinct 'artsdatabanken' accepted external ids over the same rows;
     exactly one -> id
  3. else the code falls through to taxon_min.taxon_id (returns a Sporely id;
     being fixed on a separate branch). Counted here as NO ID
     ("fallthrough_sporely_id"), never as covered.
Live picker search (artsobservasjoner.picker_search) is out of scope (fallback).

After A: retired concepts' rows disappear; their accepted 'artsdatabanken'
ids move to the survivor (as the Stage 2 supersessions do; the survivor's
norwegian_taxon_id column stays NULL).

  python -B scripts/artsobs_lookup.py --workdb <workdb> --release <release copy> \
      --manifests <audit>/manifests --desktop <mushrooms.db copy> --out <out>/artsobs_lookup_coverage.json
"""
from __future__ import annotations

import argparse
import collections
import json
import sqlite3
from pathlib import Path

def retire_map(mdir: Path, prefixes) -> dict[int, int]:
    """retired -> survivor for every index partition whose name starts with one
    of the prefixes (rev 3 groups: A0-*, A1-*, A2-*)."""
    index = json.loads((mdir / "index.json").read_text())
    out = {}
    for name, info in index["partitions"].items():
        if not name.startswith(tuple(prefixes)):
            continue
        path = mdir / info["path"]
        if info["validator_format"]:
            doc = json.loads(path.read_text())
            for row in doc["members"]:
                r = dict(zip(doc["columns"], row))
                out[r["nortaxa_sporely_taxon_id"]] = r["col_sporely_taxon_id"]
        else:
            for line in path.open():
                r = json.loads(line)
                out[r["retired_sporely_taxon_id"]] = r["survivor_sporely_taxon_id"]
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdb", required=True, type=Path)
    ap.add_argument("--release", required=True, type=Path)
    ap.add_argument("--manifests", required=True, type=Path)
    ap.add_argument("--desktop", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    a = ap.parse_args(argv)
    rel = sqlite3.connect(f"file:{a.release}?mode=ro", uri=True)
    work = sqlite3.connect(f"file:{a.workdb}?mode=ro", uri=True)
    desk = sqlite3.connect(f"file:{a.desktop}?mode=ro", uri=True)
    concepts = {}
    for sid, genus, ep, no, rank in rel.execute(
            "select taxon_id, genus, specific_epithet, norwegian_taxon_id, taxon_rank from taxon_min"):
        concepts[sid] = {"key": ((genus or "").casefold(), (ep or "").casefold()), "no": no, "rank": rank}
    adb = collections.defaultdict(set)
    for sid, eid in rel.execute("select taxon_id, external_id from taxon_external_id_min "
                                "where source_system='artsdatabanken' and id_role='accepted'"):
        adb[sid].add(eid)
    cloud_scope = {r[0] for r in work.execute("select sid from cloud_scope")}
    observed = set(r[0] for r in work.execute("select sid from obs_desktop"))
    observed |= {r[0] for r in work.execute("select sid from obs_cloud where sid is not null and kind in ('selected','resolved')")}
    desk_obs = [((g or "").strip().casefold(), (sp or "").strip().casefold(), sid) for g, sp, sid in desk.execute(
        "select genus, species, sporely_taxon_id from observations where coalesce(genus,'')<>'' "
        "and coalesce(species,'')<>'' and sporely_taxon_id is not null")]
    desk_pairs = collections.Counter(
        ((g or "").strip().casefold(), (s or "").strip().casefold())
        for g, s in desk.execute("select genus, species from observations where coalesce(genus,'')<>'' and coalesce(species,'')<>''"))

    def build(retire):
        live = {s: dict(c) for s, c in concepts.items() if s not in retire}
        ids = {s: set(adb.get(s, ())) for s in live}
        for old, new in retire.items():
            ids[new] |= adb.get(old, set())
        by_key = collections.defaultdict(list)
        for s, c in live.items():
            by_key[c["key"]].append(s)
        return live, ids, by_key

    def lookup(key, live, ids, by_key):
        """(status, returned_id): emulates the desktop function exactly."""
        rows = by_key.get(key, [])
        if not rows:
            return "no_match", None
        no = {live[s]["no"] for s in rows if live[s]["no"]}
        if len(no) == 1:
            return "id", next(iter(no))
        adbs = set().union(*(ids[s] for s in rows))
        if len(adbs) == 1:
            return "id", next(iter(adbs))
        if len(no) > 1 or len(adbs) > 1:
            return "ambiguous_none", None
        return "fallthrough_sporely_id", None

    def owners(live, ids):
        own = collections.defaultdict(set)
        for s, c in live.items():
            if c["no"]:
                own[c["no"]].add(s)
            for e in ids[s]:
                own[e].add(s)
        return own

    def verify(key, target, live, ids, by_key, own):
        """verified        : the id belongs to the target concept itself (or, for a
                            name target, to a species-rank concept with that name)
        plausible_twin    : the id belongs to another species-rank concept with the
                            same name (a duplicate twin), not the target
        wrong_rank        : the id belongs to a concept of another rank
                            (e.g. Cuphophyllus virgineus -> var. virgineus 186443)"""
        status, eid = lookup(key, live, ids, by_key)
        if status != "id":
            return status
        holders = own.get(eid, set())
        if any(live[h]["rank"] != "species" for h in holders):
            return "wrong_rank"
        if target is None or target in holders:
            return "verified"
        return "plausible_twin"

    out = {}
    views = (("before", {}), ("after_A0", retire_map(a.manifests, ["A0-"])),
             ("after_A_all_groups", retire_map(a.manifests, ["A0-", "A1-", "A2-"])))
    wrong_keys = {}
    for view, retire in views:
        live, ids, by_key = build(retire)
        own = owners(live, ids)
        res = {}
        pops = {
            "species_rank": [s for s in live if live[s]["rank"] == "species"],
            "species_rank_cloud_scope": [s for s in live if live[s]["rank"] == "species" and s in cloud_scope],
            "species_rank_with_nortaxa_id": [s for s in live if live[s]["rank"] == "species" and (live[s]["no"] or ids[s])],
        }
        for p, members in pops.items():
            c = collections.Counter(verify(live[s]["key"], s, live, ids, by_key, own) for s in members)
            res[p] = {"concepts": len(members), **c,
                      "verified_pct": round(100 * c["verified"] / len(members), 1) if members else 0}
        obs_c = collections.Counter()
        for s in observed:
            t = retire.get(s, s)
            if t not in live:
                obs_c["not_in_release"] += 1
            elif live[t]["rank"] != "species" or not live[t]["key"][1]:
                obs_c["not_species_rank_no_lookup"] += 1
            else:
                obs_c[verify(live[t]["key"], t, live, ids, by_key, own)] += 1
        n = sum(obs_c.values())
        res["observed_ids_desktop_or_cloud"] = {"ids": n, **obs_c,
                                                "verified_pct": round(100 * obs_c["verified"] / n, 1)}
        oc = collections.Counter()
        wk = set()
        for key, cnt in desk_pairs.items():
            v = verify(key, None, live, ids, by_key, own)
            oc[v] += cnt
            if v == "wrong_rank":
                wk.add(key)
        m = sum(oc.values())
        oc["name_lookup_rank_ok"] = oc.pop("verified", 0)
        res["desktop_observations_name_lookup"] = {
            "metric": "NAME-BASED lookup success with a species-rank id; NOT concept correctness",
            "observations": m, **oc,
            "name_lookup_rank_ok_pct": round(100 * oc["name_lookup_rank_ok"] / m, 1),
            "id_returned_pct_including_wrong_rank": round(100 * (oc["name_lookup_rank_ok"] + oc["wrong_rank"]) / m, 1)}
        # concept-verified: observations with a stored Sporely id; the returned
        # destination id must belong to that stored concept (after A: its survivor)
        cv = collections.Counter()
        mism = collections.Counter()
        for g, sp, sid in desk_obs:
            t = retire.get(sid, sid)
            if t not in live:
                cv["stored_id_not_in_release"] += 1
                continue
            status, eid = lookup((g, sp), live, ids, by_key)
            if status != "id":
                cv[status] += 1
                continue
            holders = own.get(eid, set())
            if t in holders:
                cv["concept_verified"] += 1
            elif any(live[h]["rank"] != "species" for h in holders):
                cv["wrong_rank"] += 1
            else:
                cv["id_of_another_concept"] += 1
                mism[(t, tuple(sorted(holders)))] += 1
        k = sum(cv.values())
        res["desktop_observations_concept_verified"] = {
            "metric": "returned destination id belongs to the observation's own stored Sporely concept "
                      "(stored-id provenance itself not re-verified)",
            "observations_with_stored_id": k, **cv,
            "concept_verified_pct": round(100 * cv["concept_verified"] / k, 1) if k else 0,
            "another_concept_cases": [{"stored_concept": a_, "id_holders": list(b_), "observations": n_}
                                      for (a_, b_), n_ in sorted(mism.items())]}
        allkeys = {c["key"] for c in live.values() if c["rank"] == "species" and c["key"][1]}
        res["species_name_keys_returning_wrong_rank_id"] = sum(
            1 for k in allkeys if verify(k, None, live, ids, by_key, own) == "wrong_rank")
        wrong_keys[view] = len(wk)
        out[view] = res
    out["desktop_name_keys_with_wrong_rank_id"] = wrong_keys
    out["notes"] = ["fallthrough_sporely_id counts as no id (bug being fixed separately)",
                    "verified = destination id belongs to the same concept/rank; wrong_rank ids are not coverage",
                    "R2/H10 baseline = desktop_observations_concept_verified.concept_verified_pct (before); "
                    "desktop_observations_name_lookup measures name-based lookup success only"]
    live, ids, by_key = build({})
    own = owners(live, ids)
    out["hygrocybe_conica_before"] = verify(concepts[16436]["key"], 16436, live, ids, by_key, own)
    cv = [s for s, c in concepts.items() if c["key"] == ("cuphophyllus", "virgineus") and c["rank"] == "species"]
    out["cuphophyllus_virgineus_before"] = [verify(concepts[s]["key"], s, live, ids, by_key, own) for s in cv]
    a.out.write_text(json.dumps(out, indent=1, sort_keys=True))
    print(json.dumps(out, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
