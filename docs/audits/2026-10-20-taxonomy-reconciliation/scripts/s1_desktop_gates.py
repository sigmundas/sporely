#!/usr/bin/env python3
"""Revision 5, S1 tasks 3-5 (desktop side): schema-driven S1 reference
inventory, executable S1 consistency gates on today's baseline, and the S1
dry run on scratch copies.

Inputs are read-only: the desktop copies (sqlite backup copies), the release
copy and the work DB through mode=ro URIs; the historical/profile databases
under the application directory through file:...?mode=ro&immutable=1 only.
It refuses to WRITE anything under "Application Support". Output is
aggregate counts and taxon ids only.

Inventory = every table and column of mushrooms.db and reference_values.db,
including every JSON leaf, scanned for
  * an S1 retiree Sporely id (988), and
  * an S1 NorTaxa identifier (988 accepted taxonIDs + 1,175 synonym taxonIDs).
A hit is "structured" when its column or JSON key names a taxon/identity
field, otherwise it is a numeric coincidence and is listed for inspection.

  python -B scripts/s1_desktop_gates.py --s1 <data/s1-candidate.json> --workdb <workdb> \
     --release <release copy> --desktop <mushrooms.db copy> --reference-db <reference_values.db copy> \
     --app-dir "<~/Library/Application Support/Sporely>" --work-dir <scratch>/dryrun-s1 --out <json>
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import re
import sqlite3
import urllib.parse
from pathlib import Path

TAXON_KEY = re.compile(r"taxon|sporely|identity|external_id|artsdata", re.I)
# keys whose numeric value is in the NorTaxa / Artsdatabanken id space
NT_KEY = re.compile(r"taxon_identity_external_id|artsdata|nortaxa|artsdatabanken|ai_selected_taxon_id|orakel", re.I)
IDX = re.compile(r"[.]?[0-9]*\[\]|[.][0-9]+")
DIGITS = re.compile(r"(?<![\d.])\d{1,7}(?![\d.])")
# column -> (class, migration action). Everything else that scans clean needs no class.
CLASSES = {
    ("mushrooms", "observations", "sporely_taxon_id"): ("live_mutable", "remap retiree->survivor"),
    ("mushrooms", "observations", "taxon_identity_external_id"): ("live_mutable", "remap when namespace=sporely_taxon_id; NorTaxa tuple kept verbatim (re-resolves via S1 bridge)"),
    ("mushrooms", "observations", "artsdata_id"): ("live_mutable", "provider id (Artsdatabanken/NorTaxa); keep, report"),
    ("mushrooms", "observations", "ai_selected_taxon_id"): ("live_mutable", "provider id; keep"),
    ("mushrooms", "observations", "inaturalist_taxon_id"): ("live_mutable", "iNaturalist id; keep"),
    ("mushrooms", "observations", "ai_state_json"): ("historical", "AI provider payload; keep"),
    ("mushrooms", "settings", "value"): ("mirror", "cloud snapshot / pending state: remap only together with its cloud row"),
    ("mushrooms", "observation_reference_uses", "snapshot_json"): ("historical", "frozen reference snapshot; keep, report"),
    ("mushrooms", "observation_reference_use_cloud_sync_state", "accepted_payload_json"): ("mirror", "mirror of cloud row; keep"),
    ("mushrooms", "observation_reference_use_cloud_sync_state", "conflict_json"): ("mirror", "conflict payload; must be empty in window"),
    ("mushrooms", "observation_reference_use_cloud_tombstones", "accepted_payload_json"): ("mirror", "pending delete payload"),
    ("mushrooms", "observation_reference_use_cloud_remote_tombstone_markers", "accepted_payload_json"): ("mirror", "remote delete marker"),
    ("mushrooms", "session_logs", "value"): ("historical", "UI event log; keep"),
    ("mushrooms", "session_logs", "metadata_json"): ("historical", "UI event log; keep"),
    ("reference_values", "reference_taxon_treatments", "taxon_id"): ("live_mutable", "remap retiree->survivor (synced user data)"),
    ("reference_values", "curated_reference_forks", "sporely_taxon_id"): ("live_mutable", "remap retiree->survivor"),
    ("reference_values", "curated_reference_forks", "source_envelope_json"): ("historical", "frozen fork source; keep"),
    ("reference_values", "curated_reference_fork_cloud_sync_state", "accepted_payload_json"): ("mirror", "mirror of cloud row"),
    ("reference_values", "reference_cloud_sync_state", "accepted_payload_json"): ("mirror", "mirror of cloud row"),
    ("reference_values", "reference_cloud_sync_state", "conflict_json"): ("mirror", "conflict payload; must be empty in window"),
    ("reference_values", "reference_cloud_tombstones", "accepted_payload_json"): ("mirror", "pending delete payload"),
    ("reference_values", "reference_cloud_remote_tombstone_markers", "accepted_payload_json"): ("mirror", "remote delete marker"),
    ("reference_values", "reference_values", "metadata_json"): ("historical", "legacy reference values"),
}


def ro(path: Path, immutable=False) -> sqlite3.Connection:
    q = "mode=ro&immutable=1" if immutable else "mode=ro"
    return sqlite3.connect(f"file:{urllib.parse.quote(str(path))}?{q}", uri=True)


def walk(o, path=""):
    if isinstance(o, dict):
        for k, v in o.items():
            yield from walk(v, f"{path}.{k}")
    elif isinstance(o, list):
        for v in o:
            yield from walk(v, path + "[]")
    else:
        yield path, o


def scan(conn: sqlite3.Connection, dbname: str, sids: set[int], ntids: set[str]) -> dict:
    """Per (table, column[, json path]) counts: rows with a value, structured hits on
    S1 Sporely ids / S1 NorTaxa ids, and numeric coincidences."""
    out = {}
    for (t,) in conn.execute("select name from sqlite_master where type='table' and name not like 'sqlite_%' order by 1"):
        cols = [r[1] for r in conn.execute(f"pragma table_info('{t}')")]
        for col in cols:
            rec = collections.Counter()
            for (v,) in conn.execute(f'select "{col}" from "{t}" where "{col}" is not null'):
                rec["rows_with_value"] += 1
                leaves = []
                if isinstance(v, str) and v[:1] in "{[":
                    try:
                        leaves = list(walk(json.loads(v)))
                    except ValueError:
                        leaves = [("", v)]
                else:
                    leaves = [("", v)]
                for p, leaf in leaves:
                    keyctx = col + IDX.sub("", p)
                    structured = bool(TAXON_KEY.search(keyctx))
                    toks = [str(leaf)] if isinstance(leaf, int) and not isinstance(leaf, bool) else \
                        (DIGITS.findall(leaf) if isinstance(leaf, str) else [])
                    for tok in toks:
                        if int(tok) in sids:
                            rec[("structured" if structured else "coincidence") + "_sporely_s1_retiree"] += 1
                            if structured:
                                rec["paths:" + keyctx] += 1
                        if tok in ntids and NT_KEY.search(keyctx):
                            rec["structured_nortaxa_s1_id"] += 1
                            rec["paths:" + keyctx] += 1
                        elif tok in ntids and structured:
                            rec["other_id_space_equal_to_s1_nortaxa_id"] += 1
            if any(k != "rows_with_value" for k in rec):
                cls = CLASSES.get((dbname, t, col), ("unclassified", "inspect"))
                out[f"{dbname}.{t}.{col}"] = {**{str(k): v for k, v in rec.items()},
                                              "class": cls[0], "action": cls[1]}
            elif (dbname, t, col) in CLASSES:
                cls = CLASSES[(dbname, t, col)]
                out[f"{dbname}.{t}.{col}"] = {"rows_with_value": rec["rows_with_value"], "class": cls[0],
                                              "action": cls[1], "s1_hits": 0}
    return out


def table_hashes(conn) -> dict:
    h = {}
    for (t,) in conn.execute("select name from sqlite_master where type='table' and name not like 'sqlite_%' "
                             "and name<>'taxonomy_migration_log' order by 1"):
        d = hashlib.sha256()
        for row in conn.execute(f'select * from "{t}" order by rowid'):
            d.update(repr(row).encode())
        h[t] = d.hexdigest()
    return h


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    for x in ("s1", "workdb", "release", "desktop", "reference-db", "app-dir", "work-dir", "out"):
        ap.add_argument("--" + x, required=True, type=Path)
    ap.add_argument("--print-g4-cloud-ids", action="store_true",
                    help="print (stdout only, never saved) the cloud ids for SQL gate CG5b")
    a = ap.parse_args(argv)
    for p in (a.desktop, a.reference_db, a.work_dir, a.out):
        if "Application Support" in str(p.resolve()):
            raise SystemExit(f"refusing: {p} is under the live application directory")
    s1 = json.loads(a.s1.read_text())["members"]
    m = {x["retired"]: x["survivor"] for x in s1}
    sids, survivors = set(m), set(m.values())
    w = ro(a.workdb)
    nt_acc = {x["nortaxa_taxon_id"] for x in s1}
    nt_syn = {r[0] for r in w.execute("select ident from usages where src='nortaxa' and alias_reason='synonym_of_accepted' "
                                      f"and sid in ({','.join(map(str, sids))})")}
    ntids = nt_acc | nt_syn
    scope = {r[0] for r in w.execute("select sid from cloud_scope")}
    rel = ro(a.release)
    src_of = dict(rel.execute("select taxon_id, source_system from taxon_min"))
    rel_ids = set(src_of)
    cand_ids = rel_ids - sids                       # what the S1 release keeps
    out = {"s1_retirees": len(sids), "s1_nortaxa_ids": {"accepted": len(nt_acc), "synonym": len(nt_syn)}}

    # ---------------- 3. inventory: live desktop (copies)
    desk, ref = ro(a.desktop), ro(a.reference_db)
    out["inventory_desktop"] = {**scan(desk, "mushrooms", sids, ntids), **scan(ref, "reference_values", sids, ntids)}
    # generated release rows (installed taxonomy file == release copy)
    gen = {}
    for t, col in (("taxon_min", "taxon_id"), ("taxon_min", "parent_taxon_id"), ("vernacular_min", "taxon_id"),
                   ("scientific_name_min", "taxon_id"), ("taxon_external_id_min", "taxon_id"),
                   ("taxon_external_id_text_min", "taxon_id"), ("taxon_redlist_min", "taxon_id")):
        gen[f"{t}.{col}"] = sum(n for s, n in rel.execute(f"select {col}, count(*) from {t} group by 1") if s in sids)
    gen["taxon_external_id_text_min(nortaxa tuple on S1 ids, any taxon)"] = sum(
        1 for (e,) in rel.execute("select external_id from taxon_external_id_text_min where source_system='nortaxa' "
                                  "and namespace='nortaxa_taxon_id'") if e in ntids)
    gen["taxon_min.norwegian_taxon_id(S1 NorTaxa ids)"] = sum(
        1 for (e,) in rel.execute("select norwegian_taxon_id from taxon_min where norwegian_taxon_id is not null")
        if str(e) in ntids)
    out["inventory_generated_release_rows"] = gen
    # historical / other-profile databases (immutable read-only)
    hist = {}
    for p in sorted(a.app_dir.rglob("*")):
        if not p.is_file() or p.stat().st_size == 0 or "/images/" in str(p) or "/thumbnails/" in str(p):
            continue
        if not (p.suffix in (".db", ".bak") or p.name.endswith(".db.bak-2026-08-22-pre-2703-repair")
                or ".db.bak-" in p.name) or p.name.endswith(("-shm", "-wal")):
            continue
        rel_name = str(p.relative_to(a.app_dir))
        if rel_name in ("mushrooms.db", "reference_values.db"):
            continue
        try:
            c = ro(p, immutable=True)
            res = scan(c, "mushrooms" if "mushrooms" in p.name else "reference_values", sids, ntids)
            c.close()
        except sqlite3.DatabaseError as exc:
            hist[rel_name] = {"error": str(exc)}
            continue
        hist[rel_name] = {k: v for k, v in res.items()
                          if any(str(x).startswith("structured") for x in v)} or "no structured S1 hit"
    for p in sorted(a.app_dir.rglob("vernacular_multilanguage_v2.sqlite3*")):
        if p.is_file() and p.stat().st_size > 0 and p.parent.name == "taxonomy_v2":
            c = ro(p, immutable=True)
            hist[str(p.relative_to(a.app_dir)) + " (taxonomy release copy)"] = {
                "taxon_min_rows_on_s1_retirees": sum(1 for (s,) in c.execute("select taxon_id from taxon_min") if s in sids),
                "content_release_id": (c.execute("select value from taxonomy_meta where key='content_release_id'").fetchone() or [None])[0]}
            c.close()
    out["inventory_historical_and_profiles"] = hist

    # ---------------- 4. gates on today's baseline
    if a.print_g4_cloud_ids:
        ids = [r[0] for r in desk.execute("select cloud_id, sporely_taxon_id, sync_status from observations "
                                          "where sporely_taxon_id is not null and cloud_id is not null")
               if r[1] not in scope and r[2] != "synced"]
        print("G4 cloud ids (do not save):", ids)
    obs = desk.execute("""select id, sporely_taxon_id, taxon_identity_source_system, taxon_identity_namespace,
        taxon_identity_external_id, taxon_identity_proof, cloud_id is not null, sync_status from observations""").fetchall()
    with_id = [o for o in obs if o[1] is not None]
    out_scope = [o for o in with_id if o[1] not in scope]
    nt_tuple = [o for o in obs if o[2] == "nortaxa" and o[3] == "nortaxa_taxon_id"]
    pinned = hashlib.sha256(",".join(str(o[0]) for o in sorted(out_scope)).encode()).hexdigest()
    gates = {
        "G1a_desktop_s1_sporely_id_refs_live_or_mirror": sum(
            v.get("structured_sporely_s1_retiree", 0)
            for v in out["inventory_desktop"].values() if v["class"] in ("live_mutable", "mirror", "unclassified")),
        "G1b_desktop_s1_nortaxa_id_refs_live_or_mirror(provider space, kept verbatim)": sum(
            v.get("structured_nortaxa_s1_id", 0)
            for v in out["inventory_desktop"].values() if v["class"] in ("live_mutable", "mirror", "unclassified")),
        "G2a_desktop_ids_absent_from_installed_release": sum(1 for o in with_id if o[1] not in rel_ids),
        "G2b_desktop_ids_absent_from_S1_release(release minus S1 retirees)": sum(1 for o in with_id if o[1] not in cand_ids),
        "G3_desktop_ids_outside_cloud_scope": len(out_scope),
        "G3_breakdown(source|tuple|proof)": dict(collections.Counter(
            f"{src_of.get(o[1])}|{o[2]}/{o[3]}|{o[5]}" for o in out_scope)),
        "G3_pinned_observation_set_sha256": pinned,
        "G3_s1_retiree_or_survivor_among_them": sum(1 for o in out_scope if o[1] in sids or o[1] in survivors),
        "G4_fallback_exposure_dirty_outside_cloud_scope": sum(1 for o in out_scope if o[7] != "synced"),
        "G4_fallback_exposure_with_external_tuple": sum(1 for o in out_scope if o[7] != "synced" and o[2] not in (None, "sporely")),
        "G5_desktop_nortaxa_tuples_total": len(nt_tuple),
        "G5_desktop_nortaxa_tuples_on_s1_ids": sum(1 for o in nt_tuple if str(o[4]) in ntids),
        "G6_pending_sync": {
            "observations_not_synced": sum(1 for o in obs if o[7] != "synced"),
            "observation_reference_use_sync_state_not_synced": desk.execute(
                "select count(*) from observation_reference_use_cloud_sync_state where sync_status not in ('synced','clean')").fetchone()[0],
            "reference_sync_state_not_synced": ref.execute(
                "select count(*) from reference_cloud_sync_state where sync_status not in ('synced','clean')").fetchone()[0],
            "tombstones_pending": desk.execute("select count(*) from observation_reference_use_cloud_tombstones").fetchone()[0]
            + ref.execute("select count(*) from reference_cloud_tombstones").fetchone()[0],
            "conflicts": desk.execute("select count(*) from observation_reference_use_cloud_sync_state "
                                      "where coalesce(conflict_json,'') not in ('','null')").fetchone()[0]
            + ref.execute("select count(*) from reference_cloud_sync_state "
                          "where coalesce(conflict_json,'') not in ('','null')").fetchone()[0],
        },
        "G7_survivors_absent_from_installed_release": len(survivors - rel_ids),
    }
    cl = [(k, s, n) for k, s, n in w.execute("select kind, sid, n from obs_cloud where sid is not null")]
    gates["C_snapshot_2026-10-20"] = {
        "selected_on_s1_retirees": sum(n for k, s, n in cl if k == "selected" and s in sids),
        "resolved_on_s1_retirees": sum(n for k, s, n in cl if k == "resolved" and s in sids),
        "selected_ids_absent_from_release": sorted({s for k, s, n in cl if k == "selected" and s not in rel_ids}),
        "resolved_ids_absent_from_release": sorted({s for k, s, n in cl if k == "resolved" and s not in rel_ids}),
        "absent_ids_that_are_s1_ids": sorted({s for k, s, n in cl if s not in rel_ids and (s in sids or s in survivors)}),
        "selected_ids_outside_cloud_scope": len({s for k, s, n in cl if k == "selected" and s not in scope}),
        "s1_retirees_in_cloud_scope": len(sids & scope),
        "s1_survivors_in_cloud_scope": len(survivors & scope),
    }
    out["gates_baseline"] = gates

    # ---------------- 5. dry run on copies
    d = a.work_dir
    d.mkdir(parents=True, exist_ok=True)
    copies = {}
    for name, src in (("mushrooms.db", a.desktop), ("reference_values.db", a.reference_db)):
        dst = d / name
        if dst.exists():
            dst.unlink()
        s, t = ro(src), sqlite3.connect(dst)
        s.backup(t)
        t.close()
        s.close()
        copies[name] = dst
    con, rcon = sqlite3.connect(copies["mushrooms.db"]), sqlite3.connect(copies["reference_values.db"])
    before = {"mushrooms": table_hashes(con), "reference_values": table_hashes(rcon)}
    msha = hashlib.sha256(json.dumps({str(k): v for k, v in sorted(m.items())}, sort_keys=True).encode()).hexdigest()
    changed = collections.Counter()
    for c in (con, rcon):
        c.execute("create table taxonomy_migration_log(tbl text, row_key text, col text, old text, new text, mapping_sha256 text)")
    with con:
        for oid, sid in con.execute("select id, sporely_taxon_id from observations where sporely_taxon_id is not null").fetchall():
            if sid in m:
                con.execute("update observations set sporely_taxon_id=? where id=?", (m[sid], oid))
                con.execute("insert into taxonomy_migration_log values('observations',?,'sporely_taxon_id',?,?,?)", (oid, sid, m[sid], msha))
                changed["observations.sporely_taxon_id"] += 1
        for oid, e in con.execute("select id, taxon_identity_external_id from observations "
                                  "where taxon_identity_namespace='sporely_taxon_id'").fetchall():
            if str(e).isdigit() and int(e) in m:
                con.execute("update observations set taxon_identity_external_id=? where id=?", (str(m[int(e)]), oid))
                con.execute("insert into taxonomy_migration_log values('observations',?,'taxon_identity_external_id',?,?,?)", (oid, e, m[int(e)], msha))
                changed["observations.taxon_identity_external_id"] += 1
    with rcon:
        for rid, t in rcon.execute("select id, taxon_id from reference_taxon_treatments").fetchall():
            if str(t or "").isdigit() and int(t) in m:
                rcon.execute("update reference_taxon_treatments set taxon_id=? where id=?", (str(m[int(t)]), rid))
                rcon.execute("insert into taxonomy_migration_log values('reference_taxon_treatments',?,'taxon_id',?,?,?)", (rid, t, m[int(t)], msha))
                changed["reference_taxon_treatments.taxon_id"] += 1
        for rid, t in rcon.execute("select rowid, sporely_taxon_id from curated_reference_forks").fetchall():
            if t in m:
                rcon.execute("update curated_reference_forks set sporely_taxon_id=? where rowid=?", (m[t], rid))
                rcon.execute("insert into taxonomy_migration_log values('curated_reference_forks',?,'sporely_taxon_id',?,?,?)", (rid, t, m[t], msha))
                changed["curated_reference_forks.sporely_taxon_id"] += 1
    after = {"mushrooms": table_hashes(con), "reference_values": table_hashes(rcon)}
    rescan = {**scan(con, "mushrooms", sids, ntids), **scan(rcon, "reference_values", sids, ntids)}
    out["dry_run"] = {
        "mapping_size": len(m), "mapping_canonical_sha256": msha,
        "changed": dict(changed),
        "tables_changed": sorted(f"{db}.{t}" for db in before for t in before[db] if before[db][t] != after[db].get(t)),
        "tables_compared": sum(len(v) for v in before.values()),
        "after_structured_s1_hits_live_or_mirror": sum(
            v.get("structured_sporely_s1_retiree", 0) for v in rescan.values() if v["class"] in ("live_mutable", "mirror", "unclassified")),
        "log_rows": con.execute("select count(*) from taxonomy_migration_log").fetchone()[0]
        + rcon.execute("select count(*) from taxonomy_migration_log").fetchone()[0],
        "integrity_check": [con.execute("pragma integrity_check").fetchone()[0], rcon.execute("pragma integrity_check").fetchone()[0]],
        "foreign_key_check_rows": len(con.execute("pragma foreign_key_check").fetchall()) + len(rcon.execute("pragma foreign_key_check").fetchall()),
    }
    con.close()
    rcon.close()
    a.out.write_text(json.dumps(out, ensure_ascii=False, indent=1, sort_keys=True) + "\n")
    print(json.dumps({k: out[k] for k in ("s1_nortaxa_ids", "gates_baseline", "dry_run", "inventory_generated_release_rows")}, indent=1))
    print("desktop columns with any S1 hit or classified:")
    for k, v in out["inventory_desktop"].items():
        print(" ", k, {x: y for x, y in v.items() if x not in ("action",)})
    print("historical:")
    for k, v in out["inventory_historical_and_profiles"].items():
        print(" ", k, v)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
