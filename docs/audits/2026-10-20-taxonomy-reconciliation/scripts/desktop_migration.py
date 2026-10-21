#!/usr/bin/env python3
"""Revision 4 (owner scope): maintenance-window migration of the owner's
desktop data. INVENTORY + DRY RUN ONLY, against scratchpad COPIES.

Never opens the live application databases: --desktop / --reference-db must be
copies (the script refuses paths under ~/Library/Application Support).

1. Builds a pinned, deterministic old-id -> survivor-id mapping per scope from
   manifests/index.json (S1 = A0-species + A0-infraspecific; ALL = every A group
   + B-placement, used only to exercise the code path), and writes it with its
   sha256 to --out-dir.
2. Inventories every desktop table/column/JSON path that can hold a Sporely
   taxon id or an identity tuple, with counts of rows referencing a retiree.
3. Dry run: copies both databases again (sqlite backup API) to
   --work-dir/<scope>/, applies the mapping there, logs every change in a
   table taxonomy_migration_log inside the copy, and verifies:
   no retiree remains in migrated columns; every new id exists in the release;
   row counts unchanged; PRAGMA integrity_check ok; sync-key and cloud-id
   columns untouched.
Revision 5: for S1 this script is superseded by s1_desktop_gates.py (schema-driven
inventory of every table, column and JSON leaf; per-table hashes before/after).
Rollback for the real run = restore the pre-migration backup files (their
sha256 is recorded before the run); see staged-release-plan.md.

  python -B scripts/desktop_migration.py --manifests <audit>/manifests --release <release copy> \
     --desktop <mushrooms.db copy> --reference-db <reference_values.db copy> \
     --work-dir <scratch>/dryrun --out-dir <out>
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
from pathlib import Path

SCOPES = {
    "S1": ("A0-species", "A0-infraspecific"),
    "ALL": None,  # every partition in the index
}


def mapping_for(mdir: Path, parts) -> dict[int, int]:
    index = json.loads((mdir / "index.json").read_text())
    out = {}
    for name, info in sorted(index["partitions"].items()):
        if parts is not None and name not in parts:
            continue
        path = mdir / info["path"]
        if info["validator_format"]:
            doc = json.loads(path.read_text())
            for row in doc["members"]:
                r = dict(zip(doc["columns"], row))
                out[int(r["nortaxa_sporely_taxon_id"])] = int(r["col_sporely_taxon_id"])
        else:
            for line in path.open():
                r = json.loads(line)
                out[int(r["retired_sporely_taxon_id"])] = int(r["survivor_sporely_taxon_id"])
    return dict(sorted(out.items()))


def backup(src: Path, dst: Path) -> None:
    s = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    d = sqlite3.connect(dst)
    s.backup(d)
    d.close()
    s.close()


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


TUPLE = re.compile(r"^sporely:(\d+)$")


def inventory(desk: sqlite3.Connection, ref: sqlite3.Connection, m: dict[int, int]) -> list[dict]:
    """(location, id space, total rows with a value, rows referencing a retiree, action)."""
    ret = set(m)
    rows = []

    def add(loc, space, total, hits, action):
        rows.append({"location": loc, "id_space": space, "rows_with_value": total,
                     "rows_referencing_retiree": hits, "migration_action": action})

    v = desk.execute("select sporely_taxon_id from observations where sporely_taxon_id is not null").fetchall()
    add("mushrooms.observations.sporely_taxon_id", "sporely", len(v), sum(1 for (x,) in v if x in ret),
        "remap old->survivor")
    v = desk.execute("""select taxon_identity_external_id from observations
        where taxon_identity_namespace='sporely_taxon_id'""").fetchall()
    add("mushrooms.observations.taxon_identity_* (namespace sporely_taxon_id)", "sporely", len(v),
        sum(1 for (x,) in v if str(x).isdigit() and int(x) in ret), "remap external_id old->survivor")
    v = desk.execute("""select taxon_identity_source_system, taxon_identity_namespace, count(*)
        from observations where taxon_identity_namespace is not null group by 1,2""").fetchall()
    add("mushrooms.observations.taxon_identity_* (provider tuples)", "provider (nortaxa/…)",
        sum(n for *_, n in v), 0, "keep tuple verbatim; it re-resolves to the survivor via the bridge")
    v = desk.execute("select inaturalist_taxon_id from observations where inaturalist_taxon_id is not null").fetchall()
    add("mushrooms.observations.inaturalist_taxon_id", "inaturalist", len(v), 0, "none (not a Sporely id)")
    v = desk.execute("select ai_selected_taxon_id from observations where ai_selected_taxon_id is not null").fetchall()
    add("mushrooms.observations.ai_selected_taxon_id", "provider", len(v), 0, "none (provider id)")
    add("mushrooms.observations.ai_state_json (inat_predictions[].taxon.id)", "inaturalist",
        desk.execute("select count(*) from observations where ai_state_json like '%taxon%'").fetchone()[0], 0,
        "none (iNaturalist ids)")
    hits = total = 0
    for (val,) in desk.execute("select value from settings where key like 'sporely_cloud_snapshot_obs_%'"):
        try:
            ti = (json.loads(val).get("observation") or {}).get("taxon_identity")
        except Exception:
            continue
        if ti:
            total += 1
            mm = TUPLE.match(str(ti))
            if mm and int(mm.group(1)) in ret:
                hits += 1
    add("mushrooms.settings[sporely_cloud_snapshot_obs_*].observation.taxon_identity", "sporely tuple",
        total, hits, "remap only together with the cloud row it mirrors (three-way consistency)")
    for tbl, col, path in (("observation_reference_uses", "snapshot_json", "taxon_id"),
                           ("observation_reference_use_cloud_sync_state", "accepted_payload_json", "snapshot_json.taxon_id")):
        total = hit = 0
        for (val,) in desk.execute(f"select {col} from {tbl}"):
            o = json.loads(val)
            for k in path.split("."):
                o = (o or {}).get(k) if isinstance(o, dict) else None
            if o not in (None, ""):
                total += 1
                if str(o).isdigit() and int(o) in ret:
                    hit += 1
        add(f"mushrooms.{tbl}.{col}.{path}", "reference taxon id (Sporely)", total, hit,
            "historical snapshot: keep; report" if tbl == "observation_reference_uses" else "mirror of cloud: keep")
    v = ref.execute("select taxon_id from reference_taxon_treatments where coalesce(taxon_id,'')<>''").fetchall()
    add("reference_values.reference_taxon_treatments.taxon_id", "sporely (text)", len(v),
        sum(1 for (x,) in v if str(x).isdigit() and int(x) in ret), "remap old->survivor (synced user data)")
    v = ref.execute("select sporely_taxon_id from curated_reference_forks where sporely_taxon_id is not null").fetchall()
    add("reference_values.curated_reference_forks.sporely_taxon_id", "sporely", len(v),
        sum(1 for (x,) in v if x in ret), "remap old->survivor")
    total = hit = 0
    for (val,) in ref.execute("select accepted_payload_json from reference_cloud_sync_state"):
        try:
            o = json.loads(val or "null")
        except Exception:
            continue
        t = o.get("taxon_id") if isinstance(o, dict) else None
        if t not in (None, ""):
            total += 1
            hit += int(str(t).isdigit() and int(t) in ret)
    add("reference_values.reference_cloud_sync_state.accepted_payload_json.taxon_id", "sporely (mirror)",
        total, hit, "mirror of cloud: keep")
    return rows


def dry_run(scope: str, m: dict[int, int], desktop: Path, reference: Path, release: Path, work: Path) -> dict:
    d = work / scope
    d.mkdir(parents=True, exist_ok=True)
    dk, rf = d / "mushrooms.db", d / "reference_values.db"
    for p in (dk, rf):
        if p.exists():
            p.unlink()
    backup(desktop, dk)
    backup(reference, rf)
    before = {"mushrooms.db": sha(dk), "reference_values.db": sha(rf)}
    rel = sqlite3.connect(f"file:{release}?mode=ro", uri=True)
    live = {r[0] for r in rel.execute("select taxon_id from taxon_min")}
    msha = hashlib.sha256(json.dumps(m, sort_keys=True).encode()).hexdigest()
    con = sqlite3.connect(dk)
    rcon = sqlite3.connect(rf)
    counts_before = {t: con.execute(f"select count(*) from {t}").fetchone()[0]
                     for t in ("observations", "observation_reference_uses", "settings")}
    keys_before = con.execute("select group_concat(coalesce(sync_key,'')||cloud_id) from observations").fetchone()[0]
    for c in (con, rcon):
        c.execute("create table taxonomy_migration_log(tbl text, row_key text, col text, old text, new text, mapping_sha256 text)")
    changed = {"observations.sporely_taxon_id": 0, "observations.taxon_identity_external_id": 0,
               "settings.cloud_snapshot.taxon_identity": 0, "reference_taxon_treatments.taxon_id": 0,
               "curated_reference_forks.sporely_taxon_id": 0}
    with con:
        for oid, sid in con.execute("select id, sporely_taxon_id from observations where sporely_taxon_id is not null").fetchall():
            if sid in m:
                con.execute("update observations set sporely_taxon_id=? where id=?", (m[sid], oid))
                con.execute("insert into taxonomy_migration_log values('observations',?,'sporely_taxon_id',?,?,?)",
                            (oid, sid, m[sid], msha))
                changed["observations.sporely_taxon_id"] += 1
        for oid, ext in con.execute("""select id, taxon_identity_external_id from observations
                where taxon_identity_namespace='sporely_taxon_id'""").fetchall():
            if str(ext).isdigit() and int(ext) in m:
                con.execute("update observations set taxon_identity_external_id=? where id=?", (str(m[int(ext)]), oid))
                con.execute("insert into taxonomy_migration_log values('observations',?,'taxon_identity_external_id',?,?,?)",
                            (oid, ext, m[int(ext)], msha))
                changed["observations.taxon_identity_external_id"] += 1
        for key, val in con.execute("select key, value from settings where key like 'sporely_cloud_snapshot_obs_%'").fetchall():
            try:
                o = json.loads(val)
            except Exception:
                continue
            ti = (o.get("observation") or {}).get("taxon_identity")
            mm = TUPLE.match(str(ti or ""))
            if mm and int(mm.group(1)) in m:
                o["observation"]["taxon_identity"] = f"sporely:{m[int(mm.group(1))]}"
                con.execute("update settings set value=? where key=?", (json.dumps(o), key))
                con.execute("insert into taxonomy_migration_log values('settings',?,'cloud_snapshot.taxon_identity',?,?,?)",
                            (key, ti, o["observation"]["taxon_identity"], msha))
                changed["settings.cloud_snapshot.taxon_identity"] += 1
    with rcon:
        for rid, t in rcon.execute("select id, taxon_id from reference_taxon_treatments").fetchall():
            if str(t or "").isdigit() and int(t) in m:
                rcon.execute("update reference_taxon_treatments set taxon_id=? where id=?", (str(m[int(t)]), rid))
                rcon.execute("insert into taxonomy_migration_log values('reference_taxon_treatments',?,'taxon_id',?,?,?)",
                             (rid, t, m[int(t)], msha))
                changed["reference_taxon_treatments.taxon_id"] += 1
        for rid, t in rcon.execute("select rowid, sporely_taxon_id from curated_reference_forks").fetchall():
            if t in m:
                rcon.execute("update curated_reference_forks set sporely_taxon_id=? where rowid=?", (m[t], rid))
                changed["curated_reference_forks.sporely_taxon_id"] += 1
    # ---- verification
    remaining = con.execute(f"""select count(*) from observations where sporely_taxon_id in ({",".join(map(str, m)) or 0})""").fetchone()[0]
    dangling = [s for (s,) in con.execute("select distinct sporely_taxon_id from observations where sporely_taxon_id is not null")
                if s not in live and s not in m.values()]
    new_ids_live = all(v in live for v in m.values())
    counts_after = {t: con.execute(f"select count(*) from {t}").fetchone()[0] for t in counts_before}
    keys_after = con.execute("select group_concat(coalesce(sync_key,'')||cloud_id) from observations").fetchone()[0]
    out = {
        "scope": scope, "mapping_size": len(m), "mapping_sha256": msha,
        "copy_sha256_before": before, "changed": changed,
        "verify": {
            "retirees_remaining_in_observations": remaining,
            "observation_ids_absent_from_release_after": len(dangling),
            "all_survivors_in_release": new_ids_live,
            "row_counts_unchanged": counts_before == counts_after,
            "sync_key_and_cloud_id_unchanged": keys_before == keys_after,
            "integrity_check": [con.execute("pragma integrity_check").fetchone()[0],
                                rcon.execute("pragma integrity_check").fetchone()[0]],
            "log_rows": con.execute("select count(*) from taxonomy_migration_log").fetchone()[0]
            + rcon.execute("select count(*) from taxonomy_migration_log").fetchone()[0],
        },
    }
    con.close()
    rcon.close()
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    for a_ in ("manifests", "release", "desktop", "reference-db", "work-dir", "out-dir"):
        ap.add_argument("--" + a_, required=True, type=Path)
    a = ap.parse_args(argv)
    for p in (a.desktop, a.reference_db):
        if "Application Support" in str(p.resolve()):
            raise SystemExit(f"refusing to open a live application database: {p}")
    a.work_dir.mkdir(parents=True, exist_ok=True)
    result = {"inventory": {}, "dry_run": {}, "mappings": {}}
    desk = sqlite3.connect(f"file:{a.desktop}?mode=ro", uri=True)
    ref = sqlite3.connect(f"file:{a.reference_db}?mode=ro", uri=True)
    for scope, parts in SCOPES.items():
        m = mapping_for(a.manifests, parts)
        mp = a.out_dir / f"id-mapping-{scope}.json"
        mp.write_text(json.dumps({"scope": scope, "partitions": parts or "all",
                                  "mapping": {str(k): v for k, v in m.items()}}, sort_keys=True, indent=0))
        result["mappings"][scope] = {"file": mp.name, "size": len(m), "sha256": sha(mp)}
        result["inventory"][scope] = inventory(desk, ref, m)
        result["dry_run"][scope] = dry_run(scope, m, a.desktop, a.reference_db, a.release, a.work_dir)
    (a.out_dir / "desktop_migration.json").write_text(json.dumps(result, indent=1, sort_keys=True))
    print(json.dumps({s: {"changed": r["changed"], "verify": r["verify"]} for s, r in result["dry_run"].items()}, indent=1))
    for s, inv in result["inventory"].items():
        for row in inv:
            if row["rows_referencing_retiree"]:
                print(s, row["location"], row["rows_referencing_retiree"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
