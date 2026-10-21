#!/usr/bin/env python3
"""Build the read-only audit working database for the 2026-10-20 taxonomy
reconciliation audit.

Reads (never writes) the inputs below and writes ONE new SQLite file at
--out (put it outside the repository, e.g. the session scratchpad):

  --release      installed tax-2026.10.07-01 SQLite (byte copy; sha256 e349e8b1...)
  --evidence-dir extracted tax-2026.10.07-01.evidence.tar.gz (sha256 e2d1dfca...)
                 (tar xzf <archive> -C <dir>; uses compiler/*.jsonl and
                 inputs/concept_supersessions.json)
  --nortaxa      sources/nortaxa/1.284/archive.zip   (sha256 29c11c54...)
  --col          sources/col_xr/2026-07-17-XR/archive.zip (sha256 397d701c...)
  --observations optional: a COPY of the desktop mushrooms.db; only
                 aggregate counts per sporely_taxon_id are stored.

Tables written: taxa, usages, proposals, supersessions, nt_raw, col_raw,
obs_desktop, inputs. The release itself is ATTACHed by the analysis scripts.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import sqlite3
import sys
import unicodedata
import zipfile
from pathlib import Path

csv.field_size_limit(10 ** 9)


def name_key(value: str) -> str:
    """Same normalisation as cross_source_mapping._canonical_name."""
    normalized = unicodedata.normalize("NFC", str(value or "").strip())
    return " ".join(p.casefold() for p in normalized.split())


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--release", required=True, type=Path)
    p.add_argument("--evidence-dir", required=True, type=Path)
    p.add_argument("--nortaxa", required=True, type=Path)
    p.add_argument("--col", required=True, type=Path)
    p.add_argument("--observations", type=Path)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--skip-hash", action="store_true")
    a = p.parse_args(argv)
    if a.out.exists():
        sys.exit(f"{a.out} exists; refusing to overwrite")
    db = sqlite3.connect(a.out)
    db.execute("pragma journal_mode=off")
    db.execute("pragma synchronous=off")
    db.execute("create table inputs(name text, path text, sha256 text)")
    if not a.skip_hash:
        for n, path in [("release", a.release), ("nortaxa", a.nortaxa), ("col", a.col)]:
            db.execute("insert into inputs values(?,?,?)", (n, str(path), sha256(path)))

    comp = a.evidence_dir / "compiler"
    # ---- taxa
    db.execute("""create table taxa(sid integer primary key, src text, ns text,
        ident text, name text, nkey text, authorship text, rank text, status text,
        kingdom text, phylum text, class text, ord text, family text, genus text,
        sp_ep text, infra_ep text, parent_ns text, parent_id text)""")
    rows = []
    with (comp / "taxa.jsonl").open() as f:
        for line in f:
            r = json.loads(line)
            c = r.get("classification") or {}
            pr = r.get("parent_name_usage_id") or {}
            u = r["canonical_source_usage"]
            rows.append((r["sporely_taxon_id"], r["canonical_source_code"], u["namespace"],
                         u["identifier"], r["scientific_name"], name_key(r["scientific_name"]),
                         r.get("authorship") or "", r.get("rank") or "",
                         r.get("taxonomic_status") or "", c.get("kingdom", ""),
                         c.get("phylum", ""), c.get("class", ""), c.get("order", ""),
                         c.get("family", ""), c.get("genus", ""), c.get("specific_epithet", ""),
                         c.get("infraspecific_epithet", ""), pr.get("namespace"), pr.get("value")))
    db.executemany("insert into taxa values(" + ",".join("?" * 19) + ")", rows)
    del rows
    db.execute("create index taxa_nkey on taxa(nkey)")
    db.execute("create index taxa_ident on taxa(src, ident)")

    # ---- usages
    db.execute("""create table usages(sid integer, src text, ns text, ident text,
        name text, nkey text, authorship text, rank text, status text, binding text,
        bec text, alias_reason text, accepted_ident text, superseded_from integer)""")
    rows = []
    with (comp / "source_usages.jsonl").open() as f:
        for line in f:
            r = json.loads(line)
            u = r["source_usage"]
            acc = r.get("accepted_source_usage") or {}
            rows.append((r["sporely_taxon_id"], u["source"], u["namespace"], u["identifier"],
                         r.get("scientific_name") or "", name_key(r.get("scientific_name") or ""),
                         r.get("authorship") or "", r.get("rank") or "",
                         r.get("taxonomic_status") or "", r.get("identity_binding") or "",
                         r.get("bridge_evidence_class") or "", r.get("alias_reason") or "",
                         acc.get("identifier"), r.get("superseded_from_sporely_taxon_id")))
            if len(rows) > 200000:
                db.executemany("insert into usages values(" + ",".join("?" * 14) + ")", rows)
                rows = []
    db.executemany("insert into usages values(" + ",".join("?" * 14) + ")", rows)
    del rows
    db.execute("create index usages_sid on usages(sid)")
    db.execute("create index usages_ident on usages(src, ident)")
    db.execute("create index usages_nkey on usages(nkey)")

    # ---- cross-source proposals (compiler mappings)
    db.execute("""create table proposals(nt_ident text, proposal_class text, reason text,
        target_ident text, n_candidates integer, candidates text, identity_applied integer,
        authorship_equal integer, status_ok integer, kingdom_ok integer)""")
    rows = []
    with (comp / "mappings.jsonl").open() as f:
        for line in f:
            r = json.loads(line)
            if r.get("kind") != "cross_source_proposal":
                continue
            ev = r.get("evidence") or {}
            ch = ev.get("checks") or {}
            rows.append((r["source_usage"]["identifier"], r["proposal_class"], ev.get("reason"),
                         (r.get("target") or {}).get("identifier"), len(r.get("candidates") or []),
                         json.dumps([c["identifier"] for c in r.get("candidates") or []]),
                         int(bool(r.get("identity_applied"))),
                         int(bool((ch.get("authorship_match") or {}).get("authorship_equal"))),
                         int((ch.get("status_compatible") or {}).get("result") == "pass"),
                         int((ch.get("kingdom_fungi") or {}).get("result") == "pass")))
    db.executemany("insert into proposals values(" + ",".join("?" * 10) + ")", rows)
    db.execute("create index proposals_nt on proposals(nt_ident)")

    # ---- supersessions (as compiled)
    sup = json.loads((a.evidence_dir / "inputs" / "concept_supersessions.json").read_text())
    db.execute("""create table supersessions(supersession_id text, superseded_sid integer,
        cur_src text, cur_ns text, cur_ident text, relationship text, review_status text,
        manifest_id text)""")
    db.executemany("insert into supersessions values(?,?,?,?,?,?,?,?)", [
        (s["supersession_id"], s["superseded_sporely_taxon_id"],
         s["current_source_usage"]["source"], s["current_source_usage"]["namespace"],
         s["current_source_usage"]["identifier"], s["relationship"], s["review_status"],
         (s.get("approved_manifest") or {}).get("manifest_id"))
        for s in sup["supersessions"]])

    # ---- NorTaxa raw rows (all rows; kingdom kept for filtering)
    db.execute("""create table nt_raw(id text, taxon_id text, accepted_id text, parent_id text,
        name text, nkey text, authorship text, rank text, status text, nom_status text,
        kingdom text, phylum text, class text, ord text, family text, genus text)""")
    rows = []
    with zipfile.ZipFile(a.nortaxa) as z, z.open("taxon.txt") as h:
        rd = csv.DictReader(io.TextIOWrapper(h, "utf-8"), delimiter="\t", quoting=csv.QUOTE_NONE)
        for r in rd:
            rows.append((r["id"], r["taxonID"], r["acceptedNameUsageID"], r["parentNameUsageID"],
                         r["scientificName"], name_key(r["scientificName"]),
                         r["scientificNameAuthorship"], (r["taxonRank"] or "").casefold(),
                         r["taxonomicStatus"], r["nomenclaturalStatus"], r["kingdom"], r["phylum"],
                         r["class"], r["order"], r["family"], r["genus"]))
    db.executemany("insert into nt_raw values(" + ",".join("?" * 16) + ")", rows)
    del rows
    db.execute("create index nt_raw_tid on nt_raw(taxon_id)")
    db.execute("create index nt_raw_acc on nt_raw(accepted_id)")
    db.execute("create index nt_raw_nkey on nt_raw(nkey)")

    # ---- COL raw rows, Fungi kingdom only
    cols = ["col:ID", "col:alternativeID", "col:sourceID", "col:parentID", "col:basionymID",
            "col:status", "col:scientificName", "col:authorship", "col:rank",
            "col:combinationAuthorship", "col:basionymAuthorship", "col:nameStatus",
            "col:genus", "col:family", "col:order", "col:class", "col:phylum", "col:kingdom",
            "col:link", "clb:merged"]
    db.execute("""create table col_raw(id text, alt_id text, source_id text, parent_id text,
        basionym_id text, status text, name text, nkey text, authorship text, rank text,
        comb_auth text, bas_auth text, name_status text, genus text, family text, ord text,
        class text, phylum text, kingdom text, link text, merged text)""")
    rows = []
    skipped = 0
    with zipfile.ZipFile(a.col) as z, z.open("NameUsage.tsv") as h:
        stream = io.TextIOWrapper(h, "utf-8")
        header = stream.readline().rstrip("\n").split("\t")
        idx = [header.index(c) for c in cols]
        kpos = header.index("col:kingdom")
        spos = header.index("col:status")
        for line in stream:
            parts = line.rstrip("\n").split("\t")
            kingdom = parts[kpos] if kpos < len(parts) else ""
            # COL publishes classification only on accepted usages; keep every
            # non-accepted usage here and drop non-fungal ones after loading.
            if kingdom != "Fungi" and parts[spos] in ("accepted", "provisionally accepted"):
                skipped += 1
                continue
            v = [parts[i] if i < len(parts) else "" for i in idx]
            rows.append(v[:7] + [name_key(v[6])] + v[7:])
            if len(rows) > 200000:
                db.executemany("insert into col_raw values(" + ",".join("?" * 21) + ")", rows)
                rows = []
    db.executemany("insert into col_raw values(" + ",".join("?" * 21) + ")", rows)
    db.execute("create index col_raw_id on col_raw(id)")
    db.execute("create index col_raw_parent on col_raw(parent_id)")
    cur = db.execute("""delete from col_raw where kingdom <> 'Fungi' and parent_id not in
        (select id from col_raw where kingdom = 'Fungi')""")
    db.execute("insert into inputs values('col_rows_skipped_non_fungi', ?, null)",
               (str(skipped + cur.rowcount),))
    db.execute("create index col_raw_nkey on col_raw(nkey)")

    # ---- desktop observation aggregates (no personal data)
    db.execute("create table obs_desktop(sid integer primary key, n integer)")
    if a.observations:
        o = sqlite3.connect(f"file:{a.observations}?mode=ro", uri=True)
        db.executemany("insert into obs_desktop values(?,?)", o.execute(
            "select sporely_taxon_id, count(*) from observations "
            "where sporely_taxon_id is not null group by 1").fetchall())
        o.close()
    db.commit()
    db.close()
    print("ok", a.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
