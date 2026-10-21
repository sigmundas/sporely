#!/usr/bin/env python3
"""Relationship pass, step 1: extract every relationship/nomenclature field
from the PINNED source archives (read-only), restricted to the fungal COL
usages already in the audit work DB (col_raw), and measure field coverage.

Reads: COL XR archive (NameUsage.tsv, NameRelation.tsv, TypeMaterial.tsv,
TaxonConceptRelation.tsv, VernacularName.tsv), NorTaxa DwC-A (taxon.txt,
vernacularname.txt), Dyntaxa DwC-A (Taxon.csv), the work DB (mode=ro).
Writes one new SQLite file (--out, scratchpad) and a coverage JSON.

  python -B extract_relations.py --workdb <workdb> --col <col archive.zip> \
     --nortaxa <nortaxa archive.zip> --dyntaxa <dyntaxa archive.zip> \
     --out <scratch>/relations.sqlite3 --coverage <relationship-pass/data/field_coverage_raw.json>
"""
from __future__ import annotations

import argparse
import collections
import csv
import io
import json
import sqlite3
import sys
import zipfile
from pathlib import Path

csv.field_size_limit(10 ** 9)
sys.dont_write_bytecode = True


def tsv(z: zipfile.ZipFile, name: str, delimiter="\t"):
    with z.open(name) as h:
        r = csv.reader(io.TextIOWrapper(h, encoding="utf-8", newline=""), delimiter=delimiter,
                       quoting=csv.QUOTE_NONE if delimiter == "\t" else csv.QUOTE_MINIMAL)
        header = next(r)
        yield header
        yield from r


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    for x in ("workdb", "col", "nortaxa", "dyntaxa", "out", "coverage"):
        ap.add_argument("--" + x, required=True, type=Path)
    a = ap.parse_args(argv)
    if a.out.exists():
        sys.exit(f"{a.out} exists")
    w = sqlite3.connect(f"file:{a.workdb}?mode=ro", uri=True)
    fungal = {r[0] for r in w.execute("select id from col_raw")}
    db = sqlite3.connect(a.out)
    db.execute("pragma journal_mode=off")
    cov = {}

    with zipfile.ZipFile(a.col) as z:
        # ---- NameUsage: all columns, fungal rows only
        it = tsv(z, "NameUsage.tsv")
        header = next(it)
        keep = ["col:ID", "col:nameAlternativeID", "col:originalSpelling", "col:combinationAuthorshipID",
                "col:combinationExAuthorship", "col:combinationAuthorshipYear", "col:basionymAuthorshipID",
                "col:basionymExAuthorship", "col:basionymAuthorshipYear", "col:namePhrase", "col:nameReferenceID",
                "col:namePublishedInYear", "col:code", "col:accordingToID", "col:scrutinizer",
                "col:nameRemarks", "col:remarks", "col:link", "col:sourceID"]
        idx = [header.index(k) for k in keep]
        db.execute("create table col_extra(" + ",".join(k.split(":")[1] for k in keep) + ")")
        filled = collections.Counter()
        n = 0
        buf = []
        for row in it:
            if len(row) < len(header) or row[0] not in fungal:
                continue
            n += 1
            for i, h in enumerate(header):
                if row[i]:
                    filled[h] += 1
            buf.append([row[i] for i in idx])
            if len(buf) > 50000:
                db.executemany(f"insert into col_extra values({','.join('?' * len(keep))})", buf)
                buf = []
        db.executemany(f"insert into col_extra values({','.join('?' * len(keep))})", buf)
        db.execute("create index col_extra_id on col_extra(ID)")
        cov["col.NameUsage"] = {"rows_fungal": n, "filled": {h: filled[h] for h in header}}

        # ---- NameRelation
        it = tsv(z, "NameRelation.tsv")
        header = next(it)
        db.execute("create table col_rel(nameID, relatedNameID, sourceID, type, referenceID, remarks)")
        types, n, buf = collections.Counter(), 0, []
        for row in it:
            if len(row) < 4 or (row[0] not in fungal and row[1] not in fungal):
                continue
            n += 1
            types[row[3]] += 1
            buf.append([row[0], row[1], row[2], row[3], row[4], row[6] if len(row) > 6 else ""])
        db.executemany("insert into col_rel values(?,?,?,?,?,?)", buf)
        db.execute("create index col_rel_a on col_rel(nameID)")
        db.execute("create index col_rel_b on col_rel(relatedNameID)")
        cov["col.NameRelation"] = {"rows_fungal": n, "types": dict(types), "header": header}

        # ---- TypeMaterial (counts + basic fields)
        it = tsv(z, "TypeMaterial.tsv")
        header = next(it)
        db.execute("create table col_type(ID, nameID, status, citation)")
        st, n, buf = collections.Counter(), 0, []
        for row in it:
            if len(row) < 5 or row[1] not in fungal:
                continue
            n += 1
            st[row[4]] += 1
            buf.append([row[0], row[1], row[4], row[3][:300]])
        db.executemany("insert into col_type values(?,?,?,?)", buf)
        db.execute("create index col_type_n on col_type(nameID)")
        cov["col.TypeMaterial"] = {"rows_fungal": n, "status": dict(st.most_common(15)), "header": header}

        # ---- TaxonConceptRelation
        it = tsv(z, "TaxonConceptRelation.tsv")
        header = next(it)
        cov["col.TaxonConceptRelation"] = {"rows_total": sum(1 for _ in it), "header": header}

        # ---- VernacularName (Norwegian only)
        it = tsv(z, "VernacularName.tsv")
        header = next(it)
        ti, li, ni = header.index("col:taxonID"), header.index("col:language"), header.index("col:name")
        si = header.index("col:sourceID")
        db.execute("create table col_vern(taxonID, language, name, sourceID)")
        langs, n, buf = collections.Counter(), 0, []
        for row in it:
            if len(row) <= max(ti, li, ni) or row[ti] not in fungal:
                continue
            n += 1
            langs[row[li]] += 1
            if row[li] in ("nob", "nno", "nor", "no", "nb", "nn"):
                buf.append([row[ti], row[li], row[ni], row[si]])
        db.executemany("insert into col_vern values(?,?,?,?)", buf)
        cov["col.VernacularName"] = {"rows_fungal": n, "norwegian_rows": len(buf),
                                     "norwegian_by_source": dict(collections.Counter(b[3] for b in buf)),
                                     "top_languages": dict(langs.most_common(10))}

    with zipfile.ZipFile(a.nortaxa) as z:
        it = tsv(z, "taxon.txt")
        header = next(it)
        db.execute("create table nt_taxon(" + ",".join(f'"{h}"' for h in header) + ")")
        filled, n, buf = collections.Counter(), 0, []
        ki = header.index("kingdom")
        for row in it:
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            n += 1
            if row[ki] == "Fungi":
                for i, h in enumerate(header):
                    if row[i]:
                        filled[h] += 1
                filled["__fungi_rows"] += 1
            buf.append(row[:len(header)])
        db.executemany(f"insert into nt_taxon values({','.join('?' * len(header))})", buf)
        db.execute("create index nt_taxon_tid on nt_taxon(taxonID)")
        cov["nortaxa.taxon"] = {"rows_total": n, "filled_fungi": dict(filled), "header": header}
        it = tsv(z, "vernacularname.txt")
        header = next(it)
        db.execute("create table nt_vern(" + ",".join(f'"{h}"' for h in header) + ")")
        rows = [r[:len(header)] + [""] * (len(header) - len(r)) for r in it]
        db.executemany(f"insert into nt_vern values({','.join('?' * len(header))})", rows)
        cov["nortaxa.vernacularname"] = {"rows_total": len(rows), "header": header}

    with zipfile.ZipFile(a.dyntaxa) as z:
        names = z.namelist()
        with z.open("Taxon.csv") as h:
            sample = h.read(4096).decode("utf-8", "replace")
        delim = "\t" if sample.count("\t") > sample.count(",") else ","
        it = tsv(z, "Taxon.csv", delimiter=delim)
        header = next(it)
        db.execute("create table dyn_taxon(" + ",".join(f'"{h}"' for h in header) + ")")
        rows = [r[:len(header)] + [""] * (len(header) - len(r)) for r in it]
        db.executemany(f"insert into dyn_taxon values({','.join('?' * len(header))})", rows)
        filled = collections.Counter(h for r in rows for h, v in zip(header, r) if v)
        cov["dyntaxa.Taxon"] = {"rows_total": len(rows), "files": names, "header": header,
                                "filled": dict(filled)}
    db.commit()
    a.coverage.write_text(json.dumps(cov, indent=1, ensure_ascii=False))
    print(json.dumps({k: {kk: vv for kk, vv in v.items() if kk in ("rows_fungal", "rows_total", "types")}
                      for k, v in cov.items()}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
