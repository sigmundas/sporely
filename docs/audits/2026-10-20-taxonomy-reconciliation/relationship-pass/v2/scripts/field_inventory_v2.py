#!/usr/bin/env python3
"""Pass 2: corrected field inventory (review-codex finding 7). Reads pass 1's
data/field_inventory.csv (unchanged) and the relations DB; writes
v2/data/field_inventory.csv (UTF-8 with BOM).

  python -B field_inventory_v2.py --pass1 ../data/field_inventory.csv --relations <relations.sqlite3> --out data/field_inventory.csv
"""
import argparse
import csv
import sqlite3
from pathlib import Path

ap = argparse.ArgumentParser()
for x in ("pass1", "relations", "out"):
    ap.add_argument("--" + x, required=True, type=Path)
a = ap.parse_args()
d = sqlite3.connect(f"file:{a.relations}?mode=ro", uri=True)
n = d.execute("select count(*) from col_extra").fetchone()[0]
if_links = d.execute("select count(*) from col_extra where link like '%indexfungorum%RecordID=%'").fetchone()[0]
any_link = d.execute("select count(*) from col_extra where link<>''").fetchone()[0]
irmng, worms = d.execute("select sum(nameAlternativeID like '%irmng%'), sum(nameAlternativeID like '%marinespecies%') "
                         "from col_extra").fetchone()
rows = list(csv.reader(a.pass1.open(encoding="utf-8-sig")))
out = [rows[0]]
for r in rows[1:]:
    f = r[2]
    if f.startswith("link"):
        out.append(r[:6] + [f"{round(100 * any_link / n, 1)} any link; Index Fungorum RecordID {if_links}/{n} = "
                             f"{round(100 * if_links / n, 1)}", r[7]])
        continue
    if f.startswith("combinationAuthorship / basionymAuthorship"):
        out.append(r[:6] + ["combinationAuthorship 45.4; basionymAuthorship 17.0", r[7]])
        continue
    if f == "nameAlternativeID":
        out.append(r[:4] + [f"discarded (IRMNG {irmng} and WoRMS {worms} LSIDs; no Index Fungorum ids)"] + r[5:])
        continue
    if f.startswith("sourceID"):
        out.append(r[:4] + [r[4] + "; pass 2 applies it as provenance to every COL relation, basionym and vernacular row"] + r[5:])
        continue
    out.append(r)
with a.out.open("w", newline="", encoding="utf-8-sig") as fh:
    csv.writer(fh).writerows(out)
print(open(a.out, encoding="utf-8-sig").read())
