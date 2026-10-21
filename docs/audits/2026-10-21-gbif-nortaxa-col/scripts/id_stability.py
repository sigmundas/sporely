"""Check whether pinned 26.7 XR usage ids also exist in GBIF's 26.6 XR (ChecklistBank 315557).

Input: data/results.csv. For every COL id in the sample whose pinned provenance is known, ask
/dataset/315557/nameusage/<id>/source and record whether it exists and has the same source record.
Output: data/id_stability.csv (UTF-8 BOM) and a printed tally by source dataset.
"""
import collections, csv, os, sys
sys.path.insert(0, os.path.dirname(__file__))
import fetch
here = os.path.dirname(__file__)
rows = list(csv.DictReader(open(os.path.join(here, "../data/results.csv"), encoding="utf-8-sig")))
seen = {}
for r in rows:
    for part in filter(None, r["provenance"].split(";")):
        cid, src = part.split("<-", 1)
        ds, sid = src.split(":", 1)
        if ds not in ("not_in_pinned_xr", "no_source_record"):
            seen.setdefault(cid, (ds, sid))
out, tally = [], collections.Counter()
for cid, (ds, sid) in sorted(seen.items()):
    b = fetch.get(f"https://api.checklistbank.org/dataset/315557/nameusage/{cid}/source")
    if "sourceDatasetKey" in b:
        st = "same_source_record" if (str(b["sourceDatasetKey"]), str(b.get("sourceId"))) == (ds, sid) else "different_source_record"
    else:
        st = "absent_in_26.6"
    grp = "2030_nortaxa" if ds == "2030" else "other_source"
    tally[(grp, st)] += 1
    out.append({"col_usage_id": cid, "pinned_26_7_source": f"{ds}:{sid}", "in_26_6": st})
with open(os.path.join(here, "../data/id_stability.csv"), "w", encoding="utf-8-sig", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(out[0]))
    w.writeheader(); w.writerows(out)
for k, v in sorted(tally.items()):
    print(k, v)
print(fetch.stats)
