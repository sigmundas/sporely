#!/usr/bin/env python3
"""Relationship pass, step 3: field inventory (present / imported / discarded /
coverage). Coverage comes from extract_relations.py (all fungal rows) and
relate.py (the 2,805 pairs' own rows). Compiler status is from reading the
repository at 19af384; each claim cites file:line.

  python -B field_inventory.py --coverage data/field_coverage_raw.json \
      --summary data/summary.json --out data/field_inventory.csv
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

# (source, file, field, compiler status, reference, pair-coverage key or None)
ROWS = [
    ("col", "NameUsage", "ID", "imported", "normalize_col_xr.py:177", None),
    ("col", "NameUsage", "parentID", "imported (parent; accepted pointer of synonyms)", "normalize_col_xr.py:178", None),
    ("col", "NameUsage", "status", "imported", "normalize_col_xr.py:199", None),
    ("col", "NameUsage", "scientificName / authorship / rank", "imported", "normalize_col_xr.py:196-198", None),
    ("col", "NameUsage", "nameStatus", "imported to raw; used only by vernacular projection, never by identity mapping",
     "normalize_col_xr.py:202; vernacular_projection.py:67,142-145", "col_C_has_nameStatus"),
    ("col", "NameUsage", "namePhrase / remarks / nameRemarks", "imported as concept_annotation (vernacular only)", "normalize_col_xr.py:204-206", None),
    ("col", "NameUsage", "basionymID", "discarded", "normalize_col_xr.py (not read)", "col_C_has_basionymID"),
    ("col", "NameUsage", "alternativeID (inat, dyntaxa, wikidata, ...)", "discarded by the normaliser (external_ids={}); read only by audit/evidence tools",
     "normalize_col_xr.py:208", "col_C_has_inat_alt_id"),
    ("col", "NameUsage", "link (Index Fungorum RecordID)", "discarded", "normalize_col_xr.py (not read)", "col_C_has_index_fungorum_record"),
    ("col", "NameUsage", "sourceID (2030 = NorTaxa, 2073 = Species Fungorum Plus)", "discarded by the normaliser; read by cross_reference_evidence.py",
     "cross_reference_evidence.py:286", "col_C_from_source_2030_nortaxa"),
    ("col", "NameUsage", "combinationAuthorship / basionymAuthorship (+Ex, Year)", "discarded", "normalize_col_xr.py (not read)", None),
    ("col", "NameUsage", "combinationAuthorshipID / basionymAuthorshipID", "absent in practice (0 fungal rows filled)", "-", None),
    ("col", "NameUsage", "nameReferenceID / namePublishedInYear / Page", "discarded", "normalize_col_xr.py (not read)", None),
    ("col", "NameUsage", "nameAlternativeID", "discarded (WoRMS LSIDs only; no Index Fungorum ids)", "-", None),
    ("col", "NameRelation", "basionym / spelling correction / homotypic / based on", "discarded (file not read)", "-", None),
    ("col", "TypeMaterial", "nameID, status, citation", "discarded (file not read)", "-", None),
    ("col", "TaxonConceptRelation", "-", "empty in the pinned archive", "-", None),
    ("col", "VernacularName", "name, language, sourceID", "imported (vernacular pipeline); sourceID 2030 rows are copies of NorTaxa", "vernacular_projection.py", "col_C_has_norwegian_vernacular"),
    ("nortaxa", "taxon.txt", "taxonID / acceptedNameUsageID", "imported (synonym binding)", "national_source.py:70,749", "nortaxa_N_has_synonyms"),
    ("nortaxa", "taxon.txt", "parentNameUsageID", "imported (resolution defect: nortaxa-parent-link-defect.md)", "national_source.py:70", None),
    ("nortaxa", "taxon.txt", "scientificNameAuthorship / taxonomicStatus", "imported", "national_source.py", None),
    ("nortaxa", "taxon.txt", "nomenclaturalStatus", "imported to raw; vernacular projection only", "national_source.py:808-816", "nortaxa_N_has_nomenclaturalStatus"),
    ("nortaxa", "taxon.txt", "namePublishedIn", "discarded (explicitly)", "national_source.py:824-825", "nortaxa_N_has_namePublishedIn"),
    ("nortaxa", "taxon.txt", "originalNameUsageID (basionym); pair column = proxy: accepted name with a parenthetical basionym author", "absent from the archive (meta.xml declares no such term)", "-", "nortaxa_N_parenthetical_author"),
    ("nortaxa", "vernacularname.txt", "vernacularName, language", "imported", "national_source.py", "nortaxa_N_has_norwegian_vernacular"),
    ("dyntaxa", "Taxon.csv", "taxonomicStatus homotypicSynonym / heterotypicSynonym", "imported, but collapsed into 'synonym'", "compile_release.py:2162-2167", None),
    ("dyntaxa", "Taxon.csv", "nomenclaturalStatus / taxonRemarks", "not used for identity", "-", None),
]
COL_KEYS = {"combinationAuthorshipID": "col:combinationAuthorshipID", "basionymID": "col:basionymID", "alternativeID": "col:alternativeID", "link": "col:link",
            "nameStatus": "col:nameStatus", "sourceID": "col:sourceID", "nameReferenceID": "col:nameReferenceID",
            "combinationAuthorship": "col:combinationAuthorship", "basionymAuthorship": "col:basionymAuthorship",
            "nameAlternativeID": "col:nameAlternativeID", "combinationAuthorshipID": "col:combinationAuthorshipID"}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    for x in ("coverage", "summary", "out"):
        ap.add_argument("--" + x, required=True, type=Path)
    a = ap.parse_args(argv)
    cov = json.loads(a.coverage.read_text())
    pc = json.loads(a.summary.read_text())["pair_row_coverage"]
    nu = cov["col.NameUsage"]
    ntf = cov["nortaxa.taxon"]["filled_fungi"]
    nf = ntf["__fungi_rows"]
    with a.out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["source", "file", "field", "present", "compiler_status", "reference",
                    "coverage_all_fungal_rows_pct", "coverage_2805_pairs"])
        for src, fil, field, status, ref, key in ROWS:
            pct = ""
            if src == "col" and fil == "NameUsage":
                k = next((v for kk, v in COL_KEYS.items() if field.startswith(kk)), None)
                if k:
                    pct = round(100 * nu["filled"].get(k, 0) / nu["rows_fungal"], 1)
            elif src == "nortaxa" and fil == "taxon.txt":
                k = field.split(" ")[0].split("/")[0].strip()
                if k in ntf:
                    pct = round(100 * ntf[k] / nf, 1)
            elif fil == "NameRelation":
                pct = json.dumps(cov["col.NameRelation"]["types"])
            elif fil == "TypeMaterial":
                pct = f'{cov["col.TypeMaterial"]["rows_fungal"]} rows'
            present = "no" if "absent" in status else "yes"
            pair = f'{pc[key]} / {pc["pairs"]}' if key else ""
            w.writerow([src, fil, field, present, status, ref, pct, pair])
    print(a.out.read_text())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
