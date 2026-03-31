#!/usr/bin/env python3
"""
Build a multi-language SQLite DB of *accepted* scientific names and vernacular names.

Inputs
------
1) iNaturalist-derived CSV with columns:
     scientificName,en,de,fr,es,da,sv,no,fi,pl,pt,it
   - Multiple names separated by ';' in each cell.
   - This CSV may contain names not present in the Artsdatabanken backbone; those are skipped.

2) Artsdatabanken artsnavnebase (optional but strongly recommended):
   - taxon.txt
   - vernacularname.txt

Behavior
--------
- The SQLite table `taxon_min.taxon_id` is the *Artsdatabanken taxonID* (from taxon.txt "id").
- Only *accepted species* from Artsdatabanken are included (taxonRank=species, taxonomicStatus=valid).
- Synonyms are ignored. (If you need synonym resolution later, add a separate alias table.)
- Norwegian names ("no") are taken from Artsdatabanken vernacularname.txt when provided; otherwise
  Norwegian names from the CSV are used.

Output schema
-------------
taxon_min:
  taxon_id (INTEGER PRIMARY KEY)   -- Artsdatabanken taxonID
  parent_taxon_id (INTEGER)        -- optional, parentNameUsageID
  genus (TEXT)
  specific_epithet (TEXT)
  family (TEXT)

vernacular_min:
  vernacular_id (INTEGER PRIMARY KEY AUTOINCREMENT)
  taxon_id (INTEGER FK->taxon_min)
  language_code (TEXT)
  vernacular_name (TEXT)
  is_preferred_name (INTEGER 0/1)

"""

from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


DEFAULT_LANGS = ["en", "de", "fr", "es", "da", "sv", "no", "fi", "pl", "pt", "it"]
DEFAULT_INPUT_CSV = "vernacular_inat_11lang.csv"
DEFAULT_OUTPUT_DB = "vernacular_multilanguage_legacy.sqlite3"
DEFAULT_NO_TAXON = "taxon.txt"
DEFAULT_NO_VERNACULAR = "vernacularname.txt"


def _set_csv_field_limit() -> None:
    try:
        csv.field_size_limit(1024 * 1024 * 10)
    except OverflowError:
        csv.field_size_limit(2_147_483_647)


def _split_names(raw: str) -> List[str]:
    if not raw:
        return []
    items: List[str] = []
    for part in raw.split(";"):
        name = part.strip()
        if name:
            items.append(name)
    return items


def _parse_scientific_name(value: str) -> Optional[Tuple[str, str]]:
    """
    Parse "Genus species ..." -> (Genus, species).
    Ignores authorships, rank markers, etc.
    """
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    parts = text.replace("\u00a0", " ").split()
    if len(parts) < 2:
        return None
    genus = parts[0].strip().strip(",")
    species = parts[1].strip().strip(",")
    if not genus or not species:
        return None
    return genus, species


def _normalize_taxon(genus: str, species: str) -> Tuple[str, str]:
    genus = genus.strip()
    species = species.strip()
    if genus:
        genus = genus[0].upper() + genus[1:]
    if species:
        species = species.lower()
    return genus, species


def _parse_bool(value: Optional[str]) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def create_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")

    conn.executescript(
        """
        DROP TABLE IF EXISTS vernacular_min;
        DROP TABLE IF EXISTS taxon_min;

        CREATE TABLE taxon_min (
            taxon_id         INTEGER PRIMARY KEY,  -- Artsdatabanken taxonID
            parent_taxon_id  INTEGER,
            genus            TEXT NOT NULL,
            specific_epithet TEXT NOT NULL,
            family           TEXT
        );

        CREATE TABLE vernacular_min (
            vernacular_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            taxon_id          INTEGER NOT NULL,
            language_code     TEXT NOT NULL,
            vernacular_name   TEXT NOT NULL,
            is_preferred_name INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (taxon_id) REFERENCES taxon_min(taxon_id)
        );

        CREATE UNIQUE INDEX idx_vern_unique
            ON vernacular_min(taxon_id, language_code, vernacular_name);

        CREATE INDEX idx_taxon_genus ON taxon_min(genus);
        CREATE INDEX idx_taxon_genus_species ON taxon_min(genus, specific_epithet);
        CREATE INDEX idx_taxon_parent ON taxon_min(parent_taxon_id);

        CREATE INDEX idx_vern_lang_name ON vernacular_min(language_code, vernacular_name);
        CREATE INDEX idx_vern_taxon_lang ON vernacular_min(taxon_id, language_code);
        """
    )


def _insert_vernacular_rows(
    conn: sqlite3.Connection,
    rows: List[Tuple[int, str, str, int]],
) -> None:
    if not rows:
        return
    conn.executemany(
        """
        INSERT INTO vernacular_min
            (taxon_id, language_code, vernacular_name, is_preferred_name)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(taxon_id, language_code, vernacular_name)
        DO UPDATE SET is_preferred_name = CASE
            WHEN excluded.is_preferred_name > vernacular_min.is_preferred_name
            THEN excluded.is_preferred_name
            ELSE vernacular_min.is_preferred_name
        END
        """,
        rows,
    )


def _insert_taxa_bulk(
    conn: sqlite3.Connection,
    taxa_rows: Iterable[Tuple[int, Optional[int], str, str, Optional[str]]],
) -> None:
    """
    Bulk insert taxa. Uses INSERT OR IGNORE and then an UPDATE to fill missing family/parent.
    """
    conn.executemany(
        """
        INSERT OR IGNORE INTO taxon_min
            (taxon_id, parent_taxon_id, genus, specific_epithet, family)
        VALUES (?, ?, ?, ?, ?)
        """,
        taxa_rows,
    )


def _ensure_taxon(
    conn: sqlite3.Connection,
    taxon_id: int,
    genus: str,
    species: str,
    family: Optional[str],
    parent_taxon_id: Optional[int],
) -> None:
    """
    Ensure a taxon row exists. Update missing family/parent if the row already exists.
    """
    conn.execute(
        """
        INSERT OR IGNORE INTO taxon_min
            (taxon_id, parent_taxon_id, genus, specific_epithet, family)
        VALUES (?, ?, ?, ?, ?)
        """,
        (taxon_id, parent_taxon_id, genus, species, family),
    )
    # Fill in missing metadata if we inserted earlier with NULLs
    if family is not None or parent_taxon_id is not None:
        conn.execute(
            """
            UPDATE taxon_min
            SET
              family = COALESCE(family, ?),
              parent_taxon_id = COALESCE(parent_taxon_id, ?)
            WHERE taxon_id = ?
            """,
            (family, parent_taxon_id, taxon_id),
        )


def _load_art_taxa(
    taxon_path: Path,
) -> Tuple[
    Dict[int, Tuple[str, str, Optional[str], Optional[int]]],
    Dict[Tuple[str, str], int],
    int,
    int,
]:
    """
    Load accepted species from Artsdatabanken taxon.txt.

    Returns:
      taxa_by_id: taxonID -> (genus, species, family, parent_taxon_id)
      accepted_id_by_name: (genus,species) -> taxonID
      total_rows, valid_species_rows
    """
    taxa_by_id: Dict[int, Tuple[str, str, Optional[str], Optional[int]]] = {}
    accepted_id_by_name: Dict[Tuple[str, str], int] = {}

    total = 0
    valid = 0

    with taxon_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            total += 1

            raw_id = (row.get("id") or "").strip()
            if not raw_id:
                continue
            try:
                taxon_id = int(raw_id)
            except ValueError:
                continue

            # We only keep accepted *species*
            if (row.get("taxonRank") or "").strip().lower() != "species":
                continue
            status = (row.get("taxonomicStatus") or "").strip().lower()
            if status != "valid":
                continue

            genus = (row.get("genus") or "").strip()
            species = (row.get("specificEpithet") or "").strip()
            if not genus or not species:
                continue
            genus, species = _normalize_taxon(genus, species)

            family = (row.get("family") or "").strip() or None

            parent_raw = (row.get("parentNameUsageID") or "").strip()
            try:
                parent_taxon_id = int(parent_raw) if parent_raw else None
            except ValueError:
                parent_taxon_id = None

            taxa_by_id[taxon_id] = (genus, species, family, parent_taxon_id)
            accepted_id_by_name[(genus, species)] = taxon_id
            valid += 1

    return taxa_by_id, accepted_id_by_name, total, valid


def _merge_norwegian_from_arts(
    conn: sqlite3.Connection,
    vernacular_path: Path,
    art_taxa_by_id: Dict[int, Tuple[str, str, Optional[str], Optional[int]]],
) -> int:
    """
    Insert Norwegian vernacular names from Artsdatabanken vernacularname.txt.
    Returns number of inserted/updated vernacular rows (attempted rows).
    """
    inserted = 0
    batch: List[Tuple[int, str, str, int]] = []

    conn.execute("BEGIN;")
    with vernacular_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            raw_id = (row.get("id") or "").strip()
            if not raw_id:
                continue
            try:
                taxon_id = int(raw_id)
            except ValueError:
                continue

            # Only Norwegian
            if (row.get("countryCode") or "").strip().upper() != "NO":
                continue

            name = (row.get("vernacularName") or "").strip()
            if not name:
                continue

            taxon = art_taxa_by_id.get(taxon_id)
            if not taxon:
                # vernacular row for a taxon we didn't load (e.g. non-species / non-valid), ignore
                continue

            genus, species, family, parent_taxon_id = taxon
            _ensure_taxon(conn, taxon_id, genus, species, family, parent_taxon_id)

            is_pref = 1 if _parse_bool(row.get("isPreferredName")) else 0
            batch.append((taxon_id, "no", name, is_pref))
            inserted += 1

            if len(batch) >= 5000:
                _insert_vernacular_rows(conn, batch)
                batch.clear()

    if batch:
        _insert_vernacular_rows(conn, batch)
        batch.clear()

    conn.commit()
    return inserted


def build_db(csv_path: Path, out_db: Path, no_taxon: Optional[Path], no_names: Optional[Path]) -> None:
    _set_csv_field_limit()

    if out_db.exists():
        out_db.unlink()

    if not no_taxon or not no_taxon.exists():
        raise SystemExit(
            "Missing Artsdatabanken taxon.txt. This script now requires it, "
            "because taxon_id must be the Artsdatabanken taxonID."
        )

    conn = sqlite3.connect(out_db)
    conn.row_factory = sqlite3.Row
    try:
        create_schema(conn)

        # -------- Load accepted taxa from Artsdatabanken --------
        art_taxa_by_id, accepted_id_by_name, total, valid = _load_art_taxa(no_taxon)
        print(f"Artsdatabanken taxon.txt: {valid} valid species out of {total} total rows")

        # Insert *all* accepted taxa up front (so DB is a complete accepted-name backbone)
        conn.execute("BEGIN;")
        _insert_taxa_bulk(
            conn,
            (
                (taxon_id, parent_taxon_id, genus, species, family)
                for taxon_id, (genus, species, family, parent_taxon_id) in art_taxa_by_id.items()
            ),
        )
        conn.commit()

        # -------- CSV import (non-Norwegian by default) --------
        use_arts_no = bool(no_names and no_names.exists())

        total_rows = 0
        parsed_rows = 0
        skipped_not_in_arts = 0
        empty_rows = 0

        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                raise ValueError(f"No header found in {csv_path}")

            fieldnames = [name.strip() for name in reader.fieldnames if name]
            if "scientificName" not in fieldnames:
                raise ValueError("CSV missing required column: scientificName")

            lang_columns = [f for f in fieldnames if f != "scientificName"]
            if not lang_columns:
                lang_columns = list(DEFAULT_LANGS)

            batch: List[Tuple[int, str, str, int]] = []
            BATCH = 5000

            conn.execute("BEGIN;")
            for row in reader:
                total_rows += 1
                sci = (row.get("scientificName") or "").strip()
                parsed = _parse_scientific_name(sci)
                if not parsed:
                    continue
                genus, species = _normalize_taxon(*parsed)
                parsed_rows += 1

                taxon_id = accepted_id_by_name.get((genus, species))
                if taxon_id is None:
                    skipped_not_in_arts += 1
                    continue

                # If Norwegian should come from Arts, skip 'no' from CSV.
                has_any = False
                for lang in lang_columns:
                    lang_code = (lang or "").strip().lower()
                    if not lang_code:
                        continue
                    if lang_code == "no" and use_arts_no:
                        continue
                    if _split_names((row.get(lang) or "")):
                        has_any = True
                        break
                if not has_any:
                    empty_rows += 1

                for lang in lang_columns:
                    lang_code = (lang or "").strip().lower()
                    if not lang_code:
                        continue
                    if lang_code == "no" and use_arts_no:
                        continue

                    names = _split_names((row.get(lang) or ""))
                    if not names:
                        continue

                    for idx, name in enumerate(names):
                        batch.append((taxon_id, lang_code, name, 1 if idx == 0 else 0))

                    if len(batch) >= BATCH:
                        _insert_vernacular_rows(conn, batch)
                        batch.clear()

            if batch:
                _insert_vernacular_rows(conn, batch)
                batch.clear()

            conn.commit()

        print(f"CSV rows: {total_rows} total, {parsed_rows} parsed scientific names")
        print(f"CSV skipped (not in Arts accepted backbone): {skipped_not_in_arts}")
        print(f"CSV rows without any kept translations: {empty_rows}")

        # -------- Norwegian merge from Artsdatabanken --------
        if use_arts_no and no_names:
            inserted = _merge_norwegian_from_arts(conn, no_names, art_taxa_by_id)
            print(f"Norwegian merge: processed {inserted} vernacular rows from Artsdatabanken")
        else:
            print("Norwegian vernacularname.txt not found; using CSV 'no' column (if present).")

        conn.execute("VACUUM;")

        taxa_n = conn.execute("SELECT COUNT(*) FROM taxon_min").fetchone()[0]
        vern_n = conn.execute("SELECT COUNT(*) FROM vernacular_min").fetchone()[0]
        lang_n = conn.execute("SELECT COUNT(DISTINCT language_code) FROM vernacular_min").fetchone()[0]
        print(f"Created: {out_db}")
        print(f"taxon_min rows: {taxa_n}")
        print(f"vernacular_min rows: {vern_n}")
        print(f"languages: {lang_n}")

    finally:
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=DEFAULT_INPUT_CSV, help="Input CSV file")
    ap.add_argument("--out", default=DEFAULT_OUTPUT_DB, help="Output SQLite DB")
    ap.add_argument(
        "--no-taxon",
        default=DEFAULT_NO_TAXON,
        help="Artsdatabanken taxon.txt (REQUIRED; source of taxon_id)",
    )
    ap.add_argument(
        "--no-vernacular",
        default=DEFAULT_NO_VERNACULAR,
        help="Artsdatabanken vernacularname.txt (optional; preferred Norwegian source)",
    )
    args = ap.parse_args()

    csv_path = Path(args.csv).resolve()
    out_db = Path(args.out).resolve()
    no_taxon = Path(args.no_taxon).resolve() if args.no_taxon else None
    no_names = Path(args.no_vernacular).resolve() if args.no_vernacular else None

    if not csv_path.exists():
        raise SystemExit(f"Missing CSV: {csv_path}")
    if not no_taxon or not no_taxon.exists():
        raise SystemExit(f"Missing Artsdatabanken taxon.txt: {no_taxon}")

    build_db(csv_path, out_db, no_taxon, no_names)


if __name__ == "__main__":
    main()
