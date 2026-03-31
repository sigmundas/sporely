#!/usr/bin/env python3
"""
Build a unified local taxonomy DB for Sporely.

The output DB keeps the existing ``taxon_min`` / ``vernacular_min`` tables, but
extends them so one local taxon row can carry:

- Norwegian Artsdatabanken taxon ID
- Swedish Artportalen taxon ID
- iNaturalist taxon ID (if a local mapping file is available)
- preferred scientific names for Norwegian and Swedish presentation
- scientific-name aliases, including synonyms

The script starts from the same fresh sources as the current multilingual DB
builder, then overlays Artportalen Swedish names and synonym mappings.
Artportalen Swedish vernacular names take precedence over older Swedish names,
while older names are preserved as secondary names.
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional

try:
    from build_multilang_vernacular_db import (
        _normalize_taxon,
        _parse_scientific_name,
        _set_csv_field_limit,
        build_db as build_base_db,
    )
except ImportError:  # pragma: no cover - fallback when imported as a module
    from database.build_multilang_vernacular_db import (
        _normalize_taxon,
        _parse_scientific_name,
        _set_csv_field_limit,
        build_db as build_base_db,
    )


SCRIPT_DIR = Path(__file__).resolve().parent

DEFAULT_INPUT_CSV = SCRIPT_DIR / "vernacular_inat_11lang.csv"
DEFAULT_OUTPUT_DB = SCRIPT_DIR / "vernacular_multilanguage.sqlite3"
DEFAULT_NO_TAXON = SCRIPT_DIR / "taxon.txt"
DEFAULT_NO_VERNACULAR = SCRIPT_DIR / "vernacularname.txt"
DEFAULT_ARTPORTALEN_MATCHED = SCRIPT_DIR / "artportalen_taxon_ids_by_genus.csv"
DEFAULT_ARTPORTALEN_RECONCILED = SCRIPT_DIR / "artportalen_taxon_ids_swedish_only_reconciled.csv"


@dataclass(frozen=True)
class NorwayTaxonRow:
    taxon_id: int
    scientific_name: str
    genus: str
    specific_epithet: str
    family: Optional[str]
    parent_taxon_id: Optional[int]
    taxon_rank: Optional[str]
    taxonomic_status: Optional[str]
    accepted_taxon_id: Optional[int]


def _int_or_none(value: object) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _scientific_parts(scientific_name: str) -> tuple[str, str]:
    parsed = _parse_scientific_name(scientific_name)
    if parsed:
        return _normalize_taxon(*parsed)

    parts = scientific_name.strip().replace("\u00a0", " ").split()
    if not parts:
        return "", ""
    genus = parts[0].strip().strip(",")
    specific = parts[1].strip().strip(",") if len(parts) > 1 else ""
    genus = genus[:1].upper() + genus[1:] if genus else ""
    specific = specific.lower() if specific else ""
    return genus, specific


def _canonical_scientific_name(genus: str, specific_epithet: str, fallback: str = "") -> str:
    if genus and specific_epithet:
        return f"{genus} {specific_epithet}"
    return fallback.strip()


def _looks_like_vernacular(name: str, scientific_name: str) -> bool:
    name = _clean_text(name)
    scientific_name = _clean_text(scientific_name)
    if not name:
        return False
    if not scientific_name:
        return True
    return name.casefold() != scientific_name.casefold()


def _load_norway_taxonomy(
    taxon_path: Path,
) -> tuple[dict[int, NorwayTaxonRow], dict[tuple[str, str], int], list[NorwayTaxonRow]]:
    rows_by_id: dict[int, NorwayTaxonRow] = {}
    accepted_species_by_name: dict[tuple[str, str], int] = {}
    synonym_rows: list[NorwayTaxonRow] = []

    with taxon_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            taxon_id = _int_or_none(row.get("id"))
            if taxon_id is None:
                continue

            genus = _clean_text(row.get("genus"))
            specific = _clean_text(row.get("specificEpithet"))
            scientific_name = _clean_text(row.get("scientificName"))
            if not scientific_name:
                scientific_name = _canonical_scientific_name(genus, specific)

            if (not genus or not specific) and scientific_name:
                parsed_genus, parsed_specific = _scientific_parts(scientific_name)
                genus = genus or parsed_genus
                specific = specific or parsed_specific

            genus, specific = _normalize_taxon(genus, specific) if genus and specific else (genus, specific)

            item = NorwayTaxonRow(
                taxon_id=taxon_id,
                scientific_name=scientific_name,
                genus=genus,
                specific_epithet=specific,
                family=_clean_text(row.get("family")) or None,
                parent_taxon_id=_int_or_none(row.get("parentNameUsageID")),
                taxon_rank=_clean_text(row.get("taxonRank")) or None,
                taxonomic_status=_clean_text(row.get("taxonomicStatus")) or None,
                accepted_taxon_id=_int_or_none(row.get("acceptedNameUsageID")),
            )
            rows_by_id[taxon_id] = item

            if (
                (item.taxon_rank or "").lower() == "species"
                and (item.taxonomic_status or "").lower() == "valid"
                and item.genus
                and item.specific_epithet
            ):
                accepted_species_by_name[(item.genus, item.specific_epithet)] = item.taxon_id

            if (item.taxonomic_status or "").lower() != "valid" and item.accepted_taxon_id:
                synonym_rows.append(item)

    return rows_by_id, accepted_species_by_name, synonym_rows


def _load_inat_mapping(path: Path | None) -> dict[str, int]:
    if not path or not path.exists():
        return {}

    mapping: dict[str, int] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return mapping

        scientific_field = None
        id_field = None
        for field in reader.fieldnames:
            lowered = (field or "").strip().lower()
            if lowered in {"scientificname", "scientific_name", "name"} and scientific_field is None:
                scientific_field = field
            if lowered in {"inaturalist_taxon_id", "inat_taxon_id", "inat_id", "taxon_id"} and id_field is None:
                id_field = field

        if not scientific_field or not id_field:
            raise SystemExit(
                f"Unsupported iNaturalist mapping CSV header in {path}. "
                "Expected a scientific-name column and an iNaturalist taxon-id column."
            )

        for row in reader:
            scientific_name = _clean_text(row.get(scientific_field))
            inat_taxon_id = _int_or_none(row.get(id_field))
            if scientific_name and inat_taxon_id is not None:
                mapping[scientific_name] = inat_taxon_id

    return mapping


def _collect_duplicate_external_ids(path: Path, column_name: str) -> set[int]:
    counts: dict[int, int] = defaultdict(int)
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            external_id = _int_or_none(row.get(column_name))
            if external_id is not None:
                counts[external_id] += 1
    return {external_id for external_id, count in counts.items() if count > 1}


def _swedish_id_is_available(conn: sqlite3.Connection, taxon_id: int, swedish_taxon_id: int) -> bool:
    row = conn.execute(
        """
        SELECT taxon_id
        FROM taxon_min
        WHERE swedish_taxon_id = ?
        LIMIT 1
        """,
        (swedish_taxon_id,),
    ).fetchone()
    return row is None or row[0] == taxon_id


def _create_extended_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        ALTER TABLE taxon_min ADD COLUMN norwegian_taxon_id INTEGER;
        ALTER TABLE taxon_min ADD COLUMN swedish_taxon_id INTEGER;
        ALTER TABLE taxon_min ADD COLUMN inaturalist_taxon_id INTEGER;
        ALTER TABLE taxon_min ADD COLUMN canonical_scientific_name TEXT;
        ALTER TABLE taxon_min ADD COLUMN taxon_rank TEXT;
        ALTER TABLE taxon_min ADD COLUMN taxonomic_status TEXT;
        ALTER TABLE taxon_min ADD COLUMN source_system TEXT;
        ALTER TABLE taxon_min ADD COLUMN preferred_scientific_name_no TEXT;
        ALTER TABLE taxon_min ADD COLUMN preferred_scientific_name_sv TEXT;
        ALTER TABLE vernacular_min ADD COLUMN source TEXT;

        CREATE TABLE scientific_name_min (
            scientific_name_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            taxon_id            INTEGER NOT NULL,
            language_code       TEXT NOT NULL,
            scientific_name     TEXT NOT NULL,
            is_preferred_name   INTEGER NOT NULL DEFAULT 0,
            source              TEXT,
            note                TEXT,
            FOREIGN KEY (taxon_id) REFERENCES taxon_min(taxon_id)
        );

        CREATE TABLE taxon_external_id_min (
            external_id_row_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            taxon_id            INTEGER NOT NULL,
            source_system       TEXT NOT NULL,
            external_id         INTEGER NOT NULL,
            id_role             TEXT NOT NULL,
            is_preferred        INTEGER NOT NULL DEFAULT 0,
            external_name       TEXT,
            note                TEXT,
            FOREIGN KEY (taxon_id) REFERENCES taxon_min(taxon_id)
        );

        CREATE UNIQUE INDEX idx_taxon_no_id
            ON taxon_min(norwegian_taxon_id)
            WHERE norwegian_taxon_id IS NOT NULL;
        CREATE UNIQUE INDEX idx_taxon_sv_id
            ON taxon_min(swedish_taxon_id)
            WHERE swedish_taxon_id IS NOT NULL;
        CREATE UNIQUE INDEX idx_taxon_inat_id
            ON taxon_min(inaturalist_taxon_id)
            WHERE inaturalist_taxon_id IS NOT NULL;
        CREATE INDEX idx_taxon_canonical_name
            ON taxon_min(canonical_scientific_name);
        CREATE INDEX idx_taxon_source_system
            ON taxon_min(source_system);

        CREATE UNIQUE INDEX idx_scientific_name_unique
            ON scientific_name_min(taxon_id, language_code, scientific_name);
        CREATE INDEX idx_scientific_name_lookup
            ON scientific_name_min(language_code, scientific_name);

        CREATE UNIQUE INDEX idx_external_source_id
            ON taxon_external_id_min(source_system, external_id, taxon_id);
        CREATE INDEX idx_external_taxon_source
            ON taxon_external_id_min(taxon_id, source_system);
        """
    )


def _upsert_taxon(
    conn: sqlite3.Connection,
    *,
    taxon_id: int,
    parent_taxon_id: int | None,
    genus: str,
    specific_epithet: str,
    family: str | None,
    norwegian_taxon_id: int | None,
    swedish_taxon_id: int | None,
    inaturalist_taxon_id: int | None,
    canonical_scientific_name: str,
    taxon_rank: str | None,
    taxonomic_status: str | None,
    source_system: str,
    preferred_scientific_name_no: str | None,
    preferred_scientific_name_sv: str | None,
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO taxon_min (
            taxon_id,
            parent_taxon_id,
            genus,
            specific_epithet,
            family,
            norwegian_taxon_id,
            swedish_taxon_id,
            inaturalist_taxon_id,
            canonical_scientific_name,
            taxon_rank,
            taxonomic_status,
            source_system,
            preferred_scientific_name_no,
            preferred_scientific_name_sv
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            taxon_id,
            parent_taxon_id,
            genus or canonical_scientific_name,
            specific_epithet or canonical_scientific_name,
            family,
            norwegian_taxon_id,
            swedish_taxon_id,
            inaturalist_taxon_id,
            canonical_scientific_name,
            taxon_rank,
            taxonomic_status,
            source_system,
            preferred_scientific_name_no,
            preferred_scientific_name_sv,
        ),
    )
    conn.execute(
        """
        UPDATE taxon_min
        SET
            parent_taxon_id = COALESCE(parent_taxon_id, ?),
            family = COALESCE(family, ?),
            norwegian_taxon_id = COALESCE(norwegian_taxon_id, ?),
            swedish_taxon_id = COALESCE(swedish_taxon_id, ?),
            inaturalist_taxon_id = COALESCE(inaturalist_taxon_id, ?),
            canonical_scientific_name = COALESCE(canonical_scientific_name, ?),
            taxon_rank = COALESCE(taxon_rank, ?),
            taxonomic_status = COALESCE(taxonomic_status, ?),
            source_system = CASE
                WHEN source_system IS NULL OR source_system = ''
                THEN ?
                ELSE source_system
            END,
            preferred_scientific_name_no = COALESCE(preferred_scientific_name_no, ?),
            preferred_scientific_name_sv = COALESCE(preferred_scientific_name_sv, ?)
        WHERE taxon_id = ?
        """,
        (
            parent_taxon_id,
            family,
            norwegian_taxon_id,
            swedish_taxon_id,
            inaturalist_taxon_id,
            canonical_scientific_name,
            taxon_rank,
            taxonomic_status,
            source_system,
            preferred_scientific_name_no,
            preferred_scientific_name_sv,
            taxon_id,
        ),
    )


def _upsert_external_id(
    conn: sqlite3.Connection,
    *,
    taxon_id: int,
    source_system: str,
    external_id: int,
    id_role: str,
    is_preferred: bool,
    external_name: str | None = None,
    note: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO taxon_external_id_min (
            taxon_id,
            source_system,
            external_id,
            id_role,
            is_preferred,
            external_name,
            note
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_system, external_id, taxon_id)
        DO UPDATE SET
            id_role = CASE
                WHEN taxon_external_id_min.id_role = 'accepted' THEN taxon_external_id_min.id_role
                ELSE excluded.id_role
            END,
            is_preferred = CASE
                WHEN excluded.is_preferred > taxon_external_id_min.is_preferred
                THEN excluded.is_preferred
                ELSE taxon_external_id_min.is_preferred
            END,
            external_name = COALESCE(excluded.external_name, taxon_external_id_min.external_name),
            note = COALESCE(excluded.note, taxon_external_id_min.note)
        """,
        (
            taxon_id,
            source_system,
            external_id,
            id_role,
            1 if is_preferred else 0,
            external_name,
            note,
        ),
    )


def _upsert_scientific_name(
    conn: sqlite3.Connection,
    *,
    taxon_id: int,
    language_code: str,
    scientific_name: str,
    is_preferred: bool,
    source: str,
    note: str | None = None,
) -> None:
    scientific_name = _clean_text(scientific_name)
    if not scientific_name:
        return
    if is_preferred:
        conn.execute(
            """
            UPDATE scientific_name_min
            SET is_preferred_name = 0
            WHERE taxon_id = ? AND language_code = ?
            """,
            (taxon_id, language_code),
        )
    conn.execute(
        """
        INSERT INTO scientific_name_min (
            taxon_id,
            language_code,
            scientific_name,
            is_preferred_name,
            source,
            note
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(taxon_id, language_code, scientific_name)
        DO UPDATE SET
            is_preferred_name = CASE
                WHEN excluded.is_preferred_name > scientific_name_min.is_preferred_name
                THEN excluded.is_preferred_name
                ELSE scientific_name_min.is_preferred_name
            END,
            source = COALESCE(scientific_name_min.source, excluded.source),
            note = COALESCE(scientific_name_min.note, excluded.note)
        """,
        (taxon_id, language_code, scientific_name, 1 if is_preferred else 0, source, note),
    )
    target_column = "preferred_scientific_name_sv" if language_code == "sv" else "preferred_scientific_name_no"
    if is_preferred and target_column:
        conn.execute(
            f"UPDATE taxon_min SET {target_column} = ? WHERE taxon_id = ?",
            (scientific_name, taxon_id),
        )


def _upsert_vernacular(
    conn: sqlite3.Connection,
    *,
    taxon_id: int,
    language_code: str,
    vernacular_name: str,
    is_preferred: bool,
    source: str,
) -> None:
    vernacular_name = _clean_text(vernacular_name)
    if not vernacular_name:
        return
    if is_preferred:
        conn.execute(
            """
            UPDATE vernacular_min
            SET is_preferred_name = 0
            WHERE taxon_id = ? AND language_code = ?
            """,
            (taxon_id, language_code),
        )
    conn.execute(
        """
        INSERT INTO vernacular_min (
            taxon_id,
            language_code,
            vernacular_name,
            is_preferred_name,
            source
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(taxon_id, language_code, vernacular_name)
        DO UPDATE SET
            is_preferred_name = CASE
                WHEN excluded.is_preferred_name > vernacular_min.is_preferred_name
                THEN excluded.is_preferred_name
                ELSE vernacular_min.is_preferred_name
            END,
            source = CASE
                WHEN excluded.is_preferred_name > vernacular_min.is_preferred_name
                THEN excluded.source
                ELSE COALESCE(vernacular_min.source, excluded.source)
            END
        """,
        (taxon_id, language_code, vernacular_name, 1 if is_preferred else 0, source),
    )


def _populate_base_metadata(
    conn: sqlite3.Connection,
    norway_rows_by_id: dict[int, NorwayTaxonRow],
    inat_mapping: dict[str, int],
) -> tuple[int, int]:
    existing_taxon_ids = {
        row[0]
        for row in conn.execute("SELECT taxon_id FROM taxon_min").fetchall()
        if row and row[0] is not None
    }
    updates = []
    scientific_alias_rows = []
    external_rows = []

    for taxon_id, row in norway_rows_by_id.items():
        if (row.taxon_rank or "").lower() != "species" or (row.taxonomic_status or "").lower() != "valid":
            continue
        if taxon_id not in existing_taxon_ids:
            continue

        canonical_name = row.scientific_name or _canonical_scientific_name(row.genus, row.specific_epithet)
        inat_id = inat_mapping.get(canonical_name)
        updates.append(
            (
                taxon_id,
                canonical_name,
                row.taxon_rank,
                row.taxonomic_status,
                "norway_backbone",
                canonical_name,
                canonical_name,
                inat_id,
                taxon_id,
            )
        )
        scientific_alias_rows.append((taxon_id, "no", canonical_name, 1, "artsdatabanken", None))
        scientific_alias_rows.append((taxon_id, "sv", canonical_name, 1, "artsdatabanken", None))
        external_rows.append((taxon_id, "artsdatabanken", taxon_id, "accepted", 1, canonical_name, None))

    conn.executemany(
        """
        UPDATE taxon_min
        SET
            norwegian_taxon_id = ?,
            canonical_scientific_name = ?,
            taxon_rank = ?,
            taxonomic_status = ?,
            source_system = ?,
            preferred_scientific_name_no = ?,
            preferred_scientific_name_sv = ?,
            inaturalist_taxon_id = COALESCE(inaturalist_taxon_id, ?)
        WHERE taxon_id = ?
        """,
        updates,
    )

    conn.executemany(
        """
        INSERT INTO scientific_name_min (
            taxon_id,
            language_code,
            scientific_name,
            is_preferred_name,
            source,
            note
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(taxon_id, language_code, scientific_name)
        DO UPDATE SET
            is_preferred_name = CASE
                WHEN excluded.is_preferred_name > scientific_name_min.is_preferred_name
                THEN excluded.is_preferred_name
                ELSE scientific_name_min.is_preferred_name
            END,
            source = COALESCE(scientific_name_min.source, excluded.source)
        """,
        scientific_alias_rows,
    )

    conn.executemany(
        """
        INSERT INTO taxon_external_id_min (
            taxon_id,
            source_system,
            external_id,
            id_role,
            is_preferred,
            external_name,
            note
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_system, external_id, taxon_id)
        DO UPDATE SET
            is_preferred = CASE
                WHEN excluded.is_preferred > taxon_external_id_min.is_preferred
                THEN excluded.is_preferred
                ELSE taxon_external_id_min.is_preferred
            END,
            external_name = COALESCE(excluded.external_name, taxon_external_id_min.external_name)
        """,
        external_rows,
    )

    conn.execute(
        """
        UPDATE vernacular_min
        SET source = CASE
            WHEN language_code = 'no' THEN 'artsdatabanken'
            ELSE 'inat_csv'
        END
        WHERE source IS NULL OR source = ''
        """
    )
    return len(updates), len(scientific_alias_rows)


def _merge_norway_synonyms(conn: sqlite3.Connection, synonym_rows: Iterable[NorwayTaxonRow]) -> int:
    existing_taxon_ids = {
        row[0]
        for row in conn.execute("SELECT taxon_id FROM taxon_min").fetchall()
        if row and row[0] is not None
    }
    inserted = 0
    for row in synonym_rows:
        accepted_id = row.accepted_taxon_id
        if not accepted_id or accepted_id == row.taxon_id:
            continue
        if accepted_id not in existing_taxon_ids:
            continue
        if not row.scientific_name:
            continue
        _upsert_scientific_name(
            conn,
            taxon_id=accepted_id,
            language_code="no",
            scientific_name=row.scientific_name,
            is_preferred=False,
            source="artsdatabanken_synonym",
            note=f"Norwegian source synonym ({row.taxon_rank or 'unknown rank'})",
        )
        inserted += 1
    return inserted


def _ensure_extra_taxon(
    conn: sqlite3.Connection,
    *,
    local_taxon_id: int,
    canonical_scientific_name: str,
    source_system: str,
    norwegian_taxon_id: int | None = None,
    swedish_taxon_id: int | None = None,
    inaturalist_taxon_id: int | None = None,
    norway_source_row: NorwayTaxonRow | None = None,
) -> None:
    genus, specific = _scientific_parts(canonical_scientific_name)
    family = norway_source_row.family if norway_source_row else None
    parent_taxon_id = norway_source_row.parent_taxon_id if norway_source_row else None
    taxon_rank = norway_source_row.taxon_rank if norway_source_row else None
    taxonomic_status = norway_source_row.taxonomic_status if norway_source_row else None
    preferred_no = (
        norway_source_row.scientific_name
        if norway_source_row and norway_source_row.scientific_name
        else canonical_scientific_name
    )
    preferred_sv = canonical_scientific_name

    _upsert_taxon(
        conn,
        taxon_id=local_taxon_id,
        parent_taxon_id=parent_taxon_id,
        genus=genus,
        specific_epithet=specific,
        family=family,
        norwegian_taxon_id=norwegian_taxon_id,
        swedish_taxon_id=swedish_taxon_id,
        inaturalist_taxon_id=inaturalist_taxon_id,
        canonical_scientific_name=canonical_scientific_name,
        taxon_rank=taxon_rank,
        taxonomic_status=taxonomic_status,
        source_system=source_system,
        preferred_scientific_name_no=preferred_no,
        preferred_scientific_name_sv=preferred_sv,
    )
    _upsert_scientific_name(
        conn,
        taxon_id=local_taxon_id,
        language_code="no",
        scientific_name=preferred_no,
        is_preferred=True,
        source="artsdatabanken" if norway_source_row else source_system,
    )
    _upsert_scientific_name(
        conn,
        taxon_id=local_taxon_id,
        language_code="sv",
        scientific_name=preferred_sv,
        is_preferred=True,
        source="artportalen" if swedish_taxon_id is not None else source_system,
    )

    if norwegian_taxon_id is not None:
        _upsert_external_id(
            conn,
            taxon_id=local_taxon_id,
            source_system="artsdatabanken",
            external_id=norwegian_taxon_id,
            id_role="accepted",
            is_preferred=True,
            external_name=preferred_no,
        )
    if swedish_taxon_id is not None:
        _upsert_external_id(
            conn,
            taxon_id=local_taxon_id,
            source_system="artportalen",
            external_id=swedish_taxon_id,
            id_role="accepted",
            is_preferred=True,
            external_name=preferred_sv,
        )


def _merge_artportalen_matches(
    conn: sqlite3.Connection,
    matched_csv: Path,
) -> tuple[int, int, set[int], set[int]]:
    duplicate_swedish_ids = _collect_duplicate_external_ids(matched_csv, "artportalen_taxon_id")
    taxon_ids_with_preferred_sv_scientific: set[int] = set()
    taxon_ids_with_preferred_sv_vernacular: set[int] = set()
    matched_rows = 0
    preferred_vernacular_rows = 0

    with matched_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            norway_taxon_id = _int_or_none(row.get("adb_taxon_id"))
            swedish_taxon_id = _int_or_none(row.get("artportalen_taxon_id"))
            if norway_taxon_id is None or swedish_taxon_id is None:
                continue

            scientific_name = _clean_text(row.get("matched_scientific_name")) or _clean_text(row.get("scientific_name"))
            swedish_name = _clean_text(row.get("swedish_name"))
            note = _clean_text(row.get("match_note")) or None
            is_unique_preferred_id = (
                swedish_taxon_id not in duplicate_swedish_ids
                and _swedish_id_is_available(conn, norway_taxon_id, swedish_taxon_id)
            )

            if is_unique_preferred_id:
                conn.execute(
                    """
                    UPDATE taxon_min
                    SET swedish_taxon_id = COALESCE(swedish_taxon_id, ?)
                    WHERE taxon_id = ?
                    """,
                    (swedish_taxon_id, norway_taxon_id),
                )

            _upsert_external_id(
                conn,
                taxon_id=norway_taxon_id,
                source_system="artportalen",
                external_id=swedish_taxon_id,
                id_role="accepted",
                is_preferred=is_unique_preferred_id,
                external_name=scientific_name or swedish_name or None,
                note=note if is_unique_preferred_id else f"{note or ''}; duplicate_artportalen_id".strip("; "),
            )
            _upsert_scientific_name(
                conn,
                taxon_id=norway_taxon_id,
                language_code="sv",
                scientific_name=scientific_name,
                is_preferred=True,
                source="artportalen",
                note=note,
            )
            taxon_ids_with_preferred_sv_scientific.add(norway_taxon_id)
            matched_rows += 1

            if _looks_like_vernacular(swedish_name, scientific_name):
                _upsert_vernacular(
                    conn,
                    taxon_id=norway_taxon_id,
                    language_code="sv",
                    vernacular_name=swedish_name,
                    is_preferred=True,
                    source="artportalen",
                )
                taxon_ids_with_preferred_sv_vernacular.add(norway_taxon_id)
                preferred_vernacular_rows += 1

    return (
        matched_rows,
        preferred_vernacular_rows,
        taxon_ids_with_preferred_sv_scientific,
        taxon_ids_with_preferred_sv_vernacular,
    )


def _merge_artportalen_reconciled(
    conn: sqlite3.Connection,
    reconciled_csv: Path,
    norway_rows_by_id: dict[int, NorwayTaxonRow],
    inat_mapping: dict[str, int],
    preferred_sv_scientific: set[int],
    preferred_sv_vernacular: set[int],
) -> dict[str, int]:
    stats = defaultdict(int)

    with reconciled_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            status = _clean_text(row.get("norway_match_status")) or "unknown"
            swedish_taxon_id = _int_or_none(row.get("artportalen_taxon_id"))
            scientific_name = _clean_text(row.get("scientific_name"))
            swedish_name = _clean_text(row.get("swedish_name"))
            norway_taxon_id = _int_or_none(row.get("norway_taxon_id"))
            norway_accepted_taxon_id = _int_or_none(row.get("norway_accepted_taxon_id"))
            accepted_scientific_name = _clean_text(row.get("norway_accepted_scientific_name"))
            note = _clean_text(row.get("note")) or None

            if swedish_taxon_id is None or not scientific_name:
                continue

            local_taxon_id: int
            norway_source_row: NorwayTaxonRow | None = None
            preferred_sv_name = scientific_name
            id_role = "accepted"
            should_set_preferred_sv_scientific = False
            should_set_preferred_sv_vernacular = False

            if status == "synonym_in_taxon_txt" and norway_accepted_taxon_id is not None:
                local_taxon_id = norway_accepted_taxon_id
                norway_source_row = norway_rows_by_id.get(local_taxon_id)
                _ensure_extra_taxon(
                    conn,
                    local_taxon_id=local_taxon_id,
                    canonical_scientific_name=accepted_scientific_name or scientific_name,
                    source_system="norway_backbone" if norway_source_row else "artportalen_mapped_synonym",
                    norwegian_taxon_id=local_taxon_id if norway_source_row else None,
                    swedish_taxon_id=None,
                    inaturalist_taxon_id=inat_mapping.get(accepted_scientific_name or scientific_name),
                    norway_source_row=norway_source_row,
                )
                preferred_sv_name = scientific_name
                id_role = "synonym"
                should_set_preferred_sv_scientific = local_taxon_id not in preferred_sv_scientific
                should_set_preferred_sv_vernacular = (
                    _looks_like_vernacular(swedish_name, scientific_name)
                    and local_taxon_id not in preferred_sv_vernacular
                )
            elif status in {"accepted_species_in_taxon_txt", "accepted_non_species_in_taxon_txt"} and norway_taxon_id:
                local_taxon_id = norway_taxon_id
                norway_source_row = norway_rows_by_id.get(local_taxon_id)
                _ensure_extra_taxon(
                    conn,
                    local_taxon_id=local_taxon_id,
                    canonical_scientific_name=accepted_scientific_name or scientific_name,
                    source_system="norway_backbone",
                    norwegian_taxon_id=local_taxon_id,
                    swedish_taxon_id=swedish_taxon_id,
                    inaturalist_taxon_id=inat_mapping.get(accepted_scientific_name or scientific_name),
                    norway_source_row=norway_source_row,
                )
                preferred_sv_name = scientific_name
                should_set_preferred_sv_scientific = True
                should_set_preferred_sv_vernacular = _looks_like_vernacular(swedish_name, scientific_name)
            else:
                local_taxon_id = -swedish_taxon_id
                _ensure_extra_taxon(
                    conn,
                    local_taxon_id=local_taxon_id,
                    canonical_scientific_name=scientific_name,
                    source_system="artportalen_only",
                    norwegian_taxon_id=None,
                    swedish_taxon_id=swedish_taxon_id,
                    inaturalist_taxon_id=inat_mapping.get(scientific_name),
                    norway_source_row=None,
                )
                preferred_sv_name = scientific_name
                should_set_preferred_sv_scientific = True
                should_set_preferred_sv_vernacular = _looks_like_vernacular(swedish_name, scientific_name)

            _upsert_external_id(
                conn,
                taxon_id=local_taxon_id,
                source_system="artportalen",
                external_id=swedish_taxon_id,
                id_role=id_role,
                is_preferred=id_role == "accepted" and should_set_preferred_sv_scientific,
                external_name=preferred_sv_name,
                note=note,
            )

            _upsert_scientific_name(
                conn,
                taxon_id=local_taxon_id,
                language_code="sv",
                scientific_name=preferred_sv_name,
                is_preferred=should_set_preferred_sv_scientific,
                source="artportalen",
                note=note,
            )

            if should_set_preferred_sv_scientific:
                preferred_sv_scientific.add(local_taxon_id)
                if id_role == "accepted" and _swedish_id_is_available(conn, local_taxon_id, swedish_taxon_id):
                    conn.execute(
                        """
                        UPDATE taxon_min
                        SET swedish_taxon_id = COALESCE(swedish_taxon_id, ?)
                        WHERE taxon_id = ?
                        """,
                        (swedish_taxon_id, local_taxon_id),
                    )

            if _looks_like_vernacular(swedish_name, scientific_name):
                _upsert_vernacular(
                    conn,
                    taxon_id=local_taxon_id,
                    language_code="sv",
                    vernacular_name=swedish_name,
                    is_preferred=should_set_preferred_sv_vernacular,
                    source="artportalen",
                )
                if should_set_preferred_sv_vernacular:
                    preferred_sv_vernacular.add(local_taxon_id)

            stats[status] += 1

    return dict(stats)


def build_unified_db(
    *,
    csv_path: Path,
    out_db: Path,
    no_taxon: Path,
    no_vernacular: Path | None,
    artportalen_matched: Path,
    artportalen_reconciled: Path,
    inat_map_path: Path | None,
) -> None:
    _set_csv_field_limit()

    if not csv_path.exists():
        raise SystemExit(f"Missing multilingual CSV: {csv_path}")
    if not no_taxon.exists():
        raise SystemExit(f"Missing Artsdatabanken taxon.txt: {no_taxon}")
    if not artportalen_matched.exists():
        raise SystemExit(f"Missing Artportalen match CSV: {artportalen_matched}")
    if not artportalen_reconciled.exists():
        raise SystemExit(f"Missing Artportalen reconciled CSV: {artportalen_reconciled}")

    norway_rows_by_id, _, norway_synonyms = _load_norway_taxonomy(no_taxon)
    inat_mapping = _load_inat_mapping(inat_map_path)

    print("Step 1/5: building fresh base multilingual DB")
    build_base_db(csv_path=csv_path, out_db=out_db, no_taxon=no_taxon, no_names=no_vernacular)

    print("Step 2/5: extending schema")
    conn = sqlite3.connect(out_db)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        conn.execute("BEGIN;")
        _create_extended_schema(conn)
        conn.commit()

        print("Step 3/5: populating Norwegian IDs and scientific aliases")
        conn.execute("BEGIN;")
        base_rows, scientific_alias_rows = _populate_base_metadata(conn, norway_rows_by_id, inat_mapping)
        norway_synonym_rows = _merge_norway_synonyms(conn, norway_synonyms)
        conn.commit()
        print(
            f"  base taxa annotated: {base_rows}, "
            f"base scientific aliases: {scientific_alias_rows}, "
            f"Norwegian synonym aliases: {norway_synonym_rows}"
        )

        print("Step 4/5: merging accepted Artportalen Swedish mappings")
        conn.execute("BEGIN;")
        (
            matched_rows,
            matched_preferred_vernaculars,
            preferred_sv_scientific,
            preferred_sv_vernacular,
        ) = _merge_artportalen_matches(conn, artportalen_matched)
        conn.commit()
        print(
            f"  accepted Swedish ID matches: {matched_rows}, "
            f"preferred Swedish vernacular overrides: {matched_preferred_vernaculars}"
        )

        print("Step 5/5: reconciling Swedish-only and synonym rows")
        conn.execute("BEGIN;")
        reconcile_stats = _merge_artportalen_reconciled(
            conn,
            artportalen_reconciled,
            norway_rows_by_id,
            inat_mapping,
            preferred_sv_scientific,
            preferred_sv_vernacular,
        )
        conn.commit()

        conn.execute("VACUUM;")

        taxa_total = conn.execute("SELECT COUNT(*) FROM taxon_min").fetchone()[0]
        vernacular_total = conn.execute("SELECT COUNT(*) FROM vernacular_min").fetchone()[0]
        scientific_total = conn.execute("SELECT COUNT(*) FROM scientific_name_min").fetchone()[0]
        swedish_only_total = conn.execute(
            "SELECT COUNT(*) FROM taxon_min WHERE norwegian_taxon_id IS NULL AND swedish_taxon_id IS NOT NULL"
        ).fetchone()[0]

        print(f"Created: {out_db}")
        print(f"taxon_min rows: {taxa_total}")
        print(f"vernacular_min rows: {vernacular_total}")
        print(f"scientific_name_min rows: {scientific_total}")
        print(f"Artportalen-only local rows: {swedish_only_total}")
        if reconcile_stats:
            print("Reconciled Swedish-only breakdown:")
            for key in sorted(reconcile_stats):
                print(f"  {key}: {reconcile_stats[key]}")
        if inat_mapping:
            filled_inat = conn.execute(
                "SELECT COUNT(*) FROM taxon_min WHERE inaturalist_taxon_id IS NOT NULL"
            ).fetchone()[0]
            print(f"Rows with iNaturalist IDs: {filled_inat}")
        else:
            print("Rows with iNaturalist IDs: 0 (no local iNaturalist mapping CSV supplied)")
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build a unified multilingual taxonomy DB with Norwegian, Swedish, "
            "and optional iNaturalist IDs plus scientific-name aliases."
        )
    )
    parser.add_argument("--csv", default=str(DEFAULT_INPUT_CSV), help="Input multilingual CSV")
    parser.add_argument("--out", default=str(DEFAULT_OUTPUT_DB), help="Output SQLite DB")
    parser.add_argument("--no-taxon", default=str(DEFAULT_NO_TAXON), help="Artsdatabanken taxon.txt")
    parser.add_argument(
        "--no-vernacular",
        default=str(DEFAULT_NO_VERNACULAR),
        help="Artsdatabanken vernacularname.txt",
    )
    parser.add_argument(
        "--artportalen-matched",
        default=str(DEFAULT_ARTPORTALEN_MATCHED),
        help="CSV with accepted Artportalen matches for existing Norwegian taxa",
    )
    parser.add_argument(
        "--artportalen-reconciled",
        default=str(DEFAULT_ARTPORTALEN_RECONCILED),
        help="Reconciled CSV for Artportalen rows not found directly in the Norwegian DB",
    )
    parser.add_argument(
        "--inat-map",
        default="",
        help=(
            "Optional CSV mapping scientific names to iNaturalist taxon IDs. "
            "Expected headers include scientificName/scientific_name and "
            "inaturalist_taxon_id/inat_taxon_id."
        ),
    )
    args = parser.parse_args()

    build_unified_db(
        csv_path=Path(args.csv).resolve(),
        out_db=Path(args.out).resolve(),
        no_taxon=Path(args.no_taxon).resolve(),
        no_vernacular=Path(args.no_vernacular).resolve() if args.no_vernacular else None,
        artportalen_matched=Path(args.artportalen_matched).resolve(),
        artportalen_reconciled=Path(args.artportalen_reconciled).resolve(),
        inat_map_path=Path(args.inat_map).resolve() if args.inat_map else None,
    )


if __name__ == "__main__":
    main()
