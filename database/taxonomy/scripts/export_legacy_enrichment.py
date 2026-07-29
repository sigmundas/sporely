#!/usr/bin/env python3
"""Extract legacy compatibility enrichment from the currently bundled desktop
taxonomy SQLite into a compiler-consumable JSONL.

The bundled DB (``database/reference_data/generated/vernacular_multilanguage.sqlite3``)
keys everything by the pre-Stage-3A NorTaxa DwC id. This script exports only
the rows that Stage 3A does NOT already carry:

* Artportalen external identifiers (``source_system == 'artportalen'``);
* vernacular names in languages that the two Stage 3A authoritative sources
  do not publish (``fr``, ``fi``, ``da``, ``de``, ``pl``, ``es``, ``en``, ``pt``,
  ``it``, and the umbrella ``no`` code).

Every row is emitted verbatim with its source provider preserved so the
compiler can resolve NorTaxa → Sporely and route the enrichment to the
matching Sporely ID. Provenance-unknown rows are labelled ``legacy_sporely``
per the identity-contract instruction not to invent a provider.

No Sporely ID is allocated by this script; that stays the compiler's job.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Iterable


# Languages Stage 3A's COL + NorTaxa 1.284 pipeline does not publish.
LANGUAGES_NOT_IN_STAGE_3A = frozenset({
    "fr", "fi", "da", "de", "pl", "es", "en", "pt", "it", "no",
})
# Non-NorTaxa external-identifier sources that require legacy import.
EXTERNAL_SOURCES_TO_IMPORT = frozenset({"artportalen"})


def _classify_provider(raw: str | None, fallback: str) -> str:
    """Return an explicit provider name, or ``legacy_sporely`` when unknown."""
    if not raw:
        return "legacy_sporely"
    return str(raw).strip() or "legacy_sporely"


def _iter_vernacular_rows(conn: sqlite3.Connection) -> Iterable[dict]:
    cursor = conn.execute(
        "SELECT taxon_id, language_code, vernacular_name, is_preferred_name, "
        "COALESCE(source, '') AS source "
        "FROM vernacular_min "
        "WHERE language_code IN (" +
        ",".join("?" for _ in LANGUAGES_NOT_IN_STAGE_3A) + ") "
        "ORDER BY taxon_id, language_code, vernacular_name",
        tuple(sorted(LANGUAGES_NOT_IN_STAGE_3A)),
    )
    for row in cursor:
        taxon_id, language, name, preferred, source = row
        yield {
            "kind": "vernacular",
            "nortaxa_taxon_id": str(taxon_id),
            "language": str(language),
            "vernacular_name": str(name),
            "is_preferred": bool(preferred),
            "provider": _classify_provider(source, "legacy_sporely"),
            "provenance": _describe_provenance(source),
        }


def _iter_external_id_rows(conn: sqlite3.Connection) -> Iterable[dict]:
    cursor = conn.execute(
        "SELECT taxon_id, source_system, external_id, id_role, "
        "COALESCE(external_name, '') AS external_name, "
        "COALESCE(is_preferred, 0) AS is_preferred, "
        "COALESCE(note, '') AS note "
        "FROM taxon_external_id_min "
        "WHERE source_system IN (" +
        ",".join("?" for _ in EXTERNAL_SOURCES_TO_IMPORT) + ") "
        "ORDER BY taxon_id, source_system, external_id",
        tuple(sorted(EXTERNAL_SOURCES_TO_IMPORT)),
    )
    for row in cursor:
        (taxon_id, source_system, external_id, id_role,
         external_name, is_preferred, note) = row
        yield {
            "kind": "external_identifier",
            "nortaxa_taxon_id": str(taxon_id),
            "source_system": str(source_system),
            "external_id": str(external_id),
            "external_id_kind": "integer",  # bundled column is INTEGER
            "id_role": str(id_role),
            "is_preferred": bool(is_preferred),
            "external_name": str(external_name) or None,
            "note": str(note) or None,
            "provider": _classify_provider(source_system, "legacy_sporely"),
            "provenance": _describe_provenance(source_system),
        }


def _describe_provenance(source: str | None) -> str:
    return {
        "artsdatabanken": "NorTaxa (Artsdatabanken) — already carried by Stage 3A",
        "artportalen": "Artportalen.se reconciled Swedish-species portal (SLU/ArtDatabanken)",
        "inat_csv": "iNaturalist multilingual vernaculars (public API export)",
        "": "legacy_sporely (provider unknown)",
        None: "legacy_sporely (provider unknown)",
    }.get(source, str(source))


def export(*, bundled_db: Path, output_path: Path) -> dict:
    if output_path.exists():
        raise SystemExit(f"output already exists: {output_path}")
    conn = sqlite3.connect(f"file:{bundled_db}?mode=ro", uri=True)
    counts = {"vernacular": 0, "external_identifier": 0}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for row in _iter_vernacular_rows(conn):
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
            counts["vernacular"] += 1
        for row in _iter_external_id_rows(conn):
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
            counts["external_identifier"] += 1
    return {"counts": counts, "output_path": str(output_path)}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundled-db", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    result = export(bundled_db=args.bundled_db, output_path=args.output)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
