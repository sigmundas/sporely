#!/usr/bin/env python3
"""Backfill ``observations.sporely_taxon_id`` idempotently.

Precedence per Stage 3B.2 contract:

1. an already-populated ``sporely_taxon_id`` that still verifies against
   the taxonomy-v2 candidate;
2. an ``ai_selected_taxon_id`` snapshot that carries an explicit NBIC-
   namespaced integer (``NBIC:54995`` or bare integer stored elsewhere)
   resolvable via ``taxon_external_id_min(source_system='artsdatabanken')``;
3. an existing ``artsdata_id`` snapshot (NorTaxa DwC id) resolved through
   the same namespaced lookup;
4. a scientific-name lookup that resolves to exactly one Sporely id
   (canonical or synonym); ambiguous names leave the column NULL;
5. no fill.

The backfill NEVER rewrites existing scientific-name / common-name / AI
snapshot columns. It touches only ``sporely_taxon_id``.

Runs in one transaction. Reports per-source resolved counts. Refuses to run
against a taxonomy DB whose ``taxonomy_meta`` says schema != 2.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


NBIC_PATTERN = re.compile(r"(?:NBIC[:_-])?(\d+)")


logger = logging.getLogger(__name__)


@dataclass
class BackfillStats:
    total_observations: int = 0
    already_populated_kept: int = 0
    already_populated_rejected: int = 0
    resolved_by_explicit_nbic: int = 0
    resolved_by_artsdata_id: int = 0
    resolved_by_unique_scientific_name: int = 0
    ambiguous_scientific_name_left_null: int = 0
    unresolved_left_null: int = 0
    rows_touched: int = 0

    def as_dict(self) -> dict:
        return self.__dict__.copy()


def _resolve_nbic_integer(text: str | None) -> int | None:
    if not text:
        return None
    match = NBIC_PATTERN.fullmatch(str(text).strip())
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


def _fetch_valid_sporely_ids(conn: sqlite3.Connection) -> set[int]:
    return {int(r[0]) for r in conn.execute("SELECT taxon_id FROM taxon_min")}


def _resolve_via_nortaxa(conn: sqlite3.Connection, value: int) -> int | None:
    row = conn.execute(
        "SELECT taxon_id FROM taxon_external_id_min "
        "WHERE source_system='artsdatabanken' AND external_id=? LIMIT 1",
        (int(value),),
    ).fetchone()
    return int(row[0]) if row else None


def _resolve_via_scientific_name(
    conn: sqlite3.Connection, name: str,
) -> tuple[set[int], str]:
    if not name or not name.strip():
        return set(), "no_name"
    cleaned = name.strip()
    accepted = {int(r[0]) for r in conn.execute(
        "SELECT taxon_id FROM taxon_min "
        "WHERE canonical_scientific_name = ? COLLATE NOCASE", (cleaned,))}
    if len(accepted) == 1:
        return accepted, "canonical_unique"
    aliases = {int(r[0]) for r in conn.execute(
        "SELECT taxon_id FROM scientific_name_min "
        "WHERE scientific_name = ? COLLATE NOCASE", (cleaned,))}
    union = accepted | aliases
    if len(union) == 1:
        return union, "alias_unique"
    if len(union) > 1:
        return union, "ambiguous"
    return set(), "not_found"


def backfill(
    *,
    observation_db_path: Path,
    taxonomy_db_path: Path,
    dry_run: bool = False,
) -> BackfillStats:
    stats = BackfillStats()
    if not observation_db_path.exists():
        raise SystemExit(f"observation database not found: {observation_db_path}")
    if not taxonomy_db_path.exists():
        raise SystemExit(f"taxonomy candidate not found: {taxonomy_db_path}")

    tax_conn = sqlite3.connect(f"file:{taxonomy_db_path}?mode=ro", uri=True)
    try:
        meta = dict(tax_conn.execute("SELECT key, value FROM taxonomy_meta"))
    except sqlite3.DatabaseError as exc:
        raise SystemExit(f"taxonomy DB missing taxonomy_meta: {exc}") from exc
    if meta.get("taxonomy_schema_version") != "2":
        raise SystemExit(
            f"taxonomy DB schema mismatch: {meta.get('taxonomy_schema_version')!r}"
        )
    valid_ids = _fetch_valid_sporely_ids(tax_conn)

    obs = sqlite3.connect(observation_db_path)
    obs.row_factory = sqlite3.Row
    try:
        cursor = obs.execute(
            "SELECT id, sporely_taxon_id, artsdata_id, "
            "       ai_selected_taxon_id, ai_selected_scientific_name, "
            "       genus, species "
            "FROM observations"
        )
        rows = cursor.fetchall()
    except sqlite3.OperationalError as exc:
        raise SystemExit(
            f"observation table lacks required column (run schema.init_database first): {exc}"
        ) from exc

    updates: list[tuple[int | None, int]] = []
    for row in rows:
        stats.total_observations += 1
        obs_id = int(row["id"])
        current = row["sporely_taxon_id"]
        if current is not None:
            if int(current) in valid_ids:
                stats.already_populated_kept += 1
                continue
            stats.already_populated_rejected += 1
            updates.append((None, obs_id))  # invalid → clear
            continue

        # Step 2: NBIC-style ai_selected_taxon_id
        nbic_value = _resolve_nbic_integer(row["ai_selected_taxon_id"])
        if nbic_value is not None:
            sporely = _resolve_via_nortaxa(tax_conn, nbic_value)
            if sporely is not None:
                stats.resolved_by_explicit_nbic += 1
                updates.append((sporely, obs_id))
                continue

        # Step 3: artsdata_id (NorTaxa DwC id)
        artsdata = row["artsdata_id"]
        if artsdata is not None:
            try:
                sporely = _resolve_via_nortaxa(tax_conn, int(artsdata))
            except (TypeError, ValueError):
                sporely = None
            if sporely is not None:
                stats.resolved_by_artsdata_id += 1
                updates.append((sporely, obs_id))
                continue

        # Step 4: unique scientific-name / synonym alias
        name_candidates = [row["ai_selected_scientific_name"]]
        if row["genus"] and row["species"]:
            name_candidates.append(f"{row['genus']} {row['species']}")
        matched: set[int] | None = None
        for name in name_candidates:
            if not name or not str(name).strip():
                continue
            candidates, _kind = _resolve_via_scientific_name(tax_conn, str(name))
            if candidates:
                matched = candidates
                break
        if matched is not None:
            if len(matched) == 1:
                stats.resolved_by_unique_scientific_name += 1
                updates.append((next(iter(matched)), obs_id))
                continue
            stats.ambiguous_scientific_name_left_null += 1
            continue

        stats.unresolved_left_null += 1

    if not dry_run:
        obs.execute("BEGIN")
        try:
            for value, obs_id in updates:
                obs.execute(
                    "UPDATE observations SET sporely_taxon_id = ? WHERE id = ?",
                    (value, obs_id),
                )
            obs.execute("COMMIT")
        except Exception:
            obs.execute("ROLLBACK")
            raise
    stats.rows_touched = sum(1 for v, _ in updates if v is not None)
    tax_conn.close()
    obs.close()
    return stats


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observations", type=Path, required=True)
    parser.add_argument("--taxonomy", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    stats = backfill(
        observation_db_path=args.observations,
        taxonomy_db_path=args.taxonomy,
        dry_run=args.dry_run,
    )
    print(json.dumps(stats.as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
