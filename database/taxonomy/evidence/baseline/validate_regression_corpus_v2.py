#!/usr/bin/env python3
"""Validate the v2 regression corpus against a taxonomy-v2 candidate SQLite.

Executes every query via `VernacularDB` / `taxon_external_id_min` /
`taxon_external_id_text_min` — the same lookup surfaces the desktop
runtime uses. Assertion is on Sporely IDs, not on the frozen legacy
integers.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path


BASELINE_DIR = Path(__file__).resolve().parent
CORPUS_PATH = BASELINE_DIR / "regression-corpus-v2.json"


def ids(conn: sqlite3.Connection, sql: str, params: tuple) -> set[int]:
    return {int(r[0]) for r in conn.execute(sql, params)}


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    if not argv:
        print("usage: validate_regression_corpus_v2.py <path/to/candidate.sqlite3>",
              file=sys.stderr)
        return 2
    db_path = Path(argv[0])
    corpus = json.loads(CORPUS_PATH.read_text(encoding="utf-8"))
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)

    failures: list[dict] = []

    for case in corpus["groups"]["accepted_scientific"]:
        expected = case["expected_sporely_taxon_id"]
        actual = ids(
            conn,
            "SELECT taxon_id FROM taxon_min "
            "WHERE lower(canonical_scientific_name) = lower(?)",
            (case["query"],))
        if expected is not None and expected not in actual:
            failures.append({"group": "accepted_scientific", **case,
                             "actual": sorted(actual)})

    for case in corpus["groups"]["scientific_synonym"]:
        expected = case["expected_sporely_taxon_id"]
        actual = ids(
            conn,
            "SELECT taxon_id FROM scientific_name_min "
            "WHERE lower(scientific_name) = lower(?)",
            (case["query"],))
        if expected is not None and expected not in actual:
            failures.append({"group": "scientific_synonym", **case,
                             "actual": sorted(actual)})

    for case in corpus["groups"]["vernacular"]:
        expected = case["expected_sporely_taxon_id"]
        language = case["language"]
        if language == "no":
            actual = ids(
                conn,
                "SELECT taxon_id FROM vernacular_min "
                "WHERE lower(vernacular_name) = lower(?) "
                "AND language_code IN ('no','nb','nn')",
                (case["query"],))
        else:
            actual = ids(
                conn,
                "SELECT taxon_id FROM vernacular_min "
                "WHERE lower(vernacular_name) = lower(?) "
                "AND language_code = ?",
                (case["query"], language))
        if expected is not None and expected not in actual:
            failures.append({"group": "vernacular", **case,
                             "actual": sorted(actual)})

    for case in corpus["groups"]["external_identifier"]:
        expected = case["expected_sporely_taxon_id"]
        source = case["source_system"]
        ident = case["external_id"]
        try:
            n = int(ident)
        except ValueError:
            n = None
        actual: set[int] = set()
        if n is not None:
            actual |= ids(
                conn,
                "SELECT taxon_id FROM taxon_external_id_min "
                "WHERE source_system = ? AND external_id = ?",
                (source, n))
        actual |= ids(
            conn,
            "SELECT taxon_id FROM taxon_external_id_text_min "
            "WHERE source_system = ? AND external_id = ?",
            (source, ident))
        if expected is not None and expected not in actual:
            failures.append({"group": "external_identifier", **case,
                             "actual": sorted(actual)})

    for case in corpus["groups"]["missing"]:
        query = case["query"]
        ns = case["namespace_or_language"]
        if ns == "nbic_scientific_name_id":
            actual = ids(
                conn,
                "SELECT taxon_id FROM taxon_external_id_text_min "
                "WHERE namespace = ? AND external_id = ?", (ns, query))
        else:
            actual = ids(
                conn,
                "SELECT taxon_id FROM taxon_min WHERE lower(canonical_scientific_name)=lower(?)"
                " UNION SELECT taxon_id FROM scientific_name_min WHERE lower(scientific_name)=lower(?)"
                " UNION SELECT taxon_id FROM vernacular_min WHERE lower(vernacular_name)=lower(?)",
                (query, query, query))
        if actual:
            failures.append({"group": "missing", **case,
                             "actual": sorted(actual)})

    if failures:
        print(f"FAIL: {len(failures)} corpus queries did not resolve as expected")
        for row in failures[:5]:
            print(f"  {row}")
        return 1
    print(f"OK: validated {corpus['case_count']} regression queries "
          f"against {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
