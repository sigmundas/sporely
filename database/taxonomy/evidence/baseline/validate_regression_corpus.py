#!/usr/bin/env python3
"""Validate the frozen Stage 0 regression corpus against the bundled SQLite."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path


BASELINE_DIR = Path(__file__).resolve().parent
CORPUS_PATH = BASELINE_DIR / "regression-corpus.json"
DB_PATH = (
    BASELINE_DIR.parents[2]
    / "reference_data"
    / "generated"
    / "vernacular_multilanguage.sqlite3"
)


def ids(conn: sqlite3.Connection, sql: str, params: tuple[object, ...]) -> set[int]:
    return {int(row[0]) for row in conn.execute(sql, params)}


def main() -> None:
    corpus = json.loads(CORPUS_PATH.read_text(encoding="utf-8"))
    groups = corpus["groups"]
    case_count = sum(len(cases) for cases in groups.values())
    assert corpus["case_count"] == case_count == 100
    assert {"se", "sma", "smj"} <= {case[1] for case in groups["missing"]}
    assert any(any(ord(char) > 127 for char in case[0]) for case in groups["vernacular"])

    digest = hashlib.sha256(DB_PATH.read_bytes()).hexdigest()
    assert digest == corpus["baseline_sqlite_sha256"]

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    for query, expected in groups["accepted_scientific"]:
        actual = ids(
            conn,
            "SELECT taxon_id FROM taxon_min WHERE lower(canonical_scientific_name)=lower(?)",
            (query,),
        )
        assert expected in actual, (query, expected, actual)

    for query, expected in groups["scientific_synonym"]:
        actual = ids(
            conn,
            "SELECT taxon_id FROM scientific_name_min "
            "WHERE lower(scientific_name)=lower(?) AND is_preferred_name=0",
            (query,),
        )
        assert expected in actual, (query, expected, actual)

    for query, language, expected in groups["vernacular"]:
        actual = ids(
            conn,
            "SELECT taxon_id FROM vernacular_min "
            "WHERE lower(vernacular_name)=lower(?) AND language_code=?",
            (query, language),
        )
        assert actual == {expected}, (query, expected, actual)

    for source, _namespace, identifier, expected in groups["external_identifier"]:
        actual = ids(
            conn,
            "SELECT taxon_id FROM taxon_external_id_min "
            "WHERE source_system=? AND cast(external_id AS text)=?",
            (source, identifier),
        )
        assert expected in actual, (source, identifier, expected, actual)

    for query, namespace_or_language, _reason in groups["missing"]:
        if namespace_or_language == "nbic_scientific_name_id":
            actual = ids(
                conn,
                "SELECT taxon_id FROM taxon_external_id_min WHERE cast(external_id AS text)=?",
                (query,),
            )
        else:
            actual = ids(
                conn,
                "SELECT taxon_id FROM taxon_min WHERE lower(canonical_scientific_name)=lower(?) "
                "UNION SELECT taxon_id FROM scientific_name_min WHERE lower(scientific_name)=lower(?) "
                "UNION SELECT taxon_id FROM vernacular_min WHERE lower(vernacular_name)=lower(?)",
                (query, query, query),
            )
        assert not actual, (query, actual)

    print(f"validated {case_count} regression queries against {DB_PATH}")


if __name__ == "__main__":
    main()
