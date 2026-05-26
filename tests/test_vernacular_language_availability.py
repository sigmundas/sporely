from __future__ import annotations

import sqlite3
from pathlib import Path

import utils.vernacular_utils as vernacular_utils


def _seed_multilang_vernacular_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE vernacular_min (
                vernacular_id INTEGER PRIMARY KEY AUTOINCREMENT,
                taxon_id INTEGER,
                language_code TEXT,
                vernacular_name TEXT,
                is_preferred_name INTEGER,
                source TEXT
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO vernacular_min (taxon_id, language_code, vernacular_name, is_preferred_name, source)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (1, "en", "Button mushroom", 1, "test"),
                (1, "no", "Sjampinjong", 1, "test"),
                (1, "sv", "Champinjon", 1, "test"),
                (1, "en", "Cultivated mushroom", 0, "test"),
            ],
        )


def test_list_available_vernacular_languages_reads_distinct_language_codes_from_multilang_db(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "vernacular-multilang.sqlite3"
    _seed_multilang_vernacular_db(db_path)

    monkeypatch.setattr(vernacular_utils, "resolve_multilang_db_path", lambda: db_path)

    assert vernacular_utils.list_available_vernacular_languages() == ["en", "sv", "no"]


def test_resolve_available_vernacular_language_prefers_available_language_over_stale_setting(
    monkeypatch,
) -> None:
    monkeypatch.setattr(vernacular_utils, "list_available_vernacular_languages", lambda: ["sv", "en"])
    assert vernacular_utils.resolve_available_vernacular_language("de") == "sv"
    assert vernacular_utils.resolve_available_vernacular_language("en") == "en"

    monkeypatch.setattr(vernacular_utils, "list_available_vernacular_languages", lambda: ["no", "sv", "en"])
    assert vernacular_utils.resolve_available_vernacular_language("de") == "no"
    assert vernacular_utils.resolve_available_vernacular_language(None) == "no"
