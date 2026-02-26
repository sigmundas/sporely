"""Helpers for vernacular language handling and DB discovery."""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

# Canonical labels for language codes used in vernacular DBs.
VERNACULAR_LANGUAGE_LABELS = {
    "en": "English",
    "de": "German",
    "fr": "French",
    "es": "Spanish",
    "da": "Danish",
    "sv": "Swedish",
    "no": "Norwegian",
    "fi": "Finnish",
    "pl": "Polish",
    "pt": "Portuguese",
    "it": "Italian",
}

COMMON_NAME_LABEL_OVERRIDES = {
    "de": "Namen",
    "no": "Norsk navn",
    "en": "Common name",
}

# Preferred ordering for UI language pickers.
VERNACULAR_LANGUAGE_ORDER = ["en", "de", "fr", "es", "da", "sv", "no", "fi", "pl", "pt", "it"]

# Candidate filenames for a multi-language DB.
MULTILANG_DB_NAMES = [
    "vernacular_multilanguage.sqlite3",
    "vernacular_multi.sqlite3",
    "taxonomy_multilang.sqlite3",
    "taxonomy_multi.sqlite3",
    "vernacular_all_languages.sqlite3",
]


def normalize_vernacular_language(code: str | None) -> str:
    """Normalize language codes to the short, canonical form used by the DB."""
    if not code:
        return "no"
    text = code.strip().lower().replace("-", "_")
    # Collapse Norwegian variants to a single code.
    if text in ("nb", "nb_no", "nn", "nn_no", "no"):
        return "no"
    if "_" in text:
        text = text.split("_", 1)[0]
    return text or "no"


def vernacular_language_label(code: str | None) -> str:
    """Return a human-friendly label for a vernacular language code."""
    lang = normalize_vernacular_language(code)
    return VERNACULAR_LANGUAGE_LABELS.get(lang, lang.upper() if lang else "")


def common_name_display_label(lang_code: str | None, default_label: str) -> str:
    """Return the preferred label for the common name field/header."""
    lang = normalize_vernacular_language(lang_code)
    override = COMMON_NAME_LABEL_OVERRIDES.get(lang)
    if override:
        return override
    label = vernacular_language_label(lang)
    if label:
        return f"{default_label} ({label})"
    return default_label


def _order_vernacular_languages(languages: Iterable[str]) -> list[str]:
    ordered = [code for code in VERNACULAR_LANGUAGE_ORDER if code in languages]
    for code in sorted(set(languages)):
        if code not in ordered:
            ordered.append(code)
    return ordered


def _candidate_roots() -> tuple[Path, ...]:
    cwd = Path.cwd()
    app_root = Path(__file__).resolve().parent.parent
    return (cwd / "database", cwd, app_root / "database", app_root)


def resolve_multilang_db_path() -> Path | None:
    """Locate a multi-language vernacular DB, if present."""
    for base in _candidate_roots():
        for name in MULTILANG_DB_NAMES:
            path = base / name
            if path.exists():
                return path
    return None


def resolve_vernacular_db_path(lang_code: str | None = None) -> Path | None:
    """Locate the vernacular DB, preferring a multi-language DB."""
    multi = resolve_multilang_db_path()
    if multi:
        return multi
    lang = normalize_vernacular_language(lang_code)
    lang_tag = lang.upper()
    for base in _candidate_roots():
        path = base / f"taxonomy_{lang_tag}.sqlite3"
        if path.exists():
            return path
    return None


def _has_language_column(conn: sqlite3.Connection) -> bool:
    cur = conn.execute("PRAGMA table_info(vernacular_min)")
    return any(row[1] == "language_code" for row in cur.fetchall())


def list_available_vernacular_languages() -> list[str]:
    """Return available vernacular languages from the DB or file discovery."""
    multi = resolve_multilang_db_path()
    if multi:
        try:
            conn = sqlite3.connect(multi)
            try:
                if not _has_language_column(conn):
                    return []
                rows = conn.execute(
                    """
                    SELECT DISTINCT language_code
                    FROM vernacular_min
                    WHERE language_code IS NOT NULL AND language_code != ''
                    ORDER BY language_code
                    """
                ).fetchall()
                return _order_vernacular_languages([row[0] for row in rows if row and row[0]])
            finally:
                conn.close()
        except Exception:
            return VERNACULAR_LANGUAGE_ORDER[:]

    found = set()
    for base in _candidate_roots():
        for path in base.glob("taxonomy_*.sqlite3"):
            parts = path.stem.split("_", 1)
            if len(parts) == 2:
                found.add(normalize_vernacular_language(parts[1]))
    if not found:
        found.add("no")
    return _order_vernacular_languages(found)
