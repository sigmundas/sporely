"""Helpers for vernacular language handling and DB discovery."""
from __future__ import annotations

from functools import lru_cache
import sqlite3
from pathlib import Path
from typing import Iterable

from database.reference_data_paths import REFERENCE_DATA_GENERATED_DIR

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
    "vernacular_multilanguage_unified.sqlite3",
    "vernacular_multilanguage_unified_test.sqlite3",
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
    return (cwd / "database", cwd, app_root / "database", app_root, REFERENCE_DATA_GENERATED_DIR)


def _vernacular_discovery_signature() -> tuple[int, str]:
    return id(resolve_multilang_db_path), str(Path.cwd())


@lru_cache(maxsize=8)
def _cached_resolve_multilang_db_path(_resolver_id: int, _cwd: str) -> Path | None:
    """Resolve the bundled multilingual DB path with a cache key that tracks monkeypatches."""
    for base in _candidate_roots():
        for name in MULTILANG_DB_NAMES:
            path = base / name
            if path.exists():
                return path
    return None


def resolve_multilang_db_path() -> Path | None:
    """Locate a multi-language vernacular DB, if present."""
    resolver_id, cwd = _vernacular_discovery_signature()
    return _cached_resolve_multilang_db_path(resolver_id, cwd)


@lru_cache(maxsize=16)
def _cached_list_available_vernacular_languages(_resolver_id: int, _cwd: str) -> tuple[str, ...]:
    """Discover available vernacular languages with cache invalidation on resolver changes."""
    multi = resolve_multilang_db_path()
    if multi:
        try:
            from database.vernacular_db import VernacularDB

            languages = VernacularDB(multi).list_languages()
            if languages:
                return tuple(_order_vernacular_languages(languages))
        except Exception:
            pass

    found = set()
    for base in _candidate_roots():
        for path in base.glob("taxonomy_*.sqlite3"):
            parts = path.stem.split("_", 1)
            if len(parts) == 2:
                found.add(normalize_vernacular_language(parts[1]))
    if not found:
        found.add("no")
    return tuple(_order_vernacular_languages(found))


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
    resolver_id, cwd = _vernacular_discovery_signature()
    return list(_cached_list_available_vernacular_languages(resolver_id, cwd))


def resolve_available_vernacular_language(preferred: str | None = None) -> str | None:
    """Return the preferred language if available, otherwise a runtime fallback."""
    available = list_available_vernacular_languages()
    if not available:
        return None
    preferred_lang = normalize_vernacular_language(preferred) if preferred else None
    if preferred_lang and preferred_lang in available:
        return preferred_lang
    if "no" in available:
        return "no"
    return available[0]
