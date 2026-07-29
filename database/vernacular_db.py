"""SQLite helper for vernacular name lookup."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from utils.vernacular_utils import (
    normalize_vernacular_language,
    resolve_query_language_codes,
)


class VernacularDB:
    """Simple helper for vernacular name lookup."""

    def __init__(self, db_path: Path, language_code: str | None = None):
        self.db_path = db_path
        # Preserve `nb`/`nn`/Sámi codes verbatim — the taxonomy v2 identity
        # contract keeps them distinct. `resolve_query_language_codes` in
        # `_language_clause` handles the umbrella `no` → `('no','nb','nn')`
        # fan-out.
        self.language_code = language_code.strip() if language_code else None
        self._has_language_column = None
        self._tables: set[str] | None = None

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _table_names(self) -> set[str]:
        if self._tables is None:
            with self._connect() as conn:
                cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                self._tables = {str(row[0] or "") for row in cur.fetchall()}
        return self._tables

    def _has_scientific_name_table(self) -> bool:
        return "scientific_name_min" in self._table_names()

    def _is_v2(self) -> bool:
        """A v2 candidate carries a ``taxonomy_meta`` table. Legacy multi-
        language DBs do not. When we detect legacy, the language clause
        keeps the umbrella `nb`/`nn`→`no` behavior for backwards compat."""
        return "taxonomy_meta" in self._table_names()

    def _has_language(self) -> bool:
        if self._has_language_column is None:
            with self._connect() as conn:
                cur = conn.execute("PRAGMA table_info(vernacular_min)")
                self._has_language_column = any(row[1] == "language_code" for row in cur.fetchall())
        return bool(self._has_language_column)

    def _language_clause(self, language_code: str | None) -> tuple[str, list[str]]:
        """Language filter.

        For **v2** taxonomy candidates (``taxonomy_meta`` present) the codes
        ``nb``, ``nn`` and Sámi variants are queried distinctly, and the
        umbrella ``no`` fans out to ``('no','nb','nn')`` per the identity
        contract. For **legacy** DBs the pre-existing umbrella behavior
        (``normalize_vernacular_language`` collapse) is preserved to keep
        old production callers unbroken.
        """
        if not self._has_language():
            return "", []
        raw = language_code or self.language_code
        if not raw:
            return "", []
        if self._is_v2():
            codes = resolve_query_language_codes(raw)
            if not codes:
                return "", []
            placeholders = ",".join("?" for _ in codes)
            return f" AND v.language_code IN ({placeholders}) ", list(codes)
        lang = normalize_vernacular_language(raw)
        if not lang:
            return "", []
        return " AND v.language_code = ? ", [lang]

    def list_languages(self) -> list[str]:
        if not self._has_language():
            return []
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT language_code
                FROM vernacular_min
                WHERE language_code IS NOT NULL AND language_code != ''
                ORDER BY language_code
                """
            )
            return [row[0] for row in cur.fetchall() if row and row[0]]

    def suggest_vernacular(self, prefix: str, genus: str | None = None, species: str | None = None) -> list[str]:
        prefix = prefix.strip()
        if not prefix:
            return []
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT v.vernacular_name
                FROM vernacular_min v
                JOIN taxon_min t ON t.taxon_id = v.taxon_id
                WHERE v.vernacular_name LIKE ? || '%'
                  AND (? IS NULL OR t.genus = ?)
                  AND (? IS NULL OR t.specific_epithet = ?)
                """
                + lang_clause
                + """
                ORDER BY v.vernacular_name
                LIMIT 200
                """,
                (prefix, genus, genus, species, species, *lang_params),
            )
            return [row[0] for row in cur.fetchall() if row and row[0]]

    def suggest_vernacular_entries(
        self,
        prefix: str,
        genus: str | None = None,
        species: str | None = None,
        limit: int = 200,
    ) -> list[dict]:
        prefix = prefix.strip()
        if not prefix:
            return []
        resolved = self.taxon_from_scientific(genus or "", species or "") if genus and species else None
        if resolved:
            genus, species, _family = resolved
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT v.vernacular_name, t.genus, t.specific_epithet, t.family, v.is_preferred_name
                FROM vernacular_min v
                JOIN taxon_min t ON t.taxon_id = v.taxon_id
                WHERE v.vernacular_name LIKE ? || '%'
                  AND (? IS NULL OR t.genus = ? COLLATE NOCASE)
                  AND (? IS NULL OR t.specific_epithet = ? COLLATE NOCASE)
                """
                + lang_clause
                + """
                ORDER BY v.is_preferred_name DESC, v.vernacular_name, t.genus, t.specific_epithet
                LIMIT ?
                """,
                (prefix, genus, genus, species, species, *lang_params, int(limit)),
            )
            return [
                {
                    "vernacular_name": row[0],
                    "genus": row[1],
                    "species": row[2],
                    "family": row[3],
                    "is_preferred_name": bool(row[4]),
                }
                for row in cur.fetchall()
                if row and row[0] and row[1] and row[2]
            ]

    def suggest_vernacular_for_taxon(
        self, genus: str | None = None, species: str | None = None, limit: int = 200
    ) -> list[str]:
        genus = genus.strip() if genus else None
        species = species.strip() if species else None
        if not genus and not species:
            return []
        resolved = self.taxon_from_scientific(genus or "", species or "") if genus and species else None
        if resolved:
            genus, species, _family = resolved
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT v.vernacular_name
                FROM vernacular_min v
                JOIN taxon_min t ON t.taxon_id = v.taxon_id
                WHERE (? IS NULL OR t.genus = ?)
                  AND (? IS NULL OR t.specific_epithet = ?)
                """
                + lang_clause
                + """
                ORDER BY v.is_preferred_name DESC, v.vernacular_name
                LIMIT ?
                """,
                (genus, genus, species, species, *lang_params, limit),
            )
            return [row[0] for row in cur.fetchall() if row and row[0]]

    def suggest_genus(self, prefix: str) -> list[str]:
        prefix = prefix.strip()
        if not prefix:
            return []
        with self._connect() as conn:
            cur = conn.cursor()
            values: list[str] = []
            seen: set[str] = set()
            cur.execute(
                """
                SELECT DISTINCT genus
                FROM taxon_min
                WHERE genus LIKE ? || '%'
                ORDER BY genus
                LIMIT 200
                """,
                (prefix,),
            )
            for row in cur.fetchall():
                genus = str(row[0] or "").strip()
                lowered = genus.casefold()
                if genus and lowered not in seen:
                    seen.add(lowered)
                    values.append(genus)
            if self._has_scientific_name_table():
                cur.execute(
                    """
                    SELECT DISTINCT scientific_name
                    FROM scientific_name_min
                    WHERE scientific_name LIKE ? || ' %'
                    ORDER BY scientific_name
                    LIMIT 400
                    """,
                    (prefix,),
                )
                for row in cur.fetchall():
                    scientific_name = str(row[0] or "").strip()
                    genus = scientific_name.split(" ", 1)[0].strip() if scientific_name else ""
                    lowered = genus.casefold()
                    if genus and lowered not in seen:
                        seen.add(lowered)
                        values.append(genus)
            return values[:200]

    def suggest_species(self, genus: str, prefix: str) -> list[str]:
        genus = genus.strip()
        prefix = prefix.strip()
        if not genus:
            return []
        with self._connect() as conn:
            cur = conn.cursor()
            values: list[str] = []
            seen: set[str] = set()
            cur.execute(
                """
                SELECT DISTINCT specific_epithet
                FROM taxon_min
                WHERE genus = ? COLLATE NOCASE
                  AND specific_epithet LIKE ? || '%'
                ORDER BY specific_epithet
                LIMIT 200
                """,
                (genus, prefix),
            )
            for row in cur.fetchall():
                species = str(row[0] or "").strip()
                lowered = species.casefold()
                if species and lowered not in seen:
                    seen.add(lowered)
                    values.append(species)
            if self._has_scientific_name_table():
                cur.execute(
                    """
                    SELECT DISTINCT scientific_name
                    FROM scientific_name_min
                    WHERE scientific_name LIKE ? || ' ' || ? || '%'
                    ORDER BY scientific_name
                    LIMIT 400
                    """,
                    (genus, prefix),
                )
                for row in cur.fetchall():
                    scientific_name = str(row[0] or "").strip()
                    parts = scientific_name.split()
                    if len(parts) < 2 or parts[0].casefold() != genus.casefold():
                        continue
                    species = parts[1].strip()
                    lowered = species.casefold()
                    if species and lowered not in seen:
                        seen.add(lowered)
                        values.append(species)
            return values[:200]

    def taxon_from_scientific(self, genus: str, species: str) -> tuple[str, str, str | None] | None:
        genus = (genus or "").strip()
        species = (species or "").strip()
        if not genus or not species:
            return None
        scientific_name = f"{genus} {species}".strip()
        with self._connect() as conn:
            cur = conn.cursor()
            if self._has_scientific_name_table():
                cur.execute(
                    """
                    SELECT t.genus, t.specific_epithet, t.family
                    FROM taxon_min t
                    LEFT JOIN scientific_name_min s ON s.taxon_id = t.taxon_id
                    WHERE (
                            t.genus = ? COLLATE NOCASE
                        AND t.specific_epithet = ? COLLATE NOCASE
                    )
                       OR (
                            t.canonical_scientific_name = ? COLLATE NOCASE
                    )
                       OR (
                            s.scientific_name = ? COLLATE NOCASE
                    )
                    ORDER BY
                        CASE
                            WHEN t.genus = ? COLLATE NOCASE AND t.specific_epithet = ? COLLATE NOCASE THEN 0
                            WHEN s.is_preferred_name = 1 THEN 1
                            ELSE 2
                        END,
                        t.genus,
                        t.specific_epithet
                    LIMIT 1
                    """,
                    (genus, species, scientific_name, scientific_name, genus, species),
                )
            else:
                cur.execute(
                    """
                    SELECT genus, specific_epithet, family
                    FROM taxon_min
                    WHERE genus = ? COLLATE NOCASE
                      AND specific_epithet = ? COLLATE NOCASE
                    ORDER BY genus, specific_epithet
                    LIMIT 1
                    """,
                    (genus, species),
                )
            row = cur.fetchone()
            if not row:
                return None
            return row[0], row[1], row[2]

    def taxon_from_vernacular(self, name: str) -> tuple[str, str, str | None] | None:
        name = name.strip()
        if not name:
            return None
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT t.genus, t.specific_epithet, t.family
                FROM vernacular_min v
                JOIN taxon_min t ON t.taxon_id = v.taxon_id
                WHERE v.vernacular_name = ?
                """
                + lang_clause
                + """
                ORDER BY v.is_preferred_name DESC, v.vernacular_name
                LIMIT 1
                """,
                (name, *lang_params),
            )
            row = cur.fetchone()
            if not row:
                return None
            return row[0], row[1], row[2]

    def taxon_id_from_scientific(self, genus: str, species: str) -> int | None:
        """Return the taxon_id (Sporely id on v2, NorTaxa DwC id on legacy)
        for a scientific name. Prefers the row with a preferred alias when
        multiple rows share the canonical name.
        """
        genus = (genus or "").strip()
        species = (species or "").strip()
        if not genus or not species:
            return None
        scientific_name = f"{genus} {species}"
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT t.taxon_id
                FROM taxon_min t
                LEFT JOIN scientific_name_min s
                  ON s.taxon_id = t.taxon_id
                 AND s.scientific_name = ? COLLATE NOCASE
                WHERE (t.genus = ? COLLATE NOCASE
                       AND t.specific_epithet = ? COLLATE NOCASE)
                   OR t.canonical_scientific_name = ? COLLATE NOCASE
                ORDER BY
                  CASE WHEN s.is_preferred_name = 1 THEN 0 ELSE 1 END,
                  CASE WHEN t.canonical_scientific_name = ? COLLATE NOCASE THEN 0 ELSE 1 END,
                  t.taxon_id
                LIMIT 1
                """,
                (scientific_name, genus, species, scientific_name, scientific_name),
            ).fetchone()
        return int(row[0]) if row else None

    def taxon_id_from_vernacular(self, name: str, language_code: str | None = None) -> int | None:
        """Resolve a vernacular query to a single taxon_id. Uses the
        language fan-out ``no → (no, nb, nn)`` when applicable."""
        name = (name or "").strip()
        if not name:
            return None
        lang_clause, lang_params = self._language_clause(language_code)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT v.taxon_id "
                "FROM vernacular_min v "
                "WHERE v.vernacular_name = ? COLLATE NOCASE "
                + lang_clause +
                " ORDER BY v.is_preferred_name DESC, v.language_code, v.vernacular_id "
                "LIMIT 1",
                (name, *lang_params),
            ).fetchone()
        return int(row[0]) if row else None

    def list_vernacular_alternatives(
        self, taxon_id: int, languages: tuple[str, ...] | None = None,
    ) -> list[dict]:
        """Return every vernacular row for ``taxon_id``, ordered so that the
        preferred user-facing language block comes first and each language
        keeps its own rows distinct.

        Ordering is deterministic: (language-priority, is_preferred DESC,
        vernacular_id). ``languages`` is a tuple of codes that should be
        promoted to the top; every remaining language follows in
        alphabetical order.
        """
        if not self._has_language():
            return []
        with self._connect() as conn:
            rows = list(conn.execute(
                "SELECT vernacular_id, language_code, vernacular_name, "
                "       is_preferred_name, source "
                "FROM vernacular_min WHERE taxon_id = ? ORDER BY "
                "vernacular_id",
                (int(taxon_id),),
            ))
        result = [
            {"language_code": str(row[1]), "vernacular_name": str(row[2]),
             "is_preferred": bool(row[3]), "source": row[4],
             "vernacular_id": int(row[0])}
            for row in rows
        ]
        promote = tuple(languages) if languages else ()

        def sort_key(item):
            lang = item["language_code"]
            if lang in promote:
                priority = promote.index(lang)
            else:
                priority = len(promote) + 1
            return (priority, 0 if item["is_preferred"] else 1,
                    item["language_code"], item["vernacular_id"])
        result.sort(key=sort_key)
        return result

    def vernacular_from_taxon(self, genus: str, species: str) -> str | None:
        if not genus or not species:
            return None
        resolved = self.taxon_from_scientific(genus, species)
        if resolved:
            genus, species, _family = resolved
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT v.vernacular_name
                FROM vernacular_min v
                JOIN taxon_min t ON t.taxon_id = v.taxon_id
                WHERE t.genus = ? COLLATE NOCASE
                  AND t.specific_epithet = ? COLLATE NOCASE
                """
                + lang_clause
                + """
                ORDER BY v.is_preferred_name DESC, v.vernacular_name
                LIMIT 1
                """,
                (genus, species, *lang_params),
            )
            row = cur.fetchone()
            return row[0] if row else None


__all__ = ["VernacularDB"]
