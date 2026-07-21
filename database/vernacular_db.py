"""SQLite helper for vernacular name lookup."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from utils.vernacular_utils import normalize_vernacular_language


class VernacularDB:
    """Simple helper for vernacular name lookup."""

    def __init__(self, db_path: Path, language_code: str | None = None):
        self.db_path = db_path
        self.language_code = normalize_vernacular_language(language_code) if language_code else None
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

    def _has_language(self) -> bool:
        if self._has_language_column is None:
            with self._connect() as conn:
                cur = conn.execute("PRAGMA table_info(vernacular_min)")
                self._has_language_column = any(row[1] == "language_code" for row in cur.fetchall())
        return bool(self._has_language_column)

    def _language_clause(self, language_code: str | None) -> tuple[str, list[str]]:
        if not self._has_language():
            return "", []
        raw = language_code or self.language_code
        if not raw:
            return "", []
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

    def vernaculars_from_taxa(
        self,
        taxa: set[tuple[str, str]] | list[tuple[str, str]],
    ) -> dict[tuple[str, str], str | None]:
        """Resolve many scientific names in one database pass."""
        requested: dict[tuple[str, str], tuple[str, str]] = {}
        for genus, species in taxa or []:
            genus_text = str(genus or "").strip()
            species_text = str(species or "").strip()
            if not genus_text or not species_text:
                continue
            requested.setdefault(
                (genus_text.casefold(), species_text.casefold()),
                (genus_text, species_text),
            )
        if not requested:
            return {}

        results = {taxon: None for taxon in requested.values()}
        lang_clause, lang_params = self._language_clause(None)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TEMP TABLE requested_taxa (
                    genus TEXT NOT NULL COLLATE NOCASE,
                    species TEXT NOT NULL COLLATE NOCASE,
                    scientific_name TEXT NOT NULL COLLATE NOCASE,
                    PRIMARY KEY (genus, species)
                ) WITHOUT ROWID
                """
            )
            conn.executemany(
                "INSERT INTO requested_taxa (genus, species, scientific_name) VALUES (?, ?, ?)",
                (
                    (genus, species, f"{genus} {species}")
                    for genus, species in requested.values()
                ),
            )

            candidate_queries = [
                """
                SELECT r.genus, r.species, t.taxon_id, 0 AS resolution_rank,
                       1 AS scientific_preferred
                FROM requested_taxa r
                JOIN taxon_min t
                  ON t.genus = r.genus COLLATE NOCASE
                 AND t.specific_epithet = r.species COLLATE NOCASE
                """,
                """
                SELECT r.genus, r.species, t.taxon_id, 1 AS resolution_rank,
                       1 AS scientific_preferred
                FROM requested_taxa r
                JOIN taxon_min t
                  ON t.canonical_scientific_name = r.scientific_name COLLATE NOCASE
                """,
            ]
            if self._has_scientific_name_table():
                candidate_queries.append(
                    """
                    SELECT r.genus, r.species, s.taxon_id, 1 AS resolution_rank,
                           s.is_preferred_name AS scientific_preferred
                    FROM requested_taxa r
                    JOIN scientific_name_min s
                      ON s.scientific_name = r.scientific_name COLLATE NOCASE
                    """
                )

            cur = conn.execute(
                """
                WITH candidates AS (
                """
                + " UNION ALL ".join(candidate_queries)
                + """
                )
                SELECT c.genus, c.species, v.vernacular_name
                FROM candidates c
                JOIN vernacular_min v ON v.taxon_id = c.taxon_id
                WHERE 1 = 1
                """
                + lang_clause
                + """
                ORDER BY c.genus COLLATE NOCASE,
                         c.species COLLATE NOCASE,
                         c.resolution_rank,
                         c.scientific_preferred DESC,
                         v.is_preferred_name DESC,
                         v.vernacular_name
                """,
                tuple(lang_params),
            )
            resolved_keys: set[tuple[str, str]] = set()
            for genus, species, vernacular_name in cur.fetchall():
                lookup_key = (str(genus).casefold(), str(species).casefold())
                if lookup_key in resolved_keys:
                    continue
                original_taxon = requested.get(lookup_key)
                if original_taxon is None:
                    continue
                results[original_taxon] = vernacular_name
                resolved_keys.add(lookup_key)
        return results


__all__ = ["VernacularDB"]
