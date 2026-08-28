"""Repository/service layer for the normalized reference library.

The library entities (``reference_works``, ``reference_taxon_treatments``,
``reference_measurement_sets``) live in ``reference_values.db``. The
observation link table (``observation_reference_uses``) lives in the
main ``mushrooms.db`` observation database. SQLite cannot enforce a
foreign key across two separate database files, so the cross-database
link is enforced here by service-layer validation.
"""
from __future__ import annotations

import json
import re
import sqlite3
import uuid
from dataclasses import dataclass, field, fields, asdict
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

from database.reference_citation import (
    build_full_citation,
    build_observation_reference_snapshot,
    build_short_label,
    observation_snapshots_semantically_equal,
    serialize_snapshot,
)
from database.reference_library_schema import (
    OBSERVATION_REFERENCE_ROLES,
    REFERENCE_MEASUREMENT_CHARACTERS,
    REFERENCE_MEASUREMENT_DATA_KINDS,
    REFERENCE_WORK_TYPES,
    init_observation_reference_uses_schema,
    init_reference_library_schema,
)
from database.reference_sync_state import (
    record_library_mutation_intent,
    record_use_mutation_intent,
)
from database.schema import get_connection, get_reference_connection


# --- Errors ------------------------------------------------------------------


class ReferenceLibraryError(Exception):
    """Base class for reference library service errors."""


class ReferenceValidationError(ReferenceLibraryError, ValueError):
    """Invalid enum or field value."""


class ReferenceIntegrityError(ReferenceLibraryError):
    """Cross-database or duplicate integrity violation."""


class ReferenceInUseError(ReferenceIntegrityError):
    """Attempted delete of a measurement set with active observation uses."""

    def __init__(self, measurement_set_id: str, use_count: int) -> None:
        super().__init__(
            f"measurement set {measurement_set_id} has {use_count} observation use(s)"
        )
        self.measurement_set_id = measurement_set_id
        self.use_count = use_count


# --- Helpers -----------------------------------------------------------------


# Data kinds that the Analysis reference-series translator can render as
# an attached observation reference. `parmasto` and any future kinds are
# intentionally excluded until they have plot support and a translator
# path so we never persist an orphan attachment row that has no visible
# entry in the UI (and therefore no detach affordance).
SUPPORTED_ATTACHMENT_DATA_KINDS: frozenset[str] = frozenset(
    {"range", "summary", "raw_points"}
)


def _new_uuid() -> str:
    return str(uuid.uuid4())


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


_DOI_STRIP_RE = re.compile(r"^(?:https?://)?(?:dx\.)?doi\.org/", re.IGNORECASE)


def normalize_doi(doi: str | None) -> str | None:
    """Normalize a DOI for equality/duplicate detection.

    Strips ``https://doi.org/``, ``http://dx.doi.org/`` prefixes and
    lowercases the result. Returns ``None`` for missing/blank input.
    """
    if doi is None:
        return None
    text = str(doi).strip()
    if not text:
        return None
    text = _DOI_STRIP_RE.sub("", text)
    return text.lower() or None


def normalize_isbn(isbn: str | None) -> str | None:
    """Normalize an ISBN to digits only (with a trailing ``X`` for ISBN-10).

    Returns ``None`` for missing/blank input.
    """
    if isbn is None:
        return None
    digits = re.sub(r"[^0-9Xx]", "", str(isbn))
    if not digits:
        return None
    return digits.upper()


def _validate_enum(value: str, allowed: Iterable[str], field_name: str) -> str:
    if value not in allowed:
        raise ReferenceValidationError(
            f"invalid {field_name} value {value!r} (allowed: {sorted(allowed)})"
        )
    return value


def _connect_reference() -> sqlite3.Connection:
    conn = get_reference_connection()
    conn.row_factory = sqlite3.Row
    init_reference_library_schema(conn)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _connect_observations() -> sqlite3.Connection:
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    init_observation_reference_uses_schema(conn)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# --- Dataclasses -------------------------------------------------------------


@dataclass
class ReferenceWork:
    id: str
    type: str
    title: str
    short_label: str
    authors_json: str = "[]"
    editors_json: str = "[]"
    citation_key: str | None = None
    container_title: str | None = None
    year: int | None = None
    edition: str | None = None
    publisher: str | None = None
    place: str | None = None
    volume: str | None = None
    issue: str | None = None
    pages: str | None = None
    doi: str | None = None
    isbn: str | None = None
    url: str | None = None
    language: str | None = None
    citation_override: str | None = None
    owner_id: str | None = None
    revision: int = 1
    created_at: str | None = None
    updated_at: str | None = None


@dataclass
class TaxonTreatment:
    id: str
    reference_work_id: str
    name_as_published: str
    taxon_id: str | None = None
    page_from: int | None = None
    page_to: int | None = None
    locator_text: str | None = None
    treatment_notes: str | None = None
    revision: int = 1
    created_at: str | None = None
    updated_at: str | None = None


@dataclass
class MeasurementSet:
    id: str
    taxon_treatment_id: str
    character: str
    data_kind: str
    raw_text: str | None = None
    length_min: float | None = None
    length_core_min: float | None = None
    length_core_max: float | None = None
    length_max: float | None = None
    width_min: float | None = None
    width_core_min: float | None = None
    width_core_max: float | None = None
    width_max: float | None = None
    q_min: float | None = None
    q_max: float | None = None
    q_mean: float | None = None
    length_mean: float | None = None
    width_mean: float | None = None
    sample_size: int | None = None
    specimen_count: int | None = None
    mount_medium: str | None = None
    stain: str | None = None
    preparation: str | None = None
    measurement_method: str | None = None
    notes: str | None = None
    raw_points_json: str | None = None
    revision: int = 1
    supersedes_id: str | None = None
    legacy_reference_value_id: int | None = None
    created_at: str | None = None
    updated_at: str | None = None


@dataclass
class ObservationReferenceUse:
    id: str
    observation_id: int
    reference_measurement_set_id: str
    role: str
    reference_revision: int
    snapshot_json: str
    note: str | None = None
    selected_at: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


@dataclass(frozen=True)
class ObservationReferenceSnapshotStatus:
    """Semantic relationship between a frozen use and its current source."""

    use_id: str
    state: str
    current_reference_revision: int | None = None


@dataclass(frozen=True)
class MeasurementSetSuccessorResolution:
    """Deterministic resolution of the explicit successor graph."""

    source_id: str
    state: str
    path_ids: tuple[str, ...]
    successor_id: str | None = None
    fork_successor_ids: tuple[str, ...] = ()
    successor_snapshot_json: str | None = None


@dataclass
class MeasurementSetCandidate:
    """Read-only projection of a measurement set joined with treatment/work
    display metadata for the attachment chooser UI. Not stored anywhere."""

    measurement_set_id: str
    short_label: str
    name_as_published: str
    locator_text: str | None
    data_kind: str
    raw_text: str | None
    revision: int
    reference_work_id: str
    reference_treatment_id: str
    work_title: str | None = None
    year: int | None = None
    taxon_id: str | None = None
    is_favorite: bool = False
    recent_use_sequence: int | None = None


@dataclass(frozen=True)
class MeasurementSetPreference:
    """Local chooser preference metadata for one normalized measurement set."""

    measurement_set_id: str
    is_favorite: bool = False
    recent_use_sequence: int | None = None


@dataclass(frozen=True)
class QuickAddReferenceRequest:
    """Validated editor output needed to create and attach a reference."""

    observation_id: int
    work: ReferenceWork
    treatment: TaxonTreatment
    measurement_set: MeasurementSet
    existing_work_id: str | None = None
    role: str = "compared"
    note: str | None = None


@dataclass(frozen=True)
class QuickAddReferenceResult:
    """Entities selected or created by one quick-add operation."""

    work: ReferenceWork
    treatment: TaxonTreatment
    measurement_set: MeasurementSet
    use: ObservationReferenceUse
    created_work: bool
    created_treatment: bool
    created_measurement_set: bool
    created_attachment: bool


def _row_to_dataclass(row: sqlite3.Row, cls):
    if row is None:
        return None
    valid = {f.name for f in fields(cls)}
    data = {key: row[key] for key in row.keys() if key in valid}
    return cls(**data)


# --- Reference work repository ----------------------------------------------


class ReferenceWorkRepository:
    """CRUD/search for ``reference_works``."""

    _COLUMNS: tuple[str, ...] = (
        "id",
        "type",
        "citation_key",
        "authors_json",
        "editors_json",
        "title",
        "container_title",
        "year",
        "edition",
        "publisher",
        "place",
        "volume",
        "issue",
        "pages",
        "doi",
        "isbn",
        "url",
        "language",
        "short_label",
        "citation_override",
        "owner_id",
        "revision",
        "created_at",
        "updated_at",
    )
    # ``verification_status`` and ``visibility`` remain as compatibility
    # columns on existing sqlite installations (see
    # ``reference_library_schema._REFERENCE_WORKS_DDL``) but the
    # application no longer reads, writes or exposes them. New INSERTs
    # simply omit both columns and rely on the database DEFAULTs so old
    # DB files keep loading without a destructive column-drop migration.

    @staticmethod
    def _validate(work: ReferenceWork) -> None:
        _validate_enum(work.type, REFERENCE_WORK_TYPES, "reference_work.type")
        if not str(work.title or "").strip():
            raise ReferenceValidationError("reference_work.title is required")
        # authors_json / editors_json must be JSON lists (empty is fine)
        for name in ("authors_json", "editors_json"):
            raw = getattr(work, name)
            try:
                parsed = json.loads(raw) if isinstance(raw, str) else raw
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ReferenceValidationError(
                    f"reference_work.{name} must be valid JSON"
                ) from exc
            if not isinstance(parsed, list):
                raise ReferenceValidationError(
                    f"reference_work.{name} must encode a list"
                )

    @classmethod
    def create(cls, work: ReferenceWork) -> ReferenceWork:
        cls._validate(work)
        if not work.id:
            work.id = _new_uuid()
        if not work.short_label:
            work.short_label = build_short_label(work) or work.title
        work.doi = normalize_doi(work.doi)
        work.isbn = normalize_isbn(work.isbn)
        now = _now()
        work.created_at = work.created_at or now
        work.updated_at = work.updated_at or now
        work.revision = work.revision or 1

        conn = _connect_reference()
        try:
            values = tuple(getattr(work, name) for name in cls._COLUMNS)
            placeholders = ", ".join("?" for _ in cls._COLUMNS)
            try:
                conn.execute(
                    f"INSERT INTO reference_works ({', '.join(cls._COLUMNS)}) "
                    f"VALUES ({placeholders})",
                    values,
                )
            except sqlite3.IntegrityError as exc:
                raise ReferenceIntegrityError(str(exc)) from exc
            conn.commit()
        finally:
            conn.close()
        return work

    @staticmethod
    def get(work_id: str) -> ReferenceWork | None:
        conn = _connect_reference()
        try:
            row = conn.execute(
                "SELECT * FROM reference_works WHERE id = ?", (work_id,)
            ).fetchone()
        finally:
            conn.close()
        return _row_to_dataclass(row, ReferenceWork)

    @classmethod
    def update(
        cls,
        work_id: str,
        updates: dict[str, Any],
        *,
        bump_revision: bool = True,
    ) -> ReferenceWork:
        """Apply ``updates`` to the work.

        ``bump_revision=True`` increments ``revision`` and updates
        ``updated_at`` (the intended path for meaningful edits).
        ``bump_revision=False`` writes without touching revision (for
        purely cosmetic normalizations).
        """
        existing = cls.get(work_id)
        if existing is None:
            raise ReferenceIntegrityError(f"reference_work {work_id} not found")

        allowed_fields = {name for name in cls._COLUMNS if name != "id"}
        for name in list(updates.keys()):
            if name not in allowed_fields:
                raise ReferenceValidationError(f"cannot update field {name!r}")
        merged = ReferenceWork(**{**asdict(existing), **updates})
        cls._validate(merged)
        merged.doi = normalize_doi(merged.doi)
        merged.isbn = normalize_isbn(merged.isbn)
        if bump_revision:
            merged.revision = int(existing.revision or 1) + 1
            merged.updated_at = _now()

        conn = _connect_reference()
        try:
            assignments = ", ".join(f"{col} = ?" for col in allowed_fields)
            params = tuple(getattr(merged, col) for col in allowed_fields) + (work_id,)
            try:
                conn.execute(
                    f"UPDATE reference_works SET {assignments} WHERE id = ?",
                    params,
                )
                record_library_mutation_intent(conn, "work", work_id)
            except sqlite3.IntegrityError as exc:
                raise ReferenceIntegrityError(str(exc)) from exc
            conn.commit()
        finally:
            conn.close()
        return merged

    @staticmethod
    def search(
        query: str | None = None,
        *,
        limit: int = 50,
    ) -> list[ReferenceWork]:
        conn = _connect_reference()
        try:
            if query:
                like = f"%{query.strip().lower()}%"
                rows = conn.execute(
                    """
                    SELECT * FROM reference_works
                    WHERE LOWER(title) LIKE ?
                       OR LOWER(COALESCE(short_label, '')) LIKE ?
                       OR LOWER(COALESCE(container_title, '')) LIKE ?
                       OR LOWER(COALESCE(authors_json, '')) LIKE ?
                       OR LOWER(COALESCE(citation_key, '')) LIKE ?
                       OR CAST(COALESCE(year, '') AS TEXT) LIKE ?
                       OR LOWER(COALESCE(doi, '')) LIKE ?
                       OR LOWER(COALESCE(isbn, '')) LIKE ?
                    ORDER BY COALESCE(year, 0) DESC, updated_at DESC
                    LIMIT ?
                    """,
                    (like, like, like, like, like, like, like, like, int(limit)),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT * FROM reference_works
                    ORDER BY updated_at DESC
                    LIMIT ?
                    """,
                    (int(limit),),
                ).fetchall()
        finally:
            conn.close()
        return [_row_to_dataclass(row, ReferenceWork) for row in rows]

    @staticmethod
    def list_recent(limit: int = 20) -> list[ReferenceWork]:
        conn = _connect_reference()
        try:
            rows = conn.execute(
                """
                SELECT w.*,
                       COALESCE(MAX(p.is_favorite), 0) AS _favorite,
                       MAX(p.recent_use_sequence) AS _recent_sequence
                FROM reference_works AS w
                LEFT JOIN reference_taxon_treatments AS t
                  ON t.reference_work_id = w.id
                LEFT JOIN reference_measurement_sets AS ms
                  ON ms.taxon_treatment_id = t.id
                LEFT JOIN reference_measurement_set_preferences AS p
                  ON p.measurement_set_id = ms.id
                GROUP BY w.id
                ORDER BY _favorite DESC,
                         CASE WHEN _recent_sequence IS NULL THEN 1 ELSE 0 END,
                         _recent_sequence DESC,
                         w.updated_at DESC,
                         w.id ASC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        finally:
            conn.close()
        return [_row_to_dataclass(row, ReferenceWork) for row in rows]

    @staticmethod
    def find_by_doi(doi: str) -> ReferenceWork | None:
        normalized = normalize_doi(doi)
        if not normalized:
            return None
        conn = _connect_reference()
        try:
            row = conn.execute(
                "SELECT * FROM reference_works WHERE doi = ? LIMIT 1",
                (normalized,),
            ).fetchone()
        finally:
            conn.close()
        return _row_to_dataclass(row, ReferenceWork)

    @staticmethod
    def find_by_isbn(isbn: str) -> ReferenceWork | None:
        normalized = normalize_isbn(isbn)
        if not normalized:
            return None
        conn = _connect_reference()
        try:
            row = conn.execute(
                "SELECT * FROM reference_works WHERE isbn = ? LIMIT 1",
                (normalized,),
            ).fetchone()
        finally:
            conn.close()
        return _row_to_dataclass(row, ReferenceWork)

    @staticmethod
    def delete(work_id: str) -> None:
        """Delete a work.

        Fails via ``ReferenceInUseError`` if any measurement set under
        this work has active observation uses. Otherwise deletes
        descendants explicitly (measurement sets, then treatments, then
        the work) — the schema uses ``ON DELETE RESTRICT`` so silent
        SQL cascade is not available.
        """
        conn = _connect_reference()
        try:
            row = conn.execute(
                "SELECT id FROM reference_works WHERE id = ?", (work_id,)
            ).fetchone()
            if row is None:
                raise ReferenceIntegrityError(f"reference_work {work_id} not found")
            set_rows = conn.execute(
                """
                SELECT ms.id
                FROM reference_measurement_sets AS ms
                JOIN reference_taxon_treatments AS t
                  ON t.id = ms.taxon_treatment_id
                WHERE t.reference_work_id = ?
                """,
                (work_id,),
            ).fetchall()
        finally:
            conn.close()
        for row in set_rows:
            uses = ObservationReferenceUseRepository.count_uses(row["id"])
            if uses:
                raise ReferenceInUseError(row["id"], uses)

        conn = _connect_reference()
        try:
            conn.execute("BEGIN")
            conn.execute(
                """
                DELETE FROM reference_measurement_sets
                WHERE taxon_treatment_id IN (
                    SELECT id FROM reference_taxon_treatments
                    WHERE reference_work_id = ?
                )
                """,
                (work_id,),
            )
            conn.execute(
                "DELETE FROM reference_taxon_treatments WHERE reference_work_id = ?",
                (work_id,),
            )
            conn.execute("DELETE FROM reference_works WHERE id = ?", (work_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


# --- Treatment repository ----------------------------------------------------


class TaxonTreatmentRepository:
    """CRUD for ``reference_taxon_treatments``."""

    _COLUMNS: tuple[str, ...] = (
        "id",
        "reference_work_id",
        "taxon_id",
        "name_as_published",
        "page_from",
        "page_to",
        "locator_text",
        "treatment_notes",
        "revision",
        "created_at",
        "updated_at",
    )

    @staticmethod
    def _validate(treatment: TaxonTreatment) -> None:
        if not str(treatment.reference_work_id or "").strip():
            raise ReferenceValidationError("taxon_treatment.reference_work_id is required")
        if not str(treatment.name_as_published or "").strip():
            raise ReferenceValidationError("taxon_treatment.name_as_published is required")

    @classmethod
    def create(cls, treatment: TaxonTreatment) -> TaxonTreatment:
        cls._validate(treatment)
        if not treatment.id:
            treatment.id = _new_uuid()
        now = _now()
        treatment.created_at = treatment.created_at or now
        treatment.updated_at = treatment.updated_at or now
        treatment.revision = treatment.revision or 1

        conn = _connect_reference()
        try:
            work_exists = conn.execute(
                "SELECT 1 FROM reference_works WHERE id = ? LIMIT 1",
                (treatment.reference_work_id,),
            ).fetchone()
            if not work_exists:
                raise ReferenceIntegrityError(
                    f"reference_work {treatment.reference_work_id} does not exist"
                )
            values = tuple(getattr(treatment, name) for name in cls._COLUMNS)
            placeholders = ", ".join("?" for _ in cls._COLUMNS)
            conn.execute(
                f"INSERT INTO reference_taxon_treatments ({', '.join(cls._COLUMNS)}) "
                f"VALUES ({placeholders})",
                values,
            )
            conn.commit()
        finally:
            conn.close()
        return treatment

    @staticmethod
    def get(treatment_id: str) -> TaxonTreatment | None:
        conn = _connect_reference()
        try:
            row = conn.execute(
                "SELECT * FROM reference_taxon_treatments WHERE id = ?",
                (treatment_id,),
            ).fetchone()
        finally:
            conn.close()
        return _row_to_dataclass(row, TaxonTreatment)

    @staticmethod
    def list_for_work(work_id: str) -> list[TaxonTreatment]:
        conn = _connect_reference()
        try:
            rows = conn.execute(
                """
                SELECT * FROM reference_taxon_treatments
                WHERE reference_work_id = ?
                ORDER BY name_as_published, created_at, id
                """,
                (work_id,),
            ).fetchall()
        finally:
            conn.close()
        return [_row_to_dataclass(row, TaxonTreatment) for row in rows]

    @staticmethod
    def list_for_taxon(taxon_id: str) -> list[TaxonTreatment]:
        conn = _connect_reference()
        try:
            rows = conn.execute(
                """
                SELECT * FROM reference_taxon_treatments
                WHERE taxon_id = ?
                ORDER BY updated_at DESC
                """,
                (taxon_id,),
            ).fetchall()
        finally:
            conn.close()
        return [_row_to_dataclass(row, TaxonTreatment) for row in rows]

    @classmethod
    def update(
        cls,
        treatment_id: str,
        updates: dict[str, Any],
        *,
        bump_revision: bool = True,
    ) -> TaxonTreatment:
        existing = cls.get(treatment_id)
        if existing is None:
            raise ReferenceIntegrityError(f"taxon_treatment {treatment_id} not found")

        allowed = {name for name in cls._COLUMNS if name != "id"}
        for name in list(updates.keys()):
            if name not in allowed:
                raise ReferenceValidationError(f"cannot update field {name!r}")
        merged = TaxonTreatment(**{**asdict(existing), **updates})
        cls._validate(merged)
        if bump_revision:
            merged.revision = int(existing.revision or 1) + 1
            merged.updated_at = _now()

        conn = _connect_reference()
        try:
            assignments = ", ".join(f"{col} = ?" for col in allowed)
            params = tuple(getattr(merged, col) for col in allowed) + (treatment_id,)
            conn.execute(
                f"UPDATE reference_taxon_treatments SET {assignments} WHERE id = ?",
                params,
            )
            record_library_mutation_intent(conn, "treatment", treatment_id)
            conn.commit()
        finally:
            conn.close()
        return merged

    @staticmethod
    def delete(treatment_id: str) -> None:
        """Delete a treatment.

        Fails via ``ReferenceInUseError`` if any measurement set under
        this treatment has active observation uses. Otherwise deletes
        measurement sets first, then the treatment (``ON DELETE
        RESTRICT`` prevents silent SQL cascade).
        """
        conn = _connect_reference()
        try:
            row = conn.execute(
                "SELECT id FROM reference_taxon_treatments WHERE id = ?",
                (treatment_id,),
            ).fetchone()
            if row is None:
                raise ReferenceIntegrityError(f"taxon_treatment {treatment_id} not found")
            set_rows = conn.execute(
                "SELECT id FROM reference_measurement_sets WHERE taxon_treatment_id = ?",
                (treatment_id,),
            ).fetchall()
        finally:
            conn.close()
        for row in set_rows:
            uses = ObservationReferenceUseRepository.count_uses(row["id"])
            if uses:
                raise ReferenceInUseError(row["id"], uses)

        conn = _connect_reference()
        try:
            conn.execute("BEGIN")
            conn.execute(
                "DELETE FROM reference_measurement_sets WHERE taxon_treatment_id = ?",
                (treatment_id,),
            )
            conn.execute(
                "DELETE FROM reference_taxon_treatments WHERE id = ?",
                (treatment_id,),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


# --- Measurement set repository ---------------------------------------------


class MeasurementSetRepository:
    """CRUD for ``reference_measurement_sets``."""

    _COLUMNS: tuple[str, ...] = (
        "id",
        "taxon_treatment_id",
        "character",
        "raw_text",
        "data_kind",
        "length_min",
        "length_core_min",
        "length_core_max",
        "length_max",
        "width_min",
        "width_core_min",
        "width_core_max",
        "width_max",
        "q_min",
        "q_max",
        "q_mean",
        "length_mean",
        "width_mean",
        "sample_size",
        "specimen_count",
        "mount_medium",
        "stain",
        "preparation",
        "measurement_method",
        "notes",
        "raw_points_json",
        "revision",
        "supersedes_id",
        "legacy_reference_value_id",
        "created_at",
        "updated_at",
    )

    @staticmethod
    def _validate(ms: MeasurementSet) -> None:
        if not str(ms.taxon_treatment_id or "").strip():
            raise ReferenceValidationError(
                "measurement_set.taxon_treatment_id is required"
            )
        _validate_enum(
            ms.character,
            REFERENCE_MEASUREMENT_CHARACTERS,
            "measurement_set.character",
        )
        _validate_enum(
            ms.data_kind,
            REFERENCE_MEASUREMENT_DATA_KINDS,
            "measurement_set.data_kind",
        )
        if ms.raw_points_json is not None and str(ms.raw_points_json).strip():
            try:
                parsed = json.loads(ms.raw_points_json)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ReferenceValidationError(
                    "measurement_set.raw_points_json must be valid JSON"
                ) from exc
            if not isinstance(parsed, list) or not parsed:
                raise ReferenceValidationError(
                    "measurement_set.raw_points_json must be a non-empty JSON list"
                )
            for point in parsed:
                # Accept either a numeric length or a {length, width, ...} dict.
                if isinstance(point, (int, float)):
                    continue
                if isinstance(point, dict):
                    numeric_present = any(
                        isinstance(point.get(key), (int, float))
                        for key in ("length", "width", "l", "w")
                    )
                    if numeric_present:
                        continue
                raise ReferenceValidationError(
                    "measurement_set.raw_points_json entries must be numeric or "
                    "dicts containing at least one numeric length/width"
                )

    @classmethod
    def create(cls, ms: MeasurementSet) -> MeasurementSet:
        cls._validate(ms)
        if not ms.id:
            ms.id = _new_uuid()
        now = _now()
        ms.created_at = ms.created_at or now
        ms.updated_at = ms.updated_at or now
        ms.revision = ms.revision or 1

        conn = _connect_reference()
        try:
            treatment_exists = conn.execute(
                "SELECT 1 FROM reference_taxon_treatments WHERE id = ? LIMIT 1",
                (ms.taxon_treatment_id,),
            ).fetchone()
            if not treatment_exists:
                raise ReferenceIntegrityError(
                    f"taxon_treatment {ms.taxon_treatment_id} does not exist"
                )
            values = tuple(getattr(ms, name) for name in cls._COLUMNS)
            placeholders = ", ".join("?" for _ in cls._COLUMNS)
            conn.execute(
                f"INSERT INTO reference_measurement_sets ({', '.join(cls._COLUMNS)}) "
                f"VALUES ({placeholders})",
                values,
            )
            conn.commit()
        finally:
            conn.close()
        return ms

    @staticmethod
    def get(set_id: str) -> MeasurementSet | None:
        conn = _connect_reference()
        try:
            row = conn.execute(
                "SELECT * FROM reference_measurement_sets WHERE id = ?", (set_id,)
            ).fetchone()
        finally:
            conn.close()
        return _row_to_dataclass(row, MeasurementSet)

    @staticmethod
    def list_for_treatment(treatment_id: str) -> list[MeasurementSet]:
        conn = _connect_reference()
        try:
            rows = conn.execute(
                """
                SELECT * FROM reference_measurement_sets
                WHERE taxon_treatment_id = ?
                ORDER BY created_at, id
                """,
                (treatment_id,),
            ).fetchall()
        finally:
            conn.close()
        return [_row_to_dataclass(row, MeasurementSet) for row in rows]

    @classmethod
    def update(
        cls,
        set_id: str,
        updates: dict[str, Any],
        *,
        bump_revision: bool = True,
    ) -> MeasurementSet:
        existing = cls.get(set_id)
        if existing is None:
            raise ReferenceIntegrityError(f"measurement_set {set_id} not found")

        allowed = {name for name in cls._COLUMNS if name != "id"}
        for name in list(updates.keys()):
            if name not in allowed:
                raise ReferenceValidationError(f"cannot update field {name!r}")
        merged = MeasurementSet(**{**asdict(existing), **updates})
        cls._validate(merged)
        if bump_revision:
            merged.revision = int(existing.revision or 1) + 1
            merged.updated_at = _now()

        conn = _connect_reference()
        try:
            assignments = ", ".join(f"{col} = ?" for col in allowed)
            params = tuple(getattr(merged, col) for col in allowed) + (set_id,)
            conn.execute(
                f"UPDATE reference_measurement_sets SET {assignments} WHERE id = ?",
                params,
            )
            record_library_mutation_intent(conn, "measurement_set", set_id)
            conn.commit()
        finally:
            conn.close()
        return merged

    @classmethod
    def create_revision(
        cls,
        set_id: str,
        updates: dict[str, Any],
    ) -> MeasurementSet:
        """Create a successor measurement set that supersedes ``set_id``.

        Returns the newly created record with ``supersedes_id`` set to the
        prior record and ``revision`` incremented.
        """
        existing = cls.get(set_id)
        if existing is None:
            raise ReferenceIntegrityError(f"measurement_set {set_id} not found")
        base = asdict(existing)
        base.pop("id", None)
        base.pop("created_at", None)
        base.pop("updated_at", None)
        base["supersedes_id"] = existing.id
        base["revision"] = int(existing.revision or 1) + 1
        base.update(updates)
        new_ms = MeasurementSet(id=_new_uuid(), **base)
        return cls.create(new_ms)

    @classmethod
    def resolve_terminal_successor(
        cls, set_id: str
    ) -> MeasurementSetSuccessorResolution:
        """Follow one unambiguous successor chain and fail closed otherwise."""
        source_id = str(set_id or "").strip()
        if not source_id or cls.get(source_id) is None:
            return MeasurementSetSuccessorResolution(
                source_id=source_id,
                state="source_missing",
                path_ids=(),
            )

        path = [source_id]
        visited = {source_id}
        current_id = source_id
        conn = _connect_reference()
        try:
            while True:
                rows = conn.execute(
                    """
                    SELECT id FROM reference_measurement_sets
                    WHERE supersedes_id = ?
                    ORDER BY id ASC
                    """,
                    (current_id,),
                ).fetchall()
                successors = tuple(str(row["id"]) for row in rows)
                if not successors:
                    if len(path) == 1:
                        return MeasurementSetSuccessorResolution(
                            source_id=source_id,
                            state="no_successor",
                            path_ids=tuple(path),
                        )
                    return MeasurementSetSuccessorResolution(
                        source_id=source_id,
                        state="successor_available",
                        path_ids=tuple(path),
                        successor_id=current_id,
                    )
                if len(successors) > 1:
                    return MeasurementSetSuccessorResolution(
                        source_id=source_id,
                        state="fork",
                        path_ids=tuple(path),
                        fork_successor_ids=successors,
                    )
                next_id = successors[0]
                if next_id in visited:
                    return MeasurementSetSuccessorResolution(
                        source_id=source_id,
                        state="cycle",
                        path_ids=tuple(path + [next_id]),
                    )
                visited.add(next_id)
                path.append(next_id)
                current_id = next_id
        finally:
            conn.close()

    @staticmethod
    def delete(set_id: str) -> None:
        uses = ObservationReferenceUseRepository.count_uses(set_id)
        if uses:
            raise ReferenceInUseError(set_id, uses)
        conn = _connect_reference()
        try:
            cursor = conn.execute(
                "DELETE FROM reference_measurement_sets WHERE id = ?", (set_id,)
            )
            if cursor.rowcount == 0:
                raise ReferenceIntegrityError(
                    f"measurement_set {set_id} not found"
                )
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def list_attachment_candidates(
        *,
        exclude_ids: Iterable[str] | None = None,
        supported_kinds: Iterable[str] | None = None,
    ) -> list[MeasurementSetCandidate]:
        """Return every measurement set joined with its treatment/work
        display metadata for the attachment chooser.

        Deterministic ordering by short label, published taxon name,
        measurement-set id. ``exclude_ids`` (optional) removes rows whose
        measurement-set UUID is already attached to the caller's active
        observation. ``supported_kinds`` (optional) restricts the result
        to measurement sets whose ``data_kind`` is currently plottable
        by the desktop; defaults to
        :data:`SUPPORTED_ATTACHMENT_DATA_KINDS` so unsupported kinds
        (e.g. ``parmasto``) never surface as attachment candidates that
        would produce orphan use rows.
        """
        exclude_set = {str(x) for x in (exclude_ids or [])}
        if supported_kinds is None:
            allowed_kinds = set(SUPPORTED_ATTACHMENT_DATA_KINDS)
        else:
            allowed_kinds = {str(x) for x in supported_kinds}
        conn = _connect_reference()
        try:
            rows = conn.execute(
                """
                SELECT
                    ms.id AS ms_id,
                    ms.data_kind AS ms_data_kind,
                    ms.raw_text AS ms_raw_text,
                    ms.revision AS ms_revision,
                    t.id AS t_id,
                    t.taxon_id AS t_taxon_id,
                    t.name_as_published AS t_name_as_published,
                    t.locator_text AS t_locator_text,
                    w.id AS w_id,
                    w.short_label AS w_short_label,
                    w.title AS w_title,
                    w.year AS w_year,
                    COALESCE(p.is_favorite, 0) AS p_is_favorite,
                    p.recent_use_sequence AS p_recent_use_sequence
                FROM reference_measurement_sets AS ms
                JOIN reference_taxon_treatments AS t
                  ON t.id = ms.taxon_treatment_id
                JOIN reference_works AS w
                  ON w.id = t.reference_work_id
                LEFT JOIN reference_measurement_set_preferences AS p
                  ON p.measurement_set_id = ms.id
                ORDER BY
                    COALESCE(p.is_favorite, 0) DESC,
                    CASE WHEN p.recent_use_sequence IS NULL THEN 1 ELSE 0 END,
                    p.recent_use_sequence DESC,
                    LOWER(COALESCE(w.short_label, w.title, '')),
                    LOWER(COALESCE(t.name_as_published, '')),
                    ms.id
                """
            ).fetchall()
        finally:
            conn.close()
        result: list[MeasurementSetCandidate] = []
        for row in rows:
            set_id = str(row["ms_id"])
            if set_id in exclude_set:
                continue
            data_kind = str(row["ms_data_kind"] or "")
            if allowed_kinds and data_kind not in allowed_kinds:
                continue
            result.append(
                MeasurementSetCandidate(
                    measurement_set_id=set_id,
                    short_label=str(row["w_short_label"] or row["w_title"] or ""),
                    name_as_published=str(row["t_name_as_published"] or ""),
                    locator_text=(str(row["t_locator_text"]) if row["t_locator_text"] else None),
                    data_kind=str(row["ms_data_kind"] or ""),
                    raw_text=(str(row["ms_raw_text"]) if row["ms_raw_text"] else None),
                    revision=int(row["ms_revision"] or 1),
                    reference_work_id=str(row["w_id"]),
                    reference_treatment_id=str(row["t_id"]),
                    work_title=(str(row["w_title"]) if row["w_title"] else None),
                    year=(int(row["w_year"]) if row["w_year"] is not None else None),
                    taxon_id=(str(row["t_taxon_id"]) if row["t_taxon_id"] else None),
                    is_favorite=bool(row["p_is_favorite"]),
                    recent_use_sequence=(
                        int(row["p_recent_use_sequence"])
                        if row["p_recent_use_sequence"] is not None
                        else None
                    ),
                )
            )
        return result


class MeasurementSetPreferenceRepository:
    """Local-only favourites and actual-use recency for chooser candidates."""

    @staticmethod
    def _from_row(row: sqlite3.Row | None) -> MeasurementSetPreference | None:
        if row is None:
            return None
        return MeasurementSetPreference(
            measurement_set_id=str(row["measurement_set_id"]),
            is_favorite=bool(row["is_favorite"]),
            recent_use_sequence=(
                int(row["recent_use_sequence"])
                if row["recent_use_sequence"] is not None
                else None
            ),
        )

    @staticmethod
    def _require_set(conn: sqlite3.Connection, measurement_set_id: str) -> None:
        if conn.execute(
            "SELECT 1 FROM reference_measurement_sets WHERE id = ?",
            (measurement_set_id,),
        ).fetchone() is None:
            raise ReferenceIntegrityError(
                f"measurement_set {measurement_set_id} not found"
            )

    @classmethod
    def get(cls, measurement_set_id: str) -> MeasurementSetPreference | None:
        conn = _connect_reference()
        try:
            row = conn.execute(
                "SELECT measurement_set_id, is_favorite, recent_use_sequence "
                "FROM reference_measurement_set_preferences "
                "WHERE measurement_set_id = ?",
                (measurement_set_id,),
            ).fetchone()
        finally:
            conn.close()
        return cls._from_row(row)

    @classmethod
    def list(cls) -> list[MeasurementSetPreference]:
        conn = _connect_reference()
        try:
            rows = conn.execute(
                "SELECT measurement_set_id, is_favorite, recent_use_sequence "
                "FROM reference_measurement_set_preferences "
                "ORDER BY is_favorite DESC, "
                "CASE WHEN recent_use_sequence IS NULL THEN 1 ELSE 0 END, "
                "recent_use_sequence DESC, measurement_set_id ASC"
            ).fetchall()
        finally:
            conn.close()
        return [cls._from_row(row) for row in rows]

    @classmethod
    def set_favorite(
        cls, measurement_set_id: str, is_favorite: bool
    ) -> MeasurementSetPreference:
        conn = _connect_reference()
        try:
            cls._require_set(conn, measurement_set_id)
            conn.execute(
                "INSERT INTO reference_measurement_set_preferences "
                "(measurement_set_id, is_favorite) VALUES (?, ?) "
                "ON CONFLICT(measurement_set_id) DO UPDATE "
                "SET is_favorite = excluded.is_favorite",
                (measurement_set_id, int(bool(is_favorite))),
            )
            conn.commit()
            row = conn.execute(
                "SELECT measurement_set_id, is_favorite, recent_use_sequence "
                "FROM reference_measurement_set_preferences "
                "WHERE measurement_set_id = ?",
                (measurement_set_id,),
            ).fetchone()
        finally:
            conn.close()
        preference = cls._from_row(row)
        assert preference is not None
        return preference

    @classmethod
    def toggle_favorite(cls, measurement_set_id: str) -> MeasurementSetPreference:
        current = cls.get(measurement_set_id)
        return cls.set_favorite(
            measurement_set_id,
            not current.is_favorite if current is not None else True,
        )

    @classmethod
    def mark_used(cls, measurement_set_id: str) -> MeasurementSetPreference:
        conn = _connect_reference()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cls._require_set(conn, measurement_set_id)
            next_sequence = int(
                conn.execute(
                    "SELECT COALESCE(MAX(recent_use_sequence), 0) + 1 "
                    "FROM reference_measurement_set_preferences"
                ).fetchone()[0]
            )
            conn.execute(
                "INSERT INTO reference_measurement_set_preferences "
                "(measurement_set_id, recent_use_sequence) VALUES (?, ?) "
                "ON CONFLICT(measurement_set_id) DO UPDATE "
                "SET recent_use_sequence = excluded.recent_use_sequence",
                (measurement_set_id, next_sequence),
            )
            conn.commit()
            row = conn.execute(
                "SELECT measurement_set_id, is_favorite, recent_use_sequence "
                "FROM reference_measurement_set_preferences "
                "WHERE measurement_set_id = ?",
                (measurement_set_id,),
            ).fetchone()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        preference = cls._from_row(row)
        assert preference is not None
        return preference


# --- Observation reference uses ---------------------------------------------


class ObservationReferenceUseRepository:
    """Cross-database observation ↔ measurement-set link management."""

    _COLUMNS: tuple[str, ...] = (
        "id",
        "observation_id",
        "reference_measurement_set_id",
        "role",
        "note",
        "selected_at",
        "reference_revision",
        "snapshot_json",
        "created_at",
        "updated_at",
    )

    @staticmethod
    def _validate_role(role: str) -> str:
        return _validate_enum(role, OBSERVATION_REFERENCE_ROLES, "observation_reference_use.role")

    @staticmethod
    def _load_measurement_set_bundle(
        set_id: str,
    ) -> tuple[MeasurementSet, TaxonTreatment, ReferenceWork] | None:
        """Fetch the measurement set + parent treatment + parent work.

        Returns ``None`` if the measurement set is missing. This is a
        single-connection lookup for efficient snapshot building.
        """
        conn = _connect_reference()
        try:
            row = conn.execute(
                """
                SELECT
                    ms.*,
                    t.id AS _t_id,
                    t.reference_work_id AS _t_reference_work_id,
                    t.taxon_id AS _t_taxon_id,
                    t.name_as_published AS _t_name_as_published,
                    t.page_from AS _t_page_from,
                    t.page_to AS _t_page_to,
                    t.locator_text AS _t_locator_text,
                    t.treatment_notes AS _t_treatment_notes,
                    t.revision AS _t_revision,
                    t.created_at AS _t_created_at,
                    t.updated_at AS _t_updated_at
                FROM reference_measurement_sets AS ms
                JOIN reference_taxon_treatments AS t
                  ON t.id = ms.taxon_treatment_id
                WHERE ms.id = ?
                """,
                (set_id,),
            ).fetchone()
            if row is None:
                return None
            work_row = conn.execute(
                "SELECT * FROM reference_works WHERE id = ?",
                (row["_t_reference_work_id"],),
            ).fetchone()
        finally:
            conn.close()
        if work_row is None:
            return None

        ms_fields = {f.name for f in fields(MeasurementSet)}
        ms_data = {key: row[key] for key in row.keys() if key in ms_fields and not key.startswith("_t_")}
        measurement_set = MeasurementSet(**ms_data)
        treatment = TaxonTreatment(
            id=row["_t_id"],
            reference_work_id=row["_t_reference_work_id"],
            taxon_id=row["_t_taxon_id"],
            name_as_published=row["_t_name_as_published"],
            page_from=row["_t_page_from"],
            page_to=row["_t_page_to"],
            locator_text=row["_t_locator_text"],
            treatment_notes=row["_t_treatment_notes"],
            revision=row["_t_revision"],
            created_at=row["_t_created_at"],
            updated_at=row["_t_updated_at"],
        )
        work = _row_to_dataclass(work_row, ReferenceWork)
        return measurement_set, treatment, work

    @classmethod
    def attach_with_status(
        cls,
        observation_id: int,
        reference_measurement_set_id: str,
        *,
        role: str = "compared",
        note: str | None = None,
        allow_dangling: bool = False,
    ) -> tuple[ObservationReferenceUse, bool]:
        """Idempotent attach that also reports whether a NEW row was created.

        Returns ``(use, created)`` where ``created`` is ``True`` only when
        this call inserted a new ``observation_reference_uses`` row. When
        the same ``(observation_id, reference_measurement_set_id)`` link
        already existed, ``created`` is ``False`` and ``use`` is the
        pre-existing record. Callers that need to roll back a failed
        follow-up step must ONLY detach when ``created`` is ``True``.
        """
        use, created = cls._do_attach(
            observation_id=observation_id,
            reference_measurement_set_id=reference_measurement_set_id,
            role=role,
            note=note,
            allow_dangling=allow_dangling,
        )
        return use, created

    @classmethod
    def attach(
        cls,
        observation_id: int,
        reference_measurement_set_id: str,
        *,
        role: str = "compared",
        note: str | None = None,
        allow_dangling: bool = False,
    ) -> ObservationReferenceUse:
        """Attach a reference measurement set to an observation.

        Validates that the observation exists. Normally validates that
        the measurement-set UUID also exists in the reference library
        and stores a canonical snapshot. ``allow_dangling=True`` is a
        test/helper mode that intentionally skips the library lookup so
        integrity-detection paths can be exercised.
        """
        use, _ = cls._do_attach(
            observation_id=observation_id,
            reference_measurement_set_id=reference_measurement_set_id,
            role=role,
            note=note,
            allow_dangling=allow_dangling,
        )
        return use

    @classmethod
    def _do_attach(
        cls,
        *,
        observation_id: int,
        reference_measurement_set_id: str,
        role: str,
        note: str | None,
        allow_dangling: bool,
    ) -> tuple[ObservationReferenceUse, bool]:
        cls._validate_role(role)

        obs_conn = _connect_observations()
        try:
            obs_row = obs_conn.execute(
                "SELECT id FROM observations WHERE id = ?", (observation_id,)
            ).fetchone()
            if obs_row is None:
                raise ReferenceIntegrityError(
                    f"observation {observation_id} does not exist"
                )
        finally:
            obs_conn.close()

        snapshot_json: str
        reference_revision: int
        if allow_dangling:
            # Test/helper: build a minimal placeholder snapshot.
            snapshot_json = json.dumps(
                {
                    "schema_version": 1,
                    "reference_measurement_set_id": reference_measurement_set_id,
                    "dangling": True,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            reference_revision = 0
        else:
            bundle = cls._load_measurement_set_bundle(reference_measurement_set_id)
            if bundle is None:
                raise ReferenceIntegrityError(
                    f"reference_measurement_set {reference_measurement_set_id} does not exist"
                )
            measurement_set, treatment, work = bundle
            snapshot = build_observation_reference_snapshot(
                work, treatment, measurement_set
            )
            snapshot_json = serialize_snapshot(snapshot)
            reference_revision = measurement_set.revision

        now = _now()
        use = ObservationReferenceUse(
            id=_new_uuid(),
            observation_id=int(observation_id),
            reference_measurement_set_id=str(reference_measurement_set_id),
            role=role,
            note=note,
            selected_at=now,
            reference_revision=reference_revision,
            snapshot_json=snapshot_json,
            created_at=now,
            updated_at=now,
        )

        conn = _connect_observations()
        try:
            existing = conn.execute(
                """
                SELECT * FROM observation_reference_uses
                WHERE observation_id = ? AND reference_measurement_set_id = ?
                """,
                (observation_id, reference_measurement_set_id),
            ).fetchone()
            if existing is not None:
                return _row_to_dataclass(existing, ObservationReferenceUse), False
            values = tuple(getattr(use, name) for name in cls._COLUMNS)
            placeholders = ", ".join("?" for _ in cls._COLUMNS)
            try:
                conn.execute(
                    f"INSERT INTO observation_reference_uses "
                    f"({', '.join(cls._COLUMNS)}) VALUES ({placeholders})",
                    values,
                )
                conn.commit()
            except sqlite3.IntegrityError:
                # Two callers observed no existing row and one lost the
                # unique-index race. Fall back to reading the winner's row
                # and returning it as an already-existing use so callers
                # never see a raw sqlite exception and rollback logic is
                # not accidentally invoked for a row we did not create.
                existing = conn.execute(
                    """
                    SELECT * FROM observation_reference_uses
                    WHERE observation_id = ? AND reference_measurement_set_id = ?
                    """,
                    (observation_id, reference_measurement_set_id),
                ).fetchone()
                if existing is not None:
                    return (
                        _row_to_dataclass(existing, ObservationReferenceUse),
                        False,
                    )
                # Genuinely unable to insert AND unable to observe the row
                # -> surface as a domain error rather than raw sqlite.
                raise ReferenceIntegrityError(
                    "attach lost the unique-index race and the winning "
                    "row could not be read back"
                )
        finally:
            conn.close()
        return use, True

    @staticmethod
    def list_for_observation(observation_id: int) -> list[ObservationReferenceUse]:
        conn = _connect_observations()
        try:
            rows = conn.execute(
                """
                SELECT * FROM observation_reference_uses
                WHERE observation_id = ?
                ORDER BY selected_at, id
                """,
                (observation_id,),
            ).fetchall()
        finally:
            conn.close()
        return [_row_to_dataclass(row, ObservationReferenceUse) for row in rows]

    @staticmethod
    def get(use_id: str) -> ObservationReferenceUse | None:
        conn = _connect_observations()
        try:
            row = conn.execute(
                "SELECT * FROM observation_reference_uses WHERE id = ?",
                (use_id,),
            ).fetchone()
        finally:
            conn.close()
        return _row_to_dataclass(row, ObservationReferenceUse)

    @classmethod
    def snapshot_status(
        cls, use_id: str
    ) -> ObservationReferenceSnapshotStatus:
        use = cls.get(use_id)
        if use is None:
            raise ReferenceIntegrityError(
                f"observation_reference_use {use_id} not found"
            )
        bundle = cls._load_measurement_set_bundle(
            use.reference_measurement_set_id
        )
        if bundle is None:
            return ObservationReferenceSnapshotStatus(
                use_id=use.id,
                state="source_missing",
            )
        measurement_set, treatment, work = bundle
        current_snapshot = build_observation_reference_snapshot(
            work, treatment, measurement_set
        )
        state = (
            "current"
            if observation_snapshots_semantically_equal(
                use.snapshot_json, current_snapshot
            )
            else "update_available"
        )
        return ObservationReferenceSnapshotStatus(
            use_id=use.id,
            state=state,
            current_reference_revision=measurement_set.revision,
        )

    @classmethod
    def successor_status(
        cls, use_id: str
    ) -> MeasurementSetSuccessorResolution:
        use = cls.get(use_id)
        if use is None:
            raise ReferenceIntegrityError(
                f"observation_reference_use {use_id} not found"
            )
        resolution = MeasurementSetRepository.resolve_terminal_successor(
            use.reference_measurement_set_id
        )
        if resolution.state != "successor_available":
            return resolution
        # Every link in the selected lineage must still resolve through its
        # treatment and work. Skipping malformed intermediate records would
        # make a broken history appear safe merely because its terminal node
        # happens to be complete.
        for path_id in resolution.path_ids:
            if cls._load_measurement_set_bundle(path_id) is None:
                return MeasurementSetSuccessorResolution(
                    source_id=resolution.source_id,
                    state="broken",
                    path_ids=resolution.path_ids,
                )
        successor_id = resolution.successor_id
        bundle = (
            cls._load_measurement_set_bundle(successor_id)
            if successor_id is not None
            else None
        )
        if bundle is None:
            return MeasurementSetSuccessorResolution(
                source_id=resolution.source_id,
                state="broken",
                path_ids=resolution.path_ids,
            )
        measurement_set, treatment, work = bundle
        snapshot = build_observation_reference_snapshot(
            work, treatment, measurement_set
        )
        # Adoption must not replace a working historical plot with a source
        # the current desktop cannot render. Import locally to keep the
        # repository's normal CRUD path independent of plotting concerns.
        from references.reference_plotting import translate_observation_reference_use

        preview = translate_observation_reference_use(
            {
                "id": use.id,
                "role": use.role,
                "reference_revision": measurement_set.revision,
                "snapshot": snapshot,
            }
        )
        if preview is None:
            return MeasurementSetSuccessorResolution(
                source_id=resolution.source_id,
                state="unsupported",
                path_ids=resolution.path_ids,
            )
        return MeasurementSetSuccessorResolution(
            source_id=resolution.source_id,
            state=resolution.state,
            path_ids=resolution.path_ids,
            successor_id=successor_id,
            successor_snapshot_json=serialize_snapshot(snapshot),
        )

    @classmethod
    def adopt_successor(
        cls,
        use_id: str,
        *,
        expected_successor_id: str | None = None,
        expected_successor_snapshot_json: str | None = None,
    ) -> ObservationReferenceUse:
        """Explicitly retarget an observation use to its terminal successor."""
        use = cls.get(use_id)
        if use is None:
            raise ReferenceIntegrityError(
                f"observation_reference_use {use_id} not found"
            )
        status = cls.successor_status(use_id)
        successor_id = status.successor_id
        if status.state != "successor_available" or not successor_id:
            raise ReferenceIntegrityError(
                f"successor lineage is not adoptable ({status.state})"
            )
        if expected_successor_id and successor_id != expected_successor_id:
            raise ReferenceIntegrityError(
                "successor lineage changed before adoption; review it again"
            )
        if not status.successor_snapshot_json:
            raise ReferenceIntegrityError("successor snapshot is unavailable")
        if expected_successor_snapshot_json is not None:
            try:
                current_snapshot = json.loads(status.successor_snapshot_json)
            except (TypeError, ValueError, json.JSONDecodeError):
                current_snapshot = None
            if not isinstance(current_snapshot, dict) or not (
                observation_snapshots_semantically_equal(
                    expected_successor_snapshot_json, current_snapshot
                )
            ):
                raise ReferenceIntegrityError(
                    "successor content changed since review; review it again"
                )
        successor = MeasurementSetRepository.get(successor_id)
        if successor is None:
            raise ReferenceIntegrityError(
                "successor source disappeared before adoption"
            )

        conn = _connect_observations()
        try:
            duplicate = conn.execute(
                """
                SELECT id FROM observation_reference_uses
                WHERE observation_id = ?
                  AND reference_measurement_set_id = ?
                  AND id != ?
                LIMIT 1
                """,
                (use.observation_id, successor_id, use.id),
            ).fetchone()
            if duplicate is not None:
                raise ReferenceIntegrityError(
                    "successor measurement set is already attached to this observation"
                )
            cursor = conn.execute(
                """
                UPDATE observation_reference_uses
                SET reference_measurement_set_id = ?,
                    reference_revision = ?,
                    snapshot_json = ?,
                    updated_at = ?
                WHERE id = ? AND reference_measurement_set_id = ?
                """,
                (
                    successor_id,
                    successor.revision,
                    status.successor_snapshot_json,
                    _now(),
                    use.id,
                    use.reference_measurement_set_id,
                ),
            )
            if cursor.rowcount != 1:
                raise ReferenceIntegrityError(
                    "attachment changed before successor adoption; review it again"
                )
            record_use_mutation_intent(conn, use.id)
            conn.commit()
        except sqlite3.IntegrityError as exc:
            conn.rollback()
            raise ReferenceIntegrityError(str(exc)) from exc
        finally:
            conn.close()
        adopted = cls.get(use.id)
        if adopted is None:
            raise ReferenceIntegrityError(
                f"observation_reference_use {use.id} disappeared during adoption"
            )
        return adopted

    @classmethod
    def refresh_snapshot(
        cls, use_id: str
    ) -> tuple[ObservationReferenceUse, bool]:
        """Explicitly replace a frozen snapshot with current canonical data.

        Identity, association, role, note and selection time remain unchanged.
        Semantically identical source saves are a true no-op.
        """
        use = cls.get(use_id)
        if use is None:
            raise ReferenceIntegrityError(
                f"observation_reference_use {use_id} not found"
            )
        bundle = cls._load_measurement_set_bundle(
            use.reference_measurement_set_id
        )
        if bundle is None:
            raise ReferenceIntegrityError(
                "reference library source is unavailable; the historical "
                "snapshot was preserved"
            )
        measurement_set, treatment, work = bundle
        current_snapshot = build_observation_reference_snapshot(
            work, treatment, measurement_set
        )
        if observation_snapshots_semantically_equal(
            use.snapshot_json, current_snapshot
        ):
            return use, False

        snapshot_json = serialize_snapshot(current_snapshot)
        conn = _connect_observations()
        try:
            conn.execute(
                """
                UPDATE observation_reference_uses
                SET reference_revision = ?, snapshot_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (measurement_set.revision, snapshot_json, _now(), use.id),
            )
            record_use_mutation_intent(conn, use.id)
            conn.commit()
        finally:
            conn.close()
        refreshed = cls.get(use.id)
        if refreshed is None:
            raise ReferenceIntegrityError(
                f"observation_reference_use {use.id} disappeared during update"
            )
        return refreshed, True

    @staticmethod
    def count_uses(reference_measurement_set_id: str) -> int:
        conn = _connect_observations()
        try:
            row = conn.execute(
                """
                SELECT COUNT(*) FROM observation_reference_uses
                WHERE reference_measurement_set_id = ?
                """,
                (reference_measurement_set_id,),
            ).fetchone()
        finally:
            conn.close()
        return int(row[0] or 0)

    @classmethod
    def update(
        cls,
        use_id: str,
        *,
        role: str | None = None,
        note: str | None = ...,
    ) -> ObservationReferenceUse:
        """Update role and/or note. Snapshot is not modified here."""
        conn = _connect_observations()
        try:
            existing = conn.execute(
                "SELECT * FROM observation_reference_uses WHERE id = ?",
                (use_id,),
            ).fetchone()
            if existing is None:
                raise ReferenceIntegrityError(
                    f"observation_reference_use {use_id} not found"
                )
            new_role = existing["role"]
            new_note = existing["note"]
            if role is not None:
                cls._validate_role(role)
                new_role = role
            if note is not ...:
                new_note = note
            conn.execute(
                """
                UPDATE observation_reference_uses
                SET role = ?, note = ?, updated_at = ?
                WHERE id = ?
                """,
                (new_role, new_note, _now(), use_id),
            )
            record_use_mutation_intent(conn, use_id)
            conn.commit()
            row = conn.execute(
                "SELECT * FROM observation_reference_uses WHERE id = ?",
                (use_id,),
            ).fetchone()
        finally:
            conn.close()
        return _row_to_dataclass(row, ObservationReferenceUse)

    @staticmethod
    def detach(use_id: str) -> None:
        conn = _connect_observations()
        try:
            cursor = conn.execute(
                "DELETE FROM observation_reference_uses WHERE id = ?",
                (use_id,),
            )
            if cursor.rowcount == 0:
                raise ReferenceIntegrityError(
                    f"observation_reference_use {use_id} not found"
                )
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def find_dangling_measurement_set_ids() -> list[str]:
        """Return distinct measurement-set UUIDs referenced by uses but missing
        from the reference library (cross-database integrity check)."""
        obs_conn = _connect_observations()
        try:
            rows = obs_conn.execute(
                """
                SELECT DISTINCT reference_measurement_set_id
                FROM observation_reference_uses
                """
            ).fetchall()
        finally:
            obs_conn.close()
        set_ids = [row[0] for row in rows if row and row[0]]
        if not set_ids:
            return []
        ref_conn = _connect_reference()
        try:
            existing_ids: set[str] = set()
            placeholders = ",".join("?" for _ in set_ids)
            rows = ref_conn.execute(
                f"SELECT id FROM reference_measurement_sets WHERE id IN ({placeholders})",
                set_ids,
            ).fetchall()
            existing_ids = {row[0] for row in rows}
        finally:
            ref_conn.close()
        return [sid for sid in set_ids if sid not in existing_ids]


class QuickAddReferenceService:
    """Create/reuse the minimum normalized hierarchy and attach it.

    The reference library and observation uses are separate SQLite files, so
    one database transaction cannot cover the complete operation.  This
    service validates before writing and, on failure, compensates in reverse
    order, deleting only rows whose creation it recorded itself.  Existing
    work and treatment rows are therefore never rollback targets.
    """

    @staticmethod
    def _normalized_locator(value: str | None) -> str | None:
        normalized = str(value or "").strip()
        return normalized or None

    @classmethod
    def _resolve_work(
        cls, request: QuickAddReferenceRequest
    ) -> tuple[ReferenceWork, bool]:
        proposed = ReferenceWork(**{**asdict(request.work), "id": ""})
        proposed.doi = normalize_doi(proposed.doi)
        proposed.isbn = normalize_isbn(proposed.isbn)
        ReferenceWorkRepository._validate(proposed)

        explicit: ReferenceWork | None = None
        if request.existing_work_id:
            explicit = ReferenceWorkRepository.get(request.existing_work_id)
            if explicit is None:
                raise ReferenceIntegrityError(
                    f"reference_work {request.existing_work_id} not found"
                )

        doi_match = (
            ReferenceWorkRepository.find_by_doi(proposed.doi)
            if proposed.doi
            else None
        )
        isbn_match = (
            ReferenceWorkRepository.find_by_isbn(proposed.isbn)
            if proposed.isbn
            else None
        )
        identifier_matches = [match for match in (doi_match, isbn_match) if match]
        matched_ids = {match.id for match in identifier_matches}
        if len(matched_ids) > 1:
            raise ReferenceIntegrityError(
                "the supplied DOI and ISBN identify different works"
            )
        if explicit is not None and matched_ids and explicit.id not in matched_ids:
            raise ReferenceIntegrityError(
                "the supplied identifier conflicts with the selected work"
            )

        selected = explicit or (identifier_matches[0] if identifier_matches else None)
        if selected is not None:
            if (
                proposed.doi
                and selected.doi
                and normalize_doi(selected.doi) != proposed.doi
            ):
                raise ReferenceIntegrityError(
                    "the supplied DOI conflicts with the selected work"
                )
            if (
                proposed.isbn
                and selected.isbn
                and normalize_isbn(selected.isbn) != proposed.isbn
            ):
                raise ReferenceIntegrityError(
                    "the supplied ISBN conflicts with the selected work"
                )
            return selected, False

        # Deliberately do not match by title, author, or other fuzzy metadata.
        return ReferenceWorkRepository.create(proposed), True

    @classmethod
    def _resolve_treatment(
        cls,
        work: ReferenceWork,
        proposed: TaxonTreatment,
    ) -> tuple[TaxonTreatment, bool]:
        name = str(proposed.name_as_published or "").strip()
        locator = cls._normalized_locator(proposed.locator_text)
        taxon_id = proposed.taxon_id
        for existing in TaxonTreatmentRepository.list_for_work(work.id):
            if (
                str(existing.name_as_published or "").strip().casefold()
                == name.casefold()
                and existing.taxon_id == taxon_id
                and cls._normalized_locator(existing.locator_text) == locator
            ):
                return existing, False

        treatment = TaxonTreatment(
            **{
                **asdict(proposed),
                "id": "",
                "reference_work_id": work.id,
                "name_as_published": name,
                "locator_text": locator,
            }
        )
        TaxonTreatmentRepository._validate(treatment)
        return TaxonTreatmentRepository.create(treatment), True

    @classmethod
    def create_and_attach(
        cls, request: QuickAddReferenceRequest
    ) -> QuickAddReferenceResult:
        """Persist a quick-add operation, compensating partial writes."""
        ObservationReferenceUseRepository._validate_role(request.role)

        # Validate domain/editor output before creating any hierarchy rows.
        proposed_set = MeasurementSet(
            **{
                **asdict(request.measurement_set),
                "id": "",
                "taxon_treatment_id": "pending",
                "supersedes_id": None,
                "revision": 1,
                "created_at": None,
                "updated_at": None,
            }
        )
        MeasurementSetRepository._validate(proposed_set)

        work: ReferenceWork | None = None
        treatment: TaxonTreatment | None = None
        measurement_set: MeasurementSet | None = None
        use: ObservationReferenceUse | None = None
        created_work = False
        created_treatment = False
        created_attachment = False
        try:
            work, created_work = cls._resolve_work(request)
            treatment, created_treatment = cls._resolve_treatment(
                work, request.treatment
            )
            proposed_set.taxon_treatment_id = treatment.id
            measurement_set = MeasurementSetRepository.create(proposed_set)
            use, created_attachment = ObservationReferenceUseRepository.attach_with_status(
                request.observation_id,
                measurement_set.id,
                role=request.role,
                note=request.note,
            )
            return QuickAddReferenceResult(
                work=work,
                treatment=treatment,
                measurement_set=measurement_set,
                use=use,
                created_work=created_work,
                created_treatment=created_treatment,
                created_measurement_set=True,
                created_attachment=created_attachment,
            )
        except Exception:
            # Reverse-order compensation across the two database files.  The
            # attachment is normally the final operation, but track it so a
            # future post-attach step cannot strand a use.
            if use is not None and created_attachment:
                ObservationReferenceUseRepository.detach(use.id)
            if measurement_set is not None:
                MeasurementSetRepository.delete(measurement_set.id)
            if treatment is not None and created_treatment:
                TaxonTreatmentRepository.delete(treatment.id)
            if work is not None and created_work:
                ReferenceWorkRepository.delete(work.id)
            raise
