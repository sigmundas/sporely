#!/usr/bin/env python3
"""Phase C — apply the human-authored migration manifest.

The manifest (produced by :mod:`tools.audit_legacy_reference_values` and
edited by hand) is the single source of truth for what happens to every
legacy ``reference_values`` row. This script:

- validates the entire manifest before writing anything;
- refuses to run ``--apply`` while any ``unresolved`` entry exists;
- uses the existing normalized repositories (UUID / revision semantics
  come from them, unchanged);
- populates ``legacy_reference_value_id`` on every created measurement
  set so a rerun is idempotent;
- preserves nulls as nulls, never synthesizes bibliography or numbers;
- reports unsupported legacy fields via the manifest's ``_unsupported``
  block (also echoed to stdout) instead of silently dropping them;
- runs ``--dry-run`` by default. Actual mutation requires the explicit
  ``--apply`` flag AND an explicit ``--confirm-backup`` acknowledgement.

The source legacy row is never modified or deleted. Cloud sync,
Supabase, landing-page work and AI parsing are all out of scope.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.reference_library import (  # noqa: E402
    MeasurementSet,
    MeasurementSetRepository,
    ReferenceLibraryError,
    ReferenceValidationError,
    ReferenceWork,
    ReferenceWorkRepository,
    TaxonTreatment,
    TaxonTreatmentRepository,
)
from database.reference_library_schema import (  # noqa: E402
    REFERENCE_WORK_TYPES,
    init_reference_library_schema,
)
from database.schema import (  # noqa: E402
    get_reference_connection,
    get_reference_database_path,
)


SUPPORTED_MANIFEST_VERSION = 1
_ACTIONS: frozenset[str] = frozenset(
    {"migrate", "attach_to_existing", "skip", "unresolved", "already_migrated"}
)


class ManifestError(Exception):
    """Raised when the manifest cannot be safely applied."""


# ---------------------------------------------------------------------------
# Manifest validation
# ---------------------------------------------------------------------------


@dataclass
class ValidatedManifest:
    version: int
    database_path: str | None
    defaults: dict[str, Any]
    works_by_key: dict[str, dict[str, Any]]
    rows: list[dict[str, Any]]


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ManifestError(message)


def _validate_work(work: dict[str, Any], index: int) -> None:
    _require(isinstance(work, dict), f"works[{index}] must be an object")
    key = work.get("work_key")
    _require(
        isinstance(key, str) and key.strip(),
        f"works[{index}].work_key is required (non-empty string)",
    )
    wtype = work.get("type")
    _require(
        isinstance(wtype, str) and wtype in REFERENCE_WORK_TYPES,
        f"works[{index}].type must be one of {sorted(REFERENCE_WORK_TYPES)}",
    )
    title = work.get("title")
    _require(
        isinstance(title, str) and title.strip(),
        f"works[{index}].title is required",
    )
    # ``verification_status`` and ``visibility`` are no longer part of the
    # product model. If a legacy manifest still contains them we simply
    # ignore the value rather than fail — an operator porting an older
    # manifest should not be blocked by a concept that has been removed.
    for list_key in ("authors", "editors"):
        entries = work.get(list_key, [])
        _require(
            isinstance(entries, list),
            f"works[{index}].{list_key} must be a JSON list (may be empty)",
        )
        for i, entry in enumerate(entries):
            _require(
                isinstance(entry, dict),
                f"works[{index}].{list_key}[{i}] must be an object with "
                "family/given/literal keys",
            )
    year = work.get("year")
    if year is not None:
        _require(
            isinstance(year, int) and not isinstance(year, bool),
            f"works[{index}].year must be an integer or null",
        )


def validate_manifest(raw: dict[str, Any]) -> ValidatedManifest:
    _require(isinstance(raw, dict), "manifest must be a JSON object")
    version = raw.get("manifest_version")
    _require(
        isinstance(version, int) and version == SUPPORTED_MANIFEST_VERSION,
        f"manifest_version must be {SUPPORTED_MANIFEST_VERSION}",
    )
    works = raw.get("works", [])
    _require(isinstance(works, list), "works must be a list")
    works_by_key: dict[str, dict[str, Any]] = {}
    for i, work in enumerate(works):
        _validate_work(work, i)
        key = work["work_key"]
        if key in works_by_key:
            raise ManifestError(
                f"duplicate work_key {key!r} in manifest works section"
            )
        works_by_key[key] = work

    rows = raw.get("rows")
    _require(isinstance(rows, list), "rows must be a list")
    legacy_ids: set[int] = set()
    for i, entry in enumerate(rows):
        _require(isinstance(entry, dict), f"rows[{i}] must be an object")
        legacy_id = entry.get("legacy_id")
        _require(
            isinstance(legacy_id, int) and not isinstance(legacy_id, bool),
            f"rows[{i}].legacy_id must be an integer",
        )
        if legacy_id in legacy_ids:
            raise ManifestError(
                f"rows[{i}].legacy_id={legacy_id} is duplicated in the manifest"
            )
        legacy_ids.add(int(legacy_id))
        action = entry.get("action")
        _require(
            action in _ACTIONS,
            f"rows[{i}].action must be one of {sorted(_ACTIONS)}",
        )
        if action == "migrate":
            work_key = entry.get("work_key")
            _require(
                isinstance(work_key, str) and work_key.strip(),
                f"rows[{i}].work_key is required when action is 'migrate'",
            )
            _require(
                work_key in works_by_key,
                f"rows[{i}].work_key={work_key!r} not defined in "
                "the top-level 'works' array",
            )
            treatment = entry.get("treatment") or {}
            _require(
                isinstance(treatment, dict),
                f"rows[{i}].treatment must be an object",
            )
            name = treatment.get("name_as_published")
            _require(
                isinstance(name, str) and name.strip(),
                f"rows[{i}].treatment.name_as_published is required",
            )
            ms = entry.get("measurement_set") or {}
            _require(
                isinstance(ms, dict),
                f"rows[{i}].measurement_set must be an object",
            )
        elif action == "attach_to_existing":
            _require(
                isinstance(entry.get("existing_measurement_set_id"), str)
                and entry["existing_measurement_set_id"].strip(),
                f"rows[{i}].existing_measurement_set_id is required "
                "for action 'attach_to_existing'",
            )

    return ValidatedManifest(
        version=version,
        database_path=(
            raw.get("database_path") if isinstance(raw.get("database_path"), str) else None
        ),
        defaults=raw.get("defaults") or {},
        works_by_key=works_by_key,
        rows=rows,
    )


# ---------------------------------------------------------------------------
# Legacy row fetch
# ---------------------------------------------------------------------------


def _fetch_legacy_row(
    conn: sqlite3.Connection, legacy_id: int
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM reference_values WHERE id = ?", (int(legacy_id),)
    ).fetchone()


def _row_stat(row: sqlite3.Row, key: str) -> Any:
    try:
        return row[key]
    except (KeyError, IndexError):
        return None


def _row_columns(conn: sqlite3.Connection) -> set[str]:
    cursor = conn.execute("PRAGMA table_info(reference_values)")
    return {r[1] for r in cursor.fetchall()}


def _unsupported_legacy_notes(row: sqlite3.Row, columns: set[str]) -> list[str]:
    """Return a report list of legacy fields not carried into the normalized
    model. Never silently dropped — included in the migration report."""
    notes: list[str] = []
    for col in (
        "parmasto_length_mean",
        "parmasto_width_mean",
        "parmasto_q_mean",
        "parmasto_v_sp_length",
        "parmasto_v_sp_width",
        "parmasto_v_sp_q",
        "parmasto_v_ind_length",
        "parmasto_v_ind_width",
        "parmasto_v_ind_q",
    ):
        if col in columns and _row_stat(row, col) is not None:
            notes.append(f"{col}={_row_stat(row, col)}")
    if "plot_color" in columns and _row_stat(row, "plot_color"):
        notes.append(f"plot_color={_row_stat(row, 'plot_color')}")
    if "metadata_json" in columns:
        raw = _row_stat(row, "metadata_json")
        if raw:
            notes.append(f"metadata_json_raw={raw!r}")
    return notes


def _compose_notes(
    manifest_notes: str | None,
    legacy_row: sqlite3.Row,
    columns: set[str],
) -> str | None:
    """Combine any explicit manifest notes with the preserved provenance
    dump for the legacy ``source`` text plus parmasto / plot_color /
    metadata_json — the operator's chosen normalized work replaces
    ``source`` as the bibliographic anchor, but the original free-text
    label is kept verbatim under the measurement set's ``notes`` field
    so a later curator can audit which legacy row this came from."""
    unsupported = _unsupported_legacy_notes(legacy_row, columns)
    pieces: list[str] = []
    if manifest_notes:
        pieces.append(str(manifest_notes).strip())
    source_text = ""
    if "source" in columns:
        source_text = str(_row_stat(legacy_row, "source") or "").strip()
    if source_text:
        pieces.append(
            f"[legacy-migration] original source: {source_text}"
        )
    if unsupported:
        pieces.append(
            "[legacy-migration] preserved from reference_values: "
            + ", ".join(unsupported)
        )
    if not pieces:
        return None
    return "\n\n".join(pieces)


# ---------------------------------------------------------------------------
# Repository writes
# ---------------------------------------------------------------------------


def _work_from_manifest_entry(
    work_entry: dict[str, Any], defaults: dict[str, Any]
) -> ReferenceWork:
    """Build a plain, local normalized ReferenceWork from a manifest entry.

    The tool creates ordinary local records — it does NOT infer approval,
    trust, or a publication scope. ``defaults`` is intentionally unused
    for the removed verification/visibility fields; callers should still
    pass any legacy manifest ``defaults`` block through unchanged so
    older manifests continue to load.
    """
    _ = defaults  # accepted for legacy manifest compatibility, otherwise unused
    authors = work_entry.get("authors") or []
    editors = work_entry.get("editors") or []
    return ReferenceWork(
        id="",
        type=work_entry["type"],
        title=work_entry["title"],
        short_label=work_entry.get("short_label") or "",
        authors_json=json.dumps(authors, ensure_ascii=False),
        editors_json=json.dumps(editors, ensure_ascii=False),
        citation_key=work_entry.get("citation_key"),
        container_title=work_entry.get("container_title"),
        year=work_entry.get("year"),
        edition=work_entry.get("edition"),
        publisher=work_entry.get("publisher"),
        place=work_entry.get("place"),
        volume=work_entry.get("volume"),
        issue=work_entry.get("issue"),
        pages=work_entry.get("pages"),
        doi=work_entry.get("doi"),
        isbn=work_entry.get("isbn"),
        url=work_entry.get("url"),
        language=work_entry.get("language"),
        citation_override=work_entry.get("citation_override"),
    )


def _find_existing_by_legacy_id(
    conn: sqlite3.Connection, legacy_id: int
) -> str | None:
    row = conn.execute(
        """
        SELECT id FROM reference_measurement_sets
        WHERE legacy_reference_value_id = ?
        LIMIT 1
        """,
        (int(legacy_id),),
    ).fetchone()
    return row["id"] if row else None


def _existing_treatment(
    conn: sqlite3.Connection, *, work_id: str, name_as_published: str
) -> str | None:
    """Deterministic reuse: within a single work, reuse a treatment whose
    ``name_as_published`` (case-sensitive, trimmed) matches exactly. The
    manifest can also override this by passing an explicit ``treatment_id``
    (not surfaced yet — currently reuse is fully driven by the taxon
    name to keep the manifest small)."""
    row = conn.execute(
        """
        SELECT id FROM reference_taxon_treatments
        WHERE reference_work_id = ? AND TRIM(name_as_published) = ?
        LIMIT 1
        """,
        (work_id, name_as_published.strip()),
    ).fetchone()
    return row["id"] if row else None


# ---------------------------------------------------------------------------
# Migration runner
# ---------------------------------------------------------------------------


@dataclass
class MigrationReport:
    created: list[dict[str, Any]] = field(default_factory=list)
    reused: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[dict[str, Any]] = field(default_factory=list)
    unresolved: list[dict[str, Any]] = field(default_factory=list)
    failed: list[dict[str, Any]] = field(default_factory=list)
    unsupported_fields: list[dict[str, Any]] = field(default_factory=list)
    dry_run: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "generated_at": datetime.now(timezone.utc).isoformat(
                timespec="seconds"
            ),
            "counts": {
                "created": len(self.created),
                "reused": len(self.reused),
                "skipped": len(self.skipped),
                "unresolved": len(self.unresolved),
                "failed": len(self.failed),
            },
            "created": self.created,
            "reused": self.reused,
            "skipped": self.skipped,
            "unresolved": self.unresolved,
            "failed": self.failed,
            "unsupported_fields": self.unsupported_fields,
        }


def _has_unresolved(manifest: ValidatedManifest) -> list[int]:
    return [
        int(r["legacy_id"])
        for r in manifest.rows
        if r["action"] == "unresolved"
    ]


def _measurement_payload_from_legacy(
    legacy_row: sqlite3.Row,
    columns: set[str],
    manifest_ms: dict[str, Any],
    *,
    treatment_id: str,
    legacy_id: int,
) -> MeasurementSet:
    """Compose a MeasurementSet payload from a legacy row + manifest hint.

    Nulls remain nulls. No synthesized means, no synthesized ranges, no
    fabricated Q values. Only fields that map cleanly to the normalized
    columns are copied; provenance for the rest is preserved in ``notes``
    by :func:`_compose_notes`.
    """
    def _pick(col: str) -> Any:
        if col in columns:
            return _row_stat(legacy_row, col)
        return None

    data_kind = manifest_ms.get("data_kind") or "summary"
    notes = _compose_notes(manifest_ms.get("notes"), legacy_row, columns)

    return MeasurementSet(
        id="",
        taxon_treatment_id=treatment_id,
        character="spore_size",
        data_kind=data_kind,
        raw_text=manifest_ms.get("raw_text"),
        length_min=_pick("length_min"),
        length_core_min=_pick("length_p05"),
        length_core_max=_pick("length_p95"),
        length_max=_pick("length_max"),
        width_min=_pick("width_min"),
        width_core_min=_pick("width_p05"),
        width_core_max=_pick("width_p95"),
        width_max=_pick("width_max"),
        q_min=_pick("q_min"),
        q_max=_pick("q_max"),
        q_mean=(
            _pick("q_avg")
            if _pick("q_avg") is not None
            else _pick("q_p50")
        ),
        length_mean=(
            _pick("length_avg")
            if _pick("length_avg") is not None
            else _pick("length_p50")
        ),
        width_mean=(
            _pick("width_avg")
            if _pick("width_avg") is not None
            else _pick("width_p50")
        ),
        sample_size=None,
        specimen_count=None,
        mount_medium=_pick("mount_medium"),
        stain=_pick("stain"),
        preparation=manifest_ms.get("preparation"),
        measurement_method=manifest_ms.get("measurement_method"),
        notes=notes,
        raw_points_json=None,
        legacy_reference_value_id=int(legacy_id),
    )


def _treatment_from_manifest(
    manifest_treatment: dict[str, Any],
    *,
    work_id: str,
) -> TaxonTreatment:
    return TaxonTreatment(
        id="",
        reference_work_id=work_id,
        name_as_published=manifest_treatment["name_as_published"].strip(),
        taxon_id=manifest_treatment.get("taxon_id"),
        page_from=manifest_treatment.get("page_from"),
        page_to=manifest_treatment.get("page_to"),
        locator_text=manifest_treatment.get("locator_text"),
        treatment_notes=manifest_treatment.get("treatment_notes"),
    )


def _apply_migrate_entry(
    entry: dict[str, Any],
    manifest: ValidatedManifest,
    *,
    legacy_conn: sqlite3.Connection,
    normalized_conn: sqlite3.Connection,
    dry_run: bool,
    report: MigrationReport,
    work_id_by_key: dict[str, str],
    columns: set[str],
) -> None:
    legacy_id = int(entry["legacy_id"])
    legacy_row = _fetch_legacy_row(legacy_conn, legacy_id)
    if legacy_row is None:
        report.failed.append(
            {
                "legacy_id": legacy_id,
                "reason": (
                    f"legacy_id {legacy_id} not found in reference_values "
                    "(manifest may be stale)"
                ),
            }
        )
        return

    # Idempotency: a measurement set already carrying this legacy_id is
    # treated as an existing successful migration. We do NOT overwrite.
    existing_set_id = _find_existing_by_legacy_id(normalized_conn, legacy_id)
    if existing_set_id is not None:
        report.reused.append(
            {
                "legacy_id": legacy_id,
                "reason": "already migrated on a prior run",
                "measurement_set_id": existing_set_id,
            }
        )
        return

    work_key = entry["work_key"]
    work_entry = manifest.works_by_key[work_key]

    # Resolve / create the ReferenceWork.
    if work_key not in work_id_by_key:
        if dry_run:
            work_id_by_key[work_key] = f"__dry_run__{work_key}"
        else:
            try:
                work = ReferenceWorkRepository.create(
                    _work_from_manifest_entry(work_entry, manifest.defaults)
                )
            except (ReferenceValidationError, ReferenceLibraryError) as exc:
                report.failed.append(
                    {
                        "legacy_id": legacy_id,
                        "reason": f"failed to create work {work_key!r}: {exc}",
                    }
                )
                return
            work_id_by_key[work_key] = work.id

    work_id = work_id_by_key[work_key]

    # Resolve / create the TaxonTreatment (reuse by name within work).
    treatment_manifest = entry.get("treatment") or {}
    treatment_id: str
    if dry_run:
        treatment_id = f"__dry_run_treatment__{legacy_id}"
    else:
        existing_treatment_id = _existing_treatment(
            normalized_conn,
            work_id=work_id,
            name_as_published=treatment_manifest["name_as_published"],
        )
        if existing_treatment_id is not None:
            treatment_id = existing_treatment_id
        else:
            try:
                treatment = TaxonTreatmentRepository.create(
                    _treatment_from_manifest(treatment_manifest, work_id=work_id)
                )
            except (ReferenceValidationError, ReferenceLibraryError) as exc:
                report.failed.append(
                    {
                        "legacy_id": legacy_id,
                        "reason": f"failed to create treatment: {exc}",
                    }
                )
                return
            treatment_id = treatment.id

    # Compose the MeasurementSet payload from legacy columns + manifest hints.
    ms_manifest = entry.get("measurement_set") or {}
    payload = _measurement_payload_from_legacy(
        legacy_row,
        columns,
        ms_manifest,
        treatment_id=treatment_id,
        legacy_id=legacy_id,
    )
    unsupported = _unsupported_legacy_notes(legacy_row, columns)
    if unsupported:
        report.unsupported_fields.append(
            {"legacy_id": legacy_id, "preserved_in_notes": unsupported}
        )

    if dry_run:
        report.created.append(
            {
                "legacy_id": legacy_id,
                "work_key": work_key,
                "would_create_measurement_set": True,
                "data_kind": payload.data_kind,
            }
        )
        return

    try:
        created = MeasurementSetRepository.create(payload)
    except (ReferenceValidationError, ReferenceLibraryError) as exc:
        report.failed.append(
            {
                "legacy_id": legacy_id,
                "reason": f"failed to create measurement set: {exc}",
            }
        )
        return
    report.created.append(
        {
            "legacy_id": legacy_id,
            "work_key": work_key,
            "work_id": work_id,
            "treatment_id": treatment_id,
            "measurement_set_id": created.id,
        }
    )


def _apply_attach_to_existing(
    entry: dict[str, Any],
    *,
    normalized_conn: sqlite3.Connection,
    dry_run: bool,
    report: MigrationReport,
) -> None:
    legacy_id = int(entry["legacy_id"])
    existing_id = entry["existing_measurement_set_id"]
    row = normalized_conn.execute(
        "SELECT id, legacy_reference_value_id FROM reference_measurement_sets "
        "WHERE id = ?",
        (existing_id,),
    ).fetchone()
    if row is None:
        report.failed.append(
            {
                "legacy_id": legacy_id,
                "reason": (
                    f"existing_measurement_set_id={existing_id!r} does "
                    "not exist in reference_measurement_sets"
                ),
            }
        )
        return
    current = row["legacy_reference_value_id"]
    if current is not None and int(current) != legacy_id:
        report.failed.append(
            {
                "legacy_id": legacy_id,
                "reason": (
                    f"measurement set {existing_id} already carries "
                    f"legacy_reference_value_id={current}; refusing to overwrite"
                ),
            }
        )
        return
    if current is not None:
        # Idempotent: already stamped on a prior run.
        report.reused.append(
            {
                "legacy_id": legacy_id,
                "measurement_set_id": existing_id,
                "reason": "attach_to_existing already applied",
            }
        )
        return
    if dry_run:
        report.reused.append(
            {
                "legacy_id": legacy_id,
                "measurement_set_id": existing_id,
                "reason": "would stamp legacy_reference_value_id on existing set",
            }
        )
        return
    try:
        MeasurementSetRepository.update(
            existing_id,
            {"legacy_reference_value_id": int(legacy_id)},
        )
    except (ReferenceValidationError, ReferenceLibraryError) as exc:
        report.failed.append(
            {
                "legacy_id": legacy_id,
                "reason": f"failed to stamp legacy id: {exc}",
            }
        )
        return
    report.reused.append(
        {
            "legacy_id": legacy_id,
            "measurement_set_id": existing_id,
            "reason": "stamped legacy_reference_value_id on existing set",
        }
    )


def run_migration(
    manifest: ValidatedManifest,
    *,
    database_path: Path,
    dry_run: bool,
) -> MigrationReport:
    report = MigrationReport(dry_run=dry_run)

    # Refuse to write while any entry remains unresolved.
    unresolved_ids = _has_unresolved(manifest)
    if unresolved_ids:
        for uid in unresolved_ids:
            report.unresolved.append({"legacy_id": uid})
        if not dry_run:
            raise ManifestError(
                f"refusing to --apply while {len(unresolved_ids)} manifest "
                f"entries are still 'unresolved': {unresolved_ids[:10]}"
            )

    legacy_conn = sqlite3.connect(database_path)
    legacy_conn.row_factory = sqlite3.Row
    normalized_conn = sqlite3.connect(database_path)
    normalized_conn.row_factory = sqlite3.Row
    try:
        init_reference_library_schema(normalized_conn)
        columns = _row_columns(legacy_conn)
        work_id_by_key: dict[str, str] = {}

        # Pre-resolve existing works by matching an incoming work_key
        # against a stored ``citation_key`` — the operator can pin a
        # normalized work to an already-existing library entry by setting
        # ``citation_key`` on the manifest work entry to the same value.
        if not dry_run:
            for key, work in manifest.works_by_key.items():
                cite = work.get("citation_key")
                if not cite:
                    continue
                row = normalized_conn.execute(
                    "SELECT id FROM reference_works WHERE citation_key = ? LIMIT 1",
                    (str(cite),),
                ).fetchone()
                if row is not None:
                    work_id_by_key[key] = row["id"]

        for entry in manifest.rows:
            action = entry["action"]
            legacy_id = int(entry["legacy_id"])
            try:
                if action == "already_migrated":
                    existing = _find_existing_by_legacy_id(
                        normalized_conn, legacy_id
                    )
                    if existing is None:
                        report.failed.append(
                            {
                                "legacy_id": legacy_id,
                                "reason": (
                                    "action=already_migrated but no "
                                    "measurement set carries this legacy id"
                                ),
                            }
                        )
                    else:
                        report.reused.append(
                            {
                                "legacy_id": legacy_id,
                                "measurement_set_id": existing,
                                "reason": "already_migrated",
                            }
                        )
                elif action == "skip":
                    report.skipped.append(
                        {
                            "legacy_id": legacy_id,
                            "reason": entry.get("reason") or "skipped by manifest",
                        }
                    )
                elif action == "unresolved":
                    # In dry-run mode we surface but do not fail. In apply
                    # mode we already raised above.
                    continue
                elif action == "migrate":
                    _apply_migrate_entry(
                        entry,
                        manifest,
                        legacy_conn=legacy_conn,
                        normalized_conn=normalized_conn,
                        dry_run=dry_run,
                        report=report,
                        work_id_by_key=work_id_by_key,
                        columns=columns,
                    )
                elif action == "attach_to_existing":
                    _apply_attach_to_existing(
                        entry,
                        normalized_conn=normalized_conn,
                        dry_run=dry_run,
                        report=report,
                    )
            except Exception as exc:  # noqa: BLE001 — surfaces to report
                report.failed.append(
                    {
                        "legacy_id": legacy_id,
                        "reason": f"unexpected error: {exc!r}",
                    }
                )
    finally:
        legacy_conn.close()
        normalized_conn.close()
    return report


# ---------------------------------------------------------------------------
# Interactive migration engine
# ---------------------------------------------------------------------------
#
# The interactive walkthrough replaces hand-edited JSON as the normal
# migration path. Legacy rows are grouped by an EXACT normalized source
# string — no fuzzy merging. The operator explicitly assigns each group
# to a normalized ReferenceWork they either created via the desktop
# Reference Library UI or already had. Progress is persisted to a local
# ignored state directory so quitting and resuming later is safe.
#
# The engine is deliberately split into an I/O-free session class
# (:class:`InteractiveMigrationSession`) and a thin stdin/stdout driver
# (:func:`interactive_loop`) so tests can exercise every decision path
# without stubbing terminal I/O.


INTERACTIVE_STATE_VERSION = 1


def _normalize_source_key(value: Any) -> str:
    """Exact-string grouping key. Never fuzzy-merges labels."""
    return str(value or "").strip().lower()


# --- Personal / Sporely-generated source classifier -------------------------
#
# Some legacy ``reference_values`` rows carry sources that are NOT
# literature citations — they were persisted by the Sporely desktop app
# itself when the operator's own cloud measurements landed in the same
# table. These rows must never be migrated into normalized ReferenceWork
# / TaxonTreatment / MeasurementSet literature records, and they must
# not clutter the operator's interactive walkthrough as groups to skip.
#
# The classifier is intentionally narrow and conservative:
#
# - matches an EXPLICIT known prefix (currently ``Cloud:`` only);
# - never inspects the row's numeric contents;
# - never matches merely because a source contains an email, a date,
#   or the word "cloud" somewhere inside a longer citation.
#
# Extend this tuple ONLY when a new Sporely-internal source-format
# convention is confirmed. When in doubt, leave a row classified as
# literature — the operator can still mark it skipped or unresolved.
_PERSONAL_SOURCE_PREFIXES: tuple[str, ...] = ("cloud:",)


def _is_personal_source(source: Any) -> bool:
    text = str(source or "").strip().lower()
    if not text:
        return False
    for prefix in _PERSONAL_SOURCE_PREFIXES:
        if text.startswith(prefix):
            return True
    return False


def _display_source_label(source: str) -> str:
    """Return the human-facing source label without embedded PII.

    For personal/cloud sources the raw legacy label often carries the
    operator's own email + a timestamp. That string must NEVER surface
    in normal CLI output — the CLI treats these groups as excluded and
    only displays a redacted label if it prints them at all.
    """
    if _is_personal_source(source):
        return "Sporely personal measurement (redacted)"
    return str(source or "")


# --- Terminal styling -------------------------------------------------------


class _Style:
    """Small ANSI style helper.

    Enabled only when stdout is a TTY and ``NO_COLOR`` is unset. When
    disabled, every method returns the text unchanged so tests, piped
    output, and any state/report file we might write remain 100% plain.
    ANSI codes are NEVER stored anywhere — only used on the terminal.
    """

    _CODES = {
        "bold": "1",
        "dim": "2",
        "red": "31",
        "green": "32",
        "yellow": "33",
        "cyan": "36",
        "bold_cyan": "1;36",
        "bold_yellow": "1;33",
    }

    def __init__(self, enabled: bool | None = None) -> None:
        if enabled is None:
            enabled = self._detect_enabled()
        self.enabled = bool(enabled)

    @staticmethod
    def _detect_enabled() -> bool:
        import os
        if os.environ.get("NO_COLOR"):
            return False
        try:
            return sys.stdout.isatty()
        except (AttributeError, ValueError):
            return False

    def _wrap(self, code: str, text: str) -> str:
        if not self.enabled or not text:
            return text
        return f"\x1b[{code}m{text}\x1b[0m"

    def bold(self, s: str) -> str: return self._wrap(self._CODES["bold"], s)
    def dim(self, s: str) -> str: return self._wrap(self._CODES["dim"], s)
    def red(self, s: str) -> str: return self._wrap(self._CODES["red"], s)
    def green(self, s: str) -> str: return self._wrap(self._CODES["green"], s)
    def yellow(self, s: str) -> str: return self._wrap(self._CODES["yellow"], s)
    def cyan(self, s: str) -> str: return self._wrap(self._CODES["cyan"], s)
    def bold_cyan(self, s: str) -> str: return self._wrap(self._CODES["bold_cyan"], s)
    def bold_yellow(self, s: str) -> str: return self._wrap(self._CODES["bold_yellow"], s)


@dataclass
class LegacyRowSummary:
    """Read-only projection of one legacy row for interactive display."""

    legacy_id: int
    genus: str
    species: str
    source: str
    suggested_data_kind: str
    plotable: bool
    already_migrated: bool
    existing_measurement_set_id: str | None
    unmapped_fields: list[str] = field(default_factory=list)

    @property
    def taxon_label(self) -> str:
        parts = [p for p in (self.genus, self.species) if p]
        return " ".join(parts) or f"(row {self.legacy_id})"


@dataclass
class SourceGroup:
    """One exact-source-string grouping of legacy rows."""

    source: str  # original text, verbatim
    source_key: str  # normalized lookup key
    rows: list[LegacyRowSummary] = field(default_factory=list)

    @property
    def unmigrated_rows(self) -> list[LegacyRowSummary]:
        return [r for r in self.rows if not r.already_migrated]

    @property
    def migrated_rows(self) -> list[LegacyRowSummary]:
        return [r for r in self.rows if r.already_migrated]


@dataclass
class WorkCandidate:
    """Human-readable projection of a :class:`ReferenceWork` for the picker."""

    work_id: str
    short_label: str
    title: str
    year: int | None
    authors_summary: str

    def display(self, index: int, style: "_Style | None" = None) -> list[str]:
        """Return the two lines the picker prints for this candidate.

        Format:

            1. Læssøe et al. 2024
               Danmarks basidiesvampe

        Year is dropped when it is already present in ``short_label`` so
        the picker does not show "Læssøe et al. 2024 (2024)". Author list
        is intentionally omitted from the visible summary — a modern
        ``short_label`` already carries the author cue. The UUID never
        appears on a normal candidate line; call :meth:`display_debug`
        to include it for troubleshooting.
        """
        st = style or _Style(enabled=False)
        short = self.short_label
        year_suffix = ""
        if self.year is not None and str(self.year) not in short:
            year_suffix = f" ({self.year})"
        header = f"  {index}. " + st.bold(f"{short}{year_suffix}")
        subtitle = (
            f"     {self.title}" if self.title and self.title != short else ""
        )
        lines = [header]
        if subtitle:
            lines.append(subtitle)
        return lines

    def display_debug(self, index: int) -> str:
        """UUID-carrying variant for troubleshooting/support only."""
        year = f" ({self.year})" if self.year is not None else ""
        return (
            f"  {index}. {self.short_label}{year} — {self.title} "
            f"[uuid={self.work_id}]"
        )


@dataclass
class InteractiveState:
    """Session state persisted between interactive runs.

    The DB is always the authoritative source for "already migrated" —
    ``migrated_legacy_ids`` here is a cache/echo for the summary view
    and does not gate anything. Source-level decisions the operator has
    made (bind, skip, unresolved) are stored so a restart never asks the
    same question twice.
    """

    version: int = INTERACTIVE_STATE_VERSION
    source_bindings: dict[str, str] = field(default_factory=dict)
    skipped_sources: list[str] = field(default_factory=list)
    unresolved_sources: list[str] = field(default_factory=list)
    deselected_rows: dict[str, list[int]] = field(default_factory=dict)
    migrated_legacy_ids: list[int] = field(default_factory=list)
    session_updated_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "state_version": self.version,
            "source_bindings": dict(self.source_bindings),
            "skipped_sources": sorted(set(self.skipped_sources)),
            "unresolved_sources": sorted(set(self.unresolved_sources)),
            "deselected_rows": {
                k: sorted(set(v)) for k, v in self.deselected_rows.items()
            },
            "migrated_legacy_ids": sorted(set(self.migrated_legacy_ids)),
            "session_updated_at": self.session_updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "InteractiveState":
        return cls(
            version=int(data.get("state_version") or INTERACTIVE_STATE_VERSION),
            source_bindings=dict(data.get("source_bindings") or {}),
            skipped_sources=list(data.get("skipped_sources") or []),
            unresolved_sources=list(data.get("unresolved_sources") or []),
            deselected_rows={
                k: [int(x) for x in v]
                for k, v in (data.get("deselected_rows") or {}).items()
            },
            migrated_legacy_ids=[int(x) for x in (data.get("migrated_legacy_ids") or [])],
            session_updated_at=data.get("session_updated_at"),
        )


def _default_state_dir(database_path: Path) -> Path:
    """Local ignored state directory adjacent to the reference database."""
    return Path(database_path).parent / ".legacy-reference-migration"


def _load_state(state_path: Path) -> InteractiveState:
    if not state_path.exists():
        return InteractiveState()
    try:
        raw = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return InteractiveState()
    if not isinstance(raw, dict):
        return InteractiveState()
    return InteractiveState.from_dict(raw)


def _save_state(state: InteractiveState, state_path: Path) -> None:
    state.session_updated_at = datetime.now(timezone.utc).isoformat(
        timespec="seconds"
    )
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(state.to_dict(), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _row_to_summary(
    row: sqlite3.Row,
    columns: set[str],
    already_migrated_map: dict[int, str],
) -> LegacyRowSummary:
    from tools.audit_legacy_reference_values import (  # local import — audit
        _analyze_row,                                   # tool is a peer module
    )

    audit = _analyze_row(
        row, columns=columns, already_migrated_map={
            int(row["id"]): [already_migrated_map[int(row["id"])]]
        } if int(row["id"]) in already_migrated_map else {},
    )
    return LegacyRowSummary(
        legacy_id=audit.legacy_id,
        genus=audit.genus or "",
        species=audit.species or "",
        source=audit.source or "",
        suggested_data_kind=audit.suggested_data_kind,
        plotable=audit.plotable,
        already_migrated=audit.already_migrated,
        existing_measurement_set_id=(
            audit.existing_normalized_ids[0]
            if audit.existing_normalized_ids
            else None
        ),
        unmapped_fields=list(audit.unmapped_fields),
    )


class InteractiveMigrationSession:
    """I/O-free engine for the interactive migration walkthrough.

    Every decision path — grouping, candidate search, refresh, assign,
    skip, unresolved, quit — is a method call so tests can exercise the
    contract without stubbing stdin. The thin :func:`interactive_loop`
    wrapper is the only piece that touches ``input``/``print``.

    Instances hold no DB connection between calls; every method opens a
    short-lived connection to ``database_path``. Two instances driving
    the same DB and state file therefore see the same live view of the
    normalized library and of prior progress.
    """

    def __init__(
        self,
        *,
        database_path: Path,
        state_path: Path,
        dry_run: bool = True,
    ) -> None:
        self.database_path = Path(database_path)
        self.state_path = Path(state_path)
        self.dry_run = bool(dry_run)
        self.state = _load_state(self.state_path)

    # -- persistence -----------------------------------------------------

    def save(self) -> None:
        _save_state(self.state, self.state_path)

    # -- library queries -------------------------------------------------

    def _open_legacy(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.database_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _open_normalized(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.database_path)
        conn.row_factory = sqlite3.Row
        init_reference_library_schema(conn)
        return conn

    def _already_migrated_map(
        self, conn: sqlite3.Connection
    ) -> dict[int, str]:
        rows = conn.execute(
            """
            SELECT id, legacy_reference_value_id
            FROM reference_measurement_sets
            WHERE legacy_reference_value_id IS NOT NULL
            """
        ).fetchall()
        return {
            int(r["legacy_reference_value_id"]): str(r["id"]) for r in rows
        }

    def load_source_groups(self) -> list[SourceGroup]:
        """Return all legacy rows grouped by exact normalized source string.

        Empty/blank source strings share one bucket keyed as ``""`` so the
        operator still has a way to handle them; they are NOT lumped
        together with any non-blank source.
        """
        legacy_conn = self._open_legacy()
        normalized_conn = self._open_normalized()
        try:
            columns = _row_columns(legacy_conn)
            already_migrated_map = self._already_migrated_map(normalized_conn)
            select_cols = list(columns)
            rows = legacy_conn.execute(
                f"SELECT {', '.join(select_cols)} FROM reference_values ORDER BY id"
            ).fetchall()
        finally:
            legacy_conn.close()
            normalized_conn.close()

        by_key: dict[str, SourceGroup] = {}
        for row in rows:
            summary = _row_to_summary(row, columns, already_migrated_map)
            key = _normalize_source_key(summary.source)
            grp = by_key.get(key)
            if grp is None:
                grp = SourceGroup(source=summary.source, source_key=key)
                by_key[key] = grp
            grp.rows.append(summary)
        # Refresh the state cache of migrated ids off the authoritative DB.
        self.state.migrated_legacy_ids = sorted(already_migrated_map.keys())
        return sorted(by_key.values(), key=lambda g: g.source_key)

    def pending_groups(self) -> list[SourceGroup]:
        """Groups the operator still has to decide on.

        A group is pending unless:

        * every row is already migrated (``legacy_reference_value_id``
          is set) — apply mode advances via this branch;
        * the operator has ALREADY bound the source to a work this
          session (``source_bindings`` has an entry) — this is what
          advances the walkthrough in dry-run mode, where no rows have
          actually been written yet but the operator has expressed their
          decision;
        * the operator has skipped or left the source unresolved;
        * the source is classified as personal / Sporely-computed.
        """
        groups = self.load_source_groups()
        skipped = set(self.state.skipped_sources)
        unresolved = set(self.state.unresolved_sources)
        bound = set(self.state.source_bindings.keys())
        pending: list[SourceGroup] = []
        for group in groups:
            if not group.unmigrated_rows:
                continue
            if _is_personal_source(group.source):
                continue
            if group.source_key in skipped or group.source_key in unresolved:
                continue
            if group.source_key in bound:
                continue
            pending.append(group)
        return pending

    def personal_groups(self) -> list[SourceGroup]:
        """Legacy source groups classified as personal/Sporely-computed.

        Retained for summary counts and for tests. These groups are
        never presented to the operator as literature-migration work
        and never migrated into normalized literature records.
        """
        return [
            g for g in self.load_source_groups()
            if _is_personal_source(g.source)
        ]

    def refresh_library(self) -> None:
        """No-op hook — every method already re-queries the library. The
        method is exposed for symmetry with the ``r`` menu action and for
        tests that want an explicit refresh event."""
        return None

    def list_work_candidates(
        self,
        query: str | None = None,
        *,
        limit: int = 25,
    ) -> list[WorkCandidate]:
        works = ReferenceWorkRepository.search(query or None, limit=int(limit))
        result: list[WorkCandidate] = []
        for work in works:
            authors_summary = ""
            try:
                authors = json.loads(work.authors_json or "[]")
            except (TypeError, ValueError, json.JSONDecodeError):
                authors = []
            if isinstance(authors, list) and authors:
                labels = []
                for entry in authors:
                    if isinstance(entry, dict):
                        family = str(entry.get("family") or "").strip()
                        literal = str(entry.get("literal") or "").strip()
                        labels.append(family or literal)
                    elif isinstance(entry, str):
                        labels.append(entry.strip())
                labels = [x for x in labels if x]
                if len(labels) > 2:
                    authors_summary = f"{labels[0]} et al."
                elif labels:
                    authors_summary = " & ".join(labels)
            result.append(
                WorkCandidate(
                    work_id=work.id,
                    short_label=work.short_label or work.title or work.id,
                    title=work.title or "",
                    year=work.year,
                    authors_summary=authors_summary,
                )
            )
        return result

    # -- decisions -------------------------------------------------------

    def skip_group(self, source_key: str) -> None:
        if source_key not in self.state.skipped_sources:
            self.state.skipped_sources.append(source_key)
        # Clearing any prior binding — skipping trumps a stale binding.
        self.state.source_bindings.pop(source_key, None)
        self.save()

    def mark_unresolved(self, source_key: str) -> None:
        if source_key not in self.state.unresolved_sources:
            self.state.unresolved_sources.append(source_key)
        self.save()

    def deselect_row(self, source_key: str, legacy_id: int) -> None:
        current = set(self.state.deselected_rows.get(source_key, []))
        current.add(int(legacy_id))
        self.state.deselected_rows[source_key] = sorted(current)
        self.save()

    def clear_deselection(self, source_key: str) -> None:
        self.state.deselected_rows.pop(source_key, None)
        self.save()

    def _resolve_group(self, source_key: str) -> SourceGroup | None:
        for group in self.load_source_groups():
            if group.source_key == source_key:
                return group
        return None

    def _verify_work(self, work_id: str) -> bool:
        return ReferenceWorkRepository.get(work_id) is not None

    def assign_group_to_work(
        self,
        source_key: str,
        work_id: str,
    ) -> MigrationReport:
        """Migrate every currently-selected legacy row under ``source_key``
        to the normalized work ``work_id``. Idempotent: rows already
        stamped with ``legacy_reference_value_id`` are reported as reused
        and never double-inserted.
        """
        report = MigrationReport(dry_run=self.dry_run)

        # Validate that the target work still exists — an operator could
        # have deleted it between candidate listing and confirmation.
        if not self._verify_work(work_id):
            report.failed.append(
                {
                    "source_key": source_key,
                    "reason": (
                        f"selected work {work_id} no longer exists in the "
                        "normalized library — refresh and pick another"
                    ),
                }
            )
            # Drop the stale binding if any.
            if self.state.source_bindings.get(source_key) == work_id:
                self.state.source_bindings.pop(source_key, None)
                self.save()
            return report

        group = self._resolve_group(source_key)
        if group is None:
            report.failed.append(
                {
                    "source_key": source_key,
                    "reason": "source group not found in the current legacy DB",
                }
            )
            return report

        # Safety net: even if a caller somehow supplies a personal/cloud
        # source key, refuse to migrate its rows into a literature work.
        if _is_personal_source(group.source):
            report.failed.append(
                {
                    "source_key": source_key,
                    "reason": (
                        "source is classified as personal/Sporely-computed "
                        "and cannot be migrated into a literature Reference Work"
                    ),
                }
            )
            return report

        # Remember the operator's choice BEFORE writing so a crash mid-
        # apply still leaves the binding intact for the resume path.
        self.state.source_bindings[source_key] = work_id
        # Skipping/unresolved is cleared if the operator now assigns.
        self.state.skipped_sources = [
            s for s in self.state.skipped_sources if s != source_key
        ]
        self.state.unresolved_sources = [
            s for s in self.state.unresolved_sources if s != source_key
        ]
        self.save()

        deselected = set(self.state.deselected_rows.get(source_key, []))

        legacy_conn = self._open_legacy()
        normalized_conn = self._open_normalized()
        try:
            columns = _row_columns(legacy_conn)
            # Iterate over every row in the group — already-migrated rows
            # must still surface as ``reused`` on a re-assign so
            # idempotency is a first-class property, not an implicit one.
            for row_summary in group.rows:
                if row_summary.legacy_id in deselected:
                    report.skipped.append(
                        {
                            "legacy_id": row_summary.legacy_id,
                            "reason": "deselected by operator",
                        }
                    )
                    continue
                # Re-check migration status on every row — a concurrent
                # session might have already migrated it.
                existing = _find_existing_by_legacy_id(
                    normalized_conn, row_summary.legacy_id
                )
                if existing is not None:
                    report.reused.append(
                        {
                            "legacy_id": row_summary.legacy_id,
                            "measurement_set_id": existing,
                            "reason": "already migrated on a prior run",
                        }
                    )
                    continue
                legacy_row = _fetch_legacy_row(
                    legacy_conn, row_summary.legacy_id
                )
                if legacy_row is None:
                    report.failed.append(
                        {
                            "legacy_id": row_summary.legacy_id,
                            "reason": "legacy row disappeared between listing and apply",
                        }
                    )
                    continue
                self._migrate_row_under_work(
                    legacy_row,
                    columns=columns,
                    work_id=work_id,
                    row_summary=row_summary,
                    normalized_conn=normalized_conn,
                    report=report,
                )
        finally:
            legacy_conn.close()
            normalized_conn.close()

        # Cache authoritative migrated-id list off the DB after writes.
        with self._open_normalized() as fresh_conn:
            self.state.migrated_legacy_ids = sorted(
                self._already_migrated_map(fresh_conn).keys()
            )
        self.save()
        return report

    def _migrate_row_under_work(
        self,
        legacy_row: sqlite3.Row,
        *,
        columns: set[str],
        work_id: str,
        row_summary: LegacyRowSummary,
        normalized_conn: sqlite3.Connection,
        report: MigrationReport,
    ) -> None:
        legacy_id = int(row_summary.legacy_id)
        taxon_name = row_summary.taxon_label

        # In dry-run, we still simulate treatment reuse to keep the report
        # accurate, but no writes hit the DB.
        if self.dry_run:
            treatment_id = f"__dry_run_treatment__{legacy_id}"
        else:
            existing_treatment_id = _existing_treatment(
                normalized_conn,
                work_id=work_id,
                name_as_published=taxon_name,
            )
            if existing_treatment_id is not None:
                treatment_id = existing_treatment_id
            else:
                try:
                    treatment = TaxonTreatmentRepository.create(
                        TaxonTreatment(
                            id="",
                            reference_work_id=work_id,
                            name_as_published=taxon_name,
                        )
                    )
                except (ReferenceValidationError, ReferenceLibraryError) as exc:
                    report.failed.append(
                        {
                            "legacy_id": legacy_id,
                            "reason": f"failed to create treatment: {exc}",
                        }
                    )
                    return
                treatment_id = treatment.id

        payload = _measurement_payload_from_legacy(
            legacy_row,
            columns,
            {"data_kind": row_summary.suggested_data_kind, "notes": None},
            treatment_id=treatment_id,
            legacy_id=legacy_id,
        )
        unsupported = _unsupported_legacy_notes(legacy_row, columns)
        if unsupported:
            report.unsupported_fields.append(
                {"legacy_id": legacy_id, "preserved_in_notes": unsupported}
            )

        if self.dry_run:
            report.created.append(
                {
                    "legacy_id": legacy_id,
                    "work_id": work_id,
                    "would_create_measurement_set": True,
                    "data_kind": payload.data_kind,
                }
            )
            return

        try:
            created = MeasurementSetRepository.create(payload)
        except (ReferenceValidationError, ReferenceLibraryError) as exc:
            report.failed.append(
                {
                    "legacy_id": legacy_id,
                    "reason": f"failed to create measurement set: {exc}",
                }
            )
            return
        report.created.append(
            {
                "legacy_id": legacy_id,
                "work_id": work_id,
                "treatment_id": treatment_id,
                "measurement_set_id": created.id,
            }
        )

    # -- summary ---------------------------------------------------------

    def summary(self) -> dict[str, Any]:
        groups = self.load_source_groups()
        # Split legacy rows into literature vs personal/computed. Every
        # count below counts LITERATURE rows only unless explicitly
        # labelled ``personal_computed_rows``.
        literature_groups = [
            g for g in groups if not _is_personal_source(g.source)
        ]
        personal = [g for g in groups if _is_personal_source(g.source)]
        total_rows = sum(len(g.rows) for g in literature_groups)
        migrated = sum(len(g.migrated_rows) for g in literature_groups)
        pending_groups = self.pending_groups()
        remaining = sum(len(g.unmigrated_rows) for g in pending_groups)
        skipped_row_count = sum(
            len(g.unmigrated_rows)
            for g in literature_groups
            if g.source_key in set(self.state.skipped_sources)
        )
        unresolved_row_count = sum(
            len(g.unmigrated_rows)
            for g in literature_groups
            if g.source_key in set(self.state.unresolved_sources)
        )
        personal_computed_rows = sum(len(g.rows) for g in personal)
        next_group = pending_groups[0] if pending_groups else None
        return {
            "total_rows": total_rows,
            "migrated": migrated,
            "remaining": remaining,
            "skipped_rows": skipped_row_count,
            "unresolved_rows": unresolved_row_count,
            "personal_computed_rows": personal_computed_rows,
            "personal_computed_groups": len(personal),
            "pending_groups": len(pending_groups),
            "next_source": next_group.source if next_group else None,
            "next_source_row_count": (
                len(next_group.unmigrated_rows) if next_group else 0
            ),
        }


# ---------------------------------------------------------------------------
# Interactive terminal driver
# ---------------------------------------------------------------------------


_MENU_ACTIONS = {
    "r": "refresh",
    "s": "skip",
    "u": "unresolved",
    "q": "quit",
    "d": "deselect",
}


def _emit(line: str, stream_out=None) -> None:
    if stream_out is None:
        print(line)
    else:
        print(line, file=stream_out)


def _print_summary(
    session: InteractiveMigrationSession,
    *,
    style: _Style | None = None,
    stream_out=None,
) -> None:
    st = style or _Style(enabled=False)
    s = session.summary()
    total = s["total_rows"]
    _emit("", stream_out)
    _emit(
        f"{st.bold('Literature migrated:')}   "
        f"{st.green(str(s['migrated']))} / {total}",
        stream_out,
    )
    _emit(f"{st.bold('Literature remaining:')} {s['remaining']}", stream_out)
    _emit(
        f"{st.bold('Personal/computed:')}     "
        f"{st.dim(str(s['personal_computed_rows']))}",
        stream_out,
    )
    _emit(f"{st.bold('Skipped:')}                {s['skipped_rows']}", stream_out)
    _emit(
        f"{st.bold('Unresolved:')}             "
        f"{st.yellow(str(s['unresolved_rows'])) if s['unresolved_rows'] else '0'}",
        stream_out,
    )
    if s["next_source"] is not None:
        next_label = s["next_source"] or ""
        if next_label:
            display = st.bold_cyan(next_label)
        else:
            display = st.bold_yellow("No source recorded")
        _emit(
            f"{st.bold('Next source:')} {display} "
            f"({s['next_source_row_count']} rows)",
            stream_out,
        )
    _emit("", stream_out)


def _print_group(
    group: SourceGroup,
    state: InteractiveState,
    *,
    style: _Style | None = None,
    stream_out=None,
) -> None:
    st = style or _Style(enabled=False)
    _emit("", stream_out)
    _emit("─" * 57, stream_out)
    if group.source.strip():
        label_display = st.bold_cyan(group.source)
    else:
        # Genuine empty legacy source — flag visually. The underlying
        # ``group.source_key`` stays "" so the operator can still bind,
        # skip, or leave unresolved without special casing.
        label_display = st.bold_yellow("No source recorded")
    _emit(f"{st.bold('Legacy source:')} {label_display}", stream_out)
    row_count = len(group.unmigrated_rows)
    row_word = "row" if row_count == 1 else "rows"
    _emit(f"{row_count} legacy {row_word}", stream_out)

    # Group-level provenance note for parmasto sources — printed ONCE
    # per group rather than as a redundant warning on every row. The
    # ``· parmasto`` kind marker on each row already conveys the data
    # shape; this line only tells the operator that the specialized
    # columns will be preserved in migration ``notes`` for provenance.
    if any(
        r.suggested_data_kind == "parmasto"
        for r in group.unmigrated_rows
    ):
        _emit(
            st.dim(
                "  Parmasto values preserved as provenance in migration notes."
            ),
            stream_out,
        )
    _emit("", stream_out)

    deselected = set(state.deselected_rows.get(group.source_key, []))
    for row in group.unmigrated_rows:
        mark = "[ ]" if row.legacy_id in deselected else "[x]"
        # Only surface `plotable=True` implicitly (silence) — call
        # attention to a row only when it is NOT plotable or has an
        # unexpected unmapped hint. ``parmasto_*`` is intentionally
        # excluded from per-row warnings: every parmasto row would
        # otherwise repeat the same string 200 times, and the row's
        # own ``· parmasto`` kind marker already signals the shape.
        warnings: list[str] = []
        if not row.plotable:
            warnings.append(st.yellow("not plotable"))
        if row.unmapped_fields:
            interesting = [
                f for f in row.unmapped_fields
                if f.startswith("plot_color")
            ]
            if interesting:
                warnings.append(st.yellow(f"({'; '.join(interesting)})"))
        row_extras = ("  " + "  ".join(warnings)) if warnings else ""
        taxon = row.taxon_label
        row_line = (
            f"  {mark} {taxon:<32s} "
            f"{st.dim(f'row {row.legacy_id}')} · {row.suggested_data_kind}"
            f"{row_extras}"
        )
        _emit(row_line, stream_out)
    if group.migrated_rows:
        _emit("", stream_out)
        _emit(f"  {st.green('Already migrated:')}", stream_out)
        for row in group.migrated_rows:
            _emit(
                f"    - {row.taxon_label} "
                f"{st.dim(f'(row {row.legacy_id})')}",
                stream_out,
            )


def _print_candidates(
    candidates: list[WorkCandidate],
    *,
    style: _Style | None = None,
    stream_out=None,
) -> None:
    st = style or _Style(enabled=False)
    _emit("", stream_out)
    _emit(st.bold("Reference Works"), stream_out)
    _emit("", stream_out)
    if not candidates:
        _emit(
            "  " + st.yellow(
                "(no matching reference works — press [r] to refresh, or "
                "use the desktop UI to create one and then press [r])"
            ),
            stream_out,
        )
        return
    for i, cand in enumerate(candidates, start=1):
        for line in cand.display(i, style=st):
            _emit(line, stream_out)


def _prompt(prompt_text: str, *, stream_in=None) -> str:
    if stream_in is None:
        return input(prompt_text)
    stream_in_line = stream_in.readline()
    if not stream_in_line:
        return "q"
    return stream_in_line.rstrip("\n")


def interactive_loop(
    session: InteractiveMigrationSession,
    *,
    stream_in=None,
    stream_out=None,
    style: _Style | None = None,
) -> None:
    """Stdin/stdout driver over :class:`InteractiveMigrationSession`.

    The engine itself is I/O-free; this function is intentionally thin so
    every branch is testable by injecting ``stream_in`` and ``stream_out``.
    Terminal styling is auto-detected (TTY + ``NO_COLOR``) unless a
    ``_Style`` instance is passed explicitly — tests always pass a
    disabled style so assertions see plain text.
    """
    import builtins as _b

    st = style or _Style()

    def _out(text: str = "") -> None:
        _emit(text, stream_out)

    def _in(prompt_text: str) -> str:
        if stream_in is None:
            return _b.input(prompt_text)
        _out(prompt_text)
        line = stream_in.readline()
        return line.rstrip("\n") if line else "q"

    # Mode banner: dry-run is the safe default so the operator should
    # never wonder whether their picks are being written. The banner is
    # printed ONCE at the top of the walkthrough.
    if session.dry_run:
        _out(st.bold_yellow(
            "DRY-RUN mode: selections are recorded but nothing is written "
            "to the normalized database. Re-run with --apply --confirm-backup "
            "to persist migrations."
        ))
    else:
        _out(st.bold_cyan(
            "APPLY mode: numeric picks migrate legacy rows immediately."
        ))
    _print_summary(session, style=st, stream_out=stream_out)
    while True:
        pending = session.pending_groups()
        if not pending:
            _out(st.green("All groups decided. Nothing left to do."))
            return
        group = pending[0]
        _print_group(group, session.state, style=st, stream_out=stream_out)
        candidates = session.list_work_candidates()
        _print_candidates(candidates, style=st, stream_out=stream_out)
        _out("")
        _out(f"    {st.dim('[r] Refresh')}")
        _out(f"    {st.dim('[s] Skip')}")
        _out(f"    {st.dim('[u] Unresolved')}")
        _out(f"    {st.dim('[d N] Deselect row')}")
        _out(f"    {st.dim('[q] Save and quit')}")
        raw = _in("Selection: ").strip()
        if not raw:
            continue
        low = raw.lower()
        if low == "q":
            session.save()
            _out(st.green("Progress saved."))
            return
        if low == "r":
            session.refresh_library()
            continue
        if low == "s":
            session.skip_group(group.source_key)
            continue
        if low == "u":
            session.mark_unresolved(group.source_key)
            continue
        if low.startswith("d "):
            try:
                legacy_id = int(low.split(None, 1)[1])
            except (ValueError, IndexError):
                _out(st.red("Enter 'd <legacy_id>'."))
                continue
            session.deselect_row(group.source_key, legacy_id)
            continue
        if raw.isdigit():
            index = int(raw)
            if 1 <= index <= len(candidates):
                chosen = candidates[index - 1]
                # A numeric candidate pick is itself the confirmation —
                # apply immediately in the current mode (dry-run or apply).
                rep = session.assign_group_to_work(
                    group.source_key, chosen.work_id
                )
                created = len(rep.created)
                reused = len(rep.reused)
                skipped = len(rep.skipped)
                failed = len(rep.failed)
                mode_tag = (
                    st.yellow(" (dry-run — no rows written)")
                    if session.dry_run
                    else ""
                )
                _out(
                    "  "
                    f"{st.green(f'created={created}')} "
                    f"{st.green(f'reused={reused}')} "
                    f"skipped={skipped} "
                    f"{(st.red(f'failed={failed}') if failed else f'failed={failed}')}"
                    f"{mode_tag}"
                )
                continue
            _out(st.red("Number out of range."))
            continue
        # Free text -> narrow the candidate list.
        candidates = session.list_work_candidates(query=raw)
        _print_candidates(candidates, style=st, stream_out=stream_out)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Legacy → normalized reference-library migration. The default "
            "workflow is the interactive terminal walkthrough (--interactive) "
            "which groups legacy rows by exact normalized source string and "
            "lets the operator explicitly bind each group to an existing "
            "normalized Reference Work. Dry-run by default; --apply requires "
            "--confirm-backup. Manifest-based operation is retained for "
            "tests and recovery."
        )
    )
    p.add_argument("--manifest", type=Path, default=None)
    p.add_argument("--database", type=Path, default=None)
    p.add_argument(
        "--interactive",
        action="store_true",
        help=(
            "Run the interactive terminal walkthrough. Groups legacy rows "
            "by exact normalized source string and asks the operator to "
            "pick a normalized Reference Work per group. Progress is "
            "persisted to the state directory (default: alongside the "
            "reference database)."
        ),
    )
    p.add_argument(
        "--state-dir",
        type=Path,
        default=None,
        help=(
            "Directory for the interactive session's persisted state. "
            "Defaults to a local ignored directory next to the reference "
            "database."
        ),
    )
    p.add_argument(
        "--summary",
        action="store_true",
        help=(
            "Print the interactive migration summary (migrated / remaining "
            "/ skipped / unresolved counts + next pending source) and exit."
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Validate + simulate without writing (this is the default).",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Actually perform writes. Requires --confirm-backup and refuses "
            "to run while any entry is still 'unresolved'."
        ),
    )
    p.add_argument(
        "--confirm-backup",
        action="store_true",
        help=(
            "Acknowledge that you have made a backup of the reference "
            "database file. Required alongside --apply."
        ),
    )
    p.add_argument(
        "--report-out",
        type=Path,
        default=None,
        help="Optional path to write the final migration report as JSON.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_argparser().parse_args(argv)

    database_path: Path = args.database or Path(get_reference_database_path())
    if not database_path.exists():
        print(f"reference database not found: {database_path}", file=sys.stderr)
        return 2

    if args.interactive or args.summary:
        state_dir: Path = args.state_dir or _default_state_dir(database_path)
        state_dir.mkdir(parents=True, exist_ok=True)
        state_path = state_dir / "interactive-state.json"
        if args.apply and not args.confirm_backup:
            print(
                "refusing to --apply without --confirm-backup: make a copy of "
                "the reference database first, then rerun with both flags.",
                file=sys.stderr,
            )
            return 2
        session = InteractiveMigrationSession(
            database_path=database_path,
            state_path=state_path,
            dry_run=not args.apply,
        )
        if args.summary:
            s = session.summary()
            print(f"Literature migrated:   {s['migrated']} / {s['total_rows']}")
            print(f"Literature remaining:  {s['remaining']}")
            print(f"Personal/computed:     {s['personal_computed_rows']}")
            print(f"Skipped:               {s['skipped_rows']}")
            print(f"Unresolved:            {s['unresolved_rows']}")
            if s["next_source"] is not None:
                next_label = s["next_source"] or "No source recorded"
                print(
                    f"Next source: {next_label} "
                    f"({s['next_source_row_count']} rows)"
                )
            return 0
        interactive_loop(session)
        return 0

    manifest_path: Path | None = args.manifest
    if manifest_path is None:
        print(
            "--manifest is required unless --interactive or --summary is passed",
            file=sys.stderr,
        )
        return 2
    if not manifest_path.exists():
        print(f"manifest not found: {manifest_path}", file=sys.stderr)
        return 2
    try:
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest = validate_manifest(raw)
    except ManifestError as exc:
        print(f"manifest validation error: {exc}", file=sys.stderr)
        return 2
    except (OSError, json.JSONDecodeError) as exc:
        print(f"could not read manifest: {exc}", file=sys.stderr)
        return 2

    if args.apply:
        if not args.confirm_backup:
            print(
                "refusing to --apply without --confirm-backup: make a copy of "
                "the reference database first, then rerun with both flags.",
                file=sys.stderr,
            )
            return 2
        dry_run = False
    else:
        dry_run = True

    try:
        report = run_migration(
            manifest, database_path=database_path, dry_run=dry_run
        )
    except ManifestError as exc:
        print(f"migration refused: {exc}", file=sys.stderr)
        return 2

    payload = report.to_dict()
    if args.report_out is not None:
        args.report_out.parent.mkdir(parents=True, exist_ok=True)
        args.report_out.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    tag = "dry-run" if dry_run else "apply"
    counts = payload["counts"]
    print(
        f"[migrate:{tag}] created={counts['created']} "
        f"reused={counts['reused']} skipped={counts['skipped']} "
        f"unresolved={counts['unresolved']} failed={counts['failed']}"
    )
    if payload["unsupported_fields"]:
        print(
            f"[migrate:{tag}] unsupported/preserved fields on "
            f"{len(payload['unsupported_fields'])} row(s)."
        )
    return 0 if not report.failed else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
