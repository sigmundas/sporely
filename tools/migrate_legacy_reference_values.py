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
    dump for parmasto / plot_color / metadata_json."""
    unsupported = _unsupported_legacy_notes(legacy_row, columns)
    pieces: list[str] = []
    if manifest_notes:
        pieces.append(str(manifest_notes).strip())
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
# CLI
# ---------------------------------------------------------------------------


def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Apply a human-authored legacy → normalized reference-library "
            "migration manifest. Dry-run by default; use --apply plus "
            "--confirm-backup to actually write."
        )
    )
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--database", type=Path, default=None)
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
    manifest_path: Path = args.manifest
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

    database_path: Path = args.database or Path(get_reference_database_path())
    if not database_path.exists():
        print(f"reference database not found: {database_path}", file=sys.stderr)
        return 2

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
