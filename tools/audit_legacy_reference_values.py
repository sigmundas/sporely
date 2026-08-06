#!/usr/bin/env python3
"""Read-only audit of the legacy ``reference_values`` table.

Phase A of the manual migration workflow into the normalized reference
library. This script NEVER modifies either the legacy reference database
or the normalized reference library. It does NOT call an AI or any
external service. It produces three artifacts:

1. A human-readable Markdown report.
2. A CSV inventory (one row per legacy record).
3. A JSON migration-template manifest (versioned, ``action: "unresolved"``
   by default) for phase B.

Duplicate detection is strictly deterministic: exact matches by
``(genus, species, source, mount_medium, stain)`` plus identical numeric
columns. Suggested "same-source" groups are marked as *suggestions
only* — the migration script never merges rows unless the manifest
explicitly says so.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.schema import get_reference_database_path  # noqa: E402


MANIFEST_VERSION = 1

# Columns the audit knows how to read from the legacy table. Any additional
# columns discovered at runtime are reported as ``unmapped_fields`` per row
# — the audit never silently drops them.
_KNOWN_LEGACY_COLUMNS: tuple[str, ...] = (
    "id",
    "genus",
    "species",
    "source",
    "mount_medium",
    "stain",
    "plot_color",
    "parmasto_length_mean",
    "parmasto_width_mean",
    "parmasto_q_mean",
    "parmasto_v_sp_length",
    "parmasto_v_sp_width",
    "parmasto_v_sp_q",
    "parmasto_v_ind_length",
    "parmasto_v_ind_width",
    "parmasto_v_ind_q",
    "length_min",
    "length_p05",
    "length_p50",
    "length_p95",
    "length_max",
    "length_avg",
    "width_min",
    "width_p05",
    "width_p50",
    "width_p95",
    "width_max",
    "width_avg",
    "q_min",
    "q_p05",
    "q_p50",
    "q_p95",
    "q_max",
    "q_avg",
    "metadata_json",
    "updated_at",
)

_PARMASTO_COLUMNS: tuple[str, ...] = (
    "parmasto_length_mean",
    "parmasto_width_mean",
    "parmasto_q_mean",
    "parmasto_v_sp_length",
    "parmasto_v_sp_width",
    "parmasto_v_sp_q",
    "parmasto_v_ind_length",
    "parmasto_v_ind_width",
    "parmasto_v_ind_q",
)

_LENGTH_STAT_COLUMNS: tuple[str, ...] = (
    "length_min",
    "length_p05",
    "length_p50",
    "length_p95",
    "length_max",
    "length_avg",
)
_WIDTH_STAT_COLUMNS: tuple[str, ...] = (
    "width_min",
    "width_p05",
    "width_p50",
    "width_p95",
    "width_max",
    "width_avg",
)
_Q_STAT_COLUMNS: tuple[str, ...] = (
    "q_min",
    "q_p05",
    "q_p50",
    "q_p95",
    "q_max",
    "q_avg",
)

# The identifying fingerprint used for exact-duplicate detection. Note:
# order intentionally includes every measurement column so that two rows
# with the same (genus, species, source, mount, stain) but different
# numeric values are NOT collapsed into one duplicate — that is a
# revision, not a duplicate.
_EXACT_DUPLICATE_FIELDS: tuple[str, ...] = (
    "genus",
    "species",
    "source",
    "mount_medium",
    "stain",
) + _LENGTH_STAT_COLUMNS + _WIDTH_STAT_COLUMNS + _Q_STAT_COLUMNS + _PARMASTO_COLUMNS


def _finite_positive(value: Any) -> bool:
    """Mirror the plotability rule used by the normalized library UI."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(f) and f > 0.0


def _read_only_uri(path: Path) -> str:
    return f"file:{path.as_posix()}?mode=ro"


def _open_legacy_readonly(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise SystemExit(
            f"legacy reference database not found: {path}. "
            f"Pass --database to point at the correct file."
        )
    conn = sqlite3.connect(_read_only_uri(path), uri=True)
    conn.row_factory = sqlite3.Row
    # Belt-and-braces: also disable writes at the SQLite level.
    conn.execute("PRAGMA query_only = ON")
    return conn


def _open_normalized_readonly(path: Path) -> sqlite3.Connection | None:
    """Return a read-only connection to the normalized library file if it
    exists AND already contains the normalized tables. Returns ``None``
    otherwise — a brand-new install may not have the tables yet.

    In the current layout the normalized tables live in the SAME sqlite
    file as the legacy ``reference_values`` table. We still open a
    separate handle so the audit code can distinguish "legacy" vs
    "normalized" reads clearly.
    """
    if not path.exists():
        return None
    conn = sqlite3.connect(_read_only_uri(path), uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='reference_measurement_sets' LIMIT 1"
    ).fetchone()
    if row is None:
        conn.close()
        return None
    return conn


# --- Row analysis -----------------------------------------------------------


@dataclass
class RowAudit:
    legacy_id: int
    genus: str | None
    species: str | None
    source: str | None
    mount_medium: str | None
    stain: str | None
    plot_color: str | None
    length_stats: dict[str, float | None] = field(default_factory=dict)
    width_stats: dict[str, float | None] = field(default_factory=dict)
    q_stats: dict[str, float | None] = field(default_factory=dict)
    parmasto: dict[str, float | None] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    metadata_malformed: bool = False
    plotable: bool = False
    already_migrated: bool = False
    existing_normalized_ids: list[str] = field(default_factory=list)
    exact_duplicate_of: list[int] = field(default_factory=list)
    same_source_group: str | None = None
    missing_publication_fields: list[str] = field(default_factory=list)
    unmapped_fields: list[str] = field(default_factory=list)
    suggested_data_kind: str = "summary"


def _stat_dict(row: sqlite3.Row, cols: Iterable[str]) -> dict[str, float | None]:
    result: dict[str, float | None] = {}
    for col in cols:
        try:
            value = row[col]
        except (KeyError, IndexError):
            continue
        if value is None:
            result[col] = None
            continue
        try:
            result[col] = float(value)
        except (TypeError, ValueError):
            result[col] = None
    return result


def _parse_metadata(raw: Any) -> tuple[dict[str, Any], bool]:
    if raw is None:
        return {}, False
    if isinstance(raw, dict):
        return dict(raw), False
    text = str(raw).strip()
    if not text:
        return {}, False
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}, True
    if not isinstance(parsed, dict):
        return {}, True
    return dict(parsed), False


def _suggest_data_kind(row_audit: RowAudit) -> str:
    """Deterministic mapping from legacy shape to normalized ``data_kind``.

    Priority:

    * any populated ``parmasto_*`` column -> ``parmasto`` (unplottable in
      the current desktop pipeline; preserved for provenance).
    * any populated length min/max/p05/p95 -> ``range``.
    * length_avg/p50 or width_avg/p50 populated (means only) -> ``summary``.
    * everything null -> ``summary`` (kind is required by the schema).
    """
    if any(v is not None for v in row_audit.parmasto.values()):
        return "parmasto"
    range_bound_cols = (
        "length_min", "length_max", "length_p05", "length_p95",
        "width_min", "width_max", "width_p05", "width_p95",
    )
    for key in range_bound_cols:
        value = row_audit.length_stats.get(key)
        if value is None:
            value = row_audit.width_stats.get(key)
        if value is not None:
            return "range"
    if (
        row_audit.length_stats.get("length_avg") is not None
        or row_audit.length_stats.get("length_p50") is not None
        or row_audit.width_stats.get("width_avg") is not None
        or row_audit.width_stats.get("width_p50") is not None
    ):
        return "summary"
    return "summary"


def _is_plotable(row_audit: RowAudit) -> bool:
    """Mirror the desktop plot-hint predicate for the audit's summary
    counts. A row is "plotable" if it can produce a finite-positive
    length/width rectangle or a finite-positive length/width mean pair.
    ``parmasto`` rows are NOT currently plotable by the desktop pipeline
    and count as unplotable regardless of parmasto values.
    """
    def _fp(*keys: str) -> bool:
        for key in keys:
            v = row_audit.length_stats.get(key)
            if v is None:
                v = row_audit.width_stats.get(key)
            if _finite_positive(v):
                return True
        return False

    length_rect_ok = (
        _finite_positive(row_audit.length_stats.get("length_min"))
        and _finite_positive(row_audit.length_stats.get("length_max"))
        and _finite_positive(row_audit.width_stats.get("width_min"))
        and _finite_positive(row_audit.width_stats.get("width_max"))
        and float(row_audit.length_stats["length_max"])
        > float(row_audit.length_stats["length_min"])
        and float(row_audit.width_stats["width_max"])
        > float(row_audit.width_stats["width_min"])
    )
    core_rect_ok = (
        _finite_positive(row_audit.length_stats.get("length_p05"))
        and _finite_positive(row_audit.length_stats.get("length_p95"))
        and _finite_positive(row_audit.width_stats.get("width_p05"))
        and _finite_positive(row_audit.width_stats.get("width_p95"))
        and float(row_audit.length_stats["length_p95"])
        > float(row_audit.length_stats["length_p05"])
        and float(row_audit.width_stats["width_p95"])
        > float(row_audit.width_stats["width_p05"])
    )
    mean_ok = _finite_positive(
        row_audit.length_stats.get("length_avg")
    ) and _finite_positive(row_audit.width_stats.get("width_avg"))
    return length_rect_ok or core_rect_ok or mean_ok


def _analyze_row(
    row: sqlite3.Row,
    *,
    columns: set[str],
    already_migrated_map: dict[int, list[str]],
) -> RowAudit:
    metadata, malformed = _parse_metadata(row["metadata_json"] if "metadata_json" in columns else None)
    audit = RowAudit(
        legacy_id=int(row["id"]),
        genus=(row["genus"] if row["genus"] is not None else None),
        species=(row["species"] if row["species"] is not None else None),
        source=(row["source"] if "source" in columns and row["source"] is not None else None),
        mount_medium=(row["mount_medium"] if "mount_medium" in columns else None),
        stain=(row["stain"] if "stain" in columns else None),
        plot_color=(row["plot_color"] if "plot_color" in columns else None),
        length_stats=_stat_dict(row, [c for c in _LENGTH_STAT_COLUMNS if c in columns]),
        width_stats=_stat_dict(row, [c for c in _WIDTH_STAT_COLUMNS if c in columns]),
        q_stats=_stat_dict(row, [c for c in _Q_STAT_COLUMNS if c in columns]),
        parmasto=_stat_dict(row, [c for c in _PARMASTO_COLUMNS if c in columns]),
        metadata=metadata,
        metadata_malformed=malformed,
    )
    audit.suggested_data_kind = _suggest_data_kind(audit)
    audit.plotable = _is_plotable(audit)

    normalized_ids = already_migrated_map.get(int(row["id"]), [])
    if normalized_ids:
        audit.already_migrated = True
        audit.existing_normalized_ids = list(normalized_ids)

    # Missing publication fields: the audit deliberately does NOT parse
    # ``source`` into authors/year. It reports whether a source label is
    # present at all so the operator can decide what to fill in.
    missing: list[str] = []
    if not audit.source:
        missing.append("source label")
    # Every legacy row is missing every structured bibliographic field
    # because the legacy shape has none of them. Report the three that
    # matter most so the operator immediately sees what has to be
    # supplied via the manifest.
    for field_name in ("title", "authors", "year"):
        missing.append(field_name)
    audit.missing_publication_fields = missing

    # Unmapped fields: parmasto columns (data_kind='parmasto' preserves
    # provenance but the desktop pipeline cannot yet render them), plot
    # colour (not part of the normalized model), and any metadata keys
    # we don't know how to translate. Reported, never silently dropped.
    unmapped: list[str] = []
    if any(v is not None for v in audit.parmasto.values()):
        unmapped.append("parmasto_* (mapped to data_kind='parmasto', preserved in notes)")
    if audit.plot_color:
        unmapped.append("plot_color (no normalized equivalent)")
    if metadata:
        for key in sorted(metadata.keys()):
            if key in {"source_type", "imported_at"}:
                continue
            unmapped.append(f"metadata_json.{key}")
    if malformed:
        unmapped.append("metadata_json (malformed — could not parse)")
    # Any column the audit does not know about must surface here rather
    # than disappear.
    for col in sorted(columns):
        if col not in _KNOWN_LEGACY_COLUMNS:
            unmapped.append(f"{col} (unknown legacy column)")
    audit.unmapped_fields = unmapped
    return audit


# --- Fingerprinting / grouping ---------------------------------------------


def _norm_stat(value: Any) -> Any:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return round(f, 6)


def _exact_fingerprint(row: sqlite3.Row, columns: set[str]) -> tuple:
    result: list[Any] = []
    for col in _EXACT_DUPLICATE_FIELDS:
        if col not in columns:
            result.append(None)
            continue
        try:
            v = row[col]
        except (KeyError, IndexError):
            result.append(None)
            continue
        if col in _EXACT_DUPLICATE_FIELDS[:5]:  # genus/species/source/mount/stain
            result.append((v or "").strip() if isinstance(v, str) else v)
        else:
            result.append(_norm_stat(v))
    return tuple(result)


def _normalize_source(value: Any) -> str:
    return str(value or "").strip().lower()


# --- Audit runner -----------------------------------------------------------


def _load_already_migrated_map(
    conn: sqlite3.Connection | None,
) -> dict[int, list[str]]:
    if conn is None:
        return {}
    result: dict[int, list[str]] = {}
    rows = conn.execute(
        """
        SELECT id, legacy_reference_value_id
        FROM reference_measurement_sets
        WHERE legacy_reference_value_id IS NOT NULL
        """
    ).fetchall()
    for r in rows:
        legacy_id = int(r["legacy_reference_value_id"])
        result.setdefault(legacy_id, []).append(str(r["id"]))
    return result


def run_audit(
    *,
    database_path: Path,
) -> dict[str, Any]:
    """Return a fully-structured audit result. No files written here.

    The returned dict is the single source of truth for the Markdown,
    CSV and JSON writers below; splitting the collection step from the
    output step keeps every emitter working from identical data.
    """
    legacy_conn = _open_legacy_readonly(database_path)
    try:
        cursor = legacy_conn.execute("PRAGMA table_info(reference_values)")
        columns = {row[1] for row in cursor.fetchall()}
        if not columns:
            raise SystemExit(
                f"no reference_values table found in {database_path}"
            )
        select_cols = [c for c in columns if isinstance(c, str)]
        rows = legacy_conn.execute(
            f"SELECT {', '.join(select_cols)} FROM reference_values ORDER BY id"
        ).fetchall()
        normalized_conn = _open_normalized_readonly(database_path)
        try:
            already_migrated_map = _load_already_migrated_map(normalized_conn)
        finally:
            if normalized_conn is not None:
                normalized_conn.close()

        audits: list[RowAudit] = [
            _analyze_row(
                row,
                columns=columns,
                already_migrated_map=already_migrated_map,
            )
            for row in rows
        ]

        # Exact duplicate grouping.
        fingerprints: dict[tuple, list[int]] = {}
        for row in rows:
            fp = _exact_fingerprint(row, columns)
            fingerprints.setdefault(fp, []).append(int(row["id"]))
        for audit in audits:
            fp = None
            for candidate, ids in fingerprints.items():
                if audit.legacy_id in ids:
                    fp = candidate
                    break
            if fp is None:
                continue
            others = [i for i in fingerprints[fp] if i != audit.legacy_id]
            audit.exact_duplicate_of = sorted(others)

        # Same-source grouping (suggestion only — not fused during migration).
        source_groups: dict[str, list[int]] = {}
        for audit in audits:
            key = _normalize_source(audit.source)
            if not key:
                continue
            source_groups.setdefault(key, []).append(audit.legacy_id)
        for audit in audits:
            key = _normalize_source(audit.source)
            if not key:
                continue
            group = source_groups.get(key, [])
            if len(group) > 1:
                audit.same_source_group = key

        return {
            "database_path": str(database_path),
            "generated_at": datetime.now(timezone.utc).isoformat(
                timespec="seconds"
            ),
            "columns": sorted(columns),
            "audits": audits,
            "source_groups": {
                key: sorted(ids)
                for key, ids in source_groups.items()
                if len(ids) > 1
            },
        }
    finally:
        legacy_conn.close()


# --- Output writers ---------------------------------------------------------


def _summary_counts(audits: list[RowAudit]) -> dict[str, int]:
    total = len(audits)
    already = sum(1 for a in audits if a.already_migrated)
    plotable = sum(1 for a in audits if a.plotable)
    incomplete = sum(1 for a in audits if not a.plotable)
    exact_dupes = sum(1 for a in audits if a.exact_duplicate_of)
    same_source = sum(1 for a in audits if a.same_source_group)
    malformed_meta = sum(1 for a in audits if a.metadata_malformed)
    return {
        "total_rows": total,
        "already_migrated": already,
        "unmigrated": total - already,
        "plotable": plotable,
        "incomplete": incomplete,
        "exact_duplicates": exact_dupes,
        "rows_sharing_source_string": same_source,
        "rows_with_malformed_metadata": malformed_meta,
    }


def _write_markdown_report(
    result: dict[str, Any],
    *,
    output: Path,
) -> None:
    audits: list[RowAudit] = result["audits"]
    counts = _summary_counts(audits)
    lines: list[str] = []
    lines.append("# Legacy reference_values audit")
    lines.append("")
    lines.append(f"- Database: `{result['database_path']}`")
    lines.append(f"- Generated at: {result['generated_at']}")
    lines.append(f"- Columns present: {', '.join(result['columns'])}")
    lines.append("")
    lines.append("## Summary counts")
    lines.append("")
    for key, value in counts.items():
        lines.append(f"- {key.replace('_', ' ')}: {value}")
    lines.append("")
    if result["source_groups"]:
        lines.append("## Suggested same-source groups (not automatic)")
        lines.append("")
        lines.append(
            "> The migration script never fuses two rows automatically. The "
            "groups below are hints for the operator to consider explicitly "
            "assigning to the same normalized ``work_key`` in the manifest."
        )
        lines.append("")
        for source, ids in sorted(result["source_groups"].items()):
            lines.append(f"- `{source}` — rows {', '.join(str(i) for i in ids)}")
        lines.append("")
    lines.append("## Row inventory")
    lines.append("")
    if not audits:
        lines.append("_No legacy reference_values rows found._")
    for audit in audits:
        lines.append(f"### Row {audit.legacy_id}")
        lines.append("")
        lines.append(
            f"- Taxon: `{audit.genus or '?'} {audit.species or '?'}`"
        )
        lines.append(f"- Source: {audit.source or '_(blank)_'}")
        lines.append(
            f"- Mount medium: {audit.mount_medium or '_(none)_'}"
        )
        lines.append(f"- Stain: {audit.stain or '_(none)_'}")
        lines.append(f"- Plot colour: {audit.plot_color or '_(none)_'}")
        lines.append(
            f"- Length statistics: {json.dumps(audit.length_stats)}"
        )
        lines.append(
            f"- Width statistics: {json.dumps(audit.width_stats)}"
        )
        lines.append(f"- Q statistics: {json.dumps(audit.q_stats)}")
        lines.append(f"- Parmasto fields: {json.dumps(audit.parmasto)}")
        if audit.metadata:
            lines.append(
                f"- metadata_json: {json.dumps(audit.metadata, sort_keys=True)}"
            )
        elif audit.metadata_malformed:
            lines.append(
                "- metadata_json: _(malformed — preserved as-is)_"
            )
        lines.append(
            f"- Suggested data_kind: `{audit.suggested_data_kind}`"
        )
        lines.append(f"- Plotable: {audit.plotable}")
        lines.append(
            f"- Already migrated: {audit.already_migrated}"
            + (
                f" (normalized ids: {', '.join(audit.existing_normalized_ids)})"
                if audit.existing_normalized_ids
                else ""
            )
        )
        if audit.exact_duplicate_of:
            lines.append(
                "- Exact-duplicate candidates: "
                + ", ".join(str(i) for i in audit.exact_duplicate_of)
            )
        if audit.same_source_group:
            lines.append(
                f"- Shares source string with: `{audit.same_source_group}`"
            )
        if audit.missing_publication_fields:
            lines.append(
                "- Missing bibliographic fields: "
                + ", ".join(audit.missing_publication_fields)
            )
        if audit.unmapped_fields:
            lines.append(
                "- Unmapped/reported fields: "
                + "; ".join(audit.unmapped_fields)
            )
        lines.append("")
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_csv_inventory(
    result: dict[str, Any],
    *,
    output: Path,
) -> None:
    audits: list[RowAudit] = result["audits"]
    field_names = [
        "legacy_id",
        "genus",
        "species",
        "source",
        "mount_medium",
        "stain",
        "plot_color",
        "suggested_data_kind",
        "plotable",
        "already_migrated",
        "existing_normalized_ids",
        "exact_duplicate_of",
        "same_source_group",
        "metadata_malformed",
        "missing_publication_fields",
        "unmapped_fields",
        "length_stats",
        "width_stats",
        "q_stats",
        "parmasto",
    ]
    with output.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=field_names)
        writer.writeheader()
        for audit in audits:
            writer.writerow(
                {
                    "legacy_id": audit.legacy_id,
                    "genus": audit.genus or "",
                    "species": audit.species or "",
                    "source": audit.source or "",
                    "mount_medium": audit.mount_medium or "",
                    "stain": audit.stain or "",
                    "plot_color": audit.plot_color or "",
                    "suggested_data_kind": audit.suggested_data_kind,
                    "plotable": str(audit.plotable),
                    "already_migrated": str(audit.already_migrated),
                    "existing_normalized_ids": ";".join(
                        audit.existing_normalized_ids
                    ),
                    "exact_duplicate_of": ";".join(
                        str(i) for i in audit.exact_duplicate_of
                    ),
                    "same_source_group": audit.same_source_group or "",
                    "metadata_malformed": str(audit.metadata_malformed),
                    "missing_publication_fields": ";".join(
                        audit.missing_publication_fields
                    ),
                    "unmapped_fields": ";".join(audit.unmapped_fields),
                    "length_stats": json.dumps(audit.length_stats),
                    "width_stats": json.dumps(audit.width_stats),
                    "q_stats": json.dumps(audit.q_stats),
                    "parmasto": json.dumps(audit.parmasto),
                }
            )


def _write_manifest_template(
    result: dict[str, Any],
    *,
    output: Path,
) -> None:
    audits: list[RowAudit] = result["audits"]
    manifest: dict[str, Any] = {
        "manifest_version": MANIFEST_VERSION,
        "generated_at": result["generated_at"],
        "database_path": result["database_path"],
        "notes": (
            "One entry per legacy reference_values row. Set 'action' to "
            "one of: migrate, attach_to_existing, skip, unresolved. For "
            "'migrate', set 'work_key' to a key defined in the top-level "
            "'works' array. Multiple rows may reference the same "
            "work_key to explicitly reuse a normalized publication. The "
            "migration script never fuses rows automatically."
        ),
        "defaults": {},
        "works": [],
        "rows": [],
    }
    for audit in audits:
        entry: dict[str, Any] = {
            "legacy_id": audit.legacy_id,
            "action": "already_migrated" if audit.already_migrated else "unresolved",
            "_suggestions": {
                "taxon": (
                    f"{audit.genus or ''} {audit.species or ''}".strip()
                ),
                "source_label": audit.source,
                "suggested_data_kind": audit.suggested_data_kind,
                "plotable": audit.plotable,
                "exact_duplicate_of": audit.exact_duplicate_of,
                "same_source_group": audit.same_source_group,
                "missing_publication_fields": audit.missing_publication_fields,
                "unmapped_fields": audit.unmapped_fields,
            },
        }
        if audit.already_migrated:
            entry["existing_normalized_ids"] = list(
                audit.existing_normalized_ids
            )
        else:
            entry["work_key"] = None
            entry["treatment"] = {
                "name_as_published": (
                    f"{audit.genus or ''} {audit.species or ''}".strip()
                    or None
                ),
                "taxon_id": None,
                "page_from": None,
                "page_to": None,
                "locator_text": None,
                "treatment_notes": None,
            }
            entry["measurement_set"] = {
                "data_kind": audit.suggested_data_kind,
                "raw_text": None,
                "notes": None,
                "measurement_method": None,
                "preparation": None,
            }
        manifest["rows"].append(entry)
    output.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=False)
        + "\n",
        encoding="utf-8",
    )


# --- CLI --------------------------------------------------------------------


def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Read-only audit of the legacy reference_values table. Emits a "
            "Markdown report, a CSV inventory, and a JSON migration-template "
            "manifest. Never modifies either database."
        )
    )
    p.add_argument(
        "--database",
        type=Path,
        default=None,
        help=(
            "Path to the reference_values.db to inspect. Defaults to the "
            "configured application reference database."
        ),
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory where the three output files are written.",
    )
    p.add_argument(
        "--markdown-name",
        type=str,
        default="legacy-reference-audit.md",
    )
    p.add_argument(
        "--csv-name",
        type=str,
        default="legacy-reference-audit.csv",
    )
    p.add_argument(
        "--manifest-name",
        type=str,
        default="legacy-reference-migration.json",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_argparser().parse_args(argv)
    database_path = args.database or get_reference_database_path()
    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    result = run_audit(database_path=Path(database_path))
    _write_markdown_report(result, output=output_dir / args.markdown_name)
    _write_csv_inventory(result, output=output_dir / args.csv_name)
    _write_manifest_template(result, output=output_dir / args.manifest_name)

    counts = _summary_counts(result["audits"])
    print(
        f"[audit] {counts['total_rows']} row(s) inspected — "
        f"{counts['already_migrated']} already migrated, "
        f"{counts['plotable']} plotable, "
        f"{counts['exact_duplicates']} with exact-duplicate candidates."
    )
    print(f"[audit] Markdown report: {output_dir / args.markdown_name}")
    print(f"[audit] CSV inventory:   {output_dir / args.csv_name}")
    print(f"[audit] Manifest template: {output_dir / args.manifest_name}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
