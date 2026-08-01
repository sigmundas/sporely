"""Offline transformer: authorised raw export → anonymised W2D snapshot.

Input: a JSONL file that an authorised operator produced via a read-only
export against a local disposable copy of the observations database (never
production). Each raw record is expected to contain the historical
taxonomy fields inventoried under
`database/taxonomy/reconciliation/snapshot/README.md` (id, artsdata_id,
artportalen_id, inaturalist_id, inaturalist_taxon_id, mushroomobserver_id,
ai_selected_service, ai_selected_taxon_id, ai_selected_scientific_name,
scientific_name_snapshot, taxon_rank_snapshot, genus, species,
common_name, species_guess, sporely_taxon_id, source_release, uncertain).

Output: an anonymised snapshot JSONL that conforms to the W2D input
contract (`database/taxonomy/docs/w2d-input-snapshot-contract.md`) and
that the reconciliation CLI can consume verbatim.

The transformer never opens a network connection, never reads
credentials, and never accepts a "production" source.

Determinism guarantees:

* input read in file order;
* signals within a record sorted by ``(source_system, namespace, external_id, origin_field)``;
* output records emitted in the input order (already deterministic when
  the source export is deterministic);
* JSON dumped with ``sort_keys=True``, ``ensure_ascii=False``.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

from .pseudonym import PseudonymKeyError, make_pseudonymiser
from .validator import (
    PROHIBITED_FIELD_NAMES,
    SCHEMA_VERSION,
    ValidationReport,
    validate_snapshot,
)


NBIC_PREFIX_RE = re.compile(r"^NBIC:(\d+)$", re.IGNORECASE)
INT_RE = re.compile(r"^\d+$")


@dataclass(frozen=True)
class TransformStats:
    input_records: int
    output_records: int
    skipped_records: int
    exact_signals: int
    text_signals: int
    raw_export_sha256: str
    snapshot_sha256: str
    schema_version: str


def _emit_signal(
    *,
    kind: str,
    source_system: str | None,
    namespace: str | None,
    external_id: str | None,
    origin_field: str,
    raw_value: Any,
    rule_id: str | None = None,
) -> dict[str, Any]:
    signal: dict[str, Any] = {
        "kind": kind,
        "source_system": source_system,
        "namespace": namespace,
        "external_id": None if external_id is None else str(external_id),
        "origin_field": origin_field,
        "raw_value": raw_value,
    }
    if rule_id is not None:
        signal["rule_id"] = rule_id
    return signal


def _derive_signals(raw: dict[str, Any]) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []

    def push_exact(source, namespace, ext, origin, raw_value, rule_id=None):
        signals.append(
            _emit_signal(
                kind="exact",
                source_system=source,
                namespace=namespace,
                external_id=str(ext),
                origin_field=origin,
                raw_value=raw_value,
                rule_id=rule_id,
            )
        )

    def push_text(source, origin, raw_value):
        signals.append(
            _emit_signal(
                kind="text-only",
                source_system=source,
                namespace=None,
                external_id=None,
                origin_field=origin,
                raw_value=raw_value,
            )
        )

    artsdata = raw.get("artsdata_id")
    if isinstance(artsdata, int) and artsdata > 0:
        push_exact("nortaxa", "nortaxa_taxon_id", artsdata, "observations.artsdata_id", artsdata, rule_id="desktop_artsdata_id_v1")

    artportalen = raw.get("artportalen_id")
    if isinstance(artportalen, int) and artportalen > 0:
        push_exact("artportalen", "artportalen_taxon_id", artportalen, "observations.artportalen_id", artportalen, rule_id="desktop_artportalen_id_v1")

    inat_taxon = raw.get("inaturalist_taxon_id")
    if isinstance(inat_taxon, int) and inat_taxon > 0:
        push_exact("inaturalist", "inaturalist_taxon_id", inat_taxon, "observations.inaturalist_taxon_id", inat_taxon, rule_id="desktop_inat_taxon_id_v1")

    service = raw.get("ai_selected_service")
    ai_taxon = raw.get("ai_selected_taxon_id")
    if isinstance(service, str) and isinstance(ai_taxon, str) and ai_taxon.strip():
        service_norm = service.strip().lower()
        if service_norm == "inat":
            service_norm = "inaturalist"
        elif service_norm == "nbic":
            service_norm = "nortaxa"
        value = ai_taxon.strip()
        m = NBIC_PREFIX_RE.match(value)
        if m:
            push_exact("nortaxa", "nortaxa_taxon_id", m.group(1), "observations.ai_selected_taxon_id", value, rule_id="nbic_prefix_v1")
        elif service_norm == "inaturalist" and INT_RE.match(value):
            push_exact("inaturalist", "inaturalist_taxon_id", value, "observations.ai_selected_taxon_id", value, rule_id="ai_service_inaturalist_v1")
        elif service_norm == "artsorakel" and INT_RE.match(value):
            push_exact("nortaxa", "nortaxa_taxon_id", value, "observations.ai_selected_taxon_id", value, rule_id="artsorakel_bare_int_v1")
        elif service_norm == "nortaxa" and INT_RE.match(value):
            push_exact("nortaxa", "nortaxa_taxon_id", value, "observations.ai_selected_taxon_id", value, rule_id="ai_service_nortaxa_v1")
        else:
            signals.append(
                _emit_signal(
                    kind="text-only",
                    source_system=service_norm,
                    namespace=None,
                    external_id=None,
                    origin_field="observations.ai_selected_taxon_id",
                    raw_value=value,
                    rule_id="ai_selected_taxon_id_unresolved_shape_v1",
                )
            )

    for f, source in (
        ("scientific_name_snapshot", "sporely_snapshot"),
        ("ai_selected_scientific_name", "artsorakel"),
        ("genus", "sporely_snapshot"),
        ("species", "sporely_snapshot"),
        ("common_name", "sporely_snapshot"),
        ("species_guess", "sporely_snapshot"),
    ):
        v = raw.get(f)
        if isinstance(v, str) and v.strip():
            push_text(source, f"observations.{f}", v.strip())

    signals.sort(
        key=lambda s: (
            s.get("source_system") or "",
            s.get("namespace") or "",
            s.get("external_id") or "",
            s.get("origin_field") or "",
        )
    )
    return signals


def _sanitise_prohibited(raw: dict[str, Any]) -> list[str]:
    prohibited_present = [k for k in raw.keys() if k.lower() in PROHIBITED_FIELD_NAMES]
    return sorted(prohibited_present)


def transform_record(raw: dict[str, Any], pseudonymise) -> tuple[dict[str, Any], list[str]]:
    """Transform a single raw export row into an anonymised snapshot record.

    Returns ``(record, prohibited_fields_seen)``. Prohibited fields are
    logged and dropped; they never reach the anonymised output.
    """

    prohibited = _sanitise_prohibited(raw)
    raw_id = raw.get("id")
    if raw_id is None or raw_id == "":
        raise ValueError("raw export row is missing the source id")
    pseudonym = pseudonymise(str(raw_id))

    record: dict[str, Any] = {
        "observation_id": pseudonym,
        "signals": _derive_signals(raw),
        "manual_identification_flag": bool(raw.get("manual_identification_flag") or raw.get("manual_name")),
        "stored_scientific_name": (raw.get("scientific_name_snapshot") or raw.get("ai_selected_scientific_name") or None),
        "stored_vernacular_name": raw.get("common_name") or None,
        "stored_rank": raw.get("taxon_rank_snapshot") or None,
        "source_release_or_timestamp": raw.get("source_release") or None,
    }
    return record, prohibited


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line_no, raw in enumerate(f, start=1):
            line = raw.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"line {line_no}: invalid JSON — {exc.msg}") from exc
            if not isinstance(doc, dict):
                raise ValueError(f"line {line_no}: expected a JSON object")
            yield doc


_INT_CSV_COLUMNS: frozenset[str] = frozenset(
    {
        "id",
        "artsdata_id",
        "artportalen_id",
        "inaturalist_id",
        "inaturalist_taxon_id",
        "mushroomobserver_id",
        "desktop_id",
        "sporely_taxon_id",
    }
)


def _coerce_csv_row(row: dict[str, str]) -> dict[str, Any]:
    coerced: dict[str, Any] = {}
    for k, v in row.items():
        if v is None:
            coerced[k] = None
            continue
        stripped = v.strip()
        if stripped == "" or stripped.upper() == "NULL":
            coerced[k] = None
            continue
        if k in _INT_CSV_COLUMNS:
            try:
                coerced[k] = int(stripped)
            except ValueError:
                coerced[k] = stripped
        else:
            coerced[k] = stripped
    return coerced


def _iter_csv(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError("CSV export has no header row")
        for line_no, row in enumerate(reader, start=2):
            yield _coerce_csv_row(row)


def _iter_raw_export(path: Path) -> Iterable[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        yield from _iter_csv(path)
    elif suffix in {".jsonl", ".ndjson", ".json"}:
        yield from _iter_jsonl(path)
    else:
        raise ValueError(
            f"unsupported raw-export format: {suffix!r}; expected .csv, .jsonl or .ndjson"
        )


def run_transform(
    *,
    raw_export: Path,
    output: Path,
    pseudonym_key_file: Path | None,
    strict: bool = True,
) -> TransformStats:
    """Read ``raw_export`` JSONL and write an anonymised snapshot JSONL to ``output``.

    Raises :class:`ValueError` on structural problems. On success writes:
    - ``<output>`` — anonymised snapshot JSONL with header line
    - ``<output>.sha256.txt`` — SHA-256 of the anonymised snapshot file
    - ``<output>.stats.json`` — deterministic stats sidecar
    """

    if not raw_export.is_file():
        raise ValueError(f"raw export not found: {raw_export}")
    if output.exists():
        raise ValueError(f"refusing to overwrite existing output: {output}")

    pseudonymise = make_pseudonymiser(pseudonym_key_file)
    raw_sha = _sha256_file(raw_export)

    output.parent.mkdir(parents=True, exist_ok=True)

    input_records = 0
    output_records = 0
    skipped = 0
    exact_signals = 0
    text_signals = 0
    prohibited_report: dict[str, int] = {}

    with output.open("w", encoding="utf-8") as out:
        header = {
            "__snapshot_header__": True,
            "schema_version": SCHEMA_VERSION,
            "raw_export_sha256": raw_sha,
            "transformer_module": "database.taxonomy.reconciliation.snapshot.transformer",
        }
        out.write(json.dumps(header, sort_keys=True, ensure_ascii=False) + "\n")

        for raw in _iter_raw_export(raw_export):
            input_records += 1
            try:
                record, prohibited = transform_record(raw, pseudonymise)
            except ValueError as exc:
                if strict:
                    raise
                skipped += 1
                continue
            for k in prohibited:
                prohibited_report[k] = prohibited_report.get(k, 0) + 1
            for s in record["signals"]:
                if s["kind"] == "exact":
                    exact_signals += 1
                else:
                    text_signals += 1
            out.write(json.dumps(record, sort_keys=True, ensure_ascii=False) + "\n")
            output_records += 1

    snapshot_sha = _sha256_file(output)
    (output.parent / (output.name + ".sha256.txt")).write_text(snapshot_sha + "\n")

    stats = TransformStats(
        input_records=input_records,
        output_records=output_records,
        skipped_records=skipped,
        exact_signals=exact_signals,
        text_signals=text_signals,
        raw_export_sha256=raw_sha,
        snapshot_sha256=snapshot_sha,
        schema_version=SCHEMA_VERSION,
    )
    stats_dict = asdict(stats)
    stats_dict["prohibited_fields_stripped"] = dict(sorted(prohibited_report.items()))
    (output.parent / (output.name + ".stats.json")).write_text(
        json.dumps(stats_dict, sort_keys=True, ensure_ascii=False, indent=2) + "\n"
    )
    return stats


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="W2D-R offline snapshot transformer / validator.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    t = sub.add_parser("transform", help="transform an authorised raw export into an anonymised snapshot")
    t.add_argument("--raw-export", type=Path, required=True)
    t.add_argument("--output", type=Path, required=True)
    t.add_argument("--pseudonym-key-file", type=Path, default=None)
    t.add_argument("--production", action="store_true", help="refused; kept only for explicit rejection")

    v = sub.add_parser("validate", help="validate an anonymised snapshot against the input contract")
    v.add_argument("--snapshot", type=Path, required=True)
    v.add_argument("--report", type=Path, default=None)

    args = parser.parse_args(argv)

    if args.cmd == "transform":
        if args.production:
            print("refuse: --production is not honoured; run only against a local disposable export", file=sys.stderr)
            return 3
        try:
            stats = run_transform(
                raw_export=args.raw_export,
                output=args.output,
                pseudonym_key_file=args.pseudonym_key_file,
            )
        except (PseudonymKeyError, ValueError) as exc:
            print(f"transform failed: {exc}", file=sys.stderr)
            return 4
        print(json.dumps(asdict(stats), sort_keys=True, ensure_ascii=False, indent=2))
        return 0

    if args.cmd == "validate":
        report = validate_snapshot(args.snapshot)
        payload = json.dumps(report.to_dict(), sort_keys=True, ensure_ascii=False, indent=2) + "\n"
        if args.report is not None:
            args.report.write_text(payload)
        print(payload, end="")
        return 0 if report.ok else 1

    return 2


if __name__ == "__main__":
    raise SystemExit(_main())
