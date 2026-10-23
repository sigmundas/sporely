#!/usr/bin/env python3
"""Source-continuity receipt for a taxonomy refresh.

A reviewed relationship (approved manual mapping, concept supersession) was
approved against exact source releases. This script does NOT re-approve
anything and never edits an approval. It compares, for every source
identifier a reviewed record references, the previous and the current
normalized source output, and writes ONE continuity receipt for the release
being prepared. The receipt states, per record, whether every referenced
source record is semantically unchanged ("carried") or not ("flagged", with
reasons). The compiler (``--continuity-receipt``) then:

* applies an approved Dyntaxa mapping whose pins name the receipt's previous
  source releases only if the record is carried and the receipt's current
  pins are the compiled inputs (the approval's meaning is unchanged: the same
  source records, byte-for-byte in their reviewed semantics);
* refuses to compile while any NorTaxa mapping or supersession is flagged.

Unchanged means, for each referenced identifier (full namespace, no
stripping): it occurs exactly once in the current output; scientific name,
rank, taxonomic status, accepted-usage reference, parent reference and
kingdom are equal; authorship is equal or differs only typographically
(whitespace, and spacing around punctuation). Rejected records are never
carried (they are not applied anyway); candidate-only scopes are preserved
because the receipt never changes the approval record. An identifier absent
from both outputs is "absent_unchanged" (historical bindings stay as they
were). An identifier absent from the previous output but present now is
flagged: the review did not see it.

``col:sourceID`` churn is not handled here: a COL usage that disappears is
flagged ``missing``; ``prepare_refresh`` lists same-name/rank COL usages as
report-only candidates.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

sys.path.insert(0, str(Path(__file__).resolve().parent))

from compile_release import _iter_taxa  # noqa: E402

RECEIPT_FORMAT = "sporely-source-continuity-receipt-v1"
HOLDS_FORMAT = "sporely-source-continuity-holds-v1"


class HoldError(ValueError):
    pass


def load_holds(path: Path | None) -> dict[str, dict]:
    """Owner-decided holds: named records whose flagged relationship is
    quarantined instead of stopping the refresh. Each needs a reason and a
    decision; a hold never applies to any record it does not name."""
    if path is None:
        return {}
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    if doc.get("format") != HOLDS_FORMAT:
        raise HoldError("holds file has an unknown format")
    holds = {}
    for hold in doc.get("holds") or []:
        for key in ("record_id", "reason", "decided_by", "decided_at"):
            if not str(hold.get(key, "")).strip():
                raise HoldError(f"hold lacks {key}: {hold}")
        holds[hold["record_id"]] = hold
    return holds
COMPARED_FIELDS = ("scientific_name", "rank", "taxonomic_status", "accepted", "parent", "kingdom")


def typography_key(authorship: str) -> str:
    """Authorship with only whitespace/punctuation-spacing differences removed."""
    text = re.sub(r"\s+", " ", authorship or "").strip()
    return re.sub(r"\s*([.,&():])\s*", r"\1", text)


@dataclass(frozen=True)
class Snapshot:
    scientific_name: str
    authorship: str
    rank: str
    taxonomic_status: str
    accepted: str
    parent: str
    kingdom: str

    @classmethod
    def of(cls, record) -> "Snapshot":
        def ref(value):
            return f"{value.get('namespace')}:{value.get('value')}" if value else ""
        name = record.scientific_name
        if record.authorship and name.endswith(" " + record.authorship):
            name = name[: -len(record.authorship) - 1]
        return cls(name, record.authorship, record.rank.casefold(), record.taxonomic_status,
                   ref(record.accepted_name_usage_id), ref(record.parent_name_usage_id),
                   record.kingdom())


def load_index(source_dirs: Iterable[Path], wanted: set[tuple[str, str, str]]) -> dict:
    """``(source, namespace, id) -> [Snapshot, ...]`` for the wanted identifiers only."""
    index: dict[tuple[str, str, str], list[Snapshot]] = {}
    sources = {key[0] for key in wanted}
    for directory in source_dirs:
        for record in _iter_taxa(directory):
            if record.source_code not in sources:
                break
            key = (record.source_code, record.taxon_id_namespace, record.taxon_id_value)
            if key in wanted:
                index.setdefault(key, []).append(Snapshot.of(record))
    return index


def compare_identifier(previous: list[Snapshot], current: list[Snapshot]) -> list[str]:
    """Reasons an identifier is not semantically unchanged; [] when it is."""
    if not previous and not current:
        return []
    if not previous:
        return ["absent_in_reviewed_release"]
    if not current:
        return ["missing"]
    if len(current) > 1 or len(previous) > 1:
        return ["ambiguous"]
    old, new = previous[0], current[0]
    reasons = [f"{name}_changed" for name in COMPARED_FIELDS if getattr(old, name) != getattr(new, name)]
    if old.authorship != new.authorship and typography_key(old.authorship) != typography_key(new.authorship):
        reasons.append("authorship_changed")
    return reasons


def reviewed_records(manual_mappings: dict, supersessions: dict, anchors: dict[int, tuple]) -> list[dict]:
    """Every approved record with the source identifiers it references."""
    manifest_sha = lambda entry: str((entry.get("approved_manifest") or {}).get("file_sha256") or "")
    out = []
    for entry in manual_mappings.get("mappings") or []:
        if entry.get("review_status") != "approved":
            continue
        su = entry["source_usage"]
        ids = [(su["source"], su["namespace"], su["identifier"])]
        target = (entry.get("target") or {}).get("source_usage")
        if target:
            ids.append((target["source"], target["namespace"], target["identifier"]))
        out.append({"record_id": entry["mapping_id"], "kind": f"{su['source']}_mapping",
                    "approval_sha256": manifest_sha(entry), "identifiers": ids})
    for entry in supersessions.get("supersessions") or []:
        if entry.get("review_status") != "approved":
            continue
        cu = entry["current_source_usage"]
        ids = [(cu["source"], cu["namespace"], cu["identifier"])]
        retired = anchors.get(int(entry["superseded_sporely_taxon_id"]))
        if retired:
            ids.append(retired)
        out.append({"record_id": entry["supersession_id"], "kind": "supersession",
                    "approval_sha256": manifest_sha(entry), "identifiers": ids})
    return out


def registry_anchors(registry_jsonl: Path) -> dict[int, tuple[str, str, str]]:
    anchors: dict[int, tuple[str, str, str]] = {}
    with registry_jsonl.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if row.get("kind") == "anchor" or (row.get("role") == "anchor"):
                anchors[int(row["sporely_taxon_id"])] = (row["source"], row["namespace"], row["identifier"])
    return anchors


def build_receipt(*, release_id: str, previous_dirs: dict[str, Path], current_dirs: dict[str, Path],
                  previous_pins: dict, current_pins: dict, manual_mappings: dict, supersessions: dict,
                  anchors: dict[int, tuple], holds: dict[str, dict] | None = None) -> dict:
    records = reviewed_records(manual_mappings, supersessions, anchors)
    holds = holds or {}
    unknown = sorted(set(holds) - {r["record_id"] for r in records})
    if unknown:
        raise HoldError(f"holds name records that are not approved reviewed records: {unknown}")
    wanted = {identifier for record in records for identifier in record["identifiers"]}
    old = load_index(previous_dirs.values(), wanted)
    new = load_index(current_dirs.values(), wanted)
    per_identifier = {identifier: compare_identifier(old.get(identifier, []), new.get(identifier, []))
                      for identifier in sorted(wanted)}
    out_records = {}
    for record in sorted(records, key=lambda r: r["record_id"]):
        reasons = sorted({f"{'/'.join(i)}:{reason}" for i in record["identifiers"]
                          for reason in per_identifier[tuple(i)]})
        out_records[record["record_id"]] = {
            "kind": record["kind"], "approval_sha256": record["approval_sha256"],
            "identifiers": ["/".join(i) for i in record["identifiers"]],
            "status": "flagged" if reasons else "carried", "reasons": reasons}
        hold = holds.get(record["record_id"])
        if hold is not None:
            if not reasons:
                raise HoldError(f"hold on {record['record_id']} is stale: the record is carried")
            out_records[record["record_id"]].update(status="held", hold=hold)
    carried = sorted(rid for rid, r in out_records.items() if r["status"] == "carried")
    summary: dict[str, dict[str, int]] = {}
    for record in out_records.values():
        bucket = summary.setdefault(record["kind"], {"carried": 0, "flagged": 0, "held": 0})
        bucket[record["status"]] += 1
    return {
        "format": RECEIPT_FORMAT,
        "release_id": release_id,
        "semantics": ("Continuity evidence only. A carried record's referenced source records are "
                      "unchanged between the pinned previous and current source releases; the approval "
                      "itself is unchanged and is not extended to any other record."),
        "previous_pins": previous_pins,
        "current_pins": current_pins,
        "summary": summary,
        "carried_sha256": hashlib.sha256(json.dumps(carried).encode()).hexdigest(),
        "records": out_records,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--previous", action="append", default=[], help="code=normalized dir (reviewed release)")
    parser.add_argument("--current", action="append", default=[], help="code=normalized dir (this release)")
    parser.add_argument("--previous-pins", type=Path, required=True)
    parser.add_argument("--current-pins", type=Path, required=True)
    parser.add_argument("--manual-mappings", type=Path, required=True)
    parser.add_argument("--concept-supersessions", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True, help="assembled registry jsonl")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--holds", type=Path, default=None, help="owner-decided holds file")
    args = parser.parse_args(argv)
    pairs = lambda values: {v.split("=", 1)[0]: Path(v.split("=", 1)[1]) for v in values}
    receipt = build_receipt(
        release_id=args.release_id, previous_dirs=pairs(args.previous), current_dirs=pairs(args.current),
        previous_pins=json.loads(args.previous_pins.read_text()), current_pins=json.loads(args.current_pins.read_text()),
        manual_mappings=json.loads(args.manual_mappings.read_text()),
        supersessions=json.loads(args.concept_supersessions.read_text()),
        anchors=registry_anchors(args.registry), holds=load_holds(args.holds))
    args.output.write_text(json.dumps(receipt, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
    print(json.dumps(receipt["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
