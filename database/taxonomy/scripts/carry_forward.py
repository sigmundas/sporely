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

from compile_release import _iter_taxa, is_aggregate_name  # noqa: E402

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
        # A hold names one reviewed record (record_id) or one source usage
        # whose registry binding would change (source_usage, "src/ns/id").
        target = hold.get("record_id") or hold.get("source_usage")
        for key, value in (("record_id or source_usage", target), ("reason", hold.get("reason")),
                           ("decided_by", hold.get("decided_by")), ("decided_at", hold.get("decided_at"))):
            if not str(value or "").strip():
                raise HoldError(f"hold lacks {key}: {hold}")
        if hold.get("record_id") and hold.get("source_usage"):
            raise HoldError(f"hold names both a record and a usage: {hold}")
        holds[target] = hold
    return holds


def load_continuity_policy(path: Path | None) -> tuple[set[str], dict[str, dict]]:
    """Named continuity rules and identifier-specific accepted changes from the holds file."""
    if path is None:
        return set(), {}
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    rules = set(doc.get("named_rules") or [])
    if rules - NAMED_RULES:
        raise HoldError(f"unknown continuity rules {sorted(rules - NAMED_RULES)}")
    exceptions = {}
    for entry in doc.get("accepted_changes") or []:
        for key in ("identifier", "field", "from", "to", "reason", "decided_by", "decided_at"):
            if key not in entry:
                raise HoldError(f"accepted change lacks {key}: {entry}")
        exceptions[entry["identifier"]] = entry
    return rules, exceptions


def load_nortaxa_fallback(path: Path | None) -> list[dict]:
    """Owner-approved NorTaxa fallbacks for vanished COL anchors (same file)."""
    if path is None:
        return []
    return list(json.loads(Path(path).read_text(encoding="utf-8")).get("nortaxa_fallback") or [])


def load_id_continuity(path: Path | None) -> dict[tuple[str, str, str], dict]:
    """Category-1 COL id continuity (documented one-to-one replacements)."""
    if path is None:
        return {}
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    return {("col_xr", "col_usage_id", e["old_col_id"]): e for e in doc.get("continuity") or []}
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


INFRASPECIFIC_MARKERS = {"var.", "f.", "subsp.", "ssp.", "subvar.", "subf."}
NAMED_RULES = {"icn_autonym_authorship_omitted"}


def is_autonym(name: str) -> bool:
    """ICN Art. 26.1: the final infraspecific epithet repeats the species epithet."""
    tokens = name.split()
    return (len(tokens) >= 4 and tokens[-2] in INFRASPECIFIC_MARKERS and tokens[-1] == tokens[1])


def accepted_change(previous: Snapshot, current: Snapshot, reasons: list[str], identifier: str,
                    rules: set[str], exceptions: dict[str, dict]) -> str | None:
    """The named rule or listed owner exception that carries this one change, or None.

    Only a single changed field is ever carried this way; anything else stays flagged.
    """
    if reasons == ["authorship_changed"] and "icn_autonym_authorship_omitted" in rules \
            and not current.authorship and is_autonym(current.scientific_name):
        return "icn_autonym_authorship_omitted"
    exception = exceptions.get(identifier)
    if exception and len(reasons) == 1:
        field = exception["field"]
        if reasons == [f"{field}_changed"] and getattr(previous, field) == exception["from"] \
                and getattr(current, field) == exception["to"]:
            return exception["reason"]
    return None


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


def anchor_meaning_changes(old: dict, new: dict, anchors: dict[int, tuple],
                           reviewed_usages: Iterable[tuple] = ()) -> tuple[dict, list[dict]]:
    """Bound usages whose own source record changed meaning in place.

    * species -> aggregate (a name gaining coll./agg./s.lat./group): the
      Sporely id must never take the aggregate meaning. For a registry anchor
      or a reviewed usage of a non-backbone source the record is
      ``species_history``: the compile keeps the binding on the species by
      using the previous (reviewed) source record and its vernaculars, so the
      historical binding is never reused for the aggregate. A backbone usage
      is flagged for review.
    * name swaps: two anchored usages exchange their names (report only).
    """
    records: dict[str, dict] = {}
    by_old_name: dict[tuple[str, str], tuple] = {}
    bound = {usage: taxon for taxon, usage in anchors.items()}
    for usage in reviewed_usages:
        bound.setdefault(tuple(usage), None)
    for taxon, usage in anchors.items():
        before = old.get(usage, [])
        if len(before) == 1:
            by_old_name[(usage[0], before[0].scientific_name)] = (taxon, usage)
    for usage, taxon in sorted(bound.items(), key=lambda kv: kv[0]):
        before, after = old.get(usage, []), new.get(usage, [])
        if len(before) != 1 or len(after) != 1:
            continue
        a, b = before[0], after[0]
        if not is_aggregate_name(a.scientific_name) and is_aggregate_name(b.scientific_name):
            key = "/".join(usage)
            backbone = usage[0] == "col_xr"
            records[f"aggregate-meaning-change:{key}"] = {
                "kind": "aggregate_meaning_change", "approval_sha256": "", "identifiers": [key],
                "sporely_taxon_id": taxon,
                "status": "flagged" if backbone else "species_history",
                "reasons": [f"{key}:scientific_name_changed:{a.scientific_name} -> {b.scientific_name}"]
                           + ([f"{key}:taxonomic_status_changed:{a.taxonomic_status} -> {b.taxonomic_status}"]
                              if a.taxonomic_status != b.taxonomic_status else []),
                "resolution": ({"reason": "species_to_aggregate_meaning_change", "decided_by": "continuity rule",
                                "decision": "A backbone usage cannot keep its history; review."} if backbone else
                               {"reason": "species_to_aggregate_meaning_change", "decided_by": "continuity rule",
                                "decision": "The Sporely id keeps its species meaning: the compile uses the previous "
                                            "reviewed source record and its vernaculars for this usage. The "
                                            "aggregate is not bound to it; it is a concept only under its own "
                                            "source id."})}
    swaps = []
    for taxon, usage in sorted(anchors.items()):
        before, after = old.get(usage, []), new.get(usage, [])
        if len(before) != 1 or len(after) != 1 or before[0].scientific_name == after[0].scientific_name:
            continue
        other = by_old_name.get((usage[0], after[0].scientific_name))
        if other and other[0] > taxon:
            o_after = new.get(other[1], [])
            if len(o_after) == 1 and o_after[0].scientific_name == before[0].scientific_name:
                swaps.append({"sporely_taxon_ids": [taxon, other[0]],
                              "usages": ["/".join(usage), "/".join(other[1])],
                              "names_before": [before[0].scientific_name, after[0].scientific_name],
                              "same_authorship": before[0].authorship == after[0].authorship})
    return records, swaps


def load_previous_rows(previous_dirs: dict[str, Path], usages: set[tuple]) -> dict[tuple, tuple[dict, list[dict]]]:
    """Raw previous normalized taxa row and its vernacular rows for a few usages."""
    out: dict[tuple, tuple[dict, list[dict]]] = {}
    for source in {u[0] for u in usages}:
        directory = previous_dirs.get(source)
        if directory is None:
            continue
        cores: dict[tuple[str, str], tuple] = {}
        with (directory / "taxa.jsonl").open(encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                key = (row["source_code"], row["taxon_id"]["namespace"], row["taxon_id"]["value"])
                if key in usages:
                    out[key] = (row, [])
                    cores[(row["core_row_id"]["namespace"], row["core_row_id"]["value"])] = key
        vernacular = directory / "vernacular.jsonl"
        if cores and vernacular.is_file():
            with vernacular.open(encoding="utf-8") as handle:
                for line in handle:
                    row = json.loads(line)
                    key = cores.get((row["core_row_id"]["namespace"], row["core_row_id"]["value"]))
                    if key is not None:
                        out[key][1].append(row)
    return out


def continuity_key(record) -> tuple[str, str, str, str]:
    """Name, rank, normalized authorship and family: the identity a renumbered
    COL usage must share with its vanished predecessor."""
    return (record.scientific_name, record.rank.casefold(), typography_key(record.authorship),
            (record.classification or {}).get("family", ""))


def col_continuity_candidates(previous_dir: Path, current_dir: Path, anchors: dict[int, tuple]) -> dict:
    """Vanished COL registry anchors and the new, unanchored COL usages that
    match them on name, rank, normalized authorship and family. Report and
    review input only: the compile never allocates a new id for a matched new
    usage unless an approved continuity binds it to the old id."""
    col_anchor = {usage[2]: taxon for taxon, usage in anchors.items() if usage[0] == "col_xr"}
    excluded_path = current_dir / "excluded_by_policy.jsonl"
    excluded = ({json.loads(line)["col_usage_id"] for line in excluded_path.open(encoding="utf-8")}
                if excluded_path.is_file() else set())
    current: dict[tuple, list[str]] = {}
    present: set[str] = set()
    for record in _iter_taxa(current_dir):
        present.add(record.taxon_id_value)
        if record.taxon_id_value not in col_anchor:
            current.setdefault(continuity_key(record), []).append(record.taxon_id_value)
    vanished: dict[str, dict] = {}
    by_key: dict[tuple, list[str]] = {}
    for record in _iter_taxa(previous_dir):
        value = record.taxon_id_value
        if value in col_anchor and value not in present and value not in excluded:
            key = continuity_key(record)
            vanished[value] = {"sporely_taxon_id": col_anchor[value], "scientific_name": record.scientific_name,
                               "authorship": record.authorship, "rank": record.rank,
                               "family": key[3]}
            by_key.setdefault(key, []).append(value)
    pairs, ambiguous = [], []
    for key, olds in sorted(by_key.items()):
        news = sorted(current.get(key, []))
        if not news:
            continue
        entry = {"old_col_ids": sorted(olds), "new_col_ids": news, "scientific_name": key[0], "rank": key[1]}
        (pairs if len(olds) == 1 and len(news) == 1 else ambiguous).append(entry)
    return {"vanished_anchors": vanished,
            "pairs": [{"old_col_id": e["old_col_ids"][0], "new_col_id": e["new_col_ids"][0],
                       "sporely_taxon_id": col_anchor[e["old_col_ids"][0]], "scientific_name": e["scientific_name"],
                       "rank": e["rank"]} for e in pairs],
            "ambiguous": ambiguous}


def build_receipt(*, release_id: str, previous_dirs: dict[str, Path], current_dirs: dict[str, Path],
                  previous_pins: dict, current_pins: dict, manual_mappings: dict, supersessions: dict,
                  anchors: dict[int, tuple], holds: dict[str, dict] | None = None,
                  rules: set[str] | None = None, exceptions: dict[str, dict] | None = None,
                  id_continuity: dict[tuple[str, str, str], dict] | None = None,
                  nortaxa_fallback: list[dict] | None = None) -> dict:
    records = reviewed_records(manual_mappings, supersessions, anchors)
    holds = holds or {}
    usage_holds = {k: v for k, v in holds.items() if v.get("source_usage")}
    holds = {k: v for k, v in holds.items() if not v.get("source_usage")}
    unknown = sorted(set(holds) - {r["record_id"] for r in records})
    if unknown:
        raise HoldError(f"holds name records that are not approved reviewed records: {unknown}")
    wanted = {identifier for record in records for identifier in record["identifiers"]}
    id_continuity = id_continuity or {}
    continued = {identifier: ("col_xr", "col_usage_id", id_continuity[identifier]["new_col_id"])
                 for identifier in wanted if identifier in id_continuity}
    anchor_usages = set(anchors.values())
    old = load_index(previous_dirs.values(), wanted | anchor_usages)
    new = load_index(current_dirs.values(), wanted | set(continued.values()) | anchor_usages)
    meaning_records, name_swaps = anchor_meaning_changes(
        old, new, anchors, (i for i in wanted if i[0] != "col_xr"))
    history = {tuple(r["identifiers"][0].split("/", 2)) for r in meaning_records.values()
               if r["status"] == "species_history"}
    previous_rows = load_previous_rows(previous_dirs, history)
    for record in meaning_records.values():
        usage = tuple(record["identifiers"][0].split("/", 2))
        if usage in history:
            record["previous_record"], record["previous_vernaculars"] = previous_rows.get(usage, (None, []))
    per_identifier, notes = {}, {}
    for identifier in sorted(wanted):
        current = new.get(continued.get(identifier, identifier), [])
        if identifier in continued and new.get(identifier):
            current = []  # both present: not a replacement
        reasons = compare_identifier(old.get(identifier, []), current)
        if identifier in continued and not reasons:
            notes[identifier] = f"col_id_continuity:{continued[identifier][2]}"
        if identifier in history:
            # Species history is kept: the compile uses the previous reviewed record.
            notes[identifier] = "species_history_carried"
            reasons = []
        accepted = (accepted_change(old[identifier][0], current[0], reasons, "/".join(identifier),
                                    rules or set(), exceptions or {})
                    if reasons and len(old.get(identifier, [])) == 1 and len(current) == 1 else None)
        if accepted:
            notes[identifier] = accepted
            reasons = []
        per_identifier[identifier] = reasons
    out_records = {}
    for record in sorted(records, key=lambda r: r["record_id"]):
        reasons = sorted({f"{'/'.join(i)}:{reason}" for i in record["identifiers"]
                          for reason in per_identifier[tuple(i)]})
        out_records[record["record_id"]] = {
            "kind": record["kind"], "approval_sha256": record["approval_sha256"],
            "identifiers": ["/".join(i) for i in record["identifiers"]],
            "status": "flagged" if reasons else "carried", "reasons": reasons}
        record_notes = sorted(f"{'/'.join(i)}:{notes[tuple(i)]}" for i in record["identifiers"] if tuple(i) in notes)
        if record_notes:
            out_records[record["record_id"]]["carried_by"] = record_notes
        hold = holds.get(record["record_id"])
        if hold is not None:
            if not reasons:
                raise HoldError(f"hold on {record['record_id']} is stale: the record is carried")
            out_records[record["record_id"]].update(status="held", hold=hold)
    for rid, record in meaning_records.items():
        # A hold on a reviewed record already withholds the same usage.
        if not any(r["status"] == "held" and set(record["identifiers"]) & set(r["identifiers"])
                   for r in out_records.values()):
            out_records[rid] = record
    carried = sorted(rid for rid, r in out_records.items() if r["status"] == "carried")
    summary: dict[str, dict[str, int]] = {}
    for record in out_records.values():
        bucket = summary.setdefault(record["kind"], {"carried": 0, "flagged": 0, "held": 0})
        bucket[record["status"]] = bucket.get(record["status"], 0) + 1
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
        # Source usages whose registry binding the compile must not change
        # (owner holds); every other alias conflict still stops the compile.
        "held_alias_conflicts": {usage: hold for usage, hold in sorted(usage_holds.items())},
        # Category-1 COL id continuity: the new usage joins the existing Sporely id.
        "col_id_continuity": [id_continuity[k] for k in sorted(id_continuity)],
        "records": out_records,
        # Owner-approved: vanished COL anchors whose id is kept from its bound,
        # valid NorTaxa record (the compile re-checks every condition).
        "nortaxa_fallback_approved": list(nortaxa_fallback or []),
        # Report only: anchored usages that exchanged names (orthographic variants).
        "name_swaps": name_swaps,
        # Vanished COL anchors and same-identity new usages (review input; the
        # compile withholds unapproved matched new usages instead of allocating).
        "col_continuity_candidates": (
            col_continuity_candidates(previous_dirs["col_xr"], current_dirs["col_xr"], anchors)
            if "col_xr" in previous_dirs and "col_xr" in current_dirs
            and previous_dirs["col_xr"] != current_dirs["col_xr"] else None),
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
