#!/usr/bin/env python3
"""Taxonomy v3 Stage 6P: the release delta, traced to approved records.

Compares a release candidate SQLite with the bundled release it replaces and
accounts for every difference. Stage 6P's gate: the delta lists only approved
bridges, supersessions and national display fields, and no Sporely id is
allocated or changed except by an approved supersession.

Every changed row must be explained by exactly one approved record; anything
left over is a violation.

* **Concepts.** No concept is new. Every concept that disappeared is retired
  by an approved ``concept_supersessions.yml`` record. A kept concept changes
  only its release id and the national preferred-name columns (whose
  provenance columns are the only added columns). A supersession survivor
  may gain an iNaturalist lookup id only if it is the lookup id of a concept
  retired onto it.
* **Provider bridges** (``taxon_external_id_text_min``). None is removed.
  Every added row is NorTaxa or Dyntaxa and is either an approved
  ``manual_mappings.yml`` record (``authoritative_bridge:manual_approved_exact``)
  bound by the registry to the row's concept, or an identifier the registry
  binds to a retired concept, emitted on that concept's survivor
  (``authoritative_bridge:reviewed_supersession``).
* **National names.** Every ``preferred_scientific_name_no``/``_sv`` cites,
  as its provenance, an authoritative bridge row on the same concept.
* **Names, vernaculars, legacy ids and the red list.** Each baseline row of a
  retired concept must reach *that concept's* survivor, in one of these forms:

  - unchanged but for the concept (``rekeyed_exact``);
  - with ``;superseded_from:<retired>;supersession:<id>`` appended to its
    note, naming that retired concept and its supersession
    (``rekeyed_with_supersession_note``);
  - for the retired concept's own accepted NorTaxa integer id, as a
    non-preferred ``reviewed_supersession`` row, when the registry binds
    that NorTaxa id to the retired concept (``rekeyed_accepted_id``);
  - for the retired concept's preferred scientific name, likewise as a
    non-preferred ``reviewed_supersession`` row (``rekeyed_accepted_name``);
  - for a scientific name or vernacular, failing the forms above, collapsed
    into a row the survivor already carries with the same language and
    spelling (``collapsed_into_survivor_name``).

  A row that reaches none of these is lost or misdirected. On a kept concept
  a row may only be *relabelled*: the same row with its note changed from
  ``cross_source_automatic_exact`` to ``manual_approved_exact`` because an
  approved mapping now bridges that concept. After those are paired off, an
  added row must be a Dyntaxa scientific name or Swedish vernacular on a
  concept with an approved Dyntaxa bridge (Stage 4P); any other addition is
  unexplained.

Read-only over both SQLite files, the committed registry and the policies.
Output is byte-deterministic.

    .venv/bin/python database/taxonomy/evidence/taxonomy-v3/audit_stage6p_release.py \\
        --baseline <tax-2026.09.26-02.sqlite3> --candidate <candidate.sqlite3> \\
        --output database/taxonomy/evidence/taxonomy-v3/stage6p/release-delta.json
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path

import yaml

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parents[3]
_TAXONOMY = _REPO / "database" / "taxonomy"
REGISTRY_DIR = _TAXONOMY / "registry" / "canonical"
MANUAL_MAPPINGS = _TAXONOMY / "policies" / "manual_mappings.yml"
SUPERSESSIONS = _TAXONOMY / "policies" / "concept_supersessions.yml"

NATIONAL_SOURCES = ("nortaxa", "dyntaxa")
NATIONAL_PROVENANCE_COLUMNS = {
    f"preferred_scientific_name_{suffix}_{part}"
    for suffix in ("no", "sv") for part in ("source_system", "namespace", "external_id")
}
KEPT_TAXON_COLUMNS_THAT_MAY_CHANGE = {
    "sporely_content_release_id", "preferred_scientific_name_no", "preferred_scientific_name_sv",
} | NATIONAL_PROVENANCE_COLUMNS
AUTOMATIC_NOTE = "cross_source_automatic_exact"
APPROVED_NOTE = "manual_approved_exact"
MANUAL_BRIDGE = "authoritative_bridge:manual_approved_exact"
SUPERSESSION_BRIDGE = "authoritative_bridge:reviewed_supersession"
# Row content without the surrogate row id, per compared table.
CONTENT_COLUMNS = {
    "scientific_name_min": ("taxon_id", "language_code", "scientific_name", "is_preferred_name",
                            "source", "note"),
    "vernacular_min": ("taxon_id", "language_code", "vernacular_name", "is_preferred_name", "source"),
    "taxon_external_id_min": ("taxon_id", "source_system", "external_id", "id_role", "is_preferred",
                              "external_name", "note"),
    "taxon_redlist_min": ("taxon_id", "source_system", "source_release", "assessment_id",
                          "assessment_area", "assessed_name_source", "assessed_name_namespace",
                          "assessed_name_id", "scientific_name_snapshot", "authorship_snapshot",
                          "taxon_rank_snapshot", "category_raw", "category_code",
                          "category_is_downgraded", "criteria", "expert_group", "assessment_url"),
}
# Tables whose rows may collapse into a survivor's row of the same spelling.
NAME_TABLES = ("scientific_name_min", "vernacular_min")
# Stage 4P additions: (table, source) -> the language codes Dyntaxa may add.
NATIONAL_ADDITIONS = {("scientific_name_min", "dyntaxa"): {"sci"}, ("vernacular_min", "dyntaxa"): {"sv"}}
TEXT_ID_COLUMNS = ("taxon_id", "source_system", "namespace", "external_id", "id_role", "is_preferred",
                   "external_name", "note")


@dataclass
class Policies:
    """The approved records the delta is traced to."""
    registry: dict[tuple[str, str, str], int]
    approved_usages: set[tuple[str, str, str]]
    # retired sporely_taxon_id -> (survivor sporely_taxon_id, supersession_id)
    supersessions: dict[int, tuple[int, str]]
    fingerprints: dict[str, str] = field(default_factory=dict)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def load_registry(directory: Path) -> tuple[dict[tuple[str, str, str], int], str]:
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    binding: dict[tuple[str, str, str], int] = {}
    for shard in manifest["shards"]:
        with (directory / shard["name"]).open(encoding="utf-8") as handle:
            for line in handle:
                entry = json.loads(line)
                if entry.get("__registry_header__"):
                    continue
                binding[(entry["source"], entry["namespace"], str(entry["identifier"]))] = \
                    int(entry["sporely_taxon_id"])
    return binding, manifest["concatenated_sha256"]


def load_policies() -> Policies:
    registry, registry_sha = load_registry(REGISTRY_DIR)
    mappings = yaml.safe_load(MANUAL_MAPPINGS.read_text(encoding="utf-8"))["mappings"]
    ledger = yaml.safe_load(SUPERSESSIONS.read_text(encoding="utf-8"))["supersessions"]
    approved_usages = {
        (m["source_usage"]["source"], m["source_usage"]["namespace"], str(m["source_usage"]["identifier"]))
        for m in mappings if m.get("review_status") == "approved"
    }
    supersessions: dict[int, tuple[int, str]] = {}
    for record in ledger:
        if record.get("review_status") != "approved":
            continue
        usage = record["current_source_usage"]
        survivor = registry.get((usage["source"], usage["namespace"], str(usage["identifier"])))
        if survivor is None:
            raise SystemExit(f"supersession {record['supersession_id']}: survivor usage not in registry")
        supersessions[int(record["superseded_sporely_taxon_id"])] = (survivor, record["supersession_id"])
    return Policies(registry, approved_usages, supersessions, {
        "committed_registry_sha256": registry_sha,
        "manual_mappings_sha256": sha256_file(MANUAL_MAPPINGS),
        "concept_supersessions_sha256": sha256_file(SUPERSESSIONS),
    })


def _rows(conn: sqlite3.Connection, table: str, columns: tuple[str, ...]) -> collections.Counter:
    return collections.Counter(conn.execute(f"SELECT {', '.join(columns)} FROM {table}"))


def _taxa(conn: sqlite3.Connection) -> tuple[list[str], dict[int, dict]]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM taxon_min").fetchall()
    conn.row_factory = None
    columns = list(rows[0].keys()) if rows else []
    return columns, {int(r["taxon_id"]): dict(r) for r in rows}


def _account_table(table: str, columns: tuple[str, ...], before: collections.Counter,
                   after: collections.Counter, policies: Policies, manual_bridged: set[int],
                   dyntaxa_bridged: set[int], violations: list[str]) -> dict:
    """Pair every removed and added row of one table with the record that explains it."""
    added, removed = after - before, before - after
    col = {name: i for i, name in enumerate(columns)}
    source_col = col.get("source_system", col.get("source"))
    note_col = col.get("note")
    counts: collections.Counter = collections.Counter()

    def consume(row: tuple, n: int) -> int:
        taken = min(n, added[row])
        added[row] -= taken
        return taken

    names_on = set()
    if table in NAME_TABLES:
        name_col = col.get("scientific_name", col.get("vernacular_name"))
        names_on = {(r[0], r[col["language_code"]], r[name_col]) for r in after}

    for row, n in sorted(removed.items(), key=repr):
        taxon_id = row[0]
        if taxon_id in policies.supersessions:
            survivor, supersession_id = policies.supersessions[taxon_id]
            moved = (survivor,) + row[1:]
            left = n
            taken = consume(moved, left)
            counts["rekeyed_exact"] += taken
            left -= taken
            if left and note_col is not None and row[note_col]:
                noted = list(moved)
                noted[note_col] = f"{row[note_col]};superseded_from:{taxon_id};supersession:{supersession_id}"
                taken = consume(tuple(noted), left)
                counts["rekeyed_with_supersession_note"] += taken
                left -= taken
            if (left and table == "taxon_external_id_min" and row[col["id_role"]] == "accepted"
                    and row[col["is_preferred"]] == 1 and row[note_col] is None
                    and policies.registry.get(("nortaxa", "nortaxa_taxon_id", str(row[col["external_id"]])))
                    == taxon_id):
                accepted = list(moved)
                accepted[col["is_preferred"]] = 0
                accepted[note_col] = "reviewed_supersession"
                taken = consume(tuple(accepted), left)
                counts["rekeyed_accepted_id"] += taken
                left -= taken
            if (left and table == "scientific_name_min" and row[col["is_preferred_name"]] == 1
                    and row[note_col] is None):
                accepted = list(moved)
                accepted[col["is_preferred_name"]] = 0
                accepted[note_col] = "reviewed_supersession"
                taken = consume(tuple(accepted), left)
                counts["rekeyed_accepted_name"] += taken
                left -= taken
            if left and table in NAME_TABLES and \
                    (survivor, row[col["language_code"]], row[name_col]) in names_on:
                counts["collapsed_into_survivor_name"] += left
                left = 0
            if left:
                counts["lost_or_misdirected"] += left
                violations.append(f"{table}: row of retired concept {taxon_id} does not reach its "
                                  f"survivor {survivor}: {row}")
            continue
        # A kept concept's row may only be relabelled automatic -> approved.
        left = n
        if note_col is not None and row[note_col] == AUTOMATIC_NOTE and taxon_id in manual_bridged:
            relabelled = row[:note_col] + (APPROVED_NOTE,) + row[note_col + 1:]
            taken = consume(relabelled, left)
            counts["relabelled_automatic_to_approved"] += taken
            left -= taken
        if left:
            counts["removed_on_kept_concepts"] += left
            violations.append(f"{table}: removed row on a kept concept {row}")

    added = +added
    national: collections.Counter = collections.Counter()
    for row, n in sorted(added.items(), key=repr):
        source = str(row[source_col])
        allowed = NATIONAL_ADDITIONS.get((table, source))
        language = row[col["language_code"]] if "language_code" in col else None
        if allowed is not None and language in allowed and row[0] in dyntaxa_bridged:
            national[f"{source}/{language}"] += n
            continue
        counts["unexplained_additions"] += n
        violations.append(f"{table}: added row without an approved relationship {row}")
    return {
        "baseline_rows": sum(before.values()), "candidate_rows": sum(after.values()),
        **{key: counts[key] for key in (
            "rekeyed_exact", "rekeyed_with_supersession_note", "rekeyed_accepted_id",
            "rekeyed_accepted_name", "collapsed_into_survivor_name", "relabelled_automatic_to_approved",
            "lost_or_misdirected", "removed_on_kept_concepts", "unexplained_additions")},
        "national_additions": dict(sorted(national.items())),
    }


def audit(baseline_path: Path, candidate_path: Path, policies: Policies | None = None) -> dict:
    policies = policies or load_policies()
    supersessions = policies.supersessions
    base = sqlite3.connect(f"file:{baseline_path}?mode=ro", uri=True)
    cand = sqlite3.connect(f"file:{candidate_path}?mode=ro", uri=True)
    meta = dict(cand.execute("SELECT key, value FROM taxonomy_meta"))
    base_meta = dict(base.execute("SELECT key, value FROM taxonomy_meta"))
    violations: list[str] = []

    # ---- concepts
    base_columns, base_taxa = _taxa(base)
    cand_columns, cand_taxa = _taxa(cand)
    added_columns = [c for c in cand_columns if c not in base_columns]
    if [c for c in cand_columns if c in base_columns] != base_columns \
            or not set(added_columns) <= NATIONAL_PROVENANCE_COLUMNS:
        violations.append(f"taxon_min columns changed beyond the national provenance columns: "
                          f"{base_columns} -> {cand_columns}")
    added_ids = sorted(set(cand_taxa) - set(base_taxa))
    removed_ids = sorted(set(base_taxa) - set(cand_taxa))
    for taxon_id in added_ids:
        violations.append(f"new concept {taxon_id}")
    unexplained_removed = [t for t in removed_ids if t not in supersessions]
    for taxon_id in unexplained_removed:
        violations.append(f"concept {taxon_id} removed without an approved supersession")
    for taxon_id in sorted(t for t in supersessions if t in cand_taxa):
        violations.append(f"retired concept {taxon_id} still emitted")
    inherited_lookup: dict[int, set] = collections.defaultdict(set)
    for retired, (survivor, _sid) in supersessions.items():
        value = base_taxa.get(retired, {}).get("inaturalist_taxon_id")
        if value is not None:
            inherited_lookup[survivor].add(value)
    changed_columns: collections.Counter = collections.Counter()
    for taxon_id in sorted(set(base_taxa) & set(cand_taxa)):
        before, after = base_taxa[taxon_id], cand_taxa[taxon_id]
        for column in cand_columns:
            if before.get(column) == after.get(column):
                continue
            changed_columns[column] += 1
            if column in KEPT_TAXON_COLUMNS_THAT_MAY_CHANGE:
                continue
            if (column == "inaturalist_taxon_id" and before.get(column) is None
                    and after.get(column) in inherited_lookup.get(taxon_id, ())):
                continue
            violations.append(f"concept {taxon_id}: {column} {before.get(column)!r} -> {after.get(column)!r}")

    # ---- provider bridges
    base_text = _rows(base, "taxon_external_id_text_min", TEXT_ID_COLUMNS)
    cand_text = _rows(cand, "taxon_external_id_text_min", TEXT_ID_COLUMNS)
    added_text = cand_text - base_text
    removed_text = base_text - cand_text
    added_bridges: collections.Counter = collections.Counter()
    for row in sorted(added_text.elements(), key=repr):
        taxon_id, source, namespace, external_id, _role, _pref, _name, note = row
        key = (source, namespace, str(external_id))
        added_bridges[(source, namespace, note)] += 1
        if source not in NATIONAL_SOURCES:
            violations.append(f"added non-national external id {row}")
        elif note == MANUAL_BRIDGE:
            if key not in policies.approved_usages:
                violations.append(f"bridge {key} has no approved manual mapping")
            elif policies.registry.get(key) != taxon_id:
                violations.append(f"bridge {key} on {taxon_id}, registry binds {policies.registry.get(key)}")
        elif note == SUPERSESSION_BRIDGE:
            retired = policies.registry.get(key)
            if retired not in supersessions or supersessions[retired][0] != taxon_id:
                violations.append(f"supersession bridge {key} on {taxon_id}: registry binds {retired}, "
                                  f"not a concept retired onto {taxon_id}")
        else:
            violations.append(f"bridge {key} with unexpected note {note!r}")
    for row in sorted(removed_text.elements(), key=repr):
        violations.append(f"removed external id row {row}")

    authoritative = {(r[0], r[1], r[2], str(r[3])) for r in cand_text
                     if str(r[7] or "").startswith("authoritative_bridge:")}
    manual_bridged = {r[0] for r in cand_text if r[7] == MANUAL_BRIDGE}
    dyntaxa_bridged = {r[0] for r in cand_text if r[1] == "dyntaxa" and r[7] == MANUAL_BRIDGE}

    # ---- national display names
    national: dict[str, dict] = {}
    for suffix in ("no", "sv"):
        column = f"preferred_scientific_name_{suffix}"
        filled = differs = 0
        for taxon_id, row in cand_taxa.items():
            name = row.get(column)
            if not name:
                continue
            filled += 1
            if name != row.get("canonical_scientific_name"):
                differs += 1
            provenance = (taxon_id, row.get(f"{column}_source_system"), row.get(f"{column}_namespace"),
                          str(row.get(f"{column}_external_id")))
            if provenance not in authoritative:
                violations.append(f"{column} on {taxon_id} cites {provenance[1:]}, not an authoritative "
                                  "bridge on that concept")
        baseline_filled = sum(1 for row in base_taxa.values() if row.get(column))
        national[suffix] = {"baseline_filled": baseline_filled, "filled": filled,
                            "differs_from_canonical": differs}

    # ---- names, vernaculars, legacy ids, red list
    tables = {
        table: _account_table(table, columns, _rows(base, table, columns), _rows(cand, table, columns),
                              policies, manual_bridged, dyntaxa_bridged, violations)
        for table, columns in CONTENT_COLUMNS.items()
    }

    return {
        "format": "sporely-taxonomy-v3-stage6p-release-delta-v2",
        "baseline": {"content_release_id": base_meta.get("content_release_id"),
                     "sqlite_sha256": sha256_file(baseline_path)},
        "candidate": {"content_release_id": meta.get("content_release_id"),
                      "sqlite_sha256": sha256_file(candidate_path),
                      "registry_sha256": meta.get("registry_sha256"),
                      "taxonomy_schema_version": meta.get("taxonomy_schema_version")},
        "policies": dict(sorted(policies.fingerprints.items())),
        "concepts": {
            "baseline": len(base_taxa), "candidate": len(cand_taxa),
            "added": len(added_ids), "removed": len(removed_ids),
            "removed_by_approved_supersession": len(removed_ids) - len(unexplained_removed),
            "approved_supersessions": len(supersessions),
            "retired_before_baseline": sorted(t for t in supersessions if t not in base_taxa),
            "kept_concepts_changed_columns": dict(sorted(changed_columns.items())),
        },
        "provider_bridges": {
            "added": sum(added_text.values()), "removed": sum(removed_text.values()),
            "added_by_source_namespace_note": {f"{s}/{n}/{note}": c
                                               for (s, n, note), c in sorted(added_bridges.items())},
        },
        "national_preferred_scientific_names": national,
        "tables": tables,
        "violations": violations,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    report = audit(args.baseline, args.candidate)
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    print(f"violations: {len(report['violations'])}", file=sys.stderr)
    return 1 if report["violations"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
