#!/usr/bin/env python3
"""Read-only preview of what a dropped name association means for observations.

A release association (``taxon_association_min``) lets a national source usage
present itself as its primary concept. When a later release drops an
association (an ``exclusions`` entry in the name-association policy), any
observation bound to the primary *through that association* may have meant
the member. This tool finds those observations and proposes the identity they
recover; it never writes.

An observation is attributed to an association by its own stored evidence:

* a picker selection records ``name_association:<association_id>`` in
  ``taxon_identity_provenance``;
* a provider resolution records ``association=<association_id>`` in the same
  column and keeps the provider tuple (``taxon_identity_source_system`` /
  ``_namespace`` / ``_external_id``);
* a provider tuple equal to an association's member usage, bound to its
  primary, counts as well.

Observations bound to a member concept's own id are unaffected: that id is
live in both releases.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path

_PICKER = re.compile(r"(?:^|[;\s])name_association:(?P<id>[^\s;\]]+)")
_PROVIDER = re.compile(r"association=(?P<id>[^\s;\]]+)")


def _open_ro(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _associations(conn: sqlite3.Connection) -> dict[str, dict]:
    try:
        return {row["association_id"]: dict(row) for row in conn.execute("SELECT * FROM taxon_association_min")}
    except sqlite3.OperationalError:
        return {}


def _release_id(conn: sqlite3.Connection) -> str:
    row = conn.execute("SELECT value FROM taxonomy_meta WHERE key = 'content_release_id'").fetchone()
    return row[0] if row else ""


def _resolve_tuple(conn: sqlite3.Connection, source: str, namespace: str, external_id: str) -> set[int]:
    ids = {int(r[0]) for r in conn.execute(
        "SELECT DISTINCT taxon_id FROM taxon_external_id_text_min "
        "WHERE source_system = ? AND namespace = ? AND external_id = ?", (source, namespace, external_id))}
    if not ids and (source, namespace) == ("nortaxa", "nortaxa_taxon_id"):
        ids = {int(r[0]) for r in conn.execute(
            "SELECT taxon_id FROM taxon_min WHERE canonical_source_system = 'nortaxa' "
            "AND canonical_external_id = ?", (external_id,))}
    return ids


def attributed_association(obs: dict, by_usage: dict[tuple, dict]) -> tuple[str | None, str]:
    provenance = str(obs.get("taxon_identity_provenance") or "")
    match = _PROVIDER.search(provenance) or _PICKER.search(provenance)
    if match:
        return match.group("id"), "provenance"
    usage = (obs.get("taxon_identity_source_system"), obs.get("taxon_identity_namespace"),
             str(obs.get("taxon_identity_external_id") or ""))
    association = by_usage.get(usage)
    if association is not None and obs.get("sporely_taxon_id") == association["primary_taxon_id"]:
        return association["association_id"], "provider_tuple"
    return None, ""


def audit(observations_db: Path, old_release: Path, new_release: Path) -> dict:
    with _open_ro(old_release) as old, _open_ro(new_release) as new, _open_ro(observations_db) as obs_conn:
        old_assoc, new_assoc = _associations(old), _associations(new)
        by_usage = {(a["member_source_system"], a["member_namespace"], a["member_external_id"]): a
                    for a in old_assoc.values()}
        live_new = {int(r[0]) for r in new.execute("SELECT taxon_id FROM taxon_min")}
        dropped = {aid for aid, a in old_assoc.items()
                   if aid not in new_assoc or new_assoc[aid]["primary_taxon_id"] != a["primary_taxon_id"]}
        rows = obs_conn.execute(
            "SELECT id, genus, species, sporely_taxon_id, scientific_name_snapshot, taxon_identity_state, "
            "taxon_identity_proof, taxon_identity_source_system, taxon_identity_namespace, "
            "taxon_identity_external_id, taxon_identity_provenance FROM observations ORDER BY id").fetchall()
        counts = {"observations": len(rows), "attributed": 0, "affected_by_drop": 0,
                  "bound_to_member_unaffected": 0}
        affected = []
        members = {a["member_taxon_id"] for a in old_assoc.values() if a["member_taxon_id"] is not None}
        for row in rows:
            obs = dict(row)
            if obs["sporely_taxon_id"] in members:
                counts["bound_to_member_unaffected"] += 1
            association_id, how = attributed_association(obs, by_usage)
            if association_id is None:
                continue
            counts["attributed"] += 1
            if association_id not in dropped:
                continue
            a = old_assoc.get(association_id)
            if a is None or obs["sporely_taxon_id"] != a["primary_taxon_id"]:
                continue
            counts["affected_by_drop"] += 1
            if a["member_kind"] == "concept" and a["member_taxon_id"] in live_new:
                proposal = {"action": "rebind_to_member_concept", "sporely_taxon_id": a["member_taxon_id"]}
            else:
                source = obs["taxon_identity_source_system"] or a["member_source_system"]
                namespace = obs["taxon_identity_namespace"] or a["member_namespace"]
                external_id = str(obs["taxon_identity_external_id"] or a["member_external_id"])
                if source == "sporely":
                    source, namespace, external_id = (a["member_source_system"], a["member_namespace"],
                                                      a["member_external_id"])
                found = _resolve_tuple(new, source, namespace, external_id)
                proposal = ({"action": "rebind_by_provider_tuple", "sporely_taxon_id": next(iter(found))}
                            if len(found) == 1 else
                            {"action": "keep_provider_tuple_unresolved", "sporely_taxon_id": None})
                proposal["provider_tuple"] = [source, namespace, external_id]
            affected.append({
                "observation_id": obs["id"],
                "stored_name": obs["scientific_name_snapshot"] or f"{obs['genus'] or ''} {obs['species'] or ''}".strip(),
                "bound_sporely_taxon_id": obs["sporely_taxon_id"],
                "association_id": association_id,
                "attributed_by": how,
                "member": {k: a[k] for k in ("member_kind", "member_taxon_id", "member_source_system",
                                             "member_namespace", "member_external_id", "member_scientific_name")},
                "proposal": proposal,
            })
        return {
            "format": "sporely-association-reversal-preview-v1",
            "writes": "none",
            "old_release": _release_id(old),
            "new_release": _release_id(new),
            "dropped_associations": len(dropped),
            "counts": counts,
            "affected": affected,
        }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--observations", type=Path, required=True)
    parser.add_argument("--old-release", type=Path, required=True)
    parser.add_argument("--new-release", type=Path, required=True)
    args = parser.parse_args(argv)
    json.dump(audit(args.observations, args.old_release, args.new_release), sys.stdout,
              ensure_ascii=False, indent=1)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
