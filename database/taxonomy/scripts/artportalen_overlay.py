#!/usr/bin/env python3
"""Reviewed Artportalen publishing-ID overlay.

An Artportalen taxon id is target-specific publishing metadata: the id Sporely
sends when a user reports an observation to Artportalen. It is not evidence of
Sporely identity, and the runtime never derives it from a name. Concepts that
the release carries no Artportalen id for get one only through the committed
overlay ``database/taxonomy/overlays/artportalen-publishing.json``, and only
after a person has accepted the mapping.

Commands:

``propose``  Enumerate the concepts of a built candidate that lack an
             Artportalen id, look for Artportalen taxa with exactly the same
             scientific name in the legacy database, and classify each concept:

             * ``unique_exact`` — one Artportalen taxon has exactly the
               concept's name, no Artportalen name is a qualified variant of it
               (``s.lat.``, ``s.str.``, ``agg.``, ...), no other concept has
               the same name, and the Artportalen id is not attached to another
               concept;
             * ``split`` — Artportalen has qualified variants of the name, so
               the concept may correspond to more than one Artportalen taxon;
             * ``ambiguous_multiple_ids``, ``ambiguous_homonym``,
               ``ambiguous_id_in_use`` — anything else that is not one-to-one.

             Writes ``artportalen-proposals.json`` and a readable summary
             ``artportalen-proposals.md``. Nothing is accepted.

``accept``   Copy explicitly chosen proposals into the overlay: a whole class
             (``--class unique_exact``) or single decisions
             (``--entry SPORELY_ID:ARTPORTALEN_ID``), recording who accepted
             them. A single decision must name one of the concept's candidates.

``check``    Validate an overlay file.

The overlay is applied by ``build_sqlite_candidate.py --publishing-overlay``,
which refuses an entry whose concept is missing, whose name no longer matches,
or whose Artportalen id is already attached to another concept.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OVERLAY = REPO_ROOT / "database/taxonomy/overlays/artportalen-publishing.json"
OVERLAY_FORMAT = "sporely-publishing-id-overlay-v1"
PROPOSALS_FORMAT = "sporely-artportalen-overlay-proposals-v1"
TARGET = "artportalen"
SEMANTICS = ("Target-specific publishing metadata: the Artportalen taxon id to report "
             "for the Sporely concept. Not evidence of Sporely identity.")
CLASSES = ("unique_exact", "split", "ambiguous_multiple_ids", "ambiguous_homonym", "ambiguous_id_in_use")

# Qualifiers Artportalen appends to a name to mark a broader or narrower
# reading of the same taxon (``Amanita muscaria s.lat.``). Any other suffix
# (an epithet, ``var.``, ``subsp.``, a hybrid sign) names a different taxon.
VARIANT_QUALIFIERS = ("s.lat.", "s.str.", "s. lat.", "s. str.", "s.l.", "s.s.", "sensu lato",
                      "sensu stricto", "agg.", "aggr.", "coll.", "(grupp)", "grupp", "complex")
VARIANT_PREFIXES = ("morphotype ",)


class OverlayError(Exception):
    pass


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _dump(data: dict) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def name_relation(concept_name: str, artportalen_name: str) -> str | None:
    """``exact``, ``variant`` (qualified reading of the same name) or None."""
    if artportalen_name == concept_name:
        return "exact"
    prefix = concept_name + " "
    if artportalen_name.startswith(prefix):
        suffix = " ".join(artportalen_name[len(prefix):].split())
        if suffix in VARIANT_QUALIFIERS or suffix.startswith(VARIANT_PREFIXES):
            return "variant"
    return None


def _artportalen_taxa(legacy_db: Path) -> dict[str, set[int]]:
    """Artportalen scientific name -> Artportalen taxon ids, from the legacy DB."""
    conn = sqlite3.connect(f"file:{legacy_db}?mode=ro", uri=True)
    try:
        names: dict[str, set[int]] = defaultdict(set)
        for external_id, name in conn.execute(
                "SELECT DISTINCT external_id, external_name FROM taxon_external_id_min "
                "WHERE source_system = 'artportalen' AND external_name IS NOT NULL "
                "AND TRIM(external_name) <> ''"):
            names[str(name).strip()].add(int(external_id))
        return names
    finally:
        conn.close()


def propose(*, candidate: Path, legacy_db: Path) -> dict:
    artportalen = _artportalen_taxa(legacy_db)
    by_binomial_prefix: dict[str, list[str]] = defaultdict(list)
    for name in artportalen:
        # Index variants under every leading run of words so a concept name
        # finds "<name> s.lat." without scanning all Artportalen names.
        words = name.split()
        for i in range(1, len(words)):
            by_binomial_prefix[" ".join(words[:i])].append(name)

    conn = sqlite3.connect(f"file:{candidate}?mode=ro", uri=True)
    try:
        meta = dict(conn.execute("SELECT key, value FROM taxonomy_meta"))
        attached: dict[int, set[int]] = defaultdict(set)   # artportalen id -> concepts
        concepts_with_id: set[int] = set()
        for taxon_id, external_id in conn.execute(
                "SELECT taxon_id, external_id FROM taxon_external_id_min WHERE source_system='artportalen'"):
            attached[int(external_id)].add(int(taxon_id))
            concepts_with_id.add(int(taxon_id))
        name_counts: dict[str, int] = defaultdict(int)
        rows = conn.execute(
            "SELECT taxon_id, canonical_scientific_name, taxon_rank, taxonomic_status "
            "FROM taxon_min ORDER BY taxon_id").fetchall()
        concept_names = {taxon_id: name for taxon_id, name, _, _ in rows}
        for _, name, _, _ in rows:
            name_counts[name] += 1
    finally:
        conn.close()

    lacking = 0
    covered_via_same_name = 0
    entries: list[dict] = []
    for taxon_id, name, rank, status in rows:
        if taxon_id in concepts_with_id or not name:
            continue
        lacking += 1
        exact = sorted(artportalen.get(name, ()))
        variants = sorted({(i, n) for n in by_binomial_prefix.get(name, ())
                           if name_relation(name, n) == "variant" for i in artportalen[n]})
        if not exact and not variants:
            continue
        candidates = [{"artportalen_taxon_id": i, "artportalen_scientific_name": name,
                       "relation": "exact", "attached_to_concepts": sorted(attached.get(i, ()))}
                      for i in exact]
        candidates += [{"artportalen_taxon_id": i, "artportalen_scientific_name": n,
                        "relation": "variant", "attached_to_concepts": sorted(attached.get(i, ()))}
                       for i, n in variants]
        holders = attached.get(exact[0], set()) if len(exact) == 1 else set()
        if variants:
            classification = "split"
        elif len(exact) > 1:
            classification = "ambiguous_multiple_ids"
        elif holders and all(concept_names.get(h) == name for h in holders):
            # A same-named concept (typically an unmapped NorTaxa concept next
            # to its COL namesake) already carries the id, and the desktop's
            # genus/species lookup reaches it. Not an overlay question.
            covered_via_same_name += 1
            continue
        elif holders:
            classification = "ambiguous_id_in_use"
        elif name_counts[name] > 1:
            classification = "ambiguous_homonym"
        else:
            classification = "unique_exact"
        entries.append({"sporely_taxon_id": taxon_id, "scientific_name": name, "taxon_rank": rank,
                        "taxonomic_status": status, "classification": classification,
                        "candidates": candidates})

    counts = {c: sum(1 for e in entries if e["classification"] == c) for c in CLASSES}
    return {
        "format": PROPOSALS_FORMAT,
        "target": TARGET,
        "semantics": SEMANTICS,
        "generated_from": {
            "candidate_sqlite_sha256": _sha256_file(candidate),
            "candidate_content_release_id": meta.get("content_release_id"),
            "legacy_db_sha256": _sha256_file(legacy_db),
            "artportalen_names": len(artportalen),
        },
        "counts": {"concepts_lacking_artportalen_id": lacking,
                   "covered_via_same_name_concept": covered_via_same_name,
                   "proposed": len(entries),
                   "needs_review": len(entries) - counts["unique_exact"], **counts},
        "entries": entries,
    }


def render_markdown(proposals: dict, examples: Iterable[str] = ("Amanita muscaria",)) -> str:
    counts = proposals["counts"]
    lines = [
        "# Artportalen publishing-ID proposals",
        "",
        proposals["semantics"],
        "",
        f"Candidate `{proposals['generated_from']['candidate_content_release_id']}` "
        f"(SQLite `{proposals['generated_from']['candidate_sqlite_sha256'][:12]}…`), legacy database "
        f"`{proposals['generated_from']['legacy_db_sha256'][:12]}…`.",
        "",
        "| | Concepts |",
        "|---|---:|",
        f"| Lacking an Artportalen id | {counts['concepts_lacking_artportalen_id']} |",
        f"| Of these, a same-named concept already carries the exact-name id (not proposed) | "
        f"{counts['covered_via_same_name_concept']} |",
        f"| With an exact-name or variant candidate (proposed) | {counts['proposed']} |",
        f"| Uniquely obvious (`unique_exact`) | {counts['unique_exact']} |",
        f"| Needing review | {counts['needs_review']} |",
    ]
    lines += [f"| &nbsp;&nbsp;`{c}` | {counts[c]} |" for c in CLASSES[1:]]
    lines += ["", "Nothing here is accepted. Only entries copied into the overlay with "
              "`artportalen_overlay.py accept` are used by a build.", ""]
    by_name = {e["scientific_name"]: e for e in proposals["entries"]}
    for name in examples:
        entry = by_name.get(name)
        lines.append(f"## {name}")
        lines.append("")
        if not entry:
            lines += ["Not proposed (already has an Artportalen id, or no candidate).", ""]
            continue
        lines.append(f"Sporely `{entry['sporely_taxon_id']}` — **{entry['classification']}**")
        lines.append("")
        for c in entry["candidates"]:
            lines.append(f"- Artportalen `{c['artportalen_taxon_id']}` {c['artportalen_scientific_name']} "
                         f"({c['relation']}; attached to {c['attached_to_concepts'] or 'no concept'})")
        lines.append("")
    for classification in CLASSES[1:]:
        sample = [e for e in proposals["entries"] if e["classification"] == classification][:10]
        if not sample:
            continue
        lines += [f"## Sample: {classification}", ""]
        for e in sample:
            ids = ", ".join(f"{c['artportalen_taxon_id']} {c['artportalen_scientific_name']}" for c in e["candidates"])
            lines.append(f"- `{e['sporely_taxon_id']}` {e['scientific_name']} → {ids}")
        lines.append("")
    return "\n".join(lines)


# --------------------------------------------------------------- overlay ---

def empty_overlay() -> dict:
    return {"format": OVERLAY_FORMAT, "target": TARGET, "semantics": SEMANTICS, "entries": []}


def load_overlay(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise OverlayError(f"cannot read overlay {path}: {exc}") from exc
    if data.get("format") != OVERLAY_FORMAT or data.get("target") != TARGET:
        raise OverlayError(f"{path}: not a {TARGET} {OVERLAY_FORMAT} overlay")
    seen_concepts: set[int] = set()
    seen_ids: set[int] = set()
    required = ("sporely_taxon_id", "scientific_name", "artportalen_taxon_id",
                "artportalen_scientific_name", "decision", "accepted_by", "accepted_on")
    for entry in data.get("entries") or []:
        missing = [k for k in required if entry.get(k) in (None, "")]
        if missing:
            raise OverlayError(f"{path}: entry {entry.get('sporely_taxon_id')} lacks {missing}")
        concept, target_id = int(entry["sporely_taxon_id"]), int(entry["artportalen_taxon_id"])
        if concept in seen_concepts:
            raise OverlayError(f"{path}: concept {concept} is listed twice")
        if target_id in seen_ids:
            raise OverlayError(f"{path}: Artportalen id {target_id} is mapped to two concepts")
        seen_concepts.add(concept)
        seen_ids.add(target_id)
    keys = [int(e["sporely_taxon_id"]) for e in data.get("entries") or []]
    if keys != sorted(keys):
        raise OverlayError(f"{path}: entries must be sorted by sporely_taxon_id")
    return data


def accept(*, proposals: dict, overlay: dict, accepted_by: str, accepted_on: str,
           classification: str | None = None, decisions: Iterable[tuple[int, int]] = (),
           note: str | None = None) -> tuple[dict, int]:
    """Return the overlay with the chosen proposals added, and how many were added."""
    if not accepted_by.strip():
        raise OverlayError("accepted_by is required")
    by_concept = {e["sporely_taxon_id"]: e for e in proposals["entries"]}
    existing = {int(e["sporely_taxon_id"]): e for e in overlay["entries"]}
    chosen: list[tuple[dict, dict, str]] = []
    if classification:
        if classification != "unique_exact":
            raise OverlayError("only unique_exact can be accepted as a class; "
                               "review cases are accepted one by one with --entry")
        for entry in proposals["entries"]:
            if entry["classification"] == "unique_exact":
                chosen.append((entry, entry["candidates"][0], "accepted_unique_exact_name_match"))
    for concept, target_id in decisions:
        entry = by_concept.get(concept)
        if entry is None:
            raise OverlayError(f"concept {concept} is not in the proposals")
        candidate = next((c for c in entry["candidates"] if c["artportalen_taxon_id"] == target_id), None)
        if candidate is None:
            raise OverlayError(f"Artportalen id {target_id} is not a candidate for concept {concept}")
        chosen.append((entry, candidate, f"accepted_after_review:{entry['classification']}"))
    added = 0
    for entry, candidate, decision in chosen:
        concept = entry["sporely_taxon_id"]
        if concept in existing:
            if int(existing[concept]["artportalen_taxon_id"]) != candidate["artportalen_taxon_id"]:
                raise OverlayError(f"concept {concept} already maps to "
                                   f"{existing[concept]['artportalen_taxon_id']} in the overlay")
            continue
        record = {"sporely_taxon_id": concept, "scientific_name": entry["scientific_name"],
                  "artportalen_taxon_id": candidate["artportalen_taxon_id"],
                  "artportalen_scientific_name": candidate["artportalen_scientific_name"],
                  "decision": decision, "accepted_by": accepted_by.strip(), "accepted_on": accepted_on}
        if note:
            record["note"] = note
        existing[concept] = record
        added += 1
    result = dict(overlay)
    result["entries"] = [existing[k] for k in sorted(existing)]
    ids = [int(e["artportalen_taxon_id"]) for e in result["entries"]]
    if len(ids) != len(set(ids)):
        raise OverlayError("an Artportalen id would map to two concepts")
    return result, added


# ------------------------------------------------------------------- CLI ---

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("propose", help="write the review report for a built candidate")
    p.add_argument("--candidate", type=Path, required=True, help="candidate SQLite built by build_release.py")
    p.add_argument("--legacy-db", type=Path,
                   default=REPO_ROOT / "database/reference_data/generated/vernacular_multilanguage.sqlite3")
    p.add_argument("--output-dir", type=Path, required=True)
    a = sub.add_parser("accept", help="copy accepted proposals into the overlay")
    a.add_argument("--proposals", type=Path, required=True)
    a.add_argument("--overlay", type=Path, default=DEFAULT_OVERLAY)
    a.add_argument("--accepted-by", required=True)
    a.add_argument("--accepted-on", default=dt.date.today().isoformat())
    a.add_argument("--class", dest="classification", choices=["unique_exact"])
    a.add_argument("--entry", action="append", default=[], metavar="SPORELY_ID:ARTPORTALEN_ID")
    a.add_argument("--note")
    c = sub.add_parser("check", help="validate an overlay")
    c.add_argument("--overlay", type=Path, default=DEFAULT_OVERLAY)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "propose":
            proposals = propose(candidate=args.candidate, legacy_db=args.legacy_db)
            args.output_dir.mkdir(parents=True, exist_ok=True)
            (args.output_dir / "artportalen-proposals.json").write_text(_dump(proposals), encoding="utf-8")
            (args.output_dir / "artportalen-proposals.md").write_text(render_markdown(proposals), encoding="utf-8")
            print(json.dumps(proposals["counts"], indent=2))
        elif args.command == "accept":
            proposals = json.loads(args.proposals.read_text(encoding="utf-8"))
            if proposals.get("format") != PROPOSALS_FORMAT:
                raise OverlayError(f"{args.proposals} is not a proposals file")
            overlay = load_overlay(args.overlay) if args.overlay.exists() else empty_overlay()
            decisions = []
            for spec in args.entry:
                concept, _, target_id = spec.partition(":")
                decisions.append((int(concept), int(target_id)))
            overlay, added = accept(proposals=proposals, overlay=overlay, accepted_by=args.accepted_by,
                                    accepted_on=args.accepted_on, classification=args.classification,
                                    decisions=decisions, note=args.note)
            args.overlay.parent.mkdir(parents=True, exist_ok=True)
            args.overlay.write_text(_dump(overlay), encoding="utf-8")
            print(f"added {added}; overlay now has {len(overlay['entries'])} entries")
        else:
            overlay = load_overlay(args.overlay)
            print(f"ok: {len(overlay['entries'])} entries")
    except OverlayError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
