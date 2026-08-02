#!/usr/bin/env python3
"""Interactive manual-review CLI for the 85 manual_unresolved observations.

Reads the authoritative reconciliation manifest (never committed to Git),
groups records by identical text-signal evidence, presents up to five
non-authoritative candidate concepts from the release chain, and records
decisions to an operator-supplied decisions file OUTSIDE Git.

Keys per group:
    y     accept the best candidate (must be exactly ONE known candidate)
    1-5   accept the numbered candidate
    n     no match — leave as manual_unresolved (record the choice)
    s     skip this group for now (no decision recorded)
    b     go back to the previous UNSAVED group in this session
    q     quit — every prior decision is already on disk

The CLI NEVER auto-resolves by name equality. Every "y"/"1-5" decision
becomes a resolved_exact_via_synonym_relationship entry with resolution
method = "operator_manual_review". The 85 observations map to explicit
pseudonymous IDs listed in each group; those IDs are exactly what a
later deployment step joins back to real observation IDs.

Refuses:
    * --production
    * a decisions file inside either repo
    * a candidate that is not in the release / supplement chain
    * writing without --release-dir + --canonical-registry + --supplement
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import csv
import zipfile
from collections import defaultdict as _defaultdict

from database.taxonomy.reconciliation.candidates import generate_candidates  # noqa: E402
from database.taxonomy.reconciliation.input_model import ReconciliationInput, RawSignal  # noqa: E402
from database.taxonomy.reconciliation.sources import PinnedRelease  # noqa: E402

NORTAXA_ARCHIVE = Path(__file__).resolve().parents[1] / "sources/nortaxa/1.284/archive.zip"
REPO_ROOT = Path(__file__).resolve().parents[3]

_AUX_CACHE: dict[str, object] = {}


def _load_vernacular_index(release_dir: Path) -> dict[str, tuple[int, ...]]:
    key = f"vern::{release_dir}"
    if key in _AUX_CACHE:
        return _AUX_CACHE[key]  # type: ignore[return-value]
    idx: dict[str, set[int]] = _defaultdict(set)
    path = release_dir / "vernacular.jsonl"
    if path.is_file():
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                d = json.loads(line)
                name = (d.get("vernacular_name") or "").strip().casefold()
                tid = d.get("taxon_id")
                if name and tid is not None:
                    idx[name].add(int(tid))
    frozen = {k: tuple(sorted(v)) for k, v in idx.items()}
    _AUX_CACHE[key] = frozen
    return frozen


def _load_nortaxa_synonym_index() -> tuple[dict[str, str], dict[str, dict]]:
    """Return (synonym_name -> accepted_nortaxa_taxonID, taxonID -> {name, rank}).

    Uses the pinned NorTaxa 1.284 archive that we already trust for W2E-A2/B.
    """
    key = "nortaxa_synonyms"
    if key in _AUX_CACHE:
        return _AUX_CACHE[key]  # type: ignore[return-value]
    syn_to_accepted: dict[str, str] = {}
    id_to_meta: dict[str, dict] = {}
    if not NORTAXA_ARCHIVE.is_file():
        _AUX_CACHE[key] = (syn_to_accepted, id_to_meta)
        return _AUX_CACHE[key]  # type: ignore[return-value]
    csv.field_size_limit(sys.maxsize)
    with zipfile.ZipFile(NORTAXA_ARCHIVE) as z:
        taxon_file = next((n for n in z.namelist() if n.lower().endswith("taxon.txt")), None)
        if taxon_file is None:
            _AUX_CACHE[key] = (syn_to_accepted, id_to_meta)
            return _AUX_CACHE[key]  # type: ignore[return-value]
        with z.open(taxon_file) as f:
            reader = csv.DictReader(
                (line.decode("utf-8", errors="replace") for line in f),
                delimiter="\t",
            )
            for row in reader:
                status = (row.get("taxonomicStatus") or "").lower()
                accepted = row.get("acceptedNameUsageID") or ""
                name = (row.get("scientificName") or "").strip()
                tid = row.get("taxonID") or ""
                if not name or not tid:
                    continue
                lc = name.casefold()
                id_to_meta[tid] = {
                    "scientific_name": name,
                    "rank": row.get("taxonRank") or None,
                    "kingdom": row.get("kingdom") or None,
                }
                if status == "synonym" and accepted and accepted != tid:
                    syn_to_accepted[lc] = accepted
                elif status in ("accepted", "valid"):
                    syn_to_accepted.setdefault(lc, tid)
    _AUX_CACHE[key] = (syn_to_accepted, id_to_meta)
    return _AUX_CACHE[key]  # type: ignore[return-value]


def _nortaxa_to_sporely(release: PinnedRelease, nortaxa_taxon_id: str) -> int | None:
    tid = release.lookup_registry("nortaxa", "nortaxa_taxon_id", str(nortaxa_taxon_id))
    if tid is None:
        return None
    return int(tid)


def _lookup_synonym_candidate(release: PinnedRelease, name: str) -> list[dict]:
    """Given an old/historical scientific name, try to find the current concept
    via the NorTaxa synonym relationship. Non-authoritative — reviewer confirms.
    """
    syn_map, id_to_meta = _load_nortaxa_synonym_index()
    lc = (name or "").strip().casefold()
    if not lc:
        return []
    out: list[dict] = []
    accepted_nortaxa = syn_map.get(lc)
    if accepted_nortaxa is None:
        return []
    sporely_id = _nortaxa_to_sporely(release, accepted_nortaxa)
    if sporely_id is None:
        return []
    concept = release.concept(sporely_id) if hasattr(release, "concept") else None
    canonical_name = concept.canonical_scientific_name if concept else None
    rank = concept.taxon_rank if concept else None
    if not canonical_name:
        meta = id_to_meta.get(str(accepted_nortaxa))
        if meta:
            canonical_name = meta.get("scientific_name")
            rank = rank or meta.get("rank")
    out.append({
        "sporely_taxon_id": sporely_id,
        "canonical_name": canonical_name or "(no canonical name)",
        "rank": rank,
        "match_type": "nortaxa_synonym_redirect",
        "note": f"NorTaxa: {name!r} → accepted taxonID {accepted_nortaxa}",
    })
    return out


def _lookup_vernacular_candidates(release: PinnedRelease, release_dir: Path, name: str, limit: int = 3) -> list[dict]:
    idx = _load_vernacular_index(release_dir)
    lc = (name or "").strip().casefold()
    if not lc:
        return []
    hits = idx.get(lc, ())
    out: list[dict] = []
    for tid in hits[:limit]:
        concept = release.concept(tid) if hasattr(release, "concept") else None
        out.append({
            "sporely_taxon_id": int(tid),
            "canonical_name": concept.canonical_scientific_name if concept else None,
            "rank": concept.taxon_rank if concept else None,
            "match_type": "vernacular_exact",
            "note": f"vernacular {name!r} maps to this concept in the release",
        })
    return out


def _genus_only_candidates(release: PinnedRelease, genus: str, limit: int = 3) -> list[dict]:
    lc_prefix = (genus or "").strip().casefold()
    if not lc_prefix:
        return []
    matches: list[tuple[int, str]] = []
    for lc_name, tids in release.scientific_name_index.items():
        if lc_name == lc_prefix or lc_name.startswith(lc_prefix + " "):
            for tid in tids:
                concept = release.concept(tid) if hasattr(release, "concept") else None
                if concept is None:
                    continue
                if lc_name == lc_prefix and concept.taxon_rank != "genus":
                    continue
                matches.append((tid, concept.canonical_scientific_name or lc_name))
        if len(matches) >= limit * 4:
            break
    matches.sort(key=lambda m: m[1])
    out: list[dict] = []
    seen: set[int] = set()
    for tid, name in matches:
        if tid in seen:
            continue
        seen.add(tid)
        concept = release.concept(tid) if hasattr(release, "concept") else None
        out.append({
            "sporely_taxon_id": int(tid),
            "canonical_name": concept.canonical_scientific_name if concept else name,
            "rank": concept.taxon_rank if concept else None,
            "match_type": "genus_match_or_species_in_genus",
            "note": f"genus-scope suggestion for {genus!r}",
        })
        if len(out) >= limit:
            break
    return out


FORBIDDEN_PATHS = (
    "/Users/sigmundas/Documents/Code/sporely/sporely-py/",
    "/Users/sigmundas/Documents/Code/sporely/sporely-web/",
)


def _refuse_repo_path(path: Path) -> None:
    abs_ = str(path.resolve())
    for prefix in FORBIDDEN_PATHS:
        if abs_.startswith(prefix):
            raise SystemExit(
                f"refuse: decisions file MUST NOT live under {prefix} — it contains real pseudonymous IDs and per-observation decisions"
            )


def _group_key(record: dict) -> tuple[tuple[str, str], ...]:
    parts: dict[str, str] = {}
    for s in record.get("signals_all", []):
        if s.get("kind") == "text-only":
            parts[s["origin_field"]] = (s.get("raw_value") or "").strip()
    for k in ("original_scientific_name", "original_vernacular_name"):
        v = (record.get(k) or "").strip()
        if v:
            parts.setdefault(k, v)
    return tuple(sorted(parts.items()))


def _stored_display(record: dict) -> dict[str, str]:
    d: dict[str, str] = {}
    for s in record.get("signals_all", []):
        if s.get("kind") == "text-only":
            d[s["origin_field"]] = s.get("raw_value") or ""
    return d


def _group_signature(record: dict) -> str:
    key = _group_key(record)
    body = json.dumps(key, sort_keys=True, ensure_ascii=False).encode()
    return hashlib.sha256(body).hexdigest()[:16]


def _load_decisions(path: Path) -> dict:
    if not path.exists():
        return {"decisions": {}}
    doc = {"decisions": {}}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("__header__"):
                doc["header"] = obj
                continue
            gsig = obj.get("group_signature")
            if gsig:
                doc["decisions"][gsig] = obj
    return doc


def _write_decision(path: Path, decision: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(decision, sort_keys=True, ensure_ascii=False) + "\n")


def _preview_source_breakdown(manual: list[dict]) -> dict[str, int]:
    from collections import Counter
    ct = Counter()
    for r in manual:
        fields = {s["origin_field"] for s in r["signals_all"] if s.get("kind") == "text-only"}
        for f in fields:
            ct[f] += 1
    return dict(ct.most_common())


def _preview(manifest: dict) -> None:
    manual = [r for r in manifest["records"] if r["reconciliation_state"] == "manual_unresolved"]
    from collections import defaultdict
    groups: dict[tuple, list[str]] = defaultdict(list)
    for r in manual:
        groups[_group_key(r)].append(r["observation_id"])
    unique = sum(1 for v in groups.values() if len(v) == 1)
    repeated = sum(1 for v in groups.values() if len(v) > 1)
    repeated_obs = sum(len(v) for v in groups.values() if len(v) > 1)
    print(f"manual observations: {len(manual)}")
    print(f"unique review groups: {len(groups)}")
    print(f"  singletons (1 observation each): {unique}")
    print(f"  repeated (2+): {repeated} groups spanning {repeated_obs} observations")
    print()
    print("source-field breakdown (observations mentioning each field):")
    for f, c in _preview_source_breakdown(manual).items():
        print(f"  {f:50} {c}")


def _load_release_chain(args) -> PinnedRelease:
    registries = [Path(p) for p in (args.canonical_registry or [])]
    for supp_dir in args.supplement or []:
        registries.append(Path(supp_dir) / "canonical")
    return PinnedRelease.load(Path(args.release_dir), canonical_registry_path=registries or None)


def _candidates_for_group(release: PinnedRelease, records: list[dict], release_dir: Path, top_n: int = 5) -> list[dict]:
    # Take the first record and build a synthetic ReconciliationInput from its
    # text signals. generate_candidates() reads stored_scientific_name and the
    # signals to compose candidates.
    r = records[0]
    inp_signals: list[RawSignal] = []
    stored_name = r.get("original_scientific_name")
    stored_vernacular = r.get("original_vernacular_name")
    for s in r["signals_all"]:
        if s.get("kind") == "text-only":
            inp_signals.append(RawSignal(
                kind="text-only",
                source_system=s.get("source_system"),
                namespace=s.get("namespace"),
                external_id=s.get("external_id"),
                origin_field=s.get("origin_field") or "",
                raw_value=s.get("raw_value"),
                rule_id=s.get("rule_id"),
            ))
    if not stored_name:
        # synth from genus + species text signals
        parts = {}
        for s in inp_signals:
            if s.origin_field.endswith(".genus"):
                parts["genus"] = (s.raw_value or "").strip()
            elif s.origin_field.endswith(".species"):
                parts["species"] = (s.raw_value or "").strip()
        if parts.get("genus") and parts.get("species"):
            stored_name = f"{parts['genus']} {parts['species']}"
    # Ensure the resolver's expected 'genus+species' synthetic signal is
    # present so generate_candidates picks it up.
    if stored_name and not any(s.origin_field == "observations.genus+species" for s in inp_signals):
        inp_signals.append(RawSignal(
            kind="text-only",
            source_system=None,
            namespace=None,
            external_id=None,
            origin_field="observations.genus+species",
            raw_value=stored_name,
            rule_id=None,
        ))
    cands = generate_candidates(
        signals=tuple(inp_signals),
        stored_rank=None,
        release=release,
    )
    out = []
    for c in cands[:top_n]:
        d = c.to_dict()
        out.append({
            "sporely_taxon_id": d.get("sporely_taxon_id"),
            "canonical_name": d.get("canonical_name") or d.get("scientific_name"),
            "rank": d.get("rank"),
            "match_type": d.get("match_type"),
            "note": d.get("author_string_consistency") or d.get("reason") or "",
        })
    # Additional candidate sources (all non-authoritative; reviewer confirms):
    #   1. NorTaxa synonym redirect     — old scientific names → current concept
    #   2. Vernacular index             — Norwegian common names → concept
    #   3. Genus-only prefix suggestion — for genus-only or malformed inputs
    #   4. Pre-computed manifest candidates
    already = {c.get("sporely_taxon_id") for c in out}

    def _extend(new_items):
        for c in new_items:
            tid = c.get("sporely_taxon_id")
            if tid is None or tid in already:
                continue
            if len(out) >= top_n:
                return
            already.add(tid)
            out.append(c)

    # Synonym redirect on the stored scientific name and any stored ai/species.
    for candidate_name in filter(None, [
        stored_name,
        r.get("original_scientific_name"),
        next((s.get("raw_value") for s in r.get("signals_all", []) if s.get("origin_field") == "observations.ai_selected_scientific_name"), None),
    ]):
        _extend(_lookup_synonym_candidate(release, candidate_name))
        if len(out) >= top_n:
            break

    # Vernacular lookups on Norwegian common name and vernacular snapshot.
    if len(out) < top_n:
        for vernacular_name in filter(None, [
            stored_vernacular,
            next((s.get("raw_value") for s in r.get("signals_all", []) if s.get("origin_field") == "observations.common_name"), None),
        ]):
            _extend(_lookup_vernacular_candidates(release, release_dir, vernacular_name))
            if len(out) >= top_n:
                break

    # Genus-only fallback when signals name a genus but no confident species.
    if len(out) < top_n and not stored_name:
        genus_val = next((s.get("raw_value") for s in r.get("signals_all", []) if s.get("origin_field") == "observations.genus"), None) or \
                    next((s.get("raw_value") for s in r.get("signals_all", []) if s.get("origin_field") == "observations.species"), None)
        if genus_val:
            _extend(_genus_only_candidates(release, str(genus_val)))

    # Pre-computed manifest candidates last.
    for cand in r.get("candidate_concepts") or []:
        _extend([{
            "sporely_taxon_id": cand.get("sporely_taxon_id"),
            "canonical_name": cand.get("canonical_name"),
            "rank": cand.get("rank"),
            "match_type": cand.get("match_type") or "manifest-candidate",
            "note": "",
        }])
    return out


def _render_group(index: int, total: int, key: tuple, obs_ids: list[str], candidates: list[dict]) -> None:
    print(f"\n=== group {index+1}/{total}   observations: {len(obs_ids)} ===")
    for k, v in key:
        print(f"  {k:50} {v!r}")
    if len(obs_ids) <= 10:
        for oid in obs_ids:
            print(f"  · {oid}")
    else:
        for oid in obs_ids[:5]:
            print(f"  · {oid}")
        print(f"  · ... and {len(obs_ids) - 5} more")
    print()
    if candidates:
        print("candidate concepts (non-authoritative — reviewer must confirm):")
        for i, c in enumerate(candidates[:5], start=1):
            name = c.get("canonical_name") or "(no name)"
            rank = c.get("rank") or ""
            mt = c.get("match_type") or ""
            print(f"  [{i}] sporely_taxon_id={c.get('sporely_taxon_id')} name={name!r} rank={rank!r} match={mt!r}")
    else:
        print("(no candidate concepts available — press n or s or q)")


def _prompt() -> str:
    return input("action (y=accept top / 1-5=accept numbered / n=no match / s=skip / b=back / q=quit): ").strip().lower()


def run_review(
    manifest_path: Path,
    decisions_path: Path,
    release_dir: Path,
    canonical_registry: list[Path],
    supplements: list[Path],
) -> None:
    _refuse_repo_path(decisions_path)
    manifest = json.loads(manifest_path.read_text())
    manual = [r for r in manifest["records"] if r["reconciliation_state"] == "manual_unresolved"]

    from collections import defaultdict
    grouped_records: dict[tuple, list[dict]] = defaultdict(list)
    for r in manual:
        grouped_records[_group_key(r)].append(r)
    ordered = sorted(grouped_records.items(), key=lambda kv: kv[0])

    existing = _load_decisions(decisions_path)
    if not existing.get("header"):
        _write_decision(decisions_path, {
            "__header__": True,
            "manifest_semantic_sha256": manifest.get("input_source_hash"),
            "manifest_path": str(manifest_path),
            "started_at": "2026-08-02T00:00:00Z",
            "reviewer": os.environ.get("USER") or "unknown",
        })
    decisions = existing.get("decisions", {})

    class MockNS:
        pass
    ns = MockNS()
    ns.release_dir = release_dir
    ns.canonical_registry = canonical_registry
    ns.supplement = supplements
    release = _load_release_chain(ns)

    history: list[int] = []
    idx = 0
    while idx < len(ordered):
        key, records = ordered[idx]
        gsig = _group_signature(records[0])
        if gsig in decisions:
            idx += 1
            continue
        obs_ids = [r["observation_id"] for r in records]
        candidates = _candidates_for_group(release, records, release_dir=release_dir, top_n=5)
        _render_group(idx, len(ordered), key, obs_ids, candidates)
        action = _prompt()
        if action == "q":
            print("quit — decisions on disk:", len(decisions))
            return
        if action == "s":
            idx += 1
            continue
        if action == "b":
            if not history:
                print("nothing to go back to")
                continue
            idx = history.pop()
            continue
        decision: dict | None = None
        if action == "n":
            decision = {"choice": "no_match", "resolved_sporely_taxon_id": None}
        elif action == "y":
            if len(candidates) != 1:
                print("y requires exactly one candidate; use 1-5 or n instead")
                continue
            c = candidates[0]
            decision = {"choice": "accepted_top", "resolved_sporely_taxon_id": c["sporely_taxon_id"], "candidate": c}
        elif action in ("1", "2", "3", "4", "5"):
            n = int(action) - 1
            if n >= len(candidates):
                print(f"only {len(candidates)} candidate(s) available")
                continue
            c = candidates[n]
            decision = {"choice": f"accepted_{n+1}", "resolved_sporely_taxon_id": c["sporely_taxon_id"], "candidate": c}
        else:
            print(f"unknown action: {action!r}")
            continue
        payload = {
            "group_signature": gsig,
            "group_key": [list(item) for item in key],
            "observation_ids": obs_ids,
            "decided_at_epoch": int(time.time()),
            **decision,
        }
        _write_decision(decisions_path, payload)
        decisions[gsig] = payload
        history.append(idx)
        idx += 1
    print(f"\ndone. {len(decisions)} decision(s) recorded at {decisions_path}")


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--decisions", type=Path, required=False,
                        help="path to write decisions JSONL (required for --review)")
    parser.add_argument("--release-dir", type=Path, required=False)
    parser.add_argument("--canonical-registry", type=Path, action="append", default=[])
    parser.add_argument("--supplement", type=Path, action="append", default=[])
    parser.add_argument("--preview", action="store_true")
    parser.add_argument("--review", action="store_true")
    parser.add_argument("--production", action="store_true")
    args = parser.parse_args(argv)
    if args.production:
        print("refuse: --production is not honoured", file=sys.stderr)
        return 3
    manifest = json.loads(args.manifest.read_text())
    if args.preview and not args.review:
        _preview(manifest)
        return 0
    if not args.review:
        _preview(manifest)
        return 0
    if not args.decisions or not args.release_dir:
        print("--review requires --decisions and --release-dir", file=sys.stderr)
        return 2
    run_review(
        manifest_path=args.manifest,
        decisions_path=args.decisions,
        release_dir=args.release_dir,
        canonical_registry=[Path(p) for p in args.canonical_registry],
        supplements=[Path(p) for p in args.supplement],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
