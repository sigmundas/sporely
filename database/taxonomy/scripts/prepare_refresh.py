#!/usr/bin/env python3
"""Quarterly taxonomy refresh: prepare one candidate and its change report.

One command, no publication. From a previous build (the identity baseline,
normally the last released or candidate build directory of
``build_release.py``) and new source releases:

1. Acquisition gate: every new source release must have a committed
   acquisition record with an approval file and a manifest pin. With
   ``--fetch``, a missing archive is fetched by that source's own acquisition
   tool (one approved GET; nothing is bypassed); without it nothing is
   downloaded.
2. Normalizes the new sources and writes source diffs against the
   previous build's normalized output.
3. ``carry_forward.py``: one source-continuity receipt for this release.
4. ``build_release.build`` on a refresh recipe (previous recipe, new source
   dirs, the receipt). If the compile allocated new ids, the post-compile
   registry is re-sharded into the build directory (never into the
   repository) and the build runs once more on it, so its two independent
   compiles prove determinism and the set can freeze.
5. Cloud export (``--candidate-validation-only``) and macrofungi scope from
   the frozen SQLite, so they bind its hash.
6. ``change-report.{json,md}``.

It stops cleanly, with the report, on any condition a reviewer must see:
a flagged NorTaxa mapping or supersession (compiler refuses), a failed build
or freeze (for example pinned vernacular counts; the measured counts are
reported), a concept that stops being live without a supersession. Flagged
Dyntaxa approvals stay unapplied and are listed. Pending release decisions do
not stop preparation; they block publication, which stays the separate
release-decision -> promote -> export/import flow.
"""
from __future__ import annotations

import argparse
import gzip
import json
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

import build_release  # noqa: E402
import carry_forward  # noqa: E402
from compile_release import _canonical_name_for_compare, _iter_taxa, _release_string  # noqa: E402
from identity_registry import shard_registry  # noqa: E402

REPO_ROOT = build_release.REPO_ROOT
TAXONOMY = REPO_ROOT / "database" / "taxonomy"
APPROVAL_FILES = ("acquisition-approval.json", "approval.json", "acquisition-attempt-1-approval.json")
FETCHERS = {"nortaxa": "acquire_nortaxa.py", "dyntaxa": "acquire_dyntaxa.py"}


class RefreshStop(Exception):
    """A condition the reviewer must see; the report is still written."""


# ----- acquisition gate ------------------------------------------------------

def acquisition_gate(source: dict, archive_root: Path, fetch: bool, python: str) -> dict:
    release_dir = REPO_ROOT / source["release_dir"]
    approval = next((release_dir / name for name in APPROVAL_FILES if (release_dir / name).is_file()), None)
    if approval is None:
        raise RefreshStop(f"{source['code']}: no acquisition approval in {source['release_dir']}")
    manifest = release_dir / "manifest.json"
    archive = archive_root / source["release_dir"] / "archive.zip"
    fetched = False
    if not archive.is_file():
        if not fetch or source["code"] not in FETCHERS:
            raise RefreshStop(f"{source['code']}: archive missing at {archive}"
                              + ("" if fetch else " (offline; pass --fetch to run its approved acquisition)"))
        mirror = archive_root / "database" / "taxonomy"
        mirror_release = archive_root / source["release_dir"]
        mirror_release.mkdir(parents=True, exist_ok=True)
        for record in release_dir.glob("*.json"):
            if record.name != "manifest.json" or source["code"] == "nortaxa":
                shutil.copy2(record, mirror_release / record.name)
        version = Path(source["release_dir"]).name
        result = subprocess.run([python, str(SCRIPTS / FETCHERS[source["code"]]), "--execute",
                                 "--version", version, "--taxonomy-root", str(mirror)],
                                cwd=REPO_ROOT, capture_output=True, text=True)
        if result.returncode != 0:
            raise RefreshStop(f"{source['code']}: acquisition failed: {result.stderr.strip()[-500:]}")
        shutil.copy2(mirror_release / "manifest.json", manifest)
        fetched = True
    if not manifest.is_file():
        raise RefreshStop(f"{source['code']}: missing acquisition manifest")
    pinned = build_release.manifest_archive_sha256(json.loads(manifest.read_text()))
    actual = build_release.sha256_file(archive)
    if actual != pinned:
        raise RefreshStop(f"{source['code']}: archive {actual} is not the manifest pin {pinned}")
    return {"approval": str(approval.relative_to(REPO_ROOT)), "archive_sha256": actual, "fetched": fetched}


# ----- diffs -----------------------------------------------------------------

def _snapshot_all(directory: Path) -> dict[tuple[str, str], carry_forward.Snapshot]:
    out = {}
    for record in _iter_taxa(directory):
        out[(record.taxon_id_namespace, record.taxon_id_value)] = carry_forward.Snapshot.of(record)
    return out


def _has_name_record_ids(directory: Path) -> bool:
    with (directory / "taxa.jsonl").open(encoding="utf-8") as handle:
        first = handle.readline()
    return bool(first) and "name_record_ids" in json.loads(first)


def source_diff(previous: Path, current: Path) -> dict:
    old, new = _snapshot_all(previous), _snapshot_all(current)
    changed: dict[str, int] = {}
    for key in old.keys() & new.keys():
        a, b = old[key], new[key]
        for name in ("scientific_name", "rank", "taxonomic_status", "accepted", "parent"):
            if getattr(a, name) != getattr(b, name):
                changed[name] = changed.get(name, 0) + 1
        if a.authorship != b.authorship:
            kind = ("authorship_typography" if carry_forward.typography_key(a.authorship)
                    == carry_forward.typography_key(b.authorship) else "authorship")
            changed[kind] = changed.get(kind, 0) + 1
    return {"previous": len(old), "current": len(new), "added": len(new.keys() - old.keys()),
            "removed": len(old.keys() - new.keys()), "changed": dict(sorted(changed.items()))}


def col_churn_candidates(receipt: dict, previous: Path, current: Path) -> list[dict]:
    """Report-only: for reviewed COL ids now missing, current COL usages with the
    same canonical name and rank that did not exist before. Never applied."""
    missing = sorted({i.split("/", 2)[2] for r in receipt["records"].values() for i in r["identifiers"]
                      if i.startswith("col_xr/") and any(x.startswith(i + ":missing") for x in r["reasons"])})
    if not missing:
        return []
    old, new = _snapshot_all(previous), _snapshot_all(current)
    by_name: dict[tuple[str, str], list[str]] = {}
    for (namespace, value), snap in new.items():
        if (namespace, value) not in old:
            by_name.setdefault((_canonical_name_for_compare(snap.scientific_name), snap.rank), []).append(value)
    out = []
    for value in missing:
        snap = old.get(("col_usage_id", value))
        key = (_canonical_name_for_compare(snap.scientific_name), snap.rank) if snap else None
        out.append({"col_usage_id": value, "name": snap.scientific_name if snap else None,
                    "same_name_rank_new_ids": sorted(by_name.get(key, [])) if key else []})
    return out


# ----- SQLite comparison -----------------------------------------------------

def _open_sqlite(path: Path, scratch: Path) -> Path:
    if path.suffix == ".gz":
        target = scratch / (path.stem + ".cmp")
        if not target.exists():
            with gzip.open(path, "rb") as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst, 1 << 20)
        return target
    return path


def live_concepts(path: Path) -> tuple[set[int], dict[int, int], dict[str, str]]:
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
        live = {row[0] for row in conn.execute("SELECT taxon_id FROM taxon_min")}
        try:
            retired = dict(conn.execute("SELECT retired_taxon_id, current_taxon_id FROM taxon_supersession_min"))
        except sqlite3.OperationalError:
            retired = {}
        meta = dict(conn.execute("SELECT key, value FROM taxonomy_meta"))
    return live, retired, meta


def policy_exclusions(col_dir: Path, anchors: dict[int, tuple], previous_archive: Path | None = None) -> dict[int, str]:
    """Sporely ids whose COL anchor falls under an explicit exclusion policy,
    with the policy reason label. Two cases: the normalizer excluded the
    current usage (``excluded_by_policy.jsonl``), or the anchored usage
    vanished from COL and its row in the previous archive matches the same
    policy predicate (same source checks). The registry keeps these
    allocations; they are reported apart from unexplained losses."""
    path = col_dir / "excluded_by_policy.jsonl"
    report_path = col_dir / "report.json"
    if not path.is_file() or not report_path.is_file():
        return {}
    exclusion_report = json.loads(report_path.read_text()).get("source_exclusions") or {}
    checks = exclusion_report.get("checks") or {}
    label = lambda code: checks.get(code, {}).get("label") or f"excluded_by_policy:{code}"
    codes: dict[str, str] = {}
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            codes[row["col_usage_id"]] = label(row["code"])
    anchored = {ident[2]: taxon for taxon, ident in anchors.items()
                if ident[0] == "col_xr" and ident[1] == "col_usage_id"}
    if previous_archive is not None and previous_archive.is_file() and exclusion_report.get("policy"):
        import zipfile
        import normalize_col_xr
        exclusions = normalize_col_xr.load_source_exclusions(Path(exclusion_report["policy"]))["exclusions"]
        current = {r.taxon_id_value for r in _iter_taxa(col_dir)}
        vanished = set(anchored) - current - set(codes)
        cache: dict = {}
        with zipfile.ZipFile(previous_archive) as z:
            member = next(n for n in z.namelist() if n.casefold().endswith("nameusage.tsv"))
            with z.open(member) as handle:
                header = handle.readline().decode("utf-8-sig").rstrip("\r\n").split("\t")
                ix = {k: header.index(f"col:{k}") for k in ("ID", "scientificName", "rank", "sourceID")}
                for raw in handle:
                    fields = raw.decode("utf-8", "replace").rstrip("\r\n").split("\t")
                    if fields[ix["ID"]] not in vanished:
                        continue
                    source_id = fields[ix["sourceID"]]
                    hit = normalize_col_xr.excluded_by_source_policy(
                        fields[ix["ID"]], fields[ix["scientificName"]], fields[ix["rank"]],
                        normalize_col_xr._source_alias(z, source_id, cache) if source_id else None, exclusions)
                    if hit is not None:
                        codes[fields[ix["ID"]]] = hit["label"]
    return {anchored[usage]: reason for usage, reason in codes.items() if usage in anchored}


def held_by_continuity(receipt: dict, anchors: dict[int, tuple]) -> dict[int, str]:
    """Sporely ids whose own anchor usage a held continuity record withholds."""
    by_usage = {"/".join(ident): taxon for taxon, ident in anchors.items()}
    out = {}
    for record_id, record in (receipt.get("records") or {}).items():
        if record.get("status") == "held":
            for identifier in record.get("identifiers") or []:
                if identifier in by_usage:
                    out[by_usage[identifier]] = f"held:{record.get('kind')}"
    # COL continuity holds the compiler applies from the receipt: a usage
    # that took another anchored id's name record, and a vanished anchor
    # whose matching new usage is withheld until the continuity is approved.
    candidates = receipt.get("col_continuity_candidates") or {}
    approved_new = {e["new_col_id"] for e in receipt.get("col_id_continuity") or []}
    vanished = candidates.get("vanished_anchors") or {}
    for transfer in candidates.get("name_record_transfers") or []:
        out.setdefault(transfer["sporely_taxon_id"], "held:name_record_transfer")
    for entry in (candidates.get("pairs") or []) + (candidates.get("ambiguous") or []):
        if set(entry.get("new_col_ids") or [entry.get("new_col_id")]) & approved_new:
            continue
        for old in entry.get("old_col_ids") or [entry.get("old_col_id")]:
            taxon = (vanished.get(old) or {}).get("sporely_taxon_id", entry.get("sporely_taxon_id"))
            if taxon is not None:
                out.setdefault(taxon, "held:col_continuity_unapproved")
    return out


def identity_comparison(previous_sqlite: Path, candidate_sqlite: Path,
                        policy_excluded: dict[int, str] | None = None,
                        held: dict[int, str] | None = None) -> dict:
    old_live, old_retired, _ = live_concepts(previous_sqlite)
    new_live, new_retired, _ = live_concepts(candidate_sqlite)
    stopped = old_live - new_live
    explained = {i for i in stopped if i in new_retired and new_retired[i] in new_live}
    policy_excluded = policy_excluded or {}
    held = held or {}
    by_policy = {i for i in stopped - explained if i in policy_excluded}
    by_hold = {i for i in stopped - explained - by_policy if i in held}
    held_reasons: dict[str, int] = {}
    for taxon in by_hold:
        held_reasons[held[taxon]] = held_reasons.get(held[taxon], 0) + 1
    reasons: dict[str, int] = {}
    for taxon in by_policy:
        reasons[policy_excluded[taxon]] = reasons.get(policy_excluded[taxon], 0) + 1
    return {"previous_live": len(old_live), "candidate_live": len(new_live),
            "new_live": len(new_live - old_live), "stopped_being_live": len(stopped),
            "stopped_by_supersession": len(explained),
            "stopped_excluded_by_policy": dict(sorted(reasons.items())),
            "stopped_held_by_continuity": dict(sorted(held_reasons.items())),
            "stopped_unexplained": sorted(stopped - explained - by_policy - by_hold),
            "newly_retired": len(set(new_retired) - set(old_retired)),
            # Excluded allocations are never live again in this candidate.
            "policy_excluded_but_live": sorted(set(policy_excluded) & new_live)}


# ----- candidate metrics -----------------------------------------------------

VERNACULAR_LANGUAGES = ("nb", "nn", "sv", "en")


def sqlite_metrics(path: Path) -> dict:
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
        q = lambda sql, *a: conn.execute(sql, a).fetchall()
        dup = {rank: q("SELECT count(*), coalesce(sum(c),0) FROM (SELECT count(*) c FROM taxon_min "
                       "WHERE taxon_rank = ? GROUP BY lower(canonical_scientific_name) HAVING c > 1)", rank)[0]
               for rank in ("species", "genus")}
        return {
            "taxa": q("SELECT count(*) FROM taxon_min")[0][0],
            "accepted_concepts": q("SELECT count(*) FROM taxon_min WHERE taxonomic_status = 'accepted'")[0][0],
            "identical_label_duplicates": {rank: {"labels": v[0], "rows": v[1]} for rank, v in dup.items()},
            "vernacular_rows": {lang: q("SELECT count(*) FROM vernacular_min WHERE language_code = ?", lang)[0][0]
                                for lang in VERNACULAR_LANGUAGES},
            "vernacular_concepts": {lang: q("SELECT count(DISTINCT taxon_id) FROM vernacular_min "
                                            "WHERE language_code = ?", lang)[0][0] for lang in VERNACULAR_LANGUAGES},
            "redlist_rows_resolved": q("SELECT count(*) FROM taxon_redlist_min WHERE taxon_id IS NOT NULL")[0][0],
            "supersession_rows": len(_supersession_ids(path)),
        }


def _supersession_ids(path: Path) -> set[str]:
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
        try:
            return {r[0] for r in conn.execute("SELECT supersession_id FROM taxon_supersession_min")}
        except sqlite3.OperationalError:
            return set()


def _external_map(path: Path) -> dict[tuple, set[int]]:
    out: dict[tuple, set[int]] = {}
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
        for taxon, system, namespace, value in conn.execute(
                "SELECT taxon_id, source_system, namespace, external_id FROM taxon_external_id_text_min"):
            out.setdefault((system, namespace, value), set()).add(taxon)
        for taxon, system, value in conn.execute(
                "SELECT taxon_id, source_system, CAST(external_id AS TEXT) FROM taxon_external_id_min"):
            out.setdefault((system, "int", value), set()).add(taxon)
    return out


def external_id_changes(previous: Path, candidate: Path) -> dict:
    old, new = _external_map(previous), _external_map(candidate)
    out: dict[str, dict[str, int]] = {}
    for key in old.keys() | new.keys():
        kind = ("lost" if key not in new else "gained" if key not in old
                else "retargeted" if old[key] != new[key] else None)
        if kind:
            bucket = out.setdefault(f"{key[0]}/{key[1]}", {"lost": 0, "gained": 0, "retargeted": 0})
            bucket[kind] += 1
    return dict(sorted(out.items()))


def compare_metrics(previous: Path, candidate: Path) -> dict:
    a, b = sqlite_metrics(previous), sqlite_metrics(candidate)
    prev_sup, cand_sup = _supersession_ids(previous), _supersession_ids(candidate)
    def delta(x, y):
        return {k: delta(x[k], y.get(k)) for k in x} if isinstance(x, dict) else {"previous": x, "candidate": y,
                                                                                    "delta": (y or 0) - (x or 0)}
    return {"metrics": delta(a, b),
            "supersessions_retained": len(prev_sup & cand_sup), "supersessions_previous": len(prev_sup),
            "supersessions_dropped": sorted(prev_sup - cand_sup)[:50],
            "external_ids": external_id_changes(previous, candidate)}


def supersession_groups(ledger: dict, candidate: Path) -> dict:
    present = _supersession_ids(candidate)
    groups: dict[str, dict[str, int]] = {}
    for entry in ledger.get("supersessions") or []:
        group = ("r1_pending_release_decision" if entry.get("release_approval") == "pending"
                 else "approved_earlier")
        bucket = groups.setdefault(group, {"total": 0, "retained": 0})
        bucket["total"] += 1
        bucket["retained"] += entry["supersession_id"] in present
    return groups


def concept_impact(receipt: dict, registry_before: Path, registry_after: Path,
                   previous: Path, candidate: Path) -> list[dict]:
    """What happens to every concept a held relationship touches."""
    def lookup(path):
        index: dict[tuple, int] = {}
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                if "sporely_taxon_id" in row:
                    index[(row["source"], row["namespace"], row["identifier"])] = int(row["sporely_taxon_id"])
        return index
    before, after = lookup(registry_before), lookup(registry_after)

    def describe(path: Path, taxon: int) -> dict:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
            q = lambda sql: conn.execute(sql, (taxon,)).fetchall()
            row = q("SELECT canonical_scientific_name, taxon_rank, taxonomic_status, canonical_source_system, "
                    "canonical_external_id FROM taxon_min WHERE taxon_id = ?")
            retired = q("SELECT current_taxon_id, supersession_id FROM taxon_supersession_min WHERE retired_taxon_id = ?")
            return {
                "live": bool(row), "taxon_min": list(row[0]) if row else None,
                "retired_to": list(retired[0]) if retired else None,
                "external_ids": sorted(f"{s}/{n}/{v}" for s, n, v in q(
                    "SELECT source_system, namespace, external_id FROM taxon_external_id_text_min WHERE taxon_id = ?"))
                + sorted(f"{s}/{v}" for s, v in q(
                    "SELECT source_system, external_id FROM taxon_external_id_min WHERE taxon_id = ?")),
                "scientific_names": sorted(f"{l}:{n}:{s}" for l, n, s in q(
                    "SELECT language_code, scientific_name, source FROM scientific_name_min WHERE taxon_id = ?")),
                "vernaculars": sorted(f"{l}:{n}" for l, n in q(
                    "SELECT language_code, vernacular_name FROM vernacular_min WHERE taxon_id = ?")),
                "redlist": [list(r) for r in q("SELECT assessment_area, category_code, scientific_name_snapshot "
                                                "FROM taxon_redlist_min WHERE taxon_id = ?")],
            }
    out = []
    held = [(rid, r) for rid, r in sorted(receipt["records"].items()) if r["status"] == "held"]
    held += [(usage, {"kind": "registry_alias_conflict", "reasons": [hold.get("observed_change", "")],
                      "identifiers": [usage]})
             for usage, hold in sorted((receipt.get("held_alias_conflicts") or {}).items())]
    for record_id, record in held:
        identifiers = [tuple(i.split("/", 2)) for i in record["identifiers"]]
        taxa = sorted({before[i] for i in identifiers if i in before} | {after[i] for i in identifiers if i in after})
        out.append({
            "record_id": record_id, "kind": record["kind"], "reasons": record["reasons"],
            "identifiers": {"/".join(i): {"registry_before": before.get(i), "registry_after": after.get(i)}
                            for i in identifiers},
            "new_allocation_for_held_usage": any(i not in before and i in after for i in identifiers),
            "concepts": {str(t): {"previous": describe(previous, t), "candidate": describe(candidate, t)} for t in taxa},
        })
    return out


def col_churn(previous_dir: Path, current_dir: Path, archive: Path, anchors: dict[int, tuple],
              receipt: dict) -> dict:
    """Real COL id churn between the two COL inputs. ``col:sourceID`` of the
    vanished usages is read from the previous archive (provenance only);
    replacement candidates stay report-only."""
    import zipfile
    old = {v for (_, v) in _snapshot_all(previous_dir)}
    new = {v for (_, v) in _snapshot_all(current_dir)}
    excluded_path = current_dir / "excluded_by_policy.jsonl"
    excluded = ({json.loads(line)["col_usage_id"] for line in excluded_path.open(encoding="utf-8")}
                if excluded_path.is_file() else set())
    excluded_missing = (old - new) & excluded
    missing = old - new - excluded
    exclusion_report = json.loads((current_dir / "report.json").read_text()).get("source_exclusions") or {}
    import normalize_col_xr
    exclusions = (normalize_col_xr.load_source_exclusions(Path(exclusion_report["policy"]))["exclusions"]
                  if exclusion_report.get("policy") else [])
    vanished_excluded: set[str] = set()
    anchored = {ident[2] for ident in anchors.values() if ident[0] == "col_xr"}
    reviewed = {i.split("/", 2)[2] for r in receipt["records"].values() for i in r["identifiers"]
                if i.startswith("col_xr/")}
    source_ids: dict[str, int] = {}
    if missing and archive.is_file():
        with zipfile.ZipFile(archive) as z:
            member = next(n for n in z.namelist() if n.casefold().endswith("nameusage.tsv"))
            with z.open(member) as handle:
                header = handle.readline().decode("utf-8-sig").rstrip("\r\n").split("\t")
                id_col = header.index("col:ID")
                src_col = header.index("col:sourceID") if "col:sourceID" in header else None
                name_col, rank_col = header.index("col:scientificName"), header.index("col:rank")
                cache: dict = {}
                for raw in handle:
                    fields = raw.decode("utf-8", "replace").rstrip("\r\n").split("\t")
                    if len(fields) > id_col and fields[id_col] in missing:
                        key = fields[src_col] if src_col is not None and len(fields) > src_col else ""
                        if exclusions and normalize_col_xr.excluded_by_source_policy(
                                fields[id_col], fields[name_col], fields[rank_col],
                                normalize_col_xr._source_alias(z, key, cache) if key else None, exclusions):
                            vanished_excluded.add(fields[id_col])
                            continue
                        source_ids[key or "(none)"] = source_ids.get(key or "(none)", 0) + 1
    missing -= vanished_excluded
    return {"previous_usages": len(old), "current_usages": len(new), "vanished": len(missing),
            "excluded_by_policy": {"previous_usages": len(excluded_missing),
                                   "registry_anchors": len(excluded_missing & anchored),
                                   "current_release_total": len(excluded),
                                   # Vanished from COL, but the previous row matches the policy predicate.
                                   "vanished_from_col": len(vanished_excluded),
                                   "vanished_registry_anchors": len(vanished_excluded & anchored),
                                   "not_in_registry": len(excluded - anchored)},
            "new": len(new - old), "vanished_registry_anchors": len(missing & anchored),
            "registry_col_anchors": len(anchored), "vanished_reviewed_targets": len(missing & reviewed),
            "reviewed_col_targets": len(reviewed),
            "vanished_by_source_id": dict(sorted(source_ids.items(), key=lambda kv: -kv[1])[:15]),
            "vanished_nortaxa_2030": source_ids.get("2030", 0)}


def col_base_usages(base_archive: Path) -> dict[str, tuple[str, str, str]]:
    """``col:ID -> (scientificName, rank, status)`` of the Fungi usages of a COL
    Base ColDP archive (the Base release matching the XR release)."""
    import zipfile
    out: dict[str, tuple[str, str, str]] = {}
    with zipfile.ZipFile(base_archive) as z:
        member = next(n for n in z.namelist() if n.casefold().endswith("nameusage.tsv"))
        with z.open(member) as handle:
            header = handle.readline().decode("utf-8-sig").rstrip("\r\n").split("\t")
            ix = {name: header.index(f"col:{name}") for name in ("ID", "scientificName", "rank", "status", "kingdom")}
            for raw in handle:
                if b"Fungi" not in raw:
                    continue
                fields = raw.decode("utf-8", "replace").rstrip("\r\n").split("\t")
                if len(fields) > ix["kingdom"] and fields[ix["kingdom"]] == "Fungi":
                    out[fields[ix["ID"]]] = (fields[ix["scientificName"]], fields[ix["rank"]], fields[ix["status"]])
    return out


def base_backing(col_dir: Path, base: dict[str, tuple[str, str, str]], usage_ids: set[str] | None = None) -> dict:
    """Diagnostic only, never identity authority: which COL XR usages of a
    normalized COL directory are Base-backed (same id, name, rank and status in
    the matching Base release) and which are XR-only."""
    tiers: dict[str, set[str]] = {"base_backed": set(), "base_id_differs": set(), "xr_only": set()}
    for record in _iter_taxa(col_dir):
        if usage_ids is not None and record.taxon_id_value not in usage_ids:
            continue
        hit = base.get(record.taxon_id_value)
        tier = ("xr_only" if hit is None else "base_backed"
                if (hit[0], hit[1], hit[2]) == (record.scientific_name, record.rank, record.taxonomic_status)
                else "base_id_differs")
        tiers[tier].add(record.taxon_id_value)
    return tiers


# ----- report ----------------------------------------------------------------

def write_report(build_dir: Path, report: dict) -> None:
    (build_dir / "change-report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    lines = [f"# Taxonomy refresh {report['release_id']}", "",
             f"Outcome: **{report['outcome']}**" + (f" — {report['stop_reason']}" if report.get("stop_reason") else ""), ""]
    for section in ("acquisition", "source_diffs", "continuity", "identity_conflicts", "build", "identity", "comparison",
                    "held_concept_impact", "col_churn", "coverage", "publication"):
        if section in report:
            lines += [f"## {section}", "", "```json",
                      json.dumps(report[section], indent=1, sort_keys=True, ensure_ascii=False)[:6000], "```", ""]
    (build_dir / "change-report.md").write_text("\n".join(lines), encoding="utf-8")


def _source_entry(spec: str, col_source_exclusions: str | None = None) -> dict:
    code, release_dir = spec.split("=", 1)
    release_dir = release_dir.rstrip("/")
    manifest = json.loads((REPO_ROOT / release_dir / "manifest.json").read_text()) \
        if (REPO_ROOT / release_dir / "manifest.json").is_file() else {}
    version = Path(release_dir).name
    if code == "col_xr":
        release = manifest.get("release") or {}
        entry = {"code": code, "normalizer": "col_xr", "release_dir": release_dir, "version": version,
                 "issued_date": release.get("issued_date")}
        if col_source_exclusions:
            entry["source_exclusion_policy"] = col_source_exclusions
        return entry
    return {"code": code, "normalizer": "national_source", "release_dir": release_dir,
            "profile": f"database/taxonomy/national_sources/{code}/{version}/source.json"}


def prepare(args: argparse.Namespace) -> dict:
    build_dir: Path = args.build_dir.resolve()
    if build_dir.exists():
        raise SystemExit(f"build dir {build_dir} already exists")
    build_dir.mkdir(parents=True)
    work = build_dir / "refresh"
    work.mkdir()
    previous_build = args.previous_build.resolve()
    previous_report = json.loads((previous_build / "build-report.json").read_text())
    previous_sqlite = previous_build / f"{previous_report['release_id']}-A.sqlite3"
    recipe = json.loads(args.previous_recipe.read_text())
    report: dict = {"release_id": args.release_id, "outcome": "stopped",
                    "previous_build": {"release_id": previous_report["release_id"],
                                       "sqlite_sha256": previous_report["sqlite"]["sha256"]}}
    try:
        # 1. acquisition gate and refresh recipe
        overrides = {entry["code"]: entry for entry in (_source_entry(spec, args.col_source_exclusions)
                                                         for spec in args.source)}
        report["acquisition"] = {}
        for code, entry in overrides.items():
            report["acquisition"][code] = acquisition_gate(entry, args.archive_root.resolve(), args.fetch, args.python)
        recipe["sources"] = [overrides.get(s["code"], s) for s in recipe["sources"]]
        unchanged = [s["code"] for s in recipe["sources"] if s["code"] not in overrides]
        report["acquisition"]["unchanged_sources"] = unchanged

        # 2. normalize new sources; diffs against the previous build's normalized output
        norm = work / "norm"
        current_dirs, previous_dirs = {}, {}
        for source in recipe["sources"]:
            previous_dirs[source["code"]] = previous_build / "norm" / source["code"]
            if source["code"] not in overrides:
                current_dirs[source["code"]] = previous_dirs[source["code"]]
                continue
            archive = str(args.archive_root.resolve() / source["release_dir"] / "archive.zip")
            out = norm / source["code"]
            if source["normalizer"] == "col_xr":
                argv = [str(SCRIPTS / "normalize_col_xr.py"), "--archive", archive, "--output", str(out),
                        "--source-release-version", source["version"],
                        "--source-release-issued-date", source["issued_date"]]
                if source.get("source_exclusion_policy"):
                    argv += ["--source-exclusion-policy", str(REPO_ROOT / source["source_exclusion_policy"])]
            else:
                argv = [str(SCRIPTS / "national_source.py"), "normalize", "--profile", source["profile"],
                        "--archive", archive, "--output", str(out)]
            result = subprocess.run([args.python, *argv], cwd=REPO_ROOT, capture_output=True, text=True)
            if result.returncode != 0:
                raise RefreshStop(f"normalize {source['code']} failed: {result.stderr[-800:]}")
            current_dirs[source["code"]] = out
        previous_col = next((s for s in json.loads(args.previous_recipe.read_text())["sources"]
                             if s["code"] == "col_xr"), None)
        if "col_xr" in overrides and previous_col and not _has_name_record_ids(previous_dirs["col_xr"]):
            # The identity baseline was normalized before name-record ids were
            # kept: re-normalize the same pinned previous archive (no exclusion
            # policy, so it stays the baseline's COL content) for evidence.
            renorm = work / "previous-norm" / "col_xr"
            argv = [str(SCRIPTS / "normalize_col_xr.py"), "--archive",
                    str(args.archive_root.resolve() / previous_col["release_dir"] / "archive.zip"),
                    "--output", str(renorm), "--source-release-version", previous_col["version"],
                    "--source-release-issued-date", previous_col["issued_date"]]
            result = subprocess.run([args.python, *argv], cwd=REPO_ROOT, capture_output=True, text=True)
            if result.returncode != 0:
                raise RefreshStop(f"re-normalize previous col_xr failed: {result.stderr[-800:]}")
            previous_dirs["col_xr"] = renorm
        report["source_diffs"] = {code: source_diff(previous_dirs[code], current_dirs[code])
                                  for code in overrides}

        # 3. continuity receipt
        _, _, previous_meta = live_concepts(previous_sqlite)
        previous_pins = {code: {"source_release_id": previous_meta[f"source_release[{code}].id"],
                                "sha256": previous_meta[f"source_release[{code}].archive_sha256"]}
                         for code in current_dirs}
        current_pins = {}
        for code, directory in current_dirs.items():
            normalized = json.loads((directory / "report.json").read_text())
            current_pins[code] = {"source_release_id": f"{code}:{_release_string(normalized['profile_source_release'])}",
                                  "sha256": normalized["archive_sha256"]}
        registry_path = work / "registry-base.jsonl"
        build_release.assemble_registry(REPO_ROOT / recipe["registry"], registry_path)
        receipt = carry_forward.build_receipt(
            release_id=args.release_id, previous_dirs=previous_dirs, current_dirs=current_dirs,
            previous_pins=previous_pins, current_pins=current_pins,
            manual_mappings=json.loads((REPO_ROOT / recipe["policies"]["manual_mappings"]).read_text()),
            supersessions=json.loads((REPO_ROOT / recipe["policies"]["concept_supersessions"]).read_text()),
            anchors=carry_forward.registry_anchors(registry_path),
            holds=carry_forward.load_holds(args.holds),
            **dict(zip(("rules", "exceptions"), carry_forward.load_continuity_policy(args.holds))),
            id_continuity=carry_forward.load_id_continuity(args.col_id_continuity),
            nortaxa_fallback=carry_forward.load_nortaxa_fallback(args.col_id_continuity))
        receipt_path = work / "continuity-receipt.json"
        receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
        flagged = {rid: r for rid, r in receipt["records"].items() if r["status"] == "flagged"}
        held = {rid: r for rid, r in receipt["records"].items() if r["status"] == "held"}
        reason_counts: dict[str, int] = {}
        for record in flagged.values():
            for reason in record["reasons"]:
                tail = reason.rsplit(":", 1)[1]
                key = f"{record['kind']}:{reason.split('/', 1)[0]}:{tail}"
                reason_counts[key] = reason_counts.get(key, 0) + 1
        report["continuity"] = {
            "receipt": str(receipt_path), "previous_pins": previous_pins, "current_pins": current_pins,
            "summary": receipt["summary"], "flag_reasons": dict(sorted(reason_counts.items())),
            "flagged_blocking": carry_forward_blockers(receipt),
            "flagged_examples": dict(list(sorted(flagged.items()))[:25]),
            "held_by_owner_decision": held,
            "carried_by_rule_or_exception": {rid: r["carried_by"] for rid, r in receipt["records"].items()
                                             if r.get("carried_by")},
            "col_id_continuity": receipt.get("col_id_continuity"),
            "col_churn_candidates_report_only": col_churn_candidates(
                receipt, previous_dirs["col_xr"], current_dirs["col_xr"]),
        }
        recipe["continuity_receipt"] = {"path": str(receipt_path), "sha256": build_release.sha256_file(receipt_path)}
        if args.vernacular_expectation:
            # An explained, evidenced change of the pinned projection for this candidate.
            expectation = json.loads(args.vernacular_expectation.read_text())
            recipe["vernacular_enrichment"]["expected_pinned_projection"] = expectation["expected_pinned_projection"]
            recipe.setdefault("extra_evidence", {})["vernacular-projection-expectation.json"] = str(
                args.vernacular_expectation.resolve())
            report["vernacular_expectation"] = expectation
        if report["continuity"]["flagged_blocking"]:
            raise RefreshStop(f"{len(report['continuity']['flagged_blocking'])} NorTaxa mappings/supersessions "
                              "are flagged by the continuity receipt; review before building")

        # 4. build (bootstrap when ids are allocated, then the deterministic final build)
        def run_build(name: str, recipe_doc: dict) -> dict:
            recipe_path = work / f"recipe-{name}.json"
            recipe_path.write_text(json.dumps(recipe_doc, indent=2, sort_keys=True) + "\n")
            options = build_release.Options(release_id=args.release_id, build_dir=build_dir / name,
                                            recipe_path=recipe_path, archive_root=args.archive_root.resolve())
            try:
                return build_release.build(options, python=args.python)
            except build_release.BuildError as exc:
                conflicts = registry_conflicts(build_dir / name / "logs" / "compileA.err")
                if conflicts:
                    report["identity_conflicts"] = conflicts
                    raise RefreshStop(f"build {name}: {len(conflicts)} registry alias conflicts (source usages "
                                      "bound to one Sporely id would move to another); see identity_conflicts") from exc
                raise RefreshStop(f"build {name} failed: {str(exc)[-1500:]}") from exc
        build_report = run_build("build-1", recipe)
        report["build"] = {"build-1": _build_summary(build_report)}
        if build_report["registry"]["new_allocations"]:
            registry_dir = work / "registry"
            shard_registry(build_dir / "build-1" / "registry-A.jsonl", registry_dir)
            recipe["registry"] = str(registry_dir)
            build_report = run_build("build-2", recipe)
            report["build"]["build-2"] = _build_summary(build_report)
        final = build_dir / ("build-2" if "build-2" in report["build"] else "build-1")
        report["build"]["final"] = final.name
        diagnostics = json.loads((final / "releaseA" / "diagnostics.json").read_text())
        report["build"]["source_continuity"] = {
            k: (v if k != "approved_mappings_not_applicable" else _count(v, "reason"))
            for k, v in (diagnostics.get("source_continuity") or {}).items()}
        report["build"]["reviewed_identity_only"] = diagnostics.get("counts", {}).get("reviewed_identity_only_sources")
        candidate = final / f"{args.release_id}-A.sqlite3"
        base_anchors = carry_forward.registry_anchors(registry_path)
        previous_col = next((s for s in json.loads(args.previous_recipe.read_text())["sources"]
                             if s["code"] == "col_xr"), None)
        excluded_ids = policy_exclusions(
            current_dirs["col_xr"], base_anchors,
            args.archive_root.resolve() / previous_col["release_dir"] / "archive.zip" if previous_col else None) \
            if "col_xr" in current_dirs else {}
        held_ids = held_by_continuity(receipt, base_anchors)
        report["identity"] = {"vs_previous": identity_comparison(previous_sqlite, candidate, excluded_ids, held_ids)}
        report["comparison"] = {"vs_previous": compare_metrics(previous_sqlite, candidate)}
        for label, path in (args.compare_sqlite or []):
            other = _open_sqlite(Path(path), work)
            report["identity"][f"vs_{label}"] = identity_comparison(other, candidate, excluded_ids, held_ids)
            report["comparison"][f"vs_{label}"] = compare_metrics(other, candidate)
        report["comparison"]["supersession_groups"] = supersession_groups(
            json.loads((REPO_ROOT / recipe["policies"]["concept_supersessions"]).read_text()), candidate)
        report["held_concept_impact"] = concept_impact(receipt, registry_path, final / "registry-A.jsonl",
                                                       previous_sqlite, candidate)
        if "col_xr" in overrides:
            old_col = next((s for s in json.loads(args.previous_recipe.read_text())["sources"]
                            if s["code"] == "col_xr"), None)
            report["col_churn"] = col_churn(previous_dirs["col_xr"], current_dirs["col_xr"],
                                            args.archive_root.resolve() / old_col["release_dir"] / "archive.zip",
                                            carry_forward.registry_anchors(registry_path), receipt)
            if args.col_base_archive:
                # Diagnostic tier of the vanished and live COL anchors against the matching Base release.
                base = col_base_usages(args.col_base_archive)
                anchored = {i[2] for i in carry_forward.registry_anchors(registry_path).values() if i[0] == "col_xr"}
                previous_tiers = base_backing(previous_dirs["col_xr"], base, anchored)
                current_tiers = base_backing(current_dirs["col_xr"], base)
                current_ids = set().union(*current_tiers.values())
                report["col_churn"]["base_backing"] = {
                    "base_archive": str(args.col_base_archive),
                    "base_archive_sha256": build_release.sha256_file(args.col_base_archive),
                    "base_fungi_usages": len(base),
                    "current_usages": {k: len(v) for k, v in current_tiers.items()},
                    "vanished_anchors": {k: len(v - current_ids) for k, v in previous_tiers.items()},
                }
        report["coverage"] = {"candidate": build_report["coverage"]["candidate"],
                              "delta_vs_bundled": build_report["coverage"].get("delta"),
                              "bundled_release_id": build_report["coverage"].get("baseline_release_id")}
        if not build_report["determinism"]["identical"]:
            raise RefreshStop(f"builds differ: {build_report['determinism']['mismatches']}")
        if "frozen" not in build_report:
            raise RefreshStop("the build did not freeze (registry changed or no bundle baseline)")
        report["build"]["freeze_sha256"] = build_report["frozen"]["freeze_sha256"]

        # 5. cloud export + scope, from the frozen bytes
        frozen = final / "frozen"
        gz = frozen / f"{args.release_id}.sqlite3.gz"
        cloud, scope = work / "cloud_export", work / "scope"
        for label, argv in (
                ("cloud_export", [str(SCRIPTS / "export_cloud_taxonomy.py"), "--artifact", str(gz),
                                  "--manifest", str(frozen / "manifest.json"), "--output", str(cloud),
                                  "--policies", str(TAXONOMY / "policies"), "--candidate-validation-only"]),
                ("macrofungi_scope", [str(TAXONOMY / "macrofungi_scope.py"), "--policy",
                                      str(TAXONOMY / "policies" / "global-macrofungi-scope.yml"),
                                      "--source-gz", str(gz), "--w1-dir", str(cloud), "--output-dir", str(scope),
                                      "--desktop", str(scope / f"desktop-{args.release_id}.sqlite3"),
                                      "--evidence", str(work / "scope-evidence.json"),
                                      "--release-id", args.release_id, "--starting-revision", args.revision])):
            result = subprocess.run([args.python, *argv], cwd=REPO_ROOT, capture_output=True, text=True)
            report.setdefault("publication", {})[label] = {"exit": result.returncode,
                                                          "tail": (result.stderr or result.stdout)[-600:]}
            if result.returncode != 0:
                raise RefreshStop(f"{label} failed")
        unexplained = report["identity"]["vs_previous"]["stopped_unexplained"]
        meta = live_concepts(candidate)[2]
        report["publication"].update({
            "release_approval_meta": {k: v for k, v in meta.items() if "approval" in k or k in ("state", "publication")},
            "informational": [f"{n} concepts stop being live: {reason} (registry keeps the allocations)"
                              for reason, n in report["identity"]["vs_previous"]["stopped_excluded_by_policy"].items()]
                             + [f"{n} concepts stop being live: {reason} (withheld until reviewed; the id keeps "
                                "its history)" for reason, n in
                                report["identity"]["vs_previous"]["stopped_held_by_continuity"].items()],
            "blockers": ([f"{len(unexplained)} concepts stop being live without a supersession"] if unexplained else [])
                        + ([f"{len(report['identity']['vs_previous']['policy_excluded_but_live'])} policy-excluded "
                            "concepts are still live"] if report["identity"]["vs_previous"]["policy_excluded_but_live"]
                           else [])
                        + ([f"{len(flagged)} reviewed records flagged (Dyntaxa ones unapplied)"] if flagged else [])
                        + ([f"{len(held)} relationships held by owner decision; impact must be assessed "
                            "(see held_concept_impact)"] if held else [])
                        + ["release decision required for the exact frozen artifact (candidate_build_only "
                           "approvals pending) before promote/export/import"],
        })
        report["outcome"] = "prepared"
    except RefreshStop as stop:
        report["stop_reason"] = str(stop)
    write_report(build_dir, report)
    return report


def registry_conflicts(err_log: Path) -> list[dict]:
    """The compiler's collected registry alias conflicts, from its error log."""
    if not err_log.is_file():
        return []
    text = err_log.read_text(encoding="utf-8", errors="replace")
    if "registry alias conflict" not in text or "[" not in text:
        return []
    try:
        return json.loads(text[text.index("["):text.rindex("]") + 1])
    except ValueError:
        return []


def carry_forward_blockers(receipt: dict) -> list[str]:
    from compile_release import continuity_blockers
    return continuity_blockers(receipt)


def _count(rows: list[dict], key: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        out[row[key]] = out.get(row[key], 0) + 1
    return dict(sorted(out.items()))


def _build_summary(report: dict) -> dict:
    return {"sqlite_sha256": report["sqlite"]["sha256"], "deterministic": report["determinism"]["identical"],
            "registry": report["registry"], "frozen": report.get("frozen"),
            "bridge_emission": report["sqlite"].get("authoritative_bridge_emission")}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--release-id", required=True, help="tax-YYYY.MM.DD-NN")
    parser.add_argument("--build-dir", type=Path, required=True, help="new scratch directory")
    parser.add_argument("--previous-build", type=Path, required=True,
                        help="build_release.py directory of the identity baseline (norm/, A sqlite, report)")
    parser.add_argument("--previous-recipe", type=Path, default=TAXONOMY / "release-recipe.json")
    parser.add_argument("--source", action="append", default=[], metavar="CODE=RELEASE_DIR",
                        help="new source release dir (repository-relative); repeatable")
    parser.add_argument("--archive-root", type=Path, required=True,
                        help="tree holding <release_dir>/archive.zip for every recipe source and the workbook")
    parser.add_argument("--holds", type=Path, default=None,
                        help="owner-decided continuity holds (named records quarantined, not applied)")
    parser.add_argument("--col-id-continuity", type=Path, default=None,
                        help="documented one-to-one COL id replacements (category 1) to keep on their Sporely ids")
    parser.add_argument("--col-source-exclusions", default=None, metavar="POLICY",
                        help="repository-relative scope policy whose approved source_characteristic_exclusions "
                             "(UNITE SH clusters) are not compiled or allocated; recorded in the recipe")
    parser.add_argument("--col-base-archive", type=Path, default=None,
                        help="COL Base ColDP archive matching the XR release: Base-backed/XR-only diagnostic only")
    parser.add_argument("--vernacular-expectation", type=Path, default=None,
                        help="evidence file with this candidate's explained expected_pinned_projection")
    parser.add_argument("--fetch", action="store_true",
                        help="run an approved acquisition when a new source archive is missing")
    parser.add_argument("--compare-sqlite", nargs=2, action="append", metavar=("LABEL", "PATH"),
                        help="extra release to compare live concepts with (.sqlite3 or .gz)")
    parser.add_argument("--revision", default="refresh", help="starting revision recorded by macrofungi_scope")
    parser.add_argument("--python", default=sys.executable)
    return parser


def main(argv: list[str] | None = None) -> int:
    report = prepare(build_parser().parse_args(argv))
    print(f"{report['outcome']}: {report.get('stop_reason', '')}".rstrip(": "))
    return 0 if report["outcome"] == "prepared" else 3


if __name__ == "__main__":
    raise SystemExit(main())
