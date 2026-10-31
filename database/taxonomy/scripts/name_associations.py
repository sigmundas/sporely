#!/usr/bin/env python3
"""Exact-name associations between national source usages and live concepts.

Owner product decision (2026-10-31): when NorTaxa or Dyntaxa name a Fungi
usage with the same canonical scientific name and rank as exactly one live
concept, Sporely treats them as one taxon. Policy:
``policies/name_association_policy.json``.

This module derives a new candidate release from a frozen base release. It
never allocates, retires or renumbers a Sporely id:

* an associated NorTaxa concept keeps its live ``taxon_min`` row and id;
* the association is a row in ``taxon_association_min`` (member -> primary),
  recorded per release, so a later release can drop it by listing it in the
  policy's ``exclusions``;
* the member's source ids, names, vernaculars and Red List rows are projected
  onto the primary concept, each labelled ``name_association:<rule>`` so they
  stay distinguishable from reviewed bridges.

The primary is the COL-backed concept when there is one. A Dyntaxa usage may
also attach to a NorTaxa-only concept when no COL concept carries its name.
"""
from __future__ import annotations

import argparse
import collections
import gzip
import hashlib
import json
import shutil
import sqlite3
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

from cross_source_mapping import _ACCEPTED_STATUSES, _canonical_name as norm  # noqa: E402
from vernacular_projection import QUALIFIED  # noqa: E402

POLICY_FORMAT = "sporely-taxonomy-name-association-policy-v1"
NOTE_PREFIX = "name_association:"
EXACT = "exact_accepted_name"
SYNONYM = "unique_synonym_bridge"
#: Synonym statuses a synonym rule may follow. Misapplied names and pro parte
#: synonyms name other concepts, so they never bridge.
PLAIN_SYNONYM_STATUSES = frozenset({"synonym", "heterotypicSynonym", "homotypicSynonym"})
#: Ranks that are concept groupings rather than names, never associated.
QUALIFIED_RANKS = frozenset({"speciesaggregate", "aggregate", "complex"})
#: Order matters: NorTaxa members collapse onto their primaries before
#: Dyntaxa looks for candidates, so Dyntaxa never sees both sides of a pair.
SOURCE_ORDER = ("nortaxa", "dyntaxa")
TAXON_NAMESPACE = {"nortaxa": "nortaxa_taxon_id", "dyntaxa": "dyntaxa_taxon_id"}
#: Integer table mirror of a source's ids, where the base release keeps one.
INTEGER_SOURCE = {"nortaxa": "artsdatabanken"}
NATIONAL_NAME_COLUMN = {"nortaxa": "no", "dyntaxa": "sv"}

ASSOCIATION_DDL = """
CREATE TABLE taxon_association_min (
    association_id          TEXT PRIMARY KEY,
    primary_taxon_id        INTEGER NOT NULL,
    member_kind             TEXT NOT NULL CHECK (member_kind IN ('concept', 'source_usage')),
    member_taxon_id         INTEGER,
    member_source_system    TEXT NOT NULL,
    member_namespace        TEXT NOT NULL,
    member_external_id      TEXT NOT NULL,
    member_scientific_name  TEXT NOT NULL,
    member_authorship       TEXT,
    member_rank             TEXT NOT NULL,
    member_family           TEXT,
    primary_scientific_name TEXT NOT NULL,
    primary_authorship      TEXT,
    primary_family          TEXT,
    rule                    TEXT NOT NULL,
    via_scientific_name     TEXT,
    flags                   TEXT NOT NULL DEFAULT '[]',
    policy_version          TEXT NOT NULL,
    content_release_id      TEXT NOT NULL,
    FOREIGN KEY (primary_taxon_id) REFERENCES taxon_min(taxon_id),
    FOREIGN KEY (member_taxon_id) REFERENCES taxon_min(taxon_id)
);
CREATE UNIQUE INDEX idx_association_member_taxon
    ON taxon_association_min(member_taxon_id) WHERE member_taxon_id IS NOT NULL;
CREATE UNIQUE INDEX idx_association_member_usage
    ON taxon_association_min(member_source_system, member_namespace, member_external_id);
CREATE INDEX idx_association_primary ON taxon_association_min(primary_taxon_id);
"""


class AssociationError(ValueError):
    pass


@dataclass(frozen=True)
class Usage:
    source: str
    identifier: str
    core_id: str
    name: str
    authorship: str
    rank: str
    status: str
    accepted_id: str | None
    family: str
    qualified: bool

    @property
    def namespace(self) -> str:
        return TAXON_NAMESPACE[self.source]

    @property
    def key(self) -> tuple[str, str]:
        return norm(self.name), self.rank


@dataclass
class Derivation:
    associations: list[dict] = field(default_factory=list)
    blocked: collections.Counter = field(default_factory=collections.Counter)
    blocked_examples: dict = field(default_factory=lambda: collections.defaultdict(list))
    unmatched: collections.Counter = field(default_factory=collections.Counter)

    def block(self, source: str, reason: str, usage: Usage, detail=None) -> None:
        self.blocked[(source, reason, rank_group(usage.rank))] += 1
        examples = self.blocked_examples[(source, reason)]
        if len(examples) < 25:
            examples.append({"usage": usage.identifier, "name": usage.name,
                             "rank": usage.rank, **({"detail": detail} if detail else {})})


def rank_group(rank: str) -> str:
    if rank in ("species", "subspecies", "variety", "form"):
        return "species_and_below"
    return rank if rank == "genus" else "above_genus"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_policy(path: Path) -> dict:
    policy = json.loads(Path(path).read_text(encoding="utf-8"))
    if policy.get("format") != POLICY_FORMAT:
        raise AssociationError(f"{path}: format must be {POLICY_FORMAT}")
    for entry in policy.get("exclusions", []):
        if not entry.get("member") or not entry.get("reason"):
            raise AssociationError(f"{path}: every exclusion needs member and reason")
    return policy


def load_holds(path: Path | None) -> set[tuple[str, str, str]]:
    if path is None:
        return set()
    holds = json.loads(Path(path).read_text(encoding="utf-8")).get("holds", [])
    return {tuple(h["source_usage"].split("/", 2)) for h in holds if h.get("source_usage")}


def _qualified(record: dict) -> bool:
    texts = [record.get("scientific_name") or "", record.get("authorship") or ""]
    texts += [v for v in (record.get("concept_annotation") or {}).values() if isinstance(v, str)]
    return any(QUALIFIED.search(t) for t in texts if t) or \
        (record.get("rank") or "").lower() in QUALIFIED_RANKS


def load_source_usages(path: Path, source: str) -> list[Usage]:
    """Fungi usages of one normalized national source (``norm/<source>/taxa.jsonl``)."""
    out = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            record = json.loads(line)
            classification = record.get("classification") or {}
            if classification.get("kingdom") != "Fungi" or record.get("source_code") != source:
                continue
            accepted = record.get("accepted_name_usage_id") or {}
            out.append(Usage(
                source=source,
                identifier=str(record["taxon_id"]["value"]),
                core_id=str(record["core_row_id"]["value"]),
                name=(record.get("scientific_name") or "").strip(),
                authorship=(record.get("authorship") or "").strip(),
                rank=(record.get("rank") or "").strip().lower(),
                status=(record.get("taxonomic_status") or "").strip(),
                accepted_id=str(accepted["value"]) if accepted.get("value") else None,
                family=(classification.get("family") or "").strip(),
                qualified=_qualified(record),
            ))
    return out


def load_col_authorship(path: Path) -> dict[str, tuple[str, str]]:
    out = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            record = json.loads(line)
            out[str(record["taxon_id"]["value"])] = (
                (record.get("authorship") or "").strip(),
                ((record.get("classification") or {}).get("family") or "").strip())
    return out


def _excluded(policy: dict) -> set[tuple[str, str, str]]:
    out = set()
    for entry in policy.get("exclusions", []):
        member = entry["member"]
        out.add((member["source"], member["namespace"], str(member["identifier"])))
    return out


def _authorship_key(value: str) -> str:
    return "".join(ch for ch in norm(value or "") if ch.isalnum())


def derive(conn: sqlite3.Connection, usages: dict[str, list[Usage]], policy: dict,
           holds: set[tuple[str, str, str]], col_authorship: dict[str, tuple[str, str]]) -> Derivation:
    """Derive associations from the base release and the national usages."""
    excluded = _excluded(policy)
    result = Derivation()
    concepts = {}
    for row in conn.execute(
            "SELECT taxon_id, canonical_scientific_name, taxon_rank, canonical_source_system, "
            "canonical_external_id, family, taxonomic_status FROM taxon_min"):
        concepts[row[0]] = row
    by_name: dict[tuple[str, str], set[int]] = collections.defaultdict(set)
    anchor: dict[tuple[str, str], int] = {}
    for tid, name, rank, css, ext, _family, status in concepts.values():
        if css == "nortaxa":
            anchor[("nortaxa", str(ext))] = tid
        if css in ("col_xr", "nortaxa") and name and (status or "").lower() in _ACCEPTED_STATUSES:
            by_name[(norm(name), (rank or "").lower())].add(tid)
    bridged: dict[tuple[str, str, str], set[int]] = collections.defaultdict(set)
    claimed: dict[tuple[str, int], set[str]] = collections.defaultdict(set)
    for tid, source, ns, ext, role in conn.execute(
            "SELECT taxon_id, source_system, namespace, external_id, id_role "
            "FROM taxon_external_id_text_min"):
        bridged[(source, ns, ext)].add(tid)
        if role == "accepted":
            claimed[(source, tid)].add(ext)
    primary_of: dict[int, int] = {}

    for source in SOURCE_ORDER:
        rows = usages.get(source, [])
        accepted = [u for u in rows if u.status.lower() in _ACCEPTED_STATUSES]
        accepted_by_key = collections.defaultdict(list)
        for u in accepted:
            accepted_by_key[u.key].append(u)
        synonyms_of = collections.defaultdict(list)
        synonym_targets = collections.defaultdict(set)
        for u in rows:
            if u.status in PLAIN_SYNONYM_STATUSES and u.accepted_id:
                synonyms_of[u.accepted_id].append(u)
                synonym_targets[u.key].add(u.accepted_id)

        def candidates(key, own):
            found = set()
            for tid in by_name.get(key, ()):
                if tid == own:
                    continue
                if source == "nortaxa" and concepts[tid][3] != "col_xr":
                    continue
                found.add(primary_of.get(tid, tid))
            return found

        proposals = []
        for u in sorted(accepted, key=lambda u: u.identifier):
            usage_key = (source, u.namespace, u.identifier)
            own = anchor.get((source, u.identifier))
            if usage_key in holds:
                result.block(source, "continuity_hold", u)
                continue
            if usage_key in excluded:
                result.block(source, "reviewed_exclusion", u)
                continue
            if bridged.get(usage_key):
                result.block(source, "already_bridged", u)
                continue
            if u.qualified:
                result.block(source, "qualified_concept", u)
                continue
            if len(accepted_by_key[u.key]) > 1:
                result.block(source, "source_homonym", u)
                continue
            rule, via, found = EXACT, None, candidates(u.key, own)
            if not found:
                bridges = {}
                for syn in synonyms_of.get(u.identifier, ()):
                    if syn.qualified or syn.key == u.key:
                        continue
                    if synonym_targets[syn.key] != {u.identifier} or accepted_by_key.get(syn.key):
                        continue
                    for tid in candidates(syn.key, own):
                        bridges.setdefault(tid, syn.name)
                if len(bridges) == 1:
                    (tid, via), = bridges.items()
                    rule, found = SYNONYM, {tid}
                elif bridges:
                    rule, found = SYNONYM, set(bridges)
            if not found:
                result.unmatched[(source, rank_group(u.rank))] += 1
                continue
            if len(found) > 1:
                result.block(source, "multiple_primary_candidates", u,
                             sorted(found)[:5] if rule == EXACT else f"{rule}:{sorted(found)[:5]}")
                continue
            primary = next(iter(found))
            if claimed.get((source, primary), set()) - {u.identifier}:
                result.block(source, "primary_claimed_by_other_usage", u, primary)
                continue
            proposals.append((u, own, primary, rule, via))

        per_primary = collections.defaultdict(list)
        for proposal in proposals:
            per_primary[proposal[2]].append(proposal)
        for primary, group in sorted(per_primary.items()):
            exact = [p for p in group if p[3] == EXACT]
            keep = exact if exact else group
            if len(keep) > 1:
                for p in group:
                    result.block(source, "primary_claimed_by_other_usage", p[0], primary)
                continue
            for p in group:
                if p is not keep[0]:
                    result.block(source, "primary_claimed_by_other_usage", p[0], primary)
            u, own, primary, rule, via = keep[0]
            pc = concepts[primary]
            p_auth, p_family = col_authorship.get(str(pc[4]), ("", "")) if pc[3] == "col_xr" else ("", "")
            p_family = p_family or (pc[5] or "")
            flags = []
            if _authorship_key(u.authorship) != _authorship_key(p_auth):
                flags.append("authorship_differs")
            if u.family and p_family and norm(u.family) != norm(p_family):
                flags.append("family_differs")
            broader = sorted({
                syn.name for syn in synonyms_of.get(u.identifier, ())
                if syn.name != via and candidates(syn.key, own) - {primary}})
            if broader:
                flags.append("broader_concept_signal")
            result.associations.append({
                "association_id": f"{source}:{u.identifier}",
                "primary_taxon_id": primary,
                "member_kind": "concept" if own is not None else "source_usage",
                "member_taxon_id": own,
                "member_source_system": source,
                "member_namespace": u.namespace,
                "member_external_id": u.identifier,
                "member_core_id": u.core_id,
                "member_scientific_name": u.name,
                "member_authorship": u.authorship,
                "member_rank": u.rank,
                "member_family": u.family,
                "primary_scientific_name": pc[1],
                "primary_authorship": p_auth,
                "primary_family": p_family,
                "rule": rule,
                "via_scientific_name": via,
                "flags": flags,
                "broader_synonyms": broader,
                "synonyms": [{"identifier": s.identifier, "name": s.name}
                             for s in sorted(synonyms_of.get(u.identifier, ()), key=lambda s: s.identifier)
                             if not s.qualified],
            })
            claimed[(source, primary)].add(u.identifier)
            if own is not None:
                primary_of[own] = primary
    result.associations.sort(key=lambda a: (a["member_source_system"], a["member_external_id"]))
    return result


def _vernaculars_from_norm(path: Path, core_ids: set[str]) -> dict[str, list[tuple[str, str, bool]]]:
    out = collections.defaultdict(list)
    if not path.exists():
        return out
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            record = json.loads(line)
            core = str((record.get("core_row_id") or {}).get("value"))
            if core in core_ids and record.get("vernacular_name"):
                out[core].append((record.get("language") or "", record["vernacular_name"].strip(),
                                  bool(record.get("is_preferred"))))
    return out


def apply_overlay(conn: sqlite3.Connection, derivation: Derivation, *, release_id: str,
                  policy: dict, norm_dir: Path) -> dict:
    """Write ``taxon_association_min`` and the projections onto primaries."""
    version = policy["policy_version"]
    counts = collections.Counter()
    conn.executescript(ASSOCIATION_DDL)
    vernaculars = {
        source: _vernaculars_from_norm(Path(norm_dir) / source / "vernacular.jsonl", {
            a["member_core_id"] for a in derivation.associations
            if a["member_source_system"] == source and a["member_kind"] == "source_usage"})
        for source in SOURCE_ORDER}
    existing_text = {(r[0], r[1], r[2], r[3]) for r in conn.execute(
        "SELECT taxon_id, source_system, namespace, external_id FROM taxon_external_id_text_min")}
    text_owner = collections.defaultdict(set)
    for tid, s, ns, e in existing_text:
        text_owner[(s, ns, e)].add(tid)

    def add_text(tid, source, ns, ext, role, name, note):
        if (tid, source, ns, ext) in existing_text or text_owner.get((source, ns, ext)):
            return
        conn.execute(
            "INSERT INTO taxon_external_id_text_min (taxon_id, source_system, namespace, external_id, "
            "id_role, is_preferred, external_name, note) VALUES (?,?,?,?,?,0,?,?)",
            (tid, source, ns, ext, role, name, note))
        existing_text.add((tid, source, ns, ext))
        text_owner[(source, ns, ext)].add(tid)
        counts[f"external_id_text:{source}:{role}"] += 1

    def add_integer(tid, source, ext, role, name, note):
        cur = conn.execute(
            "INSERT OR IGNORE INTO taxon_external_id_min (taxon_id, source_system, external_id, id_role, "
            "is_preferred, external_name, note) VALUES (?,?,?,?,0,?,?)",
            (tid, source, int(ext), role, name, note))
        counts[f"external_id_integer:{source}:{role}"] += cur.rowcount

    def add_scientific(tid, name, source, note):
        cur = conn.execute(
            "INSERT OR IGNORE INTO scientific_name_min (taxon_id, language_code, scientific_name, "
            "is_preferred_name, source, note) VALUES (?, 'sci', ?, 0, ?, ?)", (tid, name, source, note))
        counts["scientific_name"] += cur.rowcount

    def add_vernacular(tid, lang, name, preferred, source):
        if not lang or not name:
            return
        if preferred and conn.execute(
                "SELECT 1 FROM vernacular_min WHERE taxon_id=? AND language_code=? AND is_preferred_name=1",
                (tid, lang)).fetchone():
            preferred = False
        cur = conn.execute(
            "INSERT OR IGNORE INTO vernacular_min (taxon_id, language_code, vernacular_name, "
            "is_preferred_name, source) VALUES (?,?,?,?,?)", (tid, lang, name, int(preferred), source))
        counts["vernacular"] += cur.rowcount

    for a in derivation.associations:
        conn.execute(
            "INSERT INTO taxon_association_min VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (a["association_id"], a["primary_taxon_id"], a["member_kind"], a["member_taxon_id"],
             a["member_source_system"], a["member_namespace"], a["member_external_id"],
             a["member_scientific_name"], a["member_authorship"], a["member_rank"], a["member_family"],
             a["primary_scientific_name"], a["primary_authorship"], a["primary_family"], a["rule"],
             a["via_scientific_name"], json.dumps(a["flags"]), version, release_id))
        primary, member, source = a["primary_taxon_id"], a["member_taxon_id"], a["member_source_system"]
        note = f"{NOTE_PREFIX}{a['rule']}"
        ns = a["member_namespace"]
        add_text(primary, source, ns, a["member_external_id"], "accepted", a["member_scientific_name"], note)
        if source in INTEGER_SOURCE:
            add_integer(primary, INTEGER_SOURCE[source], a["member_external_id"], "accepted",
                        a["member_scientific_name"], note)
        if norm(a["member_scientific_name"]) != norm(a["primary_scientific_name"]):
            add_scientific(primary, a["member_scientific_name"], source, note)
        for syn in a["synonyms"]:
            add_scientific(primary, syn["name"], source, note)
            if source == "nortaxa":
                add_text(primary, source, ns, syn["identifier"], "synonym", syn["name"], note)
                add_integer(primary, INTEGER_SOURCE[source], syn["identifier"], "synonym", syn["name"], note)
        if member is not None:
            for lang, name, pref, vsource in conn.execute(
                    "SELECT language_code, vernacular_name, is_preferred_name, source FROM vernacular_min "
                    "WHERE taxon_id = ? ORDER BY vernacular_id", (member,)).fetchall():
                add_vernacular(primary, lang, name, bool(pref), vsource)
            for (sname, ssource) in conn.execute(
                    "SELECT scientific_name, source FROM scientific_name_min WHERE taxon_id = ? "
                    "ORDER BY scientific_name_id", (member,)).fetchall():
                if norm(sname) != norm(a["primary_scientific_name"]):
                    add_scientific(primary, sname, ssource, note)
            for (s, ext, role, name) in conn.execute(
                    "SELECT source_system, external_id, id_role, external_name FROM taxon_external_id_min "
                    "WHERE taxon_id = ? AND source_system IN ('artportalen', 'inaturalist') "
                    "ORDER BY external_id_row_id", (member,)).fetchall():
                if not conn.execute("SELECT 1 FROM taxon_external_id_min WHERE taxon_id=? AND source_system=? "
                                    "AND id_role=?", (primary, s, role)).fetchone():
                    add_integer(primary, s, ext, role, name, note)
            cur = conn.execute(
                "UPDATE taxon_min SET inaturalist_taxon_id = (SELECT inaturalist_taxon_id FROM taxon_min "
                "WHERE taxon_id = ?) WHERE taxon_id = ? AND inaturalist_taxon_id IS NULL "
                "AND (SELECT inaturalist_taxon_id FROM taxon_min WHERE taxon_id = ?) IS NOT NULL",
                (member, primary, member))
            counts["inaturalist_column"] += cur.rowcount
            cur = conn.execute("UPDATE taxon_redlist_min SET taxon_id = ? WHERE taxon_id = ?",
                               (primary, member))
            counts["redlist_repointed"] += cur.rowcount
        else:
            for lang, name, pref in vernaculars[source].get(a["member_core_id"], ()):
                add_vernacular(primary, lang, name, pref, source)
        if source == "nortaxa":
            cur = conn.execute(
                "UPDATE taxon_redlist_min SET taxon_id = ? WHERE taxon_id IS NULL "
                "AND assessed_name_namespace = 'artsnavnebase_scientific_name_id' AND assessed_name_id = ? "
                "AND lower(trim(scientific_name_snapshot)) = lower(trim(?))",
                (primary, a["member_external_id"], a["member_scientific_name"]))
            counts["redlist_resolved"] += cur.rowcount
        country = NATIONAL_NAME_COLUMN[source]
        cur = conn.execute(
            f"UPDATE taxon_min SET preferred_scientific_name_{country} = ?, "
            f"preferred_scientific_name_{country}_source_system = ?, "
            f"preferred_scientific_name_{country}_namespace = ?, "
            f"preferred_scientific_name_{country}_external_id = ? "
            f"WHERE taxon_id = ? AND preferred_scientific_name_{country} IS NULL",
            (a["member_scientific_name"], source, ns, a["member_external_id"], primary))
        counts[f"preferred_scientific_name_{country}"] += cur.rowcount
    return dict(sorted(counts.items()))


def report(derivation: Derivation, counts: dict) -> dict:
    by_rule = collections.Counter(
        (a["member_source_system"], a["member_kind"], a["rule"], rank_group(a["member_rank"]))
        for a in derivation.associations)
    flags = collections.Counter(
        (a["member_source_system"], flag) for a in derivation.associations for flag in a["flags"])
    flatten = lambda c: {"|".join(map(str, k)): v for k, v in sorted(c.items())}
    return {
        "associations_total": len(derivation.associations),
        "associations": flatten(by_rule),
        "flags": flatten(flags),
        "blocked": flatten(derivation.blocked),
        "blocked_examples": {"|".join(k): v for k, v in sorted(derivation.blocked_examples.items())},
        "unmatched_national_usages": flatten(derivation.unmatched),
        "projection_counts": counts,
    }


def write_manifest(base_manifest: Path, out_dir: Path, release_id: str, summary: dict) -> Path:
    """The candidate's outer ``manifest.json``, derived from the base release's.

    Keeps the base's Red List block (same workbook) and drops the base's
    freeze and compiler-evidence pointers, which describe the base, not this
    candidate.
    """
    manifest = json.loads(Path(base_manifest).read_text(encoding="utf-8"))
    if manifest.get("content_release_id") != summary["base_release_id"] or \
            manifest.get("sqlite_sha256") != summary["base_sqlite_sha256"]:
        raise AssociationError("base manifest does not describe the base SQLite")
    for key in ("freeze_artifact", "compiler_evidence"):
        manifest.pop(key, None)
    gz = out_dir / f"{release_id}.sqlite3.gz"
    sqlite = out_dir / f"{release_id}.sqlite3"
    manifest.update({
        "content_release_id": release_id,
        "state": "candidate",
        "publication": "none",
        "gz_artifact": gz.name,
        "gz_sha256": summary["gz_sha256"],
        "gz_bytes": gz.stat().st_size,
        "sqlite_sha256": summary["sqlite_sha256"],
        "sqlite_bytes": sqlite.stat().st_size,
        "name_association": {
            "base_release_id": summary["base_release_id"],
            "base_sqlite_sha256": summary["base_sqlite_sha256"],
            "policy_version": summary["policy_version"],
            "policy_sha256": summary["policy_sha256"],
            "associations_total": summary["associations_total"],
            "report": "association-report.json",
        },
    })
    path = out_dir / "manifest.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def build(*, base_gz: Path, base_sqlite_sha256: str, norm_dir: Path, policy_path: Path,
          holds_path: Path | None, release_id: str, out_dir: Path,
          base_manifest: Path | None = None) -> dict:
    """Derive ``<release_id>.sqlite3`` (and its gz) in ``out_dir`` from a frozen base."""
    policy = load_policy(policy_path)
    out_dir.mkdir(parents=True, exist_ok=True)
    sqlite_path = out_dir / f"{release_id}.sqlite3"
    with tempfile.TemporaryDirectory(dir=out_dir) as tmp:
        work = Path(tmp) / "work.sqlite3"
        with gzip.open(base_gz, "rb") as src, open(work, "wb") as dst:
            shutil.copyfileobj(src, dst)
        if sha256_file(work) != base_sqlite_sha256:
            raise AssociationError("base SQLite does not match the pinned sha256")
        conn = sqlite3.connect(work)
        try:
            meta = dict(conn.execute("SELECT key, value FROM taxonomy_meta"))
            base_release = meta["content_release_id"]
            inputs = {}
            usages = {}
            for source in SOURCE_ORDER:
                path = Path(norm_dir) / source / "taxa.jsonl"
                inputs[f"norm/{source}/taxa.jsonl"] = sha256_file(path)
                vpath = Path(norm_dir) / source / "vernacular.jsonl"
                if vpath.exists():
                    inputs[f"norm/{source}/vernacular.jsonl"] = sha256_file(vpath)
                usages[source] = load_source_usages(path, source)
            col_path = Path(norm_dir) / "col_xr" / "taxa.jsonl"
            inputs["norm/col_xr/taxa.jsonl"] = sha256_file(col_path)
            derivation = derive(conn, usages, policy, load_holds(holds_path), load_col_authorship(col_path))
            counts = apply_overlay(conn, derivation, release_id=release_id, policy=policy, norm_dir=norm_dir)
            policy_sha = sha256_file(policy_path)
            updates = {
                "content_release_id": release_id,
                "state": "candidate",
                "publication": "none",
                "release_approval": "pending:candidate_build_only",
                "name_association.policy_version": policy["policy_version"],
                "name_association.policy_sha256": policy_sha,
                "name_association.base_release_id": base_release,
                "name_association.base_sqlite_sha256": base_sqlite_sha256,
                "name_association.count": str(len(derivation.associations)),
            }
            for key, value in updates.items():
                conn.execute("INSERT OR REPLACE INTO taxonomy_meta (key, value) VALUES (?, ?)", (key, value))
            conn.execute("UPDATE taxon_min SET sporely_content_release_id = ? "
                         "WHERE sporely_content_release_id = ?", (release_id, base_release))
            conn.commit()
            conn.execute("VACUUM")
        finally:
            conn.close()
        work.replace(sqlite_path)
    gz_path = out_dir / f"{release_id}.sqlite3.gz"
    with open(sqlite_path, "rb") as src, gzip.GzipFile(gz_path, "wb", mtime=0) as dst:
        shutil.copyfileobj(src, dst)
    summary = {
        "format": "sporely-name-association-build-v1",
        "content_release_id": release_id,
        "base_release_id": base_release,
        "base_sqlite_sha256": base_sqlite_sha256,
        "policy_version": policy["policy_version"],
        "policy_sha256": policy_sha,
        "inputs_sha256": inputs,
        "sqlite_sha256": sha256_file(sqlite_path),
        "gz_sha256": sha256_file(gz_path),
        **report(derivation, counts),
    }
    (out_dir / "association-report.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    with open(out_dir / "associations.jsonl", "w", encoding="utf-8") as fh:
        for a in derivation.associations:
            fh.write(json.dumps(a, ensure_ascii=False, sort_keys=True) + "\n")
    if base_manifest is not None:
        write_manifest(base_manifest, out_dir, release_id, summary)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base-gz", type=Path, required=True)
    parser.add_argument("--base-sqlite-sha256", required=True)
    parser.add_argument("--norm-dir", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--holds", type=Path, default=None)
    parser.add_argument("--base-manifest", type=Path, default=None,
                        help="the base release's manifest.json; writes the candidate's manifest.json")
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = build(base_gz=args.base_gz, base_sqlite_sha256=args.base_sqlite_sha256,
                    norm_dir=args.norm_dir, policy_path=args.policy, holds_path=args.holds,
                    release_id=args.release_id, out_dir=args.out_dir,
                    base_manifest=args.base_manifest)
    print(json.dumps({k: summary[k] for k in ("content_release_id", "sqlite_sha256",
                                              "associations_total", "associations", "blocked")},
                     indent=1, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
