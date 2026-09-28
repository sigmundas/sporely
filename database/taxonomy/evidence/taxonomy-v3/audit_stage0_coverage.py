#!/usr/bin/env python3
"""Taxonomy v3 Stage 0: bridge coverage and duplicate-candidate audit.

Recomputes, against the tracked release artifact and its pinned source
archives, the two populations the Taxonomy v3 plan works on:

* **Group A** — a NorTaxa taxonID carried on a COL-canonical concept by the
  compiler's automatic exact match (``note = cross_source_automatic_exact``).
  Automatic matches are not authoritative, so none of them is published as a
  bridge.
* **Group B** — a NorTaxa-canonical concept and a COL-canonical concept that
  share a ``canonical_scientific_name`` but are separate Sporely concepts. Name
  equality only sizes this population; it proves nothing.

Every association in both groups is graded by
``scripts/cross_reference_evidence.py`` — the sources' own published synonymy
— and each group is reported for the full release and for the cloud scope
(``global_macrofungi_policy_v1``, computed in-process with
``macrofungi_scope`` exactly as the scoped export builds it).

Group A is also written out as one immutable candidate manifest per evidence
class. A manifest lists every association with its evidence detail, is pinned
to the release and source-archive fingerprints, and carries a SHA-256 over its
members. Manifests are review inputs: every member is ``needs_review``. The
``shared_synonymy`` manifest is further partitioned by the fixed rules in
``SHARED_REVIEW_CLASS_TESTS``, stated over the per-synonym kinds of
``SYNONYM_KIND_TESTS``, into one review manifest per class, so strong evidence
can be approved without approving the weak cases with it; the parent stays
whole for accounting.

Read-only over the release, archives, policies and registry. Fails closed if an
archive does not match the fingerprint the release records for it. Writes only
into ``--out-dir``; output is byte-deterministic.

    .venv/bin/python database/taxonomy/evidence/taxonomy-v3/audit_stage0_coverage.py
"""
from __future__ import annotations

import argparse
import collections
import gzip
import hashlib
import json
import re
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path

_REPO = Path(__file__).resolve().parents[4]
_TAXONOMY = _REPO / "database" / "taxonomy"
for _path in (_REPO, _TAXONOMY / "scripts"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from cross_reference_evidence import (  # noqa: E402
    EVIDENCE_NONE,
    EVIDENCE_ONE_DIRECTIONAL,
    EVIDENCE_RECIPROCAL,
    EVIDENCE_SHARED_SYNONYMY,
    REVIEWABLE,
    grade,
    read_col,
    read_col_sources,
    read_nortaxa_title,
    read_nortaxa,
    shared_synonym_keys,
)
from bridge_emission import (  # noqa: E402
    EVIDENCE_CLASS_INTRA_SOURCE_SYNONYM,
    BridgeEmissionPolicy,
)
from database.taxonomy import cloud_export, macrofungi_scope  # noqa: E402

EVIDENCE_CLASSES = (EVIDENCE_RECIPROCAL, EVIDENCE_ONE_DIRECTIONAL,
                    EVIDENCE_SHARED_SYNONYMY, EVIDENCE_NONE)

GENERATED = Path("database/reference_data/generated/taxonomy_v2")
DEFAULT_MANIFEST = GENERATED / "manifest.json"
SCOPE_POLICY = Path("database/taxonomy/policies/global-macrofungi-scope.yml")
DEFAULT_NORTAXA = Path("database/taxonomy/sources/nortaxa/1.284/archive.zip")
DEFAULT_COL = Path("database/taxonomy/sources/col_xr/2026-07-17-XR/archive.zip")
DEFAULT_OUT = Path("database/taxonomy/evidence/taxonomy-v3/stage0")
MAPPING_POLICY = Path("database/taxonomy/policies/mapping_policy.yml")

#: Raw-row note -> the bridge evidence class the compiler graded it under,
#: for rows that were not re-keyed by a reviewed supersession.
_NOTE_EVIDENCE_CLASS = {"synonym_of_accepted": EVIDENCE_CLASS_INTRA_SOURCE_SYNONYM}

#: Reviewed (non-automatic) NorTaxa bridge notes the compiler writes.
REVIEWED_NOTES = ("manual_approved_exact", "reviewed_supersession")

#: The plan's regression species, located by identifier, never by name.
REGRESSIONS = {
    "Entoloma conferendum": {"nortaxa_taxon_id": "53482"},
    "Pholiotina rugosa": {"nortaxa_taxon_id": "52369"},
    "Craterellus tubaeformis": {"nortaxa_taxon_id": "56227"},
    # Canonical names differ, so this pair is outside the name-equality
    # Group B; it is graded as an explicit pair instead.
    "Conocybe vexans / Pholiotina vexans": {"nortaxa_taxon_id": "58766",
                                            "graded_pair": ["58766", "XQZ6"]},
    "Cantharellus cibarius": {"sporely_taxon_id": 168873},
    # sporely-web pins NBIC:56449 as an id that does not resolve. It is a real
    # Group-B taxon (Gloeophyllum odoratum), so it is a temporary fixture: a
    # Stage 2 reconciliation could make it resolve.
    "NBIC:56449 (temporary unresolved fixture)": {"nortaxa_taxon_id": "56449"},
}

_SPECIES_RANK = "species"
_INFRASPECIFIC_RANKS = frozenset({"subspecies", "variety", "form"})
_LOW_OVERLAP_MIN_SYNONYMS = 5

MANIFEST_COLUMNS = [
    "nortaxa_taxon_id",
    "col_usage_id",
    "sporely_taxon_id",
    "in_cloud_scope",
    "bridge_accepted_name",
    "backbone_accepted_name",
    "accepted_names_agree",
    "bridge_lists_backbone_accepted_name_as_synonym",
    "backbone_lists_bridge_accepted_name_as_synonym",
    "shared_synonym_count",
    "shared_synonyms",
    "bridge_synonym_count",
    "backbone_synonym_count",
]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def _write_json(path: Path, value) -> str:
    data = (json.dumps(value, ensure_ascii=False, indent=1, sort_keys=True)
            + "\n").encode("utf-8")
    path.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


# ------------------------------------------------------------------ inputs ---


def verify_release(manifest_path: Path, sqlite_path: Path) -> dict:
    """Check the tracked artifact against its manifest; return fingerprints."""
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    gz_path = manifest_path.parent / manifest["gz_artifact"]
    gz_sha = _sha256_file(gz_path)
    if gz_sha != manifest["gz_sha256"]:
        raise SystemExit(f"release artifact does not match manifest: {gz_path}")
    with gzip.open(gz_path, "rb") as source, sqlite_path.open("wb") as target:
        shutil.copyfileobj(source, target, 1024 * 1024)
    if _sha256_file(sqlite_path) != manifest["sqlite_sha256"]:
        raise SystemExit("decompressed release does not match manifest")
    return {
        "content_release_id": manifest["content_release_id"],
        "gz_artifact": str(GENERATED / manifest["gz_artifact"]),
        "gz_sha256": gz_sha,
        "sqlite_sha256": manifest["sqlite_sha256"],
    }


def verify_archive(conn: sqlite3.Connection, source: str, path: Path) -> dict:
    """Fail closed unless the archive is the one the release was built from."""
    meta = dict(conn.execute("SELECT key, value FROM taxonomy_meta"))
    expected = meta[f"source_release[{source}].archive_sha256"]
    actual = _sha256_file(path)
    if actual != expected:
        raise SystemExit(
            f"{source} archive {path} has sha256 {actual}, but the release "
            f"was built from {expected}")
    # No path: the fingerprint identifies the archive wherever it lives.
    return {"sha256": actual,
            "source_release_id": meta[f"source_release[{source}].id"]}


def cloud_scope(conn: sqlite3.Connection, policy_path: Path) -> set[int]:
    """Concepts the ``global_macrofungi_policy_v1`` export carries.

    Mirrors ``macrofungi_scope.build_export``: included concepts plus their
    required classification ancestors, restricted to the W1 concept set the
    scoped export filters from.
    """
    policy = macrofungi_scope.load_policy(policy_path)
    taxa, by_col = macrofungi_scope.load_taxa(conn)
    rules = macrofungi_scope.resolve_rules(policy, by_col)
    results = macrofungi_scope.evaluate(
        taxa, rules, policy.get("source_characteristic_exclusions", []))
    included = {t for t, r in results.items() if r["state"] == "include"}
    exported = included | macrofungi_scope.required_ancestors(taxa, included)
    conn.row_factory = sqlite3.Row
    try:
        w1 = set(cloud_export.build_concept_set(conn).concept_ids)
    finally:
        conn.row_factory = None
    return exported & w1


def group_a(conn: sqlite3.Connection) -> list[tuple[str, str, int]]:
    """``(nortaxa_taxon_id, col_usage_id, sporely_taxon_id)`` per automatic row."""
    return [
        (str(ext), str(col), int(tid))
        for ext, col, tid in conn.execute(
            "SELECT e.external_id, t.canonical_external_id, t.taxon_id "
            "FROM taxon_external_id_min e JOIN taxon_min t USING (taxon_id) "
            "WHERE e.source_system = 'artsdatabanken' "
            "AND e.note = 'cross_source_automatic_exact' "
            "AND t.canonical_source_system = 'col_xr' "
            "ORDER BY e.external_id, t.taxon_id")
    ]


def group_a_unfiltered_count(conn: sqlite3.Connection) -> int:
    return conn.execute(
        "SELECT count(*) FROM taxon_external_id_min "
        "WHERE source_system = 'artsdatabanken' "
        "AND note = 'cross_source_automatic_exact'").fetchone()[0]


def group_b(conn: sqlite3.Connection) -> list[tuple[str, int, str, int]]:
    """``(nortaxa_taxon_id, nortaxa_sporely_id, col_usage_id, col_sporely_id)``."""
    return [
        (str(n_ext), int(n_id), str(c_ext), int(c_id))
        for n_ext, n_id, c_ext, c_id in conn.execute(
            "SELECT n.canonical_external_id, n.taxon_id, "
            "       c.canonical_external_id, c.taxon_id "
            "FROM taxon_min n JOIN taxon_min c "
            "  ON c.canonical_scientific_name = n.canonical_scientific_name "
            " AND c.canonical_source_system = 'col_xr' "
            "WHERE n.canonical_source_system = 'nortaxa' "
            "ORDER BY n.canonical_external_id, c.canonical_external_id")
    ]


def authoritative_nortaxa_emission(conn: sqlite3.Connection,
                                   scope: set[int]) -> list[dict]:
    """NorTaxa identifiers the scoped export publishes as authoritative.

    Mirrors both sources of ``cloud_export.emit_taxon_external_id_authoritative``
    restricted to NorTaxa: namespaced ``taxon_external_id_text_min`` rows and
    the derived ``taxon_min.norwegian_taxon_id`` row, each on a scoped concept.
    """
    rows = conn.execute(
        "SELECT external_id, taxon_id, id_role, note "
        "FROM taxon_external_id_text_min "
        "WHERE source_system = 'nortaxa' AND namespace = 'nortaxa_taxon_id' "
        "UNION ALL "
        "SELECT CAST(norwegian_taxon_id AS TEXT), taxon_id, 'accepted', "
        "'derived_from_taxon_min.norwegian_taxon_id' FROM taxon_min "
        "WHERE norwegian_taxon_id IS NOT NULL")
    return sorted(
        ({"nortaxa_taxon_id": str(ext), "sporely_taxon_id": int(tid),
          "id_role": role, "note": note}
         for ext, tid, role, note in rows if int(tid) in scope),
        key=lambda r: (r["sporely_taxon_id"], r["nortaxa_taxon_id"]))


def reconcile_emitting_hosts(conn: sqlite3.Connection, emitted: list[dict],
                             policy: BridgeEmissionPolicy) -> list[dict]:
    """Account for every raw NorTaxa row on a concept that emits a bridge.

    Each raw ``taxon_external_id_min`` row is either published or not; an
    unpublished row carries the policy's rejection reason for its evidence
    class. Fails closed on a reviewed row that is not published, or a
    published identifier with no raw row behind it.
    """
    published = {(r["sporely_taxon_id"], r["nortaxa_taxon_id"]): r
                 for r in emitted}
    hosts = sorted({r["sporely_taxon_id"] for r in emitted})
    out, seen = [], set()
    for host in hosts:
        for ext, role, note in conn.execute(
                "SELECT external_id, id_role, note FROM taxon_external_id_min "
                "WHERE source_system = 'artsdatabanken' AND taxon_id = ? "
                "ORDER BY external_id", (host,)):
            key = (host, str(ext))
            row = {"sporely_taxon_id": host, "nortaxa_taxon_id": str(ext),
                   "id_role": role, "raw_note": note}
            if key in published:
                seen.add(key)
                row.update(published=True,
                           authoritative_note=published[key]["note"])
            else:
                if note in REVIEWED_NOTES:
                    raise SystemExit(f"reviewed bridge not published: {key}")
                evidence_class = _NOTE_EVIDENCE_CLASS.get(note, note)
                row.update(published=False, evidence_class=evidence_class,
                           rejection_reason=policy.rejection_reason(
                               evidence_class))
            out.append(row)
    missing = set(published) - seen
    if missing:
        raise SystemExit(f"published bridge without raw row: {sorted(missing)}")
    return out


def reviewed_bridges(conn: sqlite3.Connection) -> list[dict]:
    placeholders = ",".join("?" * len(REVIEWED_NOTES))
    return [
        {"nortaxa_taxon_id": str(ext), "sporely_taxon_id": int(tid),
         "note": note}
        for ext, tid, note in conn.execute(
            "SELECT external_id, taxon_id, note FROM taxon_external_id_min "
            "WHERE source_system = 'artsdatabanken' "
            f"AND note IN ({placeholders}) ORDER BY external_id",
            REVIEWED_NOTES)
    ]


# --------------------------------------------------------------- reporting ---


def _histogram(values) -> dict[str, int]:
    counts = collections.Counter(values)
    return {str(k): counts[k] for k in sorted(counts)}


def summarise(rows: list[dict], *, taxon_key: str) -> dict:
    """Counts by evidence class, plus shared-synonym distributions."""
    by_class = collections.Counter(row["evidence_class"] for row in rows)
    return {
        "associations": len(rows),
        "distinct_nortaxa_taxon_ids": len({r["nortaxa_taxon_id"] for r in rows}),
        "distinct_sporely_taxa": len({r[taxon_key] for r in rows}),
        "by_evidence_class": {cls: by_class.get(cls, 0)
                              for cls in EVIDENCE_CLASSES},
        "reviewable_total": sum(by_class.get(cls, 0) for cls in REVIEWABLE),
        "shared_synonym_count_distribution": {
            cls: _histogram(r["shared_synonym_count"] for r in rows
                            if r["evidence_class"] == cls)
            for cls in REVIEWABLE
        },
    }


def build_manifest(evidence_class: str, rows: list[dict],
                   pins: dict) -> dict:
    members = [[row[column] for column in MANIFEST_COLUMNS]
               for row in rows if row["evidence_class"] == evidence_class]
    members.sort(key=lambda m: (m[0], m[1], m[2]))
    return {
        "format": "sporely-taxonomy-v3-candidate-manifest-v1",
        "population": "group_a_automatic_nortaxa_bridge",
        "evidence_class": evidence_class,
        "review_status": "needs_review",
        "note": ("Immutable review input. Membership is pinned to the release "
                 "and source archives below; it approves nothing and never "
                 "extends to taxa in a later source release. shared_synonyms "
                 "lists at most 10 names; shared_synonym_count is exact."),
        "pins": pins,
        "columns": MANIFEST_COLUMNS,
        "member_count": len(members),
        "members_sha256": hashlib.sha256(
            _canonical({"columns": MANIFEST_COLUMNS, "members": members})
        ).hexdigest(),
        "members": members,
    }


# ------------------------------------------------- shared-synonymy review ---
#
# The ``shared_synonymy`` manifest is split for review in two steps, each a
# fixed, ordered table whose rule text and predicate sit side by side, so the
# rules published in the manifests are the rules executed:
#
# 1. every shared synonym gets one kind from ``SYNONYM_KIND_TESTS`` — the first
#    test it meets, else ``ordinary`` — plus the evidence that decided it;
# 2. the association gets the first class in ``SHARED_REVIEW_CLASS_TESTS``
#    whose rule it meets, stated over those kinds.


def _levenshtein(a: str, b: str) -> int:
    previous = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        current = [i]
        for j, cb in enumerate(b, 1):
            current.append(min(previous[j] + 1, current[j - 1] + 1,
                               previous[j - 1] + (ca != cb)))
        previous = current
    return previous[-1]


def _is_near_name(name: str, accepted: str) -> bool:
    """A name almost spelled like ``accepted``: same word count once hyphens
    are removed, and every word, the genus included, within Levenshtein
    distance 2 of the accepted name's word. Both are case-folded canonical
    names. Spelling alone cannot tell a misspelling (Scolecosporium) from a
    different genus (Cladina, Parmelina), so a match is only a name variant."""
    words = name.replace("-", "").split()
    target = accepted.replace("-", "").split()
    if name == accepted or len(words) != len(target):
        return False
    return all(_levenshtein(w, t) <= 2 for w, t in zip(words, target))


#: Structured source statuses, as ``(source, field, value)``, that make a
#: shared synonym weak. Structured status and textual annotation are separate
#: signals. NorTaxa ``illegitimate`` and COL ``unacceptable`` are deliberately
#: absent: the pinned archives define neither (NorTaxa's meta.xml maps the
#: Darwin Core field, its eml.xml has no glossary), NorTaxa sets it on most
#: shared synonyms, including basionyms another source accepts, and COL's
#: value is sometimes propagated from NorTaxa. That is too broad to reject
#: automatically; an explicit textual illegitimacy or homonym warning is used
#: instead (``_ANNOTATION_ILLEGITIMATE``, ``_REMARK_ILLEGITIMATE``).
_STATUS_ORTHOGRAPHIC = frozenset({
    ("nortaxa", "nomenclaturalStatus", "orthographic")})
_STATUS_UNPUBLISHED = frozenset({("col", "col:nameStatus", "manuscript")})
_STATUS_NOT_VALIDLY_PUBLISHED = frozenset({
    ("nortaxa", "nomenclaturalStatus", "notvalidlypublished"),
    ("col", "col:nameStatus", "not established")})
_STATUS_MISAPPLIED = frozenset({
    ("nortaxa", "nomenclaturalStatus", "misapplied"),
    ("col", "col:status", "misapplied")})
_STATUS_PRO_PARTE = frozenset({("col", "col:status", "ambiguous synonym")})

_ANNOTATION_UNPUBLISHED = re.compile(r"\bined\b|\bnom\.\s*herb\b")
_ANNOTATION_NOT_VALIDLY_PUBLISHED = re.compile(r"\bnom\.\s*(nud|inval)\b")
_ANNOTATION_ILLEGITIMATE = re.compile(r"\bnom\.\s*illeg\b")
_ANNOTATION_INTERPRETATION = re.compile(r"\b(sensu|auct|ss?)\.|\bsensu\b")
_ANNOTATION_PRO_PARTE = re.compile(r"\bp\.\s*p\.|\bpro\s+parte\b")

#: Explicit nomenclatural warnings in a COL usage's ``col:remarks`` or
#: ``col:nameRemarks``, matched case-insensitively. Only these narrow phrases
#: are read; other remarks (citations, spelling or author-citation queries,
#: taxonomic doubt) are not evidence either way.
_REMARK_UNPUBLISHED = re.compile(r"\bined\b|\bnom\.\s*herb\b", re.I)
_REMARK_NOT_VALIDLY_PUBLISHED = re.compile(
    r"\bnom\.\s*(nud|inval)\b|\bpublished without a valid description\b",
    re.I)
_REMARK_ILLEGITIMATE = re.compile(r"\bnom\.\s*illeg\b|\blater homonym\b",
                                  re.I)


def _status_basis(evidence: dict, statuses: frozenset) -> str | None:
    hits = sorted(evidence["status"] & statuses)
    return "; ".join(f"{src} {field}={value}" for src, field, value in hits) \
        or None


def _annotation_basis(evidence: dict, pattern: re.Pattern) -> str | None:
    match = pattern.search(evidence["authorship"])
    return f"authorship '{match.group(0)}'" if match else None


def _remark_basis(evidence: dict, pattern: re.Pattern) -> str | None:
    for usage in evidence["col_usages"]:
        for field in ("remarks", "name_remarks"):
            match = pattern.search(usage[field])
            if match:
                return (f"COL {usage['col_usage_id']} "
                        f"col:{'nameRemarks' if field == 'name_remarks' else 'remarks'}"
                        f" '{match.group(0)}'")
    return None


def _nortaxa_derived(e: dict) -> str | None:
    """COL publishes this synonym only through usages NorTaxa supplied."""
    sources = {u["source_id"] for u in e["col_usages"]}
    if not sources or not sources <= e["nortaxa_col_sources"]:
        return None
    return "every COL usage has col:sourceID " + ", ".join(
        f"{u['source_id']} ({u['col_usage_id']}, clb:merged={u['merged']})"
        for u in e["col_usages"])


def _first_basis(*bases: str | None) -> str | None:
    return next((basis for basis in bases if basis), None)


def _near_name(e: dict) -> str | None:
    return next((f"near-spelling of accepted '{a}'"
                 for a in sorted(e["accepted"]) if _is_near_name(e["name"], a)),
                None)


def _reauthored(e: dict) -> str | None:
    return "accepted name under another authorship" \
        if e["name"] in e["accepted"] else None


#: ``(kind, rule, test)`` in precedence order, most certain evidence first:
#: the sources' structured statuses, then textual annotations (authorship,
#: then COL remarks), then exact name comparisons, then the spelling
#: heuristic. Provenance comes last: it does not make a name weak, it decides
#: whether an otherwise ordinary synonym is independently corroborated, so a
#: synonym that is already weak keeps its nomenclatural kind. A test returns
#: the evidence it found, or None.
SYNONYM_KIND_TESTS = (
    ("orthographic_variant",
     "either source's structured status marks the usage an orthographic "
     "variant (NorTaxa nomenclaturalStatus 'orthographic')",
     lambda e: _status_basis(e, _STATUS_ORTHOGRAPHIC)),
    ("unpublished",
     "COL col:nameStatus 'manuscript', or the authorship or a COL usage's "
     "col:remarks/col:nameRemarks carries 'ined' or 'nom. herb.'",
     lambda e: _first_basis(_status_basis(e, _STATUS_UNPUBLISHED),
                            _annotation_basis(e, _ANNOTATION_UNPUBLISHED),
                            _remark_basis(e, _REMARK_UNPUBLISHED))),
    ("not_validly_published",
     "NorTaxa nomenclaturalStatus 'notvalidlypublished', COL col:nameStatus "
     "'not established', the authorship carries 'nom. nud.' or "
     "'nom. inval.', or a COL usage's col:remarks/col:nameRemarks carries "
     "'nom. nud.', 'nom. inval.' or 'published without a valid description'",
     lambda e: _first_basis(
         _status_basis(e, _STATUS_NOT_VALIDLY_PUBLISHED),
         _annotation_basis(e, _ANNOTATION_NOT_VALIDLY_PUBLISHED),
         _remark_basis(e, _REMARK_NOT_VALIDLY_PUBLISHED))),
    ("illegitimate",
     "an explicit textual warning: the authorship carries 'nom. illeg.', or a "
     "COL usage's col:remarks/col:nameRemarks carries 'nom. illeg.' or "
     "'later homonym' (the structured statuses NorTaxa 'illegitimate' and "
     "COL 'unacceptable' are not used)",
     lambda e: _first_basis(_annotation_basis(e, _ANNOTATION_ILLEGITIMATE),
                            _remark_basis(e, _REMARK_ILLEGITIMATE))),
    ("interpretation_qualified",
     "a misapplied usage (NorTaxa nomenclaturalStatus or COL col:status "
     "'misapplied'), or the authorship carries a sensu-style qualifier "
     "('sensu', 's.', 'ss.', 'auct.')",
     lambda e: _first_basis(_status_basis(e, _STATUS_MISAPPLIED),
                            _annotation_basis(e, _ANNOTATION_INTERPRETATION))),
    ("pro_parte",
     "COL col:status 'ambiguous synonym', or the authorship carries 'p.p.' or "
     "'pro parte'",
     lambda e: _first_basis(_status_basis(e, _STATUS_PRO_PARTE),
                            _annotation_basis(e, _ANNOTATION_PRO_PARTE))),
    ("accepted_name_reauthored",
     "the canonical name is either accepted name itself, under another "
     "authorship", _reauthored),
    ("unauthored",
     "the authorship is empty, so the match is on the name alone",
     lambda e: "empty authorship" if not e["authorship"] else None),
    ("name_variant",
     "no structured status applies, but the name is almost spelled like "
     "either accepted name: same word count once hyphens are removed, and "
     "every word, genus included, within Levenshtein distance 2 (a possible "
     "misspelling or a near-identical combination, unconfirmed by the "
     "sources)", _near_name),
    ("nortaxa_derived",
     "otherwise ordinary, but every COL usage behind it comes from the COL "
     "source that is NorTaxa (see nortaxa_col_source): COL republishes "
     "NorTaxa's assertion, so the two sources do not independently "
     "corroborate it", _nortaxa_derived),
)
SYNONYM_KIND_ORDINARY = "ordinary"
WEAK_SYNONYM_KINDS = tuple(kind for kind, _, _ in SYNONYM_KIND_TESTS)


def synonym_evidence(key: tuple[str, str], bridge: dict, backbone: dict,
                     nortaxa_col_sources: frozenset = frozenset()) -> dict:
    """What the classifier sees of one shared synonym: its name key, both
    sources' non-empty structured statuses on it, the COL usages behind it
    (provenance and remarks), the COL source ids that are NorTaxa, and both
    accepted names."""
    status = {("nortaxa", field, value)
              for field, value in bridge.get("synonym_status", {}).get(key, ())
              if value}
    status |= {("col", field, value)
               for field, value in backbone.get("synonym_status", {}).get(key, ())
               if value}
    return {"name": key[0], "authorship": key[1], "status": frozenset(status),
            "col_usages": tuple(
                backbone.get("synonym_usages", {}).get(key, ())),
            "nortaxa_col_sources": frozenset(nortaxa_col_sources),
            "accepted": frozenset({bridge["accepted"][0],
                                   backbone["accepted"][0]})}


def classify_synonym(evidence: dict) -> tuple[str, str]:
    """``(kind, basis)`` of the first ``SYNONYM_KIND_TESTS`` entry met."""
    for kind, _, test in SYNONYM_KIND_TESTS:
        basis = test(evidence)
        if basis:
            return kind, basis
    return SYNONYM_KIND_ORDINARY, "none of the weak kinds"


def _describe_synonym(evidence: dict, kind: str, basis: str) -> str:
    status = [f"{src} {field}={value}"
              for src, field, value in sorted(evidence["status"])]
    status += [f"col source={u['source_id']} merged={u['merged']}"
               for u in evidence["col_usages"]]
    return f"{kind}: {basis}" + (f" [{', '.join(status)}]" if status else "")


def association_facts(row: dict, bridge: dict, backbone: dict,
                      nortaxa_col_sources: frozenset = frozenset()) -> dict:
    """Everything a review class rule reads, for one shared-synonymy row."""
    evidence = [synonym_evidence(key, bridge, backbone, nortaxa_col_sources)
                for key in shared_synonym_keys(bridge, backbone)]
    classified = [classify_synonym(e) for e in evidence]
    return {
        "bridge_rank": bridge["rank"],
        "backbone_rank": backbone["rank"],
        "accepted_names_agree": row["accepted_names_agree"],
        "kinds": [kind for kind, _ in classified],
        "descriptions": [_describe_synonym(e, kind, basis)
                         for e, (kind, basis) in zip(evidence, classified)],
        "bridge_synonym_count": row["bridge_synonym_count"],
        "backbone_synonym_count": row["backbone_synonym_count"],
    }


def _ranks(f: dict) -> set[str]:
    return {f["bridge_rank"], f["backbone_rank"]}


def _low_overlap(f: dict) -> bool:
    return min(f["bridge_synonym_count"], f["backbone_synonym_count"]) \
        >= _LOW_OVERLAP_MIN_SYNONYMS


def _only(kind: str):
    return lambda f: bool(f["kinds"]) and all(k == kind for k in f["kinds"])


#: ``(review_class, rule, predicate)`` in precedence order: a member belongs to
#: the first class whose rule it meets. Every rule is complete on its own, so
#: precedence only settles overlaps; ``ordinary`` states its full conditions
#: and a row meeting no rule fails the audit.
SHARED_REVIEW_CLASS_TESTS = (
    ("non_species_rank",
     "either source's accepted usage has a rank other than species or an "
     "infraspecific rank (subspecies, variety, form)",
     lambda f: not _ranks(f) <= _INFRASPECIFIC_RANKS | {_SPECIES_RANK}),
    ("rank_mismatch",
     "the two accepted usages have different ranks (for example species and "
     "variety)",
     lambda f: f["bridge_rank"] != f["backbone_rank"]),
    ("infraspecific_rank",
     "both accepted usages have the same infraspecific rank (subspecies, "
     "variety, form)",
     lambda f: _ranks(f) <= _INFRASPECIFIC_RANKS),
    ("accepted_authorship_disagrees",
     "the accepted name keys differ (canonical name plus authorship)",
     lambda f: not f["accepted_names_agree"]),
    *((f"only_{kind}",
       f"every shared synonym is of kind '{kind}'", _only(kind))
      for kind in WEAK_SYNONYM_KINDS),
    ("only_mixed_weak",
     "no shared synonym is of kind 'ordinary', and they are not all of one "
     "kind",
     lambda f: SYNONYM_KIND_ORDINARY not in f["kinds"]
     and len(set(f["kinds"])) > 1),
    ("single_shared_synonym_low_overlap",
     f"exactly one shared synonym is of kind 'ordinary' while each source "
     f"publishes at least {_LOW_OVERLAP_MIN_SYNONYMS} synonyms",
     lambda f: f["kinds"].count(SYNONYM_KIND_ORDINARY) == 1
     and _low_overlap(f)),
    ("ordinary",
     "both accepted usages are species, the accepted name keys (canonical "
     "name plus authorship) agree, at least one shared synonym is of kind "
     "'ordinary', and not low overlap (a single 'ordinary' shared synonym "
     f"while each source publishes at least {_LOW_OVERLAP_MIN_SYNONYMS} "
     "synonyms)",
     lambda f: f["bridge_rank"] == f["backbone_rank"] == _SPECIES_RANK
     and f["accepted_names_agree"]
     and (f["kinds"].count(SYNONYM_KIND_ORDINARY) > 1
          or (SYNONYM_KIND_ORDINARY in f["kinds"] and not _low_overlap(f)))),
)
SHARED_REVIEW_CLASSES = {name: rule
                         for name, rule, _ in SHARED_REVIEW_CLASS_TESTS}
SYNONYM_KINDS = {**{kind: rule for kind, rule, _ in SYNONYM_KIND_TESTS},
                 SYNONYM_KIND_ORDINARY: "meets none of the tests above"}

#: Review manifests append these to the parent's columns.
REVIEW_EXTRA_COLUMNS = ["shared_synonym_kind_counts",
                        "shared_synonym_evidence"]


def review_class_for(facts: dict) -> str:
    for name, _, predicate in SHARED_REVIEW_CLASS_TESTS:
        if predicate(facts):
            return name
    raise SystemExit(f"shared-synonymy row meets no review class: {facts}")


def shared_review_class(row: dict, bridge: dict, backbone: dict,
                        nortaxa_col_sources: frozenset = frozenset()) -> str:
    """The ``SHARED_REVIEW_CLASS_TESTS`` class a shared-synonymy row falls in."""
    return review_class_for(
        association_facts(row, bridge, backbone, nortaxa_col_sources))


def nortaxa_col_source(nortaxa_title: str, sources: dict[str, dict]) -> dict:
    """The one COL source whose metadata title is the pinned NorTaxa archive's
    EML title. Fails closed unless exactly one of ``sources`` matches."""
    matches = sorted(sid for sid, meta in sources.items()
                     if meta["title"] == nortaxa_title)
    if len(matches) != 1:
        raise SystemExit(f"expected one COL source titled {nortaxa_title!r}, "
                         f"found {matches}")
    return {"col_source_id": matches[0], "title": nortaxa_title,
            "matched_on": "COL source/<id>.yaml title == NorTaxa eml.xml "
                          "dataset title"}


def build_review_manifest(review_class: str, rows: list[dict], pins: dict,
                          parent: dict, nortaxa_source: dict | None = None
                          ) -> dict:
    """One ``shared_synonymy`` review class, derived from ``parent``.

    Members are the parent's columns followed by ``REVIEW_EXTRA_COLUMNS``: the
    exact count of shared synonyms per kind, and a description of each listed
    shared synonym, in ``shared_synonyms`` order, giving its kind, the evidence
    that decided it and the sources' structured statuses on it.
    """
    columns = MANIFEST_COLUMNS + REVIEW_EXTRA_COLUMNS
    members = sorted(
        ([row[column] for column in columns] for row in rows
         if row["evidence_class"] == EVIDENCE_SHARED_SYNONYMY
         and row.get("review_class") == review_class),
        key=lambda m: (m[0], m[1], m[2]))
    in_scope = MANIFEST_COLUMNS.index("in_cloud_scope")
    return {
        "format": "sporely-taxonomy-v3-review-manifest-v2",
        "population": "group_a_automatic_nortaxa_bridge",
        "evidence_class": EVIDENCE_SHARED_SYNONYMY,
        "review_class": review_class,
        "review_status": "needs_review",
        "membership_rule": SHARED_REVIEW_CLASSES[review_class],
        "review_class_precedence": list(SHARED_REVIEW_CLASSES),
        "synonym_kinds": SYNONYM_KINDS,
        "synonym_kind_precedence": list(SYNONYM_KINDS),
        "parent_manifest": {"evidence_class": EVIDENCE_SHARED_SYNONYMY,
                            "member_count": parent["member_count"],
                            "members_sha256": parent["members_sha256"]},
        "nortaxa_col_source": nortaxa_source,
        "pins": pins,
        "columns": columns,
        "member_count": len(members),
        "cloud_scope_member_count": sum(1 for m in members if m[in_scope]),
        "members_sha256": hashlib.sha256(
            _canonical({"columns": columns, "members": members})).hexdigest(),
        "members": members,
        "note": ("Immutable review input derived deterministically from the "
                 "parent shared_synonymy manifest; the review classes "
                 "partition it. A member belongs to the first class, in "
                 "review_class_precedence, whose membership_rule it meets; "
                 "each shared synonym has the first kind, in "
                 "synonym_kind_precedence, whose rule it meets. Membership is "
                 "pinned to the release and source archives below; it "
                 "approves nothing and never extends to taxa in a later "
                 "source release. shared_synonyms and shared_synonym_evidence "
                 "list at most 10 names; shared_synonym_count and "
                 "shared_synonym_kind_counts are exact."),
    }


def _kind_totals(rows: list[dict]) -> dict[str, int]:
    totals = collections.Counter()
    for row in rows:
        totals.update(row.get("shared_synonym_kind_counts") or {})
    return {kind: totals[kind] for kind in SYNONYM_KINDS}


def _extra_pairs() -> list[tuple[str, str]]:
    return [tuple(p["graded_pair"]) for p in REGRESSIONS.values()
            if "graded_pair" in p]


def classify_regressions(a_rows, b_rows, reviewed, scope, extra) -> dict:
    out = {}
    reviewed_by_id = {r["nortaxa_taxon_id"]: r for r in reviewed}
    for species, probe in REGRESSIONS.items():
        nortaxa_id = probe.get("nortaxa_taxon_id")
        sporely_id = probe.get("sporely_taxon_id")
        placements = []
        if nortaxa_id in reviewed_by_id:
            r = reviewed_by_id[nortaxa_id]
            placements.append({
                "population": "authoritative_bridge_emitted",
                "nortaxa_taxon_id": nortaxa_id,
                "sporely_taxon_id": r["sporely_taxon_id"],
                "note": r["note"],
                "in_cloud_scope": r["sporely_taxon_id"] in scope})
        for row in a_rows:
            if row["nortaxa_taxon_id"] == nortaxa_id or \
                    row["sporely_taxon_id"] == sporely_id:
                placements.append({
                    "population": "group_a", **{
                        k: row[k] for k in (
                            "nortaxa_taxon_id", "col_usage_id",
                            "sporely_taxon_id", "in_cloud_scope",
                            "evidence_class", "review_class")
                        if k in row}})
        for row in b_rows:
            if row["nortaxa_taxon_id"] == nortaxa_id or \
                    row["col_sporely_taxon_id"] == sporely_id:
                placements.append({
                    "population": "group_b", **{
                        k: row[k] for k in (
                            "nortaxa_taxon_id", "nortaxa_sporely_taxon_id",
                            "col_usage_id", "col_sporely_taxon_id",
                            "col_in_cloud_scope", "evidence_class")}})
        pair = tuple(probe.get("graded_pair") or ())
        if pair and not any(p["population"] == "group_b" for p in placements):
            placements.append({
                "population": "explicit_pair_outside_group_b",
                "nortaxa_taxon_id": pair[0], "col_usage_id": pair[1],
                **extra[pair]})
        if not placements:
            raise SystemExit(f"regression species not classified: {species}")
        out[species] = {"probe": probe, "placements": placements}
    return out


def audit(args: argparse.Namespace) -> dict[str, dict]:
    with tempfile.TemporaryDirectory(prefix="sporely-tv3-stage0-") as tmp:
        sqlite_path = Path(tmp) / "release.sqlite3"
        release = verify_release(args.manifest, sqlite_path)
        conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
        try:
            archives = {
                "nortaxa": verify_archive(conn, "nortaxa", args.nortaxa_archive),
                "col_xr": verify_archive(conn, "col_xr", args.col_archive),
            }
            scope = cloud_scope(conn, args.scope_policy)
            a_pairs = group_a(conn)
            a_unfiltered = group_a_unfiltered_count(conn)
            b_pairs = group_b(conn)
            reviewed = reviewed_bridges(conn)
            policy = BridgeEmissionPolicy.load(MAPPING_POLICY)
            emitted = authoritative_nortaxa_emission(conn, scope)
            host_rows = reconcile_emitting_hosts(conn, emitted, policy)
            scope_col_canonical = sum(
                1 for (tid,) in conn.execute(
                    "SELECT taxon_id FROM taxon_min "
                    "WHERE canonical_source_system = 'col_xr'")
                if tid in scope)
        finally:
            conn.close()

    if a_unfiltered != len(a_pairs):
        raise SystemExit("Group A row on a non-COL-canonical concept")

    nortaxa = read_nortaxa(args.nortaxa_archive,
                           {p[0] for p in a_pairs} | {p[0] for p in b_pairs}
                           | {p[0] for p in _extra_pairs()})
    col = read_col(args.col_archive,
                   {p[1] for p in a_pairs} | {p[2] for p in b_pairs}
                   | {p[1] for p in _extra_pairs()})
    extra = {pair: grade(nortaxa.get(pair[0]), col.get(pair[1]))
             for pair in _extra_pairs()}

    a_rows = []
    for nortaxa_id, col_id, taxon_id in a_pairs:
        a_rows.append({
            "nortaxa_taxon_id": nortaxa_id, "col_usage_id": col_id,
            "sporely_taxon_id": taxon_id, "in_cloud_scope": taxon_id in scope,
            **grade(nortaxa.get(nortaxa_id), col.get(col_id))})
    b_rows = []
    for nortaxa_id, n_taxon, col_id, c_taxon in b_pairs:
        b_rows.append({
            "nortaxa_taxon_id": nortaxa_id, "nortaxa_sporely_taxon_id": n_taxon,
            "col_usage_id": col_id, "col_sporely_taxon_id": c_taxon,
            "col_in_cloud_scope": c_taxon in scope,
            "nortaxa_in_cloud_scope": n_taxon in scope,
            **grade(nortaxa.get(nortaxa_id), col.get(col_id))})
    for row in a_rows + b_rows:
        if "shared_synonym_count" not in row:  # no accepted usage in source
            row.update({column: None for column in MANIFEST_COLUMNS
                        if column not in row})
            row["shared_synonym_count"] = 0

    pins = {"release": release, "source_archives": archives,
            "bridge_emission_policy": {
                "path": str(MAPPING_POLICY),
                "sha256": policy.policy_sha256},
            "cloud_scope_policy": {
                "path": str(SCOPE_POLICY),
                "sha256": _sha256_file(args.scope_policy),
                "scope_predicate_id": "global_macrofungi_policy_v1"}}

    shared_rows = [r for r in a_rows
                   if r["evidence_class"] == EVIDENCE_SHARED_SYNONYMY]
    provenance = collections.Counter(
        (usage["source_id"], usage["merged"])
        for row in shared_rows
        for key in shared_synonym_keys(nortaxa[row["nortaxa_taxon_id"]],
                                       col[row["col_usage_id"]])
        for usage in col[row["col_usage_id"]]["synonym_usages"][key])
    col_sources = read_col_sources(args.col_archive,
                                   {sid for sid, _ in provenance})
    nortaxa_source = nortaxa_col_source(
        read_nortaxa_title(args.nortaxa_archive), col_sources)
    nortaxa_sources = frozenset({nortaxa_source["col_source_id"]})

    for row in shared_rows:
        facts = association_facts(row, nortaxa[row["nortaxa_taxon_id"]],
                                  col[row["col_usage_id"]], nortaxa_sources)
        row["review_class"] = review_class_for(facts)
        row["shared_synonym_kind_counts"] = _histogram(facts["kinds"])
        row["shared_synonym_evidence"] = facts["descriptions"][:10]

    manifests = {cls: build_manifest(cls, a_rows, pins)
                 for cls in EVIDENCE_CLASSES}
    parent = manifests[EVIDENCE_SHARED_SYNONYMY]
    review_manifests = {cls: build_review_manifest(cls, a_rows, pins, parent,
                                                   nortaxa_source)
                        for cls in SHARED_REVIEW_CLASSES}
    width = len(MANIFEST_COLUMNS)
    split = sorted(m[:width] for rm in review_manifests.values()
                   for m in rm["members"])
    if split != sorted(parent["members"]):
        raise SystemExit("shared_synonymy review classes do not partition "
                         "the parent manifest")
    report = {
        "format": "sporely-taxonomy-v3-stage0-coverage-v1",
        "pins": pins,
        "cloud_scope": {
            "concepts": len(scope),
            "col_canonical_concepts": scope_col_canonical,
        },
        "reviewed_bridges_in_release": reviewed,
        "authoritative_nortaxa_emission": {
            "definition": ("NorTaxa rows the scoped export publishes, per "
                           "cloud_export.emit_taxon_external_id_authoritative "
                           "on the cloud scope"),
            "emitted": emitted,
            "emitted_count": len(emitted),
            "emitting_host_reconciliation": host_rows,
            "emitting_host_raw_rows": len(host_rows),
            "emitting_host_rows_not_published": sum(
                1 for r in host_rows if not r["published"]),
        },
        "group_a": {
            "definition": ("taxon_external_id_min rows with source_system "
                           "'artsdatabanken' and note "
                           "'cross_source_automatic_exact'; pair graded is "
                           "(NorTaxa taxonID, host concept's COL id)"),
            "full_release": summarise(a_rows, taxon_key="sporely_taxon_id"),
            "cloud_scope": summarise(
                [r for r in a_rows if r["in_cloud_scope"]],
                taxon_key="sporely_taxon_id"),
        },
        "group_b": {
            "definition": ("NorTaxa-canonical concept and COL-canonical "
                           "concept with equal canonical_scientific_name; "
                           "cloud scope counts pairs whose COL side is in "
                           "scope"),
            "full_release": summarise(b_rows, taxon_key="nortaxa_sporely_taxon_id"),
            "cloud_scope": summarise(
                [r for r in b_rows if r["col_in_cloud_scope"]],
                taxon_key="nortaxa_sporely_taxon_id"),
            "nortaxa_side_in_cloud_scope": sum(
                1 for r in b_rows if r["nortaxa_in_cloud_scope"]),
        },
        "group_a_shared_synonymy_review_split": {
            "definition": ("the shared_synonymy manifest partitioned into "
                           "review classes; a member belongs to the first "
                           "class, in precedence order, whose rule it meets"),
            "precedence": list(SHARED_REVIEW_CLASSES),
            "rules": SHARED_REVIEW_CLASSES,
            "nortaxa_col_source": nortaxa_source,
            "shared_synonym_col_provenance": [
                {"col_source_id": sid, "clb_merged": merged,
                 "title": col_sources.get(sid, {}).get("title", ""),
                 "col_usages": count}
                for (sid, merged), count in sorted(provenance.items())],
            "synonym_kind_precedence": list(SYNONYM_KINDS),
            "synonym_kinds": SYNONYM_KINDS,
            "shared_synonym_kind_totals": {
                "full_release": _kind_totals(a_rows),
                "cloud_scope": _kind_totals(
                    [r for r in a_rows if r["in_cloud_scope"]])},
            "full_release": {cls: m["member_count"]
                             for cls, m in review_manifests.items()},
            "cloud_scope": {cls: m["cloud_scope_member_count"]
                            for cls, m in review_manifests.items()},
        },
        "regressions": classify_regressions(
            a_rows, b_rows, emitted, scope, extra),
    }
    return {"report": report, "manifests": manifests,
            "review_manifests": review_manifests}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--scope-policy", type=Path, default=SCOPE_POLICY)
    parser.add_argument("--nortaxa-archive", type=Path, default=DEFAULT_NORTAXA)
    parser.add_argument("--col-archive", type=Path, default=DEFAULT_COL)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = audit(args)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    manifest_files = {}
    in_scope = MANIFEST_COLUMNS.index("in_cloud_scope")
    written = [(f"group-a-{cls.replace('_', '-')}.manifest.json", manifest)
               for cls, manifest in result["manifests"].items()]
    written += [(f"group-a-shared-synonymy--{cls.replace('_', '-')}"
                 ".manifest.json", manifest)
                for cls, manifest in result["review_manifests"].items()]
    for name, manifest in written:
        entry = {"evidence_class": manifest["evidence_class"]}
        if "review_class" in manifest:
            entry["review_class"] = manifest["review_class"]
        entry.update(
            member_count=manifest["member_count"],
            cloud_scope_member_count=sum(
                1 for m in manifest["members"] if m[in_scope]),
            members_sha256=manifest["members_sha256"],
            file_sha256=_write_json(args.out_dir / name, manifest))
        manifest_files[name] = entry
    report = {**result["report"], "manifests": manifest_files}
    _write_json(args.out_dir / "coverage-report.json", report)
    summary = {k: report[k] for k in ("cloud_scope", "manifests")}
    summary["group_a"] = {k: v["by_evidence_class"]
                          for k, v in report["group_a"].items()
                          if isinstance(v, dict)}
    summary["group_b"] = {k: v["by_evidence_class"]
                          for k, v in report["group_b"].items()
                          if isinstance(v, dict)}
    print(json.dumps(summary, indent=1, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
