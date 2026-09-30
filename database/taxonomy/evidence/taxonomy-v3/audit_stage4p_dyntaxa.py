#!/usr/bin/env python3
"""Taxonomy v3 Stage 4P: Dyntaxa candidate evidence against the COL concepts.

Dyntaxa is a reviewed-identity-only source (``compile_release.
REVIEWED_IDENTITY_ONLY_SOURCES``): no Dyntaxa usage is bound to a Sporely
concept until an owner-approved relationship says so (decision 2). This audit
prepares that review. For every accepted Fungi concept in the pinned Dyntaxa
archive it looks for COL-canonical concepts of the pinned release that could
be the same taxon, and sorts the result into:

* **matched** — exactly one candidate, found by the Dyntaxa accepted name
  (``candidate_basis = accepted_name``) or, when that finds none, by the names
  Dyntaxa publishes as its synonyms (``dyntaxa_synonym_name``), and no other
  Dyntaxa concept claims the same COL concept. A name only *suggests* the
  candidate; each matched pair is then graded by
  ``scripts/cross_reference_evidence.py`` from both sources' published
  synonymy, and written out as one immutable manifest per evidence class;
  ``shared_synonymy`` is partitioned further into review classes;
* **ambiguous** — more than one candidate for one basis;
* **not one-to-one** — a single candidate that another Dyntaxa concept also
  claims;
* **unmatched** — no candidate. These are Dyntaxa-only concepts. This plan
  ingests and reports them but allocates no Sporely concept for them.

Every manifest is a review input (``needs_review``). It approves nothing and
never extends to taxa in a later Dyntaxa export. An approval names a
manifest's ``file_sha256``.

The audit also records, as evidence rather than assumption, how the
Artportalen taxon ids already on the release's concepts relate to Dyntaxa
taxon ids.

Read-only over the release, the COL and Dyntaxa archives and the policies.
Fails closed if an archive does not match its pin. Writes only into
``--out-dir``; output is byte-deterministic.

    .venv/bin/python database/taxonomy/evidence/taxonomy-v3/audit_stage4p_dyntaxa.py
"""
from __future__ import annotations

import argparse
import collections
import csv
import hashlib
import io
import json
import sqlite3
import sys
import tempfile
import unicodedata
import zipfile
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parents[3]
_TAXONOMY = _REPO / "database" / "taxonomy"
for _path in (_REPO, _TAXONOMY / "scripts", _HERE):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

import audit_stage0_coverage as stage0  # noqa: E402
from audit_stage0_coverage import (  # noqa: E402
    _canonical,
    _histogram,
    _sha256_file,
    _write_json,
    cloud_scope,
    verify_archive,
    verify_release,
)
from cross_reference_evidence import (  # noqa: E402
    DYNTAXA_NON_SYNONYMY_STATUSES,
    EVIDENCE_NONE,
    EVIDENCE_ONE_DIRECTIONAL,
    EVIDENCE_RECIPROCAL,
    EVIDENCE_SHARED_SYNONYMY,
    REVIEWABLE,
    grade,
    read_col,
    read_col_sources,
    read_dyntaxa,
    shared_synonym_keys,
)

EVIDENCE_CLASSES = (EVIDENCE_RECIPROCAL, EVIDENCE_ONE_DIRECTIONAL,
                    EVIDENCE_SHARED_SYNONYMY, EVIDENCE_NONE)

DYNTAXA_RELEASE_DIR = Path("database/taxonomy/sources/dyntaxa/2026-09-30")
DEFAULT_DYNTAXA = DYNTAXA_RELEASE_DIR / "archive.zip"
DEFAULT_DYNTAXA_MANIFEST = DYNTAXA_RELEASE_DIR / "manifest.json"
DEFAULT_OUT = Path("database/taxonomy/evidence/taxonomy-v3/stage4p")
DYNTAXA_KINGDOM = "Fungi"
DYNTAXA_TAXON_LSID = "urn:lsid:dyntaxa.se:Taxon:"
POPULATION = "dyntaxa_fungi_accepted_concepts"

BASIS_ACCEPTED_NAME = "accepted_name"
BASIS_SYNONYM_NAME = "dyntaxa_synonym_name"

#: The plan's regression species, located by Sporely id, never by name.
REGRESSIONS = {
    "Entoloma conferendum": 7821,
    "Pholiotina rugosa (canonical Conocybe rugosa)": 83668,
    "Craterellus tubaeformis": 620306,
    "Conocybe vexans": 617026,
    "Cantharellus cibarius": 168873,
}

MANIFEST_COLUMNS = [
    "dyntaxa_taxon_id",
    "col_usage_id",
    "sporely_taxon_id",
    "candidate_basis",
    "in_cloud_scope",
    "dyntaxa_preferred_sv_vernacular",
    "artportalen_legacy_ids_on_concept",
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
AMBIGUOUS_COLUMNS = [
    "dyntaxa_taxon_id", "scientific_name", "authorship", "rank",
    "candidate_basis", "dyntaxa_preferred_sv_vernacular", "candidates",
]
UNMATCHED_COLUMNS = [
    "dyntaxa_taxon_id", "scientific_name", "authorship", "rank",
    "dyntaxa_synonym_count", "dyntaxa_preferred_sv_vernacular",
]


# ------------------------------------------------------------------ inputs ---


def verify_dyntaxa(archive: Path, acquisition_manifest: Path) -> dict:
    """Fail closed unless the archive is the pinned acquisition."""
    manifest = json.loads(acquisition_manifest.read_text(encoding="utf-8"))
    expected = manifest["download"]["sha256"]
    actual = _sha256_file(archive)
    if actual != expected:
        raise SystemExit(f"Dyntaxa archive {archive} has sha256 {actual}, "
                         f"but the acquisition pins {expected}")
    release = manifest["release"]
    return {"sha256": actual,
            "source_release_id":
                f"dyntaxa:{release['version']}:{release['issued_date']}",
            "acquisition_manifest": {
                "path": str(DEFAULT_DYNTAXA_MANIFEST),
                "sha256": _sha256_file(acquisition_manifest)}}


def read_dyntaxa_vernaculars(archive: Path) -> dict[str, str]:
    """``{taxonId: preferred Swedish vernacular}``; one per concept, the
    first preferred row in file order."""
    out: dict[str, str] = {}
    with zipfile.ZipFile(archive) as bundle, \
            bundle.open("VernacularName.csv") as handle:
        reader = csv.reader(io.TextIOWrapper(handle, "utf-8-sig", newline=""),
                            delimiter="\t", quoting=csv.QUOTE_NONE)
        header = next(reader)
        index = {column: position for position, column in enumerate(header)}
        for parts in reader:
            if parts[index["language"]] != "sv" or \
                    parts[index["isPreferredName"]].casefold() != "true":
                continue
            out.setdefault(parts[index["taxonId"]], parts[index["vernacularName"]])
    return out


def _name(value: str) -> str:
    normalized = unicodedata.normalize("NFC", str(value or "").strip())
    return " ".join(part.casefold() for part in normalized.split())


def col_concepts(conn: sqlite3.Connection) -> dict[tuple[str, str], list[tuple[str, int]]]:
    """``{(canonical name, rank): [(COL usage id, Sporely id), ...]}`` for
    every COL-canonical concept of the release."""
    index: dict[tuple[str, str], list[tuple[str, int]]] = {}
    for taxon_id, name, rank, col_id in conn.execute(
            "SELECT taxon_id, canonical_scientific_name, taxon_rank, "
            "canonical_external_id FROM taxon_min "
            "WHERE canonical_source_system = 'col_xr' ORDER BY taxon_id"):
        index.setdefault((_name(name), str(rank or "").casefold()), []).append(
            (str(col_id), int(taxon_id)))
    return index


def artportalen_ids(conn: sqlite3.Connection) -> dict[int, list[dict]]:
    """Artportalen rows the release carries, per Sporely concept."""
    out: dict[int, list[dict]] = {}
    for taxon_id, external_id, name, note in conn.execute(
            "SELECT taxon_id, external_id, external_name, note "
            "FROM taxon_external_id_min WHERE source_system = 'artportalen' "
            "ORDER BY taxon_id, external_id"):
        out.setdefault(int(taxon_id), []).append(
            {"external_id": str(external_id), "external_name": name or "",
             "note": note or ""})
    return out


# --------------------------------------------------------------- pairing ---


def candidates_for(concept: dict, index: dict) -> tuple[str, list[tuple[str, int]]]:
    """``(basis, candidates)``: the accepted name first; only when that finds
    nothing, the names Dyntaxa lists as the concept's synonyms."""
    found = index.get((_name(concept["scientific_name"]), concept["rank"]), [])
    if found:
        return BASIS_ACCEPTED_NAME, sorted(set(found))
    by_synonym: set[tuple[str, int]] = set()
    for key, usages in concept["synonym_usages"].items():
        for usage in usages:
            by_synonym.update(index.get((key[0], usage["rank"]), ()))
    return BASIS_SYNONYM_NAME, sorted(by_synonym)


def pair_concepts(dyntaxa: dict[str, dict], index: dict) -> dict[str, list[dict]]:
    """Partition the Dyntaxa concepts into matched, ambiguous, not one-to-one
    and unmatched rows. Deterministic in ``dyntaxa_taxon_id`` order."""
    single: list[dict] = []
    ambiguous: list[dict] = []
    unmatched: list[dict] = []
    for taxon_id in sorted(dyntaxa):
        concept = dyntaxa[taxon_id]
        if concept["accepted"] is None:
            continue
        basis, found = candidates_for(concept, index)
        row = {"dyntaxa_taxon_id": taxon_id, "candidate_basis": basis,
               "scientific_name": concept["scientific_name"],
               "authorship": concept["authorship"], "rank": concept["rank"]}
        if not found:
            unmatched.append(row)
        elif len(found) > 1:
            ambiguous.append({**row, "candidates": [list(c) for c in found]})
        else:
            col_id, sporely_id = found[0]
            single.append({**row, "col_usage_id": col_id,
                           "sporely_taxon_id": sporely_id})
    claims = collections.Counter(r["sporely_taxon_id"] for r in single)
    matched = [r for r in single if claims[r["sporely_taxon_id"]] == 1]
    not_one_to_one = [
        {**r, "candidates": [[r["col_usage_id"], r["sporely_taxon_id"]]],
         "competing_dyntaxa_concepts": claims[r["sporely_taxon_id"]]}
        for r in single if claims[r["sporely_taxon_id"]] > 1]
    return {"matched": matched, "ambiguous": ambiguous,
            "not_one_to_one": not_one_to_one, "unmatched": unmatched}


# ------------------------------------------------- shared-synonymy review ---
#
# The Stage 0 review split (``audit_stage0_coverage``) with Dyntaxa's own
# status vocabulary in place of NorTaxa's. Rule text and predicate sit side by
# side, so the rules published in the manifests are the rules executed. The
# class rules that do not read a synonym kind are Stage 0's own tuples.

_DYNTAXA_ORTHOGRAPHIC = frozenset({
    ("dyntaxa", "nomenclaturalStatus", "orthographia")})
_DYNTAXA_NOT_VALIDLY_PUBLISHED = frozenset({
    ("dyntaxa", "nomenclaturalStatus", "invalidum"),
    ("dyntaxa", "nomenclaturalStatus", "nudum"),
    ("col", "col:nameStatus", "not established")})
_DYNTAXA_ILLEGITIMATE = frozenset({
    ("dyntaxa", "nomenclaturalStatus", "illegitimum"),
    ("dyntaxa", "nomenclaturalStatus", "rejiciendum")})

SYNONYM_KIND_TESTS = (
    ("orthographic_variant",
     "Dyntaxa nomenclaturalStatus 'orthographia'",
     lambda e: stage0._status_basis(e, _DYNTAXA_ORTHOGRAPHIC)),
    ("unpublished",
     "COL col:nameStatus 'manuscript', or the authorship or a COL usage's "
     "col:remarks/col:nameRemarks carries 'ined' or 'nom. herb.'",
     lambda e: stage0._first_basis(
         stage0._status_basis(e, stage0._STATUS_UNPUBLISHED),
         stage0._annotation_basis(e, stage0._ANNOTATION_UNPUBLISHED),
         stage0._remark_basis(e, stage0._REMARK_UNPUBLISHED))),
    ("not_validly_published",
     "Dyntaxa nomenclaturalStatus 'invalidum' or 'nudum', COL col:nameStatus "
     "'not established', the authorship carries 'nom. nud.' or "
     "'nom. inval.', or a COL usage's col:remarks/col:nameRemarks carries "
     "'nom. nud.', 'nom. inval.' or 'published without a valid description'",
     lambda e: stage0._first_basis(
         stage0._status_basis(e, _DYNTAXA_NOT_VALIDLY_PUBLISHED),
         stage0._annotation_basis(e, stage0._ANNOTATION_NOT_VALIDLY_PUBLISHED),
         stage0._remark_basis(e, stage0._REMARK_NOT_VALIDLY_PUBLISHED))),
    ("illegitimate",
     "Dyntaxa nomenclaturalStatus 'illegitimum' or 'rejiciendum', the "
     "authorship carries 'nom. illeg.', or a COL usage's "
     "col:remarks/col:nameRemarks carries 'nom. illeg.' or 'later homonym'",
     lambda e: stage0._first_basis(
         stage0._status_basis(e, _DYNTAXA_ILLEGITIMATE),
         stage0._annotation_basis(e, stage0._ANNOTATION_ILLEGITIMATE),
         stage0._remark_basis(e, stage0._REMARK_ILLEGITIMATE))),
    ("interpretation_qualified",
     "a misapplied COL usage (col:status 'misapplied'), or the authorship "
     "carries a sensu-style qualifier ('sensu', 's.', 'ss.', 'auct.'). "
     "Dyntaxa's misapplied usages never reach this test: they are not "
     "synonymy and are excluded when the archive is read",
     lambda e: stage0._first_basis(
         stage0._status_basis(e, stage0._STATUS_MISAPPLIED),
         stage0._annotation_basis(e, stage0._ANNOTATION_INTERPRETATION))),
    ("pro_parte",
     "COL col:status 'ambiguous synonym', or the authorship carries 'p.p.' "
     "or 'pro parte' (Dyntaxa's proParteSynonym usages are excluded when the "
     "archive is read)",
     lambda e: stage0._first_basis(
         stage0._status_basis(e, stage0._STATUS_PRO_PARTE),
         stage0._annotation_basis(e, stage0._ANNOTATION_PRO_PARTE))),
    ("accepted_name_reauthored",
     "the canonical name is either accepted name itself, under another "
     "authorship", stage0._reauthored),
    ("unauthored",
     "the authorship is empty, so the match is on the name alone",
     lambda e: "empty authorship" if not e["authorship"] else None),
    ("name_variant",
     "no structured status applies, but the name is almost spelled like "
     "either accepted name: same word count once hyphens are removed, and "
     "every word, genus included, within Levenshtein distance 2",
     stage0._near_name),
    ("dyntaxa_derived",
     "otherwise ordinary, but every COL usage behind it comes from the COL "
     "source that is Dyntaxa (see dyntaxa_col_source): COL republishes "
     "Dyntaxa's assertion, so the two sources do not independently "
     "corroborate it",
     # Stage 0's provenance test, which reads the derived source ids from
     # the evidence key it names.
     stage0._nortaxa_derived),
)
ORDINARY = stage0.SYNONYM_KIND_ORDINARY
WEAK_SYNONYM_KINDS = tuple(kind for kind, _, _ in SYNONYM_KIND_TESTS)

_STAGE0_CLASSES = {name: (name, rule, predicate)
                   for name, rule, predicate in stage0.SHARED_REVIEW_CLASS_TESTS}
SHARED_REVIEW_CLASS_TESTS = (
    *(_STAGE0_CLASSES[name] for name in (
        "non_species_rank", "rank_mismatch", "infraspecific_rank",
        "accepted_authorship_disagrees")),
    *((f"only_{kind}", f"every shared synonym is of kind '{kind}'",
       stage0._only(kind)) for kind in WEAK_SYNONYM_KINDS),
    *(_STAGE0_CLASSES[name] for name in (
        "only_mixed_weak", "single_shared_synonym_low_overlap", "ordinary")),
)
SHARED_REVIEW_CLASSES = {name: rule
                         for name, rule, _ in SHARED_REVIEW_CLASS_TESTS}
SYNONYM_KINDS = {**{kind: rule for kind, rule, _ in SYNONYM_KIND_TESTS},
                 ORDINARY: "meets none of the tests above"}
REVIEW_EXTRA_COLUMNS = stage0.REVIEW_EXTRA_COLUMNS


def synonym_evidence(key: tuple[str, str], bridge: dict, backbone: dict,
                     dyntaxa_col_sources: frozenset = frozenset()) -> dict:
    """Stage 0's evidence shape, with the bridge statuses tagged ``dyntaxa``."""
    status = {("dyntaxa", field, value)
              for field, value in bridge.get("synonym_status", {}).get(key, ())
              if value}
    status |= {("col", field, value)
               for field, value in backbone.get("synonym_status", {}).get(key, ())
               if value}
    return {"name": key[0], "authorship": key[1], "status": frozenset(status),
            "col_usages": tuple(backbone.get("synonym_usages", {}).get(key, ())),
            "nortaxa_col_sources": frozenset(dyntaxa_col_sources),
            "accepted": frozenset({bridge["accepted"][0],
                                   backbone["accepted"][0]})}


def classify_synonym(evidence: dict) -> tuple[str, str]:
    for kind, _, test in SYNONYM_KIND_TESTS:
        basis = test(evidence)
        if basis:
            return kind, basis
    return ORDINARY, "none of the weak kinds"


def association_facts(row: dict, bridge: dict, backbone: dict,
                      dyntaxa_col_sources: frozenset = frozenset()) -> dict:
    evidence = [synonym_evidence(key, bridge, backbone, dyntaxa_col_sources)
                for key in shared_synonym_keys(bridge, backbone)]
    classified = [classify_synonym(e) for e in evidence]
    return {
        "bridge_rank": bridge["rank"],
        "backbone_rank": backbone["rank"],
        "accepted_names_agree": row["accepted_names_agree"],
        "kinds": [kind for kind, _ in classified],
        "descriptions": [stage0._describe_synonym(e, kind, basis)
                         for e, (kind, basis) in zip(evidence, classified)],
        "bridge_synonym_count": row["bridge_synonym_count"],
        "backbone_synonym_count": row["backbone_synonym_count"],
    }


def review_class_for(facts: dict) -> str:
    for name, _, predicate in SHARED_REVIEW_CLASS_TESTS:
        if predicate(facts):
            return name
    raise SystemExit(f"shared-synonymy row meets no review class: {facts}")


def dyntaxa_col_source(sources: dict[str, dict], title: str) -> dict | None:
    """The COL source whose metadata title is the Dyntaxa EML title, or
    ``None`` when no shared synonym comes from one."""
    matches = sorted(sid for sid, meta in sources.items()
                     if meta["title"] == title)
    if len(matches) > 1:
        raise SystemExit(f"more than one COL source titled {title!r}: {matches}")
    if not matches:
        return None
    return {"col_source_id": matches[0], **sources[matches[0]]}


def read_dyntaxa_title(archive: Path) -> str:
    import xml.etree.ElementTree as ET

    with zipfile.ZipFile(archive) as bundle, bundle.open("eml.xml") as handle:
        title = ET.parse(handle).getroot().find("./dataset/title")
    if title is None or not (title.text or "").strip():
        raise SystemExit(f"{archive}: eml.xml declares no dataset title")
    return title.text.strip()


# -------------------------------------------------------------- manifests ---


_MANIFEST_NOTE = (
    "Immutable review input. Membership is pinned to the release and source "
    "archives below; it approves nothing and never extends to taxa in a later "
    "Dyntaxa export. A candidate name only suggests the pair; the evidence "
    "class is graded from both sources' published synonymy. shared_synonyms "
    "lists at most 10 names; shared_synonym_count is exact.")


def _members(rows: list[dict], columns: list[str]) -> list[list]:
    return sorted(([row.get(column) for column in columns] for row in rows),
                  key=lambda m: [str(v) for v in m[:3]])


def build_manifest(kind: str, rows: list[dict], columns: list[str],
                   pins: dict, **extra) -> dict:
    members = _members(rows, columns)
    in_scope = columns.index("in_cloud_scope") if "in_cloud_scope" in columns \
        else None
    return {
        "format": "sporely-taxonomy-v3-candidate-manifest-v1",
        "population": POPULATION,
        "candidate_kind": kind,
        **extra,
        "review_status": "needs_review",
        "note": _MANIFEST_NOTE,
        "pins": pins,
        "columns": columns,
        "member_count": len(members),
        **({"cloud_scope_member_count": sum(1 for m in members if m[in_scope])}
           if in_scope is not None else {}),
        "members_sha256": hashlib.sha256(
            _canonical({"columns": columns, "members": members})).hexdigest(),
        "members": members,
    }


def build_review_manifest(review_class: str, rows: list[dict], pins: dict,
                          parent: dict, source: dict | None) -> dict:
    columns = MANIFEST_COLUMNS + REVIEW_EXTRA_COLUMNS
    return build_manifest(
        "matched", [r for r in rows if r.get("review_class") == review_class],
        columns, pins,
        evidence_class=EVIDENCE_SHARED_SYNONYMY,
        review_class=review_class,
        membership_rule=SHARED_REVIEW_CLASSES[review_class],
        review_class_precedence=list(SHARED_REVIEW_CLASSES),
        synonym_kinds=SYNONYM_KINDS,
        synonym_kind_precedence=list(SYNONYM_KINDS),
        parent_manifest={"evidence_class": EVIDENCE_SHARED_SYNONYMY,
                         "member_count": parent["member_count"],
                         "members_sha256": parent["members_sha256"]},
        dyntaxa_col_source=source,
    )


# ------------------------------------------------------------ artportalen ---


def artportalen_relation(ap_rows: dict[int, list[dict]],
                         dyntaxa_taxa: dict[str, dict]) -> dict:
    """Compare each Artportalen id on a release concept with the Dyntaxa
    taxon-concept LSID carrying the same number."""
    by_number = {tid[len(DYNTAXA_TAXON_LSID):]: concept
                 for tid, concept in dyntaxa_taxa.items()
                 if tid.startswith(DYNTAXA_TAXON_LSID)}
    counts = collections.Counter()
    samples: dict[str, list] = collections.defaultdict(list)
    notes = collections.Counter()
    for taxon_id, rows in sorted(ap_rows.items()):
        for row in rows:
            notes[row["note"].split(";")[0]] += 1
            concept = by_number.get(row["external_id"])
            if concept is None:
                outcome = "no_dyntaxa_taxon_with_that_number"
            elif concept["accepted"] is None:
                outcome = "dyntaxa_taxon_not_accepted"
            elif _name(concept["scientific_name"]) == _name(row["external_name"]):
                outcome = "same_number_same_name"
            else:
                outcome = "same_number_different_name"
            counts[outcome] += 1
            if outcome != "same_number_same_name" and len(samples[outcome]) < 25:
                samples[outcome].append({
                    "sporely_taxon_id": taxon_id,
                    "artportalen_id": row["external_id"],
                    "artportalen_name": row["external_name"],
                    "dyntaxa_name": concept["scientific_name"] if concept else None})
    return {
        "question": "Are the Artportalen taxon ids on release concepts "
                    "Dyntaxa taxon-concept numbers?",
        "method": "for every taxon_external_id_min row with source_system "
                  "'artportalen', look up 'urn:lsid:dyntaxa.se:Taxon:<id>' "
                  "among the pinned archive's Fungi accepted concepts and "
                  "compare the names",
        "artportalen_rows": sum(counts.values()),
        "outcomes": dict(sorted(counts.items())),
        "samples": {k: v for k, v in sorted(samples.items())},
        "artportalen_row_provenance": dict(sorted(notes.items())),
        "finding": None,  # filled by ``finding_text``
    }


def finding_text(relation: dict) -> str:
    total = relation["artportalen_rows"]
    same = relation["outcomes"].get("same_number_same_name", 0)
    provenance = relation["artportalen_row_provenance"]
    legacy = provenance.get("legacy_compat:artportalen", 0)
    overlay = sum(n for note, n in provenance.items()
                  if note.startswith("reviewed_publishing_overlay:"))
    return (
        f"{same} of {total} Artportalen ids on release concepts are the number "
        "of a Dyntaxa accepted Fungi concept with the same name; the rest are "
        "renamed, sensu-lato or retired Dyntaxa concepts (see samples). So "
        "'artportalen_taxon_id' <n> and 'urn:lsid:dyntaxa.se:Taxon:<n>' number "
        "the same Dyntaxa concept, which is what Artportalen reports against. "
        f"The Artportalen rows do not bind a Dyntaxa id: {legacy} "
        "were attached by name by the legacy compatibility import and "
        f"{overlay} by the reviewed Artportalen publishing overlay; both are "
        "publishing ids, not concept identity. "
        "They are listed per candidate (artportalen_legacy_ids_on_concept) as "
        "review context only.")


# ------------------------------------------------------------------ audit ---


def audit(args: argparse.Namespace) -> dict:
    with tempfile.TemporaryDirectory(prefix="sporely-tv3-stage4p-") as tmp:
        sqlite_path = Path(tmp) / "release.sqlite3"
        release = verify_release(args.manifest, sqlite_path)
        conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
        try:
            col_pin = verify_archive(conn, "col_xr", args.col_archive)
            scope = cloud_scope(conn, args.scope_policy)
            index = col_concepts(conn)
            ap_rows = artportalen_ids(conn)
            reviewed_dyntaxa = conn.execute(
                "SELECT COUNT(*) FROM taxon_external_id_text_min "
                "WHERE source_system = 'dyntaxa'").fetchone()[0]
        finally:
            conn.close()
    dyntaxa_pin = verify_dyntaxa(args.dyntaxa_archive, args.dyntaxa_manifest)
    dyntaxa = read_dyntaxa(args.dyntaxa_archive, kingdom=DYNTAXA_KINGDOM)
    vernaculars = read_dyntaxa_vernaculars(args.dyntaxa_archive)
    partition = pair_concepts(dyntaxa, index)

    col = read_col(args.col_archive,
                   {r["col_usage_id"] for r in partition["matched"]})
    rows = []
    for row in partition["matched"]:
        graded = grade(dyntaxa[row["dyntaxa_taxon_id"]],
                       col.get(row["col_usage_id"]))
        rows.append({**row, **graded,
                     "in_cloud_scope": row["sporely_taxon_id"] in scope})
    for row in rows:
        if "shared_synonym_count" not in row:
            row.update({c: None for c in MANIFEST_COLUMNS if c not in row})
            row["shared_synonym_count"] = 0
    for row in (rows + partition["ambiguous"] + partition["not_one_to_one"]
                + partition["unmatched"]):
        row["dyntaxa_preferred_sv_vernacular"] = vernaculars.get(
            row["dyntaxa_taxon_id"])
        row["dyntaxa_synonym_count"] = len(
            dyntaxa[row["dyntaxa_taxon_id"]]["synonyms"])
        if "sporely_taxon_id" in row:
            row["artportalen_legacy_ids_on_concept"] = [
                r["external_id"] for r in ap_rows.get(row["sporely_taxon_id"], [])]
    for row in partition["ambiguous"] + partition["not_one_to_one"]:
        row["candidates"] = [[c, t, t in scope] for c, t, *_ in row["candidates"]]

    shared_rows = [r for r in rows if r["evidence_class"] == EVIDENCE_SHARED_SYNONYMY]
    provenance = collections.Counter(
        usage["source_id"]
        for row in shared_rows
        for key in shared_synonym_keys(dyntaxa[row["dyntaxa_taxon_id"]],
                                       col[row["col_usage_id"]])
        for usage in col[row["col_usage_id"]]["synonym_usages"][key])
    col_sources = read_col_sources(args.col_archive, set(provenance))
    source = dyntaxa_col_source(col_sources,
                                read_dyntaxa_title(args.dyntaxa_archive))
    derived = frozenset({source["col_source_id"]}) if source else frozenset()
    for row in shared_rows:
        facts = association_facts(row, dyntaxa[row["dyntaxa_taxon_id"]],
                                  col[row["col_usage_id"]], derived)
        row["review_class"] = review_class_for(facts)
        row["shared_synonym_kind_counts"] = _histogram(facts["kinds"])
        row["shared_synonym_evidence"] = facts["descriptions"][:10]

    pins = {"release": release,
            "source_archives": {"col_xr": col_pin, "dyntaxa": dyntaxa_pin},
            "cloud_scope_policy": {
                "path": str(stage0.SCOPE_POLICY),
                "sha256": _sha256_file(args.scope_policy),
                "scope_predicate_id": "global_macrofungi_policy_v1"}}

    manifests = {
        f"dyntaxa-{cls.replace('_', '-')}.manifest.json": build_manifest(
            "matched", [r for r in rows if r["evidence_class"] == cls],
            MANIFEST_COLUMNS, pins, evidence_class=cls)
        for cls in EVIDENCE_CLASSES}
    parent = manifests["dyntaxa-shared-synonymy.manifest.json"]
    review = {
        f"dyntaxa-shared-synonymy--{cls.replace('_', '-')}.manifest.json":
            build_review_manifest(cls, shared_rows, pins, parent, source)
        for cls in SHARED_REVIEW_CLASSES}
    split = sorted(m[:len(MANIFEST_COLUMNS)] for rm in review.values()
                   for m in rm["members"])
    if split != sorted(parent["members"]):
        raise SystemExit("review classes do not partition the parent manifest")
    manifests.update(review)
    manifests["dyntaxa-ambiguous.manifest.json"] = build_manifest(
        "ambiguous", partition["ambiguous"], AMBIGUOUS_COLUMNS, pins)
    manifests["dyntaxa-not-one-to-one.manifest.json"] = build_manifest(
        "not_one_to_one", partition["not_one_to_one"],
        AMBIGUOUS_COLUMNS + ["competing_dyntaxa_concepts"], pins)
    manifests["dyntaxa-unmatched.manifest.json"] = build_manifest(
        "unmatched", partition["unmatched"], UNMATCHED_COLUMNS, pins)

    relation = artportalen_relation(ap_rows, dyntaxa)
    relation["finding"] = finding_text(relation)
    by_taxon = {r["sporely_taxon_id"]: r for r in rows}
    regressions = {}
    for label, taxon_id in REGRESSIONS.items():
        row = by_taxon.get(taxon_id)
        others = [k for k in ("ambiguous", "not_one_to_one")
                  for r in partition[k]
                  if any(c[1] == taxon_id for c in r["candidates"])]
        regressions[label] = {
            "sporely_taxon_id": taxon_id,
            "in_cloud_scope": taxon_id in scope,
            "matched": ({k: row[k] for k in (
                "dyntaxa_taxon_id", "candidate_basis", "evidence_class",
                "dyntaxa_preferred_sv_vernacular")}
                | ({"review_class": row["review_class"]}
                   if "review_class" in row else {})) if row else None,
            "candidate_in": sorted(set(others)),
        }

    def summary(selected: list[dict]) -> dict:
        by_class = collections.Counter(r["evidence_class"] for r in selected)
        return {
            "pairs": len(selected),
            "by_candidate_basis": _histogram(r["candidate_basis"] for r in selected),
            "by_evidence_class": {c: by_class.get(c, 0) for c in EVIDENCE_CLASSES},
            "reviewable_total": sum(by_class.get(c, 0) for c in REVIEWABLE),
            "with_preferred_sv_vernacular": sum(
                1 for r in selected if r["dyntaxa_preferred_sv_vernacular"]),
        }

    report = {
        "format": "sporely-taxonomy-v3-stage4p-dyntaxa-v1",
        "pins": pins,
        "population": {
            "definition": "accepted usages with kingdom 'Fungi' in the pinned "
                          "Dyntaxa archive",
            "dyntaxa_concepts": len(dyntaxa),
            "dyntaxa_usages_excluded_as_non_synonymy": {
                "statuses": sorted(DYNTAXA_NON_SYNONYMY_STATUSES),
                "count": sum(c["excluded_usage_count"] for c in dyntaxa.values())},
            "reviewed_dyntaxa_bridges_in_release": reviewed_dyntaxa,
        },
        "partition": {k: len(v) for k, v in partition.items()},
        "matched": {"full_release": summary(rows),
                    "cloud_scope": summary([r for r in rows if r["in_cloud_scope"]])},
        "ambiguous_with_a_cloud_scope_candidate": sum(
            1 for r in partition["ambiguous"] if any(c[2] for c in r["candidates"])),
        "shared_synonymy_review_split": {
            "precedence": list(SHARED_REVIEW_CLASSES),
            "rules": SHARED_REVIEW_CLASSES,
            "synonym_kind_precedence": list(SYNONYM_KINDS),
            "synonym_kinds": SYNONYM_KINDS,
            "dyntaxa_col_source": source,
            "shared_synonym_col_provenance": [
                {"col_source_id": sid,
                 "title": col_sources.get(sid, {}).get("title", ""),
                 "col_usages": count}
                for sid, count in sorted(provenance.items())
                if count >= 10 or sid == (source or {}).get("col_source_id")],
        },
        "artportalen_relation": relation,
        "regressions": regressions,
    }
    return {"report": report, "manifests": manifests}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--manifest", type=Path, default=stage0.DEFAULT_MANIFEST)
    parser.add_argument("--scope-policy", type=Path, default=stage0.SCOPE_POLICY)
    parser.add_argument("--col-archive", type=Path, default=stage0.DEFAULT_COL)
    parser.add_argument("--dyntaxa-archive", type=Path, default=DEFAULT_DYNTAXA)
    parser.add_argument("--dyntaxa-manifest", type=Path,
                        default=DEFAULT_DYNTAXA_MANIFEST)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = audit(args)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    files = {}
    for name, manifest in sorted(result["manifests"].items()):
        entry = {k: manifest[k] for k in (
            "candidate_kind", "evidence_class", "review_class", "member_count",
            "cloud_scope_member_count", "members_sha256") if k in manifest}
        entry["file_sha256"] = _write_json(args.out_dir / name, manifest)
        files[name] = entry
    report = {**result["report"], "manifests": files}
    _write_json(args.out_dir / "dyntaxa-report.json", report)
    print(json.dumps({k: report[k] for k in ("partition", "matched")},
                     indent=1, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
