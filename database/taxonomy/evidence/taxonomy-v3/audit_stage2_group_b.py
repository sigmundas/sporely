#!/usr/bin/env python3
"""Taxonomy v3 Stage 2: Group-B duplicate candidates, graded for review.

Group B is a NorTaxa-canonical concept and a COL-canonical concept that share
a ``canonical_scientific_name`` but are separate Sporely concepts. Name
equality only *finds* a pair; it is never evidence that the two are one
concept (``mapping_policy.continuity_rules.canonical_name_only``). Every pair
is graded by ``scripts/cross_reference_evidence.py`` — the two sources' own
published synonymy — exactly as Stage 0 graded it, and ``shared_synonymy`` is
split with Stage 0's review classes (``SHARED_REVIEW_CLASS_TESTS`` over
``SYNONYM_KIND_TESTS``), so a Group-B class means what the Group-A class of
the same name means.

The output is review input under plan decision 2, never a decision:

* one immutable manifest per evidence class and per ``shared_synonymy`` review
  class, pinned to the release, source archives and policies, each member
  ``needs_review``. Members are listed in cloud-impact order: pairs whose COL
  concept is in the cloud scope first, then pairs whose NorTaxa concept
  carries vernaculars, then by identifier;
* ``accepted_authorship_disagrees``, which holds nearly all Group-B
  ``shared_synonymy``, is partitioned again by how the accepted authorships
  differ (``AUTHORSHIP_DIFFERENCE_TESTS``) and by the review class its shared
  synonyms would give it if the accepted keys agreed;
* a pair that is not one-to-one (its NorTaxa concept shares its name with
  several COL concepts, or the reverse) is never in a batch manifest: a
  supersession names one current concept, so reciprocal and
  ``shared_synonymy`` such pairs go to ``group-b-not-one-to-one`` for
  individual decision;
* each manifest states how decision 2 lets it be decided:
  ``batch_by_file_sha256`` (a one-to-one reciprocal or ``shared_synonymy``
  leaf), ``individual`` (one-directional, and not-one-to-one),
  ``decided_through_its_partition`` (a parent kept whole for accounting) or
  ``not_approvable`` (``no_published_cross_reference`` stays unresolved).
  Every pair is in exactly one decision leaf;
* ``group-b-report.json`` — counts, ``pair_review_queue`` (every decidable
  pair in one global cloud-impact order, each naming its decision manifest
  and ``file_sha256``), the enumerated not-one-to-one pairs, and the
  recorded Stage 2 outcome for each regression species.

A supersession needs an explicit owner decision naming a manifest's
``file_sha256`` (or, for individual review, the pair) recorded in the plan.
This script writes no policy record.

Read-only over the release, archives and policies. Fails closed if an archive
does not match the fingerprint the release records. Writes only into
``--out-dir``; output is byte-deterministic.

    .venv/bin/python database/taxonomy/evidence/taxonomy-v3/audit_stage2_group_b.py
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import re
import sqlite3
import sys
import tempfile
import unicodedata
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import audit_stage0_coverage as stage0  # noqa: E402
from audit_stage0_coverage import (  # noqa: E402
    EVIDENCE_CLASSES,
    EVIDENCE_NONE,
    EVIDENCE_ONE_DIRECTIONAL,
    EVIDENCE_RECIPROCAL,
    EVIDENCE_SHARED_SYNONYMY,
    MANIFEST_COLUMNS,
    SHARED_REVIEW_CLASSES,
    SYNONYM_KINDS,
    _canonical,
    _histogram,
    _sha256_file,
    _write_json,
)
from cross_reference_evidence import (  # noqa: E402
    grade,
    read_col,
    read_col_sources,
    read_nortaxa,
    read_nortaxa_title,
    shared_synonym_keys,
)
from bridge_emission import BridgeEmissionPolicy  # noqa: E402

DEFAULT_OUT = Path("database/taxonomy/evidence/taxonomy-v3/stage2")

#: How decision 2 lets a manifest be decided.
APPROVAL_BATCH = "batch_by_file_sha256"
APPROVAL_INDIVIDUAL = "individual"
APPROVAL_NONE = "not_approvable"
#: A parent kept whole for accounting; its partition is what a decision names.
APPROVAL_VIA_PARTITION = "decided_through_its_partition"
APPROVAL_MODE = {
    # Split into one-to-one pairs (batch) and the rest (individual).
    EVIDENCE_RECIPROCAL: APPROVAL_VIA_PARTITION,
    EVIDENCE_ONE_DIRECTIONAL: APPROVAL_INDIVIDUAL,
    EVIDENCE_SHARED_SYNONYMY: APPROVAL_VIA_PARTITION,
    EVIDENCE_NONE: APPROVAL_NONE,
}

# ------------------------------------------------ authorship difference ---
#
# Group B is name-equal pairs the compiler did not match automatically, so
# almost every graded pair has accepted name keys that differ in authorship
# and Stage 0's ``accepted_authorship_disagrees`` class holds nearly all of
# ``shared_synonymy``. That class is therefore split again, by two fixed
# tables whose rule text and predicate sit side by side:
#
# 1. how the accepted authorships differ (``AUTHORSHIP_DIFFERENCE_TESTS``,
#    first test met), a citation-form reading only — it proves nothing about
#    identity and approves nothing;
# 2. the Stage 0 review class the pair's shared synonyms would give it if the
#    accepted name keys agreed, so strong and weak synonym evidence are never
#    in one batch.

_SANCTIONING = re.compile(r"\s*:\s*[^():]*?(?=\)|$)")


def _typography(authorship: str) -> str:
    decomposed = unicodedata.normalize("NFKD", authorship or "")
    return "".join(ch for ch in decomposed
                   if not unicodedata.combining(ch) and not ch.isspace()
                   ).casefold()


def _without_sanctioning(authorship: str) -> str:
    return _SANCTIONING.sub("", authorship or "")


#: ``(kind, rule, predicate(bridge, backbone))`` in precedence order.
AUTHORSHIP_DIFFERENCE_TESTS = (
    ("same",
     "the accepted name keys agree",
     lambda b, c: b["accepted"] == c["accepted"]),
    ("accepted_name_differs",
     "the accepted canonical names differ",
     lambda b, c: b["accepted"][0] != c["accepted"][0]),
    ("typography_only",
     "the accepted authorships are equal after removing whitespace, "
     "diacritics and letter case",
     lambda b, c: _typography(b["authorship"]) == _typography(c["authorship"])),
    ("sanctioning_citation",
     "the accepted authorships are equal after also removing a sanctioning "
     "citation (':' and the author after it, before a closing parenthesis "
     "or at the end), as in '(Pers. : Fr.) Boud.' against '(Pers.) Boud.'",
     lambda b, c: _typography(_without_sanctioning(b["authorship"]))
     == _typography(_without_sanctioning(c["authorship"]))),
    ("different_authorship",
     "none of the above: the accepted authorships name different authors or "
     "differ in a way no rule above reads (for example an abbreviation)",
     lambda b, c: True),
)
AUTHORSHIP_DIFFERENCES = {kind: rule
                          for kind, rule, _ in AUTHORSHIP_DIFFERENCE_TESTS}


def authorship_difference(bridge: dict | None, backbone: dict | None
                          ) -> str | None:
    if not bridge or not backbone or bridge.get("accepted") is None \
            or backbone.get("accepted") is None:
        return None
    for kind, _, test in AUTHORSHIP_DIFFERENCE_TESTS:
        if test(bridge, backbone):
            return kind
    raise AssertionError("unreachable: the last test always holds")


def authorship_sub_class(difference: str, facts: dict) -> str:
    """``<difference>--<review class if the accepted keys agreed>``."""
    return f"{difference}--" + stage0.review_class_for(
        {**facts, "accepted_names_agree": True})


def _sub_class_rule(sub_class: str) -> str:
    difference, review_class = sub_class.split("--")
    return (f"review class 'accepted_authorship_disagrees'; authorship "
            f"difference '{difference}': {AUTHORSHIP_DIFFERENCES[difference]}; "
            f"and, read as if the accepted name keys agreed, review class "
            f"'{review_class}': {SHARED_REVIEW_CLASSES[review_class]}")


#: Pair identity plus the facts that order the review queue, then Stage 0's
#: grade columns (its first three, the Group-A pair identity, are replaced).
PAIR_COLUMNS = [
    "nortaxa_taxon_id",
    "nortaxa_sporely_taxon_id",
    "col_usage_id",
    "col_sporely_taxon_id",
    "canonical_scientific_name",
    "col_in_cloud_scope",
    "nortaxa_vernacular_languages",
    "col_vernacular_languages",
    "col_twins_of_nortaxa_concept",
    "nortaxa_twins_of_col_concept",
    "one_to_one",
    "accepted_authorship_difference",
]
GRADE_COLUMNS = MANIFEST_COLUMNS[4:]
B_COLUMNS = PAIR_COLUMNS + GRADE_COLUMNS
REVIEW_EXTRA_COLUMNS = stage0.REVIEW_EXTRA_COLUMNS

#: Stage 2's recorded outcome for each regression species, as the owner
#: decided it on 2026-09-29 (manual verification, check
#: 'group-b-owner-decisions', gate fbeea497a1cb45c8a94baae48be43acc). The
#: evidence each outcome cites is recomputed into the report, and the audit
#: fails if it no longer holds. The supersessions themselves are in
#: policies/concept_supersessions.yml (generate_stage2_supersessions.py).
REGRESSION_OUTCOMES = {
    "Cantharellus cibarius": {
        "pair": ("56210", "QMKY"),
        "expect": (EVIDENCE_SHARED_SYNONYMY, "sanctioning_citation--ordinary"),
        "outcome": "approved",
        "reason": (
            "Group B, shared_synonymy, sub-class "
            "sanctioning_citation--ordinary. Approved by the owner both "
            "individually and as a member of the approved "
            "sanctioning_citation--ordinary manifest. NorTaxa concept 626243 "
            "is superseded by COL 168873, so 'kantarell' reaches 168873 "
            "through that approved relationship and no other way."),
    },
    "Conocybe vexans / Pholiotina vexans": {
        "pair": ("58766", "XQZ6"),
        "expect": (EVIDENCE_RECIPROCAL, None),
        "outcome": "approved",
        "reason": (
            "Not Group B (the canonical names differ); graded explicitly as "
            "reciprocal_accepted_synonymy: NorTaxa 58766 lists 'Conocybe "
            "vexans P.D. Orton' as a synonym and COL XQZ6 lists 'Pholiotina "
            "vexans (P.D. Orton) Bon'. Approved individually by the owner, "
            "the same shape as the approved 52369 -> 83668 supersession: "
            "NorTaxa concept 627000 is superseded by COL 617026, so "
            "NBIC:58766 resolves to 617026 once a release compiled with the "
            "ledger ships."),
    },
    "Gloeophyllum odoratum (NBIC:56449)": {
        "pair": ("56449", "3GBK2"),
        "expect": (EVIDENCE_SHARED_SYNONYMY, "sanctioning_citation--ordinary"),
        "outcome": "reconciled",
        "reason": (
            "Group B, shared_synonymy, sub-class "
            "sanctioning_citation--ordinary. Approved by the owner both "
            "individually and as a manifest member: NorTaxa concept 626327 is "
            "superseded by COL 11307, so NBIC:56449 becomes resolvable once "
            "a release compiled with the ledger ships. sporely-web's "
            "temporary unresolved fixture then needs a synthetic id; Stage 5 "
            "owns that replacement."),
    },
}


# ------------------------------------------------------------------ inputs ---


def vernacular_languages(conn: sqlite3.Connection) -> dict[int, list[str]]:
    """``{taxon_id: sorted language codes}`` for concepts with vernaculars."""
    out: dict[int, set[str]] = collections.defaultdict(set)
    for taxon_id, language in conn.execute(
            "SELECT taxon_id, language_code FROM vernacular_min"):
        out[int(taxon_id)].add(str(language))
    return {tid: sorted(langs) for tid, langs in out.items()}


def names_by_taxon(conn: sqlite3.Connection, ids: set[int]) -> dict[int, str]:
    return {int(tid): name for tid, name in conn.execute(
        "SELECT taxon_id, canonical_scientific_name FROM taxon_min")
        if tid in ids}


# ----------------------------------------------------------------- building ---


def pair_rows(b_pairs, scope, vernaculars, names) -> list[dict]:
    """Group-B rows with the review-order facts, ungraded."""
    col_twins = collections.Counter(p[0] for p in b_pairs)
    nortaxa_twins = collections.Counter(p[2] for p in b_pairs)
    return [{
        "nortaxa_taxon_id": nortaxa_id,
        "nortaxa_sporely_taxon_id": n_taxon,
        "col_usage_id": col_id,
        "col_sporely_taxon_id": c_taxon,
        "canonical_scientific_name": names.get(c_taxon),
        "col_in_cloud_scope": c_taxon in scope,
        "nortaxa_in_cloud_scope": n_taxon in scope,
        "nortaxa_vernacular_languages": vernaculars.get(n_taxon, []),
        "col_vernacular_languages": vernaculars.get(c_taxon, []),
        "col_twins_of_nortaxa_concept": col_twins[nortaxa_id],
        "nortaxa_twins_of_col_concept": nortaxa_twins[col_id],
        "one_to_one": col_twins[nortaxa_id] == 1
        and nortaxa_twins[col_id] == 1,
    } for nortaxa_id, n_taxon, col_id, c_taxon in b_pairs]


def impact_key(member: list, columns: list[str]) -> tuple:
    """Cloud-impact review order: COL side in the cloud scope, then NorTaxa
    side with vernaculars, then by identifier."""
    row = dict(zip(columns, member))
    return (not row["col_in_cloud_scope"],
            not row["nortaxa_vernacular_languages"],
            row["nortaxa_taxon_id"], row["col_usage_id"])


def _members(rows: list[dict], columns: list[str]) -> list[list]:
    members = [[row[c] for c in columns] for row in rows]
    members.sort(key=lambda m: impact_key(m, columns))
    return members


def _manifest_body(columns: list[str], members: list[list]) -> dict:
    in_scope = columns.index("col_in_cloud_scope")
    vern = columns.index("nortaxa_vernacular_languages")
    return {
        "columns": columns,
        "member_count": len(members),
        "cloud_scope_member_count": sum(1 for m in members if m[in_scope]),
        "cloud_scope_with_nortaxa_vernaculars_count": sum(
            1 for m in members if m[in_scope] and m[vern]),
        "members_sha256": hashlib.sha256(
            _canonical({"columns": columns, "members": members})).hexdigest(),
        "members": members,
    }


_NOTE = ("Immutable review input. It approves nothing: a Group-B pair becomes "
         "a supersession or mapping only through an owner decision recorded "
         "in the plan that names this file's file_sha256 (batch) or the pair "
         "(individual). Membership is pinned to the release and source "
         "archives below and never extends to taxa in a later source "
         "release. Members are in cloud-impact order (COL side in the cloud "
         "scope, then NorTaxa side with vernaculars, then identifier). "
         "shared_synonyms and shared_synonym_evidence list at most 10 names; "
         "the counts are exact.")


def build_manifest(evidence_class: str, rows: list[dict], pins: dict) -> dict:
    members = _members([r for r in rows
                        if r["evidence_class"] == evidence_class], B_COLUMNS)
    return {
        "format": "sporely-taxonomy-v3-group-b-manifest-v1",
        "population": "group_b_name_equal_duplicate_candidates",
        "evidence_class": evidence_class,
        "approval_mode": APPROVAL_MODE[evidence_class],
        "review_status": "needs_review",
        "note": _NOTE,
        "pins": pins,
        **_manifest_body(B_COLUMNS, members),
    }


#: Review manifests append these, then the review class and sub-class, to
#: the pair columns.
SUB_CLASS_COLUMN = "authorship_sub_class"
AUTHORSHIP_DISAGREES = "accepted_authorship_disagrees"
DECISION_COLUMNS = (B_COLUMNS + REVIEW_EXTRA_COLUMNS
                    + ["review_class", SUB_CLASS_COLUMN])


def build_review_manifest(review_class: str, rows: list[dict], pins: dict,
                          parent: dict, nortaxa_source: dict,
                          sub_class: str | None = None) -> dict:
    """One ``shared_synonymy`` review class, or one sub-class of
    ``accepted_authorship_disagrees`` when ``sub_class`` is given.

    A batch leaf holds only one-to-one pairs; the rest of its class is in
    ``NOT_ONE_TO_ONE_MANIFEST``. ``accepted_authorship_disagrees`` itself is
    a parent kept whole for accounting."""
    is_parent = review_class == AUTHORSHIP_DISAGREES and sub_class is None
    members = _members([r for r in rows
                        if r["evidence_class"] == EVIDENCE_SHARED_SYNONYMY
                        and r.get("review_class") == review_class
                        and sub_class in (None, r.get(SUB_CLASS_COLUMN))
                        and (is_parent or r["one_to_one"])],
                       DECISION_COLUMNS)
    manifest = {
        "format": "sporely-taxonomy-v3-group-b-review-manifest-v1",
        "population": "group_b_name_equal_duplicate_candidates",
        "evidence_class": EVIDENCE_SHARED_SYNONYMY,
        "review_class": review_class,
        "approval_mode": (APPROVAL_VIA_PARTITION if is_parent
                          else APPROVAL_BATCH),
        "review_status": "needs_review",
        "membership_rule": SHARED_REVIEW_CLASSES[review_class]
        + ("" if is_parent else _ONE_TO_ONE_RULE),
        "review_class_precedence": list(SHARED_REVIEW_CLASSES),
        "synonym_kinds": SYNONYM_KINDS,
        "synonym_kind_precedence": list(SYNONYM_KINDS),
        "parent_manifest": {"evidence_class": EVIDENCE_SHARED_SYNONYMY,
                            "member_count": parent["member_count"],
                            "members_sha256": parent["members_sha256"]},
        "nortaxa_col_source": nortaxa_source,
        "note": _NOTE,
        "pins": pins,
        **_manifest_body(DECISION_COLUMNS, members),
    }
    if review_class == AUTHORSHIP_DISAGREES:
        manifest["authorship_differences"] = AUTHORSHIP_DIFFERENCES
        manifest["authorship_difference_precedence"] = list(
            AUTHORSHIP_DIFFERENCES)
    if sub_class is not None:
        manifest["sub_class"] = sub_class
        manifest["membership_rule"] = _sub_class_rule(sub_class) \
            + _ONE_TO_ONE_RULE
    return manifest


_ONE_TO_ONE_RULE = ("; and the pair is one-to-one (neither concept shares its "
                    "name with another concept on the other side)")
RECIPROCAL_ONE_TO_ONE_MANIFEST = \
    "group-b-reciprocal-accepted-synonymy--one-to-one.manifest.json"
NOT_ONE_TO_ONE_MANIFEST = "group-b-not-one-to-one.manifest.json"
NOT_ONE_TO_ONE_RULE = (
    "evidence class reciprocal_accepted_synonymy or shared_synonymy, and the "
    "pair is not one-to-one: its NorTaxa concept shares its name with more "
    "than one COL concept, or its COL concept with more than one NorTaxa "
    "concept. A supersession names one current concept, so each such pair is "
    "decided individually and no batch decision covers it.")


def build_decision_manifest(name: str, rows: list[dict], pins: dict,
                            evidence_classes: tuple[str, ...], one_to_one: bool,
                            approval_mode: str, rule: str,
                            nortaxa_source: dict) -> dict:
    """A decision leaf outside the shared-synonymy review classes."""
    members = _members([r for r in rows
                        if r["evidence_class"] in evidence_classes
                        and r["one_to_one"] == one_to_one], DECISION_COLUMNS)
    return {
        "format": "sporely-taxonomy-v3-group-b-review-manifest-v1",
        "population": "group_b_name_equal_duplicate_candidates",
        "evidence_class": (evidence_classes[0] if len(evidence_classes) == 1
                           else list(evidence_classes)),
        "decision_leaf": name,
        "approval_mode": approval_mode,
        "review_status": "needs_review",
        "membership_rule": rule,
        "nortaxa_col_source": nortaxa_source,
        "note": _NOTE,
        "pins": pins,
        **_manifest_body(DECISION_COLUMNS, members),
    }


def not_one_to_one(rows: list[dict], leaf_of: dict) -> dict:
    """Pairs a single supersession cannot express without a choice,
    enumerated in cloud-impact order with the manifest that decides each."""
    pairs = [r for r in rows if not r["one_to_one"]]
    pairs.sort(key=lambda r: impact_key([r[c] for c in B_COLUMNS], B_COLUMNS))
    return {
        "definition": ("a NorTaxa concept named like more than one COL "
                       "concept, or a COL concept named like more than one "
                       "NorTaxa concept. Supersession needs one current "
                       "concept, so no batch decision covers such a pair: "
                       "reciprocal and shared_synonymy ones are in "
                       f"{NOT_ONE_TO_ONE_MANIFEST} (individual), "
                       "one-directional ones are individual anyway, and "
                       "no_published_cross_reference ones are not approvable"),
        "nortaxa_concepts_with_several_col_twins": len(
            {r["nortaxa_taxon_id"] for r in pairs
             if r["col_twins_of_nortaxa_concept"] > 1}),
        "col_concepts_with_several_nortaxa_twins": len(
            {r["col_usage_id"] for r in pairs
             if r["nortaxa_twins_of_col_concept"] > 1}),
        "pair_count": len(pairs),
        "pairs": [{
            "nortaxa_taxon_id": r["nortaxa_taxon_id"],
            "col_usage_id": r["col_usage_id"],
            "canonical_scientific_name": r["canonical_scientific_name"],
            "col_in_cloud_scope": r["col_in_cloud_scope"],
            "col_twins_of_nortaxa_concept": r["col_twins_of_nortaxa_concept"],
            "nortaxa_twins_of_col_concept": r["nortaxa_twins_of_col_concept"],
            "evidence_class": r["evidence_class"],
            "decision_manifest": leaf_of[(r["nortaxa_taxon_id"],
                                          r["col_usage_id"])],
        } for r in pairs],
    }


def pair_review_queue(rows: list[dict], leaf_of: dict, leaves: dict,
                      file_sha: dict[str, str]) -> list[dict]:
    """Every decidable pair in one global cloud-impact order: COL side in the
    cloud scope, then NorTaxa side with vernaculars, then identifier. Each
    entry names the manifest (and its file_sha256) a decision on it names."""
    queue = []
    for row in sorted(rows, key=lambda r: impact_key(
            [r[c] for c in B_COLUMNS], B_COLUMNS)):
        pair = (row["nortaxa_taxon_id"], row["col_usage_id"])
        leaf = leaf_of[pair]
        mode = leaves[leaf]["approval_mode"]
        if mode == APPROVAL_NONE:
            continue
        queue.append({
            "position": len(queue) + 1,
            "nortaxa_taxon_id": pair[0],
            "col_usage_id": pair[1],
            "nortaxa_sporely_taxon_id": row["nortaxa_sporely_taxon_id"],
            "col_sporely_taxon_id": row["col_sporely_taxon_id"],
            "canonical_scientific_name": row["canonical_scientific_name"],
            "col_in_cloud_scope": row["col_in_cloud_scope"],
            "nortaxa_has_vernaculars": bool(row["nortaxa_vernacular_languages"]),
            "one_to_one": row["one_to_one"],
            "evidence_class": row["evidence_class"],
            "review_class": row["review_class"],
            "authorship_sub_class": row[SUB_CLASS_COLUMN],
            "decision_manifest": leaf,
            "file_sha256": file_sha[leaf],
            "approval_mode": mode,
        })
    return queue


def decision_manifests(leaves: dict, file_sha: dict[str, str]) -> list[dict]:
    """Index of the non-empty decidable leaves, by name. The review order is
    ``pair_review_queue``, not this list."""
    return [{
        "manifest": name,
        "file_sha256": file_sha[name],
        "approval_mode": m["approval_mode"],
        "member_count": m["member_count"],
        "cloud_scope_member_count": m["cloud_scope_member_count"],
        "cloud_scope_with_nortaxa_vernaculars_count":
            m["cloud_scope_with_nortaxa_vernaculars_count"],
    } for name, m in sorted(leaves.items())
        if m["approval_mode"] != APPROVAL_NONE and m["member_count"]]


def regression_outcomes(rows_by_pair: dict, explicit: dict,
                        placement: dict) -> dict:
    out = {}
    for species, spec in REGRESSION_OUTCOMES.items():
        pair = spec["pair"]
        row = rows_by_pair.get(pair) or explicit.get(pair)
        if row is None:
            raise SystemExit(f"regression pair not graded: {species} {pair}")
        found = (row["evidence_class"], row.get(SUB_CLASS_COLUMN))
        if found != spec["expect"]:
            raise SystemExit(f"regression evidence changed for {species}: "
                             f"{found} != {spec['expect']}; its recorded "
                             "outcome must be reviewed again")
        out[species] = {
            "nortaxa_taxon_id": pair[0], "col_usage_id": pair[1],
            "population": "group_b" if pair in rows_by_pair
                          else "explicit_pair_outside_group_b",
            "evidence_class": row["evidence_class"],
            "review_class": row.get("review_class"),
            "authorship_sub_class": row.get(SUB_CLASS_COLUMN),
            "accepted_authorship_difference":
                row["accepted_authorship_difference"],
            "review_manifest": placement.get(pair),
            **{k: row[k] for k in (
                "nortaxa_sporely_taxon_id", "col_sporely_taxon_id",
                "col_in_cloud_scope", "nortaxa_vernacular_languages",
                "col_vernacular_languages") if k in row},
            "outcome": spec["outcome"],
            "reason": spec["reason"],
        }
    return out


# -------------------------------------------------------------------- audit ---


def audit(args: argparse.Namespace) -> dict:
    with tempfile.TemporaryDirectory(prefix="sporely-tv3-stage2-") as tmp:
        sqlite_path = Path(tmp) / "release.sqlite3"
        release = stage0.verify_release(args.manifest, sqlite_path)
        conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
        try:
            archives = {
                "nortaxa": stage0.verify_archive(conn, "nortaxa",
                                                 args.nortaxa_archive),
                "col_xr": stage0.verify_archive(conn, "col_xr",
                                                args.col_archive),
            }
            scope = stage0.cloud_scope(conn, args.scope_policy)
            b_pairs = stage0.group_b(conn)
            vernaculars = vernacular_languages(conn)
            explicit_ids = {}
            for pair in REGRESSION_OUTCOMES.values():
                n_id, c_id = pair["pair"]
                found = dict(conn.execute(
                    "SELECT canonical_source_system, taxon_id FROM taxon_min "
                    "WHERE (canonical_source_system = 'nortaxa' "
                    "       AND canonical_external_id = ?) "
                    "   OR (canonical_source_system = 'col_xr' "
                    "       AND canonical_external_id = ?)", (n_id, c_id)))
                explicit_ids[pair["pair"]] = found
            names = names_by_taxon(conn, {p[3] for p in b_pairs})
            policy = BridgeEmissionPolicy.load(stage0.MAPPING_POLICY)
        finally:
            conn.close()

    rows = pair_rows(b_pairs, scope, vernaculars, names)
    if any(r["nortaxa_in_cloud_scope"] for r in rows):
        raise SystemExit("a Group-B NorTaxa concept is in the cloud scope")
    explicit_pairs = [p for p in explicit_ids
                      if p not in {(r["nortaxa_taxon_id"], r["col_usage_id"])
                                   for r in rows}]
    nortaxa = read_nortaxa(args.nortaxa_archive,
                           {r["nortaxa_taxon_id"] for r in rows}
                           | {p[0] for p in explicit_pairs})
    col = read_col(args.col_archive, {r["col_usage_id"] for r in rows}
                   | {p[1] for p in explicit_pairs})

    for row in rows:
        row.update(grade(nortaxa.get(row["nortaxa_taxon_id"]),
                         col.get(row["col_usage_id"])))
        if "shared_synonym_count" not in row:  # no accepted usage in source
            row.update({c: None for c in GRADE_COLUMNS if c not in row})
            row["shared_synonym_count"] = 0
        row["accepted_authorship_difference"] = authorship_difference(
            nortaxa.get(row["nortaxa_taxon_id"]), col.get(row["col_usage_id"]))

    explicit = {}
    for pair in explicit_pairs:
        ids = explicit_ids[pair]
        n_taxon, c_taxon = ids.get("nortaxa"), ids.get("col_xr")
        explicit[pair] = {
            "nortaxa_sporely_taxon_id": n_taxon,
            "col_sporely_taxon_id": c_taxon,
            "col_in_cloud_scope": c_taxon in scope,
            "nortaxa_vernacular_languages": vernaculars.get(n_taxon, []),
            "col_vernacular_languages": vernaculars.get(c_taxon, []),
            "accepted_authorship_difference": authorship_difference(
                nortaxa.get(pair[0]), col.get(pair[1])),
            **grade(nortaxa.get(pair[0]), col.get(pair[1]))}

    for row in rows:
        row.update(dict.fromkeys(
            REVIEW_EXTRA_COLUMNS + ["review_class", SUB_CLASS_COLUMN]))
    shared_rows = [r for r in rows
                   if r["evidence_class"] == EVIDENCE_SHARED_SYNONYMY]
    provenance = collections.Counter(
        (usage["source_id"], usage["merged"])
        for row in shared_rows
        for key in shared_synonym_keys(nortaxa[row["nortaxa_taxon_id"]],
                                       col[row["col_usage_id"]])
        for usage in col[row["col_usage_id"]]["synonym_usages"][key])
    col_sources = read_col_sources(args.col_archive,
                                   {sid for sid, _ in provenance})
    nortaxa_source = stage0.nortaxa_col_source(
        read_nortaxa_title(args.nortaxa_archive), col_sources)
    nortaxa_sources = frozenset({nortaxa_source["col_source_id"]})
    for row in shared_rows:
        facts = stage0.association_facts(
            row, nortaxa[row["nortaxa_taxon_id"]], col[row["col_usage_id"]],
            nortaxa_sources)
        row["review_class"] = stage0.review_class_for(facts)
        row["shared_synonym_kind_counts"] = _histogram(facts["kinds"])
        row["shared_synonym_evidence"] = facts["descriptions"][:10]
        row[SUB_CLASS_COLUMN] = (
            authorship_sub_class(row["accepted_authorship_difference"], facts)
            if row["review_class"] == AUTHORSHIP_DISAGREES else None)

    pins = {"release": release, "source_archives": archives,
            "bridge_emission_policy": {
                "path": str(stage0.MAPPING_POLICY),
                "sha256": policy.policy_sha256},
            "cloud_scope_policy": {
                "path": str(stage0.SCOPE_POLICY),
                "sha256": _sha256_file(args.scope_policy),
                "scope_predicate_id": "global_macrofungi_policy_v1"}}

    manifests = {f"group-b-{cls.replace('_', '-')}.manifest.json":
                 build_manifest(cls, rows, pins) for cls in EVIDENCE_CLASSES}
    parent = manifests["group-b-shared-synonymy.manifest.json"]
    review_manifests = {
        f"group-b-shared-synonymy--{cls.replace('_', '-')}.manifest.json":
        build_review_manifest(cls, rows, pins, parent, nortaxa_source)
        for cls in SHARED_REVIEW_CLASSES}
    disagrees = review_manifests[
        "group-b-shared-synonymy--accepted-authorship-disagrees.manifest.json"]
    sub_classes = sorted({r[SUB_CLASS_COLUMN] for r in shared_rows
                          if r[SUB_CLASS_COLUMN]})
    sub_manifests = {
        "group-b-shared-synonymy--accepted-authorship-disagrees--"
        f"{sub.replace('_', '-')}.manifest.json":
        build_review_manifest(AUTHORSHIP_DISAGREES, rows, pins, parent,
                              nortaxa_source, sub)
        for sub in sub_classes}
    disagrees["sub_classes"] = sub_classes
    review_manifests.update(sub_manifests)
    review_manifests[RECIPROCAL_ONE_TO_ONE_MANIFEST] = build_decision_manifest(
        RECIPROCAL_ONE_TO_ONE_MANIFEST, rows, pins, (EVIDENCE_RECIPROCAL,),
        True, APPROVAL_BATCH,
        "evidence class reciprocal_accepted_synonymy" + _ONE_TO_ONE_RULE,
        nortaxa_source)
    review_manifests[NOT_ONE_TO_ONE_MANIFEST] = build_decision_manifest(
        NOT_ONE_TO_ONE_MANIFEST, rows, pins,
        (EVIDENCE_RECIPROCAL, EVIDENCE_SHARED_SYNONYMY), False,
        APPROVAL_INDIVIDUAL, NOT_ONE_TO_ONE_RULE, nortaxa_source)

    # Every pair is in exactly one decision leaf: the manifests that are not
    # decided through a partition.
    leaves = {name: m for name, m in [*manifests.items(),
                                      *review_manifests.items()]
              if m["approval_mode"] != APPROVAL_VIA_PARTITION}
    leaf_of = {}
    for name, m in leaves.items():
        for member in m["members"]:
            pair = (member[0], member[2])
            if pair in leaf_of:
                raise SystemExit(f"pair {pair} is in two decision manifests")
            leaf_of[pair] = name
    if set(leaf_of) != {(r["nortaxa_taxon_id"], r["col_usage_id"])
                        for r in rows} or len(leaf_of) != len(rows):
        raise SystemExit("decision manifests do not partition the pairs")
    if sum(m["member_count"] for m in manifests.values()) != len(rows):
        raise SystemExit("Group-B evidence classes do not partition the pairs")
    width = len(B_COLUMNS)
    for whole in (parent, disagrees):
        inside = {(m[0], m[2]): m for m in whole["members"]}
        parts = sorted(m[:width] for leaf in leaves.values()
                       for m in leaf["members"] if (m[0], m[2]) in inside)
        if parts != sorted(m[:width] for m in inside.values()):
            raise SystemExit("decision leaves do not partition "
                             f"{whole.get('review_class') or 'shared_synonymy'}")
    for name, leaf in leaves.items():
        if leaf["approval_mode"] == APPROVAL_BATCH and not all(
                dict(zip(leaf["columns"], m))["one_to_one"]
                for m in leaf["members"]):
            raise SystemExit(f"batch manifest holds a not-one-to-one pair: "
                             f"{name}")
    placement = leaf_of

    def counts(subset):
        by_class = collections.Counter(r["evidence_class"] for r in subset)
        return {
            "pairs": len(subset),
            "distinct_nortaxa_concepts": len(
                {r["nortaxa_taxon_id"] for r in subset}),
            "distinct_col_concepts": len({r["col_usage_id"] for r in subset}),
            "with_nortaxa_vernaculars": sum(
                1 for r in subset if r["nortaxa_vernacular_languages"]),
            "by_evidence_class": {c: by_class.get(c, 0)
                                  for c in EVIDENCE_CLASSES},
            "shared_synonymy_by_review_class": {
                c: sum(1 for r in subset if r.get("review_class") == c)
                for c in SHARED_REVIEW_CLASSES},
            "accepted_authorship_disagrees_by_sub_class": {
                c: sum(1 for r in subset if r.get(SUB_CLASS_COLUMN) == c)
                for c in sub_classes},
            "by_accepted_authorship_difference": {
                str(k): v for k, v in sorted(collections.Counter(
                    str(r["accepted_authorship_difference"])
                    for r in subset).items())},
        }

    cloud_rows = [r for r in rows if r["col_in_cloud_scope"]]
    report = {
        "format": "sporely-taxonomy-v3-stage2-group-b-v1",
        "pins": pins,
        "definition": ("NorTaxa-canonical concept and COL-canonical concept "
                       "with equal canonical_scientific_name (a finding "
                       "query, not evidence); graded by the sources' "
                       "published synonymy; cloud scope counts pairs whose "
                       "COL side is in scope — no NorTaxa side is"),
        "decision_status": (
            "owner decision 2026-09-29 (check 'group-b-owner-decisions', gate "
            "fbeea497a1cb45c8a94baae48be43acc): approved the "
            "typography_only--ordinary, sanctioning_citation--ordinary and "
            "different_authorship--ordinary manifests by file_sha256, and the "
            "pairs 56210/QMKY, 56449/3GBK2 and 58766/XQZ6; every other "
            "candidate stays open. The supersessions are in "
            "policies/concept_supersessions.yml. The manifests themselves "
            "stay needs_review inputs and are not rewritten by a decision."),
        "nortaxa_col_source": nortaxa_source,
        "authorship_difference_precedence": list(AUTHORSHIP_DIFFERENCES),
        "authorship_differences": AUTHORSHIP_DIFFERENCES,
        "full_release": counts(rows),
        "cloud_scope": counts(cloud_rows),
        "cloud_scope_with_nortaxa_vernaculars": counts(
            [r for r in cloud_rows if r["nortaxa_vernacular_languages"]]),
        "not_one_to_one": not_one_to_one(rows, leaf_of),
        "regression_outcomes": regression_outcomes(
            {(r["nortaxa_taxon_id"], r["col_usage_id"]): r for r in rows},
            explicit, placement),
    }
    return {"report": report, "manifests": manifests,
            "review_manifests": review_manifests, "rows": rows,
            "leaves": leaves, "leaf_of": leaf_of}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--manifest", type=Path, default=stage0.DEFAULT_MANIFEST)
    parser.add_argument("--scope-policy", type=Path, default=stage0.SCOPE_POLICY)
    parser.add_argument("--nortaxa-archive", type=Path,
                        default=stage0.DEFAULT_NORTAXA)
    parser.add_argument("--col-archive", type=Path, default=stage0.DEFAULT_COL)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = audit(args)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    files = {}
    for name, manifest in [*result["manifests"].items(),
                           *result["review_manifests"].items()]:
        entry = {k: manifest[k] for k in (
            "evidence_class", "approval_mode", "member_count",
            "cloud_scope_member_count",
            "cloud_scope_with_nortaxa_vernaculars_count", "members_sha256")}
        for key in ("review_class", "sub_class", "decision_leaf"):
            if key in manifest:
                entry[key] = manifest[key]
        entry["file_sha256"] = _write_json(args.out_dir / name, manifest)
        files[name] = entry
    file_sha = {n: e["file_sha256"] for n, e in files.items()}
    report = {**result["report"], "manifests": files,
              "decision_manifests": decision_manifests(result["leaves"],
                                                       file_sha),
              "pair_review_queue": pair_review_queue(
                  result["rows"], result["leaf_of"], result["leaves"],
                  file_sha)}
    _write_json(args.out_dir / "group-b-report.json", report)
    summary = {k: {c: report[k][c] for c in (
        "pairs", "by_evidence_class", "by_accepted_authorship_difference",
        "accepted_authorship_disagrees_by_sub_class")}
        for k in ("full_release", "cloud_scope")}
    summary["regression_outcomes"] = {
        s: (o["evidence_class"], o["authorship_sub_class"] or
            o["review_class"], o["outcome"])
        for s, o in report["regression_outcomes"].items()}
    print(json.dumps(summary, indent=1, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
