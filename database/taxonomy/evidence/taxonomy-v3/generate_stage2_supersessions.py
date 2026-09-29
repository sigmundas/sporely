#!/usr/bin/env python3
"""Taxonomy v3 Stage 2: reviewed supersessions for the owner-approved pairs.

On 2026-09-29 the owner approved (manual verification, check
``group-b-owner-decisions``, gate ``fbeea497a1cb45c8a94baae48be43acc``):

* three Group-B decision manifests, each by its ``file_sha256``:
  ``accepted_authorship_disagrees`` sub-classes ``typography_only--ordinary``,
  ``sanctioning_citation--ordinary`` and ``different_authorship--ordinary``;
* three pairs individually: Cantharellus cibarius (NorTaxa 56210 / COL QMKY)
  and Gloeophyllum odoratum (NorTaxa 56449 / COL 3GBK2), both also members of
  the sanctioning-citation manifest, and Conocybe vexans / Pholiotina vexans
  (NorTaxa 58766 / COL XQZ6), graded ``reciprocal_accepted_synonymy`` outside
  Group B.

Everything else stays open. In a Group-B pair both concepts already hold their
own allocated ``sporely_taxon_id``, so an approved relationship is a concept
supersession (``mapping_policy.continuity_rules.merge``), as 52369 -> 83668
was: the NorTaxa concept is retired append-only, and its usages, vernaculars
and external ids are re-keyed onto the surviving COL concept by the compiler.

This script writes them into ``policies/concept_supersessions.yml``:

* each manifest approval once, in ``approved_manifests``: path,
  ``file_sha256``, pins, approver, date and decision reference;
* one approved ``exact`` supersession per manifest member, carrying
  ``approved_manifest`` (the ``file_sha256`` and the member);
* one individually reviewed supersession for Conocybe vexans.

It stops, writing nothing, unless every manifest hashes to its approved
``file_sha256``, carries the approved pins, is a ``batch_by_file_sha256`` leaf
of one-to-one pairs, and the registry anchors every member's NorTaxa usage and
COL usage at the member's two ``sporely_taxon_id`` values; and unless the
records retire distinct concepts onto distinct survivors, none of which is
itself retired. The registry is not written. The compiler and
``validate_policies.py`` re-check every manifest-bound record with
``bridge_emission.verify_supersession_manifest_approvals``.

Idempotent and byte-deterministic: records generated here are replaced, every
other record is kept. ``--check`` verifies the committed ledger is current.

    .venv/bin/python database/taxonomy/evidence/taxonomy-v3/generate_stage2_supersessions.py
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parents[3]
_TAXONOMY = _REPO / "database" / "taxonomy"
for _path in (_HERE, _TAXONOMY / "scripts"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from bridge_emission import verify_supersession_manifest_approvals  # noqa: E402
from generate_stage1a_mappings import APPROVED_PINS, _dump  # noqa: E402
from identity_registry import IdentityRegistry  # noqa: E402

STAGE2 = "database/taxonomy/evidence/taxonomy-v3/stage2"
_SUB = "group-b-shared-synonymy--accepted-authorship-disagrees--"
#: ``(manifest file, sub-class, approved file_sha256)``, as the owner named them.
APPROVED_MANIFESTS = (
    (f"{STAGE2}/{_SUB}typography-only--ordinary.manifest.json",
     "typography_only--ordinary",
     "dd7a7bd0dcb6b57e0f147b5a7f27f91eb1e513f640bdcf126dc72c5485808e42"),
    (f"{STAGE2}/{_SUB}sanctioning-citation--ordinary.manifest.json",
     "sanctioning_citation--ordinary",
     "b49338db18c097689fb0239bf68adc5640409604ad6816cad74967c20c9fdcba"),
    (f"{STAGE2}/{_SUB}different-authorship--ordinary.manifest.json",
     "different_authorship--ordinary",
     "7edfb0f253bd278af6c4d5e3f7462cc69c7fa6f9aac1c95b71840a4271bd8653"),
)
#: Pairs the owner also approved one by one: ``(nortaxa_taxon_id, col_usage_id)``.
INDIVIDUALLY_APPROVED = {
    ("56210", "QMKY"): "Cantharellus cibarius",
    ("56449", "3GBK2"): "Gloeophyllum odoratum",
    ("58766", "XQZ6"): "Conocybe vexans / Pholiotina vexans",
}
#: The individually approved pair that is in no approved manifest.
CONOCYBE_VEXANS = {"nortaxa_taxon_id": "58766", "nortaxa_sporely_taxon_id": 627000,
                   "col_usage_id": "XQZ6", "col_sporely_taxon_id": 617026}

APPROVED_BY = "Sigmund Ås"
APPROVED_AT = "2026-09-29"
RECORD_TIMESTAMP = "2026-09-29T00:00:00Z"
DECISION_REFERENCE = (
    "manual verification 2026-09-29, check 'group-b-owner-decisions', gate "
    "fbeea497a1cb45c8a94baae48be43acc (taxonomy-v3 Stage 2, decision 2)"
)
PARTITION_REFERENCE = (
    "manual verification 2026-09-29, check 'group-b-authorship-partition', "
    "gate fbeea497a1cb45c8a94baae48be43acc: the owner accepted the "
    "typography_only / sanctioning_citation / different_authorship partition"
)
OWNER_DECISION = (
    "I approve the routine one-to-one cases with independent shared-synonym "
    "evidence across all three authorship categories: typography_only--"
    "ordinary, sanctioning_citation--ordinary, different_authorship--ordinary "
    "[by SHA-256]. I also explicitly approve: Cantharellus cibarius: NorTaxa "
    "56210 <-> COL QMKY; Gloeophyllum odoratum: NorTaxa 56449 <-> COL 3GBK2; "
    "Conocybe vexans / Pholiotina vexans: NorTaxa 58766 <-> COL XQZ6. All other "
    "cases remain open for now. In particular, this approval does not include "
    "ambiguous one-to-many cases, weak-evidence groups, cases with no "
    "published cross-reference, or the individually reviewed one-directional "
    "cases."
)
RECORD_PREFIX = "taxonomy-v3-2-"
LEDGER = _TAXONOMY / "policies" / "concept_supersessions.yml"
REGISTRY = _TAXONOMY / "registry" / "canonical"
STAGE0_REPORT = _HERE / "stage0" / "coverage-report.json"
NORTAXA_VERSION = APPROVED_PINS["source_archives"]["nortaxa"][
    "source_release_id"].split(":")[1]
APPROVED_MANIFEST_RULE = (
    "A record carrying approved_manifest was generated from an owner-approved "
    "Group-B decision manifest listed in approved_manifests. The compiler and "
    "validator refuse it unless the manifest file hashes to "
    "approved_manifest.file_sha256, carries the approval's pins, is a "
    "batch_by_file_sha256 manifest of one-to-one pairs, and the record retires "
    "exactly one member's NorTaxa concept onto that member's COL usage "
    "(bridge_emission.verify_supersession_manifest_approvals)."
)


class GenerationError(RuntimeError):
    pass


def load_manifest(path: str, sub_class: str, file_sha256: str) -> dict:
    raw = (_REPO / path).read_bytes()
    actual = hashlib.sha256(raw).hexdigest()
    if actual != file_sha256:
        raise GenerationError(
            f"{path} has sha256 {actual}; the owner approved {file_sha256}. "
            f"Stopping: a different file is not the approved manifest.")
    manifest = json.loads(raw.decode("utf-8"))
    if manifest.get("pins") != APPROVED_PINS:
        raise GenerationError(f"{path} pins differ from the approved pins")
    if manifest.get("sub_class") != sub_class \
            or manifest.get("approval_mode") != "batch_by_file_sha256":
        raise GenerationError(f"{path} is not the approved {sub_class} leaf")
    members = [dict(zip(manifest["columns"], row))
               for row in manifest["members"]]
    if manifest.get("member_count") != len(members):
        raise GenerationError(f"{path} member_count is inconsistent")
    if not all(m["one_to_one"] is True for m in members):
        raise GenerationError(f"{path} holds a pair that is not one-to-one")
    return manifest


def check_registry(pairs: list[dict]) -> None:
    """Both usages must already be anchors of the pair's two concepts."""
    registry = IdentityRegistry(REGISTRY)
    registry.load()
    for pair in pairs:
        for key, sporely_id in (
            (("nortaxa", "nortaxa_taxon_id", pair["nortaxa_taxon_id"]),
             int(pair["nortaxa_sporely_taxon_id"])),
            (("col_xr", "col_usage_id", pair["col_usage_id"]),
             int(pair["col_sporely_taxon_id"])),
        ):
            found = registry.lookup(*key)
            if found is None or found.sporely_taxon_id != sporely_id \
                    or found.kind != "anchor":
                raise GenerationError(
                    f"registry does not anchor {key!r} at sporely_taxon_id "
                    f"{sporely_id} (found {found!r}). Stopping.")


def check_conocybe_vexans() -> dict:
    """The individually approved pair must still grade as it was reviewed."""
    report = json.loads(STAGE0_REPORT.read_text(encoding="utf-8"))
    placements = report["regressions"]["Conocybe vexans / Pholiotina vexans"][
        "placements"]
    graded = [p for p in placements
              if (p.get("nortaxa_taxon_id"), p.get("col_usage_id"))
              == ("58766", "XQZ6")]
    if len(graded) != 1 or graded[0]["evidence_class"] \
            != "reciprocal_accepted_synonymy":
        raise GenerationError("Conocybe vexans pair no longer grades "
                              "reciprocal_accepted_synonymy in Stage 0")
    return graded[0]


def _manifest_id(sub_class: str) -> str:
    return ("taxonomy-v3-group-b-accepted-authorship-disagrees--"
            + sub_class.replace("_", "-"))


def _rationale(member: dict, sub_class: str) -> str:
    kinds = member["shared_synonym_kind_counts"]
    kind_text = ", ".join(f"{count} {kind}"
                          for kind, count in sorted(kinds.items()))
    return (
        f"Member of the owner-approved Group-B manifest "
        f"{_manifest_id(sub_class)} (see approved_manifests). NorTaxa concept "
        f"{member['nortaxa_taxon_id']} (sporely_taxon_id "
        f"{member['nortaxa_sporely_taxon_id']}, "
        f"'{member['bridge_accepted_name']}') and COL {member['col_usage_id']} "
        f"(sporely_taxon_id {member['col_sporely_taxon_id']}, "
        f"'{member['backbone_accepted_name']}') share "
        f"{member['shared_synonym_count']} published synonym name(s) "
        f"({kind_text}); at least one is ordinary: independently corroborated, "
        f"not NorTaxa-derived, no weak status. The accepted authorships differ "
        f"by '{member['accepted_authorship_difference']}'. The shared canonical "
        f"name only found the pair and is not identity evidence. The pair is "
        f"one-to-one. The NorTaxa concept is retired append-only and its "
        f"usages, vernaculars and external ids are re-keyed onto the COL "
        f"concept; the registry keeps both allocations."
    )


def _record(member: dict, sub_class: str, file_sha256: str, path: str) -> dict:
    nortaxa_id = str(member["nortaxa_taxon_id"])
    col_id = str(member["col_usage_id"])
    references = [
        f"{path}#nortaxa_taxon_id={nortaxa_id} file_sha256={file_sha256}",
        DECISION_REFERENCE,
    ]
    individually = INDIVIDUALLY_APPROVED.get((nortaxa_id, col_id))
    if individually:
        references.append(
            f"also approved individually by the owner in the same decision: "
            f"{individually}, NorTaxa {nortaxa_id} <-> COL {col_id}")
    return {
        "supersession_id": f"{RECORD_PREFIX}nortaxa-{nortaxa_id}"
                           f"-superseded-by-col-{col_id}",
        "superseded_sporely_taxon_id": int(member["nortaxa_sporely_taxon_id"]),
        "current_source_usage": {"source": "col_xr",
                                 "namespace": "col_usage_id",
                                 "identifier": col_id},
        "relationship": "exact",
        "review_status": "approved",
        "reviewer": APPROVED_BY,
        "rationale": _rationale(member, sub_class),
        "evidence_references": references,
        "created_at": RECORD_TIMESTAMP,
        "updated_at": RECORD_TIMESTAMP,
        "source_release_range": {"first": NORTAXA_VERSION,
                                 "last": NORTAXA_VERSION},
        "supersedes": None,
        "approved_manifest": {
            "file_sha256": file_sha256,
            "member": {k: member[k] for k in (
                "nortaxa_taxon_id", "nortaxa_sporely_taxon_id",
                "col_usage_id", "col_sporely_taxon_id")},
        },
    }


def _conocybe_record(graded: dict) -> dict:
    pair = CONOCYBE_VEXANS
    return {
        "supersession_id": f"{RECORD_PREFIX}nortaxa-58766-pholiotina-vexans"
                           f"-superseded-by-col-XQZ6",
        "superseded_sporely_taxon_id": pair["nortaxa_sporely_taxon_id"],
        "current_source_usage": {"source": "col_xr",
                                 "namespace": "col_usage_id",
                                 "identifier": pair["col_usage_id"]},
        "relationship": "exact",
        "review_status": "approved",
        "reviewer": APPROVED_BY,
        "rationale": (
            "Divergent-accepted-name pair, outside Group B because the "
            "canonical names differ. NorTaxa accepts "
            f"'{graded['bridge_accepted_name']}' (taxonID 58766, "
            "sporely_taxon_id 627000); COL accepts "
            f"'{graded['backbone_accepted_name']}' (usage XQZ6, "
            "sporely_taxon_id 617026). The evidence is reciprocal published "
            "synonymy: NorTaxa lists COL's accepted name as a synonym of "
            "58766, and COL lists NorTaxa's accepted name as a synonym of "
            "XQZ6, so each source has published that the other's accepted "
            "name belongs to its own concept. 'Pholiotina vexans (P.D. Orton) "
            "Bon' cites 'P.D. Orton' as its parenthetical basionym author: it "
            "is a recombination of 'Conocybe vexans P.D. Orton'. Same shape "
            "as the approved 52369 -> 83668 supersession. Both concepts hold "
            "allocations from tax-2026.07.29-01, so the NorTaxa concept is "
            "retired append-only rather than re-allocated; its usages, the "
            "Norwegian names 'vrang ringerlehatt', 'vrang ringkjeglesopp', "
            "'tosporet ringkjeglesopp' and its other vernaculars are re-keyed "
            "onto 617026, which keeps COL's accepted name preferred."),
        "evidence_references": [
            "database/taxonomy/evidence/taxonomy-v3/stage0/coverage-report.json"
            "#regressions.Conocybe vexans / Pholiotina vexans",
            "database/taxonomy/evidence/taxonomy-v3/stage2/group-b-report.json"
            "#regression_outcomes.Conocybe vexans / Pholiotina vexans",
            f"sources/nortaxa/1.284/archive.zip!taxon.txt sha256="
            f"{APPROVED_PINS['source_archives']['nortaxa']['sha256']}",
            f"sources/col_xr/2026-07-17-XR/archive.zip!NameUsage.tsv sha256="
            f"{APPROVED_PINS['source_archives']['col_xr']['sha256']}",
            DECISION_REFERENCE,
        ],
        "created_at": RECORD_TIMESTAMP,
        "updated_at": RECORD_TIMESTAMP,
        "source_release_range": {"first": NORTAXA_VERSION,
                                 "last": NORTAXA_VERSION},
        "supersedes": None,
        "review_note": (
            f"Approved {APPROVED_AT} by {APPROVED_BY}, recorded against "
            f"{DECISION_REFERENCE}. Reviewer's decision as given: "
            f"'{OWNER_DECISION}'"),
    }


def _approval(path: str, sub_class: str, file_sha256: str,
              manifest: dict) -> dict:
    return {
        "manifest_id": _manifest_id(sub_class),
        "path": path,
        "file_sha256": file_sha256,
        "members_sha256": manifest["members_sha256"],
        "member_count": manifest["member_count"],
        "evidence_class": manifest["evidence_class"],
        "review_class": manifest["review_class"],
        "sub_class": sub_class,
        "pins": APPROVED_PINS,
        "approved_by": APPROVED_BY,
        "approved_at": APPROVED_AT,
        "decision_reference": DECISION_REFERENCE,
        "partition_reference": PARTITION_REFERENCE,
        "rationale": (
            "One owner decision over an explicitly enumerated, immutable "
            "Group-B manifest (taxonomy-v3 decision 2). Each member is a "
            "NorTaxa-canonical and a COL-canonical concept with the same "
            "canonical name, which only found the pair. The evidence reviewed "
            "is each source's published synonymy: both accepted usages list "
            "at least one shared synonym of kind 'ordinary' (independently "
            "corroborated, not republished by COL from NorTaxa, no weak "
            "status), species rank, not low overlap, and the pair is "
            "one-to-one. The accepted authorships differ only in the way "
            "this sub-class names, which the owner accepted does not by "
            "itself prevent treating the two as one taxon. Both concepts are "
            "already allocated, so each member becomes a supersession that "
            "retires the NorTaxa concept."),
        "scope_note": (
            "Approves exactly the members enumerated in this file, pinned to "
            "the release and source releases above. It does not approve the "
            "parent manifests, any sibling review class or sub-class, "
            "not-one-to-one pairs, one-directional pairs, pairs with no "
            "published cross-reference, a regenerated manifest with a "
            "different file_sha256, or any taxon in a later source release."),
    }


def render() -> str:
    approved = [(path, sub, sha, load_manifest(path, sub, sha))
                for path, sub, sha in APPROVED_MANIFESTS]
    graded = check_conocybe_vexans()
    pairs = [dict(zip(m["columns"], row))
             for *_, m in approved for row in m["members"]]
    placed = {(p["nortaxa_taxon_id"], p["col_usage_id"]) for p in pairs}
    missing = [pair for pair in INDIVIDUALLY_APPROVED
               if pair not in placed and pair != ("58766", "XQZ6")]
    if missing:
        raise GenerationError(f"individually approved pairs in no approved "
                              f"manifest: {missing}")
    check_registry(pairs + [CONOCYBE_VEXANS])
    retiring = [int(p["nortaxa_sporely_taxon_id"]) for p in pairs] \
        + [CONOCYBE_VEXANS["nortaxa_sporely_taxon_id"]]
    surviving = [int(p["col_sporely_taxon_id"]) for p in pairs] \
        + [CONOCYBE_VEXANS["col_sporely_taxon_id"]]
    if len(set(retiring)) != len(retiring) \
            or len(set(surviving)) != len(surviving) \
            or set(retiring) & set(surviving):
        raise GenerationError("approved pairs do not retire distinct concepts "
                              "onto distinct, non-retiring survivors")

    document = json.loads(LEDGER.read_text(encoding="utf-8"))
    kept = [entry for entry in document.get("supersessions") or []
            if not str(entry.get("supersession_id", "")).startswith(
                RECORD_PREFIX)]
    clash = sorted(entry["supersession_id"] for entry in kept
                   if entry.get("superseded_sporely_taxon_id")
                   in set(retiring) | set(surviving))
    if clash:
        raise GenerationError(f"existing records touch approved concepts: "
                              f"{clash}")
    generated = sorted(
        (_record(dict(zip(m["columns"], row)), sub, sha, path)
         for path, sub, sha, m in approved for row in m["members"]),
        key=lambda r: r["superseded_sporely_taxon_id"])
    generated.append(_conocybe_record(graded))
    approvals = [a for a in document.get("approved_manifests") or []
                 if a.get("file_sha256") not in {s for _, _, s in
                                                 APPROVED_MANIFESTS}]
    approvals += [_approval(path, sub, sha, m) for path, sub, sha, m in approved]

    schema = dict(document["schema"])
    schema["approved_manifest_rule"] = APPROVED_MANIFEST_RULE
    out = {key: value for key, value in document.items()
           if key not in ("schema", "approved_manifests", "supersessions")}
    out["schema"] = schema
    out["approved_manifests"] = approvals
    out["supersessions"] = kept + generated
    bound = verify_supersession_manifest_approvals(out, repo_root=_REPO)
    if len(bound) != sum(m["member_count"] for *_, m in approved):
        raise GenerationError("not every manifest member has one record")
    return _dump(out) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true",
                        help="fail if concept_supersessions.yml is not current")
    args = parser.parse_args()
    try:
        text = render()
    except GenerationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    current = LEDGER.read_text(encoding="utf-8")
    if args.check:
        if current != text:
            print("concept_supersessions.yml is not current", file=sys.stderr)
            return 1
        print("concept_supersessions.yml is current")
        return 0
    LEDGER.write_text(text, encoding="utf-8")
    print(f"wrote {LEDGER.relative_to(_REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
