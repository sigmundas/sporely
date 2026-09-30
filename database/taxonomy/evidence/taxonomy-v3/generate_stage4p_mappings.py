#!/usr/bin/env python3
"""Taxonomy v3 Stage 4P: reviewed Dyntaxa mappings for owner-approved manifests.

Dyntaxa gains Sporely identity only through a reviewed relationship
(``compile_release.REVIEWED_IDENTITY_ONLY_SOURCES``). Under decision 2 the
owner may approve an explicitly enumerated, immutable candidate manifest from
``stage4p/`` with one decision, which must still yield one auditable
per-association record per member. This script turns such decisions into
records in ``policies/manual_mappings.yml``, exactly as
``generate_stage1a_mappings.py`` did for NorTaxa:

* each approval once, in ``approved_manifests``, with the manifest path, its
  ``file_sha256``, its pins and who approved it when;
* one approved ``exact`` mapping per member, Dyntaxa taxon LSID -> COL usage,
  with the usual review provenance and an ``approved_manifest`` reference to
  the ``file_sha256`` and the member.

``OWNER_APPROVALS`` holds the decisions, copied from the plan's owner gate.
**It is empty: no Dyntaxa manifest is approved**, so a real run generates
nothing. The path is exercised by tests with synthetic approvals only.

It stops, writing nothing, unless for every approval: the manifest hashes to
the approved ``file_sha256``; it is a matched Dyntaxa manifest of a
batch-reviewable evidence class (``reciprocal_accepted_synonymy``, or a
``shared_synonymy`` review class; decision 2 leaves
``one_directional_accepted_synonymy`` to individual review and
``no_published_cross_reference`` unresolved); its pins equal the pins the
approval names, and its Dyntaxa pin is the archive the acquisition record
pins; and the registry anchors every member's COL usage on the member's
``sporely_taxon_id`` and binds its Dyntaxa usage nowhere else. The records
therefore allocate nothing. The compiler re-checks every record against its
manifest (``bridge_emission.verify_manifest_approvals``), so a record for a
taxon the file does not list cannot borrow the approval.

Idempotent and byte-deterministic: records generated from a Dyntaxa manifest
are replaced, every other record is kept. ``--check`` verifies the committed
ledger is current without writing.

    .venv/bin/python database/taxonomy/evidence/taxonomy-v3/generate_stage4p_mappings.py
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
for _path in (_TAXONOMY / "scripts", _HERE):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from bridge_emission import verify_manifest_approvals  # noqa: E402
from generate_stage1a_mappings import _dump  # noqa: E402
from identity_registry import IdentityRegistry  # noqa: E402

#: Owner decisions, each ``{path, file_sha256, pins, approved_by,
#: approved_at, decision_reference}``, copied from the plan's recorded gate.
#: Empty until the owner approves a Dyntaxa manifest.
OWNER_APPROVALS: tuple[dict, ...] = ()

POPULATION = "dyntaxa_fungi_accepted_concepts"
BATCH_REVIEWABLE = frozenset({"reciprocal_accepted_synonymy", "shared_synonymy"})
DYNTAXA_LSID = "urn:lsid:dyntaxa.se:Taxon:"
ACQUISITION_MANIFEST = (_TAXONOMY / "sources" / "dyntaxa" / "2026-09-30"
                        / "manifest.json")
MANUAL_MAPPINGS = _TAXONOMY / "policies" / "manual_mappings.yml"
REGISTRY = _TAXONOMY / "registry" / "canonical"
RECORD_PREFIX = "taxonomy-v3-4p-dyntaxa-"


class GenerationError(RuntimeError):
    pass


def _resolve(path: str, repo_root: Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else repo_root / candidate


def load_approved_manifest(approval: dict, *, repo_root: Path,
                           acquisition_manifest: Path) -> dict:
    """The approved manifest, or :class:`GenerationError` if it is not the
    exact, batch-reviewable, correctly pinned file the approval names."""
    for field in ("path", "file_sha256", "pins", "approved_by", "approved_at",
                  "decision_reference"):
        if not approval.get(field):
            raise GenerationError(f"approval carries no {field}")
    path = _resolve(str(approval["path"]), repo_root)
    raw = path.read_bytes()
    actual = hashlib.sha256(raw).hexdigest()
    if actual != approval["file_sha256"]:
        raise GenerationError(
            f"{approval['path']} has sha256 {actual}; the owner approved "
            f"{approval['file_sha256']}. Stopping: a different file is not "
            f"the approved manifest.")
    manifest = json.loads(raw.decode("utf-8"))
    if manifest.get("population") != POPULATION or \
            manifest.get("candidate_kind") != "matched":
        raise GenerationError(
            f"{approval['path']} is not a matched Dyntaxa candidate manifest")
    if manifest.get("evidence_class") not in BATCH_REVIEWABLE:
        raise GenerationError(
            f"{approval['path']} is {manifest.get('evidence_class')!r}; "
            f"decision 2 allows batch approval only of "
            f"{sorted(BATCH_REVIEWABLE)}")
    if manifest.get("evidence_class") == "shared_synonymy" and \
            not manifest.get("review_class"):
        raise GenerationError(
            f"{approval['path']} is the parent shared_synonymy manifest; "
            f"only a review class may be batch-approved")
    if manifest.get("pins") != approval["pins"]:
        raise GenerationError(
            f"{approval['path']} pins differ from the pins the approval is "
            f"bound to. Stopping.")
    pinned = json.loads(acquisition_manifest.read_text(encoding="utf-8"))
    if manifest["pins"]["source_archives"]["dyntaxa"]["sha256"] != \
            pinned["download"]["sha256"]:
        raise GenerationError(
            f"{approval['path']} was computed against a Dyntaxa archive other "
            f"than the pinned acquisition. Stopping.")
    if manifest.get("member_count") != len(manifest.get("members") or []):
        raise GenerationError(f"{approval['path']} member_count is inconsistent")
    return manifest


def _members(manifest: dict) -> list[dict]:
    return [dict(zip(manifest["columns"], row)) for row in manifest["members"]]


def check_registry(registry: IdentityRegistry, members: list[dict]) -> None:
    for member in members:
        sporely_id = int(member["sporely_taxon_id"])
        anchor = registry.lookup("col_xr", "col_usage_id", member["col_usage_id"])
        if anchor is None or anchor.sporely_taxon_id != sporely_id or \
                anchor.kind != "anchor":
            raise GenerationError(
                f"registry does not anchor COL {member['col_usage_id']} on "
                f"sporely_taxon_id {sporely_id} (found {anchor!r}). Stopping.")
        bound = registry.lookup("dyntaxa", "dyntaxa_taxon_id",
                                member["dyntaxa_taxon_id"])
        if bound is not None and bound.sporely_taxon_id != sporely_id:
            raise GenerationError(
                f"registry binds {member['dyntaxa_taxon_id']} to "
                f"sporely_taxon_id {bound.sporely_taxon_id}, not {sporely_id}. "
                f"Stopping: a mapping may not rebind an identity.")


def _rationale(member: dict, manifest: dict) -> str:
    kind = manifest.get("review_class") or manifest["evidence_class"]
    return (
        f"Member of the owner-approved Dyntaxa manifest {kind} (see "
        f"approved_manifests). Dyntaxa {member['dyntaxa_taxon_id']} "
        f"('{member['bridge_accepted_name']}') and COL {member['col_usage_id']} "
        f"('{member['backbone_accepted_name']}') are linked by the sources' "
        f"published synonymy ({manifest['evidence_class']}); the "
        f"{member['candidate_basis']} match only proposed the pair. The COL "
        f"usage anchors sporely_taxon_id {member['sporely_taxon_id']}; no "
        f"allocation changes.")


def _record(member: dict, manifest: dict, approval: dict, version: str) -> dict:
    lsid = str(member["dyntaxa_taxon_id"])
    col_id = str(member["col_usage_id"])
    number = lsid[len(DYNTAXA_LSID):] if lsid.startswith(DYNTAXA_LSID) else lsid
    return {
        "mapping_id": f"{RECORD_PREFIX}{number}-to-col-{col_id}",
        "source_usage": {"source": "dyntaxa", "namespace": "dyntaxa_taxon_id",
                         "identifier": lsid},
        "target": {"source_usage": {"source": "col_xr",
                                    "namespace": "col_usage_id",
                                    "identifier": col_id}},
        "relationship": "exact",
        "review_status": "approved",
        "reviewer": approval["approved_by"],
        "rationale": _rationale(member, manifest),
        "evidence_references": [
            f"{approval['path']}#dyntaxa_taxon_id={lsid} "
            f"file_sha256={approval['file_sha256']}",
            approval["decision_reference"],
        ],
        "created_at": f"{approval['approved_at']}T00:00:00Z",
        "updated_at": f"{approval['approved_at']}T00:00:00Z",
        "source_release_range": {"first": version, "last": version},
        "supersedes": None,
        "approved_manifest": {
            "file_sha256": approval["file_sha256"],
            "member": {"dyntaxa_taxon_id": lsid, "col_usage_id": col_id,
                       "sporely_taxon_id": int(member["sporely_taxon_id"])},
        },
    }


def _ledger_approval(approval: dict, manifest: dict) -> dict:
    return {
        "manifest_id": "taxonomy-v3-dyntaxa-" + (
            manifest.get("review_class") or manifest["evidence_class"]),
        "path": approval["path"],
        "file_sha256": approval["file_sha256"],
        "members_sha256": manifest["members_sha256"],
        "member_count": manifest["member_count"],
        "evidence_class": manifest["evidence_class"],
        **({"review_class": manifest["review_class"]}
           if manifest.get("review_class") else {}),
        "pins": manifest["pins"],
        "approved_by": approval["approved_by"],
        "approved_at": approval["approved_at"],
        "decision_reference": approval["decision_reference"],
        "scope_note": (
            "Approves exactly the members enumerated in this file, pinned to "
            "the release and source releases above. It approves no sibling "
            "or parent manifest, no regenerated file with a different "
            "file_sha256, and no taxon of a later Dyntaxa export."),
    }


def _is_dyntaxa_manifest_record(entry: dict) -> bool:
    return bool(entry.get("approved_manifest")) and \
        (entry.get("source_usage") or {}).get("source") == "dyntaxa"


def render_document(document: dict, approvals: tuple[dict, ...] | list[dict], *,
                    registry: IdentityRegistry, repo_root: Path = _REPO,
                    acquisition_manifest: Path = ACQUISITION_MANIFEST) -> dict:
    """``document`` with its Dyntaxa manifest records regenerated from
    ``approvals``. Pure apart from reading the manifests."""
    generated: list[dict] = []
    ledger_approvals: list[dict] = []
    approved_shas: set[str] = set()
    for approval in approvals:
        manifest = load_approved_manifest(
            approval, repo_root=repo_root,
            acquisition_manifest=acquisition_manifest)
        members = _members(manifest)
        check_registry(registry, members)
        version = manifest["pins"]["source_archives"]["dyntaxa"][
            "source_release_id"].split(":")[1]
        generated += [_record(m, manifest, approval, version) for m in members]
        ledger_approvals.append(_ledger_approval(approval, manifest))
        approved_shas.add(str(approval["file_sha256"]))

    kept = [e for e in document.get("mappings") or []
            if not _is_dyntaxa_manifest_record(e)]
    generated_usages = {r["source_usage"]["identifier"] for r in generated}
    clash = sorted(
        e["mapping_id"] for e in kept
        if (e.get("source_usage") or {}).get("source") == "dyntaxa"
        and str((e.get("source_usage") or {}).get("identifier"))
        in generated_usages)
    if clash:
        raise GenerationError(
            f"existing records already map manifest members: {clash}")
    ids = [r["mapping_id"] for r in generated]
    if len(ids) != len(set(ids)):
        raise GenerationError("two approved manifests list the same member")
    generated.sort(key=lambda r: r["mapping_id"])

    dyntaxa_manifest_shas = {
        (e.get("approved_manifest") or {}).get("file_sha256")
        for e in document.get("mappings") or [] if _is_dyntaxa_manifest_record(e)}
    out = {k: v for k, v in document.items()
           if k not in ("approved_manifests", "mappings")}
    out["approved_manifests"] = [
        a for a in document.get("approved_manifests") or []
        if a.get("file_sha256") not in approved_shas | dyntaxa_manifest_shas
    ] + ledger_approvals
    out["mappings"] = kept + generated
    verify_manifest_approvals(out, repo_root=repo_root)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true",
                        help="fail if manual_mappings.yml is not current")
    args = parser.parse_args()
    registry = IdentityRegistry(REGISTRY)
    registry.load()
    document = json.loads(MANUAL_MAPPINGS.read_text(encoding="utf-8"))
    try:
        text = _dump(render_document(document, OWNER_APPROVALS,
                                     registry=registry)) + "\n"
    except GenerationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    current = MANUAL_MAPPINGS.read_text(encoding="utf-8")
    if args.check:
        if current != text:
            print("manual_mappings.yml is not current", file=sys.stderr)
            return 1
        print("manual_mappings.yml is current")
        return 0
    if not OWNER_APPROVALS:
        print("no Dyntaxa manifest is approved; nothing generated")
    if current != text:
        MANUAL_MAPPINGS.write_text(text, encoding="utf-8")
        print(f"wrote {MANUAL_MAPPINGS.relative_to(_REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
