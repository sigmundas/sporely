#!/usr/bin/env python3
"""Taxonomy v3 Stage 1A: reviewed mappings for the owner-approved manifest.

The owner approved, on 2026-09-28, exactly one Group-A review manifest:
``stage0/group-a-shared-synonymy--ordinary.manifest.json``, identified by its
``file_sha256`` and bound to the release, source and policy pins it was
computed against (plan ``docs/plans/active/2026-09-27-taxonomy-v3.md``,
"Owner gate (decision 2)"). One decision covers the whole manifest, but it
must still produce one auditable per-association record per member.

This script turns that decision into records in
``policies/manual_mappings.yml``:

* the approval itself, once, in ``approved_manifests`` — the manifest path,
  its ``file_sha256``, its pins and who approved it when;
* one approved ``exact`` mapping per member, NorTaxa taxonID -> COL usage,
  carrying the usual review provenance and an ``approved_manifest`` reference
  naming the ``file_sha256`` and the member it was generated from.

It stops, writing nothing, unless the manifest hashes to the approved
``file_sha256``, its pins equal the approved pins, and the registry already
binds every member's NorTaxa usage and COL anchor to the member's
``sporely_taxon_id``. The records therefore change no allocation. Nothing here
is keyed on an evidence class: a taxon absent from the approved file — a
sibling review class, or a new taxon in a later NorTaxa release — gets no
record, and the compiler (``bridge_emission.verify_manifest_approvals``)
refuses any record that claims the approval without being a member.

Idempotent and byte-deterministic: records generated from this manifest are
replaced, every other record is kept. ``--check`` verifies the committed file
is current without writing.

    .venv/bin/python database/taxonomy/evidence/taxonomy-v3/generate_stage1a_mappings.py
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[4]
_TAXONOMY = _REPO / "database" / "taxonomy"
if str(_TAXONOMY / "scripts") not in sys.path:
    sys.path.insert(0, str(_TAXONOMY / "scripts"))

from bridge_emission import verify_manifest_approvals  # noqa: E402
from identity_registry import IdentityRegistry  # noqa: E402

MANIFEST_PATH = (
    "database/taxonomy/evidence/taxonomy-v3/stage0/"
    "group-a-shared-synonymy--ordinary.manifest.json"
)
APPROVED_FILE_SHA256 = (
    "1eda453a7134995b2a596d09e0f10341a72ba7a007e2666a6e5cee9e118cda4d"
)
#: The pins the owner's approval is bound to, as recorded in the plan.
APPROVED_PINS = {
    "bridge_emission_policy": {
        "path": "database/taxonomy/policies/mapping_policy.yml",
        "sha256": "a5a329848c45fcd030165a51ad1193944dffe7573ddb2a9c69441f0d76ae4afb",
    },
    "cloud_scope_policy": {
        "path": "database/taxonomy/policies/global-macrofungi-scope.yml",
        "scope_predicate_id": "global_macrofungi_policy_v1",
        "sha256": "e4e796286df93b5372264c702c824eea746f680b3b4e8d6467621c99e5b64ea6",
    },
    "release": {
        "content_release_id": "tax-2026.09.26-02",
        "gz_artifact": "database/reference_data/generated/taxonomy_v2/tax-2026.09.26-02.sqlite3.gz",
        "gz_sha256": "4488bb64f4abe18ed269c76264f9be36708d742ff531e931e8d7458791e05d72",
        "sqlite_sha256": "9bf71b7e1f9b2915c3b1798743cefdd5db53bdbbfb7edc462aa3d0ad0cf8547d",
    },
    "source_archives": {
        "col_xr": {
            "sha256": "397d701c8eb269bf78d6ac7b03149915b0d9e2a2c18694be2c91445b807814f9",
            "source_release_id": "col_xr:2026-07-17-XR:2026-07-17",
        },
        "nortaxa": {
            "sha256": "29c11c54d955dc44e4e5a38944dd7932989a256d1b173777579b9f33abd2fe22",
            "source_release_id": "nortaxa:1.284:2026-07-17",
        },
    },
}
APPROVED_BY = "Sigmund Ås"
APPROVED_AT = "2026-09-28"
RECORD_TIMESTAMP = "2026-09-28T00:00:00Z"
DECISION_REFERENCE = (
    "docs/plans/active/2026-09-27-taxonomy-v3.md#owner-gate-decision-2"
)
MANUAL_MAPPINGS = _TAXONOMY / "policies" / "manual_mappings.yml"
REGISTRY = _TAXONOMY / "registry" / "canonical"


class GenerationError(RuntimeError):
    pass


def _load_manifest() -> dict:
    raw = (_REPO / MANIFEST_PATH).read_bytes()
    actual = hashlib.sha256(raw).hexdigest()
    if actual != APPROVED_FILE_SHA256:
        raise GenerationError(
            f"{MANIFEST_PATH} has sha256 {actual}; the owner approved "
            f"{APPROVED_FILE_SHA256}. Stopping: a different file is not the "
            f"approved manifest."
        )
    manifest = json.loads(raw.decode("utf-8"))
    if manifest.get("pins") != APPROVED_PINS:
        raise GenerationError(
            f"{MANIFEST_PATH} pins differ from the pins the approval is bound "
            f"to. Stopping."
        )
    if (manifest.get("evidence_class"), manifest.get("review_class")) \
            != ("shared_synonymy", "ordinary"):
        raise GenerationError(f"{MANIFEST_PATH} is not the ordinary manifest")
    if manifest.get("member_count") != len(manifest.get("members") or []):
        raise GenerationError(f"{MANIFEST_PATH} member_count is inconsistent")
    return manifest


def _members(manifest: dict) -> list[dict]:
    columns = manifest["columns"]
    return [dict(zip(columns, row)) for row in manifest["members"]]


def _check_registry(members: list[dict]) -> None:
    registry = IdentityRegistry(REGISTRY)
    registry.load()
    for member in members:
        sporely_id = int(member["sporely_taxon_id"])
        for key, kind in (
            (("nortaxa", "nortaxa_taxon_id", member["nortaxa_taxon_id"]), "alias"),
            (("col_xr", "col_usage_id", member["col_usage_id"]), "anchor"),
        ):
            found = registry.lookup(*key)
            if found is None or found.sporely_taxon_id != sporely_id \
                    or found.kind != kind:
                raise GenerationError(
                    f"registry does not bind {key!r} as {kind} of "
                    f"sporely_taxon_id {sporely_id} (found {found!r}). "
                    f"Stopping: Stage 1A may not change an allocation."
                )


def _rationale(member: dict) -> str:
    """The member-specific facts; the shared approval text is in the ledger."""
    kinds = member["shared_synonym_kind_counts"]
    kind_text = ", ".join(f"{count} {kind}" for kind, count in sorted(kinds.items()))
    return (
        f"Member of the owner-approved manifest "
        f"group-a-shared-synonymy--ordinary (see approved_manifests). NorTaxa "
        f"{member['nortaxa_taxon_id']} and COL {member['col_usage_id']} share "
        f"{member['shared_synonym_count']} published synonym name(s) "
        f"({kind_text}); at least one is ordinary: independently corroborated, "
        f"not NorTaxa-derived, no weak status. Their agreeing accepted name "
        f"'{member['bridge_accepted_name']}' is not itself identity evidence. "
        f"The registry already binds this usage to sporely_taxon_id "
        f"{member['sporely_taxon_id']}; no allocation changes."
    )


def _record(member: dict, nortaxa_version: str) -> dict:
    nortaxa_id = str(member["nortaxa_taxon_id"])
    col_id = str(member["col_usage_id"])
    return {
        "mapping_id": f"taxonomy-v3-1a-nortaxa-{nortaxa_id}-to-col-{col_id}",
        "source_usage": {
            "source": "nortaxa",
            "namespace": "nortaxa_taxon_id",
            "identifier": nortaxa_id,
        },
        "target": {
            "source_usage": {
                "source": "col_xr",
                "namespace": "col_usage_id",
                "identifier": col_id,
            }
        },
        "relationship": "exact",
        "review_status": "approved",
        "reviewer": APPROVED_BY,
        "rationale": _rationale(member),
        "evidence_references": [
            f"{MANIFEST_PATH}#nortaxa_taxon_id={nortaxa_id} "
            f"file_sha256={APPROVED_FILE_SHA256}",
            DECISION_REFERENCE,
        ],
        "created_at": RECORD_TIMESTAMP,
        "updated_at": RECORD_TIMESTAMP,
        "source_release_range": {"first": nortaxa_version, "last": nortaxa_version},
        "supersedes": None,
        "approved_manifest": {
            "file_sha256": APPROVED_FILE_SHA256,
            "member": {
                "nortaxa_taxon_id": nortaxa_id,
                "col_usage_id": col_id,
                "sporely_taxon_id": int(member["sporely_taxon_id"]),
            },
        },
    }


def _approval(manifest: dict) -> dict:
    return {
        "manifest_id": "taxonomy-v3-group-a-shared-synonymy--ordinary",
        "path": MANIFEST_PATH,
        "file_sha256": APPROVED_FILE_SHA256,
        "members_sha256": manifest["members_sha256"],
        "member_count": manifest["member_count"],
        "evidence_class": manifest["evidence_class"],
        "review_class": manifest["review_class"],
        "pins": APPROVED_PINS,
        "approved_by": APPROVED_BY,
        "approved_at": APPROVED_AT,
        "decision_reference": DECISION_REFERENCE,
        "rationale": (
            "One owner decision over an explicitly enumerated, immutable "
            "manifest (taxonomy-v3 decision 2). Each member is a Group-A "
            "association: the compiler bound the NorTaxa taxonID to the COL "
            "concept by an automatic accepted-name match, which is not "
            "identity evidence (mapping_policy.continuity_rules."
            "canonical_name_only). The evidence reviewed is each source's own "
            "published synonymy: both accepted usages list at least one shared "
            "synonym of kind 'ordinary' — independently corroborated, not "
            "republished by COL from NorTaxa (COL source 2030), and carrying no "
            "weak nomenclatural or textual status — with agreeing accepted-name "
            "keys, species rank, and not low overlap. Every member's NorTaxa "
            "usage is already a registry alias of the member's COL anchor, so "
            "the approval changes no allocation."
        ),
        "scope_note": (
            "Approves exactly the members enumerated in this file, pinned to "
            "the release and source releases above. It does not approve the "
            "parent shared_synonymy manifest, any sibling review class, a "
            "regenerated manifest with a different file_sha256, or any taxon "
            "in a later source release. It knowingly includes the "
            "autonym-only members NorTaxa 53057 (Crepidotus cesatii) and "
            "55054 (Psathyrella prona) and the Spilonema members 225848 and "
            "69418."
        ),
    }


def _dump(value, indent: int = 0, *, compact_records: bool = False) -> str:
    """JSON with the ledger's hand-written layout, byte-stable on re-render.

    A container of scalars is kept on one line when that fits in 70
    characters, which reproduces the hand-authored parts of the file exactly.
    Generated records are written one per line, so the diff for a regenerated
    manifest is one line per member.
    """
    if not isinstance(value, (dict, list)) or not value:
        return json.dumps(value, ensure_ascii=False)
    children = value.values() if isinstance(value, dict) else value
    inline = json.dumps(value, ensure_ascii=False)
    if len(inline) <= 70 and not any(
            isinstance(child, (dict, list)) for child in children):
        return inline
    pad = "  " * (indent + 1)
    if isinstance(value, dict):
        items = [
            f"{pad}{json.dumps(key, ensure_ascii=False)}: "
            f"{_dump(child, indent + 1, compact_records=key in ('mappings', 'supersessions'))}"
            for key, child in value.items()
        ]
        return "{\n" + ",\n".join(items) + "\n" + "  " * indent + "}"
    items = [
        pad + (json.dumps(child, ensure_ascii=False)
               if compact_records and "approved_manifest" in child
               else _dump(child, indent + 1))
        for child in value
    ]
    return "[\n" + ",\n".join(items) + "\n" + "  " * indent + "]"


def render() -> str:
    manifest = _load_manifest()
    members = _members(manifest)
    _check_registry(members)
    nortaxa_version = APPROVED_PINS["source_archives"]["nortaxa"][
        "source_release_id"].split(":")[1]

    document = json.loads(MANUAL_MAPPINGS.read_text(encoding="utf-8"))
    kept = [
        entry for entry in document.get("mappings") or []
        if (entry.get("approved_manifest") or {}).get("file_sha256")
        != APPROVED_FILE_SHA256
    ]
    member_usages = {str(m["nortaxa_taxon_id"]) for m in members}
    clash = sorted(
        entry["mapping_id"] for entry in kept
        if (entry.get("source_usage") or {}).get("source") == "nortaxa"
        and str((entry.get("source_usage") or {}).get("identifier"))
        in member_usages
    )
    if clash:
        raise GenerationError(
            f"existing records already map manifest members: {clash}")
    generated = [
        _record(m, nortaxa_version)
        for m in sorted(members, key=lambda m: int(m["nortaxa_taxon_id"]))
    ]
    # Stage 4P's Dyntaxa approvals and records, which its own generator keeps
    # after everything else, stay after this block, so the two generators
    # agree on the ledger's layout and each is idempotent over the other's
    # output.
    def later(entry: dict) -> bool:
        return (entry.get("source_usage") or {}).get("source") == "dyntaxa" \
            and bool(entry.get("approved_manifest"))
    others = [a for a in document.get("approved_manifests") or []
              if a.get("file_sha256") != APPROVED_FILE_SHA256]
    later_approvals = [a for a in others
                       if str(a.get("manifest_id") or "").startswith(
                           "taxonomy-v3-dyntaxa-")]
    approvals = [a for a in others if a not in later_approvals] \
        + [_approval(manifest)] + later_approvals
    out = {
        key: value for key, value in document.items()
        if key not in ("approved_manifests", "mappings")
    }
    out["approved_manifests"] = approvals
    out["mappings"] = [e for e in kept if not later(e)] + generated \
        + [e for e in kept if later(e)]
    verify_manifest_approvals(out, repo_root=_REPO)
    return _dump(out) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true",
                        help="fail if manual_mappings.yml is not current")
    args = parser.parse_args()
    try:
        text = render()
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
    MANUAL_MAPPINGS.write_text(text, encoding="utf-8")
    print(f"wrote {MANUAL_MAPPINGS.relative_to(_REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
