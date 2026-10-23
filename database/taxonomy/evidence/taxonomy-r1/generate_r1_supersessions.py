"""Render the taxonomy reconciliation R1 records of concept_supersessions.yml.

R1 is a CANDIDATE build. The owner (2026-10-23) authorised building and
testing a candidate release from three audited inputs; he did not approve a
release. Every approval and record written here therefore carries
``approval_scope: candidate_build_only`` and ``release_approval: pending``.
``review_status: approved`` is used only because the compiler applies nothing
else; it is not a release approval.

Inputs (audit branch ``audit/taxonomy-reconciliation``, directory
``docs/audits/2026-10-20-taxonomy-reconciliation/``), copied byte-identically
into ``manifests/source/`` and pinned by sha256 below:

* S1: ``A0-species`` (943) and ``A0-infraspecific`` (45) manifests.
* The 147 independently supported relationships: ``rel3-verified``.
* Decision package E, corrected per its Codex review: the E1 manifests (42)
  and the 4 E0 pairing corrections (listed in ``manifest_exclusions.csv``,
  paired with the corrected COL record). The three pairs that also differ in
  family are held out (no recorded rationale), which removes two members
  from ``E-E1-default-class-error``: that manifest is applied through a
  derived manifest listing exactly the remaining members.
* reference-holds.json: 18 rel3-verified pairs whose retiring id has live
  desktop or cloud observation references are held too (R1 rewrites no
  stored row), applied through a derived rel3-verified manifest.

No E2/E3 pair is used. Every record carries ``inherited_usages: names_only``
(policy P10). E0 pairs are one-to-one only after the documented correction,
so the manifest validator refuses them by design; they are written as
individually recorded supersessions without ``approved_manifest``.

Records generated here are prefixed ``taxonomy-r1-`` and placed after the
earlier hand-written record and before the taxonomy-v3 Stage-2 records, and
the R1 approvals before the Stage-2 approvals, so the Stage-2 generator's
rendering of the ledger is unchanged. ``--check`` verifies the ledger is
current; ``--import-from <audit dir>`` copies and verifies the source files.

    .venv/bin/python database/taxonomy/evidence/taxonomy-r1/generate_r1_supersessions.py
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import shutil
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parents[3]
_TAXONOMY = _REPO / "database" / "taxonomy"
for _path in (_TAXONOMY / "evidence" / "taxonomy-v3", _TAXONOMY / "scripts"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from bridge_emission import verify_supersession_manifest_approvals  # noqa: E402
from generate_stage1a_mappings import _dump  # noqa: E402

LEDGER = _TAXONOMY / "policies" / "concept_supersessions.yml"
REGISTRY = _TAXONOMY / "registry" / "canonical"
SOURCE_DIR = _HERE / "manifests" / "source"
DERIVED_DIR = _HERE / "manifests"
REL = "database/taxonomy/evidence/taxonomy-r1/manifests"
AUDIT = "docs/audits/2026-10-20-taxonomy-reconciliation"
AUDIT_REF = f"branch audit/taxonomy-reconciliation, {AUDIT}"

RECORD_PREFIX = "taxonomy-r1-"
V3_PREFIX = "taxonomy-v3-2-"
APPROVED_BY = "Sigmund Ås"
APPROVED_AT = "2026-10-23"
RECORD_TIMESTAMP = "2026-10-23T00:00:00Z"
APPROVAL_SCOPE = "candidate_build_only"
DECISION_REFERENCE = (
    "owner instruction 2026-10-23 (taxonomy reconciliation R1): build and test a "
    "candidate taxonomy release from the S1 proposal (988 pairs), the 147 "
    "independently supported relationships and the corrected E0/E1 decision "
    "package; exclude all E2/E3; do not install, deploy or promote. Approval "
    "scope candidate_build_only; release approval pending.")
SCOPE_NOTE = (
    "Authorises a candidate build from exactly the members enumerated in this "
    "file. It is not a release approval: installing, promoting or publishing a "
    "release built from it needs a separate owner decision. It does not cover "
    "any other partition, any E2/E3 pair, a regenerated manifest with a "
    "different file_sha256, or any taxon in a later source release.")

#: (file name, sha256, input, evidence_class, rationale) for each source file.
SOURCES = (
    ("A0-species.manifest.json",
     "62790e59dd7dc8acdcdc2a30099e085bc425f9d46a2ac1bff2424efcd9e4a9a9", "S1",
     f"{AUDIT}/manifests/validator-format/A0-species.manifest.json"),
    ("A0-infraspecific.manifest.json",
     "6706b6a67ee8c3f02ded7c82a7d142fc7dcc50f4e1c3608af975531750668668", "S1",
     f"{AUDIT}/manifests/validator-format/A0-infraspecific.manifest.json"),
    ("rel3-verified.manifest.json",
     "dc46d4a89f4a14d2a76f1b04b39eefcc7547ac6f46bdf0dd112c57b8a15e9acd", "V147",
     f"{AUDIT}/relationship-pass/v3/manifests/rel3-verified.manifest.json"),
    ("E-E1-class-concept-family-agrees.manifest.json",
     "946f873bf58c9037f58b771d209d31256c77de78bdb1fb30e12d92541a6d3f80", "E1",
     f"{AUDIT}/relationship-pass/decision-package-e/manifests/E-E1-class-concept-family-agrees.manifest.json"),
    ("E-E1-default-class-error.manifest.json",
     "2f463c83f39b74cbf5a25a41c997f9e6043411df6335e80f909138319b68ae95", "E1",
     f"{AUDIT}/relationship-pass/decision-package-e/manifests/E-E1-default-class-error.manifest.json"),
    ("E-E1-verified-author-alias-default-class-error.manifest.json",
     "f036dda8310cf8ced8bb1c4f43add3d1b67ce291f652ccfb7d6413ecd343cd33", "E1",
     f"{AUDIT}/relationship-pass/decision-package-e/manifests/E-E1-verified-author-alias-default-class-error.manifest.json"),
    ("E-E1-verified-author-alias.manifest.json",
     "e8e621d22b94f89ea9ffeda839c1b2f614801b376d23cd95ede18bde80d7c4ef", "E1",
     f"{AUDIT}/relationship-pass/decision-package-e/manifests/E-E1-verified-author-alias.manifest.json"),
    ("E0-manifest_exclusions.csv",
     "2c44e02112a326bbdf96b7d920d6a10c588e67c4d6365893264d364592ff3856", "E0",
     f"{AUDIT}/relationship-pass/decision-package-e/data/manifest_exclusions.csv"),
    ("E0-pairing_corrections.csv",
     "fe4b660a7e20109aa7110c0caf81b58ee5d7123ede84c8e9eccf262c09d396ab", "E0",
     f"{AUDIT}/relationship-pass/decision-package-e/data/pairing_corrections.csv"),
)

#: Pairs held out of R1, keyed by NorTaxa taxon id. The three family
#: differences come from decision package E's Codex review (finding 2): no
#: rationale is recorded for them, so the owner's default (hold) applies.
HELD_OUT = {
    "54854": ("E0", "Henningsomyces puber -> COL 3KV7Z: family differs "
              "(Schizophyllaceae vs Marasmiaceae); no recorded rationale"),
    "76921": ("E1", "Phyllobathelium epiphyllum -> COL 4H5B4: family differs "
              "(Phyllobatheliaceae vs Strigulaceae); no recorded rationale"),
    "226944": ("E1", "Sagiolechia monoseptata -> COL 9ZFFZ: family differs "
               "(Gomphillaceae vs Sagiolechiaceae); no recorded rationale"),
}

#: Pairs held because the retiring id has live desktop/cloud references.
HELD_OUT.update({
    nid: (entry["input"], entry["reason"]) for nid, entry in json.loads(
        (_HERE / "reference-holds.json").read_text(encoding="utf-8"))["holds"].items()})

INPUT_RATIONALE = {
    "S1": ("S1 proposal (audit revision 5): rule deterministic_name_rank_authorship_v1 "
           "partition A0 (same canonical name and rank, one same-rank twin on each "
           "side, NorTaxa valid and COL accepted, no blocking nomenclatural status, "
           "A0 authorship class, agreeing classification, no conflicting external id)."),
    "V147": ("relationship pass 3, partition rel3-verified: the relationship is "
             "supported by published nomenclatural evidence beyond name equality "
             "(see the member's 'reason' and 'provenance_class' columns)."),
    "E1": ("decision package E, group E1: every remaining difference is explained "
           "by a verified IPNI author alias, NorTaxa's default class for order "
           "Incertae sedis, or a class concept with the family agreeing; the "
           "package's contradiction audit found none."),
    "E0": ("decision package E, group E0: a documented pairing correction. The "
           "source candidate paired NorTaxa with a provisional or placeholder COL "
           "record; the corrected pair uses the unique accepted COL record, and "
           "nothing else differs. The pair is one-to-one only after the "
           "correction, so it is recorded individually, not through a manifest."),
}


class GenerationError(RuntimeError):
    pass


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode()


def import_sources(audit_dir: Path) -> None:
    """Copy the audit's source files byte-identically, verifying each pin."""
    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    for name, sha, _input, rel in SOURCES:
        src = audit_dir / rel
        if _sha(src) != sha:
            raise GenerationError(f"{src} does not hash to {sha}")
        shutil.copyfile(src, SOURCE_DIR / name)


def _source(name: str) -> Path:
    for source_name, sha, _input, _rel in SOURCES:
        if source_name == name:
            path = SOURCE_DIR / name
            if _sha(path) != sha:
                raise GenerationError(f"{path} does not hash to its pin {sha}")
            return path
    raise GenerationError(f"unknown source {name}")


def _members(manifest: dict) -> list[dict]:
    return [dict(zip(manifest["columns"], row)) for row in manifest["members"]]


def _derived(name: str, manifest: dict, held: set[str]) -> tuple[Path, dict]:
    """A manifest listing exactly the members R1 applies, citing its source."""
    position = manifest["columns"].index("nortaxa_taxon_id")
    kept = [row for row in manifest["members"] if str(row[position]) not in held]
    dropped = sorted(str(row[position]) for row in manifest["members"]
                     if str(row[position]) in held)
    doc = {key: value for key, value in manifest.items()
           if key not in ("members", "member_count", "members_sha256", "note")}
    doc.update({
        "members": kept,
        "member_count": len(kept),
        "members_sha256": hashlib.sha256(_canonical(
            {"columns": manifest["columns"], "members": kept})).hexdigest(),
        "derived_from": {"path": f"{REL}/source/{name}", "file_sha256": _sha(SOURCE_DIR / name),
                         "members_sha256": manifest["members_sha256"],
                         "member_count": manifest["member_count"]},
        "held_out": [{"nortaxa_taxon_id": nid, "reason": HELD_OUT[nid][1]} for nid in dropped],
        "note": ("R1 derived manifest: the source manifest's members minus the pairs "
                 "held out of R1. Members, columns and pins are copied unchanged. "
                 "Generated by database/taxonomy/evidence/taxonomy-r1/"
                 "generate_r1_supersessions.py."),
    })
    path = DERIVED_DIR / f"r1-{name}"
    return path, doc


def _record_id(member: dict) -> str:
    return (f"{RECORD_PREFIX}nortaxa-{member['nortaxa_taxon_id']}-superseded-by-col-"
            f"{member['col_usage_id']}")


def _record(member: dict, input_name: str, approval: dict | None,
            evidence: list[str]) -> dict:
    name = member.get("canonical_scientific_name") or member.get("scientific_name")
    record = {
        "supersession_id": _record_id(member),
        "superseded_sporely_taxon_id": int(member["nortaxa_sporely_taxon_id"]),
        "current_source_usage": {"source": "col_xr", "namespace": "col_usage_id",
                                 "identifier": str(member["col_usage_id"])},
        "relationship": "exact",
        "review_status": "approved",
        "reviewer": APPROVED_BY,
        "approval_scope": APPROVAL_SCOPE,
        "release_approval": "pending",
        "inherited_usages": "names_only",
        "rationale": (
            f"R1 candidate member ({input_name}). NorTaxa concept "
            f"{member['nortaxa_taxon_id']} (sporely_taxon_id "
            f"{member['nortaxa_sporely_taxon_id']}, '{name}' "
            f"{member.get('nortaxa_authorship') or ''}) and COL {member['col_usage_id']} "
            f"(sporely_taxon_id {member['col_sporely_taxon_id']}, "
            f"{member.get('col_authorship') or ''}). {INPUT_RATIONALE[input_name]} "
            "The NorTaxa concept is retired append-only onto the COL concept; only "
            "its own accepted usage becomes the reviewed bridge (P10), its other "
            "usages move as names. Approved for a candidate build only; release "
            "approval pending."),
        "evidence_references": evidence,
        "created_at": RECORD_TIMESTAMP,
        "updated_at": RECORD_TIMESTAMP,
        "source_release_range": {"first": "1.284", "last": "1.284"},
        "supersedes": None,
    }
    if approval is not None:
        record["approved_manifest"] = {
            "file_sha256": approval["file_sha256"],
            "member": {key: member[key] for key in (
                "nortaxa_taxon_id", "nortaxa_sporely_taxon_id",
                "col_usage_id", "col_sporely_taxon_id")},
        }
    return record


def _approval(path: Path, manifest: dict, input_name: str, source_ref: str,
              file_sha256: str | None = None) -> dict:
    rel = path.relative_to(_REPO).as_posix()
    return {
        "manifest_id": "taxonomy-r1-" + path.name.removesuffix(".manifest.json").removeprefix("r1-"),
        "path": rel,
        "file_sha256": file_sha256 or _sha(path),
        "members_sha256": manifest["members_sha256"],
        "member_count": manifest["member_count"],
        "evidence_class": manifest["evidence_class"],
        "pins": manifest["pins"],
        "approved_by": APPROVED_BY,
        "approved_at": APPROVED_AT,
        "approval_scope": APPROVAL_SCOPE,
        "release_approval": "pending",
        "inherited_usages": "names_only",
        "decision_reference": DECISION_REFERENCE,
        "source_reference": f"{AUDIT_REF}: {source_ref}",
        "rationale": f"R1 input {input_name}. {INPUT_RATIONALE[input_name]}",
        "scope_note": SCOPE_NOTE,
    }


def _check_registry(members: list[dict]) -> None:
    from identity_registry import IdentityRegistry
    registry = IdentityRegistry(REGISTRY)
    registry.load()
    for m in members:
        retired = registry.lookup("nortaxa", "nortaxa_taxon_id", str(m["nortaxa_taxon_id"]))
        current = registry.lookup("col_xr", "col_usage_id", str(m["col_usage_id"]))
        if retired is None or retired.kind != "anchor" \
                or retired.sporely_taxon_id != int(m["nortaxa_sporely_taxon_id"]):
            raise GenerationError(f"NorTaxa {m['nortaxa_taxon_id']} is not anchored at "
                                  f"{m['nortaxa_sporely_taxon_id']}")
        if current is None or current.kind != "anchor" \
                or current.sporely_taxon_id != int(m["col_sporely_taxon_id"]):
            raise GenerationError(f"COL {m['col_usage_id']} is not anchored at "
                                  f"{m['col_sporely_taxon_id']}")


def plan() -> tuple[list[tuple[Path, dict, bool]], list[dict], list[dict]]:
    """Manifests to write, approvals, records."""
    held = set(HELD_OUT)
    writes: list[tuple[Path, dict, bool]] = []
    approvals: list[dict] = []
    records: list[dict] = []
    for name, _sha256, input_name, rel in SOURCES:
        if not name.endswith(".manifest.json"):
            continue
        path = _source(name)
        manifest = json.loads(path.read_text(encoding="utf-8"))
        ids = {str(m["nortaxa_taxon_id"]) for m in _members(manifest)}
        derived = bool(ids & held)
        if derived:
            path, manifest = _derived(name, manifest, held)
            writes.append((path, manifest, True))
        members = _members(manifest)
        if not all(m["one_to_one"] is True for m in members):
            raise GenerationError(f"{name} holds a pair that is not one-to-one")
        if derived:
            # A derived manifest's sha is known only once it is rendered.
            approval = {"__derived__": path, "manifest": manifest, "input": input_name,
                        "rel": rel}
        else:
            approval = _approval(path, manifest, input_name, rel)
        approvals.append(approval)
        for m in members:
            records.append({"__member__": m, "__input__": input_name,
                            "__approval__": approval, "__rel__": rel})
    # E0: individually recorded pairing corrections.
    with (SOURCE_DIR / "E0-manifest_exclusions.csv").open(encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            if row["nortaxa_taxon_id"] in held:
                continue
            if row["group"] != "E0" or row["one_to_one_after_corrections"] != "True":
                raise GenerationError(f"unexpected E0 row {row}")
            member = {"nortaxa_taxon_id": row["nortaxa_taxon_id"],
                      "nortaxa_sporely_taxon_id": int(row["nortaxa_sporely_taxon_id"]),
                      "col_usage_id": row["col_usage_id"],
                      "col_sporely_taxon_id": int(row["col_sporely_taxon_id"]),
                      "canonical_scientific_name": row["scientific_name"],
                      "correction": row["reasons"]}
            records.append({"__member__": member, "__input__": "E0", "__approval__": None,
                            "__rel__": None})
    _check_registry([r["__member__"] for r in records])
    return writes, approvals, records


def _dumps_manifest(doc: dict) -> str:
    return json.dumps(doc, ensure_ascii=False, indent=1, sort_keys=True) + "\n"


def render() -> tuple[str, dict[Path, str]]:
    writes, approvals, pending = plan()
    files = {path: _dumps_manifest(doc) for path, doc, _ in writes}
    resolved: list[dict] = []
    for approval in approvals:
        if "__derived__" in approval:
            path = approval["__derived__"]
            sha = hashlib.sha256(files[path].encode("utf-8")).hexdigest()
            real = _approval(path, approval["manifest"], approval["input"], approval["rel"],
                             file_sha256=sha)
            real["derived_from"] = approval["manifest"]["derived_from"]
            real["held_out"] = approval["manifest"]["held_out"]
            approval.clear()
            approval.update(real)
        resolved.append(approval)
    exclusions_sha = _sha(SOURCE_DIR / "E0-manifest_exclusions.csv")
    corrections_sha = _sha(SOURCE_DIR / "E0-pairing_corrections.csv")
    records: list[dict] = []
    for item in pending:
        member, input_name, approval = item["__member__"], item["__input__"], item["__approval__"]
        if approval is not None:
            evidence = [f"{approval['path']}#nortaxa_taxon_id={member['nortaxa_taxon_id']} "
                        f"file_sha256={approval['file_sha256']}",
                        f"{AUDIT_REF}: {item['__rel__']}"]
        else:
            evidence = [
                f"{REL}/source/E0-manifest_exclusions.csv#nortaxa_taxon_id="
                f"{member['nortaxa_taxon_id']} sha256={exclusions_sha}",
                f"{REL}/source/E0-pairing_corrections.csv sha256={corrections_sha}: "
                f"{member['correction']}",
                f"{AUDIT_REF}: relationship-pass/decision-package-e/review-codex.md "
                "(finding 2: E0/E1 sound with corrections; family-differing pairs held)",
            ]
        records.append(_record(member, input_name, approval, evidence))
    records.sort(key=lambda r: r["superseded_sporely_taxon_id"])

    retiring = [r["superseded_sporely_taxon_id"] for r in records]
    if len(set(retiring)) != len(retiring):
        raise GenerationError("a concept is retired twice")
    document = json.loads(LEDGER.read_text(encoding="utf-8"))
    others = [e for e in document["supersessions"]
              if not str(e.get("supersession_id", "")).startswith(RECORD_PREFIX)]
    first_v3 = next((i for i, e in enumerate(others)
                     if str(e.get("supersession_id", "")).startswith(V3_PREFIX)), len(others))
    clash = sorted(e["supersession_id"] for e in others
                   if e.get("superseded_sporely_taxon_id") in set(retiring))
    if clash:
        raise GenerationError(f"existing records already retire R1 concepts: {clash[:5]}")
    r1_shas = {a["file_sha256"] for a in resolved}
    kept_approvals = [a for a in document.get("approved_manifests") or []
                      if not str(a.get("manifest_id", "")).startswith("taxonomy-r1-")]
    out = dict(document)
    out["schema"] = {**document["schema"], "r1_candidate_scope_rule": (
        "Records and approvals with approval_scope 'candidate_build_only' "
        "(taxonomy reconciliation R1) authorise a candidate build only; "
        "release_approval 'pending' means no release built from them may be "
        "installed, promoted or published until the owner approves it. They "
        "carry inherited_usages 'names_only' (mapping_policy.yml "
        "r1_candidate_amendments.inherited_usage_under_supersession, P10).")}
    out["approved_manifests"] = resolved + kept_approvals
    out["supersessions"] = others[:first_v3] + records + others[first_v3:]
    bound = verify_supersession_manifest_approvals(out, repo_root=_REPO, ) \
        if all(p.exists() and p.read_text(encoding="utf-8") == t for p, t in files.items()) \
        else None
    if bound is not None:
        r1_bound = [s for s, sha in bound.items() if sha in r1_shas]
        expected = sum(a["member_count"] for a in resolved)
        if len(r1_bound) != expected:
            raise GenerationError("not every R1 manifest member has one record")
    return _dump(out) + "\n", files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--import-from", type=Path, default=None,
                        help="root of an audit/taxonomy-reconciliation checkout to copy the "
                             "pinned source files from")
    args = parser.parse_args()
    try:
        if args.import_from is not None:
            import_sources(args.import_from)
        text, files = render()
    except GenerationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    stale = [p for p, t in files.items() if not p.exists() or p.read_text(encoding="utf-8") != t]
    current = LEDGER.read_text(encoding="utf-8")
    if args.check:
        if stale or current != text:
            print("R1 ledger records or derived manifests are not current", file=sys.stderr)
            return 1
        print("R1 ledger records are current")
        return 0
    for path, content in files.items():
        path.write_text(content, encoding="utf-8")
    if stale:
        text, files = render()  # re-render so the validator runs on the written manifests
    LEDGER.write_text(text, encoding="utf-8")
    print(f"wrote {LEDGER.relative_to(_REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
