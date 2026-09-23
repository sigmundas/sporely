#!/usr/bin/env python3
"""Grade cross-source associations by the sources' own synonymy assertions.

The compiler's automatic classifier matches a bridge concept to a backbone
concept on canonical name and rank, then strengthens the match with authorship,
status and kingdom agreement. `mapping_policy.continuity_rules` classifies that
as ``canonical_name_only`` — review required — and
``automatic_identity.requirements`` admits an automatic bridge only on
continuous source identity or *an authoritative explicit cross-reference*. A
name match, however strengthened, is not a cross-reference: neither source has
asserted that the two concepts are the same thing.

Some pairs do have such an assertion, published by the sources themselves. This
module finds it. For a bridge concept ``B`` and a backbone concept ``C`` it
grades the relationship using each source's own accepted/synonym structure:

``reciprocal_accepted_synonymy``
    ``B`` lists ``C``'s accepted name as one of its synonyms, **and** ``C``
    lists ``B``'s accepted name as one of its synonyms. Each source has
    independently published a statement placing the other's accepted name in
    its own concept. This is the strongest evidence available without shared
    machine identifiers, and it is the only class that speaks to the
    divergent-accepted-name case, where the two sources disagree about which
    name is correct.

``one_directional_accepted_synonymy``
    Only one side publishes the cross-reference. Still an explicit assertion,
    but unconfirmed by the other source.

``shared_synonymy``
    ``B`` and ``C`` place one or more of the *same* published names (matched on
    canonical name **and** authorship) under their respective accepted
    concepts, beyond either accepted name. Both sources independently assert
    that those names belong to their concept, so the two concepts are linked
    through a third published name rather than through the spelling of their
    accepted names. This is the class that covers the agreeing-name case,
    where there is no name divergence to key on.

``no_published_cross_reference``
    Neither source published anything connecting the two concepts. The
    association rests on the accepted-name match alone. Under the reviewed
    standard this is name-level enrichment and may not be emitted as identity.

What this module does NOT do: promote anything. Every output carries
``review_status = "needs_review"``. Evidence makes a review possible and
auditable; it does not replace it. Promotion happens only when a human records
the decision in ``policies/manual_mappings.yml`` or
``policies/concept_supersessions.yml``.

Name comparison is used to recognise *which published name* a source is
referring to — unavoidable, and how nomenclature works without shared
identifiers. The identity claim comes from the source's synonymy assertion, not
from the comparison.

Read-only over the pinned source archives. Writes only its own report.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import sys
import unicodedata
import zipfile
from pathlib import Path

# COL's NameUsage.tsv carries very large fields (citations, remarks).
csv.field_size_limit(10 ** 9)

EVIDENCE_RECIPROCAL = "reciprocal_accepted_synonymy"
EVIDENCE_ONE_DIRECTIONAL = "one_directional_accepted_synonymy"
EVIDENCE_SHARED_SYNONYMY = "shared_synonymy"
EVIDENCE_NONE = "no_published_cross_reference"

#: Evidence classes strong enough to justify putting an association in front of
#: a reviewer. Ordered strongest first.
REVIEWABLE = (EVIDENCE_RECIPROCAL, EVIDENCE_ONE_DIRECTIONAL,
              EVIDENCE_SHARED_SYNONYMY)

_ACCEPTED_NORTAXA = frozenset({"valid", "accepted", "provisionally accepted"})
_ACCEPTED_COL = frozenset({"accepted", "provisionally accepted"})


def _name_key(name: str, authorship: str) -> tuple[str, str]:
    """A published name's comparison key: canonical name plus authorship.

    The name is case-folded and whitespace-collapsed; the authorship keeps its
    capitalisation and punctuation, because publisher-year citations often
    differ only there and a looser compare would mask a genuine difference.
    """
    n = " ".join(
        part.casefold()
        for part in unicodedata.normalize("NFC", str(name or "").strip()).split()
    )
    a = " ".join(unicodedata.normalize("NFC", str(authorship or "").strip()).split())
    return (n, a)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


# ----------------------------------------------------------- source reading ---


def read_nortaxa(archive: Path, wanted: set[str]) -> dict[str, dict]:
    """Return ``{taxonID: {accepted, synonyms}}`` for the wanted concepts.

    ``accepted`` is the concept's own name key; ``synonyms`` is the set of name
    keys NorTaxa publishes under it.
    """
    concepts: dict[str, dict] = {
        taxon_id: {"accepted": None, "synonyms": set(), "scientific_name": "",
                   "authorship": ""}
        for taxon_id in wanted
    }
    with zipfile.ZipFile(archive) as bundle, bundle.open("taxon.txt") as handle:
        reader = csv.DictReader(
            io.TextIOWrapper(handle, "utf-8"), delimiter="\t")
        for row in reader:
            accepted_id = str(row.get("acceptedNameUsageID") or "")
            taxon_id = str(row.get("taxonID") or "")
            key = _name_key(row.get("scientificName", ""),
                            row.get("scientificNameAuthorship", ""))
            if taxon_id in concepts and \
                    str(row.get("taxonomicStatus") or "") in _ACCEPTED_NORTAXA:
                concepts[taxon_id]["accepted"] = key
                concepts[taxon_id]["scientific_name"] = row.get(
                    "scientificName", "")
                concepts[taxon_id]["authorship"] = row.get(
                    "scientificNameAuthorship", "")
            if accepted_id in concepts and accepted_id != taxon_id:
                concepts[accepted_id]["synonyms"].add(key)
    return concepts


def read_col(archive: Path, wanted: set[str]) -> dict[str, dict]:
    """Return ``{col:ID: {accepted, synonyms}}`` for the wanted concepts.

    COL publishes a synonym as a usage whose ``col:parentID`` is the accepted
    usage, so the synonym set is collected by parent.
    """
    concepts: dict[str, dict] = {
        usage_id: {"accepted": None, "synonyms": set(), "scientific_name": "",
                   "authorship": ""}
        for usage_id in wanted
    }
    with zipfile.ZipFile(archive) as bundle, bundle.open("NameUsage.tsv") as handle:
        stream = io.TextIOWrapper(handle, "utf-8")
        header = stream.readline().rstrip("\n").split("\t")
        index = {column: position for position, column in enumerate(header)}

        def field(parts: list[str], column: str) -> str:
            position = index.get(column)
            if position is None or position >= len(parts):
                return ""
            return parts[position]

        for line in stream:
            parts = line.rstrip("\n").split("\t")
            usage_id = field(parts, "col:ID")
            parent_id = field(parts, "col:parentID")
            if usage_id not in concepts and parent_id not in concepts:
                continue
            key = _name_key(field(parts, "col:scientificName"),
                            field(parts, "col:authorship"))
            status = field(parts, "col:status")
            if usage_id in concepts and status in _ACCEPTED_COL:
                concepts[usage_id]["accepted"] = key
                concepts[usage_id]["scientific_name"] = field(
                    parts, "col:scientificName")
                concepts[usage_id]["authorship"] = field(parts, "col:authorship")
            if parent_id in concepts and status not in _ACCEPTED_COL:
                concepts[parent_id]["synonyms"].add(key)
    return concepts


# ---------------------------------------------------------------- grading ---


def grade(bridge: dict | None, backbone: dict | None) -> dict:
    """Grade one association from the two concepts' published synonymy."""
    if not bridge or not backbone or \
            bridge.get("accepted") is None or backbone.get("accepted") is None:
        return {"evidence_class": EVIDENCE_NONE,
                "reason": "one or both concepts have no accepted usage in the "
                          "pinned source"}
    bridge_accepted = bridge["accepted"]
    backbone_accepted = backbone["accepted"]
    bridge_lists_backbone = backbone_accepted in bridge["synonyms"]
    backbone_lists_bridge = bridge_accepted in backbone["synonyms"]
    shared = sorted(
        (bridge["synonyms"] & backbone["synonyms"])
        - {bridge_accepted, backbone_accepted}
    )
    detail = {
        "bridge_accepted_name": f"{bridge['scientific_name']} "
                                f"{bridge['authorship']}".strip(),
        "backbone_accepted_name": f"{backbone['scientific_name']} "
                                  f"{backbone['authorship']}".strip(),
        "accepted_names_agree": bridge_accepted == backbone_accepted,
        "bridge_lists_backbone_accepted_name_as_synonym": bridge_lists_backbone,
        "backbone_lists_bridge_accepted_name_as_synonym": backbone_lists_bridge,
        "shared_synonym_count": len(shared),
        "shared_synonyms": [f"{name} {authorship}".strip()
                            for name, authorship in shared[:10]],
        "bridge_synonym_count": len(bridge["synonyms"]),
        "backbone_synonym_count": len(backbone["synonyms"]),
    }
    if bridge_lists_backbone and backbone_lists_bridge:
        evidence_class = EVIDENCE_RECIPROCAL
    elif bridge_lists_backbone or backbone_lists_bridge:
        evidence_class = EVIDENCE_ONE_DIRECTIONAL
    elif shared:
        evidence_class = EVIDENCE_SHARED_SYNONYMY
    else:
        evidence_class = EVIDENCE_NONE
    return {"evidence_class": evidence_class, **detail}


def grade_pairs(
    *,
    pairs: list[tuple[str, str]],
    nortaxa_archive: Path,
    col_archive: Path,
) -> dict:
    """Grade every ``(nortaxa_taxon_id, col_usage_id)`` pair."""
    nortaxa = read_nortaxa(nortaxa_archive, {p[0] for p in pairs})
    col = read_col(col_archive, {p[1] for p in pairs})
    graded = []
    counts: dict[str, int] = {}
    for nortaxa_id, col_id in pairs:
        result = grade(nortaxa.get(nortaxa_id), col.get(col_id))
        counts[result["evidence_class"]] = counts.get(
            result["evidence_class"], 0) + 1
        graded.append({
            "nortaxa_taxon_id": nortaxa_id,
            "col_usage_id": col_id,
            "review_status": "needs_review",
            **result,
        })
    graded.sort(key=lambda row: (row["evidence_class"],
                                 row["nortaxa_taxon_id"]))
    return {
        "source_archives": {
            "nortaxa": {"path": str(nortaxa_archive),
                        "sha256": _sha256(nortaxa_archive)},
            "col_xr": {"path": str(col_archive),
                       "sha256": _sha256(col_archive)},
        },
        "pair_count": len(pairs),
        "evidence_class_counts": dict(sorted(counts.items())),
        "reviewable_total": sum(
            counts.get(cls, 0) for cls in REVIEWABLE),
        "not_reviewable_total": counts.get(EVIDENCE_NONE, 0),
        "graded": graded,
    }


# -------------------------------------------------------------------- CLI ---


PRIMARY = Path("/Users/sigmundas/Documents/Code/sporely/sporely-py")
DEFAULT_NORTAXA = PRIMARY / "database/taxonomy/sources/nortaxa/1.284/archive.zip"
DEFAULT_COL = (PRIMARY /
               "database/taxonomy/sources/col_xr/2026-07-17-XR/archive.zip")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pairs", type=Path, required=True,
                        help="JSON list of [nortaxa_taxon_id, col_usage_id]")
    parser.add_argument("--nortaxa-archive", type=Path, default=DEFAULT_NORTAXA)
    parser.add_argument("--col-archive", type=Path, default=DEFAULT_COL)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    pairs = [
        (str(entry[0]), str(entry[1]))
        for entry in json.loads(args.pairs.read_text(encoding="utf-8"))
    ]
    report = grade_pairs(
        pairs=pairs,
        nortaxa_archive=args.nortaxa_archive,
        col_archive=args.col_archive,
    )
    args.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary = {key: value for key, value in report.items() if key != "graded"}
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
