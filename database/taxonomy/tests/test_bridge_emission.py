"""Reviewed cross-source bridge emission (taxonomy-v2 closeout Stage 3).

A cross-source alias binding carries the bridge source's own identity for a
concept the backbone anchors. These tests pin the rule that decides which of
those bindings may be published as a resolvable
``(source, namespace, external_id)`` identity, and prove the decision is made
by graded evidence rather than by namespace membership or by hard-coded taxa.

The fixture deliberately mixes four populations in one release, because the
mechanism is only correct if it separates them in a single pass:

``53482`` / ``Entoloma conferendum``
    The agreeing-name regression. Both sources publish the same accepted name
    and authorship, so the automatic classifier binds them — and that binding
    alone must NOT publish identity, because a name match is not a
    cross-reference. An approved ``manual_mappings.yml`` record supplies the
    reviewed relationship, and then it resolves.

``52369`` / ``Pholiotina rugosa``
    The divergent-name regression, which is also a *merge*: the sources
    disagree about the accepted name, so the bridge source was allocated its
    own concept. An approved ``concept_supersessions.yml`` record selects the
    backbone concept as current without rewriting the append-only registry.

``Inocybe ambigua``
    The control. Two equal-looking names that must stay distinct: the backbone
    holds a homonym pair, so the name/rank bucket is ambiguous and nothing may
    be emitted, no matter how the names look.

``Nolanea conferenda``
    The no-regression case. An intra-source synonym usage that rides the
    accepted concept's binding. It must keep working as a searchable name
    while never being published as concept identity.

A fifth population — a binding admitted only because authorship was absent on
one side — is covered by
:func:`test_missing_authorship_binding_is_bound_but_not_emitted`.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

_TAXONOMY = Path(__file__).resolve().parents[1]
_SCRIPTS = _TAXONOMY / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from bridge_emission import (  # noqa: E402
    UNCLASSIFIED_REASON,
    BridgeEmissionError,
    BridgeEmissionPolicy,
)
from build_sqlite_candidate import build_candidate  # noqa: E402
from compile_release import CompilerError, compile_release  # noqa: E402
from identity_registry import IdentityRegistry  # noqa: E402
from cross_source_mapping import (  # noqa: E402
    EVIDENCE_CLASS_CROSS_SOURCE_MISSING_AUTHORSHIP,
    EVIDENCE_CLASS_CROSS_SOURCE_STRICT,
    REASON_CONSERVATIVE_EXACT,
    REASON_MISSING_AUTHORSHIP,
    evidence_class_for_reason,
)

from test_compile_release import (  # noqa: E402
    _write_manual_mappings,
    _write_normalized_source,
)

_POLICY_PATH = _TAXONOMY / "policies" / "mapping_policy.yml"

_COL_RELEASE = {"version": "2026-07-17-XR", "issued_date": "2026-07-17"}
_NORTAXA_RELEASE = {"version": "1.284", "issued_date": "2026-07-17"}


# --------------------------------------------------------------- fixture ---


def _resolvable_parents(source_dir: Path, source_code: str) -> Path:
    """Point parent references at the namespace parent resolution looks up.

    ``build_sqlite_candidate._resolve_parent_sporely_id`` keys the parent
    lookup on ``(source, parent_ref.namespace, parent_ref.value)`` against the
    *taxon_id* index, but the shared test helper writes parent references under
    a ``*_parent_name_usage_id`` namespace. Left alone, every fixture concept
    gets a NULL parent, becomes an orphan, and falls out of the cloud export's
    descendant walk — which would quietly make an export test pass on an empty
    scope. Rewrite the namespace so the fixture has a real hierarchy.
    """
    taxa_path = source_dir / "taxa.jsonl"
    lines = []
    for raw in taxa_path.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        record = json.loads(raw)
        parent = record.get("parent_name_usage_id")
        if parent:
            parent["namespace"] = f"{source_code}_taxon_id"
        lines.append(json.dumps(record, ensure_ascii=False, sort_keys=True))
    taxa_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return source_dir


def _col_source(root: Path) -> Path:
    """Backbone. Authorship strings are the real COL values for these taxa."""
    return _resolvable_parents(_write_normalized_source(
        root / "col_xr",
        source_code="col_xr",
        source_release=_COL_RELEASE,
        identifier_namespace_prefix="COL:",
        rows=[
            {"core_row_id": "COL-K", "taxon_id": "COL-K",
             "scientific_name": "Fungi", "rank": "kingdom"},
            # Agreeing-name case: same name AND same authorship as NorTaxa.
            {"core_row_id": "39ZCL", "taxon_id": "39ZCL", "parent": "COL-K",
             "parent_resolution": "resolved",
             "scientific_name": "Entoloma conferendum",
             "authorship": "(Britzelm.) Noordel."},
            # Divergent-name case: the backbone's accepted name differs from
            # the bridge source's accepted name for the same species.
            {"core_row_id": "5ZT3G", "taxon_id": "5ZT3G", "parent": "COL-K",
             "parent_resolution": "resolved",
             "scientific_name": "Conocybe rugosa",
             "authorship": "(Peck) Watling"},
            # Control: a homonym pair. Same canonical name and rank, different
            # authorship, two distinct concepts that must never be merged.
            {"core_row_id": "HOM-1", "taxon_id": "HOM-1", "parent": "COL-K",
             "parent_resolution": "resolved",
             "scientific_name": "Inocybe ambigua", "authorship": "Sacc."},
            {"core_row_id": "HOM-2", "taxon_id": "HOM-2", "parent": "COL-K",
             "parent_resolution": "resolved",
             "scientific_name": "Inocybe ambigua", "authorship": "Bres."},
            # Missing-authorship case: the backbone publishes no authorship.
            # The fallback rule compares classification chains, so both sides
            # declare a family here.
            {"core_row_id": "NOAUT", "taxon_id": "NOAUT", "parent": "COL-K",
             "parent_resolution": "resolved",
             "scientific_name": "Mycena absentia", "authorship": "",
             "classification": {"family": "Mycenaceae"}},
        ],
    ), "col_xr")


def _nortaxa_source(root: Path) -> Path:
    """Bridge source, with a vernacular extension so enrichment is observable."""
    source_dir = _resolvable_parents(_write_normalized_source(
        root / "nortaxa",
        source_code="nortaxa",
        source_release=_NORTAXA_RELEASE,
        identifier_namespace_prefix="NBIC:",
        rows=[
            {"core_row_id": "row-K", "taxon_id": "9999",
             "scientific_name": "Fungi", "rank": "kingdom"},
            # Agreeing names -> strict rule -> eligible.
            {"core_row_id": "row-53482", "taxon_id": "53482",
             "parent": "9999", "parent_resolution": "resolved",
             "scientific_name": "Entoloma conferendum",
             "authorship": "(Britzelm.) Noordel.", "status": "valid"},
            # Intra-source synonym of the above. Must stay searchable but is
            # never concept identity.
            {"core_row_id": "row-59796", "taxon_id": "59796",
             "accepted": "53482", "parent": "9999",
             "parent_resolution": "resolved",
             "scientific_name": "Nolanea conferenda",
             "authorship": "(Britzelm.) Sacc.", "status": "synonym"},
            # Divergent names -> no backbone match -> own anchor.
            {"core_row_id": "row-52369", "taxon_id": "52369",
             "parent": "9999", "parent_resolution": "resolved",
             "scientific_name": "Pholiotina rugosa",
             "authorship": "(Peck) Singer", "status": "valid"},
            # Control: matches the homonym bucket, so it stays separate.
            {"core_row_id": "row-hom", "taxon_id": "70001",
             "parent": "9999", "parent_resolution": "resolved",
             "scientific_name": "Inocybe ambigua",
             "authorship": "Sacc.", "status": "valid"},
            # Authorship present here, absent on the backbone -> fallback.
            {"core_row_id": "row-noaut", "taxon_id": "70002",
             "parent": "9999", "parent_resolution": "resolved",
             "scientific_name": "Mycena absentia",
             "authorship": "Fr.", "status": "valid",
             "classification": {"family": "Mycenaceae"}},
        ],
    ), "nortaxa")
    vernaculars = [
        # The real NorTaxa names for 53482, including non-preferred variants.
        ("row-53482", "nb", "stjernesporet rødspore", True),
        ("row-53482", "nb", "stjernesporet rødskivesopp", False),
        ("row-53482", "nn", "stjernespora raudspore", True),
        ("row-53482", "nn", "stjernespora raudskivesopp", False),
        # 52369's Norwegian name, which rides its own anchor.
        ("row-52369", "nb", "slank ringkjeglesopp", True),
    ]
    with (source_dir / "vernacular.jsonl").open("w", encoding="utf-8") as handle:
        for core_row_id, language, name, preferred in vernaculars:
            handle.write(json.dumps({
                "source_code": "nortaxa",
                "source_release": _NORTAXA_RELEASE,
                "core_row_id": {"value": core_row_id,
                                "namespace": "nortaxa_dwc_id"},
                "vernacular_name": name,
                "language": language,
                "is_preferred": preferred,
            }, ensure_ascii=False, sort_keys=True) + "\n")
    report_path = source_dir / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["record_counts"]["VernacularName"] = len(vernaculars)
    report["outputs"]["vernacular"] = "vernacular.jsonl"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return source_dir


def _compile(tmp_path: Path, *, manual: list[dict] | None = None,
             release_id: str = "tax-2026.09.23-01") -> Path:
    root = tmp_path / "sources"
    col = _col_source(root)
    nortaxa = _nortaxa_source(root)
    out = tmp_path / "release"
    compile_release(
        normalized_source_dirs=[col, nortaxa],
        manual_mappings_path=_write_manual_mappings(
            tmp_path / "manual.yml", manual or []),
        mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "registry.jsonl",
        output_dir=out,
        release_id=release_id,
    )
    return out


def _usages(release_dir: Path) -> dict[tuple[str, str], dict]:
    out: dict[tuple[str, str], dict] = {}
    with (release_dir / "source_usages.jsonl").open(encoding="utf-8") as handle:
        for raw in handle:
            row = json.loads(raw)
            out[(row["source_code"], row["source_usage"]["identifier"])] = row
    return out


def _candidate(tmp_path: Path, release_dir: Path,
               name: str = "candidate.sqlite3") -> tuple[sqlite3.Connection, dict]:
    db_path = tmp_path / name
    summary = build_candidate(
        release_dir=release_dir,
        registry_path=tmp_path / "registry.jsonl",
        output_db=db_path,
    )
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn, summary


def _resolve(conn: sqlite3.Connection, source: str, namespace: str,
             external_id: str) -> list[int]:
    """Resolve exactly the way the desktop backfill resolves.

    Mirrors ``database/migrate_observations_sporely_id.
    _resolve_namespaced_external_id``: the authoritative namespaced table only,
    keyed on the full tuple, returning every match so ambiguity is visible
    rather than collapsed by a ``LIMIT 1``.
    """
    return [int(r["taxon_id"]) for r in conn.execute(
        "SELECT DISTINCT taxon_id FROM taxon_external_id_text_min "
        "WHERE source_system = ? AND namespace = ? AND external_id = ?",
        (source, namespace, external_id),
    )]


# ------------------------------------------------------- the policy itself ---


def test_no_automatic_class_is_eligible() -> None:
    """Only reviewed relationships may be published as identity.

    `automatic_identity.requirements` admits an automatic bridge on continuous
    source identity or an authoritative explicit cross-reference. Every
    classifier in this repository derives its match from a canonical-name and
    rank lookup, which `continuity_rules.canonical_name_only` sends to review.
    Strengthening that match with authorship does not turn it into a
    cross-reference, and declaring the rule reviewed in the policy file would
    not supply the relationship evidence it lacks. This test exists so no
    future change can quietly re-admit an automatic class.
    """
    policy = BridgeEmissionPolicy.load(_POLICY_PATH)
    assert policy.eligible == frozenset({
        "manual_approved_exact", "reviewed_supersession"})
    for evidence_class in (EVIDENCE_CLASS_CROSS_SOURCE_STRICT,
                           EVIDENCE_CLASS_CROSS_SOURCE_MISSING_AUTHORSHIP,
                           "intra_source_synonym", ""):
        assert not policy.is_eligible(evidence_class)
        # Every refusal is explained, so a coverage audit can account for it.
        assert policy.rejection_reason(evidence_class) != UNCLASSIFIED_REASON
        assert policy.rejection_reason(evidence_class).strip()


def test_shipped_reviewed_records_are_not_yet_approved() -> None:
    """The two regression records await a human decision.

    Both ledger entries carry the assembled cross-reference evidence but
    `review_status = needs_review`, so the compiler ignores them. This test
    pins that state deliberately: it fails the moment someone approves a
    record, which is the point at which the production candidate must be
    rebuilt and re-verified. It is not asserting that the records should stay
    unapproved forever.
    """
    mappings = json.loads(
        (_TAXONOMY / "policies" / "manual_mappings.yml").read_text(
            encoding="utf-8"))
    supersessions = json.loads(
        (_TAXONOMY / "policies" / "concept_supersessions.yml").read_text(
            encoding="utf-8"))
    entries = mappings["mappings"] + supersessions["supersessions"]
    assert entries, "the regression records must be present to be reviewable"
    for entry in entries:
        assert entry["review_status"] == "needs_review"
        assert not entry["reviewer"]
        # Evidence must be citable, or the review has nothing to stand on.
        assert entry["evidence_references"]
        assert entry["rationale"].strip()


def test_unknown_evidence_class_fails_closed() -> None:
    policy = BridgeEmissionPolicy.load(_POLICY_PATH)
    assert not policy.is_eligible("some_rule_invented_later")
    assert policy.rejection_reason("some_rule_invented_later") == \
        UNCLASSIFIED_REASON


def test_evidence_class_vocabulary_matches_the_classifier() -> None:
    assert evidence_class_for_reason(REASON_CONSERVATIVE_EXACT) == \
        EVIDENCE_CLASS_CROSS_SOURCE_STRICT
    assert evidence_class_for_reason(REASON_MISSING_AUTHORSHIP) == \
        EVIDENCE_CLASS_CROSS_SOURCE_MISSING_AUTHORSHIP
    # An unrecognised reason must not inherit a graded class.
    assert evidence_class_for_reason("something_else") == ""


def test_policy_without_the_block_is_refused(tmp_path: Path) -> None:
    """A projection may not guess the standard when it is absent."""
    path = tmp_path / "policy.yml"
    path.write_text(json.dumps({"relationships": ["exact"]}), encoding="utf-8")
    with pytest.raises(BridgeEmissionError, match="authoritative_bridge_emission"):
        BridgeEmissionPolicy.load(path)


def test_policy_with_a_class_in_both_lists_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "policy.yml"
    path.write_text(json.dumps({"authoritative_bridge_emission": {
        "eligible": [{"evidence_class": "x"}],
        "rejected": [{"evidence_class": "x", "reason": "no"}],
    }}), encoding="utf-8")
    with pytest.raises(BridgeEmissionError, match="ambiguous"):
        BridgeEmissionPolicy.load(path)


def test_rejected_class_without_a_reason_is_refused(tmp_path: Path) -> None:
    """An unexplained refusal would leave a hole in the coverage audit."""
    path = tmp_path / "policy.yml"
    path.write_text(json.dumps({"authoritative_bridge_emission": {
        "eligible": [{"evidence_class": "x"}],
        "rejected": [{"evidence_class": "y"}],
    }}), encoding="utf-8")
    with pytest.raises(BridgeEmissionError, match="needs a 'reason'"):
        BridgeEmissionPolicy.load(path)


# ------------------------------------------------- the agreeing-name case ---


def test_derived_association_alone_does_not_publish_identity(
    tmp_path: Path,
) -> None:
    """53482 is derived and bound, and that is not sufficient on its own.

    This is the distinction the stage turns on. The compiler matched the two
    concepts and used the binding to attach NorTaxa's Norwegian names, so the
    association is real. But it was produced by a name-and-authorship match,
    not by a cross-reference either source published, so it may not be
    republished as concept identity without a reviewed decision.
    """
    release = _compile(tmp_path)
    usage = _usages(release)[("nortaxa", "53482")]
    assert usage["identity_binding"] == "alias"
    assert usage["bridge_evidence_class"] == EVIDENCE_CLASS_CROSS_SOURCE_STRICT

    conn, summary = _candidate(tmp_path, release)
    assert _resolve(conn, "nortaxa", "nortaxa_taxon_id", "53482") == []
    rejected = summary["authoritative_bridge_emission"][
        "rejected_by_evidence_class_and_reason"]
    assert any(key.startswith(EVIDENCE_CLASS_CROSS_SOURCE_STRICT + "|")
               for key in rejected)
    # The enrichment it carried is untouched by the refusal.
    host = int(conn.execute(
        "SELECT taxon_id FROM taxon_min "
        "WHERE canonical_scientific_name = 'Entoloma conferendum'"
    ).fetchone()[0])
    assert conn.execute(
        "SELECT COUNT(*) FROM vernacular_min WHERE taxon_id = ?",
        (host,)).fetchone()[0] == 4
    conn.close()


_REVIEWED_53482 = {
    "mapping_id": "nortaxa-53482-to-col-39ZCL",
    "source_usage": {"source": "nortaxa", "namespace": "nortaxa_taxon_id",
                     "identifier": "53482"},
    "target": {"source_usage": {"source": "col_xr",
                                "namespace": "col_xr_taxon_id",
                                "identifier": "39ZCL"}},
    "relationship": "exact",
    "review_status": "approved",
}


def test_reviewed_agreeing_name_bridge_resolves_to_the_backbone_concept(
    tmp_path: Path,
) -> None:
    """The 53482 regression, once the reviewed relationship exists."""
    release = _compile(tmp_path, manual=[_REVIEWED_53482])
    usage = _usages(release)[("nortaxa", "53482")]
    assert usage["identity_binding"] == "alias"
    assert usage["bridge_evidence_class"] == "manual_approved_exact"

    conn, summary = _candidate(tmp_path, release)
    host = [int(r["taxon_id"]) for r in conn.execute(
        "SELECT taxon_id FROM taxon_external_id_text_min "
        "WHERE source_system='col_xr' AND external_id='39ZCL'")]
    assert len(host) == 1

    assert _resolve(conn, "nortaxa", "nortaxa_taxon_id", "53482") == host

    row = conn.execute(
        "SELECT * FROM taxon_external_id_text_min WHERE source_system='nortaxa' "
        "AND namespace='nortaxa_taxon_id' AND external_id='53482'").fetchone()
    # Enough metadata to explain which source and standard matched.
    assert row["note"] == "authoritative_bridge:manual_approved_exact"
    assert row["external_name"] == "Entoloma conferendum"
    assert row["id_role"] == "accepted"
    # The bridge never outranks the backbone's own identifier.
    assert row["is_preferred"] == 0
    preferred = conn.execute(
        "SELECT external_id FROM taxon_external_id_text_min "
        "WHERE taxon_id = ? AND is_preferred = 1", (host[0],)).fetchall()
    assert [r["external_id"] for r in preferred] == ["39ZCL"]

    assert summary["authoritative_bridge_emission"]["emitted_by_evidence_class"] \
        == {"manual_approved_exact": 1}
    conn.close()


def test_agreeing_name_vernaculars_and_search_survive(tmp_path: Path) -> None:
    """Vernacular enrichment and both name treatments stay on the concept."""
    conn, _ = _candidate(tmp_path, _compile(tmp_path, manual=[_REVIEWED_53482]))
    host = _resolve(conn, "nortaxa", "nortaxa_taxon_id", "53482")[0]

    names = {(r["vernacular_name"], r["language_code"], r["is_preferred_name"])
             for r in conn.execute(
                 "SELECT * FROM vernacular_min WHERE taxon_id = ?", (host,))}
    assert names == {
        ("stjernesporet rødspore", "nb", 1),
        ("stjernesporet rødskivesopp", "nb", 0),
        ("stjernespora raudspore", "nn", 1),
        ("stjernespora raudskivesopp", "nn", 0),
    }

    # The accepted name still finds the concept, and the intra-source synonym
    # remains searchable — the no-regression case.
    searchable = {(r["scientific_name"], r["is_preferred_name"])
                  for r in conn.execute(
                      "SELECT * FROM scientific_name_min WHERE taxon_id = ?",
                      (host,))}
    assert ("Entoloma conferendum", 1) in searchable
    assert ("Nolanea conferenda", 0) in searchable
    conn.close()


def test_intra_source_synonym_is_searchable_but_not_identity(
    tmp_path: Path,
) -> None:
    """A synonym usage must not become a resolvable concept identifier."""
    release = _compile(tmp_path)
    usage = _usages(release)[("nortaxa", "59796")]
    assert usage["alias_reason"] == "synonym_of_accepted"
    assert usage["bridge_evidence_class"] == "intra_source_synonym"

    conn, summary = _candidate(tmp_path, release)
    assert _resolve(conn, "nortaxa", "nortaxa_taxon_id", "59796") == []
    rejected = summary["authoritative_bridge_emission"][
        "rejected_by_evidence_class_and_reason"]
    assert any(key.startswith("intra_source_synonym|") for key in rejected)
    conn.close()


# --------------------------------------------- the missing-authorship tier ---


def test_missing_authorship_binding_is_bound_but_not_emitted(
    tmp_path: Path,
) -> None:
    """The weaker tier keeps enriching, and stays out of identity.

    This is the distinction the stage turns on: the binding is real enough to
    carry names onto the concept, and not strong enough to publish as the
    concept's identity, because authorship was absent on one side so the
    nomenclatural act is unpinned.
    """
    release = _compile(tmp_path)
    usage = _usages(release)[("nortaxa", "70002")]
    assert usage["identity_binding"] == "alias"
    assert usage["bridge_evidence_class"] == \
        EVIDENCE_CLASS_CROSS_SOURCE_MISSING_AUTHORSHIP

    conn, summary = _candidate(tmp_path, release)
    assert _resolve(conn, "nortaxa", "nortaxa_taxon_id", "70002") == []
    # Counted and explained, not silently dropped.
    rejected = summary["authoritative_bridge_emission"][
        "rejected_by_evidence_class_and_reason"]
    matching = [key for key in rejected
                if key.startswith(
                    EVIDENCE_CLASS_CROSS_SOURCE_MISSING_AUTHORSHIP + "|")]
    assert len(matching) == 1
    assert "authorship" in matching[0]
    conn.close()


# ------------------------------------------------------------- the control ---


def test_homonym_bucket_emits_nothing_and_stays_distinct(
    tmp_path: Path,
) -> None:
    """Two equal-looking names must remain two concepts.

    ``Inocybe ambigua`` is a homonym pair in the backbone. Even though the
    bridge source's name and authorship match one of them exactly, the
    ambiguity guard fires before any evidence is graded, so no identifier is
    published and no concept is merged.
    """
    release = _compile(tmp_path)
    usage = _usages(release)[("nortaxa", "70001")]
    assert usage["identity_binding"] == "anchor"
    assert usage["bridge_evidence_class"] == ""

    conn, _ = _candidate(tmp_path, release)
    homonyms = [int(r["taxon_id"]) for r in conn.execute(
        "SELECT taxon_id FROM taxon_min "
        "WHERE canonical_scientific_name = 'Inocybe ambigua'")]
    # Both backbone concepts plus the bridge source's own concept: three
    # distinct identities, none merged on the strength of a matching name.
    assert len(homonyms) == len(set(homonyms)) == 3
    assert _resolve(conn, "nortaxa", "nortaxa_taxon_id", "70001") == []
    conn.close()


def test_every_emitted_tuple_resolves_to_exactly_one_concept(
    tmp_path: Path,
) -> None:
    """Ambiguity control: emission may never create a second answer.

    The registry holds one Sporely id per ``(source, namespace, identifier)``,
    so publishing a binding cannot introduce a resolution that the backfill
    would have to refuse. This asserts that over every emitted row rather than
    trusting it.
    """
    conn, _ = _candidate(tmp_path, _compile(tmp_path))
    ambiguous = conn.execute(
        "SELECT source_system, namespace, external_id, COUNT(DISTINCT taxon_id) n "
        "FROM taxon_external_id_text_min "
        "GROUP BY 1, 2, 3 HAVING n > 1").fetchall()
    assert [dict(r) for r in ambiguous] == []
    conn.close()


# ---------------------------------------------------- the divergent-name case ---


def test_divergent_names_are_not_merged_automatically(tmp_path: Path) -> None:
    """No automatic rule may bridge a genuine accepted-name disagreement."""
    release = _compile(tmp_path)
    usage = _usages(release)[("nortaxa", "52369")]
    # It is anchored as its own concept, not aliased onto the backbone.
    assert usage["identity_binding"] == "anchor"
    assert usage["bridge_evidence_class"] == ""

    conn, _ = _candidate(tmp_path, release)
    own = int(conn.execute(
        "SELECT taxon_id FROM taxon_min "
        "WHERE canonical_scientific_name = 'Pholiotina rugosa'").fetchone()[0])
    backbone = int(conn.execute(
        "SELECT taxon_id FROM taxon_min "
        "WHERE canonical_scientific_name = 'Conocybe rugosa'").fetchone()[0])
    assert own != backbone

    # Nothing bridges the two, so nothing resolves onto the backbone concept.
    assert _resolve(conn, "nortaxa", "nortaxa_taxon_id", "52369") == []
    # Worth recording: a bridge-source *anchor* has no authoritative
    # namespaced row in the candidate at all. Its identifier lives only in the
    # namespace-lost integer table, and is reconstructed at export time by
    # `cloud_export.emit_taxon_external_id_authoritative`'s derived
    # `norwegian_taxon_id` branch. Unifying that is outside this change.
    assert conn.execute(
        "SELECT COUNT(*) FROM taxon_external_id_min "
        "WHERE taxon_id = ? AND external_id = 52369", (own,),
    ).fetchone()[0] == 1
    conn.close()


def test_approved_manual_bridge_emits_on_a_fresh_registry(
    tmp_path: Path,
) -> None:
    """The reviewed manual tier works, and is the only route for divergence.

    With no prior allocation for ``52369``, an approved manual mapping binds
    it onto the backbone concept, its Norwegian name follows the identity onto
    the retained concept, and its own accepted name becomes searchable there —
    so the two sources' treatments are both visible without either source's
    preferred name being overwritten.
    """
    release = _compile(tmp_path, manual=[{
        "mapping_id": "nortaxa-52369-to-col-5ZT3G",
        "source_usage": {"source": "nortaxa",
                         "namespace": "nortaxa_taxon_id",
                         "identifier": "52369"},
        "target": {"source_usage": {"source": "col_xr",
                                    "namespace": "col_xr_taxon_id",
                                    "identifier": "5ZT3G"}},
        "relationship": "exact",
        "review_status": "approved",
    }])
    usage = _usages(release)[("nortaxa", "52369")]
    assert usage["identity_binding"] == "alias"
    assert usage["bridge_evidence_class"] == "manual_approved_exact"

    conn, _ = _candidate(tmp_path, release)
    host = [int(r["taxon_id"]) for r in conn.execute(
        "SELECT taxon_id FROM taxon_external_id_text_min "
        "WHERE source_system='col_xr' AND external_id='5ZT3G'")]
    assert _resolve(conn, "nortaxa", "nortaxa_taxon_id", "52369") == host

    row = conn.execute(
        "SELECT * FROM taxon_external_id_text_min WHERE source_system='nortaxa' "
        "AND namespace='nortaxa_taxon_id' AND external_id='52369'").fetchone()
    assert row["note"] == "authoritative_bridge:manual_approved_exact"
    # The bridge source's accepted-name treatment is preserved, not flattened.
    assert row["external_name"] == "Pholiotina rugosa"

    # Both treatments searchable; the backbone keeps its preferred name.
    names = {(r["scientific_name"], r["is_preferred_name"], r["source"])
             for r in conn.execute(
                 "SELECT * FROM scientific_name_min WHERE taxon_id = ?",
                 (host[0],))}
    assert ("Conocybe rugosa", 1, "col_xr") in names
    assert ("Pholiotina rugosa", 0, "nortaxa") in names

    # Vernacular enrichment crosses with the identity.
    vern = {r["vernacular_name"] for r in conn.execute(
        "SELECT * FROM vernacular_min WHERE taxon_id = ?", (host[0],))}
    assert "slank ringkjeglesopp" in vern

    # And no duplicate concept was created for the taxon both sources describe.
    assert conn.execute(
        "SELECT COUNT(*) FROM taxon_min "
        "WHERE canonical_scientific_name IN "
        "('Conocybe rugosa', 'Pholiotina rugosa')").fetchone()[0] == 1
    conn.close()


def _write_supersessions(path: Path, entries: list[dict]) -> Path:
    path.write_text(
        json.dumps({
            "format": "sporely-taxonomy-concept-supersessions-v1",
            "schema": {},
            "supersessions": entries,
        }, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def test_reviewed_supersession_resolves_the_divergent_name_case(
    tmp_path: Path,
) -> None:
    """The 52369 regression against identity that is already allocated.

    First compile allocates 52369 its own concept, exactly as the production
    registry did in tax-2026.07.29-01. A reviewed supersession then selects the
    backbone concept as current, without rewriting that allocation.
    """
    first = _compile(tmp_path, release_id="tax-2026.09.23-01")
    own_id = _usages(first)[("nortaxa", "52369")]["sporely_taxon_id"]
    backbone_id = _usages(first)[("col_xr", "5ZT3G")]["sporely_taxon_id"]
    assert own_id != backbone_id

    release = tmp_path / "release2"
    compile_release(
        normalized_source_dirs=[tmp_path / "sources" / "col_xr",
                                tmp_path / "sources" / "nortaxa"],
        manual_mappings_path=_write_manual_mappings(tmp_path / "m2.yml", []),
        mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "registry.jsonl",
        output_dir=release,
        release_id="tax-2026.09.23-02",
        concept_supersessions_path=_write_supersessions(
            tmp_path / "supersessions.yml", [{
                "supersession_id": "supersede-52369",
                "superseded_sporely_taxon_id": own_id,
                "current_source_usage": {"source": "col_xr",
                                         "namespace": "col_xr_taxon_id",
                                         "identifier": "5ZT3G"},
                "relationship": "exact",
                "review_status": "approved",
            }]),
    )

    # The registry still records the original allocation verbatim.
    registry = IdentityRegistry(tmp_path / "registry.jsonl")
    registry.load()
    assert registry.lookup(
        "nortaxa", "nortaxa_taxon_id", "52369").sporely_taxon_id == own_id

    usage = _usages(release)[("nortaxa", "52369")]
    assert usage["sporely_taxon_id"] == backbone_id
    assert usage["identity_binding"] == "alias"
    assert usage["bridge_evidence_class"] == "reviewed_supersession"
    assert usage["superseded_from_sporely_taxon_id"] == own_id

    conn, _summary = _candidate(tmp_path, release, "superseded.sqlite3")
    try:
        assert _resolve(conn, "nortaxa", "nortaxa_taxon_id", "52369") == \
            [backbone_id]
        row = conn.execute(
            "SELECT * FROM taxon_external_id_text_min "
            "WHERE source_system='nortaxa' AND external_id='52369'").fetchone()
        assert row["note"] == "authoritative_bridge:reviewed_supersession"
        # NorTaxa's accepted-name treatment is preserved, not flattened.
        assert row["external_name"] == "Pholiotina rugosa"
        assert row["is_preferred"] == 0

        # No duplicate concept for the species either source describes.
        assert conn.execute(
            "SELECT COUNT(*) FROM taxon_min WHERE canonical_scientific_name "
            "IN ('Conocybe rugosa', 'Pholiotina rugosa')").fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM taxon_min WHERE taxon_id = ?",
            (own_id,)).fetchone()[0] == 0

        # Both treatments searchable; the backbone keeps its preferred name.
        names = {(r["scientific_name"], r["is_preferred_name"], r["source"])
                 for r in conn.execute(
                     "SELECT * FROM scientific_name_min WHERE taxon_id = ?",
                     (backbone_id,))}
        assert ("Conocybe rugosa", 1, "col_xr") in names
        assert ("Pholiotina rugosa", 0, "nortaxa") in names

        # The Norwegian name follows the identity onto the retained concept.
        vern = {r["vernacular_name"] for r in conn.execute(
            "SELECT * FROM vernacular_min WHERE taxon_id = ?", (backbone_id,))}
        assert "slank ringkjeglesopp" in vern
        assert conn.execute(
            "SELECT COUNT(*) FROM vernacular_min WHERE taxon_id = ?",
            (own_id,)).fetchone()[0] == 0
    finally:
        conn.close()


def test_supersession_chain_is_refused(tmp_path: Path) -> None:
    """A two-step supersession needs its own decision, not inference."""
    first = _compile(tmp_path, release_id="tax-2026.09.23-01")
    own_id = _usages(first)[("nortaxa", "52369")]["sporely_taxon_id"]
    backbone_id = _usages(first)[("col_xr", "5ZT3G")]["sporely_taxon_id"]
    with pytest.raises(CompilerError, match="supersession chain"):
        compile_release(
            normalized_source_dirs=[tmp_path / "sources" / "col_xr",
                                    tmp_path / "sources" / "nortaxa"],
            manual_mappings_path=_write_manual_mappings(tmp_path / "m3.yml", []),
            mapping_policy_path=_POLICY_PATH,
            registry_path=tmp_path / "registry.jsonl",
            output_dir=tmp_path / "release3",
            release_id="tax-2026.09.23-03",
            concept_supersessions_path=_write_supersessions(
                tmp_path / "chain.yml", [
                    {"supersession_id": "a", "superseded_sporely_taxon_id": own_id,
                     "current_source_usage": {"source": "col_xr",
                                              "namespace": "col_xr_taxon_id",
                                              "identifier": "5ZT3G"},
                     "relationship": "exact", "review_status": "approved"},
                    {"supersession_id": "b",
                     "superseded_sporely_taxon_id": backbone_id,
                     "current_source_usage": {"source": "col_xr",
                                              "namespace": "col_xr_taxon_id",
                                              "identifier": "39ZCL"},
                     "relationship": "exact", "review_status": "approved"},
                ]),
        )


def test_unapproved_supersession_is_ignored(tmp_path: Path) -> None:
    """A record awaiting review must change nothing."""
    first = _compile(tmp_path, release_id="tax-2026.09.23-01")
    own_id = _usages(first)[("nortaxa", "52369")]["sporely_taxon_id"]
    release = tmp_path / "release2"
    compile_release(
        normalized_source_dirs=[tmp_path / "sources" / "col_xr",
                                tmp_path / "sources" / "nortaxa"],
        manual_mappings_path=_write_manual_mappings(tmp_path / "m4.yml", []),
        mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "registry.jsonl",
        output_dir=release,
        release_id="tax-2026.09.23-02",
        concept_supersessions_path=_write_supersessions(
            tmp_path / "pending.yml", [{
                "supersession_id": "pending",
                "superseded_sporely_taxon_id": own_id,
                "current_source_usage": {"source": "col_xr",
                                         "namespace": "col_xr_taxon_id",
                                         "identifier": "5ZT3G"},
                "relationship": "exact",
                "review_status": "needs_review",
            }]),
    )
    usage = _usages(release)[("nortaxa", "52369")]
    assert usage["sporely_taxon_id"] == own_id
    assert usage["identity_binding"] == "anchor"
    assert usage["superseded_from_sporely_taxon_id"] is None


def test_manual_bridge_onto_an_already_allocated_anchor_fails_closed(
    tmp_path: Path,
) -> None:
    """The blocker this stage uncovered, pinned so it cannot regress silently.

    The production registry allocated ``(nortaxa, nortaxa_taxon_id, 52369)``
    as its *own* anchor in ``tax-2026.07.29-01``. The registry is append-only,
    so an approved manual mapping cannot retroactively turn that anchor into
    an alias of another concept: doing so would be a concept merge, which
    ``mapping_policy.continuity_rules`` requires review for and which no
    current artifact can express.

    The compiler refuses, by design. This test asserts the refusal rather than
    the wish, because a projection that silently resolved ``52369`` to the
    backbone concept while the registry still said ``624680`` would leave two
    competing answers for one identifier.
    """
    first = _compile(tmp_path, release_id="tax-2026.09.23-01")
    anchored = _usages(first)[("nortaxa", "52369")]
    assert anchored["identity_binding"] == "anchor"
    own_id = anchored["sporely_taxon_id"]

    # Recompile against the SAME registry, now adding the reviewed mapping.
    with pytest.raises(CompilerError) as excinfo:
        compile_release(
            normalized_source_dirs=[
                tmp_path / "sources" / "col_xr",
                tmp_path / "sources" / "nortaxa",
            ],
            manual_mappings_path=_write_manual_mappings(
                tmp_path / "manual2.yml", [{
                    "mapping_id": "nortaxa-52369-to-col-5ZT3G",
                    "source_usage": {"source": "nortaxa",
                                     "namespace": "nortaxa_taxon_id",
                                     "identifier": "52369"},
                    "target": {"source_usage": {"source": "col_xr",
                                                "namespace": "col_xr_taxon_id",
                                                "identifier": "5ZT3G"}},
                    "relationship": "exact",
                    "review_status": "approved",
                }]),
            mapping_policy_path=_POLICY_PATH,
            registry_path=tmp_path / "registry.jsonl",
            output_dir=tmp_path / "release2",
            release_id="tax-2026.09.23-02",
        )
    message = str(excinfo.value)
    assert "conflicts with registry" in message
    assert str(own_id) in message


# ------------------------------------------------------- release mechanics ---


def test_projection_is_deterministic_and_records_its_standard(
    tmp_path: Path,
) -> None:
    """Two projections of one release agree byte for byte and name the policy."""
    release = _compile(tmp_path)
    first_conn, first = _candidate(tmp_path, release, "a.sqlite3")
    second_conn, second = _candidate(tmp_path, release, "b.sqlite3")
    try:
        assert first["sqlite_sha256"] == second["sqlite_sha256"]
        assert first["authoritative_bridge_emission"] == \
            second["authoritative_bridge_emission"]
        expected = BridgeEmissionPolicy.load(_POLICY_PATH).policy_sha256
        recorded = dict(first_conn.execute(
            "SELECT key, value FROM taxonomy_meta"))
        assert recorded["bridge_emission_policy_sha256"] == expected
    finally:
        first_conn.close()
        second_conn.close()


def test_coverage_is_complete_every_binding_emitted_or_explained(
    tmp_path: Path,
) -> None:
    """No alias binding may be left unaccounted for.

    The audit requirement is that the two numbers add up: every cross-source
    alias binding in the release is either published as authoritative identity
    or counted with the reason it was refused. A binding that appears in
    neither column is an unexplained loss.
    """
    release = _compile(tmp_path)
    policy = BridgeEmissionPolicy.load(_POLICY_PATH)
    _conn, summary = _candidate(tmp_path, release)
    _conn.close()
    audit = summary["authoritative_bridge_emission"]

    candidates = [
        usage for usage in _usages(release).values()
        if usage["identity_binding"] != "anchor"
        and usage["source_usage"]["namespace"] != "col_usage_id"
    ]
    assert candidates, "fixture must contain alias bindings to audit"
    assert audit["emitted_total"] + audit["rejected_total"] == len(candidates)

    expected_emitted = sum(
        1 for usage in candidates
        if policy.is_eligible(usage["bridge_evidence_class"]))
    assert audit["emitted_total"] == expected_emitted
    # And the emitted set is exactly the eligible set — not a subset.
    assert sum(audit["emitted_by_evidence_class"].values()) == expected_emitted


# ------------------------------------------ survival past the SQLite candidate ---


def _export_and_scope(tmp_path: Path, release_dir: Path) -> tuple[Path, Path]:
    """Run the real cloud export over a candidate built from ``release_dir``.

    Verifying the candidate alone would not answer the question that matters:
    a bridge row that exists in desktop SQLite but is dropped by the exporter
    is worthless, and so is one the scoped projection discards. This drives
    ``cloud_export.run_export`` over a genuinely compiled candidate.
    """
    import gzip
    import sys as _sys

    _root = _TAXONOMY.parents[1]
    if str(_root) not in _sys.path:
        _sys.path.insert(0, str(_root))
    from database.taxonomy import cloud_export as ce

    release_id = json.loads(
        (release_dir / "manifest.json").read_text(encoding="utf-8")
    )["content_release_id"]
    art_dir = tmp_path / "artifact"
    art_dir.mkdir(exist_ok=True)
    sqlite_path = art_dir / f"{release_id}.sqlite3"
    build_candidate(
        release_dir=release_dir,
        registry_path=tmp_path / "registry.jsonl",
        output_db=sqlite_path,
    )
    gz_path = art_dir / f"{release_id}.sqlite3.gz"
    with sqlite_path.open("rb") as src, gz_path.open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as gz:
            while chunk := src.read(1 << 16):
                gz.write(chunk)
    manifest_path = art_dir / "manifest.json"
    manifest_path.write_text(json.dumps({
        "manifest_schema_version": 1,
        "taxonomy_schema_version": 2,
        "content_release_id": release_id,
        "state": "candidate",
        "publication": "none",
        "gz_artifact": gz_path.name,
        "gz_sha256": ce.sha256_file(gz_path),
        "gz_bytes": gz_path.stat().st_size,
        "sqlite_sha256": ce.sha256_file(sqlite_path),
        "sqlite_bytes": sqlite_path.stat().st_size,
    }, indent=2), encoding="utf-8")
    sqlite_path.unlink()
    policies = tmp_path / "policies"
    policies.mkdir(exist_ok=True)
    for name in ce.POLICY_HASH_TARGETS[:2]:
        (policies / name).write_text(f"# stub {name}\n", encoding="utf-8")
    out = tmp_path / "cloud_export"
    ce.run_export(
        artifact_gz=gz_path, manifest=manifest_path, output_dir=out,
        policy_dir=policies, generated_at="2099-01-01T00:00:00Z",
    )
    return out, gz_path


def test_reviewed_bridge_survives_cloud_export(tmp_path: Path) -> None:
    """The emitted identity must reach the cloud export, not stop at SQLite."""
    release = _compile(tmp_path, manual=[_REVIEWED_53482])
    export_dir, _gz = _export_and_scope(tmp_path, release)

    rows = [
        json.loads(line)
        for line in (export_dir / "taxon_external_id.jsonl").read_text(
            encoding="utf-8").splitlines() if line.strip()
    ]
    bridge = [r for r in rows if r["source_system"] == "nortaxa"
              and r["external_id"] == "53482"]
    assert len(bridge) == 1
    assert bridge[0]["namespace"] == "nortaxa_taxon_id"
    assert bridge[0]["note"] == "authoritative_bridge:manual_approved_exact"
    assert bridge[0]["is_preferred"] is False
    # The backbone identifier is still there and still preferred, so the
    # export gained a mapping rather than replacing one.
    backbone = [r for r in rows if r["taxon_id"] == bridge[0]["taxon_id"]
                and r["source_system"] == "col_xr"]
    assert backbone and backbone[0]["is_preferred"] is True


def test_legacy_suppression_accounts_for_every_in_scope_row(
    tmp_path: Path,
) -> None:
    """D7: the emptied legacy dataset must explain what it dropped."""
    import sys as _sys
    _root = _TAXONOMY.parents[1]
    if str(_root) not in _sys.path:
        _sys.path.insert(0, str(_root))
    from database.taxonomy.macrofungi_scope import (
        ScopeError, suppress_legacy_integer_ids,
    )

    release = _compile(tmp_path, manual=[_REVIEWED_53482])
    export_dir, _gz = _export_and_scope(tmp_path, release)
    scoped = tmp_path / "scoped"
    scoped.mkdir()
    # The scoped projection republishes the authoritative dataset verbatim for
    # the retained concepts; mirror that so the census has something to match.
    authoritative = (export_dir / "taxon_external_id.jsonl").read_text(
        encoding="utf-8")
    (scoped / "taxon_external_id.jsonl").write_text(authoritative,
                                                    encoding="utf-8")
    included = {
        json.loads(line)["taxon_id"]
        for line in authoritative.splitlines() if line.strip()
    }

    result = suppress_legacy_integer_ids(
        w1_dir=export_dir, output_dir=scoped, included=included,
    )
    census = result["census"]
    # Emptied, but accounted for: nothing is dropped without a classification.
    assert result["written"][0] == 0
    assert census["in_scope_rows"] == (
        census["represented_authoritatively"]
        + census["suppressed_not_authoritative"]
    )
    assert census["in_scope_rows"] > 0
    assert census["represented_authoritatively"] >= 1

    # A reviewed bridge present only in the legacy channel is a real loss and
    # must fail the build rather than look like a correct suppression.
    (export_dir / "taxon_external_id_legacy_integer.jsonl").write_text(
        json.dumps({"taxon_id": sorted(included)[0], "source_system":
                    "artsdatabanken", "external_id": "999999",
                    "id_role": "accepted", "is_preferred": 0,
                    "external_name": "Lost Bridge",
                    "note": "manual_approved_exact"}) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ScopeError, match="reviewed manual bridge lost"):
        suppress_legacy_integer_ids(
            w1_dir=export_dir, output_dir=scoped, included=included,
        )
