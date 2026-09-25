"""Stage 4 Part A: the dry-run audit classifies on evidence and repairs little.

The audit is the thing a reviewer signs off before any production write, so
what is pinned here is not "it found the rows" but the properties that make a
signature mean something:

* classification is deterministic and total — three axes, each accounting for
  every row exactly once, and two runs producing byte-identical output;
* only a namespaced tuple that resolves to exactly one concept is repairable,
  and none of the four forbidden inferences (equal integers, equal names,
  a unique name search, an obvious-looking species) can produce a repair;
* repair is idempotent, and a second run over repaired rows changes nothing;
* a row that was already correct is never touched by any run;
* the production gate fails closed.

Nothing here mocks the classifier or the repair writer: the tests build real
SQLite artifacts in the two shapes production actually ships and run the
module's own functions against them.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from database.audit_observation_identity import (
    ACTIONS,
    IDENTITY_CLASSES,
    NAME_CLASSES,
    NAME_LOSS_CLASSES,
    ProductionMigrationGate,
    ProductionWriteRefused,
    StaleAuditArtifact,
    TamperedAuditArtifact,
    TaxonomyArtifact,
    apply_repairs,
    audit,
    build_parser,
    gate_from_args,
    load_reviewed_artifact,
    read_cloud_rows,
    read_desktop_rows,
    require_artifact_still_describes,
    require_open_gate,
)
from utils.taxon_identity import (
    PROOF_EXTERNAL_ID_RESOLUTION,
    PROOF_TAXONOMY_V2_ARTIFACT,
    STATE_SPORELY,
    TaxonIdentity,
)


#: A gate whose five conditions an operator has asserted. Every repair test
#: has to supply one, which is the point: ``apply_repairs`` enforces the gate
#: itself, so there is no way to exercise a write without going through it.
_OPEN_GATE = ProductionMigrationGate(
    dry_run_artifact_reviewed=True,
    counts_reconcile=True,
    candidate_release_validated=True,
    rollback_procedure_documented=True,
    integrity_checks_defined=True,
    evidence=("test",),
)


# ── Fixtures: the two artifact shapes production ships ──────────────────────

#: (taxon_id, scientific name). 53482 is deliberately BOTH a NorTaxa external
#: identifier (for Entoloma conferendum) and a valid Sporely concept of its
#: own, which is the numeric collision the whole stage exists to catch.
_TAXA = (
    (7821, "Entoloma conferendum"),
    (83668, "Conocybe rugosa"),
    (53482, "Mycena galericulata"),
    (40656, "Entoloma"),
)

_MAPPINGS = (
    (7821, "col_xr", "col_usage_id", "39ZCL"),
    (7821, "nortaxa", "nortaxa_taxon_id", "53482"),
    (83668, "col_xr", "col_usage_id", "5ZT3G"),
    (83668, "nortaxa", "nortaxa_taxon_id", "52369"),
    (53482, "col_xr", "col_usage_id", "7QXYZ"),
    # A deliberately ambiguous tuple: two concepts under one identifier.
    (7821, "nortaxa", "nortaxa_taxon_id", "99999"),
    (83668, "nortaxa", "nortaxa_taxon_id", "99999"),
)


def _candidate_artifact(tmp_path: Path) -> Path:
    """``build_sqlite_candidate.py``'s shape: taxon_min + the text table."""
    path = tmp_path / "candidate.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE taxon_min (taxon_id INTEGER PRIMARY KEY,
                                canonical_scientific_name TEXT);
        CREATE TABLE taxon_external_id_text_min (
            taxon_id INTEGER, source_system TEXT, namespace TEXT,
            external_id TEXT);
        CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT);
        """
    )
    conn.executemany("INSERT INTO taxon_min VALUES (?, ?)", _TAXA)
    conn.executemany(
        "INSERT INTO taxon_external_id_text_min VALUES (?, ?, ?, ?)", _MAPPINGS
    )
    conn.execute(
        "INSERT INTO taxonomy_meta VALUES ('content_release_id', 'tax-test-01')"
    )
    conn.commit()
    conn.close()
    return path


def _desktop_pack_artifact(tmp_path: Path) -> Path:
    """``macrofungi_scope.build_desktop``'s shape: taxon + external_mapping."""
    path = tmp_path / "desktop-pack.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE taxon (taxon_id INTEGER PRIMARY KEY, scientific_name TEXT);
        CREATE TABLE external_mapping (
            taxon_id INTEGER, source_system TEXT, namespace TEXT,
            external_id TEXT, is_preferred INTEGER);
        CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT);
        """
    )
    conn.executemany("INSERT INTO taxon VALUES (?, ?)", _TAXA)
    conn.executemany(
        "INSERT INTO external_mapping VALUES (?, ?, ?, ?, 0)", _MAPPINGS
    )
    conn.execute("INSERT INTO metadata VALUES ('release_id', 'tax-test-01')")
    conn.commit()
    conn.close()
    return path


def _observations_db(tmp_path: Path, rows: list[dict]) -> Path:
    """A minimal observations table carrying the audited columns only."""
    path = tmp_path / "observations.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY,
            genus TEXT, species TEXT, common_name TEXT,
            notes TEXT, location TEXT,
            sporely_taxon_id INTEGER,
            taxon_identity_state TEXT,
            taxon_identity_proof TEXT,
            taxon_identity_source_system TEXT,
            taxon_identity_namespace TEXT,
            taxon_identity_external_id TEXT,
            taxon_identity_raw_external_id TEXT,
            taxon_identity_provenance TEXT,
            ai_selected_taxon_id TEXT,
            ai_selected_scientific_name TEXT,
            scientific_name_snapshot TEXT,
            taxon_rank_snapshot TEXT
        );
        """
    )
    for row in rows:
        keys = sorted(row)
        conn.execute(
            f"INSERT INTO observations ({', '.join(keys)}) VALUES "
            f"({', '.join('?' for _ in keys)})",
            tuple(row[key] for key in keys),
        )
    conn.commit()
    conn.close()
    return path


#: One row per class the stage requires, plus the rows that must NOT repair.
_POPULATION = [
    # 1 — already proven against this artifact. Untouchable.
    {
        "id": 1,
        "genus": "Conocybe",
        "species": "rugosa",
        **TaxonIdentity.from_taxonomy_v2_artifact(83668).to_row(),
    },
    # 2 — a preserved NBIC identifier that resolves to exactly one concept.
    {
        "id": 2,
        "genus": "Entoloma",
        "species": "conferendum",
        **TaxonIdentity.from_prefixed_external_id("NBIC:53482").to_row(),
    },
    # 3 — a preserved identifier with no authoritative mapping.
    {
        "id": 3,
        "genus": "Amanita",
        "species": "muscaria",
        **TaxonIdentity.from_prefixed_external_id("NBIC:11111").to_row(),
    },
    # 4 — a preserved identifier matching two concepts.
    {
        "id": 4,
        "genus": "Entoloma",
        "species": "sp",
        **TaxonIdentity.from_prefixed_external_id("NBIC:99999").to_row(),
    },
    # 5 — typed text and nothing else.
    {"id": 5, "genus": "Russula", "species": "emetica"},
    # 6 — THE COLLISION. A bare legacy integer 53482 that is a real Sporely
    #     concept (Mycena galericulata) AND a NorTaxa identifier for a
    #     different concept. The row's own provider identifier resolves to
    #     7821, contradicting the stored integer.
    {
        "id": 6,
        "genus": "Entoloma",
        "species": "conferendum",
        "sporely_taxon_id": 53482,
        "ai_selected_taxon_id": "NBIC:53482",
        "ai_selected_scientific_name": "Entoloma conferendum",
    },
    # 7 — a bare legacy integer with nothing to re-derive it from.
    {"id": 7, "genus": "Cantharellus", "species": "cibarius",
     "sporely_taxon_id": 83668},
    # 8 — a legacy integer whose own provider identifier re-derives it.
    {
        "id": 8,
        "genus": "Entoloma",
        "species": "conferendum",
        "sporely_taxon_id": 7821,
        "ai_selected_taxon_id": "NBIC:53482",
        "ai_selected_scientific_name": "Entoloma conferendum",
    },
    # 9 — NAME LOSS, repairable from the row's own provider candidate.
    {
        "id": 9,
        "ai_selected_taxon_id": "NBIC:53482",
        "ai_selected_scientific_name": "Entoloma conferendum",
    },
    # 10 — NAME LOSS, unrepairable: the provider string is not a binomial.
    {"id": 10, "ai_selected_scientific_name": "Entoloma"},
    # 13 — OUTSIDE the defined population: genus and species are null but a
    #      common name survived, so this is not the null-genus/species/
    #      common_name shape the stage specifies. Reported, never repaired.
    {"id": 13, "common_name": "stjernesporet rødspore",
     "ai_selected_scientific_name": "Entoloma conferendum"},
    # 11 — proven against a release this artifact is not. Not a repair.
    {
        "id": 11,
        "genus": "Ghost",
        "species": "taxon",
        **TaxonIdentity.from_taxonomy_v2_artifact(60606).to_row(),
    },
    # 12 — the collision with NOTHING to re-derive from. 53482 is a valid
    #      Sporely concept and also a NorTaxa identifier, and this row carries
    #      no provider identifier that could settle which one it means. The
    #      only honest answer is "suspicious", never a repair.
    {"id": 12, "genus": "Mycena", "species": "galericulata",
     "sporely_taxon_id": 53482},
]


@pytest.fixture(params=["candidate", "desktop_pack"])
def artifact(request, tmp_path):
    """Both compiled shapes, because an operator may hold either one."""
    builder = (
        _candidate_artifact if request.param == "candidate" else _desktop_pack_artifact
    )
    art = TaxonomyArtifact(builder(tmp_path))
    yield art
    art.close()


@pytest.fixture
def population_db(tmp_path):
    return _observations_db(tmp_path, _POPULATION)


def _by_id(report):
    return {record.observation_id: record for record in report.records}


# ── Classification ──────────────────────────────────────────────────────────


def test_every_required_class_is_produced_by_evidence(artifact, population_db):
    records = _by_id(audit(read_desktop_rows(population_db), artifact, origin="desktop"))

    assert records[1].identity_class == "proven_sporely_identity"
    assert records[2].identity_class == "proven_external_with_unique_bridge"
    assert records[2].candidate_sporely_taxon_id == 7821
    assert records[3].identity_class == "unresolved_external_identity"
    assert records[4].identity_class == "ambiguous_external_identity"
    assert records[5].identity_class == "manual_or_no_identity_evidence"
    assert records[6].identity_class == "suspicious_numeric_collision"
    assert records[7].identity_class == "legacy_unverified_identity"
    assert records[8].identity_class == "proven_external_with_unique_bridge"
    assert records[11].identity_class == "stale_sporely_identity"
    assert records[12].identity_class == "suspicious_numeric_collision"
    assert "external identifier under nortaxa/nortaxa_taxon_id" in (
        records[12].refusal_reason
    )

    assert records[9].name_class == "name_loss_repairable_from_row"
    assert records[10].name_class == "name_loss_unrepairable_from_row"
    assert records[1].name_class == "name_intact"
    assert records[13].name_class == "partial_name_loss_reported"


def test_a_surviving_common_name_is_outside_the_repairable_population(
    artifact, population_db
):
    """The stage defines the population as all THREE name fields null.

    A row that kept its common name is not that shape. Repairing it would put
    production writes outside what was specified and reviewed, so it is
    reported and its binomial is left null.
    """
    record = _by_id(
        audit(read_desktop_rows(population_db), artifact, origin="desktop")
    )[13]
    assert record.name_class == "partial_name_loss_reported"
    assert record.proposed_action == "report_only"
    assert record.restored_genus is None
    assert record.restored_species is None
    # And it is excluded from the population the acceptance gate counts.
    census = audit(
        read_desktop_rows(population_db), artifact, origin="desktop"
    ).counts()
    assert census["name_loss_population"] == 2


def test_the_collision_is_refused_with_a_stated_reason(artifact, population_db):
    """53482 is a real Sporely concept AND a NorTaxa identifier.

    A repair that trusted the integer would silently retag this observation as
    *Mycena galericulata*. The audit must refuse it and say why.
    """
    record = _by_id(
        audit(read_desktop_rows(population_db), artifact, origin="desktop")
    )[6]
    assert record.proposed_action == "report_only"
    assert record.candidate_sporely_taxon_id is None
    assert "disagrees" in record.refusal_reason
    assert "53482" in record.refusal_reason


def test_name_equality_never_produces_a_repair(artifact, tmp_path):
    """Row 5's name is in the artifact verbatim; that is still not identity.

    Every refused row here would have been "resolved" by one of the four
    inferences the repair rule forbids.
    """
    rows = [
        # exact canonical-name match in the artifact
        {"id": 1, "genus": "Conocybe", "species": "rugosa"},
        # a bare integer equal to a real Sporely concept
        {"id": 2, "genus": "Conocybe", "species": "rugosa",
         "sporely_taxon_id": 83668},
        # a bare integer provider id — no prefix, so no namespace
        {"id": 3, "ai_selected_taxon_id": "53482",
         "ai_selected_scientific_name": "Entoloma conferendum",
         "genus": "Entoloma", "species": "conferendum"},
        # an unknown provider prefix
        {"id": 4, "ai_selected_taxon_id": "GBIF:53482",
         "ai_selected_scientific_name": "Entoloma conferendum",
         "genus": "Entoloma", "species": "conferendum"},
    ]
    db = _observations_db(tmp_path, rows)
    report = audit(read_desktop_rows(db), artifact, origin="desktop")
    assert [record.candidate_sporely_taxon_id for record in report.records] == [
        None, None, None, None
    ]
    assert report.repairable() == []


def test_the_three_axes_each_account_for_every_row(artifact, population_db):
    report = audit(read_desktop_rows(population_db), artifact, origin="desktop")
    census = report.counts()
    assert census["total_observations"] == len(_POPULATION)
    assert report.reconciles() is True
    assert set(census["identity_class"]) == set(IDENTITY_CLASSES)
    assert set(census["name_class"]) == set(NAME_CLASSES)
    assert set(census["proposed_action"]) == set(ACTIONS)
    assert census["name_loss_population"] == sum(
        census["name_class"][key] for key in NAME_LOSS_CLASSES
    )
    assert census["name_loss_population"] == 2


def test_the_audit_artifact_is_byte_identical_across_runs(artifact, population_db):
    """A reviewed dry run must describe what a later run will do."""
    rows = read_desktop_rows(population_db)
    first = json.dumps(audit(rows, artifact, origin="desktop").as_dict(),
                       indent=2, sort_keys=True, ensure_ascii=False)
    second = json.dumps(audit(rows, artifact, origin="desktop").as_dict(),
                        indent=2, sort_keys=True, ensure_ascii=False)
    assert first == second


def test_every_refusal_states_a_reason(artifact, population_db):
    report = audit(read_desktop_rows(population_db), artifact, origin="desktop")
    for record in report.records:
        if record.proposed_action == "report_only":
            assert record.refusal_reason, record.observation_id


# ── Repair ──────────────────────────────────────────────────────────────────


def _snapshot(db_path: Path) -> list[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = [dict(row) for row in conn.execute("SELECT * FROM observations ORDER BY id")]
    conn.close()
    return rows


def test_repair_binds_only_proven_rows_and_leaves_the_rest_alone(
    artifact, population_db
):
    before = {row["id"]: row for row in _snapshot(population_db)}
    report = audit(read_desktop_rows(population_db), artifact, origin="desktop")
    stats = apply_repairs(
        report, observation_db_path=population_db, release_id=artifact.release_id, gate=_OPEN_GATE
    )
    after = {row["id"]: row for row in _snapshot(population_db)}

    # Rows 2 and 8 bind; row 9 binds AND regains its name.
    assert stats.identities_bound == 3
    assert stats.names_restored == 1
    assert after[2]["sporely_taxon_id"] == 7821
    assert after[2]["taxon_identity_proof"] == PROOF_EXTERNAL_ID_RESOLUTION
    assert after[2]["taxon_identity_state"] == STATE_SPORELY
    assert after[2]["taxon_identity_raw_external_id"] == "NBIC:53482"
    assert artifact.release_id in after[2]["taxon_identity_provenance"]
    assert after[9]["genus"] == "Entoloma"
    assert after[9]["species"] == "conferendum"

    untouched = {1, 3, 4, 5, 6, 7, 10, 11, 12, 13}
    for obs_id in untouched:
        assert after[obs_id] == before[obs_id], obs_id


def test_a_second_run_proposes_nothing_and_writes_nothing(artifact, population_db):
    report = audit(read_desktop_rows(population_db), artifact, origin="desktop")
    apply_repairs(
        report, observation_db_path=population_db, release_id=artifact.release_id, gate=_OPEN_GATE
    )
    settled = _snapshot(population_db)

    second = audit(read_desktop_rows(population_db), artifact, origin="desktop")
    assert second.repairable() == []
    stats = apply_repairs(
        second, observation_db_path=population_db, release_id=artifact.release_id, gate=_OPEN_GATE
    )
    assert stats.as_dict() == {
        "identities_bound": 0,
        "names_restored": 0,
        "names_skipped_changed_since_audit": 0,
        "rows_written": 0,
    }
    assert _snapshot(population_db) == settled

    # And the bound rows now read back as what they are.
    bound = {row["id"]: row for row in settled}
    identity = TaxonIdentity.from_row(bound[2])
    assert identity.is_proven_sporely is True
    assert identity.sporely_taxon_id == 7821


def test_a_name_only_repair_skips_rather_than_replacing(artifact, tmp_path):
    """The write carries its own IS NULL guard, not just a classification.

    This row has a name to restore and no resolvable identifier, so the repair
    is name-only. Filling a null cannot destroy anything, so a name that
    appeared in between is skipped — and counted, because a silent skip would
    make the applied result differ from the reviewed artifact with nothing
    recording that it did.
    """
    db = _observations_db(tmp_path, [
        {"id": 1, "ai_selected_scientific_name": "Entoloma conferendum"},
    ])
    report = audit(read_desktop_rows(db), artifact, origin="desktop")
    assert report.records[0].proposed_action == "restore_lost_names"

    # Someone identifies the observation in between the dry run and the apply.
    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE observations SET genus = 'Amanita', species = 'muscaria' WHERE id = 1"
    )
    conn.commit()
    conn.close()

    stats = apply_repairs(
        report, observation_db_path=db, release_id=None, gate=_OPEN_GATE
    )

    assert stats.names_restored == 0
    assert stats.names_skipped_changed_since_audit == 1
    row = _snapshot(db)[0]
    assert (row["genus"], row["species"]) == ("Amanita", "muscaria")


def test_a_changed_name_aborts_a_coupled_bind_instead_of_mismatching(
    artifact, tmp_path
):
    """The bound concept and the accepted name are ONE value.

    This record both binds 7821 (*Entoloma conferendum*) and restores its
    name. Someone renames the row *Amanita muscaria* in between. Skipping only
    the name write would commit 7821 beside "Amanita muscaria" — a row naming
    one taxon and identifying another, which is precisely what the sync
    contract couples these fields to prevent. The identity pre-image includes
    the name columns, so the run aborts instead.
    """
    db = _observations_db(tmp_path, [
        {"id": 1, "ai_selected_scientific_name": "Entoloma conferendum",
         "ai_selected_taxon_id": "NBIC:53482"},
    ])
    report = audit(read_desktop_rows(db), artifact, origin="desktop")
    assert report.records[0].proposed_action == (
        "bind_sporely_identity_and_restore_lost_names"
    )

    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE observations SET genus = 'Amanita', species = 'muscaria' WHERE id = 1"
    )
    conn.commit()
    conn.close()
    renamed = _snapshot(db)

    with pytest.raises(StaleAuditArtifact) as excinfo:
        apply_repairs(
            report, observation_db_path=db, release_id=None, gate=_OPEN_GATE
        )
    assert "accepted name changed" in str(excinfo.value)

    # Nothing bound, nothing renamed: no mismatched row was committed.
    assert _snapshot(db) == renamed
    assert renamed[0]["sporely_taxon_id"] is None


def test_a_changed_common_name_alone_also_aborts_a_coupled_bind(
    artifact, tmp_path
):
    """``common_name`` is part of the coupled value too, not decoration."""
    db = _observations_db(tmp_path, [
        {"id": 1, "genus": "Entoloma", "species": "conferendum",
         **TaxonIdentity.from_prefixed_external_id("NBIC:53482").to_row()},
    ])
    report = audit(read_desktop_rows(db), artifact, origin="desktop")
    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE observations SET common_name = 'flueusopp' WHERE id = 1"
    )
    conn.commit()
    conn.close()
    before = _snapshot(db)

    with pytest.raises(StaleAuditArtifact):
        apply_repairs(
            report, observation_db_path=db, release_id=None, gate=_OPEN_GATE
        )
    assert _snapshot(db) == before


def test_a_repair_touches_no_unrelated_observation_content(artifact, tmp_path):
    db = _observations_db(tmp_path, [
        {"id": 1, "notes": "under bjørk", "location": "Trondheim",
         "common_name": "stjernesporet rødspore",
         "ai_selected_taxon_id": "NBIC:53482",
         "ai_selected_scientific_name": "Entoloma conferendum"},
    ])
    report = audit(read_desktop_rows(db), artifact, origin="desktop")
    # A surviving common name puts this row outside the name-loss population,
    # so only its identity is repaired.
    assert report.records[0].name_class == "partial_name_loss_reported"
    apply_repairs(report, observation_db_path=db, release_id=None, gate=_OPEN_GATE)
    row = _snapshot(db)[0]
    assert row["genus"] is None
    assert row["species"] is None
    assert row["notes"] == "under bjørk"
    assert row["location"] == "Trondheim"
    assert row["common_name"] == "stjernesporet rødspore"
    # Provider history is a separate record and survives the repair.
    assert row["ai_selected_taxon_id"] == "NBIC:53482"


def test_a_failure_part_way_through_leaves_nothing_applied(artifact, tmp_path):
    """The apply is one transaction, so a partial repair cannot survive.

    The forced failure is the module's own hard stop against writing an
    unproven identity — a record whose resolved concept has lost its source
    tuple. That it aborts is one assertion; that the *earlier* row in the same
    run is rolled back is the one that matters, because a half-applied repair
    is worse than none: the archived dry run would no longer describe the
    database it was the pre-image of.
    """
    from dataclasses import replace

    db = _observations_db(tmp_path, [
        {"id": 1, "genus": "Entoloma", "species": "conferendum",
         **TaxonIdentity.from_prefixed_external_id("NBIC:53482").to_row()},
        {"id": 2, "genus": "Conocybe", "species": "rugosa",
         **TaxonIdentity.from_prefixed_external_id("NBIC:52369").to_row()},
    ])
    before = _snapshot(db)
    report = audit(read_desktop_rows(db), artifact, origin="desktop")
    assert len(report.repairable()) == 2
    # Corrupt the second record only, after the first has been queued.
    report.records[1] = replace(
        report.records[1], preserved_source_system=None, preserved_namespace=None
    )

    with pytest.raises(RuntimeError):
        apply_repairs(report, observation_db_path=db, release_id=None, gate=_OPEN_GATE)

    assert _snapshot(db) == before


# ── Cloud rows ──────────────────────────────────────────────────────────────


def test_cloud_rows_are_audited_through_the_same_classifier(artifact, tmp_path):
    """The cloud spelling of the bound concept is read, and nothing is written."""
    export = tmp_path / "cloud.json"
    export.write_text(json.dumps({"observations": [
        {"id": 917, "genus": None, "species": None,
         "selected_sporely_taxon_id": None, "resolved_sporely_taxon_id": None,
         "ai_selected_taxon_id": "NBIC:53482",
         "ai_selected_scientific_name": "Entoloma conferendum"},
        {"id": 918, "genus": "Conocybe", "species": "rugosa",
         "selected_sporely_taxon_id": 83668,
         "taxon_identity_state": STATE_SPORELY,
         "taxon_identity_proof": PROOF_TAXONOMY_V2_ARTIFACT},
    ]}), encoding="utf-8")

    report = audit(read_cloud_rows(export), artifact, origin="cloud")
    records = _by_id(report)
    assert records[917].identity_class == "proven_external_with_unique_bridge"
    assert records[917].candidate_sporely_taxon_id == 7821
    assert records[917].name_class == "name_loss_repairable_from_row"
    assert records[918].identity_class == "proven_sporely_identity"
    assert report.reconciles() is True


# ── The gate guards the write path, not just the tests ──────────────────────


def test_apply_refuses_every_closed_gate(artifact, population_db):
    """A gate only the caller consults protects only well-behaved callers."""
    report = audit(read_desktop_rows(population_db), artifact, origin="desktop")
    before = _snapshot(population_db)

    with pytest.raises(ProductionWriteRefused) as excinfo:
        apply_repairs(
            report,
            observation_db_path=population_db,
            release_id=None,
            gate=ProductionMigrationGate(),
        )
    assert "NEEDS_YOU" in str(excinfo.value)
    assert _snapshot(population_db) == before

    # And one missing condition is still closed.
    with pytest.raises(ProductionWriteRefused):
        apply_repairs(
            report,
            observation_db_path=population_db,
            release_id=None,
            gate=ProductionMigrationGate(
                dry_run_artifact_reviewed=True,
                counts_reconcile=True,
                candidate_release_validated=True,
                rollback_procedure_documented=True,
            ),
        )
    assert _snapshot(population_db) == before


def test_the_cli_apply_flag_carries_the_gate_and_defaults_closed(tmp_path):
    """``--apply`` alone cannot write: the five flags are separate assertions."""
    parser = build_parser()
    bare = parser.parse_args([
        "--taxonomy", "x", "--observations", "y", "--apply", "reviewed.json",
    ])
    assert gate_from_args(bare).is_open is False
    assert len(gate_from_args(bare).blocking_reasons()) == 5

    asserted = parser.parse_args([
        "--taxonomy", "x", "--observations", "y", "--apply", "reviewed.json",
        "--gate-dry-run-reviewed", "--gate-counts-reconcile",
        "--gate-release-validated", "--gate-rollback-documented",
        "--gate-integrity-checks-defined",
        "--gate-evidence", "stage4-audit.json",
    ])
    gate = gate_from_args(asserted)
    assert gate.is_open is True
    assert gate.evidence == ("stage4-audit.json",)


def test_apply_requires_a_reviewed_artifact_to_apply(tmp_path):
    """``--apply`` takes the reviewed file; there is no report-free form."""
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([
            "--taxonomy", "x", "--observations", "y", "--apply",
        ])


# ── The apply is bound to the artifact that was reviewed ────────────────────


def _write_artifact(path: Path, report) -> Path:
    path.write_text(
        json.dumps(report.as_dict(), indent=2, sort_keys=True, ensure_ascii=False),
        encoding="utf-8",
    )
    return path


def test_a_reviewed_artifact_round_trips_and_carries_its_own_digest(
    artifact, population_db, tmp_path
):
    report = audit(read_desktop_rows(population_db), artifact, origin="desktop")
    path = _write_artifact(tmp_path / "reviewed.json", report)

    loaded = load_reviewed_artifact(path)

    assert loaded.digest() == report.digest()
    assert [r.as_dict() for r in loaded.records] == [
        r.as_dict() for r in report.records
    ]
    # The pre-images the apply will use came from the file, not from a rerun.
    assert loaded.repairable()[0].stored_identity == (
        report.repairable()[0].stored_identity
    )


def test_an_edited_artifact_is_refused(artifact, population_db, tmp_path):
    """The digest is what stops a reviewed plan being rewritten after review.

    The edit here is the one that matters: widening a refusal into a repair.
    """
    report = audit(read_desktop_rows(population_db), artifact, origin="desktop")
    path = _write_artifact(tmp_path / "reviewed.json", report)

    data = json.loads(path.read_text(encoding="utf-8"))
    collision = next(
        entry for entry in data["observations"]
        if entry["identity_class"] == "suspicious_numeric_collision"
    )
    collision["proposed_action"] = "bind_sporely_identity"
    collision["candidate_sporely_taxon_id"] = 7821
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    with pytest.raises(TamperedAuditArtifact) as excinfo:
        load_reviewed_artifact(path)
    assert "edited after it was written" in str(excinfo.value)


def test_an_artifact_without_a_digest_is_refused(tmp_path):
    path = tmp_path / "hand-written.json"
    path.write_text(json.dumps({"observations": []}), encoding="utf-8")
    with pytest.raises(TamperedAuditArtifact):
        load_reviewed_artifact(path)


def test_a_row_added_after_the_review_blocks_the_whole_apply(
    artifact, population_db, tmp_path
):
    """The failure the per-row pre-image guard structurally cannot catch.

    A new observation is not in the artifact, so no per-row check ever looks
    at it. Only re-auditing the live rows and requiring the reviewed artifact
    to be reproduced exactly notices that the reviewed plan is no longer a
    plan for this database.
    """
    report = audit(read_desktop_rows(population_db), artifact, origin="desktop")
    path = _write_artifact(tmp_path / "reviewed.json", report)
    reviewed = load_reviewed_artifact(path)

    conn = sqlite3.connect(population_db)
    conn.execute(
        "INSERT INTO observations (id, ai_selected_taxon_id, "
        "ai_selected_scientific_name) VALUES (99, 'NBIC:53482', "
        "'Entoloma conferendum')"
    )
    conn.commit()
    conn.close()

    with pytest.raises(StaleAuditArtifact) as excinfo:
        require_artifact_still_describes(
            reviewed, read_desktop_rows(population_db), artifact
        )
    assert "added observations [99]" in str(excinfo.value)


def test_a_row_changed_after_the_review_blocks_the_whole_apply(
    artifact, population_db, tmp_path
):
    report = audit(read_desktop_rows(population_db), artifact, origin="desktop")
    reviewed = load_reviewed_artifact(
        _write_artifact(tmp_path / "reviewed.json", report)
    )

    conn = sqlite3.connect(population_db)
    conn.execute("UPDATE observations SET genus = 'Amanita' WHERE id = 5")
    conn.commit()
    conn.close()

    with pytest.raises(StaleAuditArtifact) as excinfo:
        require_artifact_still_describes(
            reviewed, read_desktop_rows(population_db), artifact
        )
    assert "changed [5]" in str(excinfo.value)


def test_an_unchanged_database_reproduces_the_reviewed_artifact(
    artifact, population_db, tmp_path
):
    """The check has to pass in the ordinary case, or it is just an outage."""
    report = audit(read_desktop_rows(population_db), artifact, origin="desktop")
    reviewed = load_reviewed_artifact(
        _write_artifact(tmp_path / "reviewed.json", report)
    )
    require_artifact_still_describes(
        reviewed, read_desktop_rows(population_db), artifact
    )


def test_an_unreconciled_report_is_refused_even_with_an_open_gate(
    artifact, population_db
):
    """``counts_reconcile`` is machine-checked, not taken on assertion."""
    report = audit(read_desktop_rows(population_db), artifact, origin="desktop")
    before = _snapshot(population_db)
    report.records.pop()  # the census no longer describes the audited rows
    object.__setattr__(report, "records", report.records)
    # Force a genuine mismatch between the total and the axes.
    report.counts = lambda: {
        "total_observations": len(report.records) + 1,
        "identity_class": {name: 0 for name in IDENTITY_CLASSES},
        "name_class": {name: 0 for name in NAME_CLASSES},
        "proposed_action": {name: 0 for name in ACTIONS},
        "name_loss_population": 0,
    }
    assert report.reconciles() is False

    with pytest.raises(ProductionWriteRefused):
        apply_repairs(
            report,
            observation_db_path=population_db,
            release_id=None,
            gate=_OPEN_GATE,
        )
    assert _snapshot(population_db) == before


# ── A stale artifact must not overwrite a newer identity ────────────────────


def test_a_newer_identity_chosen_after_the_dry_run_is_never_overwritten(
    artifact, tmp_path
):
    """The window between review and apply is where a user keeps working.

    Someone selects a better concept in that window. The archived artifact
    still says "this row has no identity", so a write keyed on the id alone
    would silently discard their choice. Nothing is applied instead — the
    artifact is also the rollback pre-image, so once it stops describing the
    database it must not be used at all.
    """
    db = _observations_db(tmp_path, [
        {"id": 1, "genus": "Entoloma", "species": "conferendum",
         **TaxonIdentity.from_prefixed_external_id("NBIC:53482").to_row()},
        {"id": 2, "genus": "Conocybe", "species": "rugosa",
         **TaxonIdentity.from_prefixed_external_id("NBIC:52369").to_row()},
    ])
    report = audit(read_desktop_rows(db), artifact, origin="desktop")
    assert len(report.repairable()) == 2

    # The user picks a concept for observation 2 after the review.
    chosen = TaxonIdentity.from_taxonomy_v2_artifact(53482).to_row()
    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE observations SET "
        + ", ".join(f"{name} = ?" for name in chosen)
        + " WHERE id = 2",
        tuple(chosen.values()),
    )
    conn.commit()
    conn.close()
    interrupted = _snapshot(db)

    with pytest.raises(StaleAuditArtifact) as excinfo:
        apply_repairs(
            report, observation_db_path=db, release_id=None, gate=_OPEN_GATE
        )
    assert "changed after the dry run" in str(excinfo.value)

    # Their choice survives, and observation 1 was rolled back with it.
    assert _snapshot(db) == interrupted
    surviving = TaxonIdentity.from_row(
        next(row for row in _snapshot(db) if row["id"] == 2)
    )
    assert surviving.sporely_taxon_id == 53482


def test_replaying_the_same_artifact_is_refused_rather_than_reapplied(
    artifact, population_db
):
    """Idempotence has two halves, and this is the one easy to get wrong.

    A fresh audit over a repaired database proposes nothing. Replaying the
    *already-applied* artifact is a different act, and the pre-image guard
    refuses it — an operator who re-runs the apply step by accident gets an
    error, not a silent second write.
    """
    report = audit(read_desktop_rows(population_db), artifact, origin="desktop")
    apply_repairs(
        report, observation_db_path=population_db, release_id=None, gate=_OPEN_GATE
    )
    settled = _snapshot(population_db)

    with pytest.raises(StaleAuditArtifact):
        apply_repairs(
            report, observation_db_path=population_db, release_id=None,
            gate=_OPEN_GATE,
        )
    assert _snapshot(population_db) == settled


def test_a_null_identity_pre_image_still_matches(artifact, tmp_path):
    """Regression guard for the guard: ``= NULL`` is never true.

    Almost every repairable row has NULL in most identity columns, so a
    pre-image check written with ``=`` instead of ``IS`` would abort the
    entire repair population rather than protect it.
    """
    db = _observations_db(tmp_path, [
        {"id": 1, "ai_selected_taxon_id": "NBIC:53482",
         "ai_selected_scientific_name": "Entoloma conferendum"},
    ])
    report = audit(read_desktop_rows(db), artifact, origin="desktop")
    assert all(
        value is None for value in report.records[0].stored_identity.values()
    )
    stats = apply_repairs(
        report, observation_db_path=db, release_id=None, gate=_OPEN_GATE
    )
    assert stats.identities_bound == 1


# ── Part B gate ─────────────────────────────────────────────────────────────


def test_the_production_gate_fails_closed():
    gate = ProductionMigrationGate()
    assert gate.is_open is False
    assert len(gate.blocking_reasons()) == 5
    with pytest.raises(ProductionWriteRefused) as excinfo:
        require_open_gate(gate)
    assert "NEEDS_YOU" in str(excinfo.value)


@pytest.mark.parametrize("withheld", [
    "dry_run_artifact_reviewed",
    "counts_reconcile",
    "candidate_release_validated",
    "rollback_procedure_documented",
    "integrity_checks_defined",
])
def test_any_single_missing_condition_closes_the_gate(withheld):
    conditions = {
        "dry_run_artifact_reviewed": True,
        "counts_reconcile": True,
        "candidate_release_validated": True,
        "rollback_procedure_documented": True,
        "integrity_checks_defined": True,
    }
    conditions[withheld] = False
    gate = ProductionMigrationGate(**conditions)
    assert gate.is_open is False
    with pytest.raises(ProductionWriteRefused):
        require_open_gate(gate)


def test_a_fully_satisfied_gate_opens():
    gate = ProductionMigrationGate(
        dry_run_artifact_reviewed=True,
        counts_reconcile=True,
        candidate_release_validated=True,
        rollback_procedure_documented=True,
        integrity_checks_defined=True,
        evidence=("stage4-observation-identity-audit.json",),
    )
    assert gate.is_open is True
    require_open_gate(gate)


def test_cloud_selected_unverified_identity_is_its_own_report_only_class(artifact, tmp_path):
    """A Sporely ID adopted from the cloud selection on pull carries its own
    Sporely-namespace tuple. That tuple is not independent evidence, so the
    audit must not run it through the external-bridge ladder and "re-derive"
    a proven identity from the value itself."""
    rows = [{
        "id": 1, "genus": "Conocybe", "species": "rugosa",
        "scientific_name_snapshot": "Conocybe rugosa", "taxon_rank_snapshot": "species",
        **TaxonIdentity.from_cloud_selection(
            83668, local_release_id="tax-2026.09.23-01",
            scientific_name="Conocybe rugosa", rank="species",
        ).to_row(),
    }]
    db = _observations_db(tmp_path, rows)
    [record] = audit(read_desktop_rows(db), artifact, origin="desktop").records
    assert record.identity_class == "cloud_selected_unverified_identity"
    assert record.candidate_sporely_taxon_id is None
    assert record.proposed_action == "report_only"
