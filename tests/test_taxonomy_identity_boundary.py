"""Taxonomy-v2 closeout Stage 2 Part A — desktop identity boundary.

Regressions for the rule that a Sporely-owned ``sporely_taxon_id`` may be
assigned only when a taxonomy artifact proves the integer is a Sporely ID, or
a namespaced external identifier has been explicitly resolved through an
authoritative mapping. Everything else keeps its source evidence and stays
unresolved.

The cloud-sync half of the boundary lives in
``tests/test_cloud_taxonomy_identity_sync.py``; the backfill half in
``database/taxonomy/tests/test_stage_3b_2.py``.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import sqlite3
from pathlib import Path

import pytest

from utils.taxon_identity import (
    IDENTITY_COLUMNS,
    PROOF_EXTERNAL_ID_RESOLUTION,
    PROOF_LEGACY_UNVERIFIED,
    PROOF_TAXONOMY_V2_ARTIFACT,
    STATE_EXTERNAL_UNRESOLVED,
    STATE_MANUAL_UNRESOLVED,
    STATE_NONE,
    STATE_SPORELY,
    TaxonIdentity,
    parse_prefixed_external_id,
    proven_sporely_taxon_id,
)


# ── The typed identity object ───────────────────────────────────────────────


def test_artifact_selection_is_the_only_implicitly_proven_source():
    """Regression #1: a genuine v2 Sporely selection remains stable."""
    identity = TaxonIdentity.from_taxonomy_v2_artifact(
        83668, scientific_name="Conocybe rugosa", rank="species",
    )
    assert identity.state == STATE_SPORELY
    assert identity.sporely_taxon_id == 83668
    assert identity.identity_proof == PROOF_TAXONOMY_V2_ARTIFACT
    assert identity.is_proven_sporely is True
    # Sporely's own namespace is recorded rather than left blank, so a
    # downstream reader never has to guess which registry the integer is in.
    assert (identity.source_system, identity.namespace) == (
        "sporely", "sporely_taxon_id",
    )
    assert identity.to_row()["sporely_taxon_id"] == 83668


@pytest.mark.parametrize("bad", [None, 0, -1, "", "abc"])
def test_artifact_selection_degrades_instead_of_fabricating(bad):
    identity = TaxonIdentity.from_taxonomy_v2_artifact(bad)
    assert identity.state == STATE_NONE
    assert identity.is_proven_sporely is False


def test_unresolved_external_never_carries_a_sporely_id():
    """Regression #3: NorTaxa 52369 stays a NorTaxa identity until resolved."""
    identity = TaxonIdentity.unresolved_external(
        source_system="nortaxa",
        namespace="nortaxa_taxon_id",
        external_id="52369",
        scientific_name="Pholiotina rugosa",
        rank="species",
    )
    assert identity.state == STATE_EXTERNAL_UNRESOLVED
    assert identity.sporely_taxon_id is None
    assert identity.is_proven_sporely is False
    assert identity.is_unresolved is True
    assert identity.has_external_evidence is True
    assert (
        identity.source_system, identity.namespace, identity.external_id,
    ) == ("nortaxa", "nortaxa_taxon_id", "52369")
    # The source prediction survives the failure to resolve.
    assert identity.scientific_name == "Pholiotina rugosa"


def test_numerically_colliding_external_integer_does_not_bind():
    """Regression #2: a collision with a real Sporely ID is not identity.

    ``83668`` is a real Sporely concept (``Conocybe rugosa``). The same digits
    arriving as a NorTaxa external identifier describe a different registry
    and must not bind, however convenient the coincidence.
    """
    proven = TaxonIdentity.from_taxonomy_v2_artifact(83668)
    colliding = TaxonIdentity.unresolved_external(
        source_system="nortaxa",
        namespace="nortaxa_taxon_id",
        external_id="83668",
    )
    assert proven.is_proven_sporely is True
    assert colliding.is_proven_sporely is False
    assert colliding.sporely_taxon_id is None
    assert colliding.to_row()["sporely_taxon_id"] is None
    assert proven_sporely_taxon_id(colliding.to_row()) is None
    assert proven_sporely_taxon_id(proven.to_row()) == 83668


def test_manual_text_never_creates_identity():
    """Regression #5: typed scientific text is a name, not an identity."""
    identity = TaxonIdentity.manual_text("Entoloma conferendum")
    assert identity.state == STATE_MANUAL_UNRESOLVED
    assert identity.sporely_taxon_id is None
    assert identity.is_proven_sporely is False
    assert identity.has_external_evidence is False
    assert identity.scientific_name == "Entoloma conferendum"
    assert TaxonIdentity.manual_text("   ").state == STATE_NONE


def test_failed_resolution_preserves_the_source_evidence():
    """Regression #4: failure to resolve is a state, not an absence."""
    identity = TaxonIdentity.unresolved_external(
        source_system="nortaxa",
        namespace="nortaxa_taxon_id",
        external_id="53482",
        raw_external_id="NBIC:53482",
        scientific_name="Entoloma conferendum",
    )
    # Resolution attempted and failed (no Sporely ID came back).
    still_unresolved = identity.resolved_to(None)
    assert still_unresolved == identity
    assert still_unresolved.external_id == "53482"
    assert still_unresolved.raw_external_id == "NBIC:53482"
    assert still_unresolved.scientific_name == "Entoloma conferendum"


def test_successful_resolution_records_its_proof_and_keeps_the_source():
    identity = TaxonIdentity.unresolved_external(
        source_system="nortaxa",
        namespace="nortaxa_taxon_id",
        external_id="53482",
        raw_external_id="NBIC:53482",
    ).resolved_to(7821, provenance="tax-2026.08.01-01")
    assert identity.state == STATE_SPORELY
    assert identity.sporely_taxon_id == 7821
    assert identity.identity_proof == PROOF_EXTERNAL_ID_RESOLUTION
    assert identity.is_proven_sporely is True
    # The resolution stays auditable: the source tuple is retained.
    assert identity.namespace == "nortaxa_taxon_id"
    assert identity.external_id == "53482"
    assert identity.raw_external_id == "NBIC:53482"
    # In-memory only. This assertion passed for a build whose `to_row()`
    # dropped provenance entirely, so persistence is proven separately by
    # `test_release_provenance_survives_save_and_reload`.
    assert identity.provenance == "tax-2026.08.01-01"


def test_manual_text_cannot_be_resolved_without_external_evidence():
    """Only a namespaced identifier is resolvable — a name is not."""
    assert TaxonIdentity.manual_text("Amanita muscaria").resolved_to(
        1234,
    ).is_proven_sporely is False


# ── Prefixed provider identifiers ───────────────────────────────────────────


def test_nbic_identifier_parses_into_its_own_namespace_and_keeps_the_raw():
    parsed = parse_prefixed_external_id("NBIC:53482")
    assert parsed is not None
    # identity-contract.md: an Artsorakel NBIC: id is a scientific-NAME id.
    assert parsed.source_system == "artsorakel"
    assert parsed.namespace == "nbic_scientific_name_id"
    assert parsed.local_id == "53482"
    assert parsed.raw == "NBIC:53482"


def test_nbic_bridges_to_nortaxa_taxon_id_under_a_declared_bridge():
    """The plan's required tuple, reached without skipping the contract.

    ``identity-contract.md`` declares that NorTaxa's ``dwc:taxonID`` values
    ARE Artsnavnebase scientific-name IDs — the registry Artsorakel returns
    under ``NBIC:`` — so the hop is evidenced rather than a silent strip.
    """
    parsed = parse_prefixed_external_id("NBIC:53482")
    bridged = parsed.bridged()
    assert (bridged.source_system, bridged.namespace, bridged.local_id) == (
        "nortaxa", "nortaxa_taxon_id", "53482",
    )
    assert bridged.raw == "NBIC:53482"
    assert "Artsnavnebase" in parsed.bridge_evidence

    identity = TaxonIdentity.from_prefixed_external_id("NBIC:53482")
    assert (
        identity.source_system, identity.namespace, identity.external_id,
    ) == ("nortaxa", "nortaxa_taxon_id", "53482")
    assert identity.raw_external_id == "NBIC:53482"
    # Bridging a namespace is not resolving an identity.
    assert identity.is_proven_sporely is False
    assert identity.state == STATE_EXTERNAL_UNRESOLVED


@pytest.mark.parametrize("value", ["53482", "", None, "   ", "ZZZZ:53482", "NBIC:"])
def test_a_namespace_lost_or_unknown_identifier_is_not_parsed(value):
    """A bare integer has no registry, so accepting one invents a namespace."""
    assert parse_prefixed_external_id(value) is None
    assert TaxonIdentity.from_prefixed_external_id(value).state == STATE_NONE


def test_nbic_is_never_bridged_to_the_taxon_concept_registry():
    """The two Artsdatabanken registries must not be conflated.

    ``artsnavnebase_scientific_name_id`` and
    ``artsdatabanken_taxon_concept_id`` are independent registries whose
    numeric equality is coincidence. Only the declared bridge exists.
    """
    parsed = parse_prefixed_external_id("NBIC:53482")
    assert parsed.bridged().namespace != "artsdatabanken_taxon_concept_id"
    identity = TaxonIdentity.from_prefixed_external_id("NBIC:53482", bridge=False)
    assert identity.namespace == "nbic_scientific_name_id"
    assert identity.external_id == "53482"
    assert identity.raw_external_id == "NBIC:53482"


# ── Reading persisted rows ──────────────────────────────────────────────────


def test_legacy_row_without_provenance_is_unverified_not_proven():
    identity = TaxonIdentity.from_row({"sporely_taxon_id": 83668})
    assert identity.state == STATE_SPORELY
    assert identity.identity_proof == PROOF_LEGACY_UNVERIFIED
    assert identity.is_legacy_unverified is True
    assert identity.is_proven_sporely is False
    # The integer is not destroyed — it is real data awaiting re-verification.
    assert identity.to_row()["sporely_taxon_id"] == 83668


def test_empty_row_is_no_identity_evidence():
    for row in (None, {}, {"sporely_taxon_id": None}):
        assert TaxonIdentity.from_row(row).state == STATE_NONE


def test_sporely_state_without_proof_falls_back_to_source_evidence():
    """A corrupt Sporely state is not trusted on the strength of its integer."""
    identity = TaxonIdentity.from_row({
        "sporely_taxon_id": 83668,
        "taxon_identity_state": STATE_SPORELY,
        "taxon_identity_proof": "something-made-up",
        "taxon_identity_source_system": "nortaxa",
        "taxon_identity_namespace": "nortaxa_taxon_id",
        "taxon_identity_external_id": "52369",
    })
    assert identity.state == STATE_EXTERNAL_UNRESOLVED
    assert identity.sporely_taxon_id is None
    assert identity.external_id == "52369"


def test_unknown_state_is_rejected_rather_than_guessed():
    assert TaxonIdentity.from_row({
        "sporely_taxon_id": 83668,
        "taxon_identity_state": "not-a-state",
    }).state == STATE_NONE


# ── Persistence round-trip (regression #8) ──────────────────────────────────


def _fresh_db(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "obs.sqlite3"
    from database import schema as _schema
    monkeypatch.setattr(_schema, "get_database_path", lambda: db_path)
    _schema.init_database()
    from database.models import ObservationDB
    return ObservationDB, db_path


def _read_identity_row(db_path: Path, obs_id: int) -> dict:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT sporely_taxon_id, scientific_name_snapshot, taxon_rank_snapshot, "
        + ", ".join(IDENTITY_COLUMNS)
        + " FROM observations WHERE id = ?",
        (obs_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else {}


def test_schema_creates_the_identity_provenance_columns(tmp_path, monkeypatch):
    _db, path = _fresh_db(tmp_path, monkeypatch)
    conn = sqlite3.connect(str(path))
    columns = {r[1] for r in conn.execute("PRAGMA table_info(observations)")}
    conn.close()
    assert set(IDENTITY_COLUMNS) <= columns


def test_external_identity_round_trips_through_create_and_reload(tmp_path, monkeypatch):
    """Regression #8: save/reload preserves source + namespace + external ID."""
    db, path = _fresh_db(tmp_path, monkeypatch)
    identity = TaxonIdentity.from_prefixed_external_id(
        "NBIC:53482", scientific_name="Entoloma conferendum", rank="species",
    )
    obs_id = db.create_observation(
        date="2026-09-22 12:00",
        genus="Entoloma",
        species="conferendum",
        scientific_name_snapshot="Entoloma conferendum",
        taxon_rank_snapshot="species",
        **identity.to_row(),
    )
    stored = _read_identity_row(path, obs_id)
    assert stored["sporely_taxon_id"] is None
    assert stored["taxon_identity_state"] == STATE_EXTERNAL_UNRESOLVED
    assert stored["taxon_identity_source_system"] == "nortaxa"
    assert stored["taxon_identity_namespace"] == "nortaxa_taxon_id"
    assert stored["taxon_identity_external_id"] == "53482"
    assert stored["taxon_identity_raw_external_id"] == "NBIC:53482"

    # Reloading reconstructs the same identity, so a restart cannot downgrade
    # an unresolved external identity into "no identification".
    reloaded = TaxonIdentity.from_row(stored)
    assert reloaded.state == STATE_EXTERNAL_UNRESOLVED
    assert reloaded.has_external_evidence is True
    assert reloaded.external_id == "53482"
    assert reloaded.raw_external_id == "NBIC:53482"
    assert reloaded.scientific_name == "Entoloma conferendum"


def test_proven_identity_round_trips_through_update(tmp_path, monkeypatch):
    db, path = _fresh_db(tmp_path, monkeypatch)
    obs_id = db.create_observation(
        date="2026-09-22 12:00", genus="Conocybe", species="rugosa",
    )
    identity = TaxonIdentity.from_taxonomy_v2_artifact(83668)
    db.update_observation(obs_id, allow_nulls=True, **identity.to_row())
    stored = _read_identity_row(path, obs_id)
    assert stored["sporely_taxon_id"] == 83668
    assert stored["taxon_identity_state"] == STATE_SPORELY
    assert stored["taxon_identity_proof"] == PROOF_TAXONOMY_V2_ARTIFACT
    assert TaxonIdentity.from_row(stored).is_proven_sporely is True


def test_create_refuses_a_sporely_id_under_a_non_sporely_state(tmp_path, monkeypatch):
    """Defence in depth: the row itself cannot contradict its provenance."""
    db, path = _fresh_db(tmp_path, monkeypatch)
    obs_id = db.create_observation(
        date="2026-09-22 12:00",
        genus="Entoloma",
        species="conferendum",
        sporely_taxon_id=7821,
        taxon_identity_state=STATE_EXTERNAL_UNRESOLVED,
        taxon_identity_source_system="nortaxa",
        taxon_identity_namespace="nortaxa_taxon_id",
        taxon_identity_external_id="53482",
    )
    stored = _read_identity_row(path, obs_id)
    assert stored["sporely_taxon_id"] is None
    assert stored["taxon_identity_external_id"] == "53482"


def test_clearing_identity_leaves_other_observation_content_intact(tmp_path, monkeypatch):
    """Regression #6: invalidating identity must not delete unrelated content."""
    db, path = _fresh_db(tmp_path, monkeypatch)
    identity = TaxonIdentity.from_taxonomy_v2_artifact(83668)
    obs_id = db.create_observation(
        date="2026-09-22 12:00",
        genus="Conocybe",
        species="rugosa",
        common_name="slank ringkjeglesopp",
        location="Trondheim",
        notes="under bjørk",
        ai_selected_service="artsorakel",
        ai_selected_taxon_id="NBIC:53482",
        ai_selected_scientific_name="Entoloma conferendum",
        scientific_name_snapshot="Conocybe rugosa",
        taxon_rank_snapshot="species",
        **identity.to_row(),
    )
    # The controller's invalidation path: identity goes, the rest stays.
    cleared = TaxonIdentity.none()
    db.update_observation(
        obs_id,
        scientific_name_snapshot=None,
        taxon_rank_snapshot=None,
        allow_nulls=True,
        **cleared.to_row(),
    )
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    row = dict(conn.execute(
        "SELECT * FROM observations WHERE id = ?", (obs_id,)
    ).fetchone())
    conn.close()
    assert row["sporely_taxon_id"] is None
    assert row["taxon_identity_state"] is None
    # Untouched observation content.
    assert row["genus"] == "Conocybe"
    assert row["species"] == "rugosa"
    assert row["common_name"] == "slank ringkjeglesopp"
    assert row["location"] == "Trondheim"
    assert row["notes"] == "under bjørk"
    # The AI-identification history is a separate record and survives.
    assert row["ai_selected_taxon_id"] == "NBIC:53482"
    assert row["ai_selected_scientific_name"] == "Entoloma conferendum"


# ── Controller snapshot behaviour ────────────────────────────────────────────


@pytest.fixture
def controller():
    from PySide6.QtWidgets import QApplication, QLineEdit
    from ui.taxon_input_controller import TaxonInputController
    QApplication.instance() or QApplication([])
    return TaxonInputController(
        lookup=None,
        genus_input=QLineEdit(),
        species_input=QLineEdit(),
        scientific_name_input=QLineEdit(),
    )


def test_controller_picker_snapshot_records_artifact_proof(controller):
    controller.genus_input.setText("Conocybe")
    controller.species_input.setText("rugosa")
    # An explicit selection, as the picker commits it. `commit_manual_resolution`
    # was removed by Stage 2 (it minted artifact proof from a NAME match), so a
    # committed selection is represented by its already-typed identity.
    controller.load_committed_snapshot({
        "genus": "Conocybe",
        "species": "rugosa",
        "scientific_name": "Conocybe rugosa",
        "taxon_rank_snapshot": "species",
        **TaxonIdentity.from_taxonomy_v2_artifact(83668).to_row(),
    })
    snapshot = controller.committed_snapshot()
    assert snapshot["sporely_taxon_id"] == 83668
    assert snapshot["taxon_identity_state"] == STATE_SPORELY
    assert snapshot["taxon_identity_proof"] == PROOF_TAXONOMY_V2_ARTIFACT
    identity = controller.committed_identity()
    assert identity.is_proven_sporely is True
    assert identity.sporely_taxon_id == 83668


def test_controller_holds_an_unresolved_external_identity(controller):
    identity = TaxonIdentity.from_prefixed_external_id(
        "NBIC:53482", scientific_name="Entoloma conferendum", rank="species",
    )
    assert controller.commit_external_identity(
        identity, genus="Entoloma", species="conferendum",
    ) is True
    snapshot = controller.committed_snapshot()
    assert snapshot["sporely_taxon_id"] is None
    assert snapshot["taxon_identity_state"] == STATE_EXTERNAL_UNRESOLVED
    assert snapshot["taxon_identity_external_id"] == "53482"
    assert snapshot["taxon_identity_raw_external_id"] == "NBIC:53482"
    assert controller.committed_identity().is_proven_sporely is False


def test_controller_refuses_to_smuggle_a_proven_identity_through_the_external_path(controller):
    assert controller.commit_external_identity(
        TaxonIdentity.from_taxonomy_v2_artifact(83668),
    ) is False
    assert controller.commit_external_identity(
        TaxonIdentity.manual_text("Amanita muscaria"),
    ) is False
    assert controller.committed_snapshot() is None


def test_controller_reload_preserves_external_provenance(controller):
    stored = TaxonIdentity.from_prefixed_external_id("NBIC:53482").to_row()
    controller.load_committed_snapshot({
        "genus": "Entoloma",
        "species": "conferendum",
        "scientific_name": "Entoloma conferendum",
        "taxon_rank_snapshot": "species",
        **stored,
    })
    identity = controller.committed_identity()
    assert identity.state == STATE_EXTERNAL_UNRESOLVED
    assert identity.external_id == "53482"
    assert identity.raw_external_id == "NBIC:53482"
    assert identity.sporely_taxon_id is None


def test_controller_reload_downgrades_a_bare_legacy_integer(controller):
    controller.load_committed_snapshot({
        "genus": "Conocybe",
        "species": "rugosa",
        "scientific_name": "Conocybe rugosa",
        "taxon_rank_snapshot": "species",
        "sporely_taxon_id": 83668,
    })
    identity = controller.committed_identity()
    assert identity.is_legacy_unverified is True
    assert identity.is_proven_sporely is False


def test_editing_committed_text_invalidates_identity(controller):
    """Regression #6, controller half: no rebinding from edited text."""
    controller.genus_input.setText("Conocybe")
    controller.species_input.setText("rugosa")
    # An explicit selection, as the picker commits it. `commit_manual_resolution`
    # was removed by Stage 2 (it minted artifact proof from a NAME match), so a
    # committed selection is represented by its already-typed identity.
    controller.load_committed_snapshot({
        "genus": "Conocybe",
        "species": "rugosa",
        "scientific_name": "Conocybe rugosa",
        "taxon_rank_snapshot": "species",
        **TaxonIdentity.from_taxonomy_v2_artifact(83668).to_row(),
    })
    assert controller.committed_identity().is_proven_sporely is True

    controller.species_input.setText("rugosax")
    controller._on_structured_text_changed("rugosax")

    assert controller.committed_snapshot() is None
    assert controller.committed_identity().state == STATE_NONE
    # The user's own text is not deleted by invalidation.
    assert controller.genus_input.text() == "Conocybe"
    assert controller.species_input.text() == "rugosax"


# ── Provider-response paths (desktop) ───────────────────────────────────────


def test_desktop_rejects_the_deprecation_sentinel_at_both_nesting_levels():
    """Taxonomy-v2 closeout Stage 2: the desktop flattener is hardened too.

    The pre-existing check looked only at ``item["taxon"]["vernacularName"]``,
    which matches this repository's own fixture shape. ``sporely-web``'s
    fixtures show flattened ``taxa.items[]`` candidates carrying the
    vernacular bare at the top level, and for those the nested lookup yields
    nothing — so a superseded record would survive.
    """
    from ui.image_import_dialog import AIGuessWorker
    sentinel = AIGuessWorker._ARTSORAKEL_OUTDATED_SENTINEL

    assert AIGuessWorker._is_deprecated_artsorakel_candidate(
        {"taxon": {"vernacularName": sentinel}},
    ) is True
    assert AIGuessWorker._is_deprecated_artsorakel_candidate(
        {"vernacularName": sentinel, "scientific_name_id": "NBIC:99999"},
    ) is True
    assert AIGuessWorker._is_deprecated_artsorakel_candidate(
        {"vernacular_name": f"  {sentinel}  "},
    ) is True
    # Real candidates are untouched.
    assert AIGuessWorker._is_deprecated_artsorakel_candidate(
        {"scientificName": "Entoloma conferendum", "vernacularName": "stjernesporet rødspore"},
    ) is False
    assert AIGuessWorker._is_deprecated_artsorakel_candidate(None) is False
    assert AIGuessWorker._is_deprecated_artsorakel_candidate({}) is False


def test_desktop_flattener_drops_a_bare_sentinel_item():
    from ui.image_import_dialog import AIGuessWorker
    sentinel = AIGuessWorker._ARTSORAKEL_OUTDATED_SENTINEL
    payload = {
        "predictions": [
            {
                "taxa": {
                    "items": [
                        {
                            "scientific_name_id": "NBIC:99999",
                            "scientificName": "Superseded record",
                            "probability": 0.99,
                            "vernacularName": sentinel,
                        },
                        {
                            "scientific_name_id": "NBIC:53482",
                            "scientificName": "Entoloma conferendum",
                            "probability": 0.93,
                            "taxon": {"vernacularName": "stjernesporet rødspore"},
                        },
                    ],
                },
            },
        ],
    }
    flattened = AIGuessWorker._flatten_artsorakel_predictions(payload)
    assert [p["scientific_name_id"] for p in flattened] == ["NBIC:53482"]


def test_copying_an_artsorakel_row_preserves_its_namespaced_identifier(controller):
    """Regression #3/#8, desktop producer half.

    Copying an AI suggestion into the identification used to leave the
    observation with no identity record at all: the text write invalidates any
    snapshot, and the provider's ``NBIC:`` identifier was kept only as
    AI-identification history. It is now preserved as an explicitly
    unresolved external identity with no Sporely ID.
    """
    from ui.observations_tab import ObservationDetailsDialog

    class _Host:
        _taxon_controller = controller
        _preserve_ai_external_taxon_identity = (
            ObservationDetailsDialog._preserve_ai_external_taxon_identity
        )

    host = _Host()
    assert host._preserve_ai_external_taxon_identity(
        {"id": "NBIC:53482"}, "Entoloma", "conferendum",
    ) is True

    identity = controller.committed_identity()
    assert identity.state == STATE_EXTERNAL_UNRESOLVED
    assert identity.sporely_taxon_id is None
    assert (identity.source_system, identity.namespace, identity.external_id) == (
        "nortaxa", "nortaxa_taxon_id", "53482",
    )
    assert identity.raw_external_id == "NBIC:53482"
    assert identity.scientific_name == "Entoloma conferendum"
    # Nothing proven means nothing reaches the cloud RPC.
    assert identity.is_proven_sporely is False
    assert proven_sporely_taxon_id(controller.committed_snapshot()) is None


def test_copying_a_bare_integer_provider_id_preserves_nothing(controller):
    """A namespace-lost integer is audit evidence, not an identity.

    iNaturalist ids arrive as bare integers. Inventing a namespace for one is
    exactly the defect this stage closes, so nothing is committed.
    """
    from ui.observations_tab import ObservationDetailsDialog

    class _Host:
        _taxon_controller = controller
        _preserve_ai_external_taxon_identity = (
            ObservationDetailsDialog._preserve_ai_external_taxon_identity
        )

    host = _Host()
    for raw in ("47347", "", None, "ZZZZ:47347"):
        assert host._preserve_ai_external_taxon_identity(
            {"id": raw}, "Amanita", "muscaria",
        ) is False
    assert controller.committed_snapshot() is None


def test_explicit_unidentified_clears_identity_and_its_provenance(tmp_path, monkeypatch):
    """An observer's explicit "unidentified" answers the identity question.

    Distinct from a failure to resolve: leaving a preserved external
    identifier behind would keep the row claiming an outstanding question.
    Raw AI evidence in ``ai_state_json`` is deliberately retained.
    """
    db, path = _fresh_db(tmp_path, monkeypatch)
    identity = TaxonIdentity.from_prefixed_external_id(
        "NBIC:53482", scientific_name="Entoloma conferendum", rank="species",
    )
    obs_id = db.create_observation(
        date="2026-09-22 12:00",
        genus="Entoloma",
        species="conferendum",
        scientific_name_snapshot="Entoloma conferendum",
        taxon_rank_snapshot="species",
        ai_state_json='{"raw": "candidates"}',
        **identity.to_row(),
    )
    db.clear_observation_identification(obs_id)

    stored = _read_identity_row(path, obs_id)
    assert stored["sporely_taxon_id"] is None
    assert stored["scientific_name_snapshot"] is None
    assert stored["taxon_rank_snapshot"] is None
    for column in IDENTITY_COLUMNS:
        assert stored[column] is None, column
    assert TaxonIdentity.from_row(stored).state == STATE_NONE

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    row = dict(conn.execute(
        "SELECT genus, species, ai_state_json FROM observations WHERE id = ?",
        (obs_id,),
    ).fetchone())
    conn.close()
    assert row["genus"] is None
    assert row["species"] is None
    # Raw AI candidates survive so a wrong selection can be revisited.
    assert row["ai_state_json"] == '{"raw": "candidates"}'


# ── Model write boundary: the integer and its proof move together ──────────


def test_updating_the_integer_alone_never_inherits_the_previous_proof(tmp_path, monkeypatch):
    """The partial integer/proof transition, closed.

    Reproduced before this guard: a row proven at 83668, updated with
    ``sporely_taxon_id=99`` and nothing else, kept proof
    ``taxonomy_v2_artifact`` — so ``TaxonIdentity.from_row`` read the
    REPLACEMENT integer as proven and cloud sync would emit it. Membership,
    ordering and UI gating are all irrelevant if the model layer lets a bare
    integer inherit somebody else's proof.
    """
    db, path = _fresh_db(tmp_path, monkeypatch)
    proven = TaxonIdentity.from_taxonomy_v2_artifact(83668)
    obs_id = db.create_observation(
        date="2026-09-23 12:00", genus="Conocybe", species="rugosa",
        **proven.to_row(),
    )
    assert TaxonIdentity.from_row(_read_identity_row(path, obs_id)).is_proven_sporely is True

    db.update_observation(obs_id, sporely_taxon_id=99)

    stored = _read_identity_row(path, obs_id)
    # The bare integer is REFUSED outright, not merely stored unverified:
    # "make it impossible to assign sporely_taxon_id from an unqualified
    # integer" is the objective, and storing it would still be assigning it.
    assert stored["sporely_taxon_id"] is None
    for column in IDENTITY_COLUMNS:
        assert stored[column] is None, column
    assert TaxonIdentity.from_row(stored).state == STATE_NONE
    assert proven_sporely_taxon_id(stored) is None


def test_create_refuses_a_bare_integer(tmp_path, monkeypatch):
    """An unqualified integer cannot become `sporely_taxon_id` at all.

    An earlier draft stored it and relied on it reading back as
    legacy-unverified. That still assigns the column from an unqualified
    integer, which is what the stage's objective forbids; every downstream
    gate then has to be perfect forever. Refusing at the write boundary means
    there is nothing to gate.

    Pre-existing legacy rows are unaffected — they are already stored, are
    read as legacy-unverified, and are re-verified by
    `database/migrate_observations_sporely_id.py`.
    """
    db, path = _fresh_db(tmp_path, monkeypatch)
    obs_id = db.create_observation(
        date="2026-09-23 12:00", genus="Conocybe", species="rugosa",
        sporely_taxon_id=83668,
    )
    stored = _read_identity_row(path, obs_id)
    assert stored["sporely_taxon_id"] is None
    for column in IDENTITY_COLUMNS:
        assert stored[column] is None, column


def test_writing_the_integer_with_its_proof_together_stays_proven(tmp_path, monkeypatch):
    """The guard must not break a coherent write."""
    db, path = _fresh_db(tmp_path, monkeypatch)
    obs_id = db.create_observation(date="2026-09-23 12:00", genus="Conocybe")
    db.update_observation(
        obs_id, allow_nulls=True,
        **TaxonIdentity.from_taxonomy_v2_artifact(83668).to_row(),
    )
    stored = _read_identity_row(path, obs_id)
    assert proven_sporely_taxon_id(stored) == 83668


def test_update_refuses_an_integer_under_a_non_sporely_state(tmp_path, monkeypatch):
    db, path = _fresh_db(tmp_path, monkeypatch)
    obs_id = db.create_observation(date="2026-09-23 12:00", genus="Entoloma")
    db.update_observation(
        obs_id,
        sporely_taxon_id=7821,
        taxon_identity_state=STATE_EXTERNAL_UNRESOLVED,
        taxon_identity_source_system="nortaxa",
        taxon_identity_namespace="nortaxa_taxon_id",
        taxon_identity_external_id="53482",
    )
    stored = _read_identity_row(path, obs_id)
    assert stored["sporely_taxon_id"] is None
    assert stored["taxon_identity_external_id"] == "53482"


def test_release_provenance_survives_save_and_reload(tmp_path, monkeypatch):
    """The stage requires preserving "relevant release/response provenance if
    available", and a save/reload is where "preserved" is decided.

    Before this column, ``resolved_to(..., provenance=...)`` recorded the
    taxonomy release in memory only: ``to_row()`` omitted it, so the value
    that justified the binding was gone the next time the app started, and the
    row could no longer say WHICH release it was resolved against. The
    assertion has to read the value back out of SQLite — asserting it on the
    in-memory object proves nothing about persistence.
    """
    db, path = _fresh_db(tmp_path, monkeypatch)
    resolved = TaxonIdentity.from_prefixed_external_id(
        "NBIC:53482", scientific_name="Entoloma conferendum", rank="species",
    ).resolved_to(7821, provenance="tax-2026.08.01-01")
    assert resolved.is_proven_sporely is True

    obs_id = db.create_observation(
        date="2026-09-23 12:00", genus="Entoloma", species="conferendum",
        **resolved.to_row(),
    )
    stored = _read_identity_row(path, obs_id)
    assert stored["taxon_identity_provenance"] == "tax-2026.08.01-01"

    reloaded = TaxonIdentity.from_row(stored)
    assert reloaded.provenance == "tax-2026.08.01-01"
    # The provenance rides along with, not instead of, the source evidence.
    assert reloaded.sporely_taxon_id == 7821
    assert reloaded.identity_proof == PROOF_EXTERNAL_ID_RESOLUTION
    assert reloaded.external_id == "53482"
    assert reloaded.raw_external_id == "NBIC:53482"

    # And through the update path, for an identity that is still unresolved:
    # provenance is available for the response that failed to resolve too.
    unresolved = TaxonIdentity.from_prefixed_external_id(
        "NBIC:53482", provenance="artsorakel-2026-09-23T10:11:12Z",
    )
    db.update_observation(obs_id, allow_nulls=True, **unresolved.to_row())
    stored = _read_identity_row(path, obs_id)
    assert stored["taxon_identity_provenance"] == "artsorakel-2026-09-23T10:11:12Z"
    assert TaxonIdentity.from_row(stored).state == STATE_EXTERNAL_UNRESOLVED


def test_integer_plus_one_provenance_field_cannot_ride_a_proven_row(tmp_path, monkeypatch):
    """Partial transition A: new integer + ONE provenance field, proven row.

    `test_updating_the_integer_alone_never_inherits_the_previous_proof`
    covers the integer supplied alone. This is the neighbouring bypass: the
    caller supplies the integer AND one provenance field, which is enough to
    look like a deliberate identity write while leaving the proof column
    untouched. Reproduced against the pre-guard model layer as
    ``99 | sporely_v2 | taxonomy_v2_artifact | PROVEN=99`` — the replacement
    integer wearing the previous concept's proof.

    Each provenance field is exercised separately: a guard that only inspects
    the proof column would pass the first case and fail the rest.
    """
    for field in (
        "taxon_identity_source_system",
        "taxon_identity_namespace",
        "taxon_identity_external_id",
        "taxon_identity_raw_external_id",
        "taxon_identity_provenance",
    ):
        db, path = _fresh_db(tmp_path, monkeypatch)
        obs_id = db.create_observation(
            date="2026-09-23 12:00", genus="Conocybe", species="rugosa",
            **TaxonIdentity.from_taxonomy_v2_artifact(83668).to_row(),
        )
        assert proven_sporely_taxon_id(_read_identity_row(path, obs_id)) == 83668

        db.update_observation(obs_id, sporely_taxon_id=99, **{field: "nortaxa"})

        stored = _read_identity_row(path, obs_id)
        # 99 has no proof of its own and inherits none: the whole identity is
        # rewritten from what this write supplied, and what it supplied does
        # not license a Sporely ID.
        assert stored["sporely_taxon_id"] is None, field
        assert stored["taxon_identity_proof"] is None, field
        assert stored["taxon_identity_state"] != STATE_SPORELY, field
        assert proven_sporely_taxon_id(stored) is None, field


def test_external_provenance_without_the_integer_clears_a_proven_row(tmp_path, monkeypatch):
    """Partial transition B: provenance changes, integer not mentioned.

    The mirror image of A, and the reason the guard cannot simply be "reject
    bare integers". Moving a proven row to ``external_unresolved`` without
    naming ``sporely_taxon_id`` used to leave BOTH the previous integer and
    the previous proof in place — reproduced as
    ``83668 | external_unresolved | taxonomy_v2_artifact``, a row that
    contradicts itself and still reads as proven.

    The existing external-transition test starts from a blank row, where
    there is nothing stale to inherit, so it cannot catch this.
    """
    db, path = _fresh_db(tmp_path, monkeypatch)
    obs_id = db.create_observation(
        date="2026-09-23 12:00", genus="Conocybe", species="rugosa",
        **TaxonIdentity.from_taxonomy_v2_artifact(83668).to_row(),
    )
    assert proven_sporely_taxon_id(_read_identity_row(path, obs_id)) == 83668

    db.update_observation(
        obs_id,
        taxon_identity_state=STATE_EXTERNAL_UNRESOLVED,
        taxon_identity_source_system="nortaxa",
        taxon_identity_namespace="nortaxa_taxon_id",
        taxon_identity_external_id="53482",
        taxon_identity_raw_external_id="NBIC:53482",
        taxon_identity_provenance="artsorakel-2026-09-23T10:11:12Z",
    )

    stored = _read_identity_row(path, obs_id)
    assert stored["sporely_taxon_id"] is None
    assert stored["taxon_identity_proof"] is None
    assert stored["taxon_identity_state"] == STATE_EXTERNAL_UNRESOLVED
    assert stored["taxon_identity_external_id"] == "53482"
    assert stored["taxon_identity_provenance"] == "artsorakel-2026-09-23T10:11:12Z"
    assert proven_sporely_taxon_id(stored) is None
    # The new evidence is not destroyed by the clearing of the old identity.
    assert TaxonIdentity.from_row(stored).has_external_evidence is True


# ── Identity-bearing consumers are gated on proof ──────────────────────────


def test_reference_attach_refuses_an_unverified_collision():
    """`MainWindow._active_sporely_taxon_id` drives normalized reference
    attachment, so it must read proven provenance rather than the raw column.
    ``83668`` is a real Sporely concept, so a legacy-unverified row holding
    those digits is exactly the collision that must not attach."""
    from ui.main_window import MainWindow

    legacy = {"id": 1, "sporely_taxon_id": 83668}
    assert proven_sporely_taxon_id(legacy) is None
    assert MainWindow._active_sporely_taxon_id(object(), legacy) is None

    proven = {"id": 1, **TaxonIdentity.from_taxonomy_v2_artifact(83668).to_row()}
    assert MainWindow._active_sporely_taxon_id(object(), proven) == 83668

    external = {"id": 1, **TaxonIdentity.unresolved_external(
        source_system="nortaxa", namespace="nortaxa_taxon_id", external_id="83668",
    ).to_row()}
    assert MainWindow._active_sporely_taxon_id(object(), external) is None


def test_the_removed_name_binder_is_gone():
    """`commit_manual_resolution` minted artifact proof from a NAME match.

    The editing-finished handler stopped calling it, but the callable
    boundary itself encoded the forbidden transition and several tests still
    exercised it. Identity now binds only through an explicit selection.
    """
    from ui.taxon_input_controller import TaxonInputController

    assert not hasattr(TaxonInputController, "commit_manual_resolution")
    # The display-only channel remains, and it cannot produce identity.
    assert hasattr(TaxonInputController, "set_display_only_name_match")
