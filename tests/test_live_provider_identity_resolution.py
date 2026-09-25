"""Live provider identity resolution through taxonomy-v2 (0.9.23).

The original closeout regression: an Artsorakel copy of ``NBIC:53482`` /
*Entoloma conferendum* was recorded as an unresolved (or lost) identity even
though the taxonomy knows the concept. The live identification path now
offers the provider's namespaced tuple to the installed artifact's
authoritative bridge table; an exact mapping commits the canonical concept
with ``external_id_resolution`` proof, and anything else stays
``external_unresolved`` with the provider's name and identifier intact.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from database.taxon_lookup import (
    TaxonLookupService,
    resolve_installed_external_identity,
)
from database.vernacular_db import VernacularDB
from utils import taxonomy_v2
from utils.taxon_identity import (
    PROOF_EXTERNAL_ID_RESOLUTION,
    STATE_EXTERNAL_UNRESOLVED,
    STATE_SPORELY,
    TaxonIdentity,
)

NORTAXA = dict(source_system="nortaxa", namespace="nortaxa_taxon_id")


# ── The resolver, on a synthetic artifact ────────────────────────────────────


@pytest.fixture
def synthetic_artifact(tmp_path: Path) -> Path:
    path = tmp_path / "artifact.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT);
        INSERT INTO taxonomy_meta VALUES ('taxonomy_schema_version', '2'),
                                         ('content_release_id', 'tax-test-01');
        CREATE TABLE taxon_min (taxon_id INTEGER PRIMARY KEY,
                                canonical_scientific_name TEXT, taxon_rank TEXT);
        INSERT INTO taxon_min VALUES (7821, 'Entoloma conferendum', 'species'),
                                     (4242, 'Amanita muscaria', 'species'),
                                     (100, 'Alpha one', 'species'),
                                     (101, 'Alpha two', 'species');
        CREATE TABLE taxon_external_id_text_min (taxon_id INTEGER, source_system TEXT,
                                                 namespace TEXT, external_id TEXT);
        INSERT INTO taxon_external_id_text_min VALUES
            (7821, 'nortaxa', 'nortaxa_taxon_id', '53482'),
            (100, 'nortaxa', 'nortaxa_taxon_id', '77777'),
            (101, 'nortaxa', 'nortaxa_taxon_id', '77777'),
            (9999, 'nortaxa', 'nortaxa_taxon_id', '88888');
        -- The namespace-lost legacy table must never be consulted.
        CREATE TABLE taxon_external_id_min (taxon_id INTEGER, source_system TEXT,
                                            external_id INTEGER);
        INSERT INTO taxon_external_id_min VALUES (7821, 'artsdatabanken', 11111);
        """
    )
    conn.commit()
    conn.close()
    return path


def test_exact_bridge_resolves_to_its_concept(synthetic_artifact):
    concept = resolve_installed_external_identity(synthetic_artifact, external_id="53482", **NORTAXA)
    assert concept is not None
    assert (concept.sporely_taxon_id, concept.scientific_name, concept.release_id) == (
        7821, "Entoloma conferendum", "tax-test-01")


@pytest.mark.parametrize("external_id, why", [
    ("99999", "no mapping"),
    ("4242", "numeric collision with a real concept id"),
    ("11111", "only the legacy namespace-lost integer table has it"),
    ("77777", "ambiguous: two concepts"),
    ("88888", "maps to a concept outside the release"),
])
def test_anything_short_of_one_exact_member_concept_is_unresolved(synthetic_artifact, external_id, why):
    assert resolve_installed_external_identity(
        synthetic_artifact, external_id=external_id, **NORTAXA) is None, why


def test_wrong_namespace_does_not_resolve(synthetic_artifact):
    assert resolve_installed_external_identity(
        synthetic_artifact, source_system="nortaxa", namespace="nortaxa_dwc_id",
        external_id="53482") is None


def test_pre_v2_databases_resolve_nothing(tmp_path, synthetic_artifact):
    legacy = tmp_path / "legacy.sqlite3"
    sqlite3.connect(legacy).close()
    assert resolve_installed_external_identity(legacy, external_id="53482", **NORTAXA) is None
    conn = sqlite3.connect(synthetic_artifact)
    conn.execute("DELETE FROM taxonomy_meta WHERE key = 'taxonomy_schema_version'")
    conn.commit()
    conn.close()
    assert resolve_installed_external_identity(synthetic_artifact, external_id="53482", **NORTAXA) is None
    assert resolve_installed_external_identity(None, external_id="53482", **NORTAXA) is None


# ── The controller's provider commit ─────────────────────────────────────────


@pytest.fixture
def controller_for(synthetic_artifact):
    from PySide6.QtWidgets import QApplication, QLineEdit
    from ui.taxon_input_controller import TaxonInputController

    QApplication.instance() or QApplication([])

    def build(db_path):
        lookup = TaxonLookupService(vernacular_db=VernacularDB(db_path)) if db_path else None
        return TaxonInputController(
            lookup=lookup, genus_input=QLineEdit(), species_input=QLineEdit(),
            scientific_name_input=QLineEdit(),
        )
    return build


def _provider(raw, name):
    return TaxonIdentity.from_prefixed_external_id(raw, scientific_name=name, rank="species")


def test_controller_binds_an_exact_bridge_with_resolution_proof(controller_for, synthetic_artifact):
    controller = controller_for(synthetic_artifact)
    assert controller.commit_provider_identity(
        _provider("NBIC:53482", "Entoloma conferendum"), genus="Entoloma", species="conferendum")
    snap = controller.committed_snapshot()
    identity = TaxonIdentity.from_row(snap)
    assert identity.is_proven_sporely
    assert (identity.state, identity.identity_proof, identity.sporely_taxon_id) == (
        STATE_SPORELY, PROOF_EXTERNAL_ID_RESOLUTION, 7821)
    assert (identity.source_system, identity.namespace, identity.external_id) == (
        "nortaxa", "nortaxa_taxon_id", "53482")
    assert identity.raw_external_id == "NBIC:53482"
    assert (snap["genus"], snap["species"], snap["scientific_name"]) == (
        "Entoloma", "conferendum", "Entoloma conferendum")
    assert snap["canonical_scientific_name"] == "Entoloma conferendum"


@pytest.mark.parametrize("raw", ["NBIC:99999", "NBIC:4242", "NBIC:11111", "NBIC:77777"])
def test_controller_keeps_an_unbridged_provider_identity_unresolved(controller_for, synthetic_artifact, raw):
    controller = controller_for(synthetic_artifact)
    assert controller.commit_provider_identity(_provider(raw, "Entoloma dubium"),
                                               genus="Entoloma", species="dubium")
    snap = controller.committed_snapshot()
    identity = TaxonIdentity.from_row(snap)
    assert identity.state == STATE_EXTERNAL_UNRESOLVED
    assert identity.sporely_taxon_id is None and snap.get("sporely_taxon_id") is None
    assert identity.external_id == raw.split(":")[1] and identity.raw_external_id == raw
    assert (snap["genus"], snap["species"], snap["scientific_name"]) == (
        "Entoloma", "dubium", "Entoloma dubium")


def test_controller_without_taxonomy_v2_keeps_it_unresolved(controller_for):
    controller = controller_for(None)
    assert controller.commit_provider_identity(_provider("NBIC:53482", "Entoloma conferendum"),
                                               genus="Entoloma", species="conferendum")
    assert TaxonIdentity.from_row(controller.committed_snapshot()).state == STATE_EXTERNAL_UNRESOLVED


def test_controller_refuses_proven_or_evidence_free_identities(controller_for, synthetic_artifact):
    controller = controller_for(synthetic_artifact)
    assert controller.commit_provider_identity(TaxonIdentity.from_taxonomy_v2_artifact(7821)) is False
    assert controller.commit_provider_identity(TaxonIdentity.none()) is False
    assert controller.committed_snapshot() is None


def test_a_resolver_failure_degrades_to_unresolved(controller_for, synthetic_artifact):
    controller = controller_for(synthetic_artifact)
    controller.lookup = SimpleNamespace(resolve_external_identity=lambda _i: 1 / 0)
    assert controller.commit_provider_identity(_provider("NBIC:53482", "Entoloma conferendum"),
                                               genus="Entoloma", species="conferendum")
    assert TaxonIdentity.from_row(controller.committed_snapshot()).state == STATE_EXTERNAL_UNRESOLVED


# ── The real live path, against the shipped artifact ─────────────────────────


@pytest.fixture(scope="module")
def shipped_artifact(tmp_path_factory) -> Path:
    """The tracked bundle, installed by the app's own installer."""
    app_dir = tmp_path_factory.mktemp("shipped-taxonomy")
    return taxonomy_v2.ensure_installed(app_data_dir=app_dir)


@pytest.fixture
def qapp():
    from PySide6.QtWidgets import QApplication
    return QApplication.instance() or QApplication([])


def _copy_artsorakel(monkeypatch, qapp, tmp_path, artifact, prediction):
    from tests.test_observation_details_ai_copy_and_vernacular_autofill import (
        _build_dialog,
        _select_prediction,
    )
    dialog = _build_dialog(monkeypatch, qapp, tmp_path=tmp_path, taxonomy_db_path=artifact)
    _select_prediction(dialog, prediction, source="arts")
    dialog._on_ai_copy_to_taxonomy("arts")
    qapp.processEvents()
    return dialog


def _prediction(raw, name, vernacular=""):
    return {"probability": 0.93, "scientificName": name, "name": name,
            "vernacularName": vernacular, "taxonId": raw, "id": raw}


def test_live_artsorakel_copy_of_nbic_53482_binds_entoloma_conferendum(
    monkeypatch, qapp, tmp_path, shipped_artifact,
):
    dialog = _copy_artsorakel(monkeypatch, qapp, tmp_path, shipped_artifact,
                              _prediction("NBIC:53482", "Entoloma conferendum", "stjernesporet rødspore"))
    try:
        data = dialog.get_data()
        assert (data["genus"], data["species"]) == ("Entoloma", "conferendum")
        assert data["sporely_taxon_id"] == 7821
        assert data["taxon_identity_state"] == STATE_SPORELY
        assert data["taxon_identity_proof"] == PROOF_EXTERNAL_ID_RESOLUTION
        assert (data["taxon_identity_source_system"], data["taxon_identity_namespace"],
                data["taxon_identity_external_id"], data["taxon_identity_raw_external_id"]) == (
            "nortaxa", "nortaxa_taxon_id", "53482", "NBIC:53482")
        assert "tax-2026.09.23-01" in (data["taxon_identity_provenance"] or "")
        # AI history stays history.
        assert dialog._current_ai_selected_fields["ai_selected_taxon_id"] == "NBIC:53482"
        assert dialog.is_unidentified() is False
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


@pytest.mark.parametrize("raw, name", [
    ("NBIC:987654321", "Entoloma imaginarium"),   # no bridge at all
    ("NBIC:7821", "Entoloma conferendum"),        # digits equal a real Sporely id
])
def test_live_artsorakel_copy_without_a_bridge_stays_unresolved(
    monkeypatch, qapp, tmp_path, shipped_artifact, raw, name,
):
    dialog = _copy_artsorakel(monkeypatch, qapp, tmp_path, shipped_artifact, _prediction(raw, name))
    try:
        data = dialog.get_data()
        genus, species = name.split()
        assert (data["genus"], data["species"]) == (genus, species)
        assert data["sporely_taxon_id"] is None
        assert data["taxon_identity_state"] == STATE_EXTERNAL_UNRESOLVED
        assert (data["taxon_identity_source_system"], data["taxon_identity_namespace"],
                data["taxon_identity_external_id"], data["taxon_identity_raw_external_id"]) == (
            "nortaxa", "nortaxa_taxon_id", raw.split(":")[1], raw)
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()
