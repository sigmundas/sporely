"""NorTaxa self-identity resolution.

A NorTaxa-only concept (one anchored by a NorTaxa usage, with no reviewed
bridge to COL) resolves through its own ``(nortaxa, nortaxa_taxon_id, X)``
tuple. Approved bridges still take precedence, a tuple naming two concepts
stays unresolved, and nothing here asserts equivalence with any other concept.

The dialog tests cover the owner's real case: an Artsorakel suggestion the
observer then overrides must leave no identity from the suggestion, and an
explicit picker selection keeps exactly the selected concept.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from database.taxon_lookup import (
    TaxonLookupService,
    resolve_installed_external_identity,
)
from database.vernacular_db import VernacularDB
from utils import taxonomy_v2
from utils.taxon_identity import (
    PROOF_EXTERNAL_ID_RESOLUTION,
    PROOF_TAXONOMY_V2_ARTIFACT,
    STATE_SPORELY,
    TaxonIdentity,
)
from ui.taxon_input_controller import ROLE_TAXON_CHOICE

NORTAXA = dict(source_system="nortaxa", namespace="nortaxa_taxon_id")
SELF = "taxon_min.canonical_source_usage"
BRIDGE = "taxon_external_id_text_min"


# ── The resolver, on a synthetic artifact ────────────────────────────────────


@pytest.fixture
def artifact(tmp_path: Path) -> Path:
    path = tmp_path / "artifact.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT);
        INSERT INTO taxonomy_meta VALUES ('taxonomy_schema_version', '2'),
                                         ('content_release_id', 'tax-test-01');
        CREATE TABLE taxon_min (taxon_id INTEGER PRIMARY KEY,
                                canonical_scientific_name TEXT, taxon_rank TEXT,
                                canonical_source_system TEXT, canonical_external_id TEXT);
        INSERT INTO taxon_min VALUES
            -- A NorTaxa-only concept and its same-name COL concept: distinct.
            (624659, 'Panaeolina foenisecii', 'species', 'nortaxa', '52330'),
            (40673, 'Panaeolina foenisecii', 'species', 'col_xr', '4C943'),
            -- A reviewed bridge, plus an anchor sharing the bridged id.
            (7821, 'Entoloma conferendum', 'species', 'col_xr', '39ZCL'),
            (624000, 'Entoloma conferendum', 'species', 'nortaxa', '53482'),
            -- Two anchors claiming one id: never chosen between.
            (625000, 'Alpha one', 'species', 'nortaxa', '55555'),
            (625001, 'Alpha two', 'species', 'nortaxa', '55555');
        CREATE TABLE taxon_external_id_text_min (taxon_id INTEGER, source_system TEXT,
                                                 namespace TEXT, external_id TEXT);
        INSERT INTO taxon_external_id_text_min VALUES
            (7821, 'nortaxa', 'nortaxa_taxon_id', '53482');
        """
    )
    conn.commit()
    conn.close()
    return path


def test_a_nortaxa_only_concept_resolves_through_its_own_id(artifact):
    concept = resolve_installed_external_identity(artifact, external_id="52330", **NORTAXA)
    assert concept is not None
    assert (concept.sporely_taxon_id, concept.scientific_name, concept.resolved_through) == (
        624659, "Panaeolina foenisecii", SELF)


def test_an_approved_bridge_takes_precedence_over_an_anchor(artifact):
    concept = resolve_installed_external_identity(artifact, external_id="53482", **NORTAXA)
    assert (concept.sporely_taxon_id, concept.resolved_through) == (7821, BRIDGE)


def test_two_anchors_with_one_id_stay_unresolved(artifact):
    assert resolve_installed_external_identity(artifact, external_id="55555", **NORTAXA) is None


@pytest.mark.parametrize("source_system, namespace, external_id", [
    ("nortaxa", "nortaxa_dwc_id", "52330"),     # a different NorTaxa namespace
    ("col_xr", "col_usage_id", "4C943"),         # not a source-identity namespace
    ("nortaxa", "nortaxa_taxon_id", "624659"),   # a Sporely id is not a NorTaxa id
])
def test_only_the_nortaxa_taxon_id_namespace_resolves_by_source_identity(
    artifact, source_system, namespace, external_id,
):
    assert resolve_installed_external_identity(
        artifact, source_system=source_system, namespace=namespace,
        external_id=external_id) is None


def test_an_artifact_without_canonical_columns_still_resolves_bridges(tmp_path):
    path = tmp_path / "old.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT);
        INSERT INTO taxonomy_meta VALUES ('taxonomy_schema_version', '2'),
                                         ('content_release_id', 'tax-old');
        CREATE TABLE taxon_min (taxon_id INTEGER PRIMARY KEY,
                                canonical_scientific_name TEXT, taxon_rank TEXT);
        INSERT INTO taxon_min VALUES (7821, 'Entoloma conferendum', 'species');
        CREATE TABLE taxon_external_id_text_min (taxon_id INTEGER, source_system TEXT,
                                                 namespace TEXT, external_id TEXT);
        INSERT INTO taxon_external_id_text_min VALUES
            (7821, 'nortaxa', 'nortaxa_taxon_id', '53482');
        """
    )
    conn.commit()
    conn.close()
    assert resolve_installed_external_identity(path, external_id="53482", **NORTAXA).sporely_taxon_id == 7821
    assert resolve_installed_external_identity(path, external_id="52330", **NORTAXA) is None


# ── The controller's provider commit ─────────────────────────────────────────


@pytest.fixture
def controller(artifact):
    from PySide6.QtWidgets import QApplication, QLineEdit
    from ui.taxon_input_controller import TaxonInputController

    QApplication.instance() or QApplication([])
    return TaxonInputController(
        lookup=TaxonLookupService(vernacular_db=VernacularDB(artifact)),
        genus_input=QLineEdit(), species_input=QLineEdit(),
        scientific_name_input=QLineEdit(),
    )


def _provider(raw, name):
    return TaxonIdentity.from_prefixed_external_id(raw, scientific_name=name, rank="species")


def test_controller_binds_a_nortaxa_only_suggestion_and_names_the_path(controller):
    assert controller.commit_provider_identity(
        _provider("NBIC:52330", "Panaeolina foenisecii"), genus="Panaeolina", species="foenisecii")
    identity = controller.committed_identity()
    assert identity.is_proven_sporely
    assert (identity.state, identity.identity_proof, identity.sporely_taxon_id) == (
        STATE_SPORELY, PROOF_EXTERNAL_ID_RESOLUTION, 624659)
    assert (identity.source_system, identity.namespace, identity.external_id,
            identity.raw_external_id) == ("nortaxa", "nortaxa_taxon_id", "52330", "NBIC:52330")
    assert identity.provenance == f"taxonomy_v2:tax-test-01:{SELF}"


def test_a_bridged_suggestion_keeps_its_bridge_provenance(controller):
    assert controller.commit_provider_identity(
        _provider("NBIC:53482", "Entoloma conferendum"), genus="Entoloma", species="conferendum")
    assert controller.committed_identity().provenance == f"taxonomy_v2:tax-test-01:{BRIDGE}"


def test_overriding_the_suggested_species_drops_the_suggestion_identity(controller):
    assert controller.commit_provider_identity(
        _provider("NBIC:52330", "Panaeolina foenisecii"), genus="Panaeolina", species="foenisecii")
    controller.genus_input.setText("Panaeolus")
    controller.species_input.setText("reticulatus")
    assert controller.committed_snapshot() is None
    identity = controller.committed_identity()
    assert identity.sporely_taxon_id is None
    assert identity.external_id is None and identity.raw_external_id is None


# ── The real live path, against the shipped artifact ─────────────────────────


@pytest.fixture(scope="module")
def shipped_artifact(tmp_path_factory) -> Path:
    """The tracked bundle, installed by the app's own installer."""
    return taxonomy_v2.ensure_installed(app_data_dir=tmp_path_factory.mktemp("shipped-taxonomy"))


def test_shipped_release_has_no_bridge_colliding_with_an_anchor_id(shipped_artifact):
    """The precedence rule never has to decide in the shipped release: no
    approved NorTaxa bridge id is also a NorTaxa-only concept's own id, and no
    two NorTaxa-only concepts share one."""
    conn = sqlite3.connect(f"file:{shipped_artifact}?mode=ro", uri=True)
    try:
        collisions = conn.execute(
            "SELECT COUNT(*) FROM taxon_external_id_text_min e JOIN taxon_min t "
            "ON t.canonical_source_system = 'nortaxa' AND t.canonical_external_id = e.external_id "
            "WHERE e.source_system = 'nortaxa' AND e.namespace = 'nortaxa_taxon_id'"
        ).fetchone()[0]
        shared = conn.execute(
            "SELECT COUNT(*) FROM (SELECT canonical_external_id FROM taxon_min "
            "WHERE canonical_source_system = 'nortaxa' "
            "GROUP BY canonical_external_id HAVING COUNT(*) > 1)"
        ).fetchone()[0]
    finally:
        conn.close()
    assert (collisions, shared) == (0, 0)


@pytest.fixture
def qapp():
    from PySide6.QtWidgets import QApplication
    return QApplication.instance() or QApplication([])


def _artsorakel_prediction(raw, name):
    """The shape the Artsorakel API actually returns (recorded on the owner's
    observation 1032): the prefixed id is only in ``scientific_name_id``, and
    ``scientific_name_id_shared`` is a GBIF id. No ``taxonId`` / ``id`` keys."""
    return {"name": name, "scientificName": name, "scientific_name": name,
            "probability": 0.54301, "vernacularName": "",
            "scientific_name_id": raw, "scientific_name_id_shared": "GBIF:2526698",
            "scientific_name_shared": name, "infoUrl": "https://artsdatabanken.no/Taxon/35142"}


def _copy_artsorakel(monkeypatch, qapp, tmp_path, artifact, raw, name):
    from tests.test_observation_details_ai_copy_and_vernacular_autofill import (
        _build_dialog,
        _select_prediction,
    )
    dialog = _build_dialog(monkeypatch, qapp, tmp_path=tmp_path, taxonomy_db_path=artifact)
    _select_prediction(dialog, _artsorakel_prediction(raw, name), source="arts")
    dialog._on_ai_copy_to_taxonomy("arts")
    qapp.processEvents()
    return dialog


def test_live_artsorakel_copy_of_nbic_52330_binds_the_nortaxa_concept(
    monkeypatch, qapp, tmp_path, shipped_artifact,
):
    dialog = _copy_artsorakel(monkeypatch, qapp, tmp_path, shipped_artifact,
                              "NBIC:52330", "Panaeolina foenisecii")
    try:
        data = dialog.get_data()
        assert (data["genus"], data["species"]) == ("Panaeolina", "foenisecii")
        assert data["sporely_taxon_id"] == 624659
        assert data["taxon_identity_proof"] == PROOF_EXTERNAL_ID_RESOLUTION
        assert (data["taxon_identity_source_system"], data["taxon_identity_namespace"],
                data["taxon_identity_external_id"]) == ("nortaxa", "nortaxa_taxon_id", "52330")
        shipped = taxonomy_v2.load_manifest().content_release_id
        assert data["taxon_identity_provenance"] == f"taxonomy_v2:{shipped}:{SELF}"
        # Observation 1032's defect: the suggestion's id was never recorded.
        assert dialog._current_ai_selected_fields["ai_selected_taxon_id"] == "NBIC:52330"
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


@pytest.mark.parametrize("country, expected_category", [
    ("no", "LC"),   # the local assessment for the bound concept
    ("", None),     # no area: no local category (determine_redlist_area)
])
def test_a_binding_artsorakel_copy_keeps_its_red_list_snapshot(
    monkeypatch, qapp, tmp_path, shipped_artifact, country, expected_category,
):
    """Binding the suggestion's identity must not wipe the Red List snapshot
    the same suggestion carried: the identity commit clears the Red List for
    an identity change, so it has to run before the snapshot is applied."""
    from tests.test_observation_details_ai_copy_and_vernacular_autofill import (
        _build_dialog,
        _select_prediction,
    )
    dialog = _build_dialog(monkeypatch, qapp, tmp_path=tmp_path, taxonomy_db_path=shipped_artifact)
    try:
        dialog._location_country_code = country
        prediction = {**_artsorakel_prediction("NBIC:52330", "Panaeolina foenisecii"),
                      "redListCategory": "LC", "redListCategories": {"NO": "LC"}}
        _select_prediction(dialog, prediction, source="arts")
        dialog._on_ai_copy_to_taxonomy("arts")
        for _ in range(5):
            qapp.processEvents()
        data = dialog.get_data()
        assert data["sporely_taxon_id"] == 624659
        assert data["red_list_categories_json"] == '{"NO": "LC"}'
        assert data["red_list_category"] == expected_category
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


def test_overriding_an_artsorakel_suggestion_then_picking_keeps_only_the_pick(
    monkeypatch, qapp, tmp_path, shipped_artifact,
):
    """The owner's case: Artsorakel suggests *Russula sanguinea* (NorTaxa
    57843, concept 626870); the observer overrides it with *R. emetica* and
    then explicitly picks the COL concept 61951 — not the same-name NorTaxa
    concept 626835."""
    dialog = _copy_artsorakel(monkeypatch, qapp, tmp_path, shipped_artifact,
                              "NBIC:57843", "Russula sanguinea")
    try:
        assert dialog.get_data()["sporely_taxon_id"] == 626870

        dialog.species_input.setText("emetica")
        qapp.processEvents()
        overridden = dialog.get_data()
        assert overridden["sporely_taxon_id"] is None
        assert overridden.get("taxon_identity_external_id") in (None, "")
        # The suggestion stays on record as history only.
        assert dialog._current_ai_selected_fields["ai_selected_taxon_id"] == "NBIC:57843"

        controller = dialog._taxon_controller
        dialog.scientific_name_input.setText("Russula emetica")
        controller.refresh_scientific_suggestions()
        model = controller._scientific_model
        rows = [model.item(r) for r in range(model.rowCount())]
        pick = next(item for item in rows
                    if (item.data(ROLE_TAXON_CHOICE) or {}).get("sporely_taxon_id") == 61951)
        controller.on_scientific_name_selected(model.indexFromItem(pick))
        qapp.processEvents()

        picked = dialog.get_data()
        assert (picked["genus"], picked["species"]) == ("Russula", "emetica")
        assert picked["sporely_taxon_id"] == 61951
        assert picked["taxon_identity_proof"] == PROOF_TAXONOMY_V2_ARTIFACT
        # The stored tuple is the picked concept's own, not the suggestion's.
        assert picked["taxon_identity_source_system"] != "nortaxa"
        assert picked["taxon_identity_external_id"] == "61951"
        assert picked.get("taxon_identity_raw_external_id") != "NBIC:57843"
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


def test_artsorakel_scientific_name_id_is_the_prediction_id():
    from ui.observations_tab import _normalize_ai_prediction_taxon

    taxon = _normalize_ai_prediction_taxon(
        _artsorakel_prediction("NBIC:52330", "Panaeolina foenisecii"), source="arts")
    assert taxon["id"] == "NBIC:52330"   # never the GBIF ``scientific_name_id_shared``
    legacy = _normalize_ai_prediction_taxon(
        {"scientificName": "Entoloma conferendum", "taxonId": "NBIC:53482",
         "scientific_name_id": "NBIC:1"}, source="arts")
    assert legacy["id"] == "NBIC:53482"  # an explicit taxon id still wins
    inat = _normalize_ai_prediction_taxon({"name": "Amanita muscaria", "id": 48715}, source="inat")
    assert inat["id"] == 48715


# ── Sync consequence ─────────────────────────────────────────────────────────


def _refusing_client(code="22023",
                     message="sporely_taxon_id 624659 is missing from the active taxonomy-v2 release"):
    """The push client, with the guarded RPC refusing the concept like the server."""
    from tests.test_live_provider_identity_resolution import _push_client
    from utils.cloud_sync_impl.errors import CloudWriteRejectedError

    client = _push_client()

    def rpc(name, payload):
        client.rpcs.append((name, dict(payload)))
        if name == "set_observation_selected_taxon_v2":
            raise CloudWriteRejectedError(
                f"RPC {name} failed", status_code=400, code=code, message=message)
    client._rpc = rpc
    return client


def _self_resolved_row():
    resolved = _provider("NBIC:52330", "Panaeolina foenisecii").resolved_to(
        624659, provenance=f"taxonomy_v2:tax-2026.10.07-01:{SELF}")
    return {"id": 1, "cloud_id": "917", "date": "2026-10-18", "genus": "Panaeolina",
            "species": "foenisecii", **resolved.to_row()}


def test_a_concept_missing_from_the_cloud_release_preserves_its_tuple_instead_of_failing():
    """The scoped cloud release lacks every NorTaxa-only concept. The refusal
    must not fail the observation's push; the row's own NorTaxa tuple is kept
    in the cloud as external_unresolved, and the name is not touched."""
    client = _refusing_client()
    remote = {"id": 917, "genus": "Panaeolina", "species": "foenisecii",
              "selected_sporely_taxon_id": None, "taxon_identity_state": None}
    client.push_observation(_self_resolved_row(), remote_obs=remote)
    assert client.rpcs == [
        ("set_observation_selected_taxon_v2", {"p_observation_id": 917, "p_sporely_taxon_id": 624659}),
        ("set_observation_identification_v2", {
            "p_observation_id": 917, "p_sporely_taxon_id": None,
            "p_identity_state": "external_unresolved", "p_source_system": "nortaxa",
            "p_namespace": "nortaxa_taxon_id", "p_external_id": "52330",
            "p_raw_external_id": "NBIC:52330", "p_write_name": False,
            "p_genus": None, "p_species": None, "p_common_name": None}),
    ]


def test_a_tuple_the_cloud_already_holds_is_not_rewritten():
    client = _refusing_client()
    remote = {"id": 917, "genus": "Panaeolina", "species": "foenisecii",
              "selected_sporely_taxon_id": None, "taxon_identity_state": "external_unresolved",
              "taxon_identity_source_system": "nortaxa", "taxon_identity_namespace": "nortaxa_taxon_id",
              "taxon_identity_external_id": "52330", "taxon_identity_raw_external_id": "NBIC:52330"}
    client.push_observation(_self_resolved_row(), remote_obs=remote)
    assert [name for name, _ in client.rpcs] == ["set_observation_selected_taxon_v2"]


def test_a_picker_selected_concept_missing_from_the_cloud_stays_local_without_failing():
    client = _refusing_client()
    picked = TaxonIdentity.from_taxonomy_v2_artifact(624659, scientific_name="Panaeolina foenisecii",
                                                     rank="species")
    row = {"id": 1, "cloud_id": "917", "date": "2026-10-18", "genus": "Panaeolina",
           "species": "foenisecii", **picked.to_row()}
    remote = {"id": 917, "genus": "Panaeolina", "species": "foenisecii",
              "selected_sporely_taxon_id": None, "taxon_identity_state": None}
    client.push_observation(row, remote_obs=remote)
    assert [name for name, _ in client.rpcs] == ["set_observation_selected_taxon_v2"]


def test_a_new_observation_preserves_its_tuple_once_after_the_create():
    """The create path (``push_observation`` for a new row) syncs identity with
    ``remote_obs=None`` right after the insert; the refusal must not fail it."""
    client = _refusing_client()
    client._sync_observation_selected_taxon("917", _self_resolved_row(), remote_obs=None)
    assert [name for name, _ in client.rpcs] == [
        "set_observation_selected_taxon_v2", "set_observation_identification_v2"]


@pytest.mark.parametrize("code, message", [
    ("42501", "observation not found or caller does not own it"),
    ("22023", "an external_unresolved identity cannot carry a selected_sporely_taxon_id"),
])
def test_any_other_refusal_still_fails_the_push(code, message):
    from utils.cloud_sync_impl.errors import CloudWriteRejectedError

    client = _refusing_client(code=code, message=message)
    remote = {"id": 917, "genus": "Panaeolina", "species": "foenisecii",
              "selected_sporely_taxon_id": None, "taxon_identity_state": None}
    with pytest.raises(CloudWriteRejectedError):
        client.push_observation(_self_resolved_row(), remote_obs=remote)
    assert [name for name, _ in client.rpcs] == ["set_observation_selected_taxon_v2"]


def test_a_self_resolved_identity_pushes_its_own_concept_id_through_the_rpc():
    from tests.test_live_provider_identity_resolution import _push_client

    resolved = _provider("NBIC:52330", "Panaeolina foenisecii").resolved_to(
        624659, provenance=f"taxonomy_v2:tax-2026.10.07-01:{SELF}")
    row = {"id": 1, "cloud_id": "917", "date": "2026-10-18", "genus": "Panaeolina",
           "species": "foenisecii", **resolved.to_row()}
    remote = {"id": 917, "genus": "Panaeolina", "species": "foenisecii",
              "selected_sporely_taxon_id": None, "taxon_identity_state": None}
    client = _push_client()
    client.push_observation(row, remote_obs=remote)
    assert client.rpcs == [("set_observation_selected_taxon_v2",
                            {"p_observation_id": 917, "p_sporely_taxon_id": 624659})]
