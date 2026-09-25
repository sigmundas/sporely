"""The conservative representation of a Sporely ID received from the cloud.

``PROOF_CLOUD_SELECTED_UNVERIFIED`` records that an integer came from the
cloud's ``selected_sporely_taxon_id`` and that the receiving desktop confirmed
only its presence in the installed taxonomy artifact. The cloud cannot prove
the producer (pre-Stage-2 desktops asserted unproven integers through the same
RPC), so the value is kept — displayed, restored, preserved across saves — but
is never proven: it is never re-asserted to the cloud and never drives an
identity-gated lookup. An explicit picker selection upgrades it to
``taxonomy_v2_artifact``.
"""
from __future__ import annotations

import os
import sqlite3

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

from database import models, schema
from database.models import coherent_identity_columns
from utils import cloud_sync
from utils.taxon_identity import (
    PROOF_CLOUD_SELECTED_UNVERIFIED,
    PROOF_TAXONOMY_V2_ARTIFACT,
    PROVEN_SPORELY_PROOFS,
    STATE_NONE,
    STATE_SPORELY,
    TaxonIdentity,
    proven_sporely_taxon_id,
)

RELEASE = "tax-2026.09.23-01"


def _cloud_83668() -> TaxonIdentity:
    return TaxonIdentity.from_cloud_selection(
        83668, local_release_id=RELEASE,
        scientific_name="Conocybe rugosa", rank="species",
    )


# ── Model ────────────────────────────────────────────────────────────────────


def test_constructor_records_cloud_origin_and_local_release():
    identity = _cloud_83668()
    assert identity.state == STATE_SPORELY
    assert identity.sporely_taxon_id == 83668
    assert identity.identity_proof == PROOF_CLOUD_SELECTED_UNVERIFIED
    assert (identity.source_system, identity.namespace, identity.external_id) == (
        "sporely", "sporely_taxon_id", "83668",
    )
    assert "selected_sporely_taxon_id" in identity.provenance
    assert RELEASE in identity.provenance
    assert identity.is_cloud_selected_unverified is True


def test_it_is_never_proven():
    identity = _cloud_83668()
    assert PROOF_CLOUD_SELECTED_UNVERIFIED not in PROVEN_SPORELY_PROOFS
    assert identity.is_proven_sporely is False
    assert identity.is_legacy_unverified is False
    assert proven_sporely_taxon_id(identity.to_row()) is None


@pytest.mark.parametrize("taxon_id, release", [(None, RELEASE), (0, RELEASE), (-5, RELEASE),
                                               ("abc", RELEASE), (83668, None), (83668, "  ")])
def test_constructor_refuses_without_an_id_or_a_release(taxon_id, release):
    identity = TaxonIdentity.from_cloud_selection(taxon_id, local_release_id=release)
    assert identity.state == STATE_NONE
    assert identity.sporely_taxon_id is None


def test_row_round_trip_keeps_integer_and_proof():
    row = {**_cloud_83668().to_row(), "scientific_name_snapshot": "Conocybe rugosa",
           "taxon_rank_snapshot": "species"}
    assert row["sporely_taxon_id"] == 83668
    assert row["taxon_identity_proof"] == PROOF_CLOUD_SELECTED_UNVERIFIED
    restored = TaxonIdentity.from_row(row)
    assert restored == _cloud_83668()


def test_write_boundary_accepts_it_as_one_coherent_identity():
    columns = coherent_identity_columns(**_cloud_83668().to_row())
    assert columns["sporely_taxon_id"] == 83668
    assert columns["taxon_identity_proof"] == PROOF_CLOUD_SELECTED_UNVERIFIED
    assert columns["taxon_identity_state"] == STATE_SPORELY


def test_write_boundary_still_refuses_a_bare_integer():
    columns = coherent_identity_columns(sporely_taxon_id=83668)
    assert all(value is None for value in columns.values())


# ── Persistence ──────────────────────────────────────────────────────────────


@pytest.fixture
def real_db(tmp_path, monkeypatch):
    db_path = tmp_path / "sporely.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: tmp_path / "ref.db")
    monkeypatch.setattr(schema, "get_bundled_reference_database_path", lambda: tmp_path / "bundled.db")
    monkeypatch.setattr(schema, "_migrate_reference_values", lambda *a, **k: None)
    monkeypatch.setattr(schema, "_migrate_reference_mounts_and_stains", lambda *a, **k: None)
    schema.init_database()
    return db_path


def test_observation_db_persists_and_reads_it_back(real_db):
    local_id = models.ObservationDB.create_observation(
        date="2026-09-15", genus="Conocybe", species="rugosa",
        scientific_name_snapshot="Conocybe rugosa", taxon_rank_snapshot="species",
        **_cloud_83668().to_row(),
    )
    row = models.ObservationDB.get_observation(local_id)
    assert TaxonIdentity.from_row(row) == _cloud_83668()


# ── Cloud gate ───────────────────────────────────────────────────────────────


class _RpcRecorder:
    def __init__(self):
        self.rpc_calls = []

    def set_observation_selected_taxon(self, cloud_id, sporely_taxon_id):
        self.rpc_calls.append((cloud_id, sporely_taxon_id))


@pytest.mark.parametrize("remote_selected", [None, 83668, 99999])
def test_push_never_reasserts_a_cloud_selected_identity(remote_selected):
    recorder = _RpcRecorder()
    obs = {"id": 917, **_cloud_83668().to_row()}
    cloud_sync.SporelyCloudClient._sync_observation_selected_taxon(
        recorder, "14", obs, remote_obs={"selected_sporely_taxon_id": remote_selected},
    )
    assert recorder.rpc_calls == [], "skip, never assert and never clear"


# ── Picker upgrade ───────────────────────────────────────────────────────────


def test_explicit_picker_selection_upgrades_it_to_artifact_proof():
    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import QApplication, QLineEdit

    from ui.taxon_input_controller import TaxonInputController

    QApplication.instance() or QApplication([])

    class _Lookup:
        vernacular_db = object()
        language_code = "en"

        def suggest_genera(self, prefix="", limit=200): return []
        def suggest_species(self, genus, prefix="", limit=200): return []
        def suggest_common_names(self, prefix="", genus=None, species=None, limit=200): return []
        def resolve_common_name(self, name, genus=None, species=None): return []
        def resolve_scientific(self, genus, species): return None
        def resolve_manual_scientific(self, genus, species): return None

        def suggest_scientific_names(self, prefix, limit=50):
            return [{
                "scientific_name": "Conocybe rugosa",
                "taxon_rank_snapshot": "species",
                "sporely_taxon_id": 83668,
                "canonical_scientific_name": "Conocybe rugosa",
                "canonical_rank": "species",
                "link_kind": "canonical",
            }]

    genus, species, vern, sci = QLineEdit(), QLineEdit(), QLineEdit(), QLineEdit()
    controller = TaxonInputController(_Lookup(), genus, species, vern, None,
                                      scientific_name_input=sci)
    controller.load_committed_snapshot({
        "genus": "Conocybe", "species": "rugosa",
        "scientific_name": "Conocybe rugosa", "taxon_rank_snapshot": "species",
        **_cloud_83668().to_row(),
    })
    assert controller.committed_identity().is_cloud_selected_unverified

    sci.setText("Conocybe rugosa")
    controller.refresh_scientific_suggestions()
    model = controller._scientific_model
    item = next(model.item(r) for r in range(model.rowCount())
                if str(model.item(r).data(Qt.UserRole)) == "Conocybe rugosa")
    controller.on_scientific_name_selected(model.indexFromItem(item))

    upgraded = controller.committed_identity()
    assert upgraded.identity_proof == PROOF_TAXONOMY_V2_ARTIFACT
    assert upgraded.is_proven_sporely
    assert upgraded.sporely_taxon_id == 83668


# ── Dialog restore and save ──────────────────────────────────────────────────


def test_dialog_restores_and_preserves_a_cloud_selected_identity(monkeypatch, tmp_path):
    """The dialog rebuilds identity from the restored committed snapshot on
    save; a proof it did not recognise would be erased by the first save."""
    from PySide6.QtWidgets import QApplication

    from tests.test_red_list_ai_history_not_promoted import (
        _close,
        _observation_917,
        _open_dialog,
    )

    qapp = QApplication.instance() or QApplication([])
    observation = _observation_917(**_cloud_83668().to_row())
    dialog = _open_dialog(monkeypatch, qapp, tmp_path, observation, ai_state=None)
    try:
        dialog.open_comment_input.setPlainText("unrelated edit")
        qapp.processEvents()
        data = dialog.get_data()
        assert data["sporely_taxon_id"] == 83668
        assert data["taxon_identity_state"] == STATE_SPORELY
        assert data["taxon_identity_proof"] == PROOF_CLOUD_SELECTED_UNVERIFIED
        assert data["taxon_identity_provenance"] == _cloud_83668().provenance
        assert data["scientific_name_snapshot"] == "Conocybe rugosa"
        assert data["taxon_rank_snapshot"] == "species"
    finally:
        _close(dialog)
