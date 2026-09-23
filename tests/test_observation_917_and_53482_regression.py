"""Stage 4 Parts C and D: the two end-to-end closeout regressions.

Part C uses observation 917 — null ``genus``/``species``, null identity, a W3
snapshot that recorded ``no_identity_evidence``, and real microscopy content —
to prove that correcting taxonomy identity does not damage anything else the
observation owns. The test therefore asserts far more about the microscopy
than about the taxonomy: every other table, and every other column of the
observation row, must come out byte-identical.

Part D uses ``Entoloma conferendum`` / ``NBIC:53482`` to walk the whole
corrected path: an Artsorakel identifier is normalized into its namespaced
tuple, resolved through the Stage 3 bridge, saved with a real name, pushed as
a Sporely identity, pulled back without being downgraded, and — for a
deprecated or malformed candidate — refused without clearing what was already
there.

Nothing that decides an outcome here is stubbed. The database is built by
``database.schema.init_database``; the observation is written by
``database.models.ObservationDB``; the identity repair is
``database.audit_observation_identity``; the push is
``cloud_sync.SporelyCloudClient.push_observation`` with only the HTTP
boundary replaced; the pull is ``cloud_sync._apply_remote_observation_fields``.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from database.audit_observation_identity import (
    TaxonomyArtifact,
    apply_repairs,
    audit,
    read_desktop_rows,
)
from utils.taxon_identity import (
    IDENTITY_COLUMNS,
    PROOF_EXTERNAL_ID_RESOLUTION,
    STATE_SPORELY,
    TaxonIdentity,
)


# ── The Stage 3 candidate, in both the shapes an operator may hold ──────────

#: Built by Stage 3 outside the repository. When present, the regressions also
#: run against the real artifact rather than only a reconstruction of it.
_STAGE3_PACK = Path(
    "/Users/sigmundas/Documents/Code/sporely/.stage3-build/"
    "2026-09-23-taxonomy-v2/desktop-tax-2026.09.23-01.sqlite3"
)

#: The two reviewed relationships Stage 3 emitted, and the vernacular names it
#: verified on 7821. Reproduced here so the regression is portable; the
#: ``_STAGE3_PACK`` test below is what ties these values to the real artifact.
_REVIEWED_BRIDGE = (
    (7821, "nortaxa", "nortaxa_taxon_id", "53482"),
    (7821, "col_xr", "col_usage_id", "39ZCL"),
    (83668, "nortaxa", "nortaxa_taxon_id", "52369"),
    (83668, "col_xr", "col_usage_id", "5ZT3G"),
)

_VERNACULARS = (
    (7821, "nb", "stjernesporet rødspore"),
    (7821, "nn", "stjernespora raudspore"),
)


def _reconstructed_artifact(tmp_path: Path) -> Path:
    path = tmp_path / "stage3-candidate.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE taxon (taxon_id INTEGER PRIMARY KEY, scientific_name TEXT);
        CREATE TABLE external_mapping (
            taxon_id INTEGER, source_system TEXT, namespace TEXT,
            external_id TEXT, is_preferred INTEGER);
        CREATE TABLE vernacular_name (
            taxon_id INTEGER, language TEXT, name TEXT,
            is_preferred INTEGER, source TEXT);
        CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT);
        """
    )
    conn.executemany("INSERT INTO taxon VALUES (?, ?)", (
        (7821, "Entoloma conferendum"),
        (83668, "Conocybe rugosa"),
    ))
    conn.executemany(
        "INSERT INTO external_mapping VALUES (?, ?, ?, ?, 0)", _REVIEWED_BRIDGE
    )
    conn.executemany(
        "INSERT INTO vernacular_name VALUES (?, ?, ?, 1, 'nortaxa')", _VERNACULARS
    )
    conn.execute(
        "INSERT INTO metadata VALUES ('release_id', 'tax-2026.09.23-01')"
    )
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def artifact(tmp_path):
    art = TaxonomyArtifact(_reconstructed_artifact(tmp_path))
    yield art
    art.close()


# ── Observation 917, with the microscopy it must not lose ───────────────────


def _fresh_db(tmp_path, monkeypatch) -> Path:
    db_path = Path(tmp_path) / "observations.sqlite3"
    from database import schema as _schema

    monkeypatch.setattr(_schema, "get_database_path", lambda: db_path)
    _schema.init_database()
    import database.models as _models

    monkeypatch.setattr(_models, "get_database_path", lambda: db_path, raising=False)
    return db_path


def _build_917(db_path: Path) -> int:
    """Observation 917 as production recorded it, plus its microscopy.

    The taxonomy half is the plan's verified cloud state: null ``genus``,
    null ``species``, null identity, and an Artsorakel identification history
    that is NOT an accepted identity. Everything else on the row exists so a
    repair has something to damage.
    """
    from database.models import ObservationDB

    obs_id = ObservationDB.create_observation(
        date="2026-09-14 10:30",
        genus=None,
        species=None,
        common_name=None,
        location="Bymarka, Trondheim",
        notes="på død bjørkestokk",
        gps_latitude=63.4,
        gps_longitude=10.3,
        ai_selected_service="artsorakel",
        ai_selected_taxon_id="NBIC:53482",
        ai_selected_scientific_name="Entoloma conferendum",
        ai_selected_probability=0.93,
        ai_selected_at="2026-09-14 10:35",
        spore_data_visibility="private",
        sharing_scope="private",
    )

    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO calibrations (calibration_uuid, objective_key, "
        "calibration_date, microns_per_pixel, num_measurements, is_active) "
        "VALUES ('cal-917-40x', '40x', '2026-09-01', 0.1712, 42, 1)"
    )
    calibration_id = conn.execute(
        "SELECT id FROM calibrations WHERE calibration_uuid = 'cal-917-40x'"
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO images (id, observation_id, filepath, image_type, "
        "micro_category, sort_order, objective_name, scale_microns_per_pixel, "
        "calibration_id, mount_medium, stain, contrast, notes, cloud_id) "
        "VALUES (9170, ?, '/media/917/field.jpg', 'field', NULL, 0, NULL, "
        "NULL, NULL, NULL, NULL, NULL, 'feltbilde', '77001')",
        (obs_id,),
    )
    conn.execute(
        "INSERT INTO images (id, observation_id, filepath, image_type, "
        "micro_category, sort_order, objective_name, scale_microns_per_pixel, "
        "calibration_id, mount_medium, stain, contrast, notes, cloud_id) "
        "VALUES (9171, ?, '/media/917/spores.jpg', 'microscope', 'spores', 1, "
        "'40x', 0.1712, ?, 'KOH', 'Melzer', 'DIC', 'sporer i KOH', '77002')",
        (obs_id, calibration_id),
    )
    for idx, (length, width) in enumerate(
        ((9.8, 8.6), (10.1, 8.9), (9.4, 8.2)), start=1
    ):
        conn.execute(
            "INSERT INTO spore_measurements (id, image_id, length_um, width_um, "
            "measurement_type) VALUES (?, 9171, ?, ?, 'spore')",
            (9000 + idx, length, width),
        )
    conn.execute(
        "INSERT INTO spore_annotations (image_id, spore_number, bbox_x, bbox_y, "
        "bbox_width, bbox_height, length_um, width_um, annotation_source) "
        "VALUES (9171, 1, 10.0, 12.0, 50.0, 58.0, 9.8, 8.6, 'auto')"
    )
    conn.commit()
    conn.close()
    return obs_id


def _table_digests(db_path: Path, *, exclude: set[str] = frozenset()) -> dict:
    """A content digest per table, so "unchanged" is checkable in one line."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    names = sorted(
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name NOT LIKE 'sqlite_%'"
        )
    )
    digests = {}
    for name in names:
        if name in exclude:
            continue
        rows = [
            dict(row)
            for row in conn.execute(f"SELECT * FROM {name} ORDER BY rowid")
        ]
        digests[name] = hashlib.sha256(
            json.dumps(rows, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
    conn.close()
    return digests


def _observation_row(db_path: Path, obs_id: int) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = dict(
        conn.execute("SELECT * FROM observations WHERE id = ?", (obs_id,)).fetchone()
    )
    conn.close()
    return row


#: The only columns a taxonomy repair is allowed to move.
_REPAIRABLE_COLUMNS = {"genus", "species", "sporely_taxon_id", *IDENTITY_COLUMNS}


# ── Part C ──────────────────────────────────────────────────────────────────


def test_917_before_the_repair_is_the_state_the_plan_recorded(tmp_path, monkeypatch):
    db_path = _fresh_db(tmp_path, monkeypatch)
    obs_id = _build_917(db_path)
    row = _observation_row(db_path, obs_id)

    assert row["genus"] is None
    assert row["species"] is None
    assert row["sporely_taxon_id"] is None
    assert TaxonIdentity.from_row(row).state == "no_identity_evidence"
    # The AI history exists independently and is not an accepted identity.
    assert row["ai_selected_taxon_id"] == "NBIC:53482"
    assert TaxonIdentity.from_row(row).is_proven_sporely is False


def test_917_repair_binds_the_bridged_identity_and_restores_the_name(
    tmp_path, monkeypatch, artifact
):
    db_path = _fresh_db(tmp_path, monkeypatch)
    obs_id = _build_917(db_path)

    report = audit(read_desktop_rows(db_path), artifact, origin="desktop")
    record = report.records[0]
    assert record.identity_class == "proven_external_with_unique_bridge"
    assert record.name_class == "name_loss_repairable_from_row"
    assert record.candidate_sporely_taxon_id == 7821
    assert record.preserved_namespace == "nortaxa_taxon_id"
    assert record.preserved_raw_external_id == "NBIC:53482"

    apply_repairs(
        report, observation_db_path=db_path, release_id=artifact.release_id
    )

    row = _observation_row(db_path, obs_id)
    identity = TaxonIdentity.from_row(row)
    assert identity.is_proven_sporely is True
    assert identity.sporely_taxon_id == 7821
    assert identity.identity_proof == PROOF_EXTERNAL_ID_RESOLUTION
    # The source evidence survives the resolution, so it stays auditable.
    assert identity.source_system == "nortaxa"
    assert identity.namespace == "nortaxa_taxon_id"
    assert identity.external_id == "53482"
    assert identity.raw_external_id == "NBIC:53482"
    assert "tax-2026.09.23-01" in identity.provenance
    assert (row["genus"], row["species"]) == ("Entoloma", "conferendum")


def test_917_repair_changes_nothing_but_taxonomy(tmp_path, monkeypatch, artifact):
    """The point of Part C: microscopy and media come out untouched."""
    db_path = _fresh_db(tmp_path, monkeypatch)
    obs_id = _build_917(db_path)

    before_tables = _table_digests(db_path, exclude={"observations"})
    before_row = _observation_row(db_path, obs_id)

    report = audit(read_desktop_rows(db_path), artifact, origin="desktop")
    apply_repairs(
        report, observation_db_path=db_path, release_id=artifact.release_id
    )

    after_tables = _table_digests(db_path, exclude={"observations"})
    after_row = _observation_row(db_path, obs_id)

    # Images, image metadata, measurements, annotations, calibrations,
    # thumbnails, tombstones, reference uses — every table, bit for bit.
    assert after_tables == before_tables

    # And on the observation itself, only the taxonomy columns moved.
    changed = {
        key for key in before_row if before_row[key] != after_row[key]
    }
    assert changed <= _REPAIRABLE_COLUMNS
    # Named explicitly, because a digest comparison can hide a field that
    # happens to be null on both sides for the wrong reason.
    for key in (
        "location", "notes", "gps_latitude", "gps_longitude", "date",
        "spore_data_visibility", "sharing_scope",
        "ai_selected_service", "ai_selected_taxon_id",
        "ai_selected_scientific_name", "ai_selected_probability",
        "ai_selected_at",
    ):
        assert after_row[key] == before_row[key], key


def test_917_visibility_is_not_touched_by_an_identity_repair(
    tmp_path, monkeypatch, artifact
):
    """Identity and audience are independent; a repair must not widen one."""
    db_path = _fresh_db(tmp_path, monkeypatch)
    obs_id = _build_917(db_path)
    report = audit(read_desktop_rows(db_path), artifact, origin="desktop")
    apply_repairs(report, observation_db_path=db_path, release_id=None)
    row = _observation_row(db_path, obs_id)
    assert row["spore_data_visibility"] == "private"
    assert row["sharing_scope"] == "private"


def test_917_survives_save_and_reload(tmp_path, monkeypatch, artifact):
    db_path = _fresh_db(tmp_path, monkeypatch)
    obs_id = _build_917(db_path)
    report = audit(read_desktop_rows(db_path), artifact, origin="desktop")
    apply_repairs(report, observation_db_path=db_path, release_id=None)

    from database.models import ObservationDB

    reloaded = ObservationDB.get_observation(obs_id)
    identity = TaxonIdentity.from_row(dict(reloaded))
    assert identity.is_proven_sporely is True
    assert identity.sporely_taxon_id == 7821
    assert identity.raw_external_id == "NBIC:53482"
    assert reloaded["genus"] == "Entoloma"
    assert reloaded["species"] == "conferendum"


def test_917_pushes_the_proven_identity_and_nothing_else(tmp_path, monkeypatch, artifact):
    """Sync sends the proven Sporely ID through the guarded RPC only."""
    from utils.cloud_sync import SporelyCloudClient

    db_path = _fresh_db(tmp_path, monkeypatch)
    obs_id = _build_917(db_path)
    report = audit(read_desktop_rows(db_path), artifact, origin="desktop")
    apply_repairs(report, observation_db_path=db_path, release_id=None)
    row = _observation_row(db_path, obs_id)
    row["cloud_id"] = "917"

    remote = {"id": 917, "selected_sporely_taxon_id": None}
    client = object.__new__(SporelyCloudClient)
    client.user_id = "00000000-0000-4000-8000-000000000001"
    client._resolve_existing_observation_for_push = lambda _obs, remote_obs=None: "917"
    patches: list[tuple] = []
    rpcs: list[tuple] = []
    client._patch = lambda *args, **kwargs: patches.append((args, kwargs))
    client._rpc = lambda name, payload: rpcs.append((name, payload))

    client.push_observation(row, remote_obs=remote)

    assert rpcs == [
        ("set_observation_selected_taxon_v2",
         {"p_observation_id": 917, "p_sporely_taxon_id": 7821}),
    ]
    # Identity provenance is never an ordinary observation field; the guarded
    # RPC stays the only identity channel.
    for args, kwargs in patches:
        payload = next(
            (a for a in args if isinstance(a, dict)),
            kwargs.get("payload") or kwargs.get("data") or {},
        )
        assert not (set(payload) & set(IDENTITY_COLUMNS))


def test_917_survives_the_pull_that_follows_the_push(tmp_path, monkeypatch, artifact):
    """The failure mode the sparring challenge names: one-way success.

    A push that lands is worthless if the next pull walks the row back. The
    cloud row the client reads afterwards still carries the pre-repair
    ``genus``/``species`` nulls until the cloud migration runs, so this is the
    exact window in which a naive pull would undo the repair.
    """
    from utils import cloud_sync

    db_path = _fresh_db(tmp_path, monkeypatch)
    obs_id = _build_917(db_path)
    report = audit(read_desktop_rows(db_path), artifact, origin="desktop")
    apply_repairs(report, observation_db_path=db_path, release_id=None)

    before_tables = _table_digests(db_path, exclude={"observations"})
    before_row = _observation_row(db_path, obs_id)

    cloud_sync._apply_remote_observation_fields(obs_id, {
        "id": 917,
        "desktop_id": obs_id,
        "date": "2026-09-14 10:30",
        "genus": None,
        "species": None,
        "common_name": None,
        "location": "Bymarka, Trondheim",
        "notes": "på død bjørkestokk",
        "gps_latitude": 63.4,
        "gps_longitude": 10.3,
        "visibility": "private",
        "spore_data_visibility": "private",
        "ai_selected_service": "artsorakel",
        "ai_selected_taxon_id": "NBIC:53482",
        "ai_selected_scientific_name": "Entoloma conferendum",
        "ai_selected_probability": 0.93,
        "ai_selected_at": "2026-09-14 10:35",
        # The cloud's own selection, which the pull does not write into the
        # client's identity columns — identity is client-proven, not adopted.
        "selected_sporely_taxon_id": 7821,
    })

    after_row = _observation_row(db_path, obs_id)
    identity = TaxonIdentity.from_row(after_row)
    assert identity.is_proven_sporely is True
    assert identity.sporely_taxon_id == 7821
    assert identity.raw_external_id == "NBIC:53482"
    # The repaired name is not re-nulled by a cloud row that still has nulls.
    assert (after_row["genus"], after_row["species"]) == ("Entoloma", "conferendum")
    assert after_row["spore_data_visibility"] == "private"
    assert _table_digests(db_path, exclude={"observations"}) == before_tables
    changed = {key for key in before_row if before_row[key] != after_row[key]}
    # ``species_guess`` and ``source_type`` are pull-owned provenance the
    # remote row dictates; they are not identity and not microscopy.
    assert changed <= {
        "species_guess", "source_type", "updated_at", "synced_at", "sync_status",
    }


def test_the_pull_path_cannot_reach_an_identity_column():
    """Structural: identity is absent from the pulled observation snapshot.

    Asserted against the field list itself rather than only through one
    applied row, because a future field added to the snapshot would silently
    open this door again.
    """
    from utils import cloud_sync

    assert not (set(cloud_sync._SNAPSHOT_OBS_FIELDS) & set(IDENTITY_COLUMNS))
    assert "sporely_taxon_id" not in cloud_sync._SNAPSHOT_OBS_FIELDS


def test_a_later_correct_selection_does_not_rewrite_the_historical_snapshot(
    tmp_path, monkeypatch, artifact
):
    """The 917 semantics the plan spells out.

    The W3 identification snapshot recorded ``no_identity_evidence`` and that
    remains historically true: nothing was resolvable at the time. Stage 4
    records the newly proven current selection *beside* it. The desktop-side
    checkable form of "the snapshot is immutable" is that the entire repair
    plus push emits no write naming the snapshot or its resolution link.
    """
    from utils.cloud_sync import SporelyCloudClient

    db_path = _fresh_db(tmp_path, monkeypatch)
    obs_id = _build_917(db_path)
    report = audit(read_desktop_rows(db_path), artifact, origin="desktop")
    apply_repairs(report, observation_db_path=db_path, release_id=None)
    row = _observation_row(db_path, obs_id)
    row["cloud_id"] = "917"

    client = object.__new__(SporelyCloudClient)
    client.user_id = "00000000-0000-4000-8000-000000000001"
    client._resolve_existing_observation_for_push = lambda _obs, remote_obs=None: "917"
    written: list[str] = []
    client._patch = lambda *args, **kwargs: written.append(str(args))
    client._rpc = lambda name, payload: written.append(f"{name}:{payload}")

    client.push_observation(
        row, remote_obs={"id": 917, "selected_sporely_taxon_id": None}
    )

    joined = " ".join(written)
    assert "identification_snapshot" not in joined
    assert "resolution_link" not in joined
    assert "set_observation_selected_taxon_v2" in joined
    # The provider history the snapshot was derived from is also intact.
    assert row["ai_selected_scientific_name"] == "Entoloma conferendum"


# ── Part D ──────────────────────────────────────────────────────────────────


def test_nbic_53482_normalizes_to_the_nortaxa_tuple():
    """Part D step 1."""
    identity = TaxonIdentity.from_prefixed_external_id("NBIC:53482")
    assert (identity.source_system, identity.namespace, identity.external_id) == (
        "nortaxa", "nortaxa_taxon_id", "53482",
    )
    assert identity.raw_external_id == "NBIC:53482"
    assert identity.is_proven_sporely is False


def test_the_tuple_resolves_to_7821_through_the_stage_3_bridge(artifact):
    """Part D step 2."""
    assert artifact.resolve(
        source_system="nortaxa", namespace="nortaxa_taxon_id", external_id="53482"
    ) == {7821}
    # The rugosa regression rides the same bridge.
    assert artifact.resolve(
        source_system="nortaxa", namespace="nortaxa_taxon_id", external_id="52369"
    ) == {83668}


def test_the_saved_observation_carries_the_real_binomial(
    tmp_path, monkeypatch, artifact
):
    """Part D step 3, and step 5's desktop half.

    An observation with a genus and a species is identified. "Unidentified" is
    reserved for an observation that carries no name at all, so the repaired
    row cannot render as unidentified by any reading of its own fields.
    """
    db_path = _fresh_db(tmp_path, monkeypatch)
    obs_id = _build_917(db_path)
    report = audit(read_desktop_rows(db_path), artifact, origin="desktop")
    apply_repairs(report, observation_db_path=db_path, release_id=None)
    row = _observation_row(db_path, obs_id)
    assert row["genus"] == "Entoloma"
    assert row["species"] == "conferendum"
    assert f"{row['genus']} {row['species']}" == "Entoloma conferendum"


def test_the_norwegian_vernacular_is_available_for_display(artifact):
    """Part D step 4."""
    conn = sqlite3.connect(f"file:{artifact.path}?mode=ro", uri=True)
    names = {
        (lang, name)
        for lang, name in conn.execute(
            "SELECT language, name FROM vernacular_name WHERE taxon_id = 7821"
        )
    }
    conn.close()
    assert ("nb", "stjernesporet rødspore") in names
    assert ("nn", "stjernespora raudspore") in names


def test_an_unresolved_external_identity_is_a_state_not_an_absence():
    """Part D step 5's contract: unresolved is displayed, not blanked.

    The distinction the whole closeout rests on — an observation whose
    identifier did not resolve still has a name and an explicit state, and is
    not the same thing as an observation with no identity evidence.
    """
    unresolved = TaxonIdentity.from_prefixed_external_id(
        "NBIC:11111", scientific_name="Entoloma conferendum", rank="species",
    )
    assert unresolved.state == "external_unresolved"
    assert unresolved.is_unresolved is True
    assert unresolved.scientific_name == "Entoloma conferendum"
    assert TaxonIdentity.none().state == "no_identity_evidence"
    assert TaxonIdentity.none().is_unresolved is False


def test_a_deprecated_candidate_is_rejected_at_every_nesting_level():
    """Part D step 7, first half — required regardless of Stage 1's finding."""
    from ui.image_import_dialog import AIGuessWorker

    sentinel = AIGuessWorker._ARTSORAKEL_OUTDATED_SENTINEL
    payload = {
        "predictions": [
            {"taxa": {"items": [
                {"scientific_name_id": "NBIC:53482",
                 "scientificName": "Entoloma conferendum",
                 "probability": 0.99,
                 "vernacularName": sentinel},
                {"scientific_name_id": "NBIC:53482",
                 "scientificName": "Entoloma conferendum",
                 "probability": 0.91,
                 "taxon": {"vernacularName": sentinel}},
                {"scientific_name_id": "NBIC:53482",
                 "scientificName": "Entoloma conferendum",
                 "probability": 0.88,
                 "taxon": {"vernacularName": "stjernesporet rødspore"}},
            ]}},
        ],
    }
    flattened = AIGuessWorker._flatten_artsorakel_predictions(payload)
    # Both deprecated shapes of the SAME taxon are dropped; the live one stays.
    assert len(flattened) == 1
    assert flattened[0]["probability"] == 0.88


def test_a_malformed_candidate_never_clears_an_existing_identification():
    """Part D step 7, second half — the name-destroying half of the defect.

    A candidate that carries no usable identifier must leave the committed
    identification exactly as it was. This is the desktop analogue of the save
    path that wrote null ``genus``/``species``/``common_name`` over a working
    identification.
    """
    from PySide6.QtWidgets import QApplication, QLineEdit
    from ui.observations_tab import ObservationDetailsDialog
    from ui.taxon_input_controller import TaxonInputController

    QApplication.instance() or QApplication([])
    controller = TaxonInputController(
        lookup=None,
        genus_input=QLineEdit(),
        species_input=QLineEdit(),
        scientific_name_input=QLineEdit(),
    )
    controller.load_committed_snapshot({
        "genus": "Entoloma",
        "species": "conferendum",
        "scientific_name": "Entoloma conferendum",
        "taxon_rank_snapshot": "species",
        **TaxonIdentity.from_taxonomy_v2_artifact(7821).to_row(),
    })
    established = dict(controller.committed_snapshot())

    class _Host:
        _taxon_controller = controller
        _preserve_ai_external_taxon_identity = (
            ObservationDetailsDialog._preserve_ai_external_taxon_identity
        )

    host = _Host()
    for malformed in (
        {"id": "", "scientificName": ""},              # nothing at all
        {"id": None},                                   # no identifier
        {"id": "53482"},                                # namespace-lost integer
        {"id": "GBIF:53482"},                           # unregistered prefix
    ):
        assert _Host._preserve_ai_external_taxon_identity(
            host, malformed, "", "",
        ) is False
        assert controller.committed_snapshot() == established

    identity = TaxonIdentity.from_row(controller.committed_snapshot())
    assert identity.is_proven_sporely is True
    assert identity.sporely_taxon_id == 7821


def test_53482_round_trips_through_repair_push_and_pull(
    tmp_path, monkeypatch, artifact
):
    """Part D step 6, as one pass over the whole corrected path."""
    from utils import cloud_sync
    from utils.cloud_sync import SporelyCloudClient

    db_path = _fresh_db(tmp_path, monkeypatch)
    obs_id = _build_917(db_path)

    report = audit(read_desktop_rows(db_path), artifact, origin="desktop")
    apply_repairs(report, observation_db_path=db_path, release_id=None)

    row = _observation_row(db_path, obs_id)
    row["cloud_id"] = "917"
    client = object.__new__(SporelyCloudClient)
    client.user_id = "00000000-0000-4000-8000-000000000001"
    client._resolve_existing_observation_for_push = lambda _obs, remote_obs=None: "917"
    client._patch = lambda *args, **kwargs: None
    rpcs: list[tuple] = []
    client._rpc = lambda name, payload: rpcs.append((name, payload))
    client.push_observation(row, remote_obs={"id": 917,
                                             "selected_sporely_taxon_id": None})
    assert rpcs[0][1]["p_sporely_taxon_id"] == 7821

    # The cloud now reflects the selection, and the pull preserves it.
    cloud_sync._apply_remote_observation_fields(obs_id, {
        "id": 917, "desktop_id": obs_id, "date": "2026-09-14 10:30",
        "genus": "Entoloma", "species": "conferendum",
        "ai_selected_taxon_id": "NBIC:53482",
        "ai_selected_scientific_name": "Entoloma conferendum",
        "selected_sporely_taxon_id": 7821,
        "visibility": "private", "spore_data_visibility": "private",
    })

    final = _observation_row(db_path, obs_id)
    identity = TaxonIdentity.from_row(final)
    assert identity.is_proven_sporely is True
    assert identity.sporely_taxon_id == 7821
    assert identity.state == STATE_SPORELY
    assert (final["genus"], final["species"]) == ("Entoloma", "conferendum")

    # A second push is a no-op: the remote already carries the selection.
    rpcs.clear()
    final["cloud_id"] = "917"
    client.push_observation(final, remote_obs={"id": 917,
                                               "selected_sporely_taxon_id": 7821})
    assert rpcs == []


# ── The real Stage 3 artifact, when this machine has it ─────────────────────


@pytest.mark.skipif(
    not _STAGE3_PACK.exists(),
    reason="Stage 3 build output is not present on this machine",
)
def test_the_real_stage_3_artifact_backs_the_reconstruction():
    """Ties the portable fixtures above to the artifact Stage 3 actually built.

    Without this, every assertion in this module would be a statement about a
    reconstruction rather than about the release the closeout produced.
    """
    art = TaxonomyArtifact(_STAGE3_PACK)
    try:
        assert art.release_id == "tax-2026.09.23-01"
        assert art.resolve(
            source_system="nortaxa", namespace="nortaxa_taxon_id",
            external_id="53482",
        ) == {7821}
        assert art.resolve(
            source_system="nortaxa", namespace="nortaxa_taxon_id",
            external_id="52369",
        ) == {83668}
        assert art.contains(7821)
        conn = sqlite3.connect(f"file:{_STAGE3_PACK}?mode=ro", uri=True)
        names = {
            name for (name,) in conn.execute(
                "SELECT name FROM vernacular_name WHERE taxon_id = 7821"
            )
        }
        scientific = conn.execute(
            "SELECT scientific_name FROM taxon WHERE taxon_id = 7821"
        ).fetchone()[0]
        conn.close()
        assert scientific == "Entoloma conferendum"
        assert "stjernesporet rødspore" in names
        assert "stjernespora raudspore" in names
    finally:
        art.close()
