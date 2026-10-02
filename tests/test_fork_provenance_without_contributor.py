"""Fork provenance keeps no contributor name (owner decision B, 2026-10-02).

A copy of a shared contribution stores the served envelope without
``contributor`` (and without the live ``relationship_roles``); the server
(sporely-web 20261002150000) persists the same form. Older local copies that
still carry the contributor stay valid. The contributor is shown live.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3

import pytest

from database import schema
from database.curated_reference_forks import (
    _clear_contributor_cache_for_tests,
    contributor_label_for_fork,
    copy_curated_bundle_to_personal_library,
    normalize_curated_bundle,
    validate_frozen_curated_provenance,
)
from tests.test_curated_reference_forks import shared_row
from utils.curated_reference_sync import pull_curated_reference_forks, push_curated_reference_forks

TAXON = 2_100_000_081
LABEL = "User 1"


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    reference_path = tmp_path / "reference_values.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: tmp_path / "mushrooms.db")
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: reference_path)
    monkeypatch.setattr(schema, "get_bundled_reference_database_path", lambda: tmp_path / "missing.db")
    schema.init_database()
    _clear_contributor_cache_for_tests()
    yield reference_path
    _clear_contributor_cache_for_tests()


def _stored(reference_path):
    with sqlite3.connect(reference_path) as connection:
        return connection.execute(
            "SELECT curated_measurement_set_id, bundle_revision, sporely_taxon_id, "
            "source_envelope_json, source_sha256 FROM curated_reference_forks"
        ).fetchall()


def _canonical(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _old_form_envelope() -> dict:
    row = shared_row(["compared"])
    del row["relationship_roles"]
    return row  # still has contributor, as copies made before the decision


def test_copy_stores_no_contributor(isolated):
    bundle = normalize_curated_bundle(shared_row(["contradicts"]), expected_taxon_id=TAXON)
    assert bundle.contributor_label == LABEL  # live display still has it
    assert "contributor" not in bundle.source_envelope
    copy_curated_bundle_to_personal_library(bundle)
    [(set_id, revision, taxon, envelope_json, sha)] = _stored(isolated)
    envelope = json.loads(envelope_json)
    assert "contributor" not in envelope and "relationship_roles" not in envelope
    assert LABEL not in envelope_json
    assert envelope["contribution_id"] == set_id and envelope["revision"] == revision
    assert hashlib.sha256(envelope_json.encode("utf-8")).hexdigest() == sha
    assert envelope_json == _canonical(envelope)


def test_stored_form_without_contributor_validates(isolated):
    envelope = _canonical({k: v for k, v in _old_form_envelope().items() if k != "contributor"})
    row = shared_row()
    bundle = validate_frozen_curated_provenance(
        envelope, hashlib.sha256(envelope.encode()).hexdigest(),
        curated_measurement_set_id=row["contribution_id"], bundle_revision=row["revision"],
        sporely_taxon_id=TAXON,
    )
    assert bundle.contributor_label is None and bundle.contributor_id is None
    assert bundle.source_envelope == json.loads(envelope)


def test_old_local_copy_with_contributor_still_validates_and_recopy_is_idempotent(isolated):
    old = _canonical(_old_form_envelope())
    row = shared_row()
    validate_frozen_curated_provenance(
        old, hashlib.sha256(old.encode()).hexdigest(),
        curated_measurement_set_id=row["contribution_id"], bundle_revision=row["revision"],
        sporely_taxon_id=TAXON,
    )
    # A copy made before the decision (stored with the contributor) ...
    old_bundle = normalize_curated_bundle(json.loads(old), expected_taxon_id=TAXON, frozen=True)
    assert copy_curated_bundle_to_personal_library(old_bundle).created
    # ... is recognised by a copy of the same revision made now.
    again = copy_curated_bundle_to_personal_library(normalize_curated_bundle(shared_row()))
    assert again.created is False
    assert LABEL in _stored(isolated)[0][3]  # existing row untouched


def test_frozen_envelope_with_wrong_shape_is_still_rejected():
    from database.curated_reference_forks import CuratedReferenceError

    envelope = {k: v for k, v in _old_form_envelope().items() if k not in {"contributor", "exports"}}
    with pytest.raises(CuratedReferenceError):
        normalize_curated_bundle(envelope, expected_taxon_id=TAXON, frozen=True)


class Cloud:
    user_id = "user-b"

    def __init__(self):
        self.rows = []

    def sync_reference_curated_fork(self, payload, expected):
        row = {"user_id": self.user_id, **payload, "row_version": 1,
               "created_at": "2026-10-02T12:00:00Z", "updated_at": "2026-10-02T12:00:00Z"}
        self.rows.append(row)
        return {"status": "created", "row": row}

    def list_reference_curated_forks(self):
        return list(self.rows)


def test_push_sends_no_contributor_and_server_row_pulls(isolated):
    copy_curated_bundle_to_personal_library(normalize_curated_bundle(shared_row()))
    cloud = Cloud()
    with sqlite3.connect(isolated) as connection:
        connection.execute(
            "UPDATE reference_cloud_sync_state SET cloud_user_id=?,remote_identity_state='acknowledged',"
            "cloud_row_version=1,accepted_payload_json='{}',sync_status='clean'", (cloud.user_id,),
        )
    assert push_curated_reference_forks(cloud).pushed == 1
    sent = cloud.rows[0]["source_envelope_json"]
    assert "contributor" not in json.loads(sent) and LABEL not in sent
    with sqlite3.connect(isolated) as connection:
        connection.execute("DELETE FROM curated_reference_fork_cloud_sync_state")
        connection.execute("DELETE FROM curated_reference_forks")
    result = pull_curated_reference_forks(cloud)
    assert result.errors == () and result.pulled == 1
    assert _stored(isolated)[0][3] == sent


def test_db_share_bundle_round_trip_keeps_contributor_free_provenance(tmp_path, monkeypatch):
    from tests.test_reference_library_bundle_roundtrip import _isolate, _make_paths, _seed_source
    from utils import db_share

    source_paths, _obs, work, treatment, ms, _use = _seed_source(monkeypatch, tmp_path)
    row = shared_row()
    envelope = _canonical({k: v for k, v in row.items() if k not in {"contributor", "relationship_roles"}})
    with sqlite3.connect(source_paths["ref"]) as connection:
        connection.execute(
            "INSERT INTO curated_reference_forks (curated_measurement_set_id,bundle_revision,"
            "sporely_taxon_id,reference_work_id,taxon_treatment_id,reference_measurement_set_id,"
            "source_envelope_json,source_sha256) VALUES (?,?,?,?,?,?,?,?)",
            (row["contribution_id"], row["revision"], TAXON, work.id, treatment.id, ms.id,
             envelope, hashlib.sha256(envelope.encode()).hexdigest()),
        )
    bundle_path = tmp_path / "bundle.zip"
    db_share.export_database_bundle(
        str(bundle_path), include_observations=True, include_images=False,
        include_measurements=False, include_calibrations=False, include_reference_values=True,
    )
    dest = _make_paths(tmp_path, "dest")
    _isolate(dest, monkeypatch)
    schema.init_database()
    db_share.import_database_bundle(
        str(bundle_path), include_observations=True, include_images=False,
        include_measurements=False, include_calibrations=False, include_reference_values=True,
    )
    with sqlite3.connect(dest["ref"]) as connection:
        assert connection.execute(
            "SELECT source_envelope_json FROM curated_reference_forks"
        ).fetchall() == [(envelope,)]


class _MissingFallback(dict):
    """A stored contribution envelope that also answers the legacy id key the
    portable fixture reads, without serializing it."""

    def __missing__(self, key):
        if key == "curated_measurement_set_id":
            return self["contribution_id"]
        raise KeyError(key)


def test_portable_round_trip_keeps_contributor_free_provenance(monkeypatch, tmp_path):
    import tests.test_portable_import_preview as portable

    row = shared_row()
    stored = _MissingFallback({k: v for k, v in row.items() if k not in {"contributor", "relationship_roles"}})
    monkeypatch.setattr(portable, "bundle_row", lambda *a, **k: stored)
    archive = portable._archive(monkeypatch, tmp_path)
    main, reference = portable._database_pair(monkeypatch, tmp_path / "destination")
    portable.import_portable_archive(
        archive, destination_main_database=main, destination_reference_database=reference,
        destination_assets_root=tmp_path / "destination-assets", observation_ids={1, 2},
    )
    with sqlite3.connect(reference) as connection:
        [(envelope,)] = connection.execute(
            "SELECT source_envelope_json FROM curated_reference_forks"
        ).fetchall()
    assert "contributor" not in json.loads(envelope)


class Public:
    def __init__(self, rows=None, error=None):
        self.rows, self.error, self.calls = rows, error, 0

    def get_public_reference_contribution_v2(self, contribution_id, revision):
        self.calls += 1
        if self.error:
            raise self.error
        return self.rows


def test_contributor_resolved_live_and_cached(isolated):
    row = shared_row()
    client = Public([row])
    assert contributor_label_for_fork(client, row["contribution_id"], row["revision"]) == LABEL
    assert contributor_label_for_fork(client, row["contribution_id"], row["revision"]) == LABEL
    assert client.calls == 1


@pytest.mark.parametrize("rows", [
    [],
    [{"contribution_id": "68000000-0000-4000-8000-000000006701", "revision": 2,
      "status": "withdrawn", "withdrawn_at": "2026-10-02T00:00:00Z"}],
    None,
])
def test_unavailable_contributor_is_neutral_and_never_fatal(isolated, rows):
    from ui.curated_reference_catalogue_dialog import copied_reference_contributor_text

    row = shared_row()
    text = copied_reference_contributor_text(Public(rows), row["contribution_id"], row["revision"])
    assert text == "Unknown contributor"
    assert copied_reference_contributor_text(None, row["contribution_id"], 99) == "Unknown contributor"


def test_lookup_failure_is_neutral_and_retried_later(isolated):
    from ui.curated_reference_catalogue_dialog import copied_reference_contributor_text

    row = shared_row()
    failing = Public(error=RuntimeError("offline"))
    assert copied_reference_contributor_text(failing, row["contribution_id"], row["revision"]) == "Unknown contributor"
    assert contributor_label_for_fork(Public([row]), row["contribution_id"], row["revision"]) == LABEL


def test_deleted_contributor_label_is_translated_text(isolated):
    from ui.curated_reference_catalogue_dialog import copied_reference_contributor_text

    row = shared_row()
    deleted = {**row, "contributor": {"id": None, "label": "Deleted user"}}
    assert copied_reference_contributor_text(Public([deleted]), row["contribution_id"], row["revision"]) == "Deleted user"


def _server_text(envelope_json: str) -> str:
    """The server's ``(jsonb - 'contributor')::text`` form: ', '/': '
    separators and jsonb key order (shorter keys first), not the desktop's."""
    def order(value):
        if isinstance(value, dict):
            return {k: order(value[k]) for k in sorted(value, key=lambda k: (len(k.encode()), k))}
        if isinstance(value, list):
            return [order(item) for item in value]
        return value
    value = json.loads(envelope_json)
    value.pop("contributor", None)
    return json.dumps(order(value), ensure_ascii=False)


class ServerFormCloud(Cloud):
    """Stores the envelope in the server's own text form with its own sha."""

    def sync_reference_curated_fork(self, payload, expected):
        existing = next((r for r in self.rows
                         if r["curated_measurement_set_id"] == payload["curated_measurement_set_id"]), None)
        if existing:
            return {"status": "no_change", "row": existing}
        text = _server_text(payload["source_envelope_json"])
        stored = {**payload, "source_envelope_json": text,
                  "source_sha256": hashlib.sha256(text.encode()).hexdigest()}
        return super().sync_reference_curated_fork(stored, expected)


def _mark_graph_converged(reference_path, cloud):
    with sqlite3.connect(reference_path) as connection:
        connection.execute(
            "UPDATE reference_cloud_sync_state SET cloud_user_id=?,remote_identity_state='acknowledged',"
            "cloud_row_version=1,accepted_payload_json='{}',sync_status='clean'", (cloud.user_id,),
        )


def test_server_stored_text_form_is_acknowledged_not_a_conflict(isolated):
    copy_curated_bundle_to_personal_library(normalize_curated_bundle(shared_row()))
    cloud = ServerFormCloud()
    _mark_graph_converged(isolated, cloud)

    first = push_curated_reference_forks(cloud)
    assert first.pushed == 1 and first.conflicts == () and first.errors == ()
    local_text = _stored(isolated)[0][3]
    assert cloud.rows[0]["source_envelope_json"] != local_text  # different text, same provenance
    pulled = pull_curated_reference_forks(cloud)
    assert pulled.conflicts == () and pulled.errors == ()
    assert _stored(isolated)[0][3] == local_text  # local provenance stays immutable
    with sqlite3.connect(isolated) as connection:
        assert connection.execute(
            "SELECT sync_status FROM curated_reference_fork_cloud_sync_state"
        ).fetchall() == [("clean",)]


def test_older_local_copy_with_contributor_matches_the_server_form(isolated):
    old = normalize_curated_bundle(json.loads(_canonical(_old_form_envelope())), expected_taxon_id=TAXON, frozen=True)
    copy_curated_bundle_to_personal_library(old)
    cloud = ServerFormCloud()
    _mark_graph_converged(isolated, cloud)
    result = push_curated_reference_forks(cloud)
    assert result.pushed == 1 and result.conflicts == ()
    assert "contributor" not in json.loads(cloud.rows[0]["source_envelope_json"])


def test_second_device_pulls_the_server_form(isolated):
    copy_curated_bundle_to_personal_library(normalize_curated_bundle(shared_row()))
    cloud = ServerFormCloud()
    _mark_graph_converged(isolated, cloud)
    push_curated_reference_forks(cloud)
    with sqlite3.connect(isolated) as connection:
        connection.execute("DELETE FROM curated_reference_fork_cloud_sync_state")
        connection.execute("DELETE FROM curated_reference_forks")
    result = pull_curated_reference_forks(cloud)
    assert result.pulled == 1 and result.errors == ()
    assert _stored(isolated)[0][3] == cloud.rows[0]["source_envelope_json"]


@pytest.mark.parametrize("tamper", ["content", "digest"])
def test_different_provenance_is_still_a_conflict(isolated, tamper):
    copy_curated_bundle_to_personal_library(normalize_curated_bundle(shared_row()))
    cloud = ServerFormCloud()
    _mark_graph_converged(isolated, cloud)
    push_curated_reference_forks(cloud)
    row = cloud.rows[0]
    if tamper == "content":
        value = json.loads(row["source_envelope_json"])
        value["snapshot"]["raw_text"] = "changed"
        row["source_envelope_json"] = json.dumps(value)
        row["source_sha256"] = hashlib.sha256(row["source_envelope_json"].encode()).hexdigest()
    else:
        row["source_sha256"] = "0" * 64
    result = pull_curated_reference_forks(cloud)
    assert result.conflicts or result.errors
