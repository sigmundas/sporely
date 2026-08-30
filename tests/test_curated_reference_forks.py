from __future__ import annotations

import copy
import json
import sqlite3

import pytest

from database import schema
from database.curated_reference_forks import (
    CuratedReferenceError,
    copy_curated_bundle_to_personal_library,
    normalize_curated_bundle,
    search_curated_catalogue,
    submit_personal_reference_for_curation,
    validate_frozen_curated_provenance,
)
from database.reference_library import (
    MeasurementSetRepository,
    ObservationReferenceUseRepository,
    ReferenceWorkRepository,
    TaxonTreatmentRepository,
)


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    main_path = tmp_path / "mushrooms.db"
    reference_path = tmp_path / "reference_values.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: main_path)
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: reference_path)
    monkeypatch.setattr(schema, "get_bundled_reference_database_path", lambda: tmp_path / "missing.db")
    schema.init_database()
    return reference_path


def bundle_row(*, revision: int = 2) -> dict:
    set_id = "68000000-0000-4000-8000-000000006701"
    return {
        "curated_measurement_set_id": set_id,
        "bundle_revision": revision,
        "status": "published",
        "superseded_by_id": None,
        "published_at": "2026-08-29T12:00:00+00:00",
        "sporely_taxon_id": 2_100_000_081,
        "canonical_scientific_name": "Russula publicata",
        "snapshot": {
            "schema_version": 1,
            "reference_work_id": "11000000-0000-4000-8000-000000000001",
            "reference_treatment_id": "22000000-0000-4000-8000-000000000001",
            "reference_measurement_set_id": set_id,
            "reference_revision": revision,
            "short_label": "Public 2026",
            "full_citation": "Public, A. (2026). Alpha public set.",
            "work_type": "article",
            "year": 2026,
            "doi": "10.1000/stage6g",
            "isbn": None,
            "taxon_id": None,
            "name_as_published": "Russula publicata",
            "locator_text": "p. 8–10",
            "page_from": 8,
            "page_to": 10,
            "character": "spore_size",
            "data_kind": "range",
            "raw_text": "(7–)8–10 × 5–6 µm",
            "measurements": {
                "length_min": 7.0, "length_core_min": 8.0,
                "length_core_max": 10.0, "length_max": None,
                "width_min": None, "width_core_min": 5.0,
                "width_core_max": 6.0, "width_max": None,
                "q_min": None, "q_max": None, "q_mean": None,
                "length_mean": None, "width_mean": None,
                "sample_size": 40, "specimen_count": 2,
            },
            "method": {
                "mount_medium": "Melzer", "stain": None,
                "preparation": "spore print",
                "measurement_method": "excluding ornamentation",
            },
            "raw_points": None,
        },
        "citation": {
            "schema_version": 1, "citation_key": "Public2026", "type": "article",
            "authors": [{"family": "Public", "given": "Ada"}], "editors": [],
            "title": "Alpha public set", "container_title": "Mycological Journal",
            "year": 2026, "edition": None, "publisher": None, "place": None,
            "volume": "12", "issue": "3", "pages": "8–10",
            "doi": "10.1000/stage6g", "isbn": None,
            "url": "HTTPS://example.test/source", "language": "en",
            "short_citation": "Public 2026",
            "full_citation": "Public, A. (2026). Alpha public set.",
        },
        "exports": {
            "plain_text": "Public, A. (2026). Alpha public set.",
            "bibtex": "@article{Public2026}\n",
            "csl_json": {"id": "Public2026", "type": "article-journal", "title": "Alpha public set", "DOI": "10.1000/stage6g"},
        },
    }


class Client:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def search_public_curated_reference_sets(self, taxon_id, limit, after_published_at, after_id):
        self.calls.append((taxon_id, limit, after_published_at, after_id))
        return self.rows

    def search_public_reference_contributions(self, taxon_id, limit, after_shared_at, after_id):
        self.calls.append((taxon_id, limit, after_shared_at, after_id))
        return self.rows

    def submit_private_reference_for_curation(self, *args):
        self.calls.append(args)
        return self.rows


def test_catalogue_read_requires_exact_positive_taxon_and_rejects_expansion():
    row = bundle_row()
    client = Client([row])
    assert search_curated_catalogue(client, 2_100_000_081, limit=20)[0].sporely_taxon_id == 2_100_000_081
    assert client.calls == [(2_100_000_081, 20, None, None)]
    with pytest.raises(CuratedReferenceError):
        search_curated_catalogue(client, 0)
    expanded = copy.deepcopy(row)
    expanded["owner_id"] = "private"
    with pytest.raises(CuratedReferenceError):
        search_curated_catalogue(Client([expanded]), 2_100_000_081)


def test_shared_contribution_keeps_attribution_and_legacy_fork_compatibility():
    legacy = bundle_row()
    shared = {
        "contribution_id": legacy["curated_measurement_set_id"],
        "revision": legacy["bundle_revision"],
        "status": "shared",
        "shared_at": legacy["published_at"],
        "sporely_taxon_id": legacy["sporely_taxon_id"],
        "canonical_scientific_name": legacy["canonical_scientific_name"],
        "contributor": {
            "id": "00000000-0000-4000-8000-00000000c101",
            "label": "User 1",
        },
        "snapshot": legacy["snapshot"],
        "citation": legacy["citation"],
        "exports": legacy["exports"],
    }
    bundle = search_curated_catalogue(Client([shared]), 2_100_000_081)[0]
    assert bundle.contribution_id == legacy["curated_measurement_set_id"]
    assert bundle.revision == legacy["bundle_revision"]
    assert bundle.contributor_label == "User 1"


def test_shared_search_does_not_require_legacy_catalogue_method():
    class SharedOnlyClient:
        def search_public_reference_contributions(
            self, taxon_id, limit, after_shared_at, after_id,
        ):
            assert (taxon_id, limit, after_shared_at, after_id) == (
                2_100_000_081, 20, None, None,
            )
            legacy = bundle_row()
            return [{
                "contribution_id": legacy["curated_measurement_set_id"],
                "revision": legacy["bundle_revision"],
                "status": "shared",
                "shared_at": legacy["published_at"],
                "sporely_taxon_id": legacy["sporely_taxon_id"],
                "canonical_scientific_name": legacy["canonical_scientific_name"],
                "contributor": {
                    "id": "00000000-0000-4000-8000-00000000c101",
                    "label": "User 1",
                },
                "snapshot": legacy["snapshot"],
                "citation": legacy["citation"],
                "exports": legacy["exports"],
            }]

    result = search_curated_catalogue(SharedOnlyClient(), 2_100_000_081)
    assert result[0].contributor_label == "User 1"


@pytest.mark.parametrize(
    ("mutate"),
    [
        lambda row: row["snapshot"].__setitem__("page_from", "not-an-integer"),
        lambda row: row["citation"].__setitem__("year", {"unexpected": True}),
        lambda row: row["citation"]["authors"][0].__setitem__("private", "value"),
        lambda row: row["citation"]["authors"][0].__setitem__("family", "x" * 1025),
        lambda row: row["citation"]["authors"][0].__setitem__("family", "🍄" * 513),
        lambda row: row["citation"].__setitem__("url", "file:///private/source"),
        lambda row: row["exports"]["csl_json"].__setitem__("private", "value"),
        lambda row: row["exports"]["csl_json"].__setitem__("author", {"private": "value"}),
        lambda row: row["exports"]["csl_json"].__setitem__("issued", "2026"),
        lambda row: row["exports"]["csl_json"].__setitem__("ISBN", {"value": "x"}),
        lambda row: row["exports"]["csl_json"].__setitem__("URL", "javascript:alert(1)"),
        lambda row: (row["citation"].__setitem__("citation_key", None), row["exports"]["csl_json"].__setitem__("id", None)),
        lambda row: row["exports"]["csl_json"].__setitem__("author", "Private Person"),
        lambda row: row["exports"]["csl_json"].__setitem__("author", [{"family": None}]),
        lambda row: row["exports"]["csl_json"].__setitem__("type", "book"),
        lambda row: row["exports"]["csl_json"].__setitem__("DOI", "10.1000/different"),
        lambda row: row["citation"].__setitem__("doi", "not-a-doi"),
        lambda row: row.__setitem__("published_at", "not-a-timestamp"),
    ],
)
def test_catalogue_nested_payload_is_fully_typed_and_allowlisted(mutate):
    row = bundle_row()
    mutate(row)
    with pytest.raises(CuratedReferenceError):
        normalize_curated_bundle(row)


def test_imported_frozen_provenance_rejects_tampered_digest():
    source = json.dumps(bundle_row(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    with pytest.raises(CuratedReferenceError, match="digest mismatch"):
        validate_frozen_curated_provenance(
            source, "0" * 64,
            curated_measurement_set_id=bundle_row()["curated_measurement_set_id"],
            bundle_revision=2,
            sporely_taxon_id=2_100_000_081,
        )


def test_copy_is_atomic_idempotent_and_new_revision_gets_fresh_graph(isolated):
    first = copy_curated_bundle_to_personal_library(normalize_curated_bundle(bundle_row()))
    replay = copy_curated_bundle_to_personal_library(normalize_curated_bundle(bundle_row()))
    newer = copy_curated_bundle_to_personal_library(normalize_curated_bundle(bundle_row(revision=3)))
    assert first.created is True and replay.created is False and newer.created is True
    assert replay.reference_measurement_set_id == first.reference_measurement_set_id
    assert newer.reference_measurement_set_id != first.reference_measurement_set_id
    assert first.reference_measurement_set_id != first.curated_measurement_set_id
    assert ReferenceWorkRepository.get(first.reference_work_id).revision == 1
    assert TaxonTreatmentRepository.get(first.taxon_treatment_id).taxon_id == "2100000081"
    assert MeasurementSetRepository.get(first.reference_measurement_set_id).raw_text == "(7–)8–10 × 5–6 µm"


def test_reimport_never_overwrites_edited_fork_or_frozen_source(isolated):
    bundle = normalize_curated_bundle(bundle_row())
    fork = copy_curated_bundle_to_personal_library(bundle)
    MeasurementSetRepository.update(fork.reference_measurement_set_id, {"raw_text": "owner edit"})
    replay = copy_curated_bundle_to_personal_library(bundle)
    assert replay.created is False
    assert MeasurementSetRepository.get(fork.reference_measurement_set_id).raw_text == "owner edit"
    with sqlite3.connect(isolated) as connection:
        frozen = connection.execute("SELECT source_envelope_json FROM curated_reference_forks").fetchone()[0]
    assert "(7–)8–10 × 5–6 µm" in frozen
    assert "owner edit" not in frozen


def test_copied_fork_attaches_through_normal_frozen_snapshot_path(isolated):
    fork = copy_curated_bundle_to_personal_library(normalize_curated_bundle(bundle_row()))
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute(
            "INSERT INTO observations (id, date, genus, species) "
            "VALUES (1, '2026-08-30', 'Russula', 'publicata')"
        )
    use = ObservationReferenceUseRepository.attach(1, fork.reference_measurement_set_id)
    frozen = use.snapshot_json
    MeasurementSetRepository.update(fork.reference_measurement_set_id, {"raw_text": "owner edit"})
    assert use.reference_revision == 1
    assert ObservationReferenceUseRepository.get(use.id).snapshot_json == frozen
    assert json.loads(frozen)["reference_measurement_set_id"] == fork.reference_measurement_set_id


@pytest.mark.parametrize("delete_graph", ["set", "work"])
def test_owner_can_delete_unattached_copied_fork(isolated, delete_graph):
    fork = copy_curated_bundle_to_personal_library(normalize_curated_bundle(bundle_row()))
    if delete_graph == "set":
        MeasurementSetRepository.delete(fork.reference_measurement_set_id)
    else:
        ReferenceWorkRepository.delete(fork.reference_work_id)
    with sqlite3.connect(isolated) as connection:
        assert connection.execute("SELECT COUNT(*) FROM curated_reference_forks").fetchone()[0] == 0


def test_copy_rolls_back_whole_graph_when_provenance_insert_fails(isolated, monkeypatch):
    bundle = normalize_curated_bundle(bundle_row())
    with sqlite3.connect(isolated) as connection:
        connection.execute(
            "CREATE TRIGGER fail_curated_fork BEFORE INSERT ON curated_reference_forks "
            "BEGIN SELECT RAISE(ABORT, 'forced'); END"
        )
    with pytest.raises(Exception):
        copy_curated_bundle_to_personal_library(bundle)
    with sqlite3.connect(isolated) as connection:
        assert connection.execute("SELECT count(*) FROM reference_works").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM reference_taxon_treatments").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM reference_measurement_sets").fetchone()[0] == 0


def test_submit_action_sends_server_read_ids_and_exact_current_revisions(isolated):
    fork = copy_curated_bundle_to_personal_library(normalize_curated_bundle(bundle_row()))
    client = Client({
        "status": "created",
        "submission": {
            "id": "78000000-0000-4000-8000-000000006701",
            "status": "submitted",
            "candidate_revision": 1,
            "content_hash": "a" * 64,
            "attestation_version": "rights-v1",
            "row_version": 1,
            "created_at": "2026-08-30T12:00:00Z",
            "updated_at": "2026-08-30T12:00:00Z",
        },
    })
    result = submit_personal_reference_for_curation(
        client, fork.reference_measurement_set_id,
        attestation_version="rights-v1", rights_confirmed=True,
        curation_consent_confirmed=True,
    )
    assert result.status == "created"
    assert client.calls == [(
        fork.reference_measurement_set_id, 1, 1, 1, "rights-v1", True, True,
    )]


def test_submit_action_requires_both_explicit_confirmations(isolated):
    fork = copy_curated_bundle_to_personal_library(normalize_curated_bundle(bundle_row()))
    client = Client({})
    with pytest.raises(CuratedReferenceError):
        submit_personal_reference_for_curation(
            client, fork.reference_measurement_set_id,
            attestation_version="rights-v1", rights_confirmed=True,
            curation_consent_confirmed=False,
        )
    assert client.calls == []
