from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

from database import migrate, models, schema
from ui import observations_tab
import utils.cloud_sync as cloud_sync


def _table_columns(db_path: Path) -> list[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("PRAGMA table_info(observations)").fetchall()
    return [str(row[1] or "") for row in rows]


def _init_fresh_database(tmp_path: Path, monkeypatch) -> Path:
    db_path = tmp_path / "mushrooms.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(schema, "init_reference_database", lambda *args, **kwargs: None)
    schema.init_database()
    return db_path


def _patch_connection_helpers(monkeypatch, db_path: Path) -> None:
    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))


def test_init_database_creates_ai_selected_columns(tmp_path, monkeypatch) -> None:
    db_path = _init_fresh_database(tmp_path, monkeypatch)

    columns = _table_columns(db_path)
    assert "ai_selected_service" in columns
    assert "ai_selected_taxon_id" in columns
    assert "ai_selected_scientific_name" in columns
    assert "ai_selected_probability" in columns
    assert "ai_selected_at" in columns


def test_migrate_database_adds_ai_selected_columns_idempotently(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                genus TEXT,
                species TEXT
            );
            INSERT INTO observations (id, date, genus, species) VALUES (1, '2026-05-01', 'Entoloma', 'clypeatum');
            """
        )
        conn.commit()

    monkeypatch.setattr(migrate, "get_database_path", lambda: db_path)
    monkeypatch.setattr(migrate, "backup_database", lambda: False)

    migrate.migrate_database()
    first_columns = _table_columns(db_path)

    migrate.migrate_database()
    second_columns = _table_columns(db_path)

    for column_name in (
        "ai_selected_service",
        "ai_selected_taxon_id",
        "ai_selected_scientific_name",
        "ai_selected_probability",
        "ai_selected_at",
    ):
        assert column_name in first_columns
        assert column_name in second_columns


def test_cloud_push_and_pull_carry_selected_ai_fields(tmp_path, monkeypatch) -> None:
    db_path = _init_fresh_database(tmp_path, monkeypatch)
    _patch_connection_helpers(monkeypatch, db_path)
    monkeypatch.setattr(
        cloud_sync,
        "_import_remote_measurements_for_observation",
        lambda *args, **kwargs: {"warnings": [], "conflict": False, "imported": 0},
    )

    cloud_obs = {
        "id": "cloud-123",
        "date": "2026-05-01",
        "genus": "Entoloma",
        "species": "clypeatum",
        "common_name": "ask",
        "species_guess": "Entoloma clypeatum",
        "location": "Forest",
        "habitat": None,
        "notes": None,
        "open_comment": None,
        "interesting_comment": False,
        "visibility": "public",
        "sharing_scope": "public",
        "location_public": True,
        "is_draft": False,
        "location_precision": "exact",
        "spore_data_visibility": "public",
        "uncertain": False,
        "unspontaneous": False,
        "gps_latitude": 63.0,
        "gps_longitude": 10.0,
        "publish_target": None,
        "determination_method": None,
        "source_type": "personal",
        "author": "Tester",
        "ai_selected_service": "inat",
        "ai_selected_taxon_id": "12345",
        "ai_selected_scientific_name": "Entoloma clypeatum",
        "ai_selected_probability": 0.97,
        "ai_selected_at": "2026-05-01T12:34:56Z",
    }

    local_id = cloud_sync._create_local_from_remote(
        cloud_obs,
        materialize_remote_images=False,
    )
    local_row = models.ObservationDB.get_observation(local_id)
    assert local_row is not None
    assert local_row["ai_selected_service"] == "inat"
    assert local_row["ai_selected_taxon_id"] == "12345"
    assert local_row["ai_selected_scientific_name"] == "Entoloma clypeatum"
    assert local_row["ai_selected_probability"] == 0.97
    assert local_row["ai_selected_at"] == "2026-05-01T12:34:56Z"

    updated_remote = dict(cloud_obs)
    updated_remote.update(
        {
            "ai_selected_service": "artsorakel",
            "ai_selected_taxon_id": "67890",
            "ai_selected_scientific_name": "Entoloma rhodopolium",
            "ai_selected_probability": 0.81,
            "ai_selected_at": "2026-05-02T09:10:11Z",
        }
    )
    cloud_sync._apply_remote_observation_fields(
        local_id,
        updated_remote,
        fields={
            "ai_selected_service",
            "ai_selected_taxon_id",
            "ai_selected_scientific_name",
            "ai_selected_probability",
            "ai_selected_at",
        },
    )
    updated_row = models.ObservationDB.get_observation(local_id)
    assert updated_row is not None
    assert updated_row["ai_selected_service"] == "artsorakel"
    assert updated_row["ai_selected_taxon_id"] == "67890"
    assert updated_row["ai_selected_scientific_name"] == "Entoloma rhodopolium"
    assert updated_row["ai_selected_probability"] == 0.81
    assert updated_row["ai_selected_at"] == "2026-05-02T09:10:11Z"

    client = cloud_sync.SporelyCloudClient.__new__(cloud_sync.SporelyCloudClient)
    client.user_id = "user-123"
    captured: dict[str, dict] = {}
    client._find_cloud_observation = lambda desktop_id: None

    def fake_post(path: str, payload: dict) -> list[dict]:
        captured["path"] = path
        captured["payload"] = dict(payload)
        return [{"id": "cloud-999"}]

    client._post = fake_post

    pushed_cloud_id = client.push_observation(
        {
            "id": local_id,
            "date": "2026-05-01",
            "genus": "Entoloma",
            "species": "clypeatum",
            "common_name": "ask",
            "species_guess": "Entoloma clypeatum",
            "location": "Forest",
            "gps_latitude": 63.0,
            "gps_longitude": 10.0,
            "location_public": True,
            "is_draft": False,
            "sharing_scope": "public",
            "location_precision": "exact",
            "spore_data_visibility": "public",
            "uncertain": False,
            "unspontaneous": False,
            "interesting_comment": False,
            "determination_method": None,
            "habitat": None,
            "habitat_nin2_path": None,
            "habitat_substrate_path": None,
            "habitat_host_genus": None,
            "habitat_host_species": None,
            "habitat_host_common_name": None,
            "habitat_nin2_note": None,
            "habitat_substrate_note": None,
            "habitat_grows_on_note": None,
            "notes": None,
            "open_comment": None,
            "publish_target": None,
            "artsdata_id": None,
            "artportalen_id": None,
            "inaturalist_id": None,
            "mushroomobserver_id": None,
            "spore_statistics": None,
            "auto_threshold": None,
            "source_type": "personal",
            "citation": None,
            "data_provider": None,
            "author": "Tester",
            "ai_selected_service": "inat",
            "ai_selected_taxon_id": "12345",
            "ai_selected_scientific_name": "Entoloma clypeatum",
            "ai_selected_probability": 0.97,
            "ai_selected_at": "2026-05-01T12:34:56Z",
        }
    )

    assert pushed_cloud_id == "cloud-999"
    assert captured["path"] == "observations"
    assert captured["payload"]["ai_selected_service"] == "inat"
    assert captured["payload"]["ai_selected_taxon_id"] == "12345"
    assert captured["payload"]["ai_selected_scientific_name"] == "Entoloma clypeatum"
    assert captured["payload"]["ai_selected_probability"] == 0.97
    assert captured["payload"]["ai_selected_at"] == "2026-05-01T12:34:56Z"


def test_cloud_pull_backfills_taxonomy_from_selected_ai_scientific_name(tmp_path, monkeypatch) -> None:
    db_path = _init_fresh_database(tmp_path, monkeypatch)
    _patch_connection_helpers(monkeypatch, db_path)
    monkeypatch.setattr(
        cloud_sync,
        "_import_remote_measurements_for_observation",
        lambda *args, **kwargs: {"warnings": [], "conflict": False, "imported": 0},
    )

    cloud_obs = {
        "id": "cloud-124",
        "date": "2026-05-01",
        "genus": "",
        "species": "",
        "common_name": "ask",
        "species_guess": "",
        "location": "Forest",
        "habitat": None,
        "notes": None,
        "open_comment": None,
        "interesting_comment": False,
        "visibility": "public",
        "sharing_scope": "public",
        "location_public": True,
        "is_draft": False,
        "location_precision": "exact",
        "spore_data_visibility": "public",
        "uncertain": False,
        "unspontaneous": False,
        "gps_latitude": 63.0,
        "gps_longitude": 10.0,
        "publish_target": None,
        "determination_method": None,
        "source_type": "personal",
        "author": "Tester",
        "ai_selected_service": "inat",
        "ai_selected_taxon_id": "12345",
        "ai_selected_scientific_name": "Entoloma clypeatum",
        "ai_selected_probability": 0.97,
        "ai_selected_at": "2026-05-01T12:34:56Z",
    }

    local_id = cloud_sync._create_local_from_remote(
        cloud_obs,
        materialize_remote_images=False,
    )
    local_row = models.ObservationDB.get_observation(local_id)
    assert local_row is not None
    assert local_row["genus"] == "Entoloma"
    assert local_row["species"] == "clypeatum"
    assert local_row["species_guess"] == "Entoloma clypeatum"
    assert local_row["ai_selected_scientific_name"] == "Entoloma clypeatum"


def test_clear_identification_is_atomic_and_retains_raw_ai_state(tmp_path, monkeypatch) -> None:
    db_path = _init_fresh_database(tmp_path, monkeypatch)
    _patch_connection_helpers(monkeypatch, db_path)
    raw_ai_state = '{"predictions":{"0":[{"scientificName":"Entoloma clypeatum"}]}}'
    local_id = models.ObservationDB.create_observation(
        date="2026-05-01",
        genus="Entoloma",
        species="clypeatum",
        common_name="ask",
        species_guess="Entoloma clypeatum",
        uncertain=True,
        determination_method=1,
        inaturalist_taxon_id=12345,
        red_list_category="NT",
        red_list_categories_json='{"NO":"NT"}',
        ai_state_json=raw_ai_state,
        ai_selected_service="inat",
        ai_selected_taxon_id="12345",
        ai_selected_scientific_name="Entoloma clypeatum",
        ai_selected_probability=0.97,
        ai_selected_at="2026-05-01T12:34:56Z",
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE observations SET cloud_id = 'cloud-123', sync_status = 'synced' WHERE id = ?",
            (local_id,),
        )
        conn.commit()

    models.ObservationDB.clear_observation_identification(local_id)

    row = models.ObservationDB.get_observation(local_id)
    assert row is not None
    for field in (
        "genus",
        "species",
        "common_name",
        "species_guess",
        "determination_method",
        "inaturalist_taxon_id",
        "red_list_category",
        "red_list_categories_json",
        "ai_selected_service",
        "ai_selected_taxon_id",
        "ai_selected_scientific_name",
        "ai_selected_probability",
        "ai_selected_at",
    ):
        assert row[field] is None
    assert row["uncertain"] == 0
    assert row["ai_state_json"] == raw_ai_state
    assert row["sync_status"] == "dirty"


def test_explicit_identification_nulls_are_not_restored_from_cloud() -> None:
    local = {
        "id": 7,
        "genus": None,
        "species": None,
        "common_name": None,
        "species_guess": None,
        "ai_selected_service": None,
        "ai_selected_taxon_id": None,
        "ai_selected_scientific_name": None,
        "ai_selected_probability": None,
        "ai_selected_at": None,
        "red_list_category": None,
        "red_list_categories_json": None,
    }
    remote = {
        "genus": "Entoloma",
        "species": "clypeatum",
        "common_name": "ask",
        "species_guess": "Entoloma clypeatum",
        "ai_selected_service": "inat",
        "ai_selected_taxon_id": "12345",
        "ai_selected_scientific_name": "Entoloma clypeatum",
        "ai_selected_probability": 0.97,
        "ai_selected_at": "2026-05-01T12:34:56Z",
        "red_list_category": "NT",
        "red_list_categories_json": {"NO": "NT"},
    }

    merged = cloud_sync._merge_cloud_selected_ai_fields(local, remote)
    payload = cloud_sync._observation_push_payload(merged, local=True)

    for field in (
        "genus",
        "species",
        "common_name",
        "species_guess",
        "ai_selected_service",
        "ai_selected_taxon_id",
        "ai_selected_scientific_name",
        "ai_selected_probability",
        "ai_selected_at",
        "red_list_category",
        "red_list_categories_json",
    ):
        assert payload[field] is None

    client = cloud_sync.SporelyCloudClient.__new__(cloud_sync.SporelyCloudClient)
    client.user_id = "user-123"
    client._find_cloud_observation = lambda desktop_id: "cloud-123"
    captured: dict[str, object] = {}

    def fake_patch(path: str, patch_payload: dict) -> None:
        captured["path"] = path
        captured["payload"] = dict(patch_payload)

    client._patch = fake_patch
    client.push_observation(merged, remote_obs=remote)

    assert captured["path"] == "observations?id=eq.cloud-123"
    patch_payload = captured["payload"]
    assert isinstance(patch_payload, dict)
    for field in (
        "genus",
        "species",
        "common_name",
        "species_guess",
        "ai_selected_service",
        "ai_selected_taxon_id",
        "ai_selected_scientific_name",
        "ai_selected_probability",
        "ai_selected_at",
        "red_list_category",
        "red_list_categories_json",
    ):
        assert patch_payload[field] is None


def test_offline_pull_keeps_ai_clear_dirty_until_it_can_be_pushed() -> None:
    field_changes = {
        "local_payload": {
            "genus": None,
            "species": None,
            "common_name": None,
            "species_guess": None,
        },
        "local_only_fields": [
            "ai_selected_service",
            "ai_selected_taxon_id",
            "ai_selected_scientific_name",
            "ai_selected_probability",
            "ai_selected_at",
        ],
        "conflict_fields": [],
    }

    assert cloud_sync._remaining_local_changes_after_remote_merge(
        field_changes,
        local_media_changed=False,
    )


def test_cloud_identification_rows_hydrate_desktop_ai_state(tmp_path, monkeypatch) -> None:
    db_path = _init_fresh_database(tmp_path, monkeypatch)
    _patch_connection_helpers(monkeypatch, db_path)

    cloud_rows = [
        {
            "id": "row-arts",
            "service": "artsorakel",
            "status": "success",
            "created_at": "2026-05-02T12:00:00Z",
            "top_species_url": "https://artsdatabanken.no/arter/takson/213194",
            "results": [
                {
                    "scientific_name": "Cantharellus cibarius",
                    "vernacular_name": "Kantarell",
                    "taxon_id": "NBIC:1001",
                    "probability": 0.96,
                    "species_url": "https://artsdatabanken.no/arter/takson/213194",
                },
            ],
            "top_scientific_name": "Cantharellus cibarius",
            "top_vernacular_name": "Kantarell",
            "top_taxon_id": "NBIC:1001",
            "top_probability": 0.96,
        },
        {
            "id": "row-inat",
            "service": "inat",
            "status": "success",
            "created_at": "2026-05-02T12:01:00Z",
            "results": [
                {
                    "scientificName": "Cantharellus cibarius",
                    "vernacularName": "Chanterelle",
                    "taxonId": "12345",
                    "probability": 0.89,
                },
            ],
            "top_scientific_name": "Cantharellus cibarius",
            "top_vernacular_name": "Chanterelle",
            "top_taxon_id": "12345",
            "top_probability": 0.89,
        },
    ]

    fake_client = SimpleNamespace(
        pull_observation_identifications=lambda cloud_id: cloud_rows if cloud_id == "cloud-123" else [],
    )
    monkeypatch.setattr(observations_tab.SporelyCloudClient, "from_stored_credentials", lambda: fake_client)
    monkeypatch.setattr(
        observations_tab.ImageDB,
        "get_images_for_observation",
        lambda observation_id: [
            {"id": 11, "filepath": "/tmp/one.jpg"},
            {"id": 12, "filepath": "/tmp/two.jpg"},
        ],
    )

    tab = observations_tab.ObservationsTab.__new__(observations_tab.ObservationsTab)
    tab._ai_suggestions_cache = {}

    observation = {
        "id": 42,
        "cloud_id": "cloud-123",
        "ai_state_json": None,
        "genus": "Cantharellus",
        "species": "cibarius",
        "common_name": "Kantarell",
        "ai_selected_service": "inat",
        "ai_selected_taxon_id": "12345",
        "ai_selected_scientific_name": "Cantharellus cibarius",
    }

    ai_state = tab._load_observation_ai_state(observation)
    assert ai_state is not None
    assert ai_state["predictions"][0][0]["taxon"]["scientificName"] == "Cantharellus cibarius"
    assert ai_state["predictions"][1][0]["taxon"]["vernacularName"] == "Kantarell"
    assert ai_state["predictions"][0][0]["species_url"] == "https://artsdatabanken.no/arter/takson/213194"
    assert ai_state["inat_predictions"][0][0]["taxon"]["preferred_common_name"] == "Chanterelle"
    assert ai_state["inat_selected"][0]["taxon"]["id"] == "12345"
    assert 0 not in ai_state["selected"]
    assert tab._ai_suggestions_cache[42] == ai_state
    dialog = observations_tab.ObservationDetailsDialog.__new__(observations_tab.ObservationDetailsDialog)
    link = dialog._ai_prediction_link(ai_state["predictions"][0][0], ai_state["predictions"][0][0].get("taxon") or {}, source="arts")
    assert link == "https://artsdatabanken.no/arter/takson/213194"


def test_cloud_observation_editor_hydrates_ai_state_from_cloud_rows(tmp_path, monkeypatch) -> None:
    db_path = _init_fresh_database(tmp_path, monkeypatch)
    _patch_connection_helpers(monkeypatch, db_path)

    cloud_rows = [
        {
            "id": "row-arts",
            "service": "artsorakel",
            "status": "success",
            "created_at": "2026-05-02T12:00:00Z",
            "top_species_url": "https://artsdatabanken.no/arter/takson/213194",
            "results": [
                {
                    "scientific_name": "Cantharellus cibarius",
                    "vernacular_name": "Kantarell",
                    "taxon_id": "NBIC:1001",
                    "probability": 0.96,
                    "species_url": "https://artsdatabanken.no/arter/takson/213194",
                },
            ],
            "top_scientific_name": "Cantharellus cibarius",
            "top_vernacular_name": "Kantarell",
            "top_taxon_id": "NBIC:1001",
            "top_probability": 0.96,
        },
        {
            "id": "row-inat",
            "service": "inat",
            "status": "success",
            "created_at": "2026-05-02T12:01:00Z",
            "results": [
                {
                    "scientificName": "Cantharellus cibarius",
                    "vernacularName": "Chanterelle",
                    "taxonId": "12345",
                    "probability": 0.89,
                },
            ],
            "top_scientific_name": "Cantharellus cibarius",
            "top_vernacular_name": "Chanterelle",
            "top_taxon_id": "12345",
            "top_probability": 0.89,
        },
    ]

    fake_client = SimpleNamespace(
        pull_observation_identifications=lambda cloud_id: cloud_rows if cloud_id == "cloud-456" else [],
    )
    monkeypatch.setattr(observations_tab.SporelyCloudClient, "from_stored_credentials", lambda: fake_client)

    tab = observations_tab.ObservationsTab.__new__(observations_tab.ObservationsTab)
    row_data = {
        "cloud_id": "cloud-456",
        "raw": {
            "id": "cloud-456",
            "genus": "Cantharellus",
            "species": "cibarius",
            "common_name": "Kantarell",
            "ai_selected_service": "inat",
            "ai_selected_taxon_id": "12345",
            "ai_selected_scientific_name": "Cantharellus cibarius",
        },
    }
    image_results = [
        SimpleNamespace(filepath="/tmp/cloud-one.jpg", image_id=21),
        SimpleNamespace(filepath="/tmp/cloud-two.jpg", image_id=22),
    ]

    ai_state = tab._load_cloud_observation_ai_state(row_data, image_results)
    assert ai_state is not None
    assert ai_state["predictions"][0][0]["taxon"]["scientificName"] == "Cantharellus cibarius"
    assert ai_state["predictions"][1][0]["taxon"]["vernacularName"] == "Kantarell"
    assert ai_state["inat_predictions"][0][0]["taxon"]["preferred_common_name"] == "Chanterelle"
    assert 0 not in ai_state["selected"]
    assert ai_state["inat_selected"][0]["taxon"]["id"] == "12345"


def test_cloud_identification_service_aliases_normalize_to_canonical_pair() -> None:
    assert cloud_sync._normalize_cloud_identification_service("arts") == "artsorakel"
    assert cloud_sync._normalize_cloud_identification_service("artsorakel") == "artsorakel"
    assert cloud_sync._normalize_cloud_identification_service("inat") == "inat"
    assert cloud_sync._normalize_cloud_identification_service("inaturalist") == "inat"

    arts_state = cloud_sync.build_cloud_ai_state_from_observation_identifications(
        {
            "ai_selected_service": "arts",
            "ai_selected_taxon_id": "NBIC:1001",
            "ai_selected_scientific_name": "Cantharellus cibarius",
            "common_name": "Kantarell",
        },
        [
            {
                "id": "row-arts",
                "service": "arts",
                "status": "success",
                "created_at": "2026-05-02T12:00:00Z",
                "results": [
                    {
                        "scientific_name": "Cantharellus cibarius",
                        "taxon_id": "NBIC:1001",
                        "probability": 0.96,
                    },
                ],
            },
        ],
        [{"id": 1, "filepath": "/tmp/one.jpg"}],
    )
    assert arts_state is not None
    assert arts_state["predictions"][0][0]["taxon"]["id"] == "NBIC:1001"
    assert arts_state["selected"][0]["taxon"]["id"] == "NBIC:1001"
    assert arts_state["inat_predictions"] == {}
    assert arts_state["inat_selected"] == {}

    inat_state = cloud_sync.build_cloud_ai_state_from_observation_identifications(
        {
            "ai_selected_service": "inaturalist",
            "ai_selected_taxon_id": "12345",
            "ai_selected_scientific_name": "Cantharellus cibarius",
            "common_name": "Chanterelle",
        },
        [
            {
                "id": "row-inat",
                "service": "inaturalist",
                "status": "success",
                "created_at": "2026-05-02T12:01:00Z",
                "results": [
                    {
                        "scientificName": "Cantharellus cibarius",
                        "taxonId": "12345",
                        "probability": 0.89,
                    },
                ],
            },
        ],
        [{"id": 1, "filepath": "/tmp/one.jpg"}],
    )
    assert inat_state is not None
    assert inat_state["inat_predictions"][0][0]["taxon"]["id"] == "12345"
    assert inat_state["inat_selected"][0]["taxon"]["id"] == "12345"
    assert inat_state["predictions"] == {}
    assert inat_state["selected"] == {}
