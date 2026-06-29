from __future__ import annotations

import sqlite3
from datetime import datetime
from types import SimpleNamespace

import pytest

from database import models
from database.models import ImageDB
from ui import observations_tab
from utils.artsobservasjoner_submit import (
    ArtsObservasjonerWebClient,
    UploadImageActionEndpointMismatchError,
    WebImageUploadError,
)


def test_upload_single_web_image_action_keeps_file_open_during_post(tmp_path):
    client = ArtsObservasjonerWebClient()
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"abc")

    seen: dict[str, object] = {}

    def fake_post(url, data=None, files=None, headers=None, timeout=None):
        handle = files["UploadImageViewModel.Image"][1]
        seen["closed_during_post"] = handle.closed
        seen["read_during_post"] = handle.read(1)
        return SimpleNamespace(ok=True, status_code=200, reason="OK", text="ok", headers={})

    client.session.post = fake_post  # type: ignore[method-assign]

    result = client._upload_single_web_image_action(
        sighting_id=123,
        image_path=str(image_path),
        token="token",
    )

    assert seen["closed_during_post"] is False
    assert seen["read_during_post"] == b"a"
    assert result["field_name"] == "UploadImageViewModel.Image"


def test_upload_images_web_does_not_fallback_for_local_file_handle_error(tmp_path, monkeypatch):
    client = ArtsObservasjonerWebClient()
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"abc")

    monkeypatch.setattr(client, "_load_editable_images_for_sighting", lambda sighting_id: "")
    monkeypatch.setattr(client, "_extract_request_verification_token", lambda html: "token")
    monkeypatch.setattr(
        client,
        "_upload_single_web_image_action",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("read of closed file")),
    )

    fallback_called = False

    def _unexpected_fallback(*args, **kwargs):
        nonlocal fallback_called
        fallback_called = True
        raise AssertionError("fallback should not run for local file-handle bugs")

    monkeypatch.setattr(client, "_upload_single_web_image", _unexpected_fallback)

    with pytest.raises(WebImageUploadError) as excinfo:
        client.upload_images_web(
            sighting_id=123,
            image_paths=[str(image_path)],
            allow_bruteforce_fallback=True,
        )

    assert fallback_called is False
    assert "read of closed file" in str(excinfo.value)


def test_upload_images_web_can_fallback_for_endpoint_mismatch(tmp_path, monkeypatch):
    client = ArtsObservasjonerWebClient()
    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"abc")

    monkeypatch.setattr(client, "_load_editable_images_for_sighting", lambda sighting_id: "")
    monkeypatch.setattr(client, "_extract_request_verification_token", lambda html: "token")
    monkeypatch.setattr(
        client,
        "_upload_single_web_image_action",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            UploadImageActionEndpointMismatchError(
                "https://www.artsobservasjoner.no/Media/UploadImageAction",
                404,
                "HTML error page (title: 404)",
            )
        ),
    )

    fallback_calls: list[tuple[int, str]] = []

    def _fallback(*, sighting_id, image_path, upload_targets, token):
        fallback_calls.append((sighting_id, image_path))
        return {"filename": "image.jpg", "url": "fallback", "status_code": 200, "field_name": "file"}

    monkeypatch.setattr(client, "_upload_single_web_image", _fallback)

    result = client.upload_images_web(
        sighting_id=123,
        image_paths=[str(image_path)],
        allow_bruteforce_fallback=True,
    )

    assert fallback_calls == [(123, str(image_path))]
    assert result == [{"filename": "image.jpg", "url": "fallback", "status_code": 200, "field_name": "file"}]


def test_submit_observation_web_returns_sighting_id_when_image_upload_fails(monkeypatch):
    client = ArtsObservasjonerWebClient()

    monkeypatch.setattr(
        client,
        "_load_report_form_html",
        lambda: '<input name="__RequestVerificationToken" value="token">',
    )
    monkeypatch.setattr(client, "_validate_start_datetime", lambda *args, **kwargs: None)
    monkeypatch.setattr(client, "_validate_taxon", lambda *args, **kwargs: None)
    monkeypatch.setattr(client, "_extract_sighting_id", lambda text: 987)
    monkeypatch.setattr(client, "_extract_temporary_sighting_id", lambda text: None)
    monkeypatch.setattr(client, "_recover_sighting_id_from_grid", lambda: None)
    monkeypatch.setattr(client, "_recover_saved_sighting_id", lambda previous_ids=None: None)
    monkeypatch.setattr(client, "_resolve_site", lambda: (1, "Test site"))
    monkeypatch.setattr(client, "_resolve_site_from_cookies", lambda: (1, "Test site"))
    monkeypatch.setattr(
        client,
        "upload_images_web",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            WebImageUploadError(
                "One or more web image uploads failed.",
                uploaded_images=[{"filename": "image.jpg"}],
                failures=["image.jpg: UploadImageAction(read of closed file)"],
            )
        ),
    )

    response = SimpleNamespace(ok=True, status_code=200, reason="OK", text="saved", headers={})
    client.session.post = lambda *args, **kwargs: response  # type: ignore[method-assign]

    result = client.submit_observation_web(
        taxon_id=223130,
        observed_datetime=datetime(2024, 1, 1, 12, 0, 0),
        site_id=1,
        image_paths=["image.jpg"],
    )

    assert result["sighting_id"] == 987
    assert result["uploaded_images"] == [{"filename": "image.jpg"}]
    assert result["image_upload_error"] == "One or more web image uploads failed."
    assert result["image_upload_failures"] == ["image.jpg: UploadImageAction(read of closed file)"]


def test_mark_observation_images_artsobs_web_pending_restores_retry_queue(tmp_path, monkeypatch):
    db_path = tmp_path / "artsobs.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE observations (
                id INTEGER PRIMARY KEY,
                artsdata_id INTEGER
            );
            CREATE TABLE images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observation_id INTEGER,
                filepath TEXT,
                original_filepath TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                sort_order INTEGER,
                artsobs_web_unpublished INTEGER DEFAULT 0
            );
            INSERT INTO observations (id, artsdata_id) VALUES (1, 321);
            INSERT INTO images (
                observation_id, filepath, original_filepath, sort_order, artsobs_web_unpublished
            ) VALUES (1, '/tmp/image.jpg', NULL, 0, 0);
            """
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(models, "get_connection", lambda: sqlite3.connect(db_path))

    assert ImageDB.get_pending_artsobs_web_uploads() == []

    ImageDB.mark_observation_images_artsobs_web_pending(1)

    pending_rows = ImageDB.get_pending_artsobs_web_uploads()
    assert len(pending_rows) == 1
    assert pending_rows[0]["image_id"] == 1
    assert pending_rows[0]["observation_id"] == 1
    assert pending_rows[0]["artsdata_id"] == 321


def test_upload_observation_to_artsobs_marks_pending_images_and_persists_artsdata_id_on_web_image_failure(
    monkeypatch,
):
    status_messages: list[tuple[str, str, int]] = []
    update_calls: list[tuple[int, dict]] = []
    pending_calls: list[int] = []
    uploaded_calls: list[int] = []

    image_path = "/tmp/pending-image.jpg"
    observation = {
        "id": 7,
        "genus": "Atheniella",
        "species": "flavoalba",
        "common_name": "mushroom",
        "gps_latitude": 60.0,
        "gps_longitude": 10.0,
        "date": "2024-01-01 12:00:00",
        "publish_target": "web",
        "location": "Test site",
        "habitat": "",
        "notes": "",
        "open_comment": "",
        "private_comment": "",
        "interesting_comment": 0,
        "uncertain": 0,
        "unspontaneous": 0,
        "determination_method": None,
    }

    class _FakeUploader:
        key = "web"
        label = "Artsobservasjoner"

        def upload(self, observation_payload, image_paths, cookies, progress_cb=None):
            return SimpleNamespace(
                sighting_id=444,
                raw={
                    "sighting_id": 444,
                    "image_upload_error": "One or more web image uploads failed.",
                },
            )

    fake_tab = SimpleNamespace(
        tr=lambda text: text,
        set_status_message=lambda message, level="info", auto_clear_ms=8000: status_messages.append(
            (str(message), str(level), int(auto_clear_ms))
        ),
        SETTING_INCLUDE_SPORE_STATS="include_spore_stats",
        SETTING_INCLUDE_ANNOTATIONS="include_annotations",
        SETTING_INCLUDE_MEASURE_PLOTS="include_measure_plots",
        SETTING_INCLUDE_THUMBNAIL_GALLERY="include_thumbnail_gallery",
        SETTING_INCLUDE_PLATE="include_plate",
        SETTING_INCLUDE_COPYRIGHT="include_copyright",
        _set_status_progress_visible=lambda visible: None,
        _set_status_progress=lambda text, current=0, total=1: None,
        _cleanup_publish_temp_dir=lambda temp_dir: None,
        schedule_metadata_cloud_sync=lambda observation_id: None,
        _artsobs_dead_by_observation_id={},
        _artsobs_public_published_by_observation_id={},
        _observation_publish_target=lambda obs: "web",
        _uploader_matches_publish_target=lambda uploader_key, publish_target: True,
        _observation_has_existing_upload=lambda obs, uploader_key: False,
        _publish_option_enabled=lambda setting, default=False: False,
        _publish_measurement_availability=lambda observation_id, image_paths: {
            "has_overlay_measurements": False,
            "spore_stats": False,
            "has_plot_measurements": False,
            "has_gallery_measurements": False,
        },
        _publish_image_license_code=lambda: "10",
        _publish_copyright_text=lambda obs: None,
        _preferred_publish_uploader_key=lambda obs, uploader_key: "web",
        _collect_artsobs_image_paths=lambda observation_id: [image_path],
        _prepare_publish_media_assets=lambda **kwargs: ([image_path], None, []),
        _publish_spore_stats_text=lambda observation_id, obs, spore_stats=None: None,
        _resolve_artsobs_taxon_resolution=lambda obs: SimpleNamespace(
            taxon_id=223130,
            source_field="resolved_taxonomy.artsdatabanken",
        ),
        _render_publish_cell=lambda *args, **kwargs: None,
        _update_publish_controls=lambda: None,
        refresh_observations=lambda show_status=False: None,
    )

    monkeypatch.setattr(observations_tab, "QApplication", SimpleNamespace(processEvents=lambda *args, **kwargs: None))
    monkeypatch.setattr(observations_tab, "log_artsobservasjoner_taxon_diagnostic", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        observations_tab,
        "ObservationDB",
        SimpleNamespace(
            get_observation=lambda observation_id: dict(observation),
            update_observation=lambda observation_id, **kwargs: update_calls.append((observation_id, dict(kwargs))),
            resolve_adb_taxon_id=lambda *args, **kwargs: None,
            set_artportalen_id=lambda *args, **kwargs: None,
            set_inaturalist_id=lambda *args, **kwargs: None,
            set_mushroomobserver_id=lambda *args, **kwargs: None,
        ),
    )
    monkeypatch.setattr(
        observations_tab,
        "ImageDB",
        SimpleNamespace(
            mark_observation_images_artsobs_web_pending=lambda observation_id: pending_calls.append(observation_id),
            mark_observation_images_artsobs_web_uploaded=lambda observation_id: uploaded_calls.append(observation_id),
        ),
    )
    monkeypatch.setattr("utils.artsobs_uploaders.get_uploader", lambda target_key: _FakeUploader())
    monkeypatch.setattr(
        "utils.artsobservasjoner_auto_login.ArtsObservasjonerAuth",
        lambda: SimpleNamespace(ensure_valid_cookies=lambda target="web": {"cookie": "ok"}),
    )

    ok, published_id, error = observations_tab.ObservationsTab.upload_observation_to_artsobs(
        fake_tab,
        observation_id=7,
        uploader_key="web",
        show_status=True,
        refresh_table=True,
    )

    assert ok is True
    assert published_id == 444
    assert error is None
    assert update_calls == [(7, {"artsdata_id": 444})]
    assert pending_calls == [7]
    assert uploaded_calls == []
    assert status_messages
    assert status_messages[-1][1] == "warning"
    assert "Observation published, but image upload failed. Images remain pending." in status_messages[-1][0]
    assert "One or more web image uploads failed." in status_messages[-1][0]
