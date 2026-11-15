"""Publishing sends the photos checked under the image strip (owner,
2026-11-15).

One selection: the strip's checkbox decides what Sporely Cloud keeps and what
Artsobservasjoner / iNaturalist get. The older publish-only list
(``artsobs_publish_excluded_image_ids_<obs>``) is no longer read, and no
microscope photo is picked per objective by default.

Runs on the real (isolated) database; only the Artsobservasjoner web client
and its login are faked.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

from database.models import ImageDB, ObservationDB, SettingsDB
from ui.observations_tab import ObservationsTab
from utils import cloud_sync


def _observation_with_photos(tmp_path, name: str):
    obs_id = ObservationDB.create_observation(date="2026-11-07", genus="Inocybe", species="geophylla")
    paths = {}
    ids = {}
    for key, image_type in (("field", "field"), ("p3", "microscope"), ("p12", "microscope")):
        path = tmp_path / f"{name}-{key}.jpg"
        path.write_bytes(b"jpeg-" + key.encode())
        ids[key] = ImageDB.add_image(
            obs_id, str(path), image_type,
            objective_name="63X_1.32_PL_APO" if image_type == "microscope" else None,
            copy_to_folder=False,
        )
        paths[key] = str(path)
    return obs_id, ids, paths


def _check_only(obs_id: int, checked_ids: set[int], all_ids: set[int]) -> None:
    """What the strip's checkbox stores (``_on_gallery_publish_selection_changed``)."""
    cloud_sync._ensure_cloud_image_storage_intent_initialized(obs_id)
    cloud_sync._set_cloud_image_storage_excluded_image_ids(obs_id, all_ids - checked_ids)


def test_no_microscope_photo_is_published_by_default(tmp_path):
    obs_id, ids, paths = _observation_with_photos(tmp_path, "default")

    published = ObservationsTab._collect_artsobs_image_paths(SimpleNamespace(
        _shared_image_ids=ObservationsTab._shared_image_ids,
    ), obs_id)

    assert published == [paths["field"]]
    assert cloud_sync._cloud_image_storage_excluded_image_ids(obs_id) == {ids["p3"], ids["p12"]}


def test_publishing_sends_the_checked_photo_not_the_old_publish_list(tmp_path):
    """Observation 1056's case: P3 unchecked, P12 checked, while the old
    publish-only list still had P3 in and P12 out."""
    obs_id, ids, paths = _observation_with_photos(tmp_path, "obs1056")
    SettingsDB.set_setting(
        f"artsobs_publish_excluded_image_ids_{obs_id}", json.dumps([ids["p12"]])
    )
    _check_only(obs_id, {ids["field"], ids["p12"]}, set(ids.values()))

    published = ObservationsTab._collect_artsobs_image_paths(SimpleNamespace(
        _shared_image_ids=ObservationsTab._shared_image_ids,
    ), obs_id)

    assert published == [paths["field"], paths["p12"]]


def _fake_artsobs(monkeypatch, uploads: list):
    class _FakeAuth:
        def get_valid_cookies(self, target="web"):
            return {"cookie": "ok"}

    class _FakeClient:
        def set_cookies_from_browser(self, cookies):
            pass

        def upload_images_web(self, sighting_id, image_paths, progress_cb=None):
            uploads.append((int(sighting_id), list(image_paths)))

    monkeypatch.setattr("utils.artsobservasjoner_auto_login.ArtsObservasjonerAuth", _FakeAuth)
    monkeypatch.setattr("utils.artsobservasjoner_submit.ArtsObservasjonerWebClient", _FakeClient)


def test_photos_added_later_upload_only_when_checked(tmp_path, monkeypatch):
    obs_id, ids, paths = _observation_with_photos(tmp_path, "pending")
    ObservationDB.update_observation(obs_id, artsdata_id=4242)
    ImageDB.mark_observation_images_artsobs_web_pending(obs_id)
    _check_only(obs_id, {ids["field"], ids["p12"]}, set(ids.values()))
    uploads: list = []
    _fake_artsobs(monkeypatch, uploads)
    tab = SimpleNamespace(
        tr=lambda text: text,
        set_status_message=lambda *args, **kwargs: None,
        _shared_image_ids=ObservationsTab._shared_image_ids,
    )

    # The reminder counts only the checked photos.
    notice = ObservationsTab._pending_published_image_upload_notice_for_observation(tab, obs_id)
    assert notice is not None and notice[0].startswith("2 images")

    assert ObservationsTab._upload_pending_artsobs_web_images(tab) == "uploaded"

    sent = [path for sighting, batch in uploads if sighting == 4242 for path in batch]
    assert sorted(sent) == sorted([paths["field"], paths["p12"]])
    # The unchecked photo stays pending: it goes once it is checked.
    assert ImageDB.get_pending_artsobs_web_upload_image_ids_for_observation(obs_id) == [ids["p3"]]
    assert ObservationsTab._pending_published_image_upload_notice_for_observation(tab, obs_id) is None


def test_image_viewer_checkbox_shows_the_strip_selection(tmp_path):
    obs_id, ids, _paths = _observation_with_photos(tmp_path, "viewer")
    _check_only(obs_id, {ids["p12"]}, set(ids.values()))
    SettingsDB.set_setting(
        f"artsobs_publish_excluded_image_ids_{obs_id}", json.dumps([ids["p12"]])
    )
    shown: dict[int, bool] = {}

    for image_id in (ids["p3"], ids["p12"]):
        browser = SimpleNamespace(
            current_image_id=lambda image_id=image_id: image_id,
            set_publish_state=lambda checked, enabled, image_id=image_id: shown.__setitem__(image_id, checked),
        )
        tab = SimpleNamespace(
            image_browser=browser,
            selected_observation_id=obs_id,
            _cloud_image_storage_excluded_image_ids=ObservationsTab._cloud_image_storage_excluded_image_ids,
        )
        ObservationsTab._sync_image_browser_publish_state(tab)

    assert shown == {ids["p3"]: False, ids["p12"]: True}


def test_no_login_reminder_for_unchecked_pending_photos(tmp_path, monkeypatch):
    """Only an unchecked photo is pending: nothing will upload, so Refresh
    neither asks for a login nor counts it (even with no Artsobs cookies)."""
    obs_id, ids, _paths = _observation_with_photos(tmp_path, "unchecked-only")
    ObservationDB.update_observation(obs_id, artsdata_id=4343)
    ImageDB.mark_observation_images_artsobs_web_pending(obs_id)
    ImageDB.mark_images_artsobs_web_uploaded([ids["field"], ids["p12"]])
    _check_only(obs_id, {ids["field"], ids["p12"]}, set(ids.values()))
    other_pending = {
        int(row["observation_id"]) for row in ImageDB.get_pending_artsobs_web_uploads()
    } - {obs_id}
    for other in other_pending:  # rows left by other tests in the shared test DB
        ImageDB.mark_observation_images_artsobs_web_uploaded(other)

    class _NoCookies:
        def get_valid_cookies(self, target="web"):
            return None

    monkeypatch.setattr("utils.artsobservasjoner_auto_login.ArtsObservasjonerAuth", _NoCookies)
    messages: list[str] = []
    tab = SimpleNamespace(
        tr=lambda text: text,
        set_status_message=lambda message, *args, **kwargs: messages.append(str(message)),
        _shared_image_ids=ObservationsTab._shared_image_ids,
    )

    assert ObservationsTab._upload_pending_artsobs_web_images(tab) == "none"
    assert messages == []
    assert ImageDB.get_pending_artsobs_web_upload_image_ids_for_observation(obs_id) == [ids["p3"]]


def test_moved_photos_keep_their_checkbox_state(tmp_path):
    """Moving photos to another observation (the strip's Move) keeps each
    photo's checked/unchecked state; nothing turns into the default."""
    source_id, ids, _paths = _observation_with_photos(tmp_path, "move-src")
    target_id = ObservationDB.create_observation(date="2026-11-08", genus="Inocybe", species="sp")
    cloud_sync.set_image_cloud_selected(ids["p12"], True)   # checked
    cloud_sync.set_image_cloud_selected(ids["p3"], False)   # unchecked
    moving = [ids["p3"], ids["p12"]]
    tab = SimpleNamespace(
        _pending_gallery_move_image_ids=list(moving),
        _pending_gallery_move_source_observation_id=source_id,
        _clear_pending_gallery_move=lambda **kwargs: None,
        refresh_observations=lambda **kwargs: None,
        _find_table_row_for_observation=lambda obs_id: -1,
        set_status_message=lambda *args, **kwargs: None,
        tr=lambda text: text,
    )

    ObservationsTab._complete_pending_gallery_move(tab, target_id)

    assert {ImageDB.get_image(i)["observation_id"] for i in moving} == {target_id}
    assert ObservationsTab._shared_image_ids(target_id, moving) == {ids["p12"]}
    assert cloud_sync._cloud_image_storage_excluded_image_ids(target_id) == {ids["p3"]}
    assert set(moving) <= cloud_sync._cloud_image_storage_intent_initialized_ids(target_id)
    # Nothing is left behind at the source for photos it no longer has.
    assert not set(moving) & cloud_sync._cloud_image_storage_excluded_image_ids(source_id)
    assert not set(moving) & cloud_sync._cloud_image_storage_intent_initialized_ids(source_id)


def _move_tab(source_id: int, image_ids: list[int], messages: list | None = None):
    return SimpleNamespace(
        _pending_gallery_move_image_ids=list(image_ids),
        _pending_gallery_move_source_observation_id=source_id,
        _clear_pending_gallery_move=lambda **kwargs: None,
        refresh_observations=lambda **kwargs: None,
        _find_table_row_for_observation=lambda obs_id: -1,
        set_status_message=lambda message, level="info", **kwargs: (
            messages.append((str(message), level)) if messages is not None else None
        ),
        tr=lambda text: text,
    )


def test_a_stale_selection_never_checks_a_photo_already_at_the_target(tmp_path):
    source_id, ids, _paths = _observation_with_photos(tmp_path, "stale-src")
    target_id, target_ids, _ = _observation_with_photos(tmp_path, "stale-dst")
    cloud_sync.set_image_cloud_selected(ids["p12"], True)
    cloud_sync.set_image_cloud_selected(target_ids["p3"], False)  # unchecked at target

    # The selection still lists a photo that is already at the target.
    ObservationsTab._complete_pending_gallery_move(
        _move_tab(source_id, [ids["p12"], target_ids["p3"]]), target_id,
    )

    assert ObservationsTab._shared_image_ids(
        target_id, [ids["p12"], target_ids["p3"]]
    ) == {ids["p12"]}


def test_a_failed_selection_carry_is_reported_not_hidden(tmp_path, monkeypatch):
    source_id, ids, _paths = _observation_with_photos(tmp_path, "carry-fail")
    target_id = ObservationDB.create_observation(date="2026-11-09", genus="Inocybe", species="sp")

    def _locked(*args, **kwargs):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(cloud_sync, "carry_cloud_image_storage_intent", _locked)
    messages: list = []

    ObservationsTab._complete_pending_gallery_move(
        _move_tab(source_id, [ids["p12"]], messages), target_id,
    )

    assert ImageDB.get_image(ids["p12"])["observation_id"] == target_id
    assert messages[-1][1] == "warning"
    assert "Check them again" in messages[-1][0]
