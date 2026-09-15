"""Stage 1 regression tests: a stored iNaturalist id must be verified, not trusted.

Sporely used to treat any stored ``inaturalist_id`` as proof that the remote
observation still existed, which permanently blocked republishing after the user
deleted the observation on iNaturalist. These tests pin the replacement rule:
only a confirmed-missing remote observation unlocks a republish, and the stored
id is replaced only after the new publish has succeeded.

All iNaturalist traffic is faked; nothing here touches the network.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from ui import observations_tab
from utils.artsobs_uploaders import INaturalistUploader


# --------------------------------------------------------------------------
# INaturalistUploader.check_observation_link
# --------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, status_code: int, payload=None, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


def _patch_get(monkeypatch, handler):
    calls: list[dict] = []

    def fake_get(url, headers=None, timeout=None):
        calls.append({"url": url, "headers": dict(headers or {}), "timeout": timeout})
        return handler()

    monkeypatch.setattr("utils.artsobs_uploaders.requests.get", fake_get)
    return calls


def test_check_observation_link_reports_live_for_matching_result(monkeypatch):
    calls = _patch_get(
        monkeypatch,
        lambda: _FakeResponse(200, {"total_results": 1, "results": [{"id": 4242}]}),
    )

    status = INaturalistUploader().check_observation_link(4242, {"access_token": "tok"})

    assert status.is_live is True
    assert status.is_missing is False
    assert calls[0]["url"] == "https://api.inaturalist.org/v1/observations/4242"
    assert calls[0]["headers"]["Authorization"] == "Bearer tok"


def test_check_observation_link_treats_empty_results_as_missing(monkeypatch):
    # The v1 API answers a deleted observation with 200 and an empty result set.
    _patch_get(
        monkeypatch,
        lambda: _FakeResponse(200, {"total_results": 0, "results": []}),
    )

    status = INaturalistUploader().check_observation_link(4242, {"access_token": "tok"})

    assert status.is_missing is True


@pytest.mark.parametrize("status_code", [404, 410])
def test_check_observation_link_treats_404_and_410_as_missing(monkeypatch, status_code):
    _patch_get(monkeypatch, lambda: _FakeResponse(status_code, None, "gone"))

    status = INaturalistUploader().check_observation_link(4242, {"access_token": "tok"})

    assert status.is_missing is True
    assert status.status_code == status_code


@pytest.mark.parametrize("status_code", [401, 403, 429, 500, 502, 503])
def test_check_observation_link_never_reports_missing_for_ambiguous_status(monkeypatch, status_code):
    _patch_get(monkeypatch, lambda: _FakeResponse(status_code, {"error": "nope"}))

    status = INaturalistUploader().check_observation_link(4242, {"access_token": "tok"})

    assert status.is_unverified is True
    assert status.is_missing is False


def test_check_observation_link_reports_unverified_on_network_failure(monkeypatch):
    def _raise():
        raise OSError("connection timed out")

    _patch_get(monkeypatch, _raise)

    status = INaturalistUploader().check_observation_link(4242, {"access_token": "tok"})

    assert status.is_unverified is True
    assert "connection timed out" in status.detail


def test_check_observation_link_reports_unverified_on_unreadable_payload(monkeypatch):
    _patch_get(monkeypatch, lambda: _FakeResponse(200, None, "<html>"))

    status = INaturalistUploader().check_observation_link(4242, {"access_token": "tok"})

    assert status.is_unverified is True


# --------------------------------------------------------------------------
# upload_observation_to_artsobs, iNaturalist branch
# --------------------------------------------------------------------------


_OBSERVATION_BASE = {
    "id": 7,
    "genus": "Atheniella",
    "species": "flavoalba",
    "common_name": "mushroom",
    "gps_latitude": 60.0,
    "gps_longitude": 10.0,
    "date": "2024-01-01 12:00:00",
    "publish_target": "inat",
    "location": "Test site",
}


class _RecordingUploader:
    key = "inat"
    label = "iNaturalist"

    def __init__(
        self,
        link_status,
        sighting_id: int | None = 999,
        error: Exception | None = None,
        image_upload_error: str | None = None,
    ):
        self._link_status = link_status
        self._sighting_id = sighting_id
        self._error = error
        self._image_upload_error = image_upload_error
        self.link_checks: list[int] = []
        self.uploads: list[list[str]] = []

    def check_observation_link(self, observation_id, cookies=None, timeout=None):
        self.link_checks.append(int(observation_id))
        return self._link_status

    def upload(self, observation_payload, image_paths, cookies, progress_cb=None):
        self.uploads.append(list(image_paths or []))
        if self._error is not None:
            raise self._error
        raw: dict = {"observation": {"id": self._sighting_id}}
        if self._image_upload_error:
            raw["image_upload_error"] = self._image_upload_error
        return SimpleNamespace(sighting_id=self._sighting_id, raw=raw)


class _FakeOAuthClient:
    DEFAULT_CLIENT_ID = "client-id"
    DEFAULT_REDIRECT_URI = "http://127.0.0.1/callback"

    def __init__(self, **kwargs):
        pass

    def get_valid_access_token(self) -> str:
        return "tok"


def _link_status(state: str, detail: str = ""):
    return SimpleNamespace(
        state=state,
        detail=detail,
        status_code=None,
        is_live=state == "live",
        is_missing=state == "missing",
        is_unverified=state == "unverified",
    )


def _build_env(monkeypatch, uploader, observation, confirm):
    """Wire up a minimal fake ObservationsTab around the real publish method."""
    recorded: dict[str, object] = {
        "status_messages": [],
        "set_inat_calls": [],
        "prompts": [],
        "rendered": [],
    }

    fake_tab = SimpleNamespace(
        tr=lambda text: text,
        set_status_message=lambda message, level="info", auto_clear_ms=8000: recorded[
            "status_messages"
        ].append((str(message), str(level))),
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
        _observation_publish_target=lambda obs: "inat",
        _uploader_matches_publish_target=lambda uploader_key, publish_target: True,
        _publish_option_enabled=lambda setting, default=False: False,
        _publish_measurement_availability=lambda observation_id, image_paths: {
            "has_overlay_measurements": False,
            "spore_stats": False,
            "has_plot_measurements": False,
            "has_gallery_measurements": False,
        },
        _publish_image_license_code=lambda: "10",
        _publish_copyright_text=lambda obs: None,
        _preferred_publish_uploader_key=lambda obs, uploader_key: "inat",
        _collect_artsobs_image_paths=lambda observation_id: ["/tmp/spore-plot.jpg"],
        _prepare_publish_media_assets=lambda **kwargs: (["/tmp/spore-plot.jpg"], None, []),
        _publish_spore_stats_text=lambda observation_id, obs, spore_stats=None: None,
        _resolve_inaturalist_taxon_id=lambda obs: None,
        _render_publish_cell=lambda row, obs: recorded["rendered"].append(dict(obs)),
        _update_publish_controls=lambda: None,
        _find_table_row_for_observation=lambda observation_id: -1,
        refresh_observations=lambda show_status=False: None,
    )
    # Exercise the real gating and verification logic, not stubs.
    fake_tab._observation_has_existing_upload = (
        lambda obs, uploader_key: observations_tab.ObservationsTab._observation_has_existing_upload(
            fake_tab, obs, uploader_key
        )
    )
    fake_tab._existing_upload_blocks_publish = (
        lambda uploader_key: observations_tab.ObservationsTab._existing_upload_blocks_publish(
            fake_tab, uploader_key
        )
    )
    fake_tab._resolve_inaturalist_link_before_publish = (
        lambda observation_id, obs, up, cookies: (
            observations_tab.ObservationsTab._resolve_inaturalist_link_before_publish(
                fake_tab, observation_id, obs, up, cookies
            )
        )
    )

    monkeypatch.setattr(
        observations_tab,
        "QApplication",
        SimpleNamespace(processEvents=lambda *args, **kwargs: None),
    )

    def _ask(parent, title, message, default_yes=False, **kwargs):
        recorded["prompts"].append(str(message))
        return confirm

    monkeypatch.setattr(observations_tab, "ask_wrapped_yes_no", _ask)
    monkeypatch.setattr(
        observations_tab,
        "ObservationDB",
        SimpleNamespace(
            get_observation=lambda observation_id: dict(observation),
            update_observation=lambda observation_id, **kwargs: None,
            resolve_adb_taxon_id=lambda *args, **kwargs: None,
            set_artportalen_id=lambda *args, **kwargs: None,
            set_inaturalist_id=lambda observation_id, value: recorded["set_inat_calls"].append(
                (observation_id, value)
            ),
            set_mushroomobserver_id=lambda *args, **kwargs: None,
        ),
    )
    monkeypatch.setattr("utils.artsobs_uploaders.get_uploader", lambda target_key: uploader)
    monkeypatch.setattr("utils.inat_oauth.INatOAuthClient", _FakeOAuthClient)
    monkeypatch.setattr(
        observations_tab.SettingsDB,
        "get_setting",
        staticmethod(lambda key, default=None: "client-id" if key == "inat_client_id" else default),
    )
    return fake_tab, recorded


def _publish(fake_tab):
    return observations_tab.ObservationsTab.upload_observation_to_artsobs(
        fake_tab,
        observation_id=7,
        uploader_key="inat",
        show_status=True,
        refresh_table=False,
    )


def test_publish_without_existing_id_takes_the_normal_create_path(monkeypatch):
    uploader = _RecordingUploader(_link_status("live"), sighting_id=555)
    fake_tab, recorded = _build_env(
        monkeypatch, uploader, dict(_OBSERVATION_BASE, inaturalist_id=None), confirm=False
    )

    ok, published_id, error = _publish(fake_tab)

    assert (ok, published_id, error) == (True, 555, None)
    assert uploader.link_checks == []  # nothing to verify
    assert uploader.uploads == [["/tmp/spore-plot.jpg"]]
    assert recorded["set_inat_calls"] == [(7, 555)]
    assert recorded["prompts"] == []


def test_publish_with_live_remote_observation_creates_no_duplicate(monkeypatch):
    uploader = _RecordingUploader(_link_status("live"))
    fake_tab, recorded = _build_env(
        monkeypatch, uploader, dict(_OBSERVATION_BASE, inaturalist_id=4242), confirm=True
    )

    ok, published_id, error = _publish(fake_tab)

    assert ok is False
    assert published_id is None
    assert "already has an ID" in (error or "")
    assert uploader.link_checks == [4242]
    assert uploader.uploads == []
    assert recorded["set_inat_calls"] == []
    assert recorded["prompts"] == []


@pytest.mark.parametrize("missing_reason", ["404", "410"])
def test_stale_link_offers_republish_and_replaces_id_on_success(monkeypatch, missing_reason):
    uploader = _RecordingUploader(_link_status("missing", missing_reason), sighting_id=777)
    fake_tab, recorded = _build_env(
        monkeypatch, uploader, dict(_OBSERVATION_BASE, inaturalist_id=4242), confirm=True
    )

    ok, published_id, error = _publish(fake_tab)

    assert (ok, published_id, error) == (True, 777, None)
    assert uploader.link_checks == [4242]
    assert uploader.uploads == [["/tmp/spore-plot.jpg"]]
    assert recorded["set_inat_calls"] == [(7, 777)]
    assert recorded["prompts"]
    assert "no longer exists" in recorded["prompts"][0]


def test_stale_link_keeps_old_id_when_the_replacement_publish_fails(monkeypatch):
    uploader = _RecordingUploader(
        _link_status("missing"), error=RuntimeError("iNaturalist create observation failed (422)")
    )
    fake_tab, recorded = _build_env(
        monkeypatch, uploader, dict(_OBSERVATION_BASE, inaturalist_id=4242), confirm=True
    )

    ok, published_id, error = _publish(fake_tab)

    assert ok is False
    assert published_id is None
    assert "422" in (error or "")
    assert uploader.uploads == [["/tmp/spore-plot.jpg"]]
    # The old link must survive a failed replacement.
    assert recorded["set_inat_calls"] == []


@pytest.mark.parametrize("detail", ["401 Unauthorized", "connection timed out", "HTTP 503"])
def test_unverifiable_link_keeps_old_id_and_attempts_no_replacement(monkeypatch, detail):
    uploader = _RecordingUploader(_link_status("unverified", detail))
    fake_tab, recorded = _build_env(
        monkeypatch, uploader, dict(_OBSERVATION_BASE, inaturalist_id=4242), confirm=True
    )

    ok, published_id, error = _publish(fake_tab)

    assert ok is False
    assert published_id is None
    assert "could not check" in (error or "")
    assert detail in (error or "")
    assert uploader.uploads == []
    assert recorded["set_inat_calls"] == []
    assert recorded["prompts"] == []


def test_declining_the_republish_mutates_nothing_and_creates_nothing(monkeypatch):
    uploader = _RecordingUploader(_link_status("missing"))
    fake_tab, recorded = _build_env(
        monkeypatch, uploader, dict(_OBSERVATION_BASE, inaturalist_id=4242), confirm=False
    )

    ok, published_id, error = _publish(fake_tab)

    assert (ok, published_id, error) == (False, None, None)
    assert uploader.uploads == []
    assert recorded["set_inat_calls"] == []
    assert recorded["status_messages"] == []
    assert recorded["prompts"]


# --------------------------------------------------------------------------
# Publish-action gating
# --------------------------------------------------------------------------


def test_stored_inaturalist_id_no_longer_blocks_the_publish_action():
    tab = SimpleNamespace()
    blocks = observations_tab.ObservationsTab._existing_upload_blocks_publish

    assert blocks(tab, "inat") is False
    assert blocks(tab, "web") is True
    assert blocks(tab, "artportalen") is True
    assert blocks(tab, "mo") is True


def test_stale_link_keeps_new_id_when_only_the_image_upload_fails(monkeypatch):
    """Replacement observation created, media failed: the new id must be stored.

    Forgetting the new id here would leave a live orphan on iNaturalist and make
    the next retry create yet another replacement observation.
    """
    uploader = _RecordingUploader(
        _link_status("missing"),
        sighting_id=777,
        image_upload_error="iNaturalist image upload failed (422): Photo is invalid",
    )
    fake_tab, recorded = _build_env(
        monkeypatch, uploader, dict(_OBSERVATION_BASE, inaturalist_id=4242), confirm=True
    )

    ok, published_id, error = _publish(fake_tab)

    assert ok is True
    assert published_id == 777
    # The stale 4242 is replaced by the observation that really exists now.
    assert recorded["set_inat_calls"] == [(7, 777)]
    # The media failure is reported, not swallowed as a clean success.
    assert error is not None
    assert "image upload failed" in error
    assert "Photo is invalid" in error
    assert recorded["status_messages"]
    assert recorded["status_messages"][-1][1] == "warning"


def test_first_time_publish_keeps_new_id_when_only_the_image_upload_fails(monkeypatch):
    uploader = _RecordingUploader(
        _link_status("live"),
        sighting_id=555,
        image_upload_error="iNaturalist image upload failed (500): Internal Server Error",
    )
    fake_tab, recorded = _build_env(
        monkeypatch, uploader, dict(_OBSERVATION_BASE, inaturalist_id=None), confirm=False
    )

    ok, published_id, error = _publish(fake_tab)

    assert ok is True
    assert published_id == 555
    assert recorded["set_inat_calls"] == [(7, 555)]
    assert error is not None and "image upload failed" in error
    assert recorded["status_messages"][-1][1] == "warning"


def test_inaturalist_upload_returns_partial_result_instead_of_raising(monkeypatch, tmp_path):
    """The uploader itself must not throw away a created observation id."""
    image_path = tmp_path / "spore-plot.jpg"
    image_path.write_bytes(b"jpeg")
    posts: list[str] = []

    def fake_post(url, headers=None, json=None, data=None, files=None, timeout=None):
        posts.append(url)
        if url.endswith("/observations"):
            return _FakeResponse(200, {"results": [{"id": 4321}]})
        return _FakeResponse(422, {"errors": ["Photo is invalid"]})

    monkeypatch.setattr("utils.artsobs_uploaders.requests.post", fake_post)

    result = INaturalistUploader().upload(
        {"species_guess": "Atheniella flavoalba", "observed_datetime": "2024-01-01 12:00:00"},
        [str(image_path)],
        {"access_token": "tok"},
    )

    assert result.sighting_id == 4321
    assert result.raw["images_uploaded"] == 0
    assert "Photo is invalid" in result.raw["image_upload_error"]
    assert posts == [
        "https://api.inaturalist.org/v1/observations",
        "https://api.inaturalist.org/v1/observation_photos",
    ]


def test_inaturalist_upload_still_raises_when_the_observation_is_not_created(monkeypatch):
    """Create failure must raise so the caller keeps the old local id."""
    monkeypatch.setattr(
        "utils.artsobs_uploaders.requests.post",
        lambda *args, **kwargs: _FakeResponse(422, {"errors": ["Taxon is invalid"]}),
    )

    with pytest.raises(RuntimeError, match="create observation failed"):
        INaturalistUploader().upload(
            {"species_guess": "Atheniella flavoalba"}, [], {"access_token": "tok"}
        )


# --------------------------------------------------------------------------
# "Both" stays blocked by a stored iNaturalist id
# --------------------------------------------------------------------------


def _both_tab(inat_has_existing_upload: bool, calls: list, messages: list):
    return SimpleNamespace(
        tr=lambda text: text,
        _selected_observation_ids=lambda: [7],
        _publish_target_login_status=lambda force_refresh=False: {"web": True, "inat": True},
        _publish_target_saved_login_status=lambda force_refresh=False: {"web": True, "inat": True},
        _open_online_publishing_settings=lambda: False,
        _invalidate_publish_login_status_cache=lambda: None,
        _update_publish_controls=lambda: None,
        _publish_actions={},
        _selection_has_existing_upload_for_uploader=lambda key: (
            inat_has_existing_upload if key == "inat" else False
        ),
        _selection_blocks_publish_for_uploader=lambda key: (
            observations_tab.ObservationsTab._selection_blocks_publish_for_uploader(
                SimpleNamespace(
                    _existing_upload_blocks_publish=lambda k: (
                        observations_tab.ObservationsTab._existing_upload_blocks_publish(None, k)
                    ),
                    _selection_has_existing_upload_for_uploader=lambda k: (
                        inat_has_existing_upload if k == "inat" else False
                    ),
                ),
                key,
            )
        ),
        _selection_matches_uploader_target=lambda key: True,
        _ensure_selection_publish_target=lambda uploader_key, observation_ids: True,
        refresh_observations=lambda *args, **kwargs: None,
        set_status_message=lambda message, level="info", auto_clear_ms=8000: messages.append(
            (str(message), str(level))
        ),
        upload_observation_to_artsobs=lambda observation_id, uploader_key, show_status, refresh_table, publish_bundle=None: (
            calls.append(uploader_key) or (True, 456, None)
        ),
    )


def test_stored_inaturalist_id_still_blocks_the_combined_both_action():
    """Both publishes web first, so it must not start when the iNat half may refuse."""
    calls: list[str] = []
    messages: list[tuple[str, str]] = []

    observations_tab.ObservationsTab._publish_selected_observations_both(
        _both_tab(inat_has_existing_upload=True, calls=calls, messages=messages)
    )

    # Critically, no Artsobservasjoner publish happened before the iNat refusal.
    assert calls == []
    assert messages
    assert "already uploaded to this service" in messages[-1][0]
    assert messages[-1][1] == "warning"


def test_stored_inaturalist_id_does_not_block_the_individual_inaturalist_action():
    calls: list[str] = []
    messages: list[tuple[str, str]] = []

    observations_tab.ObservationsTab._publish_selected_observations(
        _both_tab(inat_has_existing_upload=True, calls=calls, messages=messages), "inat"
    )

    # The stale-link repair path is reached for the individual action.
    assert calls == ["inat"]


def test_both_still_runs_when_no_inaturalist_id_is_stored():
    calls: list[str] = []
    messages: list[tuple[str, str]] = []

    observations_tab.ObservationsTab._publish_selected_observations_both(
        _both_tab(inat_has_existing_upload=False, calls=calls, messages=messages)
    )

    assert calls == ["web", "inat"]


# --------------------------------------------------------------------------
# Batch summary semantics in _publish_selected_observations
# --------------------------------------------------------------------------


def _batch_tab(results: dict[int, tuple], observation_ids: list[int], messages: list):
    """Fake tab driving the real batch wrapper with canned per-observation results."""
    return SimpleNamespace(
        tr=lambda text: text,
        _selected_observation_ids=lambda: list(observation_ids),
        _publish_target_login_status=lambda force_refresh=False: {"inat": True},
        _publish_target_saved_login_status=lambda force_refresh=False: {"inat": True},
        _open_online_publishing_settings=lambda: False,
        _invalidate_publish_login_status_cache=lambda: None,
        _update_publish_controls=lambda: None,
        _publish_actions={},
        _selection_has_existing_upload_for_uploader=lambda key: False,
        _selection_blocks_publish_for_uploader=lambda key: False,
        _selection_matches_uploader_target=lambda key: True,
        _ensure_selection_publish_target=lambda uploader_key, observation_ids: True,
        refresh_observations=lambda *args, **kwargs: None,
        set_status_message=lambda message, level="info", auto_clear_ms=8000: messages.append(
            (str(message), str(level))
        ),
        upload_observation_to_artsobs=lambda observation_id, uploader_key, show_status, refresh_table, publish_bundle=None: (
            results[observation_id]
        ),
    )


def _final(messages: list) -> tuple[str, str]:
    return messages[-1]


_DECLINED = (False, None, None)
_HARD_FAILURE = (False, None, "Upload failed: iNaturalist create observation failed (422).")
_PARTIAL = (True, 777, "Published to iNaturalist (ID 777), but image upload failed.")
_CLEAN = (True, 555, None)


def test_batch_declined_stale_link_is_not_reported_as_a_failure():
    messages: list[tuple[str, str]] = []
    observations_tab.ObservationsTab._publish_selected_observations(
        _batch_tab({7: _DECLINED}, [7], messages), "inat"
    )

    text, level = _final(messages)
    assert level != "error"
    assert "failed" not in text.lower()
    assert "cancelled" in text.lower()


def test_declined_stale_link_is_not_a_failure_through_the_real_publish_path(monkeypatch):
    """End-to-end: the real uploader decision reaches the real batch summary.

    The canned-tuple tests above assume a decline yields ``(False, None, None)``;
    this one wires the actual publish method into the actual batch wrapper so the
    two halves cannot drift apart.
    """
    uploader = _RecordingUploader(_link_status("missing"))
    fake_tab, recorded = _build_env(
        monkeypatch, uploader, dict(_OBSERVATION_BASE, inaturalist_id=4242), confirm=False
    )
    messages: list[tuple[str, str]] = []
    batch_tab = _batch_tab({}, [7], messages)
    batch_tab.upload_observation_to_artsobs = (
        lambda observation_id, uploader_key, show_status, refresh_table, publish_bundle=None: (
            observations_tab.ObservationsTab.upload_observation_to_artsobs(
                fake_tab,
                observation_id=observation_id,
                uploader_key=uploader_key,
                show_status=show_status,
                refresh_table=refresh_table,
            )
        )
    )

    observations_tab.ObservationsTab._publish_selected_observations(batch_tab, "inat")

    text, level = _final(messages)
    assert level != "error"
    assert "failed" not in text.lower()
    assert "cancelled" in text.lower()
    assert recorded["prompts"]  # the user really was asked
    assert recorded["set_inat_calls"] == []  # no local mutation
    assert uploader.uploads == []  # no remote creation


def test_batch_decline_alongside_a_success_still_reports_success():
    messages: list[tuple[str, str]] = []
    observations_tab.ObservationsTab._publish_selected_observations(
        _batch_tab({7: _CLEAN, 8: _DECLINED}, [7, 8], messages), "inat"
    )

    text, level = _final(messages)
    assert level == "success"
    assert "Published 1 observations" in text
    assert "failed" not in text.lower()


def test_batch_decline_does_not_inflate_the_failure_count():
    messages: list[tuple[str, str]] = []
    observations_tab.ObservationsTab._publish_selected_observations(
        _batch_tab({7: _CLEAN, 8: _DECLINED, 9: _HARD_FAILURE}, [7, 8, 9], messages), "inat"
    )

    text, level = _final(messages)
    assert level == "warning"
    assert "Failed: 1." in text  # the decline is not counted


def test_batch_reports_partial_success_and_hard_failure_together():
    messages: list[tuple[str, str]] = []
    observations_tab.ObservationsTab._publish_selected_observations(
        _batch_tab({7: _PARTIAL, 8: _HARD_FAILURE}, [7, 8], messages), "inat"
    )

    text, level = _final(messages)
    assert level == "warning"
    # The hard failure is reported...
    assert "Failed: 1." in text
    assert "create observation failed (422)" in text
    # ...and the remote observation that exists without its media is not dropped.
    assert "image upload failed" in text
    assert "ID 777" in text


def test_batch_reports_partial_success_and_total_failure_together():
    messages: list[tuple[str, str]] = []
    observations_tab.ObservationsTab._publish_selected_observations(
        _batch_tab({7: _PARTIAL, 8: _HARD_FAILURE, 9: _HARD_FAILURE}, [7, 8, 9], messages), "inat"
    )

    text, _level = _final(messages)
    assert "image upload failed" in text


def test_batch_all_clean_successes_keep_the_plain_success_summary():
    messages: list[tuple[str, str]] = []
    observations_tab.ObservationsTab._publish_selected_observations(
        _batch_tab({7: _CLEAN, 8: _CLEAN}, [7, 8], messages), "inat"
    )

    text, level = _final(messages)
    assert level == "success"
    assert text == "Published 2 observations to inat."


def test_batch_partial_success_only_is_a_warning():
    messages: list[tuple[str, str]] = []
    observations_tab.ObservationsTab._publish_selected_observations(
        _batch_tab({7: _PARTIAL}, [7], messages), "inat"
    )

    text, level = _final(messages)
    assert level == "warning"
    assert "with warnings" in text
    assert "image upload failed" in text


def test_batch_hard_failure_only_keeps_the_error_summary():
    messages: list[tuple[str, str]] = []
    observations_tab.ObservationsTab._publish_selected_observations(
        _batch_tab({7: _HARD_FAILURE}, [7], messages), "inat"
    )

    text, level = _final(messages)
    assert level == "error"
    assert "failed for all selected observations" in text
    assert "create observation failed (422)" in text


def test_selection_gate_still_blocks_non_inaturalist_targets():
    calls: list[str] = []
    tab = SimpleNamespace(
        _existing_upload_blocks_publish=lambda key: (
            observations_tab.ObservationsTab._existing_upload_blocks_publish(None, key)
        ),
        _selection_has_existing_upload_for_uploader=lambda key: calls.append(key) or True,
    )
    gate = observations_tab.ObservationsTab._selection_blocks_publish_for_uploader

    assert gate(tab, "inat") is False
    assert calls == []  # iNaturalist is not gated on the stored id at all
    assert gate(tab, "web") is True
    assert calls == ["web"]
