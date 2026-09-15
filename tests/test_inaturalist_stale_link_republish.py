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

    def __init__(self, link_status, sighting_id: int | None = 999, error: Exception | None = None):
        self._link_status = link_status
        self._sighting_id = sighting_id
        self._error = error
        self.link_checks: list[int] = []
        self.uploads: list[list[str]] = []

    def check_observation_link(self, observation_id, cookies=None, timeout=None):
        self.link_checks.append(int(observation_id))
        return self._link_status

    def upload(self, observation_payload, image_paths, cookies, progress_cb=None):
        self.uploads.append(list(image_paths or []))
        if self._error is not None:
            raise self._error
        return SimpleNamespace(sighting_id=self._sighting_id, raw={})


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
