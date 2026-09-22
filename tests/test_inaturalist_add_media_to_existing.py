"""Stage 2 tests: add selected media to an iNaturalist observation that exists.

Stage 1 made a stored ``inaturalist_id`` something Sporely verifies rather than
trusts, but a link that verified *live* still refused to publish. That left no
way to add a corrected spore plot to a find that was already published. Stage 2
turns that branch into a media append: the existing remote observation gains the
currently selected publishing media, no second observation is created, and none
of the observation's metadata is touched.

Sporely stores no mapping between local images and iNaturalist photo ids, so the
append cannot deduplicate. These tests therefore also pin the explicit
confirmation that tells the user so.

All iNaturalist traffic is faked; nothing here touches the network.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from tests.test_inaturalist_stale_link_republish import (
    _FakeResponse,
    _OBSERVATION_BASE,
    _RecordingUploader,
    _batch_tab,
    _build_env,
    _link_status,
    _publish,
)
from ui import observations_tab
from utils.artsobs_uploaders import INaturalistUploader


_CREATE_URL = "https://api.inaturalist.org/v1/observations"
_PHOTOS_URL = "https://api.inaturalist.org/v1/observation_photos"


def _live(monkeypatch, uploader, *, confirm=True, **env_kwargs):
    return _build_env(
        monkeypatch,
        uploader,
        dict(_OBSERVATION_BASE, inaturalist_id=4242),
        confirm=confirm,
        **env_kwargs,
    )


# --------------------------------------------------------------------------
# INaturalistUploader.add_images - the API operation itself
# --------------------------------------------------------------------------


def _patch_post(monkeypatch, handler):
    posts: list[dict] = []

    def fake_post(url, headers=None, json=None, data=None, files=None, timeout=None):
        posts.append(
            {
                "url": url,
                "headers": dict(headers or {}),
                "json": json,
                "data": dict(data or {}),
                "files": sorted((files or {}).keys()),
            }
        )
        return handler(len(posts))

    monkeypatch.setattr("utils.artsobs_uploaders.requests.post", fake_post)
    return posts


def _images(tmp_path, count: int) -> list[str]:
    paths = []
    for idx in range(count):
        path = tmp_path / f"image-{idx}.jpg"
        path.write_bytes(b"jpeg")
        paths.append(str(path))
    return paths


def test_add_images_posts_one_observation_photo_per_image(monkeypatch, tmp_path):
    paths = _images(tmp_path, 3)
    posts = _patch_post(monkeypatch, lambda _n: _FakeResponse(200, {"id": 11}))

    result = INaturalistUploader().add_images(4242, paths, {"access_token": "tok"})

    assert [post["url"] for post in posts] == [_PHOTOS_URL] * 3
    # Never a create request in this mode.
    assert _CREATE_URL not in [post["url"] for post in posts]
    assert all(post["data"]["observation_photo[observation_id]"] == "4242" for post in posts)
    assert all(post["headers"]["Authorization"] == "Bearer tok" for post in posts)
    assert all(post["files"] == ["file"] for post in posts)
    assert result.sighting_id == 4242
    assert result.raw["images_uploaded"] == 3
    assert result.raw["images_requested"] == 3
    assert "image_upload_error" not in result.raw


def test_add_images_reports_nothing_attached_when_the_first_post_fails(monkeypatch, tmp_path):
    paths = _images(tmp_path, 2)
    posts = _patch_post(monkeypatch, lambda _n: _FakeResponse(422, {"errors": ["Photo is invalid"]}))

    result = INaturalistUploader().add_images(4242, paths, {"access_token": "tok"})

    # It stops at the first failure instead of hammering the rest.
    assert len(posts) == 1
    assert result.sighting_id == 4242
    assert result.raw["images_uploaded"] == 0
    assert "Photo is invalid" in result.raw["image_upload_error"]


def test_add_images_reports_partial_attachment_when_a_later_post_fails(monkeypatch, tmp_path):
    paths = _images(tmp_path, 3)
    posts = _patch_post(
        monkeypatch,
        lambda n: _FakeResponse(200, {"id": 11}) if n == 1 else _FakeResponse(500, None, "boom"),
    )

    result = INaturalistUploader().add_images(4242, paths, {"access_token": "tok"})

    assert len(posts) == 2
    assert result.raw["images_uploaded"] == 1
    assert result.raw["images_requested"] == 3
    assert "500" in result.raw["image_upload_error"]


def test_add_images_reports_partial_attachment_on_a_network_failure(monkeypatch, tmp_path):
    paths = _images(tmp_path, 2)

    def handler(n):
        if n == 1:
            return _FakeResponse(200, {"id": 11})
        raise OSError("connection reset")

    _patch_post(monkeypatch, handler)

    result = INaturalistUploader().add_images(4242, paths, {"access_token": "tok"})

    assert result.raw["images_uploaded"] == 1
    assert "connection reset" in result.raw["image_upload_error"]


def test_add_images_reports_progress_without_overrunning_its_total(monkeypatch, tmp_path):
    paths = _images(tmp_path, 2)
    _patch_post(monkeypatch, lambda _n: _FakeResponse(200, {"id": 11}))
    steps: list[tuple[str, int, int]] = []

    INaturalistUploader().add_images(
        4242,
        paths,
        {"access_token": "tok"},
        progress_cb=lambda text, current, total: steps.append((text, current, total)),
    )

    assert steps
    assert all(0 <= current <= total for _text, current, total in steps)
    assert steps[-1][1] == steps[-1][2]


@pytest.mark.parametrize(
    "observation_id, paths, cookies, match",
    [
        (4242, ["/tmp/a.jpg"], {}, "access token"),
        (0, ["/tmp/a.jpg"], {"access_token": "tok"}, "observation id"),
        (None, ["/tmp/a.jpg"], {"access_token": "tok"}, "observation id"),
        (4242, [], {"access_token": "tok"}, "No images"),
    ],
)
def test_add_images_refuses_before_posting_anything(
    monkeypatch, observation_id, paths, cookies, match
):
    posts = _patch_post(monkeypatch, lambda _n: _FakeResponse(200, {"id": 11}))

    with pytest.raises(RuntimeError, match=match):
        INaturalistUploader().add_images(observation_id, paths, cookies)

    assert posts == []


def test_upload_still_creates_and_is_not_an_append_in_disguise(monkeypatch, tmp_path):
    """``upload()`` must stay unambiguously "create", whatever it is handed."""
    paths = _images(tmp_path, 1)
    posts = _patch_post(
        monkeypatch,
        lambda n: _FakeResponse(200, {"results": [{"id": 4321}]} if n == 1 else {"id": 9}),
    )

    result = INaturalistUploader().upload(
        # A stored id in the payload must not turn this into an append.
        {"species_guess": "Atheniella flavoalba", "inaturalist_id": 4242},
        paths,
        {"access_token": "tok"},
    )

    assert [post["url"] for post in posts] == [_CREATE_URL, _PHOTOS_URL]
    assert result.sighting_id == 4321
    assert posts[1]["data"]["observation_photo[observation_id]"] == "4321"


# --------------------------------------------------------------------------
# The publish flow: live link -> append, not create
# --------------------------------------------------------------------------


def test_live_link_appends_one_selected_image_to_the_existing_observation(monkeypatch):
    uploader = _RecordingUploader(_link_status("live"))
    fake_tab, recorded = _live(monkeypatch, uploader, base_image_paths=["/tmp/plot.jpg"])

    ok, published_id, error = _publish(fake_tab)

    assert (ok, published_id, error) == (True, 4242, None)
    assert uploader.link_checks == [4242]
    assert uploader.uploads == []  # no create-observation call
    assert uploader.appends == [(4242, ["/tmp/plot.jpg"])]
    assert recorded["set_inat_calls"] == []  # the stored id is never rewritten
    assert recorded["status_messages"][-1][1] == "success"
    assert "Added 1 image(s)" in recorded["status_messages"][-1][0]


def test_live_link_appends_every_selected_image_to_the_same_observation(monkeypatch):
    paths = ["/tmp/a.jpg", "/tmp/b.jpg", "/tmp/c.jpg"]
    uploader = _RecordingUploader(_link_status("live"))
    fake_tab, recorded = _live(monkeypatch, uploader, base_image_paths=paths)

    ok, published_id, error = _publish(fake_tab)

    assert (ok, published_id, error) == (True, 4242, None)
    assert uploader.appends == [(4242, paths)]
    assert uploader.uploads == []
    assert recorded["set_inat_calls"] == []


def test_append_sends_the_prepared_media_pipeline_output_not_the_raw_selection(monkeypatch):
    """Generated derivatives must reach the append, and only through the pipeline.

    The append must not build its own image variants or quietly fall back to the
    gallery paths: the paths it posts are exactly what
    ``_prepare_publish_media_assets`` returned.
    """
    base = ["/tmp/raw-1.jpg", "/tmp/raw-2.jpg"]
    prepared = ["/tmp/prepared/annotated-1.jpg", "/tmp/prepared/plot.png"]
    uploader = _RecordingUploader(_link_status("live"))
    fake_tab, recorded = _live(
        monkeypatch,
        uploader,
        base_image_paths=base,
        prepared_image_paths=prepared,
    )

    ok, _published_id, _error = _publish(fake_tab)

    assert ok is True
    # The pipeline ran once, for this observation, with the gallery selection.
    assert len(recorded["prepare_calls"]) == 1
    prepare_call = recorded["prepare_calls"][0]
    assert prepare_call["observation_id"] == 7
    assert prepare_call["base_image_paths"] == base
    # And its output - not the raw selection - is what was attached.
    assert uploader.appends == [(4242, prepared)]


def test_append_keeps_the_stored_id_unchanged_on_success(monkeypatch):
    uploader = _RecordingUploader(_link_status("live"))
    fake_tab, recorded = _live(monkeypatch, uploader)

    ok, published_id, _error = _publish(fake_tab)

    assert ok is True
    assert published_id == 4242
    assert recorded["set_inat_calls"] == []


def test_append_failure_on_the_first_image_preserves_the_link_and_creates_nothing(monkeypatch):
    uploader = _RecordingUploader(
        _link_status("live"),
        images_attached=0,
        image_upload_error="iNaturalist image upload failed (422): Photo is invalid",
    )
    fake_tab, recorded = _live(
        monkeypatch, uploader, base_image_paths=["/tmp/a.jpg", "/tmp/b.jpg"]
    )

    ok, published_id, error = _publish(fake_tab)

    # Nothing arrived, so this is an honest failure rather than a partial success.
    assert ok is False
    assert published_id is None
    assert "could not add images" in (error or "")
    assert "Photo is invalid" in (error or "")
    assert "unchanged" in (error or "")
    assert uploader.uploads == []
    assert recorded["set_inat_calls"] == []
    assert recorded["status_messages"][-1][1] == "warning"


def test_append_reports_partial_success_when_a_later_image_fails(monkeypatch):
    uploader = _RecordingUploader(
        _link_status("live"),
        images_attached=1,
        image_upload_error="iNaturalist image upload failed (500): Internal Server Error",
    )
    fake_tab, recorded = _live(
        monkeypatch, uploader, base_image_paths=["/tmp/a.jpg", "/tmp/b.jpg", "/tmp/c.jpg"]
    )

    ok, published_id, error = _publish(fake_tab)

    # ok with a message is the Stage 1 partial-success shape.
    assert ok is True
    assert published_id == 4242
    assert error is not None
    assert "Added 1 of 3 images" in error
    assert "Internal Server Error" in error
    # The loop stops at the first failure, so image 3 was never sent. Calling it
    # failed would be as untrue as calling the whole append a failure.
    assert "not attempted" in error
    assert "the rest failed" not in error
    assert "earlier photos are unchanged" in error
    assert uploader.uploads == []
    assert recorded["set_inat_calls"] == []
    assert recorded["status_messages"][-1][1] == "warning"


def test_append_raising_outright_preserves_the_link_and_creates_nothing(monkeypatch):
    uploader = _RecordingUploader(
        _link_status("live"),
        append_error=RuntimeError("Missing iNaturalist access token."),
    )
    fake_tab, recorded = _live(monkeypatch, uploader)

    ok, published_id, error = _publish(fake_tab)

    assert ok is False
    assert published_id is None
    assert "Missing iNaturalist access token." in (error or "")
    assert uploader.uploads == []
    assert recorded["set_inat_calls"] == []


def test_live_link_no_longer_refuses_with_the_old_already_has_an_id_message(monkeypatch):
    uploader = _RecordingUploader(_link_status("live"))
    fake_tab, recorded = _live(monkeypatch, uploader)

    ok, _published_id, error = _publish(fake_tab)

    assert ok is True
    assert "already has an ID" not in (error or "")
    assert not any(
        "already has an ID" in text for text, _level in recorded["status_messages"]
    )


# --------------------------------------------------------------------------
# The confirmation that carries the duplicate-media limitation
# --------------------------------------------------------------------------


def test_append_is_confirmed_with_the_count_and_the_duplicate_warning(monkeypatch):
    uploader = _RecordingUploader(_link_status("live"))
    fake_tab, recorded = _live(
        monkeypatch, uploader, base_image_paths=["/tmp/a.jpg", "/tmp/b.jpg"]
    )

    _publish(fake_tab)

    assert len(recorded["prompts"]) == 1
    prompt = recorded["prompts"][0]
    assert "2 selected image(s)" in prompt
    assert "4242" in prompt
    # The user is told Sporely cannot know what is already there.
    assert "duplicate" in prompt
    assert "does not track" in prompt


def test_declining_the_append_confirmation_is_a_skip_not_a_failure(monkeypatch):
    uploader = _RecordingUploader(_link_status("live"))
    fake_tab, recorded = _live(monkeypatch, uploader, confirm=False)

    ok, published_id, error = _publish(fake_tab)

    # The (False, None, None) shape the batch wrapper reads as "cancelled".
    assert (ok, published_id, error) == (False, None, None)
    assert uploader.appends == []
    assert uploader.uploads == []
    assert recorded["set_inat_calls"] == []
    assert recorded["status_messages"] == []
    assert recorded["prompts"]


def test_append_is_confirmed_only_after_the_media_are_prepared(monkeypatch):
    """The count in the prompt must be the prepared count, not the gallery count."""
    uploader = _RecordingUploader(_link_status("live"))
    fake_tab, recorded = _live(
        monkeypatch,
        uploader,
        base_image_paths=["/tmp/a.jpg"],
        prepared_image_paths=["/tmp/a-annotated.jpg", "/tmp/plot.png", "/tmp/plate.png"],
    )

    _publish(fake_tab)

    assert recorded["prepare_calls"]
    assert "3 selected image(s)" in recorded["prompts"][0]


def test_append_with_an_empty_selection_fails_with_a_useful_message(monkeypatch):
    uploader = _RecordingUploader(_link_status("live"))
    fake_tab, recorded = _live(monkeypatch, uploader, base_image_paths=[])

    ok, published_id, error = _publish(fake_tab)

    assert ok is False
    assert published_id is None
    assert "no images are selected" in (error or "")
    assert uploader.appends == []
    assert uploader.uploads == []
    # Nothing to confirm when there is nothing to send.
    assert recorded["prompts"] == []


# --------------------------------------------------------------------------
# Append is independent of create-only metadata
# --------------------------------------------------------------------------
#
# ``add_images()`` posts nothing but photos, so an append must not be refused
# over local fields it never transmits. These tests also prove the create
# payload is not built in append mode: constructing it evaluates ``float(lat)``,
# which would raise on a missing coordinate and turn the publish into a failure.


@pytest.mark.parametrize(
    "missing",
    [
        pytest.param({"gps_latitude": None, "gps_longitude": None}, id="no-gps"),
        pytest.param({"gps_latitude": None}, id="no-latitude"),
        pytest.param({"date": None}, id="no-date"),
        pytest.param({"date": ""}, id="blank-date"),
        pytest.param(
            {"gps_latitude": None, "gps_longitude": None, "date": None},
            id="no-gps-and-no-date",
        ),
    ],
)
def test_append_proceeds_without_create_only_metadata(monkeypatch, missing):
    uploader = _RecordingUploader(_link_status("live"))
    fake_tab, recorded = _build_env(
        monkeypatch,
        uploader,
        dict(_OBSERVATION_BASE, inaturalist_id=4242, **missing),
        confirm=True,
        base_image_paths=["/tmp/plot.jpg"],
    )

    ok, published_id, error = _publish(fake_tab)

    assert (ok, published_id, error) == (True, 4242, None)
    assert uploader.appends == [(4242, ["/tmp/plot.jpg"])]
    assert uploader.uploads == []
    assert recorded["set_inat_calls"] == []


def test_create_still_requires_gps_and_date(monkeypatch):
    for missing, expected in (
        ({"gps_latitude": None, "gps_longitude": None}, "missing GPS coordinates"),
        ({"gps_longitude": None}, "missing GPS coordinates"),
        ({"date": None}, "observation date is missing"),
        ({"date": ""}, "observation date is missing"),
    ):
        uploader = _RecordingUploader(_link_status("live"), sighting_id=555)
        fake_tab, recorded = _build_env(
            monkeypatch,
            uploader,
            dict(_OBSERVATION_BASE, inaturalist_id=None, **missing),
            confirm=True,
        )

        ok, published_id, error = _publish(fake_tab)

        assert (ok, published_id) == (False, None), missing
        assert expected in (error or ""), missing
        assert uploader.uploads == []
        assert uploader.appends == []
        assert recorded["set_inat_calls"] == []


def test_stale_link_republish_still_requires_create_metadata(monkeypatch):
    """A republish is a create, so deferring the check must not let it through."""
    for missing, expected in (
        ({"gps_latitude": None, "gps_longitude": None}, "missing GPS coordinates"),
        ({"date": None}, "observation date is missing"),
    ):
        uploader = _RecordingUploader(_link_status("missing"), sighting_id=777)
        fake_tab, recorded = _build_env(
            monkeypatch,
            uploader,
            dict(_OBSERVATION_BASE, inaturalist_id=4242, **missing),
            confirm=True,
            base_image_paths=["/tmp/plot.jpg"],
        )

        ok, published_id, error = _publish(fake_tab)

        assert (ok, published_id) == (False, None), missing
        assert expected in (error or ""), missing
        assert uploader.uploads == []  # no create attempted
        assert uploader.appends == []
        assert recorded["set_inat_calls"] == []  # the stale id survives


def test_unverified_link_without_create_metadata_still_reports_the_link_failure(monkeypatch):
    """The deferred metadata check must not pre-empt the Stage 1 refusal."""
    uploader = _RecordingUploader(_link_status("unverified", "HTTP 503"))
    fake_tab, recorded = _build_env(
        monkeypatch,
        uploader,
        dict(_OBSERVATION_BASE, inaturalist_id=4242, gps_latitude=None, gps_longitude=None),
        confirm=True,
    )

    ok, published_id, error = _publish(fake_tab)

    assert (ok, published_id) == (False, None)
    assert "could not check" in (error or "")
    assert uploader.uploads == []
    assert uploader.appends == []
    assert recorded["set_inat_calls"] == []


def test_declined_republish_without_create_metadata_is_still_a_plain_skip(monkeypatch):
    uploader = _RecordingUploader(_link_status("missing"))
    fake_tab, recorded = _build_env(
        monkeypatch,
        uploader,
        dict(_OBSERVATION_BASE, inaturalist_id=4242, date=None),
        confirm=False,
    )

    ok, published_id, error = _publish(fake_tab)

    # The decline is resolved before the deferred metadata refusal, so this
    # stays the (False, None, None) skip shape rather than becoming a failure.
    assert (ok, published_id, error) == (False, None, None)
    assert uploader.uploads == []
    assert uploader.appends == []
    assert recorded["set_inat_calls"] == []


def test_append_never_resolves_a_taxon(monkeypatch):
    """Taxon resolution is create-only, so an append must not even attempt it.

    ``_resolve_inaturalist_taxon_id()`` reads local taxonomy tables and, on a
    miss, the Artsdatabanken taxon file. ``add_images()`` sends no taxon and
    changes no remote metadata, so an append that depended on it would be
    coupled to state it never transmits. Raising on call proves the coupling is
    gone rather than merely unused.
    """
    def _explode(obs):
        raise AssertionError("taxon resolution must not run for a media append")

    uploader = _RecordingUploader(_link_status("live"))
    fake_tab, recorded = _live(monkeypatch, uploader, base_image_paths=["/tmp/plot.jpg"])
    fake_tab._resolve_inaturalist_taxon_id = _explode

    ok, published_id, error = _publish(fake_tab)

    assert (ok, published_id, error) == (True, 4242, None)
    assert uploader.appends == [(4242, ["/tmp/plot.jpg"])]
    assert uploader.uploads == []
    assert recorded["set_inat_calls"] == []


def test_create_and_republish_still_resolve_a_taxon(monkeypatch):
    """The deferral must not quietly drop taxon resolution from the create paths."""
    for stored_id, link_state, expected_new_id in (
        (None, "live", 555),
        (4242, "missing", 777),
    ):
        resolved: list[int] = []
        uploader = _RecordingUploader(_link_status(link_state), sighting_id=expected_new_id)
        fake_tab, recorded = _build_env(
            monkeypatch,
            uploader,
            dict(_OBSERVATION_BASE, inaturalist_id=stored_id),
            confirm=True,
        )
        fake_tab._resolve_inaturalist_taxon_id = lambda obs: resolved.append(48484) or 48484

        ok, published_id, _error = _publish(fake_tab)

        assert (ok, published_id) == (True, expected_new_id), stored_id
        assert resolved == [48484], stored_id
        assert uploader.uploads  # the create really ran
        assert recorded["set_inat_calls"] == [(7, expected_new_id)], stored_id


@pytest.mark.parametrize("link_state", ["unverified", "missing"])
def test_refused_or_declined_publish_resolves_no_taxon(monkeypatch, link_state):
    """An unverified link refuses, and a declined republish skips, before taxon work."""
    def _explode(obs):
        raise AssertionError("taxon resolution must not run for a refused publish")

    uploader = _RecordingUploader(_link_status(link_state, "HTTP 503"))
    # confirm=False makes the "missing" case a decline rather than a republish.
    fake_tab, recorded = _live(monkeypatch, uploader, confirm=False)
    fake_tab._resolve_inaturalist_taxon_id = _explode

    ok, published_id, _error = _publish(fake_tab)

    assert (ok, published_id) == (False, None)
    assert uploader.uploads == []
    assert uploader.appends == []
    assert recorded["set_inat_calls"] == []


def test_append_without_create_metadata_still_refuses_an_empty_selection(monkeypatch):
    uploader = _RecordingUploader(_link_status("live"))
    fake_tab, recorded = _build_env(
        monkeypatch,
        uploader,
        dict(_OBSERVATION_BASE, inaturalist_id=4242, gps_latitude=None, gps_longitude=None),
        confirm=True,
        base_image_paths=[],
    )

    ok, published_id, error = _publish(fake_tab)

    assert (ok, published_id) == (False, None)
    assert "no images are selected" in (error or "")
    assert uploader.appends == []
    assert recorded["set_inat_calls"] == []


# --------------------------------------------------------------------------
# Stage 1 behaviour that Stage 2 must not disturb
# --------------------------------------------------------------------------


def test_stale_link_still_republishes_as_a_new_observation(monkeypatch):
    uploader = _RecordingUploader(_link_status("missing"), sighting_id=777)
    fake_tab, recorded = _build_env(
        monkeypatch, uploader, dict(_OBSERVATION_BASE, inaturalist_id=4242), confirm=True
    )

    ok, published_id, error = _publish(fake_tab)

    assert (ok, published_id, error) == (True, 777, None)
    assert uploader.uploads == [["/tmp/spore-plot.jpg"]]
    assert uploader.appends == []  # a gone observation cannot be appended to
    assert recorded["set_inat_calls"] == [(7, 777)]


@pytest.mark.parametrize("detail", ["401 Unauthorized", "connection timed out", "HTTP 503"])
def test_unverifiable_link_still_neither_creates_nor_appends(monkeypatch, detail):
    uploader = _RecordingUploader(_link_status("unverified", detail))
    fake_tab, recorded = _build_env(
        monkeypatch, uploader, dict(_OBSERVATION_BASE, inaturalist_id=4242), confirm=True
    )

    ok, published_id, error = _publish(fake_tab)

    assert ok is False
    assert published_id is None
    assert "could not check" in (error or "")
    assert uploader.uploads == []
    assert uploader.appends == []
    assert recorded["set_inat_calls"] == []
    assert recorded["prompts"] == []


def test_observation_without_a_stored_id_still_takes_the_create_path(monkeypatch):
    uploader = _RecordingUploader(_link_status("live"), sighting_id=555)
    fake_tab, recorded = _build_env(
        monkeypatch, uploader, dict(_OBSERVATION_BASE, inaturalist_id=None), confirm=False
    )

    ok, published_id, error = _publish(fake_tab)

    assert (ok, published_id, error) == (True, 555, None)
    assert uploader.link_checks == []
    assert uploader.uploads == [["/tmp/spore-plot.jpg"]]
    assert uploader.appends == []
    assert recorded["set_inat_calls"] == [(7, 555)]
    # A first publish must not acquire an append confirmation.
    assert recorded["prompts"] == []


def test_stored_inaturalist_id_still_blocks_the_combined_both_action(monkeypatch):
    """Both publishes Artsobservasjoner first, so append cannot be offered there."""
    calls: list[str] = []
    messages: list[tuple[str, str]] = []
    tab = SimpleNamespace(
        tr=lambda text: text,
        _selected_observation_ids=lambda: [7],
        _publish_target_login_status=lambda force_refresh=False: {"web": True, "inat": True},
        _publish_target_saved_login_status=lambda force_refresh=False: {"web": True, "inat": True},
        _open_online_publishing_settings=lambda: False,
        _invalidate_publish_login_status_cache=lambda: None,
        _update_publish_controls=lambda: None,
        _publish_actions={},
        _selection_has_existing_upload_for_uploader=lambda key: key == "inat",
        refresh_observations=lambda *args, **kwargs: None,
        set_status_message=lambda message, level="info", auto_clear_ms=8000: messages.append(
            (str(message), str(level))
        ),
        upload_observation_to_artsobs=lambda *args, **kwargs: calls.append("published"),
    )

    observations_tab.ObservationsTab._publish_selected_observations_both(tab)

    assert calls == []
    assert "already uploaded to this service" in messages[-1][0]


# --------------------------------------------------------------------------
# Batch wrapper consistency
# --------------------------------------------------------------------------


def test_batch_reports_a_clean_append_as_a_success(monkeypatch):
    uploader = _RecordingUploader(_link_status("live"))
    fake_tab, _recorded = _live(monkeypatch, uploader)
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

    text, level = messages[-1]
    assert level == "success"
    assert "failed" not in text.lower()
    assert uploader.appends == [(4242, ["/tmp/spore-plot.jpg"])]


def test_batch_reports_a_partial_append_as_a_warning(monkeypatch):
    uploader = _RecordingUploader(
        _link_status("live"),
        images_attached=1,
        image_upload_error="iNaturalist image upload failed (500): Internal Server Error",
    )
    fake_tab, _recorded = _live(
        monkeypatch, uploader, base_image_paths=["/tmp/a.jpg", "/tmp/b.jpg"]
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

    text, level = messages[-1]
    assert level == "warning"
    assert "with warnings" in text
    assert "Added 1 of 2 images" in text
    assert "not attempted" in text


def test_batch_reports_a_declined_append_as_cancelled(monkeypatch):
    uploader = _RecordingUploader(_link_status("live"))
    fake_tab, recorded = _live(monkeypatch, uploader, confirm=False)
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

    text, level = messages[-1]
    assert level != "error"
    assert "cancelled" in text.lower()
    assert recorded["set_inat_calls"] == []
    assert uploader.appends == []
