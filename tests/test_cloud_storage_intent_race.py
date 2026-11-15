"""A checkbox made while sync records first-time defaults survives.

Sync (a background thread) seeds the per-observation storage settings —
the excluded set and the intent ledger — while the GUI thread records the
user's checks. Both read, change and write the same settings. Before
2026-11-15 the seeding wrote back the whole set it had read earlier, so a
check made meanwhile was undone, and the two steps of one checkbox click
could be split by a seeding. Real isolated database; no fakes but the
pause points.
"""

from __future__ import annotations

import threading

from database.models import ImageDB, ObservationDB
from utils import cloud_sync
from utils.cloud_sync_impl import image_policy


def _observation_with_two_microscope_photos(tmp_path, name: str):
    obs_id = ObservationDB.create_observation(date="2026-11-07", genus="Inocybe", species="geophylla")
    ids = []
    for index in (3, 12):
        path = tmp_path / f"{name}-p{index}.jpg"
        path.write_bytes(b"jpeg")
        ids.append(ImageDB.add_image(
            obs_id, str(path), "microscope", objective_name="63X_1.32_PL_APO",
            copy_to_folder=False,
        ))
    return obs_id, ids[0], ids[1]


def test_a_check_made_while_defaults_are_computed_is_kept(tmp_path, monkeypatch):
    obs_id, p3, p12 = _observation_with_two_microscope_photos(tmp_path, "interleave")
    real_anchor_ids = image_policy._cloud_metadata_only_image_ids
    state = {"checked": False}

    def _user_checks_p12_meanwhile(observation_id):
        # Runs after seeding has read the ledger and the excluded set.
        if not state["checked"]:
            state["checked"] = True
            cloud_sync.set_image_cloud_selected(p12, True)
        return real_anchor_ids(observation_id)

    monkeypatch.setattr(image_policy, "_cloud_metadata_only_image_ids", _user_checks_p12_meanwhile)

    summary = cloud_sync._ensure_cloud_image_storage_intent_initialized(obs_id)

    assert state["checked"]
    assert cloud_sync._cloud_image_storage_excluded_image_ids(obs_id) == {p3}
    assert cloud_sync.cloud_image_bytes_desired(obs_id, p12)
    assert {p3, p12} <= cloud_sync._cloud_image_storage_intent_initialized_ids(obs_id)
    # Only P3 was seeded; P12 was the user's decision.
    assert summary == {"seeded_desired": 0, "seeded_excluded": 1}


def test_seeding_cannot_split_the_two_steps_of_a_click(tmp_path, monkeypatch):
    """The click un-excludes P12, then records the decision in the ledger.
    A seeding on the sync thread in between would see P12 undecided and
    exclude it again; the shared lock makes it wait for the whole click."""
    obs_id, p3, p12 = _observation_with_two_microscope_photos(tmp_path, "split")
    cloud_sync._set_cloud_image_storage_excluded_image_ids(obs_id, {p12})  # undecided, excluded

    between_steps = threading.Event()
    release_click = threading.Event()
    sync_wants_lock = threading.Event()
    real_mark = image_policy._mark_cloud_image_storage_intent_initialized
    real_lock = image_policy._STORAGE_INTENT_LOCK

    class _ObservedLock:
        """The real lock, noting when the sync thread asks for it."""

        def __enter__(self):
            if threading.current_thread().name == "sync":
                sync_wants_lock.set()
            return real_lock.__enter__()

        def __exit__(self, *exc):
            return real_lock.__exit__(*exc)

    monkeypatch.setattr(image_policy, "_STORAGE_INTENT_LOCK", _ObservedLock())

    def _pause_between_steps(observation_id, image_ids):
        if threading.current_thread().name == "click" and p12 in set(image_ids):
            between_steps.set()
            release_click.wait(timeout=5)
        return real_mark(observation_id, image_ids)

    monkeypatch.setattr(image_policy, "_mark_cloud_image_storage_intent_initialized", _pause_between_steps)

    errors: list[BaseException] = []

    def _run(target):
        try:
            target()
        except BaseException as exc:  # surfaced below
            errors.append(exc)

    click = threading.Thread(
        name="click", target=_run,
        args=(lambda: cloud_sync.set_image_cloud_selected(p12, True),),
    )
    seeding = threading.Thread(
        name="sync", target=_run,
        args=(lambda: cloud_sync._ensure_cloud_image_storage_intent_initialized(obs_id),),
    )
    try:
        click.start()
        assert between_steps.wait(timeout=5)
        seeding.start()
        # The seeding has asked for the lock the paused click holds ...
        assert sync_wants_lock.wait(timeout=5)
        seeding.join(timeout=0.2)
        # ... and waits for the whole click.
        assert seeding.is_alive()
    finally:
        release_click.set()
        click.join(timeout=5)
        seeding.join(timeout=5)

    assert not errors, errors
    assert not click.is_alive() and not seeding.is_alive()
    assert cloud_sync.cloud_image_bytes_desired(obs_id, p12)
    assert cloud_sync._cloud_image_storage_excluded_image_ids(obs_id) == {p3}


def test_concurrent_single_photo_changes_are_not_lost(tmp_path):
    """Many checks and unchecks from two threads at once: each photo ends
    in the state its own thread set (no lost update of the shared sets)."""
    obs_id = ObservationDB.create_observation(date="2026-11-07", genus="Inocybe", species="sp")
    photo_ids = []
    for index in range(16):
        path = tmp_path / f"many-{index}.jpg"
        path.write_bytes(b"jpeg")
        photo_ids.append(ImageDB.add_image(obs_id, str(path), "microscope", copy_to_folder=False))
    to_check, to_uncheck = photo_ids[::2], photo_ids[1::2]
    cloud_sync._set_cloud_image_storage_excluded_image_ids(obs_id, set(to_check))

    threads = [
        threading.Thread(target=lambda: [cloud_sync.set_image_cloud_selected(i, True) for i in to_check]),
        threading.Thread(target=lambda: [cloud_sync.set_image_cloud_selected(i, False) for i in to_uncheck]),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert cloud_sync._cloud_image_storage_excluded_image_ids(obs_id) == set(to_uncheck)
    assert set(photo_ids) <= cloud_sync._cloud_image_storage_intent_initialized_ids(obs_id)


def test_strip_change_never_rewrites_photos_it_does_not_show(tmp_path):
    """The strip writes only the photos whose checkbox changed: a photo it
    does not show or shows wrongly (added since it loaded, seeded by sync)
    keeps its state."""
    from ui.observations_tab import ObservationsTab

    obs_id, p3, p12 = _observation_with_two_microscope_photos(tmp_path, "unshown")
    cloud_sync._ensure_cloud_image_storage_intent_initialized(obs_id)  # both unchecked
    late = tmp_path / "unshown-late.jpg"
    late.write_bytes(b"jpeg")
    late_id = ImageDB.add_image(obs_id, str(late), "microscope", copy_to_folder=False)
    cloud_sync._ensure_cloud_image_storage_intent_initialized(obs_id)  # sync seeds it

    # The user checks P12; the strip reports that change only.
    changed = ObservationsTab._apply_strip_checkbox_change(obs_id, {p12: True})

    assert changed == {p12}
    assert cloud_sync._cloud_image_storage_excluded_image_ids(obs_id) == {p3, late_id}


def test_seeding_counts_an_already_excluded_photo_as_seeded_excluded(tmp_path):
    """The diagnostic summary classifies by the resulting set: a photo
    excluded before it had a recorded decision counts as seeded excluded."""
    obs_id = ObservationDB.create_observation(date="2026-11-07", genus="Inocybe", species="sp")
    path = tmp_path / "pre-excluded-field.jpg"
    path.write_bytes(b"jpeg")
    field_id = ImageDB.add_image(obs_id, str(path), "field", copy_to_folder=False)
    cloud_sync._set_cloud_image_storage_excluded_image_ids(obs_id, {field_id})

    summary = cloud_sync._ensure_cloud_image_storage_intent_initialized(obs_id)

    assert summary == {"seeded_desired": 0, "seeded_excluded": 1}
    assert cloud_sync._cloud_image_storage_excluded_image_ids(obs_id) == {field_id}


def _strip_with_three_photos(tmp_path, qtbot):
    from PySide6.QtGui import QColor, QImage
    from ui.image_gallery_widget import ImageGalleryWidget

    items = []
    for image_id, selected in ((1, True), (2, False), (3, False)):
        path = tmp_path / f"strip-{image_id}.png"
        image = QImage(24, 24, QImage.Format_ARGB32)
        image.fill(QColor("#888888"))
        assert image.save(str(path))
        items.append({"id": image_id, "filepath": str(path), "publish_selected": selected})
    strip = ImageGalleryWidget("Images", show_publish_checkbox=True)
    qtbot.addWidget(strip)
    strip.set_items(items)
    strip.show()
    qtbot.waitUntil(lambda: len(strip.publish_checkbox_widgets()) == 3, timeout=3000)
    return strip


def _toggle_for(strip, image_id):
    for frame in strip._frames:
        if getattr(frame, "image_key", None) == image_id:
            return frame.publish_checkbox
    raise AssertionError(image_id)


def test_a_click_reports_only_the_clicked_photo(tmp_path, qtbot):
    """Photo 3 shows unchecked whatever is stored (say it arrived while the
    strip reloaded); clicking photo 2 reports photo 2 only, so photo 3's
    stored state is never touched."""
    strip = _strip_with_three_photos(tmp_path, qtbot)
    seen: list[dict] = []
    strip.publishSelectionChanged.connect(lambda _ids: seen.append(strip.last_publish_change()))

    from PySide6.QtCore import Qt

    qtbot.mouseClick(_toggle_for(strip, 2), Qt.LeftButton)

    assert seen == [{2: True}]


def test_a_host_change_reports_what_it_flipped(tmp_path, qtbot):
    strip = _strip_with_three_photos(tmp_path, qtbot)
    seen: list[dict] = []
    strip.publishSelectionChanged.connect(lambda _ids: seen.append(strip.last_publish_change()))

    strip.set_publish_selected_ids({1, 3}, emit_signal=True)
    strip.mark_cloud_delete_pending([1])

    assert seen == [{3: True}, {1: False}]


def test_the_viewer_records_a_photo_the_strip_does_not_show(tmp_path):
    from types import SimpleNamespace
    from ui.observations_tab import ObservationsTab

    obs_id, p3, p12 = _observation_with_two_microscope_photos(tmp_path, "viewer-only")
    cloud_sync.set_image_cloud_selected(p12, True)
    strip = SimpleNamespace(
        publish_selected_ids=lambda: set(),  # still loading: shows nothing
        set_publish_selected_ids=lambda ids, emit_signal=False: None,
        refresh_cloud_states=lambda ids: 0,
    )
    tab = SimpleNamespace(
        selected_observation_id=obs_id,
        gallery_widget=strip,
        _apply_strip_checkbox_change=ObservationsTab._apply_strip_checkbox_change,
    )

    ObservationsTab._on_image_browser_publish_toggled(tab, p12, False)

    assert not cloud_sync.cloud_image_bytes_desired(obs_id, p12)
    assert p12 in cloud_sync._cloud_image_storage_intent_initialized_ids(obs_id)


def test_a_change_to_the_stored_state_writes_nothing(tmp_path, monkeypatch):
    """Both strips hear the same signal; the second sees the state already
    stored and must not repeat the transition (a repeat marks dirty again)."""
    from ui.observations_tab import ObservationsTab

    obs_id, p3, p12 = _observation_with_two_microscope_photos(tmp_path, "no-repeat")
    cloud_sync._ensure_cloud_image_storage_intent_initialized(obs_id)  # both unchecked
    calls: list = []
    monkeypatch.setattr(cloud_sync, "set_image_cloud_selected", lambda *args: calls.append(args))

    assert ObservationsTab._apply_strip_checkbox_change(obs_id, {p3: False}) == set()
    assert calls == []
