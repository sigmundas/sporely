"""The Observation details dialog attributes the coordinate edits made in it.

Contract: docs/supabase-sync-contract.md, "Private ingestion and location
provenance" (desktop requirement 5 and the desktop implementation notes).
These check what ``get_data()`` returns; interactive behavior is gate G4.
"""
from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication

import ui.observations_tab as observations_tab

_PAIR = ("location_source", "location_status")


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


@pytest.fixture
def make_dialog(monkeypatch, qapp):
    fake_client = SimpleNamespace(
        user_id="user-123",
        fetch_cloud_plan_profile=lambda: {"cloud_plan": "free", "is_pro": False},
        count_remote_privacy_slots=lambda: 0,
        list_remote_observations=lambda: [],
    )
    dialog_cls = observations_tab.ObservationDetailsDialog
    monkeypatch.setattr(observations_tab.SettingsDB, "get_setting", lambda key, default=None: default)
    monkeypatch.setattr(dialog_cls, "_load_objectives", lambda self: {"default": {"is_default": True}})
    monkeypatch.setattr(dialog_cls, "_load_tag_options", lambda self, category: [f"{category}-default"])
    monkeypatch.setattr(dialog_cls, "_load_habitat_tree", lambda self, filename: [])
    monkeypatch.setattr(dialog_cls, "_apply_primary_metadata", lambda self: None)
    monkeypatch.setattr(dialog_cls, "_apply_suggested_taxon", lambda self: None)
    monkeypatch.setattr(dialog_cls, "_sync_taxon_cache", lambda self: None)
    monkeypatch.setattr(dialog_cls, "_complete_deferred_dialog_setup", lambda self: None)
    monkeypatch.setattr(dialog_cls, "_schedule_location_lookup", lambda self, *a, **k: None)
    monkeypatch.setattr(observations_tab.SporelyCloudClient, "from_stored_credentials", lambda: fake_client)
    dialogs = []

    def _make(observation=None, draft_data=None):
        dialog = dialog_cls(parent=None, observation=observation, draft_data=draft_data, image_results=[])
        qapp.processEvents()
        dialogs.append(dialog)
        return dialog

    yield _make
    for dialog in dialogs:
        dialog.deleteLater()
    qapp.processEvents()


def _existing():
    return {
        "id": 1, "date": "2026-09-20 10:00:00", "genus": "Agaricus", "species": "campestris",
        "gps_latitude": 63.4, "gps_longitude": 10.4,
        "location_source": "device_gps", "location_status": "acquired",
    }


def _pair(data):
    return tuple(data.get(field) for field in _PAIR) if _PAIR[0] in data else None


def test_unchanged_coordinates_keep_the_stored_pair(make_dialog):
    dialog = make_dialog(_existing())
    assert _pair(dialog.get_data()) == ("device_gps", "acquired")
    unattributed = dict(_existing(), location_source=None, location_status=None)
    assert _pair(make_dialog(unattributed).get_data()) is None


def test_typed_coordinates_are_manual(make_dialog):
    dialog = make_dialog(_existing())
    dialog.lat_input.setValue(60.0)
    assert _pair(dialog.get_data()) == ("manual", "acquired")


def test_coordinates_applied_from_photo_exif_are_image_exif(make_dialog):
    dialog = make_dialog(_existing())
    dialog.image_metadata = [{"latitude": 61.5, "longitude": 9.25, "filename": "a.jpg"}]
    dialog._apply_metadata_from_index(0)
    assert _pair(dialog.get_data()) == ("image_exif", "acquired")
    dialog.lat_input.setValue(61.0)
    assert _pair(dialog.get_data()) == ("manual", "acquired"), "a later typed edit is manual"


def test_clearing_existing_coordinates_is_none_skipped(make_dialog):
    dialog = make_dialog(_existing())
    dialog.lat_input.setValue(dialog.lat_input.minimum())
    dialog.lon_input.setValue(dialog.lon_input.minimum())
    data = dialog.get_data()
    assert (data["gps_latitude"], data["gps_longitude"]) == (None, None)
    assert _pair(data) == ("none", "skipped")


def test_new_observation_without_coordinates_stays_unattributed(make_dialog):
    dialog = make_dialog()
    assert _pair(dialog.get_data()) is None
    dialog.lat_input.setValue(60.0)
    dialog.lon_input.setValue(10.0)
    assert _pair(dialog.get_data()) == ("manual", "acquired")


# ── Edit Images detour (review round 2) ─────────────────────────────────────


def test_pending_pair_survives_only_while_its_coordinates_stay_selected():
    draft = {"gps_latitude": 60.0, "gps_longitude": 10.0, "location_source": "manual", "location_status": "acquired"}
    assert observations_tab._pending_location_pair(draft, 60.0, 10.0) == ("manual", "acquired")
    assert observations_tab._pending_location_pair(draft, 61.0, 10.0) is None, "Prepare Images replaced them"
    assert observations_tab._pending_location_pair({"gps_latitude": 60.0, "gps_longitude": 10.0}, 60.0, 10.0) is None
    cleared = {"gps_latitude": None, "gps_longitude": None, "location_source": "none", "location_status": "skipped"}
    assert observations_tab._pending_location_pair(cleared, None, None) == ("none", "skipped")


def test_existing_observation_reopened_after_edit_images_keeps_the_manual_attribution(make_dialog):
    first = make_dialog(_existing())
    first.lat_input.setValue(60.0)
    first.lon_input.setValue(10.0)
    data = first.get_data()
    assert _pair(data) == ("manual", "acquired")
    # ObservationsTab reopens the dialog on the draft merged over the record.
    reopened = make_dialog(dict(_existing(), **data))
    assert _pair(reopened.get_data()) == ("manual", "acquired")


def test_new_observation_draft_keeps_the_exif_attribution(make_dialog):
    first = make_dialog()
    first.image_metadata = [{"latitude": 61.5, "longitude": 9.25, "filename": "a.jpg"}]
    first._apply_metadata_from_index(0)
    data = first.get_data()
    assert _pair(data) == ("image_exif", "acquired")
    reopened = make_dialog(draft_data=dict(data))
    assert _pair(reopened.get_data()) == ("image_exif", "acquired")


def test_draft_whose_coordinates_were_replaced_is_not_reattributed(make_dialog):
    draft = {"date": "2026-09-20 10:00:00", "gps_latitude": 60.0, "gps_longitude": 10.0}
    # The new-observation flow drops the pair when Prepare Images moved the point.
    reopened = make_dialog(draft_data=draft)
    assert _pair(reopened.get_data()) is None


# --- New observation created from imported photos (the real constructor path:
# Prepare Images hands the dialog its import results; nothing private is poked).


@pytest.fixture
def open_details(make_dialog, monkeypatch):
    """Open the new-observation details dialog on Prepare Images' results."""
    dialog_cls = observations_tab.ObservationDetailsDialog
    # The shared fixture stubs this out; the import path needs the real one.
    monkeypatch.setattr(dialog_cls, "_apply_primary_metadata", _REAL_APPLY_PRIMARY_METADATA)
    made = []

    def _open(results, draft_data=None):
        dialog = dialog_cls(parent=None, observation=None, draft_data=draft_data, image_results=results)
        made.append(dialog)
        return dialog

    yield _open
    for dialog in made:
        dialog.deleteLater()


@pytest.fixture
def make_import_dialog(open_details):
    from ui.image_import_dialog import ImageImportResult

    def _make(*photos, draft_data=None):
        # Rows as an import candidate produces them: the file's EXIF GPS is
        # both the editable coordinates and the recorded import-time origin.
        results = [
            ImageImportResult(
                filepath=f"/tmp/photo-{i}.jpg",
                image_id=photo.get("image_id"),
                gps_latitude=photo.get("lat"),
                gps_longitude=photo.get("lon"),
                exif_has_gps=photo.get("lat") is not None,
                exif_gps_latitude=photo.get("lat"),
                exif_gps_longitude=photo.get("lon"),
                gps_source=bool(photo.get("gps_source")),
            )
            for i, photo in enumerate(photos)
        ]
        return open_details(results, draft_data=draft_data)

    return _make


_REAL_APPLY_PRIMARY_METADATA = observations_tab.ObservationDetailsDialog._apply_primary_metadata


def test_new_observation_from_geotagged_photo_is_image_exif(make_import_dialog):
    dialog = make_import_dialog({"lat": 59.9139, "lon": 10.7522, "gps_source": True})
    data = dialog.get_data()
    assert (data["gps_latitude"], data["gps_longitude"]) == pytest.approx((59.9139, 10.7522))
    assert _pair(data) == ("image_exif", "acquired")


def test_new_observation_typed_over_photo_exif_is_manual(make_import_dialog):
    dialog = make_import_dialog({"lat": 59.9139, "lon": 10.7522})
    dialog.lat_input.setValue(60.5)
    assert _pair(dialog.get_data()) == ("manual", "acquired")
    dialog.lat_input.setValue(59.9139)
    assert _pair(dialog.get_data()) == ("image_exif", "acquired"), "back on the photo's EXIF"


def test_new_observation_from_photo_without_gps_stays_unattributed(make_import_dialog):
    dialog = make_import_dialog({})
    assert _pair(dialog.get_data()) is None


def test_stored_image_gps_is_not_image_origin_evidence(make_import_dialog):
    # A stored (possibly cloud-materialised) image may carry GPS Sporely wrote.
    dialog = make_import_dialog({"lat": 59.9139, "lon": 10.7522, "image_id": 7})
    assert _pair(dialog.get_data()) is None


# --- Through Prepare Images itself: a real geotagged file goes through
# ImageImportDialog.add_images, and coordinates are typed into its widgets.

_PHOTO_EXIF = (59.9139, 10.7522)


def _jpeg(path, gps=None):
    from PIL import Image

    def _dms(value):
        minutes, seconds = divmod(abs(value) * 3600, 60)
        degrees, minutes = divmod(minutes, 60)
        return (degrees, minutes, round(seconds, 4))

    exif = Image.Exif()
    if gps is not None:
        exif[0x8825] = {1: "N", 2: _dms(gps[0]), 3: "E", 4: _dms(gps[1])}
    Image.new("RGB", (64, 48), "green").save(path, exif=exif)
    return str(path)


@pytest.fixture
def prepare_images(qapp, monkeypatch, tmp_path):
    """An offscreen Prepare Images dialog on a scratch database."""
    import database.schema as schema
    import ui.image_import_dialog as image_import_dialog

    images_dir = tmp_path / "images"
    monkeypatch.setattr(schema, "get_app_settings", lambda: {})
    monkeypatch.setattr(schema, "get_database_path", lambda: tmp_path / "mushrooms.db")
    monkeypatch.setattr(schema, "get_images_dir", lambda: images_dir)
    monkeypatch.setattr(schema, "init_reference_database", lambda *a, **kw: None)
    monkeypatch.setattr(image_import_dialog, "get_images_dir", lambda: images_dir)
    schema.init_database()
    made = []

    def _open(**kwargs):
        dialog = image_import_dialog.ImageImportDialog(**kwargs)
        made.append(dialog)
        return dialog

    yield _open
    for dialog in made:
        dialog.deleteLater()
    qapp.processEvents()


def _type_coordinates(prepare, lat, lon):
    prepare.lat_input.setValue(lat)
    prepare.lon_input.setValue(lon)


def test_coordinates_typed_in_prepare_images_are_manual(prepare_images, open_details, tmp_path):
    prepare = prepare_images()
    prepare.add_images([_jpeg(tmp_path / "geotagged.jpg", _PHOTO_EXIF)])
    assert (prepare.lat_input.value(), prepare.lon_input.value()) == pytest.approx(_PHOTO_EXIF)
    _type_coordinates(prepare, 60.5, 11.25)

    data = open_details(prepare.import_results).get_data()
    assert (data["gps_latitude"], data["gps_longitude"]) == pytest.approx((60.5, 11.25))
    assert _pair(data) == ("manual", "acquired")


def test_photo_exif_restored_in_details_after_typing_in_prepare_images_is_image_exif(
    prepare_images, open_details, tmp_path,
):
    prepare = prepare_images()
    prepare.add_images([_jpeg(tmp_path / "geotagged.jpg", _PHOTO_EXIF)])
    _type_coordinates(prepare, 60.5, 11.25)

    details = open_details(prepare.import_results)
    details.lat_input.setValue(_PHOTO_EXIF[0])
    details.lon_input.setValue(_PHOTO_EXIF[1])
    assert _pair(details.get_data()) == ("image_exif", "acquired")
    details.lat_input.setValue(61.0)
    assert _pair(details.get_data()) == ("manual", "acquired")


def test_only_changing_the_date_in_prepare_images_does_not_attribute(prepare_images, open_details, tmp_path):
    # A downloaded (cloud-materialised) photo whose coordinates the user left
    # alone: editing other metadata is not a coordinate entry.
    from ui.image_import_dialog import ImageImportResult

    downloaded = _jpeg(tmp_path / "downloaded.jpg", _PHOTO_EXIF)
    prepare = prepare_images(import_results=[ImageImportResult(filepath=downloaded)])
    prepare._select_image(0)
    prepare.datetime_input.setDateTime(prepare.datetime_input.dateTime().addDays(-1))
    assert prepare.import_results[0].typed_gps_latitude is None

    assert _pair(open_details(prepare.import_results).get_data()) is None


def test_coordinates_restored_in_prepare_images_to_photo_exif_are_image_exif(
    prepare_images, open_details, tmp_path,
):
    prepare = prepare_images()
    prepare.add_images([_jpeg(tmp_path / "geotagged.jpg", _PHOTO_EXIF)])
    _type_coordinates(prepare, 60.5, 11.25)
    _type_coordinates(prepare, *_PHOTO_EXIF)

    data = open_details(prepare.import_results).get_data()
    assert (data["gps_latitude"], data["gps_longitude"]) == pytest.approx(_PHOTO_EXIF)
    assert _pair(data) == ("image_exif", "acquired")


def test_coordinates_typed_for_photo_without_exif_gps_are_not_image_exif(
    prepare_images, open_details, tmp_path,
):
    prepare = prepare_images()
    prepare.add_images([_jpeg(tmp_path / "no-gps.jpg")])
    _type_coordinates(prepare, *_PHOTO_EXIF)

    data = open_details(prepare.import_results).get_data()
    assert (data["gps_latitude"], data["gps_longitude"]) == pytest.approx(_PHOTO_EXIF)
    assert _pair(data) == ("manual", "acquired")


def test_exif_read_from_a_non_session_file_is_not_image_origin_evidence(
    prepare_images, open_details, tmp_path,
):
    # Cloud-materialised rows reach Prepare Images as results, not import
    # candidates (no stored image row yet); their file's GPS may be Sporely's.
    from ui.image_import_dialog import ImageImportResult

    downloaded = _jpeg(tmp_path / "downloaded.jpg", _PHOTO_EXIF)
    prepare = prepare_images(import_results=[ImageImportResult(filepath=downloaded)])
    result = prepare.import_results[0]
    assert result.exif_has_gps and (result.gps_latitude, result.gps_longitude) == pytest.approx(_PHOTO_EXIF)

    assert _pair(open_details(prepare.import_results).get_data()) is None
