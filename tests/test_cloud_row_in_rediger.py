"""Cloud-only rows in Rediger (redesign stage 9g4c).

Real ``MainWindow`` in an isolated profile under ``tmp_path`` (the fixtures of
``tests/test_files_source.py``); no network: the cloud row's image download
and the cloud client are stand-ins (the precondition only). Edit on a
cloud-only row shows it in Rediger with “Import to this computer”; nothing is
written before the import, which writes only locally, with the cloud row's
sync key, linked to the cloud rows it came from (as the dialog's OK did);
then Rediger edits the local copy in place. The Rediger observasjon dialog
never opens.
"""
from __future__ import annotations

import os
import sqlite3
import uuid

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PIL import Image
from PySide6.QtCore import Qt

from database import schema
from database.models import ImageDB, ObservationDB
from tests.test_files_source import profile, window  # noqa: F401
from ui.image_import_dialog import ImageImportDialog
from ui.observation_details_dialog import ObservationDetailsDialog

KEY_OBS = str(uuid.UUID(int=4242))
KEY_IMG = str(uuid.UUID(int=4243))


@pytest.fixture(autouse=True)
def no_dialogs(monkeypatch):
    def fail(*_args, **_kwargs):
        raise AssertionError("a retired dialog opened")

    monkeypatch.setattr(ImageImportDialog, "exec", fail)
    monkeypatch.setattr(ObservationDetailsDialog, "exec", fail)


@pytest.fixture
def cloud_row(window, profile, monkeypatch):
    """A cloud-only row with one image; its download is a local file."""
    photo = profile.fixtures_dir / "cloud.jpg"
    Image.new("RGB", (64, 48), (90, 60, 30)).save(photo, format="JPEG")
    image_rows = [{"id": "cimg", "sync_key": KEY_IMG, "image_type": "field", "_local_path": str(photo)}]
    tab = window.observations_tab
    downloads = []
    monkeypatch.setattr(
        tab, "_download_cloud_observation_images",
        lambda row_data, download_all=True: downloads.append(row_data) or [dict(row) for row in image_rows],
    )
    monkeypatch.setattr(tab, "_window_cloud_client", lambda: None)
    row = {
        "row_kind": "cloud",
        "cloud_id": "cobs",
        "species_name": "Amanita muscaria",
        "date": "2026-09-14 10:30",
        "location": "Lade",
        "raw": {
            "id": "cobs", "sync_key": KEY_OBS, "date": "2026-09-14 10:30", "genus": "Amanita",
            "species": "muscaria", "location": "Lade", "is_draft": False, "sharing_scope": "friends",
            "gps_latitude": 63.4425, "gps_longitude": 10.4472, "open_comment": "Under bjørk",
        },
    }
    row["downloads"] = downloads
    return row


def _ids():
    return {int(r["id"]) for r in ObservationDB.get_all_observations()}


def _open(qtbot, window, row):
    window.tab_widget.setCurrentWidget(window.observations_tab)
    window.observations_tab._open_cloud_observation_editor(row)
    page = window.edit_page
    qtbot.waitUntil(lambda: page.is_showing_cloud(), timeout=3000)
    return page


def test_edit_on_a_cloud_only_row_shows_it_in_rediger_and_writes_nothing(qtbot, window, cloud_row, monkeypatch):
    first = window.ids_for_test["first"]
    before = _ids()
    tab = window.observations_tab
    # The table's single selected row is the cloud row (the precondition only).
    class _Selection:
        def selectedRows(self):
            return [type("Index", (), {"row": lambda self: 0})()]

    monkeypatch.setattr(tab.table, "selectionModel", lambda: _Selection())
    monkeypatch.setattr(tab, "_observation_id_for_row", lambda row: None)
    monkeypatch.setattr(tab, "_observation_row_data_for_row", lambda row: cloud_row)
    tab.edit_observation()
    page = window.edit_page
    qtbot.waitUntil(lambda: page.is_showing_cloud(), timeout=3000)
    assert window.tab_widget.currentWidget() is page
    assert page.editor() is None and page.observation_id() is None
    assert page.cloud_title_label.text() == "Amanita muscaria"
    assert "Lade" in page.cloud_details_label.text()
    assert page.cloud_import_btn.isVisible() and page.cloud_import_btn.text() == "Import to this computer"
    assert window.observation_header_label.text() == "Amanita muscaria · Sporely Cloud"
    assert int(window.active_observation_id) == first
    assert _ids() == before and cloud_row["downloads"] == []  # nothing downloaded or written yet


def test_import_writes_locally_linked_to_the_cloud_row_then_rediger_edits_it(qtbot, window, cloud_row):
    before = _ids()
    page = _open(qtbot, window, cloud_row)
    qtbot.mouseClick(page.cloud_import_btn, Qt.LeftButton)
    qtbot.waitUntil(lambda: len(_ids() - before) == 1, timeout=5000)
    (local,) = _ids() - before
    row = ObservationDB.get_observation(local)
    assert row["sync_key"] == KEY_OBS and row["cloud_id"] == "cobs" and row["sync_status"] == "synced"
    assert (row["genus"], row["species"], row["location"]) == ("Amanita", "muscaria", "Lade")
    assert not bool(row["is_draft"]) and row["sharing_scope"] == "friends"
    assert row["open_comment"] == "Under bjørk"
    with sqlite3.connect(schema.get_database_path()) as connection:
        images = connection.execute(
            "SELECT cloud_id, sync_key FROM images WHERE observation_id = ?", (local,),
        ).fetchall()
    assert images == [("cimg", KEY_IMG)]
    # Rediger now edits the local copy in place.
    qtbot.waitUntil(lambda: page.observation_id() == local and page.editor() is not None, timeout=8000)
    assert not page.is_showing_cloud()
    assert int(window.active_observation_id) == local
    assert window.observation_header_label.text().startswith("Amanita muscaria")


def test_a_cancelled_question_imports_nothing_and_keeps_the_button(qtbot, window, cloud_row, monkeypatch):
    monkeypatch.setattr(
        type(window.observations_tab), "_confirm_duplicate_observation_creation", lambda self, *a, **k: False,
    )
    before = _ids()
    page = _open(qtbot, window, cloud_row)
    qtbot.mouseClick(page.cloud_import_btn, Qt.LeftButton)
    qtbot.wait(100)
    assert _ids() == before
    assert page.is_showing_cloud() and page.cloud_import_btn.isEnabled()


def test_a_failure_part_way_never_offers_the_same_import_again(qtbot, window, cloud_row, monkeypatch):
    def fail(*_args, **_kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(window.observations_tab, "_apply_import_results_to_observation", fail)
    before = _ids()
    page = _open(qtbot, window, cloud_row)
    with qtbot.capture_exceptions() as raised:
        qtbot.mouseClick(page.cloud_import_btn, Qt.LeftButton)
    assert raised and "disk full" in str(raised[0][1])
    assert len(_ids() - before) == 1
    assert not page.is_showing_cloud()
    # Opening the keyed row again finds it on this computer (by its sync key): no
    # second copy. (A keyless row is linked only after its images: not covered.)
    window.observations_tab._open_cloud_observation_editor(cloud_row)
    assert not page.is_showing_cloud() and len(_ids() - before) == 1


def test_choosing_an_observation_leaves_the_cloud_row(qtbot, window, cloud_row):
    other = window.ids_for_test["other"]
    page = _open(qtbot, window, cloud_row)
    window.on_observation_selected(other, "Other", switch_tab=False)
    qtbot.waitUntil(lambda: page.observation_id() == other and page.editor() is not None, timeout=8000)
    assert not page.is_showing_cloud()
    assert window.observation_header_label.text() != "Amanita muscaria · Sporely Cloud"


def test_switching_pages_and_back_shows_the_active_observation(qtbot, window, cloud_row):
    """The cloud row is shown for Edit; Rediger shown again from the mode bar edits the active one."""
    first = window.ids_for_test["first"]
    page = _open(qtbot, window, cloud_row)
    window.tab_widget.setCurrentWidget(window.observations_tab)
    assert window.observation_header_label.text() != "Amanita muscaria · Sporely Cloud"
    window.tab_widget.setCurrentWidget(page)
    # The cloud row stays until another observation is chosen (the active one is its anchor).
    assert page.is_showing_cloud()
    assert window.observation_header_label.text() == "Amanita muscaria · Sporely Cloud"
    assert int(window.active_observation_id) == first


def test_edit_on_the_active_observation_leaves_the_cloud_row(qtbot, window, cloud_row):
    first = window.ids_for_test["first"]
    page = _open(qtbot, window, cloud_row)
    window.tab_widget.setCurrentWidget(window.observations_tab)
    assert window.open_edit_mode(first)
    qtbot.waitUntil(lambda: page.observation_id() == first and page.editor() is not None, timeout=8000)
    assert not page.is_showing_cloud()
    assert window.observation_header_label.text() != "Amanita muscaria · Sporely Cloud"
