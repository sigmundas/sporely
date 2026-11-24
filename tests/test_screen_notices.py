"""Failures, results and refusals on the screen shown (stage 10f1).

What Vis, Mål, Analyse and Bilder said in message boxes is a notice in the
bottom bar of the screen shown (``MainWindow.show_screen_notice``); errors keep
the error under Details. Mål's bar is there only while a notice is (owner).
Export image and the statistics buttons are disabled without something to
export, saying why. "Update available" is a notice whose action opens the
download page. Change microscope settings says its failures beside its buttons.

Real ``MainWindow`` and dialogs on an isolated profile; no network.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QDialog, QMessageBox

import ui.main_window as main_window
from tests.test_microscope_settings_batch import _objectives, _seed, _window, profile  # noqa: F401


@pytest.fixture
def no_message_boxes(monkeypatch):
    for kind in ("information", "warning", "critical"):
        monkeypatch.setattr(QMessageBox, kind, lambda *a, **k: pytest.fail("no message box"))


@pytest.fixture
def mal(qtbot, profile, no_message_boxes):  # noqa: F811
    observation_id, images, field = _seed(profile)
    window = _window(qtbot, observation_id, images[0])
    window.ids_for_test = {"observation": observation_id, "images": images, "field": field}
    return window


def _notice(controller):
    return controller._notice


# --- Mål's bar ------------------------------------------------------------------


def test_mals_bar_shows_only_while_a_notice_does(qtbot, mal):
    bar = mal.measure_hint_bar
    assert mal.tab_widget.currentWidget() is mal.measure_tab
    assert not bar.isVisible()
    mal.show_screen_notice("Could not open IMG_12.ORF.", details="rawpy is not installed")
    assert bar.isVisible() and mal._screen_notice_controller() is mal._measure_hint_controller
    assert bar._details_btn.isVisible()
    qtbot.mouseClick(bar._dismiss_btn, Qt.LeftButton)
    assert not bar.isVisible()
    mal.show_screen_notice("Changed 2 image(s).", "success", timeout_ms=50)
    assert bar.isVisible()
    qtbot.waitUntil(lambda: not bar.isVisible(), timeout=2000)  # it goes with its notice


def test_an_image_that_cannot_be_prepared_is_a_notice_on_mal(mal, monkeypatch):
    from utils.local_image_ingest import RawRenderingUnavailableError

    def unavailable(*_a, **_k):
        raise RawRenderingUnavailableError("rawpy is not installed")

    monkeypatch.setattr(main_window, "prepare_local_ingest_image", unavailable)
    image = main_window.ImageDB.get_image(mal.ids_for_test["images"][1])
    mal.load_image_record(image)
    notice = _notice(mal._measure_hint_controller)
    assert notice.tone == "error" and notice.text.startswith("Could not open ") and notice.text.endswith(".")
    assert notice.details == "rawpy is not installed"
    assert mal.measure_hint_bar.isVisible()


def test_a_delete_that_fails_is_a_notice_on_mal(mal, monkeypatch):
    def locked(_image_id):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(main_window.ImageDB, "delete_image", locked)
    monkeypatch.setattr(main_window, "ask_measurements_exist_delete", lambda *_a, **_k: True)
    mal._on_measure_gallery_delete_requested(mal.ids_for_test["images"][1])
    notice = _notice(mal._measure_hint_controller)
    assert notice.tone == "error" and notice.text == "Could not delete the image."
    assert notice.details == "database is locked"


def test_export_image_needs_an_image(mal):
    button = mal.measure_export_image_btn
    assert button.isEnabled() and button.toolTip() == ""
    mal.clear_current_image_display()
    assert not button.isEnabled() and button.toolTip() == "Open an image to export it."
    mal.load_image_record(main_window.ImageDB.get_image(mal.ids_for_test["images"][0]))
    assert button.isEnabled()


def test_change_microscope_settings_says_what_changed_on_mal(qtbot, mal, monkeypatch):
    """The result went to a hint controller MainWindow does not have: nothing showed."""
    from ui.microscope_settings_dialog import MicroscopeSettingsDialog

    monkeypatch.setattr(main_window.MainWindow, "load_objective_definitions", lambda self: _objectives())

    def drive(dialog):
        dialog.field_combos["contrast"].setCurrentIndex(dialog.field_combos["contrast"].findData("BF"))
        qtbot.mouseClick(dialog.apply_btn, Qt.LeftButton)
        return dialog.result()

    monkeypatch.setattr(MicroscopeSettingsDialog, "exec", drive)
    mal._on_measure_gallery_change_settings_requested(mal.ids_for_test["images"])
    notice = _notice(mal._measure_hint_controller)
    assert notice.tone == "success" and notice.text == "Changed 2 image(s)."


# --- The microscope settings dialog's own bar -------------------------------


def test_the_microscope_dialog_says_failures_beside_its_buttons(qtbot, mal, monkeypatch):
    from database.models import MicroscopeSettingsChanged, MicroscopeSettingsDB
    from ui.microscope_settings_dialog import MicroscopeSettingsDialog

    dialog = MicroscopeSettingsDialog(mal, mal.ids_for_test["images"], objectives=_objectives())
    qtbot.addWidget(dialog)
    dialog.show()
    dialog.field_combos["contrast"].setCurrentIndex(dialog.field_combos["contrast"].findData("BF"))

    def changed(*_a, **_k):
        raise MicroscopeSettingsChanged("changed")

    monkeypatch.setattr(MicroscopeSettingsDB, "apply", changed)
    qtbot.mouseClick(dialog.apply_btn, Qt.LeftButton)
    assert dialog.isVisible() and dialog.result() != QDialog.Accepted
    notice = _notice(dialog._hint_controller)
    assert notice.tone == "warning" and "changed while this window was open" in notice.text

    def broken(*_a, **_k):
        raise RuntimeError("disk I/O error")

    monkeypatch.setattr(MicroscopeSettingsDB, "apply", broken)
    qtbot.mouseClick(dialog.apply_btn, Qt.LeftButton)
    notice = _notice(dialog._hint_controller)
    assert (notice.tone, notice.text, notice.details) == ("error", "Nothing was changed.", "disk I/O error")
    assert dialog.isVisible()


# --- Analyse -------------------------------------------------------------------


def test_the_statistics_buttons_need_an_observation(qtbot, mal):
    copy, save = mal.gallery_copy_stats_btn, mal.gallery_save_stats_btn
    mal.tab_widget.setCurrentWidget(mal.analysis_tab)
    mal._update_gallery_stats_preview()
    assert copy.isEnabled() and save.isEnabled()
    mal._build_stats_text = lambda: ("", "", "")  # no observation's statistics
    mal._update_gallery_stats_preview()
    assert not copy.isEnabled() and not save.isEnabled()
    assert copy.property("_hint_disabled_text") == "Select an observation first."
    assert save.property("_hint_disabled_text") == "Select an observation first."


def test_statistics_that_cannot_be_saved_are_a_notice_on_analysis(qtbot, mal, monkeypatch, tmp_path):
    mal.tab_widget.setCurrentWidget(mal.analysis_tab)
    target = tmp_path / "no-such-folder" / "stats.txt"
    monkeypatch.setattr(main_window.QFileDialog, "getSaveFileName", lambda *_a, **_k: (str(target), ""))
    mal.save_spore_stats()
    notice = _notice(mal._gallery_hint_controller)
    assert notice.tone == "error" and notice.text == "Could not save the statistics."
    assert "no-such-folder" in notice.details
    assert not mal.measure_hint_bar.isVisible()  # said on Analysis, not Mål


def test_the_plot_export_is_said_on_analysis(qtbot, mal, monkeypatch, tmp_path):
    mal.tab_widget.setCurrentWidget(mal.analysis_tab)

    class _Accept:
        def __init__(self, *a, **k):
            pass

        def exec(self):
            return QDialog.Accepted

        def get_settings(self):
            return {"format": "png16", "theme": "light", "quality": 90}

    monkeypatch.setattr(main_window, "ExportPlotDialog", _Accept)
    target = tmp_path / "plot.png"
    monkeypatch.setattr(main_window.QFileDialog, "getSaveFileName", lambda *_a, **_k: (str(target), ""))
    mal.export_graph_plot_svg()
    notice = _notice(mal._gallery_hint_controller)
    assert (notice.tone, notice.text) == ("success", "Plot exported to plot.png.")
    assert target.exists()

    monkeypatch.setattr(
        main_window.QFileDialog, "getSaveFileName",
        lambda *_a, **_k: (str(tmp_path / "missing" / "plot.png"), ""),
    )
    mal.export_graph_plot_svg()
    notice = _notice(mal._gallery_hint_controller)
    assert notice.tone == "error" and notice.text == "Could not export the plot." and notice.details


# --- The screen shown -------------------------------------------------------------


def test_each_screen_says_it_in_its_own_bar(qtbot, mal):
    expected = {
        mal.observations_tab: mal.observations_tab._status_hint_controller,
        mal.measure_tab: mal._measure_hint_controller,
        mal.analysis_tab: mal._gallery_hint_controller,
        mal.ingestion_hub_tab: mal.ingestion_hub_tab._hint_controller,
    }
    for page, controller in expected.items():
        mal.tab_widget.setCurrentWidget(page)
        assert mal._screen_notice_controller() is controller
    live = mal.live_lab_tab
    mal.tab_widget.setCurrentWidget(live)
    live.show_source(live.SOURCE_LIVE)
    assert mal._screen_notice_controller() is live._hint_controller
    live.show_source(live.SOURCE_FILES)
    editor = mal.files_source_page.editor()
    assert editor is not None
    assert mal._screen_notice_controller() is editor._hint_controller
    mal.tab_widget.setCurrentWidget(mal.edit_page)
    qtbot.waitUntil(lambda: mal.edit_page.editor() is not None, timeout=5000)
    assert mal._screen_notice_controller() is mal.edit_page.editor()._hint_controller


def test_update_available_is_a_notice_that_opens_the_download_page(qtbot, mal, monkeypatch):
    opened = []
    monkeypatch.setattr(main_window.QDesktopServices, "openUrl", lambda url: opened.append(url.toString()))
    mal.tab_widget.setCurrentWidget(mal.observations_tab)
    mal.app_version = "0.9.28"
    mal._show_update_dialog("0.9.30", "https://sporely.no/download")
    controller = mal.observations_tab._status_hint_controller
    notice = _notice(controller)
    assert notice.tone == "info" and notice.timeout_ms == 0  # stays until dismissed
    assert notice.text == "Sporely 0.9.30 is available (you have 0.9.28)."
    bar = mal.observations_tab.status_label
    assert bar._undo_btn.isVisible() and bar._undo_btn.text() == "Open download page"
    qtbot.mouseClick(bar._undo_btn, Qt.LeftButton)
    assert opened == ["https://sporely.no/download"]
    assert not controller.has_notice()


def test_missing_files_in_filer_are_a_notice_with_the_names_under_details(qtbot, mal):
    live = mal.live_lab_tab
    mal.tab_widget.setCurrentWidget(live)
    live.show_source(live.SOURCE_FILES)
    editor = mal.files_source_page.editor()
    editor._missing_exif_paths = {"/gone/IMG_1.JPG", "/gone/IMG_2.ORF"}
    editor._show_missing_exif_warning()
    notice = _notice(editor._hint_controller)
    assert notice.tone == "warning" and "missing or were moved" in notice.text
    assert notice.details == "Missing files:\n- IMG_1.JPG\n- IMG_2.ORF"


def test_the_microscope_dialogs_bar_shows_only_with_a_notice(qtbot, mal):
    from ui.microscope_settings_dialog import MicroscopeSettingsDialog

    dialog = MicroscopeSettingsDialog(mal, mal.ids_for_test["images"], objectives=_objectives())
    qtbot.addWidget(dialog)
    dialog.show()
    assert not dialog.hint_bar.isVisible()
    dialog._hint_controller.show_notice("Nothing was changed.", "error", details="x")
    assert dialog.hint_bar.isVisible()
    qtbot.mouseClick(dialog.hint_bar._dismiss_btn, Qt.LeftButton)
    assert not dialog.hint_bar.isVisible()


@pytest.mark.parametrize("screen", ["files", "edit"])
def test_a_screen_without_a_bar_keeps_the_notice_for_the_next_one(qtbot, mal, monkeypatch, screen):
    """Codex: Filer without an editor and Rediger without a form have no bar;
    the notice went to a hidden one."""
    live = mal.live_lab_tab
    if screen == "files":
        mal.tab_widget.setCurrentWidget(live)
        live.show_source(live.SOURCE_FILES)
        monkeypatch.setattr(mal.files_source_page, "editor", lambda: None)
    else:
        mal.tab_widget.setCurrentWidget(mal.edit_page)
        monkeypatch.setattr(mal.edit_page, "editor", lambda: None)
    assert mal._screen_notice_controller() is None
    mal._show_update_dialog("0.9.30", "https://sporely.no/download")
    vis = mal.observations_tab._status_hint_controller
    assert not vis.has_notice() and not live._hint_controller.has_notice()
    if screen == "files":
        live.show_source(live.SOURCE_LIVE)  # Live's bar shows
        assert "0.9.30" in live._hint_controller.notice_text()
    else:
        mal.tab_widget.setCurrentWidget(mal.observations_tab)
        assert "0.9.30" in vis.notice_text()
    assert mal._pending_screen_notice is None


def test_a_notice_on_mal_waits_while_another_screen_shows(qtbot, mal):
    """Codex: the countdown ran while Mål's tab was hidden; the notice went unseen."""
    mal.show_screen_notice("Changed 2 image(s).", "success", timeout_ms=150)
    mal.tab_widget.setCurrentWidget(mal.observations_tab)
    qtbot.wait(400)
    assert mal._measure_hint_controller.has_notice()
    mal.tab_widget.setCurrentWidget(mal.measure_tab)
    assert mal.measure_hint_bar.isVisible()
    qtbot.waitUntil(lambda: not mal._measure_hint_controller.has_notice(), timeout=2000)
    assert not mal.measure_hint_bar.isVisible()
    # Posted while Mål is not shown: it waits too.
    mal.tab_widget.setCurrentWidget(mal.observations_tab)
    mal._measure_hint_controller.show_notice("Changed 1 image(s).", "success", timeout_ms=150)
    qtbot.wait(400)
    assert mal._measure_hint_controller.has_notice()
