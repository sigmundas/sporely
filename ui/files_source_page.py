"""Bilder › Files and SD card (redesign stage 9f, first step).

Today's Prepare Images editor (``ImageImportDialog``), in place as a page in
Bilder instead of a window, for the observation Bilder shows. It edits the
observation's images and adds new ones; “Save to observation” saves through
the same path as the dialog (``ObservationsTab.save_prepared_images``) and
“Discard changes” drops them. Unsaved work stays with its observation:
switching observation keeps that observation's editor (owner, 2026-11-10:
staged images wait for the observation they were taken for), and its new
images show as unsaved in the shared strip and the observation list.

Stage 9f2 put the per-image controls on the image. Stage 9f3: the shared
strip (View, Measure and Live) replaces the editor's own gallery while Bilder
shows Files; its clicks, selection, removal and reordering go to the editor
shown, which renders its images there, new ones first.

Stage 9g4b: Ny observasjon starts here, for a not-yet-saved observation
(owner 2026-11-12). Nothing is written before its Save, which creates it as
Utkast with the photos' date and GPS and the New observation form's defaults
(Deling, Posisjon), then Rediger opens. Bilder keeps the new observation on
screen while the observation it showed stays; choosing another observation
leaves it, and its photos are kept until saved or discarded (New shows them
again, quitting asks).

Stage 9i: “From SD card (new photos)” beside “Choose files…” adds the photos
on the camera's card that are not in Sporely yet and were taken on the
observation's day (for Ny observasjon: the newest day on the card); see
``utils.card_photos`` (owner 2026-11-12).
"""
from __future__ import annotations

from PySide6.QtCore import QTimer, Qt, Signal
from PySide6.QtWidgets import (
    QApplication, QDialog, QFileDialog, QFrame, QHBoxLayout, QInputDialog, QLabel, QMessageBox, QPushButton,
    QScrollArea, QStackedWidget, QVBoxLayout, QWidget,
)

from database.models import ImageDB, ObservationDB
from .styles import STAGING_BORDER
from .image_import_dialog import ImageImportDialog


class FilesSourcePage(QWidget):
    """One in-place editor per observation with unsaved work; one shown."""

    # (observation id, unsaved new images as strip items)
    unsavedChanged = Signal(int, object)
    # Ny observasjon was saved: the new observation's id (Rediger opens it).
    newObservationCreated = Signal(int)
    # Ny observasjon's editor came on screen (True) or went (False).
    newShownChanged = Signal(bool)

    NEW = -1  # the key of Ny observasjon's editor: there is no observation yet

    def __init__(self, observations_tab, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("filesSourcePage")
        self._observations_tab = observations_tab
        self._editors: dict[int, ImageImportDialog] = {}
        # Each editor sits in a scroll area: its minimum size (it was a large
        # dialog) must not raise the main window's.
        self._frames: dict[int, QScrollArea] = {}
        self._context: dict[int, tuple[dict, list[dict]]] = {}
        self._observation_id: int | None = None
        # Ny observasjon (stage 9g4b): the observation Bilder showed when it
        # started; re-showing that one keeps the new observation on screen.
        self._new_anchor: int | None = None
        self._new_data: tuple[dict, dict | None, str | None] | None = None
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        self._stack = QStackedWidget(self)
        layout.addWidget(self._stack, 1)
        # Saving stays in view while the editor scrolls (as Live's row does).
        self.save_row = QWidget(self)
        row = QHBoxLayout(self.save_row)
        row.setContentsMargins(0, 6, 0, 0)
        row.setSpacing(8)
        self.unsaved_label = QLabel(self.save_row)
        self.unsaved_label.setStyleSheet(f"font-weight: 600; color: {STAGING_BORDER};")
        row.addWidget(self.unsaved_label)
        row.addStretch(1)
        self.discard_btn = QPushButton(self.tr("Discard changes"), self.save_row)
        self.discard_btn.setObjectName("outlineButton")
        self.discard_btn.clicked.connect(self._discard)
        row.addWidget(self.discard_btn)
        self.save_btn = QPushButton(self.tr("Save to observation"), self.save_row)
        self.save_btn.clicked.connect(self._save)
        row.addWidget(self.save_btn)
        self.save_row.setVisible(False)
        layout.addWidget(self.save_row)
        # The shared strip (stage 9f3): it takes the shown editor's gallery place.
        self._strip = None
        self._holds_strip = False
        self._strip_editor: ImageImportDialog | None = None
        self._busy = False
        # Not every edit in the editor reports itself (removing, cropping,
        # reordering): look for unsaved work while the page is on screen.
        self._watch = QTimer(self)
        self._watch.setInterval(700)
        self._watch.timeout.connect(self._refresh_unsaved_state)
        self._last_unsaved: dict[int, tuple] = {}
        # “Choose files…” sits beside Bilder's source switch (stage 9f2).
        self.choose_files_btn = QPushButton(self.tr("Choose files…"), self)
        self.choose_files_btn.setObjectName("outlineButton")
        self.choose_files_btn.clicked.connect(self._choose_files)
        self.choose_files_btn.setEnabled(False)
        self.sd_card_btn = QPushButton(self.tr("From SD card (new photos)"), self)
        self.sd_card_btn.setObjectName("outlineButton")
        self.sd_card_btn.clicked.connect(self._from_sd_card)
        self.sd_card_btn.setEnabled(False)
        self._empty = QLabel(self.tr("Choose an observation to add files and photos from an SD card to it."), self)
        self._empty.setAlignment(Qt.AlignCenter)
        self._empty.setWordWrap(True)
        self._stack.addWidget(self._empty)

    # -- which observation ----------------------------------------------------

    def observation_id(self) -> int | None:
        """The observation shown; None for none and for Ny observasjon."""
        return None if self._observation_id == self.NEW else self._observation_id

    def is_showing_new(self) -> bool:
        return self._observation_id == self.NEW

    def editor(self) -> ImageImportDialog | None:
        return self._editors.get(self._observation_id) if self._observation_id else None

    def showEvent(self, event) -> None:  # noqa: N802 - Qt override
        super().showEvent(event)
        self._watch.start()
        editor = self.editor()
        if editor is not None and not editor.has_unsaved_changes() and not self._busy and not self.is_showing_new():
            # Shown again with nothing unsaved: start from what is saved now.
            observation_id = self._observation_id
            self._drop_editor(observation_id)
            self._observation_id = None
            self.show_observation(observation_id)

    def hideEvent(self, event) -> None:  # noqa: N802 - Qt override
        self._watch.stop()
        super().hideEvent(event)

    def show_observation(self, observation_id: int | None) -> None:
        try:
            observation_id = int(observation_id or 0) or None
        except (TypeError, ValueError):
            observation_id = None
        if self.is_showing_new():
            if observation_id == self._new_anchor:
                return  # Bilder showing its observation again: the new one stays on screen
            self._leave_new()
        elif observation_id == self._observation_id and (observation_id is None or observation_id in self._editors):
            return
        previous = self._observation_id
        if previous is not None and previous != observation_id:
            editor = self._editors.get(previous)
            if editor is not None and not editor.has_unsaved_changes() and not self._busy:
                self._drop_editor(previous)  # nothing to keep: free it
        self._observation_id = observation_id
        if observation_id is None or ObservationDB.get_observation(observation_id) is None:
            self._observation_id = None
            self._stack.setCurrentWidget(self._empty)
            self._update_save_row()
            self._sync_strip()
            return
        if observation_id not in self._editors:
            self._create_editor(observation_id)
        self._stack.setCurrentWidget(self._frames[observation_id])
        self._update_save_row()
        self._sync_strip()

    # -- editors ------------------------------------------------------------------

    def _create_editor(self, observation_id: int) -> ImageImportDialog:
        from .observations_tab import _parse_observation_datetime

        observation = ObservationDB.get_observation(observation_id) or {}
        existing_images = ImageDB.get_images_for_observation(observation_id)
        results = self._observations_tab._build_import_results_from_images(existing_images)
        editor = ImageImportDialog(
            self,
            import_results=results,
            observation_datetime=_parse_observation_datetime(observation.get("date")),
            observation_lat=observation.get("gps_latitude"),
            observation_lon=observation.get("gps_longitude"),
            continue_to_observation_details=False,
        )
        if existing_images and existing_images[0].get("filepath"):
            editor.select_image_by_path(existing_images[0]["filepath"])
        editor.set_in_place()  # after the first image loads: the baseline for “unsaved”
        self._context[observation_id] = (observation, existing_images)
        return self._install_editor(observation_id, editor)

    def _install_editor(self, key: int, editor: ImageImportDialog) -> ImageImportDialog:
        editor.finished.connect(lambda result, obs=key, ed=editor: self._on_editor_finished(obs, ed, result))
        editor.unsavedChanged.connect(lambda obs=key, ed=editor: self._mirror_unsaved(obs, ed))
        self._editors[key] = editor
        frame = QScrollArea(self)
        frame.setWidgetResizable(True)
        frame.setFrameShape(QFrame.NoFrame)
        frame.setWidget(editor)
        self._frames[key] = frame
        self._stack.addWidget(frame)
        return editor

    # -- Ny observasjon (stage 9g4b) --------------------------------------------

    def start_new_observation(self, paths: list[str] | None = None, *, anchor: int | None = None) -> bool:
        """Show Ny observasjon's editor, with ``paths`` added (dropped files).

        ``anchor`` is the observation Bilder shows now: it stays the active
        one, and showing it again keeps the new observation on screen. Photos
        kept from an earlier start that was left come back.
        """
        if self._busy:
            return False
        try:
            self._new_anchor = int(anchor or 0) or None
        except (TypeError, ValueError):
            self._new_anchor = None
        previous = self._observation_id
        if previous is not None and previous != self.NEW:
            editor = self._editors.get(previous)
            if editor is not None and not editor.has_unsaved_changes():
                self._drop_editor(previous)  # nothing to keep: free it
        editor = self._editors.get(self.NEW)
        if editor is None:
            editor = ImageImportDialog(self, continue_to_observation_details=False)
            editor.set_in_place()  # nothing saved: every photo added is new
            self._install_editor(self.NEW, editor)
        self._observation_id = self.NEW
        self._stack.setCurrentWidget(self._frames[self.NEW])
        if paths:
            editor.add_images(list(paths))
        self._update_save_row()
        self._sync_strip()
        self.newShownChanged.emit(True)
        return True

    def leave_new_observation(self) -> None:
        """An explicit choice of what Bilder shows (Edit photo): leave Ny observasjon."""
        if self.is_showing_new():
            self._leave_new()

    def observation_chosen(self, observation_id) -> None:
        """Another observation became the active one: Ny observasjon leaves the screen.

        Also while a Live session keeps Bilder on its own observation (the
        anchor), so choosing any other observation always leaves it.
        """
        try:
            observation_id = int(observation_id or 0) or None
        except (TypeError, ValueError):
            observation_id = None
        if self.is_showing_new() and observation_id != self._new_anchor:
            self._leave_new()

    def _leave_new(self) -> None:
        """Another observation is shown: the new one's photos wait, or it goes."""
        editor = self._editors.get(self.NEW)
        self._observation_id = None
        self._stack.setCurrentWidget(self._empty)  # until Bilder shows its observation
        self._update_save_row()
        self._sync_strip()
        self.newShownChanged.emit(False)
        if editor is None:
            return
        if editor.has_unsaved_changes():
            set_status = getattr(self._observations_tab, "set_status_message", None)
            if callable(set_status):
                set_status(
                    self.tr("The new observation's photos are kept. New shows them again."),
                    level="info",
                    auto_clear_ms=8000,
                )
        elif not self._busy:
            self._drop_editor(self.NEW)

    def _finish_new(self, editor: ImageImportDialog, result: int) -> None:
        """Ny observasjon's editor closed: Save creates the observation, Discard drops it."""
        prepared, self._new_data = self._new_data, None
        created = None
        try:
            if result == QDialog.Accepted and prepared is not None:
                obs_data, ai_state, _precision = prepared  # a new observation records none (as its dialog)
                created = self._observations_tab.create_observation_from_prepared(
                    obs_data, ai_state, list(editor.import_results),
                )
        finally:
            # Whatever was written (a failure part way included), these photos
            # are never saved a second time: the editor goes.
            self._close_new(created)
        if created is not None:
            self.newObservationCreated.emit(int(created))

    def _close_new(self, created: int | None) -> None:
        showing = self.is_showing_new()
        self._drop_editor(self.NEW)
        if showing:
            self._observation_id = None
            self.newShownChanged.emit(False)
            self._stack.setCurrentWidget(self._empty)  # Saved: Rediger opens the new one
            if created is None:
                self.show_observation(self._new_anchor)  # Discarded: the observation Bilder shows
        self._new_anchor = None
        self._update_save_row()
        self._sync_strip()

    def _drop_editor(self, observation_id: int) -> None:
        editor = self._editors.pop(observation_id, None)
        self._context.pop(observation_id, None)
        frame = self._frames.pop(observation_id, None)
        if editor is None:
            return
        if editor is self._strip_editor:
            editor.detach_strip()  # nothing it does while closing reaches the strip
            self._strip_editor = None
        editor.close()  # its closeEvent cleans threads and temporary previews
        if frame is not None:
            self._stack.removeWidget(frame)
            frame.deleteLater()  # and the editor with it
        if observation_id != self.NEW:
            self.unsavedChanged.emit(int(observation_id), [])

    def _on_editor_finished(self, observation_id: int, editor: ImageImportDialog, result: int) -> None:
        if self._editors.get(observation_id) is not editor:
            return
        if observation_id == self.NEW:
            self._finish_new(editor, result)
            return
        if result == QDialog.Accepted:
            self._save_editor(observation_id, editor)
        self._drop_editor(observation_id)
        if observation_id == self._observation_id:
            # A fresh editor on what is saved now (or was, after Discard).
            self._create_editor(observation_id)
            self._stack.setCurrentWidget(self._frames[observation_id])
        self._update_save_row()
        self._sync_strip()

    # -- the shared strip (stage 9f3) -----------------------------------------

    def use_shared_strip(self, strip) -> None:
        """The main window's strip (shared with View, Measure and Live)."""
        self._strip = strip
        channel = strip.channel("files", accepts_staged=True)
        channel.imageClicked.connect(lambda key, path: self._to_editor("_on_gallery_clicked", key, path))
        channel.editRequested.connect(lambda key, path: self._to_editor("_on_gallery_clicked", key, path))
        channel.selectionChanged.connect(lambda paths: self._to_editor("_on_gallery_selection_changed", paths))
        channel.deleteImagesRequested.connect(lambda keys: self._to_editor("_on_gallery_delete_images_requested", keys))
        channel.itemsReordered.connect(lambda keys: self._to_editor("_on_gallery_items_reordered", keys))

    def holds_shared_strip(self) -> bool:
        return self._holds_strip

    def claim_shared_strip(self) -> None:
        """Bilder shows Files: the strip shows the editor's images."""
        strip = self._strip
        if strip is None:
            return
        strip.set_active_channel("files")
        strip.set_context_actions(
            change_settings=False, move_to_observation=False, delete_cloud_copy=False, publish_checkbox=False,
        )
        strip.set_page_behaviour(
            toggle_on_plain_click=False, clear_on_background_click=False, reveal_mode="nudge", reorderable=True,
        )
        self._holds_strip = True
        self._strip_editor = None
        self._sync_strip()

    def release_shared_strip(self) -> None:
        """Another page takes the strip: the editor goes back to its own gallery."""
        if not self._holds_strip:
            return
        self._holds_strip = False
        editor, self._strip_editor = self._strip_editor, None
        if editor is not None:
            editor.detach_strip()
        if self._strip is not None:
            self._strip.select_image(None, center=False)  # leave no selection behind

    def _sync_strip(self) -> None:
        """While Files holds the strip it shows the editor on screen, or nothing."""
        if not self._holds_strip or self._strip is None:
            return
        editor = self.editor()
        if editor is self._strip_editor:
            return
        if self._strip_editor is not None:
            self._strip_editor.detach_strip()
        self._strip_editor = editor
        if editor is None:
            self._strip.clear()  # and out of sight: no editor holds it
            self._strip.hide()
        else:
            editor.attach_strip(self._strip)

    def _to_editor(self, handler: str, *args) -> None:
        editor = self._strip_editor
        if editor is None or not self._holds_strip or self._busy:
            return
        getattr(editor, handler)(*args)

    # -- unsaved images, for the shared strip and the observation list ---------

    def _save_editor(self, observation_id: int, editor: ImageImportDialog) -> None:
        """Save what this editor changed, nothing it only showed.

        The editor may have been open a long time: saved images it did not
        change are re-read from the database first, so newer edits made
        elsewhere (Edit photo, Live, sync) are not overwritten; images added
        elsewhere are not deleted; the GPS is written only if changed here.
        """
        _observation, snapshot = self._context.get(observation_id, ({}, []))
        fresh_rows = {int(row["id"]): row for row in ImageDB.get_images_for_observation(observation_id)}
        changed = editor.changed_existing_ids()
        tab = self._observations_tab
        results = []
        for result in editor.import_results:
            image_id = getattr(result, "image_id", None)
            if image_id is None or int(image_id) in changed:
                results.append(result)
            elif int(image_id) in fresh_rows:
                results.extend(tab._build_import_results_from_images([fresh_rows[int(image_id)]]))
            # else: deleted elsewhere and not edited here: stays deleted
        existing = [fresh_rows[int(row["id"])] for row in snapshot if int(row["id"]) in fresh_rows]
        tab.save_prepared_images(
            observation_id,
            ObservationDB.get_observation(observation_id) or {},
            existing,
            editor,
            image_results=results,
            write_gps=editor.gps_changed_since_baseline(),
            observation_date=editor.time_used_for_observation(),
            reselect=False,
        )
        host = tab.window()
        if int(getattr(host, "active_observation_id", 0) or 0) == int(observation_id):
            refresh = getattr(host, "refresh_observation_images", None)
            if callable(refresh):
                refresh(select_image_id=getattr(host, "current_image_id", None))
        if tab.selected_observation_ids() == [int(observation_id)]:
            tab.on_selection_changed()

    def header_actions(self) -> list[QWidget]:
        """Widgets for the row with Bilder's source switch, shown with this source."""
        return [self.choose_files_btn, self.sd_card_btn]

    def _choose_files(self) -> None:
        editor = self.editor()
        if editor is not None and not self._busy:
            editor.add_files()

    # -- From SD card (stage 9i) ---------------------------------------------------

    def _choose_card_folder(self):
        """The camera card to read: the one found, the one chosen of several, or a folder chosen instead."""
        from pathlib import Path

        from database.models import SettingsDB
        from utils import card_photos

        cards = card_photos.find_camera_cards()
        if len(cards) == 1:
            return cards[0]
        if len(cards) > 1:
            names = [str(card) for card in cards]
            name, ok = QInputDialog.getItem(
                self, self.tr("SD card"), self.tr("Several SD cards are connected. Read which one?"), names, 0, False,
            )
            return Path(name) if ok and name in names else None
        remembered = str(SettingsDB.get_setting(card_photos.SETTING_CARD_FOLDER, "") or "").strip()
        remembered_path = Path(remembered) if remembered and Path(remembered).is_dir() else None
        box = QMessageBox(self)
        box.setIcon(QMessageBox.Information)
        box.setWindowTitle(self.tr("SD card"))
        box.setText(self.tr("No SD card found (a drive with a DCIM folder). Read a folder instead?"))
        use_btn = None
        if remembered_path is not None:
            use_btn = box.addButton(self.tr("Use {name}").format(name=remembered_path.name or str(remembered_path)),
                                    QMessageBox.AcceptRole)
        choose_btn = box.addButton(self.tr("Choose folder…"), QMessageBox.ActionRole)
        box.addButton(QMessageBox.Cancel)
        box.exec()
        if use_btn is not None and box.clickedButton() is use_btn:
            return remembered_path
        if box.clickedButton() is not choose_btn:
            return None
        folder = QFileDialog.getExistingDirectory(
            self, self.tr("Choose the folder with the photos"), str(remembered_path or Path.home()),
        )
        if not folder:
            return None
        SettingsDB.set_setting(card_photos.SETTING_CARD_FOLDER, folder)
        return Path(folder)

    def _status(self, text: str, level: str = "info") -> None:
        set_status = getattr(self._observations_tab, "set_status_message", None)
        if callable(set_status):
            set_status(text, level=level, auto_clear_ms=10000)

    def _from_sd_card(self) -> None:
        """Add the card's photos not in Sporely yet, taken on the observation's day (owner 2026-11-12)."""
        editor = self.editor()
        if editor is None or self._busy:
            return
        folder = self._choose_card_folder()
        if folder is None or editor is not self.editor() or self._busy:
            return
        # Reading the card and adding its photos keep the event loop turning:
        # no second click meanwhile, and the editor stays (as while saving).
        self._busy = True
        self._set_row_enabled(False)
        try:
            self._add_card_photos(editor, folder)
        finally:
            self._busy = False
            self._set_row_enabled(True)
            self._update_save_row()

    def _add_card_photos(self, editor: ImageImportDialog, folder) -> None:
        from utils import card_photos

        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            rows = card_photos.new_photo_rows(folder)
        finally:
            QApplication.restoreOverrideCursor()
        if editor is not self.editor():
            return  # the page moved on while the card was read
        # The day first, from the photos not in Sporely: pressing again keeps
        # the same day (the editor's own photos are left out after).
        if self.is_showing_new():
            day = card_photos.newest_day(rows)
        else:
            day = card_photos.observation_day((ObservationDB.get_observation(self._observation_id) or {}).get("date"))
        # Not twice: photos added to this editor already (and not saved yet).
        in_editor = {
            str(path) for result in editor.import_results
            for path in (getattr(result, "filepath", None), getattr(result, "original_filepath", None),
                         getattr(result, "preview_path", None))
            if path
        } | {str(path) for path in getattr(editor, "image_paths", []) if path}
        rows = [row for row in rows if not set(card_photos.paths_for_row(row)) & in_editor]
        chosen = card_photos.rows_on_day(rows, day)
        if not chosen:
            if day is None:
                text = self.tr("No new photos on the SD card.") if not rows else self.tr(
                    "The observation has no date: no photos were added from the SD card."
                )
            else:
                others = len(rows) - len(chosen)
                text = self.tr("No new photos from {day} on the SD card ({count} new on other days).").format(
                    day=day.isoformat(), count=others,
                )
            self._status(text)
            return
        paths: list[str] = []
        for row in chosen:
            paths.extend(path for path in card_photos.paths_for_row(row) if path not in paths)
        editor.add_images(paths)
        self._status(
            self.tr("Added %n new photo(s) from the SD card, taken on {day}.", "", len(chosen)).format(day=day.isoformat()),
            level="success",
        )

    def _set_row_enabled(self, enabled: bool) -> None:
        self.save_btn.setEnabled(enabled)
        self.discard_btn.setEnabled(enabled)
        self.choose_files_btn.setEnabled(enabled and self.editor() is not None)
        self.sd_card_btn.setEnabled(enabled and self.editor() is not None)

    def _save(self) -> None:
        editor = self.editor()
        if editor is None or self._busy:
            return
        self._busy = True  # saving can process events (RAW renders): no second click meanwhile
        self._set_row_enabled(False)
        try:
            if self.is_showing_new():
                # The form's defaults, its publish notice and the duplicate
                # question first: cancelling writes nothing and keeps the photos.
                # The photo shown gives the date and GPS, as after the dialog's
                # Continue (“Use for the observation” sets them on it).
                shown = editor.primary_index
                chosen = editor.import_results[shown] if shown is not None and 0 <= shown < len(editor.import_results) else None
                results = editor.results_to_save()
                self._new_data = self._observations_tab.prepare_new_observation(
                    results,
                    primary_index=next((i for i, result in enumerate(results) if result is chosen), None),
                    suggested_taxon=editor.get_ai_selected_taxon(),
                )
                if self._new_data is None:
                    return
            editor.save_in_place()
        finally:
            self._busy = False
            self._set_row_enabled(True)
            self._update_save_row()

    def _discard(self) -> None:
        editor = self.editor()
        if editor is None or self._busy:
            return
        editor.reject()

    def _refresh_unsaved_state(self) -> None:
        editor = self.editor()
        if editor is None or self._busy:
            return
        state = (editor.has_unsaved_changes(), len(editor.unsaved_new_results()))
        if self._last_unsaved.get(self._observation_id) != state:
            self._last_unsaved[self._observation_id] = state
            self._mirror_unsaved(self._observation_id, editor)

    def _update_save_row(self) -> None:
        editor = self.editor()
        self.choose_files_btn.setEnabled(editor is not None and not self._busy)
        self.sd_card_btn.setEnabled(editor is not None and not self._busy)
        if editor is not None and self.is_showing_new():
            # A new observation can be saved without photos, as the dialog's could.
            self.save_row.setVisible(True)
            self.save_btn.setText(self.tr("Save new observation"))
            self.discard_btn.setText(self.tr("Discard"))
            count = len(editor.unsaved_new_results())
            self.unsaved_label.setText(
                self.tr("New observation, %n not saved", "", count) if count
                else self.tr("New observation, no photos yet")
            )
            return
        self.save_btn.setText(self.tr("Save to observation"))
        self.discard_btn.setText(self.tr("Discard changes"))
        unsaved = editor is not None and editor.has_unsaved_changes()
        self.save_row.setVisible(bool(unsaved))
        if not unsaved:
            return
        new = len(editor.unsaved_new_results())
        self.unsaved_label.setText(self.tr("%n not saved", "", new) if new else self.tr("Unsaved changes"))

    def _mirror_unsaved(self, observation_id: int, editor: ImageImportDialog) -> None:
        if self._editors.get(observation_id) is not editor:
            return
        if observation_id == self._observation_id:
            self._update_save_row()
        if observation_id == self.NEW:
            return  # no observation yet: nothing else shows its photos
        items = []
        for number, result in enumerate(editor.unsaved_new_results(), start=1):
            path = str(result.preview_path or result.filepath or "")
            if not path:
                continue
            items.append({"id": None, "filepath": path, "image_number": number, "badges": [self.tr("NEW")]})
        self.unsavedChanged.emit(int(observation_id), items)

    def unsaved_counts(self) -> dict[int, int]:
        return {
            observation_id: len(editor.unsaved_new_results())
            for observation_id, editor in self._editors.items()
            if observation_id != self.NEW and editor.unsaved_new_results()
        }

    def has_unsaved_work(self) -> bool:
        return any(editor.has_unsaved_changes() for editor in self._editors.values())

    def shutdown(self) -> None:
        self.release_shared_strip()
        for observation_id in list(self._editors):
            editor = self._editors.pop(observation_id)
            editor.close()
        self._context.clear()
        self._frames.clear()
