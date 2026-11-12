"""Rediger: the observation editor in place, saving as you go (redesign stage 9g2).

Today's Rediger observasjon form (``ObservationDetailsDialog``), in place as a
mode for the observation shown instead of a window. Owner's decisions
(2026-11-11):

- Autosave: a change saves about a second after typing stops, and at once
  when focus leaves the field, the observation changes or Rediger is left.
  A quiet status says “Saving…” / “Saved”; there is no Save, Cancel or
  prompt when leaving.
- The taxon changes only when a name is picked (or an AI suggestion is
  used): search text typed but not picked is never saved, and the status
  says so.
- Draft, sharing and location precision save at once, through the same
  publish notice the dialog's Save showed; Cancel puts the control back.

What is saved is every change to what the form would save, once it has
finished opening, whatever made it: the user's edits, and what the form
fills in by itself afterwards (a place name looked up for the position),
as the dialog's Save saved both. Only the changed columns are written
(a picked name and a moved point as a whole), so a field changed elsewhere
meanwhile (cloud sync, another page) is kept unless the form changed it
too. Opening, scrolling and switching write nothing unless the form
changes something. Images are not changed here: Edit images opens Bilder ›
Files and SD card. Writes go through
``ObservationsTab.write_observation_edits``, the dialog's own path.
"""
from __future__ import annotations

from PySide6.QtCore import QEvent, QObject, QTimer, Qt, Signal
from PySide6.QtWidgets import (
    QAbstractButton, QAbstractItemView, QAbstractSpinBox, QApplication, QComboBox, QDateTimeEdit, QFrame, QLabel,
    QLineEdit, QPlainTextEdit, QScrollArea, QStackedWidget, QTextEdit, QVBoxLayout, QWidget,
)

from database.models import ObservationDB

_PUBLISH_KEYS = frozenset({"is_draft", "sharing_scope", "location_public", "location_precision"})


def _key_groups() -> tuple[tuple[frozenset, frozenset], ...]:
    """(trigger, whole) pairs: when a trigger column changed, the whole group saves.

    A picked name is a whole identification and a moved point carries its
    country and region, so those save together (the user's choice wins
    entirely). Any other column saves on its own, so a value changed
    elsewhere meanwhile (cloud sync) is kept unless the user changed it too.
    """
    from utils.cloud_sync_impl.reconciliation.location_provenance import LOCATION_PROVENANCE_FIELDS
    from utils.taxon_identity import IDENTITY_COLUMNS

    taxon_core = frozenset({
        "genus", "species", "inaturalist_taxon_id", "species_guess",
        "scientific_name_snapshot", "taxon_rank_snapshot", "sporely_taxon_id", *IDENTITY_COLUMNS,
    })
    taxon = taxon_core | {"common_name", "red_list_category", "red_list_categories_json"}
    point = frozenset({"gps_latitude", "gps_longitude"})
    position = point | {"country_code", "region_id", *LOCATION_PROVENANCE_FIELDS}
    return (taxon_core, taxon), (point, position)


# Bookkeeping that changes without changing what the form shows (a save's
# timestamp, sync status): no reason to rebuild the form.
_BOOKKEEPING_COLUMNS = frozenset({
    "updated_at", "sync_status", "synced_at", "sync_error_code", "sync_error_message",
    "sync_blocked_reason", "sync_blocked_at", "mosaic_signature",
})


def _content(state):
    """A stored state without its bookkeeping columns (None stays None)."""
    if state is None:
        return None
    row, images, language = state
    return (
        {key: value for key, value in row.items() if key not in _BOOKKEEPING_COLUMNS},
        images,
        language,
    )


class EditObservationPage(QWidget):
    """One in-place editor for the observation shown; changes save as you go."""

    # (observation id, image path or "") — Edit images: open Bilder › Files and SD card
    editImagesRequested = Signal(int, str)

    AUTOSAVE_DELAY_MS = 1000

    def __init__(self, observations_tab, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("editObservationPage")
        self._observations_tab = observations_tab
        self._observation_id: int | None = None
        self._editor = None
        self._frame: QScrollArea | None = None
        self._holder: QWidget | None = None  # the scrolling form and, pinned below it, its bottom bar
        self._stored_state = None  # what the form was built from, with its own writes folded in
        self._must_reload = False  # a change that could not be saved was discarded
        self._saved_updates: dict | None = None  # the columns as last saved (or as opened)
        self._saved_observation: dict = {}
        self._saving = False
        self._saved_any = False
        self._taxonomy_changed = False
        self._save_error: str | None = None
        self._stale = False
        self._pending_show: int | None = None  # a switch asked for while the publish notice was open
        self._has_pending_show = False
        self._show_token = 0  # any newer switch (or a deletion) cancels a held one
        self._opened_ai_state: str | None = None  # the AI state as the form opened (or last saved it)
        self._ai_pending = False  # the AI state could not be saved: try again

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        self._stack = QStackedWidget(self)
        layout.addWidget(self._stack, 1)
        self._empty = QLabel(self.tr("Choose an observation to edit it."), self)
        self._empty.setAlignment(Qt.AlignCenter)
        self._empty.setWordWrap(True)
        self._stack.addWidget(self._empty)
        # The save status sits in the editor's bottom bar, where Save was.
        self.status_label = QLabel(self)
        self.status_label.setObjectName("editStatusLabel")
        self.status_label.hide()

        self._timer = QTimer(self)
        self._timer.setSingleShot(True)
        self._timer.setInterval(self.AUTOSAVE_DELAY_MS)
        self._timer.timeout.connect(self.save_now)
        # Not every change reports itself (a picked name, an AI suggestion's
        # Use, a drop, a lookup's answer): look for them while Rediger shows.
        self._watch = QTimer(self)
        self._watch.setInterval(1000)
        self._watch.timeout.connect(self._check_for_changes)
        QApplication.instance().focusChanged.connect(self._on_focus_changed)

    # -- which observation ----------------------------------------------------

    def observation_id(self) -> int | None:
        return self._observation_id

    def editor(self):
        return self._editor

    def is_busy(self) -> bool:
        """The publish notice is open (a save is waiting for its answer)."""
        return self._saving

    def show_observation(self, observation_id: int | None, _token: int | None = None) -> None:
        if _token is not None and _token != self._show_token:
            return  # a newer switch or a deletion came after this held one
        self._show_token += 1
        try:
            observation_id = int(observation_id or 0) or None
        except (TypeError, ValueError):
            observation_id = None
        if observation_id is not None and observation_id == self._observation_id and self._editor is not None:
            self._pending_show = None
            self._has_pending_show = False
            return
        if self._saving:
            # The publish notice is open: switch (once) when it is answered.
            self._pending_show = observation_id
            self._has_pending_show = True
            return
        if not self._leave_or_discard():
            return
        self._show_token += 1  # requests held while leaving are older than this switch
        self._drop_editor()
        self._observation_id = None
        if observation_id is None or ObservationDB.get_observation(observation_id) is None:
            self._stack.setCurrentWidget(self._empty)
            self._update_status()
            return
        self._create_editor(observation_id)

    def reload(self) -> None:
        """Start again from what is saved (shown again: images or sync may have changed it)."""
        observation_id = self._observation_id
        if observation_id is None or self._saving:
            return
        if not self.leave():
            return  # a change is still pending (the save failed): keep the form as it is
        self._drop_editor()
        self._observation_id = None
        self.show_observation(observation_id)

    def _leave_or_discard(self) -> bool:
        """Save before the form goes; if that fails, Try again or Discard (never silently lost)."""
        from PySide6.QtWidgets import QMessageBox

        while not self.leave():
            box = QMessageBox(self)
            box.setIcon(QMessageBox.Warning)
            box.setWindowTitle(self.tr("Edit could not save"))
            box.setText(self.tr("Your last change could not be saved: {error}").format(error=self._save_error or ""))
            retry = box.addButton(self.tr("Try again"), QMessageBox.AcceptRole)
            box.addButton(self.tr("Discard it"), QMessageBox.DestructiveRole)
            box.setDefaultButton(retry)
            box.exec()
            if box.clickedButton() is not retry:
                self._discard_pending()
                return True
        return True

    def _discard_pending(self) -> None:
        """The user chose to drop what could not be saved: the form starts again from the database."""
        self._save_error = None
        self._stale = True
        self._must_reload = True
        if self._editor is not None:
            try:
                self._saved_updates = self._current_updates()
            except Exception:
                self._saved_updates = None

    def forget_observation(self, observation_id: int) -> None:
        """The observation was deleted: drop its editor without saving."""
        if self._pending_show is not None and int(observation_id) == self._pending_show:
            self._pending_show = None
            self._has_pending_show = False
        if self._observation_id is not None and int(observation_id) == self._observation_id:
            self._show_token += 1
            self._drop_editor()
            self._observation_id = None
            self._saved_any = False
            self._stack.setCurrentWidget(self._empty)
            self._update_status()

    # -- the editor -----------------------------------------------------------

    def _create_editor(self, observation_id: int) -> None:
        from .observations_tab import ObservationDetailsDialog

        tab = self._observations_tab
        # AI suggestions from the database, not a copy kept from an earlier form.
        getattr(tab, "_ai_suggestions_cache", {}).pop(observation_id, None)
        prepared = tab.prepare_observation_edit(observation_id)
        if prepared is None:
            self._stack.setCurrentWidget(self._empty)
            return
        observation, _existing, image_results, draft_observation, ai_state = prepared
        # What the form is built from, to tell on coming back whether it is out of date.
        stored_state = self._read_stored_state(observation_id)
        # Opening restores the AI table (selecting a row): that is not an edit,
        # so the form writes no AI state until the user changes something.
        ObservationDetailsDialog._persist_ai_paused = True
        try:
            editor = ObservationDetailsDialog(
                self,
                observation=draft_observation,
                image_results=image_results,
                allow_edit_images=True,
                ai_state=ai_state,
            )
        finally:
            ObservationDetailsDialog._persist_ai_paused = False
        editor._persist_ai_paused = True
        editor.set_in_place()
        editor.add_in_place_status(self.status_label)
        self.status_label.show()
        editor.editImagesRequested.connect(
            lambda path, obs=observation_id: self._on_edit_images_requested(obs, path)
        )
        # The bottom bar (hints, Edit images, the save status) stays in view
        # below the form, which scrolls when the window is too low for it.
        bar = editor.take_bottom_bar()
        holder = QWidget(self)
        holder_layout = QVBoxLayout(holder)
        holder_layout.setContentsMargins(0, 0, 0, 0)
        holder_layout.setSpacing(0)
        frame = QScrollArea(holder)
        frame.setWidgetResizable(True)
        frame.setFrameShape(QFrame.NoFrame)
        frame.setWidget(editor)
        holder_layout.addWidget(frame, 1)
        if bar is not None:
            bar.setParent(holder)
            holder_layout.addWidget(bar, 0)
        self._stack.addWidget(holder)
        self._stack.setCurrentWidget(holder)
        self._editor = editor
        self._frame = frame
        self._holder = holder
        self._observation_id = observation_id
        self._saved_observation = dict(observation)
        self._saved_updates = None
        self._saved_any = False
        self._taxonomy_changed = False
        self._save_error = None
        self._stale = False
        self._must_reload = False
        self._stored_state = stored_state
        self._note_ai_writes(editor, observation_id)
        self._watch_inputs(editor)
        # The form finishes opening on the next turn of the event loop; what it
        # holds then is the baseline (nothing the user changed).
        QTimer.singleShot(0, self, lambda ed=editor: self._take_baseline(ed))
        self._update_status()

    def _take_baseline(self, editor) -> None:
        if editor is self._editor and self._saved_updates is None:
            self._saved_updates = self._current_updates()
            self._opened_ai_state = self._ai_state_text(editor)
            self._ai_pending = False

    def _read_stored_state(self, observation_id: int | None = None):
        """What the form shows depends on: the observation's row, its images
        and the name language (None when unreadable)."""
        from database.models import ImageDB, SettingsDB

        if observation_id is None:
            observation_id = self._observation_id
        if observation_id is None:
            return None
        try:
            row = ObservationDB.get_observation(observation_id)
            images = [dict(image) for image in ImageDB.get_images_for_observation(observation_id) or []]
            language = SettingsDB.get_setting("vernacular_language", "no")
        except Exception:
            return None
        return (dict(row), images, language) if row else None

    def _note_ai_writes(self, editor, observation_id: int) -> None:
        """The form stores AI results itself when they arrive: those are its own writes too."""
        persist = editor._persist_ai_state_now

        def persist_and_note(*args, **kwargs):
            mine = editor is self._editor and self._observation_id == observation_id
            before = self._read_stored_state(observation_id) if mine else None
            result = persist(*args, **kwargs)
            if mine and editor is self._editor:
                self._note_own_write(before, self._read_stored_state(observation_id))
            return result

        editor._persist_ai_state_now = persist_and_note

    def _note_own_write(self, before, after) -> None:
        """Fold this form's own write (``before`` → ``after``) into what it is known to show.

        Only what the write changed: a change made elsewhere before it (another
        column, other images) still differs, so coming back reloads.
        """
        known = self._stored_state
        if known is None or before is None or after is None:
            self._stored_state = None  # unknown: coming back reloads
            return
        row = dict(known[0])
        row.update({key: value for key, value in after[0].items() if before[0].get(key) != value})
        images = after[1] if known[1] == before[1] else known[1]
        self._stored_state = (row, images, known[2])

    @staticmethod
    def _ai_state_text(editor) -> str:
        import json

        return json.dumps(editor.get_ai_state(), sort_keys=True, default=str)

    def _drop_editor(self) -> None:
        editor, holder = self._editor, self._holder
        self._editor = None
        self._frame = None
        self._holder = None
        self._stored_state = None
        self._timer.stop()
        if editor is None:
            return
        QApplication.instance().removeEventFilter(self)
        self.status_label.hide()
        self.status_label.setParent(self)  # out of the editor before it goes
        editor.close()  # its closeEvent stops the AI and lookup threads
        if holder is not None:
            self._stack.removeWidget(holder)
            holder.deleteLater()  # and the editor and its bottom bar with it

    def _watch_inputs(self, editor) -> None:
        """A change in the form schedules a save; the publishing controls ask at once.

        Signals only make saving prompt: whether anything is saved is decided
        by comparing what the form would save with the last save, so a change
        no signal reports is found by the watch all the same.
        """
        immediate = {
            id(widget)
            for widget in (
                getattr(editor, "is_draft_checkbox", None),
                *getattr(getattr(editor, "sharing_scope_selector", None), "findChildren", lambda _t: [])(QAbstractButton),
                *getattr(getattr(editor, "location_precision_selector", None), "findChildren", lambda _t: [])(QAbstractButton),
            )
            if widget is not None
        }
        for widget in editor.findChildren(QWidget):
            if isinstance(widget, QLineEdit):
                widget.textChanged.connect(self._schedule)
            elif isinstance(widget, (QTextEdit, QPlainTextEdit)):
                widget.textChanged.connect(self._schedule)
            elif isinstance(widget, QDateTimeEdit):
                widget.dateTimeChanged.connect(self._schedule)
            elif isinstance(widget, QAbstractSpinBox) and hasattr(widget, "valueChanged"):
                widget.valueChanged.connect(self._schedule)
            elif isinstance(widget, QComboBox):
                widget.currentIndexChanged.connect(self._schedule)
            elif isinstance(widget, QAbstractButton):
                widget.clicked.connect(self._save_soon if id(widget) in immediate else self._schedule)
        picker = getattr(editor, "taxon_picker", None)
        if picker is not None:
            picker.concept_picked.connect(self._schedule)
            picker.free_text_committed.connect(self._schedule)
            picker.cleared.connect(self._schedule)
            picker.search_input.textChanged.connect(self._update_status)
        # The user's input lets AI suggestions they ask for be kept again.
        QApplication.instance().installEventFilter(self)

    # -- what changed ---------------------------------------------------------

    def _current_updates(self) -> dict:
        editor = self._editor
        tab = self._observations_tab
        updates = tab._edit_observation_updates(
            editor.get_data(),
            ai_state_json=None,
            unidentified=bool(editor.is_unidentified()),
        )
        updates.pop("allow_nulls", None)
        # AI suggestions save on their own (when the user asks for them), not
        # as an edit: choosing which photos AI looks at changes nothing else.
        updates.pop("ai_state_json", None)
        return updates

    def _changed_keys(self) -> set[str]:
        """Columns that differ from the last save, widened to whole groups."""
        if self._editor is None or self._saved_updates is None:
            return set()
        current = self._current_updates()
        saved = self._saved_updates
        changed = {key for key in set(current) | set(saved) if current.get(key) != saved.get(key)}
        if not changed:
            return set()
        for trigger, whole in _key_groups():
            if changed & trigger:
                changed |= whole
        return changed

    # -- autosave -------------------------------------------------------------

    _INPUT_EVENTS = (QEvent.KeyPress, QEvent.MouseButtonPress, QEvent.Wheel)

    def eventFilter(self, watched: QObject, event: QEvent) -> bool:  # noqa: N802 - Qt API
        editor = self._editor
        if (
            editor is not None
            and editor._persist_ai_paused
            and event.type() in self._INPUT_EVENTS
            and isinstance(watched, QWidget)
            and (watched is editor or editor.isAncestorOf(watched))
        ):
            editor._persist_ai_paused = False
        return super().eventFilter(watched, event)

    def _schedule(self, *_args) -> None:
        if self._editor is None or self._saving or self._saved_updates is None:
            return  # still opening: the baseline is taken once it has finished
        self._timer.start()
        self._update_status()

    def _save_soon(self, *_args) -> None:
        """Draft, sharing and precision: save (and ask) right after the click."""
        if self._editor is None or self._saving:
            return
        QTimer.singleShot(0, self, self.save_now)

    def _check_for_changes(self) -> None:
        if self._editor is None or self._saving or self._timer.isActive():
            return
        try:
            changed = bool(self._changed_keys())
            editor = self._editor
            if (
                not self._ai_pending
                and self._opened_ai_state is not None
                and not editor._persist_ai_paused
                and self._ai_state_text(editor) != self._opened_ai_state
            ):
                # AI results the form got (and may have failed to store itself).
                self._ai_pending = True
        except Exception:
            changed = True  # save_now reports what failed
        if changed or self._ai_pending:
            self.save_now()

    def _on_focus_changed(self, old, new) -> None:
        editor = self._editor
        if editor is None or old is None or not self._timer.isActive() or self._saving:
            return
        if editor.isAncestorOf(old) and (new is None or new is not old):
            self.save_now()  # leaving a field saves it

    def save_now(self, leaving: bool = False) -> bool:
        """Save what changed since the last save; False when it could not be saved.

        Every change since the form finished opening, the user's and the
        form's own, as Save did; nothing when nothing changed. A failure
        (reading or writing) keeps the change pending and says so.
        ``leaving`` is accepted for callers; leaving saves the same way.
        """
        del leaving
        self._timer.stop()
        editor = self._editor
        if editor is None or self._saving or self._observation_id is None:
            return True
        observation_id = self._observation_id
        self._saving = True
        try:
            try:
                if ObservationDB.get_observation(observation_id) is None:
                    self._saving = False
                    self.forget_observation(observation_id)  # deleted elsewhere
                    return True
                if self._saved_updates is None:
                    self._take_baseline(editor)
                changed = self._changed_keys()
                if not changed:
                    if self._ai_pending:
                        before = self._read_stored_state(observation_id)
                        self._save_ai_state(editor)
                        self._save_error = None
                        self._note_own_write(before, self._read_stored_state(observation_id))
                    return True
                if changed & _PUBLISH_KEYS:
                    accepted = editor.confirm_publish_change(changed_keys=changed)
                    # The notice is modal: the form, the observation or the
                    # page may have changed while it was open.
                    if editor is not self._editor or self._observation_id != observation_id:
                        return True
                    if ObservationDB.get_observation(observation_id) is None:
                        self._saving = False
                        self.forget_observation(observation_id)
                        return True
                    if not accepted:
                        # Cancelled: the controls show what is stored; nothing
                        # about publishing is written, anything else still saves.
                        current = self._current_updates()
                        for key in _PUBLISH_KEYS:
                            if key in current:
                                self._saved_updates[key] = current[key]
                    changed = self._changed_keys()  # and what arrived meanwhile
                    if not changed:
                        return True
                self.status_label.setText(self.tr("Saving…"))
                written = self._current_updates()
                before = self._read_stored_state(observation_id)
                taxonomy_changed = self._observations_tab.write_observation_edits(
                    observation_id, self._saved_observation, editor, only_keys=changed,
                )
            except Exception as exc:  # the database is locked or gone: keep the change pending
                self._save_error = str(exc) or exc.__class__.__name__
                return False
            self._save_error = None
            # What was written is saved; anything that changed since stays pending.
            for key in changed:
                if key in written:
                    self._saved_updates[key] = written[key]
                else:
                    self._saved_updates.pop(key, None)
            self._saved_observation.update(
                genus=self._saved_updates.get("genus"), species=self._saved_updates.get("species"),
            )
            self._saved_any = True
            # The AI state saves with the observation, as Save did (a cleared
            # name clears its AI choice too), when it changed in this form.
            try:
                self._save_ai_state(editor)
            except Exception as exc:
                self._save_error = str(exc) or exc.__class__.__name__
                return False
            if taxonomy_changed:
                self._taxonomy_changed = True
                # A new name can move the observation's folder: the images follow.
                reload_images = getattr(editor, "_reload_observation_images_after_cloud_media_materialization", None)
                if callable(reload_images):
                    reload_images()
            # What this form wrote is what it shows: coming back needs no reload for it.
            self._note_own_write(before, self._read_stored_state(observation_id))
            return True
        finally:
            self._saving = False
            self._update_status()
            if getattr(self, "_has_pending_show", False):
                self._has_pending_show = False
                pending, self._pending_show = self._pending_show, None
                token = self._show_token
                QTimer.singleShot(0, self, lambda obs=pending, tok=token: self.show_observation(obs, _token=tok))

    def _save_ai_state(self, editor) -> None:
        """Write the AI state if this form changed it (never a copy older than the stored one)."""
        current = self._ai_state_text(editor)
        if current == self._opened_ai_state:
            self._ai_pending = False
            return
        self._ai_pending = True
        editor._persist_ai_paused = False
        editor._persist_ai_state_now(raise_errors=True)
        self._opened_ai_state = current
        self._ai_pending = False

    def leave(self) -> bool:
        """Rediger is left or the observation changes: save, then refresh the lists once."""
        if self._editor is None:
            return True
        saved = self.save_now(leaving=True)
        if self._saved_any:
            observation_id = self._observation_id
            taxonomy_changed = self._taxonomy_changed
            self._saved_any = False
            self._taxonomy_changed = False
            refresh = getattr(self.window(), "refresh_after_observation_edits", None)
            if callable(refresh):
                refresh(observation_id, taxonomy_changed=taxonomy_changed)
        return saved

    def _on_edit_images_requested(self, observation_id: int, path: str) -> None:
        self._leave_or_discard()
        self.editImagesRequested.emit(int(observation_id), str(path or ""))

    # -- status ---------------------------------------------------------------

    def _update_status(self, *_args) -> None:
        editor = self._editor
        if editor is None:
            self.status_label.setText("")
            return
        if self._save_error:
            text = self.tr("Not saved: {error}").format(error=self._save_error)
        elif self._timer.isActive():
            text = self.tr("Unsaved changes…")
        elif self._saved_any:
            text = self.tr("Saved")
        else:
            text = self.tr("Changes save as you go")
        unfinished = editor.unfinished_taxon_text() if hasattr(editor, "unfinished_taxon_text") else ""
        if unfinished:
            text = self.tr("{status} · “{text}” is not saved: choose a name from the list").format(
                status=text, text=unfinished
            )
        self.status_label.setText(text)

    def showEvent(self, event) -> None:  # noqa: N802 - Qt override
        super().showEvent(event)
        self._watch.start()
        if self._stale and self._editor is not None:
            QTimer.singleShot(0, self, self._reload_if_changed)

    def _reload_if_changed(self) -> None:
        """Back in Rediger: keep the form unless the observation changed meanwhile.

        Everything was saved on leaving. Bilder, Vis or a sync may have changed
        the observation or its images since: then the form starts again from
        the database. Otherwise it stays as it is (building it takes seconds).
        """
        if not self._stale or self._editor is None or self._saving:
            return
        stored = self._read_stored_state()
        if self._must_reload or stored is None or _content(stored) != _content(self._stored_state):
            self.reload()
        else:
            self._stale = False

    def hideEvent(self, event) -> None:  # noqa: N802 - Qt override
        self._watch.stop()
        if event.spontaneous():
            self.save_now()  # the window is minimised: save quietly, ask nothing
        elif not self._saving:
            self._leave_or_discard()  # leaving Rediger: a failed save asks (Try again or Discard)
            self._stale = True
        super().hideEvent(event)

    def shutdown(self) -> None:
        """The window closes: the last save already ran (see ``MainWindow.closeEvent``)."""
        self._drop_editor()
