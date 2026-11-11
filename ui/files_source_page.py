"""Bilder › Files and SD card (redesign stage 9f, first step).

Today's Prepare Images editor (``ImageImportDialog``), in place as a page in
Bilder instead of a window, for the observation Bilder shows. It edits the
observation's images and adds new ones; “Save to observation” saves through
the same path as the dialog (``ObservationsTab.save_prepared_images``) and
“Discard changes” drops them. Unsaved work stays with its observation:
switching observation keeps that observation's editor (owner, 2026-11-10:
staged images wait for the observation they were taken for), and its new
images show as unsaved in the shared strip and the observation list.

The layout follows the dialog for now; the mockup's layout is stage 9f2.
"""
from __future__ import annotations

from PySide6.QtCore import QTimer, Qt, Signal
from PySide6.QtWidgets import (
    QDialog, QFrame, QHBoxLayout, QLabel, QPushButton, QScrollArea, QStackedWidget, QVBoxLayout, QWidget,
)

from database.models import ImageDB, ObservationDB
from .styles import STAGING_BORDER
from .image_import_dialog import ImageImportDialog


class FilesSourcePage(QWidget):
    """One in-place editor per observation with unsaved work; one shown."""

    # (observation id, unsaved new images as strip items)
    unsavedChanged = Signal(int, object)

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
        self._busy = False
        # Not every edit in the editor reports itself (removing, cropping,
        # reordering): look for unsaved work while the page is on screen.
        self._watch = QTimer(self)
        self._watch.setInterval(700)
        self._watch.timeout.connect(self._refresh_unsaved_state)
        self._last_unsaved: dict[int, tuple] = {}
        self._empty = QLabel(self.tr("Choose an observation to add files and photos from an SD card to it."), self)
        self._empty.setAlignment(Qt.AlignCenter)
        self._empty.setWordWrap(True)
        self._stack.addWidget(self._empty)

    # -- which observation ----------------------------------------------------

    def observation_id(self) -> int | None:
        return self._observation_id

    def editor(self) -> ImageImportDialog | None:
        return self._editors.get(self._observation_id) if self._observation_id else None

    def showEvent(self, event) -> None:  # noqa: N802 - Qt override
        super().showEvent(event)
        self._watch.start()
        editor = self.editor()
        if editor is not None and not editor.has_unsaved_changes() and not self._busy:
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
        if observation_id == self._observation_id and (observation_id is None or observation_id in self._editors):
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
            return
        if observation_id not in self._editors:
            self._create_editor(observation_id)
        self._stack.setCurrentWidget(self._frames[observation_id])
        self._update_save_row()

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
        editor.finished.connect(lambda result, obs=observation_id, ed=editor: self._on_editor_finished(obs, ed, result))
        editor.unsavedChanged.connect(lambda obs=observation_id, ed=editor: self._mirror_unsaved(obs, ed))
        self._editors[observation_id] = editor
        self._context[observation_id] = (observation, existing_images)
        frame = QScrollArea(self)
        frame.setWidgetResizable(True)
        frame.setFrameShape(QFrame.NoFrame)
        frame.setWidget(editor)
        self._frames[observation_id] = frame
        self._stack.addWidget(frame)
        return editor

    def _drop_editor(self, observation_id: int) -> None:
        editor = self._editors.pop(observation_id, None)
        self._context.pop(observation_id, None)
        frame = self._frames.pop(observation_id, None)
        if editor is None:
            return
        editor.close()  # its closeEvent cleans threads and temporary previews
        if frame is not None:
            self._stack.removeWidget(frame)
            frame.deleteLater()  # and the editor with it
        self.unsavedChanged.emit(int(observation_id), [])

    def _on_editor_finished(self, observation_id: int, editor: ImageImportDialog, result: int) -> None:
        if self._editors.get(observation_id) is not editor:
            return
        if result == QDialog.Accepted:
            self._save_editor(observation_id, editor)
        self._drop_editor(observation_id)
        if observation_id == self._observation_id:
            # A fresh editor on what is saved now (or was, after Discard).
            self._create_editor(observation_id)
            self._stack.setCurrentWidget(self._frames[observation_id])
        self._update_save_row()

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
            reselect=False,
        )
        host = tab.window()
        if int(getattr(host, "active_observation_id", 0) or 0) == int(observation_id):
            refresh = getattr(host, "refresh_observation_images", None)
            if callable(refresh):
                refresh(select_image_id=getattr(host, "current_image_id", None))
        if tab.selected_observation_ids() == [int(observation_id)]:
            tab.on_selection_changed()

    def _set_row_enabled(self, enabled: bool) -> None:
        self.save_btn.setEnabled(enabled)
        self.discard_btn.setEnabled(enabled)

    def _save(self) -> None:
        editor = self.editor()
        if editor is None or self._busy:
            return
        self._busy = True  # saving can process events (RAW renders): no second click meanwhile
        self._set_row_enabled(False)
        try:
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
            if editor.unsaved_new_results()
        }

    def has_unsaved_work(self) -> bool:
        return any(editor.has_unsaved_changes() for editor in self._editors.values())

    def shutdown(self) -> None:
        for observation_id in list(self._editors):
            editor = self._editors.pop(observation_id)
            editor.close()
        self._context.clear()
        self._frames.clear()
