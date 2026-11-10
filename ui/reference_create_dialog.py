"""Create a new reference in two steps (UI redesign stage 7d).

Opened by "New reference…" in the reference drawer. Step 1 is the source:
the publication, the name as published and the locator. Step 2 is the
measurements: paste the string from the literature and press Parse; the value
grid appears only after parsing or on "Fill in manually". The individual
spore rows and Parmasto biometrics stay behind their disclosures.

The form is the shared :class:`~ui.reference_entry_editor.ReferenceEntryEditor`
in its staged mode, so parsing, validation and the library payload are the
same as everywhere else. The dialog persists nothing itself: "Save to
library" and "Add to plot" call the host's callbacks (``MainWindow``), which
reuse the existing save and submit paths and report whether they stored
anything. The dialog closes only on success.
"""
from __future__ import annotations

from typing import Callable

from PySide6.QtCore import QCoreApplication, Qt
from PySide6.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QScrollArea,
    QVBoxLayout,
)

from .add_reference_dialog import EditorScrollArea
from .reference_entry_editor import ReferenceEntryEditor
from .styles import pt
from .window_state import GeometryMixin

SOURCE_STEP = 0
MEASUREMENTS_STEP = 1


class ReferenceCreateDialog(GeometryMixin, QDialog):
    _geometry_key = "ReferenceCreateDialog"

    def __init__(
        self,
        parent,
        *,
        genus: str,
        species: str,
        observation_id: int | None,
        sporely_taxon_id: int | None,
        common_name: str | None = None,
        save_callback: Callable[[ReferenceEntryEditor], object] | None = None,
        add_callback: Callable[[ReferenceEntryEditor], bool] | None = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle(self.tr("New reference"))
        self.setModal(True)
        self.setMinimumSize(640, 460)  # four German buttons in one row
        self.resize(760, 640)
        self._save_callback = save_callback
        self._add_callback = add_callback
        self.saved_measurement_set_id: str | None = None
        self.added_to_plot = False

        layout = QVBoxLayout(self)
        layout.setSpacing(8)

        header = QHBoxLayout()
        header.setSpacing(8)
        taxon = " ".join(part for part in (genus, species) if part).strip()
        self.taxon_label = QLabel(f"<i>{taxon}</i>" if taxon else self.tr("No taxon"), self)
        self.taxon_label.setTextFormat(Qt.RichText)
        self.taxon_label.setStyleSheet("font-weight: 600;")
        header.addWidget(self.taxon_label, 0)
        if common_name:
            common_label = QLabel(common_name, self)
            common_label.setStyleSheet(f"color: #7f8c8d; font-size: {pt(9)}pt;")
            header.addWidget(common_label, 0)
        header.addStretch(1)
        self.step_label = QLabel("", self)
        self.step_label.setObjectName("referenceCreateStep")
        self.step_label.setStyleSheet(f"color: #7f8c8d; font-size: {pt(9)}pt;")
        header.addWidget(self.step_label, 0)
        layout.addLayout(header)

        self.editor = ReferenceEntryEditor(
            self,
            genus,
            species,
            observation_id=observation_id,
            sporely_taxon_id=sporely_taxon_id,
            observation_taxon_id=sporely_taxon_id,
            staged=True,
        )
        self.editor.data_changed.connect(self._update_buttons)
        self.editor_scroll = EditorScrollArea(self)
        self.editor_scroll.setWidgetResizable(True)
        self.editor_scroll.setFrameShape(QScrollArea.NoFrame)
        self.editor_scroll.setWidget(self.editor)
        layout.addWidget(self.editor_scroll, 1)

        # The hint gets a row of its own: beside four buttons it was squeezed
        # to several lines (German).
        self.hint_row = self.editor.take_hint_row()
        self.hint_controller = self.editor._hint_controller
        layout.addWidget(self.hint_row)
        footer = QHBoxLayout()
        footer.setSpacing(8)
        footer.addStretch(1)
        self.cancel_btn = QPushButton(self.tr("Cancel"), self)
        self.cancel_btn.clicked.connect(self.reject)
        self.back_btn = QPushButton(self.tr("Back"), self)
        self.back_btn.clicked.connect(lambda: self.show_step(SOURCE_STEP))
        self.next_btn = QPushButton(self.tr("Next"), self)
        self.next_btn.clicked.connect(self._on_next_clicked)
        self.save_btn = QPushButton(self.tr("Save to library"), self)
        self.save_btn.setToolTip(
            self.tr("Keep this reference in the library without adding it to the plot.")
        )
        self.save_btn.clicked.connect(self._on_save_clicked)
        self.add_btn = QPushButton(self.tr("Add to plot"), self)
        self.add_btn.clicked.connect(self._on_add_clicked)
        for button in (self.cancel_btn, self.back_btn, self.next_btn, self.save_btn, self.add_btn):
            button.setAutoDefault(False)
            footer.addWidget(button)
        layout.addLayout(footer)

        self._restore_geometry()
        self.finished.connect(self._save_geometry)
        self.show_step(SOURCE_STEP)

    # -- steps ---------------------------------------------------------------

    def current_step(self) -> int:
        return self.editor.current_step()

    def _on_next_clicked(self) -> None:
        # Typed text that exactly names a library publication counts as
        # choosing it; several with that name must be told apart here.
        if self.editor.bind_typed_publication() == "ambiguous":
            self.hint_controller.set_hint(
                QCoreApplication.translate(
                    "ReferenceAddDialog",
                    "Several publications match this name — choose one from the list.",
                ),
                tone="warning",
            )
            return
        self.show_step(MEASUREMENTS_STEP)

    def show_step(self, step: int) -> None:
        self.editor.show_step(step)
        on_source = self.current_step() == SOURCE_STEP
        self.step_label.setText(
            self.tr("Step 1 of 2: Source") if on_source else self.tr("Step 2 of 2: Measurements")
        )
        self.back_btn.setVisible(not on_source)
        self.next_btn.setVisible(on_source)
        self.save_btn.setVisible(not on_source)
        self.add_btn.setVisible(not on_source)
        # Enter moves on in step 1. Step 2 has no default button: Enter in the
        # paste field parses, and must not also add the reference.
        self.next_btn.setDefault(on_source)
        self.editor_scroll.verticalScrollBar().setValue(0)
        if on_source:
            self.editor.publication_combo.setFocus(Qt.OtherFocusReason)
        self.hint_controller.set_hint(
            self.tr("Choose the publication the measurements come from, or add a new one.")
            if on_source
            else self.editor._default_hint_text
        )
        self._update_buttons()

    def _update_buttons(self) -> None:
        if not hasattr(self, "add_btn"):
            return  # the editor refreshes once while it is being built
        editor = self.editor
        ready = editor.is_ready_to_submit()
        self.add_btn.setEnabled(self._add_callback is not None and ready)
        self.save_btn.setEnabled(
            self._save_callback is not None
            and ready
            and editor.has_publication()
            and editor.library_payload_ready()
        )

    # -- actions -------------------------------------------------------------

    def _on_save_clicked(self) -> None:
        """Store the reference in the library; it is not plotted. Closes on success."""
        if self._save_callback is None or not self.editor.validate_and_build_result():
            return
        saved = self._save_callback(self.editor)
        if not saved:
            # The host has said why; nothing was stored, so stay open.
            self.hint_controller.set_hint(
                self.tr("Not saved — nothing was stored in the reference library."), tone="warning"
            )
            return
        self.saved_measurement_set_id = str(saved)
        self.accept()

    def _on_add_clicked(self) -> None:
        """Store the reference and add it to the plot. Closes on success."""
        if self._add_callback is None or not self.editor.validate_and_build_result():
            return
        if not self._add_callback(self.editor):
            return
        self.added_to_plot = True
        self.accept()


__all__ = ["ReferenceCreateDialog", "SOURCE_STEP", "MEASUREMENTS_STEP"]
