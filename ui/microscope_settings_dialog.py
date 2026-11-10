"""Change objective, contrast, mount or stain on several images (redesign plan stage 8).

Opened from Mål's gallery ("Change microscope settings…" on the selected
images). Each setting starts at "Don't change"; beside it is what the selected
images have now. Before anything is written, the dialog says how many images
change and how many measurements are recalculated for a new objective's
scale; the Apply button carries the image count. The change itself is
``MicroscopeSettingsDB.apply``: one transaction, the one rescale path, one
session-log event per observation, and sync through the observation as usual.
Only microscope images change; field images in the selection are left alone.
"""
from __future__ import annotations

from collections import Counter

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QDialog,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
)

from database.database_tags import DatabaseTerms
from database.models import (
    KEEP_FIELD,
    CalibrationDB,
    ImageDB,
    MicroscopeSettingsChanged,
    MicroscopeSettingsDB,
    MicroscopeSettingsOutcome,
    ObjectiveChoice,
    SettingsDB,
)
from database.schema import load_objectives, objective_display_name, objective_sort_value

from .styles import pt

# category for DatabaseTerms -> image column
_FIELDS = (("contrast", "contrast"), ("mount", "mount_medium"), ("stain", "stain"))


class MicroscopeSettingsDialog(QDialog):
    def __init__(self, parent, image_ids, *, objectives: dict | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle(self.tr("Change microscope settings"))
        self.setModal(True)
        self.setMinimumWidth(460)
        self._image_ids = [int(i) for i in image_ids]
        self._objectives = {
            key: objective for key, objective in (objectives if objectives is not None else load_objectives()).items()
            if _positive(objective.get("microns_per_pixel"))
        }
        self.outcome: MicroscopeSettingsOutcome | None = None
        # The change and preview the user is looking at: Apply writes exactly
        # that change, and only while the images still match that preview.
        self._shown_change: dict = {}
        self._shown_preview: MicroscopeSettingsOutcome | None = None
        images = [ImageDB.get_image(image_id) or {} for image_id in self._image_ids]
        microscope = [image for image in images if image.get("image_type") == "microscope"]

        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        intro = self.tr("%n image(s) selected.", "", len(self._image_ids))
        skipped = len(images) - len(microscope)
        if skipped:
            intro += " " + self.tr("%n field image(s) will not be changed.", "", skipped)
        self.intro_label = QLabel(intro, self)
        self.intro_label.setWordWrap(True)
        layout.addWidget(self.intro_label)

        # A grid, so every combo has the same width whatever is beside it.
        self._grid = QGridLayout()
        self._grid.setHorizontalSpacing(10)
        self._grid.setVerticalSpacing(6)
        self._grid.setColumnStretch(1, 1)
        self.objective_combo = QComboBox(self)
        self.objective_combo.addItem(self.tr("Don't change"), None)
        for key, objective in sorted(self._objectives.items(), key=lambda item: objective_sort_value(item[1], item[0])):
            self.objective_combo.addItem(objective_display_name(objective, key) or key, key)
        current_objectives = Counter(
            objective_display_name(self._objectives.get(image.get("objective_name")) or {}, image.get("objective_name"))
            or image.get("objective_name") or self.tr("Not set")
            for image in microscope
        )
        self._add_row(self.tr("Objective:"), self.objective_combo, current_objectives)

        self.field_combos: dict[str, QComboBox] = {}
        labels = {"contrast": self.tr("Contrast:"), "mount": self.tr("Mount:"), "stain": self.tr("Stain:")}
        for category, column in _FIELDS:
            combo = QComboBox(self)
            combo.addItem(self.tr("Don't change"), KEEP_FIELD)
            values = DatabaseTerms.canonicalize_list(category, SettingsDB.get_list_setting(
                DatabaseTerms.setting_key(category), DatabaseTerms.default_values(category)
            ))
            for value in values:
                combo.addItem(DatabaseTerms.translate(category, value), value)
            current = Counter(
                DatabaseTerms.translate(category, image.get(column)) if image.get(column) else self.tr("Not set")
                for image in microscope
            )
            self._add_row(labels[category], combo, current)
            self.field_combos[column] = combo
        layout.addLayout(self._grid)

        self.effect_label = QLabel("", self)
        self.effect_label.setObjectName("microscopeSettingsEffect")
        self.effect_label.setWordWrap(True)
        layout.addWidget(self.effect_label)
        layout.addStretch(1)

        buttons = QHBoxLayout()
        buttons.addStretch(1)
        self.cancel_btn = QPushButton(self.tr("Cancel"), self)
        self.cancel_btn.clicked.connect(self.reject)
        self.apply_btn = QPushButton(self.tr("Apply"), self)
        self.apply_btn.clicked.connect(self._on_apply)
        for button in (self.cancel_btn, self.apply_btn):
            button.setAutoDefault(False)
            buttons.addWidget(button)
        layout.addLayout(buttons)

        self.objective_combo.currentIndexChanged.connect(self._update_effect)
        for combo in self.field_combos.values():
            combo.currentIndexChanged.connect(self._update_effect)
        self._update_effect()

    def _add_row(self, label: str, combo: QComboBox, current: Counter) -> None:
        """A setting: its name, the combo, and what the selected images have now."""
        row = self._grid.rowCount()
        self._grid.addWidget(QLabel(label, self), row, 0)
        combo.setMinimumWidth(160)
        self._grid.addWidget(combo, row, 1)
        text = ", ".join(
            f"{value} ({count})" if len(current) > 1 else value
            for value, count in current.most_common()
        )
        now = QLabel(self.tr("Now: {values}").format(values=text) if text else "", self)
        now.setStyleSheet(f"color: #7f8c8d; font-size: {pt(9)}pt;")
        now.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self._grid.addWidget(now, row, 2)

    # -- the change ----------------------------------------------------------

    def selected_change(self) -> dict:
        key = self.objective_combo.currentData()
        objective = None
        if key:
            objective = ObjectiveChoice(
                key,
                float(self._objectives[key]["microns_per_pixel"]),
                CalibrationDB.get_active_calibration_id(key),
            )
        change = {"objective": objective}
        for column, combo in self.field_combos.items():
            change[column] = combo.currentData()
        return change

    def _update_effect(self) -> None:
        self._shown_change = self.selected_change()
        preview = MicroscopeSettingsDB.preview(self._image_ids, **self._shown_change)
        self._shown_preview = preview
        count = len(preview.image_ids)
        if not count:
            self.effect_label.setText(self.tr("Nothing to change yet: choose a new value above."))
        else:
            text = self.tr("%n image(s) will change.", "", count)
            if preview.measurements:
                text += " " + self.tr(
                    "%n measurement(s) will be recalculated for the new scale.", "", preview.measurements
                )
            self.effect_label.setText(text)
        self.apply_btn.setEnabled(bool(count))
        self.apply_btn.setText(self.tr("Apply to %n image(s)", "", count) if count else self.tr("Apply"))

    def _on_apply(self) -> None:
        try:
            self.outcome = MicroscopeSettingsDB.apply(
                self._image_ids, **self._shown_change, expected=self._shown_preview,
            )
        except MicroscopeSettingsChanged:
            self._update_effect()
            QMessageBox.information(
                self,
                self.tr("Change microscope settings"),
                self.tr("The images changed while this window was open. Check the new counts and apply again."),
            )
            return
        except Exception as exc:
            QMessageBox.warning(
                self,
                self.tr("Change microscope settings"),
                self.tr("Nothing was changed: {error}").format(error=str(exc)),
            )
            return
        self.accept()


def _positive(value) -> bool:
    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return False


__all__ = ["MicroscopeSettingsDialog"]
