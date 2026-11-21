"""Choose which existing images a re-calibrated objective applies to.

Shown after a new calibration of an objective is saved, when images still use
the previous calibration. The owner picks one of three scopes:

- new calibration only: nothing existing changes;
- update existing images: every image on the previous calibration;
- update recent images: only images taken today or in the previous N days.

The dialog only chooses. The caller runs
``CalibrationDB.recalculate_measurements_for_calibration`` with
:meth:`RecalibrationScopeDialog.since_date`, and the preview list reads the
same query (``CalibrationDB.get_recalibration_images``), so what the list
shows is what the update touches.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QFont, QPalette
from PySide6.QtWidgets import (
    QButtonGroup,
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QRadioButton,
    QSizePolicy,
    QSpinBox,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from database.models import CalibrationDB

class RecalibrationScopeDialog(QDialog):
    """Three-choice prompt: how far back a new objective calibration applies."""

    CHOICE_NEW_ONLY = "new_only"
    CHOICE_ALL = "all"
    CHOICE_RECENT = "recent"

    MAX_DAYS_BACK = 3650

    def __init__(
        self,
        parent: QWidget | None,
        *,
        calibration_id: int,
        old_scale_um: float,
        new_scale_um: float,
        today: date | None = None,
    ) -> None:
        super().__init__(parent)
        self._calibration_id = int(calibration_id)
        self._old_scale_um = float(old_scale_um)
        self._new_scale_um = float(new_scale_um)
        self._today = today or date.today()
        self._all_rows = CalibrationDB.get_recalibration_images(self._calibration_id)
        self._preview_rows: list[dict] = []
        self.setWindowTitle(self.tr("Update Measurements?"))
        self._build_ui()
        self.resize(640, 560)
        self._refresh_preview()

    # --- Construction -----------------------------------------------------

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setSpacing(10)

        diff_percent = (self._new_scale_um - self._old_scale_um) / self._old_scale_um * 100
        sign = "+" if diff_percent >= 0 else ""
        images, measurements, observations = self._totals(self._all_rows)
        intro = QLabel(
            self.tr(
                "The calibration scale has changed from {old:.2f} to {new:.2f} nm/px ({sign}{diff:.2f}%).\n\n"
                "{images} images with {measurements} spore measurements in {observations} observations "
                "use the previous calibration. Which images should use the new calibration?"
            ).format(
                old=self._old_scale_um * 1000,
                new=self._new_scale_um * 1000,
                sign=sign,
                diff=diff_percent,
                images=images,
                measurements=measurements,
                observations=observations,
            )
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)

        self._choice_group = QButtonGroup(self)
        self.new_only_radio = QRadioButton(self.tr("New calibration only"))
        self.all_radio = QRadioButton(self.tr("Update existing images"))
        self.recent_radio = QRadioButton(self.tr("Update recent images"))
        for radio in (self.new_only_radio, self.all_radio, self.recent_radio):
            self._choice_group.addButton(radio)

        layout.addWidget(self.new_only_radio)
        layout.addWidget(
            self._description(self.tr("Existing images and measurements keep the previous calibration."))
        )
        layout.addWidget(self.all_radio)
        layout.addWidget(
            self._description(
                self.tr("Rescale every image that uses the previous calibration of this objective.")
            )
        )
        layout.addWidget(self.recent_radio)

        days_row = QHBoxLayout()
        days_row.setContentsMargins(self._indent(), 0, 0, 0)
        days_row.setSpacing(6)
        days_row.addWidget(QLabel(self.tr("Taken today and the previous")))
        self.days_spin = QSpinBox()
        self.days_spin.setRange(0, self.MAX_DAYS_BACK)
        self.days_spin.setValue(0)
        days_row.addWidget(self.days_spin)
        days_row.addWidget(QLabel(self.tr("days")))
        days_row.addStretch(1)
        layout.addLayout(days_row)
        self.since_label = self._description("")
        layout.addWidget(self.since_label)

        self.summary_label = QLabel()
        self.summary_label.setWordWrap(True)
        summary_font = QFont(self.summary_label.font())
        summary_font.setBold(True)
        self.summary_label.setFont(summary_font)
        layout.addWidget(self.summary_label)

        self.preview_tree = QTreeWidget()
        self.preview_tree.setColumnCount(3)
        self.preview_tree.setHeaderLabels(
            [self.tr("Observation / image"), self.tr("Taken"), self.tr("Measurements")]
        )
        self.preview_tree.setRootIsDecorated(True)
        self.preview_tree.setUniformRowHeights(True)
        self.preview_tree.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        # Guardrail: room for the header and a handful of rows in a small window.
        self.preview_tree.setMinimumHeight(self.preview_tree.fontMetrics().lineSpacing() * 6)
        header = self.preview_tree.header()
        header.setStretchLastSection(False)
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        layout.addWidget(self.preview_tree, 1)

        self.button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        layout.addWidget(self.button_box)

        # Today's behaviour (the old "Yes") stays the default.
        self.all_radio.setChecked(True)
        self._choice_group.buttonToggled.connect(self._on_choice_toggled)
        self.days_spin.valueChanged.connect(self._on_days_changed)

    def _indent(self) -> int:
        return self.style().pixelMetric(self.style().PixelMetric.PM_ExclusiveIndicatorWidth) + 8

    def _description(self, text: str) -> QLabel:
        label = QLabel(text)
        label.setWordWrap(True)
        label.setContentsMargins(self._indent(), 0, 0, 0)
        label.setStyleSheet(f"color: {self._dim_text_color().name()};")
        return label

    def _dim_text_color(self) -> QColor:
        """Secondary text: the theme's text colour blended a third of the way to its background."""
        palette = self.palette()
        text = palette.color(QPalette.WindowText)
        background = palette.color(QPalette.Window)
        return QColor(
            round(text.red() * 0.65 + background.red() * 0.35),
            round(text.green() * 0.65 + background.green() * 0.35),
            round(text.blue() * 0.65 + background.blue() * 0.35),
        )

    # --- Choice -------------------------------------------------------------

    def choice(self) -> str:
        if self.new_only_radio.isChecked():
            return self.CHOICE_NEW_ONLY
        if self.recent_radio.isChecked():
            return self.CHOICE_RECENT
        return self.CHOICE_ALL

    def days_back(self) -> int:
        return int(self.days_spin.value())

    def since_date(self) -> date | None:
        """First day included for "Update recent images"; ``None`` for the other choices."""
        if self.choice() != self.CHOICE_RECENT:
            return None
        return self._today - timedelta(days=self.days_back())

    def selected_image_ids(self) -> list[int]:
        """The images the preview shows for the current choice; the update is limited to these."""
        return [int(row["image_id"]) for row in self._preview_rows]

    def selected_rows(self) -> list[dict]:
        """The images the current choice would update (read fresh from the database)."""
        choice = self.choice()
        if choice == self.CHOICE_NEW_ONLY:
            return []
        if choice == self.CHOICE_ALL:
            return list(self._all_rows)
        return CalibrationDB.get_recalibration_images(self._calibration_id, self.since_date())

    def _on_choice_toggled(self, _button, checked: bool) -> None:
        if checked:
            self._refresh_preview()

    def _on_days_changed(self, _value: int) -> None:
        if self.recent_radio.isChecked():
            self._refresh_preview()
        else:
            # Changing the period is a choice of recent images.
            self.recent_radio.setChecked(True)

    # --- Preview ------------------------------------------------------------

    @staticmethod
    def _totals(rows: list[dict]) -> tuple[int, int, int]:
        observations = {row.get("observation_id") for row in rows}
        measurements = sum(int(row.get("measurement_count") or 0) for row in rows)
        return len(rows), measurements, len(observations)

    def _observation_label(self, row: dict) -> str:
        if row.get("observation_id") is None:
            return self.tr("No observation")
        name = " ".join(
            part for part in (str(row.get("genus") or "").strip(), str(row.get("species") or "").strip()) if part
        )
        if not name:
            name = str(row.get("common_name") or "").strip()
        if not name:
            name = self.tr("Observation {id}").format(id=row.get("observation_id"))
        obs_date = str(row.get("observation_date") or "").strip()[:10]
        return f"{name} ({obs_date})" if obs_date else name

    def _refresh_preview(self) -> None:
        since = self._today - timedelta(days=self.days_back())
        self.since_label.setText(
            self.tr("Images taken on or after {date}.").format(date=since.isoformat())
        )

        rows = self.selected_rows()
        self._preview_rows = rows
        self.preview_tree.clear()
        choice = self.choice()
        if choice == self.CHOICE_NEW_ONLY:
            self.summary_label.setText(self.tr("No existing images or measurements will change."))
            self.preview_tree.setEnabled(False)
            return
        self.preview_tree.setEnabled(True)
        if not rows:
            self.summary_label.setText(
                self.tr("No images on the previous calibration were taken in this period.")
            )
            return

        images, measurements, observations = self._totals(rows)
        self.summary_label.setText(
            self.tr(
                "{images} images with {measurements} spore measurements in {observations} observations will be updated."
            ).format(images=images, measurements=measurements, observations=observations)
        )

        groups: dict[object, QTreeWidgetItem] = {}
        group_counts: dict[object, list[int]] = {}
        added_tooltip = self.tr("Date added; the image has no capture date.")
        for row in rows:
            key = row.get("observation_id")
            parent = groups.get(key)
            if parent is None:
                parent = QTreeWidgetItem(self.preview_tree, [self._observation_label(row), "", ""])
                groups[key] = parent
                group_counts[key] = [0, 0]
                # Rows are newest first, so the first image dates the group.
                parent.setText(1, str(row.get("image_date") or ""))
            count = int(row.get("measurement_count") or 0)
            group_counts[key][0] += 1
            group_counts[key][1] += count
            child = QTreeWidgetItem(
                parent,
                [Path(str(row.get("filepath") or "")).name, str(row.get("image_date") or ""), str(count)],
            )
            child.setTextAlignment(2, Qt.AlignRight | Qt.AlignVCenter)
            if row.get("image_date_source") == "added":
                child.setToolTip(1, added_tooltip)
                font = QFont(child.font(1))
                font.setItalic(True)
                child.setFont(1, font)
        for key, parent in groups.items():
            image_count, measurement_count = group_counts[key]
            images_text = (
                self.tr("1 image") if image_count == 1 else self.tr("{count} images").format(count=image_count)
            )
            parent.setText(0, f"{parent.text(0)} · {images_text}")
            parent.setText(2, str(measurement_count))
            parent.setTextAlignment(2, Qt.AlignRight | Qt.AlignVCenter)
