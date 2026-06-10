"""Shared RAW processing controls used by Prepare Images and Live Lab."""
from __future__ import annotations

from dataclasses import replace
from PySide6.QtCore import Signal, QSignalBlocker, Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSlider,
    QFormLayout,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from utils.raw_render import RawRenderSettings
from utils.image_processing_pipeline import raw_settings_from_basic_controls

from .segmented_selector import SegmentedSelector


class RawProcessingControls(QWidget):
    """Compact RAW controls backed by :class:`RawRenderSettings`."""

    settingsChanged = Signal(object)
    pickWhiteBalanceToggled = Signal(bool)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._settings = RawRenderSettings.default()
        self._loading = False

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        self.white_balance_selector = SegmentedSelector(self, compact=True, button_height=32, container_height=40)
        self.white_balance_selector.add_option(self.tr("Camera WB"), "camera", checked=True)
        self.white_balance_selector.add_option(self.tr("Auto WB"), "auto")
        self.white_balance_selector.add_option(self.tr("Custom WB"), "custom")
        self.white_balance_selector.selectionChanged.connect(self._on_control_changed)

        white_balance_row = QWidget(self)
        white_balance_row_layout = QHBoxLayout(white_balance_row)
        white_balance_row_layout.setContentsMargins(0, 4, 0, 4)
        white_balance_row_layout.setSpacing(8)
        white_balance_row_layout.addWidget(self.white_balance_selector, 0, Qt.AlignLeft | Qt.AlignVCenter)

        self.pick_button = QPushButton(self.tr("Pick"), white_balance_row)
        self.pick_button.setCheckable(True)
        self.pick_button.setMinimumHeight(32)
        self.pick_button.toggled.connect(self._on_pick_toggled)
        white_balance_row_layout.addWidget(self.pick_button, 0, Qt.AlignLeft | Qt.AlignVCenter)
        white_balance_row_layout.addStretch(1)
        white_balance_row.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        white_balance_row.setMinimumHeight(44)
        layout.addWidget(white_balance_row)

        self.auto_levels_checkbox = QCheckBox(self.tr("Auto levels"), self)
        self.auto_levels_checkbox.toggled.connect(self._on_control_changed)
        layout.addWidget(self.auto_levels_checkbox)

        self.preserve_tails_checkbox = QCheckBox(self.tr("Preserve tails"), self)
        self.preserve_tails_checkbox.toggled.connect(self._on_control_changed)
        layout.addWidget(self.preserve_tails_checkbox)

        self.tone_curve_checkbox = QCheckBox(self.tr("Tone curve"), self)
        self.tone_curve_checkbox.toggled.connect(self._on_control_changed)
        layout.addWidget(self.tone_curve_checkbox)

        curve_form = QFormLayout()
        curve_form.setContentsMargins(0, 0, 0, 0)
        curve_form.setHorizontalSpacing(8)
        curve_form.setVerticalSpacing(8)
        curve_form.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        self.curve_strength_row = QWidget(self)
        curve_strength_layout = QHBoxLayout(self.curve_strength_row)
        curve_strength_layout.setContentsMargins(0, 0, 0, 0)
        curve_strength_layout.setSpacing(8)
        self.curve_strength_slider = QSlider(Qt.Horizontal, self.curve_strength_row)
        self.curve_strength_slider.setRange(0, 100)
        self.curve_strength_slider.setSingleStep(1)
        self.curve_strength_slider.setPageStep(5)
        self.curve_strength_slider.valueChanged.connect(self._on_control_changed)
        self.curve_strength_value_label = QLabel("", self.curve_strength_row)
        self.curve_strength_value_label.setMinimumWidth(28)
        self.curve_strength_value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        curve_strength_layout.addWidget(self.curve_strength_slider, 1)
        curve_strength_layout.addWidget(self.curve_strength_value_label, 0)
        curve_form.addRow(self.tr("Curve strength:"), self.curve_strength_row)

        self.curve_midpoint_row = QWidget(self)
        curve_midpoint_layout = QHBoxLayout(self.curve_midpoint_row)
        curve_midpoint_layout.setContentsMargins(0, 0, 0, 0)
        curve_midpoint_layout.setSpacing(8)
        self.curve_midpoint_slider = QSlider(Qt.Horizontal, self.curve_midpoint_row)
        self.curve_midpoint_slider.setRange(0, 100)
        self.curve_midpoint_slider.setSingleStep(1)
        self.curve_midpoint_slider.setPageStep(5)
        self.curve_midpoint_slider.valueChanged.connect(self._on_control_changed)
        self.curve_midpoint_value_label = QLabel("", self.curve_midpoint_row)
        self.curve_midpoint_value_label.setMinimumWidth(28)
        self.curve_midpoint_value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        curve_midpoint_layout.addWidget(self.curve_midpoint_slider, 1)
        curve_midpoint_layout.addWidget(self.curve_midpoint_value_label, 0)
        curve_form.addRow(self.tr("Curve midpoint:"), self.curve_midpoint_row)
        layout.addLayout(curve_form)

        self._sync_controls_from_settings(self._settings)

    def settings(self) -> RawRenderSettings:
        self._settings = self._settings_from_controls()
        return self._settings

    def set_settings(self, settings: RawRenderSettings | dict | None) -> None:
        self._settings = RawRenderSettings.from_dict(settings)
        self._sync_controls_from_settings(self._settings)

    def set_pick_checked(self, checked: bool) -> None:
        with QSignalBlocker(self.pick_button):
            self.pick_button.setChecked(bool(checked))
        self._refresh_pick_button_text()

    def set_pick_enabled(self, enabled: bool) -> None:
        self.pick_button.setEnabled(bool(enabled))

    def set_controls_enabled(self, enabled: bool) -> None:
        self.setEnabled(bool(enabled))

    def set_tone_controls_enabled(self, enabled: bool) -> None:
        self._set_tone_controls_enabled(bool(enabled))

    def _sync_controls_from_settings(self, settings: RawRenderSettings) -> None:
        self._loading = True
        try:
            mode = str(settings.white_balance_mode or "camera").strip().lower() or "camera"
            if mode not in {"camera", "auto", "custom"}:
                mode = "camera"
            with QSignalBlocker(self.white_balance_selector):
                if not self.white_balance_selector.set_selected_value(mode):
                    self.white_balance_selector.set_selected_value("camera")
            with QSignalBlocker(self.auto_levels_checkbox):
                self.auto_levels_checkbox.setChecked(bool(settings.auto_levels))
            with QSignalBlocker(self.preserve_tails_checkbox):
                self.preserve_tails_checkbox.setChecked(bool(settings.auto_levels_soft_tails))
            with QSignalBlocker(self.tone_curve_checkbox):
                self.tone_curve_checkbox.setChecked(bool(settings.tone_curve_enabled))
            with QSignalBlocker(self.curve_strength_slider):
                self.curve_strength_slider.setValue(int(round(float(settings.tone_curve_strength) * 100.0)))
            with QSignalBlocker(self.curve_midpoint_slider):
                self.curve_midpoint_slider.setValue(int(round(float(settings.tone_curve_midpoint) * 100.0)))
            self.curve_strength_value_label.setText(str(int(round(float(settings.tone_curve_strength) * 100.0))))
            self.curve_midpoint_value_label.setText(str(int(round(float(settings.tone_curve_midpoint) * 100.0))))
            self._set_tone_controls_enabled(bool(settings.tone_curve_enabled))
            self._refresh_pick_button_text()
        finally:
            self._loading = False

    def _settings_from_controls(self) -> RawRenderSettings:
        base_settings = RawRenderSettings.from_dict(self._settings)
        white_balance_mode = str(self.white_balance_selector.selected_value("camera") or "camera").strip().lower() or "camera"
        tone_curve_strength = max(0.0, min(1.0, float(self.curve_strength_slider.value()) / 100.0))
        tone_curve_midpoint = max(0.0, min(1.0, float(self.curve_midpoint_slider.value()) / 100.0))
        settings = raw_settings_from_basic_controls(
            white_balance_mode=white_balance_mode if white_balance_mode in {"camera", "auto", "custom"} else "camera",
            wb_multipliers=base_settings.wb_multipliers,
            contrast=tone_curve_strength,
            midpoint=tone_curve_midpoint,
            preserve_tails=bool(self.preserve_tails_checkbox.isChecked()),
            existing_settings=base_settings,
        )
        settings = replace(
            settings,
            auto_levels=bool(self.auto_levels_checkbox.isChecked()),
            tone_curve_enabled=bool(self.tone_curve_checkbox.isChecked()),
            tone_curve_strength=tone_curve_strength,
            tone_curve_midpoint=tone_curve_midpoint,
        )
        self._set_tone_controls_enabled(bool(settings.tone_curve_enabled))
        return settings

    def _set_tone_controls_enabled(self, enabled: bool) -> None:
        for widget in (
            self.curve_strength_row,
            self.curve_midpoint_row,
            self.curve_strength_slider,
            self.curve_midpoint_slider,
            self.curve_strength_value_label,
            self.curve_midpoint_value_label,
        ):
            widget.setEnabled(bool(enabled))

    def _refresh_pick_button_text(self) -> None:
        self.pick_button.setText(self.tr("Cancel") if self.pick_button.isChecked() else self.tr("Pick"))

    def _on_pick_toggled(self, checked: bool) -> None:
        self._refresh_pick_button_text()
        if not self._loading:
            self.pickWhiteBalanceToggled.emit(bool(checked))

    def _on_control_changed(self, *_args) -> None:
        if self._loading:
            return
        self._settings = self._settings_from_controls()
        self.curve_strength_value_label.setText(str(int(round(float(self._settings.tone_curve_strength) * 100.0))))
        self.curve_midpoint_value_label.setText(str(int(round(float(self._settings.tone_curve_midpoint) * 100.0))))
        self.settingsChanged.emit(self._settings)
