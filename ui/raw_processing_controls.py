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

    def __init__(self, parent=None, *, show_shadow_lift: bool = False) -> None:
        super().__init__(parent)
        self._settings = RawRenderSettings.default()
        self._loading = False
        self._show_shadow_lift = bool(show_shadow_lift)

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

        self.light_row = QWidget(self)
        light_layout = QHBoxLayout(self.light_row)
        light_layout.setContentsMargins(0, 0, 0, 0)
        light_layout.setSpacing(8)
        self.light_label = QLabel(self.tr("Light:"), self.light_row)
        self.light_label.setMinimumWidth(72)
        self.light_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.light_slider = QSlider(Qt.Horizontal, self.light_row)
        self.light_slider.setRange(0, 20)
        self.light_slider.setSingleStep(1)
        self.light_slider.setPageStep(2)
        self.light_slider.valueChanged.connect(self._on_control_changed)
        self.light_value_label = QLabel("", self.light_row)
        self.light_value_label.setMinimumWidth(72)
        self.light_value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        light_layout.addWidget(self.light_label, 0)
        light_layout.addWidget(self.light_slider, 1)
        light_layout.addWidget(self.light_value_label, 0)
        layout.addWidget(self.light_row)
        self.exposure_row = self.light_row
        self.exposure_slider = self.light_slider
        self.exposure_value_label = self.light_value_label

        self.dark_row = QWidget(self)
        dark_layout = QHBoxLayout(self.dark_row)
        dark_layout.setContentsMargins(0, 0, 0, 0)
        dark_layout.setSpacing(8)
        self.dark_label = QLabel(self.tr("Dark:"), self.dark_row)
        self.dark_label.setMinimumWidth(72)
        self.dark_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.dark_slider = QSlider(Qt.Horizontal, self.dark_row)
        self.dark_slider.setRange(0, 10)
        self.dark_slider.setSingleStep(1)
        self.dark_slider.setPageStep(1)
        self.dark_slider.valueChanged.connect(self._on_control_changed)
        self.dark_value_label = QLabel("", self.dark_row)
        self.dark_value_label.setMinimumWidth(72)
        self.dark_value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        dark_layout.addWidget(self.dark_label, 0)
        dark_layout.addWidget(self.dark_slider, 1)
        dark_layout.addWidget(self.dark_value_label, 0)
        layout.addWidget(self.dark_row)
        self.dark_exposure_slider = self.dark_slider
        self.dark_exposure_value_label = self.dark_value_label

        self.auto_levels_checkbox = QCheckBox(self.tr("Auto levels"), self)
        self.auto_levels_checkbox.toggled.connect(self._on_control_changed)
        layout.addWidget(self.auto_levels_checkbox)

        self.tone_curve_checkbox = QCheckBox(self.tr("Tone curve"), self)
        self.tone_curve_checkbox.toggled.connect(self._on_control_changed)
        layout.addWidget(self.tone_curve_checkbox)

        curve_form = QFormLayout()
        curve_form.setContentsMargins(0, 0, 0, 0)
        curve_form.setHorizontalSpacing(8)
        curve_form.setVerticalSpacing(8)
        curve_form.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        self.curve_strength_label = QLabel(self.tr("Contrast:"), self)
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
        curve_form.addRow(self.curve_strength_label, self.curve_strength_row)
        self.contrast_slider = self.curve_strength_slider
        self.contrast_value_label = self.curve_strength_value_label

        self.curve_midpoint_label = QLabel(self.tr("Midpoint:"), self)
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
        curve_form.addRow(self.curve_midpoint_label, self.curve_midpoint_row)
        self.midpoint_slider = self.curve_midpoint_slider
        self.midpoint_value_label = self.curve_midpoint_value_label
        layout.addLayout(curve_form)

        if self._show_shadow_lift:
            self.shadow_lift_row = QWidget(self)
            shadow_lift_layout = QHBoxLayout(self.shadow_lift_row)
            shadow_lift_layout.setContentsMargins(0, 0, 0, 0)
            shadow_lift_layout.setSpacing(8)
            self.shadow_lift_label = QLabel(self.tr("Dark boost:"), self.shadow_lift_row)
            self.shadow_lift_label.setMinimumWidth(72)
            self.shadow_lift_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
            self.shadow_lift_slider = QSlider(Qt.Horizontal, self.shadow_lift_row)
            self.shadow_lift_slider.setRange(0, 100)
            self.shadow_lift_slider.setSingleStep(1)
            self.shadow_lift_slider.setPageStep(5)
            self.shadow_lift_slider.valueChanged.connect(self._on_control_changed)
            self.shadow_lift_value_label = QLabel("", self.shadow_lift_row)
            self.shadow_lift_value_label.setMinimumWidth(72)
            self.shadow_lift_value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
            shadow_lift_layout.addWidget(self.shadow_lift_label, 0)
            shadow_lift_layout.addWidget(self.shadow_lift_slider, 1)
            shadow_lift_layout.addWidget(self.shadow_lift_value_label, 0)
            layout.addWidget(self.shadow_lift_row)
            self.shadows_label = self.shadow_lift_label
            self.shadows_slider = self.shadow_lift_slider
            self.shadows_value_label = self.shadow_lift_value_label

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
            with QSignalBlocker(self.light_slider):
                self.light_slider.setValue(int(round(float(settings.light_ev) * 20.0)))
            with QSignalBlocker(self.dark_slider):
                self.dark_slider.setValue(int(round(abs(float(settings.dark_ev)) * 20.0)))
            with QSignalBlocker(self.tone_curve_checkbox):
                self.tone_curve_checkbox.setChecked(bool(settings.tone_curve_enabled))
            with QSignalBlocker(self.curve_strength_slider):
                self.curve_strength_slider.setValue(int(round(float(settings.tone_curve_strength) * 100.0)))
            with QSignalBlocker(self.curve_midpoint_slider):
                self.curve_midpoint_slider.setValue(int(round(float(settings.tone_curve_midpoint) * 100.0)))
            self.light_value_label.setText(self._ev_value_text(self.light_slider.value()))
            self.dark_value_label.setText(self._dark_ev_value_text(self.dark_slider.value()))
            self.curve_strength_value_label.setText(f"{float(settings.tone_curve_strength):.2f}")
            self.curve_midpoint_value_label.setText(f"{float(settings.tone_curve_midpoint):.2f}")
            if self._show_shadow_lift:
                self.shadow_lift_value_label.setText(self._shadow_lift_value_text(self.shadow_lift_slider.value()))
            self._set_tone_controls_enabled(bool(settings.tone_curve_enabled))
            self._refresh_pick_button_text()
        finally:
            self._loading = False

    def _settings_from_controls(self) -> RawRenderSettings:
        base_settings = RawRenderSettings.from_dict(self._settings)
        white_balance_mode = str(self.white_balance_selector.selected_value("camera") or "camera").strip().lower() or "camera"
        light_ev = max(0.0, min(1.0, float(self.light_slider.value()) / 20.0))
        dark_ev = -max(0.0, min(0.5, float(self.dark_slider.value()) / 20.0))
        tone_curve_strength = max(0.0, min(1.0, float(self.curve_strength_slider.value()) / 100.0))
        tone_curve_midpoint = max(0.0, min(1.0, float(self.curve_midpoint_slider.value()) / 100.0))
        settings = raw_settings_from_basic_controls(
            white_balance_mode=white_balance_mode if white_balance_mode in {"camera", "auto", "custom"} else "camera",
            wb_multipliers=base_settings.wb_multipliers,
            contrast=tone_curve_strength,
            midpoint=tone_curve_midpoint,
            preserve_tails=False,
            existing_settings=base_settings,
        )
        settings = replace(
            settings,
            exposure_ev=light_ev + dark_ev,
            light_ev=light_ev,
            dark_ev=dark_ev,
            auto_levels=bool(self.auto_levels_checkbox.isChecked()),
            tone_curve_enabled=bool(self.tone_curve_checkbox.isChecked()),
            tone_curve_strength=tone_curve_strength,
            tone_curve_midpoint=tone_curve_midpoint,
        )
        if self._show_shadow_lift and hasattr(self, "shadow_lift_slider"):
            settings = replace(
                settings,
                auto_levels_shadow_lift=max(0.0, min(0.10, float(self.shadow_lift_slider.value()) / 1000.0)),
            )
        if white_balance_mode in {"camera", "auto"}:
            settings = replace(
                settings,
                white_balance_mode=white_balance_mode,
                wb_multipliers=None,
                wb_selection=None,
                wb_multiplier_space=None,
                wb_sample_point=None,
                wb_selection_space=None,
            )
        else:
            settings = replace(settings, white_balance_mode="custom")
        self._set_tone_controls_enabled(bool(settings.tone_curve_enabled))
        return settings

    def _set_tone_controls_enabled(self, enabled: bool) -> None:
        for widget in (
            self.curve_strength_label,
            self.curve_strength_slider,
            self.curve_midpoint_label,
            self.curve_midpoint_slider,
            self.curve_strength_value_label,
            self.curve_midpoint_value_label,
        ):
            widget.setVisible(bool(enabled))
            widget.setEnabled(bool(enabled))

    @staticmethod
    def _ev_value_text(value: int) -> str:
        return f"{float(value) / 20.0:+.2f} EV"

    @staticmethod
    def _dark_ev_value_text(value: int) -> str:
        return f"{float(value) / 20.0:.2f} EV"

    @staticmethod
    def _shadow_lift_value_text(value: int) -> str:
        return f"{max(0.0, float(value) / 10.0):.1f}%"

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
        self.light_value_label.setText(self._ev_value_text(self.light_slider.value()))
        self.dark_value_label.setText(self._dark_ev_value_text(self.dark_slider.value()))
        self.curve_strength_value_label.setText(f"{float(self._settings.tone_curve_strength):.2f}")
        self.curve_midpoint_value_label.setText(f"{float(self._settings.tone_curve_midpoint):.2f}")
        if self._show_shadow_lift:
            self.shadow_lift_value_label.setText(self._shadow_lift_value_text(self.shadow_lift_slider.value()))
        self.settingsChanged.emit(self._settings)
