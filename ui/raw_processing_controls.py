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

from .segmented_selector import SegmentedSelector


_EV_SLIDER_SCALE = 1000
_EV_SLIDER_MAX = 2000


class RawProcessingControls(QWidget):
    """Compact RAW controls backed by :class:`RawRenderSettings`."""

    settingsChanged = Signal(object)
    pickWhiteBalanceToggled = Signal(bool)

    def __init__(self, parent=None, *, show_shadow_lift: bool = False) -> None:
        super().__init__(parent)
        self._settings = RawRenderSettings.default()
        self._auto_level_settings: RawRenderSettings | None = None
        self._loading = False
        self._slider_change_pending = False
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
        self.light_slider.setRange(0, _EV_SLIDER_MAX)
        self.light_slider.setSingleStep(1)
        self.light_slider.setPageStep(25)
        self.light_slider.valueChanged.connect(self._on_control_changed)
        self.light_slider.sliderReleased.connect(self._on_slider_released)
        self.light_value_label = QLabel("", self.light_row)
        self.light_value_label.setMinimumWidth(84)
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
        self.dark_slider.setRange(0, _EV_SLIDER_MAX)
        self.dark_slider.setSingleStep(1)
        self.dark_slider.setPageStep(25)
        self.dark_slider.valueChanged.connect(self._on_control_changed)
        self.dark_slider.sliderReleased.connect(self._on_slider_released)
        self.dark_value_label = QLabel("", self.dark_row)
        self.dark_value_label.setMinimumWidth(84)
        self.dark_value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        dark_layout.addWidget(self.dark_label, 0)
        dark_layout.addWidget(self.dark_slider, 1)
        dark_layout.addWidget(self.dark_value_label, 0)
        layout.addWidget(self.dark_row)
        self.dark_exposure_slider = self.dark_slider
        self.dark_exposure_value_label = self.dark_value_label

        self.auto_levels_checkbox = QCheckBox(self.tr("Auto levels"), self)
        self.auto_levels_checkbox.toggled.connect(self._on_control_changed)
        self.auto_levels_checkbox.setVisible(True)
        layout.addWidget(self.auto_levels_checkbox)

        self.tone_curve_checkbox = QCheckBox(self.tr("Tone curve"), self)
        self.tone_curve_checkbox.toggled.connect(self._on_control_changed)
        layout.addWidget(self.tone_curve_checkbox)

        curve_form = QFormLayout()
        curve_form.setContentsMargins(0, 0, 0, 0)
        curve_form.setHorizontalSpacing(8)
        curve_form.setVerticalSpacing(8)
        curve_form.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        self.curve_strength_label = QLabel(self.tr("Strength:"), self)
        self.curve_strength_row = QWidget(self)
        curve_strength_layout = QHBoxLayout(self.curve_strength_row)
        curve_strength_layout.setContentsMargins(0, 0, 0, 0)
        curve_strength_layout.setSpacing(8)
        self.curve_strength_slider = QSlider(Qt.Horizontal, self.curve_strength_row)
        self.curve_strength_slider.setRange(0, 100)
        self.curve_strength_slider.setSingleStep(1)
        self.curve_strength_slider.setPageStep(5)
        self.curve_strength_slider.valueChanged.connect(self._on_control_changed)
        self.curve_strength_slider.sliderReleased.connect(self._on_slider_released)
        self.curve_strength_value_label = QLabel("", self.curve_strength_row)
        self.curve_strength_value_label.setMinimumWidth(28)
        self.curve_strength_value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        curve_strength_layout.addWidget(self.curve_strength_slider, 1)
        curve_strength_layout.addWidget(self.curve_strength_value_label, 0)
        curve_form.addRow(self.curve_strength_label, self.curve_strength_row)
        self.strength_label = self.curve_strength_label
        self.strength_slider = self.curve_strength_slider
        self.strength_value_label = self.curve_strength_value_label
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
        self.curve_midpoint_slider.sliderReleased.connect(self._on_slider_released)
        self.curve_midpoint_value_label = QLabel("", self.curve_midpoint_row)
        self.curve_midpoint_value_label.setMinimumWidth(28)
        self.curve_midpoint_value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        curve_midpoint_layout.addWidget(self.curve_midpoint_slider, 1)
        curve_midpoint_layout.addWidget(self.curve_midpoint_value_label, 0)
        curve_form.addRow(self.curve_midpoint_label, self.curve_midpoint_row)
        self.midpoint_slider = self.curve_midpoint_slider
        self.midpoint_value_label = self.curve_midpoint_value_label

        self.shadows_label = QLabel(self.tr("Shadows:"), self)
        self.shadows_row = QWidget(self)
        shadows_layout = QHBoxLayout(self.shadows_row)
        shadows_layout.setContentsMargins(0, 0, 0, 0)
        shadows_layout.setSpacing(8)
        self.shadows_slider = QSlider(Qt.Horizontal, self.shadows_row)
        self.shadows_slider.setRange(-100, 100)
        self.shadows_slider.setSingleStep(1)
        self.shadows_slider.setPageStep(5)
        self.shadows_slider.valueChanged.connect(self._on_control_changed)
        self.shadows_slider.sliderReleased.connect(self._on_slider_released)
        self.shadows_value_label = QLabel("", self.shadows_row)
        self.shadows_value_label.setMinimumWidth(40)
        self.shadows_value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        shadows_layout.addWidget(self.shadows_slider, 1)
        shadows_layout.addWidget(self.shadows_value_label, 0)
        curve_form.addRow(self.shadows_label, self.shadows_row)
        self.shadow_lift_label = self.shadows_label
        self.shadow_lift_row = self.shadows_row
        self.shadow_lift_slider = self.shadows_slider
        self.shadow_lift_value_label = self.shadows_value_label

        self.highlights_label = QLabel(self.tr("Highlights:"), self)
        self.highlights_row = QWidget(self)
        highlights_layout = QHBoxLayout(self.highlights_row)
        highlights_layout.setContentsMargins(0, 0, 0, 0)
        highlights_layout.setSpacing(8)
        self.highlights_slider = QSlider(Qt.Horizontal, self.highlights_row)
        self.highlights_slider.setRange(-100, 100)
        self.highlights_slider.setSingleStep(1)
        self.highlights_slider.setPageStep(5)
        self.highlights_slider.valueChanged.connect(self._on_control_changed)
        self.highlights_slider.sliderReleased.connect(self._on_slider_released)
        self.highlights_value_label = QLabel("", self.highlights_row)
        self.highlights_value_label.setMinimumWidth(40)
        self.highlights_value_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        highlights_layout.addWidget(self.highlights_slider, 1)
        highlights_layout.addWidget(self.highlights_value_label, 0)
        curve_form.addRow(self.highlights_label, self.highlights_row)
        layout.addLayout(curve_form)

        self._sync_controls_from_settings(self._settings)

    def settings(self) -> RawRenderSettings:
        self._settings = self._settings_from_controls()
        return self._settings

    def set_settings(self, settings: RawRenderSettings | dict | None) -> None:
        self._settings = RawRenderSettings.from_dict(settings)
        if bool(self._settings.auto_levels) and self._auto_level_settings is None:
            self._auto_level_settings = RawRenderSettings.from_dict(self._settings)
        self._sync_controls_from_settings(self._settings)
        if bool(self.auto_levels_checkbox.isChecked()) and self._auto_level_settings is not None:
            self._apply_auto_level_settings()

    def set_auto_level_settings(self, settings: RawRenderSettings | dict | None) -> None:
        resolved = RawRenderSettings.from_dict(settings)
        self._auto_level_settings = resolved
        if self._loading or not self.auto_levels_checkbox.isChecked():
            return
        self._apply_auto_level_settings()

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
        self._slider_change_pending = False
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
                self.light_slider.setValue(int(round(float(settings.light_ev) * _EV_SLIDER_SCALE)))
            with QSignalBlocker(self.dark_slider):
                self.dark_slider.setValue(int(round(abs(float(settings.dark_ev)) * _EV_SLIDER_SCALE)))
            with QSignalBlocker(self.tone_curve_checkbox):
                self.tone_curve_checkbox.setChecked(bool(settings.tone_curve_enabled))
            with QSignalBlocker(self.curve_strength_slider):
                self.curve_strength_slider.setValue(int(round(float(settings.tone_curve_strength) * 100.0)))
            with QSignalBlocker(self.curve_midpoint_slider):
                self.curve_midpoint_slider.setValue(int(round(float(settings.tone_curve_midpoint) * 100.0)))
            with QSignalBlocker(self.shadows_slider):
                self.shadows_slider.setValue(int(round(float(settings.tone_shadows) * 100.0)))
            with QSignalBlocker(self.highlights_slider):
                self.highlights_slider.setValue(int(round(float(settings.tone_highlights) * 100.0)))
            self._refresh_value_labels()
            self._set_tone_controls_enabled(bool(settings.tone_curve_enabled))
            self._refresh_pick_button_text()
        finally:
            self._loading = False

    def _apply_auto_level_settings(self) -> None:
        if self._auto_level_settings is None:
            return
        with QSignalBlocker(self.light_slider):
            self.light_slider.setValue(int(round(float(self._auto_level_settings.light_ev) * _EV_SLIDER_SCALE)))
        with QSignalBlocker(self.dark_slider):
            self.dark_slider.setValue(int(round(abs(float(self._auto_level_settings.dark_ev)) * _EV_SLIDER_SCALE)))
        self._refresh_value_labels()

    def _refresh_value_labels(self) -> None:
        self.light_value_label.setText(self._ev_value_text(self.light_slider.value()))
        self.dark_value_label.setText(self._dark_ev_value_text(self.dark_slider.value()))
        self.curve_strength_value_label.setText(f"{float(self.curve_strength_slider.value()) / 100.0:.2f}")
        self.curve_midpoint_value_label.setText(f"{float(self.curve_midpoint_slider.value()) / 100.0:.2f}")
        self.shadows_value_label.setText(self._signed_percent_value_text(self.shadows_slider.value()))
        self.highlights_value_label.setText(self._signed_percent_value_text(self.highlights_slider.value()))

    def _settings_from_controls(self) -> RawRenderSettings:
        base_settings = RawRenderSettings.from_dict(self._settings)
        white_balance_mode = str(self.white_balance_selector.selected_value("camera") or "camera").strip().lower() or "camera"
        auto_levels_enabled = bool(self.auto_levels_checkbox.isChecked())
        if auto_levels_enabled:
            light_ev = 0.0
            dark_ev = 0.0
        else:
            light_ev = max(0.0, min(2.0, float(self.light_slider.value()) / _EV_SLIDER_SCALE))
            dark_ev = -max(0.0, min(2.0, float(self.dark_slider.value()) / _EV_SLIDER_SCALE))
        tone_curve_strength = max(0.0, min(1.0, float(self.curve_strength_slider.value()) / 100.0))
        tone_curve_midpoint = max(0.0, min(1.0, float(self.curve_midpoint_slider.value()) / 100.0))
        settings = replace(
            base_settings,
            white_balance_mode=white_balance_mode if white_balance_mode in {"camera", "auto", "custom"} else "camera",
            wb_multipliers=None if white_balance_mode in {"camera", "auto"} else base_settings.wb_multipliers,
            wb_selection=None if white_balance_mode in {"camera", "auto"} else base_settings.wb_selection,
            wb_multiplier_space=None if white_balance_mode in {"camera", "auto"} else base_settings.wb_multiplier_space,
            wb_sample_point=None if white_balance_mode in {"camera", "auto"} else base_settings.wb_sample_point,
            wb_selection_space=None if white_balance_mode in {"camera", "auto"} else base_settings.wb_selection_space,
            exposure_ev=light_ev + dark_ev,
            light_ev=light_ev,
            dark_ev=dark_ev,
            auto_levels=bool(self.auto_levels_checkbox.isChecked()),
            black_percentile=0.0,
            white_percentile=1.0,
            auto_levels_strength=1.0,
            auto_levels_soft_tails=False,
            auto_levels_tail_size=0.03,
            auto_levels_shadow_lift=float(base_settings.auto_levels_shadow_lift),
            tone_curve_enabled=bool(self.tone_curve_checkbox.isChecked()),
            tone_curve_strength=tone_curve_strength,
            tone_curve_midpoint=tone_curve_midpoint,
            tone_shadows=float(self.shadows_slider.value()) / 100.0,
            tone_highlights=float(self.highlights_slider.value()) / 100.0,
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
            self.shadows_label,
            self.shadows_slider,
            self.shadows_value_label,
            self.highlights_label,
            self.highlights_slider,
            self.highlights_value_label,
        ):
            widget.setVisible(bool(enabled))
            widget.setEnabled(bool(enabled))

    @staticmethod
    def _ev_value_text(value: int) -> str:
        return f"{float(value) / _EV_SLIDER_SCALE:.3f}"

    @staticmethod
    def _dark_ev_value_text(value: int) -> str:
        return f"{float(value) / _EV_SLIDER_SCALE:.3f}"

    @staticmethod
    def _signed_percent_value_text(value: int) -> str:
        return f"{int(value):+d}"

    def _refresh_pick_button_text(self) -> None:
        self.pick_button.setText(self.tr("Cancel") if self.pick_button.isChecked() else self.tr("Pick"))

    def _on_pick_toggled(self, checked: bool) -> None:
        self._refresh_pick_button_text()
        if not self._loading:
            self.pickWhiteBalanceToggled.emit(bool(checked))

    def _on_control_changed(self, *_args) -> None:
        if self._loading:
            return
        sender = self.sender()
        if sender is self.auto_levels_checkbox and self.auto_levels_checkbox.isChecked():
            self._apply_auto_level_settings()
        if isinstance(sender, QSlider):
            if sender in {self.light_slider, self.dark_slider} and self.auto_levels_checkbox.isChecked():
                with QSignalBlocker(self.auto_levels_checkbox):
                    self.auto_levels_checkbox.setChecked(False)
            self._slider_change_pending = bool(sender.isSliderDown())
            self._refresh_value_labels()
            if sender.isSliderDown():
                return
        self._settings = self._settings_from_controls()
        self._slider_change_pending = False
        self._refresh_value_labels()
        self.settingsChanged.emit(self._settings)

    def _on_slider_released(self, *_args) -> None:
        if self._loading or not self._slider_change_pending:
            return
        self._slider_change_pending = False
        self._on_control_changed()
